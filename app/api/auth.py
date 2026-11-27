import hashlib
import hmac
import logging
import math
import os
import secrets
import tempfile
import threading
import time
import uuid
from typing import Any

import jwt
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, Security, WebSocket, status
from fastapi.security import APIKeyHeader

from app.api.access import console_locked_for_request, is_local_request
from app.config import settings
from app.core.client_manager import client_manager, normalize_phone_number
from app.models.auth import AuthStatusResponse, MediaTicketResponse
from app.models.common import StatusOkResponse

router = APIRouter()
logger = logging.getLogger(__name__)

# API Key header 校验 (严格仅限请求头 X-API-Key)
api_key_header = APIKeyHeader(name="X-API-Key", scheme_name="ConsoleApiKey", auto_error=False)

# Web 2FA 会话 token 请求头 (严格仅限请求头 X-2FA-Token)
two_fa_header = APIKeyHeader(name="X-2FA-Token", scheme_name="ConsoleTwoFactor", auto_error=False)

# 部署者引导凭证请求头与 HttpOnly Cookie (仅受限引导期有效，不写入浏览器持久化存储或日志)
bootstrap_token_header = APIKeyHeader(name="X-Bootstrap-Token", scheme_name="BootstrapTokenHeader", auto_error=False)
BOOTSTRAP_COOKIE_NAME = "tg_bootstrap_session"
BOOTSTRAP_SESSION_TTL_SECONDS = 30 * 60
MIN_BOOTSTRAP_TOKEN_LENGTH = 16
MIN_BOOTSTRAP_TOKEN_UNIQUE_CHARS = 6

# 2FA 会话有效期 (秒): 与前端 tg_hub_2fa_unlocked 的 24 小时语义对齐
TWO_FA_SESSION_TTL_SECONDS = 24 * 60 * 60

# 内存限流器: IP -> 时间戳列表
MAX_RATE_LIMITER_IPS = 5000
_request_timestamps: dict[str, list[float]] = {}

# 部署者目标号码连续不匹配短时递增冷却 (IP / 引导会话)
BOOTSTRAP_MISMATCH_FREE_ATTEMPTS = 2
BOOTSTRAP_MISMATCH_COOLDOWNS = (5, 15, 30, 60)
BOOTSTRAP_MISMATCH_WINDOW_SECONDS = 300.0
_bootstrap_mismatch_state: dict[str, dict[str, float | int]] = {}
_bootstrap_mismatch_lock = threading.Lock()


class BootstrapAccessError(HTTPException):
    """结构化引导/受限拒绝异常，向调用方返回 error_code 与 detail."""

    def __init__(
        self,
        status_code: int,
        error_code: str,
        detail: str,
        headers: dict[str, str] | None = None,
    ):
        merged_headers = {"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"}
        if headers:
            merged_headers.update(headers)
        super().__init__(status_code=status_code, detail=detail, headers=merged_headers)
        self.error_code = error_code


def _constant_time_compare(val1: Any, val2: Any) -> bool:
    """
    无长度泄漏的恒定时间比对。
    两端均先做 SHA-256 哈希转换为固定 32 字节摘要，再用 hmac.compare_digest 比对，
    消除因长度不等导致早期短路的侧信道时序攻击风险。支持 str 与 bytes。
    """
    if isinstance(val1, str):
        b1 = val1.encode("utf-8")
    elif isinstance(val1, bytes):
        b1 = val1
    else:
        return False

    if isinstance(val2, str):
        b2 = val2.encode("utf-8")
    elif isinstance(val2, bytes):
        b2 = val2
    else:
        return False

    h1 = hashlib.sha256(b1).digest()
    h2 = hashlib.sha256(b2).digest()
    return hmac.compare_digest(h1, h2)


def ensure_default_account_admitted() -> None:
    """集中业务准入检查: 严格默认 Userbot 尚未验证通过前拒绝一切业务访问."""
    if not client_manager.is_default_account_admitted():
        raise BootstrapAccessError(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            error_code="telegram_bootstrap_required",
            detail="默认 Userbot 尚未完成绑定或验证，系统处于受限引导状态",
        )


def bootstrap_token_state() -> tuple[bool, str | None]:
    """检查 TG_BOOTSTRAP_TOKEN 是否已配置且满足高熵值要求."""
    raw = (settings.BOOTSTRAP_TOKEN or "").strip()
    if not raw:
        return False, None
    if (
        len(raw) < MIN_BOOTSTRAP_TOKEN_LENGTH
        or len(set(raw)) < MIN_BOOTSTRAP_TOKEN_UNIQUE_CHARS
        or _constant_time_compare(raw, settings.API_KEY)
    ):
        return False, None
    return True, raw


def resolve_bootstrap_phone_state() -> tuple[str, bool, bool]:
    """解析受限状态、默认绑定模式与 TG_DEFAULT_PHONE 配置合法性 (绝不回显明文号码)."""
    raw_phone = (settings.DEFAULT_PHONE or "").strip()
    default_phone_configured = bool(raw_phone)
    try:
        if raw_phone:
            normalize_phone_number(raw_phone)
    except ValueError:
        default_phone_valid = False
    else:
        default_phone_valid = True
    strict_default = client_manager.get_default_account_strict()
    if strict_default is None:
        return "unavailable", default_phone_configured, default_phone_valid
    if strict_default.is_admitted():
        return "admitted", default_phone_configured, default_phone_valid
    if strict_default.user_id is not None:
        return "rebind", default_phone_configured, default_phone_valid
    return "first_bind", default_phone_configured, default_phone_valid


def api_key_matches(provided: str | None) -> bool:
    """校验已配置的 Key。空 Key 的本机豁免由调用入口处理。"""
    expected = settings.API_KEY
    return bool(expected and provided and _constant_time_compare(provided, expected))


async def verify_api_key(
    header_key: str | None = Security(api_key_header),
):
    """
    检查 API Key 授权凭证 (严格仅限请求头 X-API-Key)。
    常规业务与管理 API 严禁通过 URL Query 参数传递 Key, 规避反代日志与历史记录泄露。
    若 Settings.API_KEY 未配置(留空)，默认放行；
    若配置了 API_KEY，请求头 X-API-Key 必须匹配。
    """
    if not settings.API_KEY:
        return True

    key_str = header_key if isinstance(header_key, str) else None
    if not api_key_matches(key_str):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="未经授权的控制台访问：API Key 无效或缺失")
    return True


def _load_or_create_persisted_secret() -> bytes:
    """加载 (首次则生成并持久化) 服务器端 2FA 签名密钥文件。

    密钥只落服务器本地 (DATA_DIR 下, POSIX 0600), 对客户端绝不可推导 —— API_KEY 会
    分发给每个控制台用户, 从它派生签名密钥等于让第一因子持有者离线伪造第二因子
    (CWE-836, 旧实现 sha256(b"2fa:" + API_KEY) 已废除)。原子写 (tmp + os.replace)
    防半写; 空文件视为损坏并重新生成; 单实例锁保证无并发生成竞态。"""
    os.makedirs(settings.DATA_DIR, exist_ok=True)
    secret_path = os.path.join(settings.DATA_DIR, ".web_2fa_secret")
    try:
        with open(secret_path, "rb") as f:
            existing = f.read().strip()
        if existing:
            return existing
    except FileNotFoundError:
        logger.debug("2FA 持久密钥文件不存在, 首次生成: %s", secret_path)

    secret = secrets.token_urlsafe(48).encode("utf-8")
    fd, tmp_path = tempfile.mkstemp(dir=settings.DATA_DIR)
    with os.fdopen(fd, "wb") as f:
        f.write(secret)
    os.replace(tmp_path, secret_path)
    return secret


def _get_bootstrap_secret() -> bytes | None:
    """派生受限引导 HttpOnly Cookie 签名密钥 (绑定 TG_BOOTSTRAP_TOKEN 与本机密钥)."""
    ok, raw_token = bootstrap_token_state()
    if not ok or not raw_token:
        return None
    server_secret = _load_or_create_persisted_secret()
    return hmac.new(server_secret, b"bootstrap:" + raw_token.encode("utf-8"), hashlib.sha256).digest()


def issue_bootstrap_session_token() -> str:
    """签发短期受限引导 HttpOnly 会话 JWT."""
    secret = _get_bootstrap_secret()
    if secret is None:
        raise BootstrapAccessError(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            error_code="bootstrap_token_unconfigured",
            detail="未配置有效的高熵值部署者引导密钥 TG_BOOTSTRAP_TOKEN，请检查本地部署配置",
        )
    now = int(time.time())
    payload = {
        "sub": "bootstrap",
        "iat": now,
        "exp": now + BOOTSTRAP_SESSION_TTL_SECONDS,
        "jti": uuid.uuid4().hex,
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def validate_bootstrap_session_token(token: str | None) -> bool:
    """校验短期受限引导 Cookie JWT."""
    if not token or not isinstance(token, str):
        return False
    secret = _get_bootstrap_secret()
    if secret is None:
        return False
    try:
        payload = jwt.decode(token.strip(), secret, algorithms=["HS256"])
    except jwt.PyJWTError:
        return False
    return payload.get("sub") == "bootstrap"


def resolve_bootstrap_credential(request: Request) -> str | None:
    """从 HttpOnly Cookie 或请求头 X-Bootstrap-Token 解析当前有效引导凭证."""
    ok, expected_token = bootstrap_token_state()
    if not ok or not expected_token:
        return None
    cookie_token = request.cookies.get(BOOTSTRAP_COOKIE_NAME)
    if cookie_token and validate_bootstrap_session_token(cookie_token):
        return f"cookie:{cookie_token.strip()}"
    header_token = request.headers.get("x-bootstrap-token")
    if header_token is not None and verify_bootstrap_header_token(request, header_token, expected_token):
        return f"header:{header_token.strip()}"
    return None


def verify_bootstrap_header_token(request: Request, token: str | None, expected_token: str) -> bool:
    """每次校验原始引导口令都使用同一个 IP 尝试预算。"""
    bootstrap_auth_rate_limiter(request)
    return bool(token and token.strip() and _constant_time_compare(token.strip(), expected_token))


def verify_bootstrap_deployer(request: Request) -> str:
    """受限模式下强制校验部署者认证；正常业务状态下拒绝部署者凭证授权."""
    if client_manager.is_default_account_admitted():
        raise BootstrapAccessError(
            status_code=status.HTTP_403_FORBIDDEN,
            error_code="bootstrap_not_active",
            detail="默认 Userbot 已完成验证，引导密钥在正常业务状态下无权使用",
        )
    ok, _ = bootstrap_token_state()
    if not ok:
        raise BootstrapAccessError(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            error_code="bootstrap_token_unconfigured",
            detail="未配置有效的高熵值部署者引导密钥 TG_BOOTSTRAP_TOKEN，请先在部署配置中设置后重试",
        )
    cred = resolve_bootstrap_credential(request)
    if not cred:
        raise BootstrapAccessError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            error_code="bootstrap_unauthorized",
            detail="需要部署者引导认证：TG_BOOTSTRAP_TOKEN 无效或缺失",
        )
    return cred


def _get_2fa_secret() -> bytes | None:
    """
    解析 Web 2FA 会话 JWT 签名密钥。
    优先使用 SECRET_KEY (显式配置); 缺省时加载/生成 DATA_DIR 下的服务器端持久密钥
    (旧"从 API_KEY 域分离派生"已废除 —— API_KEY 持有者可离线推导签名
    密钥伪造会话, 第二因子对第一因子持有者形同虚设)。
    每次调用现取不缓存: 兼容单测 monkeypatch 与密钥热轮换 (持久密钥跨 API_KEY 轮换
    与重启保持稳定 —— 这是新契约: 会话不再随 API_KEY 轮换而失效)。
    开放模式 (未配置 API_KEY) 返回 None, 与 verify_2fa_session 的放行语义对齐。
    """
    if settings.SECRET_KEY:
        return settings.SECRET_KEY.encode("utf-8")
    if settings.API_KEY:
        return _load_or_create_persisted_secret()
    return None


def issue_2fa_token() -> str | None:
    """
    签发 Web 2FA 会话 JWT (HS256, 有效期 24 小时)。
    本机开放模式 (未配置 API_KEY) 下无密钥, 返回 None。
    说明: payload 中的 jti 为预留字段, 服务端不校验、无吊销机制;
    未设置 SECRET_KEY 时修改 API_KEY 会使所有已签发会话立即失效 (HMAC 密钥随派生源变化)。
    """
    secret = _get_2fa_secret()
    if secret is None:
        return None
    now = int(time.time())
    payload = {
        "sub": "owner",
        "iat": now,
        "exp": now + TWO_FA_SESSION_TTL_SECONDS,
        "jti": uuid.uuid4().hex,
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def validate_2fa_token(token: str | None) -> bool:
    """校验 Web 2FA 会话 JWT (签名 + 有效期 + sub 声明); 任何异常一律按无效处理。"""
    if not token:
        return False
    secret = _get_2fa_secret()
    if secret is None:
        return False
    try:
        payload = jwt.decode(token, secret, algorithms=["HS256"])
    except jwt.PyJWTError:
        return False
    return payload.get("sub") == "owner"


async def verify_2fa_session(
    header_token: str | None = Security(two_fa_header),
):
    """
    校验 Web 2FA 会话凭证 (严格仅限请求头 X-2FA-Token)。
    常规业务与管理 API 严禁通过 URL Query 参数传递 2FA Token。
    未配置 API_KEY 的本机开放模式直接放行 (与 verify_api_key 行为对齐);
    失败返回 403 而非 401 以区分凭证层级, detail 以 "2FA" 开头供前端识别并引导重验。
    """
    if not settings.API_KEY:
        return True
    if not validate_2fa_token(header_token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="2FA 会话无效或已过期，请重新完成 Web 2FA 验证"
        )
    return True


# ── 短时效媒体 ticket 登记表 ──
# 设计约束 (关键): ticket 为 TTL 内可重复消费, 不可做成单次有效 —— <video> 播放器会以
# 同一 URL 发起多个 206 Range 分段请求, 一次性消费会直接破坏播放与进度条拖动;
# URL 泄露窗口由此收敛到 TTL 之内。内存登记 + 惰性过期清理 (签发/消费时顺手清扫)。
MEDIA_TICKET_TTL_SECONDS = 60
_media_tickets: dict[str, float] = {}
_media_tickets_lock = threading.Lock()


def issue_media_ticket() -> str:
    """签发随机短时效 ticket 并登记过期时间; 顺手清扫全部过期票据防内存无界增长。"""
    now = time.time()
    with _media_tickets_lock:
        for tk, expiry in list(_media_tickets.items()):
            if expiry <= now:
                _media_tickets.pop(tk, None)
        ticket = secrets.token_urlsafe(32)
        _media_tickets[ticket] = now + MEDIA_TICKET_TTL_SECONDS
    return ticket


def consume_media_ticket(ticket: str) -> bool:
    """校验 ticket 是否在有效期内 (TTL 内可重复消费, 仅校验不吊销); 过期票顺手剔除。"""
    now = time.time()
    with _media_tickets_lock:
        expiry = _media_tickets.get(ticket)
        if expiry is None:
            return False
        if expiry <= now:
            _media_tickets.pop(ticket, None)
            return False
    return True


async def verify_media_access(
    header_key: str | None = Security(api_key_header),
    header_token: str | None = Security(two_fa_header),
    ticket: str | None = Query(None, alias="ticket", description="短时效媒体 ticket (浏览器原生标签直连场景)"),
):
    """
    专门针对浏览器原生直连资源 (媒体流 preview / 下载 download) 的复合凭据校验。
    因 HTML <video>, <img>, <a> 标签无法附带自定义 HTTP Headers, 凭短时效 ticket 直连
    (前端先经 POST /api/auth/media-ticket 取票); 程序化客户端走双 Header 路径。
    明文 query 传递 Key / 2FA Token 的历史豁免已收口删除 (F9)。
    """
    ensure_default_account_admitted()
    expected_key = settings.API_KEY
    if not expected_key:
        return True

    # 路径 1: 短时效 ticket (浏览器原生标签; TTL 内可重复消费)
    if ticket and consume_media_ticket(ticket):
        return True

    # 路径 2: 双 Header 凭证 (程序化客户端)
    if not api_key_matches(header_key):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="未经授权的控制台访问：API Key 无效或缺失")
    if not validate_2fa_token(header_token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="2FA 会话无效或已过期，请重新完成 Web 2FA 验证"
        )
    return True


@router.post(
    "/media-ticket",
    response_model=MediaTicketResponse,
    dependencies=[Depends(verify_api_key), Depends(verify_2fa_session)],
)
async def create_media_ticket():
    """
    签发短时效媒体流 ticket: 挂 X-API-Key + X-2FA-Token 双层鉴权。
    ticket 为 TTL 内可重复消费 (兼容 <video> 同 URL 多次 206 Range 分段请求),
    供媒体流 preview/download 与 WS 握手经 `ticket=` query 参数鉴权,
    明文 Key / 2FA Token 从此不进任何 URL。
    """
    return {"ticket": issue_media_ticket(), "expires_in": MEDIA_TICKET_TTL_SECONDS}


@router.get("/status", response_model=AuthStatusResponse)
async def get_auth_status(request: Request, response: Response):
    """
    查询控制台鉴权与默认 Userbot 受限准入状态。
    此接口公开访问，不返回手机号或业务数据，供前端决策挂载受限引导界面或正常控制台。
    """
    response.headers["Cache-Control"] = "no-store"
    restricted = not client_manager.is_default_account_admitted()
    bootstrap_configured, _ = bootstrap_token_state()
    binding_mode, default_phone_configured, default_phone_valid = resolve_bootstrap_phone_state()
    bootstrap_authorized = bool(restricted and bootstrap_configured and resolve_bootstrap_credential(request))
    return {
        "auth_required": bool(settings.API_KEY),
        "console_locked": console_locked_for_request(request),
        "restricted": restricted,
        "bootstrap_token_configured": bootstrap_configured,
        "bootstrap_authorized": bootstrap_authorized,
        "default_phone_configured": default_phone_configured,
        "default_phone_valid": default_phone_valid,
        "binding_mode": binding_mode,
    }


@router.get("/verify", response_model=StatusOkResponse, dependencies=[Depends(verify_api_key)])
async def verify_auth_token():
    """
    校验 API Key 是否正确。
    用于前端登录弹窗提交 Key 时的显式验证。
    """
    return {"status": "ok", "message": "API Key 验证成功"}


async def verify_websocket_auth(websocket: WebSocket) -> bool:
    """
    WebSocket 连接握手鉴权校验 (短时效 ticket, 前端每次连接/重连先经 /api/auth/media-ticket 取票)。
    默认 Userbot 尚未验证通过时拒绝握手；accept 前关闭由 ASGI 转为 HTTP 403。
    WS 握手不经 HTTP 中间件, 本机免认证判定 (access.is_local_request) 在此自行执行 ——
    非本机来源 (公网直连/反代转发/来源不明) 在 API_KEY 留空时同样拒绝握手。
    """
    if not client_manager.is_default_account_admitted():
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return False

    expected_key = settings.API_KEY
    if not expected_key:
        if not is_local_request(websocket):
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return False
        return True

    ticket = websocket.query_params.get("ticket")
    if not ticket or not consume_media_ticket(ticket):
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return False
    return True


def _prune_rate_limiter_cache(
    store: dict[str, list[float]],
    new_key: str,
    window_start: float,
    max_capacity: int = MAX_RATE_LIMITER_IPS,
) -> None:
    """容量防爆清扫：仅当遇到新 IP 且字典达到容量上限时，清理过期 IP 并按最旧时间戳驱逐多余条目"""
    if new_key not in store and len(store) >= max_capacity:
        expired_ips = [ip for ip, ts in store.items() if not ts or ts[-1] <= window_start]
        for ip in expired_ips:
            store.pop(ip, None)
        # 若清扫后仍达到或超过容量上限，按最旧活动时间驱逐多余条目
        if len(store) >= max_capacity:
            overflow_count = len(store) - max_capacity + 1
            sorted_ips = sorted(store.keys(), key=lambda k: store.get(k, [0])[-1] if store.get(k) else 0)
            for old_ip in sorted_ips[:overflow_count]:
                store.pop(old_ip, None)


def rate_limiter_middleware(request: Request) -> None:
    """
    滑动窗口速率限制防护 (Rate Limit 防刷)。
    支持按 IP 限制每分钟最大请求数 (settings.RATE_LIMIT_PER_MINUTE)。
    内置最大容量约束 (MAX_RATE_LIMITER_IPS = 5000)，超限时清扫过期 IP，防内存无界增长。
    """
    limit = settings.RATE_LIMIT_PER_MINUTE
    if limit <= 0:
        return

    client_ip = (request.client.host if request.client and request.client.host else None) or "127.0.0.1"
    now = time.time()
    window_start = now - 60.0

    # 容量防爆清扫
    _prune_rate_limiter_cache(_request_timestamps, client_ip, window_start)

    timestamps = _request_timestamps.get(client_ip, [])
    # 过滤出 60 秒以内的请求
    valid_timestamps = [t for t in timestamps if t > window_start]
    valid_timestamps.append(now)
    # 防御单 IP 极高频泛洪导致单个列表内存膨胀 (保留最新 limit + 1 条即可判断限流)
    if len(valid_timestamps) > limit + 1:
        valid_timestamps = valid_timestamps[-(limit + 1) :]
    _request_timestamps[client_ip] = valid_timestamps

    if len(valid_timestamps) > limit:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"请求过于频繁，已触发限流机制 (上限 {limit} 次/分钟)。请稍后再试。",
        )


# 敏感操作独立低配额限速器 (触发真实短信/TG消息下发)
SENSITIVE_ACTION_LIMIT_PER_MINUTE = 5
_sensitive_action_timestamps: dict[str, list[float]] = {}

# 登录验证码/2FA 密码提交端点防爆破限速器 (不限制 QR 状态轮询)
LOGIN_ATTEMPT_LIMIT_PER_MINUTE = 10
_login_attempt_timestamps: dict[str, list[float]] = {}
_sensitive_rate_limit_lock = threading.Lock()


def _enforce_sliding_window_limit(
    store: dict[str, list[float]],
    key: str,
    limit: int,
    detail: str,
) -> None:
    with _sensitive_rate_limit_lock:
        now = time.monotonic()
        window_start = now - 60.0
        _prune_rate_limiter_cache(store, key, window_start)

        valid_timestamps = [t for t in store.get(key, []) if t > window_start]
        valid_timestamps.append(now)
        valid_timestamps = valid_timestamps[-(limit + 1) :]
        store[key] = valid_timestamps

    if len(valid_timestamps) > limit:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=detail,
        )


def sensitive_action_rate_limiter(request: Request) -> None:
    """针对触发真实短信或 Telegram 验证码下发的敏感端点独立滑动窗口限流 (5次/分钟)"""
    client_ip = (request.client.host if request.client and request.client.host else None) or "127.0.0.1"
    _enforce_sliding_window_limit(
        _sensitive_action_timestamps,
        client_ip,
        SENSITIVE_ACTION_LIMIT_PER_MINUTE,
        f"短信/验证码下发过于频繁，已触发敏感操作限流保护 (上限 {SENSITIVE_ACTION_LIMIT_PER_MINUTE} 次/分钟)。请稍后再试。",
    )


def login_attempt_rate_limiter(request: Request) -> None:
    """针对验证码与 2FA 密码校验提交的滑动窗口防爆破限流 (10次/分钟, 不影响扫码状态轮询)"""
    client_ip = (request.client.host if request.client and request.client.host else None) or "127.0.0.1"
    _enforce_sliding_window_limit(
        _login_attempt_timestamps,
        client_ip,
        LOGIN_ATTEMPT_LIMIT_PER_MINUTE,
        f"验证尝试过于频繁，已触发登录安全保护 (上限 {LOGIN_ATTEMPT_LIMIT_PER_MINUTE} 次/分钟)。请稍后再试。",
    )


def bootstrap_auth_rate_limiter(request: Request) -> None:
    """引导口令的尝试次数按可信来源 IP 计算。"""
    client_ip = (request.client.host if request.client and request.client.host else None) or "127.0.0.1"
    _enforce_sliding_window_limit(
        _login_attempt_timestamps,
        f"bootstrap:{client_ip}",
        LOGIN_ATTEMPT_LIMIT_PER_MINUTE,
        "部署者引导认证尝试过于频繁，请稍后再试",
    )


def _bootstrap_mismatch_keys(request: Request, bootstrap_cred: str) -> tuple[str, str]:
    """仅基于可信代理解析后的 request.client IP 与已认证引导会话构造限流键."""
    client_ip = (request.client.host if request.client and request.client.host else None) or "127.0.0.1"
    cred_digest = hashlib.sha256(bootstrap_cred.encode("utf-8")).hexdigest()[:24]
    return f"ip:{client_ip}", f"session:{cred_digest}"


def _prune_bootstrap_mismatch_locked(now: float) -> None:
    if len(_bootstrap_mismatch_state) < MAX_RATE_LIMITER_IPS:
        return
    expired = [
        k
        for k, entry in _bootstrap_mismatch_state.items()
        if (now - float(entry.get("window_start", 0.0))) > BOOTSTRAP_MISMATCH_WINDOW_SECONDS
        and float(entry.get("cooldown_until", 0.0)) <= now
    ]
    for k in expired:
        _bootstrap_mismatch_state.pop(k, None)
    if len(_bootstrap_mismatch_state) >= MAX_RATE_LIMITER_IPS:
        overflow = len(_bootstrap_mismatch_state) - MAX_RATE_LIMITER_IPS + 1
        oldest = sorted(
            _bootstrap_mismatch_state.keys(),
            key=lambda k: float(_bootstrap_mismatch_state[k].get("window_start", 0.0)),
        )
        for k in oldest[:overflow]:
            _bootstrap_mismatch_state.pop(k, None)


def check_bootstrap_mismatch_cooldown(request: Request, bootstrap_cred: str | None) -> int | None:
    """若当前已认证 IP 或引导会话处于号码不匹配冷却中，返回剩余秒数."""
    if not bootstrap_cred:
        return None
    keys = _bootstrap_mismatch_keys(request, bootstrap_cred)
    now = time.monotonic()
    remaining = 0
    with _bootstrap_mismatch_lock:
        for key in keys:
            entry = _bootstrap_mismatch_state.get(key)
            if not entry:
                continue
            cooldown_until = float(entry.get("cooldown_until", 0.0))
            if cooldown_until > now:
                remaining = max(remaining, math.ceil(cooldown_until - now))
            elif (now - float(entry.get("window_start", 0.0))) > BOOTSTRAP_MISMATCH_WINDOW_SECONDS:
                _bootstrap_mismatch_state.pop(key, None)
    return remaining if remaining > 0 else None


def record_bootstrap_phone_mismatch(request: Request, bootstrap_cred: str | None) -> int | None:
    """记录已认证部署者的目标号码不匹配并计算递增短时冷却 (不永久封禁，不回显服务端号码)."""
    if not bootstrap_cred:
        return None
    keys = _bootstrap_mismatch_keys(request, bootstrap_cred)
    now = time.monotonic()
    applied_cooldown: int | None = None
    with _bootstrap_mismatch_lock:
        _prune_bootstrap_mismatch_locked(now)
        for key in keys:
            entry = _bootstrap_mismatch_state.get(key)
            if not entry or (now - float(entry.get("window_start", 0.0))) > BOOTSTRAP_MISMATCH_WINDOW_SECONDS:
                count = 1
                window_start = now
            else:
                count = int(entry.get("count", 0)) + 1
                window_start = float(entry.get("window_start", now))

            cooldown_seconds = 0
            cooldown_until = float(entry.get("cooldown_until", 0.0)) if entry else 0.0
            if count > BOOTSTRAP_MISMATCH_FREE_ATTEMPTS:
                step = min(
                    count - BOOTSTRAP_MISMATCH_FREE_ATTEMPTS - 1,
                    len(BOOTSTRAP_MISMATCH_COOLDOWNS) - 1,
                )
                cooldown_seconds = BOOTSTRAP_MISMATCH_COOLDOWNS[step]
                cooldown_until = now + cooldown_seconds
                applied_cooldown = max(applied_cooldown or 0, cooldown_seconds)

            _bootstrap_mismatch_state[key] = {
                "count": count,
                "window_start": window_start,
                "cooldown_until": cooldown_until,
                "last_mismatch_ns": time.monotonic_ns(),
            }
    return applied_cooldown


def reset_bootstrap_phone_mismatch(request: Request, bootstrap_cred: str | None, *, started_ns: int) -> None:
    """目标号码匹配后，只清除该请求开始前留下的不匹配计数。"""
    if not bootstrap_cred:
        return
    keys = _bootstrap_mismatch_keys(request, bootstrap_cred)
    with _bootstrap_mismatch_lock:
        for key in keys:
            entry = _bootstrap_mismatch_state.get(key)
            if entry is not None and entry["last_mismatch_ns"] < started_ns:
                _bootstrap_mismatch_state.pop(key, None)


def reset_auth_runtime_state_for_tests() -> None:
    """重置内存限流、票据与引导冷却状态 (仅供测试隔离)."""
    _request_timestamps.clear()
    with _media_tickets_lock:
        _media_tickets.clear()
    with _sensitive_rate_limit_lock:
        _sensitive_action_timestamps.clear()
        _login_attempt_timestamps.clear()
    with _bootstrap_mismatch_lock:
        _bootstrap_mismatch_state.clear()
