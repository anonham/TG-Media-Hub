"""共享 Telegram 登录挑战编排服务 (OTP / Resend / QR / 2FA 密码续接 / 身份校验与原子会话交接).

架构边界:
- 位于 app/services/, 由 app/api/telegram_auth.py 与 app/api/accounts.py 共同调用;
- Telethon 传输、身份校验原语与回滚安全交接留在 app/core/client_manager.py;
- 所有 phone_code_hash 与临时客户端生命周期仅在服务端内部托管，绝不对外暴露或落日志。
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import secrets
import threading
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, TypeGuard

from telethon import TelegramClient, functions, types, utils
from telethon import errors as tl_errors

from app.config import settings
from app.core.client_manager import (
    LoginBinding,
    TelegramAccountClient,
    canonical_phone_number,
    client_manager,
    normalize_phone_number,
)
from app.db.account_repository import AccountRepository

logger = logging.getLogger(__name__)

MAX_ACTIVE_CHALLENGES = 128
OTP_CHALLENGE_TTL_SECONDS = 300.0
QR_MAX_TTL_SECONDS = 180.0
QR_READY_CLAIM_TTL_SECONDS = 60.0
PASSWORD_CONTINUATION_TTL_SECONDS = 180.0
TERMINAL_TOMBSTONE_TTL_SECONDS = 300.0
MAX_VERIFY_ATTEMPTS_PER_CHALLENGE = 5
MAX_PASSWORD_ATTEMPTS_PER_CHALLENGE = 5
FIRST_BIND_CREATE_LIMIT_PER_MINUTE = 10
TERMINAL_STATES = ("authenticated", "expired", "cancelled", "failed")

LoginFlow = Literal["native", "account"]
LoginMethod = Literal["otp", "qr"]

_SENT_CODE_CHANNEL_MAP: dict[str, str] = {
    "SentCodeTypeApp": "app",
    "SentCodeTypeSms": "sms",
    "SentCodeTypeCall": "call",
    "SentCodeTypeFlashCall": "flash_call",
    "SentCodeTypeMissedCall": "missed_call",
    "SentCodeTypeFragmentSms": "fragment_sms",
    "SentCodeTypeFirebaseSms": "firebase_sms",
    "SentCodeTypeEmailCode": "email",
}

_NEXT_CODE_CHANNEL_MAP: dict[str, str] = {
    "CodeTypeSms": "sms",
    "CodeTypeCall": "call",
    "CodeTypeFlashCall": "flash_call",
    "CodeTypeMissedCall": "missed_call",
    "CodeTypeFragmentSms": "fragment_sms",
}

_UNSUPPORTED_SENT_CODE_TYPES = {
    "SentCodeTypeSetUpEmailRequired": "该账号需先在官方客户端设置登录邮箱，暂不支持此方式",
    "SentCodeTypeSmsWord": "Telegram 返回了暂不支持的短信词组验证类型，请改用扫码登录",
    "SentCodeTypeSmsPhrase": "Telegram 返回了暂不支持的短信短语验证类型，请改用扫码登录",
}

_CHANNEL_LABELS = {
    "app": "Telegram 客户端官方消息",
    "sms": "手机短信",
    "call": "语音电话",
    "flash_call": "闪信来电",
    "missed_call": "未接来电验证",
    "fragment_sms": "Fragment 虚拟号短信",
    "firebase_sms": "Firebase 推送短信",
    "email": "绑定邮箱",
    "unknown": "Telegram 官方渠道",
}

_ERROR_NAME_MAP: dict[str, tuple[int, str, str]] = {
    "PhoneCodeExpiredError": (400, "challenge_expired", "登录验证码或扫码凭证已过期，请重新发起登录"),
    "AuthTokenExpiredError": (400, "challenge_expired", "登录验证码或扫码凭证已过期，请重新发起登录"),
    "SessionExpiredError": (400, "challenge_expired", "登录验证码或扫码凭证已过期，请重新发起登录"),
    "PhoneCodeInvalidError": (401, "invalid_code", "Telegram 验证码无效或已失效，请核对后重试"),
    "PhoneCodeEmptyError": (401, "invalid_code", "Telegram 验证码无效或已失效，请核对后重试"),
    "CodeInvalidError": (401, "invalid_code", "Telegram 验证码无效或已失效，请核对后重试"),
    "CodeEmptyError": (401, "invalid_code", "Telegram 验证码无效或已失效，请核对后重试"),
    "PasswordHashInvalidError": (401, "invalid_password", "2FA 两步验证密码错误，请核对后重试"),
    "PasswordEmptyError": (401, "invalid_password", "2FA 两步验证密码错误，请核对后重试"),
    "SendCodeUnavailableError": (400, "next_channel_unavailable", "Telegram 暂不支持切换到下一个验证码下发渠道"),
    "PhoneNumberBannedError": (400, "identity_mismatch", "Telegram 拒绝该手机号登录（格式无效或受限）"),
    "PhoneNumberInvalidError": (400, "identity_mismatch", "Telegram 拒绝该手机号登录（格式无效或受限）"),
    "PhoneNumberAppSignupForbiddenError": (400, "identity_mismatch", "Telegram 拒绝该手机号登录（格式无效或受限）"),
}


class TelegramLoginError(Exception):
    """结构化 Telegram 登录异常，携带 HTTP 状态码与稳定错误码."""

    def __init__(
        self,
        http_status: int,
        error_code: str,
        detail: str,
        *,
        retry_after_seconds: int | None = None,
    ) -> None:
        super().__init__(detail)
        self.http_status = http_status
        self.error_code = error_code
        self.detail = detail
        self.retry_after_seconds = retry_after_seconds


@dataclass
class LoginChallenge:
    challenge_id: str
    flow: LoginFlow
    method: LoginMethod
    account_id: int
    initiator_fingerprint: str
    target_phone: str | None
    expected_user_id: int | None
    state: str
    created_at_monotonic: float
    expires_at_monotonic: float
    expires_at_epoch: float
    log_id: str = field(default_factory=lambda: secrets.token_hex(8))
    binding: LoginBinding | None = field(default=None, repr=False)
    _phone_code_hash: str | None = field(default=None, repr=False)
    delivery_channel: str | None = None
    code_length: int | None = None
    next_delivery_channel: str | None = None
    resend_available_monotonic: float | None = None
    resend_available_epoch: float | None = None
    qr_url: str | None = field(default=None, repr=False)
    _qr_login: Any = field(default=None, repr=False)
    _watch_task: asyncio.Task[None] | None = field(default=None, repr=False)
    _expiry_task: asyncio.Task[None] | None = field(default=None, repr=False)
    _temp_client: Any = field(default=None, repr=False)
    verify_attempts: int = 0
    password_attempts: int = 0
    error_code: str | None = None
    error_message: str | None = None
    retry_after_seconds: int | None = None
    consumed: bool = False
    terminal_at_monotonic: float | None = None


def build_initiator_fingerprint(
    *,
    flow: LoginFlow,
    auth_mode: str,
    api_key: str | None = None,
    two_fa_token: str | None = None,
    bootstrap_cred: str | None = None,
    client_host: str | None = None,
    user_agent: str | None = None,
    context_token: str = "",
) -> str:
    """绑定发起方授权会话与浏览器上下文，防跨浏览器/跨会话/跨入口复用挑战."""
    key_digest = hashlib.sha256((api_key or "").encode("utf-8")).hexdigest()
    token_digest = hashlib.sha256((two_fa_token or "").encode("utf-8")).hexdigest() if auth_mode == "2fa" else ""
    bootstrap_digest = (
        hashlib.sha256((bootstrap_cred or "").encode("utf-8")).hexdigest() if auth_mode == "bootstrap" else ""
    )
    ctx_digest = hashlib.sha256(context_token.encode("utf-8")).hexdigest() if context_token else ""
    raw = "|".join(
        [
            flow,
            auth_mode,
            key_digest,
            token_digest,
            bootstrap_digest,
            ctx_digest,
            (client_host or "").strip(),
            (user_agent or "").strip()[:200],
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _is_concrete_int(val: Any) -> TypeGuard[int]:
    return type(val) is int


def _extract_sent_code_metadata(result: Any, now_mono: float, now_epoch: float) -> dict[str, Any]:
    """解析 Telethon SentCode 元数据，兼容 SDK 变体与未识别投递类型."""
    res_type_name = type(result).__name__
    if res_type_name == "SentCodePaymentRequired":
        raise TelegramLoginError(400, "delivery_unsupported", "Telegram 要求付费短信通道，暂不支持此下发方式")
    if res_type_name == "SentCodeSuccess":
        raise TelegramLoginError(400, "delivery_unsupported", "Telegram 返回了非预期的即时登录响应")

    phone_code_hash = getattr(result, "phone_code_hash", None)
    if not isinstance(phone_code_hash, str) or not phone_code_hash.strip():
        raise TelegramLoginError(502, "network_failure", "Telegram 未返回有效的验证码会话凭据")

    sent_type = getattr(result, "type", None)
    sent_type_name = type(sent_type).__name__ if sent_type is not None else ""
    if sent_type_name in _UNSUPPORTED_SENT_CODE_TYPES:
        raise TelegramLoginError(400, "delivery_unsupported", _UNSUPPORTED_SENT_CODE_TYPES[sent_type_name])

    delivery_channel = _SENT_CODE_CHANNEL_MAP.get(sent_type_name, "unknown")
    raw_len = getattr(sent_type, "length", None) if sent_type is not None else None
    code_length = raw_len if _is_concrete_int(raw_len) and 1 <= raw_len <= 16 else None

    next_type = getattr(result, "next_type", None)
    next_type_name = type(next_type).__name__ if next_type is not None else ""
    next_channel: str | None = None
    if next_type is not None:
        if next_type_name in _NEXT_CODE_CHANNEL_MAP:
            next_channel = _NEXT_CODE_CHANNEL_MAP[next_type_name]
        elif next_type_name.startswith("CodeType"):
            next_channel = "other"
        elif isinstance(next_type, str) and next_type.strip():
            next_channel = next_type.strip().lower()

    raw_timeout = getattr(result, "timeout", None)
    wait_seconds = raw_timeout if _is_concrete_int(raw_timeout) and raw_timeout >= 0 else 0
    can_resend = next_channel is not None

    return {
        "phone_code_hash": phone_code_hash,
        "delivery_channel": delivery_channel,
        "code_length": code_length,
        "next_delivery_channel": next_channel,
        "can_resend": can_resend,
        "resend_wait_seconds": wait_seconds if can_resend else 0,
        "resend_available_monotonic": (now_mono + wait_seconds) if can_resend else None,
        "resend_available_epoch": (now_epoch + wait_seconds) if can_resend else None,
    }


def _translate_telegram_error(
    exc: BaseException, *, default_message: str = "Telegram 登录请求失败"
) -> TelegramLoginError:
    """将 Telethon / 网络异常归一为稳定错误码，绝不暴露原始异常中的敏感信息."""
    if isinstance(exc, TelegramLoginError):
        return exc

    err_name = type(exc).__name__
    if "Flood" in err_name and hasattr(exc, "seconds"):
        raw_sec = getattr(exc, "seconds", 60)
        wait_s = int(raw_sec) if isinstance(raw_sec, (int, float)) and raw_sec > 0 else 60
        return TelegramLoginError(
            429,
            "flood_wait",
            f"Telegram 请求过于频繁，请等待 {wait_s} 秒后重试",
            retry_after_seconds=wait_s,
        )

    mapped = _ERROR_NAME_MAP.get(err_name)
    if mapped is not None:
        return TelegramLoginError(mapped[0], mapped[1], mapped[2])

    if isinstance(exc, (TimeoutError, asyncio.TimeoutError, OSError, ConnectionError)):
        return TelegramLoginError(502, "network_failure", "连接 Telegram 服务器超时或网络中断，请稍后重试")

    return TelegramLoginError(502, "network_failure", f"{default_message} ({err_name})")


def _compute_qr_expiry(raw_expires: Any, now_epoch: float, now_mono: float) -> tuple[float, float]:
    ttl = QR_MAX_TTL_SECONDS
    if isinstance(raw_expires, datetime):
        exp_dt = raw_expires if raw_expires.tzinfo else raw_expires.replace(tzinfo=UTC)
        delta = exp_dt.timestamp() - now_epoch
        ttl = min(delta, QR_MAX_TTL_SECONDS) if delta > 0 else 1.0
    elif isinstance(raw_expires, (int, float)) and raw_expires > now_epoch:
        ttl = min(float(raw_expires) - now_epoch, QR_MAX_TTL_SECONDS)
    return now_mono + ttl, now_epoch + ttl


class TelegramLoginService:
    """共享的 Telegram OTP 与 QR 登录挑战编排器."""

    def __init__(self) -> None:
        self._challenges: dict[str, LoginChallenge] = {}
        self._active_by_account: dict[int, str] = {}
        self._latest_by_account: dict[int, str] = {}
        self._starting_accounts: set[int] = set()
        self._account_locks: dict[tuple[int, asyncio.AbstractEventLoop], asyncio.Lock] = {}
        self._first_bind_create_timestamps: list[float] = []
        self._first_bind_quota_lock = threading.Lock()

    def reset_for_tests(self) -> None:
        """测试隔离清理，取消挂起任务并清空内存挑战状态."""
        for ch in list(self._challenges.values()):
            for task in (ch._watch_task, ch._expiry_task):
                if task and not task.done():
                    task.cancel()
        self._challenges.clear()
        self._active_by_account.clear()
        self._latest_by_account.clear()
        self._starting_accounts.clear()
        self._account_locks.clear()
        with self._first_bind_quota_lock:
            self._first_bind_create_timestamps.clear()

    def _enforce_first_bind_create_quota(self) -> None:
        """首绑 OTP/QR 挑战创建的实例级滑动窗口配额保护."""
        limit = FIRST_BIND_CREATE_LIMIT_PER_MINUTE
        if limit <= 0:
            return
        with self._first_bind_quota_lock:
            now = time.monotonic()
            window_start = now - 60.0
            valid = [t for t in self._first_bind_create_timestamps if t > window_start]
            if len(valid) >= limit:
                oldest = valid[0]
                retry_after = max(1, int((oldest + 60.0) - now + 0.999))
                self._first_bind_create_timestamps = valid
                raise TelegramLoginError(
                    429,
                    "bootstrap_quota_exceeded",
                    "首绑登录请求过于频繁，已触发实例级配额保护，请稍后再试",
                    retry_after_seconds=retry_after,
                )
            valid.append(now)
            self._first_bind_create_timestamps = valid

    def _get_account_lock(self, account_id: int) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        key = (account_id, loop)
        lock = self._account_locks.get(key)
        if lock is None:
            stale_keys = [k for k in self._account_locks if k[0] == account_id and k[1].is_closed()]
            for sk in stale_keys:
                self._account_locks.pop(sk, None)
            lock = asyncio.Lock()
            self._account_locks[key] = lock
        return lock

    def _reap_terminal_challenges(self, now_mono: float) -> None:
        """Only reap terminal records here. Each live challenge owns its expiry timer."""
        for ch in list(self._challenges.values()):
            if (
                ch.state in TERMINAL_STATES
                and ch.terminal_at_monotonic
                and now_mono - ch.terminal_at_monotonic > TERMINAL_TOMBSTONE_TTL_SECONDS
            ):
                self._forget_challenge(ch)

        if len(self._challenges) >= MAX_ACTIVE_CHALLENGES:
            terminal = sorted(
                (c for c in self._challenges.values() if c.state in TERMINAL_STATES),
                key=lambda c: c.terminal_at_monotonic or c.created_at_monotonic,
            )
            overflow = len(self._challenges) - MAX_ACTIVE_CHALLENGES + 1
            for victim in terminal[:overflow]:
                self._forget_challenge(victim)

    def _forget_challenge(self, ch: LoginChallenge) -> None:
        self._challenges.pop(ch.challenge_id, None)
        if self._active_by_account.get(ch.account_id) == ch.challenge_id:
            self._active_by_account.pop(ch.account_id, None)
        if self._latest_by_account.get(ch.account_id) == ch.challenge_id:
            self._latest_by_account.pop(ch.account_id, None)

    def _register_challenge(self, ch: LoginChallenge) -> None:
        self._challenges[ch.challenge_id] = ch
        self._active_by_account[ch.account_id] = ch.challenge_id
        self._latest_by_account[ch.account_id] = ch.challenge_id

    @asynccontextmanager
    async def _reserve_start(self, account_id: int, *, supersede: bool = True) -> AsyncIterator[None]:
        async with self._get_account_lock(account_id):
            self._reap_terminal_challenges(time.monotonic())
            occupied = self._starting_accounts | {
                ch.account_id for ch in self._challenges.values() if ch.state not in TERMINAL_STATES
            }
            if len(occupied - {account_id}) >= MAX_ACTIVE_CHALLENGES:
                raise TelegramLoginError(429, "too_many_challenges", "当前活跃登录挑战过多，请稍后重试")
            # Check and reserve without yielding to another account's start.
            self._starting_accounts.add(account_id)
            try:
                if supersede:
                    await self._supersede_account_challenge(account_id)
                yield
            finally:
                self._starting_accounts.discard(account_id)

    def _arm_expiry_timer(self, ch: LoginChallenge) -> None:
        current_task: asyncio.Task[Any] | None = None
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None
        old_timer = ch._expiry_task
        if old_timer is not None and old_timer is not current_task and not old_timer.done():
            old_timer.cancel()
        ch._expiry_task = asyncio.create_task(self._expiry_watchdog(ch))

    async def _expiry_watchdog(self, ch: LoginChallenge) -> None:
        """在后台按挑战 TTL 自动到期并关闭/断开放弃的 OTP/2FA/未领取 QR 临时连接."""
        try:
            while True:
                if ch.state in TERMINAL_STATES:
                    return
                delay = ch.expires_at_monotonic - time.monotonic()
                if delay <= 0:
                    break
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return

        try:
            async with self._get_account_lock(ch.account_id):
                if ch.state not in TERMINAL_STATES and time.monotonic() >= ch.expires_at_monotonic:
                    if self._active_by_account.get(ch.account_id) == ch.challenge_id:
                        self._active_by_account.pop(ch.account_id, None)
                    await self._expire_challenge(ch, "登录挑战已过期，已自动清理临时会话")
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.debug(
                "后台到期清理挑战异常 (account_id=%d, err=%s)",
                ch.account_id,
                type(exc).__name__,
            )

    @staticmethod
    async def _cancel_task_if_external(
        task: asyncio.Task[Any] | None, current_task: asyncio.Task[Any] | None, label: str
    ) -> None:
        if task is not None and task is not current_task and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception) as exc:
                logger.debug("清理后台%s结束 (%s)", label, type(exc).__name__)

    async def _cleanup_challenge_resources(
        self,
        ch: LoginChallenge,
        *,
        logout_temp: bool = False,
        cancel_watcher: bool = True,
    ) -> None:
        current_task: asyncio.Task[Any] | None = None
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None

        watch_task = ch._watch_task
        ch._watch_task = None
        expiry_task = ch._expiry_task
        ch._expiry_task = None

        if cancel_watcher:
            await self._cancel_task_if_external(watch_task, current_task, "QR 监听")
        await self._cancel_task_if_external(expiry_task, current_task, "过期清理")

        temp_client = ch._temp_client
        ch._temp_client = None
        ch._qr_login = None
        ch._phone_code_hash = None

        if temp_client is not None:
            await self._dispose_temp_client(temp_client, logout_temp=logout_temp)

    @staticmethod
    async def _dispose_temp_client(temp_client: Any, *, logout_temp: bool) -> None:
        if logout_temp:
            try:
                if await temp_client.is_user_authorized():
                    await asyncio.wait_for(temp_client.log_out(), timeout=5.0)
            except Exception as lo_err:
                logger.debug("清理临时登录客户端登出异常 (%s)", type(lo_err).__name__)
        try:
            if temp_client.is_connected():
                await asyncio.wait_for(temp_client.disconnect(), timeout=5.0)
        except Exception as disc_err:
            logger.debug("清理临时登录客户端断开异常 (%s)", type(disc_err).__name__)
        sess = getattr(temp_client, "session", None)
        close_fn = getattr(sess, "close", None)
        if callable(close_fn):
            try:
                close_fn()
            except Exception as close_err:
                logger.debug("关闭临时 session 异常 (%s)", type(close_err).__name__)

    async def _supersede_account_challenge(self, account_id: int, reason_state: str = "cancelled") -> None:
        old_cid = self._active_by_account.pop(account_id, None)
        if not old_cid:
            return
        old = self._challenges.get(old_cid)
        if old is None:
            return
        if old.state not in TERMINAL_STATES:
            old.state = reason_state
            old.error_code = "challenge_stale" if reason_state == "cancelled" else "challenge_expired"
            old.error_message = "已有新的登录挑战启动，旧挑战已失效"
            old.terminal_at_monotonic = time.monotonic()
        await self._cleanup_challenge_resources(old, logout_temp=False, cancel_watcher=True)

    async def shutdown(self) -> None:
        """服务关闭时取消全部挂起挑战并清理临时客户端与后台任务."""
        for ch in list(self._challenges.values()):
            if ch.state not in TERMINAL_STATES:
                ch.state = "cancelled"
                ch.error_code = "challenge_cancelled"
                ch.error_message = "服务关闭，登录挑战已取消"
                ch.terminal_at_monotonic = time.monotonic()
            await self._cleanup_challenge_resources(ch, logout_temp=False, cancel_watcher=True)
        self._challenges.clear()
        self._active_by_account.clear()
        self._latest_by_account.clear()

    async def _resolve_target_account(self, flow: LoginFlow, account_id: int | None) -> TelegramAccountClient:
        if flow == "native":
            acc = client_manager.get_default_account_strict()
            if acc is None:
                raise TelegramLoginError(
                    400,
                    "target_unavailable",
                    "未找到可用的默认 Userbot 账号（需启用且设为默认），已拒绝回退至其他账号",
                )
            return acc

        if account_id is None or account_id <= 0:
            raise TelegramLoginError(400, "target_unavailable", "缺少目标账号 ID")

        acc = client_manager.get_account(account_id)
        if not acc:
            db_acc = AccountRepository.get_by_id(account_id)
            if not db_acc:
                raise TelegramLoginError(404, "target_unavailable", "账号不存在")
            if not db_acc.get("is_active", 1):
                raise TelegramLoginError(400, "target_unavailable", "账号已禁用，请先启用后再进行登录验证")
            await client_manager.initialize()
            acc = client_manager.get_account(account_id)
            if not acc:
                raise TelegramLoginError(500, "target_unavailable", "账号客户端加载失败")
        if not acc.is_active:
            raise TelegramLoginError(400, "target_unavailable", "账号已禁用，请先启用后再进行登录验证")
        return acc

    @staticmethod
    def _normalize_optional_phone(raw: str | None, *, error_code: str, prefix: str) -> str | None:
        cleaned = (raw or "").strip()
        if not cleaned:
            return None
        try:
            return normalize_phone_number(cleaned)
        except ValueError as exc:
            raise TelegramLoginError(400, error_code, f"{prefix}{exc}") from exc

    @classmethod
    def _resolve_native_phone_and_pin(
        cls,
        *,
        auth_mode: str,
        req_norm: str | None,
        rec_norm: str | None,
        pinned_user_id: int | None,
        require_phone: bool,
    ) -> tuple[str | None, int | None]:
        mismatch_status = 403 if auth_mode == "bootstrap" else 400
        # 已绑定 user_id 的恢复/重验：仅使用持久化记录目标，不因配置变动降级，不回显服务器号码
        if pinned_user_id is not None:
            if req_norm and rec_norm and canonical_phone_number(req_norm) != canonical_phone_number(rec_norm):
                raise TelegramLoginError(
                    mismatch_status,
                    "identity_mismatch",
                    "输入的手机号与已绑定的默认账号目标不一致，禁止切换账号",
                )
            target = rec_norm or req_norm
            if require_phone and target is None:
                raise TelegramLoginError(400, "target_unavailable", "缺少已绑定的手机号，无法发起验证码登录")
            return target, pinned_user_id

        # 首绑 (pinned_user_id is None):
        # 1. 若配置 TG_DEFAULT_PHONE，规范化并锁定该配置号码 (覆盖未验证的旧 SQLite 预填值)；格式无效则拒绝并提示部署者
        raw_default = (getattr(settings, "DEFAULT_PHONE", "") or "").strip()
        locked_phone: str | None = None
        if raw_default:
            locked_phone = cls._normalize_optional_phone(
                raw_default,
                error_code="invalid_config",
                prefix="部署配置 TG_DEFAULT_PHONE 格式无效，请修正后重试: ",
            )

        # 2. OTP 与 QR 无论有无配置均要求主动输入号码
        if not req_norm:
            raise TelegramLoginError(
                400,
                "target_unavailable",
                "首次绑定请主动输入带国际区号的手机号",
            )

        # 3. 若配置存在且号码不符，在创建临时客户端前拒绝 (不回显服务器号码)
        if locked_phone is not None:
            if canonical_phone_number(req_norm) != canonical_phone_number(locked_phone):
                raise TelegramLoginError(
                    mismatch_status,
                    "phone_mismatch",
                    "输入的手机号与部署配置锁定的目标号码不一致，请核对后重试",
                )
            return locked_phone, None

        # 4. TG_DEFAULT_PHONE 留空时，仅已认证部署者 (或正常业务态已鉴权操作者) 可指定任意格式有效的手机号
        return req_norm, None

    @staticmethod
    def _resolve_account_phone_and_pin(
        *,
        req_norm: str | None,
        rec_norm: str | None,
        pinned_user_id: int | None,
        require_phone: bool,
    ) -> tuple[str | None, int | None]:
        if rec_norm:
            if req_norm and canonical_phone_number(req_norm) != canonical_phone_number(rec_norm):
                raise TelegramLoginError(400, "identity_mismatch", "请求手机号与该账号已绑定的手机号不一致")
            return rec_norm, pinned_user_id
        if require_phone:
            if not req_norm:
                raise TelegramLoginError(400, "target_unavailable", "缺少手机号，请在请求体或账号配置中提供手机号")
            return req_norm, pinned_user_id
        if pinned_user_id is None:
            raise TelegramLoginError(
                400, "target_unavailable", "该账号未绑定手机号或 Telegram 用户 ID，无法执行扫码身份校验"
            )
        return None, pinned_user_id

    @classmethod
    def _resolve_target_phone_and_pin(
        cls,
        *,
        flow: LoginFlow,
        auth_mode: str,
        acc: TelegramAccountClient,
        req_phone: str | None,
        require_phone: bool,
    ) -> tuple[str | None, int | None]:
        """在发起任何 Telegram RPC 或 QR 创建前执行目标手机号与绑定校验 (不读从账号无关配置)."""
        req_norm = cls._normalize_optional_phone(req_phone, error_code="identity_mismatch", prefix="")
        pinned_user_id = acc.user_id if _is_concrete_int(acc.user_id) and acc.user_id > 0 else None
        rec_norm = None
        if flow == "account" or pinned_user_id is not None:
            rec_norm = cls._normalize_optional_phone(
                acc.phone,
                error_code="identity_mismatch",
                prefix="账号记录的手机号格式无效: ",
            )

        if flow == "native":
            return cls._resolve_native_phone_and_pin(
                auth_mode=auth_mode,
                req_norm=req_norm,
                rec_norm=rec_norm,
                pinned_user_id=pinned_user_id,
                require_phone=require_phone,
            )
        return cls._resolve_account_phone_and_pin(
            req_norm=req_norm,
            rec_norm=rec_norm,
            pinned_user_id=pinned_user_id,
            require_phone=require_phone,
        )

    async def _prepare_challenge_client(self, acc: TelegramAccountClient) -> Any:
        """统一创建隔离的临时客户端，绝不复用已在线的官方会话句柄."""
        temp_client = client_manager.create_isolated_login_client(acc)
        try:
            if not temp_client.is_connected():
                await temp_client.connect()
        except BaseException:
            await asyncio.shield(self._dispose_temp_client(temp_client, logout_temp=False))
            raise
        return temp_client

    @staticmethod
    async def _rpc_send_initial_code(client: Any, phone: str) -> Any:
        cache = getattr(client, "_phone_code_hash", None)
        if isinstance(cache, dict):
            cache.pop(phone, None)
            parsed = utils.parse_phone(phone)
            if parsed:
                cache.pop(parsed, None)

        if isinstance(client, TelegramClient):
            parsed = utils.parse_phone(phone) or phone
            for attempt in range(3):
                try:
                    return await client(
                        functions.auth.SendCodeRequest(
                            parsed,
                            client.api_id,
                            client.api_hash,
                            types.CodeSettings(),
                        )
                    )
                except tl_errors.AuthRestartError:
                    if attempt >= 2:
                        raise
            raise RuntimeError("SendCodeRequest retries exhausted")

        return await client.send_code_request(phone)

    @staticmethod
    async def _rpc_resend_code(client: Any, phone: str, phone_code_hash: str) -> Any:
        if hasattr(client, "resend_code_request") and not isinstance(client, TelegramClient):
            return await client.resend_code_request(phone, phone_code_hash)
        parsed = utils.parse_phone(phone) or phone
        return await client(functions.auth.ResendCodeRequest(parsed, phone_code_hash))

    def _get_live_challenge(
        self,
        challenge_id: str,
        *,
        flow: LoginFlow,
        account_id: int | None,
        method: LoginMethod | None,
        initiator_fingerprint: str,
        allow_terminal_read: bool = False,
    ) -> LoginChallenge:
        ch = self._challenges.get(challenge_id)
        if ch is None:
            raise TelegramLoginError(400, "challenge_not_found", "登录挑战不存在或已失效，请重新发起登录")
        if ch.flow != flow:
            raise TelegramLoginError(403, "challenge_invalid", "登录挑战与当前入口不匹配")
        if account_id is not None and ch.account_id != account_id:
            raise TelegramLoginError(400, "challenge_invalid", "登录挑战与目标账号不匹配")
        if method is not None and ch.method != method:
            raise TelegramLoginError(400, "challenge_invalid", "登录挑战与当前验证方式不匹配")
        if ch.initiator_fingerprint != initiator_fingerprint:
            raise TelegramLoginError(403, "challenge_invalid", "登录挑战与当前授权浏览器/会话上下文不匹配")
        if ch.consumed:
            raise TelegramLoginError(400, "challenge_stale", "该登录挑战已完成，禁止重复使用")

        if not allow_terminal_read:
            self._assert_non_terminal_challenge(ch)
        return ch

    def _assert_non_terminal_challenge(self, ch: LoginChallenge) -> None:
        if ch.state == "cancelled":
            raise TelegramLoginError(400, "challenge_cancelled", "该登录挑战已取消，请重新发起")
        if ch.state == "expired":
            raise TelegramLoginError(400, "challenge_expired", "该登录挑战已过期，请重新发起")
        if ch.state == "failed":
            raise TelegramLoginError(
                400,
                ch.error_code or "challenge_stale",
                ch.error_message or "该登录挑战已失败，请重新发起",
                retry_after_seconds=ch.retry_after_seconds,
            )
        if ch.state == "authenticated":
            raise TelegramLoginError(400, "challenge_stale", "该登录挑战已完成，禁止重复提交")
        if self._active_by_account.get(ch.account_id) != ch.challenge_id:
            raise TelegramLoginError(400, "challenge_stale", "该登录挑战已被新操作取代，请使用最新挑战")

    async def _assert_target_unchanged(self, ch: LoginChallenge, acc: TelegramAccountClient) -> None:
        latest_acc = await self._resolve_target_account(ch.flow, ch.account_id)
        if latest_acc is not acc or latest_acc.account_id != ch.account_id:
            raise TelegramLoginError(400, "challenge_stale", "登录期间目标账号已变更，请重新发起")
        binding = self._read_binding(ch.flow, ch.account_id)
        if ch.binding is not None and binding != ch.binding:
            raise TelegramLoginError(400, "challenge_stale", "登录期间账号绑定已变更，请重新发起")
        if ch.expected_user_id != latest_acc.user_id:
            raise TelegramLoginError(400, "challenge_stale", "登录期间账号绑定身份已变更，请重新发起")
        if ch.expected_user_id is not None and latest_acc.phone and ch.target_phone:
            try:
                if canonical_phone_number(latest_acc.phone) != canonical_phone_number(ch.target_phone):
                    raise TelegramLoginError(400, "challenge_stale", "登录期间账号绑定手机号已变更，已拒绝本次授权")
            except ValueError as exc:
                raise TelegramLoginError(400, "identity_mismatch", str(exc)) from exc
        if ch.expected_user_id is None and ch.flow == "native" and ch.target_phone:
            raw_default = (getattr(settings, "DEFAULT_PHONE", "") or "").strip()
            if raw_default:
                norm_default = self._normalize_optional_phone(
                    raw_default,
                    error_code="invalid_config",
                    prefix="部署配置 TG_DEFAULT_PHONE 格式无效，请修正后重试: ",
                )
                if norm_default and canonical_phone_number(norm_default) != canonical_phone_number(ch.target_phone):
                    raise TelegramLoginError(400, "challenge_stale", "登录期间默认手机号配置已变更，请重新发起")

    @staticmethod
    def _read_binding(flow: LoginFlow, account_id: int) -> LoginBinding:
        db_row = AccountRepository.get_by_id(account_id)
        if db_row is None or not bool(db_row.get("is_active", 1)):
            raise TelegramLoginError(400, "target_unavailable", "目标账号已删除或已禁用，拒绝完成登录")
        if flow == "native" and not bool(db_row.get("is_default", 0)):
            raise TelegramLoginError(400, "challenge_stale", "登录期间默认主账号已变更，请重新发起")
        return LoginBinding.from_record(db_row)

    async def _persist_verified_session(
        self,
        ch: LoginChallenge,
        acc: TelegramAccountClient,
        client: Any,
        me: Any,
    ) -> None:
        try:
            await acc.apply_verified_login_session(
                client,
                me,
                target_phone=ch.target_phone,
                require_default=(ch.flow == "native"),
                expected_binding=ch.binding,
            )
        except ValueError as exc:
            ch.state = "failed"
            ch.error_code = "identity_mismatch"
            ch.error_message = str(exc)
            ch.terminal_at_monotonic = time.monotonic()
            await self._cleanup_challenge_resources(ch, logout_temp=False, cancel_watcher=False)
            raise TelegramLoginError(409, "identity_mismatch", str(exc)) from exc
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "持久化已校验 Telegram 会话失败 (flow=%s, account_id=%d, err=%s)",
                ch.flow,
                acc.account_id,
                type(exc).__name__,
            )
            ch.state = "failed"
            ch.error_code = "persistence_failed"
            ch.error_message = "账号会话持久化失败，未激活登录状态"
            ch.terminal_at_monotonic = time.monotonic()
            await self._cleanup_challenge_resources(ch, logout_temp=False, cancel_watcher=False)
            raise TelegramLoginError(500, "persistence_failed", "账号会话持久化失败，请重试") from exc

    async def _complete_verified_login(
        self,
        ch: LoginChallenge,
        acc: TelegramAccountClient,
        client: Any,
        *,
        issue_token_fn: Callable[[], str | None] | None,
    ) -> dict[str, Any]:
        """完成登录前的最终目标一致性检查、get_me 身份核验、回滚安全持久化与调度器恢复."""
        await self._assert_target_unchanged(ch, acc)

        try:
            if not await client.is_user_authorized():
                raise TelegramLoginError(401, "challenge_stale", "Telegram 会话授权状态已失效，请重新发起")
            me = await client.get_me()
        except TelegramLoginError:
            raise
        except Exception as exc:
            logger.warning(
                "完成登录时获取 Telegram 身份失败 (flow=%s, account_id=%d, err=%s)",
                ch.flow,
                acc.account_id,
                type(exc).__name__,
            )
            raise TelegramLoginError(502, "network_failure", "未能获取 Telegram 账号身份信息，请稍后重试") from exc

        ok, err_code, reason = TelegramAccountClient.validate_remote_identity(
            me,
            expected_user_id=ch.expected_user_id,
            expected_phone=ch.target_phone or acc.phone,
            detailed_phone_errors=ch.method == "qr" and ch.flow == "native" and ch.expected_user_id is None,
        )
        if not ok:
            logger.warning(
                "Telegram 登录身份校验未通过 (flow=%s, account_id=%d, method=%s, event_id=%s, reason=%s)",
                ch.flow,
                acc.account_id,
                ch.method,
                ch.log_id,
                err_code,
            )
            await self._cleanup_challenge_resources(
                ch,
                logout_temp=(acc.auth_state != "AUTHENTICATED"),
                cancel_watcher=False,
            )
            if self._active_by_account.get(ch.account_id) == ch.challenge_id:
                self._active_by_account.pop(ch.account_id, None)
            ch.state = "failed"
            ch.error_code = err_code
            ch.error_message = reason
            ch.terminal_at_monotonic = time.monotonic()
            raise TelegramLoginError(409, err_code, reason)

        await self._persist_verified_session(ch, acc, client, me)

        # 交接成功：正式客户端已接管连接与落盘 session，置空临时引用再取消后台清理定时器
        ch._temp_client = None
        ch._qr_login = None
        ch._phone_code_hash = None
        if self._active_by_account.get(ch.account_id) == ch.challenge_id:
            self._active_by_account.pop(ch.account_id, None)
        await self._cleanup_challenge_resources(ch, logout_temp=False, cancel_watcher=False)

        ch.state = "authenticated"
        ch.consumed = True
        ch.terminal_at_monotonic = time.monotonic()
        if ch.method == "qr":
            logger.info("已完成 QR 会话交接 (flow=%s, account_id=%d, event_id=%s)", ch.flow, ch.account_id, ch.log_id)

        if client_manager.is_default_account_admitted():
            client_manager.schedule_startup_recovery()

        minted_token = issue_token_fn() if (ch.flow == "native" and issue_token_fn is not None) else None
        return {
            "status": "ok",
            "state": "authenticated",
            "authenticated": True,
            "challenge_id": ch.challenge_id,
            "account_id": acc.account_id,
            "is_premium": acc.is_premium,
            "dc_id": acc.dc_id,
            "user_id": acc.user_id,
            "username": acc.username,
            "token": minted_token,
            "error_code": None,
            "retry_after_seconds": None,
            "message": "Telegram 账号鉴权成功" if ch.flow == "native" else "账号授权成功！",
        }

    async def start_otp(
        self,
        *,
        flow: LoginFlow,
        account_id: int | None,
        phone: str | None,
        auth_mode: str,
        initiator_fingerprint: str,
    ) -> dict[str, Any]:
        acc = await self._resolve_target_account(flow, account_id)
        target_phone, pinned_user_id = self._resolve_target_phone_and_pin(
            flow=flow,
            auth_mode=auth_mode,
            acc=acc,
            req_phone=phone,
            require_phone=True,
        )
        if target_phone is None:
            raise TelegramLoginError(400, "target_unavailable", "缺少手机号，无法发起验证码登录")

        binding = self._read_binding(flow, acc.account_id)
        if flow == "native" and pinned_user_id is None:
            self._enforce_first_bind_create_quota()
        async with self._reserve_start(acc.account_id):
            if self._read_binding(flow, acc.account_id) != binding:
                raise TelegramLoginError(400, "challenge_stale", "账号绑定已变更，请重新发起")

            client = None
            try:
                client = await self._prepare_challenge_client(acc)
                result = await self._rpc_send_initial_code(client, target_phone)
                now_mono = time.monotonic()
                now_epoch = time.time()
                meta = _extract_sent_code_metadata(result, now_mono, now_epoch)
            except (Exception, asyncio.CancelledError) as exc:
                if client is not None:
                    await asyncio.shield(self._dispose_temp_client(client, logout_temp=False))
                if isinstance(exc, asyncio.CancelledError):
                    raise
                raise _translate_telegram_error(exc, default_message="下发官方验证码失败") from exc

            challenge_id = secrets.token_urlsafe(24)
            expires_mono = now_mono + OTP_CHALLENGE_TTL_SECONDS
            expires_epoch = now_epoch + OTP_CHALLENGE_TTL_SECONDS
            ch = LoginChallenge(
                challenge_id=challenge_id,
                flow=flow,
                method="otp",
                account_id=acc.account_id,
                initiator_fingerprint=initiator_fingerprint,
                target_phone=target_phone,
                expected_user_id=pinned_user_id,
                state="code_sent",
                created_at_monotonic=now_mono,
                expires_at_monotonic=expires_mono,
                expires_at_epoch=expires_epoch,
                binding=binding,
                _phone_code_hash=meta["phone_code_hash"],
                delivery_channel=meta["delivery_channel"],
                code_length=meta["code_length"],
                next_delivery_channel=meta["next_delivery_channel"],
                resend_available_monotonic=meta["resend_available_monotonic"],
                resend_available_epoch=meta["resend_available_epoch"],
                _temp_client=client,
            )
            self._register_challenge(ch)
            self._arm_expiry_timer(ch)

            logger.info(
                "已发起 OTP 登录挑战 (flow=%s, account_id=%d, channel=%s, next_channel=%s)",
                flow,
                acc.account_id,
                meta["delivery_channel"],
                meta["next_delivery_channel"],
            )
            channel_label = _CHANNEL_LABELS.get(meta["delivery_channel"], "Telegram 官方渠道")
            return {
                "status": "ok",
                "account_id": acc.account_id,
                "phone": target_phone,
                "challenge_id": challenge_id,
                "method": "otp",
                "state": "code_sent",
                "delivery_channel": meta["delivery_channel"],
                "code_length": meta["code_length"],
                "next_delivery_channel": meta["next_delivery_channel"],
                "can_resend": meta["can_resend"],
                "resend_available_at": meta["resend_available_epoch"],
                "resend_wait_seconds": meta["resend_wait_seconds"],
                "expires_at": expires_epoch,
                "message": f"官方验证码已通过{channel_label}下发",
            }

    async def resend_otp(
        self,
        *,
        flow: LoginFlow,
        account_id: int | None,
        challenge_id: str,
        initiator_fingerprint: str,
    ) -> dict[str, Any]:
        ch_preview = self._get_live_challenge(
            challenge_id,
            flow=flow,
            account_id=account_id,
            method="otp",
            initiator_fingerprint=initiator_fingerprint,
        )

        async with self._get_account_lock(ch_preview.account_id):
            ch = self._get_live_challenge(
                challenge_id,
                flow=flow,
                account_id=account_id,
                method="otp",
                initiator_fingerprint=initiator_fingerprint,
            )
            now_mono = time.monotonic()
            if now_mono > ch.expires_at_monotonic:
                await self._expire_challenge(ch, "登录验证码挑战已过期，请重新获取验证码")
                raise TelegramLoginError(400, "challenge_expired", "登录验证码挑战已过期，请重新获取验证码")

            if ch.state != "code_sent":
                raise TelegramLoginError(400, "challenge_invalid", "当前状态不支持重发验证码")
            if ch.next_delivery_channel is None:
                raise TelegramLoginError(
                    400, "next_channel_unavailable", "Telegram 未提供备用下发渠道，当前无法重发验证码"
                )
            if ch.resend_available_monotonic is not None and now_mono < ch.resend_available_monotonic:
                remaining = max(1, int(ch.resend_available_monotonic - now_mono + 0.999))
                raise TelegramLoginError(
                    429,
                    "resend_cooldown",
                    f"请等待 {remaining} 秒后再重发验证码",
                    retry_after_seconds=remaining,
                )

            client = ch._temp_client
            if client is None or not ch._phone_code_hash or not ch.target_phone:
                raise TelegramLoginError(400, "challenge_stale", "登录会话句柄不可用，请重新发起")

            try:
                if not client.is_connected():
                    await client.connect()
                result = await self._rpc_resend_code(client, ch.target_phone, ch._phone_code_hash)
                now_mono = time.monotonic()
                now_epoch = time.time()
                meta = _extract_sent_code_metadata(result, now_mono, now_epoch)
            except Exception as exc:
                translated = _translate_telegram_error(exc, default_message="重发验证码失败")
                if translated.error_code == "challenge_expired":
                    await self._expire_challenge(ch, translated.detail)
                elif translated.error_code == "delivery_unsupported":
                    # Telegram accepted a resend we cannot continue. The previous hash is obsolete.
                    ch.state = "failed"
                    ch.error_code = translated.error_code
                    ch.error_message = translated.detail
                    ch.terminal_at_monotonic = time.monotonic()
                    self._active_by_account.pop(ch.account_id, None)
                    await self._cleanup_challenge_resources(ch)
                raise translated from exc

            ch._phone_code_hash = meta["phone_code_hash"]
            ch.delivery_channel = meta["delivery_channel"]
            ch.code_length = meta["code_length"]
            ch.next_delivery_channel = meta["next_delivery_channel"]
            ch.resend_available_monotonic = meta["resend_available_monotonic"]
            ch.resend_available_epoch = meta["resend_available_epoch"]

            logger.info(
                "已重发 OTP 验证码 (flow=%s, account_id=%d, channel=%s, next_channel=%s)",
                flow,
                ch.account_id,
                meta["delivery_channel"],
                meta["next_delivery_channel"],
            )
            channel_label = _CHANNEL_LABELS.get(meta["delivery_channel"], "Telegram 官方渠道")
            return {
                "status": "ok",
                "account_id": ch.account_id,
                "phone": ch.target_phone,
                "challenge_id": ch.challenge_id,
                "method": "otp",
                "state": "code_sent",
                "delivery_channel": meta["delivery_channel"],
                "code_length": meta["code_length"],
                "next_delivery_channel": meta["next_delivery_channel"],
                "can_resend": meta["can_resend"],
                "resend_available_at": meta["resend_available_epoch"],
                "resend_wait_seconds": meta["resend_wait_seconds"],
                "expires_at": ch.expires_at_epoch,
                "message": f"验证码已通过{channel_label}重新下发",
            }

    async def _expire_challenge(self, ch: LoginChallenge, message: str) -> None:
        ch.state = "expired"
        ch.error_code = "challenge_expired"
        ch.error_message = message
        ch.terminal_at_monotonic = time.monotonic()
        if self._active_by_account.get(ch.account_id) == ch.challenge_id:
            self._active_by_account.pop(ch.account_id, None)
        if ch.method == "qr":
            logger.info(
                "QR 扫码登录挑战已过期 (flow=%s, account_id=%d, event_id=%s)", ch.flow, ch.account_id, ch.log_id
            )
        await self._cleanup_challenge_resources(ch)

    async def _fail_on_exhausted_attempts(self, ch: LoginChallenge, message: str) -> None:
        ch.state = "failed"
        ch.error_code = "too_many_attempts"
        ch.error_message = message
        ch.terminal_at_monotonic = time.monotonic()
        if self._active_by_account.get(ch.account_id) == ch.challenge_id:
            self._active_by_account.pop(ch.account_id, None)
        await self._cleanup_challenge_resources(ch, logout_temp=False, cancel_watcher=True)
        raise TelegramLoginError(429, "too_many_attempts", message)

    @staticmethod
    def _password_required_payload(ch: LoginChallenge, account_id: int, message: str) -> dict[str, Any]:
        return {
            "status": "password_required",
            "state": "password_required",
            "authenticated": False,
            "challenge_id": ch.challenge_id,
            "account_id": account_id,
            "is_premium": False,
            "dc_id": 0,
            "user_id": None,
            "username": None,
            "token": None,
            "error_code": "password_required",
            "retry_after_seconds": None,
            "message": message,
        }

    async def _execute_otp_sign_in(
        self,
        ch: LoginChallenge,
        acc: TelegramAccountClient,
        client: Any,
        clean_code: str,
        clean_password: str | None,
    ) -> dict[str, Any] | None:
        """执行 OTP 或 2FA 密码 sign_in；若需补充 2FA 密码则返回中间状态响应，否则返回 None."""
        if ch.state == "password_required":
            if not clean_password:
                return self._password_required_payload(
                    ch, acc.account_id, "该账号启用了两步验证 (2FA)，请输入云端密码完成授权"
                )
            await client.sign_in(password=clean_password)
            return None

        if not clean_code:
            raise TelegramLoginError(400, "invalid_code", "请输入 Telegram 官方验证码")
        if not ch._phone_code_hash or not ch.target_phone:
            raise TelegramLoginError(400, "challenge_stale", "登录挑战凭据缺失，请重新获取验证码")

        try:
            await client.sign_in(phone=ch.target_phone, code=clean_code, phone_code_hash=ch._phone_code_hash)
            return None
        except Exception as exc:
            if "SessionPasswordNeededError" not in type(exc).__name__:
                raise
            ch.state = "password_required"
            ch.expires_at_monotonic = min(ch.expires_at_monotonic, time.monotonic() + PASSWORD_CONTINUATION_TTL_SECONDS)
            ch.expires_at_epoch = time.time() + max(0.0, ch.expires_at_monotonic - time.monotonic())
            self._arm_expiry_timer(ch)
            if clean_password:
                await client.sign_in(password=clean_password)
                return None
            return self._password_required_payload(
                ch, acc.account_id, "该账号启用了两步验证 (2FA)，请输入密码后继续完成授权"
            )

    async def verify_otp(
        self,
        *,
        flow: LoginFlow,
        account_id: int | None,
        challenge_id: str,
        code: str | None,
        password: str | None,
        initiator_fingerprint: str,
        issue_token_fn: Callable[[], str | None] | None = None,
    ) -> dict[str, Any]:
        ch_preview = self._get_live_challenge(
            challenge_id,
            flow=flow,
            account_id=account_id,
            method="otp",
            initiator_fingerprint=initiator_fingerprint,
        )

        async with self._get_account_lock(ch_preview.account_id):
            ch = self._get_live_challenge(
                challenge_id,
                flow=flow,
                account_id=account_id,
                method="otp",
                initiator_fingerprint=initiator_fingerprint,
            )
            if time.monotonic() > ch.expires_at_monotonic:
                await self._expire_challenge(ch, "登录验证码挑战已过期，请重新获取验证码")
                raise TelegramLoginError(400, "challenge_expired", "登录验证码挑战已过期，请重新获取验证码")

            acc = await self._resolve_target_account(flow, ch.account_id)
            client = ch._temp_client
            if client is None:
                raise TelegramLoginError(400, "challenge_stale", "登录客户端已释放，请重新发起")
            clean_password = password if (password is not None and password.strip()) else None
            clean_code = str(code).strip().replace(" ", "").replace("-", "") if code else ""

            try:
                if not client.is_connected():
                    await client.connect()
                pending_resp = await self._execute_otp_sign_in(ch, acc, client, clean_code, clean_password)
                if pending_resp is not None:
                    return pending_resp
            except Exception as exc:
                translated = _translate_telegram_error(exc, default_message="验证码或 2FA 密码校验失败")
                if translated.error_code == "challenge_expired":
                    await self._expire_challenge(ch, translated.detail)
                elif translated.error_code == "invalid_code":
                    ch.verify_attempts += 1
                    if ch.verify_attempts >= MAX_VERIFY_ATTEMPTS_PER_CHALLENGE:
                        await self._fail_on_exhausted_attempts(ch, "验证码错误次数过多，请重新获取验证码")
                elif translated.error_code == "invalid_password":
                    ch.password_attempts += 1
                    if ch.password_attempts >= MAX_PASSWORD_ATTEMPTS_PER_CHALLENGE:
                        await self._fail_on_exhausted_attempts(ch, "2FA 密码错误次数过多，请重新发起登录")
                raise translated from exc

            if not await client.is_user_authorized():
                ch.verify_attempts += 1
                if ch.verify_attempts >= MAX_VERIFY_ATTEMPTS_PER_CHALLENGE:
                    await self._fail_on_exhausted_attempts(ch, "验证码错误次数过多，请重新获取验证码")
                raise TelegramLoginError(401, "invalid_code", "授权失败，验证码无效或已失效")

            return await self._complete_verified_login(ch, acc, client, issue_token_fn=issue_token_fn)

    async def submit_password(
        self,
        *,
        flow: LoginFlow,
        account_id: int | None,
        method: LoginMethod | None,
        challenge_id: str,
        password: str,
        initiator_fingerprint: str,
        issue_token_fn: Callable[[], str | None] | None = None,
    ) -> dict[str, Any]:
        ch_preview = self._get_live_challenge(
            challenge_id,
            flow=flow,
            account_id=account_id,
            method=method,
            initiator_fingerprint=initiator_fingerprint,
        )

        async with self._get_account_lock(ch_preview.account_id):
            ch = self._get_live_challenge(
                challenge_id,
                flow=flow,
                account_id=account_id,
                method=method,
                initiator_fingerprint=initiator_fingerprint,
            )
            if time.monotonic() > ch.expires_at_monotonic:
                await self._expire_challenge(ch, "登录挑战已过期，请重新发起")
                raise TelegramLoginError(400, "challenge_expired", "登录挑战已过期，请重新发起")
            if ch.state != "password_required":
                raise TelegramLoginError(400, "challenge_invalid", "当前登录挑战无需输入两步验证密码")
            if not password or not password.strip():
                raise TelegramLoginError(401, "invalid_password", "请输入有效的两步验证密码")

            acc = await self._resolve_target_account(flow, ch.account_id)
            client = ch._temp_client
            if client is None:
                raise TelegramLoginError(400, "challenge_stale", "登录客户端已释放，请重新发起")
            try:
                if not client.is_connected():
                    await client.connect()
                await client.sign_in(password=password)
            except Exception as exc:
                translated = _translate_telegram_error(exc, default_message="2FA 密码校验失败")
                if translated.error_code == "challenge_expired":
                    await self._expire_challenge(ch, translated.detail)
                elif translated.error_code == "invalid_password":
                    ch.password_attempts += 1
                    if ch.password_attempts >= MAX_PASSWORD_ATTEMPTS_PER_CHALLENGE:
                        await self._fail_on_exhausted_attempts(ch, "2FA 密码错误次数过多，请重新发起登录")
                raise translated from exc

            if not await client.is_user_authorized():
                ch.password_attempts += 1
                if ch.password_attempts >= MAX_PASSWORD_ATTEMPTS_PER_CHALLENGE:
                    await self._fail_on_exhausted_attempts(ch, "2FA 密码错误次数过多，请重新发起登录")
                raise TelegramLoginError(401, "invalid_password", "2FA 密码验证未完成，请重试")

            return await self._complete_verified_login(ch, acc, client, issue_token_fn=issue_token_fn)

    async def _handle_qr_wait_exception(self, ch: LoginChallenge, exc: BaseException) -> None:
        async with self._get_account_lock(ch.account_id):
            if ch.state != "qr_pending" or self._active_by_account.get(ch.account_id) != ch.challenge_id:
                return
            if isinstance(exc, TimeoutError) or time.monotonic() >= ch.expires_at_monotonic:
                await self._expire_challenge(ch, "二维码已过期，请刷新重试")
                return
            if "SessionPasswordNeededError" in type(exc).__name__:
                now_mono = time.monotonic()
                now_epoch = time.time()
                ch.state = "password_required"
                ch.error_code = "password_required"
                ch.error_message = "扫码已确认，请输入两步验证 (2FA) 密码完成授权"
                ch.expires_at_monotonic = now_mono + PASSWORD_CONTINUATION_TTL_SECONDS
                ch.expires_at_epoch = now_epoch + PASSWORD_CONTINUATION_TTL_SECONDS
                self._arm_expiry_timer(ch)
                logger.info(
                    "QR 已扫码，等待两步验证 (flow=%s, account_id=%d, event_id=%s)", ch.flow, ch.account_id, ch.log_id
                )
                return

            translated = _translate_telegram_error(exc, default_message="扫码登录等待异常")
            ch.state = "expired" if translated.error_code == "challenge_expired" else "failed"
            ch.error_code = translated.error_code
            ch.error_message = translated.detail
            ch.retry_after_seconds = translated.retry_after_seconds
            ch.terminal_at_monotonic = time.monotonic()
            if self._active_by_account.get(ch.account_id) == ch.challenge_id:
                self._active_by_account.pop(ch.account_id, None)
            await self._cleanup_challenge_resources(ch, logout_temp=False, cancel_watcher=False)

    async def _finalize_qr_watch_success(self, ch: LoginChallenge) -> None:
        """后台仅做内存状态切换与身份预核验；真实交接、持久化与签发在匹配轮询内同步执行."""
        async with self._get_account_lock(ch.account_id):
            if ch.state != "qr_pending" or self._active_by_account.get(ch.account_id) != ch.challenge_id:
                await self._cleanup_challenge_resources(ch, logout_temp=False, cancel_watcher=False)
                return
            if time.monotonic() >= ch.expires_at_monotonic:
                await self._expire_challenge(ch, "二维码已过期，请刷新重试")
                return
            client = ch._temp_client
            if client is None:
                await self._fail_qr_watch(ch, TelegramLoginError(400, "challenge_stale", "扫码临时客户端不可用"))
                return

            try:
                async with asyncio.timeout(ch.expires_at_monotonic - time.monotonic()):
                    acc = await self._resolve_target_account(ch.flow, ch.account_id)
                    await self._assert_target_unchanged(ch, acc)
                    if not await client.is_user_authorized():
                        raise TelegramLoginError(401, "challenge_stale", "扫码会话授权未完成，请重试")
                    me = await client.get_me()
            except Exception as exc:
                if isinstance(exc, TimeoutError):
                    await self._expire_challenge(ch, "二维码已过期，请刷新重试")
                else:
                    await self._fail_qr_watch(
                        ch, _translate_telegram_error(exc, default_message="扫码后获取账号身份失败")
                    )
                return

            ok, err_code, reason = TelegramAccountClient.validate_remote_identity(
                me,
                expected_user_id=ch.expected_user_id,
                expected_phone=ch.target_phone or acc.phone,
                detailed_phone_errors=ch.flow == "native" and ch.expected_user_id is None,
            )
            if not ok:
                await self._fail_qr_watch(
                    ch,
                    TelegramLoginError(409, err_code, reason),
                    logout_temp=(acc.auth_state != "AUTHENTICATED"),
                )
                return

            now_mono = time.monotonic()
            if now_mono >= ch.expires_at_monotonic:
                await self._expire_challenge(ch, "二维码已过期，请刷新重试")
                return
            now_epoch = time.time()
            ch.state = "qr_ready"
            claim_ttl = min(max(5.0, ch.expires_at_monotonic - now_mono), QR_READY_CLAIM_TTL_SECONDS)
            ch.expires_at_monotonic = now_mono + claim_ttl
            ch.expires_at_epoch = now_epoch + claim_ttl
            self._arm_expiry_timer(ch)
            logger.info("QR 扫码身份核验通过 (flow=%s, account_id=%d, event_id=%s)", ch.flow, ch.account_id, ch.log_id)

    async def _fail_qr_watch(self, ch: LoginChallenge, error: TelegramLoginError, *, logout_temp: bool = False) -> None:
        logger.warning(
            "QR 扫码后校验失败 (flow=%s, account_id=%d, event_id=%s, reason=%s)",
            ch.flow,
            ch.account_id,
            ch.log_id,
            error.error_code,
        )
        ch.state = "failed"
        ch.error_code = error.error_code
        ch.error_message = error.detail
        ch.retry_after_seconds = error.retry_after_seconds
        ch.terminal_at_monotonic = time.monotonic()
        self._active_by_account.pop(ch.account_id, None)
        await self._cleanup_challenge_resources(ch, logout_temp=logout_temp, cancel_watcher=False)

    async def _watch_qr_challenge(self, ch: LoginChallenge) -> None:
        """后台等待扫码事件并在认证或需要 2FA 时安全推进状态（不长期持锁）."""
        qr_login = ch._qr_login
        if qr_login is None:
            return
        timeout = max(0.0, ch.expires_at_monotonic - time.monotonic())
        try:
            async with asyncio.timeout(timeout):
                await qr_login.wait(timeout=timeout)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            await self._handle_qr_wait_exception(ch, exc)
            return
        await self._finalize_qr_watch_success(ch)

    async def _resolve_qr_refresh_source(
        self,
        challenge_id: str,
        *,
        flow: LoginFlow,
        acc: TelegramAccountClient,
        initiator_fingerprint: str,
    ) -> LoginChallenge:
        previous = self._get_live_challenge(
            challenge_id,
            flow=flow,
            account_id=acc.account_id,
            method="qr",
            initiator_fingerprint=initiator_fingerprint,
            allow_terminal_read=True,
        )
        if self._latest_by_account.get(acc.account_id) != previous.challenge_id or previous.state == "cancelled":
            raise TelegramLoginError(400, "challenge_stale", "原扫码挑战已失效，请重新发起")
        await self._assert_target_unchanged(previous, acc)
        return previous

    async def start_qr(
        self,
        *,
        flow: LoginFlow,
        account_id: int | None,
        phone: str | None = None,
        auth_mode: str,
        initiator_fingerprint: str,
        refresh_challenge_id: str | None = None,
    ) -> dict[str, Any]:
        if refresh_challenge_id is not None and phone is not None:
            raise TelegramLoginError(400, "challenge_invalid", "刷新二维码时只需提交上一轮挑战 ID")
        acc = await self._resolve_target_account(flow, account_id)
        async with self._reserve_start(acc.account_id, supersede=False):
            previous = None
            if refresh_challenge_id is not None:
                previous = await self._resolve_qr_refresh_source(
                    refresh_challenge_id,
                    flow=flow,
                    acc=acc,
                    initiator_fingerprint=initiator_fingerprint,
                )
                target_phone, pinned_user_id = previous.target_phone, previous.expected_user_id
            else:
                target_phone, pinned_user_id = self._resolve_target_phone_and_pin(
                    flow=flow, auth_mode=auth_mode, acc=acc, req_phone=phone, require_phone=False
                )

            binding = self._read_binding(flow, acc.account_id)
            if flow == "native" and pinned_user_id is None:
                self._enforce_first_bind_create_quota()
            if previous is None:
                await self._supersede_account_challenge(acc.account_id)
                if self._read_binding(flow, acc.account_id) != binding:
                    raise TelegramLoginError(400, "challenge_stale", "账号绑定已变更，请重新发起")

            temp_client = None
            try:
                temp_client = await self._prepare_challenge_client(acc)
                qr_login = await temp_client.qr_login()
                qr_url = str(getattr(qr_login, "url", "") or "")
                if not qr_url.startswith("tg://login?token="):
                    raise RuntimeError("Invalid QR login URL generated")
                if previous is not None:
                    await self._supersede_account_challenge(acc.account_id)
                now_mono = time.monotonic()
                now_epoch = time.time()
                expires_mono, expires_epoch = _compute_qr_expiry(
                    getattr(qr_login, "expires", None), now_epoch, now_mono
                )
            except (Exception, asyncio.CancelledError) as exc:
                if temp_client is not None:
                    await asyncio.shield(self._dispose_temp_client(temp_client, logout_temp=False))
                if isinstance(exc, asyncio.CancelledError):
                    raise
                raise _translate_telegram_error(exc, default_message="创建扫码登录会话失败") from exc

            challenge_id = secrets.token_urlsafe(24)
            ch = LoginChallenge(
                challenge_id=challenge_id,
                flow=flow,
                method="qr",
                account_id=acc.account_id,
                initiator_fingerprint=initiator_fingerprint,
                target_phone=target_phone,
                expected_user_id=pinned_user_id,
                state="qr_pending",
                created_at_monotonic=now_mono,
                expires_at_monotonic=expires_mono,
                expires_at_epoch=expires_epoch,
                binding=binding,
                qr_url=qr_url,
                _qr_login=qr_login,
                _temp_client=temp_client,
            )
            self._register_challenge(ch)
            ch._watch_task = asyncio.create_task(self._watch_qr_challenge(ch))
            self._arm_expiry_timer(ch)

            logger.info(
                "已生成 QR 扫码登录挑战 (flow=%s, account_id=%d, event_id=%s, previous_event_id=%s)",
                flow,
                acc.account_id,
                ch.log_id,
                previous.log_id if previous is not None else None,
            )
            remaining = max(1, int(expires_mono - now_mono))
            phone_hint = None
            if flow == "native" and target_phone:
                digits = canonical_phone_number(target_phone)
                phone_hint = f"•••• {digits[-min(4, len(digits) - 3) :]}"
            return {
                "status": "ok",
                "challenge_id": challenge_id,
                "account_id": acc.account_id,
                "method": "qr",
                "state": "qr_pending",
                "qr_url": qr_url,
                "target_phone_hint": phone_hint,
                "expires_at": expires_epoch,
                "remaining_seconds": remaining,
                "message": "请使用手机 Telegram 扫描二维码确认登录",
            }

    async def get_qr_status(
        self,
        *,
        flow: LoginFlow,
        account_id: int | None,
        challenge_id: str,
        initiator_fingerprint: str,
        issue_token_fn: Callable[[], str | None] | None = None,
    ) -> dict[str, Any]:
        ch = self._get_live_challenge(
            challenge_id,
            flow=flow,
            account_id=account_id,
            method="qr",
            initiator_fingerprint=initiator_fingerprint,
            allow_terminal_read=True,
        )
        await asyncio.sleep(0)

        if ch.state in ("qr_pending", "password_required", "qr_ready") and time.monotonic() > ch.expires_at_monotonic:
            async with self._get_account_lock(ch.account_id):
                if (
                    ch.state in ("qr_pending", "password_required", "qr_ready")
                    and time.monotonic() > ch.expires_at_monotonic
                ):
                    await self._expire_challenge(ch, "二维码或验证已过期，请重新生成")

        if ch.state == "authenticated" or ch.consumed:
            raise TelegramLoginError(400, "challenge_stale", "该登录挑战已完成，禁止重复使用")
        if ch.state == "qr_ready":
            return await self._claim_qr_ready(
                challenge_id,
                flow=flow,
                account_id=account_id,
                initiator_fingerprint=initiator_fingerprint,
                issue_token_fn=issue_token_fn,
            )

        remaining = (
            max(0, int(ch.expires_at_monotonic - time.monotonic()))
            if ch.state in ("qr_pending", "password_required")
            else 0
        )
        if ch.state == "password_required":
            return {
                "status": "password_required",
                "challenge_id": ch.challenge_id,
                "account_id": ch.account_id,
                "state": "password_required",
                "authenticated": False,
                "expires_at": ch.expires_at_epoch,
                "remaining_seconds": remaining,
                "token": None,
                "error_code": "password_required",
                "retry_after_seconds": None,
                "message": ch.error_message or "扫码已确认，请输入两步验证 (2FA) 密码",
            }
        if ch.state in ("expired", "cancelled", "failed"):
            return {
                "status": ch.state,
                "challenge_id": ch.challenge_id,
                "account_id": ch.account_id,
                "state": ch.state,
                "authenticated": False,
                "expires_at": ch.expires_at_epoch,
                "remaining_seconds": 0,
                "token": None,
                "error_code": ch.error_code,
                "retry_after_seconds": ch.retry_after_seconds,
                "message": ch.error_message or "扫码登录未完成",
            }
        return {
            "status": "ok",
            "challenge_id": ch.challenge_id,
            "account_id": ch.account_id,
            "state": "qr_pending",
            "authenticated": False,
            "expires_at": ch.expires_at_epoch,
            "remaining_seconds": remaining,
            "token": None,
            "error_code": None,
            "retry_after_seconds": None,
            "message": "等待手机 Telegram 扫描二维码确认...",
        }

    async def _claim_qr_ready(
        self,
        challenge_id: str,
        *,
        flow: LoginFlow,
        account_id: int | None,
        initiator_fingerprint: str,
        issue_token_fn: Callable[[], str | None] | None,
    ) -> dict[str, Any]:
        ch_preview = self._get_live_challenge(
            challenge_id,
            flow=flow,
            account_id=account_id,
            method="qr",
            initiator_fingerprint=initiator_fingerprint,
        )
        async with self._get_account_lock(ch_preview.account_id):
            ch = self._get_live_challenge(
                challenge_id,
                flow=flow,
                account_id=account_id,
                method="qr",
                initiator_fingerprint=initiator_fingerprint,
            )
            if ch.state != "qr_ready":
                raise TelegramLoginError(400, "challenge_stale", "扫码状态已变更，请重新查询或发起登录")
            if time.monotonic() > ch.expires_at_monotonic:
                await self._expire_challenge(ch, "扫码登录超时未领取，请重新扫码")
                raise TelegramLoginError(400, "challenge_expired", "扫码登录超时未领取，请重新扫码")

            acc = await self._resolve_target_account(flow, ch.account_id)
            client = ch._temp_client
            if client is None:
                raise TelegramLoginError(400, "challenge_stale", "扫码临时客户端已释放，请重新发起")

            completed = await self._complete_verified_login(ch, acc, client, issue_token_fn=issue_token_fn)
            return {
                "status": "ok",
                "challenge_id": ch.challenge_id,
                "account_id": ch.account_id,
                "state": "authenticated",
                "authenticated": True,
                "expires_at": ch.expires_at_epoch,
                "remaining_seconds": 0,
                "token": completed.get("token"),
                "is_premium": completed.get("is_premium"),
                "dc_id": completed.get("dc_id"),
                "user_id": completed.get("user_id"),
                "username": completed.get("username"),
                "error_code": None,
                "retry_after_seconds": None,
                "message": completed.get("message") or "扫码登录授权成功",
            }

    async def cancel_challenge(
        self,
        *,
        flow: LoginFlow,
        account_id: int | None,
        challenge_id: str,
        initiator_fingerprint: str,
    ) -> dict[str, Any]:
        ch_preview = self._get_live_challenge(
            challenge_id,
            flow=flow,
            account_id=account_id,
            method=None,
            initiator_fingerprint=initiator_fingerprint,
            allow_terminal_read=True,
        )
        async with self._get_account_lock(ch_preview.account_id):
            ch = self._get_live_challenge(
                challenge_id,
                flow=flow,
                account_id=account_id,
                method=None,
                initiator_fingerprint=initiator_fingerprint,
                allow_terminal_read=True,
            )
            if ch.state not in TERMINAL_STATES:
                ch.state = "cancelled"
                ch.error_code = "challenge_cancelled"
                ch.error_message = "登录挑战已取消"
                ch.terminal_at_monotonic = time.monotonic()
            if self._active_by_account.get(ch.account_id) == ch.challenge_id:
                self._active_by_account.pop(ch.account_id, None)
            if self._latest_by_account.get(ch.account_id) == ch.challenge_id:
                self._latest_by_account.pop(ch.account_id, None)
            await self._cleanup_challenge_resources(ch, logout_temp=False, cancel_watcher=True)
            return {
                "status": "cancelled",
                "challenge_id": ch.challenge_id,
                "state": "cancelled",
                "message": "登录挑战已取消并清理临时会话",
            }


telegram_login_service = TelegramLoginService()
