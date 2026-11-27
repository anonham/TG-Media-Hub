import logging
import time

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, Security, status

from app.api._login_helpers import (
    apply_no_cache_headers,
    clear_bootstrap_cookies,
    extract_login_initiator,
    login_error_response,
    read_login_context,
    resolve_or_issue_login_context,
)
from app.api.access import is_local_request
from app.api.auth import (
    BOOTSTRAP_COOKIE_NAME,
    BOOTSTRAP_SESSION_TTL_SECONDS,
    BootstrapAccessError,
    api_key_header,
    bootstrap_token_header,
    bootstrap_token_state,
    check_bootstrap_mismatch_cooldown,
    ensure_default_account_admitted,
    issue_2fa_token,
    issue_bootstrap_session_token,
    login_attempt_rate_limiter,
    record_bootstrap_phone_mismatch,
    reset_bootstrap_phone_mismatch,
    resolve_bootstrap_credential,
    resolve_bootstrap_phone_state,
    sensitive_action_rate_limiter,
    two_fa_header,
    validate_2fa_token,
    verify_api_key,
    verify_bootstrap_deployer,
    verify_bootstrap_header_token,
)
from app.config import settings
from app.core.client_manager import client_manager
from app.models.auth import (
    BootstrapAuthResponse,
    ChallengeActionRequest,
    ChallengeCancelResponse,
    ChallengePasswordRequest,
    NativeQrChallengeStartResponse,
    NativeQrStartRequest,
    NativeSendCodeRequest,
    NativeSendCodeResponse,
    NativeVerifyCodeRequest,
    NativeVerifyCodeResponse,
    QrChallengeStatusResponse,
    TelegramAuthStatusResponse,
    Web2FAVerifyRequest,
    Web2FAVerifyResponse,
)
from app.models.common import StatusOkResponse
from app.services.telegram_login_service import TelegramLoginError, telegram_login_service

logger = logging.getLogger(__name__)
router = APIRouter()


async def verify_native_login_access(
    request: Request,
    header_key: str | None = Security(api_key_header),
    token: str | None = Security(two_fa_header),
) -> str:
    """
    native / QR 登录路由统一鉴权入口:
    - 受限态 (默认主账号未获准入): 忽略 API_KEY 与旧 2FA 放行，强制校验部署者引导认证;
    - 正常已准入态: 严格走原有 API Key + Web 2FA (或本机开放模式)，引导密钥完全无权。
    本机开放模式仅覆盖可证明的本机直连; 非本机来源 (公网入口/反代转发/来源不明) 必须
    配置并携带 API Key。
    """
    if not client_manager.is_default_account_admitted():
        request.state.bootstrap_credential = verify_bootstrap_deployer(request)
        return "bootstrap"

    if not settings.API_KEY:
        if is_local_request(request):
            return "open"
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="未经授权的控制台访问：公网入口必须配置并携带 API Key",
        )
    await verify_api_key(header_key)
    if validate_2fa_token(token):
        return "2fa"
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="2FA 会话无效或已过期，请重新完成 Web 2FA 验证")


async def _verify_status_access(
    request: Request,
    header_key: str | None = Security(api_key_header),
) -> None:
    """/api/telegram/status 鉴权: 已准入态遵循 API_KEY (本机开放模式仅覆盖本机直连)；
    受限态仅开放无敏感数据的引导状态查询."""
    if client_manager.is_default_account_admitted():
        if not settings.API_KEY and not is_local_request(request):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="未经授权的控制台访问：公网入口必须配置并携带 API Key",
            )
        await verify_api_key(header_key)


def _finalize_login_result(result: dict, auth_mode: str, response: Response) -> dict:
    if auth_mode == "bootstrap" and result.get("authenticated"):
        clear_bootstrap_cookies(response)
    return result


def _handle_start_error(
    exc: TelegramLoginError,
    *,
    request: Request,
    bootstrap_cred: str | None,
):
    if bootstrap_cred and exc.error_code == "phone_mismatch":
        applied = record_bootstrap_phone_mismatch(request, bootstrap_cred)
        if applied is not None:
            return login_error_response(
                TelegramLoginError(
                    429,
                    "phone_mismatch_cooldown",
                    f"目标手机号验证连续不匹配，请等待 {applied} 秒后重试",
                    retry_after_seconds=applied,
                )
            )
    return login_error_response(exc)


@router.post("/bootstrap/auth", response_model=BootstrapAuthResponse)
async def authenticate_bootstrap_deployer(
    request: Request,
    response: Response,
    header_bootstrap: str | None = Security(bootstrap_token_header),
):
    """校验部署者引导密钥 TG_BOOTSTRAP_TOKEN 并签发短期 HttpOnly 引导 Cookie."""
    apply_no_cache_headers(response)
    if client_manager.is_default_account_admitted():
        raise BootstrapAccessError(
            status_code=status.HTTP_403_FORBIDDEN,
            error_code="bootstrap_not_active",
            detail="默认 Userbot 已完成验证，引导密钥在正常业务状态下无权使用",
        )
    ok, expected_token = bootstrap_token_state()
    if not ok or not expected_token:
        raise BootstrapAccessError(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            error_code="bootstrap_token_unconfigured",
            detail="未配置有效的高熵值部署者引导密钥 TG_BOOTSTRAP_TOKEN，请先在部署配置中设置后重试",
        )

    if not verify_bootstrap_header_token(request, header_bootstrap, expected_token):
        raise BootstrapAccessError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            error_code="bootstrap_token_invalid",
            detail="部署者引导密钥 TG_BOOTSTRAP_TOKEN 无效或缺失",
        )

    session_jwt = issue_bootstrap_session_token()
    response.set_cookie(
        key=BOOTSTRAP_COOKIE_NAME,
        value=session_jwt,
        httponly=True,
        samesite="lax",
        secure=(request.url.scheme == "https"),
        path="/api",
        max_age=BOOTSTRAP_SESSION_TTL_SECONDS,
    )
    return {"status": "ok", "authorized": True, "expires_in": BOOTSTRAP_SESSION_TTL_SECONDS}


@router.post("/bootstrap/logout", response_model=StatusOkResponse)
async def logout_bootstrap_deployer(response: Response):
    """清除浏览器中的部署者引导 HttpOnly Cookie 与挑战上下文 Cookie."""
    apply_no_cache_headers(response)
    clear_bootstrap_cookies(response)
    return {"status": "ok", "message": "已清除引导会话凭证"}


@router.get("/status", response_model=TelegramAuthStatusResponse)
async def get_telegram_status(
    request: Request,
    response: Response,
    _authorized: None = Depends(_verify_status_access),
    probe_premium: bool = True,
):
    """获取后端 Telegram 引擎的鉴权状态与账号 Premium 档位 (受限期不返回任何业务数据与手机号)"""
    apply_no_cache_headers(response)
    if probe_premium and client_manager.is_default_account_admitted():
        try:
            await client_manager.refresh_premium()
        except Exception as exc:
            logger.debug("状态接口刷新 Premium 档位异常 (保留原值): %s", exc)

    bootstrap_configured, _ = bootstrap_token_state()
    binding_mode, default_phone_configured, default_phone_valid = resolve_bootstrap_phone_state()
    strict_default = client_manager.get_default_account_strict()

    if strict_default is None or not strict_default.is_admitted():
        return {
            "auth_state": strict_default.auth_state if strict_default is not None else "UNAUTHENTICATED",
            "is_connected": False,
            "premium": False,
            "upload_limit_mb": 2000.0,
            "startup_degraded": getattr(client_manager, "startup_degraded", False),
            "restricted": True,
            "binding_mode": binding_mode,
            "bootstrap_token_configured": bootstrap_configured,
            "bootstrap_authorized": bool(bootstrap_configured and resolve_bootstrap_credential(request)),
            "default_phone_configured": default_phone_configured,
            "default_phone_valid": default_phone_valid,
        }

    default_client = strict_default.client
    startup = None
    # startup 块的本机开放模式放行收窄为可证明的本机直连 (键控模式凭 2FA 会话不变)
    if (not settings.API_KEY and is_local_request(request)) or validate_2fa_token(request.headers.get("x-2fa-token")):
        startup = client_manager.get_startup_status()
    return {
        "auth_state": client_manager.auth_state,
        "is_connected": default_client is not None and default_client.is_connected(),
        "premium": client_manager.is_premium,
        "upload_limit_mb": round(client_manager.effective_max_file_size_bytes() / (1024 * 1024), 2),
        "startup_degraded": getattr(client_manager, "startup_degraded", False),
        "startup": startup,
        "restricted": False,
        "binding_mode": "admitted",
        "bootstrap_token_configured": bootstrap_configured,
        "bootstrap_authorized": False,
        "default_phone_configured": default_phone_configured,
        "default_phone_valid": default_phone_valid,
    }


@router.post("/web-2fa/send", response_model=StatusOkResponse)
async def send_web_2fa(request: Request, api_key_valid: bool = Depends(verify_api_key)):
    """生成并发送 Web 2FA 验证码到 Telegram 收藏夹"""
    ensure_default_account_admitted()
    sensitive_action_rate_limiter(request)
    client_ip = request.client.host if request.client else "127.0.0.1"
    try:
        success = await client_manager.send_web_2fa_code(client_ip)
        if success:
            return {"status": "ok", "message": "验证码已发送至您的 Telegram 收藏夹 (Saved Messages)"}
        raise HTTPException(status_code=500, detail="发送验证码失败，请检查 Userbot 状态")
    except HTTPException:
        raise
    except Exception as e:
        msg = str(e)
        if "锁定" in msg or "频繁" in msg or "FloodWait" in type(e).__name__:
            raise HTTPException(status_code=429, detail=msg) from e
        raise HTTPException(status_code=500, detail=f"发送验证码失败: {msg}") from e


@router.post("/web-2fa/verify", response_model=Web2FAVerifyResponse)
async def verify_web_2fa(payload: Web2FAVerifyRequest, request: Request, api_key_valid: bool = Depends(verify_api_key)):
    """校验 Web 2FA 验证码"""
    ensure_default_account_admitted()
    login_attempt_rate_limiter(request)
    client_ip = request.client.host if request.client else "127.0.0.1"
    try:
        success = client_manager.verify_web_2fa_code(client_ip, payload.code)
        if success:
            # 签发服务端强制的 2FA 会话 JWT; 本机开放模式下为 None (前端忽略)
            return {"status": "ok", "message": "Web 2FA 验证成功", "token": issue_2fa_token()}
        raise HTTPException(status_code=401, detail="验证码错误")
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e)) from e


@router.post("/native/send-code", response_model=NativeSendCodeResponse)
async def native_send_code(
    request: Request,
    response: Response,
    payload: NativeSendCodeRequest | None = None,
    auth_mode: str = Depends(verify_native_login_access),
):
    """初次部署或重绑：请求官方下发登录验证码并创建受限登录挑战"""
    apply_no_cache_headers(response)
    bootstrap_cred: str | None = request.state.bootstrap_credential if auth_mode == "bootstrap" else None
    started_ns = time.monotonic_ns()
    if auth_mode == "bootstrap":
        cooldown = check_bootstrap_mismatch_cooldown(request, bootstrap_cred)
        if cooldown is not None:
            return login_error_response(
                TelegramLoginError(
                    429,
                    "phone_mismatch_cooldown",
                    f"目标手机号验证连续不匹配，请等待 {cooldown} 秒后重试",
                    retry_after_seconds=cooldown,
                )
            )
    sensitive_action_rate_limiter(request)
    ctx_token = resolve_or_issue_login_context(request, response, auth_mode=auth_mode)
    initiator = extract_login_initiator(request, flow="native", auth_mode=auth_mode, context_token=ctx_token)
    try:
        result = await telegram_login_service.start_otp(
            flow="native",
            account_id=None,
            phone=payload.phone if payload else None,
            auth_mode=auth_mode,
            initiator_fingerprint=initiator,
        )
        if auth_mode == "bootstrap":
            reset_bootstrap_phone_mismatch(request, bootstrap_cred, started_ns=started_ns)
        return result
    except TelegramLoginError as exc:
        return _handle_start_error(exc, request=request, bootstrap_cred=bootstrap_cred)


@router.post("/native/resend-code", response_model=NativeSendCodeResponse)
async def native_resend_code(
    request: Request,
    response: Response,
    payload: ChallengeActionRequest,
    auth_mode: str = Depends(verify_native_login_access),
):
    """按 Telegram next_type 与冷却期重发登录验证码"""
    apply_no_cache_headers(response)
    sensitive_action_rate_limiter(request)
    ctx_token = read_login_context(request, auth_mode=auth_mode)
    initiator = extract_login_initiator(request, flow="native", auth_mode=auth_mode, context_token=ctx_token)
    try:
        return await telegram_login_service.resend_otp(
            flow="native",
            account_id=None,
            challenge_id=payload.challenge_id,
            initiator_fingerprint=initiator,
        )
    except TelegramLoginError as exc:
        return login_error_response(exc)


@router.post("/native/verify-code", response_model=NativeVerifyCodeResponse)
async def native_verify_code(
    request: Request,
    response: Response,
    payload: NativeVerifyCodeRequest,
    auth_mode: str = Depends(verify_native_login_access),
):
    """提交官方验证码与可选 2FA 密码完成默认主账号授权"""
    apply_no_cache_headers(response)
    login_attempt_rate_limiter(request)
    ctx_token = read_login_context(request, auth_mode=auth_mode)
    initiator = extract_login_initiator(request, flow="native", auth_mode=auth_mode, context_token=ctx_token)
    try:
        result = await telegram_login_service.verify_otp(
            flow="native",
            account_id=None,
            challenge_id=payload.challenge_id,
            code=payload.code,
            password=payload.password,
            initiator_fingerprint=initiator,
            issue_token_fn=issue_2fa_token,
        )
        return _finalize_login_result(result, auth_mode, response)
    except TelegramLoginError as exc:
        return login_error_response(exc)


@router.post("/native/password", response_model=NativeVerifyCodeResponse)
async def native_submit_password(
    request: Request,
    response: Response,
    payload: ChallengePasswordRequest,
    auth_mode: str = Depends(verify_native_login_access),
):
    """提交 2FA 云端密码续接默认主账号登录挑战"""
    apply_no_cache_headers(response)
    login_attempt_rate_limiter(request)
    ctx_token = read_login_context(request, auth_mode=auth_mode)
    initiator = extract_login_initiator(request, flow="native", auth_mode=auth_mode, context_token=ctx_token)
    try:
        result = await telegram_login_service.submit_password(
            flow="native",
            account_id=None,
            method=None,
            challenge_id=payload.challenge_id,
            password=payload.password,
            initiator_fingerprint=initiator,
            issue_token_fn=issue_2fa_token,
        )
        return _finalize_login_result(result, auth_mode, response)
    except TelegramLoginError as exc:
        return login_error_response(exc)


@router.post("/qr/start", response_model=NativeQrChallengeStartResponse)
async def native_qr_start(
    request: Request,
    response: Response,
    payload: NativeQrStartRequest | None = None,
    auth_mode: str = Depends(verify_native_login_access),
):
    """发起或刷新默认主账号 Telegram QR 扫码登录挑战"""
    apply_no_cache_headers(response)
    bootstrap_cred: str | None = request.state.bootstrap_credential if auth_mode == "bootstrap" else None
    started_ns = time.monotonic_ns()
    if auth_mode == "bootstrap":
        cooldown = check_bootstrap_mismatch_cooldown(request, bootstrap_cred)
        if cooldown is not None:
            return login_error_response(
                TelegramLoginError(
                    429,
                    "phone_mismatch_cooldown",
                    f"目标手机号验证连续不匹配，请等待 {cooldown} 秒后重试",
                    retry_after_seconds=cooldown,
                )
            )
    sensitive_action_rate_limiter(request)
    ctx_token = resolve_or_issue_login_context(request, response, auth_mode=auth_mode)
    initiator = extract_login_initiator(request, flow="native", auth_mode=auth_mode, context_token=ctx_token)
    try:
        result = await telegram_login_service.start_qr(
            flow="native",
            account_id=None,
            phone=payload.phone if payload else None,
            auth_mode=auth_mode,
            initiator_fingerprint=initiator,
            refresh_challenge_id=payload.refresh_challenge_id if payload else None,
        )
        if auth_mode == "bootstrap":
            reset_bootstrap_phone_mismatch(request, bootstrap_cred, started_ns=started_ns)
        return result
    except TelegramLoginError as exc:
        return _handle_start_error(exc, request=request, bootstrap_cred=bootstrap_cred)


@router.get("/qr/status", response_model=QrChallengeStatusResponse)
async def native_qr_status(
    request: Request,
    response: Response,
    challenge_id: str = Query(..., min_length=1, description="扫码登录挑战 ID"),
    auth_mode: str = Depends(verify_native_login_access),
):
    """轮询默认主账号 QR 扫码登录挑战状态"""
    apply_no_cache_headers(response)
    ctx_token = read_login_context(request, auth_mode=auth_mode)
    initiator = extract_login_initiator(request, flow="native", auth_mode=auth_mode, context_token=ctx_token)
    try:
        result = await telegram_login_service.get_qr_status(
            flow="native",
            account_id=None,
            challenge_id=challenge_id,
            initiator_fingerprint=initiator,
            issue_token_fn=issue_2fa_token,
        )
        return _finalize_login_result(result, auth_mode, response)
    except TelegramLoginError as exc:
        return login_error_response(exc)


@router.post("/qr/password", response_model=NativeVerifyCodeResponse)
async def native_qr_password(
    request: Request,
    response: Response,
    payload: ChallengePasswordRequest,
    auth_mode: str = Depends(verify_native_login_access),
):
    """提交 2FA 云端密码完成默认主账号 QR 扫码登录"""
    apply_no_cache_headers(response)
    login_attempt_rate_limiter(request)
    ctx_token = read_login_context(request, auth_mode=auth_mode)
    initiator = extract_login_initiator(request, flow="native", auth_mode=auth_mode, context_token=ctx_token)
    try:
        result = await telegram_login_service.submit_password(
            flow="native",
            account_id=None,
            method="qr",
            challenge_id=payload.challenge_id,
            password=payload.password,
            initiator_fingerprint=initiator,
            issue_token_fn=issue_2fa_token,
        )
        return _finalize_login_result(result, auth_mode, response)
    except TelegramLoginError as exc:
        return login_error_response(exc)


@router.post("/challenge/cancel", response_model=ChallengeCancelResponse)
async def native_cancel_challenge(
    request: Request,
    response: Response,
    payload: ChallengeActionRequest,
    auth_mode: str = Depends(verify_native_login_access),
):
    """取消默认主账号挂起的登录挑战并释放临时客户端资源"""
    apply_no_cache_headers(response)
    ctx_token = read_login_context(request, auth_mode=auth_mode)
    initiator = extract_login_initiator(request, flow="native", auth_mode=auth_mode, context_token=ctx_token)
    try:
        return await telegram_login_service.cancel_challenge(
            flow="native",
            account_id=None,
            challenge_id=payload.challenge_id,
            initiator_fingerprint=initiator,
        )
    except TelegramLoginError as exc:
        return login_error_response(exc)
