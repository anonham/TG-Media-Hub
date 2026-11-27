"""API 层共享的 Telegram 登录挑战辅助工具（无缓存响应头、浏览器挑战上下文、指纹与错误映射）。"""

import secrets

from fastapi import Request, Response
from fastapi.responses import JSONResponse

from app.api.auth import BOOTSTRAP_COOKIE_NAME
from app.services.telegram_login_service import LoginFlow, TelegramLoginError, build_initiator_fingerprint

LOGIN_CTX_COOKIE_NAME = "tg_login_ctx"

NO_CACHE_HEADERS: dict[str, str] = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}


def apply_no_cache_headers(response: Response) -> None:
    for k, v in NO_CACHE_HEADERS.items():
        response.headers[k] = v


def read_login_context(request: Request, *, auth_mode: str) -> str:
    """读取现有浏览器级随机登录上下文 Cookie (仅在 bootstrap 模式下用于单浏览器隔离)."""
    if auth_mode != "bootstrap":
        return ""
    raw = request.cookies.get(LOGIN_CTX_COOKIE_NAME, "")
    return raw.strip() if isinstance(raw, str) else ""


def resolve_or_issue_login_context(request: Request, response: Response, *, auth_mode: str) -> str:
    """在 bootstrap 发起阶段绑定或签发 HttpOnly 随机浏览器登录上下文 Cookie."""
    if auth_mode != "bootstrap":
        return ""
    existing = read_login_context(request, auth_mode=auth_mode)
    ctx_token = existing if len(existing) >= 16 else secrets.token_urlsafe(24)
    response.set_cookie(
        key=LOGIN_CTX_COOKIE_NAME,
        value=ctx_token,
        httponly=True,
        samesite="lax",
        secure=(request.url.scheme == "https"),
        path="/api",
        max_age=900,
    )
    return ctx_token


def extract_login_initiator(
    request: Request,
    *,
    flow: LoginFlow,
    auth_mode: str,
    context_token: str = "",
) -> str:
    bootstrap_cred = request.state.bootstrap_credential if auth_mode == "bootstrap" else None
    return build_initiator_fingerprint(
        flow=flow,
        auth_mode=auth_mode,
        api_key=request.headers.get("x-api-key"),
        two_fa_token=request.headers.get("x-2fa-token"),
        bootstrap_cred=bootstrap_cred,
        client_host=request.client.host if request.client else "127.0.0.1",
        user_agent=request.headers.get("user-agent"),
        context_token=context_token,
    )


def clear_bootstrap_cookies(response: Response) -> None:
    """在首绑/重绑完成或显式登出时清理受限引导上下文与会话 Cookie."""
    response.delete_cookie(key=LOGIN_CTX_COOKIE_NAME, path="/api")
    response.delete_cookie(key=BOOTSTRAP_COOKIE_NAME, path="/api")


def login_error_response(exc: TelegramLoginError) -> JSONResponse:
    content: dict[str, object] = {
        "detail": exc.detail,
        "error_code": exc.error_code,
    }
    headers = dict(NO_CACHE_HEADERS)
    if exc.retry_after_seconds is not None:
        content["retry_after_seconds"] = exc.retry_after_seconds
        if exc.http_status == 429:
            headers["Retry-After"] = str(exc.retry_after_seconds)
    return JSONResponse(status_code=exc.http_status, content=content, headers=headers)
