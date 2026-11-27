import os
import sqlite3
import time

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from app.api._login_helpers import apply_no_cache_headers, extract_login_initiator, login_error_response
from app.api.auth import login_attempt_rate_limiter, sensitive_action_rate_limiter
from app.config import settings
from app.core.client_manager import (
    SessionHandoverRecoveryError,
    canonical_phone_number,
    client_manager,
    normalize_phone_number,
)
from app.core.scheduler import task_scheduler
from app.models.auth import (
    AccountCreateRequest,
    AccountDeleteResponse,
    AccountRefreshResponse,
    AccountResponse,
    AccountSendCodeRequest,
    AccountSendCodeResponse,
    AccountUpdateRequest,
    AccountVerifyCodeRequest,
    AccountVerifyCodeResponse,
    BotCreateRequest,
    BotDeleteResponse,
    BotPingResponse,
    BotPingResult,
    BotResponse,
    ChallengeActionRequest,
    ChallengeCancelResponse,
    ChallengePasswordRequest,
    QrChallengeStartResponse,
    QrChallengeStatusResponse,
)
from app.services.account_admin_service import AccountAdminService
from app.services.telegram_login_service import TelegramLoginError, telegram_login_service

accounts_router = APIRouter()
bots_router = APIRouter()


def _mask_api_hash(raw: str | None) -> str:
    if not raw:
        return ""
    s = str(raw).strip()
    if len(s) <= 8:
        return "****"
    return f"{s[:4]}****{s[-4:]}"


def _mask_bot_token(raw: str | None) -> str:
    if not raw:
        return ""
    s = str(raw).strip()
    if ":" in s:
        bot_id_part, secret_part = s.split(":", 1)
        if len(secret_part) > 4:
            return f"{bot_id_part}:******{secret_part[-4:]}"
        return f"{bot_id_part}:******"
    if len(s) <= 8:
        return "******"
    return f"{s[:4]}******{s[-4:]}"


def _sanitize_session_stem(raw: str, fallback: str) -> str:
    """提取 basename 并过滤非字母数字/下划线/连字符，防止路径混淆与别名碰撞。"""
    stem = os.path.basename(raw.strip().replace("\\", "/"))
    if stem.endswith(".session"):
        stem = stem[:-8]
    safe = "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in stem)
    return safe or fallback


@accounts_router.get("", response_model=list[AccountResponse])
async def list_accounts():
    """获取所有 Userbot 账号列表及实时运行状态"""
    db_accounts = AccountAdminService.list_account_records()
    results = []
    now = time.time()

    for acc in db_accounts:
        acc_id = acc["id"]
        acc_client = client_manager.get_account(acc_id)

        # 实时 FloodWait 剩余秒数
        fw_until = acc.get("flood_wait_until", 0.0) or 0.0
        remaining_fw = max(0.0, fw_until - now)

        # 运行时状态
        if acc_client:
            status = acc_client.auth_state.lower()
            if status == "unauthenticated" and acc.get("status") == "invalid":
                status = "invalid"
            is_premium = acc_client.is_premium
            dc_id = acc_client.dc_id or acc.get("dc_id", 0)
            user_id = acc_client.user_id or acc.get("user_id")
            username = acc_client.username or acc.get("username")
            first_name = acc_client.first_name or acc.get("first_name")
        else:
            status = acc.get("status", "unauthenticated")
            is_premium = bool(acc.get("is_premium", 0))
            dc_id = acc.get("dc_id", 0)
            user_id = acc.get("user_id")
            username = acc.get("username")
            first_name = acc.get("first_name")

        active_tasks = task_scheduler.get_account_task_count(acc_id)

        results.append(
            AccountResponse(
                id=acc_id,
                account_name=acc["account_name"],
                phone=acc.get("phone", "") or "",
                session_file=acc["session_file"],
                api_id=acc["api_id"],
                api_hash_masked=_mask_api_hash(acc["api_hash"]),
                is_active=bool(acc.get("is_active", 1)),
                is_default=bool(acc.get("is_default", 0)),
                is_premium=is_premium,
                premium_checked_at=acc.get("premium_checked_at", 0.0) or 0.0,
                status=status,
                flood_wait_until=fw_until,
                flood_wait_remaining_seconds=round(remaining_fw, 1),
                dc_id=dc_id,
                user_id=user_id,
                username=username,
                first_name=first_name,
                max_concurrency=acc.get("max_concurrency", 1),
                active_task_count=active_tasks,
                created_at=acc.get("created_at", now),
                updated_at=acc.get("updated_at", now),
            )
        )

    return results


@accounts_router.post("", response_model=AccountResponse)
async def create_account(req: AccountCreateRequest):
    """动态录入新 Userbot 凭证并挂载连接"""
    try:
        normalized_phone = normalize_phone_number(req.phone)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    session_file = req.session_file
    if not session_file:
        safe_name = "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in req.account_name.lower())
        session_file = f"userbot_{safe_name}.session"
    else:
        session_file = f"{_sanitize_session_stem(session_file, 'userbot_custom')}.session"

    api_id = req.api_id or getattr(settings, "API_ID", None) or getattr(settings, "TG_API_ID", None)
    api_hash = (
        (req.api_hash.strip() if req.api_hash else None)
        or getattr(settings, "API_HASH", None)
        or getattr(settings, "TG_API_HASH", None)
    )
    if not api_id or not api_hash:
        raise HTTPException(
            status_code=400, detail="缺少 Telegram API ID 或 API Hash，且系统未配置默认 TG_API_ID/TG_API_HASH"
        )

    try:
        acc_client = await client_manager.add_account(
            account_name=req.account_name,
            phone=normalized_phone,
            session_file=session_file,
            api_id=int(api_id),
            api_hash=str(api_hash),
            is_active=1 if req.is_active else 0,
            is_default=1 if req.is_default else 0,
            max_concurrency=req.max_concurrency,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"创建 Userbot 账号失败: {e}") from e

    acc = AccountAdminService.get_account_record(acc_client.account_id)
    if not acc:
        raise HTTPException(status_code=500, detail="账号创建成功但读取记录失败")

    return AccountResponse(
        id=acc["id"],
        account_name=acc["account_name"],
        phone=acc.get("phone", "") or "",
        session_file=acc["session_file"],
        api_id=acc["api_id"],
        api_hash_masked=_mask_api_hash(acc["api_hash"]),
        is_active=bool(acc.get("is_active", 1)),
        is_default=bool(acc.get("is_default", 0)),
        is_premium=acc_client.is_premium,
        premium_checked_at=acc.get("premium_checked_at", 0.0) or 0.0,
        status=acc_client.auth_state.lower(),
        flood_wait_until=0.0,
        flood_wait_remaining_seconds=0.0,
        dc_id=acc_client.dc_id,
        user_id=acc_client.user_id,
        username=acc_client.username,
        first_name=acc_client.first_name,
        max_concurrency=acc.get("max_concurrency", 1),
        active_task_count=0,
        created_at=acc["created_at"],
        updated_at=acc["updated_at"],
    )


def _account_auth_mode() -> str:
    return "2fa" if settings.API_KEY else "open"


@accounts_router.post(
    "/{account_id}/send-code",
    response_model=AccountSendCodeResponse,
    dependencies=[Depends(sensitive_action_rate_limiter)],
)
async def send_account_code(
    account_id: int,
    request: Request,
    response: Response,
    req: AccountSendCodeRequest | None = None,
):
    """请求 Telegram 官方下发该账号的登录验证码"""
    apply_no_cache_headers(response)
    initiator = extract_login_initiator(request, flow="account", auth_mode=_account_auth_mode())
    try:
        return await telegram_login_service.start_otp(
            flow="account",
            account_id=account_id,
            phone=req.phone if req else None,
            auth_mode=_account_auth_mode(),
            initiator_fingerprint=initiator,
        )
    except TelegramLoginError as exc:
        return login_error_response(exc)


@accounts_router.post(
    "/{account_id}/resend-code",
    response_model=AccountSendCodeResponse,
    dependencies=[Depends(sensitive_action_rate_limiter)],
)
async def resend_account_code(
    account_id: int,
    request: Request,
    response: Response,
    req: ChallengeActionRequest,
):
    """按 Telegram next_type 与冷却期重发该账号的登录验证码"""
    apply_no_cache_headers(response)
    initiator = extract_login_initiator(request, flow="account", auth_mode=_account_auth_mode())
    try:
        return await telegram_login_service.resend_otp(
            flow="account",
            account_id=account_id,
            challenge_id=req.challenge_id,
            initiator_fingerprint=initiator,
        )
    except TelegramLoginError as exc:
        return login_error_response(exc)


@accounts_router.post(
    "/{account_id}/verify-code",
    response_model=AccountVerifyCodeResponse,
    dependencies=[Depends(login_attempt_rate_limiter)],
)
async def verify_account_code(
    account_id: int,
    request: Request,
    response: Response,
    req: AccountVerifyCodeRequest,
):
    """提交验证码与可选 2FA 密码完成指定账号会话授权"""
    apply_no_cache_headers(response)
    initiator = extract_login_initiator(request, flow="account", auth_mode=_account_auth_mode())
    try:
        return await telegram_login_service.verify_otp(
            flow="account",
            account_id=account_id,
            challenge_id=req.challenge_id,
            code=req.code,
            password=req.password,
            initiator_fingerprint=initiator,
            issue_token_fn=None,
        )
    except TelegramLoginError as exc:
        return login_error_response(exc)


@accounts_router.post(
    "/{account_id}/password",
    response_model=AccountVerifyCodeResponse,
    dependencies=[Depends(login_attempt_rate_limiter)],
)
async def submit_account_password(
    account_id: int,
    request: Request,
    response: Response,
    req: ChallengePasswordRequest,
):
    """提交 2FA 云端密码续接指定账号登录挑战"""
    apply_no_cache_headers(response)
    initiator = extract_login_initiator(request, flow="account", auth_mode=_account_auth_mode())
    try:
        return await telegram_login_service.submit_password(
            flow="account",
            account_id=account_id,
            method=None,
            challenge_id=req.challenge_id,
            password=req.password,
            initiator_fingerprint=initiator,
            issue_token_fn=None,
        )
    except TelegramLoginError as exc:
        return login_error_response(exc)


@accounts_router.post(
    "/{account_id}/qr/start",
    response_model=QrChallengeStartResponse,
    dependencies=[Depends(sensitive_action_rate_limiter)],
)
async def start_account_qr(
    account_id: int,
    request: Request,
    response: Response,
):
    """发起或刷新指定账号的 Telegram QR 扫码登录挑战"""
    apply_no_cache_headers(response)
    initiator = extract_login_initiator(request, flow="account", auth_mode=_account_auth_mode())
    try:
        return await telegram_login_service.start_qr(
            flow="account",
            account_id=account_id,
            auth_mode=_account_auth_mode(),
            initiator_fingerprint=initiator,
        )
    except TelegramLoginError as exc:
        return login_error_response(exc)


@accounts_router.get("/{account_id}/qr/status", response_model=QrChallengeStatusResponse)
async def get_account_qr_status(
    account_id: int,
    request: Request,
    response: Response,
    challenge_id: str = Query(..., min_length=1, description="扫码登录挑战 ID"),
):
    """轮询指定账号 QR 扫码登录挑战状态"""
    apply_no_cache_headers(response)
    initiator = extract_login_initiator(request, flow="account", auth_mode=_account_auth_mode())
    try:
        return await telegram_login_service.get_qr_status(
            flow="account",
            account_id=account_id,
            challenge_id=challenge_id,
            initiator_fingerprint=initiator,
            issue_token_fn=None,
        )
    except TelegramLoginError as exc:
        return login_error_response(exc)


@accounts_router.post(
    "/{account_id}/qr/password",
    response_model=AccountVerifyCodeResponse,
    dependencies=[Depends(login_attempt_rate_limiter)],
)
async def submit_account_qr_password(
    account_id: int,
    request: Request,
    response: Response,
    req: ChallengePasswordRequest,
):
    """提交 2FA 云端密码完成指定账号 QR 扫码登录"""
    apply_no_cache_headers(response)
    initiator = extract_login_initiator(request, flow="account", auth_mode=_account_auth_mode())
    try:
        return await telegram_login_service.submit_password(
            flow="account",
            account_id=account_id,
            method="qr",
            challenge_id=req.challenge_id,
            password=req.password,
            initiator_fingerprint=initiator,
            issue_token_fn=None,
        )
    except TelegramLoginError as exc:
        return login_error_response(exc)


@accounts_router.post("/{account_id}/challenge/cancel", response_model=ChallengeCancelResponse)
async def cancel_account_challenge(
    account_id: int,
    request: Request,
    response: Response,
    req: ChallengeActionRequest,
):
    """取消指定账号挂起的登录挑战并清理临时客户端资源"""
    apply_no_cache_headers(response)
    initiator = extract_login_initiator(request, flow="account", auth_mode=_account_auth_mode())
    try:
        return await telegram_login_service.cancel_challenge(
            flow="account",
            account_id=account_id,
            challenge_id=req.challenge_id,
            initiator_fingerprint=initiator,
        )
    except TelegramLoginError as exc:
        return login_error_response(exc)


@accounts_router.post("/{account_id}/refresh", response_model=AccountRefreshResponse)
async def refresh_account_status(account_id: int):
    """强制手动探测该账号的实时在线状态与 Premium 身份"""
    acc = client_manager.get_account(account_id)
    if not acc:
        raise HTTPException(status_code=404, detail="账号不存在")

    try:
        if not acc.client or not acc.client.is_connected():
            await acc.connect()
        is_premium = await acc.probe_premium(force=True)
        if acc.is_ready():
            client_manager.schedule_startup_recovery()
        auth_state = acc.auth_state
        if auth_state == "UNAUTHENTICATED":
            record = AccountAdminService.get_account_record(account_id)
            if record and record.get("status") == "invalid":
                auth_state = "INVALID"
        return {
            "status": "ok",
            "account_id": account_id,
            "auth_state": auth_state,
            "is_premium": is_premium,
            "dc_id": acc.dc_id,
            "user_id": acc.user_id,
            "username": acc.username,
            "first_name": acc.first_name,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"探测账号状态失败: {e}") from e


@accounts_router.put("/{account_id}", response_model=AccountResponse)
async def update_account(account_id: int, req: AccountUpdateRequest):
    """修改账号配置 (账号名称、并发上限、启用/禁用、设为默认)"""
    normalized_phone: str | None = None
    if req.phone is not None:
        try:
            normalized_phone = normalize_phone_number(req.phone)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

        existing_client = client_manager.get_account(account_id)
        existing_db = AccountAdminService.get_account_record(account_id)
        if not existing_db and not existing_client:
            raise HTTPException(status_code=404, detail="账号不存在")
        current_phone = (existing_client.phone if existing_client else "") or (
            existing_db.get("phone", "") if existing_db else ""
        )
        is_bound = (existing_client is not None and existing_client.auth_state == "AUTHENTICATED") or (
            existing_db is not None
            and (existing_db.get("user_id") is not None or existing_db.get("status") == "authenticated")
        )
        if is_bound and current_phone:
            try:
                current_canonical_phone = canonical_phone_number(current_phone)
            except ValueError:
                current_canonical_phone = current_phone.strip()
            if canonical_phone_number(normalized_phone) != current_canonical_phone:
                raise HTTPException(status_code=400, detail="已授权的账号禁止直接修改手机号，请删除重建或重新授权")

    try:
        updated = await client_manager.update_account(
            account_id=account_id,
            account_name=req.account_name,
            phone=normalized_phone,
            max_concurrency=req.max_concurrency,
            is_active=req.is_active,
            is_default=req.is_default,
        )
    except (ValueError, sqlite3.IntegrityError) as e:
        raise HTTPException(
            status_code=400, detail=str(e) or f"账号代号 '{req.account_name}' 已被使用，请使用唯一代号"
        ) from e
    if not updated:
        raise HTTPException(status_code=404, detail="账号不存在")

    acc_client = client_manager.get_account(account_id)
    now = time.time()
    fw_until = updated.get("flood_wait_until", 0.0) or 0.0

    return AccountResponse(
        id=updated["id"],
        account_name=updated["account_name"],
        phone=updated.get("phone", "") or "",
        session_file=updated["session_file"],
        api_id=updated["api_id"],
        api_hash_masked=_mask_api_hash(updated["api_hash"]),
        is_active=bool(updated.get("is_active", 1)),
        is_default=bool(updated.get("is_default", 0)),
        is_premium=acc_client.is_premium if acc_client else bool(updated.get("is_premium", 0)),
        premium_checked_at=updated.get("premium_checked_at", 0.0) or 0.0,
        status=acc_client.auth_state.lower() if acc_client else updated.get("status", "unauthenticated"),
        flood_wait_until=fw_until,
        flood_wait_remaining_seconds=max(0.0, fw_until - now),
        dc_id=acc_client.dc_id if acc_client else updated.get("dc_id", 0),
        user_id=updated.get("user_id"),
        username=updated.get("username"),
        first_name=updated.get("first_name"),
        max_concurrency=updated.get("max_concurrency", 1),
        active_task_count=task_scheduler.get_account_task_count(account_id),
        created_at=updated["created_at"],
        updated_at=updated["updated_at"],
    )


@accounts_router.delete("/{account_id}", response_model=AccountDeleteResponse)
async def delete_account(account_id: int, delete_session_file: bool = False):
    """卸载并删除账号 (支持可选参数 delete_session_file 清理本地 session 文件)"""
    db_acc = AccountAdminService.get_account_record(account_id)
    if not db_acc:
        raise HTTPException(status_code=404, detail="账号不存在")

    # 检查是否有正在运行的任务绑定该账号
    active_count = task_scheduler.get_account_task_count(account_id)
    if active_count > 0:
        raise HTTPException(
            status_code=400, detail=f"该账号当前有 {active_count} 个任务正在运行，请先暂停或取消相关任务"
        )

    try:
        await client_manager.remove_account(account_id, delete_session_file=delete_session_file)
    except SessionHandoverRecoveryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"status": "deleted", "account_id": account_id}


# ==========================================
# 2. Bot 令牌池管理路由 (/api/bots)
# ==========================================


@bots_router.get("", response_model=list[BotResponse])
async def list_bots():
    """获取 Bot 令牌池列表"""
    db_bots = AccountAdminService.list_bot_records()
    results = []
    now = time.time()

    for b in db_bots:
        b_id = b["id"]
        bot_client = client_manager.get_bot(b_id)
        fw_until = b.get("flood_wait_until", 0.0) or 0.0
        remaining_fw = max(0.0, fw_until - now)

        status = bot_client.status if bot_client else b.get("status", "unauthenticated")
        username = (bot_client.bot_username if bot_client else None) or b.get("bot_username")
        dc_id = (bot_client.dc_id if bot_client else 0) or b.get("dc_id", 0)

        results.append(
            BotResponse(
                id=b_id,
                bot_token_masked=_mask_bot_token(b["bot_token"]),
                bot_name=b.get("bot_name") or f"Bot #{b_id}",
                bot_username=username,
                session_name=b["session_name"],
                api_id=b["api_id"],
                api_hash_masked=_mask_api_hash(b["api_hash"]),
                is_active=bool(b.get("is_active", 1)),
                status=status,
                flood_wait_until=fw_until,
                flood_wait_remaining_seconds=round(remaining_fw, 1),
                dc_id=dc_id,
                ping_ms=None,
                created_at=b.get("created_at", now),
                updated_at=b.get("updated_at", now),
            )
        )

    return results


@bots_router.post("", response_model=BotResponse)
async def create_bot(req: BotCreateRequest):
    """动态添加新 Bot Token 入池并即时建立连接，无需重启服务"""
    token = req.bot_token.strip()
    if not token or ":" not in token:
        raise HTTPException(status_code=400, detail="无效的 Bot Token 格式")

    token_prefix = token.split(":", 1)[0]
    default_session_name = _sanitize_session_stem(f"bot_pool_{token_prefix}", "bot_pool_default")
    session_name = (
        _sanitize_session_stem(req.session_name, default_session_name) if req.session_name else default_session_name
    )

    api_id = req.api_id or settings.API_ID
    api_hash = req.api_hash or settings.API_HASH
    # 显式判空: 系统未配置默认凭证且请求未携带时给出清晰 400, 而非让 int(None) 在客户端构造中 TypeError
    if not api_id or not api_hash:
        raise HTTPException(
            status_code=400, detail="缺少 Telegram API ID 或 API Hash，且系统未配置默认 TG_API_ID/TG_API_HASH"
        )
    bot_name = req.bot_name or f"Bot #{client_manager.get_bot_count() + 1}"

    try:
        bot_client = await client_manager.add_bot(
            bot_token=token,
            bot_name=bot_name,
            session_name=session_name,
            api_id=api_id,
            api_hash=api_hash,
            is_active=1 if req.is_active else 0,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"添加 Bot Token 失败: {e}") from e

    b = AccountAdminService.get_bot_record(bot_client.bot_id)
    if not b:
        raise HTTPException(status_code=500, detail="Bot 记录创建成功但读取失败")

    return BotResponse(
        id=b["id"],
        bot_token_masked=_mask_bot_token(b["bot_token"]),
        bot_name=b.get("bot_name"),
        bot_username=bot_client.bot_username or b.get("bot_username"),
        session_name=b["session_name"],
        api_id=b["api_id"],
        api_hash_masked=_mask_api_hash(b["api_hash"]),
        is_active=bool(b.get("is_active", 1)),
        status=bot_client.status,
        flood_wait_until=0.0,
        flood_wait_remaining_seconds=0.0,
        dc_id=bot_client.dc_id,
        ping_ms=None,
        created_at=b["created_at"],
        updated_at=b["updated_at"],
    )


@bots_router.post("/ping", response_model=BotPingResponse)
async def ping_bots():
    """批量健康探活与延迟测试"""
    detailed_results = await client_manager.ping_bots_detailed()
    ping_items = [
        BotPingResult(
            bot_id=r["bot_id"],
            bot_name=r.get("bot_name"),
            bot_username=r.get("bot_username"),
            online=r["online"],
            latency_ms=r.get("latency_ms"),
            dc_id=r.get("dc_id", 0),
            status=r.get("status", "unknown"),
            error=r.get("error"),
        )
        for r in detailed_results
    ]
    online_cnt = sum(1 for p in ping_items if p.online)
    return BotPingResponse(total=len(ping_items), online_count=online_cnt, results=ping_items)


@bots_router.delete("/{bot_id}", response_model=BotDeleteResponse)
async def delete_bot(bot_id: int):
    """卸载并移出 Bot 令牌"""
    db_bot = AccountAdminService.get_bot_record(bot_id)
    if not db_bot:
        raise HTTPException(status_code=404, detail="Bot 不存在")

    await client_manager.remove_bot(bot_id)
    return {"status": "deleted", "bot_id": bot_id}
