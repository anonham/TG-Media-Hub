from typing import Literal

from pydantic import BaseModel, Field

# ==========================================
# 1. Userbot 账号模型 (Account Models)
# ==========================================


class AccountCreateRequest(BaseModel):
    account_name: str = Field(..., min_length=1, max_length=100, description="账号名称/备注")
    phone: str = Field(..., min_length=1, description="手机号 (带国际区号，必填)")
    session_file: str | None = Field(None, description="Session 文件名 (留空则自动生成)")
    api_id: int | None = Field(None, description="Telegram API ID (可选，留空使用系统默认配置)")
    api_hash: str | None = Field(None, description="Telegram API Hash (可选，留空使用系统默认配置)")
    max_concurrency: int = Field(1, ge=1, le=10, description="单账号最大并发任务数")
    is_active: bool = Field(True, description="是否启用该账号")
    is_default: bool = Field(False, description="是否设为默认主账号")


class AccountUpdateRequest(BaseModel):
    account_name: str | None = Field(None, min_length=1, max_length=100, description="账号名称/备注")
    phone: str | None = Field(None, description="手机号")
    max_concurrency: int | None = Field(None, ge=1, le=10, description="单账号最大并发任务数")
    is_active: bool | None = Field(None, description="是否启用该账号")
    is_default: bool | None = Field(None, description="是否设为默认主账号")


class AccountResponse(BaseModel):
    id: int
    account_name: str
    phone: str | None = ""
    session_file: str
    api_id: int
    api_hash_masked: str
    is_active: bool
    is_default: bool
    is_premium: bool = False
    premium_checked_at: float = 0.0
    status: str = "unauthenticated"
    flood_wait_until: float = 0.0
    flood_wait_remaining_seconds: float = 0.0
    dc_id: int = 0
    user_id: int | None = None
    username: str | None = None
    first_name: str | None = None
    max_concurrency: int = 1
    active_task_count: int = 0
    created_at: float
    updated_at: float


class AccountSendCodeRequest(BaseModel):
    phone: str | None = Field(None, description="手机号 (留空使用账号绑定的手机号)")


class AccountSendCodeResponse(BaseModel):
    status: str = "ok"
    account_id: int
    phone: str
    challenge_id: str
    method: str = "otp"
    state: str = "code_sent"
    delivery_channel: str = "unknown"
    code_length: int | None = None
    next_delivery_channel: str | None = None
    can_resend: bool = False
    resend_available_at: float | None = None
    resend_wait_seconds: int = 0
    expires_at: float
    message: str


class AccountVerifyCodeRequest(BaseModel):
    challenge_id: str = Field(..., min_length=1, description="登录挑战 ID")
    code: str | None = Field(None, description="官方 Telegram 登录验证码 (保留前导零)")
    password: str | None = Field(None, description="两步验证密码 (若账号启用 2FA)")


class AccountVerifyCodeResponse(BaseModel):
    status: str = "ok"
    state: str = "authenticated"
    authenticated: bool = True
    challenge_id: str
    message: str
    account_id: int
    is_premium: bool = False
    dc_id: int = 0
    user_id: int | None = None
    username: str | None = None
    error_code: str | None = None
    retry_after_seconds: int | None = None


class AccountRefreshResponse(BaseModel):
    status: str = "ok"
    account_id: int
    auth_state: str
    is_premium: bool = False
    dc_id: int = 0
    user_id: int | None = None
    username: str | None = None
    first_name: str | None = None


class AccountDeleteResponse(BaseModel):
    status: str = "deleted"
    account_id: int


# ==========================================
# 2. Bot 令牌池模型 (Bot Models)
# ==========================================


class BotCreateRequest(BaseModel):
    bot_token: str = Field(..., description="Telegram Bot Token (如 123456:ABC-DEF...)")
    bot_name: str | None = Field(None, description="Bot 备注名称 (留空自动命名)")
    session_name: str | None = Field(None, description="Session 标识名 (留空自动生成)")
    api_id: int | None = Field(None, description="API ID (留空使用系统默认凭证)")
    api_hash: str | None = Field(None, description="API Hash (留空使用系统默认凭证)")
    is_active: bool = Field(True, description="是否启用该 Bot")


class BotResponse(BaseModel):
    id: int
    bot_token_masked: str
    bot_name: str | None = None
    bot_username: str | None = None
    session_name: str
    api_id: int
    api_hash_masked: str
    is_active: bool
    status: str = "unauthenticated"
    flood_wait_until: float = 0.0
    flood_wait_remaining_seconds: float = 0.0
    dc_id: int = 0
    ping_ms: float | None = None
    created_at: float
    updated_at: float


class BotPingResult(BaseModel):
    bot_id: int
    bot_name: str | None = None
    bot_username: str | None = None
    online: bool
    latency_ms: float | None = None
    dc_id: int = 0
    status: str
    error: str | None = None


class BotPingResponse(BaseModel):
    total: int
    online_count: int
    results: list[BotPingResult]


class BotDeleteResponse(BaseModel):
    status: str = "deleted"
    bot_id: int


# ==========================================
# 3. 控制台与 Web 2FA 鉴权模型 (Auth Models)
# ==========================================


class AuthStatusResponse(BaseModel):
    auth_required: bool = Field(..., description="控制台是否启用了 API Key 身份验证")
    console_locked: bool = Field(False, description="公网入口未配置 API_KEY 时控制台锁定 (提示部署者完成配置)")
    restricted: bool = Field(False, description="是否因默认 Userbot 未绑定/未验证而处于受限引导状态")
    bootstrap_token_configured: bool = Field(False, description="是否已配置有效的高熵值 TG_BOOTSTRAP_TOKEN")
    bootstrap_authorized: bool = Field(False, description="当前请求是否已通过部署者引导认证")
    default_phone_configured: bool = Field(False, description="部署配置是否锁定了 TG_DEFAULT_PHONE")
    default_phone_valid: bool = Field(True, description="显式配置的 TG_DEFAULT_PHONE 格式是否合法")
    binding_mode: str = Field("admitted", description="默认账号准入模式: first_bind | rebind | admitted | unavailable")


class BootstrapAuthResponse(BaseModel):
    status: str = "ok"
    authorized: bool = True
    expires_in: int = Field(..., description="短期引导会话凭证有效期 (秒)")


class MediaTicketResponse(BaseModel):
    ticket: str = Field(..., description="短时效媒体流 ticket (TTL 内可重复消费, 兼容 <video> 206 Range 复用)")
    expires_in: int = Field(..., description="ticket 有效期 (秒)")


class TelegramStartupStatus(BaseModel):
    phase: Literal["initializing", "ready", "degraded"]
    scheduler_ready: bool
    ready_bots: int
    total_bots: int
    unready_bots: list[str]


class TelegramAuthStatusResponse(BaseModel):
    auth_state: str = Field(..., description="Telegram 鉴权状态")
    is_connected: bool = Field(..., description="主客户端是否连接")
    premium: bool = Field(..., description="是否具备 Premium 特权")
    upload_limit_mb: float = Field(..., description="单文件有效上传上限 (MiB)")
    startup_degraded: bool = Field(False, description="是否处于启动降级模式")
    startup: TelegramStartupStatus | None = Field(None, description="已准入后的引擎启动状态与启用 Bot 统计")
    restricted: bool = Field(False, description="是否因默认 Userbot 未绑定/未验证而处于受限引导状态")
    binding_mode: str = Field("admitted", description="默认账号准入模式: first_bind | rebind | admitted | unavailable")
    bootstrap_token_configured: bool = Field(False, description="是否已配置有效的高熵值 TG_BOOTSTRAP_TOKEN")
    bootstrap_authorized: bool = Field(False, description="当前请求是否已通过部署者引导认证")
    default_phone_configured: bool = Field(False, description="部署配置是否锁定了 TG_DEFAULT_PHONE")
    default_phone_valid: bool = Field(True, description="显式配置的 TG_DEFAULT_PHONE 格式是否合法")


class Web2FASendRequest(BaseModel):
    pass


class Web2FAVerifyRequest(BaseModel):
    code: str = Field(..., description="Web 2FA 验证码")


class Web2FAVerifyResponse(BaseModel):
    status: str = "ok"
    message: str = "Web 2FA 验证成功"
    token: str | None = None


class NativeSendCodeRequest(BaseModel):
    phone: str | None = Field(None, description="手机号 (带国际区号；首次绑定必填，已固定账号恢复时可省略)")


class NativeQrStartRequest(NativeSendCodeRequest):
    refresh_challenge_id: str | None = Field(
        None, min_length=1, description="刷新时提交上一轮扫码挑战 ID，不提交手机号"
    )


class NativeSendCodeResponse(BaseModel):
    status: str = "ok"
    challenge_id: str
    method: str = "otp"
    state: str = "code_sent"
    delivery_channel: str = "unknown"
    code_length: int | None = None
    next_delivery_channel: str | None = None
    can_resend: bool = False
    resend_available_at: float | None = None
    resend_wait_seconds: int = 0
    expires_at: float
    message: str


class NativeVerifyCodeRequest(BaseModel):
    challenge_id: str = Field(..., min_length=1, description="登录挑战 ID")
    code: str | None = Field(None, description="官方 Telegram 登录验证码 (保留前导零)")
    password: str | None = Field(None, description="两步验证密码 (若开启)")


class NativeVerifyCodeResponse(BaseModel):
    status: str = "ok"
    state: str = "authenticated"
    authenticated: bool = True
    challenge_id: str
    message: str = "Telegram 账号鉴权成功，已生成 session"
    token: str | None = None
    error_code: str | None = None
    retry_after_seconds: int | None = None


class ChallengeActionRequest(BaseModel):
    challenge_id: str = Field(..., min_length=1, description="登录挑战 ID")


class ChallengePasswordRequest(BaseModel):
    challenge_id: str = Field(..., min_length=1, description="登录挑战 ID")
    password: str = Field(..., min_length=1, description="两步验证密码")


class QrChallengeStartResponse(BaseModel):
    status: str = "ok"
    challenge_id: str
    account_id: int | None = None
    method: str = "qr"
    state: str = "qr_pending"
    qr_url: str
    expires_at: float
    remaining_seconds: int
    message: str


class NativeQrChallengeStartResponse(QrChallengeStartResponse):
    target_phone_hint: str | None = Field(None, description="本次扫码目标手机号掩码")


class QrChallengeStatusResponse(BaseModel):
    status: str = "ok"
    challenge_id: str
    account_id: int | None = None
    state: str
    authenticated: bool = False
    expires_at: float | None = None
    remaining_seconds: int = 0
    token: str | None = None
    is_premium: bool | None = None
    dc_id: int | None = None
    user_id: int | None = None
    username: str | None = None
    error_code: str | None = None
    retry_after_seconds: int | None = None
    message: str


class ChallengeCancelResponse(BaseModel):
    status: str = "cancelled"
    challenge_id: str
    state: str = "cancelled"
    message: str
