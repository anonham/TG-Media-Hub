from typing import Any

from pydantic import BaseModel, Field


class SystemSettingItem(BaseModel):
    key: str
    value: str
    category: str = "general"
    description: str | None = None
    updated_at: float


class SystemSettingsGroupedResponse(BaseModel):
    categories: dict[str, list[SystemSettingItem]]
    all_settings: dict[str, str]
    app_version: str = Field(default="1.0.0", description="系统主版本号")


class SystemSettingUpdateRequest(BaseModel):
    settings: dict[str, Any] = Field(..., description="待更新的配置键值映射")


class TransitBotStatusItem(BaseModel):
    bot_id: int
    bot_name: str | None = None
    session_name: str
    bot_username: str | None = None
    is_ready: bool
    in_transit_group: bool = False
    is_admin: bool = False
    error: str | None = None


class TransitVerifyHealthResponse(BaseModel):
    status: str
    message: str
    transit_chat_id: str
    transit_chat_title: str | None = None
    userbot_accessible: bool = False
    bots_status: list[TransitBotStatusItem] = []
    ready_bots_count: int = 0
    total_bots_count: int = 0
    issues: list[str] = []
