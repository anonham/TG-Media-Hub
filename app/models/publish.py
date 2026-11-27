from typing import Literal

from pydantic import AliasChoices, BaseModel, Field


class OversizedFileInfo(BaseModel):
    file_name: str
    rel_path: str
    file_size_bytes: int
    file_size_mb: float
    file_size_gb: float
    limit_mb: float
    exceeded_mb: float
    oversize_kind: str = "non_premium_limit"  # 'non_premium_limit' (非会员超限) | 'premium_limit' (平台最高上限超限)


class MediaItemInfo(BaseModel):
    file_name: str
    rel_path: str
    file_size_bytes: int
    file_size_mb: float
    media_type: str  # 'image' | 'video' | 'audio'
    is_oversized: bool = False
    is_already_published: bool = False
    file_hash: str | None = None


class IgnoredFileInfo(BaseModel):
    file_name: str
    rel_path: str
    file_size_mb: float
    extension: str
    reason: str = "非有效媒体格式 (已自动忽略)"


class UnavailableFileInfo(BaseModel):
    """巡检期文件大小不可读取的明细 (单文件异常不中止整目录巡检)"""

    file_name: str
    rel_path: str
    reason: str = "文件大小不可读取 (可能已被移除或无访问权限)，已从巡检明细中跳过"


class MediaCategorySummary(BaseModel):
    count: int = 0
    size_bytes: int = 0
    size_mb: float = 0.0


class PresetInspectRequest(BaseModel):
    channel_id: str = Field(..., description="频道 ID、Username 或链接")
    preset_type: str = Field("target", description="预设类型: 'target' | 'source'")
    account_id: int | None = Field(None, description="指定执行的 Userbot 账号 ID")


class PresetInspectResponse(BaseModel):
    channel_identifier: str
    parsed_chat_id: str
    channel_title: str
    channel_username: str | None = None
    channel_type: str = "Channel"
    is_accessible: bool
    is_restricted: bool = False
    has_read_permission: bool = False
    has_write_permission: bool = False
    recommended_name: str = ""
    permission_summary: str = ""
    error_detail: str | None = None
    account_id: int | None = None
    account_name: str | None = None


class InspectFolderRequest(BaseModel):
    folder_path: str = Field(..., description="本地媒体文件夹绝对路径")
    target_channel: str | None = Field(
        None, description="目标 Telegram 频道 ID/Username (用于防重检测预览及写权限探针)"
    )
    account_id: int | None = Field(None, description="指定执行的 Userbot 账号 ID")


class InspectFolderResponse(BaseModel):
    folder_path: str
    total_scanned_files: int
    total_media_files: int
    total_media_size_bytes: int
    total_media_size_mb: float
    total_media_size_gb: float
    valid_files_count: int
    already_published_count: int
    pending_publish_count: int = 0
    account_premium: bool = False
    effective_limit_mb: float = Field(
        2000.0,
        description="当前账号有效单文件上限 (MiB，普通账号 2000 MiB (1.95 GiB)，Premium 账号 4000 MiB (3.91 GiB))",
    )
    images: MediaCategorySummary
    videos: MediaCategorySummary
    audios: MediaCategorySummary
    ignored_files_count: int
    unavailable_files_count: int = 0
    oversized_files: list[OversizedFileInfo] = []
    media_files: list[MediaItemInfo] = []
    ignored_files: list[IgnoredFileInfo] = []
    unavailable_files: list[UnavailableFileInfo] = []
    target_channel_info: PresetInspectResponse | None = None
    # 向后兼容字段
    total_files: int | None = None
    total_size_bytes: int | None = None
    total_size_mb: float | None = None
    total_size_gb: float | None = None


class PublishFolderRequest(BaseModel):
    folder_path: str = Field(..., description="本地媒体文件夹绝对路径")
    target_channel: str = Field(..., description="目标 Telegram 频道 ID/Username (必填)")
    album_mode: bool = Field(True, description="是否打包为 Album 媒体组")
    delay_seconds: float = Field(3.0, ge=0.5, description="每次发送风控延迟 (秒)")
    account_id: int | None = Field(None, description="指定执行的 Userbot 账号 ID (可选)")
    priority: int | None = Field(10, description="任务优先级 (数值越大优先级越高，默认 10)")
    upload_concurrency: int | None = Field(
        None, ge=1, le=16, description="单任务上传并发通道数 (1-16，缺省使用系统设置)"
    )
    upload_in_flight_per_connection: int | None = Field(
        None,
        ge=1,
        le=4,
        validation_alias=AliasChoices("upload_in_flight_per_connection", "upload_in_flight"),
        description="单连接在途分块数规范契约 (1-4，缺省使用系统设置)",
    )
    force_republish: bool = Field(
        False,
        description="强制重传: True 时忽略已发布防重记录重复发送 (刷屏/风控风险, 前端需确认警示); 发布历史照常写入保持幂等",
    )
    oversize_policy: Literal["skip_and_report", "split", "strict_fail"] = Field(
        "skip_and_report",
        description="超限文件处理策略: skip_and_report=跳过并占位报告(默认); strict_fail=严格报错中断; split 仅 PikPak 流式支持",
    )
    send_placeholder_card: bool = Field(
        True,
        description="是否在频道发送缺集/失败占位说明卡 (skip_and_report 默认启用; 与 PikPak 链路共用同一开关; 严禁写入 PublishHistory)",
    )


class PresetCreateRequest(BaseModel):
    name: str = Field(..., description="频道备注名称")
    channel_id: str = Field(..., description="频道 ID / Username")
    preset_type: str = Field("target", description="预设类型: 'target' | 'source'")
    title: str | None = Field("", description="真实频道名称")


class PresetResponse(BaseModel):
    id: int
    name: str
    channel_id: str
    preset_type: str = "target"
    title: str | None = ""
    created_at: float


class PublishFailureItem(BaseModel):
    time: str
    task_id: str | None = None
    chat_id: str
    file: str
    size_mb: float
    client: str
    type: str
    reason: str
    retryable: bool = False


class PublishFailuresResponse(BaseModel):
    total: int
    page: int
    limit: int
    items: list[PublishFailureItem]


class DownloadFailureItem(BaseModel):
    time: str
    task_id: str | None = None
    chat_id: str
    file: str
    size_mb: float
    client: str
    type: str
    reason: str
    retryable: bool = False


class DownloadFailuresResponse(BaseModel):
    total: int
    page: int
    limit: int
    items: list[DownloadFailureItem]
