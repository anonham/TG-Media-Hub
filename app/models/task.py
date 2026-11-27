from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, Field


class DownloadLinkRequest(BaseModel):
    links: str = Field(..., description="Telegram 消息链接，支持单条或多行")
    account_id: int | None = Field(None, description="指定执行的 Userbot 账号 ID (可选)")
    priority: int = Field(10, description="任务优先级 (数值越大优先级越高，默认 10)")
    threads: int = Field(8, ge=1, le=32, description="并发下载通道/线程数 (推荐 8 或 16，默认 8)")
    force: bool = Field(False, description="是否忽略本地已下载历史记录并强制重新下载")


class DownloadRangeRequest(BaseModel):
    channel_identifier: str = Field(..., description="频道 Username 或 ID (-100xxx)")
    start_id: int = Field(..., ge=1, description="起始消息 ID")
    end_id: int = Field(..., ge=1, description="结束消息 ID")
    account_id: int | None = Field(None, description="指定执行的 Userbot 账号 ID (可选)")
    priority: int = Field(10, description="任务优先级 (数值越大优先级越高，默认 10)")
    threads: int = Field(8, ge=1, le=32, description="并发下载通道/线程数 (推荐 8 或 16，默认 8)")
    force: bool = Field(False, description="是否忽略本地已下载历史记录并强制重新下载")


class DownloadTaskConflictDetail(BaseModel):
    code: Literal["duplicate_download_task"]
    task_id: str
    status: Literal["pending", "queued", "running", "paused"]
    message: str


class AlreadyDownloadedDetail(BaseModel):
    code: Literal["already_downloaded"]
    message: str
    task_id: str | None = None
    file_count: int = 0
    file_names: list[str] = Field(default_factory=list)


class DownloadTaskConflictResponse(BaseModel):
    detail: DownloadTaskConflictDetail | AlreadyDownloadedDetail


class TaskRetryConflictResponse(BaseModel):
    detail: DownloadTaskConflictDetail | str


class PrecheckAccessRequest(BaseModel):
    chat_identifier: str = Field(..., description="频道/群组 Username、ID 或消息链接")
    account_id: int | None = Field(None, description="拟执行的 Userbot 账号 ID (可选，缺省时检测默认账号)")


class PrecheckAccessResponse(BaseModel):
    accessible: bool = Field(..., description="目标频道对于该账号是否可见可访问")
    chat_id: str | None = Field(None, description="解析后的 Chat ID")
    title: str | None = Field(None, description="频道或群组标题")
    username: str | None = Field(None, description="频道或群组用户名")
    is_restricted: bool = Field(False, description="是否受限保护内容")
    warning: str | None = Field(None, description="警告提示")
    reason: str | None = Field(None, description="不可访问原因或错误明细")


class InspectDownloadRangeRequest(BaseModel):
    channel_identifier: str = Field(..., description="频道 Username 或 ID (-100xxx) 或链接")
    start_id: int = Field(..., ge=1, description="起始消息 ID")
    end_id: int = Field(..., ge=1, description="结束消息 ID")


class InspectDownloadRangeResponse(BaseModel):
    channel_identifier: str
    parsed_chat_id: str
    channel_title: str
    channel_username: str | None = None
    is_accessible: bool
    is_restricted: bool = False
    channel_type: str = "Channel"
    start_id: int
    end_id: int
    total_range_messages: int
    messages_with_media: int
    text_only_messages: int
    photos_count: int
    videos_count: int
    audios_count: int
    documents_count: int
    estimated_size_bytes: int
    estimated_size_mb: float
    estimated_size_gb: float
    disk_free_bytes: int = 0
    disk_free_gb: float = 0.0
    disk_sufficient: bool = True
    already_downloaded_count: int
    all_media_downloaded: bool = Field(False, description="预检完整且所有媒体均满足本地文件与相册完成校验")
    history_check_complete: bool = Field(False, description="是否完整检查了区间内各批次的消息及本地下载历史")
    warning_messages: list[str] = []
    error_detail: str | None = None
    transit_capable: bool = False
    transit_notice: str | None = None


class UpdateTaskConcurrencyRequest(BaseModel):
    concurrency: int = Field(..., ge=1, le=32, description="新的并发通道数 (1-32，上传任务上限 16)")
    upload_in_flight_per_connection: int | None = Field(
        None,
        ge=1,
        le=4,
        validation_alias=AliasChoices("upload_in_flight_per_connection", "upload_in_flight"),
        description="单连接在途分块数规范契约 (1-4，上传任务支持)",
    )


class TaskProgressDTO(BaseModel):
    processed_bytes: int = 0
    total_bytes: int = 0
    processed_items: int = 0
    total_items: int = 0
    percent: float = 0.0
    speed_mb_s: float = 0.0
    eta_s: float = 0.0
    current_item: str | None = ""
    concurrency: int = 0
    concurrency_total: int = 0
    dc_id: int = 0
    pikpak_download_speed_mb_s: float | None = 0.0
    flood_wait_seconds: int | None = None


class TaskResponse(BaseModel):
    """任务 API 契约 (单轨制): 进度元数据仅存在于嵌套 progress (TaskProgressDTO),
    顶层扁平影子字段已收口删除。仓储层产出 dict 中残留的扁平物理列在 pydantic 校验时
    按 extra=ignore 语义丢弃; 扁平列 → 嵌套 progress 的装配单点在仓储层 _hydrate_task_dict。"""

    id: str
    task_type: str
    status: str
    progress: TaskProgressDTO
    account_id: int | None = None
    priority: int = 10
    retry_count: int = 0
    max_retries: int = 3
    next_retry_at: float = 0.0
    error_detail: str | None = ""
    params: dict[str, Any] | None = {}
    log_count: int = 0
    last_log_seq: int = 0
    created_at: float
    updated_at: float


class TaskListResponse(BaseModel):
    total: int
    page: int
    limit: int
    items: list[TaskResponse]
    status_counts: dict[str, int] = Field(
        ...,
        description="全库级状态统计 (含 all 总计与全部状态枚举键, 不受当前筛选参数影响, 反映全局队列真实水位)",
    )


class BatchTaskActionRequest(BaseModel):
    action: str = Field(..., description="操作类型: pause | resume | cancel | delete | retry")
    task_ids: list[str] = Field(..., min_length=1, max_length=200, description="任务 ID 列表")
    delete_files: bool | None = Field(False, description="删除操作时是否同时清理关联断点文件")


class BatchTaskActionResult(BaseModel):
    task_id: str
    success: bool
    status: str | None = None
    error: str | None = None


class BatchTaskActionResponse(BaseModel):
    action: str
    total: int
    success_count: int
    results: list[BatchTaskActionResult]


class QueuePauseResponse(BaseModel):
    status: str = "ok"
    paused_count: int
    message: str


class QueueResumeConflict(BaseModel):
    task_id: str = Field(description="因同键冲突保持暂停的任务 ID")
    conflict: DownloadTaskConflictDetail


class QueueResumeResponse(BaseModel):
    status: str = "ok"
    resumed_count: int
    message: str
    skipped_conflicts: list[QueueResumeConflict] = Field(default_factory=list)


class TaskStatusResponse(BaseModel):
    status: str


class TaskResumeResponse(BaseModel):
    status: str
    message: str
    queued_first: bool | None = None


class TaskDeleteResponse(BaseModel):
    status: str = "deleted"
    files_deleted: list[str] = []
    files_skipped: list[dict[str, Any]] = []


class TaskConcurrencyResponse(BaseModel):
    task_id: str
    concurrency: int
    upload_in_flight_per_connection: int | None = None
    status: str = "updated"


class TaskLogsIncrementalResponse(BaseModel):
    task_id: str
    logs: list[dict[str, Any]]
    last_seq: int
    has_more: bool


class SkipCurrentFileRequest(BaseModel):
    file_id: str | None = Field(None, description="拟跳过的特定远端文件 ID (可选，缺省时自动识别当前正在处理的文件)")
    reason: str | None = Field("用户手动跳过", description="跳过原因说明")
