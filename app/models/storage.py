from pydantic import BaseModel, Field, field_validator


class StorageDeleteRequest(BaseModel):
    paths: list[str] = Field(..., min_length=1, max_length=200, description="待删除文件的绝对/相对路径列表")


class FileItemResponse(BaseModel):
    file_name: str
    rel_path: str
    abs_path: str
    path: str | None = None
    size: int | None = None
    size_bytes: int
    size_mb: float
    mtime: float
    mime_type: str
    category: str  # 'video' | 'image' | 'audio' | 'document' | 'parts' | 'other'
    task_id: str | None = None
    channel: str | None = None
    paired: str = "na"
    file_hash: str | None = None


class FileListResponse(BaseModel):
    total: int
    page: int
    limit: int
    files: list[FileItemResponse]


class BreakpointItemResponse(BaseModel):
    part_path: str
    meta_path: str
    final_path: str
    task_id: str | None = None
    chat_id: str | None = None
    msg_id: int | None = None
    part_size_bytes: int = 0
    part_size_mb: float = 0.0
    paired_status: str  # 'ok' | 'missing_meta' | 'missing_part'
    created_at: float


class BreakpointListResponse(BaseModel):
    total: int
    orphan_count: int
    paired_count: int
    items: list[BreakpointItemResponse]


class CleanOrphansResponse(BaseModel):
    status: str
    cleaned_count: int
    freed_bytes: int
    freed_mb: float
    cleaned_files: list[str]


class StorageQuotaUpdateRequest(BaseModel):
    quota_gb: float = Field(..., ge=0, allow_inf_nan=False, description="存储配额上限 (GB, 0 表示不设上限按物理磁盘)")

    @field_validator("quota_gb", mode="before")
    @classmethod
    def _reject_boolean(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("quota_gb must be a number, not a boolean")
        return value


class StorageMetricsResponse(BaseModel):
    disk_total_bytes: int
    disk_used_bytes: int
    disk_free_bytes: int
    disk_free_gb: float
    disk_used_percent: float
    downloads_bytes: int
    downloads_mb: float
    db_size_bytes: int
    logs_size_bytes: int
    total_files_count: int
    active_downloads_locked: bool = False
    max_storage_quota_gb: float = 0.0
    max_storage_quota_bytes: int = 0
    is_quota_active: bool = False
    effective_free_bytes: int
    effective_free_gb: float
    quota_used_percent: float = 0.0
    # 前端向前兼容别名
    total_bytes: int | None = None
    free_bytes: int | None = None
    used_bytes: int | None = None
    percent_used: float | None = None


class StorageOverviewByChannelItem(BaseModel):
    chat_id: str | None = None
    count: int = 0
    bytes: int = 0


class StorageOverviewResponse(BaseModel):
    download_dir: str
    total_bytes: int
    part_bytes: int
    final_bytes: int
    file_count: int
    final_count: int
    part_count: int
    orphan_part_count: int
    orphan_meta_count: int
    unregistered_final_count: int
    by_channel: list[StorageOverviewByChannelItem] = []
    scanned_at: float | None = None
    disk_total_bytes: int
    disk_used_bytes: int
    disk_free_bytes: int
    disk_free_gb: float
    disk_used_percent: float
    max_storage_quota_gb: float = 0.0
    max_storage_quota_bytes: int = 0
    is_quota_active: bool = False
    effective_free_bytes: int = 0
    effective_free_gb: float = 0.0
    quota_used_percent: float = 0.0


class StorageDeleteSkippedItem(BaseModel):
    path: str
    reason: str


class StorageDeleteResponse(BaseModel):
    deleted: list[str] = []
    skipped: list[StorageDeleteSkippedItem] = []


class StorageQuotaResponse(BaseModel):
    status: str = "ok"
    max_storage_quota_gb: float
    message: str
