from typing import Any

from pydantic import BaseModel, Field


class ForwardChannelRequest(BaseModel):
    source_chat: str = Field(..., description="源频道/群组 Username、ID 或消息链接")
    target_chat: str = Field(..., description="目标私密频道/群组 ID/Username (必填)")
    start_id: int | None = Field(None, ge=1, description="起始消息 ID（选填）")
    end_id: int | None = Field(None, ge=1, description="结束消息 ID（选填）")
    batch_size: int = Field(50, ge=10, le=100, description="单批转发数量 (30/50/80/100)")
    delay_seconds: float = Field(1.5, ge=0.8, description="每批次发包延迟 (秒)")
    full_retransfer: bool = Field(False, description="全量重转模式: 跳过防重剔除，目标频道将出现重复消息")
    dry_run: bool = Field(False, description="试转模式: 首个批次转发完成后自动暂停，验收后手动恢复全量")
    account_id: int | None = Field(None, description="指定执行的 Userbot 账号 ID (可选)")
    priority: int | None = Field(10, description="任务优先级 (数值越大优先级越高，默认 10)")


class InspectChannelRequest(BaseModel):
    source_chat: str = Field(..., description="源频道 Username、ID 或消息链接")
    account_id: int | None = Field(None, description="指定探测的 Userbot 账号 ID (可选)")


class ForwardEstimateRequest(BaseModel):
    source_chat: str = Field(..., description="源频道/群组 Username、ID 或消息链接")
    target_chat: str = Field(..., description="目标私密频道/群组 ID/Username")
    start_id: int | None = Field(None, ge=1, description="起始消息 ID（选填，缺省时实测频道范围）")
    end_id: int | None = Field(None, ge=1, description="结束消息 ID（选填，缺省时实测频道范围）")
    batch_size: int = Field(50, ge=1, le=100, description="单批转发数量，用于估算批次数")


class ForwardSkipItem(BaseModel):
    id: int
    task_id: str | None = None
    source_chat: str
    msg_id: int
    skip_type: str
    reason: str | None = None
    target_chat: str | None = None
    created_at: float


class ForwardSkipsResponse(BaseModel):
    total: int
    page: int
    limit: int
    items: list[ForwardSkipItem]


class ForwardInspectResponse(BaseModel):
    status: str = "success"
    data: dict[str, Any]


class ForwardCreateTaskResponse(BaseModel):
    status: str = "success"
    message: str = "云端极速转发任务已成功创建并提交处理"
    task_id: str


class ForwardHistoryResponse(BaseModel):
    status: str = "success"
    count: int
    data: list[dict[str, Any]]


class ForwardEstimateData(BaseModel):
    source_chat: str
    target_chat: str
    start_id: int
    end_id: int
    span_count: int
    already_processed: int
    to_process: int
    est_batches: int
    inspected_total: int | None = None


class ForwardEstimateResponse(BaseModel):
    status: str = "success"
    data: ForwardEstimateData


class ForwardReceiptData(BaseModel):
    task_id: str
    task_status: str | None = None
    forwarded: int
    filtered: int
    bad: int
    dedup_skipped: int | None = None
    bad_ids: list[int] = []
    filtered_detail: list[dict[str, Any]] = []


class ForwardReceiptResponse(BaseModel):
    status: str = "success"
    data: ForwardReceiptData


class ForwardRunInterval(BaseModel):
    start: int
    end: int
    count: int | None = None


class ForwardCoverageData(BaseModel):
    source_chat: str
    target_chat: str
    range: dict[str, int] | None = None
    covered: list[dict[str, int]] = []
    forwarded_runs: list[ForwardRunInterval] = []
    filtered_runs: list[ForwardRunInterval] = []
    bad_runs: list[ForwardRunInterval] = []
    stats: dict[str, int] = {}
    truncated: bool = False


class ForwardCoverageResponse(BaseModel):
    status: str = "success"
    data: ForwardCoverageData


class ForwardGapItem(BaseModel):
    start: int
    end: int
    est_count: int


class ForwardCoverageGapsData(BaseModel):
    window: dict[str, int] | None = None
    gaps: list[ForwardGapItem] = []
    total_est_count: int = 0


class ForwardCoverageGapsResponse(BaseModel):
    status: str = "success"
    data: ForwardCoverageGapsData
