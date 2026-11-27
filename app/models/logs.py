from pydantic import BaseModel


class TaskLogItem(BaseModel):
    seq: int
    time: str
    level: str = "info"
    message: str
    created_at: float | None = None


class TaskLogsResponse(BaseModel):
    task_id: str
    logs: list[TaskLogItem]
    last_seq: int
    has_more: bool
    total_count: int | None = None


class SystemLogItem(BaseModel):
    timestamp: str
    level: str = "info"
    logger: str = "system"
    message: str


class SystemLogsResponse(BaseModel):
    total: int
    logs: list[SystemLogItem]
