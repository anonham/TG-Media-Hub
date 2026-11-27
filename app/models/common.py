from pydantic import BaseModel, Field


class StatusOkResponse(BaseModel):
    status: str = Field("ok", description="操作状态")
    message: str | None = Field(None, description="说明消息")


class TaskActionResponse(BaseModel):
    status: str = Field("ok", description="操作状态")
    task_id: str = Field(..., description="任务 ID")
    message: str | None = Field(None, description="说明消息")
