import uuid
from typing import Any, Literal

from fastapi import APIRouter, HTTPException

from app.config import settings
from app.core.scheduler import task_scheduler
from app.models.common import TaskActionResponse
from app.models.task import (
    DownloadLinkRequest,
    DownloadRangeRequest,
    DownloadTaskConflictResponse,
    InspectDownloadRangeRequest,
    InspectDownloadRangeResponse,
)
from app.services.downloader_service import DownloaderService
from app.services.system_settings_service import SystemSettingsService
from app.services.task_console_service import TaskConsoleService

router = APIRouter()
_DOWNLOAD_CONFLICT_RESPONSE: dict[int | str, dict[str, Any]] = {
    409: {"model": DownloadTaskConflictResponse, "description": "已有活跃的相同下载任务或目标文件均已下载"},
}


@router.post("/inspect-range", response_model=InspectDownloadRangeResponse)
async def inspect_download_range(req: InspectDownloadRangeRequest):
    if req.end_id < req.start_id:
        raise HTTPException(status_code=400, detail="结束消息 ID 必须大于等于起始 ID")

    try:
        data = await DownloaderService.inspect_download_range(
            channel_identifier=req.channel_identifier, start_id=req.start_id, end_id=req.end_id
        )
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"预检频道消息范围失败: {e}") from e


async def _submit_download(
    req: DownloadLinkRequest | DownloadRangeRequest,
    task_type: Literal["download_link", "download_range"],
    task_params: dict[str, Any],
) -> TaskActionResponse:
    task_id = str(uuid.uuid4())[:8]
    default_threads = SystemSettingsService.get_int(
        "download_parallel_connections", getattr(settings, "DOWNLOAD_PARALLEL_CONNECTIONS", 8)
    )
    explicit_threads = req.threads if "threads" in req.model_fields_set else None
    if explicit_threads is not None:
        task_params["threads"] = explicit_threads
    if req.force:
        task_params["force"] = True

    conflict = await TaskConsoleService.create_download_task(
        task_id=task_id,
        task_type=task_type,
        params=task_params,
        account_id=req.account_id,
        priority=req.priority if req.priority is not None else 10,
        concurrency_total=explicit_threads or default_threads,
        force=req.force,
    )
    if conflict is not None:
        raise HTTPException(status_code=409, detail=conflict.model_dump())
    task_scheduler.notify_new_task()
    return TaskActionResponse(task_id=task_id)


@router.post("/link", response_model=TaskActionResponse, responses=_DOWNLOAD_CONFLICT_RESPONSE)
async def download_links(req: DownloadLinkRequest):
    lines = [line.strip() for line in req.links.split("\n") if line.strip()]
    if not lines:
        raise HTTPException(status_code=400, detail="请输入至少一条有效的 Telegram 链接")
    return await _submit_download(req, "download_link", {"links": lines, "links_count": len(lines)})


@router.post("/range", response_model=TaskActionResponse, responses=_DOWNLOAD_CONFLICT_RESPONSE)
async def download_range(req: DownloadRangeRequest):
    if req.end_id < req.start_id:
        raise HTTPException(status_code=400, detail="结束消息 ID 必须大于等于起始 ID")
    return await _submit_download(
        req,
        "download_range",
        {"channel": req.channel_identifier, "start_id": req.start_id, "end_id": req.end_id},
    )
