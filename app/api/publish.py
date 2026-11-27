import asyncio
import uuid

from fastapi import APIRouter, HTTPException

from app.config import settings
from app.core.path_utils import validate_safe_folder_path as _validate_safe_folder_path
from app.core.scheduler import task_scheduler
from app.models.common import TaskActionResponse
from app.models.publish import InspectFolderRequest, InspectFolderResponse, PublishFolderRequest
from app.services.publisher_service import PublisherService
from app.services.system_settings_service import SystemSettingsService
from app.services.task_console_service import TaskConsoleService


def validate_safe_folder_path(folder_path: str) -> str:
    """本地路径安全边界防护 (SEC-001 Path Traversal Guard):
    调用核心域路径校验，并将 ValueError 转译为 FastAPI 400 异常。
    """
    try:
        return _validate_safe_folder_path(folder_path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


router = APIRouter()


@router.post("/inspect", response_model=InspectFolderResponse)
async def inspect_folder(req: InspectFolderRequest):
    safe_path = validate_safe_folder_path(req.folder_path)

    try:
        from app.core.client_manager import client_manager
        from app.services.channel_inspector import ChannelInspector

        # 动态探测所选账号 (或全局引擎) 的 Premium 身份与上传上限 (FR3)
        if req.account_id:
            await client_manager.probe_premium(account_id=req.account_id)
            acc = client_manager.get_account(req.account_id)
            account_premium = acc.is_premium if acc else False
            effective_limit = acc.effective_max_file_size_bytes() if acc else settings.MAX_FILE_SIZE_BYTES
        else:
            await client_manager.refresh_premium()
            account_premium = bool(client_manager.is_premium)
            effective_limit = client_manager.effective_max_file_size_bytes()

        data = await asyncio.to_thread(
            PublisherService.inspect_folder,
            safe_path,
            target_channel=req.target_channel,
            account_premium=account_premium,
            effective_limit_bytes=effective_limit,
        )

        # 若提供了 target_channel，附带执行频道发帖写权限诊断探针
        if req.target_channel:
            try:
                channel_info = await ChannelInspector.inspect_channel(
                    channel_identifier=req.target_channel, preset_type="target", account_id=req.account_id
                )
                data["target_channel_info"] = channel_info
            except Exception as ex:
                import logging

                logging.getLogger(__name__).warning("预检附带的目标频道发帖权限探测失败: %s", ex)

        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"扫描文件夹失败: {e}") from e


@router.post("/folder", response_model=TaskActionResponse)
async def publish_folder(req: PublishFolderRequest):
    safe_path = validate_safe_folder_path(req.folder_path)

    task_id = str(uuid.uuid4())[:8]
    task_params = {
        "folder_path": safe_path,
        "target_channel": req.target_channel,
        "album_mode": req.album_mode,
        "delay_seconds": req.delay_seconds,
        "force_republish": req.force_republish,
        "oversize_policy": req.oversize_policy,
        "send_placeholder_card": req.send_placeholder_card,
    }
    if req.upload_concurrency is not None:
        task_params["upload_concurrency"] = req.upload_concurrency
    if req.upload_in_flight_per_connection is not None:
        task_params["upload_in_flight_per_connection"] = req.upload_in_flight_per_connection

    default_upload_concurrency = SystemSettingsService.get_int(
        "upload_parallel_connections", getattr(settings, "UPLOAD_PARALLEL_CONNECTIONS", 2)
    )
    user_upload_concurrency = (
        req.upload_concurrency if (req.upload_concurrency and 1 <= req.upload_concurrency <= 16) else None
    )
    upload_concurrency_for_display = user_upload_concurrency or default_upload_concurrency

    TaskConsoleService.create_task(
        task_id=task_id,
        task_type="publish_folder",
        params=task_params,
        account_id=req.account_id,
        priority=req.priority if req.priority is not None else 10,
        concurrency_total=upload_concurrency_for_display,
    )
    # D4: 派发统一 —— 任务落库后一律经调度器调度 (notify 唤醒即扫队列);
    # 原 BackgroundTasks 即跑兜底通道 (含 callable_supports_kwarg 探测形参 hack) 已废除。
    task_scheduler.notify_new_task()
    return {"status": "ok", "task_id": task_id}
