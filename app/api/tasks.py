import logging
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query

from app.core.scheduler import task_scheduler
from app.models.common import TaskActionResponse
from app.models.task import (
    BatchTaskActionRequest,
    BatchTaskActionResponse,
    PrecheckAccessRequest,
    PrecheckAccessResponse,
    QueuePauseResponse,
    QueueResumeResponse,
    SkipCurrentFileRequest,
    TaskConcurrencyResponse,
    TaskDeleteResponse,
    TaskListResponse,
    TaskLogsIncrementalResponse,
    TaskResponse,
    TaskResumeResponse,
    TaskRetryConflictResponse,
    TaskStatusResponse,
    UpdateTaskConcurrencyRequest,
)
from app.services.task_console_service import TaskConsoleService
from app.services.task_lifecycle_service import TaskLifecycleService, prepare_task_resume

logger = logging.getLogger(__name__)

router = APIRouter()
_TASK_REQUEUE_CONFLICT: dict[int | str, dict[str, Any]] = {
    409: {"model": TaskRetryConflictResponse, "description": "任务状态冲突或已有活跃的相同下载任务"}
}


async def _prepare_task_resume(task: dict[str, Any]) -> dict[str, Any]:
    """向后兼容别名 (委托至 TaskLifecycleService / prepare_task_resume)"""
    return await prepare_task_resume(task)


@router.post("/precheck-access", response_model=PrecheckAccessResponse)
async def precheck_channel_access(req: PrecheckAccessRequest):
    """[REQ-005] 发布任务时轻量探测指定账号或全账号池对目标频道/群组的访问权限"""
    return await TaskLifecycleService.precheck_access(req)


@router.get("", response_model=TaskListResponse | list[TaskResponse])
async def get_tasks(
    status: str | None = Query(
        None,
        pattern=r"^(pending|queued|running|paused|completed|failed|cancelled)$",
        description="任务状态过滤 (running/queued/paused/completed/failed/cancelled)",
    ),
    task_type: str | None = Query(
        None,
        pattern=r"^(download_link|download_range|forward_channel|publish_folder|pikpak_publish)$",
        description="任务类型过滤",
    ),
    account_id: int | None = Query(None, ge=0, description="指定 Userbot 账号 ID 过滤"),
    query: str | None = Query(None, max_length=100, description="关键词搜索"),
    page: int | None = Query(None, ge=1, description="页码 (传入时开启分页响应模式)"),
    limit: int = Query(50, ge=1, le=500, description="每页条数"),
):
    """获取任务列表 (支持按状态、类型、账号、关键词组合筛选及服务端分页)"""
    if page is not None or status or task_type or account_id is not None or query:
        p = page if page is not None else 1
        items, total = await TaskConsoleService.query_tasks_async(
            status=status, task_type=task_type, account_id=account_id, query=query, page=p, limit=limit
        )
        if page is not None:
            status_counts = await TaskConsoleService.count_by_status_async()
            return TaskListResponse(total=total, page=p, limit=limit, items=items, status_counts=status_counts)
        return items

    # 默认无过滤参数且未传 page 时返回列表 (100% 向后兼容存量用例与前端)
    return await TaskConsoleService.get_all_async(limit)


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(task_id: str):
    """获取单个任务详情与参数元数据 (用于全景抽屉与深层链接)"""
    task = await TaskConsoleService.get_by_id_async(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    return task


@router.get("/{task_id}/logs", response_model=TaskLogsIncrementalResponse)
async def get_task_logs(task_id: str, after_seq: int = 0, limit: int = 200):
    """增量拉取任务日志 (按 seq 差量, 轮询兜底与 WS 断连补洞共用此口径)"""
    result = await TaskConsoleService.get_task_logs_async(task_id, after_seq=after_seq, limit=min(max(limit, 1), 2000))
    if result is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    logs, last_seq, has_more = result
    return {"task_id": task_id, "logs": logs, "last_seq": last_seq, "has_more": has_more}


@router.post("/queue/pause", response_model=QueuePauseResponse)
async def pause_queue():
    """全局队列控制：暂停整个下载/处理队列，彻底清空当前所有网络传输通道"""
    return await TaskLifecycleService.pause_queue()


@router.post("/queue/resume", response_model=QueueResumeResponse)
async def resume_queue():
    """全局队列控制：一键恢复整个队列，将所有暂停任务置为 queued 进入自动调度"""
    return await TaskLifecycleService.resume_queue(sched=task_scheduler)


@router.post("/{task_id}/pause", response_model=TaskStatusResponse)
async def pause_task(task_id: str):
    return await TaskLifecycleService.pause_task(task_id, sched=task_scheduler)


@router.post("/{task_id}/resume", response_model=TaskResumeResponse, responses=_TASK_REQUEUE_CONFLICT)
async def resume_task(
    task_id: str,
    mode: Annotated[str, Query(description="恢复模式: normal (原速) | steady (低速稳态)")] = "normal",
):
    return await TaskLifecycleService.resume_task(task_id, mode=mode, sched=task_scheduler)


@router.post("/{task_id}/steady-resume", response_model=TaskResumeResponse, responses=_TASK_REQUEUE_CONFLICT)
async def steady_resume_task(task_id: str):
    """[Milestone 4] 专属低速稳态恢复接口: 锁定最低安全档位 (单预取通道/单并发/20s发送延迟/金丝雀单块模式)"""
    return await TaskLifecycleService.resume_task(task_id, mode="steady", sched=task_scheduler)


@router.post("/{task_id}/skip-current", response_model=TaskActionResponse)
async def skip_current_file(task_id: str, req: SkipCurrentFileRequest | None = None):
    """[Milestone 4] 跳过当前异常文件并在目标频道留置 ⏳ 待回填占位卡，支持继续处理后续文件"""
    return await TaskLifecycleService.skip_current_file(task_id, req=req)


@router.post(
    "/{task_id}/retry",
    response_model=TaskActionResponse,
    responses=_TASK_REQUEUE_CONFLICT,
)
async def retry_task(task_id: str):
    """针对 failed / cancelled / paused 任务的一键重试与重调度接口"""
    return await TaskLifecycleService.retry_task(task_id, sched=task_scheduler)


@router.post("/{task_id}/cancel", response_model=TaskStatusResponse)
async def cancel_task(task_id: str):
    return await TaskLifecycleService.cancel_task(task_id)


@router.delete("/{task_id}", response_model=TaskDeleteResponse)
async def delete_task(task_id: str, delete_files: bool = False):
    return await TaskLifecycleService.delete_task(task_id, delete_files=delete_files)


@router.post("/batch", response_model=BatchTaskActionResponse)
async def batch_task_action(req: BatchTaskActionRequest):
    """批量任务管理接口 (pause | resume | cancel | delete | retry)"""
    return await TaskLifecycleService.batch_action(req, sched=task_scheduler)


@router.delete("", response_model=TaskStatusResponse)
async def clear_finished_tasks():
    return await TaskLifecycleService.clear_finished_tasks()


@router.post("/{task_id}/concurrency", response_model=TaskConcurrencyResponse)
@router.patch("/{task_id}/concurrency", response_model=TaskConcurrencyResponse)
async def update_task_concurrency(task_id: str, req: UpdateTaskConcurrencyRequest):
    """动态调整运行中或排队中任务的并发数与在途深度 (支持实时扩缩容)"""
    return await TaskLifecycleService.update_task_concurrency(task_id, req, sched=task_scheduler)
