import logging

from fastapi import APIRouter, HTTPException, Query

from app.core.log_setup import ring_buffer_handler
from app.models.logs import SystemLogItem, SystemLogsResponse, TaskLogItem, TaskLogsResponse
from app.services.task_console_service import TaskConsoleService

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/tasks/{task_id}", response_model=TaskLogsResponse)
async def get_task_logs(
    task_id: str,
    after_seq: int = Query(0, ge=0, description="从指定 seq 之后增量拉取"),
    limit: int = Query(200, ge=1, le=2000, description="单次拉取最大日志条数"),
    level: str | None = Query(None, description="日志级别过滤: info | warning | error"),
    query: str | None = Query(None, description="搜索日志文本关键词"),
):
    """基于 seq 的增量日志拉取与过滤检索 (过滤能力已并入仓储 get_task_logs, 查询走工作线程)"""
    result = await TaskConsoleService.get_task_logs_async(
        task_id, after_seq=after_seq, limit=limit, level=level, query=query
    )
    if result is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    rows, last_seq, has_more = result

    items = [
        TaskLogItem(
            seq=r["seq"],
            time=r["time"],
            level=r.get("level", "info"),
            message=r["message"],
            created_at=r.get("created_at"),
        )
        for r in rows
    ]

    return TaskLogsResponse(task_id=task_id, logs=items, last_seq=last_seq, has_more=has_more, total_count=len(items))


@router.get("/system", response_model=SystemLogsResponse)
async def get_system_logs(
    limit: int = Query(100, ge=1, le=500, description="最大拉取日志条数"),
    level: str | None = Query(None, description="日志级别过滤"),
    query: str | None = Query(None, description="搜索关键词"),
):
    """获取后端服务与调度器核心运行日志 (内存环形缓冲, 最近 2000 条, 新→旧)"""
    records = ring_buffer_handler.snapshot()
    records.reverse()  # 最新在前 (对齐原 task_logs 端点 ORDER BY id DESC 的语义)

    if level:
        records = [r for r in records if r["level"] == level.lower()]
    if query:
        records = [r for r in records if query in r["message"]]

    items = [
        SystemLogItem(
            timestamp=r["timestamp"],
            level=r["level"],
            logger=r["logger"],
            message=r["message"],
        )
        for r in records[: int(limit)]
    ]

    return SystemLogsResponse(total=len(items), logs=items)
