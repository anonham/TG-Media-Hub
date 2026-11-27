import asyncio
import json
import logging
import os
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.config import settings
from app.models.forward import (
    ForwardSkipItem,
    ForwardSkipsResponse,
)
from app.models.publish import (
    DownloadFailureItem,
    DownloadFailuresResponse,
    PublishFailureItem,
    PublishFailuresResponse,
)
from app.services.forward_console_service import ForwardConsoleService

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/forward-skips", response_model=ForwardSkipsResponse)
async def get_forward_skips(
    source_chat: str | None = Query(None, description="源频道 ID/Username"),
    target_chat: str | None = Query(None, description="目标频道 ID/Username"),
    skip_type: str | None = Query(None, description="跳过类型: bad_id | duplicate | filter"),
    task_id: str | None = Query(None, description="关联任务 ID"),
    page: int = Query(1, ge=1, description="页码"),
    limit: int = Query(50, ge=1, le=500, description="每页条数"),
):
    """分页多维查询转发清洗与坏消息隔离账本"""
    rows, total = await asyncio.to_thread(
        ForwardConsoleService.query_skips,
        source_chat=source_chat,
        target_chat=target_chat,
        skip_type=skip_type,
        task_id=task_id,
        page=page,
        limit=limit,
    )

    items = [
        ForwardSkipItem(
            id=r["id"],
            task_id=r.get("task_id"),
            source_chat=r["source_chat"],
            msg_id=r["msg_id"],
            skip_type=r["skip_type"],
            reason=r.get("reason"),
            target_chat=r.get("target_chat"),
            created_at=r["created_at"],
        )
        for r in rows
    ]

    return ForwardSkipsResponse(total=total, page=page, limit=limit, items=items)


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None:
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


def _read_failure_entries(
    filename: str,
    filter_type: str | None = None,
    filter_task: str | None = None,
    filter_chat: str | None = None,
) -> list[dict]:
    log_dir = getattr(settings, "LOG_DIR", os.path.join(settings.DATA_DIR, "logs"))
    base_path = os.path.join(log_dir, filename)

    # 包含历史滚动归档 (例如 download_failures.jsonl.3, .2, .1, download_failures.jsonl)
    # 按从旧到新的文件顺序读取，最终整体 reverse 即可得到严格的最新在前（倒序）
    candidate_files = []
    for i in range(3, 0, -1):
        arch_path = f"{base_path}.{i}"
        if os.path.exists(arch_path):
            candidate_files.append(arch_path)
    if os.path.exists(base_path):
        candidate_files.append(base_path)

    if not candidate_files:
        return []

    entries = []
    for file_path in candidate_files:
        try:
            with open(file_path, encoding="utf-8", errors="replace") as f:
                for line in f:
                    line_str = line.strip()
                    if not line_str:
                        continue
                    try:
                        data = json.loads(line_str)
                        if not isinstance(data, dict):
                            logger.debug("跳过非字典格式的审计日志行 (%s)", os.path.basename(file_path))
                            continue
                        err_type = data.get("type", "OTHER_ERROR")
                        data["retryable"] = err_type in ("NETWORK_TIMEOUT", "FLOOD_WAIT", "OTHER_ERROR")
                        # 流式早裁决: 不匹配的行即刻丢弃, 避免全量 dict 驻留内存
                        if filter_type and str(data.get("type", "")).strip().upper() != filter_type:
                            continue
                        if filter_task and str(data.get("task_id", "")).strip() != filter_task:
                            continue
                        if filter_chat and str(data.get("chat_id", "")).strip() != filter_chat:
                            continue
                        entries.append(data)
                    except (json.JSONDecodeError, TypeError, ValueError, RecursionError) as parse_err:
                        logger.debug("跳过格式异常的审计日志行 (%s): %s", os.path.basename(file_path), parse_err)
                        continue
        except FileNotFoundError:
            # 轮转竞态: exists 探测与 open 之间文件被日志轮转移走属预期, 跳过
            continue
        except OSError as read_err:
            # 非消失类读失败 (权限/IO) 不能静默吞成 "空结果" —— 显式报错, 杜绝
            # 200 + 不完整记录 + 虚低 total 的误导
            logger.warning("审计日志读取失败: %s (%s)", file_path, read_err)
            raise HTTPException(
                status_code=500, detail=f"审计日志读取失败: {os.path.basename(file_path)}"
            ) from read_err

    entries.reverse()
    return entries


@router.get("/publish-failures", response_model=PublishFailuresResponse)
async def get_publish_failures(
    type: str | None = Query(
        None, description="失败异常分类 (FLOOD_WAIT | NETWORK_TIMEOUT | FILE_TOO_LARGE | OTHER_ERROR)"
    ),
    task_id: str | None = Query(None, description="关联任务 ID"),
    chat_id: str | None = Query(None, description="目标频道 ID"),
    page: int = Query(1, ge=1, description="页码"),
    limit: int = Query(50, ge=1, le=500, description="每页条数"),
):
    """结构化解析并查询 publish_failures.jsonl 失败审计日志"""
    filter_type = type.strip().upper() if type else None
    filter_task = task_id.strip() if task_id else None
    filter_chat = str(chat_id).strip() if chat_id else None

    filtered = await asyncio.to_thread(
        _read_failure_entries, "publish_failures.jsonl", filter_type, filter_task, filter_chat
    )

    total = len(filtered)
    offset = max(0, (page - 1) * limit)
    paged = filtered[offset : offset + limit]

    items = [
        PublishFailureItem(
            time=str(r.get("time") or ""),
            task_id=r.get("task_id") or None,
            chat_id=str(r.get("chat_id", "")),
            file=str(r.get("file") or ""),
            size_mb=_safe_float(r.get("size_mb"), 0.0),
            client=str(r.get("client") or "Unknown"),
            type=str(r.get("type") or "OTHER_ERROR"),
            reason=str(r.get("reason") or ""),
            retryable=bool(r.get("retryable", False)),
        )
        for r in paged
    ]

    return PublishFailuresResponse(total=total, page=page, limit=limit, items=items)


@router.get("/download-failures", response_model=DownloadFailuresResponse)
async def get_download_failures(
    type: str | None = Query(
        None,
        description="失败异常分类 (FLOOD_WAIT | NETWORK_TIMEOUT | FILE_TOO_LARGE | FILE_IO_ERROR | USER_CANCELLED | TG_NO_PERMISSION | OTHER_ERROR)",
    ),
    task_id: str | None = Query(None, description="关联任务 ID"),
    chat_id: str | None = Query(None, description="目标频道 ID"),
    page: int = Query(1, ge=1, description="页码"),
    limit: int = Query(50, ge=1, le=500, description="每页条数"),
):
    """结构化解析并查询 download_failures.jsonl 失败审计日志"""
    filter_type = type.strip().upper() if type else None
    filter_task = task_id.strip() if task_id else None
    filter_chat = str(chat_id).strip() if chat_id else None

    filtered = await asyncio.to_thread(
        _read_failure_entries, "download_failures.jsonl", filter_type, filter_task, filter_chat
    )

    total = len(filtered)
    offset = max(0, (page - 1) * limit)
    paged = filtered[offset : offset + limit]

    items = [
        DownloadFailureItem(
            time=str(r.get("time") or ""),
            task_id=r.get("task_id") or None,
            chat_id=str(r.get("chat_id", "")),
            file=str(r.get("file") or ""),
            size_mb=_safe_float(r.get("size_mb"), 0.0),
            client=str(r.get("client") or "download"),
            type=str(r.get("type") or "OTHER_ERROR"),
            reason=str(r.get("reason") or ""),
            retryable=bool(r.get("retryable", False)),
        )
        for r in paged
    ]

    return DownloadFailuresResponse(total=total, page=page, limit=limit, items=items)
