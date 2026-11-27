import asyncio
import logging
import math
import uuid
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from app.core.intervals import compute_gaps, merge_intervals, runs_from_ids
from app.core.scheduler import task_scheduler
from app.models.forward import (
    ForwardChannelRequest,
    ForwardCoverageGapsResponse,
    ForwardCoverageResponse,
    ForwardCreateTaskResponse,
    ForwardEstimateRequest,
    ForwardEstimateResponse,
    ForwardHistoryResponse,
    ForwardInspectResponse,
    ForwardReceiptResponse,
    InspectChannelRequest,
)
from app.services.forward_console_service import ForwardConsoleService
from app.services.forward_service import (
    ForwardService,
)
from app.services.task_console_service import TaskConsoleService

logger = logging.getLogger(__name__)

router = APIRouter()

# 覆盖条渲染的每类 run 段数量上限 (超出截断并标记 truncated)
MAX_RENDER_RUNS = 2000

# 序列化同一时刻的 forward_channel 重复提交检查与入库操作，消除 TOCTOU 竞态
_forward_channel_submit_lock = asyncio.Lock()


@router.post("/inspect", response_model=ForwardInspectResponse)
async def inspect_channel(req: InspectChannelRequest):
    try:
        info = await ForwardService.inspect_channel_range(req.source_chat, account_id=req.account_id)
        return {"status": "success", "data": info}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"测算源频道消息范围失败: {e}") from e


def _task_peer_keys(raw_val: str, canonical_val: object = None) -> set[str]:
    """构建任务源/目标的候选匹配键集 (含原始解析、规范别名与已钉住的规范 peer ID)."""
    from app.core.entity_resolver import normalize_peer_alias_key

    keys: set[str] = set()
    parsed = ForwardService.parse_chat_identifier(raw_val)
    if parsed not in (None, ""):
        keys.add(str(parsed))
    alias = normalize_peer_alias_key(raw_val)
    if alias:
        keys.add(alias)
    if canonical_val not in (None, ""):
        keys.add(str(canonical_val))
    return keys


@router.post("/channel", response_model=ForwardCreateTaskResponse)
async def forward_channel(req: ForwardChannelRequest):
    task_id = str(uuid.uuid4())[:8]
    params = {
        "source_chat": req.source_chat,
        "target_chat": req.target_chat,
        "start_id": req.start_id,
        "end_id": req.end_id,
        "batch_size": req.batch_size,
        "delay_seconds": req.delay_seconds,
        "full_retransfer": req.full_retransfer,
        "dry_run": req.dry_run,
    }

    # 并发防护: 锁内串行执行活跃对检查与任务落库，杜绝同源→同目标并发竞态创建双任务
    async with _forward_channel_submit_lock:
        active_rows = await asyncio.to_thread(TaskConsoleService.get_active_by_type, "forward_channel")
        for row in active_rows:
            p = row.get("params") or {}
            p_source = p.get("source_chat")
            p_target = p.get("target_chat")
            if not p_source or not p_target:
                continue
            try:
                same_source = bool(
                    _task_peer_keys(p_source, p.get("canonical_source")) & _task_peer_keys(req.source_chat)
                )
                same_target = bool(
                    _task_peer_keys(p_target, p.get("canonical_target")) & _task_peer_keys(req.target_chat)
                )
            except Exception:
                continue
            if same_source and same_target:
                raise HTTPException(
                    status_code=400,
                    detail=f"该源→目标已有运行中/暂停中的转发任务 ({row['id']})，请等待其完成或取消后再提交",
                )

        insert_task = asyncio.create_task(
            asyncio.to_thread(
                TaskConsoleService.create_task,
                task_id=task_id,
                task_type="forward_channel",
                params=params,
                account_id=req.account_id,
                priority=req.priority if req.priority is not None else 10,
            )
        )
        try:
            await asyncio.shield(insert_task)
        except BaseException:
            # 若请求在写库期间被取消，必须等待后台线程 INSERT 完成后再释放提交锁
            try:
                await insert_task
            except Exception as exc:
                logger.debug("取消后等待 forward_channel 入库任务结束异常: %s", exc)
            raise

    # D4: 派发统一 —— 任务落库后一律经调度器调度; 原 BackgroundTasks 即跑兜底通道已废除
    task_scheduler.notify_new_task()

    return {"status": "success", "message": "云端极速转发任务已成功创建并提交处理", "task_id": task_id}


@router.get("/history", response_model=ForwardHistoryResponse)
async def get_forward_history(
    source_chat: str | None = None,
    target_chat: str | None = None,
    source_msg_id: int | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
):
    offset = (page - 1) * limit
    if offset > 2**63 - 1:
        return {"status": "success", "count": 0, "data": []}

    def _query():
        parsed_source = ForwardService.canonicalize_chat_key(source_chat) if source_chat else None
        parsed_target = ForwardService.canonicalize_chat_key(target_chat) if target_chat else None
        return ForwardConsoleService.query_history(
            source_chat=parsed_source,
            target_chat=parsed_target,
            source_msg_id=source_msg_id,
            limit=limit,
            offset=offset,
        )

    rows = await asyncio.to_thread(_query)
    return {"status": "success", "count": len(rows), "data": rows}


def _parse_pair(source_chat: str, target_chat: str):
    parsed_source = ForwardService.canonicalize_chat_key(source_chat)
    parsed_target = ForwardService.canonicalize_chat_key(target_chat)
    return parsed_source, parsed_target


def _runs_payload(ids: list[int]) -> tuple:
    runs = runs_from_ids(ids)
    truncated = len(runs) > MAX_RENDER_RUNS
    payload = [{"start": s, "end": e, "count": e - s + 1} for s, e in runs[:MAX_RENDER_RUNS]]
    return payload, truncated


@router.post("/estimate", response_model=ForwardEstimateResponse)
async def estimate_forward(req: ForwardEstimateRequest):
    """提交前预估: 本次将处理约 N 条，其中 M 条已处理将跳过，预计 X 批"""
    start_id, end_id = req.start_id, req.end_id
    inspected_total = None
    try:
        parsed_source, parsed_target = _parse_pair(req.source_chat, req.target_chat)
        if start_id is None or end_id is None:
            info = await ForwardService.inspect_channel_range(req.source_chat)
            start_id = start_id if start_id is not None else info["min_id"]
            end_id = end_id if end_id is not None else info["max_id"]
            inspected_total = info["total_count"]
        if end_id < start_id:
            raise HTTPException(status_code=400, detail=f"结束 ID ({end_id}) 小于起始 ID ({start_id})")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"预估失败: {e}") from e

    span_count = end_id - start_id + 1
    already = await asyncio.to_thread(
        ForwardConsoleService.count_processed_in_range, parsed_source, parsed_target, start_id, end_id
    )
    to_process = max(0, span_count - already)
    est_batches = math.ceil(to_process / req.batch_size) if to_process > 0 else 0
    return {
        "status": "success",
        "data": {
            "source_chat": parsed_source,
            "target_chat": parsed_target,
            "start_id": start_id,
            "end_id": end_id,
            "span_count": span_count,
            "already_processed": already,
            "to_process": to_process,
            "est_batches": est_batches,
            "inspected_total": inspected_total,
        },
    }


@router.get("/receipt/{task_id}", response_model=ForwardReceiptResponse)
async def get_forward_receipt(task_id: str):
    """转发任务完成回执: 成功 X / 策略过滤 Y (明细) / 坏消息隔离 Z (ID 清单) / 防重跳过 W"""
    task = await TaskConsoleService.get_by_id_async(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    if task.get("task_type") != "forward_channel":
        raise HTTPException(status_code=400, detail="非转发任务无回执")

    agg = await asyncio.to_thread(ForwardConsoleService.get_task_receipt, task_id)
    final_summary = (task.get("params") or {}).get("final_summary") or {}
    filtered_total = sum(d["count"] for d in agg["filtered_detail"])
    return {
        "status": "success",
        "data": {
            "task_id": task_id,
            "task_status": task.get("status"),
            "forwarded": final_summary.get("forwarded", agg["forwarded"]),
            "filtered": final_summary.get("filtered", filtered_total),
            "bad": final_summary.get("bad", len(agg["bad_ids"])),
            "dedup_skipped": final_summary.get("dedup_skipped"),
            "bad_ids": agg["bad_ids"],
            "filtered_detail": agg["filtered_detail"],
        },
    }


def _empty_coverage_data(parsed_source: str, parsed_target: str) -> dict:
    return {
        "source_chat": parsed_source,
        "target_chat": parsed_target,
        "range": None,
        "covered": [],
        "forwarded_runs": [],
        "filtered_runs": [],
        "bad_runs": [],
        "stats": {"forwarded": 0, "filtered": 0, "bad": 0},
        "truncated": False,
    }


@router.get("/coverage", response_model=ForwardCoverageResponse)
async def get_forward_coverage(
    source_chat: str, target_chat: str, min_id: int | None = None, max_id: int | None = None
):
    """覆盖可视化: 服务端完成区间合并与 run 聚合，返回聚合段列表 (不返回逐条数据)"""
    parsed_source, parsed_target = _parse_pair(source_chat, target_chat)

    segs = ForwardConsoleService.get_coverage_segments(parsed_source, parsed_target)
    if min_id is not None and max_id is not None:
        w_min, w_max = min_id, max_id
    else:
        bounds: list[int] = []
        for s, e in segs:
            bounds.extend([s, e])
        # 任一端点缺失时合并账本数据边界 (含逐条记录，可能比 coverage 段更宽)。
        # SQL 标量聚合取边界 (P1): 全量 get_ledger_runs 会把数十万行账本拉进 Python 堆
        # 仅为求 min/max, 且紧随其后还会再查一次 —— 小内存 VPS 上单请求可分配数十 MB
        ledger_bounds = ForwardConsoleService.get_ledger_bounds(parsed_source, parsed_target)
        if ledger_bounds:
            bounds.extend(ledger_bounds)

        if not bounds:
            return {"status": "success", "data": _empty_coverage_data(parsed_source, parsed_target)}

        w_min = min_id if min_id is not None else min(bounds)
        w_max = max_id if max_id is not None else max(bounds)

    if w_min > w_max:
        return {"status": "success", "data": _empty_coverage_data(parsed_source, parsed_target)}

    ledger = ForwardConsoleService.get_ledger_runs(parsed_source, parsed_target, w_min, w_max)
    if (
        min_id is not None
        and max_id is not None
        and not segs
        and not ledger["forwarded"]
        and not ledger["filtered"]
        and not ledger["bad"]
    ):
        return {"status": "success", "data": _empty_coverage_data(parsed_source, parsed_target)}
    forwarded_payload, t1 = _runs_payload(ledger["forwarded"])
    filtered_payload, t2 = _runs_payload(ledger["filtered"])
    bad_payload, t3 = _runs_payload(ledger["bad"])

    return {
        "status": "success",
        "data": {
            "source_chat": parsed_source,
            "target_chat": parsed_target,
            "range": {"min_id": w_min, "max_id": w_max},
            "covered": [{"start": s, "end": e} for s, e in merge_intervals(segs)],
            "forwarded_runs": forwarded_payload,
            "filtered_runs": filtered_payload,
            "bad_runs": bad_payload,
            "stats": {
                "forwarded": len(ledger["forwarded"]),
                "filtered": len(ledger["filtered"]),
                "bad": len(ledger["bad"]),
            },
            "truncated": bool(t1 or t2 or t3),
        },
    }


@router.get("/coverage/gaps", response_model=ForwardCoverageGapsResponse)
async def get_forward_coverage_gaps(
    source_chat: str, target_chat: str, min_id: int | None = None, max_id: int | None = None
):
    """未覆盖 gap 列表 (按长度降序)，est_count 为区间 ID 跨度的保守估算"""
    parsed_source, parsed_target = _parse_pair(source_chat, target_chat)
    segs = ForwardConsoleService.get_coverage_segments(parsed_source, parsed_target)

    if min_id is None or max_id is None:
        if not segs:
            return {"status": "success", "data": {"window": None, "gaps": [], "total_est_count": 0}}
        w_min = min_id if min_id is not None else min(s for s, _ in segs)
        w_max = max_id if max_id is not None else max(e for _, e in segs)
    else:
        w_min, w_max = min_id, max_id

    gaps = compute_gaps(segs, w_min, w_max)
    gaps.sort(key=lambda g: g[1] - g[0], reverse=True)
    return {
        "status": "success",
        "data": {
            "window": {"min_id": w_min, "max_id": w_max},
            "gaps": [{"start": s, "end": e, "est_count": e - s + 1} for s, e in gaps],
            "total_est_count": sum(e - s + 1 for s, e in gaps),
        },
    }
