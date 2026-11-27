"""任务队列仓储 (Task Queue Repository): 任务全生命周期 CRUD / 并发记账 / 日志水位 / 级联清理

【async-first 公共面契约】
- 同步方法 = 实现层单一真源: 供写池 worker 回调 (submit_db_write)、to_thread 包裹的辅助函数、
  测试直接驱动使用; 每个操作的 SQL/语义只在同步实现里维护一份。
- `*_async` = 调用方公共异步接口: 由 `_install_async_faces` 按清单自动生成 (asyncio.to_thread
  包装, 调用时经 getattr 解析同步实现 —— patch 同步方法对门面透明)。手写门面已全部删除。
- 特例: `append_log_async` 双段式手写 —— DB 写在工作线程, WS 广播必须留在调用方事件循环
  (工作线程内 get_running_loop 不可用, 整体 to_thread 会静默吞掉广播)。
- 红线: 事件循环内禁止直调同步方法阻塞循环 (存量调用点执行体重写收口, 禁止新增)。
"""

import asyncio
import json
import logging
import sqlite3
import time
from collections.abc import Callable, Collection, Coroutine
from contextlib import AbstractContextManager, ExitStack
from typing import Any, ClassVar, Literal

from app.db.database import db_cursor, db_immediate_tx
from app.db.publish_repository import SLOT_ACTIVE_STATES_SQL

logger = logging.getLogger(__name__)


class DuplicateDownloadTaskError(Exception):
    def __init__(self, task_id: str, status: Literal["pending", "queued", "running", "paused"]) -> None:
        super().__init__(task_id, status)
        self.task_id = task_id
        self.status = status
        self.retry_source_status: str | None = None


def _find_active_download(
    cursor: sqlite3.Cursor, task_type: str, target_key: str, exclude_id: str | None = None
) -> DuplicateDownloadTaskError | None:
    cursor.execute(
        """
        SELECT id, status FROM task_queue
        WHERE (? IS NULL OR id != ?) AND task_type = ? AND status IN ('pending', 'queued', 'running', 'paused')
              AND json_extract(params_json, '$.download_target_key') = ?
        ORDER BY (status = 'paused'), created_at, id LIMIT 1
        """,
        (exclude_id, exclude_id, task_type, target_key),
    )
    row = cursor.fetchone()
    return DuplicateDownloadTaskError(row[0], row[1]) if row else None


def _check_download_requeue(cursor: sqlite3.Cursor, task_id: str, task_type: str, params: Any) -> None:
    if task_type not in ("download_link", "download_range") or not isinstance(params, dict):
        return
    target_key = params.get("download_target_key")
    if isinstance(target_key, str) and target_key:
        if conflict := _find_active_download(cursor, task_type, target_key, exclude_id=task_id):
            raise conflict


# 任务列表排序口径 (query_tasks/get_all 单点共用):
# 活跃优先阶梯 (running > queued/pending > paused > failed > completed > cancelled)
# + 活跃任务 FIFO 入库升序 / 终态任务最近优先倒序
TASK_QUEUE_ORDER_SQL = """
ORDER BY
  CASE status
    WHEN 'running' THEN 1
    WHEN 'queued' THEN 2
    WHEN 'pending' THEN 2
    WHEN 'paused' THEN 3
    WHEN 'failed' THEN 4
    WHEN 'completed' THEN 5
    WHEN 'cancelled' THEN 6
    ELSE 99
  END ASC,
  CASE
    WHEN status IN ('running', 'queued', 'pending', 'paused') THEN created_at
    ELSE -COALESCE(updated_at, created_at)
  END ASC
"""


def _fetch_log_watermarks(cursor, task_ids: list[str] | None = None) -> dict[str, tuple[int, int]]:
    """日志水位统计单点 (query_tasks/get_all 共用): 按当页 task_ids 过滤聚合每任务条数 + 最新 seq，杜绝全表扫描"""
    if task_ids is not None:
        if not task_ids:
            return {}
        placeholders = ",".join("?" for _ in task_ids)
        cursor.execute(
            f"SELECT task_id, COUNT(*) AS cnt, COALESCE(MAX(seq), 0) AS last_seq "
            f"FROM task_logs WHERE task_id IN ({placeholders}) GROUP BY task_id",
            tuple(task_ids),
        )
    else:
        cursor.execute(
            "SELECT task_id, COUNT(*) AS cnt, COALESCE(MAX(seq), 0) AS last_seq FROM task_logs GROUP BY task_id"
        )
    return {r["task_id"]: (r["cnt"], r["last_seq"]) for r in cursor.fetchall()}


def _finalize_task_rows(rows: list[dict[str, Any]], stats: dict[str, tuple[int, int]]) -> list[dict[str, Any]]:
    """任务行收尾单点 (query_tasks/get_all 共用): params 反序列化提升 + 剔除原始 JSON 列 + 日志水位富化"""
    result = []
    for r in rows:
        TaskQueueRepository._hydrate_task_dict(r)
        r.pop("log_messages", None)
        r.pop("params_json", None)
        cnt, last_seq = stats.get(r["id"], (0, 0))
        r["log_count"] = cnt
        r["last_log_seq"] = last_seq
        result.append(r)
    return result


def _cascade_task_cleanup(cursor, task_ids: list[str]) -> None:
    """任务删除级联清理单点 (delete 与 clear_finished 共用口径, 批量 IN 形式):
    强所属: task_logs / download_breakpoints 物理删除; 弱关联: forward_history /
    forward_skips / storage_registry 置空 task_id; 占位槽: 排除式保留可行动态
    (排除口径 SLOT_ACTIVE_STATES_SQL 单源于 publish_repository)。"""
    placeholders = ",".join("?" * len(task_ids))
    cursor.execute(f"DELETE FROM task_logs WHERE task_id IN ({placeholders})", task_ids)
    cursor.execute(f"DELETE FROM download_breakpoints WHERE task_id IN ({placeholders})", task_ids)
    cursor.execute(f"UPDATE forward_history SET task_id = NULL WHERE task_id IN ({placeholders})", task_ids)
    cursor.execute(f"UPDATE forward_skips SET task_id = NULL WHERE task_id IN ({placeholders})", task_ids)
    cursor.execute(f"UPDATE storage_registry SET task_id = NULL WHERE task_id IN ({placeholders})", task_ids)
    cursor.execute(f"UPDATE publish_history SET task_id = NULL WHERE task_id IN ({placeholders})", task_ids)
    cursor.execute(
        f"DELETE FROM placeholder_slots WHERE task_id IN ({placeholders}) AND {SLOT_ACTIVE_STATES_SQL}",
        task_ids,
    )


def _preserve_completed_download_history(cursor: sqlite3.Cursor, task_ids: list[str]) -> None:
    """任务行删除前，保留相册成员所属任务已完成的证据。"""
    placeholders = ",".join("?" for _ in task_ids)
    cursor.execute(
        f"""
        UPDATE download_history SET task_completed = 1
        WHERE grouped_id IS NOT NULL AND task_id IN (
            SELECT id FROM task_queue WHERE id IN ({placeholders}) AND status = 'completed'
        )
        """,
        task_ids,
    )


class _AsyncFace[**P, R]:
    """类型化的 to_thread 异步门面描述符，支持静态推断、兼容 staticmethod 并保留动态 patch 透传。"""

    def __init__(self, sync_fn: Callable[P, R] | staticmethod) -> None:
        fn = getattr(sync_fn, "__func__", sync_fn)
        self._sync_name = getattr(fn, "__name__", "")

    def __get__(self, obj: object, owner: type) -> Callable[P, Coroutine[Any, Any, R]]:
        def face(*args: P.args, **kwargs: P.kwargs) -> Any:
            return asyncio.to_thread(getattr(owner, self._sync_name), *args, **kwargs)

        face.__name__ = f"{self._sync_name}_async"
        face.__qualname__ = f"{owner.__qualname__}.{self._sync_name}_async"
        face.__doc__ = f"{self._sync_name} 异步门面 (生成): 同步实现经 to_thread 移至工作线程"
        return face


class TaskQueueRepository:
    # 依赖反转观察者钩子 (由 app/core/metrics 与 app/core/connection_manager 注册，保持 app/db 零向上依赖)
    _status_transition_hook: ClassVar[Callable[[str], None] | None] = None
    _log_broadcast_hook: ClassVar[Callable[[dict[str, Any]], Any] | None] = None
    _background_tasks: ClassVar[set[asyncio.Task[Any]]] = set()

    @classmethod
    def register_status_transition_hook(cls, hook: Callable[[str], None]) -> None:
        cls._status_transition_hook = hook

    @classmethod
    def register_log_broadcast_hook(cls, hook: Callable[[dict[str, Any]], Any]) -> None:
        cls._log_broadcast_hook = hook

    @classmethod
    def _notify_status_transition(cls, status: str) -> None:
        if cls._status_transition_hook is not None:
            try:
                cls._status_transition_hook(status)
            except Exception as m_err:
                logger.debug("执行任务状态流转钩子 (%s) 异常 (非致命): %s", status, m_err)

    # 同步实现清单 (append_log 双段式特例手写, 不入清单)
    _ASYNC_FACE_METHODS = (
        "update_progress",
        "merge_params",
        "update_status",
        "update_concurrency",
        "update_account_id",
        "get_by_id",
        "retry_task",
        "requeue_task_for_resume",
        "pause_pikpak_for_risk",
        "mark_retry",
        "query_tasks",
        "count_by_status",
        "reset_task_for_retry",
        "prioritize_task",
        "delete",
        "clear_finished",
        "get_all",
        "get_task_logs",
        "prune_task_logs",
        "record_part_checkpoint",
        "clear_part_checkpoint",
    )

    @staticmethod
    def find_active_download(task_type: str, target_key: str) -> DuplicateDownloadTaskError | None:
        with db_cursor() as cursor:
            return _find_active_download(cursor, task_type, target_key)

    @staticmethod
    def create(
        task_id: str,
        task_type: str,
        params: dict,
        account_id: int | None = None,
        priority: int = 10,
        max_retries: int = 3,
        status: str = "pending",
        concurrency_total: int | None = None,
        total_bytes: int = 0,
        total_items: int = 0,
        current_item: str = "",
        *,
        download_target_key: str | None = None,
    ) -> dict[str, Any]:
        now = time.time()
        if download_target_key is not None:
            params = {**params, "download_target_key": download_target_key}
        params_json = json.dumps(params, ensure_ascii=False)
        concurrency_total_val = (
            concurrency_total
            if concurrency_total is not None
            else int(params.get("threads") or params.get("upload_concurrency") or 0)
        )
        transaction = db_immediate_tx() if download_target_key is not None else db_cursor(commit=True)
        with transaction as cursor:
            if download_target_key is not None and (
                conflict := _find_active_download(cursor, task_type, download_target_key)
            ):
                raise conflict
            cursor.execute(
                """
                INSERT INTO task_queue
                (id, task_type, status, params_json, processed_items, total_items, progress_bytes, total_bytes, current_item, account_id, priority, concurrency_total, retry_count, max_retries, next_retry_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, 0, ?, 0, ?, ?, ?, ?, ?, 0, ?, 0.0, ?, ?)
                """,
                (
                    task_id,
                    task_type,
                    status,
                    params_json,
                    total_items,
                    total_bytes,
                    current_item,
                    account_id,
                    priority,
                    concurrency_total_val,
                    max_retries,
                    now,
                    now,
                ),
            )
        result = {
            "id": task_id,
            "task_type": task_type,
            "status": status,
            "progress_bytes": 0,
            "total_bytes": total_bytes,
            "processed_items": 0,
            "total_items": total_items,
            "speed_mb_s": 0.0,
            "eta_s": 0.0,
            "current_item": current_item,
            "account_id": account_id,
            "priority": priority,
            "concurrency_total": concurrency_total_val,
            "retry_count": 0,
            "max_retries": max_retries,
            "params": params,
            "created_at": now,
            "updated_at": now,
        }
        return TaskQueueRepository._hydrate_task_dict(result)

    @staticmethod
    def _hydrate_task_dict(d: dict[str, Any]) -> dict[str, Any]:
        """反序列化 params_json 并提升关键元数据 (executor_names, account_ids) 至顶层属性，同时组装 progress 同构结构体"""
        if "params" not in d:
            try:
                d["params"] = json.loads(d.get("params_json") or "{}")
            except Exception:
                d["params"] = {}
        if isinstance(d.get("params"), dict):
            if "executor_names" in d["params"]:
                d["executor_names"] = d["params"]["executor_names"]
            if "account_ids" in d["params"]:
                d["account_ids"] = d["params"]["account_ids"]

        if "progress" not in d or not isinstance(d.get("progress"), dict):
            p_bytes = int(d.get("progress_bytes") or 0)
            t_bytes = int(d.get("total_bytes") or 0)
            p_items = int(d.get("processed_items") or 0)
            t_items = int(d.get("total_items") or 0)
            pct = 0.0
            if t_bytes > 0:
                pct = round(min(100.0, max(0.0, (p_bytes / t_bytes) * 100)), 1)
            elif t_items > 0:
                pct = round(min(100.0, max(0.0, (p_items / t_items) * 100)), 1)
            if d.get("status") == "completed":
                pct = 100.0
            d["progress"] = {
                "processed_bytes": p_bytes,
                "total_bytes": t_bytes,
                "processed_items": p_items,
                "total_items": t_items,
                "percent": pct,
                "speed_mb_s": float(d.get("speed_mb_s") or 0.0),
                "eta_s": float(d.get("eta_s") or 0.0),
                "current_item": d.get("current_item") or "",
                "concurrency": int(d.get("concurrency") or 0),
                "concurrency_total": int(d.get("concurrency_total") or 0),
                "dc_id": int(d.get("dc_id") or 0),
                "pikpak_download_speed_mb_s": float(d.get("pikpak_download_speed_mb_s") or 0.0),
            }
        elif isinstance(d.get("progress"), dict) and d.get("status") == "completed":
            if not float(d["progress"].get("percent") or 0.0):
                d["progress"]["percent"] = 100.0
        return d

    @staticmethod
    def get_queued_candidates(limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        """获取所有处于排队态且未在冷却期的候选任务 (按 priority DESC, created_at ASC 排序)"""
        now = time.time()
        with db_cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM task_queue
                WHERE status IN ('queued', 'pending')
                AND (next_retry_at IS NULL OR next_retry_at <= ?)
                ORDER BY priority DESC, created_at ASC
                LIMIT ? OFFSET ?
                """,
                (now, limit, offset),
            )
            rows = [dict(r) for r in cursor.fetchall()]
        for r in rows:
            TaskQueueRepository._hydrate_task_dict(r)
        return rows

    @staticmethod
    def get_queued_count() -> int:
        """获取当前处于就绪排队态的任务总数 (高效索引查询)"""
        now = time.time()
        with db_cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) FROM task_queue
                WHERE status IN ('queued', 'pending')
                AND (next_retry_at IS NULL OR next_retry_at <= ?)
                """,
                (now,),
            )
            count = cursor.fetchone()[0]
        return count

    @staticmethod
    def acquire_task(task_id: str, account_id: int | None = None, *, expected_updated_at: float | None = None) -> bool:
        """原子尝试将排队中且冷却期已到的任务置为 running 并回填执行账号 (CAS 机制避免多调度循环并发认领)"""
        now = time.time()
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "UPDATE task_queue SET status = 'running', account_id = COALESCE(?, account_id), updated_at = ? "
                "WHERE id = ? AND status IN ('queued', 'pending') AND (next_retry_at IS NULL OR next_retry_at <= ?) "
                "AND (? IS NULL OR updated_at = ?)",
                (account_id, now, task_id, now, expected_updated_at, expected_updated_at),
            )
            affected = cursor.rowcount > 0
        if affected:
            TaskQueueRepository._notify_status_transition("running")
        return affected

    @staticmethod
    def fail_queued_task(task_id: str, error_detail: str) -> bool:
        """CAS 拒绝排队中任务并原子置为 failed (如显式绑定的执行账号已失效)"""
        now = time.time()
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "UPDATE task_queue SET status = 'failed', error_detail = ?, updated_at = ? "
                "WHERE id = ? AND status IN ('queued', 'pending')",
                (error_detail, now, task_id),
            )
            affected = cursor.rowcount > 0
        if affected:
            TaskQueueRepository._notify_status_transition("failed")
        return affected

    @staticmethod
    def fail_running_task(task_id: str, error_detail: str) -> bool:
        """仅在任务仍运行时写入失败原因并清除重试时间。"""
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "UPDATE task_queue SET status = 'failed', error_detail = ?, next_retry_at = 0, updated_at = ? "
                "WHERE id = ? AND status = 'running'",
                (error_detail, time.time(), task_id),
            )
            affected = cursor.rowcount > 0
        if affected:
            TaskQueueRepository._notify_status_transition("failed")
        return affected

    @staticmethod
    def update_account_id(task_id: str, account_id: int):
        """故障转移或动态指派时更新任务执行账号"""
        now = time.time()
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "UPDATE task_queue SET account_id = ?, updated_at = ? WHERE id = ?", (account_id, now, task_id)
            )

    @staticmethod
    def reset_task_for_retry(
        task_id: str,
        *,
        retry_count: int | None = None,
        next_retry_at: float = 0.0,
        error_detail: str = "",
        from_statuses: tuple[str, ...] | None = None,
        expected_updated_at: float | None = None,
        params_patch: dict[str, Any] | None = None,
    ) -> bool:
        """原子复位任务为 queued 状态:
        1. 清零瞬态速率与活跃指标: speed_mb_s=0.0, eta_s=0.0, concurrency=0, current_item=''
        2. 清理 params_json 中的历史失败明细 (failed_files / failed_items / pikpak_download_speed_mb_s)
        3. 重置/更新重试计数与退避冷却期
        """
        now = time.time()

        where_extra = ""
        params_val: list[Any] = []
        if from_statuses is not None:
            if not from_statuses:
                return False
            placeholders = ",".join("?" for _ in from_statuses)
            where_extra = f" AND status IN ({placeholders})"
            params_val.extend(from_statuses)
        if expected_updated_at is not None:
            where_extra += " AND updated_at = ?"
            params_val.append(expected_updated_at)

        affected = False
        with db_immediate_tx() as cursor:
            # 读改写与 UPDATE 同事务
            cursor.execute(
                f"SELECT task_type, params_json, retry_count, status FROM task_queue WHERE id = ?{where_extra}",
                (task_id, *params_val),
            )
            row = cursor.fetchone()
            if not row:
                return False

            task_type, current_params_raw, current_retry_count, current_status = row

            new_params_json = current_params_raw
            if current_params_raw or params_patch:
                try:
                    p = json.loads(current_params_raw or "{}")
                except (ValueError, TypeError) as p_err:
                    logger.debug("重置任务时反序列化 params_json 异常 (非致命跳过): %s", p_err)
                    p = None
                if params_patch or isinstance(p, dict):
                    try:
                        _check_download_requeue(cursor, task_id, task_type, p)
                    except DuplicateDownloadTaskError as conflict:
                        conflict.retry_source_status = current_status
                        raise
                    p = p if isinstance(p, dict) else {}
                    p.update(params_patch or {})
                    changed = bool(params_patch)
                    clear_keys: tuple[str, ...] = (
                        "failed_files",
                        "failed_items",
                        "pikpak_download_speed_mb_s",
                        "newly_published_count",
                    )
                    if retry_count == 0:
                        clear_keys = (
                            *clear_keys,
                            "safe_paused",
                            "throttle_strike_count",
                            "data_risk_strike_count",
                        )
                    for k in clear_keys:
                        if k in p:
                            p.pop(k, None)
                            changed = True
                    if changed:
                        new_params_json = json.dumps(p, ensure_ascii=False)

            target_retry = retry_count if retry_count is not None else (current_retry_count or 0)

            update_sql = f"""
                UPDATE task_queue
                SET status = 'queued',
                    speed_mb_s = 0.0,
                    eta_s = 0.0,
                    concurrency = 0,
                    current_item = '',
                    params_json = ?,
                    retry_count = ?,
                    next_retry_at = ?,
                    error_detail = ?,
                    updated_at = ?
                WHERE id = ?{where_extra}
            """
            cursor.execute(
                update_sql, (new_params_json, target_retry, next_retry_at, error_detail, now, task_id, *params_val)
            )
            affected = cursor.rowcount > 0

        if affected:
            TaskQueueRepository._notify_status_transition("queued")
        return affected

    @staticmethod
    def requeue_task_for_resume(
        task_id: str,
        expected_status: str,
        *,
        params_patch: dict[str, Any],
        priority_floor: int | None = None,
        expected_updated_at: float | None = None,
        error_detail: str | None = None,
    ) -> bool:
        """按原状态恢复入队，原子校验同键任务并保存续传参数。"""
        return TaskQueueRepository._write_task_transition(
            task_id, (expected_status,), "queued", expected_updated_at, params_patch, error_detail, priority_floor
        )

    @staticmethod
    def resume_paused_task(
        task_id: str,
        expected_updated_at: float | None,
        *,
        params_patch: dict[str, Any] | None = None,
        error_detail: str | None = None,
        commit_guard: AbstractContextManager[bool],
    ) -> bool:
        """原地恢复暂停任务，同时提交恢复参数和错误详情。"""
        return TaskQueueRepository._write_task_transition(
            task_id, ("paused",), "running", expected_updated_at, params_patch, error_detail, commit_guard=commit_guard
        )

    @staticmethod
    def pause_pikpak_for_risk(task_id: str, params_patch: dict[str, Any], error_detail: str) -> bool:
        return TaskQueueRepository._write_task_transition(
            task_id, ("running", "paused", "queued", "pending"), "paused", None, params_patch, error_detail
        )

    @staticmethod
    def _write_task_transition(
        task_id: str,
        expected_statuses: tuple[str, ...],
        status: Literal["queued", "running", "paused"],
        expected_updated_at: float | None,
        params_patch: dict[str, Any] | None,
        error_detail: str | None,
        priority_floor: int | None = None,
        *,
        commit_guard: AbstractContextManager[bool] | None = None,
    ) -> bool:
        placeholders = ",".join("?" for _ in expected_statuses)
        # The ownership guard is acquired only after SQLite grants the write transaction, and spans its commit.
        with ExitStack() as guard_stack, db_immediate_tx() as cursor:
            cursor.execute(
                f"SELECT task_type, params_json FROM task_queue WHERE id = ? AND status IN ({placeholders}) "
                "AND (? IS NULL OR updated_at = ?)",
                (task_id, *expected_statuses, expected_updated_at, expected_updated_at),
            )
            row = cursor.fetchone()
            if row is None:
                return False
            task_type, params_json = row
            try:
                params = json.loads(params_json or "{}")
            except (ValueError, TypeError) as e:
                logger.debug("恢复任务时反序列化 params_json 异常: %s", e)
                params = {}
            if status == "queued":
                _check_download_requeue(cursor, task_id, task_type, params)
            if params_patch:
                current_params = params if isinstance(params, dict) else {}
                params_json = json.dumps({**current_params, **params_patch}, ensure_ascii=False)
            if commit_guard is not None and not guard_stack.enter_context(commit_guard):
                return False
            cursor.execute(
                """
                UPDATE task_queue
                SET status = ?, params_json = ?, updated_at = ?, error_detail = COALESCE(?, error_detail),
                    priority = CASE WHEN ? IS NULL THEN priority ELSE MAX(COALESCE(priority, 10), ?) END,
                    next_retry_at = CASE WHEN ? = 'queued' THEN 0 ELSE next_retry_at END
                WHERE id = ?
                """,
                (
                    status,
                    params_json,
                    time.time(),
                    error_detail,
                    priority_floor,
                    priority_floor,
                    status,
                    task_id,
                ),
            )
        TaskQueueRepository._notify_status_transition(status)
        return True

    @staticmethod
    def mark_retry(
        task_id: str,
        retry_count: int,
        next_retry_at: float,
        error_detail: str = "",
        *,
        from_statuses: tuple[str, ...] = ("running", "queued", "pending"),
    ) -> bool:
        """任务发生可恢复异常时重置为 queued 状态并设置退避冷却 (委托至 reset_task_for_retry)。

        默认仅允许从 running / queued / pending 转移，防止并发 pause / cancel 终态被晚到异常覆写。"""
        return TaskQueueRepository.reset_task_for_retry(
            task_id,
            retry_count=retry_count,
            next_retry_at=next_retry_at,
            error_detail=error_detail,
            from_statuses=from_statuses,
        )

    @staticmethod
    def set_error_detail(task_id: str, error_detail: str):
        """仅写任务失败的可读原因, 不改动任务状态/重试计数 (SQL 参数绑定)"""
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                """
                UPDATE task_queue
                SET error_detail = ?, updated_at = ?
                WHERE id = ?
                """,
                (error_detail, time.time(), task_id),
            )

    @staticmethod
    def prioritize_task(task_id: str, priority: int = 50):
        """提升任务调度优先级 (用于恢复任务就绪插队缓冲)"""
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "UPDATE task_queue SET priority = MAX(COALESCE(priority, 10), ?), updated_at = ? WHERE id = ?",
                (priority, time.time(), task_id),
            )

    @staticmethod
    def update_progress(
        task_id: str,
        progress_bytes: int = 0,
        total_bytes: int = 0,
        processed_items: int = 0,
        total_items: int = 0,
        speed_mb_s: float = 0.0,
        eta_s: float = 0.0,
        current_item: str = "",
        concurrency: int = 0,
        dc_id: int = 0,
        concurrency_total: int | None = None,
    ):
        now = time.time()
        assignments = (
            "progress_bytes = ?, total_bytes = ?, processed_items = ?, total_items = ?, "
            "speed_mb_s = ?, eta_s = ?, current_item = ?, concurrency = ?, dc_id = ?, updated_at = ?"
        )
        params = [
            progress_bytes,
            total_bytes,
            processed_items,
            total_items,
            speed_mb_s,
            eta_s,
            current_item,
            concurrency,
            dc_id,
            now,
        ]
        if concurrency_total is not None:
            assignments += ", concurrency_total = ?"
            params.append(concurrency_total)
        params.append(task_id)
        with db_cursor(commit=True) as cursor:
            cursor.execute(f"UPDATE task_queue SET {assignments} WHERE id = ?", tuple(params))

    @staticmethod
    def merge_params(task_id: str, patch: dict, *, reset_retry_count: bool = False):
        """将 patch 合并进任务的 params_json (保留既有键)，使用 BEGIN IMMEDIATE 独占事务杜绝并发丢失更新"""
        with db_immediate_tx() as cursor:
            cursor.execute("SELECT params_json FROM task_queue WHERE id = ?", (task_id,))
            row = cursor.fetchone()
            if row:
                try:
                    params = json.loads(row[0] or "{}")
                except Exception:
                    params = {}
                params.update(patch)
                if reset_retry_count:
                    cursor.execute(
                        "UPDATE task_queue SET params_json = ?, retry_count = 0, updated_at = ? WHERE id = ?",
                        (json.dumps(params, ensure_ascii=False), time.time(), task_id),
                    )
                else:
                    cursor.execute(
                        "UPDATE task_queue SET params_json = ?, updated_at = ? WHERE id = ?",
                        (json.dumps(params, ensure_ascii=False), time.time(), task_id),
                    )

    @staticmethod
    def overwrite_params_list(
        task_id: str, key: str, rows: list, *, keep_pred: Callable[[Any], bool] | None = None
    ) -> bool:
        """独占事务内读-改-写覆写 params[key]: 旧行中满足 keep_pred 的保留并与 rows 拼接为新值。

        读取与覆写同处 BEGIN IMMEDIATE 事务 —— 消除 read-before-write TOCTOU
        (批 A 复审: manual_skip 行保留不得被并发写入挤掉);
        事务失败时整体不落库, 存量行天然保留。任务行不存在时不写入 (与 merge_params 口径一致)。
        返回是否实际写入。"""
        with db_immediate_tx() as cursor:
            cursor.execute("SELECT params_json FROM task_queue WHERE id = ?", (task_id,))
            row = cursor.fetchone()
            if not row:
                return False
            try:
                params = json.loads(row[0] or "{}")
            except Exception:
                params = {}
            old_rows = params.get(key)
            kept = [r for r in old_rows if keep_pred(r)] if (keep_pred and isinstance(old_rows, list)) else []
            params[key] = kept + list(rows)
            cursor.execute(
                "UPDATE task_queue SET params_json = ?, updated_at = ? WHERE id = ?",
                (json.dumps(params, ensure_ascii=False), time.time(), task_id),
            )
            return True

    @staticmethod
    def commit_manual_skip_if_unchanged(
        task_id: str,
        expected_status: str,
        expected_updated_at: float | None,
        skipped_item: dict[str, Any],
        *,
        clear_checkpoint_fid: str = "",
    ) -> bool:
        """在独占写事务内按状态与版本 CAS 追加手动跳过记录并可选清理分卷检查点。"""
        with db_immediate_tx() as cursor:
            cursor.execute("SELECT status, updated_at, params_json FROM task_queue WHERE id = ?", (task_id,))
            row = cursor.fetchone()
            if not row or row[0] != expected_status:
                return False
            if expected_updated_at is not None and row[1] != expected_updated_at:
                return False
            try:
                params = json.loads(row[2] or "{}")
            except Exception:
                params = {}
            if not isinstance(params, dict):
                params = {}
            existing_skipped = params.get("skipped_files")
            skipped_list = list(existing_skipped) if isinstance(existing_skipped, list) else []
            skipped_list.append(skipped_item)
            params["skipped_files"] = skipped_list
            if clear_checkpoint_fid:
                checkpoints = params.get("pikpak_part_checkpoints")
                if isinstance(checkpoints, dict):
                    checkpoints.pop(str(clear_checkpoint_fid), None)
            cursor.execute(
                "UPDATE task_queue SET params_json = ?, updated_at = ? "
                "WHERE id = ? AND status = ? AND (? IS NULL OR updated_at = ?)",
                (
                    json.dumps(params, ensure_ascii=False),
                    time.time(),
                    task_id,
                    expected_status,
                    expected_updated_at,
                    expected_updated_at,
                ),
            )
            return cursor.rowcount > 0

    @staticmethod
    def record_part_checkpoint(
        task_id: str,
        file_id: str,
        strategy: str,
        total_parts: int,
        threshold: int,
        part_index: int,
        msg_id: int,
        plan_signature: str | None = None,
    ) -> None:
        """持久化记录多分卷交付检查点 (BEGIN IMMEDIATE 独占写入)"""
        if not task_id:
            return
        with db_immediate_tx() as cursor:
            cursor.execute("SELECT params_json FROM task_queue WHERE id = ?", (task_id,))
            row = cursor.fetchone()
            if not row:
                return
            try:
                params = json.loads(row[0] or "{}") if row[0] else {}
            except Exception:
                params = {}
            checkpoints = params.get("pikpak_part_checkpoints")
            if not isinstance(checkpoints, dict):
                checkpoints = {}
                params["pikpak_part_checkpoints"] = checkpoints
            file_cp = checkpoints.get(str(file_id))
            if not isinstance(file_cp, dict):
                file_cp = {
                    "strategy": strategy,
                    "total_parts": total_parts,
                    "threshold": threshold,
                    "plan_signature": plan_signature,
                    "delivered": {},
                }
                checkpoints[str(file_id)] = file_cp
            # 策略/总卷数/分卷阈值/切分边界签名变更时复位旧交付记录，保证幂等保真
            if (
                file_cp.get("strategy") != strategy
                or file_cp.get("total_parts") != total_parts
                or file_cp.get("threshold") != threshold
                or file_cp.get("plan_signature") != plan_signature
            ):
                file_cp["strategy"] = strategy
                file_cp["total_parts"] = total_parts
                file_cp["threshold"] = threshold
                file_cp["plan_signature"] = plan_signature
                file_cp["delivered"] = {}
            delivered = file_cp.get("delivered")
            if not isinstance(delivered, dict):
                delivered = {}
                file_cp["delivered"] = delivered
            delivered[str(part_index)] = int(msg_id)
            cursor.execute(
                "UPDATE task_queue SET params_json = ?, updated_at = ? WHERE id = ?",
                (json.dumps(params, ensure_ascii=False), time.time(), task_id),
            )

    @staticmethod
    def clear_part_checkpoint(task_id: str, file_id: str) -> None:
        """清理指定文件的分卷检查点条目 (文件全量投递或动态重规划后)"""
        if not task_id:
            return
        with db_immediate_tx() as cursor:
            cursor.execute("SELECT params_json FROM task_queue WHERE id = ?", (task_id,))
            row = cursor.fetchone()
            if not row:
                return
            try:
                params = json.loads(row[0] or "{}") if row[0] else {}
            except Exception:
                params = {}
            checkpoints = params.get("pikpak_part_checkpoints")
            if isinstance(checkpoints, dict) and str(file_id) in checkpoints:
                checkpoints.pop(str(file_id), None)
                cursor.execute(
                    "UPDATE task_queue SET params_json = ?, updated_at = ? WHERE id = ?",
                    (json.dumps(params, ensure_ascii=False), time.time(), task_id),
                )

    @staticmethod
    def get_active_by_type(task_type: str) -> list[dict[str, Any]]:
        """查询指定类型处于活跃状态 (queued/pending/running/paused 的任务，含解析后的 params)"""
        with db_cursor() as cursor:
            cursor.execute(
                "SELECT * FROM task_queue WHERE task_type = ? AND status IN ('queued', 'pending', 'running', 'paused')",
                (task_type,),
            )
            rows = [dict(r) for r in cursor.fetchall()]
        for r in rows:
            TaskQueueRepository._hydrate_task_dict(r)
        return rows

    @staticmethod
    def update_status(
        task_id: str,
        status: str,
        from_statuses: tuple[str, ...] | None = None,
        *,
        expected_updated_at: float | None = None,
    ) -> bool:
        """状态更新。可选 from_statuses CAS 守卫 (与 acquire_task/delete/占位槽三态 CAS 统一守卫式口径):
        仅当前状态 ∈ from_statuses 时生效; 返回是否生效 (未命中守卫或任务不存在均 False)。"""
        now = time.time()
        where_extra = ""
        params_val: list[Any] = [status, now, task_id]
        if from_statuses is not None:
            if not from_statuses:
                return False
            placeholders = ",".join("?" for _ in from_statuses)
            where_extra = f" AND status IN ({placeholders})"
            params_val = [status, now, task_id, *from_statuses]
        if expected_updated_at is not None:
            where_extra += " AND updated_at = ?"
            params_val.append(expected_updated_at)
        completing = status == "completed"
        with db_immediate_tx() if completing else db_cursor(commit=True) as cursor:
            cursor.execute(
                f"UPDATE task_queue SET status = ?, updated_at = ? WHERE id = ?{where_extra}", tuple(params_val)
            )
            affected = cursor.rowcount > 0
            if affected and completing:
                cursor.execute(
                    """
                    UPDATE download_history SET task_completed = 1 WHERE id IN (
                        SELECT history_id FROM download_history_reuse WHERE task_id = ?
                    )
                    """,
                    (task_id,),
                )
                cursor.execute("DELETE FROM download_history_reuse WHERE task_id = ?", (task_id,))
        if affected:
            TaskQueueRepository._notify_status_transition(status)
        return affected

    @staticmethod
    def update_concurrency(
        task_id: str,
        concurrency: int,
        upload_in_flight_per_connection: int | None = None,
        param_merger: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> bool:
        """动态更新任务并发数 (concurrency_total + params_json 任务专属键)。

        param_merger(task_type, params) 由调用方注入策略合并逻辑 (P1 层次倒置修复:
        仓储层禁止反向 import app.core.task_strategy); 未注入时回退 params["threads"]。"""
        affected = False
        with db_immediate_tx() as cursor:
            cursor.execute("SELECT task_type, params_json FROM task_queue WHERE id = ?", (task_id,))
            row = cursor.fetchone()
            if row:
                task_type, params_json = row[0], row[1]
                try:
                    params = json.loads(params_json or "{}")
                except Exception:
                    params = {}
                if param_merger is not None:
                    param_merger(task_type, params)
                else:
                    params["threads"] = concurrency
                now = time.time()
                cursor.execute(
                    "UPDATE task_queue SET concurrency_total = ?, params_json = ?, updated_at = ? WHERE id = ?",
                    (concurrency, json.dumps(params, ensure_ascii=False), now, task_id),
                )
                affected = cursor.rowcount > 0
        return affected

    @staticmethod
    def _append_log_db(task_id: str, log_msg: str) -> tuple:
        """append_log 的纯 DB 段: BEGIN IMMEDIATE 事务内分配 seq 并 INSERT, 返回 (time_str, seq)。

        不含任何事件循环交互, 可安全运行于工作线程 (to_thread)。"""
        time_str = time.strftime("%H:%M:%S")
        seq = None
        with db_immediate_tx() as cursor:
            cursor.execute("SELECT id FROM task_queue WHERE id = ?", (task_id,))
            if cursor.fetchone():
                cursor.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM task_logs WHERE task_id = ?", (task_id,))
                seq = int(cursor.fetchone()[0])
                cursor.execute(
                    "INSERT INTO task_logs (task_id, seq, time, level, message, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (task_id, seq, time_str, "info", log_msg, time.time()),
                )
        return time_str, seq

    @staticmethod
    def _broadcast_log(task_id: str, log_msg: str, time_str: str, seq: int | None):
        """WS 日志广播: 仅在事件循环线程上经已注册钩子执行 (工作线程内 get_running_loop 抛错, 静默跳过)"""
        hook = TaskQueueRepository._log_broadcast_hook
        if hook is None:
            return
        try:
            # 仅在异步上下文中广播; 同步调用(如测试/脚本)时避免创建未 await 的协程
            asyncio.get_running_loop()
            coro_or_res = hook(
                {
                    "type": "log",
                    "task_id": task_id,
                    "time": time_str,
                    "message": log_msg,
                    "seq": seq,
                    "level": "info",
                }
            )
            if asyncio.iscoroutine(coro_or_res):
                task = asyncio.create_task(coro_or_res)
                TaskQueueRepository._background_tasks.add(task)
                task.add_done_callback(TaskQueueRepository._background_tasks.discard)
        except Exception as e:
            logging.debug("WebSocket 日志广播失败: %s", e)

    @staticmethod
    def append_log(task_id: str, log_msg: str):
        """追加单条日志 (task_logs 表, 单条 INSERT + 任务内自增 seq) 并广播 WS —— 日志广播唯一写点

        seq 分配: BEGIN IMMEDIATE 事务内先取 MAX(seq)+1 再 INSERT, 杜绝并发竞态。
        任务不存在时跳过落库但仍广播 (与旧实现语义一致)。
        同步入口: 在事件循环线程上调用会阻塞循环 (busy_timeout 写竞争时最长 5s);
        async 执行器/调度路径请使用 append_log_async。
        """
        time_str, seq = TaskQueueRepository._append_log_db(task_id, log_msg)
        TaskQueueRepository._broadcast_log(task_id, log_msg, time_str, seq)

    @classmethod
    async def append_log_async(cls, task_id: str, log_msg: str):
        """append_log 异步门面: DB 写经 to_thread 移出事件循环, WS 广播仍在调用方循环上执行。

        工作线程内 get_running_loop 不可用, 故广播由本协程 (循环线程) 负责, 语义与同步版一致;
        同一任务内的顺序调用天然保序, 跨任务 seq 由 BEGIN IMMEDIATE 保证一致。"""
        time_str, seq = await asyncio.to_thread(cls._append_log_db, task_id, log_msg)
        cls._broadcast_log(task_id, log_msg, time_str, seq)

    @staticmethod
    def get_task_logs(
        task_id: str,
        after_seq: int = 0,
        limit: int = 200,
        level: str | None = None,
        query: str | None = None,
    ):
        """增量读取任务日志 (seq 升序, 可选级别/文本关键词过滤)。
        返回 (logs, last_seq, has_more); 任务不存在返回 None。"""
        with db_cursor() as cursor:
            cursor.execute("SELECT id FROM task_queue WHERE id = ?", (task_id,))
            if not cursor.fetchone():
                return None
            where_clauses = ["task_id = ?", "seq > ?"]
            params: list[Any] = [task_id, int(after_seq)]
            if level:
                where_clauses.append("level = ?")
                params.append(level.lower())
            if query:
                where_clauses.append("message LIKE ?")
                params.append(f"%{query}%")
            where_sql = " AND ".join(where_clauses)
            cursor.execute(
                f"SELECT seq, time, level, message, created_at FROM task_logs WHERE {where_sql} ORDER BY seq ASC LIMIT ?",
                tuple(params + [int(limit)]),
            )
            rows = [dict(r) for r in cursor.fetchall()]
            last_seq = rows[-1]["seq"] if rows else int(after_seq)
            cursor.execute(f"SELECT 1 FROM task_logs WHERE {where_sql} AND seq > ? LIMIT 1", tuple(params + [last_seq]))
            has_more = cursor.fetchone() is not None

        return rows, last_seq, has_more

    @classmethod
    def prune_task_logs(cls, max_per_task: int) -> int:
        """启动期日志保留修剪: 每任务仅保留最新 max_per_task 条 (按 id 降序窗口), 返回删除行数。

        task_logs 是唯一无自动保留策略的增长面 (仅任务物理删除/手动清理时清空);
        修剪挂 lifespan 启动期 (无活跃任务, 单写者前提天然满足), 单条窗口函数 DELETE 完成。"""
        if max_per_task <= 0:
            return 0
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                """
                DELETE FROM task_logs WHERE id IN (
                    SELECT id FROM (
                        SELECT id, ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY id DESC) AS rn
                        FROM task_logs
                    ) WHERE rn > ?
                )
                """,
                (int(max_per_task),),
            )
            removed = cursor.rowcount
        return removed

    @staticmethod
    def delete(task_id: str, allowed_statuses: tuple[str, ...] | None = None) -> bool:
        """物理删除任务记录。若指定 allowed_statuses 则原子校验当前状态，防范 TOCTOU 竞态"""
        if allowed_statuses is not None and not allowed_statuses:
            return False
        with db_immediate_tx() as cursor:
            if allowed_statuses is None or "completed" in allowed_statuses:
                _preserve_completed_download_history(cursor, [task_id])
            if allowed_statuses is not None:
                placeholders = ",".join(["?"] * len(allowed_statuses))
                cursor.execute(
                    f"DELETE FROM task_queue WHERE id = ? AND status IN ({placeholders})",
                    [task_id] + list(allowed_statuses),
                )
            else:
                cursor.execute("DELETE FROM task_queue WHERE id = ?", (task_id,))
            deleted = cursor.rowcount > 0
            if deleted:
                # 强所属物理级联 + 弱关联置空 + 占位槽排除式清理 (口径单点: _cascade_task_cleanup)
                _cascade_task_cleanup(cursor, [task_id])
            return deleted

    @staticmethod
    def clear_finished(exclude_ids: Collection[str] | None = None) -> list[str]:
        excluded = {str(tid) for tid in (exclude_ids or ()) if tid}
        with db_immediate_tx() as cursor:
            cursor.execute("SELECT id FROM task_queue WHERE status IN ('completed', 'cancelled', 'failed')")
            finished_ids = [r[0] for r in cursor.fetchall() if r[0] not in excluded]
            for i in range(0, len(finished_ids), 100):
                batch = finished_ids[i : i + 100]
                _preserve_completed_download_history(cursor, batch)
                _cascade_task_cleanup(cursor, batch)
                placeholders = ",".join(["?"] * len(batch))
                cursor.execute(
                    f"DELETE FROM task_queue WHERE id IN ({placeholders}) "
                    "AND status IN ('completed', 'cancelled', 'failed')",
                    batch,
                )
            return finished_ids

    @staticmethod
    def get_status(task_id: str) -> str | None:
        """轻量状态查询: 仅取 status 单列 (执行器启动自检用, 避免经 get_by_id 加载全量任务日志)"""
        with db_cursor() as cursor:
            cursor.execute("SELECT status FROM task_queue WHERE id = ?", (task_id,))
            row = cursor.fetchone()
        return row["status"] if row else None

    @staticmethod
    def get_account_id(task_id: str) -> int | None:
        """轻量账号查询: 仅取 account_id 单列 (释放配额用, 杜绝经 get_by_id 加载全量任务日志与阻塞 I/O)"""
        with db_cursor() as cursor:
            cursor.execute("SELECT account_id FROM task_queue WHERE id = ?", (task_id,))
            row = cursor.fetchone()
        return row["account_id"] if row and row["account_id"] is not None else None

    @staticmethod
    def get_by_id(task_id: str) -> dict[str, Any] | None:
        with db_cursor() as cursor:
            cursor.execute("SELECT * FROM task_queue WHERE id = ?", (task_id,))
            row = cursor.fetchone()
        if row:
            d = dict(row)
            TaskQueueRepository._hydrate_task_dict(d)
            # 日志不内嵌: 详情响应零日志, 一律经 /api/tasks/{id}/logs 增量差量拉取
            return d
        return None

    @staticmethod
    def query_tasks(
        status: str | None = None,
        task_type: str | None = None,
        account_id: int | None = None,
        query: str | None = None,
        page: int = 1,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], int]:
        """多维筛选与服务端分页查询任务列表。
        语义契约 (前端 TaskCenterView 服务端单源化):
        - status='queued' 双向兼容 pending (与状态阶梯同层口径一致);
        - account_id 筛选穿透联合执行账号 (params_json.account_ids 数组成员命中)。"""
        with db_cursor() as cursor:
            where_clauses = []
            params: list[Any] = []

            if status:
                if status == "queued":
                    where_clauses.append("status IN ('queued', 'pending')")
                else:
                    where_clauses.append("status = ?")
                    params.append(status)
            if task_type:
                where_clauses.append("task_type = ?")
                params.append(task_type)
            if account_id is not None:
                where_clauses.append(
                    "(account_id = ? OR EXISTS ("
                    "SELECT 1 FROM json_each(CASE WHEN json_valid(params_json) "
                    "THEN COALESCE(json_extract(params_json, '$.account_ids'), json_array()) "
                    "ELSE json_array() END) WHERE value = ?))"
                )
                params.extend([int(account_id), int(account_id)])
            if query:
                where_clauses.append("(id LIKE ? OR current_item LIKE ? OR params_json LIKE ?)")
                like_pat = f"%{query}%"
                params.extend([like_pat, like_pat, like_pat])

            where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

            # 查询符合条件的总记录数
            cursor.execute(f"SELECT COUNT(*) FROM task_queue{where_sql}", tuple(params))
            total_count = cursor.fetchone()[0]

            # 分页查询 (状态优先级阶梯 + 活跃任务 FIFO 入库升序 + 终态任务最近优先倒序,
            # 排序口径单点: TASK_QUEUE_ORDER_SQL 与 get_all 共用)
            offset = max(0, (page - 1) * limit)
            query_sql = f"SELECT * FROM task_queue{where_sql} {TASK_QUEUE_ORDER_SQL} LIMIT ? OFFSET ?"
            exec_params = list(params)
            exec_params.extend([limit, offset])
            cursor.execute(query_sql, tuple(exec_params))
            rows = [dict(r) for r in cursor.fetchall()]

            # 获取当页任务的日志水位统计 (单点: _fetch_log_watermarks 与 get_all 共用)
            stats = _fetch_log_watermarks(cursor, [r["id"] for r in rows])

        return _finalize_task_rows(rows, stats), total_count

    @staticmethod
    def count_by_status() -> dict[str, int]:
        """全库级状态统计 (GET /api/tasks status_counts 单点, 不受筛选参数影响):
        覆盖全部状态枚举键 (缺省补 0) 并附带 all 总计, 反映全局队列真实水位。"""
        with db_cursor() as cursor:
            cursor.execute("SELECT status, COUNT(*) AS cnt FROM task_queue GROUP BY status")
            rows = {dict(r)["status"]: int(dict(r)["cnt"]) for r in cursor.fetchall()}
        counts: dict[str, int] = {
            status: rows.get(status, 0)
            for status in ("pending", "queued", "running", "paused", "completed", "failed", "cancelled")
        }
        # 防御: 枚举外的历史残留状态也计入 (keys 原样保留 + all 为全表真实总计)
        for status, cnt in rows.items():
            if status not in counts:
                counts[status] = cnt
        counts["all"] = sum(counts.values())
        return counts

    @staticmethod
    def retry_task(
        task_id: str,
        *,
        expected_status: str | None = None,
        expected_updated_at: float | None = None,
        params_patch: dict[str, Any] | None = None,
    ) -> bool:
        """重置已失败/已取消/已暂停的任务为排队态以供重新调度 (委托至 reset_task_for_retry)"""
        return TaskQueueRepository.reset_task_for_retry(
            task_id,
            retry_count=0,
            next_retry_at=0.0,
            error_detail="",
            from_statuses=(expected_status,) if expected_status else ("failed", "cancelled", "paused"),
            expected_updated_at=expected_updated_at,
            params_patch=params_patch,
        )

    @staticmethod
    def get_all(limit: int = 50) -> list[dict[str, Any]]:
        """任务列表瘦身: 不再内嵌全量 logs (改由 /api/tasks/{id}/logs 按需差量拉取),
        采用多状态优先级与活跃任务 FIFO 动态排序，防止长耗时 running 任务被历史已完成挤出
        (排序/水位/收尾口径单点: TASK_QUEUE_ORDER_SQL + _fetch_log_watermarks + _finalize_task_rows)"""
        with db_cursor() as cursor:
            cursor.execute(f"SELECT * FROM task_queue {TASK_QUEUE_ORDER_SQL} LIMIT ?", (limit,))
            rows = [dict(r) for r in cursor.fetchall()]
            stats = _fetch_log_watermarks(cursor, [r["id"] for r in rows])
        return _finalize_task_rows(rows, stats)

    @staticmethod
    def get_ids_by_statuses(statuses: tuple[str, ...]) -> list[str]:
        """按状态集合查询任务 ID列表 (供队列级 pause/resume 等批量控制使用)"""
        if not statuses:
            return []
        placeholders = ",".join("?" for _ in statuses)
        with db_cursor() as cursor:
            cursor.execute(f"SELECT id FROM task_queue WHERE status IN ({placeholders})", tuple(statuses))
            return [r["id"] for r in cursor.fetchall()]

    @staticmethod
    def get_max_next_retry_at(task_type: str, status: str = "queued", min_retry_at: float = 0.0) -> float | None:
        """获取指定任务类型和状态下超过 min_retry_at 的最大重试时间戳 (用于 PikPak 冷却恢复等)"""
        with db_cursor() as cursor:
            cursor.execute(
                """
                SELECT MAX(next_retry_at) FROM task_queue
                WHERE task_type = ?
                AND status = ?
                AND next_retry_at > ?
                """,
                (task_type, status, min_retry_at),
            )
            row = cursor.fetchone()
            if row and row[0] is not None:
                return float(row[0])
            return None

    @staticmethod
    def get_active_download_tasks() -> list[dict[str, Any]]:
        """查询当前处于活跃（进行中/排队中/暂停中）的下载类任务"""
        with db_cursor() as cursor:
            cursor.execute(
                "SELECT id, status FROM task_queue WHERE task_type LIKE 'download%' "
                "AND status IN ('running', 'pending', 'queued', 'paused')"
            )
            return [dict(r) for r in cursor.fetchall()]

    @staticmethod
    def get_orphaned_tasks(
        statuses: tuple[str, ...] = ("running", "pending", "queued"),
    ) -> list[dict[str, Any]]:
        """服务重启时拉取未完成任务元信息 (轻量仅取 id, task_type, status)"""
        if not statuses:
            return []
        placeholders = ",".join("?" for _ in statuses)
        with db_cursor() as cursor:
            cursor.execute(
                f"SELECT id, task_type, status FROM task_queue WHERE status IN ({placeholders})",
                tuple(statuses),
            )
            return [dict(r) for r in cursor.fetchall()]

    # ---- 强类型 async 门面绑定 (ParamSpec 签名透传与静态推断) ----
    update_progress_async = _AsyncFace(update_progress)
    merge_params_async = _AsyncFace(merge_params)
    update_status_async = _AsyncFace(update_status)
    update_concurrency_async = _AsyncFace(update_concurrency)
    update_account_id_async = _AsyncFace(update_account_id)
    get_by_id_async = _AsyncFace(get_by_id)
    retry_task_async = _AsyncFace(retry_task)
    requeue_task_for_resume_async = _AsyncFace(requeue_task_for_resume)
    pause_pikpak_for_risk_async = _AsyncFace(pause_pikpak_for_risk)
    mark_retry_async = _AsyncFace(mark_retry)
    query_tasks_async = _AsyncFace(query_tasks)
    count_by_status_async = _AsyncFace(count_by_status)
    reset_task_for_retry_async = _AsyncFace(reset_task_for_retry)
    prioritize_task_async = _AsyncFace(prioritize_task)
    delete_async = _AsyncFace(delete)
    clear_finished_async = _AsyncFace(clear_finished)
    get_all_async = _AsyncFace(get_all)
    get_task_logs_async = _AsyncFace(get_task_logs)
    prune_task_logs_async = _AsyncFace(prune_task_logs)
    record_part_checkpoint_async = _AsyncFace(record_part_checkpoint)
    clear_part_checkpoint_async = _AsyncFace(clear_part_checkpoint)
    get_ids_by_statuses_async = _AsyncFace(get_ids_by_statuses)
    set_error_detail_async = _AsyncFace(set_error_detail)
