"""进程级运行时共享状态 (Runtime Shared State)

说明:
- 提供跨线程安全互斥保护 (threading.RLock), 消除事件循环协程与后台执行器工作线程间的无锁竞态
- 承载活跃任务控制信号 (active_task_controls)、活跃下载占用路径 (active_download_paths)
- 提供统一执行器入口竞态自检 (try_claim_executor_entry) 与跨线程 check-then-act 互斥守卫
"""

import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, Literal

_runtime_lock = threading.RLock()
_task_transition_locks = tuple(threading.Lock() for _ in range(64))


def _task_transition_lock(task_id: str) -> threading.Lock:
    return _task_transition_locks[hash(task_id) % len(_task_transition_locks)]


class ThreadSafeTaskControls(dict):
    """带 RLock 互斥保护的线程安全任务控制字典 (完全兼容现有 dict API)"""

    def __getitem__(self, key: str) -> dict[str, bool]:
        with _runtime_lock:
            return super().__getitem__(key)

    def __setitem__(self, key: str, value: dict[str, bool]):
        with _runtime_lock:
            super().__setitem__(key, value)

    def __delitem__(self, key: str):
        with _runtime_lock:
            super().__delitem__(key)

    def __contains__(self, key: object) -> bool:
        with _runtime_lock:
            return super().__contains__(key)

    def __len__(self) -> int:
        with _runtime_lock:
            return super().__len__()

    def __iter__(self):
        with _runtime_lock:
            return iter(list(super().keys()))

    def get(self, key: str, default: Any = None) -> Any:
        with _runtime_lock:
            val = super().get(key, default)
            return dict(val) if isinstance(val, dict) else val

    def setdefault(self, key: str, default: Any = None) -> Any:
        with _runtime_lock:
            return super().setdefault(key, default)

    def pop(self, key: str, default: Any = None) -> Any:
        with _runtime_lock:
            return super().pop(key, default)

    def clear(self):
        with _runtime_lock:
            super().clear()

    def values(self):
        with _runtime_lock:
            return list(super().values())

    def items(self):
        with _runtime_lock:
            return list(super().items())

    def keys(self):
        with _runtime_lock:
            return list(super().keys())


class ThreadSafePathRefRegistry:
    """引用计数路径注册表 (存储删除守卫): add=占用 +1, release=释放 -1 (归零才移除)。

    两个下载共享同一路径 (同频道同消息重下 / 断点续传重入) 时, 集合语义的 discard 会在
    首个下载结束时提前摘除守卫, 第二个仍在写入的文件失去删除保护 —— 引用计数修复。"""

    def __init__(self) -> None:
        self._ref_counts: dict[str, int] = {}

    def add(self, element: str) -> None:
        with _runtime_lock:
            self._ref_counts[element] = self._ref_counts.get(element, 0) + 1

    def release(self, element: str) -> None:
        """释放一个占用; 计数归零才真正移除 (多余 release 防御性忽略并告警)"""
        with _runtime_lock:
            count = self._ref_counts.get(element, 0)
            if count <= 0:
                if element in self._ref_counts:
                    del self._ref_counts[element]
                return
            if count == 1:
                del self._ref_counts[element]
            else:
                self._ref_counts[element] = count - 1

    def __contains__(self, element: object) -> bool:
        with _runtime_lock:
            return element in self._ref_counts

    def __iter__(self) -> Iterator[str]:
        with _runtime_lock:
            return iter(list(self._ref_counts.keys()))

    def __len__(self) -> int:
        with _runtime_lock:
            return len(self._ref_counts)

    def clear(self) -> None:
        with _runtime_lock:
            self._ref_counts.clear()

    def ref_count(self, element: str) -> int:
        with _runtime_lock:
            return self._ref_counts.get(element, 0)


# 运行中的任务控制信号 map: task_id -> {"cancelled": bool, "paused": bool}
active_task_controls: ThreadSafeTaskControls = ThreadSafeTaskControls()

# 活跃下载正在占用的文件绝对路径集合 (save_path/.part/.meta), 存储删除的强制守卫
# (引用计数语义: add=占用+1 / release=释放-1, 归零才摘除守卫)
active_download_paths: ThreadSafePathRefRegistry = ThreadSafePathRefRegistry()


@contextmanager
def runtime_state_lock() -> Iterator[None]:
    """跨线程 check-then-act 互斥守卫: 供存储删除等「持锁复查 + 执行」序列原子化,
    堵住快照 TOCTOU (登记发生在快照之后、unlink 之前时, 快照守卫看不见新路径)。"""
    with _runtime_lock:
        yield


def set_task_control(task_id: str, cancelled: bool | None = None, paused: bool | None = None) -> None:
    """原子更新任务控制标志，杜绝跨线程读写撕裂"""
    with _runtime_lock:
        ctrl = active_task_controls.setdefault(task_id, {"cancelled": False, "paused": False})
        if cancelled is not None:
            ctrl["cancelled"] = cancelled
        if paused is not None:
            ctrl["paused"] = paused
            if paused:
                ctrl.pop("resumed_in_place", None)


def is_task_cancelled(task_id: str) -> bool:
    """原子查询任务是否已被取消"""
    with _runtime_lock:
        return bool(active_task_controls.get(task_id, {}).get("cancelled", False))


def is_task_paused(task_id: str) -> bool:
    """原子查询任务是否已被暂停"""
    with _runtime_lock:
        return bool(active_task_controls.get(task_id, {}).get("paused", False))


def acquire_queued_task(task_id: str, account_id: int | None, *, expected_updated_at: float | None = None) -> bool:
    from app.db.task_repository import TaskQueueRepository

    with _task_transition_lock(task_id):
        return TaskQueueRepository.acquire_task(task_id, account_id=account_id, expected_updated_at=expected_updated_at)


def requeue_task_and_clear_controls(task_id: str, requeue: Callable[[], bool]) -> bool:
    """仅清理成功入队的旧控制项，并让调度认领及停止操作等到清理结束。"""
    with _task_transition_lock(task_id):
        if not requeue():
            return False
        active_task_controls.pop(task_id, None)
        return True


def stop_task_with_controls(
    task_id: str, status: Literal["paused", "cancelled"], from_statuses: tuple[str, ...]
) -> bool:
    from app.db.task_repository import TaskQueueRepository

    with _task_transition_lock(task_id):
        current = TaskQueueRepository.get_status(task_id)
        if current is None or current not in from_statuses:
            return False
        if not TaskQueueRepository.update_status(task_id, status, from_statuses=(current,)):
            return False
        with _runtime_lock:
            if current == "running" or task_id in active_task_controls:
                set_task_control(task_id, **{status: True})
        return True


def resume_paused_executor(
    task_id: str,
    expected_updated_at: float | None,
    *,
    params_patch: dict[str, Any] | None = None,
    error_detail: str | None = None,
) -> bool:
    from app.db.task_repository import TaskQueueRepository

    with _task_transition_lock(task_id):
        with _runtime_lock:
            controls: dict[str, bool] | None = dict.get(active_task_controls, task_id)
            if controls is None or any(
                controls.get(flag) for flag in ("cancelled", "pause_exiting", "executor_exiting")
            ):
                return False
        resume_controls: dict[str, bool] = controls

        @contextmanager
        def guard_commit() -> Iterator[bool]:
            with _runtime_lock:
                # A pause exit can still settle an in-flight resume. Other exits cannot keep it alive.
                allowed = dict.get(active_task_controls, task_id) is resume_controls and not any(
                    resume_controls.get(flag) for flag in ("cancelled", "executor_exiting")
                )
                yield allowed
                if allowed:
                    resume_controls["paused"] = False
                    resume_controls["resumed_in_place"] = True

        return TaskQueueRepository.resume_paused_task(
            task_id,
            expected_updated_at,
            params_patch=params_patch,
            error_detail=error_detail,
            commit_guard=guard_commit(),
        )


def mark_scheduler_managed_executor(task_id: str) -> None:
    with _runtime_lock:
        controls = active_task_controls.setdefault(task_id, {"cancelled": False, "paused": False})
        controls["scheduler_managed"] = True


def release_runner_controls(task_id: str) -> None:
    """调度器拥有执行器控制项直至完成收尾，独立执行的 runner 自行清理。"""
    with _runtime_lock:
        if not active_task_controls.get(task_id, {}).get("scheduler_managed"):
            active_task_controls.pop(task_id, None)


def mark_task_pause_exit(task_id: str) -> None:
    with _runtime_lock:
        controls = active_task_controls.setdefault(task_id, {"cancelled": False, "paused": False})
        controls["pause_exiting"] = True


def mark_task_executor_exit(task_id: str) -> None:
    """执行器异常或取消收尾时拒绝原地恢复，包括已进入数据库事务的请求。"""
    with _runtime_lock:
        controls = active_task_controls.setdefault(task_id, {"cancelled": False, "paused": False})
        controls["executor_exiting"] = True


def settle_task_pause_exit(task_id: str) -> dict[str, bool] | None:
    """完成暂停状态转换并保留已接受的恢复意图供调度器重新入队。"""
    from app.db.task_repository import TaskQueueRepository

    with _task_transition_lock(task_id):
        with _runtime_lock:
            controls = dict.get(active_task_controls, task_id, {})
        if controls.get("cancelled"):
            return None
        TaskQueueRepository.update_status(task_id, "paused", from_statuses=("running",))
        if not controls.get("resumed_in_place") or controls.get("paused"):
            return None
        return controls if TaskQueueRepository.get_status(task_id) == "paused" else None


def requeue_resumed_pause_exit(task_id: str, resume_controls: dict[str, bool]) -> bool:
    from app.db.task_repository import TaskQueueRepository

    with _task_transition_lock(task_id):
        with _runtime_lock:
            current = dict.get(active_task_controls, task_id)
            if current is not resume_controls:
                return False
            active_task_controls.pop(task_id, None)
            if not current.get("resumed_in_place") or current.get("paused") or current.get("cancelled"):
                return False
        return TaskQueueRepository.requeue_task_for_resume(task_id, "paused", params_patch={})


def try_claim_executor_entry(task_id: str) -> bool:
    """执行器入口统一自检 (调度窗口竞态修复, 线程安全互斥保护):
    1. 控制项 setdefault 合并语义, 绝不整体覆盖既有标志 —— 用户在「调度器认领 -> 执行器启动」
       窗口内置位的取消/暂停必须保留 (历史覆盖写法会把标志抹掉, 任务照常下载后最终以
       completed 覆盖 DB 中的 cancelled/paused 终态);
    2. DB 已处终态 (cancelled/failed/paused) 或标志已置位时返回 False, 执行器应立即退出:
       不下载、不改写 running, 幂等补写对应终态并清理控制项 (此时无存活协程, 不会残留引用)。
    """
    # 函数内导入: 保持零重量模块导入风格，避免循环依赖
    from app.db.task_repository import TaskQueueRepository

    with _task_transition_lock(task_id):
        db_status = TaskQueueRepository.get_status(task_id)
        terminal_to_update: str | None = None
        with _runtime_lock:
            ctrl = active_task_controls.setdefault(task_id, {"cancelled": False, "paused": False})
            if db_status in ("cancelled", "failed", "paused") or ctrl.get("cancelled") or ctrl.get("paused"):
                terminal_to_update = (
                    "cancelled" if ctrl.get("cancelled") else ("paused" if ctrl.get("paused") else db_status)
                )
                claimed = False
            else:
                claimed = True

        if not claimed:
            if terminal_to_update:
                TaskQueueRepository.update_status(
                    task_id,
                    terminal_to_update,
                    from_statuses=(db_status,) if db_status is not None else None,
                )
            with _runtime_lock:
                active_task_controls.pop(task_id, None)
        return claimed
