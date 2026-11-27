import asyncio
import logging
import random
import time
from collections import defaultdict
from collections.abc import Callable

from telethon.errors import FloodWaitError

from app.config import settings
from app.core.client_manager import client_manager
from app.core.connection_manager import connection_manager
from app.core.runtime import (
    acquire_queued_task,
    mark_scheduler_managed_executor,
    mark_task_executor_exit,
    mark_task_pause_exit,
    requeue_resumed_pause_exit,
    settle_task_pause_exit,
)
from app.db.account_repository import SystemSettingsRepository
from app.db.task_repository import DuplicateDownloadTaskError, TaskQueueRepository

logger = logging.getLogger(__name__)


class TaskControlFlowError(Exception):
    """任务控制流基础异常"""


class TaskPausedError(TaskControlFlowError):
    """显式任务暂停异常：让位退出当前协程，保留任务状态与断点"""


class TaskCancelledError(TaskControlFlowError):
    """显式任务取消异常：中止当前协程，释放资源"""


class UploadCancelledError(TaskCancelledError):
    """上传任务取消信号异常"""


class UploadPausedError(TaskPausedError):
    """上传任务暂停信号异常"""


class RetryableTaskError(Exception):
    """可恢复瞬态异常：触发调度器自适应退避重试"""

    retry_after: float | None = None
    flood_client_marked: bool = False
    count_retry: bool = True
    is_flood_wait: bool = True

    def __init__(
        self,
        *args: object,
        retry_after: float | None = None,
        flood_client_marked: bool = False,
        count_retry: bool = True,
        is_flood_wait: bool = True,
    ) -> None:
        super().__init__(*args)
        if retry_after is not None:
            self.retry_after = retry_after
        if flood_client_marked:
            self.flood_client_marked = True
        if not count_retry:
            self.count_retry = False
        if not is_flood_wait:
            self.is_flood_wait = False


def _extract_retryable_flood_wait_seconds(exc: BaseException) -> int:
    """从 RetryableTaskError 的 retry_after 或包装因果链中提取 Telegram FloodWait 秒数（0 表示非 FloodWait）."""
    direct_wait = getattr(exc, "retry_after", None)
    if isinstance(direct_wait, (int, float)) and direct_wait > 0:
        return int(direct_wait)
    visited: set[int] = set()
    cur: BaseException | None = exc.__cause__
    while cur is not None and id(cur) not in visited:
        visited.add(id(cur))
        if isinstance(cur, FloodWaitError):
            return int(getattr(cur, "seconds", 60) or 60)
        inner_cause = getattr(cur, "cause", None)
        if isinstance(inner_cause, BaseException) and id(inner_cause) not in visited:
            cur = inner_cause
            continue
        cur = cur.__cause__
    return 0


class FatalTaskError(Exception):
    """不可恢复致命异常：直接标记任务失败，终止重试"""


class _AwaitableNone:
    """双模返回值: 允许普通同步调用，同时支持被 await (不引发 TypeError)"""

    def __await__(self):
        async def _dummy():
            return None

        return _dummy().__await__()


_AWAITABLE_NONE = _AwaitableNone()


class TaskScheduler:
    """智能任务调度器：负责全局与单账号并发控制、优先级排队、异常退避重试与孤儿任务恢复"""

    # 全局最大并发下载分块线程数 (水门配额，防跨任务线程数叠加失控；完整的按 DC 感知限流登记为未来项)
    MAX_GLOBAL_DOWNLOAD_THREADS: int = 32
    GRACEFUL_STOP_TIMEOUT: float = getattr(settings, "GRACEFUL_STOP_TIMEOUT", 10.0)
    SLOT_CONTENTION_DELAY_SECONDS = 15.0

    def __init__(self, check_interval: float = 1.0):
        self.check_interval = check_interval
        self._is_running: bool = False
        self._stopping: bool = False
        self.startup_degraded: bool = False
        self._loop_task: asyncio.Task | None = None
        self._notify_event: asyncio.Event | None = None
        self._lock: asyncio.Lock | None = None

        # 内存中正在运行的任务 Task 对象: task_id -> asyncio.Task
        self._running_tasks: dict[str, asyncio.Task] = {}
        # 每个账号当前运行的任务计数: account_id -> count
        self._account_task_counts: dict[int, int] = {}
        # 记录每个任务当前实际分配绑定的主账号 ID: task_id -> account_id
        self._task_assigned_accounts: dict[str, int] = {}
        # 全局活跃下载分块线程计数 (总闸)
        self._global_download_threads: int = 0
        # 任务已分配的全局下载线程记账: task_id -> threads
        self._task_thread_allocations: dict[str, int] = {}
        # 梯队 2: 任务借用的辅助 Userbot 账号: task_id -> set(aux_account_id, ...)
        self._task_auxiliary_accounts: dict[str, set[int]] = defaultdict(set)
        # 自定义任务执行器注册表: task_type -> async callable(task_id, params, account_id)
        self._executors: dict[str, Callable] = {}
        # 后台广播任务强引用集合 (防止 RUF006 dangling task 被 GC 提前回收)
        self._bg_tasks: set[asyncio.Task] = set()
        # 进程级全局 PikPak 冷却保护锁截止时间戳 (防多任务车轮战)
        self._pikpak_cooldown_until: float = 0.0
        # 占位槽 editing 争用等待次数跟踪 (独立于故障 retry_count): task_id -> wait_count
        self._slot_contention_waits: set[str] = set()
        try:
            from app.core.metrics import download_threads_in_use

            download_threads_in_use.set(0)
        except Exception as m_err:
            logger.debug("初始化下载线程指标异常: %s", m_err)

    def set_pikpak_cooldown(self, until_timestamp: float) -> None:
        """设置进程级全局 PikPak 冷却锁 (防多任务车轮战)"""
        if until_timestamp > self._pikpak_cooldown_until:
            self._pikpak_cooldown_until = until_timestamp
            logger.info(
                "TaskScheduler: 全局 PikPak 冷却锁已更新至 %.1f (倒计时 %.1fs)",
                until_timestamp,
                max(0.0, until_timestamp - time.time()),
            )

    def clear_pikpak_cooldown(self) -> None:
        """重置全局 PikPak 冷却锁"""
        self._pikpak_cooldown_until = 0.0

    def is_pikpak_in_cooldown(self) -> bool:
        """检查全局 PikPak 是否处于冷却状态"""
        return time.time() < self._pikpak_cooldown_until

    def _restore_pikpak_cooldown(self) -> None:
        """从数据库恢复未到期的全局 PikPak 冷却锁 (服务重启或灾备恢复后保护)"""
        try:
            now = time.time()
            max_until = TaskQueueRepository.get_max_next_retry_at("pikpak_publish", status="queued", min_retry_at=now)
            if max_until and max_until > now:
                self.set_pikpak_cooldown(max_until)
                logger.info(
                    "TaskScheduler: 服务启动恢复全局 PikPak 冷却锁至 %.1f (倒计时 %.1fs)",
                    max_until,
                    max_until - now,
                )
        except Exception as e:
            logger.debug("恢复 PikPak 冷却锁异常 (非致命): %s", e)

    def get_running_task_count(self) -> int:
        """获取当前正在调度器执行器中运行的任务总数"""
        return len(self._running_tasks)

    def get_running_task_ids(self) -> frozenset[str]:
        """获取当前正在调度器执行器中运行的任务 ID 集合 (只读快照)"""
        return frozenset(self._running_tasks.keys())

    def is_task_running(self, task_id: str) -> bool:
        """查询指定任务当前是否在调度器运行集合中"""
        return task_id in self._running_tasks

    def is_waiting_for_slot_contention(self, task_id: str) -> bool:
        return task_id in self._slot_contention_waits

    def clear_slot_contention_wait(self, task_id: str) -> None:
        self._slot_contention_waits.discard(task_id)

    def has_thread_allocation(self, task_id: str) -> bool:
        """查询指定任务当前是否在下载线程分配表中"""
        return task_id in self._task_thread_allocations

    def cleanup_task_resources(self, task_id: str, account_id: int | None):
        """公共契约: 释放已中断或完成任务占用的运行时资源与配额 (委托至 _cleanup_task_resources)"""
        return self._cleanup_task_resources(task_id, account_id)

    def cleanup_finished_executor(self, task_id: str, account_id: int | None) -> None:
        """回收已结束但仍留在运行集合中的执行器。"""
        executor = self._running_tasks.get(task_id)
        if executor is not None and executor.done():
            self._cleanup_task_resources(task_id, account_id)
            self.notify_new_task()

    def get_account_task_count(self, account_id: int) -> int:
        """获取指定账号当前在调度器中活跃运行的任务数"""
        return self._account_task_counts.get(account_id, 0)

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _get_event(self) -> asyncio.Event:
        if self._notify_event is None:
            self._notify_event = asyncio.Event()
        return self._notify_event

    def register_executor(self, task_type: str, handler: Callable):
        """注册特定任务类型的异步执行函数"""
        self._executors[task_type] = handler

    def notify_new_task(self):
        """外部提交新任务或状态变更时主动唤醒调度循环"""
        self._get_event().set()

    def get_max_global_concurrency(self) -> int:
        return SystemSettingsRepository.get_int("max_global_concurrency", 3)

    @property
    def is_running(self) -> bool:
        return self._is_running

    @is_running.setter
    def is_running(self, val: bool) -> None:
        self._is_running = val

    async def start(self):
        """启动后台调度引擎"""
        if self._is_running:
            return
        self._stopping = False
        self._is_running = True
        try:
            await asyncio.to_thread(self._restore_pikpak_cooldown)
        except BaseException:
            self._is_running = False
            raise
        self._loop_task = asyncio.create_task(self._scheduler_loop())
        logger.info("TaskScheduler 智能调度引擎已启动 (全局最大并发: %d)", self.get_max_global_concurrency())

    async def stop(self):
        """三段式优雅停止调度引擎：
        1. active_task_controls 存活控制项置 paused=True (安全暂停、断点留存语义，保留已取消任务优先级)
        2. 同步等待在途调度扫描退出后，asyncio.wait 收割 self._running_tasks 全部协程，DB 状态幂等对齐 paused
        3. 最后取消调度主循环协程
        """
        self._stopping = True
        self._is_running = False
        self.notify_new_task()

        try:
            # 等待在途 _scan_queued_candidates 持锁扫描退出，防止快照后仍有新任务被派发
            async with self._get_lock():
                pass

            # 阶段 1: 将 active_task_controls 中非取消态存活控制项置 paused=True
            self._mark_running_tasks_paused()

            # 阶段 2: asyncio.wait 收割 self._running_tasks 全部协程
            harvest_tasks = list(self._running_tasks.items())
            still_running_tids: set[str] = set()
            if harvest_tasks:
                coros = [t for _, t in harvest_tasks if not t.done()]
                if coros:
                    timeout = getattr(self, "GRACEFUL_STOP_TIMEOUT", 10.0)
                    done, pending = await asyncio.wait(coros, timeout=timeout)
                    if pending:
                        logger.warning(
                            "TaskScheduler: %d 个任务协程未在 %.1fs 内响应暂停，启动强制 cancel 兜底",
                            len(pending),
                            timeout,
                        )
                        for t in pending:
                            t.cancel()
                        await asyncio.wait(pending, timeout=5.0)

                # 对已收割任务，将 DB 状态幂等补写为 paused (终态任务如 completed/failed/cancelled 不覆盖)
                still_running_tids = {tid for tid, t in harvest_tasks if not t.done()}
                for tid, t in harvest_tasks:
                    try:
                        rec = await asyncio.to_thread(TaskQueueRepository.get_by_id, tid)
                        cur_status = rec.get("status") if rec else None
                        if cur_status and cur_status not in ("completed", "failed", "cancelled", "paused"):
                            await asyncio.to_thread(TaskQueueRepository.update_status, tid, "paused", (cur_status,))
                    except Exception as e:
                        logger.warning("优雅关闭时更新任务 [%s] DB 状态失败: %s", tid, e)
                    if t.done():
                        from app.core.runtime import active_task_controls

                        active_task_controls.pop(tid, None)
                self._running_tasks.clear()
                self._global_download_threads = 0
                self._task_thread_allocations.clear()
                try:
                    from app.core.metrics import download_threads_in_use, running_tasks

                    running_tasks.set(0)
                    download_threads_in_use.set(0)
                except Exception as m_err:
                    logger.debug("停止调度器重置指标异常: %s", m_err)

            # 仅清除非运行态任务的控制项，保留仍有后台线程存活任务的暂停标志
            self._cleanup_stopped_task_controls(harvest_tasks, still_running_tids)

            # 阶段 3: 取消调度循环协程
            if self._loop_task and not self._loop_task.done():
                self._loop_task.cancel()
                try:
                    await self._loop_task
                except asyncio.CancelledError:
                    logger.debug("调度器主循环协程已确认取消终止")
            logger.info("TaskScheduler 智能调度引擎已停止")
        finally:
            self._stopping = False

    def _mark_running_tasks_paused(self) -> None:
        """将存活任务控制项置 paused=True (保留已取消任务优先级)."""
        from app.core.runtime import active_task_controls

        for tid in list(self._running_tasks.keys()):
            ctrl = active_task_controls.setdefault(tid, {"cancelled": False, "paused": False})
            if not ctrl.get("cancelled"):
                ctrl["paused"] = True
        for ctrl in active_task_controls.values():
            if not ctrl.get("cancelled"):
                ctrl["paused"] = True

    @staticmethod
    def _cleanup_stopped_task_controls(harvest_tasks: list, still_running_tids: set[str]) -> None:
        """清理已退出任务的控制项，保留仍有后台线程存活任务的暂停标志"""
        from app.core.runtime import active_task_controls

        if not harvest_tasks:
            active_task_controls.clear()
            return
        for tid in list(active_task_controls.keys()):
            if tid not in still_running_tids:
                active_task_controls.pop(tid, None)

    async def schedule_once(self) -> int:
        """执行单次调度扫描 (可供测试或即时触发使用，返回本次新调度的任务数)"""
        prev_count = len(self._running_tasks)
        await self._schedule_next_tasks()
        return len(self._running_tasks) - prev_count

    async def _scheduler_loop(self):
        """核心调度主循环"""
        while self._is_running:
            try:
                # 等待事件唤醒或心跳超时
                try:
                    await asyncio.wait_for(self._get_event().wait(), timeout=self.check_interval)
                except TimeoutError:
                    logger.debug("调度心跳等待超时，继续本轮调度扫描")
                self._get_event().clear()

                if not self._is_running:
                    break

                await self._schedule_next_tasks()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("调度主循环异常: %s", e, exc_info=True)
                await asyncio.sleep(1.0)

    async def _schedule_next_tasks(self):
        """尝试从队列拉取并派发符合并发与配额要求的任务"""
        try:
            from app.core.metrics import queued_task_depth, running_tasks

            running_tasks.set(len(self._running_tasks))
            q_count = await asyncio.to_thread(TaskQueueRepository.get_queued_count)
            queued_task_depth.set(q_count)
        except Exception as m_err:
            logger.debug("更新调度深度指标异常: %s", m_err)

        max_global = await asyncio.to_thread(self.get_max_global_concurrency)

        rejected_accounts: list[tuple[str, int, str]] = []
        try:
            await self._scan_queued_candidates(max_global, rejected_accounts)
        finally:
            for task_id, account_id, error in rejected_accounts:
                try:
                    await self._notify_account_rejection(task_id, account_id, error)
                except Exception as notify_err:
                    logger.warning("通知账号拒绝失败 (task_id=%s): %s", task_id, notify_err)

    async def _scan_queued_candidates(self, max_global: int, rejected_accounts: list[tuple[str, int, str]]) -> None:
        async with self._get_lock():
            if self._stopping:
                return
            # 1. 检查全局并发限制
            if len(self._running_tasks) >= max_global:
                return

            # 本轮调度已饱和/不可用的资源追踪，防止队头阻塞 (Head-of-Line Blocking)
            saturated_accounts: set[int] = set()

            # 2. 游标滚动扫描排队候选任务 (批次 50，最大扫描预算 500)
            offset = 0
            batch_size = 50
            max_scan_budget = 500
            total_scanned = 0

            while not self._stopping and len(self._running_tasks) < max_global and total_scanned < max_scan_budget:
                candidates = await asyncio.to_thread(
                    TaskQueueRepository.get_queued_candidates, limit=batch_size, offset=offset
                )

                if self._stopping or not candidates:
                    break

                for candidate in candidates:
                    total_scanned += 1
                    if self._stopping or len(self._running_tasks) >= max_global:
                        break

                    task_id = candidate["id"]
                    if task_id in self._running_tasks:
                        offset += 1
                        continue
                    task_type = candidate["task_type"]
                    params = candidate.get("params") or {}
                    raw_acc_id = candidate.get("account_id")

                    # 2.5 进程级全局 PikPak 冷却保护锁检查 (防多任务车轮战)
                    if task_type == "pikpak_publish" and time.time() < self._pikpak_cooldown_until:
                        logger.debug(
                            "全局 PikPak 处于冷却保护中 (剩余 %.1fs)，跳过调度任务 [%s]",
                            self._pikpak_cooldown_until - time.time(),
                            task_id,
                        )
                        offset += 1
                        continue

                    # 3. 确定并校验账号配额 (支持私有频道感知与优先分级选择)
                    from app.core.task_strategy import StrategyRegistry

                    strategy = StrategyRegistry.get_optional(task_type)
                    target_chat = strategy.extract_target_chat(params) if strategy else None

                    if raw_acc_id is None and params.get("_deleted_account_id") is not None:
                        await self._reject_unavailable_explicit_account(
                            task_id, params["_deleted_account_id"], deferred=rejected_accounts
                        )
                        continue
                    if raw_acc_id is not None:
                        if raw_acc_id <= 0:
                            acc = None
                        else:
                            if raw_acc_id in saturated_accounts:
                                offset += 1
                                continue
                            acc = client_manager.get_account(raw_acc_id)
                        if acc is None or not acc.is_active:
                            await self._reject_unavailable_explicit_account(
                                task_id, raw_acc_id, deferred=rejected_accounts
                            )
                            continue
                        dispatch_account_id: int | None = acc.account_id
                        quota_bucket_id: int = acc.account_id
                    else:
                        acc = await client_manager.get_available_account(
                            task_type, target_chat=target_chat, exclude_account_ids=saturated_accounts
                        )
                        if acc is None and saturated_accounts:
                            offset += 1
                            continue
                        dispatch_account_id = acc.account_id if acc else None
                        quota_bucket_id = dispatch_account_id if dispatch_account_id is not None else 0

                    max_acc_concurrency = acc.max_concurrency if acc else 1

                    # 检查账号是否处于饱和集合中
                    if quota_bucket_id in saturated_accounts:
                        offset += 1
                        continue

                    # 检查账号是否处于 FloodWait 封锁中
                    if acc and acc.is_flood_locked():
                        logger.debug("账号 [%s] 处于 FloodWait 冷却中，跳过调度任务 [%s]", acc.account_name, task_id)
                        saturated_accounts.add(quota_bucket_id)
                        offset += 1
                        continue

                    # 检查单账号并发配额
                    current_acc_count = self._account_task_counts.get(quota_bucket_id, 0)
                    if current_acc_count >= max_acc_concurrency:
                        logger.debug(
                            "账号 ID [%s] 并发已达上限 (%d/%d)，跳过调度任务 [%s]",
                            quota_bucket_id,
                            current_acc_count,
                            max_acc_concurrency,
                            task_id,
                        )
                        saturated_accounts.add(quota_bucket_id)
                        offset += 1
                        continue

                    # 3.1 全局活跃下载分块线程总闸门检查 (≤ 32 线程，防跨任务线程数叠加失控)
                    is_download = task_type in ("download_link", "download_range")
                    task_threads = 0
                    if is_download:
                        default_threads = int(getattr(settings, "DOWNLOAD_PARALLEL_CONNECTIONS", 8) or 8)
                        raw_threads = candidate.get("concurrency_total") or params.get("threads") or default_threads
                        try:
                            task_threads = int(raw_threads)
                        except (ValueError, TypeError):
                            task_threads = default_threads
                        task_threads = max(1, min(task_threads, self.MAX_GLOBAL_DOWNLOAD_THREADS))
                        if self._global_download_threads + task_threads > self.MAX_GLOBAL_DOWNLOAD_THREADS:
                            logger.debug(
                                "全局活跃下载线程已饱和 (%d/%d, 本次申请 %d)，跳过任务 [%s] 保留排队",
                                self._global_download_threads,
                                self.MAX_GLOBAL_DOWNLOAD_THREADS,
                                task_threads,
                                task_id,
                            )
                            offset += 1
                            continue

                    # 4. CAS 尝试认领任务并原子回填实际分配的 account_id
                    acquired = await asyncio.to_thread(
                        acquire_queued_task,
                        task_id,
                        dispatch_account_id,
                        expected_updated_at=candidate.get("updated_at"),
                    )
                    if not acquired:
                        continue

                    # 4.5 调度窗口自检: 认领到协程实际启动之间存在窗口 (错峰 sleep + 事件循环排队),
                    # 用户可能已取消/暂停。控制项 setdefault 合并语义 (绝不覆盖既有标志); 若已带
                    # 取消/暂停标志则跳过本次派发 —— 不广播 running、不创建协程, DB 终态以用户写入
                    # 为准 (此处仅幂等补写兜底), 避免任务照常执行后用 completed 覆盖 cancelled/paused
                    from app.core.runtime import active_task_controls

                    ctrl = active_task_controls.setdefault(task_id, {"cancelled": False, "paused": False})
                    if ctrl.get("cancelled") or ctrl.get("paused"):
                        terminal = "cancelled" if ctrl.get("cancelled") else "paused"
                        await asyncio.to_thread(TaskQueueRepository.update_status, task_id, terminal)
                        active_task_controls.pop(task_id, None)
                        await TaskQueueRepository.append_log_async(
                            task_id,
                            f"[调度器] 任务在调度窗口内已被用户{'取消' if terminal == 'cancelled' else '暂停'}，跳过派发且不覆盖终态",
                        )
                        logger.info("任务 [%s] 在调度窗口内已被用户%s，跳过派发", task_id, terminal)
                        continue

                    # 广播 WebSocket task_update 附带实际分配绑定的 account_id，驱动前端即刻展示 Worker
                    from app.core.connection_manager import connection_manager

                    bcast_task = asyncio.create_task(
                        connection_manager.broadcast(
                            {
                                "type": "task_update",
                                "task_id": task_id,
                                "status": "running",
                                "account_id": dispatch_account_id,
                            }
                        )
                    )
                    self._bg_tasks.add(bcast_task)
                    bcast_task.add_done_callback(self._bg_tasks.discard)

                    # 5. 占用计数与启动任务协程
                    self._account_task_counts[quota_bucket_id] = self._account_task_counts.get(quota_bucket_id, 0) + 1
                    self._task_assigned_accounts[task_id] = quota_bucket_id

                    if is_download and task_threads > 0:
                        self._global_download_threads += task_threads
                        self._task_thread_allocations[task_id] = task_threads
                        try:
                            from app.core.metrics import download_threads_in_use

                            download_threads_in_use.set(self._global_download_threads)
                        except Exception as m_err:
                            logger.debug("更新全局下载线程指标异常: %s", m_err)

                    t = asyncio.create_task(
                        self._run_task_wrapper(
                            task_id,
                            task_type,
                            params,
                            dispatch_account_id,
                            candidate.get("retry_count", 0),
                            candidate.get("max_retries", 3),
                        )
                    )
                    self._running_tasks[task_id] = t
                    try:
                        from app.core.metrics import running_tasks

                        running_tasks.set(len(self._running_tasks))
                    except Exception as m_err:
                        logger.debug("更新运行中任务指标异常: %s", m_err)
                    logger.info(
                        "任务 [%s] (%s) 已被调度启动 (全局并发: %d/%d, 账号 %s: %d/%d, 全局下载线程: %d/%d)",
                        task_id,
                        task_type,
                        len(self._running_tasks),
                        max_global,
                        dispatch_account_id,
                        self._account_task_counts[quota_bucket_id],
                        max_acc_concurrency,
                        self._global_download_threads,
                        self.MAX_GLOBAL_DOWNLOAD_THREADS,
                    )

                    # [REQ-004] 错峰调度微延时：同一调度 Tick 内启动多任务时打散瞬时网络波峰
                    if len(self._running_tasks) < max_global:
                        jitter = random.uniform(0.01, 0.05) if self.check_interval < 0.2 else random.uniform(0.3, 0.8)
                        await asyncio.sleep(jitter)

                if len(candidates) < batch_size:
                    break

    async def _reject_unavailable_explicit_account(
        self, task_id: str, requested_account_id: int, *, deferred: list[tuple[str, int, str]] | None = None
    ) -> None:
        """显式指定账号已删除/停用/不存在时，在调度准入阶段直接原子置 failed，禁止退化为 0/默认账号。"""
        err_msg = f"指定执行账号 (ID={requested_account_id}) 不存在或已停用，拒绝静默回退默认账号"
        rejected = await asyncio.to_thread(TaskQueueRepository.fail_queued_task, task_id, err_msg)
        if not rejected:
            return
        self.clear_slot_contention_wait(task_id)
        if deferred is not None:
            deferred.append((task_id, requested_account_id, err_msg))
        logger.warning("❌ [调度器] 任务 [%s] %s", task_id, err_msg)
        try:
            await TaskQueueRepository.append_log_async(task_id, f"❌ [调度器] {err_msg}")
        except Exception as log_err:
            logger.warning("记录账号拒绝日志失败 (task_id=%s): %s", task_id, log_err)
        if deferred is None:
            await self._notify_account_rejection(task_id, requested_account_id, err_msg)

    async def _notify_account_rejection(self, task_id: str, requested_account_id: int, err_msg: str) -> None:
        from app.core.pipeline.placeholder import PlaceholderSlotCoordinator

        if not await asyncio.to_thread(PlaceholderSlotCoordinator.matches_terminal_rejection, task_id, err_msg):
            return
        try:
            await PlaceholderSlotCoordinator().finalize_terminal(task_id, expected_error=err_msg)
        except Exception as finalize_err:
            logger.warning("占位槽终态化失败 (task_id=%s): %s", task_id, finalize_err)
        if not await asyncio.to_thread(PlaceholderSlotCoordinator.matches_terminal_rejection, task_id, err_msg):
            return
        try:
            from app.core.connection_manager import connection_manager

            await connection_manager.broadcast(
                {
                    "type": "task_update",
                    "task_id": task_id,
                    "status": "failed",
                    "account_id": requested_account_id,
                    "error_detail": err_msg,
                }
            )
        except Exception as bcast_err:
            logger.debug("广播显式账号失效拒绝状态异常 (非致命): %s", bcast_err)

    async def _run_task_wrapper(
        self, task_id: str, task_type: str, params: dict, account_id: int | None, retry_count: int, max_retries: int
    ):
        """执行任务包装器：捕获异常、自适应退避重试、释放信号量"""
        executor = self._executors.get(task_type)
        if not executor:
            mark_task_executor_exit(task_id)
            try:
                logger.error("未找到任务类型 [%s] 的执行器", task_type)
                await asyncio.to_thread(TaskQueueRepository.update_status, task_id, "failed")
                await TaskQueueRepository.append_log_async(
                    task_id, f"❌ 系统错误: 未找到针对任务类型 [{task_type}] 的执行器"
                )
            finally:
                self.clear_slot_contention_wait(task_id)
                await self._cleanup_task_resources(task_id, account_id)
                self.notify_new_task()
            return

        entered_contention_retry = False
        resume_controls = None
        try:
            mark_scheduler_managed_executor(task_id)
            await executor(task_id, params, account_id)
        except TaskPausedError as pe:
            mark_task_pause_exit(task_id)
            logger.info("任务 [%s] 触发正常让位暂停: %s", task_id, pe)
            resume_controls = await asyncio.to_thread(settle_task_pause_exit, task_id)
        except TaskCancelledError as ce:
            mark_task_executor_exit(task_id)
            logger.info("任务 [%s] 已被取消: %s", task_id, ce)
            current_status = (await TaskQueueRepository.get_by_id_async(task_id) or {}).get("status")
            if current_status != "cancelled":
                await TaskQueueRepository.update_status_async(task_id, "cancelled")
        except asyncio.CancelledError:
            mark_task_executor_exit(task_id)
            logger.info("任务 [%s] 协程被取消退出", task_id)
        except FloodWaitError as fwe:
            mark_task_executor_exit(task_id)
            wait_sec = getattr(fwe, "seconds", 60) or 60
            if account_id is not None:
                client_manager.mark_flood_wait(account_id, wait_sec)
            await self._handle_task_retry(
                task_id, retry_count, max_retries, f"Telegram 限速冷却 (FloodWait {wait_sec}s)", min_delay=wait_sec
            )
        except Exception as exc:
            mark_task_executor_exit(task_id)
            from app.core.pikpak.taxonomy import (
                PikPakPublishRiskControlError,
                PikPakPublishThrottledError,
            )
            from app.core.pikpak.taxonomy import PikPakRiskControlError as CliPikPakRiskControlError
            from app.core.stream_chunk_provider import (
                PikPakRiskControlError as ProviderPikPakRiskControlError,
            )
            from app.core.stream_chunk_provider import (
                PikPakThrottledError,
            )

            is_throttled = isinstance(exc, (PikPakPublishThrottledError, PikPakThrottledError)) or (
                getattr(exc, "category", "") == "throttled"
            )
            is_risk = isinstance(
                exc,
                (
                    PikPakPublishRiskControlError,
                    ProviderPikPakRiskControlError,
                    CliPikPakRiskControlError,
                ),
            ) or (getattr(exc, "category", "") == "risk_control")

            if is_throttled:
                retry_after = getattr(exc, "retry_after", None)
                await self._handle_pikpak_tiered_cooldown(
                    task_id, retry_count, max_retries, "throttled", retry_after, exc
                )
            elif is_risk:
                is_dp = getattr(exc, "is_data_plane", False) or isinstance(exc, ProviderPikPakRiskControlError)
                err_type = "data_risk_control" if is_dp else "control_risk_control"
                retry_after = getattr(exc, "retry_after", None)
                await self._handle_pikpak_tiered_cooldown(task_id, retry_count, max_retries, err_type, retry_after, exc)
            elif isinstance(exc, RetryableTaskError):
                logger.warning("任务 [%s] 发生可重试瞬态异常: %s", task_id, exc)
                flood_wait_sec = _extract_retryable_flood_wait_seconds(exc)
                is_flood = getattr(exc, "is_flood_wait", True)
                if (
                    is_flood
                    and flood_wait_sec > 0
                    and not getattr(exc, "flood_client_marked", False)
                    and account_id is not None
                ):
                    client_manager.mark_flood_wait(account_id, flood_wait_sec)
                delay_floor = float(getattr(exc, "retry_after", 0) or 0) if not is_flood else float(flood_wait_sec)
                eff_retry = retry_count
                if task_type == "pikpak_publish":
                    cur_row = await TaskQueueRepository.get_by_id_async(task_id)
                    if cur_row is not None and cur_row.get("retry_count") is not None:
                        eff_retry = int(cur_row["retry_count"])
                count_retry = getattr(exc, "count_retry", True)
                if not count_retry:
                    entered_contention_retry = True
                await self._handle_task_retry(
                    task_id,
                    eff_retry,
                    max_retries,
                    str(exc),
                    min_delay=delay_floor,
                    count_retry=count_retry,
                )
            elif isinstance(exc, FatalTaskError):
                logger.error("任务 [%s] 发生致命错误: %s", task_id, exc)
                await TaskQueueRepository.update_status_async(task_id, "failed")
                await TaskQueueRepository.append_log_async(task_id, f"❌ 任务致命错误: {exc}")
                try:
                    from app.core.pipeline.placeholder import PlaceholderSlotCoordinator

                    await PlaceholderSlotCoordinator(max_retries=max_retries).finalize_terminal(task_id)
                except Exception as finalize_err:
                    logger.warning("任务 [%s] 占位槽终态化异常 (不影响调度循环): %s", task_id, finalize_err)
                try:
                    from app.core.connection_manager import connection_manager

                    await connection_manager.broadcast(
                        {
                            "type": "task_update",
                            "task_id": task_id,
                            "status": "failed",
                            "error_detail": str(exc),
                        }
                    )
                except Exception as bcast_err:
                    logger.debug("广播 fatal 错误状态异常 (非致命): %s", bcast_err)
            else:
                logger.warning("任务 [%s] 执行异常: %s", task_id, exc)
                current_status = (await TaskQueueRepository.get_by_id_async(task_id) or {}).get("status")
                if current_status not in ("completed", "cancelled", "paused", "failed"):
                    await self._handle_task_retry(task_id, retry_count, max_retries, str(exc))
        else:
            mark_task_executor_exit(task_id)
        finally:
            if not entered_contention_retry:
                self.clear_slot_contention_wait(task_id)
            if resume_controls is None:
                await self._cleanup_task_resources(task_id, account_id)
            else:
                await self._cleanup_task_resources(task_id, account_id, preserve_controls=True)
            try:
                if resume_controls is not None:
                    await self._requeue_resumed_pause_exit(task_id, resume_controls)
            finally:
                self.notify_new_task()

    async def _requeue_resumed_pause_exit(self, task_id: str, resume_controls: dict[str, bool]) -> None:
        try:
            requeued = await asyncio.to_thread(requeue_resumed_pause_exit, task_id, resume_controls)
        except DuplicateDownloadTaskError as conflict:
            await TaskQueueRepository.append_log_async(
                task_id, f"暂停收尾后恢复遇到同键任务 (#{conflict.task_id})，保持暂停"
            )
            await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "paused"})
            return
        if requeued:
            await TaskQueueRepository.append_log_async(task_id, "[调度器] 旧执行器已完成暂停收尾，恢复请求已重新入队")
            await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "queued"})

    async def _handle_pikpak_tiered_cooldown(
        self,
        task_id: str,
        retry_count: int,
        max_retries: int,
        error_type: str,
        retry_after: float | None,
        exc: Exception,
    ) -> None:
        """PikPak 2 小时分块 TTL 窗口内的三级阶梯无人值守自愈与风控安全挂起。
        - Tier 1 (首次 Level 2 频控): 浅度静默冷却 15m~17m，保留断点续传。
        - Tier 2 (二次 Level 2 频控 或 首次 Level 3a 数据面拦截): 深度冷却 40m~43m + 跨节点换链漂移 + 自动降档单并发稳态。
        - Tier 3 (三次限流 或 二次数据面拦截 或 控制面 Level 3b 风控): 主动 Safe-Pause (不标 failed，保全断点与已发分卷)。
        """
        rec = await TaskQueueRepository.get_by_id_async(task_id) or {}
        params = rec.get("params") or {}
        throttle_strikes = int(params.get("throttle_strike_count") or 0)
        data_risk_strikes = int(params.get("data_risk_strike_count") or 0)

        is_control_risk = error_type == "control_risk_control"
        is_data_risk = error_type == "data_risk_control"

        if is_data_risk:
            data_risk_strikes += 1
        elif not is_control_risk:
            throttle_strikes += 1

        total_strikes = throttle_strikes + data_risk_strikes
        if is_control_risk or total_strikes >= 3 or throttle_strikes >= 3 or data_risk_strikes >= 2:
            tier = 3
        elif is_data_risk or throttle_strikes == 2:
            tier = 2
        else:
            tier = 1

        if tier == 1:
            min_delay = max(float(retry_after or 0.0), 900.0)
            delay = min_delay + random.uniform(0.0, 120.0)
            next_retry_at = time.time() + delay
            next_retry_count = retry_count + 1

            await TaskQueueRepository.merge_params_async(
                task_id,
                {
                    "throttle_strike_count": throttle_strikes,
                    "data_risk_strike_count": data_risk_strikes,
                },
            )
            self.set_pikpak_cooldown(next_retry_at)

            error_msg = f"⏳ CDN 边缘频控保护 (Tier 1 浅度冷却 {int(delay)}s)"
            await TaskQueueRepository.mark_retry_async(task_id, next_retry_count, next_retry_at, error_msg)
            await TaskQueueRepository.append_log_async(
                task_id,
                f"⚠️ 检测到 CDN 节点频控/过载 (HTTP 429/503)，进入 Tier 1 静默冷却 "
                f"({int(delay // 60)} 分钟后自动断点续传) [{throttle_strikes}/3]",
            )
            try:
                from app.core.connection_manager import connection_manager

                await connection_manager.broadcast(
                    {
                        "type": "task_update",
                        "task_id": task_id,
                        "status": "queued",
                        "retry_count": next_retry_count,
                        "next_retry_at": next_retry_at,
                        "error_detail": error_msg,
                        "cooldown_tier": 1,
                        "speed_mb_s": 0.0,
                        "eta_s": 0.0,
                        "concurrency": 0,
                        "current_item": "",
                    }
                )
            except Exception as bcast_err:
                logger.debug("广播 Tier 1 冷却状态异常 (非致命): %s", bcast_err)
            logger.warning("任务 [%s] 进入 PikPak Tier 1 浅度冷却 (%.1fs)", task_id, delay)

        elif tier == 2:
            min_delay = max(float(retry_after or 0.0), 2400.0)
            delay = min_delay + random.uniform(0.0, 180.0)
            next_retry_at = time.time() + delay
            next_retry_count = retry_count + 1

            params_patch = {
                "pikpak_prefetch_concurrency": 1,
                "upload_concurrency": 1,
                "send_delay_seconds": max(float(params.get("send_delay_seconds") or 3.0), 20.0),
                "throttle_strike_count": throttle_strikes,
                "data_risk_strike_count": data_risk_strikes,
                "auto_downshifted": True,
            }
            await TaskQueueRepository.merge_params_async(task_id, params_patch)
            self.update_download_threads(task_id, 1)
            self.set_pikpak_cooldown(next_retry_at)

            error_msg = f"⏳ CDN 边缘频控保护 (Tier 2 深度冷却 {int(delay)}s + 换链漂移 + 稳态降档)"
            await TaskQueueRepository.mark_retry_async(task_id, next_retry_count, next_retry_at, error_msg)
            await TaskQueueRepository.append_log_async(
                task_id,
                f"⚠️ CDN 频控/数据面拦截加剧，进入 Tier 2 深度冷却 ({int(delay // 60)} 分钟) 并已自动降档为保守稳态 "
                f"(单预取通道/单上传并发/20s发送延迟)，冷却期满后强制换链跨节点漂移续传",
            )
            try:
                from app.core.connection_manager import connection_manager

                await connection_manager.broadcast(
                    {
                        "type": "task_update",
                        "task_id": task_id,
                        "status": "queued",
                        "retry_count": next_retry_count,
                        "next_retry_at": next_retry_at,
                        "error_detail": error_msg,
                        "cooldown_tier": 2,
                        "auto_downshifted": True,
                        "speed_mb_s": 0.0,
                        "eta_s": 0.0,
                        "concurrency": 0,
                        "current_item": "",
                    }
                )
            except Exception as bcast_err:
                logger.debug("广播 Tier 2 冷却状态异常 (非致命): %s", bcast_err)
            logger.warning("任务 [%s] 进入 PikPak Tier 2 深度冷却与自动降档 (%.1fs)", task_id, delay)

        else:
            # Tier 3: Safe-Pause
            self.set_pikpak_cooldown(time.time() + 900.0)
            mark_task_pause_exit(task_id)
            if is_control_risk:
                log_msg = (
                    "🟠 [风控安全挂起] 检测到 PikPak 账号控制面风控拦截 (需人工验证/验证码)，"
                    "系统已主动安全暂停以保护账号与 IP (断点与已发分卷均已保全)。请在手机端/客户端完成验证后点击【恢复】"
                )
            elif is_data_risk:
                log_msg = (
                    "🟠 [风控安全挂起] 跨节点漂移冷却后仍遭遇数据面 HTML 质询，"
                    "系统已主动安全暂停以保护账号与 IP (断点与已发分卷均已保全)。请核验后点击【稳态恢复】"
                )
            else:
                log_msg = (
                    "🟠 [风控安全挂起] 连续遭遇上游频控拦截已达 3 次阈值，"
                    "系统已主动安全暂停以保护账号与 IP (断点与已发分卷均已保全)。请核实后点击【低速稳态恢复】继续"
                )
            if not await TaskQueueRepository.pause_pikpak_for_risk_async(
                task_id,
                {
                    "safe_paused": True,
                    "throttle_strike_count": throttle_strikes,
                    "data_risk_strike_count": data_risk_strikes,
                },
                log_msg,
            ):
                return
            await TaskQueueRepository.append_log_async(task_id, log_msg)

            try:
                from app.core.connection_manager import connection_manager

                # 只通知刷新，避免异步广播将晚到的 paused 覆盖更新的状态。
                await connection_manager.broadcast({"type": "task_update", "task_id": task_id})
            except Exception as bcast_err:
                logger.debug("广播 Tier 3 安全挂起状态异常 (非致命): %s", bcast_err)
            logger.warning("任务 [%s] 进入 PikPak Tier 3 Safe-Pause 安全挂起: %s", task_id, log_msg)

    async def _mark_retry_or_fail_conflict(
        self,
        task_id: str,
        retry_count: int,
        next_retry_at: float,
        error_msg: str,
        *,
        from_statuses: tuple[str, ...] = ("running", "queued", "pending"),
    ) -> bool:
        try:
            return await TaskQueueRepository.mark_retry_async(
                task_id, retry_count, next_retry_at, error_msg, from_statuses=from_statuses
            )
        except DuplicateDownloadTaskError as conflict:
            detail = f"自动重试遇到同键下载任务 (#{conflict.task_id}): {error_msg}"
            failed = conflict.retry_source_status == "running" and await asyncio.to_thread(
                TaskQueueRepository.fail_running_task, task_id, detail
            )
            if not failed:
                status = await asyncio.to_thread(TaskQueueRepository.get_status, task_id)
                detail += f" (当前状态: {status or '任务已不存在'})"
            await TaskQueueRepository.append_log_async(task_id, detail)
            if failed:
                try:
                    await connection_manager.broadcast(
                        {"type": "task_update", "task_id": task_id, "status": "failed", "error_detail": detail}
                    )
                except Exception as e:
                    logger.debug("广播下载重试冲突状态异常 (非致命): %s", e)
            return False

    async def _handle_task_retry(
        self,
        task_id: str,
        retry_count: int,
        max_retries: int,
        error_msg: str,
        min_delay: float = 0.0,
        *,
        count_retry: bool = True,
    ):
        """自适应指数退避重试逻辑 (DB 写经 to_thread, 防 busy_timeout 竞争冻结事件循环)"""
        if not count_retry and not self.is_waiting_for_slot_contention(task_id):
            delay = self.SLOT_CONTENTION_DELAY_SECONDS
            next_retry_at = time.time() + delay
            self._slot_contention_waits.add(task_id)
            try:
                queued = await self._mark_retry_or_fail_conflict(
                    task_id, retry_count, next_retry_at, error_msg, from_statuses=("running",)
                )
            except BaseException:
                self.clear_slot_contention_wait(task_id)
                raise
            if not queued:
                self.clear_slot_contention_wait(task_id)
                return
            await TaskQueueRepository.append_log_async(
                task_id, f"⏳ 占位槽正被并发任务编辑 ({error_msg})，{int(delay)} 秒后复查一次"
            )
            try:
                from app.core.connection_manager import connection_manager

                await connection_manager.broadcast(
                    {
                        "type": "task_update",
                        "task_id": task_id,
                        "status": "queued",
                        "retry_count": retry_count,
                        "next_retry_at": next_retry_at,
                        "error_detail": error_msg,
                        "speed_mb_s": 0.0,
                        "eta_s": 0.0,
                        "concurrency": 0,
                        "current_item": "",
                    }
                )
            except Exception as bcast_err:
                logger.debug("广播争用等待任务状态异常 (非致命): %s", bcast_err)
            return

        self.clear_slot_contention_wait(task_id)
        next_count = retry_count + 1
        if count_retry and next_count <= max_retries:
            try:
                from app.core.metrics import task_retry_total

                task_retry_total.inc()
            except Exception as m_err:
                logger.debug("更新任务重试计数指标异常: %s", m_err)
            base_backoff = min(300.0, (2**retry_count) * 5.0) + random.uniform(0.5, 2.0)
            delay = max(base_backoff, min_delay)
            next_retry_at = time.time() + delay
            if not await self._mark_retry_or_fail_conflict(task_id, next_count, next_retry_at, error_msg):
                return
            await TaskQueueRepository.append_log_async(
                task_id, f"⚠️ 任务中断 ({error_msg})，将于 {int(delay)} 秒后自动重试 [{next_count}/{max_retries}]"
            )
            try:
                from app.core.connection_manager import connection_manager

                await connection_manager.broadcast(
                    {
                        "type": "task_update",
                        "task_id": task_id,
                        "status": "queued",
                        "retry_count": next_count,
                        "next_retry_at": next_retry_at,
                        "error_detail": error_msg,
                        "speed_mb_s": 0.0,
                        "eta_s": 0.0,
                        "concurrency": 0,
                        "current_item": "",
                    }
                )
            except Exception as bcast_err:
                logger.debug("广播 retry 任务状态异常 (非致命): %s", bcast_err)
            logger.info("任务 [%s] 将于 %.1fs 后重试 (%d/%d)", task_id, delay, next_count, max_retries)
        else:
            if count_retry:
                await TaskQueueRepository.update_status_async(task_id, "failed")
                log_message = f"❌ 任务重试次数已耗尽 ({max_retries} 次)，最终失败: {error_msg}"
            else:
                wait_seconds = int(self.SLOT_CONTENTION_DELAY_SECONDS)
                error_msg = f"{error_msg}。等待 {wait_seconds} 秒后复查仍被占用，请在占位卡释放后手动重试"
                if not await asyncio.to_thread(TaskQueueRepository.fail_running_task, task_id, error_msg):
                    return
                log_message = f"❌ {error_msg}"
            await TaskQueueRepository.append_log_async(task_id, log_message)
            # 任务终态 failed → 占位槽 ❌ 终态化 (core→core 合法, J9);
            # 异常全包围, 不影响调度循环
            try:
                from app.core.pipeline.placeholder import PlaceholderSlotCoordinator

                await PlaceholderSlotCoordinator(max_retries=max_retries).finalize_terminal(task_id)
            except Exception as finalize_err:
                logger.warning("任务 [%s] 占位槽终态化异常 (不影响调度循环): %s", task_id, finalize_err)
            try:
                from app.core.connection_manager import connection_manager

                await connection_manager.broadcast(
                    {
                        "type": "task_update",
                        "task_id": task_id,
                        "status": "failed",
                        "error_detail": error_msg,
                    }
                )
            except Exception as bcast_err:
                logger.debug("广播 failed 任务状态异常 (非致命): %s", bcast_err)

    def reassign_task_account(self, task_id: str, old_account_id: int, new_account_id: int):
        """故障转移时动态调整账号活跃任务配额记账"""
        if old_account_id in self._account_task_counts:
            self._account_task_counts[old_account_id] = max(0, self._account_task_counts[old_account_id] - 1)
        self._account_task_counts[new_account_id] = self._account_task_counts.get(new_account_id, 0) + 1
        self._task_assigned_accounts[task_id] = new_account_id
        logger.info("调度器已更新任务 [%s] 执行账号计数: %d -> %d", task_id, old_account_id, new_account_id)

    def borrow_auxiliary_account(self, task_id: str, aux_account_id: int):
        """梯队 2: 任务协同下载借用辅助 Userbot 账号配额记账 (幂等保证)"""
        if not aux_account_id:
            return
        if aux_account_id in self._task_auxiliary_accounts[task_id]:
            return
        self._task_auxiliary_accounts[task_id].add(aux_account_id)
        self._account_task_counts[aux_account_id] = self._account_task_counts.get(aux_account_id, 0) + 1
        logger.debug(
            "任务 [%s] 成功借用辅助 Userbot [%d] 配额 (当前计数: %d)",
            task_id,
            aux_account_id,
            self._account_task_counts[aux_account_id],
        )

    def release_auxiliary_account(self, task_id: str, aux_account_id: int):
        """释放特定辅助 Userbot 账号配额"""
        if task_id in self._task_auxiliary_accounts and aux_account_id in self._task_auxiliary_accounts[task_id]:
            self._task_auxiliary_accounts[task_id].remove(aux_account_id)
            if aux_account_id in self._account_task_counts:
                self._account_task_counts[aux_account_id] = max(0, self._account_task_counts[aux_account_id] - 1)
            logger.debug("任务 [%s] 已释放辅助 Userbot [%d] 配额", task_id, aux_account_id)

    def update_download_threads(self, task_id: str, new_threads: int) -> None:
        """动态更新运行中下载任务的全局水门线程记账与 Prometheus 监控指标"""
        clamped_new = max(1, min(int(new_threads), self.MAX_GLOBAL_DOWNLOAD_THREADS))
        if task_id not in self._task_thread_allocations and task_id not in self._running_tasks:
            return
        old_threads = self._task_thread_allocations.get(task_id, 0)
        delta = clamped_new - old_threads
        self._task_thread_allocations[task_id] = clamped_new
        self._global_download_threads = max(0, self._global_download_threads + delta)
        try:
            from app.core.metrics import download_threads_in_use

            download_threads_in_use.set(self._global_download_threads)
        except Exception as m_err:
            logger.debug("动态更新全局下载线程指标异常: %s", m_err)
        logger.info(
            "调度器已动态更新任务 [%s] 下载线程记账: %d -> %d (差值: %+d, 当前活跃: %d/%d)",
            task_id,
            old_threads,
            clamped_new,
            delta,
            self._global_download_threads,
            self.MAX_GLOBAL_DOWNLOAD_THREADS,
        )
        if delta < 0 and self._is_running:
            self.notify_new_task()

    def _cleanup_task_resources(self, task_id: str, account_id: int | None, *, preserve_controls: bool = False):
        """清理已完成/退出任务的资源计数 (双模支持: 同步立即执行, 同时支持 await 派发)"""
        from app.core.runtime import active_task_controls

        was_running = task_id in self._running_tasks
        self._running_tasks.pop(task_id, None)
        if not preserve_controls:
            active_task_controls.pop(task_id, None)
        try:
            from app.core.metrics import running_tasks

            running_tasks.set(len(self._running_tasks))
        except Exception as m_err:
            logger.debug("任务清理释放更新运行任务指标异常: %s", m_err)

        # 优先读取内存跟踪或落库可能已故障转移后的最新 account_id 释放配额
        # 若已无 recorded assignment 且任务已不在 running_tasks，则属幂等重复清理，不双重递减
        final_acc_id = self._task_assigned_accounts.pop(task_id, None)
        if final_acc_id is None and was_running:
            final_acc_id = account_id if account_id is not None else 0

        if final_acc_id is not None and final_acc_id in self._account_task_counts:
            self._account_task_counts[final_acc_id] = max(0, self._account_task_counts[final_acc_id] - 1)

        # 梯队 2: 释放该任务借用的所有辅助账号配额
        if task_id in self._task_auxiliary_accounts:
            aux_ids = self._task_auxiliary_accounts.pop(task_id, set())
            for aid in aux_ids:
                if aid in self._account_task_counts:
                    self._account_task_counts[aid] = max(0, self._account_task_counts[aid] - 1)
                logger.debug("任务 [%s] 清理释放辅助 Userbot [%d] 配额", task_id, aid)

        # 释放全局活跃下载线程配额
        if task_id in self._task_thread_allocations:
            threads = self._task_thread_allocations.pop(task_id)
            self._global_download_threads = max(0, self._global_download_threads - threads)
            try:
                from app.core.metrics import download_threads_in_use

                download_threads_in_use.set(self._global_download_threads)
            except Exception as m_err:
                logger.debug("释放下载线程更新指标异常: %s", m_err)
            logger.debug(
                "任务 [%s] 已释放 %d 个全局下载线程配额 (当前活跃: %d)", task_id, threads, self._global_download_threads
            )

        return _AWAITABLE_NONE

    def recover_orphaned_tasks(self):
        """开机状态安全调和 (Startup Safety Reconciliation):
        默认将先前未完成的任务统一转置为 paused (安全暂停保护)，保留断点，避免重启突发并发风控；
        若显式配置 auto_resume_tasks_on_startup = True，则转置为 queued 重新进入排队调度。
        """
        auto_resume = SystemSettingsRepository.get_bool(
            "auto_resume_tasks_on_startup", default=getattr(settings, "AUTO_RESUME_TASKS_ON_STARTUP", False)
        )

        rows = TaskQueueRepository.get_orphaned_tasks(statuses=("running", "pending", "queued"))

        def _resume_hint(t_type: str) -> str:
            if t_type and str(t_type).startswith("download"):
                return "已保留 .part/.meta 断点文件，将从断点续传"
            if t_type == "forward_channel":
                return "转发账本已记录进度，将自动跳过已转发消息"
            if t_type == "publish_folder":
                return "已发布文件已记录历史，将自动跳过继续发布"
            return "可通过【重试】按钮重新调度执行"

        target_status = "queued" if auto_resume else "paused"

        for row in rows:
            t_id = row["id"]
            t_type = row["task_type"]
            if auto_resume:
                msg = f"[调度器] 服务启动自动恢复：先前未完成的任务已重新进入排队调度 ({_resume_hint(t_type)})"
            else:
                msg = (
                    "[调度器] 服务重启保护：先前未完成的任务已安全转置为暂停状态 (断点与进度已保留)，"
                    "请在任务中心人工确认后点击【恢复】继续运行"
                )
            TaskQueueRepository.update_status(t_id, target_status)
            TaskQueueRepository.append_log(t_id, msg)
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    bcast_task = loop.create_task(
                        connection_manager.broadcast({"type": "task_update", "task_id": t_id, "status": target_status})
                    )
                    self._bg_tasks.add(bcast_task)
                    bcast_task.add_done_callback(self._bg_tasks.discard)
            except Exception as bcast_err:
                logger.debug("开机恢复广播状态更新异常 (非致命): %s", bcast_err)

        if rows:
            if auto_resume:
                logger.info("已自动恢复 %d 个历史中断/排队任务进入调度队列", len(rows))
            else:
                logger.info("已安全暂停保护 %d 个历史中断/排队任务 (服务重启保护)", len(rows))


task_scheduler = TaskScheduler()
