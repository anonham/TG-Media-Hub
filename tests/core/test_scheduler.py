"""Core Runtime: 任务调度器核心状态机、优雅停机治理、并发锁竞争与自愈恢复测试 (Scheduler & Runtime Governance)

覆盖范围:
- 原 test_scheduler: 任务调度优先级、指数退避重试、孤儿任务恢复与并发上限
- 原 test_scheduler_graceful_stop: 优雅停机活跃任务收割、超时强制取消与关闭顺序
- 原 test_auto_scheduling_closure: 自动调度账号回填、私有频道适配、故障转移与容量探测
- 原 test_runtime_lock_concurrency: 运行期控制信号锁、执行器并发抢占与多线程下载路径安全
- 原 test_single_instance_lock: 单实例文件锁 (Windows msvcrt / Unix fcntl) 获取、重入与异常释放
- 原 test_startup_recovery: 启动降级状态机、登录恢复与执行器保留
- 原 test_shutdown_timeout: 停机超时动态弹性计算
- 原 test_p2_features: 全局活跃下载分块水门限额 (<= 32 线程) 调度阻塞与释放"""

import asyncio
import concurrent.futures
import os
import sys
import time
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from _async_wait import wait_until
from _cm_seed import build_default_account
from httpx import ASGITransport, AsyncClient
from telethon.errors import ChannelPrivateError, ChatAdminRequiredError

from app.api.tasks import precheck_channel_access
from app.config import settings
from app.core.client_manager import (
    TelegramBotClient,
    client_manager,
    is_channel_permission_error,
    is_private_channel_identifier,
)
from app.core.runtime import (
    active_download_paths,
    active_task_controls,
    is_task_cancelled,
    is_task_paused,
    set_task_control,
    try_claim_executor_entry,
)
from app.core.scheduler import TaskPausedError, TaskScheduler, task_scheduler
from app.core.single_instance import SingleInstanceLock
from app.db.account_repository import SystemSettingsRepository
from app.db.database import get_db
from app.db.task_repository import TaskQueueRepository
from app.main import app, lifespan
from app.models.task import PrecheckAccessRequest
from app.services.forward_service import ForwardService
from run import get_graceful_shutdown_timeout


# ==============================================================================
# Part: test_scheduler.py
# ==============================================================================
@pytest.mark.asyncio
async def test_scheduler_priority_and_concurrency():
    scheduler = TaskScheduler(check_interval=0.05)
    SystemSettingsRepository.set("max_global_concurrency", 2)

    executed_order = []
    gates = {
        "t_high": asyncio.Event(),
        "t_med": asyncio.Event(),
        "t_low": asyncio.Event(),
    }

    async def mock_handler(task_id: str, params: dict, account_id: int):
        executed_order.append(task_id)
        await gates[task_id].wait()
        TaskQueueRepository.update_status(task_id, "completed")

    scheduler.register_executor("mock_task", mock_handler)

    # 1. 提交 3 个任务：不同优先级
    # 低优先级
    TaskQueueRepository.create("t_low", "mock_task", {}, priority=5, status="queued")
    # 高优先级
    TaskQueueRepository.create("t_high", "mock_task", {}, priority=20, status="queued")
    # 中优先级
    TaskQueueRepository.create("t_med", "mock_task", {}, priority=10, status="queued")

    await scheduler.start()

    # 单账号默认 max_concurrency=1，逐段门控放行可确定性观测严格优先级顺序
    await wait_until(lambda: executed_order == ["t_high"], message="Expected t_high to execute first")
    gates["t_high"].set()

    await wait_until(lambda: executed_order == ["t_high", "t_med"], message="Expected t_med to execute second")
    gates["t_med"].set()

    gates["t_low"].set()
    await wait_until(
        lambda: (r := TaskQueueRepository.get_by_id("t_low")) is not None and r["status"] == "completed",
        message="Low priority task did not complete after gate release",
    )
    await scheduler.stop()

    assert len(executed_order) >= 2
    assert executed_order[0] == "t_high"
    assert executed_order[1] == "t_med"

    t1_db = TaskQueueRepository.get_by_id("t_low")
    assert t1_db["status"] in ("completed", "running", "queued")


@pytest.mark.asyncio
async def test_scheduler_retry_backoff():
    scheduler = TaskScheduler(check_interval=0.05)
    attempts = 0

    async def failing_handler(task_id: str, params: dict, account_id: int):
        nonlocal attempts
        attempts += 1
        raise RuntimeError("Transient network error")

    scheduler.register_executor("failing_task", failing_handler)

    TaskQueueRepository.create("t_fail", "failing_task", {}, priority=10, max_retries=2, status="queued")

    await scheduler.start()
    await wait_until(
        lambda: (rec := TaskQueueRepository.get_by_id("t_fail")) is not None and rec["retry_count"] >= 1,
        message="Task retry_count did not increment before timeout",
    )
    await scheduler.stop()

    t_db = TaskQueueRepository.get_by_id("t_fail")
    assert t_db is not None
    # 发生重试，状态被置回 queued 且 retry_count 增加
    assert t_db["retry_count"] >= 1
    assert t_db["next_retry_at"] > 0


def test_scheduler_recover_orphaned_tasks_default_paused():
    # BUG-004: 默认安全暂停保护 (防突发流量/风控)
    SystemSettingsRepository.set("auto_resume_tasks_on_startup", "false")
    TaskQueueRepository.create("orphan_1", "download_link", {"links": ["https://t.me/c/1/2"]}, status="running")
    TaskQueueRepository.create(
        "orphan_2", "forward_channel", {"source_chat": "src", "target_chat": "dst"}, status="running"
    )

    scheduler = TaskScheduler()
    scheduler.recover_orphaned_tasks()

    o1 = TaskQueueRepository.get_by_id("orphan_1")
    o2 = TaskQueueRepository.get_by_id("orphan_2")

    assert o1["status"] == "paused"
    assert o2["status"] == "paused"
    o1_logs, _, _ = TaskQueueRepository.get_task_logs("orphan_1", 0, 100)
    assert any("[调度器] 服务重启保护" in msg["message"] for msg in o1_logs)

    TaskQueueRepository.delete("orphan_1")
    TaskQueueRepository.delete("orphan_2")


def test_scheduler_recover_orphaned_tasks_auto_resume():
    # BUG-004: 开启自动恢复配置时恢复为 queued
    SystemSettingsRepository.set("auto_resume_tasks_on_startup", "true")
    try:
        TaskQueueRepository.create(
            "orphan_auto_1", "download_link", {"links": ["https://t.me/c/1/2"]}, status="running"
        )
        TaskQueueRepository.create(
            "orphan_auto_2", "forward_channel", {"source_chat": "src", "target_chat": "dst"}, status="pending"
        )

        scheduler = TaskScheduler()
        scheduler.recover_orphaned_tasks()

        o1 = TaskQueueRepository.get_by_id("orphan_auto_1")
        o2 = TaskQueueRepository.get_by_id("orphan_auto_2")

        assert o1["status"] == "queued"
        assert o2["status"] == "queued"
        o1_logs, _, _ = TaskQueueRepository.get_task_logs("orphan_auto_1", 0, 100)
        assert any("[调度器] 服务启动自动恢复" in msg["message"] for msg in o1_logs)
    finally:
        SystemSettingsRepository.set("auto_resume_tasks_on_startup", "false")
        TaskQueueRepository.delete("orphan_auto_1")
        TaskQueueRepository.delete("orphan_auto_2")


@pytest.mark.asyncio
async def test_scheduler_pause_releases_slot_and_schedules_next():
    # DISCUSS-001: 任务暂停立即释放配额并拉起下一排队任务 (Aria2 工业调度模型)
    scheduler = TaskScheduler(check_interval=0.05)
    SystemSettingsRepository.set("max_global_concurrency", 1)

    t1_started = asyncio.Event()
    t1_exit = asyncio.Event()
    t2_completed = asyncio.Event()

    from app.core.runtime import active_task_controls

    async def pausable_handler(task_id: str, params: dict, account_id: int):
        if task_id == "t_pausable":
            t1_started.set()
            while not active_task_controls.get(task_id, {}).get("paused"):
                await asyncio.sleep(0.05)
            # 模拟任务检测到暂停信号优雅退出
            t1_exit.set()
            return
        if task_id == "t_next_in_queue":
            await asyncio.sleep(0.1)
            TaskQueueRepository.update_status(task_id, "completed")
            t2_completed.set()

    scheduler.register_executor("pausable_mock", pausable_handler)

    TaskQueueRepository.create("t_pausable", "pausable_mock", {}, priority=20, status="queued")
    TaskQueueRepository.create("t_next_in_queue", "pausable_mock", {}, priority=10, status="queued")

    await scheduler.start()

    # 1. 等待 t_pausable 被拉起运行
    await asyncio.wait_for(t1_started.wait(), timeout=2.0)
    assert "t_pausable" in scheduler._running_tasks

    # 2. 模拟前端发送暂停指令：置位 paused 并改库
    TaskQueueRepository.update_status("t_pausable", "paused")
    active_task_controls["t_pausable"]["paused"] = True

    # 3. 等待 t_pausable 退出
    await asyncio.wait_for(t1_exit.wait(), timeout=2.0)

    # 4. 验证 t_next_in_queue 自动被调度执行完成
    await asyncio.wait_for(t2_completed.wait(), timeout=2.0)

    await scheduler.stop()

    t1_db = TaskQueueRepository.get_by_id("t_pausable")
    t2_db = TaskQueueRepository.get_by_id("t_next_in_queue")
    assert t1_db["status"] == "paused"
    assert t2_db["status"] == "completed"

    TaskQueueRepository.delete("t_pausable")
    TaskQueueRepository.delete("t_next_in_queue")


def test_scheduler_prioritize_task_buffering():
    # DISCUSS-001: 恢复任务若遇配额打满，设置 priority=50 缓冲入队首
    TaskQueueRepository.create("prio_low", "mock", {}, priority=10, status="queued")
    TaskQueueRepository.create("prio_high", "mock", {}, priority=20, status="queued")
    TaskQueueRepository.create("prio_resumed", "mock", {}, priority=10, status="paused")

    try:
        # 调用 prioritize_task 提升优先级并加入队列
        TaskQueueRepository.prioritize_task("prio_resumed", priority=50)
        TaskQueueRepository.update_status("prio_resumed", "queued")

        resumed_task = TaskQueueRepository.get_by_id("prio_resumed")
        assert resumed_task["status"] == "queued"
        assert resumed_task["priority"] == 50

        # 查询候选任务队列，prio_resumed 应当排在最首位 (优先级最高)
        candidates = TaskQueueRepository.get_queued_candidates(limit=10)
        candidate_ids = [c["id"] for c in candidates]
        assert candidate_ids.index("prio_resumed") < candidate_ids.index("prio_high")
        assert candidate_ids.index("prio_high") < candidate_ids.index("prio_low")
    finally:
        TaskQueueRepository.delete("prio_low")
        TaskQueueRepository.delete("prio_high")
        TaskQueueRepository.delete("prio_resumed")


@pytest.mark.asyncio
async def test_scheduler_skips_dispatch_when_cancelled_in_window():
    """调度窗口竞态: 认领后控制项已带取消标志 -> 跳过派发 (不创建协程/不广播 running), 幂等补写终态"""
    from app.core.runtime import active_task_controls

    scheduler = TaskScheduler(check_interval=0.1)
    SystemSettingsRepository.set("max_global_concurrency", 2)

    called = []

    async def mock_handler(task_id: str, params: dict, account_id: int):
        called.append(task_id)

    scheduler.register_executor("mock_task", mock_handler)

    task_id = "t_window_cancel"
    TaskQueueRepository.create(task_id, "mock_task", {}, priority=10, status="queued")
    # 模拟「标志已置位、DB 终态写尚未落库」的窗口瞬间 (resume/retry 预建控制项 + 用户立即取消)
    active_task_controls[task_id] = {"cancelled": True, "paused": False}

    try:
        await scheduler._schedule_next_tasks()

        assert called == [], "带取消标志的任务不得派发协程"
        assert task_id not in scheduler._running_tasks
        assert task_id not in active_task_controls
        # 调度器幂等补写终态, 防止 DB 停留在 running
        assert TaskQueueRepository.get_status(task_id) == "cancelled"
    finally:
        active_task_controls.pop(task_id, None)
        TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_scheduler_db_reads_do_not_freeze_event_loop(monkeypatch):
    """热点 DB 读经 to_thread: 慢查询阻塞工作线程时, 事件循环上的并发协程仍按时调度 (验收 d 运行时验证)"""
    import threading

    scheduler = TaskScheduler(check_interval=0.05)
    SystemSettingsRepository.set("max_global_concurrency", 2)

    real_get = TaskQueueRepository.get_queued_candidates
    thread_entered = threading.Event()
    release_db = threading.Event()

    def slow_get(*args, **kwargs):
        thread_entered.set()
        release_db.wait(timeout=5.0)
        return real_get(*args, **kwargs)

    monkeypatch.setattr(TaskQueueRepository, "get_queued_candidates", slow_get)

    schedule_task = asyncio.create_task(scheduler._schedule_next_tasks())
    try:
        await wait_until(thread_entered.is_set, message="Worker thread did not enter slow DB query")
        # 工作线程阻塞期间，事件循环必须能自由切换并推进并发协程
        ticks = 0
        for _ in range(5):
            await asyncio.sleep(0)
            ticks += 1
        assert ticks == 5
        assert not schedule_task.done(), "slow_get 已放行或未阻塞工作线程"
    finally:
        release_db.set()
    await asyncio.wait_for(schedule_task, timeout=2.0)


@pytest.mark.asyncio
async def test_global_download_threads_watergate_queue():
    """任务 5: 验证全局下载线程总闸门行为：2 个 16 线程任务活跃时第 3 个 16 线程任务必须排队等待"""
    from unittest.mock import MagicMock, patch

    from app.core.client_manager import client_manager
    from app.db.database import get_db

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM task_queue WHERE id LIKE 'gdt_%'")
    conn.commit()
    conn.close()

    scheduler = TaskScheduler(check_interval=0.05)
    SystemSettingsRepository.set("max_global_concurrency", 10)

    executed = []
    events = {
        "gdt_1": asyncio.Event(),
        "gdt_2": asyncio.Event(),
        "gdt_3": asyncio.Event(),
    }

    async def mock_handler(task_id: str, params: dict, account_id: int):
        executed.append(task_id)
        ev = events.get(task_id)
        if ev:
            await ev.wait()
        TaskQueueRepository.update_status(task_id, "completed")

    scheduler.register_executor("download_link", mock_handler)

    TaskQueueRepository.create(
        "gdt_1", "download_link", {"threads": 16}, priority=30, status="queued", concurrency_total=16
    )
    TaskQueueRepository.create(
        "gdt_2", "download_link", {"threads": 16}, priority=20, status="queued", concurrency_total=16
    )
    TaskQueueRepository.create(
        "gdt_3", "download_link", {"threads": 16}, priority=10, status="queued", concurrency_total=16
    )

    mock_acc = MagicMock()
    mock_acc.account_id = 888
    mock_acc.account_name = "gdt_bot"
    mock_acc.max_concurrency = 10
    mock_acc.is_flood_locked.return_value = False

    with (
        patch.object(client_manager, "get_available_account", new_callable=AsyncMock, return_value=mock_acc),
        patch.object(client_manager, "get_account", return_value=mock_acc),
    ):
        await scheduler.start()
        await wait_until(
            lambda: "gdt_1" in executed and "gdt_2" in executed,
            message="Expected gdt_1 and gdt_2 to enter execution",
        )

        # 2 个 16 线程任务已运行，总闸门满 32 线程
        assert "gdt_3" not in executed
        assert scheduler._global_download_threads == 32

        # 释放 gdt_1
        events["gdt_1"].set()
        await wait_until(
            lambda: "gdt_3" in executed and scheduler._global_download_threads == 32,
            message="Expected gdt_3 to acquire released 16 download threads",
        )

        # gdt_1 退出后释放 16 线程，gdt_3 被成功调度启动
        assert "gdt_3" in executed
        assert scheduler._global_download_threads == 32

        # 全部结束
        for ev in events.values():
            ev.set()
        await wait_until(
            lambda: scheduler._global_download_threads == 0,
            message="Expected global download threads to return to 0",
        )
        await scheduler.stop()
        assert scheduler._global_download_threads == 0


@pytest.mark.asyncio
async def test_dynamic_download_concurrency_updates_watergate_and_zero_drift():
    """需求 F: 验证运行时动态调整下载任务并发数时，TaskScheduler 水门记账、Prometheus 指标与排队唤醒即时同步，且任务结束清理零漂移"""
    from unittest.mock import MagicMock, patch

    from app.api.tasks import update_task_concurrency
    from app.core.client_manager import client_manager
    from app.core.metrics import download_threads_in_use
    from app.core.scheduler import task_scheduler as global_scheduler
    from app.db.database import get_db
    from app.models.task import UpdateTaskConcurrencyRequest

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM task_queue WHERE id LIKE 'dyn_gdt_%'")
    conn.commit()
    conn.close()

    scheduler = TaskScheduler(check_interval=0.05)
    SystemSettingsRepository.set("max_global_concurrency", 10)

    executed = []
    events = {
        "dyn_gdt_1": asyncio.Event(),
        "dyn_gdt_2": asyncio.Event(),
        "dyn_gdt_3": asyncio.Event(),
    }

    async def mock_handler(task_id: str, params: dict, account_id: int):
        executed.append(task_id)
        ev = events.get(task_id)
        if ev:
            await ev.wait()
        TaskQueueRepository.update_status(task_id, "completed")

    scheduler.register_executor("download_link", mock_handler)

    # 创建 2 个 16 线程任务 (占满 32 水门) 与 1 个 8 线程排队任务
    TaskQueueRepository.create(
        "dyn_gdt_1",
        "download_link",
        {"links": ["https://t.me/c/1/1"], "threads": 16},
        priority=30,
        status="queued",
        concurrency_total=16,
    )
    TaskQueueRepository.create(
        "dyn_gdt_2",
        "download_link",
        {"links": ["https://t.me/c/1/2"], "threads": 16},
        priority=20,
        status="queued",
        concurrency_total=16,
    )
    TaskQueueRepository.create(
        "dyn_gdt_3",
        "download_link",
        {"links": ["https://t.me/c/1/3"], "threads": 8},
        priority=10,
        status="queued",
        concurrency_total=8,
    )

    mock_acc = MagicMock()
    mock_acc.account_id = 888
    mock_acc.account_name = "dyn_gdt_bot"
    mock_acc.max_concurrency = 10
    mock_acc.is_flood_locked.return_value = False

    with (
        patch.object(client_manager, "get_available_account", new_callable=AsyncMock, return_value=mock_acc),
        patch.object(client_manager, "get_account", return_value=mock_acc),
        patch("app.api.tasks.task_scheduler", scheduler),
    ):
        await scheduler.start()
        await wait_until(
            lambda: "dyn_gdt_1" in executed and "dyn_gdt_2" in executed,
            message="Expected dyn_gdt_1 and dyn_gdt_2 to enter execution",
        )

        assert "dyn_gdt_3" not in executed
        assert scheduler._global_download_threads == 32
        assert scheduler._task_thread_allocations["dyn_gdt_1"] == 16
        assert scheduler._task_thread_allocations["dyn_gdt_2"] == 16

        # 1. 运行时通过 API 将 dyn_gdt_1 从 16 线程缩容至 8 线程 -> 释放 8 线程水门并自动唤醒拉起 dyn_gdt_3 (8 线程)
        await update_task_concurrency("dyn_gdt_1", UpdateTaskConcurrencyRequest(concurrency=8))
        assert scheduler._task_thread_allocations["dyn_gdt_1"] == 8

        await wait_until(
            lambda: "dyn_gdt_3" in executed and scheduler._global_download_threads == 32,
            message="Expected dyn_gdt_3 to start after watergate reduction",
        )
        # 此时: dyn_gdt_1(8) + dyn_gdt_2(16) + dyn_gdt_3(8) = 32
        assert scheduler._global_download_threads == 32
        assert download_threads_in_use._value.get() == 32

        # 2. 动态扩容受全局 32 线程上限钳位，缩容后在剩余容量内可正常扩容
        scheduler.update_download_threads("dyn_gdt_1", 12)
        assert scheduler._global_download_threads == 32
        assert scheduler._task_thread_allocations["dyn_gdt_1"] == 8

        scheduler.update_download_threads("dyn_gdt_1", 4)
        assert scheduler._global_download_threads == 28
        assert scheduler._task_thread_allocations["dyn_gdt_1"] == 4

        scheduler.update_download_threads("dyn_gdt_1", 8)
        assert scheduler._global_download_threads == 32
        assert scheduler._task_thread_allocations["dyn_gdt_1"] == 8

        # 3. 全部任务结束退出，验证 _cleanup_task_resources 按最新分配扣减，最终水门计数零漂移归 0
        for ev in events.values():
            ev.set()
        await wait_until(
            lambda: scheduler._global_download_threads == 0 and scheduler._task_thread_allocations == {},
            message="Expected watergate thread allocations to drain to 0",
        )
        await scheduler.stop()
        assert scheduler._global_download_threads == 0
        assert scheduler._task_thread_allocations == {}
        assert download_threads_in_use._value.get() == 0
        _ = global_scheduler


# ==============================================================================
# Part: test_scheduler_graceful_stop.py
# ==============================================================================
@pytest.mark.asyncio
async def test_scheduler_graceful_stop_harvests_active_tasks():
    """任务 1: 验证 TaskScheduler.stop() 三段式收割存活任务：置暂停、收割协程、清理控制项、DB 状态对齐 paused"""
    scheduler = TaskScheduler(check_interval=0.05)
    scheduler.GRACEFUL_STOP_TIMEOUT = 2.0
    SystemSettingsRepository.set("max_global_concurrency", 5)

    task_started = asyncio.Event()

    async def mock_pausable_executor(task_id: str, params: dict, account_id: int):
        task_started.set()
        while not active_task_controls.get(task_id, {}).get("paused"):
            await asyncio.sleep(0.02)
        raise TaskPausedError("优雅关闭让位暂停")

    scheduler.register_executor("download_link", mock_pausable_executor)

    tid = "stop_test_t1"
    TaskQueueRepository.create(tid, "download_link", {"threads": 16}, status="queued", priority=10)

    mock_acc = MagicMock()
    mock_acc.account_id = 101
    mock_acc.account_name = "test_bot"
    mock_acc.max_concurrency = 5
    mock_acc.dc_id = 2
    mock_acc.is_flood_locked.return_value = False

    with (
        patch.object(client_manager, "get_available_account", new_callable=AsyncMock, return_value=mock_acc),
        patch.object(client_manager, "get_account", return_value=mock_acc),
    ):
        await scheduler.start()
        await asyncio.wait_for(task_started.wait(), timeout=2.0)
        assert tid in scheduler._running_tasks
        assert tid in active_task_controls
        assert active_task_controls[tid]["paused"] is False

        task_coro = scheduler._running_tasks[tid]

        # 执行优雅关闭
        start_time = time.time()
        await scheduler.stop()
        duration = time.time() - start_time

        # 断言: 耗时在超时预算内
        assert duration < 2.0

        # 断言: 全部任务协程终结、无孤儿任务 (asyncio.all_tasks 过滤)
        assert task_coro.done()
        orphans = [t for t in asyncio.all_tasks() if t is task_coro and not t.done()]
        assert len(orphans) == 0
        assert len(scheduler._running_tasks) == 0
        assert len(active_task_controls) == 0

        # 断言: DB 状态收敛为 paused
        rec = TaskQueueRepository.get_by_id(tid)
        assert rec is not None
        assert rec["status"] == "paused"


@pytest.mark.asyncio
async def test_scheduler_graceful_stop_timeout_cancellation_fallback():
    """任务 1: 验证对于拒绝响应 paused 标志的任务协程，stop() 在超时后执行 cancel 强制收割"""
    scheduler = TaskScheduler(check_interval=0.05)
    scheduler.GRACEFUL_STOP_TIMEOUT = 0.05
    SystemSettingsRepository.set("max_global_concurrency", 5)

    task_started = asyncio.Event()
    cancelled = {"hit": False}

    async def stubborn_executor(task_id: str, params: dict, account_id: int):
        task_started.set()
        # 恶意忽略 paused 标志，只等 cancel
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled["hit"] = True
            raise

    scheduler.register_executor("download_link", stubborn_executor)

    tid = "stop_stubborn_t1"
    TaskQueueRepository.create(tid, "download_link", {"threads": 16}, status="queued", priority=10)

    mock_acc = MagicMock()
    mock_acc.account_id = 102
    mock_acc.account_name = "test_bot2"
    mock_acc.max_concurrency = 5
    mock_acc.dc_id = 2
    mock_acc.is_flood_locked.return_value = False

    with (
        patch.object(client_manager, "get_available_account", new_callable=AsyncMock, return_value=mock_acc),
        patch.object(client_manager, "get_account", return_value=mock_acc),
    ):
        await scheduler.start()
        await asyncio.wait_for(task_started.wait(), timeout=2.0)
        assert tid in scheduler._running_tasks
        task_coro = scheduler._running_tasks[tid]

        # 执行停止，应当在 GRACEFUL_STOP_TIMEOUT 超时后触发 cancel 收割
        await scheduler.stop()

        assert cancelled["hit"] is True
        assert task_coro.done()
        orphans = [t for t in asyncio.all_tasks() if t is task_coro and not t.done()]
        assert len(orphans) == 0
        assert len(scheduler._running_tasks) == 0
        assert len(active_task_controls) == 0

        # DB 状态幂等补写为 paused
        rec = TaskQueueRepository.get_by_id(tid)
        assert rec["status"] == "paused"


@pytest.mark.asyncio
async def test_lifespan_disconnection_order():
    """任务 1: 断言 lifespan 关闭时 task_scheduler.stop 严格先于 client_manager.disconnect_all"""
    call_order = []

    async def fake_stop():
        call_order.append("scheduler_stop")

    async def fake_disconnect():
        call_order.append("client_disconnect")

    from app.main import task_scheduler

    try:
        with (
            patch.object(task_scheduler, "start", new_callable=AsyncMock),
            patch.object(task_scheduler, "stop", side_effect=fake_stop),
            patch.object(client_manager, "disconnect_all", side_effect=fake_disconnect),
            patch.object(client_manager, "initialize", new_callable=AsyncMock),
        ):
            async with lifespan(app):
                pass

        assert call_order == ["scheduler_stop", "client_disconnect"], f"关闭顺序错误: {call_order}"
    finally:
        task_scheduler.is_running = False


@pytest.mark.asyncio
async def test_telegram_status_startup_degraded(monkeypatch):
    """任务 1: 验证 /api/telegram/status 包含 startup_degraded 字段且状态正确"""
    from app.api.auth import issue_2fa_token
    from app.config import settings

    account = build_default_account()
    account.user_id = 1
    monkeypatch.setattr(client_manager, "_accounts", {account.account_id: account})
    headers = {"X-API-Key": settings.API_KEY or "testkey"}
    token = issue_2fa_token()
    if token:
        headers["X-2FA-Token"] = token

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 默认非降级
        client_manager.startup_degraded = False
        resp = await client.get("/api/telegram/status", headers=headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "startup_degraded" in data
        assert data["startup_degraded"] is False
        assert data["startup"]["phase"] == "degraded"  # Scheduler is stopped in this fixture.
        if settings.API_KEY:
            key_only = await client.get("/api/telegram/status", headers={"X-API-Key": settings.API_KEY})
            assert key_only.status_code == 200
            assert key_only.json()["startup"] is None

        # 降级模式置位
        client_manager.startup_degraded = True
        resp2 = await client.get("/api/telegram/status", headers=headers)
        assert resp2.status_code == 200
        assert resp2.json()["startup_degraded"] is True
        client_manager.startup_degraded = False


# ==============================================================================
# Part: test_auto_scheduling_closure.py
# ==============================================================================
@pytest.mark.asyncio
async def test_acquire_task_account_id_backfill():
    """验证 CAS 认领任务时能够将动态算出的 account_id 原子回填至 DB"""
    task_id = "test_cas_backfill_001"
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="download_link",
        params={"links": ["https://t.me/c/12345/678"]},
        account_id=None,
        status="queued",
    )

    try:
        # 初始应为 None
        t_init = TaskQueueRepository.get_by_id(task_id)
        assert t_init["account_id"] is None
        assert t_init["status"] == "queued"

        # 调度器认领并回填账号 102
        acquired = TaskQueueRepository.acquire_task(task_id, account_id=102)
        assert acquired is True

        t_after = TaskQueueRepository.get_by_id(task_id)
        assert t_after["status"] == "running"
        assert t_after["account_id"] == 102

        # 二次认领应被 CAS 拦截失败 (不重复认领)
        acquired_second = TaskQueueRepository.acquire_task(task_id, account_id=103)
        assert acquired_second is False
        assert TaskQueueRepository.get_by_id(task_id)["account_id"] == 102
    finally:
        TaskQueueRepository.delete(task_id)


def test_permission_error_and_private_channel_identifier():
    """验证权限异常辅助识别与私有频道判断"""
    assert is_channel_permission_error(ChannelPrivateError(request=None)) is True
    assert is_channel_permission_error(ChatAdminRequiredError(request=None)) is True
    assert is_channel_permission_error(ValueError("Cannot find any entity with peer id")) is True
    assert is_channel_permission_error(RuntimeError("Unknown network glitch")) is False

    assert is_private_channel_identifier("https://t.me/c/9000000032/123") is True
    assert is_private_channel_identifier("-1009000000032") is True
    assert is_private_channel_identifier(-1009000000032) is True
    assert is_private_channel_identifier("https://t.me/public_channel/123") is False
    assert is_private_channel_identifier("public_name") is False


@pytest.mark.asyncio
async def test_channel_aware_account_matcher():
    """验证目标频道感知寻优: 账号 A 无权限，账号 B 有权限时，优先匹配账号 B"""
    acc_a = MagicMock()
    acc_a.account_id = 201
    acc_a.account_name = "Account_A_NoAccess"
    acc_a.is_active = True
    acc_a.is_ready.return_value = True
    acc_a.is_flood_locked.return_value = False
    acc_a.active_task_count = 0
    acc_a.max_concurrency = 2
    acc_a.client = MagicMock()
    acc_a.client.is_connected.return_value = True

    acc_b = MagicMock()
    acc_b.account_id = 202
    acc_b.account_name = "Account_B_HasAccess"
    acc_b.is_active = True
    acc_b.is_ready.return_value = True
    acc_b.is_flood_locked.return_value = False
    acc_b.active_task_count = 0
    acc_b.max_concurrency = 2
    acc_b.client = MagicMock()
    acc_b.client.is_connected.return_value = True

    async def mock_resolve(client, chat):
        if client == acc_b.client:
            mock_ent = MagicMock()
            mock_ent.id = 9000000032
            return mock_ent
        raise ChannelPrivateError(request=None)

    with (
        patch.dict(client_manager._accounts, {201: acc_a, 202: acc_b}, clear=True),
        patch("app.core.account_selection.resolve_entity", side_effect=mock_resolve),
    ):
        selected = await client_manager.get_available_account(
            task_type="download_link", target_chat="https://t.me/c/9000000032/99"
        )

        assert selected is not None
        assert selected.account_id == 202
        assert selected.account_name == "Account_B_HasAccess"


@pytest.mark.asyncio
async def test_find_failover_account():
    """验证故障转移查找器排查已失败账号，选择有效接替账号"""
    acc_fail = MagicMock()
    acc_fail.account_id = 301
    acc_fail.account_name = "Failed_Account"
    acc_fail.is_active = True
    acc_fail.is_ready.return_value = True
    acc_fail.is_flood_locked.return_value = False
    acc_fail.active_task_count = 1
    acc_fail.max_concurrency = 2

    acc_alt = MagicMock()
    acc_alt.account_id = 302
    acc_alt.account_name = "Success_Account"
    acc_alt.is_active = True
    acc_alt.is_ready.return_value = True
    acc_alt.is_flood_locked.return_value = False
    acc_alt.active_task_count = 0
    acc_alt.max_concurrency = 2
    acc_alt.client = MagicMock()
    acc_alt.client.is_connected.return_value = True

    async def mock_resolve(client, chat):
        mock_ent = MagicMock()
        mock_ent.id = 123456
        return mock_ent

    with (
        patch.dict(client_manager._accounts, {301: acc_fail, 302: acc_alt}, clear=True),
        patch("app.core.client_manager.resolve_entity", side_effect=mock_resolve),
    ):
        failover = await client_manager.find_failover_account(exclude_account_ids={301}, target_chat="-100900008")
        assert failover is not None
        assert failover.account_id == 302
        assert failover.account_name == "Success_Account"


@pytest.mark.asyncio
async def test_precheck_access_auto_pool_probe():
    """验证在 account_id=None 时，/api/tasks/precheck-access 全账号池遍历探测"""
    acc_ready = MagicMock()
    acc_ready.account_id = 401
    acc_ready.account_name = "HK_Worker"
    acc_ready.is_active = True
    acc_ready.is_ready.return_value = True
    acc_ready.is_flood_locked.return_value = False
    acc_ready.client = MagicMock()
    acc_ready.client.is_connected.return_value = True

    mock_entity = MagicMock()
    mock_entity.id = 9000000032
    mock_entity.title = "VIP 私密频道"
    mock_entity.username = None
    mock_entity.restricted = False
    mock_entity.noforwards = False

    with (
        patch.dict(client_manager._accounts, {401: acc_ready}, clear=True),
        patch.object(ForwardService, "resolve_entity", AsyncMock(return_value=mock_entity)),
    ):
        req = PrecheckAccessRequest(chat_identifier="https://t.me/c/9000000032/10", account_id=None)
        res = await precheck_channel_access(req)

        assert res.accessible is True
        assert "HK_Worker" in res.title
        assert "1 个账号" in res.title

    # 模拟全账号池均无权限
    with (
        patch.dict(client_manager._accounts, {401: acc_ready}, clear=True),
        patch.object(ForwardService, "resolve_entity", AsyncMock(side_effect=ChannelPrivateError(request=None))),
    ):
        req_fail = PrecheckAccessRequest(chat_identifier="https://t.me/c/9000000032/10", account_id=None)
        res_fail = await precheck_channel_access(req_fail)
        assert res_fail.accessible is False
        assert "暂无任何账号加入该私有频道" in res_fail.reason


# ==============================================================================
# Part: test_runtime_lock_concurrency.py
# ==============================================================================
def test_active_task_controls_thread_safe_concurrent_mutations():
    """验证多真实 OS 线程高并发对 active_task_controls 增删改查无死锁或字典状态撕裂"""
    active_task_controls.clear()
    num_threads = 16
    operations_per_thread = 200

    def worker(tid: int):
        for i in range(operations_per_thread):
            task_id = f"task_{tid}_{i}"
            # setdefault
            active_task_controls.setdefault(task_id, {"cancelled": False, "paused": False})
            # update
            set_task_control(task_id, cancelled=(i % 2 == 0), paused=(i % 3 == 0))
            # read
            _ = is_task_cancelled(task_id)
            _ = is_task_paused(task_id)
            _ = task_id in active_task_controls
            # pop
            if i % 4 == 0:
                active_task_controls.pop(task_id, None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, t) for t in range(num_threads)]
        for f in concurrent.futures.as_completed(futures):
            f.result()

    active_task_controls.clear()


def test_try_claim_executor_entry_concurrency_race():
    """验证在调度窗口内并发抢占 try_claim_executor_entry 时的线程安全与终态一致性"""
    task_id = "test_claim_concurrency_task"
    TaskQueueRepository.create(task_id, "download_link", {"links": []}, status="queued")
    active_task_controls.clear()

    # 主线程先预置取消信号
    set_task_control(task_id, cancelled=True)

    results = []

    def runner():
        claimed = try_claim_executor_entry(task_id)
        results.append(claimed)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(runner) for _ in range(8)]
        for f in concurrent.futures.as_completed(futures):
            f.result()

    # 因为已被取消，所有线程执行 claim 均应安全返回 False，且终态落库为 cancelled
    assert all(r is False for r in results)
    status = TaskQueueRepository.get_status(task_id)
    assert status == "cancelled"

    TaskQueueRepository.delete(task_id)
    active_task_controls.clear()


def test_active_download_paths_thread_safe():
    """验证 active_download_paths 引用计数注册表并发添加与释放的线程安全性"""
    active_download_paths.clear()
    num_threads = 10

    def worker(tid: int):
        for i in range(100):
            p = f"/path/{tid}_{i}.part"
            active_download_paths.add(p)
            assert p in active_download_paths
            active_download_paths.release(p)

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, t) for t in range(num_threads)]
        for f in concurrent.futures.as_completed(futures):
            f.result()

    assert len(active_download_paths) == 0


def test_active_download_paths_refcount_shared_paths():
    """共享路径引用计数: 首个下载释放后守卫仍在, 全部释放才摘除"""
    active_download_paths.clear()
    p = "/path/shared.part"

    active_download_paths.add(p)  # 下载 A 登记
    active_download_paths.add(p)  # 下载 B 登记同一路径
    assert p in active_download_paths
    assert active_download_paths.ref_count(p) == 2

    active_download_paths.release(p)  # 下载 A 结束
    assert p in active_download_paths, "下载 B 仍在写入, 守卫不得提前摘除"

    active_download_paths.release(p)  # 下载 B 结束
    assert p not in active_download_paths
    assert active_download_paths.ref_count(p) == 0

    # 多余 release 防御性忽略, 不回滚他人登记
    active_download_paths.add(p)
    active_download_paths.add(p)
    active_download_paths.release(p)
    active_download_paths.release(p)
    active_download_paths.release(p)
    assert p not in active_download_paths
    active_download_paths.clear()


# ==============================================================================
# Part: test_single_instance_lock.py
# ==============================================================================
def test_single_instance_lock_acquire_and_release(tmp_path):
    lock_file = str(tmp_path / "test.lock")
    lock = SingleInstanceLock(lock_file)
    assert not lock.is_acquired

    lock.acquire(port=8500)
    assert lock.is_acquired
    assert os.path.exists(lock_file)

    lock.release()
    assert not lock.is_acquired
    # #23: 正常释放保留锁文件以维持 inode 身份一致性，并允许后续实例重新获取
    assert os.path.exists(lock_file)
    lock_next = SingleInstanceLock(lock_file)
    lock_next.acquire(port=8501)
    try:
        assert lock_next.is_acquired
    finally:
        lock_next.release()


def test_single_instance_lock_context_manager(tmp_path):
    lock_file = str(tmp_path / "ctx_test.lock")
    with SingleInstanceLock(lock_file) as lock:
        assert lock.is_acquired
        assert os.path.exists(lock_file)

    assert not lock.is_acquired
    assert os.path.exists(lock_file)
    with SingleInstanceLock(lock_file) as lock_next:
        assert lock_next.is_acquired


def test_single_instance_lock_repeat_acquire_same_instance(tmp_path):
    lock_file = str(tmp_path / "repeat.lock")
    lock = SingleInstanceLock(lock_file)
    lock.acquire(port=8500)
    try:
        with pytest.raises(RuntimeError, match="本实例已持有该单实例锁"):
            lock.acquire(port=8500)
    finally:
        lock.release()


def test_single_instance_lock_double_acquire_rejection(tmp_path):
    lock_file = str(tmp_path / "double.lock")
    lock1 = SingleInstanceLock(lock_file)
    lock2 = SingleInstanceLock(lock_file)

    lock1.acquire(port=8500)
    try:
        with pytest.raises(RuntimeError) as exc_info:
            lock2.acquire(port=8501)
        err_msg = str(exc_info.value)
        assert f"PID: {os.getpid()}" in err_msg
        assert "端口: 8500" in err_msg
        assert "已有实例正在运行" in err_msg
    finally:
        lock1.release()

    # lock1 释放后，lock2 应能成功获取锁
    lock2.acquire(port=8501)
    try:
        assert lock2.is_acquired
    finally:
        lock2.release()


def test_single_instance_lock_update_port(tmp_path):
    lock_file = str(tmp_path / "update_port.lock")
    lock1 = SingleInstanceLock(lock_file)
    lock2 = SingleInstanceLock(lock_file)

    lock1.acquire(port=8500)
    try:
        # 更新端口为更短的数字，验证 null-pad 消除残留字节
        lock1.update_port(80)
        with pytest.raises(RuntimeError) as exc_info:
            lock2.acquire(port=8501)
        err_msg = str(exc_info.value)
        assert "端口: 80" in err_msg
    finally:
        lock1.release()


def test_single_instance_lock_parse_metadata_robustness():
    lock = SingleInstanceLock("dummy.lock")
    # 空内容
    assert lock._parse_metadata(b"") == ("未知", "未知", None)
    # 破坏数据
    assert lock._parse_metadata(b"broken\x00data") == ("未知", "未知", None)
    # 仅 null 填充
    assert lock._parse_metadata(b"\x00" * 100) == ("未知", "未知", None)
    # 正常 JSON 附带 null 填充
    raw = b'{"pid": 9999, "start_time": "2026-09-17T00:00:00", "port": 8888}\x00\x00\x00padding'
    assert lock._parse_metadata(raw) == (9999, "2026-09-17T00:00:00", 8888)


def test_single_instance_lock_posix_mock(tmp_path, monkeypatch):
    """验证非 win32 (POSIX) 平台下的加锁/解锁/二次拦截逻辑"""
    monkeypatch.setattr(sys, "platform", "linux")

    # 模拟 fcntl
    flock_state = {}

    class MockFcntl:
        LOCK_EX = 2
        LOCK_NB = 4
        LOCK_UN = 8

        @staticmethod
        def flock(fd, operation):
            if operation & MockFcntl.LOCK_EX:
                if flock_state.get(fd) is True:
                    raise BlockingIOError("Resource temporarily unavailable")
                # 检查其他已打开同路径的 fd 是否已有锁
                if any(v is True for k, v in flock_state.items() if k != fd):
                    raise BlockingIOError("Resource temporarily unavailable")
                flock_state[fd] = True
            elif operation & MockFcntl.LOCK_UN:
                flock_state[fd] = False

    monkeypatch.setitem(sys.modules, "fcntl", MockFcntl)

    lock_file = str(tmp_path / "posix.lock")
    lock1 = SingleInstanceLock(lock_file)
    lock2 = SingleInstanceLock(lock_file)

    lock1.acquire(port=9999)
    assert lock1.is_acquired

    with pytest.raises(RuntimeError, match="已有实例正在运行"):
        lock2.acquire(port=9999)

    lock1.release()
    assert not lock1.is_acquired


# ==============================================================================
# Part: test_startup_recovery.py
# ==============================================================================
@pytest.mark.asyncio
async def test_startup_degraded_retains_registered_executors():
    """验证启动时即使 client_manager.initialize 抛错进入 degraded 状态，
    执行器注册依然无条件完成，不丢失任何默认执行器挂载。"""
    with (
        patch.object(client_manager, "initialize", side_effect=RuntimeError("网络无法连通 Telegram DC")),
        patch.object(client_manager, "disconnect_all", new_callable=AsyncMock),
    ):
        async with lifespan(app):
            # 断言系统进入降级标记
            assert client_manager.startup_degraded is True
            assert task_scheduler.startup_degraded is True
            # 断言调度器未启动
            assert task_scheduler.is_running is False

            # 断言核心执行器依然注册完备
            expected_executors = {"download_link", "download_range", "publish_folder", "forward_channel"}
            for exc_key in expected_executors:
                assert exc_key in task_scheduler._executors, f"执行器丢失: {exc_key}"


@pytest.mark.asyncio
async def test_login_success_recovers_startup_degraded():
    """验证系统处于 degraded 假死状态时，用户完成 native 登录后：
    1. 自动启动后台调度器 (task_scheduler.is_running = True)
    2. client_manager.startup_degraded 与 task_scheduler.startup_degraded 均恢复为 False
    """
    client_manager.startup_degraded = True
    task_scheduler.startup_degraded = True
    await task_scheduler.stop()
    assert task_scheduler.is_running is False

    mock_acc = MagicMock()
    mock_acc.account_id = 101
    mock_acc.native_verify_code = AsyncMock(return_value=True)

    with (
        patch.object(client_manager, "get_account", return_value=mock_acc),
        patch.object(client_manager, "get_default_account_strict", return_value=mock_acc),
        patch.object(client_manager, "_connect_remaining_pool", new_callable=AsyncMock),
    ):
        success = await client_manager.native_verify_code("12025550106", "12345", "hash123")
        assert success is True

        # 断言自愈成功
        assert task_scheduler.is_running is True
        assert client_manager.startup_degraded is False
        assert task_scheduler.startup_degraded is False

    await task_scheduler.stop()


@pytest.mark.asyncio
async def test_account_client_native_verify_code_recovers_startup_degraded():
    """验证 TelegramAccountClient.native_verify_code 在认证成功时直接唤醒调度器并复位 degraded 标记"""
    from app.core.client_manager import TelegramAccountClient

    client_manager.startup_degraded = True
    task_scheduler.startup_degraded = True
    await task_scheduler.stop()
    assert task_scheduler.is_running is False

    acc = TelegramAccountClient(
        account_id=999,
        account_name="test_acc",
        phone="12025550106",
        session_file="test.session",
        api_id=1,
        api_hash="dummy",
        is_default=True,
    )
    mock_tg = AsyncMock()
    mock_tg.is_connected = MagicMock(return_value=True)
    mock_tg.sign_in = AsyncMock()
    mock_tg.is_user_authorized = AsyncMock(return_value=True)
    mock_tg.get_me = AsyncMock(
        return_value=MagicMock(id=123, phone="12025550106", username="test_u", first_name="test_f", premium=False)
    )
    acc.client = mock_tg

    with (
        patch("app.db.account_repository.AccountRepository.update_status"),
        patch("app.db.account_repository.AccountRepository.update_premium"),
        patch.object(client_manager, "get_default_account_strict", return_value=acc),
        patch.object(client_manager, "_connect_remaining_pool", new_callable=AsyncMock),
    ):
        success = await acc.native_verify_code("12025550106", "12345", "hash123")
        assert success is True
        assert task_scheduler.is_running is True
        assert client_manager.startup_degraded is False
        assert task_scheduler.startup_degraded is False

    await task_scheduler.stop()


@pytest.mark.parametrize("password", ["  secret password  ", "   ", "", None])
@pytest.mark.parametrize("retry", [False, True])
async def test_account_client_native_verify_code_preserves_password(password, retry):
    from telethon.errors import SessionPasswordNeededError

    from app.core.client_manager import TelegramAccountClient

    acc = TelegramAccountClient(
        account_id=999,
        account_name="test",
        phone="12025550106",
        session_file="test.session",
        api_id=1,
        api_hash="dummy",
    )
    mock_tg = AsyncMock()
    mock_tg.is_connected = MagicMock(return_value=True)
    mock_tg.is_user_authorized.return_value = False
    acc.client = mock_tg
    if password and password.strip():
        if retry:
            mock_tg.sign_in.side_effect = [RuntimeError("no pending password"), SessionPasswordNeededError(None), None]
        await acc.native_verify_code("12025550106", " 12-3 45 ", "hash123", password=password)
        expected = [call(password=password)]
        if retry:
            expected += [call(phone="12025550106", code="12345", phone_code_hash="hash123"), call(password=password)]
        assert mock_tg.sign_in.await_args_list == expected
    else:
        await acc.native_verify_code("12025550106", " 12-3 45 ", "hash123", password=password)
        mock_tg.sign_in.assert_awaited_once_with(phone="12025550106", code="12345", phone_code_hash="hash123")


@pytest.mark.asyncio
async def test_refresh_account_status_returns_before_background_recovery():
    """手动 Premium 探测立即返回，同时继续恢复调度器。"""
    from app.api.accounts import refresh_account_status

    client_manager.startup_degraded = True
    task_scheduler.startup_degraded = True
    await task_scheduler.stop()
    assert task_scheduler.is_running is False

    mock_acc = MagicMock()
    mock_acc.account_id = 888
    mock_acc.client = MagicMock()
    mock_acc.client.is_connected.return_value = True
    mock_acc.probe_premium = AsyncMock(return_value=False)
    mock_acc.is_ready.return_value = True
    mock_acc.auth_state = "authenticated"
    mock_acc.is_premium = False
    mock_acc.dc_id = 2
    mock_acc.user_id = 888
    mock_acc.username = "test_user"
    mock_acc.first_name = "Test"
    release_pool = asyncio.Event()

    async def wait_for_pool(**_kwargs):
        await release_pool.wait()

    with (
        patch.object(client_manager, "get_account", return_value=mock_acc),
        patch.object(client_manager, "get_default_account_strict", return_value=mock_acc),
        patch.object(client_manager, "_connect_remaining_pool", side_effect=wait_for_pool),
    ):
        try:
            res = await asyncio.wait_for(refresh_account_status(888), 2)
            assert res["status"] == "ok"
            mock_acc.probe_premium.assert_awaited_once_with(force=True)
            assert client_manager.get_startup_status()["phase"] == "initializing"
            assert task_scheduler.is_running is False
        finally:
            release_pool.set()
            if client_manager._pool_init_task is not None:
                await asyncio.wait_for(client_manager._pool_init_task, 2)
        assert task_scheduler.is_running is True
        assert client_manager.startup_degraded is False
        assert task_scheduler.startup_degraded is False

    await task_scheduler.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("scheduler_fails", [False, True])
async def test_startup_completion_reports_specific_unready_components(monkeypatch, scheduler_fails):
    from app.core.connection_manager import connection_manager

    client_manager.startup_degraded = True
    task_scheduler.startup_degraded = True
    task_scheduler.is_running = not scheduler_fails
    ready = TelegramBotClient(1, "1:test", "ready_bot", 1, "hash")
    ready.status = "ready"
    ready.client = MagicMock()
    client_manager._bots[1] = ready
    client_manager._bots[2] = TelegramBotClient(2, "2:test", "pending_bot", 1, "hash", bot_name="Auxiliary")
    client_manager._bots[3] = TelegramBotClient(3, "3:test", "disabled_bot", 1, "hash", is_active=False)
    monkeypatch.setattr(client_manager, "get_default_account_strict", MagicMock())
    monkeypatch.setattr(client_manager, "_connect_remaining_pool", AsyncMock())
    monkeypatch.setattr(task_scheduler, "start", AsyncMock(side_effect=RuntimeError("test startup failure")))
    broadcast = AsyncMock()
    monkeypatch.setattr(connection_manager, "broadcast", broadcast)

    await client_manager.recover_startup_degraded()

    assert client_manager.get_startup_status() == {
        "phase": "degraded",
        "scheduler_ready": not scheduler_fails,
        "ready_bots": 1,
        "total_bots": 2,
        "unready_bots": ["Auxiliary"],
    }
    broadcast.assert_awaited_once_with({"type": "system_event", "event": "telegram_startup_changed"})


# ==============================================================================
# Part: test_shutdown_timeout.py
# ==============================================================================
def test_graceful_shutdown_timeout_default():
    """验证默认配置下 Uvicorn 优雅退出超时预算为 18 秒 (GRACEFUL_STOP_TIMEOUT 10.0 + 8s 缓冲)"""
    timeout = get_graceful_shutdown_timeout()
    assert timeout >= 18
    assert timeout >= int(settings.GRACEFUL_STOP_TIMEOUT) + 8


def test_graceful_shutdown_timeout_minimum_floor():
    """验证即使 GRACEFUL_STOP_TIMEOUT 设置过小，亦有 15 秒保底预算"""
    with patch.object(settings, "GRACEFUL_STOP_TIMEOUT", 1.0):
        timeout = get_graceful_shutdown_timeout()
        assert timeout == 15


def test_graceful_shutdown_timeout_dynamic_scaling():
    """验证 Uvicorn 关闭预算随调度器常量动态扩大"""
    with patch.object(settings, "GRACEFUL_STOP_TIMEOUT", 25.0):
        timeout = get_graceful_shutdown_timeout()
        assert timeout == 33
        assert timeout > 25 + 5  # 大于调度器收割 (25) + cancel 兜底 (5)


# ==============================================================================
# Part: .tmp_p2_dc.py
# ==============================================================================
@pytest.mark.asyncio
async def test_dc_thread_watergate_cap():
    """[REQ-002] 验证全局活跃下载分块水门限额 (<= 32 线程) 调度阻塞与释放"""
    # 清理遗留排队任务保证测试环境独立
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM task_queue WHERE id LIKE 'p2_dc_%'")
    conn.commit()
    conn.close()

    scheduler = TaskScheduler(check_interval=0.05)
    SystemSettingsRepository.set("max_global_concurrency", 10)

    executed_tasks = []
    task_release_events = {}

    async def mock_download_handler(task_id: str, params: dict, account_id: int):
        executed_tasks.append(task_id)
        event = task_release_events.get(task_id)
        if event:
            await event.wait()
        TaskQueueRepository.update_status(task_id, "completed")

    scheduler.register_executor("download_link", mock_download_handler)

    for tid in ["p2_dc_t1", "p2_dc_t2", "p2_dc_t3"]:
        task_release_events[tid] = asyncio.Event()

    TaskQueueRepository.create(
        "p2_dc_t1", "download_link", {"threads": 16}, priority=15, status="queued", concurrency_total=16
    )
    TaskQueueRepository.create(
        "p2_dc_t2", "download_link", {"threads": 16}, priority=14, status="queued", concurrency_total=16
    )
    TaskQueueRepository.create(
        "p2_dc_t3", "download_link", {"threads": 16}, priority=13, status="queued", concurrency_total=16
    )

    mock_acc = MagicMock()
    mock_acc.account_id = 999
    mock_acc.account_name = "dc_test_bot"
    mock_acc.max_concurrency = 10
    mock_acc.dc_id = 2
    mock_acc.is_flood_locked.return_value = False

    with (
        patch.object(client_manager, "get_available_account", new_callable=AsyncMock, return_value=mock_acc),
        patch.object(client_manager, "get_account", return_value=mock_acc),
    ):
        await scheduler.start()
        await wait_until(
            lambda: "p2_dc_t1" in executed_tasks and "p2_dc_t2" in executed_tasks,
            message="Expected p2_dc_t1 and p2_dc_t2 to start",
        )

        # 验证 t1, t2 已被调度运行，而 t3 仍在排队 (全局已满 32 线程)
        assert "p2_dc_t3" not in executed_tasks

        # 检查全局活跃下载线程记录
        assert scheduler._global_download_threads == 32

        # 释放 t1 (16 线程)
        task_release_events["p2_dc_t1"].set()
        await wait_until(
            lambda: "p2_dc_t3" in executed_tasks and scheduler._global_download_threads == 32,
            message="Expected p2_dc_t3 to start after p2_dc_t1 released 16 threads",
        )

        # 全部解除阻塞
        for ev in task_release_events.values():
            ev.set()

        await wait_until(
            lambda: scheduler._global_download_threads == 0,
            message="Expected watergate threads to return to 0 after completion",
        )
        await scheduler.stop()


def test_try_claim_executor_entry_blocks_cancelled_db_status():
    """验证 try_claim_executor_entry 在 DB 为 cancelled 时正确拦截并清理"""
    with patch("app.db.task_repository.TaskQueueRepository.get_status", return_value="cancelled"):
        assert try_claim_executor_entry("t_claimed") is False


def test_scheduler_cleanup_task_resources_is_idempotent():
    """验证 _cleanup_task_resources 多次调用不发生账号配额双重递减"""
    sched = TaskScheduler()
    acc_id = 1
    sched._account_task_counts[acc_id] = 1
    sched._running_tasks["t_dup"] = MagicMock()
    sched._task_assigned_accounts["t_dup"] = acc_id

    sched._cleanup_task_resources("t_dup", acc_id)
    assert sched.get_account_task_count(acc_id) == 0

    sched._account_task_counts[acc_id] = 2
    sched._cleanup_task_resources("t_dup", acc_id)
    assert sched.get_account_task_count(acc_id) == 2


@pytest.mark.asyncio
async def test_scheduler_stop_preserves_controls_for_unfinished_tasks():
    """验证调度器 stop() 时若包装协程未能退出，仍保留该任务的 paused 控制信号"""
    sched = TaskScheduler()
    sched._is_running = True
    sched.GRACEFUL_STOP_TIMEOUT = 0.01

    async def stubborn_worker():
        while True:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                await asyncio.sleep(10)

    t_stubborn = asyncio.create_task(stubborn_worker())
    t_done = asyncio.create_task(asyncio.sleep(0))
    await t_done

    sched._running_tasks = {"t_stubborn": t_stubborn, "t_done": t_done}
    active_task_controls["t_stubborn"] = {"cancelled": False, "paused": False}
    active_task_controls["t_done"] = {"cancelled": False, "paused": False}

    try:
        with patch("app.core.scheduler.TaskQueueRepository") as m_repo:
            m_repo.get_by_id.return_value = {"status": "running"}
            await sched.stop()

        assert "t_done" not in active_task_controls
        assert active_task_controls.get("t_stubborn", {}).get("paused") is True
    finally:
        active_task_controls.clear()
        if not t_stubborn.done():
            t_stubborn.cancel()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("task_id", "threads", "expected"),
    [("t_huge", 999, TaskScheduler.MAX_GLOBAL_DOWNLOAD_THREADS), ("t_bad", "auto", 8)],
)
async def test_scheduler_clamps_oversized_and_non_numeric_threads(task_id, threads, expected):
    """验证 _schedule_next_tasks 对非法 threads 字符串回退默认值且对超大值钳位到上限"""
    sched = TaskScheduler()
    candidates = [
        {"id": task_id, "task_type": "download_link", "params": {"threads": threads}, "account_id": 1},
    ]
    mock_acc = MagicMock(account_id=1, account_name="a1", max_concurrency=5)
    mock_acc.is_flood_locked.return_value = False

    with (
        patch.object(sched, "get_max_global_concurrency", return_value=5),
        patch("app.core.scheduler.TaskQueueRepository") as m_repo,
        patch("app.core.scheduler.acquire_queued_task", return_value=True),
        patch("app.core.scheduler.client_manager.get_account", return_value=mock_acc),
        patch("app.core.scheduler.asyncio.create_task", side_effect=lambda c: (c.close(), MagicMock())[1]),
    ):
        m_repo.get_queued_count.return_value = 2
        m_repo.get_queued_candidates.side_effect = [candidates, []]
        await sched._schedule_next_tasks()

    assert task_id in sched._running_tasks
    assert sched._task_thread_allocations[task_id] == expected


def test_single_instance_lock_closes_fd_on_metadata_write_error(tmp_path):
    """验证 SingleInstanceLock.acquire() 在写元数据抛错时会释放锁并关闭文件描述符"""
    lock_file = str(tmp_path / "fail_meta.lock")
    lock = SingleInstanceLock(lock_file)
    with (
        patch("app.core.single_instance.os.write", side_effect=OSError("disk full")),
        pytest.raises(OSError, match="disk full"),
    ):
        lock.acquire(port=8500)

    assert lock._fd is None
    assert lock.is_acquired is False
    lock2 = SingleInstanceLock(lock_file)
    lock2.acquire(port=8500)
    lock2.release()


@pytest.mark.asyncio
@pytest.mark.parametrize("count_retry", [True, False])
async def test_download_auto_retry_conflict_leaves_no_running_task(monkeypatch, count_retry):
    from app.core.scheduler import RetryableTaskError

    sched = TaskScheduler()
    params = {"download_target_key": "same-target"}
    TaskQueueRepository.create("retrying", "download_link", params, status="running")
    TaskQueueRepository.create("other-download", "download_link", params, status="paused")
    broadcast = AsyncMock()
    monkeypatch.setattr("app.core.scheduler.connection_manager.broadcast", broadcast)

    async def executor(*_args):
        raise RetryableTaskError("retry reason", count_retry=count_retry, is_flood_wait=False)

    sched.register_executor("download_link", executor)
    await sched._run_task_wrapper("retrying", "download_link", params, account_id=None, retry_count=0, max_retries=3)

    row = TaskQueueRepository.get_by_id("retrying")
    assert row["status"] == "failed"
    assert "#other-download" in row["error_detail"]
    assert row["next_retry_at"] == 0
    assert TaskQueueRepository.get_by_id("other-download")["status"] == "paused"
    assert not sched.is_waiting_for_slot_contention("retrying")
    broadcast.assert_awaited_once_with(
        {"type": "task_update", "task_id": "retrying", "status": "failed", "error_detail": row["error_detail"]}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["queued", "pending"])
async def test_download_auto_retry_conflict_logs_without_overwriting_nonrunning_task(monkeypatch, status):
    sched = TaskScheduler()
    params = {"download_target_key": "same-target"}
    TaskQueueRepository.create("retrying", "download_link", params, status=status)
    TaskQueueRepository.create("other-download", "download_link", params, status="running")
    log = AsyncMock()
    broadcast = AsyncMock()
    monkeypatch.setattr(TaskQueueRepository, "append_log_async", log)
    monkeypatch.setattr("app.core.scheduler.connection_manager.broadcast", broadcast)

    await sched._handle_task_retry("retrying", retry_count=0, max_retries=3, error_msg="retry reason")

    assert TaskQueueRepository.get_status("retrying") == status
    log.assert_awaited_once_with(
        "retrying", f"自动重试遇到同键下载任务 (#other-download): retry reason (当前状态: {status})"
    )
    broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_scheduler_rescans_range_when_checkpoint_changes_before_claim(monkeypatch):
    from app.core.runtime import acquire_queued_task

    task_id = "changed-range-checkpoint"
    params = {"channel": "example", "start_id": 1, "end_id": 20}
    TaskQueueRepository.create(task_id, "download_range", params, status="queued")
    sched = TaskScheduler(check_interval=0.05)
    executor = AsyncMock()
    sched.register_executor("download_range", executor)
    monkeypatch.setattr(client_manager, "get_available_account", AsyncMock(return_value=None))
    monkeypatch.setattr("app.core.scheduler.connection_manager.broadcast", AsyncMock())

    def resume_before_claim(tid, account_id, **kwargs):
        assert TaskQueueRepository.update_status(tid, "paused", from_statuses=("queued",))
        assert TaskQueueRepository.requeue_task_for_resume(tid, "paused", params_patch={"start_id": 8})
        return acquire_queued_task(tid, account_id, **kwargs)

    with patch("app.core.scheduler.acquire_queued_task", resume_before_claim):
        await sched._scan_queued_candidates(1, [])
    executor.assert_not_awaited()
    assert TaskQueueRepository.get_status(task_id) == "queued"

    await sched._scan_queued_candidates(1, [])
    for wrapper in tuple(sched._running_tasks.values()):
        await wrapper
    assert executor.await_args.args[1]["start_id"] == 8


@pytest.mark.asyncio
@pytest.mark.parametrize("resume_wins", [False, True])
async def test_pause_exit_reconciles_live_resume(monkeypatch, resume_wins):
    from app.core.runtime import resume_paused_executor, settle_task_pause_exit

    sched = TaskScheduler()
    task_id = "pause-exit-race"
    TaskQueueRepository.create(task_id, "download_link", {}, status="paused")
    version = TaskQueueRepository.get_by_id(task_id)["updated_at"]
    set_task_control(task_id, paused=True)
    release = asyncio.Event()

    async def executor(*_args):
        await release.wait()
        raise TaskPausedError("yield after pause")

    def settle(tid):
        if not resume_wins:
            assert not resume_paused_executor(tid, version)
        return settle_task_pause_exit(tid)

    monkeypatch.setattr("app.core.scheduler.settle_task_pause_exit", settle)
    monkeypatch.setattr("app.core.scheduler.connection_manager.broadcast", AsyncMock())
    sched.register_executor("download_link", executor)
    wrapper = asyncio.create_task(sched._run_task_wrapper(task_id, "download_link", {}, None, 0, 0))
    sched._running_tasks[task_id] = wrapper
    try:
        if resume_wins:
            assert await asyncio.to_thread(resume_paused_executor, task_id, version)
    finally:
        release.set()
        await wrapper

    assert not sched.is_task_running(task_id)
    assert task_id not in active_task_controls
    assert TaskQueueRepository.get_status(task_id) == ("queued" if resume_wins else "paused")


@pytest.mark.asyncio
async def test_slot_contention_retry_does_not_consume_retry_budget_or_mark_flood(monkeypatch):
    """editing 槽争用抛 RetryableTaskError(count_retry=False, is_flood_wait=False) 不消耗 retry_count 且不误标 FloodWait."""
    from app.core.scheduler import RetryableTaskError, TaskScheduler

    sched = TaskScheduler()

    tid = "sched_slot_wait_1"
    TaskQueueRepository.create(tid, "publish_local", {}, status="running")

    async def _executor(_tid, _params, _acc_id):
        raise RetryableTaskError("slot busy", retry_after=15.0, count_retry=False, is_flood_wait=False)

    sched.register_executor("publish_local", _executor)

    flood_calls = []
    monkeypatch.setattr(
        "app.core.scheduler.client_manager.mark_flood_wait",
        lambda acc_id, sec: flood_calls.append((acc_id, sec)),
    )

    before = time.time()
    await sched._run_task_wrapper(tid, "publish_local", {}, account_id=7, retry_count=0, max_retries=3)
    row = TaskQueueRepository.get_by_id(tid)
    assert row is not None
    assert row["status"] == "queued"
    assert row["retry_count"] == 0
    assert before + 15 <= row["next_retry_at"] <= time.time() + 15
    assert sched.is_waiting_for_slot_contention(tid)
    assert flood_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("still_busy", [True, False])
async def test_slot_contention_rechecks_once(still_busy):
    from app.core.scheduler import RetryableTaskError

    sched = TaskScheduler()
    tid = "exhausted_contention"
    TaskQueueRepository.create(tid, "publish_local", {}, status="running")

    async def recheck(*args):
        if still_busy or not sched.is_waiting_for_slot_contention(tid):
            raise RetryableTaskError("slot busy", retry_after=15, count_retry=False, is_flood_wait=False)
        await TaskQueueRepository.update_status_async(tid, "completed")

    sched.register_executor("publish_local", recheck)
    await sched._run_task_wrapper(tid, "publish_local", {}, account_id=None, retry_count=0, max_retries=3)
    assert not TaskQueueRepository.acquire_task(tid)

    TaskQueueRepository.update_status(tid, "running", from_statuses=("queued",))
    await sched._run_task_wrapper(tid, "publish_local", {}, account_id=None, retry_count=0, max_retries=3)
    row = TaskQueueRepository.get_by_id(tid)
    assert row["status"] == ("failed" if still_busy else "completed")
    assert row["retry_count"] == 0
    assert not sched.is_waiting_for_slot_contention(tid)
    if still_busy:
        assert row["next_retry_at"] == 0
        assert "等待 15 秒后复查仍被占用" in row["error_detail"]
        assert TaskQueueRepository.retry_task(tid)


@pytest.mark.asyncio
async def test_scheduler_missing_executor_still_cleans_resources_on_db_error():
    """#21: 未找到执行器分支在写库异常时仍须执行 _cleanup_task_resources, 不得泄漏配额."""
    sched = TaskScheduler()
    tid = "missing_exec_cleanup"
    sched._running_tasks[tid] = asyncio.create_task(asyncio.sleep(0))
    sched._account_task_counts[5] = 1
    sched._task_assigned_accounts[tid] = 5

    try:
        with (
            patch.object(TaskQueueRepository, "update_status", side_effect=RuntimeError("db down")),
            pytest.raises(RuntimeError, match="db down"),
        ):
            await sched._run_task_wrapper(tid, "unknown_type", {}, account_id=5, retry_count=0, max_retries=1)
        assert tid not in sched._running_tasks
        assert sched._account_task_counts[5] == 0
    finally:
        for t in list(sched._running_tasks.values()):
            t.cancel()


@pytest.mark.asyncio
async def test_scheduler_stop_synchronizes_inflight_scan_and_preserves_cancelled():
    """#20 & 取消优先: stop() 须等待在途扫描中止再快照收割, 且不得把已取消任务改标 paused."""
    import threading

    from app.core.runtime import active_task_controls, set_task_control

    sched = TaskScheduler(check_interval=0.05)
    sched._is_running = True
    loop = asyncio.get_running_loop()
    scan_entered = asyncio.Event()
    release_scan = threading.Event()

    def slow_get_candidates(*args, **kwargs):
        loop.call_soon_threadsafe(scan_entered.set)
        release_scan.wait(timeout=2.0)
        return [{"id": "late_task", "task_type": "publish_local", "params": {}}]

    cancelled_tid = "already_cancelled_during_stop"
    set_task_control(cancelled_tid, cancelled=True)
    sched._running_tasks[cancelled_tid] = asyncio.create_task(asyncio.sleep(0.05))

    try:
        with (
            patch.object(TaskQueueRepository, "get_queued_candidates", side_effect=slow_get_candidates),
            patch.object(TaskQueueRepository, "acquire_task") as m_acq,
            patch.object(TaskQueueRepository, "get_by_id", return_value={"status": "cancelled"}),
        ):
            scan_task = asyncio.create_task(sched._scan_queued_candidates(3, []))
            await asyncio.wait_for(scan_entered.wait(), timeout=2.0)

            stop_task = asyncio.create_task(sched.stop())
            await asyncio.sleep(0.02)
            assert not stop_task.done(), "在途扫描释放锁前 stop() 不得提前快照"
            assert active_task_controls[cancelled_tid]["paused"] is False

            release_scan.set()
            await asyncio.wait_for(stop_task, timeout=2.0)
            await asyncio.wait_for(scan_task, timeout=2.0)
            m_acq.assert_not_called()
            assert "late_task" not in sched._running_tasks
    finally:
        release_scan.set()
        active_task_controls.pop(cancelled_tid, None)


def test_mark_running_tasks_paused_skips_cancelled_tasks():
    """直接验证 _mark_running_tasks_paused 跳过已取消任务、仅暂停运行中任务."""
    from app.core.runtime import active_task_controls, set_task_control

    sched = TaskScheduler(check_interval=0.05)
    set_task_control("cancelled_t", cancelled=True, paused=False)
    set_task_control("live_t", cancelled=False, paused=False)
    sched._running_tasks["cancelled_t"] = None  # type: ignore[assignment]
    sched._running_tasks["live_t"] = None  # type: ignore[assignment]
    try:
        sched._mark_running_tasks_paused()
        assert active_task_controls["cancelled_t"]["paused"] is False
        assert active_task_controls["live_t"]["paused"] is True
    finally:
        active_task_controls.pop("cancelled_t", None)
        active_task_controls.pop("live_t", None)


@pytest.mark.asyncio
async def test_scheduler_stop_resets_stopping_flag_for_subsequent_schedule_once():
    """stop() 结束后须复位 _stopping, 保证后续单测/手动调用 schedule_once() 不会被永久拦截."""
    sched = TaskScheduler(check_interval=0.05)
    await sched.start()
    await sched.stop()
    assert sched._stopping is False
    with patch.object(TaskQueueRepository, "get_queued_candidates", return_value=[]) as m_get:
        await sched.schedule_once()
        m_get.assert_called_once()


@pytest.mark.asyncio
async def test_scheduler_stop_guards_paused_status_write_with_cas():
    """stop() 对收割任务回写 paused 时须以刚读取的状态作 from_statuses 守卫."""
    sched = TaskScheduler(check_interval=0.05)
    tid = "stop_cas_guard_task"
    sched._running_tasks[tid] = asyncio.create_task(asyncio.sleep(0))

    with (
        patch.object(TaskQueueRepository, "get_by_id", return_value={"id": tid, "status": "running"}),
        patch.object(TaskQueueRepository, "update_status", return_value=True) as m_upd,
    ):
        await sched.stop()
        m_upd.assert_called_once_with(tid, "paused", ("running",))


def test_scheduler_get_running_task_ids_returns_frozenset():
    sched = TaskScheduler()
    sched._running_tasks["a"] = MagicMock()
    sched._running_tasks["b"] = MagicMock()
    assert sched.get_running_task_ids() == frozenset({"a", "b"})


@pytest.mark.asyncio
async def test_scheduler_retryable_floodwait_marks_cooldown_and_min_delay():
    """RetryableTaskError 包装 FloodWaitError 时必须触发账号冷却并按 wait 传 min_delay."""
    from telethon.errors import FloodWaitError

    from app.core.scheduler import RetryableTaskError

    sched = TaskScheduler()
    fw = FloodWaitError(request=None)
    fw.seconds = 420
    wrapped_exc = RetryableTaskError("album flood")
    wrapped_exc.__cause__ = fw

    async def boom_executor(task_id: str, params: dict, account_id: int) -> None:
        raise wrapped_exc

    sched._executors["local_publish"] = boom_executor
    with (
        patch("app.core.scheduler.client_manager.mark_flood_wait") as m_fw,
        patch.object(sched, "_handle_task_retry", new_callable=AsyncMock) as m_retry,
    ):
        await sched._run_task_wrapper("t_fw_1", "local_publish", {}, account_id=7, retry_count=1, max_retries=3)

    m_fw.assert_called_once_with(7, 420)
    assert m_retry.await_args is not None
    assert m_retry.await_args.kwargs["min_delay"] == 420.0

    plain_exc = RetryableTaskError("plain transient")

    async def plain_executor(task_id: str, params: dict, account_id: int) -> None:
        raise plain_exc

    sched._executors["local_publish"] = plain_executor
    with (
        patch("app.core.scheduler.client_manager.mark_flood_wait") as m_fw2,
        patch.object(sched, "_handle_task_retry", new_callable=AsyncMock) as m_retry2,
    ):
        await sched._run_task_wrapper("t_fw_2", "local_publish", {}, account_id=7, retry_count=0, max_retries=3)

    m_fw2.assert_not_called()
    assert m_retry2.await_args is not None
    assert m_retry2.await_args.kwargs["min_delay"] == 0.0


@pytest.mark.asyncio
async def test_scheduler_does_not_cooldown_userbot_when_bot_pool_hits_flood_wait(tmp_path):
    """Bot Pool 遇 FloodWait 仅冷却对应 Bot, 调度器不得连坐冷却 userbot account_id."""
    from telethon.errors import FloodWaitError

    from app.core.pipeline.engines import LocalDiskTransportEngine
    from app.core.pipeline.models import PublishContext, PublishItem
    from app.core.pipeline.pipeline import PublishPipeline
    from app.core.pipeline.runtime import BatchState, Counters
    from app.core.scheduler import RetryableTaskError

    items = []
    for i in range(2):
        f = tmp_path / f"img_{i}.jpg"
        f.write_bytes(b"jpg")
        items.append(PublishItem(item_id=str(f), name=f.name, size_bytes=3, source_type="local"))

    bot_client = AsyncMock()
    fw = FloodWaitError(request=None)
    fw.seconds = 19
    bot_client.send_file = AsyncMock(side_effect=fw)

    mock_registry = MagicMock()
    mock_registry.bots = [bot_client]
    mock_registry.bot_index = 1
    mock_registry.userbot = MagicMock()
    mock_registry.get_next_available_bot = MagicMock(return_value=bot_client)
    mock_registry.mark_flood_wait = MagicMock()

    engine = LocalDiskTransportEngine(registry=mock_registry)
    ctx = PublishContext(task_id="t_bot_fw", target_chat_id=-100777, account_id=7, delay_seconds=0.0)
    with patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()):
        outcomes = await engine.send_album(items, ctx)

    mock_registry.mark_flood_wait.assert_called_once_with(bot_client, 19)
    assert outcomes[0].flood_marked is True

    pipeline = PublishPipeline(source=MagicMock(), engine=engine)
    b = BatchState()
    counters = Counters(2)
    with pytest.raises(RetryableTaskError) as rt_exc:
        await pipeline._route_album_outcomes(items, outcomes, ctx, counters, b, is_pikpak=False)
    assert rt_exc.value.flood_client_marked is True
    assert rt_exc.value.retry_after == 19.0

    sched = TaskScheduler()
    sched._executors["publish_folder"] = AsyncMock(side_effect=rt_exc.value)
    sched._handle_task_retry = AsyncMock()
    sched._cleanup_task_resources = AsyncMock()
    with patch("app.core.scheduler.client_manager.mark_flood_wait") as m_acc_fw:
        await sched._run_task_wrapper("t_bot_fw", "publish_folder", {}, account_id=7, retry_count=0, max_retries=3)
    m_acc_fw.assert_not_called()
    assert sched._handle_task_retry.await_args.kwargs["min_delay"] == 19.0

    unmarked_err = RetryableTaskError("userbot fw", retry_after=25.0)
    sched._executors["pikpak_publish"] = AsyncMock(side_effect=unmarked_err)
    sched._handle_task_retry.reset_mock()
    with (
        patch("app.core.scheduler.client_manager.mark_flood_wait") as m_acc_fw2,
        patch.object(TaskQueueRepository, "get_by_id_async", new_callable=AsyncMock, return_value={"retry_count": 0}),
    ):
        await sched._run_task_wrapper("t_ub_fw", "pikpak_publish", {}, account_id=7, retry_count=0, max_retries=3)
    m_acc_fw2.assert_called_once_with(7, 25)


@pytest.mark.asyncio
async def test_scheduler_rejects_missing_explicit_account_and_preflight_blocks_zero_sentinel():
    """显式指定失效 account_id 在调度准入阶段原子拒绝，不退化为 0/默认账号；preflight 拒绝 0."""
    from app.core.pikpak.taxonomy import PikPakPublishFatalError
    from app.core.pipeline.models import PublishContext
    from app.core.pipeline.preflight import phase1_preflight

    sched = TaskScheduler(check_interval=0.05)
    called_with: list = []

    async def mock_exec(task_id: str, params: dict, account_id: int | None) -> None:
        called_with.append((task_id, account_id))
        TaskQueueRepository.update_status(task_id, "completed")

    sched.register_executor("mock_acc_check", mock_exec)

    TaskQueueRepository.create("t_missing_acc", "mock_acc_check", {}, account_id=None, status="queued")
    missing_candidate = {
        "id": "t_missing_acc",
        "task_type": "mock_acc_check",
        "params": {},
        "account_id": 999,
        "retry_count": 0,
        "max_retries": 3,
    }
    try:
        with (
            patch.object(TaskQueueRepository, "get_queued_candidates", side_effect=[[missing_candidate], []]),
            patch.object(client_manager, "get_account", return_value=None),
            patch("app.core.connection_manager.connection_manager.broadcast", new_callable=AsyncMock),
        ):
            await sched._schedule_next_tasks()

        rec = TaskQueueRepository.get_by_id("t_missing_acc")
        assert rec is not None and rec["status"] == "failed"
        assert "999" in (rec.get("error_detail") or "")
        assert called_with == []
    finally:
        TaskQueueRepository.delete("t_missing_acc")

    TaskQueueRepository.create("t_unassigned_none", "mock_acc_check", {}, account_id=None, status="queued")
    try:
        with (
            patch.object(client_manager, "get_available_account", new_callable=AsyncMock, return_value=None),
            patch("app.core.connection_manager.connection_manager.broadcast", new_callable=AsyncMock),
        ):
            await sched.schedule_once()
            if "t_unassigned_none" in sched._running_tasks:
                await sched._running_tasks["t_unassigned_none"]
        assert called_with == [("t_unassigned_none", None)]
    finally:
        TaskQueueRepository.delete("t_unassigned_none")

    mock_default_acc = MagicMock(account_id=1, account_name="Default", is_ready=MagicMock(return_value=True))
    with (
        patch.object(client_manager, "get_account", return_value=mock_default_acc) as m_get,
        pytest.raises(PikPakPublishFatalError, match="ID=0"),
    ):
        await phase1_preflight(PublishContext(task_id="t_zero", target_chat_id=-1001, account_id=0), True, None)
    m_get.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("rejected", [False, True])
async def test_account_admission_finalizes_only_successful_rejection(rejected):
    from app.core.pipeline.placeholder import PlaceholderSlotCoordinator

    with (
        patch.object(TaskQueueRepository, "fail_queued_task", return_value=rejected),
        patch.object(PlaceholderSlotCoordinator, "matches_terminal_rejection", return_value=True),
        patch.object(TaskQueueRepository, "append_log_async", new_callable=AsyncMock),
        patch.object(PlaceholderSlotCoordinator, "finalize_terminal", new_callable=AsyncMock) as finalize,
        patch("app.core.connection_manager.connection_manager.broadcast", new_callable=AsyncMock),
    ):
        await TaskScheduler()._reject_unavailable_explicit_account("rejected", 999)
    if rejected:
        finalize.assert_awaited_once_with(
            "rejected", expected_error="指定执行账号 (ID=999) 不存在或已停用，拒绝静默回退默认账号"
        )
    else:
        finalize.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_at", ["before_finalize", "during_finalize", "never"])
async def test_rejection_skips_stale_notifications_and_contains_finalization_error(changed_at):
    from app.core.pipeline.placeholder import PlaceholderSlotCoordinator

    checks = [False] if changed_at == "before_finalize" else [True, changed_at == "never"]
    with (
        patch.object(PlaceholderSlotCoordinator, "matches_terminal_rejection", side_effect=checks),
        patch.object(
            PlaceholderSlotCoordinator,
            "finalize_terminal",
            new_callable=AsyncMock,
            side_effect=RuntimeError("finalization failed"),
        ) as finalize,
        patch("app.core.connection_manager.connection_manager.broadcast", new_callable=AsyncMock) as broadcast,
    ):
        await TaskScheduler()._notify_account_rejection("rejected", 999, "rejection")
    assert finalize.await_count == int(changed_at != "before_finalize")
    assert broadcast.await_count == int(changed_at == "never")


@pytest.mark.asyncio
async def test_scheduler_rejection_side_effects_run_after_unlock():
    from app.core.pipeline.placeholder import PlaceholderSlotCoordinator

    scheduler = TaskScheduler()
    candidate = {"id": "missing", "task_type": "publish", "account_id": 999}

    unlocked_states = []

    async def record_lock_state(*args, **kwargs):
        unlocked_states.append(not scheduler._get_lock().locked())

    with (
        patch.object(TaskQueueRepository, "get_queued_candidates", return_value=[candidate]),
        patch.object(TaskQueueRepository, "fail_queued_task", return_value=True),
        patch.object(TaskQueueRepository, "append_log_async", new_callable=AsyncMock),
        patch.object(client_manager, "get_account", return_value=None),
        patch.object(PlaceholderSlotCoordinator, "matches_terminal_rejection", return_value=True),
        patch.object(PlaceholderSlotCoordinator, "finalize_terminal", side_effect=record_lock_state) as finalize,
        patch("app.core.connection_manager.connection_manager.broadcast", side_effect=record_lock_state) as broadcast,
    ):
        await scheduler._schedule_next_tasks()
    finalize.assert_awaited_once()
    broadcast.assert_awaited_once()
    assert unlocked_states == [True, True]


@pytest.mark.asyncio
@pytest.mark.parametrize("inactive", [False, True])
@pytest.mark.parametrize("scan_fails", [False, True])
async def test_account_rejections_survive_logging_and_scan_failures(inactive, scan_fails):
    from types import SimpleNamespace

    scheduler = TaskScheduler()
    candidates = [{"id": f"rejected_{i}", "task_type": "publish", "account_id": 999} for i in range(2)]
    if scan_fails:
        candidates.append({"id": "malformed"})
    notified = []
    registered = []

    async def broken_log(task_id, text):
        registered.append(task_id)
        raise RuntimeError("log unavailable")

    async def notify(task_id, account_id, error):
        notified.append((task_id, scheduler._get_lock().locked()))
        if task_id == "rejected_0":
            raise RuntimeError("notification unavailable")

    with (
        patch.object(TaskQueueRepository, "get_queued_candidates", return_value=candidates),
        patch.object(TaskQueueRepository, "fail_queued_task", return_value=True) as reject,
        patch.object(TaskQueueRepository, "append_log_async", side_effect=broken_log),
        patch.object(TaskQueueRepository, "acquire_task") as acquire,
        patch.object(
            client_manager, "get_account", return_value=SimpleNamespace(is_active=False) if inactive else None
        ),
        patch.object(scheduler, "_notify_account_rejection", side_effect=notify),
    ):
        if scan_fails:
            with pytest.raises(KeyError, match="task_type"):
                await scheduler._schedule_next_tasks()
        else:
            await scheduler._schedule_next_tasks()
    assert reject.call_count == 2
    assert registered == ["rejected_0", "rejected_1"]
    assert notified == [("rejected_0", False), ("rejected_1", False)]
    acquire.assert_not_called()
    assert scheduler._running_tasks == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("stopped_status", ["paused", "cancelled"])
async def test_retry_exhaustion_preserves_stopped_status(monkeypatch, stopped_status):
    from app.core.pipeline.placeholder import PlaceholderSlotCoordinator

    sched = TaskScheduler()
    task_id = f"exhaust_stop_{stopped_status}"
    TaskQueueRepository.create(task_id, "publish_folder", {}, status=stopped_status)
    broadcast = AsyncMock()
    log_mock = AsyncMock()
    finalize_mock = AsyncMock()
    monkeypatch.setattr("app.core.scheduler.connection_manager.broadcast", broadcast)
    monkeypatch.setattr(TaskQueueRepository, "append_log_async", log_mock)
    monkeypatch.setattr(PlaceholderSlotCoordinator, "finalize_terminal", finalize_mock)

    await sched._handle_task_retry(task_id, retry_count=3, max_retries=3, error_msg="boom")

    assert TaskQueueRepository.get_status(task_id) == stopped_status
    log_mock.assert_not_awaited()
    finalize_mock.assert_not_awaited()
    broadcast.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("stopped_status", ["paused", "cancelled"])
async def test_fatal_error_and_pikpak_cooldown_preserve_stopped_status(monkeypatch, stopped_status):
    from app.core.pipeline.placeholder import PlaceholderSlotCoordinator
    from app.core.scheduler import FatalTaskError
    from app.core.stream_chunk_provider import PikPakThrottledError

    sched = TaskScheduler()
    tid_fatal = f"fatal_stop_{stopped_status}"
    tid_cool = f"cool_stop_{stopped_status}"
    TaskQueueRepository.create(tid_fatal, "publish_folder", {}, status="running")
    TaskQueueRepository.create(tid_cool, "pikpak_publish", {}, status="running")
    broadcast = AsyncMock()
    finalize_mock = AsyncMock()
    monkeypatch.setattr("app.core.scheduler.connection_manager.broadcast", broadcast)
    monkeypatch.setattr(PlaceholderSlotCoordinator, "finalize_terminal", finalize_mock)

    async def fatal_exec(tid, *_args):
        TaskQueueRepository.update_status(tid, stopped_status)
        raise FatalTaskError("fatal err")

    async def cool_exec(tid, *_args):
        TaskQueueRepository.update_status(tid, stopped_status)
        raise PikPakThrottledError("429", status=429)

    sched.register_executor("publish_folder", fatal_exec)
    sched.register_executor("pikpak_publish", cool_exec)

    await sched._run_task_wrapper(tid_fatal, "publish_folder", {}, None, retry_count=0, max_retries=3)
    await sched._run_task_wrapper(tid_cool, "pikpak_publish", {}, None, retry_count=0, max_retries=3)

    assert TaskQueueRepository.get_status(tid_fatal) == stopped_status
    assert TaskQueueRepository.get_status(tid_cool) == stopped_status
    finalize_mock.assert_not_awaited()
    broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_requeue_resumed_pause_exit_retries_transient_db_error_and_drops_conflict(monkeypatch):
    from app.core.runtime import resume_paused_executor, settle_task_pause_exit

    sched = TaskScheduler()
    task_id = "resume-transient-retry"
    TaskQueueRepository.create(task_id, "download_link", {"links": ["https://t.me/c/1/2"]}, status="paused")
    version = TaskQueueRepository.get_by_id(task_id)["updated_at"]
    set_task_control(task_id, paused=True)
    assert await asyncio.to_thread(resume_paused_executor, task_id, version)
    resume_controls = settle_task_pause_exit(task_id)
    assert resume_controls is not None

    broadcast = AsyncMock()
    monkeypatch.setattr("app.core.scheduler.connection_manager.broadcast", broadcast)

    with patch.object(
        TaskQueueRepository,
        "requeue_task_for_resume",
        side_effect=RuntimeError("database is locked"),
    ):
        await sched._requeue_resumed_pause_exit(task_id, resume_controls)

    assert TaskQueueRepository.get_status(task_id) == "paused"
    assert dict.get(active_task_controls, task_id) is resume_controls
    assert task_id in sched._pending_resume_requeues

    with patch.object(TaskQueueRepository, "get_queued_candidates", return_value=[]):
        await sched._schedule_next_tasks()

    assert TaskQueueRepository.get_status(task_id) == "queued"
    assert task_id not in active_task_controls
    assert task_id not in sched._pending_resume_requeues

    # 同键冲突不得保留恢复意图或无限重试
    conflict_tid = "resume-conflict-drop"
    TaskQueueRepository.create(conflict_tid, "download_link", {"download_target_key": "dup-key"}, status="paused")
    TaskQueueRepository.create("other-active", "download_link", {"download_target_key": "dup-key"}, status="running")
    c_ver = TaskQueueRepository.get_by_id(conflict_tid)["updated_at"]
    set_task_control(conflict_tid, paused=True)
    assert await asyncio.to_thread(resume_paused_executor, conflict_tid, c_ver)
    c_controls = settle_task_pause_exit(conflict_tid)
    assert c_controls is not None
    await sched._requeue_resumed_pause_exit(conflict_tid, c_controls)
    assert TaskQueueRepository.get_status(conflict_tid) == "paused"
    assert conflict_tid not in active_task_controls
    assert conflict_tid not in sched._pending_resume_requeues


def test_update_download_threads_caps_at_global_limit():
    sched = TaskScheduler()
    sched._task_thread_allocations = {"t1": 24, "t2": 4}
    sched._global_download_threads = 28

    sched.update_download_threads("t2", 16)

    assert sched._task_thread_allocations["t2"] == 8
    assert sched._global_download_threads == sched.MAX_GLOBAL_DOWNLOAD_THREADS
