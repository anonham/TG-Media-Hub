"""Database Layer: 事务隔离级别自动恢复、多层级任务排序与闭环事务防御测试 (Transaction Recovery & Consistency)

覆盖范围:
- 原 test_transaction_isolation_recovery: 脏事务防御重置、isolation_level 异常自愈与连接池恢复
- 原 test_task_ordering: 任务队列多层级防截断排序、分页偏移量与权重稳定性
- 原 test_closed_loop_fixes: 闭环修复矩阵 (外键完整性、单写锁重入、死锁重试与事务回滚)"""

import asyncio
import concurrent.futures
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from telethon.errors import FloodWaitError

from app.api.auth import issue_2fa_token
from app.config import settings
from app.core.client_manager import TelegramAccountClient, client_manager
from app.core.runtime import active_task_controls
from app.core.scheduler import (
    RetryableTaskError,
    UploadPausedError,
    task_scheduler,
)
from app.db.account_repository import AccountRepository
from app.db.database import db_cursor, db_immediate_tx, get_db
from app.db.forward_repository import ForwardLedgerRepository
from app.db.task_repository import TaskQueueRepository
from app.main import app
from app.services.publish_runner import run_publish_folder_task
from app.services.publisher_service import PublisherService


# ==============================================================================
# Part: test_transaction_isolation_recovery.py
# ==============================================================================
def test_append_log_restores_isolation_level():
    """验证 _append_log_db 执行后严格还原原有 isolation_level，绝不向连接池泄漏 autocommit (None)"""
    conn = get_db()
    initial_isolation = conn.isolation_level
    assert initial_isolation == "", "SQLite 连接默认隔离级别应为 DEFERRED ('')"

    # 1. 模拟正常日志写入
    time_str, seq = TaskQueueRepository._append_log_db("non_existent_task", "test log line")

    # 获取当前线程复用的连接，断言其隔离级别已 100% 还原
    conn_after = get_db()
    assert conn_after.isolation_level == initial_isolation, "必须还原为初始隔离级别"
    assert conn_after.isolation_level is not None, "严禁连接残留 isolation_level = None"


def test_append_log_restores_isolation_level_on_exception(monkeypatch):
    """验证 _append_log_db 即使在内部执行报错时，finally 依然可靠还原 isolation_level"""
    conn = get_db()
    initial_isolation = conn.isolation_level

    # 构造引发异常的 mock cursor
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = RuntimeError("Simulated DB Crash")

    monkeypatch.setattr(conn, "cursor", lambda: mock_cursor)

    with pytest.raises(RuntimeError, match="Simulated DB Crash"):
        TaskQueueRepository._append_log_db("test_task_err", "crash log")

    # 校验即便抛错，finally 依然恢复了原有隔离级别
    assert conn.isolation_level == initial_isolation


def test_get_db_defensive_reset_tampered_isolation():
    """验证 get_db 签出连接时具有防御性自愈能力，能自动纠正被非法篡改的 isolation_level"""
    conn = get_db()
    # 模拟外部不合规代码篡改了连接状态
    conn.isolation_level = None

    # 再次调用 get_db 获取该连接，应被防御性复位为 ""
    conn_checked = get_db()
    assert conn_checked.isolation_level == "", "get_db 应自动纠正被篡改的 isolation_level"


# ==============================================================================
# Part: test_task_ordering.py
# ==============================================================================
def _create_raw_task(task_id: str, status: str, created_at: float, updated_at: float):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO task_queue (
            id, task_type, status, progress_bytes, total_bytes, processed_items, total_items, speed_mb_s, eta_s,
            current_item, params_json, created_at, updated_at,
            concurrency, dc_id, concurrency_total, account_id,
            priority, retry_count, max_retries, next_retry_at, error_detail
        ) VALUES (
            ?, 'download', ?, 0, 1000, 0, 0, 0.0, 0.0,
            'test_item', '{}', ?, ?,
            1, 1, 1, NULL,
            10, 0, 3, 0.0, ''
        )
        """,
        (task_id, status, created_at, updated_at),
    )
    conn.commit()
    conn.close()


def _populate_mixed_tasks():
    # 1. 插入 1 个很早创建的 running 任务 (比如 2 小时前创建的 20GB 大文件下载)
    _create_raw_task("task_running_old", "running", created_at=10.0, updated_at=15.0)

    # 2. 插入 2 个排队任务 (FIFO: t=20 先于 t=30 入队)
    _create_raw_task("task_queued_1", "queued", created_at=20.0, updated_at=20.0)
    _create_raw_task("task_queued_2", "queued", created_at=30.0, updated_at=30.0)

    # 3. 插入 1 个暂停任务
    _create_raw_task("task_paused_1", "paused", created_at=40.0, updated_at=45.0)

    # 4. 插入 1 个失败任务 (更新时间较新)
    _create_raw_task("task_failed_1", "failed", created_at=50.0, updated_at=2000.0)

    # 5. 插入 55 个新创建且已完成的任务 (创建时间 1000~1054)
    for i in range(55):
        _create_raw_task(f"task_completed_{i}", "completed", created_at=1000.0 + i, updated_at=1100.0 + i)


def test_task_multi_tier_ordering_anti_truncation():
    """验证多状态优先级阶梯排序与防截断机制:
    在有 55 条新创建的已完成任务时，较早创建的 running/queued/paused 活跃任务绝不能被 LIMIT 50 截断丢弃。
    """
    _populate_mixed_tasks()

    # 6. 调用 get_all(limit=50)
    tasks = TaskQueueRepository.get_all(limit=50)
    assert len(tasks) == 50

    # 核心断言 1: running 任务必须置顶 (Tier 1)，防截断生效！
    assert tasks[0]["id"] == "task_running_old"
    assert tasks[0]["status"] == "running"

    # 核心断言 2: queued 任务排在 Tier 2，且严格按照 FIFO (创建时间升序)
    assert tasks[1]["id"] == "task_queued_1"
    assert tasks[2]["id"] == "task_queued_2"

    # 核心断言 3: paused 任务排在 Tier 3
    assert tasks[3]["id"] == "task_paused_1"

    # 核心断言 4: failed 任务排在 Tier 4
    assert tasks[4]["id"] == "task_failed_1"

    # 核心断言 5: completed 任务排在 Tier 5，且按更新时间倒序 (最新完成的 task_completed_54 优先展示)
    assert tasks[5]["id"] == "task_completed_54"
    assert tasks[6]["id"] == "task_completed_53"


def test_query_tasks_pagination_and_ordering():
    """验证服务端分页 query_tasks 同步享有阶梯与 FIFO 排序能力"""
    _populate_mixed_tasks()
    tasks, total = TaskQueueRepository.query_tasks(page=1, limit=10)
    assert total == 60  # 1 running + 2 queued + 1 paused + 1 failed + 55 completed = 60
    assert len(tasks) == 10
    assert tasks[0]["id"] == "task_running_old"
    assert tasks[1]["id"] == "task_queued_1"
    assert tasks[2]["id"] == "task_queued_2"
    assert tasks[3]["id"] == "task_paused_1"
    assert tasks[4]["id"] == "task_failed_1"


# ==============================================================================
# Part: test_closed_loop_fixes.py
# ==============================================================================
client = TestClient(app, client=("127.0.0.1", 50000))


def auth_headers():
    headers = {}
    if settings.API_KEY:
        headers["X-API-Key"] = settings.API_KEY
        token = issue_2fa_token()
        if token:
            headers["X-2FA-Token"] = token
    return headers


@pytest.mark.asyncio
async def test_merge_params_atomic_concurrent_race():
    """验证 BEGIN IMMEDIATE 事务锁：多真实 OS 线程并发原子写入不同字段无丢失更新 (Lost Update)"""
    task_id = "test_race_task"
    TaskQueueRepository.create(task_id, "download_link", {"initial": 1})

    try:

        def thread_worker(idx: int):
            time.sleep(0.002 * (idx % 4))
            TaskQueueRepository.merge_params(task_id, {f"key_{idx}": f"val_{idx}"})

        loop = asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            tasks = [loop.run_in_executor(pool, thread_worker, i) for i in range(20)]
            await asyncio.gather(*tasks)

        t = TaskQueueRepository.get_by_id(task_id)
        params = t.get("params") or {}
        assert params.get("initial") == 1
        for i in range(20):
            assert params.get(f"key_{i}") == f"val_{i}", f"Lost update on key_{i}"
    finally:
        TaskQueueRepository.delete(task_id)


def test_forward_dry_run_resume_lifecycle():
    """验证试转模式转发任务恢复闭环 (缺陷 #1 根因闭环):
    首批完成后自动暂停 -> resume -> 验证 dry_run 清除为 False 且 start_id 正确推进
    """
    task_id = "test_fwd_dry_run"
    source = "-1000000900038"
    target = "-1000000900039"
    TaskQueueRepository.create(
        task_id,
        "forward_channel",
        {"source_chat": source, "target_chat": target, "start_id": 1, "end_id": 20, "dry_run": True, "batch_size": 5},
    )
    TaskQueueRepository.update_status(task_id, "paused")

    # 模拟账本记录已转发前 5 条 (首批试转)
    ForwardLedgerRepository.commit_batch_records(
        task_id=task_id,
        source_chat=source,
        target_chat=target,
        mappings=[(m_id, m_id + 10000) for m_id in range(1, 6)],
        skips=[],
        seg_start=1,
        seg_end=5,
    )

    try:
        resp = client.post(f"/api/tasks/{task_id}/resume", headers=auth_headers())
        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"

        # 检查持久化入库的 params_json
        t = TaskQueueRepository.get_by_id(task_id)
        params = t.get("params") or {}
        assert params.get("dry_run") is False, "恢复后 dry_run 必须被原子置为 False"
        assert params.get("start_id") == 6, "恢复后 start_id 必须推进至未覆盖起点 #6"
    finally:
        TaskQueueRepository.delete(task_id)
        conn = get_db()
        conn.cursor().execute("DELETE FROM forward_history WHERE source_chat = ? AND target_chat = ?", (source, target))
        conn.cursor().execute(
            "DELETE FROM forward_coverage WHERE source_chat = ? AND target_chat = ?", (source, target)
        )
        conn.commit()
        conn.close()


def test_download_range_contiguous_cursor_recovery():
    """验证范围下载连续游标闭环 (缺陷 #2 根因闭环):
    若消息 #100..105 连续成功，#106 失败，#107 成功后暂停 -> 游标保持 #105，恢复从 #106 开始
    """
    task_id = "test_range_cursor"
    TaskQueueRepository.create(
        task_id, "download_range", {"channel": "test_chan", "start_id": 100, "end_id": 120, "contiguous_cursor_id": 105}
    )
    TaskQueueRepository.update_status(task_id, "paused")

    try:
        resp = client.post(f"/api/tasks/{task_id}/resume", headers=auth_headers())
        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"

        t = TaskQueueRepository.get_by_id(task_id)
        params = t.get("params") or {}
        assert params.get("start_id") == 106, "恢复时必须从 contiguous_cursor_id + 1 = 106 续传，不跳过失败项"
    finally:
        TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_head_of_line_blocking_fairness():
    """验证公平调度资源排除扫描 (缺陷 #9 根因闭环):
    当队头候选任务占满账号 A 配额时，调度器自动跳过并调度后续账号 B 的任务，防止队头阻塞
    """
    # 构造两个测试账号
    acc_a = TelegramAccountClient(
        account_id=901, account_name="BotA", phone="+1001", session_file="a.session", api_id=123, api_hash="hash_a"
    )
    acc_a.max_concurrency = 1
    acc_b = TelegramAccountClient(
        account_id=902, account_name="BotB", phone="+1002", session_file="b.session", api_id=456, api_hash="hash_b"
    )
    acc_b.max_concurrency = 1

    client_manager._accounts[901] = acc_a
    client_manager._accounts[902] = acc_b

    # 注册一个测试执行器避免调度器报错
    async def dummy_exec(t_id, params, acc_id):
        await asyncio.sleep(5)

    task_scheduler.register_executor("download_link", dummy_exec)

    # 模拟账号 A 已占满 1 个并发槽位
    task_scheduler._account_task_counts[901] = 1
    dummy_sleep = asyncio.create_task(asyncio.sleep(10))
    task_scheduler._running_tasks["dummy_running_a"] = dummy_sleep

    # 在队列中塞入 3 个账号 A 的高优先级任务，以及 1 个账号 B 的任务
    t_a1 = "task_a_prio_1"
    t_a2 = "task_a_prio_2"
    t_b1 = "task_b_prio_1"

    TaskQueueRepository.create(t_a1, "download_link", {"links": ["https://t.me/c/1"]}, account_id=901, priority=50)
    TaskQueueRepository.create(t_a2, "download_link", {"links": ["https://t.me/c/2"]}, account_id=901, priority=40)
    TaskQueueRepository.create(t_b1, "download_link", {"links": ["https://t.me/c/3"]}, account_id=902, priority=10)

    try:
        # 执行单次调度扫描
        newly_scheduled = await task_scheduler.schedule_once()
        assert newly_scheduled == 1, "应成功调度 1 个跨账号任务"
        assert t_b1 in task_scheduler._running_tasks, "应跨过已饱和的账号 A，成功调度账号 B 的任务"
        assert t_a1 not in task_scheduler._running_tasks, "账号 A 并发已满，其任务应保留在排队中"
    finally:
        dummy_sleep.cancel()
        task_scheduler._running_tasks.pop("dummy_running_a", None)
        task_scheduler._running_tasks.pop(t_b1, None)
        task_scheduler._account_task_counts.clear()
        client_manager._accounts.pop(901, None)
        client_manager._accounts.pop(902, None)
        TaskQueueRepository.delete(t_a1)
        TaskQueueRepository.delete(t_a2)
        TaskQueueRepository.delete(t_b1)


def test_auxiliary_account_borrow_no_leak():
    """验证辅助账号借用幂等性与零泄漏 (缺陷 #5 根因闭环):
    同任务重复借用同一辅助账号不重复递增，任务退出时配额精准归零
    """
    task_id = "test_aux_leak"
    aux_id = 950
    acc = TelegramAccountClient(
        account_id=aux_id,
        account_name="AuxUserbot",
        phone="+1950",
        session_file="aux.session",
        api_id=789,
        api_hash="hash_aux",
    )
    client_manager._accounts[aux_id] = acc

    try:
        # 连续借用 5 次
        for _ in range(5):
            task_scheduler.borrow_auxiliary_account(task_id, aux_id)

        # 单一可信源断言
        assert task_scheduler.get_account_task_count(aux_id) == 1, "重复借用同一账号计数必须保持为 1 (幂等)"

        # 任务资源释放
        task_scheduler._cleanup_task_resources(task_id, account_id=0)

        assert task_scheduler.get_account_task_count(aux_id) == 0, "任务退出后辅助账号配额必须归零"
    finally:
        client_manager._accounts.pop(aux_id, None)


@pytest.mark.asyncio
async def test_publish_folder_pause_during_chunk_upload():
    """验证发布任务分块上传暂停状态机 (缺陷 #7 根因闭环):
    大文件上传中途触发暂停让位，必须终态落库为 paused，绝不错误写为 failed
    """
    task_id = "test_pub_pause_status"
    TaskQueueRepository.create(task_id, "publish_folder", {"folder_path": "dummy_dir", "target_channel": "-100123"})
    TaskQueueRepository.update_status(task_id, "running")
    active_task_controls[task_id] = {"cancelled": False, "paused": False}

    # 模拟上传中途用户暂停: 服务在执行器运行中检测到暂停信号让位 (调度窗口竞态修复后
    # 入口自检会拦截「启动前已带 paused 标志」的情况, 故标志需在执行器启动后才置位)
    def _pause_mid_upload(*args, **kwargs):
        active_task_controls[task_id]["paused"] = True
        raise UploadPausedError("任务让位暂停")

    with patch.object(PublisherService, "publish_folder", side_effect=_pause_mid_upload):
        with pytest.raises(UploadPausedError):
            await run_publish_folder_task(task_id, "dummy_dir", "-100123", album_mode=True, delay_seconds=1.0)

    t = TaskQueueRepository.get_by_id(task_id)
    assert t["status"] == "paused", f"暂停上传必须落库为 paused，实际为: {t['status']}"
    pause_logs, _, _ = TaskQueueRepository.get_task_logs(task_id, 0, 200)
    assert any("暂停" in l["message"] for l in pause_logs)
    assert task_id not in active_task_controls

    TaskQueueRepository.delete(task_id)


def test_delete_state_machine_guardrails():
    """验证 delete_task 状态机防线 (缺陷 #8 根因闭环):
    - queued / running: 禁止删除 (400)
    - paused 且协程存活: 禁止删除 (400)
    - paused 且协程已退出: 允许安全物理删除 (200)
    """
    # 1. queued 任务禁止删除
    t_queued = "test_del_queued"
    TaskQueueRepository.create(t_queued, "download_link", {})
    TaskQueueRepository.update_status(t_queued, "queued")
    try:
        resp = client.delete(f"/api/tasks/{t_queued}", headers=auth_headers())
        assert resp.status_code == 400
        assert "不能删除正在运行/暂停中的任务" in resp.json()["detail"]
    finally:
        TaskQueueRepository.delete(t_queued)

    # 2. paused + live_coroutine 禁止删除
    t_paused_live = "test_del_paused_live"
    TaskQueueRepository.create(t_paused_live, "download_link", {})
    TaskQueueRepository.update_status(t_paused_live, "paused")
    active_task_controls[t_paused_live] = {"cancelled": False, "paused": True}
    try:
        resp = client.delete(f"/api/tasks/{t_paused_live}", headers=auth_headers())
        assert resp.status_code == 400
        assert "不能删除正在运行/暂停中的任务" in resp.json()["detail"]
    finally:
        active_task_controls.pop(t_paused_live, None)
        TaskQueueRepository.delete(t_paused_live)

    # 3. paused + dead_coroutine 允许物理删除
    t_paused_dead = "test_del_paused_dead"
    TaskQueueRepository.create(t_paused_dead, "download_link", {})
    TaskQueueRepository.update_status(t_paused_dead, "paused")
    active_task_controls.pop(t_paused_dead, None)
    try:
        resp = client.delete(f"/api/tasks/{t_paused_dead}", headers=auth_headers())
        assert resp.status_code == 200
        assert resp.json()["status"] == "deleted"
        assert TaskQueueRepository.get_by_id(t_paused_dead) is None
    finally:
        TaskQueueRepository.delete(t_paused_dead)


@pytest.mark.asyncio
async def test_scheduler_retry_on_network_error_and_floodwait():
    """验证调度器三分类异常协议：瞬态异常自适应指数退避重试 (缺陷 #4 根因闭环)"""
    task_id = "test_retry_backoff_task"
    TaskQueueRepository.create(task_id, "test_retry_type", {}, max_retries=3)
    TaskQueueRepository.update_status(task_id, "running")

    # 注册一个抛出 RetryableTaskError 的执行器
    async def mock_fail_exec(t_id, params, acc_id):
        raise RetryableTaskError("网络连接瞬时超时 (Connection reset)")

    task_scheduler.register_executor("test_retry_type", mock_fail_exec)

    try:
        await task_scheduler._run_task_wrapper(
            task_id=task_id, task_type="test_retry_type", params={}, account_id=0, retry_count=0, max_retries=3
        )

        t = TaskQueueRepository.get_by_id(task_id)
        assert t["status"] == "queued", "可重试异常必须将任务重新置为 queued"
        assert t["retry_count"] == 1, "重试计数必须自增为 1"
        assert t["next_retry_at"] > time.time(), "必须设置未来的 next_retry_at 退避时间戳"
        retry_logs, _, _ = TaskQueueRepository.get_task_logs(task_id, 0, 200)
        assert any("重试" in l["message"] for l in retry_logs)
    finally:
        TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_download_floodwait_aborts_loop_to_scheduler():
    """验证下载遇到 FloodWaitError 立即中断循环向外抛出，绝不继续遍历后续消息 (缺陷 #4 加固)"""
    from app.services.download_runner import run_download_range_task
    from app.services.forward_service import ForwardService

    task_id = "test_fwd_wait_abort"
    TaskQueueRepository.create(task_id, "download_range", {"channel": "chan_wait", "start_id": 1, "end_id": 10})
    TaskQueueRepository.update_status(task_id, "running")

    mock_client = MagicMock()
    mock_get_msgs = AsyncMock(side_effect=FloodWaitError(request=None))
    mock_get_msgs.seconds = 30
    mock_client.get_messages = mock_get_msgs

    # 模拟在解析实体后获取消息抛出 FloodWait
    with patch.object(ForwardService, "resolve_entity", AsyncMock(return_value="dummy_entity")):
        with patch.object(client_manager, "get_client_for_task", AsyncMock(return_value=mock_client)):
            with pytest.raises(FloodWaitError):
                await run_download_range_task(task_id, "chan_wait", 1, 10)

            # 验证仅调用了 1 次就立即逃逸，绝无继续轰炸消息 #2..#10
            assert mock_client.get_messages.call_count == 1
    TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_scheduler_does_not_resurrect_failed_tasks():
    """验证底层已置为 failed 的终态任务，调度器绝不通过泛型异常将其重置为 queued 重试 (缺陷 #4 加固)"""
    task_id = "test_no_resurrect_task"
    TaskQueueRepository.create(task_id, "test_fatal_type", {}, max_retries=3)
    TaskQueueRepository.update_status(task_id, "running")

    async def mock_fatal_worker(t_id, params, acc_id):
        # 模拟业务 Worker (如发布/转发) 认定不可恢复错误，主动标记 failed
        TaskQueueRepository.update_status(t_id, "failed")
        TaskQueueRepository.append_log(t_id, "❌ 频道被封禁或无权访问")
        raise RuntimeError("Fatal unrecoverable channel error")

    task_scheduler.register_executor("test_fatal_type", mock_fatal_worker)

    try:
        await task_scheduler._run_task_wrapper(
            task_id=task_id, task_type="test_fatal_type", params={}, account_id=0, retry_count=0, max_retries=3
        )

        t = TaskQueueRepository.get_by_id(task_id)
        assert t["status"] == "failed", "底层已标记 failed 的任务必须保持 failed，绝不能被重置为 queued"
        assert t["retry_count"] == 0, "不应消耗或递增重试次数"
    finally:
        TaskQueueRepository.delete(task_id)


def test_active_task_count_zero_authority_when_running():
    """验证当调度器处于运行状态时，权威计数值 0 严格有效，杜绝回退到 legacy 假忙碌 (缺陷 #6 加固)"""
    acc = TelegramAccountClient(
        account_id=988,
        account_name="AuthorityTestBot",
        phone="+1988",
        session_file="dummy.session",
        api_id=111,
        api_hash="hash111",
    )

    # 1. 当调度器在运行中且记账为 0
    task_scheduler._is_running = True
    task_scheduler._account_task_counts[988] = 0
    try:
        assert acc.active_task_count == 0, "调度器运行态下，权威记账 0 必须生效"
    finally:
        task_scheduler._is_running = False

    # 2. 当调度器未在运行中 (离线/单测环境): 无任务可存在, 恒 0 (pkg 1.4 删 legacy 计数)
    assert acc.active_task_count == 0


def test_delete_task_atomic_toctou_conflict():
    """验证删除接口防范 TOCTOU 竞态：任务在校验后被调度器抢占为 running 时原子拒绝 (409) (缺陷 #8 加固)"""
    task_id = "test_toctou_del"
    TaskQueueRepository.create(task_id, "download_link", {})
    TaskQueueRepository.update_status(task_id, "paused")

    # 模拟在 API 读取状态后，但在 SQL 执行前，调度器抢占为 running
    orig_delete = TaskQueueRepository.delete

    def racing_delete(t_id, allowed_statuses=None):
        # 模拟并发抢占
        TaskQueueRepository.update_status(t_id, "running")
        return orig_delete(t_id, allowed_statuses=allowed_statuses)

    with patch.object(TaskQueueRepository, "delete", side_effect=racing_delete):
        resp = client.delete(f"/api/tasks/{task_id}", headers=auth_headers())
        assert resp.status_code == 409, f"并发抢占为 running 时必须返回 409 Conflict，实际为: {resp.status_code}"
        assert "任务已被调度器认领运行" in resp.json()["detail"]

    # 清理
    orig_delete(task_id)


@pytest.mark.asyncio
async def test_schedule_unassigned_tasks_skip_saturated_accounts():
    """验证未绑定账号的任务在调度时能自动绕开本轮已饱和账号，分派给空闲账号 (缺陷 #9 加固)"""
    acc_full = TelegramAccountClient(
        account_id=971, account_name="FullBot", phone="+1971", session_file="f.session", api_id=101, api_hash="h101"
    )
    acc_full.max_concurrency = 1
    acc_full.auth_state = "AUTHENTICATED"
    acc_full.client = MagicMock()
    acc_full.client.is_connected.return_value = True

    acc_free = TelegramAccountClient(
        account_id=972, account_name="FreeBot", phone="+1972", session_file="fr.session", api_id=102, api_hash="h102"
    )
    acc_free.max_concurrency = 1
    acc_free.auth_state = "AUTHENTICATED"
    acc_free.client = MagicMock()
    acc_free.client.is_connected.return_value = True

    client_manager._accounts[971] = acc_full
    client_manager._accounts[972] = acc_free

    # 模拟调度器处于运行态且 FullBot 已占满并发
    task_scheduler._is_running = True
    task_scheduler._account_task_counts[971] = 1

    # 提交一个未指定 account_id 的任务
    t_unassigned = "test_unassigned_prio"
    TaskQueueRepository.create(t_unassigned, "test_skip_type", {}, account_id=None, priority=20)

    assigned_acc_ids = []

    async def mock_exec(t_id, params, acc_id):
        assigned_acc_ids.append(acc_id)
        await asyncio.sleep(5)

    task_scheduler.register_executor("test_skip_type", mock_exec)

    try:
        newly_scheduled = await task_scheduler.schedule_once()
        assert newly_scheduled == 1, "未指定账号的任务应成功调度给空闲账号"
        assert assigned_acc_ids == [972], f"应自动绕开已饱和账号 971，分配给空闲账号 972，实际为: {assigned_acc_ids}"
    finally:
        task_scheduler._is_running = False
        task_coro = task_scheduler._running_tasks.pop(t_unassigned, None)
        if task_coro:
            task_coro.cancel()
        task_scheduler._account_task_counts.clear()
        client_manager._accounts.pop(971, None)
        client_manager._accounts.pop(972, None)
        TaskQueueRepository.delete(t_unassigned)


def test_db_cursor_rolls_back_immediately_on_exception():
    AccountRepository.create("acc_rb_before", "13800000002", "sess_rb_before.session", 12345, "hash_rb1")

    conn = get_db()

    def _insert_and_boom() -> None:
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "INSERT INTO tg_accounts "
                "(account_name, phone, session_file, api_id, api_hash, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, 1.0, 1.0)",
                ("acc_rb_should_fail", "13800000003", "sess_rb_fail.session", 12345, "hash_rb2"),
            )
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        _insert_and_boom()

    assert conn.in_transaction is False
    assert conn.execute("SELECT COUNT(*) FROM tg_accounts WHERE account_name = 'acc_rb_should_fail'").fetchone()[0] == 0


def test_db_immediate_tx_rolls_back_on_base_exception():
    class _AbortSignal(BaseException):
        pass

    conn = get_db()

    def _run_aborting_tx() -> None:
        with db_immediate_tx() as cursor:
            cursor.execute(
                "INSERT INTO channel_presets (name, channel_id, created_at) VALUES (?, ?, ?)",
                ("p_abort", "-100992", time.time()),
            )
            raise _AbortSignal()

    with pytest.raises(_AbortSignal):
        _run_aborting_tx()

    assert conn.in_transaction is False
    with db_cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM channel_presets WHERE channel_id = '-100992'")
        assert cursor.fetchone()[0] == 0
