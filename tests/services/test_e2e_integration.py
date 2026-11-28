"""端到端集成压测与并发故障转移测试集 (E2E Integration & Stress Verification)

覆盖 4 大核心极限协同场景：
1. 多 Userbot 账号并发压测 (全局并发 limit & 单账号配额 & 任务出队与资源释放)
2. 单账号 FloodWait 熔断与隔离故障转移 (420 限流触发 -> 账号锁定倒计时 -> 另一账号无缝接管)
3. 开机孤儿自愈与三类断点无缝续传 (模拟异常停机 -> recover_orphaned_tasks -> .part/.upload.meta/账本续传)
4. 动态 Bot 池轮询负载均衡与毫秒级 Ping 探活 (4-Bot 轮询 Token 分摊 -> ping_bots_detailed 毫秒级延迟与故障隔离)
"""

import asyncio
import json
import os
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from _async_wait import FakeClock
from _safeio import safe_write_bytes
from telethon.errors import FloodWaitError

from app.core.client_manager import TelegramAccountClient, TelegramBotClient, client_manager
from app.core.scheduler import TaskScheduler
from app.db.account_repository import (
    BotRepository,
    SystemSettingsRepository,
)
from app.db.forward_repository import (
    ForwardLedgerRepository,
)
from app.db.task_repository import TaskQueueRepository
from app.services.forward_service import ForwardService


# ==============================================================================
# 场景 1: 多 Userbot 账号并发压测 (Concurrent Stress & Quota Allocation)
# ==============================================================================
@pytest.mark.asyncio
async def test_e2e_multi_userbot_concurrency_stress():
    """测试多 Userbot 账号在全局并发约束与单账号配额限制下的严格出队顺序与资源自动回收"""
    scheduler = TaskScheduler(check_interval=0.05)
    SystemSettingsRepository.set("max_global_concurrency", 3)

    # 1. 注册 2 个活跃 Userbot:
    # Account 101: 并发配额 2
    # Account 102: 并发配额 1
    acc1 = TelegramAccountClient(
        account_id=101,
        account_name="Userbot-Alpha",
        phone="+12025550101",
        session_file="session_101",
        api_id=111111,
        api_hash="hash101",
        max_concurrency=2,
        is_active=True,
        is_default=True,
        dc_id=2,
    )
    mock_client1 = AsyncMock()
    mock_client1.is_connected = MagicMock(return_value=True)
    acc1.client = mock_client1
    acc1.auth_state = "AUTHENTICATED"

    acc2 = TelegramAccountClient(
        account_id=102,
        account_name="Userbot-Beta",
        phone="+12025550102",
        session_file="session_102",
        api_id=222222,
        api_hash="hash102",
        max_concurrency=1,
        is_active=True,
        is_default=False,
        dc_id=4,
    )
    mock_client2 = AsyncMock()
    mock_client2.is_connected = MagicMock(return_value=True)
    acc2.client = mock_client2
    acc2.auth_state = "AUTHENTICATED"

    client_manager._accounts = {101: acc1, 102: acc2}

    # 2. 构造压测任务执行器并记录瞬时指标
    running_concurrency_history = []
    account_load_history = {101: [], 102: []}
    completed_tasks = []

    async def stress_task_handler(task_id: str, params: dict, account_id: int):
        cur_global = len(scheduler._running_tasks)
        running_concurrency_history.append(cur_global)
        if account_id in account_load_history:
            account_load_history[account_id].append(scheduler._account_task_counts.get(account_id, 0))

        # 模拟不同任务执行耗时
        sleep_time = float(params.get("duration", 0.15))
        await asyncio.sleep(sleep_time)

        completed_tasks.append((task_id, account_id))
        TaskQueueRepository.update_status(task_id, "completed")

    scheduler.register_executor("stress_task", stress_task_handler)

    # 3. 提交 5 个并发任务 (总数超过全局上限 3，且两账号最大总并发 2+1=3)
    t_ids = [f"stress_t_{i}" for i in range(1, 6)]
    for idx, tid in enumerate(t_ids, start=1):
        TaskQueueRepository.create(tid, "stress_task", {"duration": 0.15}, priority=100 - idx * 10, status="queued")

    # 4. 启动调度器压测
    await scheduler.start()

    # 等待全部任务执行出队完成
    for _ in range(60):
        if len(completed_tasks) == 5:
            break
        await asyncio.sleep(0.05)

    await scheduler.stop()

    # 5. 断言压测指标
    assert len(completed_tasks) == 5, f"预期 5 个任务完成，实际仅完成 {len(completed_tasks)}"

    # 验证瞬时全局并发从未超过 3
    assert all(c <= 3 for c in running_concurrency_history), f"全局并发超限: {running_concurrency_history}"

    # 验证 Account 101 并发从未超过 2
    assert all(c <= 2 for c in account_load_history[101]), f"Account 101 并发超限: {account_load_history[101]}"

    # 验证 Account 102 并发从未超过 1
    assert all(c <= 1 for c in account_load_history[102]), f"Account 102 并发超限: {account_load_history[102]}"

    # 验证最终资源彻底释放回收
    assert len(scheduler._running_tasks) == 0
    assert scheduler._account_task_counts.get(101, 0) == 0
    assert scheduler._account_task_counts.get(102, 0) == 0


# ==============================================================================
# 场景 2: 单账号 FloodWait 熔断与隔离故障转移 (FloodWait Isolation & Failover)
# ==============================================================================
@pytest.mark.asyncio
async def test_e2e_floodwait_circuit_breaker_isolation():
    """测试当某 Userbot 触发 Telegram 420 限速时，仅熔断锁定该账号，备用账号无缝接管后续调度"""
    scheduler = TaskScheduler(check_interval=0.05)
    SystemSettingsRepository.set("max_global_concurrency", 2)

    # 1. 注册 Account A 与 Account B
    acc_a = TelegramAccountClient(
        account_id=201,
        account_name="Userbot-Flood",
        phone="+12025550103",
        session_file="session_201",
        api_id=201111,
        api_hash="hash201",
        max_concurrency=2,
        is_active=True,
        is_default=True,
    )
    mock_client_a = AsyncMock()
    mock_client_a.is_connected = MagicMock(return_value=True)
    acc_a.client = mock_client_a
    acc_a.auth_state = "AUTHENTICATED"

    acc_b = TelegramAccountClient(
        account_id=202,
        account_name="Userbot-Backup",
        phone="+12025550104",
        session_file="session_202",
        api_id=202222,
        api_hash="hash202",
        max_concurrency=2,
        is_active=True,
        is_default=False,
    )
    mock_client_b = AsyncMock()
    mock_client_b.is_connected = MagicMock(return_value=True)
    acc_b.client = mock_client_b
    acc_b.auth_state = "AUTHENTICATED"

    client_manager._accounts = {201: acc_a, 202: acc_b}

    # 2. 模拟执行器：Account A 执行时抛出 FloodWaitError(30s)，Account B 执行时成功
    acc_executions = []

    async def resilient_handler(task_id: str, params: dict, account_id: int):
        acc_executions.append((task_id, account_id))
        if account_id == 201:
            err = FloodWaitError(request=None)
            err.seconds = 30
            raise err
        await asyncio.sleep(0.05)
        TaskQueueRepository.update_status(task_id, "completed")

    scheduler.register_executor("resilient_task", resilient_handler)

    # 3. 提交第一个任务 (默认分配主账号 Account A)
    TaskQueueRepository.create("task_flood_hit", "resilient_task", {}, priority=10, status="queued")

    await scheduler.start()

    # 等待第一个任务触发异常并熔断
    for _ in range(30):
        if acc_a.is_flood_locked():
            break
        await asyncio.sleep(0.05)

    # 4. 验证 Account A 已经被熔断锁定
    assert acc_a.is_flood_locked() is True
    assert acc_a.flood_wait_until > time.time()
    assert (acc_a.flood_wait_until - time.time()) <= 31

    # 5. 提交第二个任务，验证调度器跳过 Account A 并自动由 Account B 接管
    TaskQueueRepository.create("task_healthy_run", "resilient_task", {}, priority=20, status="queued")
    scheduler.notify_new_task()

    for _ in range(30):
        t_healthy = TaskQueueRepository.get_by_id("task_healthy_run")
        if t_healthy and t_healthy["status"] == "completed":
            break
        await asyncio.sleep(0.05)

    await scheduler.stop()

    # 6. 断言：任务 2 成功由 Account B (202) 执行完成
    t2_record = TaskQueueRepository.get_by_id("task_healthy_run")
    assert t2_record is not None
    assert t2_record["status"] == "completed"
    assert t2_record["account_id"] == 202

    # 验证执行链路记录
    assert ("task_flood_hit", 201) in acc_executions
    assert ("task_healthy_run", 202) in acc_executions


# ==============================================================================
# 场景 3: 开机孤儿自愈与三类断点无缝续传 (Startup Self-Healing & Tri-Mode Resume)
# ==============================================================================
@pytest.mark.asyncio
async def test_e2e_startup_orphan_self_healing_and_resume(tmp_path):
    """模拟服务异常终止，验证 recover_orphaned_tasks 准确恢复任务至 queued，且三类任务断点无缝续传"""
    # 1. 模拟三类异常终止留存任务 (运行态被强制中断)
    SystemSettingsRepository.set("auto_resume_tasks_on_startup", "true")

    # 任务 A: 下载任务 (存在 .part 与 .meta)
    part_file = tmp_path / "video_resumable.mp4.part"
    safe_write_bytes(str(part_file), b"X" * (1024 * 1024 * 5))  # 5MB 已下载
    meta_file = tmp_path / "video_resumable.mp4.meta"
    safe_write_bytes(
        str(meta_file), json.dumps({"file_size": 1024 * 1024 * 10, "completed_bytes": 1024 * 1024 * 5}).encode("utf-8")
    )

    TaskQueueRepository.create(
        "orphan_dl_e2e",
        "download_range",
        {"channel": "my_chan", "start_id": 1, "end_id": 10, "file_path": str(part_file)},
        status="running",
    )

    # 任务 B: 发布任务 (存在 .upload.meta)
    pub_video = tmp_path / "upload_target.mp4"
    safe_write_bytes(str(pub_video), b"Y" * (1024 * 1024 * 12))  # 12MB
    upload_meta = tmp_path / "upload_target.mp4.upload.meta"
    safe_write_bytes(
        str(upload_meta),
        json.dumps(
            {"total_bytes": 1024 * 1024 * 12, "file_id": 99887766, "completed_parts": [0, 1], "updated_at": time.time()}
        ).encode("utf-8"),
    )

    TaskQueueRepository.create(
        "orphan_pub_e2e",
        "publish_folder",
        {"folder_path": str(tmp_path), "target_channel": "-10012345"},
        status="running",
    )

    # 任务 C: 转发任务 (账本已记录已覆盖区间 [1, 50])
    src_chat = "-100900000005"
    tgt_chat = "-100900000031"
    ForwardLedgerRepository.commit_batch_records(
        task_id="orphan_fw_e2e",
        source_chat=src_chat,
        target_chat=tgt_chat,
        mappings=[(i, 5000 + i) for i in range(1, 51)],
        skips=[],
        seg_start=1,
        seg_end=50,
    )

    TaskQueueRepository.create(
        "orphan_fw_e2e",
        "forward_channel",
        {"source_chat": src_chat, "target_chat": tgt_chat, "start_id": 1, "end_id": 100},
        status="running",
    )

    # 2. 执行开机孤儿自愈调和
    scheduler = TaskScheduler()
    scheduler.recover_orphaned_tasks()

    # 3. 验证状态自愈转置为 queued 并附带特征断点日志
    t_dl = TaskQueueRepository.get_by_id("orphan_dl_e2e")
    t_pub = TaskQueueRepository.get_by_id("orphan_pub_e2e")
    t_fw = TaskQueueRepository.get_by_id("orphan_fw_e2e")

    assert t_dl["status"] == "queued"
    assert t_pub["status"] == "queued"
    assert t_fw["status"] == "queued"

    dl_logs = [l["message"] for l in TaskQueueRepository.get_task_logs("orphan_dl_e2e", 0, 500)[0]]
    pub_logs = [l["message"] for l in TaskQueueRepository.get_task_logs("orphan_pub_e2e", 0, 500)[0]]
    fw_logs = [l["message"] for l in TaskQueueRepository.get_task_logs("orphan_fw_e2e", 0, 500)[0]]

    assert any(".part/.meta" in m for m in dl_logs), "下载任务未给出正确的 .part/.meta 续传提示"
    assert any("已发布文件已记录历史" in m for m in pub_logs), "发布任务未给出正确的历史发布跳过提示"
    assert any("转发账本已记录进度" in m for m in fw_logs), "转发任务未给出正确的账本续转提示"

    # 4. 验证断点续传业务逻辑准确生效
    # 4.1 下载断点验证：.part 与 .meta 文件完整保留
    assert os.path.exists(str(part_file))
    assert os.path.getsize(str(part_file)) == 1024 * 1024 * 5
    assert os.path.exists(str(meta_file))

    # 4.2 转发断点验证：自动计算出未覆盖 gap 起点为 51
    resume_start, mode = ForwardService.compute_resume_start(src_chat, tgt_chat, 1, 100)
    assert mode == "gap"
    assert resume_start == 51, f"预期从 51 开始续转，实际返回 {resume_start}"

    # 4.3 发布断点验证：.upload.meta 文件存在且有效
    assert os.path.exists(str(upload_meta))
    meta_content = json.loads(upload_meta.read_text(encoding="utf-8"))
    assert meta_content["file_id"] == 99887766
    assert meta_content["completed_parts"] == [0, 1]


# ==============================================================================
# 场景 4: 动态 Bot 池轮询负载均衡与毫秒级 Ping 探活 (Bot Pool Load-Balancing & Latency)
# ==============================================================================
def _install_ping_mocks(mock_clients: dict, id1: int, id2: int, id3: int, clock: FakeClock) -> None:
    """Attach deterministic FakeClock latency probes to 3 mock Bot clients."""

    async def fake_get_me_1():
        clock.advance(0.020)
        return MagicMock(id=1001, bot=True, username="bot_1_user")

    async def fake_get_me_2():
        clock.advance(0.040)
        return MagicMock(id=1002, bot=True, username="bot_2_user")

    async def fake_get_me_3():
        clock.advance(0.010)
        raise RuntimeError("Telegram Gateway Timeout (simulated)")

    mock_clients[id1].get_me = AsyncMock(side_effect=fake_get_me_1)
    mock_clients[id2].get_me = AsyncMock(side_effect=fake_get_me_2)
    mock_clients[id3].get_me = AsyncMock(side_effect=fake_get_me_3)


@pytest.mark.asyncio
async def test_e2e_bot_pool_round_robin_and_ping_health(monkeypatch):
    """测试动态 Bot 池的 Token 轮询分摊稳定性与批量毫秒级 Ping 延迟探活与故障隔离"""
    # 1. 注册 3 个 Bot 客户端
    bots = {}
    mock_clients = {}

    created_ids = []
    for b_num, dc in [(1, 2), (2, 4), (3, 5)]:
        bot_record = BotRepository.create(
            bot_token=f"{1000 + b_num}:test",
            bot_name=f"Bot_e2e_{b_num}",
            session_name=f"e2e_bot_pool_{b_num}",
            api_id=999999,
            api_hash="hash999",
            is_active=1,
        )
        b_id = bot_record["id"]
        created_ids.append(b_id)

        mock_c = AsyncMock()
        mock_c.is_connected = MagicMock(return_value=True)
        mock_c.session = MagicMock(dc_id=dc)

        bot_client = TelegramBotClient(
            bot_id=b_id,
            bot_token=bot_record["bot_token"],
            session_name=bot_record["session_name"],
            api_id=bot_record["api_id"],
            api_hash=bot_record["api_hash"],
            is_active=True,
            bot_name=f"Bot_{b_num}",
            dc_id=dc,
        )
        bot_client.client = mock_c
        bot_client.status = "ready"
        bots[b_id] = bot_client
        mock_clients[b_id] = mock_c

    client_manager._bots = bots
    client_manager._bot_index = 0

    id1, id2, id3 = created_ids

    # 2. 验证 6 次连续调用呈现严格环形轮询 (id2 -> id3 -> id1 -> id2 -> id3 -> id1)
    polled_bots = []
    for _ in range(6):
        c = client_manager.get_next_available_bot()
        polled_bots.append(c)

    expected_sequence = [
        mock_clients[id2],
        mock_clients[id3],
        mock_clients[id1],
        mock_clients[id2],
        mock_clients[id3],
        mock_clients[id1],
    ]
    assert polled_bots == expected_sequence, "Bot 池轮询顺序不符合严格环形分发"

    # 3. 模拟健康探测：Bot 1/2 正常返回，Bot 3 抛出网络超时（使用虚拟时钟确定性测量延迟）
    from app.core import client_manager as client_manager_mod

    clock = FakeClock()
    monkeypatch.setattr(client_manager_mod, "time", clock.time_namespace())
    _install_ping_mocks(mock_clients, id1, id2, id3, clock)

    # 4. 执行批量详细探活
    ping_results = await client_manager.ping_bots_detailed()
    assert len(ping_results) == 3

    res_map = {r["bot_id"]: r for r in ping_results}

    # Bot 1 正常
    assert res_map[id1]["online"] is True
    assert res_map[id1]["bot_username"] == "bot_1_user"
    assert res_map[id1]["dc_id"] == 2
    assert res_map[id1]["latency_ms"] is not None and res_map[id1]["latency_ms"] >= 10.0
    assert res_map[id1]["error"] is None

    # Bot 2 正常
    assert res_map[id2]["online"] is True
    assert res_map[id2]["bot_username"] == "bot_2_user"
    assert res_map[id2]["dc_id"] == 4
    assert res_map[id2]["latency_ms"] is not None and res_map[id2]["latency_ms"] >= 20.0
    assert res_map[id2]["error"] is None

    # Bot 3 故障隔离
    assert res_map[id3]["online"] is False
    assert res_map[id3]["latency_ms"] is None
    assert "Telegram Gateway Timeout" in (res_map[id3]["error"] or "")
    assert bots[id3].client is mock_clients[id3], "探活失败不得断开已有的传输连接"
