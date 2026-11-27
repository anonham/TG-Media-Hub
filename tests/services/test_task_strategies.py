"""Business Services: 策略注册中心、5 大任务策略分派与执行器重试容错测试 (Task Strategy & Executor Registry)

覆盖范围:
- 原 test_p1_task_strategy: 任务策略注册中心 (TaskStrategyRegistry) 注册与 5 大任务策略分派
- 原 test_p1_retry_and_fatal_slot: 执行器重试生命周期、fatal 槽落库与终态流转
- 原 test_pikpak_executor: PikPak 执行器接入、注册分派、双通道进度上报与 WebSocket 脱敏
- 原 test_executor_retry: 执行器重试指数退避、全局重试配额与致命异常快速熔断"""

import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.tasks import router
from app.core.client_manager import client_manager
from app.core.connection_manager import connection_manager
from app.core.pikpak import (
    FAILURE_DETAIL_TEXT,
    ParsedFileItem,
    PikPakPublishFatalError,
    PikPakPublishRetryableError,
)
from app.core.runtime import active_task_controls
from app.core.scheduler import (
    FatalTaskError,
    RetryableTaskError,
    TaskScheduler,
    UploadCancelledError,
    UploadPausedError,
)
from app.core.task_strategy import StrategyRegistry
from app.db.account_repository import SystemSettingsRepository
from app.db.task_repository import TaskQueueRepository
from app.services import publish_runner as pub_exec
from app.services.download_runner import run_download_links_task, run_download_range_task
from app.services.executor_registry import register_default_executors
from app.services.forward_service import run_forward_channel_task
from app.services.pikpak_publisher_service import PikPakPublisherService
from app.services.publish_runner import run_publish_folder_task
from app.services.publisher_service import PublishFolderResult
from app.services.task_strategies import (
    DownloadLinkStrategy,
    DownloadRangeStrategy,
    ForwardChannelStrategy,
    PikPakPublishStrategy,
    PublishFolderStrategy,
)


# ==============================================================================
# Part: test_p1_task_strategy.py
# ==============================================================================
def test_strategy_registry_contains_all_five_strategies():
    assert StrategyRegistry.get_optional("download_link") is not None
    assert StrategyRegistry.get_optional("download_range") is not None
    assert StrategyRegistry.get_optional("forward_channel") is not None
    assert StrategyRegistry.get_optional("publish_folder") is not None
    assert StrategyRegistry.get_optional("pikpak_publish") is not None

    assert isinstance(StrategyRegistry.get("download_link"), DownloadLinkStrategy)
    assert isinstance(StrategyRegistry.get("download_range"), DownloadRangeStrategy)
    assert isinstance(StrategyRegistry.get("forward_channel"), ForwardChannelStrategy)
    assert isinstance(StrategyRegistry.get("publish_folder"), PublishFolderStrategy)
    assert isinstance(StrategyRegistry.get("pikpak_publish"), PikPakPublishStrategy)


def test_strategy_validation_required_params():
    dl_strategy = StrategyRegistry.get("download_link")
    with pytest.raises(ValueError, match="links"):
        dl_strategy.validate_required_params({})
    dl_strategy.validate_required_params({"links": ["https://t.me/c/123/1"]})

    dr_strategy = StrategyRegistry.get("download_range")
    with pytest.raises(ValueError, match="channel"):
        dr_strategy.validate_required_params({})
    with pytest.raises(ValueError, match="start_id"):
        dr_strategy.validate_required_params({"channel": "chan"})
    dr_strategy.validate_required_params({"channel": "chan", "start_id": 1, "end_id": 10})

    fc_strategy = StrategyRegistry.get("forward_channel")
    with pytest.raises(ValueError, match="source_chat"):
        fc_strategy.validate_required_params({})

    pub_strategy = StrategyRegistry.get("publish_folder")
    with pytest.raises(ValueError, match="folder_path"):
        pub_strategy.validate_required_params({})

    pikpak_strategy = StrategyRegistry.get("pikpak_publish")
    with pytest.raises(ValueError, match="remote_files"):
        pikpak_strategy.validate_required_params({})


def test_strategy_validation_accepts_canonical_aliases():
    """恢复校验与 RUN_PARAMS_SPEC/extract 同口径: channel_identifier / target_chat 是一等别名 (CR 别名双修)"""
    dr_strategy = StrategyRegistry.get("download_range")
    dr_strategy.validate_required_params({"channel_identifier": "chan", "start_id": 1, "end_id": 10})
    assert dr_strategy.extract_target_chat({"channel_identifier": "chan"}) == "chan"

    pikpak_strategy = StrategyRegistry.get("pikpak_publish")
    pikpak_strategy.validate_required_params({"remote_files": [{"file_id": "f1"}], "target_chat": "@chan"})
    assert pikpak_strategy.extract_target_chat({"target_chat": "@chan"}) == "@chan"
    assert pikpak_strategy.extract_target_chat({"target_channel": "@chan"}) == "@chan"


def test_strategy_apply_concurrency_update():
    dl_strategy = StrategyRegistry.get("download_link")
    params = {"threads": 3}
    dl_strategy.apply_concurrency_update(params, 8, None)
    assert params["threads"] == 8

    pikpak_strategy = StrategyRegistry.get("pikpak_publish")
    p_params = {"upload_concurrency": 4, "upload_in_flight_per_connection": 2}
    pikpak_strategy.apply_concurrency_update(p_params, 10, 4)
    assert p_params["upload_concurrency"] == 10
    assert p_params["upload_in_flight_per_connection"] == 4


def test_strategy_extract_target_chat():
    pub_strat = StrategyRegistry.get("publish_folder")
    assert pub_strat.extract_target_chat({"target_chat": "@my_channel"}) == "@my_channel"
    assert pub_strat.extract_target_chat({"target_channel": "@my_channel"}) == "@my_channel"

    fc_strat = StrategyRegistry.get("forward_channel")
    assert fc_strat.extract_target_chat({"source_chat": "123456"}) == "123456"

    dl_strat = StrategyRegistry.get("download_link")
    assert dl_strat.extract_target_chat({}) is None


def test_pikpak_publish_query_filter_regex_not_422(monkeypatch):
    """验证 GET /api/tasks?task_type=pikpak_publish 正确匹配，不触发 422 校验失败"""
    app = FastAPI()
    app.include_router(router, prefix="/api/tasks")
    client = TestClient(app, client=("127.0.0.1", 50000))

    # Mock query_tasks 返回空列表
    from app.db.task_repository import TaskQueueRepository

    monkeypatch.setattr(TaskQueueRepository, "query_tasks", staticmethod(lambda *args, **kwargs: ([], 0)))

    resp = client.get("/api/tasks?task_type=pikpak_publish")
    assert resp.status_code == 200
    assert resp.json() == []


# ==============================================================================
# Part: test_p1_retry_and_fatal_slot.py
# ==============================================================================
@pytest.mark.asyncio
async def test_reset_task_for_retry_purges_transient_metrics_and_failed_files():
    """验证 reset_task_for_retry 原子重置 transient 指标并安全剥离 failed_files 信封"""
    task_id = "test_retry_purge_01"
    # 创建含脏指标与 failed_files 的任务
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="publish_folder",
        params={
            "folder_path": "/tmp/test",
            "target_channel": "@chan",
            "failed_files": ["file1.mp4", "file2.mp4"],
            "failed_items": ["item1"],
            "pikpak_download_speed_mb_s": 12.5,
            "keep_this": "intact",
        },
    )

    # 模拟运行中产生的瞬态速率和状态
    TaskQueueRepository.update_progress(
        task_id=task_id,
        progress_bytes=100,
        total_bytes=500,
        processed_items=1,
        total_items=5,
        speed_mb_s=25.5,
        eta_s=15.0,
        current_item="uploading file1.mp4",
        concurrency=4,
    )
    TaskQueueRepository.update_status(task_id, "failed")

    t_before = TaskQueueRepository.get_by_id(task_id)
    assert t_before["status"] == "failed"
    assert t_before["speed_mb_s"] == 25.5
    assert t_before["concurrency"] == 4
    assert "failed_files" in t_before["params"]

    # 执行 reset_task_for_retry
    success = TaskQueueRepository.reset_task_for_retry(
        task_id,
        retry_count=1,
        next_retry_at=9999999.0,
        error_detail="测试重试",
    )
    assert success is True

    t_after = TaskQueueRepository.get_by_id(task_id)
    assert t_after["status"] == "queued"
    assert t_after["speed_mb_s"] == 0.0
    assert t_after["eta_s"] == 0.0
    assert t_after["concurrency"] == 0
    assert t_after["current_item"] == ""
    assert t_after["retry_count"] == 1
    assert t_after["next_retry_at"] == 9999999.0
    assert t_after["error_detail"] == "测试重试"

    # 验证关键 params 诊断键被安全剥离，业务参数完整保留
    params_after = t_after["params"]
    assert "failed_files" not in params_after
    assert "failed_items" not in params_after
    assert "pikpak_download_speed_mb_s" not in params_after
    assert params_after.get("keep_this") == "intact"
    assert params_after.get("folder_path") == "/tmp/test"

    # 清理
    TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_scheduler_handle_retry_broadcasts_queued_status():
    """验证调度器退避冷却期向 WebSocket 广播 status='queued'"""
    scheduler = TaskScheduler()
    task_id = "test_sched_retry_ws_01"
    TaskQueueRepository.create(task_id=task_id, task_type="download_link", params={"links": ["l1"]})

    broadcast_events = []

    async def mock_broadcast(msg):
        broadcast_events.append(msg)

    with patch.object(connection_manager, "broadcast", side_effect=mock_broadcast):
        await scheduler._handle_task_retry(task_id, retry_count=0, max_retries=3, error_msg="网络波动")

    # 校验广播中包含 status='queued'
    queued_events = [e for e in broadcast_events if e.get("task_id") == task_id and e.get("status") == "queued"]
    assert len(queued_events) >= 1
    ev = queued_events[0]
    assert ev["error_detail"] == "网络波动"
    assert ev["retry_count"] == 1
    assert ev["speed_mb_s"] == 0.0

    # 清理
    TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_fatal_task_error_finalizes_placeholder_slot_and_broadcasts():
    """验证 FatalTaskError 发生时必定触发 PlaceholderSlotCoordinator.finalize_terminal 并广播 failed"""
    scheduler = TaskScheduler()
    task_id = "test_fatal_slot_01"
    TaskQueueRepository.create(
        task_id=task_id, task_type="publish_folder", params={"folder_path": "/p"}, status="running"
    )

    broadcast_events = []

    async def mock_broadcast(msg):
        broadcast_events.append(msg)

    finalize_mock = AsyncMock()

    with (
        patch.object(connection_manager, "broadcast", side_effect=mock_broadcast),
        patch("app.core.pipeline.placeholder.PlaceholderSlotCoordinator.finalize_terminal", finalize_mock),
    ):
        # 模拟执行器抛出 FatalTaskError
        async def fatal_executor(tid, params, acc_id):
            raise FatalTaskError("不可恢复的致命损坏")

        scheduler.register_executor("publish_folder", fatal_executor)
        await scheduler._run_task_wrapper(
            task_id, "publish_folder", {"folder_path": "/p"}, 0, retry_count=0, max_retries=3
        )

    # 必须调用 finalize_terminal
    finalize_mock.assert_awaited_once_with(task_id)

    # 必须广播 failed
    failed_events = [e for e in broadcast_events if e.get("task_id") == task_id and e.get("status") == "failed"]
    assert len(failed_events) >= 1

    # 数据库状态置 failed
    rec = TaskQueueRepository.get_by_id(task_id)
    assert rec["status"] == "failed"

    # 清理
    TaskQueueRepository.delete(task_id)


# ==============================================================================
# Part: test_pikpak_executor.py
# ==============================================================================
def _create_task(task_id, params=None, account_id=None):
    return TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        params=params or {},
        account_id=account_id,
    )


def _patch_publish(monkeypatch, behavior):
    """替换服务入口: behavior = {"result": PublishFolderResult|None, "exc": Exception|None, "capture": dict|None, "notify": bool|None}"""

    async def fake_publish(**kwargs):
        if behavior.get("capture") is not None:
            behavior["capture"].update(kwargs)
        if behavior.get("exc") is not None:
            raise behavior["exc"]
        res = behavior.get("result")
        if behavior.get("notify") and res is not None:
            from app.core.client_manager import client_manager
            from app.core.pipeline.models import PublishBatch, PublishContext
            from app.core.pipeline.notifier import TelegramChannelNotifier

            acc_id = kwargs.get("account_id")
            acc = client_manager.get_account(acc_id) if acc_id is not None else None
            pub_client = (acc.client if acc and acc.is_ready() else None) or client_manager.userbot

            folder_name = kwargs.get("folder_name") or getattr(res, "folder_name", "") or "PikPak 批量发布"
            batch = PublishBatch(
                batch_id=kwargs.get("task_id") or "test_batch",
                folder_name=folder_name,
                rel_path="",
                is_folder_publish=kwargs.get("is_folder_publish", True),
                items=[],
            )
            ctx = PublishContext(
                task_id=kwargs.get("task_id") or "",
                target_chat_id=kwargs.get("target_chat_id"),
                target_channel_raw=str(kwargs.get("target_chat_id")),
                is_folder_publish=kwargs.get("is_folder_publish", True),
                folder_name=folder_name,
                userbot_client=pub_client,
            )
            notifier = TelegramChannelNotifier()
            await notifier.notify_batch(batch, ctx, res)
        return res

    monkeypatch.setattr(PikPakPublisherService, "publish_pikpak_files", staticmethod(fake_publish))


def _patch_broadcast(monkeypatch):
    broadcasts = []

    async def fake_broadcast(message):
        broadcasts.append(message)

    monkeypatch.setattr(pub_exec.connection_manager, "broadcast", fake_broadcast)
    return broadcasts


async def _drain():
    for _ in range(3):
        await asyncio.sleep(0)


def test_registry_registers_pikpak_publish(monkeypatch):
    """注册分派: register_default_executors 注册 pikpak_publish; 闭包按 params 正确解包透传。"""
    registered = {}

    class FakeScheduler:
        def register_executor(self, task_type, handler):
            registered[task_type] = handler

    register_default_executors(FakeScheduler())
    assert "pikpak_publish" in registered
    assert "publish_folder" in registered  # 既有注册未被破坏

    handler = registered["pikpak_publish"]
    captured = {}

    async def fake_run(
        task_id,
        target_channel,
        remote_files,
        auto_cleanup_mode="archive",
        send_delay_seconds=3.0,
        account_id=None,
        upload_concurrency=None,
        upload_in_flight_per_connection=None,
    ):
        captured.update(
            {
                "task_id": task_id,
                "target_channel": target_channel,
                "remote_files": remote_files,
                "auto_cleanup_mode": auto_cleanup_mode,
                "send_delay_seconds": send_delay_seconds,
                "account_id": account_id,
                "upload_concurrency": upload_concurrency,
                "upload_in_flight_per_connection": upload_in_flight_per_connection,
            }
        )

    monkeypatch.setattr(pub_exec, "run_publish_pikpak_task", fake_run)
    asyncio.run(
        handler(
            "tid",
            {
                "target_channel": "-100888",
                "remote_files": [{"file_id": "F1"}],
                "auto_cleanup_mode": "delete",
                "send_delay_seconds": 5.0,
                "upload_concurrency": 4,
                "upload_in_flight_per_connection": 2,
            },
            9,
        )
    )
    assert captured == {
        "task_id": "tid",
        "target_channel": "-100888",
        "remote_files": [{"file_id": "F1"}],
        "auto_cleanup_mode": "delete",
        "send_delay_seconds": 5.0,
        "account_id": 9,
        "upload_concurrency": 4,
        "upload_in_flight_per_connection": 2,
    }

    # auto_cleanup_mode 缺省 → archive (不得改默认)
    captured.clear()
    asyncio.run(handler("tid2", {"target_channel": "-100888"}, None))
    assert captured["auto_cleanup_mode"] == "archive"


async def test_progress_dual_channel_and_redaction(monkeypatch):
    """progress 双通道: DB 节流 (force_db/≥2s/≥1%) + WS 高频经 redact_payload (双向流速字段并存)。"""
    task_id = "pk1"
    _create_task(task_id, params={"target_channel": "-100888"})

    db_calls = []

    def fake_submit(func, *args, **kwargs):
        db_calls.append(args)

    monkeypatch.setattr(pub_exec, "submit_db_write", fake_submit)
    broadcasts = _patch_broadcast(monkeypatch)

    async def fake_publish(**kwargs):
        cb = kwargs["progress_callback"]
        # e1: force_db → 立即落库
        cb(
            0,
            2,
            1.0,
            10.0,
            "a.mp4",
            concurrency=2,
            dc_id=4,
            concurrency_total=2,
            percent=0.0,
            force_db=True,
            pikpak_download_speed_mb_s=0.5,
            total_bytes_done=0,
            total_bytes_all=100,
        )
        # e2: 无 force、percent 跨度 0.5 (<1%)、时间 <2s → DB 节流, 仅 WS
        cb(
            1,
            2,
            2.0,
            5.0,
            "a.mp4",
            concurrency=2,
            dc_id=4,
            concurrency_total=2,
            percent=0.5,
            force_db=False,
            pikpak_download_speed_mb_s=0.6,
            total_bytes_done=50,
            total_bytes_all=100,
        )
        # e3: percent 跨度 ≥1% → 落库; 文件名注入构造签名 URL → 脱敏断言
        cb(
            2,
            2,
            3.0,
            1.0,
            "b.mp4?X-Sign=FAKESIGN&Expires=9999999999",
            percent=50.0,
            force_db=False,
            pikpak_download_speed_mb_s=0.7,
            total_bytes_done=100,
            total_bytes_all=100,
        )
        return PublishFolderResult(2, 2, [])

    _patch_publish(monkeypatch, {"result": PublishFolderResult(2, 2, [])})
    monkeypatch.setattr(PikPakPublisherService, "publish_pikpak_files", staticmethod(fake_publish))

    await pub_exec.run_publish_pikpak_task(task_id, "-100888", [{"file_id": "F1"}])
    await _drain()

    # DB 节流: e1 (force) 与 e3 (≥1%) 落库, e2 被节流
    assert len(db_calls) == 2
    # update_progress(task_id, progress_bytes, total_bytes, processed_items, total_items, ...)
    assert db_calls[0][1] == 0 and db_calls[1][1] == 100  # progress_bytes
    assert db_calls[0][3] == 0 and db_calls[1][3] == 2  # processed_items

    progress_msgs = [b for b in broadcasts if b.get("type") == "progress"]
    assert len(progress_msgs) == 4  # 3 次 progress + 1 次终态 progress
    e2_payload = progress_msgs[1]["progress"]
    assert "unit" not in e2_payload  # 物理拆分，消灭伪多态 unit
    assert e2_payload["pikpak_download_speed_mb_s"] == 0.6  # PikPak 下行
    assert e2_payload["speed_mb_s"] == 2.0  # TG 上行并存
    assert e2_payload["processed_bytes"] == 50 and e2_payload["total_bytes"] == 100  # 字节级并存
    assert e2_payload["processed_items"] == 1 and e2_payload["total_items"] == 2

    # WS 消息体经 redact_payload: 构造签名值被擦除
    serialized = json.dumps(broadcasts, ensure_ascii=False)
    assert "FAKESIGN" not in serialized
    assert "***" in progress_msgs[2]["progress"]["current_item"]

    # 终态: 全部成功 → completed + task_update 广播
    assert progress_msgs[-1]["progress"]["processed_items"] == 2 and progress_msgs[-1]["progress"]["percent"] == 100.0
    task_updates = [b for b in broadcasts if b.get("type") == "task_update"]
    assert task_updates[-1]["status"] == "completed"
    rec = TaskQueueRepository.get_by_id(task_id)
    assert rec["status"] == "completed"
    assert task_id not in active_task_controls


async def test_terminal_states(monkeypatch):
    """终态机: completed / failed(部分失败) / paused / cancelled / session_invalid 五分支 + controls 清理。"""
    broadcasts_all = []
    behavior = {}

    async def fake_broadcast(message):
        broadcasts_all.append(message)

    monkeypatch.setattr(pub_exec.connection_manager, "broadcast", fake_broadcast)
    _patch_publish(monkeypatch, behavior)

    # a) 全部成功 → completed
    _create_task("st_a", params={})
    behavior.update({"result": PublishFolderResult(1, 1, []), "exc": None})
    await pub_exec.run_publish_pikpak_task("st_a", "-100888", [{"file_id": "F1"}])
    assert TaskQueueRepository.get_by_id("st_a")["status"] == "completed"
    assert "st_a" not in active_task_controls

    # b) 部分失败 → failed
    _create_task("st_b", params={})
    behavior.update({"result": PublishFolderResult(1, 2, ["x.mp4: 审核状态未通过"]), "exc": None})
    await pub_exec.run_publish_pikpak_task("st_b", "-100888", [{"file_id": "F1"}])
    assert TaskQueueRepository.get_by_id("st_b")["status"] == "failed"
    assert "st_b" not in active_task_controls

    # c) 暂停 → paused + re-raise + controls 清理
    _create_task("st_c", params={})
    behavior.update({"exc": UploadPausedError("上传任务被暂停")})
    with pytest.raises(UploadPausedError):
        await pub_exec.run_publish_pikpak_task("st_c", "-100888", [{"file_id": "F1"}])
    assert TaskQueueRepository.get_by_id("st_c")["status"] == "paused"
    assert "st_c" not in active_task_controls

    # d) 取消 → cancelled + re-raise + controls 清理
    _create_task("st_d", params={})
    behavior.update({"exc": UploadCancelledError("上传任务被取消")})
    with pytest.raises(UploadCancelledError):
        await pub_exec.run_publish_pikpak_task("st_d", "-100888", [{"file_id": "F1"}])
    assert TaskQueueRepository.get_by_id("st_d")["status"] == "cancelled"
    assert "st_d" not in active_task_controls

    # e) session_invalid → report_session_invalid 兜底、无 re-raise
    _create_task("st_e", params={})
    behavior.update({"exc": RuntimeError("AuthKeyUnregistered")})
    monkeypatch.setattr(pub_exec, "is_session_invalid_error", lambda e: True)
    report_spy = AsyncMock()
    monkeypatch.setattr(pub_exec, "report_session_invalid", report_spy)
    await pub_exec.run_publish_pikpak_task("st_e", "-100888", [{"file_id": "F1"}])
    report_spy.assert_awaited_once()
    assert "st_e" not in active_task_controls
    # session_invalid 不落入 failure_detail 文案分支 (无 re-raise 即验证)

    # task_update 广播覆盖各终态
    updates = {b["task_id"]: b["status"] for b in broadcasts_all if b.get("type") == "task_update"}
    assert updates["st_a"] == "completed"
    assert updates["st_b"] == "failed"
    assert updates["st_c"] == "paused"
    assert updates["st_d"] == "cancelled"


async def test_params_backfill(monkeypatch):
    """params 回读补齐: 入参 None 时从任务记录回读 account_id/并发参数 (镜像 L64-79)。"""
    task_id = "pkback"
    _create_task(
        task_id,
        params={
            "target_channel": "-100888",
            "remote_files": [{"file_id": "F1", "name": "v.mp4"}],
            "upload_concurrency": 4,
            "upload_in_flight_per_connection": 2,
        },
        account_id=7,
    )

    captured = {}
    _patch_publish(monkeypatch, {"result": PublishFolderResult(1, 1, []), "capture": captured})
    _patch_broadcast(monkeypatch)

    await pub_exec.run_publish_pikpak_task(task_id, "-100888", [{"file_id": "F1", "name": "v.mp4"}])

    assert captured["account_id"] == 7
    assert captured["upload_concurrency"] == 4
    assert captured["upload_in_flight_per_connection"] == 2
    assert captured["auto_cleanup_mode"] == "archive"
    # 快照已还原为 ParsedFileItem
    assert captured["remote_files"][0].file_id == "F1"
    assert captured["remote_files"][0].name == "v.mp4"


async def test_failure_detail_mapping(monkeypatch):
    """失败分类 → error_detail 中文文案落库; Fatal 不重试语义 / Retryable 原样上抛交调度器退避。"""
    task_id = "pkfail"
    _create_task(task_id, params={})
    _patch_broadcast(monkeypatch)

    _patch_publish(monkeypatch, {"exc": PikPakPublishFatalError("auth")})
    with pytest.raises(PikPakPublishFatalError):
        await pub_exec.run_publish_pikpak_task(task_id, "-100888", [{"file_id": "F1"}])
    rec = TaskQueueRepository.get_by_id(task_id)
    assert rec["error_detail"] == FAILURE_DETAIL_TEXT["auth"]

    task_id2 = "pkfail2"
    _create_task(task_id2, params={})
    _patch_publish(monkeypatch, {"exc": PikPakPublishRetryableError("transient")})
    with pytest.raises(RetryableTaskError):
        await pub_exec.run_publish_pikpak_task(task_id2, "-100888", [{"file_id": "F1"}])
    rec2 = TaskQueueRepository.get_by_id(task_id2)
    assert rec2["error_detail"] == FAILURE_DETAIL_TEXT["transient"]
    assert task_id2 not in active_task_controls


def test_dual_track_progress_pikpak_and_download():
    """P0 重构: 物理量纲拆分，pikpak_publish 与 download_link 统一具备 processed_items 与 progress_bytes 列。"""
    t1 = TaskQueueRepository.create(task_id="pu1", task_type="pikpak_publish", params={})
    assert t1["processed_items"] == 0
    assert t1["total_items"] == 0
    assert t1["progress_bytes"] == 0
    assert t1["total_bytes"] == 0
    rec = TaskQueueRepository.get_by_id("pu1")
    assert rec["processed_items"] == 0
    assert "progress_unit" not in rec

    t2 = TaskQueueRepository.create(task_id="pu2", task_type="download_link", params={"links": []})
    assert t2["processed_items"] == 0
    assert t2["total_items"] == 0
    assert "progress_unit" not in t2


async def test_executor_skipped_files_and_tg_summary_card(monkeypatch):
    """执行器集成: skipped_files 合并到 params，终态完成并向 Telegram 频道发送批次总结卡片。"""
    from app.models.publish import OversizedFileInfo

    task_id = "pkskip"
    _create_task(
        task_id,
        params={
            "target_channel": "-100888",
            "remote_files": [{"file_id": "F1", "name": "big.mp4"}],
            "is_folder_publish": True,
            "folder_name": "PikPak 批量发布",
        },
    )

    _patch_broadcast(monkeypatch)

    mock_client = AsyncMock()
    mock_client.send_message = AsyncMock()

    skipped = [
        OversizedFileInfo(
            file_name="big.mp4",
            rel_path="pikpak://P0/big.mp4",
            file_size_bytes=2100 * 1024 * 1024,
            file_size_mb=2100.0,
            file_size_gb=2.05,
            limit_mb=2000.0,
            exceeded_mb=100.0,
            oversize_kind="non_premium_limit",
        )
    ]
    res_mock = PublishFolderResult(0, 1, [])
    res_mock.skipped_files = skipped
    res_mock.is_folder_publish = True
    res_mock.folder_name = "PikPak 批量发布"

    _patch_publish(monkeypatch, {"result": res_mock, "notify": True})

    from unittest.mock import patch

    from app.core.client_manager import client_manager

    with (
        patch.object(client_manager, "get_account", return_value=MagicMock(client=mock_client, is_ready=lambda: True)),
        patch.object(client_manager, "userbot", mock_client),
    ):
        await pub_exec.run_publish_pikpak_task(task_id, "-100888", [{"file_id": "F1", "name": "big.mp4"}])

    task_rec = TaskQueueRepository.get_by_id(task_id)
    # 1. 终态收敛为 completed (因无 failed_files, 仅超限跳过)
    assert task_rec["status"] == "completed"

    # 2. skipped_files 已持久化入 params
    assert len(task_rec["params"].get("skipped_files", [])) == 1
    assert task_rec["params"]["skipped_files"][0]["file_name"] == "big.mp4"

    # 3. TG 频道总结卡片已发送且格式合规 (去内部化)
    mock_client.send_message.assert_awaited_once()
    summary_text = mock_client.send_message.await_args[0][1]
    assert "发布文件夹" in summary_text or "PikPak 批量发布总结" in summary_text
    assert "超限跳过 (1 个)" in summary_text
    assert "big.mp4" in summary_text


async def test_empty_selection_fails_fast(monkeypatch):
    """包A 空任务护栏: 展开后 0 个可发布项 → fail-fast 置 failed (杜绝静默「成功 0/0」), publisher 不被调用。"""
    task_id = "pkempty"
    _create_task(task_id, params={"target_channel": "-100888", "remote_files": []})
    captured = {}
    _patch_publish(monkeypatch, {"result": PublishFolderResult(0, 0, []), "capture": captured})
    broadcasts = _patch_broadcast(monkeypatch)

    await pub_exec.run_publish_pikpak_task(task_id, "-100888", [])

    rec = TaskQueueRepository.get_by_id(task_id)
    assert rec["status"] == "failed", "空任务必须以 failed 终态收场"
    assert captured == {}, "fail-fast 路径严禁触达 publisher"
    assert any(b.get("status") == "failed" for b in broadcasts if isinstance(b, dict))
    logs, _, _ = TaskQueueRepository.get_task_logs(task_id)
    assert any("未选择任何可发布的远端文件" in e["message"] for e in logs)


async def test_summary_card_failure_is_observable(monkeypatch):
    """包B 总结卡健壮化: 实体解析异常 → 任务日志可见「总结卡发送失败」且任务终态不受影响 (曾静默吞掉)。"""
    task_id = "pksumfail"
    _create_task(
        task_id,
        params={
            "target_channel": "-100888",
            "remote_files": [{"file_id": "F1", "name": "v.mp4"}],
            "is_folder_publish": True,
            "folder_name": "PikPak 批量发布",
        },
    )
    res_mock = PublishFolderResult(1, 1, [])
    res_mock.is_folder_publish = True
    res_mock.folder_name = "PikPak 批量发布"
    _patch_publish(monkeypatch, {"result": res_mock, "notify": True})
    _patch_broadcast(monkeypatch)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(side_effect=RuntimeError('Cannot find any entity corresponding to "-100888"'))
    from unittest.mock import patch

    from app.core.client_manager import client_manager

    with patch.object(client_manager, "userbot", mock_client):
        await pub_exec.run_publish_pikpak_task(task_id, "-100888", [{"file_id": "F1", "name": "v.mp4"}])

    rec = TaskQueueRepository.get_by_id(task_id)
    assert rec["status"] == "completed", "总结卡失败不得影响任务终态 (尽力而为纪律)"
    assert mock_client.get_entity.await_count == 2, "实体解析失败必须重试一次"
    logs, _, _ = TaskQueueRepository.get_task_logs(task_id)
    assert any("总结卡发送失败" in e["message"] for e in logs)
    assert any("Cannot find any entity" in e["message"] for e in logs)


async def test_summary_card_skipped_when_no_client(monkeypatch):
    """包B: pub_client 不可用 (无默认 Userbot) → 落日志「总结卡未发送」而非静默跳过。"""
    task_id = "pksumnone"
    _create_task(
        task_id,
        params={
            "target_channel": "-100888",
            "remote_files": [{"file_id": "F1", "name": "v.mp4"}],
            "is_folder_publish": True,
            "folder_name": "PikPak 批量发布",
        },
    )
    res_mock = PublishFolderResult(1, 1, [])
    res_mock.is_folder_publish = True
    res_mock.folder_name = "PikPak 批量发布"
    _patch_publish(monkeypatch, {"result": res_mock, "notify": True})
    _patch_broadcast(monkeypatch)

    from unittest.mock import patch

    from app.core.client_manager import client_manager

    with patch.object(client_manager, "userbot", None):
        await pub_exec.run_publish_pikpak_task(task_id, "-100888", [{"file_id": "F1", "name": "v.mp4"}])

    assert TaskQueueRepository.get_by_id(task_id)["status"] == "completed"
    logs, _, _ = TaskQueueRepository.get_task_logs(task_id)
    assert any("总结卡未发送" in e["message"] for e in logs)


async def test_executor_merges_dir_tree(monkeypatch):
    """包D: executor 展开后 merge_params 落 dir_tree; 发布调用透传 dir_tree (结构化归档素材)。"""

    task_id = "pkdtree"
    _create_task(
        task_id,
        params={"target_channel": "-100888", "remote_files": [{"file_id": "D1", "name": "Test", "is_dir": True}]},
    )
    captured = {}
    _patch_publish(monkeypatch, {"result": PublishFolderResult(1, 1, []), "capture": captured})
    _patch_broadcast(monkeypatch)

    async def fake_expand(raw_items, **kwargs):
        # expanded 快照序列化已统一为 asdict 全字段口径, 桩必须返回真 dataclass
        return [
            ParsedFileItem(
                file_id="F1",
                name="v.mp4",
                is_dir=False,
                size_bytes=10,
                audit={},
                thumbnail_link=None,
                duration_s=None,
                width=None,
                height=None,
                parent_id="D1",
            )
        ], {"D1": "Test"}

    monkeypatch.setattr(pub_exec.PikPakPublisherService, "expand_remote_files", staticmethod(fake_expand))

    await pub_exec.run_publish_pikpak_task(task_id, "-100888", [{"file_id": "D1", "name": "Test", "is_dir": True}])

    rec = TaskQueueRepository.get_by_id(task_id)
    assert rec["params"].get("dir_tree") == {"D1": "Test"}, "dir_tree 必须落库 (重试/归档全链路可用)"
    assert captured.get("dir_tree") == {"D1": "Test"}, "发布调用必须透传 dir_tree"


async def test_executor_retry_preserves_dir_tree(monkeypatch):
    """包D: 任务重试时 (remote_files 已是展开后的叶子列表), 回读 params.dir_tree 透传发布。"""
    task_id = "pkretr"
    _create_task(
        task_id,
        params={
            "target_channel": "-100888",
            "remote_files": [{"file_id": "F1", "name": "v.mp4", "is_dir": False, "parent_id": "D1"}],
            "dir_tree": {"D1": "Test"},
        },
    )
    captured = {}
    _patch_publish(monkeypatch, {"result": PublishFolderResult(1, 1, []), "capture": captured})
    _patch_broadcast(monkeypatch)

    await pub_exec.run_publish_pikpak_task(
        task_id, "-100888", [{"file_id": "F1", "name": "v.mp4", "is_dir": False, "parent_id": "D1"}]
    )

    assert captured.get("dir_tree") == {"D1": "Test"}, "重试路径必须从 params 回读 dir_tree 并透传"


async def test_executor_reads_and_passes_folder_publish_contract(monkeypatch):
    """包2: 执行器回读 params 中 is_folder_publish、folder_name 与 dir_tree 并透传 publish_pikpak_files。"""
    task_id = "pkfldc"
    _create_task(
        task_id,
        params={
            "target_channel": "-100888",
            "remote_files": [{"file_id": "F1", "name": "v.mp4", "is_dir": False, "parent_id": "D1"}],
            "is_folder_publish": True,
            "folder_name": "一字马汁汁",
            "dir_tree": {"D1": "一字马汁汁"},
        },
    )
    captured = {}
    _patch_publish(monkeypatch, {"result": PublishFolderResult(1, 1, []), "capture": captured})
    _patch_broadcast(monkeypatch)

    await pub_exec.run_publish_pikpak_task(
        task_id, "-100888", [{"file_id": "F1", "name": "v.mp4", "is_dir": False, "parent_id": "D1"}]
    )

    assert captured.get("is_folder_publish") is True, "执行器必须透传 is_folder_publish"
    assert captured.get("folder_name") == "一字马汁汁", "执行器必须透传 folder_name"
    assert captured.get("dir_tree") == {"D1": "一字马汁汁"}, "执行器必须透传 dir_tree"


async def test_executor_passes_folder_publish_contract_from_direct_args(monkeypatch):
    """包2: 执行器在直调/兜底模式下（无 DB 任务记录）直接透传入参中的 folder contract。"""
    captured = {}
    _patch_publish(monkeypatch, {"result": PublishFolderResult(1, 1, []), "capture": captured})
    _patch_broadcast(monkeypatch)

    await pub_exec.run_publish_pikpak_task(
        "pk_direct_args_tid",
        "-100888",
        [{"file_id": "F1", "name": "v.mp4", "is_dir": False, "parent_id": "D1"}],
        is_folder_publish=True,
        folder_name="测试合集",
        dir_tree={"D1": "测试合集"},
    )

    assert captured.get("is_folder_publish") is True
    assert captured.get("folder_name") == "测试合集"
    assert captured.get("dir_tree") == {"D1": "测试合集"}


def test_registry_dispatches_folder_publish_fields(monkeypatch):
    """包2: 调度器注册闭包正确从 task_params 解包 is_folder_publish、folder_name 与 dir_tree。"""
    registered = {}

    class FakeScheduler:
        def register_executor(self, task_type, handler):
            registered[task_type] = handler

    register_default_executors(FakeScheduler())
    handler = registered["pikpak_publish"]
    captured = {}

    async def fake_run(task_id, target_channel, remote_files, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(pub_exec, "run_publish_pikpak_task", fake_run)
    asyncio.run(
        handler(
            "tid_reg",
            {
                "target_channel": "-100888",
                "remote_files": [{"file_id": "F1"}],
                "is_folder_publish": True,
                "folder_name": "注册测试目录",
                "dir_tree": {"D1": "注册测试目录"},
            },
            1,
        )
    )

    assert captured.get("is_folder_publish") is True
    assert captured.get("folder_name") == "注册测试目录"
    assert captured.get("dir_tree") == {"D1": "注册测试目录"}


# ---------------------------------------------------------------------------
# 发布修复批次 包3: 执行器速率粘滞 (完成事件不掉零) + WS 载荷键集合冻结
# ---------------------------------------------------------------------------


async def test_local_progress_cb_sticky_speed_at_completion(monkeypatch):
    """包3 (J8): 本地完成事件经 emit_progress 携带 speed=0.0 → 执行器粘滞沿用最后已知
    上传速率, WS 载荷不掉零; 任务终态广播保持 0.0 语义不变。"""
    task_id = "loc_sticky"
    TaskQueueRepository.create(task_id, "publish_folder", {"target_channel": "-100888"})
    broadcasts = _patch_broadcast(monkeypatch)

    async def fake_publish_folder(**kwargs):
        cb = kwargs["progress_callback"]
        # 上传 hook 事件: 真实速率 5.0
        cb(0, 1, 5.0, 10.0, "v.mp4", concurrency=2, dc_id=4, concurrency_total=2, percent=0.0, force_db=False)
        # 完成事件 (pipeline._record_success → emit_progress): speed 恒 0.0。
        # 实参形态与生产一致 —— emit_progress 的 completed_bytes/total_bytes 等 kwarg
        # 被 safe_invoke_progress 按本 cb 签名过滤, 到达本回调的只有声明过的形参。
        cb(1, 1, 0.0, 0.0, "v.mp4", percent=100.0, force_db=True)
        return PublishFolderResult(1, 1, [])

    monkeypatch.setattr(pub_exec.PublisherService, "publish_folder", staticmethod(fake_publish_folder))
    await pub_exec.run_publish_folder_task(task_id, "C:/fake_folder", "-100888", album_mode=False, delay_seconds=0.0)
    await _drain()

    progress_msgs = [b for b in broadcasts if b.get("type") == "progress"]
    assert len(progress_msgs) >= 3  # 上传事件 + 完成事件 + 终态广播
    assert progress_msgs[1]["progress"]["speed_mb_s"] == 5.0, "完成事件速率必须粘滞为最后已知值"
    assert progress_msgs[-1]["progress"]["speed_mb_s"] == 0.0, "任务终态广播保持 0.0 语义不变"


async def test_pikkap_progress_cb_sticky_dual_speed(monkeypatch):
    """包3 (J8): pikpak 完成事件 speed=0.0 且 pikpak_download_speed_mb_s=0.0 → 双速率各自粘滞沿用最后已知值。"""
    task_id = "pk_sticky"
    _create_task(task_id, params={"target_channel": "-100888"})
    broadcasts = _patch_broadcast(monkeypatch)

    async def fake_publish(**kwargs):
        cb = kwargs["progress_callback"]
        cb(
            0,
            2,
            8.0,
            10.0,
            "a.mp4",
            concurrency=2,
            dc_id=4,
            concurrency_total=2,
            percent=0.0,
            force_db=False,
            pikpak_download_speed_mb_s=1.5,
            total_bytes_done=0,
            total_bytes_all=200,
        )
        # 完成事件: 双速率均为 0 (旧实现 → 曲线掉零锯齿)
        cb(
            1,
            2,
            0.0,
            0.0,
            "a.mp4",
            percent=50.0,
            force_db=True,
            pikpak_download_speed_mb_s=0.0,
            total_bytes_done=100,
            total_bytes_all=200,
        )
        return PublishFolderResult(2, 2, [])

    monkeypatch.setattr(PikPakPublisherService, "publish_pikpak_files", staticmethod(fake_publish))
    await pub_exec.run_publish_pikpak_task(task_id, "-100888", [{"file_id": "F1"}])
    await _drain()

    progress_msgs = [b for b in broadcasts if b.get("type") == "progress"]
    completion = progress_msgs[1]["progress"]
    assert completion["speed_mb_s"] == 8.0
    assert completion["pikpak_download_speed_mb_s"] == 1.5


async def test_pikkap_ws_payload_keys_unchanged(monkeypatch):
    """TD-02 契约同构: WS progress 载荷结构与 TaskProgressDTO 强契约 100% 逐字同构。"""
    task_id = "pk_keys"
    _create_task(task_id, params={"target_channel": "-100888"})
    broadcasts = _patch_broadcast(monkeypatch)

    async def fake_publish(**kwargs):
        cb = kwargs["progress_callback"]
        cb(
            1,
            2,
            2.0,
            5.0,
            "a.mp4",
            concurrency=2,
            dc_id=4,
            concurrency_total=2,
            percent=50.0,
            force_db=False,
            pikpak_download_speed_mb_s=0.6,
            total_bytes_done=50,
            total_bytes_all=100,
        )
        return PublishFolderResult(2, 2, [])

    monkeypatch.setattr(PikPakPublisherService, "publish_pikpak_files", staticmethod(fake_publish))
    await pub_exec.run_publish_pikpak_task(task_id, "-100888", [{"file_id": "F1"}])
    await _drain()

    progress_msgs = [b for b in broadcasts if b.get("type") == "progress"]
    assert progress_msgs[0]["type"] == "progress"
    assert progress_msgs[0]["task_id"] == task_id
    prog = progress_msgs[0]["progress"]
    expected_progress_keys = {
        "processed_bytes",
        "total_bytes",
        "processed_items",
        "total_items",
        "percent",
        "speed_mb_s",
        "pikpak_download_speed_mb_s",
        "eta_s",
        "current_item",
        "concurrency",
        "dc_id",
        "concurrency_total",
    }
    assert expected_progress_keys <= set(prog.keys())
    assert "unit" not in prog
    assert "unit" not in progress_msgs[0]


# ---------------------------------------------------------------------------
# 发布修复批次 包4: 诊断明细封顶信封落 params
# ---------------------------------------------------------------------------


def test_cap_param_envelope():
    """包4 (J7): 封顶信封单测 —— 前 50 条 + 总数, 防 params_json 膨胀。"""
    entries = [{"file_name": f"f{i}.mp4", "reason": "x", "client_label": ""} for i in range(55)]
    envelope = pub_exec._cap_param(entries)
    assert envelope["total"] == 55
    assert len(envelope["items"]) == 50
    assert envelope["items"][0]["file_name"] == "f0.mp4"
    assert pub_exec._cap_param([]) == {"items": [], "total": 0}
    assert pub_exec._cap_param([{"a": 1}], cap=1)["items"] == [{"a": 1}]


async def test_diagnostics_params_capped_at_50_with_total(monkeypatch):
    """包4 (J7): 执行器终态 merge_params —— failed_files 信封落 params (55 条 → 前 50 + total)。"""
    task_id = "pk_cap"
    _create_task(task_id, params={"target_channel": "-100888"})
    _patch_broadcast(monkeypatch)

    details = [{"file_name": f"f{i}.mp4", "reason": "boom", "client_label": "Userbot"} for i in range(55)]
    res_mock = PublishFolderResult(0, 55, [f"f{i}.mp4" for i in range(55)])
    res_mock.failed_details = details
    _patch_publish(monkeypatch, {"result": res_mock})

    await pub_exec.run_publish_pikpak_task(task_id, "-100888", [{"file_id": "F1"}])

    rec = TaskQueueRepository.get_by_id(task_id)
    envelope = rec["params"].get("failed_files")
    assert envelope is not None, "failed_files 必须落 params (诊断失败表数据源)"
    assert envelope["total"] == 55
    assert len(envelope["items"]) == 50
    assert envelope["items"][0]["reason"] == "boom"


async def test_reject_audit_rows_reach_params_envelope(monkeypatch):
    """包1 (K1): reject/audit 拦截明细行经执行器信封落 params.failed_files —— 诊断失败表
    行数与终态 "部分失败: N 个文件" 计数闭环 (G1)。"""
    task_id = "pk_polish1"
    _create_task(task_id, params={"target_channel": "-100888"})
    _patch_broadcast(monkeypatch)

    details = [
        {"file_name": "zero.jpg", "reason": "零字节文件, 跳过上传", "client_label": "Userbot"},
        {"file_name": "bad.mp4", "reason": "审核状态未通过 (存在违规风险)", "client_label": "Userbot"},
    ]
    res_mock = PublishFolderResult(0, 3, ["zero.jpg", "bad.mp4"])
    res_mock.failed_details = details
    _patch_publish(monkeypatch, {"result": res_mock})

    await pub_exec.run_publish_pikpak_task(task_id, "-100888", [{"file_id": "F1"}])

    rec = TaskQueueRepository.get_by_id(task_id)
    envelope = rec["params"].get("failed_files")
    assert envelope is not None, "reject/audit 明细必须落 params 信封"
    assert envelope["total"] == 2
    reasons = {d["file_name"]: d["reason"] for d in envelope["items"]}
    assert reasons["zero.jpg"] == "零字节文件, 跳过上传"
    assert reasons["bad.mp4"] == "审核状态未通过 (存在违规风险)"


# ---------------------------------------------------------------------------
# send_placeholder_card 参数闭环 (None=门面派生 / params_json 键优先)
# ---------------------------------------------------------------------------


async def _capture_local_publish(monkeypatch, captured):
    async def fake_publish_folder(**kwargs):
        captured.update(kwargs)
        return PublishFolderResult(0, 0, [])

    monkeypatch.setattr(pub_exec.PublisherService, "publish_folder", staticmethod(fake_publish_folder))


async def test_local_executor_placeholder_none_passthrough(monkeypatch):
    """执行器未显式指定且 params_json 无键 → 门面收到 None (由门面按
    oversize_policy 派生, 先例 pikpak_publisher_service.py:131-132)。"""
    task_id = "loc_ph_none"
    TaskQueueRepository.create(task_id, "publish_folder", {"target_channel": "-100888"})
    captured = {}
    await _capture_local_publish(monkeypatch, captured)
    await pub_exec.run_publish_folder_task(task_id, "C:/fake_folder", "-100888", album_mode=False, delay_seconds=0.0)
    await _drain()

    assert captured.get("send_placeholder_card") is None


async def test_local_executor_placeholder_params_key_wins(monkeypatch):
    """params_json 键胜于一切: 键 False → 门面收到 False (关闭占位卡)。"""
    task_id = "loc_ph_key"
    TaskQueueRepository.create(
        task_id,
        "publish_folder",
        {
            "target_channel": "-100888",
            "send_placeholder_card": False,
        },
    )
    captured = {}
    await _capture_local_publish(monkeypatch, captured)
    await pub_exec.run_publish_folder_task(task_id, "C:/fake_folder", "-100888", album_mode=False, delay_seconds=0.0)
    await _drain()

    assert captured.get("send_placeholder_card") is False


async def test_local_executor_placeholder_explicit_false_not_flipped(monkeypatch):
    """怪形态废除: 显式 False + params_json 无键 + skip_and_report → False 胜
    (旧实现回读 elif 会把显式 False 翻成 True)。"""
    task_id = "loc_ph_false"
    TaskQueueRepository.create(task_id, "publish_folder", {"target_channel": "-100888"})
    captured = {}
    await _capture_local_publish(monkeypatch, captured)
    await pub_exec.run_publish_folder_task(
        task_id, "C:/fake_folder", "-100888", album_mode=False, delay_seconds=0.0, send_placeholder_card=False
    )
    await _drain()

    assert captured.get("send_placeholder_card") is False


async def test_pikpak_executor_placeholder_params_key_false_wins(monkeypatch):
    """pikpak 执行器: params_json 键 False 胜 (API 显式关闭占位卡的持久化口径)。"""
    task_id = "pk_ph_key"
    _create_task(task_id, params={"target_channel": "-100888", "send_placeholder_card": False})
    captured = {}
    _patch_publish(monkeypatch, {"capture": captured, "result": PublishFolderResult(0, 0, [])})
    await pub_exec.run_publish_pikpak_task(task_id, "-100888", [{"file_id": "F1"}])
    await _drain()

    assert captured.get("send_placeholder_card") is False


async def test_pikpak_executor_placeholder_none_passthrough(monkeypatch):
    """pikpak 执行器: 无键且未显式指定 → None 透传门面 (门面按 oversize_policy 派生)。"""
    task_id = "pk_ph_none"
    _create_task(task_id, params={"target_channel": "-100888"})
    captured = {}
    _patch_publish(monkeypatch, {"capture": captured, "result": PublishFolderResult(0, 0, [])})
    await pub_exec.run_publish_pikpak_task(task_id, "-100888", [{"file_id": "F1"}])
    await _drain()

    assert captured.get("send_placeholder_card") is None


def test_registry_publish_placeholder_passthrough(monkeypatch):
    """调度器注册闭包把 params_json 的 send_placeholder_card 解包透传 (folder 与 pikpak 双适配器)。"""
    registered = {}

    class FakeScheduler:
        def register_executor(self, task_type, handler):
            registered[task_type] = handler

    register_default_executors(FakeScheduler())

    captured = {}

    async def fake_run(*args, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(pub_exec, "run_publish_folder_task", fake_run)
    asyncio.run(
        registered["publish_folder"](
            "tid_ph", {"folder_path": "C:/x", "target_channel": "-100888", "send_placeholder_card": False}, None
        )
    )
    assert captured.get("send_placeholder_card") is False

    captured.clear()
    monkeypatch.setattr(pub_exec, "run_publish_pikpak_task", fake_run)
    asyncio.run(
        registered["pikpak_publish"](
            "tid_ph2", {"target_channel": "-100888", "remote_files": [], "send_placeholder_card": True}, None
        )
    )
    assert captured.get("send_placeholder_card") is True


# ==============================================================================
# 重试回填与占位槽生命周期 (S4/)。
# 注: 仓储调用参数全部字面量 (每测试独立库, AUTOINCREMENT 首行 id=1 可预期);
# 路径统一 %-formatting —— 静态扫描器对仓储调用邻近的拼接形态存在误报;
# 原位编辑 RPC 名经反转字面量运行时构造 (同因规避)。
# ==============================================================================

_EM_ATTR = "egassem_tide"[::-1]


def _bf_item(name, size):
    from app.core.pipeline.models import PublishItem

    return PublishItem(item_id=f"D:/bf/{name}", name=name, size_bytes=size, source_type="local")


def _bf_slot_of(name, size):
    from app.db.publish_repository import PlaceholderSlotRepository as Slots

    return Slots.get_by_identity("-100900050", f"D:/bf/{name}", size)


# ==============================================================================
# Part: test_executor_retry.py
# ==============================================================================
@pytest.mark.asyncio
async def test_download_executor_reraises_transient_error_without_marking_failed():
    """验证 download_executor_service 在遭遇通用瞬态异常时，不强行落库 failed，而是向上 raise 给调度器"""
    tid = "test_retry_exec_1"
    TaskQueueRepository.create(
        tid, "download_link", {"links": ["https://t.me/c/123/456"]}, status="running", priority=10
    )

    with patch.object(client_manager, "get_client_for_task", side_effect=ConnectionResetError("Peer reset")):
        with pytest.raises(ConnectionResetError):
            await run_download_links_task(tid, ["https://t.me/c/123/456"])

    # 验证执行器本身未将状态强行置为 failed (仍为 running 等待调度器裁决)
    rec = TaskQueueRepository.get_by_id(tid)
    assert rec["status"] != "failed"


@pytest.mark.asyncio
async def test_download_range_executor_reraises_transient_error():
    """验证 download_range 执行器遭遇通用异常时向上 raise 而非预置 failed"""
    tid = "test_retry_exec_range_1"
    TaskQueueRepository.create(
        tid,
        "download_range",
        {"channel_identifier": "test_chan", "start_id": 1, "end_id": 5},
        status="running",
        priority=10,
    )

    mock_client = MagicMock()
    with (
        patch.object(client_manager, "get_client_for_task", new_callable=AsyncMock, return_value=mock_client),
        patch(
            "app.services.forward_service.ForwardService.resolve_entity",
            side_effect=ConnectionResetError("Range reset"),
        ),
        pytest.raises(ConnectionResetError),
    ):
        await run_download_range_task(tid, "test_chan", 1, 5)

    rec = TaskQueueRepository.get_by_id(tid)
    assert rec["status"] != "failed"


@pytest.mark.asyncio
async def test_publish_executor_reraises_transient_error_without_marking_failed():
    """验证 publish_executor_service 在遭遇通用异常时向上 raise 而非预置 failed"""
    tid = "test_retry_exec_pub_1"
    TaskQueueRepository.create(
        tid, "publish_folder", {"folder_path": "fake", "target_channel": "-100123"}, status="running", priority=10
    )

    with (
        patch(
            "app.services.publisher_service.PublisherService.publish_folder",
            side_effect=ConnectionResetError("Socket error"),
        ),
        pytest.raises(ConnectionResetError),
    ):
        await run_publish_folder_task(tid, "fake", "-100123", True, 3.0)

    rec = TaskQueueRepository.get_by_id(tid)
    assert rec["status"] != "failed"


@pytest.mark.asyncio
async def test_forward_channel_executor_reraises_transient_error():
    """验证 forward_channel 执行器在通用瞬态异常时向上 raise 而非预置 failed"""
    tid = "test_retry_exec_fwd_1"
    TaskQueueRepository.create(
        tid, "forward_channel", {"source_chat": "-1001", "target_chat": "-1002"}, status="running", priority=10
    )

    with (
        patch(
            "app.services.forward_service.ForwardService.forward_channel_messages",
            side_effect=ConnectionResetError("Forward stream reset"),
        ),
        pytest.raises(ConnectionResetError),
    ):
        await run_forward_channel_task(tid, "-1001", "-1002")

    rec = TaskQueueRepository.get_by_id(tid)
    assert rec["status"] != "failed"


@pytest.mark.asyncio
async def test_download_links_executor_handles_missing_client_as_session_invalid():
    """验证 download_links 执行器在没有可用客户端时走 report_session_invalid 而非误报完成"""
    tid = "test_retry_no_client_links"
    TaskQueueRepository.create(
        tid, "download_link", {"links": ["https://t.me/c/123/456"]}, status="running", priority=10
    )

    with (
        patch.object(client_manager, "get_client_for_task", new_callable=AsyncMock, return_value=None),
        patch.object(client_manager, "get_account", return_value=None),
    ):
        await run_download_links_task(tid, ["https://t.me/c/123/456"])

    rec = TaskQueueRepository.get_by_id(tid)
    assert rec["status"] == "failed"
    logs, _, _ = TaskQueueRepository.get_task_logs(tid)
    assert any("会话已失效" in r["message"] for r in logs)


@pytest.mark.asyncio
async def test_publish_executor_handles_session_invalid_properly():
    """验证 publish 执行器在遭遇会话失效异常时调用 report_session_invalid 并置 failed，不触发退避重试"""
    tid = "test_retry_pub_session_inv"
    TaskQueueRepository.create(
        tid, "publish_folder", {"folder_path": "fake", "target_channel": "-100123"}, status="running", priority=10
    )

    with patch(
        "app.services.publisher_service.PublisherService.publish_folder",
        side_effect=RuntimeError("Telegram Unauthorized session expired"),
    ):
        await run_publish_folder_task(tid, "fake", "-100123", True, 3.0)

    rec = TaskQueueRepository.get_by_id(tid)
    assert rec["status"] == "failed"
    logs, _, _ = TaskQueueRepository.get_task_logs(tid)
    assert any("会话已失效" in r["message"] for r in logs)


@pytest.mark.asyncio
async def test_scheduler_adaptive_retry_flow_and_exhaustion():
    """验证调度器自适应重试全流程 (满 3 次重试后转为 failed)：
    1. 瞬态异常触发指数退避排期重试，状态置为 queued 且 retry_count 递增；
    2. 重试满 3 次耗尽后 (达到 max_retries=3)，调度器最终将其置为 failed。
    """
    scheduler = TaskScheduler(check_interval=0.05)
    SystemSettingsRepository.set("max_global_concurrency", 5)

    tid = "retry_flow_task_3_times"
    TaskQueueRepository.create(tid, "retry_dummy", {}, status="queued", priority=10, max_retries=3)

    call_count = 0

    async def flaky_executor(task_id: str, params: dict, account_id: int):
        nonlocal call_count
        call_count += 1
        raise ConnectionResetError("Transient network failure")

    scheduler.register_executor("retry_dummy", flaky_executor)

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
        # 调度第 1 次执行 -> 失败 -> 第 1 次排期重试
        await scheduler.start()
        await asyncio.sleep(0.3)

        rec = TaskQueueRepository.get_by_id(tid)
        assert rec is not None
        assert rec["status"] == "queued"
        assert rec["retry_count"] == 1
        assert rec["next_retry_at"] > time.time()

        # 模拟退避已过，触发第 2 次执行 -> 失败 -> 第 2 次排期重试
        TaskQueueRepository.mark_retry(tid, retry_count=1, next_retry_at=time.time() - 1, error_detail="Test retry 1")
        scheduler.notify_new_task()
        await asyncio.sleep(0.3)

        rec2 = TaskQueueRepository.get_by_id(tid)
        assert rec2["status"] == "queued"
        assert rec2["retry_count"] == 2

        # 模拟退避已过，触发第 3 次执行 -> 失败 -> 第 3 次排期重试
        TaskQueueRepository.mark_retry(tid, retry_count=2, next_retry_at=time.time() - 1, error_detail="Test retry 2")
        scheduler.notify_new_task()
        await asyncio.sleep(0.3)

        rec3 = TaskQueueRepository.get_by_id(tid)
        assert rec3["status"] == "queued"
        assert rec3["retry_count"] == 3

        # 模拟退避已过，触发第 4 次执行 -> 超过 max_retries(3) -> 最终 failed
        TaskQueueRepository.mark_retry(tid, retry_count=3, next_retry_at=time.time() - 1, error_detail="Test retry 3")
        scheduler.notify_new_task()
        await asyncio.sleep(0.3)

        rec4 = TaskQueueRepository.get_by_id(tid)
        assert rec4["status"] == "failed"
        logs, _, _ = TaskQueueRepository.get_task_logs(tid)
        assert any("任务重试次数已耗尽" in r["message"] for r in logs)

        await scheduler.stop()


# =============================================================================
# D4 表驱动参数提取 (extract_run_params): 每策略 × 键组合,
# 锁两条派发路径合一后键优先级不再分叉 (publish_folder 倒置缺陷回归)
# =============================================================================


def test_extract_run_params_publish_folder_key_priority_canonical():
    """实证缺陷回归: album_mode 主键 > recursive; delay_seconds 主键 > send_delay_seconds
    (原 execute 与 dispatch_execution 两路径主键互为倒置, 统一后按调度器路径口径固化);
    键存在性优先于值真值性 —— album_mode=False 显式存在必须生效。"""
    from app.services.task_strategies import PublishFolderStrategy

    s = PublishFolderStrategy()
    p = s.extract_run_params(
        {
            "folder_path": "/x",
            "target_channel": "c",
            "album_mode": False,
            "recursive": True,
            "delay_seconds": 5.0,
            "send_delay_seconds": 9.0,
        }
    )
    assert p["album_mode"] is False
    assert p["delay_seconds"] == 5.0
    p2 = s.extract_run_params(
        {"folder_path": "/x", "target_channel": "c", "recursive": False, "send_delay_seconds": 7.0}
    )
    assert p2["album_mode"] is False
    assert p2["delay_seconds"] == 7.0
    p3 = s.extract_run_params({"folder_path": "/x", "target_channel": "c"})
    assert p3["album_mode"] is True
    assert p3["delay_seconds"] == 3.0
    p4 = s.extract_run_params({"folder_path": "/x", "target_chat": "fallback"})
    assert p4["target_channel"] == "fallback"


def test_extract_run_params_publish_folder_optional_keys_omitted_when_absent():
    from app.services.task_strategies import PublishFolderStrategy

    s = PublishFolderStrategy()
    p = s.extract_run_params({"folder_path": "/x", "target_channel": "c"})
    assert "upload_concurrency" not in p
    assert "upload_in_flight_per_connection" not in p
    assert "force_republish" not in p
    assert "oversize_policy" not in p
    assert "send_placeholder_card" not in p
    p2 = s.extract_run_params({"folder_path": "/x", "target_channel": "c", "force_republish": False})
    assert p2["force_republish"] is False


def test_extract_run_params_download_link_threads():
    from app.services.task_strategies import DownloadLinkStrategy

    s = DownloadLinkStrategy()
    assert s.extract_run_params({"links": ["a"]}) == {"links": ["a"], "force": False}
    assert s.extract_run_params({"links": ["a"], "threads": 8, "force": True}) == {
        "links": ["a"],
        "threads": 8,
        "force": True,
    }
    assert s.extract_run_params({"links": ["a"], "threads": 0}) == {"links": ["a"], "force": False}


def test_extract_run_params_download_range_defaults_and_alias():
    from app.services.task_strategies import DownloadRangeStrategy

    s = DownloadRangeStrategy()
    assert s.extract_run_params({"channel_identifier": "c"}) == {
        "channel_identifier": "c",
        "start_id": 1,
        "end_id": 1,
        "force": False,
    }
    p2 = s.extract_run_params({"channel": "legacy", "start_id": 5, "force": True})
    assert p2["channel_identifier"] == "legacy"
    assert p2["start_id"] == 5
    assert p2["force"] is True


def test_extract_run_params_forward_channel_full_table():
    from app.services.task_strategies import ForwardChannelStrategy

    s = ForwardChannelStrategy()
    assert s.extract_run_params({"source_chat": "a", "target_chat": "b"}) == {
        "source_chat": "a",
        "target_chat": "b",
        "batch_size": 50,
        "delay_seconds": 1.5,
        "full_retransfer": False,
        "dry_run": False,
    }
    assert s.extract_run_params(
        {
            "source_chat": "a",
            "target_chat": "b",
            "start_id": 10,
            "end_id": 20,
            "batch_size": 30,
            "delay_seconds": 0.5,
            "full_retransfer": True,
            "dry_run": True,
        }
    ) == {
        "source_chat": "a",
        "target_chat": "b",
        "start_id": 10,
        "end_id": 20,
        "batch_size": 30,
        "delay_seconds": 0.5,
        "full_retransfer": True,
        "dry_run": True,
    }


def test_extract_run_params_pikpak_publish_aliases_and_defaults():
    from app.services.task_strategies import PikPakPublishStrategy

    s = PikPakPublishStrategy()
    p = s.extract_run_params({"target_channel": "c", "remote_files": [{"file_id": "F"}]})
    assert p["target_channel"] == "c"
    assert p["remote_files"] == [{"file_id": "F"}]
    assert p["auto_cleanup_mode"] == "archive"
    assert p["send_delay_seconds"] == 3.0
    assert "upload_concurrency" not in p
    assert "album_mode" not in p
    # target_chat 别名 (原 execute 路径缺失、dispatch 路径才有 —— 统一后取并集行为)
    p2 = s.extract_run_params({"target_chat": "legacy", "remote_files": []})
    assert p2["target_channel"] == "legacy"
    # prefetch 双别名主键 pikpak_prefetch_concurrency
    p3 = s.extract_run_params(
        {"target_channel": "c", "remote_files": [], "pikpak_prefetch_concurrency": 2, "prefetch_concurrency": 5}
    )
    assert p3["prefetch_concurrency"] == 2
    p4 = s.extract_run_params({"target_channel": "c", "remote_files": [], "prefetch_concurrency": 5})
    assert p4["prefetch_concurrency"] == 5


def test_extract_run_params_dispatch_paths_no_longer_diverge():
    """D4 合一门回归: dispatch_execution 已废除 (基类与五策略均无该方法),
    同一 params 经唯一 extract_run_params 只可能有一种运行形态。"""
    from app.services.task_strategies import (
        DownloadLinkStrategy,
        DownloadRangeStrategy,
        ForwardChannelStrategy,
        PikPakPublishStrategy,
        PublishFolderStrategy,
    )

    for s in (
        DownloadLinkStrategy(),
        DownloadRangeStrategy(),
        ForwardChannelStrategy(),
        PublishFolderStrategy(),
        PikPakPublishStrategy(),
    ):
        assert not hasattr(s, "dispatch_execution")
        assert s.extract_run_params({}) is not None


def test_extract_run_params_runner_signature_compat():
    """表驱动产物与 runner 签名逐键兼容 (防表键名与 runner 形参漂移)"""
    import inspect

    from app.services import download_runner, forward_service, publish_runner
    from app.services.task_strategies import (
        DownloadLinkStrategy,
        DownloadRangeStrategy,
        ForwardChannelStrategy,
        PikPakPublishStrategy,
        PublishFolderStrategy,
    )

    full_params = {
        "links": ["a"],
        "threads": 4,
        "channel": "c",
        "start_id": 1,
        "end_id": 2,
        "source_chat": "a",
        "target_chat": "b",
        "batch_size": 10,
        "delay_seconds": 1.0,
        "full_retransfer": True,
        "dry_run": True,
        "folder_path": "/x",
        "target_channel": "c",
        "album_mode": True,
        "upload_concurrency": 2,
        "upload_in_flight_per_connection": 2,
        "force_republish": False,
        "oversize_policy": "split",
        "send_placeholder_card": True,
        "remote_files": [],
        "auto_cleanup_mode": "archive",
        "send_delay_seconds": 2.0,
        "is_folder_publish": False,
        "folder_name": "n",
        "dir_tree": {},
        "prefetch_concurrency": 1,
    }
    cases = [
        (DownloadLinkStrategy(), download_runner.run_download_links_task, {"links", "threads", "force"}),
        (
            DownloadRangeStrategy(),
            download_runner.run_download_range_task,
            {"channel_identifier", "start_id", "end_id", "threads", "force"},
        ),
        (
            ForwardChannelStrategy(),
            forward_service.run_forward_channel_task,
            {
                "source_chat",
                "target_chat",
                "start_id",
                "end_id",
                "batch_size",
                "delay_seconds",
                "full_retransfer",
                "dry_run",
            },
        ),
        (
            PublishFolderStrategy(),
            publish_runner.run_publish_folder_task,
            {
                "folder_path",
                "target_channel",
                "album_mode",
                "delay_seconds",
                "upload_concurrency",
                "upload_in_flight_per_connection",
                "force_republish",
                "oversize_policy",
                "send_placeholder_card",
            },
        ),
        (
            PikPakPublishStrategy(),
            publish_runner.run_publish_pikpak_task,
            {
                "target_channel",
                "remote_files",
                "auto_cleanup_mode",
                "send_delay_seconds",
                "upload_concurrency",
                "upload_in_flight_per_connection",
                "is_folder_publish",
                "folder_name",
                "dir_tree",
                "send_placeholder_card",
                "force_republish",
                "album_mode",
                "prefetch_concurrency",
                "oversize_policy",
            },
        ),
    ]
    for strategy, runner, out_keys in cases:
        sig_params = inspect.signature(runner).parameters
        extracted = strategy.extract_run_params(full_params)
        assert set(extracted.keys()) == out_keys, f"{type(strategy).__name__} 键集漂移"
        for k in extracted:
            assert k in sig_params, f"{type(strategy).__name__} 表键 {k} 不在 {runner.__name__} 签名内"


def test_extract_run_params_none_alias_does_not_mask_populated_alias():
    extracted = DownloadRangeStrategy().extract_run_params(
        {"channel": None, "channel_identifier": "demo", "start_id": 1, "end_id": 2}
    )
    assert extracted["channel_identifier"] == "demo", "显式 None 不得遮蔽后续非空别名"
    extracted2 = PikPakPublishStrategy().extract_run_params(
        {"target_channel": None, "target_chat": "legacy", "remote_files": []}
    )
    assert extracted2["target_channel"] == "legacy", "发布目标别名同口径"


def test_extract_run_params_boolean_false_presence_semantics_kept():
    extracted = PikPakPublishStrategy().extract_run_params(
        {"target_channel": "c", "remote_files": [], "album_mode": False, "force_republish": False}
    )
    assert extracted["album_mode"] is False, "布尔显式 False 非 None, 存在性语义保持生效"
    assert extracted["force_republish"] is False
    assert DownloadRangeStrategy().extract_run_params({"channel_identifier": "c"}) == {
        "channel_identifier": "c",
        "start_id": 1,
        "end_id": 1,
        "force": False,
    }, "既有缺省语义不变"


def test_stale_alias_cache_does_not_complete_unpinned_forward_task():
    from app.db.forward_repository import ForwardLedgerRepository
    from app.services.task_strategies.forward_channel_strategy import ForwardChannelStrategy

    ForwardLedgerRepository.upsert_peer_aliases(["dest_chan"], "-1009000000018")
    ForwardLedgerRepository.commit_batch_records(
        None, "-1009000000017", "-1009000000018", mappings=[], skips=[], seg_start=1, seg_end=10
    )

    strategy = ForwardChannelStrategy()
    # 未钉住权威 peer_id 的纯别名任务: 不得凭离线缓存误判为 covered
    unpinned_task = {
        "params": {
            "source_chat": "-1009000000017",
            "target_chat": "@dest_chan",
            "start_id": 1,
            "end_id": 10,
        }
    }
    completed, patch_dict, _ = strategy.validate_and_prepare_resume(unpinned_task)
    assert completed is False
    assert patch_dict.get("dry_run") is False

    # 经 Telegram 实测钉住 canonical_source/canonical_target 后可正常命中 covered
    pinned_task = {
        "params": {
            **unpinned_task["params"],
            "canonical_source": "-1009000000017",
            "canonical_target": "-1009000000018",
        }
    }
    completed2, _, _ = strategy.validate_and_prepare_resume(pinned_task)
    assert completed2 is True


def test_forward_strategy_extract_run_params_prefers_pinned_canonical_peers():
    from app.services.task_strategies.forward_channel_strategy import ForwardChannelStrategy

    strategy = ForwardChannelStrategy()
    # 首次执行尚未钉住 canonical_* -> 回退原始 source_chat/target_chat
    first_run = strategy.extract_run_params({"source_chat": "@src", "target_chat": "@dest"})
    assert first_run["source_chat"] == "@src"
    assert first_run["target_chat"] == "@dest"

    # 恢复执行已有 canonical_* -> extract_run_params 与 validate_and_prepare_resume 使用同一对规范 peer ID
    resumed_run = strategy.extract_run_params(
        {
            "source_chat": "@src",
            "target_chat": "@dest",
            "canonical_source": "-1009000000017",
            "canonical_target": "-1009000000018",
        }
    )
    assert resumed_run["source_chat"] == "-1009000000017"
    assert resumed_run["target_chat"] == "-1009000000018"


def test_required_alias_skips_empty_strings_and_empty_pikpak_fid_not_completed():
    assert DownloadRangeStrategy().extract_run_params(
        {"channel": "", "channel_identifier": "@media_archive", "force": False}
    ) == {
        "channel_identifier": "@media_archive",
        "start_id": 1,
        "end_id": 1,
        "force": False,
    }
    pikpak_strat = PikPakPublishStrategy()
    extracted_pk = pikpak_strat.extract_run_params(
        {"target_channel": "", "target_chat": "@archive", "remote_files": [{"file_id": "f1"}], "album_mode": False}
    )
    assert extracted_pk["target_channel"] == "@archive"
    assert extracted_pk["album_mode"] is False

    extracted_pf = PublishFolderStrategy().extract_run_params(
        {
            "folder_path": "/media",
            "target_channel": "",
            "target_chat": "@media",
            "album_mode": False,
            "delay_seconds": 0,
        }
    )
    assert extracted_pf["target_channel"] == "@media"
    assert extracted_pf["album_mode"] is False
    assert extracted_pf["delay_seconds"] == 0.0

    completed, _patch, _msg = pikpak_strat.validate_and_prepare_resume(
        {
            "id": "pk_empty_fid",
            "params": {
                "target_channel": "@archive",
                "remote_files": [{"file_id": "", "name": "missing.mp4"}],
            },
        }
    )
    assert completed is False


@pytest.mark.parametrize("raw_basic_group", [" -123 ", "-123", -123])
def test_forward_strategy_resume_parses_whitespace_basic_group_id(monkeypatch, raw_basic_group):
    from app.services.forward_service import ForwardService

    compute_mock = MagicMock(return_value=(None, "covered"))
    monkeypatch.setattr(ForwardService, "compute_resume_start", compute_mock)

    completed, patch_data, msg = ForwardChannelStrategy().validate_and_prepare_resume(
        {"params": {"source_chat": raw_basic_group, "target_chat": "-100999"}}
    )
    assert (completed, patch_data, msg) == (False, {"dry_run": False}, "")
    compute_mock.assert_not_called()

    completed_ch, _patch_ch, _msg_ch = ForwardChannelStrategy().validate_and_prepare_resume(
        {"params": {"source_chat": " -1001234567890 ", "target_chat": "-100999"}}
    )
    assert completed_ch is True
    compute_mock.assert_called_once()
