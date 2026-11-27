"""
任务生命周期编排服务单元测试 (TaskLifecycleService Unit Tests)

覆盖：
1. _parse_precheck_target 各种链接与 ID 解析
2. pause_queue / resume_queue 全局队列控制
3. pause_task / resume_task (normal/steady 降档) 状态流转
4. cancel_task 信号与终态持久化
5. delete_task (文件级清理与断点 GC)
6. retry_task 状态重置与调度派发
7. clear_finished_tasks 终态批量清理
8. update_task_concurrency 动态并发热更新与控制信号同步
9. batch_action 批量编排与异常聚合
10. precheck_access 权限预检 (无账号 / 账号就绪 / 异常熔断)
11. skip_current_file 业务逻辑
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from app.core.pikpak.taxonomy import PikPakPublishThrottledError
from app.core.runtime import (
    acquire_queued_task,
    active_task_controls,
    is_task_cancelled,
    is_task_paused,
    mark_task_executor_exit,
    mark_task_pause_exit,
    resume_paused_executor,
    set_task_control,
    stop_task_with_controls,
)
from app.core.scheduler import RetryableTaskError, TaskPausedError, TaskScheduler, UploadPausedError
from app.db.database import get_db
from app.db.download_repository import DownloadBreakpointRepository
from app.db.publish_repository import PublishHistoryRepository
from app.db.task_repository import TaskQueueRepository
from app.models.task import (
    BatchTaskActionRequest,
    PrecheckAccessRequest,
    SkipCurrentFileRequest,
    UpdateTaskConcurrencyRequest,
)
from app.services.executor_registry import register_default_executors
from app.services.publish_runner import _publish_terminal
from app.services.task_lifecycle_service import (
    TaskLifecycleService,
    _parse_precheck_target,
    prepare_task_resume,
)


@pytest.fixture(autouse=True)
def clean_tasks_and_controls():
    """每次测试前后清理 task_queue 与 active_task_controls 状态并注册策略"""
    register_default_executors()
    active_task_controls.clear()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM task_queue")
    cursor.execute("DELETE FROM task_logs")
    cursor.execute("DELETE FROM download_breakpoints")
    conn.commit()
    conn.close()
    yield
    active_task_controls.clear()


# ==============================================================================
# 1. 预检目标解析器 (_parse_precheck_target)
# ==============================================================================


def test_parse_precheck_target():
    # 公开频道纯名与 @ 形式
    assert _parse_precheck_target("mychannel") == "mychannel"
    assert _parse_precheck_target("t.me/mychannel") == "mychannel"
    assert _parse_precheck_target("https://t.me/mychannel/123") == "mychannel"

    # 私密频道 t.me/c/<id>/<msg_id> (-100 前缀)
    assert _parse_precheck_target("https://t.me/c/9000000010/42") == -1009000000010
    assert _parse_precheck_target("t.me/c/900000046/1") == -100900000046

    # 纯数字 ID
    assert _parse_precheck_target("-1009000000010") == -1009000000010
    # 统一走 parse_chat_identifier, 9~12 位正整数自愈为 -100 频道格式
    # (旧内联解析保留裸 int, 与转发/下载链路口径不一致)
    assert _parse_precheck_target("900000009") == -100900000009


# ==============================================================================
# 2. 全局队列暂停与恢复 (pause_queue / resume_queue)
# ==============================================================================


@pytest.mark.asyncio
async def test_pause_and_resume_queue():
    mock_sched = MagicMock()
    mock_sched.is_running = True
    mock_sched.notify_new_task = MagicMock()

    with patch("app.services.task_lifecycle_service.connection_manager.broadcast", new=AsyncMock()):
        # 暂停队列
        res_pause = await TaskLifecycleService.pause_queue()
        assert res_pause["status"] == "ok"

        # 恢复队列
        res_resume = await TaskLifecycleService.resume_queue(sched=mock_sched)
        assert res_resume["status"] == "ok"
        mock_sched.notify_new_task.assert_called_once()


# ==============================================================================
# 3. 单任务暂停与恢复 (pause_task / resume_task)
# ==============================================================================


@pytest.mark.asyncio
async def test_pause_task_running_and_pending():
    task_id = "test_pause_1"
    TaskQueueRepository.create(task_id, "download_link", {"link": "https://t.me/test/1"})
    TaskQueueRepository.update_status(task_id, "running")

    # 协程活跃场景: 设置控制信号
    active_task_controls[task_id] = {"cancelled": False, "paused": False}

    with patch("app.services.task_lifecycle_service.connection_manager.broadcast", new=AsyncMock()):
        res = await TaskLifecycleService.pause_task(task_id)
        assert res["status"] == "paused"
        assert active_task_controls[task_id]["paused"] is True
        assert TaskQueueRepository.get_status(task_id) == "paused"

    # 任务不存在抛出 404
    with pytest.raises(HTTPException) as exc_info:
        await TaskLifecycleService.pause_task("non_existent_task")
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_resume_task_normal_and_steady():
    task_id = "test_resume_pikpak"
    TaskQueueRepository.create(
        task_id,
        "pikpak_publish",
        {
            "target_channel": "testchan",
            "remote_files": [{"id": "1", "name": "f1.mp4", "size": 100}],
            "upload_concurrency": 8,
            "upload_in_flight_per_connection": 2,
        },
    )
    TaskQueueRepository.update_status(task_id, "paused")

    mock_sched = MagicMock()
    mock_sched._schedule_next_tasks = AsyncMock()
    mock_sched.is_task_running = MagicMock(return_value=False)
    mock_sched.get_max_global_concurrency.return_value = 10
    mock_sched.get_running_task_count.return_value = 0
    mock_sched.get_account_task_count.return_value = 0
    mock_sched.clear_pikpak_cooldown = MagicMock()

    with (
        patch("app.services.task_lifecycle_service.connection_manager.broadcast", new=AsyncMock()),
        patch(
            "app.services.task_lifecycle_service.prepare_task_resume",
            new=AsyncMock(return_value={"completed": False}),
        ),
    ):
        # 普通恢复
        res_normal = await TaskLifecycleService.resume_task(task_id, mode="normal", sched=mock_sched)
        assert res_normal["status"] in ("pending", "running", "queued")

        # 降档 (steady) 恢复: 并发压至 1
        TaskQueueRepository.update_status(task_id, "paused")
        res_steady = await TaskLifecycleService.resume_task(task_id, mode="steady", sched=mock_sched)
        assert res_steady["status"] in ("pending", "running", "queued")
        task_data = TaskQueueRepository.get_by_id(task_id)
        assert task_data is not None
        assert task_data["params"]["upload_concurrency"] == 1
        assert task_data["params"]["pikpak_prefetch_concurrency"] == 1


# ==============================================================================
# 4. 任务取消与删除 (cancel_task / delete_task)
# ==============================================================================


@pytest.mark.asyncio
async def test_cancel_task():
    task_id = "test_cancel_1"
    TaskQueueRepository.create(task_id, "forward_channel", {})
    TaskQueueRepository.update_status(task_id, "running")
    active_task_controls[task_id] = {"cancelled": False, "paused": False}

    with patch("app.services.task_lifecycle_service.connection_manager.broadcast", new=AsyncMock()):
        res = await TaskLifecycleService.cancel_task(task_id)
        assert res["status"] == "cancelled"
        assert active_task_controls[task_id]["cancelled"] is True
        assert TaskQueueRepository.get_status(task_id) == "cancelled"


@pytest.mark.asyncio
async def test_delete_task_with_and_without_files():
    task_id = "test_del_1"
    TaskQueueRepository.create(task_id, "download_link", {})
    TaskQueueRepository.update_status(task_id, "failed")

    with (
        patch("app.services.task_lifecycle_service.connection_manager.broadcast", new=AsyncMock()),
        patch("app.services.task_lifecycle_service.gc_task_meta") as mock_gc,
    ):
        res = await TaskLifecycleService.delete_task(task_id, delete_files=False)
        assert res["status"] == "deleted"
        assert TaskQueueRepository.get_by_id(task_id) is None
        mock_gc.assert_called_once()

    # 无法删除 running 状态的任务 (必须抛出 400)
    task_id_running = "test_del_running"
    TaskQueueRepository.create(task_id_running, "download_link", {})
    TaskQueueRepository.update_status(task_id_running, "running")
    with pytest.raises(HTTPException) as exc_info:
        await TaskLifecycleService.delete_task(task_id_running)
    assert exc_info.value.status_code == 400


# ==============================================================================
# 5. 重试、清理与并发更新 (retry_task / clear / update_concurrency)
# ==============================================================================


@pytest.mark.asyncio
async def test_retry_task():
    task_id = "test_retry_1"
    TaskQueueRepository.create(task_id, "download_link", {})
    TaskQueueRepository.update_status(task_id, "failed")

    mock_sched = MagicMock()
    mock_sched._schedule_next_tasks = AsyncMock()
    mock_sched.is_task_running = MagicMock(return_value=False)

    with patch("app.services.task_lifecycle_service.connection_manager.broadcast", new=AsyncMock()):
        res = await TaskLifecycleService.retry_task(task_id, sched=mock_sched)
        assert res["status"] == "queued"
        assert res["task_id"] == task_id


@pytest.mark.asyncio
async def test_retry_task_waits_for_draining_executor():
    """cancel 后执行器收尾中: retry 有界等待退出, 不替换控制条目 (防双执行器/幽灵复活)"""
    task_id = "test_retry_drain"
    TaskQueueRepository.create(task_id, "download_link", {})
    TaskQueueRepository.update_status(task_id, "cancelled")

    mock_sched = MagicMock()
    mock_sched._schedule_next_tasks = AsyncMock()
    mock_sched.is_task_running = MagicMock(return_value=True)

    with (
        patch("app.services.task_lifecycle_service.connection_manager.broadcast", new=AsyncMock()),
        patch("app.services.task_lifecycle_service.asyncio.sleep", new_callable=AsyncMock),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await TaskLifecycleService.retry_task(task_id, sched=mock_sched)
        assert exc_info.value.status_code == 409
    assert TaskQueueRepository.get_status(task_id) == "cancelled"  # 未被重排队


@pytest.mark.asyncio
async def test_clear_finished_tasks():
    TaskQueueRepository.create("t_comp", "download_link", {})
    TaskQueueRepository.update_status("t_comp", "completed")

    TaskQueueRepository.create("t_fail", "download_link", {})
    TaskQueueRepository.update_status("t_fail", "failed")

    TaskQueueRepository.create("t_run", "download_link", {})
    TaskQueueRepository.update_status("t_run", "running")

    res = await TaskLifecycleService.clear_finished_tasks()
    assert res["status"] == "cleared"
    assert TaskQueueRepository.get_by_id("t_comp") is None
    assert TaskQueueRepository.get_by_id("t_fail") is None
    assert TaskQueueRepository.get_by_id("t_run") is not None


@pytest.mark.asyncio
async def test_update_task_concurrency():
    task_id = "test_concurrency_1"
    TaskQueueRepository.create(task_id, "download_link", {"concurrency": 2, "threads": 2})

    req = UpdateTaskConcurrencyRequest(
        concurrency=6,
        in_flight_per_connection=3,
        prefetch_concurrency=4,
    )

    with patch("app.services.task_lifecycle_service.connection_manager.broadcast", new=AsyncMock()):
        res = await TaskLifecycleService.update_task_concurrency(task_id, req)
        assert res["status"] == "updated"
        assert res["concurrency"] == 6

        # 检查持久化库中参数更新
        task = TaskQueueRepository.get_by_id(task_id)
        assert task is not None
        assert task["params"]["threads"] == 6


# ==============================================================================
# 6. 批量操作 (batch_action)
# ==============================================================================


@pytest.mark.asyncio
async def test_batch_action():
    TaskQueueRepository.create("b1", "download_link", {})
    TaskQueueRepository.update_status("b1", "running")
    TaskQueueRepository.create("b2", "download_link", {})
    TaskQueueRepository.update_status("b2", "running")

    with patch("app.services.task_lifecycle_service.connection_manager.broadcast", new=AsyncMock()):
        # 批量暂停
        req_pause = BatchTaskActionRequest(action="pause", task_ids=["b1", "b2"])
        res_pause = await TaskLifecycleService.batch_action(req_pause)
        assert len(res_pause.results) == 2
        assert all(r.success for r in res_pause.results)

        # 批量取消
        req_cancel = BatchTaskActionRequest(action="cancel", task_ids=["b1", "b2"])
        res_cancel = await TaskLifecycleService.batch_action(req_cancel)
        assert len(res_cancel.results) == 2
        assert all(r.success for r in res_cancel.results)

        # 批量删除
        req_del = BatchTaskActionRequest(action="delete", task_ids=["b1", "b2"])
        res_del = await TaskLifecycleService.batch_action(req_del)
        assert len(res_del.results) == 2
        assert all(r.success for r in res_del.results)


# ==============================================================================
# 7. 频道路由权限预检 (precheck_access)
# ==============================================================================


@pytest.mark.asyncio
async def test_precheck_access_no_accounts():
    with patch("app.core.client_manager.client_manager.get_all_accounts", return_value=[]):
        req = PrecheckAccessRequest(chat_identifier="testchannel")
        res = await TaskLifecycleService.precheck_access(req)
        assert res.accessible is False
        assert "暂无任何已连接且可用的活跃 Userbot" in res.reason


@pytest.mark.asyncio
async def test_precheck_access_success():
    mock_acc = MagicMock()
    mock_acc.account_id = 1
    mock_acc.account_name = "Userbot1"
    mock_acc.is_ready.return_value = True
    mock_acc.is_active = True
    mock_acc.is_flood_locked.return_value = False
    mock_client = MagicMock()
    mock_client.is_connected.return_value = True
    mock_acc.client = mock_client

    mock_entity = MagicMock()
    mock_entity.id = 900000009
    mock_entity.title = "Target Channel"
    mock_entity.username = "target_chan"
    mock_entity.restricted = False
    mock_entity.noforwards = False

    with (
        patch("app.core.client_manager.client_manager.get_all_accounts", return_value=[mock_acc]),
        patch("app.core.client_manager.client_manager._accounts", {1: mock_acc}),
        patch("app.services.forward_service.ForwardService.resolve_entity", new=AsyncMock(return_value=mock_entity)),
    ):
        req = PrecheckAccessRequest(chat_identifier="https://t.me/target_chan", account_id=1)
        res = await TaskLifecycleService.precheck_access(req)
        assert res.accessible is True
        assert res.chat_id == "900000009"
        assert res.username == "target_chan"
        assert "Userbot1" in res.title


# ==============================================================================
# 8. 跳过当前文件 (skip_current_file)
# ==============================================================================


@pytest.mark.asyncio
async def test_skip_current_file_active_and_inactive():
    task_id = "test_skip_1"
    TaskQueueRepository.create(
        task_id,
        "pikpak_publish",
        {
            "folder_path": "/test",
            "target_chat": "chan",
            "remote_files": [{"file_id": "f_1", "name": "f1.mp4", "size": 1000}],
        },
    )
    TaskQueueRepository.update_status(task_id, "paused")

    # 协程处于活跃控制中
    active_task_controls[task_id] = {"skip_file": False, "skip_reason": None}

    req = SkipCurrentFileRequest(file_id="f_1", reason="File damaged")
    with patch("app.services.task_lifecycle_service.connection_manager.broadcast", new=AsyncMock()):
        res = await TaskLifecycleService.skip_current_file(task_id, req)
        assert res.status == "paused"
        assert "已成功跳过文件" in res.message
        # 验证 skipped_files 账本已写入
        task = TaskQueueRepository.get_by_id(task_id)
        assert task is not None
        assert len(task["params"].get("skipped_files", [])) == 1


async def test_retry_rejects_status_change_during_drain():
    task_id = "retry_cancel_race"
    TaskQueueRepository.create(task_id, "download_link", {})
    TaskQueueRepository.update_status(task_id, "paused")
    sched = MagicMock()
    sched.is_task_running.side_effect = [True, True, False]

    async def cancel_during_wait(_delay):
        TaskQueueRepository.update_status(task_id, "cancelled")

    with (
        patch("app.services.task_lifecycle_service.asyncio.sleep", side_effect=cancel_during_wait),
        pytest.raises(HTTPException) as error,
    ):
        await TaskLifecycleService.retry_task(task_id, sched=sched)
    assert error.value.status_code == 409
    assert TaskQueueRepository.get_status(task_id) == "cancelled"


def test_parse_precheck_target_delegates_to_shared_parser():
    assert _parse_precheck_target("https://t.me/s/demo_channel/42") == "demo_channel", "/s/ 前缀不得取到 's'"
    assert _parse_precheck_target("https://t.me/joinchat/ABCDEF") == "+ABCDEF", "邀请链接取统一邀请格式"
    assert _parse_precheck_target("https://t.me/c/9000000010/42") == -1009000000010
    assert _parse_precheck_target("@demo_channel") == "@demo_channel"
    assert _parse_precheck_target("https://t.me/c/abc") == "c", "畸形 /c/ 标识符不得在 probe 前 int() 抛异常"


@pytest.mark.asyncio
async def test_resume_cancelled_task_rejects_409_while_executor_draining(monkeypatch):
    monkeypatch.setattr("app.services.task_lifecycle_service.asyncio.sleep", AsyncMock())
    task_id = "resume_drain_409"
    TaskQueueRepository.create(task_id, "download_link", {"links": ["https://t.me/c/123/1"]})
    TaskQueueRepository.update_status(task_id, "cancelled")
    active_task_controls[task_id] = {"cancelled": True, "paused": False}

    mock_sched = MagicMock()
    mock_sched.is_task_running = MagicMock(return_value=True)

    with pytest.raises(HTTPException) as exc_info:
        await TaskLifecycleService.resume_task(task_id, sched=mock_sched, drain_polls=2)

    assert exc_info.value.status_code == 409
    assert TaskQueueRepository.get_status(task_id) == "cancelled"
    assert active_task_controls[task_id]["cancelled"] is True


@pytest.mark.asyncio
async def test_resume_cancelled_task_succeeds_after_executor_drains(monkeypatch):
    monkeypatch.setattr("app.services.task_lifecycle_service.asyncio.sleep", AsyncMock())
    task_id = "resume_drain_ok"
    TaskQueueRepository.create(task_id, "download_link", {"links": ["https://t.me/c/123/1"]})
    TaskQueueRepository.update_status(task_id, "cancelled")
    active_task_controls[task_id] = {"cancelled": True, "paused": False}

    mock_sched = MagicMock()
    mock_sched.is_running = True
    mock_sched.is_task_running = MagicMock(side_effect=[True, False])
    mock_sched.get_max_global_concurrency.return_value = 5
    mock_sched.get_running_task_count.return_value = 0
    mock_sched.get_account_task_count.return_value = 0

    with patch("app.services.task_lifecycle_service.connection_manager.broadcast", new=AsyncMock()):
        res = await TaskLifecycleService.resume_task(task_id, sched=mock_sched, drain_polls=3)

    assert res["status"] == "queued"
    assert TaskQueueRepository.get_status(task_id) == "queued"
    assert task_id not in active_task_controls


@pytest.mark.asyncio
async def test_batch_action_resume_fails_fast_on_draining_executor(monkeypatch):
    monkeypatch.setattr("app.services.task_lifecycle_service.asyncio.sleep", AsyncMock())
    task_id = "batch_drain_1"
    TaskQueueRepository.create(task_id, "download_link", {"links": ["https://t.me/c/123/1"]})
    TaskQueueRepository.update_status(task_id, "cancelled")

    mock_sched = MagicMock()
    mock_sched.is_task_running = MagicMock(return_value=True)

    res = await TaskLifecycleService.batch_action(
        BatchTaskActionRequest(action="resume", task_ids=[task_id]),
        sched=mock_sched,
    )
    assert res.success_count == 0
    assert res.results[0].success is False
    assert "尚未完全退出" in (res.results[0].error or "")


@pytest.mark.asyncio
async def test_clear_finished_excludes_draining_running_executors():
    TaskQueueRepository.create("t_done", "download_link", {})
    TaskQueueRepository.update_status("t_done", "completed")
    TaskQueueRepository.create("t_dead", "download_link", {})
    TaskQueueRepository.update_status("t_dead", "failed")
    TaskQueueRepository.create("t_draining", "download_link", {})
    TaskQueueRepository.update_status("t_draining", "cancelled")
    DownloadBreakpointRepository.upsert("t_draining", "-1001", 1, "/tmp/1.part", "/tmp/1.meta", "/tmp/1.mp4")
    active_task_controls["t_draining"] = {"cancelled": True, "paused": False}

    mock_sched = MagicMock()
    mock_sched.get_running_task_ids.return_value = frozenset({"t_draining"})
    mock_sched.is_task_running.side_effect = lambda tid: tid == "t_draining"

    with (
        patch("app.services.task_lifecycle_service._sync_batch_gc") as mock_gc,
        patch("app.services.task_lifecycle_service.connection_manager.broadcast", new=AsyncMock()),
    ):
        res = await TaskLifecycleService.clear_finished_tasks(sched=mock_sched)

    assert res == {"status": "cleared"}
    assert TaskQueueRepository.get_by_id("t_done") is None
    assert TaskQueueRepository.get_by_id("t_dead") is None
    assert TaskQueueRepository.get_by_id("t_draining") is not None
    assert DownloadBreakpointRepository.get_breakpoint_paths_by_task("t_draining") == {
        "part": ["/tmp/1.part"],
        "meta": ["/tmp/1.meta"],
        "final": ["/tmp/1.mp4"],
    }
    assert active_task_controls["t_draining"]["cancelled"] is True
    mock_gc.assert_called_once()
    assert set(mock_gc.call_args.args[0]) == {"t_done", "t_dead"}


@pytest.mark.asyncio
async def test_drain_guard_accepts_executor_exiting_during_final_sleep(monkeypatch):
    monkeypatch.setattr("app.services.task_lifecycle_service.asyncio.sleep", AsyncMock())
    task_id = "drain_final_sleep_ok"
    TaskQueueRepository.create(task_id, "download_link", {"links": ["https://t.me/c/123/1"]})
    TaskQueueRepository.update_status(task_id, "cancelled")

    mock_sched = MagicMock()
    mock_sched.is_running = True
    mock_sched.is_task_running = MagicMock(side_effect=[True, True, True, False])
    mock_sched.get_max_global_concurrency.return_value = 5
    mock_sched.get_running_task_count.return_value = 0
    mock_sched.get_account_task_count.return_value = 0

    with patch("app.services.task_lifecycle_service.connection_manager.broadcast", new=AsyncMock()):
        res = await TaskLifecycleService.resume_task(task_id, sched=mock_sched, drain_polls=2)

    assert res["status"] == "queued"


@pytest.mark.asyncio
async def test_retry_task_rechecks_after_prepare_and_handles_cas_rejection():
    task_id = "retry_race_1"
    TaskQueueRepository.create(task_id, "download_link", {"links": ["https://t.me/c/123/1"]})
    TaskQueueRepository.update_status(task_id, "failed")
    active_task_controls[task_id] = {"cancelled": False, "paused": False}

    mock_sched = MagicMock()
    mock_sched.is_task_running = MagicMock(return_value=False)

    async def concurrent_resume(_task, *, persist):
        assert not persist
        TaskQueueRepository.update_status(task_id, "running")
        return {"completed": False}

    with (
        patch("app.services.task_lifecycle_service.prepare_task_resume", side_effect=concurrent_resume),
        pytest.raises(HTTPException) as exc_info,
    ):
        await TaskLifecycleService.retry_task(task_id, sched=mock_sched)

    assert exc_info.value.status_code == 409
    assert task_id in active_task_controls

    task_id2 = "retry_cas_false"
    TaskQueueRepository.create(task_id2, "download_link", {"links": ["https://t.me/c/123/1"]})
    TaskQueueRepository.update_status(task_id2, "failed")
    with (
        patch(
            "app.services.task_lifecycle_service.prepare_task_resume", new=AsyncMock(return_value={"completed": False})
        ),
        patch(
            "app.services.task_lifecycle_service.TaskQueueRepository.retry_task",
            return_value=False,
        ),
        pytest.raises(HTTPException) as exc_info2,
    ):
        await TaskLifecycleService.retry_task(task_id2, sched=mock_sched)
    assert exc_info2.value.status_code == 409


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "initial_status", "live_after_claim"),
    [("resume", "paused", True), ("resume", "cancelled", False), ("queue", "paused", False), ("queue", "paused", True)],
)
async def test_stale_resume_preserves_new_executor_cancellation(monkeypatch, action, initial_status, live_after_claim):
    task_id = "resume-ownership"
    TaskQueueRepository.create(task_id, "download_link", {"links": ["https://t.me/example/1"]}, status=initial_status)
    sched = MagicMock()
    sched.is_task_running.return_value = False
    sched.get_max_global_concurrency.return_value = 5
    sched.get_running_task_count.return_value = 0
    sched.get_account_task_count.return_value = 0
    entered = asyncio.Event()
    release = asyncio.Event()

    async def delay_first_prepare(task, *, persist=True):
        if not entered.is_set():
            entered.set()
            await release.wait()
        return await prepare_task_resume(task, persist=persist)

    monkeypatch.setattr("app.services.task_lifecycle_service.prepare_task_resume", delay_first_prepare)
    monkeypatch.setattr("app.services.task_lifecycle_service.connection_manager.broadcast", AsyncMock())

    stale_request = (
        TaskLifecycleService.resume_queue(sched=sched)
        if action == "queue"
        else TaskLifecycleService.resume_task(task_id, sched=sched)
    )
    stale = asyncio.create_task(stale_request)
    try:
        await asyncio.wait_for(entered.wait(), timeout=3)
        assert (await TaskLifecycleService.resume_task(task_id, sched=sched))["status"] == "queued"
        assert await asyncio.to_thread(acquire_queued_task, task_id, None)
        sched.is_task_running.return_value = live_after_claim
        await TaskLifecycleService.cancel_task(task_id)
        assert is_task_cancelled(task_id)
        release.set()
        if action == "queue":
            assert (await stale)["resumed_count"] == 0
        else:
            with pytest.raises(HTTPException) as error:
                await stale
            assert error.value.status_code == 409
    finally:
        if not stale.done():
            stale.cancel()
        await asyncio.gather(stale, return_exceptions=True)
    assert TaskQueueRepository.get_status(task_id) == "cancelled"
    assert is_task_cancelled(task_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("finished", [False, True])
async def test_pause_cleans_only_finished_newly_registered_executor(monkeypatch, finished):
    task_id = "pause-during-registration"
    TaskQueueRepository.create(task_id, "download_link", {}, status="running")
    sched = TaskScheduler()
    release = asyncio.Event()
    executor = asyncio.create_task(release.wait())
    if finished:
        release.set()
        await executor

    def register_and_stop(tid, state, allowed):
        sched._running_tasks[tid] = executor
        set_task_control(tid, paused=False)
        return stop_task_with_controls(tid, state, allowed)

    monkeypatch.setattr("app.services.task_lifecycle_service.stop_task_with_controls", register_and_stop)
    monkeypatch.setattr("app.services.task_lifecycle_service.connection_manager.broadcast", AsyncMock())
    try:
        assert await TaskLifecycleService.pause_task(task_id, sched=sched) == {"status": "paused"}
        assert sched._running_tasks.get(task_id) is (None if finished else executor)
        assert is_task_paused(task_id) is not finished
    finally:
        release.set()
        await executor


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "initial_status"),
    [
        ("pikpak", "paused"),
        ("pikpak", "cancelled"),
        ("single", "paused"),
        ("queue", "paused"),
        ("queue-live", "paused"),
        ("queue-inactive", "paused"),
    ],
)
async def test_stale_resume_rejects_same_status_at_new_version(monkeypatch, action, initial_status):
    task_id = "resume-new-version"
    if action == "pikpak":
        params = {"target_channel": "example", "remote_files": [{"file_id": "f1"}]}
    else:
        params = {
            "channel": "example",
            "start_id": 1,
            "end_id": 20,
            "contiguous_cursor_id": 10 if action in ("queue-live", "queue-inactive") else 20,
        }
    TaskQueueRepository.create(
        task_id, "pikpak_publish" if action == "pikpak" else "download_range", params, status=initial_status
    )
    sched = TaskScheduler()
    entered = asyncio.Event()
    release = asyncio.Event()
    finish_executor = asyncio.Event()

    async def delay_prepare(task, *, persist):
        assert not persist
        entered.set()
        await release.wait()
        return await prepare_task_resume(task, persist=persist)

    monkeypatch.setattr("app.services.task_lifecycle_service.prepare_task_resume", delay_prepare)
    broadcast = AsyncMock()
    monkeypatch.setattr("app.services.task_lifecycle_service.connection_manager.broadcast", broadcast)
    request = (
        TaskLifecycleService.resume_queue(sched)
        if action.startswith("queue")
        else TaskLifecycleService.resume_task(task_id, mode="steady" if action == "pikpak" else "normal", sched=sched)
    )
    stale = asyncio.create_task(request)
    try:
        await asyncio.wait_for(entered.wait(), timeout=3)
        assert TaskQueueRepository.requeue_task_for_resume(
            task_id, initial_status, params_patch={"contiguous_cursor_id": 2}
        )
        assert await asyncio.to_thread(acquire_queued_task, task_id, None)
        executor = None
        if action != "queue-inactive":
            executor = asyncio.create_task(finish_executor.wait())
            sched._running_tasks[task_id] = executor
        assert await asyncio.to_thread(stop_task_with_controls, task_id, initial_status, ("running",))
        await TaskQueueRepository.set_error_detail_async(task_id, "new run")
        current = TaskQueueRepository.get_by_id(task_id)
        release.set()

        if action.startswith("queue"):
            assert (await stale)["resumed_count"] == 0
        else:
            with pytest.raises(HTTPException) as error:
                await stale
            assert error.value.status_code == 409

        assert TaskQueueRepository.get_by_id(task_id) == current
        assert sched._running_tasks.get(task_id) is executor
        assert active_task_controls[task_id][initial_status] is True
        broadcast.assert_not_awaited()
    finally:
        if not release.is_set():
            stale.cancel()
        release.set()
        await asyncio.gather(stale, return_exceptions=True)
        finish_executor.set()
        if executor := sched._running_tasks.get(task_id):
            await executor


@pytest.mark.asyncio
@pytest.mark.parametrize(("mode", "exit_during_commit"), [("queue", False), ("normal", False), ("steady", True)])
async def test_live_resume_commits_changes_before_pause_exit(monkeypatch, mode, exit_during_commit):
    task_id = "live-resume-write"
    if mode == "queue":
        task_type = "download_range"
        params = {"channel": "example", "start_id": 1, "end_id": 20, "contiguous_cursor_id": 4}
    else:
        task_type = "pikpak_publish"
        params = {"upload_concurrency": 4}
    TaskQueueRepository.create(task_id, task_type, params, status="paused")
    await TaskQueueRepository.set_error_detail_async(task_id, "before resume")
    set_task_control(task_id, paused=True)
    sched = TaskScheduler()
    release = asyncio.Event()
    loop = asyncio.get_running_loop()

    async def pause_and_exit(*_args):
        await release.wait()
        raise TaskPausedError("pause already observed")

    def resume_and_release(tid, version, **changes):
        assert resume_paused_executor(tid, version, **changes)
        row = TaskQueueRepository.get_by_id(tid)
        assert row["status"] == "running"
        if mode == "queue":
            assert row["params"]["start_id"] == 5
            assert row["error_detail"] == "before resume"
        else:
            assert row["error_detail"] == ""
            assert row["params"]["upload_concurrency"] == (1 if mode == "steady" else 4)
        loop.call_soon_threadsafe(release.set)
        return True

    monkeypatch.setattr("app.services.task_lifecycle_service.resume_paused_executor", resume_and_release)
    monkeypatch.setattr("app.services.task_lifecycle_service.connection_manager.broadcast", AsyncMock())
    if exit_during_commit:
        resume_paused_task = TaskQueueRepository.resume_paused_task

        def mark_exit_before_write(tid, version, **changes):
            mark_task_pause_exit(tid)
            return resume_paused_task(tid, version, **changes)

        monkeypatch.setattr(TaskQueueRepository, "resume_paused_task", mark_exit_before_write)

    sched.register_executor(task_type, pause_and_exit)
    wrapper = asyncio.create_task(sched._run_task_wrapper(task_id, task_type, params, None, 0, 0))
    sched._running_tasks[task_id] = wrapper
    try:
        if mode == "queue":
            assert (await TaskLifecycleService.resume_queue(sched))["resumed_count"] == 1
        else:
            assert (await TaskLifecycleService.resume_task(task_id, mode, sched))["status"] == "running"
    finally:
        release.set()
        await asyncio.wait_for(wrapper, timeout=3)

    assert TaskQueueRepository.get_status(task_id) == "queued"
    assert not sched.is_task_running(task_id)


@pytest.mark.asyncio
async def test_pikpak_safe_pause_rejects_resume_until_executor_exits(monkeypatch):
    task_id = "pikpak-safe-pause-exit"
    params = {"target_channel": "example", "remote_files": [{"file_id": "f1"}], "throttle_strike_count": 2}
    TaskQueueRepository.create(task_id, "pikpak_publish", params, status="running")
    set_task_control(task_id, paused=False)
    sched = TaskScheduler()
    pause_written = asyncio.Event()
    release = asyncio.Event()
    pause_for_risk = TaskQueueRepository.pause_pikpak_for_risk_async

    async def throttle(*_args):
        raise PikPakPublishThrottledError()

    async def hold_after_pause(tid, params_patch, error_detail):
        updated = await pause_for_risk(tid, params_patch, error_detail)
        pause_written.set()
        await release.wait()
        return updated

    monkeypatch.setattr(TaskQueueRepository, "pause_pikpak_for_risk_async", hold_after_pause)
    monkeypatch.setattr("app.services.task_lifecycle_service.connection_manager.broadcast", AsyncMock())
    sched.register_executor("pikpak_publish", throttle)
    wrapper = asyncio.create_task(sched._run_task_wrapper(task_id, "pikpak_publish", params, None, 0, 0))
    sched._running_tasks[task_id] = wrapper
    try:
        await asyncio.wait_for(pause_written.wait(), timeout=3)
        with pytest.raises(HTTPException) as error:
            await TaskLifecycleService.resume_task(task_id, sched=sched)
        assert error.value.status_code == 409
        assert (await TaskLifecycleService.resume_queue(sched))["resumed_count"] == 0
    finally:
        release.set()
        await asyncio.wait_for(wrapper, timeout=3)

    assert TaskQueueRepository.get_status(task_id) == "paused"
    assert not sched.is_task_running(task_id)
    assert (await TaskLifecycleService.resume_task(task_id, sched=sched))["status"] == "queued"


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_first", [False, True])
async def test_pikpak_safe_pause_keeps_cooldown_and_cancellation(monkeypatch, cancel_first):
    task_id = "pikpak-safe-pause-cancel"
    TaskQueueRepository.create(task_id, "pikpak_publish", {"throttle_strike_count": 2}, status="running")
    sched = TaskScheduler()
    pause_for_risk = TaskQueueRepository.pause_pikpak_for_risk_async
    broadcast = AsyncMock()

    async def cancel_near_pause(tid, params_patch, error_detail):
        if cancel_first:
            await TaskLifecycleService.cancel_task(tid)
        updated = await pause_for_risk(tid, params_patch, error_detail)
        if not cancel_first:
            await TaskLifecycleService.cancel_task(tid)
        return updated

    monkeypatch.setattr(TaskQueueRepository, "pause_pikpak_for_risk_async", cancel_near_pause)
    monkeypatch.setattr("app.services.task_lifecycle_service.connection_manager.broadcast", broadcast)
    await sched._handle_pikpak_tiered_cooldown(task_id, 0, 3, "throttled", None, PikPakPublishThrottledError())

    row = TaskQueueRepository.get_by_id(task_id)
    assert sched.is_pikpak_in_cooldown()
    assert row["status"] == "cancelled"
    assert bool(row["params"].get("safe_paused")) is not cancel_first
    assert bool(row["error_detail"]) is not cancel_first
    statuses = [
        call.args[0].get("status") for call in broadcast.await_args_list if call.args[0]["type"] == "task_update"
    ]
    assert statuses == (["cancelled"] if cancel_first else ["cancelled", None])


@pytest.mark.asyncio
@pytest.mark.parametrize("intervening", ["settings", "pause", "cancel"])
async def test_deferred_resume_keeps_settings_and_respects_later_stops(monkeypatch, intervening):
    task_id = "resume-intent"
    TaskQueueRepository.create(task_id, "download_range", {"channel": "example"}, status="paused")
    set_task_control(task_id, paused=True)
    sched = TaskScheduler()
    release = asyncio.Event()
    requeue = sched._requeue_resumed_pause_exit

    async def pause_and_exit(*_args):
        await release.wait()
        raise TaskPausedError("pause already observed")

    async def update_before_requeue(tid, resume_controls):
        if intervening == "settings":
            await TaskLifecycleService.update_task_concurrency(tid, UpdateTaskConcurrencyRequest(concurrency=3), sched)
        elif intervening == "pause":
            await TaskLifecycleService.pause_task(tid, sched=sched)
        else:
            await TaskLifecycleService.cancel_task(tid)
        await requeue(tid, resume_controls)

    monkeypatch.setattr(sched, "_requeue_resumed_pause_exit", update_before_requeue)
    monkeypatch.setattr("app.services.task_lifecycle_service.connection_manager.broadcast", AsyncMock())
    sched.register_executor("download_range", pause_and_exit)
    wrapper = asyncio.create_task(sched._run_task_wrapper(task_id, "download_range", {}, None, 0, 0))
    sched._running_tasks[task_id] = wrapper
    try:
        assert (await TaskLifecycleService.resume_task(task_id, sched=sched))["status"] == "running"
    finally:
        release.set()
        await asyncio.wait_for(wrapper, timeout=3)

    row = TaskQueueRepository.get_by_id(task_id)
    assert row["status"] == {"settings": "queued", "pause": "paused", "cancel": "cancelled"}[intervening]
    if intervening == "settings":
        assert row["params"]["threads"] == 3
    assert task_id not in active_task_controls


@pytest.mark.asyncio
@pytest.mark.parametrize("retryable", [False, True])
async def test_exiting_download_rejects_live_resume_and_duplicate_dispatch(monkeypatch, retryable):
    task_id = "resume-during-executor-exit"
    params = {"links": ["https://t.me/example/1"]}
    TaskQueueRepository.create(
        task_id, "download_link", params, account_id=9, status="running" if retryable else "paused"
    )
    set_task_control(task_id, paused=not retryable)
    sched = TaskScheduler(check_interval=0.05)
    account = MagicMock(account_id=9, is_active=True, max_concurrency=2)
    account.is_flood_locked.return_value = False
    monkeypatch.setattr("app.core.scheduler.client_manager.get_account", lambda _id: account)
    monkeypatch.setattr("app.services.task_lifecycle_service.connection_manager.broadcast", AsyncMock())
    in_teardown = asyncio.Event()
    release = asyncio.Event()
    get_task = TaskQueueRepository.get_by_id_async
    append_log = TaskQueueRepository.append_log_async

    async def get_status_then_hold(tid):
        task = await get_task(tid)
        if asyncio.current_task() is wrapper:
            in_teardown.set()
            await release.wait()
        return task

    async def hold_retry_log(tid, message):
        if "injected download failure" in message:
            in_teardown.set()
            await release.wait()
        await append_log(tid, message)

    error = (
        RetryableTaskError("injected download failure", is_flood_wait=False)
        if retryable
        else RuntimeError("injected download failure")
    )
    executor = AsyncMock(side_effect=[error, None])
    sched.register_executor("download_link", executor)
    if retryable:
        monkeypatch.setattr(TaskQueueRepository, "append_log_async", hold_retry_log)
    else:
        monkeypatch.setattr(TaskQueueRepository, "get_by_id_async", get_status_then_hold)
    wrapper = asyncio.create_task(sched._run_task_wrapper(task_id, "download_link", params, 9, 0, 2))
    sched._running_tasks[task_id] = wrapper
    try:
        await asyncio.wait_for(in_teardown.wait(), timeout=3)
        if retryable:
            assert (await TaskLifecycleService.resume_task(task_id, sched=sched))["status"] == "queued"
            await sched._scan_queued_candidates(3, [])
            assert sched._running_tasks.get(task_id) is wrapper
            executor.assert_awaited_once()
            await TaskLifecycleService.pause_task(task_id, sched=sched)
        with pytest.raises(HTTPException) as rejected:
            await TaskLifecycleService.resume_task(task_id, sched=sched)
        assert rejected.value.status_code == 409
        assert (await TaskLifecycleService.resume_queue(sched))["resumed_count"] == 0
        assert TaskQueueRepository.get_status(task_id) == "paused"
    finally:
        release.set()
        await asyncio.wait_for(wrapper, timeout=3)

    assert (await TaskLifecycleService.resume_task(task_id, sched=sched))["status"] == "queued"


@pytest.mark.asyncio
async def test_publish_teardown_preserves_accepted_resume(monkeypatch):
    task_id = "publish-teardown"
    TaskQueueRepository.create(task_id, "pikpak_publish", {}, status="paused")
    set_task_control(task_id, paused=True)
    sched = TaskScheduler()
    release = asyncio.Event()

    async def pause_and_exit(*_args):
        await release.wait()
        await _publish_terminal(task_id, "paused", "publishing paused")
        raise UploadPausedError("publishing paused")

    monkeypatch.setattr("app.services.task_lifecycle_service.connection_manager.broadcast", AsyncMock())
    sched.register_executor("pikpak_publish", pause_and_exit)
    wrapper = asyncio.create_task(sched._run_task_wrapper(task_id, "pikpak_publish", {}, None, 0, 0))
    sched._running_tasks[task_id] = wrapper
    try:
        assert (await TaskLifecycleService.resume_task(task_id, mode="steady", sched=sched))["status"] == "running"
    finally:
        release.set()
        await asyncio.wait_for(wrapper, timeout=3)

    row = TaskQueueRepository.get_by_id(task_id)
    assert row["status"] == "queued"
    assert row["params"]["upload_concurrency"] == 1


@pytest.mark.parametrize("remove_controls", [False, True])
def test_rejected_resume_preserves_fields_during_executor_exit(monkeypatch, remove_controls):
    task_id = "resume-lost-controls"
    TaskQueueRepository.create(task_id, "pikpak_publish", {"upload_concurrency": 4}, status="paused")
    TaskQueueRepository.set_error_detail(task_id, "keep this error")
    set_task_control(task_id, paused=True)
    before = TaskQueueRepository.get_by_id(task_id)
    resume = TaskQueueRepository.resume_paused_task

    def exit_before_commit(tid, version, **kwargs):
        if remove_controls:
            active_task_controls.pop(tid)
        else:
            mark_task_executor_exit(tid)
        return resume(tid, version, **kwargs)

    monkeypatch.setattr(TaskQueueRepository, "resume_paused_task", exit_before_commit)
    assert not resume_paused_executor(
        task_id, before["updated_at"], params_patch={"upload_concurrency": 1}, error_detail=""
    )
    assert TaskQueueRepository.get_by_id(task_id) == before


@pytest.mark.asyncio
async def test_manual_resume_clears_inactive_task_retry_deadline(monkeypatch):
    task_id = "manual-resume-retry-deadline"
    TaskQueueRepository.create(task_id, "download_link", {"links": ["https://t.me/example/1"]}, status="queued")
    assert TaskQueueRepository.mark_retry(task_id, 1, time.time() + 3600)
    sched = TaskScheduler()
    monkeypatch.setattr("app.services.task_lifecycle_service.connection_manager.broadcast", AsyncMock())
    await TaskLifecycleService.pause_task(task_id, sched=sched)
    assert (await TaskLifecycleService.resume_task(task_id, sched=sched))["status"] == "queued"
    assert any(row["id"] == task_id for row in TaskQueueRepository.get_queued_candidates())
    assert TaskQueueRepository.get_by_id(task_id)["retry_count"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("new_status", ["queued", "cancelled"])
async def test_queued_steady_resume_rejects_stale_version_without_mutation(monkeypatch, new_status):
    task_id = "queued-stale-steady-resume"
    TaskQueueRepository.create(task_id, "pikpak_publish", {"upload_concurrency": 4}, status="queued")
    sched = TaskScheduler()
    reset = TaskQueueRepository.reset_task_for_retry_async

    async def advance_before_reset(tid, **kwargs):
        assert await asyncio.to_thread(acquire_queued_task, tid, None)
        if new_status == "queued":
            assert await TaskQueueRepository.mark_retry_async(tid, 3, time.time() + 3600, "new retry")
        else:
            await TaskLifecycleService.cancel_task(tid)
        return await reset(tid, **kwargs)

    monkeypatch.setattr(TaskQueueRepository, "reset_task_for_retry_async", advance_before_reset)
    monkeypatch.setattr("app.services.task_lifecycle_service.connection_manager.broadcast", AsyncMock())
    with pytest.raises(HTTPException) as error:
        await TaskLifecycleService.resume_task(task_id, mode="steady", sched=sched)
    assert error.value.status_code == 409
    row = TaskQueueRepository.get_by_id(task_id)
    assert row["status"] == new_status
    assert row["params"]["upload_concurrency"] == 4
    if new_status == "queued":
        assert row["retry_count"] == 3 and row["next_retry_at"] > time.time()


@pytest.mark.asyncio
@pytest.mark.parametrize("completed", [False, True])
async def test_stale_retry_rejects_same_status_at_new_version(monkeypatch, completed):
    task_id = "retry-ownership"
    params = {"channel": "example", "start_id": 1, "end_id": 20}
    TaskQueueRepository.create(task_id, "download_range", params, status="cancelled")
    sched = MagicMock()
    sched.is_task_running.return_value = False
    entered = asyncio.Event()
    release = asyncio.Event()

    async def delay_prepare(_task, *, persist):
        assert not persist
        entered.set()
        await release.wait()
        return {"completed": completed, "patch": {"start_id": 11}}

    monkeypatch.setattr("app.services.task_lifecycle_service.prepare_task_resume", delay_prepare)
    monkeypatch.setattr("app.services.task_lifecycle_service.connection_manager.broadcast", AsyncMock())
    stale = asyncio.create_task(TaskLifecycleService.retry_task(task_id, sched=sched))
    try:
        await asyncio.wait_for(entered.wait(), timeout=3)
        assert TaskQueueRepository.requeue_task_for_resume(task_id, "cancelled", params_patch={})
        assert await asyncio.to_thread(acquire_queued_task, task_id, None)
        assert (await TaskLifecycleService.cancel_task(task_id))["status"] == "cancelled"
        release.set()
        with pytest.raises(HTTPException) as error:
            await stale
        assert error.value.status_code == 409
    finally:
        if not stale.done():
            stale.cancel()
        await asyncio.gather(stale, return_exceptions=True)
    assert TaskQueueRepository.get_status(task_id) == "cancelled"
    assert TaskQueueRepository.get_by_id(task_id)["params"] == params
    assert is_task_cancelled(task_id)


@pytest.mark.asyncio
async def test_retry_applies_range_checkpoint_with_original_version():
    task_id = "retry-range-checkpoint"
    params = {"channel": "example", "start_id": 1, "end_id": 20, "contiguous_cursor_id": 10}
    TaskQueueRepository.create(task_id, "download_range", params, status="failed")
    sched = MagicMock()
    sched.is_task_running.return_value = False

    with patch("app.services.task_lifecycle_service.connection_manager.broadcast", new=AsyncMock()):
        result = await TaskLifecycleService.retry_task(task_id, sched=sched)

    assert result["status"] == "queued"
    assert TaskQueueRepository.get_by_id(task_id)["params"] == {**params, "start_id": 11}


@pytest.mark.asyncio
async def test_clear_finished_preserves_controls_if_executor_running_at_cleanup():
    TaskQueueRepository.create("t_raced", "download_link", {})
    TaskQueueRepository.update_status("t_raced", "cancelled")
    active_task_controls["t_raced"] = {"cancelled": True, "paused": False}

    mock_sched = MagicMock()
    mock_sched.get_running_task_ids.return_value = frozenset()
    mock_sched.is_task_running.side_effect = lambda tid: tid == "t_raced"

    with (
        patch("app.services.task_lifecycle_service._sync_batch_gc"),
        patch("app.services.task_lifecycle_service.connection_manager.broadcast", new=AsyncMock()),
    ):
        await TaskLifecycleService.clear_finished_tasks(sched=mock_sched)

    assert active_task_controls["t_raced"]["cancelled"] is True


@pytest.mark.asyncio
async def test_resume_task_rejects_unknown_mode():
    with pytest.raises(HTTPException) as exc_info:
        await TaskLifecycleService.resume_task("any_task", mode="steayd")
    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_skip_current_file_rejects_concurrent_retry_before_writes(monkeypatch):
    task_id = "skip_retry_race"
    TaskQueueRepository.create(
        task_id,
        "pikpak_publish",
        {
            "target_channel": "-100123",
            "remote_files": [{"file_id": "fid_1", "name": "demo.mp4", "size_bytes": 100}],
        },
        status="failed",
    )
    ensure_slot = AsyncMock(return_value=999)
    monkeypatch.setattr("app.services.task_lifecycle_service._ensure_skip_placeholder_slot", ensure_slot)
    monkeypatch.setattr(
        "app.services.task_lifecycle_service.task_scheduler.is_task_running",
        lambda _tid: False,
    )

    # 1. 在发占位卡前任务状态被重试改变 -> 409, 不发卡不写历史
    orig_get = TaskQueueRepository.get_by_id_async
    calls = 0

    async def mutate_before_slot(tid):
        nonlocal calls
        calls += 1
        if calls == 2:
            TaskQueueRepository.requeue_task_for_resume(tid, "failed", params_patch={})
        return await orig_get(tid)

    monkeypatch.setattr(TaskQueueRepository, "get_by_id_async", mutate_before_slot)
    with pytest.raises(HTTPException) as exc1:
        await TaskLifecycleService.skip_current_file(task_id, SkipCurrentFileRequest(file_id="fid_1"))
    assert exc1.value.status_code == 409
    ensure_slot.assert_not_awaited()
    assert not PublishHistoryRepository.is_published("-100123", "", 0, source_type="pikpak", remote_file_id="fid_1")

    # 2. 发卡期间重试，已经提交的跳过记录对重试可见
    monkeypatch.setattr(TaskQueueRepository, "get_by_id_async", orig_get)
    task_id2 = "skip_retry_race_during_slot"
    TaskQueueRepository.create(
        task_id2,
        "pikpak_publish",
        {
            "target_channel": "-100123",
            "remote_files": [{"file_id": "fid_2", "name": "demo2.mp4", "size_bytes": 100}],
        },
        status="failed",
    )

    async def mutate_during_slot(*_args, **_kwargs):
        TaskQueueRepository.requeue_task_for_resume(task_id2, "failed", params_patch={})
        return 888

    monkeypatch.setattr("app.services.task_lifecycle_service._ensure_skip_placeholder_slot", mutate_during_slot)
    await TaskLifecycleService.skip_current_file(task_id2, SkipCurrentFileRequest(file_id="fid_2"))
    assert PublishHistoryRepository.is_published("-100123", "", 0, source_type="pikpak", remote_file_id="fid_2")
    assert len(TaskQueueRepository.get_by_id(task_id2)["params"]["skipped_files"]) == 1


@pytest.mark.asyncio
async def test_delete_task_preserves_partial_cleanup_results_on_failure(monkeypatch):
    task_id = "del_partial_fail"
    TaskQueueRepository.create(task_id, "download_link", {}, status="failed")
    DownloadBreakpointRepository.upsert(task_id, "-1001", 1, "/tmp/a.part", "/tmp/a.meta", "/tmp/a.mp4")

    def partial_delete(targets):
        if targets == ["/tmp/a.part"]:
            return {"deleted": ["/tmp/a.part"], "skipped": []}
        raise OSError("disk error")

    monkeypatch.setattr("app.services.task_lifecycle_service.StorageService.delete_files", partial_delete)
    monkeypatch.setattr("app.services.task_lifecycle_service.gc_task_meta", lambda _tid: None)
    monkeypatch.setattr("app.services.task_lifecycle_service.connection_manager.broadcast", AsyncMock())

    res = await TaskLifecycleService.delete_task(task_id, delete_files=True)
    assert res["status"] == "deleted"
    assert res["files_deleted"] == ["/tmp/a.part"]
    assert len(res["files_skipped"]) == 1
    assert res["files_skipped"][0]["path"] == "/tmp/a.meta"
    assert "disk error" in res["files_skipped"][0]["reason"]
