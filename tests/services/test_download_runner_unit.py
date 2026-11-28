"""
下载运行器单元测试 (Download Runner Unit Tests)

覆盖 app/services/download_runner.py 中的核心编排函数与辅助逻辑:
1. _make_control_lambdas 任务取消与暂停闭包
2. _check_pause 暂停信号感知与游标保存 (含无游标与带游标两态)
3. _raise_if_cancelled 取消信号感知与终态置位
4. _finalize_failed_task 失败终态汇总与条目截断展示 (≤20 / >20 条)
5. _resolve_boot_config 启动并发与执行账号配置回读与防崩容错
6. _promote_failover_account 账号故障转移晋升与调度器重绑
7. _start_tier2_fleet Tier2 联合舰队跨账号并发发现与借壳
8. _announce_tier1 Tier1 中转桥就绪状态通告
9. _finalize_download_completed 任务 100% 完成收尾
10. run_download_links_task 调度窗口早退保护与未登录会话守卫
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.runtime import active_task_controls
from app.core.scheduler import TaskCancelledError, TaskPausedError
from app.services.download_runner import (
    _announce_tier1,
    _check_pause,
    _ClientCtx,
    _download_album_members,
    _DownloadRunState,
    _finalize_download_completed,
    _finalize_failed_task,
    _make_control_lambdas,
    _make_progress_cb,
    _promote_failover_account,
    _raise_if_cancelled,
    _resolve_boot_config,
    _start_tier2_fleet,
    _transit_download_album,
    run_download_links_task,
)


@pytest.fixture(autouse=True)
def reset_task_controls():
    active_task_controls.clear()
    yield
    active_task_controls.clear()


# ==============================================================================
# 1. 控制信号闭包与暂停感知 (_make_control_lambdas / _check_pause / _raise_if_cancelled)
# ==============================================================================


def test_make_control_lambdas():
    task_id = "test_task_ctrl"
    cancel_fn, pause_fn = _make_control_lambdas(task_id)

    assert not cancel_fn()
    assert not pause_fn()

    active_task_controls[task_id] = {"cancelled": True, "paused": False}
    assert cancel_fn()
    assert not pause_fn()

    active_task_controls[task_id] = {"cancelled": False, "paused": True}
    assert not cancel_fn()
    assert pause_fn()


@pytest.mark.asyncio
async def test_check_pause_when_not_paused():
    task_id = "test_not_paused"
    # 不抛出任何异常
    await _check_pause(task_id)


@pytest.mark.asyncio
async def test_check_pause_when_paused_without_cursor():
    task_id = "test_paused_no_cursor"
    active_task_controls[task_id] = {"paused": True}

    with (
        patch("app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock()) as mock_status,
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()) as mock_log,
        pytest.raises(TaskPausedError),
    ):
        await _check_pause(task_id)

    mock_status.assert_awaited_once_with(task_id, "paused", from_statuses=("running", "paused"))
    mock_log.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_pause_when_paused_with_cursor():
    task_id = "test_paused_cursor"
    active_task_controls[task_id] = {"paused": True}
    state = _DownloadRunState(total_items=10, contiguous_cursor_id=42)

    with (
        patch("app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock()) as mock_status,
        patch("app.services.download_runner.TaskQueueRepository.merge_params_async", new=AsyncMock()) as mock_merge,
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()) as mock_log,
        pytest.raises(TaskPausedError),
    ):
        await _check_pause(task_id, cursor=state)

    mock_status.assert_awaited_once_with(task_id, "paused", from_statuses=("running", "paused"))
    mock_merge.assert_awaited_once_with(task_id, {"contiguous_cursor_id": 42})
    assert "42" in mock_log.await_args[0][1]


@pytest.mark.asyncio
async def test_raise_if_cancelled():
    task_id = "test_cancel"
    # 未取消
    await _raise_if_cancelled(task_id)

    # 取消标记置位
    active_task_controls[task_id] = {"cancelled": True}
    with (
        patch("app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock()) as mock_status,
        pytest.raises(TaskCancelledError),
    ):
        await _raise_if_cancelled(task_id)

    mock_status.assert_awaited_once_with(task_id, "cancelled")


# ==============================================================================
# 2. 失败终态汇总 (_finalize_failed_task)
# ==============================================================================


@pytest.mark.asyncio
async def test_finalize_failed_task_few_items():
    task_id = "test_fail_few"
    failed = [("msg_1", "超时"), ("msg_2", "无权限")]

    with (
        patch("app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock()) as mock_status,
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()) as mock_log,
        patch("app.services.download_runner.connection_manager.broadcast", new=AsyncMock()) as mock_broadcast,
    ):
        await _finalize_failed_task(task_id, failed)

    mock_status.assert_awaited_once_with(task_id, "failed", from_statuses=("running",))
    assert mock_log.await_count == 2
    mock_broadcast.assert_awaited_once_with({"type": "task_update", "task_id": task_id, "status": "failed"})


@pytest.mark.asyncio
async def test_finalize_failed_task_many_items_truncated():
    task_id = "test_fail_many"
    failed = [(f"msg_{i}", f"err_{i}") for i in range(25)]

    with (
        patch("app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock()),
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()) as mock_log,
        patch("app.services.download_runner.connection_manager.broadcast", new=AsyncMock()),
    ):
        await _finalize_failed_task(task_id, failed)

    log_call = mock_log.await_args_list[0][0][1]
    assert "仅展示前 20 条，共 25 条失败" in log_call


# ==============================================================================
# 3. 启动配置回读与容错 (_resolve_boot_config)
# ==============================================================================


@pytest.mark.asyncio
async def test_resolve_boot_config():
    task_id = "test_boot_cfg"

    # 1. 显式给出参数: 直接返回，不查询数据库
    with patch("app.services.download_runner.TaskQueueRepository.get_by_id_async", new=AsyncMock()) as mock_get:
        threads, acc = await _resolve_boot_config(task_id, 4, 101, "测试")
        assert threads == 4
        assert acc == 101
        mock_get.assert_not_called()

    # 2. 参数为 None: 从数据库回读
    mock_record = {
        "params": {"threads": 8},
        "account_id": 202,
    }
    with patch(
        "app.services.download_runner.TaskQueueRepository.get_by_id_async", new=AsyncMock(return_value=mock_record)
    ):
        threads, acc = await _resolve_boot_config(task_id, None, None, "测试")
        assert threads == 8
        assert acc == 202

    # 3. 数据库查询抛异常: 优雅降级返回原值，不向外穿透异常
    with patch(
        "app.services.download_runner.TaskQueueRepository.get_by_id_async",
        new=AsyncMock(side_effect=RuntimeError("DB Lock")),
    ):
        threads, acc = await _resolve_boot_config(task_id, None, 303, "测试")
        assert threads is None
        assert acc == 303


# ==============================================================================
# 4. 故障转移与舰队编排 (_promote_failover_account / _start_tier2_fleet / _announce_tier1)
# ==============================================================================


@pytest.mark.asyncio
async def test_promote_failover_account():
    ctx = _ClientCtx(task_id="task_failover", threads=4, account_id=10)
    alt_acc = MagicMock()
    alt_acc.account_id = 20
    alt_acc.account_name = "Alt Userbot"

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=None),
        patch("app.services.download_runner.task_scheduler.reassign_task_account") as mock_reassign,
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()) as mock_log,
        patch(
            "app.services.download_runner.TaskQueueRepository.update_account_id_async", new=AsyncMock()
        ) as mock_update_acc,
        patch("app.services.download_runner.connection_manager.broadcast", new=AsyncMock()) as mock_broadcast,
    ):
        await _promote_failover_account(ctx, alt_acc, "https://t.me/c/123/456")

    assert ctx.account_id == 20
    assert 20 in ctx.attempted_accounts
    mock_reassign.assert_called_once_with("task_failover", 10, 20)
    mock_update_acc.assert_awaited_once_with("task_failover", 20)
    mock_broadcast.assert_awaited_once_with({"type": "task_update", "task_id": "task_failover", "account_id": 20})
    mock_log.assert_awaited_once()


@pytest.mark.asyncio
async def test_start_tier2_fleet():
    primary_client = MagicMock()
    ctx = _ClientCtx(task_id="task_fleet", threads=4, account_id=1, primary_client=primary_client)

    aux_client = MagicMock()
    userbot_1 = MagicMock(client=primary_client, account_id=1, account_name="Bot1")
    userbot_2 = MagicMock(client=aux_client, account_id=2, account_name="Bot2")

    # 1. 发现辅助账号 (跨账号并发联合加速)
    with (
        patch(
            "app.core.client_manager.client_manager.get_accessible_userbots",
            new=AsyncMock(return_value=[userbot_1, userbot_2]),
        ),
        patch("app.services.download_runner.task_scheduler.borrow_auxiliary_account") as mock_borrow,
        patch("app.services.download_runner.TaskQueueRepository.merge_params_async", new=AsyncMock()) as mock_merge,
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()) as mock_log,
        patch("app.services.download_runner.connection_manager.broadcast", new=AsyncMock()) as mock_broadcast,
    ):
        clients = await _start_tier2_fleet(ctx, -100900008)
        assert len(clients) == 2
        mock_borrow.assert_called_once_with("task_fleet", 2)
        mock_merge.assert_awaited_once()
        mock_broadcast.assert_awaited_once()
        mock_log.assert_awaited_once()


@pytest.mark.asyncio
async def test_announce_tier1():
    mock_acc = MagicMock(account_name="Primary User")
    mock_bot = MagicMock(session_name="HelperBot", is_ready=MagicMock(return_value=True))

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.core.client_manager.client_manager.get_all_bots", return_value=[mock_bot]),
        patch("app.services.download_runner.TaskQueueRepository.merge_params_async", new=AsyncMock()) as mock_merge,
        patch("app.services.download_runner.connection_manager.broadcast", new=AsyncMock()) as mock_broadcast,
    ):
        acc_name, bot_names = await _announce_tier1("task_t1", 1, None)
        assert acc_name == "Primary User"
        assert bot_names == ["HelperBot"]
        mock_merge.assert_awaited_once_with(
            "task_t1", {"executor_names": ["Primary User", "HelperBot"], "tier": "tier1_transit"}
        )
        mock_broadcast.assert_awaited_once()


# ==============================================================================
# 5. 完成态收尾 (_finalize_download_completed)
# ==============================================================================


@pytest.mark.asyncio
async def test_finalize_download_completed():
    task_id = "task_done"
    state = _DownloadRunState(total_items=3, cumulative_done_bytes=1048576)

    with (
        patch("app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock()) as mock_status,
        patch(
            "app.services.download_runner.TaskQueueRepository.update_progress_async", new=AsyncMock()
        ) as mock_progress,
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()) as mock_log,
        patch("app.services.download_runner.connection_manager.broadcast", new=AsyncMock()) as mock_broadcast,
    ):
        await _finalize_download_completed(task_id, state, "全部下载完成")

    mock_status.assert_awaited_once_with(task_id, "completed", from_statuses=("running",))
    mock_progress.assert_awaited_once()
    mock_log.assert_awaited_once_with(task_id, "🎉 全部下载完成！")
    assert mock_broadcast.await_count == 2


# ==============================================================================
# 6. 调度抢占与异常处理 (run_download_links_task 守卫分流)
# ==============================================================================


@pytest.mark.asyncio
async def test_run_download_links_task_claimed_false():
    # 任务在调度窗口已被抢占/取消
    with (
        patch("app.services.download_runner.try_claim_executor_entry", return_value=False),
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()) as mock_log,
        patch("app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock()) as mock_status,
    ):
        await run_download_links_task("task_already_cancelled", ["https://t.me/c/1/2"])
        mock_status.assert_not_called()
        mock_log.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_download_links_task_no_primary_client():
    with (
        patch("app.services.download_runner.try_claim_executor_entry", return_value=True),
        patch("app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock()),
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()),
        patch("app.services.download_runner._resolve_boot_config", new=AsyncMock(return_value=(4, 1))),
        patch("app.core.client_manager.client_manager.get_client_for_task", new=AsyncMock(return_value=None)),
        patch("app.core.client_manager.client_manager.userbot", None),
        patch("app.services.download_runner.report_session_invalid", new=AsyncMock()) as mock_report,
    ):
        await run_download_links_task("task_no_client", ["https://t.me/c/1/2"])
        mock_report.assert_awaited_once()


# ==============================================================================
# 7. 进度回调工厂验证 (_make_progress_cb)
# ==============================================================================


def test_make_progress_cb():
    state = _DownloadRunState(total_items=5, cumulative_done_bytes=1000, known_total_bytes=5000)
    with patch("app.services.download_runner.submit_db_write"):
        cb = _make_progress_cb("task_progress", state, 1, 5, "item_1")
        assert callable(cb)
        assert cb.received_bytes() == 0


@pytest.mark.asyncio
async def test_primary_client_owner_tracks_fallback_and_reselection():
    from app.services.download_runner import _select_primary_client

    ctx = _ClientCtx(task_id="owner", threads=4, account_id=2)
    default_client = object()
    selected_client = object()
    with patch("app.core.client_manager.client_manager") as manager:
        manager.userbot = default_client
        manager.get_client_for_task = AsyncMock(side_effect=[None, selected_client])
        manager.get_account.side_effect = lambda account_id=None: MagicMock(account_id=account_id or 1)
        await _select_primary_client(ctx)
        assert ctx.primary_client is default_client
        assert ctx.primary_account_id == 1
        assert ctx.account_id == 2
        ctx.account_id = 3
        await _select_primary_client(ctx)
        assert ctx.primary_client is selected_client
        assert ctx.primary_account_id == 3
        assert ctx.account_id == 3


@pytest.mark.asyncio
async def test_primary_client_owner_captured_before_await():
    """异步 get_client_for_task 重连期间即便默认账号切换, primary_account_id 仍保持 await 前捕获的归属"""
    from app.services.download_runner import _select_primary_client

    ctx = _ClientCtx(task_id="owner_race", threads=4, account_id=None)
    selected_client = object()
    state = {"default_id": 1}

    async def fake_get_client(account_id, mode="userbot"):
        state["default_id"] = 99
        return selected_client

    with patch("app.core.client_manager.client_manager") as manager:
        manager.get_account.side_effect = lambda account_id=None: MagicMock(
            account_id=account_id or state["default_id"]
        )
        manager.get_client_for_task = AsyncMock(side_effect=fake_get_client)
        await _select_primary_client(ctx)

    assert ctx.primary_client is selected_client
    assert ctx.primary_account_id == 1


@pytest.mark.asyncio
async def test_range_session_failure_reports_primary_owner():
    from app.services.download_runner import _resolve_range_entity

    ctx = _ClientCtx(task_id="owner", threads=4, account_id=2, primary_account_id=1)
    with (
        patch(
            "app.services.forward_service.ForwardService.resolve_entity",
            new=AsyncMock(side_effect=RuntimeError("auth key unregistered")),
        ),
        patch("app.services.download_runner.report_session_invalid", new=AsyncMock()) as report,
    ):
        assert await _resolve_range_entity(ctx, "channel") is None
    assert report.await_args.kwargs["account_id"] == 1
    assert ctx.account_id == 2


@pytest.mark.asyncio
async def test_transit_album_uses_forwarded_message_and_per_member_source_id():
    """Tier1 相册中转: message 取中转转发消息, origin_msg_id 逐成员取各自源 ID"""
    src_a, src_b = MagicMock(id=41, grouped_id=5), MagicMock(id=42, grouped_id=5)
    fwd_a, fwd_b = MagicMock(id=901), MagicMock(id=902)
    bridge = MagicMock()
    bridge.clients = [MagicMock()]
    bridge.transit_chat = MagicMock()
    bridge.source_to_forwarded = {41: fwd_a, 42: fwd_b}

    state = _DownloadRunState(total_items=2)
    ctx = _ClientCtx(task_id="t_album_transit", threads=4, account_id=None)

    captured: list[dict] = []

    async def fake_download(**kwargs):
        captured.append(kwargs)
        return "/tmp/fake_out"

    with (
        patch(
            "app.services.download_runner.DownloaderService.get_file_name_and_size",
            side_effect=lambda m, fallback_id=None: ("media.bin", 100),
        ),
        patch("app.services.download_runner.DownloaderService.download_single_message", side_effect=fake_download),
    ):
        await _transit_download_album(state, ctx, bridge, [src_a, src_b], "-100src", 1, 2, "link", force=True)

    assert len(captured) == 2
    # 身份红线: message 必须是中转群转发消息 (凭证预取按中转群鉴权)
    assert captured[0]["message"] is fwd_a
    assert captured[1]["message"] is fwd_b
    # origin_msg_id 逐成员取源 ID, 绝不共享锚点或传整对象
    assert captured[0]["origin_msg_id"] == 41
    assert captured[1]["origin_msg_id"] == 42
    assert all(call["force"] is True and call["origin_grouped_id"] == 5 for call in captured)
    assert state.failed_items == []
    assert not state.failed_sources
    assert state.cumulative_done_bytes == 200


@pytest.mark.asyncio
async def test_transit_album_missing_forward_mapping_fails_member():
    """相册成员无对应转发消息 (部分转发失败): 该成员记失败并跳过, 不误下别的成员媒体"""
    src = MagicMock(id=41)
    bridge = MagicMock()
    bridge.clients = [MagicMock()]
    bridge.transit_chat = MagicMock()
    bridge.source_to_forwarded = {}  # 桥接映射缺失

    state = _DownloadRunState(total_items=1)
    ctx = _ClientCtx(task_id="t_album_missing", threads=4, account_id=None)

    download_mock = AsyncMock()
    with (
        patch(
            "app.services.download_runner.DownloaderService.get_file_name_and_size",
            side_effect=lambda m, fallback_id=None: ("media.bin", 100),
        ),
        patch("app.services.download_runner.DownloaderService.download_single_message", new=download_mock),
    ):
        await _transit_download_album(state, ctx, bridge, [src], "-100src", 1, 1, "link")

    download_mock.assert_not_awaited()
    assert len(state.failed_sources) == 1
    assert "中转桥接失败" in next(iter(state.failed_sources.values()))[1]


@pytest.mark.asyncio
async def test_forced_album_only_retries_failed_or_deleted_members(tmp_path):
    first, second = (MagicMock(id=msg_id, chat_id=-100123) for msg_id in (41, 42))
    bridge = MagicMock(clients=[MagicMock()], source_to_forwarded={41: MagicMock(), 42: MagicMock()})
    state = _DownloadRunState(total_items=3)
    ctx = _ClientCtx(task_id="forced-album", threads=4, account_id=None)
    first_path = tmp_path / "first.bin"
    second_path = tmp_path / "second.bin"
    second_path.write_bytes(b"second")

    async def download_member(**kwargs):
        if kwargs["message"] is bridge.source_to_forwarded[42]:
            return None
        if kwargs["message"] is second:
            return str(second_path)
        first_path.write_bytes(b"first")
        return str(first_path)

    download = AsyncMock(side_effect=download_member)

    with (
        patch("app.services.download_runner.DownloaderService.get_file_name_and_size", return_value=("file", 100)),
        patch("app.services.download_runner.DownloaderService.download_single_message", new=download),
    ):
        await _transit_download_album(state, ctx, bridge, [first, second], -100123, 1, 3, "a", force=True)
        await _download_album_members(state, ctx, [first, second], "example", 2, 3, "b", [], False, force=True)
        first_path.unlink()
        await _transit_download_album(state, ctx, bridge, [first, second], -100123, 3, 3, "c", force=True)

    assert [call.kwargs["message"] for call in download.await_args_list] == [
        bridge.source_to_forwarded[41],
        bridge.source_to_forwarded[42],
        second,
        bridge.source_to_forwarded[41],
    ]
    assert all(call.kwargs["force"] for call in download.await_args_list)
    assert state.cumulative_done_bytes == 300
    assert first_path.read_bytes() == b"first"
    assert not state.failed_sources


@pytest.mark.asyncio
@pytest.mark.parametrize(("retry_result", "final_status"), [("second.bin", "completed"), (None, "failed")])
async def test_link_runner_finalizes_using_unresolved_album_failures(tmp_path, retry_result, final_status):
    from app.db.task_repository import TaskQueueRepository
    from app.services.download_runner import DownloaderService

    task_id = "album-task"
    links = ["https://t.me/example/41", "https://t.me/example/42"]
    first, second = (MagicMock(id=msg_id, chat_id=-100123, media=object()) for msg_id in (41, 42))
    TaskQueueRepository.create(task_id, "download_link", {"links": links}, status="running")

    async def select_client(ctx):
        ctx.primary_client = object()

    first_path = tmp_path / "first.bin"
    first_path.write_bytes(b"first")
    download = AsyncMock(side_effect=[str(first_path), None, retry_result])
    with (
        patch("app.services.download_runner._select_primary_client", side_effect=select_client),
        patch("app.services.download_runner._refresh_clients_pool"),
        patch("app.services.download_runner.connection_manager.broadcast", new=AsyncMock()),
        patch.object(
            DownloaderService, "resolve_message_from_link", side_effect=[("example", first), ("example", second)]
        ),
        patch.object(DownloaderService, "get_album_messages", return_value=[first, second]),
        patch.object(DownloaderService, "get_file_name_and_size", return_value=("file", 100)),
        patch.object(DownloaderService, "download_single_message", new=download),
    ):
        await run_download_links_task(task_id, links, force=True)

    assert TaskQueueRepository.get_by_id(task_id)["status"] == final_status
    assert [call.kwargs["message"] for call in download.await_args_list] == [first, second, second]


# ==============================================================================
# 8. 辅助客户端 DownloadIncompleteError 会话失效精确归因
# ==============================================================================


@pytest.mark.asyncio
async def test_links_task_aux_client_download_incomplete_attributes_aux_account():
    """私有链接 Tier2 联合下载中辅助账号触发 DownloadIncompleteError 会话失效时，仅上报辅助账号，不误伤主账号."""
    from telethon.errors import AuthKeyUnregisteredError

    from app.core.fast_telethon import DownloadIncompleteError
    from app.services.download_runner import run_download_links_task

    primary_client = MagicMock(name="primary_client")
    aux_client = MagicMock(name="aux_client")
    acc1 = MagicMock(account_id=1, account_name="Primary", client=primary_client)
    acc2 = MagicMock(account_id=2, account_name="Aux", client=aux_client)

    msg = MagicMock(id=50, chat_id=-100123, media=MagicMock())
    exc = DownloadIncompleteError(
        "/tmp/file.mp4",
        [512 * 1024],
        {512 * 1024: "AuthKeyUnregisteredError: The key is not registered in the system"},
        total_chunks=2,
        completed_chunks=1,
        last_clients={512 * 1024: aux_client},
        last_exceptions={512 * 1024: AuthKeyUnregisteredError(request=None)},
    )

    with (
        patch("app.services.download_runner.try_claim_executor_entry", return_value=True),
        patch("app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock()),
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()),
        patch("app.services.download_runner.TaskQueueRepository.merge_params_async", new=AsyncMock()),
        patch("app.services.download_runner.connection_manager.broadcast", new=AsyncMock()),
        patch("app.services.download_runner._resolve_boot_config", new=AsyncMock(return_value=(4, 1))),
        patch("app.core.client_manager.client_manager") as manager,
        patch(
            "app.services.transit_bridge_service.TransitBridgeService.is_transit_capable",
            return_value=(False, "disabled"),
        ),
        patch(
            "app.services.download_runner.DownloaderService.resolve_message_from_link",
            new=AsyncMock(return_value=(-100123, msg)),
        ),
        patch(
            "app.services.download_runner.DownloaderService.get_album_messages",
            new=AsyncMock(return_value=[msg]),
        ),
        patch(
            "app.services.download_runner.DownloaderService.get_file_name_and_size",
            return_value=("file.mp4", 1024 * 1024),
        ),
        patch(
            "app.services.download_runner.DownloaderService.download_single_message",
            new=AsyncMock(side_effect=exc),
        ),
        patch("app.services.download_runner.task_scheduler.borrow_auxiliary_account"),
        patch("app.services.download_runner.task_scheduler.release_auxiliary_account") as mock_release,
        patch("app.services.download_runner.report_session_invalid", new=AsyncMock()) as mock_report,
    ):
        manager.get_account.return_value = acc1
        manager.get_client_for_task = AsyncMock(return_value=primary_client)
        manager.get_all_active_clients.return_value = [primary_client, aux_client]
        manager.get_all_accounts.return_value = [acc1, acc2]
        manager.get_all_bots.return_value = []
        manager.get_accessible_userbots = AsyncMock(return_value=[acc1, acc2])
        manager.is_bot_client.return_value = False

        await run_download_links_task("task_aux_links", ["https://t.me/c/123/50"], threads=4, account_id=1)

    mock_report.assert_awaited_once_with("task_aux_links", exc, account_id=2)
    mock_release.assert_called_once_with("task_aux_links", 2)
    manager.invalidate_accessible_cache.assert_called_once_with(account_id=2)


@pytest.mark.asyncio
async def test_range_task_mixed_errors_attributes_only_session_invalid_aux():
    """范围下载中主号仅普通 TimeoutError、辅助号触发 AuthKeyUnregisteredError 时，仅归因辅助账号."""
    from telethon.errors import AuthKeyUnregisteredError

    from app.core.fast_telethon import DownloadIncompleteError
    from app.services.download_runner import run_download_range_task

    primary_client = MagicMock(name="primary_client")
    aux_client = MagicMock(name="aux_client")
    acc1 = MagicMock(account_id=1, account_name="Primary", client=primary_client)
    acc2 = MagicMock(account_id=2, account_name="Aux", client=aux_client)

    msg = MagicMock(id=10, chat_id="public_chan", media=MagicMock())
    primary_client.get_messages = AsyncMock(return_value=msg)

    exc = DownloadIncompleteError(
        "/tmp/range.mp4",
        [0, 512 * 1024],
        {
            0: "timed out",
            512 * 1024: "AuthKeyUnregisteredError: The key is not registered in the system",
        },
        total_chunks=2,
        completed_chunks=0,
        last_clients={0: primary_client, 512 * 1024: aux_client},
        last_exceptions={
            0: TimeoutError("timed out"),
            512 * 1024: AuthKeyUnregisteredError(request=None),
        },
    )

    with (
        patch("app.services.download_runner.try_claim_executor_entry", return_value=True),
        patch("app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock()),
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()),
        patch("app.services.download_runner.TaskQueueRepository.get_by_id_async", new=AsyncMock(return_value=None)),
        patch("app.services.download_runner.connection_manager.broadcast", new=AsyncMock()),
        patch("app.services.download_runner._resolve_boot_config", new=AsyncMock(return_value=(4, 1))),
        patch("app.core.client_manager.client_manager") as manager,
        patch(
            "app.services.forward_service.ForwardService.resolve_entity",
            new=AsyncMock(return_value=MagicMock()),
        ),
        patch(
            "app.services.download_runner.DownloaderService.get_file_name_and_size",
            return_value=("range.mp4", 1024 * 1024),
        ),
        patch(
            "app.services.download_runner.DownloaderService.download_single_message",
            new=AsyncMock(side_effect=exc),
        ),
        patch("app.services.download_runner.task_scheduler.release_auxiliary_account") as mock_release,
        patch("app.services.download_runner.report_session_invalid", new=AsyncMock()) as mock_report,
    ):
        manager.get_account.return_value = acc1
        manager.get_client_for_task = AsyncMock(return_value=primary_client)
        manager.get_all_active_clients.return_value = [primary_client, aux_client]
        manager.get_all_accounts.return_value = [acc1, acc2]
        manager.get_all_bots.return_value = []
        manager.is_bot_client.return_value = False

        await run_download_range_task("task_aux_range", "public_chan", start_id=10, end_id=10, threads=4, account_id=1)

    mock_report.assert_awaited_once_with("task_aux_range", exc, account_id=2)
    mock_release.assert_called_once_with("task_aux_range", 2)
    manager.invalidate_accessible_cache.assert_called_once_with(account_id=2)


@pytest.mark.asyncio
async def test_links_task_bot_session_invalid_does_not_invalidate_primary_userbot():
    """公开频道并发池中辅助 Bot 触发会话失效时，仅标记该 Bot invalid 并记当前条目失败，不误伤主 Userbot."""
    from telethon.errors import AuthKeyUnregisteredError

    from app.core.fast_telethon import DownloadIncompleteError
    from app.services.download_runner import run_download_links_task

    primary_client = MagicMock(name="primary_client")
    bot_client = MagicMock(name="bot_client")
    acc1 = MagicMock(account_id=1, account_name="Primary", client=primary_client)
    bot1 = MagicMock(bot_id=10, session_name="bot_10", client=bot_client)

    msg = MagicMock(id=60, chat_id="public_chan", media=MagicMock())
    exc = DownloadIncompleteError(
        "/tmp/pub.mp4",
        [0],
        {0: "AuthKeyUnregisteredError: The key is not registered in the system"},
        total_chunks=1,
        completed_chunks=0,
        last_clients={0: bot_client},
        last_exceptions={0: AuthKeyUnregisteredError(request=None)},
    )

    with (
        patch("app.services.download_runner.try_claim_executor_entry", return_value=True),
        patch("app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock()),
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()),
        patch("app.services.download_runner.connection_manager.broadcast", new=AsyncMock()),
        patch("app.services.download_runner._resolve_boot_config", new=AsyncMock(return_value=(4, 1))),
        patch("app.core.client_manager.client_manager") as manager,
        patch(
            "app.services.download_runner.DownloaderService.resolve_message_from_link",
            new=AsyncMock(return_value=("public_chan", msg)),
        ),
        patch(
            "app.services.download_runner.DownloaderService.get_album_messages",
            new=AsyncMock(return_value=[msg]),
        ),
        patch(
            "app.services.download_runner.DownloaderService.get_file_name_and_size",
            return_value=("pub.mp4", 1024 * 1024),
        ),
        patch(
            "app.services.download_runner.DownloaderService.download_single_message",
            new=AsyncMock(side_effect=exc),
        ),
        patch("app.services.download_runner.report_session_invalid", new=AsyncMock()) as mock_report,
        patch("app.services.download_runner._finalize_failed_task", new=AsyncMock()) as mock_finalize_failed,
    ):
        manager.get_account.return_value = acc1
        manager.get_client_for_task = AsyncMock(return_value=primary_client)
        manager.get_all_active_clients.return_value = [primary_client, bot_client]
        manager.get_all_accounts.return_value = [acc1]
        manager.get_all_bots.return_value = [bot1]
        manager.is_bot_client.side_effect = lambda c: c is bot_client

        await run_download_links_task("task_bot_fail", ["https://t.me/public_chan/60"], threads=4, account_id=1)

    manager.invalidate_bot_by_client.assert_called_once()
    assert manager.invalidate_bot_by_client.call_args[0][0] is bot_client
    mock_report.assert_not_awaited()
    mock_finalize_failed.assert_awaited_once()


def test_collect_session_invalid_failing_clients_requires_per_chunk_evidence():
    """无逐块会话失效证据时不得回退到全部候选 client (安全审查回归)."""
    from app.core.fast_telethon import DownloadIncompleteError
    from app.services.download_runner import _collect_session_invalid_failing_clients

    aux_client = object()
    exc = DownloadIncompleteError(
        "/tmp/ordinary.mp4",
        [512 * 1024],
        {512 * 1024: "TimeoutError: timed out"},
        total_chunks=2,
        completed_chunks=1,
        last_clients={512 * 1024: aux_client},
        last_exceptions={512 * 1024: TimeoutError("timed out")},
    )
    assert _collect_session_invalid_failing_clients(exc) == []


@pytest.mark.asyncio
async def test_links_task_filename_false_positive_degrades_without_invalidating_accounts():
    """文件名含会话失效关键词的普通下载失败: 不归因任何账号，按普通条目失败记账 (安全审查回归)."""
    from app.core.fast_telethon import DownloadIncompleteError
    from app.services.download_runner import run_download_links_task

    primary_client = MagicMock(name="primary_client")
    aux_client = MagicMock(name="aux_client")
    acc1 = MagicMock(account_id=1, account_name="Primary", client=primary_client)
    acc2 = MagicMock(account_id=2, account_name="Aux", client=aux_client)

    msg = MagicMock(id=70, chat_id="public_chan", media=MagicMock())
    exc = DownloadIncompleteError(
        r"D:\downloads\unauthorized.bin",
        [0],
        {0: "TimeoutError: timed out"},
        total_chunks=2,
        completed_chunks=1,
        last_clients={0: aux_client},
        last_exceptions={0: TimeoutError("timed out")},
    )

    with (
        patch("app.services.download_runner.try_claim_executor_entry", return_value=True),
        patch("app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock()),
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()),
        patch("app.services.download_runner.TaskQueueRepository.merge_params_async", new=AsyncMock()),
        patch("app.services.download_runner.connection_manager.broadcast", new=AsyncMock()),
        patch("app.services.download_runner._resolve_boot_config", new=AsyncMock(return_value=(4, 1))),
        patch("app.core.client_manager.client_manager") as manager,
        patch(
            "app.services.download_runner.DownloaderService.resolve_message_from_link",
            new=AsyncMock(return_value=("public_chan", msg)),
        ),
        patch(
            "app.services.download_runner.DownloaderService.get_album_messages",
            new=AsyncMock(return_value=[msg]),
        ),
        patch(
            "app.services.download_runner.DownloaderService.get_file_name_and_size",
            return_value=("unauthorized.bin", 1024 * 1024),
        ),
        patch(
            "app.services.download_runner.DownloaderService.download_single_message",
            new=AsyncMock(side_effect=exc),
        ),
        patch("app.services.download_runner.task_scheduler.release_auxiliary_account") as mock_release,
        patch("app.services.download_runner.report_session_invalid", new=AsyncMock()) as mock_report,
        patch("app.services.download_runner._finalize_failed_task", new=AsyncMock()) as mock_finalize_failed,
    ):
        manager.get_account.return_value = acc1
        manager.get_client_for_task = AsyncMock(return_value=primary_client)
        manager.get_all_active_clients.return_value = [primary_client, aux_client]
        manager.get_all_accounts.return_value = [acc1, acc2]
        manager.get_all_bots.return_value = []
        manager.is_bot_client.return_value = False

        await run_download_links_task("task_fn_fp", ["https://t.me/public_chan/70"], threads=4, account_id=1)

    mock_report.assert_not_awaited()
    mock_release.assert_not_called()
    manager.invalidate_accessible_cache.assert_not_called()
    manager.invalidate_bot_by_client.assert_not_called()
    mock_finalize_failed.assert_awaited_once()


@pytest.mark.asyncio
async def test_album_discovery_failure_marks_download_task_failed():
    task_id = "dl_album_fail"
    link = "https://t.me/public_chan/50"
    seed = MagicMock(id=50, chat_id=-100123, grouped_id=999, media=MagicMock(), input_chat=None)
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.get_messages = AsyncMock(side_effect=TimeoutError())

    with (
        patch("app.services.download_runner.try_claim_executor_entry", return_value=True),
        patch("app.core.client_manager.client_manager.get_client_for_task", new=AsyncMock(return_value=client)),
        patch("app.core.client_manager.client_manager.get_all_active_clients", return_value=[client]),
        patch("app.core.client_manager.client_manager.userbot", client),
        patch("app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock()),
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()),
        patch("app.services.download_runner.connection_manager.broadcast", new=AsyncMock()),
        patch(
            "app.services.downloader_service.DownloaderService.resolve_message_from_link",
            new=AsyncMock(return_value=("public_chan", seed)),
        ),
        patch("app.services.download_runner._finalize_failed_task", new=AsyncMock()) as mock_fail,
        patch("app.services.download_runner._finalize_download_completed", new=AsyncMock()) as mock_done,
    ):
        await run_download_links_task(task_id, [link], threads=2)

    mock_fail.assert_awaited_once()
    failed_items = mock_fail.call_args.args[1]
    assert len(failed_items) == 1
    assert failed_items[0][0] == link
    mock_done.assert_not_awaited()


@pytest.mark.asyncio
async def test_download_terminal_helpers_skip_side_effects_when_guard_misses():
    task_id = "dl_guard_missed"
    state = _DownloadRunState(total_items=2, cumulative_done_bytes=512)

    with (
        patch(
            "app.services.download_runner.TaskQueueRepository.update_status_async",
            new=AsyncMock(return_value=False),
        ) as mock_status,
        patch("app.services.download_runner.TaskQueueRepository.update_progress_async", new=AsyncMock()) as mock_prog,
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()) as mock_log,
        patch("app.services.download_runner.connection_manager.broadcast", new=AsyncMock()) as mock_bcast,
    ):
        await _finalize_failed_task(task_id, [("item1", "err")])
        await _finalize_download_completed(task_id, state, "全部下载完成")

    assert mock_status.await_count == 2
    mock_prog.assert_not_awaited()
    mock_log.assert_not_awaited()
    mock_bcast.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["completed", "failed"])
async def test_download_terminal_pause_conflict_marks_exit_and_raises_paused(terminal):
    """终态写入遇暂停冲突时先标记 executor_exiting，并转交暂停收尾；取消态不得抛 TaskPausedError。"""
    task_id = f"dl_pause_term_{terminal}"
    state = _DownloadRunState(total_items=1, cumulative_done_bytes=128)
    active_task_controls[task_id] = {"paused": True, "cancelled": False}

    with (
        patch(
            "app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock(return_value=False)
        ),
        patch("app.services.download_runner.mark_task_executor_exit") as mock_exit,
        patch("app.services.download_runner.mark_task_pause_exit") as mock_pause_exit,
        pytest.raises(TaskPausedError),
    ):
        if terminal == "completed":
            await _finalize_download_completed(task_id, state, "全部下载完成")
        else:
            await _finalize_failed_task(task_id, [("item1", "err")])

    mock_exit.assert_called_once_with(task_id)
    mock_pause_exit.assert_called_once_with(task_id)

    active_task_controls[task_id] = {"paused": False, "cancelled": True}
    with (
        patch(
            "app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock(return_value=False)
        ),
        patch("app.services.download_runner.mark_task_executor_exit") as mock_exit_cancel,
        patch("app.services.download_runner.mark_task_pause_exit") as mock_pause_cancel,
    ):
        if terminal == "completed":
            await _finalize_download_completed(task_id, state, "全部下载完成")
        else:
            await _finalize_failed_task(task_id, [("item1", "err")])

    mock_exit_cancel.assert_called_once_with(task_id)
    mock_pause_cancel.assert_not_called()
