"""发布任务编排与参数/诊断收敛测试 (publish_runner)。"""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

import app.services.publish_runner as pr
from app.core.pikpak import ParsedFileItem
from app.core.pikpak.expander import items_from_snapshot
from app.core.pikpak.taxonomy import PikPakAuthError
from app.core.runtime import active_task_controls, set_task_control
from app.core.scheduler import UploadPausedError
from app.db.task_repository import TaskQueueRepository
from app.services.publish_runner import (
    _resolve_pikpak_run_config,
    run_publish_folder_task,
    run_publish_pikpak_task,
)
from app.services.publisher_service import PublisherService, PublishFolderResult

# _expand_pikpak_remote_files 抛异常前的最小 cfg (与 _resolve_pikpak_run_config 返回键对齐)
_CFG_STUB: dict[str, Any] = {
    "account_id": None,
    "upload_concurrency": None,
    "upload_in_flight_per_connection": None,
    "prefetch_concurrency": None,
    "auto_cleanup_mode": "archive",
    "oversize_policy": "split",
    "split_strategy": None,
    "send_placeholder_card": None,
    "force_republish": False,
    "is_folder_publish": False,
    "folder_name": None,
    "album_mode": False,
    "dir_tree": {},
    "expansion_truncated": False,
    "expansion_limit": 0,
}


@pytest.mark.asyncio
async def test_param_restore_preserves_none_placeholder_card_local(monkeypatch):
    """(本地路径): params 里 send_placeholder_card=null 回读后必须保持 None"""
    task_id = "audit03b_none_local"
    TaskQueueRepository.create(
        task_id,
        "publish_folder",
        {"folder_path": "/fake", "target_channel": "-100123", "send_placeholder_card": None},
    )
    captured: dict[str, Any] = {}

    async def fake_publish(**kwargs):
        captured.update(kwargs)
        return PublishFolderResult(0, 0, [])

    monkeypatch.setattr(PublisherService, "publish_folder", fake_publish)
    try:
        await run_publish_folder_task(
            task_id, "/fake", "-100123", album_mode=False, delay_seconds=0.0, send_placeholder_card=None
        )
    finally:
        TaskQueueRepository.delete(task_id)
    assert captured.get("send_placeholder_card") is None, "null 必须保持 None (修复前 bool(None)=False)"


@pytest.mark.asyncio
async def test_param_restore_preserves_none_placeholder_card_pikpak():
    """(pikpak 路径): _resolve_pikpak_run_config 对 null 同样保持 None"""
    task_id = "audit03b_none_pikpak"
    TaskQueueRepository.create(task_id, "pikpak_publish", {"send_placeholder_card": None})
    try:
        cfg = await _resolve_pikpak_run_config(
            task_id,
            account_id=None,
            upload_concurrency=None,
            upload_in_flight_per_connection=None,
            prefetch_concurrency=None,
            auto_cleanup_mode="archive",
            oversize_policy="split",
            split_strategy=None,
            send_placeholder_card=None,
            force_republish=False,
            is_folder_publish=False,
            folder_name=None,
            album_mode=False,
            dir_tree={},
        )
    finally:
        TaskQueueRepository.delete(task_id)
    assert cfg["send_placeholder_card"] is None, "null 必须保持 None (修复前 bool(None)=False)"


@pytest.mark.asyncio
async def test_expansion_pause_lands_paused_terminal_and_pops_controls(monkeypatch):
    """expansion 抛 UploadPausedError 必须走本层 paused 终态 (含控制条目清理)"""
    task_id = "audit03b_expand_pause"
    TaskQueueRepository.create(task_id, "pikpak_publish", {"target_channel": "-100123"})
    set_task_control(task_id)  # 预置控制条目 (模拟执行器存活期)
    monkeypatch.setattr(pr, "try_claim_executor_entry", lambda tid: True)
    monkeypatch.setattr(pr, "_resolve_pikpak_run_config", AsyncMock(return_value=dict(_CFG_STUB)))
    monkeypatch.setattr(pr, "_expand_pikpak_remote_files", AsyncMock(side_effect=UploadPausedError("paused")))
    try:
        with pytest.raises(UploadPausedError):
            await run_publish_pikpak_task(task_id, "-100123", [{"file_id": "f1", "is_dir": True}])
        row = TaskQueueRepository.get_by_id(task_id)
        assert row is not None and row["status"] == "paused", "expansion 暂停必须落 paused 终态 (修复前逃逸)"
        assert task_id not in active_task_controls, "控制条目必须已清理 (修复前泄漏)"
    finally:
        active_task_controls.pop(task_id, None)
        TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_expansion_cli_failure_sets_error_detail_and_pops_controls(monkeypatch):
    """expansion 抛 CLI 异常必须在本层落 error_detail + 清理控制条目"""
    task_id = "audit03b_expand_fail"
    TaskQueueRepository.create(task_id, "pikpak_publish", {"target_channel": "-100123"})
    set_task_control(task_id)
    monkeypatch.setattr(pr, "try_claim_executor_entry", lambda tid: True)
    monkeypatch.setattr(pr, "_resolve_pikpak_run_config", AsyncMock(return_value=dict(_CFG_STUB)))
    monkeypatch.setattr(pr, "_expand_pikpak_remote_files", AsyncMock(side_effect=RuntimeError("ls failed")))
    try:
        with pytest.raises(RuntimeError):
            await run_publish_pikpak_task(task_id, "-100123", [{"file_id": "f1", "is_dir": True}])
        row = TaskQueueRepository.get_by_id(task_id)
        assert row is not None and row["error_detail"] == "ls failed", (
            "expansion 失败必须落 error_detail (修复前逃逸无明细)"
        )
        assert task_id not in active_task_controls, "控制条目必须已清理 (修复前泄漏)"
    finally:
        active_task_controls.pop(task_id, None)
        TaskQueueRepository.delete(task_id)


def _parsed_file(fid: str, name: str, *, is_dir: bool = False, size: int = 100) -> ParsedFileItem:
    return ParsedFileItem(
        file_id=fid,
        name=name,
        is_dir=is_dir,
        size_bytes=size,
        audit={},
        thumbnail_link=None,
        duration_s=None,
        width=None,
        height=None,
        parent_id="",
    )


@pytest.mark.asyncio
async def test_run_publish_pikpak_task_forwards_pre_expansion_truncation():
    """pre-expansion 触发截断时透传到汇总卡且超 3900 字符裁剪不丢失截断警告."""
    from unittest.mock import MagicMock, patch

    from app.core.pipeline.models import PublishContext, PublishResult
    from app.core.pipeline.notifier import TelegramChannelNotifier

    captured_kwargs: dict = {}

    async def fake_expand(raw_items, **kwargs):
        out = kwargs.get("truncation_out")
        if out is not None:
            out.update({"truncated": True, "max_files": 5000, "kept_files": 5000})
        return [_parsed_file("f1", "a.mp4")], {"d1": "dir"}

    async def fake_publish(**kwargs):
        captured_kwargs.update(kwargs)
        return PublishResult(published_count=1, total_files_count=1, newly_published_count=1)

    with (
        patch.object(pr, "try_claim_executor_entry", return_value=True),
        patch.object(TaskQueueRepository, "update_status"),
        patch.object(TaskQueueRepository, "append_log_async", new_callable=AsyncMock),
        patch.object(TaskQueueRepository, "get_by_id", return_value=None),
        patch.object(TaskQueueRepository, "merge_params"),
        patch.object(pr, "_finalize_publish_result", new_callable=AsyncMock),
        patch.object(pr.PikPakPublisherService, "expand_remote_files", side_effect=fake_expand),
        patch.object(pr.PikPakPublisherService, "publish_pikpak_files", side_effect=fake_publish),
    ):
        await pr.run_publish_pikpak_task(
            "t_trunc_fwd",
            "-1001",
            [{"file_id": "d1", "name": "dir", "is_dir": True, "size_bytes": 0}],
        )

    assert captured_kwargs.get("expansion_truncated") is True
    assert captured_kwargs.get("expansion_limit") == 5000

    client = AsyncMock()
    ctx = PublishContext(
        task_id="t_sum_trunc",
        target_chat_id=-1001,
        userbot_client=client,
        expansion_truncated=True,
        expansion_limit=5000,
    )
    long_reports = [
        PublishResult(
            published_count=1,
            total_files_count=1,
            newly_published_count=1,
            is_folder_publish=True,
            folder_name=f"folder_{i}_" + ("x" * 180),
        )
        for i in range(30)
    ]
    notifier = TelegramChannelNotifier()
    with patch("app.core.pipeline.notifier.resolve_entity", new_callable=AsyncMock, return_value=MagicMock()):
        await notifier.notify_task_summary(ctx, long_reports)
    sent_text = client.send_message.await_args.args[1]
    assert "目录展开截断" in sent_text


@pytest.mark.asyncio
async def test_pikpak_runner_restores_expansion_truncation_on_retry():
    """目录展开截断元数据须持久化并在任务重试时恢复传递给 publish_pikpak_files."""
    from unittest.mock import patch

    from app.core.pipeline.models import PublishResult

    merged_payloads: list[dict] = []

    def fake_merge(_task_id: str, patch_dict: dict):
        merged_payloads.append(patch_dict)

    async def fake_expand(raw_items, **kwargs):
        kwargs["truncation_out"].update({"truncated": True, "max_files": 5000, "kept_files": 1})
        return [_parsed_file("f1", "a.mp4", is_dir=False)], {"d1": "folderA"}

    with (
        patch.object(pr.PikPakPublisherService, "expand_remote_files", side_effect=fake_expand),
        patch.object(TaskQueueRepository, "append_log_async", new_callable=AsyncMock),
        patch.object(TaskQueueRepository, "merge_params", side_effect=fake_merge),
    ):
        trunc_out: dict = {}
        items, _ = await pr._expand_pikpak_remote_files(
            "t_exp_persist",
            [{"file_id": "d1", "name": "folderA", "is_dir": True}],
            {},
            truncation_out=trunc_out,
        )
    assert len(items) == 1
    assert merged_payloads[-1]["expansion_truncated"] is True
    assert merged_payloads[-1]["expansion_limit"] == 5000

    retry_row = {
        "account_id": 1,
        "params": {
            "remote_files": [{"file_id": "f1", "name": "a.mp4", "is_dir": False, "size_bytes": 100}],
            "dir_tree": {"d1": "folderA"},
            "expansion_truncated": True,
            "expansion_limit": 5000,
        },
    }
    captured_kwargs: dict = {}

    async def fake_publish(**kwargs):
        captured_kwargs.update(kwargs)
        return PublishResult(published_count=1, total_files_count=1)

    with (
        patch.object(pr, "try_claim_executor_entry", return_value=True),
        patch.object(TaskQueueRepository, "update_status"),
        patch.object(TaskQueueRepository, "get_by_id", return_value=retry_row),
        patch.object(TaskQueueRepository, "append_log_async", new_callable=AsyncMock),
        patch.object(TaskQueueRepository, "update_progress_async", new_callable=AsyncMock),
        patch.object(pr.PikPakPublisherService, "publish_pikpak_files", side_effect=fake_publish),
        patch.object(pr, "_finalize_publish_result", new_callable=AsyncMock),
    ):
        await pr.run_publish_pikpak_task(
            "t_exp_persist",
            remote_files=retry_row["params"]["remote_files"],
            target_channel="-1001",
            dir_tree=retry_row["params"]["dir_tree"],
        )

    assert captured_kwargs["expansion_truncated"] is True
    assert captured_kwargs["expansion_limit"] == 5000


@pytest.mark.asyncio
async def test_publish_diagnostics_overwrite_and_preserve_manual_skips():
    task_id = "pr_diag_overwrite"
    manual_row = {
        "file_name": "manual.mp4",
        "rel_path": "manual.mp4",
        "oversize_kind": "manual_skip",
        "reason": "用户手动跳过",
    }
    stale_auto_row = {"file_name": "old.bin", "rel_path": "old.bin", "oversize_kind": "size", "reason": "超限"}
    TaskQueueRepository.create(task_id, "publish_folder", {"skipped_files": [manual_row, stale_auto_row]})
    try:
        res = PublishFolderResult(0, 0, [], [])
        await pr._persist_publish_diagnostics(task_id, res, "-100123")
        params = TaskQueueRepository.get_by_id(task_id)["params"]
        assert [s["file_name"] for s in params["skipped_files"]] == ["manual.mp4"], (
            "手动跳过行保留, 早前尝试的自动跳过行清除"
        )
        assert params["failed_files"] == {"items": [], "total": 0}, "失败表必须以空信封覆写"
        assert params["dedup_skipped"] == {"items": [], "total": 0}, "防重表必须以空信封覆写"
    finally:
        TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_publish_diagnostics_persist_latest_results():
    task_id = "pr_diag_latest"
    TaskQueueRepository.create(task_id, "publish_folder", {})
    try:
        res = PublishFolderResult(
            1,
            2,
            ["a.mp4"],
            [{"file_name": "big.mp4", "rel_path": "big.mp4", "oversize_kind": "size", "reason": "超限"}],
            newly_published_count=0,
            failed_details=[{"file_name": "a.mp4", "reason": "网络"}],
            dedup_skipped=[{"file_name": "d.mp4", "size_bytes": 1}],
        )
        await pr._persist_publish_diagnostics(task_id, res, "-100123")
        params = TaskQueueRepository.get_by_id(task_id)["params"]
        assert [s["file_name"] for s in params["skipped_files"]] == ["big.mp4"]
        assert params["failed_files"]["total"] == 1
        assert params["dedup_skipped"]["total"] == 1
        assert params["newly_published_count"] == 0
    finally:
        TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_skip_only_completed_broadcasts_100_percent(monkeypatch):
    task_id = "pr_skip_only"
    TaskQueueRepository.create(task_id, "publish_folder", {}, status="running")
    broadcasts: list = []
    monkeypatch.setattr(pr.connection_manager, "broadcast", AsyncMock(side_effect=broadcasts.append))
    monkeypatch.setattr(TaskQueueRepository, "update_progress", lambda *args, **kwargs: None)
    try:
        res = PublishFolderResult(
            0,
            2,
            [],
            [
                {"file_name": "a.mp4", "rel_path": "a.mp4", "oversize_kind": "size", "reason": "超限"},
                {"file_name": "b.mp4", "rel_path": "b.mp4", "oversize_kind": "size", "reason": "超限"},
            ],
        )
        await pr._finalize_publish_result(
            task_id, res, "-100123", SimpleNamespace(last_progress_bytes=lambda: (0, 0)), pikpak=False
        )
    finally:
        TaskQueueRepository.delete(task_id)

    progress_payload = next(p for p in broadcasts if p.get("type") == "progress")
    assert progress_payload["progress"]["percent"] == 100.0, "纯 skip 完成任务必须广播 100%"
    status_payload = next(p for p in broadcasts if p.get("type") == "task_update")
    assert status_payload["status"] == "completed"


@pytest.mark.asyncio
async def test_partial_failure_retains_real_percent(monkeypatch):
    task_id = "pr_partial"
    TaskQueueRepository.create(task_id, "publish_folder", {}, status="running")
    broadcasts: list = []
    monkeypatch.setattr(pr.connection_manager, "broadcast", AsyncMock(side_effect=broadcasts.append))
    monkeypatch.setattr(TaskQueueRepository, "update_progress", lambda *args, **kwargs: None)
    try:
        res = PublishFolderResult(1, 2, ["a.mp4"], [])
        await pr._finalize_publish_result(
            task_id, res, "-100123", SimpleNamespace(last_progress_bytes=lambda: (0, 0)), pikpak=False
        )
    finally:
        TaskQueueRepository.delete(task_id)

    progress_payload = next(p for p in broadcasts if p.get("type") == "progress")
    assert progress_payload["progress"]["percent"] == 50.0, "部分失败保留真实部分百分比"


@pytest.mark.asyncio
async def test_expanded_snapshot_preserves_codec_fields(monkeypatch):
    task_id = "pr_codec"
    TaskQueueRepository.create(task_id, "pikpak_publish", {})
    captured: dict = {}

    def fake_merge(tid, patch, **kwargs):
        captured.update(patch)

    monkeypatch.setattr(TaskQueueRepository, "merge_params", fake_merge)
    expanded = [
        ParsedFileItem(
            file_id="F2",
            name="a.mp4",
            is_dir=False,
            size_bytes=5,
            audit={},
            thumbnail_link=None,
            duration_s=None,
            width=None,
            height=None,
            parent_id="",
            audio_codec="aac",
            video_codec="h264",
        )
    ]
    monkeypatch.setattr(
        pr.PikPakPublisherService,
        "expand_remote_files",
        AsyncMock(return_value=(expanded, {"d": "F2"})),
    )
    try:
        await pr._expand_pikpak_remote_files(
            task_id,
            [{"file_id": "D1", "name": "d", "is_dir": True, "size_bytes": 0}],
            {},
            truncation_out={},
        )
    finally:
        TaskQueueRepository.delete(task_id)

    snapshot = captured["remote_files"]
    assert snapshot[0]["audio_codec"] == "aac", "expanded 快照必须保留 audio_codec"
    assert snapshot[0]["video_codec"] == "h264", "expanded 快照必须保留 video_codec"
    assert items_from_snapshot(snapshot)[0].audio_codec == "aac", "快照回读必须还原媒体描述符"


@pytest.mark.asyncio
async def test_pikpak_auth_error_bypasses_session_invalid_report(monkeypatch):
    task_id = "pr_auth_bypass"
    TaskQueueRepository.create(task_id, "pikpak_publish", {"target_channel": "-100123"})
    set_task_control(task_id)
    report_mock = AsyncMock()
    monkeypatch.setattr(pr, "report_session_invalid", report_mock)
    monkeypatch.setattr(pr, "try_claim_executor_entry", lambda tid: True)
    monkeypatch.setattr(pr, "_resolve_pikpak_run_config", AsyncMock(return_value=dict(_CFG_STUB)))
    monkeypatch.setattr(
        pr,
        "_expand_pikpak_remote_files",
        AsyncMock(side_effect=PikPakAuthError("PikPak 认证/授权失败 (UNAUTHORIZED)")),
    )
    try:
        with pytest.raises(PikPakAuthError):
            await pr.run_publish_pikpak_task(task_id, "-100123", [{"file_id": "f1", "is_dir": True}])
        assert report_mock.await_count == 0, "PikPak 凭据失败严禁上报为 Telegram 会话失效"
        assert task_id not in active_task_controls
        row = TaskQueueRepository.get_by_id(task_id)
        assert row["error_detail"] == "PikPak 认证/授权失败 (UNAUTHORIZED)"
    finally:
        active_task_controls.pop(task_id, None)
        TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_genuine_session_invalid_still_reported(monkeypatch):
    task_id = "pr_auth_control"
    TaskQueueRepository.create(task_id, "pikpak_publish", {"target_channel": "-100123"})
    set_task_control(task_id)
    report_mock = AsyncMock()
    monkeypatch.setattr(pr, "report_session_invalid", report_mock)
    monkeypatch.setattr(pr, "try_claim_executor_entry", lambda tid: True)
    monkeypatch.setattr(pr, "_resolve_pikpak_run_config", AsyncMock(return_value=dict(_CFG_STUB)))
    monkeypatch.setattr(
        pr,
        "_expand_pikpak_remote_files",
        AsyncMock(side_effect=RuntimeError("401 unauthorized from telegram")),
    )
    try:
        await pr.run_publish_pikpak_task(task_id, "-100123", [{"file_id": "f1", "is_dir": True}])
        assert report_mock.await_count == 1, "非 PikPak 域的会话失效文本仍必须走统一上报"
        assert task_id not in active_task_controls
    finally:
        active_task_controls.pop(task_id, None)
        TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("stopped_status", ["paused", "cancelled"])
async def test_finalize_publish_result_preserves_stop_committed_during_diagnostics(monkeypatch, stopped_status):
    task_id = f"pr_stop_race_{stopped_status}"
    TaskQueueRepository.create(task_id, "publish_folder", {}, status="running")
    broadcasts: list[dict[str, Any]] = []
    finalize_slots = AsyncMock()

    async def persist_and_stop(tid, *_args, **_kwargs):
        TaskQueueRepository.update_status(tid, stopped_status, from_statuses=("running",))
        set_task_control(tid, **{stopped_status: True})

    monkeypatch.setattr(pr, "_persist_publish_diagnostics", persist_and_stop)
    monkeypatch.setattr(pr, "_finalize_placeholder_slots", finalize_slots)
    monkeypatch.setattr(pr.connection_manager, "broadcast", AsyncMock(side_effect=broadcasts.append))
    try:
        res = PublishFolderResult(1, 2, ["a.mp4"], [])
        await pr._finalize_publish_result(
            task_id, res, "-100123", SimpleNamespace(last_progress_bytes=lambda: (0, 0)), pikpak=False
        )
        assert TaskQueueRepository.get_status(task_id) == stopped_status
        finalize_slots.assert_not_awaited()
        assert broadcasts == []
    finally:
        active_task_controls.pop(task_id, None)
        TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_claim_and_mark_running_rejects_deleted_task(monkeypatch):
    """调度器托管任务在调度窗口内被删除时，_claim_and_mark_running 必须返回 False 并释放 controls。"""
    from app.core.runtime import mark_scheduler_managed_executor

    task_id = "pr_deleted_claim"
    mark_scheduler_managed_executor(task_id)
    monkeypatch.setattr(pr, "try_claim_executor_entry", lambda _tid: True)
    try:
        claimed = await pr._claim_and_mark_running(task_id)
        assert claimed is False
        assert active_task_controls[task_id]["scheduler_managed"] is True
    finally:
        active_task_controls.pop(task_id, None)
