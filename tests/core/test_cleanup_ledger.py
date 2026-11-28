"""Offline safety checks for live and UI test cleanup."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call

import httpx
import pytest
from e2e_live.harness.cleanup import CleanupLedger

from app.config import settings
from app.db.database import db_cursor


def response(status):
    return httpx.Response(status, request=httpx.Request("DELETE", "http://test.invalid/task"))


def seed_receipts():
    with db_cursor(commit=True) as cursor:
        cursor.executemany(
            """INSERT INTO publish_history (task_id,target_chat_id,tg_message_id,file_path,file_size,published_at)
               VALUES (?,?,?, ?,1,0)""",
            [
                ("ours", "chat", 101, "a"),
                ("other", "chat", 102, "b"),
                ("ours", "chat", 103, "c"),
                ("ours", "elsewhere", 104, "d"),
                ("ours", "chat", 99, "old"),
            ],
        )


async def test_only_confirmed_messages_deleted(tmp_path):
    seed_receipts()
    ledger = CleanupLedger(db_path=Path(settings.DB_PATH))
    ledger.register_dir(tmp_path / "owned")
    (tmp_path / "owned").mkdir()
    ledger.register_watermark("chat", 100)
    ledger.register_task("ours")
    ledger.register_message("chat", 101)
    api = MagicMock()
    api.post.return_value = response(409)
    api.delete.return_value = response(200)
    reader = AsyncMock()
    reader.resolve_chat.return_value = "entity"
    await ledger.flush_async(api, reader)
    assert api.method_calls == [call.post("/api/tasks/ours/cancel"), call.delete("/api/tasks/ours")]
    reader.delete_messages.assert_awaited_once_with("entity", [101, 103])
    assert not (tmp_path / "owned").exists()


async def test_keep_messages_retires_tasks_and_retries_failed_delete(caplog, tmp_path):
    ledger = CleanupLedger(keep_messages=True, keep_dirs=True)
    kept = tmp_path / "kept"
    kept.mkdir()
    ledger.register_dir(kept)
    ledger.register_task("ours")
    ledger.register_message("chat", 101)
    api = MagicMock()
    api.post.return_value = response(200)
    api.delete.side_effect = [response(500), response(404)]
    reader = AsyncMock()
    await ledger.flush_async(api, reader)
    with pytest.raises(RuntimeError, match="ours"):
        ledger.check_finished()
    for _ in range(2):
        await ledger.flush_async(api, reader)
    ledger.check_finished()
    assert kept.exists()
    assert api.delete.call_count == 2
    assert "ours" in caplog.text
    reader.delete_messages.assert_not_awaited()


async def test_ui_cleanup_uses_only_registered_task_receipts(monkeypatch):
    from e2e_ui import test_ui_tasks as ui

    seed_receipts()
    state = {"registry": ["ours"], "target_chat": "chat", "taskA": "ours", "taskB": "", "mark": 100, "owned_ids": set()}
    monkeypatch.setattr(ui, "_live_state", state)
    monkeypatch.setattr(ui, "_http_request_sync", lambda *args: (200, {"status": "completed"}))
    reader = AsyncMock()
    reader.resolve_chat.return_value = "entity"
    reader.delete_messages.side_effect = [RuntimeError("disconnected"), None]
    with pytest.raises(RuntimeError, match="disconnected"):
        await ui._cleanup_live_tasks(reader, "chat", Path(settings.DB_PATH))
    assert not state["registry"] and state["owned_ids"] == {101, 103}
    await ui._cleanup_live_tasks(reader, "chat", Path(settings.DB_PATH))
    reader.delete_messages.assert_awaited_with("entity", [101, 103])
    assert not state["owned_ids"]


def test_telethon_session_is_confined(tmp_dir):
    from telethon import TelegramClient

    with pytest.raises(ValueError, match="outside test sandbox"):
        TelegramClient(str(Path(tmp_dir) / ".." / "unowned"), 1, "dummy")
    from telethon.sessions import MemorySession

    assert isinstance(TelegramClient(MemorySession(), 1, "dummy").session, MemorySession)


def test_scratch_clone_rejects_existing_directory(tmp_path, monkeypatch):
    from e2e_live.harness import scratch_env

    src = tmp_path / "source"
    src.mkdir()
    monkeypatch.setenv("E2E_LIVE_SOURCE_DATA_DIR", str(src))
    dest = tmp_path / "existing"
    dest.mkdir()
    sentinel = dest / "user.session"
    sentinel.write_bytes(b"owned elsewhere")
    with pytest.raises(FileExistsError):
        scratch_env.clone_source_data(dest)
    with pytest.raises(ValueError, match="scratch"):
        scratch_env.clone_source_data(src / "nested")
    assert sentinel.read_bytes() == b"owned elsewhere"
