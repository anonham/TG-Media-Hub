"""CleanupLedger 单元测试 (覆盖任务取消与反转删除、水位重扫合并去重、目录清理)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.e2e_live.harness.cleanup import CleanupLedger


@pytest.mark.asyncio
async def test_cleanup_ledger_reverse_order_and_watermark_rescan(tmp_path: Path) -> None:
    ledger = CleanupLedger(keep_messages=False, keep_dirs=False)

    test_dir = tmp_path / "scratch_test"
    test_dir.mkdir()
    (test_dir / "file.txt").write_text("data", encoding="utf-8")

    ledger.register_dir(test_dir)
    ledger.register_task("task_001")
    ledger.register_task("task_002")
    ledger.register_message("-100123", 101)
    ledger.register_message("-100123", 102)
    ledger.register_watermark("-100123", 100)

    # 模拟 API 客户端
    mock_api = MagicMock()
    mock_api.post = MagicMock(return_value={"status": "ok"})
    mock_api.delete = MagicMock(return_value={"status": "deleted"})

    # 模拟 reader 客户端
    mock_reader = MagicMock()
    mock_reader.resolve_chat = AsyncMock(return_value="chat_entity_-100123")
    # 水位 100 之后发现了 102 (重复), 103 (新)
    mock_reader.collect_ids_after = AsyncMock(return_value=[102, 103])
    mock_reader.delete_messages = AsyncMock()

    await ledger.flush_async(api=mock_api, reader=mock_reader)

    # 验证任务先取消后删除
    assert mock_api.post.call_count == 2
    mock_api.post.assert_any_call("/api/tasks/task_001/cancel")
    mock_api.post.assert_any_call("/api/tasks/task_002/cancel")
    assert mock_api.delete.call_count == 2
    mock_api.delete.assert_any_call("/api/tasks/task_001")
    mock_api.delete.assert_any_call("/api/tasks/task_002")

    # 验证水位重扫
    mock_reader.resolve_chat.assert_called_with("-100123")
    mock_reader.collect_ids_after.assert_awaited_once_with("chat_entity_-100123", 100)

    # 验证去重并删除 [101, 102, 103]
    mock_reader.delete_messages.assert_awaited_once_with("chat_entity_-100123", [101, 102, 103])

    # 验证目录清理
    assert not test_dir.exists()


@pytest.mark.asyncio
async def test_cleanup_ledger_keep_flags(tmp_path: Path) -> None:
    ledger = CleanupLedger(keep_messages=True, keep_dirs=True)

    test_dir = tmp_path / "kept_dir"
    test_dir.mkdir()
    ledger.register_dir(test_dir)
    ledger.register_message("-100999", 500)
    ledger.register_watermark("-100999", 400)

    mock_reader = MagicMock()
    mock_reader.delete_messages = AsyncMock()

    await ledger.flush_async(reader=mock_reader)

    # keep_messages 为 True 时不调用删除
    mock_reader.delete_messages.assert_not_called()
    # keep_dirs 为 True 时保留目录
    assert test_dir.exists()
