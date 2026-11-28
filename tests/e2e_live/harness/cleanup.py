"""统一清理 : 任务取消与删除 → 有归属回执的消息删除 → 目录删除。

用例登记发送结果，收尾时保留失败项并报告剩余任务和消息。
"""

from __future__ import annotations

import asyncio
import logging
import shutil
from contextlib import closing
from pathlib import Path
from typing import TYPE_CHECKING

from .db_readonly import connect_ro

if TYPE_CHECKING:
    from .api_client import LiveApiClient
    from .tg_reader import TgReader

logger = logging.getLogger(__name__)


def published_message_ids(db_path: Path, task_id: str, chat_ref: str, after: int) -> list[int]:
    """Only receipts from this task and destination can authorize a deletion."""
    with closing(connect_ro(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT tg_message_id FROM publish_history
             WHERE task_id = :task AND target_chat_id = :chat AND tg_message_id > :mark
            UNION SELECT placeholder_msg_id FROM placeholder_slots
             WHERE task_id = :task AND target_chat_id = :chat AND placeholder_msg_id > :mark
            UNION SELECT real_msg_id FROM placeholder_slots
             WHERE task_id = :task AND target_chat_id = :chat AND real_msg_id > :mark
            """,
            {"task": task_id, "chat": chat_ref, "mark": after},
        )
        return [row[0] for row in rows]


class CleanupLedger:
    """发送/创建账本: register_* 登记 → flush_async / flush_sync 清理。"""

    def __init__(self, keep_messages: bool = False, keep_dirs: bool = False, db_path: Path | None = None) -> None:
        self._messages: dict[str, list[int]] = {}
        self._watermarks: dict[str, int] = {}
        self._db_path = db_path
        self._tasks: dict[str, dict[str, int]] = {}
        self._dirs: list[Path] = []
        self._keep_messages = keep_messages
        self._keep_dirs = keep_dirs

    def register_message(self, chat_ref: str, msg_id: int) -> None:
        self._messages.setdefault(chat_ref, []).append(msg_id)

    def register_watermark(self, chat_ref: str, max_id: int) -> None:
        """记录起始水位，防止清理复用的旧卡片。水位本身不能确认归属。"""
        self._watermarks[chat_ref] = max_id

    def register_task(self, task_id: str) -> None:
        self._tasks.setdefault(task_id, self._watermarks.copy())

    def register_dir(self, path: Path) -> None:
        if path not in self._dirs:
            self._dirs.append(path)

    async def _retire_task(self, api: LiveApiClient, task_id: str, marks: dict[str, int]) -> None:
        cancelled = api.post(f"/api/tasks/{task_id}/cancel")
        if cancelled.status_code not in (404, 409):
            cancelled.raise_for_status()
        for attempt in range(4):
            if self._db_path is not None and not self._keep_messages:
                for chat_ref, mark in marks.items():
                    for msg_id in published_message_ids(self._db_path, task_id, chat_ref, mark):
                        self.register_message(chat_ref, msg_id)
            deleted = api.delete(f"/api/tasks/{task_id}")
            # A cancelled executor can briefly hold the task row while finishing.
            if deleted.status_code in (400, 409) and attempt < 3:
                await asyncio.sleep(1)
                continue
            if deleted.status_code != 404:
                deleted.raise_for_status()
            break

    async def flush_async(self, api: LiveApiClient | None = None, reader: TgReader | None = None) -> None:
        """完整清理: 任务取消与删除 → 有归属回执的消息删除 → 目录删除。

        清理顺序反转为「任务优先，消息其后」：先取消并删除未终局任务，
        避免删除消息后进行中的推流任务继续写出孤儿消息。
        """
        # 1. 任务取消与删除
        if api is not None:
            for task_id, marks in list(self._tasks.items()):
                try:
                    await self._retire_task(api, task_id, marks)
                    del self._tasks[task_id]
                except Exception as exc:
                    logger.warning("清理任务行失败 (task=%s): %s", task_id, exc)

        # 3. 消息删除 (集合去重)
        if reader is not None and self._messages and not self._keep_messages:
            for chat_ref, msg_ids in list(self._messages.items()):
                unique_ids = sorted(set(msg_ids))
                if not unique_ids:
                    continue
                try:
                    chat_entity = await reader.resolve_chat(chat_ref)
                    await reader.delete_messages(chat_entity, unique_ids)
                    del self._messages[chat_ref]
                except Exception as exc:
                    logger.warning("清理频道消息失败 (chat=%s, ids=%s): %s", chat_ref, unique_ids, exc)
        elif self._messages and not self._keep_messages:
            logger.warning("缺少 reader，保留未清理的测试消息: %s", self._messages)

        # 4. 目录清理
        self.flush_dirs()

    def check_finished(self) -> None:
        messages = {} if self._keep_messages else self._messages
        if self._tasks or messages:
            raise RuntimeError(f"未清理的测试任务 {list(self._tasks)}；未清理的频道消息 {messages}")

    def flush_sync(self) -> None:
        """同步兜底: 仅目录清理 (消息/任务删除由异步用例经 flush_async 承担)。"""
        self.flush_dirs()

    def flush_dirs(self) -> None:
        if self._keep_dirs:
            return
        for path in list(self._dirs):
            shutil.rmtree(path, ignore_errors=True)
        self._dirs.clear()
