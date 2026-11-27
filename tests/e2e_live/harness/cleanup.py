"""统一清理 : 任务取消与删除 → 水位重扫与消息删除 → 目录删除。

用例过程登记, finally 中 flush (断言失败也执行)。清理失败仅记 WARNING 不抛出
——清理不得掩盖测试结果, 但必须有痕 (F4 精神)。
"""

from __future__ import annotations

import contextlib
import logging
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .api_client import LiveApiClient
    from .tg_reader import TgReader

logger = logging.getLogger(__name__)


class CleanupLedger:
    """发送/创建账本: register_* 登记 → flush_async / flush_sync 清理。"""

    def __init__(self, keep_messages: bool = False, keep_dirs: bool = False) -> None:
        self._messages: dict[str, list[int]] = {}
        self._watermarks: dict[str, int] = {}
        self._tasks: list[str] = []
        self._dirs: list[Path] = []
        self._keep_messages = keep_messages
        self._keep_dirs = keep_dirs

    def register_message(self, chat_ref: str, msg_id: int) -> None:
        self._messages.setdefault(chat_ref, []).append(msg_id)

    def register_watermark(self, chat_ref: str, max_id: int) -> None:
        """登记测试开始前的频道最大消息 ID 水位。flush 时若存在更高 ID 将自动发现并清理。"""
        if chat_ref in self._watermarks:
            self._watermarks[chat_ref] = min(self._watermarks[chat_ref], max_id)
        else:
            self._watermarks[chat_ref] = max_id

    def register_task(self, task_id: str) -> None:
        if task_id not in self._tasks:
            self._tasks.append(task_id)

    def register_dir(self, path: Path) -> None:
        if path not in self._dirs:
            self._dirs.append(path)

    async def flush_async(self, api: LiveApiClient | None = None, reader: TgReader | None = None) -> None:
        """完整清理: 任务取消与删除 → 水位重扫与消息删除 → 目录删除。

        清理顺序反转为「任务优先，消息其后」：先取消并删除未终局任务，
        避免删除消息后进行中的推流任务继续写出孤儿消息。
        """
        # 1. 任务取消与删除
        if api is not None:
            for task_id in list(self._tasks):
                try:
                    # 先尝试取消运行中的任务，防止持续发送新消息
                    with contextlib.suppress(Exception):
                        api.post(f"/api/tasks/{task_id}/cancel")
                    api.delete(f"/api/tasks/{task_id}")
                except Exception as exc:
                    logger.warning("清理任务行失败 (task=%s): %s", task_id, exc)
            self._tasks.clear()

        # 2. 水位重扫与消息合并 (自动探测断言失败前已产生但未及手工登记的消息)
        if reader is not None and not self._keep_messages:
            for chat_ref, mark in list(self._watermarks.items()):
                try:
                    chat_entity = await reader.resolve_chat(chat_ref)
                    ids_after = await reader.collect_ids_after(chat_entity, mark)
                    if ids_after:
                        existing = self._messages.setdefault(chat_ref, [])
                        existing.extend(ids_after)
                except Exception as exc:
                    logger.warning("水位重扫收集消息失败 (chat=%s, mark=%s): %s", chat_ref, mark, exc)
            self._watermarks.clear()

        # 3. 消息删除 (集合去重)
        if reader is not None and self._messages and not self._keep_messages:
            for chat_ref, msg_ids in list(self._messages.items()):
                unique_ids = sorted(set(msg_ids))
                if not unique_ids:
                    continue
                try:
                    chat_entity = await reader.resolve_chat(chat_ref)
                    await reader.delete_messages(chat_entity, unique_ids)
                except Exception as exc:
                    logger.warning("清理频道消息失败 (chat=%s, ids=%s): %s", chat_ref, unique_ids, exc)
            self._messages.clear()

        # 4. 目录清理
        self.flush_dirs()

    def flush_sync(self) -> None:
        """同步兜底: 仅目录清理 (消息/任务删除由异步用例经 flush_async 承担)。"""
        self.flush_dirs()

    def flush_dirs(self) -> None:
        if self._keep_dirs:
            return
        for path in list(self._dirs):
            shutil.rmtree(path, ignore_errors=True)
        self._dirs.clear()
