"""Telethon 只读端 。

惰性导入 telethon; 只读语义 = 不发送任何新消息, 删除仅作用于清理账本登记的
msg_id。FloodWait 是预期产物: 内置 `seconds + 1` 退避重试, 上限 3 次。
"""

import asyncio
import io
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_FLOODWAIT_RETRIES = 3


class ChatUnresolvableError(RuntimeError):
    """裸 ID 在该会话 (含全量对话缓存重建后) 仍不可解析 —— 该账号不可见目标频道。"""


def is_video_message(message: Any) -> bool:
    return getattr(message, "video", None) is not None


def is_photo_message(message: Any) -> bool:
    """Photo 形态 (占位海报卡): 有 photo 预览且非视频文档。"""
    return getattr(message, "photo", None) is not None and not is_video_message(message)


def is_document_message(message: Any) -> bool:
    """裸文件附件形态 (raw 物理分卷): document 存在且非视频。"""
    return getattr(message, "document", None) is not None and not is_video_message(message)


class TgReader:
    """复用 scratch 克隆 session 的只读 Telethon 客户端; connect() 由用例按需调用。"""

    def __init__(self, session_file: Path, api_id: int, api_hash: str) -> None:
        self._session_file = session_file
        self._api_id = api_id
        self._api_hash = api_hash
        self._client: Any | None = None

    async def connect(self) -> None:
        if self._client is not None:
            return
        from telethon import TelegramClient

        client = TelegramClient(
            str(self._session_file),
            self._api_id,
            self._api_hash,
            device_model="tg-hub-e2e-live-reader",
        )
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            raise RuntimeError(f"session 未授权: {self._session_file} (克隆源或源服务状态异常)")
        self._client = client

    async def aclose(self) -> None:
        if self._client is not None:
            client, self._client = self._client, None
            await client.disconnect()

    @property
    def connected(self) -> bool:
        return self._client is not None

    @property
    def session_file(self) -> Path:
        """本实例独占的会话文件路径 (E2E-LIVE-17 mirror 子进程据此复制会话束)。"""
        return self._session_file

    async def _call(self, fn: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
        from telethon import errors

        for attempt in range(_FLOODWAIT_RETRIES + 1):
            try:
                return await fn(*args, **kwargs)
            except errors.FloodWaitError as exc:
                if attempt >= _FLOODWAIT_RETRIES:
                    raise
                logger.warning("FloodWait %ss (第 %d 次退避): %s", exc.seconds, attempt + 1, fn)
                await asyncio.sleep(exc.seconds + 1)

    async def resolve_chat(self, chat_ref: str) -> Any:
        """解析频道实体 (与 app/core/entity_resolver.resolve_entity 同机制): 先按
        parse_chat_identifier 归一 (字符串 ID → int), 失败时全量重建对话缓存
        (上限 2000, 覆盖大对话量账号) 后重试; 仍不可解析抛 ChatUnresolvableError。"""
        from app.core.entity_resolver import parse_chat_identifier

        await self.connect()
        parsed = parse_chat_identifier(chat_ref)
        try:
            return await self._call(self._client.get_entity, parsed)  # type: ignore[union-attr]
        except Exception:
            logger.debug("get_entity 首次解析失败, 重建会话实体缓存后重试: %s", chat_ref)
            count = 0
            async for _dialog in self._client.iter_dialogs(limit=2000):  # type: ignore[union-attr]
                count += 1
            logger.debug("会话实体缓存重建完成 (%d 个对话)", count)
            try:
                return await self._call(self._client.get_entity, parsed)  # type: ignore[union-attr]
            except ValueError as exc:
                if "Cannot find any entity" in str(exc):
                    raise ChatUnresolvableError(f"{chat_ref} 在该会话不可解析 (账号不可见该频道)") from exc
                raise

    async def send_media(self, chat_ref: Any, file_path: Path, force_document: bool = True) -> Any:
        """setup 专用: 为 download/forward 用例在测试频道投放样本媒体。

        force_document=True 时字节保真 (Telegram 不重压缩), 可作下载大小断言锚点;
        断言路径仍严格只读, 本方法不进入任何断言通道。
        """
        await self.connect()
        return await self._call(self._client.send_file, chat_ref, str(file_path), force_document=force_document)  # type: ignore[union-attr]

    async def send_text(self, chat_ref: Any, text: str) -> Any:
        """setup 专用: 投放非媒体消息 (range 用例的跳过计数锚点)。"""
        await self.connect()
        return await self._call(self._client.send_message, chat_ref, text)  # type: ignore[union-attr]

    async def max_message_id(self, chat_ref: Any) -> int:
        """频道当前最大消息 id (水位), 无消息返回 0。"""
        await self.connect()
        msgs = await self._call(self._client.get_messages, chat_ref, limit=1)  # type: ignore[union-attr]
        return int(msgs[0].id) if msgs else 0

    async def collect_messages_after(self, chat_ref: Any, mark: int, limit: int = 500) -> list[Any]:
        """收集水位之后产生的全部消息 (清理账本发现通道 + 产物形态断言)。"""
        await self.connect()
        out: list[Any] = []
        async for msg in self._client.iter_messages(chat_ref, limit=limit, min_id=mark):  # type: ignore[union-attr]
            out.append(msg)
        return out

    async def collect_ids_after(self, chat_ref: Any, mark: int, limit: int = 500) -> list[int]:
        """collect_messages_after 的 id 简化形态 (清理账本登记用)。"""
        return [int(msg.id) for msg in await self.collect_messages_after(chat_ref, mark, limit)]

    async def get_entity(self, chat_ref: Any) -> Any:
        await self.connect()
        return await self._call(self._client.get_entity, chat_ref)  # type: ignore[union-attr]

    async def get_messages(self, chat_ref: Any, msg_ids: list[int]) -> list[Any]:
        """按 id 批量拉取 (只读); 不存在的 id 对应位置为 None。"""
        await self.connect()
        return list(await self._call(self._client.get_messages, chat_ref, ids=msg_ids))  # type: ignore[union-attr]

    async def download_thumb(self, message: Any) -> bytes | None:
        """只读下载消息缩略图字节 (封面亮度断言通道; thumb=-1 取最大缩略图)。

        Telethon 陷阱:
        1. thumb=True 在 Python 中等同于整数索引 1 (isinstance(True, int) 为 True),
           仅有 1 张缩略图时直接 IndexError, 多张时取到次小而非最大; 必须传 thumb=-1。
        2. 不传 file 时会落盘到进程 CWD 并返回路径字符串 —— 显式传 BytesIO 内存缓冲取字节。
        """
        await self.connect()

        async def _download_once() -> bytes:
            buf = io.BytesIO()
            await self._client.download_media(message, file=buf, thumb=-1)  # type: ignore[union-attr]
            return buf.getvalue()

        data = await self._call(_download_once)
        return data or None

    async def iter_recent(self, chat_ref: Any, limit: int = 50) -> list[Any]:
        await self.connect()
        out: list[Any] = []
        async for msg in self._client.iter_messages(chat_ref, limit=limit):  # type: ignore[union-attr]
            out.append(msg)
        return out

    async def delete_messages(self, chat_ref: Any, msg_ids: list[int]) -> None:
        """清理通道: 仅删除账本登记的 msg_id (§3.2 清理动作口径), 非探测路径。"""
        await self.connect()
        await self._call(self._client.delete_messages, chat_ref, msg_ids)  # type: ignore[union-attr]
