"""Persist confirmed album members before control returns to the publish pipeline."""

import asyncio
import sqlite3
from typing import Any

from telethon.errors import BadRequestError, FloodWaitError, ForbiddenError

from app.core.pipeline.models import PublishContext, PublishItem
from app.core.scheduler import FatalTaskError
from app.db.download_repository import DownloadHistoryRepository
from app.db.publish_repository import PublishHistoryRepository
from app.db.task_repository import TaskQueueRepository


class UnconfirmedAlbumError(FatalTaskError):
    def __init__(self) -> None:
        super().__init__("相册发送结果未确认，请核对目标消息和任务中的 pending_album_delivery 后恢复")


async def send_album_segment(
    client: Any,
    context: PublishContext,
    items: list[PublishItem],
    segment: list[tuple[int, Any]],
    *,
    is_pikpak: bool,
    single_caption: str,
) -> tuple[Any, bool]:
    members = [items[index].item_id for index, _ in segment]
    tracked = False
    if context.task_id:
        try:
            tracked = await asyncio.to_thread(TaskQueueRepository.update_album_delivery, context.task_id, members)
        except Exception as exc:
            raise UnconfirmedAlbumError() from exc
    media = [handle for _, handle in segment]
    options: dict[str, Any] = {"caption": single_caption if len(media) == 1 else None}
    if is_pikpak or len(media) == 1:
        options["force_document"] = False
    try:
        sent = await client.send_file(context.target_chat_id, media[0] if len(media) == 1 else media, **options)
    except (BadRequestError, ForbiddenError, FloodWaitError):
        if tracked:
            await asyncio.to_thread(
                TaskQueueRepository.update_album_delivery, context.task_id, members, finish=lambda _: None
            )
        raise
    except Exception as exc:
        if tracked:
            raise UnconfirmedAlbumError() from exc
        raise

    if tracked:
        messages = sent if isinstance(sent, list) else [sent]

        def record_confirmed(cursor: sqlite3.Cursor) -> None:
            for (index, _), message in zip(segment, messages, strict=False):
                msg_id = int(getattr(message, "id", 0) or 0)
                if msg_id:
                    item = items[index]
                    PublishHistoryRepository.record(
                        context.target_chat_id,
                        f"pikpak://{item.parent_folder_id}/{item.name}" if is_pikpak else item.item_id,
                        item.size_bytes,
                        "" if is_pikpak else DownloadHistoryRepository.get_hash_by_path(item.item_id),
                        source_type="pikpak" if is_pikpak else "local",
                        remote_file_id=item.item_id if is_pikpak else "",
                        tg_message_id=msg_id,
                        task_id=context.task_id,
                        transaction=cursor,
                    )

        try:
            await asyncio.to_thread(
                TaskQueueRepository.update_album_delivery, context.task_id, members, finish=record_confirmed
            )
        except Exception as exc:
            raise UnconfirmedAlbumError() from exc
    return sent, tracked
