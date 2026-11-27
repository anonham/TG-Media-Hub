"""Bound the uncertain interval between a split-volume send and its durable receipt."""

from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from telethon.errors import BadRequestError, FloodWaitError

from app.core.pikpak.taxonomy import PikPakPublishFatalError

PartReceipt = Callable[[int], Awaitable[None]]
_active_receipt: ContextVar[PartReceipt | None] = ContextVar("part_receipt", default=None)


class UnconfirmedPartError(PikPakPublishFatalError):
    def __init__(self) -> None:
        super().__init__("telegram_send_failed", "分卷发送结果未确认，请核对目标消息后再恢复任务")


@contextmanager
def part_receipt_scope(receipt: PartReceipt | None) -> Iterator[None]:
    token = _active_receipt.set(receipt)
    try:
        yield
    finally:
        _active_receipt.reset(token)


async def send_volume_file(client: Any, chat: Any, media: Any, **kwargs: Any) -> Any:
    receipt = _active_receipt.get()
    if receipt is None:
        return await client.send_file(chat, media, **kwargs)
    await receipt(-1)
    try:
        return await client.send_file(chat, media, **kwargs)
    except (BadRequestError, FloodWaitError):
        # Telegram rejected the request. Only a confirmed rejection releases this attempt.
        await receipt(0)
        raise
    except Exception as exc:
        raise UnconfirmedPartError() from exc
