"""Account ranking and Telegram channel probes used by the client pool."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from app.core.entity_resolver import parse_chat_identifier, probe_write_permission, resolve_entity

if TYPE_CHECKING:
    from app.core.client_manager import TelegramAccountClient

logger = logging.getLogger(__name__)


def ranked_candidates(
    accounts: list[TelegramAccountClient], task_type: str, *, allow_saturated: bool = False
) -> list[TelegramAccountClient]:
    """Prefer spare capacity, then secondary download accounts, then the lowest load."""
    candidates = [account for account in accounts if account.active_task_count < account.max_concurrency]
    if not candidates and allow_saturated:
        candidates = list(accounts)
    prefer_secondary = task_type in ("download", "download_link", "download_range")
    return sorted(
        candidates, key=lambda account: (prefer_secondary and account.tier != "secondary", account.active_task_count)
    )


async def preferred_channel_account(
    accounts: list[TelegramAccountClient], task_type: str, target_chat: str | int, *, require_write: bool
) -> TelegramAccountClient | None:
    chat = target_chat
    if not require_write:
        try:
            chat = parse_chat_identifier(target_chat)
        except Exception as exc:
            logger.debug("目标会话标识符标准化解析失败，回退原始标识符 %s: %s", target_chat, exc)

    candidates = ranked_candidates(accounts, task_type, allow_saturated=not require_write)
    for account in candidates:
        try:
            client = account.client
            if not client or not client.is_connected():
                continue
            probe = probe_write_permission(client, chat) if require_write else resolve_entity(client, chat)
            if await asyncio.wait_for(probe, timeout=3.0):
                logger.info("账号 [%s] 通过目标频道 [%s] 权限探测，优先指派", account.account_name, target_chat)
                return account
        except Exception as exc:
            logger.debug("账号 [%s] 探测目标频道 [%s] 失败: %s", account.account_name, target_chat, exc)
    if candidates:
        logger.warning("账号池中未探测到频道 [%s] 的所需权限，回退至常规负载均衡", target_chat)
    return None


async def probe_account_access(
    account: TelegramAccountClient, chat: str | int, *, require_write: bool, rpc_timeout: float
) -> tuple[TelegramAccountClient | None, Any]:
    """Connect on demand and check write permission on the same resolved entity."""
    try:
        if not account.client or not account.client.is_connected():
            await account.connect()
        client = account.client
        if not client or not client.is_connected():
            return None, None
        entity = await asyncio.wait_for(resolve_entity(client, chat), timeout=rpc_timeout)
        if not entity:
            return None, None
        if require_write and not await asyncio.wait_for(probe_write_permission(client, entity), timeout=rpc_timeout):
            return None, None
        return account, entity
    except Exception as exc:
        logger.debug("账号 [%s] 探测频道 [%s] 失败: %s", account.account_name, chat, exc)
        return None, None
