"""ClientManager 测试种子助手 (pkg 1.4: 兼容 facade/_fallback 影子状态删除后的统一注入面)

构造真实 TelegramAccountClient / TelegramBotClient 对象 (client 槽位挂 mock)。
派生只读属性 (userbot/bots/auth_state/is_premium/...) 从这些真实对象派生 —— 注入即生效。

两种用法:
- monkeypatch 作用域: acc = build_default_account(...); monkeypatch.setattr(client_manager, "_accounts", {acc.account_id: acc})
  (monkeypatch 收口自动还原, 零跨用例污染)
- 手工 save/restore: seed_default_account(...) 直接插入 + unseed_accounts(...) 清理
"""

from collections.abc import Sequence


def build_default_account(
    client=None,
    *,
    account_id: int = 1,
    account_name: str = "seeded_userbot",
    auth_state: str = "AUTHENTICATED",
    is_premium: bool = False,
    premium_checked_at: float = 0.0,
    session_file: str = "seeded_userbot.session",
):
    """构造默认主账号对象 (is_default=True), client 传 mock 或 None; 不做任何插入。

    client 为 mock 时统一钉死 get_me → premium=is_premium: 防止 probe_premium 对
    AsyncMock 真跑出 truthy premium (使分卷阈值/超限判定整体错位)。"""
    from app.core.client_manager import TelegramAccountClient

    acc = TelegramAccountClient(
        account_id=account_id,
        account_name=account_name,
        phone="+12025550112",
        session_file=session_file,
        api_id=1,
        api_hash="seeded_hash",
        is_active=True,
        is_default=True,
    )
    acc.client = client
    acc.auth_state = auth_state
    acc.is_premium = is_premium
    acc.premium_checked_at = premium_checked_at
    if client is not None:
        from types import SimpleNamespace
        from unittest.mock import AsyncMock, MagicMock

        try:
            client.get_me = AsyncMock(
                return_value=SimpleNamespace(premium=bool(is_premium), id=1, username=account_name)
            )
            if hasattr(client, "is_connected") and isinstance(client.is_connected, AsyncMock):
                client.is_connected = MagicMock(return_value=True)
            if hasattr(client, "_connection") and isinstance(client._connection, AsyncMock):
                client._connection = MagicMock()
        except Exception:
            pass
    return acc


def seed_default_account(client_manager, client=None, **kwargs):
    """插入/覆盖默认主账号 (直接写 _accounts), 返回账号对象。手工 restore 场景用。"""
    acc = build_default_account(client, **kwargs)
    client_manager._accounts[acc.account_id] = acc
    return acc


def unseed_accounts(client_manager) -> None:
    """清空全部种子账号 (恢复无账号态: userbot=None / auth_state=UNAUTHENTICATED)"""
    client_manager._accounts.clear()


def make_bot(client=None, *, bot_id: int = 1):
    """构造就绪态 Bot 对象 (status='ready', client 挂 mock) —— 轮询路径可直取。"""
    from app.core.client_manager import TelegramBotClient

    bot = TelegramBotClient(
        bot_id=bot_id,
        bot_token=f"{bot_id}:seeded_token",
        session_name=f"bot_seed_{bot_id}",
        api_id=1,
        api_hash="seeded_hash",
    )
    bot.client = client
    bot.status = "ready"
    if client is not None:
        from unittest.mock import AsyncMock, MagicMock

        try:
            if hasattr(client, "is_connected") and isinstance(client.is_connected, AsyncMock):
                client.is_connected = MagicMock(return_value=True)
            if hasattr(client, "_connection") and isinstance(client._connection, AsyncMock):
                client._connection = MagicMock()
        except Exception:
            pass
    return bot


def seed_bots(client_manager, clients: Sequence[object | None], *, bot_index: int = 0) -> list:
    """清空并以给定 client 列表重建真实 Bot 池 (bot_id 从 1 递增), 返回 Bot 对象列表。"""
    client_manager._bots.clear()
    bots: list[object] = []
    for i, client in enumerate(clients, start=1):
        bot = make_bot(client, bot_id=i)
        client_manager._bots[i] = bot
        bots.append(bot)
    client_manager._bot_index = bot_index
    return bots
