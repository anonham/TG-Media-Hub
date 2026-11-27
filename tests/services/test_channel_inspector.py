"""频道探针写权限判定回归 (ChannelInspector inspect_channel)

覆盖 三处判定缺陷:
- change_info 不等价于频道发帖权 (post_messages)
- 超级群管理员不被全员禁言误伤 (post_messages 是广播频道专属权)
- 媒体细分封禁 (send_videos/send_photos/send_audios/send_docs) 纳入写诊断"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.channel_inspector import ChannelInspector


def _acc(client: AsyncMock) -> MagicMock:
    acc = MagicMock()
    acc.probe_premium = AsyncMock(return_value=False)
    acc.is_premium = False
    acc.effective_max_file_size_bytes.return_value = 2000 * 1024 * 1024
    acc.account_id = 1
    acc.account_name = "userbot-1"
    acc.client = client
    acc.is_ready.return_value = True
    acc.client.is_connected = MagicMock(return_value=True)
    return acc


def _patch_env(entity):
    """固定账号调度与实体解析, 只让判定逻辑成为变量"""
    client = AsyncMock()
    client.get_messages = AsyncMock(return_value=MagicMock())  # 读权限探针通过
    acc = _acc(client)
    return (
        patch("app.services.channel_inspector.client_manager.get_account", return_value=acc),
        patch("app.services.channel_inspector.ForwardService.resolve_entity", AsyncMock(return_value=entity)),
        client,
    )


@pytest.mark.asyncio
async def test_broadcast_admin_change_info_does_not_grant_posting():
    """频道管理员仅有 change_info 时不得判定可发帖"""
    entity = SimpleNamespace(
        broadcast=True,
        megagroup=False,
        creator=False,
        admin_rights=SimpleNamespace(
            change_info=True,
            post_messages=False,
            edit_messages=False,
            delete_messages=False,
            ban_users=False,
            invite_users=False,
            pin_messages=False,
            add_admins=False,
        ),
        default_banned_rights=None,
        banned_rights=None,
        title="频道",
    )
    p1, p2, _ = _patch_env(entity)
    with p1, p2:
        res = await ChannelInspector.inspect_channel("@chan", account_id=1)
    assert res["has_write_permission"] is False
    assert "无发帖权限" in res["permission_summary"]


@pytest.mark.asyncio
async def test_megagroup_admin_not_muted_by_default_bans():
    """超级群管理员 (任一管理权) 不落入全员禁言分支"""
    entity = SimpleNamespace(
        broadcast=False,
        megagroup=True,
        creator=False,
        admin_rights=SimpleNamespace(
            change_info=True,
            post_messages=False,  # Telethon 配置超级群管理员时会移除该位
            edit_messages=False,
            delete_messages=True,
            ban_users=False,
            invite_users=False,
            pin_messages=False,
            add_admins=False,
        ),
        default_banned_rights=SimpleNamespace(send_messages=True, send_media=True),
        banned_rights=None,
        title="群",
    )
    p1, p2, _ = _patch_env(entity)
    with p1, p2:
        res = await ChannelInspector.inspect_channel("@group", account_id=1)
    assert res["has_write_permission"] is True
    assert "群组管理权限" in res["permission_summary"]


@pytest.mark.asyncio
async def test_megagroup_media_specific_ban_detected():
    """全员仅封视频/文档时不得报告无条件可写"""
    entity = SimpleNamespace(
        broadcast=False,
        megagroup=True,
        creator=False,
        admin_rights=None,
        default_banned_rights=SimpleNamespace(
            send_messages=False,
            send_media=False,
            send_videos=True,
            send_photos=False,
            send_audios=False,
            send_docs=True,
        ),
        banned_rights=None,
        title="群",
    )
    p1, p2, _ = _patch_env(entity)
    with p1, p2:
        res = await ChannelInspector.inspect_channel("@group", account_id=1)
    assert res["has_write_permission"] is False
    assert "禁止发送媒体" in res["permission_summary"]


@pytest.mark.asyncio
async def test_megagroup_ordinary_member_unrestricted_can_write():
    """无管理权且无封禁的普通成员保持可写判定 (回归护栏)"""
    entity = SimpleNamespace(
        broadcast=False,
        megagroup=True,
        creator=False,
        admin_rights=None,
        default_banned_rights=SimpleNamespace(
            send_messages=False,
            send_media=False,
            send_videos=False,
            send_photos=False,
            send_audios=False,
            send_docs=False,
        ),
        banned_rights=None,
        title="群",
    )
    p1, p2, _ = _patch_env(entity)
    with p1, p2:
        res = await ChannelInspector.inspect_channel("@group", account_id=1)
    assert res["has_write_permission"] is True
    assert "群成员正常发言权限" in res["permission_summary"]


@pytest.mark.parametrize("right", ["other", "manage_call", "manage_topics"])
async def test_admin_with_default_bans_passes_preflight(right):
    from app.core.pipeline.models import PublishContext
    from app.core.pipeline.preflight import phase1_preflight

    entity = SimpleNamespace(
        broadcast=False,
        megagroup=True,
        creator=False,
        admin_rights=SimpleNamespace(**{right: True}),
        default_banned_rights=SimpleNamespace(send_messages=True, send_media=True),
        banned_rights=None,
        title="group",
    )
    p1, p2, _ = _patch_env(entity)
    with p1, p2:
        result = await phase1_preflight(
            PublishContext(task_id=None, target_chat_id="@group", account_id=1),
            is_pikpak=False,
            channel_probe=ChannelInspector.inspect_channel,
        )
    assert result is None
