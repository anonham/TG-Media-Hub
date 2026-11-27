"""Core Runtime: 频道实体解析、PeerChannel 探测、私有频道规范化与跨 DC 鉴权保护测试 (Entity Resolver & Channel Probing)

覆盖范围:
- 原 test_entity_resolver: 频道标识解析、私有群判定、对话刷新与写权限探测
- 原 test_issue1 (resolver 部分): 9-12 位正整数自愈为 -100 格式与链接提取
- 原 test_private_channel_p1: 私有频道链接解析、客户端池纯化、同 DC AuthKey 直连与死锁熔断"""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telethon import functions
from telethon.tl.types import (
    DocumentAttributeFilename,
    InputDocumentFileLocation,
    MessageMediaDocument,
)

from app.config import settings
from app.core import fast_telethon
from app.core.entity_resolver import (
    is_private_channel_identifier,
    iter_message_link_targets,
    normalize_download_chat_key,
    parse_chat_identifier,
    probe_write_permission,
    resolve_entity,
)
from app.core.fast_telethon import DownloadIncompleteError, download_file_parallel
from app.core.scheduler import TaskCancelledError, TaskPausedError, UploadCancelledError, UploadPausedError
from app.services.downloader_service import DownloaderService
from app.services.forward_service import ForwardService


# ==============================================================================
# Part: test_entity_resolver.py
# ==============================================================================
def test_parse_chat_identifier():
    assert parse_chat_identifier(-1009000000010) == -1009000000010
    assert parse_chat_identifier(123456) == 123456
    assert parse_chat_identifier("https://t.me/c/9000000010/123") == -1009000000010
    assert parse_chat_identifier("https://t.me/sample_chan/10") == "sample_chan"
    assert parse_chat_identifier("-1009000000027") == -1009000000027
    assert parse_chat_identifier("9000000010") == -1009000000010
    assert parse_chat_identifier("my_channel") == "my_channel"
    assert parse_chat_identifier("@my_channel") == "@my_channel"
    assert parse_chat_identifier("") == ""
    assert parse_chat_identifier(None) == ""

    # ForwardService delegation compatibility
    assert ForwardService.parse_chat_identifier("https://t.me/c/999/1") == -100999
    assert ForwardService.parse_chat_identifier("my_channel") == "my_channel"


@pytest.mark.parametrize(
    ("link", "target"),
    [
        ("https://t.me/c/9000000010/12", (-1009000000010, 12)),
        ("https://t.me/c/9000000010/4/12", (-1009000000010, 12)),
        ("https://t.me/Example/12", ("Example", 12)),
        ("https://t.me/s/Example/4/12?single", ("Example", 12)),
    ],
)
def test_iter_message_link_targets_yields_one_target(link, target):
    assert list(iter_message_link_targets(link)) == [target]


@pytest.mark.parametrize("chat", ["@Example", "https://t.me/s/EXAMPLE/12", "example"])
def test_normalize_download_chat_key_normalizes_public_identifiers(chat):
    assert normalize_download_chat_key(chat) == "example"


def test_is_private_channel_identifier():
    assert is_private_channel_identifier(-1009000000010) is True
    assert is_private_channel_identifier(-12345) is True
    assert is_private_channel_identifier("https://t.me/c/9000000010/123") is True
    assert is_private_channel_identifier("-1009000000010") is True
    assert is_private_channel_identifier("-54321") is True
    assert is_private_channel_identifier("https://t.me/public_chan/10") is False
    assert is_private_channel_identifier("public_name") is False
    assert is_private_channel_identifier(12345) is False
    assert is_private_channel_identifier("") is False
    assert is_private_channel_identifier(None) is False


@pytest.mark.asyncio
async def test_resolve_entity_normal():
    client = AsyncMock()
    mock_ent = MagicMock(id=111)
    client.get_entity.return_value = mock_ent

    res = await resolve_entity(client, "my_chan")
    assert res == mock_ent
    client.get_entity.assert_called_once_with("my_chan")


@pytest.mark.asyncio
async def test_resolve_entity_fallback_peer():
    client = AsyncMock()
    mock_fallback_ent = MagicMock(id=222)
    # first call fails with string, second call with fallback_peer succeeds
    client.get_entity.side_effect = [Exception("Failed"), mock_fallback_ent]

    fallback = MagicMock(id=999)
    res = await resolve_entity(client, "my_chan", fallback_peer=fallback)
    assert res == mock_fallback_ent
    assert client.get_entity.call_count == 2


@pytest.mark.asyncio
async def test_resolve_entity_dialogs_refresh():
    client = AsyncMock()
    mock_ent = MagicMock(id=333)
    client.is_bot.return_value = False
    # first call fails, then get_dialogs called, second call succeeds
    client.get_entity.side_effect = [Exception("Not in cache"), mock_ent]

    res = await resolve_entity(client, -10012345)
    assert res == mock_ent
    client.get_dialogs.assert_called_once_with(limit=50)


@pytest.mark.asyncio
async def test_resolve_entity_bot_no_dialogs():
    client = AsyncMock()
    client.is_bot.return_value = True
    client.get_entity.side_effect = Exception("Not found")

    with pytest.raises(Exception, match="Not found"):
        await resolve_entity(client, -10012345)
    client.get_dialogs.assert_not_called()


@pytest.mark.asyncio
async def test_resolve_entity_sync_bot():
    """验证 client.is_bot 为同步普通函数或属性时，不抛 TypeError 并正确阻断 get_dialogs"""
    client = MagicMock()
    client.is_bot.return_value = True  # 同步返回 bool，非 awaitable
    client.get_entity = AsyncMock(side_effect=Exception("Not found"))
    client.get_dialogs = AsyncMock()

    with pytest.raises(Exception, match="Not found"):
        await resolve_entity(client, -10012345)
    client.get_dialogs.assert_not_called()


@pytest.mark.asyncio
async def test_probe_write_permission():
    client = AsyncMock()

    # 1. Creator on broadcast channel
    ent_creator = MagicMock(broadcast=True, megagroup=False, creator=True)
    client.get_entity.return_value = ent_creator
    assert await probe_write_permission(client, "-100111") is True

    # 2. Admin with post_messages
    admin_rights = MagicMock(post_messages=True, change_info=False)
    ent_admin = MagicMock(broadcast=True, megagroup=False, creator=False, admin_rights=admin_rights)
    client.get_entity.return_value = ent_admin
    assert await probe_write_permission(client, "-100111") is True

    # 3. Non-admin on broadcast channel
    ent_non_admin = MagicMock(broadcast=True, megagroup=False, creator=False, admin_rights=None)
    client.get_entity.return_value = ent_non_admin
    assert await probe_write_permission(client, "-100111") is False

    # 4. Megagroup with banned send_messages
    banned = MagicMock(send_messages=True, send_media=False)
    ent_banned = MagicMock(
        broadcast=False,
        megagroup=True,
        creator=False,
        admin_rights=None,
        banned_rights=banned,
        default_banned_rights=None,
    )
    client.get_entity.return_value = ent_banned
    assert await probe_write_permission(client, "-100222") is False

    # 5. Normal member in supergroup
    ent_normal = MagicMock(
        broadcast=False,
        megagroup=True,
        creator=False,
        admin_rights=None,
        banned_rights=None,
        default_banned_rights=None,
    )
    client.get_entity.return_value = ent_normal
    assert await probe_write_permission(client, "-100222") is True

    # 6. Admin in supergroup where all members are muted by default_banned_rights
    default_banned = MagicMock(send_messages=True, send_media=True)
    group_admin_rights = MagicMock(delete_messages=True, ban_users=True)
    ent_super_admin = MagicMock(
        broadcast=False,
        megagroup=True,
        creator=False,
        admin_rights=group_admin_rights,
        banned_rights=None,
        default_banned_rights=default_banned,
    )
    client.get_entity.return_value = ent_super_admin
    assert await probe_write_permission(client, "-100444") is True

    # 7. Entity resolution fails
    client.get_entity.side_effect = Exception("RPC error")
    assert await probe_write_permission(client, "-100333") is False


def test_upload_errors_sunk_to_scheduler():
    assert issubclass(UploadCancelledError, TaskCancelledError)
    assert issubclass(UploadPausedError, TaskPausedError)
    # pkg 1.4: services 侧旧名 re-export 已摘, 唯一来源 = app.core.scheduler
    from app.core import scheduler as sched_mod

    assert sched_mod.UploadCancelledError is UploadCancelledError
    assert sched_mod.UploadPausedError is UploadPausedError


# ==============================================================================
# Part: .tmp_issue1_resolver.py
# ==============================================================================
def test_parse_chat_identifier_positive_integer_healing():
    # 9-12 digit positive integers heal to -100 format
    assert parse_chat_identifier(9000000023) == -1009000000023
    assert parse_chat_identifier("9000000023") == -1009000000023
    assert parse_chat_identifier("900000000011") == -100900000000011
    # Already formatted IDs remain intact
    assert parse_chat_identifier(-1009000000023) == -1009000000023
    assert parse_chat_identifier("-1009000000023") == -1009000000023
    # Links
    assert parse_chat_identifier("https://t.me/c/9000000023/123") == -1009000000023
    assert parse_chat_identifier("https://t.me/public_chan/456") == "public_chan"
    # Username
    assert parse_chat_identifier("@public_chan") == "@public_chan"


def test_is_private_channel_identifier_healing():
    assert is_private_channel_identifier(9000000023) is True
    assert is_private_channel_identifier("9000000023") is True
    assert is_private_channel_identifier(-1009000000023) is True
    assert is_private_channel_identifier("-1009000000023") is True
    assert is_private_channel_identifier("https://t.me/c/9000000023/10") is True
    assert is_private_channel_identifier("public_chan") is False
    assert is_private_channel_identifier("@public_chan") is False


# ==============================================================================
# Part: test_private_channel_p1.py
# ==============================================================================
CHUNK = 512 * 1024


def _create_mock_message(msg_id: int = 675, chat_id: int = -1009000000029, dc_id: int = 5, file_size: int = CHUNK):
    doc = MagicMock()
    doc.size = file_size
    doc.dc_id = dc_id
    doc.attributes = [DocumentAttributeFilename(file_name="video.mp4")]
    doc.mime_type = "video/mp4"

    media = MagicMock(spec=MessageMediaDocument)
    media.document = doc

    msg = MagicMock()
    msg.id = msg_id
    msg.chat_id = chat_id
    msg.media = media
    msg.grouped_id = None
    msg.input_chat = MagicMock()
    return msg


@pytest.mark.asyncio
async def test_resolve_message_from_link_returns_int_chat_id():
    """验证私有频道链接 (t.me/c/...) 解析出的 chat_id 严格为 int 类型且为 -100... 负数"""
    mock_client = AsyncMock()
    fake_msg = MagicMock(id=675)
    mock_client.get_messages = AsyncMock(return_value=fake_msg)

    link = "https://t.me/c/9000000029/675"
    chat_id, msg = await DownloaderService.resolve_message_from_link(mock_client, link)

    assert isinstance(chat_id, int)
    assert chat_id == -1009000000029
    assert msg.id == 675

    # 验证底层 get_messages 接收的是 int 类型，防止 Telethon parse_phone 误判为手机号
    mock_client.get_messages.assert_awaited_once_with(-1009000000029, ids=675)


@pytest.mark.asyncio
async def test_private_channel_clients_pool_purification(monkeypatch):
    """验证多账号混入时，无权 Bot 预取失败后被彻底剔除，仅保留有效 Client"""
    primary_userbot = AsyncMock()
    primary_userbot.session = MagicMock(dc_id=5, auth_key=b"key_userbot")
    primary_userbot.is_connected = MagicMock(return_value=True)

    bot1 = AsyncMock()
    bot1.session = MagicMock(dc_id=5, auth_key=b"key_bot1")
    bot1.is_connected = MagicMock(return_value=True)

    bot2 = AsyncMock()
    bot2.session = MagicMock(dc_id=5, auth_key=b"key_bot2")
    bot2.is_connected = MagicMock(return_value=True)

    msg = _create_mock_message(msg_id=675)

    # primary_userbot 预取成功，bot1/bot2 预取抛异常（未加入私密群）
    primary_userbot.get_messages = AsyncMock(return_value=msg)
    bot1.get_messages = AsyncMock(side_effect=ValueError("Cannot get entity by phone number as a bot"))
    bot2.get_messages = AsyncMock(side_effect=Exception("ChannelPrivateError"))

    captured_args = {}

    async def fake_download_file_parallel(**kwargs):
        captured_args.update(kwargs)
        return "/tmp/fake_save_path"

    monkeypatch.setattr("app.services.downloader_service.download_file_parallel", fake_download_file_parallel)
    monkeypatch.setattr(
        DownloaderService, "get_file_name_and_size", staticmethod(lambda m, fallback_id=None: ("video.mp4", CHUNK))
    )

    clients_input = [primary_userbot, bot1, bot2]
    await DownloaderService.download_single_message(
        client=clients_input, chat_id=-1009000000029, message=msg, task_id=None
    )

    # 纯化断言：传入 download_file_parallel 的 client 必须仅包含 primary_userbot
    assert captured_args.get("client") == [primary_userbot]
    assert bot1 not in captured_args.get("client")
    assert bot2 not in captured_args.get("client")


@pytest.mark.asyncio
async def test_same_dc_init_sender_bypasses_export_authorization(tmp_dir, monkeypatch):
    """验证当 Userbot 所在 DC 与媒体所在 DC 相同 (DC 5) 时，直接复用 AuthKey 直连，跳过 ExportAuthorizationRequest"""
    # out_file 位于 tests/.tmp 而非真实下载目录: 注入 DOWNLOAD_DIR 使 trust_root 沙箱与落盘目录一致
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", tmp_dir)
    client = AsyncMock()
    client.session = MagicMock(dc_id=5, auth_key=b"test_same_dc_auth_key")
    client.is_connected = MagicMock(return_value=True)

    # 模拟 GetFileRequest 成功返回分块
    async def ok_call(sender, request):
        res = MagicMock()
        res.bytes = b"x" * CHUNK
        return res

    client._call = AsyncMock(side_effect=ok_call)

    # Mock GetConfig 返回 DC 配置
    dc_opt = MagicMock(id=5, ipv6=False, ip_address="149.154.167.51", port=443)
    mock_config = MagicMock(dc_options=[dc_opt])
    client.return_value = mock_config  # for client(GetConfigRequest())

    export_auth_called = False
    same_dc_sender_created_count = 0

    async def fake_export_auth(*args, **kwargs):
        nonlocal export_auth_called
        export_auth_called = True
        return MagicMock(id=1, bytes=b"auth_bytes")

    async def fake_create_same_dc(c, dc_id, dc_option=None):
        nonlocal same_dc_sender_created_count
        same_dc_sender_created_count += 1
        s = MagicMock()
        s.dc_id = dc_id
        return s

    monkeypatch.setattr(fast_telethon.sender_pool, "_create_worker_sender_same_dc", fake_create_same_dc)

    # 拦截 client() 调用以监听 ExportAuthorizationRequest
    orig_client_call = client.__call__

    async def intercept_call(req, *args, **kwargs):
        if isinstance(req, functions.auth.ExportAuthorizationRequest):
            nonlocal export_auth_called
            export_auth_called = True
            return MagicMock(id=1, bytes=b"auth_bytes")
        if isinstance(req, functions.help.GetConfigRequest):
            return mock_config
        return await orig_client_call(req, *args, **kwargs)

    client.side_effect = intercept_call

    out_file = os.path.join(tmp_dir, "same_dc_test.bin")
    raw_media = _create_mock_message(dc_id=5, file_size=2 * CHUNK).media

    res = await download_file_parallel(
        client=[client],
        location=MagicMock(dc_id=5),
        out_file_path=out_file,
        total_bytes=2 * CHUNK,
        concurrency=4,
        raw_media=raw_media,
    )

    assert res == out_file
    # 同 DC 下必须 100% 直连 _create_worker_sender_same_dc
    assert same_dc_sender_created_count == 4
    # 绝不调用 ExportAuthorizationRequest (消除 Sender 降级告警)
    assert export_auth_called is False


@pytest.mark.asyncio
async def test_poison_fallback_blocked_in_refetch_location(monkeypatch):
    """验证特定 Client 刷新失败时直接返回 None，阻断拿 Userbot 凭证借给 Bot 的毒药回退"""
    userbot = AsyncMock()
    MagicMock(name="userbot_location")
    userbot.get_messages = AsyncMock(return_value=MagicMock(media=MagicMock()))

    failing_bot = AsyncMock()
    failing_bot.get_messages = AsyncMock(side_effect=Exception("Bot forbidden in private chat"))

    refetch_fn = None

    async def capture_refetch(client, location, out_file_path, total_bytes, **kwargs):
        nonlocal refetch_fn
        refetch_fn = kwargs.get("refetch_location")
        return out_file_path

    monkeypatch.setattr("app.services.downloader_service.download_file_parallel", capture_refetch)
    monkeypatch.setattr(
        DownloaderService, "get_file_name_and_size", staticmethod(lambda m, fallback_id=None: ("test.bin", CHUNK))
    )

    msg = _create_mock_message()
    await DownloaderService.download_single_message(client=[userbot, failing_bot], chat_id=-1009000000029, message=msg)

    assert refetch_fn is not None
    # 验证 failing_bot 刷新失败时返回 None，而不是回退给 userbot 的 location
    result_for_bot = await refetch_fn(target_c=failing_bot)
    assert result_for_bot is None


@pytest.mark.asyncio
async def test_refetch_retry_limit_breaks_deadlock(tmp_dir, monkeypatch):
    """验证分块遭遇 FileReferenceExpired 连续刷新超限 (>=3) 时熔断退出，避免死锁"""
    # out_file 位于 tests/.tmp 而非真实下载目录: 注入 DOWNLOAD_DIR 使 trust_root 沙箱与落盘目录一致
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", tmp_dir)
    client = AsyncMock()
    client.session = MagicMock(dc_id=2, auth_key=b"k")
    client.is_connected = MagicMock(return_value=True)

    # 模拟每次请求均抛出 FileReferenceExpiredError
    client._call = AsyncMock(side_effect=Exception("FILE_REFERENCE_EXPIRED"))

    async def fake_sender(*args, **kwargs):
        return object()

    monkeypatch.setattr(fast_telethon.sender_pool, "_create_worker_sender_same_dc", fake_sender)
    monkeypatch.setattr(fast_telethon.sender_pool, "_create_worker_sender_with_auth", fake_sender)
    monkeypatch.setattr(fast_telethon.sender_pool, "_get_worker_sender", fake_sender)

    refetch_invocation_count = 0
    location = InputDocumentFileLocation(123, 456, b"old", "")

    async def mock_refetch(c):
        nonlocal refetch_invocation_count
        refetch_invocation_count += 1
        return InputDocumentFileLocation(123, 456, b"refreshed", "")

    out_file = os.path.join(tmp_dir, "refetch_limit.bin")
    total_bytes = 2 * CHUNK

    # 必须抛出 DownloadIncompleteError，并且重试次数不得无限循环
    with pytest.raises(DownloadIncompleteError) as exc_info:
        await download_file_parallel(
            client=[client],
            location=location,
            out_file_path=out_file,
            total_bytes=total_bytes,
            concurrency=2,
            raw_media=_create_mock_message(dc_id=2, file_size=total_bytes).media,
            refetch_location=mock_refetch,
        )

    # 验证重试在 3 次以内被熔断，未发生死循环死锁
    assert refetch_invocation_count <= 3 * 2
    assert exc_info.value.total_chunks == 2
    assert exc_info.value.completed_chunks == 0


@pytest.mark.asyncio
async def test_parse_chat_identifier_invites_previews_and_queries():
    """验证群组标识解析：提取邀请码 +hash、忽略 /s/、截断 query/fragment，且 resolve_entity 还原完整邀请 URL"""
    assert parse_chat_identifier("https://t.me/+AbCdEf12345") == "+AbCdEf12345"
    assert parse_chat_identifier("https://t.me/joinchat/XyZ123") == "+XyZ123"
    assert parse_chat_identifier("https://t.me/s/test_channel") == "test_channel"
    assert parse_chat_identifier("https://t.me/test_channel?start=abc#hash") == "test_channel"
    assert parse_chat_identifier("https://t.me/s/test_channel/1234") == "test_channel"

    mock_client = MagicMock()
    mock_client.get_entity = AsyncMock(return_value="resolved_entity")
    res = await resolve_entity(mock_client, "https://t.me/joinchat/XyZ123")
    assert res == "resolved_entity"
    mock_client.get_entity.assert_awaited_once_with("https://t.me/+XyZ123")


@pytest.mark.asyncio
async def test_probe_write_permission_requires_post_messages_on_broadcast_channel():
    """验证广播频道仅凭 change_info 不能通过写权限检查，必须具备 post_messages"""
    mock_client = MagicMock()

    class MockBroadcastEntity:
        broadcast = True
        megagroup = False
        creator = False

        def __init__(self, post_messages: bool, change_info: bool):
            self.admin_rights = MagicMock(post_messages=post_messages, change_info=change_info)

    entity_no_post = MockBroadcastEntity(post_messages=False, change_info=True)
    with patch("app.core.entity_resolver.resolve_entity", new_callable=AsyncMock, return_value=entity_no_post):
        assert await probe_write_permission(mock_client, "demo_chan") is False

    entity_with_post = MockBroadcastEntity(post_messages=True, change_info=False)
    with patch("app.core.entity_resolver.resolve_entity", new_callable=AsyncMock, return_value=entity_with_post):
        assert await probe_write_permission(mock_client, "demo_chan") is True
