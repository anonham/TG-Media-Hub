"""Business Services: 下载核心服务、数据完整性校验、断网重连与多 Userbot 协同下载测试 (Downloader Service)

覆盖范围:
- 原 test_services: DownloaderService 消息解析、文件名提取与媒体类型判定
- 原 test_download_integrity: 下载真实性校验、磁盘空间预检与 .part 断点文件保护
- 原 test_download_disconnected_recovery: 网络瞬断重试自愈、TCP 重连与连接状态自保
- 原 test_download_resume_coroutine: 下载暂停/恢复协程生命周期契约
- 原 test_multi_userbot_download: 多 Userbot 协同并行下载、分片竞争与负载均衡"""

import os
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from _cm_seed import seed_bots, seed_default_account
from _safeio import safe_write_bytes
from fastapi.testclient import TestClient
from telethon import functions
from telethon.errors import AuthKeyUnregisteredError, ChatAdminRequiredError, FloodWaitError
from telethon.tl.types import (
    Document,
    DocumentAttributeFilename,
    MessageMediaDocument,
    MessageMediaPhoto,
    Photo,
    PhotoSize,
)

from app.api.auth import issue_2fa_token
from app.api.tasks import pause_task, resume_task
from app.config import settings
from app.core import fast_telethon
from app.core.client_manager import ClientManager, TelegramAccountClient, is_channel_permission_error
from app.core.fast_telethon import (
    DownloadIncompleteError,
    download_file_parallel,
)
from app.core.fast_telethon.sender_pool import _close_worker_sender, _get_worker_sender
from app.core.runtime import active_task_controls
from app.core.scheduler import TaskScheduler, task_scheduler
from app.db.download_repository import DownloadHistoryRepository
from app.db.task_repository import TaskQueueRepository
from app.main import app
from app.services.downloader_service import AlbumDiscoveryError, DownloaderService, sanitize_filename


# ==============================================================================
# Part: test_services.py
# ==============================================================================
@pytest.mark.asyncio
async def test_resolve_message_from_link_formats():
    mock_client = MagicMock()
    mock_client.get_messages = AsyncMock(return_value=MagicMock(id=1234))

    # 1. 私有频道链接格式
    chat_id, msg = await DownloaderService.resolve_message_from_link(mock_client, "https://t.me/c/9000000010/1234")
    assert chat_id == -1009000000010
    assert isinstance(chat_id, int)
    assert msg.id == 1234

    # 2. 公开频道链接格式
    chat_id, msg = await DownloaderService.resolve_message_from_link(mock_client, "https://t.me/sample_channel/17354")
    assert chat_id == "sample_channel"

    # 3. 空链接
    chat_id, msg = await DownloaderService.resolve_message_from_link(mock_client, "   ")
    assert chat_id is None
    assert msg is None


def test_get_file_name_and_size_document():
    # 模拟包含 Document 的 Message
    mock_msg = MagicMock()
    attr = DocumentAttributeFilename(file_name="sample_video.mp4")
    doc = Document(
        id=1,
        access_hash=1,
        file_reference=b"",
        date=None,
        mime_type="video/mp4",
        size=1048576,
        dc_id=1,
        attributes=[attr],
    )
    mock_msg.media = MessageMediaDocument(document=doc)

    file_name, file_size = DownloaderService.get_file_name_and_size(mock_msg)
    assert file_name == "sample_video.mp4"
    assert file_size == 1048576


def test_get_file_name_and_size_photo():
    # 模拟包含 Photo 的 Message
    mock_msg = MagicMock()
    mock_msg.id = 88
    photo = Photo(
        id=1,
        access_hash=1,
        file_reference=b"",
        date=None,
        sizes=[PhotoSize(type="x", w=100, h=100, size=2048)],
        dc_id=1,
    )
    mock_msg.media = MessageMediaPhoto(photo=photo)

    file_name, file_size = DownloaderService.get_file_name_and_size(mock_msg)
    assert file_name == "photo_88.jpg"
    assert file_size == 2048


def test_get_file_name_and_size_none():
    mock_msg = MagicMock()
    mock_msg.media = None

    file_name, file_size = DownloaderService.get_file_name_and_size(mock_msg)
    assert file_name == "unknown_file"
    assert file_size == 0


def test_get_file_name_and_size_fallback_id_ignores_transit_message_id():
    """中转桥下载: 兜底名基准 ID 取源消息 ID, 桥接重发后文件名/断点不漂移"""
    photo = Photo(
        id=1, access_hash=1, file_reference=b"", date=None, sizes=[PhotoSize(type="x", w=1, h=1, size=10)], dc_id=1
    )
    transit_msg = MagicMock()
    transit_msg.id = 900  # 中转群临时 ID
    transit_msg.media = MessageMediaPhoto(photo=photo)

    # 不传 fallback_id → 旧行为 (用 message.id)
    name_old, _ = DownloaderService.get_file_name_and_size(transit_msg)
    assert name_old == "photo_900.jpg"

    # 传源消息 ID → 名字稳定, 不随中转重发 ID 变化
    name_new, size_new = DownloaderService.get_file_name_and_size(transit_msg, fallback_id=42)
    assert name_new == "photo_42.jpg"
    assert size_new == 10


def test_get_valid_download_path_returns_recorded_path():
    """防重命中回传历史行真实落盘路径 (兼容旧命名布局), 未下载/文件缺失返回 None"""
    final_path = safe_write_bytes(os.path.join(settings.DOWNLOAD_DIR, "7_doc.pdf"), b"payload")

    DownloadHistoryRepository.record(
        chat_id="-100chanX", msg_id=7, grouped_id=None, file_name="7_doc.pdf", file_path=final_path, file_size=7
    )
    assert DownloadHistoryRepository.get_valid_download_path("-100chanX", 7) == final_path
    assert DownloadHistoryRepository.is_downloaded("-100chanX", 7) is True

    # 磁盘文件被删 → 废行清理 + None
    os.remove(final_path)
    assert DownloadHistoryRepository.get_valid_download_path("-100chanX", 7) is None
    assert DownloadHistoryRepository.is_downloaded("-100chanX", 7) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("force", [False, True])
async def test_unreadable_download_history_still_attempts_transfer(monkeypatch, tmp_path, force):
    task_id = "history-read-error"
    inaccessible = tmp_path / "unreadable.bin"
    inaccessible.write_bytes(b"original")
    TaskQueueRepository.create(task_id, "download_link", {}, status="running")
    DownloadHistoryRepository.record("example", 7, None, inaccessible.name, str(inaccessible), 8, task_id=task_id)
    orig_stat = os.stat

    def stat_history(path, *args, **kwargs):
        if str(path) == str(inaccessible):
            raise PermissionError("history file unreadable")
        return orig_stat(path, *args, **kwargs)

    transfer = AsyncMock(side_effect=RuntimeError("transfer attempted"))
    telegram = MagicMock()
    media = SimpleNamespace(id=7, grouped_id=None, media=object())
    monkeypatch.setattr("app.db.download_repository.os.stat", stat_history)
    monkeypatch.setattr("app.services.downloader_service.download_file_parallel", transfer)
    monkeypatch.setattr(DownloaderService, "get_file_name_and_size", lambda *_a, **_kw: ("download.bin", 6))
    assert (
        await DownloaderService.download_single_message(telegram, "example", media, task_id=task_id, force=force)
        is None
    )
    transfer.assert_awaited_once()
    assert DownloadHistoryRepository.get_recent()[0]["file_path"] == str(inaccessible)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["completed", "failed", "replaced"])
async def test_reused_album_completion_preserves_original_history(monkeypatch, tmp_path, outcome):
    original = tmp_path / "original.bin"
    original.write_bytes(b"first")
    TaskQueueRepository.create("album-original", "download_range", {}, status="failed")
    TaskQueueRepository.create("album-consumer", "download_range", {}, status="running")
    DownloadHistoryRepository.record("example", 7, 42, original.name, str(original), 5, task_id="album-original")
    telegram = MagicMock()
    media = SimpleNamespace(id=7, grouped_id=42, media=object())
    transfer = AsyncMock()
    monkeypatch.setattr("app.services.downloader_service.download_file_parallel", transfer)
    monkeypatch.setattr(DownloaderService, "get_file_name_and_size", lambda *_a, **_kw: ("media.bin", 5))
    assert await DownloaderService.download_single_message(telegram, "example", media, task_id="album-consumer") == str(
        original
    )
    transfer.assert_not_awaited()
    assert DownloadHistoryRepository.get_valid_download_record("example", 7, require_completed_album=True) is None

    if outcome == "replaced":
        DownloadHistoryRepository.record("example", 7, 42, original.name, str(original), 5, task_id="album-original")
    # A reused member can be behind the new range start by the time this task completes.
    assert await TaskQueueRepository.update_status_async("album-consumer", "paused")
    assert await TaskQueueRepository.update_status_async("album-consumer", "running")
    assert await TaskQueueRepository.update_status_async(
        "album-consumer", "failed" if outcome == "failed" else "completed"
    )
    record = DownloadHistoryRepository.get_valid_download_record("example", 7)
    assert record["task_id"] == "album-original"
    assert record["album_completed"] == (outcome == "completed")


@pytest.mark.asyncio
async def test_forced_download_replaces_history_with_normalized_origin_and_task_id(monkeypatch):
    target = Path(settings.DOWNLOAD_DIR) / "example_7_media.bin"
    target.write_bytes(b"old")
    DownloadHistoryRepository.record("example", 7, None, target.name, str(target), 3, task_id="first-task")
    TaskQueueRepository.create("force-task", "download_link", {})
    media = SimpleNamespace(id=99, grouped_id=None, media=object())
    telegram = MagicMock()
    telegram.get_messages = AsyncMock(return_value=media)

    async def fake_download(**kwargs):
        return safe_write_bytes(kwargs["out_file_path"], b"new file")

    transfer = AsyncMock(side_effect=fake_download)
    monkeypatch.setattr("app.services.downloader_service.download_file_parallel", transfer)
    monkeypatch.setattr("app.services.downloader_service.get_input_location", lambda _: object())
    monkeypatch.setattr(DownloaderService, "get_file_name_and_size", lambda *_args, **_kwargs: ("media.bin", 8))

    origin = {"origin_chat_id": "@ExAmPle", "origin_msg_id": 7, "origin_grouped_id": 42}
    assert await DownloaderService.download_single_message(telegram, "transit", media, **origin) == str(target)
    transfer.assert_not_awaited()

    actual = await DownloaderService.download_single_message(
        telegram, "transit", media, task_id="force-task", force=True, **origin
    )
    assert actual == str(target)
    assert target.read_bytes() == b"new file"
    transfer.assert_awaited_once()
    assert DownloadHistoryRepository.get_valid_download_record("example", 7) == {
        "file_path": str(target),
        "file_name": "media.bin",
        "grouped_id": "42",
        "task_id": "force-task",
        "album_completed": False,
    }

    transfer.reset_mock()
    resumed = await DownloaderService.download_single_message(
        telegram, "transit", media, task_id="force-task", force=True, **origin
    )
    assert resumed == str(target)
    transfer.assert_not_awaited()

    target.unlink()
    recovered = await DownloaderService.download_single_message(
        telegram, "transit", media, task_id="force-task", force=True, **origin
    )
    assert recovered == str(target)
    assert target.read_bytes() == b"new file"
    transfer.assert_awaited_once()


@pytest.mark.asyncio
async def test_resolve_message_from_link_reraises_flood_wait():
    """链接解析遇 FloodWait/会话失效必须上抛 (外层任务策略依赖其做退避/重绑), 不得吞成 (None, None)"""
    mock_client = MagicMock()

    async def flood(*args, **kwargs):
        err = FloodWaitError(request=None)
        err.seconds = 1
        raise err

    mock_client.get_messages = flood
    with pytest.raises(FloodWaitError):
        await DownloaderService.resolve_message_from_link(mock_client, "https://t.me/c/9000000010/1234")


def test_sanitize_filename_blocks_path_traversal():
    assert sanitize_filename("..\\..\\evil.mp4") == "evil.mp4"
    assert sanitize_filename("../evil.mp4") == "evil.mp4"
    assert sanitize_filename("a/b/c.mp4") == "c.mp4"
    assert ":" not in sanitize_filename("bad:name?.mp4")
    assert "\\" not in sanitize_filename("a\\b.mp4")
    assert sanitize_filename("") == "download_file"


def test_format_folder_tag():
    from app.core.pipeline.utils import format_folder_tag

    sample_folder = os.getenv("TEST_SAMPLE_FOLDER", r"C:\Downloads\SampleFolder\@SampleTag #Tag1 #Tag2")
    tag1 = format_folder_tag(sample_folder)
    assert tag1.startswith("#")

    # 平台原生分隔符构造输入: format_folder_tag 用 os.path.basename 提取末段
    # 硬编码 Windows 反斜杠字面量在 Linux CI 下会带出路径前缀 (行为差异非产品缺陷)
    tagged_folder = os.path.join("C:\\Movies" if os.name == "nt" else "/media/Movies", "#已经带标签的文件夹 #标签2")
    tag2 = format_folder_tag(tagged_folder)
    assert tag2 == "#已经带标签的文件夹 #标签2"


def test_inspect_folder_with_categorization_and_filtering(tmp_dir):
    from app.config import settings
    from app.services.publisher_service import PublisherService

    media_folder = os.path.join(tmp_dir, "media")
    os.makedirs(media_folder, exist_ok=True)

    # 创建测试各类文件: 图片、视频、音频、非媒体过滤文件
    safe_write_bytes(os.path.join(media_folder, "pic.jpg"), b"\0" * (500 * 1024))  # 0.5MB

    safe_write_bytes(os.path.join(media_folder, "normal_video.mp4"), b"\0" * (1024 * 1024))  # 1MB

    safe_write_bytes(os.path.join(media_folder, "song.mp3"), b"\0" * (2 * 1024 * 1024))  # 2MB

    safe_write_bytes(os.path.join(media_folder, "info.txt"), b"non media file")

    safe_write_bytes(os.path.join(media_folder, "archive.zip"), b"zip archive")

    safe_write_bytes(os.path.join(media_folder, "oversized_video.mp4"), b"\0" * (5 * 1024 * 1024))  # 5MB

    orig_limit = settings.MAX_FILE_SIZE_BYTES
    try:
        settings.MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024  # 临时设为 2MB 限制

        res = PublisherService.inspect_folder(media_folder, "-100900008")
        assert res["total_media_files"] == 4
        assert res["total_scanned_files"] == 6
        assert res["images"]["count"] == 1
        assert res["videos"]["count"] == 2
        assert res["audios"]["count"] == 1
        assert res["ignored_files_count"] == 2
        assert len(res["ignored_files"]) == 2
        assert any(f["extension"] == ".txt" for f in res["ignored_files"])
        assert any(f["extension"] == ".zip" for f in res["ignored_files"])

        assert res["valid_files_count"] == 3
        assert len(res["oversized_files"]) == 1
        assert res["oversized_files"][0]["file_name"] == "oversized_video.mp4"
    finally:
        settings.MAX_FILE_SIZE_BYTES = orig_limit


@pytest.mark.asyncio
@pytest.mark.parametrize(("second_batch_fails", "album_completed"), [(False, True), (True, True), (False, False)])
async def test_inspect_download_range_mock(second_batch_fails, album_completed):
    from app.core.client_manager import client_manager
    from app.services.downloader_service import DownloaderService

    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)
    mock_entity = MagicMock(
        title="测试公开频道", username="test_chan", noforwards=True, broadcast=True, megagroup=False
    )
    mock_client.get_entity = AsyncMock(return_value=mock_entity)

    # 模拟 3 条消息 (1 条 photo, 1 条 video doc, 1 条 text only)
    msg1 = MagicMock(id=10)
    photo = Photo(
        id=1,
        access_hash=1,
        file_reference=b"",
        date=None,
        sizes=[PhotoSize(type="x", w=100, h=100, size=2048)],
        dc_id=1,
    )
    msg1.media = MessageMediaPhoto(photo=photo)

    msg2 = MagicMock(id=11)
    attr = DocumentAttributeFilename(file_name="video.mp4")
    doc = Document(
        id=1,
        access_hash=1,
        file_reference=b"",
        date=None,
        mime_type="video/mp4",
        size=1048576,
        dc_id=1,
        attributes=[attr],
    )
    msg2.media = MessageMediaDocument(document=doc)

    msg3 = MagicMock(id=12, media=None, message="纯文本内容")

    end_id = 110 if second_batch_fails else 12

    async def get_messages(_entity, *, ids):
        if ids == [110]:
            raise RuntimeError("第二批消息不可用")
        messages = {10: msg1, 11: msg2, 12: msg3}
        return [messages.get(msg_id) for msg_id in ids]

    mock_client.get_messages = AsyncMock(side_effect=get_messages)
    event_loop_thread = threading.get_ident()
    checked = []

    def get_history(chat_id, msg_id):
        assert threading.get_ident() != event_loop_thread
        checked.append((chat_id, msg_id))
        return {"file_path": str(msg_id), "album_completed": album_completed or msg_id == 10}

    orig_accounts = dict(client_manager._accounts)
    seed_default_account(client_manager, client=mock_client)
    try:
        with patch("app.services.downloader_service.DownloadHistoryRepository.get_valid_download_record", get_history):
            res = await DownloaderService.inspect_download_range("test_chan", 10, end_id)
        assert res["is_accessible"] is True
        assert res["channel_title"] == "测试公开频道"
        assert res["is_restricted"] is True
        assert res["photos_count"] == 1
        assert res["videos_count"] == 1
        assert res["messages_with_media"] == 2
        assert res["text_only_messages"] == 1
        assert res["total_range_messages"] == end_id - 9
        assert checked == [("test_chan", 10), ("test_chan", 11)]
        assert res["already_downloaded_count"] == 2
        assert res["history_check_complete"] is not second_batch_fails
        assert res["all_media_downloaded"] is (album_completed and not second_batch_fails)
    finally:
        client_manager._accounts.clear()
        client_manager._accounts.update(orig_accounts)


@pytest.mark.asyncio
async def test_publish_folder_progress_reporting(tmp_dir):
    from app.core.client_manager import client_manager
    from app.services.publisher_service import PublisherService

    media_dir = os.path.join(tmp_dir, "progress_media")
    os.makedirs(media_dir, exist_ok=True)
    # 创建 3 个测试视频
    for i in range(1, 4):
        safe_write_bytes(os.path.join(media_dir, f"video_{i}.mp4"), b"video content")

    mock_client = AsyncMock()

    async def mock_send_file(chat_id, file_path, **kwargs):
        cb = kwargs.get("progress_callback")
        if cb:
            cb(50, 100)  # 模拟上传 50% 过程
        return MagicMock()

    mock_client.send_file = mock_send_file
    mock_client.send_message = AsyncMock()
    orig_bots = dict(client_manager._bots)
    orig_bot_index = client_manager._bot_index
    seed_bots(client_manager, [mock_client])

    progress_records = []

    def record_progress(current, total, speed, eta, file_name, concurrency=0, dc_id=0, percent=None):
        progress_records.append({"current": current, "total": total, "file_name": file_name, "percent": percent})

    try:
        with patch(
            "app.services.channel_inspector.ChannelInspector.inspect_channel",
            new=AsyncMock(
                return_value={"is_accessible": True, "has_write_permission": True, "permission_summary": "拥有发帖权限"}
            ),
        ):
            count = await PublisherService.publish_folder(
                folder_path=media_dir,
                target_chat_id="-100900037",
                album_mode=False,
                delay_seconds=0.01,
                progress_callback=record_progress,
            )

        assert count == 3
        # 验证进度回调: 1-based 条目序号 (min(tot_files, cur_done + 1)) 在发布第 1 个文件时 current=1; 完成后 current=3 (3/3)
        assert progress_records[0]["current"] == 1
        assert progress_records[0]["total"] == 3
        assert round(progress_records[0]["percent"], 1) == 16.7  # (0 + 0.5) / 3 * 100

        final_call = progress_records[-1]
        assert final_call["total"] == 3
        assert final_call["current"] == 3
        assert final_call["percent"] == 100.0
    finally:
        client_manager._bots.clear()
        client_manager._bots.update(orig_bots)
        client_manager._bot_index = orig_bot_index


# ==============================================================================
# Part: test_download_integrity.py
# ==============================================================================
CHUNK = 512 * 1024


def _failing_client(exc: Exception) -> AsyncMock:
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client._call = AsyncMock(side_effect=exc)
    return client


def _ok_client() -> AsyncMock:
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)

    async def ok_call(sender, request):
        res = MagicMock()
        res.bytes = b"a" * CHUNK
        return res

    client._call = AsyncMock(side_effect=ok_call)
    return client


def _raw_media(dc_id: int = 2) -> MagicMock:
    media = MagicMock()
    media.document = MagicMock(dc_id=dc_id)
    media.photo = None
    return media


@pytest.fixture
def fake_senders(monkeypatch):
    async def fake_create(*args, **kwargs):
        return object()

    async def fake_get(*args, **kwargs):
        return object()

    monkeypatch.setattr(fast_telethon.sender_pool, "_create_worker_sender_with_auth", fake_create)
    monkeypatch.setattr(fast_telethon.sender_pool, "_get_worker_sender", fake_get)


@pytest.mark.asyncio
async def test_chunk_exhaustion_raises_and_keeps_checkpoint(tmp_dir, fake_senders, monkeypatch):
    """块重试 5 次耗尽 -> DownloadIncompleteError, .part/.meta 保留供续传 (不再假成功)"""
    # out_file 位于 tests/.tmp 而非真实下载目录: 注入 DOWNLOAD_DIR 使 trust_root 沙箱与落盘目录一致
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", tmp_dir)
    out = os.path.join(tmp_dir, "big.bin")
    total = 3 * CHUNK
    client = _failing_client(TimeoutError("mock network down"))

    with pytest.raises(DownloadIncompleteError) as ei:
        await download_file_parallel(
            client, location=MagicMock(), out_file_path=out, total_bytes=total, concurrency=2, raw_media=_raw_media()
        )

    err = ei.value
    assert err.total_chunks == 3
    assert err.completed_chunks == 0
    assert len(err.missing_offsets) == 3
    assert all("mock network down" in err.last_errors.get(o, "") for o in err.missing_offsets)
    assert "缺失 3/3" in str(err)
    # 断点保留: resume 可仅补齐缺失块
    assert os.path.exists(out + ".part")
    assert os.path.exists(out + ".meta")
    assert not os.path.exists(out)


@pytest.mark.asyncio
async def test_resume_from_checkpoint_completes(tmp_dir, fake_senders, monkeypatch):
    """失败后二次调用: 从 .meta 断点续传并成功拼接"""
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", tmp_dir)
    out = os.path.join(tmp_dir, "resume.bin")
    total = 2 * CHUNK
    client = _failing_client(TimeoutError("first attempt fails"))
    with pytest.raises(DownloadIncompleteError):
        await download_file_parallel(
            client, location=MagicMock(), out_file_path=out, total_bytes=total, concurrency=2, raw_media=_raw_media()
        )

    ok_client = _ok_client()
    res = await download_file_parallel(
        ok_client, location=MagicMock(), out_file_path=out, total_bytes=total, concurrency=2, raw_media=_raw_media()
    )
    assert res == out
    assert os.path.getsize(out) == total
    assert not os.path.exists(out + ".part")
    assert not os.path.exists(out + ".meta")


@pytest.mark.asyncio
async def test_small_file_failure_propagates_without_fake_progress(tmp_dir):
    """小文件 (<=512KB) 异常必须上抛, 不得伪造 100% 进度"""
    out = os.path.join(tmp_dir, "small.bin")
    client = AsyncMock()
    client.download_media = AsyncMock(side_effect=Exception("boom"))
    calls = []

    def cb(*args, **kwargs):
        calls.append(args)

    with pytest.raises(Exception, match="boom"):
        await download_file_parallel(
            client, location=None, out_file_path=out, total_bytes=100, progress_callback=cb, raw_media=MagicMock()
        )
    assert calls == []
    assert not os.path.exists(out)


@pytest.mark.asyncio
async def test_small_file_missing_output_raises(tmp_dir):
    """小文件下载返回但文件不存在 -> DownloadIncompleteError 而非假成功"""
    out = os.path.join(tmp_dir, "ghost.bin")
    client = AsyncMock()
    client.download_media = AsyncMock(return_value="")

    with pytest.raises(DownloadIncompleteError):
        await download_file_parallel(client, location=None, out_file_path=out, total_bytes=100, raw_media=MagicMock())


@pytest.mark.asyncio
async def test_download_single_message_failure_no_history(monkeypatch):
    """引擎抛 DownloadIncompleteError -> download_single_message 返回 None 且不写 download_history"""
    msg = MagicMock(id=7, media=MagicMock(), grouped_id=None)

    async def raise_incomplete(*args, **kwargs):
        raise DownloadIncompleteError("/x/f.bin", [0, CHUNK], {0: "timeout"}, total_chunks=3, completed_chunks=1)

    monkeypatch.setattr(
        "app.services.downloader_service.download_file_parallel", AsyncMock(side_effect=raise_incomplete)
    )
    monkeypatch.setattr(
        DownloaderService, "get_file_name_and_size", staticmethod(lambda m, fallback_id=None: ("f.bin", 3 * CHUNK))
    )

    result = await DownloaderService.download_single_message(
        client=AsyncMock(), chat_id="-100123", message=msg, task_id=None
    )
    assert result is None
    assert not DownloadHistoryRepository.is_downloaded("-100123", 7)


@pytest.mark.asyncio
async def test_range_task_fails_when_download_incomplete(monkeypatch):
    """范围任务存在失败条目 -> 终态 failed, 不广播全部完成"""
    TaskQueueRepository.create("t_fail_range", "download_range", {"channel": "c", "start_id": 1, "end_id": 2})

    fake_userbot = AsyncMock()
    fake_userbot.get_input_entity = AsyncMock(return_value=MagicMock())

    async def fake_get_messages(entity, ids=None):
        return MagicMock(id=ids, media=MagicMock())

    fake_userbot.get_messages = fake_get_messages
    from app.core.client_manager import client_manager

    seeded_acc = seed_default_account(client_manager, client=fake_userbot)
    monkeypatch.setattr(client_manager, "get_account", lambda account_id=None: seeded_acc)
    monkeypatch.setattr(client_manager, "get_client_for_task", AsyncMock(return_value=fake_userbot))
    monkeypatch.setattr(
        "app.services.download_runner.DownloaderService.download_single_message", AsyncMock(return_value=None)
    )

    from app.services.download_runner import run_download_range_task

    await run_download_range_task("t_fail_range", "c", 1, 2)

    task = TaskQueueRepository.get_by_id("t_fail_range")
    assert task["status"] == "failed"
    fail_logs, _, _ = TaskQueueRepository.get_task_logs("t_fail_range", 0, 500)
    assert any("未完成下载" in l["message"] for l in fail_logs)
    assert any("断点" in l["message"] for l in fail_logs)


@pytest.mark.asyncio
async def test_inspect_range_disk_fields(monkeypatch):
    """inspect-range 返回磁盘剩余空间字段与充足结论; 不足时追加 warning"""
    from telethon.tl.types import MessageMediaDocument

    from app.services import downloader_service as ds

    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=-100123, title="T"))

    async def gm(entity, ids=None, **kwargs):
        return [
            MagicMock(id=i, media=MessageMediaDocument(document=MagicMock(mime_type="video/mp4"))) for i in (ids or [])
        ]

    mock_client.get_messages = gm
    monkeypatch.setattr(
        DownloaderService,
        "get_file_name_and_size",
        staticmethod(lambda m, fallback_id=None: ("f.mp4", 5 * 1024 * 1024)),
    )

    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        usage = MagicMock(free=10 * 1024 * 1024 * 1024)
        monkeypatch.setattr(ds.shutil, "disk_usage", lambda p: usage)
        res = await DownloaderService.inspect_download_range("-100123", 1, 2)
        assert res["disk_free_gb"] == 10.0
        assert res["disk_free_bytes"] == 10 * 1024 * 1024 * 1024
        assert res["disk_sufficient"] is True

        usage.free = 1 * 1024 * 1024 * 1024  # 1 GB < 10MB*1.05? 否, 10MB 需求下充足 -> 用大需求测不足
        res2 = await DownloaderService.inspect_download_range("-100123", 1, 2)
        assert res2["disk_sufficient"] is True  # 需求 10MB, 1GB 仍充足

        monkeypatch.setattr(
            DownloaderService,
            "get_file_name_and_size",
            staticmethod(lambda m, fallback_id=None: ("f.mp4", 2 * 1024 * 1024 * 1024)),
        )
        res3 = await DownloaderService.inspect_download_range("-100123", 1, 2)
        assert res3["disk_sufficient"] is False
        assert any("磁盘剩余空间不足" in w for w in res3["warning_messages"])


# ==============================================================================
# Part: test_download_disconnected_recovery.py
# ==============================================================================
client = TestClient(app, client=("127.0.0.1", 50000))


def auth_headers():
    headers = {}
    if settings.API_KEY:
        headers["X-API-Key"] = settings.API_KEY
        token = issue_2fa_token()
        if token:
            headers["X-2FA-Token"] = token
    return headers


@pytest.mark.asyncio
async def test_close_worker_sender_returns_borrowed_sender():
    """Return the borrow while preserving Telethon's shared cached sender."""
    mock_client = MagicMock()
    mock_client._return_exported_sender = AsyncMock()
    mock_sender = MagicMock()
    mock_sender.disconnect = AsyncMock()

    dc_id = 5
    # 模拟 Telethon 的 _borrowed_senders 字典
    mock_client._borrowed_senders = {dc_id: (MagicMock(), mock_sender)}

    await _close_worker_sender(mock_client, mock_sender, dc_id)

    mock_sender.disconnect.assert_not_awaited()
    mock_client._return_exported_sender.assert_awaited_once_with(mock_sender)
    assert mock_client._borrowed_senders[dc_id][1] is mock_sender


@pytest.mark.asyncio
async def test_get_worker_sender_delegates_reconnection_to_telethon():
    """Telethon handles reconnecting cached senders without losing borrow counts."""
    mock_client = MagicMock()
    mock_client.session.dc_id = 4
    mock_client.session.auth_key = MagicMock()

    dead_sender = MagicMock()
    dead_sender.is_connected.return_value = False

    fresh_sender = MagicMock()
    fresh_sender.is_connected.return_value = True

    dc_id = 5
    mock_client._borrowed_senders = {dc_id: (MagicMock(), dead_sender)}

    mock_client._borrow_exported_sender = AsyncMock(return_value=fresh_sender)

    result_sender = await _get_worker_sender(mock_client, dc_id)

    assert result_sender == fresh_sender
    assert mock_client._borrow_exported_sender.called
    assert mock_client._borrowed_senders[dc_id][1] is dead_sender


def test_pause_live_task():
    """运行中且协程存活的任务，暂停时设置 paused 控制标志"""
    task_id = "test_pause_live_1"
    TaskQueueRepository.create(task_id, "download_link", {"links": ["https://t.me/test/1"]})
    TaskQueueRepository.update_status(task_id, "running")
    active_task_controls[task_id] = {"cancelled": False, "paused": False}

    try:
        resp = client.post(f"/api/tasks/{task_id}/pause", headers=auth_headers())
        assert resp.status_code == 200
        assert resp.json()["status"] == "paused"
        assert active_task_controls[task_id]["paused"] is True
        assert TaskQueueRepository.get_by_id(task_id)["status"] == "paused"
    finally:
        active_task_controls.pop(task_id, None)
        TaskQueueRepository.delete(task_id)


def test_pause_orphaned_task_safe_no_quota_underflow(monkeypatch):
    """运行中但协程已退出的僵尸任务，暂停时安全置 paused，且不会误扣调度器计数"""
    task_id = "test_pause_orphan_1"
    TaskQueueRepository.create(task_id, "download_link", {"links": ["https://t.me/test/2"]}, account_id=1)
    TaskQueueRepository.update_status(task_id, "running")

    # 确保 task_id 不在 active_task_controls 和 _running_tasks 中
    active_task_controls.pop(task_id, None)
    task_scheduler._running_tasks.pop(task_id, None)

    cleanup_mock = MagicMock()
    monkeypatch.setattr(task_scheduler, "_cleanup_task_resources", cleanup_mock)

    try:
        resp = client.post(f"/api/tasks/{task_id}/pause", headers=auth_headers())
        assert resp.status_code == 200
        assert resp.json()["status"] == "paused"
        assert TaskQueueRepository.get_by_id(task_id)["status"] == "paused"

        # 因为 task_id 不在 _running_tasks 中，严禁重复调用 _cleanup_task_resources 导致账号配额误扣
        assert not cleanup_mock.called
    finally:
        TaskQueueRepository.delete(task_id)


@pytest.mark.parametrize("executor_done", [True, False])
def test_pause_only_cleans_up_a_finished_scheduler_executor(monkeypatch, executor_done):
    """暂停后只回收已结束但仍留在调度器中的执行器。"""
    task_id = "test_pause_stuck_1"
    TaskQueueRepository.create(task_id, "download_link", {"links": ["https://t.me/test/3"]}, account_id=1)
    TaskQueueRepository.update_status(task_id, "running")

    active_task_controls.pop(task_id, None)
    task_scheduler._running_tasks[task_id] = MagicMock(done=MagicMock(return_value=executor_done))

    cleanup_mock = MagicMock()
    notify_mock = MagicMock()
    monkeypatch.setattr(task_scheduler, "_cleanup_task_resources", cleanup_mock)
    monkeypatch.setattr(task_scheduler, "notify_new_task", notify_mock)

    try:
        resp = client.post(f"/api/tasks/{task_id}/pause", headers=auth_headers())
        assert resp.status_code == 200
        assert resp.json()["status"] == "paused"
        assert TaskQueueRepository.get_by_id(task_id)["status"] == "paused"

        assert cleanup_mock.called is executor_done
        assert notify_mock.called is executor_done
        if not executor_done:
            assert active_task_controls[task_id]["paused"]
    finally:
        task_scheduler._running_tasks.pop(task_id, None)
        TaskQueueRepository.delete(task_id)


# ==============================================================================
# Part: test_download_resume_coroutine.py
# ==============================================================================
@pytest.mark.asyncio
async def test_download_task_pause_and_resume_reenters_scheduler(monkeypatch):
    """验证下载任务暂停后再恢复时，不走死协程假分支，而是进入调度队列重新拉起"""
    task_id = "test_resume_coro_1"
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="download_link",
        params={"links": ["https://t.me/c/12345/100"], "threads": 16},
        account_id=1,
        priority=10,
    )

    try:
        # 1. 模拟运行中
        active_task_controls[task_id] = {"cancelled": False, "paused": False}
        TaskQueueRepository.update_status(task_id, "running")

        # 2. 调用 pause_task
        p_res = await pause_task(task_id)
        assert p_res["status"] == "paused"
        assert TaskQueueRepository.get_by_id(task_id)["status"] == "paused"
        assert active_task_controls[task_id]["paused"] is True

        # 3. 模拟旧协程退出清理
        active_task_controls.pop(task_id, None)

        # 4. 调用 resume_task 恢复 (派发统一, 无 BackgroundTasks 形参)
        with patch.object(task_scheduler, "notify_new_task") as mock_notify:
            monkeypatch.setattr(task_scheduler, "_is_running", True)
            await resume_task(task_id)

            # 状态应置为 queued，且通知调度器拉起
            t_after = TaskQueueRepository.get_by_id(task_id)
            assert t_after["status"] == "queued"
            mock_notify.assert_called_once()
            # 残留控制条目已被清除 (旧协程已退出): 调度器认领时由 try_claim setdefault 重建
            # 整体覆盖写会抹掉调度窗口内用户新置位的取消/暂停标志
            assert task_id not in active_task_controls

    finally:
        active_task_controls.pop(task_id, None)
        TaskQueueRepository.delete(task_id)


# ==============================================================================
# Part: test_multi_userbot_download.py
# ==============================================================================
CHUNK = 512 * 1024


def _create_mock_message(msg_id: int = 888, chat_id: int = -1009000000029, dc_id: int = 5, file_size: int = CHUNK):
    doc = MagicMock()
    doc.size = file_size
    doc.dc_id = dc_id
    doc.attributes = [DocumentAttributeFilename(file_name="multi_test.mp4")]
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


def _create_mock_account(account_id: int, name: str, dc_id: int = 5):
    mock_client = AsyncMock()
    mock_client.session = MagicMock(dc_id=dc_id, auth_key=f"key_{account_id}".encode())
    mock_client.is_connected = MagicMock(return_value=True)

    acc = TelegramAccountClient(
        account_id=account_id,
        account_name=name,
        phone=f"+10000{account_id}",
        session_file=f"session_{account_id}",
        api_id=123456,
        api_hash="hash",
        max_concurrency=2,
        is_active=True,
        is_default=(account_id == 101),
        dc_id=dc_id,
    )
    acc.client = mock_client
    acc.auth_state = "AUTHENTICATED"
    return acc


@pytest.mark.asyncio
async def test_get_accessible_userbots_probe_and_cache_ttl(monkeypatch):
    """测试多 Userbot 权限探测、过滤无权账号及 60 秒防雪崩缓存命中与失效"""
    cm = ClientManager()

    acc1 = _create_mock_account(101, "Userbot-HK")
    acc2 = _create_mock_account(102, "Userbot-US")
    acc3 = _create_mock_account(103, "Userbot-SG")

    cm._accounts = {101: acc1, 102: acc2, 103: acc3}

    probe_calls = []

    async def fake_resolve_entity(client, parsed_chat):
        probe_calls.append(client)
        if client == acc1.client or client == acc2.client:
            return MagicMock(id=123)
        raise Exception("ChannelPrivateError: channel is private")

    monkeypatch.setattr("app.core.client_manager.resolve_entity", fake_resolve_entity)

    # 1. 首次探测：acc1 与 acc2 在群，acc3 无权被排除
    accessible = await cm.get_accessible_userbots("-1009000000029")
    assert len(accessible) == 2
    accessible_ids = [a.account_id for a in accessible]
    assert 101 in accessible_ids
    assert 102 in accessible_ids
    assert 103 not in accessible_ids
    assert len(probe_calls) == 3

    # 2. 60秒内二次调用：直接命中防雪崩缓存，不触发任何远程 RPC 探针
    probe_calls.clear()
    cached = await cm.get_accessible_userbots("-1009000000029")
    assert len(cached) == 2
    assert len(probe_calls) == 0  # 零远程探针 RPC

    # 3. 某账号遭遇 FloodWait 时缓存失效或自动排除
    cm.mark_flood_wait(acc2.client, 60)
    assert acc2.is_flood_locked() is True

    # 重新探测或读取时，被 Flood 锁定的账号不再出现在可用列表中
    accessible_after_flood = await cm.get_accessible_userbots("-1009000000029")
    after_ids = [a.account_id for a in accessible_after_flood]
    assert 101 in after_ids
    assert 102 not in after_ids


@pytest.mark.asyncio
async def test_dual_userbots_download_worker_distribution(tmp_dir, monkeypatch):
    """验证双 Userbot 传入 download_single_message 时，凭证分别预取并交替分配 Worker"""
    acc1 = _create_mock_account(101, "Userbot-HK", dc_id=5)
    acc2 = _create_mock_account(102, "Userbot-US", dc_id=5)

    msg = _create_mock_message(msg_id=888, file_size=CHUNK)

    # 模拟两个 Userbot 均在群内，能够独立获取自身专属 location
    loc1 = MagicMock(name="loc1")
    loc2 = MagicMock(name="loc2")

    msg_c1 = MagicMock(media=MagicMock())
    msg_c2 = MagicMock(media=MagicMock())
    acc1.client.get_messages = AsyncMock(return_value=msg_c1)
    acc2.client.get_messages = AsyncMock(return_value=msg_c2)

    monkeypatch.setattr(
        "app.services.downloader_service.get_input_location", lambda m: loc1 if m == msg_c1.media else loc2
    )
    monkeypatch.setattr(
        DownloaderService, "get_file_name_and_size", staticmethod(lambda m, fallback_id=None: ("multi.mp4", CHUNK))
    )

    captured_args = {}
    mock_out = os.path.join(tmp_dir, "multi.mp4")

    async def fake_download_file_parallel(**kwargs):
        captured_args.update(kwargs)
        safe_write_bytes(mock_out, b"mock_content")
        return mock_out

    monkeypatch.setattr("app.services.downloader_service.download_file_parallel", fake_download_file_parallel)

    # 执行下载
    res = await DownloaderService.download_single_message(
        client=[acc1.client, acc2.client], chat_id=-1009000000029, message=msg
    )

    assert res == mock_out
    clients_passed = captured_args.get("client")
    assert clients_passed == [acc1.client, acc2.client]
    client_locations_passed = captured_args.get("client_locations")
    assert acc1.client in client_locations_passed
    assert acc2.client in client_locations_passed
    assert client_locations_passed[acc1.client] == loc1
    assert client_locations_passed[acc2.client] == loc2


@pytest.mark.asyncio
async def test_mid_flight_failover_on_flood_wait(tmp_dir, monkeypatch):
    """验证多 Userbot 分块下载中，其中一号突发 FloodWait 时动态摘除，存活 Userbot 接管续传且不中断任务"""

    def _make_mock_sender():
        s = MagicMock()
        s.is_connected = MagicMock(return_value=True)
        tr = MagicMock()
        tr.set_write_buffer_limits = MagicMock()
        s._connection = MagicMock(_writer=MagicMock(transport=tr))
        return s

    client1 = AsyncMock()
    client1.session = MagicMock(dc_id=5, auth_key=b"k1")
    client1.is_connected = MagicMock(return_value=True)
    client1._borrow_exported_sender = AsyncMock(side_effect=lambda dc: _make_mock_sender())

    client2 = AsyncMock()
    client2.session = MagicMock(dc_id=5, auth_key=b"k2")
    client2.is_connected = MagicMock(return_value=True)
    client2._borrow_exported_sender = AsyncMock(side_effect=lambda dc: _make_mock_sender())

    # client1 正常返回数据
    async def c1_call(sender, request):
        res = MagicMock()
        res.bytes = b"A" * CHUNK
        return res

    # client2 模拟抛出 FloodWait
    client2_flood_raised = False

    async def c2_call(sender, request):
        nonlocal client2_flood_raised
        client2_flood_raised = True
        raise Exception("FloodWait 30s")

    client1._call = AsyncMock(side_effect=c1_call)
    client2._call = AsyncMock(side_effect=c2_call)

    async def fake_create_same_dc(c, dc_id, dc_option=None):
        s = MagicMock()
        s.dc_id = dc_id
        return s

    monkeypatch.setattr(fast_telethon.sender_pool, "_create_worker_sender_same_dc", fake_create_same_dc)
    monkeypatch.setattr(fast_telethon.sender_pool, "_close_worker_sender", AsyncMock())

    # out_file 位于 tests/.tmp 而非真实下载目录: 注入 DOWNLOAD_DIR 使 trust_root 沙箱与落盘目录一致 (先例 test_transit_bridge)
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", tmp_dir)
    out_file = os.path.join(tmp_dir, "failover_test.bin")
    total_bytes = 2 * CHUNK
    raw_media = _create_mock_message(dc_id=5, file_size=total_bytes).media

    client_locations = {client1: MagicMock(), client2: MagicMock()}

    # 执行 2 通道并发联合下载
    res = await download_file_parallel(
        client=[client1, client2],
        location=client_locations[client1],
        out_file_path=out_file,
        total_bytes=total_bytes,
        concurrency=2,
        raw_media=raw_media,
        client_locations=client_locations,
    )

    # 验证任务完成且 client2 触发了 Flood 摘除并由 client1 顺利接管完成
    assert res == out_file
    assert client2_flood_raised is True
    assert os.path.exists(out_file)
    assert os.path.getsize(out_file) == total_bytes


@pytest.mark.asyncio
async def test_single_userbot_fallback_equivalence(tmp_dir, monkeypatch):
    """验证当仅单 Userbot 在群时，100% 透明降级为 Phase 1 单 Userbot 纯净模式且同 DC 零 Export 握手"""

    def _make_mock_sender():
        s = MagicMock()
        s.is_connected = MagicMock(return_value=True)
        tr = MagicMock()
        tr.set_write_buffer_limits = MagicMock()
        s._connection = MagicMock(_writer=MagicMock(transport=tr))
        return s

    client = AsyncMock()
    client._borrow_exported_sender = AsyncMock(side_effect=lambda dc: _make_mock_sender())
    client.session = MagicMock(dc_id=5, auth_key=b"k_single")
    client.is_connected = MagicMock(return_value=True)

    async def ok_call(sender, request):
        res = MagicMock()
        res.bytes = b"S" * CHUNK
        return res

    client._call = AsyncMock(side_effect=ok_call)

    export_called = False

    async def fake_export(*args, **kwargs):
        nonlocal export_called
        export_called = True
        return MagicMock()

    client.side_effect = lambda req: (
        fake_export() if isinstance(req, functions.auth.ExportAuthorizationRequest) else None
    )

    async def fake_create_same_dc(c, dc_id, dc_option=None):
        s = MagicMock()
        s.dc_id = dc_id
        return s

    monkeypatch.setattr(fast_telethon.sender_pool, "_create_worker_sender_same_dc", fake_create_same_dc)

    monkeypatch.setattr(settings, "DOWNLOAD_DIR", tmp_dir)
    out_file = os.path.join(tmp_dir, "single_fallback.bin")
    total_bytes = 2 * CHUNK
    raw_media = _create_mock_message(dc_id=5, file_size=total_bytes).media

    res = await download_file_parallel(
        client=[client],
        location=MagicMock(dc_id=5),
        out_file_path=out_file,
        total_bytes=total_bytes,
        concurrency=2,
        raw_media=raw_media,
    )

    assert res == out_file
    assert export_called is False
    assert os.path.exists(out_file)
    assert os.path.getsize(out_file) == total_bytes


@pytest.mark.asyncio
async def test_auxiliary_account_concurrency_accounting():
    """验证辅助 Userbot 配额借用与任务清理时的原子归还"""
    scheduler = TaskScheduler()

    acc = _create_mock_account(102, "Aux-Userbot")
    with patch("app.core.scheduler.client_manager.get_account", return_value=acc):
        task_id = "task_test_999"

        # 借用配额
        scheduler.borrow_auxiliary_account(task_id, 102)
        assert scheduler._account_task_counts[102] == 1
        assert 102 in scheduler._task_auxiliary_accounts[task_id]

        # 清理任务资源
        scheduler._cleanup_task_resources(task_id, 101)
        assert scheduler._account_task_counts.get(102, 0) == 0
        assert task_id not in scheduler._task_auxiliary_accounts


def test_multi_userbot_executor_names_persistence_and_hydration():
    """验证多 Userbot 联合并发执行时，executor_names 与 account_ids 正确落库并被 hydration 提升至顶层属性"""
    task_id = f"test_multi_acc_{int(time.time() * 1000)}"
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="download_link",
        params={"links": ["https://t.me/c/123/456"]},
        account_id=1,
        priority=10,
    )
    try:
        acc_names = ["NG", "HK"]
        acc_ids = [1, 2]
        TaskQueueRepository.merge_params(task_id, {"executor_names": acc_names, "account_ids": acc_ids})

        # 1. 验证 get_by_id hydration
        fetched = TaskQueueRepository.get_by_id(task_id)
        assert fetched is not None
        assert fetched.get("executor_names") == ["NG", "HK"]
        assert fetched.get("account_ids") == [1, 2]
        assert fetched["params"].get("executor_names") == ["NG", "HK"]

        # 2. 验证 get_all hydration
        all_tasks = TaskQueueRepository.get_all(limit=20)
        target = next((item for item in all_tasks if item["id"] == task_id), None)
        assert target is not None
        assert target.get("executor_names") == ["NG", "HK"]
        assert target.get("account_ids") == [1, 2]

        # 3. 验证 query_tasks hydration
        q_tasks, _ = TaskQueueRepository.query_tasks(query=task_id, limit=5)
        assert len(q_tasks) >= 1
        assert q_tasks[0].get("executor_names") == ["NG", "HK"]
        assert q_tasks[0].get("account_ids") == [1, 2]
    finally:
        TaskQueueRepository.delete(task_id)


class _CaptureClient:
    """resolve_message_from_link 抓取客户端: 记录 (entity, ids) 调用对"""

    def __init__(self):
        self.calls: list[tuple] = []

    async def get_messages(self, entity, ids=None, **kwargs):
        self.calls.append((entity, ids))
        return SimpleNamespace(id=999, media=None)


@pytest.mark.asyncio
async def test_resolve_link_private_topic_form_fetches_last_segment():
    client = _CaptureClient()
    chat_id, msg = await DownloaderService.resolve_message_from_link(client, "https://t.me/c/9000000010/42/99")
    assert chat_id == -1009000000010
    assert client.calls[0][1] == 99, "三段式私有链接必须取末段消息 ID"
    assert msg is not None


@pytest.mark.asyncio
async def test_resolve_link_private_plain_form_unchanged():
    client = _CaptureClient()
    chat_id, _ = await DownloaderService.resolve_message_from_link(client, "https://t.me/c/9000000010/1234")
    assert chat_id == -1009000000010
    assert client.calls[0][1] == 1234, "两段式私有链接取消息 ID 语义不变"


@pytest.mark.asyncio
async def test_resolve_link_public_topic_and_preview_forms():
    client = _CaptureClient()
    await DownloaderService.resolve_message_from_link(client, "https://t.me/demo/42/99")
    assert client.calls[0] == ("demo", 99), "三段式公开链接取末段消息 ID"

    client2 = _CaptureClient()
    await DownloaderService.resolve_message_from_link(client2, "https://t.me/s/demo/17354")
    assert client2.calls[0] == ("demo", 17354), "/s/ 网页预览前缀跳过, 取频道名与消息 ID"


@pytest.mark.asyncio
async def test_download_single_message_reraises_flood_wait(monkeypatch):
    """下载引擎抛出的 FloodWaitError 必须原样上抛 (runner 拦截后走任务级退避)。"""
    import app.services.downloader_service as dls

    monkeypatch.setattr(TaskQueueRepository, "append_log_async", AsyncMock())

    def _flood(**kwargs):
        err = FloodWaitError(request=None)
        err.seconds = 30
        raise err

    monkeypatch.setattr(dls, "download_file_parallel", _flood)
    msg = SimpleNamespace(id=5, media=object())
    with pytest.raises(FloodWaitError):
        await DownloaderService.download_single_message(
            client=object(), chat_id="-100x", message=msg, task_id="dl_floodwait"
        )


@pytest.mark.asyncio
async def test_download_single_message_ordinary_failure_still_swallowed(monkeypatch):
    """普通失败保持原语义: 记日志后 return None (不误扩重抛面)"""
    calls = {"n": 0}

    async def _ordinary(tid, text):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("socket closed")

    monkeypatch.setattr(TaskQueueRepository, "append_log_async", _ordinary)
    msg = SimpleNamespace(id=6, media=object())
    result = await DownloaderService.download_single_message(
        client=object(), chat_id="-100x", message=msg, task_id="dl_ordinary"
    )
    assert result is None, "普通失败不重抛 (重抛面仅 FloodWait/会话失效)"


@pytest.mark.asyncio
async def test_album_discovery_timeout_raises_album_discovery_error():
    seed = MagicMock(id=50, grouped_id=777, input_chat=None)
    client_mock = AsyncMock()
    client_mock.get_messages = AsyncMock(side_effect=TimeoutError())

    with pytest.raises(AlbumDiscoveryError, match="抓取失败"):
        await DownloaderService.get_album_messages(client_mock, -1009000000010, seed)


@pytest.mark.asyncio
async def test_album_discovery_empty_result_raises_album_discovery_error():
    seed = MagicMock(id=50, grouped_id=777, input_chat=None)
    unrelated = MagicMock(id=51, grouped_id=888)
    client_mock = AsyncMock()
    client_mock.get_messages = AsyncMock(return_value=[unrelated])

    with pytest.raises(AlbumDiscoveryError, match="结果为空"):
        await DownloaderService.get_album_messages(client_mock, -1009000000010, seed)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc",
    [
        FloodWaitError(request=None, capture=15),
        AuthKeyUnregisteredError(request=None),
    ],
)
async def test_album_discovery_reraises_flood_wait_and_session_invalid(exc):
    seed = MagicMock(id=50, grouped_id=777, input_chat=None)
    client_mock = AsyncMock()
    client_mock.get_messages = AsyncMock(side_effect=exc)

    with pytest.raises(type(exc)):
        await DownloaderService.get_album_messages(client_mock, -1009000000010, seed)


@pytest.mark.asyncio
async def test_album_discovery_success_and_non_album_controls():
    non_album = MagicMock(id=10, grouped_id=None)
    client_mock = AsyncMock()
    assert await DownloaderService.get_album_messages(client_mock, -1001, non_album) == [non_album]
    client_mock.get_messages.assert_not_called()

    m2 = MagicMock(id=52, grouped_id=999)
    m1 = MagicMock(id=50, grouped_id=999)
    m3 = MagicMock(id=51, grouped_id=999)
    client_mock.get_messages = AsyncMock(return_value=[m2, m1, m3])
    res = await DownloaderService.get_album_messages(client_mock, "-1009000000010", m1)
    assert [m.id for m in res] == [50, 51, 52]


def test_is_channel_permission_error_traverses_cause_chain():
    inner = ChatAdminRequiredError(request=None)
    with pytest.raises(AlbumDiscoveryError) as exc_info:
        raise AlbumDiscoveryError("关联 Album 抓取失败") from inner
    assert is_channel_permission_error(exc_info.value) is True


@pytest.mark.asyncio
async def test_album_discovery_includes_msg_id_1_with_zero_min_id():
    seed = MagicMock(id=1, grouped_id=555, input_chat=None)
    client_mock = AsyncMock()
    client_mock.get_messages = AsyncMock(return_value=[seed])

    res = await DownloaderService.get_album_messages(client_mock, -1009000000010, seed)
    assert res == [seed]
    client_mock.get_messages.assert_awaited_once_with(-1009000000010, min_id=0, max_id=11)
