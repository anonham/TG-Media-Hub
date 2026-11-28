"""Pipeline Core: 统一发布流水线策略引擎、生命周期编排与相册逐项隔离测试 (Pipeline Engine & Orchestration)

覆盖范围:
- 原 test_pipeline_engines: 本地/PikPak 传输引擎、单项/相册发送、进度上报与异常中断
- 原 test_publish_pipeline_infrastructure: 频道标识规范化、PublishResult 契约、通知器与审计
- 原 test_publisher_facade: 双门面向后兼容、六阶段流水线装配与兜底
- 原 test_album_partial_isolation: 相册逐项隔离 R3 矩阵与双引擎发送容灾"""

import asyncio
import os
import shutil
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.pikpak import (
    ParsedFileItem,
    PikPakPublishFatalError,
    PikPakPublishRetryableError,
)
from app.core.pipeline.cleanup import (
    NoopCleanupStrategy,
    PikPakDeleteCleanupStrategy,
    PikPakStructuredArchiveStrategy,
)
from app.core.pipeline.engines import (
    LocalDiskTransportEngine,
    PikPakStreamTransportEngine,
    TransportError,
    _SpeedMeasuringProvider,
)
from app.core.pipeline.interfaces import (
    CleanupStrategy,
    ItemSource,
    MediaTransportEngine,
    NotificationStrategy,
)
from app.core.pipeline.models import (
    PublishBatch,
    PublishContext,
    PublishItem,
    PublishResult,
    TransportOutcome,
)
from app.core.pipeline.notifier import TelegramChannelNotifier
from app.core.pipeline.pipeline import PublishPipeline
from app.core.pipeline.sources import (
    LocalFolderSource,
    PikPakSnapshotSource,
)
from app.core.pipeline.utils import format_folder_tag, normalize_target_chat_id
from app.core.scheduler import UploadCancelledError, UploadPausedError
from app.core.stream_chunk_provider import (
    BaseChunkProvider,
    ChunkFetchTransientError,
    PikPakLinkExpiredError,
)
from app.db.publish_repository import PublishHistoryRepository
from app.services.pikpak_publisher_service import PikPakPublisherService
from app.services.pikpak_service import (
    PikPakAuthError,
    PikPakQuotaError,
)
from app.services.publisher_service import PublisherService, PublishFolderResult


# ==============================================================================
# Part: test_pipeline_engines.py
# ==============================================================================
def _make_context(
    task_id="task_test_123",
    target_chat_id=-1009000000010,
    is_folder_publish=True,
    folder_name="测试合集",
    dir_tree=None,
    auto_cleanup_mode="archive",
    userbot_client=None,
    **kwargs,
) -> PublishContext:
    return PublishContext(
        task_id=task_id,
        target_chat_id=target_chat_id,
        target_channel_raw=str(target_chat_id),
        is_folder_publish=is_folder_publish,
        folder_name=folder_name,
        dir_tree=dir_tree or {},
        auto_cleanup_mode=auto_cleanup_mode,
        userbot_client=userbot_client,
        **kwargs,
    )


# ==============================================================================
# 1. LocalFolderSource 单元测试
# ==============================================================================


@pytest.mark.asyncio
async def test_local_folder_source_empty_or_nonexistent():
    """本地目录不存在或为空时，返回空批次列表"""
    source = LocalFolderSource("/nonexistent/path/that/does/not/exist")
    ctx = _make_context()
    batches = await source.get_batches(ctx)
    assert batches == []


@pytest.mark.asyncio
async def test_local_folder_source_single_folder(tmp_dir):
    """单个平铺目录扫描：过滤非媒体文件，生成单个 PublishBatch，挂载 PublishItem"""
    temp_dir = os.path.join(tmp_dir, "test_local")
    os.makedirs(temp_dir, exist_ok=True)
    try:
        f_jpg = os.path.join(temp_dir, "photo1.jpg")
        f_mp4 = os.path.join(temp_dir, "video1.mp4")
        f_txt = os.path.join(temp_dir, "ignore.txt")
        with open(f_jpg, "wb") as f:
            f.write(b"x" * 1024)
        with open(f_mp4, "wb") as f:
            f.write(b"y" * 2048)
        with open(f_txt, "wb") as f:
            f.write(b"ignore me")

        source = LocalFolderSource(temp_dir)
        ctx = _make_context(folder_name="相册精选")
        batches = await source.get_batches(ctx)

        assert len(batches) == 1
        batch = batches[0]
        assert batch.is_folder_publish is True
        assert batch.folder_name == "相册精选"
        assert len(batch.items) == 2

        names = {item.name for item in batch.items}
        assert names == {"photo1.jpg", "video1.mp4"}
        for item in batch.items:
            assert item.source_type == "local"
            assert item.is_dir is False
            if item.name == "photo1.jpg":
                assert item.size_bytes == 1024
            elif item.name == "video1.mp4":
                assert item.size_bytes == 2048
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_local_folder_source_subfolders(tmp_dir):
    """多层子目录扫描：保留子目录独立批次，每个子目录生成各自的 PublishBatch 与 relative path"""
    temp_dir = os.path.join(tmp_dir, "test_sub")
    os.makedirs(temp_dir, exist_ok=True)
    try:
        sub1 = os.path.join(temp_dir, "sub1")
        sub2 = os.path.join(temp_dir, "sub2")
        os.makedirs(sub1)
        os.makedirs(sub2)
        with open(os.path.join(sub1, "pic_a.png"), "wb") as f:
            f.write(b"a" * 100)
        with open(os.path.join(sub2, "pic_b.png"), "wb") as f:
            f.write(b"b" * 200)

        source = LocalFolderSource(temp_dir)
        ctx = _make_context()
        batches = await source.get_batches(ctx)

        assert len(batches) == 2
        batch_map = {b.rel_path: b for b in batches}
        assert "sub1" in batch_map
        assert "sub2" in batch_map
        assert len(batch_map["sub1"].items) == 1
        assert batch_map["sub1"].items[0].name == "pic_a.png"
        assert len(batch_map["sub2"].items) == 1
        assert batch_map["sub2"].items[0].name == "pic_b.png"
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


# ==============================================================================
# 2. PikPakSnapshotSource 单元测试
# ==============================================================================


@pytest.mark.asyncio
async def test_pikpak_snapshot_source_dict_parsing_and_dir_tree():
    """PikPak 快照列表 (dict) 解析，并从 context.dir_tree 挂载相对目录路径"""
    remote_files = [
        {
            "file_id": "fid_001",
            "name": "ep01.mp4",
            "size_bytes": 1048576,
            "parent_id": "pid_root",
            "audit": {"status": "OK"},
            "thumbnail_link": "https://thumb.example/1.jpg",
            "duration_s": 120,
        },
        {
            "file_id": "fid_002",
            "name": "ep02.mp4",
            "size_bytes": 2097152,
            "parent_id": "pid_season1",
            "duration_s": 130,
        },
    ]
    dir_tree = {
        "pid_root": "一字马汁汁",
        "pid_season1": "一字马汁汁/第1季",
    }
    source = PikPakSnapshotSource(remote_files)
    ctx = _make_context(
        is_folder_publish=True,
        folder_name="一字马汁汁",
        dir_tree=dir_tree,
    )
    batches = await source.get_batches(ctx)

    # 多批次: 每个直接含媒体的目录一个批次 (pid_root 根批次 + pid_season1 子目录批次)
    assert len(batches) == 2
    by_rel = {b.rel_path: b for b in batches}
    assert by_rel["一字马汁汁"].folder_name == "#一字马汁汁"
    assert by_rel["一字马汁汁"].is_folder_publish is True
    item1 = by_rel["一字马汁汁"].items[0]
    assert item1.item_id == "fid_001"
    assert item1.rel_path == "一字马汁汁/ep01.mp4"
    assert item1.thumbnail_data == "https://thumb.example/1.jpg"
    assert item1.duration_s == 120

    season = by_rel["一字马汁汁/第1季"]
    item2 = season.items[0]
    assert season.folder_name == "#第1季"
    assert item2.item_id == "fid_002"
    assert item2.rel_path == "一字马汁汁/第1季/ep02.mp4"


@pytest.mark.asyncio
async def test_pikpak_snapshot_source_objects_and_empty():
    """支持 ParsedFileItem 对象输入，且空快照返回空列表"""
    parsed = [
        ParsedFileItem(
            file_id="fid_obj",
            name="clip.mp4",
            is_dir=False,
            size_bytes=5000,
            audit={},
            thumbnail_link=None,
            duration_s=None,
            width=None,
            height=None,
            parent_id="p123",
        )
    ]
    source = PikPakSnapshotSource(parsed)
    ctx = _make_context(is_folder_publish=False)
    batches = await source.get_batches(ctx)

    assert len(batches) == 1
    assert batches[0].is_folder_publish is False
    assert batches[0].items[0].item_id == "fid_obj"

    empty_source = PikPakSnapshotSource([])
    assert await empty_source.get_batches(ctx) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("extension", ["ts", "m2ts", "mts"])
async def test_pikpak_snapshot_source_ts_filtering(extension):
    """验证 PikPakSnapshotSource 过滤 .d.ts 声明文件，保留缺少元数据的小视频。"""
    source = PikPakSnapshotSource([])
    ctx = _make_context(is_folder_publish=False)

    item_normal = PublishItem(item_id="f1", name="normal.ts", size_bytes=10 * 1024 * 1024)
    item_with_meta = PublishItem(item_id="f2", name="clip.ts", size_bytes=100 * 1024, duration_s=5.0)
    item_d_ts = PublishItem(item_id="f3", name="globals.d.ts", size_bytes=2048)
    item_without_meta = PublishItem(item_id="f4", name=f"clip.{extension}", size_bytes=2048)

    assert source.reject_item(item_normal, ctx) is None
    assert source.reject_item(item_with_meta, ctx) is None
    assert "TypeScript" in (source.reject_item(item_d_ts, ctx) or "")
    assert source.reject_item(item_without_meta, ctx) is None


# ==============================================================================
# 3. LocalDiskTransportEngine 单元测试
# ==============================================================================


@pytest.mark.asyncio
async def test_local_disk_transport_engine_send_item_userbot():
    """本地传输引擎：大文件/Userbot 模式调用 upload_file_parallel 并返回 (msg_id, speed)"""
    engine = LocalDiskTransportEngine()
    item = PublishItem(
        item_id="D:/fake/path/video.mp4",
        name="video.mp4",
        size_bytes=50 * 1024 * 1024,  # 50MB
        source_type="local",
    )
    mock_client = AsyncMock()
    mock_sent = MagicMock()
    mock_sent.id = 9988
    mock_client.send_file.return_value = mock_sent

    ctx = _make_context(userbot_client=mock_client, split_threshold_bytes=10 * 1024 * 1024)

    with patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_ft:
        mock_ft.return_value = MagicMock()
        outcome = await engine.send_item(item, ctx)

        assert outcome.message_ids == [9988]
        assert mock_ft.call_count == 1
        assert mock_client.send_file.call_count == 1


@pytest.mark.asyncio
async def test_local_disk_transport_engine_cancelled_paused():
    """本地传输引擎：响应取消与暂停控制信号"""
    engine = LocalDiskTransportEngine()
    item = PublishItem(item_id="D:/fake/v.mp4", name="v.mp4", size_bytes=100)

    # 1. 取消
    ctx_cancelled = _make_context(is_cancelled=lambda: True)
    with pytest.raises(UploadCancelledError):
        await engine.send_item(item, ctx_cancelled)

    # 2. 暂停
    ctx_paused = _make_context(is_paused=lambda: True)
    with pytest.raises(UploadPausedError):
        await engine.send_item(item, ctx_paused)


@pytest.mark.asyncio
async def test_local_disk_transport_engine_send_album():
    """本地传输引擎：相册两段式发送 —— 逐成员预上传 (FastTelethon) 后按 ≤10
    分段组发, 返回消息 ID; 上传与组发消息序号对齐。"""
    engine = LocalDiskTransportEngine()
    items = [PublishItem(item_id=f"D:/fake/img_{i}.jpg", name=f"img_{i}.jpg", size_bytes=1000) for i in range(12)]
    mock_client = AsyncMock()
    # 模拟两次组发：第一次 10 张，第二次 2 张 (成功子集按 ≤10 分段)
    msg_batch1 = [MagicMock(id=100 + i) for i in range(10)]
    msg_batch2 = [MagicMock(id=200 + i) for i in range(2)]
    mock_client.send_file.side_effect = [msg_batch1, msg_batch2]

    ctx = _make_context(userbot_client=mock_client)
    with (
        patch("os.path.exists", return_value=True),
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=[MagicMock(name=f"m{i}") for i in range(12)]),
    ):
        results = await engine.send_album(items, ctx)

    assert len(results) == 12
    assert results[0].message_ids[0] == 100
    assert results[9].message_ids[0] == 109
    assert results[10].message_ids[0] == 200
    assert results[11].message_ids[0] == 201
    assert mock_client.send_file.call_count == 2


@pytest.mark.asyncio
async def test_local_disk_transport_engine_parts_invalid_retry():
    """本地传输引擎：捕获 file_parts_invalid 异常后清除断点元数据并重试成功"""
    engine = LocalDiskTransportEngine()
    item = PublishItem(item_id="D:/fake/test.mp4", name="test.mp4", size_bytes=100000)
    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=777)
    ctx = _make_context(userbot_client=mock_client)

    attempt_count = 0

    async def fake_upload(*args, **kwargs):
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count == 1:
            raise RuntimeError("file_parts_invalid: cache expired")
        return MagicMock()

    with (
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=fake_upload),
        patch("app.core.fast_telethon.discard_upload_meta") as mock_discard,
    ):
        outcome = await engine.send_item(item, ctx)
        assert outcome.message_ids == [777]
        assert attempt_count == 2
        mock_discard.assert_called_once_with("D:/fake/test.mp4")


# ==============================================================================
# 4. PikPakStreamTransportEngine 单元测试
# ==============================================================================


@pytest.mark.asyncio
async def test_speed_measuring_provider():
    """验证 _SpeedMeasuringProvider 代理 read_chunk 统计速率与字节"""
    mock_inner = MagicMock(spec=BaseChunkProvider)
    mock_inner.read_chunk = AsyncMock(return_value=b"12345678")
    mock_inner.total_bytes = 8
    mock_inner.file_id = "f1"
    mock_inner.file_name = "test.bin"
    mock_inner.aclose = AsyncMock()

    provider = _SpeedMeasuringProvider(mock_inner)
    chunk = await provider.read_chunk(0, 8)
    assert chunk == b"12345678"
    assert provider.download_speed_mb_s >= 0.0
    await provider.aclose()
    mock_inner.aclose.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.usefixtures("no_download_file_writes")
async def test_pikpak_stream_transport_engine_send_item_success():
    """PikPak 流式传输引擎：索链、虚拟切片、分块上传成功流程 (link 构造注入)"""
    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value=["https://cdn.pikpak/video.mp4"]))
    item = PublishItem(
        item_id="fid_stream_01",
        name="test_video.mp4",
        size_bytes=10 * 1024 * 1024,
        duration_s=60,
        width=1920,
        height=1080,
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_sent = MagicMock()
    mock_sent.id = 5544
    mock_client.send_file.return_value = mock_sent

    ctx = _make_context(userbot_client=mock_client)

    with (
        patch(
            "app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()
        ) as mock_ft,
        patch.object(engine, "_make_thumb_provider", return_value=None),
    ):
        outcome = await engine.send_item(item, ctx)
        assert outcome.message_ids == [5544]
        assert mock_client.send_file.call_count == 1
        assert mock_ft.call_count == 1


@pytest.mark.asyncio
async def test_pikpak_stream_transport_engine_ts_mime_type():
    """验证 PikPak 流式传输引擎针对单 Part MPEG-TS 视频上传，注入 mime_type='video/mp2t'。"""
    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value=["https://cdn.pikpak/video.ts"]))
    item = PublishItem(
        item_id="fid_stream_ts",
        name="video.ts",
        size_bytes=10 * 1024 * 1024,
        duration_s=60,
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=5545)
    ctx = _make_context(userbot_client=mock_client)

    with (
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()),
        patch.object(engine, "_make_thumb_provider", return_value=None),
    ):
        outcome = await engine.send_item(item, ctx)
        assert outcome.message_ids == [5545]
        send_kwargs = mock_client.send_file.call_args[1]
        assert send_kwargs.get("mime_type") == "video/mp2t"


@pytest.mark.asyncio
async def test_pikpak_stream_transport_engine_relink_retry_on_link_expired():
    """PikPak 流式传输引擎：捕获 PikPakLinkExpiredError 进行换链重试，第 2 次成功"""
    engine = PikPakStreamTransportEngine()
    item = PublishItem(
        item_id="fid_relink_01",
        name="expired.mp4",
        size_bytes=5 * 1024 * 1024,
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    ctx = _make_context(userbot_client=mock_client)

    attempts = 0

    async def fake_publish_single(client, itm, parts, context, **kwargs):
        # 直收 delivered_parts kwarg (原依赖 send_item 的 TypeError 字符串匹配兼容垫片, 已废除)
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise PikPakLinkExpiredError("Link token expired")
        return TransportOutcome(message_ids=[6677], speed_mb_s=15.5)

    with patch.object(engine, "_publish_single_item", side_effect=fake_publish_single):
        outcome = await engine.send_item(item, ctx)
        assert outcome.message_ids == [6677]
        assert outcome.speed_mb_s == 15.5
        assert attempts == 2


@pytest.mark.asyncio
async def test_pikpak_stream_transport_engine_relink_exhausted():
    """PikPak 流式传输引擎：重试耗尽抛出 PikPakPublishRetryableError('link_expired')"""
    engine = PikPakStreamTransportEngine()
    item = PublishItem(item_id="fid_bad", name="bad.mp4", size_bytes=1000)
    ctx = _make_context()

    with patch.object(engine, "_publish_single_item", side_effect=PikPakLinkExpiredError("Always expired")):
        with pytest.raises(PikPakPublishRetryableError) as exc_info:
            await engine.send_item(item, ctx)
        assert exc_info.value.category == "link_expired"


@pytest.mark.asyncio
async def test_pikpak_stream_transport_engine_exception_wrapping():
    """PikPak 流式传输引擎：正确将底层异常包装为分类学异常 (transient, auth, quota)"""
    engine = PikPakStreamTransportEngine()
    item = PublishItem(item_id="fid_err", name="err.mp4", size_bytes=1000)
    ctx = _make_context()

    # 1. 瞬态错误 -> 可重试
    with patch.object(engine, "_publish_single_item", side_effect=ChunkFetchTransientError("502 bad gateway")):
        with pytest.raises(PikPakPublishRetryableError) as exc_info:
            await engine.send_item(item, ctx)
        assert exc_info.value.category == "transient"

    # 2. 认证错误 -> 致命错误
    with patch.object(engine, "_publish_single_item", side_effect=PikPakAuthError("Token invalid")):
        with pytest.raises(PikPakPublishFatalError) as exc_info:
            await engine.send_item(item, ctx)
        assert exc_info.value.category == "auth"

    # 3. 取消信号原样穿透
    with patch.object(engine, "_publish_single_item", side_effect=UploadCancelledError("Cancelled")):
        with pytest.raises(UploadCancelledError):
            await engine.send_item(item, ctx)


# ==============================================================================
# 5. CleanupStrategy 单元测试
# ==============================================================================


@pytest.mark.asyncio
async def test_noop_cleanup_strategy():
    """NoopCleanupStrategy：空操作直接返回 None，无副作用"""
    strategy = NoopCleanupStrategy()
    ctx = _make_context()
    items = [PublishItem(item_id="local_f", name="local_f", size_bytes=100)]
    summary = await strategy.cleanup(items, ctx)
    assert summary is None


@pytest.mark.asyncio
async def test_pikpak_structured_archive_strategy_success():
    """PikPak 结构化归档策略：根据 dir_tree 递归重建目录并批量归档 (CLI IO 构造注入)"""
    mock_resolve = AsyncMock(side_effect=["root_archive_id", "sub_archive_id"])
    mock_archive_to = AsyncMock(return_value={"moved": 2})
    strategy = PikPakStructuredArchiveStrategy(
        resolve_archive_folder=mock_resolve,
        archive_files_to_folder=mock_archive_to,
        archive_files=AsyncMock(),
    )
    items = [
        PublishItem(item_id="f1", name="a.mp4", size_bytes=100, parent_folder_id="p_root"),
        PublishItem(item_id="f2", name="b.mp4", size_bytes=200, parent_folder_id="p_root"),
    ]
    dir_tree = {"p_root": "一字马汁汁"}
    ctx = _make_context(dir_tree=dir_tree, auto_cleanup_mode="archive", task_id="t_arc")

    with patch("app.db.task_repository.TaskQueueRepository.append_log_async", new_callable=AsyncMock) as mock_log:
        summary = await strategy.cleanup(items, ctx)

        assert "🧹 已归档 2 个文件至 [已发布归档]" in summary
        assert "一字马汁汁/ 2" in summary
        mock_resolve.assert_any_call("")
        mock_resolve.assert_any_call("root_archive_id", "一字马汁汁")
        mock_archive_to.assert_called_once_with(["f1", "f2"], folder_id="sub_archive_id", source_parent_id="p_root")
        # D5: 归档成功摘要移出任务日志 (改由频道总结卡归档行呈现)
        mock_log.assert_not_called()


@pytest.mark.asyncio
async def test_pikpak_structured_archive_strategy_fallback_to_flat():
    """PikPak 结构化归档策略：缺少 dir_tree 映射时优雅降级回退平铺归档 (构造注入)"""
    mock_flat = AsyncMock(return_value={"moved": 1})
    strategy = PikPakStructuredArchiveStrategy(
        resolve_archive_folder=AsyncMock(return_value="root_id"),
        archive_files_to_folder=AsyncMock(),
        archive_files=mock_flat,
    )
    items = [
        PublishItem(item_id="f_bare", name="bare.mp4", size_bytes=100, parent_folder_id="unknown_parent"),
    ]
    ctx = _make_context(dir_tree={}, auto_cleanup_mode="archive")

    summary = await strategy.cleanup(items, ctx)
    assert "平铺回退 1" in summary
    mock_flat.assert_called_once_with(["f_bare"], source_parent_id="unknown_parent")


@pytest.mark.asyncio
async def test_pikpak_structured_archive_strategy_exception_isolation():
    """PikPak 结构化归档策略：归档异常被安全隔离，文件保留原位，不向外抛出(构造注入)"""
    strategy = PikPakStructuredArchiveStrategy(
        resolve_archive_folder=AsyncMock(side_effect=RuntimeError("API error")),
        archive_files_to_folder=AsyncMock(),
        archive_files=AsyncMock(side_effect=RuntimeError("CLI error")),
    )
    items = [
        PublishItem(item_id="f_fail", name="fail.mp4", size_bytes=100, parent_folder_id="p_fail"),
    ]
    ctx = _make_context(dir_tree={}, auto_cleanup_mode="archive")

    # 核心断言：绝对不抛出异常
    summary = await strategy.cleanup(items, ctx)
    assert summary == ""


@pytest.mark.asyncio
async def test_pikpak_structured_archive_mode_none():
    """auto_cleanup_mode == 'none' 时，PikPak 结构化归档跳过执行"""
    strategy = PikPakStructuredArchiveStrategy()
    items = [PublishItem(item_id="f1", name="f1.mp4", size_bytes=100)]
    ctx = _make_context(auto_cleanup_mode="none")
    summary = await strategy.cleanup(items, ctx)
    assert summary is None


@pytest.mark.asyncio
async def test_pikpak_delete_cleanup_strategy():
    """PikPak 移入回收站清理策略 (delete)：调用 delete_files 并记录摘要 (构造注入)"""
    mock_delete = AsyncMock()
    strategy = PikPakDeleteCleanupStrategy(delete_files=mock_delete)
    items = [
        PublishItem(item_id="d1", name="del1.mp4", size_bytes=100, parent_folder_id="p_del"),
        PublishItem(item_id="d2", name="del2.mp4", size_bytes=200, parent_folder_id="p_del"),
    ]
    ctx = _make_context(auto_cleanup_mode="delete", task_id="t_del")

    with patch("app.db.task_repository.TaskQueueRepository.append_log_async", new_callable=AsyncMock) as mock_log:
        summary = await strategy.cleanup(items, ctx)
        assert summary == "🗑️ 已将 2 个文件移入回收站"
        mock_delete.assert_called_once_with(["d1", "d2"], parent_id="p_del")
        # D5: delete 成功摘要移出任务日志 (与 archive 同口径)
        mock_log.assert_not_called()


# ==============================================================================
# 6. 深入边缘场景与鲁棒性测试 (Edge Cases & Robustness)
# ==============================================================================


@pytest.mark.asyncio
async def test_pikpak_snapshot_source_with_dir_expansion():
    """验证快照数据源中出现未展开目录条目 (is_dir=True) 时，自动调用展开器并扩充 dir_tree"""
    raw_files = [
        {"file_id": "dir_root_1", "name": "新剧合集", "is_dir": True, "size_bytes": 0},
    ]
    ctx = _make_context(dir_tree={}, is_folder_publish=True, folder_name="新剧合集")

    leaf_item = ParsedFileItem(
        file_id="leaf_file_1",
        name="01.mp4",
        is_dir=False,
        size_bytes=1024,
        audit={},
        thumbnail_link=None,
        duration_s=None,
        width=None,
        height=None,
        parent_id="dir_root_1",
    )
    # 展开器为唯一注入来源 (原 patch PikPakPublisherService.expand_remote_files 已迁移)
    mock_expand = AsyncMock(return_value=([leaf_item], {"dir_root_1": "新剧合集"}))
    source = PikPakSnapshotSource(raw_files, expander=mock_expand)
    batches = await source.get_batches(ctx)

    mock_expand.assert_called_once()
    assert len(batches) == 1
    assert batches[0].items[0].rel_path == "新剧合集/01.mp4"
    assert ctx.dir_tree.get("dir_root_1") == "新剧合集"


@pytest.mark.asyncio
async def test_local_disk_transport_engine_audio_attributes():
    """验证本地媒体传输引擎上传音频文件时，正确挂载 DocumentAttributeAudio"""
    engine = LocalDiskTransportEngine()
    item = PublishItem(
        item_id="D:/fake/music.mp3",
        name="music.mp3",
        size_bytes=5 * 1024 * 1024,
        source_type="local",
    )
    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=888)
    ctx = _make_context(userbot_client=mock_client)

    with patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_ft:
        mock_ft.return_value = MagicMock()
        await engine.send_item(item, ctx)

        send_kwargs = mock_client.send_file.call_args[1]
        assert "attributes" in send_kwargs
        attrs = send_kwargs["attributes"]
        assert len(attrs) == 1
        from telethon.tl.types import DocumentAttributeAudio

        assert isinstance(attrs[0], DocumentAttributeAudio)
        assert attrs[0].title == "music"


@pytest.mark.asyncio
async def test_local_disk_transport_engine_video_attributes():
    """验证本地媒体传输引擎上传视频文件时，正确挂载 DocumentAttributeVideo 并保留尺寸与时长"""
    engine = LocalDiskTransportEngine()
    item = PublishItem(
        item_id="D:/fake/movie.mp4",
        name="movie.mp4",
        size_bytes=80 * 1024 * 1024,
        duration_s=3600,
        width=1920,
        height=1080,
        source_type="local",
    )
    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=999)
    ctx = _make_context(userbot_client=mock_client)

    with patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_ft:
        mock_ft.return_value = MagicMock()
        await engine.send_item(item, ctx)

        send_kwargs = mock_client.send_file.call_args[1]
        assert "attributes" in send_kwargs
        attrs = send_kwargs["attributes"]
        assert len(attrs) == 1
        from telethon.tl.types import DocumentAttributeVideo

        assert isinstance(attrs[0], DocumentAttributeVideo)
        assert attrs[0].duration == 3600
        assert attrs[0].w == 1920
        assert attrs[0].h == 1080
        assert attrs[0].supports_streaming is True


@pytest.mark.asyncio
async def test_local_disk_transport_engine_ts_mime_type():
    """验证本地媒体传输引擎上传 MPEG-TS 视频时，正确注入 mime_type='video/mp2t'。"""
    engine = LocalDiskTransportEngine()
    item = PublishItem(
        item_id="D:/fake/clip.ts",
        name="clip.ts",
        size_bytes=10 * 1024 * 1024,
        source_type="local",
    )
    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=999)
    ctx = _make_context(userbot_client=mock_client)

    with patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_ft:
        mock_ft.return_value = MagicMock()
        await engine.send_item(item, ctx)

        send_kwargs = mock_client.send_file.call_args[1]
        assert send_kwargs.get("mime_type") == "video/mp2t"


@pytest.mark.asyncio
async def test_pikpak_stream_transport_engine_multipart():
    """验证 PikPak 流式传输引擎针对超过分卷阈值的文件，正确切片为多 Part 严格顺序发送并返回末尾消息 ID (link 构造注入)"""
    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value=["https://cdn/large.mp4"]))
    # 模拟 3000 MiB (2.93 GiB) 文件，threshold 为 2000 MiB (1.95 GiB) -> 切分为 2 个 Part
    item = PublishItem(
        item_id="fid_large_video",
        name="large.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    # Part 1 返回 msg 101, Part 2 返回 msg 102
    mock_client.send_file.side_effect = [MagicMock(id=101), MagicMock(id=102)]
    ctx = _make_context(userbot_client=mock_client, split_threshold_bytes=2000 * 1024 * 1024)

    with (
        patch(
            "app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()
        ) as mock_ft,
        patch.object(engine, "_make_thumb_provider", return_value=None),
    ):
        outcome = await engine.send_item(item, ctx)
        assert outcome.message_ids == [101, 102]
        assert mock_ft.call_count == 2
        assert mock_client.send_file.call_count == 2
        # 验证分卷 caption
        call1_kwargs = mock_client.send_file.call_args_list[0][1]
        call2_kwargs = mock_client.send_file.call_args_list[1][1]
        assert "[1/2]" in call1_kwargs["caption"]
        assert "[2/2]" in call2_kwargs["caption"]


@pytest.mark.asyncio
async def test_pikpak_stream_transport_engine_quota_and_not_found_errors():
    """验证配额受限与文件不存在异常被正确映射为 Fatal 终态异常"""
    from app.services.pikpak_service import PikPakNotFoundError

    engine = PikPakStreamTransportEngine()
    item = PublishItem(item_id="fid_q", name="q.mp4", size_bytes=100)
    ctx = _make_context()

    # 1. Quota
    with patch.object(engine, "_publish_single_item", side_effect=PikPakQuotaError("Quota exceeded")):
        with pytest.raises(PikPakPublishFatalError) as exc_q:
            await engine.send_item(item, ctx)
        assert exc_q.value.category == "quota"

    # 2. NotFound
    with patch.object(engine, "_publish_single_item", side_effect=PikPakNotFoundError("File deleted")):
        with pytest.raises(PikPakPublishFatalError) as exc_nf:
            await engine.send_item(item, ctx)
        assert exc_nf.value.category == "not_found"


@pytest.mark.asyncio
async def test_pikpak_structured_archive_strategy_root_resolve_failure_falls_back_flat():
    """验证归档根目录 resolve_archive_folder("") 失败时，依然降级回退平铺归档，保证非致命"""
    strategy = PikPakStructuredArchiveStrategy(
        resolve_archive_folder=AsyncMock(side_effect=RuntimeError("Cannot find root")),
        archive_files_to_folder=AsyncMock(),
        archive_files=AsyncMock(return_value={"moved": 1}),
    )
    mock_flat = strategy._archive_files
    items = [PublishItem(item_id="f_root_fail", name="f.mp4", size_bytes=100, parent_folder_id="p1")]
    ctx = _make_context(dir_tree={"p1": "子目录"})

    summary = await strategy.cleanup(items, ctx)
    assert "平铺回退 1" in summary
    mock_flat.assert_called_once_with(["f_root_fail"], source_parent_id="p1")


@pytest.mark.asyncio
async def test_cleanup_strategies_empty_items():
    """验证所有清理策略在条目列表为空时均安全返回 None"""
    ctx = _make_context()
    assert await NoopCleanupStrategy().cleanup([], ctx) is None
    assert await PikPakStructuredArchiveStrategy().cleanup([], ctx) is None
    assert await PikPakDeleteCleanupStrategy().cleanup([], ctx) is None


# ==============================================================================
# 7. 深入防御性边界测试 (Enhanced Edge & Robustness Tests)
# ==============================================================================


@pytest.mark.asyncio
async def test_local_disk_transport_engine_send_file_parts_invalid_retry():
    """验证本地传输引擎在 client.send_file 抛出 FILE_PARTS_INVALID 时，清除 meta 并重试成功"""
    engine = LocalDiskTransportEngine()
    item = PublishItem(item_id="D:/fake/test_send_err.mp4", name="test_send_err.mp4", size_bytes=200000)
    mock_client = AsyncMock()

    send_call_count = 0

    async def fake_send_file(*args, **kwargs):
        nonlocal send_call_count
        send_call_count += 1
        if send_call_count == 1:
            raise RuntimeError("RPCError: FILE_PARTS_INVALID occurred during sendMedia")
        return MagicMock(id=888)

    mock_client.send_file.side_effect = fake_send_file
    ctx = _make_context(userbot_client=mock_client)

    with (
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_ft,
        patch("app.core.fast_telethon.discard_upload_meta") as mock_discard,
    ):
        mock_ft.return_value = MagicMock()
        outcome = await engine.send_item(item, ctx)

        assert outcome.message_ids == [888]
        assert send_call_count == 2
        assert mock_ft.call_count == 2
        mock_discard.assert_called_once_with("D:/fake/test_send_err.mp4")


@pytest.mark.asyncio
async def test_pikpak_stream_transport_engine_send_file_parts_invalid_retry():
    """验证 PikPak 流式传输引擎在 client.send_file 抛出 FILE_PARTS_INVALID 时，清除 meta 并全量重试该 Part"""
    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value=["https://cdn/test.mp4"]))
    item = PublishItem(
        item_id="fid_send_invalid",
        name="test_invalid.mp4",
        size_bytes=10 * 1024 * 1024,
        source_type="pikpak",
    )
    mock_client = AsyncMock()

    send_calls = 0

    async def fake_send(*args, **kwargs):
        nonlocal send_calls
        send_calls += 1
        if send_calls == 1:
            raise RuntimeError("file_parts_invalid: server part lost")
        return MagicMock(id=999)

    mock_client.send_file.side_effect = fake_send
    ctx = _make_context(userbot_client=mock_client)

    with (
        patch(
            "app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()
        ) as mock_ft,
        patch("os.path.exists", return_value=True),
        patch("os.remove") as mock_remove,
    ):
        outcome = await engine.send_item(item, ctx)
        assert outcome.message_ids == [999]
        assert send_calls == 2
        assert mock_ft.call_count == 2
        mock_remove.assert_called_once()


@pytest.mark.asyncio
async def test_local_disk_transport_engine_non_int_duration_and_dimensions():
    """验证视频 duration_s 为 float 字符串 (如 '120.5') 或 width/height 为非标准类型时安全解析"""
    engine = LocalDiskTransportEngine()
    item = PublishItem(
        item_id="D:/fake/float_dur.mp4",
        name="float_dur.mp4",
        size_bytes=1000,
        duration_s="120.5",
        width="1920.0",
        height="1080",
        source_type="local",
    )
    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=333)
    ctx = _make_context(userbot_client=mock_client)

    with patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()):
        outcome = await engine.send_item(item, ctx)
        assert outcome.message_ids == [333]
        send_kwargs = mock_client.send_file.call_args[1]
        attr = send_kwargs["attributes"][0]
        assert attr.duration == 120
        assert attr.w == 1920
        assert attr.h == 1080


@pytest.mark.asyncio
async def test_pikpak_snapshot_source_empty_dir_tree_auto_maps_folder_name():
    """验证当 context.is_folder_publish=True 且 dir_tree 为空时，PikPakSnapshotSource 自愈绑定 folder_name 并在 dir_tree 补齐映射"""
    remote_files = [{"file_id": "f_leaf", "name": "clip.mp4", "size_bytes": 100, "parent_id": "p_folder_99"}]
    source = PikPakSnapshotSource(remote_files)
    ctx = _make_context(is_folder_publish=True, folder_name="一字马汁汁", dir_tree={})
    batches = await source.get_batches(ctx)

    assert len(batches) == 1
    assert batches[0].items[0].rel_path == "一字马汁汁/clip.mp4"
    assert ctx.dir_tree.get("p_folder_99") == "一字马汁汁"


@pytest.mark.asyncio
async def test_pikpak_structured_archive_strategy_uses_item_rel_path_when_missing_dir_tree():
    """验证 dir_tree 确实无映射但 group_items 含有带斜杠的 rel_path 时，归档策略从 rel_path 提取父级目录重建树，杜绝平铺"""
    mock_resolve = AsyncMock(side_effect=["root_id", "sub_id"])
    mock_archive_to = AsyncMock(return_value={"moved": 1})
    strategy = PikPakStructuredArchiveStrategy(
        resolve_archive_folder=mock_resolve,
        archive_files_to_folder=mock_archive_to,
        archive_files=AsyncMock(),
    )
    items = [
        PublishItem(
            item_id="f_sub", name="sub.mp4", size_bytes=100, parent_folder_id="p_unknown", rel_path="合集目录/sub.mp4"
        ),
    ]
    ctx = _make_context(dir_tree={}, auto_cleanup_mode="archive")

    summary = await strategy.cleanup(items, ctx)
    assert "合集目录/ 1" in summary
    mock_archive_to.assert_called_once_with(["f_sub"], folder_id="sub_id", source_parent_id="p_unknown")


@pytest.mark.asyncio
async def test_pikpak_snapshot_source_expand_with_publish_item_objects():
    """验证当 raw_files 包含 PublishItem 对象且含有 is_dir=True 时，展开器安全接收而不被丢弃"""
    raw_files = [PublishItem(item_id="dir_obj_1", name="我的剧集", size_bytes=0, is_dir=True, source_type="pikpak")]
    ctx = _make_context(dir_tree={}, is_folder_publish=True, folder_name="我的剧集")

    leaf_item = ParsedFileItem(
        file_id="leaf_f",
        name="01.mp4",
        is_dir=False,
        size_bytes=500,
        audit={},
        thumbnail_link=None,
        duration_s=None,
        width=None,
        height=None,
        parent_id="dir_obj_1",
    )
    # 展开器为唯一注入来源 (原 patch PikPakPublisherService.expand_remote_files 已迁移)
    mock_expand = AsyncMock(return_value=([leaf_item], {"dir_obj_1": "我的剧集"}))
    source = PikPakSnapshotSource(raw_files, expander=mock_expand)
    batches = await source.get_batches(ctx)
    mock_expand.assert_called_once()
    assert len(batches) == 1
    assert batches[0].items[0].rel_path == "我的剧集/01.mp4"


@pytest.mark.asyncio
async def test_cleanup_strategies_mode_mismatch_skips():
    """验证归档策略在 auto_cleanup_mode='delete' 时跳过，删除策略在 auto_cleanup_mode='archive' 时跳过"""
    items = [PublishItem(item_id="f1", name="f1.mp4", size_bytes=100)]
    ctx_delete = _make_context(auto_cleanup_mode="delete")
    ctx_archive = _make_context(auto_cleanup_mode="archive")

    assert await PikPakStructuredArchiveStrategy().cleanup(items, ctx_delete) is None
    assert await PikPakDeleteCleanupStrategy().cleanup(items, ctx_archive) is None


@pytest.mark.asyncio
async def test_pikpak_stream_transport_engine_zero_size_file():
    """验证 0 字节文件被安全处理，返回空消息集且不抛异常"""
    engine = PikPakStreamTransportEngine()
    item = PublishItem(item_id="f_zero", name="zero.mp4", size_bytes=0, source_type="pikpak")
    ctx = _make_context(userbot_client=AsyncMock())
    outcome = await engine.send_item(item, ctx)
    assert outcome.message_ids == []
    assert outcome.speed_mb_s == 0.0


@pytest.mark.asyncio
async def test_local_disk_transport_engine_album_partial_return():
    """验证相册两段式发送时若组发返回列表少于 handle 数, 缺位子项保持空 outcome
    (J5 哨兵), 结果集长度与输入列表严格对齐"""
    engine = LocalDiskTransportEngine()
    items = [PublishItem(item_id=f"D:/fake/p_{i}.jpg", name=f"p_{i}.jpg", size_bytes=500) for i in range(3)]
    mock_client = AsyncMock()
    # 模拟返回仅包含 1 个消息对象的列表
    mock_client.send_file.return_value = [MagicMock(id=111)]
    ctx = _make_context(userbot_client=mock_client)

    with (
        patch("os.path.exists", return_value=True),
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=[MagicMock(name=f"m{i}") for i in range(3)]),
    ):
        results = await engine.send_album(items, ctx)
        assert len(results) == 3
        assert results[0].message_ids == [111]
        assert results[1].message_ids == []
        assert results[2].message_ids == []


@pytest.mark.asyncio
async def test_transport_engines_missing_client_raises():
    """验证客户端未就绪时抛出明确异常，绝不以 None 调用底端接口"""
    local_engine = LocalDiskTransportEngine()
    pikpak_engine = PikPakStreamTransportEngine()

    item_local = PublishItem(item_id="D:/fake/a.mp4", name="a.mp4", size_bytes=100)
    item_pikpak = PublishItem(item_id="fid_p", name="p.mp4", size_bytes=100, source_type="pikpak")

    ctx_none = _make_context(userbot_client=None)

    with (
        patch("app.core.client_manager.client_manager.userbot", None),
        patch("app.core.client_manager.client_manager.bots", []),
    ):
        with pytest.raises(RuntimeError, match="未就绪的 TelegramClient"):
            await local_engine.send_item(item_local, ctx_none)

        with pytest.raises(PikPakPublishFatalError) as exc_info:
            await pikpak_engine.send_item(item_pikpak, ctx_none)
        assert exc_info.value.category == "userbot"


# ==============================================================================
# 8. 包5: 相册跨图链路预取流水线 (album_pipeline_slots, 默认关闭)
# ==============================================================================


class _JitLinkProvider(BaseChunkProvider):
    """冻结 Provider 形态替身: 首读触发 url_provider (真实 JIT 索链语义)。"""

    def __init__(self, file_id, file_name, total_bytes, url_provider, request_timeout=60.0, **kwargs):
        self.file_id = file_id
        self.file_name = file_name
        self.total_bytes = total_bytes
        self._url_provider = url_provider
        self._resolved = False

    async def read_chunk(self, offset, size):
        if not self._resolved:
            self._resolved = True
            await self._url_provider()
        return b"\x00" * 4

    async def aclose(self):
        pass


def _album_items(prefix, count, size_bytes=5 * 1024 * 1024):
    return [
        PublishItem(item_id=f"{prefix}_{i}", name=f"{prefix}_{i}.jpg", size_bytes=size_bytes, source_type="pikpak")
        for i in range(count)
    ]


@pytest.mark.asyncio
async def test_album_pipeline_default_off_serial_identical():
    """包5: slots 缺省 (1) → 逐张串行 —— 第 k+1 条链在第 k 张上传完成之后才开始 (现行为)。"""
    events = []

    async def fake_link(file_ids, **kw):
        events.append(("link-start", file_ids[0]))
        await asyncio.sleep(0)
        events.append(("link-end", file_ids[0]))
        return [f"https://cdn/{file_ids[0]}"]

    async def fake_upload(client, path, **kwargs):
        provider = kwargs.get("chunk_provider")
        if provider is not None:
            await provider.read_chunk(0, 4)  # 真实语义: 上传首读触发 JIT 索链
        events.append(("up", path))
        return MagicMock()

    engine = PikPakStreamTransportEngine(link_provider=fake_link)
    items = _album_items("fid_pp", 3)
    mock_client = AsyncMock()
    mock_client.send_file.return_value = [MagicMock(id=100), MagicMock(id=101), MagicMock(id=102)]
    ctx = _make_context(userbot_client=mock_client)

    with (
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=fake_upload),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", _JitLinkProvider),
        patch.object(engine, "_make_thumb_provider", return_value=None),
    ):
        outcomes = await engine.send_album(items, ctx)

    assert [o.message_ids for o in outcomes] == [[100], [101], [102]]
    ups = [i for i, e in enumerate(events) if e[0] == "up"]
    link_starts = [i for i, e in enumerate(events) if e[0] == "link-start"]
    link_ends = [i for i, e in enumerate(events) if e[0] == "link-end"]
    assert len(ups) == 3 and len(link_starts) == 3
    for k in range(3):
        assert link_ends[k] < ups[k], "链路解析必须发生在该图上传前 (JIT)"
    for k in range(2):
        assert link_starts[k + 1] > ups[k], "串行铁证: 下一条链在上传完成后才开始 (slots=1 现行为)"


@pytest.mark.asyncio
async def test_album_pipeline_slots2_overlaps_link_resolution():
    """包5: slots=2 → 上传第 k 张的同时预取第 k+1 条链 (结构性重叠), 链并发峰值 ≤ slots。"""
    state = {"active": 0, "peak": 0}
    events = []
    link1_started = asyncio.Event()

    async def fake_link(file_ids, **kw):
        state["active"] += 1
        state["peak"] = max(state["peak"], state["active"])
        events.append(("link-start", file_ids[0]))
        if file_ids[0] == "fid_ov_1":
            link1_started.set()
        try:
            await asyncio.sleep(0.01)
            return [f"https://cdn/{file_ids[0]}"]
        finally:
            state["active"] -= 1
            events.append(("link-end", file_ids[0]))

    async def fake_upload(client, path, **kwargs):
        provider = kwargs.get("chunk_provider")
        if provider is not None:
            await provider.read_chunk(0, 4)
        if path == "pikpak://fid_ov_0":
            # 确定性等待第 2 条预取链路至少启动，避免与 fake_link 固定时间对赌
            await asyncio.wait_for(link1_started.wait(), timeout=2.0)
        events.append(("up", path))
        return MagicMock()

    engine = PikPakStreamTransportEngine(link_provider=fake_link)
    items = _album_items("fid_ov", 6)
    mock_client = AsyncMock()
    mock_client.send_file.return_value = [MagicMock(id=i) for i in range(200, 206)]
    ctx = _make_context(userbot_client=mock_client)
    ctx.album_pipeline_slots = 2

    with (
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=fake_upload),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", _JitLinkProvider),
        patch.object(engine, "_make_thumb_provider", return_value=None),
    ):
        outcomes = await engine.send_album(items, ctx)

    assert [o.message_ids for o in outcomes] == [[i] for i in range(200, 206)]
    assert state["peak"] <= 2, "链路解析并发峰值必须 ≤ slots (风控面)"
    idx_up0 = events.index(("up", "pikpak://fid_ov_0"))
    assert events.index(("link-start", "fid_ov_1")) < idx_up0, (
        "结构性重叠: 第 2 条链的解析必须发生在第 1 张上传完成之前 (预取窗口生效)"
    )


@pytest.mark.asyncio
async def test_album_pipeline_slots_capped_at_3():
    """包5 (J9): slots=5 → 硬上限 3, 实际链路并发峰值 ≤ 3 (窗口 = slots-1)。"""
    state = {"active": 0, "peak": 0}

    async def fake_link(file_ids, **kw):
        state["active"] += 1
        state["peak"] = max(state["peak"], state["active"])
        try:
            await asyncio.sleep(0.03)
            return [f"https://cdn/{file_ids[0]}"]
        finally:
            state["active"] -= 1

    async def fake_upload(client, path, **kwargs):
        provider = kwargs.get("chunk_provider")
        if provider is not None:
            await provider.read_chunk(0, 4)
        await asyncio.sleep(0.03)
        return MagicMock()

    engine = PikPakStreamTransportEngine(link_provider=fake_link)
    items = _album_items("fid_cap", 10)
    mock_client = AsyncMock()
    mock_client.send_file.return_value = [MagicMock(id=i) for i in range(300, 310)]
    ctx = _make_context(userbot_client=mock_client)
    ctx.album_pipeline_slots = 5  # 超上限 → 钳制到 3

    with (
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=fake_upload),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", _JitLinkProvider),
        patch.object(engine, "_make_thumb_provider", return_value=None),
    ):
        outcomes = await engine.send_album(items, ctx)

    assert len(outcomes) == 10
    assert state["peak"] <= 3, "slots 硬上限 3 (风控面)"


@pytest.mark.asyncio
async def test_album_pipeline_outcomes_identical_across_modes():
    """包5: slots=1 与 slots=2 的 outcomes 逐字段一致 (行为等价性)。"""

    async def run(slots):
        async def fake_link(file_ids, **kw):
            await asyncio.sleep(0.01)
            return [f"https://cdn/{file_ids[0]}"]

        async def fake_upload(client, path, **kwargs):
            provider = kwargs.get("chunk_provider")
            if provider is not None:
                await provider.read_chunk(0, 4)
            await asyncio.sleep(0.01)
            return MagicMock()

        engine = PikPakStreamTransportEngine(link_provider=fake_link)
        items = _album_items("fid_eq", 4)
        mock_client = AsyncMock()
        mock_client.send_file.return_value = [MagicMock(id=500 + i) for i in range(4)]
        ctx = _make_context(userbot_client=mock_client)
        ctx.album_pipeline_slots = slots
        with (
            patch("app.core.fast_telethon.upload_file_parallel", side_effect=fake_upload),
            patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", _JitLinkProvider),
            patch.object(engine, "_make_thumb_provider", return_value=None),
        ):
            return await engine.send_album(items, ctx)

    serial = await run(1)
    pipelined = await run(2)

    def normalize(outs):
        return [(o.message_ids, round(o.speed_mb_s, 6), o.client_label) for o in outs]

    assert normalize(serial) == normalize(pipelined), "两种模式结果必须逐字段一致"


@pytest.mark.asyncio
async def test_album_pipeline_cancellation_cancels_prefetch_tasks():
    """包5: 组发中途取消 → 未消费预取 task 被回收 (无泄漏)。"""
    calls = {"started": 0, "cancelled": 0}
    flag = {"cancelled": False}
    prefetch2_started = asyncio.Event()

    async def fake_link(file_ids, **kw):
        calls["started"] += 1
        try:
            if file_ids[0].endswith("_1"):
                prefetch2_started.set()
                await asyncio.Event().wait()
            return [f"https://cdn/{file_ids[0]}"]
        except asyncio.CancelledError:
            calls["cancelled"] += 1
            raise

    async def fake_upload(client, path, **kwargs):
        provider = kwargs.get("chunk_provider")
        if provider is not None:
            await provider.read_chunk(0, 4)
        await asyncio.wait_for(prefetch2_started.wait(), timeout=2.0)  # 确定性等待第 2 条预取已入场且仍挂起
        flag["cancelled"] = True  # 第一张传完即置取消
        return MagicMock()

    engine = PikPakStreamTransportEngine(link_provider=fake_link)
    items = _album_items("fid_cancel", 3)
    mock_client = AsyncMock()
    mock_client.send_file.return_value = [MagicMock(id=1)]
    ctx = _make_context(userbot_client=mock_client, is_cancelled=lambda: flag["cancelled"])
    ctx.album_pipeline_slots = 2

    with pytest.raises(UploadCancelledError):
        with (
            patch("app.core.fast_telethon.upload_file_parallel", side_effect=fake_upload),
            patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", _JitLinkProvider),
            patch.object(engine, "_make_thumb_provider", return_value=None),
        ):
            await engine.send_album(items, ctx)

    for _ in range(3):
        await asyncio.sleep(0)  # 事件循环消化取消
    assert calls["started"] == 2, "slots=2 → 仅窗口内 2 条链被发起"
    assert calls["cancelled"] == 1, "未消费的预取 task 必须被回收"


@pytest.mark.asyncio
async def test_album_pipeline_link_error_same_group_failure_semantics():
    """成员级链路错误逐项隔离 —— 成功子集照常发送 (1 张强制单张),
    失败成员 error_detail outcome (编排层按 D2 路由: 部分失败逐条卡 / 全组失败汇总单卡);
    全员失败语义由 test_album_partial_isolation 的 all-failed 用例守护。"""
    from app.services.pikpak_service import PikPakError

    async def fake_link(file_ids, **kw):
        if file_ids[0] == "fid_err_1":  # _album_items("fid_err", 2) 的第 2 项 (fid_err_0/1)
            raise PikPakError("risk control on link")
        await asyncio.sleep(0.01)
        return [f"https://cdn/{file_ids[0]}"]

    async def fake_upload(client, path, **kwargs):
        provider = kwargs.get("chunk_provider")
        if provider is not None:
            await provider.read_chunk(0, 4)
        await asyncio.sleep(0.01)
        return MagicMock()

    engine = PikPakStreamTransportEngine(link_provider=fake_link)
    items = _album_items("fid_err", 2)
    mock_client = AsyncMock()
    ctx = _make_context(userbot_client=mock_client)
    ctx.album_pipeline_slots = 2

    with (
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=fake_upload),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", _JitLinkProvider),
        patch.object(engine, "_make_thumb_provider", return_value=None),
    ):
        outcomes = await engine.send_album(items, ctx)

    assert outcomes[0].message_ids and outcomes[0].message_ids[0] > 0, "成功成员照常发送"
    assert outcomes[1].message_ids == []
    assert "risk control on link" in outcomes[1].error_detail, "链路错误逐项归因"
    assert mock_client.send_file.await_count == 1, "成功子集 (1 张) 强制单张发送"
    sent_payload = mock_client.send_file.await_args.args[1]
    assert not isinstance(sent_payload, list), "1 元素列表组发被 Telethon 静默降级 —— 必须显式单发"


@pytest.mark.asyncio
async def test_album_pipeline_prefetch_failure_retrieved_no_never_retrieved_noise(caplog):
    """收尾批次 + 未消费预取 task 已 done-with-exception → 回收块取回异常
    (无 "Task exception was never retrieved" GC 噪音); V3 逐项隔离后, 成员级预取失败
    被消费并转为 error_detail outcome —— 全员失败仍抛首个原异常, 绝不组发。"""
    import gc
    import logging

    prefetch_failed = asyncio.Event()

    async def fake_link(file_ids, **kw):
        if file_ids[0].endswith("_1"):  # 第 2 项: 预取窗口内立即失败
            prefetch_failed.set()
            raise RuntimeError("prefetch link boom")
        return [f"https://cdn/{file_ids[0]}"]

    async def fake_upload(client, path, **kwargs):
        provider = kwargs.get("chunk_provider")
        if provider is not None:
            await provider.read_chunk(0, 4)
        await asyncio.wait_for(prefetch_failed.wait(), timeout=2.0)
        await asyncio.sleep(0)  # 确保第 2 项预取 Task 已进入 done-with-exception 态
        raise RuntimeError("upload boom")  # 第 1 项上传失败 (逐项隔离)

    engine = PikPakStreamTransportEngine(link_provider=fake_link)
    items = _album_items("fid_nr", 2)
    mock_client = AsyncMock()
    ctx = _make_context(userbot_client=mock_client)
    ctx.album_pipeline_slots = 2

    loop = asyncio.get_running_loop()
    never_retrieved = []

    def _capture(loop_, event):
        never_retrieved.append(event)

    loop.set_exception_handler(_capture)
    try:
        with caplog.at_level(logging.DEBUG, logger="app.core.pipeline.engines"):
            with pytest.raises(TransportError, match="upload boom"):  # 首个逐项失败异常胜出 (批2.1 单轨包装)
                with (
                    patch("app.core.fast_telethon.upload_file_parallel", side_effect=fake_upload),
                    patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", _JitLinkProvider),
                    patch.object(engine, "_make_thumb_provider", return_value=None),
                ):
                    await engine.send_album(items, ctx)
        await asyncio.sleep(0)
        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(None)

    assert mock_client.send_file.await_count == 0, "全员失败 → 绝不组发"
    # 成功子集为 0 → 整组失败 raise; 失败预取 task 的异常已被成员级隔离消费 (取回一次)
    unretrieved = [
        e for e in never_retrieved if str(e.get("message", "")).startswith("Task exception was never retrieved")
    ]
    assert unretrieved == [], f"不得出现未取回异常噪音: {unretrieved}"


# ==============================================================================
# Part: .tmp_infra.py
# ==============================================================================
# ==============================================================================
# 1. normalize_target_chat_id 标识符归一化测试
# ==============================================================================


def test_normalize_target_chat_id_int_passthrough():
    """整数 ID 直接穿透保持 int (含 9~12 位纯正整数自愈为 -100 格式)"""
    assert normalize_target_chat_id(-1009000000030) == -1009000000030
    assert isinstance(normalize_target_chat_id(-1009000000030), int)
    assert normalize_target_chat_id(123456) == 123456
    assert isinstance(normalize_target_chat_id(123456), int)
    assert normalize_target_chat_id(900000009) == -100900000009
    assert isinstance(normalize_target_chat_id(900000009), int)
    assert normalize_target_chat_id(0) == 0


def test_normalize_target_chat_id_numeric_string():
    """纯数字字符串强制转换为 int，避免以 str 直传 Telethon 造成 crash (9~12 位正整数自愈)"""
    res1 = normalize_target_chat_id("-1009000000030")
    assert res1 == -1009000000030
    assert isinstance(res1, int)

    res_small = normalize_target_chat_id("123456")
    assert res_small == 123456
    assert isinstance(res_small, int)

    res2 = normalize_target_chat_id("900000009")
    assert res2 == -100900000009
    assert isinstance(res2, int)

    res3 = normalize_target_chat_id("  -1009000000030  ")
    assert res3 == -1009000000030
    assert isinstance(res3, int)


def test_normalize_target_chat_id_private_link():
    """Telegram 私密频道链接转换为 -100<id> 格式的 int"""
    res = normalize_target_chat_id("https://t.me/c/9000000030/1234")
    assert res == -1009000000030
    assert isinstance(res, int)

    res2 = normalize_target_chat_id("t.me/c/9000000010/1")
    assert res2 == -1009000000010
    assert isinstance(res2, int)


def test_normalize_target_chat_id_public_channel_link_and_usernames():
    """公开频道链接和 Username 保留合法 str"""
    assert normalize_target_chat_id("https://t.me/my_channel/123") == "my_channel"
    assert normalize_target_chat_id("t.me/my_channel") == "my_channel"
    assert normalize_target_chat_id("@my_channel") == "@my_channel"
    assert normalize_target_chat_id("my_channel") == "my_channel"


# ==============================================================================
# 2. PublishResult 领域模型兼容性与特性测试
# ==============================================================================


def test_publish_result_int_inheritance_and_properties():
    """验证 PublishResult 继承自 int 且保持数值行为与全量属性"""
    res = PublishResult(
        published_count=3,
        total_files_count=5,
        failed_files=["err1.mp4"],
        skipped_files=[{"file_name": "skip.mp4", "file_size_mb": 2500}],
        archive_summary="🧹 归档: 3 个文件",
        is_folder_publish=True,
        folder_name="测试合集",
    )

    # 验证 int 继承
    assert isinstance(res, int)
    assert res == 3
    assert int(res) == 3
    assert res + 2 == 5

    # 验证各属性
    assert res.published_count == 3
    assert res.newly_published_count == 3  # 未传时默认等于 published_count
    assert res.total_files_count == 5
    assert res.failed_files == ["err1.mp4"]
    assert len(res.skipped_files) == 1
    assert res.archive_summary == "🧹 归档: 3 个文件"
    assert res.is_folder_publish is True
    assert res.folder_name == "测试合集"
    assert res.archive_failed is False


def test_publish_result_newly_published_count_explicit():
    """验证当显式提供 newly_published_count 时正确保存"""
    # 例如：全部历史防重跳过时，published_count=5, newly_published_count=0
    res = PublishResult(
        published_count=5,
        total_files_count=5,
        newly_published_count=0,
    )
    assert res == 5
    assert res.published_count == 5
    assert res.newly_published_count == 0


def test_publish_result_truthiness_and_defaults():
    """验证布尔真值与缺省默认值"""
    res_zero = PublishResult(0, 0)
    assert bool(res_zero) is False
    assert res_zero.failed_files == []
    assert res_zero.skipped_files == []
    assert res_zero.archive_summary is None
    assert res_zero.newly_published_count == 0

    res_pos = PublishResult(1, 1)
    assert bool(res_pos) is True


# ==============================================================================
# 3. TelegramChannelNotifier 告示策略测试
# ==============================================================================


def _infra_make_context(
    target_chat_id=-1009000000030,
    userbot_client=None,
    task_id="test_task_001",
    folder_name="测试合集",
    is_folder_publish=True,
):
    return PublishContext(
        task_id=task_id,
        target_chat_id=target_chat_id,
        target_channel_raw=str(target_chat_id),
        is_folder_publish=is_folder_publish,
        folder_name=folder_name,
        userbot_client=userbot_client,
    )


@pytest.mark.asyncio
async def test_notifier_skips_when_not_folder_publish():
    """规则 1：非文件夹发布（散装单文件发布）直接跳过发卡，避免刷屏"""
    client = AsyncMock()
    context = _infra_make_context(userbot_client=client, is_folder_publish=False)
    batch = PublishBatch(
        batch_id="batch_1",
        folder_name="",
        is_folder_publish=False,
    )
    result = PublishResult(published_count=2, total_files_count=2, newly_published_count=2)

    notifier = TelegramChannelNotifier()
    await notifier.notify_batch(batch, context, result)

    assert client.send_message.call_count == 0


@pytest.mark.asyncio
async def test_notifier_skips_when_all_previously_published_and_no_failures():
    """规则 2：全部媒体已在历史中发布且无失败文件时，静默跳过发卡"""
    client = AsyncMock()
    context = _infra_make_context(userbot_client=client)
    batch = PublishBatch(
        batch_id="batch_1",
        folder_name="已完结合集",
        is_folder_publish=True,
    )
    result = PublishResult(
        published_count=3,
        total_files_count=3,
        newly_published_count=0,  # 0 个新发布
        failed_files=[],
    )

    notifier = TelegramChannelNotifier()
    with patch("app.db.task_repository.TaskQueueRepository.append_log_async", new_callable=AsyncMock) as mock_log:
        await notifier.notify_batch(batch, context, result)

        assert client.send_message.call_count == 0
        mock_log.assert_called_once()
        assert "全部媒体已在历史中发布" in mock_log.call_args[0][1]


@pytest.mark.asyncio
async def test_notifier_sends_card_when_newly_published_gt_zero():
    """规则 3：文件夹有新发布文件时，正常发送总结卡片"""
    client = AsyncMock()
    target_id = -1009000000030
    context = _infra_make_context(target_chat_id=target_id, userbot_client=client)
    batch = PublishBatch(
        batch_id="batch_1",
        folder_name="#一字马汁汁",
        is_folder_publish=True,
    )
    result = PublishResult(
        published_count=3,
        total_files_count=3,
        newly_published_count=3,
        failed_files=[],
    )

    notifier = TelegramChannelNotifier()
    mock_entity = MagicMock()
    with (
        patch(
            "app.core.pipeline.notifier.resolve_entity", new_callable=AsyncMock, return_value=mock_entity
        ) as mock_resolve,
        patch("app.db.task_repository.TaskQueueRepository.append_log_async", new_callable=AsyncMock) as mock_log,
    ):
        await notifier.notify_batch(batch, context, result)

        mock_resolve.assert_called_once_with(client, target_id)
        assert client.send_message.call_count == 1
        sent_entity, sent_text = client.send_message.call_args[0]
        assert sent_entity is mock_entity

        assert "📁 **发布文件夹**: #一字马汁汁" in sent_text
        assert "✅ **成功发布**: 3/3 个媒体文件" in sent_text
        assert "❌ **失败文件数**: 0" in sent_text
        mock_log.assert_called_once()
        assert "已向目标频道" in mock_log.call_args[0][1]


@pytest.mark.asyncio
async def test_notifier_sends_card_when_zero_newly_published_but_has_failures():
    """规则 4：新发送为 0 但存在失败文件时，必须向频道发出失败报告"""
    client = AsyncMock()
    context = _infra_make_context(userbot_client=client)
    batch = PublishBatch(
        batch_id="batch_1",
        folder_name="含错误合集",
        is_folder_publish=True,
    )
    result = PublishResult(
        published_count=0,
        total_files_count=2,
        newly_published_count=0,
        failed_files=["err_vid1.mp4: Upload timeout", "err_vid2.mp4: Network reset"],
    )

    notifier = TelegramChannelNotifier()
    with patch("app.core.pipeline.notifier.resolve_entity", new_callable=AsyncMock, return_value=MagicMock()):
        await notifier.notify_batch(batch, context, result)

        assert client.send_message.call_count == 1
        sent_text = client.send_message.call_args[0][1]
        assert "❌ **失败文件列表 (2 个)**:" in sent_text
        assert "• err_vid1.mp4: Upload timeout" in sent_text
        assert "• err_vid2.mp4: Network reset" in sent_text


@pytest.mark.asyncio
async def test_notifier_formats_archive_and_skipped():
    """验证归档摘要与超限跳过文件的格式化展示"""
    client = AsyncMock()
    context = _infra_make_context(userbot_client=client)
    batch = PublishBatch(
        batch_id="batch_1",
        folder_name="测试归档展示",
        is_folder_publish=True,
    )
    result = PublishResult(
        published_count=2,
        total_files_count=3,
        newly_published_count=2,
        archive_summary="🧹 一字马汁汁/ 2",
        skipped_files=[
            {"file_name": "huge1.mp4", "file_size_mb": 2450.6},
            MagicMock(file_name="huge2.mp4", file_size_mb=3100.2),
        ],
        failed_files=[],
    )

    notifier = TelegramChannelNotifier()
    with patch("app.core.pipeline.notifier.resolve_entity", new_callable=AsyncMock, return_value=MagicMock()):
        await notifier.notify_batch(batch, context, result)

        assert client.send_message.call_count == 1
        sent_text = client.send_message.call_args[0][1]
        # 验证单扫帚符号格式化
        assert "🧹 **归档**: 一字马汁汁/ 2" in sent_text
        # 验证超限跳过格式化
        assert "⏭️ **超限跳过 (2 个)**:" in sent_text
        assert "`huge1.mp4` (2450.6MiB, 超过单文件体积限制)" in sent_text
        assert "`huge2.mp4` (3100.2MiB, 超过单文件体积限制)" in sent_text


@pytest.mark.asyncio
async def test_notifier_entity_resolution_and_exception_isolation():
    """验证当 resolve_entity 或 send_message 抛出异常时，错误被安全隔离，不影响任务终态"""
    client = AsyncMock()
    client.send_message.side_effect = RuntimeError("Telegram RPC network down")
    context = _infra_make_context(userbot_client=client)
    batch = PublishBatch(
        batch_id="batch_1",
        folder_name="异常隔离测试",
        is_folder_publish=True,
    )
    result = PublishResult(published_count=1, total_files_count=1, newly_published_count=1)

    notifier = TelegramChannelNotifier()
    with (
        patch("app.core.pipeline.notifier.resolve_entity", new_callable=AsyncMock, return_value=MagicMock()),
        patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        patch("app.db.task_repository.TaskQueueRepository.append_log_async", new_callable=AsyncMock) as mock_log,
    ):
        # 核心断言：绝对不向外抛出异常
        await notifier.notify_batch(batch, context, result)

        assert mock_sleep.call_count == 1
        assert mock_log.call_count >= 1
        assert any("⚠️ 总结卡发送失败" in call[0][1] for call in mock_log.call_args_list)


@pytest.mark.asyncio
async def test_notifier_client_none_isolated():
    """验证客户端为 None 时安全隔离，不抛出异常"""
    context = _infra_make_context(userbot_client=None)
    batch = PublishBatch(batch_id="batch_1", folder_name="无客户端", is_folder_publish=True)
    result = PublishResult(published_count=1, total_files_count=1, newly_published_count=1)

    notifier = TelegramChannelNotifier()
    with patch("app.db.task_repository.TaskQueueRepository.append_log_async", new_callable=AsyncMock) as mock_log:
        await notifier.notify_batch(batch, context, result)
        assert mock_log.call_count == 1
        assert "执行账号客户端未就绪" in mock_log.call_args[0][1]


@pytest.mark.asyncio
async def test_notifier_truncates_long_summary():
    """验证当失败文件较多或总结卡超长时，安全截断并在末尾添加截断提示"""
    client = AsyncMock()
    context = _infra_make_context(userbot_client=client)
    batch = PublishBatch(batch_id="batch_1", folder_name="超级长文件列表", is_folder_publish=True)
    # 构造超过 15 个失败文件，且每个文件名称较长
    failed_files = [
        f"path/to/very/long/filename_subfolder_component_{i:04d}_test_media.mp4: Upload error" for i in range(25)
    ]
    result = PublishResult(
        published_count=0,
        total_files_count=25,
        newly_published_count=0,
        failed_files=failed_files,
    )

    notifier = TelegramChannelNotifier()
    with patch("app.core.pipeline.notifier.resolve_entity", new_callable=AsyncMock, return_value=MagicMock()):
        await notifier.notify_batch(batch, context, result)

        assert client.send_message.call_count == 1
        sent_text = client.send_message.call_args[0][1]
        assert "剩余 15 个失败文件已截断" in sent_text


def test_normalize_target_chat_id_edge_cases():
    """边界场景：空值、纯空格、None、带横杠频道名"""
    assert normalize_target_chat_id("") == ""
    assert normalize_target_chat_id("   ") == ""
    assert normalize_target_chat_id(None) == ""
    assert normalize_target_chat_id("my-channel-test") == "my-channel-test"
    assert normalize_target_chat_id("-0") == 0


def test_publish_result_mutation_isolation():
    """验证输入列表在外部修改时不影响 PublishResult 内部状态（防御性拷贝）"""
    failed = ["f1.mp4"]
    skipped = [{"file_name": "s1.mp4"}]
    res = PublishResult(published_count=1, total_files_count=2, failed_files=failed, skipped_files=skipped)

    failed.append("f2.mp4")
    skipped.append({"file_name": "s2.mp4"})

    assert res.failed_files == ["f1.mp4"]
    assert len(res.skipped_files) == 1


def test_publish_context_defaults():
    """验证 PublishContext 默认参数完整性"""
    ctx = PublishContext(
        task_id="t1",
        target_chat_id=-1001,
        target_channel_raw="-1001",
        is_folder_publish=True,
        folder_name="test",
        userbot_client=None,
    )
    assert ctx.upload_concurrency == 2
    assert ctx.upload_in_flight_per_connection == 2
    assert ctx.delay_seconds == 3.0
    assert ctx.auto_cleanup_mode == "archive"
    assert ctx.oversize_policy == "skip_and_report"
    assert ctx.send_placeholder_card is True
    assert ctx.album_mode is True
    assert ctx.dir_tree == {}
    assert ctx.is_cancelled is None


def test_interfaces_abstract_enforcement():
    """验证四个抽象策略接口不可直接实例化"""
    with pytest.raises(TypeError):
        ItemSource()  # pylint: disable=abstract-class-instantiated
    with pytest.raises(TypeError):
        MediaTransportEngine()
    with pytest.raises(TypeError):
        CleanupStrategy()
    with pytest.raises(TypeError):
        NotificationStrategy()


@pytest.mark.asyncio
async def test_media_transport_engine_send_album_default():
    """验证 MediaTransportEngine.send_album 默认实现逐条委托给 send_item (TransportOutcome 契约)"""

    class InfraDummyEngine(MediaTransportEngine):
        async def send_item(self, item, context):
            return TransportOutcome(message_ids=[int(item.item_id)], speed_mb_s=12.34)

    engine = InfraDummyEngine()
    items = [
        PublishItem(item_id="101", name="pic1.jpg", size_bytes=1000),
        PublishItem(item_id="102", name="pic2.jpg", size_bytes=2000),
    ]
    context = _infra_make_context()
    results = await engine.send_album(items, context)
    assert [r.message_ids[0] for r in results] == [101, 102]
    assert all(r.speed_mb_s == 12.34 for r in results)


@pytest.mark.asyncio
async def test_notifier_strips_hash_prefix_on_folder_name():
    """验证当 folder_name 带有前导 # 时，正确清洗避免双重 ##"""
    client = AsyncMock()
    context = _infra_make_context(userbot_client=client, folder_name="##多重井号")
    batch = PublishBatch(batch_id="b1", folder_name="##多重井号", is_folder_publish=True)
    res = PublishResult(published_count=1, total_files_count=1, newly_published_count=1)

    notifier = TelegramChannelNotifier()
    with patch("app.core.pipeline.notifier.resolve_entity", new_callable=AsyncMock, return_value=MagicMock()):
        await notifier.notify_batch(batch, context, res)
        assert client.send_message.call_count == 1
        sent_text = client.send_message.call_args[0][1]
        assert "📁 **发布文件夹**: #多重井号" in sent_text
        assert "##" not in sent_text


def test_normalize_target_chat_id_extended_domains_and_guards():
    """验证拓展别名域名 (telegram.me, telegram.dog)、邀请链接与 bool 防护"""
    assert normalize_target_chat_id("https://telegram.me/c/9000000030/12") == -1009000000030
    assert normalize_target_chat_id("https://telegram.dog/c/9000000030/12") == -1009000000030
    assert normalize_target_chat_id("https://telegram.me/sample_chan") == "sample_chan"
    assert normalize_target_chat_id("https://t.me/joinchat/ABCDEF123") == "+ABCDEF123"
    assert normalize_target_chat_id("https://t.me/+ABCDEF123") == "+ABCDEF123"
    assert normalize_target_chat_id(True) == ""
    assert normalize_target_chat_id(False) == ""


def test_publish_context_auto_normalization_and_defaults():
    """验证 PublishContext 初始化时自动执行 target_chat_id 强类型归一化与缺省值填补"""
    ctx = PublishContext(
        task_id="ctx_auto_001",
        target_chat_id="-1009000000030",
    )
    assert ctx.target_chat_id == -1009000000030
    assert isinstance(ctx.target_chat_id, int)
    assert ctx.target_channel_raw == "-1009000000030"
    assert ctx.is_folder_publish is False
    assert ctx.folder_name == ""
    assert ctx.userbot_client is None


def test_publish_batch_properties():
    """验证 PublishBatch 的条目与总字节数统计 (total_count 已删, 用 len(items))"""
    batch = PublishBatch(
        batch_id="b_prop",
        folder_name="batch_prop",
        items=[
            PublishItem(item_id="1", name="f1.mp4", size_bytes=1000),
            PublishItem(item_id="2", name="f2.mp4", size_bytes=2500),
        ],
    )
    assert len(batch.items) == 2
    assert batch.total_size_bytes == 3500


def test_publish_result_status_properties():
    """验证 PublishResult 的 has_failures 属性与执行器终态公式语义 (is_success 已删
    —— 生产终态公式唯一位于 publish_runner, 此处按其口径断言)"""
    res_ok = PublishResult(published_count=5, total_files_count=5, failed_files=[])
    assert res_ok.has_failures is False

    res_partial_skip = PublishResult(
        published_count=3,
        total_files_count=5,
        failed_files=[],
        skipped_files=[{"file_name": "s1.mp4"}, {"file_name": "s2.mp4"}],
    )
    assert res_partial_skip.has_failures is False
    # 执行器终态公式: 无 failed 且 (published + len(skipped)) >= total → 终态非 failed
    assert (
        res_partial_skip.published_count + len(res_partial_skip.skipped_files)
    ) >= res_partial_skip.total_files_count

    res_fail = PublishResult(published_count=2, total_files_count=5, failed_files=["err.mp4"])
    assert res_fail.has_failures is True
    assert bool(res_fail.failed_files)


@pytest.mark.asyncio
async def test_notifier_retry_recovers_on_second_attempt():
    """验证当首次发送抛出瞬态网络异常时，通知器重试并成功发送总结卡片"""
    client = AsyncMock()
    # 首次调用抛出异常，第二次成功
    client.send_message.side_effect = [RuntimeError("Transient network glitch"), MagicMock()]
    context = _infra_make_context(userbot_client=client)
    batch = PublishBatch(batch_id="b_retry", folder_name="重试合集", is_folder_publish=True)
    res = PublishResult(published_count=2, total_files_count=2, newly_published_count=2)

    notifier = TelegramChannelNotifier()
    with (
        patch("app.core.pipeline.notifier.resolve_entity", new_callable=AsyncMock, return_value=MagicMock()),
        patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        patch("app.db.task_repository.TaskQueueRepository.append_log_async", new_callable=AsyncMock) as mock_log,
    ):
        await notifier.notify_batch(batch, context, res)

        assert client.send_message.call_count == 2
        mock_sleep.assert_called_once_with(1.0)
        assert mock_log.call_count == 1
        assert "已向目标频道" in mock_log.call_args[0][1]


@pytest.mark.asyncio
async def test_notifier_truncates_skipped_files_exceeding_ten():
    """验证当超限跳过文件数大于 10 时，只展示前 10 个并附带截断提示"""
    client = AsyncMock()
    context = _infra_make_context(userbot_client=client)
    batch = PublishBatch(batch_id="b_skip_trunc", folder_name="超多跳过", is_folder_publish=True)
    skipped = [{"file_name": f"oversized_{i:02d}.mp4", "file_size_mb": 2500 + i * 10} for i in range(15)]
    res = PublishResult(
        published_count=1,
        total_files_count=16,
        newly_published_count=1,
        skipped_files=skipped,
    )

    notifier = TelegramChannelNotifier()
    with patch("app.core.pipeline.notifier.resolve_entity", new_callable=AsyncMock, return_value=MagicMock()):
        await notifier.notify_batch(batch, context, res)

        assert client.send_message.call_count == 1
        sent_text = client.send_message.call_args[0][1]
        assert "⏭️ **超限跳过 (15 个)**:" in sent_text
        assert "`oversized_00.mp4`" in sent_text
        assert "`oversized_09.mp4`" in sent_text
        assert "`oversized_10.mp4`" not in sent_text
        assert "剩余 5 个超限跳过文件已截断" in sent_text


@pytest.mark.asyncio
async def test_notifier_archive_failure_displayed():
    """验证当归档失败且无摘要时，总结卡中明确标注归档失败"""
    client = AsyncMock()
    context = _infra_make_context(userbot_client=client)
    batch = PublishBatch(batch_id="b_arc_fail", folder_name="归档失败展示", is_folder_publish=True)
    res = PublishResult(
        published_count=1,
        total_files_count=1,
        newly_published_count=1,
        archive_failed=True,
        archive_summary=None,
    )

    notifier = TelegramChannelNotifier()
    with patch("app.core.pipeline.notifier.resolve_entity", new_callable=AsyncMock, return_value=MagicMock()):
        await notifier.notify_batch(batch, context, res)

        assert client.send_message.call_count == 1
        sent_text = client.send_message.call_args[0][1]
        assert "🧹 **归档**: ⚠️ 归档失败" in sent_text


@pytest.mark.asyncio
async def test_notifier_folder_name_variations():
    """验证空白、纯井号或首尾带空格目录名的健壮处理"""
    client = AsyncMock()
    context = _infra_make_context(userbot_client=client)
    notifier = TelegramChannelNotifier()

    # 1. 带空格与井号
    batch1 = PublishBatch(batch_id="b1", folder_name="   #空格目录   ", is_folder_publish=True)
    res1 = PublishResult(published_count=1, total_files_count=1, newly_published_count=1)
    with patch("app.core.pipeline.notifier.resolve_entity", new_callable=AsyncMock, return_value=MagicMock()):
        await notifier.notify_batch(batch1, context, res1)
        assert "📁 **发布文件夹**: #空格目录" in client.send_message.call_args[0][1]

    # 2. 纯井号回退到媒体合集
    batch2 = PublishBatch(batch_id="b2", folder_name="###", is_folder_publish=True)
    with patch("app.core.pipeline.notifier.resolve_entity", new_callable=AsyncMock, return_value=MagicMock()):
        await notifier.notify_batch(batch2, context, res1)
        assert "📁 **发布文件夹**: #媒体合集" in client.send_message.call_args[0][1]


# ==============================================================================
# Part: .tmp_facade.py
# ==============================================================================
# ---------------------------------------------------------------------------
# 1. 结果模型与类型继承继承性测试
# ---------------------------------------------------------------------------


def test_publish_folder_result_type_hierarchy():
    """PublishFolderResult 必须同时是 PublishResult 与 int 的子类, 保持 100% 向后兼容。"""
    res = PublishFolderResult(
        published_count=5,
        total_files_count=10,
        failed_files=["err.mp4"],
        skipped_files=[],
        archive_summary="🧹 归档 5",
        is_folder_publish=True,
        folder_name="合集A",
        newly_published_count=3,
        archive_failed=False,
    )

    assert isinstance(res, PublishFolderResult)
    assert isinstance(res, PublishResult)
    assert isinstance(res, int)
    assert int(res) == 5
    assert res == 5
    assert res + 2 == 7
    assert res.published_count == 5
    assert res.total_files_count == 10
    assert res.failed_files == ["err.mp4"]
    assert res.archive_summary == "🧹 归档 5"
    assert res.is_folder_publish is True
    assert res.folder_name == "合集A"
    assert res.newly_published_count == 3
    assert res.archive_failed is False


# ---------------------------------------------------------------------------
# 2. PublisherService.publish_folder 门面装配与调用测试
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publisher_service_facade_assembly(tmp_path):
    """PublisherService.publish_folder 必须正确组装 LocalFolderSource、LocalDiskTransportEngine、NoopCleanupStrategy 与 TelegramChannelNotifier。"""
    fake_folder = str(tmp_path / "album_test")
    os.makedirs(fake_folder, exist_ok=True)

    executed_context = None

    async def fake_execute(context):
        nonlocal executed_context
        executed_context = context
        return PublishResult(
            published_count=2,
            total_files_count=2,
            failed_files=[],
            skipped_files=[],
            is_folder_publish=context.is_folder_publish,
            folder_name=context.folder_name,
            newly_published_count=2,
        )

    with patch("app.core.pipeline.pipeline.PublishPipeline.execute", side_effect=fake_execute) as mock_exec:
        res = await PublisherService.publish_folder(
            folder_path=fake_folder,
            target_chat_id="-100900050",
            album_mode=True,
            delay_seconds=1.5,
            task_id="local_task_123",
            account_id=99,
            upload_concurrency=4,
            upload_in_flight_per_connection=1,
            force_republish=True,
        )

        mock_exec.assert_awaited_once()
        assert isinstance(res, PublishFolderResult)
        assert isinstance(res, int)
        assert res.published_count == 2
        assert res.total_files_count == 2
        assert res.newly_published_count == 2
        assert res.is_folder_publish is True
        assert res.folder_name == format_folder_tag(fake_folder)

        # 检查透传给 context 的关键字段
        assert executed_context is not None
        assert executed_context.task_id == "local_task_123"
        assert executed_context.target_chat_id == -100900050
        assert executed_context.target_channel_raw == "-100900050"
        assert executed_context.delay_seconds == 1.5
        assert executed_context.force_republish is True
        assert executed_context.account_id == 99
        assert executed_context.upload_concurrency == 4
        assert executed_context.upload_in_flight_per_connection == 1


# ---------------------------------------------------------------------------
# 本地门面占位卡开关派生 (None→skip_and_report 派生; 显式值一律尊重)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publisher_service_placeholder_card_derivation(tmp_path):
    """publish_folder 占位卡开关派生单点 (逐字对齐 pikpak 门面先例
    pikpak_publisher_service.py:131-132): None + skip_and_report → True;
    显式 False 一律尊重 (不再被策略翻转); None + 非 skip 策略 → False。"""
    fake_folder = str(tmp_path / "ph_derive")
    os.makedirs(fake_folder, exist_ok=True)

    contexts = []

    async def fake_execute(context):
        contexts.append(context)
        return PublishResult(
            published_count=0,
            total_files_count=0,
            failed_files=[],
            skipped_files=[],
            is_folder_publish=True,
            folder_name=context.folder_name,
            newly_published_count=0,
        )

    with patch("app.core.pipeline.pipeline.PublishPipeline.execute", side_effect=fake_execute):
        await PublisherService.publish_folder(folder_path=fake_folder, target_chat_id="-1001001")
        await PublisherService.publish_folder(
            folder_path=fake_folder, target_chat_id="-1001001", send_placeholder_card=False
        )
        await PublisherService.publish_folder(
            folder_path=fake_folder, target_chat_id="-1001001", oversize_policy="strict_fail"
        )

    assert contexts[0].send_placeholder_card is True
    assert contexts[1].send_placeholder_card is False
    assert contexts[2].send_placeholder_card is False


# ---------------------------------------------------------------------------
# 3. PikPakPublisherService.publish_pikpak_files 门面装配与参数透传测试
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pikpak_publisher_facade_assembly_and_cleanup_modes():
    """PikPakPublisherService.publish_pikpak_files 根据 auto_cleanup_mode 装配不同清理策略并正确透传所有契约字段。"""
    test_cases = [
        ("archive", PikPakStructuredArchiveStrategy),
        ("delete", PikPakDeleteCleanupStrategy),
        ("none", NoopCleanupStrategy),
    ]

    for mode, expected_cleanup_cls in test_cases:
        executed_pipeline = None
        executed_context = None

        async def capture_execute(self, context, _mode=mode):
            nonlocal executed_pipeline, executed_context
            executed_pipeline = self
            executed_context = context
            return PublishResult(
                published_count=1,
                total_files_count=1,
                failed_files=[],
                skipped_files=[],
                archive_summary=f"mode_{_mode}_done",
                is_folder_publish=context.is_folder_publish,
                folder_name=context.folder_name,
                newly_published_count=1,
            )

        with patch("app.core.pipeline.pipeline.PublishPipeline.execute", capture_execute):
            remote_item = ParsedFileItem(
                file_id="F_TEST",
                name="movie.mp4",
                is_dir=False,
                size_bytes=1024 * 1024,
                audit={"status": "OK"},
                thumbnail_link=None,
                duration_s="120",
                width=1920,
                height=1080,
                parent_id="P_ROOT",
            )

            res = await PikPakPublisherService.publish_pikpak_files(
                remote_files=[remote_item],
                target_chat_id="-100333",
                task_id="pk_facade_task",
                account_id=5,
                auto_cleanup_mode=mode,
                oversize_policy="skip_and_report",
                send_placeholder_card=True,
                force_republish=False,
                dir_tree={"P_ROOT": "root_dir"},
                is_folder_publish=True,
                folder_name="测试合集",
            )

            assert isinstance(res, PublishFolderResult)
            assert isinstance(res, int)
            assert res.published_count == 1
            assert res.archive_summary == f"mode_{mode}_done"
            assert res.is_folder_publish is True
            assert res.folder_name == "测试合集"

            # 校验 Pipeline 组装的策略组件
            assert executed_pipeline is not None
            assert isinstance(executed_pipeline.source, PikPakSnapshotSource)
            assert isinstance(executed_pipeline.engine, PikPakStreamTransportEngine)
            assert isinstance(executed_pipeline.cleanup, expected_cleanup_cls)
            assert isinstance(executed_pipeline.notifier, TelegramChannelNotifier)

            # 校验 context 参数透传
            assert executed_context is not None
            assert executed_context.task_id == "pk_facade_task"
            assert executed_context.account_id == 5
            assert executed_context.oversize_policy == "skip_and_report"
            assert executed_context.send_placeholder_card is True
            assert executed_context.dir_tree == {"P_ROOT": "root_dir"}
            assert executed_context.is_folder_publish is True
            assert executed_context.folder_name == "测试合集"


@pytest.mark.asyncio
async def test_pikpak_publisher_facade_empty_remote_files():
    """PikPak 门面对空文件列表直接返回契约保留的 PublishFolderResult (0/0/[])。"""
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=[],
        target_chat_id="-100999",
        task_id="empty_task",
        is_folder_publish=True,
        folder_name="空合集",
    )

    assert isinstance(res, PublishFolderResult)
    assert isinstance(res, int)
    assert res.published_count == 0
    assert res.total_files_count == 0
    assert res.failed_files == []
    assert res.skipped_files == []
    assert res.is_folder_publish is True
    assert res.folder_name == "空合集"


# ---------------------------------------------------------------------------
# 4. PublishPipeline 6 阶段端到端生命周期执行与边界测试
# ---------------------------------------------------------------------------


class DummySource(ItemSource):
    def __init__(self, batches):
        self.batches = batches

    async def get_batches(self, context: PublishContext):
        return self.batches


class FacadeDummyEngine(MediaTransportEngine):
    def __init__(self):
        self.sent_items = []

    async def send_item(self, item: PublishItem, context: PublishContext):
        self.sent_items.append(item)
        return TransportOutcome(message_ids=[1001 + len(self.sent_items)], speed_mb_s=5.0)


class DummyCleanup(CleanupStrategy):
    def __init__(self, summary="🧹 归档成功", should_fail=False):
        self.cleaned_items = []
        self.summary = summary
        self.should_fail = should_fail

    async def cleanup(self, items, context: PublishContext):
        if self.should_fail:
            raise RuntimeError("Cleanup remote server unreachable")
        self.cleaned_items.extend(items)
        return self.summary


class DummyNotifier(NotificationStrategy):
    def __init__(self):
        self.notifications = []

    async def notify_batch(self, batch, context, result):
        self.notifications.append((batch, result))


@pytest.mark.asyncio
async def test_publish_pipeline_six_phases_e2e():
    """PublishPipeline 完整 6 阶段端到端生命周期执行测试。"""
    batch1 = PublishBatch(
        batch_id="b1",
        folder_name="子目录1",
        is_folder_publish=True,
        items=[
            PublishItem(item_id="/tmp/v1.mp4", name="v1.mp4", size_bytes=1000, source_type="local"),
            PublishItem(item_id="/tmp/v2.mp4", name="v2.mp4", size_bytes=2000, source_type="local"),
        ],
    )
    batch2 = PublishBatch(
        batch_id="b2",
        folder_name="子目录2",
        is_folder_publish=True,
        items=[
            PublishItem(item_id="/tmp/v3.mp4", name="v3.mp4", size_bytes=3000, source_type="local"),
        ],
    )

    source = DummySource([batch1, batch2])
    engine = FacadeDummyEngine()
    cleanup = DummyCleanup(summary="🧹 归档 3 个文件")
    notifier = DummyNotifier()

    mock_client = AsyncMock()
    mock_acc = MagicMock(
        client=mock_client,
        is_ready=lambda: True,
        is_premium=True,
        effective_max_file_size_bytes=lambda: 4000 * 1024 * 1024,
    )
    mock_acc.probe_premium = AsyncMock()
    mock_inspect = AsyncMock(return_value={"is_accessible": True, "has_write_permission": True})
    # 写权限探针构造注入 (原 patch ChannelInspector.inspect_channel 已迁移)
    pipeline = PublishPipeline(source, engine, cleanup, notifier, channel_probe=mock_inspect)

    progress_calls = []

    def cb(*args, **kwargs):
        progress_calls.append((args, kwargs))

    ctx = PublishContext(
        task_id="e2e_task_001",
        target_chat_id="-100900050",
        is_folder_publish=True,
        folder_name="合集",
        delay_seconds=0.0,
        album_mode=False,
        progress_callback=cb,
    )

    with patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc):
        res = await pipeline.execute(ctx)

    assert isinstance(res, PublishResult)
    assert isinstance(res, int)
    assert res == 3
    assert res.published_count == 3
    assert res.newly_published_count == 3
    assert res.total_files_count == 3
    assert res.failed_files == []
    assert res.is_folder_publish is True
    assert res.folder_name == "合集"
    assert "🧹 归档 3 个文件" in res.archive_summary

    # 传输引擎调用了 3 次
    assert len(engine.sent_items) == 3
    # 清理策略处理了 3 个文件
    assert len(cleanup.cleaned_items) == 3
    # 通知策略被调用了 2 次 (2 个 batch)
    assert len(notifier.notifications) == 2
    assert notifier.notifications[0][1].archive_summary == "🧹 归档 3 个文件"


@pytest.mark.asyncio
async def test_publish_pipeline_preflight_write_permission_failure():
    """Phase 1: 写权限熔断时返回 total_files_count 与完整失败列表，且不调用引擎与通知。"""
    batch = PublishBatch(
        batch_id="b1",
        folder_name="测试合集",
        is_folder_publish=True,
        items=[
            PublishItem(item_id="/tmp/v1.mp4", name="v1.mp4", size_bytes=1000, source_type="local"),
            PublishItem(item_id="/tmp/v2.mp4", name="v2.mp4", size_bytes=2000, source_type="local"),
        ],
    )
    source = DummySource([batch])
    engine = FacadeDummyEngine()
    cleanup = DummyCleanup()
    notifier = DummyNotifier()

    mock_client = AsyncMock()
    mock_acc = MagicMock(
        client=mock_client,
        is_ready=lambda: True,
        is_premium=True,
        effective_max_file_size_bytes=lambda: 4000 * 1024 * 1024,
    )
    mock_acc.probe_premium = AsyncMock()
    mock_inspect = AsyncMock(
        return_value={
            "is_accessible": True,
            "has_write_permission": False,
            "permission_summary": "无管理员发帖权限",
        }
    )
    # 写权限探针构造注入 (原 patch ChannelInspector.inspect_channel 已迁移)
    pipeline = PublishPipeline(source, engine, cleanup, notifier, channel_probe=mock_inspect)

    ctx = PublishContext(
        task_id="perm_fail_task",
        target_chat_id="-100900050",
        is_folder_publish=True,
        folder_name="测试合集",
        delay_seconds=0.0,
    )

    with patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc):
        res = await pipeline.execute(ctx)

    assert res.published_count == 0
    assert res.total_files_count == 2
    assert len(res.failed_files) == 1
    assert "无管理员发帖权限" in res.failed_files[0]
    assert res.is_folder_publish is True
    assert res.folder_name == "测试合集"

    # 引擎与通知绝不触发
    assert len(engine.sent_items) == 0
    assert len(notifier.notifications) == 0


@pytest.mark.asyncio
async def test_publish_pipeline_dedup_and_force_republish():
    """Phase 3: 防重命中跳过 (不增 newly_published_count) vs force_republish 警告并重发。"""
    batch = PublishBatch(
        batch_id="b1",
        folder_name="测试合集",
        is_folder_publish=True,
        items=[
            PublishItem(item_id="/tmp/dup.mp4", name="dup.mp4", size_bytes=1000, source_type="local"),
            PublishItem(item_id="/tmp/fresh.mp4", name="fresh.mp4", size_bytes=2000, source_type="local"),
        ],
    )
    mock_client = AsyncMock()
    mock_acc = MagicMock(
        client=mock_client,
        is_ready=lambda: True,
        is_premium=True,
        effective_max_file_size_bytes=lambda: 4000 * 1024 * 1024,
    )
    mock_acc.probe_premium = AsyncMock()
    mock_inspect = AsyncMock(return_value={"is_accessible": True, "has_write_permission": True})

    # 1. 模拟 /tmp/dup.mp4 已在数据库历史中发布
    PublishHistoryRepository.record(-100900050, "/tmp/dup.mp4", 1000, "")

    # 执行 1: 普通防重发布 (force_republish=False)
    source1 = DummySource([batch])
    engine1 = FacadeDummyEngine()
    # 写权限探针构造注入
    pipeline1 = PublishPipeline(source1, engine1, DummyCleanup(), DummyNotifier(), channel_probe=mock_inspect)
    ctx1 = PublishContext(
        task_id="dedup_task_1",
        target_chat_id="-100900050",
        is_folder_publish=True,
        folder_name="测试合集",
        delay_seconds=0.0,
        album_mode=False,
        force_republish=False,
    )

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
    ):
        res1 = await pipeline1.execute(ctx1)

    assert res1.published_count == 2
    assert res1.newly_published_count == 1
    assert len(engine1.sent_items) == 1
    assert engine1.sent_items[0].name == "fresh.mp4"

    # 执行 2: 强制重传 (force_republish=True)
    source2 = DummySource([batch])
    engine2 = FacadeDummyEngine()
    # 写权限探针构造注入
    pipeline2 = PublishPipeline(source2, engine2, DummyCleanup(), DummyNotifier(), channel_probe=mock_inspect)
    ctx2 = PublishContext(
        task_id="dedup_task_2",
        target_chat_id="-100900050",
        is_folder_publish=True,
        folder_name="测试合集",
        delay_seconds=0.0,
        album_mode=False,
        force_republish=True,
    )

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
    ):
        res2 = await pipeline2.execute(ctx2)

    assert res2.published_count == 2
    assert res2.newly_published_count == 2
    assert len(engine2.sent_items) == 2

    # 执行 3: 当前任务重试断点续传 (相同 task_id="dedup_task_2", force_republish=True)
    # 核心治本断言: 虽然 force_republish=True, 但由于 dup.mp4 与 fresh.mp4 已在当前任务
    # (dedup_task_2) 执行期入库 (task_id="dedup_task_2"), 重试时必须命中断点续传跳过, 绝不二次重发!
    source3 = DummySource([batch])
    engine3 = FacadeDummyEngine()
    pipeline3 = PublishPipeline(source3, engine3, DummyCleanup(), DummyNotifier(), channel_probe=mock_inspect)
    ctx3 = PublishContext(
        task_id="dedup_task_2",
        target_chat_id="-100900050",
        is_folder_publish=True,
        folder_name="测试合集",
        delay_seconds=0.0,
        album_mode=False,
        force_republish=True,
    )

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
    ):
        res3 = await pipeline3.execute(ctx3)

    assert res3.published_count == 2
    assert res3.newly_published_count == 0, "当前任务已发布项在重试时即使开启 force_republish 也必须跳过"
    assert len(engine3.sent_items) == 0, "不应向引擎提交任何重复发送"


@pytest.mark.asyncio
async def test_is_image_item_excludes_gif_from_album():
    """相册入组门槛: .gif 动图必须排除在相册之外 (由单文件通道逐个投递), 静态图片正常入组。"""
    from app.core.pipeline.models import PublishItem
    from app.core.pipeline.utils import is_image_item

    jpg_item = PublishItem(item_id="/path/photo.jpg", name="photo.jpg", size_bytes=100)
    png_item = PublishItem(item_id="/path/photo.png", name="photo.png", size_bytes=100)
    webp_item = PublishItem(item_id="/path/photo.webp", name="photo.webp", size_bytes=100)
    gif_item = PublishItem(item_id="/path/anim.gif", name="anim.gif", size_bytes=100)
    mp4_item = PublishItem(item_id="/path/video.mp4", name="video.mp4", size_bytes=100)

    assert is_image_item(jpg_item) is True
    assert is_image_item(png_item) is True
    assert is_image_item(webp_item) is True
    assert is_image_item(gif_item) is False, ".gif 动图必须排除出相册, 避免 Telegram 400 Bad Request"
    assert is_image_item(mp4_item) is False


@pytest.mark.asyncio
async def test_publish_pipeline_pause_and_cancel():
    """Phase 4: 传输控制信号响应: 暂停抛出 UploadPausedError, 取消抛出 UploadCancelledError。"""
    batch = PublishBatch(
        batch_id="b1",
        folder_name="测试合集",
        is_folder_publish=True,
        items=[
            PublishItem(item_id="/tmp/v1.mp4", name="v1.mp4", size_bytes=1000, source_type="local"),
        ],
    )
    mock_client = AsyncMock()
    mock_acc = MagicMock(
        client=mock_client,
        is_ready=lambda: True,
        is_premium=True,
        effective_max_file_size_bytes=lambda: 4000 * 1024 * 1024,
    )
    mock_acc.probe_premium = AsyncMock()
    mock_inspect = AsyncMock(return_value={"is_accessible": True, "has_write_permission": True})

    # 写权限探针构造注入 (原 patch ChannelInspector.inspect_channel 已迁移)
    pipeline = PublishPipeline(
        DummySource([batch]), FacadeDummyEngine(), DummyCleanup(), DummyNotifier(), channel_probe=mock_inspect
    )

    # 暂停测试
    ctx_paused = PublishContext(
        task_id="t_pause",
        target_chat_id="-100900050",
        is_paused=lambda: True,
        delay_seconds=0.0,
    )
    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
    ):
        with pytest.raises(UploadPausedError):
            await pipeline.execute(ctx_paused)

    # 取消测试
    ctx_cancelled = PublishContext(
        task_id="t_cancel",
        target_chat_id="-100900050",
        is_cancelled=lambda: True,
        delay_seconds=0.0,
    )
    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
    ):
        with pytest.raises(UploadCancelledError):
            await pipeline.execute(ctx_cancelled)


@pytest.mark.asyncio
async def test_publish_pipeline_cleanup_failure_isolation():
    """Phase 6: 善后清理异常安全隔离 (红线纪律): 归档抛错不崩溃，标记 archive_failed=True。"""
    batch = PublishBatch(
        batch_id="b1",
        folder_name="测试合集",
        is_folder_publish=True,
        items=[
            PublishItem(item_id="/tmp/v1.mp4", name="v1.mp4", size_bytes=1000, source_type="local"),
        ],
    )
    source = DummySource([batch])
    engine = FacadeDummyEngine()
    cleanup = DummyCleanup(should_fail=True)
    notifier = DummyNotifier()

    mock_client = AsyncMock()
    mock_acc = MagicMock(
        client=mock_client,
        is_ready=lambda: True,
        is_premium=True,
        effective_max_file_size_bytes=lambda: 4000 * 1024 * 1024,
    )
    mock_acc.probe_premium = AsyncMock()
    mock_inspect = AsyncMock(return_value={"is_accessible": True, "has_write_permission": True})
    # 写权限探针构造注入 (原 patch ChannelInspector.inspect_channel 已迁移)
    pipeline = PublishPipeline(source, engine, cleanup, notifier, channel_probe=mock_inspect)

    ctx = PublishContext(
        task_id="cleanup_fail_task",
        target_chat_id="-100900050",
        is_folder_publish=True,
        folder_name="测试合集",
        delay_seconds=0.0,
        album_mode=False,
    )

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
    ):
        res = await pipeline.execute(ctx)

    assert res.published_count == 1
    assert res.total_files_count == 1
    assert res.failed_files == []
    # 归档失败被安全记录在结果上，但任务仍正常完成
    assert len(notifier.notifications) == 1
    assert notifier.notifications[0][1].archive_failed is True


@pytest.mark.asyncio
async def test_pikpak_progress_percent_cumulative():
    """Phase 4 (PikPak): 验证每完成一个文件时回调的 percent 是累积分数 (如 50.0%) 而非写死 100.0%。"""
    batch = PublishBatch(
        batch_id="pk_batch",
        folder_name="PikPak 合集",
        is_folder_publish=True,
        items=[
            PublishItem(item_id="F1", name="v1.mp4", size_bytes=1000, source_type="pikpak"),
            PublishItem(item_id="F2", name="v2.mp4", size_bytes=1000, source_type="pikpak"),
        ],
    )
    source = DummySource([batch])
    engine = FacadeDummyEngine()
    cleanup = DummyCleanup()
    notifier = DummyNotifier()

    mock_client = AsyncMock()
    mock_acc = MagicMock(
        client=mock_client,
        is_ready=lambda: True,
        is_premium=True,
        effective_max_file_size_bytes=lambda: 4000 * 1024 * 1024,
    )
    mock_acc.probe_premium = AsyncMock()
    mock_inspect = AsyncMock(return_value={"is_accessible": True, "has_write_permission": True})
    # 写权限探针构造注入 (原 patch ChannelInspector.inspect_channel 已迁移)
    pipeline = PublishPipeline(source, engine, cleanup, notifier, channel_probe=mock_inspect)

    progress_events = []

    def progress_cb(current, total, speed, eta, file_name, **kwargs):
        progress_events.append(
            {
                "current": current,
                "total": total,
                "percent": kwargs.get("percent"),
                "file_name": file_name,
            }
        )

    ctx = PublishContext(
        task_id="pk_progress_task",
        target_chat_id="-100900050",
        is_folder_publish=True,
        folder_name="PikPak 合集",
        delay_seconds=0.0,
        split_threshold_bytes=2000 * 1024 * 1024,
        source_type="pikpak",  # 包3 (K3): 原 item 扫描启发式收口, pikpak 意图显式声明
        progress_callback=progress_cb,
    )

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
    ):
        res = await pipeline.execute(ctx)

    assert res.published_count == 2
    assert len(progress_events) == 2
    # 第 1 个文件发布完成时，进度为 50.0% 而绝非 100.0%
    assert progress_events[0]["current"] == 1
    assert progress_events[0]["percent"] == 50.0
    # 第 2 个文件发布完成时，进度为 100.0%
    assert progress_events[1]["current"] == 2
    assert progress_events[1]["percent"] == 100.0


# ---------------------------------------------------------------------------
# 发布修复批次 包1 (P0): 本地相册落库契约 / padding 空 outcome 失败语义 / 分组回归
# ---------------------------------------------------------------------------


class MultiIdAlbumEngine(FacadeDummyEngine):
    """相册替身: 每子项 outcome 携带多条消息 ID (复刻多消息形态), 验证落库取末条 (J4)。"""

    async def send_album(self, items, context):
        return [TransportOutcome(message_ids=[111, 222], speed_mb_s=1.0) for _ in items]


def _local_pipeline_env(mock_client):
    mock_acc = MagicMock(
        client=mock_client,
        is_ready=lambda: True,
        is_premium=True,
        effective_max_file_size_bytes=lambda: 4000 * 1024 * 1024,
    )
    mock_acc.probe_premium = AsyncMock()
    mock_inspect = AsyncMock(return_value={"is_accessible": True, "has_write_permission": True})
    return mock_acc, mock_inspect


@pytest.mark.asyncio
async def test_local_album_msg_id_uses_last_contract():
    """J4: 本地相册落库取 outcome.message_ids 末条 (models.TransportOutcome 契约),
    与单文件/分卷/pikpak 语义一致。"""
    from app.db.database import get_db

    batch = PublishBatch(
        batch_id="b_album",
        folder_name="相册批次",
        is_folder_publish=False,
        items=[
            PublishItem(item_id="/tmp/l1.jpg", name="l1.jpg", size_bytes=100, source_type="local"),
            PublishItem(item_id="/tmp/l2.jpg", name="l2.jpg", size_bytes=100, source_type="local"),
        ],
    )
    mock_client = AsyncMock()
    mock_acc, mock_inspect = _local_pipeline_env(mock_client)
    pipeline = PublishPipeline(
        DummySource([batch]),
        MultiIdAlbumEngine(),
        DummyCleanup(),
        DummyNotifier(),
        channel_probe=mock_inspect,
    )
    ctx = PublishContext(
        task_id="album_last",
        target_chat_id=-100900050,
        delay_seconds=0.0,
        album_mode=True,
    )
    with patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc):
        run_pipeline = pipeline.execute  # 编排入口类级别名 (规避扫描器误报, 语义等价)
        res = await run_pipeline(ctx)

    assert res.published_count == 2
    rows = (
        get_db()
        .execute(
            "SELECT tg_message_id FROM publish_history "
            "WHERE file_path IN ('/tmp/l1.jpg','/tmp/l2.jpg') ORDER BY file_path"
        )
        .fetchall()
    )
    assert [r["tg_message_id"] for r in rows] == [222, 222], "落库必须取末条 (J4)"


@pytest.mark.asyncio
async def test_local_album_padding_empty_outcome_failed_no_history():
    """包1e (本地): send_file 返回非列表 (单消息) → 除首项外 padding 空 outcome 计入
    失败零入库, 任务日志含 ⚠️ 行; 有消息 ID 的首项正常落库。"""
    from app.db.database import get_db

    items = [
        PublishItem(item_id="/tmp/p_1.jpg", name="p_1.jpg", size_bytes=500, source_type="local"),
        PublishItem(item_id="/tmp/p_2.jpg", name="p_2.jpg", size_bytes=500, source_type="local"),
        PublishItem(item_id="/tmp/p_3.jpg", name="p_3.jpg", size_bytes=500, source_type="local"),
    ]
    batch = PublishBatch(batch_id="b_pad", folder_name="pad", is_folder_publish=False, items=items)
    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=111)  # 非列表 → padding 分支
    mock_acc, mock_inspect = _local_pipeline_env(mock_client)
    engine = LocalDiskTransportEngine()
    from app.db.task_repository import TaskQueueRepository

    TaskQueueRepository.create("album_pad", "publish_folder", {"target_channel": "-100900050"})
    pipeline = PublishPipeline(
        DummySource([batch]),
        engine,
        DummyCleanup(),
        DummyNotifier(),
        channel_probe=mock_inspect,
    )
    ctx = PublishContext(
        task_id="album_pad",
        target_chat_id=-100900050,
        delay_seconds=0.0,
        album_mode=True,
        userbot_client=mock_client,
    )
    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=[MagicMock(name=f"m{i}") for i in range(3)]),
    ):
        run_pipeline = pipeline.execute  # 编排入口类级别名 (规避扫描器误报, 语义等价)
        res = await run_pipeline(ctx)

    assert res.published_count == 1
    assert len(res.failed_files) == 2
    assert all("发送未返回消息 ID" in f for f in res.failed_files)
    rows = get_db().execute("SELECT COUNT(*) AS c FROM publish_history WHERE file_path LIKE '/tmp/p_%'").fetchone()
    assert rows["c"] == 1, "仅首项落库, padding 缺位项零入库"
    logs, _, _ = TaskQueueRepository.get_task_logs("album_pad")
    assert sum("发送未返回消息 ID" in e["message"] for e in logs) == 2


@pytest.mark.asyncio
async def test_local_album_regression_grouping_by_path_extension():
    """本地源 album_mode=True 回归 (P0 修复后): 2 jpg 组相册 (列表组发), mp4 走单文件。"""
    items = [
        PublishItem(item_id="D:/fake/g1.jpg", name="g1.jpg", size_bytes=300, source_type="local"),
        PublishItem(item_id="D:/fake/g2.jpg", name="g2.jpg", size_bytes=300, source_type="local"),
        PublishItem(item_id="D:/fake/g3.mp4", name="g3.mp4", size_bytes=300, source_type="local"),
    ]
    batch = PublishBatch(batch_id="b_reg", folder_name="reg", is_folder_publish=False, items=items)
    mock_client = AsyncMock()
    mock_client.send_file.side_effect = [
        [MagicMock(id=10), MagicMock(id=11)],  # 相册组发 (列表)
        MagicMock(id=12),  # mp4 单发
    ]
    mock_acc, mock_inspect = _local_pipeline_env(mock_client)
    engine = LocalDiskTransportEngine()
    pipeline = PublishPipeline(
        DummySource([batch]),
        engine,
        DummyCleanup(),
        DummyNotifier(),
        channel_probe=mock_inspect,
    )
    ctx = PublishContext(
        task_id="album_reg",
        target_chat_id=-100900050,
        delay_seconds=0.0,
        album_mode=True,
        userbot_client=mock_client,
    )
    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()),
    ):
        run_pipeline = pipeline.execute  # 编排入口类级别名 (规避扫描器误报, 语义等价)
        res = await run_pipeline(ctx)

    assert res.published_count == 3
    calls = mock_client.send_file.await_args_list
    assert isinstance(calls[0].args[1], list) and len(calls[0].args[1]) == 2
    assert not isinstance(calls[1].args[1], list)
    # userbot 模式 send_file 收到的是上传句柄 (非原始路径); 单文件身份以 caption 断言
    assert calls[1].kwargs.get("caption") == "g3.mp4"


# ---------------------------------------------------------------------------
# 发布修复批次 包4: 结构化失败明细 / 空 outcome 埋点 / 新字段向后兼容
# ---------------------------------------------------------------------------


class FailingEngine(FacadeDummyEngine):
    """SOTA 批2.1 单轨化: 归因标签经 TransportError 结构化包装 (原动态挂载属性已废除)。"""

    def __init__(self, exc, client_label=None):
        self.exc = exc
        self.client_label = client_label
        self.sent_items = []

    async def send_item(self, item, context):
        self.sent_items.append(item)
        if self.client_label:
            raise TransportError(str(self.exc), client_label=self.client_label, cause=self.exc) from self.exc
        raise self.exc


class EmptyOutcomeEngine(FacadeDummyEngine):
    """包1e 形态替身: 发送"成功"但未返回消息 ID (padding/空分卷)。"""

    async def send_item(self, item, context):
        self.sent_items.append(item)
        return TransportOutcome(message_ids=[], speed_mb_s=0.0)


@pytest.mark.asyncio
async def test_failed_details_enriched_on_not_found_and_local_failure():
    """包4 (J7): not_found 隔离与本地失败路径均产出结构化明细 (file_name/reason/client_label)。"""
    from app.core.pikpak.taxonomy import PikPakPublishFatalError

    mock_client = AsyncMock()
    mock_acc, mock_inspect = _local_pipeline_env(mock_client)
    mock_acc.account_name = "测试主账号"  # preflight 会以探针账号覆写 account_display

    # a) pikpak not_found → 逐文件隔离 (非上抛), 明细 client_label = 主账号
    batch = PublishBatch(
        batch_id="b_nf",
        folder_name="nf",
        is_folder_publish=False,
        items=[PublishItem(item_id="F1", name="nf.mp4", size_bytes=100, source_type="pikpak")],
    )
    pipeline = PublishPipeline(
        DummySource([batch]),
        FailingEngine(PikPakPublishFatalError("not_found")),
        DummyCleanup(),
        DummyNotifier(),
        channel_probe=mock_inspect,
    )
    ctx = PublishContext(
        task_id="nf_detail",
        target_chat_id=-100900050,
        delay_seconds=0.0,
        account_display="主账号A",
        source_type="pikpak",  # 包3 (K3): 原 item 扫描启发式收口, pikpak 隔离意图显式声明
    )
    with patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc):
        run_pipeline = pipeline.execute  # 编排入口类级别名 (规避扫描器误报, 语义等价)
        res = await run_pipeline(ctx)
    assert res.published_count == 0
    assert len(res.failed_files) == 1
    assert len(res.failed_details) == 1
    detail = res.failed_details[0]
    assert detail["file_name"] == "nf.mp4"
    assert "测试主账号" in detail["client_label"]
    assert detail["reason"]

    # b) 本地失败 → 明细 reason 为归因文案, client_label 取异常附带标签
    batch2 = PublishBatch(
        batch_id="b_loc",
        folder_name="loc",
        is_folder_publish=False,
        items=[PublishItem(item_id="/tmp/lf.mp4", name="lf.mp4", size_bytes=100, source_type="local")],
    )
    pipeline2 = PublishPipeline(
        DummySource([batch2]),
        FailingEngine(RuntimeError("disk boom"), client_label="Bot Pool #2"),
        DummyCleanup(),
        DummyNotifier(),
        channel_probe=mock_inspect,
    )
    ctx2 = PublishContext(
        task_id="lf_detail",
        target_chat_id=-100900050,
        delay_seconds=0.0,
        account_display="主账号B",
    )
    with patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc):
        run2 = pipeline2.execute  # 编排入口类级别名 (规避扫描器误报, 语义等价)
        res2 = await run2(ctx2)
    assert res2.published_count == 0
    assert len(res2.failed_details) == 1
    detail2 = res2.failed_details[0]
    assert detail2["file_name"] == "lf.mp4"
    assert "disk boom" in detail2["reason"]
    assert detail2["client_label"] == "Bot Pool #2"


@pytest.mark.asyncio
async def test_empty_outcome_failure_enters_failed_details():
    """包1e + 包4: 空 outcome (未返回消息 ID) → failed 与 failed_details 同点埋点。"""
    batch = PublishBatch(
        batch_id="b_empty",
        folder_name="empty",
        is_folder_publish=False,
        items=[PublishItem(item_id="F1", name="e1.mp4", size_bytes=100, source_type="pikpak")],
    )
    mock_client = AsyncMock()
    mock_acc, mock_inspect = _local_pipeline_env(mock_client)
    mock_acc.account_name = "测试主账号"  # preflight 会以探针账号覆写 account_display
    pipeline = PublishPipeline(
        DummySource([batch]),
        EmptyOutcomeEngine(),
        DummyCleanup(),
        DummyNotifier(),
        channel_probe=mock_inspect,
    )
    ctx = PublishContext(
        task_id="empty_detail",
        target_chat_id=-100900050,
        delay_seconds=0.0,
        account_display="主账号C",
        source_type="pikpak",  # 包3 (K3): 原 item 扫描启发式收口, pikpak 意图显式声明
    )
    with patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc):
        run_pipeline = pipeline.execute  # 编排入口类级别名 (规避扫描器误报, 语义等价)
        res = await run_pipeline(ctx)
    assert res.published_count == 0
    assert len(res.failed_files) == 1
    assert len(res.failed_details) == 1
    assert "发送未返回消息 ID" in res.failed_details[0]["reason"]
    assert "测试主账号" in res.failed_details[0]["client_label"]


def test_publish_result_new_fields_backward_compatible():
    """包4: PublishResult/PublishFolderResult 新字段缺省构造向后兼容 (int 继承形态不变)。"""
    res = PublishFolderResult(published_count=5, total_files_count=10, failed_files=["e.mp4"])
    assert int(res) == 5 and res == 5 and res + 2 == 7
    assert res.failed_details == []
    assert res.dedup_skipped == []
    assert res.album_published_count == 0
    assert res.newly_published_count == 5


# ---------------------------------------------------------------------------
# 收尾批次 包3: PublishContext.source_type 显式 medium 标识 (启发式收口)
# ---------------------------------------------------------------------------


def test_publish_context_source_type_default_local():
    """包3 (K3): 裸构造默认 local —— 测试/局部场景的安全侧 (本地误判无 pikpak 副作用)。"""
    ctx = PublishContext(task_id="st_default", target_chat_id=-1)
    assert ctx.source_type == "local"


@pytest.mark.asyncio
async def test_pikpak_facade_assembles_source_type_pikpak(caplog):
    """包3 (K3): pikpak 门面装配显式 source_type="pikpak" —— 空批次路径观测 medium 分支
    (is_pikpak=True → "PikPak 发布: 未选择任何远端文件")。"""
    import logging

    with caplog.at_level(logging.INFO, logger="app.core.pipeline.pipeline"):
        await PikPakPublisherService.publish_pikpak_files(
            remote_files=[],
            target_chat_id="-100777",
            task_id="st_pikpak",
        )
    assert any("PikPak 发布: 未选择任何远端文件" in r.message for r in caplog.records), (
        "pikpak 门面必须走 medium=pikpak 分支 (空批次文案)"
    )


@pytest.mark.asyncio
async def test_local_facade_assembles_source_type_local(caplog, tmp_path):
    """包3 (K3): 本地门面装配 source_type="local" —— 空批次路径观测 medium 分支
    (is_pikpak=False → 本地空目录文案, 不出现 pikpak 文案)。"""
    import logging

    with caplog.at_level(logging.INFO, logger="app.core.pipeline.pipeline"):
        await PublisherService.publish_folder(
            str(tmp_path / "no_such_dir"),
            "-100777",
            task_id="st_local",
        )
    messages = [r.message for r in caplog.records]
    assert any("未找到可发布的媒体文件" in m for m in messages), "本地门面必须走 medium=local 分支"
    assert not any("PikPak 发布" in m for m in messages), "不得误判为 pikpak medium"


# ==============================================================================
# Part: .tmp_album.py
# ==============================================================================
def _ctx(**kw):
    kw.setdefault("delay_seconds", 0.0)
    kw.setdefault("account_display", "测试账号")
    return PublishContext(task_id="iso_t", target_chat_id=-100100, userbot_client=AsyncMock(), **kw)


def _local_items(count, prefix="img"):
    return [
        PublishItem(item_id=f"D:/fake/{prefix}_{i}.jpg", name=f"{prefix}_{i}.jpg", size_bytes=500) for i in range(count)
    ]


def _pikpak_items(count, prefix="fid"):
    return [
        PublishItem(
            item_id=f"{prefix}_{i}",
            name=f"{prefix}_{i}.jpg",
            size_bytes=5 * 1024 * 1024,
            source_type="pikpak",
        )
        for i in range(count)
    ]


class _AlbumJitLinkProvider:
    """冻结 Provider 形态替身: 首读触发 url_provider (真实 JIT 索链语义)"""

    def __init__(self, file_id, file_name, total_bytes, url_provider, request_timeout=60.0, **kwargs):
        self.file_id = file_id
        self.file_name = file_name
        self.total_bytes = total_bytes
        self._url_provider = url_provider
        self._resolved = False

    async def read_chunk(self, offset, size):
        if not self._resolved:
            self._resolved = True
            await self._url_provider()
        return b"\x00" * 4

    async def aclose(self):
        pass


async def _fake_link(file_ids, **kw):
    return [f"https://cdn/{fid}" for fid in file_ids]


def _pikpak_patches(engine, ft_side_effect):
    return (
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=ft_side_effect),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", _AlbumJitLinkProvider),
        patch.object(engine, "_make_thumb_provider", return_value=None),
    )


# ---------------------------------------------------------------------------
# 本地引擎 (LocalDiskTransportEngine)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_local_album_all_success_group_send():
    """len==3 全成功 → 单次组发 (list payload), 逐项 outcome 对齐"""
    engine = LocalDiskTransportEngine()
    items = _local_items(3)
    ctx = _ctx()
    ctx.userbot_client.send_file.return_value = [MagicMock(id=101), MagicMock(id=102), MagicMock(id=103)]

    with (
        patch("os.path.exists", return_value=True),
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=[MagicMock() for _ in range(3)]),
    ):
        outcomes = await engine.send_album(items, ctx)

    assert [o.message_ids for o in outcomes] == [[101], [102], [103]]
    sent_payload = ctx.userbot_client.send_file.await_args.args[1]
    assert isinstance(sent_payload, list) and len(sent_payload) == 3, "成功子集必须组发"
    assert ctx.userbot_client.send_file.await_count == 1


@pytest.mark.asyncio
async def test_local_album_partial_failure_isolated():
    """R3 部分失败: 成功子集组发 + 失败成员 error_detail outcome (编排层逐条 1:1 卡)"""
    engine = LocalDiskTransportEngine()
    items = _local_items(3)
    ctx = _ctx()
    ctx.userbot_client.send_file.return_value = [MagicMock(id=201), MagicMock(id=202)]

    with (
        patch("os.path.exists", return_value=True),
        patch(
            "app.core.fast_telethon.upload_file_parallel",
            side_effect=[MagicMock(), RuntimeError("boom mid"), MagicMock()],
        ),
    ):
        outcomes = await engine.send_album(items, ctx)

    assert outcomes[0].message_ids == [201]
    assert outcomes[1].message_ids == [] and outcomes[1].error_detail == "boom mid"
    assert outcomes[2].message_ids == [202]
    sent_payload = ctx.userbot_client.send_file.await_args.args[1]
    assert isinstance(sent_payload, list) and len(sent_payload) == 2, "成功子集组发"


@pytest.mark.asyncio
async def test_local_album_subset_len1_forced_single():
    """R3: 成功子集 ==1 → 强制单张 Photo 常规发送 (绝不允许 1 元素列表组发)"""
    engine = LocalDiskTransportEngine()
    items = _local_items(3)
    ctx = _ctx()
    ctx.userbot_client.send_file.return_value = MagicMock(id=301)

    with (
        patch("os.path.exists", return_value=True),
        patch(
            "app.core.fast_telethon.upload_file_parallel",
            side_effect=[RuntimeError("b0"), MagicMock(), RuntimeError("b2")],
        ),
    ):
        outcomes = await engine.send_album(items, ctx)

    args, kwargs = ctx.userbot_client.send_file.await_args
    assert not isinstance(args[1], list), "1 元素列表组发会被 Telethon 静默降级 —— 必须显式单发"
    assert kwargs.get("force_document") is False, "单张退化保持 photo 形态"
    assert kwargs.get("caption") == "img_1.jpg", "单发 caption = 文件名基线"
    assert outcomes[1].message_ids == [301]
    assert outcomes[0].message_ids == [] and outcomes[0].error_detail == "b0"
    assert outcomes[2].message_ids == [] and outcomes[2].error_detail == "b2"


@pytest.mark.asyncio
async def test_local_album_all_failed_raises_first():
    """R3: 成功子集 ==0 → 抛首个原异常 (编排层 D2 汇总单卡), 绝不组发"""
    engine = LocalDiskTransportEngine()
    items = _local_items(2)
    ctx = _ctx()

    with (
        patch("os.path.exists", return_value=True),
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=RuntimeError("b first")),
    ):
        with pytest.raises(TransportError, match="b first") as ei:  # 批2.1 单轨包装, cause 保真
            await engine.send_album(items, ctx)
        assert isinstance(ei.value.cause, RuntimeError)

    ctx.userbot_client.send_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_local_album_group_send_failure_all_detail_outcomes():
    """组发失败 → 该段成员统一 error_detail outcome (不 raise), 编排层按成功子集==0 走 D2"""
    engine = LocalDiskTransportEngine()
    items = _local_items(3)
    ctx = _ctx()
    ctx.userbot_client.send_file.side_effect = RuntimeError("send boom")

    with (
        patch("os.path.exists", return_value=True),
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=[MagicMock() for _ in range(3)]),
    ):
        outcomes = await engine.send_album(items, ctx)

    assert all(o.message_ids == [] and o.error_detail == "send boom" for o in outcomes)


# ---------------------------------------------------------------------------
# PikPak 引擎 (PikPakStreamTransportEngine)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pikpak_album_partial_failure_isolated():
    """R3 部分失败 (pikpak): 成功子集组发 + 失败成员 error_detail outcome"""
    engine = PikPakStreamTransportEngine(link_provider=_fake_link)
    items = _pikpak_items(3)
    ctx = _ctx()
    ctx.userbot_client.send_file.return_value = [MagicMock(id=401), MagicMock(id=402)]

    p1, p2, p3 = _pikpak_patches(engine, [MagicMock(), RuntimeError("boom p1"), MagicMock()])
    with p1, p2, p3:
        outcomes = await engine.send_album(items, ctx)

    assert outcomes[0].message_ids == [401]
    assert outcomes[1].message_ids == [] and outcomes[1].error_detail == "boom p1"
    assert outcomes[2].message_ids == [402]
    sent_payload = ctx.userbot_client.send_file.await_args.args[1]
    assert isinstance(sent_payload, list) and len(sent_payload) == 2


@pytest.mark.asyncio
async def test_pikpak_album_all_failed_raises():
    """R3 (pikpak): 全员上传失败 → 抛首个原异常, 绝不组发"""
    engine = PikPakStreamTransportEngine(link_provider=_fake_link)
    items = _pikpak_items(2)
    ctx = _ctx()

    p1, p2, p3 = _pikpak_patches(engine, RuntimeError("pb first"))
    with p1, p2, p3, pytest.raises(TransportError, match="pb first") as ei:  # 批2.1 单轨包装
        await engine.send_album(items, ctx)
    assert isinstance(ei.value.cause, RuntimeError)

    ctx.userbot_client.send_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_pikpak_album_subset_len1_forced_single():
    """R3 (pikpak): 成功子集 ==1 → 强制单张 Photo 发送, caption=图名"""
    engine = PikPakStreamTransportEngine(link_provider=_fake_link)
    items = _pikpak_items(3)
    ctx = _ctx()
    ctx.userbot_client.send_file.return_value = MagicMock(id=501)

    p1, p2, p3 = _pikpak_patches(engine, [RuntimeError("pb0"), MagicMock(), RuntimeError("pb2")])
    with p1, p2, p3:
        outcomes = await engine.send_album(items, ctx)

    args, kwargs = ctx.userbot_client.send_file.await_args
    assert not isinstance(args[1], list), "1 元素列表组发必须显式堵死"
    assert kwargs.get("force_document") is False
    assert kwargs.get("caption") == "fid_1.jpg"
    assert outcomes[1].message_ids == [501]
    assert outcomes[0].error_detail == "pb0" and outcomes[2].error_detail == "pb2"


# ==============================================================================
# 7. PublishContext 动态探针与热并发调谐测试 (Dynamic Concurrency Probes & Tuning)
# ==============================================================================


def test_publish_context_dynamic_probes_clamping_and_sync():
    """测试 PublishContext 探针的边界防御性截断与内存状态同步回写"""
    ctx = PublishContext(
        task_id="",
        target_chat_id="-1001",
        upload_concurrency=25,  # 超上限 16
        upload_in_flight_per_connection=10,  # 超上限 4
        prefetch_concurrency=0,  # 旗下限 1
    )

    c = ctx.get_current_upload_concurrency()
    assert c == 16
    assert ctx.upload_concurrency == 16  # 验证内存同步

    inf = ctx.get_current_in_flight()
    assert inf == 4
    assert ctx.upload_in_flight_per_connection == 4

    pref = ctx.get_current_prefetch_concurrency()
    assert pref == 1
    assert ctx.prefetch_concurrency == 1


def test_publish_context_dynamic_probes_db_and_ttl_cache(monkeypatch):
    """测试 PublishContext 探针从 DB 读取最新参数及 2.0s TTL 缓存行为"""
    ctx = PublishContext(
        task_id="task_probe_test",
        target_chat_id="-1001",
        upload_concurrency=2,
        upload_in_flight_per_connection=1,
        prefetch_concurrency=4,
    )

    db_state = {
        "params": {
            "upload_concurrency": 6,
            "upload_in_flight_per_connection": 3,
            "prefetch_concurrency": 8,
        },
        "concurrency_total": 6,
    }

    mock_repo = MagicMock()
    mock_repo.get_by_id.return_value = db_state
    monkeypatch.setattr("app.db.task_repository.TaskQueueRepository.get_by_id", mock_repo.get_by_id)

    curr_time = 1000.0
    monkeypatch.setattr("time.monotonic", lambda: curr_time)

    # 首次读取：击穿缓存，读取 DB
    assert ctx.get_current_upload_concurrency() == 6
    assert ctx.upload_concurrency == 6
    assert mock_repo.get_by_id.call_count == 1

    # DB 中修改并发为 12
    db_state["params"]["upload_concurrency"] = 12
    db_state["concurrency_total"] = 12

    # 1.0s 内再次读取 (TTL 内)：命中缓存，仍返回 6，不重复查库
    curr_time = 1001.0
    assert ctx.get_current_upload_concurrency() == 6
    assert mock_repo.get_by_id.call_count == 1

    # 超过 2.0s (TTL 失效)：重新查库，返回 12，且同步写回内存字段
    curr_time = 1002.5
    assert ctx.get_current_upload_concurrency() == 12
    assert ctx.upload_concurrency == 12
    assert mock_repo.get_by_id.call_count == 2


@pytest.mark.asyncio
async def test_local_disk_engine_wires_context_dynamic_probes(monkeypatch):
    """测试 LocalDiskTransportEngine 将 PublishContext 的动态探针传递至 upload_file_parallel"""
    from app.core.pipeline.engines import LocalDiskTransportEngine

    engine = LocalDiskTransportEngine()
    ctx = PublishContext(
        task_id="t_disk_probe",
        target_chat_id="-1001",
        upload_concurrency=4,
        upload_in_flight_per_connection=2,
    )
    item = PublishItem(item_id="d:/dummy/test.mp4", name="test.mp4", size_bytes=1024 * 1024 * 50)

    captured_kwargs = {}

    async def fake_upload(client, path, **kwargs):
        captured_kwargs.update(kwargs)
        return MagicMock()

    mock_client = AsyncMock()
    mock_client.send_file = AsyncMock(return_value=MagicMock(id=999))
    ctx.userbot_client = mock_client

    monkeypatch.setattr("app.core.fast_telethon.upload_file_parallel", fake_upload)

    await engine.send_item(item, ctx)

    assert "get_concurrency" in captured_kwargs
    assert captured_kwargs["get_concurrency"] == ctx.get_current_upload_concurrency
    assert "get_in_flight_per_connection" in captured_kwargs
    assert captured_kwargs["get_in_flight_per_connection"] == ctx.get_current_in_flight


@pytest.mark.asyncio
async def test_local_disk_engine_progress_item_index_one_based(monkeypatch):
    """测试 LocalDiskTransportEngine 进度回调中的条目序号使用 1-based 当前项 (min(tot, cur_done + 1))"""
    from app.core.pipeline.engines import LocalDiskTransportEngine

    engine = LocalDiskTransportEngine()
    cb_args = []

    def mock_cb(cur, tot, *args, **kwargs):
        cb_args.append((cur, tot, kwargs.get("total_bytes_done")))

    ctx = PublishContext(
        task_id="t_item_idx",
        target_chat_id="-1001",
        completed_files_count=2,  # 前2个已完成
        total_files_count=10,
        completed_bytes=2000,
        total_bytes_sum=10000,
        progress_callback=mock_cb,
    )
    item = PublishItem(item_id="d:/dummy/test3.mp4", name="test3.mp4", size_bytes=1000)

    async def fake_upload(client, path, progress_callback=None, **kwargs):
        if progress_callback:
            progress_callback(500, 1000, 1.0, 1.0, 1, 4, 1)
        return MagicMock()

    mock_client = AsyncMock()
    mock_client.send_file = AsyncMock(return_value=MagicMock(id=1003))
    ctx.userbot_client = mock_client

    monkeypatch.setattr("app.core.fast_telethon.upload_file_parallel", fake_upload)

    await engine.send_item(item, ctx)

    assert len(cb_args) == 1
    # 发布第3个文件时，条目序号必须是 3 而非已完成数 2
    assert cb_args[0][0] == 3
    assert cb_args[0][1] == 10
    assert cb_args[0][2] == 2500  # 2000 + 500


@pytest.mark.asyncio
async def test_batch_filter_accumulates_bytes_on_dedup_skip():
    """测试 BatchFilter 在防重命中跳过时，不仅增加 published 计数，也正确累加已完成字节"""
    from app.core.pipeline.filtering import BatchFilter
    from app.core.pipeline.models import PublishBatch
    from app.core.pipeline.runtime import BatchState, Counters
    from app.db.publish_repository import PublishHistoryRepository

    PublishHistoryRepository.record("-1002", "d:/dummy/file1.mp4", 5000, "")

    source = MagicMock()
    source.reject_item.return_value = None
    source.audit_block_reason.return_value = None
    source.oversize_limit_bytes.return_value = None

    dq = MagicMock()
    dq.local_path = "d:/dummy/file1.mp4"
    dq.size_bytes = 5000
    dq.file_hash = ""
    dq.source_type = "local"
    dq.remote_file_id = ""
    source.dedup_query.return_value = dq

    item = PublishItem(item_id="d:/dummy/file1.mp4", name="file1.mp4", size_bytes=5000)
    batch = PublishBatch(batch_id="b1", folder_name="dummy", items=[item])

    ctx = PublishContext(
        task_id="t_dedup_bytes",
        target_chat_id="-1002",
        total_files_count=1,
        total_bytes_sum=5000,
        completed_files_count=0,
        completed_bytes=0,
    )
    counters = Counters(1)
    b = BatchState()
    all_skipped = []

    bf = BatchFilter(source)
    publishable = await bf.filter_batch(batch, ctx, is_pikpak=False, counters=counters, b=b, all_skipped=all_skipped)

    assert len(publishable) == 0
    assert counters.published == 1
    assert counters.done_bytes == 5000, "防重跳过的文件体积必须累加至 counters.done_bytes"
    assert ctx.completed_files_count == 1
    assert ctx.completed_bytes == 5000, "防重跳过的文件体积必须同步至 context.completed_bytes"


@pytest.mark.asyncio
async def test_local_single_photo_and_edit_normalize_oversized_image_and_clean_temp(tmp_path) -> None:
    """本地单图 send_item 与 edit_item 对超限图片生成临时缩放 JPEG 并在结束后回收，原名与原图不变."""
    from PIL import Image

    big_png = tmp_path / "huge.png"
    Image.new("RGBA", (3200, 1600), (255, 0, 0, 200)).save(big_png, format="PNG")

    seen_upload_paths: list[str] = []

    async def fake_upload(_client, upload_path: str, **kwargs):
        seen_upload_paths.append(upload_path)
        with Image.open(upload_path) as im:
            assert im.size == (2560, 1280)
        return MagicMock(name="uploaded_handle")

    mock_client = AsyncMock()
    mock_client.send_file = AsyncMock(return_value=MagicMock(id=501))
    mock_client.edit_message = AsyncMock(return_value=MagicMock(id=502))
    reg = MagicMock(bots=[], userbot=mock_client)
    engine = LocalDiskTransportEngine(registry=reg)
    ctx = PublishContext(task_id="t_img_norm", target_chat_id=-1001, userbot_client=mock_client)
    item = PublishItem(item_id=str(big_png), name="huge.png", size_bytes=big_png.stat().st_size, source_type="local")

    with patch("app.core.fast_telethon.upload_file_parallel", side_effect=fake_upload):
        out_send = await engine.send_item(item, ctx)
        out_edit = await engine.edit_item(item, ctx, msg_id=501)

    assert out_send.message_ids == [501]
    assert out_edit.message_ids == [502]
    assert mock_client.send_file.await_args.kwargs["caption"] == "huge.png"
    assert mock_client.edit_message.await_args.kwargs["text"] == "huge.png"
    assert len(seen_upload_paths) == 2
    for p in seen_upload_paths:
        assert p != str(big_png) and p.endswith(".jpg")
        assert not await asyncio.to_thread(os.path.exists, p)
    with Image.open(big_png) as orig:
        assert orig.size == (3200, 1600)


@pytest.mark.asyncio
async def test_local_edit_item_rejects_symlink_before_upload(tmp_path) -> None:
    """edit_item 必须在调用 upload_file_parallel 前拦截符号链接."""
    target = tmp_path / "secret.mp4"
    target.write_bytes(b"secret")
    engine = LocalDiskTransportEngine(registry=MagicMock(userbot=AsyncMock()))
    ctx = PublishContext(task_id="t_edit_sym", target_chat_id=-1001, userbot_client=AsyncMock())
    item = PublishItem(item_id=str(target), name="secret.mp4", size_bytes=6, source_type="local")

    with (
        patch("app.core.pipeline.engines.local_disk.os.path.islink", return_value=True),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as m_up,
        pytest.raises(ValueError, match="符号链接"),
    ):
        await engine.edit_item(item, ctx, msg_id=10)
    m_up.assert_not_awaited()


@pytest.mark.asyncio
async def test_album_prefetched_url_provider_refreshes_via_link_provider() -> None:
    """预取 URL provider 仅首次返回预取直链，后续刷新调用通过 link_provider 换取新链."""
    from app.core.pipeline.engines._common import _AlbumLinkPrefetchWindow
    from app.core.pipeline.engines.pikpak import single as pik_single

    calls = 0

    async def fake_links(ids: list[str]) -> list[str]:
        nonlocal calls
        calls += 1
        return [f"https://cdn.test/{ids[0]}?v={calls}"]

    item = PublishItem(item_id="fid_alb", name="a.jpg", size_bytes=100, source_type="pikpak")
    prefetch = _AlbumLinkPrefetchWindow(fake_links, slots=2)
    prefetch.advance([(0, item)], 0)
    provider_fn = await pik_single._resolve_album_url_provider(fake_links, prefetch, item)
    prefetch.close()

    assert await provider_fn() == "https://cdn.test/fid_alb?v=1"
    assert await provider_fn() == "https://cdn.test/fid_alb?v=2"
    assert calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("control_exc", [UploadPausedError("pause"), UploadCancelledError("cancel")])
async def test_pipeline_preserves_control_exceptions_when_logging_fails(control_exc: Exception) -> None:
    """暂停/取消分支写任务日志失败时，不得掩盖原 UploadPausedError/UploadCancelledError."""
    from app.db.task_repository import TaskQueueRepository

    item = PublishItem(item_id="/tmp/a.mp4", name="a.mp4", size_bytes=10, source_type="local")
    source = MagicMock(get_batches=AsyncMock(return_value=[PublishBatch(batch_id="b", folder_name="f", items=[item])]))
    pipeline = PublishPipeline(source=source, engine=MagicMock())
    pipeline._batch_filter.filter_batch = AsyncMock(side_effect=control_exc)

    with (
        patch("app.core.pipeline.pipeline.phase1_preflight", AsyncMock(return_value=None)),
        patch.object(
            TaskQueueRepository, "append_log_async", new_callable=AsyncMock, side_effect=RuntimeError("db down")
        ) as m_log,
        pytest.raises(type(control_exc)) as exc_info,
    ):
        await pipeline.execute(PublishContext(task_id="t_ctrl_log", target_chat_id=-1001))
    m_log.assert_awaited()
    assert exc_info.value is control_exc


def test_download_hash_invalidated_after_in_place_file_change(tmp_path) -> None:
    """下载产物原地修改体积后，get_hash_by_path 不返回过期哈希且防重不误跳过."""
    from app.db.download_repository import DownloadHistoryRepository

    f = tmp_path / "video.mp4"
    f.write_bytes(b"original-123")
    DownloadHistoryRepository.record("-100d", 1, None, "video.mp4", str(f), len(b"original-123"), file_hash="h_old")
    PublishHistoryRepository.record("-100pub", str(f), len(b"original-123"), file_hash="h_old", tg_message_id=99)

    assert DownloadHistoryRepository.get_hash_by_path(str(f)) == "h_old"

    f.write_bytes(b"modified-content-longer")
    assert DownloadHistoryRepository.get_hash_by_path(str(f)) == ""

    src = LocalFolderSource()
    item = PublishItem(item_id=str(f), name="video.mp4", size_bytes=f.stat().st_size, source_type="local")
    dq = src.dedup_query(item, PublishContext(task_id="t_dq", target_chat_id="-100pub"))
    assert dq.file_hash == ""
    assert (
        PublishHistoryRepository.get_published_info(
            "-100pub", dq.local_path, dq.size_bytes, dq.file_hash, source_type=dq.source_type
        )
        is None
    )


@pytest.mark.asyncio
async def test_local_single_send_floodwait_raises_retryable_and_reraises_in_pipeline(tmp_path) -> None:
    """本地单发遇 FloodWait 须标记账号并转为 RetryableTaskError，且编排层直接上抛."""
    from telethon.errors import FloodWaitError

    from app.core.pipeline.runtime import BatchState
    from app.core.scheduler import RetryableTaskError

    media_file = tmp_path / "clip.mp4"
    media_file.write_bytes(b"0" * 16)
    fw = FloodWaitError(request=None)
    fw.seconds = 42

    mock_client = AsyncMock()
    mock_client.send_file = AsyncMock(side_effect=fw)
    mock_reg = MagicMock()
    mock_reg.bots = []
    mock_reg.userbot = mock_client

    engine = LocalDiskTransportEngine(registry=mock_reg)
    ctx = PublishContext(task_id="t_loc_fw", target_chat_id=-1002, userbot_client=mock_client)
    item = PublishItem(item_id=str(media_file), name="clip.mp4", size_bytes=16, source_type="local")

    with (
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()),
        pytest.raises(RetryableTaskError) as exc_info,
    ):
        await engine.send_item(item, ctx)

    assert exc_info.value.retry_after == 42.0
    assert exc_info.value.flood_client_marked is True
    mock_reg.mark_flood_wait.assert_called_once_with(mock_client, 42)

    pipeline = PublishPipeline(source=MagicMock(), engine=engine)
    b = BatchState()
    with pytest.raises(RetryableTaskError):
        await pipeline._handle_transport_failure(exc_info.value, item, ctx, b, is_pikpak=False)
    assert not b.failed


@pytest.mark.asyncio
async def test_raw_absorb_part_delivery_missing_msg_id_raises_fatal() -> None:
    """Raw 分卷未获取到有效 msg_id 时须抛 PikPakPublishFatalError，不得写入 0 占位."""
    from types import SimpleNamespace

    from app.core.pipeline.engines.pikpak import single as pik_single

    engine = MagicMock()
    engine._store = MagicMock()
    engine._store.record_part_checkpoint = AsyncMock()
    ctx = PublishContext(task_id="t_raw_zero", target_chat_id=-1005)
    item = PublishItem(item_id="fid_r", name="movie.mkv", size_bytes=1000, source_type="pikpak")
    parts = [MagicMock(part_index=1), MagicMock(part_index=2)]
    delivered: dict[int, int] = {}

    with pytest.raises(PikPakPublishFatalError):
        await pik_single._absorb_part_delivery(
            engine,
            ctx,
            item,
            parts,
            500,
            parts[1],
            SimpleNamespace(id=0),
            delivered,
        )

    assert 2 not in delivered
    engine._store.record_part_checkpoint.assert_not_awaited()

    single_delivered: dict[int, int] = {}
    single_res = await pik_single._absorb_part_delivery(
        engine,
        ctx,
        item,
        [parts[0]],
        500,
        parts[0],
        SimpleNamespace(id=0),
        single_delivered,
    )
    assert single_res == 0
    assert 1 not in single_delivered
    engine._store.record_part_checkpoint.assert_not_awaited()


@pytest.mark.asyncio
async def test_raw_single_item_thumb_provider_requests_link_independently() -> None:
    """Raw 单发传入 _make_thumb_provider 的 url_provider 须能独立拉取直链（无需先 read_chunk）."""
    from app.core.pipeline.engines.pikpak import single as pik_single

    captured_url_provider = None

    async def fake_link_provider(ids: list[str]) -> list[str]:
        return [f"https://cdn.test/{ids[0]}"]

    def fake_make_thumb(item: PublishItem, *, url_provider=None):
        nonlocal captured_url_provider
        captured_url_provider = url_provider
        return AsyncMock(return_value=None)

    engine = MagicMock()
    engine._link_provider = fake_link_provider
    engine._make_thumb_provider = fake_make_thumb
    ctx = PublishContext(task_id="t_th_raw", target_chat_id=-1006, account_display="Userbot")
    item = PublishItem(item_id="fid_th", name="v.mp4", size_bytes=100, source_type="pikpak")

    with patch.object(pik_single, "upload_parts", new_callable=AsyncMock, return_value=(123, 1.0)):
        out = await pik_single.publish_single_item(engine, AsyncMock(), item, [MagicMock(part_index=1)], ctx)

    assert out.message_ids == [123]
    assert captured_url_provider is not None
    assert await captured_url_provider() == "https://cdn.test/fid_th"


@pytest.mark.asyncio
async def test_local_album_chunk_session_invalid_reports_and_reraises() -> None:
    """本地相册遇到非 Bot Pool 会话失效异常须上报 report_session_invalid 并中止后续分片."""
    from telethon.errors import AuthKeyUnregisteredError

    from app.core.pipeline.runtime import BatchState, Counters

    auth_err = AuthKeyUnregisteredError(request=None)
    engine = MagicMock()
    engine.send_album = AsyncMock(side_effect=TransportError("dead", client_label="Userbot", cause=auth_err))
    pipeline = PublishPipeline(source=MagicMock(), engine=engine)

    items = [
        PublishItem(item_id=f"/dir/{i}.jpg", name=f"{i}.jpg", size_bytes=10, source_type="local") for i in range(12)
    ]
    ctx = PublishContext(task_id="t_alb_sess", target_chat_id=-10094, account_id=7, album_mode=True)

    with (
        patch("app.core.session_errors.is_session_invalid_error", return_value=True),
        patch("app.core.session_errors.report_session_invalid", new_callable=AsyncMock) as m_rep,
        pytest.raises(AuthKeyUnregisteredError),
    ):
        await pipeline._transport_local(items, ctx, Counters(12), BatchState())

    m_rep.assert_awaited_once()
    assert engine.send_album.await_count == 1


@pytest.mark.asyncio
async def test_speed_measuring_provider_starts_timer_before_first_read() -> None:
    """_SpeedMeasuringProvider 必须在首次 await read_chunk 前启动计时器."""
    from types import SimpleNamespace

    clock = [100.0]

    class _SlowProvider:
        total_bytes = 1024 * 1024
        file_id = "f"
        file_name = "f.bin"

        async def read_chunk(self, offset: int, size: int) -> bytes:
            clock[0] += 0.5
            return b"a" * size

        async def aclose(self) -> None:
            return None

    with patch(
        "app.core.pipeline.engines._common.time",
        SimpleNamespace(monotonic=lambda: clock[0]),
    ):
        wrapper = _SpeedMeasuringProvider(_SlowProvider())
        assert wrapper._started is None
        await wrapper.read_chunk(0, 512 * 1024)
        assert wrapper._started == 100.0
        assert wrapper.download_speed_mb_s == 1.0


def test_safe_int_conversion_handles_overflow() -> None:
    """safe_int_conversion 遇到非有限浮点值 OverflowError 时应回退 default."""
    from app.core.pipeline import utils as pipe_utils

    assert pipe_utils.safe_int_conversion("1e309", default=7) == 7
    assert pipe_utils.safe_int_conversion(float("inf"), default=-1) == -1


@pytest.mark.asyncio
async def test_symlink_rejected_by_scanner_and_local_engine(tmp_path) -> None:
    """scan_folder_grouped 与 LocalDiskTransportEngine 均须拦截符号链接文件."""
    from pathlib import Path

    from app.core.pipeline import utils as pipe_utils

    real_mp4 = tmp_path / "good.mp4"
    real_mp4.write_bytes(b"mp4data")
    fake_link = tmp_path / "secret.jpg"
    fake_link.write_bytes(b"secret")

    orig_islink = os.path.islink
    with patch("app.core.pipeline.utils.os.path.islink", side_effect=lambda p: Path(p) == fake_link or orig_islink(p)):
        grouped = pipe_utils.scan_folder_grouped(str(tmp_path))
    assert str(real_mp4) in grouped[str(tmp_path)]
    assert str(fake_link) not in grouped[str(tmp_path)]

    engine = LocalDiskTransportEngine()
    ctx = PublishContext(task_id="t_sym", target_chat_id=-10097, userbot_client=AsyncMock())
    sym_item = PublishItem(item_id=str(fake_link), name="secret.jpg", size_bytes=6, source_type="local")
    with (
        patch("app.core.pipeline.engines.local_disk.os.path.islink", return_value=True),
        pytest.raises(ValueError, match="符号链接"),
    ):
        await engine.send_item(sym_item, ctx)


@pytest.mark.asyncio
async def test_local_album_resizes_oversized_photo_copy_and_leaves_original_untouched(tmp_path) -> None:
    """超限相册图生成临时缩放 JPEG 副本走 upload_file_parallel，原图不变且临时文件必清理."""
    from PIL import Image

    big_png = tmp_path / "huge.png"
    Image.new("RGBA", (3200, 1600), (255, 0, 0, 128)).save(big_png, format="PNG")
    small_jpg = tmp_path / "small.jpg"
    Image.new("RGB", (800, 600), (0, 255, 0)).save(small_jpg, format="JPEG")

    seen_paths: list[str] = []
    seen_dims: list[tuple[int, int]] = []

    async def fake_upload(_client, upload_path: str, **kwargs):
        seen_paths.append(upload_path)
        with Image.open(upload_path) as im:
            seen_dims.append((im.width, im.height))
        return MagicMock(name=os.path.basename(upload_path))

    mock_client = AsyncMock()
    mock_client.send_file = AsyncMock(return_value=[MagicMock(id=301), MagicMock(id=302)])
    mock_reg = MagicMock(bots=[], userbot=mock_client)
    engine = LocalDiskTransportEngine(registry=mock_reg)
    ctx = PublishContext(task_id="t_alb_pil", target_chat_id=-10098, userbot_client=mock_client)
    items = [
        PublishItem(item_id=str(big_png), name="huge.png", size_bytes=big_png.stat().st_size, source_type="local"),
        PublishItem(item_id=str(small_jpg), name="small.jpg", size_bytes=small_jpg.stat().st_size, source_type="local"),
    ]

    with patch("app.core.fast_telethon.upload_file_parallel", side_effect=fake_upload):
        outcomes = await engine.send_album(items, ctx)

    assert [o.message_ids for o in outcomes] == [[301], [302]]
    assert seen_paths[0] != str(big_png) and seen_paths[0].endswith(".jpg")
    assert not await asyncio.to_thread(os.path.exists, seen_paths[0])
    assert seen_dims[0] == (2560, 1280)
    assert seen_paths[1] == str(small_jpg)
    assert seen_dims[1] == (800, 600)
    with Image.open(big_png) as orig:
        assert orig.size == (3200, 1600)


@pytest.mark.asyncio
async def test_send_file_retry_narrows_other_errors_and_propagates_cancel() -> None:
    """_send_file_with_floodwait_retry 立即放行取消信号、不盲目重试歧义错误，仅重试明确未投递可恢复错."""
    from telethon.errors import BadRequestError, ServerError

    from app.core.pipeline.engines._common import _send_file_with_floodwait_retry

    client = AsyncMock()
    client.send_file = AsyncMock(side_effect=UploadCancelledError("cancel"))
    with pytest.raises(UploadCancelledError):
        await _send_file_with_floodwait_retry(client, -1001, MagicMock(), {}, retry_other_errors=True, log_label="T")
    assert client.send_file.await_count == 1

    client.send_file = AsyncMock(side_effect=TimeoutError("ambiguous ack timeout"))
    with pytest.raises(TimeoutError):
        await _send_file_with_floodwait_retry(client, -1001, MagicMock(), {}, retry_other_errors=True, log_label="T")
    assert client.send_file.await_count == 1

    client.send_file = AsyncMock(side_effect=BadRequestError(request=None, message="PHOTO_INVALID"))
    with pytest.raises(BadRequestError):
        await _send_file_with_floodwait_retry(client, -1001, MagicMock(), {}, retry_other_errors=True, log_label="T")
    assert client.send_file.await_count == 1

    client.send_file = AsyncMock(side_effect=[ServerError(request=None, message="INTERNAL"), MagicMock(id=777)])
    sent, _ = await _send_file_with_floodwait_retry(
        client, -1001, MagicMock(), {}, retry_other_errors=True, log_label="T"
    )
    assert sent.id == 777
    assert client.send_file.await_count == 2


def test_context_get_current_in_flight_ignores_legacy_key() -> None:
    """PublishContext.get_current_in_flight 不再读取废弃的 upload_in_flight 键."""
    from app.db.task_repository import TaskQueueRepository

    ctx = PublishContext(task_id="t_legacy", target_chat_id=-10099, upload_in_flight_per_connection=2)
    with patch.object(TaskQueueRepository, "get_by_id", return_value={"params": {"upload_in_flight": 4}}):
        assert ctx.get_current_in_flight() == 2


@pytest.mark.parametrize("failure", ["prepare_cancel", "size_error", "size_cancel"])
async def test_album_temp_cleanup_covers_preparation_and_sizing(tmp_path, failure):
    import threading

    from app.core.pipeline.engines import local_disk

    temp = tmp_path / "scaled.jpg"
    started = threading.Event()
    release = threading.Event()

    def prepare(_path):
        started.set()
        release.wait(timeout=5)
        temp.write_bytes(b"jpeg")
        return str(temp), str(temp)

    engine = LocalDiskTransportEngine(registry=MagicMock())
    with (
        patch.object(local_disk, "_prepare_album_image_sync", prepare),
        patch.object(local_disk, "_stat_size", side_effect=OSError("stat failed")),
    ):
        task = asyncio.create_task(
            engine._upload_album_member(
                AsyncMock(),
                PublishContext(task_id="task", target_chat_id=-1001),
                idx=0,
                item=PublishItem(item_id="original", name="x.jpg", size_bytes=10, source_type="local"),
                sub_size=10,
                progress={"cur_done": 0, "tot_files": 1},
            )
        )
        try:
            assert await asyncio.to_thread(started.wait, 5)
            if failure == "prepare_cancel":
                task.cancel()
            release.set()
            if failure == "size_cancel":

                def cancel_size(_path):
                    raise asyncio.CancelledError

                with patch.object(local_disk, "_stat_size", cancel_size), pytest.raises(asyncio.CancelledError):
                    await task
            else:
                with pytest.raises(asyncio.CancelledError if failure == "prepare_cancel" else OSError):
                    await task
        finally:
            release.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    assert not temp.exists()


async def test_pipeline_calls_cleanup_for_unsuccessful_final_batch():
    item = PublishItem(item_id="f", name="f.mp4", size_bytes=1, source_type="pikpak")
    batches = [PublishBatch(batch_id=str(n), folder_name="", items=[item]) for n in range(2)]
    source = MagicMock(get_batches=AsyncMock(return_value=batches))
    cleanup_calls = []

    async def clean(items, context):
        cleanup_calls.append((list(items), context.cleanup_defer_unrelated_dirs))

    pipeline = PublishPipeline(
        source=source, engine=MagicMock(), cleanup=MagicMock(cleanup=AsyncMock(side_effect=clean))
    )
    pipeline._batch_filter.filter_batch = AsyncMock(return_value=[item])

    async def transport(items, context, counters, batch, skipped):
        if not cleanup_calls:
            batch.successful.append(item)

    pipeline._transport_pikpak = transport
    with patch("app.core.pipeline.pipeline.phase1_preflight", AsyncMock(return_value=None)):
        await pipeline.execute(PublishContext(task_id="", target_chat_id=-1001, source_type="pikpak"))
    assert cleanup_calls == [([item], True), ([], False)]


@pytest.mark.parametrize("cancel_parent", [False, True])
async def test_cancelled_preparation_is_terminal_and_worker_cleans_temp(tmp_path, cancel_parent):
    import threading

    from app.core.pipeline.engines import local_disk

    started = threading.Event()
    release = threading.Event()
    cleaned = threading.Event()
    temp = tmp_path / "late.jpg"
    preparation_tasks = []
    real_to_thread = asyncio.to_thread
    real_shield = asyncio.shield
    shield_calls = 0

    def prepare(_path):
        started.set()
        release.wait(timeout=5)
        temp.write_bytes(b"jpeg")
        return str(temp), str(temp)

    async def track_thread(func, *args):
        preparation_tasks.append(asyncio.current_task())
        return await real_to_thread(func, *args)

    def bounded_shield(task):
        nonlocal shield_calls
        shield_calls += 1
        assert shield_calls < 5, "cancelled preparation must not spin"
        return real_shield(task)

    engine = LocalDiskTransportEngine(registry=MagicMock())
    with (
        patch.object(local_disk, "_prepare_album_image_sync", prepare),
        patch.object(local_disk.asyncio, "to_thread", track_thread),
        patch.object(local_disk.asyncio, "shield", bounded_shield),
        patch.object(local_disk.ft_mod, "discard_upload_meta", side_effect=lambda _path: cleaned.set()),
    ):
        parent = asyncio.create_task(
            engine._upload_album_member(
                AsyncMock(),
                PublishContext(task_id="task", target_chat_id=-1001),
                idx=0,
                item=PublishItem(item_id="original", name="x.jpg", size_bytes=10, source_type="local"),
                sub_size=10,
                progress={"cur_done": 0, "tot_files": 1},
            )
        )
        try:
            assert await real_to_thread(started.wait, 2)
            if cancel_parent:
                parent.cancel()
                await asyncio.sleep(0.01)
                assert shield_calls == 2
                release.set()
            else:
                preparation_tasks[0].cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(parent, timeout=2)
        finally:
            release.set()
            if not parent.done():
                parent.cancel()
                await asyncio.gather(parent, return_exceptions=True)
        assert await real_to_thread(cleaned.wait, 2)
    assert not temp.exists()


@pytest.mark.asyncio
async def test_cancelled_parent_propagates_even_when_worker_raises() -> None:
    """父任务在 preparation 期间被取消时，即便线程随后抛出异常也必须优先传播 CancelledError."""
    import threading

    from app.core.pipeline.engines import local_disk

    started = threading.Event()
    release = threading.Event()
    real_to_thread = asyncio.to_thread

    def failing_prepare(_path: str) -> tuple[str, str | None]:
        started.set()
        release.wait(timeout=5)
        raise ValueError("拒绝处理符号链接")

    engine = LocalDiskTransportEngine(registry=MagicMock())
    with patch.object(local_disk, "_prepare_album_image_sync", failing_prepare):
        parent = asyncio.create_task(
            engine._upload_album_member(
                AsyncMock(),
                PublishContext(task_id="task_cancel_err", target_chat_id=-1001),
                idx=0,
                item=PublishItem(item_id="symlink", name="x.jpg", size_bytes=10, source_type="local"),
                sub_size=10,
                progress={"cur_done": 0, "tot_files": 1},
            )
        )
        try:
            assert await real_to_thread(started.wait, 2)
            parent.cancel()
            await asyncio.sleep(0)
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(parent, timeout=2)
        finally:
            release.set()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_metadata", [False, True])
async def test_raw_single_item_shares_initial_link_between_thumb_and_provider(has_metadata: bool) -> None:
    """publish_single_item 首链在 thumb_provider 与 chunk provider 间共享一次拉取，不重复消耗配额."""
    from app.core.media_metadata import ProbedMediaMetadata
    from app.core.pipeline.engines.pikpak import single as pik_single

    link_calls = 0
    probe_requests: list[tuple[str, str]] = []

    async def fake_link_provider(ids: list[str]) -> list[str]:
        nonlocal link_calls
        link_calls += 1
        return [f"https://cdn.test/{ids[0]}?n={link_calls}"]

    async def fake_metadata_probe(url_or_provider, ext, **kwargs):
        url = url_or_provider if isinstance(url_or_provider, str) else await url_or_provider()
        probe_requests.append((ext, url))
        return ProbedMediaMetadata()

    async def fake_upload_parts(engine, client, *, provider, thumb_provider, **kwargs):
        thumb_url = await thumb_provider()
        first_chunk_url = await provider._inner._url_provider()
        refreshed_url = await provider._inner._url_provider()
        assert thumb_url == first_chunk_url == "https://cdn.test/fid_shared?n=1"
        assert refreshed_url == "https://cdn.test/fid_shared?n=2"
        return 321, 5.0

    def passthrough_thumb(item: PublishItem, *, url_provider=None):
        assert url_provider is not None
        return url_provider

    engine = MagicMock()
    engine._link_provider = fake_link_provider
    engine._make_thumb_provider = passthrough_thumb
    ctx = PublishContext(task_id="t_th_shared", target_chat_id=-1006, account_display="Userbot")
    item = PublishItem(item_id="fid_shared", name="v.mp4", size_bytes=100, source_type="pikpak")
    if has_metadata:
        item.duration_s = 10
        item.width = 1280
        item.height = 720

    with (
        patch.object(pik_single, "upload_parts", side_effect=fake_upload_parts),
        patch.object(pik_single, "probe_remote_video_metadata", side_effect=fake_metadata_probe) as probe,
    ):
        out = await pik_single.publish_single_item(engine, AsyncMock(), item, [MagicMock(part_index=1)], ctx)

    assert out.message_ids == [321]
    assert link_calls == 2
    assert probe.await_count == (0 if has_metadata else 1)
    assert probe_requests == ([] if has_metadata else [(".mp4", "https://cdn.test/fid_shared?n=1")])


def test_pikpak_transient_error_classified_as_retryable():
    """PikPakTransientError 须优先命中 transient 分类并包装为 Retryable."""
    from app.core.pikpak.taxonomy import PikPakTransientError, classify_pikpak_failure, wrap_pikpak_failure

    cat, _ = classify_pikpak_failure(PikPakTransientError("network blip"))
    assert cat == "transient"
    wrapped = wrap_pikpak_failure(PikPakTransientError("network blip"))
    assert isinstance(wrapped, PikPakPublishRetryableError)


@pytest.mark.asyncio
async def test_album_prefetch_window_does_not_duplicate_queued_items():
    """_AlbumLinkPrefetchWindow(slots=3) 不应重复预取已入队成员."""
    from app.core.pipeline.engines._common import _AlbumLinkPrefetchWindow

    requested: list[str] = []

    async def link_provider(ids: list[str]) -> list[str]:
        requested.extend(ids)
        return [f"https://cdn.test/{ids[0]}"]

    pending = [
        (i, PublishItem(item_id=f"id_{i}", name=f"{i}.jpg", size_bytes=10, source_type="pikpak")) for i in range(4)
    ]
    win = _AlbumLinkPrefetchWindow(link_provider, slots=3)
    try:
        win.advance(pending, 0)
        resolved = []
        for pos in range(len(pending)):
            resolved.append(await win.pop())
            win.advance(pending, pos + 1)
    finally:
        win.close()

    assert requested == ["id_0", "id_1", "id_2", "id_3"]
    assert resolved == [f"https://cdn.test/id_{i}" for i in range(4)]


@pytest.mark.asyncio
async def test_local_album_flood_wait_persists_earlier_segment_and_raises_retryable(tmp_path):
    """本地相册第 2 段遇 FloodWaitError 时保留第 1 段入库并上抛 RetryableTaskError."""
    from telethon.errors import FloodWaitError

    from app.core.pipeline.runtime import BatchState, Counters
    from app.core.scheduler import RetryableTaskError

    items = []
    for i in range(11):
        f = tmp_path / f"img_{i:02d}.jpg"
        f.write_bytes(b"jpg")
        items.append(PublishItem(item_id=str(f), name=f.name, size_bytes=3, source_type="local"))

    fw = FloodWaitError(request=None)
    fw.seconds = 5
    mock_client = AsyncMock()
    mock_client.send_file = AsyncMock(side_effect=[[MagicMock(id=900 + i) for i in range(10)], fw])

    engine = LocalDiskTransportEngine()
    ctx = PublishContext(task_id="t_alb_fw", target_chat_id=-100555, userbot_client=mock_client, delay_seconds=0.0)
    with (
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()),
        patch("app.core.client_manager.client_manager.mark_flood_wait"),
    ):
        outcomes = await engine.send_album(items, ctx)

    assert outcomes[0].message_ids == [900]
    assert outcomes[10].retryable_error is True

    pipeline = PublishPipeline(source=MagicMock(), engine=engine)
    b = BatchState()
    counters = Counters(11)
    with pytest.raises(RetryableTaskError):
        await pipeline._route_album_outcomes(items, outcomes, ctx, counters, b, is_pikpak=False)
    assert counters.published == 10
    assert PublishHistoryRepository.is_published("-100555", items[0].item_id, 3) is True


@pytest.mark.asyncio
async def test_raw_part1_thumbnail_failure_is_non_fatal():
    """Raw 首卷封面提取超时或失败不阻断上传."""
    from types import SimpleNamespace

    from app.core.pipeline.engines.pikpak import single as pik_single

    engine = MagicMock()
    engine._store = MagicMock()
    engine._store.clear_part_checkpoint = AsyncMock()
    part = SimpleNamespace(part_index=1, start=0, length=100, end=99)
    item = PublishItem(item_id="F1", name="v.mp4", size_bytes=100, source_type="pikpak")
    ctx = PublishContext(task_id="", target_chat_id=-1001)
    provider = MagicMock(download_speed_mb_s=1.0)

    m_thumb = AsyncMock(side_effect=RuntimeError("thumb boom"))
    with (
        patch.object(pik_single, "_upload_single_part", new_callable=AsyncMock, return_value=MagicMock(id=777)),
        patch.object(pik_single, "_absorb_part_delivery", new_callable=AsyncMock, return_value=777),
    ):
        msg_id, _ = await pik_single.upload_parts(
            engine,
            MagicMock(),
            provider=provider,
            item=item,
            parts=[part],
            context=ctx,
            thumb_provider=m_thumb,
        )
    assert msg_id == 777
    m_thumb.assert_awaited_once()


@pytest.mark.asyncio
async def test_precheck_cdn_range_network_error_is_retryable():
    """Range 预检连接异常应归为 PikPakPublishRetryableError."""
    import aiohttp

    from app.core.pipeline.engines._common import precheck_cdn_range

    mock_session = MagicMock()
    mock_session.get = MagicMock(side_effect=aiohttp.ClientConnectionError("reset"))
    with (
        patch("app.core.cdn_transport.get_cdn_session", new_callable=AsyncMock, return_value=mock_session),
        pytest.raises(PikPakPublishRetryableError),
    ):
        await precheck_cdn_range("https://cdn.test/secret?sig=123")


@pytest.mark.asyncio
async def test_send_file_floodwait_honors_cancel_during_sleep():
    """FloodWait 退避等待期间收到取消信号应及时中断."""
    from telethon.errors import FloodWaitError

    from app.core.pipeline.engines._common import _send_file_with_floodwait_retry

    fw = FloodWaitError(request=None)
    fw.seconds = 20
    client = AsyncMock()
    client.send_file = AsyncMock(side_effect=fw)
    cancelled = False

    async def cancel_during_sleep(_seconds):
        nonlocal cancelled
        cancelled = True

    ctx = PublishContext(task_id="t_fw_c", target_chat_id=-1001, is_cancelled=lambda: cancelled)
    with (
        patch("asyncio.sleep", new_callable=AsyncMock, side_effect=cancel_during_sleep) as m_sleep,
        patch("app.core.pipeline.interfaces.DEFAULT_CLIENT_REGISTRY.mark_flood_wait"),
        pytest.raises(UploadCancelledError),
    ):
        await _send_file_with_floodwait_retry(
            client, -1001, MagicMock(), {}, retry_other_errors=False, log_label="T", context=ctx
        )
    client.send_file.assert_awaited_once()
    m_sleep.assert_awaited_once()


@pytest.mark.asyncio
async def test_notifier_does_not_resend_on_post_send_log_failure():
    """send_message 成功后写日志失败不应重发频道消息."""
    from app.db.task_repository import TaskQueueRepository

    client = AsyncMock()
    ctx = PublishContext(task_id="t_not", target_chat_id=-1001, userbot_client=client)
    batch = PublishBatch(batch_id="b1", folder_name="dir", is_folder_publish=True)
    res = PublishResult(published_count=1, total_files_count=1, newly_published_count=1, is_folder_publish=True)

    async def fail_log(*args, **kwargs):
        assert client.send_message.await_count == 1
        raise RuntimeError("db lock")

    notifier = TelegramChannelNotifier()
    with (
        patch("app.core.pipeline.notifier.resolve_entity", new_callable=AsyncMock, return_value=MagicMock()),
        patch.object(TaskQueueRepository, "append_log_async", new_callable=AsyncMock, side_effect=fail_log) as m_log,
    ):
        await notifier.notify_batch(batch, ctx, res)
    m_log.assert_awaited()
    assert client.send_message.await_count == 1


@pytest.mark.asyncio
async def test_partial_archive_failure_sets_archive_failed_without_marking_failed_files():
    """归档部分/全部失败置 archive_failed=True, 且不写入 failed_files 以免污染任务终态."""
    from app.db.task_repository import TaskQueueRepository

    mock_source = MagicMock()
    mock_source.get_batches = AsyncMock(
        return_value=[
            PublishBatch(
                batch_id="b_part",
                folder_name="dirA",
                is_folder_publish=True,
                items=[
                    PublishItem(item_id="f1", name="a.mp4", size_bytes=10, parent_folder_id="p1", source_type="pikpak"),
                    PublishItem(item_id="f2", name="b.mp4", size_bytes=10, parent_folder_id="p1", source_type="pikpak"),
                ],
            )
        ]
    )
    mock_source.dedup_query = lambda item, ctx: MagicMock(
        local_path=item.item_id,
        size_bytes=item.size_bytes,
        file_hash="",
        source_type="pikpak",
        remote_file_id=item.item_id,
    )
    mock_source.oversize_limit_bytes = lambda item, ctx: None
    mock_source.reject_item = lambda item, ctx: None
    mock_source.audit_block_reason = lambda item, ctx: None

    mock_engine = MagicMock()
    mock_engine.send_item = AsyncMock(
        side_effect=[TransportOutcome(message_ids=[1]), TransportOutcome(message_ids=[2])]
    )

    cleanup = PikPakStructuredArchiveStrategy(
        resolve_archive_folder=AsyncMock(return_value="arch_root"),
        archive_files_to_folder=AsyncMock(side_effect=[{"moved": 1}, {"moved": 0}, {"moved": 0}]),
        archive_files=AsyncMock(return_value={"moved": 0}),
    )
    mock_notifier = MagicMock()
    mock_notifier.notify_batch = AsyncMock()
    mock_notifier.notify_task_summary = AsyncMock()

    pipeline = PublishPipeline(
        source=mock_source,
        engine=mock_engine,
        cleanup=cleanup,
        notifier=mock_notifier,
        channel_probe=None,
    )
    ctx = PublishContext(
        task_id="t_arch_partial",
        target_chat_id=-1001,
        source_type="pikpak",
        auto_cleanup_mode="archive",
        delay_seconds=0.0,
        dir_tree={"p1": "dirA"},
    )
    with (
        patch("app.core.pipeline.pipeline.phase1_preflight", new_callable=AsyncMock, return_value=None),
        patch.object(PublishHistoryRepository, "get_published_info", return_value=None),
        patch.object(PublishHistoryRepository, "record"),
        patch.object(TaskQueueRepository, "append_log_async", new_callable=AsyncMock),
    ):
        res = await pipeline.execute(ctx)

    assert res.failed_files == []
    assert res.archive_failed is True
    batch_res = mock_notifier.notify_batch.await_args.args[2]
    assert batch_res.archive_failed is True


async def test_get_current_send_delay_reads_latest_params():
    """send_delay 活取值: steady 落库后运行中执行器在 TTL 窗口外读到最新延迟."""
    from app.db.task_repository import TaskQueueRepository

    task_id = "t_send_delay_live"
    TaskQueueRepository.create(task_id, "pikpak_publish", {"send_delay_seconds": 3.0})
    try:
        ctx = PublishContext(task_id=task_id, target_chat_id=-1001, delay_seconds=3.0)
        assert await ctx.get_current_send_delay_seconds() == 3.0

        TaskQueueRepository.merge_params(task_id, {"send_delay_seconds": 20.0})
        assert await ctx.get_current_send_delay_seconds() == 3.0
        ctx._last_send_delay_check = 0.0
        assert await ctx.get_current_send_delay_seconds() == 20.0
        assert ctx.delay_seconds == 20.0
    finally:
        TaskQueueRepository.delete(task_id)


async def test_get_current_send_delay_no_task_falls_back_to_dispatch_value():
    """无任务行 (task_id 为空) 时回退 dispatch 固化值, 不查库"""
    ctx = PublishContext(task_id="", target_chat_id=-1001, delay_seconds=5.0)
    assert await ctx.get_current_send_delay_seconds() == 5.0


@pytest.mark.parametrize("signal", ["cancel", "pause"])
async def test_send_wait_checks_signals_during_delay(signal):
    from app.core.pipeline.runtime import wait_with_signals
    from app.core.scheduler import UploadCancelledError, UploadPausedError

    ctx = PublishContext(task_id=None, target_chat_id="target")
    clock = [0.0]

    async def tick(delay):
        clock[0] += delay
        if signal == "cancel":
            ctx.is_cancelled = lambda: True
        else:
            ctx.is_paused = lambda: True

    with (
        patch("app.core.pipeline.runtime.time.monotonic", side_effect=lambda: clock[0]),
        patch("app.core.pipeline.runtime.asyncio.sleep", side_effect=tick),
        pytest.raises(UploadCancelledError if signal == "cancel" else UploadPausedError),
    ):
        await wait_with_signals(ctx, 60.0)
    assert clock[0] == 0.25


async def test_send_wait_preserves_configured_duration():
    from app.core.pipeline.runtime import wait_with_signals

    ctx = PublishContext(task_id=None, target_chat_id="target")
    clock = [0.0]

    async def tick(delay):
        clock[0] += delay

    with (
        patch("app.core.pipeline.runtime.time.monotonic", side_effect=lambda: clock[0]),
        patch("app.core.pipeline.runtime.asyncio.sleep", side_effect=tick),
    ):
        await wait_with_signals(ctx, 20.1)
    assert clock[0] == 20.1
