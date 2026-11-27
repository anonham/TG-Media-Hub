"""
Contract and Resilience Tests for PikPakStreamTransportEngine fMP4 Dispatch Path

1. Never silent fallback to raw slice when strategy == "fmp4_universal"
2. Atomic rollback of delivered orphan messages on failure
3. JIT relink & per-part retry on failure (up to 3 attempts)
4. Telegram FloodWaitError compliant backoff & client_manager notification
5. Real audio codec validation (copy_audio only when AAC + MP4/MOV)
6. CDN HTTP Range 206 precheck & 200 rejection
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telethon.errors import FloodWaitError

from app.core.fmp4_pipe_uploader import (
    PART_SIZE,
    PartDurationInflatedError,
    PartOversizeError,
    PartTruncatedError,
)
from app.core.iso_bmff.fragmented import Fmp4PartPlan, plan_video_fmp4_parts
from app.core.keyframe_prober import CuesProbeResult, MkvCuesProber, Mp4StssProber
from app.core.pikpak.taxonomy import PikPakPublishFatalError
from app.core.pipeline.engines import PikPakStreamTransportEngine, _maybe_log_codec_compat_hint
from app.core.pipeline.models import BreathingPolicy, PublishContext, PublishItem


@pytest.fixture(autouse=True)
def _mock_remote_media_probers(monkeypatch):
    """隔离远程媒体探测与缩略图抽取，避免单测访问伪造 CDN 触发网络/FFmpeg 超时。"""
    monkeypatch.setattr("app.core.keyframe_prober.Mp4StssProber.probe_from_url", AsyncMock(return_value=None))
    monkeypatch.setattr("app.core.keyframe_prober.MkvCuesProber.probe_from_url", AsyncMock(return_value=None))
    monkeypatch.setattr("app.core.pikpak.thumbnail.fetch_thumbnail", AsyncMock(return_value=None))
    monkeypatch.setattr("app.core.pikpak.thumbnail.extract_frame_ffmpeg", AsyncMock(return_value=None))


def _make_context(
    task_id="task_test_fmp4",
    target_chat_id=-1009000000010,
    userbot_client=None,
    split_strategy="fmp4_universal",
    split_threshold_bytes=2000 * 1024 * 1024,
    **kwargs,
) -> PublishContext:
    # 测试零等待呼吸/退避策略显式注入 (生产代码不再感知 PYTEST_CURRENT_TEST)
    kwargs.setdefault(
        "breathing",
        BreathingPolicy(min_floor_s=0.0, jitter_s=0.0, poll_s=0.01, retry_backoff_s=(0.0, 0.0), enabled=False),
    )
    return PublishContext(
        task_id=task_id,
        target_chat_id=target_chat_id,
        target_channel_raw=str(target_chat_id),
        is_folder_publish=False,
        folder_name="",
        dir_tree={},
        auto_cleanup_mode="none",
        userbot_client=userbot_client,
        split_strategy=split_strategy,
        video_split_threshold_bytes=split_threshold_bytes,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_fmp4_never_silent_fallback_to_raw():
    """Requirement 2.1: fMP4 failure must NEVER fall back to raw binary slicing."""
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/video.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    item = PublishItem(
        item_id="fid_large",
        name="large.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    ctx = _make_context(userbot_client=mock_client, split_strategy="fmp4_universal")

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch(
            "app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram",
            side_effect=PartTruncatedError("Truncated", 10.0, 50.0),
        ),
        patch.object(engine, "_publish_single_item", new_callable=AsyncMock) as mock_raw,
    ):
        with pytest.raises(PikPakPublishFatalError) as exc_info:
            await engine.send_item(item, ctx)

        assert exc_info.value.category == "fmp4_part_failed"
        # Assert raw fallback was NEVER called
        mock_raw.assert_not_called()


@pytest.mark.asyncio
async def test_fmp4_atomic_rollback_on_failure():
    """Requirement 2.2: Delete delivered messages if any subsequent part fails."""
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/video.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    item = PublishItem(
        item_id="fid_large",
        name="large.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    # Part 1 send_file succeeds and returns msg_id 101; Part 2 raises error
    mock_client.send_file.side_effect = [
        MagicMock(id=101),
    ]
    mock_client.delete_messages = AsyncMock()
    ctx = _make_context(userbot_client=mock_client, split_strategy="fmp4_universal")

    # Mock 2 parts
    plans = [
        Fmp4PartPlan(
            part_index=1, total_parts=2, start_sec=0, end_sec=900, duration_sec=900, part_name="large.part1.mp4"
        ),
        Fmp4PartPlan(
            part_index=2, total_parts=2, start_sec=900, end_sec=1800, duration_sec=900, part_name="large.part2.mp4"
        ),
    ]

    call_count = 0

    async def fake_stream_to_telegram(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return MagicMock(), 900.0
        raise PartTruncatedError("Part 2 truncated", 100.0, 900.0)

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch(
            "app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram",
            side_effect=fake_stream_to_telegram,
        ),
    ):
        with pytest.raises(PikPakPublishFatalError):
            await engine.send_item(item, ctx)

        # Verify atomic rollback deleted message 101
        mock_client.delete_messages.assert_awaited_once_with(ctx.target_chat_id, [101])


@pytest.mark.asyncio
async def test_fmp4_jit_relink_retry_on_failure():
    """Requirement 2.3: JIT relink and retry up to 3 attempts on part failure."""
    link_urls = [
        "https://cdn.example.com/url_v1.mp4",
        "https://cdn.example.com/url_v2.mp4",
        "https://cdn.example.com/url_v3.mp4",
    ]
    mock_link_fn = AsyncMock(side_effect=[[u] for u in link_urls])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    item = PublishItem(
        item_id="fid_large",
        name="large.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=201)
    ctx = _make_context(userbot_client=mock_client, split_strategy="fmp4_universal")

    plans = [
        Fmp4PartPlan(part_index=1, total_parts=1, start_sec=0, end_sec=1800, duration_sec=1800, part_name="large.mp4"),
    ]

    call_urls = []

    async def fake_stream_to_telegram(**kwargs):
        call_urls.append(kwargs.get("pikpak_url"))
        if len(call_urls) < 3:
            raise PartTruncatedError("Retryable transient error", 10.0, 1800.0)
        return MagicMock(), 1800.0

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch(
            "app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram",
            side_effect=fake_stream_to_telegram,
        ),
    ):
        outcome = await engine.send_item(item, ctx)

        assert outcome.message_ids == [201]
        assert len(call_urls) == 3
        # First call used initial link; subsequent retry calls refreshed URL
        assert call_urls[0] == link_urls[0]
        assert call_urls[1] == link_urls[1]
        assert call_urls[2] == link_urls[2]


@pytest.mark.asyncio
async def test_fmp4_flood_wait_handling():
    """Requirement 2.4: Telegram FloodWait backoff and client_manager notification."""
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/video.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    item = PublishItem(
        item_id="fid_large",
        name="large.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )
    mock_client = AsyncMock()

    # First attempt raises FloodWaitError(2s), second attempt succeeds
    fw_err = FloodWaitError(request=None)
    fw_err.seconds = 2
    mock_client.send_file.side_effect = [fw_err, MagicMock(id=301)]
    ctx = _make_context(userbot_client=mock_client, split_strategy="fmp4_universal")

    plans = [
        Fmp4PartPlan(part_index=1, total_parts=1, start_sec=0, end_sec=1800, duration_sec=1800, part_name="large.mp4"),
    ]

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch(
            "app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram",
            return_value=(MagicMock(), 1800.0),
        ),
        patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        patch("app.core.client_manager.client_manager.mark_flood_wait") as mock_mark,
    ):
        outcome = await engine.send_item(item, ctx)

        assert outcome.message_ids == [301]
        mock_mark.assert_called_once_with(mock_client, 2)
        mock_sleep.assert_awaited_with(3)  # 2 + 1 seconds


@pytest.mark.asyncio
async def test_fmp4_audio_codec_validation():
    """Requirement 2.5: copy_audio = True only for AAC in MP4/MOV; transcode otherwise."""
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/video.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    test_cases = [
        ("test.mp4", "aac", True),
        ("test.mov", "mp4a", True),
        ("test.mp4", "ac3", False),
        ("test.mkv", "aac", False),
        ("test.mkv", "eac3", False),
        ("test.mp4", "", False),
    ]

    plans = [
        Fmp4PartPlan(part_index=1, total_parts=1, start_sec=0, end_sec=100, duration_sec=100, part_name="p1.mp4"),
    ]

    for filename, codec, expected_copy_audio in test_cases:
        item = PublishItem(
            item_id="fid_codec",
            name=filename,
            size_bytes=3000 * 1024 * 1024,
            duration_s=100,
            audio_codec=codec,
            source_type="pikpak",
        )
        mock_client = AsyncMock()
        mock_client.send_file.return_value = MagicMock(id=401)
        ctx = _make_context(userbot_client=mock_client, split_strategy="fmp4_universal")

        captured_copy_audio = None

        async def fake_stream_to_telegram(**kwargs):
            nonlocal captured_copy_audio
            captured_copy_audio = kwargs.get("copy_audio")
            return MagicMock(), 100.0

        with (
            patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
            patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
            patch(
                "app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram",
                side_effect=fake_stream_to_telegram,
            ),
        ):
            await engine.send_item(item, ctx)
            assert captured_copy_audio == expected_copy_audio, (
                f"For {filename} with codec {codec}: expected copy_audio={expected_copy_audio}, got {captured_copy_audio}"
            )


@pytest.mark.asyncio
async def test_fmp4_mp4_prober_backfills_missing_audio_codec():
    """When item.audio_codec is empty (e.g. PikPak ls output), Mp4StssProber audio_codec='aac' enables copy_audio=True."""
    from app.core.keyframe_prober import CuesProbeResult

    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/video.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    item = PublishItem(
        item_id="fid_missing_acodec",
        name="movie.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=100,
        audio_codec=None,
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=402)
    ctx = _make_context(userbot_client=mock_client, split_strategy="fmp4_universal")

    plans = [
        Fmp4PartPlan(part_index=1, total_parts=1, start_sec=0, end_sec=100, duration_sec=100, part_name="p1.mp4"),
    ]
    probe_res = CuesProbeResult(
        duration_s=100.0,
        timescale=90000,
        keyframe_pts_s=[0.0, 50.0],
        has_cues=True,
        audio_codec="aac",
        video_codec="avc1",
        width=1920,
        height=1080,
    )

    captured_copy_audio = None

    async def fake_stream_to_telegram(**kwargs):
        nonlocal captured_copy_audio
        captured_copy_audio = kwargs.get("copy_audio")
        return MagicMock(), 100.0

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.keyframe_prober.Mp4StssProber.probe_from_url", new_callable=AsyncMock, return_value=probe_res),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch(
            "app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram",
            side_effect=fake_stream_to_telegram,
        ),
    ):
        await engine.send_item(item, ctx)
        assert captured_copy_audio is True
        assert item.audio_codec == "aac"
        assert item.width == 1920
        assert item.height == 1080


@pytest.mark.asyncio
async def test_fmp4_uploader_multi_sender_dispatch_and_cleanup(monkeypatch):
    """Verify ZeroDiskFmp4StreamUploader dispatches SaveBigFilePartRequest over dedicated worker senders and cleans them up."""
    from app.core.fmp4_pipe_uploader import ZeroDiskFmp4StreamUploader

    sent_via_sender = []
    closed_senders = []

    class FakeSender:
        def __init__(self, idx):
            self.idx = idx

        def send(self, req):
            fut = asyncio.get_running_loop().create_future()
            sent_via_sender.append((self.idx, req.file_part, req.file_total_parts, len(req.bytes)))
            fut.set_result(True)
            return fut

    fake_client = MagicMock()
    uploader = ZeroDiskFmp4StreamUploader(
        client=fake_client,
        max_workers=2,
        queue_size=8,
        in_flight_per_connection=2,
    )

    monkeypatch.setattr(uploader, "_is_real_telethon_client", lambda: True)

    async def fake_init_single(idx):
        return FakeSender(idx)

    monkeypatch.setattr(uploader, "_init_single_sender", fake_init_single)
    monkeypatch.setattr(
        "app.core.fast_telethon.sender_pool._close_worker_sender",
        AsyncMock(side_effect=lambda c, s, dc: closed_senders.append(s.idx)),
    )

    custom_cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=160x120:rate=25",
        "-t",
        "2.0",
        "-c:v",
        "libx264",
        "-movflags",
        "frag_keyframe+empty_moov+default_base_moof+skip_trailer",
        "-f",
        "mp4",
        "pipe:1",
    ]

    res, dur = await uploader.stream_to_telegram(
        pikpak_url="",
        start_sec=0.0,
        end_sec=2.0,
        file_name="sender_test.mp4",
        part_index=1,
        custom_cmd=custom_cmd,
    )
    assert res is not None
    assert dur >= 1.5
    assert len(sent_via_sender) >= 1
    assert sorted(closed_senders) == [0, 1]
    assert uploader._worker_senders == {}


@pytest.mark.asyncio
async def test_cdn_range_precheck_rejection():
    """Requirement 2.6: Micro Range 0-0 probe asserts HTTP 206; rejects HTTP 200."""
    import aiohttp
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    # 1. Server returning 200 OK (ignoring Range)
    app200 = web.Application()

    async def handle_200(request):
        # 模拟大响应体 (5MB), 确保若错误调用 read() 会被拦截或暴露
        return web.Response(body=b"X" * (5 * 1024 * 1024), status=200)

    app200.router.add_get("/ignore_range", handle_200)

    server200 = TestServer(app200)
    await server200.start_server()
    try:
        url200 = f"http://127.0.0.1:{server200.port}/ignore_range"
        engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value=[url200]))
        item = PublishItem(
            item_id="fid_test",
            name="video.mp4",
            size_bytes=3000 * 1024 * 1024,
            duration_s=100,
            audio_codec="aac",
            source_type="pikpak",
        )
        ctx = _make_context(userbot_client=AsyncMock(), split_strategy="fmp4_universal")

        read_called = False
        original_read = aiohttp.ClientResponse.read

        async def spy_read(self, *args, **kwargs):
            nonlocal read_called
            read_called = True
            return await original_read(self, *args, **kwargs)

        with patch.object(aiohttp.ClientResponse, "read", spy_read):
            with pytest.raises(PikPakPublishFatalError) as exc_info:
                await engine.send_item(item, ctx)

        assert exc_info.value.category == "cdn_no_range"
        assert not read_called, (
            "P1-NEW-01 守卫: CDN 返回 200 时绝对禁止调用 resp.read(), 防止全量响应体读入内存导致 OOM"
        )
    finally:
        await server200.close()


@pytest.mark.asyncio
async def test_cdn_range_precheck_206_success_drains_body():
    """Verify that HTTP 206 Partial Content successfully passes precheck and drains exactly 1 byte."""
    import aiohttp
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    app206 = web.Application()

    async def handle_206(request):
        assert request.headers.get("Range") == "bytes=0-0"
        return web.Response(body=b"A", status=206, headers={"Content-Range": "bytes 0-0/1000"})

    app206.router.add_get("/partial", handle_206)

    server206 = TestServer(app206)
    await server206.start_server()
    try:
        url206 = f"http://127.0.0.1:{server206.port}/partial"
        read_called = False
        original_read = aiohttp.ClientResponse.read

        async def spy_read(self, *args, **kwargs):
            nonlocal read_called
            read_called = True
            return await original_read(self, *args, **kwargs)

        with patch.object(aiohttp.ClientResponse, "read", spy_read):
            await PikPakStreamTransportEngine._precheck_cdn_range(url206)

        assert read_called, "HTTP 206 precheck 必须调用 resp.read() 排空 1 字节以保证连接池复用"
    finally:
        await server206.close()


@pytest.mark.asyncio
async def test_fmp4_jit_relink_per_part_freshness():
    """Requirement 2.3: Each part in a multi-part upload JIT refreshes its URL before starting."""
    link_urls = [
        "https://cdn.example.com/part1_fresh.mp4",
        "https://cdn.example.com/part2_fresh.mp4",
    ]
    mock_link_fn = AsyncMock(side_effect=[[u] for u in link_urls])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    item = PublishItem(
        item_id="fid_multipart",
        name="multi.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.send_file.side_effect = [MagicMock(id=501), MagicMock(id=502)]
    ctx = _make_context(userbot_client=mock_client, split_strategy="fmp4_universal")

    plans = [
        Fmp4PartPlan(
            part_index=1, total_parts=2, start_sec=0, end_sec=900, duration_sec=900, part_name="multi.part1.mp4"
        ),
        Fmp4PartPlan(
            part_index=2, total_parts=2, start_sec=900, end_sec=1800, duration_sec=900, part_name="multi.part2.mp4"
        ),
    ]

    call_urls = []

    async def fake_stream_to_telegram(**kwargs):
        call_urls.append(kwargs.get("pikpak_url"))
        return MagicMock(), 900.0

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch(
            "app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram",
            side_effect=fake_stream_to_telegram,
        ),
    ):
        outcome = await engine.send_item(item, ctx)

        assert outcome.message_ids == [501, 502]
        assert len(call_urls) == 2
        # Part 1 uses link_urls[0], Part 2 JIT fetches link_urls[1]
        assert call_urls[0] == link_urls[0]
        assert call_urls[1] == link_urls[1]


@pytest.mark.asyncio
async def test_fmp4_progress_callback_wiring():
    """Requirement 1.8: Sliding window speed & cumulative progress correctly wired to context.progress_callback."""
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/video.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    item = PublishItem(
        item_id="fid_progress",
        name="progress_test.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1000,
        audio_codec="aac",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=601)

    captured_progress = []

    def progress_sink(*args, **kwargs):
        captured_progress.append((args, kwargs))

    ctx = _make_context(
        userbot_client=mock_client,
        split_strategy="fmp4_universal",
        progress_callback=progress_sink,
    )

    plans = [
        Fmp4PartPlan(part_index=1, total_parts=1, start_sec=0, end_sec=1000, duration_sec=1000, part_name="part1.mp4"),
    ]

    async def fake_stream_to_telegram(**kwargs):
        cb = kwargs.get("progress_callback")
        if cb:
            # Simulate chunks emitted
            cb(524288, 2000 * 1024 * 1024, 25.5, 30.0, "part1.mp4", 4)
            cb(1048576, 2000 * 1024 * 1024, 28.0, 25.0, "part1.mp4", 4)
        return MagicMock(), 1000.0

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch(
            "app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram",
            side_effect=fake_stream_to_telegram,
        ),
    ):
        await engine.send_item(item, ctx)
        assert len(captured_progress) == 2
        # Verify positional args (cur, tot, speed, eta, name) and kwargs passed to progress callback
        first_args = captured_progress[0][0]
        first_kwargs = captured_progress[0][1]
        assert first_kwargs.get("total_bytes_done") == 524288
        assert first_args[2] == 25.5


@pytest.mark.asyncio
async def test_fmp4_oversize_dynamic_replanning_self_healing():
    """Requirement: PartOversizeError triggers orphan rollback, dynamic replanning with force_num_parts, and fresh JIT relink."""
    mock_link_fn = AsyncMock(
        side_effect=[
            ["https://cdn.example.com/video_v1.mp4"],
            ["https://cdn.example.com/video_v2.mp4"],
            ["https://cdn.example.com/video_v3.mp4"],
            ["https://cdn.example.com/video_v4.mp4"],
            ["https://cdn.example.com/video_v5.mp4"],
            ["https://cdn.example.com/video_v6.mp4"],
        ]
    )
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    item = PublishItem(
        item_id="fid_oversize",
        name="oversize_movie.mp4",
        size_bytes=3500 * 1024 * 1024,
        duration_s=2000,
        audio_codec="aac",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.delete_messages = AsyncMock()

    # Send file returns mock messages
    mock_client.send_file.side_effect = [
        MagicMock(id=101),  # Initial plan part 1
        MagicMock(id=201),  # Replanned part 1
        MagicMock(id=202),  # Replanned part 2
        MagicMock(id=203),  # Replanned part 3
    ]

    ctx = _make_context(userbot_client=mock_client, split_strategy="fmp4_universal")

    # Initial plan: 2 parts
    initial_plans = [
        Fmp4PartPlan(part_index=1, total_parts=2, start_sec=0, end_sec=1000, duration_sec=1000, part_name="p1.mp4"),
        Fmp4PartPlan(part_index=2, total_parts=2, start_sec=1000, end_sec=2000, duration_sec=1000, part_name="p2.mp4"),
    ]
    # Replanned: 3 parts
    replanned_plans = [
        Fmp4PartPlan(part_index=1, total_parts=3, start_sec=0, end_sec=666, duration_sec=666, part_name="p1.mp4"),
        Fmp4PartPlan(part_index=2, total_parts=3, start_sec=666, end_sec=1333, duration_sec=667, part_name="p2.mp4"),
        Fmp4PartPlan(part_index=3, total_parts=3, start_sec=1333, end_sec=2000, duration_sec=667, part_name="p3.mp4"),
    ]

    plan_calls = []

    def fake_plan(*args, **kwargs):
        forced = kwargs.get("force_num_parts")
        plan_calls.append(forced)
        if forced == 3:
            return replanned_plans
        return initial_plans

    upload_stream_mock = AsyncMock(
        side_effect=[
            (MagicMock(), 1000.0),  # initial part 1
            PartOversizeError(
                part_index=2, total_emitted_bytes=2100000000, max_part_bytes=2000000000
            ),  # initial part 2 oversize!
            (MagicMock(), 666.0),  # replanned part 1
            (MagicMock(), 667.0),  # replanned part 2
            (MagicMock(), 667.0),  # replanned part 3
        ]
    )

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", side_effect=fake_plan),
        patch("app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram", new=upload_stream_mock),
    ):
        outcome = await engine.send_item(item, ctx)

        # Assert delivered orphan message [101] was deleted on oversize error
        mock_client.delete_messages.assert_awaited_with(ctx.target_chat_id, [101])

        # Assert replanning was called with force_num_parts=3
        assert 3 in plan_calls

        # Assert final outcome has the 3 message IDs from replanned send
        assert outcome.message_ids == [201, 202, 203]


@pytest.mark.asyncio
async def test_vp8_webm_routes_to_raw_split_fallback():
    """Requirement: VP8/libvpx WebM items must bypass fMP4 copy and safely fall back to raw split publishing."""
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/video.webm"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    # 1. Test video_codec="vp8"
    item_vp8 = PublishItem(
        item_id="fid_vp8",
        name="sample.webm",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        audio_codec="vorbis",
        video_codec="vp8",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    ctx = _make_context(userbot_client=mock_client, split_strategy="fmp4_universal")

    with (
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
        patch.object(engine, "_publish_single_item", new_callable=AsyncMock) as mock_raw,
    ):
        mock_raw.return_value = MagicMock(message_ids=[1, 2])

        res = await engine.send_item(item_vp8, ctx)
        mock_fmp4.assert_not_called()
        mock_raw.assert_called_once()
        assert res.message_ids == [1, 2]

    # 2. Test video_codec="libvpx"
    item_libvpx = PublishItem(
        item_id="fid_libvpx",
        name="sample2.webm",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        audio_codec="opus",
        video_codec="libvpx",
        source_type="pikpak",
    )
    with (
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
        patch.object(engine, "_publish_single_item", new_callable=AsyncMock) as mock_raw,
    ):
        mock_raw.return_value = MagicMock(message_ids=[3, 4])

        res = await engine.send_item(item_libvpx, ctx)
        mock_fmp4.assert_not_called()
        mock_raw.assert_called_once()
        assert res.message_ids == [3, 4]

    # 3. Test video_codec="v_vp8": Should route to raw split fallback
    item_v_vp8 = PublishItem(
        item_id="fid_v_vp8",
        name="sample_vvp8.webm",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        audio_codec="vorbis",
        video_codec="v_vp8",
        source_type="pikpak",
    )
    with (
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
        patch.object(engine, "_publish_single_item", new_callable=AsyncMock) as mock_raw,
    ):
        mock_raw.return_value = MagicMock(message_ids=[5, 6])

        res = await engine.send_item(item_v_vp8, ctx)
        mock_fmp4.assert_not_called()
        mock_raw.assert_called_once()
        assert res.message_ids == [5, 6]

    # 4. Test video_codec="vp9": Should proceed with fMP4 copy
    item_vp9 = PublishItem(
        item_id="fid_vp9",
        name="sample_vp9.webm",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        audio_codec="opus",
        video_codec="vp9",
        source_type="pikpak",
    )
    with (
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
        patch.object(engine, "_publish_single_item", new_callable=AsyncMock) as mock_raw,
    ):
        mock_fmp4.return_value = MagicMock(message_ids=[7, 8])

        res = await engine.send_item(item_vp9, ctx)
        mock_fmp4.assert_called_once()
        mock_raw.assert_not_called()
        assert res.message_ids == [7, 8]


@pytest.mark.asyncio
async def test_fmp4_duration_inflated_dynamic_replanning_self_healing():
    """Requirement: PartDurationInflatedError triggers orphan rollback, dynamic replanning with force_num_parts, and fresh JIT relink."""
    mock_link_fn = AsyncMock(
        side_effect=[
            ["https://cdn.example.com/video_v1.mp4"],
            ["https://cdn.example.com/video_v2.mp4"],
            ["https://cdn.example.com/video_v3.mp4"],
            ["https://cdn.example.com/video_v4.mp4"],
            ["https://cdn.example.com/video_v5.mp4"],
            ["https://cdn.example.com/video_v6.mp4"],
        ]
    )
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    item = PublishItem(
        item_id="fid_inflated",
        name="inflated_movie.mp4",
        size_bytes=3500 * 1024 * 1024,
        duration_s=2000,
        audio_codec="aac",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.delete_messages = AsyncMock()

    mock_client.send_file.side_effect = [
        MagicMock(id=101),  # Initial plan part 1
        MagicMock(id=301),  # Replanned part 1
        MagicMock(id=302),  # Replanned part 2
        MagicMock(id=303),  # Replanned part 3
    ]

    ctx = _make_context(userbot_client=mock_client, split_strategy="fmp4_universal")

    initial_plans = [
        Fmp4PartPlan(part_index=1, total_parts=2, start_sec=0, end_sec=1000, duration_sec=1000, part_name="p1.mp4"),
        Fmp4PartPlan(part_index=2, total_parts=2, start_sec=1000, end_sec=2000, duration_sec=1000, part_name="p2.mp4"),
    ]
    replanned_plans = [
        Fmp4PartPlan(part_index=1, total_parts=3, start_sec=0, end_sec=666, duration_sec=666, part_name="p1.mp4"),
        Fmp4PartPlan(part_index=2, total_parts=3, start_sec=666, end_sec=1333, duration_sec=667, part_name="p2.mp4"),
        Fmp4PartPlan(part_index=3, total_parts=3, start_sec=1333, end_sec=2000, duration_sec=667, part_name="p3.mp4"),
    ]

    plan_calls = []

    def fake_plan(*args, **kwargs):
        forced = kwargs.get("force_num_parts")
        plan_calls.append(forced)
        if forced == 3:
            return replanned_plans
        return initial_plans

    upload_stream_mock = AsyncMock(
        side_effect=[
            (MagicMock(), 1000.0),  # initial part 1
            PartDurationInflatedError(
                part_index=2, real_duration=1200.0, expected_duration=1000.0, message="Duration inflated"
            ),  # initial part 2 inflated!
            (MagicMock(), 666.0),  # replanned part 1
            (MagicMock(), 667.0),  # replanned part 2
            (MagicMock(), 667.0),  # replanned part 3
        ]
    )

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", side_effect=fake_plan),
        patch("app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram", new=upload_stream_mock),
    ):
        outcome = await engine.send_item(item, ctx)

        # Assert delivered orphan message [101] was deleted on duration inflated error
        mock_client.delete_messages.assert_awaited_with(ctx.target_chat_id, [101])

        # Assert replanning was called with force_num_parts=3
        assert 3 in plan_calls

        # Assert final outcome has the 3 message IDs from replanned send
        assert outcome.message_ids == [301, 302, 303]


@pytest.mark.asyncio
async def test_cdn_range_precheck_503_429_maps_to_throttled_and_redacts_url():
    """Milestone 1: _precheck_cdn_range 遇 503/429 (含 text/html) 必须抛可重试的
    PikPakPublishRetryableError(category='throttled')，且绝不在异常消息中泄露签名直链 URL。"""
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    from app.core.pikpak.taxonomy import PikPakPublishRetryableError

    app503 = web.Application()

    async def handle_503(request):
        return web.Response(
            text="<html><body>503 Service Temporarily Unavailable</body></html>",
            status=503,
            content_type="text/html",
            headers={"Retry-After": "900"},
        )

    app503.router.add_get("/download/file.mp4", handle_503)

    server503 = TestServer(app503)
    await server503.start_server()
    try:
        signed_url = f"http://127.0.0.1:{server503.port}/download/file.mp4?X-Sign=SUPER_SECRET_SIGNATURE&Expires=999"
        with pytest.raises(PikPakPublishRetryableError) as exc_info:
            await PikPakStreamTransportEngine._precheck_cdn_range(signed_url)

        err = exc_info.value
        assert err.category == "throttled"
        assert err.retry_after == 900.0
        assert "SUPER_SECRET_SIGNATURE" not in str(err)
        assert signed_url not in str(err)
    finally:
        await server503.close()


@pytest.mark.asyncio
async def test_fmp4_ffmpeg_503_stderr_aborts_immediately_as_throttled_with_url_redacted():
    """Milestone 1: fMP4 通道当 FFmpeg stderr 报 HTTP 503/429 时，映射为 PikPakThrottledError，
    在 _stream_fmp4_volume 中不做 3 次盲目重试，直接包装为 PikPakPublishRetryableError('throttled') 上抛且脱敏 URL。"""
    from app.core.fmp4_pipe_uploader import _raise_from_ffmpeg_stderr
    from app.core.pikpak.taxonomy import PikPakPublishRetryableError
    from app.core.stream_chunk_provider import PikPakThrottledError

    secret_url = "https://dl-a10b-1195.mypikpak.com/download/?fid=123&sign=TOP_SECRET_TOKEN"
    stderr_sample = [
        "[https @ 0x55] HTTP error 503 Service Unavailable\n",
        f"{secret_url}: Server returned 503 Service Unavailable\n",
    ]

    with pytest.raises(PikPakThrottledError) as throttle_info:
        _raise_from_ffmpeg_stderr(
            stderr_sample,
            pikpak_url=secret_url,
            returncode=1,
            part_index=2,
            prefix="FFmpeg process exited abnormally",
        )

    assert throttle_info.value.status == 503
    assert "TOP_SECRET_TOKEN" not in str(throttle_info.value)

    # 验证 _publish_fmp4_split_item 捕获 PikPakThrottledError 时不触发 3 次盲目重试
    mock_link_fn = AsyncMock(return_value=[secret_url])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)
    item = PublishItem(
        item_id="fid_fmp4_503",
        name="movie_503.mkv",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        audio_codec="eac3",
        source_type="pikpak",
    )
    ctx = _make_context(userbot_client=AsyncMock(), split_strategy="fmp4_universal")
    plans = [
        Fmp4PartPlan(part_index=1, total_parts=2, start_sec=0, end_sec=900, duration_sec=900, part_name="p1.mp4"),
    ]
    stream_mock = AsyncMock(side_effect=throttle_info.value)

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch("app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram", new=stream_mock),
    ):
        with pytest.raises(PikPakPublishRetryableError) as pub_exc:
            await engine.send_item(item, ctx)

        assert pub_exc.value.category == "throttled"
        assert stream_mock.await_count == 1, "遭遇 503 频控时严禁在 _stream_fmp4_volume 内盲目重跑 3 次"


@pytest.mark.asyncio
async def test_fmp4_dimension_fallback_defaults_to_zero():
    """Requirement When item and uploader have no dimensions, attributes default to w=0, h=0 (not 1280x720)."""
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/video.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    # 1. When width=0, height=0 and uploader has no video_width -> w=0, h=0
    item = PublishItem(
        item_id="fid_dimension_test",
        name="dimension_test.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=2000.0,
        width=0,
        height=0,
    )

    mock_client = AsyncMock()
    mock_client.send_file = AsyncMock(return_value=MagicMock(id=201))

    ctx = _make_context(userbot_client=mock_client, split_strategy="fmp4_universal")

    plans = [
        Fmp4PartPlan(part_index=1, total_parts=1, start_sec=0, end_sec=2000, duration_sec=2000, part_name="p1.mp4"),
    ]

    upload_stream_mock = AsyncMock(return_value=(MagicMock(), 2000.0))

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch("app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram", new=upload_stream_mock),
    ):
        await engine.send_item(item, ctx)

        assert mock_client.send_file.awaited
        call_kwargs = mock_client.send_file.call_args.kwargs
        attributes = call_kwargs.get("attributes", [])
        assert len(attributes) == 1
        video_attr = attributes[0]
        assert video_attr.w == 0, f"Expected w=0, got {video_attr.w}"
        assert video_attr.h == 0, f"Expected h=0, got {video_attr.h}"

    # 2. When width=None, height=None -> safely defaults to 0 without TypeError
    item_none = PublishItem(
        item_id="fid_dimension_none",
        name="dimension_none.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=2000.0,
        width=None,
        height=None,
    )
    mock_client.send_file.reset_mock()
    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch("app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram", new=upload_stream_mock),
    ):
        await engine.send_item(item_none, ctx)
        call_kwargs = mock_client.send_file.call_args.kwargs
        video_attr = call_kwargs.get("attributes", [])[0]
        assert video_attr.w == 0
        assert video_attr.h == 0

    # 3. When item has width=0, but uploader parsed moov with dimensions 1920x1080 -> uses parsed dimensions
    item_probed = PublishItem(
        item_id="fid_dimension_probed",
        name="dimension_probed.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=2000.0,
        width=0,
        height=0,
    )
    mock_client.send_file.reset_mock()

    async def fake_stream_with_probed_dims(self_uploader, **kwargs):
        self_uploader.video_width = 1920
        self_uploader.video_height = 1080
        return MagicMock(), 2000.0

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch(
            "app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram", fake_stream_with_probed_dims
        ),
    ):
        await engine.send_item(item_probed, ctx)
        call_kwargs = mock_client.send_file.call_args.kwargs
        video_attr = call_kwargs.get("attributes", [])[0]
        assert video_attr.w == 1920, f"Expected probed w=1920, got {video_attr.w}"
        assert video_attr.h == 1080, f"Expected probed h=1080, got {video_attr.h}"


# -----------------------------------------------------------------------------
# E2E 捕获模式 fMP4 接线 (docs/testing/E2E_GUIDE.md §5.1, D3/E2/E3)
# -----------------------------------------------------------------------------
import json
import os

from app.config import settings


def _snapshot_provider_stub(item_id: str, item_name: str, total_bytes: int):
    """源快照 provider 替身 (复用 VSSE 契约测试的内存 provider, 避免真实网络)。"""
    from tests.pipeline.test_vsse_engine_contract import MemorySyntheticChunkProvider

    return MemorySyntheticChunkProvider(file_id=item_id, file_name=item_name, total_bytes=total_bytes, header_bytes=b"")


def _make_capture_uploader_factory(ctor_calls: list, run_impl):
    """假 ZeroDiskFmp4StreamUploader 工厂: 记录构造 kwargs (capture_sink), run_impl 驱动 sink。"""

    def _factory(**ctor_kwargs):
        ctor_calls.append(ctor_kwargs)
        up = MagicMock()
        up.total_emitted_bytes = 0
        up.last_speed = 0.0
        up.video_width = 0
        up.video_height = 0

        async def _stt(**kwargs):
            return await run_impl(ctor_kwargs, kwargs)

        up.stream_to_telegram = _stt
        return up

    return _factory


async def _run_two_part_capture(item_name: str, item_size: int):
    """两卷成功流: sink 按 (0:-1 / 1:总数) 乱序定位写; 返回 (ctx, item, ctor_calls, res)。"""
    from unittest.mock import patch

    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/cap.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)
    item = PublishItem(
        item_id="fid_fmp4_cap",
        name=item_name,
        size_bytes=item_size,
        duration_s=1800,
        audio_codec="dts",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.send_file.side_effect = [MagicMock(id=501), MagicMock(id=502)]
    mock_client.delete_messages = AsyncMock()
    ctx = _make_context(
        userbot_client=mock_client,
        split_strategy="fmp4_universal",
        split_threshold_bytes=1024 * 1024,
    )
    plans = [
        Fmp4PartPlan(
            part_index=1, total_parts=2, start_sec=0, end_sec=900, duration_sec=900, part_name="capture.part001.mp4"
        ),
        Fmp4PartPlan(
            part_index=2, total_parts=2, start_sec=900, end_sec=1800, duration_sec=900, part_name="capture.part002.mp4"
        ),
    ]
    ctor_calls = []

    async def run_impl(ctor_kwargs, stream_kwargs):
        sink = ctor_kwargs.get("capture_sink")
        assert sink is not None, "capture 开时 uploader 构造必须收到非 None capture_sink"
        # 乱序: part 0 (patch 后) 与尾块最后到达 (E1)
        sink(1, 2, b"TAIL")
        sink(0, -1, b"PART0-")
        return MagicMock(), 900.0

    factory = _make_capture_uploader_factory(ctor_calls, run_impl)
    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch(
            "app.core.stream_chunk_provider.PikPakStreamChunkProvider",
            return_value=_snapshot_provider_stub(item.item_id, item.name, item.size_bytes),
        ),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch("app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader", side_effect=factory),
    ):
        res = await engine.send_item(item, ctx)
    return ctx, item, ctor_calls, res


@pytest.mark.asyncio
async def test_capture_on_fmp4_flow_records_parts_and_manifest(monkeypatch):
    """capture 开: 逐卷 sink 定位写落盘 + rename + manifest (含源快照)。"""
    monkeypatch.setattr(settings, "E2E_CAPTURE_MODE", True)
    ctx, item, ctor_calls, res = await _run_two_part_capture("capture.mp4", 3 * 1024 * 1024)

    assert res.message_ids == [501, 502]
    assert len(ctor_calls) == 2  # 每卷一个 uploader
    assert all(c.get("capture_sink") is not None for c in ctor_calls)

    root = os.path.join(settings.E2E_CAPTURE_DIR, ctx.task_id, item.item_id)
    with open(os.path.join(root, "manifest.json"), encoding="utf-8") as f:
        manifest = json.load(f)
    assert manifest["source"]["status"] == "ok"
    assert manifest["source"]["size_bytes"] == 3 * 1024 * 1024
    entries = manifest["parts"]
    assert [e["status"] for e in entries] == ["sent", "sent"]
    assert [e["msg_id"] for e in entries] == [501, 502]
    assert all(e["channel"] == "fmp4" and e["header_len"] == 0 for e in entries)
    expected_layout = b"PART0-" + b"\x00" * (PART_SIZE - 6) + b"TAIL"
    for e in entries:
        part_path = os.path.join(root, e["path"])
        assert os.path.isfile(part_path)
        assert not os.path.exists(part_path + ".part")
        with open(part_path, "rb") as f:
            assert f.read() == expected_layout
        assert e["bytes_written"] == PART_SIZE + len(b"TAIL")


@pytest.mark.asyncio
async def test_capture_off_fmp4_sink_none_and_zero_dirs():
    """capture 关 (默认): uploader 构造 capture_sink=None, 目录零创建 (回归保障)。"""
    assert settings.E2E_CAPTURE_MODE is False

    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/off.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)
    item = PublishItem(
        item_id="fid_fmp4_off",
        name="plain.mp4",
        size_bytes=3 * 1024 * 1024,
        duration_s=1800,
        audio_codec="dts",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.send_file.side_effect = [MagicMock(id=601), MagicMock(id=602)]
    ctx = _make_context(
        userbot_client=mock_client,
        split_strategy="fmp4_universal",
        split_threshold_bytes=1024 * 1024,
    )
    plans = [
        Fmp4PartPlan(
            part_index=1, total_parts=2, start_sec=0, end_sec=900, duration_sec=900, part_name="plain.part001.mp4"
        ),
        Fmp4PartPlan(
            part_index=2, total_parts=2, start_sec=900, end_sec=1800, duration_sec=900, part_name="plain.part002.mp4"
        ),
    ]
    ctor_calls = []

    async def run_impl(ctor_kwargs, stream_kwargs):
        assert ctor_kwargs.get("capture_sink") is None
        return MagicMock(), 900.0

    factory = _make_capture_uploader_factory(ctor_calls, run_impl)
    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch("app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader", side_effect=factory),
    ):
        res = await engine.send_item(item, ctx)

    assert res.message_ids == [601, 602]
    assert os.listdir(settings.E2E_CAPTURE_DIR) == []


@pytest.mark.asyncio
async def test_capture_on_fmp4_retry_discards_and_truncates_residue(monkeypatch):
    """attempt1 失败: .part 被 discard 清场; attempt2 成功仅保留新字节 (E2 truncate 语义)。"""
    monkeypatch.setattr(settings, "E2E_CAPTURE_MODE", True)
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/retry.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)
    item = PublishItem(
        item_id="fid_fmp4_retry",
        name="retry.mp4",
        size_bytes=3 * 1024 * 1024,
        duration_s=1800,
        audio_codec="dts",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.send_file.side_effect = [MagicMock(id=701)]
    ctx = _make_context(
        userbot_client=mock_client,
        split_strategy="fmp4_universal",
        split_threshold_bytes=1024 * 1024,
    )
    plans = [
        Fmp4PartPlan(
            part_index=1, total_parts=1, start_sec=0, end_sec=1800, duration_sec=1800, part_name="retry.part001.mp4"
        ),
    ]
    attempt = {"n": 0}

    async def run_impl(ctor_kwargs, stream_kwargs):
        attempt["n"] += 1
        sink = ctor_kwargs["capture_sink"]
        assert sink is not None
        if attempt["n"] == 1:
            sink(0, -1, b"ATTEMPT1-RESIDUE")
            raise RuntimeError("transient network failure")
        sink(0, 1, b"OK")
        return MagicMock(), 10.0

    factory = _make_capture_uploader_factory([], run_impl)
    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch(
            "app.core.stream_chunk_provider.PikPakStreamChunkProvider",
            return_value=_snapshot_provider_stub(item.item_id, item.name, item.size_bytes),
        ),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch("app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader", side_effect=factory),
    ):
        res = await engine.send_item(item, ctx)

    assert attempt["n"] == 2
    assert res.message_ids == [701]
    root = os.path.join(settings.E2E_CAPTURE_DIR, ctx.task_id, item.item_id)
    with open(os.path.join(root, "manifest.json"), encoding="utf-8") as f:
        manifest = json.load(f)
    entry = manifest["parts"][0]
    assert entry["status"] == "sent" and entry["msg_id"] == 701
    with open(os.path.join(root, entry["path"]), "rb") as f:
        assert f.read() == b"OK"  # attempt1 残留已被清场, 无拼接污染


def _make_codec_hint_ctx(task_id="T-CODEC-HINT"):
    return PublishContext(task_id=task_id, target_chat_id=-100999)


def _codec_hint_item(codec):
    return PublishItem(
        item_id="pikpak://F1/hevc_movie.mp4",
        name="hevc_movie.mp4",
        size_bytes=1024,
        source_type="pikpak",
        video_codec=codec,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "codec",
    [
        "hevc",
        "HEVC",
        "h265",
        "h.265",
        "hvc1",
        "hev1",  # HEVC 家族 (API 命名 + fourcc)
        "vp9",
        "vp09",
        "av1",
        "av01",  # VP9 / AV1 (两种命名)
    ],
)
async def test_codec_hint_logged_for_no_hw_decode_codecs(monkeypatch, codec):
    logged = AsyncMock()
    monkeypatch.setattr("app.db.task_repository.TaskQueueRepository.append_log_async", logged)

    await _maybe_log_codec_compat_hint(_codec_hint_item(codec), _make_codec_hint_ctx("T-42"))

    assert logged.await_count == 1
    task_id, msg = logged.await_args.args
    assert task_id == "T-42"
    assert "无硬解" in msg and "软解码" in msg


@pytest.mark.asyncio
@pytest.mark.parametrize("codec", [None, "", "h264", "avc1", "mp4a.40.2"])
async def test_codec_hint_skipped_for_h264_and_unknown(monkeypatch, codec):
    logged = AsyncMock()
    monkeypatch.setattr("app.db.task_repository.TaskQueueRepository.append_log_async", logged)

    item = _codec_hint_item(codec) if codec else _codec_hint_item(None)
    await _maybe_log_codec_compat_hint(item, _make_codec_hint_ctx())

    assert logged.await_count == 0


@pytest.mark.asyncio
async def test_codec_hint_repo_failure_is_silent(monkeypatch):
    logged = AsyncMock(side_effect=RuntimeError("db down"))
    monkeypatch.setattr("app.db.task_repository.TaskQueueRepository.append_log_async", logged)

    # 落库失败必须静默 (非致命), 不得影响发布流程
    await _maybe_log_codec_compat_hint(_codec_hint_item("hevc"), _make_codec_hint_ctx())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("filename", "prober_attr"),
    [
        ("movie.mkv", "_probe_mkv_cues"),
        ("movie.mp4", "_probe_mp4_stss"),
    ],
)
async def test_fmp4_probe_retains_known_duration_when_probe_returns_zero(filename: str, prober_attr: str) -> None:
    """可选探测返回 0.0 时保留 item.duration_s，不得覆盖为 0 并回退按体积粗估."""
    from app.core.pipeline.engines.pikpak import fmp4 as pik_fmp4

    item = PublishItem(
        item_id="fid_dur",
        name=filename,
        size_bytes=10 * 1024 * 1024,
        duration_s=7200.0,
        source_type="pikpak",
    )
    ctx = PublishContext(task_id="t_dur", target_chat_id=-1001)
    mock_ret = (0.0, None) if prober_attr == "_probe_mkv_cues" else (0.0, None, "aac", True)
    with patch.object(pik_fmp4, prober_attr, new_callable=AsyncMock, return_value=mock_ret):
        _ext, _copy_audio, total_duration, _pts = await pik_fmp4.probe_fmp4_media("https://cdn/v", item, ctx)
    assert total_duration == 7200.0


@pytest.mark.asyncio
@pytest.mark.parametrize(("filename", "prober"), [("movie.mkv", MkvCuesProber), ("movie.m4v", Mp4StssProber)])
@pytest.mark.parametrize("known_duration", [None, 7100.0, 7500.0])
async def test_fmp4_blind_plan_uses_full_duration_without_a_keyframe_index(filename, prober, known_duration):
    from app.core.pipeline.engines.pikpak.fmp4 import probe_fmp4_media

    item = PublishItem(item_id="fid", name=filename, size_bytes=100 * 1024 * 1024, duration_s=known_duration)
    ctx = PublishContext(task_id="t_blind_duration", target_chat_id=-1001)
    no_index = CuesProbeResult(duration_s=7200.0, timescale=90000, has_cues=False)
    with patch.object(prober, "probe_from_url", new=AsyncMock(return_value=no_index)):
        _ext, _copy_audio, duration, points = await probe_fmp4_media("https://cdn.test/video", item, ctx)

    plans = plan_video_fmp4_parts(item.size_bytes, 40 * 1024 * 1024, duration, points, item.name)
    expected_duration = no_index.duration_s
    assert item.duration_s == duration == expected_duration
    assert points is None and len(plans) > 1 and all(plan.is_blind for plan in plans)
    assert plans[-1].end_sec == expected_duration


@pytest.mark.asyncio
async def test_fmp4_transient_network_exhaustion_raises_retryable() -> None:
    """stream_fmp4_volume 耗尽 3 次重试后，瞬态网络异常须抛 PikPakPublishRetryableError 而非 Fatal."""
    from app.core.pikpak.taxonomy import PikPakPublishRetryableError
    from app.core.pipeline.engines.pikpak import fmp4 as pik_fmp4
    from app.core.stream_chunk_provider import ChunkFetchTransientError

    item = PublishItem(item_id="fid_tr", name="tr.mp4", size_bytes=1000, source_type="pikpak")
    plan = MagicMock(part_index=1, total_parts=2, start_sec=0.0, end_sec=10.0, duration_sec=10.0, part_name="p1.mp4")
    ctx = PublishContext(task_id="t_tr", target_chat_id=-1001)
    capture = MagicMock()

    with (
        patch.object(
            pik_fmp4,
            "_attempt_fmp4_part_upload",
            new_callable=AsyncMock,
            side_effect=ChunkFetchTransientError("cdn timeout"),
        ),
        patch.object(pik_fmp4, "_sleep_backoff_with_control_watch", new_callable=AsyncMock),
        pytest.raises(PikPakPublishRetryableError) as exc_info,
    ):
        await pik_fmp4.stream_fmp4_volume(
            AsyncMock(),
            item,
            plan,
            "https://cdn/v1",
            ctx,
            capture,
            threshold=2000,
            ext=".mp4",
            copy_audio=True,
            thumb_bytes=None,
            prior_bytes=0,
            total_planned=2,
            workers_count=2,
            in_flight_count=1,
            refresh_url=AsyncMock(return_value="https://cdn/v2"),
        )
    assert exc_info.value.category == "transient"
    assert isinstance(exc_info.value.__cause__, ChunkFetchTransientError)


@pytest.mark.asyncio
async def test_replan_fmp4_rollback_failure_preserves_checkpoint_and_raises_retryable() -> None:
    """fMP4 重规划回滚删消息失败时不得清空 delivered_message_ids 或检查点，须抛 RetryableTaskError."""
    from app.core.pipeline.engines.pikpak import fmp4 as pik_fmp4
    from app.core.scheduler import RetryableTaskError

    client = AsyncMock()
    fw = FloodWaitError(request=None)
    fw.seconds = 33
    client.delete_messages = AsyncMock(side_effect=fw)

    ctx = PublishContext(task_id="t_fmp4_rb", target_chat_id=-1003)
    capture = MagicMock()
    plans = [
        Fmp4PartPlan(part_index=1, total_parts=2, start_sec=0.0, end_sec=10.0, duration_sec=10.0, part_name="p1.mp4")
    ]
    delivered = [101, 102]

    with pytest.raises(RetryableTaskError) as exc_info:
        await pik_fmp4.replan_fmp4_after_oversize(
            client,
            ctx,
            capture,
            plans,
            delivered,
            replan_count=1,
            max_replan_attempts=3,
            last_part_exc=PartOversizeError(1, 200, 100, "oversize"),
            get_fresh_url=AsyncMock(return_value="https://cdn.test/new"),
            item_id="fid",
            task_store=MagicMock(clear_part_checkpoint=AsyncMock()),
        )

    assert exc_info.value.retry_after == 33.0
    assert delivered == [101, 102]
    capture.mark_parts_discarded.assert_not_called()


async def test_oversize_rollback_clears_checkpoint_before_cancelled_refresh():
    from app.core.pipeline.engines.pikpak import fmp4 as pik_fmp4

    store = MagicMock(clear_part_checkpoint=AsyncMock())
    delivered = [101]

    async def cancelled_refresh():
        store.clear_part_checkpoint.assert_awaited_once_with("task", "file")
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await pik_fmp4.replan_fmp4_after_oversize(
            AsyncMock(),
            PublishContext(task_id="task", target_chat_id=-1001),
            MagicMock(),
            [],
            delivered,
            replan_count=1,
            max_replan_attempts=3,
            last_part_exc=None,
            get_fresh_url=cancelled_refresh,
            item_id="file",
            task_store=store,
        )
    assert delivered == []


def test_throttle_signature_whole_word_only():
    """非状态码数字中的 503/504 子串不应误判为频控."""
    from app.core.pipeline.engines.pikpak.fmp4 import _is_throttle_signature_error

    assert _is_throttle_signature_error(RuntimeError("read failed at offset=15030912")) is False
    assert _is_throttle_signature_error(RuntimeError("HTTP 503 Service Unavailable")) is True


@pytest.mark.asyncio
async def test_fmp4_checkpoint_signature_rejects_degraded_blind_split_same_part_count():
    """fMP4 恢复时关键帧探测失败退化为盲切且总卷数仍相等时，必须校验边界签名并清理重传."""
    from app.core.pipeline.engines.pikpak.fmp4 import Fmp4SplitChannel
    from app.core.pipeline.engines.pikpak.split_loop import execute_split_loop, get_saved_checkpoints
    from app.core.scheduler import RetryableTaskError
    from app.db.task_repository import TaskQueueRepository

    item = PublishItem(item_id="fid_sig", name="movie.mp4", size_bytes=3000, source_type="pikpak")
    threshold = 1800

    ch_kf = Fmp4SplitChannel(
        item=item,
        context=PublishContext(task_id="t_fmp4_sig", target_chat_id=-1001),
        threshold=threshold,
        get_fresh_url=AsyncMock(return_value="https://cdn.test/1"),
        initial_url="https://cdn.test/1",
        ext=".mp4",
        copy_audio=True,
        total_duration=100.0,
        keyframe_pts=[0.0, 42.0, 80.0],
        thumb_bytes=None,
    )
    kf_plans = ch_kf.plan_volumes()
    assert len(kf_plans) == 2
    kf_sig = ch_kf.get_plan_signature(kf_plans)

    ch_blind = Fmp4SplitChannel(
        item=item,
        context=PublishContext(task_id="t_fmp4_sig", target_chat_id=-1001),
        threshold=threshold,
        get_fresh_url=AsyncMock(return_value="https://cdn.test/2"),
        initial_url="https://cdn.test/2",
        ext=".mp4",
        copy_audio=True,
        total_duration=100.0,
        keyframe_pts=None,
        thumb_bytes=None,
    )
    blind_plans = ch_blind.plan_volumes()
    assert len(blind_plans) == 2
    blind_sig = ch_blind.get_plan_signature(blind_plans)
    assert blind_sig != kf_sig

    TaskQueueRepository.create("t_fmp4_sig", "pikpak_publish", {})
    try:
        await TaskQueueRepository.record_part_checkpoint_async(
            "t_fmp4_sig", "fid_sig", "fmp4", 2, threshold, 1, 111, plan_signature=kf_sig
        )
        assert await get_saved_checkpoints("t_fmp4_sig", "fid_sig", "fmp4", 2, threshold, plan_signature=kf_sig) == {
            1: 111
        }
        assert (
            await get_saved_checkpoints("t_fmp4_sig", "fid_sig", "fmp4", 2, threshold, plan_signature=blind_sig) == {}
        )

        ch_blind.prepare_part = AsyncMock()
        ch_blind.stream_part = AsyncMock(side_effect=[(211, 1500, 1.0), (212, 1500, 1.0)])
        ch_blind.aclose = AsyncMock()
        client = MagicMock()
        client.delete_messages = AsyncMock()

        with patch("app.core.pipeline.engines.pikpak.split_loop._inter_part_breathing_delay", new_callable=AsyncMock):
            outcome = await execute_split_loop(
                channel=ch_blind,
                item=item,
                context=PublishContext(task_id="t_fmp4_sig", target_chat_id=-1001),
                threshold=threshold,
                client=client,
                capture_session=MagicMock(),
                link_provider=AsyncMock(),
                reporter=MagicMock(),
            )
        client.delete_messages.assert_awaited_once_with(-1001, [111])
        assert ch_blind.stream_part.await_count == 2
        assert outcome.message_ids == [211, 212]

        await TaskQueueRepository.record_part_checkpoint_async(
            "t_fmp4_sig", "fid_sig", "fmp4", 2, threshold, 1, 333, plan_signature=kf_sig
        )
        ch_blind2 = Fmp4SplitChannel(
            item=item,
            context=PublishContext(task_id="t_fmp4_sig", target_chat_id=-1001),
            threshold=threshold,
            get_fresh_url=AsyncMock(return_value="https://cdn.test/3"),
            initial_url="https://cdn.test/3",
            ext=".mp4",
            copy_audio=True,
            total_duration=100.0,
            keyframe_pts=None,
            thumb_bytes=None,
        )
        ch_blind2.stream_part = AsyncMock()
        ch_blind2.aclose = AsyncMock()
        client_fail = MagicMock()
        client_fail.delete_messages = AsyncMock(side_effect=RuntimeError("tg delete err"))
        with pytest.raises(RetryableTaskError):
            await execute_split_loop(
                channel=ch_blind2,
                item=item,
                context=PublishContext(task_id="t_fmp4_sig", target_chat_id=-1001),
                threshold=threshold,
                client=client_fail,
                capture_session=MagicMock(),
                link_provider=AsyncMock(),
                reporter=MagicMock(),
            )
        ch_blind2.stream_part.assert_not_awaited()
        assert await get_saved_checkpoints("t_fmp4_sig", "fid_sig", "fmp4", 2, threshold, plan_signature=kf_sig) == {
            1: 333
        }
    finally:
        TaskQueueRepository.delete("t_fmp4_sig")


def test_compute_fmp4_plan_signature_distinguishes_blind_and_boundaries():
    from app.core.pipeline.engines.pikpak.fmp4 import compute_fmp4_plan_signature

    p_kf = [
        Fmp4PartPlan(1, 2, 0.0, 42.0, 42.0, "a.part001.mp4", is_blind=False),
        Fmp4PartPlan(2, 2, 42.0, 100.0, 58.0, "a.part002.mp4", is_blind=False),
    ]
    p_shift = [
        Fmp4PartPlan(1, 2, 0.0, 50.0, 50.0, "a.part001.mp4", is_blind=False),
        Fmp4PartPlan(2, 2, 50.0, 100.0, 50.0, "a.part002.mp4", is_blind=False),
    ]
    p_blind = [
        Fmp4PartPlan(1, 2, 0.0, 42.0, 42.0, "a.part001.mp4", is_blind=True),
        Fmp4PartPlan(2, 2, 42.0, 100.0, 58.0, "a.part002.mp4", is_blind=True),
    ]
    assert compute_fmp4_plan_signature(p_kf) != compute_fmp4_plan_signature(p_shift)
    assert compute_fmp4_plan_signature(p_kf) != compute_fmp4_plan_signature(p_blind)
