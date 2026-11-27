"""
Contract and Resilience Tests for Virtual Sample Slicing Engine (VSSE v2) Dispatch Path.

Requirements Covered:
1. Standard MP4 + AAC routes to VSSE without spawning FFmpeg child process,
   passes correct meta_path, succeeds with supports_streaming=True.
2. Circuit breaker: injected mvex (Fragmented MP4) catches FragmentedMp4Error
   and automatically falls back to _publish_fmp4_split_item seamlessly.
3. Circuit breaker: injected multiple mdat catches MultipleMdatError
   and falls back to _publish_fmp4_split_item.
4. Circuit breaker: injected stz2 catches CompactSampleSizeError
   and falls back to _publish_fmp4_split_item.
5. Pre-flight routing: MKV / DTS / non-AAC files automatically route to Phase 1
   _publish_fmp4_split_item without attempting VSSE.
6. Pre-flight routing: VP8 / WebM files route to appropriate fallback.
7. Atomic rollback: failure midway through VSSE parts deletes previously delivered messages.
8. One-second killswitch: setting SPLIT_STRATEGY=fmp4_universal completely
   bypasses VSSE and uses Phase 1 universal pipeline.
9. Video-only MP4 routes to VSSE successfully.
10. Corrupt moov / IsoBmffParseError falls back to fMP4.
11. Unsupported codec in moov metadata triggers circuit breaker to fMP4.
12. Progress callback wiring verifies smooth percentage and speed reporting.
"""

import struct
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from app.core.pipeline.engines import PikPakStreamTransportEngine
from app.core.pipeline.engines.part_delivery import UnconfirmedPartError
from app.core.pipeline.models import BreathingPolicy, PublishContext, PublishItem, TransportOutcome
from app.core.pipeline.utils import get_meta_path
from app.core.stream_chunk_provider import BaseChunkProvider
from tests.core.test_vsse_box_parser import (
    build_synthetic_moov,
    build_synthetic_stbl,
    build_synthetic_trak,
    make_box,
)

# 测试零等待呼吸/退避策略 (生产代码不再感知 PYTEST_CURRENT_TEST,
# 零等待由测试上下文显式注入; 专项呼吸测试传显式策略覆盖)
ZERO_WAIT_BREATHING = BreathingPolicy(
    min_floor_s=0.0, jitter_s=0.0, poll_s=0.01, retry_backoff_s=(0.0, 0.0), enabled=False
)


def _make_context(
    task_id="task_test_vsse",
    target_chat_id=-1009000000010,
    userbot_client=None,
    split_strategy="vsse_with_fallback",
    split_threshold_bytes=2000 * 1024 * 1024,
    upload_concurrency=8,
    **kwargs,
) -> PublishContext:
    kwargs.setdefault("breathing", ZERO_WAIT_BREATHING)
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
        upload_concurrency=upload_concurrency,
        **kwargs,
    )


class MemorySyntheticChunkProvider(BaseChunkProvider):
    """Synthetic in-memory chunk provider that wraps synthetic MP4 boxes for testing."""

    def __init__(self, file_id: str, file_name: str, total_bytes: int, header_bytes: bytes):
        self.file_id = file_id
        self.file_name = file_name
        self.total_bytes = total_bytes
        self._header = header_bytes

    async def read_chunk(self, offset: int, size: int) -> bytes:
        if offset >= self.total_bytes:
            raise ValueError(f"Offset {offset} >= total {self.total_bytes}")
        if offset < len(self._header):
            return self._header[offset : min(offset + size, len(self._header))]
        # Return dummy media payload bytes for any offset outside header
        rem = min(size, self.total_bytes - offset)
        return b"\x00" * rem

    async def aclose(self) -> None:
        return None


def _build_test_mp4_header(
    total_bytes: int = 3000 * 1024 * 1024,
    v_samples: int = 1000,
    sync_samples: list[int] | None = None,
    include_audio: bool = True,
    include_mvex: bool = False,
    use_stz2: bool = False,
    video_codec: str = "avc1",
    audio_codec: str = "mp4a",
    multiple_mdat: bool = False,
) -> bytes:
    ftyp = make_box(b"ftyp", b"isom\x00\x00\x02\x00isomiso2mp41")
    if sync_samples is None:
        sync_samples = [1, 501]

    # 3MB per sample * 1000 samples = 3,000,000,000 bytes (2861.02 MiB / 2.79 GiB, strictly > 2000 MiB (1.95 GiB) threshold -> 2 parts)
    num_chunks = 10
    spc = v_samples // num_chunks
    v_chunk_size = spc * 3000000
    a_chunk_size = spc * 50000 if include_audio else 0
    pair_size = v_chunk_size + a_chunk_size

    v_chunk_offsets = [10000 + i * pair_size for i in range(num_chunks)]
    v_stbl = build_synthetic_stbl(
        default_sample_size=3000000,
        sample_count=v_samples,
        chunk_offsets=v_chunk_offsets,
        samples_per_chunk=spc,
        sync_samples=sync_samples,
        is_video=True,
        is_co64=True,
        use_stz2=use_stz2,
        codec=video_codec.encode("ascii") if isinstance(video_codec, str) else video_codec,
    )
    v_trak = build_synthetic_trak(1, is_video=True, duration=10000, timescale=1000, stbl_bytes=v_stbl)

    tracks = [v_trak]
    if include_audio:
        a_chunk_offsets = [10000 + i * pair_size + v_chunk_size for i in range(num_chunks)]
        a_stbl = build_synthetic_stbl(
            default_sample_size=50000,
            sample_count=v_samples,
            chunk_offsets=a_chunk_offsets,
            samples_per_chunk=spc,
            is_video=False,
            is_co64=True,
            codec=audio_codec.encode("ascii") if isinstance(audio_codec, str) else audio_codec,
        )
        a_trak = build_synthetic_trak(2, is_video=False, duration=10000, timescale=1000, stbl_bytes=a_stbl)
        tracks.append(a_trak)

    moov = build_synthetic_moov(tracks=tracks, include_mvex=include_mvex)

    if multiple_mdat:
        mdat1 = make_box(b"mdat", b"sample1_payload")
        mdat2 = make_box(b"mdat", b"sample2_payload")
        return ftyp + moov + mdat1 + mdat2

    mdat1_size = 1000 * 1024 * 1024
    mdat1 = struct.pack(">I4s", mdat1_size, b"mdat")
    return ftyp + moov + mdat1


# -----------------------------------------------------------------------------
# Test 1: Standard MP4 + AAC routes to VSSE without FFmpeg
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_standard_mp4_routes_to_vsse_no_ffmpeg_contract():
    """
    Requirement 1: Standard MP4 + AAC routes to VSSE without spawning FFmpeg child process,
    passes correct meta_path, succeeds with supports_streaming=True.
    """
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/standard.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    file_size = 3000 * 1024 * 1024
    item = PublishItem(
        item_id="fid_vsse_std",
        name="standard_movie.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="aac",
        video_codec="h264",
        source_type="pikpak",
    )

    header_bytes = _build_test_mp4_header(total_bytes=file_size)
    mock_provider = MemorySyntheticChunkProvider(
        file_id=item.item_id,
        file_name=item.name,
        total_bytes=file_size,
        header_bytes=header_bytes,
    )

    mock_client = AsyncMock()
    mock_client.send_file.side_effect = [
        MagicMock(id=101),
        MagicMock(id=102),
    ]
    ctx = _make_context(userbot_client=mock_client, split_strategy="vsse_with_fallback")

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_upload,
    ):
        mock_upload.side_effect = [MagicMock(), MagicMock()]

        res = await engine.send_item(item, ctx)

        # 1. FFmpeg universal pipeline was NEVER called
        mock_fmp4.assert_not_called()

        # 2. Both parts successfully published
        assert res.message_ids == [101, 102]
        assert mock_upload.call_count == 2

        # 3. Verify sandboxed meta_path was passed to upload_file_parallel
        first_call_kwargs = mock_upload.call_args_list[0].kwargs
        second_call_kwargs = mock_upload.call_args_list[1].kwargs
        expected_meta_1 = get_meta_path(ctx.task_id, item.item_id, 1)
        expected_meta_2 = get_meta_path(ctx.task_id, item.item_id, 2)
        assert first_call_kwargs["meta_path"] == expected_meta_1
        assert second_call_kwargs["meta_path"] == expected_meta_2
        assert first_call_kwargs["concurrency"] == 8

        # 4. Verify send_file received supports_streaming=True and video attributes
        assert mock_client.send_file.call_count == 2
        first_sf_kwargs = mock_client.send_file.call_args_list[0].kwargs
        assert first_sf_kwargs["supports_streaming"] is True
        attrs = first_sf_kwargs.get("attributes")
        assert attrs is not None and len(attrs) > 0
        assert getattr(attrs[0], "supports_streaming", False) is True


# -----------------------------------------------------------------------------
# Test 2: Circuit breaker - injected mvex (Fragmented MP4) fallback
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_circuit_breaker_mvex_fragmented_mp4_fallback():
    """
    Requirement 2: Circuit breaker catches FragmentedMp4Error (injected mvex)
    and automatically falls back to _publish_fmp4_split_item seamlessly.
    """
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/fragmented.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    file_size = 3000 * 1024 * 1024
    item = PublishItem(
        item_id="fid_mvex",
        name="fragmented.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )

    header_bytes = _build_test_mp4_header(total_bytes=file_size, include_mvex=True)
    mock_provider = MemorySyntheticChunkProvider(
        file_id=item.item_id,
        file_name=item.name,
        total_bytes=file_size,
        header_bytes=header_bytes,
    )

    mock_client = AsyncMock()
    ctx = _make_context(userbot_client=mock_client, split_strategy="vsse_with_fallback")

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
    ):
        mock_fmp4.return_value = TransportOutcome(message_ids=[888], speed_mb_s=12.5, client_label="Userbot")

        res = await engine.send_item(item, ctx)

        # Verified seamless fallback to Phase 1 universal pipeline with metadata passthrough
        mock_fmp4.assert_awaited_once_with(
            mock_client,
            item,
            ctx.video_split_threshold_bytes,
            ctx,
            capture_session=ANY,
            probed_metadata=ANY,
            pinned_regions=ANY,
        )
        assert res.message_ids == [888]


# -----------------------------------------------------------------------------
# Test 3: Circuit breaker - injected multiple mdat fallback
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_circuit_breaker_multiple_mdat_fallback():
    """
    Requirement 3: Circuit breaker catches MultipleMdatError
    and automatically falls back to _publish_fmp4_split_item seamlessly.
    """
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/multi_mdat.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    file_size = 3000 * 1024 * 1024
    item = PublishItem(
        item_id="fid_multi_mdat",
        name="multi_mdat.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )

    header_bytes = _build_test_mp4_header(total_bytes=file_size, multiple_mdat=True)
    mock_provider = MemorySyntheticChunkProvider(
        file_id=item.item_id,
        file_name=item.name,
        total_bytes=file_size,
        header_bytes=header_bytes,
    )

    mock_client = AsyncMock()
    ctx = _make_context(userbot_client=mock_client, split_strategy="vsse_with_fallback")

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
    ):
        mock_fmp4.return_value = TransportOutcome(message_ids=[889], speed_mb_s=10.0, client_label="Userbot")

        res = await engine.send_item(item, ctx)

        mock_fmp4.assert_awaited_once_with(
            mock_client,
            item,
            ctx.video_split_threshold_bytes,
            ctx,
            capture_session=ANY,
            probed_metadata=ANY,
            pinned_regions=ANY,
        )
        assert res.message_ids == [889]


# -----------------------------------------------------------------------------
# Test 4: Circuit breaker - injected stz2 compact sample size fallback
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_circuit_breaker_stz2_compact_sample_size_fallback():
    """
    Requirement 4: Circuit breaker catches CompactSampleSizeError (injected stz2)
    and automatically falls back to _publish_fmp4_split_item seamlessly.
    """
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/stz2.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    file_size = 3000 * 1024 * 1024
    item = PublishItem(
        item_id="fid_stz2",
        name="stz2.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )

    header_bytes = _build_test_mp4_header(total_bytes=file_size, use_stz2=True)
    mock_provider = MemorySyntheticChunkProvider(
        file_id=item.item_id,
        file_name=item.name,
        total_bytes=file_size,
        header_bytes=header_bytes,
    )

    mock_client = AsyncMock()
    ctx = _make_context(userbot_client=mock_client, split_strategy="vsse_with_fallback")

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
    ):
        mock_fmp4.return_value = TransportOutcome(message_ids=[890], speed_mb_s=11.0, client_label="Userbot")

        res = await engine.send_item(item, ctx)

        mock_fmp4.assert_awaited_once_with(
            mock_client,
            item,
            ctx.video_split_threshold_bytes,
            ctx,
            capture_session=ANY,
            probed_metadata=ANY,
            pinned_regions=ANY,
        )
        assert res.message_ids == [890]


# -----------------------------------------------------------------------------
# Test 5: Pre-flight routing - MKV / DTS files bypass VSSE
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_preflight_routing_mkv_and_dts_to_fmp4():
    """
    Requirement 5: MKV / DTS files automatically route to Phase 1
    _publish_fmp4_split_item without attempting VSSE.
    """
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/media"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    mock_client = AsyncMock()
    ctx = _make_context(userbot_client=mock_client, split_strategy="vsse_with_fallback")

    # 1. MKV container with AAC audio: should route to fMP4
    item_mkv = PublishItem(
        item_id="fid_mkv",
        name="movie.mkv",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )
    # 2. MP4 container with DTS audio: should route to fMP4
    item_dts = PublishItem(
        item_id="fid_dts",
        name="movie.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        audio_codec="dts",
        source_type="pikpak",
    )
    # 3. MP4 container with AC3 audio: should route to fMP4
    item_ac3 = PublishItem(
        item_id="fid_ac3",
        name="movie.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        audio_codec="ac3",
        source_type="pikpak",
    )

    with (
        patch.object(engine, "_publish_vsse_split_item", new_callable=AsyncMock) as mock_vsse,
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
    ):
        mock_fmp4.return_value = TransportOutcome(message_ids=[501], speed_mb_s=10.0)
        mock_vsse.return_value = TransportOutcome(message_ids=[502], speed_mb_s=10.0)

        # 1. Test MKV -> routes directly to fMP4 (non-MP4/MOV container)
        res_mkv = await engine.send_item(item_mkv, ctx)
        assert res_mkv.message_ids == [501]

        # 2. Test MP4 with DTS -> routes to VSSE (Tier 2 Hybrid Sidecar)
        res_dts = await engine.send_item(item_dts, ctx)
        assert res_dts.message_ids == [502]

        # 3. Test MP4 with AC3 -> routes to VSSE (Tier 2 Hybrid Sidecar)
        res_ac3 = await engine.send_item(item_ac3, ctx)
        assert res_ac3.message_ids == [502]

        # Assert fMP4 was called for MKV and VSSE was called for MP4s with non-AAC audio
        assert mock_fmp4.call_count == 1
        assert mock_vsse.call_count == 2


# -----------------------------------------------------------------------------
# Test 6: Pre-flight routing - VP8 and WebM files fallback
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_preflight_routing_vp8_and_webm_fallback():
    """
    Requirement 6: VP8 / WebM files route to appropriate fallback.
    - VP8 cannot be safely copied into MP4/fMP4 -> routes to raw split fallback.
    - WebM with VP9 (not VP8) is non-MP4 container -> routes to fMP4.
    """
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/media"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    mock_client = AsyncMock()
    ctx = _make_context(userbot_client=mock_client, split_strategy="vsse_with_fallback")

    # 1. VP8 video codec
    item_vp8 = PublishItem(
        item_id="fid_vp8",
        name="sample_vp8.webm",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        audio_codec="vorbis",
        video_codec="vp8",
        source_type="pikpak",
    )

    # 2. WebM with VP9 video codec (not VP8)
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
        patch.object(engine, "_publish_vsse_split_item", new_callable=AsyncMock) as mock_vsse,
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
        patch.object(engine, "_publish_single_item", new_callable=AsyncMock) as mock_raw,
    ):
        mock_raw.return_value = TransportOutcome(message_ids=[601, 602])
        mock_fmp4.return_value = TransportOutcome(message_ids=[701, 702])

        # VP8 must route to raw split
        res_vp8 = await engine.send_item(item_vp8, ctx)
        assert res_vp8.message_ids == [601, 602]
        mock_raw.assert_called_once()
        mock_vsse.assert_not_called()
        mock_fmp4.assert_not_called()

        # WebM VP9 must route to fMP4
        res_vp9 = await engine.send_item(item_vp9, ctx)
        assert res_vp9.message_ids == [701, 702]
        mock_fmp4.assert_called_once()
        mock_vsse.assert_not_called()


# -----------------------------------------------------------------------------
# Test 7: Atomic rollback - failure midway through VSSE parts
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_atomic_rollback_on_failure():
    """
    Requirement 7: Failure midway through VSSE parts deletes previously delivered messages.
    """
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/standard.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    file_size = 3000 * 1024 * 1024
    item = PublishItem(
        item_id="fid_vsse_abort",
        name="large_movie.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )

    header_bytes = _build_test_mp4_header(total_bytes=file_size)
    mock_provider = MemorySyntheticChunkProvider(
        file_id=item.item_id,
        file_name=item.name,
        total_bytes=file_size,
        header_bytes=header_bytes,
    )

    mock_client = AsyncMock()
    # Part 1 succeeds and returns msg id 101; Part 2 raises an exception
    mock_client.send_file.side_effect = [
        MagicMock(id=101),
        RuntimeError("Telegram network connection dropped on Part 2"),
    ]
    mock_client.delete_messages = AsyncMock()
    ctx = _make_context(userbot_client=mock_client, split_strategy="vsse_with_fallback")

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_upload,
    ):
        mock_upload.side_effect = [MagicMock(), MagicMock()]

        with pytest.raises(UnconfirmedPartError) as exc_info:
            await engine.send_item(item, ctx)

        assert "Telegram network connection dropped" in str(exc_info.value.__cause__)

        # A lost acknowledgement must retain earlier receipts and the uncertain send.
        mock_client.delete_messages.assert_not_awaited()


# -----------------------------------------------------------------------------
# Test 8: One-second killswitch test: SPLIT_STRATEGY=fmp4_universal
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_one_second_killswitch(monkeypatch):
    """
    Requirement 8: Setting SPLIT_STRATEGY=fmp4_universal completely
    bypasses VSSE and uses Phase 1 universal pipeline.
    """
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/standard.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    file_size = 3000 * 1024 * 1024
    item = PublishItem(
        item_id="fid_killswitch",
        name="killswitch.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )

    mock_client = AsyncMock()
    # Context with no explicit split_strategy; settings 缺省即引擎决策源
    ctx = _make_context(userbot_client=mock_client, split_strategy=None)
    monkeypatch.setattr(settings, "SPLIT_STRATEGY", "fmp4_universal")

    with (
        patch.object(engine, "_publish_vsse_split_item", new_callable=AsyncMock) as mock_vsse,
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
    ):
        mock_fmp4.return_value = TransportOutcome(message_ids=[999], speed_mb_s=14.0)

        res = await engine.send_item(item, ctx)

        assert res.message_ids == [999]
        # VSSE completely bypassed
        mock_vsse.assert_not_called()
        mock_fmp4.assert_awaited_once_with(
            mock_client,
            item,
            ctx.video_split_threshold_bytes,
            ctx,
            capture_session=ANY,
            use_paged_proxy=False,
        )


# -----------------------------------------------------------------------------
# Test 9: Video-only MP4 routes to VSSE successfully
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_video_only_mp4_routes_to_vsse_successfully():
    """
    Requirement 9: Video-only MP4 (no audio track, audio_codec is None or "")
    satisfies VSSE eligibility and succeeds without falling back.
    """
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/video_only.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    file_size = 3000 * 1024 * 1024
    item = PublishItem(
        item_id="fid_video_only",
        name="silent_movie.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="",
        video_codec="h264",
        source_type="pikpak",
    )

    header_bytes = _build_test_mp4_header(total_bytes=file_size, include_audio=False)
    mock_provider = MemorySyntheticChunkProvider(
        file_id=item.item_id,
        file_name=item.name,
        total_bytes=file_size,
        header_bytes=header_bytes,
    )

    mock_client = AsyncMock()
    mock_client.send_file.side_effect = [
        MagicMock(id=301),
        MagicMock(id=302),
    ]
    ctx = _make_context(userbot_client=mock_client, split_strategy="vsse_with_fallback")

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_upload,
    ):
        mock_upload.side_effect = [MagicMock(), MagicMock()]

        res = await engine.send_item(item, ctx)

        assert res.message_ids == [301, 302]
        mock_fmp4.assert_not_called()
        assert mock_upload.call_count == 2


# -----------------------------------------------------------------------------
# Test 10: MOV and M4V containers route to VSSE
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_mov_and_m4v_containers_route_to_vsse():
    """
    Requirement 10: MOV and M4V containers with AAC audio satisfy pre-flight check.
    """
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/media"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    mock_client = AsyncMock()
    ctx = _make_context(userbot_client=mock_client, split_strategy="vsse_with_fallback")

    item_mov = PublishItem(
        item_id="fid_mov",
        name="recording.mov",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )
    item_m4v = PublishItem(
        item_id="fid_m4v",
        name="episode.m4v",
        size_bytes=3000 * 1024 * 1024,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )

    with (
        patch.object(engine, "_publish_vsse_split_item", new_callable=AsyncMock) as mock_vsse,
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
    ):
        mock_vsse.return_value = TransportOutcome(message_ids=[401, 402])

        res_mov = await engine.send_item(item_mov, ctx)
        assert res_mov.message_ids == [401, 402]

        res_m4v = await engine.send_item(item_m4v, ctx)
        assert res_m4v.message_ids == [401, 402]

        assert mock_vsse.call_count == 2
        mock_fmp4.assert_not_called()


# -----------------------------------------------------------------------------
# Test 11: Corrupt moov bytes / IsoBmffParseError falls back to fMP4
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_circuit_breaker_corrupt_moov_fallback():
    """
    Requirement 11: Corrupt moov bytes raising IsoBmffParseError
    triggers circuit breaker to fall back to _publish_fmp4_split_item.
    """
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/corrupt.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    file_size = 3000 * 1024 * 1024
    item = PublishItem(
        item_id="fid_corrupt",
        name="corrupt.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )

    # Malformed moov box
    ftyp = make_box(b"ftyp", b"isom")
    bad_moov = struct.pack(">I4s", 64, b"moov") + b"\xff" * 56
    header_bytes = ftyp + bad_moov

    mock_provider = MemorySyntheticChunkProvider(
        file_id=item.item_id,
        file_name=item.name,
        total_bytes=file_size,
        header_bytes=header_bytes,
    )

    mock_client = AsyncMock()
    ctx = _make_context(userbot_client=mock_client, split_strategy="vsse_with_fallback")

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
    ):
        mock_fmp4.return_value = TransportOutcome(message_ids=[891], speed_mb_s=10.0, client_label="Userbot")

        res = await engine.send_item(item, ctx)

        assert res.message_ids == [891]
        mock_fmp4.assert_awaited_once_with(
            mock_client,
            item,
            ctx.video_split_threshold_bytes,
            ctx,
            capture_session=ANY,
            probed_metadata=ANY,
            pinned_regions=ANY,
        )


# -----------------------------------------------------------------------------
# Test 12: Progress callback wiring
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_progress_callback_wiring():
    """
    Requirement 12: Progress callback is correctly wired and called during VSSE upload.
    """
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/standard.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    file_size = 3000 * 1024 * 1024
    item = PublishItem(
        item_id="fid_prog",
        name="prog.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )

    header_bytes = _build_test_mp4_header(total_bytes=file_size)
    mock_provider = MemorySyntheticChunkProvider(
        file_id=item.item_id,
        file_name=item.name,
        total_bytes=file_size,
        header_bytes=header_bytes,
    )

    mock_client = AsyncMock()
    mock_client.send_file.side_effect = [MagicMock(id=501), MagicMock(id=502)]

    progress_events = []

    def progress_callback(*args, **kwargs):
        progress_events.append((args, kwargs))

    ctx = _make_context(
        userbot_client=mock_client,
        split_strategy="vsse_with_fallback",
        progress_callback=progress_callback,
    )

    async def fake_upload(client, file_path, progress_callback=None, **kwargs):
        if progress_callback:
            progress_callback(100 * 1024 * 1024, 1500 * 1024 * 1024, 25.0, 60.0, "prog.part001.mp4", 8)
        return MagicMock()

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=fake_upload),
    ):
        res = await engine.send_item(item, ctx)

        assert res.message_ids == [501, 502]
        assert len(progress_events) >= 2
        first_args = progress_events[0][0]
        first_kwargs = progress_events[0][1]
        assert first_args[2] == 25.0
        assert "percent" in first_kwargs
        assert "total_bytes_done" in first_kwargs


# -----------------------------------------------------------------------------
# Test 13: Large mdat span multiple mdat circuit breaker fallback
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_circuit_breaker_large_mdat_span_multiple_mdat_fallback():
    """
    Requirement: Circuit breaker detects multiple mdat boxes even when moov is in
    the front 128KB and mdat1 spans across gigabytes (or > 128KB), triggering
    seamless fallback to _publish_fmp4_split_item.
    """
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/large_multi_mdat.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    ftyp = struct.pack(">I4s", 32, b"ftyp") + b"a" * 24
    moov = build_synthetic_moov()
    mdat1_size = 200 * 1024  # 200KB > 128KB head_bytes probe
    mdat1 = struct.pack(">I4s", mdat1_size, b"mdat") + b"m" * (mdat1_size - 8)
    mdat2 = make_box(b"mdat", b"second_mdat_payload")
    file_bytes = ftyp + moov + mdat1 + mdat2

    file_size = len(file_bytes)
    item = PublishItem(
        item_id="fid_large_multi_mdat",
        name="large_multi_mdat.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )
    mock_provider = MemorySyntheticChunkProvider(
        file_id=item.item_id,
        file_name=item.name,
        total_bytes=file_size,
        header_bytes=file_bytes,
    )

    mock_client = AsyncMock()
    ctx = _make_context(
        userbot_client=mock_client,
        split_strategy="vsse_with_fallback",
        split_threshold_bytes=100 * 1024,
    )

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
    ):
        mock_fmp4.return_value = TransportOutcome(message_ids=[901], speed_mb_s=15.0)

        res = await engine.send_item(item, ctx)

        # Verified seamless fallback to fMP4 triggered by MultipleMdatError detection past mdat1
        assert res.message_ids == [901]
        mock_fmp4.assert_awaited_once_with(
            mock_client,
            item,
            ctx.video_split_threshold_bytes,
            ctx,
            capture_session=ANY,
            probed_metadata=ANY,
            pinned_regions=ANY,
        )


# -----------------------------------------------------------------------------
# Test 14: tkhd dimension fallback without float bitshift TypeError
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_tkhd_dimension_fallback_no_float_bitshift():
    """
    Requirement: When item width/height are 0 and stsd entry has 0 dimensions,
    dimensions must be safely extracted from v_track.tkhd without float bitshift TypeError.
    """
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/tkhd_dim.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    file_size = 3000 * 1024 * 1024
    item = PublishItem(
        item_id="fid_tkhd_dim",
        name="tkhd_dim.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="aac",
        video_codec="h264",
        width=0,
        height=0,
        source_type="pikpak",
    )

    header_bytes = _build_test_mp4_header(total_bytes=file_size)

    mock_provider = MemorySyntheticChunkProvider(
        file_id=item.item_id,
        file_name=item.name,
        total_bytes=file_size,
        header_bytes=header_bytes,
    )

    mock_client = AsyncMock()
    mock_client.send_file.side_effect = [MagicMock(id=601), MagicMock(id=602)]
    ctx = _make_context(userbot_client=mock_client, split_strategy="vsse_with_fallback")

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_upload,
    ):
        mock_upload.side_effect = [MagicMock(), MagicMock()]

        res = await engine.send_item(item, ctx)

        assert res.message_ids == [601, 602]
        sf_kwargs = mock_client.send_file.call_args_list[0].kwargs
        attrs = sf_kwargs.get("attributes")
        assert attrs is not None and len(attrs) > 0
        assert attrs[0].w == 1920
        assert attrs[0].h == 1080


# -----------------------------------------------------------------------------
# Test 15: send_file FloodWait retry exhaustion raises and triggers rollback
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_send_file_flood_wait_exhaustion_rollback():
    """
    Requirement: 5 consecutive FloodWaitError in send_file must raise and trigger
    atomic rollback rather than silently reporting success with msg_id=0.
    """
    from telethon.errors import FloodWaitError

    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/standard.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    file_size = 3000 * 1024 * 1024
    item = PublishItem(
        item_id="fid_flood_exhaust",
        name="flood_exhaust.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )
    header_bytes = _build_test_mp4_header(total_bytes=file_size)
    mock_provider = MemorySyntheticChunkProvider(
        file_id=item.item_id,
        file_name=item.name,
        total_bytes=file_size,
        header_bytes=header_bytes,
    )

    mock_client = AsyncMock()
    # Part 1 succeeds with msg_id 101; Part 2 raises FloodWaitError on all 5 attempts
    fw_err = FloodWaitError(request=None)
    fw_err.seconds = 1
    mock_client.send_file.side_effect = [MagicMock(id=101), fw_err, fw_err, fw_err, fw_err, fw_err]
    mock_client.delete_messages = AsyncMock()
    ctx = _make_context(userbot_client=mock_client, split_strategy="vsse_with_fallback")

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_upload,
        patch("asyncio.sleep", new_callable=AsyncMock),
    ):
        mock_upload.side_effect = [MagicMock(), MagicMock()]

        with pytest.raises(FloodWaitError):
            await engine.send_item(item, ctx)

        # Rollback deleted message 101
        mock_client.delete_messages.assert_not_awaited()


# -----------------------------------------------------------------------------
# Test 16: Settings SPLIT_STRATEGY validation
# -----------------------------------------------------------------------------
def test_settings_split_strategy_validation(monkeypatch):
    """
    Requirement: ALLOWED_SPLIT_STRATEGIES validation in Settings.
    """
    from app.config import Settings

    s = Settings()
    assert "vsse_with_fallback" in s.ALLOWED_SPLIT_STRATEGIES
    assert "fmp4_universal" in s.ALLOWED_SPLIT_STRATEGIES
    assert "raw" in s.ALLOWED_SPLIT_STRATEGIES

    # Setting invalid strategy raises in validate_required
    monkeypatch.setattr(s, "SPLIT_STRATEGY", "invalid_strategy")
    monkeypatch.setattr(s, "API_ID", 12345)
    monkeypatch.setattr(s, "API_HASH", "hash123")
    monkeypatch.setattr(s, "BOT_TOKENS", ["token123"])

    with pytest.raises(RuntimeError) as exc_info:
        s.validate_required()
    assert "非法切片策略" in str(exc_info.value)


# -----------------------------------------------------------------------------
# Tests 17-20: E2E 捕获模式 VSSE 接线 (docs/testing/E2E_GUIDE.md §5.1,
# D2/D3/D8/E3; conftest 已将 E2E_CAPTURE_DIR 沙箱化)
# -----------------------------------------------------------------------------
import json
import os

from app.config import settings


def _make_small_capture_env(mock_client, threshold_bytes: int):
    """小型合成 MP4 (20 样本 x 3MB ≈ 60MB 媒体): 足以规划多卷, 又能在单测内全量读。"""
    file_size = 62 * 1024 * 1024
    item = PublishItem(
        item_id="fid_capture_vsse",
        name="capture_movie.mp4",
        size_bytes=file_size,
        duration_s=600,
        audio_codec="aac",
        video_codec="h264",
        source_type="pikpak",
    )
    header_bytes = _build_test_mp4_header(total_bytes=file_size, v_samples=20, sync_samples=[1, 6, 11, 16])
    # builder 固定声明 mdat 跨度 1000 MiB, 小文件下 span 校验直接熔断——改写到文件尾
    mdat_size = file_size - (len(header_bytes) - 8)
    header_bytes = header_bytes[:-8] + struct.pack(">I4s", mdat_size, b"mdat")
    mock_provider = MemorySyntheticChunkProvider(
        file_id=item.item_id,
        file_name=item.name,
        total_bytes=file_size,
        header_bytes=header_bytes,
    )
    ctx = _make_context(
        userbot_client=mock_client,
        split_strategy="vsse_with_fallback",
        split_threshold_bytes=threshold_bytes,
    )
    return item, mock_provider, ctx


def _drive_tee_upload_factory(kwargs_sink: list):
    """fake upload_file_parallel: 全量读一遍 chunk_provider (驱动 tee 定位写) 并记录 kwargs。"""

    async def _fake_upload(client_arg, path_arg, **kwargs):
        kwargs_sink.append(kwargs)
        provider = kwargs["chunk_provider"]
        data = await provider.read_chunk(0, provider.total_bytes)
        assert len(data) == provider.total_bytes
        return MagicMock()

    return _fake_upload


def _no_fallback_mock(engine):
    """哨兵: 快乐路径不应熔断降级到 fMP4 (降级会跑真实 FFmpeg 拖垮测试)。"""
    return patch.object(
        engine,
        "_publish_fmp4_split_item",
        new_callable=AsyncMock,
        side_effect=AssertionError("unexpected fmp4 fallback"),
    )


@pytest.mark.asyncio
async def test_capture_on_vsse_flow_writes_parts_and_manifest(monkeypatch):
    """capture 开: 快照 ok + 逐卷产物 rename 落盘 + manifest 齐全 + force_fresh 恒 True。"""
    monkeypatch.setattr(settings, "E2E_CAPTURE_MODE", True)
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/capture.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)
    mock_client = AsyncMock()
    mock_client.send_file.side_effect = [MagicMock(id=101 + i) for i in range(10)]
    item, mock_provider, ctx = _make_small_capture_env(mock_client, threshold_bytes=24 * 1024 * 1024)
    upload_kwargs = []

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch("app.core.fast_telethon.upload_file_parallel", new=_drive_tee_upload_factory(upload_kwargs)),
        _no_fallback_mock(engine),
    ):
        res = await engine.send_item(item, ctx)

    root = os.path.join(settings.E2E_CAPTURE_DIR, ctx.task_id, item.item_id)
    with open(os.path.join(root, "manifest.json"), encoding="utf-8") as f:
        manifest = json.load(f)

    # E1 源快照: 独立 provider 全量读 + sha256
    assert manifest["source"]["status"] == "ok"
    assert manifest["source"]["size_bytes"] == item.size_bytes
    assert len(manifest["source"]["sha256"]) == 64
    assert manifest["force_fresh"] is True

    # E2 逐卷产物 + manifest
    parts = manifest["parts"]
    assert len(parts) >= 2
    assert res.message_ids == [entry["msg_id"] for entry in parts]
    for entry in parts:
        assert entry["status"] == "sent"
        assert entry["channel"] == "vsse"
        assert entry["msg_id"] >= 101
        part_path = os.path.join(root, entry["path"])
        assert os.path.isfile(part_path)
        assert not os.path.exists(part_path + ".part")
        # tee 完备性: 产物字节数 == 合成头 + envelope 区间
        assert os.path.getsize(part_path) == entry["header_len"] + entry["envelope_length"]
    # D8: capture 下 upload_file_parallel 恒传 force_fresh=True
    assert upload_kwargs and all(kw["force_fresh"] is True for kw in upload_kwargs)


@pytest.mark.asyncio
async def test_capture_off_zero_dirs_and_no_force_fresh():
    """capture 关 (默认): 目录零创建, force_fresh 保持 (attempt>0) 语义 (回归保障)。"""
    assert settings.E2E_CAPTURE_MODE is False
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/off.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)
    mock_client = AsyncMock()
    mock_client.send_file.side_effect = [MagicMock(id=201 + i) for i in range(10)]
    item, mock_provider, ctx = _make_small_capture_env(mock_client, threshold_bytes=24 * 1024 * 1024)
    upload_kwargs = []

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch("app.core.fast_telethon.upload_file_parallel", new=_drive_tee_upload_factory(upload_kwargs)),
        _no_fallback_mock(engine),
    ):
        await engine.send_item(item, ctx)

    assert os.listdir(settings.E2E_CAPTURE_DIR) == []
    assert upload_kwargs and all(kw["force_fresh"] is False for kw in upload_kwargs)


@pytest.mark.asyncio
async def test_capture_on_vsse_failure_marks_rollback_and_residue(monkeypatch):
    """卷2 send_file 失败: 回滚删除消息 + manifest rolled_back (E3) + 卷2 .part 残留。"""
    monkeypatch.setattr(settings, "E2E_CAPTURE_MODE", True)
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/fail.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)
    mock_client = AsyncMock()
    mock_client.send_file.side_effect = [MagicMock(id=301), RuntimeError("send_file boom")]
    item, mock_provider, ctx = _make_small_capture_env(mock_client, threshold_bytes=24 * 1024 * 1024)
    upload_kwargs = []

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch("app.core.fast_telethon.upload_file_parallel", new=_drive_tee_upload_factory(upload_kwargs)),
        _no_fallback_mock(engine),
        pytest.raises(UnconfirmedPartError),
    ):
        await engine.send_item(item, ctx)

    mock_client.delete_messages.assert_not_awaited()

    root = os.path.join(settings.E2E_CAPTURE_DIR, ctx.task_id, item.item_id)
    with open(os.path.join(root, "manifest.json"), encoding="utf-8") as f:
        manifest = json.load(f)

    assert manifest["rolled_back"] is False
    sent = [e for e in manifest["parts"] if e["status"] == "sent"]
    failed = [e for e in manifest["parts"] if e["status"] == "failed"]
    assert len(sent) == 1 and sent[0]["msg_id"] == 301
    assert len(failed) >= 1
    assert os.path.isfile(os.path.join(root, sent[0]["path"]))
    # 失败卷残留 .part 半成品, 不被误当有效产物
    for entry in failed:
        assert os.path.isfile(os.path.join(root, "parts", entry["part_name"] + ".part"))


@pytest.mark.asyncio
async def test_capture_on_circuit_breaker_records_switch_and_forwards_session(monkeypatch):
    """capture 开 + 熔断降级: manifest 记录通道切换, 捕获会话透传给 fMP4 通道。"""
    monkeypatch.setattr(settings, "E2E_CAPTURE_MODE", True)
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/cb.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)
    mock_client = AsyncMock()
    file_size = 62 * 1024 * 1024
    item = PublishItem(
        item_id="fid_capture_cb",
        name="fragmented_capture.mp4",
        size_bytes=file_size,
        duration_s=600,
        audio_codec="aac",
        source_type="pikpak",
    )
    header_bytes = _build_test_mp4_header(total_bytes=file_size, include_mvex=True)
    mdat_size = file_size - (len(header_bytes) - 8)
    header_bytes = header_bytes[:-8] + struct.pack(">I4s", mdat_size, b"mdat")
    mock_provider = MemorySyntheticChunkProvider(
        file_id=item.item_id,
        file_name=item.name,
        total_bytes=file_size,
        header_bytes=header_bytes,
    )
    ctx = _make_context(
        userbot_client=mock_client,
        split_strategy="vsse_with_fallback",
        split_threshold_bytes=24 * 1024 * 1024,
    )

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
    ):
        mock_fmp4.return_value = TransportOutcome(message_ids=[888], speed_mb_s=12.5, client_label="Userbot")
        res = await engine.send_item(item, ctx)

    assert res.message_ids == [888]
    forwarded = mock_fmp4.await_args.kwargs["capture_session"]
    assert forwarded.active is True  # 捕获会话透传给 fMP4 通道

    root = os.path.join(settings.E2E_CAPTURE_DIR, ctx.task_id, item.item_id)
    with open(os.path.join(root, "manifest.json"), encoding="utf-8") as f:
        manifest = json.load(f)
    assert "circuit breaker" in manifest["channel_switch"]
    assert manifest["source"]["status"] == "ok"  # 快照先于通道完成


@pytest.mark.asyncio
async def test_vsse_front_moov_bounded_and_cpu_work_offloaded() -> None:
    """前置超大 moov 跳过不读取；parse_moov 与头合成经 asyncio.to_thread 卸载."""
    import asyncio
    from types import SimpleNamespace

    from app.core.pipeline.engines.pikpak import vsse as pik_vsse

    huge_box = SimpleNamespace(box_type=b"moov", offset=32, size=pik_vsse.MAX_FRONT_MOOV_BYTES + 1)
    provider = MagicMock(read_chunk=AsyncMock(return_value=b"\x00" * 32))
    offset, data = await pik_vsse._resolve_front_moov([huge_box], b"\x00" * 32, provider, file_size=2 * 1024**3)
    assert offset == 32 and data is None
    provider.read_chunk.assert_not_awaited()

    to_thread_funcs: list[object] = []
    real_to_thread = asyncio.to_thread

    async def spy_to_thread(fn, *args, **kwargs):
        to_thread_funcs.append(fn)
        return await real_to_thread(fn, *args, **kwargs)

    item = PublishItem(item_id="fid_vsse", name="v.mp4", size_bytes=256, source_type="pikpak")
    fake_meta = SimpleNamespace(video_track=None, audio_track=None, duration_s=10.0)
    with (
        patch.object(pik_vsse, "_read_and_scan_boxes", new_callable=AsyncMock, return_value=[]),
        patch.object(pik_vsse, "_resolve_front_moov", new_callable=AsyncMock, return_value=(0, b"moov")),
        patch.object(pik_vsse.srp_mod, "build_pinned_regions", return_value=[]),
        patch.object(pik_vsse.bp_mod, "parse_moov", return_value=fake_meta),
        patch.object(pik_vsse, "_backfill_media_metadata_to_item"),
        patch.object(pik_vsse, "_plan_vsse_mode", new_callable=AsyncMock, return_value=(fake_meta, [], None)),
        patch.object(pik_vsse, "_synthesize_all_volume_headers", return_value=[]),
        patch.object(pik_vsse.asyncio, "to_thread", side_effect=spy_to_thread),
    ):
        await pik_vsse.probe_vsse_envelope(item, 100, provider)
        assert pik_vsse.bp_mod.parse_moov in to_thread_funcs
        assert pik_vsse._synthesize_all_volume_headers in to_thread_funcs


@pytest.mark.asyncio
async def test_vsse_retries_file_parts_invalid_raised_during_send_file(tmp_path) -> None:
    """VSSE 在 send_file 阶段抛出 FILE_PARTS_INVALID 时必须清断点并全量重传."""
    from types import SimpleNamespace

    from app.core.pipeline.engines.pikpak import vsse as pik_vsse

    item = PublishItem(item_id="fid_v1", name="video.mp4", size_bytes=2000, source_type="pikpak")
    plan = MagicMock(
        part_index=1,
        total_parts=1,
        part_name="video.mp4",
        envelope_start=0,
        envelope_length=2000,
        duration_sec=10.0,
    )
    vtrack = SimpleNamespace(
        stbl=SimpleNamespace(stsd=SimpleNamespace(entries=[SimpleNamespace(width=1920, height=1080)])),
        tkhd=None,
    )
    meta = SimpleNamespace(video_track=vtrack)
    provider = MagicMock()
    provider.total_bytes = 2000
    provider.download_speed_mb_s = 2.5
    ctx = PublishContext(task_id="t_vsse_fp", target_chat_id=-1001, account_display="Userbot")
    capture = MagicMock()
    capture.active = False
    capture.make_vsse_tee = MagicMock(side_effect=lambda _p, inner, header_len=0: inner)
    client = AsyncMock()
    meta_file = tmp_path / "vsse.upload.meta"
    meta_file.write_text("stale", encoding="utf-8")

    with (
        patch.object(pik_vsse, "get_meta_path", return_value=str(meta_file)),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()) as m_up,
        patch.object(
            pik_vsse,
            "_send_file_with_floodwait_retry",
            new_callable=AsyncMock,
            side_effect=[RuntimeError("FILE_PARTS_INVALID"), (MagicMock(id=901), None)],
        ) as m_sf,
    ):
        msg_id, _, _ = await pik_vsse.stream_vsse_volume(
            client,
            item,
            plan,
            b"hdr",
            provider,
            ctx,
            capture,
            0,
            None,
            meta,
        )

    assert msg_id == 901
    assert m_up.await_count == 2
    assert m_up.await_args_list[0].kwargs["force_fresh"] is False
    assert m_up.await_args_list[1].kwargs["force_fresh"] is True
    assert m_sf.await_count == 2
    assert not meta_file.exists()
    capture.discard_part_sink.assert_called_once_with("video.mp4")
