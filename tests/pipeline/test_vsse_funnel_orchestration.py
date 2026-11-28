"""
Unit and Integration Tests for Three-Tier Funnel Orchestration and Metadata Passthrough.

Covers:
1. Tier 1 -> Tier 2 in-place upgrade when classify_vsse_mode detects non-AAC audio (HYBRID_AUDIO_SIDECAR).
2. Tier 1 -> Tier 2 in-place upgrade when plan_vsse_volumes encounters NonInterleavedMp4Error.
3. Tier 1/2 -> Tier 3 Circuit Breaker passthrough: probed_metadata and pinned_regions passed to _publish_fmp4_split_item.
4. _probe_fmp4_media zero trans-oceanic network re-probing when probed_metadata is present.
5. LocalPagedRangeProxy wiring in _publish_fmp4_split_item when pinned_regions is provided.
6. Backward compatibility of _publish_fmp4_split_item when pinned_regions is None (direct CDN URL).
7. Tier 2 sidecar failure triggers circuit breaker with Stage 0 metadata/pinned-region passthrough (Test 14).
8. Proxy-mode URL hot-refresh on oversize replan via inner_provider.update_url (Test 15).
"""

import array
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.iso_bmff.box_parser import (
    Hdlr,
    Mdhd,
    Mp4Metadata,
    Mvhd,
    SampleEntry,
    SampleTable,
    StscEntry,
    Stsd,
    SttsEntry,
    Tkhd,
    Track,
)
from app.core.iso_bmff.exceptions import (
    NonInterleavedMp4Error,
)
from app.core.iso_bmff.fragmented import Fmp4PartPlan
from app.core.pipeline.engines import PikPakStreamTransportEngine
from app.core.pipeline.engines.pikpak.fmp4 import probe_fmp4_media
from app.core.pipeline.engines.pikpak.vsse import probe_vsse_envelope
from app.core.pipeline.models import PublishItem, TransportOutcome
from app.core.vsse_audio_sidecar import SidecarAudioBundle
from app.core.vsse_provider import VsseVolumePlan
from tests.pipeline.test_vsse_engine_contract import (
    MemorySyntheticChunkProvider,
    _build_test_mp4_header,
    _make_context,
)


def _build_synthetic_metadata(
    *,
    video_codec: str = "avc1",
    audio_codec: str = "mp4a",
    duration_s: float = 60.0,
    width: int = 1920,
    height: int = 1080,
    v_samples: int = 600,
    a_samples: int = 1200,
) -> Mp4Metadata:
    """构建用于测试的合成 Mp4Metadata 对象。"""
    movie_timescale = 1000
    mvhd = Mvhd(
        version=0,
        flags=0,
        creation_time=0,
        modification_time=0,
        timescale=movie_timescale,
        duration=int(duration_s * movie_timescale),
        next_track_id=3,
    )

    # Video Track
    v_timescale = 30000
    v_stsd = Stsd(
        version=0,
        flags=0,
        entry_count=1,
        entries=[
            SampleEntry(
                format=video_codec.encode("ascii"),
                data_ref_index=1,
                codec=video_codec,
                width=width,
                height=height,
            )
        ],
    )
    v_stbl = SampleTable(
        stsd=v_stsd,
        stts=[SttsEntry(sample_count=v_samples, sample_delta=1000)],
        stsc=[StscEntry(first_chunk=1, samples_per_chunk=1, sample_description_index=1)],
        stsz=array.array("I", [1000] * v_samples),
        chunk_offsets=array.array("Q", [20000 + i * 1000 for i in range(v_samples)]),
        stss=array.array("I", [1, 301]),
    )
    v_track = Track(
        track_id=1,
        tkhd=Tkhd(
            version=0,
            flags=0,
            creation_time=0,
            modification_time=0,
            track_id=1,
            duration=int(duration_s * movie_timescale),
            width=float(width),
            height=float(height),
            width_raw=width << 16,
            height_raw=height << 16,
        ),
        mdhd=Mdhd(
            version=0,
            flags=0,
            creation_time=0,
            modification_time=0,
            timescale=v_timescale,
            duration=int(duration_s * v_timescale),
        ),
        hdlr=Hdlr(handler_type=b"vide", name="VideoHandler"),
        stbl=v_stbl,
    )

    # Audio Track
    a_timescale = 48000
    a_stsd = Stsd(
        version=0,
        flags=0,
        entry_count=1,
        entries=[
            SampleEntry(
                format=audio_codec.encode("ascii"),
                data_ref_index=1,
                codec=audio_codec,
                channel_count=2,
                sample_rate=48000,
            )
        ],
    )
    a_stbl = SampleTable(
        stsd=a_stsd,
        stts=[SttsEntry(sample_count=a_samples, sample_delta=1024)],
        stsc=[StscEntry(first_chunk=1, samples_per_chunk=1, sample_description_index=1)],
        stsz=array.array("I", [200] * a_samples),
        chunk_offsets=array.array("Q", [20500 + i * 200 for i in range(a_samples)]),
    )
    a_track = Track(
        track_id=2,
        tkhd=Tkhd(
            version=0,
            flags=0,
            creation_time=0,
            modification_time=0,
            track_id=2,
            duration=int(duration_s * movie_timescale),
            width=0.0,
            height=0.0,
            width_raw=0,
            height_raw=0,
        ),
        mdhd=Mdhd(
            version=0,
            flags=0,
            creation_time=0,
            modification_time=0,
            timescale=a_timescale,
            duration=int(duration_s * a_timescale),
        ),
        hdlr=Hdlr(handler_type=b"soun", name="SoundHandler"),
        stbl=a_stbl,
    )

    return Mp4Metadata(mvhd=mvhd, tracks=[v_track, a_track])


# -----------------------------------------------------------------------------
# Test 1: Tier 1 -> Tier 2 in-place upgrade on non-AAC audio
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_tier2_upgrade_on_non_aac_audio():
    """验证当 MP4 音频为 non-AAC (如 lpcm/ac-3) 时，_probe_vsse_envelope 原地升级到 Tier 2 (HYBRID_AUDIO_SIDECAR)。"""

    file_size = 3000 * 1024 * 1024
    # 构建含 lpcm 音频的测试文件头
    header_bytes = _build_test_mp4_header(
        total_bytes=file_size,
        audio_codec="lpcm",
    )
    provider = MemorySyntheticChunkProvider(
        file_id="fid_lpcm",
        file_name="movie.mp4",
        total_bytes=file_size,
        header_bytes=header_bytes,
    )

    item = PublishItem(
        item_id="fid_lpcm",
        name="movie.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="lpcm",
        source_type="pikpak",
    )

    hybrid_meta = _build_synthetic_metadata(audio_codec="mp4a")
    fake_bundle = SidecarAudioBundle(
        hybrid_metadata=hybrid_meta,
        aac_payload=b"FAKE_AAC_PAYLOAD_BYTES" * 100,
    )

    with patch("app.core.vsse_audio_sidecar.build_sidecar_audio_bundle", new_callable=AsyncMock) as mock_bundle_builder:
        mock_bundle_builder.return_value = fake_bundle

        speed_provider = MagicMock()
        speed_provider.read_chunk = provider.read_chunk
        speed_provider.file_id = provider.file_id
        speed_provider.total_bytes = provider.total_bytes

        meta, plans, headers = await probe_vsse_envelope(item, 2000 * 1024 * 1024, speed_provider)

        # 验证 build_sidecar_audio_bundle 被调用
        mock_bundle_builder.assert_awaited_once()
        # 验证返回的元数据为经过混合替换的 hybrid_metadata
        assert meta.audio_track is not None
        assert meta.audio_track.stbl.stsd.entries[0].codec == "mp4a"
        # 验证生成的 plans 包含 sidecar 音频切片
        assert len(plans) >= 1
        assert len(headers) == len(plans)
        # 验证合成信封头部内联了 AAC 净荷切片
        for hdr in headers:
            assert b"moov" in hdr
            assert b"mdat" in hdr


# -----------------------------------------------------------------------------
# Test 2: Tier 1 -> Tier 2 in-place upgrade on NonInterleavedMp4Error
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_tier2_upgrade_on_non_interleaved_mp4():
    """验证当 MP4 音频虽为 AAC 但触发 NonInterleavedMp4Error 时，不降级 fMP4，而是原地升级到 Tier 2。"""

    file_size = 3000 * 1024 * 1024
    header_bytes = _build_test_mp4_header(
        total_bytes=file_size,
        audio_codec="mp4a",
    )
    provider = MemorySyntheticChunkProvider(
        file_id="fid_non_interleaved",
        file_name="non_interleaved.mp4",
        total_bytes=file_size,
        header_bytes=header_bytes,
    )

    item = PublishItem(
        item_id="fid_non_interleaved",
        name="non_interleaved.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )

    hybrid_meta = _build_synthetic_metadata(audio_codec="mp4a")
    fake_bundle = SidecarAudioBundle(
        hybrid_metadata=hybrid_meta,
        aac_payload=b"INTERLEAVED_REPACK_AAC" * 50,
    )

    orig_plan = VsseVolumePlan(
        part_index=1,
        total_parts=1,
        part_name="test.mp4",
        v_start=1,
        v_end=600,
        a_start=1,
        a_end=1200,
        o_min=20000,
        o_max=800000,
        payload_len=780000,
        estimated_header_size=1024,
        estimated_total_bytes=781024,
        start_sec=0.0,
        end_sec=60.0,
        duration_sec=60.0,
        a_o_min=0,
        a_o_max=len(fake_bundle.aac_payload),
        a_payload_len=len(fake_bundle.aac_payload),
    )

    plan_call_count = 0

    def mock_plan_vsse_volumes(*args, **kwargs):
        nonlocal plan_call_count
        plan_call_count += 1
        sidecar_audio = kwargs.get("sidecar_audio", False)
        if not sidecar_audio:
            raise NonInterleavedMp4Error("Simulated audio-video track physical separation")
        return [orig_plan]

    with (
        patch("app.core.vsse_provider.plan_vsse_volumes", side_effect=mock_plan_vsse_volumes),
        patch("app.core.vsse_audio_sidecar.build_sidecar_audio_bundle", new_callable=AsyncMock) as mock_builder,
    ):
        mock_builder.return_value = fake_bundle

        speed_provider = MagicMock()
        speed_provider.read_chunk = provider.read_chunk
        speed_provider.file_id = provider.file_id
        speed_provider.total_bytes = provider.total_bytes

        meta, plans, headers = await probe_vsse_envelope(item, 2000 * 1024 * 1024, speed_provider)

        assert plan_call_count == 2  # 第一次失败触发原地升级，第二次 sidecar_audio=True 成功
        mock_builder.assert_awaited_once()
        assert len(plans) == 1
        assert len(headers) == 1


# -----------------------------------------------------------------------------
# Test 3: Tier 1/2 -> Tier 3 Circuit Breaker passthrough
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_circuit_breaker_passthrough_probed_metadata_and_pinned():
    """验证 VSSE 熔断降级时，Stage 0 探测到的 Mp4Metadata 与 pinned_regions 100% 完整透传给 _publish_fmp4_split_item。"""
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/unsupported_video.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    file_size = 3000 * 1024 * 1024
    # 构造不支持的视频编码 (mpeg4)，使 classify_vsse_mode 抛出 UnsupportedCodecError
    header_bytes = _build_test_mp4_header(
        total_bytes=file_size,
        video_codec="mp4v",
    )
    mock_provider = MemorySyntheticChunkProvider(
        file_id="fid_unsupported_video",
        file_name="unsupported_video.mp4",
        total_bytes=file_size,
        header_bytes=header_bytes,
    )

    item = PublishItem(
        item_id="fid_unsupported_video",
        name="unsupported_video.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    ctx = _make_context(userbot_client=mock_client, split_strategy="vsse_with_fallback")

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
    ):
        mock_fmp4.return_value = TransportOutcome(message_ids=[777], speed_mb_s=10.0, client_label="Userbot")

        res = await engine.send_item(item, ctx)

        assert res.message_ids == [777]
        mock_fmp4.assert_awaited_once()
        _, call_kwargs = mock_fmp4.call_args

        # 核心断言: probed_metadata 与 pinned_regions 成功透传且非空
        probed_meta = call_kwargs.get("probed_metadata")
        pinned_reg = call_kwargs.get("pinned_regions")

        assert probed_meta is not None
        assert isinstance(probed_meta, Mp4Metadata)
        assert pinned_reg is not None
        assert len(pinned_reg) >= 1
        # 验证 L0 pinned 包含 head_bytes
        assert pinned_reg[0][0] == 0


# -----------------------------------------------------------------------------
# Test 4: _probe_fmp4_media zero network re-probe when probed_metadata is present
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
@pytest.mark.parametrize("edit_duration", [None, 90.0])
async def test_probe_fmp4_media_uses_probed_metadata_zero_network(edit_duration):
    """验证当传入 probed_metadata 时，_probe_fmp4_media 完全跳过跨洋 HTTP Range 探测。"""
    from app.core.iso_bmff.box_parser import Edts, ElstEntry

    meta = _build_synthetic_metadata(
        video_codec="avc1",
        audio_codec="mp4a",
        duration_s=120.0,
        width=3840,
        height=2160,
    )
    if edit_duration is not None:
        assert meta.video_track is not None
        meta.video_track.edts = Edts(entries=[ElstEntry(int(edit_duration * meta.mvhd.timescale), 0)])

    item = PublishItem(
        item_id="fid_skip_probe",
        name="movie.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=60.0,
        audio_codec=None,
        video_codec=None,
        width=None,
        height=None,
        source_type="pikpak",
    )
    ctx = _make_context()

    with (
        patch(
            "app.core.keyframe_prober.Mp4StssProber.probe_from_url",
            side_effect=AssertionError("网络探测严禁被调用 (应直接使用 probed_metadata)"),
        ),
        patch(
            "app.core.keyframe_prober.MkvCuesProber.probe_from_url",
            side_effect=AssertionError("MkvCuesProber 严禁被调用"),
        ),
    ):
        ext, copy_audio, duration, keyframe_pts = await probe_fmp4_media(
            "https://cdn.example.com/movie.mp4",
            item,
            ctx,
            probed_metadata=meta,
        )

        assert ext == ".mp4"
        assert copy_audio is True
        assert duration == item.duration_s == (edit_duration or 120.0)
        assert item.width == 3840
        assert item.height == 2160
        assert item.video_codec == "avc1"
        assert item.audio_codec == "mp4a"
        assert keyframe_pts is not None
        assert len(keyframe_pts) == 2  # samples 1 and 301


# -----------------------------------------------------------------------------
# Test 5: LocalPagedRangeProxy wiring in _publish_fmp4_split_item
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_publish_fmp4_split_item_wires_local_paged_range_proxy():
    """验证当 pinned_regions 传入时，_publish_fmp4_split_item 启动 LocalPagedRangeProxy 并向上传器传递回环双流 URL。"""
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/target.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    meta = _build_synthetic_metadata(duration_s=60.0)
    pinned_regions = [(0, b"HEADER_128K"), (2000000, b"MOOV_BOX_BYTES")]

    item = PublishItem(
        item_id="fid_proxy_test",
        name="target.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=60.0,
        audio_codec="aac",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=991)
    ctx = _make_context(userbot_client=mock_client)

    plans = [
        Fmp4PartPlan(
            part_index=1,
            total_parts=1,
            start_sec=0.0,
            end_sec=60.0,
            duration_sec=60.0,
            part_name="target.mp4",
        )
    ]

    called_urls = []
    created_proxies = []
    from app.core.stream_readahead_proxy import LocalPagedRangeProxy

    orig_init = LocalPagedRangeProxy.__init__

    def spy_proxy_init(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)
        created_proxies.append(self)

    async def fake_stream_to_telegram(**kwargs):
        called_urls.append(kwargs.get("pikpak_url"))
        return MagicMock(), 60.0

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.keyframe_prober.Mp4StssProber.probe_from_url", new_callable=AsyncMock, return_value=None),
        patch("app.core.keyframe_prober.MkvCuesProber.probe_from_url", new_callable=AsyncMock, return_value=None),
        patch("app.core.pikpak.thumbnail.fetch_thumbnail", new_callable=AsyncMock, return_value=None),
        patch("app.core.pikpak.thumbnail.extract_frame_ffmpeg", new_callable=AsyncMock, return_value=None),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch.object(LocalPagedRangeProxy, "__init__", side_effect=spy_proxy_init, autospec=True),
        patch(
            "app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram",
            side_effect=fake_stream_to_telegram,
        ),
    ):
        outcome = await engine._publish_fmp4_split_item(
            mock_client,
            item,
            2000 * 1024 * 1024,
            ctx,
            probed_metadata=meta,
            pinned_regions=pinned_regions,
        )

        assert outcome.message_ids == [991]
        assert len(called_urls) == 1
        stream_url = called_urls[0]
        # 核心断言: 传入的是回环代理的双流 URL 对 (video_url, audio_url)
        assert isinstance(stream_url, tuple)
        assert len(stream_url) == 2
        assert stream_url[0].startswith("http://127.0.0.1:")
        assert stream_url[0].endswith("/video")
        assert stream_url[1].startswith("http://127.0.0.1:")
        assert stream_url[1].endswith("/audio")
        # 核心断言: 代理在退出时被正确关闭并释放 runner
        assert len(created_proxies) == 1
        assert created_proxies[0]._closed is True
        assert created_proxies[0]._runner is None


# -----------------------------------------------------------------------------
# Test 6: Backward compatibility of _publish_fmp4_split_item without pinned_regions
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_publish_fmp4_split_item_without_pinned_regions_uses_direct_url():
    """验证当 pinned_regions 为 None 时，_publish_fmp4_split_item 保持原有直连 URL 行为不变。"""
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/direct.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    item = PublishItem(
        item_id="fid_direct_test",
        name="direct.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=60.0,
        audio_codec="aac",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=992)
    ctx = _make_context(userbot_client=mock_client)

    plans = [
        Fmp4PartPlan(
            part_index=1,
            total_parts=1,
            start_sec=0.0,
            end_sec=60.0,
            duration_sec=60.0,
            part_name="direct.mp4",
        )
    ]

    called_urls = []

    async def fake_stream_to_telegram(**kwargs):
        called_urls.append(kwargs.get("pikpak_url"))
        return MagicMock(), 60.0

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.keyframe_prober.Mp4StssProber.probe_from_url", new_callable=AsyncMock, return_value=None),
        patch("app.core.keyframe_prober.MkvCuesProber.probe_from_url", new_callable=AsyncMock, return_value=None),
        patch("app.core.pikpak.thumbnail.fetch_thumbnail", new_callable=AsyncMock, return_value=None),
        patch("app.core.pikpak.thumbnail.extract_frame_ffmpeg", new_callable=AsyncMock, return_value=None),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch(
            "app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram",
            side_effect=fake_stream_to_telegram,
        ),
    ):
        outcome = await engine._publish_fmp4_split_item(
            mock_client,
            item,
            2000 * 1024 * 1024,
            ctx,
            pinned_regions=None,
        )

        assert outcome.message_ids == [992]
        assert len(called_urls) == 1
        # 直连 URL 保持原样字符串
        assert called_urls[0] == "https://cdn.example.com/direct.mp4"


# -----------------------------------------------------------------------------
# Test 7: Cancellation resilience of LocalPagedRangeProxy in _publish_fmp4_split_item
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_publish_fmp4_split_item_cancellation_cleans_up_proxy():
    """验证当传输遭遇 asyncio.CancelledError 时，LocalPagedRangeProxy 依然能被安全回收关闭，不悬挂端口。"""
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/cancel_test.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    meta = _build_synthetic_metadata(duration_s=60.0)
    pinned_regions = [(0, b"HEADER_128K")]

    item = PublishItem(
        item_id="fid_cancel_test",
        name="cancel_test.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=60.0,
        audio_codec="aac",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    ctx = _make_context(userbot_client=mock_client)

    plans = [
        Fmp4PartPlan(
            part_index=1,
            total_parts=1,
            start_sec=0.0,
            end_sec=60.0,
            duration_sec=60.0,
            part_name="cancel_test.mp4",
        )
    ]

    created_proxies = []
    from app.core.stream_readahead_proxy import LocalPagedRangeProxy

    orig_init = LocalPagedRangeProxy.__init__

    def spy_proxy_init(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)
        created_proxies.append(self)

    async def cancel_stream_to_telegram(**kwargs):
        raise asyncio.CancelledError("Simulated task cancellation during streaming")

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.keyframe_prober.Mp4StssProber.probe_from_url", new_callable=AsyncMock, return_value=None),
        patch("app.core.keyframe_prober.MkvCuesProber.probe_from_url", new_callable=AsyncMock, return_value=None),
        patch("app.core.pikpak.thumbnail.fetch_thumbnail", new_callable=AsyncMock, return_value=None),
        patch("app.core.pikpak.thumbnail.extract_frame_ffmpeg", new_callable=AsyncMock, return_value=None),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch.object(LocalPagedRangeProxy, "__init__", side_effect=spy_proxy_init, autospec=True),
        patch(
            "app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram",
            side_effect=cancel_stream_to_telegram,
        ),
    ):
        with pytest.raises(asyncio.CancelledError):
            await engine._publish_fmp4_split_item(
                mock_client,
                item,
                2000 * 1024 * 1024,
                ctx,
                probed_metadata=meta,
                pinned_regions=pinned_regions,
            )

        assert len(created_proxies) == 1
        proxy = created_proxies[0]
        assert proxy._closed is True
        assert proxy._runner is None


# -----------------------------------------------------------------------------
# Test 8: _probe_fmp4_media handles missing stss without OOM
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_probe_fmp4_media_handles_missing_stss_without_oom():
    """验证当 MP4 缺少 stss Box 时，_probe_fmp4_media 不会生成数千万整型列表导致 OOM，而是返回 None 盲切。"""
    from app.core.iso_bmff.box_parser import UniformStszTable

    meta = _build_synthetic_metadata(duration_s=120.0)
    # 将视频轨设为无 stss 且含有 50,000,000 samples 的 UniformStszTable
    v_track = meta.video_track
    assert v_track is not None
    v_track.stbl.stss = None
    v_track.stbl.stsz = UniformStszTable(uniform_size=100, sample_count=50_000_000)

    item = PublishItem(
        item_id="fid_no_stss",
        name="no_stss.mp4",
        size_bytes=3000 * 1024 * 1024,
        source_type="pikpak",
    )
    ctx = _make_context()

    ext, copy_audio, duration, keyframe_pts = await probe_fmp4_media(
        "https://cdn.example.com/no_stss.mp4",
        item,
        ctx,
        probed_metadata=meta,
    )

    assert ext == ".mp4"
    assert duration == 120.0
    # 无 stss 时应为 None，由下游 plan_video_fmp4_parts 均匀盲切
    assert keyframe_pts is None


# -----------------------------------------------------------------------------
# Test 9: Circuit breaker corrupt moov passthrough pinned head only
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_circuit_breaker_corrupt_moov_passthrough_pinned_head_only():
    """验证当 MP4 moov 损坏解析失败时，熔断降级仅透传 head_bytes L0 缓存且 probed_metadata 为 None。"""
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/corrupt_moov.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    file_size = 3000 * 1024 * 1024
    # 破坏性 moov 盒子
    header_bytes = b"\x00\x00\x00\x20ftypisom\x00\x00\x02\x00isomiso2mp41\x00\x00\x01\x00moovCORRUPTED_MOOV_PAYLOAD"
    mock_provider = MemorySyntheticChunkProvider(
        file_id="fid_corrupt_moov",
        file_name="corrupt_moov.mp4",
        total_bytes=file_size,
        header_bytes=header_bytes,
    )

    item = PublishItem(
        item_id="fid_corrupt_moov",
        name="corrupt_moov.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    ctx = _make_context(userbot_client=mock_client, split_strategy="vsse_with_fallback")

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
    ):
        mock_fmp4.return_value = TransportOutcome(message_ids=[881], speed_mb_s=10.0, client_label="Userbot")

        res = await engine.send_item(item, ctx)

        assert res.message_ids == [881]
        mock_fmp4.assert_awaited_once()
        _, call_kwargs = mock_fmp4.call_args

        assert call_kwargs.get("probed_metadata") is None
        pinned_reg = call_kwargs.get("pinned_regions")
        assert pinned_reg is not None
        assert len(pinned_reg) == 1
        assert pinned_reg[0][0] == 0
        assert pinned_reg[0][1] == header_bytes


# -----------------------------------------------------------------------------
# Test 10: LocalPagedRangeProxy stop and aclose method aliases
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_local_paged_range_proxy_stop_and_aclose_aliases():
    """验证 LocalPagedRangeProxy 的 stop() 与 aclose() 别名均能正确停止服务并释放端口。"""
    from app.core.stream_readahead_proxy import LocalPagedRangeProxy

    mock_provider = MemorySyntheticChunkProvider(
        file_id="fid_alias_test",
        file_name="test.mp4",
        total_bytes=1024 * 1024,
        header_bytes=b"FAKE_DATA" * 100,
    )

    # 验证 stop()
    proxy1 = LocalPagedRangeProxy(mock_provider)
    await proxy1.start()
    assert proxy1._runner is not None
    assert proxy1._closed is False
    await proxy1.stop()
    assert proxy1._closed is True
    assert proxy1._runner is None

    # 验证 aclose()
    proxy2 = LocalPagedRangeProxy(mock_provider)
    await proxy2.start()
    assert proxy2._runner is not None
    assert proxy2._closed is False
    await proxy2.aclose()
    assert proxy2._closed is True
    assert proxy2._runner is None


# -----------------------------------------------------------------------------
# Test 11: _probe_vsse_envelope backfills tkhd dimensions and audio codec
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_probe_vsse_envelope_backfills_tkhd_dimensions_and_audio_codec():
    """验证当 stsd 中缺少宽高时，_probe_vsse_envelope 从 tkhd 回填宽高并回填 audio_codec。"""

    file_size = 3000 * 1024 * 1024
    header_bytes = _build_test_mp4_header(
        total_bytes=file_size,
        video_codec="avc1",
        audio_codec="mp4a",
    )
    provider = MemorySyntheticChunkProvider(
        file_id="fid_tkhd_test",
        file_name="test.mp4",
        total_bytes=file_size,
        header_bytes=header_bytes,
    )

    item = PublishItem(
        item_id="fid_tkhd_test",
        name="test.mp4",
        size_bytes=file_size,
        duration_s=None,
        width=None,
        height=None,
        audio_codec=None,
        video_codec=None,
        source_type="pikpak",
    )

    speed_provider = MagicMock()
    speed_provider.read_chunk = provider.read_chunk
    speed_provider.file_id = provider.file_id
    speed_provider.total_bytes = provider.total_bytes

    meta, plans, headers = await probe_vsse_envelope(item, 2000 * 1024 * 1024, speed_provider)

    assert item.duration_s is not None and item.duration_s > 0
    assert item.width is not None and item.width > 0
    assert item.height is not None and item.height > 0
    assert item.audio_codec == "mp4a"
    assert item.video_codec == "avc1"


# -----------------------------------------------------------------------------
# Test 12: PikPakStreamChunkProvider.update_url dynamic URL update
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_pikpak_stream_chunk_provider_update_url():
    """验证 PikPakStreamChunkProvider.update_url 能正确更新直链并递增代际编号。"""
    from app.core.stream_chunk_provider import PikPakStreamChunkProvider

    provider = PikPakStreamChunkProvider(
        file_id="fid_update_test",
        file_name="test.mp4",
        total_bytes=1024 * 1024,
        url_provider=AsyncMock(return_value="https://cdn.example.com/new.mp4"),
        initial_url="https://cdn.example.com/initial.mp4",
    )

    assert provider._current_url == "https://cdn.example.com/initial.mp4"
    assert provider._url_generation == 1

    provider.update_url("https://cdn.example.com/refreshed.mp4")
    assert provider._current_url == "https://cdn.example.com/refreshed.mp4"
    assert provider._url_generation == 2
    await provider.aclose()


# -----------------------------------------------------------------------------
# Test 13: _publish_fmp4_split_item supports empty pinned_regions=[] or use_paged_proxy=True
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_publish_fmp4_split_item_empty_pinned_regions_or_use_paged_proxy():
    """验证传入 pinned_regions=[] 或 use_paged_proxy=True 时（如 MKV 容器无 MP4 预热块），依然正常挂载 LocalPagedRangeProxy。"""
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/movie.mkv"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    item = PublishItem(
        item_id="fid_mkv_proxy",
        name="movie.mkv",
        size_bytes=3000 * 1024 * 1024,
        duration_s=60.0,
        audio_codec="ac3",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=995)
    ctx = _make_context(userbot_client=mock_client)

    plans = [
        Fmp4PartPlan(
            part_index=1,
            total_parts=1,
            start_sec=0.0,
            end_sec=60.0,
            duration_sec=60.0,
            part_name="movie.part001.mp4",
        )
    ]

    called_urls = []

    async def fake_stream_to_telegram(**kwargs):
        called_urls.append(kwargs.get("pikpak_url"))
        return MagicMock(), 60.0

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch(
            "app.core.pipeline.engines.pikpak.engine.probe_fmp4_media",
            new_callable=AsyncMock,
            return_value=(".mkv", False, 60.0, [0.0, 30.0]),
        ),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch(
            "app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram",
            side_effect=fake_stream_to_telegram,
        ),
    ):
        outcome = await engine._publish_fmp4_split_item(
            mock_client,
            item,
            2000 * 1024 * 1024,
            ctx,
            pinned_regions=[],
            use_paged_proxy=True,
        )

        assert outcome.message_ids == [995]
        assert len(called_urls) == 1
        assert isinstance(called_urls[0], tuple)
        assert called_urls[0][0].startswith("http://127.0.0.1:")
        assert called_urls[0][0].endswith("/video")
        assert called_urls[0][1].endswith("/audio")


# -----------------------------------------------------------------------------
# Test 14: Tier 2 sidecar failure triggers circuit breaker with Stage 0 passthrough
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_vsse_tier2_sidecar_failure_triggers_circuit_breaker_with_passthrough():
    """验证 Tier 2 旁路音频提取失败时熔断降级 fMP4, 且 Stage 0 已探测的 Mp4Metadata 与 pinned_regions 完整透传。"""
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/sidecar_boom.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    file_size = 3000 * 1024 * 1024
    # lpcm 音频 → classify_vsse_mode 直接路由 HYBRID_AUDIO_SIDECAR
    header_bytes = _build_test_mp4_header(
        total_bytes=file_size,
        audio_codec="lpcm",
    )
    provider = MemorySyntheticChunkProvider(
        file_id="fid_sidecar_boom",
        file_name="sidecar_boom.mp4",
        total_bytes=file_size,
        header_bytes=header_bytes,
    )

    item = PublishItem(
        item_id="fid_sidecar_boom",
        name="sidecar_boom.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="lpcm",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    ctx = _make_context(userbot_client=mock_client, split_strategy="vsse_with_fallback")

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=provider),
        patch(
            "app.core.vsse_audio_sidecar.build_sidecar_audio_bundle",
            new_callable=AsyncMock,
            side_effect=RuntimeError("Simulated FFmpeg sidecar transcode failure"),
        ),
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
    ):
        mock_fmp4.return_value = TransportOutcome(message_ids=[888], speed_mb_s=10.0, client_label="Userbot")

        res = await engine.send_item(item, ctx)

        assert res.message_ids == [888]
        mock_fmp4.assert_awaited_once()
        _, call_kwargs = mock_fmp4.call_args

        # 核心断言: Tier 2 失败时 Stage 0 产物 (真实 parse_moov 结果) 依然透传到 Tier 3
        probed_meta = call_kwargs.get("probed_metadata")
        pinned_reg = call_kwargs.get("pinned_regions")
        assert isinstance(probed_meta, Mp4Metadata)
        assert pinned_reg is not None
        assert len(pinned_reg) >= 1
        assert pinned_reg[0][0] == 0


# -----------------------------------------------------------------------------
# Test 15: proxy-mode URL hot-refresh on oversize replan
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_publish_fmp4_split_item_replan_updates_proxy_provider_url():
    """验证代理模式下超限重规划触发 inner_provider.update_url 直链热更 (JIT 换链收口单一入口)。"""
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/replanned.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    meta = _build_synthetic_metadata(duration_s=60.0)
    pinned_regions = [(0, b"HEADER_128K")]

    item = PublishItem(
        item_id="fid_replan_refresh",
        name="replanned.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=60.0,
        audio_codec="aac",
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=996)
    ctx = _make_context(userbot_client=mock_client)

    plans = [
        Fmp4PartPlan(
            part_index=1,
            total_parts=1,
            start_sec=0.0,
            end_sec=60.0,
            duration_sec=60.0,
            part_name="replanned.mp4",
        )
    ]

    from app.core.fmp4_pipe_uploader import PartOversizeError
    from app.core.stream_chunk_provider import PikPakStreamChunkProvider
    from app.core.stream_readahead_proxy import LocalPagedRangeProxy

    orig_proxy_init = LocalPagedRangeProxy.__init__
    created_proxies = []

    def spy_proxy_init(self, *args, **kwargs):
        orig_proxy_init(self, *args, **kwargs)
        created_proxies.append(self)

    update_calls: list[str] = []
    orig_update_url = PikPakStreamChunkProvider.update_url

    def spy_update_url(self, new_url: str) -> None:
        update_calls.append(new_url)
        orig_update_url(self, new_url)

    stream_call_count = 0

    async def fake_stream_oversize_then_ok(**kwargs):
        nonlocal stream_call_count
        stream_call_count += 1
        if stream_call_count == 1:
            raise PartOversizeError(
                1, 2100 * 1024 * 1024, 2000 * 1024 * 1024, "Simulated part oversize on first attempt"
            )
        return MagicMock(), 60.0

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch(
            "app.core.pipeline.engines.pikpak.engine.probe_fmp4_media",
            new_callable=AsyncMock,
            return_value=(".mp4", True, 60.0, [0.0]),
        ),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch.object(LocalPagedRangeProxy, "__init__", side_effect=spy_proxy_init, autospec=True),
        patch.object(PikPakStreamChunkProvider, "update_url", side_effect=spy_update_url, autospec=True),
        patch(
            "app.core.pipeline.engines.pikpak.fmp4.replan_fmp4_after_oversize",
            new_callable=AsyncMock,
            return_value=(2, "https://cdn.example.com/replanned.mp4"),
        ) as mock_replan,
        patch(
            "app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram",
            side_effect=fake_stream_oversize_then_ok,
        ),
    ):
        outcome = await engine._publish_fmp4_split_item(
            mock_client,
            item,
            2000 * 1024 * 1024,
            ctx,
            probed_metadata=meta,
            pinned_regions=pinned_regions,
        )

        assert outcome.message_ids == [996]
        assert stream_call_count == 2  # 第一次超限, 第二次重规划后成功
        mock_replan.assert_awaited_once()
        # 核心断言: 重规划返回的新直链直接热更到 inner_provider，不触发二次重复 get_fresh_url 调用
        assert update_calls == ["https://cdn.example.com/replanned.mp4"]
        assert mock_link_fn.await_count == 1
        # 代理生命周期正常回收
        assert len(created_proxies) == 1
        assert created_proxies[0]._closed is True


@pytest.mark.asyncio
async def test_vsse_tier2_throttled_error_propagates_without_fmp4_fallback():
    """#12: Tier 2 旁路音频遭遇 PikPakThrottledError 时必须透传为 PikPakPublishThrottledError, 不得吞入 IsoBmffError 熔断降级。"""
    from app.core.pikpak.taxonomy import PikPakPublishThrottledError
    from app.core.stream_chunk_provider import PikPakThrottledError

    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/throttled.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    file_size = 3000 * 1024 * 1024
    header_bytes = _build_test_mp4_header(total_bytes=file_size, audio_codec="lpcm")
    provider = MemorySyntheticChunkProvider(
        file_id="fid_throttle",
        file_name="throttled.mp4",
        total_bytes=file_size,
        header_bytes=header_bytes,
    )
    item = PublishItem(
        item_id="fid_throttle",
        name="throttled.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="lpcm",
        source_type="pikpak",
    )
    ctx = _make_context(userbot_client=AsyncMock(), split_strategy="vsse_with_fallback")

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=provider),
        patch(
            "app.core.vsse_audio_sidecar.build_sidecar_audio_bundle",
            new_callable=AsyncMock,
            side_effect=PikPakThrottledError("CDN 429", status=429, retry_after=450.0),
        ),
        patch.object(engine, "_publish_fmp4_split_item", new_callable=AsyncMock) as mock_fmp4,
    ):
        with pytest.raises(PikPakPublishThrottledError) as exc_info:
            await engine.send_item(item, ctx)
        assert exc_info.value.retry_after == 450.0
        mock_fmp4.assert_not_awaited()
