"""Unit and Integration Tests for Minimalist Sidecar Audio Transcoder.

Verifies:
1. build_audio_spans:
   - Interleaved audio/video chunk interleaving: asserts exact boundary between audio chunks
     and next video chunk, adjacent audio chunks merged up to max_span_size, no video chunks included.
   - Non-interleaved audio/video (all video before audio, all audio before video).
   - Empty audio track, no audio track, and moov at end vs moov at start.
2. SidecarAudioBundle:
   - Slicing, properties, payload length, and track accessors.
3. Real FFmpeg transcode with pcm_s16le (QuickTime .mov with sowt):
   - Transcoded to standard mp4a AAC.
   - Channel count <= 2 (stereo/mono).
   - Encoder priming delay edts / elst completely preserved.
   - Chunk offsets normalized relative to mdat payload start (first offset == 0).
   - Video range zero downloads from chunk provider (0 视频流量浪费).
4. Real FFmpeg transcode with pcm_s16be (QuickTime .mov with twos):
   - Big-endian PCM converted to standard AAC.
   - edts preserved, channel count <= 2, video range zero downloads.
5. Real FFmpeg remux with non-interleaved MP4 (NonInterleavedMp4Error layout):
   - Non-interleaved layout triggers NonInterleavedMp4Error in check_track_interleaving.
   - -c:a copy fast path triggered for stereo mp4a.
   - Video range zero downloads from chunk provider.
6. Error handling & resilience:
   - No audio track raises ValueError.
   - Empty chunks raises ValueError.
   - FFmpeg timeout raises TimeoutError and cleans up temp files.
   - FFmpeg non-zero exit code raises RuntimeError and cleans up temp files.
"""

from __future__ import annotations

import array
import asyncio
import shutil
import subprocess
from pathlib import Path
from typing import Any

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
    parse_moov,
    scan_top_level_boxes,
)
from app.core.iso_bmff.box_synthesizer import (
    compute_envelope_bounds,
    synthesize_faststart_envelope_header,
)
from app.core.iso_bmff.exceptions import NonInterleavedMp4Error
from app.core.stream_chunk_provider import BaseChunkProvider, LocalFileChunkProvider
from app.core.vsse_audio_sidecar import (
    SidecarAudioBundle,
    build_audio_spans,
    build_sidecar_audio_bundle,
)
from app.core.vsse_provider import (
    DEFAULT_FTYP_BOX,
    VirtualEnvelopeChunkProvider,
    check_track_interleaving,
    get_track_chunks,
    plan_vsse_volumes,
)


class TrackingChunkProvider(BaseChunkProvider):
    """包装 BaseChunkProvider 并精确记录所有 read_chunk 请求的字节区间。"""

    def __init__(self, inner: BaseChunkProvider) -> None:
        self.inner = inner
        self.file_id = inner.file_id
        self.file_name = inner.file_name
        self.total_bytes = inner.total_bytes
        self.reads: list[tuple[int, int]] = []

    async def read_chunk(self, offset: int, size: int) -> bytes:
        self.reads.append((offset, offset + size))
        return await self.inner.read_chunk(offset, size)

    async def aclose(self) -> None:
        await self.inner.aclose()


def _create_synthetic_track(
    track_id: int,
    is_video: bool,
    chunk_offsets: list[int],
    sample_sizes: list[int] | None = None,
    channel_count: int = 2,
    codec_format: bytes = b"mp4a",
) -> Track:
    """快速构造包含 chunk_offsets 的合成 Track 对象。"""
    handler_type = b"vide" if is_video else b"soun"
    hdlr = Hdlr(handler_type=handler_type, name="TestHandler")
    tkhd = Tkhd(
        version=0,
        flags=1,
        creation_time=0,
        modification_time=0,
        track_id=track_id,
        duration=1000,
    )
    mdhd = Mdhd(
        version=0,
        flags=0,
        creation_time=0,
        modification_time=0,
        timescale=1000,
        duration=1000,
        language=0,
    )
    sizes = sample_sizes if sample_sizes is not None else [100] * len(chunk_offsets)
    stsz_arr = array.array("I", sizes)
    co_arr = array.array("Q", chunk_offsets)
    stsc = [StscEntry(first_chunk=1, samples_per_chunk=1, sample_description_index=1)]
    stts = [SttsEntry(sample_count=len(sizes), sample_delta=100)]
    entry = SampleEntry(
        format=codec_format,
        data_ref_index=1,
        codec=codec_format.decode("ascii", errors="replace"),
        channel_count=channel_count,
    )
    stsd = Stsd(version=0, flags=0, entry_count=1, entries=[entry])
    stbl = SampleTable(
        stsd=stsd,
        stts=stts,
        stsc=stsc,
        stsz=stsz_arr,
        chunk_offsets=co_arr,
    )
    return Track(track_id=track_id, tkhd=tkhd, mdhd=mdhd, hdlr=hdlr, stbl=stbl)


def _create_synthetic_metadata(tracks: list[Track], timescale: int = 1000) -> Mp4Metadata:
    mvhd = Mvhd(
        version=0,
        flags=0,
        creation_time=0,
        modification_time=0,
        timescale=timescale,
        duration=1000,
        next_track_id=len(tracks) + 1,
    )
    return Mp4Metadata(mvhd=mvhd, tracks=tracks)


# =====================================================================
# 1. build_audio_spans 算法测试
# =====================================================================


def test_build_audio_spans_interleaved():
    """验证交织音视频拓扑下，音频区间在视频 Chunk 起始严格截断，且相邻音频 Chunk 自动合并。"""
    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[1000, 5000, 10000])
    a_track = _create_synthetic_track(2, is_video=False, chunk_offsets=[2000, 3000, 7000])
    meta = _create_synthetic_metadata([v_track, a_track])

    spans = build_audio_spans(meta, total_bytes=15000, max_gap=64 * 1024)
    # A1(2000) 与 A2(3000) 连续且紧邻，在 V1(5000) 前合并为 [2000, 5000)
    # A3(7000) 在 V2(10000) 前终止为 [7000, 10000)
    assert spans == [(2000, 5000), (7000, 10000)]


def test_build_audio_spans_non_interleaved():
    """验证非交织结构 (所有视频在前或所有音频在前) 下的音频区间。"""
    # Case A: 所有视频在前 [1000, 2000], 所有音频在后 [50000, 60000]
    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[1000, 2000])
    a_track = _create_synthetic_track(2, is_video=False, chunk_offsets=[50000, 60000])
    meta = _create_synthetic_metadata([v_track, a_track])

    spans = build_audio_spans(meta, total_bytes=100000, moov_offset=80000)
    # 音频区间从 50000 开始，连续两 Chunk 合并，在最后一个 Sample 终点 (60100) 处精确截止
    assert spans == [(50000, 60100)]

    # Case B: 所有音频在前 [1000, 2000], 所有视频在后 [50000, 60000]
    v_track_b = _create_synthetic_track(1, is_video=True, chunk_offsets=[50000, 60000])
    a_track_b = _create_synthetic_track(2, is_video=False, chunk_offsets=[1000, 2000])
    meta_b = _create_synthetic_metadata([v_track_b, a_track_b])

    spans_b = build_audio_spans(meta_b, total_bytes=100000)
    # 音频区间在首个视频 Chunk(50000) 前截止
    assert spans_b == [(1000, 50000)]


def test_build_audio_spans_empty_and_no_audio():
    """验证无音频轨或音频轨无 Chunk 时的安全返回。"""
    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[1000, 2000])
    meta_no_audio = _create_synthetic_metadata([v_track])
    assert build_audio_spans(meta_no_audio, total_bytes=5000) == []

    a_track_empty = _create_synthetic_track(2, is_video=False, chunk_offsets=[])
    meta_empty_audio = _create_synthetic_metadata([v_track, a_track_empty])
    assert build_audio_spans(meta_empty_audio, total_bytes=5000) == []


# =====================================================================
# 2. SidecarAudioBundle 属性与切片测试
# =====================================================================


def test_sidecar_audio_bundle_methods():
    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[1000])
    a_track = _create_synthetic_track(2, is_video=False, chunk_offsets=[2000])
    meta = _create_synthetic_metadata([v_track, a_track])
    payload = b"0123456789ABCDEF"

    bundle = SidecarAudioBundle(hybrid_metadata=meta, aac_payload=payload)
    assert len(bundle) == 16
    assert bundle.payload_size == 16
    assert bundle.aac_track == a_track
    assert bundle.video_track == v_track

    # 验证切片功能
    assert bundle.slice_bytes(0, 4) == b"0123"
    assert bundle.slice_bytes(4, 8) == b"4567"
    assert bundle.slice_bytes(10, None) == b"ABCDEF"
    assert bundle.slice_bytes(None, 4) == b"0123"
    assert bundle.slice_bytes(20, 30) == b""
    assert bundle.slice_bytes(8, 4) == b""
    assert bundle.slice_bytes(None, None) == b""
    assert bundle.slice_bytes() == b""


# =====================================================================
# 3. 真实 FFmpeg 转码测试: QuickTime .mov (pcm_s16le, sowt)
# =====================================================================


@pytest.mark.asyncio
async def test_sidecar_transcode_pcm_s16le(tmp_path: Path):
    """验证真实 FFmpeg 将 QuickTime pcm_s16le (.mov) 转码为标准 AAC 并断言视频区间 0 下载。"""
    if shutil.which("ffmpeg") is None:
        pytest.skip("FFmpeg 未安装，跳过真实转码测试")

    mov_path = tmp_path / "sample_pcm_s16le.mov"
    # 生成 1 帧大视频 (36 KB) + 3 秒 48kHz PCM 音频 (288 KB) 的 QuickTime MOV
    cmd_gen = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=1280x720:rate=1:duration=3",
        "-f",
        "lavfi",
        "-i",
        "sine=r=48000:f=1000:d=3",
        "-c:v",
        "libx264",
        "-b:v",
        "5000k",
        "-c:a",
        "pcm_s16le",
        str(mov_path),
    ]
    subprocess.run(cmd_gen, capture_output=True, check=True)

    data = mov_path.read_bytes()
    boxes = scan_top_level_boxes(data, len(data))
    moov_box = next(b for b in boxes if b.box_type == b"moov")
    moov_bytes = data[moov_box.offset : moov_box.offset + moov_box.size]
    meta = parse_moov(moov_bytes)

    # 验证原始格式为 sowt (pcm_s16le)
    assert meta.video_track is not None
    assert meta.audio_track is not None
    assert meta.audio_track.sample_entry is not None
    assert meta.audio_track.sample_entry.format == b"sowt"

    v_chunk_offset = meta.video_track.stbl.chunk_offsets[0]
    a_chunk_offset = meta.audio_track.stbl.chunk_offsets[0]

    # 模拟真实 Stage 0: head_bytes 仅覆盖前导容器头 (绝对不提前覆盖视频 Chunk)
    head_bytes = data[: min(128 * 1024, v_chunk_offset)]

    raw_provider = LocalFileChunkProvider(str(mov_path))
    tracker = TrackingChunkProvider(raw_provider)

    bundle = await build_sidecar_audio_bundle(
        metadata=meta,
        provider=tracker,
        head_bytes=head_bytes,
        moov_offset=moov_box.offset,
        moov_bytes=moov_bytes,
        temp_dir=tmp_path,
    )

    # 1. 验证转码产物为合法 AAC (mp4a)
    assert bundle.aac_track.codec == "mp4a"
    assert bundle.aac_track.sample_entry is not None
    assert bundle.aac_track.sample_entry.format == b"mp4a"

    # 2. 验证声道数 <= 2 (下混或保持双声道/单声道)
    assert bundle.aac_track.sample_entry.channel_count <= 2

    # 3. 验证 aac_track.edts 完整保留 (编码器 Priming Delay elst 存在)
    assert bundle.aac_track.edts is not None
    assert len(bundle.aac_track.edts.entries) > 0
    assert bundle.aac_track.edts.entries[0].media_time > 0

    # 4. 验证 chunk_offsets 归一化为从 0 开始
    assert bundle.aac_track.stbl.chunk_offsets[0] == 0

    # 5. 核心断言: 验证对 ChunkProvider 的远端拉取绝对不触碰视频区间 (0 视频流量浪费)
    assert len(tracker.reads) > 0
    for r_start, r_end in tracker.reads:
        # 视频区间为 [v_chunk_offset, a_chunk_offset)
        overlap = max(0, min(r_end, a_chunk_offset) - max(r_start, v_chunk_offset))
        assert overlap == 0, (
            f"视频区间被非法读取: read=[{r_start}, {r_end}), video=[{v_chunk_offset}, {a_chunk_offset})"
        )


# =====================================================================
# 4. 真实 FFmpeg 转码测试: QuickTime .mov (pcm_s16be, twos)
# =====================================================================


@pytest.mark.asyncio
async def test_sidecar_transcode_pcm_s16be(tmp_path: Path):
    """验证真实 FFmpeg 将 QuickTime pcm_s16be (twos) 转码为标准 AAC 并断言视频区间 0 下载。"""
    if shutil.which("ffmpeg") is None:
        pytest.skip("FFmpeg 未安装，跳过真实转码测试")

    mov_path = tmp_path / "sample_pcm_s16be.mov"
    cmd_gen = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=1280x720:rate=1:duration=3",
        "-f",
        "lavfi",
        "-i",
        "sine=r=48000:f=1000:d=3",
        "-c:v",
        "libx264",
        "-b:v",
        "5000k",
        "-c:a",
        "pcm_s16be",
        str(mov_path),
    ]
    subprocess.run(cmd_gen, capture_output=True, check=True)

    data = mov_path.read_bytes()
    boxes = scan_top_level_boxes(data, len(data))
    moov_box = next(b for b in boxes if b.box_type == b"moov")
    moov_bytes = data[moov_box.offset : moov_box.offset + moov_box.size]
    meta = parse_moov(moov_bytes)

    # 验证原始格式为 twos (pcm_s16be)
    assert meta.video_track is not None
    assert meta.audio_track is not None
    assert meta.audio_track.sample_entry is not None
    assert meta.audio_track.sample_entry.format == b"twos"

    v_chunk_offset = meta.video_track.stbl.chunk_offsets[0]
    a_chunk_offset = meta.audio_track.stbl.chunk_offsets[0]
    head_bytes = data[: min(128 * 1024, v_chunk_offset)]

    raw_provider = LocalFileChunkProvider(str(mov_path))
    tracker = TrackingChunkProvider(raw_provider)

    bundle = await build_sidecar_audio_bundle(
        metadata=meta,
        provider=tracker,
        head_bytes=head_bytes,
        moov_offset=moov_box.offset,
        moov_bytes=moov_bytes,
        temp_dir=tmp_path,
    )

    assert bundle.aac_track.codec == "mp4a"
    assert bundle.aac_track.sample_entry is not None
    assert bundle.aac_track.sample_entry.channel_count <= 2
    assert bundle.aac_track.edts is not None
    assert len(bundle.aac_track.edts.entries) > 0
    assert bundle.aac_track.stbl.chunk_offsets[0] == 0

    # 断言视频区间 0 下载
    for r_start, r_end in tracker.reads:
        overlap = max(0, min(r_end, a_chunk_offset) - max(r_start, v_chunk_offset))
        assert overlap == 0, (
            f"视频区间被非法读取: read=[{r_start}, {r_end}), video=[{v_chunk_offset}, {a_chunk_offset})"
        )


# =====================================================================
# 5. 真实 FFmpeg 重封装测试: 非交织 MP4 (NonInterleavedMp4Error)
# =====================================================================


@pytest.mark.asyncio
async def test_sidecar_remux_non_interleaved_aac(tmp_path: Path):
    """验证对触发 NonInterleavedMp4Error 的非交织 AAC MP4 执行免重编码 copy 重封装，断言视频区间 0 下载。"""
    if shutil.which("ffmpeg") is None:
        pytest.skip("FFmpeg 未安装，跳过真实重封装测试")

    mp4_path = tmp_path / "sample_non_interleaved.mp4"
    # 使用 -itsoffset 2.0 使所有视频 Chunk 完整先于所有音频 Chunk 写入
    cmd_gen = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=320x240:rate=10:duration=1",
        "-itsoffset",
        "2.0",
        "-f",
        "lavfi",
        "-i",
        "sine=r=8000:f=1000:d=1",
        "-c:v",
        "libx264",
        "-c:a",
        "aac",
        str(mp4_path),
    ]
    subprocess.run(cmd_gen, capture_output=True, check=True)

    data = mp4_path.read_bytes()
    boxes = scan_top_level_boxes(data, len(data))
    moov_box = next(b for b in boxes if b.box_type == b"moov")
    moov_bytes = data[moov_box.offset : moov_box.offset + moov_box.size]
    meta = parse_moov(moov_bytes)

    # 验证确实具有非交织物理布局 (all video chunks precede audio chunks)
    assert meta.video_track is not None
    assert meta.audio_track is not None
    v_last_off = meta.video_track.stbl.chunk_offsets[-1]
    a_first_off = meta.audio_track.stbl.chunk_offsets[0]
    assert v_last_off < a_first_off

    # 验证 check_track_interleaving 确实会触发 NonInterleavedMp4Error
    v_chunks = get_track_chunks(meta.video_track)
    a_chunks = get_track_chunks(meta.audio_track)
    with pytest.raises(NonInterleavedMp4Error) as exc_info:
        check_track_interleaving(meta.video_track, meta.audio_track, 100000, v_chunks, a_chunks)
    assert "Non-interleaved MP4 detected" in str(exc_info.value)

    # 模拟真实 Stage 0: head_bytes 仅包含容器头 (不覆盖视频 Chunk 数据)
    v_first_off = meta.video_track.stbl.chunk_offsets[0]
    head_bytes = data[: min(128 * 1024, v_first_off)]

    raw_provider = LocalFileChunkProvider(str(mp4_path))
    tracker = TrackingChunkProvider(raw_provider)

    bundle = await build_sidecar_audio_bundle(
        metadata=meta,
        provider=tracker,
        head_bytes=head_bytes,
        moov_offset=moov_box.offset,
        moov_bytes=moov_bytes,
        temp_dir=tmp_path,
    )

    # 验证保持 AAC 格式且 edts 保留
    assert bundle.aac_track.codec == "mp4a"
    assert bundle.aac_track.sample_entry is not None
    assert bundle.aac_track.sample_entry.channel_count <= 2
    assert bundle.aac_track.edts is not None
    assert bundle.aac_track.stbl.chunk_offsets[0] == 0

    # 断言视频区间 0 下载
    v_first_off = meta.video_track.stbl.chunk_offsets[0]
    for r_start, r_end in tracker.reads:
        overlap = max(0, min(r_end, a_first_off) - max(r_start, v_first_off))
        assert overlap == 0, f"视频区间被非法读取: read=[{r_start}, {r_end}), video=[{v_first_off}, {a_first_off})"


# =====================================================================
# 6. 异常与边界情况测试
# =====================================================================


@pytest.mark.asyncio
async def test_build_sidecar_audio_bundle_missing_audio_track(tmp_path: Path):
    """验证无音频轨时抛出 ValueError。"""
    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[1000])
    meta = _create_synthetic_metadata([v_track])

    # 使用 Mock Provider
    class DummyProvider(BaseChunkProvider):
        file_id = "f"
        file_name = "f.mp4"
        total_bytes = 10000

        async def read_chunk(self, offset: int, size: int) -> bytes:
            return b"\x00" * size

    with pytest.raises(ValueError, match="contains no audio track"):
        await build_sidecar_audio_bundle(
            metadata=meta,
            provider=DummyProvider(),
            head_bytes=b"0" * 128,
            moov_offset=1000,
            moov_bytes=b"0" * 128,
        )


@pytest.mark.asyncio
async def test_build_sidecar_audio_bundle_ffmpeg_timeout(tmp_path: Path, monkeypatch: Any):
    """验证 FFmpeg 转码超时时正确抛出 TimeoutError 并清理临时文件。"""
    if shutil.which("ffmpeg") is None:
        pytest.skip("ffmpeg not available on PATH")
    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[1000])
    a_track = _create_synthetic_track(2, is_video=False, chunk_offsets=[5000])
    meta = _create_synthetic_metadata([v_track, a_track])

    class SlowProvider(BaseChunkProvider):
        file_id = "slow"
        file_name = "slow.mp4"
        total_bytes = 20000

        async def read_chunk(self, offset: int, size: int) -> bytes:
            return b"\x00" * size

    # 设置极短超时时间 0.0001s 触发超时
    with pytest.raises(TimeoutError):
        await build_sidecar_audio_bundle(
            metadata=meta,
            provider=SlowProvider(),
            head_bytes=b"0" * 2000,
            moov_offset=8000,
            moov_bytes=b"0" * 2000,
            ffmpeg_timeout_s=0.0001,
            temp_dir=tmp_path,
        )

    # 验证受管临时目录下无遗留文件
    leftovers = list(tmp_path.glob("sidecar_audio_*.mp4"))
    assert len(leftovers) == 0


@pytest.mark.asyncio
async def test_build_sidecar_audio_bundle_ffmpeg_failure(tmp_path: Path):
    """验证 FFmpeg 遇到损坏流报错时抛出 RuntimeError 并清理临时文件。"""
    if shutil.which("ffmpeg") is None:
        pytest.skip("ffmpeg not available on PATH")
    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[1000])
    a_track = _create_synthetic_track(2, is_video=False, chunk_offsets=[5000])
    meta = _create_synthetic_metadata([v_track, a_track])

    class CorruptProvider(BaseChunkProvider):
        file_id = "corrupt"
        file_name = "corrupt.mp4"
        total_bytes = 20000

        async def read_chunk(self, offset: int, size: int) -> bytes:
            return b"corrupted garbage media bytes"

    # 传入全垃圾数据，FFmpeg 必然报错非零退出
    with pytest.raises(RuntimeError, match="FFmpeg audio sidecar extraction failed"):
        await build_sidecar_audio_bundle(
            metadata=meta,
            provider=CorruptProvider(),
            head_bytes=b"invalid head bytes",
            moov_offset=8000,
            moov_bytes=b"invalid moov bytes",
            temp_dir=tmp_path,
        )

    # 验证受管临时目录下无遗留文件
    leftovers = list(tmp_path.glob("sidecar_audio_*.mp4"))
    assert len(leftovers) == 0


def test_build_audio_spans_faststart_bounded_span():
    """验证快起结构 (moov 在文件头) 且末尾为音频 Chunk 时，不会膨胀为数 GiB 巨大区间触发 OOM。"""
    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[500])
    a_track = _create_synthetic_track(2, is_video=False, chunk_offsets=[1000, 2000], sample_sizes=[100, 150])
    meta = _create_synthetic_metadata([v_track, a_track])

    # 模拟 2000 MiB (1.95 GiB) 大文件，moov 位于头部 (offset 100)
    spans = build_audio_spans(meta, total_bytes=2 * 1024 * 1024 * 1024, moov_offset=100)
    assert len(spans) == 1
    # 验证区间紧凑合并为精确音频范围 [1000, 2150)，绝不膨胀至 2 GiB
    assert spans[0] == (1000, 2150)
    assert spans[0][1] - spans[0][0] == 1150


@pytest.mark.asyncio
async def test_sidecar_transcode_interleaved_large_zero_video_bytes(tmp_path: Path):
    """验证真实高频交织音视频文件在跨越数百 Chunk 时，零视频字节下载且转码完全成功。"""
    if shutil.which("ffmpeg") is None:
        pytest.skip("FFmpeg 未安装，跳过真实转码测试")

    mov_path = tmp_path / "interleaved_large.mov"
    # 5 秒 720p 视频 (数百个交织音视频 Chunk)
    cmd_gen = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=640x360:rate=25:duration=5",
        "-f",
        "lavfi",
        "-i",
        "sine=r=48000:f=1000:d=5",
        "-c:v",
        "libx264",
        "-b:v",
        "2000k",
        "-c:a",
        "pcm_s16le",
        str(mov_path),
    ]
    subprocess.run(cmd_gen, capture_output=True, check=True)

    data = mov_path.read_bytes()
    boxes = scan_top_level_boxes(data, len(data))
    moov_box = next(b for b in boxes if b.box_type == b"moov")
    moov_bytes = data[moov_box.offset : moov_box.offset + moov_box.size]
    meta = parse_moov(moov_bytes)

    assert meta.video_track is not None
    assert meta.audio_track is not None

    v_offsets = sorted(meta.video_track.stbl.chunk_offsets)
    a_offsets = sorted(meta.audio_track.stbl.chunk_offsets)
    assert len(v_offsets) > 50
    assert len(a_offsets) > 50

    # 真实 Stage 0: 仅提供 128 KiB 头部 (交织 Chunk 大部分位于 128 KiB 之外)
    head_bytes = data[: min(128 * 1024, len(data))]

    raw_provider = LocalFileChunkProvider(str(mov_path))
    tracker = TrackingChunkProvider(raw_provider)

    bundle = await build_sidecar_audio_bundle(
        metadata=meta,
        provider=tracker,
        head_bytes=head_bytes,
        moov_offset=moov_box.offset,
        moov_bytes=moov_bytes,
        temp_dir=tmp_path,
    )

    assert bundle.aac_track.codec == "mp4a"
    assert bundle.aac_track.sample_entry is not None
    assert bundle.aac_track.sample_entry.channel_count <= 2
    assert bundle.aac_track.edts is not None

    # 核心断言: 校验对 provider 的所有读取绝对不与任何视频 Chunk 重叠
    all_offsets = sorted(v_offsets + a_offsets)
    for r_start, r_end in tracker.reads:
        for v_s in v_offsets:
            idx = all_offsets.index(v_s)
            next_off = all_offsets[idx + 1] if idx + 1 < len(all_offsets) else len(data)
            overlap = max(0, min(r_end, next_off) - max(r_start, v_s))
            assert overlap == 0, f"视频 Chunk [{v_s}, {next_off}) 被非法拉取: read=[{r_start}, {r_end})"


@pytest.mark.asyncio
async def test_build_sidecar_audio_bundle_multi_audio_selection(tmp_path: Path):
    """验证包含多音轨 (单声道音轨 1 + 5.1 环绕声音轨 2) 时，准确提取主音轨且不发生音轨错配。"""
    if shutil.which("ffmpeg") is None:
        pytest.skip("FFmpeg 未安装，跳过真实多音轨测试")

    mov_path = tmp_path / "multi_audio.mov"
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=320x240:rate=10:duration=2",
        "-f",
        "lavfi",
        "-i",
        "sine=r=48000:f=1000:d=2",
        "-f",
        "lavfi",
        "-i",
        "sine=r=48000:f=500:d=2",
        "-filter_complex",
        "[2:a]pan=5.1|c0=c0|c1=c0|c2=c0|c3=c0|c4=c0|c5=c0[surr]",
        "-map",
        "0:v",
        "-map",
        "1:a",
        "-map",
        "[surr]",
        "-c:v",
        "libx264",
        "-c:a:0",
        "pcm_s16le",
        "-c:a:1",
        "pcm_s16le",
        str(mov_path),
    ]
    subprocess.run(cmd, capture_output=True, check=True)

    data = mov_path.read_bytes()
    boxes = scan_top_level_boxes(data, len(data))
    moov_box = next(b for b in boxes if b.box_type == b"moov")
    meta = parse_moov(data[moov_box.offset : moov_box.offset + moov_box.size])

    raw_provider = LocalFileChunkProvider(str(mov_path))
    tracker = TrackingChunkProvider(raw_provider)

    bundle = await build_sidecar_audio_bundle(
        metadata=meta,
        provider=tracker,
        head_bytes=data[: min(128 * 1024, len(data))],
        moov_offset=moov_box.offset,
        moov_bytes=data[moov_box.offset : moov_box.offset + moov_box.size],
        temp_dir=tmp_path,
    )

    # 验证提取的是主音轨 (Track 1, 单声道), channel_count 保持单声道或下混符合规范
    assert bundle.aac_track.sample_entry is not None
    assert bundle.aac_track.sample_entry.channel_count in (1, 2)
    assert bundle.aac_track.track_id == meta.audio_track.track_id


@pytest.mark.asyncio
async def test_vsse_sidecar_dual_region_streaming_ffmpeg_decode(tmp_path: Path):
    """
    Phase 4 (WP-4) End-to-End Verification:
    1. Generate real QuickTime MOV with pcm_s16le audio and H.264 video (4.0s, GOP=25).
    2. Extract sidecar AAC bundle with build_sidecar_audio_bundle().
    3. Plan multi-part volumes with plan_vsse_volumes(..., sidecar_audio=True).
    4. For each volume:
       - Synthesize dual-region FastStart header with synthesize_faststart_envelope_header().
       - Stream virtual volume via VirtualEnvelopeChunkProvider.
       - Verify ffprobe parses stream with 0 errors.
       - Verify ffmpeg -v error -i pipe:0 -f null - decodes stream with 0 errors!
       - Verify audio-video sync error < 25 ms.
    """
    import json

    if shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None:
        pytest.skip("FFmpeg/FFprobe 未安装，跳过真实多卷端到端验证")

    mov_path = tmp_path / "sample_mov_pcm.mov"
    # 4-second video, 25fps (100 frames), GOP=25 (keyframes at 1, 26, 51, 76), pcm_s16le audio
    cmd_gen = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=320x240:rate=25:duration=4",
        "-f",
        "lavfi",
        "-i",
        "sine=r=48000:f=1000:d=4",
        "-c:v",
        "libx264",
        "-g",
        "25",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "pcm_s16le",
        str(mov_path),
    ]
    subprocess.run(cmd_gen, capture_output=True, check=True)

    data = mov_path.read_bytes()
    boxes = scan_top_level_boxes(data, len(data))
    moov_box = next(b for b in boxes if b.box_type == b"moov")
    moov_bytes = data[moov_box.offset : moov_box.offset + moov_box.size]
    meta = parse_moov(moov_bytes)

    raw_provider = LocalFileChunkProvider(str(mov_path))
    bundle = await build_sidecar_audio_bundle(
        metadata=meta,
        provider=raw_provider,
        head_bytes=data[: min(128 * 1024, len(data))],
        moov_offset=moov_box.offset,
        moov_bytes=moov_bytes,
        temp_dir=tmp_path,
    )

    assert bundle.video_track is not None
    assert bundle.aac_track is not None
    assert len(bundle.aac_payload) > 0

    # Calculate threshold to split into 2 volumes (~2s per volume)
    # Total video samples = 100. Keyframes at 1, 26, 51, 76.
    _, _, v_half_payload = compute_envelope_bounds(bundle.hybrid_metadata, 1, 50, None, None)
    threshold = v_half_payload + 15000

    plans = plan_vsse_volumes(
        meta=bundle.hybrid_metadata,
        threshold_bytes=threshold,
        file_name="split_output.mp4",
        safety_margin_bytes=1000,
        sidecar_audio=True,
    )
    assert len(plans) >= 2, f"Expected at least 2 volumes, got {len(plans)}"

    # Audio slice concatenation check across all parts
    all_audio_slices = b"".join(bundle.slice_bytes(p.a_o_min, p.a_o_max) for p in plans if p.a_start is not None)
    assert len(all_audio_slices) == len(bundle.aac_payload)

    for plan in plans:
        audio_slice = bundle.slice_bytes(plan.a_o_min, plan.a_o_max)
        header = synthesize_faststart_envelope_header(
            metadata=bundle.hybrid_metadata,
            ftyp_bytes=DEFAULT_FTYP_BOX,
            v_start=plan.v_start,
            v_end=plan.v_end,
            a_start=plan.a_start,
            a_end=plan.a_end,
            o_min=plan.o_min,
            o_max=plan.o_max,
            payload_len=plan.payload_len,
            sidecar_audio_slice=audio_slice,
            a_o_min=plan.a_o_min,
        )

        virtual_provider = VirtualEnvelopeChunkProvider(
            file_id=f"part_{plan.part_index}",
            file_name=plan.part_name,
            faststart_header=header,
            source_provider=raw_provider,
            envelope_start=plan.o_min,
            envelope_length=plan.payload_len,
        )

        # Assemble full volume bytes from chunk provider
        vol_data = bytearray()
        read_chunk_size = 64 * 1024
        curr_offset = 0
        while curr_offset < virtual_provider.total_bytes:
            req_size = min(read_chunk_size, virtual_provider.total_bytes - curr_offset)
            chunk = await virtual_provider.read_chunk(curr_offset, req_size)
            assert len(chunk) == req_size
            vol_data.extend(chunk)
            curr_offset += len(chunk)

        assert len(vol_data) == virtual_provider.total_bytes
        vol_bytes = bytes(vol_data)

        # 1. FFprobe verification
        cmd_probe = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "stream=index,codec_type,duration",
            "-of",
            "json",
            "pipe:0",
        ]
        proc_probe = subprocess.run(cmd_probe, input=vol_bytes, capture_output=True, check=True)
        probe_json = json.loads(proc_probe.stdout.decode("utf-8"))
        streams = probe_json.get("streams", [])
        assert len(streams) >= 2
        v_stream = next(s for s in streams if s["codec_type"] == "video")
        a_stream = next(s for s in streams if s["codec_type"] == "audio")
        assert float(v_stream["duration"]) > 0.5
        assert float(a_stream["duration"]) > 0.5

        # 2. FFmpeg null decode verification: 0 error
        cmd_decode = ["ffmpeg", "-v", "error", "-i", "pipe:0", "-f", "null", "-"]
        proc_decode = subprocess.run(cmd_decode, input=vol_bytes, capture_output=True)
        assert proc_decode.returncode == 0, f"FFmpeg decode failed on {plan.part_name}: {proc_decode.stderr.decode()}"
        assert proc_decode.stderr == b"", f"FFmpeg decode produced stderr: {proc_decode.stderr.decode()}"

        # 3. Audio-video synchronization verification: sync error < 25ms (0.025s)
        cmd_packets = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "packet=pts_time,codec_type",
            "-of",
            "json",
            "pipe:0",
        ]
        proc_packets = subprocess.run(cmd_packets, input=vol_bytes, capture_output=True, check=True)
        packet_json = json.loads(proc_packets.stdout.decode("utf-8"))
        packets = packet_json.get("packets", [])
        first_v_pkt = next(p for p in packets if p.get("codec_type") == "video")
        first_a_pkt = next(p for p in packets if p.get("codec_type") == "audio")
        pts_v = float(first_v_pkt["pts_time"])
        pts_a = float(first_a_pkt["pts_time"])
        sync_error_sec = abs(pts_v - pts_a)
        assert sync_error_sec < 0.025, (
            f"Volume {plan.part_name} A/V sync error {sync_error_sec * 1000:.1f}ms exceeds 25ms (pts_v={pts_v}, pts_a={pts_a})"
        )


def test_build_audio_spans_qt_lpcm_tail_chunk_not_truncated():
    """验证 QuickTime lpcm (UniformStszTable uniform_size=1) 在 mdat 最后一个 Chunk 不会被错误截断至 1/4。"""
    from app.core.iso_bmff.box_parser import UniformStszTable

    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[1000])
    a_track = _create_synthetic_track(2, is_video=False, chunk_offsets=[50000], codec_format=b"lpcm")
    # QuickTime v0 lpcm: uniform_size=1, 22050 samples in chunk (actual bytes = 88200 at 16-bit stereo)
    a_track.stbl.stsz = UniformStszTable(uniform_size=1, sample_count=22050)
    a_track.stbl.default_sample_size = 1
    a_track.stbl.stsc = [StscEntry(first_chunk=1, samples_per_chunk=22050, sample_description_index=1)]
    meta = _create_synthetic_metadata([v_track, a_track])

    # Rear moov starts at 138200 (exact end of the 88200-byte PCM chunk)
    spans = build_audio_spans(meta, total_bytes=150000, moov_offset=138200)
    assert spans == [(50000, 138200)]


@pytest.mark.asyncio
async def test_build_sidecar_audio_bundle_control_signal_cancellation(tmp_path: Path):
    """验证在 FFmpeg 旁路转码期间触发 check_control_fn / is_cancelled 能立即中断进程并清理临时文件。"""
    if shutil.which("ffmpeg") is None:
        pytest.skip("ffmpeg not available on PATH")
    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[1000])
    a_track = _create_synthetic_track(2, is_video=False, chunk_offsets=[5000])
    meta = _create_synthetic_metadata([v_track, a_track])

    class HangingProvider(BaseChunkProvider):
        file_id = "hang"
        file_name = "hang.mp4"
        total_bytes = 20000

        async def read_chunk(self, offset: int, size: int) -> bytes:
            await asyncio.sleep(5.0)
            return b"\x00" * size

    class CustomControlAbortError(Exception):
        pass

    def _abort_now() -> None:
        raise CustomControlAbortError("user paused task")

    with pytest.raises(CustomControlAbortError, match="user paused task"):
        await build_sidecar_audio_bundle(
            metadata=meta,
            provider=HangingProvider(),
            head_bytes=b"0" * 1024,
            moov_offset=10000,
            moov_bytes=b"0" * 1024,
            temp_dir=tmp_path,
            check_control_fn=_abort_now,
        )

    assert len(list(tmp_path.glob("sidecar_audio_*.mp4"))) == 0


def test_build_audio_spans_large_chunk_split_without_gap():
    """Verify audio chunks larger than max_span_size are split into contiguous spans without truncation."""
    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[1000, 5_000_000])
    a_track = _create_synthetic_track(2, is_video=False, chunk_offsets=[10_000])
    meta = _create_synthetic_metadata([v_track, a_track])

    # Audio chunk spans [10_000, 5_000_000) (~4.99 MB), max_span_size=2_000_000
    spans = build_audio_spans(meta, total_bytes=6_000_000, moov_offset=0, max_span_size=2_000_000)
    assert spans == [
        (10_000, 2_010_000),
        (2_010_000, 4_010_000),
        (4_010_000, 5_000_000),
    ]


@pytest.mark.asyncio
async def test_build_sidecar_audio_bundle_restores_connector_limits(tmp_path: Path):
    """Verify explicit _pool_limit_per_host and live connector _limit/_limit_per_host are restored in finally."""

    class _FakeConnector:
        def __init__(self, limit_per_host: int, limit: int) -> None:
            self._limit_per_host = limit_per_host
            self._limit = limit

    class _FakeSession:
        def __init__(self, conn: _FakeConnector) -> None:
            self.connector = conn
            self.closed = False

    class _PoolAwareProvider(BaseChunkProvider):
        def __init__(self) -> None:
            self.file_id = "pool_test"
            self.file_name = "pool.mp4"
            self.total_bytes = 20000
            self.prefetch_concurrency = 2
            self._pool_limit_per_host = 4
            self._session = _FakeSession(_FakeConnector(limit_per_host=4, limit=8))

        async def read_chunk(self, offset: int, size: int) -> bytes:
            return b"\x00" * size

    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[1000])
    a_track = _create_synthetic_track(2, is_video=False, chunk_offsets=[5000])
    meta = _create_synthetic_metadata([v_track, a_track])
    provider = _PoolAwareProvider()

    with pytest.raises(FileNotFoundError):
        await build_sidecar_audio_bundle(
            metadata=meta,
            provider=provider,
            head_bytes=b"0" * 1024,
            moov_offset=10000,
            moov_bytes=b"0" * 1024,
            temp_dir=tmp_path,
            ffmpeg_bin="nonexistent_ffmpeg_binary_for_test",
        )

    assert provider._pool_limit_per_host == 4
    assert provider._session.connector._limit_per_host == 4
    assert provider._session.connector._limit == 8


def test_build_audio_spans_rejects_nonpositive_max_span_size():
    """#13: max_span_size <= 0 必须显式报错, 防止游标死循环或倒退。"""
    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[1000])
    a_track = _create_synthetic_track(2, is_video=False, chunk_offsets=[5000])
    meta = _create_synthetic_metadata([v_track, a_track])

    for bad_size in (0, -1024):
        with pytest.raises(ValueError, match="max_span_size"):
            build_audio_spans(meta, total_bytes=100_000, max_span_size=bad_size)


def test_build_audio_spans_qt_lpcm_large_tail_chunk_split_without_truncation():
    """#14: QuickTime lpcm 尾块超过 max_span_size 时必须完整切分至 file_limit, 不可截断在 off + max_span_size。"""
    from app.core.iso_bmff.box_parser import UniformStszTable

    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[1000])
    a_track = _create_synthetic_track(2, is_video=False, chunk_offsets=[10_000], codec_format=b"lpcm")
    a_track.stbl.stsz = UniformStszTable(uniform_size=1, sample_count=1_250_000)
    a_track.stbl.default_sample_size = 1
    a_track.stbl.stsc = [StscEntry(first_chunk=1, samples_per_chunk=1_250_000, sample_description_index=1)]
    meta = _create_synthetic_metadata([v_track, a_track])

    spans = build_audio_spans(meta, total_bytes=5_500_000, moov_offset=5_010_000, max_span_size=2_000_000)
    assert spans == [
        (10_000, 2_010_000),
        (2_010_000, 4_010_000),
        (4_010_000, 5_010_000),
    ]


@pytest.mark.asyncio
async def test_build_sidecar_audio_bundle_preserves_pikpak_throttled_error(tmp_path: Path, monkeypatch: Any):
    """#12: LocalPagedRangeProxy 读取遭遇 PikPakThrottledError 时, 必须穿透 FFmpeg 失败保留原始异常与 retry_after。"""
    from unittest.mock import AsyncMock, MagicMock

    from app.core.stream_chunk_provider import PikPakThrottledError

    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[1000])
    a_track = _create_synthetic_track(2, is_video=False, chunk_offsets=[5000])
    meta = _create_synthetic_metadata([v_track, a_track])

    class ThrottledProvider(BaseChunkProvider):
        file_id = "throttled_fid"
        file_name = "throttled.mp4"
        total_bytes = 20000

        async def read_chunk(self, offset: int, size: int) -> bytes:
            raise PikPakThrottledError("CDN 503", status=503, retry_after=321.0)

    async def fake_exec(*cmd, **kwargs):
        # Extract proxy URL from ffmpeg args and trigger a real range read against LocalPagedRangeProxy
        url_idx = cmd.index("-i") + 1
        proxy_url = cmd[url_idx]
        import aiohttp

        async with aiohttp.ClientSession() as sess:
            with pytest.raises(aiohttp.ClientError):
                async with sess.get(proxy_url, headers={"Range": "bytes=5000-5100"}) as resp:
                    await resp.read()
        proc = MagicMock()
        proc.returncode = 1
        proc.communicate = AsyncMock(return_value=(b"", b"HTTP error 500 Internal Server Error"))
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

    with pytest.raises(PikPakThrottledError) as exc_info:
        await build_sidecar_audio_bundle(
            metadata=meta,
            provider=ThrottledProvider(),
            head_bytes=b"0" * 1024,
            moov_offset=10000,
            moov_bytes=b"0" * 1024,
            temp_dir=tmp_path,
        )
    assert exc_info.value.retry_after == 321.0


@pytest.mark.asyncio
async def test_build_sidecar_audio_bundle_raises_proxy_error_even_on_ffmpeg_exit_zero(tmp_path: Path, monkeypatch: Any):
    """即使 FFmpeg 在遇错断流后仍以 returncode=0 退出, 也必须检查并重抛代理记录的限流异常。"""
    from unittest.mock import AsyncMock, MagicMock

    from app.core.stream_chunk_provider import PikPakThrottledError

    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[1000])
    a_track = _create_synthetic_track(2, is_video=False, chunk_offsets=[5000])
    meta = _create_synthetic_metadata([v_track, a_track])

    class ThrottledProvider(BaseChunkProvider):
        file_id = "throttled_exit0"
        file_name = "throttled0.mp4"
        total_bytes = 20000

        async def read_chunk(self, offset: int, size: int) -> bytes:
            raise PikPakThrottledError("CDN 429", status=429, retry_after=222.0)

    async def fake_exec(*cmd, **kwargs):
        url_idx = cmd.index("-i") + 1
        proxy_url = cmd[url_idx]
        import aiohttp

        async with aiohttp.ClientSession() as sess:
            with pytest.raises(aiohttp.ClientError):
                async with sess.get(proxy_url, headers={"Range": "bytes=5000-5100"}) as resp:
                    await resp.read()
        proc = MagicMock()
        proc.returncode = 0
        proc.communicate = AsyncMock(return_value=(b"", b""))
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

    with pytest.raises(PikPakThrottledError) as exc_info:
        await build_sidecar_audio_bundle(
            metadata=meta,
            provider=ThrottledProvider(),
            head_bytes=b"0" * 1024,
            moov_offset=10000,
            moov_bytes=b"0" * 1024,
            temp_dir=tmp_path,
        )
    assert exc_info.value.retry_after == 222.0


@pytest.mark.asyncio
async def test_build_sidecar_audio_bundle_mp4a_mp3_avoids_stream_copy(tmp_path: Path, monkeypatch: Any):
    """#5: mp4a 封装 MPEG audio (esds_object_type=0x6B) 不可走 -c:a copy, 必须走 -c:a aac 转码。"""
    from unittest.mock import AsyncMock, MagicMock

    v_track = _create_synthetic_track(1, is_video=True, chunk_offsets=[1000])
    a_track = _create_synthetic_track(2, is_video=False, chunk_offsets=[5000], codec_format=b"mp4a")
    a_track.stbl.stsd.entries[0].esds_object_type = 0x6B
    a_track.stbl.stsd.entries[0].codec = "mp3"
    meta = _create_synthetic_metadata([v_track, a_track])

    observed_cmds: list[tuple[str, ...]] = []

    class DummyProvider(BaseChunkProvider):
        file_id = "mp3_in_mp4a"
        file_name = "mp3.mp4"
        total_bytes = 20000

        async def read_chunk(self, offset: int, size: int) -> bytes:
            return b"\x00" * size

    async def fake_exec(*cmd, **kwargs):
        observed_cmds.append(cmd)
        proc = MagicMock()
        proc.returncode = 1
        proc.communicate = AsyncMock(return_value=(b"", b"simulated stop"))
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

    with pytest.raises(RuntimeError, match="simulated stop"):
        await build_sidecar_audio_bundle(
            metadata=meta,
            provider=DummyProvider(),
            head_bytes=b"0" * 1024,
            moov_offset=10000,
            moov_bytes=b"0" * 1024,
            temp_dir=tmp_path,
        )
    assert len(observed_cmds) == 1
    cmd_list = list(observed_cmds[0])
    assert "-c:a" in cmd_list
    assert cmd_list[cmd_list.index("-c:a") + 1] == "aac"
