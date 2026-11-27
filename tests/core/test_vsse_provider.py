"""
Comprehensive Unit and Integration Tests for VSSE Provider (Task 3).

Covers:
1. Volume Planning Accuracy:
   - Monotonicity of sample indices and timestamps.
   - Strict keyframe alignment on every volume start.
   - Audio interval continuity invariant: A_start^(k+1) = A_end^(k) + 1 (no gaps/overlaps).
   - Audio PTS alignment: PTS(A_start) <= PTS(V_start) (delta >= 0).
   - Strict volume size enforcement: estimated_total_bytes <= threshold_bytes.
   - Single-volume pass-through if whole file fits in threshold.
   - Video-only volume planning.
   - Irregular/sparse keyframes planning.
   - Invalid input error handling.
2. VirtualEnvelopeChunkProvider Reading:
   - Case 1: Pure in-memory FastStart header read.
   - Case 2: Pure remote Range read.
   - Case 3: Boundary crossing read (512KB and variable size across H boundary).
   - Boundary edge cases: exact 0, exact H, exact end, 1-byte crossing, clamped tail, offset >= total_bytes, negative/zero size.
   - aclose() no-op resource safety.
3. Stream Provider Integration:
   - Integration with LocalFileChunkProvider.
4. End-to-End Real MP4 FFprobe & FFmpeg Golden Verification:
   - Feed real MP4 media into VirtualEnvelopeChunkProvider.
   - Stream slices out via 512KB chunks.
   - Verify ffprobe parsing and 100% clean ffmpeg null decode with 0 errors!
   - Video-only real MP4 slicing and decode verification.
"""

import array
import os
import subprocess

import pytest

from app.core.iso_bmff.box_parser import (
    CttsEntry,
    Edts,
    ElstEntry,
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
    compute_sample_pts_dts,
    synthesize_faststart_envelope_header,
)
from app.core.iso_bmff.exceptions import IsoBmffError, NonInterleavedMp4Error
from app.core.stream_chunk_provider import BaseChunkProvider, LocalFileChunkProvider
from app.core.vsse_provider import (
    DEFAULT_FTYP_BOX,
    VirtualEnvelopeChunkProvider,
    find_audio_start_sample,
    format_part_filename,
    plan_vsse_volumes,
)

# -----------------------------------------------------------------------------
# Helpers to build synthetic models
# -----------------------------------------------------------------------------


def create_synthetic_track(
    track_id: int = 1,
    is_video: bool = True,
    sample_sizes: list[int] | None = None,
    chunk_offsets: list[int] | None = None,
    samples_per_chunk: int = 10,
    sample_delta: int = 1000,
    timescale: int = 1000,
    sync_samples: list[int] | None = None,
    ctts_offsets: list[int] | None = None,
) -> Track:
    """Helper to create a strongly-typed Track object for testing."""
    sizes = sample_sizes if sample_sizes is not None else [1000] * 30
    num_samples = len(sizes)

    total_chunks = (num_samples + samples_per_chunk - 1) // samples_per_chunk
    if chunk_offsets is not None:
        offsets = chunk_offsets
    else:
        offsets = [10000 + i * 15000 for i in range(total_chunks)]

    stsz_arr = array.array("I", sizes)
    co_arr = array.array("Q", offsets)

    stsc = [StscEntry(first_chunk=1, samples_per_chunk=samples_per_chunk, sample_description_index=1)]
    stts = [SttsEntry(sample_count=num_samples, sample_delta=sample_delta)]

    ctts = None
    if ctts_offsets is not None:
        ctts = [CttsEntry(sample_count=1, sample_offset=off) for off in ctts_offsets]

    stss_arr = None
    if sync_samples is not None:
        stss_arr = array.array("I", sync_samples)
    elif is_video:
        stss_arr = array.array("I", [1 + i * 10 for i in range((num_samples + 9) // 10)])

    if is_video:
        sample_entry = SampleEntry(
            format=b"avc1",
            data_ref_index=1,
            codec="avc1",
            width=1920,
            height=1080,
        )
    else:
        sample_entry = SampleEntry(
            format=b"mp4a",
            data_ref_index=1,
            codec="mp4a",
            channel_count=2,
            sample_rate=48000,
        )

    stsd = Stsd(version=0, flags=0, entry_count=1, entries=[sample_entry])

    stbl = SampleTable(
        stsd=stsd,
        stts=stts,
        stsc=stsc,
        stsz=stsz_arr,
        chunk_offsets=co_arr,
        stss=stss_arr,
        ctts=ctts,
        default_sample_size=0,
        is_co64=True,
    )

    tkhd = Tkhd(
        version=0,
        flags=7,
        creation_time=0,
        modification_time=0,
        track_id=track_id,
        duration=num_samples * sample_delta,
        layer=0,
        alternate_group=0,
        volume=0 if is_video else 0x0100,
        width=1920.0 if is_video else 0.0,
        height=1080.0 if is_video else 0.0,
        width_raw=(1920 << 16) if is_video else 0,
        height_raw=(1080 << 16) if is_video else 0,
    )

    mdhd = Mdhd(
        version=0,
        flags=0,
        creation_time=0,
        modification_time=0,
        timescale=timescale,
        duration=num_samples * sample_delta,
        language=0x55C4,
    )

    handler_type = b"vide" if is_video else b"soun"
    hdlr = Hdlr(handler_type=handler_type, name="VideoHandler" if is_video else "SoundHandler")

    return Track(
        track_id=track_id,
        tkhd=tkhd,
        mdhd=mdhd,
        hdlr=hdlr,
        stbl=stbl,
        edts=None,
    )


def create_synthetic_metadata(
    v_track: Track,
    a_track: Track | None = None,
    movie_timescale: int = 1000,
) -> Mp4Metadata:
    """Helper to assemble a valid Mp4Metadata object from tracks."""
    tracks = [v_track]
    if a_track is not None:
        tracks.append(a_track)

    max_dur = max(t.tkhd.duration for t in tracks)
    mvhd = Mvhd(
        version=0,
        flags=0,
        creation_time=0,
        modification_time=0,
        timescale=movie_timescale,
        duration=max_dur,
        rate=0x00010000,
        volume=0x0100,
        matrix=(0x00010000, 0, 0, 0, 0x00010000, 0, 0, 0, 0x40000000),
        next_track_id=len(tracks) + 1,
    )

    return Mp4Metadata(mvhd=mvhd, tracks=tracks, udta_bytes=None)


class MockByteChunkProvider(BaseChunkProvider):
    """In-memory chunk provider simulating remote source reads."""

    def __init__(self, data: bytes, file_id: str = "mock_source", file_name: str = "mock.mp4"):
        self._data = bytes(data)
        self.file_id = file_id
        self.file_name = file_name
        self.total_bytes = len(self._data)
        self.read_history: list[tuple[int, int]] = []
        self.closed = False

    async def read_chunk(self, offset: int, size: int) -> bytes:
        if offset < 0:
            raise ValueError(f"Negative offset: {offset}")
        if size <= 0:
            raise ValueError(f"Non-positive size: {size}")
        if offset >= self.total_bytes:
            raise ValueError(f"Offset out of bounds: {offset} >= {self.total_bytes}")

        self.read_history.append((offset, size))
        clamped_size = min(size, self.total_bytes - offset)
        return self._data[offset : offset + clamped_size]

    async def aclose(self) -> None:
        self.closed = True


# -----------------------------------------------------------------------------
# Part 1: Volume Planning Tests
# -----------------------------------------------------------------------------


def test_format_part_filename():
    """Verify part naming formatting rules."""
    # Single part keeps original name
    assert format_part_filename("movie.mp4", 1, 1) == "movie.mp4"
    assert format_part_filename("my_video.mkv", 1, 1) == "my_video.mkv"
    assert format_part_filename("raw_stream", 1, 1) == "raw_stream"

    # Multi part adds .part001.ext
    assert format_part_filename("movie.mp4", 1, 3) == "movie.part001.mp4"
    assert format_part_filename("movie.mp4", 2, 3) == "movie.part002.mp4"
    assert format_part_filename("movie.mp4", 3, 3) == "movie.part003.mp4"

    # Fallback extension if none provided
    assert format_part_filename("videofile", 1, 2) == "videofile.part001.mp4"


def test_plan_vsse_volumes_single_part_fits():
    """
    If total virtual size fits within threshold_bytes, plan_vsse_volumes must
    return exactly 1 volume plan with part_name matching file_name.
    """
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[500] * 20,  # 20 samples * 500 bytes = 10,000 bytes
        chunk_offsets=[1000, 6000],  # 2 chunks of 10 samples
        samples_per_chunk=10,
        sync_samples=[1, 11],
    )
    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[200] * 40,  # 40 samples * 200 bytes = 8,000 bytes
        chunk_offsets=[11000, 15000],  # 2 chunks of 20 samples
        samples_per_chunk=20,
    )
    meta = create_synthetic_metadata(v_track, a_track)

    # Threshold large enough for the entire file (e.g. 500,000 bytes)
    plans = plan_vsse_volumes(meta, threshold_bytes=500_000, file_name="single.mp4")

    assert len(plans) == 1
    p = plans[0]
    assert p.part_index == 1
    assert p.total_parts == 1
    assert p.part_name == "single.mp4"
    assert p.file_name == "single.mp4"
    assert p.v_start == 1
    assert p.v_end == 20
    assert p.a_start == 1
    assert p.a_end == 40
    assert p.estimated_total_bytes <= 500_000
    assert p.start_sec == 0.0
    assert p.duration_sec > 0.0
    assert p.envelope_start == p.o_min
    assert p.envelope_length == p.payload_len


def test_plan_vsse_volumes_multi_part_video_audio():
    """
    Verify volume planning accuracy for multi-part video+audio:
    1. Monotonicity of sample indices and timestamps.
    2. Keyframe alignment (all v_start in stss).
    3. Audio continuity invariant: A_start^(k+1) = A_end^(k) + 1.
    4. Audio PTS alignment: PTS(A_start) <= PTS(V_start) (delta >= 0).
    5. No gaps or overlaps across video or audio.
    6. All volume sizes <= threshold_bytes.
    """
    # Interleaved audio and video chunks:
    # Each GOP has 1 video chunk (10,000B) followed by 1 audio chunk (5,000B)
    v_offsets = [10000 + i * 15000 for i in range(6)]
    a_offsets = [20000 + i * 15000 for i in range(6)]
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[1000] * 60,
        chunk_offsets=v_offsets,
        samples_per_chunk=10,
        sample_delta=1000,
        timescale=1000,
        sync_samples=[1, 11, 21, 31, 41, 51],
    )
    # Audio track: 120 samples, 250 bytes each (each chunk of 20 samples = 5,000 bytes)
    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[250] * 120,
        chunk_offsets=a_offsets,
        samples_per_chunk=20,
        sample_delta=500,
        timescale=1000,
    )
    meta = create_synthetic_metadata(v_track, a_track)

    # Threshold set to ~50,000 bytes so we get ~3 volumes
    threshold = 50_000
    safety_margin = 5_000
    plans = plan_vsse_volumes(
        meta,
        threshold_bytes=threshold,
        file_name="episodes.mp4",
        safety_margin_bytes=safety_margin,
    )

    assert len(plans) >= 2
    total_parts = len(plans)

    # 1. Monotonicity & part numbering
    for i, p in enumerate(plans):
        assert p.part_index == i + 1
        assert p.total_parts == total_parts
        assert p.part_name == f"episodes.part{i + 1:03d}.mp4"
        assert p.estimated_total_bytes <= threshold

    # 2. Keyframe alignment (every volume starts on a keyframe)
    for p in plans:
        assert p.v_start in v_track.stbl.stss

    # 3. Video continuity (no gaps, no overlaps)
    assert plans[0].v_start == 1
    assert plans[-1].v_end == 60
    for i in range(len(plans) - 1):
        assert plans[i].v_end + 1 == plans[i + 1].v_start
        assert plans[i].start_sec < plans[i + 1].start_sec

    # 4. Audio continuity (A_start^(k+1) = A_end^(k) + 1, no gaps, no overlaps)
    assert plans[0].a_start == 1
    assert plans[-1].a_end == 120
    for i in range(len(plans) - 1):
        assert plans[i].a_end + 1 == plans[i + 1].a_start

    # 5. Audio PTS alignment (delta >= 0)
    for p in plans:
        assert p.a_start is not None
        _, pts_v = compute_sample_pts_dts(v_track, p.v_start)
        _, pts_a = compute_sample_pts_dts(a_track, p.a_start)
        pts_v_sec = pts_v / v_track.mdhd.timescale
        pts_a_sec = pts_a / a_track.mdhd.timescale
        delta = pts_v_sec - pts_a_sec
        assert delta >= -1e-6, f"Part {p.part_index} audio PTS ({pts_a_sec}s) > video PTS ({pts_v_sec}s)"


def test_plan_vsse_volumes_video_only():
    """
    Verify volume planning for video-only files:
    - a_start and a_end are None.
    - Slicing and keyframe continuity are strictly preserved.
    """
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[2000] * 50,
        chunk_offsets=[1000 + i * 25000 for i in range(5)],
        samples_per_chunk=10,
        sync_samples=[1, 11, 21, 31, 41],
    )
    meta = create_synthetic_metadata(v_track, a_track=None)

    plans = plan_vsse_volumes(
        meta,
        threshold_bytes=40_000,
        file_name="silent.mp4",
        safety_margin_bytes=2_000,
    )

    assert len(plans) >= 2
    for i, p in enumerate(plans):
        assert p.a_start is None
        assert p.a_end is None
        assert p.v_start in v_track.stbl.stss
        assert p.estimated_total_bytes <= 40_000
        if i > 0:
            assert plans[i - 1].v_end + 1 == p.v_start

    assert plans[0].v_start == 1
    assert plans[-1].v_end == 50


def test_plan_vsse_volumes_irregular_sparse_keyframes():
    """
    Verify planning handles irregular GOP lengths without skipping frames.
    """
    # Keyframes at non-uniform intervals: 1, 5, 25, 45, 50
    sync_samples = [1, 5, 25, 45, 50]
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[1000] * 55,
        chunk_offsets=[5000 + i * 12000 for i in range(6)],
        samples_per_chunk=10,
        sync_samples=sync_samples,
    )
    meta = create_synthetic_metadata(v_track, a_track=None)

    plans = plan_vsse_volumes(
        meta,
        threshold_bytes=30_000,
        file_name="irregular.mp4",
        safety_margin_bytes=1_000,
    )

    assert len(plans) >= 2
    for p in plans:
        assert p.v_start in sync_samples
        assert p.estimated_total_bytes <= 30_000

    # Ensure full coverage
    assert plans[0].v_start == 1
    assert plans[-1].v_end == 55
    for i in range(len(plans) - 1):
        assert plans[i].v_end + 1 == plans[i + 1].v_start


def test_plan_vsse_volumes_validation_errors():
    """Verify input validation and error raising."""
    v_track = create_synthetic_track(track_id=1, is_video=True)
    meta = create_synthetic_metadata(v_track)

    # Non-positive threshold
    with pytest.raises(ValueError, match="threshold_bytes must be positive"):
        plan_vsse_volumes(meta, threshold_bytes=0, file_name="bad.mp4")

    with pytest.raises(ValueError, match="threshold_bytes must be positive"):
        plan_vsse_volumes(meta, threshold_bytes=-100, file_name="bad.mp4")

    # Missing video track
    a_track = create_synthetic_track(track_id=2, is_video=False)
    meta_audio_only = Mp4Metadata(mvhd=meta.mvhd, tracks=[a_track], udta_bytes=None)
    with pytest.raises(IsoBmffError, match="Missing video track"):
        plan_vsse_volumes(meta_audio_only, threshold_bytes=100_000, file_name="bad.mp4")


def test_find_audio_start_sample_behavior():
    """Verify find_audio_start_sample binary search."""
    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[250] * 100,
        sample_delta=1024,
        timescale=48000,
    )
    # Each sample duration = 1024 / 48000 = 0.021333s
    # Sample 1: 0.0s
    # Sample 48: 47 * 1024 / 48000 = 1.00266s
    # Sample 47: 46 * 1024 / 48000 = 0.98133s
    # Target video PTS = 1.0s
    idx = find_audio_start_sample(a_track, pts_v_sec=1.0, min_audio_sample=1)
    assert idx == 47  # sample 47 has PTS 0.98133s <= 1.0s

    # Target video PTS earlier than min_audio_sample PTS
    idx_none = find_audio_start_sample(a_track, pts_v_sec=0.5, min_audio_sample=50)
    assert idx_none is None


# -----------------------------------------------------------------------------
# Part 2: VirtualEnvelopeChunkProvider Unit Tests
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_virtual_envelope_provider_three_states_byte_exact():
    """
    Verify three-state chunk routing and byte-exact fidelity:
    - Case 1: Pure in-memory FastStart header read (offset + size <= H).
    - Case 2: Pure remote Range read (offset >= H).
    - Case 3: Boundary crossing read (head from memory, tail from remote source).
    """
    # Header: 4,096 bytes of distinct pattern
    header_bytes = bytes([i % 251 for i in range(4096)])
    H = len(header_bytes)

    # Source media data: 32,768 bytes of distinct pattern
    source_bytes = bytes([(i * 13 + 7) % 249 for i in range(32768)])
    envelope_start = 8192
    envelope_length = 16384
    mock_source = MockByteChunkProvider(source_bytes)

    # Full expected virtual file: Header (4,096B) + Source payload (16,384B) = 20,480B
    expected_virtual_data = header_bytes + source_bytes[envelope_start : envelope_start + envelope_length]
    assert len(expected_virtual_data) == H + envelope_length

    provider = VirtualEnvelopeChunkProvider(
        file_id="vsse_part_1",
        file_name="movie.part001.mp4",
        faststart_header=header_bytes,
        source_provider=mock_source,
        envelope_start=envelope_start,
        envelope_length=envelope_length,
    )

    assert provider.total_bytes == len(expected_virtual_data)
    assert provider.header_len == H
    assert provider.envelope_start == envelope_start
    assert provider.envelope_length == envelope_length

    # --- Case 1: Pure in-memory header reads ---
    data_1 = await provider.read_chunk(0, 1024)
    assert data_1 == expected_virtual_data[0:1024]

    data_2 = await provider.read_chunk(1024, 2048)
    assert data_2 == expected_virtual_data[1024:3072]

    # Read ending exactly at boundary H
    data_3 = await provider.read_chunk(3072, 1024)
    assert data_3 == expected_virtual_data[3072:4096]
    # In Case 1, mock_source should NOT have been called!
    assert len(mock_source.read_history) == 0

    # --- Case 2: Pure remote Range reads ---
    # Read starting exactly at boundary H
    data_4 = await provider.read_chunk(4096, 2048)
    assert data_4 == expected_virtual_data[4096:6144]
    assert mock_source.read_history[-1] == (envelope_start, 2048)

    # Read further inside remote payload
    data_5 = await provider.read_chunk(8192, 4096)
    assert data_5 == expected_virtual_data[8192:12288]
    assert mock_source.read_history[-1] == (envelope_start + (8192 - H), 4096)

    # --- Case 3: Boundary crossing reads ---
    # Crossing H=4096: from 3,000 to 5,000 (1,096 bytes header + 904 bytes remote)
    mock_source.read_history.clear()
    data_cross = await provider.read_chunk(3000, 2000)
    assert len(data_cross) == 2000
    assert data_cross == expected_virtual_data[3000:5000]
    assert mock_source.read_history[-1] == (envelope_start, 904)

    # Large 512KB-like chunk crossing boundary H
    # From offset 1000 with length 6000 (crossing 4096)
    data_cross_large = await provider.read_chunk(1000, 6000)
    assert len(data_cross_large) == 6000
    assert data_cross_large == expected_virtual_data[1000:7000]


@pytest.mark.asyncio
async def test_virtual_envelope_provider_boundary_edge_cases():
    """
    Verify all boundary conditions, clamping, and exception raising.
    """
    header_bytes = b"HEADER_12345"  # 12 bytes
    H = len(header_bytes)
    source_bytes = b"PAYLOAD_ABCDEFGHIJ"  # 18 bytes
    envelope_start = 2
    envelope_length = 10
    total = H + envelope_length  # 12 + 10 = 22 bytes

    mock_source = MockByteChunkProvider(source_bytes)
    provider = VirtualEnvelopeChunkProvider(
        file_id="test_edges",
        file_name="edge.mp4",
        faststart_header=header_bytes,
        source_provider=mock_source,
        envelope_start=envelope_start,
        envelope_length=envelope_length,
    )
    expected_full = header_bytes + source_bytes[envelope_start : envelope_start + envelope_length]

    # 1. Read at exact 0 (single byte)
    b0 = await provider.read_chunk(0, 1)
    assert b0 == expected_full[0:1]

    # 2. Read at exact H-1 (last byte of header)
    b_hm1 = await provider.read_chunk(H - 1, 1)
    assert b_hm1 == expected_full[H - 1 : H]

    # 3. Read at exact H (first byte of remote payload)
    b_h = await provider.read_chunk(H, 1)
    assert b_h == expected_full[H : H + 1]

    # 4. Read spanning H-1 to H+1 (2 bytes, 1 from header, 1 from payload)
    b_span = await provider.read_chunk(H - 1, 2)
    assert b_span == expected_full[H - 1 : H + 1]

    # 5. Read at exact end (last byte of file)
    b_end = await provider.read_chunk(total - 1, 1)
    assert b_end == expected_full[total - 1 : total]

    # 6. Read clamping to file EOF: offset=total-5, size=20 -> returns 5 bytes
    b_clamped = await provider.read_chunk(total - 5, 20)
    assert len(b_clamped) == 5
    assert b_clamped == expected_full[total - 5 :]

    # 7. Error cases
    with pytest.raises(ValueError, match="越界"):
        await provider.read_chunk(total, 1)

    with pytest.raises(ValueError, match="越界"):
        await provider.read_chunk(total + 10, 1)

    with pytest.raises(ValueError, match="负数"):
        await provider.read_chunk(-1, 5)

    with pytest.raises(ValueError, match="非法读取长度"):
        await provider.read_chunk(0, 0)

    with pytest.raises(ValueError, match="非法读取长度"):
        await provider.read_chunk(0, -10)

    # 8. Constructor validation
    with pytest.raises(ValueError, match="file_id"):
        VirtualEnvelopeChunkProvider("", "name", header_bytes, mock_source, 0, 10)

    with pytest.raises(ValueError, match="file_name"):
        VirtualEnvelopeChunkProvider("id", "", header_bytes, mock_source, 0, 10)

    with pytest.raises(ValueError, match="source_provider"):
        VirtualEnvelopeChunkProvider("id", "name", header_bytes, None, 0, 10)  # type: ignore

    with pytest.raises(ValueError, match="envelope_start"):
        VirtualEnvelopeChunkProvider("id", "name", header_bytes, mock_source, -1, 10)

    with pytest.raises(ValueError, match="envelope_length"):
        VirtualEnvelopeChunkProvider("id", "name", header_bytes, mock_source, 0, -5)

    # 9. aclose() is a no-op and does not close underlying provider
    await provider.aclose()
    assert mock_source.closed is False


# -----------------------------------------------------------------------------
# Part 3: Stream Provider Integration Tests
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_virtual_envelope_provider_with_local_file_chunk_provider(tmp_path):
    """
    Verify VirtualEnvelopeChunkProvider integration with LocalFileChunkProvider.
    Read entire file in 512KB chunks, verifying byte-exactness and hash fidelity.
    """
    raw_file = tmp_path / "raw_source.bin"
    file_content = bytearray()
    for i in range(100_000):
        file_content.extend((i % 256).to_bytes(1, "big"))
    raw_file.write_bytes(file_content)

    local_provider = LocalFileChunkProvider(str(raw_file))
    header_content = b"FASTSTART_ENVELOPE_HEADER_PAYLOAD_TEST_DATA" * 50  # ~2,200 bytes

    envelope_start = 10_000
    envelope_len = 50_000
    provider = VirtualEnvelopeChunkProvider(
        file_id="local_test",
        file_name="streamed.mp4",
        faststart_header=header_content,
        source_provider=local_provider,
        envelope_start=envelope_start,
        envelope_length=envelope_len,
    )

    expected_full = header_content + file_content[envelope_start : envelope_start + envelope_len]
    assert provider.total_bytes == len(expected_full)

    # Read the whole virtual stream in 512-byte blocks
    streamed_out = bytearray()
    offset = 0
    chunk_size = 512
    while offset < provider.total_bytes:
        chunk = await provider.read_chunk(offset, chunk_size)
        assert len(chunk) > 0
        streamed_out.extend(chunk)
        offset += len(chunk)

    assert bytes(streamed_out) == expected_full


# -----------------------------------------------------------------------------
# Part 4: End-to-End Real MP4 FFmpeg & FFprobe Golden Verification
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_real_mp4_vsse_provider_e2e_ffmpeg_decode(tmp_path):
    """
    End-to-End Golden Verification:
    1. Generate a real MP4 with FFmpeg (FastStart, H.264 + AAC, 6.0 seconds, 25fps = 150 frames, GOP=25).
    2. Scan top-level boxes and parse moov.
    3. Plan VSSE volumes using plan_vsse_volumes to split into 2 or 3 parts.
    4. For each volume plan:
       - Synthesize FastStart envelope header with synthesize_faststart_envelope_header.
       - Create VirtualEnvelopeChunkProvider wrapping LocalFileChunkProvider.
       - Read full stream in 512KB chunks into an output .mp4 file.
       - Verify file size matches provider.total_bytes.
       - Run ffprobe and ffmpeg full-file null decode with 0 errors!
    """
    src_mp4 = str(tmp_path / "golden_src.mp4")
    # Generate 6.0 second video (150 frames at 25fps, keyframes at 1, 26, 51, 76, 101, 126)
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=320x240:rate=25",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=1000:sample_rate=48000",
        "-t",
        "6.0",
        "-c:v",
        "libx264",
        "-g",
        "25",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-movflags",
        "+faststart",
        src_mp4,
    ]
    subprocess.run(cmd, check=True)
    assert os.path.exists(src_mp4)

    file_size = os.path.getsize(src_mp4)
    with open(src_mp4, "rb") as f:
        file_bytes = f.read()

    top_boxes = scan_top_level_boxes(file_bytes, file_size)
    moov_box = next(b for b in top_boxes if b.box_type == b"moov")
    ftyp_box = next(b for b in top_boxes if b.box_type == b"ftyp")

    ftyp_bytes = file_bytes[ftyp_box.offset : ftyp_box.offset + ftyp_box.size]
    moov_bytes = file_bytes[moov_box.offset : moov_box.offset + moov_box.size]

    metadata = parse_moov(moov_bytes)
    assert metadata.video_track is not None
    assert metadata.audio_track is not None

    # Choose a threshold to split the ~150-250KB file into 2 or 3 volumes
    threshold = file_size // 2 + 5000
    plans = plan_vsse_volumes(
        meta=metadata,
        threshold_bytes=threshold,
        file_name="movie.mp4",
        safety_margin_bytes=2000,
    )

    assert len(plans) >= 2
    local_source = LocalFileChunkProvider(src_mp4)

    for p in plans:
        # 1. Synthesize FastStart envelope header
        env_header = synthesize_faststart_envelope_header(
            metadata=metadata,
            ftyp_bytes=ftyp_bytes,
            v_start=p.v_start,
            v_end=p.v_end,
            a_start=p.a_start,
            a_end=p.a_end,
            o_min=p.o_min,
            o_max=p.o_max,
            payload_len=p.payload_len,
        )

        # 2. Wrap in VirtualEnvelopeChunkProvider
        provider = VirtualEnvelopeChunkProvider(
            file_id=f"movie_part_{p.part_index}",
            file_name=p.part_name,
            faststart_header=env_header,
            source_provider=local_source,
            envelope_start=p.o_min,
            envelope_length=p.payload_len,
        )
        assert provider.total_bytes == len(env_header) + p.payload_len

        # 3. Stream all bytes via 512KB chunks into destination file
        dest_file = str(tmp_path / p.part_name)
        with open(dest_file, "wb") as out_f:
            offset = 0
            chunk_size = 512 * 1024  # 512KB Telegram chunk size
            while offset < provider.total_bytes:
                chunk = await provider.read_chunk(offset, chunk_size)
                out_f.write(chunk)
                offset += len(chunk)

        # 4. Verify file size
        assert os.path.getsize(dest_file) == provider.total_bytes

        # 5. FFmpeg full null decode verification (must be 0 error return code)
        decode_cmd = [
            "ffmpeg",
            "-v",
            "error",
            "-i",
            dest_file,
            "-f",
            "null",
            "-",
        ]
        res = subprocess.run(decode_cmd, capture_output=True, text=True)
        assert res.returncode == 0, f"Part {p.part_name} decode failed: {res.stderr}"

        # 6. FFprobe format & stream verification
        probe_cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration,size:stream=codec_name,width,height",
            "-of",
            "csv=p=0",
            dest_file,
        ]
        probe_res = subprocess.run(probe_cmd, capture_output=True, text=True)
        assert probe_res.returncode == 0, f"Part {p.part_name} probe failed: {probe_res.stderr}"
        output = probe_res.stdout
        assert "h264" in output
        assert "aac" in output


@pytest.mark.asyncio
async def test_real_mp4_vsse_provider_video_only_e2e(tmp_path):
    """
    Verify end-to-end slicing and decoding for real video-only MP4.
    """
    src_mp4 = str(tmp_path / "video_only_src.mp4")
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=320x240:rate=25",
        "-t",
        "4.0",
        "-c:v",
        "libx264",
        "-g",
        "25",
        "-pix_fmt",
        "yuv420p",
        "-an",
        "-movflags",
        "+faststart",
        src_mp4,
    ]
    subprocess.run(cmd, check=True)
    assert os.path.exists(src_mp4)

    file_size = os.path.getsize(src_mp4)
    with open(src_mp4, "rb") as f:
        file_bytes = f.read()

    top_boxes = scan_top_level_boxes(file_bytes, file_size)
    moov_box = next(b for b in top_boxes if b.box_type == b"moov")
    ftyp_box = next(b for b in top_boxes if b.box_type == b"ftyp")

    ftyp_bytes = file_bytes[ftyp_box.offset : ftyp_box.offset + ftyp_box.size]
    moov_bytes = file_bytes[moov_box.offset : moov_box.offset + moov_box.size]

    metadata = parse_moov(moov_bytes)
    assert metadata.video_track is not None
    assert metadata.audio_track is None

    threshold = file_size // 2 + 3000
    plans = plan_vsse_volumes(
        meta=metadata,
        threshold_bytes=threshold,
        file_name="silent_movie.mp4",
        safety_margin_bytes=1000,
    )

    assert len(plans) >= 2
    local_source = LocalFileChunkProvider(src_mp4)

    for p in plans:
        env_header = synthesize_faststart_envelope_header(
            metadata=metadata,
            ftyp_bytes=ftyp_bytes,
            v_start=p.v_start,
            v_end=p.v_end,
            a_start=None,
            a_end=None,
            o_min=p.o_min,
            o_max=p.o_max,
            payload_len=p.payload_len,
        )

        provider = VirtualEnvelopeChunkProvider(
            file_id=f"silent_part_{p.part_index}",
            file_name=p.part_name,
            faststart_header=env_header,
            source_provider=local_source,
            envelope_start=p.o_min,
            envelope_length=p.payload_len,
        )

        dest_file = str(tmp_path / p.part_name)
        with open(dest_file, "wb") as out_f:
            offset = 0
            while offset < provider.total_bytes:
                chunk = await provider.read_chunk(offset, 65536)
                out_f.write(chunk)
                offset += len(chunk)

        decode_cmd = ["ffmpeg", "-v", "error", "-i", dest_file, "-f", "null", "-"]
        res = subprocess.run(decode_cmd, capture_output=True, text=True)
        assert res.returncode == 0, f"Part {p.part_name} decode failed: {res.stderr}"


@pytest.mark.asyncio
async def test_virtual_envelope_provider_large_512k_boundary_crossing():
    """
    Simulate Telegram standard 512KB chunk read crossing the FastStart header H boundary.
    - Header: 300,000 bytes.
    - Media source: 1,000,000 bytes.
    - Read 524,288 bytes starting at offset 100,000 (200,000 from header + 324,288 from remote).
    """
    H = 300_000
    header_data = bytes([i % 251 for i in range(H)])
    source_len = 1_000_000
    source_data = bytes([(i * 17 + 3) % 249 for i in range(source_len)])
    envelope_start = 50_000
    envelope_len = 800_000

    mock_source = MockByteChunkProvider(source_data)
    provider = VirtualEnvelopeChunkProvider(
        file_id="tg_512k_test",
        file_name="large_chunk.mp4",
        faststart_header=header_data,
        source_provider=mock_source,
        envelope_start=envelope_start,
        envelope_length=envelope_len,
    )

    expected_full = header_data + source_data[envelope_start : envelope_start + envelope_len]
    read_offset = 100_000
    chunk_size = 512 * 1024  # 524,288 bytes

    chunk = await provider.read_chunk(read_offset, chunk_size)
    assert len(chunk) == chunk_size
    assert chunk == expected_full[read_offset : read_offset + chunk_size]

    # Verify head/tail split in mock_source
    assert len(mock_source.read_history) == 1
    remote_off, remote_sz = mock_source.read_history[0]
    assert remote_off == envelope_start
    assert remote_sz == chunk_size - (H - read_offset)  # 524,288 - 200,000 = 324,288


def test_plan_vsse_volumes_without_stss():
    """
    Verify volume planning when video track has no stss (all samples are keyframes).
    """
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[1000] * 30,
        chunk_offsets=[1000 + i * 10000 for i in range(3)],
        samples_per_chunk=10,
        sync_samples=None,  # All samples are sync samples
    )
    # Ensure stss is None
    v_track.stbl.stss = None
    meta = create_synthetic_metadata(v_track)

    plans = plan_vsse_volumes(
        meta,
        threshold_bytes=15_000,
        file_name="all_intra.mp4",
        safety_margin_bytes=1_000,
    )
    assert len(plans) >= 2
    for p in plans:
        assert p.estimated_total_bytes <= 15_000
    assert plans[0].v_start == 1
    assert plans[-1].v_end == 30


def test_plan_vsse_volumes_single_large_gop_fallback():
    """
    Verify that if a single GOP exceeds max_volume_bytes (threshold - safety_margin),
    the planner takes the single GOP to guarantee progress without looping indefinitely.
    """
    # 2 large GOPs: samples 1..20 (20,000B) and 21..40 (20,000B)
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[1000] * 40,
        chunk_offsets=[1000, 25000],
        samples_per_chunk=20,
        sync_samples=[1, 21],
    )
    meta = create_synthetic_metadata(v_track)

    # Threshold = 25,000, safety margin = 10,000 -> max_volume_bytes = 15,000
    # 1 GOP is ~20,000B > 15,000B, but <= 25,000B
    plans = plan_vsse_volumes(
        meta,
        threshold_bytes=25_000,
        file_name="large_gop.mp4",
        safety_margin_bytes=10_000,
    )

    assert len(plans) == 2
    assert plans[0].v_start == 1
    assert plans[0].v_end == 20
    assert plans[1].v_start == 21
    assert plans[1].v_end == 40


def test_plan_vsse_volumes_with_ctts_bframes():
    """
    Verify volume planning when video track has ctts entries (B-frames).
    Ensures PTS calculations account for composition time offsets and delta >= 0.
    """
    v_offsets = [10000 + i * 15000 for i in range(4)]
    a_offsets = [20000 + i * 15000 for i in range(4)]
    # CTTS offsets for video: repeating [0, 200, 100, 300]
    ctts_pattern = [0, 200, 100, 300] * 10
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[1000] * 40,
        chunk_offsets=v_offsets,
        samples_per_chunk=10,
        sample_delta=1000,
        timescale=1000,
        sync_samples=[1, 11, 21, 31],
        ctts_offsets=ctts_pattern,
    )
    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[250] * 80,
        chunk_offsets=a_offsets,
        samples_per_chunk=20,
        sample_delta=500,
        timescale=1000,
    )
    meta = create_synthetic_metadata(v_track, a_track)

    plans = plan_vsse_volumes(
        meta,
        threshold_bytes=35_000,
        file_name="bframes.mp4",
        safety_margin_bytes=3_000,
    )
    assert len(plans) >= 2
    for p in plans:
        _, pts_v = compute_sample_pts_dts(v_track, p.v_start)
        assert p.a_start is not None
        _, pts_a = compute_sample_pts_dts(a_track, p.a_start)
        pts_v_sec = pts_v / v_track.mdhd.timescale
        pts_a_sec = pts_a / a_track.mdhd.timescale
        assert pts_v_sec - pts_a_sec >= -1e-6


def test_plan_vsse_volumes_audio_shorter_than_video():
    """
    Verify volume planning when audio ends before video:
    - Early volumes include audio samples.
    - Subsequent volumes transition cleanly to video-only (a_start = None, a_end = None).
    - No negative audio ranges (a_start > a_end) or out-of-bounds sample indexing.
    - All volumes synthesize valid FastStart headers without error.
    """
    # 100 video samples across 10 chunks (10 GOPs of 10 samples)
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[1000] * 100,
        chunk_offsets=[10000 + i * 15000 for i in range(10)],
        samples_per_chunk=10,
        sync_samples=[1 + i * 10 for i in range(10)],
    )
    # Audio ends after 20 samples (sample 20 ends at 10.0s)
    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[250] * 20,
        chunk_offsets=[20000 + i * 15000 for i in range(2)],
        samples_per_chunk=10,
        sample_delta=500,
        timescale=1000,
    )
    meta = create_synthetic_metadata(v_track, a_track)

    plans = plan_vsse_volumes(
        meta,
        threshold_bytes=35_000,
        file_name="audio_ends_early.mp4",
        safety_margin_bytes=2_000,
    )

    assert len(plans) >= 2
    # Verify audio continuity for parts that contain audio
    audio_parts = [p for p in plans if p.a_start is not None]
    video_only_parts = [p for p in plans if p.a_start is None]

    assert len(audio_parts) >= 1
    assert len(video_only_parts) >= 1

    # In audio parts, first starts at 1, last ends at 20
    assert audio_parts[0].a_start == 1
    assert audio_parts[-1].a_end == 20
    for i in range(len(audio_parts) - 1):
        assert audio_parts[i].a_end + 1 == audio_parts[i + 1].a_start

    # In video-only parts, both a_start and a_end are None
    for p in video_only_parts:
        assert p.a_start is None
        assert p.a_end is None

    # Every single plan must synthesize FastStart header successfully
    for p in plans:
        hdr = synthesize_faststart_envelope_header(
            metadata=meta,
            ftyp_bytes=DEFAULT_FTYP_BOX,
            v_start=p.v_start,
            v_end=p.v_end,
            a_start=p.a_start,
            a_end=p.a_end,
            o_min=p.o_min,
            o_max=p.o_max,
            payload_len=p.payload_len,
        )
        assert len(hdr) > 0
        assert p.estimated_total_bytes <= 35_000


@pytest.mark.asyncio
async def test_real_mp4_vsse_provider_audio_ends_early_e2e_ffmpeg_decode(tmp_path):
    """
    End-to-End Golden Verification for Real MP4 with Audio Ending Early:
    1. Generate real MP4 with FFmpeg: 5.0s video, 2.0s audio (-af atrim=end=2.0).
    2. Plan VSSE volumes.
    3. Synthesize headers and stream each slice via VirtualEnvelopeChunkProvider.
    4. Verify 100% clean ffmpeg null decode with 0 errors across all parts!
    """
    src_mp4 = str(tmp_path / "audio_ends_early_src.mp4")
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=320x240:rate=25",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=1000:sample_rate=48000",
        "-t",
        "5.0",
        "-c:v",
        "libx264",
        "-g",
        "25",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-af",
        "atrim=end=2.0",
        "-movflags",
        "+faststart",
        src_mp4,
    ]
    subprocess.run(cmd, check=True)
    assert os.path.exists(src_mp4)

    file_size = os.path.getsize(src_mp4)
    with open(src_mp4, "rb") as f:
        file_bytes = f.read()

    top_boxes = scan_top_level_boxes(file_bytes, file_size)
    moov_box = next(b for b in top_boxes if b.box_type == b"moov")
    ftyp_box = next(b for b in top_boxes if b.box_type == b"ftyp")

    ftyp_bytes = file_bytes[ftyp_box.offset : ftyp_box.offset + ftyp_box.size]
    moov_bytes = file_bytes[moov_box.offset : moov_box.offset + moov_box.size]

    metadata = parse_moov(moov_bytes)
    assert metadata.video_track is not None
    assert metadata.audio_track is not None

    threshold = file_size // 2 + 5000
    plans = plan_vsse_volumes(
        meta=metadata,
        threshold_bytes=threshold,
        file_name="early_end.mp4",
        safety_margin_bytes=2000,
    )
    assert len(plans) >= 2
    local_source = LocalFileChunkProvider(src_mp4)

    for p in plans:
        env_header = synthesize_faststart_envelope_header(
            metadata=metadata,
            ftyp_bytes=ftyp_bytes,
            v_start=p.v_start,
            v_end=p.v_end,
            a_start=p.a_start,
            a_end=p.a_end,
            o_min=p.o_min,
            o_max=p.o_max,
            payload_len=p.payload_len,
        )

        provider = VirtualEnvelopeChunkProvider(
            file_id=f"early_end_part_{p.part_index}",
            file_name=p.part_name,
            faststart_header=env_header,
            source_provider=local_source,
            envelope_start=p.o_min,
            envelope_length=p.payload_len,
        )

        dest_file = str(tmp_path / p.part_name)
        with open(dest_file, "wb") as out_f:
            offset = 0
            while offset < provider.total_bytes:
                chunk = await provider.read_chunk(offset, 65536)
                out_f.write(chunk)
                offset += len(chunk)

        decode_cmd = ["ffmpeg", "-v", "error", "-i", dest_file, "-f", "null", "-"]
        res = subprocess.run(decode_cmd, capture_output=True, text=True)
        assert res.returncode == 0, f"Part {p.part_name} decode failed: {res.stderr}"


def test_plan_vsse_volumes_multiple_audio_tracks():
    """
    Verify volume planning when metadata contains multiple audio tracks:
    - Primary audio track is selected and planned.
    - Superfluous tracks are cleanly ignored per Gate 4.
    - All volumes synthesize valid FastStart headers.
    """
    v_track = create_synthetic_track(
        1, True, [1000] * 60, [10000 + i * 15000 for i in range(6)], 10, sync_samples=[1, 21, 41]
    )
    a_track1 = create_synthetic_track(2, False, [250] * 120, [20000 + i * 15000 for i in range(6)], 20)
    a_track2 = create_synthetic_track(3, False, [250] * 120, [25000 + i * 15000 for i in range(6)], 20)

    mvhd = Mvhd(0, 0, 0, 0, 1000, 60000, 0x10000, 0x100, (0x10000, 0, 0, 0, 0x10000, 0, 0, 0, 0x40000000), 4)
    meta = Mp4Metadata(mvhd=mvhd, tracks=[v_track, a_track1, a_track2], udta_bytes=None)

    plans = plan_vsse_volumes(
        meta,
        threshold_bytes=60_000,
        file_name="multi_audio.mp4",
        safety_margin_bytes=2_000,
    )
    assert len(plans) >= 2
    for p in plans:
        assert p.estimated_total_bytes <= 60_000
    for p in plans:
        hdr = synthesize_faststart_envelope_header(
            meta, DEFAULT_FTYP_BOX, p.v_start, p.v_end, p.a_start, p.a_end, p.o_min, p.o_max, p.payload_len
        )
        assert len(hdr) > 0


def test_plan_vsse_volumes_additional_validation():
    """Verify negative safety margin and empty filename validation."""
    v_track = create_synthetic_track(1, True)
    meta = create_synthetic_metadata(v_track)

    with pytest.raises(ValueError, match="safety_margin_bytes cannot be negative"):
        plan_vsse_volumes(meta, threshold_bytes=50_000, file_name="test.mp4", safety_margin_bytes=-100)

    with pytest.raises(ValueError, match="file_name cannot be empty"):
        plan_vsse_volumes(meta, threshold_bytes=50_000, file_name="   ")

    with pytest.raises(ValueError, match="file_name cannot be empty"):
        plan_vsse_volumes(meta, threshold_bytes=50_000, file_name="")


@pytest.mark.asyncio
async def test_virtual_envelope_provider_source_bounds_and_oversize():
    """
    Verify VirtualEnvelopeChunkProvider edge validations:
    - Envelope exceeding source total_bytes raises ValueError.
    - Empty header raises ValueError.
    - Whitespace file_id / file_name raises ValueError.
    - Extra bytes from source provider are safely clamped.
    """
    mock_source = MockByteChunkProvider(b"0123456789" * 10)  # 100 bytes

    # 1. Envelope exceeding source total_bytes
    with pytest.raises(ValueError, match="切片超出源范围"):
        VirtualEnvelopeChunkProvider(
            file_id="id1",
            file_name="f.mp4",
            faststart_header=b"HEADER",
            source_provider=mock_source,
            envelope_start=80,
            envelope_length=30,  # 80 + 30 = 110 > 100
        )

    # 2. Empty faststart header
    with pytest.raises(ValueError, match="faststart_header cannot be empty"):
        VirtualEnvelopeChunkProvider(
            file_id="id1",
            file_name="f.mp4",
            faststart_header=b"",
            source_provider=mock_source,
            envelope_start=0,
            envelope_length=10,
        )

    # 3. Whitespace file_id / file_name
    with pytest.raises(ValueError, match="file_id cannot be empty"):
        VirtualEnvelopeChunkProvider(
            file_id="  ",
            file_name="f.mp4",
            faststart_header=b"HEADER",
            source_provider=mock_source,
            envelope_start=0,
            envelope_length=10,
        )

    with pytest.raises(ValueError, match="file_name cannot be empty"):
        VirtualEnvelopeChunkProvider(
            file_id="id1",
            file_name="   ",
            faststart_header=b"HEADER",
            source_provider=mock_source,
            envelope_start=0,
            envelope_length=10,
        )

    # 4. Source provider returning oversized data is defensively clamped
    class OversizedChunkProvider(BaseChunkProvider):
        total_bytes = 1000

        async def read_chunk(self, offset: int, size: int) -> bytes:
            return b"X" * (size + 50)  # Returns 50 bytes too many

        async def aclose(self) -> None:
            pass

    oversized_src = OversizedChunkProvider()
    provider = VirtualEnvelopeChunkProvider(
        file_id="id_over",
        file_name="f.mp4",
        faststart_header=b"HEAD",  # 4 bytes
        source_provider=oversized_src,
        envelope_start=0,
        envelope_length=100,
    )
    # Case 2: Pure remote read
    res_case2 = await provider.read_chunk(10, 20)
    assert len(res_case2) == 20  # clamped to requested effective size!

    # Case 3: Boundary crossing read
    res_case3 = await provider.read_chunk(2, 20)  # 2B header + 18B remote
    assert len(res_case3) == 20  # clamped to requested effective size!


def test_plan_vsse_volumes_non_interleaved_circuit_breaker():
    """
    / Gate G-5: Verify that non-interleaved MP4 layouts (physical separation
    or excessive temporal offset skew) raise NonInterleavedMp4Error, triggering
    fallback to fMP4 universal channel.
    """
    # 1. Complete physical separation (all video before audio)
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[1000] * 100,
        chunk_offsets=[1000, 21000, 41000, 61000, 81000],
        samples_per_chunk=20,
        sync_samples=[1, 21, 41, 61, 81],
    )
    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[200] * 200,
        chunk_offsets=[200000, 210000, 220000, 230000],
        samples_per_chunk=50,
    )
    meta = create_synthetic_metadata(v_track, a_track)

    with pytest.raises(NonInterleavedMp4Error) as exc_info:
        plan_vsse_volumes(meta, threshold_bytes=50000, file_name="non_interleaved.mp4")
    assert "Non-interleaved MP4 detected" in str(exc_info.value)
    assert "precede audio chunks" in str(exc_info.value)

    # 2. Complete physical separation (all audio before video)
    v_track2 = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[1000] * 100,
        chunk_offsets=[300000, 320000, 340000, 360000, 380000],
        samples_per_chunk=20,
        sync_samples=[1, 21, 41, 61, 81],
    )
    a_track2 = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[200] * 200,
        chunk_offsets=[1000, 11000, 21000, 31000],
        samples_per_chunk=50,
    )
    meta2 = create_synthetic_metadata(v_track2, a_track2)

    with pytest.raises(NonInterleavedMp4Error) as exc_info2:
        plan_vsse_volumes(meta2, threshold_bytes=50000, file_name="non_interleaved_rev.mp4")
    assert "Non-interleaved MP4 detected" in str(exc_info2.value)
    assert "precede video chunks" in str(exc_info2.value)


def test_plan_vsse_volumes_sidecar_audio_single_volume():
    """Verify plan_vsse_volumes with sidecar_audio=True for single volume:
    - a_o_min, a_o_max, a_payload_len are correctly populated
    - o_min, o_max, payload_len strictly reflect remote video range
    - estimated_header_size includes both boxes and audio payload
    """
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[1000] * 30,
        chunk_offsets=[50000, 60000, 70000],
        samples_per_chunk=10,
        sync_samples=[1, 11, 21],
    )
    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[100] * 30,
        chunk_offsets=[0, 1000, 2000],
        samples_per_chunk=10,
    )
    meta = create_synthetic_metadata(v_track, a_track)

    plans = plan_vsse_volumes(meta, threshold_bytes=200000, file_name="single.mp4", sidecar_audio=True)
    assert len(plans) == 1
    p = plans[0]

    assert p.part_name == "single.mp4"
    # Video range: [50000, 50000 + 30 * 1000 = 80000) -> payload_len = 30000
    assert p.o_min == 50000
    assert p.o_max == 80000
    assert p.payload_len == 30000

    # Audio sidecar range: [0, 30 * 100 = 3000)
    assert p.a_o_min == 0
    assert p.a_o_max == 3000
    assert p.a_payload_len == 3000

    # Header size includes audio payload
    assert p.estimated_header_size >= 3000
    assert p.estimated_total_bytes == p.estimated_header_size + p.payload_len
    assert p.estimated_total_bytes <= 200000


def test_plan_vsse_volumes_sidecar_audio_bypasses_non_interleaved_circuit_breaker():
    """
    Phase 4 (WP-4): Verify that non-interleaved media layout (which fails with NonInterleavedMp4Error
    in Tier 1) successfully plans multiple volumes when sidecar_audio=True without triggering circuit breaker:
    1. Circuit breaker is bypassed because audio is detached to in-memory sidecar.
    2. Audio continuity across volumes: A_start^(k+1) == A_end^(k) + 1.
    3. Volume size constraint: estimated_total_bytes <= threshold_bytes.
    4. Dual-region offsets: o_min/o_max strictly video; a_o_min/a_o_max strictly sidecar.
    """
    # Physically separated layout (all video before audio)
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[1000] * 100,
        chunk_offsets=[1000, 21000, 41000, 61000, 81000],
        samples_per_chunk=20,
        sync_samples=[1, 21, 41, 61, 81],
    )
    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[200] * 200,
        chunk_offsets=[200000, 210000, 220000, 230000],
        samples_per_chunk=50,
    )
    meta = create_synthetic_metadata(v_track, a_track)

    # In standard Tier 1, this raises NonInterleavedMp4Error
    with pytest.raises(NonInterleavedMp4Error):
        plan_vsse_volumes(meta, threshold_bytes=50000, file_name="movie.mp4", sidecar_audio=False)

    # In Tier 2 sidecar_audio=True, this must succeed!
    plans = plan_vsse_volumes(
        meta,
        threshold_bytes=50000,
        file_name="movie.mp4",
        safety_margin_bytes=2000,
        sidecar_audio=True,
    )
    assert len(plans) > 1

    # Invariants verification
    for i, p in enumerate(plans):
        # 1. Size constraint
        assert p.estimated_total_bytes <= 50000, f"Volume {i + 1} size {p.estimated_total_bytes} > 50000"
        # 2. Remote envelope bounds strictly video
        assert p.o_min >= 1000
        assert p.o_max <= 101000
        assert p.payload_len == p.o_max - p.o_min
        # 3. Sidecar bounds populated
        assert p.a_o_min is not None
        assert p.a_o_max is not None
        assert p.a_payload_len == p.a_o_max - p.a_o_min
        assert p.estimated_header_size >= p.a_payload_len
        assert p.estimated_total_bytes == p.estimated_header_size + p.payload_len

        # 4. Audio continuity with next volume
        if i + 1 < len(plans):
            next_p = plans[i + 1]
            assert p.a_end is not None
            assert next_p.a_start is not None
            assert next_p.a_start == p.a_end + 1, (
                f"Audio continuity broken between part {i + 1} and {i + 2}: {p.a_end} -> {next_p.a_start}"
            )


def test_plan_vsse_volumes_sidecar_audio_ending_early_multi_volume():
    """
    Verify sidecar planning when audio finishes earlier than video:
    - Volume 1 contains audio: a_start=1, a_end populated, a_o_min/a_o_max populated.
    - Volume 2 transitions to video-only: a_start=None, a_end=None, a_o_min=None, a_o_max=None, a_payload_len=0.
    - bundle.slice_bytes(p.a_o_min, p.a_o_max) correctly yields non-empty slice for Vol 1, and b"" for Vol 2.
    - Both volumes synthesize valid FastStart headers without error or orphaned audio data.
    - Both volumes wrap into VirtualEnvelopeChunkProvider and match estimated sizes.
    """
    from app.core.iso_bmff.box_synthesizer import synthesize_faststart_envelope_header
    from app.core.vsse_audio_sidecar import SidecarAudioBundle

    ftyp_box = b"\x00\x00\x00\x20ftypisom\x00\x00\x02\x00isomiso2mp41mp42"

    # 40 video samples across 2 GOPs (20 samples each, 1000B per sample)
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[1000] * 40,
        chunk_offsets=[10000, 35000],
        samples_per_chunk=20,
        sync_samples=[1, 21],
        sample_delta=40,  # 25 fps
        timescale=1000,
    )
    # Audio track only has 10 samples (ends around 0.5s, video runs to 1.6s)
    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[100] * 10,
        chunk_offsets=[0],
        samples_per_chunk=10,
        sample_delta=50,
        timescale=1000,
    )
    meta = create_synthetic_metadata(v_track, a_track)
    bundle = SidecarAudioBundle(
        hybrid_metadata=meta,
        aac_payload=b"A" * 1000,
    )

    plans = plan_vsse_volumes(
        meta=bundle.hybrid_metadata,
        threshold_bytes=26000,
        file_name="sidecar_short_audio.mp4",
        safety_margin_bytes=2000,
        sidecar_audio=True,
    )

    assert len(plans) == 2
    p1 = plans[0]
    p2 = plans[1]

    # Part 1 has audio
    assert p1.v_start == 1
    assert p1.v_end == 20
    assert p1.a_start == 1
    assert p1.a_end == 10
    assert p1.a_o_min == 0
    assert p1.a_o_max == 1000
    assert p1.a_payload_len == 1000

    # Part 2 is video-only
    assert p2.v_start == 21
    assert p2.v_end == 40
    assert p2.a_start is None
    assert p2.a_end is None
    assert p2.a_o_min is None
    assert p2.a_o_max is None
    assert p2.a_payload_len == 0

    # Extract audio slice using bundle.slice_bytes
    slice_1 = bundle.slice_bytes(p1.a_o_min, p1.a_o_max)
    slice_2 = bundle.slice_bytes(p2.a_o_min, p2.a_o_max)
    assert len(slice_1) == 1000
    assert slice_2 == b""  # Must be empty!

    # Synthesize headers
    header_1 = synthesize_faststart_envelope_header(
        metadata=bundle.hybrid_metadata,
        ftyp_bytes=ftyp_box,
        v_start=p1.v_start,
        v_end=p1.v_end,
        a_start=p1.a_start,
        a_end=p1.a_end,
        o_min=p1.o_min,
        o_max=p1.o_max,
        payload_len=p1.payload_len,
        sidecar_audio_slice=slice_1,
        a_o_min=p1.a_o_min,
    )
    assert len(header_1) > 1000
    assert header_1.endswith(b"A" * 1000)

    header_2 = synthesize_faststart_envelope_header(
        metadata=bundle.hybrid_metadata,
        ftyp_bytes=ftyp_box,
        v_start=p2.v_start,
        v_end=p2.v_end,
        a_start=p2.a_start,
        a_end=p2.a_end,
        o_min=p2.o_min,
        o_max=p2.o_max,
        payload_len=p2.payload_len,
        sidecar_audio_slice=slice_2,
        a_o_min=p2.a_o_min,
    )
    assert len(header_2) > 0
    # Header 2 has no audio inlined
    assert not header_2.endswith(b"A")

    # Wrap both into VirtualEnvelopeChunkProvider
    mock_src = MockByteChunkProvider(b"V" * 60000)
    prov_1 = VirtualEnvelopeChunkProvider("p1", p1.part_name, header_1, mock_src, p1.o_min, p1.payload_len)
    prov_2 = VirtualEnvelopeChunkProvider("p2", p2.part_name, header_2, mock_src, p2.o_min, p2.payload_len)

    assert prov_1.total_bytes == len(header_1) + p1.payload_len
    assert prov_2.total_bytes == len(header_2) + p2.payload_len


def test_plan_vsse_volumes_backoff_single_gop_overshoot():
    """#16: 若单个 GOP 严格超过硬顶 threshold_bytes, 必须抛 UnsupportedMp4LayoutError 触发 fMP4 熔断。"""
    from app.core.iso_bmff.exceptions import UnsupportedMp4LayoutError

    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[1000] * 30,
        chunk_offsets=[1000, 11000, 21000],
        samples_per_chunk=10,
        sync_samples=[1, 11, 21],
    )
    meta = create_synthetic_metadata(v_track)

    # Single GOP is 10,000 bytes. With threshold = 9,000, even 1 GOP exceeds threshold_bytes.
    with pytest.raises(UnsupportedMp4LayoutError, match="exceeds hard threshold"):
        plan_vsse_volumes(
            meta=meta,
            threshold_bytes=9000,
            file_name="overshoot.mp4",
            safety_margin_bytes=500,
        )


def test_plan_vsse_volumes_oversized_remainder_no_keyframe_raises():
    """#16: 无后续关键帧且尾段超过硬顶 threshold_bytes 时, 必须抛 UnsupportedMp4LayoutError。"""
    from app.core.iso_bmff.exceptions import UnsupportedMp4LayoutError

    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[1000] * 30,
        chunk_offsets=[1000, 11000, 21000],
        samples_per_chunk=10,
        sync_samples=[1, 11],  # sample 11..30 为 20,000B 无后续关键帧的尾段
    )
    meta = create_synthetic_metadata(v_track)

    with pytest.raises(UnsupportedMp4LayoutError, match="exceeds hard threshold"):
        plan_vsse_volumes(
            meta=meta,
            threshold_bytes=15_000,
            file_name="tail_overshoot.mp4",
            safety_margin_bytes=500,
        )


def test_estimate_faststart_header_size_with_sgpd_sbgp():
    """Verify estimate_faststart_header_size accounts for sgpd and sbgp box sizes."""
    from app.core.vsse_provider import estimate_faststart_header_size

    v_track = create_synthetic_track(track_id=1, is_video=True)
    a_track = create_synthetic_track(track_id=2, is_video=False)
    meta = create_synthetic_metadata(v_track, a_track)

    base_est = estimate_faststart_header_size(meta, 1, 10, 1, 10)

    # Attach sgpd and sbgp raw boxes
    a_track.stbl.sgpd_boxes = [b"A" * 60]
    a_track.stbl.sbgp_boxes = [b"B" * 40]
    with_sgpd_est = estimate_faststart_header_size(meta, 1, 10, 1, 10)

    assert with_sgpd_est == base_est + 100


def test_plan_vsse_volumes_multi_volume_with_source_video_mv_and_delayed_audio():
    """
    Verify multi-volume planning and header synthesis when:
    1. Source video has a large initial mv offset (e.g. mv=1500 ticks) causing c_sec < 0.
    2. Audio has a leading Empty Edit delay causing early keyframes to precede audio start.
    Ensures all volumes plan with continuous audio indices and synthesize without Gate 5 errors.
    """
    v_offsets = [10000 + i * 15000 for i in range(6)]
    a_offsets = [20000 + i * 15000 for i in range(6)]
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[1000] * 60,
        chunk_offsets=v_offsets,
        samples_per_chunk=10,
        sample_delta=1000,
        timescale=1000,
        sync_samples=[1, 11, 21, 31, 41, 51],
    )
    # Source video has mv = 1500 (1.5s)
    v_track.edts = Edts(
        elst_version=0,
        elst_flags=0,
        entries=[ElstEntry(segment_duration=58500, media_time=1500, media_rate_integer=1, media_rate_fraction=0)],
    )

    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[250] * 120,
        chunk_offsets=a_offsets,
        samples_per_chunk=20,
        sample_delta=500,
        timescale=1000,
    )
    # Source audio has a 12.0s leading Empty Edit (so at keyframe 11 [10.0s], audio still hasn't started!)
    a_track.edts = Edts(
        elst_version=0,
        elst_flags=0,
        entries=[
            ElstEntry(segment_duration=12000, media_time=-1, media_rate_integer=1, media_rate_fraction=0),
            ElstEntry(segment_duration=60000, media_time=0, media_rate_integer=1, media_rate_fraction=0),
        ],
    )

    meta = create_synthetic_metadata(v_track, a_track)
    plans = plan_vsse_volumes(
        meta=meta,
        threshold_bytes=45_000,
        file_name="dual_tier_multi.mp4",
        safety_margin_bytes=3_000,
    )

    assert len(plans) >= 2
    # Verify video and audio sample continuity across all volumes
    assert plans[0].v_start == 1
    assert plans[-1].v_end == 60
    assert plans[0].a_start == 1
    assert plans[-1].a_end == 120
    for i in range(len(plans) - 1):
        assert plans[i].v_end + 1 == plans[i + 1].v_start
        assert plans[i].a_end is not None and plans[i + 1].a_start is not None
        assert plans[i].a_end + 1 == plans[i + 1].a_start

    # Synthesize every volume header and verify valid moov/elst without Gate 5 error
    for p in plans:
        hdr = synthesize_faststart_envelope_header(
            metadata=meta,
            ftyp_bytes=DEFAULT_FTYP_BOX,
            v_start=p.v_start,
            v_end=p.v_end,
            a_start=p.a_start,
            a_end=p.a_end,
            o_min=p.o_min,
            o_max=p.o_max,
            payload_len=p.payload_len,
        )
        assert len(hdr) > 0
        boxes = scan_top_level_boxes(hdr, len(hdr) + p.payload_len)
        moov_b = next(b for b in boxes if b.box_type == b"moov")
        parsed_m = parse_moov(hdr[moov_b.offset : moov_b.offset + moov_b.size])
        assert parsed_m.audio_track is not None and parsed_m.audio_track.edts is not None
        assert len(parsed_m.audio_track.edts.entries) in (1, 2)


def test_estimate_faststart_header_size_counts_raw_stsd_and_audio_tables():
    """Verify estimate_faststart_header_size accounts for stsd.raw_box and audio ctts/stss."""
    from app.core.vsse_provider import estimate_faststart_header_size

    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[500] * 10,
        chunk_offsets=[1000],
        samples_per_chunk=10,
        sync_samples=[1],
    )
    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[200] * 10,
        chunk_offsets=[6000],
        samples_per_chunk=10,
        sync_samples=[1, 5],
    )
    v_track.stbl.stsd.raw_box = b"x" * 2048
    a_track.stbl.stsd.raw_box = b"y" * 512
    a_track.stbl.ctts = [CttsEntry(sample_count=10, sample_offset=100)]
    meta = create_synthetic_metadata(v_track, a_track)

    est_with_extra = estimate_faststart_header_size(meta, v_start=1, v_end=10, a_start=1, a_end=10)

    a_track.stbl.ctts = None
    a_track.stbl.stss = None
    v_track.stbl.stsd.raw_box = b""
    a_track.stbl.stsd.raw_box = b""
    est_baseline = estimate_faststart_header_size(meta, v_start=1, v_end=10, a_start=1, a_end=10)

    assert est_with_extra > est_baseline + 2000


def test_plan_vsse_volumes_single_volume_honors_safety_margin():
    """Verify single-volume check compares against max_volume_bytes (threshold_bytes - safety_margin_bytes)."""
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[1000] * 20,
        chunk_offsets=[1000, 11000],
        samples_per_chunk=10,
        sync_samples=[1, 11],
    )
    meta = create_synthetic_metadata(v_track, None)

    # First check unmargined single_total, then set threshold_bytes just above it with safety_margin_bytes
    plans_fit = plan_vsse_volumes(meta, threshold_bytes=100_000, file_name="margin.mp4", safety_margin_bytes=0)
    assert len(plans_fit) == 1
    exact_single_total = plans_fit[0].estimated_total_bytes

    # If threshold_bytes >= exact_single_total, but threshold_bytes - safety_margin < exact_single_total, must split
    plans_split = plan_vsse_volumes(
        meta,
        threshold_bytes=exact_single_total + 500,
        file_name="margin.mp4",
        safety_margin_bytes=2000,
    )
    assert len(plans_split) == 2


def test_plan_vsse_volumes_without_stss_uses_lazy_keyframes(monkeypatch):
    """Verify missing stss with large UniformStszTable does not materialize list(range(...))."""
    from app.core.iso_bmff.box_parser import UniformStszTable

    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[100] * 2,
        chunk_offsets=[1000],
        samples_per_chunk=2,
        sync_samples=None,
    )
    v_track.stbl.stss = None
    # 500,000 samples uniform table (~2MB payload) with no stss
    v_track.stbl.stsz = UniformStszTable(uniform_size=4, sample_count=500_000)
    v_track.stbl.default_sample_size = 4
    v_track.stbl.stsc = [StscEntry(first_chunk=1, samples_per_chunk=500_000, sample_description_index=1)]
    v_track.stbl.stts = [SttsEntry(sample_count=500_000, sample_delta=1000)]
    meta = create_synthetic_metadata(v_track, None)

    import sys

    import app.core.vsse_provider as planner

    observed_keyframes = []
    original_profile = sys.getprofile()

    def capture_keyframes(frame, event, arg):
        if event == "return" and frame.f_code is plan_vsse_volumes.__code__:
            observed_keyframes.append(frame.f_locals["keyframes"])

    def guarded_list(values=()):
        assert not isinstance(values, range), "keyframes must not be eagerly materialized"
        return list(values)

    monkeypatch.setattr(planner, "list", guarded_list, raising=False)
    sys.setprofile(capture_keyframes)
    try:
        plans = plan_vsse_volumes(meta, threshold_bytes=10_000_000, file_name="lazy.mp4")
    finally:
        sys.setprofile(original_profile)
    assert len(observed_keyframes) == 1
    assert isinstance(observed_keyframes[0], range)
    assert observed_keyframes[0] == range(1, 500_001)
    assert len(plans) == 1
    assert plans[0].v_end == 500_000
