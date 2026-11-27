"""
Unit tests for ISO-BMFF Box Parser (Task 1).

Covers:
1. Memory benchmark: 1M samples parsing memory delta strictly < 4.6MB (tracemalloc).
2. Endianness correctness: Verifies Big-Endian conversion to native integers on Little-Endian.
3. Non-standard MP4 circuit breakers:
   - mvex box in moov -> raises FragmentedMp4Error
   - multiple mdat boxes (direct and via box span jump check) -> raises MultipleMdatError
   - stz2 box in stbl -> raises CompactSampleSizeError
4. Normal MP4 parsing: Video + audio tracks, timescales, durations, sample tables.
5. Real MP4 parsing generated via ffmpeg.
6. Error handling for malformed or truncated boxes.
"""

import array
import os
import struct
import subprocess
import time
import tracemalloc

import pytest

from app.core.iso_bmff import (
    CompactSampleSizeError,
    FragmentedMp4Error,
    IsoBmffError,
    IsoBmffParseError,
    MultipleMdatError,
    SbgpEntry,
    SbgpTable,
    UniformStszTable,
    UnsupportedCodecError,
    UnsupportedMp4LayoutError,
    VsseRoutingMode,
    classify_vsse_mode,
    iter_boxes,
    parse_moov,
    scan_top_level_boxes,
    validate_vsse_eligibility,
)


def make_box(box_type: bytes, payload: bytes) -> bytes:
    """Helper to build standard 8-byte header box."""
    return struct.pack(">I4s", len(payload) + 8, box_type) + payload


def make_full_box(box_type: bytes, version: int, flags: int, payload: bytes) -> bytes:
    """Helper to build FullBox with 12-byte header."""
    vf = (version << 24) | (flags & 0x00FFFFFF)
    return struct.pack(">I4sI", len(payload) + 12, box_type, vf) + payload


def build_synthetic_stbl(
    sample_sizes: list[int] | None = None,
    default_sample_size: int = 0,
    sample_count: int = 0,
    chunk_offsets: list[int] | None = None,
    samples_per_chunk: int | None = None,
    is_co64: bool = False,
    sync_samples: list[int] | None = None,
    is_video: bool = True,
    use_stz2: bool = False,
    codec: bytes | None = None,
) -> bytes:
    """Construct a synthetic stbl box with customizable tables."""
    # stsd
    if is_video:
        v_codec = codec or b"avc1"
        entry_payload = (
            struct.pack(">6sH", b"\x00" * 6, 1)  # reserved + data_ref_index
            + struct.pack(">HH", 0, 0)  # pre_defined, reserved
            + struct.pack(">III", 0, 0, 0)  # pre_defined
            + struct.pack(">HH", 1920, 1080)  # width, height
            + struct.pack(">II", 0x00480000, 0x00480000)  # dpi
            + struct.pack(">IH", 0, 1)  # reserved, frame_count
            + b"\x00" * 32  # compressor
            + struct.pack(">hh", 24, -1)  # depth, pre_defined
        )
        entry_box = struct.pack(">I4s", len(entry_payload) + 8, v_codec) + entry_payload
    else:
        a_codec = codec or b"mp4a"
        entry_payload = (
            struct.pack(">6sH", b"\x00" * 6, 1)  # reserved + data_ref_index
            + struct.pack(">8s", b"\x00" * 8)  # reserved
            + struct.pack(">HH", 2, 16)  # channels, samplesize
            + struct.pack(">HH", 0, 0)  # pre_defined, reserved
            + struct.pack(">I", 48000 << 16)  # sample_rate (16.16)
        )
        entry_box = struct.pack(">I4s", len(entry_payload) + 8, a_codec) + entry_payload

    stsd_payload = struct.pack(">I", 1) + entry_box  # entry_count = 1
    stsd_box = make_full_box(b"stsd", 0, 0, stsd_payload)

    # Determine sample count and sizes
    if default_sample_size > 0:
        num_samples = sample_count
        sizes = []
    elif sample_sizes is not None:
        num_samples = len(sample_sizes)
        sizes = sample_sizes
    elif sample_count > 0:
        num_samples = sample_count
        sizes = [1000] * sample_count
    else:
        num_samples = 1
        sizes = [1000]

    # stts (1 entry)
    stts_payload = struct.pack(">III", 1, num_samples, 1000)
    stts_box = make_full_box(b"stts", 0, 0, stts_payload)

    # chunk offsets (stco or co64)
    offsets = chunk_offsets or [10000]

    # stsc
    spc = (
        samples_per_chunk
        if samples_per_chunk is not None
        else (num_samples // len(offsets) if offsets else num_samples)
    )
    stsc_payload = struct.pack(">IIII", 1, 1, spc, 1)
    stsc_box = make_full_box(b"stsc", 0, 0, stsc_payload)

    # stsz or stz2
    if use_stz2:
        stsz_box = make_full_box(b"stz2", 0, 0, b"\x00" * 16)
    elif default_sample_size > 0:
        stsz_payload = struct.pack(">II", default_sample_size, sample_count)
        stsz_box = make_full_box(b"stsz", 0, 0, stsz_payload)
    else:
        stsz_payload = struct.pack(">II", 0, len(sizes)) + struct.pack(f">{len(sizes)}I", *sizes)
        stsz_box = make_full_box(b"stsz", 0, 0, stsz_payload)

    if is_co64:
        co_payload = struct.pack(">I", len(offsets)) + struct.pack(f">{len(offsets)}Q", *offsets)
        co_box = make_full_box(b"co64", 0, 0, co_payload)
    else:
        co_payload = struct.pack(">I", len(offsets)) + struct.pack(f">{len(offsets)}I", *offsets)
        co_box = make_full_box(b"stco", 0, 0, co_payload)

    boxes = [stsd_box, stts_box, stsc_box, stsz_box, co_box]

    if sync_samples is not None:
        stss_payload = struct.pack(">I", len(sync_samples)) + struct.pack(f">{len(sync_samples)}I", *sync_samples)
        stss_box = make_full_box(b"stss", 0, 0, stss_payload)
        boxes.append(stss_box)

    return make_box(b"stbl", b"".join(boxes))


def build_synthetic_trak(
    track_id: int = 1,
    is_video: bool = True,
    duration: int = 10000,
    timescale: int = 1000,
    stbl_bytes: bytes | None = None,
) -> bytes:
    """Build a complete trak box."""
    # tkhd
    w_raw = (1920 << 16) if is_video else 0
    h_raw = (1080 << 16) if is_video else 0
    tkhd_payload = (
        struct.pack(">IIII", 0, 0, track_id, 0)
        + struct.pack(">I", duration)
        + struct.pack(">8s", b"\x00" * 8)
        + struct.pack(">hhh", 0, 0, 0 if is_video else 0x0100)
        + struct.pack(">2s", b"\x00" * 2)
        + struct.pack(">9i", 0x00010000, 0, 0, 0, 0x00010000, 0, 0, 0, 0x40000000)
        + struct.pack(">II", w_raw, h_raw)
    )
    tkhd_box = make_full_box(b"tkhd", 0, 1, tkhd_payload)

    # mdia: mdhd + hdlr + minf
    mdhd_payload = struct.pack(">IIIIH2s", 0, 0, timescale, duration, 0x55C4, b"\x00\x00")
    mdhd_box = make_full_box(b"mdhd", 0, 0, mdhd_payload)

    handler_type = b"vide" if is_video else b"soun"
    hdlr_payload = struct.pack(">I4s12s", 0, handler_type, b"\x00" * 12) + b"TrackHandler\x00"
    hdlr_box = make_full_box(b"hdlr", 0, 0, hdlr_payload)

    stbl = stbl_bytes or build_synthetic_stbl(is_video=is_video)
    minf_box = make_box(b"minf", stbl)
    mdia_box = make_box(b"mdia", mdhd_box + hdlr_box + minf_box)

    return make_box(b"trak", tkhd_box + mdia_box)


def build_synthetic_moov(
    tracks: list[bytes] | None = None,
    duration: int = 10000,
    timescale: int = 1000,
    include_mvex: bool = False,
) -> bytes:
    """Build moov box with mvhd and tracks."""
    mvhd_payload = (
        struct.pack(">IIII", 0, 0, timescale, duration)
        + struct.pack(">IH", 0x00010000, 0x0100)
        + struct.pack(">10s", b"\x00" * 10)
        + struct.pack(">9i", 0x00010000, 0, 0, 0, 0x00010000, 0, 0, 0, 0x40000000)
        + struct.pack(">24s", b"\x00" * 24)
        + struct.pack(">I", 3)
    )
    mvhd_box = make_full_box(b"mvhd", 0, 0, mvhd_payload)

    boxes = [mvhd_box]
    if tracks:
        boxes.extend(tracks)
    else:
        boxes.append(build_synthetic_trak(1, is_video=True))

    if include_mvex:
        mvex_box = make_box(b"mvex", b"\x00" * 16)
        boxes.append(mvex_box)

    return make_box(b"moov", b"".join(boxes))


# -----------------------------------------------------------------------------
# Test Cases
# -----------------------------------------------------------------------------


def test_scan_top_level_boxes_normal():
    """Verify normal top-level box scanning: ftyp, moov, mdat, free."""
    ftyp = make_box(b"ftyp", b"isom\x00\x00\x02\x00isomiso2mp41")
    moov = build_synthetic_moov()
    mdat_payload = b"test_video_data_12345"
    mdat = make_box(b"mdat", mdat_payload)
    free = make_box(b"free", b"junk")

    file_bytes = ftyp + moov + mdat + free
    file_size = len(file_bytes)

    headers = scan_top_level_boxes(file_bytes, file_size)

    assert len(headers) == 4
    assert [h.box_type for h in headers] == [b"ftyp", b"moov", b"mdat", b"free"]
    assert headers[0].offset == 0
    assert headers[0].size == len(ftyp)
    assert headers[1].offset == len(ftyp)
    assert headers[2].box_type == b"mdat"
    assert headers[2].payload_offset == len(ftyp) + len(moov) + 8
    assert headers[2].payload_size == len(mdat_payload)


def test_scan_top_level_boxes_multi_mdat_circuit_breaker():
    """Verify that multiple mdat boxes in header trigger MultipleMdatError."""
    ftyp = make_box(b"ftyp", b"isom")
    mdat1 = make_box(b"mdat", b"sample1")
    mdat2 = make_box(b"mdat", b"sample2")

    file_bytes = ftyp + mdat1 + mdat2
    with pytest.raises(MultipleMdatError) as exc_info:
        scan_top_level_boxes(file_bytes, len(file_bytes))
    assert "Multiple mdat boxes detected" in str(exc_info.value)


def test_scan_top_level_boxes_box_span_jump_check_multi_mdat():
    """
    Verify Box Span Jump Check:
    When mdat_offset + mdat_size < file_size, probe_range_fn is invoked at boundary.
    If it detects a second mdat, MultipleMdatError is raised immediately.
    """
    ftyp = make_box(b"ftyp", b"isom")
    moov = build_synthetic_moov()
    # 1GB mdat box header
    mdat1_size = 1024 * 1024 * 1024
    mdat1_header = struct.pack(">I4s", mdat1_size, b"mdat")

    # Header buffer only has ftyp, moov, and the mdat1 header
    header_bytes = ftyp + moov + mdat1_header
    total_file_size = len(ftyp) + len(moov) + mdat1_size + 1024

    probed_requests: list[tuple[int, int]] = []

    def mock_probe(offset: int, length: int) -> bytes:
        probed_requests.append((offset, length))
        # Return a second mdat box header at the boundary
        return struct.pack(">I4s", 1024, b"mdat") + b"\x00" * 8

    with pytest.raises(MultipleMdatError) as exc_info:
        scan_top_level_boxes(header_bytes, total_file_size, probe_range_fn=mock_probe)

    assert len(probed_requests) == 1
    expected_offset = len(ftyp) + len(moov) + mdat1_size
    assert probed_requests[0][0] == expected_offset
    assert "Multiple mdat boxes detected" in str(exc_info.value)


def test_scan_top_level_boxes_box_span_jump_check_fragmented():
    """Verify Box Span Jump Check detects fragmented MP4 (moof) at boundary."""
    ftyp = make_box(b"ftyp", b"isom")
    moov = build_synthetic_moov()
    mdat1_size = 500000000
    mdat1_header = struct.pack(">I4s", mdat1_size, b"mdat")
    header_bytes = ftyp + moov + mdat1_header
    total_file_size = len(ftyp) + len(moov) + mdat1_size + 2048

    def mock_probe(offset: int, length: int) -> bytes:
        return struct.pack(">I4s", 2048, b"moof") + b"\x00" * 8

    with pytest.raises(FragmentedMp4Error) as exc_info:
        scan_top_level_boxes(header_bytes, total_file_size, probe_range_fn=mock_probe)

    assert "Fragmented MP4 top-level box 'moof'" in str(exc_info.value)


def test_parse_moov_circuit_breaker_mvex():
    """Verify that mvex inside moov raises FragmentedMp4Error."""
    moov_bytes = build_synthetic_moov(include_mvex=True)
    with pytest.raises(FragmentedMp4Error) as exc_info:
        parse_moov(moov_bytes)
    assert "Fragmented MP4 ('mvex' box present in moov)" in str(exc_info.value)


def test_parse_moov_circuit_breaker_stz2():
    """Verify that stz2 inside stbl raises CompactSampleSizeError."""
    stbl = build_synthetic_stbl(use_stz2=True)
    trak = build_synthetic_trak(1, is_video=True, stbl_bytes=stbl)
    moov = build_synthetic_moov(tracks=[trak])

    with pytest.raises(CompactSampleSizeError) as exc_info:
        parse_moov(moov)
    assert "Compact sample size box 'stz2'" in str(exc_info.value)


def test_parse_moov_endianness_correctness():
    """
    Verify Big-Endian conversion to native uint32 / uint64 on Little-Endian systems.
    stsz, stco, and stss must retain true integer values regardless of host architecture.
    """
    sample_sizes = [100, 2048, 65536, 1048576]
    chunk_offsets = [5000, 25000, 100000]
    sync_samples = [1, 3]

    stbl = build_synthetic_stbl(
        sample_sizes=sample_sizes,
        chunk_offsets=chunk_offsets,
        sync_samples=sync_samples,
        is_video=True,
    )
    trak = build_synthetic_trak(1, is_video=True, stbl_bytes=stbl)
    moov = build_synthetic_moov(tracks=[trak])

    metadata = parse_moov(moov)
    v_track = metadata.video_track
    assert v_track is not None

    # stsz
    assert isinstance(v_track.stbl.stsz, array.array)
    assert v_track.stbl.stsz.typecode == "I"
    assert list(v_track.stbl.stsz) == sample_sizes

    # chunk offsets (stco normalized to array('Q'))
    assert isinstance(v_track.stbl.chunk_offsets, array.array)
    assert v_track.stbl.chunk_offsets.typecode == "Q"
    assert list(v_track.stbl.chunk_offsets) == chunk_offsets

    # stss
    assert v_track.stbl.stss is not None
    assert isinstance(v_track.stbl.stss, array.array)
    assert v_track.stbl.stss.typecode == "I"
    assert list(v_track.stbl.stss) == sync_samples


def test_parse_moov_co64():
    """Verify 64-bit co64 chunk offset parsing into array('Q')."""
    chunk_offsets_64 = [0x100000000, 0x200000000, 0x300000000]  # > 4GB offsets
    stbl = build_synthetic_stbl(
        sample_sizes=[1000, 2000, 3000],
        chunk_offsets=chunk_offsets_64,
        is_co64=True,
        is_video=True,
    )
    trak = build_synthetic_trak(1, is_video=True, stbl_bytes=stbl)
    moov = build_synthetic_moov(tracks=[trak])

    metadata = parse_moov(moov)
    v_track = metadata.video_track
    assert v_track is not None
    assert v_track.stbl.is_co64 is True
    assert list(v_track.stbl.chunk_offsets) == chunk_offsets_64


def test_parse_moov_default_sample_size():
    """Verify default_sample_size > 0 generates array('I') of length sample_count."""
    stbl = build_synthetic_stbl(
        default_sample_size=1024,
        sample_count=50,
        chunk_offsets=[1000],
        is_video=False,
    )
    trak = build_synthetic_trak(2, is_video=False, stbl_bytes=stbl)
    moov = build_synthetic_moov(tracks=[trak])

    metadata = parse_moov(moov)
    a_track = metadata.audio_track
    assert a_track is not None
    assert a_track.stbl.default_sample_size == 1024
    assert len(a_track.stbl.stsz) == 50
    assert all(sz == 1024 for sz in a_track.stbl.stsz)


def test_parse_moov_ctts_and_edts():
    """Verify ctts (with signed offsets) and edts/elst parsing."""
    # Build stbl with ctts v1
    sample_sizes = [500, 600, 700]
    stbl_raw = build_synthetic_stbl(sample_sizes=sample_sizes, is_video=True)
    # Remove outer stbl header to inject ctts
    stbl_payload = stbl_raw[8:]
    ctts_payload = struct.pack(">I", 2) + struct.pack(">Ii", 1, -100) + struct.pack(">Ii", 2, 200)
    ctts_box = make_full_box(b"ctts", 1, 0, ctts_payload)
    new_stbl = make_box(b"stbl", stbl_payload + ctts_box)

    # Build trak with edts v0
    trak_raw = build_synthetic_trak(1, is_video=True, stbl_bytes=new_stbl)
    trak_payload = trak_raw[8:]
    elst_payload = struct.pack(">I", 1) + struct.pack(">Iihh", 1000, 50, 1, 0)
    elst_box = make_full_box(b"elst", 0, 0, elst_payload)
    edts_box = make_box(b"edts", elst_box)
    new_trak = make_box(b"trak", trak_payload + edts_box)

    moov = build_synthetic_moov(tracks=[new_trak])
    meta = parse_moov(moov)
    v_track = meta.video_track
    assert v_track is not None

    # ctts verification
    assert v_track.stbl.ctts is not None
    assert len(v_track.stbl.ctts) == 2
    assert v_track.stbl.ctts[0].sample_offset == -100
    assert v_track.stbl.ctts[1].sample_offset == 200

    # edts verification
    assert v_track.edts is not None
    assert len(v_track.edts.entries) == 1
    assert v_track.edts.entries[0].segment_duration == 1000
    assert v_track.edts.entries[0].media_time == 50


def test_parse_moov_1m_samples_memory_benchmark():
    """
    CRITICAL MEMORY BENCHMARK:
    1,000,000 samples array parsing peak memory delta RSS strictly < 4.6MB.
    Uses tracemalloc to measure exact allocation during parse_moov.
    """
    n_samples = 1_000_000
    # Pre-generate 1M sample sizes in big-endian bytes
    # To avoid benchmark skew, create the input buffer BEFORE starting tracemalloc
    sizes_raw = struct.pack(f">{n_samples}I", *([512] * n_samples))
    stsz_payload = struct.pack(">II", 0, n_samples) + sizes_raw
    stsz_box = make_full_box(b"stsz", 0, 0, stsz_payload)

    # Build other tiny tables
    stsd_payload = struct.pack(">I", 1) + make_box(b"avc1", b"\x00" * 78)
    stsd_box = make_full_box(b"stsd", 0, 0, stsd_payload)
    stts_box = make_full_box(b"stts", 0, 0, struct.pack(">III", 1, n_samples, 1000))
    stsc_box = make_full_box(b"stsc", 0, 0, struct.pack(">IIII", 1, 1, n_samples, 1))
    stco_box = make_full_box(b"stco", 0, 0, struct.pack(">II", 1, 10000))

    stbl = make_box(b"stbl", stsd_box + stts_box + stsc_box + stsz_box + stco_box)
    trak = build_synthetic_trak(1, is_video=True, stbl_bytes=stbl)
    moov_bytes = build_synthetic_moov(tracks=[trak])

    # Warm up lazy module/regex/coverage caches on a tiny moov before tracemalloc
    warm_stsz = make_full_box(b"stsz", 0, 0, struct.pack(">II", 512, 10))
    warm_stts = make_full_box(b"stts", 0, 0, struct.pack(">III", 1, 10, 1000))
    warm_stsc = make_full_box(b"stsc", 0, 0, struct.pack(">IIII", 1, 10, 1, 1))
    warm_stbl = make_box(b"stbl", stsd_box + warm_stts + warm_stsc + warm_stsz + stco_box)
    parse_moov(build_synthetic_moov(tracks=[build_synthetic_trak(1, is_video=True, stbl_bytes=warm_stbl)]))

    # Ensure garbage collection before benchmark
    import gc

    gc.collect()

    tracemalloc.start()
    metadata = parse_moov(moov_bytes)
    current_mem, peak_mem = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    peak_mb = peak_mem / (1024 * 1024)
    current_mb = current_mem / (1024 * 1024)

    print(f"\n1M Samples Benchmark: current={current_mb:.3f} MB, peak={peak_mb:.3f} MB")

    # Assertions
    assert len(metadata.video_track.stbl.stsz) == n_samples
    assert peak_mb < 4.6, f"Peak memory {peak_mb:.3f} MB exceeds hard ceiling 4.6 MB"
    assert current_mb < 4.6, f"Retained memory {current_mb:.3f} MB exceeds hard ceiling 4.6 MB"


def test_real_mp4_parsing_with_ffmpeg(tmp_path):
    """
    Golden Integration Test:
    Generate a real MP4 file with ffmpeg (FastStart H.264 + AAC) and parse it.
    Verify all metadata against ffprobe.
    """
    mp4_file = str(tmp_path / "test_standard.mp4")
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
        "2.0",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-movflags",
        "+faststart",
        mp4_file,
    ]
    subprocess.run(cmd, check=True)
    assert os.path.exists(mp4_file)

    with open(mp4_file, "rb") as f:
        file_bytes = f.read()

    file_size = len(file_bytes)
    top_boxes = scan_top_level_boxes(file_bytes, file_size)
    assert len(top_boxes) >= 2
    types = [b.box_type for b in top_boxes]
    assert b"ftyp" in types
    assert b"moov" in types
    assert b"mdat" in types

    # Locate moov box
    moov_header = next(b for b in top_boxes if b.box_type == b"moov")
    moov_bytes = file_bytes[moov_header.offset : moov_header.offset + moov_header.size]

    meta = parse_moov(moov_bytes)

    # Verify movie properties
    assert meta.movie_timescale > 0
    assert 1.9 <= meta.duration_s <= 2.1
    assert len(meta.tracks) == 2

    # Video track
    v_track = meta.video_track
    assert v_track is not None
    assert v_track.is_video is True
    assert v_track.tkhd.width == 320
    assert v_track.tkhd.height == 240
    assert len(v_track.stbl.stsz) == 50  # 25 fps * 2.0s = 50 frames
    assert v_track.stbl.stss is not None
    assert len(v_track.stbl.stss) >= 1  # at least 1 keyframe
    assert v_track.stbl.stss[0] == 1  # first sample is keyframe (1-based)

    # Audio track
    a_track = meta.audio_track
    assert a_track is not None
    assert a_track.is_audio is True
    assert a_track.mdhd.timescale == 48000
    assert len(a_track.stbl.stsz) > 0


def test_malformed_and_truncated_box_handling():
    """Verify robust error raising for corrupted or truncated box buffers."""
    # Buffer smaller than 8 bytes
    with pytest.raises(IsoBmffParseError):
        parse_moov(b"\x00\x00\x00")

    # Invalid box size (< 8)
    with pytest.raises(IsoBmffParseError):
        parse_moov(struct.pack(">I4s", 4, b"moov"))

    # Box size exceeding container
    corrupted_moov = struct.pack(">I4s", 100, b"moov") + b"\x00" * 10
    with pytest.raises(IsoBmffParseError):
        parse_moov(corrupted_moov)

    # Moov without mvhd
    moov_no_mvhd = make_box(b"moov", make_box(b"free", b"1234"))
    with pytest.raises(IsoBmffParseError) as exc_info:
        parse_moov(moov_no_mvhd)
    assert "Missing 'mvhd' box" in str(exc_info.value)


def test_exception_hierarchy():
    """Verify ISO-BMFF exception inheritance model."""
    assert issubclass(IsoBmffParseError, IsoBmffError)
    assert issubclass(UnsupportedMp4LayoutError, IsoBmffError)
    assert issubclass(FragmentedMp4Error, UnsupportedMp4LayoutError)
    assert issubclass(MultipleMdatError, UnsupportedMp4LayoutError)
    assert issubclass(CompactSampleSizeError, UnsupportedMp4LayoutError)
    from app.core.iso_bmff.exceptions import UnsupportedCodecError

    assert issubclass(UnsupportedCodecError, UnsupportedMp4LayoutError)


def test_scan_top_level_boxes_64bit():
    """Verify 64-bit box header parsing (size == 1)."""
    ftyp = make_box(b"ftyp", b"isom")
    # 64-bit mdat header: size=1, type=b"mdat", largesize=1000
    mdat_hdr = struct.pack(">I4sQ", 1, b"mdat", 1000)
    mdat_body = b"\x00" * (1000 - 16)
    file_bytes = ftyp + mdat_hdr + mdat_body
    boxes = scan_top_level_boxes(file_bytes, len(file_bytes))
    assert len(boxes) == 2
    assert boxes[1].box_type == b"mdat"
    assert boxes[1].size == 1000
    assert boxes[1].header_size == 16
    assert boxes[1].payload_offset == len(ftyp) + 16
    assert boxes[1].payload_size == 1000 - 16


def test_scan_top_level_boxes_size_zero_eof():
    """Verify box size 0 extends to end of file."""
    ftyp = make_box(b"ftyp", b"isom")
    mdat_hdr = struct.pack(">I4s", 0, b"mdat")
    file_bytes = ftyp + mdat_hdr + b"data12345"
    boxes = scan_top_level_boxes(file_bytes, len(file_bytes))
    assert len(boxes) == 2
    assert boxes[1].box_type == b"mdat"
    assert boxes[1].size == len(file_bytes) - len(ftyp)


def test_scan_top_level_boxes_exact_span_no_probe():
    """When mdat extends exactly to file_size, probe function is not called."""
    ftyp = make_box(b"ftyp", b"isom")
    mdat = make_box(b"mdat", b"complete")
    file_bytes = ftyp + mdat
    probed = False

    def probe(off, ln):
        nonlocal probed
        probed = True
        return b""

    boxes = scan_top_level_boxes(file_bytes, len(file_bytes), probe_range_fn=probe)
    assert len(boxes) == 2
    assert probed is False


def test_parse_moov_without_stss():
    """Verify tracks without stss box (e.g. all-intra video or audio) yield stss=None."""
    stbl = build_synthetic_stbl(sync_samples=None, is_video=False)
    trak = build_synthetic_trak(1, is_video=False, stbl_bytes=stbl)
    moov = build_synthetic_moov(tracks=[trak])
    meta = parse_moov(moov)
    assert meta.tracks[0].stbl.stss is None


def test_parse_moov_edts_v1():
    """Verify edts/elst version 1 with 64-bit segment duration and signed int64 media_time."""
    stbl = build_synthetic_stbl(is_video=True)
    trak_raw = build_synthetic_trak(1, is_video=True, stbl_bytes=stbl)
    trak_payload = trak_raw[8:]
    # elst v1: version=1, entry_count=1, seg_dur=100000 (uint64), media_time=-1 (int64 empty edit), rate=1.0
    elst_payload = struct.pack(">I", 1) + struct.pack(">Qqhh", 100000, -1, 1, 0)
    elst_box = make_full_box(b"elst", 1, 0, elst_payload)
    edts_box = make_box(b"edts", elst_box)
    trak = make_box(b"trak", trak_payload + edts_box)
    moov = build_synthetic_moov(tracks=[trak])

    meta = parse_moov(moov)
    v_track = meta.video_track
    assert v_track is not None
    assert v_track.edts is not None
    assert v_track.edts.elst_version == 1
    assert len(v_track.edts.entries) == 1
    assert v_track.edts.entries[0].segment_duration == 100000
    assert v_track.edts.entries[0].media_time == -1


def test_validate_vsse_eligibility_supported():
    """Verify validate_vsse_eligibility passes for standard AVC1 + AAC."""
    v_stbl = build_synthetic_stbl(is_video=True)
    v_trak = build_synthetic_trak(1, is_video=True, stbl_bytes=v_stbl)
    a_stbl = build_synthetic_stbl(is_video=False)
    a_trak = build_synthetic_trak(2, is_video=False, stbl_bytes=a_stbl)
    moov = build_synthetic_moov(tracks=[v_trak, a_trak])
    meta = parse_moov(moov)
    validate_vsse_eligibility(meta)  # Should not raise


def test_validate_vsse_eligibility_unsupported_video_codec():
    """Verify validate_vsse_eligibility raises UnsupportedCodecError on non-standard video (e.g. VP8)."""
    entry_payload = struct.pack(">6sH", b"\x00" * 6, 1) + b"\x00" * 70
    entry_box = make_box(b"vp08", entry_payload)
    stsd_box = make_full_box(b"stsd", 0, 0, struct.pack(">I", 1) + entry_box)
    stts_box = make_full_box(b"stts", 0, 0, struct.pack(">III", 1, 1, 1000))
    stsc_box = make_full_box(b"stsc", 0, 0, struct.pack(">IIII", 1, 1, 1, 1))
    stsz_box = make_full_box(b"stsz", 0, 0, struct.pack(">III", 0, 1, 1000))
    stco_box = make_full_box(b"stco", 0, 0, struct.pack(">II", 1, 10000))
    stbl = make_box(b"stbl", stsd_box + stts_box + stsc_box + stsz_box + stco_box)
    v_trak = build_synthetic_trak(1, is_video=True, stbl_bytes=stbl)
    moov = build_synthetic_moov(tracks=[v_trak])
    meta = parse_moov(moov)

    with pytest.raises(UnsupportedCodecError) as exc_info:
        validate_vsse_eligibility(meta)
    assert "Video codec 'vp08' (b'vp08') is not supported in VSSE" in str(exc_info.value)


def test_validate_vsse_eligibility_unsupported_audio_codec():
    """Verify validate_vsse_eligibility raises UnsupportedCodecError on non-AAC audio (e.g. MP3)."""
    v_stbl = build_synthetic_stbl(is_video=True)
    v_trak = build_synthetic_trak(1, is_video=True, stbl_bytes=v_stbl)

    entry_payload = struct.pack(">6sH", b"\x00" * 6, 1) + b"\x00" * 28
    entry_box = make_box(b".mp3", entry_payload)
    stsd_box = make_full_box(b"stsd", 0, 0, struct.pack(">I", 1) + entry_box)
    stts_box = make_full_box(b"stts", 0, 0, struct.pack(">III", 1, 1, 1000))
    stsc_box = make_full_box(b"stsc", 0, 0, struct.pack(">IIII", 1, 1, 1, 1))
    stsz_box = make_full_box(b"stsz", 0, 0, struct.pack(">III", 0, 1, 1000))
    stco_box = make_full_box(b"stco", 0, 0, struct.pack(">II", 1, 10000))
    a_stbl = make_box(b"stbl", stsd_box + stts_box + stsc_box + stsz_box + stco_box)
    a_trak = build_synthetic_trak(2, is_video=False, stbl_bytes=a_stbl)

    moov = build_synthetic_moov(tracks=[v_trak, a_trak])
    meta = parse_moov(moov)

    with pytest.raises(UnsupportedCodecError) as exc_info:
        validate_vsse_eligibility(meta)
    assert "Audio codec '.mp3' (b'.mp3') is not supported in VSSE" in str(exc_info.value)


def test_validate_vsse_eligibility_missing_video_track():
    """Verify validate_vsse_eligibility raises UnsupportedCodecError when no video track exists."""
    a_stbl = build_synthetic_stbl(is_video=False)
    a_trak = build_synthetic_trak(2, is_video=False, stbl_bytes=a_stbl)
    moov = build_synthetic_moov(tracks=[a_trak])
    meta = parse_moov(moov)

    with pytest.raises(UnsupportedCodecError) as exc_info:
        validate_vsse_eligibility(meta)
    assert "No video track found" in str(exc_info.value)


def test_parse_moov_audio_sample_entry_v2():
    """Verify AudioSampleEntry version 2 (64-bit float sample rate, 32-bit channel count)."""
    entry_payload = (
        struct.pack(">6sH", b"\x00" * 6, 1)
        + struct.pack(">H", 2)
        + b"\x00" * 6
        + struct.pack(">HH", 3, 16)
        + b"\x00" * 4
        + struct.pack(">I", 0x00010000)
        + struct.pack(">I", 72)
        + struct.pack(">d", 96000.0)
        + struct.pack(">I", 6)
    )
    entry_box = make_box(b"mp4a", entry_payload)
    stsd_box = make_full_box(b"stsd", 0, 0, struct.pack(">I", 1) + entry_box)
    stts_box = make_full_box(b"stts", 0, 0, struct.pack(">III", 1, 1, 1000))
    stsc_box = make_full_box(b"stsc", 0, 0, struct.pack(">IIII", 1, 1, 1, 1))
    stsz_box = make_full_box(b"stsz", 0, 0, struct.pack(">III", 0, 1, 1000))
    stco_box = make_full_box(b"stco", 0, 0, struct.pack(">II", 1, 10000))
    a_stbl = make_box(b"stbl", stsd_box + stts_box + stsc_box + stsz_box + stco_box)
    a_trak = build_synthetic_trak(2, is_video=False, stbl_bytes=a_stbl)
    moov = build_synthetic_moov(tracks=[a_trak])

    meta = parse_moov(moov)
    a_track = meta.audio_track
    assert a_track is not None
    entry = a_track.stbl.stsd.entries[0]
    assert entry.channel_count == 6
    assert entry.sample_rate == 96000


def test_scan_top_level_boxes_trailing_garbage():
    """Verify that trailing unparsed bytes in an MP4 file raise IsoBmffParseError."""
    ftyp = make_box(b"ftyp", b"isom")
    mdat = make_box(b"mdat", b"data")
    file_bytes = ftyp + mdat + b"\x01\x02\x03"

    with pytest.raises(IsoBmffParseError) as exc_info:
        scan_top_level_boxes(file_bytes, len(file_bytes))
    assert "Truncated box header" in str(exc_info.value)


def test_scan_top_level_boxes_multiple_moov():
    """Verify that multiple moov boxes at top level trigger IsoBmffParseError."""
    ftyp = make_box(b"ftyp", b"isom")
    moov1 = build_synthetic_moov()
    moov2 = build_synthetic_moov()
    file_bytes = ftyp + moov1 + moov2

    with pytest.raises(IsoBmffParseError) as exc_info:
        scan_top_level_boxes(file_bytes, len(file_bytes))
    assert "Multiple moov boxes detected" in str(exc_info.value)


def test_iter_boxes_trailing_garbage():
    """Verify that container box with trailing leftover bytes (< 8) raises IsoBmffParseError."""
    valid_box = make_box(b"free", b"test")
    corrupted_container = valid_box + b"\xaa\xbb\xcc"

    with pytest.raises(IsoBmffParseError) as exc_info:
        list(iter_boxes(memoryview(corrupted_container)))
    assert "trailing unparsed bytes" in str(exc_info.value)


def test_parse_moov_truncated_outer_box():
    """Verify parse_moov raises IsoBmffParseError when outer moov size claims more bytes than provided."""
    moov_hdr = struct.pack(">I4s", 50000, b"moov")
    incomplete_buffer = moov_hdr + b"\x00" * 100

    with pytest.raises(IsoBmffParseError) as exc_info:
        parse_moov(incomplete_buffer)
    assert "Truncated moov box" in str(exc_info.value)


def test_stbl_excessive_sample_count_guard():
    """Verify safety guard against malicious/corrupted stsz declaring excessive sample count."""
    stsz_payload = struct.pack(">II", 0, 200_000_000)
    stsz_box = make_full_box(b"stsz", 0, 0, stsz_payload)
    stsd_box = make_full_box(b"stsd", 0, 0, struct.pack(">I", 1) + make_box(b"avc1", b"\x00" * 78))
    stts_box = make_full_box(b"stts", 0, 0, struct.pack(">III", 1, 1, 1000))
    stsc_box = make_full_box(b"stsc", 0, 0, struct.pack(">IIII", 1, 1, 1, 1))
    stco_box = make_full_box(b"stco", 0, 0, struct.pack(">II", 1, 10000))
    stbl = make_box(b"stbl", stsd_box + stts_box + stsc_box + stsz_box + stco_box)
    trak = build_synthetic_trak(1, is_video=True, stbl_bytes=stbl)
    moov = build_synthetic_moov(tracks=[trak])

    with pytest.raises(IsoBmffParseError) as exc_info:
        parse_moov(moov)
    assert "Excessive stsz sample_count" in str(exc_info.value)


def test_stsc_invalid_sample_description_index():
    """Verify stsc with sample_description_index out of range raises IsoBmffParseError."""
    stsd_box = make_full_box(b"stsd", 0, 0, struct.pack(">I", 1) + make_box(b"avc1", b"\x00" * 78))
    stts_box = make_full_box(b"stts", 0, 0, struct.pack(">III", 1, 1, 1000))
    stsc_box = make_full_box(b"stsc", 0, 0, struct.pack(">IIII", 1, 1, 1, 5))
    stsz_box = make_full_box(b"stsz", 0, 0, struct.pack(">III", 0, 1, 1000))
    stco_box = make_full_box(b"stco", 0, 0, struct.pack(">II", 1, 10000))
    stbl = make_box(b"stbl", stsd_box + stts_box + stsc_box + stsz_box + stco_box)
    trak = build_synthetic_trak(1, is_video=True, stbl_bytes=stbl)
    moov = build_synthetic_moov(tracks=[trak])

    with pytest.raises(IsoBmffParseError) as exc_info:
        parse_moov(moov)
    assert "stsc sample_description_index 5 out of range" in str(exc_info.value)


def test_stsd_multiple_entries():
    """Verify that multiple sample descriptions in stsd (entry_count > 1) are parsed."""
    entry1_payload = (
        struct.pack(">6sH", b"\x00" * 6, 1)
        + struct.pack(">HH", 0, 0)
        + struct.pack(">III", 0, 0, 0)
        + struct.pack(">HH", 1920, 1080)
        + struct.pack(">II", 0x00480000, 0x00480000)
        + struct.pack(">IH", 0, 1)
        + b"\x00" * 32
        + struct.pack(">hh", 24, -1)
    )
    entry1 = make_box(b"avc1", entry1_payload)
    entry2 = make_box(b"avc3", entry1_payload)
    stsd_payload = struct.pack(">I", 2) + entry1 + entry2
    stsd_box = make_full_box(b"stsd", 0, 0, stsd_payload)

    stts_box = make_full_box(b"stts", 0, 0, struct.pack(">III", 1, 2, 1000))
    stsc_box = make_full_box(b"stsc", 0, 0, struct.pack(">IIII", 1, 1, 2, 2))
    stsz_box = make_full_box(b"stsz", 0, 0, struct.pack(">IIII", 0, 2, 1000, 2000))
    stco_box = make_full_box(b"stco", 0, 0, struct.pack(">II", 1, 10000))
    stbl = make_box(b"stbl", stsd_box + stts_box + stsc_box + stsz_box + stco_box)
    trak = build_synthetic_trak(1, is_video=True, stbl_bytes=stbl)
    moov = build_synthetic_moov(tracks=[trak])

    meta = parse_moov(moov)
    v_track = meta.video_track
    assert v_track is not None
    assert v_track.stbl.stsd.entry_count == 2
    assert len(v_track.stbl.stsd.entries) == 2
    assert v_track.stbl.stsd.entries[0].codec == "avc1"
    assert v_track.stbl.stsd.entries[1].codec == "avc3"


def test_parse_moov_out_of_order_mdia():
    """Verify that mdia container with minf placed before hdlr still parses correctly."""
    stbl = build_synthetic_stbl(is_video=True)
    minf_box = make_box(b"minf", stbl)
    mdhd_box = make_full_box(b"mdhd", 0, 0, struct.pack(">IIIIH2s", 0, 0, 1000, 5000, 0x55C4, b"\x00\x00"))
    hdlr_box = make_full_box(b"hdlr", 0, 0, struct.pack(">I4s12s", 0, b"vide", b"\x00" * 12) + b"VideoHandler\x00")

    # mdia with minf before hdlr: mdhd + minf + hdlr
    mdia_box = make_box(b"mdia", mdhd_box + minf_box + hdlr_box)

    w_raw = 1280 << 16
    h_raw = 720 << 16
    tkhd_payload = (
        struct.pack(">IIII", 0, 0, 1, 0)
        + struct.pack(">I", 5000)
        + struct.pack(">8s", b"\x00" * 8)
        + struct.pack(">hhh", 0, 0, 0)
        + struct.pack(">2s", b"\x00" * 2)
        + struct.pack(">9i", 0x00010000, 0, 0, 0, 0x00010000, 0, 0, 0, 0x40000000)
        + struct.pack(">II", w_raw, h_raw)
    )
    tkhd_box = make_full_box(b"tkhd", 0, 1, tkhd_payload)
    trak_box = make_box(b"trak", tkhd_box + mdia_box)
    moov = build_synthetic_moov(tracks=[trak_box])

    meta = parse_moov(moov)
    v_track = meta.video_track
    assert v_track is not None
    assert v_track.is_video is True
    assert v_track.stbl.stsd.entries[0].width == 1920
    assert v_track.stbl.stsd.entries[0].height == 1080


def test_track_convenience_properties():
    """Verify Track convenience properties: duration_s, codec, and sample_entry."""
    stbl = build_synthetic_stbl(is_video=True)
    trak = build_synthetic_trak(1, is_video=True, duration=5000, timescale=1000, stbl_bytes=stbl)
    moov = build_synthetic_moov(tracks=[trak])

    meta = parse_moov(moov)
    v_track = meta.video_track
    assert v_track is not None
    assert v_track.duration_s == 5.0
    assert v_track.sample_entry is not None
    assert v_track.sample_entry.width == 1920


def test_compact_ctts_memory_and_speed_3h_60fps():
    """
    Benchmark 3h 60fps video (648,000 ctts entries).
    Verifies compact arrays keep peak heap < 15MB in CI and guards against O(N^2) latency
    regression (strict < 100ms SLA is verified in scripts/benchmarks/bench_iso_bmff.py).
    """
    import gc

    from app.core.iso_bmff.box_synthesizer import serialize_ctts

    n = 648_000
    # Alternating positive and negative composition offsets (v1 ctts)
    ctts_raw_payload = struct.pack(">I", n) + (struct.pack(">Ii", 1, 1024) + struct.pack(">Ii", 1, -512)) * (n // 2)
    ctts_box = make_full_box(b"ctts", 1, 0, ctts_raw_payload)

    # Minimal stbl boxes
    entry_payload = (
        struct.pack(">6sH", b"\x00" * 6, 1)
        + struct.pack(">HH", 0, 0)
        + struct.pack(">III", 0, 0, 0)
        + struct.pack(">HH", 1920, 1080)
        + struct.pack(">II", 0x00480000, 0x00480000)
        + struct.pack(">IH", 0, 1)
        + b"\x00" * 32
        + struct.pack(">hh", 24, -1)
    )
    entry_box = struct.pack(">I4s", len(entry_payload) + 8, b"avc1") + entry_payload
    stsd_box = make_full_box(b"stsd", 0, 0, struct.pack(">I", 1) + entry_box)

    stts_box = make_full_box(b"stts", 0, 0, struct.pack(">III", 1, n, 1000))
    stsc_box = make_full_box(b"stsc", 0, 0, struct.pack(">IIII", 1, 10, 1, 1))
    stsz_box = make_full_box(b"stsz", 0, 0, struct.pack(">II", 1000, n))
    co_box = make_full_box(b"stco", 0, 0, struct.pack(">II", 1, 10000))

    stbl = make_box(b"stbl", stsd_box + stts_box + ctts_box + stsc_box + stsz_box + co_box)
    trak = build_synthetic_trak(1, is_video=True, stbl_bytes=stbl)
    moov_bytes = build_synthetic_moov(tracks=[trak])

    # Phase 1: Deterministic memory & compact layout invariants (warmup first so
    # coverage/lazy caches don't skew tracemalloc peak on a cold xdist worker)
    parse_moov(moov_bytes)
    gc.collect()
    tracemalloc.start()
    try:
        meta = parse_moov(moov_bytes)
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    v_track = meta.video_track
    assert v_track is not None
    assert v_track.stbl.ctts is not None
    assert len(v_track.stbl.ctts) == n
    assert v_track.stbl.ctts[0].sample_offset == 1024
    assert v_track.stbl.ctts[1].sample_offset == -512

    peak_mb = peak_bytes / (1024 * 1024)
    assert peak_mb < 15.0, f"Peak memory violated threshold: {peak_mb:.2f}MB >= 15MB"

    # Phase 2: Decoupled anti-O(N^2) latency guard (tracemalloc stopped; warmup + min of 3 runs)
    parse_moov(moov_bytes)
    samples_ms: list[float] = []
    for _ in range(3):
        t0 = time.perf_counter()
        parse_moov(moov_bytes)
        samples_ms.append((time.perf_counter() - t0) * 1000)
    min_elapsed_ms = min(samples_ms)
    assert min_elapsed_ms < 500.0, f"Parse latency violated CI regression ceiling: {min_elapsed_ms:.2f}ms >= 500ms"

    # Verify serialization roundtrip
    serialized_ctts = serialize_ctts(v_track.stbl.ctts)
    assert len(serialized_ctts) == len(ctts_box)


def test_uniform_stsz_table_unit_operations():
    """Verify UniformStszTable basic operations, sequence protocol, and bounds checking."""
    tbl = UniformStszTable(uniform_size=4, sample_count=100)
    assert len(tbl) == 100
    assert tbl[0] == 4
    assert tbl[-1] == 4
    assert tbl[50] == 4

    # Slices
    sliced = tbl[10:30]
    assert isinstance(sliced, UniformStszTable)
    assert len(sliced) == 20
    assert sliced.uniform_size == 4
    assert sliced.sample_count == 20

    # Step slicing
    stepped = tbl[0:10:2]
    assert isinstance(stepped, UniformStszTable)
    assert len(stepped) == 5

    # Empty slice
    empty = tbl[30:10]
    assert len(empty) == 0

    # Bounds
    with pytest.raises(IndexError):
        _ = tbl[100]
    with pytest.raises(IndexError):
        _ = tbl[-101]
    with pytest.raises(TypeError):
        _ = tbl["invalid"]

    # Membership and counting
    assert 4 in tbl
    assert 5 not in tbl
    assert tbl.count(4) == 100
    assert tbl.count(5) == 0
    assert tbl.index(4) == 0
    with pytest.raises(ValueError):
        tbl.index(5)

    # Comparisons and representations
    assert repr(tbl) == "UniformStszTable(uniform_size=4, sample_count=100)"
    assert tbl == UniformStszTable(4, 100)
    assert tbl != UniformStszTable(4, 99)
    assert tbl != UniformStszTable(8, 100)
    assert tbl == [4] * 100
    assert tbl != [4] * 99
    assert tbl.to_list() == [4] * 100

    # Type safety & transitivity
    assert tbl != "4" * 100
    assert tbl != b"\x04" * 100
    empty1 = UniformStszTable(4, 0)
    empty2 = UniformStszTable(8, 0)
    assert empty1 == empty2
    assert empty1 == []
    assert empty1 != ""
    assert empty1 != b""

    # Invariants
    with pytest.raises(ValueError):
        UniformStszTable(-1, 100)
    with pytest.raises(ValueError):
        UniformStszTable(4, -1)


def test_uniform_stsz_350m_benchmark():
    """
    WP-1 O(1) structural/memory invariants & anti-O(N) regression guard.
    Strict < 1ms micro-benchmark SLA is verified in scripts/benchmarks/bench_iso_bmff.py.
    """
    import gc
    import sys

    n = 350_000_000
    uniform_size = 4
    stsz_box = make_full_box(b"stsz", 0, 0, struct.pack(">II", uniform_size, n))

    entry_payload = (
        struct.pack(">6sH", b"\x00" * 6, 1)
        + struct.pack(">HH", 0, 0)
        + struct.pack(">III", 0, 0, 0)
        + struct.pack(">HH", 1920, 1080)
        + struct.pack(">II", 0x00480000, 0x00480000)
        + struct.pack(">IH", 0, 1)
        + b"\x00" * 32
        + struct.pack(">hh", 24, -1)
    )
    entry_box = struct.pack(">I4s", len(entry_payload) + 8, b"avc1") + entry_payload
    stsd_box = make_full_box(b"stsd", 0, 0, struct.pack(">I", 1) + entry_box)
    stts_box = make_full_box(b"stts", 0, 0, struct.pack(">III", 1, n, 1000))
    stsc_box = make_full_box(b"stsc", 0, 0, struct.pack(">IIII", 1, 10, 1, 1))
    co_box = make_full_box(b"stco", 0, 0, struct.pack(">II", 1, 10000))

    stbl = make_box(b"stbl", stsd_box + stts_box + stsc_box + stsz_box + co_box)
    trak = build_synthetic_trak(1, is_video=True, stbl_bytes=stbl)
    moov_bytes = build_synthetic_moov(tracks=[trak])

    # Phase 1: Deterministic O(1) memory & structural invariants (warmup first so
    # coverage/lazy caches don't skew tracemalloc peak on a cold xdist worker)
    parse_moov(moov_bytes)
    gc.collect()
    tracemalloc.start()
    try:
        meta = parse_moov(moov_bytes)
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    v_track = meta.video_track
    assert v_track is not None
    assert isinstance(v_track.stbl.stsz, UniformStszTable)
    assert len(v_track.stbl.stsz) == n
    assert v_track.stbl.stsz[0] == uniform_size
    assert v_track.stbl.stsz[-1] == uniform_size
    assert v_track.stbl.default_sample_size == uniform_size

    # Spec: stsz memory increment < 1KiB (actual is ~48 bytes vs 1.4GB if expanded into array)
    assert sys.getsizeof(v_track.stbl.stsz) < 1024, f"stsz object size {sys.getsizeof(v_track.stbl.stsz)} exceeded 1KiB"
    assert peak_bytes < 64 * 1024, f"Peak memory {peak_bytes} bytes exceeded O(1) limit"

    # Phase 2: Decoupled anti-O(N) regression guard (tracemalloc stopped; warmup + min of 5 runs)
    parse_moov(moov_bytes)
    samples_ms: list[float] = []
    for _ in range(5):
        t0 = time.perf_counter()
        parse_moov(moov_bytes)
        samples_ms.append((time.perf_counter() - t0) * 1000)
    min_elapsed_ms = min(samples_ms)
    assert min_elapsed_ms < 50.0, f"Parse latency {min_elapsed_ms:.3f}ms exceeded CI O(N) regression ceiling"


def test_classify_vsse_mode():
    """Verify classify_vsse_mode three-way routing and backward compatibility."""
    # 1. Standard AVC1 + AAC
    v_stbl = build_synthetic_stbl([1000, 2000], is_video=True, codec=b"avc1")
    v_trak = build_synthetic_trak(1, is_video=True, stbl_bytes=v_stbl)
    a_stbl = build_synthetic_stbl([500, 500], is_video=False, codec=b"mp4a")
    a_trak = build_synthetic_trak(2, is_video=False, stbl_bytes=a_stbl)
    meta = parse_moov(build_synthetic_moov(tracks=[v_trak, a_trak]))

    assert classify_vsse_mode(meta) == VsseRoutingMode.PURE_ZERO_COPY
    validate_vsse_eligibility(meta)  # Should not raise

    # 2. Video only (HEVC/HVC1)
    v_stbl_hevc = build_synthetic_stbl([1000, 2000], is_video=True, codec=b"hvc1")
    v_trak_hevc = build_synthetic_trak(1, is_video=True, stbl_bytes=v_stbl_hevc)
    meta_hevc = parse_moov(build_synthetic_moov(tracks=[v_trak_hevc]))

    assert classify_vsse_mode(meta_hevc) == VsseRoutingMode.PURE_ZERO_COPY
    validate_vsse_eligibility(meta_hevc)

    # 3. Video + non-AAC audio (lpcm, sowt, ac-3, ec-3, opus, flac, mp3, alac)
    for non_aac in [b"lpcm", b"sowt", b"ac-3", b"ec-3", b"Opus", b"flac", b".mp3", b"alac"]:
        a_stbl_non_aac = build_synthetic_stbl([500, 500], is_video=False, codec=non_aac)
        a_trak_non_aac = build_synthetic_trak(2, is_video=False, stbl_bytes=a_stbl_non_aac)
        meta_hybrid = parse_moov(build_synthetic_moov(tracks=[v_trak, a_trak_non_aac]))

        assert classify_vsse_mode(meta_hybrid) == VsseRoutingMode.HYBRID_AUDIO_SIDECAR
        # validate_vsse_eligibility must raise UnsupportedCodecError on non-AAC
        with pytest.raises(UnsupportedCodecError, match="Audio codec .* is not supported in VSSE"):
            validate_vsse_eligibility(meta_hybrid)

    # 4. Missing video track
    meta_no_video = parse_moov(build_synthetic_moov(tracks=[a_trak]))
    with pytest.raises(UnsupportedCodecError, match="No video track found"):
        classify_vsse_mode(meta_no_video)

    # 5. Unsupported video codec (e.g. vp09)
    v_stbl_vp9 = build_synthetic_stbl([1000, 2000], is_video=True, codec=b"vp09")
    v_trak_vp9 = build_synthetic_trak(1, is_video=True, stbl_bytes=v_stbl_vp9)
    meta_vp9 = parse_moov(build_synthetic_moov(tracks=[v_trak_vp9]))
    with pytest.raises(UnsupportedCodecError, match="Video codec .* is not supported in VSSE"):
        classify_vsse_mode(meta_vp9)


def test_parse_sgpd_and_sbgp():
    """Verify sgpd and sbgp box parsing and structure retention."""
    sgpd_payload = b"roll" + struct.pack(">IIh", 2, 1, -1)
    sgpd_box = make_full_box(b"sgpd", 1, 0, sgpd_payload)

    sbgp_payload = b"roll" + struct.pack(">III", 1, 50, 1)
    sbgp_box = make_full_box(b"sbgp", 0, 0, sbgp_payload)

    raw_stbl = build_synthetic_stbl(default_sample_size=1024, sample_count=50, is_video=False)
    stbl_payload = raw_stbl[8:]
    stbl_with_groups = make_box(b"stbl", stbl_payload + sgpd_box + sbgp_box)

    trak = build_synthetic_trak(2, is_video=False, stbl_bytes=stbl_with_groups)
    moov = build_synthetic_moov(tracks=[trak])

    meta = parse_moov(moov)
    a_track = meta.audio_track
    assert a_track is not None
    assert len(a_track.stbl.sgpd_boxes) == 1
    assert a_track.stbl.sgpd_boxes[0] == sgpd_box
    assert len(a_track.stbl.sbgp_boxes) == 1
    assert a_track.stbl.sbgp_boxes[0] == sbgp_box
    assert len(a_track.stbl.sbgp_tables) == 1

    table = a_track.stbl.sbgp_tables[0]
    assert isinstance(table, SbgpTable)
    assert table.version == 0
    assert table.flags == 0
    assert table.grouping_type == b"roll"
    assert len(table.entries) == 1
    assert isinstance(table.entries[0], SbgpEntry)
    assert table.entries[0].sample_count == 50
    assert table.entries[0].group_description_index == 1


def test_classify_vsse_mode_empty_stsd_entries():
    """Verify classify_vsse_mode rejects video with empty stsd and routes audio with empty stsd to sidecar."""
    v_stbl = build_synthetic_stbl(is_video=True)
    v_trak = build_synthetic_trak(1, is_video=True, stbl_bytes=v_stbl)
    moov = build_synthetic_moov(tracks=[v_trak])
    meta = parse_moov(moov)
    assert meta.video_track is not None
    # Empty stsd entries on video track
    meta.video_track.stbl.stsd.entries = []
    with pytest.raises(UnsupportedCodecError, match="Video track has no sample description entries"):
        classify_vsse_mode(meta)

    # Empty stsd entries on audio track
    v_stbl2 = build_synthetic_stbl(is_video=True)
    v_trak2 = build_synthetic_trak(1, is_video=True, stbl_bytes=v_stbl2)
    a_stbl2 = build_synthetic_stbl(is_video=False)
    a_trak2 = build_synthetic_trak(2, is_video=False, stbl_bytes=a_stbl2)
    moov2 = build_synthetic_moov(tracks=[v_trak2, a_trak2])
    meta2 = parse_moov(moov2)
    assert meta2.audio_track is not None
    meta2.audio_track.stbl.stsd.entries = []
    assert classify_vsse_mode(meta2) == VsseRoutingMode.HYBRID_AUDIO_SIDECAR


def test_parse_sbgp_version_validation():
    """Verify sbgp version 1 parses grouping_type_parameter and unsupported version raises error."""
    # Version 1 with grouping_type_parameter
    sbgp_v1_payload = b"roll" + struct.pack(">III", 42, 1, 50) + struct.pack(">I", 1)
    sbgp_v1_box = make_full_box(b"sbgp", 1, 0, sbgp_v1_payload)
    raw_stbl = build_synthetic_stbl(default_sample_size=1024, sample_count=50, is_video=False)
    stbl_payload = raw_stbl[8:]
    stbl_with_v1 = make_box(b"stbl", stbl_payload + sbgp_v1_box)
    trak = build_synthetic_trak(2, is_video=False, stbl_bytes=stbl_with_v1)
    moov = build_synthetic_moov(tracks=[trak])
    meta = parse_moov(moov)
    assert meta.audio_track is not None
    table = meta.audio_track.stbl.sbgp_tables[0]
    assert table.version == 1
    assert table.grouping_type_parameter == 42
    assert table.entries[0].sample_count == 50
    assert table.entries[0].group_description_index == 1

    # Version 2 (unsupported) — 容错降级: parse 不失败, 仅丢弃该 sbgp 组信息
    sbgp_v2_box = make_full_box(b"sbgp", 2, 0, b"roll" + struct.pack(">III", 1, 50, 1))
    stbl_with_v2 = make_box(b"stbl", stbl_payload + sbgp_v2_box)
    trak_v2 = build_synthetic_trak(2, is_video=False, stbl_bytes=stbl_with_v2)
    moov_v2 = build_synthetic_moov(tracks=[trak_v2])
    meta_v2 = parse_moov(moov_v2)
    assert meta_v2.audio_track is not None
    assert meta_v2.audio_track.stbl.sbgp_boxes == []
    assert meta_v2.audio_track.stbl.sbgp_tables == []


def test_parse_sbgp_malformed_tolerated():
    """Truncated sbgp is dropped with warning instead of failing the whole parse_moov."""
    # payload 仅 4 字节 grouping_type, 缺 entry_count → _parse_sbgp 抛 Truncated sbgp box
    truncated_sbgp = make_full_box(b"sbgp", 0, 0, b"roll")
    raw_stbl = build_synthetic_stbl(default_sample_size=1024, sample_count=50, is_video=False)
    stbl_payload = raw_stbl[8:]
    stbl_bad = make_box(b"stbl", stbl_payload + truncated_sbgp)
    trak = build_synthetic_trak(2, is_video=False, stbl_bytes=stbl_bad)
    meta = parse_moov(build_synthetic_moov(tracks=[trak]))
    assert meta.audio_track is not None
    assert meta.audio_track.stbl.sbgp_boxes == []
    assert meta.audio_track.stbl.sbgp_tables == []


def _build_mp4a_entry_with_esds(object_type: int, *, wrap_in_wave: bool = False) -> bytes:
    dcd = bytes([0x04, 0x0D, object_type, 0x15, 0x00, 0x18, 0x00, 0x00, 0x01, 0xF4, 0x00, 0x00, 0x01, 0xF4, 0x00])
    es_desc = bytes([0x03, 3 + len(dcd), 0x00, 0x01, 0x00]) + dcd
    esds_box = make_full_box(b"esds", 0, 0, es_desc)
    child = make_box(b"wave", esds_box) if wrap_in_wave else esds_box
    audio_header = (
        struct.pack(">6sH", b"\x00" * 6, 1)
        + b"\x00" * 8
        + struct.pack(">HH", 2, 16)
        + b"\x00" * 4
        + struct.pack(">I", 48000 << 16)
    )
    return make_box(b"mp4a", audio_header + child)


@pytest.mark.parametrize("wrap_in_wave", [False, True])
def test_parse_moov_mp4a_esds_non_aac_routes_to_sidecar(wrap_in_wave: bool):
    """#5: mp4a 内 esds objectTypeIndication=0x6B (MPEG-1 Audio) 必须识别为非 AAC 并走 HYBRID_AUDIO_SIDECAR。"""
    v_stbl = build_synthetic_stbl(is_video=True)
    v_trak = build_synthetic_trak(1, is_video=True, stbl_bytes=v_stbl)

    mp4a_box = _build_mp4a_entry_with_esds(0x6B, wrap_in_wave=wrap_in_wave)
    stsd_box = make_full_box(b"stsd", 0, 0, struct.pack(">I", 1) + mp4a_box)
    stts_box = make_full_box(b"stts", 0, 0, struct.pack(">III", 1, 1, 1000))
    stsc_box = make_full_box(b"stsc", 0, 0, struct.pack(">IIII", 1, 1, 1, 1))
    stsz_box = make_full_box(b"stsz", 0, 0, struct.pack(">III", 0, 1, 1000))
    stco_box = make_full_box(b"stco", 0, 0, struct.pack(">II", 1, 10000))
    a_stbl = make_box(b"stbl", stsd_box + stts_box + stsc_box + stsz_box + stco_box)
    a_trak = build_synthetic_trak(2, is_video=False, stbl_bytes=a_stbl)

    meta = parse_moov(build_synthetic_moov(tracks=[v_trak, a_trak]))
    assert meta.audio_track is not None
    entry = meta.audio_track.stbl.stsd.entries[0]
    assert entry.esds_object_type == 0x6B
    assert entry.codec == "mp3"
    assert classify_vsse_mode(meta) == VsseRoutingMode.HYBRID_AUDIO_SIDECAR
    with pytest.raises(UnsupportedCodecError):
        validate_vsse_eligibility(meta)


def test_parse_moov_mp4a_esds_aac_stays_pure_zero_copy():
    """#5: mp4a 内 esds objectTypeIndication=0x40 (AAC) 保持 PURE_ZERO_COPY。"""
    v_stbl = build_synthetic_stbl(is_video=True)
    v_trak = build_synthetic_trak(1, is_video=True, stbl_bytes=v_stbl)
    mp4a_box = _build_mp4a_entry_with_esds(0x40)
    stsd_box = make_full_box(b"stsd", 0, 0, struct.pack(">I", 1) + mp4a_box)
    stts_box = make_full_box(b"stts", 0, 0, struct.pack(">III", 1, 1, 1000))
    stsc_box = make_full_box(b"stsc", 0, 0, struct.pack(">IIII", 1, 1, 1, 1))
    stsz_box = make_full_box(b"stsz", 0, 0, struct.pack(">III", 0, 1, 1000))
    stco_box = make_full_box(b"stco", 0, 0, struct.pack(">II", 1, 10000))
    a_stbl = make_box(b"stbl", stsd_box + stts_box + stsc_box + stsz_box + stco_box)
    a_trak = build_synthetic_trak(2, is_video=False, stbl_bytes=a_stbl)

    meta = parse_moov(build_synthetic_moov(tracks=[v_trak, a_trak]))
    assert meta.audio_track is not None
    entry = meta.audio_track.stbl.stsd.entries[0]
    assert entry.esds_object_type == 0x40
    assert entry.codec == "mp4a"
    assert classify_vsse_mode(meta) == VsseRoutingMode.PURE_ZERO_COPY


def test_parse_moov_dinf_dref_self_contained_flags():
    """#7: parse_moov 解析 dinf/dref 的 self-contained 标志到 stsd.dref_self_contained。"""
    stbl = build_synthetic_stbl(is_video=True)
    url_ext = make_full_box(b"url ", 0, 0, b"ext.mov\x00")
    url_self = make_full_box(b"url ", 0, 1, b"")
    dref = make_full_box(b"dref", 0, 0, struct.pack(">I", 2) + url_ext + url_self)
    dinf = make_box(b"dinf", dref)
    tkhd = make_full_box(b"tkhd", 0, 7, struct.pack(">IIIII", 0, 0, 1, 0, 1000) + b"\x00" * 60)
    mdhd = make_full_box(b"mdhd", 0, 0, struct.pack(">IIIIHH", 0, 0, 1000, 1000, 0x55C4, 0))
    hdlr = make_full_box(b"hdlr", 0, 0, struct.pack(">I4s12s", 0, b"vide", b"\x00" * 12) + b"Video\x00")
    minf = make_box(b"minf", dinf + stbl)
    mdia = make_box(b"mdia", mdhd + hdlr + minf)
    trak = make_box(b"trak", tkhd + mdia)

    meta = parse_moov(build_synthetic_moov(tracks=[trak]))
    assert meta.video_track is not None
    assert meta.video_track.stbl.stsd.dref_self_contained == (False, True)


def test_parse_moov_rejects_truncated_dref():
    """声明了 entry_count 但被截断的 dref 必须抛 IsoBmffParseError, 不能静默跳过校验。"""
    stbl = build_synthetic_stbl(is_video=True)
    # entry_count=1 但无后续条目字节
    truncated_dref = make_full_box(b"dref", 0, 0, struct.pack(">I", 1))
    dinf = make_box(b"dinf", truncated_dref)
    tkhd = make_full_box(b"tkhd", 0, 7, struct.pack(">IIIII", 0, 0, 1, 0, 1000) + b"\x00" * 60)
    mdhd = make_full_box(b"mdhd", 0, 0, struct.pack(">IIIIHH", 0, 0, 1000, 1000, 0x55C4, 0))
    hdlr = make_full_box(b"hdlr", 0, 0, struct.pack(">I4s12s", 0, b"vide", b"\x00" * 12) + b"Video\x00")
    minf = make_box(b"minf", dinf + stbl)
    mdia = make_box(b"mdia", mdhd + hdlr + minf)
    trak = make_box(b"trak", tkhd + mdia)

    with pytest.raises(IsoBmffParseError, match="Truncated dref entry"):
        parse_moov(build_synthetic_moov(tracks=[trak]))
