"""
Unit tests for ISO-BMFF Box Synthesizer (Task 2).

Covers:
1. 16-byte 64-bit mdat header tests.
2. Gate 1: stss 1-based index remapping and stss[0] == 1 enforcement.
3. Gate 2: stts / ctts RLE exact slicing and sum(sample_count) == N enforcement.
4. Gate 3: mvhd, tkhd, mdhd duration synchronization.
5. Gate 4: Superfluous track (subtitle, chapter, extra audio) and udta stripping.
6. Three-part chunk restructuring (stsc and co64) with intra-chunk offsets for first and tail chunks.
7. Endianness test ensuring valid Big-Endian output on Windows Little-Endian.
8. Audio alignment via elst (delta = PTS(v_start) - PTS(a_start) >= 0).
9. Synthetic moov synthesis validated with box_parser.parse_moov.
10. Real MP4 moov synthesis with ffmpeg validated with box_parser.parse_moov and ffmpeg decode.
"""

import array
import os
import struct
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
    SbgpEntry,
    SbgpTable,
    StscEntry,
    Stsd,
    SttsEntry,
    Tkhd,
    Track,
    UniformStszTable,
    parse_moov,
    scan_top_level_boxes,
)
from app.core.iso_bmff.box_synthesizer import (
    array_to_big_endian_bytes,
    build_64bit_mdat_header,
    compute_envelope_bounds,
    compute_sample_pts_dts,
    configure_audio_elst,
    get_sample_byte_offset,
    get_track_chunks,
    make_box,
    make_full_box,
    remap_stss,
    restructure_sample_table,
    serialize_co64,
    serialize_mdhd,
    serialize_moov,
    serialize_mvhd,
    serialize_sbgp,
    serialize_stss,
    serialize_stsz,
    serialize_tkhd,
    serialize_trak,
    slice_rle_table,
    slice_sbgp_entries,
    synchronize_durations,
    synthesize_faststart_envelope_header,
)
from app.core.iso_bmff.exceptions import IsoBmffError

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

    # Chunk setup
    total_chunks = (num_samples + samples_per_chunk - 1) // samples_per_chunk
    if chunk_offsets is not None:
        offsets = chunk_offsets
    else:
        # Default offsets spaced by 10,000 bytes
        offsets = [10000 + i * 15000 for i in range(total_chunks)]

    stsz_arr = array.array("I", sizes)
    co_arr = array.array("Q", offsets)

    # stsc: 1 entry
    stsc = [StscEntry(first_chunk=1, samples_per_chunk=samples_per_chunk, sample_description_index=1)]

    # stts: 1 entry
    stts = [SttsEntry(sample_count=num_samples, sample_delta=sample_delta)]

    # ctts
    ctts = None
    if ctts_offsets is not None:
        ctts = [CttsEntry(sample_count=1, sample_offset=off) for off in ctts_offsets]

    # stss
    stss_arr = None
    if sync_samples is not None:
        stss_arr = array.array("I", sync_samples)
    elif is_video:
        # Default keyframe every 10 samples
        stss_arr = array.array("I", [1 + i * 10 for i in range((num_samples + 9) // 10)])

    # stsd
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


# -----------------------------------------------------------------------------
# Test Cases
# -----------------------------------------------------------------------------


def test_build_64bit_mdat_header():
    """Verify 16-byte 64-bit mdat header generation and length invariances."""
    # 1. Standard payload
    hdr = build_64bit_mdat_header(1000)
    assert len(hdr) == 16
    assert hdr[:4] == b"\x00\x00\x00\x01"
    assert hdr[4:8] == b"mdat"
    large_size = struct.unpack_from(">Q", hdr, 8)[0]
    assert large_size == 16 + 1000

    # 2. Zero payload length
    hdr_zero = build_64bit_mdat_header(0)
    assert len(hdr_zero) == 16
    assert struct.unpack_from(">Q", hdr_zero, 8)[0] == 16

    # 3. > 4GB payload length
    huge_payload = 5 * 1024 * 1024 * 1024  # 5GB
    hdr_huge = build_64bit_mdat_header(huge_payload)
    assert len(hdr_huge) == 16
    assert struct.unpack_from(">Q", hdr_huge, 8)[0] == 16 + huge_payload

    # 4. Negative payload raises ValueError
    with pytest.raises(ValueError, match="non-negative"):
        build_64bit_mdat_header(-1)


def test_endianness_byteswap_safety():
    """Verify cross-platform endianness safety with array.array."""
    # Test array of 32-bit uints
    arr_u32 = array.array("I", [0x12345678, 1, 0])
    raw_bytes = array_to_big_endian_bytes(arr_u32)

    # In Big-Endian, 0x12345678 is b"\x12\x34\x56\x78"
    expected = b"\x12\x34\x56\x78\x00\x00\x00\x01\x00\x00\x00\x00"
    assert raw_bytes == expected

    # Verify input array was not mutated in place
    assert arr_u32[0] == 0x12345678
    assert arr_u32[1] == 1

    # Test array of 64-bit uints
    arr_u64 = array.array("Q", [0x0102030405060708])
    raw_bytes_u64 = array_to_big_endian_bytes(arr_u64)
    assert raw_bytes_u64 == b"\x01\x02\x03\x04\x05\x06\x07\x08"


def test_gate_1_stss_remapping_and_enforcement():
    """
    Gate 1: Verify stss 1-based remapping and strict stss[0] == 1 assertion.
    stss_new[i] = S_i - V_start + 1.
    """
    sync_samples = [1, 251, 501, 751, 1001]

    # Valid keyframe slice starting at sample 251
    remapped = remap_stss(sync_samples, v_start=251, v_end=600)
    assert len(remapped) == 2
    assert remapped[0] == 1  # 251 - 251 + 1 == 1
    assert remapped[1] == 251  # 501 - 251 + 1 == 251

    # Slice starting at non-keyframe must raise IsoBmffError
    with pytest.raises(IsoBmffError, match="Gate 1 Violation.*must strictly be 1"):
        remap_stss(sync_samples, v_start=100, v_end=600)

    # Slice containing zero keyframes must raise IsoBmffError
    with pytest.raises(IsoBmffError, match="Gate 1 Violation: No sync samples found"):
        remap_stss(sync_samples, v_start=100, v_end=200)

    # Invalid range arguments
    with pytest.raises(ValueError):
        remap_stss(sync_samples, v_start=500, v_end=400)
    with pytest.raises(ValueError):
        remap_stss(sync_samples, v_start=0, v_end=100)


def test_gate_2_stts_ctts_rle_slicing():
    """
    Gate 2: Verify exact slicing of RLE tables (stts and ctts).
    Sum of sample_count in sliced entries must strictly equal N = end_sample - start_sample + 1.
    """
    # stts with 3 entries covering 100 samples
    entries = [
        SttsEntry(sample_count=20, sample_delta=1000),  # samples 1..20
        SttsEntry(sample_count=50, sample_delta=2000),  # samples 21..70
        SttsEntry(sample_count=30, sample_delta=1500),  # samples 71..100
    ]

    # 1. Slicing across all 3 entries: samples 15 to 80 (N = 80 - 15 + 1 = 66)
    sliced = slice_rle_table(entries, 15, 80)
    assert len(sliced) == 3
    # From first entry: samples 15..20 -> 6
    assert sliced[0].sample_count == 6
    assert sliced[0].sample_delta == 1000
    # From second entry: samples 21..70 -> 50
    assert sliced[1].sample_count == 50
    assert sliced[1].sample_delta == 2000
    # From third entry: samples 71..80 -> 10
    assert sliced[2].sample_count == 10
    assert sliced[2].sample_delta == 1500

    assert sum(e.sample_count for e in sliced) == 66

    # 2. Slicing strictly within a single entry: samples 25 to 30 (N = 6)
    sliced_single = slice_rle_table(entries, 25, 30)
    assert len(sliced_single) == 1
    assert sliced_single[0].sample_count == 6
    assert sliced_single[0].sample_delta == 2000

    # 3. Exactly 1 sample: sample 1 (N = 1)
    sliced_one = slice_rle_table(entries, 1, 1)
    assert len(sliced_one) == 1
    assert sliced_one[0].sample_count == 1
    assert sliced_one[0].sample_delta == 1000

    # 4. Slicing ctts entries
    c_entries = [
        CttsEntry(sample_count=10, sample_offset=-100),
        CttsEntry(sample_count=20, sample_offset=200),
    ]
    c_sliced = slice_rle_table(c_entries, 5, 15)
    assert len(c_sliced) == 2
    assert c_sliced[0].sample_count == 6
    assert c_sliced[0].sample_offset == -100
    assert c_sliced[1].sample_count == 5
    assert c_sliced[1].sample_offset == 200
    assert sum(e.sample_count for e in c_sliced) == 11

    # 5. Out of range slicing (requesting samples past available total)
    with pytest.raises(IsoBmffError, match="Gate 2 Violation"):
        slice_rle_table(entries, 80, 120)


def test_gate_3_duration_synchronization():
    """
    Gate 3: Verify synchronization of mvhd.duration, tkhd.duration, and mdhd.duration.
    """
    v_track = create_synthetic_track(track_id=1, is_video=True, sample_delta=1000, timescale=1000)
    a_track = create_synthetic_track(track_id=2, is_video=False, sample_delta=1024, timescale=48000)

    # Change sample counts
    v_track.stbl.stts = [SttsEntry(sample_count=50, sample_delta=1000)]  # dur = 50,000 in 1000 scale -> 50s
    a_track.stbl.stts = [SttsEntry(sample_count=2343, sample_delta=1024)]  # dur = 2,399,232 in 48000 scale -> 49.984s

    mvhd = Mvhd(
        version=0,
        flags=0,
        creation_time=0,
        modification_time=0,
        timescale=1000,
        duration=0,
    )

    synchronize_durations(mvhd, [v_track, a_track], movie_timescale=1000)

    # Video check
    assert v_track.mdhd.duration == 50000
    assert v_track.tkhd.duration == 50000

    # Audio check
    assert a_track.mdhd.duration == 2399232
    assert a_track.tkhd.duration == round(2399232 * 1000 / 48000)

    # Movie duration is max of track durations
    assert mvhd.duration == max(v_track.tkhd.duration, a_track.tkhd.duration)


def test_gate_4_strip_superfluous_tracks_and_udta():
    """
    Gate 4: Verify that subtitle, chapter, secondary audio tracks, and udta boxes
    are physically stripped when generating moov.
    """
    v_track = create_synthetic_track(track_id=1, is_video=True)
    a_track1 = create_synthetic_track(track_id=2, is_video=False)
    a_track2 = create_synthetic_track(track_id=3, is_video=False)

    # Create fake subtitle track
    sub_track = create_synthetic_track(track_id=4, is_video=False)
    sub_track.hdlr.handler_type = b"sbtl"

    # Create fake chapter track
    chap_track = create_synthetic_track(track_id=5, is_video=False)
    chap_track.hdlr.handler_type = b"tmcd"

    mvhd = Mvhd(
        version=0,
        flags=0,
        creation_time=0,
        modification_time=0,
        timescale=1000,
        duration=30000,
        next_track_id=6,
    )

    metadata = Mp4Metadata(
        mvhd=mvhd,
        tracks=[v_track, a_track1, a_track2, sub_track, chap_track],
        udta_bytes=b"raw_udta_metadata_junk",
    )

    # Serialize moov
    moov_bytes = serialize_moov(metadata)

    # Parse back using box_parser
    parsed = parse_moov(moov_bytes)

    # Assertions
    assert len(parsed.tracks) == 2
    assert parsed.video_track is not None
    assert parsed.audio_track is not None
    assert parsed.video_track.track_id == 1
    assert parsed.audio_track.track_id == 2
    # Ensure udta was dropped
    assert parsed.udta_bytes is None


def test_three_part_chunk_restructuring():
    """
    Verify three-part chunk restructuring:
    1. First chunk intra-chunk sample offset (r_start)
    2. Middle chunks full chunk preservation
    3. Tail chunk intra-chunk truncation
    """
    # 3 chunks with 10 samples each (total 30 samples)
    # Sizes: sample i has size (100 + i)
    sample_sizes = [100 + i for i in range(30)]
    chunk_offsets = [10000, 25000, 40000]

    track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=sample_sizes,
        chunk_offsets=chunk_offsets,
        samples_per_chunk=10,
        sync_samples=[1, 5, 11, 21],
    )

    # Slice samples 5 to 25 (1-based, inclusive):
    # - Sample 5 is in Chunk 1 (starts at sample 1, samples 1..4 skipped)
    # - Samples 11..20 is Chunk 2 (full middle chunk)
    # - Samples 21..25 is Chunk 3 (tail chunk, samples 26..30 truncated)
    # - v_start=5 is a keyframe (sync_samples includes 5)
    new_stbl, eff_offsets = restructure_sample_table(
        track=track,
        start_sample=5,
        end_sample=25,
        o_min=5000,
        h_offset=100,
    )

    assert len(eff_offsets) == 3

    # Check Chunk 1:
    # Skipped samples: 1, 2, 3, 4 (sizes: 100, 101, 102, 103 -> sum = 406)
    expected_eff_0 = 10000 + 406
    assert eff_offsets[0] == expected_eff_0
    assert new_stbl.chunk_offsets[0] == 100 + (expected_eff_0 - 5000)

    # Check Chunk 2:
    assert eff_offsets[1] == 25000
    assert new_stbl.chunk_offsets[1] == 100 + (25000 - 5000)

    # Check Chunk 3:
    assert eff_offsets[2] == 40000
    assert new_stbl.chunk_offsets[2] == 100 + (40000 - 5000)

    # Check sample counts:
    # Chunk 1: 10 - 4 = 6 samples
    # Chunk 2: 10 samples
    # Chunk 3: 25 - 21 + 1 = 5 samples
    # Total samples: 6 + 10 + 5 = 21
    assert len(new_stbl.stsz) == 21
    assert new_stbl.stsz[0] == sample_sizes[4]  # sample 5
    assert new_stbl.stsz[-1] == sample_sizes[24]  # sample 25

    # Check stsc table
    # Chunk 1: 6 samples
    # Chunk 2: 10 samples
    # Chunk 3: 5 samples
    assert len(new_stbl.stsc) == 3
    assert new_stbl.stsc[0].first_chunk == 1 and new_stbl.stsc[0].samples_per_chunk == 6
    assert new_stbl.stsc[1].first_chunk == 2 and new_stbl.stsc[1].samples_per_chunk == 10
    assert new_stbl.stsc[2].first_chunk == 3 and new_stbl.stsc[2].samples_per_chunk == 5

    # Check stss remapped:
    # Original: [1, 5, 11, 21]
    # In range [5, 25]: 5 -> 1, 11 -> 7, 21 -> 17
    assert list(new_stbl.stss) == [1, 7, 17]


def test_chunk_restructuring_single_chunk():
    """Verify chunk restructuring when all sliced samples fall into a single chunk (m_new == 1)."""
    sample_sizes = [100] * 20
    chunk_offsets = [10000, 20000]

    track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=sample_sizes,
        chunk_offsets=chunk_offsets,
        samples_per_chunk=10,
        sync_samples=[1, 3],
    )

    # Slice samples 3 to 7 (both in Chunk 1)
    new_stbl, eff_offsets = restructure_sample_table(
        track=track,
        start_sample=3,
        end_sample=7,
        o_min=10000,
        h_offset=50,
    )

    assert len(eff_offsets) == 1
    # Samples 1 and 2 skipped (2 * 100 = 200 bytes)
    assert eff_offsets[0] == 10200
    assert new_stbl.chunk_offsets[0] == 50 + 200
    assert len(new_stbl.stsz) == 5
    assert len(new_stbl.stsc) == 1
    assert new_stbl.stsc[0].first_chunk == 1 and new_stbl.stsc[0].samples_per_chunk == 5


def test_audio_alignment_via_elst():
    """
    Verify audio alignment via elst configuration:
    - Positive delta (PTS(v_start) >= PTS(a_start)) -> single normal edit entry (media_time >= 0).
    - Negative delta (PTS(v_start) < PTS(a_start)) -> two-entry elst with leading Empty Edit (media_time == -1).
    """
    v_track = create_synthetic_track(track_id=1, is_video=True, sample_delta=1000, timescale=1000)
    a_track = create_synthetic_track(
        track_id=2, is_video=False, sample_sizes=[250] * 500, sample_delta=1024, timescale=48000
    )

    # Video sample 11: DTS = 10 * 1000 = 10,000 (10.0s)
    # Audio sample 469: DTS = 468 * 1024 = 479,232 (9.984s)
    # delta = 10.0 - 9.984 = 0.016s >= 0
    configure_audio_elst(
        audio_track=a_track,
        video_track=v_track,
        v_start=11,
        a_start=469,
        movie_timescale=1000,
    )

    assert a_track.edts is not None
    assert len(a_track.edts.entries) == 1
    entry = a_track.edts.entries[0]
    expected_media_time = round(0.016 * 48000)
    assert entry.media_time == expected_media_time
    assert entry.segment_duration == a_track.tkhd.duration

    # Tier A: If audio PTS > video PTS (negative delta), must construct two-entry elst with Empty Edit
    a_track.edts = None
    configure_audio_elst(
        audio_track=a_track,
        video_track=v_track,
        v_start=1,  # Video PTS = 0.0s
        a_start=469,  # Audio PTS = 9.984s -> effective_delta = -9.984s
        movie_timescale=1000,
    )
    assert a_track.edts is not None
    assert len(a_track.edts.entries) == 2
    empty_entry, media_entry = a_track.edts.entries
    assert empty_entry.media_time == -1
    assert empty_entry.segment_duration == 9984
    assert media_entry.media_time == 0
    audio_dur = sum(e.sample_count * e.sample_delta for e in a_track.stbl.stts)
    expected_media_dur = round(audio_dur * 1000 / 48000)
    assert media_entry.segment_duration == expected_media_dur
    assert a_track.tkhd.duration == empty_entry.segment_duration + media_entry.segment_duration


def test_audio_alignment_with_priming_delay_compensation():
    """
    Verify configure_audio_elst correctly compensates for source track priming delays
    c = (m_a / t_a) - (m_v / t_v) and deducts media_time from segment_duration.
    """
    v_track = create_synthetic_track(track_id=1, is_video=True, sample_delta=1000, timescale=1000)
    a_track = create_synthetic_track(
        track_id=2, is_video=False, sample_sizes=[250] * 500, sample_delta=1024, timescale=48000
    )

    # Source tracks with priming delay:
    # Video: mv = 0
    # Audio: ma = 1024 samples (AAC priming delay ~21.33ms)
    orig_a = create_synthetic_track(
        track_id=2, is_video=False, sample_sizes=[250] * 500, sample_delta=1024, timescale=48000
    )
    orig_a.edts = Edts(
        elst_version=0,
        elst_flags=0,
        entries=[ElstEntry(segment_duration=10000, media_time=1024, media_rate_integer=1, media_rate_fraction=0)],
    )

    # Video sample 11: DTS = 10.0s
    # Audio sample 469: DTS = 9.984s
    # delta = 10.0 - 9.984 = 0.016s
    # c = 1024 / 48000 - 0 = 0.0213333...s
    # effective_delta = 0.016 + 1024 / 48000
    # media_time = round(effective_delta * 48000) = round(0.016 * 48000 + 1024) = 768 + 1024 = 1792
    configure_audio_elst(
        audio_track=a_track,
        video_track=v_track,
        v_start=11,
        a_start=469,
        movie_timescale=1000,
        original_audio_track=orig_a,
        original_video_track=v_track,
    )

    assert a_track.edts is not None
    assert len(a_track.edts.entries) == 1
    entry = a_track.edts.entries[0]
    expected_media_time = 1792
    assert entry.media_time == expected_media_time
    total_audio_dur = sum(e.sample_count * e.sample_delta for e in a_track.stbl.stts)
    expected_segment_dur = int(round((total_audio_dur - expected_media_time) * 1000 / 48000))
    assert entry.segment_duration == expected_segment_dur
    assert entry.segment_duration == a_track.tkhd.duration


def test_synthesize_faststart_envelope_header_roundtrip():
    """
    Verify complete two-pass synthesis and roundtrip parsing with box_parser.parse_moov.
    """
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[500] * 50,
        chunk_offsets=[10000 + i * 5000 for i in range(5)],
        samples_per_chunk=10,
        sample_delta=1000,
        timescale=1000,
        sync_samples=[1, 11, 21, 31, 41],
    )
    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[250] * 50,
        chunk_offsets=[50000 + i * 2500 for i in range(5)],
        samples_per_chunk=10,
        sample_delta=1000,
        timescale=1000,
    )

    mvhd = Mvhd(
        version=0,
        flags=0,
        creation_time=0,
        modification_time=0,
        timescale=1000,
        duration=50000,
        next_track_id=3,
    )

    metadata = Mp4Metadata(mvhd=mvhd, tracks=[v_track, a_track])
    ftyp_bytes = make_box(b"ftyp", b"isom\x00\x00\x02\x00isomiso2mp41")

    # Slice video [11, 30] (20 samples, starts at keyframe 11)
    # Audio [11, 30] (20 samples)
    _, _, payload_len = compute_envelope_bounds(metadata, 11, 30, 11, 30)
    envelope = synthesize_faststart_envelope_header(
        metadata=metadata,
        ftyp_bytes=ftyp_bytes,
        v_start=11,
        v_end=30,
        a_start=11,
        a_end=30,
        payload_len=payload_len,
    )

    # Verify top-level structure
    # Virtual file size is len(envelope) + payload_len
    virtual_file_size = len(envelope) + payload_len
    top_boxes = scan_top_level_boxes(envelope, virtual_file_size)
    assert len(top_boxes) == 3
    assert [b.box_type for b in top_boxes] == [b"ftyp", b"moov", b"mdat"]
    assert top_boxes[2].header_size == 16  # 64-bit mdat header

    # Extract and parse moov box
    moov_hdr = top_boxes[1]
    moov_bytes = envelope[moov_hdr.offset : moov_hdr.offset + moov_hdr.size]
    parsed_meta = parse_moov(moov_bytes)

    # Verify parsed properties
    assert len(parsed_meta.tracks) == 2
    parsed_v = parsed_meta.video_track
    parsed_a = parsed_meta.audio_track
    assert parsed_v is not None
    assert parsed_a is not None

    # Video checks
    assert len(parsed_v.stbl.stsz) == 20
    assert parsed_v.stbl.stss is not None
    assert parsed_v.stbl.stss[0] == 1  # Gate 1 strictly verified
    assert parsed_v.stbl.is_co64 is True  # Mandatory co64 verified

    # Chunk offsets check: all offsets must be >= H
    H = len(envelope)
    for off in parsed_v.stbl.chunk_offsets:
        assert off >= H
    for off in parsed_a.stbl.chunk_offsets:
        assert off >= H

    # Durations check
    assert parsed_v.mdhd.duration == 20000
    assert parsed_a.mdhd.duration == 20000
    assert parsed_meta.mvhd.duration == 20000


def test_real_mp4_synthesis_and_ffmpeg_decode(tmp_path):
    """
    End-to-End Golden Verification:
    1. Generate real MP4 with FFmpeg (FastStart H.264 + AAC, 5.0 seconds).
    2. Parse original MP4 with box_parser.
    3. Synthesize faststart envelope header for slice [26, 75] (2.0s slice, starting on keyframe 26).
    4. Extract media payload from [O_min, O_max] and concatenate with synthesized header.
    5. Run ffprobe and ffmpeg full-file null decode with 0 errors!
    """
    src_mp4 = str(tmp_path / "src.mp4")
    # Generate 5-second video, 25fps (125 frames), GOP=25 (keyframes at 1, 26, 51, 76, 101)
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
        "-movflags",
        "+faststart",
        src_mp4,
    ]
    subprocess.run(cmd, check=True)
    assert os.path.exists(src_mp4)

    with open(src_mp4, "rb") as f:
        file_bytes = f.read()

    file_size = len(file_bytes)
    top_boxes = scan_top_level_boxes(file_bytes, file_size)
    moov_box = next(b for b in top_boxes if b.box_type == b"moov")
    ftyp_box = next(b for b in top_boxes if b.box_type == b"ftyp")

    ftyp_bytes = file_bytes[ftyp_box.offset : ftyp_box.offset + ftyp_box.size]
    moov_bytes = file_bytes[moov_box.offset : moov_box.offset + moov_box.size]

    metadata = parse_moov(moov_bytes)
    v_track = metadata.video_track
    a_track = metadata.audio_track
    assert v_track is not None
    assert a_track is not None

    # Keyframe verification
    assert v_track.stbl.stss is not None
    assert 26 in v_track.stbl.stss

    # Slice video: samples 26 to 75 (50 frames = 2.0s)
    v_start = 26
    v_end = 75

    # Compute corresponding audio samples
    # Video start time = 25 / 25 = 1.0s
    # Audio timescale = 48000, 1024 samples per frame (~21.33ms)
    # Find audio sample closest to 1.0s with PTS <= 1.0s
    a_timescale = a_track.mdhd.timescale
    a_start = 1
    for i in range(1, len(a_track.stbl.stsz) + 1):
        _, pts = compute_sample_pts_dts(a_track, i)
        if pts / a_timescale <= 1.0:
            a_start = i
        else:
            break

    # Audio end for ~2.0s duration
    a_end = min(len(a_track.stbl.stsz), a_start + int(round(2.0 * a_timescale / 1024)) + 5)

    o_min, o_max, payload_len = compute_envelope_bounds(metadata, v_start, v_end, a_start, a_end)

    # Synthesize envelope header
    env_header = synthesize_faststart_envelope_header(
        metadata=metadata,
        ftyp_bytes=ftyp_bytes,
        v_start=v_start,
        v_end=v_end,
        a_start=a_start,
        a_end=a_end,
        o_min=o_min,
        o_max=o_max,
        payload_len=payload_len,
    )

    # Extract raw media data slice from source file
    media_data = file_bytes[o_min:o_max]
    assert len(media_data) == payload_len

    # Assemble complete virtual MP4 file
    sliced_mp4 = str(tmp_path / "sliced.mp4")
    with open(sliced_mp4, "wb") as f:
        f.write(env_header)
        f.write(media_data)

    assert os.path.exists(sliced_mp4)
    sliced_size = os.path.getsize(sliced_mp4)
    assert sliced_size == len(env_header) + payload_len

    # Execute FFmpeg full decode verification (null muxer)
    decode_cmd = [
        "ffmpeg",
        "-v",
        "error",
        "-i",
        sliced_mp4,
        "-f",
        "null",
        "-",
    ]
    res = subprocess.run(decode_cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"FFmpeg decode failed with stderr: {res.stderr}"

    # Execute ffprobe verification
    probe_cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration,size:stream=codec_name,width,height",
        "-of",
        "csv=p=0",
        sliced_mp4,
    ]
    probe_res = subprocess.run(probe_cmd, capture_output=True, text=True, check=True)
    assert "h264" in probe_res.stdout
    assert "aac" in probe_res.stdout


def test_synthesize_faststart_envelope_header_video_only():
    """Verify envelope synthesis for video-only MP4 (no audio track)."""
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[500] * 30,
        chunk_offsets=[10000, 20000, 30000],
        samples_per_chunk=10,
        sync_samples=[1, 11, 21],
    )
    mvhd = Mvhd(
        version=0,
        flags=0,
        creation_time=0,
        modification_time=0,
        timescale=1000,
        duration=30000,
        next_track_id=2,
    )
    metadata = Mp4Metadata(mvhd=mvhd, tracks=[v_track])
    ftyp_bytes = make_box(b"ftyp", b"isom\x00\x00\x02\x00isomiso2mp41")

    envelope = synthesize_faststart_envelope_header(
        metadata=metadata,
        ftyp_bytes=ftyp_bytes,
        v_start=11,
        v_end=20,
    )

    # Virtual size is len(envelope) + 500 * 10
    virtual_size = len(envelope) + 5000
    top_boxes = scan_top_level_boxes(envelope, virtual_size)
    assert len(top_boxes) == 3
    moov_hdr = top_boxes[1]
    parsed_meta = parse_moov(envelope[moov_hdr.offset : moov_hdr.offset + moov_hdr.size])

    assert len(parsed_meta.tracks) == 1
    assert parsed_meta.video_track is not None
    assert parsed_meta.audio_track is None
    assert len(parsed_meta.video_track.stbl.stsz) == 10
    assert parsed_meta.video_track.stbl.stss[0] == 1


def test_chunk_restructuring_two_chunks():
    """Verify chunk restructuring when slice spans exactly two chunks (m_new == 2)."""
    sample_sizes = [100] * 30
    chunk_offsets = [10000, 20000, 30000]

    track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=sample_sizes,
        chunk_offsets=chunk_offsets,
        samples_per_chunk=10,
        sync_samples=[1, 5, 11],
    )

    # Slice samples 5 to 15 (sample 5 in chunk 1, sample 15 in chunk 2 -> m_new = 2)
    new_stbl, eff_offsets = restructure_sample_table(
        track=track,
        start_sample=5,
        end_sample=15,
        o_min=10000,
        h_offset=200,
    )

    assert len(eff_offsets) == 2
    # Chunk 1: 10 - 4 = 6 samples, skipped 4 * 100 = 400 bytes
    assert eff_offsets[0] == 10400
    # Chunk 2: 15 - 11 + 1 = 5 samples, starts at base offset 20000
    assert eff_offsets[1] == 20000

    assert len(new_stbl.stsz) == 11
    assert len(new_stbl.stsc) == 2
    assert new_stbl.stsc[0].first_chunk == 1 and new_stbl.stsc[0].samples_per_chunk == 6
    assert new_stbl.stsc[1].first_chunk == 2 and new_stbl.stsc[1].samples_per_chunk == 5


def test_serialize_boxes_version_1_large_values():
    """Verify version 1 serialization for mvhd, tkhd, and mdhd when values exceed 32 bits."""
    large_duration = 0x1_0000_0005  # > 4GB ticks
    large_time = 0x1_0000_0010

    # 1. mvhd v1
    mvhd = Mvhd(
        version=0,
        flags=0,
        creation_time=large_time,
        modification_time=large_time,
        timescale=1000,
        duration=large_duration,
    )
    raw_mvhd = serialize_mvhd(mvhd)
    assert raw_mvhd[8] == 1  # version 1 flag

    # 2. tkhd v1
    tkhd = Tkhd(
        version=0,
        flags=7,
        creation_time=large_time,
        modification_time=large_time,
        track_id=1,
        duration=large_duration,
        width=1920.0,
        height=1080.0,
    )
    raw_tkhd = serialize_tkhd(tkhd)
    assert raw_tkhd[8] == 1  # version 1 flag

    # 3. mdhd v1
    mdhd = Mdhd(
        version=0,
        flags=0,
        creation_time=large_time,
        modification_time=large_time,
        timescale=90000,
        duration=large_duration,
    )
    raw_mdhd = serialize_mdhd(mdhd)
    assert raw_mdhd[8] == 1  # version 1 flag


def test_synthesize_later_slice_audio_pts_alignment():
    """
    Regression Test:
    When slicing later parts of a media stream where a_start > len(sliced_audio),
    synthesize_faststart_envelope_header must correctly use the original audio track
    for PTS computation rather than indexing into the truncated new audio track.
    """
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[500] * 300,
        chunk_offsets=[10000 + i * 5000 for i in range(30)],
        samples_per_chunk=10,
        sample_delta=1000,
        timescale=1000,
        sync_samples=[1, 101, 201],
    )
    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[250] * 300,
        chunk_offsets=[200000 + i * 2500 for i in range(30)],
        samples_per_chunk=10,
        sample_delta=1000,
        timescale=1000,
    )

    mvhd = Mvhd(
        version=0,
        flags=0,
        creation_time=0,
        modification_time=0,
        timescale=1000,
        duration=300000,
        next_track_id=3,
    )

    metadata = Mp4Metadata(mvhd=mvhd, tracks=[v_track, a_track])
    ftyp_bytes = make_box(b"ftyp", b"isom\x00\x00\x02\x00isomiso2mp41")

    # Slice later segment: v_start=101, v_end=150 (50 samples)
    # a_start=101, a_end=140 (40 samples) -> note that 101 > 40!
    o_min, o_max, payload_len = compute_envelope_bounds(metadata, v_start=101, v_end=150, a_start=101, a_end=140)

    envelope = synthesize_faststart_envelope_header(
        metadata=metadata,
        ftyp_bytes=ftyp_bytes,
        v_start=101,
        v_end=150,
        a_start=101,
        a_end=140,
        o_min=o_min,
        o_max=o_max,
        payload_len=payload_len,
    )

    assert len(envelope) > 0
    top_boxes = scan_top_level_boxes(envelope, len(envelope) + payload_len)
    moov_box = next(b for b in top_boxes if b.box_type == b"moov")
    parsed = parse_moov(envelope[moov_box.offset : moov_box.offset + moov_box.size])

    assert parsed.video_track is not None
    assert parsed.audio_track is not None
    assert len(parsed.video_track.stbl.stsz) == 50
    assert len(parsed.audio_track.stbl.stsz) == 40
    assert parsed.video_track.stbl.stss[0] == 1


def test_real_mp4_multi_gop_slice_decode(tmp_path):
    """
    End-to-End Real MP4 Multi-GOP Test:
    Slice later GOP (keyframes 76..100) where a_start=141 is far beyond slice length (53 samples).
    Validates that FFmpeg can decode the synthesized MP4 without warnings or errors.
    """
    src_mp4 = str(tmp_path / "src_multigop.mp4")
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
        "-movflags",
        "+faststart",
        src_mp4,
    ]
    subprocess.run(cmd, check=True)

    with open(src_mp4, "rb") as f:
        file_bytes = f.read()

    top_boxes = scan_top_level_boxes(file_bytes, len(file_bytes))
    moov_box = next(b for b in top_boxes if b.box_type == b"moov")
    ftyp_box = next(b for b in top_boxes if b.box_type == b"ftyp")

    ftyp_bytes = file_bytes[ftyp_box.offset : ftyp_box.offset + ftyp_box.size]
    moov_bytes = file_bytes[moov_box.offset : moov_box.offset + moov_box.size]

    metadata = parse_moov(moov_bytes)
    v_track = metadata.video_track
    a_track = metadata.audio_track
    assert v_track is not None
    assert a_track is not None

    # Slice keyframe 76 to 100 (1 second slice at 3.0s)
    v_start = 76
    v_end = 100

    a_timescale = a_track.mdhd.timescale
    a_start = 1
    for i in range(1, len(a_track.stbl.stsz) + 1):
        _, pts = compute_sample_pts_dts(a_track, i)
        if pts / a_timescale <= (76 - 1) / 25.0:
            a_start = i
        else:
            break
    a_end = min(len(a_track.stbl.stsz), a_start + int(round(1.0 * a_timescale / 1024)) + 5)
    assert a_start > (a_end - a_start + 1)

    o_min, o_max, payload_len = compute_envelope_bounds(metadata, v_start, v_end, a_start, a_end)

    env_header = synthesize_faststart_envelope_header(
        metadata=metadata,
        ftyp_bytes=ftyp_bytes,
        v_start=v_start,
        v_end=v_end,
        a_start=a_start,
        a_end=a_end,
        o_min=o_min,
        o_max=o_max,
        payload_len=payload_len,
    )

    media_data = file_bytes[o_min:o_max]
    sliced_mp4 = str(tmp_path / "sliced_multigop.mp4")
    with open(sliced_mp4, "wb") as f:
        f.write(env_header)
        f.write(media_data)

    decode_cmd = ["ffmpeg", "-v", "error", "-i", sliced_mp4, "-f", "null", "-"]
    res = subprocess.run(decode_cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"FFmpeg decode failed: {res.stderr}"


def test_serialize_stsz_with_default_sample_size():
    """Verify that serialize_stsz writes compact 20-byte box when default_sample_size > 0."""
    stsz_arr = array.array("I", [512] * 1000)
    raw_stsz = serialize_stsz(stsz_arr, default_sample_size=512)

    # 12-byte full box header + 4-byte default_sample_size + 4-byte sample_count = 20 bytes
    assert len(raw_stsz) == 20
    uniform_size, count = struct.unpack_from(">II", raw_stsz, 12)
    assert uniform_size == 512
    assert count == 1000


def test_serialize_co64_and_stss_type_coercion():
    """Verify serialize_co64 and serialize_stss handle lists or alternative array typecodes."""
    # co64 with array('I')
    arr_u32 = array.array("I", [1000, 2000, 3000])
    raw_co64 = serialize_co64(arr_u32)
    assert len(raw_co64) == 12 + 4 + 3 * 8
    count = struct.unpack_from(">I", raw_co64, 12)[0]
    assert count == 3
    off1 = struct.unpack_from(">Q", raw_co64, 16)[0]
    assert off1 == 1000

    # co64 with list
    raw_co64_list = serialize_co64([1000, 2000, 3000])
    assert raw_co64_list == raw_co64

    # stss with list
    raw_stss = serialize_stss([1, 11, 21])
    assert len(raw_stss) == 12 + 4 + 3 * 4
    count_stss = struct.unpack_from(">I", raw_stss, 12)[0]
    assert count_stss == 3
    val1 = struct.unpack_from(">I", raw_stss, 16)[0]
    assert val1 == 1


def test_get_track_chunks_empty_handling():
    """Verify get_track_chunks returns empty list when stsc or chunk_offsets is empty."""
    track = create_synthetic_track(track_id=1, is_video=True)
    track.stbl.chunk_offsets = array.array("Q")
    assert get_track_chunks(track) == []

    track.stbl.stsc = []
    assert get_track_chunks(track) == []


def test_slice_rle_table_sbgp():
    """Verify Gate 2 slicing of SbgpEntry tables conforming to ISO/IEC 14496-12."""
    # Single entry spanning range
    single = [SbgpEntry(sample_count=100, group_description_index=1)]
    sliced = slice_rle_table(single, start_sample=11, end_sample=30)
    assert len(sliced) == 1
    assert sliced[0] == SbgpEntry(sample_count=20, group_description_index=1)

    # Empty entries list: defaults to all samples in group 0 without error
    empty_sliced = slice_sbgp_entries([], start_sample=1, end_sample=50)
    assert len(empty_sliced) == 1
    assert empty_sliced[0] == SbgpEntry(sample_count=50, group_description_index=0)

    # Partial coverage: ISO/IEC 14496-12 specifies subsequent samples have group 0
    # Available = 50 samples with group 1. Slicing [40, 70] (31 samples total: 11 in group 1, 20 in group 0)
    partial = [SbgpEntry(sample_count=50, group_description_index=1)]
    sliced_partial = slice_rle_table(partial, start_sample=40, end_sample=70)
    assert len(sliced_partial) == 2
    assert sliced_partial[0] == SbgpEntry(sample_count=11, group_description_index=1)
    assert sliced_partial[1] == SbgpEntry(sample_count=20, group_description_index=0)
    assert sum(e.sample_count for e in sliced_partial) == 31

    # Slicing completely beyond available entries -> returns single group 0 entry
    sliced_beyond = slice_rle_table(partial, start_sample=60, end_sample=100)
    assert len(sliced_beyond) == 1
    assert sliced_beyond[0] == SbgpEntry(sample_count=41, group_description_index=0)

    # Multiple entries
    multi = [
        SbgpEntry(sample_count=50, group_description_index=1),
        SbgpEntry(sample_count=50, group_description_index=2),
        SbgpEntry(sample_count=50, group_description_index=3),
    ]
    # Slicing from sample 25 to 125 (total 101 samples)
    sliced_multi = slice_rle_table(multi, start_sample=25, end_sample=125)
    assert len(sliced_multi) == 3
    # head: 50 - 25 + 1 = 26
    assert sliced_multi[0] == SbgpEntry(sample_count=26, group_description_index=1)
    # mid: 50
    assert sliced_multi[1] == SbgpEntry(sample_count=50, group_description_index=2)
    # tail: 125 - 100 = 25
    assert sliced_multi[2] == SbgpEntry(sample_count=25, group_description_index=3)
    assert sum(e.sample_count for e in sliced_multi) == 101

    # Run merging when slicing produces adjacent runs with identical group_description_index
    runs_with_group0 = [
        SbgpEntry(sample_count=30, group_description_index=1),
        SbgpEntry(sample_count=20, group_description_index=0),
    ]
    # Slicing [25, 70]: head gets 6 of group 1, then 20 of group 0, and 20 padded group 0 -> merged into 40 of group 0
    sliced_runs = slice_rle_table(runs_with_group0, start_sample=25, end_sample=70)
    assert len(sliced_runs) == 2
    assert sliced_runs[0] == SbgpEntry(sample_count=6, group_description_index=1)
    assert sliced_runs[1] == SbgpEntry(sample_count=40, group_description_index=0)
    assert sum(e.sample_count for e in sliced_runs) == 46


def test_get_sample_byte_offset_uniform_stsz():
    """Verify get_sample_byte_offset uses O(1) multiplication with UniformStszTable."""
    track = create_synthetic_track(track_id=1, is_video=False)
    track.stbl.stsz = UniformStszTable(uniform_size=4, sample_count=1000)
    track.stbl.default_sample_size = 4
    track.stbl.stsc = [StscEntry(first_chunk=1, samples_per_chunk=100, sample_description_index=1)]
    track.stbl.chunk_offsets = array.array("Q", [1000, 2000, 3000, 4000])

    # Sample 1: first sample in chunk 1 (offset 1000)
    s_start, s_end = get_sample_byte_offset(track, sample_index=1)
    assert s_start == 1000
    assert s_end == 1004

    # Sample 105: 5th sample in chunk 2 (chunk 2 base = 2000, skipped 4 samples = 16B)
    s_start, s_end = get_sample_byte_offset(track, sample_index=105)
    assert s_start == 2000 + 4 * 4
    assert s_end == 2000 + 4 * 4 + 4


def test_restructure_sample_table_uniform_stsz():
    """Verify restructure_sample_table operates in O(1) and produces sliced UniformStszTable."""
    track = create_synthetic_track(track_id=1, is_video=False)
    track.stbl.stsz = UniformStszTable(uniform_size=4, sample_count=1000)
    track.stbl.default_sample_size = 4
    track.stbl.stts = [SttsEntry(sample_count=1000, sample_delta=1000)]
    track.stbl.stsc = [StscEntry(first_chunk=1, samples_per_chunk=100, sample_description_index=1)]
    track.stbl.chunk_offsets = array.array("Q", [1000, 2000, 3000, 4000])

    # Slicing samples 50 to 150 (total 101 samples)
    new_stbl, eff_offsets = restructure_sample_table(track, start_sample=50, end_sample=150, o_min=1000, h_offset=0)
    assert isinstance(new_stbl.stsz, UniformStszTable)
    assert len(new_stbl.stsz) == 101
    assert new_stbl.stsz.uniform_size == 4
    assert new_stbl.default_sample_size == 4

    # First chunk offset: sample 50 is in chunk 1 (base 1000, skipped 49 samples * 4 = 196)
    assert eff_offsets[0] == 1000 + 49 * 4
    # Middle chunk: chunk 2 starts at 2000
    assert eff_offsets[1] == 2000


def test_restructure_sample_table_with_sgpd_and_sbgp():
    """Verify sgpd is preserved and sbgp is RLE-sliced in restructure_sample_table and serialize_trak."""
    track = create_synthetic_track(track_id=2, is_video=False)
    sgpd_box = make_full_box(b"sgpd", 1, 0, b"roll" + struct.pack(">IIh", 2, 1, -1))
    sbgp_entry = SbgpEntry(sample_count=len(track.stbl.stsz), group_description_index=1)
    sbgp_table = SbgpTable(version=0, flags=0, grouping_type=b"roll", entries=[sbgp_entry])

    track.stbl.sgpd_boxes = [sgpd_box]
    track.stbl.sbgp_tables = [sbgp_table]
    track.stbl.sbgp_boxes = [serialize_sbgp(sbgp_table)]

    # Slicing samples 11 to 30 (20 samples)
    new_stbl, _ = restructure_sample_table(track, start_sample=11, end_sample=30, o_min=1000, h_offset=0)
    assert len(new_stbl.sgpd_boxes) == 1
    assert new_stbl.sgpd_boxes[0] == sgpd_box
    assert len(new_stbl.sbgp_tables) == 1
    assert len(new_stbl.sbgp_tables[0].entries) == 1
    assert new_stbl.sbgp_tables[0].entries[0].sample_count == 20
    assert new_stbl.sbgp_tables[0].entries[0].group_description_index == 1

    # Verify serialization includes sgpd and sbgp
    new_track = Track(
        track_id=track.track_id,
        tkhd=track.tkhd,
        mdhd=track.mdhd,
        hdlr=track.hdlr,
        stbl=new_stbl,
        edts=None,
    )
    raw_trak = serialize_trak(new_track)
    assert b"sgpd" in raw_trak
    assert b"sbgp" in raw_trak


def test_serialize_stsz_uniform_stsz_table():
    """Verify serialize_stsz emits compact 20-byte box when passed UniformStszTable."""
    tbl = UniformStszTable(uniform_size=512, sample_count=350_000_000)
    raw_stsz = serialize_stsz(tbl)
    assert len(raw_stsz) == 20
    uniform_size, count = struct.unpack_from(">II", raw_stsz, 12)
    assert uniform_size == 512
    assert count == 350_000_000


def test_serialize_sbgp_validation():
    """Verify serialize_sbgp handles string grouping_type and validates versions and length."""
    # Version 0 with string grouping_type
    sbgp = SbgpTable(version=0, flags=0, grouping_type="roll", entries=[SbgpEntry(10, 1)])
    raw = serialize_sbgp(sbgp)
    assert len(raw) == 28
    assert raw[4:8] == b"sbgp"
    assert raw[12:16] == b"roll"

    # Version 1 with grouping_type_parameter
    sbgp_v1 = SbgpTable(
        version=1, flags=0, grouping_type=b"roll", entries=[SbgpEntry(10, 1)], grouping_type_parameter=99
    )
    raw_v1 = serialize_sbgp(sbgp_v1)
    assert len(raw_v1) == 32
    param = struct.unpack_from(">I", raw_v1, 16)[0]
    assert param == 99

    # Invalid version
    with pytest.raises(IsoBmffError, match="Unsupported sbgp version"):
        serialize_sbgp(SbgpTable(version=2, flags=0, grouping_type=b"roll", entries=[]))

    # Invalid grouping_type length
    with pytest.raises(IsoBmffError, match="grouping_type must be 4 bytes"):
        serialize_sbgp(SbgpTable(version=0, flags=0, grouping_type=b"toolong", entries=[]))


def test_restructure_sample_table_sbgp_partial_coverage():
    """Verify restructure_sample_table succeeds and pads group 0 when sbgp covers fewer samples than track."""
    track = create_synthetic_track(track_id=2, is_video=False, sample_sizes=[500] * 500)
    # Track has 500 samples, but sbgp only covers first 100 samples with pre-roll
    sbgp_entry = SbgpEntry(sample_count=100, group_description_index=1)
    sbgp_table = SbgpTable(version=0, flags=0, grouping_type=b"roll", entries=[sbgp_entry])
    track.stbl.sbgp_tables = [sbgp_table]
    track.stbl.sbgp_boxes = [serialize_sbgp(sbgp_table)]

    # Slicing samples 200 to 300 (which is completely past the 100 covered samples)
    new_stbl, _ = restructure_sample_table(track, start_sample=200, end_sample=300, o_min=1000, h_offset=0)
    assert len(new_stbl.sbgp_tables) == 1
    table = new_stbl.sbgp_tables[0]
    assert len(table.entries) == 1
    # Samples 200..300 (101 samples) should all have implicit group 0
    assert table.entries[0].sample_count == 101
    assert table.entries[0].group_description_index == 0


def test_estimate_faststart_header_size_uniform_stsz():
    """Verify estimate_faststart_header_size uses 20 bytes for UniformStszTable without exploding."""
    from app.core.vsse_provider import estimate_faststart_header_size

    v_track = create_synthetic_track(track_id=1, is_video=True)
    a_track = create_synthetic_track(track_id=2, is_video=False)

    # Simulate 350M sample uniform audio track (e.g. QuickTime lpcm)
    n = 350_000_000
    a_track.stbl.stsz = UniformStszTable(uniform_size=4, sample_count=n)
    a_track.stbl.default_sample_size = 4

    meta = Mp4Metadata(
        mvhd=Mvhd(
            version=0,
            flags=0,
            creation_time=0,
            modification_time=0,
            timescale=1000,
            duration=10000,
            rate=1.0,
            volume=1.0,
            matrix=[0] * 9,
            next_track_id=3,
        ),
        tracks=[v_track, a_track],
    )

    # Slicing 10M audio samples
    h_est = estimate_faststart_header_size(meta, v_start=1, v_end=100, a_start=1, a_end=10_000_000)
    # With bug: h_est would be > 40 MiB (10M * 4 bytes). With fix: h_est is < 10 KiB
    assert h_est < 10240, f"Header size {h_est} unexpectedly exceeded 10 KiB"


def test_synthesize_faststart_envelope_header_sidecar_dual_region_offsets():
    """
    Phase 4 (WP-4): Test faststart envelope header synthesis with sidecar audio slice
    supporting dual-region offsets:
    1. In-memory audio prefix: co64_A[k] = H_box + (EffOffset_A[k] - a_min)
    2. Remote video range: co64_V[k] = H_box + A_len + (EffOffset_V[k] - v_min)
    3. Exact length verification: len(header) == H_box + A_len.
    4. 64-bit mdat header length matches declared A_len + payload_len.
    """
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[1000] * 30,
        chunk_offsets=[50000, 70000, 90000],
        samples_per_chunk=10,
        sync_samples=[1, 11, 21],
    )
    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[150] * 30,
        chunk_offsets=[0, 1500, 3000],
        samples_per_chunk=10,
    )
    meta = Mp4Metadata(
        mvhd=Mvhd(
            version=0,
            flags=0,
            creation_time=0,
            modification_time=0,
            timescale=1000,
            duration=30000,
            rate=65536,
            volume=256,
            matrix=[0] * 9,
            next_track_id=3,
        ),
        tracks=[v_track, a_track],
    )

    ftyp_box = b"\x00\x00\x00\x20ftypisom\x00\x00\x02\x00isomiso2mp41mp42"
    sidecar_audio_slice = b"A" * 3000
    a_len = len(sidecar_audio_slice)

    v_omin, v_omax, v_payload = compute_envelope_bounds(meta, 1, 20, None, None)
    assert v_omin == 50000
    assert v_omax == 80000
    assert v_payload == 30000

    header = synthesize_faststart_envelope_header(
        metadata=meta,
        ftyp_bytes=ftyp_box,
        v_start=1,
        v_end=20,
        a_start=1,
        a_end=20,
        o_min=v_omin,
        o_max=v_omax,
        payload_len=v_payload,
        sidecar_audio_slice=sidecar_audio_slice,
        a_o_min=0,
    )

    # 1. Header must end with sidecar_audio_slice bytes
    assert header.endswith(sidecar_audio_slice)

    # 2. Parse the moov and mdat boxes from header
    virtual_file_size = len(header) + v_payload
    top_boxes = scan_top_level_boxes(header, virtual_file_size)
    assert len(top_boxes) >= 3
    assert [b.box_type for b in top_boxes[:3]] == [b"ftyp", b"moov", b"mdat"]
    moov_b = top_boxes[1]

    h_box = len(ftyp_box) + moov_b.size + 16
    assert len(header) == h_box + a_len

    parsed_meta = parse_moov(header[moov_b.offset : moov_b.offset + moov_b.size])
    parsed_v = parsed_meta.video_track
    parsed_a = parsed_meta.audio_track
    assert parsed_v is not None
    assert parsed_a is not None

    # 3. Check audio chunk offsets: must point to in-memory prefix [H_box, H_box + A_len)
    assert len(parsed_a.stbl.chunk_offsets) == 2
    for k, off in enumerate(parsed_a.stbl.chunk_offsets):
        expected_a_off = h_box + (k * 1500)
        assert off == expected_a_off
        assert h_box <= off < h_box + a_len

    # 4. Check video chunk offsets: must point to remote range starting at H_box + A_len
    assert len(parsed_v.stbl.chunk_offsets) == 2
    for k, off in enumerate(parsed_v.stbl.chunk_offsets):
        expected_v_off = h_box + a_len + (k * 20000)
        assert off == expected_v_off
        assert off >= h_box + a_len

    # 5. Check 64-bit mdat header declared payload length:
    mdat_header_bytes = header[h_box - 16 : h_box]
    mdat_total_len = struct.unpack(">Q", mdat_header_bytes[8:16])[0]
    assert mdat_total_len == 16 + a_len + v_payload


def test_synthesize_faststart_envelope_header_sidecar_edge_cases():
    """Verify edge cases for synthesize_faststart_envelope_header sidecar mode:
    - memoryview input for sidecar_audio_slice
    - inferred a_o_min (a_o_min=None)
    - inferred o_min and payload_len (None)
    - empty sidecar_audio_slice (b"")
    """
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[500] * 20,
        chunk_offsets=[10000, 15000],
        samples_per_chunk=10,
        sync_samples=[1, 11],
    )
    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[100] * 20,
        chunk_offsets=[200, 1200],
        samples_per_chunk=10,
    )
    meta = Mp4Metadata(
        mvhd=Mvhd(
            version=0,
            flags=0,
            creation_time=0,
            modification_time=0,
            timescale=1000,
            duration=20000,
            rate=65536,
            volume=256,
            matrix=[0] * 9,
            next_track_id=3,
        ),
        tracks=[v_track, a_track],
    )
    ftyp_box = b"\x00\x00\x00\x20ftypisom\x00\x00\x02\x00isomiso2mp41mp42"

    # 1. memoryview input
    raw_slice = bytearray(b"M" * 2000)
    mv_slice = memoryview(raw_slice)
    header_mv = synthesize_faststart_envelope_header(
        metadata=meta,
        ftyp_bytes=ftyp_box,
        v_start=1,
        v_end=10,
        a_start=1,
        a_end=10,
        sidecar_audio_slice=mv_slice,
    )
    assert header_mv.endswith(b"M" * 2000)

    # 2. Inferred bounds (o_min=None, payload_len=None, a_o_min=None)
    header_inferred = synthesize_faststart_envelope_header(
        metadata=meta,
        ftyp_bytes=ftyp_box,
        v_start=1,
        v_end=10,
        a_start=1,
        a_end=10,
        sidecar_audio_slice=b"AUDIO1000",
    )
    assert header_inferred.endswith(b"AUDIO1000")

    # 3. Empty sidecar_audio_slice (b"")
    header_empty = synthesize_faststart_envelope_header(
        metadata=meta,
        ftyp_bytes=ftyp_box,
        v_start=1,
        v_end=10,
        a_start=1,
        a_end=10,
        sidecar_audio_slice=b"",
    )
    assert isinstance(header_empty, bytes)
    assert len(header_empty) > 0


def test_synthesize_faststart_envelope_header_sidecar_validation_errors():
    """Verify validation errors for invalid offset parameters and video-only sidecar usage."""
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[500] * 10,
        chunk_offsets=[10000],
        samples_per_chunk=10,
        sync_samples=[1],
    )
    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[100] * 10,
        chunk_offsets=[200],
        samples_per_chunk=10,
    )
    meta = Mp4Metadata(
        mvhd=Mvhd(
            version=0,
            flags=0,
            creation_time=0,
            modification_time=0,
            timescale=1000,
            duration=10000,
            rate=65536,
            volume=256,
            matrix=[0] * 9,
            next_track_id=3,
        ),
        tracks=[v_track, a_track],
    )
    ftyp_box = b"\x00\x00\x00\x20ftypisom\x00\x00\x02\x00isomiso2mp41mp42"

    # 1. Non-empty sidecar_audio_slice when a_start is None (video-only envelope)
    with pytest.raises(ValueError, match="sidecar_audio_slice cannot be non-empty when audio track is omitted"):
        synthesize_faststart_envelope_header(
            metadata=meta,
            ftyp_bytes=ftyp_box,
            v_start=1,
            v_end=10,
            a_start=None,
            a_end=None,
            sidecar_audio_slice=b"LEAKED_AUDIO",
        )

    # 2. Negative o_min
    with pytest.raises(ValueError, match="o_min cannot be negative"):
        synthesize_faststart_envelope_header(
            metadata=meta,
            ftyp_bytes=ftyp_box,
            v_start=1,
            v_end=10,
            o_min=-100,
        )

    # 3. Negative a_o_min
    with pytest.raises(ValueError, match="a_o_min cannot be negative"):
        synthesize_faststart_envelope_header(
            metadata=meta,
            ftyp_bytes=ftyp_box,
            v_start=1,
            v_end=10,
            a_start=1,
            a_end=10,
            sidecar_audio_slice=b"AUDIO",
            a_o_min=-5,
        )

    # 4. a_o_min exceeds first audio chunk offset
    with pytest.raises(ValueError, match="cannot exceed first audio chunk offset"):
        synthesize_faststart_envelope_header(
            metadata=meta,
            ftyp_bytes=ftyp_box,
            v_start=1,
            v_end=10,
            a_start=1,
            a_end=10,
            sidecar_audio_slice=b"AUDIO",
            a_o_min=500,  # First audio chunk starts at 200
        )

    # 5. o_min exceeds first video chunk offset
    with pytest.raises(ValueError, match="cannot exceed first video chunk offset"):
        synthesize_faststart_envelope_header(
            metadata=meta,
            ftyp_bytes=ftyp_box,
            v_start=1,
            v_end=10,
            o_min=50000,  # First video chunk starts at 10000
        )


# -----------------------------------------------------------------------------
# Dual-Tier Adaptive AV Alignment Tests (Tier A Empty Edit + Tier B Source mv)
# -----------------------------------------------------------------------------


def test_audio_alignment_negative_delta_empty_edit_tier_a():
    """
    Tier A: Verify that when audio starts later than video (effective_delta < 0),
    configure_audio_elst builds a 2-entry elst with an Empty Edit (media_time == -1)
    followed by a normal media edit, and synchronize_durations preserves mdhd/tkhd/mvhd invariants.
    """
    v_track = create_synthetic_track(
        track_id=1, is_video=True, sample_sizes=[1000] * 20, sample_delta=1000, timescale=1000
    )
    a_track = create_synthetic_track(
        track_id=2, is_video=False, sample_sizes=[250] * 500, sample_delta=1024, timescale=48000
    )
    # Simulate source audio having a 0.5s leading Empty Edit (dwell=500 in movie_timescale=1000)
    orig_a = create_synthetic_track(
        track_id=2, is_video=False, sample_sizes=[250] * 500, sample_delta=1024, timescale=48000
    )
    orig_a.edts = Edts(
        elst_version=0,
        elst_flags=0,
        entries=[
            ElstEntry(segment_duration=500, media_time=-1, media_rate_integer=1, media_rate_fraction=0),
            ElstEntry(segment_duration=10667, media_time=1024, media_rate_integer=1, media_rate_fraction=0),
        ],
    )

    # At v_start=1 (0.0s), a_start=1 (0.0s):
    # a_empty_sec = 0.5s, ma = 1024 (0.021333s) -> c = 0.021333 - 0.5 = -0.478667s < 0
    configure_audio_elst(
        audio_track=a_track,
        video_track=v_track,
        v_start=1,
        a_start=1,
        movie_timescale=1000,
        original_audio_track=orig_a,
        original_video_track=v_track,
    )

    assert a_track.edts is not None
    assert len(a_track.edts.entries) == 2
    empty_e, media_e = a_track.edts.entries
    assert empty_e.media_time == -1
    assert empty_e.segment_duration == round(abs(1024 / 48000 - 0.5) * 1000)  # 479 ticks
    assert media_e.media_time == 0

    mvhd = Mvhd(version=0, flags=0, creation_time=0, modification_time=0, timescale=1000, duration=0)
    synchronize_durations(mvhd, [v_track, a_track], movie_timescale=1000)

    # mdhd.duration must strictly equal sum(stts) without empty_dur
    assert a_track.mdhd.duration == 500 * 1024
    # tkhd.duration must strictly equal sum(elst.segment_duration)
    assert a_track.tkhd.duration == empty_e.segment_duration + media_e.segment_duration
    assert mvhd.duration == max(v_track.tkhd.duration, a_track.tkhd.duration)


def test_source_video_mv_offset_absorption_tier_b():
    """
    Tier B: Verify that when source video track has a positive media_time offset mv > ctts_v0
    on the first volume (v_start == 1), synthesize_faststart_envelope_header absorbs mv into
    the synthesized video elst rather than transferring a false negative delta to the audio track.
    """
    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[500] * 30,
        chunk_offsets=[10000, 15000, 20000],
        samples_per_chunk=10,
        sample_delta=1000,
        timescale=1000,
        sync_samples=[1, 11, 21],
    )
    # Source video has mv = 2000 (2.0s trim offset) while ctts_v0 = 0
    v_track.edts = Edts(
        elst_version=0,
        elst_flags=0,
        entries=[ElstEntry(segment_duration=28000, media_time=2000, media_rate_integer=1, media_rate_fraction=0)],
    )

    a_track = create_synthetic_track(
        track_id=2,
        is_video=False,
        sample_sizes=[200] * 30,
        chunk_offsets=[30000, 32000, 34000],
        samples_per_chunk=10,
        sample_delta=1000,
        timescale=1000,
    )
    mvhd = Mvhd(version=0, flags=0, creation_time=0, modification_time=0, timescale=1000, duration=28000)
    meta = Mp4Metadata(mvhd=mvhd, tracks=[v_track, a_track])
    ftyp_box = b"\x00\x00\x00\x20ftypisom\x00\x00\x02\x00isomiso2mp41mp42"

    # Without Tier B, c = 0 - 2.0 = -2.0s at v_start=1, a_start=1 would have triggered Gate 5 or
    # inserted a false 2.0s audio delay. With Tier B, video absorbs mv=2000 and audio has media_time=0.
    header = synthesize_faststart_envelope_header(
        metadata=meta,
        ftyp_bytes=ftyp_box,
        v_start=1,
        v_end=20,
        a_start=1,
        a_end=20,
    )

    _, _, payload_len = compute_envelope_bounds(meta, 1, 20, 1, 20)
    top_boxes = scan_top_level_boxes(header, len(header) + payload_len)
    moov_b = next(b for b in top_boxes if b.box_type == b"moov")
    parsed_meta = parse_moov(header[moov_b.offset : moov_b.offset + moov_b.size])

    parsed_v = parsed_meta.video_track
    parsed_a = parsed_meta.audio_track
    assert parsed_v is not None and parsed_v.edts is not None
    assert len(parsed_v.edts.entries) == 1
    assert parsed_v.edts.entries[0].media_time == 2000
    assert parsed_v.edts.entries[0].segment_duration == 18000  # (20000 - 2000)

    assert parsed_a is not None and parsed_a.edts is not None
    assert len(parsed_a.edts.entries) == 1
    assert parsed_a.edts.entries[0].media_time == 0


def test_negative_delta_sub_tick_rounds_to_single_entry():
    """
    Edge Case: When effective_delta < 0 but its magnitude is smaller than 0.5 movie_timescale tick
    (so empty_dur rounds to 0), configure_audio_elst must emit a single normal ElstEntry rather
    than a 0-duration Empty Edit (protecting ExoPlayer / AVPlayer demuxers).
    """
    a_track = create_synthetic_track(track_id=2, is_video=False, sample_delta=1000, timescale=1_000_000)

    # Video sample 1: PTS = 0.0s
    # Audio sample 1 with ma = -200 ticks (-0.0002s = -0.2ms):
    # effective_delta = -0.0002s -> in movie_timescale=1000, round(0.0002 * 1000) == 0 ticks
    orig_v = create_synthetic_track(track_id=1, is_video=True, sample_delta=1000, timescale=1_000_000)
    orig_v.edts = Edts(
        elst_version=0,
        elst_flags=0,
        entries=[ElstEntry(segment_duration=10000, media_time=200, media_rate_integer=1, media_rate_fraction=0)],
    )
    # Set v_start=2 so Tier B v_start==1 absorption does not apply, testing pure sub-tick negative delta
    orig_v.stbl.stts = [SttsEntry(sample_count=30, sample_delta=0)]
    configure_audio_elst(
        audio_track=a_track,
        video_track=orig_v,
        v_start=2,
        a_start=1,
        movie_timescale=1000,
        original_audio_track=a_track,
        original_video_track=orig_v,
    )

    assert a_track.edts is not None
    assert len(a_track.edts.entries) == 1
    assert a_track.edts.entries[0].media_time == 0


def test_negative_delta_elst_v1_64bit_serialization_roundtrip():
    """
    Edge Case: Verify that when segment_duration > 0xFFFFFFFF or media_time > 0x7FFFFFFF in a
    2-entry Empty Edit elst, elst_version is promoted to 1 and media_time=-1 is packed as
    64-bit 0xFFFFFFFFFFFFFFFF and parsed back as -1.
    """
    v_track = create_synthetic_track(track_id=1, is_video=True, sample_delta=1000, timescale=1000)
    a_track = create_synthetic_track(track_id=2, is_video=False, sample_delta=1000, timescale=1000)
    # Huge audio duration forcing segment_duration > 0xFFFFFFFF in movie_timescale
    a_track.stbl.stts = [SttsEntry(sample_count=5_000_000, sample_delta=1000)]

    configure_audio_elst(
        audio_track=a_track,
        video_track=v_track,
        v_start=1,  # 0.0s
        a_start=5,  # 4.0s -> effective_delta = -4.0s
        movie_timescale=1000,
    )

    assert a_track.edts is not None
    assert a_track.edts.elst_version == 1
    assert len(a_track.edts.entries) == 2
    assert a_track.edts.entries[0].media_time == -1
    assert a_track.edts.entries[0].segment_duration == 4000
    assert a_track.edts.entries[1].segment_duration == 5_000_000_000
    assert a_track.edts.entries[1].segment_duration > 0xFFFFFFFF

    # Roundtrip serialize moov and parse back
    mvhd = Mvhd(version=1, flags=0, creation_time=0, modification_time=0, timescale=1000, duration=5_000_004_000)
    synchronize_durations(mvhd, [v_track, a_track], movie_timescale=1000)
    moov_bytes = serialize_moov(Mp4Metadata(mvhd=mvhd, tracks=[v_track, a_track]))
    parsed = parse_moov(moov_bytes)

    parsed_a = parsed.audio_track
    assert parsed_a is not None and parsed_a.edts is not None
    assert parsed_a.edts.elst_version == 1
    assert len(parsed_a.edts.entries) == 2
    assert parsed_a.edts.entries[0].media_time == -1
    assert parsed_a.edts.entries[0].segment_duration == 4000
    assert parsed_a.edts.entries[1].media_time == 0
    assert parsed_a.edts.entries[1].segment_duration == 5_000_000_000


def test_real_mp4_negative_audio_delay_ffmpeg_xerror_decode(tmp_path):
    """
    End-to-End Golden Verification (Tier A + Tier B):
    Generate a real MP4 with B-frames (video elst mv > 0) and delayed audio start (-itsoffset 0.4s),
    synthesize virtual volume via synthesize_faststart_envelope_header, and verify 100% clean
    FFmpeg full bitstream decode with -xerror.
    """
    src_mp4 = str(tmp_path / "delayed_audio_src.mp4")
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
        "-itsoffset",
        "0.4",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=1000:sample_rate=48000",
        "-t",
        "4.0",
        "-c:v",
        "libx264",
        "-g",
        "25",
        "-bf",
        "2",
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

    with open(src_mp4, "rb") as f:
        file_bytes = f.read()

    top_boxes = scan_top_level_boxes(file_bytes, len(file_bytes))
    moov_box = next(b for b in top_boxes if b.box_type == b"moov")
    ftyp_box = next(b for b in top_boxes if b.box_type == b"ftyp")
    ftyp_bytes = file_bytes[ftyp_box.offset : ftyp_box.offset + ftyp_box.size]
    moov_bytes = file_bytes[moov_box.offset : moov_box.offset + moov_box.size]

    metadata = parse_moov(moov_bytes)
    v_track = metadata.video_track
    a_track = metadata.audio_track
    assert v_track is not None and a_track is not None

    # Force a negative delta scenario by slicing video from keyframe 1 (0.0s) and audio from sample 20 (~0.42s)
    v_start, v_end = 1, 50
    a_start, a_end = 20, min(len(a_track.stbl.stsz), 90)
    o_min, o_max, payload_len = compute_envelope_bounds(metadata, v_start, v_end, a_start, a_end)

    env_header = synthesize_faststart_envelope_header(
        metadata=metadata,
        ftyp_bytes=ftyp_bytes,
        v_start=v_start,
        v_end=v_end,
        a_start=a_start,
        a_end=a_end,
        o_min=o_min,
        o_max=o_max,
        payload_len=payload_len,
    )

    sliced_mp4 = str(tmp_path / "sliced_neg_delta.mp4")
    with open(sliced_mp4, "wb") as f:
        f.write(env_header)
        f.write(file_bytes[o_min:o_max])

    # Verify synthesized moov contains the 2-entry Empty Edit elst on the audio track
    s_boxes = scan_top_level_boxes(env_header, len(env_header) + payload_len)
    s_moov = next(b for b in s_boxes if b.box_type == b"moov")
    s_meta = parse_moov(env_header[s_moov.offset : s_moov.offset + s_moov.size])
    assert s_meta.audio_track is not None and s_meta.audio_track.edts is not None
    assert len(s_meta.audio_track.edts.entries) == 2
    assert s_meta.audio_track.edts.entries[0].media_time == -1
    assert s_meta.audio_track.edts.entries[0].segment_duration > 0

    # Strict FFmpeg decode verification with -xerror
    decode_cmd = ["ffmpeg", "-v", "error", "-xerror", "-i", sliced_mp4, "-f", "null", "-"]
    res = subprocess.run(decode_cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"FFmpeg decode failed: {res.stderr}"
    assert res.stderr == ""


def test_synthesize_faststart_envelope_header_rejects_non_monotonic_chunks():
    """Verify non-monotonic middle chunks outside [o_min, o_max) raise UnsupportedMp4LayoutError."""
    from app.core.iso_bmff.exceptions import UnsupportedMp4LayoutError

    # Case 1: middle chunk precedes o_min -> must raise UnsupportedMp4LayoutError (not OverflowError)
    v_track_before = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[500] * 30,
        chunk_offsets=[10000, 5000, 20000],
        samples_per_chunk=10,
        sync_samples=[1, 11, 21],
    )
    meta_before = Mp4Metadata(
        mvhd=Mvhd(
            version=0,
            flags=0,
            creation_time=0,
            modification_time=0,
            timescale=1000,
            duration=30000,
            rate=65536,
            volume=256,
            matrix=[0] * 9,
            next_track_id=2,
        ),
        tracks=[v_track_before],
    )
    ftyp_box = b"\x00\x00\x00\x20ftypisom\x00\x00\x02\x00isomiso2mp41mp42"
    with pytest.raises(UnsupportedMp4LayoutError, match="Non-monotonic"):
        synthesize_faststart_envelope_header(
            metadata=meta_before,
            ftyp_bytes=ftyp_box,
            v_start=1,
            v_end=30,
        )

    # Case 2: middle chunk lies beyond o_max -> must raise UnsupportedMp4LayoutError
    v_track_after = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[500] * 30,
        chunk_offsets=[10000, 50000, 20000],
        samples_per_chunk=10,
        sync_samples=[1, 11, 21],
    )
    meta_after = Mp4Metadata(
        mvhd=meta_before.mvhd,
        tracks=[v_track_after],
    )
    with pytest.raises(UnsupportedMp4LayoutError, match="Non-monotonic"):
        synthesize_faststart_envelope_header(
            metadata=meta_after,
            ftyp_bytes=ftyp_box,
            v_start=1,
            v_end=30,
        )


@pytest.mark.parametrize("uniform", [False, True])
@pytest.mark.parametrize("sidecar", [None, b""])
def test_envelope_rejects_chunk_end_overrun(uniform, sidecar):
    from app.core.iso_bmff.exceptions import UnsupportedMp4LayoutError

    track = create_synthetic_track(
        sample_sizes=[500] * 30,
        chunk_offsets=[10000, 24000, 20000],
        samples_per_chunk=10,
        sync_samples=[1, 11, 21],
    )
    if uniform:
        track.stbl.stsz = UniformStszTable(uniform_size=500, sample_count=30)
        track.stbl.default_sample_size = 500
    meta = Mp4Metadata(mvhd=Mvhd(0, 0, 0, 0, 1000, 30000, 65536, 256, [0] * 9, 2), tracks=[track])
    with pytest.raises(UnsupportedMp4LayoutError, match="chunk span"):
        synthesize_faststart_envelope_header(
            meta,
            b"",
            1,
            30,
            sidecar_audio_slice=sidecar,
        )
    # Exact equality at the end of the selected final chunk remains valid.
    track.stbl.chunk_offsets[1] = 15000
    assert synthesize_faststart_envelope_header(meta, b"", 1, 30, sidecar_audio_slice=sidecar)


def test_configure_video_elst_uses_presentation_span_with_constant_ctts():
    """#6: 10 个 100-tick 样本带恒定 ctts=100 时, 展现区间为 [100, 1100], segment_duration 应为 1000 而非 900。"""
    from app.core.iso_bmff.box_synthesizer import configure_video_elst

    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[500] * 10,
        chunk_offsets=[10000],
        samples_per_chunk=10,
        sample_delta=100,
        timescale=1000,
        sync_samples=[1],
    )
    v_track.stbl.ctts = [CttsEntry(sample_count=10, sample_offset=100)]

    configure_video_elst(v_track, v_start=1, movie_timescale=1000)

    assert v_track.edts is not None
    assert len(v_track.edts.entries) == 1
    assert v_track.edts.entries[0].media_time == 100
    assert v_track.edts.entries[0].segment_duration == 1000
    assert v_track.tkhd.duration == 1000


def test_synthesize_remaps_self_contained_data_ref_index_and_rejects_external():
    """#7: self-contained sample description data_ref_index=2 重写为 1; 非 self-contained 抛 UnsupportedMp4LayoutError。"""
    from app.core.iso_bmff.exceptions import UnsupportedMp4LayoutError

    v_track = create_synthetic_track(
        track_id=1,
        is_video=True,
        sample_sizes=[500] * 10,
        chunk_offsets=[10000],
        samples_per_chunk=10,
        sample_delta=100,
        timescale=1000,
        sync_samples=[1],
    )
    e1 = SampleEntry(format=b"avc1", data_ref_index=1, codec="avc1", width=1920, height=1080)
    e2 = SampleEntry(format=b"avc1", data_ref_index=2, codec="avc1", width=1920, height=1080)
    v_track.stbl.stsd = Stsd(version=0, flags=0, entry_count=2, entries=[e1, e2], dref_self_contained=(False, True))
    v_track.stbl.stsc = [StscEntry(first_chunk=1, samples_per_chunk=10, sample_description_index=2)]

    meta = Mp4Metadata(mvhd=Mvhd(0, 0, 0, 0, 1000, 1000), tracks=[v_track])
    header = synthesize_faststart_envelope_header(meta, b"", 1, 10)
    top = scan_top_level_boxes(header, len(header) + 5000)
    moov_b = next(b for b in top if b.box_type == b"moov")
    parsed = parse_moov(header[moov_b.offset : moov_b.offset + moov_b.size])
    assert parsed.video_track is not None
    assert all(e.data_ref_index == 1 for e in parsed.video_track.stbl.stsd.entries)

    # 引用非 self-contained 条目 (index=1, flag=False) 必须拒绝
    v_track.stbl.stsc = [StscEntry(first_chunk=1, samples_per_chunk=10, sample_description_index=1)]
    with pytest.raises(UnsupportedMp4LayoutError, match="Non-self-contained data reference"):
        synthesize_faststart_envelope_header(meta, b"", 1, 10)
