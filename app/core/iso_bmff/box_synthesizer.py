"""
Pure-function ISO-BMFF Box Synthesizer.

Synthesizes standard-compliant ISO-BMFF box structures and faststart envelope headers
for the VSSE (Virtual Sample Slicing Engine) channel:
- 16-byte 64-bit mdat header generation
- Mandatory 64-bit co64 chunk offset table conversion
- 4 Player Gate Checks:
  1. stss 1-based index remapping (strictly asserting stss_new[0] == 1)
  2. stts / ctts RLE exact slicing (sum(sample_count) == N)
  3. Three durations synchronized update (mvhd, tkhd, mdhd)
  4. Physical stripping of superfluous tracks (retaining 1 video and <= 1 audio;
     dropping subtitle/chapter/udta)
- Three-part chunk restructuring (first chunk intra-offset, middle chunks full, tail chunk truncation)
- Audio alignment via elst (delta = PTS(v_start) - PTS(a_start) >= 0)
- Two-pass length calculation for exact H header offset
- Cross-platform Big-Endian / Little-Endian safety via array.array byteswap
"""

from __future__ import annotations

import array
import bisect
import itertools
import operator
import struct
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, TypeVar

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
)
from app.core.iso_bmff.exceptions import IsoBmffError, UnsupportedMp4LayoutError

T = TypeVar("T")

_GET_SAMPLE_COUNT = operator.attrgetter("sample_count")
_GET_SAMPLE_DELTA = operator.attrgetter("sample_delta")
_GET_SAMPLE_OFFSET = operator.attrgetter("sample_offset")
_GET_STTS_PAIR = operator.attrgetter("sample_count", "sample_delta")
_GET_CTTS_PAIR = operator.attrgetter("sample_count", "sample_offset")


def make_box(box_type: bytes, payload: bytes) -> bytes:
    """Construct an ISO-BMFF box with 8-byte header."""
    return struct.pack(">I4s", len(payload) + 8, box_type) + payload


def make_full_box(box_type: bytes, version: int, flags: int, payload: bytes) -> bytes:
    """Construct an ISO-BMFF FullBox with 12-byte header."""
    vf = ((version & 0xFF) << 24) | (flags & 0x00FFFFFF)
    return struct.pack(">I4sI", len(payload) + 12, box_type, vf) + payload


def build_64bit_mdat_header(payload_len: int) -> bytes:
    """
    Generate a 16-byte 64-bit ISO-BMFF mdat header.

    Format:
    - 4 bytes size: 0x00000001 (indicates 64-bit large size follows)
    - 4 bytes type: b"mdat"
    - 8 bytes largesize: 16 + payload_len (big-endian uint64)
    """
    if payload_len < 0:
        raise ValueError(f"payload_len must be non-negative, got {payload_len}")
    return b"\x00\x00\x00\x01mdat" + struct.pack(">Q", 16 + payload_len)


def array_to_big_endian_bytes(arr: array.array) -> bytes:
    """
    Serialize an array.array to Big-Endian bytes safely.
    On Little-Endian architectures (x86_64 Windows/Linux), performs byteswap on a copy.
    """
    copy_arr = array.array(arr.typecode, arr)
    if sys.byteorder == "little":
        copy_arr.byteswap()
    return copy_arr.tobytes()


def remap_stss(stss: array.array | list[int], v_start: int, v_end: int) -> array.array:
    """
    Gate 1: Remap 1-based sync sample indices for the slice [v_start, v_end].

    stss_new[i] = S_i - v_start + 1.
    Strictly asserts that stss_new[0] == 1.
    """
    if v_start > v_end:
        raise ValueError(f"v_start ({v_start}) cannot exceed v_end ({v_end})")
    if v_start < 1:
        raise ValueError(f"v_start ({v_start}) must be >= 1")

    stss_new = array.array("I")
    for s in stss:
        if v_start <= s <= v_end:
            stss_new.append(s - v_start + 1)

    if not stss_new:
        raise IsoBmffError(f"Gate 1 Violation: No sync samples found in video range [{v_start}, {v_end}]")
    if stss_new[0] != 1:
        raise IsoBmffError(
            f"Gate 1 Violation: First sync sample after remapping is {stss_new[0]}, must strictly be 1 "
            f"(v_start={v_start} is not a keyframe)"
        )
    return stss_new


def slice_sbgp_entries(
    entries: Sequence[SbgpEntry],
    start_sample: int,
    end_sample: int,
) -> list[SbgpEntry]:
    """
    Slice sample-to-group (sbgp) entries conforming to ISO/IEC 14496-12 Section 8.18.2.

    If sum(sample_count) < end_sample, all subsequent samples implicitly belong
    to group 0 (no group description). Empty entries list implies all samples have group 0.
    Adjacent entries with identical group_description_index are merged.
    """
    if start_sample < 1:
        raise ValueError(f"start_sample ({start_sample}) must be >= 1")
    if start_sample > end_sample:
        raise ValueError(f"start_sample ({start_sample}) cannot exceed end_sample ({end_sample})")

    n_expected = end_sample - start_sample + 1
    if not entries:
        return [SbgpEntry(sample_count=n_expected, group_description_index=0)]

    entries_list = list(entries)
    total_available = sum(e.sample_count for e in entries_list)
    if total_available < end_sample:
        gap = end_sample - total_available
        if entries_list[-1].group_description_index == 0:
            entries_list[-1] = SbgpEntry(
                sample_count=entries_list[-1].sample_count + gap,
                group_description_index=0,
            )
        else:
            entries_list.append(SbgpEntry(sample_count=gap, group_description_index=0))

    cum_counts = array.array("I", itertools.accumulate(e.sample_count for e in entries_list))
    i_first = bisect.bisect_left(cum_counts, start_sample)
    i_last = bisect.bisect_left(cum_counts, end_sample, lo=i_first)

    if i_first == i_last:
        return [
            SbgpEntry(
                sample_count=n_expected,
                group_description_index=entries_list[i_first].group_description_index,
            )
        ]

    first_overlap = cum_counts[i_first] - start_sample + 1
    head_entry = SbgpEntry(
        sample_count=first_overlap,
        group_description_index=entries_list[i_first].group_description_index,
    )

    last_start = cum_counts[i_last - 1] + 1
    last_overlap = end_sample - last_start + 1
    tail_entry = SbgpEntry(
        sample_count=last_overlap,
        group_description_index=entries_list[i_last].group_description_index,
    )

    if i_first + 1 < i_last:
        middle_entries = entries_list[i_first + 1 : i_last]
        raw_sliced = [head_entry, *middle_entries, tail_entry]
    else:
        raw_sliced = [head_entry, tail_entry]

    merged: list[SbgpEntry] = [raw_sliced[0]]
    for e in raw_sliced[1:]:
        if e.group_description_index == merged[-1].group_description_index:
            merged[-1] = SbgpEntry(
                sample_count=merged[-1].sample_count + e.sample_count,
                group_description_index=e.group_description_index,
            )
        else:
            merged.append(e)

    return merged


def slice_rle_table(
    entries: list[Any] | Sequence[Any],
    start_sample: int,
    end_sample: int,
) -> list[Any]:
    """
    Gate 2: Exact slicing of RLE tables (stts, ctts, or sbgp).

    Sum of sample_count in sliced entries must strictly equal N = end_sample - start_sample + 1.
    """
    if start_sample < 1:
        raise ValueError(f"start_sample ({start_sample}) must be >= 1")
    if start_sample > end_sample:
        raise ValueError(f"start_sample ({start_sample}) cannot exceed end_sample ({end_sample})")

    n_expected = end_sample - start_sample + 1
    if not entries:
        raise IsoBmffError(
            f"Gate 2 Violation: Sliced RLE entry count sum (0) != expected ({n_expected}) "
            f"for range [{start_sample}, {end_sample}]"
        )

    first_item = entries[0]
    if hasattr(first_item, "group_description_index"):
        return slice_sbgp_entries(entries, start_sample, end_sample)
    if hasattr(first_item, "sample_delta"):
        entry_type = "stts"
    elif hasattr(first_item, "sample_offset"):
        entry_type = "ctts"
    else:
        raise TypeError(f"Unsupported RLE entry type: {type(first_item)}")

    def _make_entry(count: int, orig: Any) -> Any:
        if entry_type == "stts":
            return SttsEntry(sample_count=count, sample_delta=orig.sample_delta)
        return CttsEntry(sample_count=count, sample_offset=orig.sample_offset)

    counts_iter = entries.counts if hasattr(entries, "counts") else map(_GET_SAMPLE_COUNT, entries)
    cum_counts = array.array("I", itertools.accumulate(counts_iter))
    total_available = cum_counts[-1] if cum_counts else 0
    if total_available < end_sample:
        actual_sum = max(0, total_available - start_sample + 1)
        raise IsoBmffError(
            f"Gate 2 Violation: Sliced RLE entry count sum ({actual_sum}) != expected ({n_expected}) "
            f"for range [{start_sample}, {end_sample}]"
        )

    i_first = bisect.bisect_left(cum_counts, start_sample)
    i_last = bisect.bisect_left(cum_counts, end_sample, lo=i_first)

    if i_first == i_last:
        return [_make_entry(n_expected, entries[i_first])]

    first_overlap = cum_counts[i_first] - start_sample + 1
    head_entry = _make_entry(first_overlap, entries[i_first])

    last_start = cum_counts[i_last - 1] + 1
    last_overlap = end_sample - last_start + 1
    tail_entry = _make_entry(last_overlap, entries[i_last])

    if i_first + 1 < i_last:
        if isinstance(entries, list):
            middle_entries = entries[i_first + 1 : i_last]
        elif entry_type == "stts" and hasattr(entries, "counts") and hasattr(entries, "deltas"):
            middle_entries = list(
                map(SttsEntry, entries.counts[i_first + 1 : i_last], entries.deltas[i_first + 1 : i_last])
            )
        elif entry_type == "ctts" and hasattr(entries, "counts") and hasattr(entries, "offsets"):
            middle_entries = list(
                map(CttsEntry, entries.counts[i_first + 1 : i_last], entries.offsets[i_first + 1 : i_last])
            )
        else:
            middle_entries = list(entries[i_first + 1 : i_last])
        sliced_entries: list[Any] = [head_entry, *middle_entries, tail_entry]
    else:
        sliced_entries = [head_entry, tail_entry]

    return sliced_entries


@dataclass(slots=True)
class ChunkInfo:
    """Internal representation of a chunk's position and contents."""

    chunk_index: int
    start_sample: int
    end_sample: int
    sample_count: int
    desc_index: int
    base_offset: int


def get_track_chunks(track: Track) -> list[ChunkInfo]:
    """Decompress stsc to map each chunk to its sample range and offset."""
    stsc = track.stbl.stsc
    chunk_offsets = track.stbl.chunk_offsets
    total_chunks = len(chunk_offsets)
    if total_chunks == 0 or not stsc:
        return []

    stsc_idx = 0
    num_stsc = len(stsc)
    cur_sample = 1
    chunks: list[ChunkInfo] = []

    for c in range(1, total_chunks + 1):
        while stsc_idx + 1 < num_stsc and stsc[stsc_idx + 1].first_chunk <= c:
            stsc_idx += 1
        cnt = stsc[stsc_idx].samples_per_chunk
        desc = stsc[stsc_idx].sample_description_index
        base_off = chunk_offsets[c - 1]
        chunks.append(
            ChunkInfo(
                chunk_index=c,
                start_sample=cur_sample,
                end_sample=cur_sample + cnt - 1,
                sample_count=cnt,
                desc_index=desc,
                base_offset=base_off,
            )
        )
        cur_sample += cnt
    return chunks


def get_sample_byte_offset(
    track: Track,
    sample_index: int,
    chunks: list[ChunkInfo] | None = None,
) -> tuple[int, int]:
    """Compute (byte_start, byte_end) for a 1-based sample index in the original file."""
    total_samples = len(track.stbl.stsz)
    if sample_index < 1 or sample_index > total_samples:
        raise ValueError(f"Sample index {sample_index} out of bounds (1..{total_samples})")

    if chunks is None:
        chunks = get_track_chunks(track)
    if not chunks:
        raise IsoBmffError(f"Track has no chunks to locate sample {sample_index}")

    # Binary search or fast scan
    low = 0
    high = len(chunks) - 1
    target: ChunkInfo | None = None
    while low <= high:
        mid = (low + high) // 2
        chk = chunks[mid]
        if chk.start_sample <= sample_index <= chk.end_sample:
            target = chk
            break
        if sample_index < chk.start_sample:
            high = mid - 1
        else:
            low = mid + 1

    if target is None:
        raise IsoBmffError(f"Could not locate chunk for sample {sample_index}")

    offset = target.base_offset
    stsz = track.stbl.stsz
    if isinstance(stsz, UniformStszTable) or track.stbl.default_sample_size > 0:
        u_size = getattr(stsz, "uniform_size", track.stbl.default_sample_size)
        offset += (sample_index - target.start_sample) * u_size
        sample_size = u_size
    else:
        for j in range(target.start_sample, sample_index):
            offset += stsz[j - 1]
        sample_size = stsz[sample_index - 1]

    return offset, offset + sample_size


class SampleTimeline:
    """Precomputed prefix-sum index for O(log N) PTS/DTS lookups with minimal memory footprint."""

    __slots__ = ("ctts", "ctts_samples", "stts", "stts_dts", "stts_samples")

    def __init__(self, stts: Any, ctts: Any | None = None) -> None:
        self.stts = stts
        self.ctts = ctts

        stts_counts = stts.counts if hasattr(stts, "counts") else list(map(_GET_SAMPLE_COUNT, stts))
        stts_deltas = stts.deltas if hasattr(stts, "deltas") else list(map(_GET_SAMPLE_DELTA, stts))
        self.stts_samples = array.array("I", itertools.accumulate(stts_counts, initial=0))
        self.stts_dts = array.array(
            "Q",
            itertools.accumulate(map(operator.mul, stts_counts, stts_deltas), initial=0),
        )

        self.ctts_samples: array.array | None = None
        if ctts:
            ctts_counts = ctts.counts if hasattr(ctts, "counts") else map(_GET_SAMPLE_COUNT, ctts)
            self.ctts_samples = array.array("I", itertools.accumulate(ctts_counts, initial=0))

    def compute(self, sample_index: int) -> tuple[int, int]:
        rem_s = sample_index - 1
        idx = bisect.bisect_right(self.stts_samples, rem_s) - 1
        base_s = self.stts_samples[idx]
        base_d = self.stts_dts[idx]
        delta_s = rem_s - base_s
        delta_val = self.stts.deltas[idx] if hasattr(self.stts, "deltas") else self.stts[idx].sample_delta
        dts = base_d + delta_s * delta_val

        pts = dts
        if self.ctts and self.ctts_samples:
            c_idx = bisect.bisect_right(self.ctts_samples, rem_s) - 1
            c_offset = self.ctts.offsets[c_idx] if hasattr(self.ctts, "offsets") else self.ctts[c_idx].sample_offset
            pts += c_offset
        return dts, pts


def compute_sample_pts_dts(track: Track, sample_index: int) -> tuple[int, int]:
    """Compute (dts, pts) in track timescale for a 1-based sample index."""
    total_samples = len(track.stbl.stsz)
    if sample_index < 1 or sample_index > total_samples:
        raise ValueError(f"Sample index {sample_index} out of range (1..{total_samples})")

    stbl = track.stbl

    # O(1) Fast-Path: single CFR stts entry and no ctts
    if len(stbl.stts) == 1 and not stbl.ctts:
        dts = (sample_index - 1) * stbl.stts[0].sample_delta
        return dts, dts

    # Use cached timeline if already created
    tl = getattr(stbl, "_timeline", None)
    if tl is not None:
        return tl.compute(sample_index)

    # If tables are large (> 8 entries), construct and cache timeline
    if len(stbl.stts) > 8 or (stbl.ctts and len(stbl.ctts) > 8):
        tl = SampleTimeline(stbl.stts, stbl.ctts)
        stbl._timeline = tl
        return tl.compute(sample_index)

    # Fallback to direct linear scan for small tables (<= 8 entries)
    dts = 0
    remaining = sample_index - 1
    for entry in stbl.stts:
        if remaining <= 0:
            break
        take = min(remaining, entry.sample_count)
        dts += take * entry.sample_delta
        remaining -= take

    pts = dts
    if stbl.ctts:
        c_rem = sample_index - 1
        for c_entry in stbl.ctts:
            if c_rem < c_entry.sample_count:
                pts += c_entry.sample_offset
                break
            c_rem -= c_entry.sample_count

    return dts, pts


def compute_envelope_bounds(
    metadata: Mp4Metadata,
    v_start: int,
    v_end: int,
    a_start: int | None = None,
    a_end: int | None = None,
    v_chunks: list[ChunkInfo] | None = None,
    a_chunks: list[ChunkInfo] | None = None,
) -> tuple[int, int, int]:
    """
    Compute (o_min, o_max, payload_len) across video and optional audio ranges.
    O_min = min(ByteOffset(V_start), ByteOffset(A_start))
    O_max = max(ByteEnd(V_end), ByteEnd(A_end))
    payload_len = O_max - O_min
    """
    v_track = metadata.video_track
    if v_track is None:
        raise IsoBmffError("Missing video track")

    if v_chunks is None:
        v_chunks = get_track_chunks(v_track)
    v_start_off, _ = get_sample_byte_offset(v_track, v_start, chunks=v_chunks)
    _, v_end_off = get_sample_byte_offset(v_track, v_end, chunks=v_chunks)

    o_min = v_start_off
    o_max = v_end_off

    a_track = metadata.audio_track
    if a_track is not None and a_start is not None and a_end is not None:
        if a_chunks is None:
            a_chunks = get_track_chunks(a_track)
        a_start_off, _ = get_sample_byte_offset(a_track, a_start, chunks=a_chunks)
        _, a_end_off = get_sample_byte_offset(a_track, a_end, chunks=a_chunks)
        o_min = min(o_min, a_start_off)
        o_max = max(o_max, a_end_off)

    payload_len = o_max - o_min
    if payload_len < 0:
        raise IsoBmffError(f"Invalid payload_len ({payload_len}): o_min={o_min}, o_max={o_max}")

    return o_min, o_max, payload_len


def restructure_sample_table(
    track: Track,
    start_sample: int,
    end_sample: int,
    o_min: int = 0,
    h_offset: int = 0,
    chunks: list[ChunkInfo] | None = None,
) -> tuple[SampleTable, list[int]]:
    """
    Perform three-part chunk restructuring and sample table rebuild for [start_sample, end_sample].

    Returns:
        (new_sample_table, eff_offsets)
    """
    total_samples = len(track.stbl.stsz)
    if start_sample < 1 or start_sample > end_sample or end_sample > total_samples:
        raise ValueError(f"Invalid sample range [{start_sample}, {end_sample}] (total: {total_samples})")

    if chunks is None:
        chunks = get_track_chunks(track)
    c_start_idx = bisect.bisect_left(chunks, start_sample, key=lambda c: c.end_sample)
    c_end_idx = bisect.bisect_left(chunks, end_sample, key=lambda c: c.end_sample)

    if not (0 <= c_start_idx < len(chunks) and 0 <= c_end_idx < len(chunks)):
        raise IsoBmffError(f"Could not map samples [{start_sample}, {end_sample}] to chunks")

    m_new = c_end_idx - c_start_idx + 1
    new_chunk_sample_counts: list[int] = []
    new_chunk_desc_indices: list[int] = []
    eff_offsets: list[int] = []
    stsz = track.stbl.stsz

    if m_new == 1:
        chk = chunks[c_start_idx]
        n_samples = end_sample - start_sample + 1
        if isinstance(stsz, UniformStszTable) or track.stbl.default_sample_size > 0:
            u_size = getattr(stsz, "uniform_size", track.stbl.default_sample_size)
            skipped_bytes = (start_sample - chk.start_sample) * u_size
        else:
            skipped_bytes = sum(stsz[j - 1] for j in range(chk.start_sample, start_sample))
        eff_offset = chk.base_offset + skipped_bytes

        new_chunk_sample_counts.append(n_samples)
        new_chunk_desc_indices.append(chk.desc_index)
        eff_offsets.append(eff_offset)
    else:
        # First chunk (k = 1)
        chk_first = chunks[c_start_idx]
        r_start = start_sample - chk_first.start_sample
        n_first = chk_first.sample_count - r_start
        if isinstance(stsz, UniformStszTable) or track.stbl.default_sample_size > 0:
            u_size = getattr(stsz, "uniform_size", track.stbl.default_sample_size)
            skipped_bytes = r_start * u_size
        else:
            skipped_bytes = sum(stsz[j - 1] for j in range(chk_first.start_sample, start_sample))
        eff_offsets.append(chk_first.base_offset + skipped_bytes)
        new_chunk_sample_counts.append(n_first)
        new_chunk_desc_indices.append(chk_first.desc_index)

        # Middle chunks (k = 2 .. m_new - 1)
        for i in range(c_start_idx + 1, c_end_idx):
            chk_mid = chunks[i]
            new_chunk_sample_counts.append(chk_mid.sample_count)
            new_chunk_desc_indices.append(chk_mid.desc_index)
            eff_offsets.append(chk_mid.base_offset)

        # Tail chunk (k = m_new)
        chk_last = chunks[c_end_idx]
        n_last = end_sample - chk_last.start_sample + 1
        new_chunk_sample_counts.append(n_last)
        new_chunk_desc_indices.append(chk_last.desc_index)
        eff_offsets.append(chk_last.base_offset)

    expected_total_samples = end_sample - start_sample + 1
    if sum(new_chunk_sample_counts) != expected_total_samples:
        raise IsoBmffError(
            f"Restructured chunk sample count mismatch: sum={sum(new_chunk_sample_counts)}, "
            f"expected={expected_total_samples}"
        )

    # Rebuild compressed stsc table
    new_stsc: list[StscEntry] = []
    prev_cnt: int | None = None
    prev_desc: int | None = None

    for k in range(1, m_new + 1):
        cnt = new_chunk_sample_counts[k - 1]
        desc = new_chunk_desc_indices[k - 1]
        if cnt != prev_cnt or desc != prev_desc:
            new_stsc.append(StscEntry(first_chunk=k, samples_per_chunk=cnt, sample_description_index=desc))
            prev_cnt = cnt
            prev_desc = desc

    _validate_self_contained_stsd(track.stbl.stsd, {e.sample_description_index for e in new_stsc})

    # co64 chunk offsets
    co64_arr = array.array("Q")
    for eff in eff_offsets:
        if eff < o_min:
            raise UnsupportedMp4LayoutError(f"Non-monotonic chunk offset {eff} precedes envelope start {o_min}")
        co64_arr.append(h_offset + (eff - o_min))

    # stsz slice
    stsz_new = track.stbl.stsz[start_sample - 1 : end_sample]

    # Gate 2: stts exact slicing
    stts_new = slice_rle_table(track.stbl.stts, start_sample, end_sample)

    # ctts exact slicing (if present)
    ctts_new = slice_rle_table(track.stbl.ctts, start_sample, end_sample) if track.stbl.ctts else None

    # Gate 1: stss remapping (if present)
    stss_new = remap_stss(track.stbl.stss, start_sample, end_sample) if track.stbl.stss is not None else None

    # sgpd: preserve unmodified
    sgpd_boxes_new = list(track.stbl.sgpd_boxes)

    # sbgp: slice RLE tables
    sbgp_tables_new: list[SbgpTable] = []
    for sbgp in track.stbl.sbgp_tables:
        sliced_entries = slice_sbgp_entries(sbgp.entries, start_sample, end_sample)
        sbgp_tables_new.append(
            SbgpTable(
                version=sbgp.version,
                flags=sbgp.flags,
                grouping_type=sbgp.grouping_type,
                entries=sliced_entries,
                grouping_type_parameter=sbgp.grouping_type_parameter,
            )
        )
    sbgp_boxes_new = [serialize_sbgp(t) for t in sbgp_tables_new] if sbgp_tables_new else list(track.stbl.sbgp_boxes)

    def_size = (
        track.stbl.stsz.uniform_size
        if isinstance(track.stbl.stsz, UniformStszTable)
        else track.stbl.default_sample_size
    )

    new_stbl = SampleTable(
        stsd=track.stbl.stsd,
        stts=stts_new,
        stsc=new_stsc,
        stsz=stsz_new,
        chunk_offsets=co64_arr,
        stss=stss_new,
        ctts=ctts_new,
        default_sample_size=def_size,
        is_co64=True,
        sgpd_boxes=sgpd_boxes_new,
        sbgp_tables=sbgp_tables_new,
        sbgp_boxes=sbgp_boxes_new,
    )

    return new_stbl, eff_offsets


def extract_track_edit_offset(track: Track | None, movie_timescale: int = 1000) -> tuple[float, int]:
    """
    Extract (empty_dwell_sec, media_time_ticks) from a track's edts/elst box.

    - empty_dwell_sec: cumulative duration in seconds of leading Empty Edit entries (media_time == -1).
    - media_time_ticks: media_time (in track timescale) of the first non-empty media edit entry (>= 0).
    """
    if track is None or track.edts is None or not track.edts.entries:
        return 0.0, 0

    m_scale = movie_timescale if movie_timescale > 0 else 1
    empty_ticks = 0
    media_time = 0
    for entry in track.edts.entries:
        if entry.media_time == -1:
            empty_ticks += max(0, entry.segment_duration)
        elif entry.media_time >= 0:
            media_time = entry.media_time
            break
    return empty_ticks / m_scale, media_time


def _validate_self_contained_stsd(stsd: Stsd, used_desc_indices: set[int] | None = None) -> None:
    """Reject tracks referencing non-self-contained dref entries before synthesizing a single-dref minf."""
    if not stsd.dref_self_contained:
        return
    indices = used_desc_indices if used_desc_indices is not None else set(range(1, len(stsd.entries) + 1))
    for desc_idx in indices:
        if desc_idx < 1 or desc_idx > len(stsd.entries):
            continue
        ref_idx = stsd.entries[desc_idx - 1].data_ref_index
        if ref_idx < 1 or ref_idx > len(stsd.dref_self_contained) or not stsd.dref_self_contained[ref_idx - 1]:
            raise UnsupportedMp4LayoutError(
                f"Non-self-contained data reference (data_ref_index={ref_idx}) is not supported in VSSE"
            )


def _compute_track_presentation_end(track: Track) -> int:
    """Compute max presentation end timestamp (DTS + sample_delta + ctts_offset) in track timescale."""
    stts = track.stbl.stts
    ctts = track.stbl.ctts
    if not ctts:
        return sum(e.sample_count * e.sample_delta for e in stts)

    stts_iter = iter(stts)
    ctts_iter = iter(ctts)
    s_cur = next(stts_iter, None)
    c_cur = next(ctts_iter, None)
    s_rem = s_cur.sample_count if s_cur is not None else 0
    c_rem = c_cur.sample_count if c_cur is not None else 0

    dts = 0
    max_pres_end = 0
    while s_cur is not None:
        if s_rem == 0:
            s_cur = next(stts_iter, None)
            if s_cur is None:
                break
            s_rem = s_cur.sample_count
            continue
        if c_cur is not None and c_rem == 0:
            c_cur = next(ctts_iter, None)
            c_rem = c_cur.sample_count if c_cur is not None else 0
            continue
        take = min(s_rem, c_rem) if c_cur is not None else s_rem
        c_off = c_cur.sample_offset if c_cur is not None else 0
        dts_end = dts + take * s_cur.sample_delta
        max_pres_end = max(max_pres_end, dts_end + c_off)
        dts = dts_end
        s_rem -= take
        if c_cur is not None:
            c_rem -= take
    return max(0, max_pres_end)


def configure_video_elst(
    video_track: Track,
    v_start: int,
    movie_timescale: int,
    original_video_track: Track | None = None,
) -> None:
    """
    Configure video track edts/elst box (Tier B: Source mv Offset Preservation & B-frame CTTS Compensation).

    - For v_start == 1: preserves max(ctts_v0, orig_mv) when 0 < orig_mv < v_dur, preventing
      source video edit-list offsets from being falsely transferred to the audio track.
    - For v_start > 1: sets media_time = ctts_v0 so the volume starts immediately on the
      sliced keyframe's presentation timestamp.
    """
    orig_v = original_video_track if original_video_track is not None else video_track
    dts_v0, pts_v0 = compute_sample_pts_dts(orig_v, v_start)
    ctts_v0 = max(0, pts_v0 - dts_v0)

    v_timescale = video_track.mdhd.timescale or 1
    m_scale = movie_timescale if movie_timescale > 0 else 1
    v_dur = sum(e.sample_count * e.sample_delta for e in video_track.stbl.stts)
    v_pres_end = _compute_track_presentation_end(video_track)

    _, orig_mv = extract_track_edit_offset(orig_v, m_scale)
    v_media_time = ctts_v0
    if v_start == 1 and orig_mv > ctts_v0 and orig_mv < v_dur:
        v_media_time = orig_mv

    if v_media_time > 0:
        v_seg_dur = round(max(0, v_pres_end - v_media_time) * m_scale / v_timescale)
        v_elst_v = 1 if (v_media_time > 0x7FFFFFFF or v_seg_dur > 0xFFFFFFFF) else 0
        video_track.edts = Edts(
            elst_version=v_elst_v,
            elst_flags=0,
            entries=[
                ElstEntry(
                    segment_duration=v_seg_dur,
                    media_time=v_media_time,
                    media_rate_integer=1,
                    media_rate_fraction=0,
                )
            ],
        )
    else:
        v_seg_dur = round(v_pres_end * m_scale / v_timescale)
        video_track.edts = None

    video_track.tkhd.duration = v_seg_dur


def configure_audio_elst(
    audio_track: Track,
    video_track: Track,
    v_start: int,
    a_start: int,
    movie_timescale: int,
    original_audio_track: Track | None = None,
    original_video_track: Track | None = None,
) -> None:
    """
    Configure audio elst box for hardware-level Dual-Tier Adaptive AV alignment.

    Tier B (Pre-Source Offset Cancellation):
        Accounts for any source video mv offset beyond ctts_v0 absorbed into `video_track.edts`,
        avoiding artificial negative delta transfer to the audio track.
    Tier A (Bidirectional Adaptive elst):
        - When effective_delta >= 0: single ElstEntry trimming leading audio (media_time >= 0).
        - When effective_delta < 0: two-entry ElstEntry list starting with an ISO/IEC 14496-12
          Empty Edit (media_time = -1, segment_duration = empty_dur) followed by the normal
          media edit, eliminating Gate 5 fallback.
    """
    orig_a = original_audio_track if original_audio_track is not None else audio_track
    orig_v = original_video_track if original_video_track is not None else video_track
    dts_v, pts_v = compute_sample_pts_dts(orig_v, v_start)
    dts_a, pts_a = compute_sample_pts_dts(orig_a, a_start)
    ctts_v0 = max(0, pts_v - dts_v)
    ctts_a0 = max(0, pts_a - dts_a)

    v_timescale = orig_v.mdhd.timescale or 1
    a_timescale = orig_a.mdhd.timescale or 1
    m_scale = movie_timescale if movie_timescale > 0 else 1

    pts_v_sec = pts_v / v_timescale
    pts_a_sec = pts_a / a_timescale

    delta = pts_v_sec - pts_a_sec

    # Extract source track edit list priming delays and leading empty edits
    v_empty_sec, mv = extract_track_edit_offset(orig_v, m_scale)
    a_empty_sec, ma = extract_track_edit_offset(orig_a, m_scale)

    # Tier B: Absorb valid initial video mv offset on v_start == 1 so it is not transferred to audio
    v_absorbed_extra = 0
    if v_start == 1:
        if (video_track.edts is None or not video_track.edts.entries) and mv > ctts_v0:
            configure_video_elst(video_track, v_start, m_scale, orig_v)
        if video_track.edts and video_track.edts.entries:
            _, v_out_mt = extract_track_edit_offset(video_track, m_scale)
            v_absorbed_extra = max(0, v_out_mt - ctts_v0)

    effective_mv = max(0, mv - v_absorbed_extra)
    c = (ma / a_timescale - a_empty_sec) - (effective_mv / v_timescale - v_empty_sec)
    effective_delta = delta + c

    if abs(effective_delta) < 1e-6:
        effective_delta = 0.0

    out_timescale = audio_track.mdhd.timescale or 1
    audio_dur = sum(e.sample_count * e.sample_delta for e in audio_track.stbl.stts)

    if effective_delta >= 0.0:
        media_time = ctts_a0 + round(effective_delta * out_timescale)
        assert media_time >= 0, f"media_time must be non-negative, got {media_time}"
        # Deduct media_time from track duration per ISO-BMFF specification
        segment_dur = round(max(0, audio_dur - media_time) * m_scale / out_timescale)
        entries = [
            ElstEntry(
                segment_duration=segment_dur,
                media_time=media_time,
                media_rate_integer=1,
                media_rate_fraction=0,
            )
        ]
    else:
        # Tier A: Audio starts after video -> prepend ISO/IEC 14496-12 Empty Edit (media_time = -1)
        empty_dur = round(abs(effective_delta) * m_scale)
        media_time = ctts_a0
        segment_dur = round(max(0, audio_dur - media_time) * m_scale / out_timescale)
        if empty_dur > 0:
            entries = [
                ElstEntry(
                    segment_duration=empty_dur,
                    media_time=-1,
                    media_rate_integer=1,
                    media_rate_fraction=0,
                ),
                ElstEntry(
                    segment_duration=segment_dur,
                    media_time=media_time,
                    media_rate_integer=1,
                    media_rate_fraction=0,
                ),
            ]
        else:
            entries = [
                ElstEntry(
                    segment_duration=segment_dur,
                    media_time=media_time,
                    media_rate_integer=1,
                    media_rate_fraction=0,
                )
            ]

    elst_v = (
        1
        if any(
            e.media_time > 0x7FFFFFFF or e.media_time < -0x80000000 or e.segment_duration > 0xFFFFFFFF for e in entries
        )
        else 0
    )
    audio_track.edts = Edts(elst_version=elst_v, elst_flags=0, entries=entries)
    audio_track.tkhd.duration = sum(e.segment_duration for e in entries)


def synchronize_durations(mvhd: Mvhd, tracks: list[Track], movie_timescale: int) -> None:
    """
    Gate 3: Synchronize durations across mvhd, tkhd, and mdhd boxes.
    """
    max_tkhd_dur = 0
    for trak in tracks:
        track_dur = sum(e.sample_count * e.sample_delta for e in trak.stbl.stts)
        trak.mdhd.duration = track_dur

        if trak.edts and trak.edts.entries:
            tkhd_dur = sum(e.segment_duration for e in trak.edts.entries)
        else:
            scale = trak.mdhd.timescale if trak.mdhd.timescale > 0 else 1
            tkhd_dur = int(round(track_dur * movie_timescale / scale))

        trak.tkhd.duration = tkhd_dur
        max_tkhd_dur = max(max_tkhd_dur, tkhd_dur)

    mvhd.duration = max_tkhd_dur


# -----------------------------------------------------------------------------
# Box Binary Serializers
# -----------------------------------------------------------------------------


def serialize_mvhd(mvhd: Mvhd) -> bytes:
    """Serialize Mvhd box."""
    is_v1 = (
        mvhd.version == 1
        or mvhd.duration > 0xFFFFFFFF
        or mvhd.creation_time > 0xFFFFFFFF
        or mvhd.modification_time > 0xFFFFFFFF
    )
    if is_v1:
        payload = (
            struct.pack(">QQIQ", mvhd.creation_time, mvhd.modification_time, mvhd.timescale, mvhd.duration)
            + struct.pack(">IH", mvhd.rate, mvhd.volume)
            + b"\x00" * 10
            + struct.pack(">9i", *mvhd.matrix)
            + b"\x00" * 24
            + struct.pack(">I", mvhd.next_track_id)
        )
        return make_full_box(b"mvhd", 1, mvhd.flags, payload)
    payload = (
        struct.pack(">IIII", mvhd.creation_time, mvhd.modification_time, mvhd.timescale, mvhd.duration)
        + struct.pack(">IH", mvhd.rate, mvhd.volume)
        + b"\x00" * 10
        + struct.pack(">9i", *mvhd.matrix)
        + b"\x00" * 24
        + struct.pack(">I", mvhd.next_track_id)
    )
    return make_full_box(b"mvhd", 0, mvhd.flags, payload)


def serialize_tkhd(tkhd: Tkhd) -> bytes:
    """Serialize Tkhd box."""
    is_v1 = (
        tkhd.version == 1
        or tkhd.duration > 0xFFFFFFFF
        or tkhd.creation_time > 0xFFFFFFFF
        or tkhd.modification_time > 0xFFFFFFFF
    )
    w_raw = tkhd.width_raw if tkhd.width_raw > 0 else int(round(tkhd.width * 65536))
    h_raw = tkhd.height_raw if tkhd.height_raw > 0 else int(round(tkhd.height * 65536))
    flags = tkhd.flags if tkhd.flags != 0 else 7

    if is_v1:
        payload = (
            struct.pack(">QQIIQ", tkhd.creation_time, tkhd.modification_time, tkhd.track_id, 0, tkhd.duration)
            + b"\x00" * 8
            + struct.pack(">hhh", tkhd.layer, tkhd.alternate_group, tkhd.volume)
            + b"\x00" * 2
            + struct.pack(">9i", *tkhd.matrix)
            + struct.pack(">II", w_raw, h_raw)
        )
        return make_full_box(b"tkhd", 1, flags, payload)
    payload = (
        struct.pack(">IIIII", tkhd.creation_time, tkhd.modification_time, tkhd.track_id, 0, tkhd.duration)
        + b"\x00" * 8
        + struct.pack(">hhh", tkhd.layer, tkhd.alternate_group, tkhd.volume)
        + b"\x00" * 2
        + struct.pack(">9i", *tkhd.matrix)
        + struct.pack(">II", w_raw, h_raw)
    )
    return make_full_box(b"tkhd", 0, flags, payload)


def serialize_edts(edts: Edts) -> bytes:
    """Serialize Edts box containing Elst box."""
    entries_raw = bytearray()
    for e in edts.entries:
        if edts.elst_version == 1:
            entries_raw.extend(
                struct.pack(">Qqhh", e.segment_duration, e.media_time, e.media_rate_integer, e.media_rate_fraction)
            )
        else:
            entries_raw.extend(
                struct.pack(">Iihh", e.segment_duration, e.media_time, e.media_rate_integer, e.media_rate_fraction)
            )

    elst_payload = struct.pack(">I", len(edts.entries)) + bytes(entries_raw)
    elst_box = make_full_box(b"elst", edts.elst_version, edts.elst_flags, elst_payload)
    return make_box(b"edts", elst_box)


def serialize_mdhd(mdhd: Mdhd) -> bytes:
    """Serialize Mdhd box."""
    is_v1 = (
        mdhd.version == 1
        or mdhd.duration > 0xFFFFFFFF
        or mdhd.creation_time > 0xFFFFFFFF
        or mdhd.modification_time > 0xFFFFFFFF
    )
    if is_v1:
        payload = struct.pack(
            ">QQIQH2s",
            mdhd.creation_time,
            mdhd.modification_time,
            mdhd.timescale,
            mdhd.duration,
            mdhd.language,
            b"\x00\x00",
        )
        return make_full_box(b"mdhd", 1, mdhd.flags, payload)
    payload = struct.pack(
        ">IIIIH2s",
        mdhd.creation_time,
        mdhd.modification_time,
        mdhd.timescale,
        mdhd.duration,
        mdhd.language,
        b"\x00\x00",
    )
    return make_full_box(b"mdhd", 0, mdhd.flags, payload)


def serialize_hdlr(hdlr: Hdlr) -> bytes:
    """Serialize Hdlr box."""
    name_bytes = hdlr.name.encode("utf-8") + b"\x00"
    payload = struct.pack(">I4s12s", 0, hdlr.handler_type, b"\x00" * 12) + name_bytes
    return make_full_box(b"hdlr", 0, 0, payload)


def serialize_sample_entry(entry: SampleEntry) -> bytes:
    """Serialize a SampleEntry if raw_bytes is empty."""
    if entry.raw_bytes:
        return entry.raw_bytes

    if entry.width > 0 or entry.height > 0:
        payload = (
            struct.pack(">6sH", b"\x00" * 6, entry.data_ref_index)
            + struct.pack(">HH", 0, 0)
            + struct.pack(">III", 0, 0, 0)
            + struct.pack(">HH", entry.width, entry.height)
            + struct.pack(">II", 0x00480000, 0x00480000)
            + struct.pack(">IH", 0, 1)
            + b"\x00" * 32
            + struct.pack(">hh", 24, -1)
        )
        return make_box(entry.format, payload)
    payload = (
        struct.pack(">6sH", b"\x00" * 6, entry.data_ref_index)
        + struct.pack(">8s", b"\x00" * 8)
        + struct.pack(">HH", entry.channel_count or 2, 16)
        + struct.pack(">HH", 0, 0)
        + struct.pack(">I", (entry.sample_rate or 48000) << 16)
    )
    return make_box(entry.format, payload)


def _serialize_sample_entry_with_dref1(entry: SampleEntry) -> bytes:
    if entry.raw_bytes:
        if entry.data_ref_index == 1 or len(entry.raw_bytes) < 16:
            return entry.raw_bytes
        buf = bytearray(entry.raw_bytes)
        struct.pack_into(">H", buf, 14, 1)
        return bytes(buf)
    if entry.data_ref_index == 1:
        return serialize_sample_entry(entry)
    patched = SampleEntry(
        format=entry.format,
        data_ref_index=1,
        codec=entry.codec,
        width=entry.width,
        height=entry.height,
        channel_count=entry.channel_count,
        sample_rate=entry.sample_rate,
        raw_bytes=entry.raw_bytes,
        esds_object_type=entry.esds_object_type,
    )
    return serialize_sample_entry(patched)


def _rewrite_raw_stsd_dref_to_one(raw_box: bytes) -> bytes:
    if len(raw_box) < 16:
        return raw_box
    b_size = struct.unpack_from(">I", raw_box, 0)[0]
    hdr_len = 16 if b_size == 1 else 8
    if len(raw_box) < hdr_len + 8:
        return raw_box
    buf = bytearray(raw_box)
    entry_count = struct.unpack_from(">I", buf, hdr_len + 4)[0]
    pos = hdr_len + 8
    for _ in range(entry_count):
        if pos + 8 > len(buf):
            break
        entry_size = struct.unpack_from(">I", buf, pos)[0]
        if entry_size < 16 or pos + entry_size > len(buf):
            break
        struct.pack_into(">H", buf, pos + 14, 1)
        pos += entry_size
    return bytes(buf)


def serialize_stsd(stsd: Stsd) -> bytes:
    """Serialize Stsd box, remapping sample description data_ref_index to 1."""
    if stsd.raw_box:
        if all(e.data_ref_index == 1 for e in stsd.entries):
            return stsd.raw_box
        return _rewrite_raw_stsd_dref_to_one(stsd.raw_box)
    entries_bytes = b"".join(_serialize_sample_entry_with_dref1(e) for e in stsd.entries)
    payload = struct.pack(">I", len(stsd.entries)) + entries_bytes
    return make_full_box(b"stsd", stsd.version, stsd.flags, payload)


def serialize_stts(stts: Any) -> bytes:
    """Serialize Stts box."""
    if hasattr(stts, "counts") and hasattr(stts, "deltas"):
        pairs = itertools.chain.from_iterable(zip(stts.counts, stts.deltas, strict=False))
    else:
        pairs = itertools.chain.from_iterable(map(_GET_STTS_PAIR, stts))
    raw_interleaved = array.array("I", pairs)
    if sys.byteorder == "little":
        raw_interleaved.byteswap()
    payload = struct.pack(">I", len(stts)) + raw_interleaved.tobytes()
    return make_full_box(b"stts", 0, 0, payload)


def serialize_ctts(ctts: Any) -> bytes:
    """Serialize Ctts box."""
    if hasattr(ctts, "counts") and hasattr(ctts, "offsets"):
        is_v1 = getattr(ctts, "version", 0) == 1 or min(ctts.offsets, default=0) < 0
        pairs = itertools.chain.from_iterable(zip(ctts.counts, ctts.offsets, strict=False))
    else:
        is_v1 = min(map(_GET_SAMPLE_OFFSET, ctts), default=0) < 0
        pairs = itertools.chain.from_iterable(map(_GET_CTTS_PAIR, ctts))
    raw_interleaved = array.array("i" if is_v1 else "I", pairs)
    if sys.byteorder == "little":
        raw_interleaved.byteswap()
    payload = struct.pack(">I", len(ctts)) + raw_interleaved.tobytes()
    return make_full_box(b"ctts", 1 if is_v1 else 0, 0, payload)


def serialize_stsc(stsc: list[StscEntry]) -> bytes:
    """Serialize Stsc box."""
    payload = struct.pack(">I", len(stsc)) + b"".join(
        struct.pack(">III", e.first_chunk, e.samples_per_chunk, e.sample_description_index) for e in stsc
    )
    return make_full_box(b"stsc", 0, 0, payload)


def serialize_sbgp(sbgp: SbgpTable) -> bytes:
    """Serialize Sbgp box (Sample-to-Group) into standard FullBox."""
    if sbgp.version not in (0, 1):
        raise IsoBmffError(f"Unsupported sbgp version {sbgp.version} (must be 0 or 1)")
    gt = sbgp.grouping_type
    if isinstance(gt, str):
        gt = gt.encode("ascii")
    if len(gt) != 4:
        raise IsoBmffError(f"sbgp grouping_type must be 4 bytes, got {len(gt)} ({gt!r})")

    payload_parts = [gt]
    if sbgp.version == 1:
        payload_parts.append(struct.pack(">I", sbgp.grouping_type_parameter))
    payload_parts.append(struct.pack(">I", len(sbgp.entries)))
    for e in sbgp.entries:
        payload_parts.append(struct.pack(">II", e.sample_count, e.group_description_index))
    payload = b"".join(payload_parts)
    return make_full_box(b"sbgp", sbgp.version, sbgp.flags, payload)


def serialize_stsz(stsz: array.array | list[int] | UniformStszTable, default_sample_size: int = 0) -> bytes:
    """Serialize Stsz box with cross-platform endianness safety."""
    if isinstance(stsz, UniformStszTable):
        payload = struct.pack(">II", stsz.uniform_size, stsz.sample_count)
        return make_full_box(b"stsz", 0, 0, payload)

    if default_sample_size > 0 and (len(stsz) == 0 or all(s == default_sample_size for s in stsz)):
        payload = struct.pack(">II", default_sample_size, len(stsz))
    else:
        if not isinstance(stsz, array.array) or stsz.typecode != "I":
            stsz = array.array("I", stsz)
        raw_stsz = array_to_big_endian_bytes(stsz)
        payload = struct.pack(">II", 0, len(stsz)) + raw_stsz
    return make_full_box(b"stsz", 0, 0, payload)


def serialize_co64(chunk_offsets: array.array | list[int]) -> bytes:
    """Mandatory 64-bit co64 chunk offset table serializer."""
    if not isinstance(chunk_offsets, array.array) or chunk_offsets.typecode != "Q":
        chunk_offsets = array.array("Q", chunk_offsets)
    raw_co64 = array_to_big_endian_bytes(chunk_offsets)
    payload = struct.pack(">I", len(chunk_offsets)) + raw_co64
    return make_full_box(b"co64", 0, 0, payload)


def serialize_stss(stss: array.array | list[int]) -> bytes:
    """Serialize Stss box."""
    if not isinstance(stss, array.array) or stss.typecode != "I":
        stss = array.array("I", stss)
    raw_stss = array_to_big_endian_bytes(stss)
    payload = struct.pack(">I", len(stss)) + raw_stss
    return make_full_box(b"stss", 0, 0, payload)


def serialize_minf(is_video: bool, stbl_box: bytes) -> bytes:
    """Serialize Minf box containing vmhd/smhd, dinf, and stbl."""
    if is_video:
        media_header = make_full_box(b"vmhd", 0, 1, struct.pack(">hhhh", 0, 0, 0, 0))
    else:
        media_header = make_full_box(b"smhd", 0, 0, struct.pack(">hh", 0, 0))

    url_box = make_full_box(b"url ", 0, 1, b"")
    dref_payload = struct.pack(">I", 1) + url_box
    dref_box = make_full_box(b"dref", 0, 0, dref_payload)
    dinf_box = make_box(b"dinf", dref_box)

    return make_box(b"minf", media_header + dinf_box + stbl_box)


def serialize_trak(track: Track) -> bytes:
    """Serialize a complete Track box."""
    tkhd_box = serialize_tkhd(track.tkhd)
    boxes = [tkhd_box]
    if track.edts and track.edts.entries:
        boxes.append(serialize_edts(track.edts))

    mdhd_box = serialize_mdhd(track.mdhd)
    hdlr_box = serialize_hdlr(track.hdlr)

    stbl_boxes = [serialize_stsd(track.stbl.stsd), serialize_stts(track.stbl.stts)]
    if track.stbl.ctts:
        stbl_boxes.append(serialize_ctts(track.stbl.ctts))
    stbl_boxes.append(serialize_stsc(track.stbl.stsc))
    stbl_boxes.append(serialize_stsz(track.stbl.stsz, track.stbl.default_sample_size))
    # Mandatory 64-bit co64
    stbl_boxes.append(serialize_co64(track.stbl.chunk_offsets))
    if track.stbl.stss is not None and len(track.stbl.stss) > 0:
        stbl_boxes.append(serialize_stss(track.stbl.stss))
    for sgpd in track.stbl.sgpd_boxes:
        stbl_boxes.append(sgpd)
    if track.stbl.sbgp_tables:
        for sbgp in track.stbl.sbgp_tables:
            stbl_boxes.append(serialize_sbgp(sbgp))
    elif track.stbl.sbgp_boxes:
        for sbgp_raw in track.stbl.sbgp_boxes:
            stbl_boxes.append(sbgp_raw)

    stbl_box = make_box(b"stbl", b"".join(stbl_boxes))
    minf_box = serialize_minf(track.is_video, stbl_box)
    mdia_box = make_box(b"mdia", mdhd_box + hdlr_box + minf_box)
    boxes.append(mdia_box)

    return make_box(b"trak", b"".join(boxes))


def serialize_moov(metadata: Mp4Metadata) -> bytes:
    """
    Serialize Mp4Metadata into a moov box.

    Gate 4: Retains strictly 1 video track and at most 1 audio track;
    physically strips subtitle, chapter, and udta boxes.
    """
    mvhd_box = serialize_mvhd(metadata.mvhd)
    boxes = [mvhd_box]

    v_track = metadata.video_track
    if v_track is None:
        raise IsoBmffError("Cannot serialize moov: no video track present")
    boxes.append(serialize_trak(v_track))

    a_track = metadata.audio_track
    if a_track is not None:
        boxes.append(serialize_trak(a_track))

    return make_box(b"moov", b"".join(boxes))


# -----------------------------------------------------------------------------
# High-Level Synthesizer
# -----------------------------------------------------------------------------


def synthesize_faststart_envelope_header(
    metadata: Mp4Metadata,
    ftyp_bytes: bytes,
    v_start: int,
    v_end: int,
    a_start: int | None = None,
    a_end: int | None = None,
    o_min: int | None = None,
    o_max: int | None = None,
    payload_len: int | None = None,
    sidecar_audio_slice: bytes | memoryview | None = None,
    a_o_min: int | None = None,
) -> bytes:
    """
    Two-pass synthesizer producing continuous faststart envelope header bytes:
    [ftyp box] + [moov box with co64] + [16-byte 64-bit mdat header] + [optional sidecar audio]

    Pass 1: Computes exact envelope box header size H_box = len(ftyp + moov_p1 + 16B mdat).
    Pass 2: Fixes co64 offsets:
      - Standard Tier 1: co64[k] = H_box + (EffOffset[k] - O_min).
      - Hybrid Tier 2 (sidecar):
          co64_A[k] = H_box + (EffOffset_A[k] - a_min)  (in-memory prefix)
          co64_V[k] = H_box + A_len + (EffOffset_V[k] - v_min)  (remote video range)
    """
    v_track = metadata.video_track
    if v_track is None:
        raise IsoBmffError("Video track missing from metadata")

    total_v = len(v_track.stbl.stsz)
    if v_start < 1 or v_start > v_end or v_end > total_v:
        raise ValueError(f"Invalid video range [{v_start}, {v_end}] (total: {total_v})")

    a_track = metadata.audio_track
    has_audio = a_track is not None and a_start is not None and a_end is not None

    if has_audio and a_track is not None and a_start is not None and a_end is not None:
        total_a = len(a_track.stbl.stsz)
        if a_start < 1 or a_start > a_end or a_end > total_a:
            raise ValueError(f"Invalid audio range [{a_start}, {a_end}] (total: {total_a})")

    # Compute bounds if not passed
    if o_min is None or o_max is None or payload_len is None:
        if sidecar_audio_slice is not None:
            calc_omin, calc_omax, calc_payload = compute_envelope_bounds(
                metadata,
                v_start,
                v_end,
                None,
                None,
            )
        else:
            calc_omin, calc_omax, calc_payload = compute_envelope_bounds(
                metadata,
                v_start,
                v_end,
                a_start if has_audio else None,
                a_end if has_audio else None,
            )
        if o_min is None:
            o_min = calc_omin
        if o_max is None:
            o_max = calc_omax
        if payload_len is None:
            payload_len = calc_payload

    if o_min is not None:
        if o_min < 0:
            raise ValueError(f"o_min cannot be negative: {o_min}")
        first_v_off, _ = get_sample_byte_offset(v_track, v_start)
        if o_min > first_v_off:
            raise ValueError(f"o_min ({o_min}) cannot exceed first video chunk offset ({first_v_off})")

    if has_audio and a_track is not None and a_start is not None:
        first_a_off, _ = get_sample_byte_offset(a_track, a_start)
        if a_o_min is not None:
            if a_o_min < 0:
                raise ValueError(f"a_o_min cannot be negative: {a_o_min}")
            if a_o_min > first_a_off:
                raise ValueError(f"a_o_min ({a_o_min}) cannot exceed first audio chunk offset ({first_a_off})")
        if sidecar_audio_slice is None and o_min is not None and o_min > first_a_off:
            raise ValueError(f"o_min ({o_min}) cannot exceed first audio chunk offset ({first_a_off})")
    elif a_o_min is not None and a_o_min < 0:
        raise ValueError(f"a_o_min cannot be negative: {a_o_min}")

    sidecar_bytes = bytes(sidecar_audio_slice) if sidecar_audio_slice is not None else b""
    a_len = len(sidecar_bytes)
    if not has_audio and a_len > 0:
        raise ValueError("sidecar_audio_slice cannot be non-empty when audio track is omitted (a_start is None)")

    mdat_header = build_64bit_mdat_header(a_len + payload_len)
    assert len(mdat_header) == 16, f"mdat header must be 16 bytes, got {len(mdat_header)}"

    # Pass 1: Restructure sample tables with preliminary h_offset = 0
    new_v_stbl, v_eff_offsets = restructure_sample_table(v_track, v_start, v_end, o_min=o_min, h_offset=0)
    v_dur = sum(e.sample_count * e.sample_delta for e in new_v_stbl.stts)

    new_v_trak = Track(
        track_id=v_track.track_id,
        tkhd=Tkhd(
            version=v_track.tkhd.version,
            flags=v_track.tkhd.flags,
            creation_time=v_track.tkhd.creation_time,
            modification_time=v_track.tkhd.modification_time,
            track_id=v_track.tkhd.track_id,
            duration=v_track.tkhd.duration,
            layer=v_track.tkhd.layer,
            alternate_group=v_track.tkhd.alternate_group,
            volume=v_track.tkhd.volume,
            matrix=v_track.tkhd.matrix,
            width=v_track.tkhd.width,
            height=v_track.tkhd.height,
            width_raw=v_track.tkhd.width_raw,
            height_raw=v_track.tkhd.height_raw,
        ),
        mdhd=Mdhd(
            version=v_track.mdhd.version,
            flags=v_track.mdhd.flags,
            creation_time=v_track.mdhd.creation_time,
            modification_time=v_track.mdhd.modification_time,
            timescale=v_track.mdhd.timescale,
            duration=v_dur,
            language=v_track.mdhd.language,
        ),
        hdlr=v_track.hdlr,
        stbl=new_v_stbl,
        edts=None,
    )
    configure_video_elst(
        video_track=new_v_trak,
        v_start=v_start,
        movie_timescale=metadata.mvhd.timescale,
        original_video_track=v_track,
    )

    new_tracks = [new_v_trak]
    a_eff_offsets: list[int] = []

    if has_audio and a_track is not None and a_start is not None and a_end is not None:
        a_pass1_omin = (a_o_min if a_o_min is not None else 0) if sidecar_audio_slice is not None else o_min
        new_a_stbl, a_eff_offsets = restructure_sample_table(
            a_track,
            a_start,
            a_end,
            o_min=a_pass1_omin,
            h_offset=0,
        )
        new_a_trak = Track(
            track_id=a_track.track_id,
            tkhd=Tkhd(
                version=a_track.tkhd.version,
                flags=a_track.tkhd.flags,
                creation_time=a_track.tkhd.creation_time,
                modification_time=a_track.tkhd.modification_time,
                track_id=a_track.tkhd.track_id,
                duration=a_track.tkhd.duration,
                layer=a_track.tkhd.layer,
                alternate_group=a_track.tkhd.alternate_group,
                volume=a_track.tkhd.volume,
                matrix=a_track.tkhd.matrix,
                width=a_track.tkhd.width,
                height=a_track.tkhd.height,
                width_raw=a_track.tkhd.width_raw,
                height_raw=a_track.tkhd.height_raw,
            ),
            mdhd=Mdhd(
                version=a_track.mdhd.version,
                flags=a_track.mdhd.flags,
                creation_time=a_track.mdhd.creation_time,
                modification_time=a_track.mdhd.modification_time,
                timescale=a_track.mdhd.timescale,
                duration=a_track.mdhd.duration,
                language=a_track.mdhd.language,
            ),
            hdlr=a_track.hdlr,
            stbl=new_a_stbl,
            edts=None,
        )
        # Configure audio elst (Dual-Tier Adaptive AV Alignment: pass new_v_trak for Tier B mv coordination)
        configure_audio_elst(
            audio_track=new_a_trak,
            video_track=new_v_trak,
            v_start=v_start,
            a_start=a_start,
            movie_timescale=metadata.mvhd.timescale,
            original_audio_track=a_track,
            original_video_track=v_track,
        )

        new_tracks.append(new_a_trak)

    # Synchronize durations (Gate 3)
    new_mvhd = Mvhd(
        version=metadata.mvhd.version,
        flags=metadata.mvhd.flags,
        creation_time=metadata.mvhd.creation_time,
        modification_time=metadata.mvhd.modification_time,
        timescale=metadata.mvhd.timescale,
        duration=metadata.mvhd.duration,
        rate=metadata.mvhd.rate,
        volume=metadata.mvhd.volume,
        matrix=metadata.mvhd.matrix,
        next_track_id=max(t.track_id for t in new_tracks) + 1,
    )
    synchronize_durations(new_mvhd, new_tracks, metadata.mvhd.timescale)

    pass1_metadata = Mp4Metadata(mvhd=new_mvhd, tracks=new_tracks, udta_bytes=None)
    moov_p1 = serialize_moov(pass1_metadata)

    # Determine exact header size H_box
    h_box = len(ftyp_bytes) + len(moov_p1) + len(mdat_header)

    def _check_in_range(effs: list[int], stbl: SampleTable, lo: int, hi: int, label: str) -> None:
        sample_index = 0
        stsc_index = 0
        for chunk_index, eff in enumerate(effs, start=1):
            while stsc_index + 1 < len(stbl.stsc) and stbl.stsc[stsc_index + 1].first_chunk <= chunk_index:
                stsc_index += 1
            count = stbl.stsc[stsc_index].samples_per_chunk
            if isinstance(stbl.stsz, UniformStszTable) or stbl.default_sample_size > 0:
                size = getattr(stbl.stsz, "uniform_size", stbl.default_sample_size) * count
            else:
                size = sum(stbl.stsz[i] for i in range(sample_index, sample_index + count))
            sample_index += count
            chunk_end = eff + size
            if eff < lo or chunk_end > hi or (eff >= hi and not (hi == lo == eff and size == 0)):
                raise UnsupportedMp4LayoutError(
                    f"Non-monotonic {label} chunk span [{eff}, {chunk_end}) outside envelope [{lo}, {hi})"
                )

    # Pass 2: Re-write chunk offsets with exact h_box and dual-region routing
    if sidecar_audio_slice is not None:
        a_min = a_o_min if a_o_min is not None else (a_eff_offsets[0] if (has_audio and a_eff_offsets) else 0)
        v_min = o_min if o_min is not None else (v_eff_offsets[0] if v_eff_offsets else 0)
        if v_eff_offsets and v_min > v_eff_offsets[0]:
            raise ValueError(f"o_min ({v_min}) cannot exceed first video chunk offset ({v_eff_offsets[0]})")
        if has_audio and a_eff_offsets and a_min > a_eff_offsets[0]:
            raise ValueError(f"a_o_min ({a_min}) cannot exceed first audio chunk offset ({a_eff_offsets[0]})")
        v_hi = o_max if o_max is not None else (v_min + payload_len)
        _check_in_range(v_eff_offsets, new_v_stbl, v_min, v_hi, "video")
        if has_audio and a_eff_offsets and a_track is not None and a_end is not None:
            _, a_end_off = get_sample_byte_offset(a_track, a_end)
            _check_in_range(a_eff_offsets, new_a_stbl, a_min, a_end_off, "sidecar audio")
        new_v_stbl.chunk_offsets = array.array("Q", [h_box + a_len + (eff - v_min) for eff in v_eff_offsets])
        if has_audio:
            new_a_stbl.chunk_offsets = array.array("Q", [h_box + (eff - a_min) for eff in a_eff_offsets])
    else:
        if v_eff_offsets and o_min is not None and o_min > v_eff_offsets[0]:
            raise ValueError(f"o_min ({o_min}) cannot exceed first video chunk offset ({v_eff_offsets[0]})")
        if has_audio and a_eff_offsets and o_min is not None and o_min > a_eff_offsets[0]:
            raise ValueError(f"o_min ({o_min}) cannot exceed first audio chunk offset ({a_eff_offsets[0]})")
        env_hi = o_max if o_max is not None else (o_min + payload_len)
        _check_in_range(v_eff_offsets, new_v_stbl, o_min, env_hi, "video")
        if has_audio and a_eff_offsets:
            _check_in_range(a_eff_offsets, new_a_stbl, o_min, env_hi, "audio")
        new_v_stbl.chunk_offsets = array.array("Q", [h_box + (eff - o_min) for eff in v_eff_offsets])
        if has_audio:
            new_a_stbl.chunk_offsets = array.array("Q", [h_box + (eff - o_min) for eff in a_eff_offsets])

    moov_p2 = serialize_moov(pass1_metadata)
    if len(moov_p2) != len(moov_p1):
        raise IsoBmffError(
            f"Pass 2 moov size mismatch: Pass 1 was {len(moov_p1)} bytes, Pass 2 is {len(moov_p2)} bytes"
        )

    envelope_header = ftyp_bytes + moov_p2 + mdat_header + sidecar_bytes
    expected_header_len = h_box + a_len
    if len(envelope_header) != expected_header_len:
        raise IsoBmffError(
            f"Synthesized envelope header length mismatch: got {len(envelope_header)}, expected {expected_header_len}"
        )

    return envelope_header


__all__ = [
    "ChunkInfo",
    "SbgpEntry",
    "SbgpTable",
    "UniformStszTable",
    "array_to_big_endian_bytes",
    "build_64bit_mdat_header",
    "compute_envelope_bounds",
    "compute_sample_pts_dts",
    "configure_audio_elst",
    "configure_video_elst",
    "extract_track_edit_offset",
    "get_sample_byte_offset",
    "get_track_chunks",
    "make_box",
    "make_full_box",
    "remap_stss",
    "restructure_sample_table",
    "serialize_co64",
    "serialize_ctts",
    "serialize_edts",
    "serialize_hdlr",
    "serialize_mdhd",
    "serialize_minf",
    "serialize_moov",
    "serialize_mvhd",
    "serialize_sample_entry",
    "serialize_sbgp",
    "serialize_stsc",
    "serialize_stsd",
    "serialize_stss",
    "serialize_stsz",
    "serialize_stts",
    "serialize_tkhd",
    "serialize_trak",
    "slice_rle_table",
    "slice_sbgp_entries",
    "synchronize_durations",
    "synthesize_faststart_envelope_header",
]
