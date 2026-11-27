"""
Virtual Sample Slicing Engine (VSSE v2) Provider and Volume Planner.

This module provides:
1. Volume Envelope Planner (`plan_vsse_volumes`):
   Plans sample slicing boundaries for virtual MP4 volumes based on real keyframe
   tables (stss) and sample sizes (stsz), enforcing:
   - Volume size safety: H + (O_max - O_min) <= threshold_bytes.
   - Audio continuity invariant: A_start^(k+1) = A_end^(k) + 1.
   - Hardware AV alignment: delta = PTS(V_start) - PTS(A_start) >= 0.
   - Monotonicity and single-volume pass-through.
2. Virtual Envelope Chunk Provider (`VirtualEnvelopeChunkProvider`):
   Subclass of `BaseChunkProvider` providing seamless virtual MP4 streaming:
   - Synthesized in-memory FastStart header [0, H).
   - Range reads from underlying media source provider [H, H + payload_len).
   - Byte-exact three-state chunk routing across boundary H.
"""

from __future__ import annotations

import bisect
import logging
import os
import struct
from collections.abc import Sequence
from dataclasses import dataclass
from typing import NamedTuple

from app.core.iso_bmff.box_parser import Mp4Metadata, Track, UniformStszTable
from app.core.iso_bmff.box_synthesizer import (
    ChunkInfo,
    compute_envelope_bounds,
    compute_sample_pts_dts,
    extract_track_edit_offset,
    get_sample_byte_offset,
    get_track_chunks,
    slice_rle_table,
)
from app.core.iso_bmff.exceptions import IsoBmffError, NonInterleavedMp4Error, UnsupportedMp4LayoutError
from app.core.stream_chunk_provider import BaseChunkProvider

logger = logging.getLogger(__name__)

__all__ = [
    "VirtualEnvelopeChunkProvider",
    "VsseVolumePlan",
    "check_track_interleaving",
    "estimate_faststart_header_size",
    "find_audio_start_sample",
    "format_part_filename",
    "plan_vsse_volumes",
]

# Standard 32-byte ISO ftyp box used for FastStart header synthesis
DEFAULT_FTYP_BOX = b"\x00\x00\x00\x20ftypisom\x00\x00\x02\x00isomiso2mp41mp42"
assert len(DEFAULT_FTYP_BOX) == struct.unpack(">I", DEFAULT_FTYP_BOX[:4])[0], (
    f"DEFAULT_FTYP_BOX length mismatch: actual {len(DEFAULT_FTYP_BOX)} != declared {struct.unpack('>I', DEFAULT_FTYP_BOX[:4])[0]}"
)


@dataclass(frozen=True)
class VsseVolumePlan:
    """
    Specification for a single virtual MP4 volume in the VSSE channel.
    """

    part_index: int  # 1-based volume index (1..total_parts)
    total_parts: int  # Total number of volumes
    part_name: str  # Volume filename (e.g. video.part001.mp4 or video.mp4)
    v_start: int  # 1-based video start sample index (strictly a keyframe)
    v_end: int  # 1-based video end sample index (inclusive)
    a_start: int | None  # 1-based audio start sample index (None if video-only)
    a_end: int | None  # 1-based audio end sample index (inclusive, None if video-only)
    o_min: int  # Minimum media byte offset in source file
    o_max: int  # Maximum media byte end offset in source file
    payload_len: int  # Media payload length (o_max - o_min)
    estimated_header_size: int  # FastStart envelope header size H
    estimated_total_bytes: int  # Total virtual volume size (H + payload_len)
    start_sec: float = 0.0  # Video start timestamp in seconds
    end_sec: float = 0.0  # Video end timestamp in seconds
    duration_sec: float = 0.0  # Video duration in seconds
    a_o_min: int | None = None  # 本卷音频在 sidecar aac_payload 中的起始偏移
    a_o_max: int | None = None  # 本卷音频在 sidecar aac_payload 中的结束偏移
    a_payload_len: int = 0  # 本卷内联至 faststart_header 尾部的音频净荷字节数

    @property
    def file_name(self) -> str:
        """Alias for part_name."""
        return self.part_name

    @property
    def envelope_start(self) -> int:
        """Alias for o_min."""
        return self.o_min

    @property
    def envelope_end(self) -> int:
        """Alias for o_max."""
        return self.o_max

    @property
    def envelope_length(self) -> int:
        """Alias for payload_len."""
        return self.payload_len

    @property
    def total_bytes(self) -> int:
        """Alias for estimated_total_bytes."""
        return self.estimated_total_bytes

    @property
    def header_size(self) -> int:
        """Alias for estimated_header_size."""
        return self.estimated_header_size


def format_part_filename(file_name: str, part_index: int, total_parts: int) -> str:
    """
    Format volume filename following the project convention:
    - Single part (total_parts <= 1): keep original file_name.
    - Multiple parts (total_parts > 1): file.part001.mp4, file.part002.mp4, etc.
    """
    if total_parts <= 1:
        return file_name

    base, ext = os.path.splitext(file_name)
    if not ext:
        ext = ".mp4"
    return f"{base}.part{part_index:03d}{ext}"


def _compute_plan_timestamps(
    v_track: Track,
    v_start: int,
    v_end: int,
) -> tuple[float, float, float]:
    """Compute (start_sec, end_sec, duration_sec) for a video slice."""
    scale = v_track.mdhd.timescale or 1
    _, pts_start = compute_sample_pts_dts(v_track, v_start)
    start_sec = pts_start / scale

    sliced_stts = slice_rle_table(v_track.stbl.stts, v_start, v_end)
    dur_ticks = sum(e.sample_count * e.sample_delta for e in sliced_stts)
    dur_sec = dur_ticks / scale
    end_sec = start_sec + dur_sec
    return round(start_sec, 6), round(end_sec, 6), round(dur_sec, 6)


def _estimate_audio_header_size(
    a_track: Track,
    a_start: int,
    a_end: int,
    a_chunks: list[ChunkInfo] | None = None,
    a_chunk_ends: Sequence[int] | None = None,
) -> int:
    """Estimate serialized moov/trak overhead for an audio track slice."""
    a_sample_cnt = a_end - a_start + 1
    h_size = 400  # trak + tkhd + edts/elst + mdhd + hdlr + minf + smhd + dinf + stbl
    if isinstance(a_track.stbl.stsz, UniformStszTable) or a_track.stbl.default_sample_size > 0:
        h_size += 20
    else:
        h_size += 20 + a_sample_cnt * 4  # stsz
    h_size += 16 + len(a_track.stbl.stts) * 8  # stts
    if a_track.stbl.ctts:
        h_size += 16 + len(a_track.stbl.ctts) * 8
    if a_track.stbl.stss is not None:
        a_stss = a_track.stbl.stss
        ai_left = bisect.bisect_left(a_stss, a_start)
        ai_right = bisect.bisect_right(a_stss, a_end)
        h_size += 16 + max(0, ai_right - ai_left) * 4
    if a_chunks is None:
        a_chunks = get_track_chunks(a_track)
    if a_chunks:
        c_a_end_samples = a_chunk_ends if a_chunk_ends is not None else [c.end_sample for c in a_chunks]
        c_a_start = bisect.bisect_right(c_a_end_samples, a_start - 1)
        c_a_end = bisect.bisect_right(c_a_end_samples, a_end - 1)
        num_a_chunks = max(1, min(len(a_chunks), c_a_end + 1) - c_a_start)
        h_size += 16 + num_a_chunks * 8
        h_size += 16 + num_a_chunks * 12
    h_size += len(a_track.stbl.stsd.raw_box) or (len(a_track.stbl.stsd.entries) * 100 + 30)
    return h_size


def estimate_faststart_header_size(
    metadata: Mp4Metadata,
    v_start: int,
    v_end: int,
    a_start: int | None = None,
    a_end: int | None = None,
    v_chunks: list[ChunkInfo] | None = None,
    a_chunks: list[ChunkInfo] | None = None,
    v_chunk_ends: Sequence[int] | None = None,
    a_chunk_ends: Sequence[int] | None = None,
) -> int:
    """
    Fast O(1) estimate of synthesized faststart envelope header size H:
    ftyp (32B) + moov + 16B 64-bit mdat header.
    """
    v_track = metadata.video_track
    if v_track is None:
        raise IsoBmffError("Missing video track in metadata")

    # Base container overhead:
    # ftyp (32) + mdat header (16) + moov header (8) + mvhd (~120)
    h_size = 32 + 16 + 8 + 120

    # Video trak estimation
    v_sample_cnt = v_end - v_start + 1
    h_size += 350  # trak + tkhd + mdia + mdhd + hdlr + minf + vmhd + dinf + stbl headers
    if isinstance(v_track.stbl.stsz, UniformStszTable) or v_track.stbl.default_sample_size > 0:
        h_size += 20
    else:
        h_size += 20 + v_sample_cnt * 4  # stsz (version/flags + sample_size + count + entries)

    # stss
    if v_track.stbl.stss is not None:
        stss = v_track.stbl.stss
        i_left = bisect.bisect_left(stss, v_start)
        i_right = bisect.bisect_right(stss, v_end)
        sync_cnt = max(0, i_right - i_left)
        h_size += 16 + sync_cnt * 4

    # stts & ctts
    h_size += 16 + len(v_track.stbl.stts) * 8
    if v_track.stbl.ctts:
        h_size += 16 + len(v_track.stbl.ctts) * 8

    # Video chunk tables (co64 and stsc)
    if v_chunks is None:
        v_chunks = get_track_chunks(v_track)
    if v_chunks:
        c_end_samples = v_chunk_ends if v_chunk_ends is not None else [c.end_sample for c in v_chunks]
        c_start_idx = bisect.bisect_right(c_end_samples, v_start - 1)
        c_end_idx = bisect.bisect_right(c_end_samples, v_end - 1)
        num_v_chunks = max(1, min(len(v_chunks), c_end_idx + 1) - c_start_idx)
        h_size += 16 + num_v_chunks * 8  # co64
        h_size += 16 + num_v_chunks * 12  # stsc upper bound

    # Video stsd
    h_size += len(v_track.stbl.stsd.raw_box) or (len(v_track.stbl.stsd.entries) * 120 + 30)
    if v_track.stbl.sgpd_boxes:
        h_size += sum(len(b) for b in v_track.stbl.sgpd_boxes)
    if v_track.stbl.sbgp_boxes:
        h_size += sum(len(b) for b in v_track.stbl.sbgp_boxes)

    # Audio trak estimation (if present)
    a_track = metadata.audio_track
    if a_track is not None and a_start is not None and a_end is not None:
        h_size += _estimate_audio_header_size(a_track, a_start, a_end, a_chunks, a_chunk_ends)
        if a_track.stbl.sgpd_boxes:
            h_size += sum(len(b) for b in a_track.stbl.sgpd_boxes)
        if a_track.stbl.sbgp_boxes:
            h_size += sum(len(b) for b in a_track.stbl.sbgp_boxes)

    # Safety cushion for RLE entries or padding
    return h_size + 256


def _compute_or_estimate_header_size(
    meta: Mp4Metadata,
    v_start: int,
    v_end: int,
    a_start: int | None,
    a_end: int | None,
    o_min: int,
    o_max: int,
    payload_len: int,
    v_chunks: list[ChunkInfo] | None = None,
    a_chunks: list[ChunkInfo] | None = None,
    v_chunk_ends: Sequence[int] | None = None,
    a_chunk_ends: Sequence[int] | None = None,
) -> int:
    """
    Fast analytical estimation of synthesized faststart envelope header size H.
    """
    return estimate_faststart_header_size(
        meta, v_start, v_end, a_start, a_end, v_chunks, a_chunks, v_chunk_ends, a_chunk_ends
    )


def check_track_interleaving(
    v_track: Track,
    a_track: Track | None,
    threshold_bytes: int,
    v_chunks: list[ChunkInfo],
    a_chunks: list[ChunkInfo] | None,
) -> None:
    """
    Circuit breaker for non-interleaved or severely skewed MP4 media.

    If video and audio chunks are physically partitioned (e.g. all video chunks
    at the start of file, all audio at the end) or the spatial-temporal offset
    distance exceeds safe envelope bounds (e.g. > threshold_bytes // 2), raises
    NonInterleavedMp4Error to trigger seamless fallback to fMP4 remuxing.
    """
    if a_track is None or not a_chunks or not v_chunks:
        return

    # Check 1: Strict complete physical separation (video entirely before audio, or vice versa)
    v_first_off = v_chunks[0].base_offset
    v_last_off = v_chunks[-1].base_offset
    a_first_off = a_chunks[0].base_offset
    a_last_off = a_chunks[-1].base_offset

    if v_last_off < a_first_off:
        raise NonInterleavedMp4Error(
            f"Non-interleaved MP4 detected: all video chunks (ends at {v_last_off}) "
            f"precede audio chunks (starts at {a_first_off}). "
            f"Envelope slicing would cause volume explosion."
        )
    if a_last_off < v_first_off:
        raise NonInterleavedMp4Error(
            f"Non-interleaved MP4 detected: all audio chunks (ends at {a_last_off}) "
            f"precede video chunks (starts at {v_first_off}). "
            f"Envelope slicing would cause volume explosion."
        )

    # Check 2: Sample interleaving skew across temporal checkpoints (25%, 50%, 75%)
    total_v = len(v_track.stbl.stsz)
    total_a = len(a_track.stbl.stsz)
    max_allowed_skew = max(100 * 1024 * 1024, int(threshold_bytes * 0.5))

    for ratio in (0.25, 0.50, 0.75):
        sample_v = max(1, min(total_v, int(total_v * ratio)))
        sample_a = max(1, min(total_a, int(total_a * ratio)))
        off_v_start, _ = get_sample_byte_offset(v_track, sample_v, v_chunks)
        off_a_start, _ = get_sample_byte_offset(a_track, sample_a, a_chunks)
        skew = abs(off_v_start - off_a_start)
        if skew > max_allowed_skew:
            raise NonInterleavedMp4Error(
                f"Severe non-interleaved track skew detected at {int(ratio * 100)}% ({skew / (1024 * 1024):.1f}MB > "
                f"{max_allowed_skew / (1024 * 1024):.1f}MB allowed). Envelope slicing would cause volume explosion."
            )


def find_audio_start_sample(
    a_track: Track,
    pts_v_sec: float,
    min_audio_sample: int,
    c_sec: float = 0.0,
) -> int | None:
    """
    Find largest audio sample index in [min_audio_sample, total_a] such that
    PTS(a) / timescale_A <= pts_v_sec + c_sec + 1e-9.

    Returns None if no sample in range satisfies effective_delta >= 0.
    """
    total_a = len(a_track.stbl.stsz)
    if min_audio_sample > total_a:
        return None

    a_timescale = a_track.mdhd.timescale or 1
    target_pts_sec = pts_v_sec + c_sec
    # Check if even min_audio_sample exceeds video target PTS
    _, pts_min = compute_sample_pts_dts(a_track, min_audio_sample)
    if (pts_min / a_timescale) > target_pts_sec + 1e-6:
        return None

    low = min_audio_sample
    high = total_a
    best = min_audio_sample

    while low <= high:
        mid = (low + high) // 2
        _, pts_a = compute_sample_pts_dts(a_track, mid)
        pts_a_sec = pts_a / a_timescale
        if pts_a_sec <= target_pts_sec + 1e-9:
            best = mid
            low = mid + 1
        else:
            high = mid - 1

    return best


class _SplitEval(NamedTuple):
    """evaluate_split 的结构化求值结果 (替代 10 元素裸元组, 消除消费端魔数索引)。"""

    v_end: int
    a_end: int | None
    a_start_next: int | None
    o_min: int
    o_max: int
    payload: int
    total_est: int
    a_o_min: int | None
    a_o_max: int | None
    a_payload: int


def _compute_volume_bounds(
    meta: Mp4Metadata,
    *,
    v_start: int,
    v_end: int,
    a_start: int | None,
    a_end: int | None,
    sidecar_audio: bool,
    v_chunks: list[ChunkInfo],
    a_chunks: list[ChunkInfo] | None,
) -> tuple[int, int, int, int | None, int | None, int]:
    """计算单候选分卷的包络与音频切片, 返回 (o_min, o_max, payload, a_o_min, a_o_max, a_payload)。

    - 标准 Tier 1: envelope 为音视频联合远端区间, a_o_* 恒为 None;
    - Hybrid Tier 2 (sidecar_audio=True): envelope 为纯视频远端区间,
      音频切片边界来自内存 sidecar aac_payload (a_start/a_end 为其样本区间)。
    """
    a_track = meta.audio_track
    if sidecar_audio:
        o_min, o_max, payload = compute_envelope_bounds(
            meta, v_start, v_end, None, None, v_chunks=v_chunks, a_chunks=None
        )
        if a_track is not None and a_start is not None and a_end is not None and a_start <= a_end:
            a_o_min = get_sample_byte_offset(a_track, a_start, a_chunks)[0]
            a_o_max = get_sample_byte_offset(a_track, a_end, a_chunks)[1]
            a_payload = a_o_max - a_o_min
        else:
            a_o_min = None
            a_o_max = None
            a_payload = 0
    else:
        o_min, o_max, payload = compute_envelope_bounds(
            meta, v_start, v_end, a_start, a_end, v_chunks=v_chunks, a_chunks=a_chunks
        )
        a_o_min = None
        a_o_max = None
        a_payload = 0
    return o_min, o_max, payload, a_o_min, a_o_max, a_payload


def plan_vsse_volumes(
    meta: Mp4Metadata,
    threshold_bytes: int,
    file_name: str,
    safety_margin_bytes: int = 20 * 1024 * 1024,
    sidecar_audio: bool = False,
) -> list[VsseVolumePlan]:
    """
    Volume envelope planner for the VSSE channel.

    Calculates volume envelope boundaries based on real keyframe table `stss`
    and sample sizes `stsz`, enforcing:
    1. Keyframe alignment: Every volume strictly starts on a sync sample (keyframe).
    2. Audio continuity: A_start^(k+1) = A_end^(k) + 1 (no audio gaps or overlaps).
    3. Audio PTS alignment: PTS(A_start) <= PTS(V_start) (delta >= 0).
    4. Envelope bounds:
       - Standard Tier 1:
           O_min = min(Offset(V_start), Offset(A_start)) (or Offset(V_start) if video-only)
           O_max = max(End(V_end), End(A_end)) (or End(V_end) if video-only)
           payload_len = O_max - O_min
       - Hybrid Tier 2 (sidecar_audio=True):
           O_min = Offset(V_start) (strictly remote video start)
           O_max = End(V_end) (strictly remote video end)
           payload_len = O_max - O_min (strictly remote video payload)
           a_o_min = Offset(A_start) in sidecar aac_payload
           a_o_max = End(A_end) in sidecar aac_payload
           a_payload_len = a_o_max - a_o_min (inlined to faststart header)
    5. Size threshold constraint:
       Each volume's virtual size H + payload_len <= threshold_bytes (e.g. 2000 MiB (1.95 GiB) or 4000 MiB (3.91 GiB)).
    6. Single volume support:
       If the entire file fits within threshold_bytes, returns a single volume with
       file_name preserved.
    7. Multi-part naming:
       file.part001.mp4, file.part002.mp4, etc.
    """
    if threshold_bytes <= 0:
        raise ValueError(f"threshold_bytes must be positive, got {threshold_bytes}")
    if safety_margin_bytes < 0:
        raise ValueError(f"safety_margin_bytes cannot be negative, got {safety_margin_bytes}")
    if not file_name or not file_name.strip():
        raise ValueError("file_name cannot be empty")

    v_track = meta.video_track
    if v_track is None:
        raise IsoBmffError("Missing video track in metadata")

    total_v = len(v_track.stbl.stsz)
    if total_v == 0:
        raise IsoBmffError("Video track has 0 samples")

    a_track = meta.audio_track
    if a_track is not None and len(a_track.stbl.stsz) > 0:
        has_audio = True
        total_a = len(a_track.stbl.stsz)
        a_chunks = get_track_chunks(a_track)
    else:
        has_audio = False
        total_a = 0
        a_chunks = None

    # Precompute track chunks and chunk end samples for efficiency
    v_chunks = get_track_chunks(v_track)
    v_chunk_ends = tuple(c.end_sample for c in v_chunks)
    a_chunk_ends = tuple(c.end_sample for c in a_chunks) if a_chunks else None

    # Sync samples / keyframes (keep range lazy to avoid O(total_v) allocation on malformed uniform tables)
    keyframes: Sequence[int]
    if v_track.stbl.stss is not None and len(v_track.stbl.stss) > 0:
        keyframes = v_track.stbl.stss
    else:
        keyframes = range(1, total_v + 1)

    # Determine effective safety margin (clamp if threshold is small in synthetic tests)
    if safety_margin_bytes >= threshold_bytes:
        effective_margin = max(0, int(threshold_bytes * 0.05))
    else:
        effective_margin = safety_margin_bytes
    max_volume_bytes = threshold_bytes - effective_margin

    v_timescale = v_track.mdhd.timescale or 1
    m_scale = meta.mvhd.timescale or 1000
    c_sec = 0.0
    if has_audio and a_track is not None:
        a_timescale = a_track.mdhd.timescale or 1
        v_empty_sec, mv = extract_track_edit_offset(v_track, m_scale)
        a_empty_sec, ma = extract_track_edit_offset(a_track, m_scale)
        c_sec = (ma / a_timescale - a_empty_sec) - (mv / v_timescale - v_empty_sec)
        _, pts_first_a = compute_sample_pts_dts(a_track, 1)
        total_audio_dur_ticks = sum(e.sample_count * e.sample_delta for e in a_track.stbl.stts)
        audio_end_sec = (pts_first_a + total_audio_dur_ticks - ma) / a_timescale + a_empty_sec
    else:
        a_timescale = 1
        audio_end_sec = 0.0

    # Check if single volume fits entire file
    single_omin, single_omax, single_payload, a_omin, a_omax, a_payload = _compute_volume_bounds(
        meta,
        v_start=1,
        v_end=total_v,
        a_start=1 if has_audio else None,
        a_end=total_a if has_audio else None,
        sidecar_audio=sidecar_audio,
        v_chunks=v_chunks,
        a_chunks=a_chunks,
    )
    single_h_boxes = _compute_or_estimate_header_size(
        meta,
        v_start=1,
        v_end=total_v,
        a_start=1 if has_audio else None,
        a_end=total_a if has_audio else None,
        o_min=single_omin,
        o_max=single_omax,
        payload_len=single_payload,
        v_chunks=v_chunks,
        a_chunks=a_chunks,
        v_chunk_ends=v_chunk_ends,
        a_chunk_ends=a_chunk_ends,
    )
    single_h = single_h_boxes + a_payload
    single_total = single_h + single_payload

    if single_total <= max_volume_bytes:
        # Whole file fits in 1 volume!
        start_sec, end_sec, dur_sec = _compute_plan_timestamps(v_track, 1, total_v)
        return [
            VsseVolumePlan(
                part_index=1,
                total_parts=1,
                part_name=file_name,
                v_start=1,
                v_end=total_v,
                a_start=1 if has_audio else None,
                a_end=total_a if has_audio else None,
                o_min=single_omin,
                o_max=single_omax,
                payload_len=single_payload,
                estimated_header_size=single_h,
                estimated_total_bytes=single_total,
                start_sec=start_sec,
                end_sec=end_sec,
                duration_sec=dur_sec,
                a_o_min=a_omin,
                a_o_max=a_omax,
                a_payload_len=a_payload,
            )
        ]

    # Circuit Breaker: Verify track interleaving before multi-volume split planning (skipped for sidecar audio)
    if not sidecar_audio:
        check_track_interleaving(v_track, a_track, threshold_bytes, v_chunks, a_chunks)

    def evaluate_split(cand_kf: int, cur_v: int, cur_a: int | None) -> _SplitEval | None:
        v_end_c = cand_kf - 1
        if not has_audio or a_track is None or cur_a is None:
            a_end_c = None
            a_start_next = None
        else:
            _, pts_v_c = compute_sample_pts_dts(v_track, cand_kf)
            pts_v_c_sec = pts_v_c / v_timescale
            if pts_v_c_sec + c_sec >= audio_end_sec - 1e-6:
                # Audio track has completely ended before cand_kf
                a_end_c = total_a
                a_start_next = None
            else:
                a_start_next = find_audio_start_sample(a_track, pts_v_c_sec, min_audio_sample=cur_a + 1, c_sec=c_sec)
                if a_start_next is None:
                    # Audio starts later than cand_kf (effective_delta < 0 for next volume).
                    # With Dual-Tier Adaptive AV Alignment (Empty Edit support), if cur_a + 1 <= total_a,
                    # assign at least 1 audio sample (cur_a) to current volume and start next volume at cur_a + 1.
                    if cur_a + 1 <= total_a:
                        a_start_next = cur_a + 1
                    else:
                        return None
                a_end_c = a_start_next - 1

        o_min_c, o_max_c, payload_c, a_omin_c, a_omax_c, a_payload_c = _compute_volume_bounds(
            meta,
            v_start=cur_v,
            v_end=v_end_c,
            a_start=cur_a,
            a_end=a_end_c,
            sidecar_audio=sidecar_audio,
            v_chunks=v_chunks,
            a_chunks=a_chunks,
        )
        h_est = estimate_faststart_header_size(
            meta,
            cur_v,
            v_end_c,
            cur_a,
            a_end_c,
            v_chunks,
            a_chunks,
            v_chunk_ends=v_chunk_ends,
            a_chunk_ends=a_chunk_ends,
        )
        total_est = h_est + a_payload_c + payload_c
        return _SplitEval(
            v_end=v_end_c,
            a_end=a_end_c,
            a_start_next=a_start_next,
            o_min=o_min_c,
            o_max=o_max_c,
            payload=payload_c,
            total_est=total_est,
            a_o_min=a_omin_c,
            a_o_max=a_omax_c,
            a_payload=a_payload_c,
        )

    # Multiple volumes needed
    plans: list[VsseVolumePlan] = []
    cur_v_start = 1
    cur_a_start = 1 if has_audio else None
    part_idx = 1

    while cur_v_start <= total_v:
        # Check if the entire remainder fits within threshold_bytes
        rem_a_start = cur_a_start
        rem_a_end = total_a if (has_audio and cur_a_start is not None) else None
        if rem_a_start is not None and rem_a_end is not None and rem_a_start > rem_a_end:
            rem_a_start = None
            rem_a_end = None

        rem_omin, rem_omax, rem_payload, rem_a_omin, rem_a_omax, rem_a_payload = _compute_volume_bounds(
            meta,
            v_start=cur_v_start,
            v_end=total_v,
            a_start=rem_a_start,
            a_end=rem_a_end,
            sidecar_audio=sidecar_audio,
            v_chunks=v_chunks,
            a_chunks=a_chunks,
        )
        rem_h_boxes = _compute_or_estimate_header_size(
            meta,
            v_start=cur_v_start,
            v_end=total_v,
            a_start=rem_a_start,
            a_end=rem_a_end,
            o_min=rem_omin,
            o_max=rem_omax,
            payload_len=rem_payload,
            v_chunks=v_chunks,
            a_chunks=a_chunks,
            v_chunk_ends=v_chunk_ends,
            a_chunk_ends=a_chunk_ends,
        )
        rem_h = rem_h_boxes + rem_a_payload
        rem_total = rem_h + rem_payload

        if rem_total <= max_volume_bytes:
            # Remainder fits in final volume
            start_sec, end_sec, dur_sec = _compute_plan_timestamps(v_track, cur_v_start, total_v)
            plans.append(
                VsseVolumePlan(
                    part_index=part_idx,
                    total_parts=part_idx,
                    part_name="",
                    v_start=cur_v_start,
                    v_end=total_v,
                    a_start=rem_a_start,
                    a_end=rem_a_end,
                    o_min=rem_omin,
                    o_max=rem_omax,
                    payload_len=rem_payload,
                    estimated_header_size=rem_h,
                    estimated_total_bytes=rem_total,
                    start_sec=start_sec,
                    end_sec=end_sec,
                    duration_sec=dur_sec,
                    a_o_min=rem_a_omin,
                    a_o_max=rem_a_omax,
                    a_payload_len=rem_a_payload,
                )
            )
            break

        # Candidate keyframes strictly greater than cur_v_start
        kf_idx = bisect.bisect_right(keyframes, cur_v_start)
        if kf_idx >= len(keyframes):
            if rem_total > threshold_bytes:
                raise UnsupportedMp4LayoutError(
                    f"VSSE remainder volume [{cur_v_start}..{total_v}] ({rem_total} bytes) "
                    f"exceeds hard threshold {threshold_bytes} bytes with no further keyframe"
                )
            # No further keyframe exists; remainder still fits within hard threshold_bytes
            start_sec, end_sec, dur_sec = _compute_plan_timestamps(v_track, cur_v_start, total_v)
            plans.append(
                VsseVolumePlan(
                    part_index=part_idx,
                    total_parts=part_idx,
                    part_name="",
                    v_start=cur_v_start,
                    v_end=total_v,
                    a_start=rem_a_start,
                    a_end=rem_a_end,
                    o_min=rem_omin,
                    o_max=rem_omax,
                    payload_len=rem_payload,
                    estimated_header_size=rem_h,
                    estimated_total_bytes=rem_total,
                    start_sec=start_sec,
                    end_sec=end_sec,
                    duration_sec=dur_sec,
                    a_o_min=rem_a_omin,
                    a_o_max=rem_a_omax,
                    a_payload_len=rem_a_payload,
                )
            )
            break

        cand_kfs = keyframes[kf_idx:]
        low = 0
        high = len(cand_kfs) - 1
        best_cand_idx = -1
        best_cand_eval = None

        while low <= high:
            mid = (low + high) // 2
            ev = evaluate_split(cand_kfs[mid], cur_v_start, cur_a_start)
            if ev is None:
                # Audio alignment impossible at this keyframe (delta < 0), must look later
                low = mid + 1
                continue

            total_est = ev.total_est
            if total_est <= max_volume_bytes:
                best_cand_idx = mid
                best_cand_eval = ev
                low = mid + 1
            else:
                high = mid - 1

        if best_cand_idx == -1:
            # Even a single GOP exceeds max_volume_bytes; take the first valid keyframe
            for mid, kf in enumerate(cand_kfs):
                ev = evaluate_split(kf, cur_v_start, cur_a_start)
                if ev is not None:
                    best_cand_idx = mid
                    best_cand_eval = ev
                    break

        if best_cand_eval is None:
            if rem_total > threshold_bytes:
                raise UnsupportedMp4LayoutError(
                    f"VSSE remainder volume [{cur_v_start}..{total_v}] ({rem_total} bytes) "
                    f"exceeds hard threshold {threshold_bytes} bytes without valid alignment keyframe"
                )
            # No candidate keyframe admits valid alignment; remainder still fits within hard threshold_bytes
            start_sec, end_sec, dur_sec = _compute_plan_timestamps(v_track, cur_v_start, total_v)
            plans.append(
                VsseVolumePlan(
                    part_index=part_idx,
                    total_parts=part_idx,
                    part_name="",
                    v_start=cur_v_start,
                    v_end=total_v,
                    a_start=rem_a_start,
                    a_end=rem_a_end,
                    o_min=rem_omin,
                    o_max=rem_omax,
                    payload_len=rem_payload,
                    estimated_header_size=rem_h,
                    estimated_total_bytes=rem_total,
                    start_sec=start_sec,
                    end_sec=end_sec,
                    duration_sec=dur_sec,
                    a_o_min=rem_a_omin,
                    a_o_max=rem_a_omax,
                    a_payload_len=rem_a_payload,
                )
            )
            break

        (
            chosen_v_end,
            chosen_a_end,
            chosen_a_start_next,
            chosen_omin,
            chosen_omax,
            chosen_payload,
            _,
            chosen_a_omin,
            chosen_a_omax,
            chosen_a_payload,
        ) = best_cand_eval
        chosen_next_kf = cand_kfs[best_cand_idx]

        chosen_h_boxes = _compute_or_estimate_header_size(
            meta,
            v_start=cur_v_start,
            v_end=chosen_v_end,
            a_start=cur_a_start,
            a_end=chosen_a_end,
            o_min=chosen_omin,
            o_max=chosen_omax,
            payload_len=chosen_payload,
            v_chunks=v_chunks,
            a_chunks=a_chunks,
            v_chunk_ends=v_chunk_ends,
            a_chunk_ends=a_chunk_ends,
        )
        chosen_h = chosen_h_boxes + (chosen_a_payload if sidecar_audio else 0)

        # Back off if actual synthesized header pushes total size beyond threshold_bytes
        current_idx = best_cand_idx
        while chosen_h + chosen_payload > threshold_bytes and current_idx > 0:
            current_idx -= 1
            ev_smaller = evaluate_split(cand_kfs[current_idx], cur_v_start, cur_a_start)
            if ev_smaller is not None:
                h_sm_boxes = _compute_or_estimate_header_size(
                    meta,
                    cur_v_start,
                    ev_smaller.v_end,
                    cur_a_start,
                    ev_smaller.a_end,
                    ev_smaller.o_min,
                    ev_smaller.o_max,
                    ev_smaller.payload,
                    v_chunks,
                    a_chunks,
                    v_chunk_ends=v_chunk_ends,
                    a_chunk_ends=a_chunk_ends,
                )
                h_sm = h_sm_boxes + ev_smaller.a_payload
                if h_sm + ev_smaller.payload <= threshold_bytes:
                    best_cand_idx = current_idx
                    best_cand_eval = ev_smaller
                    (
                        chosen_v_end,
                        chosen_a_end,
                        chosen_a_start_next,
                        chosen_omin,
                        chosen_omax,
                        chosen_payload,
                        _,
                        chosen_a_omin,
                        chosen_a_omax,
                        chosen_a_payload,
                    ) = ev_smaller
                    chosen_h = h_sm
                    chosen_next_kf = cand_kfs[current_idx]
                    break

        if chosen_h + chosen_payload > threshold_bytes:
            raise UnsupportedMp4LayoutError(
                f"Single GOP [{cur_v_start}..{chosen_v_end}] ({chosen_h + chosen_payload} bytes) "
                f"exceeds hard threshold {threshold_bytes} bytes"
            )

        start_sec, end_sec, dur_sec = _compute_plan_timestamps(v_track, cur_v_start, chosen_v_end)

        plans.append(
            VsseVolumePlan(
                part_index=part_idx,
                total_parts=part_idx,
                part_name="",
                v_start=cur_v_start,
                v_end=chosen_v_end,
                a_start=cur_a_start,
                a_end=chosen_a_end,
                o_min=chosen_omin,
                o_max=chosen_omax,
                payload_len=chosen_payload,
                estimated_header_size=chosen_h,
                estimated_total_bytes=chosen_h + chosen_payload,
                start_sec=start_sec,
                end_sec=end_sec,
                duration_sec=dur_sec,
                a_o_min=chosen_a_omin,
                a_o_max=chosen_a_omax,
                a_payload_len=chosen_a_payload,
            )
        )

        cur_v_start = chosen_next_kf
        cur_a_start = chosen_a_start_next
        if cur_a_start is not None and cur_a_start > total_a:
            cur_a_start = None
        part_idx += 1

    # Post-process: assign total_parts and final part_name
    total_parts = len(plans)
    final_plans: list[VsseVolumePlan] = []
    for p in plans:
        p_name = format_part_filename(file_name, p.part_index, total_parts)
        final_plans.append(
            VsseVolumePlan(
                part_index=p.part_index,
                total_parts=total_parts,
                part_name=p_name,
                v_start=p.v_start,
                v_end=p.v_end,
                a_start=p.a_start,
                a_end=p.a_end,
                o_min=p.o_min,
                o_max=p.o_max,
                payload_len=p.payload_len,
                estimated_header_size=p.estimated_header_size,
                estimated_total_bytes=p.estimated_total_bytes,
                start_sec=p.start_sec,
                end_sec=p.end_sec,
                duration_sec=p.duration_sec,
                a_o_min=p.a_o_min,
                a_o_max=p.a_o_max,
                a_payload_len=p.a_payload_len,
            )
        )

    return final_plans


class VirtualEnvelopeChunkProvider(BaseChunkProvider):
    """
    Virtual MP4 Envelope Chunk Provider for the VSSE channel.

    Combines:
    - Synthesized FastStart envelope header in memory [0, H).
    - Remote media payload Range read from source provider [H, H + envelope_length).

    Three-state read routing:
    - Case 1: Pure in-memory FastStart header read (offset + size <= H).
    - Case 2: Pure remote Range read (offset >= H).
    - Case 3: Boundary crossing read (head from header, tail from remote source).

    Standard BaseChunkProvider contract:
    - offset >= total_bytes or size <= 0 raises ValueError.
    - offset + size > total_bytes is clamped to total_bytes - offset.
    - aclose() is a no-op to avoid closing the underlying shared connection.
    """

    def __init__(
        self,
        file_id: str,
        file_name: str,
        faststart_header: bytes,
        source_provider: BaseChunkProvider,
        envelope_start: int,
        envelope_length: int,
    ) -> None:
        if not file_id or not str(file_id).strip():
            raise ValueError("file_id cannot be empty")
        if not file_name or not str(file_name).strip():
            raise ValueError("file_name cannot be empty")
        if faststart_header is None:
            raise ValueError("faststart_header cannot be None")
        if len(faststart_header) == 0:
            raise ValueError("faststart_header cannot be empty")
        if source_provider is None:
            raise ValueError("source_provider cannot be None")
        if envelope_start < 0:
            raise ValueError(f"envelope_start cannot be negative: {envelope_start}")
        if envelope_length < 0:
            raise ValueError(f"envelope_length cannot be negative: {envelope_length}")

        if hasattr(source_provider, "total_bytes") and source_provider.total_bytes is not None:
            if envelope_start + envelope_length > source_provider.total_bytes:
                raise ValueError(
                    f"切片超出源范围: envelope_start({envelope_start}) + envelope_length({envelope_length}) = "
                    f"{envelope_start + envelope_length} > source_provider.total_bytes({source_provider.total_bytes})"
                )

        self.file_id = str(file_id)
        self.file_name = str(file_name)
        self._faststart_header = bytes(faststart_header)
        self._source_provider = source_provider
        self.envelope_start = envelope_start
        self.envelope_length = envelope_length
        self._header_len = len(self._faststart_header)
        self.total_bytes = self._header_len + self.envelope_length

    @property
    def faststart_header(self) -> bytes:
        return self._faststart_header

    @property
    def header_len(self) -> int:
        return self._header_len

    @property
    def source_provider(self) -> BaseChunkProvider:
        return self._source_provider

    async def read_chunk(self, offset: int, size: int) -> bytes:
        """
        Read [offset, offset + size) bytes from the virtual envelope MP4 stream.
        """
        if offset < 0:
            raise ValueError(f"读取偏移不能为负数: offset={offset}")
        if size <= 0:
            raise ValueError(f"非法读取长度: size={size}")
        if offset >= self.total_bytes:
            raise ValueError(f"读取偏移越界: offset={offset} >= total_bytes={self.total_bytes}")

        effective_size = min(size, self.total_bytes - offset)
        H = self._header_len

        # Case 1: Pure in-memory FastStart header read
        if offset + effective_size <= H:
            return self._faststart_header[offset : offset + effective_size]

        # Case 2: Pure remote Range read
        if offset >= H:
            remote_offset = self.envelope_start + (offset - H)
            data = await self._source_provider.read_chunk(remote_offset, effective_size)
            if len(data) > effective_size:
                return data[:effective_size]
            return data

        # Case 3: Boundary crossing (head from memory, tail from remote source)
        head_len = H - offset
        head_bytes = self._faststart_header[offset:H]
        tail_len = effective_size - head_len
        remote_offset = self.envelope_start
        tail_bytes = await self._source_provider.read_chunk(remote_offset, tail_len)
        if len(tail_bytes) > tail_len:
            tail_bytes = tail_bytes[:tail_len]
        return head_bytes + tail_bytes

    async def aclose(self) -> None:
        """
        No-op to avoid closing the underlying shared connection.
        """
