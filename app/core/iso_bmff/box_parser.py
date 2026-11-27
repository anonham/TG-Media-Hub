"""
Pure-function ISO-BMFF Box Parser.

Accepts pure bytes / memoryview and produces strongly-typed metadata structures
using compact native C arrays (array.array) to enforce strict memory budgets
(< 4.6MB RSS per million samples) and cross-platform Big-Endian / Little-Endian safety.
"""

from __future__ import annotations

import array
import enum
import logging
import math
import struct
import sys
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any, overload

from app.core.iso_bmff.exceptions import (
    CompactSampleSizeError,
    FragmentedMp4Error,
    IsoBmffParseError,
    MultipleMdatError,
    UnsupportedCodecError,
)

logger = logging.getLogger(__name__)

BytesLike = bytes | bytearray | memoryview

SUPPORTED_VIDEO_CODECS = {b"avc1", b"avc3", b"hvc1", b"hev1", b"av01"}
SUPPORTED_AUDIO_CODECS = {b"mp4a"}
AAC_ESDS_OBJECT_TYPES = frozenset({0x40, 0x66, 0x67, 0x68})
_ESDS_NON_AAC_CODEC_NAMES: dict[int, str] = {
    0x69: "mp3",
    0x6B: "mp3",
    0xA5: "ac3",
    0xA6: "eac3",
    0xA9: "dts",
    0xAD: "opus",
    0xDD: "vorbis",
}


@dataclass(slots=True)
class BoxHeader:
    """Header information of an ISO-BMFF box."""

    box_type: bytes
    offset: int
    size: int
    header_size: int

    @property
    def type_str(self) -> str:
        try:
            return self.box_type.decode("ascii", errors="replace")
        except Exception:
            return repr(self.box_type)

    @property
    def payload_offset(self) -> int:
        return self.offset + self.header_size

    @property
    def payload_size(self) -> int:
        return self.size - self.header_size


@dataclass(slots=True)
class Mvhd:
    """Movie Header Box (mvhd)."""

    version: int
    flags: int
    creation_time: int
    modification_time: int
    timescale: int
    duration: int
    rate: int = 0x00010000
    volume: int = 0x0100
    matrix: tuple[int, ...] = (0x00010000, 0, 0, 0, 0x00010000, 0, 0, 0, 0x40000000)
    next_track_id: int = 1


@dataclass(slots=True)
class Tkhd:
    """Track Header Box (tkhd)."""

    version: int
    flags: int
    creation_time: int
    modification_time: int
    track_id: int
    duration: int  # in movie timescale
    layer: int = 0
    alternate_group: int = 0
    volume: int = 0
    matrix: tuple[int, ...] = (0x00010000, 0, 0, 0, 0x00010000, 0, 0, 0, 0x40000000)
    width: float = 0.0
    height: float = 0.0
    width_raw: int = 0
    height_raw: int = 0


def rotation_degrees_from_matrix(matrix: Sequence[int] | None) -> int:
    """Extract orthogonal clockwise rotation angle (0, 90, 180, 270) from an ISO-BMFF 3x3 tkhd matrix.

    Pure display-dimension helper: does not mutate Tkhd or SampleEntry values used by muxers.
    """
    if not matrix or len(matrix) < 5:
        return 0
    a = matrix[0] / 65536.0
    b = matrix[1] / 65536.0
    c = matrix[3] / 65536.0
    d = matrix[4] / 65536.0
    if (abs(a) < 1e-6 and abs(b) < 1e-6) or (abs(c) < 1e-6 and abs(d) < 1e-6):
        return 0
    if abs(a) < 1e-3 and abs(d) < 1e-3 and abs(b) > 1e-3 and abs(c) > 1e-3:
        angle = round(math.degrees(math.atan2(b, a))) % 360
        return 90 if angle == 90 else 270
    if abs(b) < 1e-3 and abs(c) < 1e-3 and abs(a) > 1e-3 and abs(d) > 1e-3:
        return 180 if (a < 0 and d < 0) else 0
    return 0


def display_dimensions(width: int, height: int, rotation_deg: int) -> tuple[int, int]:
    """Compute display (width, height) by applying orthogonal 90/270-degree rotation once."""
    if width <= 0 or height <= 0:
        return width, height
    if (rotation_deg % 180) == 90:
        return height, width
    return width, height


def scaled_display_width(base_pixels: int, numerator: int, denominator: int) -> int:
    """Scale a pixel dimension by an aspect ratio, returning zero for invalid display width."""
    if min(base_pixels, numerator, denominator) <= 0:
        return 0
    width = (base_pixels * numerator + denominator // 2) // denominator
    return width if 0 < width < (1 << 31) else 0  # Telegram w is a signed TL int.


def has_valid_display_dimensions(width: int, height: int) -> bool:
    return 0 < width < (1 << 31) and 0 < height < (1 << 31)


def parse_track_aperture_dimensions(payload: BytesLike) -> tuple[int, int] | None:
    """Read QuickTime clean aperture dimensions, with production aperture as fallback."""
    view = memoryview(payload)
    production = None
    try:
        for kind, pos, size, header in iter_boxes(view):
            start = pos + header
            if kind not in (b"clef", b"prof") or size - header < 12 or view[start] != 0:
                continue
            width, height = struct.unpack_from(">II", view, start + 4)
            dimensions = (width >> 16, height >> 16)
            if not has_valid_display_dimensions(*dimensions):
                continue
            if kind == b"clef":
                return dimensions
            production = dimensions
    except IsoBmffParseError:
        return production  # Optional malformed metadata must not invalidate the track.
    return production


def track_display_dimensions(
    aperture: tuple[int, int] | None,
    track: tuple[int, int],
    sample: tuple[int, int],
    rotation: int,
) -> tuple[int, int]:
    for width, height in (aperture or (0, 0), track, sample):
        if has_valid_display_dimensions(width, height):
            return display_dimensions(width, height, rotation)
    return 0, 0


@dataclass(slots=True)
class ElstEntry:
    """Edit List entry within elst box."""

    segment_duration: int  # in movie timescale
    media_time: int  # in track timescale, signed (-1 indicates empty edit)
    media_rate_integer: int = 1
    media_rate_fraction: int = 0


@dataclass(slots=True)
class Edts:
    """Edit Box (edts) containing Edit List Box (elst)."""

    elst_version: int = 0
    elst_flags: int = 0
    entries: list[ElstEntry] = field(default_factory=list)


@dataclass(slots=True)
class Mdhd:
    """Media Header Box (mdhd)."""

    version: int
    flags: int
    creation_time: int
    modification_time: int
    timescale: int
    duration: int  # in track timescale
    language: int = 0x55C4


@dataclass(slots=True)
class Hdlr:
    """Handler Reference Box (hdlr)."""

    handler_type: bytes  # b"vide", b"soun", etc.
    name: str = ""


@dataclass(slots=True)
class SampleEntry:
    """Visual or Audio Sample Entry within stsd."""

    format: bytes
    data_ref_index: int
    codec: str
    width: int = 0
    height: int = 0
    channel_count: int = 0
    sample_rate: int = 0
    raw_bytes: bytes = b""
    esds_object_type: int | None = None


@dataclass(slots=True)
class Stsd:
    """Sample Description Box (stsd)."""

    version: int
    flags: int
    entry_count: int
    entries: list[SampleEntry]
    raw_box: bytes = b""
    dref_self_contained: tuple[bool, ...] = ()


@dataclass(slots=True)
class SttsEntry:
    """Time-to-Sample entry (stts)."""

    sample_count: int
    sample_delta: int


@dataclass(slots=True)
class CttsEntry:
    """Composition Time-to-Sample entry (ctts)."""

    sample_count: int
    sample_offset: int  # uint32 in v0, int32 in v1


class CompactSttsTable(Sequence):
    """
    Compact memory-efficient representation of stts box entries using contiguous arrays.
    """

    __slots__ = ("counts", "deltas")

    def __init__(self, counts: array.array, deltas: array.array) -> None:
        self.counts = counts
        self.deltas = deltas

    def __len__(self) -> int:
        return len(self.counts)

    def __getitem__(self, idx: Any) -> Any:
        if isinstance(idx, slice):
            return CompactSttsTable(self.counts[idx], self.deltas[idx])
        return SttsEntry(sample_count=self.counts[idx], sample_delta=self.deltas[idx])

    def __iter__(self):
        for i in range(len(self.counts)):
            yield SttsEntry(sample_count=self.counts[i], sample_delta=self.deltas[i])

    def to_list(self) -> list[SttsEntry]:
        return list(self)


class CompactCttsTable(Sequence):
    """
    Compact memory-efficient representation of ctts box entries using contiguous arrays.
    """

    __slots__ = ("counts", "offsets", "version")

    def __init__(self, counts: array.array, offsets: array.array, version: int = 0) -> None:
        self.counts = counts
        self.offsets = offsets
        self.version = version

    def __len__(self) -> int:
        return len(self.counts)

    def __getitem__(self, idx: Any) -> Any:
        if isinstance(idx, slice):
            return CompactCttsTable(self.counts[idx], self.offsets[idx], self.version)
        return CttsEntry(sample_count=self.counts[idx], sample_offset=self.offsets[idx])

    def __iter__(self):
        for i in range(len(self.counts)):
            yield CttsEntry(sample_count=self.counts[i], sample_offset=self.offsets[i])

    def to_list(self) -> list[CttsEntry]:
        return list(self)


class UniformStszTable(Sequence[int]):
    """
    Compact memory-efficient representation of stsz box when uniform_size > 0.

    Eliminates high RSS (OOM) and expensive loops for constant sample size tracks
    such as QuickTime lpcm or constant-bitrate audio/video.
    """

    __slots__ = ("sample_count", "uniform_size")

    def __init__(self, uniform_size: int, sample_count: int) -> None:
        if uniform_size < 0:
            raise ValueError(f"uniform_size must be non-negative, got {uniform_size}")
        if sample_count < 0:
            raise ValueError(f"sample_count must be non-negative, got {sample_count}")
        self.uniform_size = uniform_size
        self.sample_count = sample_count

    def __len__(self) -> int:
        return self.sample_count

    @overload
    def __getitem__(self, idx: int) -> int: ...

    @overload
    def __getitem__(self, idx: slice) -> UniformStszTable: ...

    def __getitem__(self, idx: int | slice) -> int | UniformStszTable:
        if isinstance(idx, slice):
            start, stop, step = idx.indices(self.sample_count)
            sliced_len = len(range(start, stop, step))
            return UniformStszTable(self.uniform_size, max(0, sliced_len))
        if isinstance(idx, int):
            if idx < 0:
                idx += self.sample_count
            if idx < 0 or idx >= self.sample_count:
                raise IndexError(f"UniformStszTable index out of range: {idx} (count={self.sample_count})")
            return self.uniform_size
        raise TypeError(f"UniformStszTable indices must be integers or slices, not {type(idx).__name__}")

    def __iter__(self) -> Iterator[int]:
        for _ in range(self.sample_count):
            yield self.uniform_size

    def __contains__(self, value: object) -> bool:
        return self.sample_count > 0 and value == self.uniform_size

    def count(self, value: Any) -> int:
        return self.sample_count if value == self.uniform_size else 0

    def index(self, value: Any, start: int = 0, stop: int | None = None) -> int:
        if stop is None:
            stop = self.sample_count
        start, stop, _ = slice(start, stop).indices(self.sample_count)
        if value == self.uniform_size and stop > start:
            return start
        raise ValueError(f"{value} is not in UniformStszTable")

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, (str, bytes, bytearray)):
            return False
        if isinstance(other, UniformStszTable):
            if self.sample_count == 0 and other.sample_count == 0:
                return True
            return self.uniform_size == other.uniform_size and self.sample_count == other.sample_count
        if isinstance(other, Sequence):
            if len(other) != self.sample_count:
                return False
            if self.sample_count == 0:
                return True
            return all(x == self.uniform_size for x in other)
        return False

    def to_list(self) -> list[int]:
        return list(self)

    def __repr__(self) -> str:
        return f"UniformStszTable(uniform_size={self.uniform_size}, sample_count={self.sample_count})"


@dataclass(slots=True)
class SbgpEntry:
    """Sample-to-Group entry (sbgp)."""

    sample_count: int
    group_description_index: int


@dataclass(slots=True)
class SbgpTable:
    """Sample-to-Group box (sbgp) containing RLE sample group mappings."""

    version: int
    flags: int
    grouping_type: bytes
    entries: list[SbgpEntry]
    grouping_type_parameter: int = 0


@dataclass(slots=True)
class StscEntry:
    """Sample-to-Chunk entry (stsc)."""

    first_chunk: int
    samples_per_chunk: int
    sample_description_index: int


@dataclass(slots=True)
class SampleTable:
    """Complete Sample Table (stbl) using compact native arrays."""

    stsd: Stsd
    stts: list[SttsEntry] | CompactSttsTable
    stsc: list[StscEntry]
    stsz: array.array | UniformStszTable  # array('I') or UniformStszTable
    chunk_offsets: array.array  # array('Q')
    stss: array.array | None = None  # array('I')
    ctts: list[CttsEntry] | CompactCttsTable | None = None
    default_sample_size: int = 0
    is_co64: bool = False
    sgpd_boxes: list[bytes] = field(default_factory=list)
    sbgp_tables: list[SbgpTable] = field(default_factory=list)
    sbgp_boxes: list[bytes] = field(default_factory=list)
    _timeline: Any = None


@dataclass(slots=True)
class Track:
    """Track (trak) box container representation."""

    track_id: int
    tkhd: Tkhd
    mdhd: Mdhd
    hdlr: Hdlr
    stbl: SampleTable
    edts: Edts | None = None
    aperture_dimensions: tuple[int, int] | None = None

    @property
    def is_video(self) -> bool:
        return self.hdlr.handler_type.strip().lower() == b"vide"

    @property
    def is_audio(self) -> bool:
        return self.hdlr.handler_type.strip().lower() == b"soun"

    @property
    def timescale(self) -> int:
        return self.mdhd.timescale

    @property
    def duration(self) -> int:
        return self.mdhd.duration

    @property
    def duration_s(self) -> float:
        if self.mdhd.timescale > 0:
            return self.mdhd.duration / self.mdhd.timescale
        return 0.0

    @property
    def sample_count(self) -> int:
        return len(self.stbl.stsz)

    @property
    def sample_entry(self) -> SampleEntry | None:
        if self.stbl.stsd.entries:
            return self.stbl.stsd.entries[0]
        return None

    @property
    def codec(self) -> str:
        entry = self.sample_entry
        return entry.codec if entry else ""


@dataclass(slots=True)
class Mp4Metadata:
    """Parsed moov box representation."""

    mvhd: Mvhd
    tracks: list[Track]
    udta_bytes: bytes | None = None

    @property
    def movie_timescale(self) -> int:
        return self.mvhd.timescale

    @property
    def duration_ticks(self) -> int:
        return self.mvhd.duration

    @property
    def duration_s(self) -> float:
        if self.mvhd.timescale > 0:
            return self.mvhd.duration / self.mvhd.timescale
        return 0.0

    @property
    def video_track(self) -> Track | None:
        for t in self.tracks:
            if t.is_video:
                return t
        return None

    @property
    def audio_track(self) -> Track | None:
        for t in self.tracks:
            if t.is_audio:
                return t
        return None

    @property
    def video_tracks(self) -> list[Track]:
        return [t for t in self.tracks if t.is_video]

    @property
    def audio_tracks(self) -> list[Track]:
        return [t for t in self.tracks if t.is_audio]


def _to_memoryview(data: BytesLike) -> memoryview:
    """Convert input to memoryview without copying underlying data."""
    if isinstance(data, memoryview):
        return data
    return memoryview(data)


def iter_boxes(
    buf: memoryview,
    start: int = 0,
    end: int | None = None,
) -> Iterator[tuple[bytes, int, int, int]]:
    """
    Yields (box_type, box_start, box_size, header_size) for boxes in buf[start:end].

    Raises:
        IsoBmffParseError: If box headers or sizes are malformed.
    """
    pos = start
    limit = len(buf) if end is None else end

    while pos + 8 <= limit:
        b_size, b_type = struct.unpack_from(">I4s", buf, pos)
        if b_size == 1:
            if pos + 16 > limit:
                raise IsoBmffParseError(f"Truncated 64-bit box header at offset {pos}")
            b_size = struct.unpack_from(">Q", buf, pos + 8)[0]
            hdr_size = 16
        elif b_size == 0:
            b_size = limit - pos
            hdr_size = 8
        else:
            hdr_size = 8

        if b_size < hdr_size:
            raise IsoBmffParseError(f"Invalid box size {b_size} (< header {hdr_size}) for {b_type!r} at offset {pos}")
        if pos + b_size > limit:
            raise IsoBmffParseError(f"Box {b_type!r} at offset {pos} with size {b_size} exceeds boundary {limit}")

        yield b_type, pos, b_size, hdr_size
        pos += b_size

    if pos < limit:
        raise IsoBmffParseError(
            f"Truncated box header: {limit - pos} trailing unparsed bytes at offset {pos} (boundary {limit})"
        )


def scan_top_level_boxes(
    header_bytes: BytesLike,
    file_size: int,
    probe_range_fn: Callable[[int, int], bytes] | None = None,
) -> list[BoxHeader]:
    """
    Scan sequential top-level boxes in an MP4 file.

    Features:
    - Verifies ftyp, moov, mdat, free, etc.
    - Box Span Jump Check: If mdat_offset + mdat_size < file_size, probes the boundary
      to detect second mdat boxes located gigabytes later.
    - Non-standard MP4 Circuit Breakers:
      - Raises MultipleMdatError if more than one mdat box exists.
      - Raises FragmentedMp4Error if moof or mvex boxes are encountered.
      - Raises CompactSampleSizeError if stz2 box is encountered.

    Args:
        header_bytes: Initial bytes of file (e.g. first 128KB or more).
        file_size: Total file size in bytes.
        probe_range_fn: Optional probe function (offset, length) -> bytes.

    Returns:
        List of scanned BoxHeader objects.
    """
    if file_size < 8:
        raise IsoBmffParseError(f"File size {file_size} is too small for a valid ISO-BMFF file (minimum 8 bytes)")

    mv = _to_memoryview(header_bytes)
    header_len = len(mv)
    pos = 0
    boxes: list[BoxHeader] = []
    mdat_count = 0
    moov_count = 0

    while pos < file_size:
        if file_size - pos < 8:
            raise IsoBmffParseError(
                f"Truncated box header at offset {pos}: only {file_size - pos} bytes remaining in file"
            )

        if pos + 8 <= header_len:
            raw = mv[pos : min(pos + 16, header_len)]
            b_size, b_type = struct.unpack_from(">I4s", raw, 0)
            if b_size == 1:
                if len(raw) >= 16:
                    b_size = struct.unpack_from(">Q", raw, 8)[0]
                    hdr_size = 16
                elif probe_range_fn is not None:
                    ext = probe_range_fn(pos, 16)
                    if len(ext) < 16:
                        raise IsoBmffParseError(f"Truncated 64-bit box header at offset {pos}")
                    b_size = struct.unpack_from(">Q", ext, 8)[0]
                    hdr_size = 16
                else:
                    raise IsoBmffParseError(f"Truncated 64-bit box header at offset {pos}")
            elif b_size == 0:
                b_size = file_size - pos
                hdr_size = 8
            else:
                hdr_size = 8
        elif pos < header_len:
            if probe_range_fn is not None:
                needed = min(16, file_size - pos)
                ext = probe_range_fn(pos, needed)
                if len(ext) < 8:
                    raise IsoBmffParseError(f"Truncated box header at offset {pos}: got {len(ext)} bytes")
                b_size, b_type = struct.unpack_from(">I4s", ext, 0)
                if b_size == 1:
                    if len(ext) < 16:
                        ext = probe_range_fn(pos, 16)
                        if len(ext) < 16:
                            raise IsoBmffParseError(f"Truncated 64-bit box header at offset {pos}")
                    b_size = struct.unpack_from(">Q", ext, 8)[0]
                    hdr_size = 16
                elif b_size == 0:
                    b_size = file_size - pos
                    hdr_size = 8
                else:
                    hdr_size = 8
            else:
                # Buffer ends mid-header; cannot read further without probe_range_fn
                break
        elif probe_range_fn is not None:
            needed = min(16, file_size - pos)
            ext = probe_range_fn(pos, needed)
            if len(ext) < 8:
                raise IsoBmffParseError(f"Truncated box header at offset {pos}: got {len(ext)} bytes")
            b_size, b_type = struct.unpack_from(">I4s", ext, 0)
            if b_size == 1:
                if len(ext) < 16:
                    ext = probe_range_fn(pos, 16)
                    if len(ext) < 16:
                        raise IsoBmffParseError(f"Truncated 64-bit box header at offset {pos}")
                b_size = struct.unpack_from(">Q", ext, 8)[0]
                hdr_size = 16
            elif b_size == 0:
                b_size = file_size - pos
                hdr_size = 8
            else:
                hdr_size = 8
        else:
            # Cannot read past header_bytes without probe_range_fn
            break

        if b_size < hdr_size:
            raise IsoBmffParseError(
                f"Invalid top-level box size {b_size} (< header {hdr_size}) for {b_type!r} at offset {pos}"
            )
        if pos + b_size > file_size:
            raise IsoBmffParseError(
                f"Top-level box {b_type!r} at offset {pos} with size {b_size} exceeds file size {file_size}"
            )

        # Circuit breaker checks
        if b_type == b"mdat":
            mdat_count += 1
            if mdat_count > 1:
                raise MultipleMdatError(f"Multiple mdat boxes detected in file (second mdat found at offset {pos})")

        if b_type == b"moov":
            moov_count += 1
            if moov_count > 1:
                raise IsoBmffParseError(f"Multiple moov boxes detected in file (second moov found at offset {pos})")

        if b_type in (b"moof", b"mvex"):
            type_str = b_type.decode("ascii", errors="replace")
            raise FragmentedMp4Error(f"Fragmented MP4 top-level box {type_str!r} detected at offset {pos}")

        if b_type == b"stz2":
            raise CompactSampleSizeError(f"Compact sample size box 'stz2' detected at offset {pos}")

        boxes.append(BoxHeader(box_type=b_type, offset=pos, size=b_size, header_size=hdr_size))
        pos += b_size

    return boxes


def _parse_mvhd(mv: memoryview, start: int, size: int, hdr_size: int) -> Mvhd:
    payload = mv[start + hdr_size : start + size]
    if len(payload) < 20:
        raise IsoBmffParseError("Truncated mvhd box")

    version = payload[0]
    flags = struct.unpack_from(">I", payload, 0)[0] & 0x00FFFFFF

    if version == 1:
        if len(payload) < 112:
            raise IsoBmffParseError(f"Truncated mvhd v1 box: expected at least 112 bytes, got {len(payload)}")
        cr_time, mod_time, timescale, duration = struct.unpack_from(">QQIQ", payload, 4)
        c_pos = 32
    elif version == 0:
        if len(payload) < 100:
            raise IsoBmffParseError(f"Truncated mvhd v0 box: expected at least 100 bytes, got {len(payload)}")
        cr_time, mod_time, timescale, duration = struct.unpack_from(">IIII", payload, 4)
        c_pos = 20
    else:
        raise IsoBmffParseError(f"Unsupported mvhd version {version}")

    rate, volume = struct.unpack_from(">IH", payload, c_pos)
    matrix = struct.unpack_from(">9i", payload, c_pos + 16)
    next_track_id = struct.unpack_from(">I", payload, c_pos + 76)[0]

    return Mvhd(
        version=version,
        flags=flags,
        creation_time=cr_time,
        modification_time=mod_time,
        timescale=timescale,
        duration=duration,
        rate=rate,
        volume=volume,
        matrix=matrix,
        next_track_id=next_track_id,
    )


def _parse_tkhd(mv: memoryview, start: int, size: int, hdr_size: int) -> Tkhd:
    payload = mv[start + hdr_size : start + size]
    if len(payload) < 24:
        raise IsoBmffParseError("Truncated tkhd box")

    version = payload[0]
    flags = struct.unpack_from(">I", payload, 0)[0] & 0x00FFFFFF

    if version == 1:
        if len(payload) < 96:
            raise IsoBmffParseError(f"Truncated tkhd v1 box: expected >= 96 bytes, got {len(payload)}")
        cr_time, mod_time, track_id, _, duration = struct.unpack_from(">QQIIQ", payload, 4)
        c_pos = 36
    elif version == 0:
        if len(payload) < 84:
            raise IsoBmffParseError(f"Truncated tkhd v0 box: expected >= 84 bytes, got {len(payload)}")
        cr_time, mod_time, track_id, _, duration = struct.unpack_from(">IIIII", payload, 4)
        c_pos = 24
    else:
        raise IsoBmffParseError(f"Unsupported tkhd version {version}")

    layer, alt_grp, volume = struct.unpack_from(">hhh", payload, c_pos + 8)
    matrix = struct.unpack_from(">9i", payload, c_pos + 16)
    w_raw, h_raw = struct.unpack_from(">II", payload, c_pos + 52)
    width = w_raw / 65536.0
    height = h_raw / 65536.0

    return Tkhd(
        version=version,
        flags=flags,
        creation_time=cr_time,
        modification_time=mod_time,
        track_id=track_id,
        duration=duration,
        layer=layer,
        alternate_group=alt_grp,
        volume=volume,
        matrix=matrix,
        width=width,
        height=height,
        width_raw=w_raw,
        height_raw=h_raw,
    )


def _parse_edts(mv: memoryview, start: int, size: int, hdr_size: int) -> Edts:
    edts_payload = mv[start + hdr_size : start + size]
    edts = Edts()

    for b_type, b_pos, b_size, b_hdr in iter_boxes(edts_payload):
        if b_type == b"elst":
            elst_payload = edts_payload[b_pos + b_hdr : b_pos + b_size]
            if len(elst_payload) < 8:
                raise IsoBmffParseError("Truncated elst box")

            version = elst_payload[0]
            flags = struct.unpack_from(">I", elst_payload, 0)[0] & 0x00FFFFFF
            entry_count = struct.unpack_from(">I", elst_payload, 4)[0]

            edts.elst_version = version
            edts.elst_flags = flags

            pos = 8
            for _ in range(entry_count):
                if version == 1:
                    if pos + 20 > len(elst_payload):
                        raise IsoBmffParseError("Truncated elst v1 entry")
                    seg_dur, m_time, r_int, r_frac = struct.unpack_from(">Qqhh", elst_payload, pos)
                    pos += 20
                else:
                    if pos + 12 > len(elst_payload):
                        raise IsoBmffParseError("Truncated elst v0 entry")
                    seg_dur, m_time, r_int, r_frac = struct.unpack_from(">Iihh", elst_payload, pos)
                    pos += 12
                edts.entries.append(
                    ElstEntry(
                        segment_duration=seg_dur,
                        media_time=m_time,
                        media_rate_integer=r_int,
                        media_rate_fraction=r_frac,
                    )
                )

    return edts


def _parse_mdhd(mv: memoryview, start: int, size: int, hdr_size: int) -> Mdhd:
    payload = mv[start + hdr_size : start + size]
    if len(payload) < 20:
        raise IsoBmffParseError("Truncated mdhd box")

    version = payload[0]
    flags = struct.unpack_from(">I", payload, 0)[0] & 0x00FFFFFF

    if version == 1:
        if len(payload) < 36:
            raise IsoBmffParseError("Truncated mdhd v1 box")
        cr_time, mod_time, timescale, duration = struct.unpack_from(">QQIQ", payload, 4)
        c_pos = 32
    elif version == 0:
        if len(payload) < 24:
            raise IsoBmffParseError("Truncated mdhd v0 box")
        cr_time, mod_time, timescale, duration = struct.unpack_from(">IIII", payload, 4)
        c_pos = 20
    else:
        raise IsoBmffParseError(f"Unsupported mdhd version {version}")

    language = struct.unpack_from(">H", payload, c_pos)[0]

    return Mdhd(
        version=version,
        flags=flags,
        creation_time=cr_time,
        modification_time=mod_time,
        timescale=timescale,
        duration=duration,
        language=language,
    )


def _parse_hdlr(mv: memoryview, start: int, size: int, hdr_size: int) -> Hdlr:
    payload = mv[start + hdr_size : start + size]
    if len(payload) < 24:
        raise IsoBmffParseError("Truncated hdlr box")

    handler_type = bytes(payload[8:12])
    raw_name = bytes(payload[24:])
    name = raw_name.rstrip(b"\x00").decode("utf-8", errors="replace")

    return Hdlr(handler_type=handler_type, name=name)


def _read_mp4_descr_size(buf: bytes | memoryview, pos: int) -> tuple[int, int] | None:
    size = 0
    for _ in range(4):
        if pos >= len(buf):
            return None
        byte = buf[pos]
        pos += 1
        size = (size << 7) | (byte & 0x7F)
        if not (byte & 0x80):
            return size, pos
    return None


def _skip_es_descriptor_header(esds_payload: bytes | memoryview, pos: int) -> int | None:
    parsed = _read_mp4_descr_size(esds_payload, pos)
    if parsed is None:
        return None
    _, pos = parsed
    if pos + 3 > len(esds_payload):
        return None
    flags = esds_payload[pos + 2]
    pos += 3
    if flags & 0x80:
        pos += 2
    if flags & 0x40:
        if pos >= len(esds_payload):
            return None
        pos += 1 + esds_payload[pos]
    if flags & 0x20:
        pos += 2
    return pos


def _parse_esds_object_type(esds_payload: bytes | memoryview) -> int | None:
    """Parse objectTypeIndication from an ISO-BMFF esds FullBox payload."""
    if len(esds_payload) < 6:
        return None
    pos = 4  # Skip FullBox version (1) + flags (3)
    tag = esds_payload[pos]
    pos += 1
    if tag == 0x03:
        next_pos = _skip_es_descriptor_header(esds_payload, pos)
        if next_pos is None or next_pos >= len(esds_payload):
            return None
        tag = esds_payload[next_pos]
        pos = next_pos + 1
    parsed = _read_mp4_descr_size(esds_payload, pos) if tag == 0x04 else None
    if parsed is None or parsed[1] >= len(esds_payload):
        return None
    return int(esds_payload[parsed[1]])


def _find_esds_object_type_in_boxes(container: memoryview, *, allow_wave: bool) -> int | None:
    try:
        for b_type, b_pos, b_size, b_hdr in iter_boxes(container):
            body = container[b_pos + b_hdr : b_pos + b_size]
            if b_type == b"esds":
                obj_type = _parse_esds_object_type(body)
                if obj_type is not None:
                    return obj_type
            elif allow_wave and b_type == b"wave":
                obj_type = _find_esds_object_type_in_boxes(body, allow_wave=False)
                if obj_type is not None:
                    return obj_type
    except IsoBmffParseError:
        return None
    return None


def extract_mp4a_esds_object_type(entry_data: BytesLike) -> int | None:
    """Extract esds objectTypeIndication from an mp4a AudioSampleEntry (v0/v1/v2 or QuickTime wave)."""
    mv = _to_memoryview(entry_data)
    if len(mv) < 36:
        return None
    audio_version = struct.unpack_from(">H", mv, 16)[0]
    child_off = 52 if audio_version == 1 else (72 if audio_version == 2 else 36)
    if child_off >= len(mv):
        return None
    return _find_esds_object_type_in_boxes(mv[child_off:], allow_wave=True)


def resolve_mp4a_codec_name(esds_object_type: int | None, default_name: str = "aac") -> str:
    """Map parsed mp4a esds objectTypeIndication to a concrete codec name."""
    if esds_object_type is None or esds_object_type in AAC_ESDS_OBJECT_TYPES:
        return default_name
    return _ESDS_NON_AAC_CODEC_NAMES.get(esds_object_type, f"mp4a_0x{esds_object_type:02x}")


def _parse_dref_self_contained(minf_payload: memoryview) -> tuple[bool, ...]:
    """Parse dinf/dref entries in minf and return self-contained flags (1-based indexed)."""
    for m_type, m_pos, m_size, m_hdr in iter_boxes(minf_payload):
        if m_type != b"dinf":
            continue
        dinf_payload = minf_payload[m_pos + m_hdr : m_pos + m_size]
        for d_type, d_pos, d_size, d_hdr in iter_boxes(dinf_payload):
            if d_type != b"dref":
                continue
            dref_payload = dinf_payload[d_pos + d_hdr : d_pos + d_size]
            if len(dref_payload) < 8:
                raise IsoBmffParseError("Truncated dref box")
            entry_count = struct.unpack_from(">I", dref_payload, 4)[0]
            if entry_count == 0:
                raise IsoBmffParseError("dref box has 0 data reference entries")
            flags_list: list[bool] = []
            pos = 8
            for _ in range(entry_count):
                if pos + 8 > len(dref_payload):
                    raise IsoBmffParseError("Truncated dref entry")
                e_size, _e_type = struct.unpack_from(">I4s", dref_payload, pos)
                if e_size < 12 or pos + e_size > len(dref_payload):
                    raise IsoBmffParseError(f"Invalid dref entry size {e_size}")
                e_flags = struct.unpack_from(">I", dref_payload, pos + 8)[0] & 0x00FFFFFF
                flags_list.append(bool(e_flags & 0x000001))
                pos += e_size
            return tuple(flags_list)
    return ()


def _parse_stsd(mv: memoryview, start: int, size: int, hdr_size: int, handler_type: bytes) -> Stsd:
    raw_stsd_box = bytes(mv[start : start + size])
    payload = mv[start + hdr_size : start + size]
    if len(payload) < 8:
        raise IsoBmffParseError("Truncated stsd box")

    version = payload[0]
    flags = struct.unpack_from(">I", payload, 0)[0] & 0x00FFFFFF
    entry_count = struct.unpack_from(">I", payload, 4)[0]

    if entry_count == 0:
        raise IsoBmffParseError("stsd box has 0 sample entries")

    entries: list[SampleEntry] = []
    pos = 8
    limit = len(payload)

    for _ in range(entry_count):
        if pos + 8 > limit:
            raise IsoBmffParseError("Truncated sample entry in stsd")

        entry_size, fmt = struct.unpack_from(">I4s", payload, pos)
        if entry_size < 8 or pos + entry_size > limit:
            raise IsoBmffParseError(f"Invalid sample entry size {entry_size} in stsd")

        entry_data = payload[pos : pos + entry_size]
        data_ref_index = struct.unpack_from(">H", entry_data, 14)[0] if entry_size >= 16 else 1
        codec_str = fmt.decode("ascii", errors="replace")

        w, h = 0, 0
        channels, rate = 0, 0

        # Video detection: handler_type or standard video formats
        is_video = handler_type == b"vide" or fmt in (
            b"avc1",
            b"avc3",
            b"hvc1",
            b"hev1",
            b"mp4v",
            b"dvh1",
            b"dvhe",
            b"vp08",
            b"vp09",
            b"av01",
        )
        is_audio = handler_type == b"soun" or fmt in (
            b"mp4a",
            b"ac-3",
            b"ec-3",
            b"opus",
            b"alac",
            b"flac",
            b"samr",
            b"sawb",
        )

        if is_video and entry_size >= 36:
            w, h = struct.unpack_from(">HH", entry_data, 32)
        elif is_audio and entry_size >= 36:
            audio_version = struct.unpack_from(">H", entry_data, 16)[0] if entry_size >= 18 else 0
            if audio_version == 2 and entry_size >= 52:
                rate_f64 = struct.unpack_from(">d", entry_data, 40)[0]
                rate = int(rate_f64)
                channels = struct.unpack_from(">I", entry_data, 48)[0]
            else:
                channels = struct.unpack_from(">H", entry_data, 24)[0]
                rate_raw = struct.unpack_from(">I", entry_data, 32)[0]
                rate = rate_raw >> 16

        esds_obj_type: int | None = None
        if fmt == b"mp4a":
            esds_obj_type = extract_mp4a_esds_object_type(entry_data)
            if esds_obj_type is not None and esds_obj_type not in AAC_ESDS_OBJECT_TYPES:
                codec_str = resolve_mp4a_codec_name(esds_obj_type)

        entries.append(
            SampleEntry(
                format=fmt,
                data_ref_index=data_ref_index,
                codec=codec_str,
                width=w,
                height=h,
                channel_count=channels,
                sample_rate=rate,
                raw_bytes=bytes(entry_data),
                esds_object_type=esds_obj_type,
            )
        )
        pos += entry_size

    return Stsd(
        version=version,
        flags=flags,
        entry_count=entry_count,
        entries=entries,
        raw_box=raw_stsd_box,
    )


def _parse_sbgp(box_content: memoryview) -> SbgpTable:
    """Parse sbgp box payload (FullBox content after 8-byte box header)."""
    if len(box_content) < 12:
        raise IsoBmffParseError(f"Truncated sbgp box: header too short ({len(box_content)} bytes)")
    version = box_content[0]
    if version not in (0, 1):
        raise IsoBmffParseError(f"Unsupported sbgp box version: {version} (must be 0 or 1)")
    flags = struct.unpack_from(">I", box_content, 0)[0] & 0x00FFFFFF
    grouping_type = bytes(box_content[4:8])
    pos = 8
    grouping_type_parameter = 0
    if version == 1:
        if len(box_content) < pos + 4:
            raise IsoBmffParseError("Truncated sbgp box: missing grouping_type_parameter")
        grouping_type_parameter = struct.unpack_from(">I", box_content, pos)[0]
        pos += 4
    if len(box_content) < pos + 4:
        raise IsoBmffParseError("Truncated sbgp box: missing entry_count")
    entry_count = struct.unpack_from(">I", box_content, pos)[0]
    pos += 4
    needed = pos + entry_count * 8
    if len(box_content) < needed:
        raise IsoBmffParseError(
            f"Truncated sbgp box: expected {needed} bytes for {entry_count} entries, got {len(box_content)}"
        )
    entries: list[SbgpEntry] = []
    for _ in range(entry_count):
        s_cnt, g_idx = struct.unpack_from(">II", box_content, pos)
        entries.append(SbgpEntry(sample_count=s_cnt, group_description_index=g_idx))
        pos += 8
    return SbgpTable(
        version=version,
        flags=flags,
        grouping_type=grouping_type,
        entries=entries,
        grouping_type_parameter=grouping_type_parameter,
    )


def _parse_stbl(mv: memoryview, start: int, size: int, hdr_size: int, handler_type: bytes) -> SampleTable:
    stbl_payload = mv[start + hdr_size : start + size]

    stsd: Stsd | None = None
    stts: list[SttsEntry] | CompactSttsTable | None = None
    ctts: list[CttsEntry] | CompactCttsTable | None = None
    stsc: list[StscEntry] | None = None
    stsz_table: array.array | UniformStszTable | None = None
    chunk_offsets: array.array | None = None
    stss_arr: array.array | None = None
    default_sample_size: int = 0
    is_co64: bool = False
    sgpd_boxes: list[bytes] = []
    sbgp_tables: list[SbgpTable] = []
    sbgp_boxes: list[bytes] = []

    for b_type, b_pos, b_size, b_hdr in iter_boxes(stbl_payload):
        # Circuit breaker: stz2 is unsupported
        if b_type == b"stz2":
            raise CompactSampleSizeError("Compact sample size box 'stz2' is not supported in VSSE")

        box_content = stbl_payload[b_pos + b_hdr : b_pos + b_size]

        if b_type == b"stsd":
            stsd = _parse_stsd(stbl_payload, b_pos, b_size, b_hdr, handler_type)

        elif b_type == b"sgpd":
            sgpd_boxes.append(bytes(stbl_payload[b_pos : b_pos + b_size]))

        elif b_type == b"sbgp":
            sbgp_boxes.append(bytes(stbl_payload[b_pos : b_pos + b_size]))
            try:
                sbgp_tables.append(_parse_sbgp(box_content))
            except IsoBmffParseError as sbgp_err:
                # 容错降级: 畸形/不支持版本的 sbgp 直接丢弃 (裸盒未经 RLE 切片不可透传进分卷,
                # 否则样本索引错位), 而非让整个 parse_moov 失败把文件挤到 fMP4 兜底通道。
                sbgp_boxes.pop()
                logger.warning("Dropped malformed sbgp box in stbl (non-fatal): %s", sbgp_err)

        elif b_type == b"stts":
            if len(box_content) < 8:
                raise IsoBmffParseError("Truncated stts box")
            entry_count = struct.unpack_from(">I", box_content, 4)[0]
            if len(box_content) < 8 + entry_count * 8:
                raise IsoBmffParseError(
                    f"Truncated stts box: expected {8 + entry_count * 8} bytes for {entry_count} entries, got {len(box_content)}"
                )
            if entry_count <= 8:
                entries: list[SttsEntry] = []
                e_pos = 8
                for _ in range(entry_count):
                    cnt, delta = struct.unpack_from(">II", box_content, e_pos)
                    entries.append(SttsEntry(sample_count=cnt, sample_delta=delta))
                    e_pos += 8
                stts = entries
            else:
                raw_arr = array.array("I")
                raw_arr.frombytes(box_content[8 : 8 + entry_count * 8])
                if sys.byteorder == "little":
                    raw_arr.byteswap()
                counts = raw_arr[0::2]
                deltas = raw_arr[1::2]
                del raw_arr
                stts = CompactSttsTable(counts, deltas)

        elif b_type == b"ctts":
            if len(box_content) < 8:
                raise IsoBmffParseError("Truncated ctts box")
            version = box_content[0]
            entry_count = struct.unpack_from(">I", box_content, 4)[0]
            if len(box_content) < 8 + entry_count * 8:
                raise IsoBmffParseError(
                    f"Truncated ctts box: expected {8 + entry_count * 8} bytes for {entry_count} entries, got {len(box_content)}"
                )
            if entry_count <= 8:
                c_entries: list[CttsEntry] = []
                e_pos = 8
                for _ in range(entry_count):
                    if version == 1:
                        cnt, off = struct.unpack_from(">Ii", box_content, e_pos)
                    else:
                        cnt, off = struct.unpack_from(">II", box_content, e_pos)
                    c_entries.append(CttsEntry(sample_count=cnt, sample_offset=off))
                    e_pos += 8
                ctts = c_entries
            else:
                raw_arr = array.array("I")
                raw_arr.frombytes(box_content[8 : 8 + entry_count * 8])
                if sys.byteorder == "little":
                    raw_arr.byteswap()
                counts = raw_arr[0::2]
                if version == 1:
                    offsets = array.array("i")
                    offsets.frombytes(memoryview(raw_arr[1::2]).cast("b"))
                else:
                    offsets = raw_arr[1::2]
                del raw_arr
                ctts = CompactCttsTable(counts, offsets, version=version)

        elif b_type == b"stsc":
            if len(box_content) < 8:
                raise IsoBmffParseError("Truncated stsc box")
            entry_count = struct.unpack_from(">I", box_content, 4)[0]
            if len(box_content) < 8 + entry_count * 12:
                raise IsoBmffParseError(
                    f"Truncated stsc box: expected {8 + entry_count * 12} bytes for {entry_count} entries, got {len(box_content)}"
                )
            sc_entries: list[StscEntry] = []
            e_pos = 8
            for _ in range(entry_count):
                f_chk, s_per_chk, s_idx = struct.unpack_from(">III", box_content, e_pos)
                sc_entries.append(
                    StscEntry(
                        first_chunk=f_chk,
                        samples_per_chunk=s_per_chk,
                        sample_description_index=s_idx,
                    )
                )
                e_pos += 12
            stsc = sc_entries

        elif b_type == b"stsz":
            if len(box_content) < 12:
                raise IsoBmffParseError("Truncated stsz box")
            uniform_size, sample_count = struct.unpack_from(">II", box_content, 4)
            default_sample_size = uniform_size

            if uniform_size > 0:
                stsz_table = UniformStszTable(uniform_size, sample_count)
            else:
                if sample_count > 100_000_000:
                    raise IsoBmffParseError(f"Excessive stsz sample_count {sample_count} exceeds safety threshold")
                stsz_arr = array.array("I")
                needed = sample_count * 4
                if len(box_content) < 12 + needed:
                    raise IsoBmffParseError(
                        f"Truncated stsz box: expected {needed} bytes for {sample_count} samples, got {len(box_content) - 12}"
                    )
                raw_stsz = box_content[12 : 12 + needed]
                stsz_arr.frombytes(raw_stsz)
                if sys.byteorder == "little":
                    stsz_arr.byteswap()
                stsz_table = stsz_arr

        elif b_type == b"stco":
            if len(box_content) < 8:
                raise IsoBmffParseError("Truncated stco box")
            entry_count = struct.unpack_from(">I", box_content, 4)[0]
            needed = entry_count * 4
            if len(box_content) < 8 + needed:
                raise IsoBmffParseError(
                    f"Truncated stco box: expected {needed} bytes for {entry_count} entries, got {len(box_content) - 8}"
                )
            raw_stco = box_content[8 : 8 + needed]
            arr_u32 = array.array("I")
            arr_u32.frombytes(raw_stco)
            if sys.byteorder == "little":
                arr_u32.byteswap()
            chunk_offsets = array.array("Q", arr_u32)
            is_co64 = False

        elif b_type == b"co64":
            if len(box_content) < 8:
                raise IsoBmffParseError("Truncated co64 box")
            entry_count = struct.unpack_from(">I", box_content, 4)[0]
            needed = entry_count * 8
            if len(box_content) < 8 + needed:
                raise IsoBmffParseError(
                    f"Truncated co64 box: expected {needed} bytes for {entry_count} entries, got {len(box_content) - 8}"
                )
            raw_co64 = box_content[8 : 8 + needed]
            chunk_offsets = array.array("Q")
            chunk_offsets.frombytes(raw_co64)
            if sys.byteorder == "little":
                chunk_offsets.byteswap()
            is_co64 = True

        elif b_type == b"stss":
            if len(box_content) < 8:
                raise IsoBmffParseError("Truncated stss box")
            entry_count = struct.unpack_from(">I", box_content, 4)[0]
            needed = entry_count * 4
            if len(box_content) < 8 + needed:
                raise IsoBmffParseError(
                    f"Truncated stss box: expected {needed} bytes for {entry_count} entries, got {len(box_content) - 8}"
                )
            raw_stss = box_content[8 : 8 + needed]
            stss_arr = array.array("I")
            stss_arr.frombytes(raw_stss)
            if sys.byteorder == "little":
                stss_arr.byteswap()

    if stsd is None:
        raise IsoBmffParseError("Missing 'stsd' box in 'stbl'")
    if not stsd.entries:
        raise IsoBmffParseError("Missing sample entries in 'stsd' box")
    if stts is None:
        raise IsoBmffParseError("Missing 'stts' box in 'stbl'")
    if stsc is None:
        raise IsoBmffParseError("Missing 'stsc' box in 'stbl'")
    if stsz_table is None:
        raise IsoBmffParseError("Missing 'stsz' box in 'stbl'")
    if chunk_offsets is None:
        raise IsoBmffParseError("Missing chunk offset table ('stco' or 'co64') in 'stbl'")

    # Validate stsc entry invariants
    for entry in stsc:
        if entry.first_chunk < 1:
            raise IsoBmffParseError(f"Invalid stsc first_chunk {entry.first_chunk} (must be >= 1)")
        if entry.sample_description_index < 1 or entry.sample_description_index > len(stsd.entries):
            raise IsoBmffParseError(
                f"stsc sample_description_index {entry.sample_description_index} out of range (stsd entries: {len(stsd.entries)})"
            )

    return SampleTable(
        stsd=stsd,
        stts=stts,
        stsc=stsc,
        stsz=stsz_table,
        chunk_offsets=chunk_offsets,
        stss=stss_arr,
        ctts=ctts,
        default_sample_size=default_sample_size,
        is_co64=is_co64,
        sgpd_boxes=sgpd_boxes,
        sbgp_tables=sbgp_tables,
        sbgp_boxes=sbgp_boxes,
    )


def _parse_trak(mv: memoryview, start: int, size: int, hdr_size: int) -> Track:
    trak_payload = mv[start + hdr_size : start + size]

    tkhd: Tkhd | None = None
    edts: Edts | None = None
    mdhd: Mdhd | None = None
    hdlr: Hdlr | None = None
    stbl: SampleTable | None = None
    aperture = None

    for b_type, b_pos, b_size, b_hdr in iter_boxes(trak_payload):
        if b_type == b"tkhd":
            tkhd = _parse_tkhd(trak_payload, b_pos, b_size, b_hdr)
        elif b_type == b"edts":
            edts = _parse_edts(trak_payload, b_pos, b_size, b_hdr)
        elif b_type == b"tapt":
            aperture = parse_track_aperture_dimensions(trak_payload[b_pos + b_hdr : b_pos + b_size])
        elif b_type == b"mdia":
            mdia_payload = trak_payload[b_pos + b_hdr : b_pos + b_size]
            minf_box = None
            for m_type, m_pos, m_size, m_hdr in iter_boxes(mdia_payload):
                if m_type == b"mdhd":
                    mdhd = _parse_mdhd(mdia_payload, m_pos, m_size, m_hdr)
                elif m_type == b"hdlr":
                    hdlr = _parse_hdlr(mdia_payload, m_pos, m_size, m_hdr)
                elif m_type == b"minf":
                    minf_box = (m_pos, m_size, m_hdr)

            if minf_box is not None:
                m_pos, m_size, m_hdr = minf_box
                minf_payload = mdia_payload[m_pos + m_hdr : m_pos + m_size]
                dref_flags = _parse_dref_self_contained(minf_payload)
                for s_type, s_pos, s_size, s_hdr in iter_boxes(minf_payload):
                    if s_type == b"stbl":
                        handler_type = hdlr.handler_type if hdlr else b""
                        stbl = _parse_stbl(minf_payload, s_pos, s_size, s_hdr, handler_type)
                        if dref_flags and not stbl.stsd.dref_self_contained:
                            stbl.stsd.dref_self_contained = dref_flags

    if tkhd is None:
        raise IsoBmffParseError("Missing 'tkhd' box in 'trak'")
    if mdhd is None:
        raise IsoBmffParseError("Missing 'mdhd' box in 'trak'")
    if hdlr is None:
        raise IsoBmffParseError("Missing 'hdlr' box in 'trak'")
    if stbl is None:
        raise IsoBmffParseError("Missing 'stbl' box in 'trak'")

    return Track(
        track_id=tkhd.track_id,
        tkhd=tkhd,
        mdhd=mdhd,
        hdlr=hdlr,
        stbl=stbl,
        edts=edts,
        aperture_dimensions=aperture,
    )


def parse_moov(moov_bytes: BytesLike) -> Mp4Metadata:
    """
    Parse ISO-BMFF moov box into typed Mp4Metadata.

    Enforces:
    - Pure function without side effects, accepting bytes or memoryview.
    - Zero-copy internal memoryview slicing.
    - Compact native array storage for sample sizes, keyframes, and offsets.
    - Non-standard MP4 Circuit Breakers:
      - Fragmented MP4 ('mvex' box present) -> raises FragmentedMp4Error.
      - Compact sample size ('stz2' box present) -> raises CompactSampleSizeError.

    Args:
        moov_bytes: Bytes or memoryview of moov box (either full box or box payload).

    Returns:
        Mp4Metadata with all parsed tracks and header values.
    """
    mv = _to_memoryview(moov_bytes)
    if len(mv) < 8:
        raise IsoBmffParseError("Buffer too small to contain a moov box")

    # Determine if passed buffer has 'moov' outer box header
    b_size, b_type = struct.unpack_from(">I4s", mv, 0)
    if b_type == b"moov":
        if b_size == 1:
            if len(mv) < 16:
                raise IsoBmffParseError("Truncated 64-bit moov header")
            b_size = struct.unpack_from(">Q", mv, 8)[0]
            hdr_len = 16
        else:
            hdr_len = 8
        if b_size < hdr_len and b_size != 0:
            raise IsoBmffParseError(f"Invalid moov box size {b_size} (< header {hdr_len})")
        if b_size > 0 and len(mv) < b_size:
            raise IsoBmffParseError(f"Truncated moov box: expected {b_size} bytes, got {len(mv)}")
        end_pos = b_size if b_size > 0 else len(mv)
        moov_content = mv[hdr_len:end_pos]
    else:
        moov_content = mv

    mvhd: Mvhd | None = None
    tracks: list[Track] = []
    udta_bytes: bytes | None = None

    for b_type, b_pos, b_size, b_hdr in iter_boxes(moov_content):
        # Circuit breaker: fragmented MP4 (mvex or moof) is not supported
        if b_type in (b"mvex", b"moof"):
            type_str = b_type.decode("ascii", errors="replace")
            raise FragmentedMp4Error(f"Fragmented MP4 ('{type_str}' box present in moov) is not supported in VSSE")

        if b_type == b"mvhd":
            mvhd = _parse_mvhd(moov_content, b_pos, b_size, b_hdr)
        elif b_type == b"trak":
            tracks.append(_parse_trak(moov_content, b_pos, b_size, b_hdr))
        elif b_type == b"udta":
            udta_bytes = bytes(moov_content[b_pos : b_pos + b_size])

    if mvhd is None:
        raise IsoBmffParseError("Missing 'mvhd' box in 'moov'")
    if not tracks:
        raise IsoBmffParseError("No 'trak' boxes found in 'moov'")

    return Mp4Metadata(mvhd=mvhd, tracks=tracks, udta_bytes=udta_bytes)


class VsseRoutingMode(enum.StrEnum):
    """VSSE routing modes for multi-tier publishing funnel."""

    PURE_ZERO_COPY = "pure_zero_copy"
    HYBRID_AUDIO_SIDECAR = "hybrid_audio_sidecar"


def classify_vsse_mode(metadata: Mp4Metadata) -> VsseRoutingMode:
    """
    Classify MP4 metadata into VSSE routing mode:
    - PURE_ZERO_COPY: Video codec is supported (AVC/HEVC/AV1) and audio is absent or standard AAC (mp4a).
    - HYBRID_AUDIO_SIDECAR: Video codec is supported (AVC/HEVC/AV1), but audio codec is non-AAC
      (lpcm/sowt/ac-3/ec-3/opus/flac/mp3/alac etc.) requiring sidecar transcoding.

    Raises:
        UnsupportedCodecError: If video track is missing or video codec is unsupported.
    """
    v_track = metadata.video_track
    if v_track is None:
        raise UnsupportedCodecError("No video track found in MP4 file")

    if not v_track.stbl.stsd.entries:
        raise UnsupportedCodecError("Video track has no sample description entries in stsd")

    for v_entry in v_track.stbl.stsd.entries:
        if v_entry.format not in SUPPORTED_VIDEO_CODECS:
            raise UnsupportedCodecError(
                f"Video codec {v_entry.codec!r} ({v_entry.format!r}) is not supported in VSSE (requires AVC/HEVC/AV1)"
            )

    a_track = metadata.audio_track
    if a_track is not None:
        if not a_track.stbl.stsd.entries:
            return VsseRoutingMode.HYBRID_AUDIO_SIDECAR
        for a_entry in a_track.stbl.stsd.entries:
            if a_entry.format not in SUPPORTED_AUDIO_CODECS:
                return VsseRoutingMode.HYBRID_AUDIO_SIDECAR
            if a_entry.esds_object_type is not None and a_entry.esds_object_type not in AAC_ESDS_OBJECT_TYPES:
                return VsseRoutingMode.HYBRID_AUDIO_SIDECAR

    return VsseRoutingMode.PURE_ZERO_COPY


def validate_vsse_eligibility(metadata: Mp4Metadata) -> None:
    """
    Validate that parsed MP4 metadata satisfies VSSE v2 fast-path requirements.

    Enforces:
    - At least one video track is present.
    - Video codec is standard AVC (H.264), HEVC (H.265), or AV1.
    - Audio codec (if present) is standard AAC (mp4a). Non-AAC audio triggers fallback.

    Raises:
        UnsupportedCodecError: If video or audio codec is not supported in VSSE.
    """
    mode = classify_vsse_mode(metadata)
    if mode != VsseRoutingMode.PURE_ZERO_COPY:
        a_track = metadata.audio_track
        if a_track is not None and a_track.stbl.stsd.entries:
            a_entry = a_track.stbl.stsd.entries[0]
            raise UnsupportedCodecError(
                f"Audio codec {a_entry.codec!r} ({a_entry.format!r}) is not supported in VSSE (requires AAC)"
            )
        raise UnsupportedCodecError("Audio track format is not supported in VSSE (requires AAC)")


__all__ = [
    "AAC_ESDS_OBJECT_TYPES",
    "SUPPORTED_AUDIO_CODECS",
    "SUPPORTED_VIDEO_CODECS",
    "BoxHeader",
    "CompactCttsTable",
    "CompactSttsTable",
    "CttsEntry",
    "Edts",
    "ElstEntry",
    "Hdlr",
    "Mdhd",
    "Mp4Metadata",
    "Mvhd",
    "SampleEntry",
    "SampleTable",
    "SbgpEntry",
    "SbgpTable",
    "StscEntry",
    "Stsd",
    "SttsEntry",
    "Tkhd",
    "Track",
    "UniformStszTable",
    "VsseRoutingMode",
    "classify_vsse_mode",
    "extract_mp4a_esds_object_type",
    "iter_boxes",
    "parse_moov",
    "resolve_mp4a_codec_name",
    "scan_top_level_boxes",
    "validate_vsse_eligibility",
]
