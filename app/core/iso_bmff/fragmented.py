"""
Fragmented MP4 / moov 轻量解析、Box 二进制修补与分片规划器 (fMP4 流式切片通道专用)。

从 fmp4_pipe_uploader 收拢:
- move 不 rewrite: fMP4 上传链路 (moov 属性探测、moof 分片时长 + SAP 判定)
  与 VSSE 通道的 box_parser 并列的独立解析面。这些函数面向 ffmpeg 管道实时输出流,
  刻意不做完整 Box 树校验 (与 box_parser 的强类型 parse_moov 不同), 调用方以容错降级方式消费其返回值。
- 容器 Box 二进制修补 (`patch_all_durations` / `ensure_mvex_mehd_box` /
  `build_sidx_and_free_box`) 与可播分卷规划 (`Fmp4PartPlan` / `plan_video_fmp4_parts`)，
  保持 `iso_bmff` 包纯内存计算、零网络/磁盘 I/O 的纯净边界。
"""

import itertools
import logging
import math
import os
import struct
from collections.abc import Iterator
from dataclasses import dataclass

__all__ = [
    "RESERVED_SIDX_SPACE",
    "Fmp4PartPlan",
    "build_sidx_and_free_box",
    "ensure_mvex_mehd_box",
    "parse_moof_video_fragment_info",
    "parse_moov_video_properties",
    "parse_moov_video_track",
    "patch_all_durations",
    "patch_mvhd_duration",
    "plan_video_fmp4_parts",
]

logger = logging.getLogger(__name__)

RESERVED_SIDX_SPACE = 64 * 1024  # 65,536 bytes (64KB reserved in part 0 for sidx + free)


@dataclass(frozen=True)
class Fmp4PartPlan:
    """可播放分卷规格契约"""

    part_index: int  # 1-based 分卷序号 (用于 caption [i/n])
    total_parts: int  # 总分卷数
    start_sec: float  # 视频剪切起始时间 (对齐关键帧)
    end_sec: float  # 视频剪切结束时间 (对齐关键帧)
    duration_sec: float  # 本分卷确定性时长 (end_sec - start_sec)
    part_name: str  # 目标文件名 (如: movie.part001.mp4)
    is_blind: bool = False  # 是否为时间等分盲切 (未命中关键帧探测)


def _iter_iso_boxes(buf: bytes | bytearray, start: int, end: int) -> Iterator[tuple[bytes, int, int, int]]:
    """Iterate valid ISO-BMFF boxes within [start, end), yielding (type, pos, header_len, size)."""
    pos = start
    while pos + 8 <= end:
        b_size, b_type = struct.unpack_from(">I4s", buf, pos)
        if b_size == 1 and pos + 16 <= end:
            b_size = struct.unpack_from(">Q", buf, pos + 8)[0]
            header_len = 16
        else:
            header_len = 8
        if b_size < header_len or pos + b_size > end:
            break
        yield b_type, pos, header_len, b_size
        pos += b_size


def _parse_tkhd_track_and_dims(box_content: bytes) -> tuple[int, int, int]:
    track_id = 0
    width = 0
    height = 0
    if len(box_content) >= 20:
        version = box_content[0]
        if version == 1 and len(box_content) >= 28:
            track_id = struct.unpack_from(">I", box_content, 20)[0]
        else:
            track_id = struct.unpack_from(">I", box_content, 12)[0]
        if len(box_content) >= 84:
            w_fixed, h_fixed = struct.unpack_from(">II", box_content, len(box_content) - 8)
            width = w_fixed >> 16
            height = h_fixed >> 16
    return track_id, width, height


def _parse_mdia_video_and_timescale(mdia_content: bytes) -> tuple[bool, int]:
    is_video = False
    timescale = 0
    for mb_type, m_pos, mb_head, mb_size in _iter_iso_boxes(mdia_content, 0, len(mdia_content)):
        m_data = mdia_content[m_pos + mb_head : m_pos + mb_size]
        if mb_type == b"hdlr" and len(m_data) >= 12:
            if m_data[8:12] == b"vide":
                is_video = True
        elif mb_type == b"mdhd" and len(m_data) >= 16:
            version = m_data[0]
            if version == 1 and len(m_data) >= 24:
                timescale = struct.unpack_from(">I", m_data, 20)[0]
            else:
                timescale = struct.unpack_from(">I", m_data, 12)[0]
    return is_video, timescale


def _parse_trak_video_properties(trak_body: bytes) -> tuple[bool, int, int, int, int]:
    curr_track_id = 0
    curr_timescale = 0
    curr_width = 0
    curr_height = 0
    is_video = False
    for sub_type, t_pos, sub_head, sub_size in _iter_iso_boxes(trak_body, 0, len(trak_body)):
        box_content = trak_body[t_pos + sub_head : t_pos + sub_size]
        if sub_type == b"tkhd":
            curr_track_id, curr_width, curr_height = _parse_tkhd_track_and_dims(box_content)
        elif sub_type == b"mdia":
            is_video, curr_timescale = _parse_mdia_video_and_timescale(box_content)
    return is_video, curr_track_id, curr_timescale, curr_width, curr_height


def parse_moov_video_properties(moov_bytes: bytes) -> tuple[int, int, int, int]:
    """
    Scans moov box for the video track to extract:
    (video_track_id, video_timescale, video_width, video_height)
    """
    pos = 0
    end = len(moov_bytes)
    if end >= 8:
        b_size, b_type = struct.unpack_from(">I4s", moov_bytes, 0)
        if b_type == b"moov":
            pos = 16 if (b_size == 1 and end >= 16) else 8

    video_track_id = 1
    video_timescale = 90000
    video_width = 0
    video_height = 0

    for b_type, b_pos, header_len, b_size in _iter_iso_boxes(moov_bytes, pos, end):
        if b_type != b"trak":
            continue
        is_video, curr_track_id, curr_timescale, curr_width, curr_height = _parse_trak_video_properties(
            moov_bytes[b_pos + header_len : b_pos + b_size]
        )
        if is_video and curr_timescale > 0:
            return (curr_track_id or 1, curr_timescale, curr_width, curr_height)
        if is_video and curr_track_id > 0:
            video_track_id = curr_track_id
            video_width = curr_width
            video_height = curr_height

    return (video_track_id, video_timescale, video_width, video_height)


def parse_moov_video_track(moov_bytes: bytes) -> tuple[int, int]:
    """
    Scans moov box for the video track to extract:
    (video_track_id, video_timescale)
    Backward compatible wrapper around parse_moov_video_properties.
    """
    props = parse_moov_video_properties(moov_bytes)
    return (props[0], props[1])


def _parse_tfhd_info(moof_bytes: bytes, t_pos: int, target_track_id: int) -> tuple[bool, int]:
    flags = struct.unpack_from(">I", moof_bytes, t_pos + 8)[0] & 0xFFFFFF
    track_id = struct.unpack_from(">I", moof_bytes, t_pos + 12)[0]
    offset = t_pos + 16
    if flags & 0x000001:
        offset += 8
    if flags & 0x000002:
        offset += 4
    default_sample_duration = struct.unpack_from(">I", moof_bytes, offset)[0] if (flags & 0x000008) else 0
    return track_id == target_track_id, default_sample_duration


def _extract_trun_first_sample_flags(
    moof_bytes: bytes, offset: int, box_end: int, flags: int, sample_count: int
) -> tuple[int | None, int]:
    first_sample_flags: int | None = None
    if flags & 0x000004:
        if offset + 4 <= box_end:
            first_sample_flags = struct.unpack_from(">I", moof_bytes, offset)[0]
        offset += 4
    elif (flags & 0x000400) and sample_count > 0:
        # 仅存在逐样本 flags 时，首视频样本标志位位于第一条样本记录内（位于可选 duration/size 之后）
        record_flags_pos = offset + (4 if (flags & 0x000100) else 0) + (4 if (flags & 0x000200) else 0)
        if record_flags_pos + 4 <= box_end:
            first_sample_flags = struct.unpack_from(">I", moof_bytes, record_flags_pos)[0]
    return first_sample_flags, offset


def _parse_trun_fragment(
    moof_bytes: bytes,
    t_pos: int,
    sub_size: int,
    t_end: int,
    default_sample_duration: int,
    current_sap: bool | None,
) -> tuple[int, bool | None]:
    flags = struct.unpack_from(">I", moof_bytes, t_pos + 8)[0] & 0xFFFFFF
    sample_count = struct.unpack_from(">I", moof_bytes, t_pos + 12)[0]
    offset = t_pos + 16 + (4 if (flags & 0x000001) else 0)
    first_sample_flags, offset = _extract_trun_first_sample_flags(
        moof_bytes, offset, t_pos + sub_size, flags, sample_count
    )
    sap = current_sap
    if sap is None:
        # SAP 判定只看首个视频 trun (分片首视频样本), 后续 trun 不改写
        if first_sample_flags is None:
            sap = False
        else:
            sap = (first_sample_flags & 0x03000000) == 0x02000000 and (first_sample_flags & 0x00010000) == 0

    stride = (
        (4 if (flags & 0x100) else 0)
        + (4 if (flags & 0x200) else 0)
        + (4 if (flags & 0x400) else 0)
        + (4 if (flags & 0x800) else 0)
    )
    if flags & 0x000100:
        dur = sum(
            struct.unpack_from(">I", moof_bytes, offset + i * stride)[0]
            for i in range(sample_count)
            if offset + i * stride + 4 <= t_end
        )
        return dur, sap
    return sample_count * default_sample_duration, sap


def _parse_traf_video_fragment(
    moof_bytes: bytes, t_pos: int, t_end: int, target_track_id: int
) -> tuple[bool, int, bool | None]:
    is_target_track = False
    default_sample_duration = 0
    frag_duration = 0
    sap: bool | None = None

    while t_pos + 8 <= t_end:
        sub_size, sub_type = struct.unpack_from(">I4s", moof_bytes, t_pos)
        if sub_size < 8 or t_pos + sub_size > t_end:
            break
        if sub_type == b"tfhd":
            is_target_track, default_sample_duration = _parse_tfhd_info(moof_bytes, t_pos, target_track_id)
        elif sub_type == b"trun" and is_target_track:
            trun_dur, sap = _parse_trun_fragment(moof_bytes, t_pos, sub_size, t_end, default_sample_duration, sap)
            frag_duration += trun_dur
        t_pos += sub_size

    return is_target_track, frag_duration, sap


def parse_moof_video_fragment_info(moof_bytes: bytes, target_track_id: int = 1) -> tuple[int, bool | None]:
    """
    Scans moof box to extract video fragment duration ticks and the first-sample SAP flag
    for target_track_id.

    Returns (duration_ticks, sap):
    - duration_ticks: accumulated sample duration of the target track's traf (0 when the
      moof carries no target-track traf, e.g. audio-only fragments);
    - sap: True when the FIRST target-track trun carries first_sample_flags with
      sample_depends_on=2 (mask 0x03000000) and sample_is_non_sync_sample=0 (mask
      0x00010000) — the exact pattern ffmpeg movenc writes when the fragment's first
      video sample is a keyframe; False when the field is absent or holds any other
      value; None when the moof has no target-track trun at all.
    """
    pos = 8
    end = len(moof_bytes)
    if end >= 8:
        b_size, _b_type = struct.unpack_from(">I4s", moof_bytes, 0)
        if b_size == 1 and end >= 16:
            pos = 16
    while pos + 8 <= end:
        b_size, b_type = struct.unpack_from(">I4s", moof_bytes, pos)
        if b_size < 8 or pos + b_size > end:
            break
        if b_type == b"traf":
            is_target_track, frag_duration, sap = _parse_traf_video_fragment(
                moof_bytes, pos + 8, pos + b_size, target_track_id
            )
            if is_target_track:
                return frag_duration, sap
        pos += b_size
    return 0, None


def _locate_moov_bounds(buf: bytearray) -> tuple[int, int] | None:
    for b_type, pos, header_len, b_size in _iter_iso_boxes(buf, 0, len(buf)):
        if b_type == b"moov":
            return pos + header_len, pos + b_size
    return None


def _read_and_patch_mvhd(buf: bytearray, moov_start: int, moov_end: int, duration_sec: float) -> tuple[int, int, int]:
    movie_timescale = 1000
    mvhd_info: tuple[int, bool] | None = None
    for mb_type, m_pos, mb_head, mb_size in _iter_iso_boxes(buf, moov_start, moov_end):
        child_end = m_pos + mb_size
        if mb_type != b"mvhd":
            continue
        content_pos = m_pos + mb_head
        if content_pos + 4 <= child_end:
            version = buf[content_pos]
            if version == 1 and content_pos + 32 <= child_end:
                movie_timescale = struct.unpack_from(">I", buf, content_pos + 20)[0] or 1000
                mvhd_info = (content_pos + 24, True)
            elif version == 0 and content_pos + 20 <= child_end:
                movie_timescale = struct.unpack_from(">I", buf, content_pos + 12)[0] or 1000
                mvhd_info = (content_pos + 16, False)
        break

    movie_ticks = int(duration_sec * movie_timescale)
    movie_ticks_32 = min(0xFFFFFFFF, max(0, movie_ticks))
    movie_ticks_64 = max(0, movie_ticks)
    if mvhd_info:
        offset, is_v1 = mvhd_info
        struct.pack_into(">Q" if is_v1 else ">I", buf, offset, movie_ticks_64 if is_v1 else movie_ticks_32)
    return movie_timescale, movie_ticks_32, movie_ticks_64


def _patch_mdia_mdhd_duration(
    buf: bytearray, mdia_start: int, mdia_end: int, duration_sec: float, movie_timescale: int
) -> None:
    for mda_type, mda_pos, mda_head, mda_size in _iter_iso_boxes(buf, mdia_start, mdia_end):
        child_end = mda_pos + mda_size
        mda_content = mda_pos + mda_head
        if mda_type != b"mdhd" or mda_content + 4 > child_end:
            continue
        m_ver = buf[mda_content]
        if m_ver == 1 and mda_content + 32 <= child_end:
            track_ts = struct.unpack_from(">I", buf, mda_content + 20)[0] or movie_timescale
            track_ticks = max(0, int(duration_sec * track_ts))
            struct.pack_into(">Q", buf, mda_content + 24, track_ticks)
        elif m_ver == 0 and mda_content + 20 <= child_end:
            track_ts = struct.unpack_from(">I", buf, mda_content + 12)[0] or movie_timescale
            track_ticks_32 = min(0xFFFFFFFF, max(0, int(duration_sec * track_ts)))
            struct.pack_into(">I", buf, mda_content + 16, track_ticks_32)


def _patch_trak_durations(
    buf: bytearray,
    trak_start: int,
    trak_end: int,
    duration_sec: float,
    movie_timescale: int,
    movie_ticks_32: int,
    movie_ticks_64: int,
) -> None:
    for tb_type, t_pos, tb_head, tb_size in _iter_iso_boxes(buf, trak_start, trak_end):
        child_end = t_pos + tb_size
        t_content = t_pos + tb_head
        if tb_type == b"tkhd" and t_content + 4 <= child_end:
            t_ver = buf[t_content]
            if t_ver == 1 and t_content + 36 <= child_end:
                struct.pack_into(">Q", buf, t_content + 28, movie_ticks_64)
            elif t_ver == 0 and t_content + 24 <= child_end:
                struct.pack_into(">I", buf, t_content + 20, movie_ticks_32)
        elif tb_type == b"mdia":
            _patch_mdia_mdhd_duration(buf, t_content, t_pos + tb_size, duration_sec, movie_timescale)


def _patch_mvex_durations(
    buf: bytearray, mvex_start: int, mvex_end: int, movie_ticks_32: int, movie_ticks_64: int
) -> None:
    for mv_type, mv_pos, mv_head, mv_size in _iter_iso_boxes(buf, mvex_start, mvex_end):
        child_end = mv_pos + mv_size
        mv_content = mv_pos + mv_head
        if mv_type == b"mehd" and mv_content + 4 <= child_end:
            me_ver = buf[mv_content]
            if me_ver == 1 and mv_content + 12 <= child_end:
                struct.pack_into(">Q", buf, mv_content + 4, movie_ticks_64)
            elif me_ver == 0 and mv_content + 8 <= child_end:
                struct.pack_into(">I", buf, mv_content + 4, movie_ticks_32)


def patch_all_durations(part_0_buffer: bytearray, duration_sec: float) -> None:
    """
    基于 ISO/IEC 14496-12 Box 树安全遍历，同步刷新 moov 内四处 Duration：
    1. moov -> mvhd (movie_timescale)
    2. moov -> trak -> tkhd (movie_timescale)
    3. moov -> trak -> mdia -> mdhd (各自 track_timescale)
    4. moov -> mvex -> mehd (若存在, movie_timescale)
    彻底杜绝裸字符串 find(b"mvhd") 误匹配及音画/多端播放器时长显示异常。
    """
    if len(part_0_buffer) < 16:
        return
    bounds = _locate_moov_bounds(part_0_buffer)
    if bounds is None or bounds[1] <= bounds[0]:
        return
    moov_start, moov_end = bounds

    movie_timescale, movie_ticks_32, movie_ticks_64 = _read_and_patch_mvhd(
        part_0_buffer, moov_start, moov_end, duration_sec
    )
    for mb_type, m_pos, mb_head, mb_size in _iter_iso_boxes(part_0_buffer, moov_start, moov_end):
        box_start = m_pos + mb_head
        box_end = m_pos + mb_size
        if mb_type == b"trak":
            _patch_trak_durations(
                part_0_buffer, box_start, box_end, duration_sec, movie_timescale, movie_ticks_32, movie_ticks_64
            )
        elif mb_type == b"mvex":
            _patch_mvex_durations(part_0_buffer, box_start, box_end, movie_ticks_32, movie_ticks_64)


def patch_mvhd_duration(part_0_buffer: bytearray, duration_sec: float) -> None:
    """向后兼容的 Duration 刷新入口 (委托给基于 Box 树安全遍历的 patch_all_durations)"""
    patch_all_durations(part_0_buffer, duration_sec)


def _parse_moov_header(moov_bytes: bytes) -> tuple[int, int] | None:
    if len(moov_bytes) < 8:
        return None
    moov_size, moov_type = struct.unpack_from(">I4s", moov_bytes, 0)
    if moov_type != b"moov":
        return None
    if moov_size == 1:
        if len(moov_bytes) < 16:
            return None
        moov_head = 16
        moov_size = struct.unpack_from(">Q", moov_bytes, 8)[0]
    elif moov_size == 0:
        moov_head = 8
        moov_size = len(moov_bytes)
    else:
        moov_head = 8

    if moov_size < moov_head or moov_size > len(moov_bytes):
        return None
    if moov_head == 8 and moov_size + 16 > 0xFFFFFFFF:
        return None
    return moov_head, moov_size


def _can_inject_mehd_into_mvex(moov_bytes: bytes, mvex_pos: int, mvex_head: int, mvex_size: int) -> bool:
    if mvex_head == 8 and mvex_size + 16 > 0xFFFFFFFF:
        return False
    mvex_end = mvex_pos + mvex_size
    sub_pos = mvex_pos + mvex_head
    for sub_type, cur_pos, _sub_head, sub_size in _iter_iso_boxes(moov_bytes, sub_pos, mvex_end):
        if sub_type == b"mehd":
            return False
        sub_pos = cur_pos + sub_size
    return sub_pos == mvex_end


def ensure_mvex_mehd_box(moov_bytes: bytes) -> bytes:
    """
    检查并在 moov -> mvex 内注入 16 字节的 mehd (Movie Extends Header Box)。
    FFmpeg 的 empty_moov 流式输出模式默认省略 mehd Box，导致播放器无法在流开头
    获知切片总时长。本函数检查 mvex：
    - 若已存在 mehd，幂等原样返回；
    - 若不存在 mehd，在 mvex 的首个子 Box 前插入 16 字节 mehd (version=0, flags=0, duration=0)，
      并同步递增 mvex 与 moov 的 box_size；
    - 流结束时，patch_all_durations() 会自动定位该 mehd 并填入精准 movie_ticks。
    """
    parsed = _parse_moov_header(moov_bytes)
    if parsed is None:
        return moov_bytes
    moov_head, moov_size = parsed

    mvex_info: tuple[int, int, int] | None = None
    for b_type, pos, b_head, b_size in _iter_iso_boxes(moov_bytes, moov_head, moov_size):
        if b_type == b"mvex":
            mvex_info = (pos, b_head, b_size)
            break
    if mvex_info is None:
        return moov_bytes

    mvex_pos, mvex_head, mvex_size = mvex_info
    if not _can_inject_mehd_into_mvex(moov_bytes, mvex_pos, mvex_head, mvex_size):
        return moov_bytes

    # 构造 16 字节 mehd Box: size=16, type=b"mehd", version=0, flags=0, duration=0
    mehd_box = struct.pack(">I4sBBHI", 16, b"mehd", 0, 0, 0, 0)
    moov_ba = bytearray(moov_bytes)
    struct.pack_into(">I" if moov_head == 8 else ">Q", moov_ba, 0 if moov_head == 8 else 8, moov_size + 16)
    struct.pack_into(
        ">I" if mvex_head == 8 else ">Q", moov_ba, mvex_pos if mvex_head == 8 else mvex_pos + 8, mvex_size + 16
    )

    insert_pos = mvex_pos + mvex_head
    moov_ba[insert_pos:insert_pos] = mehd_box
    return bytes(moov_ba)


def build_sidx_and_free_box(
    fragments_meta: list[tuple[int, int, bool | None]],
    timescale: int,
    reserved_size: int = RESERVED_SIDX_SPACE,
    track_id: int = 1,
) -> bytes:
    """
    Synthesize top-level sidx box containing index for all fragments, plus free box padding.
    first_offset is set to free_size = reserved_size - sidx_size.
    Supports index condensation when fragment count > 5,458; a merged entry inherits the
    FIRST constituent's sap flag (starts_with_SAP describes the referenced range start).
    Reference word 3rd field: 0x90000000 (starts_with_SAP=1, SAP type 1) only when the
    fragment's first video sample is truly SAP (sap=True); 0x00000000 otherwise.
    """
    frags = list(fragments_meta)
    while len(frags) * 12 + 32 > reserved_size - 8 and len(frags) > 1:
        merged = []
        for i in range(0, len(frags), 2):
            if i + 1 < len(frags):
                merged.append((frags[i][0] + frags[i + 1][0], frags[i][1] + frags[i + 1][1], frags[i][2]))
            else:
                merged.append(frags[i])
        frags = merged

    sidx_size = 32 + len(frags) * 12
    free_size = reserved_size - sidx_size
    if free_size < 8:
        raise ValueError(f"sidx box size ({sidx_size} bytes) exceeds reserved space ({reserved_size} bytes)")

    sidx_header = bytearray(sidx_size)
    struct.pack_into(">I4s", sidx_header, 0, sidx_size, b"sidx")
    sidx_header[8] = 0  # version 0
    struct.pack_into(">I", sidx_header, 12, track_id)
    struct.pack_into(">I", sidx_header, 16, timescale)
    struct.pack_into(">I", sidx_header, 20, 0)  # earliest_presentation_time
    struct.pack_into(">I", sidx_header, 24, free_size)  # first_offset points to first moof!
    struct.pack_into(">H", sidx_header, 28, 0)
    struct.pack_into(">H", sidx_header, 30, len(frags))  # reference_count

    pos = 32
    for frag_size, frag_dur, frag_sap in frags:
        if not 0 <= frag_size <= 0x7FFFFFFF:
            raise ValueError(f"SIDX reference size out of range: {frag_size}")
        ref_field = frag_size
        sap_field = 0x90000000 if frag_sap else 0x00000000
        struct.pack_into(">III", sidx_header, pos, ref_field, frag_dur, sap_field)
        pos += 12

    free_header = bytearray(free_size)
    struct.pack_into(">I4s", free_header, 0, free_size, b"free")
    return bytes(sidx_header + free_header)


def _compute_split_points(
    *,
    num_parts: int,
    target_dur: float,
    max_part_dur: float,
    total_duration_s: float,
    keyframe_pts_s: list[float] | None,
    file_name: str,
) -> list[float]:
    duration_tolerance = 1e-9
    split_points = [0.0]
    for k in range(1, num_parts):
        ideal_time = k * target_dur
        min_allowed_time = max(split_points[-1] + 2.0, total_duration_s - (num_parts - k) * max_part_dur)
        max_allowed_time = min(total_duration_s - 2.0, split_points[-1] + max_part_dur)
        if min_allowed_time > max_allowed_time + duration_tolerance:
            # Cannot safely insert more cut points
            break

        if abs(min_allowed_time - max_allowed_time) <= duration_tolerance:
            min_allowed_time = max_allowed_time

        target_candidate = min(max(ideal_time, min_allowed_time), max_allowed_time)
        if keyframe_pts_s:
            valid_cues = [pts for pts in keyframe_pts_s if min_allowed_time <= pts <= max_allowed_time]
            if valid_cues:
                split_points.append(min(valid_cues, key=lambda pts: abs(pts - target_candidate)))
                continue
            logger.warning(
                "plan_fmp4_split: valid_cues is empty in window [%.2f, %.2f] for %s (part %d), falling back to unaligned target_candidate %.2f",
                min_allowed_time,
                max_allowed_time,
                file_name,
                k,
                target_candidate,
            )
        split_points.append(target_candidate)

    if split_points[-1] < total_duration_s:
        split_points.append(total_duration_s)
    else:
        split_points[-1] = total_duration_s

    deduped_points = [split_points[0]]
    for pt in split_points[1:]:
        if pt > deduped_points[-1] + 0.1:
            deduped_points.append(pt)
    if deduped_points[-1] < total_duration_s:
        deduped_points.append(total_duration_s)
    max_segment_dur = max((b - a for a, b in itertools.pairwise(deduped_points)), default=0.0)
    if max_segment_dur > max_part_dur + duration_tolerance:
        logger.warning(
            "plan_fmp4_split: max segment duration %.2fs exceeds soft bound %.2fs for %s; returning best-effort plan",
            max_segment_dur,
            max_part_dur,
            file_name,
        )
    return deduped_points


def plan_video_fmp4_parts(
    total_bytes: int,
    threshold_bytes: int,
    total_duration_s: float,
    keyframe_pts_s: list[float] | None,
    file_name: str,
    safety_margin: float = 0.90,
    force_num_parts: int | None = None,
) -> list[Fmp4PartPlan]:
    """
    Computes split plans with a 10% VBR margin and aligns cutpoints to keyframes
    while strictly enforcing safe byte threshold constraints per part.
    Optionally accepts force_num_parts for dynamic re-planning on oversize.
    """
    is_blind = not bool(keyframe_pts_s)
    if is_blind and total_bytes > threshold_bytes:
        logger.warning(
            "Fmp4Planner keyframe probe missing or format unsupported for %s, "
            "falling back to blind time split with -noaccurate_seek guardrail",
            file_name,
        )

    if total_bytes <= 0 or threshold_bytes <= 0 or total_duration_s <= 0:
        return [
            Fmp4PartPlan(
                part_index=1,
                total_parts=1,
                start_sec=0.0,
                end_sec=max(0.0, total_duration_s),
                duration_sec=max(0.0, total_duration_s),
                part_name=file_name,
                is_blind=is_blind,
            )
        ]

    if safety_margin <= 0:
        raise ValueError("safety_margin must be > 0")

    safe_threshold = max(1, int(threshold_bytes * safety_margin))
    avg_bytes_per_sec = total_bytes / total_duration_s
    if force_num_parts is not None and force_num_parts >= 2:
        num_parts = force_num_parts
        target_dur = total_duration_s / num_parts
        max_part_dur = (
            min(target_dur * 1.25, safe_threshold / avg_bytes_per_sec) if avg_bytes_per_sec > 0 else target_dur * 1.25
        )
    else:
        num_parts = max(2, math.ceil(total_bytes / safe_threshold))
        target_dur = total_duration_s / num_parts
        max_part_dur = safe_threshold / avg_bytes_per_sec if avg_bytes_per_sec > 0 else target_dur * 1.5

    split_points = _compute_split_points(
        num_parts=num_parts,
        target_dur=target_dur,
        max_part_dur=max_part_dur,
        total_duration_s=total_duration_s,
        keyframe_pts_s=keyframe_pts_s,
        file_name=file_name,
    )

    base_name, _ext = os.path.splitext(file_name)
    actual_num_parts = max(1, len(split_points) - 1)
    plans: list[Fmp4PartPlan] = []
    for i in range(actual_num_parts):
        start_t = split_points[i]
        end_t = split_points[i + 1]
        p_name = f"{base_name}.part{i + 1:03d}.mp4"
        part_is_blind = is_blind or (
            start_t > 0.0 and not any(abs(pts - start_t) < 0.05 for pts in (keyframe_pts_s or []))
        )
        plans.append(
            Fmp4PartPlan(
                part_index=i + 1,
                total_parts=actual_num_parts,
                start_sec=start_t,
                end_sec=end_t,
                duration_sec=end_t - start_t,
                part_name=p_name,
                is_blind=part_is_blind,
            )
        )

    return plans
