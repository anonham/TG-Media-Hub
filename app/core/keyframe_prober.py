"""
Remote & Local Media Keyframe Prober (MKV Cues / MP4 stss).

从 fmp4_pipe_uploader 收拢:
- 双模 HTTP Range / 本地文件微切片读取 (`_async_fetch_range`, `_async_get_content_length`, `_read_range_body`)
- MKV/WebM EBML Cues 关键帧探测器 (`MkvCuesProber`)
- MP4/MOV stss + stts + elst + ctts 关键帧探测器 (`Mp4StssProber`)
- URL 日志脱敏工具 (`_safe_url_for_log`, `_sanitize_err_with_url`)
"""

import asyncio
import logging
import math
import os
import struct
from collections.abc import Iterator
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import aiohttp

from app.core.cdn_transport import (
    cdn_bulk_timeout,
    cdn_liveness_timeout,
    get_cdn_session,
)
from app.core.iso_bmff.box_parser import (
    extract_mp4a_esds_object_type,
    has_valid_display_dimensions,
    parse_track_aperture_dimensions,
    resolve_mp4a_codec_name,
    rotation_degrees_from_matrix,
    scaled_display_width,
    track_display_dimensions,
)
from app.core.iso_bmff.ebml import _read_ebml_element_id, _read_ebml_vint
from app.core.iso_bmff.fragmented import _iter_iso_boxes
from app.core.redact import redact_text

__all__ = [
    "CuesProbeResult",
    "MkvCuesProber",
    "Mp4StssProber",
]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CuesProbeResult:
    """MKV Cues / MP4 STSS 探测返回契约"""

    duration_s: float
    timescale: int
    keyframe_pts_s: list[float] = field(default_factory=list)
    has_cues: bool = False
    audio_codec: str = ""
    video_codec: str = ""
    width: int = 0
    height: int = 0


async def _read_range_body(
    content: aiohttp.StreamReader,
    expected: int,
) -> bytes:
    """从 aiohttp 响应流中累积读取最多 expected 字节。

    按 64KB 分块迭代累积，读满 expected 或遇到 EOF 自然结束。
    累积缓冲上限为 ``expected + 64KB``；返回时另有一次性的 ``bytes`` 拷贝，
    故瞬时峰值约为 ``2 * expected``（累积缓冲 + 返回值），不做额外的切片副本。
    若传输异常中断，iter_chunked 会抛出 aiohttp.ClientPayloadError，由外层捕获降级。
    """
    buf = bytearray()
    async for chunk in content.iter_chunked(64 * 1024):
        buf.extend(chunk)
        if len(buf) >= expected:
            break
    # 原地截断，避免 buf[:expected] 产生额外的 bytearray 副本（峰值 3x -> 2x）
    if len(buf) > expected:
        del buf[expected:]
    return bytes(buf)


def _safe_url_for_log(url_or_path: str) -> str:
    """脱敏 URL 日志标签：HTTP(S) 直链仅保留 scheme://host/<redacted>，本地路径原样返回。"""
    if not isinstance(url_or_path, str) or not url_or_path:
        return ""
    if url_or_path.startswith(("http://", "https://")):
        try:
            parts = urlsplit(url_or_path)
            if parts.scheme and parts.netloc:
                host = parts.hostname
                if host:
                    if ":" in host:
                        host = f"[{host}]"
                    port = f":{parts.port}" if parts.port is not None else ""
                    return f"{parts.scheme}://{host}{port}/<redacted>"
        except Exception as parse_err:
            logger.debug("解析脱敏 URL host 异常: %s", parse_err)
        return "<redacted-cdn-url>"
    return str(redact_text(url_or_path))


def _sanitize_err_with_url(err_text: str, url_or_path: str = "") -> str:
    """擦除异常/stderr 文本中的原始直链与签名查询参数。"""
    if not err_text:
        return ""
    cleaned = err_text
    if url_or_path and url_or_path.startswith(("http://", "https://")):
        cleaned = cleaned.replace(url_or_path, _safe_url_for_log(url_or_path))
    return str(redact_text(cleaned))


def _read_local_range_sync(path: str, start: int, end: int) -> bytes:
    if not os.path.exists(path):
        return b""
    real_size = os.path.getsize(path)
    if start >= real_size:
        return b""
    read_end = min(end, real_size - 1)
    with open(path, "rb") as f:
        f.seek(start)
        return f.read(read_end - start + 1)


async def _fetch_http_range(url: str, start: int, end: int) -> bytes:
    expected = end - start + 1
    safe_target = _safe_url_for_log(url)
    try:
        session = await get_cdn_session()
        headers = {"Range": f"bytes={start}-{end}"}
        async with session.get(url, headers=headers, timeout=cdn_bulk_timeout()) as resp:
            if resp.status == 206:
                # aiohttp 传输层由 ClientPayloadError 保障响应体与声明一致性；
                # 若返回长度小于 expected，符合 RFC 7233 服务端在 EOF 处截断的规范，
                # 返回可用前缀数据。使用 _read_range_body 累积读取，严格控制内存峰值。
                return await _read_range_body(resp.content, expected)
            if resp.status == 200:
                if start != 0:
                    logger.warning("HTTP Range 请求非期望响应 HTTP 200 (start=%d): %s", start, safe_target)
                    return b""
                declared = resp.content_length
                if declared is not None and declared > expected * 4:
                    logger.warning("CDN 忽略 Range 且响应体过大 (%d bytes), 放弃读取: %s", declared, safe_target)
                    return b""
                return await _read_range_body(resp.content, expected)
            logger.debug("HTTP Range 请求状态码非 200/206 (%d): %s", resp.status, safe_target)
            return b""
    except (TimeoutError, aiohttp.ClientError) as exc:
        logger.debug(
            "HTTP Range 切片拉取网络异常 (非致命): %s (%s, bytes=%d-%d)",
            _sanitize_err_with_url(str(exc), url),
            safe_target,
            start,
            end,
        )
    except Exception as exc:
        logger.warning(
            "HTTP Range 切片未知异常 (非致命): %s (%s, bytes=%d-%d)",
            _sanitize_err_with_url(str(exc), url),
            safe_target,
            start,
            end,
        )
    return b""


async def _async_fetch_range(
    url_or_path: str,
    start: int,
    end: int,
) -> bytes:
    """双模切片读取器：本地文件走 asyncio.to_thread 隔离，网络直链走控制面统一 CDN Session Range 请求。

    契约规范：
    - 若请求范围右边界跨越文件末尾 (EOF)，本地文件与 HTTP 网络均按 RFC 7233 截断并返回实际可用前缀字节；
    - 内存上限严格由请求窗口 (expected) 决定，使用 _read_range_body 累积读取至 expected 或 EOF，绝不因 CDN 异常过量发包而发生 OOM；
    - aiohttp 传输层通过 ClientPayloadError 保障单次响应传输完整性，遇网络故障或超时将安全降级返回 b""。
    """
    if not url_or_path.startswith(("http://", "https://")):
        return await asyncio.to_thread(_read_local_range_sync, url_or_path, start, end)
    return await _fetch_http_range(url_or_path, start, end)


def _local_file_size_sync(path: str) -> int:
    if os.path.exists(path):
        return os.path.getsize(path)
    return 0


async def _async_get_content_length(
    url_or_path: str,
) -> int:
    """获取网络直链 Content-Length 或本地文件大小。"""
    if not url_or_path.startswith(("http://", "https://")):
        return await asyncio.to_thread(_local_file_size_sync, url_or_path)

    safe_target = _safe_url_for_log(url_or_path)
    try:
        session = await get_cdn_session()
        async with session.head(url_or_path, timeout=cdn_liveness_timeout(), allow_redirects=True) as resp:
            if resp.status in (200, 206):
                cl = resp.headers.get("Content-Length")
                return int(cl) if cl and cl.isdigit() else 0
            logger.debug("HEAD 请求状态码非 200/206 (%d): %s", resp.status, safe_target)
            return 0
    except (TimeoutError, aiohttp.ClientError) as exc:
        logger.debug(
            "HEAD 请求拉取大小网络异常 (非致命): %s (%s)",
            _sanitize_err_with_url(str(exc), url_or_path),
            safe_target,
        )
        return 0
    except Exception as exc:
        logger.warning(
            "HEAD 请求拉取大小未知异常 (非致命): %s (%s)",
            _sanitize_err_with_url(str(exc), url_or_path),
            safe_target,
        )
        return 0


def _iter_ebml_elements(
    data: bytes,
    start: int = 0,
    *,
    min_remaining: int = 2,
    break_on_truncated: bool = False,
) -> Iterator[tuple[int, bytes]]:
    pos = start
    data_len = len(data)
    while pos + min_remaining < data_len:
        elem_id, id_len = _read_ebml_element_id(data, pos)
        if id_len <= 0:
            pos += 1
            continue
        elem_size, size_len = _read_ebml_vint(data, pos + id_len)
        if size_len <= 0:
            pos += 1
            continue
        data_start = pos + id_len + size_len
        if break_on_truncated and data_start + elem_size > data_len:
            break
        data_end = min(data_len, data_start + elem_size) if elem_size >= 0 else data_len
        yield elem_id, data[data_start:data_end]
        pos = data_end


def _parse_mkv_seekhead_cues_pos(body: bytes, segment_data_offset: int) -> int:
    cues_pos = -1
    for sid, s_entry in _iter_ebml_elements(body, 0, min_remaining=4):
        if sid != 0x4DBB:  # Seek
            continue
        cur_seek_id: bytes | None = None
        cur_seek_pos: int | None = None
        for kid, kval in _iter_ebml_elements(s_entry, 0, min_remaining=2):
            if kid == 0x53AB:  # SeekID
                cur_seek_id = kval
            elif kid == 0x53AC:  # SeekPosition (1~8 byte int)
                cur_seek_pos = int.from_bytes(kval, "big")
        if cur_seek_id == b"\x1c\x53\xbb\x6b" and cur_seek_pos is not None:
            cues_pos = segment_data_offset + cur_seek_pos
    return cues_pos


def _parse_mkv_info_element(body: bytes, default_timescale: int) -> tuple[int, float]:
    timescale = default_timescale
    raw_duration = 0.0
    has_duration = False
    for iid, ival in _iter_ebml_elements(body, break_on_truncated=True):
        if iid == 0x2AD7B1:  # TimecodeScale
            val = int.from_bytes(ival, "big")
            if val > 0:
                timescale = val
        elif iid == 0x4489:  # Duration (float 4 or 8 bytes)
            if len(ival) == 4:
                raw_duration = struct.unpack(">f", ival)[0]
                has_duration = True
            elif len(ival) == 8:
                raw_duration = struct.unpack(">d", ival)[0]
                has_duration = True
    duration_s = (raw_duration * timescale) / 1e9 if has_duration else 0.0
    return timescale, duration_s if math.isfinite(duration_s) and duration_s > 0.0 else 0.0


def _parse_mkv_video_element(body: bytes) -> tuple[int, int]:
    """Parse EBML Video (0xE0) element for display/pixel width and height."""
    fields = {
        element_id: int.from_bytes(value, "big")
        for element_id, value in _iter_ebml_elements(body, break_on_truncated=True)
        if len(value) <= 8
    }
    pixel_w = fields.get(0xB0, 0)  # PixelWidth
    pixel_h = fields.get(0xBA, 0)  # PixelHeight
    display_unit = fields.get(0x54B2, 0)
    cropped_h = pixel_h - fields.get(0x54BB, 0) - fields.get(0x54AA, 0)  # top/bottom crop
    if display_unit == 0:  # Pixels
        cropped_w = pixel_w - fields.get(0x54CC, 0) - fields.get(0x54DD, 0)  # left/right crop
        width = fields.get(0x54B0, cropped_w)  # DisplayWidth
        height = fields.get(0x54BA, cropped_h)  # DisplayHeight
        if has_valid_display_dimensions(width, height):
            return width, height
    elif display_unit in (1, 2, 3):  # Centimeters, inches, or display aspect ratio
        display_w = fields.get(0x54B0, 0)
        display_h = fields.get(0x54BA, 0)
        width = scaled_display_width(cropped_h, display_w, display_h)
        if has_valid_display_dimensions(width, cropped_h):
            return width, cropped_h
    return (pixel_w, pixel_h) if has_valid_display_dimensions(pixel_w, pixel_h) else (0, 0)


def _parse_mkv_tracks_element(body: bytes) -> tuple[int, int, str, str]:
    """Parse EBML Tracks (0x1654AE6B) for primary video width/height and codec IDs."""
    width = 0
    height = 0
    video_codec = ""
    audio_codec = ""
    best_video_priority: tuple[bool, bool] | None = None
    for tid, track_body in _iter_ebml_elements(body, min_remaining=2, break_on_truncated=True):
        if tid != 0xAE:  # TrackEntry
            continue
        track_type = 0
        flag_enabled = 1
        flag_default = 1
        codec_id = ""
        v_w = 0
        v_h = 0
        for eid, eval_bytes in _iter_ebml_elements(track_body, break_on_truncated=True):
            if eid == 0x83 and eval_bytes:  # TrackType (1=video, 2=audio)
                track_type = int.from_bytes(eval_bytes, "big")
            elif eid == 0xB9 and eval_bytes:  # FlagEnabled
                flag_enabled = int.from_bytes(eval_bytes, "big")
            elif eid == 0x88:  # FlagDefault
                flag_default = int.from_bytes(eval_bytes, "big")
            elif eid == 0x86 and eval_bytes:  # CodecID
                codec_id = eval_bytes.decode("ascii", errors="ignore").strip().lower()
            elif eid == 0xE0 and eval_bytes:  # Video
                v_w, v_h = _parse_mkv_video_element(eval_bytes)
        if flag_enabled == 0:
            continue
        if track_type == 1:
            priority = (v_w > 0 and v_h > 0, flag_default != 0)
            if best_video_priority is None or priority > best_video_priority:
                best_video_priority = priority
                width, height, video_codec = v_w, v_h, codec_id
        elif track_type == 2 and codec_id and not audio_codec:
            audio_codec = codec_id
    return width, height, video_codec, audio_codec


def _parse_mkv_cue_points(cues_data: bytes, c_pos: int, timescale: int) -> list[float]:
    keyframe_pts: list[float] = []
    for cid, cue_body in _iter_ebml_elements(cues_data, c_pos, min_remaining=4, break_on_truncated=True):
        if cid != 0xBB:  # CuePoint element
            continue
        for qid, qval in _iter_ebml_elements(cue_body, 0, min_remaining=2):
            if qid == 0xB3:  # CueTime
                cue_ticks = int.from_bytes(qval, "big")
                keyframe_pts.append(round((cue_ticks * timescale) / 1e9, 3))
    return keyframe_pts


async def _fetch_mkv_cues_pts(url: str, cues_pos: int, timescale: int) -> list[float]:
    init_size = 65536
    cues_data = await _async_fetch_range(url, cues_pos, cues_pos + init_size - 1)
    c_pos = 0
    if len(cues_data) >= 8:
        c_id, c_id_len = _read_ebml_element_id(cues_data, 0)
        if c_id == 0x1C53BB6B:  # Cues element
            c_size, c_size_len = _read_ebml_vint(cues_data, c_id_len)
            if c_size_len > 0:
                c_pos = c_id_len + c_size_len
                fetch_target = min(c_id_len + c_size_len + c_size, 16 * 1024 * 1024)
                if fetch_target > len(cues_data):
                    cues_data += await _async_fetch_range(url, cues_pos + len(cues_data), cues_pos + fetch_target - 1)
    return _parse_mkv_cue_points(cues_data, c_pos, timescale)


def _scan_mkv_head_elements(
    head_data: bytes, scan_start: int, segment_data_offset: int
) -> tuple[int, int, float, int, int, str, str]:
    cues_pos = -1
    timescale = 1000000
    duration_s = 0.0
    width = 0
    height = 0
    video_codec = ""
    audio_codec = ""
    for elem_id, body in _iter_ebml_elements(head_data, scan_start, min_remaining=4):
        if elem_id == 0x114D9B74:  # SeekHead
            found_pos = _parse_mkv_seekhead_cues_pos(body, segment_data_offset)
            if found_pos >= 0:
                cues_pos = found_pos
        elif elem_id == 0x1549A966:  # Info
            timescale, duration_s = _parse_mkv_info_element(body, timescale)
        elif elem_id == 0x1654AE6B:  # Tracks
            t_w, t_h, t_vcodec, t_acodec = _parse_mkv_tracks_element(body)
            if t_w > 0 and t_h > 0:
                width, height = t_w, t_h
            video_codec = video_codec or t_vcodec
            audio_codec = audio_codec or t_acodec
    return cues_pos, timescale, duration_s, width, height, video_codec, audio_codec


class MkvCuesProber:
    """Lightweight EBML Cues prober via HTTP Range requests."""

    @staticmethod
    async def probe_from_url(
        url: str,
    ) -> CuesProbeResult:
        """
        Extracts keyframe timestamps and duration from remote MKV file.
        Uses 2 micro Range requests (0-64KB, and cues block).
        Strict 1~8 byte EBML VINT decoding, element-bounded cues traversal,
        and duration extracted from Info.Duration.
        """
        try:
            head_data = await _async_fetch_range(url, 0, 65535)
            if not head_data or len(head_data) < 16:
                return CuesProbeResult(duration_s=0.0, timescale=1000000, has_cues=False)

            seg_idx = head_data.find(b"\x18\x53\x80\x67")
            segment_data_offset = 0
            if seg_idx != -1:
                _seg_size, vint_len = _read_ebml_vint(head_data, seg_idx + 4)
                segment_data_offset = seg_idx + 4 + (vint_len if vint_len > 0 else 0)

            scan_start = segment_data_offset if segment_data_offset > 0 else 0
            cues_pos, timescale, duration_s, width, height, video_codec, audio_codec = _scan_mkv_head_elements(
                head_data, scan_start, segment_data_offset
            )

            rounded_dur = max(0.001, round(duration_s, 3)) if duration_s > 0 else 0.0
            keyframe_pts = await _fetch_mkv_cues_pts(url, cues_pos, timescale) if cues_pos > 0 else []
            return CuesProbeResult(
                duration_s=rounded_dur,
                timescale=timescale,
                keyframe_pts_s=keyframe_pts,
                has_cues=bool(keyframe_pts),
                audio_codec=audio_codec,
                video_codec=video_codec,
                width=width,
                height=height,
            )
        except Exception as e:
            logger.debug("MkvCuesProber probe failed (non-fatal): %s", e)
            return CuesProbeResult(duration_s=0.0, timescale=1000000, has_cues=False)


def _compute_raw_sample_ticks(stts_entries: list[tuple[int, int]], stss_samples: list[int]) -> list[int]:
    raw_ticks_list: list[int] = []
    stts_idx = 0
    stts_len = len(stts_entries)
    current_sample = 1
    current_ticks = 0
    stts_sample_rem = stts_entries[0][0]
    stts_delta = stts_entries[0][1]

    for target_sample in stss_samples:
        while current_sample < target_sample and stts_idx < stts_len:
            diff = target_sample - current_sample
            if diff <= stts_sample_rem:
                current_ticks += diff * stts_delta
                current_sample += diff
                stts_sample_rem -= diff
                if stts_sample_rem == 0 and stts_idx + 1 < stts_len:
                    stts_idx += 1
                    stts_sample_rem = stts_entries[stts_idx][0]
                    stts_delta = stts_entries[stts_idx][1]
                break
            current_ticks += stts_sample_rem * stts_delta
            current_sample += stts_sample_rem
            stts_idx += 1
            if stts_idx < stts_len:
                stts_sample_rem = stts_entries[stts_idx][0]
                stts_delta = stts_entries[stts_idx][1]
            else:
                stts_sample_rem = 0
        raw_ticks_list.append(current_ticks)
    return raw_ticks_list


def _compute_sample_ctts_offsets(
    ctts_entries: list[tuple[int, int]],
    stss_samples: list[int],
) -> list[int]:
    """Compute per-sample composition offsets (in track timescale) for 1-based stss samples."""
    if not ctts_entries or not stss_samples:
        return [0] * len(stss_samples)

    indexed = sorted((sample, idx) for idx, sample in enumerate(stss_samples))
    offsets = [0] * len(stss_samples)
    c_idx = 0
    c_len = len(ctts_entries)
    cur_sample = 1
    rem_count, cur_offset = ctts_entries[0]

    for target_sample, orig_idx in indexed:
        while cur_sample < target_sample and c_idx < c_len:
            diff = target_sample - cur_sample
            if diff < rem_count:
                cur_sample += diff
                rem_count -= diff
                break
            cur_sample += rem_count
            c_idx += 1
            if c_idx < c_len:
                rem_count, cur_offset = ctts_entries[c_idx]
            else:
                rem_count = 0
                cur_offset = 0
        offsets[orig_idx] = cur_offset if (c_idx < c_len and cur_sample == target_sample) else 0
    return offsets


def _resolve_elst_offsets(
    elst_entries: list[tuple[int, int]] | None, movie_timescale: int, first_cts: int
) -> tuple[bool, float, int]:
    empty_dwell_s = 0.0
    media_offset_ticks = 0
    has_elst_offset = False
    if elst_entries:
        m_timescale = movie_timescale if movie_timescale > 0 else 1000
        for seg_dur, m_time in elst_entries:
            if m_time == -1:
                empty_dwell_s += seg_dur / m_timescale
                has_elst_offset = True
            elif m_time >= 0:
                media_offset_ticks = max(0, m_time - first_cts)
                has_elst_offset = True
                break
    return has_elst_offset, empty_dwell_s, media_offset_ticks


def _parse_mvhd_timescale(mvhd_content: bytes, default: int = 1000) -> int:
    if len(mvhd_content) < 16:
        return default
    version = mvhd_content[0]
    if version == 1 and len(mvhd_content) >= 28:
        return struct.unpack_from(">I", mvhd_content, 20)[0] or default
    if version == 0 and len(mvhd_content) >= 20:
        return struct.unpack_from(">I", mvhd_content, 12)[0] or default
    return default


def _parse_edts_elst(edts_content: bytes) -> list[tuple[int, int]]:
    entries: list[tuple[int, int]] = []
    for eb_type, e_pos, eb_head, eb_size in _iter_iso_boxes(edts_content, 0, len(edts_content)):
        if eb_type != b"elst":
            continue
        elst_content = edts_content[e_pos + eb_head : e_pos + eb_size]
        if len(elst_content) < 8:
            continue
        ev = elst_content[0]
        ecount = struct.unpack_from(">I", elst_content, 4)[0]
        entry_pos = 8
        stride = 20 if ev == 1 else 12
        fmt = ">Qq" if ev == 1 else ">Ii"
        for _ in range(ecount):
            if entry_pos + stride > len(elst_content):
                break
            entries.append(struct.unpack_from(fmt, elst_content, entry_pos))
            entry_pos += stride
    return entries


_CODEC_ALIASES = {"mp4a": "aac", "ac-3": "ac3", "ec-3": "eac3"}


def _parse_stsd_codec(table_body: bytes) -> str:
    if len(table_body) < 16:
        return ""
    entry_count = struct.unpack_from(">I", table_body, 4)[0]
    if entry_count <= 0:
        return ""
    entry_size, fmt = struct.unpack_from(">I4s", table_body, 8)
    if entry_size < 8:
        return ""
    if fmt == b"mp4a" and len(table_body) >= 8 + entry_size:
        obj_type = extract_mp4a_esds_object_type(table_body[8 : 8 + entry_size])
        if obj_type is not None:
            return resolve_mp4a_codec_name(obj_type, default_name="aac")
    raw_codec = str(fmt.decode("ascii", errors="replace")).strip().lower()
    return _CODEC_ALIASES.get(raw_codec, raw_codec)


def _parse_ctts_entries(table_body: bytes) -> list[tuple[int, int]]:
    if len(table_body) < 8:
        return []
    c_ver = table_body[0]
    c_count = struct.unpack_from(">I", table_body, 4)[0]
    c_fmt = ">Ii" if c_ver == 1 else ">II"
    entries: list[tuple[int, int]] = []
    c_pos = 8
    for _ in range(c_count):
        if c_pos + 8 > len(table_body):
            break
        entries.append(struct.unpack_from(c_fmt, table_body, c_pos))
        c_pos += 8
    return entries


def _parse_stsd_visual_dims(table_body: bytes) -> tuple[int, int]:
    """Extract encoded (width, height) from the first VisualSampleEntry in stsd."""
    if len(table_body) < 44:
        return 0, 0
    entry_count = struct.unpack_from(">I", table_body, 4)[0]
    if entry_count <= 0:
        return 0, 0
    entry_size = struct.unpack_from(">I", table_body, 8)[0]
    if entry_size < 40 or len(table_body) < 44:
        return 0, 0
    w, h = struct.unpack_from(">HH", table_body, 40)
    return int(w), int(h)


def _parse_tkhd_dims_and_rotation(box_content: bytes) -> tuple[int, int, int]:
    """Extract presentation width, height and rotation from the tkhd box body."""
    if len(box_content) < 84:
        return 0, 0, 0
    version = box_content[0]
    if version == 1:
        if len(box_content) < 96:
            return 0, 0, 0
        c_pos = 36
    elif version == 0:
        c_pos = 24
    else:
        return 0, 0, 0
    matrix = struct.unpack_from(">9i", box_content, c_pos + 16)
    w_fixed, h_fixed = struct.unpack_from(">II", box_content, c_pos + 52)
    return w_fixed >> 16, h_fixed >> 16, rotation_degrees_from_matrix(matrix)


def _parse_stbl_entries(
    stbl_data: bytes,
) -> tuple[str, list[tuple[int, int]], list[int], list[tuple[int, int]], int, int]:
    codec = ""
    stts: list[tuple[int, int]] = []
    stss: list[int] = []
    ctts: list[tuple[int, int]] = []
    stsd_w = 0
    stsd_h = 0

    for b_box_type, b_pos, b_box_head, b_box_size in _iter_iso_boxes(stbl_data, 0, len(stbl_data)):
        table_body = stbl_data[b_pos + b_box_head : b_pos + b_box_size]
        if b_box_type == b"stsd":
            codec = _parse_stsd_codec(table_body) or codec
            stsd_w, stsd_h = _parse_stsd_visual_dims(table_body)
        elif b_box_type == b"stts" and len(table_body) >= 8:
            entry_count = struct.unpack_from(">I", table_body, 4)[0]
            e_pos = 8
            for _ in range(entry_count):
                if e_pos + 8 > len(table_body):
                    break
                stts.append(struct.unpack_from(">II", table_body, e_pos))
                e_pos += 8
        elif b_box_type == b"stss" and len(table_body) >= 8:
            entry_count = struct.unpack_from(">I", table_body, 4)[0]
            e_pos = 8
            for _ in range(entry_count):
                if e_pos + 4 > len(table_body):
                    break
                stss.append(struct.unpack_from(">I", table_body, e_pos)[0])
                e_pos += 4
        elif b_box_type == b"ctts":
            ctts = _parse_ctts_entries(table_body)
    return codec, stts, stss, ctts, stsd_w, stsd_h


def _parse_mdia_track_info(
    mdia_content: bytes,
) -> tuple[bool, bool, str, int, int, list[tuple[int, int]], list[int], list[tuple[int, int]], int, int]:
    is_video = False
    is_audio = False
    codec = ""
    timescale = 0
    dur_ticks = 0
    stts: list[tuple[int, int]] = []
    stss: list[int] = []
    ctts: list[tuple[int, int]] = []
    stsd_w = 0
    stsd_h = 0

    for mb_type, m_pos, mb_head, mb_size in _iter_iso_boxes(mdia_content, 0, len(mdia_content)):
        m_data = mdia_content[m_pos + mb_head : m_pos + mb_size]
        if mb_type == b"hdlr" and len(m_data) >= 12:
            handler_sub = m_data[8:12]
            if handler_sub == b"vide":
                is_video = True
            elif handler_sub == b"soun":
                is_audio = True
        elif mb_type == b"mdhd" and len(m_data) >= 16:
            v = m_data[0]
            if v == 1 and len(m_data) >= 28:
                timescale = struct.unpack_from(">I", m_data, 20)[0]
                dur_ticks = struct.unpack_from(">Q", m_data, 24)[0]
            elif v == 0 and len(m_data) >= 20:
                timescale = struct.unpack_from(">I", m_data, 12)[0]
                dur_ticks = struct.unpack_from(">I", m_data, 16)[0]
        elif mb_type == b"minf":
            for sb_type, s_pos, sb_head, sb_size in _iter_iso_boxes(m_data, 0, len(m_data)):
                if sb_type == b"stbl":
                    codec, stts, stss, ctts, stsd_w, stsd_h = _parse_stbl_entries(
                        m_data[s_pos + sb_head : s_pos + sb_size]
                    )

    return is_video, is_audio, codec, timescale, dur_ticks, stts, stss, ctts, stsd_w, stsd_h


def _parse_trak_keyframe_info(
    trak_body: bytes,
) -> tuple[
    bool,
    bool,
    str,
    int,
    int,
    int,
    int,
    list[tuple[int, int]],
    list[int],
    list[tuple[int, int]],
    list[tuple[int, int]],
]:
    is_video = False
    is_audio = False
    t_codec = ""
    t_timescale = 0
    t_dur_ticks = 0
    t_w = 0
    t_h = 0
    t_rot = 0
    stsd_w = 0
    stsd_h = 0
    aperture = None
    t_stts: list[tuple[int, int]] = []
    t_stss: list[int] = []
    t_elst: list[tuple[int, int]] = []
    t_ctts: list[tuple[int, int]] = []

    for sub_type, t_pos, sub_head, sub_size in _iter_iso_boxes(trak_body, 0, len(trak_body)):
        box_content = trak_body[t_pos + sub_head : t_pos + sub_size]
        if sub_type == b"tkhd" and len(box_content) >= 84:
            t_w, t_h, t_rot = _parse_tkhd_dims_and_rotation(box_content)
        elif sub_type == b"tapt":
            aperture = parse_track_aperture_dimensions(box_content)
        elif sub_type == b"edts":
            t_elst.extend(_parse_edts_elst(box_content))
        elif sub_type == b"mdia":
            is_video, is_audio, t_codec, t_timescale, t_dur_ticks, t_stts, t_stss, t_ctts, stsd_w, stsd_h = (
                _parse_mdia_track_info(box_content)
            )

    if is_video:
        t_w, t_h = track_display_dimensions(aperture, (t_w, t_h), (stsd_w, stsd_h), t_rot)

    return is_video, is_audio, t_codec, t_timescale, t_dur_ticks, t_w, t_h, t_stts, t_stss, t_elst, t_ctts


async def _find_moov_in_head(url: str, head_bytes: bytes, file_size: int) -> bytes | None:
    pos = 0
    head_len = len(head_bytes)
    while pos + 8 <= head_len:
        b_size, b_type = struct.unpack_from(">I4s", head_bytes, pos)
        if b_size == 1:
            if pos + 16 > head_len:
                break
            b_size = struct.unpack_from(">Q", head_bytes, pos + 8)[0]
            h_len = 16
        else:
            h_len = 8
            if b_size == 0 and file_size > pos:
                b_size = file_size - pos
        if b_size < h_len:
            break
        if b_type == b"moov":
            if pos + b_size <= head_len:
                return head_bytes[pos : pos + b_size]
            fetch_end = min(pos + b_size, 16 * 1024 * 1024)
            return await _async_fetch_range(url, pos, fetch_end - 1)
        pos += b_size
    return None


class TailMoovScanner:
    """Bound candidate validation across all windows ending at the same EOF."""

    def __init__(self, validation_bytes: int) -> None:
        self._candidate_budget = 64
        self._validation_bytes = validation_bytes

    @property
    def exhausted(self) -> bool:
        return self._candidate_budget <= 0 or self._validation_bytes <= 0

    def find(self, data: bytes, search_end: int) -> bytes | None:
        while not self.exhausted:
            moov_idx = data.rfind(b"moov", 0, search_end)
            if moov_idx < 4:
                return None
            self._candidate_budget -= 1
            search_end = moov_idx
            start = moov_idx - 4
            size = struct.unpack_from(">I", data, start)[0]
            header_size = 8
            if size == 1:
                header_size = 16
                if start + header_size > len(data):
                    continue
                size = struct.unpack_from(">Q", data, start + 8)[0]
            elif size == 0:
                size = len(data) - start
            end = start + size
            if size < header_size + 8 or end > len(data):
                continue
            if size > self._validation_bytes:
                continue
            self._validation_bytes -= size
            if any(kind in (b"mvhd", b"trak") for kind, *_ in _iter_iso_boxes(data, start + header_size, end)):
                return data[start:end]
        return None


async def _find_moov_in_tail(url: str, file_size: int) -> bytes | None:
    initial_tail_size = 4 * 1024 * 1024 if file_size > 500 * 1024 * 1024 else 1048576
    tail_fetch_size = min(file_size, initial_tail_size)
    tail_start = file_size - tail_fetch_size
    tail_bytes = await _async_fetch_range(url, tail_start, file_size - 1)
    if len(tail_bytes) != tail_fetch_size or len(tail_bytes) < 8:
        return None

    max_tail_size = min(file_size, 16 * 1024 * 1024)
    scanner = TailMoovScanner(max_tail_size)
    search_end = len(tail_bytes)
    while True:
        candidate = scanner.find(tail_bytes, search_end)
        if candidate is not None or scanner.exhausted or tail_fetch_size >= max_tail_size:
            return candidate
        next_fetch_size = min(tail_fetch_size * 2, max_tail_size)
        next_start = file_size - next_fetch_size
        expected_prefix_len = tail_start - next_start
        prefix = await _async_fetch_range(url, next_start, tail_start - 1)
        if len(prefix) != expected_prefix_len:
            return None
        tail_bytes = prefix + tail_bytes
        tail_fetch_size = next_fetch_size
        tail_start = next_start
        # Rescan only the boundary where a size header or type may have straddled the old window.
        search_end = min(len(tail_bytes), expected_prefix_len + 7)


def _moov_children_offset(moov_bytes: bytes) -> int:
    end = len(moov_bytes)
    if end >= 8:
        b_size, b_type = struct.unpack_from(">I4s", moov_bytes, 0)
        if b_type == b"moov":
            return 16 if (b_size == 1 and end >= 16) else 8
    return 0


class Mp4StssProber:
    """Lightweight MP4/MOV stss keyframe prober via HTTP Range or local file."""

    @staticmethod
    def _compute_keyframe_pts(
        stts_entries: list[tuple[int, int]],
        stss_samples: list[int],
        timescale: int,
        elst_entries: list[tuple[int, int]] | None = None,
        movie_timescale: int = 1000,
        first_cts: int = 0,
        ctts_entries: list[tuple[int, int]] | None = None,
    ) -> list[float]:
        """
        Computes PTS timestamps (seconds) for 1-based sample numbers in stss using stts run-length entries,
        incorporating per-sample ctts composition offsets, elst (Edit List Box) media_time offsets,
        and empty dwell edits.
        Complexity: O(M + N), memory < a few KB.
        """
        if not stss_samples or timescale <= 0 or not stts_entries:
            return []

        raw_dts_list = _compute_raw_sample_ticks(stts_entries, stss_samples)
        if ctts_entries:
            ctts_offsets = _compute_sample_ctts_offsets(ctts_entries, stss_samples)
            pts_ticks_list = [dts + cts for dts, cts in zip(raw_dts_list, ctts_offsets, strict=True)]
            has_elst_offset, empty_dwell_s, media_offset_ticks = _resolve_elst_offsets(elst_entries, movie_timescale, 0)
        else:
            pts_ticks_list = raw_dts_list
            has_elst_offset, empty_dwell_s, media_offset_ticks = _resolve_elst_offsets(
                elst_entries, movie_timescale, first_cts
            )

        if has_elst_offset:
            return [
                round((ticks - media_offset_ticks) / timescale + empty_dwell_s, 3)
                for ticks in pts_ticks_list
                if ticks >= media_offset_ticks
            ]
        return [round(ticks / timescale, 3) for ticks in pts_ticks_list]

    @staticmethod
    def _parse_moov_keyframes(moov_bytes: bytes) -> CuesProbeResult:
        pos = _moov_children_offset(moov_bytes)
        end = len(moov_bytes)

        movie_timescale = 1000
        timescale = 90000
        duration_s = 0.0
        width = 0
        height = 0
        video_codec = ""
        audio_codec = ""
        stts_entries: list[tuple[int, int]] = []
        stss_samples: list[int] = []
        elst_entries: list[tuple[int, int]] = []
        ctts_entries: list[tuple[int, int]] = []
        has_video_trak = False

        for b_type, b_pos, header_len, b_size in _iter_iso_boxes(moov_bytes, pos, end):
            box_body = moov_bytes[b_pos + header_len : b_pos + b_size]
            if b_type == b"mvhd":
                movie_timescale = _parse_mvhd_timescale(box_body, movie_timescale)
            elif b_type == b"trak":
                is_video, is_audio, t_codec, t_timescale, t_dur_ticks, t_w, t_h, t_stts, t_stss, t_elst, t_ctts = (
                    _parse_trak_keyframe_info(box_body)
                )
                if is_video and not has_video_trak:
                    has_video_trak = True
                    timescale = t_timescale or 90000
                    duration_s = (t_dur_ticks / timescale) if (timescale > 0 and t_dur_ticks > 0) else 0.0
                    width, height = t_w, t_h
                    stts_entries, stss_samples, elst_entries, ctts_entries = t_stts, t_stss, t_elst, t_ctts
                    if t_codec:
                        video_codec = t_codec
                elif is_audio and not audio_codec and t_codec:
                    audio_codec = t_codec

                if has_video_trak and audio_codec:
                    break

        if not has_video_trak:
            return CuesProbeResult(duration_s=0.0, timescale=90000, has_cues=False)

        keyframe_pts: list[float] = []
        if stss_samples:
            keyframe_pts = Mp4StssProber._compute_keyframe_pts(
                stts_entries,
                stss_samples,
                timescale,
                elst_entries=elst_entries,
                movie_timescale=movie_timescale,
                ctts_entries=ctts_entries,
            )

        if elst_entries and movie_timescale > 0:
            elst_dur_s = sum(seg_dur for seg_dur, _ in elst_entries) / movie_timescale
            if elst_dur_s > 0:
                duration_s = elst_dur_s

        rounded_dur = max(0.001, round(duration_s, 3)) if duration_s > 0 else 0.0
        return CuesProbeResult(
            duration_s=rounded_dur,
            timescale=timescale,
            keyframe_pts_s=keyframe_pts,
            has_cues=bool(keyframe_pts),
            audio_codec=audio_codec,
            video_codec=video_codec,
            width=width,
            height=height,
        )

    @staticmethod
    async def probe_from_url(
        url: str,
        file_size: int | None = None,
    ) -> CuesProbeResult:
        """
        Extracts keyframe timestamps, duration, and dimensions from remote or local MP4/MOV.

        Lightweight streaming probe: reads front 128KB (faststart), otherwise the file tail
        (1MB, or 4MB when file_size > 500MB), with a hard 16MB cap on any single moov fetch.
        Strictly zero physical disk write.

        Memory note: peak is bounded by the largest single range window, i.e. O(16MB) worst
        case for a 16MB moov; ``_read_range_body`` keeps the transient peak at ~2x that window
        (accumulation buffer + returned bytes), never proportional to the whole file size.
        """
        try:
            if file_size is None or file_size <= 0:
                file_size = await _async_get_content_length(url)

            # Step 1: Read head 128KB
            head_bytes = await _async_fetch_range(url, 0, 131071)
            if not head_bytes or len(head_bytes) < 8:
                return CuesProbeResult(duration_s=0.0, timescale=90000, has_cues=False)

            moov_bytes = await _find_moov_in_head(url, head_bytes, file_size)

            # Step 2: If moov not in head, check tail for rear-moov
            if moov_bytes is None and file_size and file_size > 131072:
                moov_bytes = await _find_moov_in_tail(url, file_size)

            if not moov_bytes:
                return CuesProbeResult(duration_s=0.0, timescale=90000, has_cues=False)

            return Mp4StssProber._parse_moov_keyframes(moov_bytes)
        except Exception as e:
            logger.debug("Mp4StssProber probe failed (non-fatal): %s", e)
            return CuesProbeResult(duration_s=0.0, timescale=90000, has_cues=False)
