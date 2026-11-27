"""Bounded local & remote video metadata probing and remote MPEG-TS head verification.

Architecture:
- Lives in `app/core/` (strictly downward dependencies only).
- Local probing reuses bounded ISO-BMFF / EBML parsers for MP4/MOV/M4V/MKV/WebM first and
  falls back to a bounded local `ffprobe` JSON call (whitelist `file,pipe`)
  for missing duration/dimensions or other supported containers (AVI/WMV/FLV/TS/M2TS/MTS).
- Remote (PikPak) metadata enrichment and MPEG-TS head verification share ONE cross-task
  concurrency semaphore, ONE strict HTTP 206 Range reader (`BudgetedRangeReader`), and a
  total task deadline that covers queue wait + JIT link lookup + Range read + bounded stdin
  `ffprobe` (`pipe:0`, whitelist `pipe`). Never invokes `ffprobe` against remote URLs and never
  logs signed URLs.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import shutil
import struct
import time
import weakref
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import aiohttp

from app.core import cdn_transport as cdn_mod
from app.core.iso_bmff.box_parser import display_dimensions, scaled_display_width
from app.core.keyframe_prober import (
    MkvCuesProber,
    Mp4StssProber,
    TailMoovScanner,
    _safe_url_for_log,
    _sanitize_err_with_url,
)
from app.core.media_sniff import (
    MPEG_TS_MIN_PROBE_BYTES,
    MPEG_TS_MIN_REJECTION_BYTES,
    TS_AMBIGUOUS_EXTENSIONS,
    is_plain_utf8_text,
    is_valid_mpeg_ts_buffer,
)
from app.core.scheduler import UploadCancelledError, UploadPausedError

logger = logging.getLogger(__name__)

# Remote optional metadata enrichment budgets
REMOTE_METADATA_PROBE_DEADLINE_S = 4.0
REMOTE_METADATA_PROBE_MAX_BYTES = 1024 * 1024  # 1 MiB hard cap across all requests per probe
REMOTE_METADATA_PROBE_CONCURRENCY = 2
REMOTE_PREFIX_PROBE_BYTES = 131072  # 128 KiB bounded prefix for non-MP4 pipe:0 ffprobe

# Local bounded ffprobe timeout
LOCAL_FFPROBE_TIMEOUT_S = 4.0
_FFPROBE_CLEANUP_RESERVE_S = 0.25

# Remote MPEG-TS head verification limits
REMOTE_TS_HEAD_MAX_BYTES = 2048
REMOTE_TS_VERIFY_TIMEOUT_S = 4.0

REMOTE_METADATA_PROBE_EXTENSIONS: frozenset[str] = frozenset(
    {".mp4", ".mov", ".m4v", ".mkv", ".webm", ".avi", ".flv", ".wmv", *TS_AMBIGUOUS_EXTENSIONS}
)

_FFPROBE_SHOW_ENTRIES = (
    "format=duration:"
    "stream=index,codec_type,codec_name,width,height,duration,sample_aspect_ratio:"
    "stream_disposition=default,attached_pic,timed_thumbnails:"
    "stream_tags=rotate:"
    "stream_side_data=rotation"
)

_CONTENT_RANGE_RE = re.compile(r"^bytes\s+(\d+)-(\d+)/(\d+|\*)$", re.IGNORECASE)

UrlOrProvider = str | Callable[[], Awaitable[str]]

# Loop-scoped semaphore so concurrent jobs share the small probe limit without cross-loop issues
_LOOP_SEMAPHORES: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, weakref.ReferenceType[asyncio.Semaphore]] = (
    weakref.WeakKeyDictionary()
)


def _get_remote_probe_semaphore() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    cached = _LOOP_SEMAPHORES.get(loop)
    sem = cached() if cached is not None else None
    if sem is None:
        sem = asyncio.Semaphore(REMOTE_METADATA_PROBE_CONCURRENCY)
        _LOOP_SEMAPHORES[loop] = weakref.ref(sem)
    return sem


@dataclass(frozen=True, slots=True)
class ProbedMediaMetadata:
    """Normalized media metadata result (display dimensions, seconds duration, codecs)."""

    duration_s: float = 0.0
    width: int = 0
    height: int = 0
    video_codec: str = ""
    audio_codec: str = ""
    audio_codecs: tuple[str, ...] | None = None


def _valid_positive_float(val: Any) -> float:
    if val is None:
        return 0.0
    try:
        fval = float(val)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    if not math.isfinite(fval) or fval <= 0.0:
        return 0.0
    return fval


def _valid_positive_int(val: Any) -> int:
    if val is None:
        return 0
    try:
        ival = int(float(val))
    except (TypeError, ValueError, OverflowError):
        return 0
    return ival if ival > 0 else 0


def _round_duration(duration_s: float) -> float:
    if not math.isfinite(duration_s) or duration_s <= 0.0:
        return 0.0
    return max(0.001, round(duration_s, 3))


def needs_metadata_probe(item: Any, *, require_codecs: bool = False) -> bool:
    """Return True when duration or either display dimension (or codecs if required) is missing."""
    has_dur = _valid_positive_float(getattr(item, "duration_s", None)) > 0.0
    has_w = _valid_positive_int(getattr(item, "width", None)) > 0
    has_h = _valid_positive_int(getattr(item, "height", None)) > 0
    if not (has_dur and has_w and has_h):
        return True
    if require_codecs:
        has_vcodec = bool(str(getattr(item, "video_codec", "") or "").strip())
        has_acodec = bool(str(getattr(item, "audio_codec", "") or "").strip())
        return not (has_vcodec and has_acodec)
    return False


def apply_probed_metadata(item: Any, meta: ProbedMediaMetadata) -> None:
    """Backfill missing metadata onto item without overwriting complete authoritative fields.

    If item already has complete positive (width, height), both are preserved.
    If either width or height is missing/zero, both are updated coherently from meta when valid.
    """
    if _valid_positive_float(getattr(item, "duration_s", None)) <= 0.0 and meta.duration_s > 0.0:
        item.duration_s = meta.duration_s

    cur_w = _valid_positive_int(getattr(item, "width", None))
    cur_h = _valid_positive_int(getattr(item, "height", None))
    if not (cur_w > 0 and cur_h > 0) and meta.width > 0 and meta.height > 0:
        item.width = meta.width
        item.height = meta.height

    if not str(getattr(item, "video_codec", "") or "").strip() and meta.video_codec:
        item.video_codec = meta.video_codec
    if not str(getattr(item, "audio_codec", "") or "").strip() and meta.audio_codec:
        item.audio_codec = meta.audio_codec
    if getattr(item, "audio_codecs", None) is None and meta.audio_codecs is not None:
        item.audio_codecs = meta.audio_codecs


def _check_control_signals(
    is_cancelled: Callable[[], bool] | None,
    is_paused: Callable[[], bool] | None,
) -> None:
    if is_cancelled and is_cancelled():
        raise UploadCancelledError("任务已取消")
    if is_paused and is_paused():
        raise UploadPausedError("任务已暂停")


def merge_probed_metadata(primary: ProbedMediaMetadata, fallback: ProbedMediaMetadata) -> ProbedMediaMetadata:
    dur = primary.duration_s if primary.duration_s > 0.0 else fallback.duration_s
    if primary.width > 0 and primary.height > 0:
        w, h = primary.width, primary.height
    else:
        w, h = fallback.width, fallback.height
    vcodec = primary.video_codec or fallback.video_codec
    acodec = primary.audio_codec or fallback.audio_codec
    audio_codecs = primary.audio_codecs if primary.audio_codecs is not None else fallback.audio_codecs
    return ProbedMediaMetadata(
        duration_s=dur, width=w, height=h, video_codec=vcodec, audio_codec=acodec, audio_codecs=audio_codecs
    )


# ---------------------------------------------------------------------------
# Shared bounded ffprobe JSON metadata path (local file & stdin pipe:0)
# ---------------------------------------------------------------------------


def _extract_ffprobe_rotation(stream: dict[str, Any]) -> int:
    """Extract orthogonal rotation degrees from ffprobe stream side_data_list or tags."""
    side_data_list = stream.get("side_data_list")
    if isinstance(side_data_list, list):
        for sd in side_data_list:
            if isinstance(sd, dict) and sd.get("rotation") is not None:
                try:
                    return round(float(sd["rotation"])) % 360
                except (TypeError, ValueError):
                    continue
    tags = stream.get("tags")
    if isinstance(tags, dict) and tags.get("rotate") is not None:
        try:
            return round(float(tags["rotate"])) % 360
        except (TypeError, ValueError):
            return 0
    return 0


def _stream_disposition(stream: dict[str, Any]) -> dict[str, Any]:
    raw_disp = stream.get("disposition")
    return raw_disp if isinstance(raw_disp, dict) else {}


def _ffprobe_display_width(width: int, sample_aspect_ratio: object) -> int:
    if not isinstance(sample_aspect_ratio, str):
        return width
    numerator, _, denominator = sample_aspect_ratio.partition(":")
    try:
        return scaled_display_width(width, int(numerator), int(denominator)) or width
    except ValueError:
        return width


def _select_primary_video_stream(streams: list[Any]) -> dict[str, Any] | None:
    """Select the actual primary video stream, skipping attached_pic / timed_thumbnails / audio."""
    candidates: list[dict[str, Any]] = []
    for st in streams:
        if not isinstance(st, dict) or st.get("codec_type") != "video":
            continue
        disp = _stream_disposition(st)
        if disp.get("attached_pic") == 1 or disp.get("timed_thumbnails") == 1:
            continue
        candidates.append(st)
    if not candidates:
        return None
    for st in candidates:
        if (
            _stream_disposition(st).get("default") == 1
            and _valid_positive_int(st.get("width")) > 0
            and _valid_positive_int(st.get("height")) > 0
        ):
            return st
    for st in candidates:
        if _valid_positive_int(st.get("width")) > 0 and _valid_positive_int(st.get("height")) > 0:
            return st
    return candidates[0]


def _parse_ffprobe_json(payload: bytes) -> ProbedMediaMetadata:
    data = json.loads(payload.decode("utf-8", errors="replace"))
    if not isinstance(data, dict):
        return ProbedMediaMetadata()
    raw_fmt = data.get("format")
    fmt: dict[str, Any] = raw_fmt if isinstance(raw_fmt, dict) else {}
    raw_streams = data.get("streams")
    streams: list[Any] = raw_streams if isinstance(raw_streams, list) else []
    v_stream = _select_primary_video_stream(streams)

    duration_s = _valid_positive_float(fmt.get("duration"))
    width = 0
    height = 0
    video_codec = ""
    if v_stream is not None:
        if duration_s <= 0.0:
            duration_s = _valid_positive_float(v_stream.get("duration"))
        enc_w = _valid_positive_int(v_stream.get("width"))
        enc_h = _valid_positive_int(v_stream.get("height"))
        rot = _extract_ffprobe_rotation(v_stream)
        display_w = _ffprobe_display_width(enc_w, v_stream.get("sample_aspect_ratio"))
        width, height = display_dimensions(display_w, enc_h, rot)
        video_codec = str(v_stream.get("codec_name") or "").strip().lower()

    audio_codecs = tuple(
        str(st.get("codec_name") or "").strip().lower()
        for st in streams
        if isinstance(st, dict) and st.get("codec_type") == "audio"
    )

    return ProbedMediaMetadata(
        duration_s=_round_duration(duration_s),
        width=width,
        height=height,
        video_codec=video_codec,
        audio_codec=next((codec for codec in audio_codecs if codec), ""),
        audio_codecs=audio_codecs or None,
    )


async def _terminate_proc(proc: asyncio.subprocess.Process | None, deadline: float, reason: str) -> None:
    if proc is None or proc.returncode is not None:
        return
    try:
        with suppress(ProcessLookupError):
            proc.kill()
        cleanup_deadline = min(deadline, asyncio.get_running_loop().time() + _FFPROBE_CLEANUP_RESERVE_S)
        async with asyncio.timeout_at(cleanup_deadline):
            # Drain the pipes as well as waiting for exit. wait() alone can stall on buffered output.
            await proc.communicate()
    except Exception as kill_err:
        logger.debug("%s终止 ffprobe 进程异常: %s", reason, kill_err)


async def _run_ffprobe(
    *,
    target: str,
    whitelist: str,
    timeout_s: float,
    input_bytes: bytes | None = None,
    log_label: str = "",
) -> ProbedMediaMetadata:
    """Shared bounded ffprobe JSON runner for local paths (`file,pipe`) and stdin (`pipe`)."""
    if timeout_s <= 0.02 or shutil.which("ffprobe") is None:
        return ProbedMediaMetadata()

    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-protocol_whitelist",
        whitelist,
        "-show_entries",
        _FFPROBE_SHOW_ENTRIES,
        "-of",
        "json",
        target,
    ]
    deadline = asyncio.get_running_loop().time() + timeout_s
    work_deadline = deadline - min(_FFPROBE_CLEANUP_RESERVE_S, timeout_s / 2)
    proc: asyncio.subprocess.Process | None = None
    try:
        async with asyncio.timeout_at(work_deadline):
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE if input_bytes is not None else asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_data, _ = await proc.communicate(input=input_bytes)
        if proc.returncode == 0 and stdout_data:
            return _parse_ffprobe_json(stdout_data)
        logger.debug("ffprobe 返回非零或空输出 (rc=%s): %s", proc.returncode, log_label or target)
        return ProbedMediaMetadata()
    except asyncio.CancelledError:
        await _terminate_proc(proc, deadline, "取消时")
        raise
    except TimeoutError:
        logger.debug("ffprobe 探测超时 (%.2fs, 非致命): %s", timeout_s, log_label or target)
        await _terminate_proc(proc, deadline, "超时")
        return ProbedMediaMetadata()
    except Exception as exc:
        logger.debug("ffprobe 探测异常 (非致命): %s (%s)", log_label or target, exc)
        await _terminate_proc(proc, deadline, "异常时")
        return ProbedMediaMetadata()


async def probe_local_ffprobe(file_path: str, *, timeout_s: float = LOCAL_FFPROBE_TIMEOUT_S) -> ProbedMediaMetadata:
    """Bounded local ffprobe fallback (nonfatal; never invoked on remote URLs)."""
    if not file_path or "://" in file_path:
        return ProbedMediaMetadata()
    return await _run_ffprobe(target=file_path, whitelist="file,pipe", timeout_s=timeout_s, log_label=file_path)


async def probe_bytes_ffprobe(data: bytes, *, timeout_s: float = LOCAL_FFPROBE_TIMEOUT_S) -> ProbedMediaMetadata:
    """Bounded ffprobe on already-fetched bytes via stdin `pipe:0` (whitelist `pipe` only)."""
    if not data:
        return ProbedMediaMetadata()
    return await _run_ffprobe(
        target="pipe:0",
        whitelist="pipe",
        timeout_s=timeout_s,
        input_bytes=data,
        log_label="pipe:0",
    )


async def probe_local_video_metadata(file_path: str, ext: str) -> ProbedMediaMetadata:
    """Probe local video duration & display dimensions via fast MP4/MKV parser + ffprobe gap fill."""
    ext_l = ext.lower()
    primary = ProbedMediaMetadata()
    try:
        if ext_l in (".mp4", ".mov", ".m4v"):
            res = await Mp4StssProber.probe_from_url(file_path)
            primary = ProbedMediaMetadata(
                duration_s=res.duration_s,
                width=res.width,
                height=res.height,
                video_codec=res.video_codec,
                audio_codec=res.audio_codec,
            )
        elif ext_l in (".mkv", ".webm"):
            res = await MkvCuesProber.probe_from_url(file_path)
            primary = ProbedMediaMetadata(
                duration_s=res.duration_s,
                width=res.width,
                height=res.height,
                video_codec=res.video_codec,
                audio_codec=res.audio_codec,
            )
    except asyncio.CancelledError:
        raise
    except Exception as parse_err:
        logger.debug("本地容器头探测异常 (将尝试 ffprobe 补齐): %s (%s)", file_path, parse_err)

    if primary.duration_s > 0.0 and primary.width > 0 and primary.height > 0:
        return primary

    fallback = await probe_local_ffprobe(file_path)
    return merge_probed_metadata(primary, fallback)


# ---------------------------------------------------------------------------
# Shared strict HTTP 206 Range reader (byte- & time-budgeted)
# ---------------------------------------------------------------------------


def _parse_content_range(header_val: str | None) -> tuple[int, int, int | None] | None:
    if not header_val:
        return None
    m = _CONTENT_RANGE_RE.match(header_val.strip())
    if not m:
        return None
    start = int(m.group(1))
    end = int(m.group(2))
    total_raw = m.group(3)
    total = int(total_raw) if total_raw != "*" else None
    if end < start:
        return None
    return start, end, total


def _validate_content_length(raw_cl: str | None, declared_len: int, safe_target: str) -> bool:
    if raw_cl is None:
        return True
    try:
        cl_val = int(raw_cl.strip())
    except ValueError:
        return False
    if cl_val != declared_len:
        logger.debug(
            "远端 Range 读取 Content-Length (%d) 与 Content-Range (%d) 不一致: %s",
            cl_val,
            declared_len,
            safe_target,
        )
        return False
    return True


def _validate_range_response(
    resp: aiohttp.ClientResponse,
    *,
    start: int,
    clamped_end: int,
    file_size: int,
    safe_target: str,
) -> int | None:
    """Validate strict HTTP 206 + Content-Range + Content-Length before any body read.

    Returns exact expected body length (`declared_len`) on success, or None to reject.
    """
    ct = (resp.headers.get("Content-Type") or "").lower()
    ce = (resp.headers.get("Content-Encoding") or "").strip().lower()
    if resp.status != 206 or "text/html" in ct or (ce and ce != "identity"):
        logger.debug(
            "远端 Range 读取响应头被拒 (status=%d ct=%s ce=%s): %s",
            resp.status,
            ct,
            ce,
            safe_target,
        )
        return None
    cr = _parse_content_range(resp.headers.get("Content-Range"))
    if cr is None:
        logger.debug("远端 Range 读取缺少合法 Content-Range: %s", safe_target)
        return None
    cr_start, cr_end, cr_total = cr
    total_bad = cr_total is not None and (cr_end >= cr_total or (file_size > 0 and cr_total != file_size))
    incomplete_non_eof = cr_end < clamped_end and (cr_total is None or cr_end != cr_total - 1)
    if cr_start != start or cr_end > clamped_end or total_bad or incomplete_non_eof:
        logger.debug("远端 Range 读取 Content-Range 边界/长度非法: %s", safe_target)
        return None
    declared_len = cr_end - cr_start + 1
    if not _validate_content_length(resp.headers.get("Content-Length"), declared_len, safe_target):
        return None
    return declared_len


class BudgetedRangeReader:
    """Strict HTTP 206 Range reader enforcing a hard cumulative byte cap and total deadline."""

    def __init__(
        self,
        url: str,
        *,
        max_bytes: int = REMOTE_METADATA_PROBE_MAX_BYTES,
        deadline_monotonic: float,
        file_size: int = 0,
        is_cancelled: Callable[[], bool] | None = None,
        is_paused: Callable[[], bool] | None = None,
    ) -> None:
        self.url = url
        self.max_bytes = max(0, int(max_bytes))
        self.deadline_monotonic = deadline_monotonic
        self.file_size = file_size
        self.bytes_reserved = 0
        self.bytes_read = 0
        self.requests_made = 0
        self._is_cancelled = is_cancelled
        self._is_paused = is_paused

    @property
    def remaining_bytes(self) -> int:
        return max(0, self.max_bytes - max(self.bytes_reserved, self.bytes_read))

    def remaining_time(self) -> float:
        return max(0.0, self.deadline_monotonic - time.monotonic())

    async def _read_exact_bounded(self, resp: aiohttp.ClientResponse, expected_len: int) -> bytes:
        buf = bytearray()
        while len(buf) < expected_len:
            _check_control_signals(self._is_cancelled, self._is_paused)
            rem_time = self.remaining_time()
            if rem_time <= 0.01:
                resp.close()
                return b""
            step = min(32 * 1024, expected_len - len(buf))
            try:
                chunk = await asyncio.wait_for(resp.content.readexactly(step), timeout=rem_time)
            except asyncio.IncompleteReadError as inc:
                self.bytes_read += len(inc.partial)
                resp.close()
                return b""
            except TimeoutError:
                resp.close()
                return b""
            self.bytes_read += len(chunk)
            buf.extend(chunk)
        # Confirm HTTP framing without consuming a byte beyond the Range budget.
        await asyncio.wait_for(resp.content.wait_eof(), timeout=self.remaining_time())
        if not resp.content.at_eof():
            resp.close()
            return b""
        _check_control_signals(self._is_cancelled, self._is_paused)
        return bytes(buf)

    async def fetch_range(self, start: int, end: int) -> bytes:
        """Fetch [start, end] inclusive via strict HTTP 206, clamped by remaining byte & time budgets."""
        _check_control_signals(self._is_cancelled, self._is_paused)
        if start < 0 or end < start or self.remaining_bytes <= 0:
            return b""
        rem_time = self.remaining_time()
        if rem_time <= 0.02:
            return b""

        req_len = min(end - start + 1, self.remaining_bytes)
        clamped_end = start + req_len - 1
        # Reserve requested byte budget before any network read so failed/partial reads cannot exceed cap
        self.bytes_reserved += req_len
        self.requests_made += 1
        safe_target = _safe_url_for_log(self.url)

        session = await cdn_mod.get_cdn_session()
        timeout = cdn_mod.cdn_liveness_timeout(total=rem_time, connect=min(3.0, rem_time))
        headers = {"Range": f"bytes={start}-{clamped_end}", "Accept-Encoding": "identity"}
        try:
            async with session.get(self.url, headers=headers, timeout=timeout, auto_decompress=False) as resp:
                declared_len = _validate_range_response(
                    resp,
                    start=start,
                    clamped_end=clamped_end,
                    file_size=self.file_size,
                    safe_target=safe_target,
                )
                if declared_len is None:
                    resp.close()
                    return b""
                return await self._read_exact_bounded(resp, declared_len)
        except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
            raise
        except (TimeoutError, aiohttp.ClientError, OSError) as exc:
            logger.debug(
                "远端 Range 读取失败 (非致命): %s (%s)",
                _sanitize_err_with_url(str(exc), self.url),
                safe_target,
            )
            return b""


# ---------------------------------------------------------------------------
# Remote (PikPak) metadata probing & MPEG-TS head verification
# ---------------------------------------------------------------------------


async def _resolve_remote_url(url_or_provider: UrlOrProvider) -> str:
    url = await url_or_provider() if callable(url_or_provider) else url_or_provider
    if not isinstance(url, str) or not url.startswith(("http://", "https://")):
        return ""
    return url


async def _find_head_moov(reader: BudgetedRangeReader, head_bytes: bytes) -> bytes | None:
    pos = 0
    while pos + 8 <= len(head_bytes):
        b_size, b_type = struct.unpack_from(">I4s", head_bytes, pos)
        if b_size == 1:
            if pos + 16 > len(head_bytes):
                return None
            b_size = struct.unpack_from(">Q", head_bytes, pos + 8)[0]
            h_len = 16
        else:
            h_len = 8
            if b_size == 0 and reader.file_size > pos:
                b_size = reader.file_size - pos
        if b_size < h_len:
            return None
        if b_type == b"moov":
            if pos + b_size <= len(head_bytes):
                return head_bytes[pos : pos + b_size]
            if b_size <= reader.remaining_bytes + (len(head_bytes) - pos):
                missing_start = len(head_bytes)
                missing_end = pos + b_size - 1
                extra = await reader.fetch_range(missing_start, missing_end)
                if len(extra) == (missing_end - missing_start + 1):
                    return head_bytes[pos:] + extra
            return None
        pos += b_size
    return None


async def _probe_remote_mp4(reader: BudgetedRangeReader, file_size: int) -> ProbedMediaMetadata:
    head_len = min(REMOTE_PREFIX_PROBE_BYTES, reader.remaining_bytes)
    if file_size > 0:
        head_len = min(head_len, file_size)
    if head_len < 8:
        return ProbedMediaMetadata()

    head_bytes = await reader.fetch_range(0, head_len - 1)
    if len(head_bytes) < 8:
        return ProbedMediaMetadata()

    moov_bytes = await _find_head_moov(reader, head_bytes)
    if moov_bytes is None and file_size > len(head_bytes) and reader.remaining_bytes >= 16:
        tail_budget = min(file_size - len(head_bytes), reader.remaining_bytes)
        tail_start = file_size - tail_budget
        tail_bytes = await reader.fetch_range(tail_start, file_size - 1)
        if len(tail_bytes) == tail_budget and len(tail_bytes) >= 8:
            moov_bytes = TailMoovScanner(tail_budget).find(tail_bytes, len(tail_bytes))

    if not moov_bytes:
        return ProbedMediaMetadata()
    res = Mp4StssProber._parse_moov_keyframes(moov_bytes)
    return ProbedMediaMetadata(
        duration_s=res.duration_s,
        width=res.width,
        height=res.height,
        video_codec=res.video_codec,
        audio_codec=res.audio_codec,
    )


async def _probe_remote_via_ffprobe_prefix(
    reader: BudgetedRangeReader,
    file_size: int,
    *,
    is_ts: bool,
) -> ProbedMediaMetadata:
    """Fetch bounded 128 KiB prefix via strict 206 Range and probe via stdin `pipe:0` ffprobe."""
    head_len = min(REMOTE_PREFIX_PROBE_BYTES, reader.remaining_bytes)
    if file_size > 0:
        head_len = min(head_len, file_size)
    if head_len < 16:
        return ProbedMediaMetadata()
    head = await reader.fetch_range(0, head_len - 1)
    if not head:
        return ProbedMediaMetadata()
    rem_time = reader.remaining_time()
    if rem_time <= 0.05:
        return ProbedMediaMetadata()
    meta = await probe_bytes_ffprobe(head, timeout_s=rem_time)
    if is_ts and (file_size <= 0 or len(head) < file_size):
        # Truncated TS prefix span is NOT full file duration; keep dimensions/codecs only
        return ProbedMediaMetadata(
            duration_s=0.0,
            width=meta.width,
            height=meta.height,
            video_codec=meta.video_codec,
            audio_codec=meta.audio_codec,
            audio_codecs=meta.audio_codecs,
        )
    return meta


async def probe_remote_video_metadata(
    url_or_provider: UrlOrProvider,
    ext: str,
    *,
    file_size: int = 0,
    deadline_s: float = REMOTE_METADATA_PROBE_DEADLINE_S,
    max_bytes: int = REMOTE_METADATA_PROBE_MAX_BYTES,
    is_cancelled: Callable[[], bool] | None = None,
    is_paused: Callable[[], bool] | None = None,
) -> ProbedMediaMetadata:
    """Fail-open, time- & byte-budgeted remote metadata enrichment for PikPak single/edit paths.

    - Unsupported extensions return immediately with zero semaphore/JIT/network work.
    - Total deadline starts before concurrency queue wait and bounds JIT link resolution,
      strict HTTP 206 Range reads, and bounded stdin `pipe:0` ffprobe.
    - Never runs ffprobe against remote URLs and never logs signed URLs.
    - Returns empty ProbedMediaMetadata on timeout/failure; propagates cancel/pause signals.
    """
    _check_control_signals(is_cancelled, is_paused)
    ext_l = ext.lower()
    if ext_l not in REMOTE_METADATA_PROBE_EXTENSIONS:
        return ProbedMediaMetadata()
    if isinstance(url_or_provider, str) and not url_or_provider.startswith(("http://", "https://")):
        return ProbedMediaMetadata()

    budget_s = max(0.1, float(deadline_s))
    deadline_mono = time.monotonic() + budget_s
    sem = _get_remote_probe_semaphore()
    resolved_url = url_or_provider if isinstance(url_or_provider, str) else ""

    async def _run() -> ProbedMediaMetadata:
        nonlocal resolved_url
        async with sem:
            _check_control_signals(is_cancelled, is_paused)
            url = await _resolve_remote_url(url_or_provider)
            if not url:
                return ProbedMediaMetadata()
            resolved_url = url
            reader = BudgetedRangeReader(
                url,
                max_bytes=max_bytes,
                deadline_monotonic=deadline_mono,
                file_size=file_size,
                is_cancelled=is_cancelled,
                is_paused=is_paused,
            )
            if ext_l in (".mp4", ".mov", ".m4v"):
                return await _probe_remote_mp4(reader, file_size)
            return await _probe_remote_via_ffprobe_prefix(
                reader,
                file_size,
                is_ts=(ext_l in TS_AMBIGUOUS_EXTENSIONS),
            )

    try:
        result = await asyncio.wait_for(_run(), timeout=budget_s)
    except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
        raise
    except TimeoutError:
        logger.debug(
            "远端视频元数据探测超时 (%.1fs, 非致命): %s", budget_s, _safe_url_for_log(resolved_url or "remote")
        )
        result = ProbedMediaMetadata()
    except Exception as exc:
        logger.debug(
            "远端视频元数据探测异常 (非致命): %s (%s)",
            _sanitize_err_with_url(str(exc), resolved_url),
            _safe_url_for_log(resolved_url or "remote"),
        )
        result = ProbedMediaMetadata()
    _check_control_signals(is_cancelled, is_paused)
    return result


async def verify_remote_mpeg_ts_head(
    url_or_provider: UrlOrProvider,
    *,
    file_size: int,
    ext: str = ".ts",
    max_bytes: int = REMOTE_TS_HEAD_MAX_BYTES,
    timeout_s: float = REMOTE_TS_VERIFY_TIMEOUT_S,
    is_cancelled: Callable[[], bool] | None = None,
    is_paused: Callable[[], bool] | None = None,
) -> tuple[bool | None, str | None]:
    """Verify remote .ts/.m2ts/.mts file head (<= 2048 bytes) via shared strict 206 Range reader.

    Shares the cross-task remote probe concurrency semaphore and bounds queue wait + JIT link
    resolution + head read within `timeout_s`.

    Returns:
        (True, None)    - Verified valid MPEG-TS packets.
        (False, reason) - Validated HTTP range with enough bytes to rule out all supported packet layouts.
        (None, None)    - Inconclusive response or insufficient sample: fail-open.
    """
    _check_control_signals(is_cancelled, is_paused)
    if file_size <= 0:
        return None, None
    if isinstance(url_or_provider, str) and not url_or_provider.startswith(("http://", "https://")):
        return None, None

    cap = min(max(1, int(max_bytes)), REMOTE_TS_HEAD_MAX_BYTES)
    expected_len = min(cap, int(file_size))
    if expected_len < MPEG_TS_MIN_PROBE_BYTES:
        return None, None
    budget_s = max(0.1, float(timeout_s))
    deadline_mono = time.monotonic() + budget_s
    sem = _get_remote_probe_semaphore()
    resolved_url = url_or_provider if isinstance(url_or_provider, str) else ""

    async def _run() -> tuple[bool | None, str | None]:
        nonlocal resolved_url
        async with sem:
            _check_control_signals(is_cancelled, is_paused)
            url = await _resolve_remote_url(url_or_provider)
            if not url:
                return None, None
            resolved_url = url
            reader = BudgetedRangeReader(
                url,
                max_bytes=expected_len,
                deadline_monotonic=deadline_mono,
                file_size=file_size,
                is_cancelled=is_cancelled,
                is_paused=is_paused,
            )
            head = await reader.fetch_range(0, expected_len - 1)
            if len(head) != expected_len:
                return None, None
            if is_valid_mpeg_ts_buffer(head):
                return True, None
            if len(head) < MPEG_TS_MIN_REJECTION_BYTES:
                return None, None
            if len(head) < file_size and not is_plain_utf8_text(head):
                return None, None
            return False, f"非有效 MPEG-TS 视频格式 ({ext} 疑似文本源码或不合规封包)，已自动过滤"

    try:
        result = await asyncio.wait_for(_run(), timeout=budget_s)
    except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
        raise
    except TimeoutError:
        logger.debug(
            "远端 TS 头部校验超时 (%.1fs, fail-open): %s", budget_s, _safe_url_for_log(resolved_url or "remote")
        )
        result = None, None
    except Exception as exc:
        logger.debug(
            "远端 TS 头部校验异常 (fail-open): %s (%s)",
            _sanitize_err_with_url(str(exc), resolved_url),
            _safe_url_for_log(resolved_url or "remote"),
        )
        result = None, None
    _check_control_signals(is_cancelled, is_paused)
    return result
