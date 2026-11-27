"""
ZeroDisk FMP4 Stream Uploader Module (Phase 1 Universal Fallback Channel)

Production-grade streaming fMP4 slice uploader.
Zero physical disk write, zero /dev/shm usage, peak RSS memory strictly < 35MB.
Video 0 re-encoding (-c:v copy).

Architecture Reference:
- docs/architecture/STREAMING_AND_SPLITTING.md
"""

import asyncio
import collections
import logging
import re
import secrets
import struct
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, NoReturn

import aiohttp

from app.core.cdn_transport import cdn_bulk_timeout
from app.core.iso_bmff.fragmented import (
    RESERVED_SIDX_SPACE,
    build_sidx_and_free_box,
    ensure_mvex_mehd_box,
    parse_moof_video_fragment_info,
    parse_moov_video_properties,
    patch_all_durations,
)
from app.core.keyframe_prober import _safe_url_for_log
from app.core.redact import redact_text
from app.core.scheduler import UploadPausedError
from app.core.stream_chunk_provider import (
    ChunkFetchTransientError,
    PikPakLinkExpiredError,
    PikPakRiskControlError,
    PikPakThrottledError,
    _parse_retry_after,
)
from app.core.stream_chunk_provider import (
    PikPakNotFoundError as ProviderPikPakNotFoundError,
)

__all__ = [
    "PART_SIZE",
    "Fmp4RuntimeState",
    "PartDurationInflatedError",
    "PartOversizeError",
    "PartTruncatedError",
    "ZeroDiskFmp4StreamUploader",
    "fmp4_runtime_state",
    "get_fmp4_global_semaphore",
]

logger = logging.getLogger(__name__)

PART_SIZE = 512 * 1024  # 524,288 bytes (Telegram standard upload part size)
_SMALL_FILE_MAX_BYTES = 10 * 1024 * 1024  # Telethon InputFile/SaveFilePartRequest 上限 (10 MiB)


@dataclass
class Fmp4RuntimeState:
    """fMP4 管道上传器运行态容器 (替代裸 global 变量修改)"""

    semaphore: asyncio.Semaphore | None = None
    semaphore_loop: asyncio.AbstractEventLoop | None = None
    min_frag_duration_ok: bool | None = None


fmp4_runtime_state = Fmp4RuntimeState()
_FFMPEG_GLOBAL_SEMAPHORE: asyncio.Semaphore | None = None
_FFMPEG_SEMAPHORE_LOOP: asyncio.AbstractEventLoop | None = None


def get_fmp4_global_semaphore(limit: int = 2) -> asyncio.Semaphore:
    """安全获取全局 FFmpeg 管道并发信号量 (默认 2，防 OOM / 2C2G CPU 打满)。"""
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None
    sem = _FFMPEG_GLOBAL_SEMAPHORE
    if sem is None or current_loop != _FFMPEG_SEMAPHORE_LOOP:
        sem = asyncio.Semaphore(limit)
        fmp4_runtime_state.semaphore = sem
        fmp4_runtime_state.semaphore_loop = current_loop
        mod: Any = sys.modules[__name__]
        mod._FFMPEG_GLOBAL_SEMAPHORE = sem
        mod._FFMPEG_SEMAPHORE_LOOP = current_loop
    return sem


_FFMPEG_MIN_FRAG_DURATION_OK: bool | None = None


async def _ensure_ffmpeg_min_frag_duration_support() -> None:
    """一次性能力预检 (模块级缓存): mp4 muxer 必须支持 -min_frag_duration (ffmpeg 2.2/2014 起)。
    不支持时 fail fast 并给出可行动的运维指引, 把"ffmpeg 拒识参数的神秘 stderr"变成一句话报错。
    仅在构建默认命令的路径调用 (custom_cmd 干跑不预检)。
    预检进程自身的执行异常 (ffmpeg 缺失/超时) 不缓存、原样上抛——避免把环境故障误判为
    "能力缺失"并永久缓存, 同时保留原始错误的可诊断性。"""
    ok_flag = _FFMPEG_MIN_FRAG_DURATION_OK
    if ok_flag is None:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-hide_banner",
            "-h",
            "muxer=mp4",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_data, _ = await asyncio.wait_for(proc.communicate(), timeout=10.0)
        ok_flag = b"min_frag_duration" in stdout_data
        fmp4_runtime_state.min_frag_duration_ok = ok_flag
        mod: Any = sys.modules[__name__]
        mod._FFMPEG_MIN_FRAG_DURATION_OK = ok_flag
    if not ok_flag:
        raise RuntimeError(
            "服务器 ffmpeg 过旧: mp4 muxer 不支持 -min_frag_duration (fMP4 分片关键帧起始必需)。"
            "请升级 ffmpeg 至 2.2 (2014) 或更新版本后重试。"
        )


class PartTruncatedError(Exception):
    """分卷尾部静默截断异常"""

    def __init__(
        self,
        part_index: int | str = 1,
        real_duration: float = 0.0,
        expected_duration: float = 0.0,
        message: str = "",
    ):
        if isinstance(part_index, str) and not message and real_duration > 0:
            # Tolerant to PartTruncatedError("msg", real_d, exp_d)
            message = part_index
            self.part_index = 1
        else:
            self.part_index = (
                int(part_index)
                if isinstance(part_index, int) or (isinstance(part_index, str) and part_index.isdigit())
                else 1
            )
        self.real_duration = float(real_duration)
        self.expected_duration = float(expected_duration)
        super().__init__(
            message
            or f"Part {self.part_index} truncated: real_duration={self.real_duration:.2f}s < expected={self.expected_duration:.2f}s"
        )


class PartDurationInflatedError(PartTruncatedError):
    """分卷时长超出安全上界异常（由大 GOP 或切点漂移引起，应触发重规划而非重试）"""


class PartOversizeError(Exception):
    """分卷体积超出平台硬顶熔断异常"""

    def __init__(self, part_index: int, total_emitted_bytes: int, max_part_bytes: int, message: str = ""):
        self.part_index = part_index
        self.total_emitted_bytes = total_emitted_bytes
        self.max_part_bytes = max_part_bytes
        super().__init__(
            message
            or f"Part {part_index} oversize: emitted {total_emitted_bytes} bytes exceeds hard cap {max_part_bytes} bytes"
        )


_FFMPEG_THROTTLE_RE = re.compile(
    r"(?:Server returned|HTTP error)\s+(?:(?P<code>429|502|503|504)|5XX)\b"
    r"|\b(?P<phrase>429 Too Many Requests|503 Service Unavailable|502 Bad Gateway|504 Gateway Time-?out)\b",
    re.IGNORECASE,
)
_FFMPEG_NOT_FOUND_RE = re.compile(
    r"(?:Server returned|HTTP error)\s+404\b|\b404 Not Found\b",
    re.IGNORECASE,
)
_FFMPEG_LINK_EXPIRED_RE = re.compile(
    r"(?:Server returned|HTTP error)\s+(?:403|410)\b|\b(?:403 Forbidden|410 Gone)\b",
    re.IGNORECASE,
)


def _raise_from_ffmpeg_stderr(
    stderr_lines: collections.abc.Iterable[str],
    *,
    pikpak_url: str | tuple[str, ...] | list[str],
    returncode: int | None,
    part_index: int,
    prefix: str,
) -> NoReturn:
    """解析 FFmpeg stderr：识别 429/502/503/504 频控、403/410 直链过期与 404，统一脱敏后抛出对应分类异常 (全路径必抛)。"""
    raw_err = "".join(stderr_lines).strip()
    cleaned = raw_err
    urls = pikpak_url if isinstance(pikpak_url, (tuple, list)) else (pikpak_url,)
    for u in urls:
        u_str = str(u)
        if u_str and u_str.startswith(("http://", "https://")):
            cleaned = cleaned.replace(u_str, _safe_url_for_log(u_str))
    safe_err = str(redact_text(cleaned))

    m_throttle = _FFMPEG_THROTTLE_RE.search(raw_err)
    if m_throttle:
        code_str = m_throttle.group("code")
        if not code_str and m_throttle.group("phrase"):
            code_str = m_throttle.group("phrase")[:3]
        status_code = int(code_str) if (code_str and code_str.isdigit()) else 503
        raise PikPakThrottledError(
            f"fMP4 FFmpeg 遭遇 CDN 节点频控/过载 (part={part_index}, status={status_code}): {safe_err}",
            status=status_code,
        )

    if _FFMPEG_LINK_EXPIRED_RE.search(raw_err):
        raise PikPakLinkExpiredError(f"fMP4 FFmpeg 远端直链已过期 (part={part_index}): {safe_err}")

    if _FFMPEG_NOT_FOUND_RE.search(raw_err):
        raise ProviderPikPakNotFoundError(f"fMP4 FFmpeg 远端源文件不存在 (part={part_index}, status=404): {safe_err}")

    raise RuntimeError(f"{prefix} (exit code {returncode}): {safe_err}")


def _make_reconnect_args(url: str) -> list[str]:
    if url.startswith(("http://", "https://")):
        return [
            "-reconnect",
            "1",
            "-reconnect_at_eof",
            "1",
            "-reconnect_on_network_error",
            "1",
            "-reconnect_streamed",
            "1",
            "-reconnect_delay_max",
            "5",
        ]
    return []


def _full_http_source_size(response: aiohttp.ClientResponse, expected_bytes: int | None) -> int | None:
    status = response.status
    if status in (403, 410):
        raise PikPakLinkExpiredError(f"MPEG-TS source link expired (HTTP {status})")
    if status == 404:
        raise ProviderPikPakNotFoundError("MPEG-TS source no longer exists (HTTP 404)")
    if status in (429, 502, 503, 504):
        raise PikPakThrottledError(
            f"MPEG-TS source is throttled (HTTP {status})",
            status=status,
            retry_after=_parse_retry_after(response.headers.get("Retry-After")),
        )
    if response.content_type == "text/html":
        raise PikPakRiskControlError("MPEG-TS source returned an HTML page")
    if status not in (200, 206):
        raise ChunkFetchTransientError(f"Unexpected MPEG-TS source response (HTTP {status})")
    if response.headers.get("Content-Encoding", "identity").lower() != "identity":
        raise ChunkFetchTransientError("MPEG-TS source did not return identity encoding")

    has_expected_size = bool(expected_bytes and expected_bytes > 0)
    response_size = response.content_length
    if status == 206:
        content_range = response.headers.get("Content-Range", "")
        match = re.fullmatch(r"bytes 0-(\d+)/(\d+|\*)", content_range, re.IGNORECASE)
        if not match:
            raise ChunkFetchTransientError("Invalid Content-Range for full MPEG-TS source")
        range_size = int(match[1]) + 1
        if (
            (match[2] == "*" and not has_expected_size)
            or (match[2] != "*" and int(match[2]) != range_size)
            or response_size not in (None, range_size)
        ):
            raise ChunkFetchTransientError("Range response does not cover the full MPEG-TS source")
        response_size = range_size
    if has_expected_size and response_size not in (None, expected_bytes):
        raise ChunkFetchTransientError("MPEG-TS HTTP size does not match the source snapshot")
    return expected_bytes if has_expected_size else response_size


async def _pipe_full_http_source(url: str, stdin: asyncio.StreamWriter, expected_bytes: int | None) -> None:
    """Feed one finite HTTP response to ffmpeg and verify its length before accepting the output."""
    try:
        async with aiohttp.ClientSession(timeout=cdn_bulk_timeout(), auto_decompress=False, trust_env=False) as session:
            headers = {"Range": "bytes=0-", "Accept-Encoding": "identity"}
            async with session.get(url, headers=headers) as response:
                source_size = _full_http_source_size(response, expected_bytes)
                received = 0
                async for chunk in response.content.iter_chunked(65536):
                    received += len(chunk)
                    if source_size is not None and received > source_size:
                        raise ChunkFetchTransientError("MPEG-TS HTTP body exceeds the expected source size")
                    stdin.write(chunk)
                    await stdin.drain()
                if source_size is not None and received != source_size:
                    raise ChunkFetchTransientError("MPEG-TS HTTP body ended before the expected source size")
    except (TimeoutError, aiohttp.ClientError) as exc:
        raise ChunkFetchTransientError(f"MPEG-TS HTTP read failed ({type(exc).__name__})") from exc
    finally:
        stdin.close()


async def _finish_full_http_source(
    task: asyncio.Task[None], returncode: int | None, check_control_signals: Callable[[], None]
) -> None:
    if returncode and not task.done():
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return
    while not task.done():
        check_control_signals()
        await asyncio.wait((task,), timeout=0.2)
    try:
        await task
    except (BrokenPipeError, ConnectionResetError):
        if returncode == 0:
            raise RuntimeError("FFmpeg stopped reading before MPEG-TS input completed") from None
        # The subprocess failed first. Its stderr contains the cause.


def _resolve_stream_urls(
    pikpak_url: str | tuple[str, str] | list[str],
    audio_input_url: str | None,
    copy_audio: bool,
    *,
    force_single_input: bool = False,
) -> tuple[str, str, bool]:
    if isinstance(pikpak_url, (tuple, list)):
        if len(pikpak_url) != 2:
            raise ValueError(
                f"pikpak_url pair must contain exactly 2 URLs: (video_url, audio_url), got {len(pikpak_url)}"
            )
        video_url, audio_url = str(pikpak_url[0]), str(pikpak_url[1])
        is_dual = True
    elif audio_input_url is not None:
        video_url, audio_url = str(pikpak_url), str(audio_input_url)
        is_dual = True
    else:
        video_url = audio_url = str(pikpak_url)
        video_is_http = video_url.startswith(("http://", "https://"))
        # 当直连远程 HTTP 且需转码音轨时自动使用双 -i 独立流避免跨轨乒乓 Seek;
        # 整文件顺序重封装 (force_single_input=True) 单流顺序读更省连接与流量
        is_dual = False if force_single_input else bool(video_is_http and not copy_audio)

    if not video_url:
        raise ValueError("pikpak_url (or video_url) is required for media streaming")
    if is_dual and not audio_url:
        raise ValueError("audio_url is required when dual stream separation is enabled")
    return video_url, audio_url, is_dual


def _build_ffmpeg_command(
    *,
    video_url: str,
    audio_url: str,
    is_dual: bool,
    start_sec: float,
    end_sec: float,
    file_name: str,
    copy_audio: bool,
    is_mkv: bool | None,
    full_stream: bool = False,
    audio_copy_tracks: tuple[int, ...] | None = None,
) -> tuple[list[str], float, tuple[str, ...]]:
    urls_for_stderr: tuple[str, ...] = (video_url, audio_url) if is_dual else (video_url,)
    if is_mkv is None:
        is_mkv = file_name.lower().endswith((".mkv", ".webm")) or any(x in video_url.lower() for x in (".mkv", ".webm"))

    if full_stream:
        duration = float(end_sec - start_sec) if (end_sec > start_sec) else 0.0
    else:
        seek_sec = start_sec + 0.18 if (start_sec > 0.0 and is_mkv) else start_sec
        seek_options = ["-noaccurate_seek"] if start_sec > 0.0 else []
        duration = max(0.1, end_sec - seek_sec)

    # 包4 (4a): 保留源声道布局 (移除 -ac 2 降混)——Telegram 客户端对多声道 AAC 支持良好
    map_all_audio = full_stream or audio_copy_tracks is not None
    if map_all_audio:
        copy_tracks = audio_copy_tracks if audio_copy_tracks is not None else ((0,) if copy_audio else ())
        audio_args = ["-c:a", "aac", "-b:a", "192k"]
        for index in copy_tracks:
            audio_args.extend((f"-c:a:{index}", "copy", f"-bsf:a:{index}", "aac_adtstoasc"))
    elif copy_audio:
        audio_args = ["-c:a", "copy"]
    else:
        audio_args = ["-c:a", "aac", "-b:a", "192k"]
    strict_input_args = ["-xerror", "-abort_on", "empty_output_stream"] if full_stream else []
    common_tail = [
        "-c:v",
        "copy",
        *audio_args,
        "-avoid_negative_ts",
        "make_zero",
        "-movflags",
        "frag_keyframe+empty_moov+default_base_moof+skip_trailer",
        "-min_frag_duration",
        "2000000",
        "-f",
        "mp4",
        "pipe:1",
    ]

    if is_dual:
        v_seek_args = [] if full_stream else [*seek_options, "-ss", f"{seek_sec:.3f}", "-t", f"{duration:.3f}"]
        a_seek_args = [] if full_stream else [*seek_options, "-ss", f"{seek_sec:.3f}", "-t", f"{duration:.3f}"]
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            *_make_reconnect_args(video_url),
            *v_seek_args,
            "-an",
            "-sn",
            "-dn",
            "-probesize",
            "4194304",
            "-analyzeduration",
            "2000000",
            "-i",
            video_url,
            *_make_reconnect_args(audio_url),
            *a_seek_args,
            "-vn",
            "-sn",
            "-dn",
            "-probesize",
            "1048576",
            "-analyzeduration",
            "2000000",
            "-i",
            audio_url,
            "-map",
            "0:v:0",
            "-map",
            "1:a?" if map_all_audio else "1:a:0?",
            *common_tail,
        ]
    else:
        seek_args = [] if full_stream else [*seek_options, "-ss", f"{seek_sec:.3f}", "-t", f"{duration:.3f}"]
        pipe_http = full_stream and video_url.startswith(("http://", "https://"))
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            *strict_input_args,
            *(["-protocol_whitelist", "pipe"] if pipe_http else _make_reconnect_args(video_url)),
            *seek_args,
            "-i",
            "pipe:0" if pipe_http else video_url,
            "-map",
            "0:v:0",
            "-map",
            "0:a?" if map_all_audio else "0:a:0?",
            # If an input subtitle cannot be copied into MP4, fall back to the source file.
            *(["-map", "0:s?", "-c:s", "copy"] if full_stream else []),
            *common_tail,
        ]
    return cmd, duration, urls_for_stderr


async def _prepare_ffmpeg_command(
    *,
    pikpak_url: str | tuple[str, str] | list[str],
    start_sec: float,
    end_sec: float,
    file_name: str,
    copy_audio: bool,
    custom_cmd: list[str] | None,
    is_mkv: bool | None,
    audio_input_url: str | None,
    full_stream: bool = False,
    force_single_input: bool = False,
    audio_copy_tracks: tuple[int, ...] | None = None,
) -> tuple[list[str], float, tuple[str, ...]]:
    if custom_cmd:
        duration = end_sec - start_sec if (end_sec > start_sec) else 10.0
        base_urls = tuple(str(u) for u in pikpak_url) if isinstance(pikpak_url, (tuple, list)) else (str(pikpak_url),)
        urls_for_stderr: tuple[str, ...] = base_urls + ((str(audio_input_url),) if audio_input_url is not None else ())
        return custom_cmd, duration, urls_for_stderr

    if not pikpak_url:
        raise ValueError("pikpak_url is required for media streaming")
    await _ensure_ffmpeg_min_frag_duration_support()
    video_url, audio_url, is_dual = _resolve_stream_urls(
        pikpak_url,
        audio_input_url,
        copy_audio,
        force_single_input=force_single_input or full_stream,
    )
    if full_stream and is_dual:
        raise ValueError("Full stream remux requires a single input")
    return _build_ffmpeg_command(
        video_url=video_url,
        audio_url=audio_url,
        is_dual=is_dual,
        start_sec=start_sec,
        end_sec=end_sec,
        file_name=file_name,
        copy_audio=copy_audio,
        is_mkv=is_mkv,
        full_stream=full_stream,
        audio_copy_tracks=audio_copy_tracks,
    )


def _validate_part_duration(
    part_index: int,
    real_duration: float,
    start_sec: float,
    end_sec: float,
    is_blind: bool,
    *,
    full_stream: bool = False,
) -> None:
    expected_dur = end_sec - start_sec
    if expected_dur <= 0:
        return
    # A TS container duration can include a different audio end time or a timestamp gap.
    min_dur = expected_dur - (max(1.0, expected_dur * 0.05) if full_stream else max(0.5, expected_dur * 0.01))
    max_dur_allowance = max(30.0, expected_dur * 0.15) if is_blind else max(2.0, expected_dur * 0.05)
    max_dur = expected_dur + max_dur_allowance
    if real_duration < min_dur:
        raise PartTruncatedError(
            part_index=part_index,
            real_duration=real_duration,
            expected_duration=expected_dur,
            message=f"Part {part_index} truncated: real_duration={real_duration:.2f}s < min_allowed={min_dur:.2f}s",
        )
    if real_duration > max_dur:
        raise PartDurationInflatedError(
            part_index=part_index,
            real_duration=real_duration,
            expected_duration=expected_dur,
            message=f"Part {part_index} duration inflated: real_duration={real_duration:.2f}s > max_allowed={max_dur:.2f}s",
        )


def _inject_sidx_and_patch_durations(
    part_0_buffer: bytearray,
    *,
    fragments_meta: list[tuple[int, int, bool | None]],
    video_timescale: int,
    video_track_id: int,
    sidx_insert_offset: int,
    sidx_reserve_size: int,
    has_reserved_sidx: bool,
    real_duration: float,
) -> None:
    if (
        has_reserved_sidx
        and sidx_insert_offset + sidx_reserve_size <= PART_SIZE
        and len(part_0_buffer) >= sidx_insert_offset + sidx_reserve_size
    ):
        try:
            sidx_and_free = build_sidx_and_free_box(
                fragments_meta, video_timescale, sidx_reserve_size, track_id=video_track_id
            )
            part_0_buffer[sidx_insert_offset : sidx_insert_offset + sidx_reserve_size] = sidx_and_free
        except Exception as sidx_err:
            logger.warning("Failed to build/inject sidx box (graceful fallback): %s", sidx_err)
    else:
        logger.warning(
            "Skipping sidx injection (has_reserved=%s, offset=%d, part_0_len=%d)",
            has_reserved_sidx,
            sidx_insert_offset,
            len(part_0_buffer),
        )
    patch_all_durations(part_0_buffer, real_duration)


async def _drain_stderr_lines(
    proc_stderr: asyncio.StreamReader,
    stderr_lines: collections.deque[str],
) -> None:
    while True:
        try:
            line = await proc_stderr.readline()
            if not line:
                break
            stderr_lines.append(line.decode(errors="replace"))
        except asyncio.CancelledError:
            raise
        except Exception:
            break


async def _await_stderr_task(stderr_task: asyncio.Task[None]) -> None:
    if stderr_task.done():
        return
    try:
        await asyncio.wait_for(stderr_task, timeout=1.0)
    except asyncio.CancelledError:
        raise
    except Exception:
        if not stderr_task.done():
            stderr_task.cancel()


def _make_input_file_big(file_id: int, total_parts: int, file_name: str) -> Any:
    try:
        from telethon.tl.types import InputFileBig

        return InputFileBig(id=file_id, parts=total_parts, name=file_name)
    except Exception:
        return type("InputFileBig", (), {"id": file_id, "parts": total_parts, "name": file_name})()


def _make_input_file(file_id: int, total_parts: int, file_name: str) -> Any:
    try:
        from telethon.tl.types import InputFile

        return InputFile(id=file_id, parts=total_parts, name=file_name, md5_checksum="")
    except Exception:
        return type("InputFile", (), {"id": file_id, "parts": total_parts, "name": file_name, "md5_checksum": ""})()


class _Fmp4StreamSession:
    """单次 fMP4 管道流式切片状态机：负责 Box 扫描、分卷缓冲切分、进度测速与尾块发射。"""

    def __init__(
        self,
        *,
        uploader: "ZeroDiskFmp4StreamUploader",
        proc: asyncio.subprocess.Process,
        proc_stdout: asyncio.StreamReader,
        file_name: str,
        part_index: int,
        max_part_bytes: int | None,
        progress_callback: Callable[[int, int, float, float, str, int], None] | None,
        is_cancelled: Callable[[], bool] | None,
        is_paused: Callable[[], bool] | None,
        inspect_sink: Callable[[int, int, bytes], Awaitable[None]] | None,
        pause_yield: bool = False,
        auto_small_file_mode: bool = False,
        progress_total_bytes: int | None = None,
    ) -> None:
        self.uploader = uploader
        self.proc = proc
        self.proc_stdout = proc_stdout
        self.file_name = file_name
        self.part_index = part_index
        self.max_part_bytes = max_part_bytes
        self.progress_total_bytes = progress_total_bytes
        self.progress_callback = progress_callback
        self.is_cancelled = is_cancelled
        self.is_paused = is_paused
        self.inspect_sink = inspect_sink
        self.pause_yield = pause_yield
        self.staging_small_file = bool(auto_small_file_mode and inspect_sink is None)
        self.staged_chunks: list[tuple[int, bytes]] = []

        self.out_buf = bytearray()
        self.part_0_buffer: bytearray | None = None
        self.pending_chunk: bytes | None = None
        self.current_part_idx = 0
        self.fragments_meta: list[tuple[int, int, bool | None]] = []
        self.video_track_id = 1
        self.video_timescale = 90000
        self.sidx_insert_offset = 0
        self.sidx_reserve_size = RESERVED_SIDX_SPACE
        self.has_reserved_sidx = False
        self.cur_moof_size = 0
        self.cur_moof_dur = 0
        self.cur_moof_sap: bool | None = None
        self.total_emitted_bytes = 0
        self.speed_window: collections.deque[tuple[float, int]] = collections.deque(maxlen=10)
        self.incomplete_read: asyncio.IncompleteReadError | None = None

    def _check_control_signals(self) -> None:
        if self.uploader._error_event.is_set():
            raise self.uploader._worker_exception or RuntimeError("Worker exception detected")
        if self.is_cancelled and self.is_cancelled():
            raise asyncio.CancelledError("Upload cancelled by caller")
        if self.pause_yield and self.is_paused and self.is_paused():
            raise UploadPausedError("Upload paused by caller")

    async def _wait_if_paused(self) -> None:
        if self.is_paused and self.is_paused():
            if self.pause_yield:
                raise UploadPausedError("Upload paused by caller")
            while self.is_paused():
                await asyncio.sleep(0.5)
                if self.is_cancelled and self.is_cancelled():
                    raise asyncio.CancelledError("Upload cancelled during pause")

    def update_speed_and_progress(self, bytes_delta: int) -> None:
        self.total_emitted_bytes += bytes_delta
        self.uploader.total_emitted_bytes = self.total_emitted_bytes
        if self.max_part_bytes and self.total_emitted_bytes > self.max_part_bytes:
            if self.proc.returncode is None:
                try:
                    self.proc.kill()
                except Exception as kill_err:
                    logger.debug("fMP4 超限熔断终止进程跳过: %s", kill_err)
            raise PartOversizeError(
                part_index=self.part_index,
                total_emitted_bytes=self.total_emitted_bytes,
                max_part_bytes=self.max_part_bytes,
            )
        now = time.monotonic()
        self.speed_window.append((now, self.total_emitted_bytes))
        if len(self.speed_window) >= 2:
            dt = self.speed_window[-1][0] - self.speed_window[0][0]
            db = self.speed_window[-1][1] - self.speed_window[0][1]
            if dt > 0.05:
                self.uploader.last_speed = (db / dt) / (1024 * 1024)

        if self.progress_callback:
            est_total = max(self.progress_total_bytes or 0, self.total_emitted_bytes)
            eta = (
                ((est_total - self.total_emitted_bytes) / (self.uploader.last_speed * 1024 * 1024))
                if self.uploader.last_speed > 0
                else 0.0
            )
            try:
                self.progress_callback(
                    self.total_emitted_bytes,
                    est_total,
                    self.uploader.last_speed,
                    max(0.0, eta),
                    self.file_name,
                    self.uploader.max_workers,
                )
            except Exception as cb_err:
                logger.debug("fMP4 进度回调异常: %s", cb_err)

    async def _dispatch_chunk(self, part_idx: int, total_flag: int, data: bytes) -> None:
        if self.inspect_sink:
            await self.inspect_sink(part_idx, total_flag, data)
        else:
            await self.uploader._enqueue_chunk(
                (part_idx, total_flag, data),
                is_cancelled=self.is_cancelled,
                is_paused=self.is_paused,
                pause_yield=self.pause_yield,
            )

    async def _flush_staged_as_big(self) -> None:
        """总输出跨过 10 MiB 阈值时，切换为 SaveBigFilePartRequest 并立即刷出已暂存的中间块。"""
        self.staging_small_file = False
        self.uploader._use_small_file_parts = False
        staged = self.staged_chunks
        self.staged_chunks = []
        for idx, data in staged:
            await self._dispatch_chunk(idx, -1, data)

    async def _emit_or_stage_previous_chunk(self, part_idx: int, chunk: bytes | None) -> None:
        if chunk is None:
            return
        if self.staging_small_file:
            if self.total_emitted_bytes > _SMALL_FILE_MAX_BYTES:
                await self._flush_staged_as_big()
                await self._dispatch_chunk(part_idx, -1, chunk)
            else:
                self.staged_chunks.append((part_idx, chunk))
        else:
            await self._dispatch_chunk(part_idx, -1, chunk)

    async def flush_available_chunks(self) -> None:
        while len(self.out_buf) >= PART_SIZE:
            self._check_control_signals()
            await self._wait_if_paused()

            block = bytes(self.out_buf[:PART_SIZE])
            del self.out_buf[:PART_SIZE]
            self.update_speed_and_progress(len(block))

            if self.current_part_idx == 0:
                self.part_0_buffer = bytearray(block)
            else:
                await self._emit_or_stage_previous_chunk(self.current_part_idx - 1, self.pending_chunk)
                self.pending_chunk = block
            self.current_part_idx += 1

    async def _await_stdout_read(self, read_fn: Callable[[int], Awaitable[bytes]], n: int) -> bytes:
        while True:
            try:
                return await asyncio.wait_for(read_fn(n), timeout=0.5)
            except TimeoutError:
                self._check_control_signals()

    async def _read_exact_or_eof(self, n: int, *, box_boundary: bool = False) -> bytes | None:
        try:
            return await self._await_stdout_read(self.proc_stdout.readexactly, n)
        except asyncio.IncompleteReadError as exc:
            if not box_boundary or exc.partial:
                self.incomplete_read = exc
            return None

    async def _drain_unbounded_box(self, header: bytes) -> None:
        self.out_buf.extend(header)
        while True:
            chunk = await self._await_stdout_read(self.proc_stdout.read, 65536)
            if not chunk:
                break
            self.out_buf.extend(chunk)
            await self.flush_available_chunks()

    def _handle_moov_box(self, raw_box: bytes) -> None:
        full_box = ensure_mvex_mehd_box(raw_box)
        mehd_growth = len(full_box) - len(raw_box)
        self.sidx_reserve_size = max(64, RESERVED_SIDX_SPACE - mehd_growth)
        self.out_buf.extend(full_box)

        v_id, v_ts, v_w, v_h = parse_moov_video_properties(full_box)
        self.video_track_id = v_id
        self.video_timescale = v_ts
        self.uploader.video_width = v_w
        self.uploader.video_height = v_h

        if len(self.out_buf) + self.sidx_reserve_size <= PART_SIZE:
            self.sidx_insert_offset = len(self.out_buf)
            # 预留空间直接初始化为合法 free box，即便 sidx 回填异常或跳过，容器结构依然 100% 合规
            initial_free = struct.pack(">I4s", self.sidx_reserve_size, b"free") + b"\x00" * (self.sidx_reserve_size - 8)
            self.out_buf.extend(initial_free)
            self.has_reserved_sidx = True
        else:
            logger.warning(
                "moov box offset (%d) + sidx_reserve_size (%d) > PART_SIZE (%d); skipping sidx reservation",
                len(self.out_buf),
                self.sidx_reserve_size,
                PART_SIZE,
            )
            self.sidx_insert_offset = 0
            self.has_reserved_sidx = False

    async def _passthrough_box_body(self, header: bytes, body_len: int) -> None:
        self.out_buf.extend(header)
        remain = body_len
        while remain > 0:
            chunk = await self._await_stdout_read(self.proc_stdout.read, min(65536, remain))
            if not chunk:
                self.incomplete_read = asyncio.IncompleteReadError(b"", remain)
                break
            self.out_buf.extend(chunk)
            remain -= len(chunk)
            await self.flush_available_chunks()

    async def _read_next_box_header(self) -> tuple[bytes, int, bytes] | None:
        header = await self._read_exact_or_eof(8, box_boundary=True)
        if header is None:
            return None

        box_size, box_type = struct.unpack(">I4s", header)
        if box_size == 1:
            ext_len_bytes = await self._read_exact_or_eof(8)
            if ext_len_bytes is None:
                return None
            box_size = struct.unpack(">Q", ext_len_bytes)[0]
            header += ext_len_bytes
        elif box_size == 0:
            await self._drain_unbounded_box(header)
            return None
        elif box_size < 8:
            return None
        return header, box_size, box_type

    async def _process_next_box(self) -> bool:
        parsed = await self._read_next_box_header()
        if parsed is None:
            return False
        header, box_size, box_type = parsed
        body_len = box_size - len(header)

        if box_type in (b"ftyp", b"moov", b"moof"):
            body = await self._read_exact_or_eof(body_len)
            if body is None:
                return False
            full_box = header + body
            if box_type == b"moov":
                self._handle_moov_box(full_box)
            elif box_type == b"ftyp":
                self.out_buf.extend(full_box)
            else:
                self.cur_moof_size = box_size
                self.cur_moof_dur, self.cur_moof_sap = parse_moof_video_fragment_info(
                    full_box, target_track_id=self.video_track_id
                )
                self.out_buf.extend(full_box)
                await self.flush_available_chunks()
            return True

        if box_type == b"mdat" and self.cur_moof_size > 0:
            self.fragments_meta.append((self.cur_moof_size + box_size, self.cur_moof_dur, self.cur_moof_sap))
            self.cur_moof_size = 0
            self.cur_moof_dur = 0
            self.cur_moof_sap = None
        await self._passthrough_box_body(header, body_len)
        return True

    async def read_box_stream(self) -> None:
        while True:
            self._check_control_signals()
            if not await self._process_next_box():
                break

    async def finalize_tail_chunk(self) -> int:
        if len(self.out_buf) == 0:
            return self.current_part_idx
        self.update_speed_and_progress(len(self.out_buf))
        if self.current_part_idx == 0:
            self.part_0_buffer = bytearray(self.out_buf)
            return 1
        await self._emit_or_stage_previous_chunk(self.current_part_idx - 1, self.pending_chunk)
        self.pending_chunk = bytes(self.out_buf)
        return self.current_part_idx + 1

    async def emit_part0_and_tail(self, total_parts: int) -> None:
        assert self.part_0_buffer is not None
        if self.staging_small_file and self.total_emitted_bytes > _SMALL_FILE_MAX_BYTES:
            await self._flush_staged_as_big()
        if self.staging_small_file:
            self.uploader._use_small_file_parts = True
            if total_parts == 1:
                await self._dispatch_chunk(0, 1, bytes(self.part_0_buffer))
                return
            await self._dispatch_chunk(0, -1, bytes(self.part_0_buffer))
            staged = self.staged_chunks
            self.staged_chunks = []
            for idx, data in staged:
                await self._dispatch_chunk(idx, -1, data)
            assert self.pending_chunk is not None
            await self._dispatch_chunk(total_parts - 1, total_parts, self.pending_chunk)
            return
        if total_parts == 1:
            await self._dispatch_chunk(0, 1, bytes(self.part_0_buffer))
            return
        # 多分卷流结束必然留有尾块 (total_parts > 1 蕴含已跨过至少一次分卷边界并赋值 pending_chunk)
        assert self.pending_chunk is not None
        await self._dispatch_chunk(0, -1, bytes(self.part_0_buffer))
        await self._dispatch_chunk(total_parts - 1, total_parts, self.pending_chunk)


class ZeroDiskFmp4StreamUploader:
    """
    Zero-disk, peak RSS < 35MB streaming fMP4 slice uploader.

    Architecture & Resilience Guarantees:
    - Lightweight resilience model: adopts volume-level JIT link renewal and full part retry,
      strictly avoiding on-disk chunk checkpoint metadata and tmpfs buffering.
    - Telegram streaming protocol: uses file_total_parts=-1 for non-final streaming chunks,
      and finalizes total_parts on the lookahead tail chunk.
    - Memory contract: fragments_meta list scales with slice duration (~3,600 items for 2h
      at 2s fragmenting = ~150KB RAM), well within the 35MB peak RSS boundary.
    """

    def __init__(
        self,
        client: Any,
        max_workers: int = 4,
        queue_size: int = 12,
        in_flight_per_connection: int = 1,
        capture_sink: Callable[[int, int, bytes], None] | None = None,
    ):
        self.client = client
        self.max_workers = max(1, int(max_workers or 1))
        self.in_flight_per_connection = max(1, min(int(in_flight_per_connection or 1), 4))
        self.queue_size = queue_size
        self.chunk_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        self.file_id = secrets.randbits(63)
        # E2E 捕获 (docs/testing/E2E_GUIDE.md §5.1 D3/E1): 伴随真实上传的产物取证,
        # (idx, total_flag, data) 同步可调用, 定位写 seek(idx*PART_SIZE); 与 inspect_sink
        # (干跑, 整体跳过真实上传) 互不干扰——capture 挂 _enqueue_chunk 入口, inspect 分支不走 enqueue。
        self._capture_sink = capture_sink
        self.last_speed = 0.0
        self.total_emitted_bytes = 0
        self.video_width = 0
        self.video_height = 0
        self._proc: asyncio.subprocess.Process | None = None
        self._workers: list[asyncio.Task] = []
        self._worker_senders: dict[int, Any] = {}
        self._sender_locks: dict[int, asyncio.Lock] = {}
        self._dc_options: list[Any] = []
        self._dc_id: int = 4
        self._error_event = asyncio.Event()
        self._worker_exception: Exception | None = None
        self._flood_wait_until: float = 0.0
        self._total_flood_wait: float = 0.0
        self._max_cumulative_flood_wait: float = 180.0
        self._use_small_file_parts: bool = False

    def _is_real_telethon_client(self) -> bool:
        if self.client is None:
            return False
        try:
            from telethon import TelegramClient
            from telethon.crypto import AuthKey

            if not isinstance(self.client, TelegramClient):
                return False
            session = getattr(self.client, "session", None)
            auth_key = getattr(session, "auth_key", None)
            dc_id = getattr(session, "dc_id", None)
            return bool(isinstance(auth_key, AuthKey) and auth_key and dc_id)
        except Exception as e:
            logger.debug("fMP4 客户端类型探测跳过: %s", e)
            return False

    async def _init_single_sender(self, sender_idx: int) -> Any:
        if not self._is_real_telethon_client():
            return None
        from app.core.fast_telethon import sender_pool

        dc_opt = self._dc_options[sender_idx % len(self._dc_options)] if self._dc_options else None
        try:
            s = await sender_pool._create_worker_sender_same_dc(self.client, self._dc_id, dc_option=dc_opt)
            if s:
                sender_pool._tune_sender_socket(s)
            return s
        except Exception as e:
            logger.warning("⚠️ [fMP4 Upload] 预建独立 Sender %d 降级: %s", sender_idx + 1, e)
            try:
                s = await sender_pool._get_worker_sender(self.client, self._dc_id)
                if s:
                    sender_pool._tune_sender_socket(s)
                return s
            except Exception as fb_err:
                logger.debug("fMP4 借用 Sender %d 降级失败: %s", sender_idx + 1, fb_err)
                return None

    async def _prepare_worker_senders(self) -> None:
        if not self._is_real_telethon_client():
            return
        session = getattr(self.client, "session", None)
        self._dc_id = int(getattr(session, "dc_id", 4) or 4)
        try:
            from app.core.fast_telethon import sender_pool

            self._dc_options = await sender_pool._prepare_upload_dc_options(self.client, self._dc_id)
        except Exception as e:
            logger.debug("fMP4 上传 DC 配置获取与端点探测异常: %s", e)

        try:
            from app.core.fast_telethon import sender_pool

            senders = await sender_pool._gather_worker_senders_with_cleanup(
                [self._init_single_sender(i) for i in range(self.max_workers)],
                [self.client],
                self._dc_id,
            )
            for i, s in enumerate(senders):
                if s is not None:
                    self._worker_senders[i] = s
        except BaseException as e:
            logger.debug("fMP4 并发预建独立 Sender 中断或降级: %s", e)
            await self._close_all_worker_senders()
            if isinstance(e, asyncio.CancelledError):
                raise

    async def _reconnect_worker_sender(self, sender_idx: int, failed_sender: Any) -> Any:
        lock = self._sender_locks.setdefault(sender_idx, asyncio.Lock())
        async with lock:
            current = self._worker_senders.get(sender_idx)
            if current is not None and current is not failed_sender:
                return current
            if current is not None:
                try:
                    from app.core.fast_telethon import sender_pool

                    await sender_pool._close_worker_sender(self.client, current, self._dc_id)
                except Exception as close_err:
                    logger.debug("fMP4 关闭失效 Sender %d 异常: %s", sender_idx, close_err)
                self._worker_senders.pop(sender_idx, None)
            new_sender = await self._init_single_sender(sender_idx)
            if new_sender is not None:
                self._worker_senders[sender_idx] = new_sender
            return new_sender

    async def _close_all_worker_senders(self) -> None:
        if not self._worker_senders:
            return
        try:
            from app.core.fast_telethon import sender_pool

            for idx, s in list(self._worker_senders.items()):
                if s is not None:
                    try:
                        await sender_pool._close_worker_sender(self.client, s, self._dc_id)
                    except Exception as close_err:
                        logger.debug("fMP4 释放 Sender %d 异常: %s", idx, close_err)
        finally:
            self._worker_senders.clear()

    async def _send_part_request(self, sender: Any, req: Any) -> None:
        res: Any = True
        if sender is not None and hasattr(sender, "send"):
            fut = sender.send(req)
            if isinstance(fut, list):
                fut = fut[0]
            if hasattr(fut, "__await__") or isinstance(fut, (asyncio.Future, asyncio.Task)):
                res = await asyncio.wait_for(fut, timeout=30.0)
            else:
                res = fut
        else:
            res = await asyncio.wait_for(self.client(req), timeout=30.0)
        if res is False:
            raise RuntimeError(
                f"Telegram returned False for upload part request (part={getattr(req, 'file_part', '?')})"
            )

    async def _wait_flood_cooldown(self) -> None:
        while not self._error_event.is_set():
            wait_rem = self._flood_wait_until - time.time()
            if wait_rem <= 0:
                return
            await asyncio.sleep(wait_rem)

    async def _handle_worker_flood_wait(self, exc: Exception, part_index: int, total_parts: int) -> None:
        wait_sec = max(int(getattr(exc, "seconds", 5) or 5), 1)
        now_ts = time.time()
        effective_base = max(self._flood_wait_until, now_ts)
        new_deadline = max(self._flood_wait_until, now_ts + wait_sec)
        non_overlapping_wait = max(0.0, new_deadline - effective_base)
        self._flood_wait_until = new_deadline
        self._total_flood_wait += non_overlapping_wait
        logger.warning(
            "Telegram FloodWaitError in upload worker (part %d, total %s): waiting %d+1s (cumulative: %.1fs/%.1fs)",
            part_index,
            str(total_parts),
            wait_sec,
            self._total_flood_wait,
            self._max_cumulative_flood_wait,
        )
        try:
            from app.core.client_manager import client_manager

            client_manager.mark_flood_wait(self.client, wait_sec)
        except Exception as mark_err:
            logger.debug("fMP4 标记 FloodWait 状态异常: %s", mark_err)

        if self._total_flood_wait > self._max_cumulative_flood_wait:
            self._worker_exception = exc
            self._error_event.set()
            logger.error(
                "Cumulative FloodWait (%.1fs) exceeded threshold (%.1fs) on part %d",
                self._total_flood_wait,
                self._max_cumulative_flood_wait,
                part_index,
            )
            raise exc

        await asyncio.sleep(wait_sec + 1)

    async def _upload_single_item(
        self,
        worker_id: int,
        sender_idx: int,
        item: tuple[int, int, bytes],
        save_big_file_part_req: Any,
        flood_wait_error_cls: Any,
        save_file_part_req: Any = None,
    ) -> None:
        part_index, total_parts, payload = item
        part_uploaded = False
        net_attempt = 0

        while not part_uploaded and not self._error_event.is_set():
            await self._wait_flood_cooldown()
            if self._error_event.is_set():
                return

            sender = self._worker_senders.get(sender_idx)
            try:
                if self.client:
                    if self._use_small_file_parts and save_file_part_req:
                        req = save_file_part_req(
                            file_id=self.file_id,
                            file_part=part_index,
                            bytes=payload,
                        )
                        await self._send_part_request(sender, req)
                    elif save_big_file_part_req:
                        req = save_big_file_part_req(
                            file_id=self.file_id,
                            file_part=part_index,
                            file_total_parts=int(total_parts),
                            bytes=payload,
                        )
                        await self._send_part_request(sender, req)
                part_uploaded = True
                return
            except Exception as exc:
                if flood_wait_error_cls and isinstance(exc, flood_wait_error_cls):
                    await self._handle_worker_flood_wait(exc, part_index, total_parts)
                    continue

                net_attempt += 1
                if sender is not None and net_attempt < 5:
                    try:
                        await self._reconnect_worker_sender(sender_idx, sender)
                    except Exception as reconn_err:
                        logger.debug("fMP4 Worker %d 重建独立 Sender 异常: %s", worker_id, reconn_err)
                if net_attempt >= 5:
                    self._worker_exception = exc
                    self._error_event.set()
                    logger.error(
                        "Upload worker failed after 5 network attempts on part %d (total_parts=%s): %s",
                        part_index,
                        str(total_parts),
                        exc,
                    )
                    raise
                await asyncio.sleep(1.0 * net_attempt)

        if not part_uploaded and not self._error_event.is_set():
            self._worker_exception = RuntimeError(f"Part {part_index} upload dropped unexpectedly")
            self._error_event.set()
            raise self._worker_exception

    async def _upload_worker(self, worker_id: int = 0) -> None:
        try:
            from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest
        except ImportError:
            SaveBigFilePartRequest = None
            SaveFilePartRequest = None

        try:
            from telethon.errors import FloodWaitError
        except ImportError:
            FloodWaitError = None

        sender_idx = worker_id % self.max_workers
        while True:
            item = await self.chunk_queue.get()
            if item is None:
                self.chunk_queue.task_done()
                break
            try:
                if not self._error_event.is_set():
                    await self._upload_single_item(
                        worker_id,
                        sender_idx,
                        item,
                        SaveBigFilePartRequest,
                        FloodWaitError,
                        save_file_part_req=SaveFilePartRequest,
                    )
            finally:
                self.chunk_queue.task_done()

    async def _enqueue_chunk(
        self,
        chunk_item: tuple[int, int, bytes] | None,
        *,
        is_cancelled: Callable[[], bool] | None = None,
        is_paused: Callable[[], bool] | None = None,
        pause_yield: bool = False,
    ) -> None:
        # E2E 捕获伴随调用 (E1): 终止哨兵 None 不捕获; sink 异常吞断告警——engines 侧
        # 整卷重试的 except 面极宽, 磁盘异常冒泡会触发无意义 FFmpeg 重跑 (E2)
        sink = self._capture_sink
        if sink is not None and chunk_item is not None:
            try:
                sink(*chunk_item)
            except Exception as cap_err:
                logger.warning("E2E 捕获 capture_sink 异常 (忽略, 继续上传): %s", cap_err)
        while True:
            if self._error_event.is_set():
                raise self._worker_exception or RuntimeError("Worker exception encountered")
            if is_cancelled and is_cancelled():
                raise asyncio.CancelledError("Upload cancelled by caller")
            if pause_yield and is_paused and is_paused():
                raise UploadPausedError("Upload paused by caller")
            try:
                await asyncio.wait_for(self.chunk_queue.put(chunk_item), timeout=0.2)
                return
            except TimeoutError:
                continue

    async def stream_to_telegram(
        self,
        pikpak_url: str | tuple[str, str] | list[str],
        start_sec: float,
        end_sec: float,
        file_name: str,
        copy_audio: bool = False,
        part_index: int = 1,
        max_part_bytes: int | None = None,
        progress_callback: Callable[[int, int, float, float, str, int], None] | None = None,
        is_cancelled: Callable[[], bool] | None = None,
        is_paused: Callable[[], bool] | None = None,
        inspect_sink: Callable[[int, int, bytes], Awaitable[None]] | None = None,
        custom_cmd: list[str] | None = None,
        is_mkv: bool | None = None,
        is_blind: bool = False,
        capture_sink: Callable[[int, int, bytes], None] | None = None,
        audio_input_url: str | None = None,
        *,
        full_stream: bool = False,
        force_single_input: bool = False,
        pause_yield: bool = False,
        auto_small_file_mode: bool = False,
        estimated_output_bytes: int | None = None,
        expected_input_bytes: int | None = None,
        audio_copy_tracks: tuple[int, ...] | None = None,
    ) -> tuple[Any, float]:
        if capture_sink is not None:
            # 允许按调用覆盖 (构造期注入与调用期注入二选一即可)
            self._capture_sink = capture_sink
        sem = get_fmp4_global_semaphore()
        async with sem:
            return await self._stream_to_telegram_core(
                pikpak_url=pikpak_url,
                start_sec=start_sec,
                end_sec=end_sec,
                file_name=file_name,
                copy_audio=copy_audio,
                part_index=part_index,
                max_part_bytes=max_part_bytes,
                progress_callback=progress_callback,
                is_cancelled=is_cancelled,
                is_paused=is_paused,
                inspect_sink=inspect_sink,
                custom_cmd=custom_cmd,
                is_mkv=is_mkv,
                is_blind=is_blind,
                audio_input_url=audio_input_url,
                full_stream=full_stream,
                force_single_input=force_single_input,
                pause_yield=pause_yield,
                auto_small_file_mode=auto_small_file_mode,
                estimated_output_bytes=estimated_output_bytes,
                expected_input_bytes=expected_input_bytes,
                audio_copy_tracks=audio_copy_tracks,
            )

    async def _finalize_workers(
        self,
        workers: list[asyncio.Task[None]],
        *,
        is_cancelled: Callable[[], bool] | None = None,
        is_paused: Callable[[], bool] | None = None,
        pause_yield: bool = False,
    ) -> None:
        for _ in range(len(workers)):
            await self._enqueue_chunk(
                None,
                is_cancelled=is_cancelled,
                is_paused=is_paused,
                pause_yield=pause_yield,
            )
        pending: set[asyncio.Task[None]] = set(workers)
        while pending:
            if is_cancelled and is_cancelled():
                raise asyncio.CancelledError("Upload cancelled by caller")
            if pause_yield and is_paused and is_paused():
                raise UploadPausedError("Upload paused by caller")
            done, pending = await asyncio.wait(pending, timeout=0.2, return_when=asyncio.FIRST_EXCEPTION)
            for task in done:
                exc = task.exception()
                if exc is not None:
                    raise exc
        if self._error_event.is_set():
            raise self._worker_exception or RuntimeError("Worker exception occurred during final flush")

    async def _cleanup_stream_resources(
        self,
        proc: asyncio.subprocess.Process,
        stderr_task: asyncio.Task[None] | None,
        workers: list[asyncio.Task[None]],
        http_input_task: asyncio.Task[None] | None,
    ) -> None:
        stream_tasks = [task for task in (stderr_task, http_input_task) if task is not None]
        for task in stream_tasks:
            if not task.done():
                task.cancel()
        if proc.returncode is None:
            try:
                proc.kill()
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except Exception as kill_err:
                logger.debug("fMP4 终止 FFmpeg 子进程跳过: %s", kill_err)

        for w in workers:
            if not w.done():
                w.cancel()
        await asyncio.gather(*workers, *stream_tasks, return_exceptions=True)
        await self._close_all_worker_senders()

    async def _stream_to_telegram_core(
        self,
        pikpak_url: str | tuple[str, str] | list[str],
        start_sec: float,
        end_sec: float,
        file_name: str,
        copy_audio: bool,
        part_index: int,
        max_part_bytes: int | None,
        progress_callback: Callable[[int, int, float, float, str, int], None] | None,
        is_cancelled: Callable[[], bool] | None,
        is_paused: Callable[[], bool] | None,
        inspect_sink: Callable[[int, int, bytes], Awaitable[None]] | None,
        custom_cmd: list[str] | None,
        is_mkv: bool | None = None,
        is_blind: bool = False,
        audio_input_url: str | None = None,
        full_stream: bool = False,
        force_single_input: bool = False,
        pause_yield: bool = False,
        auto_small_file_mode: bool = False,
        estimated_output_bytes: int | None = None,
        expected_input_bytes: int | None = None,
        audio_copy_tracks: tuple[int, ...] | None = None,
    ) -> tuple[Any, float]:
        self._use_small_file_parts = False
        cmd, duration, urls_for_stderr = await _prepare_ffmpeg_command(
            pikpak_url=pikpak_url,
            start_sec=start_sec,
            end_sec=end_sec,
            file_name=file_name,
            copy_audio=copy_audio,
            custom_cmd=custom_cmd,
            is_mkv=is_mkv,
            audio_input_url=audio_input_url,
            full_stream=full_stream,
            force_single_input=force_single_input,
            audio_copy_tracks=audio_copy_tracks,
        )

        pipe_http = full_stream and not custom_cmd and urls_for_stderr[0].startswith(("http://", "https://"))
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE if pipe_http else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        assert proc.stdout is not None and proc.stderr is not None  # 上方已显式声明双 PIPE
        self._proc = proc

        stderr_lines: collections.deque[str] = collections.deque(maxlen=200)
        stderr_task: asyncio.Task[None] | None = None
        http_input_task: asyncio.Task[None] | None = None
        workers: list[asyncio.Task[None]] = []
        session = _Fmp4StreamSession(
            uploader=self,
            proc=proc,
            proc_stdout=proc.stdout,
            file_name=file_name,
            part_index=part_index,
            max_part_bytes=max_part_bytes,
            progress_callback=progress_callback,
            is_cancelled=is_cancelled,
            is_paused=is_paused,
            inspect_sink=inspect_sink,
            pause_yield=pause_yield,
            auto_small_file_mode=auto_small_file_mode,
            progress_total_bytes=estimated_output_bytes if full_stream else max_part_bytes,
        )

        try:
            stderr_task = asyncio.create_task(_drain_stderr_lines(proc.stderr, stderr_lines))
            if pipe_http:
                assert proc.stdin is not None
                http_input_task = asyncio.create_task(
                    _pipe_full_http_source(urls_for_stderr[0], proc.stdin, expected_input_bytes)
                )
            if inspect_sink is None:
                await self._prepare_worker_senders()
            total_worker_coros = self.max_workers * self.in_flight_per_connection
            workers = [asyncio.create_task(self._upload_worker(i)) for i in range(total_worker_coros)]
            self._workers = workers

            await session.read_box_stream()
            await proc.wait()
            await _await_stderr_task(stderr_task)
            if http_input_task is not None:
                await _finish_full_http_source(http_input_task, proc.returncode, session._check_control_signals)

            if self._error_event.is_set():
                raise self._worker_exception or RuntimeError("Worker exception occurred during stream")
            if proc.returncode != 0:
                _raise_from_ffmpeg_stderr(
                    stderr_lines,
                    pikpak_url=urls_for_stderr,
                    returncode=proc.returncode,
                    part_index=part_index,
                    prefix="FFmpeg process exited abnormally",
                )
            if session.incomplete_read is not None:
                raise session.incomplete_read

            total_parts = await session.finalize_tail_chunk()
            if session.part_0_buffer is None:
                _raise_from_ffmpeg_stderr(
                    stderr_lines,
                    pikpak_url=urls_for_stderr,
                    returncode=proc.returncode,
                    part_index=part_index,
                    prefix="FFmpeg stream completed without producing media data",
                )

            total_ticks = sum(frag[1] for frag in session.fragments_meta)
            if full_stream and (session.video_timescale <= 0 or total_ticks <= 0):
                raise RuntimeError("FFmpeg full stream completed without timed video fragments")
            real_duration = (
                (total_ticks / session.video_timescale)
                if (session.video_timescale > 0 and total_ticks > 0)
                else duration
            )
            _validate_part_duration(part_index, real_duration, start_sec, end_sec, is_blind, full_stream=full_stream)
            _inject_sidx_and_patch_durations(
                session.part_0_buffer,
                fragments_meta=session.fragments_meta,
                video_timescale=session.video_timescale,
                video_track_id=session.video_track_id,
                sidx_insert_offset=session.sidx_insert_offset,
                sidx_reserve_size=session.sidx_reserve_size,
                has_reserved_sidx=session.has_reserved_sidx,
                real_duration=real_duration,
            )

            await session.emit_part0_and_tail(total_parts)
            if not inspect_sink:
                await self._finalize_workers(
                    workers,
                    is_cancelled=is_cancelled,
                    is_paused=is_paused,
                    pause_yield=pause_yield,
                )

            res_obj = (
                _make_input_file(self.file_id, total_parts, file_name)
                if self._use_small_file_parts
                else _make_input_file_big(self.file_id, total_parts, file_name)
            )
            logger.info(
                "🎬 fMP4 切片完成: part=%d, duration=%.2fs, bytes=%d, v_pts_aligned=%s",
                part_index,
                real_duration,
                session.total_emitted_bytes,
                not is_blind,
            )
            return res_obj, real_duration
        finally:
            await self._cleanup_stream_resources(proc, stderr_task, workers, http_input_task)
