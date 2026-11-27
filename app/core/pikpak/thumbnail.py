"""PikPak 发布封面链路 (自 pikpak_publisher_service 下沉; 传输引擎与相册引擎共用)。

红线: 直链只存内存、只传 Provider, 禁止写日志/DB/WS; 直链仅校验 scheme http/https ——
SSRF 边界与冻结 Provider 口径一致。
"""

import asyncio
import inspect
import logging
import math
import os
import time
import weakref
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

from app.config import settings
from app.core.pikpak.expander import ParsedFileItem
from app.core.thumbnail_image import (
    THUMBNAIL_HARD_MAX_BYTES,
    THUMBNAIL_MAX_DIM,
    THUMBNAIL_PASSTHROUGH_MAX_BYTES,
    is_near_black_image,
    load_normalized_placeholder_poster,
    make_named_thumbnail_stream,
    normalize_thumbnail_bytes,
)

logger = logging.getLogger(__name__)

# 封面内存下载超时
THUMBNAIL_TIMEOUT_S = 10.0

# 封面响应体硬上限 (CWE-400 防御: 防止异常大响应耗尽内存)
THUMBNAIL_MAX_BYTES = 8 * 1024 * 1024  # 8 MiB

# FFmpeg 抽帧硬超时 (原 5.0s; seek 位置后移 + 远程直链网络抖动余量, PLAYBACK_QUALITY 包2)
FFMPEG_THUMB_TIMEOUT_S = 12.0

# 单项封面生成总预算与阶段预算 (单调时钟, 含跨任务排队与重试)
THUMBNAIL_TOTAL_BUDGET_S = 12.0
LIST_IMAGE_STAGE_BUDGET_S = 2.5
LOOKUP_STAGE_BUDGET_S = 2.0
LOOKUP_IMAGE_STAGE_BUDGET_S = 2.5

REMOTE_THUMB_CONCURRENCY = 4
FFMPEG_THUMB_CONCURRENCY = 2
FFMPEG_MAX_STDOUT_BYTES = THUMBNAIL_HARD_MAX_BYTES
_FFMPEG_CLEANUP_RESERVE_S = 0.3

_REMOTE_SEMAPHORES: weakref.WeakValueDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore] = (
    weakref.WeakValueDictionary()
)
_FFMPEG_SEMAPHORES: weakref.WeakValueDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore] = (
    weakref.WeakValueDictionary()
)
_REMOTE_REQUESTS: set[asyncio.Task[str | None]] = set()

_FFMPEG_SCALE_FILTER = (
    "scale="
    "w='max(2,trunc(if(gte(iw*sar,ih),min(320,iw*sar),min(320,ih)*iw*sar/ih)/2)*2)':"
    "h='max(2,trunc(if(gte(iw*sar,ih),min(320,iw*sar)*ih/(iw*sar),min(320,ih))/2)*2)',"
    "setsar=1"
)


def _get_remote_semaphore() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    sem = _REMOTE_SEMAPHORES.get(loop)
    if sem is None:
        sem = asyncio.Semaphore(REMOTE_THUMB_CONCURRENCY)
        _REMOTE_SEMAPHORES[loop] = sem
    return sem


def _get_ffmpeg_semaphore() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    sem = _FFMPEG_SEMAPHORES.get(loop)
    if sem is None:
        sem = asyncio.Semaphore(FFMPEG_THUMB_CONCURRENCY)
        _FFMPEG_SEMAPHORES[loop] = sem
    return sem


@asynccontextmanager
async def _acquire_bounded(sem: asyncio.Semaphore | asyncio.Lock, timeout_s: float) -> AsyncIterator[bool]:
    if timeout_s <= 0:
        yield False
        return
    try:
        await asyncio.wait_for(sem.acquire(), timeout=timeout_s)
    except TimeoutError:
        yield False
        return
    try:
        yield True
    finally:
        sem.release()


def _is_http_url(url: Any) -> bool:
    return isinstance(url, str) and url.lower().startswith(("http://", "https://"))


def _remaining_time(deadline: float) -> float:
    return max(0.0, deadline - time.monotonic())


def _parse_positive_duration(duration_s: Any) -> float | None:
    if duration_s is None:
        return None
    try:
        d = float(duration_s)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(d) or d <= 0:
        return None
    return d


def compute_thumb_seek_sec(duration_s: Any) -> float:
    """封面抽帧位置 (秒): clamp(0.1×时长, 3.0, 30.0); 已知短时长按安全余量收口; 未知/非法 → 5.0。"""
    d = _parse_positive_duration(duration_s)
    if d is None:
        return 5.0
    base = min(30.0, max(3.0, 0.1 * d))
    if d < 4.0:
        safe_cap = max(0.0, d - max(0.5, min(1.0, 0.25 * d)))
        return round(min(base, max(0.0, 0.5 * d), safe_cap), 3)
    return base


def _compute_alt_seek_sec(primary_seek: float, duration_s: Any) -> float | None:
    d = _parse_positive_duration(duration_s)
    if d is not None:
        safe_cap = max(0.0, d - max(0.5, min(1.0, 0.25 * d)))
        cand = min(safe_cap, 45.0, max(primary_seek + 3.0, 0.35 * d))
        if abs(cand - primary_seek) >= 0.5:
            return round(cand, 3)
        cand2 = min(safe_cap, 0.7 * d)
        if abs(cand2 - primary_seek) >= 0.5:
            return round(cand2, 3)
        return 0.0 if primary_seek >= 0.5 else None
    if primary_seek < 10.0:
        return round(min(15.0, primary_seek + 7.0), 3)
    return 1.0


async def _read_http_image_bytes(resp: Any, file_id: str) -> bytes | None:
    status = getattr(resp, "status", 200)
    if status is not None and not (200 <= int(status) < 300):
        logger.info("封面 HTTP 状态异常, 跳过封面: file_id=%s status=%s", file_id, status)
        return None
    content_type = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    if not content_type.startswith("image/"):
        logger.info("封面 Content-Type 非 image/*, 跳过封面: file_id=%s ct=%s", file_id, content_type)
        return None
    buf = bytearray()
    async for chunk in resp.content.iter_chunked(64 * 1024):
        if len(buf) + len(chunk) > THUMBNAIL_MAX_BYTES:
            logger.info("封面超过大小上限 (%d bytes), 跳过封面: file_id=%s", THUMBNAIL_MAX_BYTES, file_id)
            return None
        buf.extend(chunk)
    return bytes(buf)


async def _fetch_thumbnail_url(
    url: str,
    file_id: str,
    *,
    deadline: float,
    stage_budget_s: float,
) -> bytes | None:
    """下载并规范化远程封面字节; 严格校验 HTTP(S)、状态码、Content-Type 与大小上限, 严禁日志含 URL。"""
    stage_deadline = min(deadline, time.monotonic() + stage_budget_s)
    wait_budget = _remaining_time(stage_deadline) if _is_http_url(url) else 0.0
    if wait_budget <= 0:
        return None
    try:
        async with _acquire_bounded(_get_remote_semaphore(), wait_budget) as acquired:
            req_budget = _remaining_time(stage_deadline) if acquired else 0.0
            if req_budget <= 0:
                if not acquired:
                    logger.debug("封面下载排队超时 (跳过): file_id=%s", file_id)
                return None

            from app.core.cdn_transport import cdn_liveness_timeout, get_cdn_session

            async with asyncio.timeout(req_budget):
                session = await get_cdn_session()
                async with session.get(str(url), timeout=cdn_liveness_timeout(req_budget)) as resp:
                    raw = await _read_http_image_bytes(resp, file_id)
            if not raw:
                return None
            normalized = normalize_thumbnail_bytes(raw, max_input_bytes=THUMBNAIL_MAX_BYTES)
            if normalized is None:
                logger.debug("封面图片解码/规范化失败 (跳过): file_id=%s", file_id)
            return normalized
    except TimeoutError:
        logger.debug("封面下载超时 (跳过): file_id=%s", file_id)
        return None
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.debug("封面下载失败 (跳过不报错): file_id=%s err_type=%s", file_id, type(exc).__name__)
        return None


async def fetch_thumbnail(
    item: ParsedFileItem,
    *,
    deadline: float | None = None,
    stage_budget_s: float = THUMBNAIL_TIMEOUT_S,
) -> bytes | None:
    """内存下载封面字节 (不落盘): 校验 http/https、2xx、image/*、体积上限与图片合法性; 直链不进日志。"""
    link = item.thumbnail_link
    if not _is_http_url(link):
        return None
    effective_deadline = deadline if deadline is not None else (time.monotonic() + max(0.1, float(stage_budget_s)))
    return await _fetch_thumbnail_url(
        str(link),
        item.file_id,
        deadline=effective_deadline,
        stage_budget_s=stage_budget_s,
    )


def _build_ffmpeg_cmd(input_source: str, seek_sec: float, *, remote: bool) -> list[str]:
    return [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-ss",
        f"{max(0.0, float(seek_sec)):.3f}",
        "-an",
        "-sn",
        "-dn",
        "-probesize",
        "1048576",
        "-analyzeduration",
        "1000000",
        "-protocol_whitelist",
        "http,https,tcp,tls,httpproxy" if remote else "file,pipe",
        "-threads",
        "1",
        "-i",
        input_source,
        "-map",
        "0:V:0",
        "-frames:v",
        "1",
        "-threads",
        "1",
        "-vf",
        _FFMPEG_SCALE_FILTER,
        "-c:v",
        "mjpeg",
        "-q:v",
        "4",
        "-fs",
        str(FFMPEG_MAX_STDOUT_BYTES),
        "-f",
        "mjpeg",
        "pipe:1",
    ]


async def _kill_and_drain_proc(proc: asyncio.subprocess.Process | None, label: str, deadline: float) -> None:
    if proc is None:
        return
    try:
        if proc.returncode is None:
            with suppress(ProcessLookupError):
                proc.kill()
        async with asyncio.timeout(max(0.01, _remaining_time(deadline))):
            await proc.communicate()
    except Exception as exc:
        logger.debug("回收 FFmpeg 进程失败: label=%s err_type=%s", label, type(exc).__name__)


async def _execute_ffmpeg_subprocess(cmd: list[str], label: str, deadline: float) -> bytes | None:
    proc: asyncio.subprocess.Process | None = None
    remaining = _remaining_time(deadline)
    reserve = min(_FFMPEG_CLEANUP_RESERVE_S, remaining / 2)
    try:
        async with asyncio.timeout(remaining - reserve):
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            stdout_data, _ = await proc.communicate()
        if proc.returncode != 0 or not stdout_data:
            logger.debug("FFmpeg 抽帧未返回有效数据 (rc=%s): label=%s", proc.returncode, label)
            return None
        return normalize_thumbnail_bytes(stdout_data, max_input_bytes=FFMPEG_MAX_STDOUT_BYTES)
    except asyncio.CancelledError:
        await _kill_and_drain_proc(proc, label, min(deadline, time.monotonic() + _FFMPEG_CLEANUP_RESERVE_S))
        raise
    except Exception as exc:
        logger.debug("FFmpeg 抽帧失败 (使用备选封面): label=%s err_type=%s", label, type(exc).__name__)
        await _kill_and_drain_proc(proc, label, deadline)
        return None


async def _run_ffmpeg_once(
    input_source: str,
    label: str,
    seek_sec: float,
    *,
    deadline: float,
) -> bytes | None:
    wait_budget = _remaining_time(deadline)
    if wait_budget <= 0:
        return None
    async with _acquire_bounded(_get_ffmpeg_semaphore(), wait_budget) as acquired:
        timeout_s = _remaining_time(deadline) if acquired else 0.0
        if timeout_s <= 0:
            if not acquired:
                logger.debug("FFmpeg 抽帧排队超时 (跳过): label=%s", label)
            return None
        cmd = _build_ffmpeg_cmd(input_source, seek_sec, remote=_is_http_url(input_source))
        return await _execute_ffmpeg_subprocess(cmd, label, deadline)


async def _extract_ffmpeg_with_fallback(
    input_source: str,
    label: str,
    *,
    seek_sec: float,
    duration_s: Any,
    deadline: float,
) -> bytes | None:
    d = _parse_positive_duration(duration_s)
    primary_seek = max(0.0, float(seek_sec))
    if d is not None and primary_seek >= d:
        primary_seek = compute_thumb_seek_sec(d)

    frame = await _run_ffmpeg_once(input_source, label, primary_seek, deadline=deadline)
    tried_start = frame is None
    if tried_start:
        if primary_seek <= 0.25 or _remaining_time(deadline) <= 0.5:
            return None
        frame = await _run_ffmpeg_once(input_source, label, 0.0, deadline=deadline)
    if frame is None or not is_near_black_image(frame):
        return frame

    alt_seek = _compute_alt_seek_sec(primary_seek, d)
    if tried_start and (d is None or alt_seek in (None, 0.0)):
        # The first seek may have passed EOF. Try between the two attempted positions.
        alt_seek = round(min(0.5, primary_seek / 2.0), 3)
    if alt_seek is None or _remaining_time(deadline) <= 0.5:
        return None
    alt_frame = await _run_ffmpeg_once(input_source, label, alt_seek, deadline=deadline)
    return alt_frame if alt_frame is not None and not is_near_black_image(alt_frame) else None


async def extract_frame_ffmpeg(
    direct_url: str,
    file_id: str,
    seek_sec: float = 5.0,
    *,
    duration_s: Any = None,
    deadline: float | None = None,
) -> bytes | None:
    """利用本地 FFmpeg 通过 HTTP Range 从 PikPak 免落盘直链抽帧并规范化。"""
    if not _is_http_url(direct_url):
        return None
    effective_deadline = deadline if deadline is not None else (time.monotonic() + FFMPEG_THUMB_TIMEOUT_S)
    return await _extract_ffmpeg_with_fallback(
        str(direct_url),
        file_id,
        seek_sec=seek_sec,
        duration_s=duration_s,
        deadline=effective_deadline,
    )


async def get_local_thumbnail(
    path: str | os.PathLike[str],
    duration_s: Any = None,
    *,
    deadline_s: float = THUMBNAIL_TOTAL_BUDGET_S,
) -> bytes | None:
    """从本地视频文件抽帧并规范化封面; 共享 FFmpeg 并发信号量与单调截止时间, 失败降级静态海报。"""
    deadline = time.monotonic() + max(0.1, float(deadline_s))
    local_path = os.fspath(path)
    try:
        is_file = await asyncio.wait_for(
            asyncio.to_thread(os.path.isfile, local_path),
            timeout=_remaining_time(deadline),
        )
    except TimeoutError:
        return load_normalized_placeholder_poster()
    if is_file:
        seek_sec = compute_thumb_seek_sec(duration_s)
        frame = await _extract_ffmpeg_with_fallback(
            local_path,
            Path(local_path).name,
            seek_sec=seek_sec,
            duration_s=duration_s,
            deadline=deadline,
        )
        if frame:
            return frame
    return load_normalized_placeholder_poster()


async def _call_remote_provider(
    provider: Callable[[], Any],
    label: str,
    *,
    deadline: float,
) -> str | None:
    budget = min(LOOKUP_STAGE_BUDGET_S, _remaining_time(deadline))
    if budget <= 0:
        return None

    async def _request() -> str | None:
        # On timeout, the CLI may still need to kill its process tree. Keep the slot
        # until cleanup completes, while allowing the thumbnail workflow to proceed.
        async with _get_remote_semaphore():
            try:
                result = provider()
                link = await result if inspect.isawaitable(result) else result
                return link if _is_http_url(link) else None
            except Exception as exc:
                logger.debug("%s失败 (跳过): err_type=%s", label, type(exc).__name__)
                return None

    request = asyncio.create_task(_request())
    _REMOTE_REQUESTS.add(request)
    request.add_done_callback(_REMOTE_REQUESTS.discard)
    try:
        async with asyncio.timeout(budget):
            return await asyncio.shield(request)
    except TimeoutError:
        logger.debug("%s超时 (跳过)", label)
        return None
    finally:
        if not request.done():
            request.cancel()


async def _try_ffmpeg_via_url_provider(
    item: ParsedFileItem,
    url_provider: Callable[[], Any],
    *,
    seek_sec: float,
    deadline: float,
) -> bytes | None:
    direct_url = await _call_remote_provider(url_provider, "获取封面抽帧直链", deadline=deadline)
    if not direct_url or _remaining_time(deadline) <= 0:
        return None
    return await extract_frame_ffmpeg(
        direct_url,
        item.file_id,
        seek_sec=seek_sec,
        duration_s=item.duration_s,
        deadline=deadline,
    )


async def _resolve_video_fallback(
    item: ParsedFileItem,
    *,
    seek_sec: float,
    url_provider: Callable[[], Any] | None,
    thumbnail_link_provider: Callable[[str], Awaitable[str | None]] | None,
    deadline: float,
) -> bytes | None:
    if thumbnail_link_provider is not None and _remaining_time(deadline) > 0:
        looked_up = await _call_remote_provider(
            lambda: thumbnail_link_provider(item.file_id), "补查官方封面链接", deadline=deadline
        )
        if looked_up and looked_up != item.thumbnail_link and _remaining_time(deadline) > 0:
            looked_up_thumb = await _fetch_thumbnail_url(
                looked_up,
                item.file_id,
                deadline=deadline,
                stage_budget_s=LOOKUP_IMAGE_STAGE_BUDGET_S,
            )
            if looked_up_thumb:
                return looked_up_thumb

    if url_provider is not None:
        frame = await _try_ffmpeg_via_url_provider(item, url_provider, seek_sec=seek_sec, deadline=deadline)
        if frame:
            return frame

    return load_normalized_placeholder_poster()


def make_thumb_provider(
    item: ParsedFileItem,
    url_provider: Callable[[], Any] | None = None,
    *,
    thumbnail_link_provider: Callable[[str], Awaitable[str | None]] | None = None,
    provided_bytes: bytes | bytearray | memoryview | None = None,
    deadline_s: float = THUMBNAIL_TOTAL_BUDGET_S,
) -> Callable[[], Awaitable[bytes | None]]:
    """封面 Provider 工厂 (单调总预算 + 单生命周期结果记忆; 取消不记忆可重试)。

    视频链路: 有效 provided_bytes → 列表 thumbnail_link → 单次注入 thumbnail_link_provider
    → 其图片 → FFmpeg 抽帧 → 规范化内置 placeholder_poster.jpg。
    非视频仅尝试 provided_bytes 与列表 thumbnail_link, 缺失返回 None。
    """
    seek_sec = compute_thumb_seek_sec(getattr(item, "duration_s", None))
    ext = os.path.splitext(item.name or item.file_id)[1].lower()
    is_video = (
        not item.is_dir
        and ext not in settings.AUDIO_EXTENSIONS
        and ext not in settings.IMAGE_EXTENSIONS
        and (ext in settings.VIDEO_EXTENSIONS or bool(item.duration_s))
    )
    memoized = False
    memoized_value: bytes | None = None
    lock: asyncio.Lock | None = None

    async def _resolve(deadline: float) -> bytes | None:
        if provided_bytes is not None:
            normalized_provided = normalize_thumbnail_bytes(provided_bytes, max_input_bytes=THUMBNAIL_MAX_BYTES)
            if normalized_provided:
                return normalized_provided
        if _remaining_time(deadline) > 0:
            list_thumb = await fetch_thumbnail(item, deadline=deadline, stage_budget_s=LIST_IMAGE_STAGE_BUDGET_S)
            if list_thumb:
                return list_thumb
        if not is_video:
            return None
        return await _resolve_video_fallback(
            item,
            seek_sec=seek_sec,
            url_provider=url_provider,
            thumbnail_link_provider=thumbnail_link_provider,
            deadline=deadline,
        )

    async def _provide() -> bytes | None:
        nonlocal memoized, memoized_value, lock
        if memoized:
            return memoized_value
        if lock is None:
            lock = asyncio.Lock()
        deadline = time.monotonic() + max(0.0, deadline_s)
        async with _acquire_bounded(lock, _remaining_time(deadline)) as acquired:
            if not acquired:
                return load_normalized_placeholder_poster() if is_video else None
            if memoized:
                return memoized_value
            result = await _resolve(deadline)
            memoized_value = result
            memoized = True
            return result

    return _provide


__all__ = [
    "FFMPEG_MAX_STDOUT_BYTES",
    "FFMPEG_THUMB_CONCURRENCY",
    "FFMPEG_THUMB_TIMEOUT_S",
    "LIST_IMAGE_STAGE_BUDGET_S",
    "LOOKUP_IMAGE_STAGE_BUDGET_S",
    "LOOKUP_STAGE_BUDGET_S",
    "REMOTE_THUMB_CONCURRENCY",
    "THUMBNAIL_HARD_MAX_BYTES",
    "THUMBNAIL_MAX_BYTES",
    "THUMBNAIL_MAX_DIM",
    "THUMBNAIL_PASSTHROUGH_MAX_BYTES",
    "THUMBNAIL_TIMEOUT_S",
    "THUMBNAIL_TOTAL_BUDGET_S",
    "compute_thumb_seek_sec",
    "extract_frame_ffmpeg",
    "fetch_thumbnail",
    "get_local_thumbnail",
    "is_near_black_image",
    "load_normalized_placeholder_poster",
    "make_named_thumbnail_stream",
    "make_thumb_provider",
    "normalize_thumbnail_bytes",
]
