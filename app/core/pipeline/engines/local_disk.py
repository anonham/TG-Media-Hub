"""统一发布流水线传输引擎 - 本地磁盘传输引擎"""

import asyncio
import logging
import os
import shutil
import tempfile
from collections.abc import Callable
from concurrent.futures import Future
from typing import Any

from telethon.errors import FloodWaitError

from app.config import settings
from app.core import fast_telethon as ft_mod
from app.core import fmp4_pipe_uploader as fmp4_mod
from app.core.media_metadata import (
    ProbedMediaMetadata,
    apply_probed_metadata,
    merge_probed_metadata,
    needs_metadata_probe,
    probe_local_video_metadata,
)
from app.core.pikpak.thumbnail import get_local_thumbnail, make_named_thumbnail_stream
from app.core.pipeline.engines._common import (
    _LOCAL_MEDIA_META_CACHE,
    _LOCAL_MEDIA_META_CACHE_MAX_ENTRIES,
    _raise_if_control_signal,
    _safe_invoke_engine_progress,
)
from app.core.pipeline.engines._remux import (
    build_remux_video_attributes,
    decide_mpegts_remux,
    is_mpegts_remux_candidate,
    remux_output_filename,
)
from app.core.pipeline.engines._types import SendFileKwargs
from app.core.pipeline.engines.exceptions import TransportError
from app.core.pipeline.interfaces import (
    DEFAULT_CLIENT_REGISTRY,
    DEFAULT_TASK_STORE,
    ClientRegistryPort,
    MediaTransportEngine,
    TaskStorePort,
)
from app.core.pipeline.models import PublishContext, PublishItem, TransportOutcome
from app.core.pipeline.utils import (
    UploadSpeedTracker,
    build_media_attributes,
    is_file_parts_invalid,
    is_image_item,
    safe_invoke_progress,
)
from app.core.scheduler import RetryableTaskError, UploadCancelledError, UploadPausedError

logger = logging.getLogger(__name__)


_ALBUM_PHOTO_MAX_DIM = 2560
_ALBUM_PHOTO_MAX_BYTES = 10_000_000


def _discard_album_temp_file(temp_copy_path: str | None) -> None:
    if temp_copy_path is None:
        return
    try:
        os.remove(temp_copy_path)
    except OSError as rm_err:
        logger.debug("清理相册缩放临时文件异常: %s", rm_err)
    try:
        ft_mod.discard_upload_meta(temp_copy_path)
    except Exception as meta_err:
        logger.debug("清理相册缩放临时 meta 异常: %s", meta_err)


def _cleanup_worker_temp(result: Future[str | None]) -> None:
    if result.cancelled() or result.exception() is not None:
        return
    _discard_album_temp_file(result.result())


async def _prepare_image_cancelled_safe(file_path: str) -> tuple[str, str | None]:
    """取消安全的静态图片尺寸/体积归一化: 在线程中执行 _prepare_album_image_sync 并确保取消时清理临时文件."""
    worker_temp: Future[str | None] = Future()

    def prepare_owned() -> tuple[str, str | None]:
        result = _prepare_album_image_sync(file_path)
        worker_temp.set_result(result[1])
        return result

    preparation = asyncio.create_task(asyncio.to_thread(prepare_owned))
    cancelled = False
    while True:
        try:
            upload_path, temp_copy_path = await asyncio.shield(preparation)
            break
        except asyncio.CancelledError:
            if preparation.cancelled():
                worker_temp.add_done_callback(_cleanup_worker_temp)
                raise
            cancelled = True
        except Exception as prep_err:
            if cancelled:
                raise asyncio.CancelledError from prep_err
            raise
    if cancelled:
        _discard_album_temp_file(temp_copy_path)
        raise asyncio.CancelledError
    return upload_path, temp_copy_path


def _is_normalizable_image(file_path: str, item: PublishItem) -> bool:
    """判定是否为静态图片 (排除 GIF 动图, 防破坏动画)."""
    ext = os.path.splitext(file_path)[1].lower()
    return (ext in settings.IMAGE_EXTENSIONS and ext != ".gif") or is_image_item(item)


def _stat_size(file_path: str) -> int:
    """文件体积探测。"""
    return os.path.getsize(file_path) if os.path.exists(file_path) else 0


def _validate_local_file_sync(file_path: str, *, need_size: bool = False) -> int:
    """同步校验本地文件不得为符号链接, 按需顺带返回文件体积 (供 asyncio.to_thread 卸载)."""
    if os.path.islink(file_path):
        raise ValueError(f"安全策略拒绝读取符号链接文件: {file_path}")
    return _stat_size(file_path) if need_size else 0


def _prepare_album_image_sync(file_path: str) -> tuple[str, str | None]:
    """预检相册图片不得为符号链接及分辨率/体积；超限时生成临时缩放 JPEG 副本，原文件保持只读不动。"""
    if os.path.islink(file_path):
        raise ValueError(f"安全策略拒绝读取符号链接文件: {file_path}")
    if not os.path.isfile(file_path):
        return file_path, None
    try:
        from PIL import Image, ImageOps
    except ImportError:
        return file_path, None

    tmp_path: str | None = None
    try:
        file_size = os.path.getsize(file_path)
        with Image.open(file_path) as raw_img:
            raw_img.load()
            img = ImageOps.exif_transpose(raw_img)
            if (
                img.width <= _ALBUM_PHOTO_MAX_DIM
                and img.height <= _ALBUM_PHOTO_MAX_DIM
                and file_size <= _ALBUM_PHOTO_MAX_BYTES
            ):
                return file_path, None

            exif_bytes = img.info.get("exif")
            img.thumbnail((_ALBUM_PHOTO_MAX_DIM, _ALBUM_PHOTO_MAX_DIM), Image.Resampling.LANCZOS)
            if img.mode != "RGB":
                if img.has_transparency_data:
                    rgba = img if img.mode == "RGBA" else img.convert("RGBA")
                    bg = Image.new("RGB", rgba.size, (255, 255, 255))
                    bg.paste(rgba, mask=rgba.getchannel("A"))
                    result = bg
                else:
                    result = img.convert("RGB")
            else:
                result = img

            with tempfile.NamedTemporaryFile(suffix=".jpg", prefix="tg_album_norm_", delete=False) as tmp:
                tmp_path = tmp.name
            save_kwargs: dict[str, Any] = {"format": "JPEG", "quality": 92, "progressive": True}
            if exif_bytes:
                save_kwargs["exif"] = exif_bytes
            result.save(tmp_path, **save_kwargs)
            return tmp_path, tmp_path
    except Exception as img_err:
        if tmp_path is not None:
            try:
                os.remove(tmp_path)
            except OSError as rm_err:
                logger.debug("清理失败的相册缩放临时文件异常: %s", rm_err)
        logger.debug("相册图片预检跳过 (沿用原文件): %s (%s)", file_path, img_err)
        return file_path, None


class LocalDiskTransportEngine(MediaTransportEngine):
    """本地磁盘媒体传输策略引擎。

    封装本地相册打包发送 (send_album) 与单媒体文件并行上传 (send_item)。
    - 大图/视频/音频：按体积阈值路由 Userbot / Bot Pool；
    - Userbot 模式调用 upload_file_parallel 进行并发加速分块上传；
    - 支持断点缓存失效 (FILE_PARTS_INVALID) 自动清理 meta 并全量重试;
    - 支持取消 (UploadCancelledError) 与暂停 (UploadPausedError) 控制信号穿透；
    - 支持实时上传速率测量；失败异常附带 client_label。

    账号池/持久层依赖端口化 —— 构造注入 ClientRegistryPort/TaskStorePort,
    默认参数 = 现状单例适配器 (调用期经模块属性读取, 既有测试 patch 路径保持穿透)。
    """

    def __init__(
        self,
        *,
        registry: ClientRegistryPort | None = None,
        store: TaskStorePort | None = None,
    ) -> None:
        self._registry = registry if registry is not None else DEFAULT_CLIENT_REGISTRY
        self._store = store if store is not None else DEFAULT_TASK_STORE
        self._last_thumb: tuple[PublishItem, bytes | None] | None = None

    async def _resolve_local_video_thumb(self, item: PublishItem, context: PublishContext) -> bytes | None:
        """本地视频封面抽帧与引擎内单次记忆 (edit→send 降级与重试复用, 取消/暂停不记忆)。"""
        _raise_if_control_signal(context)
        if self._last_thumb is not None and self._last_thumb[0] is item:
            return self._last_thumb[1]

        thumb_bytes: bytes | None = None
        try:
            thumb_bytes = await get_local_thumbnail(item.item_id, item.duration_s)
        except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
            raise
        except Exception as th_err:
            logger.debug("本地视频封面生成失败 (非致命): %s", type(th_err).__name__)
            thumb_bytes = None

        _raise_if_control_signal(context)
        self._last_thumb = (item, thumb_bytes)
        return thumb_bytes

    async def _ensure_local_video_metadata(self, item: PublishItem, *, require_codecs: bool = False) -> None:
        """本地视频消息属性探测补齐 (PLAYBACK_QUALITY ; 探测/回填拆解)。

        覆盖全部受支持视频扩展 (MP4/M4V/MOV/MKV/WebM/AVI/WMV/FLV/TS/M2TS/MTS):
        优先有界纯 Python 解析, 缺失时长或宽高时走有界本地 ffprobe 补齐。
        探测值 > 0 才回填 (E4)——路径不存在/探测失败返回空结果时不改写字段;
        异常 logger.debug 静默降级, 绝不阻断发布。
        结果按 (path, size, mtime) 模块级缓存, edit_item 复用零重复 IO。
        """
        ext = os.path.splitext(item.item_id)[1].lower()
        if ext not in settings.VIDEO_EXTENSIONS:
            return
        try:
            stat_res = await asyncio.to_thread(os.stat, item.item_id)
        except OSError:
            return  # 路径不存在 (含测试假路径): 保持退化属性 (现状语义)
        cache_key = (item.item_id, int(stat_res.st_size), stat_res.st_mtime)
        cached = _LOCAL_MEDIA_META_CACHE.get(cache_key)
        if cached is not None:
            _LOCAL_MEDIA_META_CACHE.move_to_end(cache_key)  # LRU 命中置新
            apply_probed_metadata(item, cached)
            if not needs_metadata_probe(item, require_codecs=require_codecs):
                return
        try:
            probed = await probe_local_video_metadata(item.item_id, ext)
        except Exception as probe_err:
            logger.debug("本地视频属性探测失败 (非致命, 属性保持现状): %s %s", item.item_id, probe_err)
            return
        if probed == ProbedMediaMetadata():
            return
        # Another caller may have completed the same probe while this request was waiting.
        current = _LOCAL_MEDIA_META_CACHE.get(cache_key)
        if current is not None:
            probed = merge_probed_metadata(current, probed)
        elif len(_LOCAL_MEDIA_META_CACHE) >= _LOCAL_MEDIA_META_CACHE_MAX_ENTRIES:
            _LOCAL_MEDIA_META_CACHE.popitem(last=False)
        _LOCAL_MEDIA_META_CACHE[cache_key] = probed
        _LOCAL_MEDIA_META_CACHE.move_to_end(cache_key)
        apply_probed_metadata(item, probed)

    def _resolve_send_client(self, context: PublishContext, size_bytes: int) -> tuple[Any, str]:
        """客户端路由 (Userbot / Bot Pool): 超阈值或无 Bot 池 → Userbot, 否则 Bot 轮询。"""
        registry = self._registry
        threshold = context.bot_routing_threshold_bytes or (settings.USERBOT_UPLOAD_THRESHOLD_MB * 1024 * 1024)
        if size_bytes > threshold or not registry.bots:
            return context.userbot_client or registry.userbot, context.account_display or "Userbot"
        bot = registry.get_next_available_bot()
        if bot is None:
            return context.userbot_client or registry.userbot, context.account_display or "Userbot"
        return bot, f"Bot Pool #{registry.bot_index}"

    def _make_ft_progress_hook(self, context: PublishContext, progress: dict[str, Any]) -> Callable[..., None]:
        """FastTelethon 并发上传进度钩子 (file_size 键见 progress 字典)。"""
        file_name = progress["file_name"]
        file_size = progress["file_size"]

        def ft_progress_hook(
            current: int,
            total: int,
            speed: float,
            eta: float,
            workers: int,
            dc_id: int,
            workers_total: int = 0,
        ) -> None:
            _raise_if_control_signal(context)
            progress["last_speed"] = speed
            if context.progress_callback:
                try:
                    cur_done = context.completed_files_count
                    tot_files = context.total_files_count or 1
                    overall_bytes_done = context.completed_bytes
                    overall_bytes_tot = context.total_bytes_sum
                    concurrency_val = int(workers) if workers else 1
                    file_fraction = (current / total) if total > 0 else 0.0
                    overall_percent = ((cur_done + file_fraction) / tot_files * 100) if tot_files > 0 else 0.0
                    cur_item = min(tot_files, cur_done + 1)
                    safe_invoke_progress(
                        context.progress_callback,
                        cur_item,
                        tot_files,
                        speed,
                        eta,
                        file_name,
                        concurrency=concurrency_val,
                        dc_id=dc_id,
                        concurrency_total=workers_total or context.upload_concurrency,
                        percent=overall_percent,
                        total_bytes_done=overall_bytes_done + current,
                        total_bytes_all=overall_bytes_tot or total or file_size,
                    )
                except Exception as e:
                    logger.debug("进度回调异常: %s", e)

        return ft_progress_hook

    def _build_local_send_kwargs(
        self,
        progress: dict[str, Any],
        media_attributes: list[Any],
        is_video: bool,
        *,
        thumb_bytes: bytes | None = None,
    ) -> SendFileKwargs:
        """本地单发 send_file 参数构建 (caption=文件名 + 流媒体标记 + 属性 + 独立封面流)。"""
        send_kwargs: SendFileKwargs = {
            "caption": progress["file_name"],
            "supports_streaming": is_video,
        }
        if media_attributes:
            send_kwargs["attributes"] = media_attributes
        ext = os.path.splitext(progress.get("file_name", ""))[1].lower()
        if ext in (".ts", ".m2ts", ".mts"):
            send_kwargs["mime_type"] = "video/mp2t"
        elif ext == ".m4v":
            send_kwargs["mime_type"] = "video/mp4"
        if is_video and thumb_bytes:
            send_kwargs["thumb"] = make_named_thumbnail_stream(thumb_bytes)
        return send_kwargs

    async def _send_via_userbot(
        self,
        client: Any,
        context: PublishContext,
        *,
        item: PublishItem,
        progress: dict[str, Any],
        media_attributes: list[Any],
        is_video: bool,
        mode_str: str,
        thumb_bytes: bytes | None = None,
    ) -> TransportOutcome:
        """Userbot 通道: FastTelethon 并行上传 (2 次尝试, FILE_PARTS_INVALID 自愈) + 发送。"""
        file_path = progress["file_path"]
        file_name = progress["file_name"]
        ft_progress_hook = self._make_ft_progress_hook(context, progress)

        sent_msg = None
        for attempt in range(2):
            try:
                uploaded_media = await ft_mod.upload_file_parallel(
                    client,
                    file_path,
                    progress_callback=ft_progress_hook,
                    concurrency=context.upload_concurrency or 2,
                    get_concurrency=context.get_current_upload_concurrency,
                    in_flight_per_connection=context.upload_in_flight_per_connection or 2,
                    get_in_flight_per_connection=context.get_current_in_flight,
                    is_cancelled=context.is_cancelled,
                    is_paused=context.is_paused,
                    force_fresh=(attempt > 0),
                )
                _raise_if_control_signal(context)
                send_kwargs = self._build_local_send_kwargs(
                    progress,
                    media_attributes,
                    is_video,
                    thumb_bytes=thumb_bytes,
                )
                sent_msg = await client.send_file(context.target_chat_id, uploaded_media, **send_kwargs)
                break
            except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
                raise
            except Exception as upload_err:
                if is_file_parts_invalid(upload_err) and attempt == 0:
                    # 重试前落任务日志 (还原旧基线可行动文案, 旧实现仅 logger);
                    # meta 清理收口 discard_upload_meta (新键 + 旧 sidecar 双清)
                    retry_warn = (
                        f"⚠️ 发送文件 {file_name} 检测到 Telegram 服务端分块缓存已失效 ({upload_err})，"
                        f"正在清除失效断点并自动全量重新上传..."
                    )
                    logger.warning(retry_warn)
                    if context.task_id:
                        await self._store.append_log(context.task_id, retry_warn)

                    ft_mod.discard_upload_meta(file_path)
                    continue
                raise

        msg_id = getattr(sent_msg, "id", 0)
        return TransportOutcome(
            message_ids=[int(msg_id or 0)], speed_mb_s=progress["last_speed"], client_label=mode_str
        )

    async def _send_via_bot(
        self,
        client: Any,
        context: PublishContext,
        *,
        item: PublishItem,
        progress: dict[str, Any],
        media_attributes: list[Any],
        is_video: bool,
        mode_str: str,
        thumb_bytes: bytes | None = None,
    ) -> TransportOutcome:
        """Bot Pool 通道: Telethon 原生路径直发 (UploadSpeedTracker 测速)。"""
        file_path = progress["file_path"]
        file_size = progress["file_size"]
        tracker = UploadSpeedTracker(file_size)

        def single_progress_hook(current: int, total: int) -> None:
            _raise_if_control_signal(context)
            tracker.update(current)
            progress["last_speed"] = tracker.speed
            if context.progress_callback:
                try:
                    cur_done = context.completed_files_count
                    tot_files = context.total_files_count or 1
                    overall_bytes_done = context.completed_bytes
                    overall_bytes_tot = context.total_bytes_sum
                    file_fraction = (current / total) if total > 0 else 0.0
                    overall_percent = ((cur_done + file_fraction) / tot_files * 100) if tot_files > 0 else 0.0
                    cur_item = min(tot_files, cur_done + 1)
                    safe_invoke_progress(
                        context.progress_callback,
                        cur_item,
                        tot_files,
                        tracker.speed,
                        tracker.eta,
                        progress["file_name"],
                        concurrency=1,
                        dc_id=getattr(client.session, "dc_id", 4) if hasattr(client, "session") else 0,
                        concurrency_total=1,
                        percent=overall_percent,
                        total_bytes_done=overall_bytes_done + current,
                        total_bytes_all=overall_bytes_tot or total or file_size,
                    )
                except Exception as e:
                    logger.debug("进度回调异常: %s", e)

        send_kwargs = self._build_local_send_kwargs(
            progress,
            media_attributes,
            is_video,
            thumb_bytes=thumb_bytes,
        )
        send_kwargs["progress_callback"] = single_progress_hook

        _raise_if_control_signal(context)
        sent_msg = await client.send_file(context.target_chat_id, file_path, **send_kwargs)
        msg_id = getattr(sent_msg, "id", 0)
        return TransportOutcome(
            message_ids=[int(msg_id or 0)], speed_mb_s=progress["last_speed"], client_label=mode_str
        )

    def _make_local_remux_progress_cb(
        self,
        context: PublishContext,
        *,
        file_name: str,
        file_size: int,
        workers_count: int,
        dc_id: int,
        speed_state: dict[str, float],
    ) -> Callable[[int, int, float, float, str, int], None]:
        def _cb(current: int, total: int, speed: float, eta: float, _fname: str, workers: int) -> None:
            _raise_if_control_signal(context)
            speed_state["last_speed"] = speed
            if not context.progress_callback:
                return
            cur_done = context.completed_files_count
            tot_files = context.total_files_count or 1
            est_total = max(file_size, total, current)
            file_fraction = (current / est_total) if est_total > 0 else 0.0
            overall_percent = ((cur_done + file_fraction) / tot_files * 100) if tot_files > 0 else 0.0
            _safe_invoke_engine_progress(
                context,
                min(tot_files, cur_done + 1),
                tot_files,
                speed,
                eta,
                file_name,
                label="本地 Remux 进度回调异常",
                concurrency=int(workers) if workers else workers_count,
                dc_id=dc_id,
                concurrency_total=workers_count,
                percent=overall_percent,
                total_bytes_done=context.completed_bytes + min(current, file_size),
                total_bytes_all=context.total_bytes_sum or est_total,
            )

        return _cb

    async def _deliver_local_remuxed_media(
        self,
        client: Any,
        context: PublishContext,
        *,
        item: PublishItem,
        res_file: Any,
        actual_dur: float,
        uploader: fmp4_mod.ZeroDiskFmp4StreamUploader,
        last_speed: float,
        file_name: str,
        mode_str: str,
        copy_audio: bool,
        thumb_bytes: bytes | None,
        msg_id: int | None,
    ) -> TransportOutcome:
        attrs = build_remux_video_attributes(item, actual_dur, uploader.video_width, uploader.video_height)
        if msg_id is not None:
            edit_kwargs: dict[str, Any] = {
                "file": res_file,
                "text": file_name,
                "attributes": attrs or None,
                "supports_streaming": True,
            }
            if thumb_bytes:
                edit_kwargs["thumb"] = make_named_thumbnail_stream(thumb_bytes)
            sent_msg = await client.edit_message(context.target_chat_id, int(msg_id), **edit_kwargs)
            resolved_id = int(getattr(sent_msg, "id", 0) or int(msg_id))
            return TransportOutcome(message_ids=[resolved_id], speed_mb_s=last_speed, client_label=mode_str)

        send_kwargs: SendFileKwargs = {"caption": file_name, "supports_streaming": True, "mime_type": "video/mp4"}
        if attrs:
            send_kwargs["attributes"] = attrs
        if thumb_bytes:
            send_kwargs["thumb"] = make_named_thumbnail_stream(thumb_bytes)
        sent_msg = await client.send_file(context.target_chat_id, res_file, **send_kwargs)
        sent_id = int(getattr(sent_msg, "id", 0) or 0)
        logger.info(
            "🎬 本地 MPEG-TS 自动重封装为 MP4 发布成功: %s (msg_id=%s, duration=%.2fs, copy_audio=%s)",
            file_name,
            sent_id,
            actual_dur,
            copy_audio,
        )
        return TransportOutcome(message_ids=[sent_id], speed_mb_s=last_speed, client_label=mode_str)

    async def _log_local_remux_warning(self, context: PublishContext, warn_msg: str) -> None:
        logger.warning(warn_msg)
        if context.task_id:
            try:
                await self._store.append_log(context.task_id, f"⚠️ {warn_msg}")
            except Exception as log_err:
                logger.debug("本地 remux 日志写入异常 (非致命): %s", log_err)

    async def _remux_and_upload_local_ts(
        self,
        client: Any,
        context: PublishContext,
        *,
        item: PublishItem,
        file_path: str,
        file_name: str,
        file_size: int,
        mode_str: str,
        is_bot_client: bool,
        thumb_bytes: bytes | None = None,
        msg_id: int | None = None,
    ) -> TransportOutcome | None:
        """本地 .ts/.m2ts/.mts 零落盘重封装为 MP4 并流式上传。

        返回 None 表示降级回原样 .ts 文件直发。
        """
        decision = decide_mpegts_remux(item, ffmpeg_available=bool(shutil.which("ffmpeg")), require_known_vcodec=True)
        if not decision.should_remux:
            await self._log_local_remux_warning(context, f"[{file_name}] {decision.skip_reason}")
            return None

        workers_count = 1 if is_bot_client else max(1, min(int(context.upload_concurrency or 2), 8))
        in_flight_count = 1 if is_bot_client else max(1, min(int(context.upload_in_flight_per_connection or 2), 4))
        max_bytes = context.task_limit_bytes or (
            settings.PREMIUM_MAX_FILE_SIZE_BYTES if context.is_premium else settings.MAX_FILE_SIZE_BYTES
        )
        speed_state = {"last_speed": 0.0}
        progress_cb = self._make_local_remux_progress_cb(
            context,
            file_name=file_name,
            file_size=file_size,
            workers_count=workers_count,
            dc_id=getattr(client.session, "dc_id", 4) if hasattr(client, "session") else 0,
            speed_state=speed_state,
        )
        retried_delivery = False
        while True:
            _raise_if_control_signal(context)
            uploader = fmp4_mod.ZeroDiskFmp4StreamUploader(
                client=client,
                max_workers=workers_count,
                queue_size=max(12, workers_count * in_flight_count * 2),
                in_flight_per_connection=in_flight_count,
            )
            try:
                res_file, actual_dur = await uploader.stream_to_telegram(
                    pikpak_url=file_path,
                    start_sec=0.0,
                    end_sec=float(item.duration_s or 0.0),
                    file_name=remux_output_filename(file_name),
                    copy_audio=decision.copy_audio,
                    part_index=1,
                    max_part_bytes=max_bytes,
                    progress_callback=progress_cb,
                    is_cancelled=context.is_cancelled,
                    is_paused=context.is_paused,
                    full_stream=True,
                    force_single_input=True,
                    pause_yield=True,
                    auto_small_file_mode=True,
                    estimated_output_bytes=file_size,
                    audio_copy_tracks=decision.audio_copy_tracks,
                )
            except (
                UploadCancelledError,
                UploadPausedError,
                asyncio.CancelledError,
                RetryableTaskError,
                FloodWaitError,
            ):
                raise
            except Exception as remux_err:
                await self._log_local_remux_warning(
                    context, f"[{file_name}] 本地 MPEG-TS 零落盘重封装失败 ({remux_err})，降级为原样 .ts 文档发送"
                )
                return None

            _raise_if_control_signal(context)
            try:
                return await self._deliver_local_remuxed_media(
                    client,
                    context,
                    item=item,
                    res_file=res_file,
                    actual_dur=actual_dur,
                    uploader=uploader,
                    last_speed=uploader.last_speed or speed_state["last_speed"],
                    file_name=file_name,
                    mode_str=mode_str,
                    copy_audio=decision.copy_audio,
                    thumb_bytes=thumb_bytes,
                    msg_id=msg_id,
                )
            except Exception as delivery_err:
                if retried_delivery or not is_file_parts_invalid(delivery_err):
                    raise
                retried_delivery = True
                speed_state["last_speed"] = 0.0
                await self._log_local_remux_warning(
                    context, f"[{file_name}] 重封装上传遇 FILE_PARTS_INVALID，重新生成分片并上传一次"
                )

    async def send_item(self, item: PublishItem, context: PublishContext) -> TransportOutcome:
        """本地单文件发送。"""
        _raise_if_control_signal(context)

        file_path = item.item_id
        probed_size = await asyncio.to_thread(_validate_local_file_sync, file_path, need_size=not item.size_bytes)
        file_size = item.size_bytes or probed_size

        ext = os.path.splitext(file_path)[1].lower()
        is_video = ext in settings.VIDEO_EXTENSIONS
        is_image = _is_normalizable_image(file_path, item)
        file_name = item.name or os.path.basename(file_path)
        remux_candidate = is_video and is_mpegts_remux_candidate(file_name, context)
        # PLAYBACK_QUALITY 包3: 本地视频探测补齐 duration/宽高 (假路径/失败静默保持缺省)
        thumb_bytes: bytes | None = None
        if is_video:
            await self._ensure_local_video_metadata(item, require_codecs=remux_candidate)
            thumb_bytes = await self._resolve_local_video_thumb(item, context)
        # 属性构建器收口共享函数 (send/edit 双路径复用; 音频优先判定)。
        # 本地 MP4/MKV 探测回填后 duration_s 可有值; 缺省 None 语义与旧内联副本等价。
        media_attributes = build_media_attributes(item)

        upload_path = file_path
        temp_copy_path: str | None = None
        if is_image:
            upload_path, temp_copy_path = await _prepare_image_cancelled_safe(file_path)

        try:
            effective_size = (
                (await asyncio.to_thread(_stat_size, upload_path) or file_size)
                if temp_copy_path is not None
                else file_size
            )
            client, mode_str = self._resolve_send_client(context, effective_size)
            if client is None:
                raise RuntimeError("未就绪的 TelegramClient: 无可用的 Userbot 或 Bot 客户端")

            is_userbot = client == (context.userbot_client or self._registry.userbot) or not mode_str.startswith(
                "Bot Pool"
            )
            progress: dict[str, Any] = {
                "file_path": upload_path,
                "file_name": file_name,
                "file_size": effective_size,
                "last_speed": 0.0,
            }

            try:
                if remux_candidate and await asyncio.to_thread(os.path.isfile, file_path):
                    remux_outcome = await self._remux_and_upload_local_ts(
                        client,
                        context,
                        item=item,
                        file_path=file_path,
                        file_name=file_name,
                        file_size=effective_size,
                        mode_str=mode_str,
                        is_bot_client=not is_userbot,
                        thumb_bytes=thumb_bytes,
                    )
                    if remux_outcome is not None:
                        return remux_outcome

                if is_userbot:
                    return await self._send_via_userbot(
                        client,
                        context,
                        item=item,
                        progress=progress,
                        media_attributes=media_attributes,
                        is_video=is_video,
                        mode_str=mode_str,
                        thumb_bytes=thumb_bytes,
                    )
                return await self._send_via_bot(
                    client,
                    context,
                    item=item,
                    progress=progress,
                    media_attributes=media_attributes,
                    is_video=is_video,
                    mode_str=mode_str,
                    thumb_bytes=thumb_bytes,
                )
            except (UploadCancelledError, UploadPausedError, asyncio.CancelledError, RetryableTaskError):
                raise
            except FloodWaitError as fw_err:
                wait_sec = getattr(fw_err, "seconds", 5) or 5
                flood_marked = False
                try:
                    self._registry.mark_flood_wait(client, wait_sec)
                    flood_marked = True
                except Exception as reg_err:
                    logger.debug("本地单文件发送标记 FloodWait 状态异常: %s", reg_err)
                raise RetryableTaskError(
                    f"本地单文件发送遇 FloodWait: {fw_err}",
                    retry_after=float(wait_sec),
                    flood_client_marked=flood_marked,
                ) from fw_err
            except Exception as exc:
                raise TransportError(str(exc), item=item, client_label=mode_str, cause=exc) from exc
        finally:
            _discard_album_temp_file(temp_copy_path)

    async def _upload_album_member(
        self,
        client: Any,
        context: PublishContext,
        *,
        idx: int,
        item: PublishItem,
        sub_size: int,
        progress: dict[str, Any],
    ) -> Any:
        """单成员预上传拿 handle (超限图片先预检缩放再统一走 FastTelethon 并发 + 断点续传; 异常原样上抛由调用方逐项隔离)。"""
        upload_path, temp_copy_path = await _prepare_image_cancelled_safe(item.item_id)

        effective_size = sub_size
        tracker = UploadSpeedTracker(effective_size)
        cur_done = progress["cur_done"]
        tot_files = progress["tot_files"]

        # B023 收口: tracker/item 经默认参数绑定当前迭代 (同迭代消费, 显式格子防晚绑定)
        def upload_progress_hook(
            current: int,
            total: int,
            speed: float,
            eta: float,
            workers: int,
            dc_id: int,
            workers_total: int = 0,
        ) -> None:
            _raise_if_control_signal(context)
            tracker.update(current)
            progress["last_speed"] = tracker.speed or speed
            file_fraction = (current / total) if total > 0 else 0.0
            overall_percent = ((cur_done + idx + file_fraction) / tot_files * 100) if tot_files > 0 else 0.0
            cur_item = min(tot_files, cur_done + idx + 1)
            _safe_invoke_engine_progress(
                context,
                cur_item,
                tot_files,
                tracker.speed,
                tracker.eta,
                item.name,
                label="Album 预上传进度回调异常",
                concurrency=1,
                dc_id=dc_id,
                concurrency_total=workers_total or 1,
                percent=overall_percent,
            )

        try:
            if temp_copy_path is not None:
                effective_size = await asyncio.to_thread(_stat_size, upload_path) or sub_size
                tracker = UploadSpeedTracker(effective_size)
            return await ft_mod.upload_file_parallel(
                client,
                upload_path,
                progress_callback=upload_progress_hook,
                concurrency=context.upload_concurrency or 2,
                get_concurrency=context.get_current_upload_concurrency,
                in_flight_per_connection=context.upload_in_flight_per_connection or 2,
                get_in_flight_per_connection=context.get_current_in_flight,
                is_cancelled=context.is_cancelled,
                is_paused=context.is_paused,
            )
        finally:
            _discard_album_temp_file(temp_copy_path)

    async def _send_album_segment(
        self,
        client: Any,
        context: PublishContext,
        items: list[PublishItem],
        *,
        segment: list[tuple[int, Any]],
        outcomes: list[TransportOutcome],
        mode_str: str,
        last_speed: float,
    ) -> None:
        """成功子集段发送 (R3 矩阵 + Telegram 组发 ≤10 防御): 段内 ≥2 组发 / ==1 强制单张;
        发送失败: 该段成员统一记 error_detail outcome (不 raise), 已上传 handle 作废。"""
        try:
            _raise_if_control_signal(context)
            if len(segment) >= 2:
                sent = await client.send_file(
                    context.target_chat_id,
                    [media for _, media in segment],
                    caption=None,
                )
            else:
                idx0, media0 = segment[0]
                sent = await client.send_file(
                    context.target_chat_id,
                    media0,
                    caption=items[idx0].name or os.path.basename(items[idx0].item_id),
                    force_document=False,
                )
        except (UploadCancelledError, UploadPausedError, asyncio.CancelledError, RetryableTaskError):
            raise
        except FloodWaitError as fw_err:
            wait_sec = getattr(fw_err, "seconds", 5) or 5
            flood_marked = False
            try:
                self._registry.mark_flood_wait(client, wait_sec)
                flood_marked = True
            except Exception as reg_err:
                logger.debug("相册组发标记 FloodWait 状态异常: %s", reg_err)
            for idx, _media in segment:
                outcomes[idx] = TransportOutcome(
                    message_ids=[],
                    error_detail=str(fw_err),
                    client_label=mode_str,
                    retryable_error=True,
                    retry_after_s=int(wait_sec),
                    flood_marked=flood_marked,
                )
            return
        except Exception as send_exc:
            for idx, _media in segment:
                outcomes[idx] = TransportOutcome(
                    message_ids=[],
                    error_detail=str(send_exc),
                    client_label=mode_str,
                )
            return
        sent_list = sent if isinstance(sent, list) else [sent]
        for pos, (idx, _media) in enumerate(segment):
            msg_id = int(getattr(sent_list[pos], "id", 0)) if pos < len(sent_list) else 0
            if msg_id:
                outcomes[idx] = TransportOutcome(
                    message_ids=[msg_id],
                    speed_mb_s=last_speed,
                    client_label=mode_str,
                )

    async def send_album(self, items: list[PublishItem], context: PublishContext) -> list[TransportOutcome]:
        """本地图片相册发送:

        - 逐成员 upload_file_parallel 预上传拿 handle (获得 FastTelethon 并发上传与断点
          续传; 旧行为为路径直发 Telethon 内部单连接上传, 无断点) —— per-item try 隔离,
          单张失败记 error_detail outcome 绝不熔断整组;
        - 同 chunk 内统一单客户端上传+发送 (handle 与上传会话绑定, 跨客户端复用有风险);
          客户端选择保持既有 max(sub_sizes) 阈值分支;
        - 成功子集按 R3 矩阵发送: ≥2 组发 Album / ==1 强制单张 Photo 常规发送
          (pipeline.py:249-252 "单图不组相册"先例 —— Telethon 对 1 元素列表是静默
          降级, 形态歧义显式堵死) / ==0 → 抛首个原异常 (编排层 D2 汇总单卡);
        - 组发/单发失败: 该段成员统一记 error_detail outcome (不 raise) —— 编排层按
          "成功子集==0"路由 D2 汇总单卡。
        """
        if not items:
            return []
        _raise_if_control_signal(context)

        sub_sizes = [itm.size_bytes or (await asyncio.to_thread(_stat_size, itm.item_id)) for itm in items]
        max_size = max(sub_sizes) if sub_sizes else 0
        client, mode_str = self._resolve_send_client(context, max_size)
        if client is None:
            raise RuntimeError("未就绪的 TelegramClient: 无可用的 Userbot 或 Bot 客户端")

        outcomes: list[TransportOutcome] = [TransportOutcome() for _ in items]
        handles: list[tuple[int, Any]] = []
        first_failure_exc: Exception | None = None

        progress: dict[str, Any] = {
            "cur_done": context.completed_files_count,
            "tot_files": context.total_files_count or len(items) or 1,
            "last_speed": 0.0,
        }

        for idx, item in enumerate(items):
            _raise_if_control_signal(context)
            try:
                media = await self._upload_album_member(
                    client, context, idx=idx, item=item, sub_size=sub_sizes[idx], progress=progress
                )
            except (UploadCancelledError, UploadPausedError, asyncio.CancelledError, RetryableTaskError):
                raise
            except FloodWaitError as fw_err:
                wait_sec = getattr(fw_err, "seconds", 5) or 5
                flood_marked = False
                try:
                    self._registry.mark_flood_wait(client, wait_sec)
                    flood_marked = True
                except Exception as reg_err:
                    logger.debug("相册成员上传标记 FloodWait 状态异常: %s", reg_err)
                raise RetryableTaskError(
                    f"相册成员上传遇 FloodWait: {fw_err}",
                    retry_after=float(wait_sec),
                    flood_client_marked=flood_marked,
                ) from fw_err
            except Exception as upload_exc:
                # F2 逐项隔离: 单张上传失败不熔断整组 —— 记 error_detail outcome 继续
                outcomes[idx] = TransportOutcome(
                    message_ids=[],
                    error_detail=str(upload_exc),
                    client_label=mode_str,
                )
                if first_failure_exc is None:
                    first_failure_exc = upload_exc
                continue
            handles.append((idx, media))

        if not handles:
            # R3: 成功子集 == 0 → 整组失败, 抛首个原异常 (编排层 except 分支 → D2 汇总
            # 单卡, 严禁整组逐条连发); 逐项 error_detail 明细随 raise 一并弃用 (整组归因)。
            root = first_failure_exc or RuntimeError("相册全部成员上传失败")
            raise TransportError(str(root), client_label=mode_str, cause=root) from root

        # 成功子集发送 (R3 矩阵 + Telegram 组发 ≤10 防御): 段内 ≥2 组发 / ==1 强制单张
        for seg_start in range(0, len(handles), 10):
            await self._send_album_segment(
                client,
                context,
                items,
                segment=handles[seg_start : seg_start + 10],
                outcomes=outcomes,
                mode_str=mode_str,
                last_speed=progress["last_speed"],
            )
        return outcomes

    async def _try_edit_local_remux(
        self,
        client: Any,
        context: PublishContext,
        *,
        item: PublishItem,
        file_path: str,
        file_name: str,
        msg_id: int,
        remux_candidate: bool,
    ) -> TransportOutcome | None:
        if not remux_candidate or not await asyncio.to_thread(os.path.isfile, file_path):
            return None
        try:
            file_size = item.size_bytes or await asyncio.to_thread(_stat_size, file_path)
            thumb_bytes = await self._resolve_local_video_thumb(item, context)
            return await self._remux_and_upload_local_ts(
                client,
                context,
                item=item,
                file_path=file_path,
                file_name=file_name,
                file_size=file_size,
                mode_str=context.account_display or "Userbot",
                is_bot_client=False,
                thumb_bytes=thumb_bytes,
                msg_id=int(msg_id),
            )
        except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
            raise
        except Exception as edit_err:
            raise TransportError(
                str(edit_err),
                item=item,
                client_label=context.account_display or "Userbot",
                cause=edit_err,
            ) from edit_err

    async def edit_item(self, item: PublishItem, context: PublishContext, msg_id: int) -> TransportOutcome:
        """本地原地回填 (S4/)。

        F1 两段式硬约束: 先 upload_file_parallel 拿已上传 handle, 再 edit_message
        (file=handle) —— 严禁 file= 直接传本地路径 (静默走 Telethon 内部单连接上传,
        断点续传与并发全部失效); F3: attributes 经共享构建器, edit 后保持流媒体播放器;
        caption 基线 = 文件名 (engines.py:180 语义一致)。客户端恒 userbot;
        FILE_PARTS_INVALID 自愈照 send_item 先例 (清 meta + force_fresh 全量重传)。
        """
        file_path = item.item_id
        file_name = item.name or os.path.basename(file_path)
        await asyncio.to_thread(_validate_local_file_sync, file_path, need_size=False)
        client = context.userbot_client or self._registry.userbot
        if client is None:
            raise RuntimeError("未就绪的 TelegramClient: 回填恒走 Userbot, 无可用客户端")
        ext = os.path.splitext(file_path)[1].lower()
        is_video = ext in settings.VIDEO_EXTENSIONS
        is_image = _is_normalizable_image(file_path, item)
        remux_candidate = is_video and is_mpegts_remux_candidate(file_name, context)
        # PLAYBACK_QUALITY 包3: 探测缓存命中 (send_item 已探测同文件), 无重复 IO
        if is_video:
            await self._ensure_local_video_metadata(item, require_codecs=remux_candidate)
        media_attributes = build_media_attributes(item)

        upload_path = file_path
        temp_copy_path: str | None = None
        if is_image:
            upload_path, temp_copy_path = await _prepare_image_cancelled_safe(file_path)

        last_speed = 0.0

        def backfill_progress_hook(
            current: int,
            total: int,
            speed: float,
            eta: float,
            workers: int,
            dc_id: int,
            workers_total: int = 0,
        ) -> None:
            nonlocal last_speed
            _raise_if_control_signal(context)
            last_speed = speed
            cur_done = context.completed_files_count
            tot_files = context.total_files_count or 1
            cur_item = min(tot_files, cur_done + 1)
            _safe_invoke_engine_progress(
                context,
                cur_item,
                tot_files,
                speed,
                eta,
                file_name,
                label="回填进度回调异常",
                concurrency=int(workers) if workers else 1,
                dc_id=dc_id,
                concurrency_total=workers_total or 1,
            )

        max_attempts = 2
        last_edit_err: Exception | None = None
        try:
            remux_outcome = await self._try_edit_local_remux(
                client,
                context,
                item=item,
                file_path=file_path,
                file_name=file_name,
                msg_id=msg_id,
                remux_candidate=remux_candidate,
            )
            if remux_outcome is not None:
                return remux_outcome

            for attempt in range(max_attempts):
                try:
                    uploaded_media = await ft_mod.upload_file_parallel(
                        client,
                        upload_path,
                        progress_callback=backfill_progress_hook,
                        concurrency=context.upload_concurrency or 2,
                        get_concurrency=context.get_current_upload_concurrency,
                        in_flight_per_connection=context.upload_in_flight_per_connection or 2,
                        get_in_flight_per_connection=context.get_current_in_flight,
                        is_cancelled=context.is_cancelled,
                        is_paused=context.is_paused,
                        force_fresh=(attempt > 0),
                    )
                    _raise_if_control_signal(context)
                    thumb_bytes = await self._resolve_local_video_thumb(item, context) if is_video else None
                    edit_kwargs: dict[str, Any] = {
                        "file": uploaded_media,
                        "text": file_name,
                        "attributes": media_attributes or None,
                        "supports_streaming": True if is_video else False,
                    }
                    if is_video and thumb_bytes:
                        edit_kwargs["thumb"] = make_named_thumbnail_stream(thumb_bytes)
                    sent = await client.edit_message(
                        context.target_chat_id,
                        int(msg_id),
                        **edit_kwargs,
                    )
                    return TransportOutcome(
                        message_ids=[int(getattr(sent, "id", 0) or int(msg_id))],
                        speed_mb_s=last_speed,
                        client_label=context.account_display or "Userbot",
                    )
                except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
                    raise
                except Exception as edit_err:
                    last_edit_err = edit_err
                    if is_file_parts_invalid(edit_err) and attempt == 0:
                        logger.warning("回填上传遇 FILE_PARTS_INVALID, 清除断点后全量重传: %s", file_name)
                        ft_mod.discard_upload_meta(upload_path)
                        continue
                    raise TransportError(
                        str(edit_err), item=item, client_label=context.account_display or "Userbot", cause=edit_err
                    ) from edit_err
        finally:
            _discard_album_temp_file(temp_copy_path)
        if last_edit_err is not None:
            raise TransportError(
                str(last_edit_err),
                item=item,
                client_label=context.account_display or "Userbot",
                cause=last_edit_err,
            ) from last_edit_err
        raise RuntimeError(f"原地回填在 {max_attempts} 次尝试后仍未成功完成: {file_name}")
