"""统一发布流水线传输引擎 - PikPak 单发 / 相册 / 原地回填所有权归位。

从门面 (engine.py) 迁入: send_item 路由体 / send_album 图片相册 / edit_item 回填 /
raw 分卷上传 _upload_parts / 单文件发布 _publish_single_item。
门面仅保留薄委托方法与共享编排 (_publish_vsse/fmp4_split_item / 预检 / 封面 /
捕获会话); 本模块函数首参恒为门面实例 (TYPE_CHECKING 导入防环), 门面方法仍是
既有测试的 patch seam (engine._publish_single_item 等)。
"""

import asyncio
import logging
import os
import shutil
from collections.abc import Awaitable, Callable
from typing import Any, cast

from telethon.errors import FloodWaitError

from app.config import settings
from app.core import fast_telethon as ft_mod
from app.core import fmp4_pipe_uploader as fmp4_mod
from app.core import stream_chunk_provider as scp_mod
from app.core import stream_splitter as ss_mod
from app.core.media_metadata import (
    REMOTE_METADATA_PROBE_EXTENSIONS,
    apply_probed_metadata,
    needs_metadata_probe,
    probe_remote_video_metadata,
)
from app.core.pikpak.taxonomy import (
    PikPakAuthError,
    PikPakCliTimeoutError,
    PikPakError,
    PikPakNotFoundError,
    PikPakPublishError,
    PikPakPublishFatalError,
    PikPakQuotaError,
    PikPakTransientError,
    wrap_pikpak_failure,
)
from app.core.pikpak.taxonomy import (
    PikPakRiskControlError as CliPikPakRiskControlError,
)
from app.core.pikpak.thumbnail import make_named_thumbnail_stream
from app.core.pipeline.engines._common import (
    ProgressReporter,
    _AlbumLinkPrefetchWindow,
    _discard_stale_upload_meta_sync,
    _inter_part_breathing_delay,
    _is_huge_file_size,
    _make_pikpak_url_provider,
    _raise_if_control_signal,
    _safe_invoke_engine_progress,
    _send_file_with_floodwait_retry,
    _SpeedMeasuringProvider,
)
from app.core.pipeline.engines._remux import (
    RemuxDecision,
    build_remux_video_attributes,
    decide_mpegts_remux,
    is_mpegts_remux_candidate,
    remux_output_filename,
)
from app.core.pipeline.engines._types import SendFileKwargs
from app.core.pipeline.engines.exceptions import TransportError
from app.core.pipeline.engines.pikpak.split_loop import get_saved_checkpoints
from app.core.pipeline.models import PublishContext, PublishItem, TransportOutcome
from app.core.pipeline.utils import (
    build_media_attributes,
    get_meta_path,
    is_file_parts_invalid,
)
from app.core.scheduler import RetryableTaskError, UploadCancelledError, UploadPausedError

logger = logging.getLogger(__name__)


def _make_prefetched_url_provider(
    link: str,
    fallback: Callable[[], Awaitable[str]],
) -> Callable[[], Awaitable[str]]:
    """预取链路 → 零参 url_provider 工厂 (B023 收口: 首次返回预取直链, 后续刷新经 fallback 换取新链)。"""
    consumed = False

    async def _url_provider() -> str:
        nonlocal consumed
        if not consumed:
            consumed = True
            return link
        return await fallback()

    return _url_provider


def _make_part_hook(
    *,
    item: PublishItem,
    parts: list[Any],
    part_index: int,
    prior_bytes: int,
    provider: _SpeedMeasuringProvider,
    context: PublishContext,
) -> Callable[..., Any]:
    reporter = ProgressReporter(item, context, provider=provider, log_label="PikPak")
    return reporter.make_part_hook(parts, part_index, prior_bytes)


def _consume_checkpoint_hit(part: Any, delivered_parts: dict[int, int], total_parts: int) -> int | None:
    """TD-07 换链重试幂等: 已成功投递的分卷直接跳过上传与发送 (未投递返回 None)。"""
    if part.part_index not in delivered_parts:
        return None
    cached_msg_id = delivered_parts[part.part_index]
    logger.info(
        "⏩ PikPak 换链重试: 分卷 Part %d/%d 已成功投递 (msg_id=%d)，幂等跳过重发",
        part.part_index,
        total_parts,
        cached_msg_id,
    )
    return cached_msg_id


def _build_raw_send_kwargs(item: PublishItem, part: Any, total_parts: int, thumb_bytes: bytes | None) -> SendFileKwargs:
    """raw 分卷发送参数构建: 分卷 caption + 音频优先判定属性 + 封面注入。"""
    ext = os.path.splitext(item.name)[1].lower()
    is_single_part = total_parts <= 1
    # 音频判定优先 (P0): PikPak 会为 MP3/FLAC 等返回媒体时长, 若 duration 直接参与
    # 视频判定, 音频将误挂 DocumentAttributeVideo (黑屏播放器) 且音频分支沦为死代码
    is_audio = ext in settings.AUDIO_EXTENSIONS
    is_video = (ext in settings.VIDEO_EXTENSIONS) or (bool(item.duration_s) and not is_audio)

    # 属性构建收口共享函数 (eligible=is_single_part —— 多分卷
    # 视频不注流媒体属性, 加固收窄语义保持)
    media_attributes = build_media_attributes(item, eligible=is_single_part)

    send_kwargs: SendFileKwargs = {
        "caption": ss_mod.format_part_caption(item.name, part.part_index, total_parts),
        "supports_streaming": True if (is_video and is_single_part) else False,
    }
    if media_attributes:
        send_kwargs["attributes"] = media_attributes
    if ext in (".ts", ".m2ts", ".mts"):
        send_kwargs["mime_type"] = "video/mp2t"
    elif ext == ".m4v" and is_single_part:
        send_kwargs["mime_type"] = "video/mp4"
    if thumb_bytes:
        send_kwargs["thumb"] = make_named_thumbnail_stream(thumb_bytes)
    return send_kwargs


def _raw_upload_concurrency_kwargs(item: PublishItem, context: PublishContext) -> dict[str, Any]:
    """upload_file_parallel 并发细分参数 (huge 文件预取并发封顶 2)。"""
    is_huge = _is_huge_file_size(item.size_bytes)
    return {
        "prefetch_concurrency": (
            min(2, max(1, context.get_current_prefetch_concurrency()))
            if is_huge
            else (context.prefetch_concurrency or 3)
        ),
        "get_prefetch_concurrency": (
            (lambda: min(2, max(1, context.get_current_prefetch_concurrency())))
            if is_huge
            else context.get_current_prefetch_concurrency
        ),
    }


async def _upload_single_part(
    engine: Any,
    client: Any,
    *,
    provider: _SpeedMeasuringProvider,
    item: PublishItem,
    part: Any,
    parts: list[Any],
    context: PublishContext,
    prior_bytes: int,
    meta_path: str,
    thumb_bytes: bytes | None,
) -> Any:
    """单 Part 上传 + 发送 (2 次尝试; FILE_PARTS_INVALID 自愈: 清失效断点后全量重传)。"""
    sliced_provider = scp_mod.SlicedChunkProvider(
        inner=provider,
        offset=part.offset,
        length=part.length,
        part_index=part.part_index,
        total_parts=len(parts),
    )
    concurrency_kwargs = _raw_upload_concurrency_kwargs(item, context)
    sent = None
    for attempt in range(2):
        try:
            uploaded_media = await ft_mod.upload_file_parallel(
                client,
                f"pikpak://{item.item_id}",
                progress_callback=_make_part_hook(
                    item=item,
                    parts=parts,
                    part_index=part.part_index,
                    prior_bytes=prior_bytes,
                    provider=provider,
                    context=context,
                ),
                concurrency=context.upload_concurrency or 2,
                get_concurrency=context.get_current_upload_concurrency,
                in_flight_per_connection=context.upload_in_flight_per_connection or 2,
                get_in_flight_per_connection=context.get_current_in_flight,
                is_cancelled=context.is_cancelled,
                is_paused=context.is_paused,
                force_fresh=(attempt > 0),
                chunk_provider=sliced_provider,
                meta_path=meta_path,
                **concurrency_kwargs,
            )
            send_kwargs = _build_raw_send_kwargs(item, part, len(parts), thumb_bytes)
            sent = await client.send_file(context.target_chat_id, uploaded_media, **send_kwargs)
            break
        except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
            raise
        except Exception as upload_err:
            if is_file_parts_invalid(upload_err) and attempt == 0:
                logger.warning(
                    "⚠️ PikPak 发布检测到 Telegram 服务端分块缓存已失效 (file_id=%s part=%s/%s)，"
                    "正在清除失效断点并全量重新上传该 Part...",
                    item.item_id,
                    part.part_index,
                    len(parts),
                )
                await asyncio.to_thread(_discard_stale_upload_meta_sync, meta_path)
                continue
            raise

    if sent is None:
        raise PikPakPublishFatalError("unknown")
    return sent


async def _absorb_part_delivery(
    engine: Any,
    context: PublishContext,
    item: PublishItem,
    parts: list[Any],
    raw_threshold: int,
    part: Any,
    sent: Any,
    delivered_parts: dict[int, int],
) -> int:
    """交付记账: msg_id 提取 + delivered_parts 台账 + 多分卷检查点落库。返回 last_message_id (0 = 未获取)。"""
    msg_id = getattr(sent, "id", 0)
    last_message_id = int(msg_id) if msg_id else 0
    if last_message_id:
        delivered_parts[part.part_index] = last_message_id
        if len(parts) > 1:
            await engine._store.record_part_checkpoint(
                context.task_id,
                item.item_id,
                "raw",
                len(parts),
                raw_threshold,
                part.part_index,
                last_message_id,
            )
        return last_message_id
    if len(parts) > 1:
        raise PikPakPublishFatalError(
            "telegram_send_failed",
            f"Raw Part {part.part_index}/{len(parts)} 消息发送失败 (未获取到有效 msg_id)",
        )
    # 单片未获有效 msg_id 不写 0 哨兵, 返回 0 由编排层按「发送未返回消息 ID」记为条目级失败并继续批次
    return 0


async def upload_parts(
    engine: Any,
    client: Any,
    *,
    provider: _SpeedMeasuringProvider,
    item: PublishItem,
    parts: list[Any],
    context: PublishContext,
    thumb_provider: Callable[[], Awaitable[bytes | None]] | None = None,
    delivered_parts: dict[int, int] | None = None,
) -> tuple[int, float]:
    """raw 物理分卷上传主循环。"""
    if delivered_parts is None:
        delivered_parts = {}

    if hasattr(context, "reset_probe_cache"):
        context.reset_probe_cache()

    last_message_id = 0
    last_speed = 0.0
    prior_bytes = 0

    raw_threshold = context.video_split_threshold_bytes or ss_mod.derive_split_threshold(context.is_premium)
    if len(parts) > 1:
        saved_checkpoints = await get_saved_checkpoints(
            context.task_id, item.item_id, "raw", len(parts), raw_threshold, store=engine._store
        )
        delivered_parts.update(saved_checkpoints)

    for part in parts:
        _raise_if_control_signal(context)

        cached_msg_id = _consume_checkpoint_hit(part, delivered_parts, len(parts))
        if cached_msg_id is not None:
            last_message_id = max(last_message_id, cached_msg_id)
            prior_bytes += part.length
            continue

        # 空 task_id 兜底命名还原旧基线 "pikpak-direct" (孤儿 GC 前缀兼容)
        meta_path = get_meta_path(context.task_id or "pikpak-direct", item.item_id, part.part_index)
        thumb_bytes = None
        if part.part_index == 1 and thumb_provider is not None:
            try:
                thumb_bytes = await thumb_provider()
            except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
                raise
            except Exception as th_err:
                logger.debug("Raw 提取封面失败 (非致命): %s", type(th_err).__name__)
                thumb_bytes = None
            _raise_if_control_signal(context)

        sent = await _upload_single_part(
            engine,
            client,
            provider=provider,
            item=item,
            part=part,
            parts=parts,
            context=context,
            prior_bytes=prior_bytes,
            meta_path=meta_path,
            thumb_bytes=thumb_bytes,
        )
        last_message_id = max(
            last_message_id,
            await _absorb_part_delivery(engine, context, item, parts, raw_threshold, part, sent, delivered_parts),
        )
        last_speed = provider.download_speed_mb_s
        prior_bytes += part.length

        if len(parts) > 1 and part.part_index < len(parts):
            await _inter_part_breathing_delay(context, part.part_index, len(parts), "Raw")

    if len(parts) > 1 and len(delivered_parts) == len(parts):
        await engine._store.clear_part_checkpoint(context.task_id, item.item_id)

    return last_message_id, last_speed


def _pop_engine_prefetched_link(engine: Any, item_id: str) -> str | None:
    """安全消费引擎暂存的 TS 校验直链 (仅读真实类方法, 兼容测试 MagicMock engine)。"""
    if getattr(type(engine), "pop_prefetched_link", None) is None:
        return None
    val = engine.pop_prefetched_link(item_id)
    return val if isinstance(val, str) and val else None


async def _maybe_enrich_pikpak_video_metadata(
    item: PublishItem,
    context: PublishContext,
    shared_initial_url: str | None,
    raw_url_provider: Callable[[], Awaitable[str]],
    *,
    label: str,
    require_codecs: bool = False,
) -> str | None:
    """当 PikPak 视频缺失时长或宽高且扩展名受支持时执行有界在线元数据补齐。

    JIT 索链在 probe_remote_video_metadata 的跨任务并发信号量与总超时预算内执行；
    若成功取链则返回供后续首次上传/回填复用。
    """
    ext = os.path.splitext(item.name or item.item_id)[1].lower()
    is_audio = ext in settings.AUDIO_EXTENSIONS
    is_video = (ext in settings.VIDEO_EXTENSIONS) or (bool(item.duration_s) and not is_audio)
    if not (
        is_video
        and ext in REMOTE_METADATA_PROBE_EXTENSIONS
        and needs_metadata_probe(item, require_codecs=require_codecs)
    ):
        return shared_initial_url

    resolved_url: str | None = shared_initial_url

    async def _capturing_url_provider() -> str:
        nonlocal resolved_url
        url = await raw_url_provider()
        if url:
            resolved_url = url
        return url

    try:
        probed = await probe_remote_video_metadata(
            shared_initial_url or _capturing_url_provider,
            ext,
            file_size=item.size_bytes,
            is_cancelled=context.is_cancelled,
            is_paused=context.is_paused,
        )
        apply_probed_metadata(item, probed)
    except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
        raise
    except Exception as probe_err:
        logger.debug(
            "PikPak %s视频元数据补齐失败 (非致命): file_id=%s err=%s",
            label,
            item.item_id,
            type(probe_err).__name__,
        )
    return resolved_url


async def publish_single_item(
    engine: Any,
    client: Any,
    item: PublishItem,
    parts: list[Any],
    context: PublishContext,
    delivered_parts: dict[int, int] | None = None,
) -> TransportOutcome:
    if client is None:
        raise PikPakPublishFatalError("userbot")

    if engine._link_provider is None:
        raise PikPakPublishFatalError(
            "unknown",
            "PikPakStreamTransportEngine 缺少 link_provider 注入 (门面装配缺失)",
        )

    chunk_provider_cls = scp_mod.PikPakStreamChunkProvider

    is_huge = _is_huge_file_size(item.size_bytes)
    effective_prefetch = (
        min(2, max(1, context.get_current_prefetch_concurrency())) if is_huge else (context.prefetch_concurrency or 3)
    )
    raw_url_provider = _make_pikpak_url_provider(engine._link_provider, item.item_id, strict=False)
    shared_initial_url = _pop_engine_prefetched_link(engine, item.item_id)
    initial_url_consumed = False

    if len(parts) <= 1:
        shared_initial_url = await _maybe_enrich_pikpak_video_metadata(
            item, context, shared_initial_url, raw_url_provider, label="单发"
        )

    async def _provider_url_provider() -> str:
        nonlocal shared_initial_url, initial_url_consumed
        if not initial_url_consumed:
            initial_url_consumed = True
            if shared_initial_url:
                return shared_initial_url
        url = await raw_url_provider()
        if shared_initial_url is None:
            shared_initial_url = url
        return url

    inner_provider = chunk_provider_cls(
        file_id=item.item_id,
        file_name=item.name or item.item_id,
        total_bytes=item.size_bytes,
        url_provider=_provider_url_provider,
        prefetch_concurrency=effective_prefetch,
    )
    provider = _SpeedMeasuringProvider(inner_provider)

    async def _thumb_url_provider() -> str:
        nonlocal shared_initial_url
        current_url = getattr(inner_provider, "_current_url", None)
        if isinstance(current_url, str) and current_url:
            return current_url
        if shared_initial_url:
            return shared_initial_url
        shared_initial_url = await raw_url_provider()
        return shared_initial_url

    effective_thumb_provider = engine._make_thumb_provider(
        item,
        url_provider=_thumb_url_provider,
    )

    try:
        last_message_id, last_speed = await upload_parts(
            engine,
            client,
            provider=provider,
            item=item,
            parts=parts,
            context=context,
            thumb_provider=effective_thumb_provider,
            delivered_parts=delivered_parts,
        )
        return TransportOutcome(
            message_ids=[last_message_id] if last_message_id else [],
            speed_mb_s=last_speed,
            client_label=context.account_display,
        )
    finally:
        await provider.aclose()


_PikpakFailureMatchingTypes = (
    scp_mod.ChunkFetchTransientError,
    scp_mod.PikPakThrottledError,
    PikPakTransientError,
    PikPakAuthError,
    scp_mod.PikPakRiskControlError,
    CliPikPakRiskControlError,
    PikPakNotFoundError,
    scp_mod.PikPakNotFoundError,
    PikPakQuotaError,
    scp_mod.PikPakLinkExpiredError,
    PikPakError,
)

_SPLIT_PUBLISHERS = ("fmp4_universal", "vsse_with_fallback")


def _is_vp8_unsafe_for_mp4_copy(vcodec: str) -> bool:
    """VP8 族编码 MP4/fMP4 容器不支持 -c:v copy (必死崩溃), 须安全降级 raw 物理分卷。"""
    return vcodec in ("vp8", "libvpx", "vp80", "v_vp8") or vcodec.startswith("vp8")


async def _publish_split_by_strategy(
    engine: Any,
    client: Any,
    item: PublishItem,
    threshold: int,
    context: PublishContext,
    strategy: str,
    ext: str,
) -> TransportOutcome:
    """双轨切片发布路由 (vsse_with_fallback 容器校验 / fmp4_universal 直入 fMP4)。"""
    if strategy == "vsse_with_fallback" and ext not in (".mp4", ".mov", ".m4v"):
        logger.info(
            "VSSE pre-flight 校验未通过 (非 MP4/MOV 容器, ext=%s)，自动路由至 fMP4 全能保底发布通道",
            ext,
        )
        capture_session = await engine._open_capture_session(item, context, threshold, strategy)
        return cast(
            TransportOutcome,
            await engine._publish_fmp4_split_item(client, item, threshold, context, capture_session=capture_session),
        )
    # E2E 捕获: 路由判定后、通道发布前完成源快照
    capture_session = await engine._open_capture_session(item, context, threshold, strategy)
    if strategy == "vsse_with_fallback":
        return cast(
            TransportOutcome,
            await engine._publish_vsse_split_item(client, item, threshold, context, capture_session=capture_session),
        )
    # 契约闭环: fMP4 通道分卷失败严禁捕获后静默降级为不可播 raw 硬切
    return cast(
        TransportOutcome,
        await engine._publish_fmp4_split_item(
            client,
            item,
            threshold,
            context,
            capture_session=capture_session,
            use_paged_proxy=settings.E2E_FORCE_PAGED_PROXY,
        ),
    )


async def _publish_raw_with_relink(
    engine: Any,
    client: Any,
    item: PublishItem,
    parts: list[Any],
    context: PublishContext,
) -> TransportOutcome:
    """raw 物理分卷发布 + PikPakLinkExpiredError 3 次换链重建 Provider 重试。"""
    last_link_exc: Exception | None = None
    delivered_parts: dict[int, int] = {}
    for attempt in range(engine._LINK_EXPIRED_RELINK_ATTEMPTS):
        try:
            return cast(
                TransportOutcome,
                await engine._publish_single_item(client, item, parts, context, delivered_parts=delivered_parts),
            )
        except scp_mod.PikPakLinkExpiredError as exc:
            last_link_exc = exc
            logger.warning(
                "🔁 PikPak 直链过期, 重新索链重建 Provider (%d/%d): file_id=%s",
                attempt + 1,
                engine._LINK_EXPIRED_RELINK_ATTEMPTS,
                item.item_id,
            )
        except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
            raise
        except Exception as exc:
            if isinstance(exc, _PikpakFailureMatchingTypes):
                raise engine._wrap_failure(exc) from exc
            raise

    raise engine._wrap_failure(last_link_exc or PikPakError("直链过期且无可用错误信息"))


async def _log_remux_fallback(engine: Any, context: PublishContext, msg: str) -> None:
    logger.warning(msg)
    if context.task_id and getattr(engine, "_store", None) is not None:
        try:
            await engine._store.append_log(context.task_id, f"⚠️ {msg}")
        except Exception as log_err:
            logger.debug("remux 降级日志写入异常 (非致命): %s", log_err)


async def _stream_pikpak_ts_remux(
    engine: Any,
    client: Any,
    item: PublishItem,
    context: PublishContext,
    *,
    decision: RemuxDecision,
    initial_url: str,
    raw_url_provider: Callable[[], Awaitable[str]],
    max_bytes: int,
    workers_count: int,
    in_flight_count: int,
    msg_id: int | None,
) -> tuple[Any, float, float, Any | None, str, TransportOutcome | None]:
    """执行带直链过期重连重试的 PikPak MPEG-TS 零落盘转封装上传。"""
    reporter = ProgressReporter(item, context, log_label="PikPak-Remux")
    last_speed = 0.0
    url = initial_url
    max_attempts = max(1, int(getattr(engine, "_LINK_EXPIRED_RELINK_ATTEMPTS", 3) or 3))

    def _remux_progress_cb(
        current_bytes: int, total_bytes: int, speed: float, eta: float, _fname: str, workers: int
    ) -> None:
        nonlocal last_speed
        _raise_if_control_signal(context)
        last_speed = speed
        reporter.emit_progress(
            current_bytes=current_bytes,
            total_bytes=max(item.size_bytes, current_bytes, total_bytes),
            speed=speed,
            eta=eta,
            workers=int(workers) if workers else workers_count,
            file_name_override=item.name,
        )

    for attempt in range(max_attempts):
        _raise_if_control_signal(context)
        try:
            if attempt > 0:
                logger.warning(
                    "🔁 PikPak MPEG-TS 重封装源读取失败，重新索链重试 (%d/%d): file_id=%s",
                    attempt + 1,
                    max_attempts,
                    item.item_id,
                )
                url = await raw_url_provider()
                _raise_if_control_signal(context)

            uploader = fmp4_mod.ZeroDiskFmp4StreamUploader(
                client=client,
                max_workers=workers_count,
                queue_size=max(12, workers_count * in_flight_count * 2),
                in_flight_per_connection=in_flight_count,
            )
            res_file, actual_dur = await uploader.stream_to_telegram(
                pikpak_url=url,
                start_sec=0.0,
                end_sec=float(item.duration_s or 0.0),
                file_name=remux_output_filename(item.name),
                copy_audio=decision.copy_audio,
                part_index=1,
                max_part_bytes=max_bytes,
                progress_callback=_remux_progress_cb,
                is_cancelled=context.is_cancelled,
                is_paused=context.is_paused,
                full_stream=True,
                force_single_input=True,
                pause_yield=True,
                auto_small_file_mode=True,
                estimated_output_bytes=item.size_bytes if item.size_bytes > 0 else None,
                expected_input_bytes=item.size_bytes if item.size_bytes > 0 else None,
                audio_copy_tracks=decision.audio_copy_tracks,
            )
            return res_file, actual_dur, (uploader.last_speed or last_speed), uploader, url, None
        except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
            raise
        except (
            scp_mod.PikPakLinkExpiredError,
            scp_mod.ChunkFetchTransientError,
            PikPakTransientError,
            PikPakCliTimeoutError,
        ) as source_err:
            if attempt + 1 >= max_attempts:
                await _log_remux_fallback(
                    engine, context, f"[{item.name}] 重封装源读取重试耗尽 ({source_err})，降级为原样 .ts 文档发送"
                )
                return None, 0.0, last_speed, None, url, None
        except fmp4_mod.PartOversizeError as oversize_err:
            split_outcome = await _handle_remux_oversize(engine, client, item, context, oversize_err, msg_id)
            return None, 0.0, last_speed, None, url, split_outcome
        except (
            scp_mod.PikPakThrottledError,
            scp_mod.PikPakRiskControlError,
            CliPikPakRiskControlError,
            PikPakAuthError,
            PikPakQuotaError,
            PikPakNotFoundError,
            scp_mod.PikPakNotFoundError,
            PikPakPublishError,
        ) as cdn_exc:
            raise wrap_pikpak_failure(cdn_exc) from cdn_exc
        except Exception as remux_err:
            await _log_remux_fallback(
                engine, context, f"[{item.name}] MPEG-TS 零落盘重封装失败 ({remux_err})，安全降级为原样 .ts 文档发送"
            )
            return None, 0.0, last_speed, None, url, None

    return None, 0.0, last_speed, None, url, None


async def _handle_remux_oversize(
    engine: Any,
    client: Any,
    item: PublishItem,
    context: PublishContext,
    oversize_err: fmp4_mod.PartOversizeError,
    msg_id: int | None,
) -> TransportOutcome | None:
    if msg_id is None and context.oversize_policy == "split":
        strategy = context.split_strategy or settings.SPLIT_STRATEGY
        if strategy in _SPLIT_PUBLISHERS:
            threshold = min(
                oversize_err.max_part_bytes,
                context.video_split_threshold_bytes or ss_mod.derive_split_threshold(context.is_premium),
            )
            capture_session = await engine._open_capture_session(item, context, threshold, strategy)
            return cast(
                TransportOutcome,
                await engine._publish_fmp4_split_item(
                    client,
                    item,
                    threshold,
                    context,
                    capture_session=capture_session,
                    estimated_output_bytes=oversize_err.total_emitted_bytes,
                ),
            )
    await _log_remux_fallback(
        engine, context, f"[{item.name}] 重封装后体积触发单文件上限 ({oversize_err})，降级为原样 .ts 发送"
    )
    return None


async def _deliver_pikpak_remuxed_media(
    engine: Any,
    client: Any,
    item: PublishItem,
    context: PublishContext,
    *,
    res_file: Any,
    actual_dur: float,
    last_speed: float,
    uploader: Any,
    url: str,
    decision: Any,
    msg_id: int | None,
) -> TransportOutcome:
    _raise_if_control_signal(context)
    thumb_bytes = await engine._fetch_engine_thumb(item, lambda: url, log_label="PikPak-Remux")
    _raise_if_control_signal(context)
    attrs = build_remux_video_attributes(item, actual_dur, uploader.video_width, uploader.video_height)

    if msg_id is not None:
        edit_kwargs: dict[str, Any] = {
            "file": res_file,
            "text": item.name or item.item_id,
            "attributes": attrs or None,
            "supports_streaming": True,
        }
        if thumb_bytes:
            edit_kwargs["thumb"] = make_named_thumbnail_stream(thumb_bytes)
        sent = await client.edit_message(context.target_chat_id, int(msg_id), **edit_kwargs)
        resolved_id = int(getattr(sent, "id", 0) or int(msg_id))
        return TransportOutcome(message_ids=[resolved_id], speed_mb_s=last_speed, client_label=context.account_display)

    send_kwargs: SendFileKwargs = {"caption": item.name, "supports_streaming": True, "mime_type": "video/mp4"}
    if attrs:
        send_kwargs["attributes"] = attrs
    if thumb_bytes:
        send_kwargs["thumb"] = make_named_thumbnail_stream(thumb_bytes)
    sent, last_sf_err = await _send_file_with_floodwait_retry(
        client,
        context.target_chat_id,
        res_file,
        send_kwargs,
        retry_other_errors=True,
        log_label="PikPak-Remux send_file",
        registry=engine._registry,
        context=context,
    )
    sent_id = int(getattr(sent, "id", 0) or 0)
    if not sent_id and last_sf_err is not None:
        raise last_sf_err
    logger.info(
        "🎬 PikPak MPEG-TS 自动重封装为 MP4 发布成功: %s (msg_id=%s, duration=%.2fs, copy_audio=%s)",
        item.name,
        sent_id,
        actual_dur,
        decision.copy_audio,
    )
    return TransportOutcome(
        message_ids=[sent_id] if sent_id else [], speed_mb_s=last_speed, client_label=context.account_display
    )


async def _remux_and_upload_pikpak_ts(
    engine: Any,
    client: Any,
    item: PublishItem,
    context: PublishContext,
    *,
    msg_id: int | None = None,
) -> TransportOutcome | None:
    """单文件 .ts/.m2ts/.mts 零落盘转封装 MP4 并流式上传。

    返回 None 表示需安全降级回原样 .ts 文档直传（不兼容视频编码 / ffmpeg 缺失或转封装失败）。
    """
    if client is None:
        raise PikPakPublishFatalError("userbot")
    if engine._link_provider is None:
        raise PikPakPublishFatalError("unknown", "PikPakStreamTransportEngine 缺少 link_provider 注入 (门面装配缺失)")

    raw_url_provider = _make_pikpak_url_provider(engine._link_provider, item.item_id, strict=False)
    shared_initial_url = await _maybe_enrich_pikpak_video_metadata(
        item,
        context,
        _pop_engine_prefetched_link(engine, item.item_id),
        raw_url_provider,
        label="回填重封装" if msg_id is not None else "单发重封装",
        require_codecs=True,
    )
    decision = decide_mpegts_remux(item, ffmpeg_available=bool(shutil.which("ffmpeg")), require_known_vcodec=False)
    if not decision.should_remux:
        await _log_remux_fallback(engine, context, f"[{item.name}] {decision.skip_reason}")
        return None

    url = shared_initial_url or await raw_url_provider()
    retried_delivery = False
    while True:
        res_file, actual_dur, last_speed, uploader, url, split_outcome = await _stream_pikpak_ts_remux(
            engine,
            client,
            item,
            context,
            decision=decision,
            initial_url=url,
            raw_url_provider=raw_url_provider,
            max_bytes=context.task_limit_bytes
            or (settings.PREMIUM_MAX_FILE_SIZE_BYTES if context.is_premium else settings.MAX_FILE_SIZE_BYTES),
            workers_count=max(1, min(int(context.upload_concurrency or 2), 8)),
            in_flight_count=max(1, min(int(context.upload_in_flight_per_connection or 1), 4)),
            msg_id=msg_id,
        )
        if split_outcome is not None:
            return split_outcome
        if res_file is None or uploader is None:
            return None
        try:
            return await _deliver_pikpak_remuxed_media(
                engine,
                client,
                item,
                context,
                res_file=res_file,
                actual_dur=actual_dur,
                last_speed=last_speed,
                uploader=uploader,
                url=url,
                decision=decision,
                msg_id=msg_id,
            )
        except Exception as delivery_err:
            if retried_delivery or not is_file_parts_invalid(delivery_err):
                raise
            retried_delivery = True
            logger.warning("[%s] 重封装上传遇 FILE_PARTS_INVALID，重新生成分片并上传一次", item.name)


async def send_item(engine: Any, item: PublishItem, context: PublishContext) -> TransportOutcome:
    """单文件发布编排: 切片策略命中 → 双轨通道 / 未命中 → raw 换链重试。"""
    _raise_if_control_signal(context)

    client = context.userbot_client
    if client is None:
        client = engine._registry.userbot

    threshold = context.video_split_threshold_bytes or ss_mod.derive_split_threshold(context.is_premium)
    parts = ss_mod.VirtualStreamSplitter.split(item.size_bytes, threshold)
    if not parts:
        logger.info(
            "PikPakStreamTransportEngine: 文件体积为0或无可分卷数据: file_id=%s name=%s", item.item_id, item.name
        )
        return TransportOutcome(message_ids=[], speed_mb_s=0.0, client_label=context.account_display)

    ext = os.path.splitext(item.name)[1].lower()
    is_audio = ext in settings.AUDIO_EXTENSIONS
    is_video = (ext in settings.VIDEO_EXTENSIONS) or (bool(item.duration_s) and not is_audio)
    strategy = context.split_strategy or settings.SPLIT_STRATEGY
    vcodec = (item.video_codec or "").lower().strip()

    if len(parts) > 1 and is_video and strategy in _SPLIT_PUBLISHERS and not _is_vp8_unsafe_for_mp4_copy(vcodec):
        try:
            return await _publish_split_by_strategy(engine, client, item, threshold, context, strategy, ext)
        except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
            raise
        except Exception as split_exc:
            if isinstance(split_exc, _PikpakFailureMatchingTypes):
                raise engine._wrap_failure(split_exc) from split_exc
            raise

    if len(parts) > 1 and is_video and _is_vp8_unsafe_for_mp4_copy(vcodec):
        logger.warning(
            "⚠️ 视频 %s 编码为 %s，MP4/fMP4 容器不支持该编码复制 (-c:v copy 必死崩溃)，安全降级至 raw 物理分卷发布",
            item.name,
            vcodec,
        )

    if len(parts) == 1 and is_mpegts_remux_candidate(item.name, context):
        try:
            remux_outcome = await _remux_and_upload_pikpak_ts(engine, client, item, context)
            if remux_outcome is not None:
                return remux_outcome
        except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
            raise
        except Exception as remux_exc:
            if isinstance(remux_exc, _PikpakFailureMatchingTypes):
                raise engine._wrap_failure(remux_exc) from remux_exc
            raise

    return await _publish_raw_with_relink(engine, client, item, parts, context)


async def send_album(engine: Any, items: list[PublishItem], context: PublishContext) -> list[TransportOutcome]:
    """PikPak 图片相册发送 (· D1/D6; 三段拆解:
    _collect_pending_album_items 入组拦截 / _upload_album_members 预取+逐项隔离 /
    _send_album_handles 组发矩阵)。"""
    if not items:
        return []
    _raise_if_control_signal(context)

    client = context.userbot_client
    if client is None:
        client = engine._registry.userbot
    if client is None:
        raise PikPakPublishFatalError("userbot")
    link_provider = engine._link_provider
    if link_provider is None:
        raise PikPakPublishFatalError(
            "unknown",
            "PikPakStreamTransportEngine 缺少 link_provider 注入 (门面装配缺失)",
        )

    threshold = context.video_split_threshold_bytes or ss_mod.derive_split_threshold(context.is_premium)
    outcomes, pending = _collect_pending_album_items(items, context, threshold)
    if not pending:
        return outcomes

    progress: dict[str, Any] = {
        "cur_done": context.completed_files_count,
        "tot_files": context.total_files_count or len(items) or 1,
        "overall_bytes_done": context.completed_bytes,
        "overall_bytes_tot": context.total_bytes_sum,
        "pending": pending,
    }

    handles, first_failure_exc = await _upload_album_members(
        engine,
        client,
        context,
        link_provider=link_provider,
        pending=pending,
        outcomes=outcomes,
        progress=progress,
    )

    if not handles:
        root = first_failure_exc or PikPakError("相册全部成员上传失败")
        raise TransportError(str(root), client_label=context.account_display, cause=root) from root

    return await _send_album_handles(
        client,
        items,
        context,
        handles=handles,
        outcomes=outcomes,
        last_speed=progress.get("last_speed", 0.0),
    )


def _collect_pending_album_items(
    items: list[PublishItem], context: PublishContext, threshold: int
) -> tuple[list[TransportOutcome], list[tuple[int, PublishItem]]]:
    """相册入组拦截: 不低于分卷阈值的成员绝不入组 (走占位卡/跳过路径)。"""
    outcomes: list[TransportOutcome] = [TransportOutcome() for _ in items]
    pending: list[tuple[int, PublishItem]] = []
    for idx, it in enumerate(items):
        if it.size_bytes >= threshold:
            logger.warning(
                "相册入组拦截: %s (%.1fMB) 不低于分卷阈值 %.0fMB, 绝不入组 (走占位卡/跳过路径)",
                it.name,
                it.size_bytes / (1024 * 1024),
                threshold / (1024 * 1024),
            )
            continue
        pending.append((idx, it))
    return outcomes, pending


def _make_album_photo_hook(
    context: PublishContext,
    *,
    pos: int,
    item: PublishItem,
    provider: _SpeedMeasuringProvider,
    progress: dict[str, Any],
) -> Callable[..., None]:
    """相册成员上传进度钩子 (B023 收口: 经默认参数绑定当前迭代)。"""

    def photo_hook(
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
        cur_done = progress["cur_done"]
        tot_files = progress["tot_files"]
        file_fraction = (current / total) if total > 0 else 0.0
        percent = ((cur_done + pos + file_fraction) / tot_files * 100) if tot_files else 0.0
        cur_item = min(tot_files, cur_done + pos + 1)
        _safe_invoke_engine_progress(
            context,
            cur_item,
            tot_files,
            speed,
            eta,
            item.name,
            label="PikPak 相册进度回调异常",
            concurrency=int(workers) if workers else 1,
            dc_id=dc_id,
            concurrency_total=workers_total or (context.upload_concurrency or 2),
            percent=percent,
            force_db=False,
            pikpak_download_speed_mb_s=provider.download_speed_mb_s,
            total_bytes_done=progress["overall_bytes_done"] + current,
            total_bytes_all=progress["overall_bytes_tot"] or item.size_bytes,
        )

    return photo_hook


async def _resolve_album_url_provider(
    link_provider: Callable[[list[str]], Awaitable[list[str]]],
    prefetch: _AlbumLinkPrefetchWindow,
    item: PublishItem,
) -> Callable[[], Awaitable[str]]:
    """预取窗口有存货 → 立即消费预取链 (eager); 否则返回串行 JIT 索链 provider (provider 首读触发)。"""
    if prefetch.depth:
        prefetched_link = await prefetch.pop()
        return _make_prefetched_url_provider(
            prefetched_link,
            _make_pikpak_url_provider(link_provider, item.item_id, strict=False),
        )
    return _make_pikpak_url_provider(link_provider, item.item_id, strict=False)


async def _upload_one_album_member(
    engine: Any,
    client: Any,
    context: PublishContext,
    *,
    pos: int,
    item: PublishItem,
    prefetch: _AlbumLinkPrefetchWindow,
    progress: dict[str, Any],
    link_provider: Callable[[list[str]], Awaitable[list[str]]],
) -> Any:
    """单成员预取索链 → Provider 构建 → 上传拿 handle (异常原样上抛由调用方逐项隔离)。"""
    url_provider = await _resolve_album_url_provider(link_provider, prefetch, item)
    prefetch.advance(progress["pending"], pos + 1)

    inner_provider = scp_mod.PikPakStreamChunkProvider(
        file_id=item.item_id,
        file_name=item.name or item.item_id,
        total_bytes=item.size_bytes,
        url_provider=url_provider,
    )
    provider = _SpeedMeasuringProvider(inner_provider)
    meta_path = get_meta_path(context.task_id or "pikpak-direct", item.item_id, 1)
    try:
        return await ft_mod.upload_file_parallel(
            client,
            f"pikpak://{item.item_id}",
            progress_callback=_make_album_photo_hook(context, pos=pos, item=item, provider=provider, progress=progress),
            concurrency=context.upload_concurrency or 2,
            get_concurrency=context.get_current_upload_concurrency,
            in_flight_per_connection=context.upload_in_flight_per_connection or 2,
            get_in_flight_per_connection=context.get_current_in_flight,
            is_cancelled=context.is_cancelled,
            is_paused=context.is_paused,
            chunk_provider=provider,
            meta_path=meta_path,
        )
    finally:
        await provider.aclose()


async def _upload_album_members(
    engine: Any,
    client: Any,
    context: PublishContext,
    *,
    link_provider: Callable[[list[str]], Awaitable[list[str]]],
    pending: list[tuple[int, PublishItem]],
    outcomes: list[TransportOutcome],
    progress: dict[str, Any],
) -> tuple[list[tuple[int, Any]], Exception | None]:
    """预取窗口 + 逐项隔离上传 (F2: 单张失败记 error_detail outcome 绝不熔断整组)。
    返回 (handles, first_failure_exc)。"""
    prefetch = _AlbumLinkPrefetchWindow(link_provider, context.album_pipeline_slots or 1)
    prefetch.advance(pending, 0)

    handles: list[tuple[int, Any]] = []
    first_failure_exc: Exception | None = None
    try:
        for pos, (idx, item) in enumerate(pending):
            _raise_if_control_signal(context)
            try:
                media = await _upload_one_album_member(
                    engine,
                    client,
                    context,
                    pos=pos,
                    item=item,
                    prefetch=prefetch,
                    progress=progress,
                    link_provider=link_provider,
                )
            except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
                raise
            except Exception as item_exc:
                outcomes[idx] = TransportOutcome(
                    message_ids=[],
                    error_detail=str(item_exc),
                    client_label=context.account_display,
                )
                if first_failure_exc is None:
                    first_failure_exc = item_exc
                continue
            handles.append((idx, media))
    finally:
        prefetch.close()
    return handles, first_failure_exc


async def _send_album_handles(
    client: Any,
    items: list[PublishItem],
    context: PublishContext,
    *,
    handles: list[tuple[int, Any]],
    outcomes: list[TransportOutcome],
    last_speed: float,
) -> list[TransportOutcome]:
    """成功子集发送 (R3 矩阵): ≥2 组发 Album / ==1 强制单张 Photo (Telethon 1 元素列表静默降级显式堵死);
    组发/单发失败: 该段成员统一记 error_detail outcome (不 raise)。"""
    try:
        if len(handles) >= 2:
            sent = await client.send_file(
                context.target_chat_id,
                [m for _, m in handles],
                caption=None,
                force_document=False,
            )
        else:
            idx0, media0 = handles[0]
            sent = await client.send_file(
                context.target_chat_id,
                media0,
                caption=items[idx0].name or items[idx0].item_id,
                force_document=False,
            )
    except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
        raise
    except FloodWaitError as fw_err:
        raise RetryableTaskError(f"相册组发遇 FloodWait: {fw_err}") from fw_err
    except Exception as send_exc:
        for idx, _media in handles:
            outcomes[idx] = TransportOutcome(
                message_ids=[],
                error_detail=str(send_exc),
                client_label=context.account_display,
            )
        return outcomes
    sent_list = sent if isinstance(sent, list) else [sent]
    for pos, (idx, _item) in enumerate(handles):
        msg_id = int(getattr(sent_list[pos], "id", 0)) if pos < len(sent_list) else 0
        if msg_id:
            outcomes[idx] = TransportOutcome(
                message_ids=[msg_id],
                speed_mb_s=last_speed,
                client_label=context.account_display,
            )
    return outcomes


async def _try_edit_pikpak_remux(
    engine: Any,
    client: Any,
    item: PublishItem,
    context: PublishContext,
    msg_id: int,
) -> TransportOutcome | None:
    if not is_mpegts_remux_candidate(item.name, context):
        return None
    try:
        return await _remux_and_upload_pikpak_ts(engine, client, item, context, msg_id=int(msg_id))
    except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
        raise
    except Exception as exc:
        raise TransportError(str(exc), item=item, client_label=context.account_display, cause=exc) from exc


async def edit_item(engine: Any, item: PublishItem, context: PublishContext, msg_id: int) -> TransportOutcome:
    """PikPak 原地回填 (单分卷条目; S4/)。"""
    if item.size_bytes <= 0:
        raise TransportError("PikPak 文件大小未知或为零，无法回填", item=item, client_label=context.account_display)
    threshold = context.video_split_threshold_bytes or ss_mod.derive_split_threshold(context.is_premium)
    if len(ss_mod.VirtualStreamSplitter.split(item.size_bytes, threshold)) > 1:
        raise RuntimeError("多分卷条目不支持原地回填 (编排层将回退常规发送)")

    client = context.userbot_client
    if client is None:
        client = engine._registry.userbot
    if client is None:
        raise PikPakPublishFatalError("userbot")
    if engine._link_provider is None:
        raise PikPakPublishFatalError("unknown", "PikPakStreamTransportEngine 缺少 link_provider 注入")

    remux_outcome = await _try_edit_pikpak_remux(engine, client, item, context, msg_id)
    if remux_outcome is not None:
        return remux_outcome

    last_speed = 0.0

    raw_url_provider = _make_pikpak_url_provider(engine._link_provider, item.item_id, strict=False)
    shared_initial_url = await _maybe_enrich_pikpak_video_metadata(
        item,
        context,
        _pop_engine_prefetched_link(engine, item.item_id),
        raw_url_provider,
        label="回填",
    )
    initial_url_consumed = False

    async def _edit_provider_url_provider() -> str:
        nonlocal shared_initial_url, initial_url_consumed
        if not initial_url_consumed:
            initial_url_consumed = True
            if shared_initial_url:
                return shared_initial_url
        url = await raw_url_provider()
        if url:
            shared_initial_url = url
        return url

    inner_provider = scp_mod.PikPakStreamChunkProvider(
        file_id=item.item_id,
        file_name=item.name or item.item_id,
        total_bytes=item.size_bytes,
        url_provider=_edit_provider_url_provider,
    )
    provider = _SpeedMeasuringProvider(inner_provider)
    meta_path = get_meta_path(context.task_id or "pikpak-direct", item.item_id, 1)

    def backfill_hook(
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

    uploaded_url: str | None = None
    try:
        try:
            media = await ft_mod.upload_file_parallel(
                client,
                f"pikpak://{item.item_id}",
                progress_callback=backfill_hook,
                concurrency=context.upload_concurrency or 2,
                get_concurrency=context.get_current_upload_concurrency,
                in_flight_per_connection=context.upload_in_flight_per_connection or 2,
                get_in_flight_per_connection=context.get_current_in_flight,
                is_cancelled=context.is_cancelled,
                is_paused=context.is_paused,
                chunk_provider=provider,
                meta_path=meta_path,
            )
            cur = getattr(inner_provider, "_current_url", None)
            if isinstance(cur, str) and cur:
                uploaded_url = cur
        finally:
            await provider.aclose()

        _raise_if_control_signal(context)

        async def _edit_thumb_url_provider() -> str:
            nonlocal shared_initial_url
            if uploaded_url:
                return uploaded_url
            if shared_initial_url:
                return shared_initial_url
            shared_initial_url = await raw_url_provider()
            return shared_initial_url

        thumb_bytes = await engine._fetch_engine_thumb(item, _edit_thumb_url_provider, log_label="PikPak 回填")
        _raise_if_control_signal(context)

        edit_kwargs: dict[str, Any] = {
            "file": media,
            "text": item.name or item.item_id,
            "attributes": build_media_attributes(item, eligible=True) or None,
        }
        if thumb_bytes:
            edit_kwargs["thumb"] = make_named_thumbnail_stream(thumb_bytes)

        sent = await client.edit_message(
            context.target_chat_id,
            int(msg_id),
            **edit_kwargs,
        )
        return TransportOutcome(
            message_ids=[int(getattr(sent, "id", 0) or int(msg_id))],
            speed_mb_s=last_speed,
            client_label=context.account_display,
        )
    except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
        raise
    except Exception as exc:
        raise TransportError(str(exc), item=item, client_label=context.account_display, cause=exc) from exc
