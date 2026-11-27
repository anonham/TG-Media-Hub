"""统一发布流水线传输引擎 - PikPak fMP4 零落盘动态重分卷通道"""

import asyncio
import hashlib
import io
import json
import logging
import math
import os
import re
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import aiohttp
from telethon.errors import BadMessageError, FloodWaitError, ServerError

from app.core import fmp4_pipe_uploader as fmp4_mod
from app.core import keyframe_prober as kp_mod
from app.core import stream_chunk_provider as scp_mod
from app.core import stream_readahead_proxy as srp_mod
from app.core import stream_splitter as ss_mod
from app.core.e2e_capture import E2ECaptureSession
from app.core.iso_bmff import fragmented as frag_mod
from app.core.iso_bmff.fragmented import Fmp4PartPlan
from app.core.media_metadata import apply_probed_metadata, probe_remote_video_metadata
from app.core.pikpak.taxonomy import (
    PikPakAuthError,
    PikPakNotFoundError,
    PikPakPublishError,
    PikPakPublishFatalError,
    PikPakPublishRetryableError,
    PikPakQuotaError,
    PikPakTransientError,
    wrap_pikpak_failure,
)
from app.core.pikpak.taxonomy import (
    PikPakRiskControlError as CliPikPakRiskControlError,
)
from app.core.pipeline.engines._common import (
    ProgressReporter,
    _attributes_snapshot,
    _backfill_media_metadata_to_item,
    _has_valid_display_dims,
    _is_huge_file_size,
    _maybe_log_codec_compat_hint,
    _raise_if_control_signal,
    _send_file_with_floodwait_retry,
)
from app.core.pipeline.engines._remux import REMUX_TARGET_EXTENSIONS, mpegts_audio_copy_tracks
from app.core.pipeline.engines._types import SendFileKwargs, SplitChannel, SplitVolumePlan, VolumeJob
from app.core.pipeline.interfaces import DEFAULT_CLIENT_REGISTRY, ClientRegistryPort, TaskStorePort
from app.core.pipeline.models import PublishContext, PublishItem
from app.core.pipeline.utils import build_media_attributes
from app.core.scheduler import RetryableTaskError, UploadCancelledError, UploadPausedError

logger = logging.getLogger(__name__)


def _compute_stss_keyframe_pts(v_track: Any, probed_metadata: Any) -> list[float] | None:
    """stss 关键帧时间轴计算 (供 probed_metadata 提取段; 无 stss 返回 None)。"""
    if not v_track.stbl.stss:
        return None
    stts_entries = [(e.sample_count, e.sample_delta) for e in v_track.stbl.stts]
    stss_samples = list(v_track.stbl.stss)
    first_cts = 0
    if v_track.stbl.ctts and len(v_track.stbl.ctts) > 0 and v_track.stbl.ctts[0].sample_count > 0:
        first_cts = v_track.stbl.ctts[0].sample_offset
    timescale = v_track.mdhd.timescale if v_track.mdhd.timescale > 0 else 1000
    movie_timescale = probed_metadata.mvhd.timescale if probed_metadata.mvhd.timescale > 0 else 1000
    elst_entries = (
        [(e.segment_duration, e.media_time) for e in v_track.edts.entries]
        if v_track.edts and v_track.edts.entries
        else None
    )
    return kp_mod.Mp4StssProber._compute_keyframe_pts(
        stts_entries=stts_entries,
        stss_samples=stss_samples,
        timescale=timescale,
        elst_entries=elst_entries,
        movie_timescale=movie_timescale,
        first_cts=first_cts,
    )


def _extract_from_probed_metadata(
    item: PublishItem,
    probed_metadata: Any,
    is_mp4_mov: bool,
    total_duration: float,
) -> tuple[float, list[float] | None, str, bool]:
    """Stage 0 透传元数据提取段:
    时长/宽高/编码回填 (经共享回填器) + elst 时长覆盖 + stss 关键帧轴。
    返回 (total_duration, keyframe_pts, audio_codec, copy_audio); 异常 debug 降级。
    """
    audio_codec = (item.audio_codec or "").lower().strip()
    keyframe_pts: list[float] | None = None
    try:
        if math.isfinite(probed_metadata.duration_s) and probed_metadata.duration_s > 0.0:
            total_duration = probed_metadata.duration_s
            item.duration_s = total_duration

        v_track = probed_metadata.video_track
        if v_track is not None:
            # 宽高/视频编码/音频编码回填与 VSSE probe 同构 —— 收敛共享回填器
            _backfill_media_metadata_to_item(item, probed_metadata)

            elst_entries = (
                [(e.segment_duration, e.media_time) for e in v_track.edts.entries]
                if v_track.edts and v_track.edts.entries
                else None
            )
            movie_timescale = probed_metadata.mvhd.timescale if probed_metadata.mvhd.timescale > 0 else 1000
            if elst_entries and movie_timescale > 0:
                elst_dur_s = sum(seg_dur for seg_dur, _ in elst_entries) / movie_timescale
                if elst_dur_s > 0:
                    total_duration = elst_dur_s
                    item.duration_s = elst_dur_s

            keyframe_pts = _compute_stss_keyframe_pts(v_track, probed_metadata)

        a_track = probed_metadata.audio_track
        if a_track is not None and a_track.stbl.stsd.entries:
            a_entry = a_track.stbl.stsd.entries[0]
            acodec = getattr(a_entry, "codec", "") or a_entry.format.decode("ascii", errors="replace")
            if not audio_codec and acodec:
                audio_codec = acodec.lower().strip()
                item.audio_codec = audio_codec
    except Exception as meta_extract_err:
        logger.debug("从 probed_metadata 提取元数据异常 (非致命): %s", meta_extract_err)
    is_aac = audio_codec in ("aac", "mp4a")
    return total_duration, keyframe_pts, audio_codec, bool(is_aac and is_mp4_mov)


async def _probe_mkv_cues(url: str, item: PublishItem) -> tuple[float, list[float] | None]:
    """MKV/WebM Cues 探测段: 关键帧轴 + 时长回填; 异常 debug 静默。返回 (total_duration, keyframe_pts)。"""
    total_duration = 0.0
    keyframe_pts: list[float] | None = None
    try:
        cues_res = await kp_mod.MkvCuesProber.probe_from_url(url)
        if cues_res.has_cues:
            keyframe_pts = cues_res.keyframe_pts_s
        if math.isfinite(cues_res.duration_s) and cues_res.duration_s > 0.0:
            total_duration = cues_res.duration_s
        if not _has_valid_display_dims(item.width, item.height) and _has_valid_display_dims(
            cues_res.width, cues_res.height
        ):
            item.width = cues_res.width
            item.height = cues_res.height
        if not item.video_codec and cues_res.video_codec:
            item.video_codec = cues_res.video_codec
        if not item.audio_codec and cues_res.audio_codec:
            item.audio_codec = cues_res.audio_codec
    except Exception as probe_err:
        logger.debug("MkvCuesProber 探测异常 (非致命): %s", probe_err)
    return total_duration, keyframe_pts


async def _probe_mp4_stss(
    url: str, item: PublishItem, audio_codec: str, is_mp4_mov: bool
) -> tuple[float, list[float] | None, str, bool]:
    """MP4/MOV/M4V STSS 探测段: 关键帧轴 + 时长/宽高/编解码回填; 异常 debug 静默。
    返回 (total_duration, keyframe_pts, audio_codec, copy_audio)。"""
    total_duration = 0.0
    keyframe_pts: list[float] | None = None
    try:
        mp4_res = await kp_mod.Mp4StssProber.probe_from_url(url, file_size=item.size_bytes)
        if mp4_res.has_cues:
            keyframe_pts = mp4_res.keyframe_pts_s
        if math.isfinite(mp4_res.duration_s) and mp4_res.duration_s > 0.0:
            total_duration = mp4_res.duration_s
        if not _has_valid_display_dims(item.width, item.height) and _has_valid_display_dims(
            mp4_res.width, mp4_res.height
        ):
            item.width = mp4_res.width
            item.height = mp4_res.height
        if not audio_codec and mp4_res.audio_codec:
            audio_codec = mp4_res.audio_codec.lower().strip()
            item.audio_codec = audio_codec
        if not item.video_codec and mp4_res.video_codec:
            item.video_codec = mp4_res.video_codec
    except Exception as probe_err:
        logger.debug("Mp4StssProber 探测异常 (非致命): %s", probe_err)
    is_aac = audio_codec in ("aac", "mp4a")
    return total_duration, keyframe_pts, audio_codec, bool(is_aac and is_mp4_mov)


async def probe_fmp4_media(
    url: str,
    item: PublishItem,
    context: PublishContext,
    probed_metadata: Any | None = None,
) -> tuple[str, bool, float, list[float] | None]:
    """fMP4 头部 MKV/MP4 探测回填段:
    关键帧/时长/宽高/编解码回填 + 编码提示; 探测值回填 item, 探测失败 logger.debug 静默。
    支持透传 Stage 0 的 probed_metadata 消除二次跨洋 HTTP Range 探测。
    返回 (ext, copy_audio, total_duration, keyframe_pts); 时长兜底 (size / 2MBps, 下限 10s) 在此收口。
    """
    ext = os.path.splitext(item.name)[1].lower()
    audio_codec = (item.audio_codec or "").lower().strip()
    is_mp4_mov = ext in (".mp4", ".mov", ".m4v")
    copy_audio = bool(audio_codec in ("aac", "mp4a") and is_mp4_mov)

    total_duration = float(item.duration_s or 0.0)
    if not math.isfinite(total_duration):
        total_duration = 0.0
    keyframe_pts = None
    probed_dur = 0.0

    if probed_metadata is not None:
        total_duration, keyframe_pts, audio_codec, copy_audio = _extract_from_probed_metadata(
            item, probed_metadata, is_mp4_mov, total_duration
        )
    elif ext in (".mkv", ".webm"):
        probed_dur, keyframe_pts = await _probe_mkv_cues(url, item)
    elif is_mp4_mov:
        probed_dur, keyframe_pts, audio_codec, copy_audio = await _probe_mp4_stss(url, item, audio_codec, is_mp4_mov)
    elif ext in REMUX_TARGET_EXTENSIONS and item.audio_codecs is None:
        meta = await probe_remote_video_metadata(
            url, ext, file_size=item.size_bytes, is_cancelled=context.is_cancelled, is_paused=context.is_paused
        )
        apply_probed_metadata(item, meta)
        if total_duration <= 0.0:
            total_duration = meta.duration_s

    if probed_dur > 0.0:
        total_duration = probed_dur
        item.duration_s = probed_dur

    await _maybe_log_codec_compat_hint(item, context)

    if total_duration <= 0.0:
        total_duration = max(10.0, item.size_bytes / (2 * 1024 * 1024))
    return ext, copy_audio, total_duration, keyframe_pts


def build_fmp4_send_kwargs(
    item: PublishItem, uploader: Any, plan: SplitVolumePlan, actual_dur: float, thumb_bytes: bytes | None
) -> SendFileKwargs:
    """fMP4 单卷发送参数构建: 分卷 caption + 实测时长优先属性 + 宽高链 + 封面注入。"""
    send_kwargs: SendFileKwargs = {
        "caption": ss_mod.format_part_caption(item.name, plan.part_index, plan.total_parts),
        "supports_streaming": True,
    }
    vid_w, vid_h = item.width or 0, item.height or 0
    if not _has_valid_display_dims(vid_w, vid_h):
        vid_w, vid_h = int(uploader.video_width or 0), int(uploader.video_height or 0)
    if not _has_valid_display_dims(vid_w, vid_h):
        vid_w, vid_h = 0, 0
    send_kwargs["attributes"] = build_media_attributes(
        item, duration_s=(actual_dur or plan.duration_sec), width=vid_w, height=vid_h
    )
    if thumb_bytes:
        thumb_stream = io.BytesIO(thumb_bytes)
        thumb_stream.name = "thumbnail.jpg"
        send_kwargs["thumb"] = thumb_stream
    return send_kwargs


async def _attempt_fmp4_part_upload(
    client: Any,
    item: PublishItem,
    plan: Fmp4PartPlan,
    url: str | tuple[str, str] | list[str],
    context: PublishContext,
    capture_session: E2ECaptureSession,
    *,
    threshold: int,
    ext: str,
    copy_audio: bool,
    thumb_bytes: bytes | None,
    prior_bytes: int,
    total_planned: int,
    workers_count: int,
    in_flight_count: int,
    registry: ClientRegistryPort | None,
) -> tuple[int, int, float, str | tuple[str, str] | list[str]]:
    """fMP4 单次分卷尝试体:
    流式上传 → 发送 → 捕获记账; 成功返回 (msg_id, emitted_bytes, speed, url)。"""
    uploader = fmp4_mod.ZeroDiskFmp4StreamUploader(
        client=client,
        max_workers=workers_count,
        queue_size=max(12, workers_count * in_flight_count * 2),
        in_flight_per_connection=in_flight_count,
        capture_sink=capture_session.make_fmp4_sink(plan.part_name, fmp4_mod.PART_SIZE),
    )

    res_file, actual_dur = await uploader.stream_to_telegram(
        pikpak_url=url,
        start_sec=plan.start_sec,
        end_sec=plan.end_sec,
        file_name=plan.part_name,
        copy_audio=copy_audio,
        part_index=plan.part_index,
        max_part_bytes=threshold,
        progress_callback=ProgressReporter(item, context, log_label="fMP4").make_split_part_hook(
            plan, prior_bytes, tolerant_args=False
        ),
        is_cancelled=context.is_cancelled,
        is_paused=context.is_paused,
        is_mkv=bool(ext in (".mkv", ".webm")),
        is_blind=plan.is_blind,
        audio_copy_tracks=mpegts_audio_copy_tracks(item) if ext in REMUX_TARGET_EXTENSIONS else None,
    )
    last_speed = uploader.last_speed
    raw_emitted = uploader.total_emitted_bytes

    send_kwargs = build_fmp4_send_kwargs(
        item,
        uploader,
        plan,
        actual_dur,
        thumb_bytes,
    )

    sent, last_sf_err = await _send_file_with_floodwait_retry(
        client,
        context.target_chat_id,
        res_file,
        send_kwargs,
        retry_other_errors=True,
        log_label="fMP4 send_file",
        registry=registry,
        context=context,
    )

    msg_id = getattr(sent, "id", 0)
    if not msg_id:
        raise PikPakPublishFatalError(
            "telegram_send_failed",
            f"fMP4 Part {plan.part_index}/{plan.total_parts} 消息发送失败 (未获取到有效 msg_id): {last_sf_err}",
        )
    sent_attrs = _attributes_snapshot(send_kwargs.get("attributes"))
    if sent_attrs is not None:
        sent_attrs["actual_dur"] = actual_dur
    capture_session.record_part_sent(
        plan.part_name,
        msg_id=int(msg_id),
        channel="fmp4",
        plan=plan,
        sent_attributes=sent_attrs,
    )

    logger.info(
        "🚀 fMP4 分卷发布成功: Part %d/%d (msg_id=%s, duration=%.2fs, bytes=%d, speed=%.2f MB/s)",
        plan.part_index,
        plan.total_parts,
        msg_id,
        actual_dur or plan.duration_sec,
        raw_emitted,
        last_speed,
    )

    return int(msg_id), (raw_emitted or int(item.size_bytes / total_planned)), last_speed, url


_THROTTLE_SIG_RE = re.compile(r"\b(?:429|502|503|504)\b|Too Many Requests|Service Unavailable")


def _is_throttle_signature_error(pe: Exception) -> bool:
    """底层异常文本含节点频控/过载特征 (503/429/502/504/Too Many Requests/Service Unavailable)。"""
    return bool(_THROTTLE_SIG_RE.search(str(pe)))


async def _sleep_backoff_with_control_watch(context: PublishContext, backoff: float, plan: Fmp4PartPlan) -> None:
    """重试退避睡眠, 保持 poll 粒度响应控制信号。"""
    end_time = time.monotonic() + backoff
    while time.monotonic() < end_time:
        _raise_if_control_signal(context)
        step = min(0.5, max(0.0, end_time - time.monotonic()))
        if step > 0:
            await asyncio.sleep(step)


async def stream_fmp4_volume(
    client: Any,
    item: PublishItem,
    plan: Fmp4PartPlan,
    url: str | tuple[str, str] | list[str],
    context: PublishContext,
    capture_session: E2ECaptureSession,
    *,
    threshold: int,
    ext: str,
    copy_audio: bool,
    thumb_bytes: bytes | None,
    prior_bytes: int,
    total_planned: int,
    workers_count: int,
    in_flight_count: int,
    refresh_url: Callable[[], Awaitable[str | tuple[str, str]]],
    registry: ClientRegistryPort | None = None,
) -> tuple[int, int, float, str | tuple[str, str] | list[str]]:
    """单分卷 fMP4 流式上传 + 发送 (3 次重试; 超限/时长膨胀原样上抛由编排层重规划)。
    单次尝试体拆至 _attempt_fmp4_part_upload, 本函数仅保留重试退避编排。"""
    last_part_exc: Exception | None = None

    for part_attempt in range(3):
        _raise_if_control_signal(context)

        if part_attempt > 0:
            logger.warning(
                "🔁 fMP4 分卷 Part %d/%d 触发重新索链并重跑 (尝试 %d/3): file_id=%s",
                plan.part_index,
                plan.total_parts,
                part_attempt + 1,
                item.item_id,
            )
            try:
                url = await refresh_url()
            except Exception as link_err:
                logger.warning("重新获取直链异常: %s", link_err)

        try:
            return await _attempt_fmp4_part_upload(
                client,
                item,
                plan,
                url,
                context,
                capture_session,
                threshold=threshold,
                ext=ext,
                copy_audio=copy_audio,
                thumb_bytes=thumb_bytes,
                prior_bytes=prior_bytes,
                total_planned=total_planned,
                workers_count=workers_count,
                in_flight_count=in_flight_count,
                registry=registry,
            )
        except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
            raise
        except (fmp4_mod.PartOversizeError, fmp4_mod.PartDurationInflatedError):
            raise
        except (
            scp_mod.PikPakThrottledError,
            scp_mod.PikPakRiskControlError,
            CliPikPakRiskControlError,
            PikPakAuthError,
            PikPakQuotaError,
            PikPakNotFoundError,
            scp_mod.PikPakNotFoundError,
            PikPakPublishError,
        ) as risk_or_fatal_exc:
            capture_session.discard_part_sink(plan.part_name)
            raise wrap_pikpak_failure(risk_or_fatal_exc) from risk_or_fatal_exc
        except Exception as pe:
            last_part_exc = pe
            if _is_throttle_signature_error(pe):
                capture_session.discard_part_sink(plan.part_name)
                raise wrap_pikpak_failure(
                    scp_mod.PikPakThrottledError(f"fMP4 传输检测到节点频控/过载: {pe}", status=503)
                ) from pe

            logger.warning(
                "⚠️ fMP4 Part %d/%d 传输异常 (尝试 %d/3): %s",
                plan.part_index,
                plan.total_parts,
                part_attempt + 1,
                pe,
            )
            capture_session.discard_part_sink(plan.part_name)
            if part_attempt < 2:
                # 退避阶梯入 BreathingPolicy (生产代码不再感知测试环境/幽灵属性)
                backoff_ladder = context.breathing.retry_backoff_s
                backoff = backoff_ladder[min(part_attempt, len(backoff_ladder) - 1)]
                logger.info(
                    "⏳ fMP4 Part %d/%d 网络抖动退避 %.1fs 后重试...",
                    plan.part_index,
                    plan.total_parts,
                    backoff,
                )
                await _sleep_backoff_with_control_watch(context, backoff, plan)

    if isinstance(
        last_part_exc,
        (
            scp_mod.ChunkFetchTransientError,
            PikPakTransientError,
            scp_mod.PikPakLinkExpiredError,
            RetryableTaskError,
            TimeoutError,
            ConnectionError,
            aiohttp.ClientError,
            ServerError,
            BadMessageError,
        ),
    ):
        category = "link_expired" if isinstance(last_part_exc, scp_mod.PikPakLinkExpiredError) else "transient"
        raise PikPakPublishRetryableError(
            category,
            f"Part {plan.part_index}/{plan.total_parts} 经历 3 次重试后仍然失败: {last_part_exc}",
            retry_after=getattr(last_part_exc, "retry_after", None),
        ) from last_part_exc

    raise PikPakPublishFatalError(
        "fmp4_part_failed", f"Part {plan.part_index}/{plan.total_parts} 经历 3 次重试后仍然失败: {last_part_exc}"
    ) from last_part_exc


async def replan_fmp4_after_oversize(
    client: Any,
    context: PublishContext,
    capture_session: E2ECaptureSession,
    plans: Sequence[SplitVolumePlan],
    delivered_message_ids: list[int],
    replan_count: int,
    max_replan_attempts: int,
    last_part_exc: Exception | None,
    get_fresh_url: Callable[[], Awaitable[str]],
    item_id: str,
    task_store: TaskStorePort,
) -> tuple[int, str]:
    """超限重规划段: 预算判定 + 已发卷物理回滚 + 强制分卷数递增 + 直链刷新。"""
    if replan_count > max_replan_attempts:
        raise PikPakPublishFatalError(
            "fmp4_part_failed", f"分卷超限重规划尝试 {max_replan_attempts} 次后仍未能自愈: {last_part_exc}"
        ) from last_part_exc

    if delivered_message_ids:
        logger.warning(
            "🧹 动态重规划：物理回滚已发送的 %d 条分卷消息: %s", len(delivered_message_ids), delivered_message_ids
        )
        try:
            await client.delete_messages(context.target_chat_id, list(delivered_message_ids))
        except FloodWaitError as fw_err:
            raise RetryableTaskError(
                f"重规划回滚遇 FloodWait ({fw_err.seconds}s)",
                retry_after=float(fw_err.seconds or 60),
            ) from fw_err
        except Exception as del_err:
            logger.error("回滚孤儿消息异常: %s", del_err)
            raise RetryableTaskError(f"重规划回滚已发分卷失败: {del_err}") from del_err
        await task_store.clear_part_checkpoint(context.task_id, item_id)
        delivered_message_ids.clear()
        capture_session.mark_parts_discarded("fmp4 replan rollback")

    forced_parts = len(plans) + 1
    logger.info(
        "🔄 动态重规划分卷数由 %d 增加至 %d (重规划尝试 %d/%d)",
        len(plans),
        forced_parts,
        replan_count,
        max_replan_attempts,
    )
    url: str = ""
    try:
        url = await get_fresh_url()
    except Exception as link_err:
        logger.debug("fMP4 重规划刷新直链异常 (非致命): %s", link_err)
    return forced_parts, url


def compute_fmp4_plan_signature(
    plans: Sequence[Fmp4PartPlan], *, audio_copy_tracks: tuple[int, ...] | None = None
) -> str:
    """计算 fMP4 分卷切分边界的确定性签名 (含 part_index/start_sec/end_sec/is_blind)。

    防止断点恢复时因关键帧探测失败退化为等分盲切 (且分卷总数巧合一致) 而按错位边界续传拼接。
    """
    payload = [
        [int(p.part_index), round(float(p.start_sec), 6), round(float(p.end_sec), 6), bool(p.is_blind)] for p in plans
    ]
    # Saved TS volumes from the old single-track path must not be mixed with multitrack volumes.
    signature_payload = (
        payload if audio_copy_tracks is None else {"parts": payload, "audio_copy_tracks": audio_copy_tracks}
    )
    encoded = json.dumps(signature_payload, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


class Fmp4SplitChannel(SplitChannel):
    """fMP4 动态分卷与管道流式传输通道实现。"""

    channel_name: str = "fmp4"
    channel_label: str = "fMP4"

    def __init__(
        self,
        item: PublishItem,
        context: PublishContext,
        threshold: int,
        get_fresh_url: Callable[[], Awaitable[str]],
        initial_url: str,
        ext: str,
        copy_audio: bool,
        total_duration: float,
        keyframe_pts: list[float] | None,
        thumb_bytes: bytes | None,
        pinned_regions: list[tuple[int, bytes]] | None = None,
        use_paged_proxy: bool = False,
        registry: ClientRegistryPort | None = None,
        estimated_output_bytes: int | None = None,
    ) -> None:
        self.item = item
        self.context = context
        self.threshold = threshold
        self.get_fresh_url = get_fresh_url
        self.current_url = initial_url
        self.ext = ext
        self.copy_audio = copy_audio
        self.total_duration = total_duration
        self.keyframe_pts = keyframe_pts
        self.thumb_bytes = thumb_bytes
        self.pinned_regions = pinned_regions
        self.use_paged_proxy = use_paged_proxy
        self.estimated_output_bytes = estimated_output_bytes
        # 账号池端口 (FloodWait 冷却标记), 默认 = 现状单例适配器
        self._registry = registry if registry is not None else DEFAULT_CLIENT_REGISTRY

        self.workers_count = max(2, min(int(context.upload_concurrency or 2), 8))
        self.in_flight_count = max(1, min(int(context.upload_in_flight_per_connection or 1), 4))
        self.proxy: Any | None = None
        self.inner_provider: Any | None = None
        self.stream_input_url: str | tuple[str, str] | list[str] = initial_url
        self.current_plans: list[Fmp4PartPlan] = []

    async def init_proxy(self) -> None:
        """按需初始化本地回环代理。"""
        if self.pinned_regions is not None or self.use_paged_proxy:
            is_huge = _is_huge_file_size(self.item.size_bytes)
            effective_prefetch = (
                min(2, max(1, self.context.get_current_prefetch_concurrency()))
                if is_huge
                else (self.context.prefetch_concurrency or 3)
            )
            self.inner_provider = scp_mod.PikPakStreamChunkProvider(
                file_id=self.item.item_id,
                file_name=self.item.name or self.item.item_id,
                total_bytes=self.item.size_bytes,
                url_provider=self.get_fresh_url,
                initial_url=self.current_url,
                prefetch_concurrency=effective_prefetch,
            )
            self.proxy = srp_mod.LocalPagedRangeProxy(
                provider=self.inner_provider,
                pinned_regions=self.pinned_regions,
            )
            await self.proxy.start()
            self.stream_input_url = (self.proxy.video_url, self.proxy.audio_url)

    async def refresh_url(self, prefetched_url: str | None = None) -> str | tuple[str, str]:
        """获取或应用新直链: 代理模式下热更 inner_provider 并返回稳定的回环 URL 对。"""
        fresh = prefetched_url if prefetched_url is not None else await self.get_fresh_url()
        self.current_url = fresh
        if self.proxy is not None and self.inner_provider is not None:
            self.inner_provider.update_url(fresh)
            return (self.proxy.video_url, self.proxy.audio_url)
        return fresh

    def plan_volumes(self, force_parts: int | None = None) -> list[Fmp4PartPlan]:
        plans = frag_mod.plan_video_fmp4_parts(
            total_bytes=max(self.item.size_bytes, self.estimated_output_bytes or 0),
            threshold_bytes=self.threshold,
            total_duration_s=self.total_duration,
            keyframe_pts_s=self.keyframe_pts,
            file_name=self.item.name,
            force_num_parts=force_parts,
        )
        self.current_plans = plans
        return plans

    def get_plan_signature(self, plans: Sequence[SplitVolumePlan]) -> str | None:
        if not plans or not all(isinstance(p, Fmp4PartPlan) for p in plans):
            return None
        return compute_fmp4_plan_signature(
            [p for p in plans if isinstance(p, Fmp4PartPlan)],
            audio_copy_tracks=mpegts_audio_copy_tracks(self.item) if self.ext in REMUX_TARGET_EXTENSIONS else None,
        )

    def get_cached_volume_bytes(self, plan_idx: int, plan: SplitVolumePlan) -> int:
        # Fmp4PartPlan 无单卷体积字段: 按均值估算 (与原 getattr 缺省回退语义逐字等价)
        return int(self.item.size_bytes // max(1, len(self.current_plans)))

    async def prepare_part(self, plan_idx: int, plan: SplitVolumePlan) -> None:
        if plan_idx > 0:
            self.stream_input_url = await self.refresh_url()

    async def stream_part(
        self,
        job: VolumeJob,
        client: Any,
        context: PublishContext,
        capture_session: E2ECaptureSession,
        reporter: ProgressReporter,
    ) -> tuple[int, int, float]:
        msg_id, emitted_bytes, volume_speed, new_url = await stream_fmp4_volume(
            client=client,
            item=self.item,
            plan=self.current_plans[job.part_index - 1],
            url=self.stream_input_url,
            context=context,
            capture_session=capture_session,
            threshold=self.threshold,
            ext=self.ext,
            copy_audio=self.copy_audio,
            thumb_bytes=self.thumb_bytes,
            prior_bytes=job.prior_bytes,
            total_planned=job.total_parts,
            workers_count=self.workers_count,
            in_flight_count=self.in_flight_count,
            refresh_url=self.refresh_url,
            registry=self._registry,
        )
        self.stream_input_url = new_url
        return msg_id, emitted_bytes, volume_speed

    async def handle_oversize(
        self,
        client: Any,
        context: PublishContext,
        capture_session: E2ECaptureSession,
        plans: Sequence[SplitVolumePlan],
        delivered_message_ids: list[int],
        replan_count: int,
        max_replan_attempts: int,
        exc: Exception,
        item_id: str,
        task_store: TaskStorePort,
    ) -> int:
        forced_parts, fresh_url = await replan_fmp4_after_oversize(
            client=client,
            context=context,
            capture_session=capture_session,
            plans=plans,
            delivered_message_ids=delivered_message_ids,
            replan_count=replan_count,
            max_replan_attempts=max_replan_attempts,
            last_part_exc=exc,
            get_fresh_url=self.get_fresh_url,
            item_id=item_id,
            task_store=task_store,
        )
        if fresh_url:
            self.stream_input_url = await self.refresh_url(fresh_url)
        return int(forced_parts)

    async def aclose(self) -> None:
        if self.proxy is not None:
            try:
                await self.proxy.close()
            except Exception as p_stop_err:
                logger.debug("关闭 LocalPagedRangeProxy 异常 (非致命): %s", p_stop_err)
        if self.inner_provider is not None:
            try:
                await self.inner_provider.aclose()
            except Exception as p_close_err:
                logger.debug("关闭 inner_provider 异常 (非致命): %s", p_close_err)
