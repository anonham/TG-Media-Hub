"""统一发布流水线传输引擎 - PikPak VSSE 零重编码虚拟分卷通道"""

import asyncio
import io
import logging
import struct
from collections.abc import Sequence
from typing import Any

from app.core import fast_telethon as ft_mod
from app.core import stream_chunk_provider as scp_mod
from app.core import stream_readahead_proxy as srp_mod
from app.core import stream_splitter as ss_mod
from app.core import vsse_audio_sidecar as vas_mod
from app.core import vsse_provider as vp_mod
from app.core.e2e_capture import E2ECaptureSession
from app.core.iso_bmff import box_parser as bp_mod
from app.core.iso_bmff import box_synthesizer as bs_mod
from app.core.iso_bmff import exceptions as ib_exc
from app.core.pikpak.taxonomy import PikPakPublishFatalError
from app.core.pipeline.engines._common import (
    ProgressReporter,
    _attach_probe_context,
    _attributes_snapshot,
    _backfill_media_metadata_to_item,
    _discard_stale_upload_meta_sync,
    _extract_video_display_dims,
    _has_valid_display_dims,
    _is_huge_file_size,
    _raise_if_control_signal,
    _safe_invoke_engine_progress,
    _send_file_with_floodwait_retry,
)
from app.core.pipeline.engines._types import SendFileKwargs, SplitChannel, SplitVolumePlan, VolumeJob
from app.core.pipeline.engines.exceptions import VsseProbeContext, VsseProbeError
from app.core.pipeline.interfaces import DEFAULT_CLIENT_REGISTRY, ClientRegistryPort, TaskStorePort
from app.core.pipeline.models import PublishContext, PublishItem
from app.core.pipeline.utils import (
    build_media_attributes,
    get_meta_path,
    is_file_parts_invalid,
)
from app.core.scheduler import UploadCancelledError, UploadPausedError
from app.core.vsse_provider import VsseVolumePlan

logger = logging.getLogger(__name__)

# 前置/后置 moov 元数据单次读取上限 (32 MiB, 防畸形 MP4 声明超大 box 导致内存 OOM / CWE-400)
MAX_FRONT_MOOV_BYTES = 32 * 1024 * 1024


async def _read_and_scan_boxes(
    head_bytes: bytes,
    provider: Any,
    file_size: int,
) -> list[Any]:
    """读取并扫描 MP4 顶层 box (probe_range 线程安全桥接)。"""
    probe_loop = asyncio.get_running_loop()

    def _probe_range(offset: int, length: int) -> bytes:
        chunk: bytes = asyncio.run_coroutine_threadsafe(provider.read_chunk(offset, length), probe_loop).result(
            timeout=30.0
        )
        return chunk

    try:
        return await asyncio.to_thread(bp_mod.scan_top_level_boxes, head_bytes, file_size, _probe_range)
    except (ib_exc.IsoBmffError, UploadCancelledError, UploadPausedError, asyncio.CancelledError):
        raise
    except Exception as scan_err:
        logger.debug("VSSE 顶层 Box 扫描跳过: %s", scan_err)
        return []


async def _resolve_front_moov(
    boxes: list[Any],
    head_bytes: bytes,
    provider: Any,
    file_size: int,
) -> tuple[int, bytes | None]:
    """检查前置 moov 并提取数据。"""
    moov_header = next((b for b in boxes if b.box_type == b"moov"), None)
    if moov_header is None:
        return 0, None
    abs_moov_start = moov_header.offset
    if moov_header.offset + moov_header.size <= len(head_bytes):
        return abs_moov_start, head_bytes[moov_header.offset : moov_header.offset + moov_header.size]
    if moov_header.offset + moov_header.size <= file_size and moov_header.size <= MAX_FRONT_MOOV_BYTES:
        fetch_len = min(moov_header.size, file_size - moov_header.offset)
        moov_bytes = await provider.read_chunk(moov_header.offset, fetch_len)
        return abs_moov_start, moov_bytes
    return abs_moov_start, None


async def _resolve_rear_moov_ladder(
    provider: Any,
    file_size: int,
    head_len: int,
) -> tuple[int, bytes | None]:
    """阶梯式搜索文件尾部后置 moov (4MB -> 16MB -> 32MB)。"""
    if file_size <= head_len:
        return 0, None
    tail_bytes = bytearray()
    for tail_budget in (4 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024 * 1024):
        target_tail_len = min(file_size, tail_budget)
        needed_bytes = target_tail_len - len(tail_bytes)
        if needed_bytes <= 0:
            break
        chunk_start = file_size - target_tail_len
        chunk_data = await provider.read_chunk(chunk_start, needed_bytes)
        tail_bytes = bytearray(chunk_data) + tail_bytes
        tail_start = file_size - len(tail_bytes)

        moov_idx = tail_bytes.rfind(b"moov")
        if moov_idx >= 4:
            b_size = struct.unpack_from(">I", tail_bytes, moov_idx - 4)[0]
            if b_size == 1:
                if moov_idx + 12 <= len(tail_bytes):
                    b_size = struct.unpack_from(">Q", tail_bytes, moov_idx + 4)[0]
                else:
                    continue
            elif b_size == 0:
                b_size = file_size - (tail_start + moov_idx - 4)

            rel_start = moov_idx - 4
            abs_moov_start = tail_start + rel_start
            if rel_start + b_size <= len(tail_bytes):
                return abs_moov_start, bytes(tail_bytes[rel_start : rel_start + b_size])
            if b_size <= 32 * 1024 * 1024 and abs_moov_start + b_size <= file_size:
                moov_bytes = await provider.read_chunk(abs_moov_start, b_size)
                return abs_moov_start, moov_bytes
    return 0, None


async def _plan_vsse_mode(
    item: PublishItem,
    metadata: Any,
    threshold: int,
    provider: Any,
    head_bytes: bytes,
    abs_moov_start: int,
    moov_bytes: bytes,
    context: PublishContext | None,
) -> tuple[Any, list[Any], Any | None]:
    """分类 VSSE 路由模式 (Tier 1 PURE_ZERO_COPY / Tier 2 HYBRID_AUDIO_SIDECAR) 并生成切片规划。"""
    mode = bp_mod.classify_vsse_mode(metadata)
    logger.info("VSSE tier routing: file=%s, mode=%s", item.name, mode.value)

    plans = None
    bundle = None
    effective_metadata = metadata

    if mode == bp_mod.VsseRoutingMode.PURE_ZERO_COPY:
        try:
            plans = vp_mod.plan_vsse_volumes(
                meta=metadata,
                threshold_bytes=threshold,
                file_name=item.name or item.item_id,
                sidecar_audio=False,
            )
        except ib_exc.NonInterleavedMp4Error as nie:
            logger.info(
                "VSSE detected non-interleaved MP4 (%s), upgrading to HYBRID_AUDIO_SIDECAR",
                nie,
            )
            mode = bp_mod.VsseRoutingMode.HYBRID_AUDIO_SIDECAR

    if mode == bp_mod.VsseRoutingMode.HYBRID_AUDIO_SIDECAR:
        try:
            sidecar_kwargs: dict[str, Any] = {}
            if context is not None:
                sidecar_kwargs["check_control_fn"] = lambda: _raise_if_control_signal(context)

                def _emit_sidecar_tick() -> None:
                    if not context.progress_callback:
                        return
                    cur_done = context.completed_files_count
                    tot_files = context.total_files_count or 1
                    overall_done = context.completed_bytes
                    overall_tot = context.total_bytes_sum or item.size_bytes
                    cur_item = min(tot_files, cur_done + 1)
                    dl_spd = provider.download_speed_mb_s
                    pct = (cur_done / tot_files * 100.0) if tot_files else 0.0
                    _safe_invoke_engine_progress(
                        context,
                        cur_item,
                        tot_files,
                        0.0,
                        0.0,
                        f"{item.name} (正在提取旁路音频...)",
                        label="VSSE sidecar progress callback exception",
                        concurrency=1,
                        concurrency_total=context.upload_concurrency or 2,
                        percent=pct,
                        force_db=False,
                        pikpak_download_speed_mb_s=dl_spd,
                        total_bytes_done=overall_done,
                        total_bytes_all=overall_tot,
                    )

                sidecar_kwargs["on_tick_fn"] = _emit_sidecar_tick
            bundle = await vas_mod.build_sidecar_audio_bundle(
                metadata=metadata,
                provider=provider,
                head_bytes=head_bytes,
                moov_offset=abs_moov_start,
                moov_bytes=moov_bytes,
                **sidecar_kwargs,
            )
            effective_metadata = bundle.hybrid_metadata
            plans = vp_mod.plan_vsse_volumes(
                meta=effective_metadata,
                threshold_bytes=threshold,
                file_name=item.name or item.item_id,
                sidecar_audio=True,
            )
        except (
            UploadCancelledError,
            UploadPausedError,
            asyncio.CancelledError,
            scp_mod.PikPakThrottledError,
            scp_mod.PikPakRiskControlError,
            scp_mod.PikPakNotFoundError,
            scp_mod.PikPakLinkExpiredError,
        ):
            raise
        except Exception as sidecar_err:
            logger.warning(
                "VSSE Tier 2 旁路音频提取/规划异常 (%s: %s)，触发熔断回退至 fMP4 全能保底发布通道",
                type(sidecar_err).__name__,
                sidecar_err,
            )
            raise ib_exc.IsoBmffError(f"VSSE Tier 2 sidecar failed: {sidecar_err}") from sidecar_err

    if plans is None:
        raise ib_exc.IsoBmffError(f"VSSE 未能生成有效分卷规划: file_id={item.item_id}")

    return effective_metadata, plans, bundle


def _synthesize_all_volume_headers(
    plans: list[Any],
    metadata: Any,
    bundle: Any | None,
) -> list[bytes]:
    """为全部计划分卷合成 faststart 信封头。"""
    synthesized_headers = []
    for plan in plans:
        sidecar_slice = bundle.slice_bytes(plan.a_o_min, plan.a_o_max) if bundle is not None else None
        hdr = bs_mod.synthesize_faststart_envelope_header(
            metadata=metadata,
            ftyp_bytes=vp_mod.DEFAULT_FTYP_BOX,
            v_start=plan.v_start,
            v_end=plan.v_end,
            a_start=plan.a_start,
            a_end=plan.a_end,
            o_min=plan.envelope_start,
            o_max=plan.envelope_end,
            payload_len=plan.envelope_length,
            sidecar_audio_slice=sidecar_slice,
            a_o_min=plan.a_o_min,
        )
        synthesized_headers.append(hdr)

    if synthesized_headers and plans:
        bp_mod.scan_top_level_boxes(synthesized_headers[0], len(synthesized_headers[0]) + plans[0].payload_len)

    return synthesized_headers


async def probe_vsse_envelope(
    item: PublishItem,
    threshold: int,
    provider: Any,
    context: PublishContext | None = None,
) -> tuple[Any, list[VsseVolumePlan], list[bytes]]:
    """VSSE 探测与规划段 (圈复杂度低内聚协调器):
    顶层 box 扫描 → moov 定位 → 解析校验 → 三级流式分流 (Tier 1/Tier 2) → 分卷规划 → 信封头合成。
    """
    metadata = None
    pinned_regions: list[tuple[int, bytes]] | None = None
    try:
        head_size = min(item.size_bytes, 128 * 1024)
        head_bytes = await provider.read_chunk(0, head_size)
        pinned_regions = [(0, head_bytes)] if head_bytes else []

        boxes = await _read_and_scan_boxes(head_bytes, provider, item.size_bytes)
        abs_moov_start, moov_bytes = await _resolve_front_moov(boxes, head_bytes, provider, item.size_bytes)

        if moov_bytes is None:
            abs_moov_start, moov_bytes = await _resolve_rear_moov_ladder(provider, item.size_bytes, len(head_bytes))

        if not moov_bytes:
            probe_ctx = VsseProbeContext(probed_metadata=None, pinned_regions=pinned_regions)
            raise VsseProbeError(
                f"无法在 MP4 文件中定位到有效的 moov 元数据: file_id={item.item_id}",
                probe_context=probe_ctx,
            )

        pinned_regions = srp_mod.build_pinned_regions(head_bytes, abs_moov_start, moov_bytes)
        metadata = await asyncio.to_thread(bp_mod.parse_moov, moov_bytes)
        _backfill_media_metadata_to_item(item, metadata)

        effective_metadata, plans, bundle = await _plan_vsse_mode(
            item, metadata, threshold, provider, head_bytes, abs_moov_start, moov_bytes, context
        )
        synthesized_headers = await asyncio.to_thread(_synthesize_all_volume_headers, plans, effective_metadata, bundle)
        return effective_metadata, plans, synthesized_headers
    except BaseException as exc:
        _attach_probe_context(exc, metadata, pinned_regions)
        raise


async def stream_vsse_volume(
    client: Any,
    item: PublishItem,
    plan: VsseVolumePlan,
    faststart_header: bytes,
    provider: Any,
    context: PublishContext,
    capture_session: E2ECaptureSession,
    prior_bytes: int,
    thumb_bytes: bytes | None,
    metadata: Any,
    registry: ClientRegistryPort | None = None,
) -> tuple[int, int, float]:
    """单分卷 VSSE 流式上传 + 发送 (2 次断点自愈 + 5 次发送尝试)。"""
    base_vsse_provider: scp_mod.BaseChunkProvider = vp_mod.VirtualEnvelopeChunkProvider(
        file_id=f"{item.item_id}_part{plan.part_index:03d}" if plan.total_parts > 1 else item.item_id,
        file_name=plan.part_name,
        faststart_header=faststart_header,
        source_provider=provider,
        envelope_start=plan.envelope_start,
        envelope_length=plan.envelope_length,
    )

    meta_path = get_meta_path(context.task_id or "pikpak-direct", item.item_id, plan.part_index)

    send_kwargs: SendFileKwargs = {
        "caption": ss_mod.format_part_caption(item.name, plan.part_index, plan.total_parts),
        "supports_streaming": True,
    }
    v_track = metadata.video_track
    vid_w = int(item.width or 0)
    vid_h = int(item.height or 0)
    if not _has_valid_display_dims(vid_w, vid_h):
        vid_w, vid_h = _extract_video_display_dims(v_track) if v_track else (0, 0)
    send_kwargs["attributes"] = build_media_attributes(item, duration_s=plan.duration_sec, width=vid_w, height=vid_h)
    if thumb_bytes:
        thumb_stream = io.BytesIO(thumb_bytes)
        thumb_stream.name = "thumbnail.jpg"
        send_kwargs["thumb"] = thumb_stream

    vsse_provider = base_vsse_provider
    sent = None
    last_sf_err = None
    volume_speed = 0.0
    for attempt in range(2):
        _raise_if_control_signal(context)
        vsse_provider = capture_session.make_vsse_tee(plan, base_vsse_provider, header_len=len(faststart_header))

        try:
            uploaded_media = await ft_mod.upload_file_parallel(
                client,
                f"pikpak://{item.item_id}",
                progress_callback=ProgressReporter(
                    item, context, provider=provider, log_label="VSSE"
                ).make_split_part_hook(plan, prior_bytes, tolerant_args=True),
                concurrency=context.upload_concurrency or 2,
                get_concurrency=context.get_current_upload_concurrency,
                in_flight_per_connection=context.upload_in_flight_per_connection or 2,
                get_in_flight_per_connection=context.get_current_in_flight,
                prefetch_concurrency=(
                    min(2, max(1, context.get_current_prefetch_concurrency()))
                    if _is_huge_file_size(item.size_bytes)
                    else (context.prefetch_concurrency or 3)
                ),
                get_prefetch_concurrency=(
                    (lambda: min(2, max(1, context.get_current_prefetch_concurrency())))
                    if _is_huge_file_size(item.size_bytes)
                    else context.get_current_prefetch_concurrency
                ),
                is_cancelled=context.is_cancelled,
                is_paused=context.is_paused,
                force_fresh=(attempt > 0) or capture_session.active,
                chunk_provider=vsse_provider,
                meta_path=meta_path,
            )
            volume_speed = provider.download_speed_mb_s
            sent, last_sf_err = await _send_file_with_floodwait_retry(
                client,
                context.target_chat_id,
                uploaded_media,
                send_kwargs,
                retry_other_errors=False,
                log_label="VSSE",
                registry=registry,
                context=context,
            )
            break
        except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
            raise
        except Exception as upload_err:
            if is_file_parts_invalid(upload_err) and attempt == 0:
                logger.warning(
                    "⚠️ VSSE 检测到分块缓存失效 (part=%d/%d)，清除断点重试: %s",
                    plan.part_index,
                    plan.total_parts,
                    upload_err,
                )
                capture_session.discard_part_sink(plan.part_name)
                await asyncio.to_thread(_discard_stale_upload_meta_sync, meta_path)
                continue
            raise

    msg_id = getattr(sent, "id", 0)
    if not msg_id:
        raise PikPakPublishFatalError(
            "telegram_send_failed",
            f"VSSE Part {plan.part_index}/{plan.total_parts} 消息发送失败 (未获取到有效 msg_id): {last_sf_err}",
        )
    capture_session.record_part_sent(
        plan.part_name,
        msg_id=int(msg_id),
        sent_attributes=_attributes_snapshot(send_kwargs.get("attributes")),
    )

    logger.info(
        "🚀 VSSE 分卷发布成功: Part %d/%d (msg_id=%s, duration=%.2fs, bytes=%d, speed=%.2f MB/s)",
        plan.part_index,
        plan.total_parts,
        msg_id,
        plan.duration_sec,
        vsse_provider.total_bytes,
        volume_speed,
    )
    return int(msg_id), vsse_provider.total_bytes, volume_speed


class VsseSplitChannel(SplitChannel):
    """VSSE 零重编码虚拟分卷传输通道实现。"""

    channel_name: str = "vsse"
    channel_label: str = "VSSE"

    def __init__(
        self,
        item: PublishItem,
        metadata: Any,
        plans: list[VsseVolumePlan],
        synthesized_headers: list[bytes],
        provider: Any,
        thumb_bytes: bytes | None,
        registry: ClientRegistryPort | None = None,
    ) -> None:
        self.item = item
        self.metadata = metadata
        self.plans = plans
        self.synthesized_headers = synthesized_headers
        self.provider = provider
        self.thumb_bytes = thumb_bytes
        # 账号池端口 (FloodWait 冷却标记), 默认 = 现状单例适配器
        self._registry = registry if registry is not None else DEFAULT_CLIENT_REGISTRY

    def plan_volumes(self, force_parts: int | None = None) -> list[VsseVolumePlan]:
        return self.plans

    def get_plan_signature(self, plans: Sequence[SplitVolumePlan]) -> str | None:
        return None

    def get_cached_volume_bytes(self, plan_idx: int, plan: SplitVolumePlan) -> int:
        # 信封长度属通道私有面: 经自持具体 plan 列表读取 (与传入 plan 为同一对象)
        return len(self.synthesized_headers[plan_idx]) + int(self.plans[plan_idx].envelope_length)

    async def prepare_part(self, plan_idx: int, plan: SplitVolumePlan) -> None:
        return

    async def stream_part(
        self,
        job: VolumeJob,
        client: Any,
        context: PublishContext,
        capture_session: E2ECaptureSession,
        reporter: ProgressReporter,
    ) -> tuple[int, int, float]:
        return await stream_vsse_volume(
            client,
            self.item,
            self.plans[job.part_index - 1],
            self.synthesized_headers[job.part_index - 1],
            self.provider,
            context,
            capture_session,
            job.prior_bytes,
            self.thumb_bytes,
            self.metadata,
            registry=self._registry,
        )

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
        raise NotImplementedError("VSSE 通道不支持分卷超限重规划")

    async def aclose(self) -> None:
        await self.provider.aclose()
