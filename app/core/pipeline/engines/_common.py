"""统一发布流水线传输引擎 - 共享辅助与运行时小件"""

import asyncio
import logging
import os
import random
import time
from collections import OrderedDict, deque
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import aiohttp
from telethon.errors import BadMessageError, FloodWaitError, ServerError

from app.config import settings
from app.core import cdn_transport as cdn_mod
from app.core import redact as redact_mod
from app.core import stream_chunk_provider as scp_mod
from app.core.pikpak.taxonomy import (
    PikPakPublishError,
    PikPakPublishFatalError,
    wrap_pikpak_failure,
)
from app.core.pipeline.engines._types import SendFileKwargs, SplitVolumePlan
from app.core.pipeline.engines.exceptions import VsseProbeContext
from app.core.pipeline.interfaces import (
    DEFAULT_CLIENT_REGISTRY,
    DEFAULT_TASK_STORE,
    ClientRegistryPort,
)
from app.core.pipeline.models import PublishContext, PublishItem
from app.core.pipeline.utils import safe_invoke_progress
from app.core.scheduler import UploadCancelledError, UploadPausedError
from app.core.stream_chunk_provider import BaseChunkProvider

if TYPE_CHECKING:
    from app.core.media_metadata import ProbedMediaMetadata

logger = logging.getLogger(__name__)


# 包4 (4b/E8): 无硬解风险编码标签 (小写子串匹配; 兼顾 API 命名 hevc/vp9/av1 与
# MP4 fourcc hvc1/hev1/vp09/av01 两种形态)
_CODEC_NO_HW_DECODE_TAGS: tuple[str, ...] = (
    "hevc",
    "h265",
    "h.265",
    "hvc1",
    "hev1",
    "vp9",
    "vp09",
    "v_vp9",
    "av1",
    "av01",
)


async def _maybe_log_codec_compat_hint(item: PublishItem, context: PublishContext) -> None:
    """HEVC/H.265/VP9/AV1 部分客户端无硬解, 播放可能软解码卡顿——归因提示落任务日志。

    只提示不改变发布行为; fMP4 通道每文件进入一次即提示一次 (effective codec 由
    调用方在探测后回填 item.video_codec, 探测结果优先、回落 PikPak 元数据);
    落库失败静默 (非致命)。
    """
    vcodec = (item.video_codec or "").lower().strip()
    if not vcodec or not any(tag in vcodec for tag in _CODEC_NO_HW_DECODE_TAGS):
        return
    try:
        # 持久层经 TaskStorePort 默认适配器 (调用期读取, 类级 patch 保持穿透)
        await DEFAULT_TASK_STORE.append_log(
            context.task_id or "",
            f"⚠️ 视频编码 {vcodec}: 部分客户端无硬解, 播放可能出现软解码卡顿 (发布流程不受影响)",
        )
    except Exception as log_err:
        logger.debug("编码兼容提示落库失败 (非致命): %s", log_err)


def _has_valid_display_dims(width: int | None, height: int | None) -> bool:
    return (width or 0) > 0 and (height or 0) > 0


def _extract_video_display_dims(v_track: Any) -> tuple[int, int]:
    """从 Mp4Metadata video_track 计算展示宽高 (仅应用一次 tkhd.matrix 旋转, 不修改原 track 字段)。"""
    from app.core.iso_bmff.box_parser import rotation_degrees_from_matrix, track_display_dimensions

    width = 0
    height = 0
    tkhd = v_track.tkhd
    if tkhd:
        width = int(tkhd.width or (tkhd.width_raw >> 16) or 0)
        height = int(tkhd.height or (tkhd.height_raw >> 16) or 0)
    entries = v_track.stbl.stsd.entries
    entry = entries[0] if entries else None
    sample = int(getattr(entry, "width", 0) or 0), int(getattr(entry, "height", 0) or 0)
    rotation = rotation_degrees_from_matrix(getattr(tkhd, "matrix", None))
    return track_display_dimensions(getattr(v_track, "aperture_dimensions", None), (width, height), sample, rotation)


def _backfill_media_metadata_to_item(item: PublishItem, metadata: Any) -> None:
    """将 moov 探测出的时长、展示宽高和编码信息安全回填至 item (VSSE probe 与 fMP4 共享回填器)。

    自 VsseSplitChannel 侧同名私有函数收敛而来 —— 两个通道的
    probed_metadata (Mp4Metadata) 结构同形, 回填语义逐字一致。
    若 item 已有完整的远端权威宽高则保留; 缺失或仅含半边宽高时以旋转归一后的成对展示宽高补齐。
    """
    if not item.duration_s and metadata.duration_s > 0:
        item.duration_s = metadata.duration_s

    v_track = metadata.video_track
    if v_track:
        if v_track.stbl.stsd.entries and not item.video_codec:
            item.video_codec = getattr(v_track.stbl.stsd.entries[0], "codec", "") or item.video_codec
        if not _has_valid_display_dims(item.width, item.height):
            disp_w, disp_h = _extract_video_display_dims(v_track)
            if disp_w > 0 and disp_h > 0:
                item.width = disp_w
                item.height = disp_h

    a_track = metadata.audio_track
    if a_track and a_track.stbl.stsd.entries and not item.audio_codec:
        a_entry = a_track.stbl.stsd.entries[0]
        item.audio_codec = getattr(a_entry, "codec", "") or a_entry.format.decode("ascii", errors="replace")


def _attributes_snapshot(attrs: list[Any] | None) -> dict[str, Any] | None:
    """提取首个属性元素快照供 E2E manifest 记录 (A5, 读已发送对象不重算)。

    仅取 JSON 安全标量; 空列表返回 None (record_part_sent 不写键, 旧 manifest 兼容)。"""
    if not attrs:
        return None
    first = attrs[0]
    return {
        "duration": getattr(first, "duration", None),
        "w": getattr(first, "w", None),
        "h": getattr(first, "h", None),
        "supports_streaming": bool(getattr(first, "supports_streaming", False)),
    }


def _raise_if_control_signal(context: PublishContext) -> None:
    """取消/暂停控制信号统一检查点 (引擎钩子与分卷/成员循环共用, 语义逐字等价)。"""
    if context.is_cancelled and context.is_cancelled():
        raise UploadCancelledError("上传任务被取消")
    if context.is_paused and context.is_paused():
        raise UploadPausedError("上传任务被暂停")


def _is_huge_file_size(size_bytes: Any) -> bool:
    """安全判定文件是否超过标准账号上限 2000 MiB (1.95 GiB) (容忍 Mock/None 非数值)。"""
    if isinstance(size_bytes, (int, float)):
        return size_bytes > settings.MAX_FILE_SIZE_BYTES
    try:
        return int(size_bytes) > settings.MAX_FILE_SIZE_BYTES
    except (TypeError, ValueError):
        return False


async def precheck_cdn_range(stream_url: str) -> None:
    """微型 Range: bytes=0-0 探测 CDN 是否支持 HTTP 206 Partial Content。

    自门面 `_precheck_cdn_range` 迁入 (门面保留薄静态委托 = 既有 patch seam)。
    """
    if not (stream_url.startswith(("http://", "https://"))):
        return

    try:
        session = await cdn_mod.get_cdn_session()
        headers = {"Range": "bytes=0-0"}
        async with session.get(stream_url, headers=headers, timeout=cdn_mod.cdn_liveness_timeout(10.0)) as resp:
            if resp.status in (403, 410):
                raise wrap_pikpak_failure(
                    scp_mod.PikPakLinkExpiredError(f"PikPak CDN Range 预检直链过期 (HTTP {resp.status})")
                )
            if resp.status == 404:
                raise wrap_pikpak_failure(scp_mod.PikPakNotFoundError("PikPak CDN Range 预检源文件不存在 (HTTP 404)"))
            if resp.status in (429, 502, 503, 504):
                retry_after = scp_mod._parse_retry_after(resp.headers.get("Retry-After"))
                raise wrap_pikpak_failure(
                    scp_mod.PikPakThrottledError(
                        f"PikPak CDN Range 预检触发节点频控/过载 (HTTP {resp.status})",
                        status=resp.status,
                        retry_after=retry_after,
                    )
                )
            content_type = (resp.headers.get("Content-Type") or "").lower()
            if "text/html" in content_type:
                raise wrap_pikpak_failure(
                    scp_mod.PikPakRiskControlError(f"PikPak CDN Range 预检返回 HTML 风控验证页 (HTTP {resp.status})")
                )
            if resp.status == 200:
                raise PikPakPublishFatalError(
                    "cdn_no_range",
                    "PikPak CDN 不支持 HTTP Range 寻址 (返回 200 OK 而非 206 Partial Content), 拒绝流式任务以免全量重复下载",
                )
            if resp.status != 206:
                raise PikPakPublishFatalError("cdn_range_probe_failed", f"PikPak CDN Range 预检异常 HTTP {resp.status}")
            await resp.read()  # 仅在 206 时显式排空微型响应体 (1 字节), 确保底层 TCP 连接放回连接池以供后续探测复用
            logger.info("✅ PikPak CDN HTTP 206 Range 校验通过")
    except PikPakPublishError:
        raise
    except (aiohttp.ClientError, TimeoutError, OSError) as ex:
        safe_err = redact_mod.redact_text(str(ex).replace(stream_url, "<redacted-cdn-url>"))
        raise wrap_pikpak_failure(
            scp_mod.ChunkFetchTransientError(f"PikPak CDN Range 预检连接瞬断: {safe_err}")
        ) from ex
    except Exception as ex:
        safe_err = redact_mod.redact_text(str(ex).replace(stream_url, "<redacted-cdn-url>"))
        raise PikPakPublishFatalError("cdn_range_probe_failed", f"PikPak CDN Range 预检连接失败: {safe_err}") from ex


def _discard_stale_upload_meta_sync(meta_path: str) -> None:
    """FILE_PARTS_INVALID 自愈: 清除失效断点 meta 文件 (同步形态)。

    调用侧经 asyncio.to_thread 卸载, 避免 async 路径阻塞 (ASYNC240)。
    """
    if os.path.exists(meta_path):
        try:
            os.remove(meta_path)
        except Exception as rm_err:
            logger.debug("清理失效断点 meta 文件失败 (非致命): %s", rm_err)


_VsseProbeContext = VsseProbeContext


def _attach_probe_context(exc: BaseException, metadata: Any, pinned_regions: Any) -> None:
    """把 Stage 0 已探测元数据与 L0 内存区域挂载到异常对象上, 供熔断降级读取 (单键
    `probe_context`; SOTA 批2.1 起废除 `_vsse_probe_context` 双键兼容)。

    极少数使用 __slots__ 且未声明 __dict__ 的异常类拒收属性时安全跳过 (非致命)。
    """
    ctx = VsseProbeContext(metadata, pinned_regions)
    try:
        from typing import cast

        cast(Any, exc).probe_context = ctx
    except Exception as attr_err:
        logger.debug("附加 probe_context 异常 (非致命): %s", attr_err)


def _read_probe_context(exc: BaseException) -> VsseProbeContext | None:
    """读取异常对象上挂载的 VSSE probe context (未挂载返回 None)。"""
    return getattr(exc, "probe_context", None)


class ProgressReporter:
    """统一进度计算与分发器，收敛单发、相册、VSSE 及 fMP4 中的进度计算逻辑。"""

    def __init__(
        self,
        item: PublishItem,
        context: PublishContext,
        *,
        provider: Any | None = None,
        log_label: str = "PikPak",
    ) -> None:
        self.item = item
        self.context = context
        self.provider = provider
        self.log_label = log_label

    def compute_file_fraction(
        self,
        part_current: int | float,
        part_total: int | float,
        *,
        plan: SplitVolumePlan | None = None,
        prior_bytes: int = 0,
    ) -> tuple[int, float, int]:
        """计算当前分卷/文件的已传输字节 (file_done)、文件完成比例 (file_fraction)、及预估总字节数。"""
        part_ratio = (float(part_current) / float(part_total)) if (part_total and part_total > 0) else 0.0
        file_done = prior_bytes + int(part_current)
        est_total_file_bytes = max(self.item.size_bytes, file_done)
        if plan is not None and plan.total_parts > 0:
            part_idx = plan.part_index or 1
            tot_parts = plan.total_parts or 1
            file_fraction = (part_idx - 1 + part_ratio) / tot_parts
        else:
            file_fraction = (file_done / est_total_file_bytes) if est_total_file_bytes > 0 else 0.0
        return file_done, file_fraction, est_total_file_bytes

    def emit_progress(
        self,
        *,
        current_bytes: int | float,
        total_bytes: int | float,
        speed: float = 0.0,
        eta: float = 0.0,
        workers: int = 1,
        dc_id: int = 0,
        workers_total: int = 0,
        plan: SplitVolumePlan | None = None,
        prior_bytes: int = 0,
        tolerant_args: bool = False,
        file_name_override: str | None = None,
        pikpak_download_speed: float | None = None,
    ) -> None:
        """执行统一的进度回调分发。"""
        if not self.context.progress_callback:
            return

        file_done, file_fraction, est_total = self.compute_file_fraction(
            current_bytes, total_bytes, plan=plan, prior_bytes=prior_bytes
        )
        cur_done = self.context.completed_files_count
        tot_files = self.context.total_files_count or 1
        overall_bytes_done = self.context.completed_bytes
        overall_bytes_tot = self.context.total_bytes_sum
        overall_percent = ((cur_done + file_fraction) / tot_files * 100.0) if tot_files else 0.0
        cur_item = min(tot_files, cur_done + 1)

        dl_speed = (
            pikpak_download_speed
            if pikpak_download_speed is not None
            else (self.provider.download_speed_mb_s if self.provider is not None else 0.0)
        )

        invoke_kwargs: dict[str, Any] = {
            "concurrency": int(workers) if workers else 1,
            "percent": overall_percent,
            "force_db": False,
            "total_bytes_done": overall_bytes_done + file_done,
            "total_bytes_all": overall_bytes_tot or est_total,
        }
        if dl_speed > 0:
            invoke_kwargs["pikpak_download_speed_mb_s"] = dl_speed
        if tolerant_args:
            invoke_kwargs["dc_id"] = dc_id
            invoke_kwargs["concurrency_total"] = workers_total or (self.context.upload_concurrency or 2)

        _safe_invoke_engine_progress(
            self.context,
            cur_item,
            tot_files,
            speed,
            eta,
            file_name_override or self.item.name,
            label=f"{self.log_label} progress callback exception",
            **invoke_kwargs,
        )

    def make_split_part_hook(
        self,
        plan: SplitVolumePlan,
        prior_bytes: int,
        *,
        tolerant_args: bool = False,
    ) -> Callable[..., Any]:
        """创建用于 upload_file_parallel 或 ZeroDiskFmp4StreamUploader 的分卷进度回调。"""
        if tolerant_args:

            def _cb_tolerant(
                part_current: int | float,
                part_total: int | float,
                speed: float = 0.0,
                eta: float = 0.0,
                *args: Any,
                **kwargs: Any,
            ) -> None:
                _raise_if_control_signal(self.context)
                workers_val, dc_id_val, workers_total_val = _parse_upload_hook_args(args, kwargs)
                self.emit_progress(
                    current_bytes=part_current,
                    total_bytes=part_total,
                    speed=speed,
                    eta=eta,
                    workers=workers_val,
                    dc_id=dc_id_val,
                    workers_total=workers_total_val,
                    plan=plan,
                    prior_bytes=prior_bytes,
                    tolerant_args=True,
                )

            return _cb_tolerant

        def _cb_strict(
            part_current: int | float,
            part_total: int | float,
            speed: float,
            eta: float,
            fname: str,
            workers: int,
        ) -> None:
            _raise_if_control_signal(self.context)
            self.emit_progress(
                current_bytes=part_current,
                total_bytes=part_total,
                speed=speed,
                eta=eta,
                workers=int(workers) if workers else 1,
                plan=plan,
                prior_bytes=prior_bytes,
                tolerant_args=False,
            )

        return _cb_strict

    def make_part_hook(
        self,
        parts: list[Any],
        part_index: int,
        prior_bytes: int,
    ) -> Callable[..., Any]:
        """单文件多 Part 传输进度钩子 (PikPak 直传与相册)。"""

        def hook(
            current: int,
            total: int,
            speed: float,
            eta: float,
            workers: int,
            dc_id: int,
            workers_total: int = 0,
        ) -> None:
            _raise_if_control_signal(self.context)
            if self.context.progress_callback is None:
                return
            part_total = parts[part_index - 1].length
            part_done = min(int(current), part_total)
            file_done = prior_bytes + part_done
            cur_done = self.context.completed_files_count
            tot_files = self.context.total_files_count or 1
            overall_bytes_done = self.context.completed_bytes
            overall_bytes_tot = self.context.total_bytes_sum
            file_fraction = (file_done / self.item.size_bytes) if self.item.size_bytes > 0 else 0.0
            overall_percent = ((cur_done + file_fraction) / tot_files * 100.0) if tot_files else 0.0
            cur_item = min(tot_files, cur_done + 1)
            dl_speed = self.provider.download_speed_mb_s if self.provider is not None else 0.0
            try:
                safe_invoke_progress(
                    self.context.progress_callback,
                    cur_item,
                    tot_files,
                    speed,
                    eta,
                    self.item.name,
                    concurrency=int(workers) if workers else 1,
                    dc_id=dc_id,
                    concurrency_total=workers_total or (self.context.upload_concurrency or 2),
                    percent=overall_percent,
                    force_db=False,
                    pikpak_download_speed_mb_s=dl_speed,
                    total_bytes_done=overall_bytes_done + file_done,
                    total_bytes_all=overall_bytes_tot or self.item.size_bytes,
                )
            except Exception as e:
                logger.debug("%s 上传进度回调异常: %s", self.log_label, e)

        return hook


async def _inter_part_breathing_delay(
    context: PublishContext,
    part_index: int,
    total_parts: int,
    channel_label: str,
) -> None:
    """分卷间呼吸间隔 (Inter-Part Breathing):
    在多卷发布 (VSSE / fMP4 / Raw) 中，当 Part k 发送成功后、Part k+1 开始拉取前，
    强制插入呼吸间隔: T = max(delay_seconds, min_floor_s) + uniform(0, jitter_s) 秒。
    等待期间保持 poll_s 粒度响应取消/暂停控制信号; enabled=False 时零等待直通 (仅查信号)。
    策略经 PublishContext.breathing 注入 —— 生产代码不再感知测试环境,
    呼吸/退避相关幽灵属性已废除 (由 lint_standards 守卫锁定)。
    """
    policy = context.breathing
    _raise_if_control_signal(context)
    if not policy.enabled:
        return

    base_delay = float(await context.get_current_send_delay_seconds())
    min_floor = float(policy.min_floor_s)
    if min_floor <= 0.0 and base_delay <= 0.0:
        return
    jitter_max = float(policy.jitter_s)
    jitter = random.uniform(0.0, jitter_max) if (min_floor > 0 and jitter_max > 0) else 0.0
    delay_sec = max(base_delay, min_floor) + jitter
    if delay_sec <= 0:
        return

    logger.info(
        "⏳ [%s] 分卷 Part %d/%d 发送成功，插入分卷呼吸间隔 %.1fs (准备拉取 Part %d)...",
        channel_label,
        part_index,
        total_parts,
        delay_sec,
        part_index + 1,
    )
    poll_step = float(policy.poll_s)
    end_time = time.monotonic() + delay_sec
    while time.monotonic() < end_time:
        _raise_if_control_signal(context)
        step = min(poll_step, max(0.0, end_time - time.monotonic()))
        if step > 0:
            await asyncio.sleep(step)


def _safe_invoke_engine_progress(
    context: PublishContext, *args: Any, label: str = "进度回调异常", **kwargs: Any
) -> None:
    """引擎进度回调安全分发骨架: 回调缺失零开销 + 异常 debug 降级 (非致命)。"""
    if not context.progress_callback:
        return
    try:
        safe_invoke_progress(context.progress_callback, *args, **kwargs)
    except Exception as e:
        logger.debug("%s: %s", label, e)


def _parse_head_positional_hook_args(args: tuple[Any, ...]) -> tuple[int, int, int]:
    """位置形态 A: (workers, dc_id?, workers_total?) —— 首元素为数值并发数。"""
    workers_val = int(args[0])
    dc_id_val = args[1] if len(args) > 1 and isinstance(args[1], int) else 0
    workers_total_val = int(args[2]) if len(args) > 2 and isinstance(args[2], (int, float)) else 0
    return workers_val, dc_id_val, workers_total_val


def _parse_shifted_positional_hook_args(args: tuple[Any, ...]) -> tuple[int, int, int]:
    """位置形态 B: (fname, workers, dc_id?, workers_total?) —— 首元素为文件名, 并发后移一位。"""
    if len(args) > 1 and isinstance(args[1], (int, float)):
        dc_id_val = args[2] if len(args) > 2 and isinstance(args[2], int) else 0
        workers_total_val = int(args[3]) if len(args) > 3 and isinstance(args[3], (int, float)) else 0
        return int(args[1]), dc_id_val, workers_total_val
    return 1, 0, 0


def _merge_kwarg_hook_overrides(
    kwargs: dict[str, Any], workers_val: int, dc_id_val: int, workers_total_val: int
) -> tuple[int, int, int]:
    """关键字形态覆盖 (workers / dc_id / workers_total), 非法值 debug 跳过保持原值。"""
    if "workers" in kwargs:
        try:
            workers_val = int(kwargs["workers"])
        except Exception as w_err:
            logger.debug("解析 workers 参数跳过: %s", w_err)
    if "dc_id" in kwargs:
        dc_id_val = kwargs["dc_id"]
    if "workers_total" in kwargs:
        try:
            workers_total_val = int(kwargs["workers_total"])
        except Exception as wt_err:
            logger.debug("解析 workers_total 参数跳过: %s", wt_err)
    return workers_val, dc_id_val, workers_total_val


def _parse_upload_hook_args(args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[int, int, int]:
    """解析 upload_file_parallel 进度钩子的并发细分参数 (位置/关键字双形态容错)。

    自 CC 13 拆解为位置 A/B 两形态解析器 + 关键字覆盖合并。
    """
    if not args:
        workers_val, dc_id_val, workers_total_val = 1, 0, 0
    elif isinstance(args[0], (int, float)):
        workers_val, dc_id_val, workers_total_val = _parse_head_positional_hook_args(args)
    else:
        workers_val, dc_id_val, workers_total_val = _parse_shifted_positional_hook_args(args)
    return _merge_kwarg_hook_overrides(kwargs, workers_val, dc_id_val, workers_total_val)


def _make_pikpak_url_provider(
    link_provider: Callable[[list[str]], Awaitable[list[str]]], item_id: str, *, strict: bool
) -> Callable[[], Awaitable[str]]:
    """PikPak JIT 索链 URL provider 共享工厂 (6 处闭包副本收敛)。

    strict=True: 空链路即刻抛 PikPakPublishFatalError("link") (VSSE/fMP4/快照路径);
    strict=False: 首元素原样返回, 空链路由 Provider 首读报错 (单发/相册/回填串行路径)。
    """

    async def _url_provider() -> str:
        links = await link_provider([item_id])
        if strict and (not links or not links[0]):
            raise PikPakPublishFatalError("link", f"获取直链失败: file_id={item_id}")
        return links[0]

    return _url_provider


async def _send_file_with_floodwait_retry(
    client: Any,
    chat_id: Any,
    media: Any,
    send_kwargs: SendFileKwargs,
    *,
    retry_other_errors: bool,
    log_label: str,
    registry: ClientRegistryPort | None = None,
    context: PublishContext | None = None,
) -> tuple[Any, Exception | None]:
    """send_file 五次尝试骨架 (VSSE/fMP4 共享): FloodWait 睡眠 wait+1 重试并标记账号冷却;
    其余异常按 retry_other_errors 决定立即上抛 (VSSE) 或耗尽尝试后上抛 (fMP4)。
    thumb 流每轮 seek(0) (BytesIO 消费后可重发)。返回 (sent, last_sf_err)。
    账号池经 ClientRegistryPort 注入 (None → 默认单例适配器)。"""
    effective_registry = registry or DEFAULT_CLIENT_REGISTRY
    sent = None
    last_sf_err = None
    for sf_attempt in range(5):
        if context is not None:
            _raise_if_control_signal(context)
        if "thumb" in send_kwargs and hasattr(send_kwargs["thumb"], "seek"):
            send_kwargs["thumb"].seek(0)
        try:
            sent = await client.send_file(chat_id, media, **send_kwargs)
            break
        except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
            raise
        except Exception as sf_err:
            last_sf_err = sf_err

            if isinstance(sf_err, FloodWaitError):
                if sf_attempt == 4:
                    raise
                wait_sec = getattr(sf_err, "seconds", 5) or 5
                logger.warning("Telegram FloodWaitError in send_file: waiting %d+1 seconds", wait_sec)
                try:
                    effective_registry.mark_flood_wait(client, wait_sec)
                except Exception as fw_err:
                    logger.debug("%s 标记 FloodWait 状态异常: %s", log_label, fw_err)
                if context is not None and (context.is_cancelled is not None or context.is_paused is not None):
                    remaining = float(wait_sec + 1)
                    while remaining > 0:
                        _raise_if_control_signal(context)
                        step = min(0.5, remaining)
                        await asyncio.sleep(step)
                        remaining -= step
                else:
                    await asyncio.sleep(wait_sec + 1)
                continue
            if retry_other_errors and isinstance(sf_err, (ServerError, BadMessageError)):
                if sf_attempt == 4:
                    raise
                continue
            raise
    return sent, last_sf_err


class _AlbumLinkPrefetchWindow:
    """相册逐张链路预取窗口 (包5 J9 收拢): 上传第 k 张句柄的同时预取第 k+1..k+slots-1 张
    下载链 (get_download_links), 掩蔽每张 1×RTT 索链等待。

    仅预取链接 (KB 级内存), 不做 chunk 预读; 消费点 (pop) 的异常语义与串行 JIT 索链
    一致, 仅抛出点提前。close() 回收未消费 task: 已取消者跳过、已完成者取回异常一次
    (消除 "Task exception was never retrieved" GC 噪音), 挂起者 cancel。
    """

    def __init__(self, link_provider: Callable[[list[str]], Awaitable[list[str]]], slots: int) -> None:
        self._link_provider = link_provider
        self._slots = max(1, min(int(slots or 1), 3))
        self._futures: deque[asyncio.Future[str]] = deque()
        self._next_pos = 0

    @property
    def depth(self) -> int:
        return len(self._futures)

    def advance(self, pending: list[tuple[int, PublishItem]], next_pos: int) -> None:
        self._next_pos = max(self._next_pos, next_pos)
        while len(self._futures) < self._slots - 1 and self._next_pos < len(pending):
            item_id = pending[self._next_pos][1].item_id

            async def _resolve(_id: str = item_id) -> str:
                links = await self._link_provider([_id])
                return links[0]

            self._futures.append(asyncio.ensure_future(_resolve()))
            self._next_pos += 1

    async def pop(self) -> str:
        return await self._futures.popleft()

    def close(self) -> None:
        while self._futures:
            pending_task = self._futures.popleft()
            if pending_task.done():
                if not pending_task.cancelled():
                    exc = pending_task.exception()
                    if exc is not None:
                        logger.debug("未消费预取链路解析失败 (已回收): %s", exc)
            else:
                pending_task.cancel()


class _SpeedMeasuringProvider(BaseChunkProvider):
    """BaseChunkProvider 测量包装:
    代理 read_chunk 并统计累计字节/耗时 -> download_speed_mb_s (全程累计均值)。
    """

    def __init__(self, inner: BaseChunkProvider):
        self._inner = inner
        self.total_bytes = inner.total_bytes
        self.file_id = inner.file_id
        self.file_name = inner.file_name
        self._bytes_read = 0
        self._started: float | None = None

    async def read_chunk(self, offset: int, size: int) -> bytes:
        if self._started is None:
            self._started = time.monotonic()
        data = await self._inner.read_chunk(offset, size)
        self._bytes_read += len(data)
        return data

    async def aclose(self) -> None:
        await self._inner.aclose()

    @property
    def download_speed_mb_s(self) -> float:
        if self._started is None or self._bytes_read <= 0:
            return 0.0
        elapsed = max(time.monotonic() - self._started, 1e-6)
        return round((self._bytes_read / (1024 * 1024)) / elapsed, 3)


# PLAYBACK_QUALITY 包3: 本地视频消息属性探测缓存 —— 键 (path, size, mtime),
# 任一变化自动失效 (LocalDiskTransportEngine._ensure_local_video_metadata 单点读写)。
# 有界 LRU (128 项, 命中置新 / 淘汰最旧), 替代无界 dict 防内存泄露。
_LOCAL_MEDIA_META_CACHE_MAX_ENTRIES = 128
_LOCAL_MEDIA_META_CACHE: "OrderedDict[tuple[str, int, float], ProbedMediaMetadata]" = OrderedDict()
