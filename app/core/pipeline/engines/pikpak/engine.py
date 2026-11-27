"""统一发布流水线传输引擎 - PikPak 零落盘流式传输引擎 (纯门面路由)。

门面职责: send_item/send_album/edit_item 路由委托 (实现体在 single.py)、
VSSE/fMP4 分卷发布编排 (_publish_*_split_item)、JIT 索链与 CDN Range 预检、
封面与 E2E 捕获会话工厂。单发/相册/回填/raw 分卷上传的所有权在
`pikpak/single.py`; 分卷流式与重规划在 `vsse.py` / `fmp4.py` 通道自持
(引擎反向引用与 staticmethod 重绑 shim 已废除)。
"""

import asyncio
import logging
import os
import time
from collections.abc import Awaitable, Callable
from typing import Any

from app.core import stream_chunk_provider as scp_mod
from app.core.e2e_capture import E2ECaptureSession
from app.core.iso_bmff import exceptions as ib_exc
from app.core.media_metadata import verify_remote_mpeg_ts_head
from app.core.media_sniff import TS_AMBIGUOUS_EXTENSIONS
from app.core.pikpak.expander import ParsedFileItem
from app.core.pikpak.taxonomy import (
    PikPakPublishError,
    PikPakPublishFatalError,
    wrap_pikpak_failure,
)
from app.core.pikpak.thumbnail import make_thumb_provider
from app.core.pipeline.engines._common import (
    ProgressReporter,
    _is_huge_file_size,
    _make_pikpak_url_provider,
    _read_probe_context,
    _SpeedMeasuringProvider,
    precheck_cdn_range,
)
from app.core.pipeline.engines.pikpak.fmp4 import (
    Fmp4SplitChannel,
    probe_fmp4_media,
)
from app.core.pipeline.engines.pikpak.split_loop import execute_split_loop
from app.core.pipeline.engines.pikpak.vsse import (
    VsseSplitChannel,
    probe_vsse_envelope,
)
from app.core.pipeline.interfaces import (
    DEFAULT_CLIENT_REGISTRY,
    DEFAULT_TASK_STORE,
    ClientRegistryPort,
    MediaTransportEngine,
    TaskStorePort,
)
from app.core.pipeline.models import PublishContext, PublishItem, TransportOutcome
from app.core.scheduler import UploadCancelledError, UploadPausedError

from . import single as single_mod

logger = logging.getLogger(__name__)

# Local reuse window for a preflight URL, independent of the CDN's actual expiry.
_TS_PREFLIGHT_LINK_REUSE_MAX_AGE_S = 5.0
TS_FAIL_OPEN_LOG_PREFIX = "⚠️ 远端 TS 头部校验未决 (fail-open 放行)"


class PikPakStreamTransportEngine(MediaTransportEngine):
    """PikPak 网盘零落盘流式传输引擎 (门面)。

    封装 PikPak JIT 索链、虚拟分卷 (VirtualStreamSplitter) 与 direct streaming upload 的
    路由与编排; 单发/相册/回填实现见 `single.py`, 通道自持见 `vsse.py` / `fmp4.py`。
    - 结合 PikPakStreamChunkProvider 与 SlicedChunkProvider 进行无落盘分卷切片；
    - 针对 PikPakLinkExpiredError 具备最多 3 次换链重新索链与断点恢复能力；
    - 针对 FILE_PARTS_INVALID 具备清空失效断点后就地单 Part 重试能力；
    - 封面异步提取与内存注入（优先官方封面，降级本地 FFmpeg HTTP Range 抽帧）；
    - send_album 图片相册 —— 句柄缓冲后组发, photo 形态；
    - 正确响应 is_cancelled 与 is_paused 控制信号。
    """

    _LINK_EXPIRED_RELINK_ATTEMPTS = 3

    def __init__(
        self,
        link_provider: Callable[[list[str]], Awaitable[list[str]]] | None = None,
        *,
        thumbnail_link_provider: Callable[[str], Awaitable[str | None]] | None = None,
        registry: ClientRegistryPort | None = None,
        store: TaskStorePort | None = None,
    ):
        # JIT 索链 IO 由门面构造注入 (pikpak_service.get_download_links),
        # core 引擎不再动态解析 services —— 单一来源 (缺注入在使用点明确报错)。
        self._link_provider = link_provider
        self._thumbnail_link_provider = thumbnail_link_provider
        # 账号池/持久层依赖端口化 —— 默认参数 = 现状单例适配器
        # (调用期经模块属性读取, 既有测试 patch 路径保持穿透)。
        self._registry = registry if registry is not None else DEFAULT_CLIENT_REGISTRY
        self._store = store if store is not None else DEFAULT_TASK_STORE
        self._prefetched_links: dict[str, tuple[str, float]] = {}
        self._last_thumb: tuple[PublishItem, bytes | None] | None = None

    def pop_prefetched_link(self, item_id: str) -> str | None:
        """取出并消费 TS 头部校验阶段已获取的 JIT 签名直链 (仅内存驻留, 永不落日志)。"""
        prefetched = self._prefetched_links.pop(item_id, None)
        if prefetched is None:
            return None
        url, acquired_at = prefetched
        if time.monotonic() - acquired_at > _TS_PREFLIGHT_LINK_REUSE_MAX_AGE_S:
            return None
        return url

    async def preflight_item(self, item: PublishItem, context: PublishContext) -> tuple[bool, str | None]:
        """PikPak 传输前单条目预检: 对 .ts/.m2ts/.mts 执行有界文件头校验。"""
        return await self.verify_remote_ts_item(item, context)

    async def verify_remote_ts_item(self, item: PublishItem, context: PublishContext) -> tuple[bool, str | None]:
        """对 PikPak .ts/.m2ts/.mts 条目执行独立 2048B 上限文件头 Range 校验 (不依赖官方元数据)。

        仅当 HTTP 206 + Content-Range + 完整头部字节确认非法时返回 (False, reason);
        网络/HTTP/短读异常一律 fail-open 返回 (True, None), 取消/暂停信号穿透上抛。
        索链与头部读取共享跨任务并发信号量与总超时预算。
        """
        name_lower = (item.name or item.item_id or "").lower()
        if name_lower.endswith(".d.ts"):
            return False, "TypeScript 类型声明文件 (.d.ts)，跳过上传"
        ext = os.path.splitext(name_lower)[1]
        if ext not in TS_AMBIGUOUS_EXTENSIONS or self._link_provider is None:
            return True, None

        link_provider = self._link_provider
        prefetched: tuple[str, float] | None = None

        async def _resolve_link() -> str:
            nonlocal prefetched
            links = await link_provider([item.item_id])
            url = links[0] if links else ""
            if url:
                prefetched = (url, time.monotonic())
            return url

        verified, reason = await verify_remote_mpeg_ts_head(
            _resolve_link,
            file_size=item.size_bytes,
            ext=ext,
            is_cancelled=context.is_cancelled,
            is_paused=context.is_paused,
        )
        if verified is False:
            return False, reason
        if verified is True and prefetched is not None:
            self._prefetched_links[item.item_id] = prefetched
        elif verified is None:
            label = item.name or item.item_id
            logger.warning(
                "%s: task_id=%s file_id=%s name=%s",
                TS_FAIL_OPEN_LOG_PREFIX,
                context.task_id,
                item.item_id,
                label,
            )
            if context.task_id:
                try:
                    await self._store.append_log(context.task_id, f"{TS_FAIL_OPEN_LOG_PREFIX} [{label}]")
                except Exception:
                    logger.warning(
                        "%s: 任务日志写入失败 task_id=%s file_id=%s",
                        TS_FAIL_OPEN_LOG_PREFIX,
                        context.task_id,
                        item.item_id,
                        exc_info=True,
                    )
        return True, None

    @classmethod
    def _wrap_failure(cls, exc: Exception) -> PikPakPublishError:
        return wrap_pikpak_failure(exc)

    def _make_thumb_provider(
        self,
        item: PublishItem,
        url_provider: Callable[[], Any] | None = None,
    ) -> Callable[[], Awaitable[bytes | None]]:
        provided_bytes: bytes | bytearray | memoryview | None = (
            item.thumbnail_data if isinstance(item.thumbnail_data, (bytes, bytearray, memoryview)) else None
        )
        parsed_item = ParsedFileItem(
            file_id=item.item_id,
            name=item.name,
            size_bytes=item.size_bytes,
            thumbnail_link=item.thumbnail_data if isinstance(item.thumbnail_data, str) else None,
            duration_s=str(item.duration_s) if item.duration_s else None,
            width=str(item.width) if item.width else None,
            height=str(item.height) if item.height else None,
            parent_id=item.parent_folder_id,
            is_dir=item.is_dir,
            audit=item.audit or {},
        )
        inner = make_thumb_provider(
            parsed_item,
            url_provider=url_provider,
            thumbnail_link_provider=self._thumbnail_link_provider,
            provided_bytes=provided_bytes,
        )

        async def _memoized_thumb() -> bytes | None:
            if self._last_thumb is not None and self._last_thumb[0] is item:
                return self._last_thumb[1]
            result = await inner()
            self._last_thumb = (item, result)
            return result

        return _memoized_thumb

    @staticmethod
    async def _precheck_cdn_range(stream_url: str) -> None:
        """CDN Range 预检薄委托 (实现体下沉 `_common.precheck_cdn_range`;
        本门面方法保留为既有测试的 patch seam)。"""
        await precheck_cdn_range(stream_url)

    async def _probe_and_precheck_stream_url(self, item_id: str) -> str:
        """JIT direct link acquisition and HTTP Range 206 precheck (always fresh for VSSE)."""
        if self._link_provider is None:
            raise PikPakPublishFatalError(
                "unknown",
                "PikPakStreamTransportEngine 缺少 link_provider 注入 (门面装配缺失)",
            )
        # Discard any prefetched TS link to avoid stale-link failure on long split prechecks
        self._prefetched_links.pop(item_id, None)
        fresh_links = await self._link_provider([item_id])
        if not fresh_links or not fresh_links[0]:
            raise PikPakPublishFatalError("link", f"获取直链失败: file_id={item_id}")
        url = fresh_links[0]
        await self._precheck_cdn_range(url)
        return url

    async def _open_capture_session(
        self, item: PublishItem, context: PublishContext, threshold: int, strategy: str
    ) -> E2ECaptureSession:
        """E2E 捕获会话工厂 (计划 D2/D7): 捕获关闭或目录不可创建时返回 inactive 透传哨兵。

        时序: 源快照在路由判定后、通道发布前 await 完成 (先基准后实验, 不与
        上传流争抢 CDN 带宽); 独立 provider 即时新取直链, 规避过期。
        """
        session = E2ECaptureSession.open(
            context.task_id or "pikpak-direct",
            item.item_id,
            target_chat_id=context.target_chat_id,
            split_strategy=strategy,
            threshold_bytes=threshold,
        )
        if not session.active:
            return session

        link_provider = self._link_provider
        if link_provider is None:
            raise PikPakPublishFatalError("link", f"获取直链失败 (link_provider 未注入): file_id={item.item_id}")

        def _make_snapshot_provider() -> scp_mod.PikPakStreamChunkProvider:
            return scp_mod.PikPakStreamChunkProvider(
                file_id=item.item_id,
                file_name=item.name or item.item_id,
                total_bytes=item.size_bytes,
                url_provider=_make_pikpak_url_provider(link_provider, item.item_id, strict=True),
            )

        await session.snapshot_source(_make_snapshot_provider, item.name or item.item_id, item.size_bytes)
        return session

    async def _fetch_engine_thumb(
        self, item: PublishItem, url_provider: Callable[[], Any] | None, log_label: str = ""
    ) -> bytes | None:
        """封面一次性预取 (VSSE/fMP4 共享): 失败 debug 降级, 不阻断发布。"""
        try:
            return await self._make_thumb_provider(item, url_provider=url_provider)()
        except (UploadCancelledError, UploadPausedError):
            raise
        except Exception as th_err:
            logger.debug("%s提取封面失败 (非致命): %s", log_label, type(th_err).__name__)
            return None

    async def _publish_vsse_split_item(
        self,
        client: Any,
        item: PublishItem,
        threshold: int,
        context: PublishContext,
        capture_session: E2ECaptureSession | None = None,
    ) -> TransportOutcome:
        """VSSE 零重编码极速通道 (编排): 探测规划 → 逐卷流式发布 → 失败回滚。

        熔断语义不变: 探测/规划段 IsoBmffError 族 → 100% 无缝降级 fMP4 全能保底通道。
        """
        if capture_session is None:
            capture_session = E2ECaptureSession.inactive()
        if client is None:
            raise PikPakPublishFatalError("userbot")
        if self._link_provider is None:
            raise PikPakPublishFatalError(
                "unknown",
                "PikPakStreamTransportEngine 缺少 link_provider 注入 (门面装配缺失)",
            )

        url = await self._probe_and_precheck_stream_url(item.item_id)

        is_huge = _is_huge_file_size(item.size_bytes)
        effective_prefetch = (
            min(2, max(1, context.get_current_prefetch_concurrency()))
            if is_huge
            else (context.prefetch_concurrency or 3)
        )
        inner_provider = scp_mod.PikPakStreamChunkProvider(
            file_id=item.item_id,
            file_name=item.name or item.item_id,
            total_bytes=item.size_bytes,
            url_provider=_make_pikpak_url_provider(self._link_provider, item.item_id, strict=True),
            initial_url=url,
            prefetch_concurrency=effective_prefetch,
        )
        provider = _SpeedMeasuringProvider(inner_provider)

        # Circuit Breaker Block: 探测/规划失败 (IsoBmffError 族) 熔断降级 fMP4
        try:
            try:
                metadata, plans, synthesized_headers = await probe_vsse_envelope(
                    item, threshold, provider, context=context
                )
            except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
                raise
            except (ib_exc.UnsupportedMp4LayoutError, ib_exc.IsoBmffError) as cb_exc:
                logger.warning(
                    "⚡ [CIRCUIT BREAKER] VSSE 熔断降级触发 (%s: %s)，100%% 无缝回退至 fMP4 全能保底发布通道",
                    type(cb_exc).__name__,
                    cb_exc,
                    exc_info=True,
                )
                capture_session.record_channel_switch(f"vsse circuit breaker: {type(cb_exc).__name__}: {cb_exc}")
                probe_ctx = _read_probe_context(cb_exc)
                probed_metadata = probe_ctx.probed_metadata if probe_ctx is not None else None
                pinned_regions = probe_ctx.pinned_regions if probe_ctx is not None else None
                return await self._publish_fmp4_split_item(
                    client,
                    item,
                    threshold,
                    context,
                    capture_session=capture_session,
                    probed_metadata=probed_metadata,
                    pinned_regions=pinned_regions,
                )

            thumb_bytes = await self._fetch_engine_thumb(
                item,
                lambda: getattr(inner_provider, "_current_url", None) or url,
                log_label="VSSE",
            )

            channel = VsseSplitChannel(
                item=item,
                metadata=metadata,
                plans=plans,
                synthesized_headers=synthesized_headers,
                provider=provider,
                thumb_bytes=thumb_bytes,
                registry=self._registry,
            )
            reporter = ProgressReporter(item, context, provider=provider, log_label="VSSE")
            return await execute_split_loop(
                channel=channel,
                item=item,
                context=context,
                threshold=threshold,
                client=client,
                capture_session=capture_session,
                link_provider=self._link_provider,
                reporter=reporter,
                max_replan_attempts=0,
                store=self._store,
            )
        finally:
            await provider.aclose()

    async def _publish_fmp4_split_item(
        self,
        client: Any,
        item: PublishItem,
        threshold: int,
        context: PublishContext,
        capture_session: E2ECaptureSession | None = None,
        *,
        probed_metadata: Any | None = None,
        pinned_regions: list[tuple[int, bytes]] | None = None,
        use_paged_proxy: bool = False,
        estimated_output_bytes: int | None = None,
    ) -> TransportOutcome:
        """全能保底通道 (编排): MKV/MP4 探测回填 → 分卷规划 → 逐卷流式 (超限动态重规划) → 失败回滚。"""
        if capture_session is None:
            capture_session = E2ECaptureSession.inactive()
        if client is None:
            raise PikPakPublishFatalError("userbot")
        if self._link_provider is None:
            raise PikPakPublishFatalError(
                "unknown",
                "PikPakStreamTransportEngine 缺少 link_provider 注入 (门面装配缺失)",
            )

        get_fresh_url = _make_pikpak_url_provider(self._link_provider, item.item_id, strict=True)
        # Discard any prefetched TS link so fMP4 split always starts from a fresh link
        self._prefetched_links.pop(item.item_id, None)
        url = await get_fresh_url()
        await self._precheck_cdn_range(url)

        ext, copy_audio, total_duration, keyframe_pts = await probe_fmp4_media(
            url, item, context, probed_metadata=probed_metadata
        )

        thumb_bytes = await self._fetch_engine_thumb(item, lambda: url, log_label="fMP4")

        channel = Fmp4SplitChannel(
            item=item,
            context=context,
            threshold=threshold,
            get_fresh_url=get_fresh_url,
            initial_url=url,
            ext=ext,
            copy_audio=copy_audio,
            total_duration=total_duration,
            keyframe_pts=keyframe_pts,
            thumb_bytes=thumb_bytes,
            pinned_regions=pinned_regions,
            use_paged_proxy=use_paged_proxy,
            registry=self._registry,
            estimated_output_bytes=estimated_output_bytes,
        )
        try:
            await channel.init_proxy()
            reporter = ProgressReporter(item, context, log_label="fMP4")
            return await execute_split_loop(
                channel=channel,
                item=item,
                context=context,
                threshold=threshold,
                client=client,
                capture_session=capture_session,
                link_provider=self._link_provider,
                reporter=reporter,
                max_replan_attempts=3,
                store=self._store,
            )
        finally:
            await channel.aclose()

    # ---- 单发 / 相册 / 回填: 所有权在 single.py, 门面方法为既有测试的 patch seam ----

    async def send_item(self, item: PublishItem, context: PublishContext) -> TransportOutcome:
        return await single_mod.send_item(self, item, context)

    async def send_album(self, items: list[PublishItem], context: PublishContext) -> list[TransportOutcome]:
        return await single_mod.send_album(self, items, context)

    async def edit_item(self, item: PublishItem, context: PublishContext, msg_id: int) -> TransportOutcome:
        """PikPak 原地回填 (单分卷条目; S4/)。"""
        return await single_mod.edit_item(self, item, context, msg_id)

    async def _publish_single_item(
        self,
        client: Any,
        item: PublishItem,
        parts: list[Any],
        context: PublishContext,
        delivered_parts: dict[int, int] | None = None,
    ) -> TransportOutcome:
        return await single_mod.publish_single_item(self, client, item, parts, context, delivered_parts)
