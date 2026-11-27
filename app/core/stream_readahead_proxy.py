"""LocalPagedRangeProxy: 本地分页/区间回环 HTTP Range 代理服务。

服务于 vNext 三级流式漏斗架构中的:
- Tier 2 (HYBRID_AUDIO_SIDECAR): 旁路音频提取模式 (基于 audio_spans 有界拉取, 0 视频流量浪费)
- Tier 3 (FMP4_PAGED_PROXY): 双输入流 (-an 视频 + -vn 音频) 有界块缓存 fMP4 兜底流

核心机制:
1. L0 钉死热缓存 (Pinned Metadata Regions):
    Stage 0 已下载的 head_bytes 与 moov_bytes 注入为 L0 内存区域, 100% 内存直接命中 (0 ms, 0 远端请求)。
2. L1 通道独立分页缓存 (Per-Channel Paged Ring Cache):
   - video / main 通道: 默认 2 MiB 块大小, 单通道最多 4 页 (8 MiB), 顺序读时自动触发后台单飞预读下 1 页。
   - audio 通道: 支持传入 precomputed audio_spans 进行精确有界拉取, 或使用 128 KiB 紧凑分页。
3. 单飞去重 (Single-Flight Page/Span Dedup):
    同一 (channel, page/span) 在途拉取由独立的 asyncio.Task 托管并共享, 任意等待者取消不会破坏其他协程或导致死锁。
4. 零断连 Keep-Alive:
    本地回环代理将 FFmpeg 的无界 Range: bytes=X- 转化为对底层 Provider 的有界块读取,
    避免远端 TCP RST 与高 RTT 跨洋 TLS 重握手。

生命周期与安全边界:
- 仅绑定 127.0.0.1:0 (随机可用端口)。
- 路径受一次性随机 session_token 保护 (secrets.token_urlsafe(16))。
- 支持 `async with LocalPagedRangeProxy(...) as proxy:` 上下文管理, 退出时自动取消预读任务并释放资源。
"""

from __future__ import annotations

import asyncio
import bisect
import logging
import re
import secrets
from collections import OrderedDict
from collections.abc import Sequence
from typing import Any

from aiohttp import web

from app.core.stream_chunk_provider import (
    BaseChunkProvider,
    ChunkProviderError,
    PikPakLinkExpiredError,
    PikPakNotFoundError,
    PikPakRiskControlError,
    PikPakThrottledError,
)

logger = logging.getLogger(__name__)

# Range 头部正则解析: bytes=start-end 或 bytes=start- 或 bytes=-suffix
_RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$")


def coalesce_byte_spans(
    spans: Sequence[tuple[int, int]],
    max_gap: int = 64 * 1024,
    max_span_size: int = 2 * 1024 * 1024,
) -> list[tuple[int, int]]:
    """合并物理相邻或微小间隙 (<= max_gap) 的字节区间, 且单区间不超过 max_span_size。

    常用于旁路音频提取: 将相邻的音频 Chunk 区间聚合成单次拉取 Span, 消除跨洋高频 HTTP Seek。
    """
    if not spans:
        return []
    valid_spans = sorted([(s, e) for s, e in spans if e > s >= 0], key=lambda x: (x[0], x[1]))
    if not valid_spans:
        return []

    result: list[tuple[int, int]] = []
    cur_start, cur_end = valid_spans[0]

    for next_start, next_end in valid_spans[1:]:
        gap = next_start - cur_end
        new_size = max(cur_end, next_end) - cur_start
        if gap <= max_gap and new_size <= max_span_size:
            cur_end = max(cur_end, next_end)
        else:
            result.append((cur_start, cur_end))
            cur_start, cur_end = next_start, next_end

    result.append((cur_start, cur_end))
    return result


def build_pinned_regions(head_bytes: bytes, moov_offset: int, moov_bytes: bytes | None) -> list[tuple[int, bytes]]:
    """构建互斥无重叠的 L0 Pinned 内存区域表。"""
    if not head_bytes and not moov_bytes:
        return []
    if not moov_bytes:
        return [(0, head_bytes)] if head_bytes else []
    if not head_bytes:
        return [(moov_offset, moov_bytes)]

    pinned: list[tuple[int, bytes]] = []
    if moov_offset < len(head_bytes):
        if moov_offset > 0:
            pinned.append((0, head_bytes[:moov_offset]))
        pinned.append((moov_offset, moov_bytes))
        moov_end = moov_offset + len(moov_bytes)
        if len(head_bytes) > moov_end:
            pinned.append((moov_end, head_bytes[moov_end:]))
    else:
        pinned.append((0, head_bytes))
        pinned.append((moov_offset, moov_bytes))
    return pinned


class _ChannelState:
    """单个逻辑通道 (video, audio, main 等) 的独立 LRU 缓存与预读状态。"""

    def __init__(self, name: str, page_size: int, max_pages: int) -> None:
        self.name = name
        self.page_size = page_size
        self.max_pages = max_pages
        self.pages: OrderedDict[int, bytes] = OrderedDict()
        self.in_flight: dict[int, asyncio.Task[bytes]] = {}
        self.last_accessed_page: int | None = None


class LocalPagedRangeProxy:
    """本地分页/区间回环 HTTP Range 代理服务。"""

    DEFAULT_PAGE_SIZE = 2 * 1024 * 1024  # 2 MiB (video / main 大吞吐模式)
    DEFAULT_AUDIO_PAGE_SIZE = 128 * 1024  # 128 KiB (audio 通道紧凑分页)
    DEFAULT_MAX_PAGES_PER_CHANNEL = 4  # 4 页 (每通道最多 8 MiB, 双通道 16 MiB)
    DEFAULT_SPAN_PREFETCH_WINDOW = 8  # Tier 2 音频 Span 滑动窗口并发预读数 (掩盖跨洋 RTT)
    DEFAULT_MAX_CACHED_SPANS = 32  # Tier 2 音频 Span 缓存容量上限 (48 KiB × 32 ≈ 1.5 MiB)

    def __init__(
        self,
        provider: BaseChunkProvider,
        *,
        pinned_regions: Sequence[tuple[int, bytes]] | None = None,
        audio_spans: Sequence[tuple[int, int]] | None = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        audio_page_size: int = DEFAULT_AUDIO_PAGE_SIZE,
        max_pages_per_channel: int = DEFAULT_MAX_PAGES_PER_CHANNEL,
        span_prefetch_window: int = DEFAULT_SPAN_PREFETCH_WINDOW,
        max_cached_spans: int = DEFAULT_MAX_CACHED_SPANS,
        session_token: str | None = None,
        enable_prefetch: bool = True,
        zero_fill_unmapped: bool = False,
        allowed_channels: Sequence[str] | None = None,
    ) -> None:
        self.provider = provider
        self.page_size = max(4096, page_size)
        self.audio_page_size = max(4096, audio_page_size)
        self.max_pages_per_channel = max(1, max_pages_per_channel)
        self.span_prefetch_window = max(0, span_prefetch_window)
        self.max_cached_spans = max(self.max_pages_per_channel, max_cached_spans, self.span_prefetch_window + 4)
        self.session_token = session_token or secrets.token_urlsafe(16)
        self.enable_prefetch = enable_prefetch
        self.zero_fill_unmapped = zero_fill_unmapped
        self._allowed_channels: frozenset[str] | None = (
            frozenset(ch.strip("/").lower() or "main" for ch in allowed_channels)
            if allowed_channels is not None
            else None
        )

        # L0 钉死热缓存: 存储为 (start, end, data)
        self._pinned_regions: list[tuple[int, int, bytes]] = []
        if pinned_regions:
            for start, data in pinned_regions:
                if data:
                    self._pinned_regions.append((start, start + len(data), data))
            self._pinned_regions.sort(key=lambda x: x[0])

        # Tier 2 音频活跃区间表: 存储为已排序的 (span_start, span_end)
        self._audio_spans: list[tuple[int, int]] = []
        self._audio_span_starts: list[int] = []
        if audio_spans:
            self._audio_spans = sorted([(s, e) for s, e in audio_spans if e > s >= 0], key=lambda x: x[0])
            self._audio_span_starts = [s for s, _ in self._audio_spans]

        # Tier 2 音频 Span 缓存与单飞锁
        self._span_cache: OrderedDict[int, bytes] = OrderedDict()
        self._span_in_flight: dict[int, asyncio.Task[bytes]] = {}

        # 通道独立状态表
        self._channels: dict[str, _ChannelState] = {}

        # 后台预读任务集合
        self._background_tasks: set[asyncio.Task[Any]] = set()

        # aiohttp 内部服务组件
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._port: int | None = None
        self._closed = False

        # 统计指标 (供单测校验与监控观测)
        self.l0_hits: int = 0
        self.l1_hits: int = 0
        self.remote_fetches: int = 0
        self.bytes_served: int = 0
        self.bytes_fetched: int = 0
        self._last_provider_error: ChunkProviderError | None = None

    @property
    def last_provider_error(self) -> ChunkProviderError | None:
        return self._last_provider_error

    async def _read_provider_chunk(self, offset: int, size: int) -> bytes:
        try:
            return await self.provider.read_chunk(offset, size)
        except ChunkProviderError as exc:
            if self._last_provider_error is None or isinstance(
                exc,
                (PikPakThrottledError, PikPakRiskControlError, PikPakNotFoundError, PikPakLinkExpiredError),
            ):
                self._last_provider_error = exc
            raise

    @property
    def file_id(self) -> str:
        return self.provider.file_id

    @property
    def total_bytes(self) -> int:
        return self.provider.total_bytes

    @property
    def port(self) -> int:
        if self._port is None:
            raise RuntimeError("LocalPagedRangeProxy 尚未启动 (port 为 None)")
        return self._port

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}/{self.session_token}"

    @property
    def video_url(self) -> str:
        return self.get_url("video")

    @property
    def audio_url(self) -> str:
        return self.get_url("audio")

    @property
    def main_url(self) -> str:
        return self.get_url("main")

    def get_url(self, channel: str = "main") -> str:
        norm_ch = channel.strip("/").lower() or "main"
        return f"http://127.0.0.1:{self.port}/{self.session_token}/{norm_ch}"

    async def start(self) -> LocalPagedRangeProxy:
        """启动 127.0.0.1 回环 HTTP 代理服务。"""
        if self._runner is not None:
            return self

        app = web.Application()
        app.router.add_route("*", "/{token}/{channel}", self._handle_request)
        app.router.add_route("*", "/{token}", self._handle_request)

        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, host="127.0.0.1", port=0)
        await site.start()

        # runner.addresses 形如 [('127.0.0.1', port)]
        if not runner.addresses:
            raise RuntimeError("LocalPagedRangeProxy 未能成功绑定到 127.0.0.1")
        self._port = runner.addresses[0][1]
        self._app = app
        self._runner = runner
        self._site = site
        self._closed = False

        logger.debug(
            "[LocalPagedRangeProxy] 启动成功: port=%d, file_id=%s, total_bytes=%d",
            self._port,
            self.file_id,
            self.total_bytes,
        )
        return self

    def _cancel_all_tasks(self) -> list[asyncio.Task[Any]]:
        tasks: list[asyncio.Task[Any]] = []
        for task in list(self._background_tasks):
            task.cancel()
            tasks.append(task)
        for ch_state in self._channels.values():
            for task in list(ch_state.in_flight.values()):
                task.cancel()
                tasks.append(task)
            ch_state.in_flight.clear()
            ch_state.pages.clear()
        for task in list(self._span_in_flight.values()):
            task.cancel()
            tasks.append(task)
        self._span_in_flight.clear()
        self._span_cache.clear()
        return tasks

    async def _gather_cancelled_tasks(
        self,
        tasks: list[asyncio.Task[Any]],
        label: str,
    ) -> asyncio.CancelledError | None:
        if not tasks:
            return None
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError as cancel_err:
            return cancel_err
        except Exception as gather_err:
            logger.debug("[LocalPagedRangeProxy] %s异常 (非致命): %s", label, gather_err)
        finally:
            self._background_tasks.clear()
        return None

    async def _cleanup_runner_uninterruptibly(self) -> asyncio.CancelledError | None:
        runner = self._runner
        if runner is None:
            return None
        cleanup_task = asyncio.create_task(runner.cleanup())
        caller_cancelled: asyncio.CancelledError | None = None
        try:
            while not cleanup_task.done():
                try:
                    await asyncio.shield(cleanup_task)
                except asyncio.CancelledError as cancel_err:
                    if caller_cancelled is None:
                        caller_cancelled = cancel_err
                except Exception as runner_err:
                    logger.debug("[LocalPagedRangeProxy] runner.cleanup 异常 (非致命): %s", runner_err)
                    break
        finally:
            self._runner = None
            self._site = None
            self._app = None
        return caller_cancelled

    async def close(self) -> None:
        """关闭代理服务并释放所有资源。"""
        if self._closed:
            return
        self._closed = True
        caller_cancelled: asyncio.CancelledError | None = None

        try:
            # 1. 先取消当前在途与后台预读任务, 让正在等待 read_chunk 的 handler 快速退出而非卡死 cleanup
            caller_cancelled = await self._gather_cancelled_tasks(self._cancel_all_tasks(), "取消在途任务")
        finally:
            # 2. 不可中断地停止接入并清理 aiohttp 站点与 runner (排空全部活跃 handler)
            try:
                runner_cancelled = await self._cleanup_runner_uninterruptibly()
                if caller_cancelled is None:
                    caller_cancelled = runner_cancelled
            finally:
                # 3. 终扫: 无论前两步是否遭遇外部取消, 均清理关闭窗口内留下的尾部任务
                straggler_cancelled = await self._gather_cancelled_tasks(self._cancel_all_tasks(), "终扫取消任务")
                if caller_cancelled is None:
                    caller_cancelled = straggler_cancelled

        if caller_cancelled is not None:
            raise caller_cancelled

        logger.debug("[LocalPagedRangeProxy] 服务已停止: file_id=%s", self.file_id)

    async def stop(self) -> None:
        """stop 别名 (对齐 close)。"""
        await self.close()

    async def aclose(self) -> None:
        """aclose 别名 (对齐 close)。"""
        await self.close()

    async def __aenter__(self) -> LocalPagedRangeProxy:
        return await self.start()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    def _get_channel_state(self, channel: str) -> _ChannelState:
        norm_ch = channel.strip("/").lower() or "main"
        if norm_ch not in self._channels:
            if norm_ch == "audio":
                p_size = self.audio_page_size
            else:
                p_size = self.page_size
            self._channels[norm_ch] = _ChannelState(
                name=norm_ch,
                page_size=p_size,
                max_pages=self.max_pages_per_channel,
            )
        return self._channels[norm_ch]

    def _find_pinned_region(self, offset: int) -> tuple[int, int, bytes] | None:
        """查找覆盖目标 offset 的 L0 钉死热缓存区域。"""
        for p_start, p_end, p_data in self._pinned_regions:
            if p_start <= offset < p_end:
                return (p_start, p_end, p_data)
        return None

    def _find_audio_span(self, offset: int) -> tuple[int, int] | None:
        """二分查找覆盖目标 offset 的 Tier 2 音频活跃区间。"""
        if not self._audio_spans:
            return None
        idx = bisect.bisect_right(self._audio_span_starts, offset) - 1
        if idx >= 0:
            span = self._audio_spans[idx]
            if span[0] <= offset < span[1]:
                return span
        return None

    async def _fetch_page_data(self, state: _ChannelState, page_idx: int) -> bytes:
        """实际执行底层单页拉取 (被单一 asyncio.Task 托管)。"""
        try:
            offset = page_idx * state.page_size
            size = min(state.page_size, self.total_bytes - offset)
            if size <= 0:
                data = b""
            else:
                # 检查页起始是否部分或全部覆盖 L0 Pinned 区域
                pinned = self._find_pinned_region(offset)
                if pinned is not None:
                    p_start, p_end, p_data = pinned
                    if offset + size <= p_end:
                        # 100% 内存直接覆盖
                        data = p_data[offset - p_start : offset + size - p_start]
                        self.l0_hits += 1
                    else:
                        # 前半段命中 Pinned, 仅后半段拉取远端
                        head_part = p_data[offset - p_start : p_end - p_start]
                        rem_offset = p_end
                        rem_size = offset + size - p_end
                        self.remote_fetches += 1
                        tail_part = await self._read_provider_chunk(rem_offset, rem_size)
                        self.bytes_fetched += len(tail_part)
                        self.l0_hits += 1
                        data = head_part + tail_part
                else:
                    self.remote_fetches += 1
                    data = await self._read_provider_chunk(offset, size)
                    self.bytes_fetched += len(data)

            state.pages[page_idx] = data
            state.pages.move_to_end(page_idx)
            while len(state.pages) > state.max_pages:
                state.pages.popitem(last=False)
            return data
        finally:
            state.in_flight.pop(page_idx, None)

    async def _get_page_bytes(self, state: _ChannelState, page_idx: int, *, is_prefetch: bool = False) -> bytes:
        """获取 L1 分页缓存, 附带单飞去重 (Single-Flight Dedup)。"""
        # 1. 命中 L1 内存缓存
        if page_idx in state.pages:
            state.pages.move_to_end(page_idx)
            self.l1_hits += 1
            if not is_prefetch:
                self._maybe_trigger_prefetch(state, page_idx)
            return state.pages[page_idx]

        # 2. 命中在途单飞请求或创建全新在途任务
        task = state.in_flight.get(page_idx)
        if task is None:
            if self._closed:
                raise asyncio.CancelledError("LocalPagedRangeProxy is closed")
            task = asyncio.create_task(self._fetch_page_data(state, page_idx))
            task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
            state.in_flight[page_idx] = task

        data = await asyncio.shield(task)
        if not is_prefetch:
            self._maybe_trigger_prefetch(state, page_idx)
        return data

    async def _fetch_span_data(self, span_start: int, span_end: int) -> bytes:
        """实际执行 Tier 2 音频 Span 数据拉取 (被单一 asyncio.Task 托管)。"""
        try:
            span_len = span_end - span_start
            if span_len <= 0:
                return b""

            self.remote_fetches += 1
            data = await self._read_provider_chunk(span_start, span_len)
            self.bytes_fetched += len(data)
            self._span_cache[span_start] = data
            self._span_cache.move_to_end(span_start)
            while len(self._span_cache) > self.max_cached_spans:
                self._span_cache.popitem(last=False)
            return data
        finally:
            self._span_in_flight.pop(span_start, None)

    async def _get_span_bytes(self, span_start: int, span_end: int, *, is_prefetch: bool = False) -> bytes:
        """获取 Tier 2 音频活跃区间数据, 附带单飞去重 (Single-Flight Dedup) 与滑动窗口并发预读。"""
        span_len = span_end - span_start
        if span_len <= 0:
            return b""

        # 1. 命中 Span 内存缓存
        if span_start in self._span_cache:
            self._span_cache.move_to_end(span_start)
            self.l1_hits += 1
            if not is_prefetch:
                self._maybe_trigger_span_prefetch(span_start)
            return self._span_cache[span_start]

        # 2. 命中在途单飞请求或创建全新任务
        task = self._span_in_flight.get(span_start)
        if task is None:
            if self._closed:
                raise asyncio.CancelledError("LocalPagedRangeProxy is closed")
            task = asyncio.create_task(self._fetch_span_data(span_start, span_end))
            task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
            self._span_in_flight[span_start] = task

        if not is_prefetch:
            self._maybe_trigger_span_prefetch(span_start)
        return await asyncio.shield(task)

    def _maybe_trigger_span_prefetch(self, current_span_start: int) -> None:
        """当访问 Tier 2 音频 Span 时，自动触发后续 span_prefetch_window 个音频区间的后台并发预读。"""
        if self._closed or not self.enable_prefetch or self.span_prefetch_window <= 0 or not self._audio_spans:
            return
        idx = bisect.bisect_left(self._audio_span_starts, current_span_start)
        if idx >= len(self._audio_spans) or self._audio_spans[idx][0] != current_span_start:
            return

        limit_idx = min(len(self._audio_spans), idx + 1 + self.span_prefetch_window)
        for next_idx in range(idx + 1, limit_idx):
            n_start, n_end = self._audio_spans[next_idx]
            if n_start not in self._span_cache and n_start not in self._span_in_flight:
                task = asyncio.create_task(self._prefetch_span(n_start, n_end))
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)

    async def _prefetch_span(self, span_start: int, span_end: int) -> None:
        """后台音频 Span 预读协程: 遇到取消或异常时静默记录日志并退出。"""
        try:
            await self._get_span_bytes(span_start, span_end, is_prefetch=True)
        except asyncio.CancelledError:
            logger.debug("[LocalPagedRangeProxy] 音频 Span 预读已取消: span=[%d, %d)", span_start, span_end)
        except Exception as exc:
            logger.debug(
                "[LocalPagedRangeProxy] 音频 Span 预读失败: span=[%d, %d), err=%s",
                span_start,
                span_end,
                exc,
            )

    def _maybe_trigger_prefetch(self, state: _ChannelState, page_idx: int) -> None:
        """检测到顺序读页 (page_idx == prev_page_idx + 1) 时自动触发后台预读 page_idx + 1。"""
        prev_page = state.last_accessed_page
        state.last_accessed_page = page_idx
        if self._closed or not self.enable_prefetch:
            return
        if prev_page is not None and page_idx == prev_page + 1:
            next_page = page_idx + 1
            next_offset = next_page * state.page_size
            if next_offset < self.total_bytes:
                if next_page not in state.pages and next_page not in state.in_flight:
                    task = asyncio.create_task(self._prefetch_page(state, next_page))
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)

    async def _prefetch_page(self, state: _ChannelState, page_idx: int) -> None:
        """后台预读协程: 遇到取消或异常时静默记录日志并退出。"""
        try:
            await self._get_page_bytes(state, page_idx, is_prefetch=True)
        except asyncio.CancelledError:
            logger.debug("[LocalPagedRangeProxy] 预读任务已取消: ch=%s, page=%d", state.name, page_idx)
        except Exception as exc:
            logger.debug("[LocalPagedRangeProxy] 预读失败: ch=%s, page=%d, err=%s", state.name, page_idx, exc)

    async def _handle_request(self, request: web.Request) -> web.StreamResponse:
        """统一处理 HTTP GET 与 HEAD 请求 (支持 Range 与无界顺读)。"""
        if self._closed:
            return web.Response(status=503, text="Service Unavailable: Proxy Closed")
        token = request.match_info.get("token", self.session_token)
        if token != self.session_token:
            return web.Response(status=403, text="Forbidden: Invalid Session Token")

        raw_ch = request.match_info.get("channel", "main")
        channel = raw_ch.strip("/").lower() or "main"
        if self._allowed_channels is not None and channel not in self._allowed_channels:
            return web.Response(status=403, text="Forbidden: Channel Not Allowed")
        state = self._get_channel_state(channel)

        # 边界情况: 0 字节空文件处理
        if self.total_bytes == 0:
            if request.headers.get("Range"):
                return web.Response(
                    status=416,
                    headers={"Content-Range": "bytes */0"},
                    text="Requested Range Not Satisfiable",
                )
            headers = {
                "Content-Type": "application/octet-stream",
                "Accept-Ranges": "bytes",
                "Content-Length": "0",
            }
            return web.Response(status=200, body=b"", headers=headers)

        # 解析 Range 请求头
        range_header = request.headers.get("Range")
        is_range = False
        is_unbounded = False
        req_start = 0
        req_end = self.total_bytes - 1

        if range_header:
            match = _RANGE_RE.match(range_header.strip())
            if not match:
                return web.Response(status=400, text="Bad Request: Invalid Range Header")
            raw_s, raw_e = match.group(1), match.group(2)
            if not raw_s and not raw_e:
                return web.Response(status=400, text="Bad Request: Empty Range Specification")
            if not raw_s:
                # Suffix range: bytes=-N
                suffix_len = int(raw_e)
                if suffix_len == 0:
                    return web.Response(
                        status=416,
                        headers={"Content-Range": f"bytes */{self.total_bytes}"},
                        text="Requested Range Not Satisfiable",
                    )
                req_start = max(0, self.total_bytes - suffix_len)
                req_end = self.total_bytes - 1
            elif not raw_e:
                # Unbounded range: bytes=N-
                req_start = int(raw_s)
                req_end = self.total_bytes - 1
                is_unbounded = True
            else:
                req_start = int(raw_s)
                req_end = int(raw_e)

            if req_start >= self.total_bytes or req_start > req_end:
                return web.Response(
                    status=416,
                    headers={"Content-Range": f"bytes */{self.total_bytes}"},
                    text="Requested Range Not Satisfiable",
                )
            req_end = min(req_end, self.total_bytes - 1)
            is_range = True

        # HEAD 方法直接返回报头
        if request.method == "HEAD":
            status = 206 if is_range else 200
            content_length = max(0, req_end - req_start + 1)
            headers = {
                "Content-Type": "application/octet-stream",
                "Accept-Ranges": "bytes",
                "Content-Length": str(content_length),
            }
            if is_range:
                headers["Content-Range"] = f"bytes {req_start}-{req_end}/{self.total_bytes}"
            return web.Response(status=status, headers=headers)

        # =====================================================================
        # 路径 A: Tier 2 audio 通道且配置了 audio_spans
        # =====================================================================
        if channel == "audio" and self._audio_spans:
            # 当启用 zero_fill_unmapped 时，文件逻辑上全覆盖 (Pinned + Audio Spans + 零填充非音频间隙)
            # 使用 StreamResponse 进行完整流式响应，避免 FFmpeg 面对截断 206 时判定 "Stream ends prematurely"
            if self.zero_fill_unmapped:
                resp_status = 206 if is_range else 200
                headers = {
                    "Content-Type": "application/octet-stream",
                    "Accept-Ranges": "bytes",
                    "Content-Length": str(req_end - req_start + 1),
                }
                if is_range:
                    headers["Content-Range"] = f"bytes {req_start}-{req_end}/{self.total_bytes}"

                resp = web.StreamResponse(status=resp_status, headers=headers)
                await resp.prepare(request)

                cur_offset = req_start
                try:
                    while cur_offset <= req_end:
                        # 1. 优先命中 L0 Pinned 内存
                        p = self._find_pinned_region(cur_offset)
                        if p is not None:
                            p_start, p_end, p_data = p
                            avail = len(p_data) - (cur_offset - p_start)
                            if avail <= 0:
                                break
                            take_len = min(req_end + 1 - cur_offset, p_end - cur_offset, avail)
                            chunk = p_data[cur_offset - p_start : cur_offset - p_start + take_len]
                            if not chunk:
                                break
                            await resp.write(chunk)
                            self.l0_hits += 1
                            self.bytes_served += len(chunk)
                            cur_offset += len(chunk)
                            continue

                        # 2. 命中 Tier 2 音频活跃区间
                        span = self._find_audio_span(cur_offset)
                        if span is not None:
                            s_start, s_end = span
                            span_data = await self._get_span_bytes(s_start, s_end)
                            avail = len(span_data) - (cur_offset - s_start)
                            if avail <= 0:
                                break
                            take_len = min(req_end + 1 - cur_offset, s_end - cur_offset, avail)
                            chunk = span_data[cur_offset - s_start : cur_offset - s_start + take_len]
                            if not chunk:
                                break
                            await resp.write(chunk)
                            self.bytes_served += len(chunk)
                            cur_offset += len(chunk)
                            continue

                        # 3. 落在音频 Span 与 Pinned 区域之外的非音频间隙 (例如视频 Chunk):
                        # 直接回送零字节占位, 彻底消灭网络视频流量浪费
                        ns_idx = bisect.bisect_right(self._audio_span_starts, cur_offset)
                        next_span_start = (
                            self._audio_span_starts[ns_idx]
                            if ns_idx < len(self._audio_span_starts)
                            else self.total_bytes
                        )
                        next_pinned_start = next(
                            (p[0] for p in self._pinned_regions if p[0] > cur_offset), self.total_bytes
                        )
                        next_boundary = min(next_span_start, next_pinned_start, req_end + 1, self.total_bytes)
                        take_len = min(state.page_size, next_boundary - cur_offset)
                        if take_len <= 0:
                            break
                        chunk = b"\x00" * take_len
                        await resp.write(chunk)
                        self.bytes_served += len(chunk)
                        cur_offset += len(chunk)

                    await resp.write_eof()
                except (ConnectionError, asyncio.CancelledError) as exc:
                    logger.debug(
                        "[LocalPagedRangeProxy] 音频客户端断开连接 (channel=%s, offset=%d): %s",
                        channel,
                        cur_offset,
                        exc,
                    )
                return resp

            # 未启用 zero_fill_unmapped: 保持有界单块响应语义
            # 1. 优先命中 L0 Pinned 区域
            pinned = self._find_pinned_region(req_start)
            if pinned is not None:
                p_start, p_end, p_data = pinned
                chunk_end = min(req_end, p_end - 1)
                body = p_data[req_start - p_start : chunk_end + 1 - p_start]
                self.l0_hits += 1
                self.bytes_served += len(body)
                headers = {
                    "Content-Type": "application/octet-stream",
                    "Accept-Ranges": "bytes",
                    "Content-Range": f"bytes {req_start}-{chunk_end}/{self.total_bytes}",
                    "Content-Length": str(len(body)),
                }
                return web.Response(status=206, body=body, headers=headers)

            # 2. 命中 Tier 2 音频活跃区间
            span = self._find_audio_span(req_start)
            if span is not None:
                s_start, s_end = span
                chunk_end = min(req_end, s_end - 1)
                span_data = await self._get_span_bytes(s_start, s_end)
                body = span_data[req_start - s_start : chunk_end + 1 - s_start]
                self.bytes_served += len(body)
                headers = {
                    "Content-Type": "application/octet-stream",
                    "Accept-Ranges": "bytes",
                    "Content-Range": f"bytes {req_start}-{chunk_end}/{self.total_bytes}",
                    "Content-Length": str(len(body)),
                }
                return web.Response(status=206, body=body, headers=headers)

            # 3. 落在音频 Span 之外的探测/回跳: 按有界紧凑块拉取, 且不越过下一个音频 Span 起点或 Pinned 边界
            next_span_start = next((s for s in self._audio_span_starts if s > req_start), self.total_bytes)
            next_pinned_start = next((p[0] for p in self._pinned_regions if p[0] > req_start), self.total_bytes)
            next_boundary = min(next_span_start, next_pinned_start, self.total_bytes)
            max_len = min(state.page_size, next_boundary - req_start)
            chunk_end = min(req_end, req_start + max_len - 1)
            fetch_size = chunk_end - req_start + 1

            body = await self._read_provider_chunk(req_start, fetch_size)
            self.remote_fetches += 1
            self.bytes_fetched += len(body)

            self.bytes_served += len(body)
            headers = {
                "Content-Type": "application/octet-stream",
                "Accept-Ranges": "bytes",
                "Content-Range": f"bytes {req_start}-{chunk_end}/{self.total_bytes}",
                "Content-Length": str(len(body)),
            }
            return web.Response(status=206, body=body, headers=headers)

        # =====================================================================
        # 路径 B: video / main 通道 (或未指定 spans 的通用通道)
        # =====================================================================
        # 1. 检查请求起始是否落在 L0 Pinned 区域内
        pinned = self._find_pinned_region(req_start)
        if pinned is not None:
            p_start, p_end, p_data = pinned
            # 若请求完全落在 Pinned 区域内，或客户端发起无界 Range 探测 (bytes=N-)，直接截断返回有界片段
            if req_end <= p_end - 1 or is_unbounded:
                chunk_end = min(req_end, p_end - 1)
                body = p_data[req_start - p_start : chunk_end + 1 - p_start]
                self.l0_hits += 1
                self.bytes_served += len(body)
                headers = {
                    "Content-Type": "application/octet-stream",
                    "Accept-Ranges": "bytes",
                    "Content-Length": str(len(body)),
                }
                if is_range:
                    headers["Content-Range"] = f"bytes {req_start}-{chunk_end}/{self.total_bytes}"
                return web.Response(status=206 if is_range else 200, body=body, headers=headers)

        # 2. 检查请求是否落在单页范围内 (非跨页单块请求直接返回普通 Response)
        page_idx = req_start // state.page_size
        end_page = req_end // state.page_size
        if page_idx == end_page and pinned is None:
            page_data = await self._get_page_bytes(state, page_idx)
            page_start = page_idx * state.page_size
            s_offset = req_start - page_start
            e_offset = req_end - page_start
            body = page_data[s_offset : e_offset + 1]
            self.bytes_served += len(body)
            headers = {
                "Content-Type": "application/octet-stream",
                "Accept-Ranges": "bytes",
                "Content-Length": str(len(body)),
            }
            if is_range:
                headers["Content-Range"] = f"bytes {req_start}-{req_end}/{self.total_bytes}"
            return web.Response(status=206 if is_range else 200, body=body, headers=headers)

        # 3. 跨页或大范围/无界拉流: 启动 StreamResponse 流式输出
        resp_status = 206 if is_range else 200
        headers = {
            "Content-Type": "application/octet-stream",
            "Accept-Ranges": "bytes",
            "Content-Length": str(req_end - req_start + 1),
        }
        if is_range:
            headers["Content-Range"] = f"bytes {req_start}-{req_end}/{self.total_bytes}"

        resp = web.StreamResponse(status=resp_status, headers=headers)
        await resp.prepare(request)

        cur_offset = req_start
        try:
            while cur_offset <= req_end:
                # 优先消耗 Pinned 内存
                p = self._find_pinned_region(cur_offset)
                if p is not None:
                    p_start, p_end, p_data = p
                    take_len = min(req_end + 1 - cur_offset, p_end - cur_offset)
                    chunk = p_data[cur_offset - p_start : cur_offset - p_start + take_len]
                    await resp.write(chunk)
                    self.l0_hits += 1
                    self.bytes_served += len(chunk)
                    cur_offset += len(chunk)
                    continue

                # 消费 L1 分页
                p_idx = cur_offset // state.page_size
                p_data_bytes = await self._get_page_bytes(state, p_idx)
                p_start_offset = p_idx * state.page_size
                offset_in_page = cur_offset - p_start_offset
                avail = len(p_data_bytes) - offset_in_page
                if avail <= 0:
                    break
                take_len = min(req_end + 1 - cur_offset, avail)
                chunk = p_data_bytes[offset_in_page : offset_in_page + take_len]
                await resp.write(chunk)
                self.bytes_served += len(chunk)
                cur_offset += len(chunk)

            await resp.write_eof()
        except (ConnectionError, asyncio.CancelledError) as exc:
            logger.debug(
                "[LocalPagedRangeProxy] 客户端断开连接 (channel=%s, offset=%d): %s",
                channel,
                cur_offset,
                exc,
            )

        return resp


__all__ = ["LocalPagedRangeProxy", "build_pinned_regions", "coalesce_byte_spans"]
