"""FastTelethon 上传预读生产者协调器: 本地磁盘预读 / Provider 并发拉流与滑动窗口保序提交.

纯搬迁自原 app/core/fast_telethon.py 的 _UploadSessionState 预读半区 (Step 3 结构抽离,
零运行时逻辑改动)。与 _UploadSessionState 显式解耦: 全部依赖经构造注入 (队列/停止事件/
失败计数共享 dict/熔断回调/数据面只读引用/动态并发取值器), 不反向持有会话对象;
会话侧经 provider_read / run_prefetch_producer 同名委托保持外部行为与监控指标等价。
"""

import asyncio
import logging
import random
import time
from collections.abc import Callable
from contextlib import suppress
from typing import Any

from app.config import settings
from app.core.stream_chunk_provider import BaseChunkProvider, ChunkFetchTransientError, SlidingWindowRingBuffer

from . import breakpoints
from .primitives import AdaptivePrefetchQueue

logger = logging.getLogger(__name__)


def _inspect_source_total_bytes(provider: Any) -> int:
    """遍历 Provider 包装层 (_inner, _source_provider)，提取最底层源文件的原始 total_bytes。"""
    curr = provider
    visited: set[int] = set()
    while curr is not None and id(curr) not in visited:
        visited.add(id(curr))
        if hasattr(curr, "_inner"):
            curr = curr._inner
        elif hasattr(curr, "inner"):
            curr = curr.inner
        elif hasattr(curr, "_source_provider"):
            curr = curr._source_provider
        elif hasattr(curr, "source_provider"):
            curr = curr.source_provider
        else:
            break
    if curr is not None and hasattr(curr, "total_bytes"):
        try:
            return int(curr.total_bytes)
        except (TypeError, ValueError) as parse_err:
            logger.debug("解析 provider.total_bytes 异常: %s", parse_err)
    return 0


def _create_ring_buffer_if_enabled(chunk_provider: BaseChunkProvider | None) -> SlidingWindowRingBuffer | None:
    if chunk_provider is None or not settings.PIKPAK_ENABLE_RING_BUFFER:
        return None
    from app.db.account_repository import SystemSettingsRepository

    ring_mb = SystemSettingsRepository.get_int("pikpak_ring_buffer_mb", settings.PIKPAK_RING_BUFFER_MB)
    return SlidingWindowRingBuffer(ring_mb * 1024 * 1024)


def _drain_async_queue_nowait(q: asyncio.Queue) -> None:
    while not q.empty():
        try:
            q.get_nowait()
        except Exception:
            break


class _UploadPrefetchCoordinator:
    """上传预读生产者协调器 (本地磁盘 to_thread 预读 / Provider 并发拉流 + 金丝雀探针 + 保序提交)."""

    def __init__(
        self,
        *,
        prefetch_queue: AdaptivePrefetchQueue,
        retry_queue: asyncio.Queue[tuple[int, bytes | None, float]],
        stop_event: asyncio.Event,
        part_failed_counts: dict[int, int],
        on_fatal_error: Callable[[BaseException], None],
        chunk_provider: BaseChunkProvider | None,
        ring_buffer: SlidingWindowRingBuffer | None,
        file_descriptor: Any,
        pending_part_ids: list[int],
        chunk_size: int,
        total_bytes: int,
        get_target_concurrency: Callable[[], int],
        get_target_in_flight: Callable[[], int],
        get_target_prefetch_concurrency: Callable[[], int],
    ) -> None:
        self.prefetch_queue = prefetch_queue
        self.retry_queue = retry_queue
        self.stop_event = stop_event
        self.part_failed_counts = part_failed_counts
        self.on_fatal_error = on_fatal_error
        self.chunk_provider = chunk_provider
        self.ring_buffer = ring_buffer
        self.file_descriptor = file_descriptor
        self.pending_part_ids = pending_part_ids
        self.chunk_size = chunk_size
        self.total_bytes = total_bytes
        self.get_target_concurrency = get_target_concurrency
        self.get_target_in_flight = get_target_in_flight
        self.get_target_prefetch_concurrency = get_target_prefetch_concurrency

    async def provider_read(self, offset: int, size: int) -> bytes:
        if self.ring_buffer is not None:
            cached = self.ring_buffer.get(offset)
            if cached is not None and len(cached) == size:
                return cached
        assert self.chunk_provider is not None
        data = await self.chunk_provider.read_chunk(offset, size)
        if self.ring_buffer is not None:
            self.ring_buffer.put(offset, data)
        return data

    async def _run_local_prefetch(self) -> None:
        current_read_task: asyncio.Task | None = None
        try:
            for part_id in self.pending_part_ids:
                if self.stop_event.is_set():
                    break
                self.prefetch_queue.tune(self.get_target_concurrency(), self.get_target_in_flight())
                offset = part_id * self.chunk_size
                size = min(self.chunk_size, self.total_bytes - offset)
                t_read_start = time.perf_counter()
                current_read_task = asyncio.create_task(
                    asyncio.to_thread(breakpoints._sync_read_chunk, self.file_descriptor, offset, size)
                )
                try:
                    chunk_data = await asyncio.shield(current_read_task)
                except asyncio.CancelledError:
                    with suppress(Exception):
                        await current_read_task
                    raise
                read_ms = (time.perf_counter() - t_read_start) * 1000.0
                if self.stop_event.is_set():
                    break
                if len(chunk_data) < size:
                    raise OSError(f"文件读取不完整: 预期 {size} 字节，实际读取 {len(chunk_data)} 字节")
                await self.prefetch_queue.put((part_id, chunk_data, read_ms))
        except asyncio.CancelledError:
            logger.debug("预读工作协程正常取消退出")
        except Exception as e:
            logger.error("预读流水线异常: %s", e)
            self.on_fatal_error(e)

    async def _apply_prefetch_pacing(self) -> bool:
        q_max = getattr(self.prefetch_queue, "maxsize", 0) or getattr(self.prefetch_queue, "min_size", 32)
        if self.prefetch_queue.qsize() >= (q_max // 2):
            await asyncio.sleep(random.uniform(0.05, 0.25))
        return not self.stop_event.is_set()

    async def _fetch_provider_part_with_retries(self, part_id: int) -> tuple[bytes, float] | None:
        offset = part_id * self.chunk_size
        size = min(self.chunk_size, self.total_bytes - offset)
        max_micro_retries = 2
        backoffs = (0.2, 0.4)

        for attempt in range(max_micro_retries + 1):
            if not await self._apply_prefetch_pacing():
                return None
            try:
                t_read_start = time.perf_counter()
                chunk_data = await self.provider_read(offset, size)
                read_ms = (time.perf_counter() - t_read_start) * 1000.0
                return (chunk_data, read_ms)
            except ChunkFetchTransientError as e:
                self.part_failed_counts[part_id] = self.part_failed_counts.get(part_id, 0) + 1
                if self.part_failed_counts[part_id] >= 5:
                    logger.error("❌ 读取块 part_id=%d 瞬断超过 5 次: %s", part_id, e)
                    self.on_fatal_error(e)
                    return None
                if attempt < max_micro_retries:
                    backoff = backoffs[attempt]
                    logger.warning(
                        "⚠️ [Prefetch] 读取块 part_id=%d 遭遇微观瞬断 (尝试 %d/%d): %s, 退避 %.1fs",
                        part_id,
                        attempt + 1,
                        max_micro_retries,
                        e,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                else:
                    logger.warning("⚠️ [Prefetch] 读取块 part_id=%d 微观重试耗尽, 委托 retry_queue: %s", part_id, e)
                    self.retry_queue.put_nowait((part_id, None, 0.0))
                    return None
        return None

    async def _provider_fetch_worker(
        self,
        part_id: int,
        sem: asyncio.Semaphore,
        reorder_buffer: dict[int, tuple[bytes, float] | None],
        commit_event: asyncio.Event,
    ) -> None:
        try:
            async with sem:
                if self.stop_event.is_set():
                    return
                reorder_buffer[part_id] = await self._fetch_provider_part_with_retries(part_id)
                commit_event.set()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("预读块 part_id=%d 异常: %s", part_id, e)
            self.on_fatal_error(e)
            reorder_buffer[part_id] = None
            commit_event.set()

    async def _flush_ready_reorder_items(
        self,
        next_commit_idx: int,
        total_parts: int,
        reorder_buffer: dict[int, tuple[bytes, float] | None],
        commit_advanced_event: asyncio.Event,
    ) -> int:
        while next_commit_idx < total_parts and self.pending_part_ids[next_commit_idx] in reorder_buffer:
            target_pid = self.pending_part_ids[next_commit_idx]
            item = reorder_buffer.pop(target_pid)
            next_commit_idx += 1
            commit_advanced_event.set()

            if item is not None:
                c_data, r_ms = item
                exp_size = min(self.chunk_size, self.total_bytes - target_pid * self.chunk_size)
                if len(c_data) < exp_size:
                    raise OSError(f"文件读取不完整: part_id={target_pid}")
                self.prefetch_queue.tune(self.get_target_concurrency(), self.get_target_in_flight())
                await self.prefetch_queue.put((target_pid, c_data, r_ms))
        return next_commit_idx

    async def _run_reorder_commit_loop(
        self,
        state_idx: dict[str, int],
        total_parts: int,
        reorder_buffer: dict[int, tuple[bytes, float] | None],
        commit_event: asyncio.Event,
        commit_advanced_event: asyncio.Event,
    ) -> None:
        try:
            while state_idx["next_commit_idx"] < total_parts and not self.stop_event.is_set():
                commit_event.clear()
                state_idx["next_commit_idx"] = await self._flush_ready_reorder_items(
                    state_idx["next_commit_idx"], total_parts, reorder_buffer, commit_advanced_event
                )
                if state_idx["next_commit_idx"] >= total_parts or self.stop_event.is_set():
                    break
                if self.pending_part_ids[state_idx["next_commit_idx"]] in reorder_buffer:
                    continue
                with suppress(TimeoutError):
                    await asyncio.wait_for(commit_event.wait(), timeout=0.2)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("保序提交流水线异常: %s", e)
            self.on_fatal_error(e)

    async def _wait_for_reorder_window_slot(
        self,
        dispatch_idx: int,
        state_idx: dict[str, int],
        max_reorder_window: int,
        commit_task: asyncio.Task,
        commit_advanced_event: asyncio.Event,
    ) -> bool:
        while (
            not self.stop_event.is_set()
            and not commit_task.done()
            and dispatch_idx - state_idx["next_commit_idx"] >= max_reorder_window
        ):
            commit_advanced_event.clear()
            if dispatch_idx - state_idx["next_commit_idx"] < max_reorder_window:
                break
            with suppress(TimeoutError):
                await asyncio.wait_for(commit_advanced_event.wait(), timeout=0.2)
        return not (self.stop_event.is_set() or commit_task.done())

    async def _cleanup_prefetch_tasks(
        self, commit_task: asyncio.Task, in_flight_fetch_tasks: set[asyncio.Task]
    ) -> None:
        if not commit_task.done():
            commit_task.cancel()
            with suppress(asyncio.CancelledError):
                await commit_task
        for t in list(in_flight_fetch_tasks):
            if not t.done():
                t.cancel()
        if in_flight_fetch_tasks:
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*in_flight_fetch_tasks, return_exceptions=True)

    async def _run_provider_prefetch(self) -> None:
        concurrency = self.get_target_prefetch_concurrency()
        sem = asyncio.Semaphore(concurrency)
        reorder_buffer: dict[int, tuple[bytes, float] | None] = {}
        commit_event = asyncio.Event()
        commit_advanced_event = asyncio.Event()
        in_flight_fetch_tasks: set[asyncio.Task] = set()
        max_reorder_window = min(16, max(8, concurrency * 2))
        state_idx = {"next_commit_idx": 0}
        total_parts = len(self.pending_part_ids)

        commit_task = asyncio.create_task(
            self._run_reorder_commit_loop(state_idx, total_parts, reorder_buffer, commit_event, commit_advanced_event)
        )
        try:
            for dispatch_idx, part_id in enumerate(self.pending_part_ids):
                can_dispatch = await self._wait_for_reorder_window_slot(
                    dispatch_idx, state_idx, max_reorder_window, commit_task, commit_advanced_event
                )
                if not can_dispatch:
                    break

                fetch_t = asyncio.create_task(self._provider_fetch_worker(part_id, sem, reorder_buffer, commit_event))
                in_flight_fetch_tasks.add(fetch_t)
                fetch_t.add_done_callback(in_flight_fetch_tasks.discard)

                if dispatch_idx < 16:
                    await fetch_t
                    if (dispatch_idx == 15 or dispatch_idx == total_parts - 1) and not self.stop_event.is_set():
                        logger.info("🐥 金丝雀探针成功完成前 %d 块校验，平滑放开至目标并发拉流", dispatch_idx + 1)

            if in_flight_fetch_tasks:
                await asyncio.gather(*in_flight_fetch_tasks, return_exceptions=True)
            await commit_task
        except asyncio.CancelledError:
            logger.debug("流水线总控协程被取消退出")
        finally:
            await self._cleanup_prefetch_tasks(commit_task, in_flight_fetch_tasks)
            reorder_buffer.clear()

    async def run_prefetch_producer(self) -> None:
        if self.chunk_provider is None:
            await self._run_local_prefetch()
        else:
            await self._run_provider_prefetch()
