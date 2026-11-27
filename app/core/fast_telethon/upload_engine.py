"""FastTelethon 多通道并行上传状态机: 通道池调度、单连接在途管线化、分块 RPC、FloodWait 退避与主入口.

预读/保序流水线已抽离至 prefetch.py (_UploadPrefetchCoordinator, 经同名委托保持
外部行为与监控指标等价)。跨子模块函数调用统一走模块对象引用 (sender_pool./
breakpoints./primitives./prefetch.), 保证单测对子模块属性 monkeypatch.setattr
时不会被早绑定引用击穿。
"""

import asyncio
import io
import logging
import math
import random
import time
from collections.abc import Callable
from contextlib import suppress
from typing import Any, cast

from telethon import TelegramClient, types
from telethon.errors import FloodError, FloodWaitError
from telethon.errors.rpcbaseerrors import BadRequestError
from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest

from app.config import settings
from app.core.stream_chunk_provider import (
    BaseChunkProvider,
    ChunkFetchTransientError,
    ChunkProviderError,
    SlidingWindowRingBuffer,
)

from . import breakpoints, prefetch, primitives, sender_pool
from .breakpoints import _UploadMetaSaver
from .prefetch import _UploadPrefetchCoordinator
from .primitives import AdaptivePrefetchQueue, SpeedTracker, _CachedTargetResolver, _DynamicInFlightLimiter
from .sender_pool import FloodPremiumWaitError

logger = logging.getLogger(__name__)


async def _upload_small_file_direct(
    primary_client: TelegramClient,
    file_path: str,
    file_name: str,
    total_bytes: int,
    chunk_provider: BaseChunkProvider | None,
    meta_path: str | None,
    progress_callback: Callable[[int, int, float, float, int, int, int], None] | None,
) -> types.InputFile | types.InputFileBig:
    if chunk_provider is not None:
        if not meta_path:
            raise ValueError("Provider 模式必须显式注入 meta_path (沙箱化断点元数据路径)")
        fast_data = await chunk_provider.read_chunk(0, total_bytes)
        return await primary_client.upload_file(
            io.BytesIO(fast_data),
            file_name=file_name,
            progress_callback=lambda c, t: (
                progress_callback(c, t, 0.0, 0.0, 1, primary_client.session.dc_id, 1) if progress_callback else None
            ),
        )
    return await primary_client.upload_file(
        file_path,
        progress_callback=lambda c, t: (
            progress_callback(c, t, 0.0, 0.0, 1, primary_client.session.dc_id, 1) if progress_callback else None
        ),
    )


def _build_input_file_result(
    is_big: bool, file_id: int, num_chunks: int, file_name: str
) -> types.InputFile | types.InputFileBig:
    if is_big:
        return types.InputFileBig(id=file_id, parts=num_chunks, name=file_name)
    return types.InputFile(id=file_id, parts=num_chunks, name=file_name, md5_checksum="")


async def _init_upload_worker_sender(
    i: int,
    clients: list[TelegramClient],
    dc_id: int,
    dc_options: list[Any],
    auth_cache: dict[Any, Any],
) -> Any:
    """薄委托 sender_pool._init_worker_sender (tune_socket=True: 上传侧建连后套接字加固)。"""
    return await sender_pool._init_worker_sender(
        i, clients, dc_id, dc_options, auth_cache, tune_socket=True, log_tag="FastTelethon Upload"
    )


async def _execute_upload_part_rpc(worker_client: TelegramClient, sender: Any, req: Any) -> None:
    res: Any = True
    if sender:
        client_call = getattr(worker_client, "_call", None)
        is_telethon_standard_call = (
            client_call is not None and getattr(client_call, "__func__", None) is TelegramClient._call
        )
        if client_call is not None and not is_telethon_standard_call:
            res = await asyncio.wait_for(client_call(sender, req), timeout=30.0)
        elif hasattr(sender, "send"):
            fut = sender.send(req)
            if isinstance(fut, list):
                fut = fut[0]
            if hasattr(fut, "__await__") or isinstance(fut, (asyncio.Future, asyncio.Task)):
                res = await asyncio.wait_for(fut, timeout=30.0)
            else:
                res = fut
        elif client_call is not None:
            res = await asyncio.wait_for(client_call(sender, req), timeout=30.0)
    else:
        res = await asyncio.wait_for(worker_client(req), timeout=30.0)
    if res is False:
        raise RuntimeError(f"Telegram returned False for {type(req).__name__} (part={getattr(req, 'file_part', '?')})")


def _populate_upload_metrics_out(
    metrics_out: dict[str, Any] | None,
    rpc_latencies: list[float],
    chunk_retry_count: int,
    sender_reconnect_count: int,
) -> None:
    if metrics_out is None:
        return
    metrics_out["rpc_latencies"] = list(rpc_latencies)
    if rpc_latencies:
        sorted_lat = sorted(rpc_latencies)
        metrics_out["rtt_min"] = sorted_lat[0]
        metrics_out["rtt_p50"] = sorted_lat[int(len(sorted_lat) * 0.5)]
        metrics_out["rtt_p90"] = sorted_lat[int(len(sorted_lat) * 0.9)]
        metrics_out["rtt_tp95"] = sorted_lat[min(int(len(sorted_lat) * 0.95), len(sorted_lat) - 1)]
        metrics_out["rtt_tp99"] = sorted_lat[min(int(len(sorted_lat) * 0.99), len(sorted_lat) - 1)]
        metrics_out["rtt_max"] = sorted_lat[-1]
    metrics_out["retry_count"] = chunk_retry_count
    metrics_out["reconnect_count"] = sender_reconnect_count


class _UploadSessionState:
    """单次并行上传会话共享状态、预读流水线与 Worker 协调器"""

    def __init__(
        self,
        *,
        clients: list[TelegramClient],
        dc_id: int,
        file_name: str,
        file_id: int,
        is_big: bool,
        total_bytes: int,
        chunk_size: int,
        num_chunks: int,
        pending_part_ids: list[int],
        completed_parts: set[int],
        already_uploaded_bytes: int,
        meta_file_path: str,
        file_descriptor: Any,
        chunk_provider: BaseChunkProvider | None,
        ring_buffer: SlidingWindowRingBuffer | None,
        get_target_concurrency: Callable[[], int],
        get_target_in_flight: Callable[[], int],
        get_target_prefetch_concurrency: Callable[[], int],
        progress_callback: Callable[[int, int, float, float, int, int, int], None] | None,
        is_cancelled: Callable[[], bool] | None,
        is_paused: Callable[[], bool] | None,
        source_fingerprint: str | None = None,
    ) -> None:
        self.clients = clients
        self.dc_id = dc_id
        self.file_name = file_name
        self.file_id = file_id
        self.is_big = is_big
        self.total_bytes = total_bytes
        self.chunk_size = chunk_size
        self.num_chunks = num_chunks
        self.pending_part_ids = pending_part_ids
        self.completed_parts = completed_parts
        self.uploaded_bytes = already_uploaded_bytes
        self.tracker = SpeedTracker(total_bytes, initial_bytes=already_uploaded_bytes)
        self.meta_file_path = meta_file_path
        self.file_descriptor = file_descriptor
        self.chunk_provider = chunk_provider
        self.ring_buffer = ring_buffer
        self.get_target_concurrency = get_target_concurrency
        self.get_target_in_flight = get_target_in_flight
        self.get_target_prefetch_concurrency = get_target_prefetch_concurrency
        self.progress_callback = progress_callback
        self.is_cancelled = is_cancelled
        self.is_paused = is_paused

        self.meta_saver = _UploadMetaSaver(
            meta_file_path,
            total_bytes,
            file_id,
            completed_parts,
            num_chunks,
            source_fingerprint=source_fingerprint,
        )
        self.part_failed_counts: dict[int, int] = {}
        self.active_workers_count = 0
        self.flood_wait_until: float = 0.0

        self.total_read_wait_ms: float = 0.0
        self.total_rpc_wait_ms: float = 0.0
        self.chunk_retry_count: int = 0
        self.sender_reconnect_count: int = 0
        self.timeout_count: int = 0
        self.active_in_flight_count: int = 0
        self.rpc_latencies: list[float] = []

        self.last_meta_save_time: float = time.time()
        self.last_meta_save_parts_count: int = len(completed_parts)

        self.fatal_error: BaseException | None = None
        self.last_part_error: BaseException | None = None

        if chunk_provider is not None:
            self.prefetch_queue = AdaptivePrefetchQueue(min_size=24, max_size=48)
        else:
            self.prefetch_queue = AdaptivePrefetchQueue(min_size=32, max_size=64)
        self.retry_queue: asyncio.Queue[tuple[int, bytes | None, float]] = asyncio.Queue()
        self.stop_event = asyncio.Event()
        self.prefetcher = _UploadPrefetchCoordinator(
            prefetch_queue=self.prefetch_queue,
            retry_queue=self.retry_queue,
            stop_event=self.stop_event,
            part_failed_counts=self.part_failed_counts,
            on_fatal_error=self.set_fatal_error,
            chunk_provider=chunk_provider,
            ring_buffer=ring_buffer,
            file_descriptor=file_descriptor,
            pending_part_ids=pending_part_ids,
            chunk_size=chunk_size,
            total_bytes=total_bytes,
            get_target_concurrency=get_target_concurrency,
            get_target_in_flight=get_target_in_flight,
            get_target_prefetch_concurrency=get_target_prefetch_concurrency,
        )

        self.worker_senders: dict[int, Any] = {}
        self.auth_cache: dict[Any, Any] = {}
        self.dc_options: list[Any] = []
        self.prefetch_task: asyncio.Task[None] | None = None
        self.heartbeat_active = True
        self.heartbeat_task: asyncio.Task[None] | None = None

    def _start_heartbeat(self) -> None:
        """按需启动心跳协程 (构造期零副作用: 建连前抛错不产生待取消的后台 Task)。"""
        if self.heartbeat_task is None:
            self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    def is_user_aborted(self) -> bool:
        return bool((self.is_cancelled and self.is_cancelled()) or (self.is_paused and self.is_paused()))

    def should_halt(self) -> bool:
        return bool(self.is_user_aborted() or self.fatal_error is not None or self.stop_event.is_set())

    def set_fatal_error(self, err: BaseException) -> None:
        if self.fatal_error is None:
            self.fatal_error = err
        self.stop_event.set()

    async def _heartbeat_loop(self) -> None:
        while self.heartbeat_active:
            await asyncio.sleep(0.8)
            if self.progress_callback:
                try:
                    self.progress_callback(
                        self.uploaded_bytes,
                        self.total_bytes,
                        self.tracker.speed,
                        self.tracker.eta,
                        self.active_workers_count,
                        self.dc_id,
                        self.get_target_concurrency(),
                    )
                except Exception as e:
                    logger.debug("上传心跳回调异常: %s", e)

    async def provider_read(self, offset: int, size: int) -> bytes:
        """预读协调器同款 RingBuffer 读写穿透 (外部行为与监控指标等价的同名委托)。"""
        return await self.prefetcher.provider_read(offset, size)

    async def init_sender(self, i: int) -> Any:
        return await _init_upload_worker_sender(i, self.clients, self.dc_id, self.dc_options, self.auth_cache)

    async def prepare_senders(self, concurrency: int) -> None:
        self._start_heartbeat()
        self.dc_options = await sender_pool._prepare_upload_dc_options(self.clients[0], self.dc_id)
        init_senders_list = await sender_pool._gather_worker_senders_with_cleanup(
            [self.init_sender(i) for i in range(concurrency)], self.clients, self.dc_id
        )
        self.worker_senders.update(dict(enumerate(init_senders_list)))
        self.tracker.reset_start_time()

    async def _close_all_worker_senders(self) -> None:
        for i, s in list(self.worker_senders.items()):
            if s:
                with suppress(Exception):
                    await sender_pool._close_worker_sender(self.clients[i % len(self.clients)], s, self.dc_id)
        self.worker_senders.clear()

    async def release_resources(self) -> None:
        self.stop_event.set()
        if self.prefetch_task and not self.prefetch_task.done():
            self.prefetch_task.cancel()
            with suppress(Exception, asyncio.CancelledError):
                await self.prefetch_task

        prefetch._drain_async_queue_nowait(self.prefetch_queue)
        prefetch._drain_async_queue_nowait(self.retry_queue)
        if self.ring_buffer is not None:
            self.ring_buffer.clear()

        if self.file_descriptor is not None and not self.file_descriptor.closed:
            with suppress(Exception):
                self.file_descriptor.close()

        self.heartbeat_active = False
        if self.heartbeat_task is not None:
            self.heartbeat_task.cancel()
            with suppress(Exception, asyncio.CancelledError):
                await self.heartbeat_task

        await self._close_all_worker_senders()
        with suppress(Exception):
            await self.meta_saver.flush()

    async def run_prefetch_producer(self) -> None:
        """预读生产者委托 (本地磁盘预读 / Provider 并发拉流 + 保序提交均由 prefetcher 承载)。"""
        await self.prefetcher.run_prefetch_producer()

    async def _wait_for_flood_cooldown(self) -> None:
        while not self.should_halt():
            wait_rem = self.flood_wait_until - time.time()
            if wait_rem <= 0:
                break
            await asyncio.sleep(min(0.5, wait_rem))

    def _record_part_uploaded(self, part_id: int, chunk_len: int, read_ms: float, rpc_ms: float) -> None:
        if part_id not in self.completed_parts:
            self.completed_parts.add(part_id)
            self.uploaded_bytes += chunk_len
            self.tracker.update(self.uploaded_bytes)
        self.total_read_wait_ms += read_ms
        self.total_rpc_wait_ms += rpc_ms
        self.rpc_latencies.append(rpc_ms)

        now = time.time()
        if (now - self.last_meta_save_time >= 30.0) or (
            len(self.completed_parts) - self.last_meta_save_parts_count >= 256
        ):
            self.last_meta_save_time = now
            self.last_meta_save_parts_count = len(self.completed_parts)
            self.meta_saver.trigger()

    async def _handle_upload_flood_wait(
        self, worker_id: int, part_id: int, chunk_data: bytes | None, e: BaseException
    ) -> None:
        try:
            wait_s = max(int(getattr(e, "seconds", 3) or 3), 1)
        except (ValueError, TypeError):
            wait_s = 3
        self.flood_wait_until = max(self.flood_wait_until, time.time() + wait_s)
        logger.warning(
            f"⚠️ [FastTelethon Upload] Worker {worker_id} part {part_id} 遭遇 {type(e).__name__}: 需等待 {wait_s} 秒 (账号级冷却至 {wait_s}s 后)"
        )
        self.retry_queue.put_nowait((part_id, chunk_data, 0.0))
        await asyncio.sleep(wait_s)

    def _handle_upload_provider_error(self, part_id: int, e: ChunkProviderError) -> int:
        if not isinstance(e, ChunkFetchTransientError):
            self.set_fatal_error(e)
            logger.error(f"❌ 上传 part_id={part_id} 数据源触发非瞬断保护熔断: {e}")
            return 0
        self.last_part_error = e
        self.chunk_retry_count += 1
        self.part_failed_counts[part_id] = self.part_failed_counts.get(part_id, 0) + 1
        if self.part_failed_counts[part_id] < 5:
            self.retry_queue.put_nowait((part_id, None, 0.0))
        else:
            logger.error(f"❌ 上传块 part_id={part_id} 失败超过 5 次: {e}")
            self.set_fatal_error(e)
        return 1

    async def _handle_upload_generic_error(
        self, worker_id: int, part_id: int, chunk_data: bytes | None, e: Exception
    ) -> tuple[int, bool]:
        if getattr(e, "seconds", None) is not None:
            await self._handle_upload_flood_wait(worker_id, part_id, chunk_data, e)
            return 0, False

        self.last_part_error = e
        err_str = str(e).lower()
        if any(pat in err_str for pat in ("file_parts_invalid", "file_part_size_changed", "file_part_too_big")):
            self.set_fatal_error(e)
            logger.error(f"❌ 上传 part_id={part_id} 收到确定性拒绝: {e}")
            return 0, False

        if isinstance(e, (asyncio.TimeoutError, TimeoutError)):
            self.timeout_count += 1
        self.chunk_retry_count += 1

        self.part_failed_counts[part_id] = self.part_failed_counts.get(part_id, 0) + 1
        if self.part_failed_counts[part_id] < 5:
            self.retry_queue.put_nowait((part_id, None if self.chunk_provider is not None else chunk_data, 0.0))
        else:
            logger.error(f"❌ 上传块 part_id={part_id} 失败超过 5 次: {e}")
            self.set_fatal_error(e)
        return 1, True

    async def _upload_part(
        self,
        worker_id: int,
        worker_client: TelegramClient,
        sender: Any,
        part_id: int,
        chunk_data: bytes | None,
        read_ms: float,
    ) -> None:
        await self._wait_for_flood_cooldown()
        if self.should_halt():
            if part_id not in self.completed_parts:
                self.retry_queue.put_nowait((part_id, chunk_data, 0.0))
            return
        if chunk_data is None:
            chunk_data = await self.provider_read(
                part_id * self.chunk_size, min(self.chunk_size, self.total_bytes - part_id * self.chunk_size)
            )
        if self.is_big:
            req = SaveBigFilePartRequest(
                file_id=self.file_id, file_part=part_id, file_total_parts=self.num_chunks, bytes=chunk_data
            )
        else:
            req = SaveFilePartRequest(file_id=self.file_id, file_part=part_id, bytes=chunk_data)

        t_rpc_start = time.perf_counter()
        await _execute_upload_part_rpc(worker_client, sender, req)
        rpc_ms = (time.perf_counter() - t_rpc_start) * 1000.0
        self._record_part_uploaded(part_id, len(chunk_data), read_ms, rpc_ms)

    def _is_done_or_halted(self) -> bool:
        return self.should_halt() or len(self.completed_parts) >= self.num_chunks

    def _try_dequeue_immediate(self) -> tuple[int, bytes | None, float] | None:
        with suppress(asyncio.QueueEmpty):
            return self.retry_queue.get_nowait()
        with suppress(asyncio.QueueEmpty):
            # AdaptivePrefetchQueue 未参数化泛型 (asyncio.Queue[Any]): cast 显式收敛
            return cast("tuple[int, bytes | None, float]", self.prefetch_queue.get_nowait())
        return None

    def _has_no_in_flight_work(self, in_flight_tasks: dict[int, asyncio.Task]) -> bool:
        return self.active_in_flight_count == 0 and not in_flight_tasks

    async def _await_exhausted_prefetch(
        self, in_flight_tasks: dict[int, asyncio.Task]
    ) -> tuple[int, bytes | None, float] | bool | None:
        if not self.retry_queue.empty():
            with suppress(asyncio.QueueEmpty):
                return self.retry_queue.get_nowait()
            return False
        if self._has_no_in_flight_work(in_flight_tasks):
            return None
        try:
            return await asyncio.wait_for(self.retry_queue.get(), timeout=0.2)
        except TimeoutError:
            should_stop = self._is_done_or_halted() or (
                self._has_no_in_flight_work(in_flight_tasks) and self.retry_queue.empty()
            )
            return None if should_stop else False

    async def _fetch_next_chunk(
        self, in_flight_tasks: dict[int, asyncio.Task]
    ) -> tuple[int, bytes | None, float] | bool | None:
        if self._is_done_or_halted():
            return None
        await self._wait_for_flood_cooldown()
        if self._is_done_or_halted():
            return None

        immediate = self._try_dequeue_immediate()
        if immediate is not None:
            return immediate

        assert self.prefetch_task is not None
        if self.prefetch_task.done() and self.prefetch_queue.empty():
            return await self._await_exhausted_prefetch(in_flight_tasks)

        try:
            return await asyncio.wait_for(self.prefetch_queue.get(), timeout=0.2)
        except TimeoutError:
            return None if self._is_done_or_halted() else False

    async def _cancel_and_drain_in_flight(self, in_flight_tasks: dict[int, asyncio.Task]) -> None:
        for t in list(in_flight_tasks.values()):
            if not t.done():
                t.cancel()
        if in_flight_tasks:
            with suppress(Exception):
                await asyncio.gather(*in_flight_tasks.values(), return_exceptions=True)
            in_flight_tasks.clear()

    async def _rebuild_worker_sender(
        self,
        worker_id: int,
        worker_client: TelegramClient,
        sender: Any,
        consecutive_errors: int,
        in_flight_tasks: dict[int, asyncio.Task],
    ) -> tuple[Any, bool]:
        logger.warning(f"⚠️ [FastTelethon Upload] Worker {worker_id} 连续失败 3 次，关闭并重建底层 Sender 连接")
        await self._cancel_and_drain_in_flight(in_flight_tasks)

        if sender:
            with suppress(Exception):
                await sender_pool._close_worker_sender(worker_client, sender, self.dc_id)
            self.worker_senders[worker_id] = None
            self.sender_reconnect_count += 1

        backoff = min(2.0, 0.2 * (2**consecutive_errors) + random.uniform(0.05, 0.15))
        await asyncio.sleep(backoff)

        try:
            new_sender = await self.init_sender(worker_id)
            self.worker_senders[worker_id] = new_sender
            return new_sender, True
        except Exception as rebuild_err:
            logger.warning(f"⚠️ [FastTelethon Upload] Worker {worker_id} 重建 Sender 失败: {rebuild_err}")
            await asyncio.sleep(1.0)
            return None, False

    async def _ensure_worker_sender(self, worker_id: int, current_sender: Any) -> Any:
        if self.worker_senders.get(worker_id):
            return current_sender
        try:
            sender = await self.init_sender(worker_id)
            self.worker_senders[worker_id] = sender
            return sender
        except Exception as init_err:
            logger.warning(f"⚠️ [FastTelethon Upload] Worker {worker_id} 动态初始化 Sender 降级: {init_err}")
            self.worker_senders[worker_id] = None
            return None

    async def _run_part_task(
        self,
        worker_id: int,
        worker_client: TelegramClient,
        sender: Any,
        part_id: int,
        chunk_data: bytes | None,
        read_ms: float,
        w_state: dict[str, Any],
        in_flight_tasks: dict[int, asyncio.Task],
        sem: _DynamicInFlightLimiter,
    ) -> None:
        try:
            await self._upload_part(worker_id, worker_client, sender, part_id, chunk_data, read_ms)
            w_state["consecutive_errors"] = 0
        except (FloodWaitError, FloodPremiumWaitError, FloodError) as e:
            await self._handle_upload_flood_wait(worker_id, part_id, chunk_data, e)
        except BadRequestError as e:
            self.set_fatal_error(e)
            logger.error(f"❌ 上传 part_id={part_id} 收到服务端确定性拒绝，中止整个上传: {e}")
        except ChunkProviderError as e:
            w_state["consecutive_errors"] += self._handle_upload_provider_error(part_id, e)
        except asyncio.CancelledError:
            if part_id not in self.completed_parts:
                self.retry_queue.put_nowait((part_id, chunk_data, 0.0))
            raise
        except Exception as e:
            err_inc, check_rebuild = await self._handle_upload_generic_error(worker_id, part_id, chunk_data, e)
            w_state["consecutive_errors"] += err_inc
            if check_rebuild and w_state["consecutive_errors"] >= 3:
                w_state["needs_rebuild"] = True
        finally:
            self.active_in_flight_count = max(0, self.active_in_flight_count - 1)
            in_flight_tasks.pop(id(asyncio.current_task()), None)
            sem.release()

    async def _acquire_slot_for_worker(
        self, worker_id: int, sem: _DynamicInFlightLimiter, w_state: dict[str, Any]
    ) -> str:
        await self._wait_for_flood_cooldown()
        try:
            await asyncio.wait_for(sem.acquire(), timeout=0.5)
        except TimeoutError:
            return "continue"

        if self.should_halt():
            sem.release()
            return "break"
        if worker_id >= self.get_target_concurrency() or w_state["needs_rebuild"]:
            sem.release()
            return "continue"

        await self._wait_for_flood_cooldown()
        if self.should_halt():
            sem.release()
            return "break"
        return "ok"

    def _sync_in_flight_semaphore(self, sem: _DynamicInFlightLimiter, in_flight_limit: int) -> int:
        target_in_flight = self.get_target_in_flight()
        if target_in_flight != in_flight_limit:
            return sem.set_limit(target_in_flight)
        return in_flight_limit

    async def run_worker(self, worker_id: int) -> None:
        worker_client = self.clients[worker_id % len(self.clients)]
        sender = self.worker_senders.get(worker_id)
        w_state: dict[str, Any] = {"consecutive_errors": 0, "needs_rebuild": False}

        in_flight_limit = self.get_target_in_flight()
        sem = _DynamicInFlightLimiter(in_flight_limit)
        in_flight_tasks: dict[int, asyncio.Task] = {}
        current_chunk: tuple[int, bytes | None, float] | None = None

        self.active_workers_count += 1
        try:
            while not self._is_done_or_halted() and worker_id < self.get_target_concurrency():
                sender = await self._ensure_worker_sender(worker_id, sender)
                if w_state["needs_rebuild"]:
                    sender, ok = await self._rebuild_worker_sender(
                        worker_id, worker_client, sender, w_state["consecutive_errors"], in_flight_tasks
                    )
                    if not ok:
                        continue
                    w_state["consecutive_errors"] = 0
                    w_state["needs_rebuild"] = False
                    in_flight_limit = self.get_target_in_flight()
                    sem = _DynamicInFlightLimiter(in_flight_limit)

                in_flight_limit = self._sync_in_flight_semaphore(sem, in_flight_limit)
                slot_status = await self._acquire_slot_for_worker(worker_id, sem, w_state)
                if slot_status == "continue":
                    continue
                if slot_status == "break":
                    break

                chunk_item = await self._fetch_next_chunk(in_flight_tasks)
                if not isinstance(chunk_item, tuple):
                    sem.release()
                    if chunk_item is None:
                        break
                    continue

                current_chunk = chunk_item
                part_id, chunk_data, read_ms = chunk_item
                self.active_in_flight_count += 1
                part_task = asyncio.create_task(
                    self._run_part_task(
                        worker_id, worker_client, sender, part_id, chunk_data, read_ms, w_state, in_flight_tasks, sem
                    )
                )
                in_flight_tasks[id(part_task)] = part_task
                current_chunk = None

            if in_flight_tasks and not self.should_halt():
                await asyncio.gather(*in_flight_tasks.values(), return_exceptions=True)
        finally:
            if current_chunk is not None and current_chunk[0] not in self.completed_parts:
                self.retry_queue.put_nowait((current_chunk[0], current_chunk[1], 0.0))
            await self._cancel_and_drain_in_flight(in_flight_tasks)
            self.active_workers_count = max(0, self.active_workers_count - 1)
            if sender:
                await sender_pool._close_worker_sender(worker_client, sender, self.dc_id)
            self.worker_senders.pop(worker_id, None)

    def _is_queues_and_prefetch_drained(self) -> bool:
        assert self.prefetch_task is not None
        return self.prefetch_task.done() and self.prefetch_queue.empty() and self.retry_queue.empty()

    def _reap_and_scale_workers(self, active_worker_tasks: dict[int, asyncio.Task], current_target: int) -> None:
        for wid, t in list(active_worker_tasks.items()):
            if wid >= current_target and not t.done():
                t.cancel()
            elif t.done():
                if not t.cancelled():
                    exc = t.exception()
                    if exc:
                        logger.debug(f"Upload Worker {wid} 退出异常: {exc}")
                if wid >= current_target:
                    active_worker_tasks.pop(wid, None)

        running_workers = [wid for wid, task in active_worker_tasks.items() if not task.done() and wid < current_target]
        if (
            len(running_workers) < current_target
            and not self._is_queues_and_prefetch_drained()
            and self.fatal_error is None
        ):
            for wid in range(current_target):
                existing_task = active_worker_tasks.get(wid)
                if existing_task is None or existing_task.done():
                    active_worker_tasks[wid] = asyncio.create_task(self.run_worker(wid))

    async def supervise_workers(self, initial_concurrency: int, metrics_out: dict[str, Any] | None) -> None:
        self.prefetch_task = asyncio.create_task(self.run_prefetch_producer())
        active_worker_tasks: dict[int, asyncio.Task] = {
            i: asyncio.create_task(self.run_worker(i)) for i in range(initial_concurrency)
        }
        try:
            while (
                self.fatal_error is None and not self.is_user_aborted() and len(self.completed_parts) < self.num_chunks
            ):
                current_target = self.get_target_concurrency()
                self.prefetch_queue.tune(current_target, self.get_target_in_flight())
                self._reap_and_scale_workers(active_worker_tasks, current_target)

                if len(self.completed_parts) == self.num_chunks:
                    break

                running_tasks = [t for t in active_worker_tasks.values() if not t.done()]
                if self._is_queues_and_prefetch_drained() and not running_tasks:
                    break
                if running_tasks:
                    await asyncio.wait(running_tasks, timeout=0.5, return_when=asyncio.FIRST_COMPLETED)
                else:
                    await asyncio.sleep(0.5)
        finally:
            await self._cancel_and_drain_in_flight(active_worker_tasks)
            await self.release_resources()
            _populate_upload_metrics_out(
                metrics_out, self.rpc_latencies, self.chunk_retry_count, self.sender_reconnect_count
            )

    def log_completion_diagnostics(self) -> None:
        total_io_ms = self.total_read_wait_ms + self.total_rpc_wait_ms
        read_pct = (self.total_read_wait_ms / total_io_ms * 100) if total_io_ms > 0 else 0.0
        rpc_pct = (self.total_rpc_wait_ms / total_io_ms * 100) if total_io_ms > 0 else 0.0
        metrics_summary = (
            f"耗时分析: 磁盘读 {self.total_read_wait_ms:.1f}ms ({read_pct:.1f}%), "
            f"网络RPC {self.total_rpc_wait_ms:.1f}ms ({rpc_pct:.1f}%), "
            f"重试 {self.chunk_retry_count}次 (超时 {self.timeout_count}次), "
            f"重建连接 {self.sender_reconnect_count}次"
        )
        logger.info(f"✅ 文件完成极速上传: {self.file_name}")
        logger.info(f"📊 [FastTelethon 诊断] {self.file_name} -> {metrics_summary}")

        total_duration = time.time() - self.tracker.start_time
        avg_speed_mb = (self.total_bytes / (1024 * 1024)) / max(total_duration, 0.001)
        if self.rpc_latencies:
            sorted_lat = sorted(self.rpc_latencies)
            p95_ms = sorted_lat[min(int(len(sorted_lat) * 0.95), len(sorted_lat) - 1)]
            p99_ms = sorted_lat[min(int(len(sorted_lat) * 0.99), len(sorted_lat) - 1)]
        else:
            p95_ms, p99_ms = 0.0, 0.0

        logger.info(
            f"📊 [FastTelethon 传输报告] 连接数: {self.get_target_concurrency()}, 单连接在途: {self.get_target_in_flight()}, "
            f"总分块: {self.num_chunks}, 平均速度: {avg_speed_mb:.1f}MB/s, "
            f"RPC P95延迟: {p95_ms:.0f}ms (P99: {p99_ms:.0f}ms), "
            f"重试次数: {self.chunk_retry_count}, 重连次数: {self.sender_reconnect_count}"
        )


def _finalize_upload_or_raise(session: _UploadSessionState) -> types.InputFile | types.InputFileBig:
    if session.fatal_error is not None:
        logger.warning("⚠️ 上传因服务端确定性拒绝中止，保留 .upload.meta 断点元数据。")
        raise session.fatal_error

    if session.is_cancelled and session.is_cancelled():
        logger.warning("⚠️ 上传任务被取消，保留 .upload.meta 断点元数据。")
        from app.core.scheduler import UploadCancelledError

        raise UploadCancelledError("上传已被用户取消")

    if session.is_paused and session.is_paused():
        logger.warning("⚠️ 上传任务被暂停，保留 .upload.meta 断点元数据。")
        from app.core.scheduler import UploadPausedError

        raise UploadPausedError("上传已被用户暂停让位")

    if len(session.completed_parts) == session.num_chunks:
        breakpoints._remove_upload_meta_sync(session.meta_file_path)
        session.log_completion_diagnostics()
        return _build_input_file_result(session.is_big, session.file_id, session.num_chunks, session.file_name)

    detail = f"，最后错误: {session.last_part_error}" if session.last_part_error else ""
    raise RuntimeError(f"文件上传未完成，完成度: {len(session.completed_parts)}/{session.num_chunks}{detail}")


def _raise_if_upload_signal(is_cancelled: Callable[[], bool] | None, is_paused: Callable[[], bool] | None) -> None:
    if is_cancelled and is_cancelled():
        from app.core.scheduler import UploadCancelledError

        raise UploadCancelledError("上传已被用户取消")
    if is_paused and is_paused():
        from app.core.scheduler import UploadPausedError

        raise UploadPausedError("上传已被用户暂停让位")


async def upload_file_parallel(
    client: TelegramClient | list[TelegramClient],
    file_path: str,
    progress_callback: Callable[[int, int, float, float, int, int, int], None] | None = None,
    concurrency: int | Callable[[], int] | None = None,
    is_cancelled: Callable[[], bool] | None = None,
    is_paused: Callable[[], bool] | None = None,
    force_fresh: bool = False,
    get_concurrency: Callable[[], int] | None = None,
    in_flight_per_connection: int | Callable[[], int] | None = None,
    get_in_flight_per_connection: Callable[[], int] | None = None,
    metrics_out: dict[str, Any] | None = None,
    chunk_provider: BaseChunkProvider | None = None,
    meta_path: str | None = None,
    prefetch_concurrency: int | Callable[[], int] | None = None,
    get_prefetch_concurrency: Callable[[], int] | None = None,
) -> types.InputFile | types.InputFileBig:
    """
    FastTelethon 多通道并发极速上传引擎：
    支持并发建立 N 条独立 TCP Sender 通道突破 Userbot 单连接上传限速，
    支持单连接多在途请求管线化 (Pipelining，支持 1-4 个在途请求填满 BDP)，
    支持传输过程中动态调整并发数与在途数 (实时扩缩通道与流水线深度，无需重启)，
    支持断点续传 (.upload.meta 持久化)，内置分块 TTL 时效过期自愈与 force_fresh 全量重传机制。
    """
    if chunk_provider is not None:
        total_bytes = int(chunk_provider.total_bytes)
        file_name = chunk_provider.file_name
        source_total_bytes = prefetch._inspect_source_total_bytes(chunk_provider)
    else:
        total_bytes, file_name = breakpoints._check_upload_file_sync(file_path)
        source_total_bytes = total_bytes

    target_concurrency_resolver = _CachedTargetResolver(
        getter=get_concurrency,
        raw_val=concurrency,
        setting_key="upload_parallel_connections",
        default_attr="UPLOAD_PARALLEL_CONNECTIONS",
        fallback_default=2,
        max_val=16,
        ignore_if_equal_default=True,
    )
    target_in_flight_resolver = _CachedTargetResolver(
        getter=get_in_flight_per_connection,
        raw_val=in_flight_per_connection,
        setting_key="upload_in_flight_per_connection",
        default_attr="UPLOAD_IN_FLIGHT_PER_CONNECTION",
        fallback_default=2,
        max_val=4,
    )
    max_file_size_bytes = settings.MAX_FILE_SIZE_MIB * 1024 * 1024
    is_huge_file = bool(total_bytes > max_file_size_bytes or source_total_bytes > max_file_size_bytes)
    target_prefetch_resolver = _CachedTargetResolver(
        getter=get_prefetch_concurrency,
        raw_val=prefetch_concurrency,
        setting_key="pikpak_prefetch_concurrency",
        default_attr="PIKPAK_PREFETCH_CONCURRENCY",
        fallback_default=2 if is_huge_file else 3,
        max_val=2 if is_huge_file else 16,
        force_one=False,
    )

    initial_concurrency = target_concurrency_resolver()
    clients = primitives._normalize_client_list(client)
    primary_client = clients[0]

    if total_bytes <= 512 * 1024:
        _raise_if_upload_signal(is_cancelled, is_paused)
        result = await _upload_small_file_direct(
            primary_client, file_path, file_name, total_bytes, chunk_provider, meta_path, progress_callback
        )
        _raise_if_upload_signal(is_cancelled, is_paused)
        return result

    is_big = total_bytes > 10 * 1024 * 1024
    chunk_size = 512 * 1024
    num_chunks = math.ceil(total_bytes / chunk_size)
    meta_file_path = breakpoints._resolve_upload_meta_file_path(file_path, total_bytes, chunk_provider, meta_path)
    source_fingerprint = (
        None
        if chunk_provider is not None
        else await asyncio.to_thread(breakpoints._compute_local_upload_fingerprint_sync, file_path, total_bytes)
    )

    default_file_id = random.getrandbits(63)  # nosec B311
    file_id, completed_parts = breakpoints._load_upload_breakpoint_sync(
        meta_file_path,
        file_name,
        total_bytes,
        default_file_id,
        force_fresh,
        source_fingerprint=source_fingerprint,
    )

    already_uploaded_bytes = sum(min(chunk_size, total_bytes - pid * chunk_size) for pid in completed_parts)
    pending_part_ids = [pid for pid in range(num_chunks) if pid not in completed_parts]

    if not pending_part_ids and len(completed_parts) == num_chunks:
        breakpoints._remove_upload_meta_sync(meta_file_path)
        return _build_input_file_result(is_big, file_id, num_chunks, file_name)

    file_descriptor = None if chunk_provider is not None else breakpoints._open_binary_file_sync(file_path, "rb")
    ring_buffer = prefetch._create_ring_buffer_if_enabled(chunk_provider)
    dc_id = getattr(primary_client.session, "dc_id", 4)

    session = _UploadSessionState(
        clients=clients,
        dc_id=dc_id,
        file_name=file_name,
        file_id=file_id,
        is_big=is_big,
        total_bytes=total_bytes,
        chunk_size=chunk_size,
        num_chunks=num_chunks,
        pending_part_ids=pending_part_ids,
        completed_parts=completed_parts,
        already_uploaded_bytes=already_uploaded_bytes,
        meta_file_path=meta_file_path,
        file_descriptor=file_descriptor,
        chunk_provider=chunk_provider,
        ring_buffer=ring_buffer,
        get_target_concurrency=target_concurrency_resolver,
        get_target_in_flight=target_in_flight_resolver,
        get_target_prefetch_concurrency=target_prefetch_resolver,
        progress_callback=progress_callback,
        is_cancelled=is_cancelled,
        is_paused=is_paused,
        source_fingerprint=source_fingerprint,
    )
    session.prefetch_queue.tune(initial_concurrency, target_in_flight_resolver())

    try:
        await session.prepare_senders(initial_concurrency)
    except BaseException:
        await session.release_resources()
        raise

    await session.supervise_workers(initial_concurrency, metrics_out)
    return _finalize_upload_or_raise(session)
