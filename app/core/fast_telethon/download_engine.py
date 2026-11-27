"""FastTelethon 多通道并行下载状态机: 异常定义、分块 RPC、会话调度器与主入口 download_file_parallel.

纯搬迁自原单文件 app/core/fast_telethon.py (拆分重构, 零运行时逻辑改动)。
跨子模块函数调用统一走模块对象引用 (sender_pool./breakpoints./primitives.),
保证单测对子模块属性 monkeypatch.setattr 时不会被早绑定引用击穿。
"""

import asyncio
import logging
import math
import os
import random
import tempfile
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

from telethon import TelegramClient
from telethon.tl.functions.upload import GetFileRequest

from app.config import settings

from . import breakpoints, primitives, sender_pool
from .breakpoints import _DownloadMetaSaver
from .primitives import SpeedTracker, _CachedTargetResolver

logger = logging.getLogger(__name__)


class DownloadIncompleteError(RuntimeError):
    """并行下载分块覆盖率不足 (部分块重试耗尽被丢弃)，文件不完整，禁止按成功返回。
    .part/.meta 断点已保留，resume 后可仅补齐缺失分块续传。"""

    def __init__(
        self,
        file_path: str,
        missing_offsets: list[int],
        last_errors: dict | None = None,
        total_chunks: int = 0,
        completed_chunks: int = 0,
        last_clients: dict[int, Any] | None = None,
        last_exceptions: dict[int, BaseException] | None = None,
    ):
        self.file_path = file_path
        self.missing_offsets = sorted(missing_offsets)
        self.last_errors = dict(last_errors or {})
        self.total_chunks = total_chunks
        self.completed_chunks = completed_chunks
        self.last_clients: dict[int, Any] = dict(last_clients or {})
        self.last_exceptions: dict[int, BaseException] = dict(last_exceptions or {})
        sample = ", ".join(
            f"offset={o} (原因: {self.last_errors.get(o, 'unknown')})" for o in self.missing_offsets[:20]
        )
        more = f" ...等共 {len(self.missing_offsets)} 个分块" if len(self.missing_offsets) > 20 else ""
        super().__init__(
            f"文件下载不完整: {os.path.basename(file_path)} 缺失 {len(self.missing_offsets)}/{total_chunks} 个分块 [{sample}{more}]，已保留断点可续传"
        )


def _extract_download_dc_and_location(location: Any, raw_media: Any, primary_client: TelegramClient) -> tuple[int, Any]:
    input_location = location
    extracted_dc_id = None
    if isinstance(location, tuple) and len(location) == 2:
        extracted_dc_id, input_location = location
    elif hasattr(location, "dc_id"):
        extracted_dc_id = location.dc_id

    dc_id = extracted_dc_id
    if not dc_id and raw_media:
        if hasattr(raw_media, "document") and raw_media.document:
            dc_id = raw_media.document.dc_id
        elif hasattr(raw_media, "photo") and raw_media.photo:
            dc_id = raw_media.photo.dc_id

    resolved_dc = int(dc_id or getattr(primary_client.session, "dc_id", 4) or 4)
    return resolved_dc, input_location


async def _download_small_file_direct(
    primary_client: TelegramClient,
    raw_media: Any,
    location: Any,
    out_file_path: str,
    dc_id: int,
    progress_callback: Callable[..., None] | None,
    is_cancelled: Callable[[], bool] | None,
    is_paused: Callable[[], bool] | None = None,
    *,
    expected_size: int = 0,
) -> str:
    staging_path: str | None = None

    def should_stop() -> bool:
        return bool((is_cancelled and is_cancelled()) or (is_paused and is_paused()))

    try:
        # 传入文件对象，避免 Telethon 为已存在的临时路径另选文件名。
        with tempfile.NamedTemporaryFile(
            dir=os.path.dirname(out_file_path) or ".", suffix=".part", delete=False
        ) as staging_file:
            staging_path = staging_file.name
            download = (
                primary_client.download_media(raw_media, file=staging_file)
                if raw_media is not None
                else primary_client.download_file(location, file=staging_file)
            )
            try:
                await asyncio.wait_for(download, timeout=30.0)
            except Exception:
                if should_stop():
                    return out_file_path
                raise
        if should_stop():
            return out_file_path
        actual_size = breakpoints._verify_small_download_sync(staging_path)
        if actual_size <= 0 or (expected_size > 0 and actual_size != expected_size):
            raise DownloadIncompleteError(
                out_file_path,
                [0],
                {0: f"小文件下载长度校验失败: 实际 {actual_size}, 预期 {expected_size or '未知'}"},
                total_chunks=1,
                completed_chunks=0,
                last_clients={0: primary_client},
            )
        os.replace(staging_path, out_file_path)
        staging_path = None
    finally:
        if staging_path is not None:
            try:
                os.remove(staging_path)
            except OSError as e:
                logger.warning("清理小文件下载临时文件失败: %s", e)
    if progress_callback:
        try:
            progress_callback(actual_size, actual_size, 0.0, 0.0, 1, dc_id)
        except Exception as e:
            logger.debug("单通道进度回调异常: %s", e)
    return out_file_path


async def _init_download_worker_sender(
    i: int,
    clients: list[TelegramClient],
    dc_id: int,
    dc_options: list[Any],
    auth_cache: dict[Any, Any],
    target_client: TelegramClient | None = None,
) -> Any:
    """薄委托 sender_pool._init_worker_sender (符号保留: download_engine 侧 monkeypatch 挂载点)。"""
    return await sender_pool._init_worker_sender(
        i, clients, dc_id, dc_options, auth_cache, target_client=target_client, log_tag="FastTelethon"
    )


def _download_media_identity(location: Any) -> str | None:
    """MTProto 文件引用会刷新，媒体 ID 与缩略图规格在同一文件内保持不变。"""
    media_id = getattr(location, "id", None)
    if type(media_id) is int:
        return f"{type(location).__name__}:{media_id}:{getattr(location, 'thumb_size', '')}"
    volume_id = getattr(location, "volume_id", None)
    local_id = getattr(location, "local_id", None)
    if type(volume_id) is int and type(local_id) is int:
        return f"{type(location).__name__}:{volume_id}:{local_id}"
    return None


def _resolve_worker_location(worker_client: TelegramClient, client_locations: dict | None, input_location: Any) -> Any:
    loc = (client_locations.get(worker_client) if client_locations else None) or input_location
    loc = loc[1] if isinstance(loc, tuple) else loc
    expected = _download_media_identity(input_location)
    return input_location if expected and _download_media_identity(loc) != expected else loc


async def _execute_download_chunk_rpc(
    worker_id: int,
    worker_client: TelegramClient,
    sender: Any,
    worker_location: Any,
    offset: int,
    chunk_size: int,
    dc_id: int,
) -> bytes:
    await asyncio.sleep(random.uniform(0.01, 0.05))
    request = GetFileRequest(location=worker_location, offset=offset, limit=chunk_size)
    if sender:
        call = getattr(worker_client, "_call", None)
        if call is not None:
            result = await asyncio.wait_for(call(sender, request), timeout=30.0)
        else:
            future = sender.send(request)
            target_fut = future[0] if isinstance(future, list) else future
            result = await asyncio.wait_for(target_fut, timeout=30.0)
        chunk_data = result.bytes
    elif hasattr(worker_client.session, "dc_id") and worker_client.session.dc_id == dc_id:
        result = await asyncio.wait_for(worker_client(request), timeout=15.0)
        chunk_data = result.bytes
    else:
        raise ConnectionError(f"Worker-{worker_id} 无法建立至 DC {dc_id} 的传输通道")

    if not chunk_data:
        raise RuntimeError(f"分块 offset={offset} 未获取到有效数据")
    # result.bytes 为 Any (telethon 动态属性): bytes() 显式收敛消除 no-any-return
    return bytes(chunk_data)


def _is_download_flood_error(e: Exception, err_msg: str) -> bool:
    return "floodwait" in err_msg or "flood" in err_msg or "slowmode" in err_msg or type(e).__name__ == "FloodWaitError"


def _inc_download_metric(metric_name: str) -> None:
    try:
        from app.core import metrics as metrics_mod

        getattr(metrics_mod, metric_name).inc()
    except Exception as m_err:
        logger.debug("更新 %s 指标异常 (非致命): %s", metric_name, m_err)


# 单账号下载触发 FloodWait 时的最大自动冷却等待阈值 (秒); 超过此值直接失败中止, 避免长时阻塞或死循环重拉
# TODO(issue): 后续暴露到 system_settings / app.config 支持运行时配置
SINGLE_CLIENT_DOWNLOAD_FLOOD_WAIT_MAX_SECONDS = 300


class _DownloadSessionState:
    """单次并行下载会话共享状态与 Worker 协调器"""

    def __init__(
        self,
        *,
        clients: list[TelegramClient],
        dc_id: int,
        input_location: Any,
        client_locations: dict | None,
        refetch_location: Callable[[Any], Awaitable[Any]] | None,
        total_bytes: int,
        chunk_size: int,
        num_chunks: int,
        chunks_queue: asyncio.Queue[int],
        completed_offsets: set[int],
        already_downloaded_bytes: int,
        file_descriptor: Any,
        meta_file_path: str,
        get_target_concurrency: Callable[[], int],
        progress_callback: Callable[..., None] | None,
        is_cancelled: Callable[[], bool] | None,
        is_paused: Callable[[], bool] | None,
    ) -> None:
        self.clients = clients
        self.dc_id = dc_id
        self.input_location = input_location
        self.client_locations = client_locations
        self.refetch_location = refetch_location
        self.total_bytes = total_bytes
        self.chunk_size = chunk_size
        self.num_chunks = num_chunks
        self.chunks_queue = chunks_queue
        self.completed_offsets = completed_offsets
        self.downloaded_bytes = already_downloaded_bytes
        self.tracker = SpeedTracker(total_bytes, initial_bytes=already_downloaded_bytes)
        self.file_descriptor = file_descriptor
        self.get_target_concurrency = get_target_concurrency
        self.progress_callback = progress_callback
        self.is_cancelled = is_cancelled
        self.is_paused = is_paused

        self.lock = asyncio.Lock()
        self.refetch_lock = asyncio.Lock()
        self.active_worker_lock = asyncio.Lock()
        self.failed_clients_lock = asyncio.Lock()

        self.meta_saver = _DownloadMetaSaver(
            meta_file_path,
            total_bytes,
            completed_offsets,
            self.lock,
            chunk_size=chunk_size,
            sync_part_fn=self._sync_part_fd,
            media_identity=_download_media_identity(input_location),
        )
        self.flood_wait_until: float = 0.0
        self.chunk_failed_counts: dict[int, int] = {}
        self.refetch_counts: dict[int, int] = {}
        self.chunk_last_errors: dict[int, str] = {}
        self.chunk_last_clients: dict[int, TelegramClient] = {}
        self.chunk_last_exceptions: dict[int, BaseException] = {}
        self.failed_clients: set[TelegramClient] = set()
        self.worker_senders: dict[int, Any] = {}
        self.auth_cache: dict[Any, Any] = {}
        self.dc_options: list[Any] = []
        self.active_workers_count = 0
        self.heartbeat_active = True
        self.fatal_error: BaseException | None = None
        self.worker_error_count = 0
        self.heartbeat_task: asyncio.Task[None] | None = None

    def _start_heartbeat(self) -> None:
        """按需启动心跳协程 (构造期零副作用: 建连前抛错不产生待取消的后台 Task)。"""
        if self.heartbeat_task is None:
            self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    def should_abort(self) -> bool:
        return bool((self.is_cancelled and self.is_cancelled()) or (self.is_paused and self.is_paused()))

    async def _heartbeat_loop(self) -> None:
        while self.heartbeat_active:
            await asyncio.sleep(0.8)
            if self.progress_callback:
                try:
                    self.progress_callback(
                        self.downloaded_bytes,
                        self.total_bytes,
                        self.tracker.speed,
                        self.tracker.eta,
                        self.active_workers_count,
                        self.dc_id,
                        self.get_target_concurrency(),
                    )
                except Exception as e:
                    logger.debug("下载心跳回调异常: %s", e)

    async def init_sender(self, i: int, target_client: TelegramClient | None = None) -> Any:
        return await _init_download_worker_sender(
            i, self.clients, self.dc_id, self.dc_options, self.auth_cache, target_client
        )

    async def prepare_senders(self, concurrency: int) -> None:
        self._start_heartbeat()
        self.auth_cache, self.dc_options = await sender_pool._prepare_download_auth_and_dc(self.clients, self.dc_id)
        init_senders_list = await sender_pool._gather_worker_senders_with_cleanup(
            [self.init_sender(i) for i in range(concurrency)], self.clients, self.dc_id
        )
        self.worker_senders.update(dict(enumerate(init_senders_list)))
        self.tracker.reset_start_time()

    async def release_resources(self) -> None:
        try:
            try:
                self.heartbeat_active = False
                if self.heartbeat_task is not None:
                    self.heartbeat_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await self.heartbeat_task
            except Exception as e:
                logger.debug("收停下载心跳协程异常: %s", e)
            for i, s in list(self.worker_senders.items()):
                if s:
                    with suppress(Exception):
                        await sender_pool._close_worker_sender(self.clients[i % len(self.clients)], s, self.dc_id)
            self.worker_senders.clear()
            try:
                await self.meta_saver.flush()
            except Exception as e:
                logger.debug("收尾保存下载元数据异常: %s", e)
        finally:
            try:
                if not self.file_descriptor.closed:
                    self.file_descriptor.close()
            except Exception as e:
                logger.debug("关闭下载文件句柄异常: %s", e)

    async def _switch_to_surviving_client(
        self, worker_id: int, worker_client: TelegramClient, sender: Any
    ) -> tuple[TelegramClient | None, Any, Any]:
        if sender:
            await sender_pool._close_worker_sender(worker_client, sender, self.dc_id)
        surviving = [c for c in self.clients if c not in self.failed_clients]
        if not surviving:
            return None, None, None
        new_client = surviving[worker_id % len(surviving)]
        new_loc = _resolve_worker_location(new_client, self.client_locations, self.input_location)
        return new_client, new_loc, None

    def _settle_chunk_error(self, on_settled: Callable[[], None] | None) -> None:
        self.chunks_queue.task_done()
        if on_settled is not None:
            on_settled()

    def _record_chunk_error(
        self,
        offset: int,
        worker_client: TelegramClient,
        exc: BaseException,
        message: str | None = None,
    ) -> None:
        if message is None:
            exc_text = str(exc)
            exc_name = type(exc).__name__
            if exc_name.lower() not in exc_text.lower() and exc_name not in (
                "Exception",
                "RuntimeError",
                "TimeoutError",
                "ConnectionError",
                "OSError",
                "ValueError",
            ):
                message = f"{exc_name}: {exc_text}"
            else:
                message = exc_text
        chunk_last_errors = getattr(self, "chunk_last_errors", None)
        if isinstance(chunk_last_errors, dict):
            chunk_last_errors[offset] = message
        chunk_last_clients = getattr(self, "chunk_last_clients", None)
        if isinstance(chunk_last_clients, dict):
            chunk_last_clients[offset] = worker_client
        chunk_last_exceptions = getattr(self, "chunk_last_exceptions", None)
        if isinstance(chunk_last_exceptions, dict):
            chunk_last_exceptions[offset] = exc

    async def _handle_flood_error(
        self,
        worker_id: int,
        worker_client: TelegramClient,
        sender: Any,
        offset: int,
        e: Exception,
        on_settled: Callable[[], None] | None = None,
    ) -> tuple[bool, TelegramClient, Any, Any]:
        wait_sec = getattr(e, "seconds", 60) or 60
        try:
            from app.core.client_manager import client_manager

            client_manager.mark_flood_wait(worker_client, wait_sec)
        except Exception as cm_err:
            logger.debug("上报 client_manager FloodWait 异常: %s", cm_err)

        if len(self.clients) > 1:
            async with self.failed_clients_lock:
                self.failed_clients.add(worker_client)
            logger.warning(f"⚠️ [FastTelethon Worker-{worker_id}] Client 触发 FloodWait ({wait_sec}s)，动态摘除该账号")

            await self.chunks_queue.put(offset)
            self._settle_chunk_error(on_settled)

            new_client, new_loc, new_sender = await self._switch_to_surviving_client(worker_id, worker_client, sender)
            if new_client is not None:
                logger.info(f"🔄 [FastTelethon Worker-{worker_id}] 已平滑切换至存活 Client 继续下载 (offset={offset})")
                await asyncio.sleep(0.2)
                return True, new_client, new_loc, new_sender

            self.chunk_failed_counts[offset] = self.chunk_failed_counts.get(offset, 0) + 1
            self._record_chunk_error(offset, worker_client, e, f"所有 Client 均触发限速: {e}")
            return False, worker_client, None, None

        if wait_sec > SINGLE_CLIENT_DOWNLOAD_FLOOD_WAIT_MAX_SECONDS:
            logger.error(
                f"❌ [FastTelethon Worker-{worker_id}] 单账号触发超长 FloodWait ({wait_sec}s > {SINGLE_CLIENT_DOWNLOAD_FLOOD_WAIT_MAX_SECONDS}s)，终止下载 (offset={offset})"
            )
            async with self.failed_clients_lock:
                self.failed_clients.add(worker_client)
            self.chunk_failed_counts[offset] = self.chunk_failed_counts.get(offset, 0) + 1
            self._record_chunk_error(
                offset,
                worker_client,
                e,
                f"单账号触发超长限速 FloodWait ({wait_sec}s > {SINGLE_CLIENT_DOWNLOAD_FLOOD_WAIT_MAX_SECONDS}s): {e}",
            )
            if sender:
                await sender_pool._close_worker_sender(worker_client, sender, self.dc_id)
            self._settle_chunk_error(on_settled)
            return False, worker_client, None, None

        self.flood_wait_until = max(self.flood_wait_until, time.time() + wait_sec)
        await self.chunks_queue.put(offset)
        self._settle_chunk_error(on_settled)
        if sender:
            await sender_pool._close_worker_sender(worker_client, sender, self.dc_id)

        logger.warning(
            f"⚠️ [FastTelethon Worker-{worker_id}] 单账号触发 FloodWait ({wait_sec}s)，等待冷却后重试 (offset={offset})"
        )
        while not self.should_abort():
            wait_rem = self.flood_wait_until - time.time()
            if wait_rem <= 0:
                break
            await asyncio.sleep(min(0.5, wait_rem))
        if not self.should_abort():
            new_loc = _resolve_worker_location(worker_client, self.client_locations, self.input_location)
            return True, worker_client, new_loc, None

        self.chunk_failed_counts[offset] = self.chunk_failed_counts.get(offset, 0) + 1
        self._record_chunk_error(offset, worker_client, e, f"所有 Client 均触发限速: {e}")
        return False, worker_client, None, None

    async def _refresh_worker_location(self, worker_client: TelegramClient) -> Any:
        assert self.refetch_location is not None
        async with self.refetch_lock:
            try:
                new_loc = await self.refetch_location(worker_client)
                if new_loc:
                    if isinstance(new_loc, tuple):
                        new_loc = new_loc[1]
                    identity_fields = ("id", "volume_id", "local_id", "thumb_size")
                    if type(new_loc) is not type(self.input_location) or any(
                        getattr(new_loc, field, None) != getattr(self.input_location, field, None)
                        for field in identity_fields
                    ):
                        self.fatal_error = RuntimeError("Refreshed download location identifies a different file")
                        return None
                    if self.client_locations is not None:
                        self.client_locations[worker_client] = new_loc
                    logger.info("✅ [FastTelethon] worker_location 已成功自动刷新！")
                    return new_loc
            except Exception as ref_err:
                logger.warning(f"⚠️ [FastTelethon] 刷新 FileReference 异常: {ref_err}")
        return None

    async def _handle_ref_expired_error(
        self,
        worker_id: int,
        worker_client: TelegramClient,
        worker_location: Any,
        sender: Any,
        offset: int,
        e: Exception,
        on_settled: Callable[[], None] | None = None,
    ) -> tuple[TelegramClient, Any, Any]:
        failing_client = worker_client
        self.refetch_counts[offset] = self.refetch_counts.get(offset, 0) + 1
        self._record_chunk_error(offset, failing_client, e)

        if self.refetch_counts[offset] >= 3:
            logger.error(
                f"❌ [FastTelethon Worker-{worker_id}] 分块 offset={offset} FileReference 刷新重试超限 (>=3): {e}"
            )
            self.chunk_failed_counts[offset] = self.chunk_failed_counts.get(offset, 0) + 1
            self._settle_chunk_error(on_settled)
            return worker_client, worker_location, sender

        logger.warning(
            f"⚠️ [FastTelethon Worker-{worker_id}] FileReference 过期 (第 {self.refetch_counts[offset]} 次)，自动刷新 Worker Client 的 location 并重试 (offset={offset})..."
        )
        new_loc = await self._refresh_worker_location(worker_client)
        if new_loc:
            await self.chunks_queue.put(offset)
            self._settle_chunk_error(on_settled)
            await asyncio.sleep(0.5 + random.uniform(0.05, 0.15))
            return worker_client, new_loc, sender

        logger.warning(f"⚠️ [FastTelethon Worker-{worker_id}] FileReference 刷新返回空，动态摘除该 Client")
        if len(self.clients) > 1:
            async with self.failed_clients_lock:
                self.failed_clients.add(worker_client)
            switched_client, switched_loc, switched_sender = await self._switch_to_surviving_client(
                worker_id, worker_client, sender
            )
            if switched_client is not None:
                worker_client, worker_location, sender = switched_client, switched_loc, switched_sender

        if len(self.clients) > 1 and self.refetch_counts[offset] < 3:
            await self.chunks_queue.put(offset)
        else:
            self.chunk_failed_counts[offset] = self.chunk_failed_counts.get(offset, 0) + 1
            self._record_chunk_error(offset, failing_client, e, "FileReference 刷新失败 (无权访问或凭证已失效)")
        self._settle_chunk_error(on_settled)
        return worker_client, worker_location, sender

    async def _handle_generic_chunk_error(
        self,
        worker_id: int,
        worker_client: TelegramClient,
        sender: Any,
        offset: int,
        e: Exception,
        err_msg: str,
        on_settled: Callable[[], None] | None = None,
    ) -> Any:
        is_expected_reset = (
            "0 bytes read on a total of 8 expected bytes" in err_msg
            or "server closed the connection" in err_msg
            or isinstance(e, (ConnectionError, asyncio.IncompleteReadError))
        )
        if is_expected_reset:
            logger.debug(f"[FastTelethon Worker-{worker_id}] 服务端长连接空闲重置 (offset={offset}): {e}")

        if sender:
            await sender_pool._close_worker_sender(worker_client, sender, self.dc_id)

        self.chunk_failed_counts[offset] = self.chunk_failed_counts.get(offset, 0) + 1
        self._record_chunk_error(offset, worker_client, e)
        if self.chunk_failed_counts[offset] < 5:
            _inc_download_metric("chunk_retry_total")
            await self.chunks_queue.put(offset)
        else:
            _inc_download_metric("chunk_dropped_total")
            logger.error(f"❌ 块 offset={offset} 5次重试失败: {e}")
        self._settle_chunk_error(on_settled)
        await asyncio.sleep(0.3 + random.uniform(0.05, 0.15))
        return None

    def _write_at(self, offset: int, data: bytes) -> None:
        self.file_descriptor.seek(offset)
        self.file_descriptor.write(data)

    def _sync_part_fd(self) -> None:
        fd = self.file_descriptor
        if fd is None or getattr(fd, "closed", False):
            return
        flush_fn = getattr(fd, "flush", None)
        if callable(flush_fn):
            flush_fn()
        fileno_fn = getattr(fd, "fileno", None)
        if callable(fileno_fn):
            fno = fileno_fn()
            if isinstance(fno, int) and not isinstance(fno, bool):
                os.fsync(fno)

    async def _write_downloaded_chunk(self, offset: int, chunk_data: bytes) -> None:
        expected_len = min(self.chunk_size, self.total_bytes - offset)
        valid_bytes = chunk_data[:expected_len]
        async with self.lock:
            write_task = asyncio.create_task(asyncio.to_thread(self._write_at, offset, valid_bytes))
            try:
                await asyncio.shield(write_task)
            except BaseException:
                while not write_task.done():
                    with suppress(asyncio.CancelledError):
                        await asyncio.shield(write_task)
                raise
            self.downloaded_bytes += len(valid_bytes)
            self.completed_offsets.add(offset)
            self.chunk_last_errors.pop(offset, None)
            self.chunk_last_clients.pop(offset, None)
            self.chunk_last_exceptions.pop(offset, None)
            self.tracker.update(self.downloaded_bytes)
            should_save = len(self.completed_offsets) % 20 == 0
        if should_save:
            self.meta_saver.trigger()

    async def _dispatch_chunk_error(
        self,
        worker_id: int,
        worker_client: TelegramClient,
        worker_location: Any,
        sender: Any,
        offset: int,
        e: Exception,
        on_settled: Callable[[], None] | None = None,
    ) -> tuple[bool, TelegramClient, Any, Any]:
        err_msg = str(e).lower()
        if _is_download_flood_error(e, err_msg):
            return await self._handle_flood_error(worker_id, worker_client, sender, offset, e, on_settled)

        is_ref_expired = "file reference" in err_msg or "fileref" in err_msg or "expired" in err_msg
        if is_ref_expired and self.refetch_location:
            worker_client, worker_location, sender = await self._handle_ref_expired_error(
                worker_id, worker_client, worker_location, sender, offset, e, on_settled
            )
            return True, worker_client, worker_location, sender

        sender = await self._handle_generic_chunk_error(
            worker_id, worker_client, sender, offset, e, err_msg, on_settled
        )
        return True, worker_client, worker_location, sender

    async def _fetch_chunk_with_cancellation_guard(
        self, worker_id: int, worker_client: TelegramClient, sender: Any, worker_location: Any, offset: int
    ) -> bytes:
        try:
            return await _execute_download_chunk_rpc(
                worker_id, worker_client, sender, worker_location, offset, self.chunk_size, self.dc_id
            )
        except asyncio.CancelledError:
            if offset not in self.completed_offsets:
                self.chunks_queue.put_nowait(offset)
                self.chunks_queue.task_done()
            raise

    async def _wait_flood_cooldown_if_needed(self) -> None:
        while not self.should_abort():
            wait_rem = self.flood_wait_until - time.time()
            if wait_rem <= 0:
                break
            await asyncio.sleep(min(0.5, wait_rem))

    async def _run_download_chunk_attempt(
        self,
        worker_id: int,
        worker_client: TelegramClient,
        worker_location: Any,
        sender: Any,
        offset: int,
    ) -> tuple[bool, TelegramClient, Any, Any]:
        """执行单个下载分块的初始化、拉取与落盘，确保未完成提交的 offset 在任意取消路径均能回队。"""
        offset_settled = False
        initialized_sender = False

        def mark_settled() -> None:
            nonlocal offset_settled
            offset_settled = True

        try:
            if not sender:
                sender = await self.init_sender(worker_id, worker_client)
                initialized_sender = True

            try:
                chunk_data = await self._fetch_chunk_with_cancellation_guard(
                    worker_id, worker_client, sender, worker_location, offset
                )
                expected_len = min(self.chunk_size, self.total_bytes - offset)
                if len(chunk_data) < expected_len:
                    raise RuntimeError(f"Short download chunk at {offset}: {len(chunk_data)} < {expected_len}")
            except asyncio.CancelledError:
                # Prepared senders remain owned by run_worker; a sender created
                # in this attempt is released by the outer exception handler.
                offset_settled = True
                raise
            except Exception as e:
                keep_going, worker_client, worker_location, sender = await self._dispatch_chunk_error(
                    worker_id, worker_client, worker_location, sender, offset, e, mark_settled
                )
                offset_settled = True
                return keep_going, worker_client, worker_location, sender

            if self.should_abort():
                self.chunks_queue.task_done()
                offset_settled = True
                return False, worker_client, worker_location, sender

            try:
                await self._write_downloaded_chunk(offset, chunk_data)
            except asyncio.CancelledError:
                raise
            except Exception as write_err:
                self._record_chunk_error(offset, worker_client, write_err, f"本地分块写入失败: {write_err}")
                self.fatal_error = write_err
                raise
            self.chunks_queue.task_done()
            offset_settled = True
            return True, worker_client, worker_location, sender
        except BaseException:
            if not offset_settled:
                if offset not in self.completed_offsets:
                    self.chunks_queue.put_nowait(offset)
                self.chunks_queue.task_done()
            # The caller only receives a newly initialized sender on a normal return.
            if initialized_sender and sender:
                await sender_pool._close_worker_sender(worker_client, sender, self.dc_id)
            raise

    async def run_worker(self, worker_id: int) -> None:
        worker_client = self.clients[worker_id % len(self.clients)]
        worker_location = _resolve_worker_location(worker_client, self.client_locations, self.input_location)
        sender = self.worker_senders.get(worker_id)

        async with self.active_worker_lock:
            self.active_workers_count += 1

        try:
            while not self.chunks_queue.empty():
                await self._wait_flood_cooldown_if_needed()

                if self.should_abort() or worker_id >= self.get_target_concurrency():
                    break

                if worker_client in self.failed_clients:
                    switched_client, switched_loc, sender = await self._switch_to_surviving_client(
                        worker_id, worker_client, sender
                    )
                    if switched_client is None:
                        break
                    worker_client, worker_location = switched_client, switched_loc

                try:
                    offset = self.chunks_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                keep_going, worker_client, worker_location, sender = await self._run_download_chunk_attempt(
                    worker_id, worker_client, worker_location, sender, offset
                )
                if not keep_going:
                    break
        finally:
            async with self.active_worker_lock:
                self.active_workers_count = max(0, self.active_workers_count - 1)
            if sender:
                await sender_pool._close_worker_sender(worker_client, sender, self.dc_id)
                self.worker_senders[worker_id] = None

    def _reap_and_scale_workers(self, active_worker_tasks: dict[int, asyncio.Task], current_target: int) -> None:
        for wid, t in list(active_worker_tasks.items()):
            if wid >= current_target and not t.done():
                t.cancel()
            elif t.done():
                if not t.cancelled():
                    exc = t.exception()
                    if exc:
                        self.worker_error_count += 1
                        logger.warning(f"⚠️ Download Worker {wid} 退出异常 ({self.worker_error_count}): {exc}")
                        if self.fatal_error is None and self.worker_error_count >= 5:
                            self.fatal_error = exc
                active_worker_tasks.pop(wid, None)

        if self.fatal_error is not None:
            return

        running_workers = [wid for wid, task in active_worker_tasks.items() if not task.done() and wid < current_target]
        if len(running_workers) < current_target and not self.chunks_queue.empty():
            for wid in range(current_target):
                existing_task = active_worker_tasks.get(wid)
                if existing_task is None or existing_task.done():
                    active_worker_tasks[wid] = asyncio.create_task(self.run_worker(wid))

    async def supervise_workers(self, initial_concurrency: int) -> None:
        active_worker_tasks: dict[int, asyncio.Task] = {
            i: asyncio.create_task(self.run_worker(i)) for i in range(initial_concurrency)
        }
        try:
            while not self.should_abort() and len(self.completed_offsets) < self.num_chunks:
                all_clients_failed = bool(self.clients) and len(self.failed_clients) >= len(self.clients)
                if all_clients_failed or self.fatal_error is not None:
                    break

                self._reap_and_scale_workers(active_worker_tasks, self.get_target_concurrency())
                if self.fatal_error is not None or len(self.completed_offsets) == self.num_chunks:
                    break

                running_tasks = [t for t in active_worker_tasks.values() if not t.done()]
                if (self.chunks_queue.empty() or all_clients_failed) and not running_tasks:
                    break
                if running_tasks:
                    await asyncio.wait(running_tasks, timeout=0.5, return_when=asyncio.FIRST_COMPLETED)
                else:
                    await asyncio.sleep(0.5)
        finally:
            for t in active_worker_tasks.values():
                if not t.done():
                    t.cancel()
            try:
                if active_worker_tasks:
                    with suppress(Exception):
                        await asyncio.gather(*active_worker_tasks.values(), return_exceptions=True)
            finally:
                await self.release_resources()


def _set_download_baseline(callback: Callable[[int], None] | None, resumed_bytes: int) -> None:
    if callback is not None:
        try:
            callback(resumed_bytes)
        except Exception as exc:
            logger.debug("设置下载断点计量基线失败: %s", exc)


async def download_file_parallel(
    client: TelegramClient | list[TelegramClient],
    location: Any,
    out_file_path: str,
    total_bytes: int,
    progress_callback: Callable[..., None] | None = None,
    concurrency: int | Callable[[], int] = settings.DOWNLOAD_PARALLEL_CONNECTIONS,
    is_cancelled: Callable[[], bool] | None = None,
    is_paused: Callable[[], bool] | None = None,
    raw_media: Any | None = None,
    client_locations: dict | None = None,
    refetch_location: Callable[[Any], Awaitable[Any]] | None = None,
    get_concurrency: Callable[[], int] | None = None,
    set_initial_bytes: Callable[[int], None] | None = None,
) -> str:
    """
    FastTelethon 多通道并发极速下载引擎：
    支持多 Client 实例 (Userbot + 4 Bots) 各自导出专属 Location，
    并发建立 N 条独立 TCP Sender 通道突破单连接 2MB/s 带制限速，
    支持传输过程中动态调整并发数 (实时扩缩通道，无需重启)，
    内置 FileReferenceExpiredError 自动感知与线程安全静默刷新机制。
    """
    breakpoints._ensure_parent_dir_sync(out_file_path)

    target_concurrency_resolver = _CachedTargetResolver(
        getter=get_concurrency,
        raw_val=concurrency,
        setting_key="download_parallel_connections",
        default_attr="DOWNLOAD_PARALLEL_CONNECTIONS",
        fallback_default=8,
        max_val=32,
        ignore_if_equal_default=True,
    )
    initial_concurrency = target_concurrency_resolver()
    clients = primitives._normalize_client_list(client)
    primary_client = clients[0]
    dc_id, input_location = _extract_download_dc_and_location(location, raw_media, primary_client)

    # 对于 <= 512KB 的极小文件，直接走单流下载
    if total_bytes <= 512 * 1024 or location is None:
        return await _download_small_file_direct(
            primary_client,
            raw_media,
            location,
            out_file_path,
            dc_id,
            progress_callback,
            is_cancelled,
            is_paused,
            expected_size=total_bytes,
        )

    # 多通道并发分块极速下载 (支持断点续传)
    part_file_path = breakpoints.resolve_breakpoint_path(out_file_path, ".part", trust_root=settings.DOWNLOAD_DIR)
    meta_file_path = breakpoints.resolve_breakpoint_path(out_file_path, ".meta", trust_root=settings.DOWNLOAD_DIR)

    chunk_size = settings.CHUNK_SIZE  # 512 KB
    completed_offsets = await asyncio.to_thread(
        breakpoints._prepare_download_breakpoint_sync,
        part_file_path,
        meta_file_path,
        total_bytes,
        chunk_size,
        media_identity=_download_media_identity(input_location),
    )

    chunks_queue: asyncio.Queue[int] = asyncio.Queue()
    num_chunks = math.ceil(total_bytes / chunk_size)
    already_downloaded_bytes = 0

    for i in range(num_chunks):
        offset = i * chunk_size
        if offset in completed_offsets:
            already_downloaded_bytes += min(chunk_size, total_bytes - offset)
        else:
            chunks_queue.put_nowait(offset)

    _set_download_baseline(set_initial_bytes, already_downloaded_bytes)

    if chunks_queue.empty():
        await asyncio.to_thread(
            breakpoints._finalize_download_files_sync, out_file_path, part_file_path, meta_file_path
        )
        if progress_callback:
            try:
                progress_callback(total_bytes, total_bytes, 0.0, 0.0, initial_concurrency, dc_id)
            except Exception as e:
                logger.debug("完成进度回调异常: %s", e)
        return out_file_path

    file_descriptor = breakpoints._open_binary_file_sync(part_file_path, "rb+")
    session = _DownloadSessionState(
        clients=clients,
        dc_id=dc_id,
        input_location=input_location,
        client_locations=client_locations,
        refetch_location=refetch_location,
        total_bytes=total_bytes,
        chunk_size=chunk_size,
        num_chunks=num_chunks,
        chunks_queue=chunks_queue,
        completed_offsets=completed_offsets,
        already_downloaded_bytes=already_downloaded_bytes,
        file_descriptor=file_descriptor,
        meta_file_path=meta_file_path,
        get_target_concurrency=target_concurrency_resolver,
        progress_callback=progress_callback,
        is_cancelled=is_cancelled,
        is_paused=is_paused,
    )

    try:
        await session.prepare_senders(initial_concurrency)
    except BaseException:
        await session.release_resources()
        raise

    await session.supervise_workers(initial_concurrency)

    if session.should_abort():
        logger.warning("⚠️ 下载任务已暂停/取消，保留 .part 断点文件。")
        return part_file_path

    if session.fatal_error is not None:
        raise session.fatal_error

    if len(completed_offsets) == num_chunks:
        await asyncio.to_thread(
            breakpoints._finalize_download_files_sync, out_file_path, part_file_path, meta_file_path
        )
        logger.info(f"✅ 文件完成极速下载拼接: {out_file_path}")
        return out_file_path

    missing_offsets = [i * chunk_size for i in range(num_chunks) if (i * chunk_size) not in completed_offsets]
    raise DownloadIncompleteError(
        out_file_path,
        missing_offsets,
        session.chunk_last_errors,
        total_chunks=num_chunks,
        completed_chunks=len(completed_offsets),
        last_clients=session.chunk_last_clients,
        last_exceptions=session.chunk_last_exceptions,
    )
