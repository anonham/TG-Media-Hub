"""Core Runtime: Fast Telethon 多 DC 传输引擎、流水线管线化、跨洋传输与并发自愈测试 (Fast Telethon Core)

覆盖范围:
- 原 test_fast_telethon_cancellation: fast_telethon 任务级取消传播、资源清理与 AuthKey 发送器回收
- 原 test_upload_cross_ocean_engine: 跨洋高并发上传、内存有界预取、FloodWait 协作回退与断线重连
- 原 test_upload_pipelining: 单连接 Pipelining 管线化并发、延迟分位数采样与配置级联
- 原 test_transfer_concurrency_fixes: 进度回调签名安全自省、动态缩容取消与并发 TTL 缓存
- 原 test_p2_features: SuppressExpectedCloseFilter 0 bytes read 底噪过滤"""

import asyncio
import io
import json
import logging
import os
import shutil
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from _async_wait import wait_until
from _safeio import safe_write_bytes
from telethon import TelegramClient
from telethon.errors import AuthKeyUnregisteredError
from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest
from telethon.tl.types import InputDocumentFileLocation

from app.config import settings
from app.core import fast_telethon
from app.core.fast_telethon import (
    AdaptivePrefetchQueue,
    SpeedTracker,
    SuppressExpectedCloseFilter,
    download_file_parallel,
    upload_file_parallel,
)
from app.core.fast_telethon import download_engine as de
from app.core.fast_telethon import sender_pool as sp
from app.core.fast_telethon import upload_engine as ue
from app.core.fast_telethon.breakpoints import _sync_read_chunk
from app.core.fast_telethon.sender_pool import _probe_dc_endpoints
from app.core.pipeline.utils import safe_invoke_progress
from app.core.scheduler import UploadCancelledError, UploadPausedError
from app.core.session_errors import is_session_invalid_error
from app.db.account_repository import SystemSettingsRepository
from app.services.downloader_service import DownloaderService
from app.services.publisher_service import PublisherService

# ==============================================================================
# Part: .tmp_fast_canc.py
# ==============================================================================
CHUNK = 512 * 1024


def _cancel_raw_media(dc_id: int = 2) -> MagicMock:
    media = MagicMock()
    media.document = MagicMock(dc_id=dc_id)
    media.photo = None
    return media


def _ok_client() -> AsyncMock:
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)

    async def ok_call(sender, request):
        res = MagicMock()
        res.bytes = b"a" * CHUNK
        return res

    client._call = AsyncMock(side_effect=ok_call)
    return client


def _blocking_client(block_after: int = 1, blocked_event: asyncio.Event | None = None) -> AsyncMock:
    """前 block_after 个分块正常返回数据, 之后永久挂起 (由 task.cancel() 打断)"""
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    calls = {"n": 0}

    async def gated_call(sender, request):
        if calls["n"] < block_after:
            calls["n"] += 1
            res = MagicMock()
            res.bytes = b"a" * CHUNK
            return res
        if blocked_event is not None:
            blocked_event.set()
        await asyncio.Event().wait()

    client._call = AsyncMock(side_effect=gated_call)
    return client


def _collect_orphans() -> list:
    return [
        t
        for t in asyncio.all_tasks()
        if t is not asyncio.current_task()
        and not t.done()
        and t.get_coro() is not None
        and t.get_coro().cr_code.co_name in ("heartbeat_loop", "worker")
    ]


async def _wait_orphans_drained(timeout: float = 2.0) -> list:
    """给被取消的 worker/心跳协程若干循环周期完成自身清理, 返回仍存活的孤儿"""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    orphans = _collect_orphans()
    while orphans and loop.time() < deadline:
        await asyncio.sleep(0.05)
        orphans = _collect_orphans()
    return orphans


@pytest.fixture
def _cancel_fake_senders(monkeypatch):
    async def fake_create(*args, **kwargs):
        return object()

    async def fake_get(*args, **kwargs):
        return object()

    monkeypatch.setattr(fast_telethon.sender_pool, "_create_worker_sender_with_auth", fake_create)
    monkeypatch.setattr(fast_telethon.sender_pool, "_get_worker_sender", fake_get)


@pytest.mark.asyncio
@pytest.mark.usefixtures("_cancel_fake_senders")
async def test_download_cancel_propagates_and_cleans_up(tmp_dir, monkeypatch):
    """下载中途任务级取消: CancelledError 上抛 + 无孤儿协程 + fd 已关 (严格 rmtree 通过)"""
    dl_dir = os.path.join(tmp_dir, "dl")
    os.makedirs(dl_dir, exist_ok=True)
    # out_file 位于 tmp_dir 子目录 (避开同目录下测试 DB 的连接句柄), trust_root 沙箱与落盘目录一致
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", dl_dir)
    out = os.path.join(dl_dir, "cancel.bin")
    total = 3 * CHUNK
    blocked_event = asyncio.Event()
    client = _blocking_client(block_after=1, blocked_event=blocked_event)

    dl_task = asyncio.create_task(
        download_file_parallel(
            client,
            location=MagicMock(),
            out_file_path=out,
            total_bytes=total,
            concurrency=2,
            raw_media=_cancel_raw_media(),
        )
    )
    await asyncio.wait_for(blocked_event.wait(), timeout=2.0)
    await asyncio.sleep(0)
    assert not dl_task.done(), "分块应处于阻塞态等待取消"
    dl_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dl_task

    orphans = await _wait_orphans_drained()
    assert orphans == [], f"存在孤儿协程: {[t.get_coro().cr_code.co_name for t in orphans]}"

    # 严格 rmtree (不带 ignore_errors): fd 泄漏时 Windows 上必抛 PermissionError
    shutil.rmtree(dl_dir)


@pytest.mark.asyncio
@pytest.mark.usefixtures("_cancel_fake_senders")
async def test_download_cancel_keeps_meta_and_resumes(tmp_dir, monkeypatch):
    """中途取消后 .meta 必须存在 (finally save_meta), 重新调用同一路径断点续传至完成"""
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", tmp_dir)
    out = os.path.join(tmp_dir, "resume_cancel.bin")
    total = 2 * CHUNK
    blocked_event = asyncio.Event()
    client = _blocking_client(block_after=1, blocked_event=blocked_event)

    dl_task = asyncio.create_task(
        download_file_parallel(
            client,
            location=MagicMock(),
            out_file_path=out,
            total_bytes=total,
            concurrency=1,
            raw_media=_cancel_raw_media(),
        )
    )
    await asyncio.wait_for(blocked_event.wait(), timeout=2.0)
    await asyncio.sleep(0)
    dl_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dl_task

    # 修复前 save_meta 不在 finally, 未到 5 块边界被取消时 .meta 缺失/过期, 无法续传
    assert os.path.exists(out + ".meta")
    assert os.path.exists(out + ".part")

    res = await download_file_parallel(
        _ok_client(),
        location=MagicMock(),
        out_file_path=out,
        total_bytes=total,
        concurrency=2,
        raw_media=_cancel_raw_media(),
    )
    assert res == out
    assert os.path.getsize(out) == total
    assert not os.path.exists(out + ".part")
    assert not os.path.exists(out + ".meta")


@pytest.mark.asyncio
async def test_upload_cancel_propagates_and_keeps_meta(tmp_dir, monkeypatch):
    """上传中途任务级取消: CancelledError 上抛 + fd 已关 (严格 rmtree 通过) + .upload.meta 保留"""
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", os.path.join(tmp_dir, "meta_sandbox"))
    up_dir = os.path.join(tmp_dir, "up")
    os.makedirs(up_dir, exist_ok=True)
    test_file = safe_write_bytes(os.path.join(up_dir, "up_video.mp4"), b"U" * (1024 * 1024 * 2))  # 2MB -> 4 分块

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    calls = {"n": 0}
    blocked_event = asyncio.Event()

    async def gated_call(request):
        # sender 预建在 mock 下必然降级为 None, worker 实际走 client(req) 兜底发送路径,
        # 故门控挂在实例调用上: 首个分块成功, 之后永久挂起等待 task.cancel() 打断
        if isinstance(request, (SaveBigFilePartRequest, SaveFilePartRequest)):
            calls["n"] += 1
            if calls["n"] <= 1:
                return True
            blocked_event.set()
            await asyncio.Event().wait()
        cfg = MagicMock()
        cfg.dc_options = []
        return cfg  # GetConfigRequest 等其余调用

    client.side_effect = gated_call

    up_task = asyncio.create_task(upload_file_parallel(client, str(test_file), concurrency=1))
    await asyncio.wait_for(blocked_event.wait(), timeout=2.0)
    await asyncio.sleep(0)
    assert not up_task.done(), "上传应处于阻塞态等待取消"
    up_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await up_task

    orphans = await _wait_orphans_drained()
    assert orphans == [], f"存在孤儿协程: {[t.get_coro().cr_code.co_name for t in orphans]}"

    # .upload.meta 必须在取消路径落盘 (断点续传依据; 收拢至沙箱)
    from app.core.fast_telethon import local_upload_meta_path

    assert os.path.exists(local_upload_meta_path(str(test_file), 2 * 1024 * 1024))

    # 严格 rmtree (不带 ignore_errors): fd 泄漏时 Windows 上必抛 PermissionError
    shutil.rmtree(up_dir)


@pytest.mark.asyncio
async def test_init_gather_cancel_closes_created_senders(tmp_dir, monkeypatch):
    """init 阶段 gather 被取消: 已建立的 sender 必须逐个关闭 (修复前直接泄漏)"""
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", tmp_dir)

    disconnect = AsyncMock()
    created = []
    blocked_event = asyncio.Event()

    async def fake_create_with_auth(client, dc_id, auth_id, auth_bytes, dc_option=None):
        s = MagicMock()
        s.disconnect = disconnect
        created.append(s)
        if len(created) == 1:
            return s
        # 第 2 条通道建立时永久挂起, 等待 cancel 打断 init gather
        blocked_event.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(fast_telethon.sender_pool, "_create_worker_sender_with_auth", fake_create_with_auth)
    monkeypatch.setattr(fast_telethon.sender_pool, "_get_worker_sender", AsyncMock(return_value=None))

    # session dc_id 与媒体 dc_id 不同 -> 强制走 auth 导出分支 (即被 patch 的建链函数)
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=9)

    dl_task = asyncio.create_task(
        download_file_parallel(
            client,
            location=MagicMock(),
            out_file_path=os.path.join(tmp_dir, "init_cancel.bin"),
            total_bytes=3 * CHUNK,
            concurrency=2,
            raw_media=_cancel_raw_media(),
        )
    )
    await asyncio.wait_for(blocked_event.wait(), timeout=2.0)
    await asyncio.sleep(0)
    assert not dl_task.done(), "init gather 应处于阻塞态 (第 2 条通道等待取消打断)"
    dl_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dl_task

    assert disconnect.await_count >= 1, "已建立的 sender 必须被显式关闭, 不允许泄漏"


# ==============================================================================
# Part: test_upload_cross_ocean_engine.py
# ==============================================================================
CHUNK = 512 * 1024


@pytest.mark.asyncio
async def test_upload_prefetch_bounded_memory(tmp_path, monkeypatch):
    """1. 预读队列背压与内存上限测试 (test_upload_prefetch_bounded_memory):
    构造 100 块文件，模拟网络 ACK 延迟，验证 prefetch_queue.qsize() <= 32，驻留内存受控。
    """
    num_chunks = 100
    file_bytes = b"M" * (CHUNK * num_chunks)
    test_file = safe_write_bytes(str(tmp_path / "bounded_prefetch.bin"), file_bytes)

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    max_observed_qsize = 0
    orig_put = AdaptivePrefetchQueue.put

    async def tracking_put(self, item):
        nonlocal max_observed_qsize
        max_observed_qsize = max(max_observed_qsize, self.qsize())
        return await orig_put(self, item)

    monkeypatch.setattr(AdaptivePrefetchQueue, "put", tracking_put)

    received_parts = []

    async def mock_call(sender, req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            await asyncio.sleep(0.005)
            received_parts.append(req.file_part)
            return True
        return cfg

    client._call = mock_call

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        sender = MagicMock()
        sender.disconnect = AsyncMock()
        sender._connection = None
        return [sender for _ in coros]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    res = await upload_file_parallel(
        client,
        str(test_file),
        concurrency=2,
        in_flight_per_connection=1,
    )

    assert res.parts == 100
    assert len(received_parts) == 100
    # 验证预读队列背压有效生效，采样到的队列深度不超过容量上限 32
    assert max_observed_qsize <= 32, f"预读队列未生效背压流控，最大观测队列深度: {max_observed_qsize}"


@pytest.mark.asyncio
async def test_upload_cancellation_safe_fd_closure(tmp_path, monkeypatch):
    """2. 退出安全与文件句柄无竞态关闭测试 (test_upload_cancellation_safe_fd_closure):
    预读中途触发取消，断言 prefetch_task 先行退出，fd.close() 绝不引发 ValueError: I/O operation on closed file。
    """
    num_chunks = 20
    file_bytes = b"C" * (CHUNK * num_chunks)
    test_file = safe_write_bytes(str(tmp_path / "cancellation_fd.bin"), file_bytes)

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    cancelled = False
    closed_file_read_detected = False

    orig_sync_read = _sync_read_chunk

    def instrumented_sync_read(fd, offset, size):
        nonlocal closed_file_read_detected
        if getattr(fd, "closed", False):
            closed_file_read_detected = True
            raise ValueError("I/O operation on closed file")
        return orig_sync_read(fd, offset, size)

    monkeypatch.setattr("app.core.fast_telethon.breakpoints._sync_read_chunk", instrumented_sync_read)

    async def mock_call(sender, req):
        nonlocal cancelled
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            if req.file_part >= 4:
                cancelled = True
            await asyncio.sleep(0.01)
            return True
        return cfg

    client._call = mock_call

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        sender = MagicMock()
        sender.disconnect = AsyncMock()
        sender._connection = None
        return [sender for _ in coros]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    with pytest.raises(UploadCancelledError):
        await upload_file_parallel(
            client,
            str(test_file),
            concurrency=2,
            is_cancelled=lambda: cancelled,
        )

    # 验证未发生文件关闭后的非法读盘竞态
    assert not closed_file_read_detected, "取消时发生竞争读盘: 在文件关闭后仍然尝试读取句柄"


@pytest.mark.asyncio
async def test_upload_work_stealing_imbalance_distribution(tmp_path, monkeypatch):
    """3. 工作窃取快慢通道分配测试 (test_upload_work_stealing_imbalance_distribution):
    模拟 Channel 0 延迟 5ms，Channel 1 延迟 150ms。验证 Channel 0 发射的分块数占比 >= 80%。
    """
    num_chunks = 40
    file_bytes = b"W" * (CHUNK * num_chunks)
    test_file = safe_write_bytes(str(tmp_path / "work_stealing.bin"), file_bytes)

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    sender_0 = MagicMock()
    sender_0.name = "fast_channel_0"
    sender_1 = MagicMock()
    sender_1.name = "slow_channel_1"

    worker_parts = {sender_0.name: [], sender_1.name: []}
    slow_release = asyncio.Event()

    async def mock_call(sender, req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            name = getattr(sender, "name", "unknown")
            worker_parts[name].append(req.file_part)
            if name == sender_0.name:
                await asyncio.sleep(0)  # 让出调度以便慢通道进入阻塞点
                if len(worker_parts[sender_0.name]) >= 36:
                    slow_release.set()
            else:
                await slow_release.wait()  # 确定性模拟慢通道滞后直到快通道完成 >= 36 块
            return True
        return cfg

    client._call = mock_call

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [sender_0, sender_1]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    res = await asyncio.wait_for(
        upload_file_parallel(
            client,
            str(test_file),
            concurrency=2,
            in_flight_per_connection=1,
        ),
        timeout=10.0,
    )

    assert res.parts == 40
    fast_count = len(worker_parts[sender_0.name])
    slow_count = len(worker_parts[sender_1.name])
    assert fast_count + slow_count == 40

    fast_ratio = fast_count / 40.0
    assert fast_ratio >= 0.80, f"快通道工作窃取比例不足 80%: 快={fast_count}, 慢={slow_count}, 比例={fast_ratio:.2%}"


@pytest.mark.asyncio
async def test_upload_retry_queue_priority(tmp_path, monkeypatch):
    """4. 失败块 retry_queue 优先接管测试 (test_upload_retry_queue_priority):
    模拟 Channel 1 超时，分块推入 retry_queue，断言 Channel 0 优先重发，不重复读盘。
    """
    num_chunks = 8
    file_bytes = b"R" * (CHUNK * num_chunks)
    test_file = safe_write_bytes(str(tmp_path / "retry_priority.bin"), file_bytes)

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    read_counts = {}
    orig_read = _sync_read_chunk

    def tracking_read(fd, offset, size):
        part = offset // CHUNK
        read_counts[part] = read_counts.get(part, 0) + 1
        return orig_read(fd, offset, size)

    monkeypatch.setattr("app.core.fast_telethon.breakpoints._sync_read_chunk", tracking_read)

    sender_0 = MagicMock()
    sender_0.name = "ch0"
    sender_1 = MagicMock()
    sender_1.name = "ch1"

    failed_once = False
    dispatched_order = []

    async def mock_call(sender, req):
        nonlocal failed_once
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            pid = req.file_part
            dispatched_order.append((pid, sender.name))
            if pid == 1 and not failed_once:
                failed_once = True
                await asyncio.sleep(0.01)
                raise TimeoutError("模拟分块超时")
            await asyncio.sleep(0.01)
            return True
        return cfg

    client._call = mock_call

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [sender_0, sender_1]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    res = await upload_file_parallel(
        client,
        str(test_file),
        concurrency=2,
        in_flight_per_connection=1,
    )

    assert res.parts == 8
    assert failed_once is True
    # 验证 part 1 只从磁盘读取了 1 次（重试直接从 retry_queue 内存获取）
    assert read_counts[1] == 1, f"失败分块发生重复读盘，读盘次数: {read_counts[1]}"


@pytest.mark.asyncio
async def test_upload_direct_send_no_sleep(tmp_path, monkeypatch):
    """5. 绕过 Telethon _call 2秒休眠验证 (test_upload_direct_send_no_sleep):
    模拟真实客户端下网络超时，断言没有触发 Telethon 内部的 await asyncio.sleep(2)。
    """
    num_chunks = 4
    file_bytes = b"D" * (CHUNK * num_chunks)
    test_file = safe_write_bytes(str(tmp_path / "direct_send.bin"), file_bytes)

    # 构造真实的 TelegramClient 模拟对象，挂载标准的 TelegramClient._call
    client = MagicMock(spec=TelegramClient)
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    client._call = TelegramClient._call.__get__(client, TelegramClient)

    cfg = MagicMock()
    cfg.dc_options = []
    client_call_mock = AsyncMock(return_value=cfg)
    client.__call__ = client_call_mock

    # 监控 sender.send 直发
    direct_send_called = 0
    mock_sender = MagicMock()

    def fake_send(req):
        nonlocal direct_send_called
        direct_send_called += 1
        fut = asyncio.Future()
        fut.set_result(True)
        return fut

    mock_sender.send = fake_send

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [mock_sender]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    sleep_calls = []
    orig_sleep = asyncio.sleep

    async def tracking_sleep(delay, *args, **kwargs):
        sleep_calls.append(delay)
        return await orig_sleep(0.001)

    monkeypatch.setattr(asyncio, "sleep", tracking_sleep)

    res = await upload_file_parallel(
        client,
        str(test_file),
        concurrency=1,
        in_flight_per_connection=1,
    )

    assert res.parts == 4
    # 验证直接走 sender.send 协议直发，避开了 TelegramClient._call
    assert direct_send_called == 4
    # 验证没有触发 Telethon 内部的 2.0s 惩罚性休眠
    assert 2.0 not in sleep_calls


def test_speed_tracker_pause_smoothing():
    """6. SpeedTracker 3.5s ~ 4.5s 停顿区间平滑测试 (test_speed_tracker_pause_smoothing):
    在 t = base + 3.8s（无新块到达但未达 4.5s）断言 speed > 0.0 且连续递减，无直接跳 0。
    """
    total_bytes = 10 * 1024 * 1024
    tracker = SpeedTracker(total_bytes, initial_bytes=0, window_seconds=3.5, stagnant_timeout=4.5)

    base_time = tracker.start_time
    # 模拟以 2MB/s 稳定传输 3.5 秒
    for i in range(1, 8):
        t = base_time + i * 0.5
        with patch("time.time", return_value=t):
            tracker.update(i * 1024 * 1024)

    last_time = base_time + 3.5
    with patch("time.time", return_value=last_time):
        spd_base = tracker.speed
        assert spd_base > 1.8

    # 边界连续性验证 (3.49s -> 3.50s -> 3.51s): 杜绝在 3.5s 处产生正弦波毛刺反弹
    with patch("time.time", return_value=last_time + 3.49):
        spd_3_49 = tracker.speed
    with patch("time.time", return_value=last_time + 3.50):
        spd_3_50 = tracker.speed
    with patch("time.time", return_value=last_time + 3.51):
        spd_3_51 = tracker.speed

    assert spd_3_49 >= spd_3_50 >= spd_3_51, (
        f"3.5s 边界处未单调衰减发生毛刺突增: {spd_3_49} -> {spd_3_50} -> {spd_3_51}"
    )

    # 停顿进入 3.5s ~ 4.5s 死区 (无新块到达，idle_time 在 3.5s 到 4.5s 之间)
    with patch("time.time", return_value=last_time + 3.8):
        spd_3_8 = tracker.speed
        assert 0.0 < spd_3_8 < spd_3_51, f"3.8s 时速度应平滑递减，实际为 {spd_3_8}"

    with patch("time.time", return_value=last_time + 4.2):
        spd_4_2 = tracker.speed
        assert 0.0 < spd_4_2 < spd_3_8, f"4.2s 时速度应低于 3.8s，实际为 {spd_4_2}"

    # 超过 4.5s 停顿清零
    with patch("time.time", return_value=last_time + 4.6):
        assert tracker.speed == 0.0
        assert tracker.eta == 0.0


@pytest.mark.asyncio
async def test_probe_dc_endpoints_skips_when_proxy():
    """7. 代理环境下探针跳过测试 (test_probe_dc_endpoints_skips_when_proxy):
    配置代理时，断言 _probe_dc_endpoints 直接返回候选端点，未发起直连 TCP 握手。
    """
    dc1 = MagicMock(ip_address="149.154.167.50", port=443)
    dc2 = MagicMock(ip_address="149.154.167.51", port=443)
    dc3 = MagicMock(ip_address="149.154.167.52", port=443)
    candidates = [dc1, dc2, dc3]

    # 1. 存在代理时 -> 直接返回原始候选端点，不建立 TCP 握手
    client_with_proxy = MagicMock()
    client_with_proxy._proxy = {"proxy_type": 2, "addr": "127.0.0.1", "port": 1080}

    with patch("asyncio.open_connection") as mock_open:
        res = await _probe_dc_endpoints(client_with_proxy, candidates)
        assert res == candidates
        mock_open.assert_not_called()

    # 2. 无代理时 -> 并发测试 RTT 并挑选最优 Top 2 端点
    client_no_proxy = MagicMock()
    client_no_proxy._proxy = None

    async def fake_open(ip, port):
        # dc1: 50ms, dc2: 200ms, dc3: 10ms
        delay_map = {
            "149.154.167.50": 0.05,
            "149.154.167.51": 0.20,
            "149.154.167.52": 0.01,
        }
        await asyncio.sleep(delay_map.get(ip, 0.1))
        mock_writer = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()
        return MagicMock(), mock_writer

    with patch("asyncio.open_connection", side_effect=fake_open):
        best_candidates = await _probe_dc_endpoints(client_no_proxy, candidates)
        assert len(best_candidates) == 2
        # 最优前两名应为 dc3 (10ms) 和 dc1 (50ms)
        assert best_candidates[0] == dc3
        assert best_candidates[1] == dc1


@pytest.mark.asyncio
async def test_prefetch_producer_disk_error_propagates(tmp_path, monkeypatch):
    """8. 预读读盘异常快速熔断测试 (test_prefetch_producer_disk_error_propagates):
    模拟中途磁盘发生不可恢复 I/O 错误，断言 fatal_error 立即触发并原样上抛，而非静默吞没导致上传假死。
    """
    num_chunks = 10
    file_bytes = b"E" * (CHUNK * num_chunks)
    test_file = safe_write_bytes(str(tmp_path / "disk_err.bin"), file_bytes)

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    orig_read = _sync_read_chunk

    def faulty_read(fd, offset, size):
        part = offset // CHUNK
        if part >= 3:
            raise OSError("模拟磁盘不可恢复物理 I/O 错误")
        return orig_read(fd, offset, size)

    monkeypatch.setattr("app.core.fast_telethon.breakpoints._sync_read_chunk", faulty_read)

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        sender = MagicMock()
        sender.disconnect = AsyncMock()
        sender._connection = None
        return [sender for _ in coros]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    with pytest.raises(OSError, match="模拟磁盘不可恢复物理 I/O 错误"):
        await upload_file_parallel(
            client,
            str(test_file),
            concurrency=2,
            in_flight_per_connection=1,
        )


@pytest.mark.asyncio
async def test_worker_scale_up_sender_init_failure_safe(tmp_path, monkeypatch):
    """9. 动态扩容 Worker 初始化建连失败安全流转测试:
    新 Worker 建立 Sender 异常时，确保不会吞没或丢失预取分块，其他通道安全接管并完成 100% 上传。
    """
    num_chunks = 8
    file_bytes = b"S" * (CHUNK * num_chunks)
    test_file = safe_write_bytes(str(tmp_path / "scale_up_fail.bin"), file_bytes)

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    sender_good = MagicMock()
    sender_good.name = "good_ch"

    async def mock_call(sender, req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            await asyncio.sleep(0.01)
            return True
        return cfg

    client._call = mock_call

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [sender_good]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    init_call_count = 0

    async def flaky_create(cl, dc_id, dc_option=None):
        nonlocal init_call_count
        init_call_count += 1
        if init_call_count == 1:
            raise ConnectionResetError("模拟动态扩容通道建连瞬态重置")
        return sender_good

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._create_worker_sender_same_dc", flaky_create)

    current_concurrency = 1

    def get_c():
        return current_concurrency

    async def trigger_scale_up():
        await asyncio.sleep(0.02)
        nonlocal current_concurrency
        current_concurrency = 2

    asyncio.create_task(trigger_scale_up())

    res = await upload_file_parallel(
        client,
        str(test_file),
        concurrency=1,
        get_concurrency=get_c,
        in_flight_per_connection=1,
    )

    assert res.parts == 8


@pytest.mark.asyncio
async def test_probe_dc_endpoints_deduplicates_ips():
    """10. DC 端点探针重复 IP/端口去重测试:
    当存在多个相同 IP/端口的候选配置时，确保不会发起冗余并发探针。
    """
    dc1 = MagicMock(ip_address="149.154.167.50", port=443)
    dc1_dup = MagicMock(ip_address="149.154.167.50", port=443)
    dc2 = MagicMock(ip_address="149.154.167.51", port=443)
    candidates = [dc1, dc1_dup, dc2]

    client = MagicMock()
    client._proxy = None

    probed_ips = []

    async def fake_open(ip, port):
        probed_ips.append((ip, port))
        mock_writer = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()
        return MagicMock(), mock_writer

    with patch("asyncio.open_connection", side_effect=fake_open):
        best = await _probe_dc_endpoints(client, candidates)
        assert len(best) == 2
        # 断言探针未对重复 IP 发起两次 open_connection
        assert len(probed_ips) == 2
        assert probed_ips == [("149.154.167.50", 443), ("149.154.167.51", 443)]


@pytest.mark.asyncio
async def test_upload_flood_premium_wait_cooperative_backoff(tmp_path, monkeypatch):
    """11. 非会员账号 FloodPremiumWaitError 异常捕获与跨 Worker 协同退避测试:
    验证遭遇 FloodPremiumWaitError (seconds=3) 时:
    1. 异常被捕获且不作为 5-strike 致命错误误杀 (即使重试 6 次仍正常恢复);
    2. 全局共享 flood_wait_until 触发跨 Worker 协同退避;
    3. 冷却退避后全部文件分块上传成功。
    """
    from telethon.errors import FloodPremiumWaitError

    num_chunks = 4
    file_bytes = b"F" * (CHUNK * num_chunks)
    test_file = safe_write_bytes(str(tmp_path / "flood_premium.bin"), file_bytes)

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    sender_0 = MagicMock()
    sender_0.name = "ch_0"
    sender_1 = MagicMock()
    sender_1.name = "ch_1"

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [sender_0, sender_1]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    sim_time = [1000.0]

    def fake_time():
        return sim_time[0]

    slept_intervals = []
    original_sleep = asyncio.sleep

    async def fast_sim_sleep(seconds):
        slept_intervals.append(seconds)
        sim_time[0] += max(0.0, float(seconds))
        await original_sleep(0.001)

    monkeypatch.setattr(time, "time", fake_time)
    monkeypatch.setattr(asyncio, "sleep", fast_sim_sleep)

    flood_err = FloodPremiumWaitError(request=None)
    flood_err.seconds = 3

    flood_strikes = 0
    call_timestamps = []
    flood_event_times = []

    async def mock_call(sender, req):
        nonlocal flood_strikes
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            now = sim_time[0]
            call_timestamps.append((now, getattr(sender, "name", "unknown"), req.file_part))
            if req.file_part == 0 and flood_strikes < 6:
                flood_strikes += 1
                flood_event_times.append(now)
                raise flood_err
            return True
        return cfg

    client._call = mock_call

    res = await upload_file_parallel(
        client,
        str(test_file),
        concurrency=2,
        in_flight_per_connection=1,
    )

    assert res.parts == 4
    # 验证 part 0 连续经历了 6 次 FloodPremiumWaitError，未被 5-strike 机制误杀
    assert flood_strikes == 6

    # 验证记录了 >= 3s 的冷却休眠
    assert any(s >= 3.0 for s in slept_intervals), "未能记录到 >= 3s 的 Flood 退避休眠"

    # 验证协同退避: 在冷却窗口内，其他 Worker 协同等待，3 秒冷却窗口内绝无任何发包请求穿透
    for t_flood in flood_event_times:
        leaked_calls = [t for t, _, _ in call_timestamps if t_flood < t < t_flood + 3.0]
        assert not leaked_calls, f"Flood 冷却窗口 (至 {t_flood + 3.0}s) 内发生了穿透发包: {leaked_calls}"


@pytest.mark.asyncio
async def test_upload_flood_wait_multi_worker_varying_seconds_and_custom_seconds(tmp_path, monkeypatch):
    """12. 多 Worker 遭遇不同冷却秒数及自定义异常 seconds 属性测试:
    验证:
    1. Worker 0 (3s) 与 Worker 1 (5s) 相继触发 Flood 时，flood_wait_until 取 max 扩展至 5s;
    2. 自定义包含 getattr(e, 'seconds', None) 的非 Telethon 原生异常被识别为 Flood 等待;
    3. 所有发包均严格在退避窗口结束后发起。
    """
    from telethon.errors import FloodPremiumWaitError, FloodWaitError

    num_chunks = 4
    file_bytes = b"M" * (CHUNK * num_chunks)
    test_file = safe_write_bytes(str(tmp_path / "multi_flood.bin"), file_bytes)

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    sender_0 = MagicMock()
    sender_0.name = "ch_0"
    sender_1 = MagicMock()
    sender_1.name = "ch_1"

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [sender_0, sender_1]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    sim_time = [2000.0]

    def fake_time():
        return sim_time[0]

    original_sleep = asyncio.sleep

    async def fast_sim_sleep(seconds):
        sim_time[0] += max(0.0, float(seconds))
        await original_sleep(0.001)

    monkeypatch.setattr(time, "time", fake_time)
    monkeypatch.setattr(asyncio, "sleep", fast_sim_sleep)

    class CustomRateLimitError(Exception):
        """测试第三方或自定义带 seconds 属性的限流异常"""

        def __init__(self, s):
            self.seconds = s

    err3 = FloodPremiumWaitError(request=None)
    err3.seconds = 3
    err5 = FloodWaitError(request=None)
    err5.seconds = 5
    err4_custom = CustomRateLimitError(4)

    calls = []
    flood_windows = []
    strikes = {0: 0, 1: 0, 2: 0}

    async def mock_call(sender, req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            now = sim_time[0]
            calls.append((now, getattr(sender, "name", "unknown"), req.file_part))
            if req.file_part == 0 and strikes[0] < 1:
                strikes[0] += 1
                flood_windows.append((now, now + 3.0))
                raise err3
            if req.file_part == 1 and strikes[1] < 1:
                strikes[1] += 1
                flood_windows.append((now, now + 5.0))
                raise err5
            if req.file_part == 2 and strikes[2] < 1:
                strikes[2] += 1
                flood_windows.append((now, now + 4.0))
                raise err4_custom
            return True
        return cfg

    client._call = mock_call

    res = await upload_file_parallel(
        client,
        str(test_file),
        concurrency=2,
        in_flight_per_connection=1,
    )

    assert res.parts == 4
    # 验证自定义异常和 Telethon 异常均被干净重试
    assert strikes[0] == 1 and strikes[1] == 1 and strikes[2] == 1

    # 验证每一个 Flood 窗口期内均无发包穿透
    for f_start, f_end in flood_windows:
        for c_time, ch, pid in calls:
            assert not (f_start < c_time < f_end - 1e-6), (
                f"在 Flood 窗口 [{f_start}, {f_end}] 内发包泄漏: 块 {pid} 由 {ch} 在 {c_time} 发送"
            )


@pytest.mark.asyncio
async def test_upload_flood_wait_exception_with_none_seconds_hits_strike_limit(tmp_path, monkeypatch):
    """13. 异常中 seconds 属性为 None 时不误判为 Flood 异常:
    验证当异常有 seconds 属性但为 None 时，正常走普通错误分支累加失败计数并触发 5 次熔断。
    """

    class ErrorWithNoneSeconds(Exception):
        seconds = None

    num_chunks = 2
    file_bytes = b"E" * (CHUNK * num_chunks)
    test_file = safe_write_bytes(str(tmp_path / "none_seconds.bin"), file_bytes)

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    sender_0 = MagicMock()

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [sender_0]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    attempt_count = 0

    async def mock_call(sender, req):
        nonlocal attempt_count
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            if req.file_part == 0:
                attempt_count += 1
                raise ErrorWithNoneSeconds("普通错误，但带有 None seconds 属性")
            return True
        return cfg

    async def mock_direct_call(req):
        return await mock_call(None, req)

    client._call = mock_call
    client.side_effect = mock_direct_call

    # 预期达到 5 次上限后抛出 fatal_error 中止上传
    with pytest.raises(ErrorWithNoneSeconds):
        await upload_file_parallel(
            client,
            str(test_file),
            concurrency=1,
            in_flight_per_connection=1,
        )

    # 验证精准重试 5 次后熔断，未被当成无限退避的 Flood 处理
    assert attempt_count == 5


@pytest.mark.asyncio
async def test_upload_flood_wait_pause_cancels_cleanly_without_transmission(tmp_path, monkeypatch):
    """14. 在 Flood 冷却休眠期间任务被暂停:
    验证处于等待中的 Worker 醒来后检测到 pause 状态，不会向 Telegram 发包，且在途未完成块妥善留存。
    """
    from telethon.errors import FloodPremiumWaitError

    from app.core.scheduler import UploadPausedError

    num_chunks = 3
    file_bytes = b"P" * (CHUNK * num_chunks)
    test_file = safe_write_bytes(str(tmp_path / "pause_during_flood.bin"), file_bytes)

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    sender_0 = MagicMock()

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [sender_0]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    is_paused_flag = False
    calls_after_pause = []
    flood_triggered = False

    flood_err = FloodPremiumWaitError(request=None)
    flood_err.seconds = 3

    async def mock_call(sender, req):
        nonlocal flood_triggered
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            if is_paused_flag:
                calls_after_pause.append(req.file_part)
            if req.file_part == 0 and not flood_triggered:
                flood_triggered = True
                raise flood_err
            return True
        return cfg

    client._call = mock_call

    # 在后台异步触发暂停 (模拟 0.1s 后用户暂停)
    async def trigger_pause_soon():
        await asyncio.sleep(0.1)
        nonlocal is_paused_flag
        is_paused_flag = True

    pause_task = asyncio.create_task(trigger_pause_soon())

    with pytest.raises(UploadPausedError):
        await upload_file_parallel(
            client,
            str(test_file),
            concurrency=1,
            in_flight_per_connection=1,
            is_paused=lambda: is_paused_flag,
        )

    await pause_task

    # 验证暂停后没有向服务端发送任何请求
    assert not calls_after_pause, f"暂停后泄漏了请求: {calls_after_pause}"


@pytest.mark.asyncio
async def test_upload_worker_restart_reinitializes_sender(tmp_path, monkeypatch):
    """15. Worker 退出后再次重启/扩容重新初始化专用 Sender 测试:
    初始并发为 2，中间动态缩容至 1 导致 Worker 1 退出并清理 Sender，
    随后动态扩容回 2，验证 Worker 1 重启后能够重新成功执行 init_sender 获取独立 Sender，
    杜绝 worker_senders 保留 None 键导致永久退化为慢速单连接。
    """
    num_chunks = 20
    file_bytes = b"R" * (CHUNK * num_chunks)
    test_file = safe_write_bytes(str(tmp_path / "worker_restart.bin"), file_bytes)

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    created_senders = []
    sender_used_in_calls = []
    worker_1_first_sender = None
    worker_1_closed = asyncio.Event()
    worker_1_restarted = asyncio.Event()
    gate_later_parts = asyncio.Event()

    async def fake_create_sender(cl, dc_id, dc_option=None):
        s = MagicMock(name=f"sender_{len(created_senders)}")
        created_senders.append(s)
        if len(created_senders) == 3:
            worker_1_restarted.set()
        return s

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._create_worker_sender_same_dc", fake_create_sender)

    async def fake_gather(coros, *args, **kwargs):
        results = []
        for c in coros:
            res = await c
            results.append(res)
        return results

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    orig_close = fast_telethon.sender_pool._close_worker_sender

    async def tracking_close(cl, sender, dc):
        if sender == worker_1_first_sender:
            worker_1_closed.set()
        return await orig_close(cl, sender, dc)

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._close_worker_sender", tracking_close)

    current_concurrency = 2

    def get_c():
        return current_concurrency

    worker_1_handled_parts = []

    async def mock_call(sender, req):
        nonlocal worker_1_first_sender
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            sender_used_in_calls.append(sender)
            if len(created_senders) >= 2 and sender == created_senders[1] and worker_1_first_sender is None:
                worker_1_first_sender = sender
            if sender == worker_1_first_sender:
                worker_1_handled_parts.append(req.file_part)
            if req.file_part >= 5:
                await gate_later_parts.wait()
            await asyncio.sleep(0.01)
            return True
        return cfg

    client._call = mock_call

    async def scale_controller():
        # 等待 worker 1 处理至少一个分块
        while not worker_1_handled_parts:
            await asyncio.sleep(0.01)
        # 触发缩容至 1
        nonlocal current_concurrency
        current_concurrency = 1
        # 等待 worker 1 退出且 close 被调用
        await asyncio.wait_for(worker_1_closed.wait(), timeout=5.0)
        # 此时 worker 1 已退出且 worker_senders.pop(1, None) 已完成
        # 触发重新扩容至 2
        current_concurrency = 2
        # 等待 worker 1 重新拉起并创建了第 3 个 sender
        await asyncio.wait_for(worker_1_restarted.wait(), timeout=5.0)
        # 放行剩余分块
        gate_later_parts.set()

    scale_task = asyncio.create_task(scale_controller())

    res = await upload_file_parallel(
        client,
        str(test_file),
        concurrency=2,
        get_concurrency=get_c,
        in_flight_per_connection=1,
    )
    await scale_task

    assert res.parts == num_chunks
    # 验证 Worker 1 重启时再次调用了 fake_create_sender，总共创建了至少 3 个 Sender (Worker 0, Worker 1 首次, Worker 1 重启)
    assert len(created_senders) >= 3, f"预期至少创建 3 个 Sender，实际创建: {len(created_senders)}"
    # 验证所有上传调用均携带了有效的 Sender，从未退化为 sender is None
    assert all(s is not None for s in sender_used_in_calls), "存在未关联独立 Sender 的退化上传调用"
    # 验证重启后的 Sender 也参与了后续分块发送
    assert created_senders[2] in sender_used_in_calls, "重启后的专用 Sender 未被使用"


# ==============================================================================
# Part: test_upload_pipelining.py
# ==============================================================================
CHUNK = 512 * 1024


@pytest.mark.asyncio
async def test_single_connection_pipelining_concurrency_and_ack_order(tmp_dir, monkeypatch):
    """测试单连接管线化 (Pipelining): 验证单个连接能并发提交 2 个在途分块，且无需等待前一个分块 ACK 即可发起下一个"""
    num_chunks = 4
    file_bytes = b"P" * (CHUNK * num_chunks)
    test_file = safe_write_bytes(os.path.join(tmp_dir, "pipelined.bin"), file_bytes)

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    # 监控并发在途分块
    in_flight_parts = set()
    max_observed_in_flight = 0
    received_parts = []
    events = {}
    for i in range(num_chunks):
        events[i] = asyncio.Event()

    # 拦截 _call / send 模拟网络延迟与在途管线
    async def mock_call(sender, req):
        nonlocal max_observed_in_flight
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            pid = req.file_part
            in_flight_parts.add(pid)
            max_observed_in_flight = max(max_observed_in_flight, len(in_flight_parts))

            # 让分块 0 等待分块 1 也进入在途状态后再返回 ACK
            if pid == 0:
                # 等待分块 1 发起
                for _ in range(50):
                    if 1 in in_flight_parts:
                        break
                    await asyncio.sleep(0.02)

            await asyncio.sleep(0.05)
            received_parts.append(pid)
            in_flight_parts.discard(pid)
            return True
        return cfg

    client._call = mock_call

    # 仅建 1 个 Worker 连接，单连接在途设为 2
    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        sender = MagicMock()
        sender.disconnect = AsyncMock()
        sender._connection = None
        return [sender for _ in coros]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    await upload_file_parallel(client, str(test_file), concurrency=1, in_flight_per_connection=2)

    # 验证最大在途分块数达到 2 (管线化生效，打破停等协议)
    assert max_observed_in_flight == 2, f"预期单连接在途峰值为 2，实际为 {max_observed_in_flight}"
    # 验证所有 4 个分块均成功收到 ACK
    assert len(received_parts) == 4
    assert set(received_parts) == {0, 1, 2, 3}


@pytest.mark.asyncio
async def test_pipelining_pause_reclaims_unacked_chunks(tmp_dir, monkeypatch):
    """测试暂停时的强安全语义：在途未 ACK 分块必须被可靠拦截并推回队列，.upload.meta 仅记录已 ACK 分块"""
    num_chunks = 10
    file_bytes = b"R" * (CHUNK * num_chunks)
    test_file = safe_write_bytes(os.path.join(tmp_dir, "resume_test.bin"), file_bytes)
    # meta 收拢至沙箱 (键含 size, 与 upload_file_parallel 内部派生一致)
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", os.path.join(tmp_dir, "meta_sandbox"))
    from app.core.fast_telethon import local_upload_meta_path

    meta_path = local_upload_meta_path(str(test_file), len(file_bytes))

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []

    is_paused = False
    started_parts = []
    acked_parts = []

    async def mock_call(sender, req):
        nonlocal is_paused
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            pid = req.file_part
            started_parts.append(pid)
            # 分块 0, 1 顺利完成
            if pid in (0, 1):
                await asyncio.sleep(0.02)
                acked_parts.append(pid)
                return True

            # 当分块 2, 3 发起时触发暂停，并挂起等待取消
            if pid in (2, 3):
                is_paused = True
                await asyncio.sleep(10.0)  # 模拟在途耗时，等待被 pause 取消
                acked_parts.append(pid)
                return True

            await asyncio.sleep(0.01)
            acked_parts.append(pid)
            return True
        return cfg

    client._call = mock_call

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        sender = MagicMock()
        sender.disconnect = AsyncMock()
        sender._connection = None
        return [sender for _ in coros]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    # 首次上传并暂停
    with pytest.raises(UploadPausedError):
        await upload_file_parallel(
            client, str(test_file), concurrency=1, in_flight_per_connection=2, is_paused=lambda: is_paused
        )

    # 1. 验证断点文件已保存
    assert os.path.exists(meta_path), "暂停收尾必须保存 .upload.meta"
    with open(meta_path, encoding="utf-8") as f:
        meta_data = json.load(f)

    # 2. 强安全语义验证：在途的 2, 3 绝不能出现在 completed_parts 中
    assert set(meta_data["completed_parts"]) == {0, 1}, (
        f"断点中仅应包含已 ACK 的分块 [0, 1]，实际为 {meta_data['completed_parts']}"
    )

    # 3. 恢复上传 (Resume)：重置暂停状态，续传剩余分块
    is_paused = False
    started_parts.clear()
    acked_parts.clear()

    async def mock_call_resume(sender, req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            pid = req.file_part
            started_parts.append(pid)
            acked_parts.append(pid)
            return True
        return cfg

    client._call = mock_call_resume

    await upload_file_parallel(
        client, str(test_file), concurrency=1, in_flight_per_connection=2, is_paused=lambda: False
    )

    # 验证续传从分块 2 开始补齐 (2 ~ 9)
    assert set(started_parts) == set(range(2, 10))
    # 验证上传完成后断点文件被清理
    assert not os.path.exists(meta_path)


def test_config_cascade_priority(setup_test_db, monkeypatch):
    """测试配置级联优先级: task.params -> SystemSettingsRepository -> settings 兜底与越界约束 [1, 4]"""
    # 1. settings 环境变量兜底
    monkeypatch.setattr(settings, "UPLOAD_IN_FLIGHT_PER_CONNECTION", 2)
    val_db = SystemSettingsRepository.get("upload_in_flight_per_connection")
    assert val_db == "2"

    # 2. 模拟 DB 修改为 3
    SystemSettingsRepository.set("upload_in_flight_per_connection", "3")
    assert SystemSettingsRepository.get_int("upload_in_flight_per_connection", 2) == 3

    # 3. 测试 PublisherService 内部闭包解析优先级

    # 当 task.params 传入 4 时，覆盖 DB 的 3
    # 构造 mock task
    from app.db.task_repository import TaskQueueRepository

    task_id = "test_cascade_task"
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="publish_folder",
        params={"upload_in_flight_per_connection": 4, "folder_path": "dummy", "target_channel": "-100123"},
    )

    # 测试通过 get_current_in_flight_per_connection 解析
    # 验证读取 task.params 优先得到 4
    task_rec = TaskQueueRepository.get_by_id(task_id)
    assert (task_rec.get("params") or {}).get("upload_in_flight_per_connection") == 4


@pytest.mark.asyncio
async def test_rpc_latency_percentiles_and_report(tmp_dir, monkeypatch, caplog):
    """测试 RPC P95 / P99 延迟计算及 [FastTelethon 传输报告] 格式化日志输出"""
    num_chunks = 20
    file_bytes = b"L" * (CHUNK * num_chunks)
    test_file = safe_write_bytes(os.path.join(tmp_dir, "latency_test.bin"), file_bytes)

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []

    # 模拟每次 RPC 延迟
    async def mock_call(sender, req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            await asyncio.sleep(0.01)
            return True
        return cfg

    client._call = mock_call

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        sender = MagicMock()
        sender.disconnect = AsyncMock()
        sender._connection = None
        return [sender for _ in coros]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    with caplog.at_level(logging.INFO):
        await upload_file_parallel(client, str(test_file), concurrency=2, in_flight_per_connection=2)

    # 验证日志中输出了规范化的传输报告
    report_logs = [rec.message for rec in caplog.records if "FastTelethon 传输报告" in rec.message]
    assert len(report_logs) >= 1, "必须输出 [FastTelethon 传输报告]"
    rep = report_logs[0]
    assert "连接数: 2" in rep
    assert "单连接在途: 2" in rep
    assert f"总分块: {num_chunks}" in rep
    assert "RPC P95延迟:" in rep
    assert "平均速度:" in rep


# ==============================================================================
# Part: test_transfer_concurrency_fixes.py
# ==============================================================================
CHUNK = 512 * 1024


def _raw_media(dc_id: int = 2) -> MagicMock:
    media = MagicMock()
    media.document = MagicMock(dc_id=dc_id)
    media.photo = None
    return media


@pytest.fixture
def fake_senders(monkeypatch):
    async def fake_create(*args, **kwargs):
        return object()

    async def fake_get(*args, **kwargs):
        return object()

    monkeypatch.setattr(fast_telethon.sender_pool, "_create_worker_sender_with_auth", fake_create)
    monkeypatch.setattr(fast_telethon.sender_pool, "_get_worker_sender", fake_get)


def test_safe_invoke_progress_signature_handling():
    """验证 _safe_invoke_progress 正确根据签名自省传参，不盲目尝试"""
    # 1. 普通 5 参数回调
    calls_5 = []

    def cb_5(a, b, c, d, e):
        calls_5.append((a, b, c, d, e))

    safe_invoke_progress(cb_5, 1, 10, 5.0, 10.0, "file.mp4", concurrency=4, dc_id=2, concurrency_total=8, percent=10.0)
    assert calls_5 == [(1, 10, 5.0, 10.0, "file.mp4")]

    # 2. 支持部分关键字参数的回调
    calls_subset = []

    def cb_subset(a, b, c, d, e, concurrency=None, percent=None):
        calls_subset.append((a, b, c, d, e, concurrency, percent))

    safe_invoke_progress(
        cb_subset, 1, 10, 5.0, 10.0, "file.mp4", concurrency=4, dc_id=2, concurrency_total=8, percent=10.0
    )
    assert calls_subset == [(1, 10, 5.0, 10.0, "file.mp4", 4, 10.0)]

    # 3. 支持 **kwargs 的回调
    calls_kw = []

    def cb_kw(a, b, c, d, e, **kwargs):
        calls_kw.append((a, b, c, d, e, kwargs))

    safe_invoke_progress(cb_kw, 1, 10, 5.0, 10.0, "file.mp4", concurrency=4, dc_id=2, concurrency_total=8, percent=10.0)
    assert len(calls_kw) == 1
    assert calls_kw[0][5]["concurrency"] == 4
    assert calls_kw[0][5]["dc_id"] == 2
    assert calls_kw[0][5]["concurrency_total"] == 8
    assert calls_kw[0][5]["percent"] == 10.0

    # 4. 形参顺序中关键字参数位于其它位置参数之前的回调 (防 TypeError: got multiple values for argument)
    calls_pos_kw = []

    def cb_pos_kw(current, total, percent=0, speed=0, eta=0, file_name=""):
        calls_pos_kw.append((current, total, percent, speed, eta, file_name))

    safe_invoke_progress(
        cb_pos_kw, 1, 10, 5.0, 10.0, "file.mp4", concurrency=4, dc_id=2, concurrency_total=8, percent=10.0
    )
    assert calls_pos_kw == [(1, 10, 10.0, 0, 0, "")]

    # 5. 仅位置参数 (POSITIONAL_ONLY) 与仅关键字参数 (KEYWORD_ONLY) 回调
    calls_pos_only = []

    def cb_pos_only(a, b, /, c=0, *, percent=0):
        calls_pos_only.append((a, b, c, percent))

    safe_invoke_progress(cb_pos_only, 1, 10, 5.0, 10.0, "file.mp4", concurrency=4, percent=10.0)
    assert calls_pos_only == [(1, 10, 5.0, 10.0)]

    # 6. 带 *args 与关键字参数的回调
    calls_var_pos = []

    def cb_var_pos(a, b, *args, percent=0):
        calls_var_pos.append((a, b, args, percent))

    safe_invoke_progress(cb_var_pos, 1, 10, 5.0, 10.0, "file.mp4", concurrency=4, percent=10.0)
    assert calls_var_pos == [(1, 10, (5.0, 10.0, "file.mp4"), 10.0)]


def test_safe_invoke_progress_unintrospectable_callable_fallback(monkeypatch):
    """验证当 inspect.signature 无法自省 callable (如某些 C 扩展或特化 callable) 时的回退与容错"""
    # 1. 模拟无法自省的 5 参数 C 扩展回调 (不接受任何 kwargs)
    c_calls_5 = []

    def raw_c_cb_5(*args, **kwargs):
        if kwargs:
            raise TypeError("raw_c_cb_5 takes no keyword arguments")
        if len(args) != 5:
            raise TypeError(f"raw_c_cb_5 expected 5 arguments, got {len(args)}")
        c_calls_5.append(args)

    with patch("inspect.signature", side_effect=TypeError("no signature found for builtin")):
        safe_invoke_progress(raw_c_cb_5, 1, 10, 5.0, 10.0, "file.mp4", concurrency=4, percent=10.0)
    assert c_calls_5 == [(1, 10, 5.0, 10.0, "file.mp4")]

    # 2. 模拟无法自省的 2 参数 C 扩展回调 (不接受任何 kwargs)
    c_calls_2 = []

    def raw_c_cb_2(*args, **kwargs):
        if kwargs:
            raise TypeError("raw_c_cb_2 takes no keyword arguments")
        if len(args) != 2:
            raise TypeError(f"raw_c_cb_2 expected 2 arguments, got {len(args)}")
        c_calls_2.append(args)

    with patch("inspect.signature", side_effect=TypeError("no signature found for builtin")):
        safe_invoke_progress(raw_c_cb_2, 1, 10, 5.0, 10.0, "file.mp4", concurrency=4, percent=10.0)
    assert c_calls_2 == [(1, 10)]

    # 3. 模拟无法自省的 callable 内部发生业务 TypeError 时不被掩盖
    def buggy_c_cb(*args, **kwargs):
        return "number: " + 123

    with patch("inspect.signature", side_effect=TypeError("no signature found for builtin")):
        with pytest.raises(TypeError, match="can only concatenate str"):
            safe_invoke_progress(buggy_c_cb, 1, 10, 5.0, 10.0, "file.mp4", concurrency=4, percent=10.0)


def test_safe_invoke_progress_internal_type_error_not_masked():
    """验证回调函数内部发生 TypeError 时不会被错误地捕获并重试掩盖"""
    call_count = 0

    def buggy_cb(a, b, c, d, e, concurrency=None):
        nonlocal call_count
        call_count += 1
        # 内部业务逻辑引发 TypeError
        return "number: " + 123

    with pytest.raises(TypeError, match="can only concatenate str"):
        safe_invoke_progress(
            buggy_cb, 1, 10, 5.0, 10.0, "file.mp4", concurrency=4, dc_id=2, concurrency_total=8, percent=10.0
        )

    # 确保只被调用了一次，未触发多层重试盲试
    assert call_count == 1


@pytest.mark.asyncio
async def test_publisher_service_legacy_callback_compatibility(tmp_dir, monkeypatch):
    """验证 PublisherService 对只接受 5 个位置参数的传统回调函数具有完全兼容性，
    覆盖 5 处关键进度汇报点：超限跳过、防重跳过、相册上传中hook、相册完成、单文件完成，
    确保不抛出 'TypeError: legacy_progress_cb() got an unexpected keyword argument'。
    """
    from _safeio import safe_write_bytes

    from app.db.publish_repository import PublishHistoryRepository

    calls = []

    def legacy_progress_cb(current, total, speed, eta, file_name):
        calls.append((current, total, speed, eta, file_name))

    # 1. 单元级验证：通过 _safe_invoke_progress 直接调用，覆盖所有关键分支的参数组合
    # 分支 1: 单文件超限跳过分支 (带 percent)
    safe_invoke_progress(legacy_progress_cb, 0, 5, 0.0, 0.0, "oversized.mp4", percent=0.0)
    # 分支 2: 防重跳过已发布分支 (带 percent)
    safe_invoke_progress(legacy_progress_cb, 1, 5, 0.0, 0.0, "already_pub.mp4", percent=20.0)
    # 分支 3: 相册模式上传中进度 (带 speed, eta, concurrency, dc_id, concurrency_total, percent)
    safe_invoke_progress(
        legacy_progress_cb,
        1,
        5,
        1024.0,
        5.0,
        "album_img2.jpg",
        concurrency=1,
        dc_id=4,
        concurrency_total=1,
        percent=30.0,
    )
    # 分支 4: 相册模式发布完成汇报 (5 参)
    safe_invoke_progress(legacy_progress_cb, 2, 5, 0.0, 0.0, "album_img2.jpg")
    # 分支 5: 单文件发布成功汇报 (带 percent)
    safe_invoke_progress(legacy_progress_cb, 3, 5, 0.0, 0.0, "single_video.mp4", percent=60.0)
    # 扩展: Userbot 独立分块上传 hook (带 workers, dc_id, workers_total, percent)
    safe_invoke_progress(
        legacy_progress_cb,
        3,
        5,
        2048.0,
        2.0,
        "single_video.mp4",
        concurrency=4,
        dc_id=2,
        concurrency_total=4,
        percent=50.0,
    )

    assert len(calls) == 6
    for call in calls:
        assert len(call) == 5
        assert isinstance(call[0], int)
        assert isinstance(call[1], int)
        assert isinstance(call[2], float)
        assert isinstance(call[3], float)
        assert isinstance(call[4], str)

    calls.clear()

    # 边界情况验证: None 回调安全无异常
    safe_invoke_progress(None, 1, 5, 0.0, 0.0, "test.mp4", percent=20.0)

    # 边界情况验证: 2 参数极简回调 (current, total)
    calls_2 = []

    def cb_2(current, total):
        calls_2.append((current, total))

    safe_invoke_progress(cb_2, 1, 5, 1024.0, 5.0, "test.mp4", concurrency=1, percent=20.0)
    assert calls_2 == [(1, 5)]

    # 2. 业务级端到端验证：在 publish_folder 真实流程中覆盖全部 5 处分支
    target_chat = "-100900044"
    safe_write_bytes(os.path.join(tmp_dir, "oversized.jpg"), b"x" * 200)
    published_path = safe_write_bytes(os.path.join(tmp_dir, "published.jpg"), b"p" * 50)
    safe_write_bytes(os.path.join(tmp_dir, "img1.jpg"), b"i" * 50)
    safe_write_bytes(os.path.join(tmp_dir, "img2.jpg"), b"i" * 50)
    safe_write_bytes(os.path.join(tmp_dir, "single.mp4"), b"v" * 50)

    # 预先录入已发布历史
    PublishHistoryRepository.record(target_chat, str(published_path), 50, "")

    # 模拟账号与客户端
    account_client = AsyncMock()

    async def mock_send_file(chat_id, file_path_or_list, **kwargs):
        hook = kwargs.get("progress_callback")
        if hook:
            hook(50, 100)
        # 相册组发对齐真实 TG 形态: media group 逐张返回消息列表 (包1e J5 语义下,
        # 非列表 stub 的缺位项会被判为"未返回消息 ID"而计入失败)
        if isinstance(file_path_or_list, list):
            return [MagicMock() for _ in file_path_or_list]
        return MagicMock()

    account_client.send_file = mock_send_file
    account_client.send_message = AsyncMock()

    mock_acc = MagicMock()
    mock_acc.account_id = 99
    mock_acc.account_name = "Userbot_Test"
    mock_acc.is_ready.return_value = True
    mock_acc.is_premium = True
    mock_acc.effective_max_file_size_bytes.return_value = 100  # 100 字节上限，使得 oversized.jpg (200B) 超限
    mock_acc.probe_premium = AsyncMock(return_value=True)
    mock_acc.client = account_client

    mock_inspect = AsyncMock(
        return_value={
            "channel_identifier": target_chat,
            "is_accessible": True,
            "has_write_permission": True,
            "permission_summary": "测试发布权限",
        }
    )

    bot_client = AsyncMock()
    bot_client.send_file = mock_send_file

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
        patch.object(fast_telethon, "upload_file_parallel", new_callable=AsyncMock) as mock_ft_upload,
    ):

        async def fake_upload_parallel(client, path, **kwargs):
            hook = kwargs.get("progress_callback")
            if hook:
                hook(50, 100, 1024.0, 1.0, 2, 2, 2)
            return MagicMock()

        mock_ft_upload.side_effect = fake_upload_parallel

        result = await PublisherService.publish_folder(
            folder_path=tmp_dir,
            target_chat_id=target_chat,
            album_mode=True,
            delay_seconds=0.01,
            progress_callback=legacy_progress_cb,
        )

        assert result.total_files_count == 5
        assert result.published_count == 4
        # D3: 超限文件 → skipped_files (非 failed)
        assert len(result.skipped_files) == 1
        assert result.failed_files == []
        assert result.skipped_files[0].file_name == "oversized.jpg"

        assert len(calls) > 0
        for call in calls:
            assert len(call) == 5

        file_names_called = [c[4] for c in calls]
        assert "oversized.jpg" in file_names_called
        assert "published.jpg" in file_names_called
        assert "img2.jpg" in file_names_called
        assert "single.mp4" in file_names_called


@pytest.mark.asyncio
async def test_downloader_concurrency_ttl_cache(monkeypatch):
    """验证 get_current_download_concurrency 具有 2.0 秒短时 TTL 缓存，避免高频穿透查库"""
    from app.db.account_repository import SystemSettingsRepository

    db_queries = 0

    def mock_get(key, default=None):
        nonlocal db_queries
        db_queries += 1
        return "8"

    monkeypatch.setattr(SystemSettingsRepository, "get", mock_get)

    msg = MagicMock(id=999, media=MagicMock(), grouped_id=None)
    monkeypatch.setattr(
        DownloaderService, "get_file_name_and_size", staticmethod(lambda m, fallback_id=None: ("test.bin", 1024))
    )

    fake_client = AsyncMock()
    fake_client.is_connected = MagicMock(return_value=True)

    async def fake_download(*args, **kwargs):
        get_conc = kwargs.get("get_concurrency")
        if get_conc:
            for _ in range(10):
                get_conc()
        return "/tmp/fake.bin"

    monkeypatch.setattr("app.services.downloader_service.download_file_parallel", fake_download)
    monkeypatch.setattr("app.db.download_repository.DownloadHistoryRepository.is_downloaded", lambda *a: False)

    await DownloaderService.download_single_message(
        client=fake_client, chat_id="-1001", message=msg, task_id=None, threads=None
    )

    # 在 2.0s 内调用了 10+ 次 get_current_download_concurrency，查库次数应仅为 1 次
    assert db_queries == 1


@pytest.mark.asyncio
async def test_dynamic_scale_down_cancels_excess_workers(tmp_dir, fake_senders, monkeypatch):
    """验证动态缩容时超出目标并发的挂起 Worker Task 能被显式 cancel 且通道安全关闭"""
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", tmp_dir)
    out = os.path.join(tmp_dir, "scaledown.bin")
    total = 8 * CHUNK

    # 模拟阻塞客户端: Worker 在网络请求中挂起
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)

    block_event = asyncio.Event()
    worker_invocations = set()
    cancelled_workers = 0

    async def blocking_call(sender, request):
        nonlocal cancelled_workers
        worker_invocations.add(request.offset)
        try:
            await block_event.wait()
        except asyncio.CancelledError:
            cancelled_workers += 1
            raise
        res = MagicMock()
        res.bytes = b"x" * CHUNK
        return res

    client._call = AsyncMock(side_effect=blocking_call)

    current_target = 4

    def get_target():
        return current_target

    dl_task = asyncio.create_task(
        download_file_parallel(
            client,
            location=MagicMock(),
            out_file_path=out,
            total_bytes=total,
            concurrency=4,
            raw_media=_raw_media(),
            get_concurrency=get_target,
        )
    )

    # 等待至少 3 个 Worker 启动并进入网络阻塞
    await wait_until(lambda: len(worker_invocations) >= 3, message="Expected >= 3 download workers to start")

    # 动态缩容到 1
    current_target = 1

    # 等待 Supervisor TTL 刷新并取消超出目标并发 (4 -> 1) 的 Worker
    await wait_until(
        lambda: cancelled_workers >= 3,
        timeout_s=5.0,
        message="Expected Supervisor to cancel excess download workers after scale-down",
    )

    # 验证此时多余的 Worker 已被 cancel，放行剩余下载
    block_event.set()

    # 此时仅剩 1 个 Worker 继续拉取剩余分块，最终成功完成
    res = await asyncio.wait_for(dl_task, timeout=10.0)
    assert res == out
    assert os.path.exists(out)


@pytest.mark.asyncio
async def test_download_all_clients_failed_breaks_without_hang(tmp_dir, fake_senders, monkeypatch):
    """验证多 Client 模式下所有 Client 均触发 FloodWait 时，Supervisor 能够立即 break 退出并抛出 DownloadIncompleteError 而不发生死循环自旋"""
    from telethon.errors import FloodWaitError

    monkeypatch.setattr(settings, "DOWNLOAD_DIR", tmp_dir)
    out = os.path.join(tmp_dir, "all_flood.bin")

    c1 = AsyncMock()
    c1.is_connected = MagicMock(return_value=True)
    c1.session = MagicMock(dc_id=2)
    c2 = AsyncMock()
    c2.is_connected = MagicMock(return_value=True)
    c2.session = MagicMock(dc_id=2)

    c1._call = AsyncMock(side_effect=FloodWaitError(request=None, capture=10))
    c2._call = AsyncMock(side_effect=FloodWaitError(request=None, capture=10))

    with pytest.raises(fast_telethon.DownloadIncompleteError) as exc_info:
        await asyncio.wait_for(
            download_file_parallel(
                [c1, c2],
                location=MagicMock(),
                out_file_path=out,
                total_bytes=1024 * 1024,
                concurrency=2,
                raw_media=_raw_media(),
            ),
            timeout=5.0,
        )
    assert "所有 Client 均触发限速" in str(exc_info.value) or "FloodWait" in str(exc_info.value)


@pytest.mark.asyncio
async def test_dynamic_scale_down_cancels_excess_upload_workers(tmp_dir, monkeypatch):
    """验证上传动态缩容时超出目标并发的挂起 Worker Task 能被显式 cancel，未完成分块放回队列且最终完整上传"""
    from _safeio import safe_write_bytes

    test_file = safe_write_bytes(os.path.join(tmp_dir, "scaledown_up.bin"), b"u" * (6 * CHUNK))

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    block_event = asyncio.Event()
    worker_invocations = set()
    cancelled_workers = 0

    async def blocking_call(sender, request):
        nonlocal cancelled_workers
        worker_invocations.add(request.file_part)
        try:
            await block_event.wait()
        except asyncio.CancelledError:
            cancelled_workers += 1
            raise
        res = MagicMock()
        return res

    client._call = AsyncMock(side_effect=blocking_call)

    async def fake_gather(coros, clients, dc_id):
        for c in coros:
            c.close()
        return [MagicMock() for _ in coros]

    monkeypatch.setattr(fast_telethon.sender_pool, "_gather_worker_senders_with_cleanup", fake_gather)

    current_target = 4

    def get_target():
        return current_target

    up_task = asyncio.create_task(
        upload_file_parallel(client, str(test_file), concurrency=4, get_concurrency=get_target)
    )

    # 等待至少 3 个 Worker 启动并进入网络阻塞
    await wait_until(lambda: len(worker_invocations) >= 3, message="Expected >= 3 upload workers to start")

    # 动态缩容到 1
    current_target = 1

    # 等待 Supervisor TTL 刷新并取消超出目标并发 (4 -> 1) 的 Worker
    await wait_until(
        lambda: cancelled_workers >= 3,
        timeout_s=5.0,
        message="Expected Supervisor to cancel excess upload workers after scale-down",
    )

    # 放行网络请求
    block_event.set()

    res = await asyncio.wait_for(up_task, timeout=10.0)
    assert res is not None
    assert res.parts == 6


# ==============================================================================
# Part: .tmp_p2_noise.py
# ==============================================================================
def test_terminal_noise_suppress_filter():
    """[TASK-001] 验证 SuppressExpectedCloseFilter 正确抑制 0 bytes read 连接关闭底噪"""
    noise_filter = SuppressExpectedCloseFilter()

    # 1. Telethon 常见的静默关闭底噪日志 -> 应当被过滤抑制 (返回 False)
    record_noise = logging.LogRecord(
        name="telethon.network.mtprotosender",
        level=logging.INFO,
        pathname="mtprotosender.py",
        lineno=100,
        msg="Server closed the connection: 0 bytes read on a total of 8 expected bytes",
        args=(),
        exc_info=None,
    )
    assert noise_filter.filter(record_noise) is False

    # 2. 其他正规日志 -> 应当正常放行 (返回 True)
    record_normal = logging.LogRecord(
        name="telethon.network.mtprotosender",
        level=logging.INFO,
        pathname="mtprotosender.py",
        lineno=105,
        msg="Connecting to 149.154.167.51:443/TcpFull...",
        args=(),
        exc_info=None,
    )
    assert noise_filter.filter(record_normal) is True


def test_default_download_parallel_connections_is_8(monkeypatch):
    """需求 B: 验证 DOWNLOAD_PARALLEL_CONNECTIONS 默认值已由 16 收敛至 8"""
    monkeypatch.delenv("DOWNLOAD_PARALLEL_CONNECTIONS", raising=False)
    assert int(os.getenv("DOWNLOAD_PARALLEL_CONNECTIONS", "8")) == 8
    assert settings.DOWNLOAD_PARALLEL_CONNECTIONS == 8


@pytest.mark.asyncio
async def test_dynamic_in_flight_limiter_no_leak_across_downscale_upscale():
    """#19: 缩容后再扩容不得发生 Semaphore 许可泄漏导致实际在途突破上限。"""
    limiter = fast_telethon.primitives._DynamicInFlightLimiter(2)
    await limiter.acquire()
    await limiter.acquire()

    limiter.set_limit(1)
    pending = asyncio.create_task(limiter.acquire())
    await asyncio.sleep(0.02)
    assert not pending.done(), "当前 active=2 >= limit=1，不应放行新请求"

    limiter.release()  # active -> 1, 仍 >= limit=1
    await asyncio.sleep(0.02)
    assert not pending.done(), "active=1 >= limit=1，必须继续阻塞"

    limiter.release()  # active -> 0 < limit=1
    await asyncio.wait_for(pending, timeout=1.0)
    assert limiter._active == 1

    limiter.set_limit(2)
    await asyncio.wait_for(limiter.acquire(), timeout=1.0)
    assert limiter._active == 2


@pytest.mark.asyncio
async def test_upload_breakpoint_invalidated_on_local_content_replacement(tmp_dir, monkeypatch):
    """#22: 本地文件同路径同大小内容替换后，必须失效旧断点并从头重传所有分块。"""
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", os.path.join(tmp_dir, "meta_sandbox"))
    file_path = os.path.join(tmp_dir, "same_size_replace.bin")
    safe_write_bytes(file_path, b"A" * (2 * CHUNK))

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        sender = MagicMock()
        sender.disconnect = AsyncMock()
        sender._connection = None
        return [sender for _ in coros]

    monkeypatch.setattr(fast_telethon.sender_pool, "_gather_worker_senders_with_cleanup", fake_gather)

    cancelled = False
    first_pass_parts: list[int] = []

    async def first_call(sender, req):
        nonlocal cancelled
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            if req.file_part == 0:
                first_pass_parts.append(req.file_part)
                cancelled = True
                return True
            await asyncio.sleep(10.0)
        return cfg

    client._call = first_call
    with pytest.raises(UploadCancelledError):
        await upload_file_parallel(client, file_path, concurrency=1, is_cancelled=lambda: cancelled)
    assert first_pass_parts == [0]

    # 以完全相同的总字节数覆写为全新内容
    safe_write_bytes(file_path, b"B" * (2 * CHUNK))

    second_pass_parts: list[int] = []

    async def second_call(sender, req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            second_pass_parts.append(req.file_part)
            return True
        return cfg

    client._call = second_call
    res = await upload_file_parallel(client, file_path, concurrency=1)
    assert res is not None
    assert set(second_pass_parts) == {0, 1}, f"指纹变更后必须重传全部分块，实传: {second_pass_parts}"


@pytest.mark.asyncio
async def test_upload_part_rpc_returns_false_triggers_failure():
    """#15: SavePart/SaveBigFilePart 显式返回 False 时不能静默记为成功。"""
    client = AsyncMock()
    client._call = AsyncMock(return_value=False)
    req = SaveFilePartRequest(file_id=1, file_part=0, bytes=b"x")
    with pytest.raises(RuntimeError, match="False"):
        await fast_telethon.upload_engine._execute_upload_part_rpc(client, MagicMock(), req)

    client_ok = AsyncMock()
    client_ok._call = AsyncMock(return_value=True)
    await fast_telethon.upload_engine._execute_upload_part_rpc(client_ok, MagicMock(), req)


def test_download_meta_rejects_mismatched_chunk_size_and_unaligned_offsets(tmp_dir, monkeypatch):
    """#12: 下载 .meta 中 chunk_size 不匹配或存在越界/非对齐 offset 时必须忽略无效偏移。"""
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", tmp_dir)
    part_path = os.path.join(tmp_dir, "dl.bin.part")
    meta_path = os.path.join(tmp_dir, "dl.bin.meta")
    total_bytes = 2 * CHUNK
    safe_write_bytes(part_path, b"\x00" * total_bytes)
    media_identity = "InputDocumentFileLocation:123:"
    load = fast_telethon.breakpoints._prepare_download_breakpoint_sync
    base_meta = {"total_bytes": total_bytes, "media_identity": media_identity}

    # 1. chunk_size 不匹配 -> 全部丢弃
    safe_write_bytes(
        meta_path,
        json.dumps({**base_meta, "chunk_size": CHUNK * 2, "completed_offsets": [0]}).encode("utf-8"),
    )
    assert load(part_path, meta_path, total_bytes, CHUNK, media_identity=media_identity) == set()

    # 2. chunk_size 匹配但含非对齐/越界 offset -> 仅保留合法 offset
    safe_write_bytes(
        meta_path,
        json.dumps({**base_meta, "chunk_size": CHUNK, "completed_offsets": [0, 123, total_bytes, -CHUNK]}).encode(
            "utf-8"
        ),
    )
    assert load(part_path, meta_path, total_bytes, CHUNK, media_identity=media_identity) == {0}
    assert load(part_path, meta_path, total_bytes, CHUNK, media_identity="InputDocumentFileLocation:456:") == set()
    assert load(part_path, meta_path, total_bytes, CHUNK) == set()
    safe_write_bytes(
        meta_path,
        json.dumps({**base_meta, "chunk_size": CHUNK, "completed_offsets": [False, True]}).encode(),
    )
    assert load(part_path, meta_path, total_bytes, CHUNK, media_identity=media_identity) == set()
    base_meta.pop("media_identity")
    safe_write_bytes(meta_path, json.dumps({**base_meta, "chunk_size": CHUNK, "completed_offsets": [0]}).encode())
    assert load(part_path, meta_path, total_bytes, CHUNK, media_identity=media_identity) == set()


@pytest.mark.parametrize("part_exists_with_expected_size", [False, True])
def test_download_discards_old_metadata_before_writing_new_media(tmp_path, monkeypatch, part_exists_with_expected_size):
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", str(tmp_path))
    part = tmp_path / "media.part"
    meta = tmp_path / "media.meta"
    part.write_bytes(b"A1230000" if part_exists_with_expected_size else b"A123")
    meta.write_text(
        json.dumps({"media_identity": "A", "total_bytes": 8, "chunk_size": 4, "completed_offsets": [0]}),
        encoding="utf-8",
    )
    prepare = fast_telethon.breakpoints._prepare_download_breakpoint_sync

    restarted_identity = "B" if part_exists_with_expected_size else "A"
    assert prepare(str(part), str(meta), 8, 4, media_identity=restarted_identity) == set()
    assert not meta.exists()
    assert part.stat().st_size == 8
    # A write followed by interruption before the next metadata save leaves no old offsets to trust.
    with part.open("r+b") as partial:
        partial.write(b"B123")
    assert prepare(str(part), str(meta), 8, 4, media_identity="A") == set()


def test_download_checkpoint_paths_do_not_follow_leaf_symlinks(tmp_path):
    output = tmp_path / "media.bin"
    target = tmp_path / "other.bin"
    target.write_bytes(b"another file")
    meta = output.with_name("media.bin.meta")
    try:
        output.symlink_to(target)
        meta.symlink_to(target)
    except OSError as exc:
        pytest.skip(f"symlinks unavailable: {exc}")

    resolve = fast_telethon.breakpoints.resolve_breakpoint_path
    assert resolve(str(output), ".part", trust_root=str(tmp_path)) == str(output.with_name("media.bin.part").resolve())
    with pytest.raises(ValueError, match="符号链接"):
        resolve(str(output), ".meta", trust_root=str(tmp_path))


@pytest.mark.asyncio
async def test_upload_small_file_respects_cancel_and_pause(tmp_dir):
    """#21: <=512 KiB 小文件上传必须感知取消与暂停信号。"""
    small_file = safe_write_bytes(os.path.join(tmp_dir, "small_ctrl.bin"), b"z" * 1024)
    client = AsyncMock()
    client.session = MagicMock(dc_id=4)
    client.upload_file = AsyncMock()

    with pytest.raises(UploadCancelledError):
        await upload_file_parallel(client, small_file, is_cancelled=lambda: True)
    client.upload_file.assert_not_awaited()

    with pytest.raises(UploadPausedError):
        await upload_file_parallel(client, small_file, is_paused=lambda: True)
    client.upload_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_download_single_client_floodwait_sleeps_cooldown(tmp_dir, monkeypatch):
    """#9: 单 Client 下载遇到 FloodWait 时必须遵守冷却等待，而非 0.2s 自旋轰炸。"""
    from telethon.errors import FloodWaitError

    monkeypatch.setattr(settings, "DOWNLOAD_DIR", tmp_dir)
    out = os.path.join(tmp_dir, "single_flood.bin")
    slept: list[float] = []
    orig_sleep = asyncio.sleep
    fake_now = [1000.0]

    async def spy_sleep(delay, *args, **kwargs):
        slept.append(delay)
        fake_now[0] += max(0.0, delay)
        await orig_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", spy_sleep)
    monkeypatch.setattr(time, "time", lambda: fake_now[0])

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=2)
    attempts = 0

    async def mock_call(sender, req):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise FloodWaitError(request=None, capture=2)
        res = MagicMock()
        res.bytes = b"D" * CHUNK
        return res

    client._call = mock_call

    sender = MagicMock()
    sender.disconnect = AsyncMock()
    sender._connection = None

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [sender for _ in coros]

    monkeypatch.setattr(fast_telethon.sender_pool, "_gather_worker_senders_with_cleanup", fake_gather)
    monkeypatch.setattr(fast_telethon.download_engine, "_init_download_worker_sender", AsyncMock(return_value=sender))

    res_path = await download_file_parallel(
        client,
        location=MagicMock(),
        out_file_path=out,
        total_bytes=2 * CHUNK,
        concurrency=1,
        raw_media=_raw_media(),
    )
    assert res_path == out
    assert await asyncio.to_thread(os.path.exists, res_path)
    assert any(d >= 0.4 for d in slept), f"单账号 FloodWait 必须实际等待冷却，实际 sleep={slept}"


@pytest.mark.asyncio
async def test_upload_meta_saver_flush_waits_for_inflight_worker_at_100pct(tmp_dir, monkeypatch):
    """#13: 即使完成度已达 100%，_UploadMetaSaver.flush 仍须等待在途后台写盘结束再返回。"""
    meta_path = os.path.join(tmp_dir, "race.upload.meta")
    started = asyncio.Event()
    release_write = asyncio.Event()

    def slow_write(path, data, sync=False, **kwargs):
        started.set()
        while not release_write.is_set():
            time.sleep(0.01)

    monkeypatch.setattr(fast_telethon.breakpoints, "_write_meta_disk_sync", slow_write)

    saver = fast_telethon.breakpoints._UploadMetaSaver(meta_path, 2 * CHUNK, 99, {0}, 2)
    saver.trigger()
    try:
        await asyncio.wait_for(started.wait(), timeout=2.0)

        # 模拟最后一个分块已完成 (100%)
        saver.completed_parts.add(1)
        flush_task = asyncio.create_task(saver.flush())
        await asyncio.sleep(0.03)
        assert not flush_task.done(), "在途 save_task 尚未结束前 flush 不能提前返回"

    finally:
        release_write.set()
    await asyncio.wait_for(flush_task, timeout=2.0)


def test_upload_fingerprint_detects_middle_change_with_preserved_mtime(tmp_path):
    path = tmp_path / "source.bin"
    path.write_bytes(b"a" * (256 * 1024))
    before = path.stat()
    fingerprint = fast_telethon.breakpoints._compute_local_upload_fingerprint_sync(str(path), before.st_size)
    with path.open("r+b") as stream:
        stream.seek(128 * 1024)
        stream.write(b"b")
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert fast_telethon.breakpoints._compute_local_upload_fingerprint_sync(str(path), before.st_size) != fingerprint


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [fast_telethon.FloodWaitError(request=None, capture=61), RuntimeError("connection reset")]
)
async def test_download_error_cancellation_settles_offset_once(monkeypatch, error):
    client = MagicMock()
    chunks_queue: asyncio.Queue[int] = asyncio.Queue()
    chunks_queue.put_nowait(0)
    chunks_queue.get_nowait()
    state = fast_telethon.download_engine._DownloadSessionState(
        clients=[client],
        dc_id=1,
        input_location=None,
        client_locations=None,
        refetch_location=None,
        total_bytes=CHUNK,
        chunk_size=CHUNK,
        num_chunks=1,
        chunks_queue=chunks_queue,
        completed_offsets=set(),
        already_downloaded_bytes=0,
        file_descriptor=MagicMock(),
        meta_file_path="",
        get_target_concurrency=lambda: 1,
        progress_callback=None,
        is_cancelled=None,
        is_paused=None,
    )
    state._fetch_chunk_with_cancellation_guard = AsyncMock(side_effect=error)
    sleeping = asyncio.Event()

    async def blocked_sleep(delay):
        sleeping.set()
        await asyncio.Future()

    monkeypatch.setattr(asyncio, "sleep", blocked_sleep)
    task = asyncio.create_task(state._run_download_chunk_attempt(0, client, None, MagicMock(), 0))
    monkeypatch.setattr(fast_telethon.sender_pool, "_close_worker_sender", AsyncMock())
    try:
        await asyncio.wait_for(sleeping.wait(), timeout=2)
        if isinstance(error, fast_telethon.FloodWaitError):
            assert state.flood_wait_until >= time.time() + 60
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert state.chunks_queue.qsize() == 1
    assert state.chunks_queue.get_nowait() == 0
    state.chunks_queue.task_done()
    await asyncio.wait_for(state.chunks_queue.join(), timeout=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("preexisting_sender", [False, True])
@pytest.mark.parametrize("cancelled", [False, True])
async def test_download_attempt_write_failure_preserves_sender_ownership(monkeypatch, preexisting_sender, cancelled):
    client, sender = MagicMock(), MagicMock()
    chunks_queue: asyncio.Queue[int] = asyncio.Queue()
    chunks_queue.put_nowait(0)
    chunks_queue.get_nowait()
    state = fast_telethon.download_engine._DownloadSessionState(
        clients=[client],
        dc_id=1,
        input_location=None,
        client_locations=None,
        refetch_location=None,
        total_bytes=CHUNK,
        chunk_size=CHUNK,
        num_chunks=1,
        chunks_queue=chunks_queue,
        completed_offsets=set(),
        already_downloaded_bytes=0,
        file_descriptor=MagicMock(),
        meta_file_path="",
        get_target_concurrency=lambda: 1,
        progress_callback=None,
        is_cancelled=None,
        is_paused=None,
    )
    state.init_sender = AsyncMock(return_value=sender)
    state._fetch_chunk_with_cancellation_guard = AsyncMock(return_value=b"x" * CHUNK)
    error = asyncio.CancelledError if cancelled else OSError
    state._write_downloaded_chunk = AsyncMock(side_effect=error("write interrupted"))
    close_sender = AsyncMock()
    monkeypatch.setattr(fast_telethon.sender_pool, "_close_worker_sender", close_sender)

    with pytest.raises(error, match="write interrupted"):
        await state._run_download_chunk_attempt(0, client, None, sender if preexisting_sender else None, 0)

    if preexisting_sender:
        state.init_sender.assert_not_awaited()
        close_sender.assert_not_awaited()
    else:
        state.init_sender.assert_awaited_once_with(0, client)
        close_sender.assert_awaited_once_with(client, sender, state.dc_id)
    assert state.chunks_queue.qsize() == 1
    assert state.chunks_queue.get_nowait() == 0
    state.chunks_queue.task_done()
    await asyncio.wait_for(state.chunks_queue.join(), timeout=1)


@pytest.mark.asyncio
async def test_download_single_client_floodwait_above_300s_fails_fast(tmp_dir, monkeypatch):
    """单账号 FloodWait 超过 300s 阈值时应快速失败，既不长时间挂起也不死循环重拉 worker。"""
    from telethon.errors import FloodWaitError

    monkeypatch.setattr(settings, "DOWNLOAD_DIR", tmp_dir)
    out = os.path.join(tmp_dir, "single_long_flood.bin")
    slept: list[float] = []
    orig_sleep = asyncio.sleep

    async def spy_sleep(delay, *args, **kwargs):
        slept.append(delay)
        await orig_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", spy_sleep)

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=2)
    attempts = 0

    async def mock_call(sender, req):
        nonlocal attempts
        attempts += 1
        raise FloodWaitError(request=None, capture=301)

    client._call = mock_call

    sender = MagicMock()
    sender.disconnect = AsyncMock()
    sender._connection = None

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [sender for _ in coros]

    monkeypatch.setattr(fast_telethon.sender_pool, "_gather_worker_senders_with_cleanup", fake_gather)
    monkeypatch.setattr(fast_telethon.download_engine, "_init_download_worker_sender", AsyncMock(return_value=sender))

    with pytest.raises(fast_telethon.DownloadIncompleteError) as exc_info:
        await asyncio.wait_for(
            download_file_parallel(
                client,
                location=MagicMock(),
                out_file_path=out,
                total_bytes=2 * CHUNK,
                concurrency=1,
                raw_media=_raw_media(),
            ),
            timeout=2.0,
        )

    assert attempts == 1, f"超过 300s 阈值应立即失败中止，实际 RPC 调用次数={attempts}"
    assert "300s" in str(exc_info.value) or any("300s" in msg for msg in exc_info.value.chunk_errors.values())
    assert all(d == 0.8 or d < 0.1 for d in slept), f"超过 300s 阈值不应进入 FloodWait 冷却循环 (0.5s): {slept}"


@pytest.mark.asyncio
async def test_download_write_oserror_aborts_without_infinite_retry(tmp_dir, monkeypatch):
    """本地落盘抛出永久性 OSError 时应立即终止下载并向上抛出异常，避免无限重入死循环。"""
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", tmp_dir)
    out = os.path.join(tmp_dir, "disk_full.bin")

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=2)
    attempts = 0

    async def mock_call(sender, req):
        nonlocal attempts
        attempts += 1
        res = MagicMock()
        res.bytes = b"D" * CHUNK
        return res

    client._call = mock_call

    sender = MagicMock()
    sender.disconnect = AsyncMock()
    sender._connection = None

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [sender for _ in coros]

    monkeypatch.setattr(fast_telethon.sender_pool, "_gather_worker_senders_with_cleanup", fake_gather)
    monkeypatch.setattr(fast_telethon.download_engine, "_init_download_worker_sender", AsyncMock(return_value=sender))

    async def fail_write(self, offset, chunk_data):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(fast_telethon.download_engine._DownloadSessionState, "_write_downloaded_chunk", fail_write)

    with pytest.raises(OSError, match="No space left on device"):
        await asyncio.wait_for(
            download_file_parallel(
                client,
                location=MagicMock(),
                out_file_path=out,
                total_bytes=2 * CHUNK,
                concurrency=1,
                raw_media=_raw_media(),
            ),
            timeout=2.0,
        )

    assert attempts == 1, f"写入磁盘 OSError 不应触发无限重试，实际 RPC 调用次数={attempts}"


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["failure", "success", "cancelled"])
async def test_download_reaps_completed_workers_once_and_refills_only_with_work(outcome):
    state = fast_telethon.download_engine._DownloadSessionState(
        clients=[MagicMock()],
        dc_id=1,
        input_location=None,
        client_locations=None,
        refetch_location=None,
        total_bytes=CHUNK,
        chunk_size=CHUNK,
        num_chunks=1,
        chunks_queue=asyncio.Queue(),
        completed_offsets=set(),
        already_downloaded_bytes=0,
        file_descriptor=MagicMock(),
        meta_file_path="",
        get_target_concurrency=lambda: 1,
        progress_callback=None,
        is_cancelled=None,
        is_paused=None,
    )
    blocked = asyncio.Event()

    async def run_worker(worker_id):
        await blocked.wait()

    state.run_worker = AsyncMock(side_effect=run_worker)

    async def finish_worker():
        if outcome == "failure":
            raise OSError("worker failed")
        if outcome == "cancelled":
            raise asyncio.CancelledError

    finished = asyncio.create_task(finish_worker())
    await asyncio.gather(finished, return_exceptions=True)
    running = asyncio.create_task(run_worker(1))
    tasks = {0: finished, 1: running}
    try:
        for _ in range(6):
            state._reap_and_scale_workers(tasks, 2)
        assert tasks == {1: running}
        assert state.worker_error_count == (1 if outcome == "failure" else 0)
        assert state.fatal_error is None
        state.run_worker.assert_not_called()

        state.chunks_queue.put_nowait(0)
        state._reap_and_scale_workers(tasks, 2)
        assert tasks[0] is not finished
        assert not tasks[0].done()
        assert tasks[1] is running
        state.run_worker.assert_called_once_with(0)
    finally:
        for task in tasks.values():
            task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)


def test_validated_breakpoint_rejects_symlink_even_without_trust_root(tmp_path):
    """#9: 未传 trust_root 时 _validated_breakpoint 也须拦截指向外部目录的符号链接."""
    outside = tmp_path / "outside"
    sandbox = tmp_path / "sandbox"
    outside.mkdir()
    sandbox.mkdir()
    target_file = outside / "evil.meta"
    target_file.write_text("{}")
    link_file = sandbox / "redirect.upload.meta"
    try:
        link_file.symlink_to(target_file)
    except (OSError, NotImplementedError):
        pytest.skip("当前 Windows 权限不支持创建符号链接")

    with pytest.raises(ValueError, match="越界"):
        fast_telethon.breakpoints._validated_breakpoint(str(link_file))


@pytest.mark.asyncio
async def test_download_meta_saver_fsyncs_part_before_meta_and_release_flushes_first(tmp_path, monkeypatch):
    """#11 & #12: 写入 .meta 前须 flush+fsync .part, 且 release_resources 须先 flush 元数据再关 fd."""
    part_path = tmp_path / "video.part"
    meta_path = tmp_path / "video.meta"
    part_path.write_bytes(b"\x00" * CHUNK)
    fd = await asyncio.to_thread(fast_telethon.breakpoints._open_binary_file_sync, str(part_path), "rb+")

    events: list[str] = []
    orig_fsync = os.fsync

    def spy_fsync(fno):
        if fno == fd.fileno():
            events.append("part_fsync")
        return orig_fsync(fno)

    def spy_write_meta(target_path, meta_dict):
        assert not fd.closed, "写入 .meta 时 .part 句柄必须仍处于打开并已落盘状态"
        events.append("meta_write")

    monkeypatch.setattr(os, "fsync", spy_fsync)
    monkeypatch.setattr(fast_telethon.breakpoints, "_write_download_meta_disk_sync", spy_write_meta)

    state = fast_telethon.download_engine._DownloadSessionState(
        clients=[MagicMock()],
        dc_id=1,
        input_location=None,
        client_locations=None,
        refetch_location=None,
        total_bytes=CHUNK,
        chunk_size=CHUNK,
        num_chunks=1,
        chunks_queue=asyncio.Queue(),
        completed_offsets=set(),
        already_downloaded_bytes=0,
        file_descriptor=fd,
        meta_file_path=str(meta_path),
        get_target_concurrency=lambda: 1,
        progress_callback=None,
        is_cancelled=None,
        is_paused=None,
    )
    try:
        await state._write_downloaded_chunk(0, b"z" * CHUNK)
        await state.release_resources()
        assert fd.closed is True
    finally:
        if not fd.closed:
            fd.close()

    assert events.index("part_fsync") < events.index("meta_write")


@pytest.mark.asyncio
async def test_init_worker_sender_times_out_on_hung_init_connection(monkeypatch):
    """#14: _create_worker_sender_with_auth 在 InitConnection 挂死时须超时并断开清理."""
    client = MagicMock()
    client.api_id = 1
    client._log = None
    client._get_dc = AsyncMock(return_value=MagicMock())
    monkeypatch.setattr(fast_telethon.sender_pool, "_build_client_connection", lambda c, dc, dcid: MagicMock())

    hung_sender = MagicMock()
    hung_sender.connect = AsyncMock()
    hung_sender.send = MagicMock(return_value=asyncio.Future())
    hung_sender.disconnect = AsyncMock()
    monkeypatch.setattr(fast_telethon.sender_pool, "MTProtoSender", lambda *a, **kw: hung_sender)

    orig_wait_for = asyncio.wait_for

    async def fast_wait_for(aw, *args, **kwargs):
        return await orig_wait_for(aw, timeout=0.05)

    monkeypatch.setattr(fast_telethon.sender_pool.asyncio, "wait_for", fast_wait_for)
    with pytest.raises(TimeoutError):
        await fast_telethon.sender_pool._create_worker_sender_with_auth(client, 2, 123, b"auth")
    hung_sender.disconnect.assert_awaited()


def test_upload_signal_prioritizes_cancellation_over_pause():
    """取消信号优先级必须高于暂停信号 (含小文件与大文件收尾路径)."""
    from app.core.scheduler import UploadCancelledError

    with pytest.raises(UploadCancelledError):
        fast_telethon.upload_engine._raise_if_upload_signal(lambda: True, lambda: True)

    session = MagicMock()
    session.fatal_error = None
    session.is_cancelled = lambda: True
    session.is_paused = lambda: True
    session.completed_parts = set()
    session.num_chunks = 2
    with pytest.raises(UploadCancelledError):
        fast_telethon.upload_engine._finalize_upload_or_raise(session)


@pytest.mark.asyncio
async def test_write_downloaded_chunk_waits_for_write_thread_before_unlock_on_cancel(monkeypatch):
    """写盘线程进行中若协程被取消，须等待线程写完再释放 self.lock，防止共享 fd 错位."""
    import threading

    write_started = threading.Event()
    release_write = threading.Event()
    write_finished = threading.Event()
    fd = MagicMock()

    def slow_write_at(offset, data):
        write_started.set()
        release_write.wait(timeout=2.0)
        write_finished.set()

    state = fast_telethon.download_engine._DownloadSessionState(
        clients=[MagicMock()],
        dc_id=1,
        input_location=None,
        client_locations=None,
        refetch_location=None,
        total_bytes=CHUNK,
        chunk_size=CHUNK,
        num_chunks=1,
        chunks_queue=asyncio.Queue(),
        completed_offsets=set(),
        already_downloaded_bytes=0,
        file_descriptor=fd,
        meta_file_path="",
        get_target_concurrency=lambda: 1,
        progress_callback=None,
        is_cancelled=None,
        is_paused=None,
    )
    monkeypatch.setattr(state, "_write_at", slow_write_at)

    task = asyncio.create_task(state._write_downloaded_chunk(0, b"a" * CHUNK))
    try:
        await asyncio.to_thread(write_started.wait, 2.0)
        task.cancel()
        await asyncio.sleep(0.03)
        assert not task.done()
        assert state.lock.locked()
    finally:
        release_write.set()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert write_finished.is_set()
    assert not state.lock.locked()


@pytest.mark.asyncio
async def test_release_resources_closes_fd_when_sender_close_cancelled(monkeypatch):
    """关闭 sender 期间若遭 CancelledError，release_resources 仍须关闭 .part fd."""
    fd = MagicMock(closed=False)
    state = fast_telethon.download_engine._DownloadSessionState(
        clients=[MagicMock()],
        dc_id=1,
        input_location=None,
        client_locations=None,
        refetch_location=None,
        total_bytes=CHUNK,
        chunk_size=CHUNK,
        num_chunks=1,
        chunks_queue=asyncio.Queue(),
        completed_offsets=set(),
        already_downloaded_bytes=0,
        file_descriptor=fd,
        meta_file_path="",
        get_target_concurrency=lambda: 1,
        progress_callback=None,
        is_cancelled=None,
        is_paused=None,
    )
    state.worker_senders[0] = MagicMock()
    monkeypatch.setattr(
        fast_telethon.sender_pool,
        "_close_worker_sender",
        AsyncMock(side_effect=asyncio.CancelledError),
    )

    with pytest.raises(asyncio.CancelledError):
        await state.release_resources()
    fd.close.assert_called_once()


@pytest.mark.asyncio
async def test_init_worker_sender_bounds_export_auth_refresh_timeout(monkeypatch):
    """ExportAuthorizationRequest 重试路径须受超时约束并可安全降级."""
    loop = asyncio.get_running_loop()
    first_auth = loop.create_future()
    first_auth.set_result(MagicMock(id=1, bytes=b"a"))

    client = MagicMock()
    client.session = MagicMock(dc_id=1, auth_key=b"k")
    client.side_effect = [first_auth, loop.create_future()]

    monkeypatch.setattr(
        fast_telethon.sender_pool,
        "_create_worker_sender_with_auth",
        AsyncMock(side_effect=TimeoutError("init timed out")),
    )
    fallback_sender = MagicMock()
    get_fallback = AsyncMock(return_value=fallback_sender)
    monkeypatch.setattr(fast_telethon.sender_pool, "_get_worker_sender", get_fallback)

    orig_wait_for = asyncio.wait_for
    recorded_timeouts: list[float] = []

    async def fast_wait_for(aw, *args, **kwargs):
        if "timeout" in kwargs:
            recorded_timeouts.append(kwargs["timeout"])
        elif args:
            recorded_timeouts.append(args[0])
        return await orig_wait_for(aw, timeout=0.05)

    monkeypatch.setattr(fast_telethon.sender_pool.asyncio, "wait_for", fast_wait_for)
    res = await fast_telethon.sender_pool._init_worker_sender(0, [client], 2, [], {})
    assert res is fallback_sender
    assert client.call_count == 2
    assert recorded_timeouts == [75.0, 15.0]
    get_fallback.assert_awaited_once_with(client, 2)


@pytest.fixture
def download_state(tmp_path):
    return de._DownloadSessionState(
        clients=[MagicMock()],
        dc_id=2,
        input_location=InputDocumentFileLocation(1, 2, b"old", ""),
        client_locations={},
        refetch_location=None,
        total_bytes=8,
        chunk_size=4,
        num_chunks=2,
        chunks_queue=asyncio.Queue(),
        completed_offsets=set(),
        already_downloaded_bytes=0,
        file_descriptor=io.BytesIO(),
        meta_file_path=str(tmp_path / "download.meta"),
        get_target_concurrency=lambda: 1,
        progress_callback=None,
        is_cancelled=None,
        is_paused=None,
    )


@pytest.mark.parametrize("with_media", [False, True])
async def test_small_download_uses_supported_api(tmp_path, with_media):
    destination = tmp_path / "small.bin"
    destination.write_bytes(b"data")
    client = SimpleNamespace(download_media=AsyncMock(), download_file=AsyncMock())
    media = object() if with_media else None
    location = InputDocumentFileLocation(1, 2, b"ref", "")
    used = client.download_media if with_media else client.download_file
    unused = client.download_file if with_media else client.download_media

    async def transfer(_source, *, file):
        assert destination.read_bytes() == b"data"
        file.write(b"replacement")

    used.side_effect = transfer
    assert await de._download_small_file_direct(client, media, location, str(destination), 2, None, None) == str(
        destination
    )
    used.assert_awaited_once()
    assert used.await_args.args == (media if with_media else location,)
    unused.assert_not_awaited()
    assert destination.read_bytes() == b"replacement"
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.parametrize("interruption", ["error", "cancel", "pause"])
async def test_small_download_interruption_preserves_existing_file(tmp_path, interruption):
    destination = tmp_path / "small.bin"
    destination.write_bytes(b"existing")
    stopped = False

    async def transfer(_location, *, file):
        nonlocal stopped
        file.write(b"incomplete")
        stopped = True
        if interruption == "error":
            raise OSError("transfer interrupted")

    client = SimpleNamespace(download_file=transfer)
    location = InputDocumentFileLocation(1, 2, b"ref", "")
    args = (client, None, location, str(destination), 2, None)
    if interruption == "error":
        with pytest.raises(OSError, match="transfer interrupted"):
            await de._download_small_file_direct(*args, None)
    else:
        await de._download_small_file_direct(
            *args, lambda: stopped and interruption == "cancel", lambda: stopped and interruption == "pause"
        )
    assert destination.read_bytes() == b"existing"
    assert list(tmp_path.iterdir()) == [destination]


def test_parallel_download_replace_failure_preserves_existing_file(tmp_path, monkeypatch):
    destination = tmp_path / "download.bin"
    part = tmp_path / "download.bin.part"
    meta = tmp_path / "download.bin.meta"
    destination.write_bytes(b"existing")
    part.write_bytes(b"replacement")
    meta.write_text("{}", encoding="utf-8")

    def fail_replace(source, target):
        assert (source, target) == (str(part), str(destination))
        raise PermissionError("destination is busy")

    monkeypatch.setattr(de.breakpoints.os, "replace", fail_replace)
    with pytest.raises(PermissionError, match="destination is busy"):
        de.breakpoints._finalize_download_files_sync(str(destination), str(part), str(meta))
    assert destination.read_bytes() == b"existing"
    assert part.read_bytes() == b"replacement"
    assert meta.exists()


@pytest.mark.parametrize("document_id", [1, 3])
async def test_refresh_preserves_document_identity(download_state, document_id):
    state = download_state
    client = state.clients[0]
    refreshed = InputDocumentFileLocation(document_id, 2, b"new", "")
    state.refetch_location = AsyncMock(return_value=(2, refreshed))
    result = await state._refresh_worker_location(client)
    if document_id == 1:
        assert result is refreshed
        assert state.client_locations[client] is refreshed
        assert state.fatal_error is None
    else:
        assert result is None
        assert client not in state.client_locations
        assert isinstance(state.fatal_error, RuntimeError)


async def test_short_response_retries_without_writing(download_state, monkeypatch):
    state = download_state
    state._fetch_chunk_with_cancellation_guard = AsyncMock(return_value=b"xx")
    state.init_sender = AsyncMock(return_value=None)
    monkeypatch.setattr(de.asyncio, "sleep", AsyncMock())
    state.chunks_queue.put_nowait(0)
    state.chunks_queue.get_nowait()
    await state._run_download_chunk_attempt(0, state.clients[0], state.input_location, None, 0)
    assert not state.completed_offsets
    assert state.file_descriptor.getvalue() == b""
    assert state.chunk_failed_counts[0] == 1
    assert state.chunks_queue.get_nowait() == 0
    state.chunks_queue.task_done()
    await asyncio.wait_for(state.chunks_queue.join(), 1)


async def test_cancel_during_worker_shutdown_releases_resources(download_state):
    state = download_state
    state.should_abort = lambda: True
    shutting_down = asyncio.Event()

    async def worker(_):
        try:
            await asyncio.Future()
        finally:
            shutting_down.set()
            await asyncio.Future()

    state.run_worker = worker
    state.release_resources = AsyncMock()
    state.should_abort = lambda: False
    supervisor = asyncio.create_task(state.supervise_workers(1))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    supervisor.cancel()
    await asyncio.wait_for(shutting_down.wait(), 1)
    supervisor.cancel()
    with pytest.raises(asyncio.CancelledError):
        await supervisor
    state.release_resources.assert_awaited_once()


async def test_shared_borrow_returns_each_reference_without_disconnect():
    sender = SimpleNamespace(disconnect=AsyncMock())
    client = SimpleNamespace(
        session=SimpleNamespace(dc_id=1),
        _borrowed_senders={2: (object(), sender)},
        _borrow_exported_sender=AsyncMock(return_value=sender),
        _return_exported_sender=AsyncMock(),
    )
    first = await sp._get_worker_sender(client, 2)
    second = await sp._get_worker_sender(client, 2)
    await sp._close_worker_sender(client, first, 2)
    assert client._borrowed_senders[2][1] is sender
    sender.disconnect.assert_not_awaited()
    await sp._close_worker_sender(client, second, 2)
    assert client._return_exported_sender.await_count == 2
    owned = SimpleNamespace(disconnect=AsyncMock())
    await sp._close_worker_sender(client, owned, 2)
    owned.disconnect.assert_awaited_once()


@pytest.mark.parametrize("prepared", [False, True])
@pytest.mark.parametrize("borrowed", [False, True])
async def test_cancel_worker_releases_sender_once(download_state, monkeypatch, prepared, borrowed):
    state = download_state
    sender = SimpleNamespace(disconnect=AsyncMock())
    client = MagicMock(
        _borrowed_senders={2: (object(), sender)} if borrowed else {},
        _return_exported_sender=AsyncMock(),
    )
    state.clients = [client]
    state.init_sender = AsyncMock(return_value=sender)
    if prepared:
        state.worker_senders[0] = sender
    state.chunks_queue.put_nowait(0)
    rpc_started = asyncio.Event()

    async def blocked_rpc(*_):
        rpc_started.set()
        await asyncio.Future()

    monkeypatch.setattr(de, "_execute_download_chunk_rpc", blocked_rpc)
    worker = asyncio.create_task(state.run_worker(0))
    try:
        await asyncio.wait_for(rpc_started.wait(), 1)
    finally:
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker
    if borrowed:
        client._return_exported_sender.assert_awaited_once_with(sender)
        sender.disconnect.assert_not_awaited()
        assert client._borrowed_senders[2][1] is sender
    else:
        sender.disconnect.assert_awaited_once()
    assert state.chunks_queue.get_nowait() == 0
    state.chunks_queue.task_done()
    await asyncio.wait_for(state.chunks_queue.join(), 1)
    await state.release_resources()
    assert client._return_exported_sender.await_count == int(borrowed)
    assert sender.disconnect.await_count == int(not borrowed)


async def test_overlapping_upload_attempts_keep_independent_tracking():
    state = object.__new__(ue._UploadSessionState)
    state.active_in_flight_count = 2
    gates = [asyncio.Event(), asyncio.Event()]
    started = asyncio.Queue()

    async def upload(*_):
        index = started.qsize()
        started.put_nowait(index)
        await gates[index].wait()

    state._upload_part = upload
    tracked = {}
    sem = MagicMock()
    tasks = []
    for _ in range(2):
        task = asyncio.create_task(state._run_part_task(0, None, None, 7, b"data", 0, {}, tracked, sem))
        tracked[id(task)] = task
        tasks.append(task)
    await asyncio.sleep(0)
    assert started.qsize() == 2
    gates[0].set()
    await tasks[0]
    assert list(tracked.values()) == [tasks[1]]
    gates[1].set()
    await tasks[1]
    assert not tracked
    assert state.active_in_flight_count == 0


async def test_download_incomplete_preserves_failing_client_and_clears_recovered_chunk(tmp_path, monkeypatch):
    chunk = settings.CHUNK_SIZE
    total = 2 * chunk
    out = tmp_path / "multi_client_fail.bin"
    orig_sleep = asyncio.sleep
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", str(tmp_path))
    monkeypatch.setattr(de._DownloadSessionState, "_start_heartbeat", lambda self: None)
    monkeypatch.setattr(de.asyncio, "sleep", lambda *_: orig_sleep(0))
    monkeypatch.setattr(de.sender_pool, "_prepare_download_auth_and_dc", AsyncMock(return_value=({}, [])))

    async def _close_coros_and_return(coros, *_args):
        for coro in coros:
            coro.close()
        return [None, None]

    monkeypatch.setattr(de.sender_pool, "_gather_worker_senders_with_cleanup", _close_coros_and_return)
    monkeypatch.setattr(de, "_init_download_worker_sender", AsyncMock(return_value=object()))
    monkeypatch.setattr(de.sender_pool, "_close_worker_sender", AsyncMock())

    primary_client = MagicMock(session=SimpleNamespace(dc_id=2))
    aux_client = MagicMock(session=SimpleNamespace(dc_id=2))
    primary_attempts = 0
    aux_attempts = 0
    aux_exhausted = asyncio.Event()
    aux_err = AuthKeyUnregisteredError(request=None)

    async def fake_rpc(worker_id, worker_client, sender, worker_location, offset, chunk_size, dc_id):
        nonlocal primary_attempts, aux_attempts
        if worker_client is aux_client:
            aux_attempts += 1
            if aux_attempts >= 5:
                aux_exhausted.set()
            raise aux_err
        primary_attempts += 1
        if primary_attempts == 1:
            await asyncio.wait_for(aux_exhausted.wait(), timeout=2.0)
            await orig_sleep(0)
            await orig_sleep(0)
            raise TimeoutError("transient timeout")
        return b"x" * chunk

    monkeypatch.setattr(de, "_execute_download_chunk_rpc", fake_rpc)

    with pytest.raises(de.DownloadIncompleteError) as ei:
        await de.download_file_parallel(
            [primary_client, aux_client],
            location=InputDocumentFileLocation(1, 2, b"ref", ""),
            out_file_path=str(out),
            total_bytes=total,
            concurrency=2,
        )

    err = ei.value
    assert err.missing_offsets == [chunk]
    assert 0 not in err.last_clients
    assert 0 not in err.last_errors
    assert 0 not in err.last_exceptions
    assert err.last_clients == {chunk: aux_client}
    assert err.last_exceptions[chunk] is aux_err
    assert "AuthKeyUnregisteredError" in err.last_errors[chunk]
    assert is_session_invalid_error(err) is True
