"""Pipeline Core: 边下边传预取队列、背压流控与 L1 环形缓冲/L2 容灾测试 (PikPak Prefetch Pipeline & Ring Buffer)

覆盖范围:
- 原 test_pikpak_prefetch_pipeline: 并发预取保序、背压限制、微重试、直接连接与降级链
- 原 test_upload_ring_buffer: L1 环形缓冲区预算淘汰、高并发防爆内存、L2 回退容灾与取消资源释放"""

import asyncio
import os
import shutil
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from _safeio import safe_write_bytes
from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest

from app.config import settings
from app.core.fast_telethon import (
    AdaptivePrefetchQueue,
    upload_file_parallel,
)
from app.core.scheduler import UploadCancelledError
from app.core.stream_chunk_provider import (
    BaseChunkProvider,
    ChunkFetchTransientError,
    LocalFileChunkProvider,
    PikPakStreamChunkProvider,
    SlidingWindowRingBuffer,
)
from app.db.account_repository import SystemSettingsRepository
from app.services.pikpak_service import _run_cli

# ==============================================================================
# Part: test_pikpak_prefetch_pipeline.py
# ==============================================================================
CHUNK_SIZE = 512 * 1024


class MockStreamProvider(BaseChunkProvider):
    """用于测试的模拟流分块提供器"""

    def __init__(self, total_bytes: int, file_id: str = "mock-file-123", file_name: str = "mock_stream.mp4"):
        self.total_bytes = total_bytes
        self.file_id = file_id
        self.file_name = file_name
        self.delays: dict[int, float] = {}
        self.errors: dict[int, list[Exception]] = {}
        self.read_history: list[int] = []

    async def read_chunk(self, offset: int, size: int) -> bytes:
        part_id = offset // CHUNK_SIZE
        self.read_history.append(part_id)

        # 模拟瞬态错误注入
        if self.errors.get(part_id):
            err = self.errors[part_id].pop(0)
            raise err

        # 模拟不同分块的网络往返耗时（乱序到达）
        delay = self.delays.get(part_id, 0.0)
        if delay > 0:
            await asyncio.sleep(delay)

        chunk_data = b"X" * size
        return chunk_data

    async def aclose(self) -> None:
        pass


def _setup_mock_client():
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg
    return client


@pytest.mark.asyncio
@pytest.mark.usefixtures("no_download_file_writes")
async def test_concurrent_prefetch_reorder_in_order(tmp_path, monkeypatch):
    """1. 乱序到达下保序提交测试:
    设置 prefetch_concurrency=4，故意给 part 0 设置大延迟，part 1,2,3 先完成；
    验证 prefetch_queue 必须严格按 0, 1, 2, 3 ... 顺序提交。
    """
    total_parts = 10
    total_bytes = total_parts * CHUNK_SIZE
    provider = MockStreamProvider(total_bytes)

    # part 0 慢返回，part 1, 2, 3 快返回
    provider.delays[0] = 0.06
    provider.delays[1] = 0.01
    provider.delays[2] = 0.005
    provider.delays[3] = 0.02

    committed_parts = []
    orig_put = AdaptivePrefetchQueue.put

    async def tracking_put(self, item):
        part_id, chunk_data, read_ms = item
        committed_parts.append(part_id)
        return await orig_put(self, item)

    monkeypatch.setattr(AdaptivePrefetchQueue, "put", tracking_put)

    client = _setup_mock_client()
    received_parts = []

    async def mock_call(sender, req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            await asyncio.sleep(0.005)
            received_parts.append(req.file_part)
            return True
        return MagicMock()

    client._call = mock_call

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [MagicMock() for _ in coros]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    meta_file = tmp_path / "stream_reorder.meta"
    # 注入允许的 meta_dir
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", str(tmp_path))

    res = await upload_file_parallel(
        client,
        "pikpak://mock-file-123",
        chunk_provider=provider,
        meta_path=str(meta_file),
        concurrency=2,
        in_flight_per_connection=1,
        prefetch_concurrency=4,
    )

    assert res.parts == total_parts
    # 核心断言: 即使网络层乱序返回，写入 prefetch_queue 的分块 ID 必须严格升序
    assert committed_parts == list(range(total_parts)), f"分块未保序提交: {committed_parts}"
    assert sorted(received_parts) == list(range(total_parts))


@pytest.mark.asyncio
async def test_prefetch_backpressure_and_bounded_memory(tmp_path, monkeypatch):
    """2. 背压流控与内存上限测试:
    大分块总数 (55 块)，模拟消费端迟缓，采样 AdaptivePrefetchQueue 队列深度。
    验证 prefetch_queue 在 chunk_provider 模式下容量严格 <= 48 块 (< 24MB)。
    """
    total_parts = 55
    total_bytes = total_parts * CHUNK_SIZE
    provider = MockStreamProvider(total_bytes)

    max_qsize = 0
    orig_put = AdaptivePrefetchQueue.put

    async def tracking_put(self, item):
        nonlocal max_qsize
        max_qsize = max(max_qsize, self.qsize())
        return await orig_put(self, item)

    monkeypatch.setattr(AdaptivePrefetchQueue, "put", tracking_put)

    client = _setup_mock_client()

    async def mock_call(sender, req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            # 模拟上传延时以积压预读队列
            await asyncio.sleep(0.015)
            return True
        return MagicMock()

    client._call = mock_call

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [MagicMock() for _ in coros]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", str(tmp_path))

    meta_file = tmp_path / "stream_backpressure.meta"
    res = await upload_file_parallel(
        client,
        "pikpak://mock-file-123",
        chunk_provider=provider,
        meta_path=str(meta_file),
        concurrency=2,
        in_flight_per_connection=1,
        prefetch_concurrency=6,
    )

    assert res.parts == total_parts
    # 严格内存红线校验: 容量上限必须 <= 48 块 (24MB)，背压自然生效
    assert max_qsize <= 48, f"预读队列深度超限: max_qsize={max_qsize} > 48"


@pytest.mark.asyncio
async def test_prefetch_micro_retry_transient_error(tmp_path, monkeypatch):
    """3. 微观瞬断重试与委托重试队列测试:
    part 1 注入 1 次 ChunkFetchTransientError，应在微观重试中就地恢复 (0.2s 退避)；
    part 2 注入 3 次 ChunkFetchTransientError，微观重试 2 次耗尽后委托 retry_queue 且流水线不发生死锁。
    """
    total_parts = 6
    total_bytes = total_parts * CHUNK_SIZE
    provider = MockStreamProvider(total_bytes)

    # part 1: 失败 1 次后成功 (微观重试第 1 次成功)
    provider.errors[1] = [ChunkFetchTransientError("CDN connection reset")]

    # part 2: 连续失败 3 次 (耗尽微观重试 2 次后委托 retry_queue)
    provider.errors[2] = [
        ChunkFetchTransientError("CDN timeout 1"),
        ChunkFetchTransientError("CDN timeout 2"),
        ChunkFetchTransientError("CDN timeout 3"),
    ]

    client = _setup_mock_client()
    received_parts = []

    async def mock_call(sender, req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            await asyncio.sleep(0.005)
            received_parts.append(req.file_part)
            return True
        return MagicMock()

    client._call = mock_call

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [MagicMock() for _ in coros]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", str(tmp_path))

    meta_file = tmp_path / "stream_retry.meta"
    res = await upload_file_parallel(
        client,
        "pikpak://mock-file-123",
        chunk_provider=provider,
        meta_path=str(meta_file),
        concurrency=2,
        in_flight_per_connection=1,
        prefetch_concurrency=4,
    )

    assert res.parts == total_parts
    # 验证全部分块最终均被成功上传 (包括微观自愈的 part 1 与 retry_queue 接盘的 part 2)
    assert set(received_parts) == set(range(total_parts))


@pytest.mark.asyncio
async def test_control_plane_proxy_isolation(monkeypatch):
    """4. 控制面代理隔离测试:
    配置 pikpak_proxy_url 后，验证 _run_cli 注入子进程 env 字典，
    且主进程 os.environ 绝对保持干净（红线：数据面 zero proxy 消耗）。
    """
    # 确保当前主进程没有代理环境变量污染
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        monkeypatch.delenv(key, raising=False)

    fake_proxy = "http://127.0.0.1:7890"
    monkeypatch.setattr(
        "app.db.account_repository.SystemSettingsRepository.get_str",
        lambda key, default="": fake_proxy if key == "pikpak_proxy_url" else default,
    )

    captured_env = None

    class FakeProcess:
        returncode = 0

        async def communicate(self):
            return b'{"status": "ok"}', b""

    async def fake_create_subprocess_exec(*args, **kwargs):
        nonlocal captured_env
        captured_env = kwargs.get("env")
        return FakeProcess()

    monkeypatch.setattr("asyncio.create_subprocess_exec", fake_create_subprocess_exec)

    res = await _run_cli(["auth", "status"], json_mode=True)
    assert res == {"status": "ok"}
    assert captured_env is not None

    # 子进程 env 必须显式包含全部 5 个代理环境变量
    assert captured_env["HTTP_PROXY"] == fake_proxy
    assert captured_env["HTTPS_PROXY"] == fake_proxy
    assert captured_env["http_proxy"] == fake_proxy
    assert captured_env["https_proxy"] == fake_proxy
    assert captured_env["all_proxy"] == fake_proxy

    # 核心安全红线: 主进程 os.environ 绝对不受任何污染
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        assert key not in os.environ, f"主进程 os.environ 被代理污染: {key}={os.environ[key]}"


@pytest.mark.asyncio
async def test_stream_chunk_provider_direct_connection_trust_env_false():
    """5. 数据面 100% 物理直连测试:
    PikPakStreamChunkProvider._ensure_session 创建长连接池 TCPConnector，
    且 ClientSession 显式声明 trust_env=False (绝不读取系统/环境变量走代理)。
    """
    provider = PikPakStreamChunkProvider(
        file_id="fid-999",
        file_name="video.mp4",
        total_bytes=1024 * 1024,
        url_provider=AsyncMock(return_value="https://cdn.pikpak.com/video.mp4"),
    )

    try:
        session = await provider._ensure_session()
        assert isinstance(session, aiohttp.ClientSession)
        # 数据面零代理消耗红线: trust_env 必须为 False
        assert session.trust_env is False

        connector = session.connector
        assert isinstance(connector, aiohttp.TCPConnector)
        assert connector._limit == 8
        assert connector._limit_per_host == 6
        assert connector._keepalive_timeout == 60.0
        assert connector._cached_hosts._ttl == 300.0
    finally:
        await provider.aclose()
        assert session.closed is True


@pytest.mark.asyncio
async def test_settings_api_validation(monkeypatch):
    """6. 设置中心 API 校验测试:
    校验 pikpak_prefetch_concurrency (1~16) 与 pikpak_proxy_url (http/https/socks5/socks5h 及留空)。
    """
    from fastapi import HTTPException

    from app.api.settings import update_settings
    from app.models.settings import SystemSettingUpdateRequest

    # 正常用例: 合法并发与合法代理
    req = SystemSettingUpdateRequest(
        settings={
            "pikpak_prefetch_concurrency": "8",
            "pikpak_proxy_url": "http://127.0.0.1:7890",
        }
    )
    monkeypatch.setattr("app.db.account_repository.SystemSettingsRepository.set_batch", lambda s: None)
    monkeypatch.setattr(
        "app.db.account_repository.SystemSettingsRepository.get_all_grouped",
        lambda: {"categories": {}, "all_settings": {}},
    )
    res = await update_settings(req)
    assert res is not None

    # 正常用例: 留空代理 (直连)
    req_empty = SystemSettingUpdateRequest(settings={"pikpak_proxy_url": ""})
    await update_settings(req_empty)

    # 异常用例: 并发超限 (>16)
    with pytest.raises(HTTPException) as exc_info:
        await update_settings(SystemSettingUpdateRequest(settings={"pikpak_prefetch_concurrency": "17"}))
    assert exc_info.value.status_code == 400

    # 异常用例: 并发为 0
    with pytest.raises(HTTPException) as exc_info:
        await update_settings(SystemSettingUpdateRequest(settings={"pikpak_prefetch_concurrency": "0"}))
    assert exc_info.value.status_code == 400

    # 异常用例: 非法代理协议 (ftp://)
    with pytest.raises(HTTPException) as exc_info:
        await update_settings(SystemSettingUpdateRequest(settings={"pikpak_proxy_url": "ftp://127.0.0.1:21"}))
    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_prefetch_concurrency_degradation_chain(monkeypatch):
    """7. 动态预拉取并发降级链测试 (Task Params -> Context -> SystemSettingsRepository -> settings)"""
    from app.core.pipeline.engines import PikPakStreamTransportEngine
    from app.core.pipeline.engines.pikpak import single as pikpak_single
    from app.core.pipeline.models import PublishContext
    from app.core.stream_splitter import PartSpec

    engine = PikPakStreamTransportEngine()
    item = MagicMock(item_id="item-1", name="test.mp4")
    part = PartSpec(offset=0, length=1024, part_index=1)
    provider = MagicMock(total_bytes=1024, file_name="test.mp4")
    client = MagicMock()
    client.send_file = AsyncMock(return_value=MagicMock(id=100))

    captured_concurrency = None

    async def fake_upload(client_arg, path_arg, **kwargs):
        nonlocal captured_concurrency
        get_p = kwargs.get("get_prefetch_concurrency")
        captured_concurrency = get_p() if get_p else kwargs.get("prefetch_concurrency")
        return MagicMock(parts=1)

    with patch("app.core.fast_telethon.upload_file_parallel", fake_upload):
        # raw 分卷上传所有权已迁 single.upload_parts
        # Level 1: Task Params 优先级最高 (12)
        task_mock = {"params": {"pikpak_prefetch_concurrency": 12}}
        monkeypatch.setattr("app.db.task_repository.TaskQueueRepository.get_by_id", lambda tid: task_mock)
        ctx1 = PublishContext(task_id="t1", target_chat_id=-100, prefetch_concurrency=4)
        await pikpak_single.upload_parts(engine, client, provider=provider, item=item, parts=[part], context=ctx1)
        assert captured_concurrency == 12, f"Level 1 期望 12, 实际 {captured_concurrency}"

        # Level 2: Context 生效 (当 Task Params 未指定时)
        monkeypatch.setattr("app.db.task_repository.TaskQueueRepository.get_by_id", lambda tid: None)
        ctx2 = PublishContext(task_id="t2", target_chat_id=-100, prefetch_concurrency=9)
        await pikpak_single.upload_parts(engine, client, provider=provider, item=item, parts=[part], context=ctx2)
        assert captured_concurrency == 9, f"Level 2 期望 9, 实际 {captured_concurrency}"

        # Level 3: Context 为 None 时降级到 SystemSettingsRepository (11)
        ctx3 = PublishContext(task_id="t3", target_chat_id=-100, prefetch_concurrency=None)
        monkeypatch.setattr(
            "app.db.account_repository.SystemSettingsRepository.get_int",
            lambda k, d=0: 11 if k == "pikpak_prefetch_concurrency" else d,
        )
        await pikpak_single.upload_parts(engine, client, provider=provider, item=item, parts=[part], context=ctx3)
        assert captured_concurrency == 11, f"Level 3 期望 11, 实际 {captured_concurrency}"

        # Level 4: SystemSettingsRepository 异常/未设置时降级到 settings (默认 3)
        def fail_repo(k, d=0):
            raise RuntimeError("DB error")

        monkeypatch.setattr("app.db.account_repository.SystemSettingsRepository.get_int", fail_repo)
        monkeypatch.setattr(settings, "PIKPAK_PREFETCH_CONCURRENCY", 3)
        await pikpak_single.upload_parts(engine, client, provider=provider, item=item, parts=[part], context=ctx3)
        assert captured_concurrency == 3, f"Level 4 期望 3, 实际 {captured_concurrency}"


@pytest.mark.asyncio
async def test_persistent_failure_quota_shared_and_fatal_at_5(tmp_path, monkeypatch):
    """8. 微观重试与发送物化共享 5 次上限测试:
    注入恒瞬断块，验证总计恰 5 次尝试后任务 fatal，不多不少。
    """
    total_parts = 3
    total_bytes = total_parts * CHUNK_SIZE
    provider = MockStreamProvider(total_bytes)

    # part 1 恒瞬断
    provider.errors[1] = [ChunkFetchTransientError(f"transient error #{i}") for i in range(100)]

    client = _setup_mock_client()

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [MagicMock() for _ in coros]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", str(tmp_path))

    meta_file = tmp_path / "stream_quota.meta"

    with pytest.raises(ChunkFetchTransientError):
        await upload_file_parallel(
            client,
            "pikpak://mock-file-123",
            chunk_provider=provider,
            meta_path=str(meta_file),
            concurrency=2,
            in_flight_per_connection=1,
            prefetch_concurrency=4,
        )

    # 验证 part 1 被调用的次数恰好为 5 (prefetch 尝试 3 次 [1初始+2微重试] + worker 尝试 2 次)
    assert provider.read_history.count(1) == 5, f"期望恰 5 次，实际 {provider.read_history.count(1)}"


# ==============================================================================
# Part: test_upload_ring_buffer.py
# ==============================================================================
CHUNK = 512 * 1024


@pytest.fixture(autouse=True)
def _pikpak_meta_trust_root(tmp_dir, monkeypatch):
    """·F2 联动: Provider 模式 meta_path 强制 trust_root 校验, 本文件 meta 落点
    同在 tmp_dir\\upload_meta 下, 沙箱根 patch 到该路径 (只加基建, 既有用例断言零改动)。"""
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", os.path.join(tmp_dir, "upload_meta"))


class CountingProvider(LocalFileChunkProvider):
    """记录 read_chunk 入参的计数包装 (断言 L1 不回读 / L2 定向补拉)"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.read_calls = []

    async def read_chunk(self, offset, size):
        self.read_calls.append((offset, size))
        return await super().read_chunk(offset, size)


def _make_client(fail_parts=None):
    """client mock 配方 (照既有回归): fail_parts={part_id: 剩余失败次数}, 命中时首次发送抛瞬断"""
    fail_parts = dict(fail_parts or {})

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    async def mock_call(sender, req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            pid = int(req.file_part)
            if fail_parts.get(pid, 0) > 0:
                fail_parts[pid] -= 1
                raise ConnectionResetError(f"injected transient failure part={pid}")
            return True
        return True

    client._call = mock_call
    return client


@pytest.fixture
def bypass_senders(monkeypatch):
    """旁路 sender 建立 (照 test_upload_pipelining fake_gather 范式)"""

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [MagicMock() for _ in coros]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)


def test_ring_budget_eviction():
    """预算淘汰: 超出预算逐出最旧 offset; 命中不弹出; 覆写计费正确; clear 归零"""
    ring = SlidingWindowRingBuffer(2 * CHUNK)
    ring.put(0, b"a" * CHUNK)
    ring.put(CHUNK, b"b" * CHUNK)
    assert ring.byte_size == 2 * CHUNK
    assert ring.get(0) == b"a" * CHUNK
    assert ring.get(CHUNK) == b"b" * CHUNK

    ring.put(2 * CHUNK, b"c" * CHUNK)  # 超预算 → 淘汰最旧 offset=0
    assert ring.get(0) is None
    assert ring.byte_size == 2 * CHUNK
    assert ring.get(CHUNK) == b"b" * CHUNK
    assert ring.get(2 * CHUNK) == b"c" * CHUNK

    ring.put(CHUNK, b"d" * (CHUNK // 2))  # 同 offset 覆写: 先扣旧账再入新账
    assert ring.byte_size == CHUNK + CHUNK // 2

    ring.clear()
    assert ring.byte_size == 0
    assert len(ring) == 0


def test_budget_never_exceeded_under_flood():
    """内存预算: 40 块 (20MB, 对齐 FloodPremiumWait ~20MB 触发粒度) 连续写入, byte_size 恒 ≤ 预算"""
    budget = 4 * CHUNK
    ring = SlidingWindowRingBuffer(budget)
    original_put = ring.put
    peak = 0

    def tracking_put(offset, data):
        nonlocal peak
        original_put(offset, data)
        peak = max(peak, ring.byte_size)

    ring.put = tracking_put
    for part in range(40):  # 40 × 512KB = 20MB ≫ 2MB 预算
        ring.put(part * CHUNK, b"x" * CHUNK)

    assert peak <= budget, f"内存峰值 {peak} 突破预算 {budget}"
    assert ring.byte_size <= budget


@pytest.mark.asyncio
async def test_l1_hit_provider_not_recalled(tmp_dir, bypass_senders, monkeypatch):
    """L1 命中: part 1 瞬断重试由环形缓存补发, Provider 每块只被预读调用一次 (零网络回读)"""
    monkeypatch.setattr(settings, "PIKPAK_RING_BUFFER_MB", 50)  # 显式钉住默认档
    num_chunks = 3
    payload = b"E" * (CHUNK * num_chunks)
    src = safe_write_bytes(os.path.join(tmp_dir, "l1.bin"), payload)
    provider = CountingProvider(src, file_id="l1", file_name="l1.bin")
    meta_path = os.path.join(tmp_dir, "upload_meta", "l1.upload.meta")
    client = _make_client({1: 1})  # part 1 首次发送注入瞬断

    res = await upload_file_parallel(
        client,
        "pikpak://l1",
        concurrency=2,
        in_flight_per_connection=1,
        chunk_provider=provider,
        meta_path=meta_path,
    )

    assert res.parts == num_chunks
    expected = [(i * CHUNK, CHUNK) for i in range(num_chunks)]
    assert sorted(provider.read_calls) == expected, (
        f"瞬断重试必须由 L1 补发 (Provider 仅预读各一次), 实际调用: {provider.read_calls}"
    )
    assert not os.path.exists(meta_path)


@pytest.mark.asyncio
async def test_l2_fallback_directed_read(tmp_dir, bypass_senders, monkeypatch):
    """L2 回退: 预算归零强制环缓存恒空 → part 1 重试触发 provider.read_chunk 定向补拉 (精确 offset/size)"""
    monkeypatch.setattr(settings, "PIKPAK_RING_BUFFER_MB", 0)
    # 环形缓冲容量已 DB 化 (设置可调): 预算归零语义须同时钉住 DB 读取点, 其余键透传原实现
    _real_get_int = SystemSettingsRepository.get_int

    def _pinned_get_int(key, default=0):
        if key == "pikpak_ring_buffer_mb":
            return 0
        return _real_get_int(key, default)

    monkeypatch.setattr(SystemSettingsRepository, "get_int", staticmethod(_pinned_get_int))
    num_chunks = 3
    payload = b"W" * (CHUNK * num_chunks)
    src = safe_write_bytes(os.path.join(tmp_dir, "l2.bin"), payload)
    provider = CountingProvider(src, file_id="l2", file_name="l2.bin")
    meta_path = os.path.join(tmp_dir, "upload_meta", "l2.upload.meta")
    client = _make_client({1: 1})

    res = await upload_file_parallel(
        client,
        "pikpak://l2",
        concurrency=2,
        in_flight_per_connection=1,
        chunk_provider=provider,
        meta_path=meta_path,
    )

    assert res.parts == num_chunks
    part1_calls = [c for c in provider.read_calls if c[0] == CHUNK]
    assert part1_calls == [(CHUNK, CHUNK), (CHUNK, CHUNK)], (
        f"part 1 应被预读一次 + L2 定向补拉一次, 实际: {provider.read_calls}"
    )
    assert len(provider.read_calls) == num_chunks + 1


@pytest.mark.asyncio
async def test_ring_disabled_always_provider(tmp_dir, bypass_senders, monkeypatch):
    """降级档: PIKPAK_ENABLE_RING_BUFFER=False → L1 零命中, 瞬断重试永远走 Provider"""
    monkeypatch.setattr(settings, "PIKPAK_ENABLE_RING_BUFFER", False)
    num_chunks = 3
    payload = b"D" * (CHUNK * num_chunks)
    src = safe_write_bytes(os.path.join(tmp_dir, "l1off.bin"), payload)
    provider = CountingProvider(src, file_id="l1off", file_name="l1off.bin")
    meta_path = os.path.join(tmp_dir, "upload_meta", "l1off.upload.meta")
    client = _make_client({1: 1})

    res = await upload_file_parallel(
        client,
        "pikpak://l1off",
        concurrency=2,
        in_flight_per_connection=1,
        chunk_provider=provider,
        meta_path=meta_path,
    )

    assert res.parts == num_chunks
    part1_calls = [c for c in provider.read_calls if c[0] == CHUNK]
    assert part1_calls == [(CHUNK, CHUNK), (CHUNK, CHUNK)], (
        f"关闭环缓存后瞬断重试必须回读 Provider, 实际: {provider.read_calls}"
    )
    assert len(provider.read_calls) == num_chunks + 1


@pytest.mark.asyncio
async def test_cancel_releases_ring_no_orphans(tmp_dir, bypass_senders):
    """取消安全: 中途取消 → UploadCancelledError + 沙箱 meta 保留 + 孤儿协程扫描为空 + 严格 rmtree 成功"""
    num_chunks = 4
    payload = b"X" * (CHUNK * num_chunks)
    # 媒体文件放自控子目录: tmp_dir 根下有 autouse setup_test_db 持有句柄的 SQLite,
    # 不能对 tmp_dir 整体做严格 rmtree
    src = safe_write_bytes(os.path.join(tmp_dir, "media", "cancel.bin"), payload)
    provider = LocalFileChunkProvider(src, file_id="cancel", file_name="cancel.bin")
    meta_path = os.path.join(tmp_dir, "upload_meta", "cancel.upload.meta")

    state = {"cancelled": False}
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    async def cancel_call(sender, req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            if int(req.file_part) >= 1:
                state["cancelled"] = True
            return True
        return True

    client._call = cancel_call

    def _collect_orphans():
        names = ("heartbeat_loop", "worker", "_prefetch_producer")
        current = asyncio.current_task()
        return [
            t
            for t in asyncio.all_tasks()
            if t is not current and not t.done() and any(name in repr(t.get_coro()) for name in names)
        ]

    with pytest.raises(UploadCancelledError):
        await upload_file_parallel(
            client,
            "pikpak://cancel",
            concurrency=1,
            in_flight_per_connection=1,
            is_cancelled=lambda: state["cancelled"],
            chunk_provider=provider,
            meta_path=meta_path,
        )

    assert os.path.isfile(meta_path), "取消后 meta 必须保留在沙箱 (可续传)"
    for _ in range(20):
        if not _collect_orphans():
            break
        await asyncio.sleep(0.1)
    orphans = _collect_orphans()
    assert not orphans, f"取消后不允许 worker/heartbeat/prefetch 孤儿协程残留: {orphans}"
    # 严格 rmtree (不带 ignore_errors): 媒体与 meta 均无 fd/句柄泄漏时必然成功 (回归网);
    # tmp_dir 根因 SQLite 句柄由 conftest teardown 释放, 不在严格删除范围
    shutil.rmtree(os.path.dirname(src))
    shutil.rmtree(os.path.dirname(meta_path))
