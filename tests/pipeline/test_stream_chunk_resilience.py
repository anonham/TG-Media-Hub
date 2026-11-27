"""Pipeline Core: 远端流分块容灾、404/403 熔断跳过与切片 Provider 健壮性测试 (Stream Chunk Resilience)

覆盖范围:
- 原 test_issue5_stream_chunk_404: 404 远端流熔断跳过与 pipeline 容灾
- 原 test_provider_read_resiliency: 预读生产者 Provider 瞬断重排队与重试上限
- 原 test_stream_chunk_provider: PikPakStreamChunkProvider 契约、分片读取、并发刷新与生命周期"""

import asyncio
import os
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telethon import types
from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest


def _make_client(captured_parts=None):
    """既有回归同款 client mock 配方: AsyncMock + dc_id=4 + cfg 兜底 + _call 分块载荷捕获"""
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4)
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    async def mock_call(sender, req):
        if captured_parts is not None and isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            captured_parts[int(req.file_part)] = bytes(req.bytes)
        return True

    client._call = mock_call
    return client


@pytest.fixture
def bypass_senders(monkeypatch):
    """旁路 sender 建立: 关闭 init 协程并返回 MagicMock sender"""

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [MagicMock() for _ in coros]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)


from _safeio import safe_write_bytes
from aiohttp import web
from aiohttp.test_utils import TestServer

from app.config import settings
from app.core import fast_telethon as ft
from app.core.fast_telethon import upload_file_parallel
from app.core.pikpak.taxonomy import (
    PikPakPublishFatalError,
    PikPakPublishRetryableError,
    classify_pikpak_failure,
    wrap_pikpak_failure,
)
from app.core.pipeline.models import PublishContext, PublishItem
from app.core.pipeline.pipeline import PublishPipeline
from app.core.pipeline.runtime import BatchState
from app.core.stream_chunk_provider import (
    BaseChunkProvider,
    ChunkFetchTransientError,
    ChunkProviderError,
    LocalFileChunkProvider,
    PikPakLinkExpiredError,
    PikPakRiskControlError,
    PikPakStreamChunkProvider,
    PikPakThrottledError,
    SlicedChunkProvider,
    _parse_retry_after,
)
from app.core.stream_chunk_provider import (
    PikPakNotFoundError as ProviderPikPakNotFoundError,
)


# ==============================================================================
# Part: test_issue5_stream_chunk_404.py
# ==============================================================================
@pytest.mark.asyncio
async def test_stream_chunk_provider_raises_404_not_found():
    provider = PikPakStreamChunkProvider(
        file_id="test_fid_1",
        file_name="test_404.mp4",
        total_bytes=1024,
        url_provider=AsyncMock(return_value="https://cdn.example.com/test_404.mp4"),
    )

    # Mock session.get returning status 404
    mock_resp = AsyncMock()
    mock_resp.status = 404
    mock_resp.headers = {"Content-Type": "application/octet-stream"}
    mock_resp.__aenter__.return_value = mock_resp
    mock_resp.__aexit__.return_value = None

    mock_session = MagicMock()
    mock_session.closed = False
    mock_session.get.return_value = mock_resp
    provider._session = mock_session

    with pytest.raises(ProviderPikPakNotFoundError) as exc_info:
        await provider.read_chunk(0, 512)

    assert "404" in str(exc_info.value)
    assert "test_fid_1" in str(exc_info.value)


def test_classify_and_wrap_provider_404_failure():
    exc = ProviderPikPakNotFoundError("404 Not Found")
    category, detail = classify_pikpak_failure(exc)
    assert category == "not_found"
    assert "文件不存在" in detail

    wrapped = wrap_pikpak_failure(exc)
    assert isinstance(wrapped, PikPakPublishFatalError)
    assert wrapped.category == "not_found"


@pytest.mark.asyncio
async def test_pipeline_handles_not_found_by_skipping_gracefully():
    # Test that _handle_transport_failure for not_found returns without re-raising
    pipeline = PublishPipeline(
        source=MagicMock(),
        engine=MagicMock(),
    )
    context = PublishContext(
        task_id="task_404",
        target_chat_id="-1009000000010",
        source_type="pikpak",
    )
    b = BatchState()
    item = PublishItem(item_id="fid_missing", name="missing.mp4", size_bytes=100)

    exc = wrap_pikpak_failure(ProviderPikPakNotFoundError("404"))

    with patch("app.db.task_repository.TaskQueueRepository.append_log_async", new_callable=AsyncMock):
        # Should not raise exception
        await pipeline._handle_transport_failure(exc, item, context, b, is_pikpak=True)

    assert len(b.failed) == 1
    assert "missing.mp4" in b.failed[0]
    assert len(b.failed_details) == 1
    assert b.failed_details[0]["file_name"] == "missing.mp4"


# ==============================================================================
# Part: test_provider_read_resiliency.py
# ==============================================================================
CHUNK = 512 * 1024


@pytest.fixture(autouse=True)
def _pikpak_meta_trust_root(tmp_dir, monkeypatch):
    """·F2 联动: Provider 模式 meta_path 强制 trust_root 校验, 本文件 meta 落点
    同在 tmp_dir\\upload_meta 下, 沙箱根 patch 到该路径 (与 test_upload_provider_mode 同范式)。"""
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", os.path.join(tmp_dir, "upload_meta"))


class CountingProvider(LocalFileChunkProvider):
    """按 offset 计数并脚本化抛错的 Provider 包装 (瞬断/换链失效注入)。"""

    def __init__(self, file_path, *, fail_script=None, always_error=None, **kwargs):
        super().__init__(file_path, **kwargs)
        self.calls = {}  # offset -> read_chunk 调用次数
        self.fail_script = dict(fail_script or {})  # offset -> 剩余瞬断次数
        self.always_error = always_error  # 非 None 时恒抛该异常

    async def read_chunk(self, offset, size):
        self.calls[offset] = self.calls.get(offset, 0) + 1
        if self.always_error is not None:
            raise self.always_error
        remaining = self.fail_script.get(offset, 0)
        if remaining > 0:
            self.fail_script[offset] = remaining - 1
            raise ChunkFetchTransientError(f"模拟读取瞬断 (offset={offset})")
        return await super().read_chunk(offset, size)


@pytest.mark.asyncio
async def test_transient_once_self_heals(tmp_dir, bypass_senders):
    """瞬断一次后自愈: 上传成功、重组字节等价、该 offset 恰被调用 2 次
    (producer 1 次 + L2 物化 1 次; 读取失败未入环, L1 不可能命中)。"""
    num_chunks = 5
    payload = bytes(range(256)) * (CHUNK * num_chunks // 256)
    src = safe_write_bytes(os.path.join(tmp_dir, "heal.bin"), payload)
    meta_path = os.path.join(tmp_dir, "upload_meta", "f1_heal.upload.meta")
    provider = CountingProvider(src, file_id="f1-heal", file_name="heal.mp4")
    target_offset = CHUNK  # part 1
    provider.fail_script[target_offset] = 1

    captured = {}
    res = await upload_file_parallel(
        _make_client(captured),
        "pikpak://f1-heal",
        concurrency=2,
        in_flight_per_connection=1,
        chunk_provider=provider,
        meta_path=meta_path,
    )

    assert isinstance(res, types.InputFile)  # 2.5MB < 10MB 大文件阈值, 与 test_provider_mode_byte_equivalence 同口径
    assert res.parts == num_chunks
    assert b"".join(captured[i] for i in range(num_chunks)) == payload
    assert provider.calls[target_offset] == 2, f"期望 2 次调用 (producer+L2 物化), 实际 {provider.calls}"
    assert not os.path.exists(meta_path), "完成后 meta 必须被清理"


@pytest.mark.asyncio
async def test_persistent_transient_fatal_after_limit(tmp_dir, bypass_senders):
    """持续瞬断达共享额度上限: 整任务 fatal 且异常类型 ChunkFetchTransientError;
    调用次数恰 5 (producer 1 + 发送路径物化 4, 读取+发送共享每块 5 次额度);
    沙箱 meta 保留可续传。"""
    payload = b"P" * (CHUNK * 3)
    src = safe_write_bytes(os.path.join(tmp_dir, "persist.bin"), payload)
    meta_path = os.path.join(tmp_dir, "upload_meta", "f1_persist.upload.meta")
    provider = CountingProvider(src, file_id="f1-persist", file_name="persist.mp4")
    target_offset = CHUNK
    provider.fail_script[target_offset] = 10**9  # 恒瞬断

    with pytest.raises(ChunkFetchTransientError):
        await upload_file_parallel(
            _make_client(),
            "pikpak://f1-persist",
            concurrency=2,
            in_flight_per_connection=1,
            chunk_provider=provider,
            meta_path=meta_path,
        )

    assert provider.calls[target_offset] == 5, f"期望恰 5 次后 fatal, 实际 {provider.calls}"
    assert os.path.isfile(meta_path), "fatal 后 meta 必须保留 (可续传)"


@pytest.mark.asyncio
async def test_link_expired_stays_fatal(tmp_dir, bypass_senders):
    """LinkExpired 保持 fatal 语义: 直接 fatal 且不进重试队列 (该 offset 只被调用 1 次)。"""
    payload = b"E" * (CHUNK * 2)
    src = safe_write_bytes(os.path.join(tmp_dir, "expired.bin"), payload)
    meta_path = os.path.join(tmp_dir, "upload_meta", "f1_expired.upload.meta")
    provider = CountingProvider(
        src,
        file_id="f1-expired",
        file_name="expired.mp4",
        always_error=PikPakLinkExpiredError("模拟单飞换链后仍 403"),
    )

    with pytest.raises(PikPakLinkExpiredError):
        await upload_file_parallel(
            _make_client(),
            "pikpak://f1-expired",
            concurrency=2,
            in_flight_per_connection=1,
            chunk_provider=provider,
            meta_path=meta_path,
        )

    assert provider.calls[0] == 1, f"LinkExpired 不得重排队, 实际 {provider.calls}"


@pytest.mark.asyncio
async def test_legacy_disk_read_failure_still_fatal(tmp_dir, bypass_senders, monkeypatch):
    """legacy 固化: chunk_provider=None 时磁盘读失败仍 fatal (新分支对本地路径不可达,
    IOError 不被瞬断分支拦截)。"""
    payload = b"L" * (CHUNK * 3)
    src = safe_write_bytes(os.path.join(tmp_dir, "legacy.bin"), payload)
    real_sync_read = ft.breakpoints._sync_read_chunk
    state = {"failed": False}

    def flaky_sync_read(fd, offset, size):
        if not state["failed"]:
            state["failed"] = True
            raise OSError("模拟磁盘故障")
        return real_sync_read(fd, offset, size)

    monkeypatch.setattr(ft.breakpoints, "_sync_read_chunk", flaky_sync_read)
    with pytest.raises(IOError, match="模拟磁盘故障"):
        await upload_file_parallel(
            _make_client(),
            src,
            concurrency=2,
            in_flight_per_connection=1,
        )


# ==============================================================================
# Part: test_stream_chunk_provider.py
# ==============================================================================
CHUNK = 512 * 1024
TOTAL = CHUNK * 6  # 3MB 测试体


def _payload() -> bytes:
    return bytes(range(256)) * (TOTAL // 256)


class FakeCdn:
    """进程内 PikPak CDN 替身: generation=0 一律 403 (直链过期), >=1 按 Range 精确切 206。

    mode: serve (正常) | html (风控页) | abort (建连后瞬断);
    refresh_fixes: 控制 url_provider 换链后服务端是否恢复服务 (测"刷新后仍 403"时置 False)。
    """

    def __init__(self, payload: bytes):
        self.payload = payload
        self.generation = 1
        self.mode = "serve"
        self.refresh_fixes = True
        self.range_requests = []
        self.url_provider_calls = 0
        self.server = None

    async def handle(self, request):
        self.range_requests.append(request.headers.get("Range"))
        if self.mode == "abort":
            # 半途断体: 先发 206 头再断连 → 客户端在 resp.read() 阶段收到 ClientPayloadError
            # (传输层不对此重试, 请求计数确定; 纯建连后 abort 会触发 aiohttp 透明重连)
            resp = web.StreamResponse(
                status=206,
                headers={
                    "Content-Range": f"bytes 0-{CHUNK - 1}/{len(self.payload)}",
                    "Content-Type": "application/octet-stream",
                },
            )
            await resp.prepare(request)
            await resp.write(self.payload[:1024])
            request.transport.abort()
            return resp
        if self.mode == "html":
            return web.Response(status=200, text="<html>risk-control</html>", content_type="text/html")
        if self.generation == 0:
            return web.Response(status=403, text="expired")
        rng = request.headers.get("Range")
        if not rng:
            return web.Response(status=200, body=self.payload, content_type="application/octet-stream")
        spec = rng.split("=", 1)[1]
        start_s, _, end_s = spec.partition("-")
        start = int(start_s)
        end = int(end_s) if end_s else len(self.payload) - 1
        end = min(end, len(self.payload) - 1)
        return web.Response(
            status=206,
            body=self.payload[start : end + 1],
            headers={"Content-Range": f"bytes {start}-{end}/{len(self.payload)}"},
            content_type="application/octet-stream",
        )

    async def start(self):
        app = web.Application()
        app.router.add_route("GET", "/{tail:.*}", self.handle)
        self.server = TestServer(app)
        await self.server.start_server()

    async def stop(self):
        await self.server.close()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.server.port}/file"

    def make_url_provider(self):
        async def url_provider() -> str:
            self.url_provider_calls += 1
            if self.refresh_fixes:
                self.generation = max(self.generation, 1)  # 换链后服务正常代际
            return self.base_url

        return url_provider

    def make_provider(self, **kwargs) -> PikPakStreamChunkProvider:
        return PikPakStreamChunkProvider(
            file_id="fake-file-id",
            file_name="video.mp4",
            total_bytes=len(self.payload),
            url_provider=self.make_url_provider(),
            **kwargs,
        )


@pytest.fixture
async def cdn():
    fake = FakeCdn(_payload())
    await fake.start()
    yield fake
    await fake.stop()


@pytest.mark.asyncio
async def test_read_sequential_and_random_slices(cdn):
    """Range 206 语义: 顺序整读拼接 == 源字节; 首/中/尾任意偏移 512KB 读 == 源切片 (对齐 三段实测口径)"""
    provider = cdn.make_provider()

    collected = b""
    offset = 0
    while offset < TOTAL:
        piece = await provider.read_chunk(offset, CHUNK)
        assert len(piece) <= CHUNK
        collected += piece
        offset += len(piece)
    assert collected == cdn.payload

    assert await provider.read_chunk(0, CHUNK) == cdn.payload[0:CHUNK]
    mid = TOTAL // 2
    assert await provider.read_chunk(mid, CHUNK) == cdn.payload[mid : mid + CHUNK]
    tail = TOTAL - 100
    assert await provider.read_chunk(tail, CHUNK) == cdn.payload[tail:]
    assert cdn.url_provider_calls == 1, "顺序读全程只应索取一次直链"
    await provider.aclose()


@pytest.mark.asyncio
async def test_single_flight_refresh_concurrent_403(cdn):
    """L3 单飞换链: 并发 8 读块同时触发 403 → 全部成功且字节正确, url_provider 只被调用一次"""
    cdn.generation = 0  # 当前代际直链一律 403
    payload = cdn.payload
    offsets = [i * (CHUNK // 4) for i in range(8)]

    provider = cdn.make_provider()
    provider._current_url = cdn.base_url  # 预置直链: 首次取链不计入 url_provider 计数
    provider._url_generation = 1

    results = await asyncio.gather(*[provider.read_chunk(off, CHUNK) for off in offsets])

    assert cdn.url_provider_calls == 1, "并发 403 下必须单飞刷新, url_provider 仅一次"
    for off, data in zip(offsets, results, strict=False):
        assert data == payload[off : off + CHUNK]
    # 请求计数下界: 每块至少 1 次 + 至少 1 次换链重试; 后到的读块可能直接命中换链后
    # 的正常代际 (首试即成功), 故不设上界
    assert len(cdn.range_requests) >= 9
    await provider.aclose()


@pytest.mark.asyncio
async def test_refresh_still_403_raises(cdn):
    """刷新后仍 403 → PikPakLinkExpiredError 上抛, 且不再二次刷新 (url_provider 计数 == 1)"""
    cdn.generation = 0
    cdn.refresh_fixes = False  # 换链后服务端仍返回过期代际

    provider = cdn.make_provider()
    provider._current_url = cdn.base_url
    provider._url_generation = 1

    with pytest.raises(PikPakLinkExpiredError):
        await provider.read_chunk(0, CHUNK)
    assert cdn.url_provider_calls == 1
    await provider.aclose()


@pytest.mark.asyncio
async def test_html_risk_control_rejected(cdn):
    """Content-Type text/html (风控/登录页) → PikPakRiskControlError, 不重试"""
    cdn.mode = "html"
    provider = cdn.make_provider()
    try:
        with pytest.raises(PikPakRiskControlError):
            await provider.read_chunk(0, CHUNK)
        assert len(cdn.range_requests) == 1
    finally:
        await provider.aclose()


@pytest.mark.asyncio
async def test_transient_error_raised_no_loop(cdn):
    """建连后响应体半途断流 → ChunkFetchTransientError 立即上抛, Provider 内部零重试"""
    cdn.mode = "abort"
    provider = cdn.make_provider()
    try:
        with pytest.raises(ChunkFetchTransientError):
            await provider.read_chunk(0, CHUNK)
        assert len(cdn.range_requests) == 1, "瞬断必须直接上抛可重试错误, 不得内部循环重试"
    finally:
        await provider.aclose()


@pytest.mark.asyncio
async def test_edge_offsets(cdn):
    """边界: 越界/非法长度 ValueError; 非 http(s) 直链 ChunkProviderError; 尾块截断语义"""
    provider = cdn.make_provider()
    with pytest.raises(ValueError):
        await provider.read_chunk(TOTAL, CHUNK)
    with pytest.raises(ValueError):
        await provider.read_chunk(0, 0)
    tail = await provider.read_chunk(TOTAL - 100, CHUNK)
    assert tail == cdn.payload[-100:]

    strict = cdn.make_provider()
    strict._current_url = "ftp://example.invalid/file"
    strict._url_generation = 1
    try:
        with pytest.raises(ChunkProviderError):
            await strict.read_chunk(0, 1024)
    finally:
        await provider.aclose()
        await strict.aclose()


@pytest.mark.asyncio
async def test_aclose_idempotent(cdn):
    """aclose 幂等; 关闭后再读不复活会话 (明确 ChunkProviderError)"""
    provider = cdn.make_provider()
    await provider.read_chunk(0, 1024)  # 惰性建 session
    await provider.aclose()
    await provider.aclose()
    with pytest.raises(ChunkProviderError):
        await provider.read_chunk(0, 1024)


class _MemoryChunkProvider(BaseChunkProvider):
    """用于测试 SlicedChunkProvider 的纯内存 Provider"""

    def __init__(self, payload: bytes, file_id: str = "mem-1", file_name: str = "video.mp4"):
        self.payload = payload
        self.file_id = file_id
        self.file_name = file_name
        self.total_bytes = len(payload)
        self.closed = False

    async def read_chunk(self, offset: int, size: int) -> bytes:
        if self.closed:
            raise RuntimeError("provider closed")
        if offset >= self.total_bytes:
            raise ValueError("offset out of range")
        if size <= 0:
            raise ValueError("invalid size")
        return self.payload[offset : offset + size]

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_sliced_chunk_provider_contract():
    """SlicedChunkProvider 属性代理与初始化参数防御断言"""
    payload = b"0123456789ABCDEF" * 100  # 1600 bytes
    base = _MemoryChunkProvider(payload, file_id="file-999", file_name="movie.mkv")

    # 1. 规范属性校验: 单卷 (total_parts <= 1)
    single = SlicedChunkProvider(base, offset=0, length=500, part_index=1, total_parts=1)
    assert single.total_bytes == 500
    assert single.file_id == "file-999"
    assert single.file_name == "movie.mkv"

    # 2. 规范属性校验: 多分卷 (total_parts > 1) 派生编号
    part1 = SlicedChunkProvider(base, offset=0, length=700, part_index=1, total_parts=3)
    assert part1.total_bytes == 700
    assert part1.file_id == "file-999_part001"
    assert part1.file_name == "movie.mkv.part001"

    part2 = SlicedChunkProvider(base, offset=700, length=700, part_index=2, total_parts=3)
    assert part2.total_bytes == 700
    assert part2.file_id == "file-999_part002"
    assert part2.file_name == "movie.mkv.part002"

    part3 = SlicedChunkProvider(base, offset=1400, length=200, part_index=3, total_parts=3)
    assert part3.total_bytes == 200
    assert part3.file_id == "file-999_part003"
    assert part3.file_name == "movie.mkv.part003"

    # 3. 初始化防御校验
    with pytest.raises(ValueError, match="inner provider 不能为空"):
        SlicedChunkProvider(None, 0, 100)

    with pytest.raises(ValueError, match="切片起始偏移不能为负数"):
        SlicedChunkProvider(base, -1, 100)

    with pytest.raises(ValueError, match="切片长度必须为正整数"):
        SlicedChunkProvider(base, 0, 0)

    with pytest.raises(ValueError, match="切片超出源文件范围"):
        SlicedChunkProvider(base, 1000, 700)  # 1000 + 700 = 1700 > 1600


@pytest.mark.asyncio
async def test_sliced_chunk_provider_read_and_boundaries():
    """SlicedChunkProvider 读流切片映射与越界边界拦截"""
    payload = b"abcdefghijklmnopqrstuvwxyz" * 10  # 260 bytes
    base = _MemoryChunkProvider(payload)

    # 提取 [50, 150) 区间共 100 字节
    slice_prov = SlicedChunkProvider(base, offset=50, length=100, part_index=2, total_parts=3)
    expected_slice = payload[50:150]

    # 1. 顺序分块读取并拼装验证等价性
    chunk1 = await slice_prov.read_chunk(0, 40)
    chunk2 = await slice_prov.read_chunk(40, 40)
    chunk3 = await slice_prov.read_chunk(80, 40)  # 超出切片尾部 (80 + 40 = 120 > 100)
    assert chunk1 == expected_slice[0:40]
    assert chunk2 == expected_slice[40:80]
    assert chunk3 == expected_slice[80:100]  # 必须截断到 20 字节
    assert chunk1 + chunk2 + chunk3 == expected_slice

    # 2. 越界保护
    with pytest.raises(ValueError, match="读取偏移越界"):
        await slice_prov.read_chunk(100, 10)  # offset == total_bytes 越界

    with pytest.raises(ValueError, match="读取偏移越界"):
        await slice_prov.read_chunk(150, 10)

    with pytest.raises(ValueError, match="非法读取长度"):
        await slice_prov.read_chunk(0, 0)


# ==============================================================================
# Milestone 1: Signal Deconstruction, PikPakThrottledError & Zero Micro-Retry
# ==============================================================================
@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [429, 502, 503, 504])
async def test_stream_chunk_provider_status_priority_over_html_raises_throttled(status_code):
    """503/429/502/504 即使响应头为 Content-Type: text/html (Nginx/Cloudflare 默认错误页)，
    也必须按状态码优先映射为 PikPakThrottledError，绝不误判为 PikPakRiskControlError。"""
    provider = PikPakStreamChunkProvider(
        file_id="fid_throttle",
        file_name="throttle_video.mp4",
        total_bytes=1024,
        url_provider=AsyncMock(return_value="https://cdn.example.com/throttle.mp4?sign=SECRET_SIGN_123"),
    )

    mock_resp = AsyncMock()
    mock_resp.status = status_code
    mock_resp.headers = {
        "Content-Type": "text/html; charset=utf-8",
        "Retry-After": "900",
    }
    mock_resp.__aenter__.return_value = mock_resp
    mock_resp.__aexit__.return_value = None

    mock_session = MagicMock()
    mock_session.closed = False
    mock_session.get.return_value = mock_resp
    provider._session = mock_session

    with pytest.raises(PikPakThrottledError) as exc_info:
        await provider.read_chunk(0, 512)

    err = exc_info.value
    assert not isinstance(err, ChunkFetchTransientError)
    assert err.status == status_code
    assert err.retry_after == 900.0
    assert "SECRET_SIGN_123" not in str(err)


def test_parse_retry_after_numeric_and_http_date():
    assert _parse_retry_after(None) is None
    assert _parse_retry_after("") is None
    assert _parse_retry_after("120") == 120.0
    assert _parse_retry_after("invalid-header") is None
    # Past HTTP-Date clamps to 0.0
    assert _parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0


def test_classify_and_wrap_throttled_failure():
    exc = PikPakThrottledError("CDN 503", status=503, retry_after=900.0)
    category, detail = classify_pikpak_failure(exc)
    assert category == "throttled"
    assert "频控" in detail

    wrapped = wrap_pikpak_failure(exc)
    assert isinstance(wrapped, PikPakPublishRetryableError)
    assert wrapped.category == "throttled"
    assert wrapped.retry_after == 900.0


@pytest.mark.asyncio
async def test_throttled_and_risk_control_zero_micro_retries_in_fast_telethon(tmp_dir, bypass_senders):
    """PikPakThrottledError 与 PikPakRiskControlError 在 upload_file_parallel 中必须
    0 次微观重试立即熔断 (calls == 1)，绝不触发 5 次重试风暴。"""
    payload = b"T" * (CHUNK * 3)
    src = safe_write_bytes(os.path.join(tmp_dir, "throttled.bin"), payload)
    meta_path = os.path.join(tmp_dir, "upload_meta", "f1_throttled.upload.meta")

    provider = CountingProvider(
        src,
        file_id="f1-throttled",
        file_name="throttled.mp4",
        always_error=PikPakThrottledError("CDN 503", status=503, retry_after=600.0),
    )

    with pytest.raises(PikPakThrottledError):
        await upload_file_parallel(
            _make_client(),
            "pikpak://f1-throttled",
            concurrency=1,
            in_flight_per_connection=1,
            prefetch_concurrency=1,
            chunk_provider=provider,
            meta_path=meta_path,
        )

    assert provider.calls.get(0, 0) == 1, f"PikPakThrottledError 必须 0 次微观重试立即熔断, 实际 {provider.calls}"
    assert os.path.isfile(meta_path), "熔断后必须保留 .upload.meta 供冷却后断点续传"


@pytest.mark.asyncio
async def test_sliced_chunk_provider_aclose_noop():
    """SlicedChunkProvider.aclose() 为 no-op, 绝不关闭底层 inner Provider"""
    payload = b"hello world test"
    base = _MemoryChunkProvider(payload)

    sliced = SlicedChunkProvider(base, offset=0, length=5, part_index=1, total_parts=2)
    chunk = await sliced.read_chunk(0, 5)
    assert chunk == b"hello"

    # 切片 aclose 必须是 no-op
    await sliced.aclose()
    assert not base.closed, "切片 aclose() 严禁关闭底层 Provider!"

    # 底层与后续切片仍可正常读取
    sliced2 = SlicedChunkProvider(base, offset=6, length=5, part_index=2, total_parts=2)
    chunk2 = await sliced2.read_chunk(0, 5)
    assert chunk2 == b"world"


@pytest.mark.asyncio
async def test_fetch_range_caps_end_header(cdn):
    """Range 请求头 end 字段必须受 total_bytes - 1 约束, 避免严格 CDN 报 416"""
    provider = cdn.make_provider()
    try:
        # 当请求 (TOTAL - 100, CHUNK) 时，offset + size - 1 远大于 TOTAL - 1
        tail = await provider.read_chunk(TOTAL - 100, CHUNK)
        assert len(tail) == 100
        # 检查发给 CDN 的 Range 头: 必须是 bytes={TOTAL-100}-{TOTAL-1}
        assert cdn.range_requests, "必须发出 Range 请求"
        last_range = cdn.range_requests[-1]
        expected_range = f"bytes={TOTAL - 100}-{TOTAL - 1}"
        assert last_range == expected_range, f"Range 头部未做 min 边界截断约束: {last_range}"
    finally:
        await provider.aclose()


# ==============================================================================
# Part: Milestone 2: 事前拟人化流量整形、消除冗余请求与长流主动微轮转
# ==============================================================================
@pytest.mark.asyncio
async def test_stream_chunk_provider_initial_url_no_redundant_fetch(cdn):
    """M2-1: 注入 initial_url 时首读绝不重复调用 url_provider，消除 VSSE 双倍索链 Bug"""
    mock_provider = AsyncMock(return_value=cdn.base_url)
    provider = PikPakStreamChunkProvider(
        file_id="test_init_url",
        file_name="init_url.mp4",
        total_bytes=TOTAL,
        url_provider=mock_provider,
        initial_url=cdn.base_url,
    )
    try:
        data = await provider.read_chunk(0, 512)
        assert len(data) == 512
        # 初始注入后首读零控制面调用
        assert mock_provider.await_count == 0
        assert provider._url_generation == 1
    finally:
        await provider.aclose()


@pytest.mark.asyncio
async def test_stream_chunk_provider_huge_file_concurrency_and_pool_clamping():
    """M2-2: 文件体积超过 2000 MiB (1.95 GiB) 封顶钳制 prefetch_concurrency=2 且连接池 limit_per_host=2"""
    mock_url = AsyncMock(return_value="https://cdn.example.com/huge.mp4")
    # 超出普通上限 2000 MiB (1.95 GiB)
    huge_bytes = settings.MAX_FILE_SIZE_BYTES + 1024
    provider = PikPakStreamChunkProvider(
        file_id="test_huge_file",
        file_name="huge.mp4",
        total_bytes=huge_bytes,
        url_provider=mock_url,
        prefetch_concurrency=4,  # 传入 4 必须被封顶钳制为 2
    )
    try:
        assert provider.prefetch_concurrency == 2
        session = await provider._ensure_session()
        connector = session.connector
        assert connector is not None
        assert connector.limit_per_host == 2  # max(2, prefetch_concurrency)
    finally:
        await provider.aclose()

    # 传入 1 保持为 1
    provider_1 = PikPakStreamChunkProvider(
        file_id="test_huge_file_1",
        file_name="huge.mp4",
        total_bytes=huge_bytes,
        url_provider=mock_url,
        prefetch_concurrency=1,
    )
    try:
        assert provider_1.prefetch_concurrency == 1
    finally:
        await provider_1.aclose()

    # 传入 None 时默认配置 (3) 仍自动封顶钳制为 2
    provider_none = PikPakStreamChunkProvider(
        file_id="test_huge_file_none",
        file_name="huge.mp4",
        total_bytes=huge_bytes,
        url_provider=mock_url,
        prefetch_concurrency=None,
    )
    try:
        assert provider_none.prefetch_concurrency == 2
    finally:
        await provider_none.aclose()

    # 未超限文件保持配置并发与连接池限制
    normal_bytes = 100 * 1024 * 1024
    normal_provider = PikPakStreamChunkProvider(
        file_id="test_normal_file",
        file_name="normal.mp4",
        total_bytes=normal_bytes,
        url_provider=mock_url,
        prefetch_concurrency=4,
    )
    try:
        assert normal_provider.prefetch_concurrency == 4
        session_norm = await normal_provider._ensure_session()
        connector_norm = session_norm.connector
        assert connector_norm is not None
        assert connector_norm.limit == 8
        assert connector_norm.limit_per_host == 6
    finally:
        await normal_provider.aclose()


def test_cached_target_resolver_huge_file_fallback_bounded():
    """验证 _CachedTargetResolver 在 huge file 模式下 (max_val=2) 遇到异常类型转换时 fallback_default 被严密钳制 <= 2"""
    from app.core.fast_telethon.primitives import _CachedTargetResolver

    resolver = _CachedTargetResolver(
        getter=lambda: "invalid_non_int",  # 触发 ValueError / TypeError
        raw_val=None,
        setting_key="pikpak_prefetch_concurrency",
        default_attr="PIKPAK_PREFETCH_CONCURRENCY",
        fallback_default=3,  # 即使传入 3，也必须被 max_val 钳制为 2
        max_val=2,
        force_one=False,
    )
    assert resolver() == 2


@pytest.mark.asyncio
async def test_stream_chunk_provider_session_rotation(cdn):
    """M2-3: 累计读取字节超过轮转阈值时，主动微轮转 ClientSession 并重置计数"""
    provider = cdn.make_provider()
    # 模拟缩小的轮转阈值与超短呼吸
    provider._min_rotation_bytes = 1000
    provider._max_rotation_bytes = 1000
    provider._rotation_threshold = 1000
    provider._min_rotation_sleep = 0.01
    provider._max_rotation_sleep = 0.02

    try:
        # 第 1 块读取 600 字节，未超阈值
        chunk1 = await provider.read_chunk(0, 600)
        assert len(chunk1) == 600
        assert provider._session_bytes_read == 600
        first_session = provider._session
        assert first_session is not None and not first_session.closed

        # 第 2 块读取 600 字节，累计 1200 字节 (> 1000 阈值)
        chunk2 = await provider.read_chunk(600, 600)
        assert len(chunk2) == 600
        assert provider._session_bytes_read == 1200

        # 第 3 块读取前触发 _maybe_rotate_session: 旧 session 关闭，建立新 session
        chunk3 = await provider.read_chunk(1200, 600)
        assert len(chunk3) == 600
        assert first_session.closed, "旧 ClientSession 必须已被优雅关闭"
        assert provider._session is not None and provider._session is not first_session
        assert provider._session_bytes_read == 600  # 计数复位并累加本块字节
    finally:
        await provider.aclose()


def test_fast_telethon_inspect_source_total_bytes():
    """M2-2 helper: _inspect_source_total_bytes 正确解包 SlicedChunkProvider 与嵌套 Provider"""
    from app.core.fast_telethon.prefetch import _inspect_source_total_bytes

    mock_url = AsyncMock(return_value="https://cdn.example.com/test.mp4")
    root_huge = PikPakStreamChunkProvider(
        file_id="test_nest",
        file_name="nest.mp4",
        total_bytes=settings.MAX_FILE_SIZE_BYTES + 5000,
        url_provider=mock_url,
    )
    sliced = SlicedChunkProvider(
        inner=root_huge,
        offset=0,
        length=1000,
        part_index=1,
        total_parts=2,
    )
    assert sliced.total_bytes == 1000
    assert _inspect_source_total_bytes(sliced) == settings.MAX_FILE_SIZE_BYTES + 5000


@pytest.mark.asyncio
async def test_inter_part_breathing_delay():
    """M2-4: 呼吸延迟 T = max(delay_seconds, min_floor_s) + uniform(0, jitter_s)，
    并能快速响应取消信号 —— 显式注入策略 (生产默认经 conftest 零等待覆盖)。"""
    from app.core.pipeline.engines import _inter_part_breathing_delay
    from app.core.pipeline.models import BreathingPolicy
    from app.core.scheduler import UploadCancelledError

    context = PublishContext(
        task_id="test_breath",
        target_chat_id="-1001",
        delay_seconds=0.05,
        breathing=BreathingPolicy(min_floor_s=0.05, jitter_s=0.05, poll_s=0.5),
    )

    t0 = time.monotonic()
    # 模拟 0.1s 呼吸 (jitter=0.05, max(0.05, 0.05) + 0.05 = 0.1s; 严禁 patch builtins.max 破坏 Linux EpollSelector 事件循环)
    with patch("random.uniform", return_value=0.05):
        await _inter_part_breathing_delay(context, 1, 3, "TestChannel")
    elapsed = time.monotonic() - t0
    assert elapsed >= 0.1

    # 验证在呼吸等待期间能够响应取消信号
    cancelled_context = PublishContext(
        task_id="test_cancel",
        target_chat_id="-1001",
        delay_seconds=3.0,
        is_cancelled=lambda: True,
    )
    with pytest.raises(UploadCancelledError):
        await _inter_part_breathing_delay(cancelled_context, 1, 2, "TestCancel")


@pytest.mark.asyncio
async def test_fmp4_volume_throttled_error_immediate_fatal():
    """M2-4: fMP4 单卷传输遇到 503 频控特征时，立即上抛 throttled 异常，严禁短周期盲目重试 3 次"""
    from app.core.e2e_capture import E2ECaptureSession
    from app.core.iso_bmff.fragmented import Fmp4PartPlan
    from app.core.pipeline.engines.pikpak.fmp4 import stream_fmp4_volume

    client = _make_client()
    item = PublishItem(item_id="fid_fmp4_503", name="fmp4_503.mp4", size_bytes=1000)
    plan = Fmp4PartPlan(
        part_index=1,
        total_parts=2,
        start_sec=0.0,
        end_sec=60.0,
        part_name="fmp4_503.part001.mp4",
        duration_sec=60.0,
    )
    context = PublishContext(task_id="t_503", target_chat_id="-1001")
    capture_session = E2ECaptureSession.inactive()

    stream_attempts = 0

    async def fake_stream_to_telegram(*args, **kwargs):
        nonlocal stream_attempts
        stream_attempts += 1
        raise RuntimeError("Server returned 503 Service Unavailable")

    with (
        patch(
            "app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram",
            side_effect=fake_stream_to_telegram,
        ),
        pytest.raises(PikPakPublishRetryableError) as exc_info,
    ):
        # fMP4 单卷传输所有权已迁 fmp4.stream_fmp4_volume 模块函数
        await stream_fmp4_volume(
            client=client,
            item=item,
            plan=plan,
            url="https://cdn.example.com/stream",
            context=context,
            capture_session=capture_session,
            threshold=2000 * 1024 * 1024,
            ext=".mp4",
            copy_audio=True,
            thumb_bytes=None,
            prior_bytes=0,
            total_planned=2,
            workers_count=2,
            in_flight_count=1,
            refresh_url=AsyncMock(return_value="https://cdn.example.com/stream2"),
        )

    assert exc_info.value.category == "throttled"
    assert stream_attempts == 1, "遇到 503 必须立即刹停，严禁在引擎内重试 3 次"


@pytest.mark.asyncio
async def test_stream_chunk_provider_session_rotation_concurrent_reads(cdn):
    """M2-3: 并发读块过程中触发轮转时，在途请求平滑完成，无连接强关异常且旧 session 优雅析构"""
    provider = cdn.make_provider()
    provider._min_rotation_bytes = 1000
    provider._max_rotation_bytes = 1000
    provider._rotation_threshold = 1000
    provider._min_rotation_sleep = 0.02
    provider._max_rotation_sleep = 0.03

    try:
        # 并发执行读块，跨越 1000 字节轮转阈值
        results = await asyncio.gather(
            provider.read_chunk(0, 600),
            provider.read_chunk(600, 600),
            provider.read_chunk(1200, 600),
        )
        assert len(results) == 3
        assert all(len(r) == 600 for r in results)
        assert provider._closed is False
        assert len(provider._closing_sessions) == 0
    finally:
        await provider.aclose()


@pytest.mark.asyncio
async def test_stream_chunk_provider_ensure_session_concurrency_lock():
    """M2-3: 多个协程并发调用 _ensure_session 时，严格双检锁保证单例 session，无连接泄漏"""
    mock_url = AsyncMock(return_value="https://cdn.example.com/test.mp4")
    provider = PikPakStreamChunkProvider(
        file_id="test_lock",
        file_name="lock.mp4",
        total_bytes=1024 * 1024,
        url_provider=mock_url,
    )
    try:
        sessions = await asyncio.gather(*[provider._ensure_session() for _ in range(10)])
        first_session = sessions[0]
        assert all(s is first_session for s in sessions), "所有并发协程必须共享同一 ClientSession"
    finally:
        await provider.aclose()


@pytest.mark.asyncio
async def test_inter_part_breathing_delay_paused():
    """M2-4: 分卷呼吸延迟等待期间能够快速响应暂停信号"""
    from app.core.pipeline.engines import _inter_part_breathing_delay
    from app.core.scheduler import UploadPausedError

    paused_context = PublishContext(
        task_id="test_pause_breath",
        target_chat_id="-1001",
        delay_seconds=3.0,
        is_paused=lambda: True,
    )
    with pytest.raises(UploadPausedError):
        await _inter_part_breathing_delay(paused_context, 1, 2, "TestPause")


@pytest.mark.asyncio
async def test_stream_chunk_provider_rotation_blocks_concurrent_reads_during_pause(cdn):
    """Verify concurrent reads wait behind _rotation_gate during rotation drain/sleep instead of opening a new session early."""
    provider = cdn.make_provider()
    provider._min_rotation_bytes = 500
    provider._max_rotation_bytes = 500
    provider._rotation_threshold = 500
    provider._min_rotation_sleep = 0.05
    provider._max_rotation_sleep = 0.05

    sleep_entered = asyncio.Event()
    release_sleep = asyncio.Event()
    orig_sleep = asyncio.sleep

    async def gated_rotation_sleep(delay: float):
        # 仅拦截并门控轮转静默呼吸阶段，使并发读取窗口 100% 确定性可控
        sleep_entered.set()
        await release_sleep.wait()
        await orig_sleep(0)

    try:
        first = await provider.read_chunk(0, 600)
        assert len(first) == 600
        old_session = provider._session
        assert old_session is not None

        with patch("app.core.stream_chunk_provider.asyncio.sleep", side_effect=gated_rotation_sleep):
            # Start rotating read and wait deterministically until rotation pause starts
            t1 = asyncio.create_task(provider.read_chunk(600, 200))
            await asyncio.wait_for(sleep_entered.wait(), timeout=2.0)
            assert not provider._rotation_gate.is_set()

            # Concurrent read launched during rotation sleep must block behind _rotation_gate
            t2 = asyncio.create_task(provider.read_chunk(800, 200))
            await orig_sleep(0)
            await orig_sleep(0)
            assert not t2.done()
            assert provider._session is None

            release_sleep.set()
            res1, res2 = await asyncio.gather(t1, t2)
        assert len(res1) == 200 and len(res2) == 200
        assert provider._session is not None and provider._session is not old_session
    finally:
        release_sleep.set()
        await provider.aclose()


async def test_session_rotation_cancelled_during_drain_closes_old_session(cdn):
    provider = cdn.make_provider()
    old_session = await provider._ensure_session()
    provider._session_bytes_read = provider._rotation_threshold
    provider._session_in_flight[old_session] = 1
    drain_started = asyncio.Event()

    class PendingDrain:
        async def wait(self):
            drain_started.set()
            await asyncio.Event().wait()

    provider._session_drained_events[old_session] = PendingDrain()
    rotation = asyncio.create_task(provider._maybe_rotate_session())
    try:
        await asyncio.wait_for(drain_started.wait(), timeout=1)
        rotation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await rotation
        assert old_session.closed
        assert old_session not in provider._closing_sessions
        assert old_session not in provider._session_drained_events
        assert provider._rotation_gate.is_set()
    finally:
        rotation.cancel()
        await asyncio.gather(rotation, return_exceptions=True)
        await provider.aclose()


@pytest.mark.asyncio
async def test_fetch_range_rejects_mismatched_and_missing_content_range():
    """#9: 206 响应缺少或返回错位 Content-Range 时必须拒绝并抛 ChunkFetchTransientError。"""
    for bad_headers in [
        {"Content-Type": "application/octet-stream"},
        {"Content-Type": "application/octet-stream", "Content-Range": "bytes 0-511/1024"},
        {"Content-Type": "application/octet-stream", "Content-Range": "bytes 512-1023/9999"},
    ]:
        app = web.Application()

        async def handle(_request, hdrs=bad_headers):
            return web.Response(status=206, body=b"X" * 512, headers=hdrs)

        app.router.add_route("GET", "/{tail:.*}", handle)
        server = TestServer(app)
        await server.start_server()
        provider = PikPakStreamChunkProvider(
            file_id="cr_mismatch",
            file_name="cr.bin",
            total_bytes=1024,
            url_provider=AsyncMock(return_value=str(server.make_url("/file.bin"))),
        )
        try:
            with pytest.raises(ChunkFetchTransientError, match="Content-Range"):
                await provider.read_chunk(512, 512)
        finally:
            await provider.aclose()
            await server.close()


@pytest.mark.asyncio
async def test_fetch_range_rejects_oversized_206_body_without_full_buffering():
    """#10: 206 响应体超出请求 expected 字节数时流式拦截并抛 ChunkFetchTransientError。"""
    app = web.Application()

    async def handle(_request):
        resp = web.StreamResponse(
            status=206,
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Range": "bytes 0-99/1024",
            },
        )
        await resp.prepare(_request)
        await resp.write(b"Z" * (256 * 1024))
        await resp.write_eof()
        return resp

    app.router.add_route("GET", "/{tail:.*}", handle)
    server = TestServer(app)
    await server.start_server()
    provider = PikPakStreamChunkProvider(
        file_id="oversized_206",
        file_name="oversized.bin",
        total_bytes=1024,
        url_provider=AsyncMock(return_value=str(server.make_url("/file.bin"))),
    )
    try:
        with pytest.raises(ChunkFetchTransientError, match="超长"):
            await provider.read_chunk(0, 100)
    finally:
        await provider.aclose()
        await server.close()
