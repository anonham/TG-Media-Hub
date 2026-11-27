"""Unit & Contract tests for LocalPagedRangeProxy.

Verifies:
1. coalesce_byte_spans: merging adjacent/small-gap byte spans up to 2 MiB limit, handling nested & negative spans.
2. LocalPagedRangeProxy lifecycle, token authorization, port binding, HEAD requests, and 416/400 errors.
3. L0 Pinned Metadata Cache: 100% memory hit for rear moov and head_bytes, zero remote network fetches for unbounded Range: bytes=0-.
4. L1 Ring Cache: forward micro-jump (49 KiB) and backward seek (2.2 MiB) zero reconnect/disconnect hit.
5. LRU page eviction and capacity limits per channel.
6. Single-Flight Concurrency: multiple simultaneous requests for same page share a single provider fetch.
7. Single-Flight Cancellation: cancellation of initiator does not deadlock other concurrent waiters.
8. Sequential prefetch trigger on sequential page access.
9. Tier 2 Audio Channel: audio_spans bounded fetches with zero video gap bytes downloaded.
10. Multi-page continuous streaming via StreamResponse.
11. Real FFmpeg demuxing and audio extraction over 127.0.0.1 proxy.
12. Edge cases: suffix range, single byte range, 0-byte empty file, boundary crossing, provider error recovery, and high-frequency alternating seeks.
"""

from __future__ import annotations

import asyncio
import os
import random
import shutil
import subprocess
from unittest.mock import MagicMock

import aiohttp
import pytest
from _async_wait import wait_until

from app.core.stream_chunk_provider import BaseChunkProvider, ChunkProviderError
from app.core.stream_readahead_proxy import LocalPagedRangeProxy, coalesce_byte_spans


class MockMemoryChunkProvider(BaseChunkProvider):
    """用于测试的内存分块提供者, 记录所有 read_chunk 调用的偏移与大小。"""

    def __init__(
        self,
        data: bytes,
        file_id: str = "mock_file_id",
        file_name: str = "mock.mp4",
        read_delay: float = 0.0,
        entered_event: asyncio.Event | None = None,
        gate_event: asyncio.Event | None = None,
    ) -> None:
        self.data = data
        self.file_id = file_id
        self.file_name = file_name
        self.total_bytes = len(data)
        self.read_delay = read_delay
        self.entered_event = entered_event
        self.gate_event = gate_event
        self.fetch_history: list[tuple[int, int]] = []
        self.call_count = 0

    async def read_chunk(self, offset: int, size: int) -> bytes:
        self.call_count += 1
        self.fetch_history.append((offset, size))
        if self.entered_event is not None:
            self.entered_event.set()
        if self.gate_event is not None:
            await self.gate_event.wait()
        if self.read_delay > 0:
            await asyncio.sleep(self.read_delay)
        if offset >= self.total_bytes:
            raise ValueError(f"offset {offset} >= total_bytes {self.total_bytes}")
        if size <= 0:
            raise ValueError(f"size {size} <= 0")
        return self.data[offset : min(self.total_bytes, offset + size)]


# =====================================================================
# 1. coalesce_byte_spans 纯算法测试
# =====================================================================


def test_coalesce_byte_spans_empty_and_invalid():
    assert coalesce_byte_spans([]) == []
    # 无效区间 (start >= end) 自动过滤
    assert coalesce_byte_spans([(100, 100), (200, 150)]) == []
    # 负数偏移过滤
    assert coalesce_byte_spans([(-10, 50)]) == []


def test_coalesce_byte_spans_small_gaps_merged():
    # 间距 20 字节 <= 64 KiB, 合并为单个 span
    spans = [(0, 100), (120, 200)]
    assert coalesce_byte_spans(spans, max_gap=64 * 1024) == [(0, 200)]

    # 乱序输入自动先排序
    spans = [(120, 200), (0, 100)]
    assert coalesce_byte_spans(spans, max_gap=64 * 1024) == [(0, 200)]


def test_coalesce_byte_spans_large_gap_not_merged():
    # 间距 70 KiB > 64 KiB, 不合并
    spans = [(0, 100), (70 * 1024 + 100, 70 * 1024 + 200)]
    result = coalesce_byte_spans(spans, max_gap=64 * 1024)
    assert len(result) == 2
    assert result[0] == (0, 100)
    assert result[1] == (70 * 1024 + 100, 70 * 1024 + 200)


def test_coalesce_byte_spans_max_span_size_limit():
    # 间距 0, 但合并后大小 2 MiB + 100 字节 > 2 MiB, 停止合并
    max_size = 2 * 1024 * 1024
    spans = [(0, max_size), (max_size, max_size + 100)]
    result = coalesce_byte_spans(spans, max_gap=64 * 1024, max_span_size=max_size)
    assert len(result) == 2
    assert result[0] == (0, max_size)
    assert result[1] == (max_size, max_size + 100)


def test_coalesce_byte_spans_nested_and_overlapping():
    # 包含关系区间: (0, 100) 包含 (10, 50)
    spans = [(0, 100), (10, 50)]
    assert coalesce_byte_spans(spans) == [(0, 100)]

    # 重叠关系: (0, 100) 与 (50, 150)
    spans = [(0, 100), (50, 150)]
    assert coalesce_byte_spans(spans) == [(0, 150)]


# =====================================================================
# 2. LocalPagedRangeProxy 生命周期与安全鉴权测试
# =====================================================================


@pytest.mark.asyncio
async def test_proxy_lifecycle_and_security():
    data = b"0123456789" * 1000  # 10,000 bytes
    provider = MockMemoryChunkProvider(data)

    async with LocalPagedRangeProxy(provider) as proxy:
        assert proxy.port > 0
        assert proxy.video_url.startswith("http://127.0.0.1:")
        assert proxy.video_url.endswith("/video")
        assert proxy.audio_url.endswith("/audio")
        assert proxy.main_url.endswith("/main")
        assert proxy.file_id == "mock_file_id"
        assert proxy.total_bytes == 10000

        async with aiohttp.ClientSession() as session:
            # 1. 合法请求 (无 Range 头顺读完整数据)
            async with session.get(proxy.main_url) as resp:
                assert resp.status == 200
                body = await resp.read()
                assert body == data

            # 2. 伪造 session_token 应返回 403 Forbidden
            bad_url = f"http://127.0.0.1:{proxy.port}/invalid_token/main"
            async with session.get(bad_url) as resp:
                assert resp.status == 403

            # 3. HEAD 请求测试
            async with session.head(proxy.main_url) as resp:
                assert resp.status == 200
                assert resp.headers["Content-Length"] == "10000"
                assert resp.headers["Accept-Ranges"] == "bytes"
                head_body = await resp.read()
                assert head_body == b""

            # 4. Range 越界测试 (416)
            async with session.get(proxy.main_url, headers={"Range": "bytes=15000-"}) as resp:
                assert resp.status == 416
                assert "Content-Range" in resp.headers

            # 5. 畸形 Range 测试 (400)
            async with session.get(proxy.main_url, headers={"Range": "invalid_range"}) as resp:
                assert resp.status == 400

    # 退出上下文后 close 幂等调用不报错
    await proxy.close()


# =====================================================================
# 3. L0 Pinned Metadata Cache: moov 与 head 零网络命中测试
# =====================================================================


@pytest.mark.asyncio
async def test_l0_moov_zero_network_hit():
    total_len = 50 * 1024 * 1024  # 50 MiB 模拟大文件
    raw_data = bytearray(total_len)

    # 注入 Stage 0 已知元数据
    head_bytes = b"FTYP_HEADER_BOX_PAYLOAD_128K" * 100  # 2.8 KB head
    moov_bytes = b"MOOV_METADATA_BOX_PAYLOAD_50K" * 50  # 1.45 KB moov at tail
    moov_offset = total_len - len(moov_bytes)

    raw_data[0 : len(head_bytes)] = head_bytes
    raw_data[moov_offset : moov_offset + len(moov_bytes)] = moov_bytes

    provider = MockMemoryChunkProvider(bytes(raw_data))

    pinned_regions = [
        (0, head_bytes),
        (moov_offset, moov_bytes),
    ]

    async with LocalPagedRangeProxy(provider, pinned_regions=pinned_regions) as proxy:
        async with aiohttp.ClientSession() as session:
            # 1. 模拟 FFmpeg 启动探测头部: 发起真实的无界 Range: bytes=0-
            async with session.get(proxy.video_url, headers={"Range": "bytes=0-"}) as resp:
                assert resp.status == 206
                assert resp.headers["Content-Range"] == f"bytes 0-{len(head_bytes) - 1}/{total_len}"
                body = await resp.read()
                assert body == head_bytes
                assert proxy.l0_hits == 1
                assert provider.call_count == 0  # 0 次远端拉取

            # 2. 模拟探测头部有界切片: Range: bytes=0-999
            async with session.get(proxy.video_url, headers={"Range": "bytes=0-999"}) as resp:
                assert resp.status == 206
                body = await resp.read()
                assert body == head_bytes[:1000]
                assert proxy.l0_hits == 2
                assert provider.call_count == 0  # 0 次远端拉取

            # 3. 模拟 FFmpeg Seek 到文件末尾读取 moov 盒子: Range: bytes={moov_offset}-
            async with session.get(proxy.video_url, headers={"Range": f"bytes={moov_offset}-"}) as resp:
                assert resp.status == 206
                body = await resp.read()
                assert body == moov_bytes
                assert proxy.l0_hits >= 3
                assert provider.call_count == 0  # 依然 0 次远端拉取!

    # 整个生命周期内对 head 和 moov 的读取 100% 内存命中
    assert proxy.remote_fetches == 0


# =====================================================================
# 4. L1 Ring Cache: 正向微跳 (49 KiB) 与反向回退 (2.2 MiB) 零断连测试
# =====================================================================


@pytest.mark.asyncio
async def test_l1_forward_micro_jump_and_backward_seek():
    total_len = 10 * 1024 * 1024  # 10 MiB
    data = os.urandom(total_len)
    provider = MockMemoryChunkProvider(data)

    page_size = 2 * 1024 * 1024  # 2 MiB 固定页

    async with (
        LocalPagedRangeProxy(provider, page_size=page_size, max_pages_per_channel=4, enable_prefetch=False) as proxy,
        aiohttp.ClientSession() as session,
    ):
        # 1. 首次读取 page 0 范围内的微小切片 (offset 100,000 ~ 120,000)
        async with session.get(proxy.video_url, headers={"Range": "bytes=100000-119999"}) as resp:
            assert resp.status == 206
            body = await resp.read()
            assert body == data[100000:120000]
            assert provider.call_count == 1  # 产生 1 次 2 MiB 有界拉取
            assert provider.fetch_history[0] == (0, page_size)

        # 2. 正向微跳 49 KiB (从 100,000 跳到 150,176, 仍在 page 0 内)
        jump_offset = 100000 + 49 * 1024  # 150,176
        async with session.get(proxy.video_url, headers={"Range": f"bytes={jump_offset}-{jump_offset + 9999}"}) as resp:
            assert resp.status == 206
            body = await resp.read()
            assert body == data[jump_offset : jump_offset + 10000]
            assert proxy.l1_hits >= 1
            assert provider.call_count == 1  # 零远端网络请求!

        # 3. 顺读进入 page 1 (offset 2.5 MiB ~ 2.6 MiB)
        p1_offset = int(2.5 * 1024 * 1024)
        async with session.get(proxy.video_url, headers={"Range": f"bytes={p1_offset}-{p1_offset + 9999}"}) as resp:
            assert resp.status == 206
            body = await resp.read()
            assert body == data[p1_offset : p1_offset + 10000]
            assert provider.call_count == 2  # 仅拉取 page 1 (2,097,152, 2,097,152)

        # 4. 反向回退 2.4 MiB (从 2.5 MiB 倒回 100,000)
        async with session.get(proxy.video_url, headers={"Range": "bytes=100000-119999"}) as resp:
            assert resp.status == 206
            body = await resp.read()
            assert body == data[100000:120000]
            # page 0 仍在 4 页 LRU 环形缓存中, 100% 内存命中!
            assert proxy.l1_hits >= 2
            assert provider.call_count == 2  # 零远端重连, 零新拉取!


# =====================================================================
# 5. L1 缓存 LRU 淘汰与通道隔离测试
# =====================================================================


@pytest.mark.asyncio
async def test_l1_lru_eviction_and_channel_isolation():
    page_size = 64 * 1024  # 64 KiB 页方便测试
    total_len = 10 * page_size
    data = os.urandom(total_len)
    provider = MockMemoryChunkProvider(data)

    async with (
        LocalPagedRangeProxy(provider, page_size=page_size, max_pages_per_channel=2, enable_prefetch=False) as proxy,
        aiohttp.ClientSession() as session,
    ):
        # video 通道访问 page 0, page 1
        await (await session.get(proxy.video_url, headers={"Range": f"bytes=0-{page_size - 1}"})).read()
        p1_start = page_size
        await (
            await session.get(proxy.video_url, headers={"Range": f"bytes={p1_start}-{p1_start + page_size - 1}"})
        ).read()
        assert provider.call_count == 2

        # 再次访问 page 0 (应命中)
        await (await session.get(proxy.video_url, headers={"Range": f"bytes=0-{page_size - 1}"})).read()
        assert provider.call_count == 2  # 命中, 未增加

        # 访问 page 2 -> 触发 LRU 淘汰最久未访问的 page 1 (因 page 0 刚刚访问过)
        p2_start = 2 * page_size
        await (
            await session.get(proxy.video_url, headers={"Range": f"bytes={p2_start}-{p2_start + page_size - 1}"})
        ).read()
        assert provider.call_count == 3

        # 此时访问 page 0 依然命中
        await (await session.get(proxy.video_url, headers={"Range": f"bytes=0-{page_size - 1}"})).read()
        assert provider.call_count == 3

        # 访问已被淘汰的 page 1 -> 触发重新拉取
        await (
            await session.get(proxy.video_url, headers={"Range": f"bytes={p1_start}-{p1_start + page_size - 1}"})
        ).read()
        assert provider.call_count == 4


# =====================================================================
# 6. 单飞去重 (Single-Flight Dedup) 并发测试
# =====================================================================


@pytest.mark.asyncio
async def test_single_flight_page_dedup():
    data = os.urandom(2 * 1024 * 1024)
    # 模拟 50ms 网络延迟
    provider = MockMemoryChunkProvider(data, read_delay=0.05)

    async with LocalPagedRangeProxy(provider) as proxy, aiohttp.ClientSession() as session:

        async def do_fetch():
            async with session.get(proxy.video_url, headers={"Range": "bytes=0-9999"}) as resp:
                assert resp.status == 206
                return await resp.read()

        # 5 个并发请求同时拉取同一未缓存页面
        results = await asyncio.gather(do_fetch(), do_fetch(), do_fetch(), do_fetch(), do_fetch())
        for res in results:
            assert res == data[:10000]

        # 单飞锁保证底层 Provider 仅被调用 1 次
        assert provider.call_count == 1


# =====================================================================
# 7. 单飞请求发起方取消时的鲁棒性测试 (防并发死锁)
# =====================================================================


@pytest.mark.asyncio
async def test_single_flight_cancellation_resilience():
    """验证当创建 in-flight Task 的发起方协程中途被取消时，并发等待者不会死锁并能成功拿到数据。"""
    data = os.urandom(1024 * 1024)
    fetch_entered = asyncio.Event()
    fetch_gate = asyncio.Event()
    provider = MockMemoryChunkProvider(data, entered_event=fetch_entered, gate_event=fetch_gate)

    async with LocalPagedRangeProxy(provider, page_size=64 * 1024) as proxy:
        state = proxy._get_channel_state("video")

        # 协程 1 发起拉取 page 0 并确定性阻塞在底层 read_chunk 上
        task1 = asyncio.create_task(proxy._get_page_bytes(state, 0))
        await asyncio.wait_for(fetch_entered.wait(), timeout=2.0)

        # 协程 2 作为并发等待者加入同一 in-flight 请求
        task2 = asyncio.create_task(proxy._get_page_bytes(state, 0))
        await asyncio.sleep(0)

        # 取消协程 1 后放行底层拉取
        task1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task1
        fetch_gate.set()

        # 协程 2 必须在合理时间内成功拿到数据，绝不能永久挂起死锁
        res2 = await asyncio.wait_for(task2, timeout=1.0)
        assert res2 == data[: 64 * 1024]
        assert len(state.in_flight) == 0


# =====================================================================
# 8. 顺序读后台单飞预读 (Sequential Prefetch) 测试
# =====================================================================


@pytest.mark.asyncio
async def test_sequential_prefetch():
    page_size = 64 * 1024
    total_len = 5 * page_size
    data = os.urandom(total_len)
    provider = MockMemoryChunkProvider(data, read_delay=0.01)

    async with LocalPagedRangeProxy(provider, page_size=page_size, max_pages_per_channel=4) as proxy:
        async with aiohttp.ClientSession() as session:
            # 读 page 0
            await (await session.get(proxy.video_url, headers={"Range": f"bytes=0-{page_size - 1}"})).read()
            assert provider.call_count == 1

            # 读 page 1 (检测到 prev_page=0, page_idx=1 顺序读, 自动后台预读 page 2)
            p1_start = page_size
            await (
                await session.get(proxy.video_url, headers={"Range": f"bytes={p1_start}-{p1_start + page_size - 1}"})
            ).read()
            assert provider.call_count >= 2

            # 轮询等待后台预读协程完成 (避免固定 50ms sleep 在慢 CI 上竞态失败)
            await wait_until(
                lambda: provider.call_count >= 3,
                timeout_s=2.0,
                message="page 2 background prefetch did not finish",
            )

            # 此时 page 2 已经被后台拉取进缓存
            assert provider.call_count == 3

            # 正式请求 page 2, 应立即命中内存
            p2_start = 2 * page_size
            resp = await session.get(proxy.video_url, headers={"Range": f"bytes={p2_start}-{p2_start + page_size - 1}"})
            body = await resp.read()
            assert body == data[p2_start : p2_start + page_size]
            # 顺序读 page 2 会触发 page 3 预读
            await wait_until(
                lambda: provider.call_count >= 4,
                timeout_s=2.0,
                message="page 3 background prefetch did not finish",
            )
            assert provider.call_count == 4


# =====================================================================
# 9. Tier 2 音频通道: audio_spans 有界拉取与视频间隙 0 下载测试
# =====================================================================


@pytest.mark.asyncio
async def test_audio_channel_zero_video_gap_download():
    total_len = 10 * 1024 * 1024
    data = os.urandom(total_len)
    provider = MockMemoryChunkProvider(data)

    audio_spans = [
        (0, 50000),
        (2000000, 2050000),
        (5000000, 5080000),
    ]

    async with LocalPagedRangeProxy(provider, audio_spans=audio_spans) as proxy, aiohttp.ClientSession() as session:
        # 1. FFmpeg 发起无界 Range 请求获取 Chunk 0 音频: Range: bytes=0-
        async with session.get(proxy.audio_url, headers={"Range": "bytes=0-"}) as resp:
            assert resp.status == 206
            assert resp.headers["Content-Range"] == f"bytes 0-49999/{total_len}"
            body = await resp.read()
            assert len(body) == 50000
            assert body == data[0:50000]

        # 2. FFmpeg Seek 跳跃到 Chunk 1 音频发起请求: Range: bytes=2000000-
        async with session.get(proxy.audio_url, headers={"Range": "bytes=2000000-"}) as resp:
            assert resp.status == 206
            assert resp.headers["Content-Range"] == f"bytes 2000000-2049999/{total_len}"
            body = await resp.read()
            assert len(body) == 50000
            assert body == data[2000000:2050000]

        # 3. FFmpeg Seek 跳跃到 Chunk 2 音频: Range: bytes=5000000-
        async with session.get(proxy.audio_url, headers={"Range": "bytes=5000000-"}) as resp:
            assert resp.status == 206
            assert resp.headers["Content-Range"] == f"bytes 5000000-5079999/{total_len}"
            body = await resp.read()
            assert len(body) == 80000
            assert body == data[5000000:5080000]

    # 核心断言: 校验 Provider 实际拉取的字节区间
    total_fetched_bytes = sum(size for _, size in provider.fetch_history)
    expected_audio_bytes = 50000 + 50000 + 80000  # 180,000 字节
    assert total_fetched_bytes == expected_audio_bytes

    # 验证视频大间隙 [50000, 2000000) 与 [2050000, 5000000) 绝对零下载!
    for offset, size in provider.fetch_history:
        end = offset + size
        assert not (offset < 2000000 and end > 50000)
        assert not (offset < 5000000 and end > 2050000)


# =====================================================================
# 10. 音频通道配置 audio_spans 时通道 fallback 页大小契约测试
# =====================================================================


@pytest.mark.asyncio
async def test_audio_channel_fallback_page_size_with_spans():
    """验证配置 audio_spans 时，落在 span 之外的读取严格遵循 audio_page_size (128 KiB) 而非 2 MiB。"""
    total_len = 10 * 1024 * 1024
    data = os.urandom(total_len)
    provider = MockMemoryChunkProvider(data)

    # 设定跨度从 200 KiB 开始，前 200 KiB 为视频
    audio_spans = [(200 * 1024, 250 * 1024)]
    audio_page_size = 32 * 1024

    async with LocalPagedRangeProxy(provider, audio_spans=audio_spans, audio_page_size=audio_page_size) as proxy:
        async with aiohttp.ClientSession() as session:
            # 访问 offset 0 (在 span 之外)
            async with session.get(proxy.audio_url, headers={"Range": "bytes=0-"}) as resp:
                assert resp.status == 206
                # 拉取大小应当受到 audio_page_size (32 KiB) 严格限制
                assert resp.headers["Content-Length"] == str(audio_page_size)
                assert provider.fetch_history[0][1] == audio_page_size


# =====================================================================
# 11. 跨多页流式拉取 (StreamResponse) 测试
# =====================================================================


@pytest.mark.asyncio
async def test_multipage_streaming_response():
    page_size = 64 * 1024
    total_len = 4 * page_size  # 256 KiB
    data = os.urandom(total_len)
    provider = MockMemoryChunkProvider(data)

    async with LocalPagedRangeProxy(provider, page_size=page_size) as proxy, aiohttp.ClientSession() as session:
        # 请求跨越整整 4 页的大范围: Range: bytes=1000-200000
        async with session.get(proxy.video_url, headers={"Range": "bytes=1000-200000"}) as resp:
            assert resp.status == 206
            assert resp.headers["Content-Range"] == f"bytes 1000-200000/{total_len}"
            body = await resp.read()
            assert len(body) == 200000 - 1000 + 1
            assert body == data[1000:200001]


# =====================================================================
# 12. 真实 FFmpeg 本地解复用与音频提取集成测试
# =====================================================================


@pytest.mark.asyncio
async def test_real_ffmpeg_proxy_integration(tmp_path):
    if shutil.which("ffmpeg") is None:
        pytest.skip("FFmpeg 未安装, 跳过真实命令行集成测试")

    # 生成测试用的合法标准 MP4 (含 1 秒 H264 视频 + 1 秒 AAC 音频)
    test_mp4_path = str(tmp_path / "fixture.mp4")
    cmd_gen = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=duration=1:size=160x120:rate=10",
        "-f",
        "lavfi",
        "-i",
        "sine=duration=1:frequency=1000",
        "-c:v",
        "libx264",
        "-c:a",
        "aac",
        "-movflags",
        "+faststart",
        test_mp4_path,
    ]
    subprocess.run(cmd_gen, capture_output=True, check=True)

    with open(test_mp4_path, "rb") as f:
        file_bytes = f.read()
    provider = MockMemoryChunkProvider(file_bytes)

    async with LocalPagedRangeProxy(provider, page_size=16 * 1024) as proxy:
        # 1. 验证 FFmpeg 解复用 video_url (全流正常解码)
        proc_v = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            proxy.video_url,
            "-f",
            "null",
            "-",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, err_v = await proc_v.communicate()
        assert proc_v.returncode == 0, f"FFmpeg video 解复用失败: {err_v.decode()}"

        # 2. 验证 FFmpeg 仅提取音频: -vn -sn -dn
        proc_a = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-vn",
            "-sn",
            "-dn",
            "-i",
            proxy.audio_url,
            "-f",
            "null",
            "-",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, err_a = await proc_a.communicate()
        assert proc_a.returncode == 0, f"FFmpeg audio 提取失败: {err_a.decode()}"


# =====================================================================
# 13. 边界情况: 后缀 Range、单字节 Range 与边界读取
# =====================================================================


@pytest.mark.asyncio
async def test_edge_case_suffix_and_single_byte_range():
    data = b"0123456789ABCDEF" * 100  # 1600 bytes
    provider = MockMemoryChunkProvider(data)

    async with LocalPagedRangeProxy(provider, page_size=256) as proxy, aiohttp.ClientSession() as session:
        # 1. Suffix Range: bytes=-50 (最后 50 字节)
        async with session.get(proxy.main_url, headers={"Range": "bytes=-50"}) as resp:
            assert resp.status == 206
            assert resp.headers["Content-Range"] == "bytes 1550-1599/1600"
            body = await resp.read()
            assert body == data[-50:]

        # 2. 单字节 Range: bytes=0-0
        async with session.get(proxy.main_url, headers={"Range": "bytes=0-0"}) as resp:
            assert resp.status == 206
            assert resp.headers["Content-Range"] == "bytes 0-0/1600"
            body = await resp.read()
            assert body == data[0:1]

        # 3. 最后一个字节: bytes=1599-1599
        async with session.get(proxy.main_url, headers={"Range": "bytes=1599-1599"}) as resp:
            assert resp.status == 206
            assert resp.headers["Content-Range"] == "bytes 1599-1599/1600"
            body = await resp.read()
            assert body == data[1599:1600]

        # 4. 后缀为 0 应返回 416
        async with session.get(proxy.main_url, headers={"Range": "bytes=-0"}) as resp:
            assert resp.status == 416


# =====================================================================
# 14. 边界情况: 0 字节特殊空文件
# =====================================================================


@pytest.mark.asyncio
async def test_edge_case_zero_byte_empty_file():
    provider = MockMemoryChunkProvider(b"")

    async with LocalPagedRangeProxy(provider) as proxy, aiohttp.ClientSession() as session:
        # 1. GET 无 Range 应返回 200, Content-Length=0, 空字节
        async with session.get(proxy.main_url) as resp:
            assert resp.status == 200
            assert resp.headers["Content-Length"] == "0"
            body = await resp.read()
            assert body == b""

        # 2. HEAD 无 Range 应返回 200, Content-Length=0
        async with session.head(proxy.main_url) as resp:
            assert resp.status == 200
            assert resp.headers["Content-Length"] == "0"

        # 3. GET 带 Range 应返回 416
        async with session.get(proxy.main_url, headers={"Range": "bytes=0-0"}) as resp:
            assert resp.status == 416
            assert resp.headers["Content-Range"] == "bytes */0"


# =====================================================================
# 15. 边界情况: Pinned 区域与非 Pinned 跨界拉取
# =====================================================================


@pytest.mark.asyncio
async def test_edge_case_pinned_and_unpinned_boundary_crossing():
    total_len = 10000
    data = os.urandom(total_len)
    head_bytes = data[:1000]  # Pinned [0, 1000)
    provider = MockMemoryChunkProvider(data)

    async with LocalPagedRangeProxy(provider, pinned_regions=[(0, head_bytes)], page_size=512) as proxy:
        async with aiohttp.ClientSession() as session:
            # 请求跨越 [500, 1500] (前半段命中 Pinned, 后半段由 Provider 供流)
            async with session.get(proxy.video_url, headers={"Range": "bytes=500-1500"}) as resp:
                assert resp.status == 206
                body = await resp.read()
                assert len(body) == 1001
                assert body == data[500:1501]
                assert proxy.l0_hits >= 1


# =====================================================================
# 16. 异常恢复: Provider 报错时不锁死单飞 Future 并支持后续正常请求
# =====================================================================


class FailingChunkProvider(BaseChunkProvider):
    def __init__(self, data: bytes):
        self.data = data
        self.file_id = "failing_id"
        self.total_bytes = len(data)
        self.should_fail = True

    async def read_chunk(self, offset: int, size: int) -> bytes:
        if self.should_fail:
            raise ChunkProviderError("Simulated remote failure")
        return self.data[offset : offset + size]


@pytest.mark.asyncio
async def test_edge_case_provider_error_recovery():
    data = os.urandom(4096)
    provider = FailingChunkProvider(data)

    async with LocalPagedRangeProxy(provider, page_size=1024) as proxy, aiohttp.ClientSession() as session:
        # 1. 触发 Provider 异常 (返回 500)
        async with session.get(proxy.video_url, headers={"Range": "bytes=0-100"}) as resp:
            assert resp.status == 500

        # 2. 恢复 Provider 正常状态后，重试拉取必须成功 (验证 in_flight 未残留死锁)
        provider.should_fail = False
        async with session.get(proxy.video_url, headers={"Range": "bytes=0-100"}) as resp:
            assert resp.status == 206
            body = await resp.read()
            assert body == data[:101]


# =====================================================================
# 17. 攻击测试: 慢速 Provider 下高频交替前后 Seek 并发测试
# =====================================================================


@pytest.mark.asyncio
async def test_high_frequency_random_seeks_slow_provider():
    """验证当底层网络延迟较大时，高频前后交替 Seek 不会发生死锁、漏页或任务泄漏。"""
    total_len = 8 * 1024 * 1024  # 8 MiB
    data = os.urandom(total_len)
    provider = MockMemoryChunkProvider(data, read_delay=0.01)

    async with LocalPagedRangeProxy(provider, page_size=256 * 1024, max_pages_per_channel=4) as proxy:
        async with aiohttp.ClientSession() as session:

            async def do_random_seek(idx: int):
                # 随机生成前后跳转的 Range 请求
                s = random.randint(0, total_len - 1000)
                e = min(total_len - 1, s + random.randint(10, 5000))
                async with session.get(proxy.video_url, headers={"Range": f"bytes={s}-{e}"}) as resp:
                    assert resp.status == 206
                    body = await resp.read()
                    assert body == data[s : e + 1]

            # 20 个高并发交替 seek 任务
            tasks = [do_random_seek(i) for i in range(20)]
            await asyncio.gather(*tasks)


# =====================================================================
# 18. Tier 2 音频 Span 滑动窗口并发预读 (Concurrent Span Prefetch) 测试
# =====================================================================


@pytest.mark.asyncio
async def test_audio_span_sliding_window_prefetch():
    """验证访问首个音频 Span 时自动触发后续 span_prefetch_window 个音频区间的并发预读，掩盖跨洋 RTT。"""
    total_len = 2 * 1024 * 1024
    data = os.urandom(total_len)
    # 模拟 40ms 单次跨洋 RTT
    provider = MockMemoryChunkProvider(data, read_delay=0.04)

    # 构造 10 个离散的 4 KiB 音频 Span (间隔 64 KiB 视频间隙)
    audio_spans = [(i * 65536, i * 65536 + 4096) for i in range(10)]

    async with (
        LocalPagedRangeProxy(
            provider,
            audio_spans=audio_spans,
            span_prefetch_window=6,
            max_cached_spans=16,
            enable_prefetch=True,
        ) as proxy,
        aiohttp.ClientSession() as session,
    ):
        # 仅请求第 0 个音频 Span
        async with session.get(proxy.audio_url, headers={"Range": "bytes=0-4095"}) as resp:
            assert resp.status == 206
            body = await resp.read()
            assert body == data[0:4096]

        # 有界轮询等待后台并发预读将后续 6 个 Span (Span 1..6) 全部拉入缓存
        await wait_until(
            lambda: provider.call_count >= 7 and not proxy._span_in_flight,
            timeout_s=2.0,
            message=f"span prefetch did not complete; call_count= {provider.call_count}",
        )
        assert provider.call_count == 7  # Span 0 + 预读 Span 1..6

        # 此时顺序读取 Span 1..6 应 100% 命中 L1 Span 缓存
        initial_l1_hits = proxy.l1_hits
        for i in range(1, 7):
            s_start, s_end = audio_spans[i]
            async with session.get(proxy.audio_url, headers={"Range": f"bytes={s_start}-{s_end - 1}"}) as resp:
                assert resp.status == 206
                body = await resp.read()
                assert body == data[s_start:s_end]
        assert proxy.l1_hits >= initial_l1_hits + 6


@pytest.mark.asyncio
async def test_allowed_channels_blocks_non_audio_channel():
    """Verify allowed_channels=('audio',) restricts access to non-audio channels."""
    data = os.urandom(64 * 1024)
    provider = MockMemoryChunkProvider(data)

    async with (
        LocalPagedRangeProxy(
            provider, audio_spans=[(0, 1024)], zero_fill_unmapped=True, allowed_channels=("audio",)
        ) as proxy,
        aiohttp.ClientSession() as session,
    ):
        async with session.get(proxy.audio_url, headers={"Range": "bytes=0-1023"}) as resp:
            assert resp.status == 206
            assert await resp.read() == data[:1024]

        async with session.get(proxy.main_url, headers={"Range": "bytes=0-1023"}) as resp:
            assert resp.status == 403

        async with session.get(proxy.base_url, headers={"Range": "bytes=0-1023"}) as resp:
            assert resp.status == 403


@pytest.mark.asyncio
async def test_proxy_close_blocks_new_tasks_and_requests():
    """验证 LocalPagedRangeProxy.close() 后拒绝新建拉取任务并安全清理后台任务。"""
    data = os.urandom(64 * 1024)
    provider = MockMemoryChunkProvider(data)
    proxy = LocalPagedRangeProxy(provider, page_size=4096)
    await proxy.start()
    await proxy.close()

    assert proxy._closed is True
    assert not proxy._background_tasks

    state = proxy._get_channel_state("main")
    with pytest.raises(asyncio.CancelledError):
        await proxy._get_page_bytes(state, 0)
    with pytest.raises(asyncio.CancelledError):
        await proxy._get_span_bytes(0, 1024)
    proxy._maybe_trigger_prefetch(state, 0)
    assert not proxy._background_tasks

    resp = await proxy._handle_request(MagicMock())
    assert resp.status == 503


@pytest.mark.asyncio
async def test_proxy_promotes_expired_link_error_over_prior_transient_error():
    """先前发生瞬态错误后再次发生 PikPakLinkExpiredError 时, 必须覆写并保留直链过期异常。"""
    from app.core.stream_chunk_provider import ChunkFetchTransientError, PikPakLinkExpiredError

    class SequenceErrorProvider(BaseChunkProvider):
        file_id = "seq_err"
        file_name = "seq.mp4"
        total_bytes = 8192

        def __init__(self) -> None:
            self.calls = 0

        async def read_chunk(self, offset: int, size: int) -> bytes:
            self.calls += 1
            if self.calls == 1:
                raise ChunkFetchTransientError("transient read glitch")
            raise PikPakLinkExpiredError("cdn link expired")

    proxy = LocalPagedRangeProxy(SequenceErrorProvider(), page_size=4096)
    with pytest.raises(ChunkFetchTransientError):
        await proxy._read_provider_chunk(0, 4096)
    with pytest.raises(PikPakLinkExpiredError):
        await proxy._read_provider_chunk(4096, 4096)
    assert isinstance(proxy.last_provider_error, PikPakLinkExpiredError)


@pytest.mark.asyncio
async def test_proxy_close_preserves_caller_cancellation_after_cleanup(monkeypatch: pytest.MonkeyPatch):
    """外部在 runner.cleanup 期间取消 close() 时, 必须等待真实清理与尾部终扫完成再抛 CancelledError。"""
    cleanup_entered = asyncio.Event()
    allow_cleanup_finish = asyncio.Event()
    cleanup_completed = False

    proxy = LocalPagedRangeProxy(MockMemoryChunkProvider(b"x" * 8192), page_size=4096)
    await proxy.start()
    runner = proxy._runner
    assert runner is not None
    orig_cleanup = type(runner).cleanup

    async def barrier_cleanup(self_runner: object) -> None:
        nonlocal cleanup_completed
        if self_runner is runner:
            cleanup_entered.set()
            await allow_cleanup_finish.wait()
        await orig_cleanup(self_runner)  # type: ignore[arg-type]
        if self_runner is runner:
            cleanup_completed = True

    monkeypatch.setattr(type(runner), "cleanup", barrier_cleanup)

    close_task = asyncio.create_task(proxy.close())
    try:
        await asyncio.wait_for(cleanup_entered.wait(), timeout=2.0)
        close_task.cancel()
        await asyncio.sleep(0)
        assert not cleanup_completed
        allow_cleanup_finish.set()
        with pytest.raises(asyncio.CancelledError):
            await close_task
        assert cleanup_completed is True
        assert proxy._runner is None
        assert proxy._site is None
        assert proxy._app is None
        assert not proxy._background_tasks
    finally:
        allow_cleanup_finish.set()
        if not close_task.done():
            close_task.cancel()
        await asyncio.gather(close_task, return_exceptions=True)
