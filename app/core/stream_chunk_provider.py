"""PikPak 零媒体落盘链路的数据面抽象 (Phase 1 基座)。

BaseChunkProvider: 上传引擎 upload_file_parallel 的内存分块读取接口
(两参签名 read_chunk(offset, size) 以 , part_id 可由
offset // CHUNK_SIZE 派生)。实现方:
- LocalFileChunkProvider: 包装真实本地文件 (字节语义与 seek+read 完全一致), 供测试
  与存量路径平滑迁移;
- PikPakStreamChunkProvider: PikPak CDN HTTP Range 内存流 (403/410 单飞换链 + 风控页拒绝)。

零媒体落盘约束 (生产模式): Provider 读流全程内存; 磁盘只允许出现 KB 级 .upload.meta JSON。
显式例外: `E2E_CAPTURE_MODE` 开启时按 `settings.E2E_CAPTURE_DIR` 留存取证副本
(app/core/e2e_capture.py, 生产模式承诺不变)。

模块级约束: 禁止 import app.core.fast_telethon (引擎单向依赖本模块, 反向导入即循环)。
"""

import asyncio
import collections
import email.utils
import logging
import os
import random
import re
import time
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

import aiohttp

logger = logging.getLogger(__name__)

_CONTENT_RANGE_RE = re.compile(r"^bytes\s+(\d+)-(\d+)/(\d+|\*)$", re.IGNORECASE)
_RANGE_READ_CHUNK_BYTES = 64 * 1024


class BaseChunkProvider(ABC):
    """上传引擎分块读取抽象: 引擎经 read_chunk(offset, size) 按需取内存字节, 任何媒体字节不落盘。"""

    file_id: str  # PikPak 远端文件标识 (防重/日志用; 与 MTProto 上传的数字 file_id 无关)
    total_bytes: int  # 字节级总大小 (来自 ls --raw 的 size; Local 实现取 os.path.getsize)
    file_name: str  # 展示名 (传给 Telegram 的媒体文件名)

    @abstractmethod
    async def read_chunk(self, offset: int, size: int) -> bytes:
        """读取 [offset, offset+size) 区间字节。

        字节约定: offset+size 允许超出 total_bytes, 此时返回截断到文件尾的实际字节
        (与 seek+read 语义一致); offset >= total_bytes 或 size <= 0 抛 ValueError。
        """

    async def aclose(self) -> None:
        """释放持有的资源 (非抽象默认空实现): 由创建方或外层引擎在 finally 中统一调用，不在 upload_file_parallel 内部关闭。"""
        return


class LocalFileChunkProvider(BaseChunkProvider):
    """本地文件分块读取实现: 任意 (offset, size) 返回值与
    open(file_path, 'rb').seek(offset).read(size) 完全一致 (包C 字节等价测试的物理基础)。
    """

    def __init__(self, file_path: str, file_id: str | None = None, file_name: str | None = None):
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")
        self._file_path = file_path
        self.file_id = file_id if file_id is not None else file_path
        self.file_name = file_name if file_name is not None else os.path.basename(file_path)
        self.total_bytes = os.path.getsize(file_path)

    async def read_chunk(self, offset: int, size: int) -> bytes:
        if offset >= self.total_bytes:
            raise ValueError(f"读取偏移越界: offset={offset} >= total_bytes={self.total_bytes}")
        if size <= 0:
            raise ValueError(f"非法读取长度: size={size}")
        # 同步磁盘 seek+read 放入工作线程, 对齐引擎 _sync_read_chunk 的"事件循环零微冻结"语义
        data = await asyncio.to_thread(self._read_sync, offset, size)
        expected = min(size, self.total_bytes - offset)
        if len(data) < expected:
            raise OSError(f"文件读取不完整: 预期 {expected} 字节，实际读取 {len(data)} 字节")
        return data

    def _read_sync(self, offset: int, size: int) -> bytes:
        with open(self._file_path, "rb") as f:
            f.seek(offset)
            return f.read(size)


class SlicedChunkProvider(BaseChunkProvider):
    """虚拟分卷切片 Provider: 代理底层 Provider, 将其 [slice_offset, slice_offset + slice_length)
    逻辑切片映射为独立的零偏移 Provider (total_bytes == slice_length)。

    - offset 绝对映射: read_chunk(offset, size) -> inner.read_chunk(slice_offset + offset, clamped_size)
    - 边界保护: offset >= total_bytes 或 size <= 0 抛 ValueError; 超长读取自动截断到 total_bytes 尾部
    - 规范属性:
        - total_bytes: 本切片长度 (part.length), 供底层引擎计算分块数并拦截 >2000 MiB (1.95 GiB) / >4000 MiB (3.91 GiB) 溢出
        - file_id: 单卷保持 inner.file_id; 多卷派生为 f"{inner.file_id}_part{part_index:03d}"
        - file_name: 单卷保持 inner.file_name; 多卷派生为 f"{inner.file_name}.part{part_index:03d}"
    - 资源管理: aclose() 为 no-op (底层 ClientSession 由外层持有者统一关闭, 切片结束严禁误关底层连接)
    """

    def __init__(
        self,
        inner: BaseChunkProvider,
        offset: int,
        length: int,
        part_index: int = 1,
        total_parts: int = 1,
    ):
        if inner is None:
            raise ValueError("inner provider 不能为空")
        if offset < 0:
            raise ValueError(f"切片起始偏移不能为负数: offset={offset}")
        if length <= 0:
            raise ValueError(f"切片长度必须为正整数: length={length}")
        if offset + length > inner.total_bytes:
            raise ValueError(
                f"切片超出源文件范围: offset({offset}) + length({length}) = "
                f"{offset + length} > inner.total_bytes({inner.total_bytes})"
            )

        self._inner = inner
        self._slice_offset = offset
        self.total_bytes = length
        self._part_index = part_index
        self._total_parts = total_parts

        if total_parts <= 1:
            self.file_id = inner.file_id
            self.file_name = inner.file_name
        else:
            self.file_id = f"{inner.file_id}_part{part_index:03d}"
            self.file_name = f"{inner.file_name}.part{part_index:03d}"

    async def read_chunk(self, offset: int, size: int) -> bytes:
        if offset >= self.total_bytes:
            raise ValueError(f"读取偏移越界: offset={offset} >= total_bytes={self.total_bytes}")
        if size <= 0:
            raise ValueError(f"非法读取长度: size={size}")

        clamped_size = min(size, self.total_bytes - offset)
        data = await self._inner.read_chunk(self._slice_offset + offset, clamped_size)
        if len(data) > clamped_size:
            return data[:clamped_size]
        return data

    async def aclose(self) -> None:
        """分卷切片关闭为 no-op: 底层 Provider 连接由上层调度统一管理"""
        return


class SlidingWindowRingBuffer:
    """按 offset 键控的滑动窗口内存环 (架构 §三.1 L1 容灾缓存)。

    固定字节预算 (PIKPAK_RING_BUFFER_MB), 缓存"最近已发送"的分块; 超出预算按最旧
    offset 淘汰 (OrderedDict 队首弹出); 内存计费按实际缓存字节, RSS 增量硬上限 = 预算。
    """

    def __init__(self, budget_bytes: int):
        self._budget = max(0, int(budget_bytes))
        self._store: collections.OrderedDict[int, bytes] = collections.OrderedDict()
        self._bytes = 0

    def put(self, offset: int, data: bytes) -> None:
        if offset in self._store:
            self._bytes -= len(self._store[offset])
            del self._store[offset]
        self._store[offset] = data
        self._bytes += len(data)
        while self._bytes > self._budget and self._store:
            _, oldest = self._store.popitem(last=False)
            self._bytes -= len(oldest)

    def get(self, offset: int) -> bytes | None:
        """命中不弹出 (近期窗口语义): 允许同一块多次补发复用"""
        return self._store.get(offset)

    def clear(self) -> None:
        self._store.clear()
        self._bytes = 0

    @property
    def byte_size(self) -> int:
        """当前实际缓存字节数 (内存计费断言依赖)"""
        return self._bytes

    def __len__(self) -> int:
        return len(self._store)


# ---------------------------------------------------------------------------
# PikPak CDN HTTP Range 内存流实现
# ---------------------------------------------------------------------------


class ChunkProviderError(Exception):
    """Provider 数据面基础异常"""


class PikPakLinkExpiredError(ChunkProviderError):
    """直链过期 (HTTP 403/410) 且单飞换链重试后仍失败: 上层须重新走索链流程, Provider 不再自愈"""


class ChunkFetchTransientError(ChunkProviderError):
    """网络瞬断 (连接重置/超时/流中断/短读): 可重试错误, 由引擎侧 L1/L2 容灾处置"""


class PikPakRiskControlError(ChunkProviderError):
    """CDN 返回 text/html (风控/登录页特征): 不得把 HTML 当媒体读入"""


class PikPakNotFoundError(ChunkProviderError):
    """远端源文件不存在 (HTTP 404): 底层对象丢失或已删除，无法恢复"""


class PikPakThrottledError(ChunkProviderError):
    """CDN 节点频控/过载 (HTTP 429/502/503/504): 严禁微观重试风暴, 须触发长周期静默冷却"""

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        retry_after: float | None = None,
    ):
        self.status = status
        self.retry_after = retry_after
        super().__init__(message)


def _parse_retry_after(header_val: str | None) -> float | None:
    """解析 HTTP Retry-After 响应头 (支持秒数或 RFC 7231 HTTP-Date)。"""
    if not header_val:
        return None
    raw = str(header_val).strip()
    if not raw:
        return None
    try:
        val = float(raw)
        return max(0.0, val)
    except ValueError as num_err:
        logger.debug("Retry-After 非纯数字秒数, 尝试按 HTTP-Date 解析: %s", num_err)
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        if dt is not None:
            return max(0.0, dt.timestamp() - time.time())
    except (TypeError, ValueError, OverflowError) as dt_err:
        logger.debug("解析 Retry-After HTTP-Date 失败 (忽略): %s", dt_err)
    return None


class PikPakStreamChunkProvider(BaseChunkProvider):
    """PikPak CDN HTTP Range 内存流实现 (L2 补包器语义: 单块定向按需拉取)。

    - read_chunk: 对当前直链发 GET + Range, 期望 206, 响应体读入内存后返回, 任何字节不落盘;
      服务端不按 Range 返回 206 (含 200) 一律按可重试错误拒绝, 不读取大体响应体;
    - L3 单飞换链: 首个响应即 403/410 时, 经 asyncio.Lock + 代际计数 (generation) 触发
      url_provider 热刷新 —— 并发读块只允许一个刷新在途, 其余等待者复用新 URL 后重试本块;
      刷新后重试仍 403/410 → PikPakLinkExpiredError 上抛;
    - Content-Type 为 text/html → PikPakRiskControlError (风控/登录页, 不重试);
    - 其余网络错误 (连接重置/超时) → ChunkFetchTransientError, Provider 内部不做任何无限重试。

    安全红线: 日志/异常消息只含 file_id/offset/size/状态码, 不携带直链 URL (含签名参数)。
    URL 仅允许 http/https; 来源为 pikpak CLI 官方输出 (非用户输入), 内网地址校验由索链层负责。
    PIKPAK_STREAM_PREFETCH_MB 为引擎预读提示值, 本类不消费、不改引擎 (Phase 3 接线)。
    """

    def __init__(
        self,
        file_id: str,
        file_name: str,
        total_bytes: int,
        url_provider: Callable[[], Awaitable[str]],
        request_timeout: float = 60.0,
        initial_url: str | None = None,
        prefetch_concurrency: int | None = None,
        pool_limit_per_host: int | None = None,
    ):
        self.file_id = file_id
        self.file_name = file_name
        self.total_bytes = int(total_bytes)
        self._url_provider = url_provider
        self._request_timeout = request_timeout
        self._session: aiohttp.ClientSession | None = None
        self._closed = False
        self._current_url: str | None = initial_url
        self._url_generation = 1 if initial_url else 0
        self._refresh_lock = asyncio.Lock()
        self._pool_limit_per_host = pool_limit_per_host

        from app.config import settings

        # 超大文件 (> 2000 MiB (1.95 GiB)) 封顶 2 并发预取通道 (防爆内存与 CDN 限流, 允许在 [1, 2] 内响应用户设置)
        if self.total_bytes > settings.MAX_FILE_SIZE_BYTES:
            req_c = (
                prefetch_concurrency
                if prefetch_concurrency is not None
                else getattr(settings, "PIKPAK_PREFETCH_CONCURRENCY", 3)
            )
            self.prefetch_concurrency = min(2, max(1, int(req_c)))
        elif prefetch_concurrency is not None:
            self.prefetch_concurrency = max(1, int(prefetch_concurrency))
        else:
            self.prefetch_concurrency = getattr(settings, "PIKPAK_PREFETCH_CONCURRENCY", 3)

        # 单卷内长流主动微轮转状态 (规避 3.43 GiB 边缘单连接反盗链风控)
        self._session_bytes_read = 0
        self._rotation_lock = asyncio.Lock()
        self._rotation_gate = asyncio.Event()
        self._rotation_gate.set()
        self._session_lock = asyncio.Lock()
        self._session_in_flight: dict[aiohttp.ClientSession, int] = {}
        self._session_drained_events: dict[aiohttp.ClientSession, asyncio.Event] = {}
        self._closing_sessions: set[aiohttp.ClientSession] = set()
        self._min_rotation_bytes = int(1.2 * 1024 * 1024 * 1024)
        self._max_rotation_bytes = int(1.5 * 1024 * 1024 * 1024)
        self._min_rotation_sleep = 3.0
        self._max_rotation_sleep = 5.0
        self._rotation_threshold = self._next_rotation_threshold()

    def _next_rotation_threshold(self) -> int:
        return random.randint(self._min_rotation_bytes, self._max_rotation_bytes)

    async def _maybe_rotate_session(self) -> None:
        """单卷内长流主动微轮转 (规避 CDN 单连接 3.43 GiB 截断红线)。

        当当前 ClientSession 累计读取字节达到 1.2~1.5 GiB 随机阈值时，
        关闭读取门闩并主动优雅关闭当前 session，静默呼吸 3.0~5.0 秒，随后放行重建连接。
        """
        await self._rotation_gate.wait()
        if self._closed or self._session_bytes_read < self._rotation_threshold:
            return

        async with self._rotation_lock:
            if self._closed or self._session_bytes_read < self._rotation_threshold:
                return

            self._rotation_gate.clear()
            try:
                accumulated_gib = self._session_bytes_read / (1024**3)
                logger.info(
                    "🔄 [PikPakStreamChunkProvider] 单连接累计读取已达 %.2f GiB (阈值 %.2f GiB)，"
                    "主动微轮转 ClientSession 以规避 CDN 边缘 3.43 GiB 截断红线...",
                    accumulated_gib,
                    self._rotation_threshold / (1024**3),
                )
                old_session = self._session
                self._session = None
                self._session_bytes_read = 0
                self._rotation_threshold = self._next_rotation_threshold()

                if old_session is not None and not old_session.closed:
                    self._closing_sessions.add(old_session)
                    try:
                        # 等待所有仍在使用 old_session 的在途并发读块平滑完成（最多 3.0s），避免强关连接引发 ClientPayloadError
                        if self._session_in_flight.get(old_session, 0) > 0:
                            drained = self._session_drained_events.setdefault(old_session, asyncio.Event())
                            try:
                                await asyncio.wait_for(drained.wait(), timeout=3.0)
                            except TimeoutError:
                                logger.debug(
                                    "旧 ClientSession 等待在途读取排空超时 (remaining=%d)",
                                    self._session_in_flight.get(old_session, 0),
                                )
                        await old_session.close()
                    except Exception as close_err:
                        logger.debug("关闭旧 ClientSession 异常 (非致命): %s", close_err)
                    finally:
                        try:
                            if not old_session.closed:
                                await old_session.close()
                        finally:
                            self._closing_sessions.discard(old_session)
                            self._session_drained_events.pop(old_session, None)

                sleep_sec = random.uniform(self._min_rotation_sleep, self._max_rotation_sleep)
                logger.info("⏳ [PikPakStreamChunkProvider] 微轮转静默呼吸 %.2f 秒...", sleep_sec)
                await asyncio.sleep(sleep_sec)
            finally:
                self._rotation_gate.set()

    async def _ensure_session(self) -> aiohttp.ClientSession:
        await self._rotation_gate.wait()
        if self._closed:
            raise ChunkProviderError(f"Provider 已关闭, 不可复用 (file_id={self.file_id})")
        if self._session is None or self._session.closed:
            async with self._session_lock:
                if self._session is None or self._session.closed:
                    from app.config import settings

                    # 连接池收敛: 超大文件 (> 2000 MiB (1.95 GiB)) limit_per_host 收敛至 max(2, prefetch_concurrency)，普通文件保持 6/8
                    if self._pool_limit_per_host is not None:
                        limit_per_host = max(2, int(self._pool_limit_per_host))
                        limit = max(8, limit_per_host * 2)
                    elif self.total_bytes > settings.MAX_FILE_SIZE_BYTES:
                        limit_per_host = max(2, self.prefetch_concurrency)
                        limit = max(4, limit_per_host * 2)
                    else:
                        limit = 8
                        limit_per_host = 6
                    connector = aiohttp.TCPConnector(
                        limit=limit,
                        limit_per_host=limit_per_host,
                        keepalive_timeout=60.0,
                        ttl_dns_cache=300,
                    )
                    self._session = aiohttp.ClientSession(
                        connector=connector,
                        timeout=aiohttp.ClientTimeout(total=self._request_timeout),
                        trust_env=False,
                    )
        return self._session

    async def _fetch_url(self) -> "tuple[str, int]":
        """取当前直链与代际; 首次为空时经锁单飞索取 (JIT 索链)"""
        if self._current_url is None:
            async with self._refresh_lock:
                if self._current_url is None:
                    self._current_url = await self._url_provider()
                    self._url_generation += 1
        assert self._current_url is not None
        return self._current_url, self._url_generation

    async def _refresh_url(self, stale_generation: int) -> str:
        """L3 单飞换链: 仅当代际未变时真正刷新一次, 其余并发等待者直接复用新 URL"""
        async with self._refresh_lock:
            if self._url_generation == stale_generation:
                self._current_url = await self._url_provider()
                self._url_generation += 1
            assert self._current_url is not None
            return self._current_url

    def update_url(self, new_url: str) -> None:
        """从外部动态更新当前有效直链 (例如分卷超限重规划或 JIT 换链时)。"""
        self._current_url = new_url
        self._url_generation += 1

    async def read_chunk(self, offset: int, size: int) -> bytes:
        if offset >= self.total_bytes:
            raise ValueError(f"读取偏移越界: offset={offset} >= total_bytes={self.total_bytes}")
        if size <= 0:
            raise ValueError(f"非法读取长度: size={size}")

        await self._maybe_rotate_session()

        url, generation = await self._fetch_url()
        try:
            data = await self._fetch_range(url, offset, size)
        except PikPakLinkExpiredError as exp_err:
            logger.info("直链疑似过期 (代际=%s, err=%s)，触发单飞刷新并重试一次", generation, exp_err)
            # L3: 单飞换链后本块仅重试一次; 仍 403/410 则 PikPakLinkExpiredError 直接上抛
            refreshed_url = await self._refresh_url(generation)
            data = await self._fetch_range(refreshed_url, offset, size)

        self._session_bytes_read += len(data)
        return data

    async def _fetch_range(self, url: str, offset: int, size: int) -> bytes:
        if not (url.startswith("http://") or url.startswith("https://")):
            raise ChunkProviderError(f"非法直链协议 (file_id={self.file_id})")
        session = await self._ensure_session()
        end = min(offset + size - 1, self.total_bytes - 1)
        headers = {"Range": f"bytes={offset}-{end}"}
        self._session_in_flight[session] = self._session_in_flight.get(session, 0) + 1
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status in (403, 410):
                    raise PikPakLinkExpiredError(f"直链过期 (file_id={self.file_id}, status={resp.status})")
                if resp.status == 404:
                    raise PikPakNotFoundError(f"远端源文件不存在 (file_id={self.file_id}, status=404)")
                if resp.status in (429, 502, 503, 504):
                    retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
                    raise PikPakThrottledError(
                        f"CDN 节点频控/过载 (file_id={self.file_id}, offset={offset}, status={resp.status})",
                        status=resp.status,
                        retry_after=retry_after,
                    )
                content_type = (resp.headers.get("Content-Type") or "").lower()
                if "text/html" in content_type:
                    raise PikPakRiskControlError(
                        f"CDN 返回风控页 (file_id={self.file_id}, offset={offset}, status={resp.status})"
                    )
                if resp.status != 206:
                    raise ChunkFetchTransientError(
                        f"CDN 未按 Range 返回 206 (file_id={self.file_id}, offset={offset}, status={resp.status})"
                    )
                expected = min(size, self.total_bytes - offset)
                cr_header = resp.headers.get("Content-Range") or ""
                cr_match = _CONTENT_RANGE_RE.match(cr_header.strip())
                if not cr_match:
                    raise ChunkFetchTransientError(
                        f"Range 响应缺少或畸形 Content-Range (file_id={self.file_id}, offset={offset}): {cr_header!r}"
                    )
                cr_start = int(cr_match.group(1))
                cr_end = int(cr_match.group(2))
                cr_total_raw = cr_match.group(3)
                expected_end = offset + expected - 1
                if (
                    cr_start != offset
                    or cr_end != expected_end
                    or (cr_total_raw != "*" and int(cr_total_raw) != self.total_bytes)
                ):
                    raise ChunkFetchTransientError(
                        f"Range 响应 Content-Range 不匹配 "
                        f"(file_id={self.file_id}, 请求={offset}-{expected_end}/{self.total_bytes}, 实际={cr_header!r})"
                    )
                buf = bytearray()
                async for chunk in resp.content.iter_chunked(_RANGE_READ_CHUNK_BYTES):
                    if len(buf) + len(chunk) > expected:
                        raise ChunkFetchTransientError(
                            f"Range 响应超长 (file_id={self.file_id}, offset={offset}): "
                            f"预期 {expected} 字节, 已收到超过 {len(buf) + len(chunk)} 字节"
                        )
                    buf.extend(chunk)
                if len(buf) < expected:
                    raise ChunkFetchTransientError(
                        f"Range 响应短读 (file_id={self.file_id}, offset={offset}): 预期 {expected} 字节, 实际 {len(buf)} 字节"
                    )
                return bytes(buf)
        # Provider 专属异常 (ChunkProviderError 及其子类: LinkExpired/RiskControl/Transient)
        # 天然穿透至调用方 (引擎 L1/L2 容灾处置), 仅传输层异常在下方包装为 Transient
        except (TimeoutError, aiohttp.ClientError) as e:
            # 网络瞬断: 只透出异常类型与偏移, 不透出 URL (安全红线)
            raise ChunkFetchTransientError(
                f"网络瞬断 (file_id={self.file_id}, offset={offset}): {type(e).__name__}"
            ) from e
        finally:
            remaining = self._session_in_flight.get(session, 1) - 1
            if remaining <= 0:
                self._session_in_flight.pop(session, None)
                drained = self._session_drained_events.get(session)
                if drained is not None:
                    drained.set()
            else:
                self._session_in_flight[session] = remaining

    async def aclose(self) -> None:
        """幂等关闭: 释放持有的 ClientSession; 关闭后再读不复活会话"""
        self._closed = True
        sessions_to_close = list(self._closing_sessions)
        if self._session is not None and not self._session.closed:
            sessions_to_close.append(self._session)
            self._session = None
        for s in sessions_to_close:
            try:
                await s.close()
            except Exception as close_err:
                logger.debug("关闭 ClientSession 异常 (非致命): %s", close_err)
        self._closing_sessions.clear()
        await asyncio.sleep(0.05)
