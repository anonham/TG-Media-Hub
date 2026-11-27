"""FastTelethon 底层原语: 日志过滤器、自适应预读队列、测速器、动态并发解析器与在途限流器.

纯搬迁自原单文件 app/core/fast_telethon.py (拆分重构, 零运行时逻辑改动)。
"""

import asyncio
import collections
import logging
import time
from collections.abc import Callable
from typing import Any

from telethon import TelegramClient

from app.config import settings

logger = logging.getLogger(__name__)


class SuppressExpectedCloseFilter(logging.Filter):
    """过滤 Telegram 服务端空闲正常断开 TCP 产生的非致命 0 字节关闭噪音 (TASK-001)"""

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return not (
            "0 bytes read on a total of 8 expected bytes" in msg or "Server closed the connection: 0 bytes read" in msg
        )


def _install_telethon_log_filter() -> None:
    """应用过滤器到 Telethon 内部网络日志记录器 (幂等: 模块重载/重复导入时不叠加过滤器)."""
    for lg_name in ("telethon.network.mtprotosender", "telethon.network.connection.connection", "telethon.network"):
        lg = logging.getLogger(lg_name)
        if not any(isinstance(f, SuppressExpectedCloseFilter) for f in lg.filters):
            lg.addFilter(SuppressExpectedCloseFilter())


_install_telethon_log_filter()


class AdaptivePrefetchQueue(asyncio.Queue):
    """自适应动态调谐预读队列 (32~64 块):
    根据当前在途与并发自适应计算缓冲队列容量，并支持容量扩大时平滑唤醒生产者。
    """

    _maxsize: int
    _queue: collections.deque
    _putters: collections.deque

    def __init__(self, min_size: int = 32, max_size: int = 64):
        super().__init__(maxsize=min_size)
        self.min_size = min_size
        self.max_size = max_size

    def tune(self, target_concurrency: int, target_in_flight: int) -> None:
        """自适应 32~64 块动态调谐"""
        c = max(1, int(target_concurrency or 1))
        inf = max(1, int(target_in_flight or 1))
        new_max = min(self.max_size, max(self.min_size, c * inf * 2))
        if new_max != self._maxsize:
            self._maxsize = new_max
            slots = self._maxsize - len(self._queue)
            while self._putters and slots > 0:
                putter = self._putters.popleft()
                if not putter.done():
                    putter.set_result(None)
                    slots -= 1


class SpeedTracker:
    """平滑计算下载/上传速度 (MB/s) 和预计剩余时间 (ETA)，基于滑动时间窗口平均、EWMA与分段连续线性衰减模型"""

    def __init__(
        self, total_bytes: int, initial_bytes: int = 0, window_seconds: float = 3.5, stagnant_timeout: float = 4.5
    ):
        self.total_bytes = total_bytes
        self.initial_bytes = initial_bytes
        self.window_seconds = window_seconds
        self.stagnant_timeout = stagnant_timeout
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.last_bytes = initial_bytes
        self.current_bytes = initial_bytes
        self._calculated_speed = 0.0  # MB/s
        self._smoothed_speed = 0.0  # EWMA 平滑速度
        self._last_sample_speed = 0.0
        # 存储滑动窗口采样点: collections.deque[(timestamp, current_bytes)]
        self.samples: collections.deque[tuple[float, int]] = collections.deque()
        self.samples.append((self.start_time, initial_bytes))

    def reset_start_time(self) -> None:
        """重置测速起始时钟与采样窗口 (Worker 通道建连完成后正式传输前调用，消除握手时延对首包测速的低估影响)"""
        now = time.time()
        self.start_time = now
        self.last_update_time = now
        self._calculated_speed = 0.0
        self._smoothed_speed = 0.0
        self._last_sample_speed = 0.0
        self.samples.clear()
        self.samples.append((now, self.current_bytes))

    def _calc_window_speed(self, now: float) -> float:
        window_start = now - self.window_seconds
        if len(self.samples) >= 2:
            t0, b0 = self.samples[0]
            t_last, b_last = self.samples[-1]
            if t0 < window_start:
                t1, b1 = self.samples[1]
                if t1 > t0:
                    ratio = (window_start - t0) / (t1 - t0)
                    b_base = b0 + ratio * (b1 - b0)
                else:
                    b_base = b0
                t_base = window_start
            else:
                t_base = t0
                b_base = b0

            db = b_last - b_base
            dt_effective = max(t_last - t_base, now - t_base)
            if dt_effective > 0 and db >= 0:
                return (db / (1024 * 1024)) / dt_effective
            return 0.0
        if len(self.samples) == 1:
            t0, b0 = self.samples[0]
            dt = now - t0
            db = self.current_bytes - b0
            if dt > 0 and db > 0:
                return (db / (1024 * 1024)) / dt
            return 0.0
        return 0.0

    def update(self, current_bytes: int) -> None:
        now = time.time()
        idle_time = now - self.last_update_time
        self.current_bytes = current_bytes
        if current_bytes > self.last_bytes:
            # 传输长时间停滞恢复后，清空过期的远古采样点并重置 EWMA，避免被远古零速拖累
            if idle_time > self.stagnant_timeout:
                self.samples.clear()
                self.samples.append((now, current_bytes))
                self._smoothed_speed = 0.0
                self._last_sample_speed = 0.0
            else:
                self.samples.append((now, current_bytes))

            self.last_update_time = now
            self.last_bytes = current_bytes

            # 剪枝：剔除超出滑动时间窗口的旧采样点 (保留窗口边界左侧至多一个基线点)
            window_start = now - self.window_seconds
            while len(self.samples) >= 2 and self.samples[1][0] <= window_start:
                self.samples.popleft()

            # 计算当前滑动窗口瞬时速率并以 EWMA (alpha=0.4) 融合更新
            raw = self._calc_window_speed(now)
            if raw > 0:
                if self._smoothed_speed <= 0:
                    self._smoothed_speed = raw
                else:
                    self._smoothed_speed = 0.4 * raw + 0.6 * self._smoothed_speed
                self._last_sample_speed = self._smoothed_speed

    @property
    def speed(self) -> float:
        now = time.time()
        idle_time = now - self.last_update_time

        # 1. 严格停滞超时清零 (> stagnant_timeout)
        if idle_time > self.stagnant_timeout:
            self._calculated_speed = 0.0
            return 0.0

        # 2. 长途 ACK / 抖动停顿死区 (window_seconds < idle_time <= stagnant_timeout):
        # 采用连续线性衰减因子平滑过渡，杜绝 4MB/s ~ 0 骤降与毛刺反弹
        if idle_time > self.window_seconds:
            decay_span = self.stagnant_timeout - self.window_seconds
            ratio = max(0.0, 1.0 - (idle_time - self.window_seconds) / decay_span) if decay_span > 0 else 0.0
            base_spd = (0.6 * self._smoothed_speed) if self._smoothed_speed > 0 else (0.6 * self._last_sample_speed)
            self._calculated_speed = base_spd * ratio
            return self._calculated_speed

        # 3. 稳态传输窗口 (idle_time <= window_seconds)
        window_start = now - self.window_seconds
        while len(self.samples) >= 2 and self.samples[1][0] <= window_start:
            self.samples.popleft()

        raw_spd = self._calc_window_speed(now)
        if self._smoothed_speed > 0 and raw_spd > 0:
            self._calculated_speed = 0.4 * raw_spd + 0.6 * self._smoothed_speed
        elif raw_spd > 0:
            self._calculated_speed = raw_spd
        elif self._smoothed_speed > 0:
            self._calculated_speed = 0.6 * self._smoothed_speed
        else:
            self._calculated_speed = 0.0

        return self._calculated_speed

    @property
    def eta(self) -> float:
        spd = self.speed
        if spd <= 0:
            return 0.0
        remaining_mb = (self.total_bytes - self.current_bytes) / (1024 * 1024)
        return max(0.0, remaining_mb / spd)


class _CachedTargetResolver:
    """动态并发/在途/预取目标值 TTL 缓存解析器 (2.0s 缓存窗口)"""

    def __init__(
        self,
        *,
        getter: Callable[[], int] | None,
        raw_val: int | Callable[[], int] | None,
        setting_key: str,
        default_attr: str,
        fallback_default: int,
        max_val: int,
        ignore_if_equal_default: bool = False,
        force_one: bool = False,
    ) -> None:
        self._getter = getter
        self._raw_val = raw_val
        self._setting_key = setting_key
        self._default_attr = default_attr
        self._fallback_default = fallback_default
        self._max_val = max_val
        self._ignore_if_equal_default = ignore_if_equal_default
        self._force_one = force_one
        self._cached: int | None = None
        self._last_check: float = 0.0

    def _evaluate_raw(self) -> Any:
        if self._getter is not None:
            try:
                return self._getter()
            except Exception as cb_err:
                logger.debug("动态获取并发目标异常: %s", cb_err)
                return None
        if callable(self._raw_val):
            try:
                return self._raw_val()
            except Exception as cb_err:
                logger.debug("并发配置 callable 评估异常: %s", cb_err)
                return None
        if self._raw_val is not None:
            default_cfg = getattr(settings, self._default_attr, self._fallback_default)
            if not (self._ignore_if_equal_default and self._raw_val == default_cfg):
                return self._raw_val
        return None

    def _resolve_from_db(self) -> Any:
        from app.db.account_repository import SystemSettingsRepository

        default_cfg = getattr(settings, self._default_attr, self._fallback_default)
        try:
            return SystemSettingsRepository.get_int(self._setting_key, default_cfg)
        except Exception:
            return default_cfg

    def __call__(self) -> int:
        now = time.monotonic()
        if self._cached is not None and (now - self._last_check < 2.0):
            return self._cached

        target = self._evaluate_raw()
        if target is None:
            target = self._resolve_from_db()

        if self._force_one:
            val = 1
        else:
            try:
                val = max(1, min(int(target), self._max_val))
            except (ValueError, TypeError):
                val = max(1, min(int(self._fallback_default), self._max_val))

        self._cached = val
        self._last_check = now
        return val


def _normalize_client_list(client: TelegramClient | list[TelegramClient]) -> list[TelegramClient]:
    if isinstance(client, list):
        clients = [c for c in client if c]
        if not clients:
            raise ValueError("传入的 client 列表为空")
        return clients
    return [client]


class _DynamicInFlightLimiter:
    """支持双向动态扩缩容的在途并发控制器，避免 Semaphore 缩容后再扩容产生许可泄漏。"""

    def __init__(self, limit: int) -> None:
        self._limit = max(1, int(limit))
        self._active = 0
        self._slot_free = asyncio.Event()
        self._slot_free.set()

    def _refresh_event(self) -> None:
        if self._active < self._limit:
            self._slot_free.set()
        else:
            self._slot_free.clear()

    def set_limit(self, limit: int) -> int:
        self._limit = max(1, int(limit))
        self._refresh_event()
        return self._limit

    async def acquire(self) -> None:
        while self._active >= self._limit:
            self._slot_free.clear()
            await self._slot_free.wait()
        self._active += 1
        self._refresh_event()

    def release(self) -> None:
        self._active = max(0, self._active - 1)
        self._refresh_event()
