"""统一发布流水线共享运行时小件 (计数器 / 批次聚合 / 信号 / 进度统一出口)"""

import asyncio
import logging
import time
from typing import Any

from app.core.pipeline.models import PublishContext, PublishItem
from app.core.scheduler import UploadCancelledError, UploadPausedError

logger = logging.getLogger(__name__)


class Counters:
    """任务级计数器 (published 含历史防重跳过数, 兼容旧进度语义)"""

    def __init__(self, total_files: int):
        self.total_files = total_files
        self.published = 0
        self.newly = 0
        self.done_bytes = 0

    def percent(self) -> float:
        return (self.published / self.total_files * 100) if self.total_files else 0.0


class BatchState:
    """批次内聚合 (published/newly/failed/skipped/successful)"""

    def __init__(self) -> None:
        self.published = 0
        self.newly = 0
        self.failed: list[str] = []
        self.skipped: list[Any] = []
        self.successful: list[PublishItem] = []
        self.album_published = 0  # 相册形态成功张数 (批次总结卡 "📷 相册" 行)
        # 包4 (J7): 诊断明细 —— b.failed 为字符串形态 (终态公式/notifier 消费面不动),
        # 结构化明细与 b.failed 同点填充, 供执行器终态 merge_params (封顶信封)。
        self.failed_details: list[dict[str, Any]] = []
        self.dedup_hits: list[dict[str, Any]] = []


def check_signals(context: PublishContext) -> None:
    if context.is_cancelled and context.is_cancelled():
        raise UploadCancelledError("上传任务被取消")
    if context.is_paused and context.is_paused():
        raise UploadPausedError("上传任务被暂停")


def emit_progress(
    context: PublishContext,
    *,
    current: int | float,
    total: int | float,
    name: str,
    percent: float,
    force_db: bool,
    speed: float = 0.0,
    done_bytes: int = 0,
    all_bytes: int = 0,
    dl_speed: float | None = None,
    concurrency: int | None = None,
    dc_id: int = 0,
    concurrency_total: int | None = None,
) -> None:
    """进度统一出口: 双方言 kwarg 同时下发 (本地执行器 completed_bytes/total_bytes,
    PikPak 执行器 total_bytes_done/total_bytes_all), safe_invoke_progress 自动过滤
    宿主未声明形参 —— 两侧契约互不干扰。字节口径统一全任务 (context.total_bytes_sum)。"""
    if not context.progress_callback:
        return
    kwargs: dict[str, Any] = {
        "percent": percent,
        "force_db": force_db,
        "completed_bytes": done_bytes,
        "total_bytes": all_bytes,
        "total_bytes_done": done_bytes,
        "total_bytes_all": all_bytes,
    }
    if dl_speed is not None:
        kwargs["pikpak_download_speed_mb_s"] = dl_speed
    if concurrency is not None:
        kwargs.update(concurrency=concurrency, dc_id=dc_id, concurrency_total=concurrency_total)
    try:
        from app.core.pipeline.utils import safe_invoke_progress

        safe_invoke_progress(context.progress_callback, current, total, speed, 0.0, name, **kwargs)
    except Exception as e:
        logger.debug("进度回调异常: %s", e)


async def wait_with_signals(context: PublishContext, delay: float) -> None:
    """Wait for the configured delay while polling task controls."""
    deadline = time.monotonic() + delay
    while True:
        check_signals(context)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        await asyncio.sleep(min(0.25, remaining))


CARD_DRAIN_FLOOR = 0.7  # 暂停排水模式的占位卡发卡地板间隔 (秒): 快速但不零间隔, 兼顾风控节奏


async def card_throttle_wait(context: PublishContext, delay: float) -> None:
    """占位卡节流 (暂停排水语义): 暂停信号触发排水模式 —— 当前路由队列剩余的占位卡以
    CARD_DRAIN_FLOOR 地板间隔快速发完, 暂停 raise 延迟到下一个常规检查点收口
    (paused 是电平型标志, 后续 wait_with_signals / check_signals 必然触发);
    取消不排水, 照常立即中断; 排水不改变 封顶与串行发送 (R7 防刷屏语义保留)。"""
    started = time.monotonic()
    if not context.pause_drain:
        try:
            await wait_with_signals(context, delay)
            return
        except UploadPausedError:
            context.pause_drain = True
            logger.warning("⏸️ 暂停信号已收到：剩余占位卡将快速发完后暂停 (取消信号不受影响)")

    # Count time already spent throttling toward the floor, and defer only pause.
    deadline = started + CARD_DRAIN_FLOOR
    while True:
        if context.is_cancelled and context.is_cancelled():
            raise UploadCancelledError("上传任务被取消")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        await asyncio.sleep(min(0.25, remaining))
