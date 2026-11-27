"""专用 DB 写线程池 (Single-Writer DB Write Pool)

仅承载事件循环线程上「同步进度回调」触发的写路径 (update_progress, 回调无法 await,
只能 fire-and-forget)。稳态写入速率 = 心跳 0.8s × max_global_concurrency(默认 3)
≈ 3.75 写/s, 单线程余量充足 (单写 <1ms); BEGIN IMMEDIATE 本就串行化写 + busy_timeout=5000 兜底。
max_workers=1: 池内严格 FIFO, 杜绝完成态写先于队尾进度写提交 (进度数字短暂回跳); 与默认
线程池 (sha256 等大文件任务) 隔离, 防止被写竞争阻塞的 DB 任务占满默认池造成饥饿。
"""

import asyncio
import functools
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any


@dataclass
class WritePoolState:
    """专用 DB 写线程池状态容器 (替代裸 global 变量修改)"""

    pool: ThreadPoolExecutor | None = field(
        default_factory=lambda: ThreadPoolExecutor(max_workers=1, thread_name_prefix="db-write")
    )


write_pool_state = WritePoolState()
_DB_WRITE_POOL: ThreadPoolExecutor | None = write_pool_state.pool


def _sync_module_attr(pool: ThreadPoolExecutor | None) -> None:
    write_pool_state.pool = pool
    mod: Any = sys.modules[__name__]
    mod._DB_WRITE_POOL = pool


def get_db_write_pool() -> ThreadPoolExecutor:
    """获取专用 DB 写线程池；若写池已关闭则自动重启初始化 (自愈机制)"""
    pool = _DB_WRITE_POOL if _DB_WRITE_POOL is not write_pool_state.pool else write_pool_state.pool
    if pool is None or getattr(pool, "_shutdown", False):
        pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="db-write")
        _sync_module_attr(pool)
    return pool


def submit_db_write(fn, /, *args, **kwargs):
    """把同步 DB 写提交到专用写池 (fire-and-forget)。

    若在事件循环线程上调用，通过 loop.run_in_executor 派发；
    若在无事件循环的后台工作线程上调用，安全降级至 pool.submit 派发。
    异常经 done_callback 记录日志, 不向调用方传播 (进度写失败不应中断传输)。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    # 事件循环分支返回 asyncio.Future, 线程分支返回 concurrent Future —— 两者均只需
    # add_done_callback/exception 鸭子面, 用 Any 收口
    future: Any = None
    for attempt in range(2):
        pool = get_db_write_pool()
        try:
            if loop is not None:
                future = loop.run_in_executor(pool, functools.partial(fn, *args, **kwargs))
            else:
                future = pool.submit(fn, *args, **kwargs)
            break
        except RuntimeError as submit_err:
            if attempt == 0:
                continue
            logging.warning("后台 DB 写提交失败 (%s): %s", getattr(fn, "__name__", fn), submit_err)
            return

    def _log_failure(f):
        if f.cancelled():
            return
        exc = f.exception()
        if exc:
            logging.error("后台 DB 写失败 (%s): %s", getattr(fn, "__name__", fn), exc)

    if future is not None:
        future.add_done_callback(_log_failure)


def drain_db_write_pool_sync(timeout: float = 3.0) -> bool:
    """同步等待专用 DB 写池排空当前全部积压任务"""
    pool = _DB_WRITE_POOL if _DB_WRITE_POOL is not write_pool_state.pool else write_pool_state.pool
    if pool is None or getattr(pool, "_shutdown", False):
        return True
    try:
        future = pool.submit(lambda: None)
        future.result(timeout=timeout)
        return True
    except Exception as e:
        logging.warning("drain_db_write_pool 超时或异常 (%.1fs): %s", timeout, e)
        return False


async def drain_db_write_pool(timeout: float = 3.0) -> bool:
    """异步等待专用 DB 写池排空当前全部积压任务"""
    return await asyncio.to_thread(drain_db_write_pool_sync, timeout)


def shutdown_db_write_pool_sync(timeout: float = 3.0):
    """同步排空并安全释放专用 DB 写池，并在原位重建就绪的新写池 (自愈)"""
    drained = drain_db_write_pool_sync(timeout=timeout)
    old_pool = _DB_WRITE_POOL if _DB_WRITE_POOL is not write_pool_state.pool else write_pool_state.pool
    new_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="db-write")
    _sync_module_attr(new_pool)
    if old_pool and not getattr(old_pool, "_shutdown", False):
        old_pool.shutdown(wait=drained, cancel_futures=not drained)


async def shutdown_db_write_pool(timeout: float = 3.0):
    """异步排空并安全释放专用 DB 写池，并在原位重建就绪的新写池 (自愈)"""
    await asyncio.to_thread(shutdown_db_write_pool_sync, timeout)


def cancel_queued_and_wait_active_writes_sync() -> None:
    """停机排空超时后的受控收口: 取消尚未开始的排队写任务并等待当前在途写线程退出。"""
    old_pool = _DB_WRITE_POOL if _DB_WRITE_POOL is not write_pool_state.pool else write_pool_state.pool
    new_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="db-write")
    _sync_module_attr(new_pool)
    if old_pool and not getattr(old_pool, "_shutdown", False):
        old_pool.shutdown(wait=True, cancel_futures=True)


async def cancel_queued_and_wait_active_writes() -> None:
    """异步版停机写池兜底收口: 确保所有在途写线程退出后才继续 WAL 检查点或放锁。"""
    await asyncio.to_thread(cancel_queued_and_wait_active_writes_sync)
