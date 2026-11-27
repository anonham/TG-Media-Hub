"""执行器进度回调共享工厂 (Executor Progress Callback Factory)

收拢: download_executor ×3 (字节协议) + publish_executor ×2 (条目协议) 五处
手写嵌套信封的单一实现。两种进度回调协议并存是有意设计, 勿强行合一:

- 字节协议 (下载执行器): current/total 为**当前条目字节**; 每次调用都落库 (无节流);
  哑哨兵 (100/100 零速) 跳过字节计量但以 total 视为完成; delta 喂 download_bytes_total;
  消费方 (DownloaderService) 以 ``hasattr(cb, "set_initial_bytes")`` 鸭子类型注入断点
  起始字节, 调用方以 ``cb.received_bytes()`` 读回实际字节数做累计对账。

- 条目协议 (发布执行器): current/total 为**条目序号**, 字节经 total_bytes_done/_all
  kwarg 传入; SQLite 节流 (>=2.0s 或 >=1.0% 或 force_db) + 速率粘滞 (完成事件速率
  掉零沿用最后非零值, J8 双速率); PikPak 变体加字节粘滞 / 拉流速率键 / redact。

patch 透明性契约: ``submit_fn`` 由各执行器调用点以模块全局名注入 (而非工厂模块内
import), ``monkeypatch.setattr(executor_module, "submit_db_write", ...)`` 形态的既有
测试 patch 面保持不变; connection_manager 为单例对象, 广播 patch 天然穿透。
"""

import asyncio
import logging
import time
from collections.abc import Callable
from typing import Any

from app.core.connection_manager import connection_manager
from app.db.task_repository import TaskQueueRepository

logger = logging.getLogger(__name__)

_THROTTLE_DB_SECONDS = 2.0
_THROTTLE_PERCENT_STEP = 1.0
_THROTTLE_BYTE_DB_SECONDS = 0.8
_BG_BROADCAST_TASKS: set[asyncio.Task] = set()


def _schedule_ws_broadcast(payload: dict) -> None:
    """派发后台 WS 广播协程并登记强引用，防止被 GC 提前回收 (清零 RUF006)"""
    task = asyncio.create_task(connection_manager.broadcast(payload))
    _BG_BROADCAST_TASKS.add(task)
    task.add_done_callback(_BG_BROADCAST_TASKS.discard)


def make_executor_progress_cb(
    task_id: str,
    *,
    submit_fn: Callable,
    # ── 字节协议 (下载执行器) ─────────────────────────────
    byte_accounting: bool = False,
    item_label_fn: Callable[[], str] | None = None,
    item_position_fn: Callable[[], tuple[int, int]] | None = None,
    done_base_fn: Callable[[], int] | None = None,
    known_total_fn: Callable[[], int] | None = None,
    # ── 条目协议 (发布执行器) ─────────────────────────────
    sticky_download_speed: bool = False,
    sticky_bytes: bool = False,
    initial_total_bytes: int = 0,
    download_speed_key: bool = False,
    redact: bool = False,
):
    """构造执行器进度回调 (含 set_initial_bytes / received_bytes 附件, 字节协议)。

    byte_accounting=True 走字节协议 (item_label_fn/item_position_fn/done_base_fn/
    known_total_fn 为必配惰性取值器, 保持外层循环变量的闭包格子语义 —— 基线在回调
    调用时惰性读取, 与原嵌套 def 逐字等价); 否则走条目协议 (节流 + 速率粘滞)。
    """
    state: dict[str, Any] = {
        "last_bytes": 0,
        "last_db_write_time": 0.0,
        "last_db_percent": -1.0,
        "last_speed": 0.0,
        "last_dl_speed": 0.0,
        "last_done_bytes": 0,
        "last_all_bytes": initial_total_bytes,
    }

    def _submit_progress(
        done_bytes: int,
        all_bytes: int,
        item_pos: int,
        item_total: int,
        speed: float,
        eta: float,
        item_text: str,
        concurrency: int,
        dc_id: int,
        concurrency_total: int,
    ) -> None:
        submit_fn(
            TaskQueueRepository.update_progress,
            task_id,
            done_bytes,
            all_bytes,
            item_pos,
            item_total,
            speed,
            eta,
            item_text,
            concurrency,
            dc_id,
            concurrency_total=concurrency_total or None,
        )

    if byte_accounting:
        assert item_label_fn is not None
        assert item_position_fn is not None
        assert done_base_fn is not None
        assert known_total_fn is not None

        def byte_progress_cb(current, total, speed, eta, file_name, concurrency=0, dc_id=0, concurrency_total=0):
            item_text = f"[{item_label_fn()}] {file_name}"
            is_skip_dummy = concurrency == 0 and speed == 0.0 and eta == 0.0 and current == 100 and total == 100
            if not is_skip_dummy:
                delta = current - state["last_bytes"]
                if delta > 0:
                    try:
                        from app.core.metrics import download_bytes_total

                        download_bytes_total.inc(delta)
                    except Exception as m_err:
                        logger.debug("更新下载字节指标异常 (非致命): %s", m_err)
                state["last_bytes"] = current
            task_done_bytes = done_base_fn() + (total if is_skip_dummy else current)
            task_all_bytes = max(known_total_fn(), task_done_bytes, 1)
            task_percent = (task_done_bytes / task_all_bytes * 100) if task_all_bytes > 0 else 0.0
            pos, total_items = item_position_fn()

            # SQLite 节流写入: 首个分块、单文件完成 (current >= total)、距上次落库 >= 0.8s 或进度跨越 >= 1.0% 时落盘
            now = time.time()
            time_diff = now - state["last_db_write_time"]
            percent_diff = abs(task_percent - state["last_db_percent"])
            is_item_finished = total > 0 and current >= total
            if (
                is_skip_dummy
                or is_item_finished
                or time_diff >= _THROTTLE_BYTE_DB_SECONDS
                or percent_diff >= _THROTTLE_PERCENT_STEP
            ):
                state["last_db_write_time"] = now
                state["last_db_percent"] = task_percent
                _submit_progress(
                    task_done_bytes,
                    task_all_bytes,
                    pos,
                    total_items,
                    speed,
                    eta,
                    item_text,
                    concurrency,
                    dc_id,
                    concurrency_total,
                )
            _schedule_ws_broadcast(
                {
                    "type": "progress",
                    "task_id": task_id,
                    "progress": {
                        "processed_bytes": task_done_bytes,
                        "total_bytes": task_all_bytes,
                        "processed_items": pos,
                        "total_items": total_items,
                        "percent": round(task_percent, 1),
                        "speed_mb_s": speed,
                        "eta_s": eta,
                        "current_item": item_text,
                        "concurrency": concurrency,
                        "concurrency_total": concurrency_total or 0,
                        "dc_id": dc_id,
                    },
                    "item_progress_bytes": current,
                    "item_total_bytes": total,
                }
            )

        def set_initial_bytes(val: int):
            state["last_bytes"] = max(0, val)

        def received_bytes() -> int:
            return int(state["last_bytes"])

        byte_progress_cb.set_initial_bytes = set_initial_bytes  # type: ignore[attr-defined]
        byte_progress_cb.received_bytes = received_bytes  # type: ignore[attr-defined]
        return byte_progress_cb

    def progress_cb(
        current,
        total,
        speed,
        eta,
        file_name,
        concurrency=0,
        dc_id=0,
        concurrency_total=0,
        percent=None,
        force_db: bool = False,
        pikpak_download_speed_mb_s: float = 0.0,
        total_bytes_done: int = 0,
        total_bytes_all: int = 0,
        completed_bytes: int = 0,
        total_bytes: int = 0,
        **kwargs,
    ):
        if speed:
            state["last_speed"] = speed
        else:
            speed = state["last_speed"]
        if sticky_download_speed:
            if pikpak_download_speed_mb_s:
                state["last_dl_speed"] = pikpak_download_speed_mb_s
            else:
                pikpak_download_speed_mb_s = state["last_dl_speed"]
        if sticky_bytes:
            # PikPak 变体: 字节粘滞 (引擎分卷间隙掉零沿用最后非零值), 初值 = 预解析体积和
            if total_bytes_done:
                state["last_done_bytes"] = total_bytes_done
            if total_bytes_all:
                state["last_all_bytes"] = total_bytes_all
            done_bytes = state["last_done_bytes"]
            all_bytes = state["last_all_bytes"]
        else:
            # 文件夹变体: 字节取新鲜 kwarg 回退链; 粘滞格同步记录 (终态收尾读取
            # last_progress_bytes 与原实现读点一致)
            done_bytes = completed_bytes or total_bytes_done or 0
            all_bytes = total_bytes or total_bytes_all or 0
            if done_bytes:
                state["last_done_bytes"] = done_bytes
            if all_bytes:
                state["last_all_bytes"] = all_bytes
        calc_percent = (
            percent
            if percent is not None
            else ((done_bytes / all_bytes * 100) if all_bytes > 0 else ((current / total * 100) if total > 0 else 0))
        )
        current_item_text = f"[{current}/{total}] {file_name}"

        # SQLite 节流写入 (满足 >=2.0s 或 进度跨越 >=1.0% 或 force_db 时落盘)
        now = time.time()
        time_diff = now - state["last_db_write_time"]
        percent_diff = abs(calc_percent - state["last_db_percent"])
        if force_db or time_diff >= _THROTTLE_DB_SECONDS or percent_diff >= _THROTTLE_PERCENT_STEP:
            state["last_db_write_time"] = now
            state["last_db_percent"] = calc_percent
            _submit_progress(
                int(done_bytes),
                int(all_bytes),
                current,
                total,
                speed,
                eta,
                current_item_text,
                concurrency,
                dc_id,
                concurrency_total,
            )

        progress_dict: dict[str, Any] = {
            "processed_bytes": done_bytes,
            "total_bytes": all_bytes,
            "processed_items": current,
            "total_items": total,
            "percent": round(calc_percent, 1),
            "speed_mb_s": speed,
            "eta_s": eta,
            "current_item": current_item_text,
            "concurrency": concurrency,
            "concurrency_total": concurrency_total or 0,
            "dc_id": dc_id,
        }
        if download_speed_key:
            progress_dict["pikpak_download_speed_mb_s"] = pikpak_download_speed_mb_s
        payload: dict[str, Any] = {
            "type": "progress",
            "task_id": task_id,
            "progress": progress_dict,
        }
        if redact:
            from app.core.redact import redact_payload

            payload = redact_payload(payload)
        _schedule_ws_broadcast(payload)

    def last_progress_bytes() -> tuple[int, int]:
        """终态收尾读点: (最后已知完成字节, 最后已知总字节) —— 与原闭包粘滞变量读点一致。"""
        return int(state["last_done_bytes"]), int(state["last_all_bytes"])

    progress_cb.last_progress_bytes = last_progress_bytes  # type: ignore[attr-defined]
    return progress_cb
