"""集中定义 Prometheus 运行时监控指标 (模块级单例，线程安全)

指标清单涵盖:
- 任务生命周期状态流转与调度重试
- Telegram FloodWait 账号/Bot 熔断记账
- FastTelethon 分块并行下载重试与丢弃
- 累计下载流量 (字节)
- 运行中任务、待调度队列深度、活跃 WebSocket 连接、全局下载线程闸及处于 Flood 锁定的账号数
"""

import logging

from prometheus_client import Counter, Gauge

from app.db.task_repository import TaskQueueRepository

# ==========================================
# 1. 计数器 (Counters)
# ==========================================

task_status_transitions_total = Counter(
    "task_status_transitions_total", "任务状态流转计数 (按目标状态分组)", ["status"]
)


def _record_status_transition(status: str) -> None:
    task_status_transitions_total.labels(status=status).inc()


TaskQueueRepository.register_status_transition_hook(_record_status_transition)

task_retry_total = Counter("task_retry_total", "调度器任务自动重试总次数")

floodwait_total = Counter(
    "floodwait_total",
    "Telegram 触发 FloodWait 频控限制总次数",
    ["scope"],  # "account" | "bot"
)

chunk_retry_total = Counter("chunk_retry_total", "FastTelethon 分块下载重试总次数")

chunk_dropped_total = Counter("chunk_dropped_total", "FastTelethon 分块 5 次重试耗尽最终丢弃总次数")

download_bytes_total = Counter("download_bytes_total", "系统累计接收的下载数据量 (字节)")


# ==========================================
# 2. 仪表 (Gauges)
# ==========================================

running_tasks = Gauge("running_tasks", "当前处于 running 状态的活跃任务数")

queued_task_depth = Gauge("queued_task_depth", "待调度就绪任务队列深度")

active_ws_connections = Gauge("active_ws_connections", "当前建立并保持的活跃 WebSocket 连接数")

download_threads_in_use = Gauge(
    "download_threads_in_use", "当前已占用的全局下载分块线程数 (上限 MAX_GLOBAL_DOWNLOAD_THREADS)"
)


def _get_flood_locked_accounts_count() -> int:
    try:
        import time

        from app.core.client_manager import client_manager

        now = time.time()
        return sum(1 for acc in list(client_manager._accounts.values()) if getattr(acc, "flood_wait_until", 0) > now)
    except Exception as exc:
        logging.getLogger(__name__).debug("收集 flood_locked_accounts 指标异常: %s", exc)
        return 0


flood_locked_accounts = Gauge("flood_locked_accounts", "当前处于 FloodWait 冷却锁定期的 Telegram 账号数")
flood_locked_accounts.set_function(_get_flood_locked_accounts_count)
