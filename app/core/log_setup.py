"""应用日志体系: dictConfig 幂等封装 + 系统日志环形缓冲 + I/O handler 异步队列分发。

此前全库无任何 logging 配置, scheduler / client_manager / fast_telethon 等模块的
logger.info 在运行期被默认 root logger 丢弃, 业务进度只能依赖散落的 print。

本模块提供:
- console handler (stderr, INFO, 人类可读格式: 时间/级别/logger 名)
- RotatingFileHandler 写 settings.LOG_DIR/app.log (5MB × 5 备份, INFO)
- RingBufferHandler 内存环形缓冲 (2000 条), 挂 app 根 logger, 供 GET /api/logs/system
  读取真实后端与调度器运行日志
- QueueHandler/QueueListener: 耗 I/O 的 console 与 app_file 迁入专用后台监听线程,
  事件循环内仅做有界入队 (maxsize=10000, 溢出丢弃), 慢盘/管道背压不再微冻结事件循环

覆盖 python run.py 与 uvicorn app.main:app (Docker) 两种入口: 两处均在模块导入时调用
configure_logging(), 模块级 guard 保证幂等 (重复调用不产生重复 handler)。
dictConfig 使用 disable_existing_loggers=False, 不影响 main/fast_telethon 在 telethon.*
logger 上挂载的 SuppressExpectedCloseFilter 过滤链 (filter 与 handler 配置正交)。
"""

import logging
import logging.config
import os
import queue as queue_mod
import sys
import time
from collections import deque
from dataclasses import dataclass
from logging.handlers import QueueHandler, QueueListener
from typing import Any

from app.config import settings
from app.core.redact import RedactLogFilter, redact_text

_RING_CAPACITY = 2000
_QUEUE_MAXSIZE = 10000


class RingBufferHandler(logging.Handler):
    """内存环形缓冲 handler: 保留最近 N 条 app.* 日志, 供 /api/logs/system 读取"""

    def __init__(self, capacity: int = _RING_CAPACITY):
        super().__init__(level=logging.INFO)
        self._buffer: deque = deque(maxlen=capacity)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._buffer.append(
                {
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.created)),
                    "level": record.levelname.lower(),
                    "logger": record.name,
                    "message": str(redact_text(record.getMessage())),
                }
            )
        except Exception:
            self.handleError(record)

    def snapshot(self) -> list[dict[str, str]]:
        """一致性快照 (deque 定长追加不会使 list() 迭代失效, 单线程事件循环内调用安全)"""
        return list(self._buffer)


# 端点引用的单例: dictConfig 每次会按配置重建 console/file handler, 环形缓冲必须
# 保持同一实例供 /api/logs/system 读取, 故以手动 addHandler 方式挂载 (guard 内仅一次)
ring_buffer_handler = RingBufferHandler()


@dataclass
class LoggingRuntimeState:
    """应用日志体系运行态容器 (替代裸 global 变量修改)"""

    configured: bool = False
    queue_listener: QueueListener | None = None


logging_state = LoggingRuntimeState()
_configured: bool = False
# I/O 监听线程单例: 仅持有 dictConfig 产出的 console + app_file, 由 shutdown_logging 停止
_queue_listener: QueueListener | None = None


def _sync_log_module_attrs(configured: bool, listener: QueueListener | None) -> None:
    logging_state.configured = configured
    logging_state.queue_listener = listener
    mod: Any = sys.modules[__name__]
    mod._configured = configured
    mod._queue_listener = listener


class RedactingFormatter(logging.Formatter):
    """全量日志脱敏格式化器: 对完整渲染结果 (含 traceback / exc_text / stack_info) 执行 redact_text."""

    def format(self, record: logging.LogRecord) -> str:
        formatted = super().format(record)
        return str(redact_text(formatted))


class _NonBlockingQueueHandler(QueueHandler):
    """有界非阻塞队列处理器: 队列满时静默丢弃并累计计数, 绝不调用 handleError 同步写 stderr."""

    def __init__(self, log_queue: queue_mod.Queue) -> None:
        super().__init__(log_queue)
        self.dropped_count: int = 0

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.enqueue(self.prepare(record))
        except Exception:
            self.dropped_count += 1


def _build_config() -> dict:
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "()": "app.core.log_setup.RedactingFormatter",
                "format": log_format,
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        # 全局脱敏过滤器挂 handler 层 (对经该 handler 的所有 record 生效,
        # 覆盖第三方 logger 冒泡路径), 擦除 PikPak 签名直链与 PIKPAK_TOKEN 字面值
        "filters": {
            "pikpak_redact": {"()": "app.core.redact.RedactLogFilter"},
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "standard",
                "filters": ["pikpak_redact"],
                "level": "INFO",
            },
            "app_file": {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "standard",
                "filters": ["pikpak_redact"],
                "level": "INFO",
                "filename": os.path.join(settings.LOG_DIR, "app.log"),
                "maxBytes": 5 * 1024 * 1024,
                "backupCount": 5,
                "encoding": "utf-8",
            },
        },
        "loggers": {
            # app.* 业务与调度核心日志: INFO 起步, 自带 console + 文件 + 环形缓冲, 不再向 root 重复传播
            "app": {"level": "INFO", "handlers": ["console", "app_file"], "propagate": False},
        },
        # root 仅收第三方库 WARNING+ (telethon/aiohttp 等异常告警可见), app.* 不经过 root
        "root": {"level": "WARNING", "handlers": ["console"]},
    }


def configure_logging() -> None:
    """幂等配置应用日志 (console + 旋转文件 + 环形缓冲); 重复调用不产生重复 handler"""
    if _configured and _queue_listener is not None:
        return
    os.makedirs(settings.LOG_DIR, exist_ok=True)
    logging.config.dictConfig(_build_config())
    # 环形缓冲单例不经 dictConfig 重建, 需手动挂同一脱敏过滤器 (guard 保证仅一次)
    ring_buffer_handler.addFilter(RedactLogFilter())
    app_logger = logging.getLogger("app")
    # I/O 阻塞型 handler (console + app_file) 迁入后台监听线程: 调用侧仅做有界入队,
    # 慢盘/管道背压不再同步冻结事件循环; 队列满时 QueueHandler 丢弃并 handleError, 不阻塞
    io_handlers = [h for h in app_logger.handlers if not isinstance(h, QueueHandler) and h is not ring_buffer_handler]
    log_queue: queue_mod.Queue = queue_mod.Queue(maxsize=_QUEUE_MAXSIZE)
    listener = QueueListener(log_queue, *io_handlers, respect_handler_level=True)
    listener.start()
    # app logger 仅保留 [_NonBlockingQueueHandler, 环形缓冲直连]: ring 为纯内存 deque 操作 (<1µs),
    # 必须同步直连保证 /api/logs/system 轮询时效与测试确定性, 绝不入队
    app_logger.handlers = [_NonBlockingQueueHandler(log_queue), ring_buffer_handler]
    _sync_log_module_attrs(True, listener)


def _wait_queue_slot(listener: QueueListener, q: Any, timeout: float = 5.0) -> None:
    """在有限时间内等待存活的消费线程腾出空位，保留所有待落盘记录。"""
    if q is None or getattr(q, "maxsize", 0) <= 0:
        return
    thread = getattr(listener, "_thread", None)
    deadline = time.monotonic() + timeout
    while q.full() and thread is not None and thread.is_alive() and time.monotonic() < deadline:
        time.sleep(0.005)


def shutdown_logging() -> None:
    """停止日志监听线程: QueueListener 以 FIFO sentinel 收尾, stop() 返回前队列内残余
    日志已全部写入目标 handler (落盘 flush)。幂等, 未启动时为空操作。"""
    listener = _queue_listener
    if listener is not None:
        try:
            q = getattr(listener, "queue", None)
            _wait_queue_slot(listener, q)
            listener.stop()
            _sync_log_module_attrs(_configured, None)
        except queue_mod.Full:
            try:
                q = getattr(listener, "queue", None)
                _wait_queue_slot(listener, q)
                listener.stop()
                _sync_log_module_attrs(_configured, None)
            except Exception as e:
                logging.getLogger(__name__).warning("停止日志监听器异常: %s", e)
        except Exception as e:
            logging.getLogger(__name__).warning("停止日志监听器异常: %s", e)
