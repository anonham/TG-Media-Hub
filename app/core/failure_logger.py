import json
import logging
import os
import threading
import time
from logging.handlers import RotatingFileHandler
from typing import Any, ClassVar

from app.config import settings
from app.core.redact import redact_payload


class FailureLogger:
    """
    专为低配受限设备（NAS、低配 VPS、树莓派）设计的轻量级失败日志记录器（支持发布与下载独立日志）：
    1. 容量硬锁定：通过 RotatingFileHandler 限制单文件最大 2MB，最多保留 3 个备份（总体积限制在 6MB 内）。
    2. 单行结构化 JSONL：去除多行 Python 堆栈 Traceback，压缩为约 150 字节的单行 JSON，减少 I/O 刷盘损耗。
    3. 异常标签分类：将泛化的 Exception 自动归类为 FLOOD_WAIT, NETWORK_TIMEOUT, FILE_TOO_LARGE 等标准分类。
    """

    _loggers: ClassVar[dict[tuple[str, str], logging.Logger]] = {}
    _lock = threading.Lock()

    @classmethod
    def get_logger(cls, filename: str = "publish_failures.jsonl") -> logging.Logger:
        log_dir = getattr(settings, "LOG_DIR", os.path.join(settings.DATA_DIR, "logs"))
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.abspath(os.path.join(log_dir, filename))
        cache_key = (filename, log_path)

        with cls._lock:
            if cache_key not in cls._loggers:
                logger_name = f"FailureLogger_{filename}_{abs(hash(log_path))}"
                logger = logging.getLogger(logger_name)
                logger.setLevel(logging.INFO)
                logger.propagate = False

                while logger.handlers:
                    h = logger.handlers.pop()
                    h.close()

                handler = RotatingFileHandler(
                    log_path,
                    maxBytes=2 * 1024 * 1024,  # 单文件上限 2MB
                    backupCount=3,  # 历史归档最多 3 个
                    encoding="utf-8",
                )
                handler.setFormatter(logging.Formatter("%(message)s"))
                logger.addHandler(handler)

                cls._loggers[cache_key] = logger
            return cls._loggers[cache_key]

    @classmethod
    def close_all(cls) -> None:
        """关闭并注销所有活跃的 FailureLogger handler，释放文件句柄（供测试 teardown 或服务停机调用）。"""
        with cls._lock:
            for logger in cls._loggers.values():
                while logger.handlers:
                    h = logger.handlers.pop()
                    try:
                        h.close()
                    except Exception as exc:
                        logging.getLogger("FailureLogger").debug("Failed closing failure logger handler: %s", exc)
            cls._loggers.clear()

    @classmethod
    def classify_error(cls, error_obj: Exception) -> str:
        err_str = str(error_obj).lower()
        err_type = type(error_obj).__name__.lower()

        # 1. Telegram 远程频道/群组权限不足 RPC 错误
        tg_no_perm_patterns = (
            "chat admin privileges",
            "chat_admin_required",
            "chat_write_forbidden",
            "chat_send_media_forbidden",
            "channel_private",
            "user_banned_in_channel",
            "right_forbidden",
            "not enough rights",
            "have no rights",
            "need admin rights",
            "admin privileges are required",
            "无发帖权限",
            "写权限前置熔断",
            "can't write",
            "cant write",
            "can't send",
            "cant send",
            "banned from sending",
            "chat forbidden",
            "broadcast forbidden",
        )
        tg_perm_type_patterns = (
            "chatadminrequired",
            "channelprivate",
            "chatwriteforbidden",
            "chatsendmediaforbidden",
            "chatforbidden",
            "broadcastforbidden",
            "rightforbidden",
            "userbannedinchannel",
            "channelbanned",
            "forbiddenerror",
            "bannedrights",
        )
        if any(p in err_str for p in tg_no_perm_patterns) or any(p in err_type for p in tg_perm_type_patterns):
            return "TG_NO_PERMISSION"

        # 2. Telegram 频控与慢速模式
        if "flood" in err_str or "flood" in err_type or "slowmode" in err_str or "slowmode" in err_type:
            return "FLOOD_WAIT"

        # 3. 网络超时与连接中断
        # (注意: Python 标准库中 ConnectionError, socket.gaierror 等均为 OSError 子类, 须在 FILE_IO_ERROR 前置匹配)
        network_str_patterns = (
            "timeout",
            "timed out",
            "connection",
            "reset by peer",
            "broken pipe",
            "network",
            "socket",
            "unreachable",
            "name resolution",
            "dns",
            "handshake failed",
        )
        network_type_patterns = (
            "timeout",
            "connection",
            "network",
            "socket",
            "gaierror",
            "herror",
            "brokenpipe",
            "timedout",
        )
        if (
            any(p in err_str for p in network_str_patterns)
            or any(p in err_type for p in network_type_patterns)
            or isinstance(error_obj, (TimeoutError, ConnectionError))
        ):
            return "NETWORK_TIMEOUT"

        # 4. 文件体积超限 (含任务中途 Premium 失效引发的 400 拒绝与 FilePart* 异常)
        file_too_large_str_patterns = (
            "too large",
            "too big",
            "entity_too_large",
            "entity too large",
            "file_parts_invalid",
            "file_part_too_big",
            "file part is too big",
            "file part too big",
            "file parts is invalid",
            "file parts invalid",
            "parts is invalid",
            "parts invalid",
            "上传限制",
            "上传上限",
            "文件过大",
            "request too large",
        )
        file_too_large_type_patterns = (
            "toolarge",
            "fileparttoobig",
            "filepartsinvalid",
            "filepartlengthinvalid",
            "filepartsizeinvalid",
        )
        if any(p in err_str for p in file_too_large_str_patterns) or any(
            p in err_type for p in file_too_large_type_patterns
        ):
            return "FILE_TOO_LARGE"

        # 5. 用户主动取消 / 中断
        if "cancel" in err_str or "cancel" in err_type or "abort" in err_str:
            return "USER_CANCELLED"

        # 6. 文件系统与磁盘 I/O 异常
        file_io_str_patterns = (
            "no such file",
            "permission denied",
            "access is denied",
            "[errno ",
            "no space left",
            "disk full",
            "disk is full",
            "read-only file system",
            "input/output error",
            "目标文件不存在",
            "文件不存在",
            "空文件",
        )
        file_io_type_patterns = (
            "filenotfound",
            "permissionerror",
            "isadirectory",
            "notadirectory",
            "diskfull",
            "fileexists",
        )
        if (
            isinstance(error_obj, (OSError, IOError, FileNotFoundError, PermissionError))
            or any(p in err_str for p in file_io_str_patterns)
            or any(p in err_type for p in file_io_type_patterns)
        ):
            return "FILE_IO_ERROR"

        return "OTHER_ERROR"

    @classmethod
    def log_failure(
        cls,
        file_path: str,
        target_chat_id: Any,
        error_obj: Exception,
        client_type: str = "Unknown",
        task_id: str | None = None,
        log_filename: str = "publish_failures.jsonl",
        file_size_mb: float | None = None,
    ):
        try:
            logger = cls.get_logger(log_filename)
            file_name = os.path.basename(file_path) if file_path else "unknown"
            if file_size_mb is not None:
                calc_size_mb = round(float(file_size_mb), 2)
            elif file_path and os.path.exists(file_path):
                calc_size_mb = round(os.path.getsize(file_path) / (1024 * 1024), 2)
            else:
                calc_size_mb = 0.0
            error_type = cls.classify_error(error_obj)

            # 简短原因：剥离多行 Traceback，只留首行短文本 (最多 150 字符)
            raw_reason = redact_payload(str(error_obj)).split("\n")[0].strip()
            short_reason = raw_reason[:150] if raw_reason else type(error_obj).__name__

            log_entry = {
                "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "task_id": task_id or "",
                "chat_id": str(target_chat_id),
                "file": file_name,
                "size_mb": calc_size_mb,
                "client": client_type,
                "type": error_type,
                "reason": short_reason,
            }
            sanitized_entry = redact_payload(log_entry)
            logger.info(json.dumps(sanitized_entry, ensure_ascii=False))
            for h in logger.handlers:
                h.flush()
        except Exception as exc:
            logging.getLogger(__name__).debug("写入 failure_logger 失败 (非致命忽略): %s", exc)
