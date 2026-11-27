"""Telegram 会话失效异常的统一识别与上报

下载/转发/发布三条链路在关键异常处调用 is_session_invalid_error() 识别
Unauthorized / AuthKeyDuplicated / SessionRevoked 等会话级失效，
再由 report_session_invalid() 完成统一处置:
任务置 failed + 指引日志 + 引擎标记失效 + WS 全局事件 (system_event) 广播。
"""

import logging

from app.core.connection_manager import connection_manager
from app.db.task_repository import TaskQueueRepository

logger = logging.getLogger(__name__)


# Telethon 会话失效异常类型 (getattr 防御不同版本类名缺失)
def _collect_session_invalid_types() -> tuple:
    from telethon import errors as tl_errors

    names = (
        "UnauthorizedError",
        "AuthKeyError",
        "AuthKeyDuplicatedError",
        "AuthKeyUnregisteredError",
        "AuthKeyInvalidError",
        "AuthKeyNotFoundError",
        "SessionRevokedError",
        "SessionExpiredError",
        "SessionDuplicatedError",
        "UserDeactivatedError",
        "UserDeactivatedBanError",
    )
    return tuple(t for t in (getattr(tl_errors, n, None) for n in names) if isinstance(t, type))


_SESSION_INVALID_TYPES = _collect_session_invalid_types()

# 错误文本兜底特征 (类型名未命中时按消息文本识别, 含空格/下划线两种变体)
_SESSION_INVALID_TEXT_PATTERNS = (
    "unauthorized",
    "auth key duplicated",
    "auth key unregistered",
    "auth key invalid",
    "auth key not found",
    "authkeyduplicated",
    "authkeyunregistered",
    "auth_key_duplicated",
    "auth_key_unregistered",
    "auth_key_invalid",
    "auth_key_not_found",
    "session revoked",
    "session expired",
    "session duplicated",
    "session_revoked",
    "session_expired",
    "user deactivated",
    "user_deactivated",
)


def _matches_session_invalid_text(text: str) -> bool:
    lowered = text.lower()
    return any(p in lowered for p in _SESSION_INVALID_TEXT_PATTERNS)


def _has_session_invalid_chunk_error(exc: BaseException) -> bool:
    last_exceptions = getattr(exc, "last_exceptions", None)
    last_errors = getattr(exc, "last_errors", None)
    if not isinstance(last_exceptions, dict) and not isinstance(last_errors, dict):
        return False

    exc_map = last_exceptions if isinstance(last_exceptions, dict) else {}
    err_map = last_errors if isinstance(last_errors, dict) else {}
    missing_offsets = getattr(exc, "missing_offsets", None)
    offsets = missing_offsets if missing_offsets else list(set(exc_map.keys()) | set(err_map.keys()))
    for off in offsets:
        sub_exc = exc_map.get(off)
        if isinstance(sub_exc, BaseException) and sub_exc is not exc and is_session_invalid_error(sub_exc):
            return True
        sub_err = err_map.get(off)
        if isinstance(sub_err, BaseException):
            if sub_err is not exc and is_session_invalid_error(sub_err):
                return True
        elif sub_err is not None and _matches_session_invalid_text(str(sub_err)):
            return True
    return False


def _has_chunk_failure_maps(exc: BaseException) -> bool:
    """聚合下载异常是否携带逐块失败明细 (last_errors/last_exceptions)"""
    return isinstance(getattr(exc, "last_exceptions", None), dict) or isinstance(
        getattr(exc, "last_errors", None), dict
    )


def is_session_invalid_error(exc: BaseException) -> bool:
    """判定异常是否属于 Telegram 会话失效 (需重新绑定账号才能恢复)

    聚合下载异常 (携带逐块失败明细) 的消息文本内嵌媒体文件名 (发布者可控，
    如 unauthorized.bin)，不得参与鉴权文本分类 —— 只认逐块明细中的会话失效
    证据，避免普通下载失败触发跨账号误伤 (安全审查发现)。
    """
    if not exc:
        return False
    if _SESSION_INVALID_TYPES and isinstance(exc, _SESSION_INVALID_TYPES):
        return True
    if _has_chunk_failure_maps(exc):
        return _has_session_invalid_chunk_error(exc)
    return _matches_session_invalid_text(f"{type(exc).__name__} {exc}")


async def report_session_invalid(task_id: str | None, exc: BaseException, account_id: int | None = None) -> None:
    """会话失效统一处置: 任务置 failed + 重绑指引 + 引擎失效标记 + WS system_event 广播"""
    from app.core.client_manager import client_manager

    detail = f"{type(exc).__name__}: {exc}"
    logger.error("Telegram 账号会话已失效: %s (account_id=%s)", detail, account_id)

    client_manager.invalidate_session(detail, account_id=account_id)

    if task_id:
        await TaskQueueRepository.update_status_async(task_id, "failed")
        await TaskQueueRepository.append_log_async(
            task_id,
            "⚠️ Telegram 账号会话已失效，任务已终止。请通过页面顶部【重新绑定账号】完成重新登录后，再对该任务点击【重试】。",
        )
        await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "failed"})

    # 全局系统事件 (非任务域): 前端据此展示顶部横幅并引导重绑
    from typing import Any

    payload: dict[str, Any] = {
        "type": "system_event",
        "event": "session_invalid",
        "message": "Telegram 账号会话已失效，请重新绑定账号",
        "detail": detail,
    }
    if account_id is not None:
        payload["account_id"] = account_id
    await connection_manager.broadcast(payload)
