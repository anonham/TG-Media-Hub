"""PikPak 发布失败分类学与 CLI 域异常体系 (自 pikpak_publisher_service / pikpak_service 下沉)。

core 层唯一实现, services 侧旧名 re-export 兼容 (执行器 / API / 测试 / GC 的既有 import 面不变)。
纯定义零 IO —— 依赖方向: app.services.{pikpak_service, pikpak_publisher_service} → 本模块, 严禁反转。

import 注记: CLI 域 PikPakRiskControlError (pikpak_service 控制面) 与 Provider 域
PikPakRiskControlError (stream_chunk_provider 数据面) 同名不同层, 消费方分别以别名导入, 不得混用。
"""

from typing import Any

from app.core.pikpak.expander import ParsedFileItem
from app.core.scheduler import FatalTaskError, RetryableTaskError

# ---------------------------------------------------------------------------
# CLI 域异常体系 (自 pikpak_service 下沉; message 面向 UI, 禁含凭据/签名直链)
# ---------------------------------------------------------------------------


class PikPakError(Exception):
    """PikPak CLI 域错误基类; message 面向 UI, 禁含凭据/签名直链 (经 redact_text)"""


class PikPakAuthError(PikPakError):
    """认证/授权/凭据域 (UNAUTHENTICATED/UNAUTHORIZED/TOKEN_EXPIRED/TOKEN_REFRESH_FAILED/CREDENTIALS_ERROR/ACCESS_DENIED)"""


class PikPakQuotaError(PikPakError):
    """配额域 (CONNECTED_APPS_QUOTA_EXCEEDED/FILE_SPACE_NOT_ENOUGH/TRANSFER_QUOTA_EXCEEDED)"""


class PikPakNotFoundError(PikPakError):
    """资源不存在 (FILE_NOT_FOUND/INVALID_FILE_ID/INVALID_PARENT_ID)"""


class PikPakRiskControlError(PikPakError):
    """风控/验证页特征 (CLI 无稳定错误码, 预留映射; 真实样本到位后校准, 见 )"""


class PikPakTransientError(PikPakError):
    """可重试瞬断 (NETWORK_ERROR/TIMEOUT/REQUEST_FAILED; P3 执行器失败分类复用)"""


class PikPakCliError(PikPakError):
    """其余 CLI 域失败 (非零退出/超时/JSON 解析失败, 携带原始 code)"""


class PikPakCliTimeoutError(PikPakCliError):
    """PikPak CLI 等待并发槽或子进程执行超时"""


# ---------------------------------------------------------------------------
# 发布失败分类学
# ---------------------------------------------------------------------------

FAILURE_DETAIL_TEXT = {
    "transient": "PikPak 网络瞬断，容灾重试后仍未恢复，任务将自动重试 (已发布文件自动跳过，未完成文件断点续传)",
    "link_expired": "PikPak 直链过期且重新索链失败，任务将自动重试 (下次执行将重新 JIT 索链)",
    "throttled": "PikPak CDN 节点触发频控或临时过载 (HTTP 429/503)，系统将执行静默冷却后自动续传",
    "auth": "PikPak 凭据失效且自动恢复失败，请人工介入: 前往 PikPak 页面重新完成设备码登录绑定",
    "quota": "PikPak 配额/限速受限 (CONNECTED_APPS_QUOTA_EXCEEDED 等)，请检查账号配额后重试",
    "not_found": "PikPak 文件不存在或已被删除/移动，请检查网盘文件后重试",
    "risk_control": "PikPak 触发风控 (返回登录页/验证页特征)，请人工登录处理后再试",
    "cli": "PikPak CLI 调用失败，请查看任务日志与服务端日志定位",
    "permission": "执行账号对目标频道无发帖权限，任务已中止，请更换账号或调整频道权限",
    "userbot": "无可用 Userbot 账号 (强制 Userbot 模式，Bot 池不开放)，请先绑定并就绪 Userbot",
    "unknown": "发布过程中发生未预期错误，请查看任务日志与服务端日志",
}

# audit 通过态取值: 真实 CLI 样本为 STATUS_OK, 兼容 OK; 空/缺失宽容放行 —— 宁漏拦不误拦
AUDIT_PASS_STATUSES = frozenset({"OK", "STATUS_OK"})


class PikPakPublishError(Exception):
    """PikPak 发布链路包装异常基类: 携带分类与面向 UI 的中文文案 (str(exc) == user_detail)。"""

    def __init__(
        self,
        category: str = "unknown",
        user_detail: str | None = None,
        *,
        retry_after: float | None = None,
    ):
        self.category = category
        self.user_detail = user_detail or FAILURE_DETAIL_TEXT.get(category, FAILURE_DETAIL_TEXT["unknown"])
        self.retry_after = retry_after
        super().__init__(self.user_detail)


class PikPakPublishRetryableError(PikPakPublishError, RetryableTaskError):
    """任务级可重试: 调度器退避重试 (已发布文件防重跳过、未完成文件断点 meta 续传)。"""


class PikPakPublishFatalError(PikPakPublishError, FatalTaskError):
    """致命/人工介入: 调度器直接 failed 终态, 不自动重试。"""


class PikPakPublishThrottledError(PikPakPublishRetryableError):
    """PikPak CDN 节点频控 (HTTP 429/502/503/504)，触发调度器三级阶梯无人值守自愈。"""

    def __init__(self, message: str | None = None, *, retry_after: float | None = None):
        detail = message or FAILURE_DETAIL_TEXT["throttled"]
        super().__init__("throttled", detail, retry_after=retry_after)


class PikPakPublishRiskControlError(PikPakPublishError):
    """PikPak 边缘 WAF 或控制面风控拦截。
    is_data_plane=True: 数据面 HTML 质询 (Level 3a，首犯进入 Tier 2，二次 Safe-Pause)
    is_data_plane=False: 控制面 API 风控 (Level 3b，直接 Safe-Pause)
    """

    def __init__(
        self,
        message: str | None = None,
        *,
        is_data_plane: bool = False,
        retry_after: float | None = None,
    ):
        self.is_data_plane = is_data_plane
        detail = message or FAILURE_DETAIL_TEXT["risk_control"]
        super().__init__("risk_control", detail, retry_after=retry_after)


def classify_pikpak_failure(exc: Exception) -> tuple[str, str]:
    """异常 → (category, user_detail 中文文案)。纯函数, 供处置上抛点与执行器兜底映射复用。

    Provider 域 (数据面) 与 CLI 域 (控制面) 异常分别判定; 未知异常归 unknown。
    """
    from app.core.stream_chunk_provider import (
        ChunkFetchTransientError,
        PikPakLinkExpiredError,
        PikPakThrottledError,
    )
    from app.core.stream_chunk_provider import (
        PikPakNotFoundError as ProviderPikPakNotFoundError,
    )
    from app.core.stream_chunk_provider import (
        PikPakRiskControlError as ProviderPikPakRiskControlError,
    )

    if isinstance(exc, PikPakThrottledError):
        category = "throttled"
    elif isinstance(exc, ChunkFetchTransientError):
        category = "transient"
    elif isinstance(exc, PikPakLinkExpiredError):
        category = "link_expired"
    elif isinstance(exc, ProviderPikPakRiskControlError):
        category = "risk_control"
    elif isinstance(exc, PikPakAuthError):
        category = "auth"
    elif isinstance(exc, PikPakQuotaError):
        category = "quota"
    elif isinstance(exc, (PikPakNotFoundError, ProviderPikPakNotFoundError)):
        category = "not_found"
    elif isinstance(exc, PikPakRiskControlError):
        category = "risk_control"
    elif isinstance(exc, PikPakTransientError):
        category = "transient"
    elif isinstance(exc, PikPakError):
        category = "cli"
    else:
        category = "unknown"
    return category, FAILURE_DETAIL_TEXT.get(category, FAILURE_DETAIL_TEXT["unknown"])


def audit_blocked_reason(item: ParsedFileItem | Any) -> str | None:
    """audit 拦截判定: 非通过 → 返回原因文案 (UI 标红数据源); 缺失/空 → 宽容放行。
    兼顾 ParsedFileItem 与 PublishItem 双重契约。"""
    audit = getattr(item, "audit", None) or {}
    status = str(audit.get("status") or "").strip().upper()
    if not status:
        return None
    if status not in AUDIT_PASS_STATUSES:
        reason = str(audit.get("message") or audit.get("title") or status)
        return f"审核状态未通过 ({reason})"
    return None


def wrap_pikpak_failure(exc: Exception) -> PikPakPublishError:
    """按分类学包装 (engines._wrap_failure 语义): 已是发布分类学异常 → 原样返回;
    throttled → PikPakPublishThrottledError;
    risk_control → PikPakPublishRiskControlError (区分数据面与控制面);
    transient/link_expired → Retryable (调度器退避重试);
    其余已知 CLI/Provider 域 → Fatal。"""
    if isinstance(exc, PikPakPublishError):
        return exc
    from app.core.stream_chunk_provider import (
        PikPakRiskControlError as ProviderPikPakRiskControlError,
    )
    from app.core.stream_chunk_provider import (
        PikPakThrottledError,
    )

    if isinstance(exc, PikPakThrottledError):
        return PikPakPublishThrottledError(str(exc), retry_after=exc.retry_after)
    if isinstance(exc, ProviderPikPakRiskControlError):
        return PikPakPublishRiskControlError(
            str(exc), is_data_plane=True, retry_after=getattr(exc, "retry_after", None)
        )
    if isinstance(exc, PikPakRiskControlError):
        return PikPakPublishRiskControlError(str(exc), is_data_plane=False)

    category, detail = classify_pikpak_failure(exc)
    retry_after = getattr(exc, "retry_after", None)
    if category == "throttled":
        return PikPakPublishThrottledError(detail, retry_after=retry_after)
    if category == "risk_control":
        is_dp = getattr(exc, "is_data_plane", False)
        return PikPakPublishRiskControlError(detail, is_data_plane=is_dp, retry_after=retry_after)
    if category in ("transient", "link_expired"):
        return PikPakPublishRetryableError(category, detail, retry_after=retry_after)
    return PikPakPublishFatalError(category, detail, retry_after=retry_after)
