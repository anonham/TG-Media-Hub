"""统一发布流水线传输引擎 - 异常体系定义

定义强类型的传输异常、探测异常及元数据上下文，严格保留异常分类学感知。
"""

from dataclasses import dataclass
from typing import Any

from app.core.iso_bmff.exceptions import IsoBmffError
from app.core.pipeline.models import PublishItem


@dataclass(slots=True)
class VsseProbeContext:
    """熔断降级时随异常透传的 Stage 0 探测产物 (元数据跨层零损耗透传)。"""

    probed_metadata: Any | None = None
    pinned_regions: list[tuple[int, bytes]] | None = None


class TransportError(Exception):
    """传输引擎规范化异常，仅包装第三方未知异常，取代动态挂载 client_label / _vsse_probe_context。"""

    def __init__(
        self,
        message: str,
        *,
        item: PublishItem | None = None,
        client_label: str = "",
        probe_context: Any | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.item = item
        self.client_label = client_label
        self.probe_context = probe_context
        self.cause = cause
        if cause is not None:
            self.__cause__ = cause


class VsseProbeError(IsoBmffError):
    """VSSE 探测/解析阶段失败降级专用异常 (继承 IsoBmffError 保障 100% 熔断降级兼容)。"""

    def __init__(
        self,
        message: str,
        *,
        probe_context: VsseProbeContext | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.probe_context = probe_context
        self.cause = cause
        if cause is not None:
            self.__cause__ = cause
