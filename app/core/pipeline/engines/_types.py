"""统一发布流水线传输引擎 - 类型定义与通道协议契约"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol, TypedDict, runtime_checkable

from app.core.e2e_capture import E2ECaptureSession
from app.core.pipeline.interfaces import TaskStorePort
from app.core.pipeline.models import PublishContext


@runtime_checkable
class SplitVolumePlan(Protocol):
    """分卷计划公共面协议: VSSE 信封计划与 fMP4 时间窗计划的结构交集。

    通道私有字段 (VSSE 信封偏移 / fMP4 时间窗与 is_blind) 留在各通道具体类型内,
    由通道经自持的具体 plan 列表访问; 本协议只承载分卷主循环与进度分发所需公共面。
    成员声明为只读 property (与冻结 dataclass 具体类型的只读字段结构兼容)。
    注: 计划书草案中的 estimated_size 在两个具体 plan 类型上均不存在 —— 分卷字节
    估算职责本就在通道 get_cached_volume_bytes, 不入公共面 (执行勘误)。
    """

    @property
    def part_index(self) -> int: ...

    @property
    def total_parts(self) -> int: ...

    @property
    def part_name(self) -> str: ...

    @property
    def duration_sec(self) -> float: ...


@dataclass(frozen=True, slots=True)
class VolumeJob:
    """单个分卷任务的不可变描述对象。"""

    part_index: int
    total_parts: int
    plan: SplitVolumePlan
    prior_bytes: int
    estimated_bytes: int
    part_name: str


@dataclass(slots=True)
class VolumeProgress:
    """分卷循环运行时交付进度状态。"""

    delivered_message_ids: list[int]
    prior_bytes: int = 0
    last_speed: float = 0.0


class SendFileKwargs(TypedDict, total=False):
    """Telethon send_file 关键字参数字典。"""

    caption: str
    parse_mode: str
    thumb: Any
    attributes: list[Any]
    progress_callback: Any
    supports_streaming: bool
    force_document: bool
    mime_type: str


class UploadProgressHook(Protocol):
    """上传进度回调通用协议。"""

    def __call__(self, current: int, total: int, *args: Any, **kwargs: Any) -> None: ...


@runtime_checkable
class SplitChannel(Protocol):
    """虚拟分卷 (VSSE) 与动态重切片 (fMP4) 传输通道协议契约。"""

    channel_name: str
    channel_label: str

    def plan_volumes(self, force_parts: int | None = None) -> Sequence[SplitVolumePlan]:
        """规划或重规划分卷切片。"""
        ...

    def get_plan_signature(self, plans: Sequence[SplitVolumePlan]) -> str | None:
        """返回分卷切分边界签名 (用于断点恢复时核对边界一致性; None 表示不校验)。"""
        ...

    def get_cached_volume_bytes(self, plan_idx: int, plan: SplitVolumePlan) -> int:
        """检查点命中时，计算已发送分卷的估算或精确字节数。"""
        ...

    async def prepare_part(self, plan_idx: int, plan: SplitVolumePlan) -> None:
        """分卷开始传输前的前置准备 (如 JIT 索链刷新)。"""
        ...

    async def stream_part(
        self,
        job: VolumeJob,
        client: Any,
        context: PublishContext,
        capture_session: E2ECaptureSession,
        reporter: Any,
    ) -> tuple[int, int, float]:
        """流式上传单个分卷，返回 (msg_id, emitted_bytes, speed)。"""
        ...

    async def handle_oversize(
        self,
        client: Any,
        context: PublishContext,
        capture_session: E2ECaptureSession,
        plans: Sequence[SplitVolumePlan],
        delivered_message_ids: list[int],
        replan_count: int,
        max_replan_attempts: int,
        exc: Exception,
        item_id: str,
        task_store: TaskStorePort,
    ) -> int:
        """处理分卷超限异常并返回重规划强制分卷数 (forced_parts)。"""
        ...

    async def aclose(self) -> None:
        """资源释放与清理 (关闭代理、provider 等)。"""
        ...
