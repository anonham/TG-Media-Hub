"""统一发布流水线接口协议 (Interfaces & Protocols)

V2 策略化: ItemSource 携带 Phase 3 通用过滤 hooks (拒绝/audit/防重/超限阈值),
MediaTransportEngine 统一 TransportOutcome 契约 —— 数据形态差异内聚策略对象,
编排层 (pipeline) 只保留介质无关流程。
传输引擎对账号池 (ClientRegistryPort) 与任务仓储 (TaskStorePort) 的
依赖面协议化 —— 引擎构造注入端口, 默认值 = 现状单例适配器 (调用期经模块属性
读取, 既有测试 patch 路径 100% 保持穿透)。
"""

from abc import ABC, abstractmethod
from typing import Any, Protocol

from app.core import client_manager as cm_mod
from app.core.pipeline.models import (
    DedupQuery,
    PublishBatch,
    PublishContext,
    PublishItem,
    PublishResult,
    TransportOutcome,
)
from app.db import task_repository as tr_mod


class ItemSource(ABC):
    """数据源抽象接口"""

    @abstractmethod
    async def get_batches(self, context: PublishContext) -> list[PublishBatch]:
        """拉取并归一化为 PublishBatch 批次列表，按子目录组织媒体项"""

    # ---- Phase 3 通用过滤 hooks (V2: audit 判定为 pikpak-only 注入钩子, 基类恒放行) ----

    def reject_item(self, item: PublishItem, context: PublishContext) -> str | None:
        """条目预过滤: 返回拒绝文案 (计入 failed_files), None = 放行。基类不拒绝。"""
        return None

    def audit_block_reason(self, item: PublishItem, context: PublishContext) -> str | None:
        """audit 拦截判定 (pikpak-only 钩子): 非通过返回原因文案; 基类恒放行。"""
        return None

    def dedup_query(self, item: PublishItem, context: PublishContext) -> DedupQuery:
        """防重查询参数 (pipeline 单点调用 PublishHistoryRepository.is_published)。
        默认按本地三元组语义 (路径+大小, 无哈希); 数据源可覆写。"""
        return DedupQuery(local_path=item.item_id, size_bytes=item.size_bytes, source_type="local")

    def oversize_limit_bytes(self, item: PublishItem, context: PublishContext) -> int | None:
        """单文件超限阈值 (字节); None/0 = 不判超限。默认取账号协议上限。"""
        return context.task_limit_bytes or None


class MediaTransportEngine(ABC):
    """媒体传输策略接口 (统一 TransportOutcome 契约)"""

    @abstractmethod
    async def send_item(self, item: PublishItem, context: PublishContext) -> TransportOutcome:
        """传输并发送单个媒体条目"""

    async def send_album(self, items: list[PublishItem], context: PublishContext) -> list[TransportOutcome]:
        """批量发送图片 Album（仅部分引擎支持，默认逐个回退）"""
        results = []
        for item in items:
            results.append(await self.send_item(item, context))
        return results

    async def edit_item(self, item: PublishItem, context: PublishContext, msg_id: int) -> TransportOutcome:
        """原地回填 —— 将占位卡消息编辑为正式媒体 (F1 两段式: 先 upload 拿
        handle 再 edit_message(file=handle); D4 恒 userbot)。默认不支持,
        编排层探测到未覆写即回退常规发送。"""
        raise NotImplementedError("该引擎不支持原地回填")

    async def preflight_item(self, item: PublishItem, context: PublishContext) -> tuple[bool, str | None]:
        """传输前单条目预检钩子 (默认放行; 引擎可覆写以在超限治理/回填前拦截无效条目)。"""
        return True, None


class CleanupStrategy(ABC):
    """资产善后清理策略接口"""

    async def archive_single_item(self, item: PublishItem, context: PublishContext) -> bool:
        """单文件上传成功后触发的即时归档移动 (默认空实现)"""
        return False

    @abstractmethod
    async def cleanup(self, items: list[PublishItem], context: PublishContext) -> str | None:
        """对已发布的条目执行归档或删除操作，返回归档摘要文本（失败不影响任务终态）"""


class NotificationStrategy(ABC):
    """终态告示与通知策略接口"""

    @abstractmethod
    async def notify_batch(self, batch: PublishBatch, context: PublishContext, result: PublishResult) -> None:
        """根据批次发布形态向目标频道发送总结告示卡片（仅文件夹形态且有新发送或失败文件时发送）"""

    async def notify_task_summary(self, context: PublishContext, batch_reports: list[PublishResult]) -> None:
        """任务级汇总卡: 批次卡之外整任务一张; 基类空实现。"""
        return


class ClientRegistryPort(Protocol):
    """客户端注册表端口: 传输引擎对账号池能力的最小依赖面。

    面 = engines 包实测调用面 (userbot/bots/bot_index/get_next_available_bot/mark_flood_wait),
    以 engines 实际调用为准最小化定义, 后续按需演进。
    """

    @property
    def userbot(self) -> Any: ...

    @property
    def bots(self) -> list[Any]: ...

    @property
    def bot_index(self) -> int: ...

    def get_next_available_bot(self) -> Any: ...

    def mark_flood_wait(self, client: Any, wait_seconds: int) -> None: ...


class TaskStorePort(Protocol):
    """任务仓储端口: 引擎对持久层的最小依赖面 (日志 / 检查点 / 任务读取)。"""

    async def append_log(self, task_id: str, line: str) -> None: ...

    async def get_by_id(self, task_id: str) -> dict[str, Any] | None: ...

    async def record_part_checkpoint(
        self,
        task_id: str,
        file_id: str,
        strategy: str,
        total_parts: int,
        threshold: int,
        part_index: int,
        msg_id: int,
        plan_signature: str | None = None,
    ) -> None: ...

    async def clear_part_checkpoint(self, task_id: str, file_id: str) -> None: ...


class _DefaultClientRegistry:
    """默认适配器: 每次调用经模块属性访问读取现状单例 —— 不在构造期捕获,
    patch("app.core.client_manager.client_manager.<attr>") 照常穿透 (测试零改动)。
    """

    @property
    def userbot(self) -> Any:
        return cm_mod.client_manager.userbot

    @property
    def bots(self) -> list[Any]:
        return cm_mod.client_manager.bots

    @property
    def bot_index(self) -> int:
        return cm_mod.client_manager.bot_index

    def get_next_available_bot(self) -> Any:
        return cm_mod.client_manager.get_next_available_bot()

    def mark_flood_wait(self, client: Any, wait_seconds: int) -> None:
        cm_mod.client_manager.mark_flood_wait(client, wait_seconds)


class _DefaultTaskStore:
    """默认适配器: 调用时经模块属性访问读取 TaskQueueRepository 类属性 ——
    patch.object(TaskQueueRepository, "<method>") 类级 patch 照常穿透 (测试零改动)。
    """

    async def append_log(self, task_id: str, line: str) -> None:
        await tr_mod.TaskQueueRepository.append_log_async(task_id, line)

    async def get_by_id(self, task_id: str) -> dict[str, Any] | None:
        return await tr_mod.TaskQueueRepository.get_by_id_async(task_id)

    async def record_part_checkpoint(
        self,
        task_id: str,
        file_id: str,
        strategy: str,
        total_parts: int,
        threshold: int,
        part_index: int,
        msg_id: int,
        plan_signature: str | None = None,
    ) -> None:
        await tr_mod.TaskQueueRepository.record_part_checkpoint_async(
            task_id, file_id, strategy, total_parts, threshold, part_index, msg_id, plan_signature=plan_signature
        )

    async def clear_part_checkpoint(self, task_id: str, file_id: str) -> None:
        await tr_mod.TaskQueueRepository.clear_part_checkpoint_async(task_id, file_id)


DEFAULT_CLIENT_REGISTRY: ClientRegistryPort = _DefaultClientRegistry()
DEFAULT_TASK_STORE: TaskStorePort = _DefaultTaskStore()
