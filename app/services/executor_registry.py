"""任务执行器与任务类型策略显式注册中心 (Task Executor & Strategy Registry)

为 TaskScheduler 和 StrategyRegistry 显式注册内置任务策略，解耦 app/core 与具体业务执行器的直接依赖。
"""

from typing import TYPE_CHECKING, Optional

from app.core.task_strategy import StrategyRegistry
from app.services.task_strategies import (
    DownloadLinkStrategy,
    DownloadRangeStrategy,
    ForwardChannelStrategy,
    PikPakPublishStrategy,
    PublishFolderStrategy,
)

if TYPE_CHECKING:
    from app.core.scheduler import TaskScheduler


def register_default_executors(scheduler: Optional["TaskScheduler"] = None) -> None:
    """向策略注册中心与调度器显式注册系统内置任务策略与执行器"""
    strategies = [
        DownloadLinkStrategy(),
        DownloadRangeStrategy(),
        ForwardChannelStrategy(),
        PublishFolderStrategy(),
        PikPakPublishStrategy(),
    ]
    for strategy in strategies:
        StrategyRegistry.register(strategy)
        if scheduler is not None:
            scheduler.register_executor(strategy.task_type, strategy.execute)
