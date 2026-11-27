"""任务类型抽象策略基类与注册中心 (Task Type Strategy Pattern)

遵循 Clean Architecture 与分层守卫：本模块位于 app/core，零外部依赖 (不依赖 app.api 与 app.services)。
具体执行逻辑通过依赖倒置在 app/services/task_strategies 中实现并在运行时注册。

(执行器双层合并): TaskTypeStrategy.execute 是唯一执行入口 —— 任务创建一律入
task_queue + scheduler 调度; 原 dispatch_execution (BackgroundTasks 即跑通道) 与其
参数提取键优先级倒置缺陷 (publish_folder 的 album_mode/recursive、delay_seconds/
send_delay_seconds 两条路径主键互为倒置) 一并废除, 参数提取统一收敛到表驱动
extract_run_params (RUN_PARAMS_SPEC 声明别名键优先级, 单点可测)。
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, ClassVar


class TaskTypeStrategy(ABC):
    """任务类型多态行为抽象基类"""

    #: 表驱动参数提取规格: (runner 形参名, 别名键元组, 转换器, 缺省值, 是否必传)。
    #: 别名按声明顺序取首个**存在于 params 的键** (键优先级显式化, 修复历史倒置缺陷);
    #: 命中值经 converter 转换 (值为 None 时跳过转换), 无命中取 default;
    #: 结果为 None 且非 required 的形参从运行参数中省略 (交由 runner 签名缺省)。
    RUN_PARAMS_SPEC: tuple[tuple[str, tuple[str, ...], Callable | None, Any, bool], ...] = ()

    @property
    @abstractmethod
    def task_type(self) -> str:
        """任务类型唯一枚举值 (e.g. 'download_link', 'download_range', 'forward_channel', 'publish_folder', 'pikpak_publish')"""

    @property
    @abstractmethod
    def max_concurrency_limit(self) -> int:
        """任务支持的最大并发数限制 (例如下载类为 32，发布类为 16)"""

    @abstractmethod
    def extract_target_chat(self, params: dict[str, Any]) -> str | None:
        """提取用于账号鉴权与配额检查的目标会话/频道标识 (纯参数解析，无 I/O)"""

    @abstractmethod
    def validate_required_params(self, params: dict[str, Any]) -> None:
        """恢复/调度前置参数完整性校验，缺失或非法抛出 ValueError"""

    @abstractmethod
    def apply_concurrency_update(
        self, params: dict[str, Any], concurrency: int, upload_in_flight_per_connection: int | None = None
    ) -> None:
        """更新 params_json 中的任务专属并发配置 (threads vs upload_concurrency / upload_in_flight_per_connection)"""

    @abstractmethod
    def get_resume_hint(self) -> str:
        """系统重启清理孤儿任务时的恢复提示文案"""

    @abstractmethod
    def get_resume_log_message(self, feedback_msg: str, params: dict[str, Any]) -> str:
        """任务恢复执行时的日志提示文案"""

    @abstractmethod
    def validate_and_prepare_resume(self, task: dict[str, Any]) -> tuple[bool, dict[str, Any], str]:
        """恢复前断点预处理与全覆盖判定。

        返回: (is_already_completed, param_patch, message)
        - 若已全量完成，返回 (True, {}, "完成说明文案")；
        - 若需继续恢复，返回 (False, patch_dict, "")。
        """

    @abstractmethod
    async def execute(self, task_id: str, params: dict[str, Any], account_id: int | None) -> None:
        """派发异步执行任务 (唯一执行入口; 参数经 extract_run_params 表驱动提取)"""

    def extract_run_params(self, params: dict[str, Any]) -> dict[str, Any]:
        """表驱动参数提取 (唯一提取点, 调度器 execute 路径与表驱动测试共用)。

        别名解析取首个「存在且非 None」的键: 显式 None 不再遮蔽后续非空别名
        (—— validate 层用 or 口径, 校验通过但 dispatch 取到 None 的
        两侧不一致缺陷); 布尔等显式假值 (album_mode=False) 非 None, 存在性语义保持生效。
        """
        out: dict[str, Any] = {}
        for out_key, aliases, converter, default, required in self.RUN_PARAMS_SPEC:
            raw = next(
                (
                    params[k]
                    for k in aliases
                    if params.get(k) is not None
                    and not (required and isinstance(params[k], str) and not params[k].strip())
                ),
                default,
            )
            val = converter(raw) if (converter is not None and raw is not None) else raw
            if val is not None or required:
                out[out_key] = val
        return out


class StrategyRegistry:
    """策略全局单例注册中心"""

    _strategies: ClassVar[dict[str, TaskTypeStrategy]] = {}

    @classmethod
    def register(cls, strategy: TaskTypeStrategy) -> None:
        """注册具体策略实例"""
        cls._strategies[strategy.task_type] = strategy

    @classmethod
    def get(cls, task_type: str) -> TaskTypeStrategy:
        """获取指定任务类型的策略实例，若未注册抛出 KeyError"""
        strategy = cls._strategies.get(task_type)
        if not strategy:
            raise KeyError(f"未注册的任务类型策略: {task_type}")
        return strategy

    @classmethod
    def get_optional(cls, task_type: str) -> TaskTypeStrategy | None:
        """安全获取指定任务类型的策略实例"""
        return cls._strategies.get(task_type)

    @classmethod
    def supported_types(cls) -> list[str]:
        """返回所有已注册的任务类型列表"""
        return list(cls._strategies.keys())

    @classmethod
    def clear(cls) -> None:
        """清空注册表 (仅用于单元测试重置)"""
        cls._strategies.clear()
