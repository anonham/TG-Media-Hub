from typing import Any

from app.core.task_strategy import TaskTypeStrategy
from app.services import publish_runner


class PublishFolderStrategy(TaskTypeStrategy):
    # D4 表驱动参数提取 —— 实证缺陷修复: 原 execute 与 dispatch_execution 两条派发路径
    # 对 album_mode/recursive、delay_seconds/send_delay_seconds 的主键优先级互为倒置
    # (同一 params 走不同路径行为分叉)。现按原 execute (调度器) 路径口径固化单一主键序:
    # album_mode > recursive、delay_seconds > send_delay_seconds, 键存在性优先于值真值性。
    RUN_PARAMS_SPEC = (
        ("folder_path", ("folder_path",), None, None, True),
        ("target_channel", ("target_channel", "target_chat"), None, None, True),
        ("album_mode", ("album_mode", "recursive"), bool, True, False),
        ("delay_seconds", ("delay_seconds", "send_delay_seconds"), float, 3.0, False),
        ("upload_concurrency", ("upload_concurrency",), int, None, False),
        ("upload_in_flight_per_connection", ("upload_in_flight_per_connection",), int, None, False),
        ("force_republish", ("force_republish",), bool, None, False),
        ("oversize_policy", ("oversize_policy",), None, None, False),
        ("send_placeholder_card", ("send_placeholder_card",), bool, None, False),
    )

    @property
    def task_type(self) -> str:
        return "publish_folder"

    @property
    def max_concurrency_limit(self) -> int:
        return 16

    def extract_target_chat(self, params: dict[str, Any]) -> str | None:
        return params.get("target_channel") or params.get("target_chat")

    def validate_required_params(self, params: dict[str, Any]) -> None:
        folder_path = params.get("folder_path")
        target_channel = params.get("target_channel") or params.get("target_chat")
        if not folder_path or not target_channel:
            raise ValueError("任务缺少恢复所需参数(folder_path/target_channel), 无法恢复")

    def apply_concurrency_update(
        self, params: dict[str, Any], concurrency: int, upload_in_flight_per_connection: int | None = None
    ) -> None:
        params["upload_concurrency"] = concurrency
        if upload_in_flight_per_connection is not None:
            params["upload_in_flight_per_connection"] = upload_in_flight_per_connection

    def get_resume_hint(self) -> str:
        return "已发布文件已记录历史，恢复后将自动跳过继续发布"

    def get_resume_log_message(self, feedback_msg: str, params: dict[str, Any]) -> str:
        return f"[调度器] 发布任务恢复：{feedback_msg} (自动跳过已发布文件继续执行)"

    def validate_and_prepare_resume(self, task: dict[str, Any]) -> tuple[bool, dict[str, Any], str]:
        return False, {}, ""

    async def execute(self, task_id: str, params: dict[str, Any], account_id: int | None) -> None:
        await publish_runner.run_publish_folder_task(task_id, account_id=account_id, **self.extract_run_params(params))
