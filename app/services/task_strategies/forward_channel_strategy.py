from typing import Any

from app.core.task_strategy import TaskTypeStrategy
from app.services import forward_service
from app.services.forward_service import ForwardService


class ForwardChannelStrategy(TaskTypeStrategy):
    # D4 表驱动参数提取: 两路径原提取语义一致, 主键无歧义
    RUN_PARAMS_SPEC = (
        ("source_chat", ("canonical_source", "source_chat"), None, None, True),
        ("target_chat", ("canonical_target", "target_chat"), None, None, True),
        ("start_id", ("start_id",), int, None, False),
        ("end_id", ("end_id",), int, None, False),
        ("batch_size", ("batch_size",), int, 50, False),
        ("delay_seconds", ("delay_seconds",), float, 1.5, False),
        ("full_retransfer", ("full_retransfer",), bool, False, False),
        ("dry_run", ("dry_run",), bool, False, False),
    )

    @property
    def task_type(self) -> str:
        return "forward_channel"

    @property
    def max_concurrency_limit(self) -> int:
        return 32

    def extract_target_chat(self, params: dict[str, Any]) -> str | None:
        return params.get("source_chat")

    def validate_required_params(self, params: dict[str, Any]) -> None:
        source_chat = params.get("source_chat")
        target_chat = params.get("target_chat")
        if not source_chat or not target_chat:
            raise ValueError("任务缺少恢复所需参数(source_chat/target_chat), 无法恢复")

    def apply_concurrency_update(
        self, params: dict[str, Any], concurrency: int, upload_in_flight_per_connection: int | None = None
    ) -> None:
        params["threads"] = concurrency

    def get_resume_hint(self) -> str:
        return "转发账本已记录进度，恢复后将按防重自动续转 (不会产生重复消息)"

    def get_resume_log_message(self, feedback_msg: str, params: dict[str, Any]) -> str:
        start_id = params.get("start_id")
        return f"[调度器] 转发任务恢复：{feedback_msg} (从断点 #{start_id} 继续转发)"

    def validate_and_prepare_resume(self, task: dict[str, Any]) -> tuple[bool, dict[str, Any], str]:
        params = task.get("params") or {}
        patch: dict[str, Any] = {}
        source_chat = params.get("canonical_source") or params.get("source_chat")
        target_chat = params.get("canonical_target") or params.get("target_chat")
        has_pinned_peers = bool(params.get("canonical_source") and params.get("canonical_target"))
        start_id = params.get("start_id")
        end_id = params.get("end_id")
        # 普通群历史消息序号随账号变化，必须先在线解析并校验账本归属，再逐批防重。
        parsed_source = ForwardService.parse_chat_identifier(source_chat) if source_chat is not None else ""
        if isinstance(parsed_source, int) and -(10**12) < parsed_source < 0:
            return False, {"dry_run": False}, ""
        if source_chat and target_chat:
            resume_start, resume_mode = ForwardService.compute_resume_start(
                source_chat,
                target_chat,
                start_id,
                end_id,
                full_retransfer=bool(params.get("full_retransfer")),
                allow_alias_cache=has_pinned_peers,
            )
            if resume_mode == "covered":
                return True, {}, "该源→目标在指定范围内已全覆盖，无需重转"
            patch["dry_run"] = False
            if resume_start is not None:
                patch["start_id"] = resume_start
        return False, patch, ""

    async def execute(self, task_id: str, params: dict[str, Any], account_id: int | None) -> None:
        await forward_service.run_forward_channel_task(
            task_id, account_id=account_id, **self.extract_run_params(params)
        )
