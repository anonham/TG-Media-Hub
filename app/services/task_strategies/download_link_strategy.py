from typing import Any

from app.core.task_strategy import TaskTypeStrategy
from app.services import download_runner


class DownloadLinkStrategy(TaskTypeStrategy):
    # D4 表驱动参数提取: threads 真值才收敛为 int (0/None → 省略, 交由 runner 缺省 16)
    RUN_PARAMS_SPEC = (
        ("links", ("links",), None, None, True),
        ("threads", ("threads",), lambda v: int(v) if v else None, None, False),
        ("force", ("force",), bool, False, False),
    )

    @property
    def task_type(self) -> str:
        return "download_link"

    @property
    def max_concurrency_limit(self) -> int:
        return 32

    def extract_target_chat(self, params: dict[str, Any]) -> str | None:
        links_val = params.get("links") or []
        if isinstance(links_val, list) and links_val:
            return links_val[0]
        if isinstance(links_val, str):
            first_line = links_val.split("\n")[0].strip()
            return first_line if first_line else None
        return None

    def validate_required_params(self, params: dict[str, Any]) -> None:
        links = params.get("links") or []
        if not links:
            raise ValueError("任务缺少恢复所需参数(links), 无法恢复")

    def apply_concurrency_update(
        self, params: dict[str, Any], concurrency: int, upload_in_flight_per_connection: int | None = None
    ) -> None:
        params["threads"] = concurrency

    def get_resume_hint(self) -> str:
        return "保留的 .part/.meta 断点文件可随时恢复续传"

    def get_resume_log_message(self, feedback_msg: str, params: dict[str, Any]) -> str:
        return f"[调度器] 下载任务恢复：{feedback_msg} (将从 .part/.meta 断点续传)"

    def validate_and_prepare_resume(self, task: dict[str, Any]) -> tuple[bool, dict[str, Any], str]:
        # 下载链接任务无前置完成态跳过逻辑，从既有断点续传
        return False, {}, ""

    async def execute(self, task_id: str, params: dict[str, Any], account_id: int | None) -> None:
        await download_runner.run_download_links_task(task_id, account_id=account_id, **self.extract_run_params(params))
