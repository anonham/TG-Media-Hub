import logging
from typing import Any

from app.core.task_strategy import TaskTypeStrategy
from app.services import download_runner

logger = logging.getLogger(__name__)


class DownloadRangeStrategy(TaskTypeStrategy):
    # D4 表驱动参数提取: start_id/end_id 缺省 1 (与原 execute 路径 int(params.get(k, 1)) 一致);
    # threads 真值才收敛为 int (0/None → 省略, 交由 runner 缺省 16)
    RUN_PARAMS_SPEC = (
        ("channel_identifier", ("channel", "channel_identifier"), None, None, True),
        ("start_id", ("start_id",), int, 1, True),
        ("end_id", ("end_id",), int, 1, True),
        ("threads", ("threads",), lambda v: int(v) if v else None, None, False),
        ("force", ("force",), bool, False, False),
    )

    @property
    def task_type(self) -> str:
        return "download_range"

    @property
    def max_concurrency_limit(self) -> int:
        return 32

    def extract_target_chat(self, params: dict[str, Any]) -> str | None:
        return params.get("channel") or params.get("channel_identifier")

    def validate_required_params(self, params: dict[str, Any]) -> None:
        # 与 extract_target_chat/RUN_PARAMS_SPEC 同口径: channel_identifier 是一等别名,
        # 缺失会让带该别名的暂停任务在 resume 校验处被误拒
        channel = params.get("channel") or params.get("channel_identifier")
        start_id = params.get("start_id")
        end_id = params.get("end_id")
        if not channel or start_id is None or end_id is None:
            raise ValueError("任务缺少恢复所需参数(channel/start_id/end_id), 无法恢复")
        try:
            int(start_id)
            int(end_id)
        except (ValueError, TypeError):
            raise ValueError("任务参数(start_id/end_id)格式无效, 无法恢复") from None

    def apply_concurrency_update(
        self, params: dict[str, Any], concurrency: int, upload_in_flight_per_connection: int | None = None
    ) -> None:
        params["threads"] = concurrency

    def get_resume_hint(self) -> str:
        return "保留的 .part/.meta 断点文件可随时恢复续传"

    def get_resume_log_message(self, feedback_msg: str, params: dict[str, Any]) -> str:
        return f"[调度器] 范围下载任务恢复：{feedback_msg} (将从 .part/.meta 断点续传)"

    def validate_and_prepare_resume(self, task: dict[str, Any]) -> tuple[bool, dict[str, Any], str]:
        params = task.get("params") or {}
        patch = {}
        contiguous_cursor = params.get("contiguous_cursor_id")
        if contiguous_cursor is not None:
            try:
                next_start = int(contiguous_cursor) + 1
                end_id = int(params.get("end_id", 0))
                if end_id > 0 and next_start > end_id:
                    return True, {}, "范围下载任务所有消息均已下载完成"
                if next_start > int(params.get("start_id", 1)):
                    patch["start_id"] = next_start
            except (ValueError, TypeError) as val_err:
                logger.debug("计算连续游标断点异常 (非致命): %s", val_err)
        return False, patch, ""

    async def execute(self, task_id: str, params: dict[str, Any], account_id: int | None) -> None:
        await download_runner.run_download_range_task(task_id, account_id=account_id, **self.extract_run_params(params))
