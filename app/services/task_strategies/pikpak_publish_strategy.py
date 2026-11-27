import logging
from typing import Any

from app.core.task_strategy import TaskTypeStrategy
from app.services import publish_runner

logger = logging.getLogger(__name__)


class PikPakPublishStrategy(TaskTypeStrategy):
    # D4 表驱动参数提取: target_channel 补 target_chat 别名回退 (原 execute 路径缺失、
    # dispatch 路径才有 —— 统一后取两路径并集行为); prefetch 主键 pikpak_prefetch_concurrency。
    RUN_PARAMS_SPEC = (
        ("target_channel", ("target_channel", "target_chat"), None, None, True),
        ("remote_files", ("remote_files",), None, None, True),
        ("auto_cleanup_mode", ("auto_cleanup_mode",), None, "archive", False),
        ("send_delay_seconds", ("send_delay_seconds",), float, 3.0, False),
        ("upload_concurrency", ("upload_concurrency",), int, None, False),
        ("upload_in_flight_per_connection", ("upload_in_flight_per_connection",), int, None, False),
        ("is_folder_publish", ("is_folder_publish",), bool, None, False),
        ("folder_name", ("folder_name",), None, None, False),
        ("dir_tree", ("dir_tree",), None, None, False),
        ("send_placeholder_card", ("send_placeholder_card",), bool, None, False),
        ("split_strategy", ("split_strategy",), None, None, False),
        ("oversize_policy", ("oversize_policy",), None, None, False),
        ("force_republish", ("force_republish",), bool, None, False),
        ("album_mode", ("album_mode",), bool, None, False),
        (
            "prefetch_concurrency",
            ("pikpak_prefetch_concurrency", "prefetch_concurrency"),
            lambda v: int(v) if v else None,
            None,
            False,
        ),
    )

    @property
    def task_type(self) -> str:
        return "pikpak_publish"

    @property
    def max_concurrency_limit(self) -> int:
        return 16

    def extract_target_chat(self, params: dict[str, Any]) -> str | None:
        return params.get("target_channel") or params.get("target_chat")

    def validate_required_params(self, params: dict[str, Any]) -> None:
        remote_files = params.get("remote_files") or []
        # 与 RUN_PARAMS_SPEC 同口径: target_chat 是一等别名, 缺失会让带别名的暂停任务被误拒
        target_channel = params.get("target_channel") or params.get("target_chat")
        if not remote_files or not target_channel:
            raise ValueError("任务缺少恢复所需参数(remote_files/target_channel), 无法恢复")

    def apply_concurrency_update(
        self, params: dict[str, Any], concurrency: int, upload_in_flight_per_connection: int | None = None
    ) -> None:
        params["upload_concurrency"] = concurrency
        if upload_in_flight_per_connection is not None:
            params["upload_in_flight_per_connection"] = upload_in_flight_per_connection

    def get_resume_hint(self) -> str:
        return "已发布文件已记录历史，恢复后将自动跳过继续发布"

    def get_resume_log_message(self, feedback_msg: str, params: dict[str, Any]) -> str:
        return f"[调度器] PikPak 发布任务恢复：{feedback_msg} (自动跳过已发布文件继续执行)"

    def validate_and_prepare_resume(self, task: dict[str, Any]) -> tuple[bool, dict[str, Any], str]:
        params = task.get("params") or {}
        remote_files = params.get("remote_files") or []
        target_channel = params.get("target_channel") or params.get("target_chat")
        task_id = str(task.get("id") or "")
        force_republish = bool(params.get("force_republish"))
        if remote_files and target_channel:
            try:
                from app.core.entity_resolver import parse_chat_identifier
                from app.db.publish_repository import PublishHistoryRepository

                parsed_target = parse_chat_identifier(target_channel)
                all_published = True
                for rf in remote_files:
                    fid = rf.get("file_id") if isinstance(rf, dict) else getattr(rf, "file_id", None)
                    if not fid or (isinstance(fid, str) and not fid.strip()):
                        all_published = False
                        break
                    if force_republish:
                        pub_info = PublishHistoryRepository.get_published_info(
                            parsed_target, "", 0, source_type="pikpak", remote_file_id=str(fid)
                        )
                        # 若开启了 force_republish，仅当已发布记录确系由当前任务自身发布时才算本任务已完成，
                        # 避免历史旧任务的发布记录误导致未跑完的强制重传任务被过早标记为 completed
                        if pub_info is None or (task_id and pub_info.task_id != task_id):
                            all_published = False
                            break
                    elif not PublishHistoryRepository.is_published(
                        parsed_target, "", 0, source_type="pikpak", remote_file_id=str(fid)
                    ):
                        all_published = False
                        break
                if all_published:
                    return True, {}, "PikPak 发布任务中所有远端文件均已发布完成"
            except Exception as exc:
                logger.debug("检查 PikPak 远端文件发布历史异常 (非致命): %s", exc)
        return False, {}, ""

    async def execute(self, task_id: str, params: dict[str, Any], account_id: int | None) -> None:
        await publish_runner.run_publish_pikpak_task(task_id, account_id=account_id, **self.extract_run_params(params))
