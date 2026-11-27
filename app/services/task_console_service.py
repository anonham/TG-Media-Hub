"""任务控制台读写门面: app/api 层经本服务查询任务/日志与创建入队任务, 不再直连 app/db 仓库。

普通任务委托仓储创建，下载提交另外规范化目标并检查活跃任务与下载历史。"""

import asyncio
import hashlib
import json
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Literal

from app.core.entity_resolver import iter_message_link_targets, normalize_download_chat_key
from app.db.download_repository import DownloadHistoryRepository
from app.db.task_repository import DuplicateDownloadTaskError, TaskQueueRepository
from app.models.task import AlreadyDownloadedDetail, DownloadTaskConflictDetail

logger = logging.getLogger(__name__)


def _download_message_key(link: str) -> tuple[str, str, int]:
    target = next(iter_message_link_targets(link), None)
    if target is None:
        return "link", link, 0
    chat, msg_id = target
    return "message", normalize_download_chat_key(chat), msg_id


def _find_downloaded_targets(
    targets: Iterable[tuple[str, int]], required_range: tuple[str, int, int] | None
) -> AlreadyDownloadedDetail | None:
    if required_range is not None:
        chat, start_id, end_id = required_range
        if DownloadHistoryRepository.count_in_range(chat, start_id, end_id) < end_id - start_id + 1:
            return None

    file_names: list[str] = []
    task_ids: set[str | None] = set()
    for chat, msg_id in targets:
        try:
            record = DownloadHistoryRepository.get_valid_download_record(chat, msg_id, require_completed_album=True)
        except OSError as exc:
            logger.debug("下载提交无法确认历史文件 %s/%s: %s", chat, msg_id, exc)
            return None
        # 链接执行器会展开整个相册，单条历史无法证明所有成员均已下载。
        if record is None or (required_range is None and record["grouped_id"]):
            return None
        file_names.append(record["file_name"] or Path(record["file_path"]).name)
        task_ids.add(record["task_id"])
    if not file_names:
        return None

    task_id = next(iter(task_ids)) if len(task_ids) == 1 else None
    return AlreadyDownloadedDetail(
        code="already_downloaded",
        message="目标文件均已下载到本地",
        task_id=task_id if task_id and TaskQueueRepository.get_by_id(task_id) else None,
        file_count=len(file_names),
        file_names=file_names,
    )


class TaskConsoleService:
    """tasks / logs / 任务提交路由的队列读写门面"""

    @staticmethod
    async def create_download_task(
        task_id: str,
        task_type: Literal["download_link", "download_range"],
        params: dict[str, Any],
        account_id: int | None,
        priority: int,
        concurrency_total: int,
        force: bool = False,
    ) -> DownloadTaskConflictDetail | AlreadyDownloadedDetail | None:
        targets: Iterable[tuple[str, int]] = ()
        required_range: tuple[str, int, int] | None = None
        if task_type == "download_link":
            link_keys = sorted({_download_message_key(link) for link in params["links"]})
            target = json.dumps(link_keys)
            if all(kind == "message" and chat for kind, chat, _ in link_keys):
                targets = ((chat, msg_id) for _, chat, msg_id in link_keys)
        else:
            chat = normalize_download_chat_key(params["channel"])
            start_id, end_id = params["start_id"], params["end_id"]
            required_range = (chat, start_id, end_id)
            target = json.dumps(required_range)
            targets = ((chat, msg_id) for msg_id in range(start_id, end_id + 1))
        download_target_key = hashlib.sha256(target.encode()).hexdigest()

        try:
            if not force:
                if active := await asyncio.to_thread(
                    TaskQueueRepository.find_active_download, task_type, download_target_key
                ):
                    raise active
                if downloaded := await asyncio.to_thread(_find_downloaded_targets, targets, required_range):
                    # 磁盘校验期间可能已提交同一目标，返回当下的活跃任务冲突。
                    if active := await asyncio.to_thread(
                        TaskQueueRepository.find_active_download, task_type, download_target_key
                    ):
                        raise active
                    return downloaded
            await asyncio.to_thread(
                TaskQueueRepository.create,
                task_id,
                task_type,
                params,
                account_id=account_id,
                priority=priority,
                concurrency_total=concurrency_total,
                download_target_key=download_target_key,
            )
        except DuplicateDownloadTaskError as conflict:
            return DownloadTaskConflictDetail(
                code="duplicate_download_task",
                task_id=conflict.task_id,
                status=conflict.status,
                message="已有暂停的下载任务" if conflict.status == "paused" else "已有相同的下载任务",
            )
        return None

    @classmethod
    def create_task(
        cls,
        task_id: str,
        task_type: str,
        params: dict[str, Any],
        account_id: int | None = None,
        priority: int = 10,
        max_retries: int = 3,
        status: str = "pending",
        concurrency_total: int | None = None,
        total_bytes: int = 0,
        total_items: int = 0,
        current_item: str = "",
    ) -> dict[str, Any]:
        return TaskQueueRepository.create(
            task_id=task_id,
            task_type=task_type,
            params=params,
            account_id=account_id,
            priority=priority,
            max_retries=max_retries,
            status=status,
            concurrency_total=concurrency_total,
            total_bytes=total_bytes,
            total_items=total_items,
            current_item=current_item,
        )

    @classmethod
    async def update_progress_async(
        cls,
        task_id: str,
        progress_bytes: int = 0,
        total_bytes: int = 0,
        processed_items: int = 0,
        total_items: int = 0,
        speed_mb_s: float = 0.0,
        eta_s: float = 0.0,
        current_item: str = "",
        concurrency: int = 0,
        dc_id: int = 0,
        concurrency_total: int | None = None,
    ) -> None:
        await TaskQueueRepository.update_progress_async(
            task_id,
            progress_bytes,
            total_bytes,
            processed_items,
            total_items,
            speed_mb_s,
            eta_s,
            current_item,
            concurrency,
            dc_id,
            concurrency_total,
        )

    @classmethod
    def get_active_by_type(cls, task_type: str) -> list[dict[str, Any]]:
        return TaskQueueRepository.get_active_by_type(task_type)

    @classmethod
    async def get_by_id_async(cls, task_id: str) -> dict[str, Any] | None:
        return await TaskQueueRepository.get_by_id_async(task_id)

    @classmethod
    async def query_tasks_async(
        cls,
        status: str | None = None,
        task_type: str | None = None,
        account_id: int | None = None,
        query: str | None = None,
        page: int = 1,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], int]:
        return await TaskQueueRepository.query_tasks_async(
            status=status,
            task_type=task_type,
            account_id=account_id,
            query=query,
            page=page,
            limit=limit,
        )

    @classmethod
    async def count_by_status_async(cls) -> dict[str, int]:
        return await TaskQueueRepository.count_by_status_async()

    @classmethod
    async def get_all_async(cls, limit: int = 50) -> list[dict[str, Any]]:
        return await TaskQueueRepository.get_all_async(limit)

    @classmethod
    async def get_task_logs_async(
        cls,
        task_id: str,
        after_seq: int = 0,
        limit: int = 200,
        level: str | None = None,
        query: str | None = None,
    ) -> tuple[list[dict[str, Any]], int, bool] | None:
        return await TaskQueueRepository.get_task_logs_async(
            task_id,
            after_seq=after_seq,
            limit=limit,
            level=level,
            query=query,
        )
