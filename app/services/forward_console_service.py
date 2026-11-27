"""转发控制台查询门面: app/api 层经本服务查询转发历史与防重账本, 不再直连 app/db 仓库。

依赖方向契约 app/api → app/services → app/core → app/db 的收口 —— 薄委托、零状态、
行为与直连仓库完全一致 (既有测试对仓库类属性的 patch 照常穿透)。"""

from typing import Any

from app.db.forward_repository import ForwardHistoryRepository, ForwardLedgerRepository
from app.services.forward_service import ForwardService


def _canon(chat_id: str | None) -> str | None:
    if not chat_id:
        return chat_id
    return ForwardService.canonicalize_chat_key(chat_id)


class ForwardConsoleService:
    """forward / audit 路由的转发历史与防重账本查询门面 (纯读)"""

    @classmethod
    def query_history(
        cls,
        source_chat: str | None = None,
        target_chat: str | None = None,
        source_msg_id: int | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        return ForwardHistoryRepository.query_history(
            source_chat=_canon(source_chat),
            target_chat=_canon(target_chat),
            source_msg_id=source_msg_id,
            limit=limit,
            offset=offset,
        )

    @classmethod
    def count_processed_in_range(cls, source_chat: str, target_chat: str, min_id: int, max_id: int) -> int:
        return ForwardLedgerRepository.count_processed_in_range(
            ForwardService.canonicalize_chat_key(source_chat),
            ForwardService.canonicalize_chat_key(target_chat),
            min_id,
            max_id,
        )

    @classmethod
    def get_task_receipt(cls, task_id: str) -> dict[str, Any]:
        return ForwardLedgerRepository.get_task_receipt(task_id)

    @classmethod
    def get_coverage_segments(cls, source_chat: str, target_chat: str) -> list[tuple[int, int]]:
        return ForwardLedgerRepository.get_coverage_segments(
            ForwardService.canonicalize_chat_key(source_chat),
            ForwardService.canonicalize_chat_key(target_chat),
        )

    @classmethod
    def get_ledger_bounds(cls, source_chat: str, target_chat: str) -> tuple[int, int] | None:
        return ForwardLedgerRepository.get_ledger_bounds(
            ForwardService.canonicalize_chat_key(source_chat),
            ForwardService.canonicalize_chat_key(target_chat),
        )

    @classmethod
    def get_ledger_runs(
        cls, source_chat: str, target_chat: str, min_id: int | None = None, max_id: int | None = None
    ) -> dict[str, list[int]]:
        return ForwardLedgerRepository.get_ledger_runs(
            ForwardService.canonicalize_chat_key(source_chat),
            ForwardService.canonicalize_chat_key(target_chat),
            min_id,
            max_id,
        )

    @classmethod
    def query_skips(
        cls,
        source_chat: str | None = None,
        target_chat: str | None = None,
        skip_type: str | None = None,
        task_id: str | None = None,
        page: int = 1,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], int]:
        return ForwardLedgerRepository.query_skips(
            source_chat=_canon(source_chat),
            target_chat=_canon(target_chat),
            skip_type=skip_type,
            task_id=task_id,
            page=page,
            limit=limit,
        )
