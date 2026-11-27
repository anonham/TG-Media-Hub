"""账号/机器人档案服务: app/api 层经本服务读取档案, 不再直连 app/db 仓库。

依赖方向契约 app/api → app/services → app/core → app/db 的收口 —— 薄委托、零状态、
行为与直连仓库完全一致 (既有测试对仓库类属性的 patch 照常穿透)。"""

from typing import Any

from app.db.account_repository import AccountRepository, BotRepository


class AccountAdminService:
    """accounts 路由的账号与 Bot 档案读取门面 (纯读)"""

    @classmethod
    def list_account_records(cls, only_active: bool = False) -> list[dict[str, Any]]:
        return AccountRepository.get_all(only_active)

    @classmethod
    def get_account_record(cls, account_id: int) -> dict[str, Any] | None:
        return AccountRepository.get_by_id(account_id)

    @classmethod
    def list_bot_records(cls, only_active: bool = False) -> list[dict[str, Any]]:
        return BotRepository.get_all(only_active)

    @classmethod
    def get_bot_record(cls, bot_id: int) -> dict[str, Any] | None:
        return BotRepository.get_by_id(bot_id)
