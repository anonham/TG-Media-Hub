"""系统动态配置服务: app/api 层经本服务读写配置, 不再直连 app/db 仓库。

依赖方向契约 app/api → app/services → app/core → app/db 的收口 —— 薄委托、零状态、
行为与直连仓库完全一致 (既有测试对仓库类属性的 patch 照常穿透)。"""

from typing import Any

from app.db.account_repository import SystemSettingsRepository


class SystemSettingsService:
    """系统动态配置读写门面 (settings / storage / download / publish / pikpak 路由共用)"""

    @classmethod
    def get_str(cls, key: str, default: str = "") -> str:
        return SystemSettingsRepository.get_str(key, default)

    @classmethod
    def get_int(cls, key: str, default: int = 0) -> int:
        return SystemSettingsRepository.get_int(key, default)

    @classmethod
    def get_bool(cls, key: str, default: bool = False) -> bool:
        return SystemSettingsRepository.get_bool(key, default)

    @classmethod
    def set(cls, key: str, value: Any, category: str = "general", description: str = "") -> None:
        SystemSettingsRepository.set(key, value, category=category, description=description)

    @classmethod
    def get_all_grouped(cls) -> dict[str, Any]:
        return SystemSettingsRepository.get_all_grouped()

    @classmethod
    def set_batch(cls, settings_dict: dict[str, Any]) -> None:
        SystemSettingsRepository.set_batch(settings_dict)
