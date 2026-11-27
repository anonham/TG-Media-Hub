"""预设服务: app/api 层经本服务读写预设, 不再直连 app/db 仓库。

依赖方向契约 app/api → app/services → app/core → app/db 的收口 —— 薄委托、零状态、
行为与直连仓库完全一致 (既有测试对仓库类属性的 patch 照常穿透)。"""

from typing import Any

from app.db.preset_repository import PresetRepository


class PresetService:
    """presets 路由的预设读写门面"""

    @classmethod
    def get_all(cls, preset_type: str | None = None) -> list[dict[str, Any]]:
        return PresetRepository.get_all(preset_type)

    @classmethod
    def create(cls, name: str, channel_id: str, preset_type: str = "target", title: str = "") -> dict[str, Any]:
        return PresetRepository.create(name=name, channel_id=channel_id, preset_type=preset_type, title=title)

    @classmethod
    def delete(cls, preset_id: int) -> None:
        PresetRepository.delete(preset_id)
