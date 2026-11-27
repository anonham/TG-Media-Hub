"""频道预设仓储 (Channel Preset Repository): 发布目标/来源频道的快捷预设持久化"""

import time
from typing import Any

from app.db.database import db_cursor


class PresetRepository:
    @staticmethod
    def get_all(preset_type: str | None = None) -> list[dict[str, Any]]:
        with db_cursor() as cursor:
            if preset_type:
                cursor.execute("SELECT * FROM channel_presets WHERE preset_type = ? ORDER BY id ASC", (preset_type,))
            else:
                cursor.execute("SELECT * FROM channel_presets ORDER BY id ASC")
            rows = [dict(r) for r in cursor.fetchall()]
        for r in rows:
            if "preset_type" not in r or not r["preset_type"]:
                r["preset_type"] = "target"
            if "title" not in r or r["title"] is None:
                r["title"] = ""
        return rows

    @staticmethod
    def create(name: str, channel_id: str, preset_type: str = "target", title: str = "") -> dict[str, Any]:
        with db_cursor(commit=True) as cursor:
            created_at = time.time()
            cursor.execute(
                "INSERT OR REPLACE INTO channel_presets (name, channel_id, preset_type, title, created_at) VALUES (?, ?, ?, ?, ?)",
                (name, channel_id, preset_type, title, created_at),
            )
            preset_id = cursor.lastrowid
        return {
            "id": preset_id,
            "name": name,
            "channel_id": channel_id,
            "preset_type": preset_type,
            "title": title,
            "created_at": created_at,
        }

    @staticmethod
    def delete(preset_id: int):
        with db_cursor(commit=True) as cursor:
            cursor.execute("DELETE FROM channel_presets WHERE id = ?", (preset_id,))
