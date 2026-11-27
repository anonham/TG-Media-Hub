"""PikPak 只读探测与发布任务环境降级辅助 ( / F10)。

只读语义: 仅 GET /api/pikpak/* 与任务状态/日志读取; 不上传/移动/删除网盘
文件 (远端样本由使用者经 PikPak 客户端预置, auto_cleanup_mode=none 保持原状)。
发布任务因频道发帖权限失败 (服务器侧 ChatWriteForbidden 族) 时转 skip (F10
环境前置降级), 其余失败一律为真实断言失败。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from . import api_client


def auth_state(live_api: api_client.LiveApiClient) -> str:
    """PikPak 登录态 (logged_in / 其他)。"""
    resp = live_api.get("/api/pikpak/auth/status")
    resp.raise_for_status()
    return str(resp.json().get("state", ""))


def list_remote_page(live_api: api_client.LiveApiClient, folder_id: str) -> list[dict[str, Any]]:
    """单目录全量列举 (翻页聚合); 返回文件/目录快照列表。"""
    items: list[dict[str, Any]] = []
    token: str | None = None
    while True:
        params: dict[str, Any] = {"folder_id": folder_id}
        if token:
            params["page_token"] = token
        resp = live_api.get("/api/pikpak/files/list", params=params)
        resp.raise_for_status()
        data = resp.json()
        items.extend(data.get("items", []))
        token = data.get("next_page_token")
        if not token:
            return items


def resolve_remote_folder(live_api: api_client.LiveApiClient, path: str) -> str | None:
    """按路径逐级解析 PikPak 目录 id (服务端无 search, 只能逐级浏览)。"""
    folder_id = ""
    for seg in [s for s in path.replace("\\", "/").split("/") if s]:
        match = next(
            (i for i in list_remote_page(live_api, folder_id) if i.get("is_dir") and i.get("name") == seg),
            None,
        )
        if match is None:
            return None
        folder_id = match["file_id"]
    return folder_id


def dir_size(root: Path) -> int:
    """目录字节总量 (不存在视为 0)。"""
    if not root.exists():
        return 0
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file())


def downloads_snapshot(root: Path) -> dict[Path, int]:
    """目录逐文件字节快照 (relative_path -> size_bytes), 供无抵消增量计算使用。"""
    if not root.exists():
        return {}
    snapshot: dict[Path, int] = {}
    for entry in root.rglob("*"):
        try:
            if entry.is_file():
                snapshot[entry.relative_to(root)] = entry.stat().st_size
        except OSError:
            continue
    return snapshot


def downloads_added_bytes(root: Path, baseline: dict[Path, int]) -> int:
    """相对基线快照的新增/增长字节总量 (基线文件被删或缩小不会抵消新落盘字节)。"""
    current = downloads_snapshot(root)
    return sum(max(0, size - baseline.get(rel, 0)) for rel, size in current.items())


def logs_text(live_api: api_client.LiveApiClient, task_id: str, limit: int = 2000) -> str:
    """任务日志全量文本 (limit 经服务端钳制 [1, 2000]); JSON 串接保证中文/emoji
    关键词可检索 (orjson 输出原生 UTF-8, 但串接形态对响应模型形状不敏感)。"""
    resp = live_api.get(f"/api/tasks/{task_id}/logs", params={"after_seq": 0, "limit": limit})
    if resp.status_code != 200:
        return ""
    return json.dumps(resp.json(), ensure_ascii=False)


def mentions_write_forbidden(text: str) -> bool:
    lowered = text.lower()
    return any(
        key in lowered
        for key in (
            "chatwriteforbidden",
            "chat_admin_required",
            "forbidden",
            "no permission to send",
            "无发帖",
            "发帖权限",
            "写入权限",
        )
    )


def completed_or_env_skip(live_api: api_client.LiveApiClient, final: dict[str, Any]) -> None:
    """终态断言 + 环境前置降级: 任务因频道发帖权限失败时转 skip;
    其余失败一律为真实断言失败。"""
    if final.get("status") == "failed":
        detail = str(final.get("error_detail") or "")
        if mentions_write_forbidden(detail):
            pytest.skip(f"测试频道无发帖权限 (服务器侧): {detail}")
        logs = live_api.get(f"/api/tasks/{final['id']}/logs", params={"after_seq": 0, "limit": 200})
        if logs.status_code == 200 and mentions_write_forbidden(logs.text):
            pytest.skip(f"测试频道无发帖权限 (服务器侧日志): {detail}")
    assert final.get("status") == "completed", final.get("error_detail")
