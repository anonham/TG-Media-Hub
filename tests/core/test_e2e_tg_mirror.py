"""E2E mirror 编排脚本单测 (fake client, 对应 scripts/e2e_tg_mirror.py)。"""

import hashlib
import importlib.util
import json
import os
import types

import pytest

from app.config import settings
from tests._safeio import safe_write_bytes


def _load_mirror_module():
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    path = os.path.join(repo_root, "scripts", "e2e_tg_mirror.py")
    spec = importlib.util.spec_from_file_location("e2e_tg_mirror", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _FakeMessage:
    def __init__(self, payload: bytes, doc_size: int):
        self.media = object()
        self.document = types.SimpleNamespace(size=doc_size)
        self._payload = payload

    async def download_media(self, file=None):
        safe_write_bytes(file, self._payload)


class _FakeClient:
    def __init__(self, messages: dict):
        self._messages = messages

    async def get_messages(self, chat_id, ids):
        return self._messages.get(ids)


def _write_manifest(root: str, manifest: dict) -> str:
    path = os.path.join(root, "manifest.json")
    safe_write_bytes(path, json.dumps(manifest, ensure_ascii=False).encode())
    return path


@pytest.mark.asyncio
async def test_mirror_ok_missing_and_skipped_parts():
    mod = _load_mirror_module()
    payload = os.urandom(150_000)
    manifest = {
        "task_id": "t",
        "item_id": "i",
        "target_chat_id": -1009000000010,
        "rolled_back": False,
        "parts": [
            {"part_name": "a.part001.mp4", "msg_id": 11, "status": "sent"},
            {"part_name": "a.part002.mp4", "msg_id": 22, "status": "failed"},
            {"part_name": "a.part003.mp4", "msg_id": 33, "status": "sent"},
        ],
        "tg_mirror": [],
    }
    root = os.path.join(settings.E2E_CAPTURE_DIR, "task_m", "item_m")
    manifest_path = _write_manifest(root, manifest)
    client = _FakeClient({11: _FakeMessage(payload, len(payload))})  # 22 无需, 33 缺消息

    await mod.mirror_manifest(manifest_path, client)

    with open(manifest_path, encoding="utf-8") as f:
        reloaded = json.load(f)
    entries = {m["part_name"]: m for m in reloaded["tg_mirror"]}
    ok_entry = entries["a.part001.mp4"]
    assert ok_entry["status"] == "ok"
    assert ok_entry["sha256"] == hashlib.sha256(payload).hexdigest()
    assert ok_entry["document_size"] == len(payload)
    assert ok_entry["path"] == "tg_mirror/a.part001.mp4"
    with open(os.path.join(root, "tg_mirror", "a.part001.mp4"), "rb") as f:
        assert f.read() == payload
    assert entries["a.part002.mp4"]["status"] == "pending"  # 非 sent 不回下载
    missing = entries["a.part003.mp4"]
    assert missing["status"] == "failed"
    assert "suspected rollback" in missing["error"]  # E3: 单独归因, 与普通失败区分


@pytest.mark.asyncio
async def test_mirror_rolled_back_marks_skipped():
    mod = _load_mirror_module()
    manifest = {
        "task_id": "t",
        "item_id": "i",
        "target_chat_id": -1009000000010,
        "rolled_back": True,
        "parts": [{"part_name": "a.part001.mp4", "msg_id": 11, "status": "sent"}],
        "tg_mirror": [],
    }
    root = os.path.join(settings.E2E_CAPTURE_DIR, "task_rb", "item_rb")
    manifest_path = _write_manifest(root, manifest)
    client = _FakeClient({11: _FakeMessage(b"never downloaded", 15)})

    await mod.mirror_manifest(manifest_path, client)

    with open(manifest_path, encoding="utf-8") as f:
        reloaded = json.load(f)
    entry = reloaded["tg_mirror"][0]
    assert entry["status"] == "skipped"
    assert "rolled_back" in entry["error"]
    assert not os.path.exists(os.path.join(root, "tg_mirror", "a.part001.mp4"))


@pytest.mark.asyncio
async def test_mirror_uses_download_media_returned_path_and_handles_none(tmp_path):
    """验证 mirror_manifest 尊重 download_media 返回路径，并在返回 None 且无文件时记为 failed"""
    mod = _load_mirror_module()
    payload = b"telethon-renamed-video"

    class _RenamedMessage:
        def __init__(self):
            self.media = object()
            self.document = types.SimpleNamespace(size=len(payload))

        async def download_media(self, file=None):
            renamed = f"{file}.mp4"
            safe_write_bytes(renamed, payload)
            return renamed

    class _NoneMessage:
        media = object()
        document = types.SimpleNamespace(size=10)

        async def download_media(self, file=None):
            return None

    manifest = {
        "task_id": "t_dl",
        "item_id": "i_dl",
        "target_chat_id": -1009000000010,
        "rolled_back": False,
        "parts": [
            {"part_name": "noext", "msg_id": 11, "status": "sent"},
            {"part_name": "missing.mp4", "msg_id": 22, "status": "sent"},
        ],
        "tg_mirror": [],
    }
    manifest_path = _write_manifest(str(tmp_path), manifest)
    client = _FakeClient({11: _RenamedMessage(), 22: _NoneMessage()})

    await mod.mirror_manifest(manifest_path, client)
    entries = {m["part_name"]: m for m in json.loads((tmp_path / "manifest.json").read_text())["tg_mirror"]}
    assert entries["noext"]["status"] == "ok"
    assert entries["noext"]["path"] == "tg_mirror/noext.mp4"
    assert entries["noext"]["sha256"] == hashlib.sha256(payload).hexdigest()
    assert entries["missing.mp4"]["status"] == "failed"
    assert "None" in entries["missing.mp4"]["error"]
