"""Pipeline Core: PikPak 搬运任务恢复预检、断点续传与上传元数据 GC 回收测试 (PikPak Resume & Meta GC)

覆盖范围:
- 原 test_issue2_pikpak_resume: PikPak 搬运任务恢复预检与断点续传成功分支
- 原 test_pikpak_meta_gc: 任务删除孤儿 upload_meta GC 回收、前缀隔离与目录容错"""

import logging
import os
from unittest.mock import AsyncMock, patch

import pytest
from _safeio import safe_write_bytes
from fastapi.testclient import TestClient

from app.api.tasks import _prepare_task_resume, resume_task
from app.config import settings
from app.core.pikpak.meta import META_SUFFIX, meta_stem  # 唯一原语直取 core
from app.core.scheduler import task_scheduler
from app.db.task_repository import TaskQueueRepository
from app.main import app
from app.services.upload_meta_gc import gc_task_meta


# ==============================================================================
# Part: test_issue2_pikpak_resume.py
# ==============================================================================
@pytest.mark.asyncio
async def test_prepare_task_resume_pikpak():
    # 1. Incomplete task
    task_incomplete = {
        "id": "t_pikpak_1",
        "task_type": "pikpak_publish",
        "params": {"target_channel": "-1009000000010", "remote_files": [{"file_id": "fid_1", "name": "1.mp4"}]},
    }
    with patch("app.db.publish_repository.PublishHistoryRepository.is_published", return_value=False):
        res = await _prepare_task_resume(task_incomplete)
        assert res.get("completed") is False

    # 2. All files published
    with patch("app.db.publish_repository.PublishHistoryRepository.is_published", return_value=True):
        res = await _prepare_task_resume(task_incomplete)
        assert res.get("completed") is True
        assert "发布完成" in res.get("message", "")


@pytest.mark.asyncio
async def test_resume_task_pikpak_publish_success():
    task_id = "task_pikpak_paused"
    TaskQueueRepository.create(
        task_id,
        "pikpak_publish",
        {
            "target_channel": "-1009000000010",
            "remote_files": [{"file_id": "fid_1", "name": "1.mp4"}],
            "upload_concurrency": 4,
            "upload_in_flight_per_connection": 2,
        },
        account_id=1,
        status="paused",
    )

    with (
        patch("app.core.connection_manager.connection_manager.broadcast", new_callable=AsyncMock),
        patch("app.core.scheduler.task_scheduler.notify_new_task") as mock_notify,
        patch.object(type(task_scheduler), "is_running", True),
        patch("app.core.scheduler.task_scheduler.get_max_global_concurrency", return_value=5),
    ):
        resp = await resume_task(task_id, mode="normal")

        assert resp["status"] == "queued"
        assert "恢复运行" in resp["message"]
        assert TaskQueueRepository.get_by_id(task_id)["status"] == "queued"
        mock_notify.assert_called_once()


# ==============================================================================
# Part: test_pikpak_meta_gc.py
# ==============================================================================
client = TestClient(app, client=("127.0.0.1", 50000))


def _make_key(monkeypatch) -> str:
    """测试期 API Key: 运行时随机生成 (非源码凭据字面量)。"""
    import uuid

    key = "pk-" + uuid.uuid4().hex
    monkeypatch.setattr(settings, "API_KEY", key)
    return key


def _auth_headers(monkeypatch) -> dict:
    key = _make_key(monkeypatch)
    from app.api.auth import issue_2fa_token

    headers = {"X-API-Key": key}
    token = issue_2fa_token()
    if token:
        headers["X-2FA-Token"] = token
    return headers


@pytest.fixture
def meta_sandbox(tmp_dir, monkeypatch):
    sandbox = os.path.join(tmp_dir, "upload_meta")
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", sandbox)
    return sandbox


def _write_meta(sandbox, task_id, item_id, seg):
    """用包B 命名原语构造真实 meta 文件 (守护「同一助手」约定)。"""
    return safe_write_bytes(
        os.path.join(sandbox, meta_stem(task_id, item_id, seg) + META_SUFFIX),
        b"{}",
    )


def test_gc_removes_only_target_task_meta(meta_sandbox):
    """GC 只删本 task 的 meta, 其他任务 meta 保留。"""
    _write_meta(meta_sandbox, "taskA", "F1", 1)
    _write_meta(meta_sandbox, "taskA", "F2", 2)
    keep = _write_meta(meta_sandbox, "taskB", "F1", 1)

    removed = gc_task_meta("taskA")
    assert removed == 2
    assert not os.path.exists(os.path.join(meta_sandbox, meta_stem("taskA", "F1", 1) + META_SUFFIX))
    assert not os.path.exists(os.path.join(meta_sandbox, meta_stem("taskA", "F2", 2) + META_SUFFIX))
    assert os.path.exists(keep)  # 其他任务 meta 保留


def test_gc_prefix_isolation(meta_sandbox):
    """前缀碰撞防线: task 'abc' 的 GC 不误删 'abc1' 任务的 meta。"""
    _write_meta(meta_sandbox, "abc", "F1", 1)
    keep = _write_meta(meta_sandbox, "abc1", "F1", 1)

    removed = gc_task_meta("abc")
    assert removed == 1
    assert not os.path.exists(os.path.join(meta_sandbox, meta_stem("abc", "F1", 1) + META_SUFFIX))
    assert os.path.exists(keep)


def test_gc_missing_dir_tolerant(tmp_dir, monkeypatch):
    """沙箱根不存在 → 返回 0, 无异常。"""
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", os.path.join(tmp_dir, "nonexistent_meta"))
    assert gc_task_meta("taskA") == 0


def test_gc_unlink_failure_tolerant(meta_sandbox, monkeypatch, caplog):
    """单文件删除失败 → 记 warning 跳过, 其余文件仍被删, 不抛出。"""
    from pathlib import Path

    p1 = _write_meta(meta_sandbox, "taskA", "F1", 1)
    p2 = _write_meta(meta_sandbox, "taskA", "F2", 2)

    original_unlink = Path.unlink

    def flaky_unlink(self, missing_ok=False):
        if str(self) == p1:
            raise OSError("Permission denied (canned)")
        return original_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", flaky_unlink)
    with caplog.at_level(logging.WARNING, logger="app.services.upload_meta_gc"):
        removed = gc_task_meta("taskA")
    assert removed == 1  # p2 成功, p1 失败跳过
    assert os.path.exists(p1)
    assert not os.path.exists(p2)
    assert any("upload_meta GC 单文件删除失败" in r.message for r in caplog.records)


def test_delete_task_api_triggers_gc(meta_sandbox, monkeypatch):
    """API 单删 + 批量 delete (复用 delete_task) 均触发 GC; 非删除操作不触发。"""
    headers = _auth_headers(monkeypatch)

    # 单删: 终态任务 + 其 meta → 删除后 meta 消失
    TaskQueueRepository.create(task_id="gctask1", task_type="pikpak_publish", params={}, status="completed")
    m1 = _write_meta(meta_sandbox, "gctask1", "F1", 1)
    resp = client.delete("/api/tasks/gctask1", headers=headers)
    assert resp.status_code == 200, resp.text
    assert not os.path.exists(m1)

    # 批量 delete 复用 delete_task → GC 一并覆盖
    TaskQueueRepository.create(task_id="gctask2", task_type="pikpak_publish", params={}, status="failed")
    m2 = _write_meta(meta_sandbox, "gctask2", "F2", 1)
    resp = client.post(
        "/api/tasks/batch",
        json={
            "action": "delete",
            "task_ids": ["gctask2"],
        },
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    assert not os.path.exists(m2)

    # 其他任务的 meta 不受影响
    keep = _write_meta(meta_sandbox, "gcother", "F9", 1)
    TaskQueueRepository.create(task_id="gctask3", task_type="pikpak_publish", params={}, status="completed")
    resp = client.delete("/api/tasks/gctask3", headers=headers)
    assert resp.status_code == 200
    assert os.path.exists(keep)
