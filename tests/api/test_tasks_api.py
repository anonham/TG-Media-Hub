"""API Layer: 任务管理 REST 端点、状态机转换、任务日志、重试与全局队列控制测试 (Tasks API & State Machine)

覆盖范围:
- 任务 CRUD、列表过滤、分页与重试接口 (/api/tasks)
- 增强状态机转换与终态防破坏校验 (pause / resume / cancel / delete)
- SEC-001 查询参数注入校验与全局队列暂停/恢复开关
- 任务独立日志表原子落盘、增量轮询与 404 隔离 (/api/tasks/{task_id}/logs)
- 频道路由预检 (/api/tasks/precheck-access) 与并发容量感知
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from fastapi.testclient import TestClient

from app.api.auth import issue_2fa_token, issue_media_ticket
from app.config import settings
from app.core.connection_manager import connection_manager
from app.core.entity_resolver import parse_chat_identifier
from app.core.runtime import active_task_controls
from app.core.scheduler import task_scheduler
from app.db.database import get_db
from app.db.publish_repository import PlaceholderSlotRepository, PublishHistoryRepository
from app.db.task_repository import TaskQueueRepository
from app.main import app

client = TestClient(app, client=("127.0.0.1", 50000))
TEST_LINK = os.getenv("TEST_CHANNEL_LINK", "https://t.me/telegram/1")


def auth_headers():
    headers = {}
    if settings.API_KEY:
        headers["X-API-Key"] = settings.API_KEY
        token = issue_2fa_token()
        if token:
            headers["X-2FA-Token"] = token
    return headers


def test_read_root():
    response = client.get("/")
    assert response.status_code == 200


def test_get_tasks_empty():
    response = client.get("/api/tasks", headers=auth_headers())
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_task_control_routes():
    # 手动创建一个测试任务 (模拟协程存活: 注册控制信号表 + 调度器运行集合)
    task_id = "test_ctrl_1"
    TaskQueueRepository.create(task_id, "download_link", {})
    TaskQueueRepository.update_status(task_id, "running")
    from app.core.runtime import active_task_controls
    from app.core.scheduler import task_scheduler

    active_task_controls[task_id] = {"cancelled": False, "paused": False}
    # 执行器存活性以调度器运行集合为准 (控制条目本身不足以证明协程存活)
    # cancel 后执行器收尾退出 -> 运行集合摘除, 模拟真实生命周期
    live = {"alive": True}

    def _fake_is_running(tid: str) -> bool:
        return tid == task_id and live["alive"]

    try:
        with patch.object(task_scheduler, "is_task_running", side_effect=_fake_is_running):
            # 暂停 (运行中且协程存活)
            resp = client.post(f"/api/tasks/{task_id}/pause", headers=auth_headers())
            assert resp.status_code == 200
            assert resp.json()["status"] == "paused"

            # 恢复 (暂停中且协程存活 -> 清除标志继续)
            resp = client.post(f"/api/tasks/{task_id}/resume", headers=auth_headers())
            assert resp.status_code == 200
            assert resp.json()["status"] == "running"

            # 取消
            resp = client.post(f"/api/tasks/{task_id}/cancel", headers=auth_headers())
            assert resp.status_code == 200
            assert resp.json()["status"] == "cancelled"

            # 执行器收尾退出: 运行集合摘除 + 控制条目由 finally 清理
            live["alive"] = False
            active_task_controls.pop(task_id, None)

            # 删除已取消的任务
            resp = client.delete(f"/api/tasks/{task_id}", headers=auth_headers())
            assert resp.status_code == 200
            assert resp.json()["status"] == "deleted"
    finally:
        active_task_controls.pop(task_id, None)


def test_pause_terminal_task_rejected():
    # 状态机校验: 终态任务 (completed/failed) 暂停应被 409 拒绝 (仅 running/queued/pending 可暂停)
    task_id = "test_pause_409"
    TaskQueueRepository.create(task_id, "download_link", {})
    TaskQueueRepository.update_status(task_id, "completed")
    resp = client.post(f"/api/tasks/{task_id}/pause", headers=auth_headers())
    assert resp.status_code == 409

    TaskQueueRepository.update_status(task_id, "failed")
    resp = client.post(f"/api/tasks/{task_id}/pause", headers=auth_headers())
    assert resp.status_code == 409

    resp = client.post(f"/api/tasks/{task_id}/cancel", headers=auth_headers())
    assert resp.status_code == 409
    assert "无法取消" in resp.json()["detail"]

    TaskQueueRepository.delete(task_id)


def test_delete_running_task_forbidden():
    task_id = "running_task"
    TaskQueueRepository.create(task_id, "download_link", {})
    TaskQueueRepository.update_status(task_id, "running")

    resp = client.delete(f"/api/tasks/{task_id}", headers=auth_headers())
    assert resp.status_code == 400
    assert "不能删除正在运行/暂停中的任务" in resp.json()["detail"]


def test_resume_cancelled_download_task():
    task_id = "resume_dl_1"
    TaskQueueRepository.create(task_id, "download_link", {"links": [TEST_LINK], "links_count": 1})
    TaskQueueRepository.update_status(task_id, "cancelled")

    try:
        resp = client.post(f"/api/tasks/{task_id}/resume", headers=auth_headers())
        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"
    finally:
        TaskQueueRepository.delete(task_id)


def test_resume_cancelled_publish_allowed():
    task_id = "resume_pub_1"
    TaskQueueRepository.create(
        task_id,
        "publish_folder",
        {"folder_path": "D:/tmp", "target_channel": "-10012345", "album_mode": True, "delay_seconds": 3.0},
    )
    TaskQueueRepository.update_status(task_id, "cancelled")

    try:
        resp = client.post(f"/api/tasks/{task_id}/resume", headers=auth_headers())
        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"
    finally:
        TaskQueueRepository.delete(task_id)


def test_sec001_query_param_validation():
    # 1. 传入非法的 status 应当返回 422 校验失败
    resp_invalid_status = client.get("/api/tasks?status=malicious'OR'1'='1", headers=auth_headers())
    assert resp_invalid_status.status_code == 422

    # 2. 传入非法的 task_type 应当返回 422 校验失败
    resp_invalid_type = client.get("/api/tasks?task_type=hack_system", headers=auth_headers())
    assert resp_invalid_type.status_code == 422

    # 3. 合法 status 与 task_type 应当返回 200
    resp_valid = client.get("/api/tasks?status=running&task_type=download_link", headers=auth_headers())
    assert resp_valid.status_code == 200

    # 4. 合法关键词查询
    resp_query = client.get("/api/tasks?query=test_safe_task", headers=auth_headers())
    assert resp_query.status_code == 200


def test_discuss001_global_queue_pause_and_resume():
    t_id = "test_queue_toggle_task"
    TaskQueueRepository.create(t_id, "download_link", {"links": []}, status="queued")
    try:
        # 1. 暂停全局队列
        resp_pause = client.post("/api/tasks/queue/pause", headers=auth_headers())
        assert resp_pause.status_code == 200
        data_pause = resp_pause.json()
        assert data_pause["status"] == "ok"
        assert TaskQueueRepository.get_by_id(t_id)["status"] == "paused"

        # 2. 恢复全局队列
        resp_resume = client.post("/api/tasks/queue/resume", headers=auth_headers())
        assert resp_resume.status_code == 200
        data_resume = resp_resume.json()
        assert data_resume["status"] == "ok"
        assert TaskQueueRepository.get_by_id(t_id)["status"] == "queued"
    finally:
        TaskQueueRepository.delete(t_id)


def test_discuss001_resume_task_capacity_detection_and_priority():
    task_id = "test_resume_capacity_p1"
    TaskQueueRepository.create(task_id, "download_link", {"links": ["https://t.me/c/1/2"]}, account_id=999, priority=10)
    TaskQueueRepository.update_status(task_id, "paused")

    try:
        # 1. 模拟并发打满场景: 模拟当前运行任务数达到全局并发上限
        with (
            patch.object(task_scheduler, "get_max_global_concurrency", return_value=1),
            patch.object(task_scheduler, "_running_tasks", {"mock_running": MagicMock()}),
            patch.object(task_scheduler, "_is_running", True),
            patch.object(task_scheduler, "notify_new_task"),
        ):
            resp = client.post(f"/api/tasks/{task_id}/resume", headers=auth_headers())
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "queued"
            assert data["queued_first"] is True
            assert "优先运行" in data["message"]

            t_db = TaskQueueRepository.get_by_id(task_id)
            assert t_db["priority"] == 50
            assert t_db["status"] == "queued"

        # 2. 再次重置为暂停与默认优先级
        from app.db.database import get_db

        conn = get_db()
        conn.cursor().execute("UPDATE task_queue SET priority = 10, status = 'paused' WHERE id = ?", (task_id,))
        conn.commit()
        conn.close()

        # 3. 模拟并发未打满场景: 有空闲槽位
        with (
            patch.object(task_scheduler, "get_max_global_concurrency", return_value=5),
            patch.object(task_scheduler, "_running_tasks", {}),
            patch.object(task_scheduler, "_is_running", True),
            patch.object(task_scheduler, "notify_new_task"),
        ):
            resp2 = client.post(f"/api/tasks/{task_id}/resume", headers=auth_headers())
            assert resp2.status_code == 200
            data2 = resp2.json()
            assert data2["status"] == "queued"
            assert data2["queued_first"] is False

    finally:
        TaskQueueRepository.delete(task_id)


def test_precheck_access_api():
    """[REQ-005] 验证 /api/tasks/precheck-access 接口校验与异常阻断"""
    # 1. 目标频道标识为空 -> 应当直接返回 accessible=False
    resp1 = client.post(
        "/api/tasks/precheck-access", json={"chat_identifier": "", "account_id": 99999}, headers=auth_headers()
    )
    assert resp1.status_code == 200
    data1 = resp1.json()
    assert data1["accessible"] is False
    assert "不能为空" in data1["reason"]

    # 2. 指定不存在的 Userbot 账号 -> 应当返回 accessible=False
    resp2 = client.post(
        "/api/tasks/precheck-access",
        json={"chat_identifier": "https://t.me/c/9000000010/123", "account_id": 9999999},
        headers=auth_headers(),
    )
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert data2["accessible"] is False
    assert "不存在" in data2["reason"]


client = TestClient(app, client=("127.0.0.1", 50000))


def test_task_filtering_and_pagination():
    # 创建不同状态与优先级的任务
    t1 = "phase2_t1"
    t2 = "phase2_t2"
    t3 = "phase2_t3"

    TaskQueueRepository.create(t1, "download_link", {"links": ["https://t.me/c/1/1"]}, account_id=1, priority=15)
    TaskQueueRepository.update_status(t1, "running")

    TaskQueueRepository.create(t2, "publish_folder", {"folder_path": "/tmp/a"}, account_id=2, priority=10)
    TaskQueueRepository.update_status(t2, "failed")

    TaskQueueRepository.create(
        t3, "forward_channel", {"source_chat": "-1001", "target_chat": "-1002"}, account_id=1, priority=20
    )
    TaskQueueRepository.update_status(t3, "completed")

    # 1. 分页查询
    resp = client.get("/api/tasks?page=1&limit=2", headers=auth_headers())
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] >= 3
    assert len(data["items"]) == 2
    assert data["page"] == 1

    # 2. 按状态过滤
    resp = client.get("/api/tasks?status=failed", headers=auth_headers())
    assert resp.status_code == 200
    items = resp.json()
    assert any(it["id"] == t2 for it in items)
    assert not any(it["id"] == t1 for it in items)

    # 3. 按账号过滤
    resp = client.get("/api/tasks?account_id=2", headers=auth_headers())
    assert resp.status_code == 200
    items = resp.json()
    assert all(it.get("account_id") == 2 for it in items)

    # 4. 关键词搜索
    resp = client.get("/api/tasks?query=phase2_t3", headers=auth_headers())
    assert resp.status_code == 200
    items = resp.json()
    assert len(items) == 1
    assert items[0]["id"] == t3

    # 清理
    TaskQueueRepository.delete(t1)
    TaskQueueRepository.delete(t2)
    TaskQueueRepository.delete(t3)


def test_task_retry_endpoint():
    t_fail = "retry_test_fail"
    TaskQueueRepository.create(t_fail, "download_link", {"links": []})
    TaskQueueRepository.update_status(t_fail, "failed")

    # 重试 failed 任务
    resp = client.post(f"/api/tasks/{t_fail}/retry", headers=auth_headers())
    assert resp.status_code == 200
    assert resp.json()["status"] == "queued"

    task = TaskQueueRepository.get_by_id(t_fail)
    assert task["status"] == "queued"
    assert task["retry_count"] == 0

    # 无法重试 running 任务
    TaskQueueRepository.update_status(t_fail, "running")
    resp = client.post(f"/api/tasks/{t_fail}/retry", headers=auth_headers())
    assert resp.status_code == 400

    TaskQueueRepository.delete(t_fail)


def test_batch_task_actions():
    b1 = "batch_task_1"
    b2 = "batch_task_2"

    TaskQueueRepository.create(b1, "download_link", {})
    TaskQueueRepository.update_status(b1, "running")
    active_task_controls[b1] = {"cancelled": False, "paused": False}

    TaskQueueRepository.create(b2, "download_link", {})
    TaskQueueRepository.update_status(b2, "running")
    active_task_controls[b2] = {"cancelled": False, "paused": False}

    # 执行器存活性以调度器运行集合为准; cancel 后执行器收尾退出 -> 摘除 + 清理条目
    from app.core.scheduler import task_scheduler

    live = {"alive": True}

    def _fake_is_running(tid: str) -> bool:
        return tid in (b1, b2) and live["alive"]

    try:
        with patch.object(task_scheduler, "is_task_running", side_effect=_fake_is_running):
            # 1. 批量暂停
            resp = client.post(
                "/api/tasks/batch", json={"action": "pause", "task_ids": [b1, b2]}, headers=auth_headers()
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["action"] == "pause"
            assert data["success_count"] == 2

            # 2. 批量恢复 (live-paused: 原地清除暂停标志续跑)
            resp = client.post(
                "/api/tasks/batch", json={"action": "resume", "task_ids": [b1, b2]}, headers=auth_headers()
            )
            assert resp.status_code == 200
            assert resp.json()["success_count"] == 2

            # 3. 批量取消
            resp = client.post(
                "/api/tasks/batch", json={"action": "cancel", "task_ids": [b1, b2]}, headers=auth_headers()
            )
            assert resp.status_code == 200
            assert resp.json()["success_count"] == 2

            # 执行器收尾退出
            live["alive"] = False
            active_task_controls.pop(b1, None)
            active_task_controls.pop(b2, None)

            # 4. 批量删除
            resp = client.post(
                "/api/tasks/batch", json={"action": "delete", "task_ids": [b1, b2]}, headers=auth_headers()
            )
            assert resp.status_code == 200
            assert resp.json()["success_count"] == 2

            assert TaskQueueRepository.get_by_id(b1) is None
            assert TaskQueueRepository.get_by_id(b2) is None

    finally:
        active_task_controls.pop(b1, None)
        active_task_controls.pop(b2, None)


def test_create_tasks_with_account_and_priority():
    # 1. 链接下载任务入参带有 account_id 和 priority
    resp = client.post(
        "/api/download/link",
        json={"links": "https://t.me/test_c/123", "account_id": 2, "priority": 18},
        headers=auth_headers(),
    )
    assert resp.status_code == 200
    t_id = resp.json()["task_id"]
    task = TaskQueueRepository.get_by_id(t_id)
    assert task["account_id"] == 2
    assert task["priority"] == 18
    TaskQueueRepository.delete(t_id)

    # 2. 范围下载任务
    resp = client.post(
        "/api/download/range",
        json={"channel_identifier": "my_chan", "start_id": 1, "end_id": 5, "account_id": 3, "priority": 12},
        headers=auth_headers(),
    )
    assert resp.status_code == 200
    t_id = resp.json()["task_id"]
    task = TaskQueueRepository.get_by_id(t_id)
    assert task["account_id"] == 3
    assert task["priority"] == 12
    TaskQueueRepository.delete(t_id)


def test_get_single_task():
    t_id = "test_single_task_1"
    TaskQueueRepository.create(t_id, "download_link", {"links": ["https://t.me/c/1/2"]}, account_id=1, priority=15)
    try:
        resp = client.get(f"/api/tasks/{t_id}", headers=auth_headers())
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == t_id
        assert data["task_type"] == "download_link"
        assert data["account_id"] == 1
        assert data["priority"] == 15
        assert "params" in data

        # Non-existent task 404
        resp404 = client.get("/api/tasks/non_existent_999", headers=auth_headers())
        assert resp404.status_code == 404
    finally:
        TaskQueueRepository.delete(t_id)


def test_batch_task_retry():
    t1 = "batch_retry_1"
    t2 = "batch_retry_2"
    TaskQueueRepository.create(t1, "download_link", {"links": ["https://t.me/c/1/2"]})
    TaskQueueRepository.update_status(t1, "failed")
    TaskQueueRepository.create(t2, "download_link", {"links": ["https://t.me/c/1/3"]})
    TaskQueueRepository.update_status(t2, "cancelled")

    try:
        resp = client.post("/api/tasks/batch", json={"action": "retry", "task_ids": [t1, t2]}, headers=auth_headers())
        assert resp.status_code == 200
        data = resp.json()
        assert data["action"] == "retry"
        assert data["success_count"] == 2
        assert TaskQueueRepository.get_by_id(t1)["status"] == "queued"
        assert TaskQueueRepository.get_by_id(t2)["status"] == "queued"
    finally:
        TaskQueueRepository.delete(t1)
        TaskQueueRepository.delete(t2)


@pytest.mark.parametrize(
    ("task_type", "original_status", "active_status", "message"),
    [
        ("download_link", "failed", "running", "已有相同的下载任务 (#active-download)"),
        ("download_range", "cancelled", "paused", "已有暂停的下载任务 (#active-download)"),
        ("download_range", "paused", "queued", "已有相同的下载任务 (#active-download)"),
    ],
)
def test_retry_download_conflict_keeps_task_unchanged(task_type, original_status, active_status, message, monkeypatch):
    params = (
        {"links": [TEST_LINK]} if task_type == "download_link" else {"channel": "example", "start_id": 1, "end_id": 3}
    )
    params["download_target_key"] = "same-target"
    TaskQueueRepository.create("original", task_type, params, status=original_status)
    TaskQueueRepository.create("active-download", task_type, {**params, "force": True}, status=active_status)
    notify = Mock()
    broadcast = AsyncMock()
    monkeypatch.setattr(task_scheduler, "is_running", True)
    monkeypatch.setattr(task_scheduler, "notify_new_task", notify)
    monkeypatch.setattr(connection_manager, "broadcast", broadcast)

    headers = auth_headers()
    res = client.post("/api/tasks/original/retry", headers=headers)
    assert res.status_code == 409
    assert res.json()["detail"] == {
        "code": "duplicate_download_task",
        "task_id": "active-download",
        "status": active_status,
        "message": message,
    }

    batch = client.post("/api/tasks/batch", json={"action": "retry", "task_ids": ["original"]}, headers=headers)
    assert batch.status_code == 200
    assert batch.json()["success_count"] == 0
    result = batch.json()["results"][0]
    assert result["success"] is False
    assert result["error"] == message
    assert TaskQueueRepository.get_by_id("original")["status"] == original_status
    notify.assert_not_called()
    broadcast.assert_not_awaited()

    assert TaskQueueRepository.update_status("active-download", "completed")
    assert client.post("/api/tasks/original/retry", headers=headers).status_code == 200
    assert TaskQueueRepository.get_by_id("original")["status"] == "queued"
    notify.assert_called_once()


# 覆盖:
# - paused 且无存活协程的 resume -> 走重调度分支 (不再永久卡 running)
# - 僵尸 running (无协程) 的 cancel -> 直接落库 cancelled (僵尸修复路径)
# - 暂停中的 live 协程任务不可删除 (防删除后协程 KeyError)

client = TestClient(app, client=("127.0.0.1", 50000))


@pytest.mark.parametrize("action", ["resume", "steady-resume"])
def test_resume_queued_download_conflict(action):
    params = {"links": [TEST_LINK], "download_target_key": "same-target"}
    TaskQueueRepository.create("cooling-download", "download_link", params, status="queued")
    assert TaskQueueRepository.mark_retry("cooling-download", 1, 123.0, "冷却中")
    TaskQueueRepository.create("active-download", "download_link", params, status="running")

    response = client.post(f"/api/tasks/cooling-download/{action}", headers=auth_headers())
    assert response.status_code == 409
    assert response.json()["detail"] == {
        "code": "duplicate_download_task",
        "task_id": "active-download",
        "status": "running",
        "message": "已有相同的下载任务 (#active-download)",
    }
    row = TaskQueueRepository.get_by_id("cooling-download")
    assert row["status"] == "queued"
    assert row["next_retry_at"] == 123.0


@pytest.mark.parametrize("original_status", ["failed", "cancelled", "paused"])
def test_resume_inactive_download_conflict_preserves_checkpoint(original_status):
    params = {
        "channel": "example",
        "start_id": 1,
        "end_id": 10,
        "contiguous_cursor_id": 4,
        "download_target_key": "same-target",
    }
    TaskQueueRepository.create("original", "download_range", params, status=original_status)
    TaskQueueRepository.create("active-download", "download_range", params, status="running")

    response = client.post("/api/tasks/original/resume", headers=auth_headers())
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "duplicate_download_task"
    assert response.json()["detail"]["task_id"] == "active-download"
    row = TaskQueueRepository.get_by_id("original")
    assert row["status"] == original_status
    assert row["params"] == params

    assert TaskQueueRepository.update_status("active-download", "completed")
    assert client.post("/api/tasks/original/resume", headers=auth_headers()).status_code == 200
    row = TaskQueueRepository.get_by_id("original")
    assert row["status"] == "queued"
    assert row["params"] == {**params, "start_id": 5}


def test_concurrent_resume_only_enqueues_one_download_for_a_target():
    params = {"links": [TEST_LINK], "download_target_key": "same-target"}
    task_ids = ("failed-a", "failed-b")
    for task_id in task_ids:
        TaskQueueRepository.create(task_id, "download_link", params, status="failed")

    headers = auth_headers()
    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda tid: client.post(f"/api/tasks/{tid}/resume", headers=headers), task_ids))

    winners = [tid for tid, res in zip(task_ids, responses, strict=True) if res.status_code == 200]
    assert len(winners) == 1
    conflict = next(res for res in responses if res.status_code == 409)
    assert conflict.json()["detail"]["task_id"] == winners[0]
    assert TaskQueueRepository.get_by_id(winners[0])["status"] == "queued"
    assert TaskQueueRepository.get_by_id(conflict.request.url.path.split("/")[-2])["status"] == "failed"


def test_resume_queue_reports_conflict_and_resumes_other_tasks():
    params = {"links": [TEST_LINK], "download_target_key": "same-target"}
    TaskQueueRepository.create("blocked", "download_link", params, status="paused")
    TaskQueueRepository.create("active-download", "download_link", params, status="running")
    TaskQueueRepository.create("ready", "download_link", {**params, "download_target_key": "other"}, status="paused")

    response = client.post("/api/tasks/queue/resume", headers=auth_headers())
    assert response.status_code == 200
    result = response.json()
    assert result["resumed_count"] == 1
    assert result["skipped_conflicts"] == [
        {
            "task_id": "blocked",
            "conflict": {
                "code": "duplicate_download_task",
                "task_id": "active-download",
                "status": "running",
                "message": "已有相同的下载任务 (#active-download)",
            },
        }
    ]
    assert TaskQueueRepository.get_by_id("blocked")["status"] == "paused"
    assert TaskQueueRepository.get_by_id("ready")["status"] == "queued"


def test_resume_paused_without_coroutine_reschedules(monkeypatch):
    """paused 但协程已死 (无 active_task_controls) -> resume 落入重调度分支 (统一入队等待调度器)"""
    task_id = "zombie_paused"
    TaskQueueRepository.create(task_id, "download_range", {"channel": "c", "start_id": 1, "end_id": 2})
    TaskQueueRepository.update_status(task_id, "paused")

    resp = client.post(f"/api/tasks/{task_id}/resume", headers=auth_headers())
    assert resp.status_code == 200
    assert resp.json()["status"] == "queued"

    task = TaskQueueRepository.get_by_id(task_id)
    # D4: 调度器未运行时任务保持 queued, 由调度器启动后拾起 (BackgroundTasks 直跑通道已废除)
    assert task["status"] == "queued"
    logs, _, _ = TaskQueueRepository.get_task_logs(task_id, 0, 200)
    assert any("恢复" in l["message"] or "续传" in l["message"] for l in logs)


def test_cancel_zombie_running_marks_cancelled():
    """僵尸 running (协程已死) 的 cancel 允许直接落库终态 (修复路径)"""
    task_id = "zombie_running"
    TaskQueueRepository.create(task_id, "download_link", {"links": ["https://t.me/x/1"]})
    TaskQueueRepository.update_status(task_id, "running")

    resp = client.post(f"/api/tasks/{task_id}/cancel", headers=auth_headers())
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"
    assert TaskQueueRepository.get_by_id(task_id)["status"] == "cancelled"


def test_delete_paused_live_coroutine_forbidden():
    """暂停中且协程存活的任务不可删除 (防 runner 后续访问 controls 时 KeyError)"""
    task_id = "paused_live"
    TaskQueueRepository.create(task_id, "download_link", {"links": ["https://t.me/x/1"]})
    TaskQueueRepository.update_status(task_id, "paused")
    active_task_controls[task_id] = {"cancelled": False, "paused": True}
    try:
        resp = client.delete(f"/api/tasks/{task_id}", headers=auth_headers())
        assert resp.status_code == 400
        assert "不能删除" in resp.json()["detail"]
    finally:
        active_task_controls.pop(task_id, None)
        TaskQueueRepository.delete(task_id)


def test_delete_paused_zombie_allowed():
    """暂停中但协程已死 (残留状态) 的任务可直接删除"""
    task_id = "paused_zombie"
    TaskQueueRepository.create(task_id, "download_link", {"links": ["https://t.me/x/1"]})
    TaskQueueRepository.update_status(task_id, "paused")

    resp = client.delete(f"/api/tasks/{task_id}", headers=auth_headers())
    assert resp.status_code == 200
    assert resp.json()["status"] == "deleted"


@pytest.mark.asyncio
async def test_cancelled_in_dispatch_window_skips_execution(monkeypatch):
    """调度窗口竞态: 认领与执行器启动之间用户已取消 -> 执行器直接退出, DB 终态保持 cancelled"""
    from app.services.download_runner import run_download_range_task

    task_id = "window_cancelled"
    TaskQueueRepository.create(task_id, "download_range", {"channel": "c", "start_id": 1, "end_id": 2})
    # 模拟调度器已认领 (控制项已创建) 后、执行器启动前用户取消: 标志置位 + cancel 端点写 DB 终态
    active_task_controls[task_id] = {"cancelled": True, "paused": False}
    TaskQueueRepository.update_status(task_id, "cancelled")

    called = []

    async def forbidden(*args, **kwargs):
        called.append(args)

    monkeypatch.setattr("app.services.download_runner.DownloaderService.resolve_message_from_link", forbidden)

    try:
        await run_download_range_task(task_id, "c", 1, 2)

        # 终态不被执行器改写为 running/completed
        assert TaskQueueRepository.get_status(task_id) == "cancelled"
        assert called == [], "执行器必须跳过下载, 不得发起任何消息解析"
        logs, _, _ = TaskQueueRepository.get_task_logs(task_id, 0, 200)
        assert any("调度窗口" in l["message"] for l in logs)
        # 无存活协程持有控制项 -> 自检路径应清理, 不残留
        assert task_id not in active_task_controls
    finally:
        active_task_controls.pop(task_id, None)
        TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_paused_in_dispatch_window_skips_execution(monkeypatch):
    """调度窗口竞态: 窗口内置 paused -> 执行器直接退出, DB 终态保持 paused"""
    from app.services.download_runner import run_download_links_task

    task_id = "window_paused"
    TaskQueueRepository.create(task_id, "download_link", {"links": ["https://t.me/c/1/2"]})
    active_task_controls[task_id] = {"cancelled": False, "paused": True}
    TaskQueueRepository.update_status(task_id, "paused")

    called = []

    async def forbidden(*args, **kwargs):
        called.append(args)

    monkeypatch.setattr("app.services.download_runner.DownloaderService.resolve_message_from_link", forbidden)

    try:
        await run_download_links_task(task_id, ["https://t.me/c/1/2"])

        assert TaskQueueRepository.get_status(task_id) == "paused"
        assert called == []
        logs, _, _ = TaskQueueRepository.get_task_logs(task_id, 0, 200)
        assert any("调度窗口" in l["message"] for l in logs)
        assert task_id not in active_task_controls
    finally:
        active_task_controls.pop(task_id, None)
        TaskQueueRepository.delete(task_id)


client = TestClient(app, client=("127.0.0.1", 50000))


def _make_task(task_id: str, task_type: str = "download_link", params: dict = None):
    TaskQueueRepository.create(task_id, task_type, params or {"links_count": 1})
    return task_id


def test_append_log_seq_incremental():
    task_id = _make_task("log_seq_1")
    for msg in ("first", "second", "third"):
        TaskQueueRepository.append_log(task_id, msg)

    logs, last_seq, has_more = TaskQueueRepository.get_task_logs(task_id)
    assert [l["seq"] for l in logs] == [1, 2, 3]
    assert [l["message"] for l in logs] == ["first", "second", "third"]
    assert last_seq == 3
    assert not has_more

    # 增量: after_seq=2 只返回第三条
    logs, last_seq, has_more = TaskQueueRepository.get_task_logs(task_id, after_seq=2)
    assert [l["seq"] for l in logs] == [3]
    assert last_seq == 3
    assert not has_more


def test_append_log_concurrent_no_loss():
    """多线程并发 append: seq 唯一且无缺失 (INSERT OR IGNORE + MAX(seq)+1 兜底)"""
    task_id = _make_task("log_conc_1")
    total, workers, per_worker = 200, 8, 25

    def _append_many(prefix: str):
        for i in range(per_worker):
            TaskQueueRepository.append_log(task_id, f"{prefix}-{i}")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(_append_many, [f"w{w}" for w in range(workers)]))

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT seq FROM task_logs WHERE task_id = ? ORDER BY seq", (task_id,))
    seqs = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT COUNT(DISTINCT message) FROM task_logs WHERE task_id = ?", (task_id,))
    distinct = cursor.fetchone()[0]
    conn.close()

    assert len(seqs) == total
    assert distinct == total
    # 允许 INSERT OR IGNORE 语义下的并发保护 (理论不应触发), 但序列必须连续且唯一
    assert seqs == list(range(1, total + 1))


def test_logs_endpoint_incremental_and_404():
    task_id = _make_task("log_api_1")
    for i in range(5):
        TaskQueueRepository.append_log(task_id, f"line-{i}")

    resp = client.get(f"/api/tasks/{task_id}/logs", headers=auth_headers())
    assert resp.status_code == 200
    body = resp.json()
    assert body["task_id"] == task_id
    assert body["last_seq"] == 5
    assert not body["has_more"]
    assert [l["seq"] for l in body["logs"]] == [1, 2, 3, 4, 5]

    # 差量
    resp = client.get(
        f"/api/tasks/{task_id}/logs",
        params={"after_seq": 3, "limit": 10},
        headers=auth_headers(),
    )
    body = resp.json()
    assert [l["seq"] for l in body["logs"]] == [4, 5]

    # limit 夹取 + has_more
    resp = client.get(
        f"/api/tasks/{task_id}/logs",
        params={"limit": 2},
        headers=auth_headers(),
    )
    body = resp.json()
    assert len(body["logs"]) == 2
    assert body["has_more"] is True

    # 不存在的任务
    resp = client.get("/api/tasks/no_such_task/logs", headers=auth_headers())
    assert resp.status_code == 404

    TaskQueueRepository.delete(task_id)


def test_get_task_logs_level_and_keyword_filters():
    """get_task_logs 合并后的 level/关键词过滤 (api/logs 端点共用此仓储查询)"""
    task_id = _make_task("log_filter_1")
    TaskQueueRepository.append_log(task_id, "INFO ordinary line")
    TaskQueueRepository.append_log(task_id, "INFO target needle here")
    TaskQueueRepository.append_log(task_id, "ERROR broken line")
    TaskQueueRepository.append_log(task_id, "WARNING shaky line")
    rows, last_seq, has_more = TaskQueueRepository.get_task_logs(task_id)
    assert [r["seq"] for r in rows] == [1, 2, 3, 4]
    assert has_more is False
    assert all("created_at" in r for r in rows)

    rows, _, _ = TaskQueueRepository.get_task_logs(task_id, level="info")
    assert [r["seq"] for r in rows] == [1, 2, 3, 4]

    # append_log 写点固定 level="info", 其余级别过滤为空 (语义锁定: 过滤不虚构级别)
    rows, _, _ = TaskQueueRepository.get_task_logs(task_id, level="error")
    assert rows == []

    rows, _, _ = TaskQueueRepository.get_task_logs(task_id, query="needle")
    assert [r["seq"] for r in rows] == [2]

    # 过滤与增量组合: after_seq=2 之后仅剩 ERROR 一条
    rows, _, _ = TaskQueueRepository.get_task_logs(task_id, after_seq=2, query="line")
    assert [r["seq"] for r in rows] == [3, 4]

    # 不存在的任务
    assert TaskQueueRepository.get_task_logs("no_such_task") is None
    TaskQueueRepository.delete(task_id)


def test_task_logs_independent_storage():
    """验证 task_logs 独立存储，task_queue 表不包含废弃 log_messages 列"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(task_queue)")
    cols = {row["name"] for row in cursor.fetchall()}
    assert "log_messages" not in cols
    conn.close()

    task_id = _make_task("log_storage_1")
    TaskQueueRepository.append_log(task_id, "日志一")
    TaskQueueRepository.append_log(task_id, "日志二")

    logs, last_seq, _ = TaskQueueRepository.get_task_logs(task_id)
    assert [l["message"] for l in logs] == ["日志一", "日志二"]
    assert [l["seq"] for l in logs] == [1, 2]
    assert last_seq == 2

    TaskQueueRepository.append_log(task_id, "新日志")
    logs, _, _ = TaskQueueRepository.get_task_logs(task_id)
    assert logs[-1]["seq"] == 3
    assert logs[-1]["message"] == "新日志"


def test_get_all_slim_with_log_stats():
    """GET /api/tasks 瘦身: 无全量 logs, 附 log_count / last_log_seq"""
    task_id = _make_task("log_slim_1")
    for i in range(3):
        TaskQueueRepository.append_log(task_id, f"msg{i}")

    tasks = TaskQueueRepository.get_all(50)
    target = next(t for t in tasks if t["id"] == task_id)
    assert "logs" not in target
    assert "log_messages" not in target
    assert "params_json" not in target
    assert target["log_count"] == 3
    assert target["last_log_seq"] == 3

    resp = client.get("/api/tasks", headers=auth_headers())
    assert resp.status_code == 200
    api_target = next(t for t in resp.json() if t["id"] == task_id)
    assert "logs" not in api_target
    assert api_target["log_count"] == 3
    assert api_target["last_log_seq"] == 3

    TaskQueueRepository.delete(task_id)


def test_delete_task_cascades_logs():
    task_id = _make_task("log_del_1")
    TaskQueueRepository.append_log(task_id, "to be deleted")
    TaskQueueRepository.delete(task_id)

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM task_logs WHERE task_id = ?", (task_id,))
    assert cursor.fetchone()[0] == 0
    cursor.execute("SELECT COUNT(*) FROM download_breakpoints WHERE task_id = ?", (task_id,))
    assert cursor.fetchone()[0] == 0
    conn.close()

    # clear_finished 级联
    task_id2 = _make_task("log_del_2")
    TaskQueueRepository.append_log(task_id2, "finished log")
    TaskQueueRepository.update_status(task_id2, "cancelled")
    TaskQueueRepository.clear_finished()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM task_logs WHERE task_id = ?", (task_id2,))
    assert cursor.fetchone()[0] == 0
    conn.close()
    assert TaskQueueRepository.get_by_id(task_id2) is None


def test_ws_log_message_has_seq_and_level():
    """WS log 消息新增 seq/level 字段 (增量字段, 既有字段不变 —— 红线5)"""
    import asyncio

    task_id = _make_task("log_ws_1")
    query = f"?ticket={issue_media_ticket()}" if settings.API_KEY else ""

    with client.websocket_connect(f"/ws/progress{query}") as websocket:
        # append_log 的广播经 create_task 调度, 需在事件循环内调用并留出一拍让其执行
        async def _append_and_drive():
            TaskQueueRepository.append_log(task_id, "ws message")
            await asyncio.sleep(0.2)

        asyncio.run(_append_and_drive())
        data = websocket.receive_json()
        assert data["type"] == "log"
        assert data["task_id"] == task_id
        assert data["message"] == "ws message"
        assert "time" in data
        assert data["seq"] == 1
        assert data["level"] == "info"


def test_steady_resume_endpoint_and_params():
    """测试低速稳态恢复接口成功将任务降档收敛至单通道稳态参数"""
    task_id = f"test_steady_{int(time.time() * 1000)}"
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        status="paused",
        params={
            "target_channel": "-1009000000022",
            "remote_files": [{"file_id": "f_steady_01", "name": "file.mp4"}],
            "prefetch_threads": 4,
            "upload_threads": 4,
            "min_send_delay_seconds": 3.0,
            "auto_downshifted": False,
        },
    )

    try:
        mock_acc = MagicMock(account_id=1, max_concurrency=5, is_flood_locked=lambda: False)
        with (
            patch.object(type(task_scheduler), "is_running", True),
            patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        ):
            resp = client.post(f"/api/tasks/{task_id}/steady-resume", headers=auth_headers())
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] in ("queued", "running")

            # 校验 DB 中 params 已原地更新
            task = TaskQueueRepository.get_by_id(task_id)
            assert task is not None
            params = task.get("params") or {}
            assert params.get("pikpak_prefetch_concurrency") == 1
            assert params.get("upload_concurrency") == 1
            assert params.get("auto_downshifted") is True
            assert params.get("send_delay_seconds") >= 20.0
    finally:
        TaskQueueRepository.delete(task_id)


def test_resume_queued_task_in_cooldown():
    """测试在冷却倒计时中的 queued 任务可被手动立即恢复，并清除全局 PikPak 冷却锁"""
    task_id = f"test_cooldown_res_{int(time.time() * 1000)}"
    future_retry = time.time() + 900.0
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        status="queued",
        params={"target_channel": "-1009000000022"},
    )
    TaskQueueRepository.mark_retry(task_id, 1, future_retry, "Tier 1 冷却中")

    # 设置调度器全局冷却
    task_scheduler.set_pikpak_cooldown(future_retry)
    assert task_scheduler.is_pikpak_in_cooldown() is True

    try:
        mock_acc = MagicMock(account_id=1, max_concurrency=5, is_flood_locked=lambda: False)
        with (
            patch.object(type(task_scheduler), "is_running", True),
            patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        ):
            resp = client.post(f"/api/tasks/{task_id}/resume", headers=auth_headers())
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "queued"

            # 冷却锁与 next_retry_at 均已清空
            assert task_scheduler.is_pikpak_in_cooldown() is False
            task = TaskQueueRepository.get_by_id(task_id)
            assert task is not None
            assert task.get("next_retry_at") == 0.0
    finally:
        TaskQueueRepository.delete(task_id)
        task_scheduler.clear_pikpak_cooldown()


def test_skip_current_file_happy_path():
    """测试跳过当前异常文件：留置占位卡、登记防重、清除分卷检查点并记录 skipped_files"""
    task_id = f"test_skip_{int(time.time() * 1000)}"
    file_id = "fid_skip_001"
    target_channel = "-1009000000010"
    target_chat_id = parse_chat_identifier(target_channel)

    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        status="paused",
        params={
            "target_channel": target_channel,
            "remote_files": [
                {
                    "file_id": file_id,
                    "file_name": "corrupt_video.mp4",
                    "file_size_bytes": 104857600,
                    "parent_folder_id": "root",
                }
            ],
            "pikpak_part_checkpoints": {
                file_id: {
                    "current_part_id": 2,
                    "delivered": {"1": 101},
                }
            },
        },
    )

    fake_sent_msg = MagicMock(id=8888)
    fake_client = MagicMock()
    fake_client.send_file = AsyncMock(return_value=fake_sent_msg)
    fake_acc = MagicMock(account_id=1, account_name="TestBot", client=fake_client, is_ready=lambda: True)

    try:
        with (
            patch("app.core.client_manager.client_manager.get_account", return_value=fake_acc),
            patch("app.core.pipeline.placeholder._load_static_poster", return_value=b"fake_image_bytes"),
        ):
            resp = client.post(
                f"/api/tasks/{task_id}/skip-current",
                json={"file_id": file_id, "reason": "CDN 503 频控阻断跳过"},
                headers=auth_headers(),
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "paused"
            assert "corrupt_video.mp4" in data["message"]

            # 1. 验证 Userbot 留置了占位卡
            fake_client.send_file.assert_awaited_once()

            # 2. 验证防重账本已登记该文件
            assert PublishHistoryRepository.is_published(
                target_chat_id, "", 0, source_type="pikpak", remote_file_id=file_id
            )

            # 3. 验证占位槽已登记
            slot = PlaceholderSlotRepository.get_by_identity(
                target_chat_id, "pikpak://root/corrupt_video.mp4", 104857600
            )
            assert slot is not None
            assert slot["state"] == "placeholder_sent"
            assert slot["placeholder_msg_id"] == 8888
            assert slot["task_id"] == task_id

            # 4. 验证检查点已清除，skipped_files 已记录
            task = TaskQueueRepository.get_by_id(task_id)
            assert task is not None
            params = task.get("params") or {}
            checkpoints = params.get("pikpak_part_checkpoints") or {}
            assert file_id not in checkpoints

            skipped = params.get("skipped_files") or []
            assert len(skipped) == 1
            assert skipped[0]["file_name"] == "corrupt_video.mp4"
            assert skipped[0]["reason"] == "CDN 503 频控阻断跳过"
    finally:
        TaskQueueRepository.delete(task_id)


def test_skip_current_file_defensive_validations():
    """测试跳过文件接口的防御性校验 (409 运行中, 400 终态, 404 不存在)"""
    task_id = f"test_skip_val_{int(time.time() * 1000)}"
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        status="running",
        params={"target_channel": "-1009000000010", "remote_files": []},
    )

    try:
        # 1. 运行中任务直接跳过 -> 409 Conflict
        resp = client.post(f"/api/tasks/{task_id}/skip-current", headers=auth_headers())
        assert resp.status_code == 409

        # 2. 已完成任务 -> 400 Bad Request
        TaskQueueRepository.update_status(task_id, "completed")
        resp = client.post(f"/api/tasks/{task_id}/skip-current", headers=auth_headers())
        assert resp.status_code == 400

        # 3. 不存在任务 -> 404 Not Found
        resp = client.post("/api/tasks/non_existent_9999/skip-current", headers=auth_headers())
        assert resp.status_code == 404
    finally:
        TaskQueueRepository.delete(task_id)


def test_steady_resume_running_task_online_downshift():
    """测试对运行中任务在线触发低速稳态恢复，原地将参数收敛为最低稳态档位"""
    task_id = f"test_run_down_{int(time.time() * 1000)}"
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        status="running",
        params={
            "target_channel": "-1009000000022",
            "remote_files": [{"file_id": "f_run_01", "name": "run.mp4"}],
            "upload_concurrency": 4,
            "pikpak_prefetch_concurrency": 4,
            "send_delay_seconds": 3.0,
            "auto_downshifted": False,
        },
    )

    try:
        resp = client.post(f"/api/tasks/{task_id}/steady-resume", headers=auth_headers())
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "running"
        assert "低速稳态" in data["message"]

        task = TaskQueueRepository.get_by_id(task_id)
        assert task is not None
        params = task.get("params") or {}
        assert params.get("upload_concurrency") == 1
        assert params.get("pikpak_prefetch_concurrency") == 1
        assert params.get("auto_downshifted") is True
        assert params.get("send_delay_seconds") >= 20.0
    finally:
        TaskQueueRepository.delete(task_id)


def test_resume_safe_paused_clears_error_detail_and_resets_strikes():
    """测试从 Safe-Pause 恢复时清空 error_detail、重置 strike 计数与 safe_paused 标志"""
    task_id = f"test_sp_res_{int(time.time() * 1000)}"
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        status="paused",
        params={
            "target_channel": "-1009000000022",
            "remote_files": [{"file_id": "f_sp_01", "name": "sp.mp4"}],
            "throttle_strike_count": 3,
            "data_risk_strike_count": 2,
            "safe_paused": True,
        },
    )
    TaskQueueRepository.set_error_detail(task_id, "🟠 [风控安全挂起] 连续遭遇上游频控拦截已达 3 次阈值")

    try:
        mock_acc = MagicMock(account_id=1, max_concurrency=5, is_flood_locked=lambda: False)
        with (
            patch.object(type(task_scheduler), "is_running", True),
            patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        ):
            resp = client.post(f"/api/tasks/{task_id}/resume?mode=steady", headers=auth_headers())
            assert resp.status_code == 200

            task = TaskQueueRepository.get_by_id(task_id)
            assert task is not None
            assert task.get("error_detail") == ""
            params = task.get("params") or {}
            assert params.get("safe_paused") is False
            assert params.get("throttle_strike_count") == 0
            assert params.get("data_risk_strike_count") == 0
            assert params.get("auto_downshifted") is True
    finally:
        TaskQueueRepository.delete(task_id)


def test_skip_current_file_reuses_existing_placeholder_slot():
    """测试跳过文件时复用既有占位槽 (J6 守卫: 不重复向 Telegram 发卡防刷屏)"""
    task_id = f"test_skip_reuse_{int(time.time() * 1000)}"
    file_id = "fid_reuse_001"
    target_channel = "-1009000000010"
    target_chat_id = parse_chat_identifier(target_channel)
    slot_identity = "pikpak://my_folder/file_reuse.mp4"
    file_size = 52428800

    # 预先登记既有占位槽
    PlaceholderSlotRepository.insert_new_slot(
        target_chat_id,
        slot_identity,
        file_size,
        slot_kind="backfillable",
        state="placeholder_sent",
        placeholder_msg_id=7777,
        client_label="ExistingBot",
        sender_account_id=1,
        task_id=task_id,
        fail_reason="先前的网络异常",
    )

    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        status="paused",
        params={
            "target_channel": target_channel,
            "remote_files": [
                {
                    "file_id": file_id,
                    "name": "file_reuse.mp4",
                    "size_bytes": file_size,
                    "parent_id": "my_folder",
                }
            ],
        },
    )

    fake_client = MagicMock()
    fake_client.send_file = AsyncMock()
    fake_acc = MagicMock(account_id=1, account_name="TestBot", client=fake_client, is_ready=lambda: True)

    try:
        with patch("app.core.client_manager.client_manager.get_account", return_value=fake_acc):
            resp = client.post(
                f"/api/tasks/{task_id}/skip-current",
                json={"file_id": file_id, "reason": "用户在诊断面板手动跳过"},
                headers=auth_headers(),
            )
            assert resp.status_code == 200
            data = resp.json()
            assert "7777" in data["message"]

            # 验证没有向 Telegram 客户端重复发送文件 (复用既有卡)
            fake_client.send_file.assert_not_called()

            # 验证既有槽 fail_count 自增且 reason 更新
            slot = PlaceholderSlotRepository.get_by_identity(target_chat_id, slot_identity, file_size)
            assert slot is not None
            assert slot["fail_count"] >= 1
            assert slot["fail_reason"] == "用户在诊断面板手动跳过"

            # 验证防重账本记录了 msg_id=7777
            assert PublishHistoryRepository.is_published(
                target_chat_id, "", 0, source_type="pikpak", remote_file_id=file_id
            )
    finally:
        TaskQueueRepository.delete(task_id)


def test_skip_current_file_with_parent_id():
    """测试使用 parent_id (PikPakFileItem 标准字段) 正确派生 pikpak:// 身份路径"""
    task_id = f"test_skip_pid_{int(time.time() * 1000)}"
    file_id = "fid_pid_001"
    target_channel = "-1009000000010"
    target_chat_id = parse_chat_identifier(target_channel)

    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        status="paused",
        params={
            "target_channel": target_channel,
            "remote_files": [
                {
                    "file_id": file_id,
                    "name": "standard_file.mp4",
                    "size_bytes": 20480,
                    "parent_id": "parent_dir_999",
                }
            ],
        },
    )

    fake_sent_msg = MagicMock(id=9999)
    fake_client = MagicMock()
    fake_client.send_file = AsyncMock(return_value=fake_sent_msg)
    fake_acc = MagicMock(account_id=1, account_name="TestBot", client=fake_client, is_ready=lambda: True)

    try:
        with (
            patch("app.core.client_manager.client_manager.get_account", return_value=fake_acc),
            patch("app.core.pipeline.placeholder._load_static_poster", return_value=b"fake_poster"),
            patch("app.core.entity_resolver.resolve_entity", AsyncMock(return_value=target_chat_id)),
        ):
            resp = client.post(
                f"/api/tasks/{task_id}/skip-current",
                json={"file_id": file_id},
                headers=auth_headers(),
            )
            assert resp.status_code == 200

            # 验证槽身份路径包含 parent_dir_999
            slot = PlaceholderSlotRepository.get_by_identity(
                target_chat_id, "pikpak://parent_dir_999/standard_file.mp4", 20480
            )
            assert slot is not None
            assert slot["placeholder_msg_id"] == 9999
    finally:
        TaskQueueRepository.delete(task_id)


def test_skip_current_file_rejects_already_published():
    """测试跳过文件时拒绝跳过已发布完成的文件"""
    task_id = f"test_skip_dup_{int(time.time() * 1000)}"
    file_id = "fid_dup_001"
    target_channel = "-1009000000010"
    target_chat_id = parse_chat_identifier(target_channel)

    # 预置已发布记录
    PublishHistoryRepository.record(
        target_chat_id,
        "pikpak://dir/done.mp4",
        1000,
        "",
        source_type="pikpak",
        remote_file_id=file_id,
        tg_message_id=555,
    )

    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        status="paused",
        params={
            "target_channel": target_channel,
            "remote_files": [
                {
                    "file_id": file_id,
                    "name": "done.mp4",
                    "size_bytes": 1000,
                    "parent_id": "dir",
                }
            ],
        },
    )

    try:
        resp = client.post(
            f"/api/tasks/{task_id}/skip-current",
            json={"file_id": file_id},
            headers=auth_headers(),
        )
        assert resp.status_code == 400
        assert "无需重复跳过" in resp.json()["detail"]
    finally:
        TaskQueueRepository.delete(task_id)


def test_retry_endpoint_clears_safe_paused_strikes_and_global_cooldown():
    """测试通用 /retry 接口同样会清除 safe_paused、阶梯风控计数与全局 PikPak 冷却锁"""
    task_id = f"test_retry_clear_{int(time.time() * 1000)}"
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        status="paused",
        params={
            "target_channel": "-1009000000010",
            "remote_files": [{"file_id": "fid_r1", "name": "r1.mp4", "size_bytes": 1024}],
            "safe_paused": True,
            "throttle_strike_count": 3,
            "data_risk_strike_count": 1,
        },
    )
    task_scheduler.set_pikpak_cooldown(time.time() + 900.0)

    try:
        resp = client.post(f"/api/tasks/{task_id}/retry", headers=auth_headers())
        assert resp.status_code == 200
        row = TaskQueueRepository.get_by_id(task_id)
        assert row is not None
        assert row["status"] == "queued"
        assert row["retry_count"] == 0
        assert not row["params"].get("safe_paused")
        assert not row["params"].get("throttle_strike_count")
        assert not row["params"].get("data_risk_strike_count")
        assert task_scheduler.is_pikpak_in_cooldown() is False
    finally:
        TaskQueueRepository.delete(task_id)
