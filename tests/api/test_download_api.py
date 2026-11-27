"""API Layer: 链接与区间下载任务提交、参数校验与并发线程透传测试 (Download API)

覆盖范围:
- 链接下载参数校验 (/api/download/link 空值/格式/账号校验)
- 区间下载参数校验 (/api/download/range 范围/起止编号)
- 区间预检接口 (/api/download/inspect-range)
- 下载线程入参校验、落库与参数透传 (threads 8/16/越界校验)
"""

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.api.auth import issue_2fa_token
from app.config import settings
from app.core.scheduler import task_scheduler
from app.db.download_repository import DownloadHistoryRepository
from app.db.task_repository import TaskQueueRepository
from app.main import app
from app.models.task import DownloadLinkRequest, DownloadRangeRequest
from app.services.downloader_service import DownloaderService

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


def record_download(msg_id, *, chat="example", task_id=None, grouped_id=None, content=b"media"):
    path = Path(settings.DOWNLOAD_DIR) / f"media-{msg_id}.bin"
    path.write_bytes(content)
    DownloadHistoryRepository.record(chat, msg_id, grouped_id, path.name, str(path), len(content), task_id=task_id)
    return path


@pytest.mark.parametrize(
    ("kind", "payload"),
    [
        ("link", {"links": "https://t.me/Example/10\nhttps://t.me/s/example/8/11"}),
        ("range", {"channel_identifier": "@Example", "start_id": 10, "end_id": 11}),
    ],
)
def test_download_history_conflict_and_force(kind, payload, monkeypatch):
    inspect = AsyncMock()
    monkeypatch.setattr(DownloaderService, "inspect_download_range", inspect)
    notify = Mock()
    monkeypatch.setattr(task_scheduler, "notify_new_task", notify)
    TaskQueueRepository.create("original", f"download_{kind}", {}, status="completed")
    for msg_id in (10, 11):
        record_download(msg_id, task_id="original")

    path = f"/api/download/{kind}"
    headers = auth_headers()
    response = client.post(path, json=payload, headers=headers)
    assert response.status_code == 409
    assert response.json()["detail"] == {
        "code": "already_downloaded",
        "message": "目标文件均已下载到本地",
        "task_id": "original",
        "file_count": 2,
        "file_names": ["media-10.bin", "media-11.bin"],
    }
    notify.assert_not_called()

    forced = client.post(path, json={**payload, "force": True}, headers=headers)
    assert forced.status_code == 200
    forced_id = forced.json()["task_id"]
    assert TaskQueueRepository.get_by_id(forced_id)["params"]["force"] is True
    for status in ("pending", "queued", "running", "paused"):
        assert TaskQueueRepository.update_status(forced_id, status)
        for force in (False, True):
            active = client.post(path, json={**payload, "force": force}, headers=headers)
            assert active.status_code == 409
            assert active.json()["detail"] == {
                "code": "duplicate_download_task",
                "task_id": forced_id,
                "status": status,
                "message": "已有暂停的下载任务" if status == "paused" else "已有相同的下载任务",
            }
    assert len(TaskQueueRepository.get_all()) == 2
    notify.assert_called_once()
    inspect.assert_not_awaited()


@pytest.mark.parametrize(
    "links",
    ["https://t.me/example/10\nhttps://t.me/example/11", "https://t.me/example/10\nunresolved link"],
)
def test_link_history_partial_or_unresolved_does_not_block(links):
    record_download(10)
    response = client.post("/api/download/link", json={"links": links}, headers=auth_headers())
    assert response.status_code == 200
    assert "force" not in TaskQueueRepository.get_by_id(response.json()["task_id"])["params"]


def test_unreadable_link_history_does_not_block_submission(monkeypatch):
    path = record_download(10)
    stat_file = os.stat

    def guarded_stat(candidate, *args, **kwargs):
        if str(candidate) == str(path):
            raise PermissionError("history file unreadable")
        return stat_file(candidate, *args, **kwargs)

    monkeypatch.setattr("app.db.download_repository.os.stat", guarded_stat)
    response = client.post(
        "/api/download/link", json={"links": "https://t.me/example/10\nhttps://t.me/example/11"}, headers=auth_headers()
    )
    assert response.status_code == 200
    assert DownloadHistoryRepository.get_recent()[0]["file_path"] == str(path)


@pytest.mark.parametrize("discard", ["missing", "empty", "directory"])
def test_range_stale_history_record_is_removed(discard):
    record_download(10)
    path = record_download(11, content=b"" if discard == "empty" else b"media")
    if discard != "empty":
        path.unlink()
        if discard == "directory":
            path.mkdir()
    payload = {"channel_identifier": "example", "start_id": 10, "end_id": 11}
    response = client.post("/api/download/range", json=payload, headers=auth_headers())
    assert response.status_code == 200
    assert [row["msg_id"] for row in DownloadHistoryRepository.get_recent()] == [10]


def test_sparse_range_skips_disk_checks_and_rpc(monkeypatch):
    record_download(10)
    check_disk = Mock()
    inspect = AsyncMock()
    monkeypatch.setattr(DownloadHistoryRepository, "get_valid_download_record", check_disk)
    monkeypatch.setattr(DownloaderService, "inspect_download_range", inspect)
    response = client.post(
        "/api/download/range",
        json={"channel_identifier": "example", "start_id": 10, "end_id": 10**9},
        headers=auth_headers(),
    )
    assert response.status_code == 200
    check_disk.assert_not_called()
    inspect.assert_not_awaited()


@pytest.mark.parametrize("kind", ["link", "range"])
@pytest.mark.parametrize("owner_status", [None, "failed", "completed"])
def test_partial_album_history_only_blocks_completed_range(kind, owner_status):
    if owner_status is not None:
        TaskQueueRepository.create(
            "album-owner",
            "download_range",
            {"channel": "-1009000000010", "start_id": 12, "end_id": 12},
            status=owner_status,
        )
    record_download(12, chat="-1009000000010", grouped_id="42", task_id="album-owner")
    link = "https://t.me/c/9000000010/4/12"
    payload = {"links": link} if kind == "link" else {"channel_identifier": link, "start_id": 12, "end_id": 12}
    response = client.post(f"/api/download/{kind}", json=payload, headers=auth_headers())
    if kind == "range" and owner_status == "completed":
        assert response.status_code == 409
        assert response.json()["detail"]["task_id"] == "album-owner"
    else:
        assert response.status_code == 200
        assert "force" not in TaskQueueRepository.get_by_id(response.json()["task_id"])["params"]


@pytest.mark.parametrize("cleanup", ["single", "bulk"])
@pytest.mark.parametrize("status", ["completed", "failed", "cancelled"])
def test_range_album_history_survives_task_cleanup_only_for_completed_owner(cleanup, status):
    task_id = "album-owner"
    TaskQueueRepository.create(task_id, "download_range", {}, status=status)
    file_path = record_download(12, chat="-1009000000010", grouped_id="42", task_id=task_id)
    headers = auth_headers()
    endpoint = f"/api/tasks/{task_id}" if cleanup == "single" else "/api/tasks"
    assert client.delete(endpoint, headers=headers).status_code == 200
    assert TaskQueueRepository.get_by_id(task_id) is None
    assert file_path.is_file()

    payload = {"channel_identifier": "-1009000000010", "start_id": 12, "end_id": 12}
    response = client.post("/api/download/range", json=payload, headers=headers)
    if status == "completed":
        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "already_downloaded"
        assert response.json()["detail"]["task_id"] is None
        file_path.unlink()
        assert client.post("/api/download/range", json=payload, headers=headers).status_code == 200
    else:
        assert response.status_code == 200


def test_history_from_multiple_tasks_has_no_historical_task_id():
    for msg_id in (10, 11):
        task_id = f"original-{msg_id}"
        TaskQueueRepository.create(task_id, "download_link", {}, status="completed")
        record_download(msg_id, task_id=task_id)
    response = client.post(
        "/api/download/link",
        json={"links": "https://t.me/example/10\nhttps://t.me/example/11"},
        headers=auth_headers(),
    )
    assert response.status_code == 409
    assert response.json()["detail"]["task_id"] is None


def test_download_link_validation():
    # 测试空链接校验
    response = client.post("/api/download/link", json={"links": "   \n  "}, headers=auth_headers())
    assert response.status_code == 400

    # 测试有效链接入参
    response = client.post("/api/download/link", json={"links": TEST_LINK}, headers=auth_headers())
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "task_id" in data


def test_download_range_validation():
    # 结束 ID < 起始 ID
    response = client.post(
        "/api/download/range",
        json={"channel_identifier": "test_channel", "start_id": 100, "end_id": 50},
        headers=auth_headers(),
    )
    assert response.status_code == 400

    # 正确入参
    response = client.post(
        "/api/download/range",
        json={"channel_identifier": "test_channel", "start_id": 1, "end_id": 10},
        headers=auth_headers(),
    )
    assert response.status_code == 200
    assert "task_id" in response.json()


def test_download_inspect_range_api():
    # 结束 ID < 起始 ID
    resp = client.post(
        "/api/download/inspect-range",
        json={"channel_identifier": "test_chan", "start_id": 50, "end_id": 10},
        headers=auth_headers(),
    )
    assert resp.status_code == 400


@pytest.mark.parametrize(
    ("status", "message"),
    [
        ("pending", "已有相同的下载任务"),
        ("queued", "已有相同的下载任务"),
        ("running", "已有相同的下载任务"),
        ("paused", "已有暂停的下载任务"),
        ("completed", None),
        ("failed", None),
        ("cancelled", None),
    ],
)
def test_range_duplicate_follows_current_task_status(status, message):
    headers = auth_headers()
    path = "/api/download/range"
    payload = {"channel_identifier": "@SomeChannel", "start_id": 1, "end_id": 10}
    first = client.post(path, json=payload, headers=headers)
    assert first.status_code == 200
    task_id = first.json()["task_id"]
    assert TaskQueueRepository.update_status(task_id, status)

    payload["channel_identifier"] = "https://t.me/s/somechannel/8"
    retry = client.post(path, json=payload, headers=headers)
    if message is None:
        assert retry.status_code == 200
        assert retry.json()["task_id"] != task_id
    else:
        assert retry.status_code == 409
        assert retry.json()["detail"] == {
            "code": "duplicate_download_task",
            "task_id": task_id,
            "status": status,
            "message": message,
        }
        assert len(TaskQueueRepository.get_all()) == 1


def test_concurrent_equivalent_link_submissions_create_one_task(monkeypatch):
    headers = auth_headers()
    both_ready = Barrier(2)
    notify = Mock()
    monkeypatch.setattr(task_scheduler, "notify_new_task", notify)
    requests = [
        {"links": "https://t.me/Channel/10\nhttps://t.me/c/9000000010/12", "threads": 8},
        {"links": "https://t.me/c/9000000010/4/12\nhttps://t.me/s/channel/10?single", "threads": 16},
    ]

    def submit(payload):
        both_ready.wait(timeout=5)
        return client.post("/api/download/link", json=payload, headers=headers)

    with ThreadPoolExecutor(max_workers=2) as pool:
        created, conflict = sorted(pool.map(submit, requests), key=lambda response: response.status_code)

    assert (created.status_code, conflict.status_code) == (200, 409)
    task_id = created.json()["task_id"]
    assert conflict.json()["detail"]["task_id"] == task_id
    assert [task["id"] for task in TaskQueueRepository.get_all()] == [task_id]
    notify.assert_called_once()


def test_different_link_batches_and_ranges_can_queue():
    headers = auth_headers()
    for links in ("https://t.me/channel/10", "https://t.me/channel/10\nhttps://t.me/channel/11"):
        assert client.post("/api/download/link", json={"links": links}, headers=headers).status_code == 200
    for end_id in (10, 11):
        payload = {"channel_identifier": "channel", "start_id": 10, "end_id": end_id}
        assert client.post("/api/download/range", json=payload, headers=headers).status_code == 200


def test_threads_parameter_flow():
    """[REQ-001] 校验 8/16 线程入参校验、落库与参数透传"""
    # 1. 正常 8 / 16 线程模式
    req8 = DownloadLinkRequest(links="https://t.me/c/12345/678", threads=8)
    assert req8.threads == 8

    req16 = DownloadRangeRequest(channel_identifier="-100900008", start_id=10, end_id=20, threads=16)
    assert req16.threads == 16

    # 2. 默认值兜底为 8 (对齐 DOWNLOAD_PARALLEL_CONNECTIONS=8)
    req_default = DownloadLinkRequest(links="https://t.me/c/12345/678")
    assert req_default.threads == 8

    # 3. 越界校验 (ge=1, le=32)
    with pytest.raises(ValidationError):
        DownloadLinkRequest(links="https://t.me/c/12345/678", threads=0)

    with pytest.raises(ValidationError):
        DownloadLinkRequest(links="https://t.me/c/12345/678", threads=64)

    # 4. DB 存储与读取 concurrency_total
    task_id = "test_threads_db_record"
    TaskQueueRepository.create(task_id, "download_link", {"threads": 8}, status="queued", concurrency_total=8)
    task_in_db = TaskQueueRepository.get_by_id(task_id)
    assert task_in_db is not None
    assert task_in_db["concurrency_total"] == 8
