"""API Layer: 本地发布、目录预检、频道预设、存储文件分页与路径逃逸防御测试 (Publish & Storage API)

覆盖范围:
- 发布参数校验与目录预检 (/api/publish/folder, /api/publish/inspect)
- SEC-001 路径穿越、空字节注入与系统受限目录防御防护
- 频道目标/来源预设增删查 (/api/presets)
- 本地存储文件浏览分页、HTTP 206 视频流式切片与路径安全 (/api/storage/files)
"""

import os
import threading
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from _safeio import safe_write_bytes
from fastapi.testclient import TestClient

from app.api.auth import issue_2fa_token, issue_media_ticket
from app.config import settings
from app.db.download_repository import DownloadHistoryRepository
from app.db.storage_repository import StorageRegistryRepository
from app.db.task_repository import TaskQueueRepository
from app.main import app

client = TestClient(app, client=("127.0.0.1", 50000))


def auth_headers():
    headers = {}
    if settings.API_KEY:
        headers["X-API-Key"] = settings.API_KEY
        token = issue_2fa_token()
        if token:
            headers["X-2FA-Token"] = token
    return headers


def test_presets_api():
    # 获取现有列表
    resp = client.get("/api/presets", headers=auth_headers())
    assert resp.status_code == 200
    initial_count = len(resp.json())

    # 新增预设
    resp = client.post("/api/presets", json={"name": "API测试频道", "channel_id": "-10088888"}, headers=auth_headers())
    assert resp.status_code == 200
    preset_id = resp.json()["id"]

    # 再次查询
    resp = client.get("/api/presets", headers=auth_headers())
    assert len(resp.json()) == initial_count + 1

    # 删除预设
    resp = client.delete(f"/api/presets/{preset_id}", headers=auth_headers())
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_publish_folder_non_existent():
    resp = client.post(
        "/api/publish/folder",
        json={
            "folder_path": "/invalid/non_existent_path_12345",
            "target_channel": "-10012345",
            "album_mode": True,
            "delay_seconds": 1.5,
        },
        headers=auth_headers(),
    )
    assert resp.status_code == 400
    assert "本地文件夹路径不存在" in resp.json()["detail"]


def test_publish_folder_placeholder_card_param_loop(tmp_dir, monkeypatch):
    """/api/publish/folder 参数闭环 —— task_params 持久化
    send_placeholder_card (派发统一入 task_queue, 由调度器拾起执行)。"""
    pub_folder = os.path.join(tmp_dir, "ph_loop")
    os.makedirs(pub_folder, exist_ok=True)

    resp = client.post(
        "/api/publish/folder",
        json={
            "folder_path": pub_folder,
            "target_channel": "-10012345",
            "album_mode": True,
            "delay_seconds": 1.5,
            "send_placeholder_card": False,
        },
        headers=auth_headers(),
    )
    assert resp.status_code == 200
    task_id = resp.json()["task_id"]
    rec = TaskQueueRepository.get_by_id(task_id)
    assert rec is not None
    assert rec.get("params").get("send_placeholder_card") is False


def test_publish_folder_placeholder_card_default_true(tmp_dir, monkeypatch):
    """不传 send_placeholder_card → 默认 True 入 params (现网默认行为零变化)。"""
    pub_folder = os.path.join(tmp_dir, "ph_default")
    os.makedirs(pub_folder, exist_ok=True)

    resp = client.post(
        "/api/publish/folder",
        json={
            "folder_path": pub_folder,
            "target_channel": "-10012345",
            "album_mode": True,
            "delay_seconds": 1.5,
        },
        headers=auth_headers(),
    )
    assert resp.status_code == 200
    rec = TaskQueueRepository.get_by_id(resp.json()["task_id"])
    assert rec.get("params").get("send_placeholder_card") is True


def test_publish_inspect_api(tmp_dir, monkeypatch):
    # 路径不存在
    resp = client.post(
        "/api/publish/inspect",
        json={"folder_path": os.path.join(tmp_dir, "non_existent_folder")},
        headers=auth_headers(),
    )
    assert resp.status_code == 400

    # 正常扫描 (隔离通道探测与 Premium 刷新，避免单测触发真实 MTProto 连接)
    safe_write_bytes(os.path.join(tmp_dir, "sample.mp4"), b"\0" * (1024 * 1024))
    monkeypatch.setattr(
        "app.services.channel_inspector.ChannelInspector.inspect_channel",
        AsyncMock(
            return_value={
                "channel_identifier": "-100900008",
                "parsed_chat_id": "-100900008",
                "channel_title": "Test Channel",
                "is_accessible": True,
                "has_write_permission": True,
                "permission_summary": "ok",
            }
        ),
    )
    monkeypatch.setattr("app.core.client_manager.client_manager.refresh_premium", AsyncMock(return_value=False))

    resp = client.post(
        "/api/publish/inspect", json={"folder_path": tmp_dir, "target_channel": "-100900008"}, headers=auth_headers()
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_files"] == 1
    assert data["valid_files_count"] == 1
    assert data["total_size_mb"] == 1.0
    assert data["oversized_files"] == []
    assert data["images"]["count"] == 0
    assert data["videos"]["count"] == 1


def test_publish_inspect_api_reports_unavailable_files(tmp_dir, monkeypatch):
    """巡检期不可读文件经响应模型序列化后必须可达 HTTP 客户端
    (FastAPI response_model 过滤未声明字段 —— 服务层字典有键而模型未声明时端点层是哑的)"""
    safe_write_bytes(os.path.join(tmp_dir, "ok.mkv"), b"V" * 10)
    safe_write_bytes(os.path.join(tmp_dir, "gone.mkv"), b"V" * 10)

    real_getsize = os.path.getsize

    def flaky_getsize(path, *args, **kwargs):
        if str(path).endswith("gone.mkv"):
            raise OSError("file disappeared between walk and inspect")
        return real_getsize(path, *args, **kwargs)

    monkeypatch.setattr("app.services.publisher_service.os.path.getsize", flaky_getsize)

    resp = client.post("/api/publish/inspect", json={"folder_path": tmp_dir}, headers=auth_headers())
    assert resp.status_code == 200
    data = resp.json()
    assert data["unavailable_files_count"] == 1, "响应模型必须声明 unavailable_files_count"
    assert data["unavailable_files"][0]["file_name"] == "gone.mkv"
    assert data["unavailable_files"][0]["reason"]
    media_names = {f["file_name"] for f in data["media_files"]}
    assert media_names == {"ok.mkv"}, "不可读文件不得出现在媒体明细"


def test_preset_api_with_types():
    # 1. 创建目标预设
    resp_t = client.post(
        "/api/presets",
        json={"name": "API测试目标频道", "channel_id": "-100900036", "preset_type": "target", "title": "目标标题"},
        headers=auth_headers(),
    )
    assert resp_t.status_code == 200
    preset_t_id = resp_t.json()["id"]

    # 2. 创建源预设
    resp_s = client.post(
        "/api/presets",
        json={"name": "API测试源频道", "channel_id": "-100900049", "preset_type": "source", "title": "源标题"},
        headers=auth_headers(),
    )
    assert resp_s.status_code == 200
    preset_s_id = resp_s.json()["id"]

    # 3. 按类型过滤查询
    resp_all = client.get("/api/presets", headers=auth_headers())
    assert resp_all.status_code == 200
    assert any(p["id"] == preset_t_id for p in resp_all.json())
    assert any(p["id"] == preset_s_id for p in resp_all.json())

    resp_target_only = client.get("/api/presets?type=target", headers=auth_headers())
    assert resp_target_only.status_code == 200
    assert any(p["id"] == preset_t_id for p in resp_target_only.json())
    assert not any(p["id"] == preset_s_id for p in resp_target_only.json())

    # 4. 删除
    client.delete(f"/api/presets/{preset_t_id}", headers=auth_headers())
    client.delete(f"/api/presets/{preset_s_id}", headers=auth_headers())


def test_sec001_path_traversal_guard(tmp_dir):
    # 1. 检查相对路径穿越 ../
    resp_traversal = client.post(
        "/api/publish/inspect", json={"folder_path": "../../windows/system32"}, headers=auth_headers()
    )
    assert resp_traversal.status_code == 400
    assert "检测到相对路径逃逸字符" in resp_traversal.json()["detail"]

    # 2. 检查空字节注入
    resp_null_byte = client.post(
        "/api/publish/inspect", json={"folder_path": "/tmp/test\x00/hack"}, headers=auth_headers()
    )
    assert resp_null_byte.status_code == 400
    assert "包含非法空字符" in resp_null_byte.json()["detail"]

    # 3. 检查系统受限目录 (例如 Windows 根系统目录)
    system_dir = "C:\\Windows" if os.name == "nt" else "/etc"
    if os.path.exists(system_dir):
        resp_sensitive = client.post("/api/publish/inspect", json={"folder_path": system_dir}, headers=auth_headers())
        assert resp_sensitive.status_code == 400
        assert "受限的系统目录" in resp_sensitive.json()["detail"]

    # 4. 检查工程根目录直接扫描防护
    resp_base = client.post(
        "/api/publish/inspect", json={"folder_path": str(settings.BASE_DIR)}, headers=auth_headers()
    )
    assert resp_base.status_code == 400
    assert "系统工程根目录" in resp_base.json()["detail"]

    # 5. 检查发布接口的路径穿越防护 (publish_folder)
    resp_publish = client.post(
        "/api/publish/folder",
        json={"folder_path": "../../etc/shadow", "target_channel": "my_channel"},
        headers=auth_headers(),
    )
    assert resp_publish.status_code == 400
    assert "检测到相对路径逃逸字符" in resp_publish.json()["detail"]

    # 6. 检查敏感凭据目录 (.gemini) 及伪造 antigravity 路径绕过防护
    fake_gemini = os.path.join(tmp_dir, "antigravity", ".gemini")
    os.makedirs(fake_gemini, exist_ok=True)
    resp_fake = client.post("/api/publish/inspect", json={"folder_path": fake_gemini}, headers=auth_headers())
    assert resp_fake.status_code == 400
    assert "检测到敏感凭据或配置目录" in resp_fake.json()["detail"]


client = TestClient(app, client=("127.0.0.1", 50000))


def _touch(path: str, size: int = 1024) -> str:
    return safe_write_bytes(path, b"x" * size)


def test_storage_files_pagination_and_filter(tmp_dir):
    old_download = settings.DOWNLOAD_DIR
    dl_dir = os.path.join(tmp_dir, "downloads")
    os.makedirs(dl_dir, exist_ok=True)
    settings.DOWNLOAD_DIR = dl_dir
    try:
        # 创建多种类型文件
        _touch(os.path.join(dl_dir, "movie.mp4"), 2048)
        _touch(os.path.join(dl_dir, "photo.jpg"), 1024)
        _touch(os.path.join(dl_dir, "doc.pdf"), 512)
        _touch(os.path.join(dl_dir, "part1.part"), 300)

        # 扫描
        resp = client.post("/api/storage/scan", headers=auth_headers())
        assert resp.status_code == 200

        # 分页查询全部
        resp = client.get("/api/storage/files?page=1&limit=2", headers=auth_headers())
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 4
        assert len(data["files"]) == 2
        assert data["page"] == 1

        # 分类查询 video
        resp = client.get("/api/storage/files?page=1&limit=10&kind=video", headers=auth_headers())
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["files"][0]["file_name"] == "movie.mp4"
        assert data["files"][0]["category"] == "video"

        # 关键词搜索
        resp = client.get("/api/storage/files?page=1&limit=10&query=photo", headers=auth_headers())
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["files"][0]["file_name"] == "photo.jpg"
    finally:
        settings.DOWNLOAD_DIR = old_download


def test_http_206_video_streaming(tmp_dir):
    old_download = settings.DOWNLOAD_DIR
    dl_dir = os.path.join(tmp_dir, "downloads")
    os.makedirs(dl_dir, exist_ok=True)
    settings.DOWNLOAD_DIR = dl_dir
    try:
        # 创建 1000 字节测试视频
        safe_write_bytes(os.path.join(dl_dir, "stream_test.mp4"), b"A" * 500 + b"B" * 500)

        # 1. 无 Range 请求 -> 200 OK
        resp = client.get("/api/storage/preview/stream_test.mp4", headers=auth_headers())
        assert resp.status_code == 200
        assert resp.headers.get("Accept-Ranges") == "bytes"
        assert resp.headers.get("Content-Length") == "1000"
        assert len(resp.content) == 1000

        # 2. 正常 Range: bytes=0-499 -> 206 Partial Content
        headers = auth_headers()
        headers["Range"] = "bytes=0-499"
        resp = client.get("/api/storage/preview/stream_test.mp4", headers=headers)
        assert resp.status_code == 206
        assert resp.headers.get("Content-Range") == "bytes 0-499/1000"
        assert resp.headers.get("Content-Length") == "500"
        assert resp.content == b"A" * 500

        # 3. 后半段 Range: bytes=500- -> 206 Partial Content
        headers["Range"] = "bytes=500-"
        resp = client.get("/api/storage/preview/stream_test.mp4", headers=headers)
        assert resp.status_code == 206
        assert resp.headers.get("Content-Range") == "bytes 500-999/1000"
        assert resp.headers.get("Content-Length") == "500"
        assert resp.content == b"B" * 500

        # 4. 后缀 Range: bytes=-200 -> 206 Partial Content
        headers["Range"] = "bytes=-200"
        resp = client.get("/api/storage/preview/stream_test.mp4", headers=headers)
        assert resp.status_code == 206
        assert resp.headers.get("Content-Range") == "bytes 800-999/1000"
        assert resp.headers.get("Content-Length") == "200"
        assert resp.content == b"B" * 200

        # 5. 越界 Range: bytes=2000- -> 416 Range Not Satisfiable
        headers["Range"] = "bytes=2000-"
        resp = client.get("/api/storage/preview/stream_test.mp4", headers=headers)
        assert resp.status_code == 416

    finally:
        settings.DOWNLOAD_DIR = old_download


def test_path_traversal_prevention(tmp_dir):
    old_download = settings.DOWNLOAD_DIR
    dl_dir = os.path.join(tmp_dir, "downloads")
    os.makedirs(dl_dir, exist_ok=True)
    settings.DOWNLOAD_DIR = dl_dir
    try:
        # 尝试越界访问
        resp = client.get("/api/storage/preview/../../../../etc/passwd", headers=auth_headers())
        assert resp.status_code in (400, 403, 404)

        resp = client.get("/api/storage/download/../../../../Windows/System32/cmd.exe", headers=auth_headers())
        assert resp.status_code in (400, 403, 404)

        resp = client.delete("/api/storage/files?path=../../test.txt", headers=auth_headers())
        assert resp.status_code == 200
        # 守卫应在 skipped 中记录路径越界
        assert len(resp.json()["skipped"]) > 0
        assert "越界" in resp.json()["skipped"][0]["reason"]
    finally:
        settings.DOWNLOAD_DIR = old_download


def test_storage_listed_path_deletes_completed_file_and_records():
    target = Path(settings.DOWNLOAD_DIR) / "two  spaces.mp4"
    target.write_bytes(b"completed data")
    DownloadHistoryRepository.record("-100123", 12, None, target.name, str(target), target.stat().st_size)

    headers = auth_headers()
    listed = client.get("/api/storage/files?page=1", headers=headers)
    assert listed.status_code == 200
    path = listed.json()["files"][0]["path"]
    assert path == target.name

    response = client.post("/api/storage/delete", json={"paths": [path]}, headers=headers)
    assert response.status_code == 200
    assert response.json() == {"deleted": [str(target.resolve())], "skipped": []}
    assert not target.exists()
    assert StorageRegistryRepository.query_files() == []
    assert DownloadHistoryRepository.get_recent() == []


def test_storage_listed_symlink_retains_its_path():
    directory = Path(settings.DOWNLOAD_DIR)
    target = directory / "movie.mp4"
    target.write_bytes(b"completed data")
    link = directory / "linked.part"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("当前环境无法创建文件符号链接")

    headers = auth_headers()
    listed = client.get("/api/storage/files?page=1", headers=headers)
    assert listed.status_code == 200
    path = next(file["path"] for file in listed.json()["files"] if file["file_name"] == link.name)
    assert path == link.name
    response = client.delete("/api/storage/files", params={"path": path}, headers=headers)
    assert response.status_code == 200
    assert response.json() == {"deleted": [], "skipped": [{"path": path, "reason": "拒绝删除符号链接"}]}
    assert link.is_symlink()
    assert target.exists()


@pytest.mark.parametrize("symlink", [False, True])
def test_storage_rejects_relative_traversal_before_symlink_check(symlink):
    directory = Path(settings.DOWNLOAD_DIR)
    target = directory.parent / "outside.bin"
    target.write_bytes(b"keep this file")
    entry = target
    if symlink:
        entry = directory.parent / "outside_link"
        try:
            entry.symlink_to(target)
        except OSError:
            pytest.skip("当前环境无法创建文件符号链接")

    path = f"../{entry.name}"
    response = client.post("/api/storage/delete", json={"paths": [path]}, headers=auth_headers())
    assert response.status_code == 200
    assert response.json() == {
        "deleted": [],
        "skipped": [{"path": path, "reason": "路径越界 (仅允许删除下载目录内文件)"}],
    }
    assert target.read_bytes() == b"keep this file"
    assert entry.is_symlink() == symlink


def test_storage_relative_paths_keep_active_breakpoints():
    part = Path(settings.DOWNLOAD_DIR) / "movie.mp4.part"
    meta = part.with_suffix(".meta")
    part.write_bytes(b"part data")
    meta.write_bytes(b"metadata")
    TaskQueueRepository.create("active-download", "download_link", {})

    response = client.post("/api/storage/delete", json={"paths": [part.name]}, headers=auth_headers())
    assert response.status_code == 200
    assert response.json()["deleted"] == []
    assert "断点文件已锁定" in response.json()["skipped"][0]["reason"]
    assert part.exists() and meta.exists()


def test_storage_keeps_active_final_file_under_symlink_root(tmp_path, monkeypatch):
    from app.services.downloader_service import (
        _register_active_download_paths_sync,
        _unregister_active_download_paths_sync,
    )

    real_root = tmp_path / "media"
    real_root.mkdir()
    configured_root = tmp_path / "downloads"
    try:
        configured_root.symlink_to(real_root, target_is_directory=True)
    except OSError:
        pytest.skip("当前环境无法创建目录符号链接")
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", str(configured_root))
    target = real_root / "active.mp4"
    target.write_bytes(b"current download")
    keys = _register_active_download_paths_sync((str(configured_root / target.name),))
    try:
        response = client.post(
            "/api/storage/delete", json={"paths": [target.name, str(target)]}, headers=auth_headers()
        )
        assert response.status_code == 200
        assert response.json()["deleted"] == []
        assert [entry["reason"] for entry in response.json()["skipped"]] == ["活跃任务正在使用该文件"] * 2
        assert target.read_bytes() == b"current download"
    finally:
        _unregister_active_download_paths_sync(keys)

    response = client.post("/api/storage/delete", json={"paths": [target.name]}, headers=auth_headers())
    assert response.json()["deleted"] == [str(target.resolve())]


def test_storage_breakpoints_and_metrics(tmp_dir):
    old_download = settings.DOWNLOAD_DIR
    dl_dir = os.path.join(tmp_dir, "downloads")
    os.makedirs(dl_dir, exist_ok=True)
    settings.DOWNLOAD_DIR = dl_dir
    try:
        # 创建配对断点与孤儿断点
        p_ok = _touch(os.path.join(dl_dir, "task1.mp4.part"), 500)
        m_ok = _touch(os.path.join(dl_dir, "task1.mp4.meta"), 100)
        p_orphan = _touch(os.path.join(dl_dir, "orphan.mp4.part"), 800)

        resp = client.get("/api/storage/breakpoints", headers=auth_headers())
        assert resp.status_code == 200
        bp_data = resp.json()
        assert bp_data["total"] >= 2
        assert bp_data["orphan_count"] >= 1
        assert bp_data["paired_count"] >= 1

        # 清理孤儿碎片
        resp = client.post("/api/storage/clean-orphans", headers=auth_headers())
        assert resp.status_code == 200
        clean_data = resp.json()
        assert clean_data["status"] == "ok"
        assert clean_data["cleaned_count"] >= 1
        assert not os.path.exists(p_orphan)
        assert os.path.exists(p_ok)
        assert os.path.exists(m_ok)

        # 查询指标
        resp = client.get("/api/storage/metrics", headers=auth_headers())
        assert resp.status_code == 200
        metrics = resp.json()
        assert "disk_total_bytes" in metrics
        assert "disk_free_bytes" in metrics
        assert "downloads_bytes" in metrics
        assert "effective_free_bytes" in metrics
        assert "is_quota_active" in metrics

    finally:
        settings.DOWNLOAD_DIR = old_download


def test_storage_quota_and_cached_disk_usage():
    from app.services.storage_service import compute_storage_quota_metrics, get_cached_disk_usage

    # 1. 测试 5 秒内存 TTL 缓存
    with patch("shutil.disk_usage", return_value=(1000, 400, 600)) as mock_du:
        d1 = get_cached_disk_usage("test_cache_path", ttl_seconds=5.0)
        get_cached_disk_usage("test_cache_path", ttl_seconds=5.0)
        assert d1["disk_total_bytes"] == 1000
        assert d1["disk_free_bytes"] == 600
        assert mock_du.call_count == 1  # 5秒内命中缓存，只调用一次内核 API

    # 2. 测试配额为 0 (不限配额) 时的有效可用计算
    from app.db.account_repository import SystemSettingsRepository

    SystemSettingsRepository.set("max_storage_quota_gb", "0")
    q0 = compute_storage_quota_metrics(downloads_bytes=100, disk_free_bytes=500)
    assert q0["is_quota_active"] is False
    assert q0["effective_free_bytes"] == 500  # 等于物理剩余

    # 3. 测试通过 API 设置配额 (支持 PUT 与 POST 双方法)
    resp_post = client.post("/api/storage/quota", json={"quota_gb": 15.0}, headers=auth_headers())
    assert resp_post.status_code == 200
    assert resp_post.json()["max_storage_quota_gb"] == 15.0

    resp = client.put("/api/storage/quota", json={"quota_gb": 10.0}, headers=auth_headers())
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    assert resp.json()["max_storage_quota_gb"] == 10.0

    # 4. 测试配额剩余小于物理剩余：取配额剩余
    # 配额 10 GiB = 10737418240 字节，已下载 2 GiB = 2147483648 字节，配额剩余 8 GiB
    # 物理可用 100 GiB -> 有效剩余应当取 8 GiB (8589934592)
    q_limited = compute_storage_quota_metrics(downloads_bytes=2 * (1024**3), disk_free_bytes=100 * (1024**3))
    assert q_limited["is_quota_active"] is True
    assert q_limited["effective_free_bytes"] == 8 * (1024**3)

    # 5. 测试配额剩余大于物理剩余：取物理剩余 (绝不虚报超量可用)
    # 配额 10 GB，已下载 1 GB，配额剩余 9 GB；但物理可用仅剩 3 GB -> 有效剩余应当取 3 GB
    q_physical_limited = compute_storage_quota_metrics(downloads_bytes=1 * (1024**3), disk_free_bytes=3 * (1024**3))
    assert q_physical_limited["effective_free_bytes"] == 3 * (1024**3)

    # 6. 清理恢复配额为 0
    client.put("/api/storage/quota", json={"quota_gb": 0}, headers=auth_headers())


def test_storage_media_preview_and_download_shared_guard(tmp_dir):
    """preview/download 共用 _resolve_media_file 前置三步 (校验/404/mimetype), 覆盖 200/206/attachment/404"""
    media_dir = os.path.join(settings.DOWNLOAD_DIR, "sample")
    os.makedirs(media_dir, exist_ok=True)
    media_path = os.path.join(media_dir, "clip.mp4")
    safe_write_bytes(media_path, b"\x00\x01\x02\x03fake-media-bytes")  # 20 字节

    media_qs = {"ticket": issue_media_ticket()}
    base = "/api/storage"

    # preview: 200 全量 + Range 能力 + mimetype 推断
    resp = client.get(f"{base}/preview/sample/clip.mp4", params=media_qs)
    assert resp.status_code == 200
    assert resp.headers["accept-ranges"] == "bytes"
    assert resp.headers["content-type"] == "video/mp4"

    # preview: 206 分块 (Range 是请求头, 鉴权凭短时效 ticket 且 TTL 内可重复消费)
    resp = client.get(f"{base}/preview/sample/clip.mp4", params=media_qs, headers={"Range": "bytes=0-3"})
    assert resp.status_code == 206
    assert resp.headers["content-range"] == "bytes 0-3/20"

    # download: attachment 头
    resp = client.get(f"{base}/download/sample/clip.mp4", params=media_qs)
    assert resp.status_code == 200
    assert "attachment" in resp.headers.get("content-disposition", "")

    # 共用 404 守卫 (两路由一致)
    assert client.get(f"{base}/preview/missing.mp4", params=media_qs).status_code == 404
    assert client.get(f"{base}/download/missing.mp4", params=media_qs).status_code == 404


def test_storage_metrics_auto_initialization(tmp_dir):
    """
    验证 Copilot 提出的指标时序正确性:
    即使未主动调用 /scan 扫描接口，直接访问 /api/storage/metrics 也必须自动初始化登记，
    绝不能误报 downloads_bytes 为 0。
    """
    from app.db.database import get_db

    old_download = settings.DOWNLOAD_DIR
    dl_dir = os.path.join(tmp_dir, "downloads_unscanned")
    os.makedirs(dl_dir, exist_ok=True)
    settings.DOWNLOAD_DIR = dl_dir

    try:
        # 1. 模拟全新的物理文件落地
        _touch(os.path.join(dl_dir, "fresh_movie.mkv"), 1024 * 1024)  # 1MB
        _touch(os.path.join(dl_dir, "fresh_photo.png"), 512 * 1024)  # 512KB

        # 2. 模拟 DB 中的登记表与扫描戳为空 (从未执行过扫描)
        conn = get_db()
        conn.execute("DELETE FROM storage_registry")
        conn.commit()
        conn.close()

        conn = get_db()
        conn.execute("DELETE FROM system_settings WHERE key = 'last_storage_scan_at'")
        conn.commit()
        conn.close()

        # 3. 未调用 /scan，直接读取 /metrics
        resp = client.get("/api/storage/metrics", headers=auth_headers())
        assert resp.status_code == 200
        metrics = resp.json()

        # 4. 必须自动触发初始化扫描并正确返回非零指标
        expected_bytes = (1024 * 1024) + (512 * 1024)
        assert metrics["downloads_bytes"] == expected_bytes
        assert metrics["total_files_count"] == 2
    finally:
        settings.DOWNLOAD_DIR = old_download


@pytest.mark.asyncio
async def test_publish_inspect_runs_off_event_loop(tmp_dir, monkeypatch):
    from app.api.publish import inspect_folder
    from app.models.publish import InspectFolderRequest
    from app.services.publisher_service import PublisherService

    caller_threads: list[int] = []
    event_loop_tid = threading.get_ident()
    real_inspect = PublisherService.inspect_folder

    def spy_inspect(*args, **kwargs):
        caller_threads.append(threading.get_ident())
        return real_inspect(*args, **kwargs)

    monkeypatch.setattr("app.services.publisher_service.PublisherService.inspect_folder", spy_inspect)
    monkeypatch.setattr("app.core.client_manager.client_manager.refresh_premium", AsyncMock(return_value=False))
    data = await inspect_folder(InspectFolderRequest(folder_path=str(tmp_dir)))
    assert data["folder_path"]
    assert caller_threads and caller_threads[0] != event_loop_tid


@pytest.mark.asyncio
async def test_storage_overview_runs_off_event_loop(monkeypatch):
    from app.api.storage import storage_overview
    from app.services.storage_service import StorageService

    caller_threads: list[int] = []
    event_loop_tid = threading.get_ident()
    real_overview = StorageService.get_overview

    def spy_overview(*args, **kwargs):
        caller_threads.append(threading.get_ident())
        return real_overview(*args, **kwargs)

    monkeypatch.setattr("app.services.storage_service.StorageService.get_overview", spy_overview)
    data = await storage_overview()
    assert "total_bytes" in data
    assert caller_threads and caller_threads[0] != event_loop_tid


def test_storage_files_respects_offset_without_page(monkeypatch):
    captured: list[dict] = []

    def fake_query_files(**kwargs):
        captured.append(kwargs)
        return [], 0

    monkeypatch.setattr("app.services.storage_service.StorageService.query_files", fake_query_files)
    resp = client.get("/api/storage/files?category=video&limit=10&offset=20", headers=auth_headers())
    assert resp.status_code == 200
    assert captured and captured[0]["offset"] == 20


def test_storage_preview_sets_sandbox_csp_and_bounds_body(tmp_dir, monkeypatch):
    from pathlib import Path

    monkeypatch.setattr(settings, "DOWNLOAD_DIR", str(tmp_dir))
    html_file = Path(tmp_dir) / "probe.html"
    html_file.write_bytes(b"<script>alert(1)</script>")

    resp = client.get("/api/storage/preview/probe.html", headers=auth_headers())
    assert resp.status_code == 200
    assert resp.headers.get("content-security-policy") == "sandbox"
    assert resp.headers.get("x-content-type-options") == "nosniff"

    resp_range = client.get("/api/storage/preview/probe.html", headers={**auth_headers(), "Range": "bytes=0-5"})
    assert resp_range.status_code == 206
    assert resp_range.headers.get("content-security-policy") == "sandbox"
    assert resp_range.headers.get("x-content-type-options") == "nosniff"

    # 若文件在响应构造 stat 之后继续追加写入，200 流式响应体也不得超过 Content-Length
    import builtins

    part_file = Path(tmp_dir) / "growing.part"
    part_file.write_bytes(b"12345")
    orig_open = builtins.open

    def fake_open(file, mode="r", *args, **kwargs):
        if "rb" in str(mode) and str(file).endswith("growing.part"):
            with orig_open(part_file, "ab") as af:
                af.write(b"67890")
        return orig_open(file, mode, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)
    resp_grow = client.get("/api/storage/preview/growing.part", headers=auth_headers())
    assert resp_grow.status_code == 200
    assert resp_grow.headers.get("content-length") == "5"
    assert resp_grow.content == b"12345"


def test_storage_quota_rejects_boolean():
    resp_bool = client.put("/api/storage/quota", json={"quota_gb": False}, headers=auth_headers())
    assert resp_bool.status_code == 422

    resp_zero = client.put("/api/storage/quota", json={"quota_gb": 0}, headers=auth_headers())
    assert resp_zero.status_code == 200
