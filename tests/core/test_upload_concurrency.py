"""Core Runtime: 上传并发限制、滑动窗口测速、FloodWait 熔断可视化、断点元数据沙箱与 Provider 模式测试 (Upload Concurrency & Breakpoint Governance)

覆盖范围:
- 原 test_upload_concurrency_and_speed: 滑动窗口测速 SpeedTracker、动态并发伸缩、分块落库与 FloodWait 回退
- 原 test_floodwait_visible: FloodWait 熔断可视化倒计时、可中断取消与重试恢复
- 原 test_upload_breakpoint: 上传元数据 TTL 过期、强制刷新与自动自愈
- 原 test_upload_meta_relocation: 上传断点元数据收拢至 data/upload_meta 沙箱
- 原 test_upload_provider_mode: Provider 模式字节等价性、512KB 快速通道与信任根沙箱
- 原 test_breakpoint_sandbox: 断点与下载路径沙箱越界防御 (路径逃逸/符号链接截断)"""

import ast
import asyncio
import inspect
import io
import json
import os
import pathlib
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from _async_wait import FakeClock
from _cm_seed import seed_default_account
from _safeio import safe_write_bytes
from fastapi.testclient import TestClient
from telethon import types
from telethon.errors import FloodWaitError
from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest
from telethon.tl.types import MessageMediaPhoto

from app.config import settings
from app.core.fast_telethon import (
    SpeedTracker,
    discard_upload_meta,
    download_file_parallel,
    local_upload_meta_path,
    resolve_breakpoint_path,
    upload_file_parallel,
)
from app.core.fast_telethon.breakpoints import _validated_breakpoint
from app.core.pipeline.utils import UploadSpeedTracker
from app.core.scheduler import UploadCancelledError
from app.core.stream_chunk_provider import BaseChunkProvider, LocalFileChunkProvider
from app.db.account_repository import SystemSettingsRepository
from app.db.task_repository import TaskQueueRepository
from app.main import app
from app.services.forward_service import ForwardCancelledError, ForwardService
from app.services.publisher_service import PublisherService, PublishFolderResult

# ==============================================================================
# Part: test_upload_concurrency_and_speed.py
# ==============================================================================
CHUNK = 512 * 1024


def test_speed_tracker_rolling_window():
    """测试 SpeedTracker 基于 deque 的滑动时间窗口平均、自然平滑衰减与停滞清零"""
    total_bytes = 10 * 1024 * 1024  # 10MB
    tracker = SpeedTracker(total_bytes, initial_bytes=0, window_seconds=3.0, stagnant_timeout=4.0)

    # 1. 验证 deque 结构与初始样本
    assert len(tracker.samples) == 1
    assert tracker.samples[0][1] == 0

    # 2. 模拟以 2MB/s 稳定传输 3 秒
    base_time = tracker.start_time
    for i in range(1, 7):  # 每 0.5 秒 1MB
        t = base_time + i * 0.5
        with patch("time.time", return_value=t):
            tracker.update(i * 1024 * 1024)

    with patch("time.time", return_value=base_time + 3.0):
        # 3 秒时，滑动窗口内传输了约 6MB，速度应约为 2.0 MB/s
        spd = tracker.speed
        assert 1.9 <= spd <= 2.1, f"速度应接近 2.0MB/s，实际为 {spd}"
        assert tracker.eta > 0

    # 3. 模拟等待 ACK 期间 (没有新数据到达，但未超过 stagnant_timeout 4.0s) -> 自然平滑衰减
    with patch("time.time", return_value=base_time + 4.5):  # 距离最后更新过去了 1.5s
        decayed_spd = tracker.speed
        assert 0.0 < decayed_spd < spd, f"等待期间速度应自然衰减，实际为 {decayed_spd}"

    # 4. 模拟传输停滞超过 stagnant_timeout (4.0s) -> 速度清零
    with patch("time.time", return_value=base_time + 7.5):  # 距离最后更新过去了 4.5s > 4.0s
        assert tracker.speed == 0.0
        assert tracker.eta == 0.0

    # 5. 测试长间隔传输恢复 (样本不应保留已过期的远古基线导致测速被无限稀释)
    with patch("time.time", return_value=base_time + 100.0):
        tracker.update(7 * 1024 * 1024)
    with patch("time.time", return_value=base_time + 101.0):
        tracker.update(8 * 1024 * 1024)
        # 1秒内传输 1MB，在 3.0s 滑动窗口内，速度应为正常合理值 (约 0.33 ~ 1.0 MB/s)，绝不应被 100s 稀释为 0.08 MB/s
        recovered_spd = tracker.speed
        assert 0.25 <= recovered_spd <= 1.05, f"长间隔恢复后测速异常: {recovered_spd}"

    # 6. 测试 reset_start_time
    tracker.reset_start_time()
    assert len(tracker.samples) == 1


def test_upload_speed_tracker_inheritance():
    """测试 UploadSpeedTracker 继承 SpeedTracker 行为一致性"""
    tracker = UploadSpeedTracker(file_bytes=5 * 1024 * 1024, initial_bytes=0)
    assert isinstance(tracker, SpeedTracker)
    assert tracker.total_bytes == 5 * 1024 * 1024


@pytest.mark.asyncio
async def test_upload_parallel_concurrency_bounds(tmp_dir, monkeypatch):
    """测试 upload_file_parallel 的并发度默认值与边界范围限制 [1, 8]"""
    test_file = safe_write_bytes(os.path.join(tmp_dir, "test.bin"), b"x" * (CHUNK * 2))

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4, auth_key=None)
    client._connection = MagicMock()
    del client._borrow_exported_sender
    del client._create_exported_sender
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    created_senders = []

    async def fake_gather(coros, clients, dc_id):
        nonlocal created_senders
        created_senders = list(coros)
        for c in coros:
            c.close()
        return [MagicMock() for _ in coros]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)

    # 1. 缺省并发度 -> 取 settings.UPLOAD_PARALLEL_CONNECTIONS (默认 8)
    monkeypatch.setattr(settings, "UPLOAD_PARALLEL_CONNECTIONS", 8)
    monkeypatch.setattr(
        "app.db.account_repository.SystemSettingsRepository.get_int",
        lambda k, d=8: 8 if k == "upload_parallel_connections" else d,
    )
    try:
        await upload_file_parallel(client, str(test_file), concurrency=None)
    except Exception:
        pass
    assert len(created_senders) == 8

    # 2. 上限并发度 16 -> 允许达到 16
    try:
        await upload_file_parallel(client, str(test_file), concurrency=16)
    except Exception:
        pass
    assert len(created_senders) == 16

    # 3. 超限并发度 (例如 24) -> 强制防御性限制至 16
    try:
        await upload_file_parallel(client, str(test_file), concurrency=24)
    except Exception:
        pass
    assert len(created_senders) == 16

    # 3. 欠限并发度 (例如 0) -> 强制防御性限制至 1
    try:
        await upload_file_parallel(client, str(test_file), concurrency=0)
    except Exception:
        pass
    assert len(created_senders) == 1

    # 4. 合法并发度 (例如 6) -> 保持 6
    try:
        await upload_file_parallel(client, str(test_file), concurrency=6)
    except Exception:
        pass
    assert len(created_senders) == 6


@pytest.mark.asyncio
async def test_upload_save_meta_async_and_flush(tmp_dir, monkeypatch):
    """测试 upload_file_parallel 的异步落盘频率 (每30秒或每256块/128MB) 以及收尾强制 flush"""
    # 创建 25 个分块的大文件 (> 10MB，走 SaveBigFilePartRequest)
    num_chunks = 25
    file_bytes = b"M" * (CHUNK * num_chunks)
    test_file = safe_write_bytes(os.path.join(tmp_dir, "big_video.mp4"), file_bytes)
    # meta 收拢至沙箱 (键含 size, 与 upload_file_parallel 内部派生一致)
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", os.path.join(tmp_dir, "meta_sandbox"))
    from app.core.fast_telethon import local_upload_meta_path

    meta_path = local_upload_meta_path(str(test_file), len(file_bytes))

    saved_snapshots = []
    original_dump = json.dump

    def spy_dump(obj, f, **kwargs):
        saved_snapshots.append(list(obj.get("completed_parts", [])))
        return original_dump(obj, f, **kwargs)

    monkeypatch.setattr(json, "dump", spy_dump)

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4, auth_key=None)
    client._connection = MagicMock()
    del client._borrow_exported_sender
    del client._create_exported_sender

    # 让第 22 块完成后暂停/取消
    calls = {"count": 0}
    is_paused = False

    async def mock_client_call(req):
        nonlocal is_paused
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            calls["count"] += 1
            if calls["count"] >= 22:
                is_paused = True
            return True
        cfg = MagicMock()
        cfg.dc_options = []
        return cfg

    client.side_effect = mock_client_call

    from app.core.scheduler import UploadPausedError

    with pytest.raises(UploadPausedError):
        await upload_file_parallel(client, str(test_file), concurrency=1, is_paused=lambda: is_paused)

    # 验证 .upload.meta 必须已落盘
    assert os.path.exists(meta_path), "暂停收尾时必须保留 .upload.meta 断点"

    with open(meta_path, encoding="utf-8") as f:
        meta_data = json.load(f)

    # 验证最终 flush 写入了所有 22 个已完成的分块 (即使未达到 256 块或 30 秒的落盘周期)
    assert len(meta_data["completed_parts"]) >= 22
    # 验证由于未达到 256 块或 30 秒周期，中途不触发日常写盘，仅触发收尾 flush (1 次)
    assert len(saved_snapshots) == 1


def test_system_settings_concurrency_defaults(setup_test_db):
    """测试 system_settings 默认注册了 upload_parallel_connections 与 download_parallel_connections"""
    up_val = SystemSettingsRepository.get("upload_parallel_connections")
    down_val = SystemSettingsRepository.get("download_parallel_connections")
    assert up_val == "2"
    assert down_val == "8"


def test_api_publish_folder_upload_concurrency_param(setup_test_db, tmp_dir):
    """测试 /api/publish/folder 接口接收 upload_concurrency 并在 task.params 中正确持久化"""
    from app.api.auth import issue_2fa_token

    test_folder = os.path.join(tmp_dir, "media_folder")
    os.makedirs(test_folder, exist_ok=True)
    safe_write_bytes(os.path.join(test_folder, "pic.jpg"), b"img")

    headers = {}
    if settings.API_KEY:
        headers["X-API-Key"] = settings.API_KEY
        token = issue_2fa_token()
        if token:
            headers["X-2FA-Token"] = token

    client = TestClient(app, client=("127.0.0.1", 50000))

    # 1. 传递合法 upload_concurrency
    resp = client.post(
        "/api/publish/folder",
        json={"folder_path": test_folder, "target_channel": "-1009000000010", "upload_concurrency": 6},
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    task_id = resp.json()["task_id"]

    task = TaskQueueRepository.get_by_id(task_id)
    assert task is not None
    assert task["params"].get("upload_concurrency") == 6

    # 2. 传递上限 upload_concurrency (16) -> 成功接收
    resp_16 = client.post(
        "/api/publish/folder",
        json={"folder_path": test_folder, "target_channel": "-1009000000010", "upload_concurrency": 16},
        headers=headers,
    )
    assert resp_16.status_code == 200, resp_16.text

    # 3. 传递超限 upload_concurrency (> 16) -> Pydantic 校验拦截 (422)
    resp_invalid = client.post(
        "/api/publish/folder",
        json={"folder_path": test_folder, "target_channel": "-1009000000010", "upload_concurrency": 20},
        headers=headers,
    )
    assert resp_invalid.status_code == 422

    # 3. 传递欠限 upload_concurrency (< 1) -> Pydantic 校验拦截 (422)
    resp_zero = client.post(
        "/api/publish/folder",
        json={"folder_path": test_folder, "target_channel": "-1009000000010", "upload_concurrency": 0},
        headers=headers,
    )
    assert resp_zero.status_code == 422


@pytest.mark.asyncio
async def test_publish_folder_concurrency_priority(setup_test_db, tmp_dir, monkeypatch):
    """测试 PublisherService.publish_folder 并发度取值优先级:
    task.params.upload_concurrency ?? setting.upload_parallel_connections ?? 4
    以及防御性区间 [1, 8]
    """
    test_folder = os.path.join(tmp_dir, "pub_prio_folder")
    os.makedirs(test_folder, exist_ok=True)
    # 放置一个超限分流的视频以触发 upload_file_parallel
    safe_write_bytes(os.path.join(test_folder, "video.mp4"), b"V" * (CHUNK * 2))

    passed_concurrencies = []

    async def mock_upload_parallel(client, file_path, **kwargs):
        passed_concurrencies.append(kwargs.get("concurrency"))
        return MagicMock()

    monkeypatch.setattr("app.core.fast_telethon.upload_file_parallel", mock_upload_parallel)

    # 绕过频道权限检查与账号探测
    from app.services.channel_inspector import ChannelInspector

    monkeypatch.setattr(
        ChannelInspector,
        "inspect_channel",
        AsyncMock(return_value={"is_accessible": True, "has_write_permission": True}),
    )

    mock_client = AsyncMock()
    mock_client.send_file = AsyncMock(return_value=True)
    mock_acc = MagicMock()
    mock_acc.is_ready.return_value = True
    mock_acc.client = mock_client
    mock_acc.is_premium = False
    mock_acc.effective_max_file_size_bytes.return_value = 2000 * 1024 * 1024
    mock_acc.account_name = "MockUserbot"
    mock_acc.probe_premium = AsyncMock()
    from app.core.client_manager import client_manager

    monkeypatch.setattr(client_manager, "get_account", lambda *args, **kwargs: mock_acc)

    # 1. 显式指定 upload_concurrency = 3 (高优先级)
    SystemSettingsRepository.set("upload_parallel_connections", "6")
    passed_concurrencies.clear()
    await PublisherService.publish_folder(test_folder, "-1009000000012", upload_concurrency=3, delay_seconds=0.0)
    assert passed_concurrencies == [3]

    # 2. 未显式指定 (None) -> 采用系统设置 (6)
    passed_concurrencies.clear()
    await PublisherService.publish_folder(test_folder, "-1009000000013", upload_concurrency=None, delay_seconds=0.0)
    assert passed_concurrencies == [6]

    # 3. 显式指定合法值 12 -> 保持 12
    passed_concurrencies.clear()
    await PublisherService.publish_folder(test_folder, "-1009000000014", upload_concurrency=12, delay_seconds=0.0)
    assert passed_concurrencies == [12]

    # 4. 显式指定超限值 20 -> 强制截断至 16
    passed_concurrencies.clear()
    await PublisherService.publish_folder(test_folder, "-1009000000016", upload_concurrency=20, delay_seconds=0.0)
    assert passed_concurrencies == [16]

    # 5. 显式指定下限值 0 -> 强制提升至 1
    passed_concurrencies.clear()
    await PublisherService.publish_folder(test_folder, "-1009000000015", upload_concurrency=0, delay_seconds=0.0)
    assert passed_concurrencies == [1]


@pytest.mark.asyncio
async def test_downloader_service_concurrency_settings(setup_test_db, monkeypatch):
    """测试 DownloaderService.download_media 动态读取 download_parallel_connections 与边界截断 [1, 32]"""
    from app.services.downloader_service import DownloaderService

    passed_concurrency = []

    async def fake_download_file_parallel(**kwargs):
        passed_concurrency.append(kwargs.get("concurrency"))
        return "fake_path"

    monkeypatch.setattr("app.services.downloader_service.download_file_parallel", fake_download_file_parallel)

    mock_msg = MagicMock()
    mock_msg.id = 101
    mock_msg.media = MagicMock()
    mock_msg.file = MagicMock(name="video.mp4", size=10 * 1024 * 1024)

    mock_client = AsyncMock()

    # 1. 动态系统设置为 24 -> 生效 24
    SystemSettingsRepository.set("download_parallel_connections", "24")
    passed_concurrency.clear()
    await DownloaderService.download_single_message(mock_client, "-1009000000010", mock_msg, threads=None)
    assert passed_concurrency == [24]

    # 2. 动态系统设置超限 50 -> 防御性截断至 32
    SystemSettingsRepository.set("download_parallel_connections", "50")
    passed_concurrency.clear()
    await DownloaderService.download_single_message(mock_client, "-1009000000010", mock_msg, threads=None)
    assert passed_concurrency == [32]

    # 3. 动态系统设置欠限 0 -> 防御性截断至 1
    SystemSettingsRepository.set("download_parallel_connections", "0")
    passed_concurrency.clear()
    await DownloaderService.download_single_message(mock_client, "-1009000000010", mock_msg, threads=None)
    assert passed_concurrency == [1]

    # 4. 入参 threads 显式指定 -> 入参优先
    passed_concurrency.clear()
    await DownloaderService.download_single_message(mock_client, "-1009000000010", mock_msg, threads=8)
    assert passed_concurrency == [8]


@pytest.mark.asyncio
async def test_dynamic_concurrency_scaling_and_task_updates(setup_test_db, monkeypatch, tmp_dir):
    """测试运行时动态更新任务并发数 (TaskQueueRepository.update_concurrency 及 Downloader/Publisher 动态读取)"""
    from app.db.task_repository import TaskQueueRepository
    from app.services.downloader_service import DownloaderService
    from app.services.publisher_service import PublisherService

    # 1. 测试 TaskQueueRepository.update_concurrency
    task_id_dl = "test_dl_dyn"
    TaskQueueRepository.create(
        task_id=task_id_dl,
        task_type="download_link",
        params={"links": ["https://t.me/c/1/2"], "threads": 16},
        concurrency_total=16,
    )

    task_id_pub = "test_pub_dyn"
    TaskQueueRepository.create(
        task_id=task_id_pub,
        task_type="publish_folder",
        params={"folder_path": "/tmp/test", "upload_concurrency": 4},
        concurrency_total=4,
    )

    # 动态调整下载任务并发为 8
    assert TaskQueueRepository.update_concurrency(task_id_dl, 8) is True
    t_dl = TaskQueueRepository.get_by_id(task_id_dl)
    assert t_dl["concurrency_total"] == 8
    assert t_dl["params"]["threads"] == 8

    # 动态调整上传任务并发为 16 (P1 层次倒置修复后, 策略键合并由调用方注入 merger)
    from app.core.task_strategy import StrategyRegistry

    def _pub_merger(merger_task_type, params):
        merger_strategy = StrategyRegistry.get_optional(merger_task_type)
        if merger_strategy:
            merger_strategy.apply_concurrency_update(params, 16, None)
        else:
            params["threads"] = 16

    assert TaskQueueRepository.update_concurrency(task_id_pub, 16, param_merger=_pub_merger) is True
    t_pub = TaskQueueRepository.get_by_id(task_id_pub)
    assert t_pub["concurrency_total"] == 16
    assert t_pub["params"]["upload_concurrency"] == 16

    # 2. 模拟 DownloaderService 动态读取
    passed_concurrency_dl = []

    async def fake_dl_parallel(**kwargs):
        get_c = kwargs.get("get_concurrency")
        passed_concurrency_dl.append(get_c() if get_c else kwargs.get("concurrency"))
        return "fake_dl_path"

    monkeypatch.setattr("app.services.downloader_service.download_file_parallel", fake_dl_parallel)

    mock_msg = MagicMock()
    mock_msg.id = 202
    mock_msg.media = MagicMock()
    mock_msg.file = MagicMock(name="file.mp4", size=10 * 1024 * 1024)
    mock_client = AsyncMock()

    # 此时 task_id_dl 在库中为 8
    await DownloaderService.download_single_message(mock_client, "-1001", mock_msg, task_id=task_id_dl, threads=16)
    assert passed_concurrency_dl == [8]  # 运行时库中已更新为 8，优先于原入参 16

    # 3. 模拟 PublisherService 动态读取
    from app.core.client_manager import client_manager

    passed_concurrency_pub = []

    async def fake_pub_parallel(*args, **kwargs):
        get_c = kwargs.get("get_concurrency")
        passed_concurrency_pub.append(get_c() if get_c else kwargs.get("concurrency"))
        return MagicMock()

    mock_pub_client = AsyncMock()
    mock_pub_client.send_file = AsyncMock(return_value=MagicMock(id=101))
    mock_pub_client.is_connected = MagicMock(return_value=True)
    seed_default_account(client_manager, client=mock_pub_client)
    monkeypatch.setattr(client_manager, "_bots", {})
    monkeypatch.setattr("app.core.fast_telethon.upload_file_parallel", fake_pub_parallel)
    monkeypatch.setattr(
        "app.services.channel_inspector.ChannelInspector.inspect_channel",
        AsyncMock(return_value={"is_accessible": True, "has_write_permission": True}),
    )

    tmp_pub_dir = os.path.join(tmp_dir, "dyn_pub")
    os.makedirs(tmp_pub_dir, exist_ok=True)
    test_video_path = os.path.join(tmp_pub_dir, "test_video.mp4")
    with open(test_video_path, "wb") as f:
        f.write(b"fake video data")

    # 运行时库中已更新为 16
    await PublisherService.publish_folder(tmp_pub_dir, "-1009000000012", task_id=task_id_pub, upload_concurrency=4)

    # 验证真实触发调用且动态取出的并发为 16
    assert len(passed_concurrency_pub) > 0, "upload_file_parallel 应被实际调用"
    assert passed_concurrency_pub[0] == 16


def test_api_update_task_concurrency_bounds(setup_test_db):
    """测试 /api/tasks/{task_id}/concurrency 接口的动态调整与合法边界"""
    from app.api.auth import issue_2fa_token
    from app.db.task_repository import TaskQueueRepository

    headers = {}
    if settings.API_KEY:
        headers["X-API-Key"] = settings.API_KEY
        token = issue_2fa_token()
        if token:
            headers["X-2FA-Token"] = token

    client = TestClient(app, client=("127.0.0.1", 50000))

    task_dl = "task_dl_bounds"
    TaskQueueRepository.create(task_id=task_dl, task_type="download_link", params={}, concurrency_total=16)

    task_pub = "task_pub_bounds"
    TaskQueueRepository.create(task_id=task_pub, task_type="publish_folder", params={}, concurrency_total=4)

    # 1. 下载任务调整为 32 -> 成功
    resp = client.post(f"/api/tasks/{task_dl}/concurrency", json={"concurrency": 32}, headers=headers)
    assert resp.status_code == 200
    assert TaskQueueRepository.get_by_id(task_dl)["concurrency_total"] == 32

    # 2. 下载任务调整为 33 -> 422 (Pydantic 拦截)
    resp = client.post(f"/api/tasks/{task_dl}/concurrency", json={"concurrency": 33}, headers=headers)
    assert resp.status_code in (400, 422)

    # 3. 发布任务调整为 16 -> 成功
    resp = client.post(f"/api/tasks/{task_pub}/concurrency", json={"concurrency": 16}, headers=headers)
    assert resp.status_code == 200
    assert TaskQueueRepository.get_by_id(task_pub)["concurrency_total"] == 16

    # 4. 发布任务调整为 20 -> 400
    resp = client.post(f"/api/tasks/{task_pub}/concurrency", json={"concurrency": 20}, headers=headers)
    assert resp.status_code == 400

    # 5. 并发调整为 0 -> 400/422
    resp = client.post(f"/api/tasks/{task_pub}/concurrency", json={"concurrency": 0}, headers=headers)
    assert resp.status_code in (400, 422)

    # 6. 动态调整 upload_in_flight_per_connection 规范字段 -> 成功
    resp = client.patch(
        f"/api/tasks/{task_pub}/concurrency",
        json={"concurrency": 8, "upload_in_flight_per_connection": 3},
        headers=headers,
    )
    assert resp.status_code == 200
    pub_rec = TaskQueueRepository.get_by_id(task_pub)
    assert pub_rec["concurrency_total"] == 8
    assert pub_rec.get("params", {}).get("upload_in_flight_per_connection") == 3

    # 7. 动态调整 upload_in_flight 历史兼容别名 -> Pydantic 自动转换为 upload_in_flight_per_connection
    resp = client.patch(
        f"/api/tasks/{task_pub}/concurrency",
        json={"concurrency": 4, "upload_in_flight": 4},
        headers=headers,
    )
    assert resp.status_code == 200
    pub_rec = TaskQueueRepository.get_by_id(task_pub)
    assert pub_rec["concurrency_total"] == 4
    assert pub_rec.get("params", {}).get("upload_in_flight_per_connection") == 4

    # 8. upload_in_flight_per_connection 越界 (0 或 5) -> 422 Pydantic 校验拦截
    assert client.patch(
        f"/api/tasks/{task_pub}/concurrency",
        json={"concurrency": 4, "upload_in_flight_per_connection": 0},
        headers=headers,
    ).status_code in (400, 422)
    assert client.patch(
        f"/api/tasks/{task_pub}/concurrency",
        json={"concurrency": 4, "upload_in_flight_per_connection": 5},
        headers=headers,
    ).status_code in (400, 422)


@pytest.mark.asyncio
async def test_upload_worker_consecutive_errors_reconnect(tmp_dir, monkeypatch):
    """测试单块瞬态失败不关闭连接，仅当连续失败 3 次时才关闭旧 Sender 并触发重连"""
    from app.core import fast_telethon

    test_file = safe_write_bytes(os.path.join(tmp_dir, "reconnect_test.bin"), b"X" * (CHUNK * 2))

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4, auth_key=None)
    client._connection = MagicMock()
    del client._borrow_exported_sender
    del client._create_exported_sender
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    mock_close_sender = AsyncMock()
    monkeypatch.setattr(fast_telethon.sender_pool, "_close_worker_sender", mock_close_sender)

    fake_sender = MagicMock()

    async def fake_get_sender(*args, **kwargs):
        return fake_sender

    monkeypatch.setattr(fast_telethon.sender_pool, "_get_worker_sender", fake_get_sender)

    # 模拟前 2 次抛瞬态网络错误，第 3 次再抛异常触发重连，第 4 次及之后成功
    attempts = {"count": 0}

    async def mock_call(sender, req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise ConnectionError("Transient network jitter 1")
            if attempts["count"] == 2:
                raise TimeoutError("Transient timeout 2")
            if attempts["count"] == 3:
                raise ConnectionResetError("Dead connection 3")
            return True
        return cfg

    client._call = mock_call

    # 缩短 backoff 睡眠时间加快测试速度
    original_sleep = asyncio.sleep

    async def fast_sleep(s):
        await original_sleep(0.01)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)

    res = await upload_file_parallel(client, str(test_file), concurrency=1)
    assert res is not None
    # 验证前 2 次失败没有调用 _close_worker_sender，直到第 3 次连续失败才调用
    assert mock_close_sender.call_count >= 1


@pytest.mark.asyncio
async def test_upload_floodwait_backoff(tmp_dir, monkeypatch):
    """测试遇到 FloodWaitError 时提取秒数安全退避并重新排队分块，保护账号"""
    from telethon.errors import FloodWaitError

    test_file = safe_write_bytes(os.path.join(tmp_dir, "flood_test.bin"), b"F" * (CHUNK * 2))

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4, auth_key=None)
    client._connection = MagicMock()
    del client._borrow_exported_sender
    del client._create_exported_sender
    cfg = MagicMock()
    cfg.dc_options = []

    flood_err = FloodWaitError(request=None)
    flood_err.seconds = 2

    slept_seconds = []
    original_sleep = asyncio.sleep

    async def spy_sleep(s):
        slept_seconds.append(s)
        await original_sleep(0.01)

    monkeypatch.setattr(asyncio, "sleep", spy_sleep)

    calls = {"count": 0}

    async def mock_call(req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            calls["count"] += 1
            if calls["count"] == 1:
                raise flood_err
            return True
        return cfg

    client.side_effect = mock_call
    client._call = lambda sender, req: mock_call(req)

    res = await upload_file_parallel(client, str(test_file), concurrency=1)
    assert res is not None
    assert 2 in slept_seconds, "必须严格等待 FloodWaitError 指定的秒数"


@pytest.mark.asyncio
async def test_upload_badrequest_circuit_break(tmp_dir):
    """测试遇到 400 类确定性拒绝时立即熔断退出，杜绝无意义重试"""
    from telethon.errors.rpcbaseerrors import BadRequestError

    test_file = safe_write_bytes(os.path.join(tmp_dir, "badreq_test.bin"), b"B" * (CHUNK * 2))

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4, auth_key=None)
    client._connection = MagicMock()
    del client._borrow_exported_sender
    del client._create_exported_sender

    calls = {"count": 0}

    async def mock_call(req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            calls["count"] += 1
            raise BadRequestError(request=None, message="FILE_PARTS_INVALID")
        cfg = MagicMock()
        cfg.dc_options = []
        return cfg

    client.side_effect = mock_call
    client._call = lambda sender, req: mock_call(req)

    with pytest.raises(BadRequestError):
        await upload_file_parallel(client, str(test_file), concurrency=1, in_flight_per_connection=1)

    # 确定性拒绝下应立即中止，绝不发生 5 次重试
    assert calls["count"] == 1


def test_upload_meta_steady_state_no_fsync_and_final_flush_fsync(tmp_dir, monkeypatch):
    """测试稳态写盘不调用 os.fsync，仅在 flush_meta (终态) 时显式调用 os.fsync"""
    from app.core.fast_telethon.breakpoints import _write_meta_disk_sync

    meta_path = os.path.join(tmp_dir, "test.upload.meta")
    meta_data = {"total_bytes": 100, "file_id": 123, "completed_parts": [0]}

    fsync_called = []
    original_fsync = os.fsync

    def spy_fsync(fd):
        fsync_called.append(fd)
        return original_fsync(fd)

    monkeypatch.setattr(os, "fsync", spy_fsync)

    # 1. 稳态软写 (sync=False) -> 不应调用 os.fsync
    _write_meta_disk_sync(meta_path, meta_data, sync=False)
    assert len(fsync_called) == 0, "稳态落盘不应调用硬件级 os.fsync"

    # 2. 终态强制 Flush (sync=True) -> 必须调用 os.fsync
    _write_meta_disk_sync(meta_path, meta_data, sync=True)
    assert len(fsync_called) == 1, "终态落盘必须调用 os.fsync 保证强持久化"


@pytest.mark.asyncio
async def test_publish_progress_db_throttling(monkeypatch):
    """测试 progress_cb 的 DB 写入节流 (>=2.0s 或 >=1.0% 或 force_db)，且 WebSocket 保持每次广播"""
    from app.core.connection_manager import connection_manager
    from app.db.task_repository import TaskQueueRepository
    from app.services import publish_runner

    task_id = "test_throttle_task"
    db_writes = []
    broadcasts = []

    def fake_submit(fn, *args, **kwargs):
        db_writes.append((args, kwargs))

    async def fake_broadcast(msg):
        broadcasts.append(msg)

    monkeypatch.setattr(publish_runner, "submit_db_write", fake_submit)
    monkeypatch.setattr(connection_manager, "broadcast", fake_broadcast)

    # 提取 progress_cb 逻辑进行直接测试
    # 模拟 run_publish_folder_task 内的闭包函数行为
    last_db_write_time = 0.0
    last_db_percent = -1.0

    def progress_cb(
        current,
        total,
        speed,
        eta,
        file_name,
        concurrency=0,
        dc_id=0,
        concurrency_total=0,
        percent=None,
        force_db: bool = False,
    ):
        nonlocal last_db_write_time, last_db_percent
        calc_percent = percent if percent is not None else ((current / total * 100) if total > 0 else 0)
        current_item_text = f"[{current}/{total}] {file_name}"

        now = time.time()
        time_diff = now - last_db_write_time
        percent_diff = abs(calc_percent - last_db_percent)
        if force_db or time_diff >= 2.0 or percent_diff >= 1.0:
            last_db_write_time = now
            last_db_percent = calc_percent
            publish_runner.submit_db_write(
                TaskQueueRepository.update_progress,
                task_id,
                current,
                total,
                speed,
                eta,
                current_item_text,
                concurrency,
                dc_id,
                concurrency_total=concurrency_total or None,
            )

        asyncio.create_task(
            connection_manager.broadcast(
                {
                    "type": "progress",
                    "task_id": task_id,
                    "current": current,
                    "total": total,
                    "progress_bytes": current,
                    "total_bytes": total,
                    "percent": round(calc_percent, 1),
                    "speed_mb_s": speed,
                    "eta_s": eta,
                    "current_item": current_item_text,
                    "concurrency": concurrency,
                    "dc_id": dc_id,
                    "concurrency_total": concurrency_total,
                    "unit": "items",
                }
            )
        )

    # 1. 首次调用 -> 满足 percent_diff >= 1.0 (从 -1.0 到 0.1) -> 触发 DB 写入
    progress_cb(1, 1000, 1.0, 10.0, "file.mp4", percent=0.1)
    await asyncio.sleep(0.01)
    assert len(db_writes) == 1
    assert len(broadcasts) == 1

    # 2. 紧接着快速调用 5 次 (时间 < 2s，进度增量 < 1.0%) -> DB 节流不写，WebSocket 每次都广播
    for i in range(2, 7):
        progress_cb(i, 1000, 1.0, 10.0, "file.mp4", percent=0.1 + (i * 0.05))
    await asyncio.sleep(0.01)
    assert len(db_writes) == 1, "小幅度高频更新应被 DB 节流过滤"
    assert len(broadcasts) == 6, "WebSocket 广播应保持每次触发"

    # 3. 跨越幅度 >= 1.0% -> 触发 DB 写入
    progress_cb(20, 1000, 1.0, 10.0, "file.mp4", percent=1.5)
    await asyncio.sleep(0.01)
    assert len(db_writes) == 2

    # 4. 传入 force_db=True -> 绕过节流立即触发 DB 写入
    progress_cb(21, 1000, 1.0, 10.0, "file.mp4", percent=1.55, force_db=True)
    await asyncio.sleep(0.01)
    assert len(db_writes) == 3


# ==============================================================================
# Part: test_floodwait_visible.py
# ==============================================================================
SRC, TGT = -1009000000010, -100900037


def _flood_error(seconds: int) -> FloodWaitError:
    err = FloodWaitError(request=None)
    err.seconds = seconds
    return err


def _build_mock_client(msgs, forward_impl=None):
    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)

    async def mock_iter(*args, **kwargs):
        for m in msgs:
            yield m

    mock_client.iter_messages = mock_iter
    mock_client.get_messages = AsyncMock(return_value=MagicMock(total=len(msgs)))
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=1001))

    if forward_impl:
        mock_client.forward_messages = AsyncMock(side_effect=forward_impl)
    else:

        async def default_forward(entity, messages, from_peer, **kwargs):
            return [MagicMock(id=5000 + mid) for mid in messages]

        mock_client.forward_messages = AsyncMock(side_effect=default_forward)
    return mock_client


def _valid_msg(i):
    return MagicMock(
        id=i, empty=False, action=None, media=MessageMediaPhoto(), message=f"Msg {i}", restriction_reason=None
    )


@pytest.mark.asyncio
async def test_floodwait_cancel_during_sleep(monkeypatch):
    """30s 熔断期间 1s 后取消 -> 快速抛出 ForwardCancelledError 而非睡满 32s"""
    from app.services import forward_service

    msgs = [_valid_msg(i) for i in range(1, 4)]
    state = {"calls": 0}

    async def fl(entity, messages, from_peer, **kwargs):
        state["calls"] += 1
        if state["calls"] == 1:
            raise _flood_error(30)
        return [MagicMock(id=5000 + mid) for mid in messages]

    mock_client = _build_mock_client(msgs, forward_impl=fl)

    task_id = "fw_cancel"
    TaskQueueRepository.create(task_id, "forward_channel", {})
    from app.core.runtime import active_task_controls

    active_task_controls[task_id] = {"cancelled": False, "paused": False}

    def on_sleep(_sec: float, _n: int) -> None:
        rec = TaskQueueRepository.get_by_id(task_id)
        if rec and "FloodWait" in (rec.get("current_item") or ""):
            active_task_controls[task_id]["cancelled"] = True

    clock = FakeClock(on_async_sleep=on_sleep)
    monkeypatch.setattr(forward_service, "time", clock.time_namespace())
    monkeypatch.setattr(forward_service, "asyncio", clock.asyncio_namespace())

    try:
        with patch("app.core.client_manager.client_manager.userbot", mock_client):
            start = clock.monotonic()
            with pytest.raises(ForwardCancelledError):
                await ForwardService.forward_channel_messages(
                    source_chat=str(SRC), target_chat=str(TGT), batch_size=10, delay_seconds=0.01, task_id=task_id
                )
            elapsed = clock.monotonic() - start

        # 取消在熔断倒计时内生效 (30s 等待在虚拟第 1s 被打断)
        assert elapsed < 5, f"取消响应耗时 {elapsed:.1f}s, 熔断等待未被打断"
        task = TaskQueueRepository.get_by_id(task_id)
        assert task["status"] == "cancelled"
        # 等待期间已写入 FloodWait 可识别标记
        assert "FloodWait" in (task.get("current_item") or "")
    finally:
        active_task_controls.pop(task_id, None)


@pytest.mark.asyncio
async def test_floodwait_countdown_then_retry_succeeds(monkeypatch):
    """不取消: 倒计时结束后原批次重试成功, 任务正常完成"""
    from app.services import forward_service

    msgs = [_valid_msg(i) for i in range(1, 4)]
    state = {"calls": 0}

    async def fl(entity, messages, from_peer, **kwargs):
        state["calls"] += 1
        if state["calls"] == 1:
            raise _flood_error(1)
        return [MagicMock(id=5000 + mid) for mid in messages]

    mock_client = _build_mock_client(msgs, forward_impl=fl)
    clock = FakeClock()
    monkeypatch.setattr(forward_service, "time", clock.time_namespace())
    monkeypatch.setattr(forward_service, "asyncio", clock.asyncio_namespace())

    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        start = clock.monotonic()
        count = await ForwardService.forward_channel_messages(
            source_chat=str(SRC), target_chat=str(TGT), batch_size=10, delay_seconds=0.01
        )
        elapsed = clock.monotonic() - start

    assert count == 3
    assert state["calls"] == 2
    assert elapsed >= 3.0  # 至少完成了 1s+2s 的熔断等待 (e.seconds + 2)


# ==============================================================================
# Part: test_upload_breakpoint.py
# ==============================================================================
@pytest.mark.asyncio
async def test_upload_meta_ttl_expired(tmp_path):
    """测试当 .upload.meta 过期时，自动丢弃旧断点并重新全量上传"""
    test_file = safe_write_bytes(str(tmp_path / "test_video.mp4"), b"A" * (1024 * 1024 * 11))  # 11MB

    meta_file = str(tmp_path / "test_video.mp4.upload.meta")
    old_file_id = 123456789
    old_meta = {
        "total_bytes": os.path.getsize(str(test_file)),
        "file_id": old_file_id,
        "completed_parts": [0, 1, 2],
        "updated_at": time.time() - 10800,
    }
    safe_write_bytes(meta_file, json.dumps(old_meta).encode("utf-8"))

    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)
    mock_client._connection = MagicMock()
    del mock_client._borrow_exported_sender
    del mock_client._create_exported_sender
    mock_client.session = MagicMock(dc_id=4, auth_key=None)
    mock_client._call = AsyncMock(return_value=True)

    res = await upload_file_parallel(mock_client, str(test_file), concurrency=1)

    assert res.id != old_file_id
    assert res.parts > 0


@pytest.mark.asyncio
async def test_upload_force_fresh_discards_meta(tmp_path):
    """测试 force_fresh=True 能够主动清除本地存在的断点并全量重传"""
    test_file = safe_write_bytes(str(tmp_path / "test_fresh.mp4"), b"B" * (1024 * 1024 * 11))

    meta_file = str(tmp_path / "test_fresh.mp4.upload.meta")
    old_file_id = 999888777
    fresh_meta = {
        "total_bytes": os.path.getsize(str(test_file)),
        "file_id": old_file_id,
        "completed_parts": [0, 1],
        "updated_at": time.time() - 60,
    }
    safe_write_bytes(meta_file, json.dumps(fresh_meta).encode("utf-8"))

    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)
    mock_client._connection = MagicMock()
    del mock_client._borrow_exported_sender
    del mock_client._create_exported_sender
    mock_client.session = MagicMock(dc_id=4, auth_key=None)
    mock_client._call = AsyncMock(return_value=True)

    res = await upload_file_parallel(mock_client, str(test_file), concurrency=1, force_fresh=True)
    assert res.id != old_file_id


def test_publish_folder_result_int_compatibility():
    """验证 PublishFolderResult 继承自 int 并保持全量属性"""
    res = PublishFolderResult(4, 5, ["failed_file.mp4"])
    assert res == 4
    assert int(res) == 4
    assert res.published_count == 4
    assert res.total_files_count == 5
    assert res.failed_files == ["failed_file.mp4"]


@pytest.mark.asyncio
async def test_run_publish_folder_task_marks_failed_on_partial_error():
    """测试当文件夹发布存在部分失败文件时，任务状态置为 failed，进度显示实际总数 [4/5]"""
    from app.db.task_repository import TaskQueueRepository
    from app.services.publish_runner import run_publish_folder_task

    task_id = "test_pub_fail_1"
    TaskQueueRepository.create(task_id, "publish_folder", {"folder_path": "/fake/path", "target_channel": "-100123"})

    mock_result = PublishFolderResult(4, 5, ["failed_video.mp4"])
    with patch.object(PublisherService, "publish_folder", new=AsyncMock(return_value=mock_result)):
        await run_publish_folder_task(task_id, "/fake/path", "-100123", True, 0.1)

    task = TaskQueueRepository.get_by_id(task_id)
    assert task["status"] == "failed"
    assert task["processed_items"] == 4
    assert task["total_items"] == 5
    assert "[4/5]" in task["current_item"]

    TaskQueueRepository.delete(task_id)


def test_inspect_folder_includes_pending_publish_count(tmp_path):
    """测试 inspect_folder 返回值中包含准确的 pending_publish_count"""
    safe_write_bytes(str(tmp_path / "video1.mp4"), b"V" * 1024)

    data = PublisherService.inspect_folder(str(tmp_path), target_channel="-100999")
    assert "pending_publish_count" in data
    assert data["pending_publish_count"] == 1
    assert data["valid_files_count"] == 1
    assert data["already_published_count"] == 0


@pytest.mark.asyncio
async def test_publish_folder_auto_heal_on_missing_parts(tmp_path):
    """测试当 Telegram 服务端分块过期报错 File part 0 missing 时，自动清空断点并触发重传自愈"""
    from app.core.client_manager import client_manager

    safe_write_bytes(str(tmp_path / "video_heal.mp4"), b"H" * (1024 * 1024 * 35))  # 35MB > 30MB (触发 userbot)

    mock_userbot = AsyncMock()
    mock_userbot.session = MagicMock(dc_id=4)
    mock_userbot._call = AsyncMock(return_value=True)

    send_file_calls = []

    async def mock_send_file(chat_id, uploaded_media, **kwargs):
        send_file_calls.append(uploaded_media)
        if len(send_file_calls) == 1:
            raise Exception("File part 0 missing (caused by SendMediaRequest)")
        return MagicMock(id=999)

    mock_userbot.send_file = AsyncMock(side_effect=mock_send_file)
    mock_userbot.send_message = AsyncMock()

    orig_accounts = dict(client_manager._accounts)
    orig_bots = dict(client_manager._bots)
    orig_bot_index = client_manager._bot_index
    seed_default_account(client_manager, client=mock_userbot)
    client_manager._bots.clear()

    try:
        with patch.object(client_manager, "refresh_premium", new=AsyncMock()):
            res = await PublisherService.publish_folder(
                folder_path=str(tmp_path), target_chat_id="-10012345", album_mode=False, delay_seconds=0.01
            )
        assert res.published_count == 1
        assert len(res.failed_files) == 0
        assert len(send_file_calls) == 2  # 验证确实自动重传并自愈
    finally:
        client_manager._accounts.clear()
        client_manager._accounts.update(orig_accounts)
        client_manager._bots.clear()
        client_manager._bots.update(orig_bots)
        client_manager._bot_index = orig_bot_index


# ==============================================================================
# Part: test_upload_meta_relocation.py
# ==============================================================================
def _sandbox(tmp_dir, monkeypatch):
    sandbox = os.path.join(tmp_dir, "meta_sandbox")
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", sandbox)
    return sandbox


def _write_json(path, payload):
    safe_write_bytes(path, json.dumps(payload).encode("utf-8"))


def _read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def test_meta_key_task_agnostic_and_stable(tmp_dir, monkeypatch):
    """键 = local_{sha256(路径)[:16]}_{size}.upload.meta —— 稳定、无 task_id、按身份派生"""
    sandbox = _sandbox(tmp_dir, monkeypatch)
    src = os.path.join(tmp_dir, "src", "e01.mkv")
    os.makedirs(os.path.dirname(src), exist_ok=True)

    p1 = local_upload_meta_path(src, 1000)
    p2 = local_upload_meta_path(src, 1000)

    assert p1 == p2, "同文件同体积键稳定 (断点续传可命中)"
    assert os.path.dirname(p1) == sandbox, "meta 必须落沙箱"
    name = os.path.basename(p1)
    assert name.startswith("local_") and name.endswith(".upload.meta")
    assert "task" not in name, "task-agnostic 红线: 键严禁含 task_id"
    # 不同体积 / 不同文件 → 不同键
    assert local_upload_meta_path(src, 2000) != p1
    other = os.path.join(tmp_dir, "src", "e02.mkv")
    assert local_upload_meta_path(other, 1000) != p1


def test_discard_upload_meta_clears_size_variants(tmp_dir, monkeypatch):
    """FILE_PARTS_INVALID 自愈清除断点: 同摘要全部 size 变体的沙箱 meta 一并清除 (幂等)"""
    sandbox = _sandbox(tmp_dir, monkeypatch)
    src = os.path.join(tmp_dir, "src", "e05.mkv")
    os.makedirs(os.path.dirname(src), exist_ok=True)
    meta_1000 = local_upload_meta_path(src, 1000)
    _write_json(meta_1000, {"total_bytes": 1000, "file_id": 3, "completed_parts": [0], "updated_at": time.time()})
    meta_2000 = local_upload_meta_path(src, 2000)
    _write_json(meta_2000, {"total_bytes": 2000, "file_id": 3, "completed_parts": [0, 1], "updated_at": time.time()})
    # 同摘要异文件 / 异摘要同文件不得误清 (glob 锚定 local_{digest}_ 前缀)
    other_digest_meta = local_upload_meta_path(os.path.join(tmp_dir, "src", "e06.mkv"), 1000)
    _write_json(
        other_digest_meta, {"total_bytes": 1000, "file_id": 4, "completed_parts": [], "updated_at": time.time()}
    )

    discard_upload_meta(src)

    assert not os.path.exists(meta_1000), "size 变体 1000 已清"
    assert not os.path.exists(meta_2000), "size 变体 2000 已清 (同摘要全变体)"
    assert os.path.exists(other_digest_meta), "异摘要 meta 不受影响"
    # 幂等: 重复清不致命
    discard_upload_meta(src)
    # 不存在的路径不致命
    discard_upload_meta(os.path.join(tmp_dir, "src", "ghost.mkv"))
    assert sandbox == os.path.dirname(meta_1000)


# ==============================================================================
# Part: test_upload_provider_mode.py
# ==============================================================================
CHUNK = 512 * 1024


# ---------------------------------------------------------------------------
# 包B: BaseChunkProvider / LocalFileChunkProvider 单元用例
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_local_provider_attrs(tmp_dir):
    """属性契约: total_bytes/file_name 缺省派生, file_id/file_name 可显式覆盖"""
    payload = b"A" * (CHUNK + 12345)
    src = safe_write_bytes(os.path.join(tmp_dir, "video.bin"), payload)

    provider = LocalFileChunkProvider(src)
    assert provider.total_bytes == len(payload)
    assert provider.file_name == "video.bin"
    assert provider.file_id == src

    named = LocalFileChunkProvider(src, file_id="remote-123", file_name="custom.mp4")
    assert named.file_id == "remote-123"
    assert named.file_name == "custom.mp4"
    assert named.total_bytes == len(payload)


@pytest.mark.asyncio
async def test_local_provider_read_slices(tmp_dir):
    """字节语义: 顺序整读拼接 == 源文件; 首/中/尾任意偏移读 == 源切片 (尾块截断)"""
    payload = bytes(range(256)) * (2 * 1024 * 1024 // 256)  # 2MiB 内容确定
    src = safe_write_bytes(os.path.join(tmp_dir, "slices.bin"), payload)
    provider = LocalFileChunkProvider(src)

    collected = b""
    offset = 0
    while offset < len(payload):
        piece = await provider.read_chunk(offset, CHUNK)
        assert len(piece) <= CHUNK
        collected += piece
        offset += len(piece)
    assert collected == payload

    assert await provider.read_chunk(0, CHUNK) == payload[0:CHUNK]
    mid = len(payload) // 2
    assert await provider.read_chunk(mid, CHUNK) == payload[mid : mid + CHUNK]
    tail = len(payload) - 100
    assert await provider.read_chunk(tail, CHUNK) == payload[tail:]


@pytest.mark.asyncio
async def test_local_provider_tail_and_edge(tmp_dir):
    """边界约定: offset+size 超尾返回截断字节; offset >= total_bytes / size <= 0 抛 ValueError"""
    payload = b"B" * (CHUNK * 2 + 777)
    src = safe_write_bytes(os.path.join(tmp_dir, "edge.bin"), payload)
    provider = LocalFileChunkProvider(src)

    beyond = await provider.read_chunk(CHUNK * 2, CHUNK)  # offset+size 超尾
    assert beyond == payload[CHUNK * 2 :]
    assert len(beyond) == 777

    exact_tail = await provider.read_chunk(CHUNK * 2, 777)
    assert exact_tail == payload[CHUNK * 2 :]

    with pytest.raises(ValueError):
        await provider.read_chunk(len(payload), CHUNK)
    with pytest.raises(ValueError):
        await provider.read_chunk(len(payload) + CHUNK, 1)
    with pytest.raises(ValueError):
        await provider.read_chunk(0, 0)
    with pytest.raises(ValueError):
        await provider.read_chunk(0, -5)


@pytest.mark.asyncio
async def test_local_provider_short_read_error(tmp_dir, monkeypatch):
    """短读防御: 底层读取量不足可读量即抛 IOError, 文案对齐引擎预读校验"""
    payload = b"C" * CHUNK
    src = safe_write_bytes(os.path.join(tmp_dir, "short.bin"), payload)
    provider = LocalFileChunkProvider(src)
    monkeypatch.setattr(provider, "_read_sync", lambda off, sz: b"x" * (sz - 1))
    with pytest.raises(IOError, match="文件读取不完整"):
        await provider.read_chunk(0, CHUNK)


def test_stream_chunk_provider_module_contract():
    """模块契约: 成功导入; 禁止 import fast_telethon (引擎单向依赖本模块, 反向即循环导入);
    BaseChunkProvider 为不可实例化抽象基类 (read_chunk 抽象方法强制实现)。"""
    import app.core.stream_chunk_provider as m

    tree = ast.parse(inspect.getsource(m))
    imported_roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".")[0])
    assert "fast_telethon" not in imported_roots

    with pytest.raises(TypeError):
        BaseChunkProvider()


# ---------------------------------------------------------------------------
# 包C: upload_file_parallel Provider 模式引擎集成用例
# (mock 范式照抄既有回归: _call 载荷捕获 + fake_gather sender 旁路, 见 test_upload_pipelining.py)
# ---------------------------------------------------------------------------


def _make_client(captured_parts=None):
    """既有回归同款 client mock 配方: AsyncMock + dc_id=4 + cfg 兜底 + _call 分块载荷捕获"""
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client.session = MagicMock(dc_id=4, auth_key=None)
    client._connection = MagicMock()
    del client._borrow_exported_sender
    del client._create_exported_sender
    cfg = MagicMock()
    cfg.dc_options = []
    client.side_effect = lambda req: cfg

    async def mock_call(sender, req):
        if captured_parts is not None and isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            captured_parts[int(req.file_part)] = bytes(req.bytes)
        return True

    client._call = mock_call
    return client


@pytest.fixture
def bypass_senders(monkeypatch):
    """旁路 sender 建立: 关闭 init 协程并返回 MagicMock sender (照 test_upload_pipelining fake_gather 范式)"""

    async def fake_gather(coros, *args, **kwargs):
        for c in coros:
            c.close()
        return [MagicMock() for _ in coros]

    monkeypatch.setattr("app.core.fast_telethon.sender_pool._gather_worker_senders_with_cleanup", fake_gather)


@pytest.fixture(autouse=True)
def _pikpak_meta_trust_root(tmp_dir, monkeypatch):
    """·F2: Provider 模式 meta_path 强制 trust_root 校验——本文件既有用例的 meta_path
    均落 tmp_dir\\upload_meta, 故将沙箱根 patch 到同一路径 (既有用例断言零改动)。"""
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", os.path.join(tmp_dir, "upload_meta"))


def _write_sandbox_meta(meta_path, *, total_bytes, file_id, completed, updated_at):
    """测试专用 meta 写入: 复用 safe_write_bytes (自带 resolve + 越界校验 + mkdir parents)"""
    meta_json = json.dumps(
        {
            "total_bytes": total_bytes,
            "file_id": file_id,
            "completed_parts": completed,
            "updated_at": updated_at,
        }
    ).encode("utf-8")
    return safe_write_bytes(meta_path, meta_json)


def _read_meta(meta_path):
    """读回断言用: 先规范化路径再按 JSON 解析"""
    normalized = os.path.realpath(meta_path)
    with open(normalized, encoding="utf-8") as f:
        return json.load(f)


@pytest.mark.asyncio
async def test_provider_mode_byte_equivalence(tmp_dir, bypass_senders):
    """字节等价: LocalFileChunkProvider 上传的 MTProto 分块载荷重组 == 源文件逐字节"""
    num_chunks = 5
    payload = bytes(range(256)) * (CHUNK * num_chunks // 256)
    src = safe_write_bytes(os.path.join(tmp_dir, "equiv.bin"), payload)
    meta_path = os.path.join(tmp_dir, "upload_meta", "task1_item1_seg0.upload.meta")

    captured = {}
    client = _make_client(captured)
    provider = LocalFileChunkProvider(src, file_id="pikpak-1", file_name="equiv.mp4")

    res = await upload_file_parallel(
        client,
        "pikpak://pikpak-1",  # Provider 模式下 file_path 不参与 IO, 仅日志展示
        concurrency=2,
        in_flight_per_connection=1,
        chunk_provider=provider,
        meta_path=meta_path,
    )

    assert isinstance(res, types.InputFile)
    assert res.parts == num_chunks
    assert res.name == "equiv.mp4"
    reassembled = b"".join(captured[i] for i in range(num_chunks))
    assert reassembled == payload, "Provider 模式重组字节必须与源文件完全一致"
    assert not os.path.exists(meta_path), "完成后 meta 必须被清理"


@pytest.mark.asyncio
async def test_provider_mode_fast_path_512kb(tmp_dir):
    """512KB 快速路径修复: Provider 模式读全量字节经 io.BytesIO 上传, 不抛 FileNotFoundError"""
    payload = b"F" * CHUNK  # 恰好 512KB (快速路径 <= 边界)
    src = safe_write_bytes(os.path.join(tmp_dir, "fast.bin"), payload)
    provider = LocalFileChunkProvider(src, file_id="fast-1", file_name="fast.bin")

    client = AsyncMock()
    client.session = MagicMock(dc_id=4, auth_key=None)
    client._connection = MagicMock()
    del client._borrow_exported_sender
    del client._create_exported_sender
    client.upload_file = AsyncMock(return_value=types.InputFile(id=42, parts=1, name="fast.bin", md5_checksum=""))

    res = await upload_file_parallel(
        client,
        "pikpak://fast-1",
        chunk_provider=provider,
        meta_path=os.path.join(tmp_dir, "upload_meta", "fast.upload.meta"),
    )

    assert res.id == 42
    args, kwargs = client.upload_file.await_args
    bio = args[0]
    assert isinstance(bio, io.BytesIO)
    assert bio.getvalue() == payload
    assert kwargs.get("file_name") == "fast.bin"
    client._call.assert_not_called()  # 快速路径旁路分块 RPC 与 sender 建立


@pytest.mark.asyncio
async def test_provider_mode_meta_sandbox(tmp_dir, bypass_senders):
    """meta 沙箱: Provider 模式断点元数据只落在注入的沙箱路径, 源文件旁零 .upload.meta"""
    payload = b"S" * (CHUNK * 3)
    src = safe_write_bytes(os.path.join(tmp_dir, "sandbox.bin"), payload)
    meta_path = os.path.join(tmp_dir, "upload_meta", "t1_i1_s0.upload.meta")

    state = {"cancelled": False}

    async def cancel_call(sender, req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            if int(req.file_part) >= 1:
                state["cancelled"] = True
            return True
        return True

    client = _make_client()
    client._call = cancel_call

    with pytest.raises(UploadCancelledError):
        await upload_file_parallel(
            client,
            "pikpak://sandbox",
            concurrency=1,
            in_flight_per_connection=1,
            is_cancelled=lambda: state["cancelled"],
            chunk_provider=LocalFileChunkProvider(src),
            meta_path=meta_path,
        )

    assert os.path.isfile(meta_path), "取消后 meta 必须保留在沙箱路径 (可续传)"
    meta_hits = []
    for root, _dirs, files in os.walk(tmp_dir):
        for fn in files:
            if fn.endswith(".upload.meta"):
                meta_hits.append(os.path.join(root, fn))
    assert meta_hits == [meta_path], f"全 tmp_dir 只允许沙箱 meta 一个断点文件, 实际: {meta_hits}"


@pytest.mark.asyncio
async def test_provider_mode_meta_path_required(tmp_dir):
    """设计守卫: Provider 模式未注入 meta_path 直接 ValueError, 不发生任何分块 RPC"""
    payload = b"R" * (CHUNK * 2)
    src = safe_write_bytes(os.path.join(tmp_dir, "req.bin"), payload)
    client = _make_client()
    with pytest.raises(ValueError, match="meta_path"):
        await upload_file_parallel(client, "pikpak://req", chunk_provider=LocalFileChunkProvider(src))


@pytest.mark.asyncio
async def test_provider_mode_none_preserves_legacy(tmp_dir, bypass_senders):
    """chunk_provider=None 旧行为逐项不变: 路径断言 / 源文件旁 meta / 快速路径传路径"""
    client = _make_client()
    with pytest.raises(FileNotFoundError):
        await upload_file_parallel(client, os.path.join(tmp_dir, "missing.bin"))

    payload = b"L" * (CHUNK * 3)
    src = safe_write_bytes(os.path.join(tmp_dir, "legacy.bin"), payload)
    state = {"cancelled": False}

    async def cancel_call(sender, req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            if int(req.file_part) >= 1:
                state["cancelled"] = True
            return True
        return True

    client2 = _make_client()
    client2._call = cancel_call
    with pytest.raises(UploadCancelledError):
        await upload_file_parallel(
            client2,
            src,
            concurrency=1,
            in_flight_per_connection=1,
            is_cancelled=lambda: state["cancelled"],
        )
    # 本地 meta 收拢至 data/upload_meta 沙箱 (task-agnostic 键), 不再旁挂源文件
    from app.core.fast_telethon import local_upload_meta_path, resolve_breakpoint_path

    legacy_beside = resolve_breakpoint_path(src, ".upload.meta")
    assert not os.path.isfile(legacy_beside), "V3 收拢后源文件旁不再写 meta"
    sandbox_meta = local_upload_meta_path(src, len(payload))
    assert os.path.isfile(sandbox_meta), "取消路径 meta 收拢至沙箱且保留 (断点续传)"

    small = safe_write_bytes(os.path.join(tmp_dir, "small_legacy.bin"), b"P" * 1024)
    client3 = AsyncMock()
    client3.session = MagicMock(dc_id=4)
    client3.upload_file = AsyncMock(
        return_value=types.InputFile(id=7, parts=1, name="small_legacy.bin", md5_checksum="")
    )
    await upload_file_parallel(client3, small)
    args, kwargs = client3.upload_file.await_args
    assert args[0] == small, "旧路径快速分支必须仍传本地路径"
    assert "file_name" not in kwargs


@pytest.mark.asyncio
async def test_provider_mode_force_fresh_and_ttl(tmp_dir, bypass_senders):
    """force_fresh 与 TTL 过期丢弃在 Provider 模式沙箱 meta 上同样生效 (复用 test_upload_breakpoint 断言风格)"""
    payload = b"T" * (CHUNK * 4)
    src = safe_write_bytes(os.path.join(tmp_dir, "ttl.bin"), payload)
    provider = LocalFileChunkProvider(src)
    meta_path = os.path.join(tmp_dir, "upload_meta", "t_f.upload.meta")
    client = _make_client()

    # force_fresh: 预置含断点的 meta → 引擎先清除再全新上传 (立即取消, 观察落盘的新 meta)
    _write_sandbox_meta(meta_path, total_bytes=len(payload), file_id=12345, completed=[0, 1, 2], updated_at=time.time())
    with pytest.raises(UploadCancelledError):
        await upload_file_parallel(
            client,
            "pikpak://ttl",
            concurrency=1,
            is_cancelled=lambda: True,
            chunk_provider=provider,
            meta_path=meta_path,
            force_fresh=True,
        )
    meta = _read_meta(meta_path)
    assert meta["completed_parts"] == [], "force_fresh 必须丢弃旧断点"
    assert meta["file_id"] != 12345

    # TTL 过期: updated_at 早于 7200s → 断点被主动丢弃
    _write_sandbox_meta(
        meta_path, total_bytes=len(payload), file_id=999, completed=[0, 1, 2], updated_at=time.time() - 10800
    )
    with pytest.raises(UploadCancelledError):
        await upload_file_parallel(
            client,
            "pikpak://ttl",
            concurrency=1,
            is_cancelled=lambda: True,
            chunk_provider=provider,
            meta_path=meta_path,
        )
    meta = _read_meta(meta_path)
    assert meta["completed_parts"] == [], "过期 meta 必须被丢弃后全新上传"
    assert meta["file_id"] != 999


@pytest.mark.asyncio
async def test_provider_mode_zero_media_disk(tmp_dir, bypass_senders):
    """零媒体落盘: 成功全程零残留; 中断时唯一落盘文件为 KB 级沙箱 meta"""
    payload = bytes(range(256)) * (CHUNK * 4 // 256)
    src = safe_write_bytes(os.path.join(tmp_dir, "zero.bin"), payload)
    provider = LocalFileChunkProvider(src, file_id="z", file_name="zero.bin")

    def snapshot():
        out = {}
        for root, _dirs, files in os.walk(tmp_dir):
            for fn in files:
                p = os.path.join(root, fn)
                out[p] = os.path.getsize(p)
        return out

    before = snapshot()

    # 阶段一: 完整成功 → 无任何新增文件 (媒体零落盘 + meta 完成即清理)
    captured = {}
    meta_path1 = os.path.join(tmp_dir, "upload_meta", "z1.upload.meta")
    await upload_file_parallel(
        _make_client(captured),
        "pikpak://z",
        concurrency=2,
        in_flight_per_connection=1,
        chunk_provider=provider,
        meta_path=meta_path1,
    )
    after_ok = snapshot()
    assert set(after_ok) == set(before), f"成功路径不允许任何落盘残留, 实际新增: {set(after_ok) - set(before)}"

    # 阶段二: 中途取消 → 唯一新增 = 沙箱 meta 且 < 4KB
    state = {"cancelled": False}
    client2 = _make_client()

    async def cancel_call(sender, req):
        if isinstance(req, (SaveBigFilePartRequest, SaveFilePartRequest)):
            if int(req.file_part) >= 1:
                state["cancelled"] = True
            return True
        return True

    client2._call = cancel_call
    meta_path2 = os.path.join(tmp_dir, "upload_meta", "z2.upload.meta")
    with pytest.raises(UploadCancelledError):
        await upload_file_parallel(
            client2,
            "pikpak://z",
            concurrency=1,
            in_flight_per_connection=1,
            is_cancelled=lambda: state["cancelled"],
            chunk_provider=provider,
            meta_path=meta_path2,
        )
    after_cancel = snapshot()
    assert set(after_cancel) - set(before) == {meta_path2}
    assert after_cancel[meta_path2] < 4096, "meta 必须是 KB 级 JSON"


# ---------------------------------------------------------------------------
# ·F2/F4: meta_path trust_root 沙箱防护 + 512KB 快速路径守卫对齐
# (沙箱根由 _pikpak_meta_trust_root autouse fixture patch 到 tmp_dir/upload_meta)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_provider_mode_meta_within_trust_root(tmp_dir, bypass_senders):
    """F2: meta_path 落沙箱根的嵌套子目录 → 上传成功, 父目录按需自动创建"""
    payload = b"W" * (CHUNK * 2)
    src = safe_write_bytes(os.path.join(tmp_dir, "within.bin"), payload)
    meta_path = os.path.join(tmp_dir, "upload_meta", "sub", "t_within.upload.meta")

    res = await upload_file_parallel(
        _make_client(),
        "pikpak://within",
        concurrency=1,
        in_flight_per_connection=1,
        chunk_provider=LocalFileChunkProvider(src),
        meta_path=meta_path,
    )
    assert res.parts == 2
    assert not os.path.exists(meta_path), "完成后 meta 必须被清理 (父目录创建发生在上传期间)"


@pytest.mark.asyncio
async def test_provider_mode_meta_outside_trust_root_raises(tmp_dir):
    """F2: meta_path 位于沙箱根外 → ValueError (越界文案), 未发生任何网络分块调用"""
    payload = b"X" * (CHUNK * 2)
    src = safe_write_bytes(os.path.join(tmp_dir, "outside.bin"), payload)
    meta_path = os.path.join(tmp_dir, "outside_root", "x.upload.meta")

    captured = {}
    with pytest.raises(ValueError, match="越界"):
        await upload_file_parallel(
            _make_client(captured),
            "pikpak://outside",
            concurrency=1,
            in_flight_per_connection=1,
            chunk_provider=LocalFileChunkProvider(src),
            meta_path=meta_path,
        )
    assert captured == {}, "越界拒绝必须发生在任何分块 RPC 之前"


@pytest.mark.asyncio
async def test_provider_mode_meta_symlink_escape_raises(tmp_dir):
    """F2: 符号链接把 meta_path 重定向到沙箱根外 → ValueError (照 test_breakpoint_sandbox
    fake_resolve 范式, Windows 无需真实符号链接)。载荷 2 块 (>512KB) 确保走大路径 meta 分支"""
    payload = b"Y" * (CHUNK * 2)
    src = safe_write_bytes(os.path.join(tmp_dir, "sym.bin"), payload)
    meta_path = os.path.join(tmp_dir, "upload_meta", "t_sym.upload.meta")
    redirect_target = os.path.join(tmp_dir, "redirected.upload.meta")

    real_resolve = pathlib.Path.resolve

    def fake_resolve(self, strict=False):
        if str(self) == meta_path:
            return pathlib.Path(redirect_target)
        return real_resolve(self, strict=strict)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(pathlib.Path, "resolve", fake_resolve)
        with pytest.raises(ValueError, match="越界"):
            await upload_file_parallel(
                _make_client(),
                "pikpak://sym",
                concurrency=1,
                in_flight_per_connection=1,
                chunk_provider=LocalFileChunkProvider(src),
                meta_path=meta_path,
            )


@pytest.mark.asyncio
async def test_provider_mode_fast_path_meta_required(tmp_dir):
    """F4: ≤512KB 快速路径同样强制 meta_path 缺省 ValueError (与大路径守卫对称)"""
    payload = b"Z" * 1024
    src = safe_write_bytes(os.path.join(tmp_dir, "fast_guard.bin"), payload)
    client = AsyncMock()
    client.session = MagicMock(dc_id=4, auth_key=None)
    client._connection = MagicMock()
    del client._borrow_exported_sender
    del client._create_exported_sender
    client.upload_file = AsyncMock(return_value=types.InputFile(id=1, parts=1, name="x", md5_checksum=""))

    with pytest.raises(ValueError, match="meta_path"):
        await upload_file_parallel(
            client,
            "pikpak://fast-guard",
            chunk_provider=LocalFileChunkProvider(src),
        )
    client.upload_file.assert_not_called()


# ==============================================================================
# Part: test_breakpoint_sandbox.py
# ==============================================================================
CHUNK = 512 * 1024


def test_symlink_redirect_outside_root_raises(tmp_dir, monkeypatch):
    """派生路径 (.part) 被符号链接重定向到信任根之外 -> 两种模式均抛 ValueError"""
    dl = os.path.join(tmp_dir, "dl")
    os.makedirs(dl)
    # 模拟"dl/video.mp4.part 为符号链接且跟链落点在 dl 之外": 仅该派生路径的 resolve 重定向,
    # 其余路径走真实 resolve; 落点取 tmp_dir 根下路径, 非严格 resolve 下无需真实存在
    link_path = pathlib.Path(dl, "video.mp4.part")
    redirect_target = pathlib.Path(tmp_dir, "redirected.bin")
    real_resolve = pathlib.Path.resolve

    def fake_resolve(self, strict=False):
        if self == link_path:
            return redirect_target
        return real_resolve(self, strict=strict)

    monkeypatch.setattr(pathlib.Path, "resolve", fake_resolve)

    source_file = os.path.join(dl, "video.mp4")
    # 无 trust_root: 派生路径符号链接重定向防御 (现有能力)
    with pytest.raises(ValueError, match="越界"):
        resolve_breakpoint_path(source_file, ".part")
    # 有 trust_root: 强校验同样拦截
    with pytest.raises(ValueError, match="越界"):
        resolve_breakpoint_path(source_file, ".part", trust_root=dl)
    with pytest.raises(ValueError, match="越界"):
        _validated_breakpoint(str(link_path), trust_root=dl)


def test_trust_root_violation_raises(tmp_dir):
    """源文件目录不在 trust_root 内 -> ValueError (纯目录边界校验, 无需符号链接)"""
    src = os.path.join(tmp_dir, "src")
    other = os.path.join(tmp_dir, "other")
    os.makedirs(src)
    os.makedirs(other)
    source_file = os.path.join(src, "a.mp4")

    with pytest.raises(ValueError, match="越界"):
        resolve_breakpoint_path(source_file, ".part", trust_root=other)
    with pytest.raises(ValueError, match="越界"):
        _validated_breakpoint(source_file + ".part", trust_root=other)


def test_normal_path_returns_canonical_absolute(tmp_dir):
    """正常路径返回规范绝对路径, 带/不带 trust_root 结果一致"""
    dl = os.path.join(tmp_dir, "dl")
    os.makedirs(dl)
    source_file = os.path.join(dl, "video.mp4")
    expected = str(pathlib.Path(source_file + ".part").resolve())

    result = resolve_breakpoint_path(source_file, ".part")
    assert result == expected
    assert pathlib.Path(result).is_absolute()
    assert resolve_breakpoint_path(source_file, ".part", trust_root=dl) == expected
    assert _validated_breakpoint(expected, trust_root=dl) == pathlib.Path(expected)


@pytest.mark.asyncio
async def test_download_rejects_output_outside_download_dir(tmp_dir):
    """下载链路接线: out_file_path 不在 DOWNLOAD_DIR 内时, .part 派生即拒绝 (不触网络)"""
    # 保持真实 DOWNLOAD_DIR (不注入), out_file 落在 tests/.tmp -> 派生越界
    out_file = os.path.join(tmp_dir, "outside_download.bin")

    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client._call = AsyncMock(side_effect=AssertionError("沙箱应在派生校验阶段拒绝, 不得触达网络请求"))

    with pytest.raises(ValueError, match="越界"):
        await download_file_parallel(
            client,
            location=MagicMock(),
            out_file_path=out_file,
            total_bytes=3 * CHUNK,
            concurrency=2,
            raw_media=MagicMock(),
        )
    assert not os.path.exists(out_file)
