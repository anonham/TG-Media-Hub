"""Core Runtime: 跨横切面基础架构、日志脱敏、指标度量、存储治理与系统分层守卫测试 (Runtime Infrastructure & Governance)

覆盖范围:
- 原 test_log_setup / test_log_perf / test_failure_logger: 日志架构、环形缓冲、10万行性能基线与失败日志落盘
- 原 test_security_redact: 敏感凭证过滤脱敏、URL 签名脱敏与递归载荷脱敏
- 原 test_metrics: Prometheus / Influx / 运行时指标采集、单调递增与重置隔离
- 原 test_layering: 架构分层守卫 (core 不得逆向依赖 api/services)
- 原 test_config_validation: Settings 环境变量延迟凭证校验与默认值回退
- 原 test_natural_sort: 自然语义排序算法 (Human/Natural Sort)
- 原 test_intervals: 区间集合代数、gap 计算与连续 run 聚合
- 原 test_file_hash: 流式哈希计算与去重校验
- 原 test_path_normalization: 跨平台路径归一化规范
- 原 test_run_uvicorn_kwargs: uvicorn 平台自适应启动配置 (Windows vs Linux uvloop)
- 原 test_storage: 本地存储扫描登记、三重删除守卫与孤儿断点清理"""

import ast
import asyncio
import concurrent.futures
import hashlib
import json
import logging
import os
import pathlib
import subprocess
import sys
import time
from logging.handlers import QueueHandler
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from _safeio import safe_write_bytes
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.api.auth import issue_2fa_token
from app.config import _load_dotenv, secure_file_permissions, settings
from app.core.failure_logger import FailureLogger
from app.core.hash_utils import sha256_of_file
from app.core.intervals import compute_gaps, merge_intervals, runs_from_ids
from app.core.log_setup import configure_logging, ring_buffer_handler
from app.core.metrics import (
    active_ws_connections,
    chunk_dropped_total,
    chunk_retry_total,
    download_bytes_total,
    download_threads_in_use,
    flood_locked_accounts,
    floodwait_total,
    queued_task_depth,
    running_tasks,
    task_retry_total,
    task_status_transitions_total,
)
from app.core.pipeline.utils import natural_sort_key
from app.core.redact import RedactLogFilter, redact_payload, redact_text
from app.core.runtime import active_download_paths
from app.core.single_instance import SingleInstanceLock, acquire_process_singleton_lock
from app.db.database import get_db
from app.db.download_repository import DownloadBreakpointRepository, DownloadHistoryRepository
from app.db.preset_repository import PresetRepository
from app.db.publish_repository import PlaceholderSlotRepository, PublishHistoryRepository, norm_file_path
from app.db.task_repository import TaskQueueRepository
from app.main import app, lifespan
from app.models.pikpak import PikPakAuthStatusResponse
from app.models.task import DownloadLinkRequest, DownloadRangeRequest
from app.services.downloader_service import DownloaderService
from app.services.storage_service import StorageService
from run import build_uvicorn_kwargs

# ==============================================================================
# Part: test_log_setup.py
# ==============================================================================
client = TestClient(app, client=("127.0.0.1", 50000))


def auth_headers():
    headers = {}
    if settings.API_KEY:
        headers["X-API-Key"] = settings.API_KEY
        token = issue_2fa_token()
        if token:
            headers["X-2FA-Token"] = token
    return headers


def test_configure_logging_idempotent():
    """重复调用 configure_logging 不得产生重复 handler, 且 handler 实例不被重建"""
    configure_logging()
    app_logger = logging.getLogger("app")
    handlers_before = list(app_logger.handlers)
    configure_logging()
    configure_logging()
    assert app_logger.handlers == handlers_before
    # 环形缓冲 handler 有且仅有一个 (单例挂载)
    assert sum(1 for h in app_logger.handlers if h is ring_buffer_handler) == 1


def test_system_logs_endpoint_reads_ring_buffer():
    """向 app.* logger 写一条 -> /api/logs/system 可读到, 字段形状 (timestamp/level/logger/message) 保持"""
    logging.getLogger("app.core.testprobe").warning("系统日志探针 probe_marker_xyz")

    resp = client.get("/api/logs/system?limit=50", headers=auth_headers())
    assert resp.status_code == 200
    data = resp.json()
    assert set(data.keys()) == {"total", "logs"}
    hits = [l for l in data["logs"] if "probe_marker_xyz" in l["message"]]
    assert hits, "环形缓冲中必须能读到刚写入的系统日志"
    item = hits[0]
    assert item["level"] == "warning"
    assert item["logger"] == "app.core.testprobe"
    assert item["timestamp"]


def test_app_log_file_created():
    """RotatingFileHandler 目标文件 settings.LOG_DIR/app.log 存在 (INFO 级日志落盘)

    app_file 已迁入 QueueListener 后台线程, 文件创建异步完成: 有界轮询等待,
    正常延迟为毫秒级, 5s 上限兜底, 无竞态 flake。
    """
    configure_logging()
    logging.getLogger("app.core.testprobe").info("app.log 落盘探针 file_probe_marker")
    log_path = os.path.join(settings.LOG_DIR, "app.log")
    deadline = time.time() + 5
    while not os.path.exists(log_path) and time.time() < deadline:
        time.sleep(0.05)
    assert os.path.exists(log_path)


def test_queue_listener_wiring_and_shutdown():
    """I/O handler 经 QueueListener 异步分发且 ring buffer 保持直连; shutdown 确定性 flush"""
    from app.core import log_setup

    configure_logging()
    app_logger = logging.getLogger("app")

    # 1. wiring: app logger 恰为 [QueueHandler, ring_buffer_handler 直连], 监听线程存活
    queue_handlers = [h for h in app_logger.handlers if isinstance(h, QueueHandler)]
    assert len(queue_handlers) == 1
    assert sum(1 for h in app_logger.handlers if h is ring_buffer_handler) == 1
    assert log_setup._queue_listener is not None
    assert log_setup._queue_listener._thread is not None
    assert log_setup._queue_listener._thread.is_alive()

    # 2. 写入 marker 后停监听: QueueListener.stop() 的 FIFO sentinel 保证返回前
    #    残余队列已全部写入 handler, 此断言确定性成立, 无需轮询
    logging.getLogger("app.core.testprobe").warning("queue_flush_probe_marker")
    log_setup.shutdown_logging()
    assert log_setup._queue_listener is None
    with open(os.path.join(settings.LOG_DIR, "app.log"), encoding="utf-8") as f:
        assert "queue_flush_probe_marker" in f.read()

    # 3. 状态还原: 重置 guard 重建日志体系 (沿用测试直改内部状态先例), 供套件收尾使用
    log_setup._configured = False
    configure_logging()
    app_logger = logging.getLogger("app")
    assert sum(1 for h in app_logger.handlers if isinstance(h, QueueHandler)) == 1
    assert sum(1 for h in app_logger.handlers if h is ring_buffer_handler) == 1
    assert log_setup._queue_listener is not None
    assert log_setup._queue_listener._thread.is_alive()


def test_wait_queue_slot_times_out_without_dropping_records(monkeypatch):
    """队列持续满且消费线程卡死时, _wait_queue_slot 按 timeout 有界退出且不丢弃任何日志记录"""
    import queue

    from _async_wait import FakeClock

    from app.core import log_setup

    clock = FakeClock()
    monkeypatch.setattr(log_setup, "time", clock.time_namespace())

    q: queue.Queue[str] = queue.Queue(maxsize=1)
    q.put_nowait("pending_record")
    listener = MagicMock()
    listener._thread.is_alive.return_value = True

    log_setup._wait_queue_slot(listener, q, timeout=0.05)

    assert clock.now >= 0.05
    assert 10 <= len(clock.sleep_calls) <= 11
    assert q.qsize() == 1
    assert q.get_nowait() == "pending_record"


# ==============================================================================
# Part: test_log_perf.py
# ==============================================================================
client = TestClient(app, client=("127.0.0.1", 50000))

TOTAL_LOGS = 100_000
APPEND_SAMPLES = 100


def _seed_logs(task_id: str, count: int):
    """executemany 直插模拟长任务历史日志 (真实长度日志行), 并 checkpoint 归零 WAL"""
    conn = get_db()
    conn.executemany(
        "INSERT INTO task_logs (task_id, seq, time, level, message, created_at) VALUES (?, ?, '12:00:00', 'info', ?, 0)",
        [(task_id, i, f"📥 分块下载完成 {i % 256}/256 speed=8.2MB/s dc=2") for i in range(1, count + 1)],
    )
    conn.commit()
    # 大批量预插会把 WAL 撑大, checkpoint 截断避免测量期被摊销的回写开销干扰
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()


def test_append_log_under_1ms_with_100k_rows():
    """10万行历史日志下验证增量 append_log 序列递增正确性及 CI 防全表扫描退化阈值。
    严格 < 1ms SLA 微基准已迁移至 scripts/benchmarks/bench_sqlite_logs.py。
    """
    task_id = "perf_append_1"
    TaskQueueRepository.create(task_id, "download_range", {"channel": "c", "start_id": 1, "end_id": 2})
    _seed_logs(task_id, TOTAL_LOGS)

    # 预热一次 (首次调用含 import/文件句柄开销)
    TaskQueueRepository.append_log(task_id, "warmup")

    durations = []
    for i in range(APPEND_SAMPLES):
        t0 = time.perf_counter()
        TaskQueueRepository.append_log(task_id, f"perf {i}")
        durations.append(time.perf_counter() - t0)

    avg_ms = (sum(durations) / len(durations)) * 1000
    assert avg_ms < 20.0, f"单条 append 平均耗时 {avg_ms:.3f}ms 超过 CI 防退化基线 (20ms)"

    logs, last_seq, _ = TaskQueueRepository.get_task_logs(task_id, after_seq=TOTAL_LOGS)
    assert last_seq == TOTAL_LOGS + 1 + APPEND_SAMPLES
    assert len(logs) == APPEND_SAMPLES + 1
    TaskQueueRepository.delete(task_id)


def test_tasks_poll_response_under_50kb():
    """50 个任务 + 10 万条日志场景下, 4s 轮询端点响应体必须 < 50KB"""
    for n in range(50):
        tid = f"perf_task_{n}"
        TaskQueueRepository.create(
            tid,
            "download_range",
            {
                "channel": f"channel_{n}",
                "start_id": 1,
                "end_id": 100,
                "note": "参数填充以贴近真实体积" * 4,
            },
        )
        TaskQueueRepository.update_progress(
            tid, 12345678, 99999999, 1, 100, 5.5, 120.0, f"[1/100] video_file_{n}.mp4", 3, 2
        )
    _seed_logs("perf_task_0", TOTAL_LOGS)

    resp = client.get("/api/tasks", headers=auth_headers())
    assert resp.status_code == 200
    assert len(resp.content) < 50 * 1024, f"/api/tasks 响应体 {len(resp.content)} 字节超过 50KB 基线"
    body = resp.json()
    assert len(body) == 50
    target = next(t for t in body if t["log_count"] >= TOTAL_LOGS)
    assert target["last_log_seq"] >= TOTAL_LOGS

    # 增量端点默认页 (200 条) 亦在基线内; 客户端按需调大 limit 自担体积
    resp = client.get("/api/tasks/perf_task_0/logs", params={"after_seq": 50000}, headers=auth_headers())
    assert resp.status_code == 200
    assert len(resp.content) < 50 * 1024
    assert len(resp.json()["logs"]) == 200
    assert resp.json()["has_more"] is True

    # 清理
    conn = get_db()
    conn.execute("DELETE FROM task_queue WHERE id LIKE 'perf_task_%'")
    conn.execute("DELETE FROM task_logs WHERE task_id LIKE 'perf_task_%'")
    conn.commit()
    conn.close()


# ==============================================================================
# Part: test_failure_logger.py
# ==============================================================================
client = TestClient(app, client=("127.0.0.1", 50000))


def test_publish_failures_backward_compatibility(tmp_dir):
    """验证向后兼容: 不传 log_filename 时默认写入 publish_failures.jsonl, 格式与行为零破坏"""
    old_log_dir = settings.LOG_DIR
    settings.LOG_DIR = tmp_dir
    FailureLogger._loggers.clear()
    try:
        test_file = safe_write_bytes(os.path.join(tmp_dir, "compat.mp4"), b"test" * 100)
        FailureLogger.log_failure(
            file_path=test_file,
            target_chat_id="-100900008",
            error_obj=TimeoutError("Network timeout"),
            client_type="Bot",
            task_id="pub_task_compat",
        )

        pub_path = os.path.join(tmp_dir, "publish_failures.jsonl")
        dl_path = os.path.join(tmp_dir, "download_failures.jsonl")
        assert os.path.exists(pub_path), "publish_failures.jsonl 必须被创建"
        assert not os.path.exists(dl_path), "download_failures.jsonl 不应被创建"

        with open(pub_path, encoding="utf-8") as f:
            lines = [json.loads(line.strip()) for line in f if line.strip()]
        assert len(lines) == 1
        assert lines[0]["task_id"] == "pub_task_compat"
        assert lines[0]["client"] == "Bot"
        assert lines[0]["type"] == "NETWORK_TIMEOUT"
    finally:
        FailureLogger._loggers.clear()
        settings.LOG_DIR = old_log_dir


def test_download_failures_logging_and_fields(tmp_dir):
    """验证下载失败日志写入独立 download_failures.jsonl 且字段结构完整"""
    old_log_dir = settings.LOG_DIR
    settings.LOG_DIR = tmp_dir
    FailureLogger._loggers.clear()
    try:
        test_file = safe_write_bytes(os.path.join(tmp_dir, "dl_sample.mkv"), b"12345678" * 1024)
        FailureLogger.log_failure(
            file_path=test_file,
            target_chat_id="-100900045",
            error_obj=RuntimeError("Download incomplete: 5 retries exhausted"),
            client_type="download",
            task_id="dl_task_001",
            log_filename="download_failures.jsonl",
        )

        dl_path = os.path.join(tmp_dir, "download_failures.jsonl")
        assert os.path.exists(dl_path)

        with open(dl_path, encoding="utf-8") as f:
            entry = json.loads(f.readline().strip())

        assert entry["task_id"] == "dl_task_001"
        assert entry["chat_id"] == "-100900045"
        assert entry["file"] == "dl_sample.mkv"
        assert entry["size_mb"] > 0
        assert entry["client"] == "download"
        assert entry["type"] == "OTHER_ERROR"
        assert "5 retries exhausted" in entry["reason"]
        assert "time" in entry
    finally:
        FailureLogger._loggers.clear()
        settings.LOG_DIR = old_log_dir


def test_download_failures_classification():
    """抽测下载链路典型异常分类正确性 (FloodWait / 超时 / 超限 / 缺失 / 耗尽 / 权限 / 连接中断 / 磁盘满)"""
    import socket

    import telethon.errors

    class FakeFloodWait(Exception):
        pass

    assert FailureLogger.classify_error(FakeFloodWait("A wait of 30 seconds is required")) == "FLOOD_WAIT"
    assert FailureLogger.classify_error(TimeoutError("Client timed out")) == "NETWORK_TIMEOUT"
    assert FailureLogger.classify_error(ConnectionResetError()) == "NETWORK_TIMEOUT"
    assert FailureLogger.classify_error(ConnectionError()) == "NETWORK_TIMEOUT"
    assert (
        FailureLogger.classify_error(socket.gaierror(-3, "Temporary failure in name resolution")) == "NETWORK_TIMEOUT"
    )
    assert FailureLogger.classify_error(Exception("The file is too large to download")) == "FILE_TOO_LARGE"
    assert FailureLogger.classify_error(telethon.errors.FilePartsInvalidError(request=None)) == "FILE_TOO_LARGE"
    assert FailureLogger.classify_error(telethon.errors.FilePartTooBigError(request=None)) == "FILE_TOO_LARGE"
    assert FailureLogger.classify_error(telethon.errors.ChatWriteForbiddenError(request=None)) == "TG_NO_PERMISSION"
    assert FailureLogger.classify_error(telethon.errors.UserBannedInChannelError(request=None)) == "TG_NO_PERMISSION"
    assert FailureLogger.classify_error(FileNotFoundError("No such file or directory")) == "FILE_IO_ERROR"
    assert FailureLogger.classify_error(Exception("No space left on device")) == "FILE_IO_ERROR"
    assert FailureLogger.classify_error(Exception("Disk full error")) == "FILE_IO_ERROR"
    assert FailureLogger.classify_error(RuntimeError("目标文件不存在或为空，下载不完整")) == "FILE_IO_ERROR"
    assert FailureLogger.classify_error(RuntimeError("Download incomplete: chunk retry failed")) == "OTHER_ERROR"


def test_audit_download_failures_api(tmp_dir):
    """验证 GET /api/audit/download-failures 支持结构化查询、倒序展示、分页与分类过滤"""
    old_log_dir = settings.LOG_DIR
    settings.LOG_DIR = tmp_dir
    FailureLogger._loggers.clear()
    try:
        # 写入 3 条下载失败记录
        test_file = os.path.join(tmp_dir, "audit_test.mp4")
        FailureLogger.log_failure(
            test_file, "-1001", TimeoutError("Timeout 1"), "download", "task_1", log_filename="download_failures.jsonl"
        )
        FailureLogger.log_failure(
            test_file, "-1002", Exception("floodwait 60"), "download", "task_2", log_filename="download_failures.jsonl"
        )
        FailureLogger.log_failure(
            test_file,
            "-1001",
            Exception("entity_too_large"),
            "download",
            "task_3",
            log_filename="download_failures.jsonl",
        )

        # 1. 默认查询全部 (倒序: task_3, task_2, task_1)
        resp = client.get("/api/audit/download-failures", headers=auth_headers())
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        assert len(data["items"]) == 3
        assert data["items"][0]["task_id"] == "task_3"
        assert data["items"][1]["task_id"] == "task_2"
        assert data["items"][2]["task_id"] == "task_1"

        # 2. 分页验证
        resp_p1 = client.get("/api/audit/download-failures?page=1&limit=2", headers=auth_headers())
        assert resp_p1.status_code == 200
        assert len(resp_p1.json()["items"]) == 2
        assert resp_p1.json()["items"][0]["task_id"] == "task_3"

        resp_p2 = client.get("/api/audit/download-failures?page=2&limit=2", headers=auth_headers())
        assert resp_p2.status_code == 200
        assert len(resp_p2.json()["items"]) == 1
        assert resp_p2.json()["items"][0]["task_id"] == "task_1"

        # 3. 按 type 过滤
        resp_type = client.get("/api/audit/download-failures?type=FLOOD_WAIT", headers=auth_headers())
        assert resp_type.status_code == 200
        assert resp_type.json()["total"] == 1
        assert resp_type.json()["items"][0]["task_id"] == "task_2"

        # 4. 按 task_id 过滤
        resp_task = client.get("/api/audit/download-failures?task_id=task_1", headers=auth_headers())
        assert resp_task.status_code == 200
        assert resp_task.json()["total"] == 1
        assert resp_task.json()["items"][0]["type"] == "NETWORK_TIMEOUT"

        # 5. 按 chat_id 过滤
        resp_chat = client.get("/api/audit/download-failures?chat_id=-1001", headers=auth_headers())
        assert resp_chat.status_code == 200
        assert resp_chat.json()["total"] == 2
    finally:
        FailureLogger._loggers.clear()
        settings.LOG_DIR = old_log_dir


@pytest.mark.asyncio
async def test_downloader_service_failure_integration(tmp_dir, monkeypatch):
    """验证 DownloaderService 发生异常与空文件防御分支时均向 download_failures.jsonl 记账"""
    old_log_dir = settings.LOG_DIR
    settings.LOG_DIR = tmp_dir
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", tmp_dir)
    FailureLogger._loggers.clear()
    try:
        mock_client = AsyncMock()
        mock_msg = MagicMock()
        mock_msg.id = 999
        mock_msg.file = MagicMock(name="test_video.mp4", size=1024 * 1024)

        # 1. 模拟下载异常抛出
        with patch(
            "app.services.downloader_service.download_file_parallel",
            side_effect=TimeoutError("Download parallel timeout"),
        ):
            res = await DownloaderService.download_single_message(
                client=mock_client, chat_id="-100999", message=mock_msg, task_id="task_fail_exc"
            )
            assert res is None

        dl_path = os.path.join(tmp_dir, "download_failures.jsonl")
        assert os.path.exists(dl_path)

        with open(dl_path, encoding="utf-8") as f:
            lines = [json.loads(line.strip()) for line in f if line.strip()]

        assert len(lines) == 1
        assert lines[0]["task_id"] == "task_fail_exc"
        assert lines[0]["client"] == "download"
        assert lines[0]["type"] == "NETWORK_TIMEOUT"
        assert lines[0]["size_mb"] == 1.0

        # 2. 模拟下载后文件为空 (0 字节防御分支)
        zero_file = safe_write_bytes(os.path.join(tmp_dir, "empty.mp4"), b"")
        with patch("app.services.downloader_service.download_file_parallel", return_value=zero_file):
            res_zero = await DownloaderService.download_single_message(
                client=mock_client, chat_id="-100999", message=mock_msg, task_id="task_fail_empty"
            )
            assert res_zero is None

        with open(dl_path, encoding="utf-8") as f:
            lines = [json.loads(line.strip()) for line in f if line.strip()]

        assert len(lines) == 2
        assert lines[1]["task_id"] == "task_fail_empty"
        assert lines[1]["client"] == "download"
        assert lines[1]["size_mb"] == 1.0
        assert "不存在或为空" in lines[1]["reason"]
    finally:
        FailureLogger._loggers.clear()
        settings.LOG_DIR = old_log_dir


def test_audit_download_failures_null_size_mb_and_malformed(tmp_dir):
    """验证当日志中存在 null 的 size_mb 或损坏行时，审计接口不会抛 500 且返回安全默认值"""
    old_log_dir = settings.LOG_DIR
    settings.LOG_DIR = tmp_dir
    FailureLogger._loggers.clear()
    try:
        dl_path = os.path.join(tmp_dir, "download_failures.jsonl")
        with open(dl_path, "w", encoding="utf-8") as f:
            # 正常记录
            f.write(
                json.dumps(
                    {
                        "time": "2026-09-18 10:00:00",
                        "task_id": "t1",
                        "chat_id": "-1",
                        "file": "f1.mp4",
                        "size_mb": 10.0,
                        "client": "download",
                        "type": "OTHER_ERROR",
                        "reason": "r1",
                    }
                )
                + "\n"
            )
            # size_mb 为 null
            f.write(
                json.dumps(
                    {
                        "time": "2026-09-18 10:01:00",
                        "task_id": "t2",
                        "chat_id": "-1",
                        "file": "f2.mp4",
                        "size_mb": None,
                        "client": "download",
                        "type": "FLOOD_WAIT",
                        "reason": "r2",
                    }
                )
                + "\n"
            )
            # 损坏 JSON
            f.write("{malformed json line}\n")
            # 缺少部分字段
            f.write(json.dumps({"task_id": "t3", "type": "NETWORK_TIMEOUT"}) + "\n")

        resp = client.get("/api/audit/download-failures", headers=auth_headers())
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        # 倒序展示: t3, t2, t1
        assert data["items"][0]["task_id"] == "t3"
        assert data["items"][0]["size_mb"] == 0.0
        assert data["items"][1]["task_id"] == "t2"
        assert data["items"][1]["size_mb"] == 0.0
        assert data["items"][2]["task_id"] == "t1"
        assert data["items"][2]["size_mb"] == 10.0
    finally:
        FailureLogger._loggers.clear()
        settings.LOG_DIR = old_log_dir


def test_audit_download_failures_rotated_archives(tmp_dir):
    """验证审计接口能够跨滚动归档文件 (.1, .2) 查询并保持严格的最新记录在前"""
    old_log_dir = settings.LOG_DIR
    settings.LOG_DIR = tmp_dir
    FailureLogger._loggers.clear()
    try:
        base_path = os.path.join(tmp_dir, "download_failures.jsonl")
        arch_path = f"{base_path}.1"

        # 归档卷写入历史旧记录
        with open(arch_path, "w", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "time": "2026-09-18 09:00:00",
                        "task_id": "old_1",
                        "chat_id": "-100",
                        "file": "old.mp4",
                        "size_mb": 5.0,
                        "client": "download",
                        "type": "OTHER_ERROR",
                        "reason": "old err",
                    }
                )
                + "\n"
            )

        # 当前活跃卷写入新记录
        with open(base_path, "w", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "time": "2026-09-18 09:30:00",
                        "task_id": "new_1",
                        "chat_id": "-100",
                        "file": "new.mp4",
                        "size_mb": 8.0,
                        "client": "download",
                        "type": "NETWORK_TIMEOUT",
                        "reason": "new err",
                    }
                )
                + "\n"
            )

        resp = client.get("/api/audit/download-failures", headers=auth_headers())
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert data["items"][0]["task_id"] == "new_1"
        assert data["items"][1]["task_id"] == "old_1"
    finally:
        FailureLogger._loggers.clear()
        settings.LOG_DIR = old_log_dir


def test_failure_logger_concurrent_threads(tmp_dir):
    """验证多线程并发调用 FailureLogger.log_failure 时线程安全且不发生死锁或崩溃"""

    old_log_dir = settings.LOG_DIR
    settings.LOG_DIR = tmp_dir
    FailureLogger._loggers.clear()
    try:

        def log_task(idx):
            FailureLogger.log_failure(
                file_path=f"/fake/path/file_{idx}.mp4",
                target_chat_id="-100999",
                error_obj=TimeoutError(f"Timeout {idx}"),
                client_type="download",
                task_id=f"concurrent_task_{idx}",
                log_filename="download_failures.jsonl",
                file_size_mb=1.5,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(log_task, i) for i in range(50)]
            for f in concurrent.futures.as_completed(futures):
                f.result()

        dl_path = os.path.join(tmp_dir, "download_failures.jsonl")
        assert os.path.exists(dl_path)
        with open(dl_path, encoding="utf-8") as f:
            lines = [json.loads(line.strip()) for line in f if line.strip()]
        assert len(lines) == 50
    finally:
        FailureLogger._loggers.clear()
        settings.LOG_DIR = old_log_dir


def test_get_file_name_and_size_progressive_photo():
    """验证 DownloaderService.get_file_name_and_size 在 Photo 具有 PhotoSizeProgressive 时提取正确大小"""
    from unittest.mock import MagicMock

    from telethon.tl.types import MessageMediaPhoto

    mock_msg = MagicMock()
    mock_msg.id = 777
    mock_photo = MagicMock()

    # 模拟 PhotoSizeProgressive
    prog_size = MagicMock()
    prog_size.size = 0
    prog_size.sizes = [50000, 150000, 320000]

    mock_photo.sizes = [prog_size]
    mock_msg.media = MessageMediaPhoto(photo=mock_photo)

    file_name, file_size = DownloaderService.get_file_name_and_size(mock_msg)
    assert file_name == "photo_777.jpg"
    assert file_size == 320000


# ==============================================================================
# Part: test_security_redact.py
# ==============================================================================
FAKE_TOKEN = "pikpak-fake-token-abc123"
SIGNED_URL = "https://cdn.fake.test/dl/file.bin?Expires=9999999999&X-Sign=FAKESIGN&auth_key=KEY123&x=1"
PLAIN_URL = "https://cdn.fake.test/dl/file.bin?x=1&y=2"


def test_redact_text_strips_signed_url_params():
    """URL 签名参数值擦除: 参数名保留、值变 ***、大小写变体命中、普通文本/非敏感参数原样"""
    out = redact_text(SIGNED_URL)
    assert "FAKESIGN" not in out and "KEY123" not in out and "9999999999" not in out
    assert "X-Sign=***" in out and "auth_key=***" in out and "Expires=***" in out
    assert out.startswith("https://cdn.fake.test/dl/file.bin?")
    assert "x=1" in out, "非敏感查询参数不受影响"

    assert redact_text("?Token=abc&ACCESS_TOKEN=def") == "?Token=***&ACCESS_TOKEN=***"
    assert redact_text(PLAIN_URL) == PLAIN_URL
    assert redact_text("普通文本无 URL") == "普通文本无 URL"


def test_redact_text_removes_token_literal(monkeypatch):
    """PIKPAK_TOKEN 字面值整段替换 (懒读取: 每次调用取 settings 当前值)"""
    monkeypatch.setattr(settings, "PIKPAK_TOKEN", FAKE_TOKEN)
    text = f"curl '{SIGNED_URL}' -H 'Authorization: Bearer {FAKE_TOKEN}'"
    out = redact_text(text)
    assert FAKE_TOKEN not in out and "FAKESIGN" not in out
    assert "Bearer ***" in out

    # settings 无 token 时仅做参数擦除 (不缓存旧值)
    monkeypatch.setattr(settings, "PIKPAK_TOKEN", None)
    assert redact_text(f"Bearer {FAKE_TOKEN}") == f"Bearer {FAKE_TOKEN}"


def test_redact_payload_recursive():
    """递归 dict/list/tuple/str: str 叶子全部擦除, 非 str 原样, 结构形状不变"""
    payload = {
        "url": SIGNED_URL,
        "nested": {"link": [SIGNED_URL, ("inner", 5, SIGNED_URL)], "count": 3},
        "none": None,
        "flag": True,
    }
    out = redact_payload(payload)
    assert out["url"].endswith("x=1") and "FAKESIGN" not in out["url"]
    assert "FAKESIGN" not in json.dumps(out, ensure_ascii=False, default=str)
    assert "FAKESIGN" not in str(out["nested"]["link"][1]) and "KEY123" not in str(out["nested"]["link"][1])
    assert out["nested"]["count"] == 3 and out["none"] is None and out["flag"] is True


def test_redact_log_filter_covers_msg_and_args():
    """RedactLogFilter: record.msg 与 record.args 中的 str 元素均被擦除, 且不吞记录"""
    flt = RedactLogFilter()

    rec_msg = logging.LogRecord("t", logging.ERROR, __file__, 1, f"直接 {SIGNED_URL}", None, None)
    assert flt.filter(rec_msg) is True
    assert "FAKESIGN" not in rec_msg.getMessage() and "***" in rec_msg.getMessage()

    rec_args = logging.LogRecord("t", logging.ERROR, __file__, 1, "link %s code=%d", (SIGNED_URL, 7), None)
    assert flt.filter(rec_args) is True
    rendered = rec_args.getMessage()
    assert "FAKESIGN" not in rendered and "code=7" in rendered and "***" in rendered


def test_ring_handler_records_redacted(monkeypatch):
    """集成: configure_logging 后经 'app' logger 输出 → ring handler 环内记录已脱敏
    (console/app_file/ring 三 handler 挂载点生效; ring 为项目 /api/logs/system 数据源)。
    token 字面值擦除依赖 settings 当前值 (懒读取), 故一并 monkeypatch。"""
    monkeypatch.setattr(settings, "PIKPAK_TOKEN", FAKE_TOKEN)
    configure_logging()
    logger = logging.getLogger("app.security_redact_test")
    logger.error("pikpak link %s token=%s", SIGNED_URL, FAKE_TOKEN)

    snapshot = ring_buffer_handler.snapshot()
    assert snapshot, "ring handler 必须捕获到记录"
    last = snapshot[-1]
    assert "FAKESIGN" not in last["message"] and FAKE_TOKEN not in last["message"]
    assert "***" in last["message"]


def test_settings_api_never_echoes_pikpak_token(monkeypatch):
    """settings API 任何响应体 (含错误信息) 不回显 PIKPAK_TOKEN; 路由集精确枚举
    (GET/PUT + verify-transit, 无任何 pikpak token 写端点)"""
    from app.main import app

    monkeypatch.setattr(settings, "PIKPAK_TOKEN", FAKE_TOKEN)

    headers = {}
    if settings.API_KEY:
        from app.api.auth import issue_2fa_token

        headers["X-API-Key"] = settings.API_KEY
        token = issue_2fa_token()
        if token:
            headers["X-2FA-Token"] = token

    client = TestClient(app, client=("127.0.0.1", 50000))

    resp = client.get("/api/settings", headers=headers)
    assert resp.status_code == 200
    assert FAKE_TOKEN not in json.dumps(resp.json(), ensure_ascii=False, default=str)

    resp = client.put("/api/settings", json={"settings": {"max_global_concurrency": "3"}}, headers=headers)
    assert resp.status_code == 200
    assert FAKE_TOKEN not in json.dumps(resp.json(), ensure_ascii=False, default=str)

    resp = client.put("/api/settings", json={"settings": {}}, headers=headers)
    assert resp.status_code == 400
    assert FAKE_TOKEN not in json.dumps(resp.json(), ensure_ascii=False, default=str)

    # settings 只读结构固化: 经 OpenAPI 发布面枚举 —— 新版 FastAPI 子路由以
    # _IncludedRouter 包装存储 (app.routes 无展开 path), openapi.json 才是稳定契约面
    openapi = client.get("/openapi.json").json()
    settings_paths = {
        p: sorted(m.upper() for m in v) for p, v in openapi["paths"].items() if p.startswith("/api/settings")
    }
    assert settings_paths == {
        "/api/settings": ["GET", "PUT"],
        "/api/settings/verify-transit": ["POST"],
    }, f"settings 路由集漂移, 存在新增写端点风险: {settings_paths}"


# ==============================================================================
# Part: test_metrics.py
# ==============================================================================
client = TestClient(app, client=("127.0.0.1", 50000))


def metrics_headers():
    """/metrics 凭证 (PR1 起为受保护入口): 仅需 X-API-Key, 无需 2FA (Prometheus 抓取语义)。
    空配置 (本机免认证) 时返回空头, 与业务侧 auth_headers 语义一致。"""
    return {"X-API-Key": settings.API_KEY} if settings.API_KEY else {}


def test_metrics_endpoint_response():
    """验证 GET /metrics 端点返回 200 并包含全部清单内定义的指标"""
    resp = client.get("/metrics", headers=metrics_headers())
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]
    text = resp.text

    expected_metrics = [
        "task_status_transitions_total",
        "task_retry_total",
        "floodwait_total",
        "chunk_retry_total",
        "chunk_dropped_total",
        "download_bytes_total",
        "running_tasks",
        "queued_task_depth",
        "active_ws_connections",
        "download_threads_in_use",
        "flood_locked_accounts",
    ]
    for m in expected_metrics:
        assert m in text, f"缺少预期指标: {m}"


def test_task_status_transition_metric():
    """模拟任务状态流转后断言 /metrics 输出中对应 label 计数器递增"""
    tid = "metric_test_task_1"
    TaskQueueRepository.create(tid, "download_link", {}, status="queued")

    # 初始获取 /metrics 内容
    initial_running = task_status_transitions_total.labels(status="running")._value.get()
    initial_completed = task_status_transitions_total.labels(status="completed")._value.get()

    # 1. CAS 认领任务 (queued -> running)
    acquired = TaskQueueRepository.acquire_task(tid, account_id=1)
    assert acquired is True
    assert task_status_transitions_total.labels(status="running")._value.get() == initial_running + 1

    # 2. 更新状态为 completed
    TaskQueueRepository.update_status(tid, "completed")
    assert task_status_transitions_total.labels(status="completed")._value.get() == initial_completed + 1

    # 2.5 更新不存在的任务，不应虚假递增计数器
    curr_completed = task_status_transitions_total.labels(status="completed")._value.get()
    TaskQueueRepository.update_status("nonexistent_task_xyz_999", "completed")
    assert task_status_transitions_total.labels(status="completed")._value.get() == curr_completed

    # 3. 校验端点输出渲染
    resp = client.get("/metrics", headers=metrics_headers())
    assert resp.status_code == 200
    assert 'task_status_transitions_total{status="completed"}' in resp.text
    assert 'task_status_transitions_total{status="running"}' in resp.text

    TaskQueueRepository.delete(tid)


@pytest.mark.asyncio
async def test_concurrent_increments_thread_safety():
    """在 to_thread 工作线程与事件循环并发对 counter 累加，断言严格一致无竞争异常"""
    initial = chunk_retry_total._value.get()
    asyncio.get_running_loop()

    num_threads = 5
    num_iters = 200

    def thread_worker():
        for _ in range(num_iters):
            chunk_retry_total.inc()

    async def async_worker():
        for _ in range(num_iters):
            chunk_retry_total.inc()
            await asyncio.sleep(0)

    # 启动工作线程池与异步协程并发累加
    thread_futures = [asyncio.to_thread(thread_worker) for _ in range(num_threads)]
    coros = [async_worker() for _ in range(num_threads)]

    await asyncio.gather(*thread_futures, *coros)

    expected_total = initial + (num_threads * num_iters * 2)
    assert chunk_retry_total._value.get() == expected_total


def test_route_snapshot_integrity():
    """验证加入 /metrics 后不破坏既有业务路由结构与顺序"""
    openapi_paths = list(app.openapi()["paths"].keys())
    assert "/metrics" in openapi_paths

    # 抽测核心既有路由完整性
    core_expected_routes = [
        "/api/tasks",
        "/api/tasks/{task_id}",
        "/api/tasks/{task_id}/pause",
        "/api/tasks/{task_id}/resume",
        "/api/tasks/{task_id}/cancel",
        "/api/logs/tasks/{task_id}",
        "/api/logs/system",
        "/api/audit/forward-skips",
        "/api/audit/publish-failures",
        "/api/audit/download-failures",
        "/api/auth/status",
        "/api/auth/verify",
        "/api/telegram/web-2fa/send",
        "/api/telegram/native/send-code",
    ]
    for r in core_expected_routes:
        assert r in openapi_paths, f"既有路由丢失: {r}"


def test_rate_limiter_allows_metrics_without_429(monkeypatch):
    """已认证的高频 /metrics 抓取可跳过全局限流。"""
    monkeypatch.setattr(settings, "API_KEY", "test_metrics_scrape_key")
    for _ in range(130):
        resp = client.get("/metrics", headers=metrics_headers())
        assert resp.status_code == 200


@pytest.mark.parametrize(("host", "url_host"), [("127.0.0.2", "127.0.0.2"), ("::1", "[::1]")])
def test_live_server_uses_same_loopback_for_child_and_probe(tmp_path, monkeypatch, host, url_host):
    from e2e_live.harness import scratch_env, server_proc

    monkeypatch.setenv("TG_API_KEY", "legacy_test_key_should_not_leak")
    monkeypatch.setattr(server_proc, "_read_bytes_guarded", lambda _path: None)
    subprocess_start = MagicMock()
    monkeypatch.setattr(server_proc.subprocess, "Popen", subprocess_start)
    server = server_proc.LiveServer(tmp_path, 8500, host)
    server.start()

    assert server.base_url == f"http://{url_host}:8500"
    assert subprocess_start.call_args.kwargs["env"]["HOST"] == host
    assert subprocess_start.call_args.kwargs["env"]["TG_API_KEY"] == ""
    assert subprocess_start.call_args.kwargs["env"]["CONSOLE_REQUIRE_API_KEY"] == "false"

    probe_socket = MagicMock()
    probe_socket.__enter__.return_value.connect_ex.return_value = 1
    socket_factory = MagicMock(return_value=probe_socket)
    monkeypatch.setattr(scratch_env.socket, "socket", socket_factory)
    scratch_env.ensure_no_real_server(host, 8500)
    family = scratch_env.socket.AF_INET6 if host == "::1" else scratch_env.socket.AF_INET
    socket_factory.assert_called_once_with(family, scratch_env.socket.SOCK_STREAM)
    probe_socket.__enter__.return_value.connect_ex.assert_called_once_with((host, 8500))


def test_live_server_rejects_non_loopback_bind(tmp_path):
    from e2e_live.harness import server_proc

    with pytest.raises(ValueError, match="E2E_LIVE_HOST"):
        server_proc.LiveServer(tmp_path, 8500, "0.0.0.0")


@pytest.mark.asyncio
async def test_queued_task_depth_accuracy_under_saturation():
    """验证当系统处于满载并发饱和状态时，queued_task_depth 仍准确刷新且不被批次大小截断"""
    from app.core.scheduler import task_scheduler

    # 创建 60 个排队任务 (> batch_size=50)
    created_ids = []
    try:
        for i in range(60):
            t_id = f"test_depth_task_{i}"
            TaskQueueRepository.create(t_id, "download_link", {}, status="queued")
            created_ids.append(t_id)

        # 模拟 32 个满载任务已在 running 中，使 scheduler 触发并发饱和早退
        task_scheduler._running_tasks = {f"running_mock_{k}": asyncio.create_task(asyncio.sleep(10)) for k in range(32)}

        # 执行一次调度循环 tick
        await task_scheduler._schedule_next_tasks()

        # 断言 queued_task_depth 不被截断为 50，而是准确反映真实的 60
        assert queued_task_depth._value.get() >= 60

        # 取消 mock 任务
        for t in task_scheduler._running_tasks.values():
            t.cancel()
        task_scheduler._running_tasks.clear()
    finally:
        for t_id in created_ids:
            TaskQueueRepository.delete(t_id)


def test_floodwait_and_locked_accounts_metrics():
    """验证 ClientManager 标记 FloodWait 时 floodwait_total 计数与 flood_locked_accounts 仪表求值"""
    import time
    from unittest.mock import MagicMock

    from app.core.client_manager import client_manager

    init_acc = floodwait_total.labels(scope="account")._value.get()
    init_bot = floodwait_total.labels(scope="bot")._value.get()

    mock_acc = MagicMock()
    mock_acc.account_id = 77777
    mock_acc.flood_wait_until = 0

    def mock_mark_acc_flood(sec):
        mock_acc.flood_wait_until = time.time() + sec

    mock_acc.mark_flood_wait = mock_mark_acc_flood
    client_manager._accounts[77777] = mock_acc

    mock_bot = MagicMock()
    mock_bot.bot_id = 88888

    def mock_mark_bot_flood(sec):
        mock_bot.flood_wait_until = time.time() + sec

    mock_bot.mark_flood_wait = mock_mark_bot_flood
    client_manager._bots[88888] = mock_bot

    try:
        # 1. 触发账号 FloodWait
        client_manager.mark_flood_wait(77777, 60)
        assert floodwait_total.labels(scope="account")._value.get() == init_acc + 1

        # 2. 触发 Bot FloodWait
        client_manager.mark_flood_wait(88888, 60)
        assert floodwait_total.labels(scope="bot")._value.get() == init_bot + 1

        # 3. 校验 flood_locked_accounts 仪表动态求值
        samples = list(flood_locked_accounts.collect())[0].samples
        assert samples[0].value >= 1.0

        resp = client.get("/metrics", headers=metrics_headers())
        assert resp.status_code == 200
        assert 'floodwait_total{scope="account"}' in resp.text
        assert 'floodwait_total{scope="bot"}' in resp.text
        assert "flood_locked_accounts" in resp.text
    finally:
        client_manager._accounts.pop(77777, None)
        client_manager._bots.pop(88888, None)


@pytest.mark.asyncio
async def test_active_ws_connections_metric():
    """验证 WebSocket 连接建立与断开时 active_ws_connections 仪表即时联动"""
    from unittest.mock import AsyncMock

    from app.core.connection_manager import connection_manager

    initial_ws = len(connection_manager._connections)
    mock_ws = AsyncMock()

    await connection_manager.connect(mock_ws)
    assert active_ws_connections._value.get() == initial_ws + 1

    connection_manager.disconnect(mock_ws)
    assert active_ws_connections._value.get() == initial_ws


@pytest.mark.asyncio
async def test_download_threads_and_retry_metrics():
    """验证调度器线程水门配额与自动重试指标的正确递增"""
    from app.core.scheduler import task_scheduler

    initial_retries = task_retry_total._value.get()
    tid = "metric_test_retry_task"
    TaskQueueRepository.create(tid, "download_link", {}, status="running")

    try:
        # 模拟任务重试
        await task_scheduler._handle_task_retry(tid, retry_count=0, max_retries=3, error_msg="Test temporary error")
        assert task_retry_total._value.get() == initial_retries + 1

        # 校验 download_threads_in_use 分配与释放
        initial_threads = download_threads_in_use._value.get()
        async with task_scheduler._get_lock():
            task_scheduler._global_download_threads += 4
            task_scheduler._task_thread_allocations[tid] = 4
            download_threads_in_use.set(task_scheduler._global_download_threads)

        assert download_threads_in_use._value.get() == initial_threads + 4

        # 模拟清理释放
        task_scheduler._cleanup_task_resources(tid, account_id=None)
        assert download_threads_in_use._value.get() == initial_threads
    finally:
        TaskQueueRepository.delete(tid)


def test_download_bytes_and_chunk_dropped_metrics():
    """验证累计下载字节与分块耗尽丢弃指标"""
    initial_bytes = download_bytes_total._value.get()
    download_bytes_total.inc(1048576)
    assert download_bytes_total._value.get() == initial_bytes + 1048576

    initial_dropped = chunk_dropped_total._value.get()
    chunk_dropped_total.inc()
    assert chunk_dropped_total._value.get() == initial_dropped + 1


@pytest.mark.asyncio
async def test_download_bytes_progress_cb_hooks():
    """深测 download_executor_service 中 progress_cb 的真实流量累加、断点基线与跳过文件防重"""
    initial_bytes = download_bytes_total._value.get()

    last_bytes = 0

    def progress_cb(current, total, speed, eta, file_name, concurrency=0, dc_id=0, concurrency_total=0):
        nonlocal last_bytes
        is_skip_dummy = concurrency == 0 and speed == 0.0 and eta == 0.0 and current == 100 and total == 100
        if not is_skip_dummy:
            delta = current - last_bytes
            if delta > 0:
                download_bytes_total.inc(delta)
            last_bytes = current

    def set_initial_bytes(val: int):
        nonlocal last_bytes
        last_bytes = max(0, val)

    progress_cb.set_initial_bytes = set_initial_bytes

    # 1. 模拟防重跳过文件 (dummy 100/100, concurrency=0)，断言不应虚假累加流量
    progress_cb(100, 100, 0.0, 0.0, "skipped_video.mp4", concurrency=0, dc_id=0, concurrency_total=4)
    assert download_bytes_total._value.get() == initial_bytes

    # 2. 模拟断点续传场景：已有 50MB 断点基线
    set_initial_bytes(52428800)
    # 第一次心跳触发已下载 50MB，delta 应为 0，不重复计费
    progress_cb(52428800, 104857600, 5.0, 10.0, "resumed_video.mp4", concurrency=4, dc_id=2, concurrency_total=4)
    assert download_bytes_total._value.get() == initial_bytes

    # 3. 接下来新下载 1MB 分块 (50MB -> 51MB)，精准累加 1MB
    progress_cb(
        52428800 + 1048576, 104857600, 5.0, 9.8, "resumed_video.mp4", concurrency=4, dc_id=2, concurrency_total=4
    )
    assert download_bytes_total._value.get() == initial_bytes + 1048576


def test_client_manager_string_and_fallback_floodwait():
    """深测 ClientManager 支持字符串 ID 及未在池内注册 client 的保底 account scope 记账"""
    import time
    from unittest.mock import MagicMock

    from app.core.client_manager import client_manager

    init_acc = floodwait_total.labels(scope="account")._value.get()

    mock_acc = MagicMock()
    mock_acc.account_id = 99999
    mock_acc.flood_wait_until = 0
    mock_acc.mark_flood_wait = lambda sec: setattr(mock_acc, "flood_wait_until", time.time() + sec)
    client_manager._accounts[99999] = mock_acc

    try:
        # 字符串 ID 传递
        client_manager.mark_flood_wait("99999", 30)
        assert floodwait_total.labels(scope="account")._value.get() == init_acc + 1

        # 未映射的临时 client 实例 fallback
        unmapped_client = MagicMock()
        client_manager.mark_flood_wait(unmapped_client, 30)
        assert floodwait_total.labels(scope="account")._value.get() == init_acc + 2
    finally:
        client_manager._accounts.pop(99999, None)


@pytest.mark.asyncio
async def test_running_tasks_immediate_responsiveness_and_stop_reset():
    """深测 running_tasks 仪表在任务调度启动、资源清理和系统关闭时的即时一致性"""
    from app.core.scheduler import task_scheduler

    initial_running = running_tasks._value.get()
    tid = "metric_immediate_test_task"

    # 1. 模拟任务加入 _running_tasks 并即时更新仪表
    dummy_task = asyncio.create_task(asyncio.sleep(10))
    task_scheduler._running_tasks[tid] = dummy_task
    running_tasks.set(len(task_scheduler._running_tasks))
    assert running_tasks._value.get() == initial_running + 1

    # 2. 调用 _cleanup_task_resources，断言即时扣减仪表
    task_scheduler._cleanup_task_resources(tid, account_id=None)
    assert running_tasks._value.get() == initial_running

    dummy_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dummy_task

    # 3. 校验调度器线程水门清空
    task_scheduler._global_download_threads = 8
    download_threads_in_use.set(8)
    task_scheduler._running_tasks["temp_tid"] = asyncio.create_task(asyncio.sleep(0.1))
    await task_scheduler.stop()
    assert running_tasks._value.get() == 0
    assert download_threads_in_use._value.get() == 0


@pytest.mark.asyncio
async def test_fast_telethon_single_client_floodwait_reporting():
    """验证 FastTelethon 在单客户端模式下遇到 FloodWait 时，依然正确向 client_manager 上报并累加指标"""
    from unittest.mock import MagicMock

    from app.core.client_manager import client_manager

    init_flood = floodwait_total.labels(scope="account")._value.get()

    mock_client = MagicMock()
    mock_client.session = MagicMock()
    mock_client.session.dc_id = 2
    mock_client.is_connected = MagicMock(return_value=True)

    # 注册进入 client_manager 作为 account
    mock_acc = MagicMock()
    mock_acc.client = mock_client
    mock_acc.account_id = 55555
    mock_acc.flood_wait_until = 0
    mock_acc.mark_flood_wait = lambda sec: setattr(mock_acc, "flood_wait_until", 9999999999)
    client_manager._accounts[55555] = mock_acc

    try:
        # 直接调用 client_manager.mark_flood_wait 模拟 worker 触发
        client_manager.mark_flood_wait(mock_client, 120)
        assert floodwait_total.labels(scope="account")._value.get() == init_flood + 1
        assert mock_acc.flood_wait_until > 0
    finally:
        client_manager._accounts.pop(55555, None)


# ==============================================================================
APP = pathlib.Path(__file__).resolve().parent.parent.parent / "app"


def _imports_of(path: pathlib.Path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                yield a.name
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                yield node.module
            elif node.level > 0:
                parts = path.relative_to(APP.parent).parts[:-1]
                if len(parts) >= node.level:
                    base_pkg = ".".join(parts[: len(parts) - node.level + 1])
                    mod = f"{base_pkg}.{node.module}" if node.module else base_pkg
                    yield mod


@pytest.mark.parametrize("py", [p for p in (APP / "core").rglob("*.py")], ids=lambda p: p.name)
def test_core_must_not_import_api_or_services(py):
    banned = [
        m
        for m in _imports_of(py)
        if m == "app.api" or m.startswith("app.api.") or m == "app.services" or m.startswith("app.services.")
    ]
    assert not banned, f"分层违规: {py.name} import 了 {banned} (app/core 禁止依赖 api/services)"


@pytest.mark.parametrize("py", [p for p in (APP / "services").rglob("*.py")], ids=lambda p: p.name)
def test_services_must_not_import_api(py):
    banned = [m for m in _imports_of(py) if m == "app.api" or m.startswith("app.api.")]
    assert not banned, f"分层违规: {py.name} import 了 {banned} (app/services 禁止依赖 app/api)"


def test_deprecated_facades_fully_removed():
    """断言已被废弃的兼容门面与旧模块在物理磁盘上已被彻底移除"""
    assert not (APP / "core" / "telegram_engine.py").exists(), "telegram_engine.py 必须被彻底删除"
    assert not (APP / "api" / "deps.py").exists(), "app/api/deps.py 必须被彻底删除"


def test_no_telegram_engine_imports_in_app_and_tests():
    """断言全库 app/ 与 tests/ 内部零 telegram_engine 导入"""
    tests_dir = pathlib.Path(__file__).parent
    violations = []
    for search_dir in (APP, tests_dir):
        for py_path in search_dir.rglob("*.py"):
            for m in _imports_of(py_path):
                if "telegram_engine" in m:
                    violations.append(f"{py_path.name} imports {m}")
    assert not violations, f"发现已废弃的 telegram_engine 引用残留: {violations}"


# ==============================================================================
# Part: test_config_validation.py
# ==============================================================================
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_validate_required_passes_when_configured():
    """当前测试环境 (本地 .env / CI 注入哑凭证) 下凭证齐备, 校验不应抛错"""
    settings.validate_required()


def test_validate_required_collects_all_missing(monkeypatch):
    """三个必填项全部缺失时, 一次性列出全部缺失键而不是命中第一个即抛"""
    monkeypatch.setattr(settings, "API_ID", None)
    monkeypatch.setattr(settings, "API_HASH", None)
    monkeypatch.setattr(settings, "BOT_TOKENS", [])
    with pytest.raises(RuntimeError) as exc_info:
        settings.validate_required()
    message = str(exc_info.value)
    for key in ("TG_API_ID", "TG_API_HASH", "TG_BOT_TOKENS"):
        assert key in message


def test_validate_required_rejects_legacy_tg_api_key_alias_without_api_key(monkeypatch):
    """TG_API_KEY 别名已移除 —— 残留旧变量且未显式设置 API_KEY 时快速失败强制改名,
    防止旧部署升级后鉴权静默降级为免认证"""
    monkeypatch.setenv("TG_API_KEY", "legacy_alias_value_0123456789")
    monkeypatch.setattr(settings, "API_KEY", "")
    with pytest.raises(RuntimeError) as exc_info:
        settings.validate_required()
    message = str(exc_info.value)
    assert "TG_API_KEY" in message
    assert "API_KEY" in message


def test_validate_required_allows_both_api_key_and_legacy_alias(monkeypatch):
    """两者并存时放行启动 (弃用告警由 lifespan 日志承载), 不阻塞部署侧清理"""
    monkeypatch.setenv("TG_API_KEY", "legacy_alias_value_0123456789")
    monkeypatch.setattr(settings, "API_KEY", "explicit_api_key_0123456789")
    settings.validate_required()


def _env_without_credentials():
    """空串占位屏蔽凭证: _load_dotenv 对已存在的环境变量 (含空串) 不覆盖, 可在本地真实屏蔽 .env"""
    env = dict(os.environ)
    env["TG_API_ID"] = ""
    env["TG_API_HASH"] = ""
    env["TG_BOT_TOKENS"] = ""
    return env


def test_config_import_succeeds_without_credentials():
    """fresh clone 无 .env 场景: import app.config 必须成功, 缺失字段置 None/空列表 (子进程隔离)"""
    code = (
        "import app.config as c\n"
        "print('IMPORT_OK')\n"
        "print(c.settings.API_ID, c.settings.API_HASH, c.settings.BOT_TOKENS)\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=_env_without_credentials(),
        cwd=REPO_ROOT,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert "IMPORT_OK" in proc.stdout
    assert "None None []" in proc.stdout


def test_run_py_fails_fast_listing_all_missing_keys():
    """run.py 启动前校验: 无凭证时快速失败且一次性列出全部缺失键 (uvicorn 不得启动)"""
    proc = subprocess.run(
        [sys.executable, "run.py"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=_env_without_credentials(),
        cwd=REPO_ROOT,
        timeout=60,
    )
    assert proc.returncode != 0
    for key in ("TG_API_ID", "TG_API_HASH", "TG_BOT_TOKENS"):
        assert key in proc.stderr


def test_run_py_help_and_version_exit_cleanly_without_credentials():
    """run.py --help / --version 在无凭证环境下直接输出并 0 码退出, 不触发单实例锁或凭证报错"""
    from app import APP_VERSION
    from run import parse_cli_args

    parsed = parse_cli_args(["--host", "0.0.0.0", "--port", "9000"])
    assert parsed.host == "0.0.0.0"
    assert parsed.port == 9000

    help_proc = subprocess.run(
        [sys.executable, "run.py", "--help"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=_env_without_credentials(),
        cwd=REPO_ROOT,
        timeout=30,
    )
    assert help_proc.returncode == 0, help_proc.stderr
    assert "--host" in help_proc.stdout
    assert "--port" in help_proc.stdout
    assert "TG_API_ID" in help_proc.stdout

    ver_proc = subprocess.run(
        [sys.executable, "run.py", "--version"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=_env_without_credentials(),
        cwd=REPO_ROOT,
        timeout=30,
    )
    assert ver_proc.returncode == 0, ver_proc.stderr
    assert f"tg-media-hub v{APP_VERSION}" in ver_proc.stdout


def test_find_available_port_raises_when_scan_exhausted(monkeypatch):
    """端口回退扫描耗尽时必须显式报错, 不得静默返回不可用的 start_port

    回归场景: winnat/Hyper-V 保留段整段覆盖默认端口 (如 8430-8529 覆盖 8500~8519),
    旧实现静默返回 start_port, 直到 uvicorn 绑定才以 winerror 10013 暴露。
    """
    import socket

    from run import find_available_port

    def refuse_bind(self, addr):
        raise OSError(10013, "以一种访问权限不允许的方式做了一个访问套接字的尝试。")

    monkeypatch.setattr(socket.socket, "bind", refuse_bind)
    with pytest.raises(RuntimeError) as exc_info:
        find_available_port("0.0.0.0", 8500)
    message = str(exc_info.value)
    assert "8500" in message
    assert "excludedportrange" in message


def test_resolve_bind_family_matches_host_family(monkeypatch):
    """探测 socket 地址族必须与请求 host 匹配 (IPv6 host 不再全败)"""
    import socket

    from run import _resolve_bind_family

    assert _resolve_bind_family("127.0.0.1") == socket.AF_INET
    assert _resolve_bind_family("::1") == socket.AF_INET6
    assert _resolve_bind_family("::") == socket.AF_INET6

    # 解析失败回退 AF_INET 保持旧行为
    def fail_resolution(*args, **kwargs):
        raise socket.gaierror("mock resolution failure")

    monkeypatch.setattr(socket, "getaddrinfo", fail_resolution)
    assert _resolve_bind_family("not-a-valid-host-name") == socket.AF_INET


# ==============================================================================
# Part: test_natural_sort.py
# ==============================================================================
def test_natural_sort_pure_numbers():
    """纯数字文件名自然排序：1 < 2 < 10 < 20 (ASCII 字典序中 10 会排在 2 前)"""
    files = ["file10.mp4", "file2.mp4", "file1.mp4", "file20.mp4"]
    sorted_files = sorted(files, key=lambda x: (natural_sort_key(x), x))
    assert sorted_files == ["file1.mp4", "file2.mp4", "file10.mp4", "file20.mp4"]


def test_natural_sort_leading_zeros():
    """前导零序列：01 < 02 < 09 < 10"""
    files = ["02.mp4", "10.mp4", "01.mp4", "09.mp4"]
    sorted_files = sorted(files, key=lambda x: (natural_sort_key(x), x))
    assert sorted_files == ["01.mp4", "02.mp4", "09.mp4", "10.mp4"]


def test_natural_sort_mixed_leading_and_non_leading_zeros():
    """混排前导零：相同数值时保证稳定排序，且小于更大数值"""
    files = ["10.mp4", "2.mp4", "01.mp4", "1.mp4", "02.mp4"]
    sorted_files = sorted(files, key=lambda x: (natural_sort_key(x), x))
    # 1 与 01 均小于 2 与 02，且均小于 10
    assert sorted_files.index("10.mp4") == 4
    assert sorted_files[:2] in (["1.mp4", "01.mp4"], ["01.mp4", "1.mp4"])
    assert sorted_files[2:4] in (["2.mp4", "02.mp4"], ["02.mp4", "2.mp4"])


def test_natural_sort_chinese_episodes():
    """中文剧集格式排序：第1集 < 第2集 < 第10集"""
    files = ["第10集.mp4", "第2集.mp4", "第1集.mp4", "第20集.mp4"]
    sorted_files = sorted(files, key=lambda x: (natural_sort_key(x), x))
    assert sorted_files == ["第1集.mp4", "第2集.mp4", "第10集.mp4", "第20集.mp4"]


def test_natural_sort_case_insensitivity():
    """大小写不敏感排序：ep01, EP02, Ep10 满足自然序，而非大写全排在小写前"""
    files = ["ep10.mp4", "EP02.mp4", "Ep01.mp4"]
    sorted_files = sorted(files, key=lambda x: (natural_sort_key(x), x))
    assert sorted_files == ["Ep01.mp4", "EP02.mp4", "ep10.mp4"]


def test_natural_sort_tiebreaker():
    """次级 Tiebreaker 稳定性：同名不同大小写时具备确定性顺序"""
    files = ["file.mp4", "File.mp4"]
    sorted_files = sorted(files, key=lambda x: (natural_sort_key(x), x))
    assert sorted_files == ["File.mp4", "file.mp4"]


def test_natural_sort_safe_none_and_empty():
    """None、空字符串与非字符串入参安全防御，不抛异常"""
    assert natural_sort_key(None) == []
    assert natural_sort_key("") == []
    assert natural_sort_key("   ") != []

    items = ["b.mp4", None, "", "a.mp4"]
    sorted_items = sorted(items, key=lambda x: (natural_sort_key(x), x or ""))
    assert sorted_items == [None, "", "a.mp4", "b.mp4"]


def test_natural_sort_type_error_protection():
    """防跨类型比对异常：确保数字开头的字符串与字母开头的字符串比较时不抛出 TypeError"""
    files = ["10a.mp4", "a10.mp4", "2.mp4", "b.mp4"]
    # 无论正排倒排均不抛出 TypeError: '<' not supported between instances of 'int' and 'str'
    sorted_files = sorted(files, key=natural_sort_key)
    assert len(sorted_files) == 4


def test_natural_sort_unicode_digit_like_no_crash():
    """No 类字形数字 ('²','②','¼') 不崩溃: isdigit() 误报 True 但 int() 拒收 (修复前 ValueError)"""
    files = ["file10", "file²", "file¼", "file②"]
    sorted_files = sorted(files, key=lambda x: (natural_sort_key(x), x))
    assert len(sorted_files) == 4
    assert sorted_files[0] == "file10"  # ASCII 数值路径不受影响
    # No 类字符归入文本块 (类型标记 1), 绝不进入 int() 路径
    assert natural_sort_key("file²") == [(1, "file²")]
    assert natural_sort_key("a②10") == [(1, "a②"), (0, 10, 2)]


def test_natural_sort_fullwidth_digits_sort_numerically():
    """全角数字 (Nd 类, 可被正则切分且 isdecimal=True) 仍走数值排序: １ < ２ < １０"""
    files = ["第１０集", "第２集", "第１集"]
    sorted_files = sorted(files, key=lambda x: (natural_sort_key(x), x))
    assert sorted_files == ["第１集", "第２集", "第１０集"]
    assert natural_sort_key("第１集") == [(1, "第"), (0, 1, 1), (1, "集")]


def test_natural_sort_mixed_unicode_and_ascii_digits():
    """混合场景: 全角/No 类/ASCII 数字同现, 排序确定且无异常 (扩展名选 .flac 避免扩展名数字干扰)"""
    files = ["第②话.flac", "第１话.flac", "第2话.flac", "第10话.flac", "第²话.flac"]
    sorted_files = sorted(files, key=lambda x: (natural_sort_key(x), x))
    assert sorted_files[:3] == ["第１话.flac", "第2话.flac", "第10话.flac"]
    assert len(sorted_files) == 5


# ==============================================================================
# Part: test_intervals.py
# ==============================================================================
# =========================================================================
# merge_intervals
# =========================================================================


def test_merge_overlapping_and_adjacent():
    assert merge_intervals([(1, 10), (11, 20), (30, 40)]) == [(1, 20), (30, 40)]
    assert merge_intervals([(5, 15), (1, 10)]) == [(1, 15)]
    assert merge_intervals([(1, 100), (20, 30)]) == [(1, 100)]
    assert merge_intervals([(1, 5), (7, 9)]) == [(1, 5), (7, 9)]  # 端点差 2 不合并


def test_merge_unsorted_and_duplicates():
    assert merge_intervals([(50, 60), (1, 3), (2, 2), (4, 5)]) == [(1, 5), (50, 60)]
    assert merge_intervals([]) == []
    assert merge_intervals([(10, 1)]) == []  # 非法区间丢弃


# =========================================================================
# compute_gaps
# =========================================================================


def test_gaps_basic_window():
    assert compute_gaps([(10, 20)], 1, 30) == [(1, 9), (21, 30)]
    assert compute_gaps([(1, 30)], 1, 30) == []
    assert compute_gaps([], 1, 30) == [(1, 30)]


def test_gaps_clips_segments_outside_window():
    # 窗口外部分被裁剪，不产生窗口外 gap
    assert compute_gaps([(0, 15), (18, 100)], 10, 20) == [(16, 17)]  # 部分相交被裁剪
    assert compute_gaps([(0, 5), (25, 100)], 10, 20) == [(10, 20)]  # 完全不相交 -> 整窗皆 gap
    assert compute_gaps([(50, 60)], 1, 30) == [(1, 30)]


def test_gaps_multiple_and_adjacent_segments():
    # 相邻段合并后无缝隙
    assert compute_gaps([(1, 10), (11, 20)], 1, 30) == [(21, 30)]
    assert compute_gaps([(1, 10), (15, 20), (25, 25)], 1, 30) == [(11, 14), (21, 24), (26, 30)]


def test_gaps_invalid_window():
    assert compute_gaps([(1, 10)], 10, 5) == []


# =========================================================================
# runs_from_ids
# =========================================================================


def test_runs_consecutive_and_breaks():
    assert runs_from_ids([1, 2, 3, 5, 6, 9]) == [(1, 3), (5, 6), (9, 9)]
    assert runs_from_ids([]) == []


def test_runs_unsorted_and_dedup():
    assert runs_from_ids([7, 5, 6, 5, 1]) == [(1, 1), (5, 7)]


# ==============================================================================
# Part: test_file_hash.py
# ==============================================================================
def test_sha256_streaming_matches_hashlib(tmp_dir):
    # 5MB 随机内容文件, 流式结果必须与一次性 hashlib 一致
    payload = os.urandom(5 * 1024 * 1024)
    path = safe_write_bytes(os.path.join(tmp_dir, "blob.bin"), payload)

    assert sha256_of_file(path) == hashlib.sha256(payload).hexdigest()
    # 强制 1 字节分块, 验证多块流式路径 (禁止全量读入内存的实现将显著变慢但结果一致)
    assert sha256_of_file(path, chunk_size=1) == hashlib.sha256(payload).hexdigest()


def test_download_history_hash_roundtrip(tmp_dir):
    file_path = safe_write_bytes(os.path.join(tmp_dir, "hashed.mp4"), b"content-bytes")

    digest = sha256_of_file(file_path)
    DownloadHistoryRepository.record(
        chat_id="-100h1",
        msg_id=601,
        grouped_id=None,
        file_name="hashed.mp4",
        file_path=file_path,
        file_size=13,
        file_hash=digest,
    )
    assert DownloadHistoryRepository.get_hash_by_path(file_path) == digest

    # 无记录路径返回空串 (发布端回退旧键)
    assert DownloadHistoryRepository.get_hash_by_path(os.path.join(tmp_dir, "nope.mp4")) == ""


def test_publish_dedup_hash_key_upgrade():
    """同路径同大小不同内容不再误判已发布; 存量行 (hash='') 行为不变"""
    target = "-100pub"
    path = "D:/media/same_path.mp4"
    size = 1024

    # 新键语义: 记录 h1 后, 同 size 的 h2 不再命中 (旧实现会误判已发布)
    PublishHistoryRepository.record(target, path, size, file_hash="h1" * 32)
    assert PublishHistoryRepository.is_published(target, path, size, file_hash="h1" * 32)
    assert not PublishHistoryRepository.is_published(target, path, size, file_hash="h2" * 32)

    # 哈希缺失时回退旧三元组键: 对带哈希的行不误判 (哈希不匹配且行非 legacy)
    assert not PublishHistoryRepository.is_published(target, path, size, file_hash="")

    # legacy 行 (hash='') 保持升级前语义: size 匹配即已发布
    legacy_path = "D:/media/legacy.mp4"
    PublishHistoryRepository.record(target, legacy_path, size)  # 未传哈希 -> ''
    assert PublishHistoryRepository.is_published(target, legacy_path, size)
    assert PublishHistoryRepository.is_published(target, legacy_path, size, file_hash="")
    assert PublishHistoryRepository.is_published(
        target, legacy_path, size, file_hash="any"
    )  # 兼容: legacy 行按 size 命中
    assert not PublishHistoryRepository.is_published(target, legacy_path, size + 1)

    # 不同目标频道互不影响
    assert not PublishHistoryRepository.is_published("-100other", path, size, file_hash="h1" * 32)


def test_inspect_folder_includes_file_hash(tmp_dir):
    """inspect 预检展示已下载成品的哈希 (来自 download_history, 未计算为 None)"""
    from app.services.publisher_service import PublisherService

    media_folder = os.path.join(tmp_dir, "pub_media")
    os.makedirs(media_folder, exist_ok=True)

    downloaded = safe_write_bytes(os.path.join(media_folder, "downloaded.mp4"), b"downloaded-content")
    safe_write_bytes(os.path.join(media_folder, "fresh.mp4"), b"fresh-content")

    digest = sha256_of_file(downloaded)
    DownloadHistoryRepository.record(
        chat_id="-100h2",
        msg_id=602,
        grouped_id=None,
        file_name="downloaded.mp4",
        file_path=downloaded,
        file_size=18,
        file_hash=digest,
    )

    res = PublisherService.inspect_folder(media_folder, None)
    by_name = {m["file_name"]: m for m in res["media_files"]}
    assert by_name["downloaded.mp4"]["file_hash"] == digest
    assert by_name["fresh.mp4"]["file_hash"] is None


# ==============================================================================
# Part: test_path_normalization.py
# ==============================================================================
def test_norm_file_path_logic():
    """验证 norm_file_path 工具函数的归一化规则与协议白名单 (兼顾 Linux 与 Windows 路径)"""
    assert norm_file_path("") == ""
    assert norm_file_path("pikpak://folder/sub/vid.mp4") == "pikpak://folder/sub/vid.mp4"

    # 1. POSIX / Linux 格式路径
    assert norm_file_path("/media/movie/ep01.mp4") == "/media/movie/ep01.mp4"
    assert norm_file_path("/media/movie//sub/../ep01.mp4") == "/media/movie/ep01.mp4"

    # 2. Windows 本地路径统一规范化斜杠与盘符小写
    p1 = norm_file_path("D:/media/movie/ep01.mp4")
    p2 = norm_file_path("D:\\media\\movie\\ep01.mp4")
    p3 = norm_file_path("d:/media/movie/ep01.mp4")
    assert p1 == p2 == p3 == "d:/media/movie/ep01.mp4"


def test_publish_history_cross_platform_slash_dedup():
    """验证 PublishHistoryRepository 插入与查重在 Windows 正反斜杠及 Linux 路径下完全互通"""
    target = "-1008881"
    slash_path = "D:/media/library/film.mkv"
    backslash_path = "D:\\media\\library\\film.mkv"
    upper_path = "D:\\MEDIA\\LIBRARY\\FILM.MKV"
    file_size = 5242880

    # 1. 以正斜杠路径记录
    PublishHistoryRepository.record(target, slash_path, file_size)

    # 2. 以反斜杠路径查重必须命中
    assert PublishHistoryRepository.is_published(target, backslash_path, file_size) is True
    # 3. 以大写反斜杠路径查重必须命中 (Windows 忽略大小写)
    assert PublishHistoryRepository.is_published(target, upper_path, file_size) is True
    # 4. 以原始正斜杠路径查重必须命中
    assert PublishHistoryRepository.is_published(target, slash_path, file_size) is True

    # 5. Linux 格式路径查重验证
    linux_path = "/media/library/film.mkv"
    PublishHistoryRepository.record(target, linux_path, file_size)
    assert PublishHistoryRepository.is_published(target, linux_path, file_size) is True


def test_placeholder_slot_cross_platform_slash_match():
    """验证 PlaceholderSlotRepository 插入槽与检索槽在 Windows 及 Linux 路径下完全互通"""
    target = "-1008882"
    slash_path = "C:/downloads/anime/ep01.mkv"
    backslash_path = "C:\\downloads\\anime\\ep01.mkv"
    file_size = 1048576

    # 1. 以正斜杠路径插入槽
    slot_id = PlaceholderSlotRepository.insert_new_slot(
        target,
        slash_path,
        file_size,
        slot_kind="backfillable",
        placeholder_msg_id=77,
        task_id="t_norm_01",
    )
    assert slot_id > 0

    # 2. 以反斜杠路径通过 identity 检索必须命中
    slot = PlaceholderSlotRepository.get_by_identity(target, backslash_path, file_size)
    assert slot is not None
    assert slot["id"] == slot_id
    assert slot["placeholder_msg_id"] == 77

    # 3. 以反斜杠路径尝试 CAS 获取编辑权必须成功
    acquired = PlaceholderSlotRepository.cas_acquire_edit(target, backslash_path, file_size)
    assert acquired is True

    # 4. 批量状态查询验证正反斜杠映射
    states = PlaceholderSlotRepository.states_for_identities(target, [(slash_path, file_size)])
    assert states.get(f"{slash_path}|{file_size}") == "editing"

    # 5. Linux 格式路径占位槽验证
    linux_path = "/downloads/anime/ep01.mkv"
    linux_slot_id = PlaceholderSlotRepository.insert_new_slot(
        target,
        linux_path,
        file_size,
        slot_kind="backfillable",
        placeholder_msg_id=78,
        task_id="t_norm_02",
    )
    assert linux_slot_id > 0
    linux_slot = PlaceholderSlotRepository.get_by_identity(target, linux_path, file_size)
    assert linux_slot is not None
    assert linux_slot["id"] == linux_slot_id


def test_pikpak_virtual_path_preservation():
    """验证 pikpak:// 协议虚拟路径在占位槽与查重中保留原状"""
    target = "-1008883"
    pik_path = "pikpak://P0123/movie.mp4"
    file_size = 2048

    PlaceholderSlotRepository.insert_new_slot(
        target,
        pik_path,
        file_size,
        slot_kind="backfillable",
        placeholder_msg_id=88,
        task_id="t_pik_01",
    )
    slot = PlaceholderSlotRepository.get_by_identity(target, pik_path, file_size)
    assert slot is not None
    assert slot["file_path"] == pik_path


# ==============================================================================
# Part: test_run_uvicorn_kwargs.py
# ==============================================================================
def test_non_windows_enables_uvloop_httptools(monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    kwargs = build_uvicorn_kwargs("10.0.0.1")
    assert kwargs["loop"] == "uvloop"
    assert kwargs["http"] == "httptools"
    assert kwargs["reload"] is False
    assert kwargs["proxy_headers"] is True
    assert kwargs["forwarded_allow_ips"] == "10.0.0.1"
    assert isinstance(kwargs["timeout_graceful_shutdown"], int)
    assert kwargs["workers"] == 1, "必须显式单 worker (防 WEB_CONCURRENCY 多进程绕过单实例锁)"


def test_windows_keeps_default_loop(monkeypatch):
    """Windows 无 uvloop 轮子: 不传 loop/http, 保持默认 asyncio + h11"""
    monkeypatch.setattr(sys, "platform", "win32")
    kwargs = build_uvicorn_kwargs("127.0.0.1")
    assert "loop" not in kwargs
    assert "http" not in kwargs
    assert kwargs["reload"] is False
    assert kwargs["workers"] == 1, "必须显式单 worker"


# ==============================================================================
# Part: test_storage.py
# ==============================================================================
client = TestClient(app, client=("127.0.0.1", 50000))


@pytest.fixture
def download_dir(tmp_dir):
    """将 DOWNLOAD_DIR 指向独立临时子目录 (与 conftest 放在同目录的测试 DB 文件隔离)"""
    original = settings.DOWNLOAD_DIR
    target = os.path.join(tmp_dir, "downloads")
    os.makedirs(target, exist_ok=True)
    settings.DOWNLOAD_DIR = target
    yield target
    settings.DOWNLOAD_DIR = original
    active_download_paths.clear()


def _touch(path: str, size: int = 1024) -> str:
    return safe_write_bytes(path, b"x" * size)


def test_scan_and_register_classification(download_dir):
    # 配对断点 + 孤儿 part + 孤儿 meta + 已登记成品 + 未登记成品
    paired_part = _touch(os.path.join(download_dir, "101_video.mp4.part"), 5000)
    _touch(os.path.join(download_dir, "101_video.mp4.meta"), 100)
    orphan_part = _touch(os.path.join(download_dir, "102_audio.mp3.part"), 3000)
    orphan_meta = _touch(os.path.join(download_dir, "103_img.jpg.meta"), 50)
    final_registered = _touch(os.path.join(download_dir, "104_doc.pdf"), 7000)
    _touch(os.path.join(download_dir, "105_manual.dat"), 900)

    DownloadHistoryRepository.record(
        chat_id="-100chan1",
        msg_id=104,
        grouped_id=None,
        file_name="104_doc.pdf",
        file_path=final_registered,
        file_size=7000,
    )

    overview = StorageService.scan_and_register()

    assert overview["file_count"] == 6
    assert overview["part_count"] == 4  # part + meta
    assert overview["orphan_part_count"] == 1
    assert overview["orphan_meta_count"] == 1
    assert overview["unregistered_final_count"] == 1
    assert overview["total_bytes"] == 5000 + 100 + 3000 + 50 + 7000 + 900
    assert overview["part_bytes"] == 5000 + 100 + 3000 + 50

    # 按频道统计: 已登记成品归入 -100chan1, 未登记归入 None
    channels = {c["chat_id"]: c for c in overview["by_channel"]}
    assert channels["-100chan1"]["count"] == 1
    assert channels["-100chan1"]["bytes"] == 7000
    assert channels.get(None)["count"] == 1

    # files 列表含配对与归属信息
    files = StorageService.list_files(kind="part")
    by_path = {f["path"]: f for f in files}
    assert by_path[paired_part]["paired"] == "ok"
    assert by_path[orphan_part]["paired"] == "missing_meta"
    meta_files = {f["path"]: f for f in StorageService.list_files(kind="meta")}
    assert meta_files[orphan_meta]["paired"] == "missing_part"


def test_delete_guards(download_dir):
    part = _touch(os.path.join(download_dir, "201_a.mp4.part"), 4000)
    meta = _touch(os.path.join(download_dir, "201_a.mp4.meta"), 10)
    final = _touch(os.path.join(download_dir, "202_b.mp4"), 6000)
    outside = _touch(os.path.join(os.path.dirname(download_dir), "outside_secret.txt"), 10)

    DownloadHistoryRepository.record(
        chat_id="-100chan2",
        msg_id=202,
        grouped_id=None,
        file_name="202_b.mp4",
        file_path=final,
        file_size=6000,
    )
    PresetRepository.create("占位-勿删", "placeholder_pub", preset_type="target")  # 确认删除不误伤其他表

    # 守卫①: 路径越界
    result = StorageService.delete_files([outside])
    assert result["deleted"] == []
    assert result["skipped"][0]["reason"].startswith("路径越界")
    assert os.path.exists(outside)

    # 守卫②: 活跃占用登记
    active_download_paths.add(os.path.abspath(part))
    result = StorageService.delete_files([part])
    assert result["deleted"] == []
    assert result["skipped"][0]["reason"].startswith("活跃任务正在使用")
    assert os.path.exists(part)
    active_download_paths.clear()

    # 守卫③: 存在 running 下载任务时断点锁定
    TaskQueueRepository.create("stor_dl_running", "download_link", {"links": []})
    TaskQueueRepository.update_status("stor_dl_running", "running")
    result = StorageService.delete_files([part])
    assert result["deleted"] == []
    assert result["skipped"][0]["reason"].startswith("存在进行中的下载任务")
    assert os.path.exists(part)
    TaskQueueRepository.update_status("stor_dl_running", "completed")

    # 放行后: 删 part 连带 meta; final 联动清 download_history 且不触碰 publish_history
    conn = get_db()
    conn.execute(
        "INSERT INTO publish_history (target_chat_id, file_path, file_size, published_at) VALUES ('-100t', ?, 6000, 0)",
        (final,),
    )
    conn.commit()
    conn.close()

    result = StorageService.delete_files([part, final])
    assert sorted(result["deleted"]) == sorted([part, meta, final])
    assert result["skipped"] == []
    assert not os.path.exists(part)
    assert not os.path.exists(meta)
    assert not os.path.exists(final)
    assert not DownloadHistoryRepository.is_downloaded("-100chan2", 202)

    conn = get_db()
    pub_cnt = conn.execute("SELECT COUNT(*) FROM publish_history WHERE file_path = ?", (final,)).fetchone()[0]
    reg_cnt = conn.execute(
        "SELECT COUNT(*) FROM storage_registry WHERE path IN (?, ?, ?)", (part, meta, final)
    ).fetchone()[0]
    conn.close()
    assert pub_cnt == 1  # publish_history 不动
    assert reg_cnt == 0  # registry 联动清理
    os.remove(outside)


def test_delete_task_with_files_via_api(download_dir):
    task_id = "stor_del_task"
    part = _touch(os.path.join(download_dir, "301_c.mp4.part"), 8000)
    meta = _touch(os.path.join(download_dir, "301_c.mp4.meta"), 10)
    other_part = _touch(os.path.join(download_dir, "302_d.mp4.part"), 2000)  # 其他任务的断点, 不应被误删

    TaskQueueRepository.create(task_id, "download_link", {"links": []})
    TaskQueueRepository.update_status(task_id, "cancelled")
    DownloadBreakpointRepository.upsert(task_id, "-100chan3", 301, part, meta, os.path.join(download_dir, "301_c.mp4"))

    resp = client.delete(f"/api/tasks/{task_id}", params={"delete_files": "true"}, headers=auth_headers())
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "deleted"
    assert sorted(body["files_deleted"]) == sorted([part, meta])
    assert body["files_skipped"] == []
    assert not os.path.exists(part)
    assert not os.path.exists(meta)
    assert os.path.exists(other_part)  # 无关断点原样保留

    # 断点归属行随任务级联清理
    conn = get_db()
    cnt = conn.execute("SELECT COUNT(*) FROM download_breakpoints WHERE task_id = ?", (task_id,)).fetchone()[0]
    conn.close()
    assert cnt == 0
    os.remove(other_part)


def test_delete_files_api_and_scan_api(download_dir):
    orphan = _touch(os.path.join(download_dir, "401_e.mp4.part"), 1000)

    resp = client.post("/api/storage/scan", headers=auth_headers())
    assert resp.status_code == 200
    assert resp.json()["orphan_part_count"] == 1

    resp = client.get("/api/storage/files", params={"kind": "part"}, headers=auth_headers())
    assert resp.status_code == 200
    assert len(resp.json()) == 1

    resp = client.post("/api/storage/delete", json={"paths": [orphan]}, headers=auth_headers())
    assert resp.status_code == 200
    assert resp.json()["deleted"] == [orphan]
    assert not os.path.exists(orphan)

    # 校验入参
    resp = client.post("/api/storage/delete", json={"paths": []}, headers=auth_headers())
    assert resp.status_code == 422


def test_clean_orphans_freed_bytes_counts_only_deleted(download_dir):
    """freed_bytes 只计真实删除的文件: 被活跃登记守卫跳过的孤儿不得虚报释放空间"""
    orphan_deleted = _touch(os.path.join(download_dir, "501_a.mp4.part"), 3000)
    orphan_skipped = _touch(os.path.join(download_dir, "502_b.mp4.part"), 5000)

    task_id = "orphan_acc_task"
    # 仅登记断点行 (不建任务行, 避免 pending 状态触发 clean_orphans 的活跃下载 409 守卫)
    DownloadBreakpointRepository.upsert(
        task_id, "-100chan9", 501, orphan_deleted, orphan_deleted + ".meta", orphan_deleted[:-5]
    )
    DownloadBreakpointRepository.upsert(
        task_id, "-100chan9", 502, orphan_skipped, orphan_skipped + ".meta", orphan_skipped[:-5]
    )
    # 两个孤儿: meta 从未创建 (part 存在) → clean_orphans 候选
    # 孤儿 2 被活跃登记守卫拦下 → 不应计入 freed
    active_download_paths.add(os.path.abspath(orphan_skipped))
    try:
        result = StorageService.clean_orphans()
    finally:
        active_download_paths.clear()

    assert result["cleaned_count"] == 1
    assert result["freed_bytes"] == 3000
    assert result["freed_mb"] == round(3000 / (1024 * 1024), 2)
    assert os.path.exists(orphan_skipped)  # 被守卫跳过, 文件保留
    assert not os.path.exists(orphan_deleted)


def test_breakpoint_repository_lifecycle(download_dir):
    """upsert -> (成功) delete 生命周期; download_single_message 的登记/清除接线由 repository 语义保证"""
    task_id = "stor_bp_life"
    TaskQueueRepository.create(task_id, "download_link", {"links": []})
    DownloadBreakpointRepository.upsert(task_id, "-100c", 501, "p.part", "p.meta", "p")
    DownloadBreakpointRepository.upsert(task_id, "-100c", 501, "p2.part", "p2.meta", "p2")  # REPLACE 幂等
    paths = DownloadBreakpointRepository.get_breakpoint_paths_by_task(task_id)
    assert paths["part"] == ["p2.part"]

    DownloadBreakpointRepository.delete(task_id, "-100c", 501)
    assert DownloadBreakpointRepository.get_breakpoint_paths_by_task(task_id)["part"] == []
    TaskQueueRepository.delete(task_id)


def test_redact_log_filter_preserves_dict_args(monkeypatch):
    """验证 RedactLogFilter 正确处理字典插值、嵌套字典凭证与 query 占位符而不崩溃漏密"""
    from app.core.redact import RedactLogFilter

    monkeypatch.setattr(settings, "PIKPAK_TOKEN", FAKE_TOKEN)
    filter_obj = RedactLogFilter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="URL: %(url)s, ID: %(id)d",
        args={"url": "https://cdn.example.com/file?token=secret123", "id": 42},
        exc_info=None,
    )
    assert filter_obj.filter(record) is True
    assert isinstance(record.args, dict)
    formatted = record.getMessage()
    assert "https://cdn.example.com/file?token=***" in formatted
    assert "secret123" not in formatted
    assert "ID: 42" in formatted

    num_rec = logging.LogRecord(
        "app.services.pikpak_service", logging.WARNING, __file__, 1, "超时 (%.1fs)", (0.3,), None
    )
    assert filter_obj.filter(num_rec) is True
    assert num_rec.args == (0.3,)

    nested_rec = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="url=https://cdn.invalid/?sign=%s ctx=%s",
        args=("sig_secret_val", {"credentials": {"token": FAKE_TOKEN, "url": SIGNED_URL}}),
        exc_info=None,
    )
    assert filter_obj.filter(nested_rec) is True
    nested_msg = nested_rec.getMessage()
    assert "sig_secret_val" not in nested_msg
    assert FAKE_TOKEN not in nested_msg
    assert "FAKESIGN" not in nested_msg
    assert "?sign=***" in nested_msg


def test_redact_text_scrubs_url_userinfo_without_corrupting_normal_urls():
    """验证 URL 中的明文代理认证凭据被有效脱敏，且带端口和 @ 的普通 URL 不被误损"""
    from app.core.redact import redact_text

    raw = "Proxy changed to http://admin:mypassword123@10.0.0.1:8080/path"
    redacted = redact_text(raw)
    assert "mypassword123" not in redacted
    assert "admin:***" in redacted or "***:***" in redacted

    normal = "Visit https://example.com:8500/a:b?to=@user#frag"
    assert redact_text(normal) == normal


def test_failure_logger_redacts_jsonl_records(tmp_path):
    """验证 FailureLogger 写入 JSONL 日志前擦除凭证"""
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    with (
        patch("app.config.settings.LOG_DIR", str(log_dir)),
        patch("app.config.settings.PIKPAK_TOKEN", "my_secret_token"),
    ):
        FailureLogger.close_all()
        logger_file = "fail_test.jsonl"
        FailureLogger.log_failure(
            file_path="video.mp4",
            target_chat_id="-100111",
            error_obj=RuntimeError("Download failed with token=my_secret_token in url https://x.com?sign=secret_sign"),
            client_type="Userbot",
            task_id="t99",
            log_filename=logger_file,
        )
        FailureLogger.close_all()

        content = (log_dir / logger_file).read_text(encoding="utf-8")
        assert "my_secret_token" not in content
        assert "secret_sign" not in content
        assert json.loads(content)["reason"].endswith("?sign=***")


def test_shutdown_logging_drains_full_queue_without_error():
    """验证日志队列已满时 shutdown_logging() 不会因 queue.Full 失败且会复位监听器引用"""
    import queue
    from logging.handlers import QueueListener

    from app.core import log_setup

    full_q: queue.Queue = queue.Queue(maxsize=1)
    full_q.put_nowait("pending-log")
    import threading

    entered = threading.Event()
    release = threading.Event()
    delivered = []

    class CollectHandler(logging.Handler):
        def emit(self, record):
            delivered.append(record)

    class GatedListener(QueueListener):
        def dequeue(self, block):
            entered.set()
            assert release.wait(5)
            return super().dequeue(block)

    listener = GatedListener(full_q, CollectHandler())
    listener.start()
    thread = listener._thread
    assert entered.wait(5)
    prev_cfg = log_setup._configured
    prev_listener = log_setup._queue_listener
    try:
        log_setup._sync_log_module_attrs(True, listener)
        original_wait = log_setup._wait_queue_slot

        def release_during_shutdown(*args, **kwargs):
            assert full_q.full()
            release.set()
            return original_wait(*args, **kwargs)

        with patch.object(log_setup, "_wait_queue_slot", side_effect=release_during_shutdown):
            log_setup.shutdown_logging()
        assert log_setup._queue_listener is None
        assert not thread.is_alive()
        assert delivered == ["pending-log"]
    finally:
        release.set()
        if thread.is_alive():
            listener.stop()
        log_setup._sync_log_module_attrs(prev_cfg, prev_listener)


def test_shutdown_logging_retains_listener_after_failed_stops():
    import queue

    from app.core import log_setup

    listener = MagicMock()
    listener.stop.side_effect = [queue.Full, RuntimeError("stop failed"), None]
    previous = (log_setup._configured, log_setup._queue_listener)
    try:
        log_setup._sync_log_module_attrs(True, listener)
        with patch.object(log_setup, "_wait_queue_slot"):
            log_setup.shutdown_logging()
            assert log_setup._queue_listener is listener
            log_setup.shutdown_logging()
            assert log_setup._queue_listener is None
    finally:
        log_setup._sync_log_module_attrs(*previous)


@pytest.mark.parametrize(
    "reason",
    [
        "x" * 140 + " http://user:password@proxy.example.com",
        "x" * 140 + " my_secret_token",
        "https://x.com?sign=secret_sign\nnext line",
    ],
)
def test_failure_logger_redacts_before_truncation(tmp_path, reason):
    with (
        patch("app.config.settings.LOG_DIR", str(tmp_path)),
        patch("app.config.settings.PIKPAK_TOKEN", "my_secret_token"),
    ):
        FailureLogger.close_all()
        FailureLogger.log_failure("video.mp4", "1", RuntimeError(reason), log_filename="redacted.jsonl")
        FailureLogger.close_all()
        from app.core.redact import redact_text

        expected = redact_text(reason).split("\n")[0][:150]
    entry = json.loads((tmp_path / "redacted.jsonl").read_text())
    assert entry["reason"] == expected


def test_redacting_formatter_scrubs_exception_traceback(monkeypatch):
    """#15: RedactingFormatter 须对包含 exc_info traceback 的完整日志文本脱敏."""
    from app.core.log_setup import RedactingFormatter

    monkeypatch.setattr(settings, "PIKPAK_TOKEN", "tok_secret_tb")
    fmt = RedactingFormatter("%(levelname)s %(message)s")
    try:
        raise RuntimeError("failed at https://cdn.example.com/v?sign=sig123 token=tok_secret_tb")
    except RuntimeError:
        import sys

        exc_info = sys.exc_info()
    rec = logging.LogRecord("root", logging.ERROR, __file__, 1, "boom", None, exc_info)
    rendered = fmt.format(rec)
    assert "sig123" not in rendered
    assert "tok_secret_tb" not in rendered
    assert "***" in rendered


def test_non_blocking_queue_handler_drops_without_stderr_on_full(capsys):
    """#16: _NonBlockingQueueHandler 队列满时静默丢弃并递增计数器, 不调用 handleError 写 stderr."""
    import queue

    from app.core.log_setup import _NonBlockingQueueHandler

    q: queue.Queue = queue.Queue(maxsize=1)
    handler = _NonBlockingQueueHandler(q)
    rec1 = logging.LogRecord("app.t", logging.INFO, __file__, 1, "msg1", None, None)
    rec2 = logging.LogRecord("app.t", logging.INFO, __file__, 1, "secret_msg2 token=abc", None, None)
    handler.emit(rec1)
    handler.emit(rec2)
    assert handler.dropped_count == 1
    captured = capsys.readouterr()
    assert captured.err == ""


def test_validate_safe_folder_path_blocks_run_prefix(monkeypatch):
    """#17: validate_safe_folder_path 须拦截 /run 及子目录 (应对 /var/run -> /run 符号链接)."""
    from app.core.path_utils import validate_safe_folder_path

    monkeypatch.setattr(os.path, "realpath", lambda p: "/run/secrets")
    monkeypatch.setattr(os.path, "exists", lambda p: True)
    monkeypatch.setattr(os.path, "isdir", lambda p: True)
    with pytest.raises(ValueError, match="受限的系统目录"):
        validate_safe_folder_path("/var/run/secrets")


def test_try_claim_executor_entry_uses_cas_from_statuses():
    """#19: try_claim_executor_entry 补写终态时须传 from_statuses 守卫防止覆盖并发取消."""
    from app.core.runtime import active_task_controls, set_task_control, try_claim_executor_entry

    tid = "claim_cas_guard_t1"
    set_task_control(tid, paused=True)
    try:
        with (
            patch("app.db.task_repository.TaskQueueRepository.get_status", return_value="running"),
            patch("app.db.task_repository.TaskQueueRepository.update_status") as m_upd,
        ):
            claimed = try_claim_executor_entry(tid)
            assert claimed is False
            m_upd.assert_called_once_with(tid, "paused", from_statuses=("running",))
    finally:
        active_task_controls.pop(tid, None)


def test_redact_log_filter_handles_namedtuple_and_circular_args():
    """RedactLogFilter 遇到 namedtuple 与自引用循环容器时不得向外抛异常，且须安全脱敏."""
    from collections import namedtuple

    from app.core.redact import RedactLogFilter

    Pair = namedtuple("Pair", ["url", "code"])
    flt = RedactLogFilter()

    nt_rec = logging.LogRecord(
        "test",
        logging.INFO,
        __file__,
        1,
        "item=%s",
        (Pair("https://cdn.example.com/a?token=secret999", 200),),
        None,
    )
    assert flt.filter(nt_rec) is True
    nt_msg = nt_rec.getMessage()
    assert "secret999" not in nt_msg
    assert "token=***" in nt_msg

    cyc: list = ["https://cdn.example.com/b?sign=sig_cyc"]
    cyc.append(cyc)
    cyc_rec = logging.LogRecord("test", logging.INFO, __file__, 1, "cyc=%s", (cyc,), None)
    assert flt.filter(cyc_rec) is True
    cyc_msg = cyc_rec.getMessage()
    assert "sig_cyc" not in cyc_msg
    assert "sign=***" in cyc_msg

    with patch("app.core.redact.redact_payload", side_effect=RuntimeError("boom")):
        err_rec = logging.LogRecord(
            "test", logging.INFO, __file__, 1, "fallback=%s", ("https://cdn.example.com/c?token=tok_fb",), None
        )
        assert flt.filter(err_rec) is True
        err_msg = err_rec.getMessage()
        assert "tok_fb" not in err_msg
        assert "token=***" in err_msg
        assert err_rec.args is None


@pytest.mark.asyncio
async def test_lifespan_single_instance_lock_blocks_second_owner_and_reuses_same_process(tmp_path, monkeypatch):
    lock_file = str(tmp_path / "app.lock")
    monkeypatch.setattr(settings, "LOCK_PATH", lock_file)
    monkeypatch.setattr(settings, "API_ID", 1)
    monkeypatch.setattr(settings, "API_HASH", "dummy")
    monkeypatch.setattr(settings, "BOT_TOKENS", ["1:dummy"])

    # 1. 未被 run.py 加锁时，lifespan 必须持有单实例锁并阻止第二个实例
    with (
        patch("app.main.init_db"),
        patch("app.main.task_scheduler.recover_orphaned_tasks"),
        patch("app.main.task_scheduler.start", new_callable=AsyncMock),
        patch("app.main.task_scheduler.stop", new_callable=AsyncMock),
        patch("app.main.client_manager.initialize", new_callable=AsyncMock),
        patch("app.main.client_manager.disconnect_all", new_callable=AsyncMock),
        patch("app.services.storage_service.StorageService.scan_and_register", return_value={}),
        patch("app.main.shutdown_logging"),
    ):
        async with lifespan(app):
            other = SingleInstanceLock(lock_file)
            with pytest.raises(RuntimeError, match="已有实例正在运行"):
                other.acquire()

    # 2. run.py 已在同进程持有锁时，acquire_process_singleton_lock 静默复用不二次加锁
    outer = SingleInstanceLock(lock_file)
    outer.acquire(port=8500)
    try:
        inner = acquire_process_singleton_lock(lock_file)
        assert inner is None
    finally:
        outer.release()


@pytest.mark.asyncio
async def test_lifespan_exceptional_exit_cleans_up_before_releasing_lock(tmp_path, monkeypatch):
    lock_file = str(tmp_path / "exc.lock")
    monkeypatch.setattr(settings, "LOCK_PATH", lock_file)
    monkeypatch.setattr(settings, "API_ID", 1)
    monkeypatch.setattr(settings, "API_HASH", "dummy")
    monkeypatch.setattr(settings, "BOT_TOKENS", ["1:dummy"])

    events: list[str] = []
    orig_release = SingleInstanceLock.release

    def track_release(self):
        events.append("release")
        return orig_release(self)

    monkeypatch.setattr(SingleInstanceLock, "release", track_release)

    async def stop_spy():
        events.append("stop")

    async def disc_spy():
        events.append("disconnect")

    with (
        patch("app.main.init_db"),
        patch("app.main.task_scheduler.recover_orphaned_tasks"),
        patch("app.main.task_scheduler.start", new_callable=AsyncMock),
        patch("app.main.task_scheduler.stop", side_effect=stop_spy),
        patch("app.main.client_manager.initialize", new_callable=AsyncMock),
        patch("app.main.client_manager.disconnect_all", side_effect=disc_spy),
        patch("app.db.write_pool.drain_db_write_pool", new_callable=AsyncMock),
        patch("app.main.run_wal_checkpoint"),
        patch("app.services.storage_service.StorageService.scan_and_register", return_value={}),
        patch("app.main.shutdown_logging"),
        pytest.raises(RuntimeError, match="boom"),
    ):
        async with lifespan(app):
            raise RuntimeError("boom")

    assert events == ["stop", "disconnect", "release"]


@pytest.mark.asyncio
async def test_lifespan_drain_timeout_cancels_queued_and_waits_active_before_lock_release(tmp_path, monkeypatch):
    lock_file = str(tmp_path / "drain_timeout.lock")
    monkeypatch.setattr(settings, "LOCK_PATH", lock_file)
    monkeypatch.setattr(settings, "API_ID", 1)
    monkeypatch.setattr(settings, "API_HASH", "dummy")
    monkeypatch.setattr(settings, "BOT_TOKENS", ["1:dummy"])

    events: list[str] = []
    orig_release = SingleInstanceLock.release

    def track_release(self):
        events.append("release")
        return orig_release(self)

    async def drain_timeout(*args, **kwargs):
        events.append("drain_timeout")
        return False

    async def cancel_wait():
        events.append("cancel_and_wait")

    def checkpoint_spy():
        events.append("checkpoint")
        return (0, 0, 0)

    monkeypatch.setattr(SingleInstanceLock, "release", track_release)

    with (
        patch("app.main.init_db"),
        patch("app.main.task_scheduler.recover_orphaned_tasks"),
        patch("app.main.task_scheduler.start", new_callable=AsyncMock),
        patch("app.main.task_scheduler.stop", new_callable=AsyncMock),
        patch("app.main.client_manager.initialize", new_callable=AsyncMock),
        patch("app.main.client_manager.disconnect_all", new_callable=AsyncMock),
        patch("app.db.write_pool.drain_db_write_pool", side_effect=drain_timeout),
        patch("app.db.write_pool.cancel_queued_and_wait_active_writes", side_effect=cancel_wait),
        patch("app.main.run_wal_checkpoint", side_effect=checkpoint_spy),
        patch("app.services.storage_service.StorageService.scan_and_register", return_value={}),
        patch("app.main.shutdown_logging"),
    ):
        async with lifespan(app):
            pass

    assert events == ["drain_timeout", "cancel_and_wait", "checkpoint", "release"]


@pytest.mark.asyncio
async def test_lifespan_shield_cancelled_still_waits_for_cleanup_before_lock_release(tmp_path, monkeypatch):
    lock_file = str(tmp_path / "shield_cancel.lock")
    monkeypatch.setattr(settings, "LOCK_PATH", lock_file)
    monkeypatch.setattr(settings, "API_ID", 1)
    monkeypatch.setattr(settings, "API_HASH", "dummy")
    monkeypatch.setattr(settings, "BOT_TOKENS", ["1:dummy"])

    events: list[str] = []
    entered_lifespan = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    orig_release = SingleInstanceLock.release

    def track_release(self):
        events.append("release")
        return orig_release(self)

    async def slow_cleanup(**kwargs):
        events.append("cleanup_start")
        cleanup_started.set()
        await release_cleanup.wait()
        events.append("cleanup_done")

    monkeypatch.setattr(SingleInstanceLock, "release", track_release)

    async def drive_lifespan():
        async with lifespan(app):
            entered_lifespan.set()
            await asyncio.Future()

    with (
        patch("app.main.init_db"),
        patch("app.main.task_scheduler.recover_orphaned_tasks"),
        patch("app.main.task_scheduler.start", new_callable=AsyncMock),
        patch("app.main.client_manager.initialize", new_callable=AsyncMock),
        patch("app.services.storage_service.StorageService.scan_and_register", return_value={}),
        patch("app.main._cleanup_lifespan", side_effect=slow_cleanup),
    ):
        runner = asyncio.create_task(drive_lifespan())
        try:
            async with asyncio.timeout(3):
                await entered_lifespan.wait()
                runner.cancel()
                await cleanup_started.wait()
                runner.cancel()
                await asyncio.sleep(0)
                assert not runner.done() and "release" not in events
                release_cleanup.set()
                done, _ = await asyncio.wait({runner}, timeout=2)
                assert runner in done and runner.cancelled()
        finally:
            release_cleanup.set()
            if not runner.done():
                runner.cancel()
            done, _ = await asyncio.wait({runner}, timeout=3)
            assert runner in done, "lifespan runner did not exit after releasing cleanup"
            if not runner.cancelled():
                runner.result()

    assert events == ["cleanup_start", "cleanup_done", "release"]


def test_load_dotenv_quoted_values_and_inline_comments(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                'TEST_DOTENV_INT="123" # account id',
                "TEST_DOTENV_HASH='abc#def' # trailing comment",
                "TEST_DOTENV_PLAIN=456 # unquoted comment",
                'TEST_DOTENV_ESCAPED="a\\"b#c" # escaped quote',
            ]
        ),
        encoding="utf-8",
    )
    for k in ("TEST_DOTENV_INT", "TEST_DOTENV_HASH", "TEST_DOTENV_PLAIN", "TEST_DOTENV_ESCAPED"):
        monkeypatch.delenv(k, raising=False)

    _load_dotenv(str(env_file))

    assert os.environ["TEST_DOTENV_INT"] == "123"
    assert os.environ["TEST_DOTENV_HASH"] == "abc#def"
    assert os.environ["TEST_DOTENV_PLAIN"] == "456"
    assert os.environ["TEST_DOTENV_ESCAPED"] == 'a\\"b#c'


def test_pikpak_meta_dir_respects_env(monkeypatch):
    import runpy

    custom_dir = "/custom/upload_meta_dir"
    monkeypatch.setenv("PIKPAK_META_DIR", custom_dir)
    ns = runpy.run_path(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "app", "config.py"))
    actual_dir = ns["settings"].PIKPAK_META_DIR
    assert actual_dir == custom_dir


def test_models_validation_contracts():
    # PikPakAuthStatusResponse 一致性校验
    ok = PikPakAuthStatusResponse(logged_in=False, state="not_bound")
    assert ok.logged_in is False

    with pytest.raises(ValidationError):
        PikPakAuthStatusResponse(logged_in=False)  # 默认 state='logged_in' 矛盾

    with pytest.raises(ValidationError):
        PikPakAuthStatusResponse(logged_in=True, state="unreachable")

    # DownloadLinkRequest / DownloadRangeRequest 拒绝 priority=None
    with pytest.raises(ValidationError):
        DownloadLinkRequest(links="https://t.me/c/1/2", priority=None)  # type: ignore[arg-type]

    with pytest.raises(ValidationError):
        DownloadRangeRequest(channel_identifier="-1001", start_id=1, end_id=2, priority=None)  # type: ignore[arg-type]


def test_secure_file_permissions_warns_on_posix_failure(tmp_path, caplog):
    secret_file = tmp_path / "tg_service.db"
    secret_file.write_bytes(b"sqlite")

    with (
        patch("app.config.os.name", "posix"),
        patch("app.config.os.chmod", side_effect=PermissionError("denied")),
        caplog.at_level(logging.WARNING),
    ):
        secure_file_permissions(str(secret_file))

    assert any("设置敏感文件权限失败" in rec.message and rec.levelno == logging.WARNING for rec in caplog.records)
