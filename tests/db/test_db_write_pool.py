"""Database Layer: 专用单写线程池、读写分离、线程连接重用与 PRAGMA 配置测试 (Database Connection & Write Pool)

覆盖范围:
- 原 test_db_write_pool: submit_db_write 单写线程池串行写入、超时控制、优雅排空与异常传播
- 原 test_database: get_db 线程本地连接重用、PRAGMA busy_timeout/foreign_keys/wal 设置与连接生命周期"""

import os
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest
from _safeio import safe_write_bytes

from app.db.download_repository import DownloadHistoryRepository
from app.db.preset_repository import PresetRepository
from app.db.task_repository import TaskQueueRepository
from app.db.write_pool import (
    drain_db_write_pool,
    drain_db_write_pool_sync,
    get_db_write_pool,
    shutdown_db_write_pool,
    shutdown_db_write_pool_sync,
    submit_db_write,
)


# ==============================================================================
# Part: test_db_write_pool.py
# ==============================================================================
@pytest.mark.asyncio
async def test_drain_db_write_pool():
    """验证 drain_db_write_pool 等待所有已提交的写入任务排空完成"""
    completed_items = []

    def slow_write(item_id: int):
        time.sleep(0.05)
        completed_items.append(item_id)

    # 连续提交 3 个写入任务
    submit_db_write(slow_write, 1)
    submit_db_write(slow_write, 2)
    submit_db_write(slow_write, 3)

    # 此时应尚未完全完成
    # 调用 drain
    drained = await drain_db_write_pool(timeout=2.0)
    assert drained is True
    assert completed_items == [1, 2, 3]


@pytest.mark.asyncio
async def test_shutdown_and_self_healing_db_write_pool():
    """验证 shutdown_db_write_pool 安全释放旧池并无缝自愈重建，不阻塞后续任务"""
    items = []

    def quick_write(msg: str):
        items.append(msg)

    submit_db_write(quick_write, "before_shutdown")
    await drain_db_write_pool(timeout=2.0)
    assert items == ["before_shutdown"]

    # 执行关闭
    await shutdown_db_write_pool(timeout=2.0)

    # 自愈验证: 关闭后再次提交写入，应自动拉起新线程池正常工作，绝不报 cannot schedule new futures
    submit_db_write(quick_write, "after_shutdown")
    await drain_db_write_pool(timeout=2.0)
    assert items == ["before_shutdown", "after_shutdown"]


def test_sync_drain_and_shutdown():
    """验证同步版本的 drain_db_write_pool_sync 与 shutdown_db_write_pool_sync"""
    res = drain_db_write_pool_sync(timeout=1.0)
    assert res is True
    shutdown_db_write_pool_sync(timeout=1.0)
    # 验证新池已就绪
    pool = get_db_write_pool()
    assert pool is not None
    assert not getattr(pool, "_shutdown", False)


def test_drain_timeout_and_safe_shutdown():
    """验证当写入任务耗时较长时，drain_db_write_pool_sync 超时返回 False，
    且 shutdown_db_write_pool_sync 配合 cancel_futures 安全释放不产生死锁或永久阻塞。"""
    import threading

    entered = threading.Event()
    release = threading.Event()

    def long_task():
        entered.set()
        release.wait()

    pool = get_db_write_pool()
    try:
        pool.submit(long_task)
        assert entered.wait(timeout=2.0), "后台写入任务未按预期进入执行"

        # 极短超时：应返回 False
        drained = drain_db_write_pool_sync(timeout=0.05)
        assert drained is False

        # 安全关闭旧池并自愈
        shutdown_db_write_pool_sync(timeout=0.05)
        new_pool = get_db_write_pool()
        assert new_pool is not None
        assert not getattr(new_pool, "_shutdown", False)
    finally:
        release.set()


def test_submit_db_write_from_worker_thread_without_loop():
    """验证从无事件循环的纯后台线程调用 submit_db_write 时安全降级执行，不抛 RuntimeError"""
    import threading

    executed = []

    def thread_worker():
        submit_db_write(lambda msg: executed.append(msg), "worker_data")

    t = threading.Thread(target=thread_worker)
    t.start()
    t.join()

    # 排空
    drain_db_write_pool_sync(timeout=2.0)
    assert executed == ["worker_data"]


# ==============================================================================
# Part: test_database.py
# ==============================================================================
def test_preset_repository():
    # 1. 空库不再预插品牌种子预设 (快赢: 移除 "Mypikpakshare" 种子)
    presets = PresetRepository.get_all()
    assert len(presets) == 0

    # 2. 创建新预设 (测试 target 与 source)
    target_preset = PresetRepository.create("测试目标频道", "-100900000009", preset_type="target", title="真实目标频道")
    assert target_preset["name"] == "测试目标频道"
    assert target_preset["channel_id"] == "-100900000009"
    assert target_preset["preset_type"] == "target"
    assert target_preset["title"] == "真实目标频道"

    source_preset = PresetRepository.create("测试源频道", "-100900000046", preset_type="source", title="真实源频道")
    assert source_preset["preset_type"] == "source"

    # 按类型过滤
    targets_only = PresetRepository.get_all(preset_type="target")
    assert any(p["channel_id"] == "-100900000009" for p in targets_only)
    assert not any(p["channel_id"] == "-100900000046" for p in targets_only)

    sources_only = PresetRepository.get_all(preset_type="source")
    assert any(p["channel_id"] == "-100900000046" for p in sources_only)
    assert not any(p["channel_id"] == "-100900000009" for p in sources_only)

    # 3. 删除预设
    PresetRepository.delete(target_preset["id"])
    PresetRepository.delete(source_preset["id"])


def test_download_history_repository(tmp_dir):
    chat_id = "-1009999"
    msg_id = 101

    # 初始未下载
    assert not DownloadHistoryRepository.is_downloaded(chat_id, msg_id)

    # 模拟物理文件
    file_path = safe_write_bytes(os.path.join(tmp_dir, "test_media.mp4"), b"dummy data content")

    # 记录下载
    DownloadHistoryRepository.record(
        chat_id=chat_id,
        msg_id=msg_id,
        grouped_id="group_1",
        file_name="test_media.mp4",
        file_path=file_path,
        file_size=18,
    )

    # 物理文件存在，应返回 True
    assert DownloadHistoryRepository.is_downloaded(chat_id, msg_id)
    recent = DownloadHistoryRepository.get_recent(10)
    assert len(recent) >= 1
    assert recent[0]["file_name"] == "test_media.mp4"

    # 删除物理文件，触发废弃数据库记录自动清理
    os.remove(file_path)
    assert not DownloadHistoryRepository.is_downloaded(chat_id, msg_id)


def test_task_queue_repository():
    task_id = "test_t1"

    # 1. 创建任务
    task = TaskQueueRepository.create(task_id, "download_link", {"links_count": 1})
    assert task["id"] == task_id
    assert task["status"] == "pending"

    # 2. 查询任务
    t_fetched = TaskQueueRepository.get_by_id(task_id)
    assert t_fetched is not None
    assert t_fetched["params"]["links_count"] == 1

    # 3. 更新进度与状态
    TaskQueueRepository.update_progress(task_id, 500, 1000, 1, 2, 2.5, 10.0, "video.mp4")
    TaskQueueRepository.update_status(task_id, "running")
    TaskQueueRepository.append_log(task_id, "下载进度 50%")

    t_updated = TaskQueueRepository.get_by_id(task_id)
    assert t_updated["status"] == "running"
    assert t_updated["progress_bytes"] == 500
    assert t_updated["total_bytes"] == 1000
    assert t_updated["processed_items"] == 1
    assert t_updated["total_items"] == 2
    assert t_updated["speed_mb_s"] == 2.5
    # 日志不经 get_by_id 内嵌, 一律差量读取
    logs_rows, _, _ = TaskQueueRepository.get_task_logs(task_id, 0, 100)
    assert len(logs_rows) == 1
    assert logs_rows[0]["message"] == "下载进度 50%"
    assert logs_rows[0]["seq"] == 1

    # 4. 测试孤儿任务恢复 (统一走调度器实现, P1: 仓储重复版 cleanup_orphaned_tasks 已删)
    from app.core.scheduler import task_scheduler
    from app.db.account_repository import SystemSettingsRepository

    SystemSettingsRepository.set("auto_resume_tasks_on_startup", "true")
    try:
        task_scheduler.recover_orphaned_tasks()
    finally:
        SystemSettingsRepository.set("auto_resume_tasks_on_startup", "false")
    orphan_logs, _, _ = TaskQueueRepository.get_task_logs(task_id, 0, 100)
    assert any("服务启动自动恢复" in l["message"] for l in orphan_logs)
    assert TaskQueueRepository.get_by_id(task_id)["status"] == "queued"

    # 5. 测试清理已完成/已取消任务 (置终态后清理)
    TaskQueueRepository.update_status(task_id, "cancelled")
    TaskQueueRepository.clear_finished()
    assert TaskQueueRepository.get_by_id(task_id) is None


def test_publish_history_repository():
    from app.db.publish_repository import PublishHistoryRepository

    target_chat = "-100900006"
    file_path = "D:/media/test_pub.mp4"
    file_size = 1048576

    assert not PublishHistoryRepository.is_published(target_chat, file_path, file_size)

    PublishHistoryRepository.record(target_chat, file_path, file_size)

    assert PublishHistoryRepository.is_published(target_chat, file_path, file_size)
    assert not PublishHistoryRepository.is_published("-100900051", file_path, file_size)
    assert not PublishHistoryRepository.is_published(target_chat, file_path, 2048)


def test_submit_db_write_retries_after_concurrent_pool_shutdown():
    closed_pool = ThreadPoolExecutor(max_workers=1)
    closed_pool.shutdown(wait=True)
    fresh_pool = ThreadPoolExecutor(max_workers=1)
    ran: list[str] = []

    try:
        with patch("app.db.write_pool.get_db_write_pool", side_effect=[closed_pool, fresh_pool]):
            submit_db_write(ran.append, "recovered")
        future = fresh_pool.submit(lambda: None)
        future.result(timeout=2.0)
        assert ran == ["recovered"]
    finally:
        fresh_pool.shutdown(wait=True)
        drain_db_write_pool_sync(timeout=1.0)
