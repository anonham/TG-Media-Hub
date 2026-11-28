"""Database Layer: Baseline-Only Schema 治理与 WAL 自保检查点测试 (DB Baseline & WAL Checkpoint)

迁移链全坍缩, `_create_baseline_tables` 为唯一 schema 真源,
`PRAGMA user_version` 单值语义 (必须等于 LATEST_VERSION, 否则拒启)。

覆盖范围:
- Baseline 结构断言: 全新库直建全部表列与索引 (含 idx_download_history_path)
- 版本治理: 旧版本库 / 存量无版本库 / 版本超前库一律 CRITICAL 拒启
- 幂等: 重复 init_db 不重复建索引不副作用
- 原 test_wal_checkpoint: SQLite WAL 检查点生命周期、体积告警、被动检查点与应用关闭 flush"""

import asyncio
import logging
import os
import sqlite3
import threading
import time
from unittest.mock import patch

import pytest
from _async_wait import wait_until

from app.config import settings
from app.db.database import (
    LATEST_VERSION,
    close_all_thread_connections,
    close_thread_cached_connection,
    db_cursor,
    get_db,
    init_db,
    run_wal_checkpoint,
)
from app.db.task_repository import TaskQueueRepository
from app.main import _wal_checkpoint_loop, app, lifespan


# ==============================================================================
# Part: Baseline 结构与版本治理
# ==============================================================================
def test_fresh_database_initialized_to_latest_version(tmp_dir):
    """全新库初始化后 user_version 直接为 LATEST_VERSION, 且具备完整表和索引 (唯一 schema 真源)"""
    db_file = os.path.join(tmp_dir, "fresh.db")
    settings.DB_PATH = db_file
    close_thread_cached_connection()

    init_db()

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA user_version")
    ver = cursor.fetchone()[0]
    assert ver == LATEST_VERSION == 1

    cursor.execute("PRAGMA table_info(task_queue)")
    cols = {row["name"] for row in cursor.fetchall()}
    for expected_col in [
        "concurrency",
        "dc_id",
        "processed_items",
        "total_items",
        "concurrency_total",
        "account_id",
        "priority",
        "retry_count",
    ]:
        assert expected_col in cols, f"全新库缺少列: {expected_col}"
    assert "progress_unit" not in cols, "全新库不得包含废弃列 progress_unit"
    assert "log_messages" not in cols, "全新库不得包含废弃列 log_messages"

    # 验证索引就绪 (含发布防重点查键路径索引)
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
    indexes = {row["name"] for row in cursor.fetchall()}
    assert "idx_task_queue_sched" in indexes
    assert "idx_task_queue_account" in indexes
    assert "idx_download_history_path" in indexes
    assert "idx_publish_hash" in indexes
    assert "idx_publish_pikpak_dedup" in indexes
    assert "idx_placeholder_slots_task" in indexes
    assert "idx_placeholder_slots_state" in indexes

    # 全部基线表就绪
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row["name"] for row in cursor.fetchall()}
    for expected_table in [
        "channel_presets",
        "download_history",
        "download_history_reuse",
        "task_queue",
        "task_logs",
        "publish_history",
        "download_breakpoints",
        "storage_registry",
        "forward_history",
        "forward_skips",
        "forward_coverage",
        "tg_accounts",
        "tg_bots",
        "system_settings",
        "placeholder_slots",
    ]:
        assert expected_table in tables, f"全新库缺少基线表: {expected_table}"

    # 验证 tg_accounts.max_concurrency 基线默认值为 1，且 download_parallel_connections 种子默认为 8
    cursor.execute("PRAGMA table_info(tg_accounts)")
    acc_cols = {row["name"]: row for row in cursor.fetchall()}
    assert "max_concurrency" in acc_cols
    assert str(acc_cols["max_concurrency"]["dflt_value"]) == "1"

    cursor.execute("SELECT value FROM system_settings WHERE key = 'download_parallel_connections'")
    dl_conn_row = cursor.fetchone()
    assert dl_conn_row is not None
    assert dl_conn_row["value"] == "8"


def test_stale_version_database_refuses_startup(tmp_dir):
    """Baseline-Only 拒启语义: 旧版本库 / 存量无版本库 / 版本超前库 → RuntimeError 快速失败,
    且不得篡改既有库 (不建表、不推进版本)"""
    scenarios = [
        ("v0_with_tables", 0, True),  # 存量无版本库 (历史遗留)
        ("v6_legacy", 6, False),  # 旧迁移链时代的版本号
        ("v99_future", 99, False),  # 未知未来版本
    ]
    for name, stale_ver, with_tables in scenarios:
        db_file = os.path.join(tmp_dir, f"stale_{name}.db")
        settings.DB_PATH = db_file
        close_thread_cached_connection()

        if with_tables:
            raw = sqlite3.connect(db_file)
            raw.execute("CREATE TABLE task_queue (id TEXT PRIMARY KEY, status TEXT)")
            raw.commit()
            raw.close()

        raw = sqlite3.connect(db_file)
        raw.execute(f"PRAGMA user_version = {stale_ver}")
        raw.commit()
        raw.close()

        with pytest.raises(RuntimeError) as exc_info:
            init_db()
        assert "schema 版本不兼容" in str(exc_info.value), f"场景 {name} 应拒启"

        # 拒启路径不得篡改库: 版本不变; 无表场景不得凭空建基线表
        raw = sqlite3.connect(db_file)
        ver_now = raw.execute("PRAGMA user_version").fetchone()[0]
        assert ver_now == stale_ver, f"场景 {name} 版本被篡改"
        table_count = raw.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchone()[0]
        if not with_tables:
            assert table_count == 0, f"场景 {name} 拒启时不应创建任何表"
        raw.close()


def test_repeated_init_db_idempotent(tmp_dir):
    """数据库已处于基线版本时, 重复调用 init_db 幂等无副作用"""
    db_file = os.path.join(tmp_dir, "idempotent.db")
    settings.DB_PATH = db_file
    close_thread_cached_connection()

    init_db()

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA user_version")
    assert cursor.fetchone()[0] == LATEST_VERSION

    # 再次调用，不应抛出任何错误，且版本保持最新
    init_db()
    cursor.execute("PRAGMA user_version")
    assert cursor.fetchone()[0] == LATEST_VERSION


def test_fresh_db_contains_pikpak_publish_columns(tmp_dir):
    """全新库直建即含 PikPak 防重 3 列 + 部分唯一索引 (baseline DDL 唯一真源)"""
    db_file = os.path.join(tmp_dir, "fresh_publish.db")
    settings.DB_PATH = db_file
    close_thread_cached_connection()

    init_db()

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA user_version")
    assert cursor.fetchone()[0] == LATEST_VERSION

    cursor.execute("PRAGMA table_info(publish_history)")
    cols = {row["name"] for row in cursor.fetchall()}
    for col in ("source_type", "remote_file_id", "tg_message_id", "task_id"):
        assert col in cols, f"全新库 publish_history 缺少列: {col}"

    cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND name='idx_publish_pikpak_dedup'")
    row = cursor.fetchone()
    assert row is not None, "全新库缺少部分唯一索引 idx_publish_pikpak_dedup"
    assert "WHERE source_type='pikpak'" in row["sql"], "索引必须为部分索引 (仅 pikpak 行参与防重)"


def test_publish_partial_index_idempotent_on_reinit(tmp_dir):
    """重复 init_db 幂等 —— 部分唯一索引不重复创建"""
    db_file = os.path.join(tmp_dir, "idempotent_publish.db")
    settings.DB_PATH = db_file
    close_thread_cached_connection()

    init_db()
    init_db()

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA user_version")
    assert cursor.fetchone()[0] == LATEST_VERSION

    cursor.execute("PRAGMA index_list(publish_history)")
    dedup_idx = [row for row in cursor.fetchall() if row["name"] == "idx_publish_pikpak_dedup"]
    assert len(dedup_idx) == 1, "部分唯一索引必须只存在一份 (IF NOT EXISTS 幂等)"


def test_pikpak_dedup_partial_index(tmp_dir):
    """部分索引行为——同 (target, remote_file_id) 第二条 pikpak 行冲突 (裸 SQL 验证索引本身);
    不同 remote_file_id 共存; 多条 local 行 ('') 共存不受影响; record() 走 INSERT OR REPLACE 幂等覆盖"""
    db_file = os.path.join(tmp_dir, "partial_idx.db")
    settings.DB_PATH = db_file
    close_thread_cached_connection()
    init_db()

    raw_conn = sqlite3.connect(db_file)
    raw_conn.execute(
        "INSERT INTO publish_history (target_chat_id, file_path, file_size, published_at, source_type, remote_file_id) "
        "VALUES ('-100123', 'pikpak://tv/movie.mp4', 1073741824, 1700000000.0, 'pikpak', 'abc123')"
    )
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
        raw_conn.execute(
            "INSERT INTO publish_history (target_chat_id, file_path, file_size, published_at, source_type, remote_file_id) "
            "VALUES ('-100123', 'pikpak://tv/other.mp4', 999, 1700000001.0, 'pikpak', 'abc123')"
        )
    raw_conn.execute(
        "INSERT INTO publish_history (target_chat_id, file_path, file_size, published_at, source_type, remote_file_id) "
        "VALUES ('-100123', 'pikpak://tv/second.mp4', 555, 1700000002.0, 'pikpak', 'def456')"
    )
    for i in range(3):
        raw_conn.execute(
            "INSERT INTO publish_history (target_chat_id, file_path, file_size, published_at, source_type, remote_file_id) "
            "VALUES (?, ?, ?, ?, 'local', '')",
            ("-200456", f"local_{i}.mp4", 1000 + i, 1700000003.0),
        )
    raw_conn.commit()
    raw_conn.close()

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) AS n FROM publish_history WHERE source_type='pikpak'")
    assert cursor.fetchone()["n"] == 2, "不同 remote_file_id 的 pikpak 行必须共存"
    cursor.execute("SELECT COUNT(*) AS n FROM publish_history WHERE source_type='local'")
    assert cursor.fetchone()["n"] == 3, "多条 local 行 (remote_file_id='') 不受部分索引影响"

    # record() 的 INSERT OR REPLACE 幂等覆盖: 同键重复记录行数不增
    from app.db.publish_repository import PublishHistoryRepository

    PublishHistoryRepository.record(
        "-100123", "pikpak://tv/movie.mp4", 1073741824, source_type="pikpak", remote_file_id="abc123", tg_message_id=42
    )
    PublishHistoryRepository.record(
        "-100123", "pikpak://tv/movie.mp4", 1073741824, source_type="pikpak", remote_file_id="abc123", tg_message_id=43
    )
    cursor.execute("SELECT COUNT(*) AS n FROM publish_history WHERE source_type='pikpak' AND remote_file_id='abc123'")
    assert cursor.fetchone()["n"] == 1, "INSERT OR REPLACE 必须幂等覆盖"
    cursor.execute("SELECT tg_message_id FROM publish_history WHERE source_type='pikpak' AND remote_file_id='abc123'")
    assert cursor.fetchone()["tg_message_id"] == 43


def test_repository_pikpak_record_and_dedup(tmp_dir):
    """repository 双路径——pikpak 按 (target, remote_file_id) 查重; 默认参数 local 行为与升级前一致"""
    db_file = os.path.join(tmp_dir, "repo_publish.db")
    settings.DB_PATH = db_file
    close_thread_cached_connection()
    init_db()

    from app.db.publish_repository import PublishHistoryRepository

    assert not PublishHistoryRepository.is_published(
        "-100123", "pikpak://tv/movie.mp4", 1073741824, source_type="pikpak", remote_file_id="abc123"
    )
    PublishHistoryRepository.record(
        "-100123", "/cloud/tv/movie.mp4", 1073741824, source_type="pikpak", remote_file_id="abc123", tg_message_id=777
    )
    assert PublishHistoryRepository.is_published(
        "-100123", "pikpak://tv/movie.mp4", 1073741824, source_type="pikpak", remote_file_id="abc123"
    )
    assert not PublishHistoryRepository.is_published(
        "-100123", "pikpak://tv/movie.mp4", 1073741824, source_type="pikpak", remote_file_id="other"
    ), "不同 remote_file_id 不得误判已发布"
    assert not PublishHistoryRepository.is_published(
        "-999", "pikpak://tv/movie.mp4", 1073741824, source_type="pikpak", remote_file_id="abc123"
    ), "不同 target_chat_id 不得误判已发布"

    # 默认参数 local 路径: 与既有行为一致 (file_path+size 三元组 OR 查重, 不加 source_type 过滤)
    PublishHistoryRepository.record("-200456", "downloads/local.mp4", 4096)
    assert PublishHistoryRepository.is_published("-200456", "downloads/local.mp4", 4096)
    assert not PublishHistoryRepository.is_published("-200456", "downloads/local.mp4", 8192)
    # 路径/大小完全不同的 local 查询不受 pikpak 行影响 (同三元组命中属既有 OR 语义, 非污染)
    assert not PublishHistoryRepository.is_published("-100123", "/cloud/tv/other.mp4", 1073741824)

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT tg_message_id FROM publish_history WHERE target_chat_id='-100123' AND remote_file_id='abc123'"
    )
    assert cursor.fetchone()["tg_message_id"] == 777


def test_placeholder_slots_fresh_database(tmp_dir):
    """全新库直建 placeholder_slots (表与双索引就绪, user_version=LATEST)"""
    db_file = os.path.join(tmp_dir, "placeholder_fresh.db")
    settings.DB_PATH = db_file
    close_thread_cached_connection()

    init_db()

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA user_version")
    assert cursor.fetchone()[0] == LATEST_VERSION == 1
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='placeholder_slots'")
    assert cursor.fetchone(), "全新库必须直建 placeholder_slots"
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
    idx = {row["name"] for row in cursor.fetchall()}
    assert {"idx_placeholder_slots_task", "idx_placeholder_slots_state"} <= idx


def test_placeholder_slot_unique_identity(tmp_dir):
    """身份键 UNIQUE 三元组 —— 同 (chat, path, size) 冲突, 异 chat/异 size 共存"""
    db_file = os.path.join(tmp_dir, "placeholder_unique.db")
    settings.DB_PATH = db_file
    close_thread_cached_connection()
    init_db()

    from app.db.publish_repository import PlaceholderSlotRepository as Slots

    Slots.insert_new_slot("-100123", "/tv/e01.mkv", 12345, task_id="t1", placeholder_msg_id=55)
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
        Slots.insert_new_slot("-100123", "/tv/e01.mkv", 12345, task_id="t2", placeholder_msg_id=66)
    Slots.insert_new_slot("-100123", "/tv/e01.mkv", 99999, task_id="t1", placeholder_msg_id=55)
    Slots.insert_new_slot("-200456", "/tv/e01.mkv", 12345, task_id="t1", placeholder_msg_id=55)


def test_task_queue_progress_columns_fresh_database(tmp_dir):
    """全新库 task_queue 包含 processed_items / total_items, 且无 progress_unit / log_messages 废弃列"""
    db_file = os.path.join(tmp_dir, "progress_fresh.db")
    settings.DB_PATH = db_file
    close_thread_cached_connection()

    init_db()

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA user_version")
    assert cursor.fetchone()[0] == LATEST_VERSION == 1

    cursor.execute("PRAGMA table_info(task_queue)")
    cols = {row["name"] for row in cursor.fetchall()}
    assert "processed_items" in cols
    assert "total_items" in cols
    assert "progress_unit" not in cols
    assert "log_messages" not in cols


def test_task_logs_prune_retention(tmp_dir):
    """启动期日志保留修剪: 每任务仅保留最新 N 条 (按 id 降序窗口), 分任务独立生效"""
    db_file = os.path.join(tmp_dir, "log_retention.db")
    settings.DB_PATH = db_file
    close_thread_cached_connection()
    init_db()

    TaskQueueRepository.create("prune_a", "download_link", {"links": ["https://t.me/x/1"]})
    TaskQueueRepository.create("prune_b", "download_link", {"links": ["https://t.me/x/2"]})
    for i in range(10):
        TaskQueueRepository.append_log("prune_a", f"a-{i}")
    for i in range(3):
        TaskQueueRepository.append_log("prune_b", f"b-{i}")

    removed = TaskQueueRepository.prune_task_logs(3)

    assert removed == 7, "仅 prune_a 超限 7 条被清理"
    logs_a, _, _ = TaskQueueRepository.get_task_logs("prune_a", 0, 100)
    assert [l["message"] for l in logs_a] == ["a-7", "a-8", "a-9"], "保留最新 3 条"
    logs_b, _, _ = TaskQueueRepository.get_task_logs("prune_b", 0, 100)
    assert len(logs_b) == 3, "未超限任务不受影响"
    # 幂等: 再次修剪零删除
    assert TaskQueueRepository.prune_task_logs(3) == 0


# ==============================================================================
# Part: test_wal_checkpoint.py
# ==============================================================================
def test_wal_checkpoint_basic_passive(tmp_path):
    """验证基础 PASSIVE 检查点成功移交 WAL 帧"""
    db_path = str(tmp_path / "test_wal.db")
    init_db(db_path)

    # 写入一些数据以在 WAL 中产生帧
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO channel_presets (name, channel_id, created_at) VALUES ('test', 'ch1', ?)", (time.time(),)
        )
        conn.commit()
    finally:
        conn.close()

    busy, log, checkpointed = run_wal_checkpoint(db_path)
    assert busy == 0
    assert isinstance(log, int)
    assert isinstance(checkpointed, int)


def test_wal_checkpoint_nonexistent_db(tmp_path):
    """验证不存在的数据库路径安全返回 (0, 0, 0)"""
    db_path = str(tmp_path / "nonexistent.db")
    busy, log, checkpointed = run_wal_checkpoint(db_path)
    assert (busy, log, checkpointed) == (0, 0, 0)


def test_wal_checkpoint_size_warning(tmp_path, monkeypatch, caplog):
    """验证 WAL 文件体积超过阈值时触发警告日志"""
    db_path = str(tmp_path / "test_size.db")
    init_db(db_path)

    # 将阈值设为极小值 (50 字节)
    monkeypatch.setattr("app.db.database.WAL_SIZE_WARNING_THRESHOLD_BYTES", 50)

    # 写入足够数据使 WAL 产生且大于 50 字节
    conn = sqlite3.connect(db_path)
    try:
        for i in range(10):
            conn.execute(
                "INSERT INTO channel_presets (name, channel_id, created_at) VALUES (?, ?, ?)",
                (f"preset_{i}", f"ch_{i}", time.time()),
            )
        conn.commit()
    finally:
        conn.close()

    wal_path = f"{db_path}-wal"
    if os.path.exists(wal_path) and os.path.getsize(wal_path) > 50:
        with caplog.at_level(logging.WARNING):
            run_wal_checkpoint(db_path)
        assert any("SQLite WAL 文件体积已达" in record.message for record in caplog.records)


def test_wal_checkpoint_busy_handling(tmp_path, monkeypatch, caplog):
    """验证遇到 busy=1 时正常返回并记录 debug 日志"""
    db_path = str(tmp_path / "test_busy.db")
    init_db(db_path)

    # 模拟 PRAGMA wal_checkpoint 返回 busy=1
    class MockCursor:
        def execute(self, sql):
            pass

        def fetchone(self):
            return (1, 100, 20)

        def close(self):
            pass

    class MockConn:
        def cursor(self):
            return MockCursor()

        def close(self):
            pass

    monkeypatch.setattr(sqlite3, "connect", lambda *a, **kw: MockConn())

    with caplog.at_level(logging.DEBUG, logger="app.db.database"):
        busy, log, checkpointed = run_wal_checkpoint(db_path)

    assert busy == 1
    assert log == 100
    assert checkpointed == 20
    assert any("遇到活跃读写事务被跳过" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_wal_checkpoint_loop_lifecycle(monkeypatch):
    """验证后台定期 checkpoint 协程在 sleep 期间可干净取消"""
    checkpoint_called = []

    def mock_checkpoint():
        checkpoint_called.append(True)
        return (0, 0, 0)

    monkeypatch.setattr("app.main.run_wal_checkpoint", mock_checkpoint)
    monkeypatch.setattr("app.main.WAL_CHECKPOINT_INTERVAL_SECONDS", 0.05)

    task = asyncio.create_task(_wal_checkpoint_loop())
    await wait_until(lambda: len(checkpoint_called) >= 1, message="WAL checkpoint loop did not tick")
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert task.done()
    assert len(checkpoint_called) >= 1


@pytest.mark.asyncio
async def test_lifespan_final_wal_flush(tmp_path, monkeypatch):
    """验证 lifespan 退出阶段执行最终 WAL 刷盘检查点"""
    db_path = str(tmp_path / "lifespan_test.db")
    monkeypatch.setattr("app.config.settings.DB_PATH", db_path)

    final_flush_called = []
    from app.db import database

    orig_checkpoint = database.run_wal_checkpoint

    def tracking_checkpoint(path=None):
        final_flush_called.append(path or db_path)
        return orig_checkpoint(path)

    monkeypatch.setattr("app.main.run_wal_checkpoint", tracking_checkpoint)
    monkeypatch.setattr("app.main.WAL_CHECKPOINT_INTERVAL_SECONDS", 9999.0)

    # 屏蔽 client_manager 与 scheduler 的真实启动/断开
    from app.core.client_manager import client_manager
    from app.core.scheduler import task_scheduler

    async def noop_async():
        pass

    monkeypatch.setattr(client_manager, "initialize", noop_async)
    monkeypatch.setattr(client_manager, "disconnect_all", noop_async)
    monkeypatch.setattr(task_scheduler, "start", noop_async)
    monkeypatch.setattr(task_scheduler, "stop", noop_async)
    monkeypatch.setattr(task_scheduler, "recover_orphaned_tasks", lambda: None)

    try:
        async with lifespan(app):
            pass
    finally:
        task_scheduler.is_running = False

    assert len(final_flush_called) >= 1


def test_close_all_thread_connections_allows_surviving_worker_recovery():
    errors: list[Exception] = []
    ready = threading.Event()
    proceed = threading.Event()

    def worker():
        try:
            with db_cursor() as cursor:
                cursor.execute("SELECT 1")
            ready.set()
            proceed.wait(timeout=5.0)
            with db_cursor() as cursor:
                cursor.execute("SELECT 1")
                assert cursor.fetchone()[0] == 1
        except Exception as exc:
            errors.append(exc)

    t = threading.Thread(target=worker)
    t.start()
    try:
        assert ready.wait(timeout=5.0)
        close_all_thread_connections()
    finally:
        proceed.set()
        t.join(timeout=5.0)

    assert not t.is_alive()
    assert not errors


def test_init_db_rolls_back_tables_if_baseline_fails(tmp_dir):
    db_file = os.path.join(tmp_dir, "atomic_init_fail.db")

    def _create_and_boom(cursor):
        cursor.execute("CREATE TABLE partial_baseline (id INTEGER PRIMARY KEY)")
        raise RuntimeError("ddl boom")

    with (
        patch("app.db.database._create_baseline_tables", side_effect=_create_and_boom),
        pytest.raises(RuntimeError, match="ddl boom"),
    ):
        init_db(db_path=db_file)

    raw = sqlite3.connect(db_file)
    try:
        ver = raw.execute("PRAGMA user_version").fetchone()[0]
        cnt = raw.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchone()[0]
        assert ver == 0
        assert cnt == 0
    finally:
        raw.close()

    # 恢复后可正常初始化至 LATEST_VERSION
    init_db(db_path=db_file)
    raw = sqlite3.connect(db_file)
    try:
        assert raw.execute("PRAGMA user_version").fetchone()[0] == LATEST_VERSION
    finally:
        raw.close()
