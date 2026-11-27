"""仓储拆域等价与各域仓储边界行为测试 (docs/architecture/DATABASE_AND_STORAGE.md §3.2)。"""

import importlib
import os
import stat as stat_mod
import time
from unittest.mock import patch

import pytest

import app.db as db_pkg
from app.db import write_pool
from app.db.database import db_cursor, get_db
from app.db.download_repository import DownloadBreakpointRepository, DownloadHistoryRepository
from app.db.forward_repository import ForwardHistoryRepository, ForwardLedgerRepository
from app.db.preset_repository import PresetRepository
from app.db.publish_repository import PlaceholderSlotRepository, PublishHistoryRepository, norm_file_path
from app.db.task_repository import TASK_QUEUE_ORDER_SQL, TaskQueueRepository
from app.services.storage_service import StorageService


def test_domain_modules_export_full_surface():
    """调用方直翻新模块: app.db 包聚合导出与具名模块为同一对象; repository 旧模块已删除"""
    assert db_pkg.TaskQueueRepository is TaskQueueRepository
    assert db_pkg.PresetRepository is PresetRepository
    assert db_pkg.DownloadHistoryRepository is DownloadHistoryRepository
    assert db_pkg.DownloadBreakpointRepository is DownloadBreakpointRepository
    assert db_pkg.PublishHistoryRepository is PublishHistoryRepository
    assert db_pkg.PlaceholderSlotRepository is PlaceholderSlotRepository
    assert db_pkg.ForwardHistoryRepository is ForwardHistoryRepository
    assert db_pkg.ForwardLedgerRepository is ForwardLedgerRepository
    assert db_pkg.norm_file_path is not None
    assert db_pkg.submit_db_write is write_pool.submit_db_write

    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("app.db.repository")


def _seed_task_with_full_associations(task_id: str, status: str, mid: int):
    """造一个带全套关联行 (logs/breakpoints/forward_history/skips/storage_registry/占位槽) 的终态任务。
    mid = 独立源消息 ID (forward_history UNIQUE 三键使不同任务种子不得共用 msg_id, 防止互相覆盖)"""
    TaskQueueRepository.create(task_id, "forward_channel", {"source_chat": "-1001"}, status=status)
    TaskQueueRepository.append_log(task_id, f"{task_id} line-1")
    TaskQueueRepository.append_log(task_id, f"{task_id} line-2")
    DownloadBreakpointRepository.upsert(task_id, "-1001", mid, f"{task_id}.part", f"{task_id}.meta", f"{task_id}.final")
    ForwardHistoryRepository.save_forward_batch([(task_id, "-1001", mid, "-1002", mid * 10)])
    ForwardLedgerRepository.commit_batch_records(
        task_id,
        "-1001",
        "-1002",
        mappings=[],
        skips=[(mid, "bad_id", "媒体缺失", None)],
    )
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO storage_registry (path, kind, size, mtime, task_id, scanned_at) VALUES (?, ?, ?, ?, ?, ?)",
        (f"downloads/{task_id}.mp4", "video", 123, 0.0, task_id, 0.0),
    )
    conn.commit()
    conn.close()
    slot_terminal = PlaceholderSlotRepository.insert_new_slot(
        "-1002",
        f"pikpak://folder/{task_id}",
        456,
        task_id=task_id,
        state="placeholder_sent",
    )
    PlaceholderSlotRepository.mark_published(slot_terminal, 777)
    PlaceholderSlotRepository.insert_new_slot(
        "-1002",
        f"pikpak://folder/{task_id}_active",
        789,
        task_id=task_id,
        state="placeholder_sent",
    )


def _snapshot_associations(task_id: str, mid: int) -> dict:
    """逐表快照该任务的关联行存在性 (级联效果等价断言的比对单元)"""
    conn = get_db()
    cursor = conn.cursor()

    def scalar(sql, params=()):
        cursor.execute(sql, params)
        return cursor.fetchone()[0]

    snap = {
        "queue": scalar("SELECT COUNT(*) FROM task_queue WHERE id = ?", (task_id,)),
        "logs": scalar("SELECT COUNT(*) FROM task_logs WHERE task_id = ?", (task_id,)),
        "breakpoints": scalar("SELECT COUNT(*) FROM download_breakpoints WHERE task_id = ?", (task_id,)),
        "forward_history_nulled": scalar(
            "SELECT COUNT(*) FROM forward_history WHERE task_id IS NULL AND source_msg_id = ? AND source_chat = '-1001'",
            (mid,),
        ),
        "forward_skips_nulled": scalar(
            "SELECT COUNT(*) FROM forward_skips WHERE task_id IS NULL AND msg_id = ?", (mid,)
        ),
        "storage_nulled": scalar(
            "SELECT COUNT(*) FROM storage_registry WHERE task_id IS NULL AND path LIKE ?", (f"downloads/{task_id}%",)
        ),
        "slots_terminal_gone": 1
        - scalar("SELECT COUNT(*) FROM placeholder_slots WHERE task_id = ? AND state = 'published'", (task_id,)),
        "slots_active_kept": scalar(
            "SELECT COUNT(*) FROM placeholder_slots WHERE task_id = ? AND state = 'placeholder_sent'", (task_id,)
        ),
    }
    conn.close()
    return snap


_EXPECTED_CASCADE = {
    "queue": 0,
    "logs": 0,
    "breakpoints": 0,
    "forward_history_nulled": 1,
    "forward_skips_nulled": 1,
    "storage_nulled": 1,
    "slots_terminal_gone": 1,
    "slots_active_kept": 1,
}


def test_delete_and_clear_finished_same_cascade_effect():
    """delete(task) 与 clear_finished() 对同构终态任务的级联效果逐表一致 (口径单点 _cascade_task_cleanup);
    占位槽排除式保留可行动态, 终态槽清除"""
    _seed_task_with_full_associations("casc_del_task", "cancelled", mid=101)
    _seed_task_with_full_associations("casc_clr_task", "failed", mid=202)

    assert TaskQueueRepository.delete("casc_del_task") is True
    snap_deleted = _snapshot_associations("casc_del_task", mid=101)
    assert snap_deleted == _EXPECTED_CASCADE

    cleared = TaskQueueRepository.clear_finished()
    assert "casc_clr_task" in cleared
    snap_cleared = _snapshot_associations("casc_clr_task", mid=202)
    assert snap_cleared == _EXPECTED_CASCADE

    # delete 与 clear_finished 对同构任务的级联效果逐键一致
    assert set(snap_deleted.keys()) == set(snap_cleared.keys())


def test_query_tasks_and_get_all_same_order():
    """成对排序/水位 SQL 收敛等价: 同一数据集下 query_tasks 与 get_all 输出顺序与富化字段一致"""
    specs = [
        ("ord_task_running", "running"),
        ("ord_task_queued", "queued"),
        ("ord_task_pending", "pending"),
        ("ord_task_paused", "paused"),
        ("ord_task_failed", "failed"),
        ("ord_task_completed", "completed"),
        ("ord_task_cancelled", "cancelled"),
    ]
    for tid, status in specs:
        TaskQueueRepository.create(tid, "download_link", {"links": ["https://t.me/c/1/2"]}, status=status)
        TaskQueueRepository.append_log(tid, f"{tid} watermark-log")

    rows_all = TaskQueueRepository.get_all(limit=100)
    rows_query, _ = TaskQueueRepository.query_tasks(page=1, limit=100)

    ids_all = [r["id"] for r in rows_all if r["id"].startswith("ord_task_")]
    ids_query = [r["id"] for r in rows_query if r["id"].startswith("ord_task_")]
    assert ids_all == ids_query
    # 期望排序: 活跃阶梯在前 (running > queued/pending > paused), 终态最近优先在后
    assert ids_all == [
        "ord_task_running",
        "ord_task_queued",
        "ord_task_pending",
        "ord_task_paused",
        "ord_task_failed",
        "ord_task_completed",
        "ord_task_cancelled",
    ]
    # 水位富化字段两侧一致
    for r_all in rows_all:
        if not r_all["id"].startswith("ord_task_"):
            continue
        r_q = next(r for r in rows_query if r["id"] == r_all["id"])
        assert r_all["log_count"] == r_q["log_count"] == 1
        assert r_all["last_log_seq"] == r_q["last_log_seq"] == 1

    # TASK_QUEUE_ORDER_SQL 为模块级单点常量 (成对 SQL 收敛的物证)
    assert "CASE status" in TASK_QUEUE_ORDER_SQL
    for tid, _ in specs:
        TaskQueueRepository.delete(tid)


def test_download_is_downloaded_handles_vanished_file_stat(tmp_dir):
    f_path = os.path.join(tmp_dir, "vanished.mp4")
    with open(f_path, "wb") as f:
        f.write(b"1234")
    DownloadHistoryRepository.record("-10088", 77, None, "vanished.mp4", f_path, 4)

    with patch("app.db.download_repository.os.stat", side_effect=FileNotFoundError):
        assert DownloadHistoryRepository.is_downloaded("-10088", 77) is False

    # 废弃记录已被清理
    with db_cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM download_history WHERE chat_id = '-10088' AND msg_id = 77")
        assert cursor.fetchone()[0] == 0


def test_publish_history_pikpak_path_collision():
    target = "-100777"
    PublishHistoryRepository.record(
        target,
        "pikpak://folder1/episode.mkv",
        10240,
        source_type="pikpak",
        remote_file_id="fid_A",
        tg_message_id=101,
    )
    PublishHistoryRepository.record(
        target,
        "pikpak://folder1/episode.mkv",
        10240,
        source_type="pikpak",
        remote_file_id="fid_B",
        tg_message_id=102,
    )
    info_a = PublishHistoryRepository.get_published_info(
        target, "pikpak://folder1/episode.mkv", 10240, source_type="pikpak", remote_file_id="fid_A"
    )
    info_b = PublishHistoryRepository.get_published_info(
        target, "pikpak://folder1/episode.mkv", 10240, source_type="pikpak", remote_file_id="fid_B"
    )
    assert info_a is not None and info_a.tg_message_id == 101
    assert info_b is not None and info_b.tg_message_id == 102


def test_task_repository_empty_status_guards_and_mark_retry_protection():
    tid = "repo_guard_t1"
    TaskQueueRepository.create(tid, "download_link", {"links": ["https://t.me/c/1/2"]}, status="running")

    # 空元组 () 守卫必须返回 False 且不得修改或删除任务
    assert TaskQueueRepository.update_status(tid, "completed", from_statuses=()) is False
    assert TaskQueueRepository.reset_task_for_retry(tid, from_statuses=()) is False
    assert TaskQueueRepository.delete(tid, allowed_statuses=()) is False
    assert TaskQueueRepository.get_status(tid) == "running"

    # 并发 pause 后迟到的 mark_retry 不得覆写 paused / cancelled
    assert TaskQueueRepository.update_status(tid, "paused", from_statuses=("running",)) is True
    assert TaskQueueRepository.mark_retry(tid, 1, time.time() + 60, "late error") is False
    assert TaskQueueRepository.get_status(tid) == "paused"

    assert TaskQueueRepository.update_status(tid, "cancelled", from_statuses=("paused",)) is True
    assert TaskQueueRepository.mark_retry(tid, 2, time.time() + 60, "late error") is False
    assert TaskQueueRepository.get_status(tid) == "cancelled"

    # running / queued 允许 mark_retry
    tid2 = "repo_guard_t2"
    TaskQueueRepository.create(tid2, "download_link", {}, status="running")
    assert TaskQueueRepository.mark_retry(tid2, 1, time.time() + 60, "recoverable") is True
    assert TaskQueueRepository.get_status(tid2) == "queued"


def test_download_is_downloaded_handles_replaced_parent(tmp_path):
    parent = tmp_path / "channel"
    parent.mkdir()
    recorded_file = parent / "video.mp4"
    recorded_file.write_bytes(b"1234")
    DownloadHistoryRepository.record("-10088", 78, None, recorded_file.name, str(recorded_file), 4)
    recorded_file.unlink()
    parent.rmdir()
    parent.write_bytes(b"replacement")

    assert DownloadHistoryRepository.is_downloaded("-10088", 78) is False
    with db_cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM download_history WHERE chat_id = '-10088' AND msg_id = 78")
        assert cursor.fetchone()[0] == 0


def test_download_is_downloaded_preserves_history_on_permission_error(tmp_path):
    recorded_file = tmp_path / "video.mp4"
    recorded_file.write_bytes(b"1234")
    DownloadHistoryRepository.record("-10088", 79, None, recorded_file.name, str(recorded_file), 4)

    with (
        patch("app.db.download_repository.os.stat", side_effect=PermissionError),
        pytest.raises(PermissionError),
    ):
        DownloadHistoryRepository.is_downloaded("-10088", 79)

    with db_cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM download_history WHERE chat_id = '-10088' AND msg_id = 79")
        assert cursor.fetchone()[0] == 1


def test_norm_file_path_preserves_posix_case_and_folds_windows_paths():
    assert norm_file_path("/media/A.mp4") == "/media/A.mp4"
    assert norm_file_path("/media/a.mp4") == "/media/a.mp4"
    assert norm_file_path("/media/A.mp4") != norm_file_path("/media/a.mp4")
    assert norm_file_path(r"D:\Media\A.mp4") == "d:/media/a.mp4"
    assert norm_file_path(r"\\NAS\Share\A.mp4") == "//nas/share/a.mp4"
    assert norm_file_path("//NAS/Share/A.mp4") == "//nas/share/a.mp4"

    PublishHistoryRepository.record("-100991", "/media/A.mp4", 100)
    assert PublishHistoryRepository.is_published("-100991", "/media/A.mp4", 100) is True
    assert PublishHistoryRepository.is_published("-100991", "/media/a.mp4", 100) is False


def test_download_get_valid_path_rejects_directory_with_positive_size(tmp_path):
    target_dir = tmp_path / "replaced_with_dir"
    target_dir.mkdir()
    DownloadHistoryRepository.record("-10088", 80, None, "replaced_with_dir", str(target_dir), 4096)

    fake_dir_stat = os.stat_result((stat_mod.S_IFDIR | 0o755, 0, 0, 1, 0, 0, 4096, 0, 0, 0))
    with patch("app.db.download_repository.os.stat", return_value=fake_dir_stat):
        assert DownloadHistoryRepository.get_valid_download_path("-10088", 80) is None
        assert DownloadHistoryRepository.is_downloaded("-10088", 80) is False


def test_download_get_valid_path_preserves_concurrently_replaced_record(tmp_path):
    old_file = tmp_path / "old.mp4"
    new_file = tmp_path / "new.mp4"
    new_file.write_bytes(b"new_bytes")
    DownloadHistoryRepository.record("-10088", 81, None, "old.mp4", str(old_file), 4)

    def _replace_during_stat(_path):
        DownloadHistoryRepository.record("-10088", 81, None, "new.mp4", str(new_file), 9)
        raise FileNotFoundError

    with patch("app.db.download_repository.os.stat", side_effect=_replace_during_stat):
        assert DownloadHistoryRepository.get_valid_download_path("-10088", 81) is None

    assert DownloadHistoryRepository.get_valid_download_path("-10088", 81) == str(new_file)


def test_task_repository_acquire_respects_cooldown_and_active_downloads_includes_queued():
    tid_cool = "cool_t1"
    TaskQueueRepository.create(tid_cool, "download_link", {}, status="queued")
    assert TaskQueueRepository.mark_retry(tid_cool, 1, time.time() + 120.0, "cooldown") is True
    assert TaskQueueRepository.acquire_task(tid_cool, account_id=1) is False
    assert TaskQueueRepository.get_status(tid_cool) == "queued"

    active = TaskQueueRepository.get_active_download_tasks()
    assert any(r["id"] == tid_cool and r["status"] == "queued" for r in active)
    assert StorageService._has_active_download_tasks() is True

    assert TaskQueueRepository.mark_retry(tid_cool, 1, time.time() - 1.0, "ready") is True
    assert TaskQueueRepository.acquire_task(tid_cool, account_id=1) is True
    assert TaskQueueRepository.get_status(tid_cool) == "running"


def test_overwrite_params_list_preserves_matching_rows_in_one_transaction():
    """读改写同事务: keep_pred 命中的旧行保留 + 新行拼接, 未命中旧行丢弃 (整键覆写语义)"""
    task_id = "repo_ow_params"
    TaskQueueRepository.create(
        task_id,
        "publish_folder",
        {"skipped_files": [{"file_name": "m.mp4", "oversize_kind": "manual_skip"}, {"file_name": "old.bin"}]},
    )
    try:
        written = TaskQueueRepository.overwrite_params_list(
            task_id,
            "skipped_files",
            [{"file_name": "new.mp4", "oversize_kind": "size"}],
            keep_pred=lambda row: isinstance(row, dict) and row.get("oversize_kind") == "manual_skip",
        )
        assert written is True
        rows = TaskQueueRepository.get_by_id(task_id)["params"]["skipped_files"]
        assert [r["file_name"] for r in rows] == ["m.mp4", "new.mp4"]
    finally:
        TaskQueueRepository.delete(task_id)


def test_overwrite_params_list_missing_task_is_noop():
    """任务行不存在时不写入 (与 merge_params 口径一致), 不抛异常不造行"""
    written = TaskQueueRepository.overwrite_params_list("repo_ow_missing", "skipped_files", [{"a": 1}])
    assert written is False
    assert TaskQueueRepository.get_by_id("repo_ow_missing") is None
