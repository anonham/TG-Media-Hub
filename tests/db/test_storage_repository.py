"""StorageRegistryRepository.apply_scan_diff 增量语义、大批量移除与 scan_and_register 端到端测试。"""

import os
import time

from _safeio import safe_write_bytes

from app.config import settings
from app.db.account_repository import SystemSettingsRepository
from app.db.database import get_db
from app.db.storage_repository import StorageRegistryRepository
from app.services.storage_service import StorageService


def _entry(path: str, kind: str, size: int, mtime: float, paired: str = "na", channel=None, task_id=None, now=0.0):
    return (path, kind, size, mtime, paired, channel, task_id, "registered", now)


def _row(path: str):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT path, kind, size, mtime, paired, channel, task_id, status, scanned_at FROM storage_registry WHERE path = ?",
        (path,),
    )
    row = cursor.fetchone()
    conn.close()
    return tuple(row) if row else None


def test_apply_scan_diff_incremental_semantics():
    now1 = time.time()
    e_a = _entry("dl/a.mp4", "final", 100, 1.0, channel="-1001", now=now1)
    e_b = _entry("dl/b.part", "part", 50, 2.0, paired="ok", task_id="t1", now=now1)

    # 首扫: 全部 INSERT
    assert StorageRegistryRepository.apply_scan_diff([e_a, e_b]) == {
        "total": 2,
        "inserted": 2,
        "updated": 0,
        "removed": 0,
        "skipped": 0,
    }
    row_a = _row("dl/a.mp4")
    assert row_a[1:4] == ("final", 100, 1.0) and row_a[5] == "-1001"

    # 二扫 (内容未变, 扫描时间更新): 全部跳过, 行内 scanned_at 保持首扫值
    now2 = now1 + 10
    assert StorageRegistryRepository.apply_scan_diff(
        [
            _entry("dl/a.mp4", "final", 100, 1.0, channel="-1001", now=now2),
            _entry("dl/b.part", "part", 50, 2.0, paired="ok", task_id="t1", now=now2),
        ]
    ) == {"total": 2, "inserted": 0, "updated": 0, "removed": 0, "skipped": 2}
    assert _row("dl/a.mp4")[8] == now1

    # 三扫: a 变更 (size 变) / b 消失 / c 新增
    now3 = now2 + 10
    diff = StorageRegistryRepository.apply_scan_diff(
        [
            _entry("dl/a.mp4", "final", 200, 1.0, channel="-1001", now=now3),
            _entry("dl/c.meta", "meta", 30, 3.0, paired="missing_part", now=now3),
        ]
    )
    assert diff == {"total": 2, "inserted": 1, "updated": 1, "removed": 1, "skipped": 0}
    assert _row("dl/a.mp4")[2] == 200 and _row("dl/a.mp4")[8] == now3
    assert _row("dl/b.part") is None
    assert _row("dl/c.meta") is not None


def test_scan_and_register_incremental_and_overview_timestamp(tmp_dir):
    """端到端: 未变文件重扫零重写 (行 scanned_at 不动), overview 扫描时间取 scan 动作时间 (取新)"""
    media_path = os.path.join(settings.DOWNLOAD_DIR, "clip.mp4")
    safe_write_bytes(media_path, b"\x00\x01\x02\x03media")

    overview1 = StorageService.scan_and_register()
    ts1_row = _row(media_path)
    assert ts1_row is not None and ts1_row[1] == "final"

    time.sleep(0.02)
    overview2 = StorageService.scan_and_register()
    assert overview2["file_count"] == 1
    # 未变行保留首扫 scanned_at (零重写), overview 时间取较新的扫描动作时间戳
    assert _row(media_path)[8] == ts1_row[8]
    assert overview2["scanned_at"] >= overview1["scanned_at"]
    assert overview2["scanned_at"] == float(SystemSettingsRepository.get("last_storage_scan_at"))

    # 变更行: 内容变化 → size 变化 → 行被 UPDATE (scanned_at 刷新)
    new_bytes = b"\x00\x01\x02\x03mediaextra-bytes"
    safe_write_bytes(media_path, new_bytes)
    overview3 = StorageService.scan_and_register()
    assert _row(media_path)[2] == len(new_bytes)
    assert _row(media_path)[8] > ts1_row[8]
    assert overview3["file_count"] == 1


def test_storage_apply_scan_diff_large_removal_and_overview():
    now = time.time()
    entries = [(f"dl/bulk_{i}.mp4", "final", 100 + i, now, "na", "-1001", None, "registered", now) for i in range(1200)]
    res1 = StorageRegistryRepository.apply_scan_diff(entries)
    assert res1["inserted"] == 1200

    # 清空全部 1200 条（超旧版 999 参数上限），不应触发 SQLite too many SQL variables
    res2 = StorageRegistryRepository.apply_scan_diff([])
    assert res2["removed"] == 1200

    overview = StorageRegistryRepository.get_overview_stats()
    assert overview["file_count"] == 0
    assert overview["total_bytes"] == 0
