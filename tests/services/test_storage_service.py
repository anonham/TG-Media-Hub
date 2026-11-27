"""
存储管理服务单元测试 (StorageService & StorageRepository Unit Tests)

覆盖：
1. validate_safe_path 安全沙箱校验 (正常路径 / 相对路径 / 越界穿越 403 / 空路径 400)
2. compute_storage_quota_metrics 配额与剩余物理容量计算
3. categorize_file 文件格式分类 (video, audio, image, part, meta, other)
4. scan_and_register & get_overview 全量与增量注册
5. list_files & query_files 过滤、排序、分页
6. clean_orphans 孤儿 meta / 无效断点清理
7. get_breakpoints 断点汇总
8. delete_files 安全批量删除与越界防护
9. StorageRegistryRepository 附加统计方法 (count, overview_stats, metrics, delete_records)
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest
from _safeio import safe_write_bytes
from fastapi import HTTPException

from app.config import settings
from app.db.database import get_db
from app.db.storage_repository import StorageRegistryRepository
from app.services.storage_service import (
    StorageService,
    compute_storage_quota_metrics,
    validate_safe_path,
)


@pytest.fixture(autouse=True)
def clean_storage_db():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM storage_registry")
    cursor.execute("DELETE FROM download_breakpoints")
    cursor.execute("DELETE FROM task_queue")
    conn.commit()
    conn.close()


# ==============================================================================
# 1. 路径沙箱安全校验 (validate_safe_path)
# ==============================================================================


def test_validate_safe_path_valid():
    download_root = Path(settings.DOWNLOAD_DIR).resolve()
    # 相对路径解析
    p1 = validate_safe_path("sub/file.mp4")
    assert p1 == download_root / "sub" / "file.mp4"

    # 绝对路径且在 download_root 内
    target_abs = str(download_root / "test.mp4")
    p2 = validate_safe_path(target_abs)
    assert p2 == Path(target_abs).resolve()


def test_validate_safe_path_traversal_forbidden():
    # 空路径
    with pytest.raises(HTTPException) as exc_400:
        validate_safe_path("   ")
    assert exc_400.value.status_code == 400

    # 目录穿越 (403)
    with pytest.raises(HTTPException) as exc_403:
        validate_safe_path("../../windows/system32")
    assert exc_403.value.status_code == 403


# ==============================================================================
# 2. 存储配额与磁盘用量计算 (compute_storage_quota_metrics)
# ==============================================================================


def test_compute_storage_quota_metrics():
    # 1. 未启用配额 (quota_gb = 0)
    with patch("app.db.account_repository.SystemSettingsRepository.get_float", return_value=0.0):
        metrics = compute_storage_quota_metrics(downloads_bytes=1000, disk_free_bytes=5000)
        assert metrics["is_quota_active"] is False
        assert metrics["effective_free_bytes"] == 5000

    # 2. 启用配额 (quota_gb = 10, downloads = 2GB, disk_free = 20GB)
    with patch("app.db.account_repository.SystemSettingsRepository.get_float", return_value=10.0):
        two_gb = 2 * (1024**3)
        twenty_gb = 20 * (1024**3)
        metrics2 = compute_storage_quota_metrics(downloads_bytes=two_gb, disk_free_bytes=twenty_gb)
        assert metrics2["is_quota_active"] is True
        # 剩余配额为 8GB，物理剩余 20GB，有效剩余取两者较小值即 8GB
        assert metrics2["effective_free_bytes"] == 8 * (1024**3)
        assert metrics2["quota_used_percent"] == 20.0


# ==============================================================================
# 3. 文件类型推断 (categorize_file)
# ==============================================================================


def test_categorize_file():
    assert StorageService.categorize_file("movie.mp4") == "video"
    assert StorageService.categorize_file("clip.mkv") == "video"
    assert StorageService.categorize_file("video.ts") == "video"
    assert StorageService.categorize_file("video.m2ts") == "video"
    assert StorageService.categorize_file("video.mts") == "video"
    assert StorageService.categorize_file("types.d.ts") == "document"
    assert StorageService.categorize_file("track.flac") == "audio"
    assert StorageService.categorize_file("video.part") == "parts"
    assert StorageService.categorize_file("video.part.meta") == "parts"
    assert StorageService.categorize_file("video.meta") == "parts"
    assert StorageService.categorize_file("thumb.jpg") == "image"
    assert StorageService.categorize_file("thumb.webp") == "image"
    assert StorageService.categorize_file("data.bin") == "other"


# ==============================================================================
# 4. 扫描登记与文件列表查询 (scan_and_register / list_files / query_files)
# ==============================================================================


def test_scan_and_query_files(tmp_path):
    dl_dir = Path(settings.DOWNLOAD_DIR)
    f1 = dl_dir / "vid1.mp4"
    f1.write_bytes(b"\x00" * 1024)
    f2 = dl_dir / "vid2.part"
    f2.write_bytes(b"\x00" * 512)

    # 扫描入库
    overview = StorageService.scan_and_register()
    assert overview["file_count"] >= 2

    # 查询列表
    files = StorageService.list_files(limit=10, offset=0)
    assert len(files) >= 2

    # 按类别过滤
    finals = StorageService.list_files(kind="final")
    assert any("vid1.mp4" in f["path"] for f in finals)
    assert not any("vid2.part" in f["path"] for f in finals)

    # query_files 条件检索
    q_items, q_total = StorageService.query_files(category="parts")
    assert q_total >= 1
    assert any("vid2.part" in r["path"] for r in q_items)


# ==============================================================================
# 5. 断点汇总与孤儿清理 (get_breakpoints / clean_orphans)
# ==============================================================================


def test_clean_orphans_and_get_breakpoints(tmp_path):
    dl_dir = Path(settings.DOWNLOAD_DIR)
    orphan_meta = dl_dir / "orphan_sample.part.meta"
    orphan_meta.write_bytes(b'{"total_bytes": 100}')

    # 孤儿清理：orphan_meta 对应的 .part 不存在，应被清除
    res_clean = StorageService.clean_orphans()
    assert res_clean["cleaned_count"] >= 1
    assert not orphan_meta.exists()

    # get_breakpoints 汇总验证
    bp_data = StorageService.get_breakpoints()
    assert "total" in bp_data
    assert "orphan_count" in bp_data
    assert "paired_count" in bp_data
    assert "items" in bp_data


# ==============================================================================
# 6. 文件安全批量删除 (delete_files)
# ==============================================================================


def test_delete_files_security_and_execution(tmp_path):
    dl_dir = Path(settings.DOWNLOAD_DIR)
    target_file = dl_dir / "to_delete.mp4"
    target_file.write_bytes(b"temp")

    # 登记到库
    StorageService.scan_and_register()

    # 执行安全删除
    del_res = StorageService.delete_files([str(target_file)])
    assert str(target_file.resolve()) in [str(Path(p).resolve()) for p in del_res["deleted"]]
    assert not target_file.exists()


# ==============================================================================
# 7. 仓库底层方法测试 (StorageRegistryRepository)
# ==============================================================================


def test_storage_registry_repository_methods():
    entry = (
        "downloads/sample.mp4",
        "final",
        1024,
        100.0,
        "na",
        None,
        None,
        "registered",
        100.0,
    )
    StorageRegistryRepository.apply_scan_diff([entry])

    # count
    assert StorageRegistryRepository.count() == 1

    # get_overview_stats
    stats = StorageRegistryRepository.get_overview_stats()
    assert stats["file_count"] == 1
    assert stats["final_count"] == 1
    assert stats["final_bytes"] == 1024

    # get_downloads_metrics
    m_bytes, m_count = StorageRegistryRepository.get_downloads_metrics()
    assert m_bytes == 1024
    assert m_count == 1

    # delete_records_for_deleted_files
    StorageRegistryRepository.delete_records_for_deleted_files([("downloads/sample.mp4", "final")])
    assert StorageRegistryRepository.count() == 0


def test_download_registry_normalizes_registration_and_release(monkeypatch):
    from app.core.runtime import active_download_paths
    from app.services.downloader_service import (
        _register_active_download_paths_sync,
        _unregister_active_download_paths_sync,
    )
    from app.services.storage_service import _norm

    monkeypatch.setattr("os.path.normcase", lambda path: path.lower())
    path = "/Downloads/Active.PART"
    keys = _register_active_download_paths_sync((path,))
    try:
        assert _norm(path.lower()) in active_download_paths
    finally:
        _unregister_active_download_paths_sync(keys)
    assert _norm(path) not in active_download_paths


def test_final_path_stays_protected_after_symlink_is_replaced(tmp_path):
    from app.services.downloader_service import (
        _register_active_download_paths_sync,
        _unregister_active_download_paths_sync,
    )

    final = Path(settings.DOWNLOAD_DIR) / "active-final.mp4"
    link_target = tmp_path / "target.mp4"
    link_target.write_bytes(b"target")
    try:
        final.symlink_to(link_target)
    except OSError as exc:
        pytest.skip(f"symlinks unavailable: {exc}")
    completed = final.with_suffix(".part")
    completed.write_bytes(b"new download")

    keys = _register_active_download_paths_sync((str(final), str(completed)))
    try:
        completed.replace(final)
        result = StorageService.delete_files([final.name])
        assert result["skipped"][0]["reason"] == "活跃任务正在使用该文件"
        assert final.read_bytes() == b"new download"
    finally:
        _unregister_active_download_paths_sync(keys)
    assert StorageService.delete_files([final.name])["deleted"]


def test_delete_files_rejects_symlink_to_completed_file(tmp_path):
    download_root = settings.DOWNLOAD_DIR
    workdir = os.path.join(download_root, "sym_guard")
    os.makedirs(workdir, exist_ok=True)
    target = os.path.join(workdir, "movie.mp4")
    safe_write_bytes(target, b"completed media")
    link = os.path.join(workdir, "stale.part")
    try:
        os.symlink(target, link)
    except OSError:
        pytest.skip("当前环境无法创建文件符号链接 (需管理员或开发者模式)")

    try:
        res = StorageService.delete_files([link])
        assert res["deleted"] == []
        assert res["skipped"][0]["reason"] == "拒绝删除符号链接", "符号链接必须整体拒绝"
        assert os.path.exists(target), "链接指向的真实成品文件不得被删除"
    finally:
        if os.path.lexists(link):
            os.remove(link)
        if os.path.exists(target):
            os.remove(target)
        os.rmdir(workdir)


def test_delete_files_still_deletes_regular_file(tmp_path):
    download_root = settings.DOWNLOAD_DIR
    workdir = os.path.join(download_root, "regular_del")
    os.makedirs(workdir, exist_ok=True)
    victim = os.path.join(workdir, "fragment.part")
    safe_write_bytes(victim, b"part data")
    try:
        res = StorageService.delete_files([victim])
        assert victim in res["deleted"], "普通文件删除行为不变"
        assert not os.path.exists(victim)
    finally:
        if os.path.exists(victim):
            os.remove(victim)
        if os.path.isdir(workdir):
            os.rmdir(workdir)
