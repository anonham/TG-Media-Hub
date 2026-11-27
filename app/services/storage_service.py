"""存储治理服务: downloads/ 目录扫描登记、孤儿断点识别、守卫式文件删除。

安全红线:
1. 任何删除路径不得触碰活跃任务正在使用的文件 —— 三重守卫
   (路径边界 + active_download_paths 内存登记 + DB 运行态下载任务交叉校验);
2. 扫描只登记不删除, 删除只来自显式 API 调用;
3. 成品文件删除联动清理 download_history 记录, 但绝不触碰 publish_history (防重历史必须保留)。
"""

import logging
import mimetypes
import os
import shutil
import time
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.config import settings
from app.core.runtime import active_download_paths, active_task_controls, runtime_state_lock
from app.db.account_repository import SystemSettingsRepository
from app.db.download_repository import DownloadBreakpointRepository, DownloadHistoryRepository
from app.db.storage_repository import StorageRegistryRepository
from app.db.task_repository import TaskQueueRepository

logger = logging.getLogger(__name__)

_disk_usage_cache: dict[str, Any] = {"timestamp": 0.0, "path": "", "data": None}


def get_cached_disk_usage(path: str, ttl_seconds: float = 5.0) -> dict[str, Any]:
    """
    轻量化内核 O(1) 磁盘容量探测器 (带 5 秒 TTL 内存缓存)。
    底层调用 Win32 GetDiskFreeSpaceExW / Unix statvfs，
    只读内核在内存中的文件系统超级块元数据，耗时 < 0.05ms，零物理磁盘 I/O 磨损。
    """
    now = time.time()
    cached_data = _disk_usage_cache.get("data")
    if (
        isinstance(cached_data, dict)
        and _disk_usage_cache.get("path") == path
        and (now - float(_disk_usage_cache.get("timestamp") or 0.0) < ttl_seconds)
    ):
        return dict(cached_data)

    try:
        total, used, free = shutil.disk_usage(path)
    except Exception:
        total, used, free = 0, 0, 0

    disk_used_percent = round((used / total) * 100, 1) if total > 0 else 0.0
    data = {
        "disk_total_bytes": total,
        "disk_used_bytes": used,
        "disk_free_bytes": free,
        "disk_free_gb": round(free / (1024**3), 2),
        "disk_used_percent": disk_used_percent,
        # 前端向前兼容别名
        "total_bytes": total,
        "used_bytes": used,
        "free_bytes": free,
        "percent_used": disk_used_percent,
    }
    _disk_usage_cache.update({"timestamp": now, "path": path, "data": data})
    return dict(data)


def compute_storage_quota_metrics(downloads_bytes: int, disk_free_bytes: int) -> dict[str, Any]:
    """
    根据用户配置的最大存储上限与物理可用容量，计算有效可用容量（取两者最小值，不超过物理可用空间）。
    """
    quota_gb = SystemSettingsRepository.get_float("max_storage_quota_gb", 0.0)
    is_quota_active = quota_gb > 0.0
    quota_bytes = int(quota_gb * (1024**3)) if is_quota_active else 0

    if is_quota_active:
        remaining_quota_bytes = max(0, quota_bytes - downloads_bytes)
        # 核心约束：取物理剩余与配额剩余两者的最小值，绝不超过物理可用空间
        effective_free_bytes = min(disk_free_bytes, remaining_quota_bytes)
        quota_used_percent = round(min(100.0, (downloads_bytes / quota_bytes) * 100), 1) if quota_bytes > 0 else 0.0
    else:
        effective_free_bytes = disk_free_bytes
        quota_used_percent = 0.0

    return {
        "max_storage_quota_gb": quota_gb,
        "max_storage_quota_bytes": quota_bytes,
        "is_quota_active": is_quota_active,
        "effective_free_bytes": effective_free_bytes,
        "effective_free_gb": round(effective_free_bytes / (1024**3), 2),
        "quota_used_percent": quota_used_percent,
    }


def _norm(path: str) -> str:
    """跨平台路径归一化 (Windows 大小写不敏感)。"""
    return os.path.normcase(os.path.abspath(path))


def _active_path_key(path: str, *, follow_leaf: bool = True) -> str:
    """登记与删除守卫共用真实路径，兼容符号链接下载根目录。"""
    if follow_leaf:
        return _norm(os.path.realpath(path))
    parent, name = os.path.split(os.path.abspath(path))
    return _norm(os.path.join(os.path.realpath(parent), name))


def validate_safe_path(raw_path: str) -> Path:
    """严格沙箱校验：必须且仅能访问 DOWNLOAD_DIR 内部的文件，严禁 .. 目录穿越"""
    if not raw_path or not raw_path.strip():
        raise HTTPException(status_code=400, detail="文件路径不能为空")

    download_root = Path(settings.DOWNLOAD_DIR).resolve()

    p = Path(raw_path)
    if not p.is_absolute():
        resolved = (download_root / raw_path).resolve()
    else:
        resolved = p.resolve()

    if not resolved.is_relative_to(download_root):
        raise HTTPException(status_code=403, detail="路径越界 (严禁访问下载目录之外的任何路径)")

    return resolved


class StorageService:
    @staticmethod
    def validate_safe_path(raw_path: str) -> Path:
        return validate_safe_path(raw_path)

    # ── 扫描登记 ────────────────────────────────────────────

    @classmethod
    def ensure_registry_initialized(cls) -> None:
        """
        确保 storage_registry 至少执行过一次初始化扫描。
        若系统从未执行过扫描，或登记库为空但物理下载目录中实际已有文件，
        自动触发一次扫描登记，确保 get_metrics 与 get_overview 绝不误报 0 字节与 0 文件。
        """
        last_scan = SystemSettingsRepository.get("last_storage_scan_at")
        if not last_scan:
            cls.scan_and_register()
            return

        count = StorageRegistryRepository.count()

        if count == 0:
            dl_path = Path(settings.DOWNLOAD_DIR)
            if dl_path.exists():
                try:
                    if any(p.is_file() for p in dl_path.iterdir()):
                        cls.scan_and_register()
                except OSError as os_err:
                    logger.debug("初次启动扫描下载目录文件失败 (非致命): %s", os_err)

    @staticmethod
    def scan_and_register() -> dict[str, object]:
        """全量刷新 storage_registry (扫描缓存), 返回 overview。

        分类规则:
        - *.part: 兄弟 .meta 存在 -> paired=ok (可续传断点), 否则 missing_meta (孤儿断点)
        - *.meta: 兄弟 .part 存在 -> paired=ok, 否则 missing_part (孤儿元数据)
        - 其他:   final 成品, join download_history 得归属频道; 无记录 -> 未登记文件

        落库下沉 StorageRegistryRepository.apply_scan_diff (单 BEGIN IMMEDIATE 事务 +
        增量 diff: 未变行跳过零写放大, 端态与历史全量 DELETE+重插一致)。"""
        download_dir = Path(settings.DOWNLOAD_DIR)
        now = time.time()

        # 归属查找表 (normcase 比对, 规避分隔符/大小写差异)
        history_rows = DownloadHistoryRepository.get_history_map()
        history_by_path = {_norm(r[0]): r[1] for r in history_rows if r[0]}
        bp_rows = DownloadBreakpointRepository.get_tasks_map()
        task_by_path: dict[str, str] = {}
        for r in bp_rows:
            task_by_path[_norm(r[0])] = r[3]
            task_by_path[_norm(r[1])] = r[3]
            task_by_path[_norm(r[2])] = r[3]

        entries = []
        if download_dir.exists():
            for p in download_dir.iterdir():
                try:
                    if not p.is_file():
                        continue
                    name = p.name
                    if name.endswith(".part"):
                        kind, base = "part", name[: -len(".part")]
                        paired = "ok" if (download_dir / (base + ".meta")).exists() else "missing_meta"
                    elif name.endswith(".meta"):
                        kind, base = "meta", name[: -len(".meta")]
                        paired = "ok" if (download_dir / (base + ".part")).exists() else "missing_part"
                    else:
                        kind, base, paired = "final", name, "na"
                    stat = p.stat()
                    norm_key = _norm(str(p))
                    entries.append(
                        (
                            str(p),
                            kind,
                            stat.st_size,
                            stat.st_mtime,
                            paired,
                            history_by_path.get(norm_key) if kind == "final" else None,
                            task_by_path.get(norm_key),
                            "registered",
                            now,
                        )
                    )
                except OSError:
                    continue  # 扫描期间被删除/占用跳过, 不中断整体登记

        diff = StorageRegistryRepository.apply_scan_diff(entries)
        logger.debug("storage 扫描增量登记: %s", diff)

        # 记录最近一次全量扫描登记完成时间戳
        SystemSettingsRepository.set(
            "last_storage_scan_at", str(now), category="storage", description="最近一次存储扫描时间戳"
        )
        return StorageService.get_overview(ensure_init=False)

    @staticmethod
    def get_overview(ensure_init: bool = True) -> dict[str, object]:
        if ensure_init:
            StorageService.ensure_registry_initialized()

        stats = StorageRegistryRepository.get_overview_stats()
        total_bytes = stats["total_bytes"]
        scanned_at = stats["scanned_at"]
        # 增量 diff 下未变行保留原 scanned_at (登记内容未变动), 扫描动作时间以
        # last_storage_scan_at 为准 —— 两者取新, 保证 overview 展示"最近一次扫描"而非"最近一次内容变动"
        raw_last = SystemSettingsRepository.get("last_storage_scan_at")
        if raw_last:
            try:
                last_scan_ts = float(raw_last)
                scanned_at = last_scan_ts if scanned_at is None else max(scanned_at, last_scan_ts)
            except (ValueError, TypeError) as ts_err:
                logger.debug("last_storage_scan_at 时间戳解析失败 (非致命, 沿用行内 scanned_at): %s", ts_err)

        disk_info = get_cached_disk_usage(settings.DOWNLOAD_DIR)
        quota_info = compute_storage_quota_metrics(total_bytes, disk_info["disk_free_bytes"])

        return {
            "download_dir": settings.DOWNLOAD_DIR,
            "total_bytes": total_bytes,
            "part_bytes": stats["part_bytes"],
            "final_bytes": stats["final_bytes"],
            "file_count": stats["file_count"],
            "final_count": stats["final_count"],
            "part_count": stats["part_count"],
            "orphan_part_count": stats["orphan_part_count"],
            "orphan_meta_count": stats["orphan_meta_count"],
            "unregistered_final_count": stats["unregistered_final_count"],
            "by_channel": stats["by_channel"],
            "scanned_at": scanned_at,
            "disk_total_bytes": disk_info["disk_total_bytes"],
            "disk_used_bytes": disk_info["disk_used_bytes"],
            "disk_free_bytes": disk_info["disk_free_bytes"],
            "disk_free_gb": disk_info["disk_free_gb"],
            "disk_used_percent": disk_info["disk_used_percent"],
            **quota_info,
        }

    @staticmethod
    def list_files(kind: str = "", limit: int = 500, offset: int = 0) -> list[dict[str, object]]:
        return StorageRegistryRepository.list_files(kind=kind, limit=limit, offset=offset)

    @staticmethod
    def categorize_file(filename: str, kind: str = "") -> str:
        """根据文件后缀与 kind 判定分类"""
        if (
            kind in ("part", "meta")
            or filename.endswith(".part")
            or filename.endswith(".meta")
            or filename.endswith(".upload.meta")
        ):
            return "parts"
        lower = filename.lower()
        if lower.endswith(
            (
                ".d.ts",
                ".pdf",
                ".txt",
                ".doc",
                ".docx",
                ".zip",
                ".rar",
                ".7z",
                ".tar",
                ".gz",
                ".json",
                ".csv",
            )
        ):
            return "document"
        if lower.endswith(settings.VIDEO_EXTENSIONS):
            return "video"
        if lower.endswith(settings.IMAGE_EXTENSIONS):
            return "image"
        if lower.endswith(settings.AUDIO_EXTENSIONS):
            return "audio"
        return "other"

    @staticmethod
    def query_files(
        category: str = "",
        task_id: str = "",
        query: str = "",
        page: int = 1,
        limit: int = 50,
        offset: int | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """多维分页查询下载目录中的媒体文件列表"""
        StorageService.scan_and_register()

        hashes = {_norm(r[0]): r[1] for r in DownloadHistoryRepository.get_hashes_map()}

        where_clauses = []
        params: list[Any] = []

        if task_id:
            where_clauses.append("task_id = ?")
            params.append(task_id)

        if query:
            where_clauses.append("path LIKE ?")
            params.append(f"%{query}%")

        where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        rows = StorageRegistryRepository.query_files(where_sql=where_sql, params=tuple(params))

        download_root = Path(os.path.abspath(settings.DOWNLOAD_DIR))
        items = []
        for r in rows:
            p_str = r["path"]
            fname = os.path.basename(p_str)
            try:
                # 保留登记项的路径，避免把符号链接显示为其目标并交给删除接口。
                rel_p = Path(os.path.abspath(p_str)).relative_to(download_root).as_posix()
            except ValueError:
                rel_p = fname

            cat = StorageService.categorize_file(fname, r["kind"])
            if category and category.lower() != "all":
                c_low = category.lower()
                if c_low in ("part", "meta", "final"):
                    if r["kind"] != c_low:
                        continue
                elif c_low != cat:
                    continue

            mime, _ = mimetypes.guess_type(p_str)
            if not mime:
                if cat == "video":
                    mime = "video/mp4"
                elif cat == "image":
                    mime = "image/jpeg"
                elif cat == "audio":
                    mime = "audio/mpeg"
                elif cat == "parts":
                    mime = "application/octet-stream"
                else:
                    mime = "application/octet-stream"

            norm_p = _norm(p_str)
            f_hash = hashes.get(norm_p)

            items.append(
                {
                    "path": rel_p,
                    "size": r["size"],
                    "file_name": fname,
                    "rel_path": rel_p,
                    "abs_path": p_str,
                    "size_bytes": r["size"],
                    "size_mb": round(r["size"] / (1024 * 1024), 2),
                    "mtime": r["mtime"],
                    "mime_type": mime,
                    "category": cat,
                    "task_id": r.get("task_id"),
                    "channel": r.get("channel"),
                    "paired": r.get("paired", "na"),
                    "file_hash": f_hash,
                }
            )

        total_count = len(items)
        effective_offset = max(0, offset) if offset is not None else max(0, (page - 1) * limit)
        paginated_items = items[effective_offset : effective_offset + limit]
        return paginated_items, total_count

    @staticmethod
    def get_breakpoints() -> dict[str, Any]:
        """获取所有断点碎片文件与配对状态"""
        download_dir = Path(settings.DOWNLOAD_DIR)
        bp_rows = DownloadBreakpointRepository.get_all_breakpoints()

        bp_dict = {}
        for r in bp_rows:
            bp_dict[_norm(r["part_path"])] = r
            bp_dict[_norm(r["meta_path"])] = r

        items = []
        orphan_count = 0
        paired_count = 0

        if download_dir.exists():
            for p in download_dir.iterdir():
                try:
                    if not p.is_file():
                        continue
                    name = p.name
                    if name.endswith(".part"):
                        base = name[: -len(".part")]
                        meta_p = download_dir / (base + ".meta")
                        meta_exists = meta_p.exists()
                        paired_status = "ok" if meta_exists else "missing_meta"
                        if paired_status == "ok":
                            paired_count += 1
                        else:
                            orphan_count += 1
                        norm_p = _norm(str(p))
                        bp_info = bp_dict.get(norm_p)
                        items.append(
                            {
                                "part_path": str(p),
                                "meta_path": str(meta_p),
                                "final_path": str(bp_info["final_path"]) if bp_info else str(download_dir / base),
                                "task_id": bp_info["task_id"] if bp_info else None,
                                "chat_id": bp_info["chat_id"] if bp_info else None,
                                "msg_id": bp_info["msg_id"] if bp_info else None,
                                "part_size_bytes": p.stat().st_size,
                                "part_size_mb": round(p.stat().st_size / (1024 * 1024), 2),
                                "paired_status": paired_status,
                                "created_at": p.stat().st_mtime,
                            }
                        )
                    elif name.endswith(".meta") and not (download_dir / (name[: -len(".meta")] + ".part")).exists():
                        orphan_count += 1
                        base = name[: -len(".meta")]
                        part_p = download_dir / (base + ".part")
                        norm_p = _norm(str(p))
                        bp_info = bp_dict.get(norm_p)
                        items.append(
                            {
                                "part_path": str(part_p),
                                "meta_path": str(p),
                                "final_path": str(bp_info["final_path"]) if bp_info else str(download_dir / base),
                                "task_id": bp_info["task_id"] if bp_info else None,
                                "chat_id": bp_info["chat_id"] if bp_info else None,
                                "msg_id": bp_info["msg_id"] if bp_info else None,
                                "part_size_bytes": p.stat().st_size,
                                "part_size_mb": round(p.stat().st_size / (1024 * 1024), 2),
                                "paired_status": "missing_part",
                                "created_at": p.stat().st_mtime,
                            }
                        )
                except OSError:
                    continue

        return {"total": len(items), "orphan_count": orphan_count, "paired_count": paired_count, "items": items}

    @staticmethod
    def clean_orphans() -> dict[str, Any]:
        """一键扫描并清理无活跃任务关联的孤儿断点碎片"""
        if StorageService._has_active_download_tasks():
            raise HTTPException(status_code=409, detail="当前存在进行中的下载任务，断点文件已锁定，无法执行清理")

        bp_data = StorageService.get_breakpoints()
        orphan_files = []
        orphan_size_map: dict[str, int] = {}

        for item in bp_data["items"]:
            if item["paired_status"] != "ok":
                if item["paired_status"] == "missing_meta" and os.path.exists(item["part_path"]):
                    orphan_size_map[os.path.abspath(item["part_path"])] = int(item["part_size_bytes"] or 0)
                    orphan_files.append(item["part_path"])
                elif item["paired_status"] == "missing_part" and os.path.exists(item["meta_path"]):
                    orphan_size_map[os.path.abspath(item["meta_path"])] = int(item["part_size_bytes"] or 0)
                    orphan_files.append(item["meta_path"])

        cleaned_files = []
        freed_bytes = 0
        if orphan_files:
            res = StorageService.delete_files(orphan_files)
            cleaned_files = res["deleted"]
            # freed 只计真实删除的文件: 删除失败/守卫跳过的候选不得虚报释放空间
            for p in cleaned_files:
                freed_bytes += orphan_size_map.get(os.path.abspath(p), 0)

        return {
            "status": "ok",
            "cleaned_count": len(cleaned_files),
            "freed_bytes": freed_bytes,
            "freed_mb": round(freed_bytes / (1024 * 1024), 2),
            "cleaned_files": cleaned_files,
        }

    @staticmethod
    def get_metrics() -> dict[str, Any]:
        StorageService.ensure_registry_initialized()

        download_dir = settings.DOWNLOAD_DIR
        logs_dir = settings.LOG_DIR
        db_path = settings.DB_PATH

        # 1. 内核 O(1) 磁盘容量读取 (带 5s 内存 TTL 缓存, 零物理磁盘 I/O)
        disk_info = get_cached_disk_usage(download_dir)

        # 2. 数据库索引秒级聚合，彻底废除全量递归 os.walk 物理磁盘遍历
        downloads_bytes, total_files_count = StorageRegistryRepository.get_downloads_metrics()

        # 3. 计算用户最大存储上限与有效剩余可用容量 (取两者最小值，不超过物理可用空间)
        quota_info = compute_storage_quota_metrics(downloads_bytes, disk_info["disk_free_bytes"])

        db_size_bytes = os.path.getsize(db_path) if os.path.exists(db_path) else 0

        logs_size_bytes = 0
        if os.path.exists(logs_dir):
            try:
                for entry in os.scandir(logs_dir):
                    if entry.is_file():
                        logs_size_bytes += entry.stat().st_size
            except OSError as os_err:
                logger.debug("统计日志目录大小异常 (非致命): %s", os_err)

        return {
            **disk_info,
            **quota_info,
            "downloads_bytes": downloads_bytes,
            "downloads_mb": round(downloads_bytes / (1024 * 1024), 2),
            "db_size_bytes": db_size_bytes,
            "logs_size_bytes": logs_size_bytes,
            "total_files_count": total_files_count,
            "active_downloads_locked": StorageService._has_active_download_tasks(),
        }

    # ── 守卫式删除 ──────────────────────────────────────────

    @staticmethod
    def _has_active_download_tasks() -> bool:
        """存在进行中(或暂停且协程存活)的下载任务时, 一切 .part/.meta 拒绝删除 (交叉校验)"""
        rows = TaskQueueRepository.get_active_download_tasks()
        for r in rows:
            if r["status"] in ("running", "pending", "queued"):
                return True
            if r["status"] == "paused" and r["id"] in active_task_controls:
                return True
        return False

    @staticmethod
    def _pair_part_meta_targets(resolved: Path) -> list[Path]:
        """成对策略: 删 .part 连带 .meta (反之亦然); 孤儿单文件单独删。"""
        targets = [resolved]
        if resolved.name.endswith(".part"):
            sibling = resolved.with_name(resolved.name[: -len(".part")] + ".meta")
            if sibling.exists():
                targets.append(sibling)
        elif resolved.name.endswith(".meta"):
            sibling = resolved.with_name(resolved.name[: -len(".meta")] + ".part")
            if sibling.exists():
                targets.append(sibling)
        return targets

    @staticmethod
    def delete_files(paths: list[str]) -> dict[str, list]:
        """按请求删除文件 (成对 .part/.meta 连带删除; final 联动清 download_history)。

        返回 {"deleted": [...], "skipped": [{"path", "reason"}]}; 单文件失败不中断批次。
        """
        deleted: list[str] = []
        skipped: list[dict[str, str]] = []
        if not paths:
            return {"deleted": deleted, "skipped": skipped}

        lexical_root = Path(os.path.abspath(settings.DOWNLOAD_DIR))
        download_root = Path(settings.DOWNLOAD_DIR).resolve()
        active_paths = {_active_path_key(path) for path in active_download_paths}
        lock_breakpoints = StorageService._has_active_download_tasks()

        db_cleanup_targets: list[tuple[str, str]] = []
        for raw in paths:
            raw_entry = Path(raw)
            is_absolute = raw_entry.is_absolute()
            if not is_absolute:
                raw_entry = lexical_root / raw_entry
            try:
                lexical_entry = Path(os.path.abspath(raw_entry))
                if not lexical_entry.is_relative_to(lexical_root) and not (
                    is_absolute and lexical_entry.is_relative_to(download_root)
                ):
                    skipped.append({"path": raw, "reason": "路径越界 (仅允许删除下载目录内文件)"})
                    continue
                # is_symlink() 可能传播权限类 OSError (Python 3.12 pathlib), 必须置于 per-file
                # 守卫内: 单条检查失败仅记 skipped, 不得中止整批 (批 A 复审)
                if raw_entry.is_symlink():
                    # 符号链接一律拒绝: resolve() 会把删除目标改写为链接指向的真实文件 ——
                    # 孤儿清理场景下 .part 指向下载目录内成品媒体时, 会误删成品且留下悬空链接
                    # 删除链接本身亦非本接口语义, 交由用户手工处置
                    skipped.append({"path": raw, "reason": "拒绝删除符号链接"})
                    continue
                resolved = raw_entry.resolve()
            except OSError:
                skipped.append({"path": raw, "reason": "路径解析失败"})
                continue

            # 守卫①: 路径边界 —— 只允许删除 downloads/ 目录内的文件
            if not resolved.is_relative_to(download_root):
                skipped.append({"path": raw, "reason": "路径越界 (仅允许删除下载目录内文件)"})
                continue

            targets = StorageService._pair_part_meta_targets(resolved)
            target_keys = [_active_path_key(str(t)) for t in targets]

            # 守卫②: 内存登记的活跃占用
            if any(key in active_paths for key in target_keys):
                skipped.append({"path": raw, "reason": "活跃任务正在使用该文件"})
                continue
            # 守卫③: 存在进行中的下载任务时, 断点文件一律拒绝
            if lock_breakpoints and any(t.suffix in (".part", ".meta") for t in targets):
                skipped.append({"path": raw, "reason": "存在进行中的下载任务, 断点文件已锁定"})
                continue

            for t, key in zip(targets, target_keys, strict=True):
                # 持锁复查 + unlink 原子化: 登记 (事件循环) 与删除 (purge 工作线程) 跨线程,
                # 快照守卫②存在 TOCTOU —— 批内新登记的 .part/.meta 必须在这里被拦住
                with runtime_state_lock():
                    if key in active_download_paths:
                        skipped.append({"path": str(t), "reason": "活跃任务正在使用该文件 (删除前复查)"})
                        break
                    if t.exists():
                        try:
                            t.unlink()
                            deleted.append(str(t))
                        except OSError as e:
                            skipped.append({"path": str(t), "reason": f"物理删除失败: {e}"})
                            continue
                    # 文件已不存在视为幂等成功, 继续清理登记
                    db_cleanup_targets.append((str(t), t.suffix))

        StorageRegistryRepository.delete_records_for_deleted_files(db_cleanup_targets)
        return {"deleted": deleted, "skipped": skipped}
