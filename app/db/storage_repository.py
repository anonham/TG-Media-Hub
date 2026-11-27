"""存储登记仓储 (Storage Registry Repository): downloads/ 扫描登记与统计的持久化仓储

扫描登记语义 (增量 diff): 与存量行逐路径比对 —— 新路径 INSERT,
扫描字段 (kind/size/mtime/paired/channel/task_id) 任一变化 UPDATE (status 复位 registered,
与历史全量重插口径一致), 未变行跳过 (零写放大), 消失路径 DELETE。单 BEGIN IMMEDIATE
事务提交, 端态与历史 DELETE+重插逐字段一致。
"""

from collections.abc import Sequence
from typing import Any

from app.db.database import db_cursor, db_immediate_tx, db_read_tx

# 扫描条目形态: (path, kind, size, mtime, paired, channel, task_id, status, scanned_at)
_SCAN_ENTRY = tuple[str, str, int, float, str, str | None, str | None, str, float]


class StorageRegistryRepository:
    @staticmethod
    def apply_scan_diff(entries: Sequence[_SCAN_ENTRY]) -> dict[str, int]:
        """扫描登记增量落库 (单事务), 返回 {total, inserted, updated, removed, skipped}。

        diff 键 = 扫描可变字段六元组; path 为主键。跳过行保留原 scanned_at (登记内容
        未变动的时间戳语义), 扫描动作时间戳由 SystemSettings.last_storage_scan_at 承载。"""
        with db_immediate_tx() as cursor:
            cursor.execute("SELECT path, kind, size, mtime, paired, channel, task_id FROM storage_registry")
            existing = {r["path"]: r for r in cursor.fetchall()}

            inserts: list[_SCAN_ENTRY] = []
            updates: list[tuple] = []
            skipped = 0
            kept_paths = set()
            for path, kind, size, mtime, paired, channel, task_id, status, scanned_at in entries:
                kept_paths.add(path)
                row = existing.get(path)
                if row is None:
                    inserts.append((path, kind, size, mtime, paired, channel, task_id, status, scanned_at))
                    continue
                if (row["kind"], row["size"], row["mtime"], row["paired"], row["channel"], row["task_id"]) == (
                    kind,
                    size,
                    mtime,
                    paired,
                    channel,
                    task_id,
                ):
                    skipped += 1
                    continue
                updates.append((kind, size, mtime, paired, channel, task_id, status, scanned_at, path))

            if inserts:
                cursor.executemany(
                    "INSERT INTO storage_registry (path, kind, size, mtime, paired, channel, task_id, status, scanned_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    inserts,
                )
            if updates:
                cursor.executemany(
                    "UPDATE storage_registry SET kind = ?, size = ?, mtime = ?, paired = ?, channel = ?, task_id = ?, "
                    "status = ?, scanned_at = ? WHERE path = ?",
                    updates,
                )
            gone = [p for p in existing if p not in kept_paths]
            if gone:
                cursor.executemany("DELETE FROM storage_registry WHERE path = ?", [(p,) for p in gone])

        return {
            "total": len(entries),
            "inserted": len(inserts),
            "updated": len(updates),
            "removed": len(gone),
            "skipped": skipped,
        }

    @staticmethod
    def count() -> int:
        """获取已登记条目总数"""
        with db_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM storage_registry")
            row = cursor.fetchone()
            return row[0] if row else 0

    @staticmethod
    def get_overview_stats() -> dict[str, Any]:
        """聚合存储概览全维度指标 (单快照只读事务聚合)"""
        with db_read_tx() as cursor:
            cursor.execute(
                """
                SELECT
                    COALESCE(SUM(size), 0),
                    COUNT(*),
                    COALESCE(SUM(CASE WHEN kind IN ('part', 'meta') THEN size ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN kind IN ('part', 'meta') THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN kind = 'final' THEN size ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN kind = 'final' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN kind = 'part' AND paired != 'ok' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN kind = 'meta' AND paired != 'ok' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN kind = 'final' AND channel IS NULL THEN 1 ELSE 0 END), 0),
                    MAX(scanned_at)
                FROM storage_registry
                """
            )
            (
                total_bytes,
                file_count,
                part_bytes,
                part_count,
                final_bytes,
                final_count,
                orphan_part_count,
                orphan_meta_count,
                unregistered_final_count,
                scanned_at,
            ) = cursor.fetchone()

            cursor.execute(
                "SELECT COALESCE(channel, ''), COUNT(*), COALESCE(SUM(size), 0) FROM storage_registry "
                "WHERE kind = 'final' GROUP BY COALESCE(channel, '') ORDER BY 3 DESC LIMIT 50"
            )
            by_channel = [{"chat_id": (r[0] or None), "count": r[1], "bytes": r[2]} for r in cursor.fetchall()]

        return {
            "total_bytes": total_bytes,
            "part_bytes": part_bytes,
            "final_bytes": final_bytes,
            "file_count": file_count,
            "final_count": final_count,
            "part_count": part_count,
            "orphan_part_count": orphan_part_count,
            "orphan_meta_count": orphan_meta_count,
            "unregistered_final_count": unregistered_final_count,
            "by_channel": by_channel,
            "scanned_at": scanned_at,
        }

    @staticmethod
    def list_files(kind: str = "", limit: int = 500, offset: int = 0) -> list[dict[str, Any]]:
        """按类型分页列表查询"""
        with db_cursor() as cursor:
            if kind:
                cursor.execute(
                    "SELECT path, kind, size, mtime, paired, channel, task_id FROM storage_registry "
                    "WHERE kind = ? ORDER BY size DESC LIMIT ? OFFSET ?",
                    (kind, limit, offset),
                )
            else:
                cursor.execute(
                    "SELECT path, kind, size, mtime, paired, channel, task_id FROM storage_registry "
                    "ORDER BY size DESC LIMIT ? OFFSET ?",
                    (limit, offset),
                )
            return [dict(r) for r in cursor.fetchall()]

    @staticmethod
    def query_files(where_sql: str = "", params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        """按条件聚合查询已登记文件列表"""
        with db_cursor() as cursor:
            cursor.execute(
                f"SELECT path, kind, size, mtime, paired, channel, task_id FROM storage_registry{where_sql} ORDER BY size DESC",
                params,
            )
            return [dict(r) for r in cursor.fetchall()]

    @staticmethod
    def get_downloads_metrics() -> tuple[int, int]:
        """获取下载产物总字节数与文件数"""
        with db_cursor() as cursor:
            cursor.execute("SELECT COALESCE(SUM(size), 0), COUNT(*) FROM storage_registry")
            row = cursor.fetchone()
            return (row[0], row[1]) if row else (0, 0)

    @staticmethod
    def delete_records_for_deleted_files(items: list[tuple[str, str]]) -> None:
        """删除物理文件后联动清理数据库登记 (单事务保证原子性)
        items: list of (file_path_str, suffix_lower)
        """
        if not items:
            return
        with db_immediate_tx() as cursor:
            for path_str, suffix in items:
                cursor.execute("DELETE FROM storage_registry WHERE path = ?", (path_str,))
                if suffix == ".part":
                    cursor.execute("DELETE FROM download_breakpoints WHERE part_path = ?", (path_str,))
                elif suffix == ".meta":
                    cursor.execute("DELETE FROM download_breakpoints WHERE meta_path = ?", (path_str,))
                else:
                    cursor.execute("DELETE FROM download_history WHERE file_path = ?", (path_str,))
