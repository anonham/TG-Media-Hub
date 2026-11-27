"""下载产物仓储 (Download Repository): 下载历史防重记录 + 断点文件归属登记"""

import os
import stat
import time
from typing import Any, TypedDict

from app.db.database import db_cursor


class DownloadRecord(TypedDict):
    file_path: str
    file_name: str | None
    grouped_id: str | None
    task_id: str | None
    album_completed: bool


class DownloadHistoryRepository:
    @staticmethod
    def is_downloaded(chat_id: str, msg_id: int) -> bool:
        """检查数据库记录，同时物理校验磁盘文件是否存在且非空。若文件已被删除，自动清理旧记录并允许重新下载"""
        return DownloadHistoryRepository.get_valid_download_path(chat_id, msg_id) is not None

    @staticmethod
    def get_valid_download_path(
        chat_id: str, msg_id: int, *, task_id: str | None = None, reused_by_task_id: str | None = None
    ) -> str | None:
        """返回已下载且磁盘文件真实存在的 file_path (未下载/文件已删返回 None 并清理废行)。

        独立于 is_downloaded 暴露: 防重跳过时需要回传真实落盘路径 (历史行可能为旧命名布局)。"""
        record = DownloadHistoryRepository.get_valid_download_record(
            chat_id, msg_id, task_id=task_id, reused_by_task_id=reused_by_task_id
        )
        return record["file_path"] if record else None

    @staticmethod
    def get_valid_download_record(
        chat_id: str,
        msg_id: int,
        *,
        require_completed_album: bool = False,
        task_id: str | None = None,
        reused_by_task_id: str | None = None,
    ) -> DownloadRecord | None:
        if not -(2**63) <= msg_id < 2**63:
            return None
        with db_cursor() as cursor:
            cursor.execute(
                """
                SELECT h.id, h.file_path, h.downloaded_at, h.file_name, h.grouped_id, h.task_id, t.status, h.task_completed
                FROM download_history AS h LEFT JOIN task_queue AS t ON t.id = h.task_id
                WHERE h.chat_id = ? AND h.msg_id = ? AND (? IS NULL OR h.task_id = ?)
                """,
                (str(chat_id), msg_id, task_id, task_id),
            )
            row = cursor.fetchone()

        if row is None:
            return None
        record_id, file_path, downloaded_at, file_name, grouped_id, task_id, task_status, task_completed = row
        # 物理磁盘校验：单次 stat 消除 exists+getsize 竞态, 必须为常规文件且大小 > 0 字节
        st: os.stat_result | None = None
        if file_path:
            try:
                st = os.stat(file_path)
            except (FileNotFoundError, NotADirectoryError):
                st = None
        if st is not None and stat.S_ISREG(st.st_mode) and st.st_size > 0:
            album_completed = not grouped_id or task_status == "completed" or bool(task_completed)
            if require_completed_album and not album_completed:
                return None
            if reused_by_task_id and reused_by_task_id != task_id and not album_completed:
                with db_cursor(commit=True) as cursor:
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO download_history_reuse (task_id, history_id)
                        SELECT t.id, h.id FROM download_history AS h JOIN task_queue AS t ON t.id = ?
                        WHERE h.id = ? AND h.downloaded_at = ?
                        """,
                        (reused_by_task_id, record_id, downloaded_at),
                    )
            return DownloadRecord(
                file_path=file_path,
                file_name=file_name,
                grouped_id=grouped_id,
                task_id=task_id,
                album_completed=album_completed,
            )
        # 物理文件已被删除或非有效常规文件，仅清理本次校验到的精确记录行 (防并发写入新记录被误删)
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "DELETE FROM download_history WHERE id = ? AND downloaded_at = ?",
                (record_id, downloaded_at),
            )
        return None

    @staticmethod
    def count_in_range(chat_id: str, start_id: int, end_id: int) -> int:
        if start_id < -(2**63) or end_id >= 2**63:
            return 0
        with db_cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM download_history WHERE chat_id = ? AND msg_id BETWEEN ? AND ?",
                (chat_id, start_id, end_id),
            )
            return int(cursor.fetchone()[0])

    @staticmethod
    def record(
        chat_id: str,
        msg_id: int,
        grouped_id: str | int | None,
        file_name: str,
        file_path: str,
        file_size: int,
        file_hash: str = "",
        task_id: str | None = None,
    ):
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                """
                INSERT OR REPLACE INTO download_history
                (chat_id, msg_id, grouped_id, file_name, file_path, file_size, file_hash, task_id, downloaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(chat_id),
                    msg_id,
                    str(grouped_id) if grouped_id else None,
                    file_name,
                    file_path,
                    file_size,
                    file_hash,
                    task_id,
                    time.time(),
                ),
            )

    @staticmethod
    def get_recent(limit: int = 50) -> list[dict[str, Any]]:
        with db_cursor() as cursor:
            cursor.execute("SELECT * FROM download_history ORDER BY downloaded_at DESC LIMIT ?", (limit,))
            rows = [dict(r) for r in cursor.fetchall()]
        return rows

    @staticmethod
    def get_hash_by_path(file_path: str) -> str:
        """按成品路径查询已记录的 SHA-256 (无记录、未计算或磁盘体积已变化返回 ''), 供发布防重键使用"""
        with db_cursor() as cursor:
            cursor.execute(
                "SELECT file_hash, file_size FROM download_history WHERE file_path = ? ORDER BY downloaded_at DESC LIMIT 1",
                (file_path,),
            )
            row = cursor.fetchone()
        if not row or not row[0]:
            return ""
        stored_hash, stored_size = str(row[0]), row[1]
        if stored_size is not None:
            try:
                st = os.stat(file_path)
            except OSError:
                return ""
            if st.st_size != int(stored_size):
                return ""
        return stored_hash

    @staticmethod
    def get_history_map() -> list[tuple[str, str]]:
        """获取所有已完成下载记录的 (file_path, chat_id)"""
        with db_cursor() as cursor:
            cursor.execute("SELECT file_path, chat_id FROM download_history")
            return cursor.fetchall()

    @staticmethod
    def get_hashes_map() -> list[tuple[str, str]]:
        """获取所有已计算哈希的 (file_path, file_hash)"""
        with db_cursor() as cursor:
            cursor.execute(
                "SELECT file_path, file_hash FROM download_history WHERE file_hash IS NOT NULL AND file_hash != ''"
            )
            return cursor.fetchall()


class DownloadBreakpointRepository:
    """下载断点归属表: 下载前登记 / 成功后清除, 支撑「删除任务时精确清理断点文件」"""

    @staticmethod
    def upsert(task_id: str, chat_id: str, msg_id: int, part_path: str, meta_path: str, final_path: str):
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                """
                INSERT OR REPLACE INTO download_breakpoints
                (task_id, chat_id, msg_id, part_path, meta_path, final_path, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (task_id, str(chat_id), int(msg_id), part_path, meta_path, final_path, time.time()),
            )

    @staticmethod
    def delete(task_id: str, chat_id: str, msg_id: int):
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "DELETE FROM download_breakpoints WHERE task_id = ? AND chat_id = ? AND msg_id = ?",
                (task_id, str(chat_id), int(msg_id)),
            )

    @staticmethod
    def get_breakpoint_paths_by_task(task_id: str) -> dict[str, list[str]]:
        """返回 {"part": [...], "meta": [...], "final": [...]}; final 为成品路径仅供展示不参与断点删除"""
        with db_cursor() as cursor:
            cursor.execute(
                "SELECT part_path, meta_path, final_path FROM download_breakpoints WHERE task_id = ?", (task_id,)
            )
            rows = cursor.fetchall()
        return {
            "part": [r[0] for r in rows],
            "meta": [r[1] for r in rows],
            "final": [r[2] for r in rows],
        }

    @staticmethod
    def get_tasks_map() -> list[tuple[str, str, str, str]]:
        """获取所有断点的 (part_path, meta_path, final_path, task_id)"""
        with db_cursor() as cursor:
            cursor.execute("SELECT part_path, meta_path, final_path, task_id FROM download_breakpoints")
            return cursor.fetchall()

    @staticmethod
    def get_all_breakpoints() -> list[dict[str, Any]]:
        """获取所有断点完整记录列表"""
        with db_cursor() as cursor:
            cursor.execute(
                "SELECT part_path, meta_path, final_path, task_id, chat_id, msg_id, created_at FROM download_breakpoints"
            )
            return [dict(r) for r in cursor.fetchall()]
