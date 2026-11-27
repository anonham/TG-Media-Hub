"""转发域仓储 (Forward Repository): 转发映射历史 + 防重账本 (skips/coverage) 三表体系"""

import logging
import re
import sqlite3
import time
from collections.abc import Sequence
from typing import Any

from app.db.database import db_cursor, db_immediate_tx
from app.db.task_repository import _AsyncFace

logger = logging.getLogger(__name__)


class ForwardHistoryRepository:
    @staticmethod
    def save_forward_batch(records: list):
        """
        批量保存消息转发映射
        records: list of tuples (task_id, source_chat, source_msg_id, target_chat, target_msg_id)
        """
        if not records:
            return
        now = time.time()
        with db_cursor(commit=True) as cursor:
            cursor.executemany(
                """
                INSERT OR REPLACE INTO forward_history
                (task_id, source_chat, source_msg_id, target_chat, target_msg_id, forwarded_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [(r[0], str(r[1]), int(r[2]), str(r[3]), int(r[4]), now) for r in records],
            )

    @staticmethod
    def query_history(
        source_chat: str | None = None,
        target_chat: str | None = None,
        source_msg_id: int | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """查询转发映射历史"""
        query = "SELECT * FROM forward_history WHERE 1=1"
        params: list[Any] = []

        if source_chat:
            query += " AND source_chat = ?"
            params.append(str(source_chat))
        if target_chat:
            query += " AND target_chat = ?"
            params.append(str(target_chat))
        if source_msg_id is not None:
            query += " AND source_msg_id = ?"
            params.append(int(source_msg_id))

        query += " ORDER BY id DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with db_cursor() as cursor:
            cursor.execute(query, params)
            rows = [dict(r) for r in cursor.fetchall()]
        return rows

    save_forward_batch_async = _AsyncFace(save_forward_batch)
    query_history_async = _AsyncFace(query_history)


class ForwardLedgerRepository:
    """转发防重账本：成功映射 + 处置跳过 + 覆盖区间，三表同一事务落库"""

    @staticmethod
    def get_processed_ids(source_chat: str, target_chat: str, msg_ids: list[int]) -> set[int]:
        """
        批量判定哪些源消息 ID 已被处理 (成功转发到该目标，或已被目标无关的过滤 skip / 该目标的 bad_id skip 处置)
        一批一次往返；IN 占位符仅由数量生成，不含任何外部输入，外部值全部参数绑定
        """
        id_list = [int(i) for i in msg_ids]
        if not id_list:
            return set()
        placeholders = ",".join(["?"] * len(id_list))
        done: set[int] = set()
        with db_cursor() as cursor:
            cursor.execute(
                f"SELECT source_msg_id FROM forward_history "
                f"WHERE source_chat = ? AND target_chat = ? AND source_msg_id IN ({placeholders})",
                [str(source_chat), str(target_chat)] + id_list,
            )
            done.update(r[0] for r in cursor.fetchall())
            cursor.execute(
                f"SELECT msg_id FROM forward_skips "
                f"WHERE source_chat = ? AND msg_id IN ({placeholders}) "
                f"AND (target_chat IS NULL OR target_chat = ?)",
                [str(source_chat)] + id_list + [str(target_chat)],
            )
            done.update(r[0] for r in cursor.fetchall())
        return done

    @staticmethod
    def commit_batch_records(
        task_id: str | None,
        source_chat: str,
        target_chat: str,
        mappings: list[tuple[int, int]],
        skips: list[tuple[int, str, str | None, str | None]],
        seg_start: int | None = None,
        seg_end: int | None = None,
    ):
        """
        单事务落库一批的全部账本记录：
        - mappings: [(source_msg_id, target_msg_id)] -> INSERT OR REPLACE forward_history (幂等)
        - skips: [(msg_id, skip_type, reason, skip_target)] -> INSERT OR IGNORE forward_skips (首条留痕)
        - 覆盖区间 [seg_start, seg_end] 并入 forward_coverage (相邻/重叠合并)
        三者任一失败整体回滚，中断不会出现"区间盖住但账本缺行"
        (本方法保留手工连接管理: OperationalError 退避重试需逐次取还连接 + 尝试间 sleep,
        与 db_immediate_tx 的单事务样板形状不同)
        """
        last_err: Exception | None = None
        for _attempt in range(3):
            try:
                with db_immediate_tx() as cursor:
                    now = time.time()
                    if mappings:
                        cursor.executemany(
                            "INSERT OR REPLACE INTO forward_history "
                            "(task_id, source_chat, source_msg_id, target_chat, target_msg_id, forwarded_at) "
                            "VALUES (?, ?, ?, ?, ?, ?)",
                            [(task_id, str(source_chat), int(s), str(target_chat), int(t), now) for s, t in mappings],
                        )
                    if skips:
                        cursor.executemany(
                            "INSERT OR IGNORE INTO forward_skips "
                            "(task_id, source_chat, msg_id, skip_type, reason, target_chat, created_at) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)",
                            [
                                (
                                    task_id,
                                    str(source_chat),
                                    int(m),
                                    str(st),
                                    rn,
                                    (str(tg) if tg is not None else None),
                                    now,
                                )
                                for m, st, rn, tg in skips
                            ],
                        )
                    if seg_start is not None and seg_end is not None and seg_end >= seg_start:
                        cursor.execute(
                            "SELECT id, seg_start, seg_end FROM forward_coverage "
                            "WHERE source_chat = ? AND target_chat = ? AND seg_end >= ? AND seg_start <= ?",
                            (str(source_chat), str(target_chat), int(seg_start) - 1, int(seg_end) + 1),
                        )
                        rows = cursor.fetchall()
                        if rows:
                            merged_start = min(int(seg_start), min(r["seg_start"] for r in rows))
                            merged_end = max(int(seg_end), max(r["seg_end"] for r in rows))
                            cursor.executemany("DELETE FROM forward_coverage WHERE id = ?", [(r["id"],) for r in rows])
                            cursor.execute(
                                "INSERT INTO forward_coverage (source_chat, target_chat, seg_start, seg_end, updated_at) "
                                "VALUES (?, ?, ?, ?, ?)",
                                (str(source_chat), str(target_chat), merged_start, merged_end, now),
                            )
                        else:
                            cursor.execute(
                                "INSERT INTO forward_coverage (source_chat, target_chat, seg_start, seg_end, updated_at) "
                                "VALUES (?, ?, ?, ?, ?)",
                                (str(source_chat), str(target_chat), int(seg_start), int(seg_end), now),
                            )
                return
            except sqlite3.OperationalError as e:
                last_err = e
                time.sleep(0.2)
        # 循环耗尽退出必经 except 分支记录 last_err; None 分支仅为类型收窄, 不可达
        assert last_err is not None
        raise last_err

    @staticmethod
    def get_task_receipt(task_id: str) -> dict[str, Any]:
        """
        按 task_id 聚合转发回执明细:
        - forwarded: 成功映射条数
        - bad_ids: 坏消息隔离 ID 清单 (上限 500)
        - filtered_detail: 过滤类处置按 skip_type 分组 (含样本 ID，上限每组 20)
        """
        with db_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM forward_history WHERE task_id = ?", (task_id,))
            forwarded = cursor.fetchone()[0]
            cursor.execute(
                "SELECT msg_id FROM forward_skips WHERE task_id = ? AND skip_type = 'bad_id' ORDER BY msg_id ASC LIMIT 500",
                (task_id,),
            )
            bad_ids = [r[0] for r in cursor.fetchall()]
            cursor.execute(
                "SELECT skip_type, msg_id FROM forward_skips WHERE task_id = ? AND skip_type != 'bad_id' ORDER BY msg_id ASC",
                (task_id,),
            )
            groups: dict[str, dict[str, Any]] = {}
            for skip_type, msg_id in cursor.fetchall():
                g = groups.setdefault(skip_type, {"count": 0, "sample_ids": []})
                g["count"] += 1
                if len(g["sample_ids"]) < 20:
                    g["sample_ids"].append(msg_id)
        filtered_detail = [
            {"skip_type": st, "count": g["count"], "sample_ids": g["sample_ids"]}
            for st, g in sorted(groups.items(), key=lambda kv: -kv[1]["count"])
        ]
        return {"forwarded": forwarded, "bad_ids": bad_ids, "filtered_detail": filtered_detail}

    @staticmethod
    def get_coverage_segments(source_chat: str, target_chat: str) -> list[tuple[int, int]]:
        """获取 (源,目标) 的已覆盖区间列表，按 seg_start 升序 (未合并的原始行)"""
        with db_cursor() as cursor:
            cursor.execute(
                "SELECT seg_start, seg_end FROM forward_coverage "
                "WHERE source_chat = ? AND target_chat = ? ORDER BY seg_start ASC",
                (str(source_chat), str(target_chat)),
            )
            rows = [(r["seg_start"], r["seg_end"]) for r in cursor.fetchall()]
        return rows

    @staticmethod
    def get_ledger_runs(
        source_chat: str, target_chat: str, min_id: int | None = None, max_id: int | None = None
    ) -> dict[str, list[int]]:
        """
        获取范围内逐条账本 ID 列表，按渲染语义分类:
        - forwarded: 成功转发到该目标 (forward_history)
        - filtered:  过滤类处置 (skip_type != bad_id，target 无关或同目标)
        - bad:       坏消息隔离 (skip_type = bad_id 且同目标)
        同时存在于 history 与 skips 的 ID 以 forwarded (绿) 优先
        """
        with db_cursor() as cursor:
            cursor.execute(
                "SELECT source_msg_id FROM forward_history "
                "WHERE source_chat = ? AND target_chat = ? "
                "AND source_msg_id >= ? AND source_msg_id <= ?",
                (str(source_chat), str(target_chat), int(min_id or 0), int(max_id or (1 << 62))),
            )
            forwarded = {r[0] for r in cursor.fetchall()}
            cursor.execute(
                "SELECT msg_id, skip_type FROM forward_skips "
                "WHERE source_chat = ? AND msg_id >= ? AND msg_id <= ? "
                "AND (target_chat IS NULL OR target_chat = ?)",
                (str(source_chat), int(min_id or 0), int(max_id or (1 << 62)), str(target_chat)),
            )
            filtered: list[int] = []
            bad: list[int] = []
            for msg_id, skip_type in cursor.fetchall():
                if msg_id in forwarded:
                    continue  # 绿优先
                (bad if skip_type == "bad_id" else filtered).append(msg_id)
        return {"forwarded": sorted(forwarded), "filtered": sorted(filtered), "bad": sorted(bad)}

    @staticmethod
    def get_ledger_bounds(source_chat: str, target_chat: str) -> tuple[int, int] | None:
        """获取 (源,目标) 账本全量 ID 边界 [min_id, max_id] (history ∪ skips, 语义同
        get_ledger_runs 无窗口: history 同目标, skips 目标无关或同目标)。两表均无记录返回 None。

        SQL 标量聚合替代全表 fetchall 求边界 (P1): 数十万行账本不再装入 Python 堆。
        """
        with db_cursor() as cursor:
            cursor.execute(
                "SELECT MIN(source_msg_id), MAX(source_msg_id) FROM forward_history "
                "WHERE source_chat = ? AND target_chat = ?",
                (str(source_chat), str(target_chat)),
            )
            h_min, h_max = cursor.fetchone()
            cursor.execute(
                "SELECT MIN(msg_id), MAX(msg_id) FROM forward_skips "
                "WHERE source_chat = ? AND (target_chat IS NULL OR target_chat = ?)",
                (str(source_chat), str(target_chat)),
            )
            s_min, s_max = cursor.fetchone()
        bounds = [v for v in (h_min, h_max, s_min, s_max) if v is not None]
        if not bounds:
            return None
        return (min(bounds), max(bounds))

    @staticmethod
    def count_processed_in_range(source_chat: str, target_chat: str, min_id: int, max_id: int) -> int:
        """统计 [min_id, max_id] 内已处理 (成功映射 ∪ 处置 skip) 的去重消息数"""
        with db_cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM ("
                " SELECT source_msg_id AS id FROM forward_history "
                " WHERE source_chat = ? AND target_chat = ? AND source_msg_id >= ? AND source_msg_id <= ? "
                " UNION "
                " SELECT msg_id FROM forward_skips "
                " WHERE source_chat = ? AND msg_id >= ? AND msg_id <= ? "
                " AND (target_chat IS NULL OR target_chat = ?)"
                ")",
                (
                    str(source_chat),
                    str(target_chat),
                    int(min_id),
                    int(max_id),
                    str(source_chat),
                    int(min_id),
                    int(max_id),
                    str(target_chat),
                ),
            )
            n = cursor.fetchone()[0]
        return n

    @staticmethod
    def query_skips(
        source_chat: str | None = None,
        target_chat: str | None = None,
        skip_type: str | None = None,
        task_id: str | None = None,
        page: int = 1,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], int]:
        """分页多维查询转发清洗与坏消息隔离账本"""
        with db_cursor() as cursor:
            where_clauses = []
            params: list[Any] = []

            if source_chat:
                where_clauses.append("source_chat = ?")
                params.append(str(source_chat))
            if target_chat:
                where_clauses.append("target_chat = ?")
                params.append(str(target_chat))
            if skip_type:
                where_clauses.append("skip_type = ?")
                params.append(skip_type)
            if task_id:
                where_clauses.append("task_id = ?")
                params.append(task_id)

            where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

            cursor.execute(f"SELECT COUNT(*) FROM forward_skips{where_sql}", tuple(params))
            total_count = cursor.fetchone()[0]

            offset = max(0, (page - 1) * limit)
            query_sql = f"SELECT * FROM forward_skips{where_sql} ORDER BY id DESC LIMIT ? OFFSET ?"
            exec_params = list(params)
            exec_params.extend([limit, offset])
            cursor.execute(query_sql, tuple(exec_params))
            rows = [dict(r) for r in cursor.fetchall()]
        return rows, total_count

    @staticmethod
    def upsert_peer_aliases(aliases: Sequence[str], peer_id: str) -> None:
        """记录非数字频道别名到规范 peer_id 的映射 (用于后续同步/离线路径对齐)."""
        peer_str = str(peer_id or "").strip()
        if not aliases or not re.fullmatch(r"-?\d+", peer_str):
            return
        rows = [(a, peer_str, time.time()) for a in aliases if a]
        if not rows:
            return
        with db_cursor(commit=True) as cursor:
            cursor.executemany(
                "INSERT OR REPLACE INTO peer_aliases (alias_key, peer_id, updated_at) VALUES (?, ?, ?)",
                rows,
            )

    @staticmethod
    def get_peer_id_by_alias(alias_key: str) -> str | None:
        """按规范化别名键查询已缓存的数字 peer_id."""
        if not alias_key:
            return None
        with db_cursor() as cursor:
            cursor.execute("SELECT peer_id FROM peer_aliases WHERE alias_key = ?", (alias_key,))
            row = cursor.fetchone()
        return str(row["peer_id"]) if row and row["peer_id"] else None

    get_processed_ids_async = _AsyncFace(get_processed_ids)
    commit_batch_records_async = _AsyncFace(commit_batch_records)
    get_task_receipt_async = _AsyncFace(get_task_receipt)
    get_coverage_segments_async = _AsyncFace(get_coverage_segments)
    get_ledger_runs_async = _AsyncFace(get_ledger_runs)
    get_ledger_bounds_async = _AsyncFace(get_ledger_bounds)
    count_processed_in_range_async = _AsyncFace(count_processed_in_range)
    query_skips_async = _AsyncFace(query_skips)
