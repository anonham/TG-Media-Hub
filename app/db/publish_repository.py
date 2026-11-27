"""发布域仓储 (Publish Repository): 发布防重历史 + 占位槽生命周期 + 发布身份路径归一"""

import os
import posixpath
import re
import sqlite3
import time
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from app.db.database import db_cursor

# 占位槽可行动态 (跨任务槽复用保留态): 任务级联清理与 delete_terminal_by_task 共用此排除口径 (单点)
SLOT_ACTIVE_STATES_SQL = "state NOT IN ('sending', 'placeholder_sent', 'editing')"

_WIN_DRIVE_PATH_RE = re.compile(r"^[A-Za-z]:[/\\]")


@dataclass(frozen=True, slots=True)
class PublishHistoryRecord:
    """发布历史记录实体"""

    id: int
    target_chat_id: str
    file_path: str
    file_size: int
    published_at: float
    file_hash: str = ""
    source_type: str = "local"
    remote_file_id: str = ""
    tg_message_id: int = 0
    task_id: str = ""


def norm_file_path(p: str) -> str:
    """跨平台发布身份路径归一 (写入与查询双侧统一):
    - Windows 盘符/UNC 路径 (或 Windows 宿主上的非 POSIX 相对路径): 反斜杠归一为正斜杠 + 小写,
      解决 Windows 大小写不敏感与正反斜杠混用导致 SQLite BINARY 精确匹配失效问题;
    - POSIX 绝对路径 (`/media/...`) 及非 Windows 相对路径: 保留原始大小写, 避免 Linux 大小写敏感文件系统碰撞;
    - pikpak:// 虚拟协议路径: 保持原样返回 (远端身份大小写敏感)。
    红线: 归一路径仅作发布身份键与展示, 严禁回读作文件系统访问路径。
    """
    if not p:
        return ""
    if p.startswith("pikpak://"):
        return p
    is_unc_path = p.startswith("\\\\") or (p.startswith("//") and not p.startswith("///"))
    if _WIN_DRIVE_PATH_RE.match(p) or is_unc_path or (os.name == "nt" and not p.startswith("/")):
        return posixpath.normpath(p.replace("\\", "/")).lower()
    return posixpath.normpath(p)


class PublishHistoryRepository:
    @staticmethod
    def get_published_info(
        target_chat_id: str | int,
        file_path: str,
        file_size: int,
        file_hash: str = "",
        *,
        source_type: str = "local",
        remote_file_id: str = "",
    ) -> PublishHistoryRecord | None:
        """获取指定文件在目标频道/群组的发布历史信息。

        防重键:
        - source_type='pikpak': 按 (target_chat_id, remote_file_id) 查重;
        - source_type='local': 提供哈希时优先 (target, path, file_hash) 精确匹配,
          哈希缺失时回退三元组 (target, path, size)。
        """
        with db_cursor() as cursor:
            if source_type == "pikpak":
                cursor.execute(
                    """
                    SELECT id, target_chat_id, file_path, file_size, published_at,
                           file_hash, source_type, remote_file_id, tg_message_id,
                           COALESCE(task_id, '') AS task_id
                    FROM publish_history
                    WHERE target_chat_id = ? AND source_type = 'pikpak' AND remote_file_id = ?
                    """,
                    (str(target_chat_id), remote_file_id),
                )
            else:
                # 有意图设计: 仅库中 legacy 行 (stored file_hash='') 回退至 (target, path, size) 匹配;
                # 若库中存有哈希而本次查询无哈希 (file_hash=''), 不得跨哈希形态误判已发布 (见 test_publish_dedup_hash_key_upgrade)
                cursor.execute(
                    """
                    SELECT id, target_chat_id, file_path, file_size, published_at,
                           file_hash, source_type, remote_file_id, tg_message_id,
                           COALESCE(task_id, '') AS task_id
                    FROM publish_history
                    WHERE target_chat_id = ? AND file_path = ?
                      AND ( (? != '' AND file_hash = ? AND file_size = ? )
                         OR ( file_hash = '' AND file_size = ? ) )
                    """,
                    (str(target_chat_id), norm_file_path(file_path), file_hash, file_hash, file_size, file_size),
                )
            row = cursor.fetchone()
        if not row:
            return None
        return PublishHistoryRecord(
            id=int(row["id"]),
            target_chat_id=str(row["target_chat_id"]),
            file_path=str(row["file_path"]),
            file_size=int(row["file_size"]),
            published_at=float(row["published_at"]),
            file_hash=str(row["file_hash"] or ""),
            source_type=str(row["source_type"] or "local"),
            remote_file_id=str(row["remote_file_id"] or ""),
            tg_message_id=int(row["tg_message_id"] or 0),
            task_id=str(row["task_id"] or ""),
        )

    @staticmethod
    def is_published(
        target_chat_id: str | int,
        file_path: str,
        file_size: int,
        file_hash: str = "",
        *,
        source_type: str = "local",
        remote_file_id: str = "",
    ) -> bool:
        """校验文件是否已被成功发送到指定频道/群组。"""
        return (
            PublishHistoryRepository.get_published_info(
                target_chat_id,
                file_path,
                file_size,
                file_hash,
                source_type=source_type,
                remote_file_id=remote_file_id,
            )
            is not None
        )

    @staticmethod
    def record(
        target_chat_id: str | int,
        file_path: str,
        file_size: int,
        file_hash: str = "",
        *,
        source_type: str = "local",
        remote_file_id: str = "",
        tg_message_id: int = 0,
        task_id: str = "",
        transaction: sqlite3.Cursor | None = None,
    ):
        """记录文件发送历史 (file_hash 缺省 '' 时按旧键语义记录)。

        source_type='pikpak': file_path 存 PikPak 远端路径 (展示/调试用),
        防重键为 remote_file_id (部分唯一索引 idx_publish_pikpak_dedup 兜底);
        INSERT OR REPLACE 沿用既有幂等覆盖语义 (同键重复 record 覆盖而非报错)。

        写入侧路径归一: file_path 统一经 norm_file_path 落库,
        查询侧单值等值完整命中 UNIQUE 前缀索引 (is_published 同口径)。
        """
        with nullcontext(transaction) if transaction is not None else db_cursor(commit=True) as cursor:
            common_params = (
                str(target_chat_id),
                norm_file_path(file_path),
                file_size,
                file_hash,
                time.time(),
                source_type,
                remote_file_id,
                tg_message_id,
                task_id or "",
            )
            if source_type == "pikpak":
                cursor.execute(
                    """
                    INSERT INTO publish_history
                    (target_chat_id, file_path, file_size, file_hash, published_at,
                     source_type, remote_file_id, tg_message_id, task_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(target_chat_id, remote_file_id) WHERE source_type='pikpak'
                    DO UPDATE SET
                        file_path = excluded.file_path,
                        file_size = excluded.file_size,
                        file_hash = excluded.file_hash,
                        published_at = excluded.published_at,
                        tg_message_id = excluded.tg_message_id,
                        task_id = excluded.task_id
                    """,
                    common_params,
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO publish_history
                    (target_chat_id, file_path, file_size, file_hash, published_at,
                     source_type, remote_file_id, tg_message_id, task_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(target_chat_id, file_path, file_size) WHERE source_type='local'
                    DO UPDATE SET
                        file_hash = excluded.file_hash,
                        published_at = excluded.published_at,
                        remote_file_id = excluded.remote_file_id,
                        tg_message_id = excluded.tg_message_id,
                        task_id = excluded.task_id
                    """,
                    common_params,
                )


class PlaceholderSlotRepository:
    """发布占位槽仓储: 失败占位卡的生命周期持久化。

    - 身份键 = (target_chat_id, file_path, file_size), 对齐 publish_history 防重三元组;
      file_path 语义 = 发布身份逻辑路径 (R6 已裁决): 本地为绝对路径, PikPak 为
      pikpak://{parent_folder_id}/{name};
    - slot_kind: 'backfillable' (1:1 可回填卡) | 'group_summary' (整组失败汇总卡, 各成员
      行记录共享汇总卡 msg_id 但结构上禁入回填 —— R2 严禁多文件共享同一 msg_id 参与回填);
    - state (backfillable): placeholder_sent → editing → published | failed_terminal | lost;
      failed_terminal 可经后续成功重试原地复活 (CAS 守卫三态);
      state (group_summary): placeholder_sent → flipped | failed_terminal (文本终态);
    - 单点防重: UNIQUE 三键 + CAS 状态转移; 红线: 本表为占位信息唯一落点, 严禁写 publish_history。
    """

    @staticmethod
    def get_by_id(slot_id: int) -> dict[str, Any] | None:
        with db_cursor() as cursor:
            cursor.execute("SELECT * FROM placeholder_slots WHERE id = ?", (int(slot_id),))
            row = cursor.fetchone()
        return dict(row) if row else None

    @staticmethod
    def get_by_identity(target_chat_id, file_path: str, file_size: int) -> dict[str, Any] | None:
        with db_cursor() as cursor:
            cursor.execute(
                "SELECT * FROM placeholder_slots WHERE target_chat_id = ? AND file_path = ? AND file_size = ?",
                (str(target_chat_id), norm_file_path(file_path), int(file_size)),
            )
            row = cursor.fetchone()
        return dict(row) if row else None

    @staticmethod
    def insert_new_slot(
        target_chat_id,
        file_path: str,
        file_size: int,
        *,
        slot_kind: str = "backfillable",
        state: str = "placeholder_sent",
        placeholder_msg_id: int = 0,
        client_label: str = "",
        sender_account_id: int = 0,
        task_id: str = "",
        fail_reason: str = "",
    ) -> int:
        """插入新槽 (默认 state=placeholder_sent; terminal 卡直接落 failed_terminal)。
        UNIQUE 冲突 (并发防重单点) 抛 sqlite3.IntegrityError, 由调用方捕获后转入槽
        复用路径 (fail_count++ 不发新卡)。"""
        now = time.time()
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                """
                INSERT INTO placeholder_slots
                (target_chat_id, file_path, file_size, slot_kind, state, placeholder_msg_id,
                  fail_reason, client_label, sender_account_id, task_id,
                  placeholder_sent_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(target_chat_id),
                    norm_file_path(file_path),
                    int(file_size),
                    slot_kind,
                    state,
                    int(placeholder_msg_id),
                    fail_reason,
                    client_label,
                    int(sender_account_id),
                    task_id,
                    now,
                    now,
                    now,
                ),
            )
            slot_id = int(cursor.lastrowid or 0)
        return slot_id

    @staticmethod
    def activate_reserved_slot(
        slot_id: int,
        *,
        placeholder_msg_id: int,
        state: str = "placeholder_sent",
    ) -> bool:
        """激活发卡前预占的 sending 槽位，写入实际消息 ID 与目标状态。"""
        now = time.time()
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                """
                UPDATE placeholder_slots
                   SET state = ?, placeholder_msg_id = ?, placeholder_sent_at = ?, updated_at = ?
                 WHERE id = ? AND state = 'sending'
                """,
                (state, int(placeholder_msg_id), now, now, int(slot_id)),
            )
            return cursor.rowcount > 0

    @staticmethod
    def delete_unactivated_slot(slot_id: int) -> bool:
        """仅删除尚未激活的 sending 预占槽位 (发卡失败或取消时的安全回收)."""
        with db_cursor(commit=True) as cursor:
            cursor.execute("DELETE FROM placeholder_slots WHERE id = ? AND state = 'sending'", (int(slot_id),))
            return cursor.rowcount > 0

    @staticmethod
    def delete_stale_sending_slots() -> int:
        """启动期自愈: 删除崩溃遗留的未激活 sending 预占槽 (lifespan 单实例锁 + 无活跃任务,
        不可能竞态活跃预占; 已发卡未激活的残留为无槽卡, 下次失败可能重复发卡 —— 装饰性)."""
        with db_cursor(commit=True) as cursor:
            cursor.execute("DELETE FROM placeholder_slots WHERE state = 'sending'")
            return cursor.rowcount

    @staticmethod
    def increment_fail(slot_id: int, fail_reason: str = "") -> None:
        """原子递增失败计数 (J6: 同文件重复失败复用既有槽, 不发新卡防刷屏)"""
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "UPDATE placeholder_slots SET fail_count = fail_count + 1, fail_reason = ?, updated_at = ? WHERE id = ?",
                (fail_reason, time.time(), int(slot_id)),
            )

    @staticmethod
    def cas_acquire_edit(target_chat_id, file_path: str, file_size: int) -> bool:
        """CAS 获取回填编辑权 (R5 独占守卫 + 终态可复活; slot_kind 双过滤 —— J11):
        仅 backfillable 槽且 state ∈ {placeholder_sent, failed_terminal} 可获取 (排除 editing 防并发双写)。
        返回 True = 获得编辑权 (状态已置 editing); False = 他处持有/终态不可复活/非回填槽。"""
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                """
                UPDATE placeholder_slots
                   SET state = 'editing', edit_attempted_at = ?, updated_at = ?
                 WHERE target_chat_id = ? AND file_path = ? AND file_size = ?
                   AND slot_kind = 'backfillable'
                   AND state IN ('placeholder_sent', 'failed_terminal')
                """,
                (time.time(), time.time(), str(target_chat_id), norm_file_path(file_path), int(file_size)),
            )
            acquired = cursor.rowcount > 0
        return acquired

    @staticmethod
    def mark_published(slot_id: int, real_msg_id: int) -> None:
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "UPDATE placeholder_slots SET state = 'published', real_msg_id = ?, resolved_at = ?, updated_at = ? WHERE id = ?",
                (int(real_msg_id), time.time(), time.time(), int(slot_id)),
            )

    @staticmethod
    def mark_lost(slot_id: int, fail_reason: str = "") -> None:
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "UPDATE placeholder_slots SET state = 'lost', fail_reason = ?, resolved_at = ?, updated_at = ? WHERE id = ?",
                (fail_reason, time.time(), time.time(), int(slot_id)),
            )

    @staticmethod
    def mark_failed_terminal(slot_id: int, fail_reason: str = "") -> bool:
        """条件终态化: 保护 editing 编辑锁与已发布/已翻转等非待终态槽不被无条件覆盖。"""
        now = time.time()
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                """
                UPDATE placeholder_slots
                   SET state = 'failed_terminal', fail_reason = ?, resolved_at = ?, updated_at = ?
                 WHERE id = ? AND state IN ('placeholder_sent', 'failed_terminal')
                """,
                (fail_reason, now, now, int(slot_id)),
            )
            return cursor.rowcount > 0

    @staticmethod
    def mark_failed_terminal_for_rejection(slot_id: int, task_id: str, expected_error: str, fail_reason: str) -> bool:
        """Keep a retry racing with Telegram edits from terminalizing its slots."""
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                """UPDATE placeholder_slots
                   SET state = 'failed_terminal', fail_reason = ?, resolved_at = ?, updated_at = ?
                   WHERE id = ? AND task_id = ? AND state = 'placeholder_sent'
                     AND EXISTS (SELECT 1 FROM task_queue
                                 WHERE id = ? AND status = 'failed' AND error_detail = ?)""",
                (fail_reason, time.time(), time.time(), int(slot_id), task_id, task_id, expected_error),
            )
            return cursor.rowcount > 0

    @staticmethod
    def mark_flipped(slot_id: int) -> None:
        """group_summary 专属: 整组重发成功后汇总卡 caption 翻 ✅ (R2 best-effort)"""
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "UPDATE placeholder_slots SET state = 'flipped', resolved_at = ?, updated_at = ? WHERE id = ? AND slot_kind = 'group_summary'",
                (time.time(), time.time(), int(slot_id)),
            )

    @staticmethod
    def revert_editing_to_sent() -> int:
        """启动期自愈 (R5): 崩溃遗留的 editing 孤儿槽回滚为 placeholder_sent。
        注意: 源于 failed_terminal 的槽会被短暂复活为可回填态 —— fail_count≥4
        保留, 下次失败立即重终态 (自校正, 已接受的瞬态不一致)。"""
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "UPDATE placeholder_slots SET state = 'placeholder_sent', updated_at = ? WHERE state = 'editing'",
                (time.time(),),
            )
            changed = cursor.rowcount
        return changed

    @staticmethod
    def reset_to_sent(slot_id: int) -> None:
        """编辑失败回退 (非终态类): editing → placeholder_sent (⏳ 卡保留可复用)"""
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "UPDATE placeholder_slots SET state = 'placeholder_sent', updated_at = ? WHERE id = ? AND state = 'editing'",
                (time.time(), int(slot_id)),
            )

    @staticmethod
    def list_by_task(task_id: str) -> list[dict[str, Any]]:
        with db_cursor() as cursor:
            cursor.execute("SELECT * FROM placeholder_slots WHERE task_id = ?", (task_id,))
            rows = cursor.fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def list_by_placeholder_msg_id(target_chat_id, placeholder_msg_id: int) -> list[dict[str, Any]]:
        """按频道与占位消息 ID 查询共享同一卡片的所有槽位 (用于 group_summary 翻转完整性校验)."""
        with db_cursor() as cursor:
            cursor.execute(
                "SELECT * FROM placeholder_slots WHERE target_chat_id = ? AND placeholder_msg_id = ?",
                (str(target_chat_id), int(placeholder_msg_id)),
            )
            rows = cursor.fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def states_for_identities(target_chat_id, identities: list[tuple[str, int]]) -> dict[str, str]:
        """按身份批量查当前 state (诊断富化用; 身份数量由调用方按信封封顶 ≤50)。
        返回 {"{file_path}|{file_size}": state}, 键保持调用方传入的原始路径形态。
        单条 IN 批查 (P1: 替代逐身份 N+1); 写入侧已归一 (insert_new_slot 统一
        norm_file_path 落库), 查询键同样归一后等值匹配。"""
        if not identities:
            return {}
        chat = str(target_chat_id)
        norm_to_orig: dict[str, str] = {}
        for file_path, file_size in identities:
            norm_key = f"{norm_file_path(file_path)}|{int(file_size)}"
            norm_to_orig.setdefault(norm_key, f"{file_path}|{int(file_size)}")
        out: dict[str, str] = {}
        with db_cursor() as cursor:
            if norm_to_orig:
                params: list[Any] = [chat]
                params.extend(norm_to_orig.keys())
                placeholders = ",".join("?" * len(norm_to_orig))
                cursor.execute(
                    f"SELECT file_path || '|' || file_size AS identity_key, state FROM placeholder_slots "
                    f"WHERE target_chat_id = ? AND (file_path || '|' || file_size) IN ({placeholders})",
                    params,
                )
                rows = {r["identity_key"]: r["state"] for r in cursor.fetchall()}
                for norm_key, orig_key in norm_to_orig.items():
                    state = rows.get(norm_key)
                    if state is not None:
                        out[orig_key] = state
        return out

    @staticmethod
    def delete_terminal_by_task(task_id: str) -> int:
        """任务级联清理: 排除式保留可行动态 (placeholder_sent/editing 支持跨任务槽
        复用), 清除一切终态 (published/lost/failed_terminal/flipped)。返回删除行数。"""
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                f"DELETE FROM placeholder_slots WHERE task_id = ? AND {SLOT_ACTIVE_STATES_SQL}",
                (task_id,),
            )
            removed = cursor.rowcount
        return removed
