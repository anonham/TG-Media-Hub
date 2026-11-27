import sqlite3
import time
from typing import Any

from app.db.database import db_cursor, db_immediate_tx
from app.db.task_repository import _AsyncFace

_HANDOVER_KEY_PREFIX = "_internal.telegram_handover:"


class AccountRepository:
    """多 Userbot 账号数据库操作仓储"""

    @staticmethod
    def get_all(only_active: bool = False) -> list[dict[str, Any]]:
        with db_cursor() as cursor:
            if only_active:
                cursor.execute("SELECT * FROM tg_accounts WHERE is_active = 1 ORDER BY is_default DESC, id ASC")
            else:
                cursor.execute("SELECT * FROM tg_accounts ORDER BY is_default DESC, id ASC")
            return [dict(r) for r in cursor.fetchall()]

    @staticmethod
    def get_by_id(account_id: int) -> dict[str, Any] | None:
        with db_cursor() as cursor:
            cursor.execute("SELECT * FROM tg_accounts WHERE id = ?", (account_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    @staticmethod
    def get_default() -> dict[str, Any] | None:
        with db_cursor() as cursor:
            cursor.execute("SELECT * FROM tg_accounts WHERE is_default = 1 AND is_active = 1 LIMIT 1")
            row = cursor.fetchone()
            if not row:
                cursor.execute("SELECT * FROM tg_accounts WHERE is_active = 1 ORDER BY id ASC LIMIT 1")
                row = cursor.fetchone()
            return dict(row) if row else None

    @staticmethod
    def create(
        account_name: str,
        phone: str,
        session_file: str,
        api_id: int,
        api_hash: str,
        is_active: int = 1,
        is_default: int = 0,
        max_concurrency: int = 1,
        tier: str = "primary",
        device_model: str = "",
        system_version: str = "",
        app_version: str = "",
        lang_code: str = "en",
    ) -> dict[str, Any]:
        now = time.time()
        try:
            with db_immediate_tx() as cursor:
                if is_default:
                    cursor.execute("UPDATE tg_accounts SET is_default = 0")
                cursor.execute(
                    """
                    INSERT INTO tg_accounts
                    (account_name, phone, session_file, api_id, api_hash, is_active, is_default, max_concurrency, tier, device_model, system_version, app_version, lang_code, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_name,
                        phone,
                        session_file,
                        api_id,
                        api_hash,
                        is_active,
                        is_default,
                        max_concurrency,
                        tier,
                        device_model,
                        system_version,
                        app_version,
                        lang_code,
                        now,
                        now,
                    ),
                )
                acc_id = cursor.lastrowid
        except sqlite3.IntegrityError as e:
            raise ValueError(f"账号代号 '{account_name}' 或会话文件已存在，禁止重名") from e
        assert acc_id is not None
        created = AccountRepository.get_by_id(acc_id)
        # 插入成功后按主键回读必然命中; None 分支仅为类型收窄, 不可达
        assert created is not None
        return created

    @staticmethod
    def update_status(
        account_id: int,
        status: str,
        user_id: int | None = None,
        username: str | None = None,
        first_name: str | None = None,
        dc_id: int | None = None,
    ):
        now = time.time()
        updates = ["status = ?", "updated_at = ?"]
        params: list[Any] = [status, now]
        if user_id is not None:
            updates.append("user_id = ?")
            params.append(user_id)
        if username is not None:
            updates.append("username = ?")
            params.append(username)
        if first_name is not None:
            updates.append("first_name = ?")
            params.append(first_name)
        if dc_id is not None:
            updates.append("dc_id = ?")
            params.append(dc_id)
        params.append(account_id)
        with db_cursor(commit=True) as cursor:
            cursor.execute(f"UPDATE tg_accounts SET {', '.join(updates)} WHERE id = ?", tuple(params))

    @staticmethod
    def update_device_fingerprint(
        account_id: int, device_model: str, system_version: str, app_version: str, lang_code: str = "en"
    ):
        now = time.time()
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                """
                UPDATE tg_accounts
                SET device_model = ?, system_version = ?, app_version = ?, lang_code = ?, updated_at = ?
                WHERE id = ?
                """,
                (device_model, system_version, app_version, lang_code, now, account_id),
            )

    @staticmethod
    def mark_flood_wait(account_id: int, wait_seconds: int):
        now = time.time()
        flood_until = now + wait_seconds
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "UPDATE tg_accounts SET status = 'flood_wait', flood_wait_until = ?, updated_at = ? WHERE id = ?",
                (flood_until, now, account_id),
            )

    @staticmethod
    def update_premium(account_id: int, is_premium: bool):
        now = time.time()
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "UPDATE tg_accounts SET is_premium = ?, premium_checked_at = ?, updated_at = ? WHERE id = ?",
                (1 if is_premium else 0, now, now, account_id),
            )

    @staticmethod
    def commit_verified_login(
        account_id: int,
        *,
        phone: str | None = None,
        expected_user_id: int | None = None,
        expected_phone: str | None = None,
        expected_session_file: str | None = None,
        require_default: bool = False,
        handover_id: str | None = None,
        device_model: str = "",
        system_version: str = "",
        app_version: str = "",
        lang_code: str = "en",
        user_id: int,
        username: str | None,
        first_name: str,
        dc_id: int,
        is_premium: bool,
    ) -> dict[str, Any]:
        """单事务守卫式提交已验证的 Telegram 登录结果 (防停用/删除/切换默认/清空或改绑 user_id/换号/换会话文件)."""
        now = time.time()
        with db_immediate_tx() as cursor:
            cursor.execute(
                "SELECT id, phone, session_file, is_active, is_default, user_id FROM tg_accounts WHERE id = ?",
                (account_id,),
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError("目标账号记录已不存在，拒绝完成登录")
            if not bool(row["is_active"]):
                raise ValueError("账号已禁用，拒绝复活或激活登录")
            if require_default and not bool(row["is_default"]):
                raise ValueError("默认主账号绑定已变更，拒绝完成登录")

            existing_uid = int(row["user_id"]) if row["user_id"] is not None else None
            if existing_uid != expected_user_id:
                raise ValueError("账号绑定 Telegram 用户状态已变更，当前登录挑战已失效")

            if expected_phone is not None and (row["phone"] or "") != expected_phone:
                raise ValueError("登录期间账号绑定手机号已变更，拒绝完成登录")
            if expected_session_file is not None and (row["session_file"] or "") != expected_session_file:
                raise ValueError("登录期间账号会话目标已变更，拒绝完成登录")

            updates = [
                "status = 'authenticated'",
                "user_id = ?",
                "username = ?",
                "first_name = ?",
                "dc_id = ?",
                "is_premium = ?",
                "premium_checked_at = ?",
                "updated_at = ?",
            ]
            params: list[Any] = [
                user_id,
                username,
                first_name,
                dc_id,
                1 if is_premium else 0,
                now,
                now,
            ]
            if phone and ((require_default and existing_uid is None) or not (row["phone"] or "").strip()):
                updates.append("phone = ?")
                params.append(phone)
            if device_model and system_version and app_version:
                updates.extend(["device_model = ?", "system_version = ?", "app_version = ?", "lang_code = ?"])
                params.extend([device_model, system_version, app_version, lang_code or "en"])

            where_clause = "id = ? AND is_active = 1" + (" AND is_default = 1" if require_default else "")
            params.append(account_id)
            cursor.execute(f"UPDATE tg_accounts SET {', '.join(updates)} WHERE {where_clause}", tuple(params))
            if cursor.rowcount != 1:
                raise ValueError("账号状态已变更，无法提交登录结果")

            if handover_id is not None:
                cursor.execute(
                    "INSERT OR REPLACE INTO system_settings (key, value, category, updated_at) VALUES (?, ?, ?, ?)",
                    (f"{_HANDOVER_KEY_PREFIX}{account_id}", handover_id, "_internal", now),
                )

            cursor.execute("SELECT * FROM tg_accounts WHERE id = ?", (account_id,))
            updated = cursor.fetchone()
            if updated is None:
                raise ValueError("提交后回读账号记录失败")
            return dict(updated)

    @staticmethod
    def get_committed_handover(account_id: int) -> str | None:
        with db_cursor() as cursor:
            cursor.execute("SELECT value FROM system_settings WHERE key = ?", (f"{_HANDOVER_KEY_PREFIX}{account_id}",))
            row = cursor.fetchone()
            return str(row["value"]) if row is not None else None

    @staticmethod
    def update(
        account_id: int,
        account_name: str | None = None,
        phone: str | None = None,
        max_concurrency: int | None = None,
        is_active: bool | None = None,
        is_default: bool | None = None,
        tier: str | None = None,
        device_model: str | None = None,
        system_version: str | None = None,
        app_version: str | None = None,
        lang_code: str | None = None,
    ) -> dict[str, Any] | None:
        now = time.time()
        updates = ["updated_at = ?"]
        params: list[Any] = [now]
        field_values = (
            ("account_name", account_name),
            ("phone", phone),
            ("max_concurrency", max_concurrency),
            ("is_active", (1 if is_active else 0) if is_active is not None else None),
            ("is_default", (1 if is_default else 0) if is_default is not None else None),
            ("tier", tier),
            ("device_model", device_model),
            ("system_version", system_version),
            ("app_version", app_version),
            ("lang_code", lang_code),
        )
        for col, val in field_values:
            if val is not None:
                updates.append(f"{col} = ?")
                params.append(val)

        params.append(account_id)
        try:
            with db_immediate_tx() as cursor:
                if is_default:
                    cursor.execute("SELECT 1 FROM tg_accounts WHERE id = ?", (account_id,))
                    if cursor.fetchone() is None:
                        return None
                    cursor.execute("UPDATE tg_accounts SET is_default = 0")
                cursor.execute(f"UPDATE tg_accounts SET {', '.join(updates)} WHERE id = ?", tuple(params))
        except sqlite3.IntegrityError as e:
            target_name = account_name or str(account_id)
            raise ValueError(f"账号代号 '{target_name}' 已存在，禁止重名") from e
        return AccountRepository.get_by_id(account_id)

    @staticmethod
    def delete(account_id: int):
        with db_immediate_tx() as cursor:
            # 级联置空: 显式将绑定该账号的任务 account_id 置 NULL，杜绝悬挂外键
            cursor.execute("UPDATE task_queue SET account_id = NULL WHERE account_id = ?", (account_id,))
            cursor.execute("DELETE FROM tg_accounts WHERE id = ?", (account_id,))
            cursor.execute("DELETE FROM system_settings WHERE key = ?", (f"{_HANDOVER_KEY_PREFIX}{account_id}",))

    get_all_async = _AsyncFace(get_all)
    get_by_id_async = _AsyncFace(get_by_id)
    get_default_async = _AsyncFace(get_default)
    create_async = _AsyncFace(create)
    update_status_async = _AsyncFace(update_status)
    update_device_fingerprint_async = _AsyncFace(update_device_fingerprint)
    mark_flood_wait_async = _AsyncFace(mark_flood_wait)
    update_premium_async = _AsyncFace(update_premium)
    update_async = _AsyncFace(update)
    delete_async = _AsyncFace(delete)


class BotRepository:
    """动态 Bot 令牌池数据库仓储"""

    @staticmethod
    def get_all(only_active: bool = False) -> list[dict[str, Any]]:
        with db_cursor() as cursor:
            if only_active:
                cursor.execute("SELECT * FROM tg_bots WHERE is_active = 1 ORDER BY id ASC")
            else:
                cursor.execute("SELECT * FROM tg_bots ORDER BY id ASC")
            return [dict(r) for r in cursor.fetchall()]

    @staticmethod
    def get_by_id(bot_id: int) -> dict[str, Any] | None:
        with db_cursor() as cursor:
            cursor.execute("SELECT * FROM tg_bots WHERE id = ?", (bot_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    @staticmethod
    def create(
        bot_token: str, bot_name: str, session_name: str, api_id: int, api_hash: str, is_active: int = 1
    ) -> dict[str, Any]:
        now = time.time()
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                """
                INSERT INTO tg_bots
                (bot_token, bot_name, session_name, api_id, api_hash, is_active, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 'unauthenticated', ?, ?)
                """,
                (bot_token, bot_name, session_name, api_id, api_hash, is_active, now, now),
            )
            bot_id = cursor.lastrowid
        assert bot_id is not None
        created = BotRepository.get_by_id(bot_id)
        # 插入成功后按主键回读必然命中; None 分支仅为类型收窄, 不可达
        assert created is not None
        return created

    @staticmethod
    def update_status(bot_id: int, status: str, bot_username: str | None = None, dc_id: int | None = None):
        now = time.time()
        updates = ["status = ?", "updated_at = ?"]
        params: list[Any] = [status, now]
        if bot_username is not None:
            updates.append("bot_username = ?")
            params.append(bot_username)
        if dc_id is not None:
            updates.append("dc_id = ?")
            params.append(dc_id)
        params.append(bot_id)
        with db_cursor(commit=True) as cursor:
            cursor.execute(f"UPDATE tg_bots SET {', '.join(updates)} WHERE id = ?", tuple(params))

    @staticmethod
    def mark_flood_wait(bot_id: int, wait_seconds: int):
        now = time.time()
        flood_until = now + wait_seconds
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                "UPDATE tg_bots SET status = 'flood_wait', flood_wait_until = ?, updated_at = ? WHERE id = ?",
                (flood_until, now, bot_id),
            )

    @staticmethod
    def update(bot_id: int, bot_name: str | None = None, is_active: bool | None = None) -> dict[str, Any] | None:
        now = time.time()
        updates = ["updated_at = ?"]
        params: list[Any] = [now]
        if bot_name is not None:
            updates.append("bot_name = ?")
            params.append(bot_name)
        if is_active is not None:
            updates.append("is_active = ?")
            params.append(1 if is_active else 0)
        params.append(bot_id)
        with db_cursor(commit=True) as cursor:
            cursor.execute(f"UPDATE tg_bots SET {', '.join(updates)} WHERE id = ?", tuple(params))
        return BotRepository.get_by_id(bot_id)

    @staticmethod
    def delete(bot_id: int):
        with db_cursor(commit=True) as cursor:
            cursor.execute("DELETE FROM tg_bots WHERE id = ?", (bot_id,))

    get_all_async = _AsyncFace(get_all)
    get_by_id_async = _AsyncFace(get_by_id)
    create_async = _AsyncFace(create)
    update_status_async = _AsyncFace(update_status)
    mark_flood_wait_async = _AsyncFace(mark_flood_wait)
    update_async = _AsyncFace(update)
    delete_async = _AsyncFace(delete)


class SystemSettingsRepository:
    """系统动态配置表仓储"""

    @staticmethod
    def get(key: str, default: str | None = None) -> str | None:
        with db_cursor() as cursor:
            cursor.execute("SELECT value FROM system_settings WHERE key = ?", (key,))
            row = cursor.fetchone()
            return row[0] if row else default

    @staticmethod
    def get_str(key: str, default: str = "") -> str:
        val = SystemSettingsRepository.get(key)
        return str(val) if val is not None else default

    @staticmethod
    def get_int(key: str, default: int = 0) -> int:
        val = SystemSettingsRepository.get(key)
        if val is None:
            return default
        try:
            return int(val)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def get_float(key: str, default: float = 0.0) -> float:
        val = SystemSettingsRepository.get(key)
        if val is None:
            return default
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def get_bool(key: str, default: bool = False) -> bool:
        val = SystemSettingsRepository.get(key)
        if val is None:
            return default
        return val.lower() in ("1", "true", "yes", "on")

    @staticmethod
    def set(key: str, value: Any, category: str = "general", description: str = ""):
        if key.startswith(_HANDOVER_KEY_PREFIX):
            raise ValueError("不允许修改内部会话交接记录")
        now = time.time()
        with db_cursor(commit=True) as cursor:
            cursor.execute(
                """
                INSERT OR REPLACE INTO system_settings (key, value, category, description, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (key, str(value), category, description, now),
            )

    @staticmethod
    def set_batch(settings_dict: dict[str, Any]):
        """批量更新配置项，保持既有 category 与 description (BEGIN IMMEDIATE 独占事务)"""
        if any(k.startswith(_HANDOVER_KEY_PREFIX) for k in settings_dict):
            raise ValueError("不允许修改内部会话交接记录")
        now = time.time()
        with db_immediate_tx() as cursor:
            for k, v in settings_dict.items():
                cursor.execute("SELECT category, description FROM system_settings WHERE key = ?", (k,))
                row = cursor.fetchone()
                category = row["category"] if row else "general"
                description = row["description"] if row else ""
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO system_settings (key, value, category, description, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (k, str(v), category, description, now),
                )

    @staticmethod
    def get_all() -> dict[str, str]:
        with db_cursor() as cursor:
            cursor.execute("SELECT key, value FROM system_settings")
            return {r["key"]: r["value"] for r in cursor.fetchall() if not r["key"].startswith(_HANDOVER_KEY_PREFIX)}

    @staticmethod
    def get_all_grouped() -> dict[str, Any]:
        """获取所有动态配置项，按 category 分组返回"""
        with db_cursor() as cursor:
            cursor.execute(
                "SELECT key, value, category, description, updated_at FROM system_settings ORDER BY category ASC, key ASC"
            )
            rows = cursor.fetchall()
        categories: dict[str, list] = {}
        all_settings: dict[str, str] = {}
        for r in rows:
            if r["key"].startswith(_HANDOVER_KEY_PREFIX):
                continue
            item = {
                "key": r["key"],
                "value": r["value"],
                "category": r["category"],
                "description": r["description"] or "",
                "updated_at": r["updated_at"],
            }
            cat = r["category"] or "general"
            categories.setdefault(cat, []).append(item)
            all_settings[r["key"]] = r["value"]
        return {"categories": categories, "all_settings": all_settings}

    get_async = _AsyncFace(get)
    get_str_async = _AsyncFace(get_str)
    get_int_async = _AsyncFace(get_int)
    get_float_async = _AsyncFace(get_float)
    get_bool_async = _AsyncFace(get_bool)
    set_async = _AsyncFace(set)
    set_batch_async = _AsyncFace(set_batch)
    get_all_async = _AsyncFace(get_all)
    get_all_grouped_async = _AsyncFace(get_all_grouped)
