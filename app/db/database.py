"""SQLite 数据库底层管理与基线 Schema 体系 (Baseline-Only Schema Management)

【Schema 治理规范 (baseline 唯一真源)】
1. 版本号管理：
   - `_create_baseline_tables()` 是唯一 schema 真源，不依赖历史迁移链；
   - `PRAGMA user_version` 单值语义: 必须等于 `LATEST_VERSION` (=1), 否则 init_db 直接
     CRITICAL 拒启 (旧版本库 / 未知库 / 版本超前库一律快速失败, 提示清库重建或恢复备份);
   - 开发期修改 schema 的方式: 直接编辑 `_create_baseline_tables()` 并清库重建 (先备份
     `data/tg_service.db` 到 data/ 外);
   - 后续版本若需演进 schema，从当前 baseline 冻结出 `_create_baseline_tables()` 快照并追加幂等迁移。
2. 基线编写约束：
   - 全部 CREATE TABLE / CREATE INDEX 使用 IF NOT EXISTS 幂等写法, 仅在全新空库上执行;
   - 每次修改 baseline 必须同步更新 tests/db/test_db_migrations.py 的结构断言。
3. 种子与数据运维边界：
   - `_bootstrap_from_env` (随环境变量动态导入的 settings/账号/Bot 种子) 属应用层运维,
     不纳入版本管理, 每次启动幂等运行。
"""

import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager

from app.config import secure_file_permissions, settings

logger = logging.getLogger(__name__)


def _secure_db_files(path: str) -> None:
    """对 SQLite 主库及配套 -wal / -shm 文件应用非公开权限 (POSIX 0600)。"""
    if not path or path == ":memory:" or path.startswith("file:"):
        return
    for suffix in ("", "-wal", "-shm"):
        secure_file_permissions(f"{path}{suffix}")


class _CachedSQLiteConnection(sqlite3.Connection):
    """线程本地缓存连接: close() 归还缓存而非真关闭。

    Windows/生产实测每次 connect+close 约 4ms, 高频 append_log/进度写入被连接建立开销支配;
    复用连接后单条写入降至亚毫秒。事务语义不变 (异常路径由调用方 rollback)。
    """

    _closed: bool = False

    def close(self) -> None:
        return None

    def actual_close(self) -> None:
        self._closed = True
        with _connections_lock:
            _active_connections.discard(self)
        try:
            super().close()
        except Exception as close_err:
            logger.debug("关闭底层 SQLite 连接异常 (非致命): %s", close_err)


_active_connections: set[_CachedSQLiteConnection] = set()
_connections_lock = threading.Lock()
_conn_cache = threading.local()


def _reset_cached_connection(entry) -> None:
    if entry is not None:
        try:
            entry[1].actual_close()
        except Exception as reset_err:
            logger.debug("重置缓存 SQLite 连接异常 (非致命): %s", reset_err)


def close_thread_cached_connection() -> None:
    """关闭当前线程的缓存连接 (测试切换 DB_PATH 前释放文件句柄, 便于临时目录清理)"""
    entry = getattr(_conn_cache, "entry", None)
    _reset_cached_connection(entry)
    _conn_cache.entry = None


def close_all_thread_connections() -> None:
    """强力关闭所有线程中已打开的缓存 SQLite 连接 (多线程与写线程池文件句柄彻底清理)"""
    close_thread_cached_connection()
    with _connections_lock:
        conns = list(_active_connections)
        _active_connections.clear()
    for conn in conns:
        try:
            conn.actual_close()
        except Exception as close_err:
            logger.debug("强力清理活跃 SQLite 连接异常 (非致命): %s", close_err)


def get_db():
    path = settings.DB_PATH
    entry = getattr(_conn_cache, "entry", None)
    if entry is not None and entry[0] == path:
        conn = entry[1]
        if not getattr(conn, "_closed", False):
            try:
                # 防御性自愈 1：若当前线程复用的连接残留了未决的脏事务，自动 rollback 复位
                if getattr(conn, "in_transaction", False):
                    conn.rollback()
                # 防御性自愈 2：若当前连接隔离级别被非预期修改（如残留 None/autocommit），强制复位为默认 DEFERRED ("")
                if conn.isolation_level != "":
                    conn.isolation_level = ""
                return conn
            except sqlite3.ProgrammingError:
                _conn_cache.entry = None
            except Exception as reuse_err:
                logger.warning("复用 SQLite 连接自愈失败，重建连接: %s", reuse_err)
                _reset_cached_connection(entry)
                _conn_cache.entry = None
        else:
            _conn_cache.entry = None
    elif entry is not None:
        # DB_PATH 变更 (测试切换临时库): 先释放旧连接句柄
        _reset_cached_connection(entry)
        _conn_cache.entry = None
    conn = sqlite3.connect(path, check_same_thread=False, timeout=5.0, factory=_CachedSQLiteConnection)
    _secure_db_files(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    # WAL 配套 NORMAL: commit 不再逐次 fsync (幂等/原子性保持, 仅掉电时可能回滚最后若干事务)
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    with _connections_lock:
        _active_connections.add(conn)
    _conn_cache.entry = (path, conn)
    return conn


@contextmanager
def db_cursor(commit: bool = False):
    """仓储读写样板收敛: 取线程缓存连接 → yield cursor → 可选提交 → 归还缓存。

    close 为缓存归还 (非真关闭); 异常退出立即 rollback, 避免脏事务或写锁残留。
    commit=True 时在 yield 正常返回后提交 (rowcount/lastrowid 读取发生在提交前, 语义不变)。"""
    conn = get_db()
    cursor = conn.cursor()
    try:
        yield cursor
        if commit:
            conn.commit()
    except BaseException:
        if getattr(conn, "in_transaction", False):
            try:
                conn.rollback()
            except Exception as rb_err:
                logger.warning("db_cursor 异常退出回滚失败: %s", rb_err)
        raise
    finally:
        conn.close()


@contextmanager
def db_read_tx():
    """只读快照事务样板: 显式 BEGIN DEFERRED，确保多条 SELECT 共享同一数据库快照。"""
    conn = get_db()
    orig_isolation = conn.isolation_level
    conn.isolation_level = None
    cursor = conn.cursor()
    try:
        cursor.execute("BEGIN")
        yield cursor
    finally:
        try:
            if getattr(conn, "in_transaction", False):
                cursor.execute("ROLLBACK")
        except Exception as rb_err:
            logger.debug("只读事务回滚异常 (非致命): %s", rb_err)
        try:
            conn.isolation_level = orig_isolation
        except Exception as iso_err:
            logger.debug("复原 SQLite 隔离级别异常: %s", iso_err)
        conn.close()


@contextmanager
def db_immediate_tx():
    """BEGIN IMMEDIATE 独占写事务样板: 立即取写锁 → yield cursor → COMMIT, 异常 ROLLBACK。

    隔离级别统一切 autocommit (None) 并在退出时复原 —— 并发调用下 seq 分配 / 读改写
    (merge_params / reset_task_for_retry / update_concurrency) 与删除级联共用此单点口径。"""
    conn = get_db()
    orig_isolation = conn.isolation_level
    conn.isolation_level = None
    cursor = conn.cursor()
    try:
        cursor.execute("BEGIN IMMEDIATE")
        yield cursor
        cursor.execute("COMMIT")
    except BaseException:
        try:
            cursor.execute("ROLLBACK")
        except Exception as rb_err:
            logger.warning("IMMEDIATE 事务回滚失败: %s", rb_err)
        raise
    finally:
        try:
            conn.isolation_level = orig_isolation
        except Exception as iso_err:
            logger.debug("复原 SQLite 隔离级别异常: %s", iso_err)
        conn.close()


# Baseline-Only: 单值版本语义。两常量当前恒等，引入增量迁移链后分离
# (BASELINE_VERSION 冻结基线快照, LATEST_VERSION 随迁移递增)。
BASELINE_VERSION = 1
LATEST_VERSION = 1


def _create_baseline_tables(cursor: sqlite3.Cursor) -> None:
    """创建全新数据库的完整基线表结构 (唯一 schema 真源; 仅在全新空库上执行)"""
    # 1. 频道预设表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS channel_presets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        channel_id TEXT NOT NULL UNIQUE,
        created_at REAL NOT NULL,
        preset_type TEXT DEFAULT 'target',
        title TEXT DEFAULT ''
    )
    """)

    # 2. 下载防重历史表 (file_path 为发布防重点查键路径, 建索引防全表扫描)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS download_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id TEXT NOT NULL,
        msg_id INTEGER NOT NULL,
        grouped_id TEXT,
        file_name TEXT,
        file_path TEXT,
        file_size INTEGER,
        file_hash TEXT,
        task_id TEXT,
        task_completed INTEGER NOT NULL DEFAULT 0,
        downloaded_at REAL NOT NULL,
        UNIQUE(chat_id, msg_id)
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_download_history_path ON download_history(file_path)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_download_history_task ON download_history(task_id)")

    # 3. 任务队列表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS task_queue (
        id TEXT PRIMARY KEY,
        task_type TEXT NOT NULL,
        status TEXT NOT NULL,
        progress_bytes INTEGER DEFAULT 0,
        total_bytes INTEGER DEFAULT 0,
        processed_items INTEGER DEFAULT 0,
        total_items INTEGER DEFAULT 0,
        speed_mb_s REAL DEFAULT 0.0,
        eta_s REAL DEFAULT 0.0,
        current_item TEXT,
        params_json TEXT,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        concurrency INTEGER DEFAULT 0,
        dc_id INTEGER DEFAULT 0,
        concurrency_total INTEGER DEFAULT 0,
        account_id INTEGER REFERENCES tg_accounts(id) ON DELETE SET NULL,
        priority INTEGER DEFAULT 10,
        retry_count INTEGER DEFAULT 0,
        max_retries INTEGER DEFAULT 3,
        next_retry_at REAL DEFAULT 0.0,
        error_detail TEXT DEFAULT ''
    )
    """)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_task_queue_sched ON task_queue(status, priority DESC, created_at ASC)"
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_task_queue_account ON task_queue(account_id, status)")

    # 相册成员复用记录随任务断点保留，成功收尾后才写入历史完成证据。
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS download_history_reuse (
        task_id TEXT NOT NULL REFERENCES task_queue(id) ON DELETE CASCADE,
        history_id INTEGER NOT NULL REFERENCES download_history(id) ON DELETE CASCADE,
        PRIMARY KEY (task_id, history_id)
    )
    """)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_download_history_reuse_history ON download_history_reuse(history_id)"
    )

    # 4. 任务日志独立表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS task_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        time TEXT NOT NULL,
        level TEXT NOT NULL DEFAULT 'info',
        message TEXT NOT NULL,
        created_at REAL NOT NULL,
        UNIQUE(task_id, seq)
    )
    """)

    # 5. 发布历史防重表 (local/pikpak 分离部分唯一索引, 避免 PikPak 展示路径碰撞误删另一远端文件历史)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS publish_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        target_chat_id TEXT NOT NULL,
        file_path TEXT NOT NULL,
        file_size INTEGER NOT NULL,
        published_at REAL NOT NULL,
        file_hash TEXT DEFAULT '',
        source_type TEXT NOT NULL DEFAULT 'local',
        remote_file_id TEXT DEFAULT '',
        tg_message_id INTEGER DEFAULT 0,
        task_id TEXT DEFAULT ''
    )
    """)
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_publish_local_dedup "
        "ON publish_history(target_chat_id, file_path, file_size) WHERE source_type='local'"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_publish_hash ON publish_history(target_chat_id, file_path, file_hash)"
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_publish_task ON publish_history(task_id)")
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_publish_pikpak_dedup "
        "ON publish_history(target_chat_id, remote_file_id) WHERE source_type='pikpak'"
    )

    # 6. 下载断点归属表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS download_breakpoints (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id TEXT NOT NULL,
        chat_id TEXT NOT NULL,
        msg_id INTEGER NOT NULL,
        part_path TEXT NOT NULL,
        meta_path TEXT NOT NULL,
        final_path TEXT NOT NULL,
        created_at REAL NOT NULL,
        UNIQUE(task_id, chat_id, msg_id)
    )
    """)

    # 7. 存储登记表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS storage_registry (
        path TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        size INTEGER NOT NULL,
        mtime REAL NOT NULL,
        paired TEXT NOT NULL DEFAULT 'na',
        channel TEXT,
        task_id TEXT,
        status TEXT NOT NULL DEFAULT 'registered',
        scanned_at REAL NOT NULL
    )
    """)

    # 8. 极速转发消息映射历史表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS forward_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id TEXT,
        source_chat TEXT NOT NULL,
        source_msg_id INTEGER NOT NULL,
        target_chat TEXT NOT NULL,
        target_msg_id INTEGER NOT NULL,
        forwarded_at REAL NOT NULL,
        UNIQUE(source_chat, source_msg_id, target_chat)
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_forward_source ON forward_history(source_chat, source_msg_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_forward_task ON forward_history(task_id)")

    # 9. 转发处置账本表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS forward_skips (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id TEXT,
        source_chat TEXT NOT NULL,
        msg_id INTEGER NOT NULL,
        skip_type TEXT NOT NULL,
        reason TEXT,
        target_chat TEXT,
        created_at REAL NOT NULL
    )
    """)
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_forward_skips
    ON forward_skips(source_chat, msg_id, COALESCE(target_chat, ''), skip_type)
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_skips_source ON forward_skips(source_chat, msg_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_skips_task ON forward_skips(task_id)")

    # 10. 转发覆盖区间表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS forward_coverage (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_chat TEXT NOT NULL,
        target_chat TEXT NOT NULL,
        seg_start INTEGER NOT NULL,
        seg_end INTEGER NOT NULL,
        updated_at REAL NOT NULL
    )
    """)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_coverage_pair ON forward_coverage(source_chat, target_chat, seg_start)"
    )

    # 11. 多 Userbot 账号表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tg_accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_name TEXT NOT NULL UNIQUE,
        phone TEXT,
        session_file TEXT NOT NULL UNIQUE,
        api_id INTEGER NOT NULL,
        api_hash TEXT NOT NULL,
        is_active INTEGER NOT NULL DEFAULT 1,
        is_default INTEGER NOT NULL DEFAULT 0,
        is_premium INTEGER NOT NULL DEFAULT 0,
        premium_checked_at REAL DEFAULT 0.0,
        status TEXT NOT NULL DEFAULT 'unauthenticated',
        flood_wait_until REAL DEFAULT 0.0,
        dc_id INTEGER DEFAULT 0,
        user_id INTEGER,
        username TEXT,
        first_name TEXT,
        max_concurrency INTEGER NOT NULL DEFAULT 1,
        tier TEXT DEFAULT 'primary',
        device_model TEXT DEFAULT '',
        system_version TEXT DEFAULT '',
        app_version TEXT DEFAULT '',
        lang_code TEXT DEFAULT 'en',
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tg_accounts_active ON tg_accounts(is_active, status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tg_accounts_default ON tg_accounts(is_default)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tg_accounts_tier ON tg_accounts(tier, is_active)")

    # 12. 动态 Bot 令牌池表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tg_bots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_token TEXT NOT NULL UNIQUE,
        bot_name TEXT,
        bot_username TEXT,
        session_name TEXT NOT NULL UNIQUE,
        api_id INTEGER NOT NULL,
        api_hash TEXT NOT NULL,
        is_active INTEGER NOT NULL DEFAULT 1,
        status TEXT NOT NULL DEFAULT 'unauthenticated',
        flood_wait_until REAL DEFAULT 0.0,
        dc_id INTEGER DEFAULT 0,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tg_bots_active ON tg_bots(is_active, status)")

    # 13. 系统动态配置表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS system_settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        category TEXT NOT NULL DEFAULT 'general',
        description TEXT,
        updated_at REAL NOT NULL
    )
    """)

    # 14. 发布占位槽表
    #     失败占位卡生命周期持久化; 防重键对齐 publish_history UNIQUE 三元组;
    #     红线: 占位信息严禁写入 publish_history, 占位槽独立成表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS placeholder_slots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        target_chat_id TEXT NOT NULL,
        file_path TEXT NOT NULL,
        file_size INTEGER NOT NULL,
        slot_kind TEXT NOT NULL DEFAULT 'backfillable',
        state TEXT NOT NULL DEFAULT 'placeholder_sent',
        placeholder_msg_id INTEGER DEFAULT 0,
        real_msg_id INTEGER DEFAULT 0,
        fail_count INTEGER NOT NULL DEFAULT 0,
        fail_reason TEXT DEFAULT '',
        client_label TEXT DEFAULT '',
        sender_account_id INTEGER DEFAULT 0,
        task_id TEXT DEFAULT '',
        placeholder_sent_at REAL DEFAULT 0,
        edit_attempted_at REAL DEFAULT 0,
        resolved_at REAL DEFAULT 0,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        UNIQUE(target_chat_id, file_path, file_size)
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_placeholder_slots_task ON placeholder_slots(task_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_placeholder_slots_state ON placeholder_slots(state)")

    # 15. 频道别名 → 规范 Peer ID 映射缓存表
    #     将 @username / t.me/... 别名统一映射到数字 peer_id (-100...),
    #     保证写账本 / 断点快路径 / API 覆盖率查询共享同一规范主键
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS peer_aliases (
        alias_key TEXT PRIMARY KEY,
        peer_id TEXT NOT NULL,
        updated_at REAL NOT NULL
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_peer_aliases_peer ON peer_aliases(peer_id)")


def init_db(db_path: str | None = None):
    """初始化 SQLite 数据库 (baseline-only 语义) 并幂等执行环境种子。

    1. 全新库 (user_version == 0 且无用户表): 执行完整基线 DDL，置 user_version = LATEST_VERSION;
    2. 版本匹配 (user_version == LATEST_VERSION): 跳过 DDL，仅执行幂等种子;
    3. 其余一切状态 (旧版本库 / 存量无版本库 / 版本超前库): CRITICAL 拒启 ——
       baseline-only 世界无升级路径，快速失败并提示清库重建或恢复备份。
    """
    target_db_path = db_path or settings.DB_PATH
    if db_path:
        conn = sqlite3.connect(db_path, check_same_thread=False, timeout=5.0)
        _secure_db_files(db_path)
        conn.row_factory = sqlite3.Row
    else:
        conn = get_db()
    try:
        cursor = conn.cursor()
        # WAL 模式: 读写不再互相阻塞 (日志高频 INSERT 与轮询查询并行), 对数据库文件持久生效
        cursor.execute("PRAGMA journal_mode=WAL")
        _secure_db_files(target_db_path)

        cursor.execute("PRAGMA user_version")
        current_ver = cursor.fetchone()[0]

        cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        table_count = cursor.fetchone()[0]

        if current_ver == 0 and table_count == 0:
            # 全新库：显式事务原子包裹基线建表与版本号赋值，防止半初始化锁死库
            orig_isolation = conn.isolation_level
            conn.isolation_level = None
            try:
                cursor.execute("BEGIN IMMEDIATE")
                _create_baseline_tables(cursor)
                cursor.execute(f"PRAGMA user_version = {LATEST_VERSION}")
                cursor.execute("COMMIT")
                logger.info("全新数据库初始化完成 (baseline-only)，PRAGMA user_version = %d", LATEST_VERSION)
            except BaseException:
                try:
                    cursor.execute("ROLLBACK")
                except Exception as rb_err:
                    logger.warning("初始化基线表回滚失败: %s", rb_err)
                raise
            finally:
                conn.isolation_level = orig_isolation
        elif current_ver != LATEST_VERSION:
            logger.critical(
                "数据库 user_version=%d 与基线版本 LATEST_VERSION=%d 不一致 (旧版本库/未知库)，"
                "baseline-only 语义下无升级路径，服务拒启。请先备份后删除数据文件清库重建，或恢复匹配版本的备份",
                current_ver,
                LATEST_VERSION,
            )
            raise RuntimeError(
                f"数据库 schema 版本不兼容 (user_version={current_ver}, 期望 {LATEST_VERSION})。"
                "请清库重建 (先备份 data/ 下的数据库文件) 或恢复匹配版本的备份"
            )

        _bootstrap_from_env(cursor)
        conn.commit()
        _secure_db_files(target_db_path)
    except BaseException:
        if getattr(conn, "in_transaction", False):
            try:
                conn.rollback()
            except Exception as rb_err:
                logger.warning("init_db 异常回滚失败: %s", rb_err)
        raise
    finally:
        conn.close()


def _bootstrap_from_env(cursor):
    """系统初次启动时，自动将 .env 中的凭证与配置幂等导入 DB"""
    now = time.time()

    # 1. 初始化 system_settings 默认值
    default_settings = [
        ("max_global_concurrency", "3", "concurrency", "允许同时处于 running 状态的任务总数上限"),
        (
            "download_parallel_connections",
            str(getattr(settings, "DOWNLOAD_PARALLEL_CONNECTIONS", 8)),
            "concurrency",
            "新建下载任务默认分配的 TCP 连接通道数，推荐 8，范围 4~32，步长 4",
        ),
        (
            "upload_parallel_connections",
            str(getattr(settings, "UPLOAD_PARALLEL_CONNECTIONS", 2)),
            "concurrency",
            "新建上传任务默认分配的并发通道数，推荐 2，范围 1~16",
        ),
        (
            "upload_in_flight_per_connection",
            str(getattr(settings, "UPLOAD_IN_FLIGHT_PER_CONNECTION", 2)),
            "concurrency",
            "每个上传连接多路复用的在途并发分块数，推荐 2，范围 1~4",
        ),
        (
            "default_target_channel",
            str(getattr(settings, "DEFAULT_TARGET_CHANNEL", "-1009000000010")),
            "telegram",
            "新建发布/转存任务时缺省绑定的 Telegram 私密频道 ID",
        ),
        # 种子值跟随环境变量 USERBOT_UPLOAD_THRESHOLD (MB 语义): 首次建库时环境变量即生效,
        # 否则该行恒存在会使 publisher_service 中 settings 兜底分支在真实部署里不可达
        (
            "userbot_upload_threshold_mb",
            str(settings.USERBOT_UPLOAD_THRESHOLD_MB),
            "telegram",
            "超过该容量切换为 Userbot 发送 (MB)",
        ),
        ("max_storage_quota_gb", "0", "storage", "下载存储配额上限 (GB, 0 表示不设上限按物理磁盘)"),
        ("last_storage_scan_at", "0", "storage", "最近一次存储扫描时间戳"),
        (
            "enable_transit_acceleration",
            str(getattr(settings, "ENABLE_TRANSIT_ACCELERATION", True)).lower(),
            "transit",
            "开启后符合条件的大文件自动触发集群多路并行拉取；异常或小文件自动降级直连",
        ),
        (
            "transit_chat_id",
            str(getattr(settings, "TRANSIT_CHAT_ID", "")),
            "transit",
            "用于多 Bot 临时分块传输的 Telegram 私有群 ID，格式形如 -100xxxxxxxxxx。要求：主账号具备发信与删信权限，各辅助 Bot 已入群",
        ),
        (
            "transit_min_file_size_mb",
            str(getattr(settings, "TRANSIT_MIN_FILE_SIZE_MB", 20)),
            "transit",
            "仅对超过该大小的文件启用集群加速，默认 20 MB；小文件直接下载更高效，避免转发开销",
        ),
        (
            "pikpak_upload_concurrency",
            str(getattr(settings, "PIKPAK_UPLOAD_CONCURRENCY", 2)),
            "pikpak",
            "PikPak 流式发布上传并发通道数，推荐 2，范围 1~16",
        ),
        (
            "pikpak_upload_in_flight",
            str(getattr(settings, "PIKPAK_UPLOAD_IN_FLIGHT", 1)),
            "pikpak",
            "PikPak 每个通道多路复用在途分块数，推荐 1，范围 1~4",
        ),
        (
            "pikpak_ring_buffer_mb",
            str(getattr(settings, "PIKPAK_RING_BUFFER_MB", 50)),
            "pikpak",
            "PikPak 内存环形容灾缓冲区大小 (MB)，推荐 50，范围 10~1024",
        ),
        (
            "pikpak_prefetch_concurrency",
            str(getattr(settings, "PIKPAK_PREFETCH_CONCURRENCY", 3)),
            "pikpak",
            "PikPak 流式发布预拉取并发通道数，推荐 3，范围 1~16",
        ),
        (
            "pikpak_proxy_url",
            str(getattr(settings, "PIKPAK_PROXY_URL", "") or ""),
            "pikpak",
            "PikPak 控制面专享代理地址 (例如 http://127.0.0.1:7890 或 socks5://127.0.0.1:1080，留空直连)",
        ),
        (
            "pikpak_default_cleanup_mode",
            "archive",
            "pikpak",
            "PikPak 任务发布后默认清理模式 (archive: 归档到根目录 [已发布归档], none: 保留原状, delete: 移入回收站)",
        ),
        (
            "pikpak_default_oversize_policy",
            "skip_and_report",
            "pikpak",
            "PikPak 超限文件默认处理策略 (skip_and_report: 跳过并占位报告, split: 虚拟切片分卷)",
        ),
        (
            "pikpak_default_split_strategy",
            "vsse_with_fallback",
            "pikpak",
            "PikPak 超限流式切片引擎策略 (vsse_with_fallback: VSSE 极速重构首选, fmp4_universal: fMP4 全能保底, raw: 原始二进制硬切)",
        ),
        (
            "auto_remux_mpegts_to_mp4",
            "true" if getattr(settings, "AUTO_REMUX_MPEGTS_TO_MP4", True) else "false",
            "telegram",
            "发布 .ts/.m2ts/.mts 视频前自动零落盘重封装为 MP4，避免 Telegram 剥离视频属性导致无法流式播放",
        ),
    ]
    for k, v, cat, desc in default_settings:
        cursor.execute(
            "INSERT OR IGNORE INTO system_settings (key, value, category, description, updated_at) VALUES (?, ?, ?, ?, ?)",
            (k, v, cat, desc, now),
        )
        # 热更新存量库中的产品化描述与分类 (保持已有的用户自定义 value 不变)
        cursor.execute("UPDATE system_settings SET description = ?, category = ? WHERE key = ?", (desc, cat, k))

    # 2. 导入 Userbot 主账号 (tg_accounts)
    try:
        api_id = getattr(settings, "API_ID", None)
        api_hash = getattr(settings, "API_HASH", None)
        session_name = getattr(settings, "USERBOT_SESSION_NAME", "userbot_session")
        phone = getattr(settings, "DEFAULT_PHONE", "")
        if api_id and api_hash:
            session_file = f"{session_name}.session" if not session_name.endswith(".session") else session_name
            cursor.execute("SELECT COUNT(*) FROM tg_accounts")
            count = cursor.fetchone()[0]
            if count == 0:
                cursor.execute(
                    """
                    INSERT INTO tg_accounts
                    (account_name, phone, session_file, api_id, api_hash, is_active, is_default, status, max_concurrency, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, 1, 1, 'unauthenticated', 1, ?, ?)
                    """,
                    ("Default Userbot", phone, session_file, int(api_id), str(api_hash), now, now),
                )
    except Exception as e:
        logging.warning("从 .env 自动导入 Userbot 账号失败: %s", e)

    # 3. 导入 Bot 池 (tg_bots)
    try:
        bot_tokens = getattr(settings, "BOT_TOKENS", [])
        # 联合判空: 防止 TG_BOT_TOKENS 有值但 API_ID/API_HASH 缺失时 int(None) TypeError (缺失凭证由启动期校验拦截)
        if bot_tokens and getattr(settings, "API_ID", None) and getattr(settings, "API_HASH", None):
            cursor.execute("SELECT COUNT(*) FROM tg_bots")
            bot_count = cursor.fetchone()[0]
            if bot_count == 0:
                for idx, token in enumerate(bot_tokens, start=1):
                    session_name = f"bot_pool_{idx}"
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO tg_bots
                        (bot_token, bot_name, session_name, api_id, api_hash, is_active, status, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, 1, 'unauthenticated', ?, ?)
                        """,
                        (
                            token,
                            f"Bot #{idx}",
                            session_name,
                            int(settings.API_ID or 0),
                            str(settings.API_HASH),
                            now,
                            now,
                        ),
                    )
    except Exception as e:
        logging.warning("从 .env 自动导入 Bot 池失败: %s", e)


# WAL 检查点自保与体积告警常量 (唯一来源 settings; 模块级再导出供 main.py 导入与测试 monkeypatch)
WAL_CHECKPOINT_INTERVAL_SECONDS: float = settings.WAL_CHECKPOINT_INTERVAL_SECONDS
WAL_SIZE_WARNING_THRESHOLD_BYTES: int = settings.WAL_SIZE_WARNING_THRESHOLD_BYTES


def run_wal_checkpoint(db_path: str | None = None) -> tuple[int, int, int]:
    """执行 SQLite PASSIVE 模式 WAL 检查点自保收割与体积告警。

    参数:
        db_path: 数据库文件路径，为 None 时取 settings.DB_PATH。

    返回:
        (busy, log, checkpointed) 三元组:
        - busy: 0 表示检查点成功执行，1 表示因存在活跃读写事务而被跳过；
        - log: WAL 日志中的总帧数；
        - checkpointed: 成功移入主数据库文件的帧数。
    """
    path = db_path or settings.DB_PATH
    if not os.path.exists(path):
        return (0, 0, 0)

    busy, log, checkpointed = 0, 0, 0
    try:
        conn = sqlite3.connect(path, timeout=5.0)
        try:
            cursor = conn.cursor()
            cursor.execute("PRAGMA wal_checkpoint(PASSIVE)")
            row = cursor.fetchone()
            if row:
                busy, log, checkpointed = int(row[0]), int(row[1]), int(row[2])
        finally:
            conn.close()

        if busy == 1:
            logger.debug(
                "SQLite WAL 检查点 (PASSIVE) 遇到活跃读写事务被跳过，将在下周期重试 (busy=1, log=%d, checkpointed=%d)",
                log,
                checkpointed,
            )
        else:
            logger.debug("SQLite WAL 检查点 (PASSIVE) 执行完成 (busy=0, log=%d, checkpointed=%d)", log, checkpointed)

        # 检查 WAL 文件体积是否超过告警阈值 (默认 50MB)
        wal_path = f"{path}-wal"
        if os.path.exists(wal_path):
            wal_size = os.path.getsize(wal_path)
            if wal_size > WAL_SIZE_WARNING_THRESHOLD_BYTES:
                logger.warning(
                    "⚠️ SQLite WAL 文件体积已达 %.2f MB，超过预警阈值 (%.2f MB)，请关注长事务或高频写入 (path=%s)",
                    wal_size / (1024 * 1024),
                    WAL_SIZE_WARNING_THRESHOLD_BYTES / (1024 * 1024),
                    wal_path,
                )
    except Exception as e:
        logger.warning("执行 SQLite WAL 检查点异常 (path=%s): %s", path, e)

    return (busy, log, checkpointed)
