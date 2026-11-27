"""Database Layer: PRAGMA foreign_keys 级联约束、账号/Bot 数据持久化与外键完整性测试 (Foreign Keys & Account Repositories)

覆盖范围:
- 原 test_foreign_keys_closure: 任务队列 account_id 外键约束、级联删除与孤儿阻止
- 原 test_account_bot_repo: AccountRepository 与 BotRepository CRUD、唯一性约束与状态流转"""

import time

from app.db.account_repository import AccountRepository, BotRepository, SystemSettingsRepository
from app.db.database import _bootstrap_from_env, get_db
from app.db.task_repository import TaskQueueRepository


# ==============================================================================
# Part: test_foreign_keys_closure.py
# ==============================================================================
def test_sqlite_runtime_foreign_keys_enabled():
    """验证运行时 get_db 签出的连接已显式开启 PRAGMA foreign_keys = ON"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys")
    row = cursor.fetchone()
    assert row[0] == 1, "PRAGMA foreign_keys 必须为 1 (开启)"


def test_task_deletion_cascades_and_nullifies_related_tables():
    """验证任务删除时，强关联表级联删除、弱关联表置空，杜绝孤儿悬挂数据"""
    conn = get_db()
    cursor = conn.cursor()

    task_id = "test_fk_cascade_task_01"
    TaskQueueRepository.create(task_id, "download_link", {"links": []})

    # 1. 插入关联数据
    cursor.execute(
        "INSERT INTO task_logs (task_id, seq, time, message, created_at) VALUES (?, 1, '12:00', 'log', 0)", (task_id,)
    )
    cursor.execute(
        "INSERT INTO download_breakpoints (task_id, chat_id, msg_id, part_path, meta_path, final_path, created_at) VALUES (?, 'c1', 1, 'p', 'm', 'f', 0)",
        (task_id,),
    )
    cursor.execute(
        "INSERT INTO forward_history (task_id, source_chat, source_msg_id, target_chat, target_msg_id, forwarded_at) VALUES (?, 's1', 1, 't1', 10, 0)",
        (task_id,),
    )
    cursor.execute(
        "INSERT INTO forward_skips (task_id, source_chat, msg_id, skip_type, reason, target_chat, created_at) VALUES (?, 's1', 2, 'filter', 'r', 't1', 0)",
        (task_id,),
    )
    cursor.execute(
        "INSERT INTO storage_registry (path, kind, size, mtime, task_id, scanned_at) VALUES ('/tmp/f1', 'media', 100, 0, ?, 0)",
        (task_id,),
    )
    conn.commit()

    # 2. 删除任务
    deleted = TaskQueueRepository.delete(task_id)
    assert deleted is True

    # 3. 校验强关联表已级联物理删除
    cursor.execute("SELECT COUNT(*) FROM task_logs WHERE task_id = ?", (task_id,))
    assert cursor.fetchone()[0] == 0, "task_logs 应已被级联删除"

    cursor.execute("SELECT COUNT(*) FROM download_breakpoints WHERE task_id = ?", (task_id,))
    assert cursor.fetchone()[0] == 0, "download_breakpoints 应已被级联删除"

    # 4. 校验弱关联表已置空 task_id，业务账本完好保留
    cursor.execute("SELECT task_id FROM forward_history WHERE source_chat = 's1' AND source_msg_id = 1")
    assert cursor.fetchone()[0] is None, "forward_history.task_id 应已置 NULL"

    cursor.execute("SELECT task_id FROM forward_skips WHERE source_chat = 's1' AND msg_id = 2")
    assert cursor.fetchone()[0] is None, "forward_skips.task_id 应已置 NULL"

    cursor.execute("SELECT task_id FROM storage_registry WHERE path = '/tmp/f1'")
    assert cursor.fetchone()[0] is None, "storage_registry.task_id 应已置 NULL"


def test_account_deletion_nullifies_task_queue_account_id():
    """验证账号删除时，绑定该账号的任务 account_id 自动置空，杜绝悬挂外键"""
    acc = AccountRepository.create(
        account_name="FK Deletion Test Acc",
        phone="+12025550116",
        session_file="fk_acc_test.session",
        api_id=12345,
        api_hash="hash_test",
    )
    acc_id = acc["id"]

    task_id = "test_acc_fk_task_01"
    TaskQueueRepository.create(task_id, "download_link", {"links": []}, account_id=acc_id)

    # 校验初始绑定状态
    t_init = TaskQueueRepository.get_by_id(task_id)
    assert t_init["account_id"] == acc_id

    # 删除账号
    AccountRepository.delete(acc_id)

    # 校验任务依然存在，但 account_id 已置空
    t_after = TaskQueueRepository.get_by_id(task_id)
    assert t_after is not None
    assert t_after["account_id"] is None, "账号删除后，任务 account_id 必须已置空"

    TaskQueueRepository.delete(task_id)


# ==============================================================================
# Part: test_account_bot_repo.py
# ==============================================================================
def test_account_repository_crud():
    # 1. 创建 Userbot 账号
    acc = AccountRepository.create(
        account_name="Test Userbot",
        phone="+12025550127",
        session_file="test_userbot.session",
        api_id=123456,
        api_hash="abcdef0123456789abcdef",
        is_active=1,
        is_default=1,
        max_concurrency=3,
    )
    assert acc is not None
    assert acc["account_name"] == "Test Userbot"
    assert acc["is_default"] == 1
    assert acc["max_concurrency"] == 3

    # 2. 查询单个与全部
    fetched = AccountRepository.get_by_id(acc["id"])
    assert fetched["phone"] == "+12025550127"

    default_acc = AccountRepository.get_default()
    assert default_acc["id"] == acc["id"]

    all_accs = AccountRepository.get_all(only_active=True)
    assert len(all_accs) >= 1

    # 3. 状态更新与 FloodWait
    AccountRepository.update_status(
        acc["id"], "authenticated", user_id=987654321, username="test_user", first_name="Tester", dc_id=2
    )
    updated = AccountRepository.get_by_id(acc["id"])
    assert updated["status"] == "authenticated"
    assert updated["user_id"] == 987654321
    assert updated["username"] == "test_user"
    assert updated["dc_id"] == 2

    AccountRepository.mark_flood_wait(acc["id"], 60)
    fw = AccountRepository.get_by_id(acc["id"])
    assert fw["status"] == "flood_wait"
    assert fw["flood_wait_until"] > time.time()

    AccountRepository.update_premium(acc["id"], True)
    prem = AccountRepository.get_by_id(acc["id"])
    assert prem["is_premium"] == 1

    # 4. 删除
    AccountRepository.delete(acc["id"])
    assert AccountRepository.get_by_id(acc["id"]) is None


def test_bot_repository_crud():
    # 1. 创建 Bot
    bot = BotRepository.create(
        bot_token="123456789:ABCdefGhIJKlmNoPQRstuVWXyz",
        bot_name="Test Pool Bot 1",
        session_name="test_bot_1",
        api_id=123456,
        api_hash="abcdef0123456789abcdef",
        is_active=1,
    )
    assert bot is not None
    assert bot["bot_name"] == "Test Pool Bot 1"
    assert bot["status"] == "unauthenticated"

    # 2. 更新状态与 FloodWait
    BotRepository.update_status(bot["id"], "ready", bot_username="my_test_bot", dc_id=4)
    updated = BotRepository.get_by_id(bot["id"])
    assert updated["status"] == "ready"
    assert updated["bot_username"] == "my_test_bot"
    assert updated["dc_id"] == 4

    BotRepository.mark_flood_wait(bot["id"], 45)
    fw = BotRepository.get_by_id(bot["id"])
    assert fw["status"] == "flood_wait"
    assert fw["flood_wait_until"] > time.time()

    # 3. 列表与删除
    active_bots = BotRepository.get_all(only_active=True)
    assert any(b["id"] == bot["id"] for b in active_bots)

    BotRepository.delete(bot["id"])
    assert BotRepository.get_by_id(bot["id"]) is None


def test_system_settings_repository():
    # 默认值
    assert SystemSettingsRepository.get("non_existent_key", "default_val") == "default_val"
    assert SystemSettingsRepository.get_int("non_existent_int", 42) == 42
    assert SystemSettingsRepository.get_float("non_existent_float", 3.14) == 3.14
    assert SystemSettingsRepository.get_bool("non_existent_bool", True) is True

    # 写入与读取
    SystemSettingsRepository.set("max_global_concurrency", 5, category="concurrency", description="全局并发数")
    assert SystemSettingsRepository.get_int("max_global_concurrency") == 5

    SystemSettingsRepository.set("enable_auto_retry", "true", category="scheduler")
    assert SystemSettingsRepository.get_bool("enable_auto_retry") is True

    all_settings = SystemSettingsRepository.get_all()
    assert "max_global_concurrency" in all_settings
    assert all_settings["max_global_concurrency"] == "5"


def test_bootstrap_from_env_idempotency():
    conn = get_db()
    cursor = conn.cursor()
    # 执行 bootstrap
    _bootstrap_from_env(cursor)
    conn.commit()

    # 再次执行 bootstrap (幂等性验证)
    _bootstrap_from_env(cursor)
    conn.commit()
    conn.close()

    accs = AccountRepository.get_all()
    assert len(accs) >= 1
    default_acc = accs[0]
    assert default_acc["is_default"] == 1

    bots = BotRepository.get_all()
    assert len(bots) >= 1

    settings_map = SystemSettingsRepository.get_all()
    assert "max_global_concurrency" in settings_map


def test_account_update_missing_id_does_not_clear_default():
    acc = AccountRepository.create(
        "acc_default_keep", "13800000001", "sess_default_keep.session", 12345, "hash_keep", is_default=True
    )
    assert acc["is_default"] == 1

    res = AccountRepository.update(999999, is_default=True)
    assert res is None

    persisted = AccountRepository.get_by_id(acc["id"])
    assert persisted is not None
    assert persisted["is_default"] == 1
    still_default = AccountRepository.get_default()
    assert still_default is not None
    assert still_default["id"] == acc["id"]
