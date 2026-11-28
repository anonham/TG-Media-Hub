"""Core Runtime: Userbot/Bot 客户端池生命周期、动态轮询、Premium 特权感知与账号分级治理测试 (Client Manager & Bot Pool)

覆盖范围:
- 原 test_client_manager: TelegramAccountClient 状态机、多账号池与 Bot 动态注册
- 原 test_issue1 (probing 部分): 多账号频道可访问性探测 (probe_chat_access)
- 原 test_premium: Premium 会员探测、动态大小上限 (2000 MiB (1.95 GiB) / 4000 MiB (3.91 GiB))、错误分类与协议保护
- 原 test_p2_features: 账号分级优先级调度 (primary vs secondary) 与设备指纹持久化

pkg 1.4: ClientManager 兼容 facade (userbot/bots/auth_state/is_premium 的 setter 三连
+ _fallback_* 影子状态) 已删除, 属性为派生只读视图 —— 测试注入统一走 tests/_cm_seed.py
种子真实账号对象。"""

import asyncio
import os
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from _cm_seed import seed_default_account, unseed_accounts
from fastapi.testclient import TestClient
from telethon import TelegramClient

import app.services.publisher_service as publisher_module
from app.api.auth import issue_2fa_token
from app.config import settings
from app.core.client_manager import (
    ClientManager,
    TelegramAccountClient,
    client_manager,
    generate_device_fingerprint,
)
from app.core.failure_logger import FailureLogger
from app.core.pipeline.utils import classify_oversize, describe_upload_failure
from app.core.session_errors import report_session_invalid
from app.db.account_repository import AccountRepository, BotRepository
from app.main import app
from app.models.publish import InspectFolderResponse
from app.services.publisher_service import PublisherService


# ==============================================================================
# Part: test_client_manager.py
# ==============================================================================
@pytest.mark.asyncio
async def test_telegram_account_client_lifecycle():
    # 1. 模拟 TelegramClient
    mock_client = AsyncMock(spec=TelegramClient)
    mock_client.is_connected = MagicMock(return_value=True)
    mock_client.is_user_authorized = AsyncMock(return_value=True)
    mock_me = MagicMock()
    mock_me.id = 112233
    mock_me.username = "mock_user"
    mock_me.first_name = "Mock"
    mock_me.premium = True
    mock_client.get_me.return_value = mock_me

    acc = TelegramAccountClient(
        account_id=1,
        account_name="Mock Account",
        phone="+12025550112",
        session_file="mock.session",
        api_id=123,
        api_hash="hash123",
        max_concurrency=2,
        is_active=True,
        is_default=True,
    )
    acc.client = mock_client
    acc.auth_state = "AUTHENTICATED"

    assert acc.is_ready() is True
    assert acc.is_flood_locked() is False

    # 2. Premium 探测与缓存 (粘性语义在账号层)
    is_prem = await acc.probe_premium(force=True)
    assert is_prem is True
    assert acc.effective_max_file_size_bytes() == 4000 * 1024 * 1024

    # 3. FloodWait 标记与熔断
    acc.mark_flood_wait(30)
    assert acc.is_flood_locked() is True
    assert acc.is_ready() is False
    assert acc.auth_state == "FLOOD_WAIT"


@pytest.mark.asyncio
async def test_client_manager_dynamic_accounts_and_bots():
    manager = ClientManager()

    # 1. 创建 Mock 账号
    acc_record = AccountRepository.create(
        account_name="Dynamic Acc 1",
        phone="+12025550113",
        session_file="dyn1.session",
        api_id=123456,
        api_hash="hash_dyn",
        is_active=1,
        is_default=1,
        max_concurrency=2,
    )
    # 创建 Mock Bot
    BotRepository.create(
        bot_token="111:AAABBB",
        bot_name="Dynamic Bot 1",
        session_name="dyn_bot_1",
        api_id=123456,
        api_hash="hash_dyn",
        is_active=1,
    )

    from pathlib import Path

    (Path(settings.DATA_DIR) / "dyn1.session").touch()
    with patch("app.core.client_manager.TelegramClient") as MockTGClient:
        instance = AsyncMock()
        instance.is_connected = MagicMock(return_value=True)
        instance.session = MagicMock(dc_id=4)
        instance.is_user_authorized = AsyncMock(return_value=True)
        instance.get_me = AsyncMock(
            return_value=MagicMock(
                id=9001, phone="12025550113", username="dyn_user_or_bot", first_name="Dyn", premium=False
            )
        )
        MockTGClient.return_value = instance

        await manager.initialize()

        assert len(manager.get_all_accounts()) >= 1
        assert len(manager.get_all_bots()) >= 1

        default_acc = manager.get_account()
        assert default_acc is not None
        assert default_acc.account_name == "Dynamic Acc 1"

        client = await manager.get_client_for_task(mode="userbot")
        assert client is not None

        # 测试 FloodWait 隔离
        manager.mark_flood_wait(acc_record["id"], 60)
        assert manager.get_account(acc_record["id"]).is_flood_locked() is True

        await manager.disconnect_all()


# ==============================================================================
# Part: .tmp_issue1_probing.py
# ==============================================================================
@pytest.mark.asyncio
async def test_client_manager_probe_chat_access_multi_account():
    # Setup mock accounts
    acc1 = MagicMock(spec=TelegramAccountClient)
    acc1.account_id = 1
    acc1.account_name = "Account 1"
    acc1.is_active = True
    acc1.is_flood_locked.return_value = False
    acc1.client = MagicMock()
    acc1.client.is_connected.return_value = True

    acc2 = MagicMock(spec=TelegramAccountClient)
    acc2.account_id = 2
    acc2.account_name = "Account 2"
    acc2.is_active = True
    acc2.is_flood_locked.return_value = False
    acc2.client = MagicMock()
    acc2.client.is_connected.return_value = True

    with patch.object(client_manager, "_accounts", {1: acc1, 2: acc2}):
        # Mock resolve_entity: acc1 fails, acc2 succeeds
        async def mock_resolve(client, peer, fallback_peer=None):
            if client == acc1.client:
                raise Exception("Could not find input entity")
            mock_entity = MagicMock()
            mock_entity.id = 9000000023
            mock_entity.title = "Target Channel"
            return mock_entity

        with patch("app.core.account_selection.resolve_entity", side_effect=mock_resolve):
            probed_acc, entity = await client_manager.probe_chat_access(9000000023, account_id=None)
            assert probed_acc is not None
            assert probed_acc.account_id == 2
            assert entity.title == "Target Channel"


# ==============================================================================
# Part: test_premium.py
# ==============================================================================
client = TestClient(app, client=("127.0.0.1", 50000))

MB = 1024 * 1024


def auth_headers():
    headers = {}
    if settings.API_KEY:
        headers["X-API-Key"] = settings.API_KEY
        token = issue_2fa_token()
        if token:
            headers["X-2FA-Token"] = token
    return headers


def inspect_with_virtual_files(folder: str, virtual_files: dict) -> dict:
    """以虚拟文件清单 (文件名 → 字节数) 执行真实 inspect_folder，不落盘任何文件"""

    def fake_walk(root):
        return [(root, list(virtual_files))]

    def fake_getsize(path):
        return virtual_files[os.path.basename(path)]

    with (
        patch.object(publisher_module, "walk_publish_files", fake_walk),
        patch.object(publisher_module.os.path, "getsize", fake_getsize),
    ):
        return PublisherService.inspect_folder(folder)


# ---------- 有效上限计算 (注入走 _accounts 种子, pkg 1.4) ----------


def test_effective_limit_non_premium():
    seed_default_account(client_manager, client=None, is_premium=False)
    try:
        assert client_manager.effective_max_file_size_bytes() == settings.MAX_FILE_SIZE_BYTES
    finally:
        unseed_accounts(client_manager)


def test_effective_limit_premium():
    seed_default_account(client_manager, client=None, is_premium=True)
    try:
        assert client_manager.effective_max_file_size_bytes() == settings.PREMIUM_MAX_FILE_SIZE_BYTES
    finally:
        unseed_accounts(client_manager)


def test_effective_limit_clamped_by_protocol():
    seed_default_account(client_manager, client=None, is_premium=True)
    try:
        with patch.object(settings, "PREMIUM_MAX_FILE_SIZE_BYTES", 5 * 1024 * 1024 * 1024):
            assert client_manager.effective_max_file_size_bytes() == settings.PROTOCOL_MAX_FILE_SIZE_BYTES
    finally:
        unseed_accounts(client_manager)


def test_refresh_premium_ttl_cache():
    """TTL 命中时不应触发任何 RPC (userbot 为 None 也安全返回缓存值)"""
    seed_default_account(
        client_manager,
        client=None,
        is_premium=True,
        premium_checked_at=time.monotonic(),
    )
    try:
        import asyncio

        assert asyncio.run(client_manager.refresh_premium()) is True
    finally:
        unseed_accounts(client_manager)


# ---------- 超限三分类 ----------


def test_classify_oversize_kinds():
    # 2.5GB 非 Premium → 开会员可解
    kind, reason = classify_oversize(int(2.5 * 1024 * 1024 * 1024), settings.MAX_FILE_SIZE_BYTES, False)
    assert kind == "non_premium_limit"
    assert "Premium" in reason and "2000" in reason

    # 2.5GB Premium → 未超限
    kind, reason = classify_oversize(int(2.5 * 1024 * 1024 * 1024), settings.PREMIUM_MAX_FILE_SIZE_BYTES, True)
    assert kind == "" and reason is None

    # 4.05GB (> 4000 MiB (3.91 GiB)) Premium → 超过 Telegram 单文件最高上限
    kind, reason = classify_oversize(4250000000, settings.PREMIUM_MAX_FILE_SIZE_BYTES, True)
    assert kind == "premium_limit"
    assert "4000" in reason

    # 4.5GB (> 4000 MiB (3.91 GiB)) 非 Premium 亦超过全平台单文件最高上限 (开会员亦不可解，二态收敛)
    kind, reason = classify_oversize(int(4.5 * 1024 * 1024 * 1024), settings.MAX_FILE_SIZE_BYTES, False)
    assert kind == "premium_limit"
    assert "4000" in reason


def test_classify_oversize_premium_custom_threshold_boundary():
    """验证 Premium 账号在自定义/特殊门限 (如 PikPak 3900 MiB) 下，3900~4000 MiB 文件判定为 premium_limit 且不包含开通会员升级提示"""
    limit_3900_bytes = 3900 * 1024 * 1024
    file_3950_bytes = 3950 * 1024 * 1024

    # 1. 真实 Premium 账号超出自建门限 (如 3900 MiB)，但未超全平台 4000 MiB：
    # 必须归类为 premium_limit，且绝对不能提示用户“开通 Telegram Premium”
    kind, reason = classify_oversize(file_3950_bytes, limit_3900_bytes, is_premium=True)
    assert kind == "premium_limit"
    assert "超过当前 Premium 账号有效上传上限 3900 MiB" in reason
    assert "必须分卷切片" in reason
    assert "开通 Telegram Premium" not in reason
    assert "非 Premium" not in reason

    # 2. 非 Premium 账号在相同门限下：归类为 non_premium_limit 并引导开通 Premium
    kind_non_prem, reason_non_prem = classify_oversize(file_3950_bytes, limit_3900_bytes, is_premium=False)
    assert kind_non_prem == "non_premium_limit"
    assert "开通 Telegram Premium" in reason_non_prem
    assert "4000 MiB (3.91 GiB)" in reason_non_prem


# ---------- inspect_folder 动态上限与报告字段 ----------


def test_inspect_folder_dynamic_limit(tmp_dir):
    from unittest.mock import patch as _patch

    with (
        _patch.object(settings, "MAX_FILE_SIZE_BYTES", 1 * MB),
        _patch.object(settings, "PREMIUM_MAX_FILE_SIZE_BYTES", 2 * MB),
        _patch.object(settings, "PROTOCOL_MAX_FILE_SIZE_BYTES", 4 * MB),
    ):
        virtual_files = {
            "small.mp4": 512 * 1024,  # 两档均通过
            "mid.mp4": int(1.5 * MB),  # 非 Premium 超限 (开会员可解) / Premium 通过
            "big.mp4": 3 * MB,  # 两档均超限 (超过全平台最高上限 2MB)
            "huge.mp4": 5 * MB,  # 超过全平台最高上限 2MB
        }

        seed_default_account(client_manager, client=None, is_premium=False)
        try:
            report = inspect_with_virtual_files(tmp_dir, virtual_files)
            assert report["account_premium"] is False
            assert report["effective_limit_mb"] == 1.0
            kinds = {item["file_name"]: item["oversize_kind"] for item in report["oversized_files"]}
            assert kinds == {
                "mid.mp4": "non_premium_limit",
                "big.mp4": "premium_limit",
                "huge.mp4": "premium_limit",
            }
            assert report["valid_files_count"] == 1

            seed_default_account(client_manager, client=None, is_premium=True)
            report2 = inspect_with_virtual_files(tmp_dir, virtual_files)
            assert report2["account_premium"] is True
            assert report2["effective_limit_mb"] == 2.0
            kinds2 = {item["file_name"]: item["oversize_kind"] for item in report2["oversized_files"]}
            assert kinds2 == {
                "big.mp4": "premium_limit",
                "huge.mp4": "premium_limit",
            }
            assert report2["valid_files_count"] == 2

            # Pydantic 契约兼容校验
            InspectFolderResponse(**report2)
        finally:
            unseed_accounts(client_manager)


# ---------- FR5: 错误分类与归因包装 ----------


def test_classify_error_premium_patterns():
    assert (
        FailureLogger.classify_error(Exception("FILE_PARTS_INVALID: The number of parts is invalid"))
        == "FILE_TOO_LARGE"
    )
    assert FailureLogger.classify_error(Exception("Request too large")) == "FILE_TOO_LARGE"
    assert (
        FailureLogger.classify_error(
            Exception("当前账号非 Premium，上传上限 2000 MiB (1.95 GiB) (当前文件 2500.00 MiB)")
        )
        == "FILE_TOO_LARGE"
    )
    # 既有分类回归
    assert FailureLogger.classify_error(Exception("connection reset")) == "NETWORK_TIMEOUT"
    assert FailureLogger.classify_error(Exception("FloodWait 60s")) == "FLOOD_WAIT"


def test_describe_upload_failure():
    err = Exception("FILE_PARTS_INVALID: parts invalid")
    wrapped = describe_upload_failure(err, int(2.5 * 1024 * 1024 * 1024), True)
    assert wrapped is not err
    assert "Premium" in str(wrapped) and "原始错误" in str(wrapped)

    # 文件未超过非 Premium 上限 → 不包装，原样返回
    small_err = Exception("FILE_PARTS_INVALID")
    assert describe_upload_failure(small_err, 100 * MB, True) is small_err
    # 超限但错误不匹配额度签名 → 原样返回
    other_err = Exception("SomeOtherError")
    assert describe_upload_failure(other_err, int(2.5 * 1024 * 1024 * 1024), True) is other_err


# ---------- API 契约 ----------


def test_telegram_status_premium_fields():
    account = seed_default_account(
        client_manager,
        client=None,
        is_premium=True,
        premium_checked_at=time.monotonic(),  # TTL 命中，不发 RPC
    )
    account.user_id = 1
    try:
        resp = client.get("/api/telegram/status", headers=auth_headers())
        assert resp.status_code == 200
        data = resp.json()
        assert data["premium"] is True
        assert data["upload_limit_mb"] == round(settings.PREMIUM_MAX_FILE_SIZE_BYTES / MB, 2)
        assert "auth_state" in data and "is_connected" in data
    finally:
        unseed_accounts(client_manager)


def test_publish_context_in_flight_per_connection_dynamism(monkeypatch):
    """验证 PublishContext 完全以 upload_in_flight_per_connection 为单一规范属性，无别名，且动态探针生效"""
    from app.core.pipeline.models import PublishContext

    ctx = PublishContext(
        task_id="ctx_dyn_test",
        target_chat_id="-1001",
        upload_concurrency=4,
        upload_in_flight_per_connection=2,
    )
    # 验证 Zero-Alias Clean Break: 绝对不存在 upload_in_flight 属性或 @property
    assert ctx.upload_in_flight_per_connection == 2
    assert not hasattr(ctx, "upload_in_flight")

    # 模拟 DB 更新
    db_state = {
        "params": {
            "upload_concurrency": 2,
            "upload_in_flight_per_connection": 3,
        },
        "concurrency_total": 2,
    }
    mock_repo = MagicMock()
    mock_repo.get_by_id.return_value = db_state
    monkeypatch.setattr("app.db.task_repository.TaskQueueRepository.get_by_id", mock_repo.get_by_id)
    monkeypatch.setattr("time.monotonic", lambda: 5000.0)

    # 触发动态读取
    val = ctx.get_current_in_flight()
    assert val == 3
    assert ctx.upload_in_flight_per_connection == 3
    assert not hasattr(ctx, "upload_in_flight")


# ==============================================================================
# Part: .tmp_p2_cm.py
# ==============================================================================
def test_device_fingerprint_and_tier_persistence():
    """[REQ-003] 验证账号分级 (tier) 与设备指纹 (device_model / system_version) 持久化"""
    # 1. 测试指纹随机生成器
    fp = generate_device_fingerprint()
    assert "device_model" in fp
    assert "system_version" in fp
    assert "app_version" in fp
    assert "lang_code" in fp

    # 2. 创建带有指纹和分级的测试账号
    acc = AccountRepository.create(
        account_name="FingerprintTestUser",
        phone="+12025550106",
        session_file="dummy.session",
        api_id=12345,
        api_hash="dummy_hash",
        tier="secondary",
        device_model="MacBook Pro",
        system_version="macOS 14.3",
        app_version="5.4.1",
        lang_code="en",
    )
    acc_id = acc["id"]

    try:
        # 查询验证
        fetched = AccountRepository.get_by_id(acc_id)
        assert fetched["tier"] == "secondary"
        assert fetched["device_model"] == "MacBook Pro"
        assert fetched["system_version"] == "macOS 14.3"
        assert fetched["app_version"] == "5.4.1"
        assert fetched["lang_code"] == "en"

        # 测试指纹更新
        AccountRepository.update_device_fingerprint(
            acc_id, device_model="PC 64bit", system_version="Windows 11", app_version="5.4.1", lang_code="zh-CN"
        )
        updated = AccountRepository.get_by_id(acc_id)
        assert updated["device_model"] == "PC 64bit"
        assert updated["system_version"] == "Windows 11"
        assert updated["lang_code"] == "zh-CN"
    finally:
        AccountRepository.delete(acc_id)


@pytest.mark.asyncio
async def test_account_tiering_priority():
    """[REQ-003] 验证下载类任务优先使用 secondary/archive 账号，保护主账号 primary"""
    # 创建 mock 客户端
    primary_client = MagicMock()
    primary_client.account_id = 101
    primary_client.account_name = "PrimaryAccount"
    primary_client.tier = "primary"
    primary_client.is_active = True
    primary_client.is_ready.return_value = True
    primary_client.is_flood_locked.return_value = False
    primary_client.active_task_count = 0
    primary_client.max_concurrency = 2

    secondary_client = MagicMock()
    secondary_client.account_id = 102
    secondary_client.account_name = "SecondaryAccount"
    secondary_client.tier = "secondary"
    secondary_client.is_active = True
    secondary_client.is_ready.return_value = True
    secondary_client.is_flood_locked.return_value = False
    secondary_client.active_task_count = 0
    secondary_client.max_concurrency = 2

    with patch.dict(client_manager._accounts, {101: primary_client, 102: secondary_client}, clear=True):
        # 下载任务优先匹配 secondary 账号
        selected = await client_manager.get_available_account(task_type="download_link")
        assert selected is not None
        assert selected.account_id == 102
        assert selected.tier == "secondary"

        # 如果 secondary 不就绪，应平滑回退到 primary 账号
        secondary_client.is_ready.return_value = False
        fallback = await client_manager.get_available_account(task_type="download_link")
        assert fallback is not None
        assert fallback.account_id == 101


async def test_saturated_pool_probes_private_download_but_skips_publish_probe(monkeypatch):
    cm = ClientManager()
    primary = seed_default_account(cm, MagicMock(), account_id=1)
    secondary = seed_default_account(cm, MagicMock(), account_id=2)
    secondary.tier = "secondary"
    secondary.is_default = False
    monkeypatch.setattr(TelegramAccountClient, "active_task_count", property(lambda a: a.max_concurrency))
    resolve = AsyncMock(return_value=object())
    can_write = AsyncMock()
    monkeypatch.setattr("app.core.account_selection.resolve_entity", resolve)
    monkeypatch.setattr("app.core.account_selection.probe_write_permission", can_write)

    assert await cm.get_available_account("download_link", "-10012345") is secondary
    resolve.assert_awaited_once_with(secondary.client, -10012345)

    assert await cm.get_available_account("publish_folder", "-10012345") is primary
    can_write.assert_not_awaited()


def test_account_default_max_concurrency_is_1():
    """需求 C: 验证 TelegramAccountClient 与 AccountRepository.create 缺省单账号并发限额已由 2 收敛至 1"""
    client_obj = TelegramAccountClient(
        account_id=777,
        account_name="DefaultConcurrencyClient",
        phone="+12025550128",
        session_file="def_conc_777.session",
        api_id=123456,
        api_hash="hash_def_conc",
    )
    assert client_obj.max_concurrency == 1

    created = AccountRepository.create(
        account_name="DefaultConcurrencyRepoAcc",
        phone="+12025550129",
        session_file="def_conc_repo.session",
        api_id=123456,
        api_hash="hash_def_conc",
    )
    try:
        assert created["max_concurrency"] == 1
    finally:
        AccountRepository.delete(created["id"])


@pytest.mark.asyncio
async def test_report_session_invalid_targets_specific_account():
    """验证 report_session_invalid 传递 account_id 时仅失效目标账号、异步落库并随广播下发"""
    with (
        patch.object(client_manager, "invalidate_session") as m_inv,
        patch("app.core.session_errors.connection_manager.broadcast", new_callable=AsyncMock) as m_bcast,
        patch("app.core.session_errors.TaskQueueRepository") as m_repo,
    ):
        m_repo.update_status_async = AsyncMock()
        m_repo.append_log_async = AsyncMock()
        exc = RuntimeError("SessionRevokedError")
        await report_session_invalid("t123", exc, account_id=2)

        m_inv.assert_called_once_with(f"RuntimeError: {exc}", account_id=2)
        m_repo.update_status_async.assert_awaited_once_with("t123", "failed")
        m_repo.append_log_async.assert_awaited_once()

        sys_event_calls = [c[0][0] for c in m_bcast.call_args_list if c[0][0].get("type") == "system_event"]
        assert len(sys_event_calls) == 1
        assert sys_event_calls[0]["account_id"] == 2


@pytest.mark.asyncio
async def test_web_2fa_lockout_persists_across_expiry_and_resend():
    """验证 Web 2FA 连续 3 次输错后锁定 30 分钟，不被 3 分钟验证码过期或重发验证码绕过"""
    acc = TelegramAccountClient(
        account_id=99,
        account_name="test_2fa",
        phone="123456",
        session_file="dummy.session",
        api_id=1,
        api_hash="hash",
    )
    acc.auth_state = "AUTHENTICATED"
    acc.client = MagicMock()
    acc.client.send_message = AsyncMock(return_value=True)

    ip = "192.168.1.100"
    assert await acc.send_web_2fa_code(ip) is True
    assert acc.verify_web_2fa_code(ip, "000000") is False

    # 未满 3 次重发验证码时，attempts 计数不被重置清空
    await acc.send_web_2fa_code(ip)
    assert acc.verify_web_2fa_code(ip, "000000") is False

    with pytest.raises(Exception, match="连续输入错误 3 次，IP已被锁定 30 分钟。"):
        acc.verify_web_2fa_code(ip, "000000")

    # 200s 后挑战码(180s)已过期，但仍在 1800s 锁定窗口内
    future_time = time.time() + 200
    with patch("time.time", return_value=future_time):
        with pytest.raises(Exception, match="尝试过于频繁，已锁定"):
            await acc.send_web_2fa_code(ip)
        with pytest.raises(Exception, match="尝试过于频繁，已锁定"):
            acc.verify_web_2fa_code(ip, "000000")


def test_web_2fa_verify_without_pending_code_skips_lock_allocation_and_bounds_capacity():
    """验证无待验挑战码的 verify 调用不写入 _web_2fa_locks，且锁表受容量上限保护"""
    acc = TelegramAccountClient(
        account_id=98,
        account_name="test_2fa_cap",
        phone="123456",
        session_file="dummy_cap.session",
        api_id=1,
        api_hash="hash",
    )
    assert acc.verify_web_2fa_code("10.0.0.1", "123456") is False
    assert "10.0.0.1" not in acc._web_2fa_locks

    now = time.time()
    acc._web_2fa_locks = {
        f"10.0.0.{i}": {"attempts": 1, "locked_until": 0.0, "reset_at": now + 100 + i}
        for i in range(acc._MAX_WEB_2FA_LOCKS)
    }
    acc._prune_web_2fa_locks(now, "10.0.0.0")
    assert "10.0.0.0" in acc._web_2fa_locks
    assert "10.0.0.1" not in acc._web_2fa_locks
    assert len(acc._web_2fa_locks) == acc._MAX_WEB_2FA_LOCKS - 1


def test_get_account_returns_none_for_unknown_explicit_id_and_falls_back_on_zero():
    """验证显式传入不存在的正数 account_id 返回 None，0 与 None 回退默认账号"""
    acc1 = TelegramAccountClient(
        account_id=1,
        account_name="primary",
        phone="111",
        session_file="1.session",
        api_id=1,
        api_hash="hash",
        is_default=True,
        is_active=True,
    )
    with patch.dict(client_manager._accounts, {1: acc1}, clear=True):
        assert client_manager.get_account(1) is acc1
        assert client_manager.get_account(None) is acc1
        assert client_manager.get_account(0) is acc1
        with patch("app.db.account_repository.AccountRepository.get_all", return_value=[]):
            assert client_manager.get_account(999) is None


def test_client_manager_resolve_owner_and_invalidate_bot():
    """验证 resolve_client_account_id / is_bot_client / invalidate_bot_by_client 归属反查与隔离."""
    from app.core.client_manager import TelegramBotClient

    manager = ClientManager()
    user_client = MagicMock(name="user_client")
    bot_client = MagicMock(name="bot_client")
    unknown_client = MagicMock(name="unknown_client")

    acc = TelegramAccountClient(
        account_id=7,
        account_name="Acc7",
        phone="123",
        session_file="7.session",
        api_id=1,
        api_hash="h",
    )
    acc.client = user_client
    bot = TelegramBotClient(
        bot_id=11,
        bot_token="11:tok",
        session_name="bot_11",
        api_id=1,
        api_hash="h",
    )
    bot.client = bot_client
    bot.status = "ready"

    manager._accounts = {7: acc}
    manager._bots = {11: bot}

    assert manager.resolve_client_account_id(user_client) == 7
    assert manager.resolve_client_account_id(bot_client) is None
    assert manager.resolve_client_account_id(unknown_client) is None

    assert manager.is_bot_client(bot_client) is True
    assert manager.is_bot_client(user_client) is False

    with patch("app.core.client_manager.BotRepository.update_status") as mock_upd:
        assert manager.invalidate_bot_by_client(bot_client, "AuthKeyUnregisteredError") is True
        assert bot.status == "invalid"
        mock_upd.assert_called_once_with(11, "invalid")
        assert manager.invalidate_bot_by_client(user_client, "nope") is False


def test_is_session_invalid_error_detects_beyond_20_chunk_truncation():
    """超过 20 个缺失分块时，即便 __str__ 截断前 20 个，is_session_invalid_error 仍应识别第 25 块的会话失效."""
    from telethon.errors import AuthKeyUnregisteredError

    from app.core.fast_telethon import DownloadIncompleteError
    from app.core.session_errors import is_session_invalid_error

    missing = [i * 512 * 1024 for i in range(25)]
    last_errors = {off: "timed out" for off in missing[:-1]}
    last_errors[missing[-1]] = "The key is not registered in the system"
    last_exceptions = {missing[-1]: AuthKeyUnregisteredError(request=None)}

    err = DownloadIncompleteError(
        "/tmp/large.bin",
        missing,
        last_errors,
        total_chunks=25,
        completed_chunks=0,
        last_exceptions=last_exceptions,
    )
    assert "not registered" not in str(err)
    assert is_session_invalid_error(err) is True


def test_is_session_invalid_error_ignores_media_controlled_filename():
    """聚合下载异常消息内嵌媒体文件名 (发布者可控)，含会话失效关键词的文件名不得触发鉴权分类 (安全审查回归)."""
    from app.core.fast_telethon import DownloadIncompleteError
    from app.core.session_errors import is_session_invalid_error

    chunk = 512 * 1024
    err = DownloadIncompleteError(
        r"D:\downloads\unauthorized.bin",
        [0, chunk],
        {0: "TimeoutError: timed out", chunk: "TimeoutError: timed out"},
        total_chunks=2,
        completed_chunks=0,
        last_clients={0: object(), chunk: object()},
        last_exceptions={0: TimeoutError("timed out"), chunk: TimeoutError("timed out")},
    )
    assert "unauthorized" in str(err)
    assert is_session_invalid_error(err) is False

    # 对照: 无逐块明细的普通异常仍走文本匹配 (语义不受影响)
    assert is_session_invalid_error(RuntimeError("unauthorized access")) is True


@pytest.mark.asyncio
async def test_flood_wait_auto_recovers_on_is_ready():
    """#3: FloodWait 到期后调用 is_ready() 须先解封再检查状态, 不可永久卡死."""
    from app.core.client_manager import TelegramBotClient

    acc = TelegramAccountClient(11, "fw_acc", "+1", "fw.session", 1, "h", is_active=True)
    acc.auth_state = "AUTHENTICATED"
    acc.client = MagicMock()
    acc.client.is_connected.return_value = True
    with patch.object(AccountRepository, "mark_flood_wait"), patch.object(AccountRepository, "update_status"):
        acc.mark_flood_wait(10)
        assert acc.is_ready() is False
        acc.flood_wait_until = time.time() - 1.0
        assert acc.is_ready() is True
        assert acc.auth_state == "AUTHENTICATED"

    bot = TelegramBotClient(21, "1:tok", "fw_bot", 1, "h", is_active=True)
    bot.client = MagicMock()
    bot.client.is_connected.return_value = True
    with patch.object(BotRepository, "mark_flood_wait"), patch.object(BotRepository, "update_status"):
        bot.mark_flood_wait(10)
        assert bot.is_ready() is False
        bot.flood_wait_until = time.time() - 1.0
        assert bot.is_ready() is True
        assert bot.status == "ready"


@pytest.mark.asyncio
async def test_account_connect_propagates_session_invalid_from_get_me():
    """#4: connect() 内 get_me() 抛出会话失效异常时须透传至外层 invalidate_session."""
    from telethon.errors import AuthKeyUnregisteredError

    acc = TelegramAccountClient(12, "inv_acc", "+1", "inv.session", 1, "h", is_active=True)
    acc.device_model, acc.system_version, acc.app_version = "PC", "Win", "1.0"
    mock_client = MagicMock()
    mock_client.is_connected.return_value = True
    mock_client.is_user_authorized = AsyncMock(return_value=True)
    mock_client.get_me = AsyncMock(side_effect=AuthKeyUnregisteredError(request=None))
    mock_client.session = MagicMock(dc_id=2)
    acc.client = mock_client

    with patch.object(AccountRepository, "update_status") as m_upd:
        await acc.connect(force_reconnect=False)
        assert acc.auth_state == "UNAUTHENTICATED"
        m_upd.assert_called_with(12, "invalid")


@pytest.mark.asyncio
async def test_bot_connect_and_ping_require_non_none_get_me():
    """#5: Bot connect/ping/ping_bots_detailed 遇到 get_me() 返回 None 时不得标记 ready."""
    from app.core.client_manager import TelegramBotClient

    bot = TelegramBotClient(22, "2:tok", "none_bot", 1, "h", is_active=True)
    mock_client = MagicMock()
    mock_client.is_connected.return_value = True
    mock_client.start = AsyncMock()
    mock_client.disconnect = AsyncMock()
    mock_client.get_me = AsyncMock(return_value=None)
    mock_client.session = MagicMock(dc_id=2)
    bot.client = mock_client

    with (
        patch.object(BotRepository, "update_status"),
        patch("app.core.client_manager.TelegramClient", return_value=mock_client),
    ):
        await bot.connect()
        assert bot.status == "unauthenticated"
        assert bot.is_ready() is False

        cm = ClientManager()
        cm._bots[22] = bot
        ping_res = await cm.ping_bots()
        assert ping_res[22] is False
        assert bot.status == "unauthenticated"

        detailed = await cm.ping_bots_detailed()
        assert detailed[0]["online"] is False
        assert detailed[0]["status"] == "unauthenticated"


async def test_repeated_transient_bot_probes_keep_existing_connection():
    from app.core.client_manager import TelegramBotClient

    bot = TelegramBotClient(1, "1:test", "live_bot", 1, "hash")
    client = MagicMock()
    client.is_connected.return_value = True
    client.get_me = AsyncMock(
        side_effect=[
            TimeoutError("first timeout"),
            TimeoutError("second timeout"),
            SimpleNamespace(id=1, bot=True, username="live_bot"),
        ]
    )
    client.disconnect = AsyncMock()
    bot.client = client
    bot.status = "ready"

    with patch.object(BotRepository, "update_status"):
        for failure in ("first timeout", "second timeout"):
            assert await bot.connect(probe=True) == failure
            assert not bot.is_ready_snapshot()
            assert bot.client is client
        assert await bot.connect(probe=True) is None

    client.disconnect.assert_not_awaited()
    assert bot.is_ready_snapshot()


@pytest.mark.parametrize("session_invalid", [False, True])
async def test_bot_probe_detaches_live_client_when_authorization_is_lost(session_invalid):
    from telethon.errors import AuthKeyUnregisteredError

    from app.core.client_manager import TelegramBotClient

    bot = TelegramBotClient(1, "1:test", "lost_session_bot", 1, "hash")
    client = MagicMock()
    client.is_connected.return_value = True
    client.get_me = AsyncMock(return_value=None)
    if session_invalid:
        client.get_me.side_effect = AuthKeyUnregisteredError(request=None)
    client.disconnect = AsyncMock()
    bot.client = client
    bot.status = "ready"

    with patch.object(BotRepository, "update_status") as update_status:
        assert await bot.connect(probe=True) is not None

    assert bot.status == ("invalid" if session_invalid else "unauthenticated")
    update_status.assert_called_once_with(bot.bot_id, bot.status)
    assert bot.client is None
    client.disconnect.assert_awaited_once()


async def test_bot_probe_replaces_client_already_marked_invalid():
    from app.core.client_manager import TelegramBotClient

    bot = TelegramBotClient(1, "1:test", "invalid_bot", 1, "hash")
    old_client = MagicMock()
    old_client.is_connected.return_value = True
    old_client.get_me = AsyncMock()
    old_client.disconnect = AsyncMock()
    bot.client = old_client
    bot.status = "invalid"
    replacement = MagicMock()
    replacement.is_connected.return_value = False

    async def start(**_kwargs):
        replacement.is_connected.return_value = True

    replacement.start = AsyncMock(side_effect=start)
    replacement.get_me = AsyncMock(return_value=SimpleNamespace(id=1, bot=True, username="restored_bot"))

    with (
        patch.object(BotRepository, "update_status"),
        patch("app.core.client_manager.TelegramClient", return_value=replacement),
    ):
        assert await bot.connect(probe=True) is None

    old_client.disconnect.assert_awaited_once()
    old_client.get_me.assert_not_awaited()
    replacement.start.assert_awaited_once_with(bot_token=bot.bot_token)
    assert bot.client is replacement
    assert bot.is_ready_snapshot()


@pytest.mark.parametrize("cancel", [False, True])
async def test_failed_bot_connect_releases_client_and_manual_ping_recovers(monkeypatch, cancel):
    from app.core.client_manager import TelegramBotClient
    from app.core.scheduler import task_scheduler

    manager = ClientManager()
    bot = TelegramBotClient(1, "1:test", "retry_bot", 1, "hash", bot_name="Retry Bot")
    manager._bots[1] = bot
    monkeypatch.setattr(task_scheduler, "is_running", True)
    monkeypatch.setattr(task_scheduler, "startup_degraded", False)
    entered = asyncio.Event()

    async def fail_start(**_kwargs):
        entered.set()
        if cancel:
            await asyncio.Future()
        raise OSError("test network failure")

    failed_client = MagicMock()
    failed_client.start = AsyncMock(side_effect=fail_start)
    failed_client.is_connected.return_value = False
    failed_client.disconnect = AsyncMock()
    recovered_client = MagicMock()
    recovered_client.get_me = AsyncMock(return_value=SimpleNamespace(id=1, bot=True, username="retry_bot"))
    recovered_client.is_connected.return_value = True

    with (
        patch.object(BotRepository, "update_status"),
        patch("app.core.client_manager.TelegramClient", side_effect=[failed_client, recovered_client]),
        patch("app.core.client_manager.connection_manager.broadcast", new_callable=AsyncMock) as broadcast,
    ):
        connect = asyncio.create_task(bot.connect())
        await asyncio.wait_for(entered.wait(), timeout=2)
        if cancel:
            connect.cancel()
            with pytest.raises(asyncio.CancelledError):
                await connect
        else:
            assert await connect == "test network failure"
        assert bot.client is None
        failed_client.disconnect.assert_awaited_once()
        assert manager.get_startup_status()["phase"] == "degraded"

        result = await manager.ping_bots_detailed()
        assert result[0]["online"] is True
        assert manager.get_startup_status()["phase"] == "ready"
        broadcast.assert_awaited_once_with({"type": "system_event", "event": "telegram_startup_changed"})


async def test_pool_recovery_does_not_connect_removed_bot_from_snapshot(monkeypatch):
    from app.core.client_manager import TelegramBotClient

    manager = ClientManager()
    first = TelegramBotClient(1, "1:test", "first_bot", 1, "hash")
    removed = TelegramBotClient(2, "2:test", "removed_bot", 1, "hash")
    manager._bots = {1: first, 2: removed}
    started, release = asyncio.Event(), asyncio.Event()

    async def block_first():
        started.set()
        await release.wait()

    monkeypatch.setattr(manager, "_sync_pool_records_from_db", MagicMock())
    monkeypatch.setattr(first, "connect", block_first)
    monkeypatch.setattr(BotRepository, "delete", MagicMock())
    factory = MagicMock()
    monkeypatch.setattr("app.core.client_manager.TelegramClient", factory)
    recovery = asyncio.create_task(manager._connect_remaining_pool())
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        await asyncio.wait_for(manager.remove_bot(2), timeout=2)
    finally:
        release.set()
        await asyncio.wait_for(recovery, timeout=2)
    assert await removed.connect() == "Bot 未启用或已移除"
    factory.assert_not_called()


async def test_removing_bot_during_get_me_waits_for_client_cleanup(monkeypatch):
    from app.core.client_manager import TelegramBotClient

    manager = ClientManager()
    bot = TelegramBotClient(1, "1:test", "retiring_bot", 1, "hash")
    manager._bots[1] = bot
    probing, release, deleting = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def get_me():
        probing.set()
        await release.wait()
        return SimpleNamespace(id=1, bot=True, username="retiring_bot")

    client = MagicMock()
    client.is_connected.return_value = True
    client.get_me = AsyncMock(side_effect=get_me)
    client.disconnect = AsyncMock()
    bot.client = client
    monkeypatch.setattr(BotRepository, "delete", lambda _bot_id: deleting.set())
    write_status = MagicMock()
    monkeypatch.setattr(BotRepository, "update_status", write_status)

    connect = asyncio.create_task(bot.connect())
    await asyncio.wait_for(probing.wait(), timeout=2)
    remove = asyncio.create_task(manager.remove_bot(1))
    try:
        await asyncio.wait_for(deleting.wait(), timeout=2)
        assert manager.get_bot(1) is None
        assert not remove.done()
    finally:
        release.set()
        await remove
    assert await connect == "Bot 已移除"
    assert bot.client is None
    client.disconnect.assert_awaited_once()
    write_status.assert_not_called()


def test_startup_snapshot_counts_expired_bot_flood_wait_without_writing(monkeypatch):
    from app.core.client_manager import TelegramBotClient

    manager = ClientManager()
    bot = TelegramBotClient(1, "1:test", "cooldown_bot", 1, "hash")
    bot.client = MagicMock()
    bot.status = "flood_wait"
    bot.flood_wait_until = time.time() + 60
    manager._bots[1] = bot
    write = MagicMock()
    monkeypatch.setattr(BotRepository, "update_status", write)

    assert manager.get_startup_status()["ready_bots"] == 0
    bot.flood_wait_until = time.time() - 60
    assert manager.get_startup_status()["ready_bots"] == 1
    assert bot.status == "flood_wait"
    write.assert_not_called()
    assert bot.is_ready()
    write.assert_called_once_with(1, "ready")


@pytest.mark.asyncio
async def test_get_accessible_userbots_exclusion_does_not_poison_cache(monkeypatch):
    """#6: 带 exclude_account_ids 的探测结果不得写入无过滤的共享缓存."""
    cm = ClientManager()
    a1 = TelegramAccountClient(101, "A1", "+1", "a1.session", 1, "h", is_active=True)
    a2 = TelegramAccountClient(102, "A2", "+2", "a2.session", 1, "h", is_active=True)
    for a in (a1, a2):
        a.auth_state = "AUTHENTICATED"
        a.client = MagicMock()
        a.client.is_connected.return_value = True
        cm._accounts[a.account_id] = a

    monkeypatch.setattr("app.core.client_manager.resolve_entity", AsyncMock(return_value=MagicMock(id=999)))

    res_excluded = await cm.get_accessible_userbots("-100999", exclude_account_ids={101})
    assert [a.account_id for a in res_excluded] == [102]

    res_full = await cm.get_accessible_userbots("-100999")
    assert {a.account_id for a in res_full} == {101, 102}


@pytest.mark.asyncio
async def test_update_account_loads_newly_activated_account_into_pool():
    """#7: 启动时未加载的停用账号经 update_account 启用后须自动创建并连接."""
    cm = ClientManager()
    db_row = {
        "id": 77,
        "account_name": "new_active",
        "phone": "+15550000077",
        "session_file": "new_active.session",
        "api_id": 1,
        "api_hash": "hash",
        "max_concurrency": 2,
        "is_active": 1,
        "is_default": 0,
        "dc_id": 0,
        "tier": "primary",
        "device_model": "PC",
        "system_version": "Win",
        "app_version": "1.0",
        "lang_code": "en",
    }
    with (
        patch.object(AccountRepository, "update", return_value=db_row),
        patch.object(TelegramAccountClient, "connect", new_callable=AsyncMock) as m_conn,
    ):
        updated = await cm.update_account(77, is_active=True)
        assert updated == db_row
        assert 77 in cm._accounts
        m_conn.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("account_id", [31, None])
async def test_probe_chat_access_preserves_entity_for_write_permission(monkeypatch, account_id):
    """#8: probe_chat_access(require_write=True) 须把原始 Entity 传给 probe_write_permission."""
    from telethon.tl.types import Chat

    cm = ClientManager()
    acc = TelegramAccountClient(31, "ProbeAcc", "+1", "p.session", 1, "h", is_active=True)
    acc.auth_state = "AUTHENTICATED"
    acc.client = MagicMock()
    acc.client.is_connected.return_value = True
    cm._accounts[31] = acc

    chat_entity = Chat(
        id=12345,
        title="TestGroup",
        photo=None,
        participants_count=5,
        date=0,
        version=1,
        creator=True,
    )
    monkeypatch.setattr("app.core.account_selection.resolve_entity", AsyncMock(return_value=chat_entity))

    found_acc, found_entity = await cm.probe_chat_access("-10012345", account_id=account_id, require_write=True)
    assert found_acc is acc
    assert found_entity is chat_entity


@pytest.mark.asyncio
async def test_update_account_retries_existing_non_ready_and_invalidates_stale_probe(monkeypatch):
    """激活已存在但未就绪账号时须重连、失效访问缓存并防止在途旧探针回填过期缓存."""
    import asyncio

    cm = ClientManager()
    acc1 = TelegramAccountClient(101, "A1", "+1", "a1.session", 1, "h", is_active=True)
    acc1.auth_state = "AUTHENTICATED"
    acc1.client = MagicMock()
    acc1.client.is_connected.return_value = True

    acc2 = TelegramAccountClient(102, "A2", "+2", "a2.session", 1, "h", is_active=False)
    acc2.auth_state = "UNAUTHENTICATED"
    cm._accounts[101] = acc1
    cm._accounts[102] = acc2

    probe_started = asyncio.Event()
    release_probe = asyncio.Event()

    async def slow_resolve(client, parsed):
        if client is acc1.client:
            probe_started.set()
            await release_probe.wait()
            return MagicMock(id=999)
        return MagicMock(id=999)

    monkeypatch.setattr("app.core.client_manager.resolve_entity", slow_resolve)

    probe_task = asyncio.create_task(cm.get_accessible_userbots("-100999"))
    await asyncio.wait_for(probe_started.wait(), timeout=2.0)

    async def fake_connect():
        acc2.is_active = True
        acc2.auth_state = "AUTHENTICATED"
        acc2.client = MagicMock()
        acc2.client.is_connected.return_value = True

    db_row = {"id": 102, "is_active": 1}
    with (
        patch.object(AccountRepository, "update", return_value=db_row),
        patch.object(acc2, "connect", side_effect=fake_connect) as m_conn,
        patch.object(cm, "recover_startup_degraded", new_callable=AsyncMock) as m_recover,
    ):
        await cm.update_account(102, is_active=True)
        m_conn.assert_awaited_once()
        m_recover.assert_not_awaited()

    release_probe.set()
    first_res = await probe_task
    assert [a.account_id for a in first_res] == [101]
    assert "-100999" not in cm._chat_accessible_cache

    fresh_res = await cm.get_accessible_userbots("-100999")
    assert {a.account_id for a in fresh_res} == {101, 102}


@pytest.mark.asyncio
async def test_update_account_reactivating_already_ready_account_invalidates_cache(monkeypatch):
    """停用但保持连接就绪的账号重新启用时, 也须失效访问缓存防止旧缓存漏选."""
    cm = ClientManager()
    acc1 = TelegramAccountClient(101, "A1", "+1", "a1.session", 1, "h", is_active=True)
    acc2 = TelegramAccountClient(102, "A2", "+2", "a2.session", 1, "h", is_active=False)
    for a in (acc1, acc2):
        a.auth_state = "AUTHENTICATED"
        a.client = MagicMock()
        a.client.is_connected.return_value = True
        cm._accounts[a.account_id] = a

    monkeypatch.setattr("app.core.client_manager.resolve_entity", AsyncMock(return_value=MagicMock(id=999)))

    initial = await cm.get_accessible_userbots("-100888")
    assert [a.account_id for a in initial] == [101]
    assert "-100888" in cm._chat_accessible_cache

    with patch.object(AccountRepository, "update", return_value={"id": 102, "is_active": 1}):
        await cm.update_account(102, is_active=True)

    assert "-100888" not in cm._chat_accessible_cache
    refreshed = await cm.get_accessible_userbots("-100888")
    assert {a.account_id for a in refreshed} == {101, 102}


@pytest.mark.asyncio
async def test_native_verify_rejects_identity_mismatch():
    acc = TelegramAccountClient(
        account_id=888,
        account_name="LockedOwner",
        phone="+12025550119",
        session_file="locked_owner.session",
        api_id=1,
        api_hash="dummy",
        user_id=10001,
    )
    mock_tg = AsyncMock()
    mock_tg.is_connected = MagicMock(return_value=True)
    mock_tg.send_code_request = AsyncMock(return_value=MagicMock(phone_code_hash="h_id"))
    mock_tg.sign_in = AsyncMock()
    mock_tg.is_user_authorized = AsyncMock(return_value=True)
    mock_tg.get_me = AsyncMock(return_value=MagicMock(id=99999, username="attacker", first_name="Attacker"))
    mock_tg.log_out = AsyncMock()
    mock_tg.disconnect = AsyncMock()
    acc.client = mock_tg

    await acc.native_send_code("+12025550119")
    with pytest.raises(ValueError, match="身份与原绑定账号不一致"):
        await acc.native_verify_code("+12025550119", "12345", "h_id")
    mock_tg.log_out.assert_awaited_once()
    assert acc.auth_state == "UNAUTHENTICATED"


@pytest.mark.asyncio
async def test_client_manager_native_verify_requires_matching_pending_challenge(monkeypatch):
    acc = TelegramAccountClient(
        account_id=10,
        account_name="NativeLock",
        phone="",
        session_file="native_lock.session",
        api_id=1,
        api_hash="dummy",
        is_default=True,
    )
    mock_tg = AsyncMock()
    mock_tg.is_connected = MagicMock(return_value=True)
    mock_tg.send_code_request = AsyncMock(return_value=MagicMock(phone_code_hash="h_native"))
    mock_tg.sign_in = AsyncMock()
    mock_tg.is_user_authorized = AsyncMock(return_value=True)
    mock_tg.get_me = AsyncMock(return_value=MagicMock(id=5050, username="nat", first_name="Nat", premium=False))
    mock_tg.session = MagicMock(dc_id=4)
    acc.client = mock_tg
    monkeypatch.setattr(client_manager, "_accounts", {10: acc})

    # 未先 send-code 直接调 verify 必须被拦截
    with pytest.raises(ValueError, match="请先请求下发登录验证码"):
        await client_manager.native_verify_code("+12025550121", "12345", "h_native")

    # 下发验证码给号 A，尝试用号 B 验证必须被拦截
    h = await client_manager.native_send_code("+12025550121")
    assert h == "h_native"
    with pytest.raises(ValueError, match="不一致"):
        await client_manager.native_verify_code("+12025550122", "12345", "h_native")
    with pytest.raises(ValueError, match="Hash"):
        await client_manager.native_verify_code("+12025550121", "12345", "wrong_hash")


@pytest.mark.asyncio
async def test_connect_rejects_mismatched_restored_session_without_overwriting_user_id(tmp_path):
    db_acc = AccountRepository.create(
        account_name="ConnectIdentityLock",
        phone="+12025550123",
        session_file="connect_lock.session",
        api_id=1,
        api_hash="dummy",
    )
    acc_id = db_acc["id"]
    AccountRepository.update_status(acc_id, "authenticated", user_id=11111, username="orig_owner")

    acc = TelegramAccountClient(
        account_id=acc_id,
        account_name="ConnectIdentityLock",
        phone="+12025550123",
        session_file=str(tmp_path / "connect_lock.session"),
        api_id=1,
        api_hash="dummy",
        user_id=11111,
    )
    mock_tg = AsyncMock()
    mock_tg.is_connected = MagicMock(return_value=True)
    mock_tg.is_user_authorized = AsyncMock(return_value=True)
    mock_tg.get_me = AsyncMock(return_value=MagicMock(id=22222, username="swapped_user"))
    mock_tg.session = MagicMock(dc_id=4)
    acc.client = mock_tg

    try:
        await acc.connect()
        assert acc.auth_state == "UNAUTHENTICATED"
        assert acc.user_id == 11111
        stored = AccountRepository.get_by_id(acc_id)
        assert stored is not None
        assert stored["user_id"] == 11111
        assert stored["status"] == "invalid"
        mock_tg.disconnect.assert_awaited()
    finally:
        AccountRepository.delete(acc_id)


@pytest.mark.asyncio
async def test_native_verify_get_me_failure_fails_closed_without_logout():
    acc = TelegramAccountClient(
        account_id=777,
        account_name="TransientLookup",
        phone="+12025550124",
        session_file="transient_lookup.session",
        api_id=1,
        api_hash="dummy",
        user_id=10002,
    )
    mock_tg = AsyncMock()
    mock_tg.is_connected = MagicMock(return_value=True)
    mock_tg.send_code_request = AsyncMock(return_value=MagicMock(phone_code_hash="h_tr"))
    mock_tg.sign_in = AsyncMock()
    mock_tg.is_user_authorized = AsyncMock(return_value=True)
    mock_tg.get_me = AsyncMock(side_effect=RuntimeError("transient rpc failure"))
    mock_tg.log_out = AsyncMock()
    acc.client = mock_tg

    await acc.native_send_code("+12025550124")
    with pytest.raises(RuntimeError, match="未能获取 Telegram 账号身份"):
        await acc.native_verify_code("+12025550124", "12345", "h_tr")
    mock_tg.log_out.assert_not_awaited()
    assert acc.auth_state == "UNAUTHENTICATED"


@pytest.mark.asyncio
async def test_connect_transient_get_me_failure_keeps_unauthenticated_and_preserves_session(tmp_path):
    db_acc = AccountRepository.create(
        account_name="TransientGetMeConnect",
        phone="+12025550125",
        session_file=str(tmp_path / "transient_get_me.session"),
        api_id=1,
        api_hash="dummy",
    )
    acc_id = db_acc["id"]
    AccountRepository.update_status(acc_id, "authenticated", user_id=11111, username="orig_owner")

    acc = TelegramAccountClient(
        account_id=acc_id,
        account_name="TransientGetMeConnect",
        phone="+12025550125",
        session_file=str(tmp_path / "transient_get_me.session"),
        api_id=1,
        api_hash="dummy",
        user_id=11111,
    )
    mock_tg = AsyncMock()
    mock_tg.is_connected = MagicMock(return_value=True)
    mock_tg.is_user_authorized = AsyncMock(return_value=True)
    mock_tg.get_me = AsyncMock(side_effect=RuntimeError("transient rpc timeout"))
    mock_tg.session = MagicMock(dc_id=4)
    acc.client = mock_tg

    try:
        await acc.connect()
        assert acc.auth_state == "UNAUTHENTICATED"
        assert acc.is_ready() is False
        assert acc.user_id == 11111
        stored = AccountRepository.get_by_id(acc_id)
        assert stored is not None
        assert stored["user_id"] == 11111
        assert stored["status"] == "unauthenticated"
    finally:
        AccountRepository.delete(acc_id)


@pytest.mark.asyncio
async def test_concurrent_native_verify_code_serializes_without_logging_out_session():
    acc = TelegramAccountClient(
        account_id=888,
        account_name="ConcurrentVerify",
        phone="+12025550126",
        session_file="concurrent_verify.session",
        api_id=1,
        api_hash="dummy",
    )
    entered_sign_in = asyncio.Event()
    release_sign_in = asyncio.Event()
    sign_in_calls = 0

    async def fake_sign_in(**kwargs):
        nonlocal sign_in_calls
        sign_in_calls += 1
        entered_sign_in.set()
        await release_sign_in.wait()

    mock_tg = AsyncMock()
    mock_tg.is_connected = MagicMock(return_value=True)
    mock_tg.send_code_request = AsyncMock(return_value=MagicMock(phone_code_hash="h_conc"))
    mock_tg.sign_in = AsyncMock(side_effect=fake_sign_in)
    mock_tg.is_user_authorized = AsyncMock(return_value=True)
    mock_tg.get_me = AsyncMock(
        return_value=MagicMock(id=55555, phone="12025550126", username="owner", first_name="Owner", premium=False)
    )
    mock_tg.log_out = AsyncMock()
    acc.client = mock_tg

    await acc.native_send_code("+12025550126")

    with (
        patch("app.db.account_repository.AccountRepository.update_status"),
        patch("app.db.account_repository.AccountRepository.update_premium"),
    ):
        t1 = asyncio.create_task(acc.native_verify_code("+12025550126", "12345", "h_conc"))
        t2 = None
        try:
            async with asyncio.timeout(5):
                await entered_sign_in.wait()
                t2 = asyncio.create_task(acc.native_verify_code("+12025550126", "12345", "h_conc"))
                await asyncio.sleep(0)
                release_sign_in.set()
                r1, r2 = await asyncio.gather(t1, t2, return_exceptions=True)
        finally:
            release_sign_in.set()
            pending = [t for t in (t1, t2) if t is not None and not t.done()]
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    assert r1 is True
    assert isinstance(r2, ValueError)
    assert sign_in_calls == 1
    assert acc.auth_state == "AUTHENTICATED"
    mock_tg.log_out.assert_not_awaited()


@pytest.mark.asyncio
async def test_connect_unbound_account_accepts_telethon_phone_without_plus(tmp_path):
    db_acc = AccountRepository.create(
        account_name="FirstConnectPhoneMatch",
        phone="+12025550118",
        session_file=str(tmp_path / "first_connect_match.session"),
        api_id=1,
        api_hash="dummy",
    )
    acc_id = db_acc["id"]
    acc = TelegramAccountClient(
        account_id=acc_id,
        account_name="FirstConnectPhoneMatch",
        phone="+12025550118",
        session_file=str(tmp_path / "first_connect_match.session"),
        api_id=1,
        api_hash="dummy",
        user_id=None,
    )
    mock_tg = AsyncMock()
    mock_tg.is_connected = MagicMock(return_value=True)
    mock_tg.is_user_authorized = AsyncMock(return_value=True)
    mock_tg.get_me = AsyncMock(
        return_value=MagicMock(id=33333, phone="12025550118", username="real_u", first_name="Real", premium=False)
    )
    mock_tg.session = MagicMock(dc_id=4)
    acc.client = mock_tg

    try:
        await acc.connect()
        assert acc.auth_state == "AUTHENTICATED"
        assert acc.user_id == 33333
        stored = AccountRepository.get_by_id(acc_id)
        assert stored is not None
        assert stored["user_id"] == 33333
        assert stored["status"] == "authenticated"
    finally:
        AccountRepository.delete(acc_id)


@pytest.mark.asyncio
async def test_connect_unbound_account_rejects_mismatched_session_phone(tmp_path):
    db_acc = AccountRepository.create(
        account_name="FirstConnectPhoneMismatch",
        phone="+12025550118",
        session_file=str(tmp_path / "first_connect_mismatch.session"),
        api_id=1,
        api_hash="dummy",
    )
    acc_id = db_acc["id"]
    acc = TelegramAccountClient(
        account_id=acc_id,
        account_name="FirstConnectPhoneMismatch",
        phone="+12025550118",
        session_file=str(tmp_path / "first_connect_mismatch.session"),
        api_id=1,
        api_hash="dummy",
        user_id=None,
    )
    mock_tg = AsyncMock()
    mock_tg.is_connected = MagicMock(return_value=True)
    mock_tg.is_user_authorized = AsyncMock(return_value=True)
    mock_tg.get_me = AsyncMock(
        return_value=MagicMock(id=44444, phone="8613900009999", username="other_u", first_name="Other", premium=False)
    )
    mock_tg.session = MagicMock(dc_id=4)
    acc.client = mock_tg

    try:
        await acc.connect()
        assert acc.auth_state == "UNAUTHENTICATED"
        assert acc.user_id is None
        stored = AccountRepository.get_by_id(acc_id)
        assert stored is not None
        assert stored["user_id"] is None
        assert stored["status"] == "invalid"
        mock_tg.disconnect.assert_awaited()
    finally:
        AccountRepository.delete(acc_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("session_phone", [None, "", "   "])
async def test_connect_unbound_account_missing_phone_stays_unauthenticated(tmp_path, session_phone):
    db_acc = AccountRepository.create(
        account_name="FirstConnectMissingPhone",
        phone="+12025550118",
        session_file=str(tmp_path / "first_connect_missing_phone.session"),
        api_id=1,
        api_hash="dummy",
    )
    acc_id = db_acc["id"]
    acc = TelegramAccountClient(
        account_id=acc_id,
        account_name="FirstConnectMissingPhone",
        phone="+12025550118",
        session_file=str(tmp_path / "first_connect_missing_phone.session"),
        api_id=1,
        api_hash="dummy",
        user_id=None,
    )
    mock_tg = AsyncMock()
    mock_tg.is_connected = MagicMock(return_value=True)
    mock_tg.is_user_authorized = AsyncMock(return_value=True)
    mock_tg.get_me = AsyncMock(
        return_value=MagicMock(
            id=55555, phone=session_phone, username="no_phone_u", first_name="NoPhone", premium=False
        )
    )
    mock_tg.session = MagicMock(dc_id=4)
    acc.client = mock_tg

    try:
        await acc.connect()
        assert acc.auth_state == "UNAUTHENTICATED"
        assert acc.is_ready() is False
        assert acc.user_id is None
        stored = AccountRepository.get_by_id(acc_id)
        assert stored is not None
        assert stored["user_id"] is None
        assert stored["status"] == "unauthenticated"
        mock_tg.disconnect.assert_awaited()
    finally:
        AccountRepository.delete(acc_id)


@pytest.mark.parametrize(("bot_flag", "remote_id"), [(False, 1), (True, 2)])
async def test_bot_session_identity_mismatch_disconnects(bot_flag, remote_id):
    from app.core.client_manager import TelegramBotClient

    bot = TelegramBotClient(1, "1:test", "isolated", 1, "hash")
    tg = MagicMock()
    tg.is_connected.return_value = True
    tg.get_me = AsyncMock(return_value=SimpleNamespace(bot=bot_flag, id=remote_id))
    tg.disconnect = AsyncMock()
    bot.client = tg
    with patch.object(BotRepository, "update_status") as update:
        assert await bot.connect(probe=True)
    update.assert_called_once_with(1, "invalid")
    tg.disconnect.assert_awaited_once()
    assert bot.client is None
