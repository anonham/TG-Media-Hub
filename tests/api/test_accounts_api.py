from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

from app.api.auth import issue_2fa_token
from app.config import settings
from app.core.client_manager import TelegramAccountClient, _handover_path, client_manager
from app.db.account_repository import AccountRepository
from app.main import app

client = TestClient(app, client=("127.0.0.1", 50000))


def auth_headers():
    headers = {}
    if settings.API_KEY:
        headers["X-API-Key"] = settings.API_KEY
        token = issue_2fa_token()
        if token:
            headers["X-2FA-Token"] = token
    return headers


def test_list_and_create_account():
    # 1. 列表查询
    resp = client.get("/api/accounts", headers=auth_headers())
    assert resp.status_code == 200
    initial_count = len(resp.json())

    # 2. 创建新账号
    payload = {
        "account_name": "Test Userbot 2",
        "phone": "+12025550106",
        "session_file": "test_ub2.session",
        "api_id": 12345,
        "api_hash": "test_hash_abcdef",
        "max_concurrency": 3,
        "is_active": True,
        "is_default": False,
    }
    with (
        patch("telethon.TelegramClient.connect", new_callable=AsyncMock),
        patch("telethon.TelegramClient.is_user_authorized", new_callable=AsyncMock, return_value=False),
    ):
        resp = client.post("/api/accounts", json=payload, headers=auth_headers())
        assert resp.status_code == 200
        data = resp.json()
        assert data["account_name"] == "Test Userbot 2"
        assert data["max_concurrency"] == 3
        assert data["api_hash_masked"] == "test****cdef"
        assert "api_hash" not in data
        acc_id = data["id"]

    # 3. 再次查询列表
    resp = client.get("/api/accounts", headers=auth_headers())
    assert resp.status_code == 200
    assert len(resp.json()) == initial_count + 1

    # 4. 修改账号配置
    update_payload = {"account_name": "Updated Userbot 2", "max_concurrency": 4, "is_default": True}
    resp = client.put(f"/api/accounts/{acc_id}", json=update_payload, headers=auth_headers())
    assert resp.status_code == 200
    up_data = resp.json()
    assert up_data["account_name"] == "Updated Userbot 2"
    assert up_data["max_concurrency"] == 4
    assert up_data["is_default"] is True

    # 5. 删除账号
    resp = client.delete(f"/api/accounts/{acc_id}?delete_session_file=true", headers=auth_headers())
    assert resp.status_code == 200
    assert resp.json()["status"] == "deleted"


def test_pending_handover_recovery_failure_keeps_session_and_binding(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "DATA_DIR", str(tmp_path))
    session = tmp_path / "broken_handover.session"
    session.write_bytes(b"preserved session")
    journal = _handover_path(str(session), ".pending")
    journal.parent.mkdir()
    journal.write_text("invalid journal", encoding="ascii")
    account = AccountRepository.create(
        account_name="Pending recovery",
        phone="+12025550106",
        session_file=session.name,
        api_id=12345,
        api_hash="dummy",
    )
    account_id = account["id"]
    AccountRepository.update_status(account_id, "authenticated", user_id=1001)

    with patch("app.core.client_manager.TelegramClient") as telegram:
        refresh = client.post(f"/api/accounts/{account_id}/refresh", headers=auth_headers())
        telegram.assert_not_called()
    assert refresh.status_code == 200
    assert refresh.json()["auth_state"] == "UNAUTHENTICATED"
    row = AccountRepository.get_by_id(account_id)
    assert row is not None
    assert row["status"] == "unauthenticated"
    assert row["user_id"] == 1001

    update = client.put(f"/api/accounts/{account_id}", json={"phone": "+12025550105"}, headers=auth_headers())
    assert update.status_code == 400
    delete = client.delete(f"/api/accounts/{account_id}", headers=auth_headers())
    assert delete.status_code == 503
    assert AccountRepository.get_by_id(account_id) is not None
    assert client_manager._accounts[account_id].is_active
    assert journal.read_text(encoding="ascii") == "invalid journal"
    assert session.read_bytes() == b"preserved session"

    delete_with_session = client.delete(f"/api/accounts/{account_id}?delete_session_file=true", headers=auth_headers())
    assert delete_with_session.status_code == 200
    assert AccountRepository.get_by_id(account_id) is None
    assert not journal.exists()
    assert not session.exists()


def test_account_send_and_verify_code(monkeypatch):
    # 创建测试账号
    db_acc = AccountRepository.create(
        account_name="Auth Test Acc",
        phone="+12025550133",
        session_file="auth_test.session",
        api_id=123456,
        api_hash="hash_test_123",
    )
    acc_id = db_acc["id"]
    acc_client = client_manager.get_account(acc_id)
    assert acc_client is not None

    from types import SimpleNamespace

    mock_tg = AsyncMock()
    mock_tg.is_connected = MagicMock(return_value=True)
    mock_tg.session = SimpleNamespace(
        dc_id=4,
        server_address="149.154.167.91",
        port=443,
        auth_key=SimpleNamespace(key=b"\x01" * 256),
        takeout_id=None,
    )
    mock_tg.send_code_request = AsyncMock(
        return_value=MagicMock(phone_code_hash="mocked_phone_hash_123", next_type=None)
    )
    mock_tg.sign_in = AsyncMock()
    mock_tg.is_user_authorized = AsyncMock(return_value=True)
    mock_tg.get_me = AsyncMock(
        return_value=MagicMock(
            id=999111, phone="12025550133", username="test_user_ok", first_name="Tester", premium=True
        )
    )
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda acc: mock_tg)

    headers = auth_headers()
    # 发送验证码 (不得暴露 phone_code_hash)
    resp = client.post(f"/api/accounts/{acc_id}/send-code", json={"phone": "+12025550133"}, headers=headers)
    assert resp.status_code == 200
    send_data = resp.json()
    assert "phone_code_hash" not in send_data
    challenge_id = send_data["challenge_id"]
    assert challenge_id

    # 提交验证码
    resp = client.post(
        f"/api/accounts/{acc_id}/verify-code",
        json={"challenge_id": challenge_id, "code": "12345"},
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    assert resp.json()["is_premium"] is True

    # 刷新探测
    resp = client.post(f"/api/accounts/{acc_id}/refresh", headers=headers)
    assert resp.status_code == 200
    assert resp.json()["is_premium"] is True

    acc_client.auth_state = "UNAUTHENTICATED"
    acc_client.probe_premium = AsyncMock(return_value=False)
    for stored_status, auth_state in (("invalid", "INVALID"), ("unauthenticated", "UNAUTHENTICATED")):
        AccountRepository.update_status(acc_id, stored_status)
        resp = client.get("/api/accounts", headers=headers)
        assert resp.status_code == 200
        assert next(row for row in resp.json() if row["id"] == acc_id)["status"] == stored_status
        resp = client.post(f"/api/accounts/{acc_id}/refresh", headers=headers)
        assert resp.status_code == 200
        assert resp.json()["auth_state"] == auth_state

    # 清理
    AccountRepository.delete(acc_id)
    client_manager._accounts.pop(acc_id, None)


def test_bots_crud_and_ping():
    # 1. 列表查询
    resp = client.get("/api/bots", headers=auth_headers())
    assert resp.status_code == 200
    init_bots = len(resp.json())

    # 2. 添加新 Bot
    with (
        patch("telethon.TelegramClient.start", new_callable=AsyncMock),
        patch("telethon.TelegramClient.is_connected", return_value=True),
        patch(
            "telethon.TelegramClient.get_me",
            new_callable=AsyncMock,
            return_value=MagicMock(username="my_new_bot", id=12345),
        ),
    ):
        resp = client.post(
            "/api/bots",
            json={
                "bot_token": "987654321:ABCDefgh-12345",
                "bot_name": "API Test Bot",
                "session_name": "bot_api_test",
                "api_id": 12345,
                "api_hash": "hash123",
            },
            headers=auth_headers(),
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["bot_token_masked"] == "987654321:******2345"
        assert data["api_hash_masked"] == "****"
        assert "bot_token" not in data
        assert "api_hash" not in data
        bot_id = data["id"]

    # 3. 再次查询
    resp = client.get("/api/bots", headers=auth_headers())
    assert len(resp.json()) == init_bots + 1

    # 4. Ping 探活
    with (
        patch("telethon.TelegramClient.start", new_callable=AsyncMock),
        patch("telethon.TelegramClient.is_connected", return_value=True),
        patch(
            "telethon.TelegramClient.get_me",
            new_callable=AsyncMock,
            return_value=MagicMock(username="my_new_bot", id=12345),
        ),
    ):
        resp = client.post("/api/bots/ping", headers=auth_headers())
        assert resp.status_code == 200
        ping_data = resp.json()
        assert "total" in ping_data
        assert "online_count" in ping_data
        assert isinstance(ping_data["results"], list)

    # 5. 删除 Bot
    resp = client.delete(f"/api/bots/{bot_id}", headers=auth_headers())
    assert resp.status_code == 200
    assert resp.json()["status"] == "deleted"


def test_create_account_with_default_api_credentials():
    """测试创建 Userbot 账号时省略 api_id 与 api_hash，应自动回退到系统默认 settings.API_ID/API_HASH"""
    payload = {"account_name": "Default Creds Userbot", "phone": "+12025550108", "max_concurrency": 2}
    with (
        patch("telethon.TelegramClient.connect", new_callable=AsyncMock),
        patch("telethon.TelegramClient.is_user_authorized", new_callable=AsyncMock, return_value=False),
    ):
        resp = client.post("/api/accounts", json=payload, headers=auth_headers())
        assert resp.status_code == 200
        data = resp.json()
        assert data["account_name"] == "Default Creds Userbot"
        assert data["phone"] == "+12025550108"
        assert data["api_id"] == settings.API_ID
        acc_id = data["id"]

        # 清理测试账号
        del_resp = client.delete(f"/api/accounts/{acc_id}?delete_session_file=true", headers=auth_headers())
        assert del_resp.status_code == 200


def test_duplicate_account_name_returns_400_and_connection_healthy():
    """验证创建和修改同名账号时返回 HTTP 400，且事务回滚后数据库连接健康不锁死"""
    payload_a = {
        "account_name": "DupTest_A",
        "phone": "+12025550109",
        "session_file": "dup_test_a.session",
        "api_id": 12345,
        "api_hash": "hash_dup_a",
    }
    payload_b = {
        "account_name": "DupTest_B",
        "phone": "+12025550110",
        "session_file": "dup_test_b.session",
        "api_id": 12345,
        "api_hash": "hash_dup_b",
    }

    acc_a_id = None
    acc_b_id = None
    try:
        with (
            patch("telethon.TelegramClient.connect", new_callable=AsyncMock),
            patch("telethon.TelegramClient.is_user_authorized", new_callable=AsyncMock, return_value=False),
        ):
            # 1. 成功创建 A
            resp_a = client.post("/api/accounts", json=payload_a, headers=auth_headers())
            assert resp_a.status_code == 200
            acc_a_id = resp_a.json()["id"]

            # 2. 成功创建 B
            resp_b = client.post("/api/accounts", json=payload_b, headers=auth_headers())
            assert resp_b.status_code == 200
            acc_b_id = resp_b.json()["id"]

            # 3. 尝试创建同名账号（同 DupTest_A），必须返回 400
            dup_payload = {
                "account_name": "DupTest_A",
                "phone": "+12025550111",
                "session_file": "dup_test_c.session",
                "api_id": 12345,
                "api_hash": "hash_dup_c",
            }
            resp_dup = client.post("/api/accounts", json=dup_payload, headers=auth_headers())
            assert resp_dup.status_code == 400
            assert "已存在" in resp_dup.text or "使用" in resp_dup.text

            # 4. 尝试将 B 改名为 A 的名字（DupTest_A），必须返回 400 且不能返回 500
            resp_rename = client.put(
                f"/api/accounts/{acc_b_id}", json={"account_name": "DupTest_A"}, headers=auth_headers()
            )
            assert resp_rename.status_code == 400
            assert "已存在" in resp_rename.text or "使用" in resp_rename.text

            # 5. 验证数据库连接池依然 100% 健康，后续查询与写操作无锁死
            resp_list = client.get("/api/accounts", headers=auth_headers())
            assert resp_list.status_code == 200
            ids = [x["id"] for x in resp_list.json()]
            assert acc_a_id in ids
            assert acc_b_id in ids

    finally:
        if acc_a_id:
            client.delete(f"/api/accounts/{acc_a_id}?delete_session_file=true", headers=auth_headers())
        if acc_b_id:
            client.delete(f"/api/accounts/{acc_b_id}?delete_session_file=true", headers=auth_headers())


def _make_verify_account(sign_in_side_effect=None):
    """构造 verify-code 路由所需的真实账号对象与 Mock Telethon 客户端"""
    from types import SimpleNamespace

    db_acc = AccountRepository.create(
        account_name=f"VerifyAcc_{id(sign_in_side_effect)}",
        phone="+12025550116",
        session_file="verify_999.session",
        api_id=1,
        api_hash="dummy",
    )
    acc = TelegramAccountClient(
        account_id=int(db_acc["id"]),
        account_name=db_acc["account_name"],
        phone="+12025550116",
        session_file="verify_999.session",
        api_id=1,
        api_hash="dummy",
    )
    mock_tg = AsyncMock()
    mock_tg.is_connected = MagicMock(return_value=True)
    mock_tg.session = SimpleNamespace(
        dc_id=4,
        server_address="149.154.167.91",
        port=443,
        auth_key=SimpleNamespace(key=b"\x01" * 256),
        takeout_id=None,
    )
    mock_tg.send_code_request = AsyncMock(return_value=MagicMock(phone_code_hash="internal_hash", next_type=None))
    mock_tg.sign_in = AsyncMock(side_effect=sign_in_side_effect)
    mock_tg.is_user_authorized = AsyncMock(return_value=True)
    mock_tg.get_me = AsyncMock(
        return_value=MagicMock(id=42, phone="12025550116", username=None, first_name="", premium=False)
    )
    return acc, mock_tg


def test_verify_code_wrong_code_keeps_401(monkeypatch):
    """验证码错误必须保留显式 401 (修复前被 except Exception 重包为 400)"""
    from telethon.errors import PhoneCodeInvalidError

    acc, mock_tg = _make_verify_account(sign_in_side_effect=PhoneCodeInvalidError(request=None))
    monkeypatch.setattr(client_manager, "get_account", lambda aid: acc if aid == acc.account_id else None)
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _acc: mock_tg)
    headers = auth_headers()
    send_resp = client.post(f"/api/accounts/{acc.account_id}/send-code", json={}, headers=headers)
    assert send_resp.status_code == 200
    cid = send_resp.json()["challenge_id"]

    resp = client.post(
        f"/api/accounts/{acc.account_id}/verify-code",
        json={"challenge_id": cid, "code": "12345"},
        headers=headers,
    )
    assert resp.status_code == 401, "401 不得被 except Exception 覆写成 400"
    assert resp.json()["error_code"] == "invalid_code"


def test_verify_code_passes_password_unstripped(monkeypatch):
    """2FA 密码原文透传 (首尾空格保留), 只规范化验证码"""
    from telethon.errors import SessionPasswordNeededError

    acc, mock_tg = _make_verify_account(sign_in_side_effect=[SessionPasswordNeededError(request=None), None])
    monkeypatch.setattr(client_manager, "get_account", lambda aid: acc if aid == acc.account_id else None)
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _acc: mock_tg)
    monkeypatch.setattr(client_manager, "recover_startup_degraded", AsyncMock())
    headers = auth_headers()
    send_resp = client.post(f"/api/accounts/{acc.account_id}/send-code", json={}, headers=headers)
    assert send_resp.status_code == 200
    cid = send_resp.json()["challenge_id"]

    resp = client.post(
        f"/api/accounts/{acc.account_id}/verify-code",
        json={"challenge_id": cid, "code": " 1 2-3 45 ", "password": "  padded 2fa  "},
        headers=headers,
    )
    assert resp.status_code == 200
    assert mock_tg.sign_in.await_args_list[0].kwargs["code"] == "12345"
    assert mock_tg.sign_in.await_args_list[1].kwargs["password"] == "  padded 2fa  "


def test_create_account_requires_valid_phone():
    for bad_payload in (
        {"account_name": "NoPhone", "api_id": 123, "api_hash": "abc"},
        {"account_name": "EmptyPhone", "phone": "", "api_id": 123, "api_hash": "abc"},
        {"account_name": "BlankPhone", "phone": "   ", "api_id": 123, "api_hash": "abc"},
        {"account_name": "InvalidPhone", "phone": "abc-xyz", "api_id": 123, "api_hash": "abc"},
    ):
        resp = client.post("/api/accounts", json=bad_payload, headers=auth_headers())
        assert resp.status_code in (400, 422), f"非法手机号载荷应被拒绝: {bad_payload}"


def test_account_send_verify_challenge_binding_and_empty_seed_persistence(monkeypatch):
    # 1) 模拟 .env 首次导入的空手机号默认账号，可通过 send-code + verify-code 首次绑定手机号
    db_acc = AccountRepository.create(
        account_name="SeedEmptyPhone",
        phone="",
        session_file="seed_empty_phone.session",
        api_id=123456,
        api_hash="hash_seed_123",
    )
    acc_id = db_acc["id"]
    acc_client = TelegramAccountClient(
        account_id=acc_id,
        account_name="SeedEmptyPhone",
        phone="",
        session_file="seed_empty_phone.session",
        api_id=123456,
        api_hash="hash_seed_123",
    )
    from types import SimpleNamespace

    mock_tg = AsyncMock()
    mock_tg.is_connected = MagicMock(return_value=True)
    mock_tg.session = SimpleNamespace(
        dc_id=4,
        server_address="149.154.167.91",
        port=443,
        auth_key=SimpleNamespace(key=b"\x01" * 256),
        takeout_id=None,
    )
    mock_tg.send_code_request = AsyncMock(return_value=MagicMock(phone_code_hash="hash_seed_ok"))
    mock_tg.sign_in = AsyncMock()
    mock_tg.is_user_authorized = AsyncMock(return_value=True)
    mock_tg.get_me = AsyncMock(
        return_value=MagicMock(id=777001, phone="12025550117", username="seed_u", first_name="Seed", premium=False)
    )
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _acc: mock_tg)
    client_manager._accounts[acc_id] = acc_client

    headers = auth_headers()
    try:
        # 未先 send-code 直接 verify-code 必须被拒绝
        resp_no_send = client.post(
            f"/api/accounts/{acc_id}/verify-code",
            json={"challenge_id": "missing_challenge", "code": "12345"},
            headers=headers,
        )
        assert resp_no_send.status_code == 400

        # send-code 下发验证码
        resp_send = client.post(
            f"/api/accounts/{acc_id}/send-code",
            json={"phone": "+1 202-555-0117"},
            headers=headers,
        )
        assert resp_send.status_code == 200
        assert resp_send.json()["phone"] == "+12025550117"
        assert "phone_code_hash" not in resp_send.json()
        cid = resp_send.json()["challenge_id"]

        # challenge_id 不匹配必须被拒绝
        resp_bad_hash = client.post(
            f"/api/accounts/{acc_id}/verify-code",
            json={"challenge_id": "wrong_challenge_id", "code": "12345"},
            headers=headers,
        )
        assert resp_bad_hash.status_code == 400

        # 正确 challenge_id 验证成功，且手机号回写 DB
        resp_ok = client.post(
            f"/api/accounts/{acc_id}/verify-code",
            json={"challenge_id": cid, "code": "12345"},
            headers=headers,
        )
        assert resp_ok.status_code == 200
        persisted = AccountRepository.get_by_id(acc_id)
        assert persisted is not None
        assert persisted["phone"] == "+12025550117"

        # 绑定完成后再 send-code 传其他手机号必须拒绝
        resp_other_phone = client.post(
            f"/api/accounts/{acc_id}/send-code",
            json={"phone": "+12025550132"},
            headers=headers,
        )
        assert resp_other_phone.status_code == 400

        # 已授权账号禁止通过 PUT 修改手机号
        resp_put_phone = client.put(
            f"/api/accounts/{acc_id}",
            json={"phone": "+12025550132"},
            headers=headers,
        )
        assert resp_put_phone.status_code == 400
    finally:
        AccountRepository.delete(acc_id)
        client_manager._accounts.pop(acc_id, None)


def test_native_bootstrap_rejects_switching_bound_phone(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "secret_key_for_test")
    monkeypatch.setattr(settings, "BOOTSTRAP_TOKEN", "unit-test-bootstrap-secret-0123456789")
    bootstrap_headers = {"X-Bootstrap-Token": settings.BOOTSTRAP_TOKEN}
    acc = TelegramAccountClient(
        account_id=1,
        account_name="Default",
        phone="+12025550120",
        session_file="def.session",
        api_id=1,
        api_hash="dummy",
        is_default=True,
        user_id=123,
    )
    acc.auth_state = "UNAUTHENTICATED"
    monkeypatch.setattr(client_manager, "_accounts", {1: acc})

    # 未携带 2FA Token 走 bootstrap 恢复通道时，不允许用不同手机号发码或伪造挑战验码
    resp_send = client.post(
        "/api/telegram/native/send-code",
        json={"phone": "+12025550131"},
        headers=bootstrap_headers,
    )
    assert resp_send.status_code == 403
    assert "绑定" in resp_send.json()["detail"]

    resp_verify = client.post(
        "/api/telegram/native/verify-code",
        json={"challenge_id": "forged_challenge", "code": "12345"},
        headers=bootstrap_headers,
    )
    assert resp_verify.status_code == 400


def test_create_bot_sanitizes_default_session_name():
    with (
        patch("telethon.TelegramClient.start", new_callable=AsyncMock),
        patch("telethon.TelegramClient.is_connected", return_value=True),
        patch(
            "telethon.TelegramClient.get_me",
            new_callable=AsyncMock,
            return_value=MagicMock(username="sanitized_bot", id=555),
        ),
    ):
        resp = client.post(
            "/api/bots",
            json={
                "bot_token": "../../../outside:SECRET_PART",
                "bot_name": "Sanitize Bot",
                "api_id": 12345,
                "api_hash": "hash123",
            },
            headers=auth_headers(),
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "/" not in data["session_name"]
        assert ".." not in data["session_name"]
        client.delete(f"/api/bots/{data['id']}", headers=auth_headers())


def test_normalize_phone_maps_00_prefix():
    from app.core.client_manager import normalize_phone_number

    assert normalize_phone_number("001 202-555-0117") == "+12025550117"
    assert normalize_phone_number("12025550106") == "12025550106"
