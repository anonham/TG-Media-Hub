"""Targeted offline lifecycle and bootstrap tests for default Userbot admission.

Covers:
1. FastAPI TestClient(app) lifespan behavior across missing, corrupt real SQLite   unauthorized, and authorized default sessions (mocked Telegram network, real temp files).
2. Enforcement that business HTTP, /metrics, media Range, and /ws/progress stay locked
   until the default Userbot identity is admitted.
3. End-to-end mocked OTP bootstrap login with X-Bootstrap-Token (TG_BOOTSTRAP_TOKEN)   verifying session handover and unlocked business access in open API key mode.
"""

from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _safeio import safe_write_bytes
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient
from telethon.crypto import AuthKey
from telethon.sessions import MemorySession, SQLiteSession
from telethon.tl.types.auth import SentCodeTypeApp

from app.api import auth as auth_module
from app.api.auth import issue_2fa_token, issue_bootstrap_session_token, issue_media_ticket
from app.config import settings
from app.core import client_manager as cm_module
from app.core.client_manager import _handover_path, client_manager
from app.core.connection_manager import connection_manager
from app.db.account_repository import AccountRepository
from app.db.database import db_cursor
from app.main import app
from app.services.telegram_login_service import TelegramLoginError, telegram_login_service

TEST_BOOTSTRAP_TOKEN = "bootstrap_deployer_secret_token_987654321"
TEST_PHONE = "+12025550117"
TEST_USER_ID = 888001


class _FakeTelegramClient:
    """Offline TelegramClient replacement backed by real SQLiteSession or MemorySession."""

    def __init__(
        self,
        session,
        api_id: int,
        api_hash: str,
        *,
        authorized: bool = False,
        me: SimpleNamespace | None = None,
        otp_me: SimpleNamespace | None = None,
        **_kwargs,
    ):
        self.session = SQLiteSession(session) if isinstance(session, str) else session
        self.api_id = api_id
        self.api_hash = api_hash
        self._connected = False
        self._authorized = authorized
        self._me = me
        self._otp_me = otp_me or SimpleNamespace(
            id=TEST_USER_ID,
            phone=TEST_PHONE.lstrip("+"),
            username="boot_userbot",
            first_name="Boot",
            premium=False,
            bot=False,
        )
        self._is_bot = False
        self._bot_id = 990001

    def is_connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False

    async def start(self, bot_token: str | None = None, **_kwargs):
        self._connected = True
        if bot_token:
            self._is_bot = True
            self._authorized = True
            token_id = bot_token.partition(":")[0]
            if token_id.isdigit():
                self._bot_id = int(token_id)
        return self

    async def is_user_authorized(self) -> bool:
        return self._authorized

    async def get_me(self):
        if self._is_bot:
            return SimpleNamespace(id=self._bot_id, username="mock_helper_bot", bot=True, first_name="MockBot")
        if not self._authorized:
            return None
        return self._me or self._otp_me

    async def send_code_request(self, phone: str, **_kwargs):
        return SimpleNamespace(
            phone_code_hash="mock_phone_code_hash_001",
            type=SentCodeTypeApp(length=5),
            next_type=None,
            timeout=60,
        )

    async def sign_in(
        self, phone: str | None = None, code: str | None = None, phone_code_hash: str | None = None, **_kwargs
    ):
        self._authorized = True
        if isinstance(self.session, MemorySession) or getattr(self.session, "auth_key", None) is None:
            self.session.set_dc(4, "149.154.167.91", 443)
            self.session.auth_key = AuthKey(b"\x07" * 256)
        return self._otp_me

    async def log_out(self) -> bool:
        self._authorized = False
        return True


def _reset_accounts_and_bots(
    *,
    phone: str = TEST_PHONE,
    session_file: str = "default_userbot.session",
    user_id: int | None = None,
) -> int:
    with db_cursor(commit=True) as cursor:
        cursor.execute("DELETE FROM tg_accounts")
        cursor.execute("DELETE FROM tg_bots")
    client_manager._accounts.clear()
    client_manager._bots.clear()
    client_manager._pool_init_task = None
    client_manager._shutting_down = False
    client_manager.startup_degraded = False

    row = AccountRepository.create(
        account_name="Default Userbot",
        phone=phone,
        session_file=session_file,
        api_id=123456,
        api_hash="0123456789abcdef0123456789abcdef",
        is_active=1,
        is_default=1,
    )
    acc_id = int(row["id"])
    if user_id is not None:
        AccountRepository.update_status(acc_id, "unauthenticated", user_id=user_id, dc_id=4)
    return acc_id


def _write_valid_sqlite_session(session_base: str, *, with_auth_key: bool) -> str:
    sess = SQLiteSession(session_base)
    try:
        sess.set_dc(4, "149.154.167.91", 443)
        if with_auth_key:
            sess.auth_key = AuthKey(b"\x05" * 256)
        sess.save()
    finally:
        sess.close()
    return f"{session_base}.session"


def _assert_surface_locked(client: TestClient) -> None:
    assert client.get("/api/telegram/status").json()["startup"] is None
    tasks_resp = client.get("/api/tasks")
    assert tasks_resp.status_code == 503
    assert tasks_resp.json()["error_code"] == "telegram_bootstrap_required"

    accounts_resp = client.get("/api/accounts")
    assert accounts_resp.status_code == 503
    assert accounts_resp.json()["error_code"] == "telegram_bootstrap_required"

    assert client.get("/api/pikpak/files/list").status_code == 503
    assert client.post("/api/auth/media-ticket").status_code == 503
    metrics_resp = client.get("/metrics")
    assert metrics_resp.status_code == 503
    assert metrics_resp.json()["error_code"] == "telegram_bootstrap_required"

    old_ticket = issue_media_ticket()
    range_resp = client.get(
        "/api/storage/preview/sample.mp4", params={"ticket": old_ticket}, headers={"Range": "bytes=0-9"}
    )
    assert range_resp.status_code == 503
    assert range_resp.json()["error_code"] == "telegram_bootstrap_required"

    with (
        pytest.raises(WebSocketDisconnect) as ws_exc,
        client.websocket_connect(f"/ws/progress?ticket={old_ticket}"),
    ):
        pytest.fail("连接未验证的默认账号时不应完成 WebSocket 握手")
    assert ws_exc.value.code == 1008


def _assert_surface_unlocked(client: TestClient) -> None:
    status_resp = client.get("/api/auth/status")
    assert status_resp.status_code == 200
    status_data = status_resp.json()
    assert status_data["auth_required"] is False
    assert status_data["restricted"] is False
    assert status_data["binding_mode"] == "admitted"

    tasks_resp = client.get("/api/tasks")
    assert tasks_resp.status_code == 200

    metrics_resp = client.get("/metrics")
    assert metrics_resp.status_code == 200

    range_resp = client.get("/api/storage/preview/sample.mp4", headers={"Range": "bytes=0-9"})
    assert range_resp.status_code == 206
    assert range_resp.headers["content-range"] == "bytes 0-9/100"
    assert range_resp.content == b"0123456789"

    with client.websocket_connect("/ws/progress") as ws:
        ws.send_text("ping")


def _assert_bootstrap_startup_completes(client, release_pool, broadcast):
    status_url = "/api/telegram/status?probe_premium=false"
    initializing = client.get(status_url).json()["startup"]
    assert initializing == {
        "phase": "initializing",
        "scheduler_ready": False,
        "ready_bots": 0,
        "total_bots": 1,
        "unready_bots": ["Bot #1"],
    }

    async def finish_recovery():
        release_pool.set()
        await asyncio.wait_for(client_manager._pool_init_task, 5)

    with client.websocket_connect("/ws/progress") as ws:
        client.portal.call(finish_recovery)
        event = {"type": "system_event", "event": "telegram_startup_changed"}
        broadcast.assert_awaited_once_with(event)
        assert ws.receive_json() == event

    assert client.get(status_url).json()["startup"] == {
        "phase": "ready",
        "scheduler_ready": True,
        "ready_bots": 1,
        "total_bots": 1,
        "unready_bots": [],
    }


@pytest.fixture
def bootstrap_env(monkeypatch):
    """Configure open API key mode, temp sample media, and mock Telegram settings."""
    monkeypatch.setattr(settings, "API_ID", 123456)
    monkeypatch.setattr(settings, "API_HASH", "0123456789abcdef0123456789abcdef")
    monkeypatch.setattr(settings, "BOT_TOKENS", ["12345:mock_bot_token"])
    monkeypatch.setattr(settings, "API_KEY", "")
    monkeypatch.setattr(settings, "SECRET_KEY", "")
    monkeypatch.setattr(settings, "RATE_LIMIT_PER_MINUTE", 0)
    monkeypatch.setattr(settings, "DEFAULT_PHONE", TEST_PHONE)
    monkeypatch.setattr(settings, "BOOTSTRAP_TOKEN", TEST_BOOTSTRAP_TOKEN)
    monkeypatch.setattr(settings, "USERBOT_SESSION_NAME", "default_userbot")

    sample_path = os.path.join(settings.DOWNLOAD_DIR, "sample.mp4")
    safe_write_bytes(sample_path, b"0123456789" * 10)
    return sample_path


def test_restricted_media_ticket_rejects_even_valid_business_credentials(bootstrap_env, monkeypatch):
    _reset_accounts_and_bots()
    monkeypatch.setattr(settings, "API_KEY", "test-console-key")
    monkeypatch.setattr(settings, "SECRET_KEY", "test-signing-key-for-media-ticket-123")
    web_2fa = issue_2fa_token()
    assert web_2fa is not None
    with TestClient(app, client=("127.0.0.1", 50000)) as client:
        response = client.post(
            "/api/auth/media-ticket", headers={"X-API-Key": settings.API_KEY, "X-2FA-Token": web_2fa}
        )
    assert response.status_code == 503
    assert response.json()["error_code"] == "telegram_bootstrap_required"


def test_bootstrap_header_attempts_share_budget_but_cookie_and_public_reads_do_not(bootstrap_env, monkeypatch):
    _reset_accounts_and_bots()
    monkeypatch.setattr(auth_module, "LOGIN_ATTEMPT_LIMIT_PER_MINUTE", 4)
    with TestClient(app, client=("127.0.0.1", 50000)) as client:
        for url in ("/api/auth/status", "/api/telegram/status"):
            response = client.get(url, headers={"X-Bootstrap-Token": "incorrect"})
            assert response.status_code == 200 and not response.json()["bootstrap_authorized"]
        assert (
            client.post("/api/telegram/bootstrap/auth", headers={"X-Bootstrap-Token": "incorrect"}).status_code == 401
        )
        headers = {"X-Bootstrap-Token": TEST_BOOTSTRAP_TOKEN}
        response = client.post("/api/telegram/native/send-code", json={"phone": "+12025550107"}, headers=headers)
        assert response.json()["error_code"] == "phone_mismatch"
        for url in ("/api/auth/status", "/api/telegram/status"):
            assert client.get(url, headers=headers).status_code == 429
            assert client.get(url).status_code == 200
        client.cookies.set(auth_module.BOOTSTRAP_COOKIE_NAME, issue_bootstrap_session_token(), path="/api")
        assert client.get("/api/auth/status").json()["bootstrap_authorized"]
        assert client.get("/api/telegram/status").json()["bootstrap_authorized"]


@pytest.mark.parametrize("route", ["native/send-code", "qr/start"])
def test_phone_mismatch_uses_credential_that_admitted_the_request(bootstrap_env, monkeypatch, route):
    _reset_accounts_and_bots()
    cookie_valid = True

    def validate_cookie(_token):
        return cookie_valid

    async def fail_start(**_kwargs):
        nonlocal cookie_valid
        cookie_valid = False
        raise TelegramLoginError(400, "phone_mismatch", "test mismatch")

    monkeypatch.setattr(auth_module, "validate_bootstrap_session_token", validate_cookie)
    monkeypatch.setattr(telegram_login_service, "start_qr" if route == "qr/start" else "start_otp", fail_start)
    monkeypatch.setattr(auth_module, "BOOTSTRAP_MISMATCH_FREE_ATTEMPTS", 0)
    with TestClient(app, client=("127.0.0.1", 50000)) as client:
        client.cookies.set(auth_module.BOOTSTRAP_COOKIE_NAME, "test-cookie", path="/api")
        response = client.post(f"/api/telegram/{route}", json={"phone": TEST_PHONE})
    assert response.status_code == 429
    assert response.json()["error_code"] == "phone_mismatch_cooldown"


async def test_pending_otp_success_does_not_clear_new_mismatch_cooldown(bootstrap_env, monkeypatch):
    _reset_accounts_and_bots()
    started, release = asyncio.Event(), asyncio.Event()
    monkeypatch.setattr(auth_module, "BOOTSTRAP_MISMATCH_FREE_ATTEMPTS", 0)

    async def start_otp(*, phone, **_kwargs):
        if phone != TEST_PHONE:
            raise TelegramLoginError(400, "phone_mismatch", "test mismatch")
        started.set()
        await release.wait()
        return {"challenge_id": "test-challenge", "expires_at": 60, "message": "test"}

    monkeypatch.setattr(telegram_login_service, "start_otp", start_otp)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
        headers={"X-Bootstrap-Token": TEST_BOOTSTRAP_TOKEN},
    ) as client:
        url = "/api/telegram/native/send-code"
        pending = asyncio.create_task(client.post(url, json={"phone": TEST_PHONE}))
        try:
            await asyncio.wait_for(started.wait(), timeout=2)
            mismatch = await client.post(url, json={"phone": "+12025550107"})
            assert mismatch.json()["error_code"] == "phone_mismatch_cooldown"
        finally:
            release.set()
            sent = await pending
        assert sent.status_code == 200
        blocked = await client.post(url, json={"phone": TEST_PHONE})
        assert blocked.json()["error_code"] == "phone_mismatch_cooldown"


async def test_premium_probe_checks_admission_again_before_returning_status(bootstrap_env, monkeypatch):
    _reset_accounts_and_bots()
    admitted = True
    account = SimpleNamespace(
        is_admitted=lambda: admitted, user_id=TEST_USER_ID, auth_state="AUTHENTICATED", client=SimpleNamespace()
    )
    monkeypatch.setattr(client_manager, "get_default_account_strict", lambda: account)

    async def revoke_during_probe():
        nonlocal admitted
        admitted = False

    monkeypatch.setattr(client_manager, "refresh_premium", revoke_during_probe)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.get("/api/telegram/status")
    assert response.status_code == 200
    data = response.json()
    assert data["restricted"] and data["binding_mode"] == "rebind"
    assert not data["is_connected"] and not data["premium"]
    assert data["startup"] is None


def test_lifespan_missing_default_session_locks_all_surfaces(bootstrap_env, monkeypatch):
    _reset_accounts_and_bots()

    def fake_factory(*_args, **_kwargs):
        pytest.fail("默认 session 缺失时不应建立 Telegram 连接")

    monkeypatch.setattr(cm_module, "TelegramClient", fake_factory)

    with TestClient(app, client=("127.0.0.1", 50000)) as client:
        assert not client_manager.is_default_account_admitted()
        status_resp = client.get("/api/auth/status")
        assert status_resp.status_code == 200
        assert status_resp.json()["restricted"] is True
        assert status_resp.json()["binding_mode"] == "first_bind"
        assert not os.path.exists(os.path.join(settings.DATA_DIR, "default_userbot.session"))
        _assert_surface_locked(client)


def test_lifespan_corrupt_sqlite_session_locks_all_surfaces(bootstrap_env, monkeypatch):
    _reset_accounts_and_bots(user_id=TEST_USER_ID)
    corrupt_path = os.path.join(settings.DATA_DIR, "default_userbot.session")
    safe_write_bytes(corrupt_path, b"corrupt-sqlite-header-not-a-database-0000")

    monkeypatch.setattr(
        cm_module,
        "TelegramClient",
        lambda session, api_id, api_hash, **kw: _FakeTelegramClient(session, api_id, api_hash, authorized=False),
    )

    with TestClient(app, client=("127.0.0.1", 50000)) as client:
        assert not client_manager.is_default_account_admitted()
        strict_default = client_manager.get_default_account_strict()
        assert strict_default is not None
        assert strict_default.client is None
        assert strict_default.auth_state == "UNAUTHENTICATED"
        saved = AccountRepository.get_by_id(strict_default.account_id)
        assert saved is not None
        assert saved["status"] == "unauthenticated"
        with open(corrupt_path, "rb") as session_file:
            assert session_file.read() == b"corrupt-sqlite-header-not-a-database-0000"

        status_resp = client.get("/api/auth/status")
        assert status_resp.status_code == 200
        assert status_resp.json()["restricted"] is True
        assert status_resp.json()["binding_mode"] == "rebind"
        _assert_surface_locked(client)


def test_lifespan_unauthorized_default_session_locks_all_surfaces(bootstrap_env, monkeypatch):
    _reset_accounts_and_bots(user_id=TEST_USER_ID)
    session_base = os.path.join(settings.DATA_DIR, "default_userbot")
    _write_valid_sqlite_session(session_base, with_auth_key=False)

    monkeypatch.setattr(
        cm_module,
        "TelegramClient",
        lambda session, api_id, api_hash, **kw: _FakeTelegramClient(session, api_id, api_hash, authorized=False),
    )

    with TestClient(app, client=("127.0.0.1", 50000)) as client:
        assert not client_manager.is_default_account_admitted()
        tg_status = client.get("/api/telegram/status")
        assert tg_status.status_code == 200
        assert tg_status.json()["restricted"] is True
        assert tg_status.json()["is_connected"] is False
        _assert_surface_locked(client)


def test_lifespan_authorized_default_session_admits_all_surfaces_open_mode(bootstrap_env, monkeypatch):
    _reset_accounts_and_bots(user_id=TEST_USER_ID)
    session_base = os.path.join(settings.DATA_DIR, "default_userbot")
    _write_valid_sqlite_session(session_base, with_auth_key=True)

    authorized_me = SimpleNamespace(
        id=TEST_USER_ID,
        phone="12025550130",
        username="authorized_user",
        first_name="Authorized",
        premium=False,
        bot=False,
    )
    monkeypatch.setattr(
        cm_module,
        "TelegramClient",
        lambda session, api_id, api_hash, **kw: _FakeTelegramClient(
            session, api_id, api_hash, authorized=True, me=authorized_me
        ),
    )

    with TestClient(app, client=("127.0.0.1", 50000)) as client:
        assert client_manager.is_default_account_admitted()
        _assert_surface_unlocked(client)


def test_interrupted_handover_is_restored_before_missing_session_check(bootstrap_env, monkeypatch, tmp_path):
    _reset_accounts_and_bots(user_id=TEST_USER_ID)
    monkeypatch.setattr(settings, "BASE_DIR", str(tmp_path))
    official = str(tmp_path / "default_userbot.session")
    backup = _handover_path(official, ".backup")
    backup.parent.mkdir()
    session_file = _write_valid_sqlite_session(str(tmp_path / "previous"), with_auth_key=True)
    os.replace(session_file, backup)
    pending = _handover_path(official, ".pending")
    pending.write_text("1 " + "a" * 32, encoding="ascii")
    monkeypatch.setattr(
        cm_module,
        "TelegramClient",
        lambda session, api_id, api_hash, **_kw: _FakeTelegramClient(session, api_id, api_hash, authorized=True),
    )

    with TestClient(app, client=("127.0.0.1", 50000)) as client:
        assert client.get("/api/auth/status").json()["restricted"] is False
        assert os.path.isfile(official)
        assert not pending.exists()


def test_phone_mismatch_backs_off_before_starting_telegram_challenge(bootstrap_env, monkeypatch):
    _reset_accounts_and_bots()

    def unexpected_connection(*_args, **_kwargs):
        pytest.fail("错误号码不得发起 Telegram 请求")

    monkeypatch.setattr(cm_module, "TelegramClient", unexpected_connection)
    headers = {"X-Bootstrap-Token": TEST_BOOTSTRAP_TOKEN}
    with TestClient(app, client=("127.0.0.1", 50000)) as client:
        for endpoint in ("/api/telegram/native/send-code", "/api/telegram/qr/start"):
            rejected = client.post(endpoint, json={"phone": "+12025550130"}, headers=headers)
            assert rejected.status_code == 403
            assert rejected.json()["error_code"] == "phone_mismatch"
            assert TEST_PHONE not in rejected.text
        throttled = client.post("/api/telegram/native/send-code", json={"phone": "+12025550130"}, headers=headers)
        assert throttled.status_code == 429
        assert int(throttled.headers["Retry-After"]) > 0
        correct_during_cooldown = client.post(
            "/api/telegram/native/send-code", json={"phone": TEST_PHONE}, headers=headers
        )
        assert correct_during_cooldown.status_code == 429


def test_invalid_deployment_phone_leaves_lifespan_restricted(bootstrap_env, monkeypatch):
    _reset_accounts_and_bots()
    monkeypatch.setattr(settings, "DEFAULT_PHONE", "invalid-deployment-phone")
    monkeypatch.setattr(cm_module, "TelegramClient", lambda *_a, **_kw: pytest.fail("不应发起 Telegram 连接"))
    with TestClient(app, client=("127.0.0.1", 50000)) as client:
        status_data = client.get("/api/auth/status").json()
        assert status_data["restricted"] is True
        assert status_data["default_phone_configured"] is True
        assert status_data["default_phone_valid"] is False
        response = client.post(
            "/api/telegram/native/send-code",
            json={"phone": TEST_PHONE},
            headers={"X-Bootstrap-Token": TEST_BOOTSTRAP_TOKEN},
        )
        assert response.status_code == 400
        assert response.json()["error_code"] == "invalid_config"


@pytest.mark.parametrize("configured_phone", ["", TEST_PHONE])
def test_e2e_mocked_otp_bootstrap_handover_unlocks_open_mode_business_access(
    bootstrap_env, monkeypatch, configured_phone
):
    monkeypatch.setattr(settings, "DEFAULT_PHONE", configured_phone)
    acc_id = _reset_accounts_and_bots(phone="legacy-unverified-phone")

    monkeypatch.setattr(
        cm_module,
        "TelegramClient",
        lambda session, api_id, api_hash, **kw: _FakeTelegramClient(
            session, api_id, api_hash, authorized=isinstance(session, str)
        ),
    )
    release_pool = asyncio.Event()
    connect_remaining = client_manager._connect_remaining_pool

    async def connect_after_release(**kwargs):
        await release_pool.wait()
        await connect_remaining(**kwargs)

    broadcast = AsyncMock(wraps=connection_manager.broadcast)
    monkeypatch.setattr(connection_manager, "broadcast", broadcast)
    monkeypatch.setattr(client_manager, "_connect_remaining_pool", connect_after_release)
    premium_probe = AsyncMock(return_value=False)
    monkeypatch.setattr(client_manager, "refresh_premium", premium_probe)

    with TestClient(app, client=("127.0.0.1", 50000)) as client:
        _assert_surface_locked(client)

        # Native OTP is blocked without TG_BOOTSTRAP_TOKEN header or bootstrap cookie.
        no_token_resp = client.post("/api/telegram/native/send-code", json={"phone": TEST_PHONE})
        assert no_token_resp.status_code == 401
        assert no_token_resp.json()["error_code"] == "bootstrap_unauthorized"

        headers = {"X-Bootstrap-Token": TEST_BOOTSTRAP_TOKEN}
        auth_resp = client.post("/api/telegram/bootstrap/auth", headers=headers)
        assert auth_resp.status_code == 200
        assert "httponly" in auth_resp.headers["set-cookie"].lower()
        send_resp = client.post("/api/telegram/native/send-code", json={"phone": TEST_PHONE})
        assert send_resp.status_code == 200
        challenge_id = send_resp.json()["challenge_id"]
        assert challenge_id

        verify_resp = client.post(
            "/api/telegram/native/verify-code",
            json={"challenge_id": challenge_id, "code": "12345"},
        )
        assert verify_resp.status_code == 200
        verify_data = verify_resp.json()
        assert verify_data["authenticated"] is True
        assert verify_data["state"] == "authenticated"
        assert verify_data["token"] is None

        # Handover persisted SQLite session file and admitted the default account.
        official_session = os.path.join(settings.DATA_DIR, "default_userbot.session")
        assert os.path.isfile(official_session)
        saved_acc = AccountRepository.get_by_id(acc_id)
        assert saved_acc is not None
        assert saved_acc["user_id"] == TEST_USER_ID
        assert saved_acc["phone"] == TEST_PHONE
        assert saved_acc["status"] == "authenticated"
        assert client_manager.is_default_account_admitted()
        _assert_bootstrap_startup_completes(client, release_pool, broadcast)
        premium_probe.assert_not_awaited()

        # Business HTTP, metrics, media Range, and WebSocket are now unlocked in open API key mode.
        _assert_surface_unlocked(client)

        # Bootstrap credentials must be rejected once normal admitted mode is active.
        post_admit_bootstrap = client.post("/api/telegram/bootstrap/auth", headers=headers)
        assert post_admit_bootstrap.status_code == 403
        assert post_admit_bootstrap.json()["error_code"] == "bootstrap_not_active"

    client_manager._accounts.clear()
    with TestClient(app, client=("127.0.0.1", 50000)) as restarted:
        _assert_surface_unlocked(restarted)
