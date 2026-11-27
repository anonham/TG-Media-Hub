"""Targeted regressions for shared Telegram login challenges (OTP / Resend / QR / Identity / Handover / Bounds)."""

from __future__ import annotations

import asyncio
import inspect
import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient
from telethon.errors import (
    FloodWaitError,
    PasswordHashInvalidError,
    SessionPasswordNeededError,
)
from telethon.tl.types.auth import (
    CodeTypeSms,
    SentCodeTypeApp,
    SentCodeTypeSms,
    SentCodeTypeSmsPhrase,
)

from app.api.auth import (
    _login_attempt_timestamps,
    _sensitive_action_timestamps,
    issue_2fa_token,
    validate_2fa_token,
)
from app.config import settings
from app.core.client_manager import (
    TelegramAccountClient,
    _recover_session_handover_sync,
    _stage_session_file_sync,
    _swap_with_backup_sync,
    client_manager,
)
from app.db.account_repository import AccountRepository, SystemSettingsRepository
from app.db.database import db_cursor
from app.main import app
from app.services.telegram_login_service import MAX_ACTIVE_CHALLENGES, TelegramLoginError, telegram_login_service

BOOTSTRAP_TOKEN = "unit-test-bootstrap-secret-0123456789"
BOOTSTRAP_HEADERS = {"X-Bootstrap-Token": BOOTSTRAP_TOKEN}
client = TestClient(app, headers=BOOTSTRAP_HEADERS, client=("127.0.0.1", 50000))


def _dummy_auth_key(seed: bytes = b"\xab" * 256) -> SimpleNamespace:
    return SimpleNamespace(key=seed)


def _seed_db_account(
    *,
    account_name: str = "Default",
    phone: str = "+12025550117",
    session_file: str = "def1.session",
    is_active: bool = True,
    is_default: bool = True,
    user_id: int | None = None,
    auth_state: str = "UNAUTHENTICATED",
    dc_id: int = 0,
    is_premium: bool = False,
) -> TelegramAccountClient:
    row = AccountRepository.create(
        account_name=account_name,
        phone=phone,
        session_file=session_file,
        api_id=1,
        api_hash="dummy",
        is_active=1 if is_active else 0,
        is_default=1 if is_default else 0,
    )
    acc_id = int(row["id"])
    if user_id is not None or auth_state != "UNAUTHENTICATED" or dc_id or is_premium:
        AccountRepository.update_status(
            acc_id,
            "authenticated" if auth_state == "AUTHENTICATED" else "unauthenticated",
            user_id=user_id,
            dc_id=dc_id if dc_id else None,
        )
        if is_premium:
            AccountRepository.update_premium(acc_id, True)
    acc = client_manager.get_account(acc_id)
    if acc is None:
        acc = TelegramAccountClient(
            account_id=acc_id,
            account_name=account_name,
            phone=phone,
            session_file=session_file,
            api_id=1,
            api_hash="dummy",
            is_active=is_active,
            is_default=is_default,
            dc_id=dc_id,
            user_id=user_id,
        )
    acc.is_active = is_active
    acc.is_default = is_default
    acc.user_id = user_id
    acc.auth_state = auth_state
    acc.dc_id = dc_id
    acc.is_premium = is_premium
    client_manager._accounts[acc_id] = acc
    return acc


def _make_mock_client(
    *,
    send_results=None,
    resend_results=None,
    sign_in_side_effect=None,
    me=None,
    qr_url="tg://login?token=mock_qr_token_xyz",
    qr_wait_coro=None,
    authorized=True,
    connected=True,
    auth_key_bytes: bytes | None = b"\xcd" * 256,
):
    tg = AsyncMock()
    tg.is_connected = MagicMock(return_value=connected)
    tg.connect = AsyncMock()
    tg.disconnect = AsyncMock()
    tg.log_out = AsyncMock()
    tg.is_user_authorized = AsyncMock(return_value=authorized)
    tg.session = SimpleNamespace(
        dc_id=4,
        server_address="149.154.167.91",
        port=443,
        auth_key=_dummy_auth_key(auth_key_bytes) if auth_key_bytes is not None else None,
        takeout_id=None,
        close=MagicMock(),
    )
    tg._phone_code_hash = {}

    if send_results is not None:
        tg.send_code_request = AsyncMock(side_effect=send_results)
    else:
        tg.send_code_request = AsyncMock(
            return_value=SimpleNamespace(
                phone_code_hash="internal_secret_hash_1",
                type=SentCodeTypeApp(length=5),
                next_type=None,
                timeout=None,
            )
        )

    if resend_results is not None:
        tg.resend_code_request = AsyncMock(side_effect=resend_results)
    else:
        tg.resend_code_request = AsyncMock()

    tg.sign_in = AsyncMock(side_effect=sign_in_side_effect)
    if inspect.isfunction(me):
        tg.get_me = AsyncMock(side_effect=me)
    else:
        tg.get_me = AsyncMock(return_value=me)

    qr_obj = MagicMock()
    qr_obj.url = qr_url
    qr_obj.expires = datetime.now(UTC) + timedelta(seconds=30)
    if qr_wait_coro is not None:
        qr_obj.wait = AsyncMock(side_effect=qr_wait_coro)
    else:
        qr_obj.wait = AsyncMock(return_value=me)
    tg.qr_login = AsyncMock(return_value=qr_obj)
    return tg, qr_obj


def _mock_replacement_rpc(monkeypatch, me, connect):
    monkeypatch.setattr("telethon.TelegramClient.connect", AsyncMock(side_effect=connect))
    monkeypatch.setattr("telethon.TelegramClient.is_user_authorized", AsyncMock(return_value=True))
    monkeypatch.setattr("telethon.TelegramClient.get_me", AsyncMock(return_value=me))


@pytest.fixture(autouse=True)
def _clean_login_state(request, monkeypatch):
    monkeypatch.setattr(settings, "BOOTSTRAP_TOKEN", BOOTSTRAP_TOKEN)
    if request.node.originalname in {
        "test_secondary_account_ignores_irrelevant_default_phone",
        "test_challenge_binding_expired_stale_cross_account_and_cross_initiator",
        "test_qr_login_with_2fa_password_continuation_and_secondary_no_console_token",
        "test_delete_retains_committed_session_for_reuse",
    }:
        monkeypatch.setattr(client_manager, "is_default_account_admitted", lambda: True)
    _sensitive_action_timestamps.clear()
    _login_attempt_timestamps.clear()
    telegram_login_service.reset_for_tests()
    client_manager._accounts.clear()
    client.cookies.clear()
    yield
    telegram_login_service.reset_for_tests()
    _sensitive_action_timestamps.clear()
    _login_attempt_timestamps.clear()
    client_manager._accounts.clear()
    client.cookies.clear()


def test_first_binding_requires_explicit_phone_and_prefers_deployment_configuration(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "test_api_key_123456")
    monkeypatch.setattr(settings, "DEFAULT_PHONE", "")
    acc = _seed_db_account(account_name="Default", phone="", session_file="def1.session")

    factory_called = 0

    def _fail_if_called(_acc):
        nonlocal factory_called
        factory_called += 1
        raise AssertionError("Must fail closed before creating client or issuing RPC/QR")

    monkeypatch.setattr(client_manager, "create_isolated_login_client", _fail_if_called)

    # 首绑无论是否配置部署号码，都必须显式输入目标。
    r_missing_otp = client.post(
        "/api/telegram/native/send-code",
        json={},
        headers={"X-API-Key": "test_api_key_123456"},
    )
    assert r_missing_otp.status_code == 400
    assert r_missing_otp.json()["error_code"] == "target_unavailable"

    r_missing_qr = client.post(
        "/api/telegram/qr/start",
        json={},
        headers={"X-API-Key": "test_api_key_123456"},
    )
    assert r_missing_qr.status_code == 400
    assert factory_called == 0

    # 2. DEFAULT_PHONE configured, but omitting phone on first binding OTP or QR fails closed
    monkeypatch.setattr(settings, "DEFAULT_PHONE", "+1 202-555-0117")
    r_omit_otp = client.post(
        "/api/telegram/native/send-code",
        json={},
        headers={"X-API-Key": "test_api_key_123456"},
    )
    assert r_omit_otp.status_code == 400
    assert r_omit_otp.json()["error_code"] == "target_unavailable"

    r_omit_qr = client.post(
        "/api/telegram/qr/start",
        headers={"X-API-Key": "test_api_key_123456"},
    )
    assert r_omit_qr.status_code == 400
    assert r_omit_qr.json()["error_code"] == "target_unavailable"

    # 3. Wrong phone rejected before RPC/QR
    _sensitive_action_timestamps.clear()
    r_wrong_otp = client.post(
        "/api/telegram/native/send-code",
        json={"phone": "+12025550130"},
        headers={"X-API-Key": "test_api_key_123456"},
    )
    assert r_wrong_otp.status_code == 403
    assert r_wrong_otp.json()["error_code"] == "phone_mismatch"
    r_wrong_qr = client.post(
        "/api/telegram/qr/start",
        json={"phone": "+12025550130"},
        headers={"X-API-Key": "test_api_key_123456"},
    )
    assert r_wrong_qr.status_code == 403
    assert r_wrong_qr.json()["error_code"] == "phone_mismatch"
    assert factory_called == 0

    # 4. Equivalent normalized phone (001...) passes both OTP and QR
    mock_tg, _ = _make_mock_client()
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: mock_tg)
    r_norm_ok = client.post(
        "/api/telegram/native/send-code",
        json={"phone": "001 202-555-0117"},
        headers={"X-API-Key": "test_api_key_123456"},
    )
    assert r_norm_ok.status_code == 200
    assert r_norm_ok.headers.get("cache-control", "").startswith("no-store")

    # 5. 部署号码覆盖未核验的旧预填值。
    acc.phone = "+12025550114"
    AccountRepository.update(acc.account_id, phone="+12025550114")
    monkeypatch.setattr(settings, "DEFAULT_PHONE", "+12025550132")
    r_persisted_ok = client.post(
        "/api/telegram/qr/start",
        json={"phone": "+12025550132"},
        headers={"X-API-Key": "test_api_key_123456"},
    )
    assert r_persisted_ok.status_code == 200
    mock_tg.qr_login.assert_awaited_once_with()

    # 新挑战拒绝过期预填号码，连续不符触发冷却。
    r_persisted_mismatch = client.post(
        "/api/telegram/qr/start",
        json={"phone": "+12025550114"},
        headers={"X-API-Key": "test_api_key_123456"},
    )
    assert r_persisted_mismatch.status_code == 403
    assert r_persisted_mismatch.json()["error_code"] == "phone_mismatch"


def test_secondary_account_ignores_irrelevant_default_phone(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "k_sec_ignore_cfg")
    monkeypatch.setattr(settings, "DEFAULT_PHONE", "not-a-valid-phone!!!")
    acc_sec = _seed_db_account(
        account_name="Sec",
        phone="+12025550119",
        session_file="sec_ign.session",
        is_active=True,
        is_default=False,
    )
    tfa = issue_2fa_token()
    mock_tg, _ = _make_mock_client()
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: mock_tg)

    r = client.post(
        f"/api/accounts/{acc_sec.account_id}/qr/start",
        headers={"X-API-Key": "k_sec_ignore_cfg", "X-2FA-Token": tfa},
    )
    assert r.status_code == 200


def test_native_requires_strict_active_default_even_with_other_live_account(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "")
    monkeypatch.setattr(settings, "DEFAULT_PHONE", "+12025550117")

    _seed_db_account(
        account_name="InactiveDefault",
        phone="+12025550117",
        session_file="def_inact.session",
        is_active=False,
        is_default=True,
    )
    _seed_db_account(
        account_name="OtherLive",
        phone="+12025550119",
        session_file="other.session",
        is_active=True,
        is_default=False,
        auth_state="AUTHENTICATED",
    )

    r_otp = client.post("/api/telegram/native/send-code", json={"phone": "+12025550117"})
    assert r_otp.status_code == 400
    assert r_otp.json()["error_code"] == "target_unavailable"

    r_qr = client.post("/api/telegram/qr/start", json={"phone": "+12025550117"})
    assert r_qr.status_code == 400
    assert r_qr.json()["error_code"] == "target_unavailable"


@pytest.mark.asyncio
async def test_first_bind_changes_phone_then_refreshes_expired_qr(monkeypatch, caplog):
    import httpx

    monkeypatch.setattr(settings, "DEFAULT_PHONE", "")
    monkeypatch.setattr(settings, "API_KEY", "")
    monkeypatch.setattr(client_manager, "schedule_startup_recovery", MagicMock())
    caplog.set_level("INFO", logger="app.services.telegram_login_service")
    old_phone = "+12025550117"
    new_phone = "+12025550119"
    acc = _seed_db_account(phone=old_phone)
    otp, _ = _make_mock_client()
    expired, expired_qr = _make_mock_client(qr_wait_coro=TimeoutError())
    old_closed = asyncio.Event()
    scan = asyncio.Event()

    async def wait_for_scan(**_kwargs):
        await scan.wait()

    correct, _ = _make_mock_client(me=SimpleNamespace(id=9002, phone=new_phone.lstrip("+")), qr_wait_coro=wait_for_scan)
    expired.disconnect.side_effect = old_closed.set
    clients = iter([otp, expired, correct])
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _acc: next(clients))

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver", headers=BOOTSTRAP_HEADERS
    ) as browser:
        assert (await browser.post("/api/telegram/native/send-code", json={"phone": old_phone})).status_code == 200
        otp.send_code_request.assert_awaited_once_with(old_phone)

        started = await browser.post("/api/telegram/qr/start", json={"phone": new_phone})
        assert started.status_code == 200
        old_id = started.json()["challenge_id"]
        otp.disconnect.assert_awaited_once()
        await asyncio.wait_for(old_closed.wait(), 2)

        refresh = {"refresh_challenge_id": old_id}
        refreshed = await browser.post("/api/telegram/qr/start", json=refresh)
        assert refreshed.status_code == 200
        assert refreshed.json()["target_phone_hint"] == f"•••• {new_phone[-4:]}"
        refreshed_id = refreshed.json()["challenge_id"]
        assert refreshed_id != old_id
        assert (await browser.get("/api/telegram/qr/status", params={"challenge_id": old_id})).json()[
            "state"
        ] == "expired"
        assert (await browser.post("/api/telegram/qr/start", json=refresh)).json()["error_code"] == "challenge_stale"

        assert (await browser.get("/api/accounts")).status_code == 503
        watcher = telegram_login_service._challenges[refreshed_id]._watch_task
        scan.set()
        await asyncio.wait_for(watcher, 2)
        claim = await browser.get("/api/telegram/qr/status", params={"challenge_id": refreshed_id})
        assert claim.json()["authenticated"] is True
        saved = AccountRepository.get_by_id(acc.account_id)
        assert saved["phone"] == new_phone
        assert saved["user_id"] == 9002
        assert await asyncio.to_thread(os.path.isfile, os.path.join(settings.DATA_DIR, acc.session_file))
        assert (await browser.get("/api/accounts")).status_code == 200

        first_event = telegram_login_service._challenges[old_id].log_id
        assert f"previous_event_id={first_event}" in caplog.text
        assert all(value not in caplog.text for value in (old_id, refreshed_id, old_phone, new_phone, expired_qr.url))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scan_phone", "error_code"),
    [("12025550117", "identity_phone_mismatch"), (None, "identity_phone_unavailable")],
)
async def test_qr_refresh_preserves_target_and_rejects_unverified_identity(monkeypatch, caplog, scan_phone, error_code):
    import httpx

    monkeypatch.setattr(settings, "DEFAULT_PHONE", "")
    monkeypatch.setattr(settings, "API_KEY", "")
    caplog.set_level("INFO", logger="app.services.telegram_login_service")
    acc = _seed_db_account()

    async def wait_without_scan(**_kwargs):
        await asyncio.Event().wait()

    pending, _ = _make_mock_client(qr_wait_coro=wait_without_scan)
    unavailable, _ = _make_mock_client()
    unavailable.qr_login.side_effect = ConnectionError()
    wrong, _ = _make_mock_client(me=SimpleNamespace(id=9001, phone=scan_phone))
    wrong_closed = asyncio.Event()
    wrong.disconnect.side_effect = wrong_closed.set
    clients = iter([pending, unavailable, wrong])
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _acc: next(clients))

    transport = httpx.ASGITransport(app=app)
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://testserver", headers=BOOTSTRAP_HEADERS) as browser,
        httpx.AsyncClient(transport=transport, base_url="http://testserver", headers=BOOTSTRAP_HEADERS) as other,
    ):
        started = await browser.post("/api/telegram/qr/start", json={"phone": "+12025550119"})
        old_id = started.json()["challenge_id"]
        refresh = {"refresh_challenge_id": old_id}
        assert (await other.post("/api/telegram/qr/start", json=refresh)).status_code == 403
        override = await browser.post("/api/telegram/qr/start", json={**refresh, "phone": "+12025550117"})
        assert override.status_code == 400

        unavailable_response = await browser.post("/api/telegram/qr/start", json=refresh)
        assert unavailable_response.json()["error_code"] == "network_failure"
        old_status = await browser.get("/api/telegram/qr/status", params={"challenge_id": old_id})
        assert old_status.json()["state"] == "qr_pending"

        replacement = await browser.post("/api/telegram/qr/start", json=refresh)
        assert replacement.status_code == 200
        assert replacement.json()["target_phone_hint"] == "•••• 0119"
        pending.disconnect.assert_awaited_once()
        await asyncio.wait_for(wrong_closed.wait(), 2)
        old_status = await browser.get("/api/telegram/qr/status", params={"challenge_id": old_id})
        assert old_status.json()["state"] == "cancelled"

        failed = await browser.get(
            "/api/telegram/qr/status", params={"challenge_id": replacement.json()["challenge_id"]}
        )
        assert failed.json()["error_code"] == error_code
        assert failed.json()["authenticated"] is False
        assert AccountRepository.get_by_id(acc.account_id)["user_id"] is None
        assert (await browser.get("/api/accounts")).status_code == 503
        assert f"reason={error_code}" in caplog.text


@pytest.mark.asyncio
async def test_native_qr_bootstrap_end_to_end_with_api_key_and_cross_browser_isolation(monkeypatch):
    """Full HTTP bootstrap flow with API_KEY and no initial 2FA token: same browser claims token once."""
    import httpx

    monkeypatch.setattr(settings, "API_KEY", "k_qr_real_bootstrap_key_1234")
    acc = _seed_db_account(
        account_name="BootstrapDefault",
        phone="+12025550117",
        session_file="qr_boot.session",
        is_active=True,
        is_default=True,
        user_id=None,
    )
    other = _seed_db_account(
        account_name="SecondaryOther",
        phone="+12025550119",
        session_file="qr_other.session",
        is_active=True,
        is_default=False,
    )

    me_ok = SimpleNamespace(id=9001, phone="12025550117", username="boot_owner", first_name="Owner", premium=True)
    scan_release = asyncio.Event()

    async def delayed_qr_scan(**_kwargs):
        await scan_release.wait()
        return me_ok

    mock_tg, _ = _make_mock_client(me=me_ok, qr_wait_coro=delayed_qr_scan)
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: mock_tg)

    shared_headers = {
        **BOOTSTRAP_HEADERS,
        "X-API-Key": "k_qr_real_bootstrap_key_1234",
        "User-Agent": "SameBrowserUA/1.0",
    }
    transport = httpx.ASGITransport(app=app)
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://testserver") as browser_a,
        httpx.AsyncClient(transport=transport, base_url="http://testserver") as browser_b,
    ):
        r_start = await browser_a.post("/api/telegram/qr/start", json={"phone": "+12025550117"}, headers=shared_headers)
        assert r_start.status_code == 200
        cid = r_start.json()["challenge_id"]
        assert "tg_login_ctx" in r_start.cookies
        mock_tg.qr_login.assert_awaited_once_with()

        # Pending poll on same browser works
        r_pend = await browser_a.get(f"/api/telegram/qr/status?challenge_id={cid}", headers=shared_headers)
        assert r_pend.status_code == 200
        assert r_pend.json()["state"] == "qr_pending"
        assert acc.auth_state == "UNAUTHENTICATED"

        # Trigger Telegram scan completion in background and let watcher advance to qr_ready in memory
        scan_release.set()
        for _ in range(30):
            await asyncio.sleep(0.01)
            ch_obj = telegram_login_service._challenges.get(cid)
            if ch_obj is not None and ch_obj.state == "qr_ready":
                break
        assert telegram_login_service._challenges[cid].state == "qr_ready"
        # Watcher must NOT mark account authenticated or persist before matching browser poll
        assert acc.auth_state == "UNAUTHENTICATED"

        # Impostor browser with identical API key, IP, User-Agent CANNOT claim (even after qr_ready!)
        r_b_poll = await browser_b.get(f"/api/telegram/qr/status?challenge_id={cid}", headers=shared_headers)
        assert r_b_poll.status_code == 403
        assert r_b_poll.json()["error_code"] == "challenge_invalid"
        assert acc.auth_state == "UNAUTHENTICATED"

        # Browser A polls and receives synchronized persistence + single-use 2FA token
        r_claim = await browser_a.get(f"/api/telegram/qr/status?challenge_id={cid}", headers=shared_headers)
        assert r_claim.status_code == 200
        claimed = r_claim.json()
        assert claimed["state"] == "authenticated"
        assert claimed["authenticated"] is True
        assert validate_2fa_token(claimed["token"])
        assert acc.auth_state == "AUTHENTICATED"
        assert acc.user_id == 9001
        assert acc.is_premium is True
        assert other.auth_state == "UNAUTHENTICATED"

        # Replay without 2FA token rejected by verify_2fa_session_or_bootstrap, and replay with 2FA token rejected as invalid/stale
        r_replay_no_2fa = await browser_a.get(f"/api/telegram/qr/status?challenge_id={cid}", headers=shared_headers)
        assert r_replay_no_2fa.status_code == 403
        r_replay_with_2fa = await browser_a.get(
            f"/api/telegram/qr/status?challenge_id={cid}",
            headers={**shared_headers, "X-2FA-Token": claimed["token"]},
        )
        assert r_replay_with_2fa.status_code in (400, 403)


def test_otp_resend_guards_hash_rotation_and_leading_zeros(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "k_resend_test")
    _seed_db_account(account_name="Default", phone="+12025550117", session_file="res.session")

    # Step 1: No next_type -> resend rejected with ZERO RPCs
    mock_no_next, _ = _make_mock_client(
        send_results=[
            SimpleNamespace(
                phone_code_hash="hash_no_next",
                type=SentCodeTypeApp(length=5),
                next_type=None,
                timeout=None,
            )
        ]
    )
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: mock_no_next)
    r1 = client.post(
        "/api/telegram/native/send-code",
        json={"phone": "+12025550117"},
        headers={"X-API-Key": "k_resend_test"},
    )
    assert r1.status_code == 200
    d1 = r1.json()
    assert d1["delivery_channel"] == "app"
    assert d1["code_length"] == 5
    assert d1["can_resend"] is False
    assert "phone_code_hash" not in d1

    r_resend_unavail = client.post(
        "/api/telegram/native/resend-code",
        json={"challenge_id": d1["challenge_id"]},
        headers={"X-API-Key": "k_resend_test"},
    )
    assert r_resend_unavail.status_code == 400
    assert r_resend_unavail.json()["error_code"] == "next_channel_unavailable"
    mock_no_next.resend_code_request.assert_not_awaited()

    # Step 2: next_type=CodeTypeSms, timeout=30 -> cooldown honored (0 RPC), then rotated hash used
    me_ok = SimpleNamespace(id=5001, phone="12025550117", username="leadzero", first_name="LZ", premium=False)
    mock_rot, _ = _make_mock_client(
        send_results=[
            SimpleNamespace(
                phone_code_hash="hash_initial_secret",
                type=SentCodeTypeApp(length=5),
                next_type=CodeTypeSms(),
                timeout=30,
            )
        ],
        resend_results=[
            SimpleNamespace(
                phone_code_hash="hash_rotated_secret",
                type=SentCodeTypeSms(length=5),
                next_type=None,
                timeout=None,
            )
        ],
        me=me_ok,
    )
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: mock_rot)

    r2 = client.post(
        "/api/telegram/native/send-code",
        json={"phone": "+12025550117"},
        headers={"X-API-Key": "k_resend_test"},
    )
    assert r2.status_code == 200
    cid = r2.json()["challenge_id"]
    assert r2.json()["can_resend"] is True
    assert r2.json()["next_delivery_channel"] == "sms"

    r_early = client.post(
        "/api/telegram/native/resend-code",
        json={"challenge_id": cid},
        headers={"X-API-Key": "k_resend_test"},
    )
    assert r_early.status_code == 429
    assert r_early.json()["error_code"] == "resend_cooldown"
    mock_rot.resend_code_request.assert_not_awaited()

    telegram_login_service._challenges[cid].resend_available_monotonic = 0.0
    r_resend = client.post(
        "/api/telegram/native/resend-code",
        json={"challenge_id": cid},
        headers={"X-API-Key": "k_resend_test"},
    )
    assert r_resend.status_code == 200
    assert r_resend.json()["delivery_channel"] == "sms"
    assert r_resend.json()["can_resend"] is False
    assert "phone_code_hash" not in r_resend.json()
    mock_rot.resend_code_request.assert_awaited_once_with("+12025550117", "hash_initial_secret")

    # Verify leading zeros preserved and rotated hash used
    r_ver = client.post(
        "/api/telegram/native/verify-code",
        json={"challenge_id": cid, "code": "00421"},
        headers={"X-API-Key": "k_resend_test"},
    )
    assert r_ver.status_code == 200
    assert r_ver.json()["authenticated"] is True
    assert validate_2fa_token(r_ver.json()["token"])
    mock_rot.sign_in.assert_awaited_once_with(
        phone="+12025550117",
        code="00421",
        phone_code_hash="hash_rotated_secret",
    )


def test_cross_browser_bootstrap_isolation_http_layer(monkeypatch):
    """Same API_KEY, IP, and User-Agent on two browsers cannot hijack OTP/resend/verify/cancel."""
    monkeypatch.setattr(settings, "API_KEY", "k_boot_iso_key_9999")
    _seed_db_account(account_name="Default", phone="+12025550117", session_file="iso.session")
    mock_tg, _ = _make_mock_client(
        send_results=[
            SimpleNamespace(
                phone_code_hash="h_iso",
                type=SentCodeTypeApp(length=5),
                next_type=CodeTypeSms(),
                timeout=0,
            )
        ]
    )
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: mock_tg)

    shared = {**BOOTSTRAP_HEADERS, "X-API-Key": "k_boot_iso_key_9999", "User-Agent": "SharedAgent/1.0"}
    browser_a = TestClient(app, client=("127.0.0.1", 50000))
    browser_b = TestClient(app, client=("127.0.0.1", 50000))

    r_start = browser_a.post("/api/telegram/native/send-code", json={"phone": "+12025550117"}, headers=shared)
    assert r_start.status_code == 200
    cid = r_start.json()["challenge_id"]

    for method_call in (
        lambda: browser_b.post("/api/telegram/native/resend-code", json={"challenge_id": cid}, headers=shared),
        lambda: browser_b.post(
            "/api/telegram/native/verify-code", json={"challenge_id": cid, "code": "12345"}, headers=shared
        ),
        lambda: browser_b.post(
            "/api/telegram/native/password", json={"challenge_id": cid, "password": "secret"}, headers=shared
        ),
        lambda: browser_b.post("/api/telegram/challenge/cancel", json={"challenge_id": cid}, headers=shared),
    ):
        resp = method_call()
        assert resp.status_code == 403
        assert resp.json()["error_code"] == "challenge_invalid"

    # Browser A still can cancel its own challenge cleanly
    r_cancel_a = browser_a.post("/api/telegram/challenge/cancel", json={"challenge_id": cid}, headers=shared)
    assert r_cancel_a.status_code == 200
    mock_tg.disconnect.assert_awaited()


def test_challenge_binding_expired_stale_cross_account_and_cross_initiator(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "k_bind_test")
    tfa1 = issue_2fa_token()
    tfa2 = issue_2fa_token()

    acc1 = _seed_db_account(account_name="Acc1", phone="+12025550117", session_file="a1.session", is_default=True)
    acc2 = _seed_db_account(account_name="Acc2", phone="+12025550119", session_file="a2.session", is_default=False)

    mock_tg, _ = _make_mock_client()
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: mock_tg)

    h1 = {"X-API-Key": "k_bind_test", "X-2FA-Token": tfa1}
    h2 = {"X-API-Key": "k_bind_test", "X-2FA-Token": tfa2}

    r_start = client.post(f"/api/accounts/{acc1.account_id}/send-code", json={}, headers=h1)
    assert r_start.status_code == 200
    cid1 = r_start.json()["challenge_id"]

    r_cross_acc = client.post(
        f"/api/accounts/{acc2.account_id}/verify-code", json={"challenge_id": cid1, "code": "12345"}, headers=h1
    )
    assert r_cross_acc.status_code == 400
    assert r_cross_acc.json()["error_code"] == "challenge_invalid"

    r_cross_flow = client.post(
        "/api/telegram/native/verify-code", json={"challenge_id": cid1, "code": "12345"}, headers=h1
    )
    assert r_cross_flow.status_code == 403
    assert r_cross_flow.json()["error_code"] == "challenge_invalid"

    r_cross_init = client.post(
        f"/api/accounts/{acc1.account_id}/verify-code", json={"challenge_id": cid1, "code": "12345"}, headers=h2
    )
    assert r_cross_init.status_code == 403
    assert r_cross_init.json()["error_code"] == "challenge_invalid"

    r_start_2 = client.post(f"/api/accounts/{acc1.account_id}/send-code", json={}, headers=h1)
    assert r_start_2.status_code == 200
    cid2 = r_start_2.json()["challenge_id"]
    r_stale = client.post(
        f"/api/accounts/{acc1.account_id}/verify-code", json={"challenge_id": cid1, "code": "12345"}, headers=h1
    )
    assert r_stale.status_code == 400
    assert r_stale.json()["error_code"] in ("challenge_cancelled", "challenge_stale")

    telegram_login_service._challenges[cid2].expires_at_monotonic = 0.0
    r_exp = client.post(
        f"/api/accounts/{acc1.account_id}/verify-code", json={"challenge_id": cid2, "code": "12345"}, headers=h1
    )
    assert r_exp.status_code == 400
    assert r_exp.json()["error_code"] == "challenge_expired"


def test_qr_login_with_2fa_password_continuation_and_secondary_no_console_token(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "k_qr_2fa")
    tfa = issue_2fa_token()
    headers = {"X-API-Key": "k_qr_2fa", "X-2FA-Token": tfa}

    acc1 = _seed_db_account(account_name="Acc1", phone="+12025550117", session_file="a1q.session", is_default=True)
    acc2 = _seed_db_account(account_name="Acc2", phone="+12025550119", session_file="a2q.session", is_default=False)

    me_acc2 = SimpleNamespace(id=2002, phone="12025550119", username="sec2", first_name="Sec2", premium=False)
    mock_tg, _ = _make_mock_client(
        qr_wait_coro=SessionPasswordNeededError(request=None),
        sign_in_side_effect=[PasswordHashInvalidError(request=None), None],
        me=me_acc2,
    )
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: mock_tg)

    r_start = client.post(f"/api/accounts/{acc2.account_id}/qr/start", headers=headers)
    assert r_start.status_code == 200
    assert r_start.headers.get("cache-control", "").startswith("no-store")
    cid = r_start.json()["challenge_id"]
    assert r_start.json()["qr_url"].startswith("tg://login?token=")
    mock_tg.qr_login.assert_awaited_once_with()

    r_poll = client.get(f"/api/accounts/{acc2.account_id}/qr/status?challenge_id={cid}", headers=headers)
    assert r_poll.status_code == 200
    assert r_poll.json()["state"] == "password_required"
    assert r_poll.json()["authenticated"] is False
    assert r_poll.json()["token"] is None

    r_bad_pwd = client.post(
        f"/api/accounts/{acc2.account_id}/qr/password",
        json={"challenge_id": cid, "password": "wrong"},
        headers=headers,
    )
    assert r_bad_pwd.status_code == 401
    assert r_bad_pwd.json()["error_code"] == "invalid_password"

    r_ok = client.post(
        f"/api/accounts/{acc2.account_id}/qr/password",
        json={"challenge_id": cid, "password": "  right pass  "},
        headers=headers,
    )
    assert r_ok.status_code == 200
    data = r_ok.json()
    assert data["authenticated"] is True
    assert data["account_id"] == acc2.account_id
    assert "token" not in data
    assert acc2.auth_state == "AUTHENTICATED"
    assert acc2.user_id == 2002
    assert acc1.auth_state == "UNAUTHENTICATED"

    r_replay = client.post(
        f"/api/accounts/{acc2.account_id}/qr/password",
        json={"challenge_id": cid, "password": "  right pass  "},
        headers=headers,
    )
    assert r_replay.status_code == 400


@pytest.mark.parametrize(
    "bad_me",
    [
        None,
        SimpleNamespace(id=0, phone="12025550117"),
        SimpleNamespace(id=-5, phone="12025550117"),
        SimpleNamespace(id=9999, phone="12025550132"),
        SimpleNamespace(id=9999, phone=None),
    ],
)
def test_qr_wrong_or_missing_identity_does_not_persist_and_preserves_existing_session(monkeypatch, bad_me):
    monkeypatch.setattr(settings, "API_KEY", "k_qr_id")
    tfa = issue_2fa_token()
    headers = {"X-API-Key": "k_qr_id", "X-2FA-Token": tfa}

    official_client = AsyncMock()
    acc = _seed_db_account(
        account_name="ExistingGood",
        phone="+12025550117",
        session_file="good.session",
        is_default=True,
        is_active=True,
        user_id=1111,
        auth_state="AUTHENTICATED",
    )
    acc.client = official_client

    mock_temp, _ = _make_mock_client(me=bad_me)
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: mock_temp)
    commit_spy = MagicMock()
    monkeypatch.setattr("app.db.account_repository.AccountRepository.commit_verified_login", commit_spy)

    # Verified existing recovery allows omitting phone on qr/start and reuses stored target + user_id
    r_start = client.post("/api/telegram/qr/start", headers=headers)
    assert r_start.status_code == 200
    cid = r_start.json()["challenge_id"]
    mock_temp.qr_login.assert_awaited_once_with()

    r_poll = client.get(f"/api/telegram/qr/status?challenge_id={cid}", headers=headers)
    assert r_poll.status_code == 200
    poll_data = r_poll.json()
    assert poll_data["state"] == "failed"
    assert poll_data["authenticated"] is False
    assert poll_data["token"] is None
    assert poll_data["error_code"] == "identity_mismatch"

    assert acc.auth_state == "AUTHENTICATED"
    assert acc.user_id == 1111
    assert acc.client is official_client
    official_client.log_out.assert_not_awaited()
    commit_spy.assert_not_called()
    mock_temp.disconnect.assert_awaited()


def test_real_file_handover_rollback_on_db_error_and_missing_auth_key(monkeypatch):
    """Real .session files in tests/.tmp DATA_DIR must restore previous bytes & DB fields on handover failure."""
    monkeypatch.setattr(settings, "API_KEY", "k_handover_rb")
    acc = _seed_db_account(
        account_name="RollbackAcc",
        phone="+12025550117",
        session_file="rb_real.session",
        is_default=True,
        is_active=True,
        user_id=1111,
        auth_state="AUTHENTICATED",
        dc_id=2,
        is_premium=False,
    )
    official_path = os.path.join(settings.DATA_DIR, "rb_real.session")
    prev_bytes = b"PREVIOUS_KNOWN_GOOD_SESSION_DATA_V1" * 16
    with open(official_path, "wb") as f:
        f.write(prev_bytes)

    tfa = issue_2fa_token()
    headers = {"X-API-Key": "k_handover_rb", "X-2FA-Token": tfa}

    # Subcase A: missing auth_key -> hard failure, official_path & DB preserved
    mock_no_key, _ = _make_mock_client(
        me=SimpleNamespace(id=1111, phone="12025550117", username="ok", first_name="OK", premium=True),
        auth_key_bytes=None,
    )
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: mock_no_key)
    r1 = client.post("/api/telegram/native/send-code", json={"phone": "+12025550117"}, headers=headers)
    cid1 = r1.json()["challenge_id"]
    r_v1 = client.post(
        "/api/telegram/native/verify-code", json={"challenge_id": cid1, "code": "12345"}, headers=headers
    )
    assert r_v1.status_code == 409
    with open(official_path, "rb") as f:
        assert f.read() == prev_bytes

    # Subcase B: DB commit failure AFTER staging + swap -> restores prev_bytes and leaves zero staging/bak files
    me_same = SimpleNamespace(id=1111, phone="12025550117", username="new_u", first_name="New", premium=True)
    mock_ok, _ = _make_mock_client(me=me_same, auth_key_bytes=b"\xef" * 256)
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: mock_ok)
    monkeypatch.setattr(
        "app.db.account_repository.AccountRepository.commit_verified_login",
        MagicMock(side_effect=RuntimeError("simulated sqlite disk error")),
    )

    r2 = client.post("/api/telegram/native/send-code", json={"phone": "+12025550117"}, headers=headers)
    cid2 = r2.json()["challenge_id"]
    r_v2 = client.post(
        "/api/telegram/native/verify-code", json={"challenge_id": cid2, "code": "12345"}, headers=headers
    )
    assert r_v2.status_code == 500
    assert r_v2.json()["error_code"] == "persistence_failed"
    assert "token" not in r_v2.json()

    with open(official_path, "rb") as f:
        assert f.read() == prev_bytes
    leftover = [fn for fn in os.listdir(settings.DATA_DIR) if "rb_real.session." in fn]
    assert leftover == []
    db_row = AccountRepository.get_by_id(acc.account_id)
    assert db_row is not None
    assert db_row["user_id"] == 1111
    assert db_row["is_premium"] == 0
    assert acc.is_premium is False


@pytest.mark.asyncio
async def test_handover_disconnect_and_cancelled_error_restore_previous_file_and_block_disabled_revival(monkeypatch):
    acc = _seed_db_account(
        account_name="CancelHandover",
        phone="+12025550117",
        session_file="cancel_hb.session",
        is_default=True,
        is_active=True,
        user_id=1001,
        auth_state="AUTHENTICATED",
    )
    from pathlib import Path

    official_path = Path(settings.DATA_DIR) / "cancel_hb.session"
    prev_bytes = b"KNOWN_GOOD_CANCEL_TEST_BYTES" * 10
    await asyncio.to_thread(official_path.write_bytes, prev_bytes)

    me_ok = SimpleNamespace(id=1001, phone="12025550117", username="u1", first_name="U1", premium=True)

    # 1. Reconnect failure after swap rolls back file
    mock_disconn, _ = _make_mock_client(me=me_ok, connected=False, auth_key_bytes=b"\x77" * 256)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("telethon.TelegramClient.connect", AsyncMock(side_effect=OSError("net down before commit")))
        with pytest.raises(OSError, match="net down"):
            await acc.apply_verified_login_session(mock_disconn, me_ok, target_phone="+12025550117")
    assert await asyncio.to_thread(official_path.read_bytes) == prev_bytes

    # 2. CancelledError during commit_verified_login rolls back file & preserves prior persisted state
    mock_cancel, _ = _make_mock_client(me=me_ok, connected=True, auth_key_bytes=b"\x88" * 256)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.db.account_repository.AccountRepository.commit_verified_login",
            MagicMock(side_effect=asyncio.CancelledError()),
        )
        with pytest.raises(asyncio.CancelledError):
            await acc.apply_verified_login_session(mock_cancel, me_ok, target_phone="+12025550117")
    assert await asyncio.to_thread(official_path.read_bytes) == prev_bytes

    # 3. Account disabled in DB while awaiting cannot be revived
    mock_disabled, _ = _make_mock_client(me=me_ok, connected=True, auth_key_bytes=b"\x99" * 256)
    with db_cursor(commit=True) as cur:
        cur.execute("UPDATE tg_accounts SET is_active = 0 WHERE id = ?", (acc.account_id,))
    with pytest.raises(ValueError, match="禁用"):
        await acc.apply_verified_login_session(mock_disabled, me_ok, target_phone="+12025550117")
    assert await asyncio.to_thread(official_path.read_bytes) == prev_bytes
    assert AccountRepository.get_by_id(acc.account_id)["is_active"] == 0


@pytest.mark.asyncio
async def test_handover_stage_failure_disposes_client_and_db_refencing_blocks_inflight_mutations():
    from pathlib import Path

    acc = _seed_db_account(
        account_name="RefenceAcc",
        phone="+12025550117",
        session_file="refence.session",
        is_default=True,
        is_active=True,
        user_id=1001,
        auth_state="AUTHENTICATED",
    )
    official_path = Path(settings.DATA_DIR) / "refence.session"
    prev_bytes = b"REFENCE_KNOWN_GOOD_BYTES" * 10
    await asyncio.to_thread(official_path.write_bytes, prev_bytes)
    me_ok = SimpleNamespace(id=1001, phone="12025550117", username="u1", first_name="U1", premium=False)

    # 1. Stage failure disposes isolated client and leaves official bytes/DB intact
    mock_stage_fail, _ = _make_mock_client(me=me_ok, connected=True, auth_key_bytes=b"\x55" * 256)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.core.client_manager._stage_session_file_sync",
            MagicMock(side_effect=OSError("stage write denied")),
        )
        with pytest.raises(OSError, match="stage write denied"):
            await acc.apply_verified_login_session(mock_stage_fail, me_ok, target_phone="+12025550117")
    mock_stage_fail.disconnect.assert_awaited()
    mock_stage_fail.session.close.assert_called()
    assert await asyncio.to_thread(official_path.read_bytes) == prev_bytes


@pytest.mark.asyncio
async def test_handover_db_refencing_blocks_inflight_mutations_and_allows_legacy_blank_phone():
    from pathlib import Path

    acc = _seed_db_account(
        account_name="RefenceAccDb",
        phone="+12025550117",
        session_file="refence_db.session",
        is_default=True,
        is_active=True,
        user_id=1001,
        auth_state="AUTHENTICATED",
    )
    official_path = Path(settings.DATA_DIR) / "refence_db.session"
    prev_bytes = b"REFENCE_DB_GOOD_BYTES" * 10
    await asyncio.to_thread(official_path.write_bytes, prev_bytes)
    me_ok = SimpleNamespace(id=1001, phone="12025550117", username="u1", first_name="U1", premium=False)

    # 1. Clearing user_id to NULL while handover is in flight invalidates recovery commit & rolls back file
    async def clear_uid_during_connect(*_args, **_kwargs):
        with db_cursor(commit=True) as cur:
            cur.execute("UPDATE tg_accounts SET user_id = NULL WHERE id = ?", (acc.account_id,))

    mock_uid_cleared, _ = _make_mock_client(me=me_ok, connected=False, auth_key_bytes=b"\x56" * 256)
    with pytest.MonkeyPatch.context() as mp:
        _mock_replacement_rpc(mp, me_ok, clear_uid_during_connect)
        with pytest.raises(ValueError, match="用户状态已变更"):
            await acc.apply_verified_login_session(mock_uid_cleared, me_ok, target_phone="+12025550117")
    assert await asyncio.to_thread(official_path.read_bytes) == prev_bytes
    assert AccountRepository.get_by_id(acc.account_id)["user_id"] is None

    # Restore user_id for next subcase
    with db_cursor(commit=True) as cur:
        cur.execute("UPDATE tg_accounts SET user_id = 1001 WHERE id = ?", (acc.account_id,))

    # 3. Changing persisted phone or session_file while handover is in flight fails fenced commit & rolls back file
    async def change_phone_during_connect(*_args, **_kwargs):
        with db_cursor(commit=True) as cur:
            cur.execute("UPDATE tg_accounts SET phone = '+12025550132' WHERE id = ?", (acc.account_id,))

    mock_phone_changed, _ = _make_mock_client(me=me_ok, connected=False, auth_key_bytes=b"\x57" * 256)
    with pytest.MonkeyPatch.context() as mp:
        _mock_replacement_rpc(mp, me_ok, change_phone_during_connect)
        with pytest.raises(ValueError, match="手机号已变更"):
            await acc.apply_verified_login_session(mock_phone_changed, me_ok, target_phone="+12025550117")
    assert await asyncio.to_thread(official_path.read_bytes) == prev_bytes
    assert AccountRepository.get_by_id(acc.account_id)["phone"] == "+12025550132"

    # Restore phone and test session_file refence
    with db_cursor(commit=True) as cur:
        cur.execute("UPDATE tg_accounts SET phone = '+12025550117' WHERE id = ?", (acc.account_id,))

    async def change_session_during_connect(*_args, **_kwargs):
        with db_cursor(commit=True) as cur:
            cur.execute("UPDATE tg_accounts SET session_file = 'tampered.session' WHERE id = ?", (acc.account_id,))

    mock_sess_changed, _ = _make_mock_client(me=me_ok, connected=False, auth_key_bytes=b"\x58" * 256)
    with pytest.MonkeyPatch.context() as mp:
        _mock_replacement_rpc(mp, me_ok, change_session_during_connect)
        with pytest.raises(ValueError, match="会话目标已变更"):
            await acc.apply_verified_login_session(mock_sess_changed, me_ok, target_phone="+12025550117")
    assert await asyncio.to_thread(official_path.read_bytes) == prev_bytes

    # 4. DB phone disagreeing with target at handover start fails closed before swap
    acc_mismatch = _seed_db_account(
        account_name="MismatchStart",
        phone="+12025550117",
        session_file="mismatch_start.session",
        is_default=False,
        is_active=True,
    )
    with db_cursor(commit=True) as cur:
        cur.execute("UPDATE tg_accounts SET phone = '+12025550115' WHERE id = ?", (acc_mismatch.account_id,))
    mock_mm, _ = _make_mock_client(me=me_ok, connected=True, auth_key_bytes=b"\x59" * 256)
    with pytest.raises(ValueError, match="数据库记录的手机号"):
        await acc_mismatch.apply_verified_login_session(mock_mm, me_ok, target_phone="+12025550117")

    # 5. Legitimate legacy blank-phone account commits cleanly without config/env conflict rejection
    acc_blank = _seed_db_account(
        account_name="LegacyBlank",
        phone="",
        session_file="legacy_blank.session",
        is_default=False,
        is_active=True,
    )
    mock_blank, _ = _make_mock_client(me=me_ok, connected=True, auth_key_bytes=b"\x5a" * 256)
    await acc_blank.apply_verified_login_session(mock_blank, me_ok, target_phone="+12025550117")
    assert acc_blank.auth_state == "AUTHENTICATED"
    assert AccountRepository.get_by_id(acc_blank.account_id)["phone"] == "+12025550117"


@pytest.mark.asyncio
async def test_qr_otp_race_cancel_and_shutdown_cleanup(monkeypatch):
    acc = _seed_db_account(account_name="RaceAcc", phone="+12025550117", session_file="race.session")

    wait_entered = asyncio.Event()
    release_wait = asyncio.Event()

    async def blocking_wait(**_kwargs):
        wait_entered.set()
        await release_wait.wait()
        return SimpleNamespace(id=8888, phone="12025550117", username="race_u", first_name="Race", premium=False)

    mock_qr_client, _ = _make_mock_client(qr_wait_coro=blocking_wait)
    mock_otp_client, _ = _make_mock_client()
    clients = iter([mock_qr_client, mock_otp_client])
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: next(clients))

    qr_res = await telegram_login_service.start_qr(
        flow="native",
        account_id=None,
        phone="+12025550117",
        auth_mode="open",
        initiator_fingerprint="fp_1",
    )
    qr_cid = qr_res["challenge_id"]
    await asyncio.wait_for(wait_entered.wait(), timeout=2.0)

    otp_res = await telegram_login_service.start_otp(
        flow="native",
        account_id=None,
        phone="+12025550117",
        auth_mode="open",
        initiator_fingerprint="fp_1",
    )
    assert otp_res["challenge_id"] != qr_cid
    mock_qr_client.disconnect.assert_awaited()
    assert acc.auth_state == "UNAUTHENTICATED"

    await telegram_login_service.shutdown()
    mock_otp_client.disconnect.assert_awaited()


@pytest.mark.asyncio
async def test_abandoned_otp_and_password_challenges_auto_expire_without_poll_and_registry_bounded(monkeypatch):
    _seed_db_account(account_name="ExpAcc", phone="+12025550117", session_file="exp_auto.session")

    # 1. Abandoned OTP auto-expires via background watchdog without another client poll
    mock_otp, _ = _make_mock_client()
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: mock_otp)
    monkeypatch.setattr("app.services.telegram_login_service.OTP_CHALLENGE_TTL_SECONDS", 0.05)

    res = await telegram_login_service.start_otp(
        flow="native",
        account_id=None,
        phone="+12025550117",
        auth_mode="open",
        initiator_fingerprint="fp_exp",
    )
    cid = res["challenge_id"]
    await asyncio.sleep(0.15)
    assert telegram_login_service._challenges[cid].state == "expired"
    mock_otp.disconnect.assert_awaited()
    mock_otp.session.close.assert_called()

    # 2. Password-pending challenge also auto-expires and disconnects without poll
    monkeypatch.setattr("app.services.telegram_login_service.OTP_CHALLENGE_TTL_SECONDS", 300.0)
    monkeypatch.setattr("app.services.telegram_login_service.PASSWORD_CONTINUATION_TTL_SECONDS", 0.05)
    mock_pwd_wait, _ = _make_mock_client(qr_wait_coro=SessionPasswordNeededError(request=None))
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: mock_pwd_wait)
    qr_res = await telegram_login_service.start_qr(
        flow="native",
        account_id=None,
        phone="+12025550117",
        auth_mode="open",
        initiator_fingerprint="fp_pwd_exp",
    )
    qr_cid = qr_res["challenge_id"]
    await asyncio.sleep(0.15)
    assert telegram_login_service._challenges[qr_cid].state == "expired"
    mock_pwd_wait.disconnect.assert_awaited()

    # 3. Bounded registry never pops active work without cleanup
    acc_cap = _seed_db_account(
        account_name="AccCap", phone="+12025550120", session_file="cap.session", is_default=False
    )
    for idx in range(MAX_ACTIVE_CHALLENGES):
        dummy_id = f"active_{idx}"
        telegram_login_service._challenges[dummy_id] = MagicMock(
            challenge_id=dummy_id,
            account_id=10000 + idx,
            state="code_sent",
            created_at_monotonic=float(idx),
            expires_at_monotonic=9e18,
            terminal_at_monotonic=None,
        )
    with pytest.raises(TelegramLoginError) as exc_info:
        await telegram_login_service.start_otp(
            flow="account",
            account_id=acc_cap.account_id,
            phone="+12025550120",
            auth_mode="open",
            initiator_fingerprint="fp_cap",
        )
    assert getattr(exc_info.value, "error_code", None) == "too_many_challenges"
    assert len([k for k in telegram_login_service._challenges if k.startswith("active_")]) == MAX_ACTIVE_CHALLENGES


@pytest.mark.parametrize("had_old", [False, True])
@pytest.mark.parametrize("committed", [False, True])
def test_session_handover_recovers_interrupted_commit(tmp_path, had_old, committed):
    acc = _seed_db_account(user_id=1001)
    base = tmp_path / "recover"
    official = base.with_suffix(".session")
    if had_old:
        official.write_bytes(b"old authorized session")
    staged = _stage_session_file_sync(str(base), 4, "149.154.167.91", 443, _dummy_auth_key())
    if os.name == "posix":
        assert os.stat(staged).st_mode & 0o777 == 0o600
    with open(staged, "rb") as staged_file:
        new_bytes = staged_file.read()
    transaction_id = "a" * 32
    _swap_with_backup_sync(str(official), staged, transaction_id)
    assert official.read_bytes() == new_bytes
    if committed:
        AccountRepository.commit_verified_login(
            acc.account_id,
            expected_user_id=1001,
            handover_id=transaction_id,
            user_id=1001,
            username=None,
            first_name="Test",
            dc_id=4,
            is_premium=False,
        )
        assert transaction_id not in SystemSettingsRepository.get_all().values()

    _recover_session_handover_sync(str(official), acc.account_id)
    _recover_session_handover_sync(str(official), acc.account_id)
    if committed or had_old:
        assert official.read_bytes() == (new_bytes if committed else b"old authorized session")
    else:
        assert not official.exists()
    assert not list((tmp_path / ".telegram-handover").iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize("loaded", [False, True])
async def test_delete_retains_committed_session_for_reuse(monkeypatch, tmp_path, loaded):
    monkeypatch.setattr(settings, "DATA_DIR", str(tmp_path))
    acc = _seed_db_account(user_id=1001, session_file="retained.session")
    base = tmp_path / "retained"
    official = base.with_suffix(".session")
    official.write_bytes(b"old session")
    staged = _stage_session_file_sync(str(base), 4, "149.154.167.91", 443, _dummy_auth_key())
    transaction_id = "b" * 32
    _swap_with_backup_sync(str(official), staged, transaction_id)
    committed_session = official.read_bytes()
    AccountRepository.commit_verified_login(
        acc.account_id,
        expected_user_id=1001,
        handover_id=transaction_id,
        user_id=1001,
        username=None,
        first_name="Test",
        dc_id=4,
        is_premium=False,
    )
    if not loaded:
        client_manager._accounts.pop(acc.account_id)

    await client_manager.remove_account(acc.account_id)

    assert AccountRepository.get_by_id(acc.account_id) is None
    assert AccountRepository.get_committed_handover(acc.account_id) is None
    new_account = _seed_db_account(session_file="retained.session")
    assert new_account.account_id != acc.account_id
    _recover_session_handover_sync(str(official), new_account.account_id)
    assert official.read_bytes() == committed_session
    assert not list((tmp_path / ".telegram-handover").iterdir())


@pytest.mark.asyncio
async def test_failed_file_rollback_marks_account_unavailable(monkeypatch):
    from app.core.client_manager import LoginBinding

    acc = _seed_db_account(user_id=1001, auth_state="AUTHENTICATED")
    row = AccountRepository.get_by_id(acc.account_id)
    assert row is not None
    monkeypatch.setattr(
        "app.core.client_manager._recover_session_handover_sync",
        MagicMock(side_effect=OSError("session temporarily locked")),
    )
    await acc._rollback_handover_state(
        old_client=None,
        binding=LoginBinding.from_record(row),
        official_path="unused.session",
        activated_client=None,
        persisted_session=None,
        source_client=None,
    )
    assert acc.auth_state == "UNAUTHENTICATED"
    row = AccountRepository.get_by_id(acc.account_id)
    assert row is not None
    assert row["status"] == "unauthenticated"
    assert row["user_id"] == 1001


@pytest.mark.asyncio
async def test_handover_reconnect_restores_old_client_on_db_failure(monkeypatch):
    acc = _seed_db_account(user_id=1001, auth_state="AUTHENTICATED")
    me = SimpleNamespace(id=1001, phone="12025550117")
    old, _ = _make_mock_client(me=me)
    acc.client = old
    connected = True

    async def disconnect():
        nonlocal connected
        connected = False

    async def connect():
        nonlocal connected
        connected = True

    old.is_connected.side_effect = lambda: connected
    old.disconnect.side_effect = disconnect
    old.connect.side_effect = connect
    temp, _ = _make_mock_client(me=me)
    monkeypatch.setattr(AccountRepository, "commit_verified_login", MagicMock(side_effect=OSError("db failed")))

    with pytest.raises(OSError, match="db failed"):
        await acc.apply_verified_login_session(temp, me)

    assert acc.client is old
    assert acc.is_ready()
    old.connect.assert_awaited_once()
    old.get_me.assert_awaited_once()


@pytest.mark.asyncio
async def test_start_reserves_capacity_and_cleans_up_cancelled_client(monkeypatch):
    from app.services import telegram_login_service as login_module

    monkeypatch.setattr(login_module, "MAX_ACTIVE_CHALLENGES", 1)
    first = _seed_db_account(account_name="First", session_file="first.session")
    second = _seed_db_account(account_name="Second", session_file="second.session", is_default=False)
    slow, _ = _make_mock_client()
    next_client, _ = _make_mock_client()
    entered = asyncio.Event()

    async def slow_send(*_args, **_kwargs):
        entered.set()
        await asyncio.Event().wait()

    slow.send_code_request.side_effect = slow_send
    clients = iter((slow, next_client))
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _acc: next(clients))

    async def start(account_id):
        return await telegram_login_service.start_otp(
            flow="account", account_id=account_id, phone=None, auth_mode="2fa", initiator_fingerprint="browser"
        )

    task = asyncio.create_task(start(first.account_id))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        with pytest.raises(TelegramLoginError) as exc:
            await start(second.account_id)
        assert exc.value.error_code == "too_many_challenges"
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    slow.disconnect.assert_awaited_once()
    slow.session.close.assert_called_once()
    assert (await start(second.account_id))["state"] == "code_sent"
    await telegram_login_service.shutdown()


@pytest.mark.asyncio
async def test_cleared_binding_during_identity_rpc_is_not_restored(monkeypatch):
    acc = _seed_db_account()

    async def get_me():
        await client_manager.update_account(acc.account_id, phone="")
        return SimpleNamespace(id=1001, phone="12025550117")

    mock, _ = _make_mock_client(me=get_me)
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _acc: mock)
    identity = dict(flow="account", account_id=acc.account_id, initiator_fingerprint="browser")
    challenge = await telegram_login_service.start_otp(phone="12025550117", auth_mode="2fa", **identity)
    with pytest.raises(TelegramLoginError, match="绑定已变更"):
        await telegram_login_service.verify_otp(
            challenge_id=challenge["challenge_id"], code="12345", password=None, **identity
        )
    assert AccountRepository.get_by_id(acc.account_id)["phone"] == ""
    assert AccountRepository.get_by_id(acc.account_id)["user_id"] is None
    await telegram_login_service.shutdown()


@pytest.mark.asyncio
async def test_qr_expiry_interrupts_identity_lookup_without_polling(monkeypatch):
    from app.services import telegram_login_service as login_module

    acc = _seed_db_account()
    disconnected = asyncio.Event()

    async def get_me():
        await asyncio.Event().wait()

    async def disconnect():
        disconnected.set()

    mock, _ = _make_mock_client(me=get_me)
    mock.disconnect.side_effect = disconnect
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _acc: mock)
    monkeypatch.setattr(login_module, "_compute_qr_expiry", lambda _expiry, wall, mono: (mono + 0.1, wall + 0.1))
    qr = await telegram_login_service.start_qr(
        flow="native", account_id=None, phone="+12025550117", auth_mode="bootstrap", initiator_fingerprint="browser"
    )
    await asyncio.wait_for(disconnected.wait(), 2)
    ch = telegram_login_service._challenges[qr["challenge_id"]]
    assert ch.state == "expired"
    assert ch._temp_client is None
    assert AccountRepository.get_by_id(acc.account_id)["user_id"] is None
    await telegram_login_service.shutdown()


@pytest.mark.asyncio
async def test_unsupported_resend_ends_challenge_and_discards_old_hash(monkeypatch):
    acc = _seed_db_account()
    first = SimpleNamespace(phone_code_hash="first", type=SentCodeTypeApp(length=5), next_type=CodeTypeSms(), timeout=0)
    second = SimpleNamespace(phone_code_hash="second", type=SentCodeTypeSmsPhrase())
    mock, _ = _make_mock_client(send_results=[first], resend_results=[second])
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _acc: mock)
    start = await telegram_login_service.start_otp(
        flow="account", account_id=acc.account_id, phone=None, auth_mode="2fa", initiator_fingerprint="browser"
    )
    cid = start["challenge_id"]
    with pytest.raises(TelegramLoginError) as exc:
        await telegram_login_service.resend_otp(
            flow="account", account_id=acc.account_id, challenge_id=cid, initiator_fingerprint="browser"
        )
    assert exc.value.error_code == "delivery_unsupported"
    ch = telegram_login_service._challenges[cid]
    assert ch.state == "failed"
    assert ch._phone_code_hash is None
    mock.disconnect.assert_awaited_once()
    await telegram_login_service.shutdown()


def test_verify_attempt_rate_limits_do_not_throttle_qr_polling(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "k_rate_test_123456")
    acc = _seed_db_account(account_name="RateAcc", phone="+12025550117", session_file="rl.session")
    never_done = asyncio.Event()

    async def wait_forever(**_kwargs):
        await never_done.wait()

    mock_qr, _ = _make_mock_client(qr_wait_coro=wait_forever)
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: mock_qr)

    browser = TestClient(app, client=("127.0.0.1", 50000))
    bootstrap = browser.post("/api/telegram/bootstrap/auth", headers=BOOTSTRAP_HEADERS)
    assert bootstrap.status_code == 200
    r_qr = browser.post(
        "/api/telegram/qr/start",
        json={"phone": "+12025550117"},
    )
    assert r_qr.status_code == 200
    qr_cid = r_qr.json()["challenge_id"]

    # Established bootstrap sessions can keep polling without spending header attempts.
    for _ in range(16):
        rp = browser.get(
            f"/api/telegram/qr/status?challenge_id={qr_cid}",
        )
        assert rp.status_code == 200

    # Verify-code wrong attempts are bounded per-challenge (5 attempts -> 429 too_many_attempts)
    from telethon.errors import PhoneCodeInvalidError

    mock_otp, _ = _make_mock_client(sign_in_side_effect=PhoneCodeInvalidError(request=None))
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: mock_otp)
    r_otp = browser.post(
        "/api/telegram/native/send-code",
        json={"phone": "+12025550117"},
    )
    assert r_otp.status_code == 200
    otp_cid = r_otp.json()["challenge_id"]

    for _ in range(4):
        bad = browser.post(
            "/api/telegram/native/verify-code",
            json={"challenge_id": otp_cid, "code": "99999"},
        )
        assert bad.status_code == 401
        assert bad.json()["error_code"] == "invalid_code"

    fifth = browser.post(
        "/api/telegram/native/verify-code",
        json={"challenge_id": otp_cid, "code": "99999"},
    )
    assert fifth.status_code == 429
    assert fifth.json()["error_code"] == "too_many_attempts"
    assert telegram_login_service._challenges[otp_cid].state == "failed"
    assert acc.auth_state == "UNAUTHENTICATED"


def test_flood_wait_translated_without_raw_rpc_leak(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "")
    acc = _seed_db_account(account_name="FloodAcc", phone="+12025550117", session_file="fw.session")
    fw_err = FloodWaitError(request=None, capture=45)
    mock_tg, _ = _make_mock_client(send_results=fw_err)
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _a: mock_tg)

    resp = client.post("/api/telegram/native/send-code", json={"phone": "+12025550117"})
    assert resp.status_code == 429
    body = resp.json()
    assert body["error_code"] == "flood_wait"
    assert body["retry_after_seconds"] == 45
    assert acc.auth_state == "UNAUTHENTICATED"
