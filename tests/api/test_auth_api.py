"""API Layer: 身份鉴权、2FA 两步验证、JWT 会话生命周期与请求安全防护测试 (Auth API & Security)

覆盖范围:
- 原 test_auth_api: 登录/登出、密码校验、token 签发与刷新
- 原 test_2fa_session: Web 2FA 服务端强制校验、JWT 会话化、密钥派生与中间件拦截
- 原 test_auth_security: 时序攻击防御、API Key 校验、请求头校验与重放攻击防护"""

import asyncio
import hashlib
import time
from unittest.mock import AsyncMock, patch

import jwt as pyjwt
import pytest
from _cm_seed import seed_default_account
from _safeio import safe_write_bytes
from fastapi import HTTPException, Request, WebSocketDisconnect
from fastapi.testclient import TestClient

from app.api.auth import (
    MAX_RATE_LIMITER_IPS,
    MEDIA_TICKET_TTL_SECONDS,
    _constant_time_compare,
    _get_2fa_secret,
    _media_tickets,
    _request_timestamps,
    _sensitive_action_timestamps,
    consume_media_ticket,
    issue_2fa_token,
    issue_media_ticket,
    rate_limiter_middleware,
    validate_2fa_token,
    verify_api_key,
    verify_media_access,
)
from app.config import settings
from app.core.client_manager import client_manager
from app.core.connection_manager import connection_manager
from app.main import app

# ==============================================================================
# Part: test_auth_api.py
# ==============================================================================
client = TestClient(app, client=("127.0.0.1", 50000))


def test_auth_status_endpoint():
    """测试 /api/auth/status 端点"""
    response = client.get("/api/auth/status")
    assert response.status_code == 200
    data = response.json()
    assert "auth_required" in data


def test_auth_verify_without_api_key(monkeypatch):
    """测试未配置 API_KEY 时 /api/auth/verify 放行"""
    monkeypatch.setattr(settings, "API_KEY", "")
    response = client.get("/api/auth/verify")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_auth_verify_with_api_key(monkeypatch):
    """测试配置 API_KEY 时 /api/auth/verify 拦截与允许"""
    secret = "my_secret_test_key_123"
    monkeypatch.setattr(settings, "API_KEY", secret)

    # 未携带 Header -> 401
    resp_no_key = client.get("/api/auth/verify")
    assert resp_no_key.status_code == 401

    # 错误 Header -> 401 (由正确 secret 派生错误值, 避免引入字面量凭据)
    resp_wrong_key = client.get("/api/auth/verify", headers={"X-API-Key": secret + "_wrong"})
    assert resp_wrong_key.status_code == 401

    # 正确 Header -> 200
    resp_right_key = client.get("/api/auth/verify", headers={"X-API-Key": secret})
    assert resp_right_key.status_code == 200
    assert resp_right_key.json()["status"] == "ok"


def test_protected_endpoints_with_api_key(monkeypatch):
    """测试受保护的 /api/tasks 和 /api/presets 接口在有/无 Key 时的行为"""
    secret = "super_secure_key_456"
    monkeypatch.setattr(settings, "API_KEY", secret)

    # 无 Key
    res_unauth = client.get("/api/tasks")
    assert res_unauth.status_code == 401

    # 有 Key 但无 2FA 会话 token -> 403 (与 401 区分)
    res_no_2fa = client.get("/api/tasks", headers={"X-API-Key": secret})
    assert res_no_2fa.status_code == 403
    assert res_no_2fa.json()["detail"].startswith("2FA")

    # 有 Key + 2FA 会话 token -> 200
    res_auth = client.get("/api/tasks", headers={"X-API-Key": secret, "X-2FA-Token": issue_2fa_token()})
    assert res_auth.status_code == 200


def test_query_credentials_rejected_for_standard_api(monkeypatch):
    """验证常规管理 API 严格拒绝 URL Query 凭据传参 (防止敏感凭证在日志/历史记录中泄露)"""
    secret = "query_leak_prevention_key"
    monkeypatch.setattr(settings, "API_KEY", secret)

    # 1. /api/auth/verify 传 Query token -> 401
    resp = client.get(f"/api/auth/verify?token={secret}")
    assert resp.status_code == 401

    # 2. /api/tasks 传 Query token 与 two_fa_token -> 401 (无法绕过 Header 限制)
    tfa = issue_2fa_token()
    resp = client.get(f"/api/tasks?token={secret}&two_fa_token={tfa}")
    assert resp.status_code == 401

    # 3. /api/storage/files 传 Query token -> 401
    resp = client.get(f"/api/storage/files?token={secret}&two_fa_token={tfa}")
    assert resp.status_code == 401


def test_media_access_ticket_and_header_paths(monkeypatch, tmp_path):
    """验证媒体流接口 (preview/download) 的两条鉴权路径:
    浏览器原生标签凭短时效 ticket 直连 (TTL 内可重复消费); 程序化客户端走双 Header;
    明文 query 传 Key/2FA Token 的历史豁免已收口, 一律 401。"""

    secret = "media_stream_key_789"
    monkeypatch.setattr(settings, "API_KEY", secret)
    old_dl = settings.DOWNLOAD_DIR
    settings.DOWNLOAD_DIR = str(tmp_path)
    try:
        safe_write_bytes(str(tmp_path / "sample.mp4"), b"0123456789" * 10)
        tfa = issue_2fa_token()

        # 1. 签发 ticket 后 preview/download 均可凭 ticket= 直连
        issue_resp = client.post("/api/auth/media-ticket", headers={"X-API-Key": secret, "X-2FA-Token": tfa})
        assert issue_resp.status_code == 200
        ticket = issue_resp.json()["ticket"]
        assert issue_resp.json()["expires_in"] == MEDIA_TICKET_TTL_SECONDS

        resp_prev = client.get(f"/api/storage/preview/sample.mp4?ticket={ticket}")
        assert resp_prev.status_code == 200
        # TTL 内可重复消费 (兼容 <video> 同 URL 多次 206 Range 分段请求)
        resp_prev_again = client.get(f"/api/storage/preview/sample.mp4?ticket={ticket}")
        assert resp_prev_again.status_code == 200
        resp_dl = client.get(f"/api/storage/download/sample.mp4?ticket={ticket}")
        assert resp_dl.status_code == 200

        # 2. 双 Header 路径 (程序化客户端) 依旧可用
        resp_header = client.get("/api/storage/preview/sample.mp4", headers={"X-API-Key": secret, "X-2FA-Token": tfa})
        assert resp_header.status_code == 200

        # 3. 错误/过期 ticket -> 401
        resp_bad_ticket = client.get("/api/storage/preview/sample.mp4?ticket=not_a_real_ticket")
        assert resp_bad_ticket.status_code == 401
        _media_tickets[ticket] = 0.0  # 强制置为已过期
        resp_expired = client.get(f"/api/storage/preview/sample.mp4?ticket={ticket}")
        assert resp_expired.status_code == 401
        assert not consume_media_ticket(ticket)

        # 4. 明文 query 凭据历史豁免已收口 -> 401
        resp_legacy = client.get(f"/api/storage/preview/sample.mp4?token={secret}&two_fa_token={tfa}")
        assert resp_legacy.status_code == 401
    finally:
        settings.DOWNLOAD_DIR = old_dl


# ==============================================================================
# Part: test_2fa_session.py
# ==============================================================================
client = TestClient(app, client=("127.0.0.1", 50000))

# 统一使用 >=32 字节测试密钥, 避免 PyJWT InsecureKeyLengthWarning
TEST_KEY = "unit_test_api_key_0123456789abcdef"
TEST_STANDALONE_KEY = "standalone_secret_0123456789abcdef"


@pytest.fixture(autouse=True)
def _isolate_security_settings(monkeypatch):
    """每个用例使用干净的密钥配置并关闭限流, 避免真实 .env 取值与 429 干扰。"""
    monkeypatch.setattr(settings, "SECRET_KEY", "")
    monkeypatch.setattr(settings, "RATE_LIMIT_PER_MINUTE", 0)


# ===== token 本体: 签发 / 密钥派生 / 失效 =====


def test_issue_token_claims(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)
    token = issue_2fa_token()
    assert token
    payload = pyjwt.decode(token, _get_2fa_secret(), algorithms=["HS256"])
    assert payload["sub"] == "owner"
    assert payload["jti"]  # 预留字段: 存在但服务端不校验
    assert payload["exp"] - payload["iat"] == 24 * 3600  # 24h TTL


def test_open_mode_issues_no_token(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "")
    assert issue_2fa_token() is None
    assert not validate_2fa_token("anything")


def test_secret_rotation_invalidates_tokens(monkeypatch, tmp_path):
    """SECRET_KEY 轮换使已签发 token 失效; 持久密钥模式下 API_KEY 轮换不再影响会话 (新契约)。"""
    monkeypatch.setattr(settings, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)
    token = issue_2fa_token()
    assert validate_2fa_token(token)

    # 显式配置 SECRET_KEY 后, 持久密钥签发的 token 失效
    monkeypatch.setattr(settings, "SECRET_KEY", TEST_STANDALONE_KEY)
    assert not validate_2fa_token(token)
    assert validate_2fa_token(issue_2fa_token())

    # 回到持久密钥模式并轮换 API_KEY: 签名密钥与 API_KEY 解耦, 旧 token 依旧有效 (新契约)
    monkeypatch.setattr(settings, "SECRET_KEY", "")
    monkeypatch.setattr(settings, "API_KEY", "secret_b")
    assert validate_2fa_token(token)


def test_signing_key_never_derived_from_api_key(monkeypatch, tmp_path):
    """回归锁: 签名密钥绝不从 API_KEY 派生 —— 旧派生密钥 (sha256(b"2fa:"+API_KEY))
    与 API_KEY 原文都不能验证已签发会话。"""
    monkeypatch.setattr(settings, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)
    token = issue_2fa_token()
    legacy_derived = hashlib.sha256(b"2fa:" + TEST_KEY.encode()).digest()
    with pytest.raises(pyjwt.InvalidSignatureError):
        pyjwt.decode(token, legacy_derived, algorithms=["HS256"])
    with pytest.raises(pyjwt.InvalidSignatureError):
        pyjwt.decode(token, TEST_KEY.encode(), algorithms=["HS256"])


def test_api_key_holder_cannot_forge_session(monkeypatch, tmp_path):
    """主回归: 仅持有 API_KEY 的攻击者无法离线伪造有效 2FA 会话 (CWE-836)。"""
    monkeypatch.setattr(settings, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)
    now = int(time.time())
    legacy_key = hashlib.sha256(b"2fa:" + TEST_KEY.encode()).digest()
    forged = pyjwt.encode({"sub": "owner", "iat": now, "exp": now + 3600, "jti": "x"}, legacy_key, algorithm="HS256")
    assert not validate_2fa_token(forged)


def test_persisted_secret_stable_across_calls_and_restarts(monkeypatch, tmp_path):
    """持久密钥: 同一 DATA_DIR 下多次取用 (跨"重启") 稳定, 文件落盘且内容非空。"""
    monkeypatch.setattr(settings, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)
    s1 = _get_2fa_secret()
    s2 = _get_2fa_secret()
    assert s1 == s2
    secret_file = tmp_path / ".web_2fa_secret"
    assert secret_file.exists()
    assert secret_file.read_bytes().strip() == s1

    # 模拟重启: 后续调用读取同一文件 (非重新生成)
    assert _get_2fa_secret() == s1


def test_persisted_secret_private_before_write(monkeypatch, tmp_path):
    import os
    import stat

    from app.api import auth

    monkeypatch.setattr(settings, "DATA_DIR", str(tmp_path))
    real_fdopen = os.fdopen
    observed = []

    def check_fdopen(fd, mode):
        # Inspect the file before the writer can write any secret bytes.
        observed.append(stat.S_IMODE(os.fstat(fd).st_mode))
        assert os.fstat(fd).st_size == 0
        if os.name == "posix":
            assert observed[-1] == 0o600
        return real_fdopen(fd, mode)

    monkeypatch.setattr(auth.os, "fdopen", check_fdopen)
    previous_umask = os.umask(0)
    try:
        secret = auth._load_or_create_persisted_secret()
    finally:
        os.umask(previous_umask)
    if os.name == "posix":
        assert observed == [0o600]
    assert (tmp_path / ".web_2fa_secret").read_bytes() == secret
    assert list(tmp_path.iterdir()) == [tmp_path / ".web_2fa_secret"]


def test_persisted_secret_regenerated_when_file_corrupted(monkeypatch, tmp_path):
    """空/损坏密钥文件视为不存在 -> 重新生成 (无 API_KEY 派生旧值可回退)。"""
    monkeypatch.setattr(settings, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)
    (tmp_path / ".web_2fa_secret").write_bytes(b"\n  \n")
    s1 = _get_2fa_secret()
    assert s1
    assert (tmp_path / ".web_2fa_secret").read_bytes().strip() == s1
    assert _get_2fa_secret() == s1  # 重生成后稳定


def test_expired_token_rejected(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)
    now = int(time.time())
    expired = pyjwt.encode(
        {"sub": "owner", "iat": now - 100, "exp": now - 10, "jti": "x"},
        _get_2fa_secret(),
        algorithm="HS256",
    )
    assert not validate_2fa_token(expired)
    resp = client.get("/api/tasks", headers={"X-API-Key": TEST_KEY, "X-2FA-Token": expired})
    assert resp.status_code == 403


def test_tampered_payload_rejected(monkeypatch):
    """篡改 token payload 段任意一字符 -> 签名校验失败 -> 403。"""
    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)
    token = issue_2fa_token()
    head, payload_b64, sig = token.split(".")
    flipped = "A" if payload_b64[0] != "A" else "B"
    tampered = f"{head}.{flipped}{payload_b64[1:]}.{sig}"
    assert not validate_2fa_token(tampered)
    resp = client.get("/api/tasks", headers={"X-API-Key": TEST_KEY, "X-2FA-Token": tampered})
    assert resp.status_code == 403


def test_forged_signature_rejected(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)
    now = int(time.time())
    forged = pyjwt.encode(
        {"sub": "owner", "iat": now, "exp": now + 3600, "jti": "x"},
        b"attacker-known-secret-0123456789abcdef",
        algorithm="HS256",
    )
    assert not validate_2fa_token(forged)


def test_wrong_sub_rejected(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)
    now = int(time.time())
    alien = pyjwt.encode(
        {"sub": "someone-else", "iat": now, "exp": now + 3600},
        _get_2fa_secret(),
        algorithm="HS256",
    )
    assert not validate_2fa_token(alien)


# ===== verify_2fa_session 路由分支 =====


def test_business_route_requires_2fa_beyond_api_key(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)

    # 无 Key -> 401 (API Key 层先拦截)
    assert client.get("/api/tasks").status_code == 401

    # 有 Key 无 2FA -> 403, detail 前缀 "2FA" 为前端识别契约
    resp = client.get("/api/tasks", headers={"X-API-Key": TEST_KEY})
    assert resp.status_code == 403
    assert resp.json()["detail"].startswith("2FA")

    # Key + 有效 2FA -> 进入业务
    resp = client.get(
        "/api/tasks",
        headers={"X-API-Key": TEST_KEY, "X-2FA-Token": issue_2fa_token()},
    )
    assert resp.status_code == 200

    # 空 body 走到 422 校验层, 证明已穿过鉴权层 (curl 验收同款断言)
    resp = client.post(
        "/api/download/range",
        headers={"X-API-Key": TEST_KEY, "X-2FA-Token": issue_2fa_token()},
        json={},
    )
    assert resp.status_code == 422


def test_open_mode_bypasses_2fa(monkeypatch):
    """本机开放模式 (未配置 API_KEY): verify_2fa_session 与 verify_api_key 一致放行。"""
    monkeypatch.setattr(settings, "API_KEY", "")
    assert client.get("/api/tasks").status_code == 200


# ===== 豁免路由清单 =====


def test_exempt_routes_do_not_require_2fa(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)
    key = {"X-API-Key": TEST_KEY}

    # /api/auth/status 无需任何凭证 (前端启动探测)
    assert client.get("/api/auth/status").status_code == 200
    # 其余豁免路由仅需 API Key
    assert client.get("/api/auth/verify", headers=key).status_code == 200
    assert client.get("/api/telegram/status", headers=key).status_code == 200

    # web-2fa/verify 可达: 错码返回业务语义 401 (验证码错误), 而非鉴权语义 403
    # (注: 既有实现的 except 会把内层 HTTPException 重包装为 "401: 验证码错误")
    monkeypatch.setattr(client_manager, "verify_web_2fa_code", lambda ip, code: False)
    resp = client.post("/api/telegram/web-2fa/verify", json={"code": "000000"}, headers=key)
    assert resp.status_code == 401
    assert "验证码错误" in resp.json()["detail"]


def test_web_2fa_verify_success_mints_token(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)
    monkeypatch.setattr(client_manager, "verify_web_2fa_code", lambda ip, code: True)
    resp = client.post(
        "/api/telegram/web-2fa/verify",
        json={"code": "123456"},
        headers={"X-API-Key": TEST_KEY},
    )
    assert resp.status_code == 200
    token = resp.json()["token"]
    assert token and validate_2fa_token(token)


# ===== native 登录: 条件引导豁免 =====


def test_native_routes_enforce_2fa_when_authenticated(monkeypatch):
    """已登录状态下 native 路由受 2FA 强制保护 (防止持 Key 者重绑 Telegram 账号)。"""
    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)
    acc = seed_default_account(client_manager, client=None, auth_state="AUTHENTICATED")
    monkeypatch.setattr(client_manager, "_accounts", {acc.account_id: acc})
    resp = client.post(
        "/api/telegram/native/send-code",
        json={"phone": "+12025550116"},
        headers={"X-API-Key": TEST_KEY},
    )
    assert resp.status_code == 403


def _mock_isolated_otp_client(phone: str = "12025550116", user_id: int = 1001):
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    mock_tg = AsyncMock()
    mock_tg.is_connected = MagicMock(return_value=True)
    mock_tg.session = SimpleNamespace(
        dc_id=4,
        server_address="149.154.167.91",
        port=443,
        auth_key=SimpleNamespace(key=b"\x01" * 256),
        takeout_id=None,
    )
    mock_tg.send_code_request = AsyncMock(return_value=MagicMock(phone_code_hash="internal_h", next_type=None))
    mock_tg.sign_in = AsyncMock()
    mock_tg.is_user_authorized = AsyncMock(return_value=True)
    mock_tg.get_me = AsyncMock(
        return_value=MagicMock(id=user_id, phone=phone, username="u_nat", first_name="Nat", premium=False)
    )
    return mock_tg


def test_native_routes_allow_valid_2fa_token(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)
    acc = seed_default_account(client_manager, client=None, auth_state="AUTHENTICATED")
    acc.phone = "+12025550116"
    monkeypatch.setattr(client_manager, "_accounts", {acc.account_id: acc})
    mock_tg = _mock_isolated_otp_client()
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _acc: mock_tg)
    resp = client.post(
        "/api/telegram/native/send-code",
        json={"phone": "+12025550116"},
        headers={"X-API-Key": TEST_KEY, "X-2FA-Token": issue_2fa_token()},
    )
    assert resp.status_code == 200
    assert "phone_code_hash" not in resp.json()
    assert resp.json()["challenge_id"]


def test_native_routes_require_deployer_token(monkeypatch):
    """默认主账号待验证时须提供独立的部署者凭据。"""
    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)
    monkeypatch.setattr(settings, "BOOTSTRAP_TOKEN", "unit-test-bootstrap-secret-0123456789")
    acc = seed_default_account(client_manager, client=None, auth_state="UNAUTHENTICATED")
    acc.phone = "+12025550116"
    monkeypatch.setattr(client_manager, "_accounts", {acc.account_id: acc})
    mock_tg = _mock_isolated_otp_client()
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _acc: mock_tg)
    resp = client.post(
        "/api/telegram/native/send-code",
        json={"phone": "+12025550116"},
        headers={"X-API-Key": TEST_KEY},
    )
    assert resp.status_code == 401
    resp = client.post(
        "/api/telegram/native/send-code",
        json={"phone": "+12025550116"},
        headers={"X-Bootstrap-Token": settings.BOOTSTRAP_TOKEN},
    )
    assert resp.status_code == 200
    assert resp.json()["challenge_id"]


def test_native_verify_success_mints_token(monkeypatch):
    """原生登录成功等同通过 2FA (前端 persist2FAUnlock 依赖), 同步签发会话 token。"""
    from app.db.account_repository import AccountRepository

    monkeypatch.setattr(settings, "API_KEY", TEST_KEY)
    monkeypatch.setattr(settings, "BOOTSTRAP_TOKEN", "unit-test-bootstrap-secret-0123456789")
    bootstrap_headers = {"X-Bootstrap-Token": settings.BOOTSTRAP_TOKEN}
    db_acc = AccountRepository.create(
        account_name="NativeTokenTest",
        phone="+12025550116",
        session_file="native_token_test.session",
        api_id=1,
        api_hash="dummy",
        is_active=1,
        is_default=1,
    )
    acc = client_manager.get_account(db_acc["id"])
    assert acc is not None
    acc.auth_state = "UNAUTHENTICATED"
    monkeypatch.setattr(client_manager, "_accounts", {acc.account_id: acc})
    mock_tg = _mock_isolated_otp_client()
    monkeypatch.setattr(client_manager, "create_isolated_login_client", lambda _acc: mock_tg)

    send_resp = client.post(
        "/api/telegram/native/send-code",
        json={"phone": "+12025550116"},
        headers=bootstrap_headers,
    )
    assert send_resp.status_code == 200
    cid = send_resp.json()["challenge_id"]

    resp = client.post(
        "/api/telegram/native/verify-code",
        json={"challenge_id": cid, "code": "12345"},
        headers=bootstrap_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["authenticated"] is True
    assert validate_2fa_token(resp.json()["token"])
    saved_account = AccountRepository.get_by_id(db_acc["id"])
    assert saved_account is not None
    assert saved_account["user_id"] == mock_tg.get_me.return_value.id


# ===== WebSocket 握手鉴权 (F9: 短时效 ticket 单路径, 明文 query 形态已收口) =====


def test_ws_reject_without_ticket_with_1008(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "secret_ws_0123456789abcdef")
    with pytest.raises(WebSocketDisconnect) as exc, client.websocket_connect("/ws/progress"):
        pass
    assert exc.value.code == 1008


def test_ws_reject_bad_or_expired_ticket_with_1008(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "secret_ws_0123456789abcdef")
    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect("/ws/progress?ticket=not_a_real_ticket"):
            pass
    assert exc.value.code == 1008

    expired = issue_media_ticket()
    _media_tickets[expired] = 0.0
    with pytest.raises(WebSocketDisconnect) as exc_expired:
        with client.websocket_connect(f"/ws/progress?ticket={expired}"):
            pass
    assert exc_expired.value.code == 1008


def test_ws_reject_plaintext_query_credentials_with_1008(monkeypatch):
    """F9 收口回归锁: 明文 token/tfa_token query 形态不再被 WS 握手接受"""
    monkeypatch.setattr(settings, "API_KEY", "secret_ws_0123456789abcdef")
    token = issue_2fa_token()
    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect(f"/ws/progress?token=secret_ws_0123456789abcdef&tfa_token={token}"):
            pass
    assert exc.value.code == 1008


def test_ws_accept_with_ticket(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "secret_ws_0123456789abcdef")
    with client.websocket_connect(f"/ws/progress?ticket={issue_media_ticket()}") as websocket:
        test_msg = {"type": "progress", "task_id": "ws_ticket_t1", "percent": 50.0}
        asyncio.run(connection_manager.broadcast(test_msg))
        data = websocket.receive_json()
        assert data["type"] == "progress"
        assert data["task_id"] == "ws_ticket_t1"


def test_rate_limit_returns_429_per_client(monkeypatch):
    """限流触发应返回 429 (middleware 就地转换), 而非冒泡成 500。"""
    monkeypatch.setattr(settings, "API_KEY", "")
    monkeypatch.setattr(settings, "RATE_LIMIT_PER_MINUTE", 2)
    assert client.get("/api/auth/status").status_code == 200
    assert client.get("/api/auth/status").status_code == 200
    resp = client.get("/api/auth/status")
    assert resp.status_code == 429
    assert "限流" in resp.json()["detail"]


# ==============================================================================
# Part: test_auth_security.py
# ==============================================================================
client = TestClient(app, client=("127.0.0.1", 50000))


@pytest.fixture(autouse=True)
def clean_rate_limiters():
    _request_timestamps.clear()
    _sensitive_action_timestamps.clear()
    yield
    _request_timestamps.clear()
    _sensitive_action_timestamps.clear()


def test_constant_time_compare_correctness():
    """验证恒定时间比较在各种边界输入下的准确性"""
    assert _constant_time_compare("secret_12345", "secret_12345") is True
    assert _constant_time_compare("secret_12345", "secret_12346") is False
    assert _constant_time_compare("short", "much_longer_secret_string") is False
    assert _constant_time_compare("much_longer_secret_string", "short") is False
    assert _constant_time_compare("", "") is True
    assert _constant_time_compare("", "non_empty") is False
    assert _constant_time_compare(None, "secret") is False
    assert _constant_time_compare("secret", None) is False
    assert _constant_time_compare(None, None) is False
    # 支持 bytes 与 str 互转比对
    assert _constant_time_compare(b"secret_12345", "secret_12345") is True
    assert _constant_time_compare("secret_12345", b"secret_12345") is True
    assert _constant_time_compare(b"secret_12345", b"secret_12345") is True


@pytest.mark.asyncio
async def test_verify_api_key_constant_time(monkeypatch):
    """验证 verify_api_key 采用安全比对"""
    monkeypatch.setattr(settings, "API_KEY", "super_secret_key")

    # 匹配通过
    assert await verify_api_key(header_key="super_secret_key") is True

    # 错误 Key 抛出 401
    with pytest.raises(HTTPException) as exc_info:
        await verify_api_key(header_key="wrong_key")
    assert exc_info.value.status_code == 401

    # 长度不同抛出 401
    with pytest.raises(HTTPException) as exc_info2:
        await verify_api_key(header_key="super_secret_key_extra")
    assert exc_info2.value.status_code == 401

    # 空配置时一律放行
    monkeypatch.setattr(settings, "API_KEY", "")
    assert await verify_api_key(header_key=None) is True
    assert await verify_api_key(header_key="any") is True


@pytest.mark.asyncio
async def test_verify_media_access_constant_time(monkeypatch):
    """验证 verify_media_access 复合校验逻辑 (ticket 直连 / 双 Header / 明文 query 拒绝)"""
    monkeypatch.setattr(settings, "API_KEY", "media_secret_token")
    from app.api.auth import issue_2fa_token

    valid_tfa = issue_2fa_token()

    # 双 Header 正确
    assert await verify_media_access(header_key="media_secret_token", header_token=valid_tfa) is True
    # 有效 ticket 直连 (TTL 内可重复消费)
    ticket = issue_media_ticket()
    assert await verify_media_access(ticket=ticket) is True
    assert await verify_media_access(ticket=ticket) is True

    # 错误 Key
    with pytest.raises(HTTPException) as exc:
        await verify_media_access(header_key="invalid_token", header_token=valid_tfa)
    assert exc.value.status_code == 401

    # 无效 ticket 且缺 Header
    with pytest.raises(HTTPException) as exc_ticket:
        await verify_media_access(ticket="not_a_real_ticket")
    assert exc_ticket.value.status_code == 401


def test_rate_limiter_bounded_growth(monkeypatch):
    """验证限流器内部字典规模有界且清扫不破坏活跃 IP 计数"""
    monkeypatch.setattr(settings, "RATE_LIMIT_PER_MINUTE", 60)
    _request_timestamps.clear()
    now = time.time()
    old_time = now - 120.0  # 2 分钟前 (已过期)

    # 灌入 5200 个过期假 IP
    for i in range(5200):
        _request_timestamps[f"10.0.0.{i}"] = [old_time]

    # 添加一个近期有活动的合法 IP
    active_ip = "192.168.1.100"
    _request_timestamps[active_ip] = [now - 10.0]

    # 构造请求模拟中间件触发
    mock_request = Request(
        {"type": "http", "method": "GET", "path": "/api/test", "headers": [], "client": ("192.168.1.200", 12345)}
    )

    rate_limiter_middleware(mock_request)

    # 断言容量被有效压缩在 MAX_RATE_LIMITER_IPS 内
    assert len(_request_timestamps) <= MAX_RATE_LIMITER_IPS
    # 断言活动 IP 没有被误删，且记录了新时间戳
    assert active_ip in _request_timestamps
    assert "192.168.1.200" in _request_timestamps

    # 测试全并发未过期场景 (DDoS 攻击注入 5100 个当前时间戳活跃 IP)
    _request_timestamps.clear()
    for i in range(5100):
        _request_timestamps[f"172.16.{i // 256}.{i % 256}"] = [now]

    rate_limiter_middleware(mock_request)
    assert len(_request_timestamps) <= MAX_RATE_LIMITER_IPS


def test_sensitive_action_rate_limit(monkeypatch):
    """验证 /web-2fa/send 与 /native/send-code 独立限流 (5次/分钟) 生效且 429 文案明确"""
    _sensitive_action_timestamps.clear()
    monkeypatch.setattr(settings, "API_KEY", "")

    # Mock client_manager.send_web_2fa_code 返回 True
    with patch.object(client_manager, "send_web_2fa_code", new_callable=AsyncMock) as mock_send:
        mock_send.return_value = True

        # 前 5 次正常成功
        for i in range(5):
            res = client.post("/api/telegram/web-2fa/send")
            assert res.status_code == 200, f"第 {i + 1} 次应当成功"

        # 第 6 次必须触发 429 敏感限流
        res_6 = client.post("/api/telegram/web-2fa/send")
        assert res_6.status_code == 429
        detail = res_6.json()["detail"]
        assert "敏感操作限流保护" in detail
        assert "5 次/分钟" in detail

    _sensitive_action_timestamps.clear()
    acc = seed_default_account(client_manager, client=None, auth_state="UNAUTHENTICATED")
    acc.phone = "+12025550106"
    monkeypatch.setattr(client_manager, "_accounts", {acc.account_id: acc})
    mock_tg = _mock_isolated_otp_client(phone="12025550106")
    # 验证 native/send-code 同样受独立限速保护
    with patch.object(client_manager, "create_isolated_login_client", return_value=mock_tg):
        for _i in range(5):
            res = client.post("/api/telegram/native/send-code", json={"phone": "+12025550106"})
            assert res.status_code == 200

        res_6 = client.post("/api/telegram/native/send-code", json={"phone": "+12025550106"})
        assert res_6.status_code == 429
        detail = res_6.json()["detail"]
        assert "敏感操作限流保护" in detail


def test_rate_limiter_eviction_does_not_wipe_active_caller_history():
    """验证满容量清扫不会误驱逐正在请求的现有活跃 IP，导致其限流计数被重置清零"""
    _request_timestamps.clear()
    now = time.time()
    target_ip = "192.168.1.100"

    # target_ip 已在 30s 前发送了 5 次请求 (达到限额)
    _request_timestamps[target_ip] = [now - 30.0] * 5

    # 其余 4999 个 IP 发送时间更新 (比 target_ip 晚，因此 target_ip 曾经是最旧条目)
    for i in range(MAX_RATE_LIMITER_IPS - 1):
        _request_timestamps[f"10.0.{i // 256}.{i % 256}"] = [now - 10.0]

    assert len(_request_timestamps) == MAX_RATE_LIMITER_IPS
    assert target_ip in _request_timestamps

    mock_request = Request(
        {"type": "http", "method": "GET", "path": "/api/test", "headers": [], "client": (target_ip, 12345)}
    )

    old_limit = settings.RATE_LIMIT_PER_MINUTE
    settings.RATE_LIMIT_PER_MINUTE = 5
    try:
        with pytest.raises(HTTPException) as exc_info:
            rate_limiter_middleware(mock_request)
        assert exc_info.value.status_code == 429
    finally:
        settings.RATE_LIMIT_PER_MINUTE = old_limit
        _request_timestamps.clear()


@pytest.mark.asyncio
async def test_verify_websocket_auth_ticket_only(monkeypatch):
    """验证 verify_websocket_auth 仅凭短时效 ticket 鉴权 (F9 收口: 明文 query/Headers 形态一律拒绝)"""
    from unittest.mock import AsyncMock, MagicMock

    from app.api.auth import issue_2fa_token, verify_websocket_auth

    monkeypatch.setattr(settings, "API_KEY", "ws_secret_key_123")
    valid_tfa = issue_2fa_token()

    def make_ws(query_dict, headers_dict=None):
        ws = MagicMock()
        ws.query_params = query_dict
        ws.headers = headers_dict or {}
        ws.close = AsyncMock()
        return ws

    # 1. 有效 ticket -> 放行
    ws1 = make_ws({"ticket": issue_media_ticket()})
    assert await verify_websocket_auth(ws1) is True

    # 2. 明文 query 形态 (token/api_key + tfa_token/two_fa_token) 一律拒绝, 收口关闭码 1008
    ws2 = make_ws({"api_key": "ws_secret_key_123", "two_fa_token": valid_tfa})
    assert await verify_websocket_auth(ws2) is False
    ws2.close.assert_called_with(code=1008)

    # 3. Headers 形态同样不再豁免
    ws3 = make_ws({}, {"x-api-key": "ws_secret_key_123", "x-2fa-token": valid_tfa})
    assert await verify_websocket_auth(ws3) is False
    ws3.close.assert_called_with(code=1008)

    # 4. 缺失/无效 ticket
    ws4 = make_ws({"ticket": "not_a_real_ticket"})
    assert await verify_websocket_auth(ws4) is False
    ws4.close.assert_called_with(code=1008)


def test_prune_rate_limiter_cache_edge_cases():
    """验证 _prune_rate_limiter_cache 在空列表、异常时间戳下的健壮性"""
    from app.api.auth import _prune_rate_limiter_cache

    store = {}
    now = time.time()

    # 注入空列表与极旧时间戳
    store["empty_ip"] = []
    store["old_ip"] = [now - 200]
    store["active_ip"] = [now]

    # 容量为 2，加入新 IP 触发清理
    _prune_rate_limiter_cache(store, "new_ip", now - 60, max_capacity=2)
    # empty_ip 与 old_ip 均应被成功清扫
    assert "empty_ip" not in store
    assert "old_ip" not in store
    assert "active_ip" in store


def test_web_2fa_send_preserves_500_on_send_failure(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "test_key_2fa")
    monkeypatch.setattr(client_manager, "send_web_2fa_code", AsyncMock(return_value=False))
    resp = client.post("/api/telegram/web-2fa/send", headers={"X-API-Key": "test_key_2fa"})
    assert resp.status_code == 500


def test_startup_rejects_shared_console_keys(monkeypatch):
    monkeypatch.setattr(settings, "API_ID", 100)
    monkeypatch.setattr(settings, "API_HASH", "test-hash")
    monkeypatch.setattr(settings, "BOT_TOKENS", ["1:example"])
    monkeypatch.setattr(settings, "API_KEY", "test-first-factor")
    monkeypatch.setattr(settings, "SECRET_KEY", settings.API_KEY)
    with pytest.raises(RuntimeError, match="SECRET_KEY"):
        settings.validate_required()
    monkeypatch.setattr(settings, "SECRET_KEY", "")
    settings.validate_required()
