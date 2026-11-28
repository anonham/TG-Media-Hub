"""API Layer: 公网入口控制台锁定门禁验收矩阵 (PR1 收紧公网控制台访问)

三条验收准则 (HTTP 与 WebSocket 全覆盖, 装具 TestClient(app, client=(ip, port)) 自定义对端):
1. 真回环直连 (绑定回环 + 对端回环 + 无转发痕迹) → 本机免认证全可用2. 非回环直连 (公网 / LAN / docker-proxy 网关对端) → API_KEY 留空时一律 503 console_locked / WS 10083. 来源不明与明确的公网入口 → 拒绝免认证:
   3a 非回环绑定下的回环对端 (同机反代漏传来源头) → 503 console_locked   3b 转发标记存在或对端未知 → 拒绝本机豁免。
   3c 显式公网策略下，回环反代漏传来源头仍执行公网规则。
另覆盖: 公网有效 Key 全链路 (metrics 仅 X-API-Key / openapi.json Key 可读 / 媒体票据 + Range /
WS ticket / 文档 UI 404)、未准入门禁次序 (telegram_bootstrap_required 先于 console_locked)。
"""

import copy
import os

import pytest
from _safeio import safe_write_bytes
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient

from app.api.access import bind_is_loopback, is_local_request
from app.api.auth import issue_2fa_token, issue_media_ticket
from app.config import settings
from app.core.client_manager import client_manager
from app.main import app

SAMPLE_BYTES = b"0123456789" * 10  # 100 字节, Range bytes=0-9 → 206

# 对端形态: 44444 为测试桩端口 (对端端口不参与判定)
LOCAL_PEER = ("127.0.0.1", 44444)
PUBLIC_PEER = ("203.0.113.7", 44444)  # TEST-NET-3 文档保留段
LAN_PEER = ("192.168.1.5", 44444)
DOCKER_FABRIC_PEER = ("172.17.0.1", 44444)  # docker-proxy 网关形态 (统一规则: 非回环 → 锁定)


def _local_client() -> TestClient:
    return TestClient(app, client=LOCAL_PEER)


def _peer_client(peer: tuple[str, int]) -> TestClient:
    return TestClient(app, client=peer)


@pytest.fixture
def sample_media(tmp_path, monkeypatch):
    """沙箱化 DOWNLOAD_DIR 并写入样例媒体, 返回文件名 (预览/下载 206 Range 断言用)。"""
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", str(tmp_path))
    safe_write_bytes(os.path.join(str(tmp_path), "sample.mp4"), SAMPLE_BYTES)
    return "sample.mp4"


# ==============================================================================
# Part 1: is_local_request / bind_is_loopback 单元矩阵
# ==============================================================================


def _make_request(peer: tuple[str, int] | None, headers: dict[str, str] | None = None):
    from fastapi import Request

    scope: dict = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [(name.lower().encode(), value.encode()) for name, value in (headers or {}).items()],
    }
    if peer is not None:
        scope["client"] = peer
    return Request(scope)


@pytest.mark.parametrize(
    ("bind", "peer", "headers", "expected"),
    [
        # 绑定回环 + 对端回环 (含 ::1 与 v4-mapped 解包) → 本机直连
        ("127.0.0.1", ("127.0.0.1", 12345), None, True),
        ("127.0.0.1", ("::1", 12345), None, True),
        ("127.0.0.1", ("::ffff:127.0.0.1", 12345), None, True),
        ("localhost", ("127.0.0.1", 12345), None, True),
        ("::1", ("::1", 12345), None, True),
        # 仅数字回环地址有本机豁免资格。
        ("127.0.0.1", None, None, False),
        ("127.0.0.1", ("", 50000), None, False),
        ("127.0.0.1", ("testclient", 50000), None, False),
        ("127.0.0.1", ("localhost", 50000), None, False),
        # 转发痕迹头任一存在 = 明确的公网入口 (即便对端/转发源是回环)
        ("127.0.0.1", ("127.0.0.1", 12345), {"X-Forwarded-For": "203.0.113.7"}, False),
        ("127.0.0.1", ("127.0.0.1", 12345), {"X-Forwarded-Proto": "https"}, False),
        ("127.0.0.1", ("127.0.0.1", 12345), {"X-Real-IP": "203.0.113.7"}, False),
        ("127.0.0.1", ("127.0.0.1", 12345), {"Forwarded": "for=203.0.113.7"}, False),
        # 转发标记存在即拒绝豁免。
        ("127.0.0.1", ("127.0.0.1", 12345), {"X-Forwarded-For": ""}, False),
        ("127.0.0.1", ("127.0.0.1", 12345), {"Forwarded": ""}, False),
        # 非回环对端 → 非本机 (公网 / LAN / docker 网关, 统一规则无容器豁免)
        ("127.0.0.1", ("203.0.113.7", 12345), None, False),
        ("127.0.0.1", ("192.168.1.5", 12345), None, False),
        ("127.0.0.1", ("172.17.0.1", 12345), None, False),
        # 绑定非回环 = 存在明确公网入口, 回环对端亦属「来源不明」(同机反代漏传来源头形态)
        ("0.0.0.0", ("127.0.0.1", 12345), None, False),
        ("0.0.0.0", ("203.0.113.7", 12345), None, False),
        ("::", ("::1", 12345), None, False),
        ("192.168.1.10", ("127.0.0.1", 12345), None, False),
    ],
)
def test_is_local_request_matrix(monkeypatch, bind, peer, headers, expected):
    monkeypatch.setattr(settings, "HOST", bind)
    assert bind_is_loopback() is (bind.lower() in {"127.0.0.1", "::1", "localhost"})
    assert is_local_request(_make_request(peer, headers)) is expected


# ==============================================================================
# Part 2: 准则 1 —— 真回环直连, 本机免认证全可用 (HTTP + WS)
# ==============================================================================


def test_local_open_mode_full_surface(sample_media, monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "")
    c = _local_client()

    status_resp = c.get("/api/auth/status")
    assert status_resp.status_code == 200
    assert status_resp.json()["console_locked"] is False
    assert status_resp.json()["auth_required"] is False

    assert c.get("/api/tasks").status_code == 200

    ticket_resp = c.post("/api/auth/media-ticket")
    assert ticket_resp.status_code == 200
    ticket = ticket_resp.json()["ticket"]

    range_resp = c.get(f"/api/storage/preview/sample.mp4?ticket={ticket}", headers={"Range": "bytes=0-9"})
    assert range_resp.status_code == 206
    assert range_resp.headers["content-range"] == "bytes 0-9/100"

    assert c.get("/metrics").status_code == 200  # 本机免认证下指标入口放行

    # 文档三入口本机可达 (内置入口已关闭, 由自管路由按来源放行)
    assert c.get("/docs").status_code == 200
    assert c.get("/redoc").status_code == 200
    openapi_resp = c.get("/openapi.json")
    assert openapi_resp.status_code == 200
    assert "/api/tasks" in openapi_resp.json()["paths"]

    with c.websocket_connect("/ws/progress") as ws:  # 本机 open 模式免票握手
        ws.send_text("ping")


# ==============================================================================
# Part 3: 准则 2 —— 非回环直连, API_KEY 留空时一律锁定 (HTTP + WS)
# ==============================================================================


@pytest.mark.parametrize("peer", [PUBLIC_PEER, LAN_PEER, DOCKER_FABRIC_PEER])
def test_non_loopback_direct_locked_without_key(peer, sample_media, monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "")
    c = _peer_client(peer)

    tasks_resp = c.get("/api/tasks")
    assert tasks_resp.status_code == 503
    assert tasks_resp.json()["error_code"] == "console_locked"

    assert c.post("/api/auth/media-ticket").status_code == 503  # 媒体票据入口关闭
    assert c.get("/metrics").status_code == 503  # 禁止匿名抓取指标
    assert c.get("/docs").status_code == 503
    assert c.get("/redoc").status_code == 503
    assert c.get("/openapi.json").status_code == 503

    # 有效 ticket 也不豁免锁定门禁 (预览/下载入口关闭)
    preview_resp = c.get(f"/api/storage/preview/sample.mp4?ticket={issue_media_ticket()}")
    assert preview_resp.status_code == 503
    assert preview_resp.json()["error_code"] == "console_locked"

    # 已准入 + 锁定态: 放行清单内的 telegram/status 由端点级本机判定收口 → 401
    assert c.get("/api/telegram/status").status_code == 401

    # 保留面: 首页/健康检查、最小公开状态、静态资源
    assert c.get("/").status_code == 200
    assert c.get("/favicon.ico").status_code == 204
    status_resp = c.get("/api/auth/status")
    assert status_resp.status_code == 200
    assert status_resp.json()["console_locked"] is True
    assert c.get("/static/nonexistent.js").status_code == 404  # 静态资源放行 (404 = StaticFiles 业务语义)

    with pytest.raises(WebSocketDisconnect) as ws_exc, c.websocket_connect("/ws/progress"):
        pytest.fail("公网直连未配置 API_KEY 时不应完成 WebSocket 握手")
    assert ws_exc.value.code == 1008


# ==============================================================================
# Part 4: 准则 3a —— 非回环绑定下的回环对端 = 来源不明, 拒绝免认证 (HTTP + WS)
# ==============================================================================


def test_loopback_peer_on_public_bind_is_unknown_origin(sample_media, monkeypatch):
    """同机反代漏传来源头时对端即 127.0.0.1: 非回环绑定下无法与真本机区分 → fail-closed。"""
    monkeypatch.setattr(settings, "API_KEY", "")
    monkeypatch.setattr(settings, "HOST", "0.0.0.0")
    c = _local_client()  # 回环对端

    tasks_resp = c.get("/api/tasks")
    assert tasks_resp.status_code == 503
    assert tasks_resp.json()["error_code"] == "console_locked"

    assert c.get("/metrics").status_code == 503
    assert c.get("/docs").status_code == 503

    status_resp = c.get("/api/auth/status")
    assert status_resp.status_code == 200
    assert status_resp.json()["console_locked"] is True

    with pytest.raises(WebSocketDisconnect) as ws_exc, c.websocket_connect("/ws/progress"):
        pytest.fail("来源不明 (非回环绑定 + 回环对端) 不应完成 WebSocket 握手")
    assert ws_exc.value.code == 1008


# ==============================================================================
# Part 5: 准则 3b —— 转发痕迹头 = 明确的公网入口 (HTTP + WS)
# ==============================================================================


@pytest.mark.parametrize("marks", [{"X-Forwarded-For": "203.0.113.7"}, {"X-Real-IP": "203.0.113.7"}, {"Forwarded": ""}])
def test_proxy_marked_loopback_peer_requires_key(marks, sample_media, monkeypatch):
    """转发标记即使为空，也会阻止回环对端获得本机豁免。"""
    monkeypatch.setattr(settings, "API_KEY", "")
    c = _local_client()

    tasks_resp = c.get("/api/tasks", headers=marks)
    assert tasks_resp.status_code == 503
    assert tasks_resp.json()["error_code"] == "console_locked"

    assert c.get("/metrics", headers=marks).status_code == 503

    with pytest.raises(WebSocketDisconnect) as ws_exc, c.websocket_connect("/ws/progress", headers=marks):
        pytest.fail("携带转发痕迹的请求不应免认证完成 WebSocket 握手")
    assert ws_exc.value.code == 1008


def test_public_policy_locks_loopback_proxy_without_forwarding_headers(sample_media, monkeypatch):
    monkeypatch.setattr(settings, "HOST", "127.0.0.1")
    monkeypatch.setattr(settings, "CONSOLE_REQUIRE_API_KEY", True)
    monkeypatch.setattr(settings, "API_KEY", "")
    client = _local_client()

    for path in (
        "/api/tasks",
        "/metrics",
        "/docs",
        "/redoc",
        "/openapi.json",
        f"/api/storage/preview/{sample_media}",
        f"/api/storage/download/{sample_media}",
    ):
        response = client.get(path)
        assert response.status_code == 503, path
        assert response.json()["error_code"] == "console_locked", path
    assert client.post("/api/auth/media-ticket").json()["error_code"] == "console_locked"
    assert client.get("/").status_code == 200
    assert client.get("/api/auth/status").json()["console_locked"] is True
    with pytest.raises(WebSocketDisconnect) as ws_exc, client.websocket_connect("/ws/progress"):
        pytest.fail("回环反代缺少来源头时不应获得本机免认证")
    assert ws_exc.value.code == 1008


def test_public_policy_uses_route_path_with_asgi_mount_prefix(monkeypatch):
    monkeypatch.setattr(settings, "CONSOLE_REQUIRE_API_KEY", True)
    monkeypatch.setattr(settings, "API_KEY", "")
    client = TestClient(app, root_path="/static", client=LOCAL_PEER)

    assert client.get("/static/api/auth/status").json()["console_locked"] is True
    response = client.get("/static/api/tasks")
    assert response.status_code == 503
    assert response.json()["error_code"] == "console_locked"


@pytest.mark.parametrize("path", ["/docs", "/redoc"])
def test_docs_ui_openapi_url_includes_asgi_mount_prefix(path):
    client = TestClient(app, root_path="/hub/", client=LOCAL_PEER)

    response = client.get(path)

    assert response.status_code == 200
    assert "/hub/openapi.json" in response.text


@pytest.mark.parametrize(
    ("peer", "api_key", "require_api_key"),
    [(LOCAL_PEER, "", False), (PUBLIC_PEER, "mounted_openapi_test_key", True)],
    ids=["local", "api-key"],
)
def test_openapi_schema_includes_mount_prefix_without_mutating_cache(peer, api_key, require_api_key, monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", api_key)
    monkeypatch.setattr(settings, "CONSOLE_REQUIRE_API_KEY", require_api_key)
    cached_schema = copy.deepcopy(app.openapi())
    client = TestClient(app, root_path="/hub/", client=peer)
    headers = {"X-API-Key": api_key} if api_key else {}

    response = client.get("/openapi.json", headers=headers)

    assert response.status_code == 200
    assert response.json()["servers"][0] == {"url": "/hub"}
    assert app.openapi() == cached_schema


# ==============================================================================
# Part 6: 公网有效 Key 全链路 (指标仅 X-API-Key / 文档矩阵 / 票据 + Range / WS ticket)
# ==============================================================================


@pytest.mark.parametrize("peer", [PUBLIC_PEER, LOCAL_PEER], ids=["public-direct", "loopback-proxy-no-headers"])
def test_public_keyed_mode_full_surface(peer, sample_media, monkeypatch):
    secret = "public_keyed_matrix_key_0123456789"
    monkeypatch.setattr(settings, "API_KEY", secret)
    monkeypatch.setattr(settings, "SECRET_KEY", "")
    monkeypatch.setattr(settings, "CONSOLE_REQUIRE_API_KEY", peer == LOCAL_PEER)
    c = _peer_client(peer)

    status_resp = c.get("/api/auth/status")
    assert status_resp.status_code == 200
    assert status_resp.json()["console_locked"] is False
    assert status_resp.json()["auth_required"] is True

    # 业务 API: 无头 401 → 仅 Key 403 (2FA) → 双头 200
    assert c.get("/api/tasks").status_code == 401
    assert c.get("/api/tasks", headers={"X-API-Key": secret}).status_code == 403
    dual_headers = {"X-API-Key": secret, "X-2FA-Token": issue_2fa_token()}
    assert c.get("/api/tasks", headers=dual_headers).status_code == 200

    # 媒体票据 + Range 直连
    assert c.post("/api/auth/media-ticket").status_code == 401
    assert c.get(f"/api/storage/preview/{sample_media}").status_code == 401
    assert c.get(f"/api/storage/download/{sample_media}").status_code == 401
    ticket_resp = c.post("/api/auth/media-ticket", headers=dual_headers)
    assert ticket_resp.status_code == 200
    ticket = ticket_resp.json()["ticket"]
    range_resp = c.get(f"/api/storage/preview/sample.mp4?ticket={ticket}", headers={"Range": "bytes=0-9"})
    assert range_resp.status_code == 206
    assert range_resp.headers["content-range"] == "bytes 0-9/100"

    # 指标入口受保护且无需 2FA (Prometheus 抓取语义)
    assert c.get("/metrics").status_code == 401
    assert c.get("/metrics", headers={"X-API-Key": secret}).status_code == 200

    # 文档 UI 对公网隐藏; schema 凭 Key 程序化可读 (与 /metrics 同语义)
    for path in ("/docs", "/redoc"):
        assert c.get(path).status_code == 404
        assert c.get(path, headers={"X-API-Key": secret}).status_code == 404
    assert c.get("/openapi.json").status_code == 404
    assert c.get("/openapi.json", headers={"X-API-Key": secret + "_wrong"}).status_code == 401
    openapi_resp = c.get("/openapi.json", headers={"X-API-Key": secret})
    assert openapi_resp.status_code == 200
    assert "/api/tasks" in openapi_resp.json()["paths"]

    # WS: 有效票握手成功, 坏票 1008
    with c.websocket_connect(f"/ws/progress?ticket={issue_media_ticket()}") as ws:
        ws.send_text("ping")
    with pytest.raises(WebSocketDisconnect) as ws_exc, c.websocket_connect("/ws/progress"):
        pytest.fail("公网键控模式下匿名请求不应完成 WebSocket 握手")
    assert ws_exc.value.code == 1008
    with pytest.raises(WebSocketDisconnect) as ws_exc, c.websocket_connect("/ws/progress?ticket=not_a_real_ticket"):
        pytest.fail("公网键控模式下无效 ticket 不应完成 WebSocket 握手")
    assert ws_exc.value.code == 1008


# ==============================================================================
# Part 7: 未准入门禁次序 (受限引导门禁先于控制台锁定门禁) + 放行清单端点收口
# ==============================================================================


def test_unadmitted_gate_precedes_console_lock(monkeypatch):
    """两道门禁同时成立时，受限引导优先且部署者认证仍可用。"""
    token = "test_deployer_bootstrap_token_012345"
    monkeypatch.setattr(settings, "API_KEY", "")
    monkeypatch.setattr(settings, "CONSOLE_REQUIRE_API_KEY", True)
    monkeypatch.setattr(settings, "BOOTSTRAP_TOKEN", token)
    monkeypatch.setattr(client_manager, "is_default_account_admitted", lambda: False)
    c = _local_client()

    for path in ("/api/tasks", "/metrics", "/openapi.json", "/api/storage/preview/sample.mp4"):
        response = c.get(path)
        assert response.status_code == 503, path
        assert response.json()["error_code"] == "telegram_bootstrap_required", path

    with pytest.raises(WebSocketDisconnect) as ws_exc, c.websocket_connect("/ws/progress"):
        pytest.fail("默认账号未准入时不应完成 WebSocket 握手")
    assert ws_exc.value.code == 1008

    status_resp = c.get("/api/auth/status")
    assert status_resp.status_code == 200
    assert status_resp.json()["restricted"] is True
    assert status_resp.json()["console_locked"] is True
    assert status_resp.json()["bootstrap_authorized"] is False
    assert c.post("/api/telegram/native/send-code", json={"phone": "+12025550116"}).status_code == 401
    authorized = c.post("/api/telegram/bootstrap/auth", headers={"X-Bootstrap-Token": token})
    assert authorized.status_code == 200
    assert c.get("/api/tasks").json()["error_code"] == "telegram_bootstrap_required"


def test_metrics_limits_missing_and_invalid_keys_but_keeps_valid_scrapes(monkeypatch):
    key = "test_metrics_key_0123456789"
    monkeypatch.setattr(settings, "API_KEY", key)
    monkeypatch.setattr(settings, "RATE_LIMIT_PER_MINUTE", 2)
    client = _peer_client(PUBLIC_PEER)

    assert client.get("/metrics").status_code == 401
    assert client.get("/metrics", headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get("/metrics").status_code == 429
    for _ in range(3):
        assert client.get("/metrics", headers={"X-API-Key": key}).status_code == 200
    assert client.post("/metrics", headers={"X-API-Key": key}).status_code == 429


def test_admitted_locked_public_native_login_requires_key(monkeypatch):
    """已准入 + 公网锁定态: 放行清单内的 native 登录入口由端点级本机判定收口 → 401 (防重绑)。"""
    monkeypatch.setattr(settings, "API_KEY", "")
    c = _peer_client(PUBLIC_PEER)
    resp = c.post("/api/telegram/native/send-code", json={"phone": "+12025550116"})
    assert resp.status_code == 401
    assert "公网入口" in resp.json()["detail"]
