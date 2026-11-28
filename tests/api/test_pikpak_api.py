"""/api/pikpak/* 路由测试: 双依赖整组挂载 / 路由集固化 / publish 落库红线 / 分页透传 / 设备码状态机。

范式照 test_upload_concurrency_and_speed.py:202-254 (TestClient + X-API-Key + issue_2fa_token 签发 X-2FA-Token)。
service 层 mock 于 app.api.pikpak 命名空间; 罐头直链一律 cdn.fake.test + FAKESIGN 构造值;
测试期 API Key 为运行时随机生成值 (非源码凭据字面量, 仅 monkeypatch 进 settings.API_KEY)。
"""

import asyncio
import json
import time
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.db.task_repository import TaskQueueRepository
from app.main import app
from app.services import pikpak_service as pikpak_service_module

client = TestClient(app, client=("127.0.0.1", 50000))

PIKPAK_ENDPOINTS = [
    ("GET", "/api/pikpak/auth/status"),
    ("POST", "/api/pikpak/auth/device/start"),
    ("POST", "/api/pikpak/auth/device/poll"),
    ("GET", "/api/pikpak/files/list"),
    ("POST", "/api/pikpak/inspect"),
    ("POST", "/api/pikpak/publish"),
]


def _set_api_key(monkeypatch) -> str:
    """测试期 API Key: 每次运行时随机生成 (非源码凭据字面量), 仅 monkeypatch 进 settings.API_KEY。"""
    key = "pk-" + uuid.uuid4().hex
    monkeypatch.setattr(settings, "API_KEY", key)
    return key


def _auth_headers(monkeypatch) -> dict:
    """签发双鉴权头 (X-API-Key + issue_2fa_token 生成的 X-2FA-Token)。"""
    key = _set_api_key(monkeypatch)
    from app.api.auth import issue_2fa_token

    headers = {"X-API-Key": key}
    token = issue_2fa_token()
    if token:
        headers["X-2FA-Token"] = token
    return headers


def test_all_routes_require_api_key(monkeypatch):
    """双依赖第 1 层: 无 X-API-Key → 全部 6 端点 401 (整组挂载生效)。"""
    _set_api_key(monkeypatch)
    for method, path in PIKPAK_ENDPOINTS:
        resp = client.request(method, path, json={})
        assert resp.status_code == 401, f"{method} {path} 未拒绝无凭据请求: {resp.status_code}"


def test_all_routes_require_2fa(monkeypatch):
    """双依赖第 2 层: 仅带 API Key (无 X-2FA-Token) → 全部 6 端点 403 (与 401 区分凭证层级)。"""
    key = _set_api_key(monkeypatch)
    for method, path in PIKPAK_ENDPOINTS:
        resp = client.request(method, path, json={}, headers={"X-API-Key": key})
        assert resp.status_code == 403, f"{method} {path} 2FA 校验未生效: {resp.status_code}"


def test_route_set_frozen_no_token_endpoints(monkeypatch):
    """/api/pikpak 路由集精确枚举 (经 openapi.json, 新路由挂载为 _IncludedRouter)。"""
    _auth_headers(monkeypatch)
    openapi = client.get("/openapi.json").json()
    pikpak_paths = {
        p: sorted(m.upper() for m in methods) for p, methods in openapi["paths"].items() if p.startswith("/api/pikpak")
    }
    assert pikpak_paths == {
        "/api/pikpak/auth/status": ["GET"],
        "/api/pikpak/auth/device/start": ["POST"],
        "/api/pikpak/auth/device/poll": ["POST"],
        "/api/pikpak/files/list": ["GET"],
        "/api/pikpak/inspect": ["POST"],
        "/api/pikpak/publish": ["POST"],
    }
    # 无任何 token 修改 / 写 .env 端点 (路由集固化即红线断言)


def test_publish_creates_task_params_safely(monkeypatch):
    """/publish 落库断言: task_type/params_json (含 auto_cleanup_mode 默认 archive、remote ids);
    params_json 无直链/Token 字段; remote_files 键集 ⊆ schema 白名单。"""
    headers = _auth_headers(monkeypatch)
    payload = {
        "remote_files": [
            {
                "file_id": "F1",
                "name": "video.mp4",
                "size_bytes": 5242880,
                "parent_id": "P0",
                "duration_s": "12",
                "audit": {"status": "OK"},
                "thumbnail_link": "https://cdn.fake.test/thumb.png",
            }
        ],
        "target_channel": "-100888",
        "upload_concurrency": 4,
        "force_republish": True,
    }
    resp = client.post("/api/pikpak/publish", json=payload, headers=headers)
    assert resp.status_code == 200, resp.text
    task_id = resp.json()["task_id"]

    task = TaskQueueRepository.get_by_id(task_id)
    assert task is not None
    assert task["task_type"] == "pikpak_publish"
    params = task["params"]
    assert params["target_channel"] == "-100888"
    assert params["auto_cleanup_mode"] == "archive"  # 默认 archive
    assert [f["file_id"] for f in params["remote_files"]] == ["F1"]
    assert params["upload_concurrency"] == 4
    assert params["force_republish"] is True  # 包C: 强制重传透传落库

    # 红线: params_json 不含 Token/直链签名值; 键集白名单
    params_json = json.dumps(params, ensure_ascii=False)
    assert "FAKESIGN" not in params_json
    assert "token" not in params_json.lower()
    allowed_keys = {
        "file_id",
        "name",
        "is_dir",
        "size_bytes",
        "audit",
        "thumbnail_link",
        "duration_s",
        "width",
        "height",
        "parent_id",
    }
    assert set(params["remote_files"][0].keys()) <= allowed_keys
    # thumbnail_link 为封面展示字段 (schema 白名单内), 下载直链字段不存在
    assert "download_url" not in params_json.lower()


def test_publish_accepts_and_persists_folder_fields(monkeypatch):
    """包2: /publish 接受 is_folder_publish、folder_name 与 dir_tree 并透传落库至 task_params。"""
    headers = _auth_headers(monkeypatch)
    payload = {
        "remote_files": [
            {
                "file_id": "F1",
                "name": "video.mp4",
                "size_bytes": 5242880,
                "parent_id": "D1",
            }
        ],
        "target_channel": "-100888",
        "is_folder_publish": True,
        "folder_name": "一字马汁汁",
        "dir_tree": {"D1": "一字马汁汁", "D2": "一字马汁汁/sub"},
    }
    resp = client.post("/api/pikpak/publish", json=payload, headers=headers)
    assert resp.status_code == 200, resp.text
    task_id = resp.json()["task_id"]

    task = TaskQueueRepository.get_by_id(task_id)
    assert task is not None
    params = task["params"]
    assert params["is_folder_publish"] is True
    assert params["folder_name"] == "一字马汁汁"
    assert params["dir_tree"] == {"D1": "一字马汁汁", "D2": "一字马汁汁/sub"}


def test_publish_defaults_folder_fields(monkeypatch):
    """包2: /publish 未传 is_folder_publish、folder_name 与 dir_tree 时采用安全默认值。"""
    headers = _auth_headers(monkeypatch)
    payload = {
        "remote_files": [
            {
                "file_id": "F2",
                "name": "video2.mp4",
                "size_bytes": 1024,
                "parent_id": "P0",
            }
        ],
        "target_channel": "-100888",
    }
    resp = client.post("/api/pikpak/publish", json=payload, headers=headers)
    assert resp.status_code == 200, resp.text
    task_id = resp.json()["task_id"]

    task = TaskQueueRepository.get_by_id(task_id)
    assert task is not None
    params = task["params"]
    assert params["is_folder_publish"] is False
    assert params["folder_name"] is None
    assert params["dir_tree"] == {}


def test_publish_null_dir_tree_normalizes_to_dict(monkeypatch):
    """包2: /publish 显式传入 null 的 dir_tree 与 folder_name 时规范化为 {} 与 null。"""
    headers = _auth_headers(monkeypatch)
    payload = {
        "remote_files": [
            {
                "file_id": "F3",
                "name": "video3.mp4",
                "size_bytes": 1024,
                "parent_id": "P0",
            }
        ],
        "target_channel": "-100888",
        "is_folder_publish": False,
        "folder_name": None,
        "dir_tree": None,
    }
    resp = client.post("/api/pikpak/publish", json=payload, headers=headers)
    assert resp.status_code == 200, resp.text
    task_id = resp.json()["task_id"]

    task = TaskQueueRepository.get_by_id(task_id)
    assert task is not None
    params = task["params"]
    assert params["is_folder_publish"] is False
    assert params["folder_name"] is None
    assert params["dir_tree"] == {}


def test_publish_rejects_directory_items(monkeypatch):
    """包A 护栏: remote_files 含 is_dir=True 条目 → 422 (前端子树选择后载荷只应含叶子快照)。"""
    headers = _auth_headers(monkeypatch)
    payload = {
        "remote_files": [
            {"file_id": "D1", "name": "Test", "is_dir": True, "size_bytes": 0, "parent_id": ""},
            {"file_id": "F1", "name": "v.mp4", "is_dir": False, "size_bytes": 10, "parent_id": "D1"},
        ],
        "target_channel": "-100888",
    }
    resp = client.post("/api/pikpak/publish", json=payload, headers=headers)
    assert resp.status_code == 422, f"目录条目必须被 422 拒绝: {resp.status_code}"
    detail = str(resp.json().get("detail", ""))
    assert "目录条目" in detail and "Test" in detail

    # 响应体同样无敏感字段
    body = json.dumps(resp.json(), ensure_ascii=False)
    assert "FAKESIGN" not in body


def test_files_list_passthrough_and_pagination(monkeypatch):
    """files/list service mock 透传: 字段映射 + next_page_token 分页 + mock 传参断言。"""
    headers = _auth_headers(monkeypatch)
    from app.services.pikpak_service import ParsedFileItem

    calls = []

    async def fake_list(folder_id="", page_token=None, **kwargs):
        calls.append((folder_id, page_token))
        if page_token is None:
            items = [
                ParsedFileItem(
                    file_id="F1",
                    name="a.mp4",
                    is_dir=False,
                    size_bytes=1024,
                    audit={"status": "OK"},
                    thumbnail_link=None,
                    duration_s="12",
                    width="1920",
                    height="1080",
                    parent_id="",
                ),
                ParsedFileItem(
                    file_id="D1",
                    name="sub",
                    is_dir=True,
                    size_bytes=0,
                    audit={},
                    thumbnail_link=None,
                    duration_s=None,
                    width=None,
                    height=None,
                    parent_id="",
                ),
            ]
            return items, "T2"
        return [], None

    monkeypatch.setattr("app.api.pikpak.list_files_raw", fake_list)

    resp = client.get("/api/pikpak/files/list", headers=headers)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert calls == [("", None)]
    assert data["next_page_token"] == "T2"
    assert len(data["items"]) == 2
    item = data["items"][0]
    assert item["file_id"] == "F1" and item["duration_s"] == "12"  # CLI 字符串语义保持
    assert item["audit"] == {"status": "OK"}
    assert data["items"][1]["is_dir"] is True

    # 带 folder_id + page_token 二次调用透传
    resp = client.get("/api/pikpak/files/list", params={"folder_id": "P9", "page_token": "T2"}, headers=headers)
    assert resp.status_code == 200
    assert calls[-1] == ("P9", "T2")


def _fake_classify(outcome):
    """classify_auth_failure 罐头 (API 层归因 mock 用)"""

    async def _classify():
        return outcome

    return _classify


def test_auth_status_tristate(monkeypatch):
    """auth/status 三态契约 (恒 200): logged_in 兼容镜像 state; 认证失败经探针交叉归因
    → not_bound / unreachable 二分 (不再坍缩为统一「未登录」)。"""
    headers = _auth_headers(monkeypatch)

    async def fake_status(**kwargs):
        return {
            "logged_in": True,
            "user_id": "u1",
            "name": "tester",
            "email": "t@fake.test",
            "vip": {"type": "Platinum"},
            "storage": {"total": 2199023255552, "used": 10885718179295, "in_trash": 1024},
            "additional_quotas": {"trash_duration": 25200},
            "token_source": "env",
        }

    monkeypatch.setattr("app.api.pikpak.auth_status", fake_status)
    resp = client.get("/api/pikpak/auth/status", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["logged_in"] is True
    assert data["state"] == "logged_in"
    assert data["state_reason"] is None
    assert data["token_source"] == "env"  # P4 契约: env 时前端禁扫码/禁编辑
    assert data["storage"]["used_bytes"] == 10885718179295
    assert data["trash_duration_s"] == 25200

    async def not_logged_in(**kwargs):
        raise pikpak_service_module.PikPakAuthError("未登录")

    monkeypatch.setattr(settings, "PIKPAK_TOKEN", None)
    monkeypatch.setattr("app.api.pikpak.auth_status", not_logged_in)
    monkeypatch.setattr("app.api.pikpak.classify_auth_failure", _fake_classify("not_bound"))
    resp = client.get("/api/pikpak/auth/status", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["logged_in"] is False
    assert data["state"] == "not_bound"
    assert data["state_reason"]
    assert data["token_source"] == "stored"

    monkeypatch.setattr("app.api.pikpak.classify_auth_failure", _fake_classify("transient"))
    resp = client.get("/api/pikpak/auth/status", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["logged_in"] is False
    assert data["state"] == "unreachable"
    assert data["state_reason"]


def test_auth_status_transient_direct_no_probe(monkeypatch):
    """transient 直达 → 200 unreachable 且不触发交叉归因探针 (错误本身即网络不可达实证)。"""
    headers = _auth_headers(monkeypatch)
    classify_calls = []

    async def fake_classify():
        classify_calls.append(1)
        return "transient"

    async def transient(**kwargs):
        raise pikpak_service_module.PikPakTransientError("PikPak 网络瞬断 (REQUEST_FAILED)")

    monkeypatch.setattr("app.api.pikpak.auth_status", transient)
    monkeypatch.setattr("app.api.pikpak.classify_auth_failure", fake_classify)
    resp = client.get("/api/pikpak/auth/status", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["state"] == "unreachable" and data["logged_in"] is False
    assert "REQUEST_FAILED" in data["state_reason"]
    assert classify_calls == [], "transient 直达路径严禁触发交叉归因探针"


def test_auth_status_cli_error_unreachable(monkeypatch):
    """CLI 异常 (超时/解析失败/未知码) → 200 unreachable (原 502 双轨废除)。"""
    headers = _auth_headers(monkeypatch)

    async def cli_error(**kwargs):
        raise pikpak_service_module.PikPakCliError("PikPak CLI 调用失败 (rc=2, code=UNKNOWN)")

    monkeypatch.setattr("app.api.pikpak.auth_status", cli_error)
    resp = client.get("/api/pikpak/auth/status", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["state"] == "unreachable" and data["logged_in"] is False
    assert data["state_reason"]


@pytest.mark.parametrize(
    "exc_cls,verdict,expected_state",
    [
        ("PikPakAuthError", "not_bound", "not_bound"),
        ("PikPakAuthError", "transient", "unreachable"),
        ("PikPakTransientError", None, "unreachable"),
        ("PikPakCliError", None, "unreachable"),
        ("PikPakError", None, "unreachable"),
    ],
)
def test_auth_status_never_5xx(monkeypatch, exc_cls, verdict, expected_state):
    """恒 200 契约固化: 五类失败形态全部 200 三态 (原 502 双轨废除)。"""
    headers = _auth_headers(monkeypatch)
    monkeypatch.setattr(settings, "PIKPAK_TOKEN", None)

    async def boom(**kwargs):
        raise getattr(pikpak_service_module, exc_cls)("canned failure")

    monkeypatch.setattr("app.api.pikpak.auth_status", boom)
    if verdict is not None:
        monkeypatch.setattr("app.api.pikpak.classify_auth_failure", _fake_classify(verdict))
    resp = client.get("/api/pikpak/auth/status", headers=headers)
    assert resp.status_code == 200, f"{exc_cls} 必须恒 200 三态"
    data = resp.json()
    assert data["state"] == expected_state
    assert data["logged_in"] is False


def test_auth_status_reason_redacted(monkeypatch):
    """state_reason 脱敏红线: 直链签名参数值 (X-Sign/auth_key/expires) 擦除后才能进响应。"""
    headers = _auth_headers(monkeypatch)

    async def leaky(**kwargs):
        raise pikpak_service_module.PikPakError(
            "transient https://cdn.fake.test/dl/1?X-Sign=FAKESIGN123&auth_key=KEYXYZ&expires=999"
        )

    monkeypatch.setattr("app.api.pikpak.auth_status", leaky)
    resp = client.get("/api/pikpak/auth/status", headers=headers)
    assert resp.status_code == 200
    reason = resp.json()["state_reason"] or ""
    assert "FAKESIGN123" not in reason and "KEYXYZ" not in reason and "999" not in reason
    assert "auth_key=" in reason, "参数名保留仅值擦除 (redact_text 契约)"


def test_schema_validation_422(monkeypatch):
    """Pydantic 校验: 非法 auto_cleanup_mode / 空 remote_files / 缺 file_id → 422。"""
    headers = _auth_headers(monkeypatch)
    base = {"remote_files": [{"file_id": "F1"}], "target_channel": "-100888"}

    bad_mode = dict(base, auto_cleanup_mode="purge")
    assert client.post("/api/pikpak/publish", json=bad_mode, headers=headers).status_code == 422

    empty_files = dict(base, remote_files=[])
    assert client.post("/api/pikpak/publish", json=empty_files, headers=headers).status_code == 422

    no_file_id = dict(base, remote_files=[{"name": "x.mp4"}])
    assert client.post("/api/pikpak/publish", json=no_file_id, headers=headers).status_code == 422

    bad_dir_tree = dict(base, dir_tree="not-a-dict")
    assert client.post("/api/pikpak/publish", json=bad_dir_tree, headers=headers).status_code == 422

    bad_folder_flag = dict(base, is_folder_publish="not-a-bool")
    assert client.post("/api/pikpak/publish", json=bad_folder_flag, headers=headers).status_code == 422

    assert client.post("/api/pikpak/publish", json={"target_channel": "-100888"}, headers=headers).status_code == 422


def test_device_flow_two_phase_bounded_poll(monkeypatch):
    """设备码两段式: start 罐头 → poll 有界服务端轮询 (pending×2 → success); 恒 pending → timed_out。"""
    headers = _auth_headers(monkeypatch)

    async def fake_start(**kwargs):
        return {
            "device_code": "DC123",
            "verification_uri": "https://fake.test/activate",
            "user_code": "ABCD-EFGH",
            "expires_in": 300,
            "interval": 1,
        }

    monkeypatch.setattr("app.api.pikpak.device_login_start", fake_start)

    poll_calls = {"n": 0}

    async def fake_poll(device_code, **kwargs):
        poll_calls["n"] += 1
        assert device_code == "DC123"
        if poll_calls["n"] <= 2:
            return {"status": "pending"}  # 非 success 状态「待校准」: 罐头用 pending 占位
        return {"status": "success", "name": "tester", "email": "t@fake.test"}

    monkeypatch.setattr("app.api.pikpak.device_login_poll", fake_poll)

    resp = client.post("/api/pikpak/auth/device/start", json={}, headers=headers)
    assert resp.status_code == 200
    start = resp.json()
    assert start["user_code"] == "ABCD-EFGH" and start["device_code"] == "DC123"

    resp = client.post(
        "/api/pikpak/auth/device/poll",
        json={
            "device_code": "DC123",
            "interval_s": 0.5,
            "max_wait_s": 3.0,
        },
        headers=headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "success" and data["logged_in"] is True
    assert data["user_name"] == "tester"
    assert poll_calls["n"] == 3

    # 恒 pending → 服务端有界等待后 timed_out (前端可继续轮询)
    poll_calls["n"] = 0

    async def always_pending(device_code, **kwargs):
        return {"status": "pending"}

    monkeypatch.setattr("app.api.pikpak.device_login_poll", always_pending)
    resp = client.post(
        "/api/pikpak/auth/device/poll",
        json={
            "device_code": "DC123",
            "interval_s": 0.5,
            "max_wait_s": 1.2,
        },
        headers=headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["timed_out"] is True and data["logged_in"] is False
    assert data["status"] == "pending"

    # 终止态 (罐头 expired, 待校准) → 立即返回不再轮询
    async def expired(device_code, **kwargs):
        return {"status": "expired"}

    monkeypatch.setattr("app.api.pikpak.device_login_poll", expired)
    resp = client.post(
        "/api/pikpak/auth/device/poll",
        json={
            "device_code": "DC123",
            "interval_s": 0.5,
            "max_wait_s": 3.0,
        },
        headers=headers,
    )
    assert resp.json()["status"] == "expired"
    assert resp.json()["timed_out"] is False


def test_files_list_auth_error_returns_424_not_401(monkeypatch):
    """防鉴权污染断言: 当底层 CLI 抛出 PikPakAuthError 时，接口必须返回 424 而绝非 401。"""
    headers = _auth_headers(monkeypatch)

    async def fake_auth_error(*args, **kwargs):
        raise pikpak_service_module.PikPakAuthError("CLI 未登录凭据缺失")

    monkeypatch.setattr("app.api.pikpak.list_files_raw", fake_auth_error)
    resp = client.get("/api/pikpak/files/list", headers=headers)
    assert resp.status_code == 424, f"期望返回 424 Failed Dependency, 实际: {resp.status_code}"
    assert "401" not in str(resp.status_code)
    assert "PikPak 错误: CLI 未登录凭据缺失" in resp.json()["detail"]


def test_files_list_refresh_param_bypasses_cache(monkeypatch):
    """强制刷新断言: GET /files/list?refresh=true 触发 invalidate_folder_cache 并以 use_cache=False 调用。"""
    headers = _auth_headers(monkeypatch)
    invalidated = []
    list_calls = []

    def fake_invalidate(folder_id: str):
        invalidated.append(folder_id)
        return 1

    async def fake_list(folder_id="", page_token=None, *, use_cache=True, **kwargs):
        list_calls.append((folder_id, page_token, use_cache))
        return [], None

    monkeypatch.setattr("app.api.pikpak.invalidate_folder_cache", fake_invalidate)
    monkeypatch.setattr("app.api.pikpak.list_files_raw", fake_list)

    resp = client.get("/api/pikpak/files/list?folder_id=F888&refresh=true", headers=headers)
    assert resp.status_code == 200
    assert invalidated == ["F888"]
    assert list_calls == [("F888", None, False)]


def test_parse_size_to_bytes_all_formats():
    """parse_size_to_bytes: 兼容 int/float/纯数字字符串与真实 CLI 带单位字符串 (10.00 TB / 0 B / 512 MB 等)。"""
    from app.api.pikpak import parse_size_to_bytes

    # 1. 整数 / 浮点数 / 纯数字字符串 (Mock 数据兼容)
    assert parse_size_to_bytes(0) == 0
    assert parse_size_to_bytes(1024) == 1024
    assert parse_size_to_bytes(2199023255552) == 2199023255552
    assert parse_size_to_bytes(1024.0) == 1024
    assert parse_size_to_bytes("0") == 0
    assert parse_size_to_bytes("1024") == 1024
    assert parse_size_to_bytes("2199023255552") == 2199023255552

    # 2. 真实 CLI 格式字符串
    assert parse_size_to_bytes("0 B") == 0
    assert parse_size_to_bytes("512 MB") == 512 * 1024 * 1024
    assert parse_size_to_bytes("10.00 TB") == 10 * (1024**4)
    assert parse_size_to_bytes("9.90 TB") == int(9.90 * (1024**4))
    assert parse_size_to_bytes("1.5 GB") == int(1.5 * (1024**3))
    assert parse_size_to_bytes("64 KB") == 64 * 1024
    assert parse_size_to_bytes("1024 KiB") == 1024 * 1024

    # 3. 边界值与非法输入容错
    assert parse_size_to_bytes(None) is None
    assert parse_size_to_bytes(True) is None
    assert parse_size_to_bytes(False) is None
    assert parse_size_to_bytes("") is None
    assert parse_size_to_bytes("   ") is None
    assert parse_size_to_bytes("invalid_size") is None
    assert parse_size_to_bytes(-50) == 0
    assert parse_size_to_bytes("-10 MB") is None


def test_auth_status_parses_cli_unit_strings(monkeypatch):
    """auth/status: 真实 CLI 单位字符串被正确解析为整数字节, 且支持 ?force=true 透传。"""
    headers = _auth_headers(monkeypatch)
    force_values = []

    async def fake_status(force: bool = False, **kwargs):
        force_values.append(force)
        return {
            "logged_in": True,
            "user_id": "u_real",
            "name": "real_user",
            "email": "real@fake.test",
            "vip": {"type": "Platinum"},
            "storage": {
                "total": "10.00 TB",
                "used": "512 MB",
                "in_trash": "0 B",
            },
            "additional_quotas": {"trash_duration": 86400},
            "token_source": "stored",
        }

    monkeypatch.setattr("app.api.pikpak.auth_status", fake_status)
    resp = client.get("/api/pikpak/auth/status", headers=headers)
    assert resp.status_code == 200
    assert force_values == [False]
    data = resp.json()
    assert data["logged_in"] is True
    storage = data["storage"]
    assert storage["total_bytes"] == 10 * (1024**4)
    assert storage["used_bytes"] == 512 * 1024 * 1024
    assert storage["in_trash_bytes"] == 0

    # 验证 ?force=true 透传至 service
    resp_force = client.get("/api/pikpak/auth/status?force=true", headers=headers)
    assert resp_force.status_code == 200
    assert force_values == [False, True]


@pytest.mark.asyncio
async def test_auth_status_60s_memory_cache(monkeypatch):
    """auth_status 60s 内存 TTL 缓存治理: force=False 命中快照, force=True 穿透, reset 清理。"""
    pikpak_service_module.reset_files_cache_for_tests()
    cli_calls = {"n": 0}

    async def fake_run_cli(args, **kwargs):
        cli_calls["n"] += 1
        return {
            "logged_in": True,
            "user_id": f"u{cli_calls['n']}",
            "name": "tester",
            "email": "t@fake.test",
            "storage": {"total": "10.00 TB", "used": "1.00 GB", "in_trash": "0 B"},
        }

    monkeypatch.setattr(pikpak_service_module, "_run_cli", fake_run_cli)

    # 1. 首次调用: 缓存未命中, 调用 CLI
    res1 = await pikpak_service_module.auth_status(force=False)
    assert cli_calls["n"] == 1
    assert res1["user_id"] == "u1"

    # 2. 60s 内第二次调用: 默认命中内存缓存快照, 零 CLI 调用
    res2 = await pikpak_service_module.auth_status(force=False)
    assert cli_calls["n"] == 1
    assert res2["user_id"] == "u1"

    # 3. force=True 强制刷新: 穿透缓存并更新快照
    res3 = await pikpak_service_module.auth_status(force=True)
    assert cli_calls["n"] == 2
    assert res3["user_id"] == "u2"

    # 4. reset_files_cache_for_tests() 清空缓存
    pikpak_service_module.reset_files_cache_for_tests()
    res4 = await pikpak_service_module.auth_status(force=False)
    assert cli_calls["n"] == 3
    assert res4["user_id"] == "u3"


def test_publish_split_strategy_contract_and_persistence(monkeypatch):
    """测试 /api/pikpak/publish 接收 split_strategy 强类型枚举与落库持久化，支持回退系统默认与非法 422 拒绝。"""
    headers = _auth_headers(monkeypatch)

    base_payload = {
        "remote_files": [
            {
                "file_id": "F_STRAT",
                "name": "big_movie.mp4",
                "size_bytes": 5000000000,
                "parent_id": "P0",
            }
        ],
        "target_channel": "-100888",
        "oversize_policy": "split",
        "send_placeholder_card": True,
    }

    # 1. 显式传递 vsse_with_fallback
    payload_vsse = dict(base_payload, split_strategy="vsse_with_fallback")
    resp = client.post("/api/pikpak/publish", json=payload_vsse, headers=headers)
    assert resp.status_code == 200, resp.text
    t_id1 = resp.json()["task_id"]
    t1 = TaskQueueRepository.get_by_id(t_id1)
    assert t1["params"]["split_strategy"] == "vsse_with_fallback"
    assert t1["params"]["send_placeholder_card"] is True

    # 2. 显式传递 fmp4_universal
    payload_fmp4 = dict(base_payload, split_strategy="fmp4_universal")
    resp = client.post("/api/pikpak/publish", json=payload_fmp4, headers=headers)
    assert resp.status_code == 200, resp.text
    t_id2 = resp.json()["task_id"]
    t2 = TaskQueueRepository.get_by_id(t_id2)
    assert t2["params"]["split_strategy"] == "fmp4_universal"

    # 3. 缺省 split_strategy 时自动取系统设置兜底 (默认 vsse_with_fallback)
    payload_default = dict(base_payload)
    resp = client.post("/api/pikpak/publish", json=payload_default, headers=headers)
    assert resp.status_code == 200, resp.text
    t_id3 = resp.json()["task_id"]
    t3 = TaskQueueRepository.get_by_id(t_id3)
    assert t3["params"]["split_strategy"] in ("vsse_with_fallback", "fmp4_universal")

    # 4. 非法枚举值拒绝 (422 Unprocessable Entity)
    payload_invalid = dict(base_payload, split_strategy="invalid_engine_xxx")
    resp = client.post("/api/pikpak/publish", json=payload_invalid, headers=headers)
    assert resp.status_code == 422


def test_pikpak_device_poll_respects_deadline_and_caps_timeout(monkeypatch):
    captured_timeouts: list[float] = []

    async def pending_poll(device_code, *, timeout_s=None):
        captured_timeouts.append(timeout_s)
        return {"status": "pending"}

    monkeypatch.setattr("app.api.pikpak.device_login_poll", pending_poll)
    t0 = time.monotonic()
    resp = client.post(
        "/api/pikpak/auth/device/poll",
        json={"device_code": "DC_CAP", "interval_s": 30.0, "max_wait_s": 1.0},
        headers=_auth_headers(monkeypatch),
    )
    elapsed = time.monotonic() - t0
    assert resp.status_code == 200
    assert resp.json()["timed_out"] is True
    assert elapsed < 3.0, "不能因为 interval_s=30s 而睡超 max_wait_s=1.0s"
    assert captured_timeouts and all(0 < t <= 1.05 for t in captured_timeouts)


@pytest.mark.asyncio
async def test_pikpak_device_login_poll_clears_all_account_caches(monkeypatch):
    pikpak_service_module._auth_status_cache.put(("status", None), {"logged_in": True})
    pikpak_service_module._files_lru.put(("root", None), ([], None))
    pikpak_service_module._meta_cache.put(("meta", "F1"), {"id": "F1"})
    pikpak_service_module._archive_folder_lru.put(("root", "arch"), "A1")

    async def fake_run_cli(args, **kwargs):
        return {"status": "success", "name": "NewUser"}

    monkeypatch.setattr(pikpak_service_module, "_run_cli", fake_run_cli)
    res = await pikpak_service_module.device_login_poll("DC_CLEAR")
    assert res["status"] == "success"
    assert len(pikpak_service_module._auth_status_cache) == 0
    assert len(pikpak_service_module._files_lru) == 0
    assert len(pikpak_service_module._meta_cache) == 0
    assert len(pikpak_service_module._archive_folder_lru) == 0


def test_pikpak_inspect_reads_channel_inspector_keys(monkeypatch):
    async def fake_inspect(**kwargs):
        return {
            "is_accessible": True,
            "has_write_permission": True,
            "parsed_chat_id": "-100900050",
            "channel_title": "Test Channel",
            "channel_username": "test_chan",
            "permission_summary": "OK",
        }

    monkeypatch.setattr("app.services.channel_inspector.ChannelInspector.inspect_channel", fake_inspect)
    resp = client.post(
        "/api/pikpak/inspect",
        json={"target_channel": "@test_chan", "remote_files": []},
        headers=_auth_headers(monkeypatch),
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["chat_id"] == "-100900050"
    assert data["channel_title"] == "Test Channel"
    assert data["username"] == "test_chan"


def test_pikpak_publish_commits_initial_progress_atomically(monkeypatch):
    # 即使 update_progress_async 会抛错，也不再在 /publish 路由被二次调用，且任务入库即带初始总字节/条数
    monkeypatch.setattr(
        TaskQueueRepository,
        "update_progress_async",
        AsyncMock(side_effect=RuntimeError("should not be called")),
    )
    resp = client.post(
        "/api/pikpak/publish",
        json={
            "target_channel": "-100888",
            "remote_files": [
                {"file_id": "F1", "name": "a.mp4", "size_bytes": 1000},
                {"file_id": "F2", "name": "b.mp4", "size_bytes": 2500},
            ],
        },
        headers=_auth_headers(monkeypatch),
    )
    assert resp.status_code == 200
    task_id = resp.json()["task_id"]
    task = TaskQueueRepository.get_by_id(task_id)
    assert task is not None
    assert task["total_bytes"] == 3500
    assert task["total_items"] == 2
    assert task["current_item"] == "[0/2] 等待调度启动..."
    TaskQueueRepository.delete(task_id)


def test_pikpak_device_poll_budget_timeout_returns_timed_out_not_502(monkeypatch):
    from app.core.pikpak.taxonomy import PikPakCliTimeoutError

    async def slow_poll(device_code, *, timeout_s=None):
        raise PikPakCliTimeoutError("CLI 超时")

    monkeypatch.setattr("app.api.pikpak.device_login_poll", slow_poll)
    resp = client.post(
        "/api/pikpak/auth/device/poll",
        json={"device_code": "DC_BUDGET", "interval_s": 5.0, "max_wait_s": 1.0},
        headers=_auth_headers(monkeypatch),
    )
    assert resp.status_code == 200
    assert resp.json()["timed_out"] is True
    assert resp.json()["logged_in"] is False


@pytest.mark.asyncio
async def test_pikpak_slot_timeout_does_not_log_device_code(monkeypatch, caplog):
    from app.core.pikpak.taxonomy import PikPakCliTimeoutError

    fake_sem = asyncio.Semaphore(0)
    monkeypatch.setattr(pikpak_service_module, "_get_spawn_semaphore", lambda: fake_sem)
    caplog.set_level("WARNING")

    with pytest.raises(PikPakCliTimeoutError):
        await pikpak_service_module._run_cli(["auth", "login", "--continue", "LIVE_SECRET_DC_XYZ"], timeout_s=0.05)

    assert "LIVE_SECRET_DC_XYZ" not in caplog.text
    assert "--continue" not in caplog.text


@pytest.mark.asyncio
async def test_pikpak_recomputes_timeout_after_subprocess_startup(monkeypatch):
    from app.core.pikpak.taxonomy import PikPakCliTimeoutError

    clock = {"now": 100.0}
    monkeypatch.setattr(pikpak_service_module.time, "monotonic", lambda: clock["now"])

    fake_proc = MagicMock()
    fake_proc.returncode = None
    fake_proc.communicate = AsyncMock(return_value=(b"{}", b""))

    async def slow_spawn(*args, **kwargs):
        clock["now"] += 2.0  # 进程启动已耗尽 1.0s 超时预算
        return fake_proc

    kill_mock = AsyncMock()
    monkeypatch.setattr(pikpak_service_module.asyncio, "create_subprocess_exec", slow_spawn)
    monkeypatch.setattr(pikpak_service_module, "_kill_process_tree", kill_mock)

    with pytest.raises(PikPakCliTimeoutError):
        await pikpak_service_module._run_cli(["auth", "status"], timeout_s=1.0)

    kill_mock.assert_awaited_once_with(fake_proc)
    fake_proc.communicate.assert_not_awaited()
