import json
import os
from pathlib import Path

import pytest

from app.main import app

SNAPSHOT_FILE = Path(__file__).parent / "openapi_snapshot.json"


@pytest.mark.parametrize(
    ("path", "method"),
    [("/api/tasks", "get"), ("/api/accounts", "get"), ("/api/auth/media-ticket", "post")],
)
def test_console_security_requires_both_headers(path, method):
    schema = app.openapi()
    schemes = schema["components"]["securitySchemes"]
    assert schemes["ConsoleApiKey"] == {"type": "apiKey", "in": "header", "name": "X-API-Key"}
    assert schemes["ConsoleTwoFactor"] == {"type": "apiKey", "in": "header", "name": "X-2FA-Token"}
    assert schema["paths"][path][method]["security"] == [{"ConsoleApiKey": [], "ConsoleTwoFactor": []}]
    assert schema["paths"]["/metrics"]["get"]["security"] == [{"ConsoleApiKey": []}]


def test_all_endpoints_have_200_contract():
    """保证 100% API 端点具备 200 响应模型契约 (assert schema_200 is not None)"""
    openapi_schema = app.openapi()
    paths = openapi_schema.get("paths", {})
    assert len(paths) > 0, "OpenAPI 路径集合不能为空"

    missing_contract = []
    total_endpoints = 0

    for path_str, methods in sorted(paths.items()):
        for method, op in sorted(methods.items()):
            if method.lower() not in ("get", "post", "put", "delete", "patch"):
                continue
            total_endpoints += 1
            responses = op.get("responses", {})
            resp_200 = responses.get("200")
            if not resp_200:
                missing_contract.append(f"{method.upper()} {path_str} (missing 200 response)")
                continue

            content = resp_200.get("content", {})
            if not content:
                missing_contract.append(f"{method.upper()} {path_str} (missing 200 content schema)")
                continue

            schema_200 = None
            if "application/json" in content:
                schema_200 = content["application/json"].get("schema")
            elif "*/*" in content:
                schema_200 = content["*/*"].get("schema")
            else:
                schema_200 = next(iter(content.values())).get("schema")

            assert schema_200 is not None, f"{method.upper()} {path_str} 的 200 响应 schema 不能为空"

    assert not missing_contract, (
        f"发现未锁定 200 响应契约的端点 ({len(missing_contract)}/{total_endpoints}):\n" + "\n".join(missing_contract)
    )
    assert total_endpoints >= 45, f"端点总数 ({total_endpoints}) 应覆盖系统完整路由集合"


def test_openapi_snapshot_match():
    """快照测试: 深度对比当前生成的 OpenAPI 规范与已提交的快照基线 (Fail-Closed)"""
    current_schema = json.loads(json.dumps(app.openapi(), ensure_ascii=False, sort_keys=True))

    if os.getenv("UPDATE_OPENAPI_SNAPSHOT") == "1":
        with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
            json.dump(current_schema, f, indent=2, ensure_ascii=False, sort_keys=True)

    assert SNAPSHOT_FILE.exists(), (
        f"未找到 OpenAPI 快照文件: {SNAPSHOT_FILE}。"
        "请显式运行 UPDATE_OPENAPI_SNAPSHOT=1 uv run pytest tests/api/test_openapi_snapshot.py 生成。"
    )

    with open(SNAPSHOT_FILE, encoding="utf-8") as f:
        snapshot_schema = json.load(f)

    # 1. 验证路径集合完全一致
    assert sorted(current_schema.get("paths", {}).keys()) == sorted(snapshot_schema.get("paths", {}).keys()), (
        "API 路由路径清单与快照不一致"
    )

    # 2. 验证各个路径下的 HTTP 方法完全一致
    for path, methods in current_schema.get("paths", {}).items():
        assert sorted(methods.keys()) == sorted(snapshot_schema.get("paths", {}).get(path, {}).keys()), (
            f"路径 {path} 的 HTTP 方法与快照不一致"
        )

    # 3. 验证 components.schemas 模型清单与内部结构完全一致
    current_models = current_schema.get("components", {}).get("schemas", {})
    snapshot_models = snapshot_schema.get("components", {}).get("schemas", {})
    assert sorted(current_models.keys()) == sorted(snapshot_models.keys()), "components.schemas 模型清单与快照不一致"

    # 4. 深度全量结构对比 (防字段名、类型、参数或响应 schema 静默漂移)
    assert current_schema == snapshot_schema, (
        "OpenAPI 完整 Schema 与快照基线不一致！若本次修改涉及 API 契约变更，"
        "请运行 `UPDATE_OPENAPI_SNAPSHOT=1 uv run pytest tests/api/test_openapi_snapshot.py` "
        "并执行 `cd frontend && pnpm run openapi-ts` 同步前端 SDK。"
    )
