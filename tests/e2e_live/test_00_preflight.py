"""E2E-LIVE-01 harness preflight 。

验证: scratch 克隆可用 / 服务器在 scratch 上启动就绪 / 版本两腿一致 / 低阈值
注入契约 / 非法 SPLIT_STRATEGY 启动期 fail-fast (承 E2E-CFG-09)。
preflight 不发送任何 Telegram 消息 (零真账号发送副作用)。
"""

import subprocess
import sys
import time
from pathlib import Path

import pytest

from e2e_live.harness import env_gate, server_proc

pytestmark = [pytest.mark.e2e_live]


def test_scratch_clone_carries_baseline_db_and_session(scratch_data_dir: Path) -> None:
    """§4.3 克隆语义: db 主文件与会话文件必须随克隆迁移, 大目录被排除。"""
    assert (scratch_data_dir / "tg_service.db").is_file(), "scratch 缺少克隆库 tg_service.db"
    sessions = list(scratch_data_dir.glob("*.session"))
    assert sessions, f"scratch 未克隆到任何 session 文件: {scratch_data_dir}"
    for excluded in ("downloads", "logs", "e2e_capture", "upload_meta"):
        assert not (scratch_data_dir / excluded).exists(), f"排除目录 {excluded} 不应被克隆"


def test_server_boots_on_scratch_and_settings_ready(live_api, live_server, scratch_data_dir: Path) -> None:
    resp = live_api.get("/api/settings")
    assert resp.status_code == 200
    payload = resp.json()
    assert isinstance(payload.get("app_version"), str) and payload["app_version"]
    if live_server.child_env_snapshot is None:
        pytest.skip("attach 模式无路径重定向可断言")
    # 隔离双保险 (R-1): 服务器必须持有 scratch 锁; 真实 data/ 的锁文件在服务器运行
    # 期间决不能被创建或改写 (快照前后对比, 与进程拓扑无关)
    scratch_lock = scratch_data_dir / "tg_service.lock"
    assert scratch_lock.is_file(), "服务器未持有 scratch 单实例锁——路径重定向未生效 (应经 server_bootstrap 启动)"
    real_lock = env_gate.REPO_ROOT / "data" / "tg_service.lock"
    after = real_lock.read_bytes() if real_lock.exists() else None
    assert after == live_server.real_lock_before, (
        f"真实 data/ 锁文件在服务器运行期间被创建/改写: {real_lock}——服务器泄漏到真实数据目录"
    )


def test_app_version_consistent_with_backend_constant(live_api) -> None:
    """版本两腿一致: API 响应 app_version == 代码常量 (前端腿归 E2E-UI-01)。"""
    from app import APP_VERSION

    assert live_api.get("/api/settings").json()["app_version"] == APP_VERSION


def test_threshold_injection_contract(live_server) -> None:
    """§9.0 低阈值注入契约: 子进程 env 必须携带与 env_gate 一致的低门限与 scratch DATA_DIR。

    行为级验证 (小样本实际切出 2 分卷) 由 E2E-LIVE-07 承担。
    """
    snapshot = live_server.child_env_snapshot
    if snapshot is None:
        pytest.skip("attach 模式 (E2E_LIVE_BASE_URL) 无子进程 env 可断言")
    expected = {
        "MAX_FILE_SIZE_MB": str(env_gate.max_file_size_mb()),
        "PREMIUM_MAX_FILE_SIZE_MB": str(env_gate.premium_max_file_size_mb()),
        "DATA_DIR": str(live_server.data_dir),
    }
    for key, value in expected.items():
        assert snapshot.get(key) == value, f"子进程 {key} 注入不符: {snapshot.get(key)!r} != {value!r}"


def test_invalid_split_strategy_fails_fast(tmp_path: Path) -> None:
    """承 E2E-CFG-09: 非法 SPLIT_STRATEGY 启动期中止并列出允许选项 (本地-dummy)。

    经 scratch 引导器启动 (与主服务器同机制, 零真实数据接触): 锁落在 tmp_path,
    validate_required 在锁之后执行并以非零码退出 (run.py __main__ 次序: 锁 → 校验)。
    """
    host = "127.0.0.1"
    port = server_proc.find_free_port(host)
    log_path = tmp_path / "failfast_stdout.log"
    env = server_proc.child_env(tmp_path, port, host)
    env["SPLIT_STRATEGY"] = "invalid_strategy"
    with open(log_path, "wb") as log:
        proc = subprocess.Popen(
            [sys.executable, str(server_proc.BOOTSTRAP_PATH), str(tmp_path)],
            cwd=str(env_gate.REPO_ROOT),
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline and proc.poll() is None:
            time.sleep(0.5)
        if proc.poll() is None:
            proc.kill()
            pytest.fail("非法 SPLIT_STRATEGY 未在 60s 内中止启动 (fail-fast 失效)")
    text = log_path.read_text(encoding="utf-8", errors="ignore")
    assert proc.returncode != 0, "fail-fast 应以非零码退出"
    assert "非法切片策略" in text, f"错误信息未指明非法策略:\n{text[-2000:]}"
    assert "vsse_with_fallback" in text, "错误信息未列出允许选项"
