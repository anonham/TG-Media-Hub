"""Business Services: PikPak CLI 包装服务、鉴权、文件树检索与 Relink 防重幂等测试 (PikPak Service & File Domain)

覆盖范围:
- 原 test_pikpak_service: PikPak CLI 包装、登录/登出、凭据持久化与限流告警
- 原 test_pikpak_files_service: PikPak 文件域操作 (列表、目录树、重命名、移动、删除与垃圾桶清理)
- 原 test_p1_pikpak_relink_idempotency: PikPak 链接刷新与 Relink 幂等防重机制"""

import asyncio
import hashlib
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from contextlib import suppress
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from _safeio import safe_write_bytes

import app.services.pikpak_service as svc
from app.config import settings
from app.core.pipeline.engines import PikPakStreamTransportEngine
from app.core.pipeline.models import PublishContext, PublishItem
from app.core.stream_chunk_provider import PikPakLinkExpiredError
from app.services.pikpak_service import (
    ARCHIVE_FOLDER_NAME,
    ParsedFileItem,
    PikPakAuthError,
    PikPakCliError,
    PikPakNotFoundError,
    PikPakQuotaError,
    PikPakTransientError,
    _archive_folder_lru,
    _files_lru,
    _run_cli,
    _with_auth_recovery,
    archive_files,
    auth_status,
    delete_files,
    device_login_poll,
    device_login_start,
    get_download_links,
    invalidate_folder_cache,
    list_files_raw,
    resolve_archive_folder,
)

# ==============================================================================
# Part: .tmp_p1.py
# ==============================================================================
_TESTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_FAKE_MAIN = os.path.join(_TESTS_DIR, "fake_pikpak", "fake_pikpak_main.py")
_RUN_DIR = os.path.join(_TESTS_DIR, "fake_pikpak", "_runs")
FAKE_TOKEN = "fake-token-x"


@pytest.fixture(autouse=True)
def _reset_service_singletons():
    """pytest-asyncio 每用例新事件循环: 重置模块级 Semaphore/Lock (loop 绑定) 与恢复时间戳及文件域缓存"""
    svc.reset_runtime_state_for_tests()
    svc.reset_files_cache_for_tests()
    yield
    svc.reset_runtime_state_for_tests()
    svc.reset_files_cache_for_tests()


@pytest.fixture
def fake_pikpak(tmp_dir, monkeypatch):
    """生成跨平台 launcher 并 patch PIKPAK_CLI_BIN; 返回 {set_scenario, events, reset_state}"""
    monkeypatch.delenv("PIKPAK_TOKEN", raising=False)
    monkeypatch.setattr(settings, "PIKPAK_TOKEN", None)
    if sys.platform == "win32":
        launcher = safe_write_bytes(
            os.path.join(tmp_dir, "fake_pikpak.cmd"),
            f'@echo off\r\n"{sys.executable}" "{_FAKE_MAIN}" %*\r\n'.encode(),
        )
    else:
        launcher = safe_write_bytes(
            os.path.join(tmp_dir, "fake_pikpak.sh"),
            f'#!/bin/sh\nexec "{sys.executable}" "{_FAKE_MAIN}" "$@"\n'.encode(),
        )
        os.chmod(launcher, 0o700)
    monkeypatch.setattr(settings, "PIKPAK_CLI_BIN", launcher)
    run_dir = os.path.join(tmp_dir, "_fake_runs")
    shutil.rmtree(run_dir, ignore_errors=True)
    os.makedirs(run_dir, exist_ok=True)
    monkeypatch.setenv("PIKPAK_FAKE_RUN_DIR", run_dir)
    monkeypatch.setenv("PIKPAK_FAKE_SCENARIO", "normal")

    def set_scenario(name):
        monkeypatch.setenv("PIKPAK_FAKE_SCENARIO", name)

    def reset_state():
        shutil.rmtree(run_dir, ignore_errors=True)
        os.makedirs(run_dir, exist_ok=True)
        svc.reset_runtime_state_for_tests()

    def events():
        out = []
        if os.path.isdir(run_dir):
            for fn in os.listdir(run_dir):
                if fn.startswith("inv-"):
                    with open(os.path.join(run_dir, fn), encoding="utf-8") as f:
                        out.append(json.load(f))
        out.sort(key=lambda e: e.get("enter_ts", 0))
        return out

    def spawn_count(cmd0):
        return len([e for e in events() if e.get("args") and e["args"][0] == cmd0])

    yield SimpleNamespace(set_scenario=set_scenario, events=events, reset_state=reset_state, spawn_count=spawn_count)
    shutil.rmtree(run_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_auth_status_parses_canned_json(fake_pikpak, monkeypatch):
    """auth status 罐头解析 + token_source 两态 (settings 有值 'env' / 无值 'stored')"""
    monkeypatch.setattr(settings, "PIKPAK_TOKEN", None)
    data = await auth_status()
    assert data["logged_in"] is True
    assert data["user_id"] == "fake-user-001"
    assert data["storage"]["used"] == 108857181792
    assert data["additional_quotas"]["trash_duration"] == "15"
    assert data["token_source"] == "stored"

    monkeypatch.setattr(settings, "PIKPAK_TOKEN", FAKE_TOKEN)
    assert (await auth_status())["token_source"] == "env"


@pytest.mark.asyncio
async def test_cli_error_mapping_quadrants(fake_pikpak):
    """错误映射: 认证/配额/资源/网络四象限 + 用法错误(rc=2) + JSON 解析失败"""
    for scenario, exc_type, code in [
        ("quota", PikPakQuotaError, "CONNECTED_APPS_QUOTA_EXCEEDED"),
        ("not_found", PikPakNotFoundError, "FILE_NOT_FOUND"),
        ("auth_error", PikPakAuthError, "UNAUTHENTICATED"),
        ("network_error", PikPakTransientError, "NETWORK_ERROR"),
    ]:
        fake_pikpak.set_scenario(scenario)
        with pytest.raises(exc_type) as exc_info:
            await _run_cli(["auth", "status"])
        assert code in str(exc_info.value), f"{scenario} 映射错误: {exc_info.value}"

    fake_pikpak.set_scenario("normal")
    with pytest.raises(PikPakCliError):
        await _run_cli(["definitely-not-a-command"])  # 退出码 2 (用法错误)

    fake_pikpak.set_scenario("bad_json")
    with pytest.raises(PikPakCliError):
        await _run_cli(["auth", "status"])  # exit 0 但 stdout 非 JSON


@pytest.mark.asyncio
async def test_spawn_semaphore_caps_concurrency(fake_pikpak):
    """spawn 限流: 8 并发下按 journal enter/exit 区间计算峰值并发 ≤ _CLI_SPAWN_LIMIT"""
    fake_pikpak.set_scenario("slow")
    results = await asyncio.gather(*[_run_cli(["auth", "status"]) for _ in range(8)])
    assert len(results) == 8 and all(r.get("logged_in") for r in results)

    intervals = [(e["enter_ts"], e["exit_ts"]) for e in fake_pikpak.events() if "exit_ts" in e]
    assert len(intervals) == 8
    max_overlap = max(sum(1 for s2, e2 in intervals if s2 <= s < e2) for s, _e in intervals)
    assert max_overlap <= svc._CLI_SPAWN_LIMIT, f"spawn 并发峰值 {max_overlap} 超限"


@pytest.mark.asyncio
async def test_cli_timeout_kills_process(fake_pikpak):
    """超时兜底: 超时 → PikPakCliError 且进程被 kill (journal 仅留 enter 事件)"""
    fake_pikpak.set_scenario("hang")
    with pytest.raises(PikPakCliError, match="超时"):
        await _run_cli(["auth", "status"], timeout_s=0.5)

    events = fake_pikpak.events()
    assert events, "spawn 必须已发生 (enter 事件落盘)"
    assert all("exit_ts" not in e for e in events), "被强杀进程不得留下 exit 事件"


@pytest.mark.asyncio
async def test_run_cli_cancellation_kills_tree_and_reraises(fake_pikpak, monkeypatch):
    """communicate 期间被取消 → 先杀进程树再上抛 CancelledError (进程必须真死,
    否则孤儿 CLI 存活 + spawn 并发上限被绕过)"""
    fake_pikpak.set_scenario("hang")

    real_exec = asyncio.create_subprocess_exec
    captured: dict[str, asyncio.subprocess.Process] = {}

    async def spy_exec(*args, **kwargs):
        proc = await real_exec(*args, **kwargs)
        captured.setdefault("cli", proc)  # 首个 spawn = CLI 本体 (后续 taskkill spawn 不覆盖)
        return proc

    monkeypatch.setattr(svc.asyncio, "create_subprocess_exec", spy_exec)

    task = asyncio.create_task(_run_cli(["auth", "status"], timeout_s=30.0))
    await asyncio.sleep(0.5)  # 进入 communicate (hang 场景永不返回)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    events = fake_pikpak.events()
    assert events, "spawn 必须已发生 (enter 事件落盘)"
    assert "cli" in captured, "必须捕获到被测进程句柄"
    # 修复前: 进程存活 → wait 永超时; 修复后: _kill_process_tree 已收割
    await asyncio.wait_for(captured["cli"].wait(), timeout=10.0)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX direct-kill drain path")
@pytest.mark.asyncio
async def test_kill_process_tree_bounds_stuck_drain(monkeypatch, caplog):
    """A killed process can leave pipes open; draining must still time out."""

    async def communicate():
        await asyncio.Event().wait()

    proc = SimpleNamespace(kill=MagicMock(), communicate=AsyncMock(side_effect=communicate))
    drain_timeout = 0.3
    monkeypatch.setattr(svc, "_CLI_DRAIN_TIMEOUT_S", drain_timeout)
    for name in ("app", "app.services", "app.services.pikpak_service"):
        monkeypatch.setattr(logging.getLogger(name), "propagate", True)

    started = time.monotonic()
    with caplog.at_level(logging.WARNING):
        await asyncio.wait_for(svc._kill_process_tree(proc), timeout=2.0)
    assert time.monotonic() - started < 2.0
    proc.kill.assert_called_once_with()
    proc.communicate.assert_awaited_once_with()
    assert any(
        record.name == "app.services.pikpak_service"
        and record.levelno == logging.WARNING
        and "强杀后管道排水超时" in record.message
        and record.args == (drain_timeout,)
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_kill_process_tree_drain_bounded_when_taskkill_fails(fake_pikpak, monkeypatch):
    """Real-process cleanup integration; Windows also exercises taskkill spawn failure."""
    fake_pikpak.set_scenario("hang")
    proc = await asyncio.create_subprocess_exec(
        settings.PIKPAK_CLI_BIN,
        "auth",
        "status",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    async def spawn_fail(*_args, **_kwargs):
        raise FileNotFoundError("taskkill unavailable")

    monkeypatch.setattr(svc.asyncio, "create_subprocess_exec", spawn_fail)
    monkeypatch.setattr(svc, "_CLI_DRAIN_TIMEOUT_S", 0.3)
    t0 = time.monotonic()
    try:
        await asyncio.wait_for(svc._kill_process_tree(proc), timeout=10.0)
        assert time.monotonic() - t0 < 5.0, "有界排水应远小于 5s"
    except TimeoutError:
        pytest.fail("_kill_process_tree 排水无界 (回归: communicate 永久挂起)")
    finally:
        # 兜底清扫: 测试自身不得泄漏孤儿进程 (真实 taskkill 杀树 / POSIX 直杀;
        # to_thread 包阻塞调用避免 ASYNC221)
        if proc.returncode is None:
            if sys.platform == "win32":
                await asyncio.to_thread(
                    subprocess.run, ["taskkill", "/T", "/F", "/PID", str(proc.pid)], capture_output=True, check=False
                )
            else:
                with suppress(ProcessLookupError):
                    os.kill(proc.pid, signal.SIGKILL)
        with suppress(Exception):
            await asyncio.wait_for(proc.wait(), timeout=5.0)


@pytest.mark.asyncio
async def test_delete_files_invalidates_root_cache(fake_pikpak):
    """root (parent_id="") 删除也必须失效根列表缓存 (truthiness 漏洞 → TTL 内陈旧)"""
    await list_files_raw("")  # 根列表入缓存
    await delete_files(["file-001"], parent_id="")
    await list_files_raw("")  # 缓存必须已失效 → re-spawn
    assert fake_pikpak.spawn_count("rm") == 1
    assert fake_pikpak.spawn_count("ls") == 2, "根列表缓存未随 root 删除失效"


@pytest.mark.asyncio
async def test_device_login_two_phase_state_machine(fake_pikpak):
    """设备码两段式: start 返回 user_code/device_code; 轮询 pending×2 → success;
    denied/expired 场景原样上呈 (状态机由调用方驱动)"""
    start = await device_login_start()
    assert start["user_code"] == "ABCD-1234"
    assert start["device_code"] == "FAKEDEVICECODE123"
    assert start["interval"] == 5

    assert (await device_login_poll(start["device_code"]))["status"] == "authorization_pending"
    assert (await device_login_poll(start["device_code"]))["status"] == "authorization_pending"
    svc._auth_status_cache.put(("status", None), {"logged_in": False})
    assert len(svc._auth_status_cache) == 1
    done = await device_login_poll(start["device_code"])
    assert done["status"] == "success" and done["user_id"] == "fake-user-001"
    assert len(svc._auth_status_cache) == 0

    fake_pikpak.set_scenario("denied")
    assert (await device_login_poll("FAKEDEVICECODE123"))["status"] == "access_denied"
    fake_pikpak.set_scenario("expired")
    assert (await device_login_poll("FAKEDEVICECODE123"))["status"] == "expired_token"


@pytest.mark.asyncio
async def test_credential_recovery_single_flight(fake_pikpak, monkeypatch):
    """凭据刷新单飞: 并发 8 业务失败 → 探针恰 1 次; auth_flaky_first → 恢复后重试成功"""
    monkeypatch.setattr(settings, "PIKPAK_TOKEN", None)

    # 阶段一: 业务命令恒 UNAUTHENTICATED (auth_status_ok 场景: 仅探针命令成功)
    fake_pikpak.set_scenario("auth_status_ok")
    results = await asyncio.gather(
        *[_with_auth_recovery(lambda: _run_cli(["ls", "--raw"])) for _ in range(8)],
        return_exceptions=True,
    )
    assert all(isinstance(r, PikPakAuthError) for r in results), "恒失败场景 8 并发应全部失败"

    def probe_count(evs):
        return len([e for e in evs if e["args"][:2] == ["auth", "status"]])

    assert probe_count(fake_pikpak.events()) == 1, "并发失败只允许触发一次恢复探针 (单飞)"
    biz_calls = [e for e in fake_pikpak.events() if e["args"] and e["args"][0] == "ls"]
    assert len(biz_calls) == 16, "每协程恰好 1 次初始调用 + 1 次恢复后重试"

    # 阶段二: 首次调用认证失败 → 探针成功 → 重试成功 (恢复链路端到端)
    fake_pikpak.reset_state()
    fake_pikpak.set_scenario("auth_flaky_first")
    data = await auth_status()
    assert data["logged_in"] is True
    status_calls = [e for e in fake_pikpak.events() if e["args"][:2] == ["auth", "status"]]
    assert len(status_calls) == 3, "首次失败 + 探针 + 成功重试 = 3 次 auth status"


@pytest.mark.asyncio
async def test_barrier_probe_transient_does_not_set_window(fake_pikpak):
    """缺陷③修复: 业务认证失败 + 探针网络错 → 结局 transient 且成功窗严禁置位 (30s 恢复窗不被伪占据)"""
    fake_pikpak.set_scenario("auth_then_network")
    with pytest.raises(PikPakAuthError):
        await _with_auth_recovery(lambda: _run_cli(["ls", "--raw"]))
    assert svc._refresh_success_at == 0.0, "探针非认证失败不得置恢复成功窗 (缺陷③)"
    assert await svc.classify_auth_failure() == "transient", "探针网络错必须归因为不可达"


@pytest.mark.asyncio
async def test_barrier_probe_ok_sets_window(fake_pikpak, monkeypatch):
    """既有恢复链显式化: 首次认证失败 → 探针成功 → 成功窗置位 + 窗内复用结局"""
    monkeypatch.setattr(settings, "PIKPAK_TOKEN", None)
    fake_pikpak.set_scenario("auth_flaky_first")
    data = await auth_status()
    assert data["logged_in"] is True
    assert svc._refresh_success_at > 0.0, "探针成功必须置 30s 恢复窗"
    assert await svc._credential_recovery_barrier() == "ok_window"


@pytest.mark.asyncio
async def test_classify_auth_failure_mapping(monkeypatch):
    """交叉归因映射: memo auth→not_bound / transient|cli→transient; 无新鲜失败 memo → 经屏障补探针"""
    svc._last_probe = (time.monotonic(), "auth")
    assert await svc.classify_auth_failure() == "not_bound"
    svc._last_probe = (time.monotonic(), "transient")
    assert await svc.classify_auth_failure() == "transient"
    svc._last_probe = (time.monotonic(), "cli")
    assert await svc.classify_auth_failure() == "transient"

    barrier_calls = []

    async def fake_barrier():
        barrier_calls.append(1)
        return "transient"

    monkeypatch.setattr(svc, "_credential_recovery_barrier", fake_barrier)
    svc._last_probe = (time.monotonic(), "ok")  # 成功结局不构成失败 memo
    assert await svc.classify_auth_failure() == "transient"
    assert len(barrier_calls) == 1, "无新鲜失败 memo 时必须经屏障补一次探针"


@pytest.mark.asyncio
async def test_failed_probe_memo_reuse_window(fake_pikpak):
    """防探针风暴: 持续失败下 15s memo 复用窗 —— 恢复链 + 两次归因合计只 1 次探针 spawn"""
    fake_pikpak.set_scenario("auth_then_network")
    with pytest.raises(PikPakAuthError):
        await _with_auth_recovery(lambda: _run_cli(["ls", "--raw"]))

    def probe_count(evs):
        return len([e for e in evs if e["args"][:2] == ["auth", "status"]])

    assert probe_count(fake_pikpak.events()) == 1, "恢复链探针恰 1 次"
    assert await svc.classify_auth_failure() == "transient"
    assert await svc.classify_auth_failure() == "transient"
    assert probe_count(fake_pikpak.events()) == 1, "15s memo 窗内归因必须零新增探针 (防风暴)"


@pytest.mark.asyncio
async def test_with_auth_recovery_skips_retry_on_failed_probe(fake_pikpak):
    """探针证实恢复无望 (transient) → 不做必然失败的无效重试 (省一次 spawn)"""
    fake_pikpak.set_scenario("auth_then_network")
    with pytest.raises(PikPakAuthError):
        await _with_auth_recovery(lambda: _run_cli(["ls", "--raw"]))
    biz_calls = [e for e in fake_pikpak.events() if e["args"] and e["args"][0] == "ls"]
    assert len(biz_calls) == 1, "探针 transient 结局必须跳过业务重试"


@pytest.mark.asyncio
async def test_token_passthrough_to_child_env(fake_pikpak, monkeypatch):
    """子进程 env 透传: PIKPAK_TOKEN 仅 settings 有值才注入; NO_LOG/NO_UPDATE_NOTIFIER 恒注入;
    PIKPAK_DIR 经环境透传保持"""
    monkeypatch.setenv("PIKPAK_DIR", "D:\\fake\\pikpak-dir")
    monkeypatch.setenv("TG_BOOTSTRAP_TOKEN", "test-only-bootstrap-secret")
    real_exec = asyncio.create_subprocess_exec

    async def checked_exec(*args, **kwargs):
        assert "TG_BOOTSTRAP_TOKEN" not in kwargs["env"]
        return await real_exec(*args, **kwargs)

    monkeypatch.setattr(svc.asyncio, "create_subprocess_exec", checked_exec)
    monkeypatch.setattr(settings, "PIKPAK_TOKEN", None)
    await _run_cli(["auth", "status"])
    env_without = fake_pikpak.events()[-1]["env"]
    assert "PIKPAK_TOKEN" not in env_without, "settings 无 token 时不得注入"
    assert env_without.get("PIKPAK_NO_LOG") == "1"
    assert env_without.get("PIKPAK_NO_UPDATE_NOTIFIER") == "1"
    assert env_without.get("PIKPAK_DIR") == "D:\\fake\\pikpak-dir"

    monkeypatch.setattr(settings, "PIKPAK_TOKEN", FAKE_TOKEN)
    await _run_cli(["auth", "status"])
    env_with = fake_pikpak.events()[-1]["env"]
    assert env_with.get("PIKPAK_TOKEN") == hashlib.sha256(FAKE_TOKEN.encode()).hexdigest(), (
        "settings 有值时必须注入且值一致"
    )


@pytest.mark.asyncio
async def test_logs_and_errors_contain_no_credentials(fake_pikpak, monkeypatch, caplog):
    """日志红线: 异常 message 与服务日志均不含 token 字面值/签名直链片段"""
    monkeypatch.setattr(settings, "PIKPAK_TOKEN", FAKE_TOKEN)
    # 'app' logger propagate=False, caplog 在 root: 打通传播链以便断言 (仅本用例)
    monkeypatch.setattr(logging.getLogger("app"), "propagate", True)
    monkeypatch.setattr(logging.getLogger("app.services"), "propagate", True)
    monkeypatch.setattr(logging.getLogger("app.services.pikpak_service"), "propagate", True)

    fake_pikpak.set_scenario("quota")
    with caplog.at_level(logging.DEBUG), pytest.raises(PikPakQuotaError) as exc_info:
        await auth_status()
    assert FAKE_TOKEN not in str(exc_info.value)
    assert FAKE_TOKEN not in caplog.text
    assert "FAKESIGN" not in caplog.text


class TestResolveArchiveFolder:
    """问题 8 专项防护网: 归档目录查找、分页遍历、缓存持久性与防自杀机制验证"""

    @staticmethod
    def _make_item(file_id: str, name: str, is_dir: bool = False, parent_id: str = "") -> ParsedFileItem:
        return ParsedFileItem(
            file_id=file_id,
            name=name,
            is_dir=is_dir,
            size_bytes=0,
            audit={},
            thumbnail_link=None,
            duration_s=None,
            width=None,
            height=None,
            parent_id=parent_id,
        )

    @pytest.mark.asyncio
    async def test_case_1_target_in_page_1(self, monkeypatch):
        """用例 1: 目标文件夹在第 1 页: 直接命中返回其 ID, 不触发后续翻页且绝不调用 mkdir"""
        call_tokens = []
        mkdir_called = False

        async def fake_list_files(parent_id="", page_token=None, **kwargs):
            call_tokens.append(page_token)
            items = [
                self._make_item("file-1", "video.mp4", is_dir=False, parent_id=parent_id),
                self._make_item("fld-arch-1", ARCHIVE_FOLDER_NAME, is_dir=True, parent_id=parent_id),
            ]
            return items, "token_page_2"

        async def fake_run_cli(args, **kwargs):
            nonlocal mkdir_called
            if args and args[0] == "mkdir":
                mkdir_called = True
            return {"id": "fld-new"}

        monkeypatch.setattr(svc, "list_files_raw", fake_list_files)
        monkeypatch.setattr(svc, "_run_cli", fake_run_cli)

        res_id = await resolve_archive_folder("parent_p1")
        assert res_id == "fld-arch-1"
        assert call_tokens == [None], "第 1 页命中后不得继续请求第 2 页"
        assert not mkdir_called, "文件夹已存在, 严禁调用 mkdir"
        assert _archive_folder_lru.get(("parent_p1", ARCHIVE_FOLDER_NAME)) == "fld-arch-1"

    @pytest.mark.asyncio
    async def test_case_2_target_in_page_2_with_page_1_cached(self, monkeypatch):
        """用例 2: 目标文件夹在第 2 页且第 1 页已被缓存 (核心场景):
        首页命中 _files_lru, 必须继续发起网络翻页至第 2 页, 找到并返回 ID, 绝不旁路跳过, 绝不 mkdir"""
        p1_items = [
            self._make_item("file-101", "first.mp4", is_dir=False, parent_id="parent_p2"),
        ]
        _files_lru.put(("parent_p2", None), (p1_items, "token_page_2"))

        call_tokens = []
        mkdir_called = False

        async def fake_list_files(parent_id="", page_token=None, **kwargs):
            call_tokens.append(page_token)
            if page_token is None:
                return p1_items, "token_page_2"
            if page_token == "token_page_2":
                return [
                    self._make_item("fld-arch-2", ARCHIVE_FOLDER_NAME, is_dir=True, parent_id=parent_id),
                ], None
            return [], None

        async def fake_run_cli(args, **kwargs):
            nonlocal mkdir_called
            if args and args[0] == "mkdir":
                mkdir_called = True
            return {"id": "fld-new"}

        monkeypatch.setattr(svc, "list_files_raw", fake_list_files)
        monkeypatch.setattr(svc, "_run_cli", fake_run_cli)

        res_id = await resolve_archive_folder("parent_p2")
        assert res_id == "fld-arch-2"
        assert call_tokens == [None, "token_page_2"], "必须遍历翻页至第 2 页, 杜绝被 is_cached 旁路"
        assert not mkdir_called, "第 2 页已找到同名目录, 绝不得调用 mkdir"
        assert _archive_folder_lru.get(("parent_p2", ARCHIVE_FOLDER_NAME)) == "fld-arch-2"

    @pytest.mark.asyncio
    async def test_case_3_target_not_found_in_any_page(self, monkeypatch):
        """用例 3: 全部分页均不存在目标文件夹: 翻完全部页, 触发 mkdir 并正确放入 _archive_folder_lru"""
        call_tokens = []
        mkdir_args = []

        async def fake_list_files(parent_id="", page_token=None, **kwargs):
            call_tokens.append(page_token)
            if page_token is None:
                return [self._make_item("f-1", "1.mp4", is_dir=False, parent_id=parent_id)], "token_2"
            if page_token == "token_2":
                return [self._make_item("f-2", "2.mp4", is_dir=False, parent_id=parent_id)], None
            return [], None

        async def fake_run_cli(args, **kwargs):
            if args and args[0] == "mkdir":
                mkdir_args.append(list(args))
                return {"id": "fld-created-999"}
            return {}

        monkeypatch.setattr(svc, "list_files_raw", fake_list_files)
        monkeypatch.setattr(svc, "_run_cli", fake_run_cli)

        res_id = await resolve_archive_folder("parent_p3")
        assert res_id == "fld-created-999"
        assert call_tokens == [None, "token_2"]
        assert len(mkdir_args) == 1
        assert mkdir_args[0] == ["mkdir", ARCHIVE_FOLDER_NAME, "-p", "parent_p3"]
        assert _archive_folder_lru.get(("parent_p3", ARCHIVE_FOLDER_NAME)) == "fld-created-999"

    @pytest.mark.asyncio
    async def test_case_4_lru_durability_and_no_suicide(self, monkeypatch):
        """用例 4: LRU 缓存持久性与防清理: resolve 写入及 invalidate_folder_cache 后 _archive_folder_lru 依然有效"""

        async def fake_list_files(parent_id="", page_token=None, **kwargs):
            return [self._make_item("arch-keep-id", ARCHIVE_FOLDER_NAME, is_dir=True, parent_id=parent_id)], None

        monkeypatch.setattr(svc, "list_files_raw", fake_list_files)

        res_id = await resolve_archive_folder("parent_p4")
        assert res_id == "arch-keep-id"
        assert _archive_folder_lru.get(("parent_p4", ARCHIVE_FOLDER_NAME)) == "arch-keep-id"

        # 核心防自杀断言: 模拟后续 mv / mkdir 触发目录缓存失效, 归档目录缓存绝不能被抹除
        invalidate_folder_cache("parent_p4")
        assert _archive_folder_lru.get(("parent_p4", ARCHIVE_FOLDER_NAME)) == "arch-keep-id", (
            "invalidate_folder_cache 绝不能清除 _archive_folder_lru"
        )

        # 再次调用 resolve_archive_folder, 必须直接命中 LRU, 零 list_files_raw 调用
        calls = []

        async def poisoned_list_files(*args, **kwargs):
            calls.append(args)
            raise AssertionError("命中 LRU 时不应调用 list_files_raw")

        monkeypatch.setattr(svc, "list_files_raw", poisoned_list_files)
        cached_id = await resolve_archive_folder("parent_p4")
        assert cached_id == "arch-keep-id"
        assert len(calls) == 0


# ==============================================================================
# Part: .tmp_p2.py
# ==============================================================================


@pytest.mark.asyncio
async def test_ls_pagination_aggregates(fake_pikpak):
    """分页: 严格按 next_page_token 透传 (T1 默认页 → T2 尾页), token 语义正确"""
    items1, token1 = await list_files_raw("")
    assert [i.file_id for i in items1] == ["file-001", "folder-100"]
    assert token1 == "T2"

    items2, token2 = await list_files_raw("", token1)
    assert [i.file_id for i in items2] == ["file-002"]
    assert token2 is None

    assert fake_pikpak.spawn_count("ls") == 2


@pytest.mark.asyncio
async def test_lru_hit_avoids_spawn(fake_pikpak):
    """LRU 命中不 spawn CLI; use_cache=False 绕过缓存; audit/thumbnail 顺带入内存缓存"""
    items1, _ = await list_files_raw("")
    items2, _ = await list_files_raw("")
    assert fake_pikpak.spawn_count("ls") == 1, "第二次调用必须命中缓存 (零 spawn)"
    assert items1 == items2

    items3, _ = await list_files_raw("", use_cache=False)
    assert fake_pikpak.spawn_count("ls") == 2, "use_cache=False 必须重新 spawn"

    meta = svc._meta_cache.get("file-001")
    assert meta is not None
    assert meta["thumbnail_link"] == "https://thumb.fake.test/t1.jpg"
    assert meta["duration_s"] == "1420" and meta["width"] == "1920" and meta["height"] == "1080"
    assert meta["audit"]["status"] == "PASS"


@pytest.mark.asyncio
async def test_lru_invalidation_after_changes(fake_pikpak):
    """变更操作 (mv 经 archive) 后源目录缓存失效, 其他目录缓存保留"""
    await list_files_raw("fldA")  # spawn 1
    await list_files_raw("fldB")  # spawn 2

    # resolve 命中 fldA 首页缓存, 翻页查第 2 页 (spawn 3) → 均未命中 → mkdir+mv
    await archive_files(["file-001"], source_parent_id="fldA")
    assert fake_pikpak.spawn_count("mkdir") == 1
    assert fake_pikpak.spawn_count("mv") == 1

    items_a, _ = await list_files_raw("fldA")  # spawn 4 (fldA 已被失效 → re-spawn)
    assert any(i.is_dir and i.name == "已发布归档" for i in items_a), "归档目录应已并入 ls 结果"
    assert fake_pikpak.spawn_count("ls") == 4

    _items_b, _ = await list_files_raw("fldB")  # fldB 缓存保留, 零 spawn
    assert fake_pikpak.spawn_count("ls") == 4


@pytest.mark.asyncio
async def test_batch_download_links(fake_pikpak, monkeypatch, caplog):
    """批量 JIT 直链: 单次 spawn、逐行一一对应; 数量不符报错; 直链不进日志 (红线)"""
    monkeypatch.setattr(logging.getLogger("app"), "propagate", True)
    monkeypatch.setattr(logging.getLogger("app.services"), "propagate", True)
    monkeypatch.setattr(logging.getLogger("app.services.pikpak_service"), "propagate", True)
    with caplog.at_level(logging.DEBUG):
        links = await get_download_links(["file-001", "file-002", "file-003"])

    assert len(links) == 3
    assert all(ln.startswith("https://cdn.fake.test/dl/") and "X-Sign=FAKESIGN" in ln for ln in links)
    assert fake_pikpak.spawn_count("download") == 1, "批量必须单次 spawn (受 spawn 信号量约束)"
    assert "FAKESIGN" not in caplog.text, "直链绝不落日志 (安全红线)"

    # 数量不符: bad_json 场景输出单行非直链文本 → 3 请求 1 实得 → PikPakCliError
    monkeypatch.setenv("PIKPAK_FAKE_SCENARIO", "bad_json")
    with pytest.raises(PikPakCliError, match="数量不符"):
        await get_download_links(["a", "b", "c"])


@pytest.mark.asyncio
async def test_archive_flow_mkdir_idempotent_then_mv(fake_pikpak):
    """auto_cleanup 默认 archive: mkdir 幂等 (第二次命中 ls 内同名目录, 不再 spawn) + mv 调用"""
    result1 = await archive_files(["file-001"], source_parent_id="")
    assert result1 == {"archive_folder_id": "folder-created-1", "moved": 1}
    assert fake_pikpak.spawn_count("mkdir") == 1
    assert fake_pikpak.spawn_count("mv") == 1

    result2 = await archive_files(["file-002"], source_parent_id="")
    assert result2["archive_folder_id"] == "folder-created-1", "第二次必须复用已建目录 (mkdir 幂等)"
    assert fake_pikpak.spawn_count("mkdir") == 1, "mkdir 不得重复 spawn"
    assert fake_pikpak.spawn_count("mv") == 2

    mv_calls = [e for e in fake_pikpak.events() if e["args"] and e["args"][0] == "mv"]
    assert mv_calls[0]["args"] == ["mv", "file-001", "folder-created-1"]
    assert mv_calls[1]["args"] == ["mv", "file-002", "folder-created-1"]


@pytest.mark.asyncio
async def test_delete_flow_invokes_rm(fake_pikpak):
    """显式删除模式 (rm 进回收站): 调用断言 + parent LRU 失效"""
    await list_files_raw("fldX")  # spawn 1
    await delete_files(["file-001"], parent_id="fldX")
    assert fake_pikpak.spawn_count("rm") == 1
    rm_calls = [e for e in fake_pikpak.events() if e["args"] and e["args"][0] == "rm"]
    assert rm_calls[0]["args"] == ["rm", "file-001"]

    await list_files_raw("fldX")  # 缓存已失效 → re-spawn
    assert fake_pikpak.spawn_count("ls") == 2


@pytest.mark.asyncio
async def test_parse_fields_and_audit_passthrough(fake_pikpak):
    """ls --raw 字段映射: 字符串字节 size → int; kind → is_dir; params/audit 原样透传"""
    items, _ = await list_files_raw("")
    by_id = {i.file_id: i for i in items}

    movie = by_id["file-001"]
    assert movie.is_dir is False
    assert movie.size_bytes == 392927574, "raw 的 size 为字符串字节, 必须转 int"
    assert movie.duration_s == "1420" and movie.width == "1920" and movie.height == "1080"
    assert movie.audit == {"status": "PASS", "message": "", "title": ""}
    assert movie.thumbnail_link == "https://thumb.fake.test/t1.jpg"

    folder = by_id["folder-100"]
    assert folder.is_dir is True and folder.size_bytes == 0

    meta = svc._meta_cache.get("file-001")
    assert meta is not None and meta["duration_s"] == "1420"


@pytest.mark.asyncio
async def test_files_error_mapping(fake_pikpak, monkeypatch):
    """错误映射在文件域同样生效: not_found → NotFound; quota → Quota"""
    monkeypatch.setenv("PIKPAK_FAKE_SCENARIO", "not_found")
    with pytest.raises(PikPakNotFoundError):
        await list_files_raw("")

    monkeypatch.setenv("PIKPAK_FAKE_SCENARIO", "quota")
    with pytest.raises(PikPakQuotaError):
        await get_download_links(["file-001"])


def test_lru_cache_ttl_unit():
    """_LruCache TTL 淘汰单测: 支持单条目 TTL / 默认 TTL / 过期清除。"""
    cache = svc._LruCache(max_entries=2, default_ttl_s=10.0)
    cache.put("k1", "v1", ttl_s=0.5, now=100.0)
    assert cache.get("k1", now=100.2) == "v1"
    assert cache.get("k1", now=100.6) is None  # 过期返回 None
    assert len(cache) == 0

    cache.put("k2", "v2", now=200.0)  # 使用默认 TTL 10.0
    assert cache.get("k2", now=205.0) == "v2"
    assert cache.get("k2", now=211.0) is None


@pytest.mark.asyncio
async def test_lru_cache_ttl_and_anti_staleness(fake_pikpak):
    """TTL 过期自愈测试: 缓存超时后自动失效并重新 spawn CLI。"""
    original_ttl = svc._files_lru._default_ttl_s
    try:
        svc._files_lru._default_ttl_s = 0.3
        items1, _ = await list_files_raw("")
        assert fake_pikpak.spawn_count("ls") == 1

        # 未过期时命中缓存 (零 spawn)
        items2, _ = await list_files_raw("")
        assert fake_pikpak.spawn_count("ls") == 1
        assert items1 == items2

        # 超过 TTL 自动失效回源
        await asyncio.sleep(0.4)
        items3, _ = await list_files_raw("")
        assert fake_pikpak.spawn_count("ls") == 2, "TTL 过期后必须重新 spawn CLI 回源"
        assert items3 == items1
    finally:
        svc._files_lru._default_ttl_s = original_ttl


# ==============================================================================
# Part: test_p1_pikpak_relink_idempotency.py
# ==============================================================================
@pytest.mark.asyncio
async def test_pikpak_relink_retry_multipart_idempotency():
    """TD-07 核心验证:
    多分卷大文件在 Part 1 成功投递后，若 Part 2 发生 PikPakLinkExpiredError 换链重试，
    第 2 次重试必须跳过 Part 1 的上传与 send_file 调用，绝不对频道重复投递 Part 1 消息。
    """
    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value=["https://cdn/relink.mp4"]))

    # 4000 MiB (3.91 GiB) 文件，threshold 2000 MiB (1.95 GiB) -> 切为 2 个分卷
    item = PublishItem(
        item_id="fid_multipart_relink",
        name="movie.mp4",
        size_bytes=4000 * 1024 * 1024,
        duration_s=7200,
        source_type="pikpak",
    )

    mock_client = AsyncMock()
    # 记录 send_file 被调用的详情
    sent_captions = []

    async def fake_send_file(chat_id, uploaded_media, **kwargs):
        caption = kwargs.get("caption", "")
        sent_captions.append(caption)
        # 为每个成功发送的消息生成唯一 ID
        msg_obj = MagicMock()
        msg_obj.id = 1000 + len(sent_captions)
        return msg_obj

    mock_client.send_file.side_effect = fake_send_file

    ctx = PublishContext(
        task_id="test_relink_ctx_01",
        target_chat_id="@test_channel",
        userbot_client=mock_client,
        video_split_threshold_bytes=2000 * 1024 * 1024,
        bot_routing_threshold_bytes=30 * 1024 * 1024,
    )

    upload_part_calls = []

    async def fake_upload(client, url, **kwargs):
        chunk_provider = kwargs.get("chunk_provider")
        part_idx = getattr(chunk_provider, "_part_index", None)
        upload_part_calls.append(part_idx)

        # 模拟在第 1 次尝试的 Part 2 上传期间抛出直链过期
        # upload_part_calls: [1, 2] -> 2 抛出过期; 重试时 [1, 2] 中的 1 被跳过，仅 2 执行
        if len(upload_part_calls) == 2:
            raise PikPakLinkExpiredError("PikPak CDN stream URL expired during part 2")

        return MagicMock()

    with (
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=fake_upload),
        patch.object(engine, "_make_thumb_provider", return_value=None),
    ):
        outcome = await engine.send_item(item, ctx)

    # 1. 验证整个生命周期中，send_file 仅被调用了 2 次 (Part 1 一次，Part 2 一次)
    assert len(sent_captions) == 2, f"预期恰好发送 2 个分卷消息，实际发送了 {len(sent_captions)} 次: {sent_captions}"
    assert "1/2" in sent_captions[0]
    assert "2/2" in sent_captions[1]

    # 2. 验证 upload_file_parallel 的调用:
    # 第一次尝试: Part 1 成功, Part 2 失败
    # 第二次尝试: Part 1 被 delivered_parts 账本幂等跳过，仅 Part 2 被上传
    # 因此 upload_part_calls 序列应为 [1, 2 (失败), 2 (成功)]
    assert upload_part_calls == [1, 2, 2], f"实际上传分卷顺序: {upload_part_calls}"

    # 3. 验证返回终态消息 ID 为 Part 2 的消息 ID (1002)
    assert outcome.message_ids == [1001, 1002]


def test_run_cli_rejects_metachars_for_batch_launcher(monkeypatch):
    from app.services.pikpak_service import _reject_batch_launcher_metachars

    monkeypatch.setattr(settings, "PIKPAK_CLI_BIN", "C:\\tools\\pikpak.cmd")
    with pytest.raises(PikPakCliError, match="元字符"):
        _reject_batch_launcher_metachars(["mkdir", "a&whoami"])
    with pytest.raises(PikPakCliError):
        _reject_batch_launcher_metachars(["mkdir", "a%b"])
    with pytest.raises(PikPakCliError):
        _reject_batch_launcher_metachars(["mkdir", "line\nbreak"])
    # 空格/括号等合法 Windows 目录名字符放行 (list2cmdline 会加引号)
    _reject_batch_launcher_metachars(["mkdir", "我的目录 (1)"])


def test_run_cli_metachar_guard_skipped_for_direct_executable(monkeypatch):
    from app.services.pikpak_service import _reject_batch_launcher_metachars

    monkeypatch.setattr(settings, "PIKPAK_CLI_BIN", "C:\\tools\\pikpak.exe")
    _reject_batch_launcher_metachars(["mkdir", "a&whoami"])  # 直连可执行无 shell 介入, 不触发校验
    monkeypatch.setattr(settings, "PIKPAK_CLI_BIN", "pikpak")
    _reject_batch_launcher_metachars(["mkdir", "a|b"])  # 无后缀默认二进制同样放行
