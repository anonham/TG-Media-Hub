"""UI E2E 会话与函数级夹具 (纯异步 Playwright + open-mode 后端 + 前端编排).

实现依据:
- docs/testing/E2E_GUIDE.md (§2.1, §4 用例真源)
"""

from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from e2e_live.harness import env_gate, local_samples, scratch_env, server_proc
from playwright.async_api import Browser, BrowserContext, Page, async_playwright

_PACKAGE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _PACKAGE_DIR.parents[1]


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """门控纪律: 默认自动全部 skip，Mocked UI 套件支持 E2E_UI_MOCKED=1 或 E2E_UI=1; Live UI 套件要求 E2E_UI=1."""
    is_ui_enabled = os.environ.get("E2E_UI") == "1"
    is_mocked_enabled = is_ui_enabled or os.environ.get("E2E_UI_MOCKED") == "1"
    is_xdist = bool(os.environ.get("PYTEST_XDIST_WORKER"))

    session_loop_mark = pytest.mark.asyncio(loop_scope="session")

    for item in items:
        if not Path(str(item.fspath)).is_relative_to(_PACKAGE_DIR):
            continue

        is_mocked_test = "test_ui_mocked" in str(item.fspath)
        allowed = (is_mocked_enabled if is_mocked_test else is_ui_enabled) and not is_xdist

        if allowed:
            if not is_mocked_test:
                # pytest-asyncio 1.x: 测试的 loop_scope 取 closest asyncio marker。测试函数
                # 自带的裸 `@pytest.mark.asyncio` 会遮蔽 add_marker (实测 closest kwargs={})
                # 导致测试跑在 function loop 而其 session 级夹具活在 session loop —
                # playwright transport 绑死于永不运转的 session loop, 首个 RPC 永久挂起。
                # 先剥离裸标记再显式打 session 标记, 并在收集期硬校验 (静默回归→当场失败)。
                item.own_markers = [m for m in item.own_markers if not (m.name == "asyncio" and not m.kwargs)]
                item.add_marker(session_loop_mark)
                closest = item.get_closest_marker("asyncio")
                if closest is None or closest.kwargs.get("loop_scope") != "session":
                    pytest.fail(f"{item.nodeid}: asyncio loop_scope=session 未生效 (pytest-asyncio marker 语义回归)")
        elif is_xdist:
            item.add_marker(pytest.mark.skip(reason="UI E2E 套件禁止 xdist 并行"))
        elif is_mocked_test:
            item.add_marker(pytest.mark.skip(reason="Mocked UI E2E 需要显式设置 E2E_UI_MOCKED=1 或 E2E_UI=1"))
        else:
            item.add_marker(
                pytest.mark.skip(reason="UI E2E 需要显式环境变量 E2E_UI=1; 触发指南见 tests/e2e_ui/README.md")
            )


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> Iterator[None]:
    """记录各阶段运行结果，供 page 夹具在用例失败时留存现场截图与 trace."""
    outcome = yield
    rep = outcome.get_result()
    setattr(item, f"rep_{rep.when}", rep)


@pytest.fixture(scope="session")
def ui_gate() -> None:
    """会话级双重门禁兜底."""
    if os.environ.get("E2E_UI") != "1" and os.environ.get("E2E_UI_MOCKED") != "1":
        pytest.skip("E2E_UI 或 E2E_UI_MOCKED 未设置 (默认 skip)")
    if os.environ.get("PYTEST_XDIST_WORKER"):
        pytest.skip("UI E2E 套件禁止 xdist")


def resolve_cached_chromium() -> str | None:
    """优先探测并复用本地已缓存的 Chromium 二进制 (跨平台: Windows, Linux, macOS)."""
    search_dirs: list[Path] = []

    # 1. Windows: %LOCALAPPDATA%/ms-playwright
    if sys.platform == "win32":
        local_appdata = os.environ.get("LOCALAPPDATA")
        if local_appdata:
            search_dirs.append(Path(local_appdata) / "ms-playwright")
    # 2. macOS: ~/Library/Caches/ms-playwright
    elif sys.platform == "darwin":
        search_dirs.append(Path.home() / "Library" / "Caches" / "ms-playwright")
    # 3. Linux / Unix: $XDG_CACHE_HOME/ms-playwright 或 ~/.cache/ms-playwright
    else:
        xdg_cache = os.environ.get("XDG_CACHE_HOME")
        if xdg_cache:
            search_dirs.append(Path(xdg_cache) / "ms-playwright")
        search_dirs.append(Path.home() / ".cache" / "ms-playwright")

    patterns = [
        "chromium-*/chrome-win*/chrome.exe",
        "chromium-*/chrome-linux/chrome",
        "chromium-*/chrome-mac/Chromium.app/Contents/MacOS/Chromium",
    ]

    for base in search_dirs:
        if not base.is_dir():
            continue
        for pat in patterns:
            matches = sorted(base.glob(pat))
            if matches:
                return str(matches[-1])

    return None


def _probe_http(url: str, timeout: float = 3.0) -> bool:
    """HTTP GET 就绪探测 (绕过本地系统代理)."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(url, timeout=timeout) as resp:
            return bool(resp.status < 500)
    except (urllib.error.URLError, ConnectionError, OSError):
        return False


def _kill_proc_tree(proc: subprocess.Popen[Any] | None) -> None:
    """Windows 下对启动器树执行整树强杀，避免孤儿进程残留占用端口 (R-1)."""
    if proc is None or proc.poll() is not None:
        return
    if sys.platform == "win32":
        subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"], capture_output=True, check=False)
    else:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


async def _teardown_bound(coro: Any, limit: float) -> None:
    """收尾专用限时执行: 超时后发起取消但不等待其完成。

    asyncio.wait_for 在"取消不可达"的挂死上 (传输死亡后内部吞掉 CancelledError) 自身会
    永久阻塞; 这里改为 asyncio.wait 限时等待, 超时即放弃 — 残留任务由 loop 关停回收。
    永不抛出 (收尾失败静默, 不遮蔽测试结果)。
    """
    task = asyncio.create_task(coro)
    done, pending = await asyncio.wait({task}, timeout=limit)
    for t in done:
        if not t.cancelled() and t.exception() is not None:
            t.exception()  # 检索以避免 "never retrieved" 告警
    if pending:
        pending.pop().cancel()


@pytest.fixture(scope="session")
def ui_scratch_dir(ui_gate: None) -> Iterator[Path]:
    """scratch DATA_DIR: 克隆真实 data/ 至 tests/tmp/ 并预置种子文件与观测目录."""
    stamp = uuid.uuid4().hex
    dest = _REPO_ROOT / "tests" / "tmp" / f"e2e_ui_{stamp}"
    scratch_env.clone_source_data(dest)

    downloads = dest / "downloads"
    downloads.mkdir(parents=True, exist_ok=True)
    base = local_samples.ensure_v3_base()

    # 预置 65 个种子文件供 E2E-UI-10 跨页勾选使用 (~4MB 小文件档: 低速网络下
    # 观测任务也能快速跑完, 避免 live 观测链路排队超时)
    for i in range(1, 66):
        shutil.copy2(base["s"], downloads / f"ui_seed_{i:02d}.mp4")

    # 预置发布观测任务文件夹 (ui_task_a / ui_task_b, 6 × ~4 MB; 与 dest 平级存放于 tests/tmp/)
    runs = local_samples.samples_root() / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    folders: dict[str, str] = {}
    for tag in ("ui_task_a", "ui_task_b"):
        folder = runs / f"{tag}_{stamp}"
        folder.mkdir(parents=True, exist_ok=True)
        for i in range(1, 7):
            shutil.copy2(base["s"], folder / f"{tag}_{i}.mp4")
        folders[tag] = str(folder)

    # 留存 manifest 元数据
    _ui_manifest_cache.clear()
    _ui_manifest_cache.update({"scratch": str(dest), "downloads": str(downloads), "folders": folders})

    yield dest

    if not env_gate.keep_scratch():
        shutil.rmtree(dest, ignore_errors=True)
        for folder_path in folders.values():
            shutil.rmtree(folder_path, ignore_errors=True)


_ui_manifest_cache: dict[str, Any] = {}


@pytest.fixture(scope="session")
def ui_manifest(ui_scratch_dir: Path) -> dict[str, Any]:
    return dict(_ui_manifest_cache)


@pytest.fixture(scope="session")
def ui_backend_server(
    ui_gate: None,
    ui_scratch_dir: Path,
) -> Iterator[server_proc.LiveServer | server_proc.ExternalServer]:
    """真实后端服务: 启动于 8500 端口，open-mode 无鉴权且关闭 API 限流 (Fail Closed)."""
    external = os.environ.get("E2E_UI_BACKEND_URL")
    if external:
        ext_server = server_proc.ExternalServer(external)
        ext_server.wait_ready(timeout=60.0)
        yield ext_server
        return

    host = "127.0.0.1"
    port = 8500
    try:
        scratch_env.ensure_no_real_server(host, port)
    except RuntimeError as exc:
        raise RuntimeError(f"8500 端口已被监听，UI E2E 拒绝复用未知服务 (Fail Closed): {exc}") from exc

    server = server_proc.LiveServer(ui_scratch_dir, port, host)
    server.start()
    try:
        # 300s: Telegram 握手耗时波动大 (实测 18s ~ 31s+), 网络劣化时 180s 会把整个
        # live 组拖成 setup ERROR; 就绪门禁期间业务 API 短暂 503 属预期
        server.wait_ready(timeout=300.0)
        yield server
    finally:
        server.stop()


@pytest.fixture(scope="session")
def ui_frontend_server(ui_backend_server: Any, ui_scratch_dir: Path) -> Iterator[str]:
    """管理前端 Vite 服务生命周期 (仅在显式提供 E2E_UI_BASE_URL 时复用外部前端; 否则 5173 实施 Fail Closed)."""
    external = os.environ.get("E2E_UI_BASE_URL")
    if external:
        yield external.rstrip("/")
        return

    host = "127.0.0.1"
    port = 5173
    base_url = f"http://{host}:{port}"
    try:
        scratch_env.ensure_no_real_server(host, port)
    except RuntimeError as exc:
        raise RuntimeError(
            f"5173 端口已被监听，UI E2E 拒绝盲目复用未知前端服务 (Fail Closed)；"
            f"请先停止占用进程，或显式设置 E2E_UI_BASE_URL={base_url}: {exc}"
        ) from exc

    pnpm_bin = shutil.which("pnpm") or shutil.which("pnpm.cmd") or "pnpm"
    cmd = [pnpm_bin, "run", "dev", "--", "--port", str(port), "--strictPort"]
    vite_env = os.environ.copy()
    vite_env["VITE_BACKEND_URL"] = str(ui_backend_server.base_url)
    vite_log_path = ui_scratch_dir / "vite_stdout.log"
    with open(vite_log_path, "wb") as log_file:
        proc = subprocess.Popen(
            cmd,
            cwd=str(_REPO_ROOT / "frontend"),
            env=vite_env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )

    def _vite_log_tail(limit: int = 4000) -> str:
        try:
            return vite_log_path.read_text(encoding="utf-8", errors="ignore")[-limit:]
        except OSError:
            return "<无日志>"

    ready = False
    deadline = time.monotonic() + 120.0
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"前端 Vite 开发服务器提前退出 (code={proc.returncode}):\n{_vite_log_tail()}")
        if _probe_http(f"{base_url}/static/") or _probe_http(base_url):
            ready = True
            break
        time.sleep(1.0)

    if not ready:
        _kill_proc_tree(proc)
        raise RuntimeError(f"前端 Vite 开发服务器 120s 内未就绪:\n{_vite_log_tail()}")

    try:
        yield base_url
    finally:
        _kill_proc_tree(proc)


_PLACEHOLDER_CHATS = {"", "telegram", "-1009000000010"}


def resolve_ui_target_chat() -> str:
    """按优先级解析测试频道引用: E2E_UI_TARGET_CHAT -> E2E_LIVE_TARGET_CHAT -> .env TEST_CHANNEL_LINK."""
    raw = os.environ.get("E2E_UI_TARGET_CHAT", "").strip() or (env_gate.target_chat() or "")
    chat_ref = env_gate.chat_ref_from_link(raw)
    if chat_ref in _PLACEHOLDER_CHATS:
        return ""
    return chat_ref


@pytest.fixture(scope="session")
def ui_target_chat(ui_gate: None) -> str:
    """测试频道引用 (零硬编码; 缺失或仍为模板占位值时直接报错中止)."""
    chat_ref = resolve_ui_target_chat()
    if not chat_ref:
        pytest.fail(
            "未配置有效的 UI E2E 测试频道 (Fail Closed): 请在 .env 中设置真实的 TEST_CHANNEL_LINK，"
            "或通过环境变量 E2E_UI_TARGET_CHAT / E2E_LIVE_TARGET_CHAT 显式指定"
        )
    return chat_ref


def _reader_creds() -> tuple[int, str] | None:
    from app import config as app_config

    s = app_config.settings
    if not s.API_ID or not s.API_HASH:
        return None
    return s.API_ID, s.API_HASH


def _userbot_session_candidates(scratch_data_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    warm = scratch_data_dir / "_live_reader_warm.session"
    if warm.exists():
        candidates.append(warm)
    default = scratch_env.find_session_file(scratch_data_dir)
    if default is not None:
        candidates.append(default)
    reserved = {
        "live_reader.session",
        "live_cleanup.session",
        "live_stage.session",
        "_live_reader_warm.session",
    }
    for item in sorted(scratch_data_dir.glob("*.session")):
        if item.name in reserved or item.name.startswith("bot_pool"):
            continue
        candidates.append(item)
    return candidates


@pytest_asyncio.fixture(loop_scope="session")
async def tg_reader(ui_gate: None, ui_scratch_dir: Path, ui_target_chat: str) -> AsyncIterator[Any]:
    """函数级 Telethon 只读客户端，支持测试频道消息水位捕获与清理."""
    from e2e_live.harness import tg_reader as tg_reader_mod

    candidates = _userbot_session_candidates(ui_scratch_dir)
    if not candidates:
        pytest.skip("scratch 无可用 session 文件")
    creds = _reader_creds()
    if creds is None:
        pytest.skip("TG_API_ID/TG_API_HASH 缺失")

    warm_file = ui_scratch_dir / "_live_reader_warm.session"
    chosen_candidate: Path | None = warm_file if warm_file.exists() else None

    if chosen_candidate is None:
        for cand in candidates:
            test_copy = ui_scratch_dir / f"ui_test_probe_{cand.stem}_{int(time.time() * 1000)}.session"
            shutil.copy2(cand, test_copy)
            probe_reader = tg_reader_mod.TgReader(test_copy, *creds)
            try:
                await probe_reader.connect()
                await probe_reader.resolve_chat(ui_target_chat)
                chosen_candidate = cand
                shutil.copy2(test_copy, warm_file)
                break
            except Exception:
                continue
            finally:
                await probe_reader.aclose()
                if test_copy.exists():
                    test_copy.unlink(missing_ok=True)

    if chosen_candidate is None:
        pytest.skip(f"无任何 userbot 会话可解析测试频道 {ui_target_chat}")

    session_copy = ui_scratch_dir / f"ui_reader_{int(time.time() * 1000)}.session"
    shutil.copy2(warm_file if warm_file.exists() else chosen_candidate, session_copy)
    reader = tg_reader_mod.TgReader(session_copy, *creds)
    await reader.connect()
    try:
        yield reader
    finally:
        # 收尾硬化: Telethon 断连在传输异常时可能永久挂起 — 超时后放弃 (残留 session 文件随后清理)
        await _teardown_bound(reader.aclose(), 15.0)
        if session_copy.exists():
            session_copy.unlink(missing_ok=True)


# ── 纯异步 Playwright 夹具族 (Session 级浏览器复用 + Function 级 Context 隔离) ──────────


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def ui_playwright(ui_gate: None) -> AsyncIterator[Any]:
    pw = await async_playwright().start()
    try:
        yield pw
    finally:
        # 收尾硬化: 驱动管道异常时 stop 可能永久挂起 — 超时后放弃 (残留进程由 OS/人工回收)
        await _teardown_bound(pw.stop(), 20.0)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def ui_browser(ui_playwright: Any) -> AsyncIterator[Browser]:
    exe = resolve_cached_chromium()
    headless = os.environ.get("E2E_UI_HEADLESS", "1") != "0"
    try:
        browser = await ui_playwright.chromium.launch(
            executable_path=exe,
            headless=headless,
        )
    except Exception as exc:
        raise RuntimeError(
            "Chromium 启动失败。若本地尚未下载或缓存 Chromium 二进制，请执行: "
            "`uv run python -m playwright install chromium`"
        ) from exc
    try:
        yield browser
    finally:
        await _teardown_bound(browser.close(), 20.0)


@pytest_asyncio.fixture(loop_scope="session")
async def ui_context(ui_browser: Browser, ui_frontend_server: str) -> AsyncIterator[BrowserContext]:
    context = await ui_browser.new_context(
        base_url=ui_frontend_server,
        viewport={"width": 1280, "height": 800},
    )
    context.set_default_timeout(10_000)
    context.set_default_navigation_timeout(30_000)
    await context.tracing.start(screenshots=True, snapshots=True, sources=True)
    try:
        yield context
    finally:
        await _teardown_bound(context.close(), 20.0)


@pytest_asyncio.fixture(loop_scope="session")
async def page(ui_context: BrowserContext, request: pytest.FixtureRequest) -> AsyncIterator[Page]:
    p = await ui_context.new_page()
    try:
        yield p
    finally:
        rep_call = getattr(request.node, "rep_call", None)
        if rep_call and rep_call.failed:
            out_dir = _REPO_ROOT / "tests" / ".tmp" / "playwright_ui" / f"{request.node.name}_{int(time.time())}"
            out_dir.mkdir(parents=True, exist_ok=True)
            await _teardown_bound(p.screenshot(path=str(out_dir / "failure.png"), full_page=True), 15.0)
            await _teardown_bound(ui_context.tracing.stop(path=str(out_dir / "trace.zip")), 15.0)
        else:
            await _teardown_bound(ui_context.tracing.stop(), 15.0)
        await _teardown_bound(p.close(), 15.0)
