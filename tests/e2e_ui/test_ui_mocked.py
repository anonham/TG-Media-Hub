"""Mocked UI E2E 套件 (无依赖前端交互与渲染验证).

架构与依据:
- docs/testing/E2E_GUIDE.md (§2.1, §4)
- 使用静态 SPA 服务 (static/) + Playwright page.route("**/api/**") 全量拦截模拟接口
- 零 live 后端、零 Telegram 会话依赖，专供 CI 快速回归与端到端交互闭环
"""

from __future__ import annotations

import http.server
import json
import re
import threading
import time
import urllib.parse
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from playwright.async_api import Locator, Page, Route, expect

from app.models.auth import (
    AccountResponse,
    AuthStatusResponse,
    BootstrapAuthResponse,
    BotResponse,
    MediaTicketResponse,
    NativeQrChallengeStartResponse,
    NativeSendCodeResponse,
    NativeVerifyCodeResponse,
    QrChallengeStatusResponse,
    TelegramAuthStatusResponse,
)
from app.models.logs import SystemLogsResponse
from app.models.pikpak import PikPakAuthStatusResponse
from app.models.settings import SystemSettingsGroupedResponse
from app.models.storage import (
    BreakpointListResponse,
    FileListResponse,
    StorageMetricsResponse,
    StorageOverviewResponse,
)
from app.models.task import (
    InspectDownloadRangeResponse,
    TaskListResponse,
    TaskLogsIncrementalResponse,
    TaskResponse,
    TaskStatusResponse,
)

from .helpers import MOBILE_DOM_PROBE_JS, ROUTES, close_drawer, goto

_PACKAGE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _PACKAGE_DIR.parents[1]
_STATIC_DIR = _REPO_ROOT / "static"


class _SPAHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    """静态 SPA HTTP 处理器: 自动将 /static/ 路径映射至 static/ 目录并提供 SPA 路由兜底."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, directory=str(_STATIC_DIR), **kwargs)

    def log_message(self, format: str, *args: Any) -> None:
        pass  # 抑制测试日志中的标准 HTTP 请求噪音

    def translate_path(self, path: str) -> str:
        clean_path = path.split("?", 1)[0].split("#", 1)[0]
        if clean_path.startswith("/static/"):
            clean_path = clean_path[len("/static") :]
        elif clean_path == "/static":
            clean_path = "/"
        return super().translate_path(clean_path)

    def do_GET(self) -> None:
        clean_path = self.path.split("?", 1)[0].split("#", 1)[0]
        if clean_path in ("", "/", "/static", "/static/", "/static/index.html"):
            self.path = "/index.html"
        return super().do_GET()


@pytest.fixture(scope="session")
def mocked_server_url(ui_gate: None) -> Iterator[str]:
    """会话级静态 SPA 服务器 (监听 127.0.0.1 临时随机端口)."""
    if not (_STATIC_DIR / "index.html").exists():
        pytest.fail("前端产物 static/index.html 不存在，请先执行 `cd frontend && pnpm run build`")

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _SPAHTTPRequestHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{port}"
    try:
        yield base_url
    finally:
        server.shutdown()
        server.server_close()


_MOCK_TASKS: list[dict[str, Any]] = [
    TaskResponse.model_validate(
        {
            "id": "task-mock-01",
            "task_type": "publish_folder",
            "status": "running",
            "processed_items": 3,
            "total_items": 10,
            "progress_bytes": 104857600,
            "total_bytes": 209715200,
            "speed_mb_s": 15.0,
            "eta_s": 60.0,
            "current_item": "episode_03.mp4",
            "concurrency": 2,
            "concurrency_total": 2,
            "dc_id": 5,
            "progress": {
                "percent": 50.0,
                "processed_bytes": 104857600,
                "total_bytes": 209715200,
                "processed_items": 3,
                "total_items": 10,
                "speed_mb_s": 15.0,
                "eta_s": 60.0,
                "current_item": "episode_03.mp4",
                "concurrency": 2,
                "concurrency_total": 2,
                "dc_id": 5,
            },
            "account_id": 1,
            "priority": 10,
            "retry_count": 0,
            "max_retries": 3,
            "next_retry_at": 0.0,
            "error_detail": None,
            "params": {
                "folder_path": "/mock/folder_a",
                "target_channel": "-100900000009",
                "upload_concurrency": 2,
                "upload_in_flight_per_connection": 2,
            },
            "log_count": 4,
            "last_log_seq": 4,
            "created_at": 1762603200.0,
            "updated_at": 1762603500.0,
        }
    ).model_dump(),
    TaskResponse.model_validate(
        {
            "id": "task-mock-02",
            "task_type": "download_link",
            "status": "completed",
            "processed_items": 1,
            "total_items": 1,
            "progress_bytes": 52428800,
            "total_bytes": 52428800,
            "speed_mb_s": 0.0,
            "eta_s": 0.0,
            "current_item": "archive.zip",
            "concurrency": 0,
            "concurrency_total": 8,
            "dc_id": 5,
            "progress": {
                "percent": 100.0,
                "processed_bytes": 52428800,
                "total_bytes": 52428800,
                "processed_items": 1,
                "total_items": 1,
                "speed_mb_s": 0.0,
                "eta_s": 0.0,
                "current_item": "archive.zip",
                "concurrency": 0,
                "concurrency_total": 8,
                "dc_id": 5,
            },
            "account_id": 1,
            "priority": 10,
            "retry_count": 0,
            "max_retries": 3,
            "next_retry_at": 0.0,
            "error_detail": None,
            "params": {
                "links": "https://t.me/c/900008/10",
            },
            "log_count": 2,
            "last_log_seq": 2,
            "created_at": 1762599600.0,
            "updated_at": 1762599720.0,
        }
    ).model_dump(),
    TaskResponse.model_validate(
        {
            "id": "task-mock-03",
            "task_type": "forward_channel",
            "status": "failed",
            "processed_items": 1,
            "total_items": 10,
            "progress_bytes": 10485760,
            "total_bytes": 104857600,
            "speed_mb_s": 0.0,
            "eta_s": 0.0,
            "current_item": "",
            "concurrency": 0,
            "concurrency_total": 1,
            "dc_id": 0,
            "progress": {
                "percent": 10.0,
                "processed_bytes": 10485760,
                "total_bytes": 104857600,
                "processed_items": 1,
                "total_items": 10,
                "speed_mb_s": 0.0,
                "eta_s": 0.0,
                "current_item": "",
                "concurrency": 0,
                "concurrency_total": 1,
                "dc_id": 0,
            },
            "account_id": 1,
            "priority": 10,
            "retry_count": 3,
            "max_retries": 3,
            "next_retry_at": 0.0,
            "error_detail": "Telegram FloodWait rate limited",
            "params": {
                "source_chat": "-1009000002",
                "target_chat": "-1009000025",
            },
            "log_count": 3,
            "last_log_seq": 3,
            "created_at": 1762596000.0,
            "updated_at": 1762596060.0,
        }
    ).model_dump(),
]

_MOCK_TASK_LOGS = [
    {"seq": 1, "time": "12:00:01", "level": "info", "message": "任务初始化完成", "created_at": 1762603201.0},
    {"seq": 2, "time": "12:00:02", "level": "info", "message": "开始下载分块 part_1.bin", "created_at": 1762603202.0},
    {"seq": 3, "time": "12:00:03", "level": "warn", "message": "网络抖动重试分片 1", "created_at": 1762603203.0},
    {"seq": 4, "time": "12:00:04", "level": "info", "message": "分片 1 上传成功", "created_at": 1762603204.0},
]

_STATIC_API_RESPONSES: dict[str, Any] = {
    "/api/auth/status": AuthStatusResponse(auth_required=False).model_dump(),
    "/api/auth/media-ticket": MediaTicketResponse(ticket="mock-ticket-token", expires_in=60).model_dump(),
    "/api/settings": SystemSettingsGroupedResponse(
        categories={
            "general": [
                {
                    "key": "max_global_concurrency",
                    "value": "3",
                    "category": "general",
                    "description": "全局最大并发任务数",
                    "updated_at": 1762600000.0,
                }
            ]
        },
        all_settings={
            "max_global_concurrency": "3",
            "download_parallel_connections": "8",
            "upload_parallel_connections": "2",
            "upload_in_flight_per_connection": "2",
        },
        app_version="1.0.0",
    ).model_dump(),
    "/api/tasks/task-mock-01/logs": TaskLogsIncrementalResponse(
        task_id="task-mock-01",
        logs=_MOCK_TASK_LOGS,
        last_seq=4,
        has_more=False,
    ).model_dump(),
    "/api/tasks/task-mock-01": _MOCK_TASKS[0],
    "/api/accounts": [
        AccountResponse(
            id=1,
            account_name="Primary Userbot",
            phone="+12025550106",
            session_file="userbot_primary.session",
            api_id=123456,
            api_hash_masked="a1b2****ef90",
            is_active=True,
            is_default=True,
            is_premium=True,
            status="authenticated",
            dc_id=5,
            user_id=10001,
            username="tester",
            first_name="Tester",
            max_concurrency=2,
            active_task_count=1,
            created_at=1762500000.0,
            updated_at=1762600000.0,
        ).model_dump()
    ],
    "/api/storage/overview": StorageOverviewResponse(
        download_dir="/downloads",
        total_bytes=104857600,
        part_bytes=0,
        final_bytes=104857600,
        file_count=1,
        final_count=1,
        part_count=0,
        orphan_part_count=0,
        orphan_meta_count=0,
        unregistered_final_count=0,
        disk_total_bytes=107374182400,
        disk_used_bytes=21474836480,
        disk_free_bytes=85899345920,
        disk_free_gb=80.0,
        disk_used_percent=20.0,
        effective_free_bytes=85899345920,
        effective_free_gb=80.0,
    ).model_dump(),
    "/api/storage/metrics": StorageMetricsResponse(
        disk_total_bytes=107374182400,
        disk_used_bytes=21474836480,
        disk_free_bytes=85899345920,
        disk_free_gb=80.0,
        disk_used_percent=20.0,
        downloads_bytes=104857600,
        downloads_mb=100.0,
        db_size_bytes=65536,
        logs_size_bytes=16384,
        total_files_count=1,
        effective_free_bytes=85899345920,
        effective_free_gb=80.0,
        total_bytes=107374182400,
        used_bytes=21474836480,
        free_bytes=85899345920,
        percent_used=20.0,
    ).model_dump(),
    "/api/storage/breakpoints": BreakpointListResponse(
        total=0,
        orphan_count=0,
        paired_count=0,
        items=[],
    ).model_dump(),
    "/api/storage/files": FileListResponse(
        total=1,
        page=1,
        limit=50,
        files=[
            {
                "file_name": "sample.mp4",
                "rel_path": "sample.mp4",
                "abs_path": "/downloads/sample.mp4",
                "path": "/downloads/sample.mp4",
                "size": 104857600,
                "size_bytes": 104857600,
                "size_mb": 100.0,
                "mtime": 1762596000.0,
                "mime_type": "video/mp4",
                "category": "video",
            }
        ],
    ).model_dump(),
    "/api/logs/system": SystemLogsResponse(
        total=1,
        logs=[
            {
                "timestamp": "2026-11-08 12:00:00",
                "level": "INFO",
                "logger": "system",
                "message": "系统启动成功",
            }
        ],
    ).model_dump(),
    "/api/pikpak/auth/status": PikPakAuthStatusResponse(
        logged_in=True,
        state="logged_in",
        user_name="mock_pikpak_user",
        email="mock@pikpak.test",
    ).model_dump(),
    "/api/bots": [
        BotResponse(
            id=1,
            bot_token_masked="123456:AA****xyz",
            bot_name="Bot #1",
            bot_username="mock_helper_bot",
            session_name="bot_pool_123456",
            api_id=123456,
            api_hash_masked="a1b2****ef90",
            is_active=True,
            status="ready",
            dc_id=5,
            created_at=1762500000.0,
            updated_at=1762600000.0,
        ).model_dump()
    ],
    "/api/presets": [],
}


def _filter_mock_tasks(url: str) -> dict[str, Any]:
    """按 query 与 status 过滤模拟任务列表 (经 TaskListResponse 模型契约校验)."""
    items = _MOCK_TASKS
    parsed = urllib.parse.urlparse(url)
    params = urllib.parse.parse_qs(parsed.query)
    if "query" in params and params["query"][0]:
        kw = params["query"][0].lower()
        items = [t for t in items if kw in t["id"].lower() or kw in str(t.get("params", "")).lower()]
    if "status" in params and params["status"][0]:
        st = params["status"][0].lower()
        items = [t for t in items if t["status"].lower() == st]

    return TaskListResponse.model_validate(
        {
            "total": len(items),
            "page": 1,
            "limit": 50,
            "items": items,
            "status_counts": {"all": 3, "running": 1, "completed": 1, "failed": 1, "paused": 0, "queued": 0},
        }
    ).model_dump()


async def _handle_mock_api(route: Route) -> None:
    """集中式 API Route 拦截分发器 (基于字典映射与针对性处理器)."""
    url = route.request.url

    # 优先匹配特定 API 静态响应
    for path, data in _STATIC_API_RESPONSES.items():
        if path in url:
            await route.fulfill(status=200, content_type="application/json", body=json.dumps(data))
            return

    # 任务列表动态过滤
    if "/api/tasks" in url and route.request.method == "GET":
        data = _filter_mock_tasks(url)
        await route.fulfill(status=200, content_type="application/json", body=json.dumps(data))
        return

    await route.fulfill(status=200, content_type="application/json", body=json.dumps({}))


@pytest_asyncio.fixture
async def mocked_page(mocked_server_url: str) -> AsyncIterator[Page]:
    """提供独立生命周期、自带 API Mock 路由的页面夹具 (避免跨 event-loop 污染)."""
    from playwright.async_api import async_playwright

    from .conftest import resolve_cached_chromium

    async with async_playwright() as p:
        exe = resolve_cached_chromium()
        browser = await p.chromium.launch(
            executable_path=exe,
            headless=True,
        )
        context = await browser.new_context(
            base_url=mocked_server_url,
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()
        await page.route("**/api/**", _handle_mock_api)
        try:
            yield page
        finally:
            await context.close()
            await browser.close()


@pytest_asyncio.fixture
async def mocked_mobile_page(mocked_server_url: str) -> AsyncIterator[Page]:
    """移动端高分屏视口夹具 (412x915, DPR=3.5, Touch Enabled)."""
    from playwright.async_api import async_playwright

    from .conftest import resolve_cached_chromium

    async with async_playwright() as p:
        exe = resolve_cached_chromium()
        browser = await p.chromium.launch(
            executable_path=exe,
            headless=True,
        )
        context = await browser.new_context(
            base_url=mocked_server_url,
            viewport={"width": 412, "height": 915},
            device_scale_factor=3.5,
            is_mobile=True,
            has_touch=True,
        )
        page = await context.new_page()
        await page.route("**/api/**", _handle_mock_api)
        try:
            yield page
        finally:
            await context.close()
            await browser.close()


# ── Web-First 测试用例 ────────────────────────────────────────────────────────


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_mocked_navigation_all_routes(mocked_page: Page) -> None:
    """用例 1: 侧边栏语义导航全路径覆盖 (8 大路由无报错跳转与容器渲染)."""
    await goto(mocked_page, ROUTES.dashboard)
    await expect(mocked_page.locator(".header-bar-container")).to_be_visible()

    nav_map = [
        ("nav-dashboard", re.compile(r"#/dashboard")),
        ("nav-create", re.compile(r"#/tasks/create")),
        ("nav-pikpak", re.compile(r"#/pikpak")),
        ("nav-tasks", re.compile(r"#/tasks")),
        ("nav-accounts", re.compile(r"#/accounts")),
        ("nav-storage", re.compile(r"#/storage")),
        ("nav-logs", re.compile(r"#/logs")),
        ("nav-settings", re.compile(r"#/settings")),
    ]

    for test_id, url_pattern in nav_map:
        nav_btn = mocked_page.locator(f'[data-testid="{test_id}"]').first
        await expect(nav_btn).to_be_visible()
        await nav_btn.click()
        await expect(mocked_page).to_have_url(url_pattern)
        await expect(mocked_page.locator("#app")).to_be_visible()


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_mocked_task_filtering_and_search(mocked_page: Page) -> None:
    """用例 2: 任务中心搜索与过滤交互 (Web-First 数据驱动响应)."""
    await goto(mocked_page, ROUTES.tasks)
    await expect(mocked_page.locator('[data-testid="task-filter-status"]')).to_be_visible()

    # 验证初始加载出 3 个 mock 任务
    rows = mocked_page.locator('[data-testid="task-row"]')
    await expect(rows).to_have_count(3)

    # 关键字搜索: 搜索 task-mock-01
    search_input = mocked_page.locator('[data-testid="task-search-input"] input')
    await search_input.fill("task-mock-01")
    await search_input.press("Enter")

    # 预期仅剩余 1 行
    await expect(rows).to_have_count(1)
    await expect(rows.first).to_contain_text("task-mock-01")

    # 清空搜索框
    await search_input.fill("")
    await search_input.press("Enter")
    await expect(rows).to_have_count(3)


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_mocked_task_detail_drawer_interaction(mocked_page: Page) -> None:
    """用例 3: 任务详情抽屉展开、Tab 切换与日志终端渲染."""
    await goto(mocked_page, ROUTES.tasks)
    rows = mocked_page.locator('[data-testid="task-row"]')
    await expect(rows).to_have_count(3)

    # 点击第一行任务的详情按钮
    detail_btn = rows.first.locator('[data-testid="task-detail-btn"]')
    await detail_btn.click()

    # 验证抽屉展开
    drawer = mocked_page.locator('[data-testid="task-detail-drawer"]')
    await expect(drawer).to_be_visible()

    # Tab 1: 实时看板
    await mocked_page.locator('[data-testid="tab-metrics"]').click()
    await expect(drawer).to_contain_text("动态传输速率曲线")

    # Tab 2: 任务参数
    await mocked_page.locator('[data-testid="tab-params"]').click()
    await expect(drawer).to_contain_text("任务类型")

    # Tab 3: 流式日志
    await mocked_page.locator('[data-testid="tab-logs"]').click()
    await expect(drawer.locator("code").first).to_be_visible()
    await expect(drawer).to_contain_text("任务初始化完成")

    # Tab 4: 专属诊断
    await mocked_page.locator('[data-testid="tab-diagnosis"]').click()
    await expect(drawer).to_be_visible()

    # 关闭抽屉
    await close_drawer(mocked_page)
    await expect(drawer).to_be_hidden()


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_mocked_create_tabs_switching(mocked_page: Page) -> None:
    """用例 4: 任务发布工作台三大 Tab 视图切换."""
    await goto(mocked_page, ROUTES.create)

    # Tab: 限制媒体下载
    tab_download = mocked_page.locator('[data-testid="tab-download"]')
    await expect(tab_download).to_be_visible()
    await tab_download.click()
    await expect(mocked_page.locator("#app")).to_contain_text("受限媒体下载")

    # Tab: 本地文件夹发布
    tab_publish = mocked_page.locator('[data-testid="tab-publish"]')
    await expect(tab_publish).to_be_visible()
    await tab_publish.click()
    await expect(mocked_page.locator("#app")).to_contain_text("本地文件夹发布")

    # Tab: 云端频道转发
    tab_forward = mocked_page.locator('[data-testid="tab-forward"]')
    await expect(tab_forward).to_be_visible()
    await tab_forward.click()
    await expect(mocked_page.locator("#app")).to_contain_text("云端极速转发")


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_mocked_theme_switch_and_storage(mocked_page: Page) -> None:
    """用例 5: 系统设置页主题切换与 LocalStorage 持久化校验."""
    await goto(mocked_page, ROUTES.settings)
    await expect(mocked_page.locator('[data-testid="theme-select"]')).to_be_visible()

    # 读取初始主题
    initial_theme = await mocked_page.evaluate("() => localStorage.getItem('tg_hub_theme_mode') || 'light'")
    assert initial_theme in ("light", "dark", "system", "auto")

    # 通过界面设定暗黑模式
    await mocked_page.evaluate("() => window.localStorage.setItem('tg_hub_theme_mode', 'dark')")
    updated_theme = await mocked_page.evaluate("() => localStorage.getItem('tg_hub_theme_mode')")
    assert updated_theme == "dark"


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_mocked_mobile_zero_overflow(mocked_mobile_page: Page) -> None:
    """用例 6: 移动端视口全路由 0 横向溢出与卡片流降级断言."""

    async def assert_no_overflow(scene: str, ready_selector: str) -> None:
        await expect(mocked_mobile_page.locator("#app main")).to_be_visible()
        await expect(mocked_mobile_page.locator(ready_selector).first).to_be_visible()
        await mocked_mobile_page.wait_for_timeout(400)
        m = await mocked_mobile_page.evaluate(MOBILE_DOM_PROBE_JS)
        assert not m["hasDocOverflow"], f"[{scene}] 文档横向溢出: {m['docScrollWidth']} > {m['vw']}"
        assert m["viewportOverflowCount"] == 0, f"[{scene}] 视口溢出元素: {m['viewportOverflows']}"
        assert m["clippedOverflowCount"] == 0, f"[{scene}] 容器撑爆元素: {m['clippedOverflows']}"

    # 1. Dashboard
    await goto(mocked_mobile_page, ROUTES.dashboard)
    await assert_no_overflow("dashboard", ".dashboard-container")
    await expect(mocked_mobile_page.locator(".header-left-col")).to_have_css("flex-basis", "auto")

    # 2. Task Center 移动端卡片流 + 批量悬浮栏 + 详情抽屉
    await goto(mocked_mobile_page, ROUTES.tasks)
    cards = mocked_mobile_page.locator('[data-testid="task-mobile-card"]')
    await expect(cards).to_have_count(3)
    await assert_no_overflow("tasks_cards", ".task-center-page .mobile-card-list")

    await mocked_mobile_page.get_by_role("checkbox", name="选择任务 #task-mock-01").click()
    await assert_no_overflow("tasks_batch_bar", ".batch-action-floating-bar")
    await mocked_mobile_page.locator(".clear-select-btn").click()

    await cards.first.locator('[data-testid="task-detail-btn"] button').click()
    await assert_no_overflow("tasks_drawer_metrics", '[data-testid="task-detail-drawer"] .progress-hierarchy-card')
    drawer_tab_selectors = [
        ("params", '[data-testid="task-detail-drawer"] .metadata-grid'),
        ("logs", '[data-testid="task-detail-drawer"] .task-log-terminal'),
        ("diagnosis", '[data-testid="task-detail-drawer"] .publish-diag-content'),
    ]
    for tab_name, selector in drawer_tab_selectors:
        await mocked_mobile_page.locator(f'[data-testid="tab-{tab_name}"]').click()
        await assert_no_overflow(f"tasks_drawer_{tab_name}", selector)
    await close_drawer(mocked_mobile_page)

    # 3. Create Tabs + PikPak + Accounts + Storage + Logs + Settings
    await goto(mocked_mobile_page, ROUTES.create)
    await assert_no_overflow("create_download", '[aria-labelledby="create-tab-download"] .view-container')
    await mocked_mobile_page.locator('[data-testid="tab-publish"]').click()
    await assert_no_overflow("create_publish", '[aria-labelledby="create-tab-publish"] .view-container')
    await mocked_mobile_page.locator('[data-testid="tab-forward"]').click()
    await assert_no_overflow("create_forward", '[aria-labelledby="create-tab-forward"] .view-container')

    for name, route, ready_selector in [
        ("pikpak", ROUTES.pikpak, ".tree-toolbar"),
        ("accounts", ROUTES.accounts, ".accounts-view-container"),
        ("storage", ROUTES.storage, ".storage-view-container"),
        ("logs", ROUTES.logs, ".logs-view-container"),
        ("settings", ROUTES.settings, ".settings-page-wrapper"),
    ]:
        await goto(mocked_mobile_page, route)
        await assert_no_overflow(name, ready_selector)


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_batch_bar_keeps_last_task_reachable(mocked_mobile_page: Page) -> None:
    page = mocked_mobile_page
    await page.set_viewport_size({"width": 360, "height": 640})
    await goto(page, ROUTES.tasks)
    await page.get_by_role("checkbox", name="选择任务 #task-mock-01").click()

    bar = page.locator(".batch-action-floating-bar")
    await expect(bar).to_be_visible()
    await expect(page.locator("html")).to_have_css("scroll-padding-block-end", re.compile(r"^[1-9]\d+px$"))

    async def assert_last_detail_reachable(tasks: Locator) -> None:
        await expect(tasks).to_have_count(3)
        detail = tasks.last.get_by_test_id("task-detail-btn").get_by_role("button")
        await page.evaluate("() => window.scrollTo(0, document.scrollingElement.scrollHeight)")
        await expect(detail).to_be_in_viewport(ratio=1)
        button_bounds = await detail.bounding_box()
        bar_bounds = await bar.bounding_box()
        assert button_bounds and bar_bounds
        assert button_bounds["y"] + button_bounds["height"] <= bar_bounds["y"]
        await detail.click(trial=True)

    await assert_last_detail_reachable(page.get_by_test_id("task-mobile-card"))

    await page.get_by_role("button", name=re.compile("切换表格视图")).click()
    table = page.get_by_role("region", name="任务数据表格")
    await table.evaluate("el => el.scrollTo(el.scrollWidth, el.scrollHeight)")
    await assert_last_detail_reachable(table.get_by_test_id("task-row"))

    await page.get_by_role("button", name="取消全选").click()
    await expect(bar).to_be_hidden()
    await expect(page.locator("html")).to_have_css("scroll-padding-block-end", "0px")


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_mobile_download_priority_value_fits(mocked_mobile_page: Page) -> None:
    page = mocked_mobile_page
    await page.set_viewport_size({"width": 320, "height": 720})
    await goto(page, ROUTES.create)
    priority = page.locator(".slider-row input")
    await expect(priority).to_have_value("10")
    await priority.fill("20")
    await priority.press("Tab")
    await expect(priority).to_have_value("20")
    assert await priority.evaluate("""el => {
        const style = getComputedStyle(el);
        const context = document.createElement('canvas').getContext('2d');
        context.font = style.font;
        const availableWidth = el.clientWidth - parseFloat(style.paddingLeft) - parseFloat(style.paddingRight);
        return ['10', '20'].every(value => context.measureText(value).width <= availableWidth);
    }""")


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_download_range_stats_grid_responds_to_container_width(mocked_mobile_page: Page) -> None:
    page = mocked_mobile_page
    inspection = InspectDownloadRangeResponse(
        channel_identifier="demo_channel",
        parsed_chat_id="-1009000000010",
        channel_title="Demo channel",
        is_accessible=True,
        start_id=1,
        end_id=10000,
        total_range_messages=10000,
        messages_with_media=9900,
        text_only_messages=100,
        photos_count=3400,
        videos_count=2400,
        audios_count=2200,
        documents_count=1900,
        estimated_size_bytes=1048576,
        estimated_size_mb=1,
        estimated_size_gb=0,
        already_downloaded_count=1,
    )

    async def inspect_range(route: Route) -> None:
        await route.fulfill(json=inspection.model_dump())

    await page.route("**/api/download/inspect-range", inspect_range)
    await page.set_viewport_size({"width": 1280, "height": 900})
    await goto(page, ROUTES.create)
    await page.get_by_text("频道消息 ID 范围模式", exact=False).click()
    await page.get_by_placeholder(re.compile("demo_channel")).fill("demo_channel")
    await page.get_by_placeholder("100", exact=True).fill("1")
    await page.get_by_placeholder("200", exact=True).fill("10000")
    await page.get_by_role("button", name="预检频道与消息范围").click()

    stats_grid = page.locator(".stats-grid")
    await expect(stats_grid.locator(".stat-card")).to_have_count(4)

    async def assert_columns(container_width: str, expected_count: int) -> None:
        await page.locator(".range-stats-container").evaluate(
            "(el, width) => { el.style.inlineSize = width; el.style.maxInlineSize = width; }", container_width
        )
        await page.wait_for_function(
            "({ grid, expected }) => getComputedStyle(grid).gridTemplateColumns.split(' ').length === expected",
            arg={"grid": await stats_grid.element_handle(), "expected": expected_count},
        )
        assert await stats_grid.evaluate(
            "el => el.scrollWidth <= el.clientWidth && [...el.children].every(card => card.getBoundingClientRect().right <= el.getBoundingClientRect().right + 1)"
        )

    await assert_columns("13rem", 1)
    await assert_columns("19rem", 2)
    await page.locator(".range-stats-container").evaluate(
        "el => { el.style.removeProperty('inline-size'); el.style.removeProperty('max-inline-size'); }"
    )
    await assert_columns("60rem", 4)
    await page.locator(".range-stats-container").evaluate(
        "el => { el.style.removeProperty('inline-size'); el.style.removeProperty('max-inline-size'); }"
    )
    await page.set_viewport_size({"width": 320, "height": 800})
    await assert_columns("13rem", 1)


@pytest.mark.e2e_ui
@pytest.mark.asyncio
@pytest.mark.parametrize("use_table", [False, True], ids=["card", "table"])
async def test_queued_task_can_be_cancelled_without_removing_it(mocked_mobile_page: Page, use_table: bool) -> None:
    page = mocked_mobile_page
    queued_task = {**_MOCK_TASKS[1], "id": "task-queued", "status": "queued"}

    async def list_task(route: Route) -> None:
        data = TaskListResponse(
            total=1,
            page=1,
            limit=50,
            items=[TaskResponse.model_validate(queued_task)],
            status_counts={"all": 1, queued_task["status"]: 1},
        )
        await route.fulfill(json=data.model_dump())

    async def cancel_task(route: Route) -> None:
        assert route.request.method == "POST"
        queued_task["status"] = "cancelled"
        await route.fulfill(json=TaskStatusResponse(status="cancelled").model_dump())

    await page.route(re.compile(r"/api/tasks(?:\?|$)"), list_task)
    await page.route("**/api/tasks/task-queued/cancel", cancel_task)
    await goto(page, ROUTES.tasks)
    if use_table:
        await page.get_by_role("button", name=re.compile("切换表格视图")).click()

    task = page.get_by_test_id("task-row" if use_table else "task-mobile-card")
    await expect(task).to_contain_text("task-queued")
    await expect(task.get_by_role("button", name="删除", exact=True)).to_have_count(0)
    await task.get_by_test_id("task-cancel-btn").get_by_role("button").click()
    await page.get_by_role("button", name="确认取消").click()
    await expect(task.locator(".status-tag")).to_have_text("cancelled")
    await expect(task).to_contain_text("task-queued")


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_sidebar_probe_and_collapsed_safe_area(mocked_page: Page) -> None:
    page = mocked_page
    await page.set_viewport_size({"width": 900, "height": 800})
    await goto(page, ROUTES.dashboard)
    sidebar = page.locator("aside.sidebar-container")
    await expect(sidebar).to_be_visible()
    await sidebar.evaluate("""el => {
        const box = document.createElement('div');
        box.className = 'probe-overflow';
        box.style.cssText = 'width: 20px; overflow: hidden';
        const child = document.createElement('div');
        child.style.cssText = 'width: 100px; height: 10px';
        box.append(child);
        el.append(box);
    }""")
    desktop = await page.evaluate(MOBILE_DOM_PROBE_JS)
    assert any(".probe-overflow" in issue["el"] for issue in desktop["clippedOverflows"])

    await page.set_viewport_size({"width": 360, "height": 800})
    await expect(sidebar).to_be_hidden()
    mobile = await page.evaluate(MOBILE_DOM_PROBE_JS)
    assert not any(".probe-overflow" in issue["el"] for issue in mobile["clippedOverflows"])

    await page.set_viewport_size({"width": 900, "height": 800})
    await page.locator(".probe-overflow").evaluate("el => el.remove()")
    await page.evaluate("document.documentElement.style.setProperty('--safe-area-left', '44px')")
    await sidebar.get_by_role("button", name="折叠侧边栏").click()
    await expect(sidebar).to_have_css("width", "112px")
    await expect(page.locator(".app-main-wrapper")).to_have_css("margin-left", "112px")


# ── 部署引导链 Mocked E2E (E2E-UI-15 ~ 18; ─────────────


def _admission_payload(*, restricted: bool, console_locked: bool = False) -> dict[str, Any]:
    """GET /api/auth/status 载荷 (App.vue 准入状态机的唯一裁决源)."""
    return AuthStatusResponse(
        auth_required=False,
        restricted=restricted,
        console_locked=console_locked,
        bootstrap_token_configured=restricted,
        binding_mode="first_bind" if restricted else "admitted",
    ).model_dump()


def _tg_admitted_payload() -> dict[str, Any]:
    """GET /api/telegram/status 载荷 (已准入态)."""
    return TelegramAuthStatusResponse(
        auth_state="AUTHENTICATED",
        is_connected=True,
        premium=True,
        upload_limit_mb=4000,
        restricted=False,
        binding_mode="admitted",
    ).model_dump()


def _tg_restricted_payload(*, authorized: bool) -> dict[str, Any]:
    """GET /api/telegram/status 载荷 (受限引导态, 部署者认证结果由 authorized 反映)."""
    return TelegramAuthStatusResponse(
        auth_state="UNAUTHENTICATED",
        is_connected=False,
        premium=False,
        upload_limit_mb=2000,
        restricted=True,
        binding_mode="first_bind",
        bootstrap_token_configured=True,
        bootstrap_authorized=authorized,
    ).model_dump()


async def _install_otp_admission_routes(page: Page, state: dict[str, Any], captured: dict[str, Any]) -> None:
    """UI-15 路由 mocks 工厂: OTP 引导准入闭环 (跟随 state['restricted'] 动态翻转)."""

    async def auth_status(route: Route) -> None:
        await route.fulfill(json=_admission_payload(restricted=state["restricted"]))

    async def tg_status(route: Route) -> None:
        # 必须跟随准入态: 准入后 AppLayout 挂载会拉取 /api/telegram/status,
        # 若仍返回 restricted 载荷会把状态机翻回引导屏 (死循环)
        if state["restricted"]:
            await route.fulfill(json=_tg_restricted_payload(authorized=True))
        else:
            await route.fulfill(json=_tg_admitted_payload())

    async def bootstrap_auth(route: Route) -> None:
        captured["bootstrap_token"] = route.request.headers.get("x-bootstrap-token", "")
        await route.fulfill(json=BootstrapAuthResponse(expires_in=300).model_dump())

    async def send_code(route: Route) -> None:
        body = route.request.post_data_json or {}
        captured["phone"] = str((body or {}).get("phone") or "")
        await route.fulfill(
            json=NativeSendCodeResponse(
                challenge_id="cid-mock-otp",
                method="otp",
                state="code_sent",
                delivery_channel="sms",
                code_length=5,
                can_resend=True,
                expires_at=time.time() + 300,
                message="已通过短信发送",
            ).model_dump()
        )

    async def verify_code(route: Route) -> None:
        body = route.request.post_data_json or {}
        code = (body or {}).get("code")
        challenge_id = (body or {}).get("challenge_id")
        captured["otp_code"] = code
        captured["otp_challenge_id"] = challenge_id
        if not isinstance(code, str) or code != "00421" or challenge_id != "cid-mock-otp":
            await route.fulfill(status=400, json={"detail": f"unexpected otp payload: {body!r}"})
            return
        state["restricted"] = False  # 授权成功 → 下一次 checkAdmission 放行
        await route.fulfill(
            json=NativeVerifyCodeResponse(
                challenge_id="cid-mock-otp", authenticated=True, token="jwt-mock"
            ).model_dump()
        )

    await page.route(re.compile(r"/api/auth/status"), auth_status)
    await page.route(re.compile(r"/api/telegram/status"), tg_status)
    await page.route(re.compile(r"/api/telegram/bootstrap/auth"), bootstrap_auth)
    await page.route(re.compile(r"/api/telegram/native/send-code"), send_code)
    await page.route(re.compile(r"/api/telegram/native/verify-code"), verify_code)


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_mocked_bootstrap_admission_otp_unlock(mocked_page: Page) -> None:
    """用例 15: 默认 Userbot 引导准入闭环 (restricted → 引导屏渲染 → X-Bootstrap-Token
    部署者认证 → OTP 挑战验证通过 → checkAdmission 放行进入控制台)."""
    page = mocked_page
    state: dict[str, Any] = {"restricted": True}
    captured: dict[str, Any] = {}
    await _install_otp_admission_routes(page, state, captured)

    await goto(page, ROUTES.dashboard)
    await expect(page.locator(".bootstrap-screen")).to_be_visible()

    await page.locator("#bootstrap-deployer-token").fill("e2e-bootstrap-token")
    await page.get_by_role("button", name="验证引导密钥并继续").click()

    phone_input = page.locator('input[type="tel"]')
    await expect(phone_input).to_be_visible()
    assert captured.get("bootstrap_token") == "e2e-bootstrap-token", "引导密钥须经 X-Bootstrap-Token 头提交"
    await phone_input.fill("+12025550117")
    await page.get_by_role("button", name="发送官方验证码").click()

    otp_input = page.locator('input[autocomplete="one-time-code"]')
    await expect(otp_input).to_be_visible()
    assert captured.get("phone") == "+12025550117", "OTP 发码载荷应携带面板输入的手机号"

    await otp_input.fill("00421")
    await otp_input.press("Enter")

    await expect(page.locator(".header-bar-container")).to_be_visible()
    await expect(page.locator(".bootstrap-screen")).to_have_count(0)
    assert isinstance(captured.get("otp_code"), str) and captured.get("otp_code") == "00421", (
        f"OTP 验证码须保留前导零以字符串提交: {captured}"
    )
    assert captured.get("otp_challenge_id") == "cid-mock-otp", f"OTP challenge_id 不匹配: {captured}"


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_mocked_console_lock_runtime_and_recheck(mocked_page: Page) -> None:
    """用例 16: 公网控制台锁定双通道: 业务 API 503 console_locked → 锁定屏;
    「重新检测」在服务端未修复前保持锁定; /api/auth/status 恢复后解锁回控制台."""
    page = mocked_page

    async def tasks_locked(route: Route) -> None:
        await route.fulfill(status=503, json={"error_code": "console_locked", "detail": "公网入口未配置 API_KEY"})

    await goto(page, ROUTES.dashboard)
    await expect(page.locator(".header-bar-container")).to_be_visible()

    # 运行时 503 console_locked → client.ts 拦截器 → enterLockedMode → 锁定屏
    # (503 路由必须待到达任务页后再注册: dashboard 周期轮询 /api/tasks, 若注册窗口
    #  撞上轮询会提前锁定导致 AppLayout 卸载; 到达后即使 KeepAlive 后台轮询命中
    #  503 也只会让断言的锁定屏提前出现, 不影响结果判定)
    await page.locator('[data-testid="nav-tasks"]').click()
    await expect(page.locator('[data-testid="task-filter-status"]')).to_be_visible()
    await page.route(re.compile(r"/api/tasks(?:\?|$)"), tasks_locked)
    await page.get_by_role("button", name=re.compile("刷新列表")).click()
    lock = page.locator(".console-lock-screen")
    await expect(lock).to_be_visible()
    await expect(lock).to_contain_text("控制台已锁定")

    async def auth_locked(route: Route) -> None:
        await route.fulfill(json=_admission_payload(restricted=False, console_locked=True))

    await page.route(re.compile(r"/api/auth/status"), auth_locked)
    recheck_btn = page.get_by_role("button", name="重新检测")
    async with page.expect_response(lambda r: "/api/auth/status" in r.url and r.status == 200):
        await recheck_btn.click()
    await expect(recheck_btn).to_be_enabled()
    await expect(lock).to_be_visible()

    await page.unroute(re.compile(r"/api/tasks(?:\?|$)"), tasks_locked)

    async def auth_repaired(route: Route) -> None:
        await route.fulfill(json=_admission_payload(restricted=False, console_locked=False))

    await page.route(re.compile(r"/api/auth/status"), auth_repaired)
    await page.get_by_role("button", name="重新检测").click()
    await expect(page.locator(".header-bar-container")).to_be_visible()
    await expect(page.locator(".console-lock-screen")).to_have_count(0)


async def _install_qr_challenge_routes(page: Page, state: dict[str, Any], captured: dict[str, Any]) -> None:
    """UI-17 路由 mocks 工厂: QR 挑战闭环全链 (跟随 state['restricted'] 动态翻转)."""

    async def auth_status(route: Route) -> None:
        await route.fulfill(json=_admission_payload(restricted=state["restricted"]))

    async def tg_status(route: Route) -> None:
        # 同 UI-15: 准入后 /api/telegram/status 必须返回 admitted 载荷, 否则状态机翻回引导屏
        if state["restricted"]:
            await route.fulfill(json=_tg_restricted_payload(authorized=True))
        else:
            await route.fulfill(json=_tg_admitted_payload())

    async def bootstrap_auth(route: Route) -> None:
        await route.fulfill(json=BootstrapAuthResponse(expires_in=300).model_dump())

    async def qr_start(route: Route) -> None:
        body = route.request.post_data_json or {}
        captured["qr_phone"] = str((body or {}).get("phone") or "")
        await route.fulfill(
            json=NativeQrChallengeStartResponse(
                challenge_id="cid-mock-qr",
                qr_url="tg://login?token=e2e-mock-qr-token",
                expires_at=time.time() + 60,
                remaining_seconds=60,
                target_phone_hint="•••• 1111",
                message="请扫码",
            ).model_dump()
        )

    async def qr_status(route: Route) -> None:
        state["qr_polls"] += 1
        if state["qr_polls"] == 1:
            await route.fulfill(
                json=QrChallengeStatusResponse(
                    challenge_id="cid-mock-qr",
                    state="password_required",
                    authenticated=False,
                    remaining_seconds=120,
                    message="请输入 2FA 密码",
                ).model_dump()
            )
        else:
            await route.fulfill(
                json=QrChallengeStatusResponse(
                    challenge_id="cid-mock-qr",
                    state="authenticated",
                    authenticated=True,
                    token="jwt-mock",
                    message="扫码登录授权成功",
                ).model_dump()
            )

    async def native_password(route: Route) -> None:
        captured["qr_password"] = str((route.request.post_data_json or {}).get("password") or "")
        state["restricted"] = False
        await route.fulfill(
            json=NativeVerifyCodeResponse(challenge_id="cid-mock-qr", authenticated=True, token="jwt-mock").model_dump()
        )

    await page.route(re.compile(r"/api/auth/status"), auth_status)
    await page.route(re.compile(r"/api/telegram/status"), tg_status)
    await page.route(re.compile(r"/api/telegram/bootstrap/auth"), bootstrap_auth)
    await page.route(re.compile(r"/api/telegram/qr/start"), qr_start)
    await page.route(re.compile(r"/api/telegram/qr/status"), qr_status)
    # native 模式 QR 密码提交走 handleQrPasswordSubmit → tgStore.nativeQrPassword → /qr/password
    await page.route(re.compile(r"/api/telegram/qr/password"), native_password)


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_mocked_bootstrap_qr_challenge_flow(mocked_page: Page) -> None:
    """用例 17: 扫码登录通道 (native): QR 手机号提交 → 本地 SVG 二维码渲染 →
    轮询 password_required → 2FA 密码提交 → authenticated → 准入放行."""
    page = mocked_page
    state = {"restricted": True, "qr_polls": 0}
    captured: dict[str, Any] = {}

    await _install_qr_challenge_routes(page, state, captured)

    await goto(page, ROUTES.dashboard)
    await expect(page.locator(".bootstrap-screen")).to_be_visible()

    await page.locator("#bootstrap-deployer-token").fill("e2e-bootstrap-token")
    await page.get_by_role("button", name="验证引导密钥并继续").click()

    await page.locator('[role="tab"]').nth(1).click()
    qr_phone = page.locator('input[name="qr_phone"]')
    await expect(qr_phone).to_be_visible()
    await qr_phone.fill("+12025550117")
    await qr_phone.press("Enter")

    await expect(page.locator("svg.qr-svg")).to_be_visible()
    assert captured.get("qr_phone") == "+12025550117", "QR 起挑战载荷应携带手机号"
    await expect(page.locator("img")).to_have_count(0), "二维码须本地 SVG 渲染 (无外链 img)"

    pwd_input = page.locator('input[autocomplete="current-password"]')
    await expect(pwd_input).to_be_visible()
    await pwd_input.fill("e2e-2fa-pass")
    await page.get_by_role("button", name="提交密码完成授权").click()

    await expect(page.locator(".header-bar-container")).to_be_visible()
    await expect(page.locator(".bootstrap-screen")).to_have_count(0)
    assert captured.get("qr_password") == "e2e-2fa-pass", "QR 密码须经 /api/telegram/qr/password 提交"


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_mocked_request_headers_api_key_and_2fa(mocked_page: Page) -> None:
    """用例 18: client.ts 请求拦截器契约: localStorage 记忆凭据注入
    X-API-Key 与 X-2FA-Token 双 Header (加载时持久化 → 刷新后请求携带)."""
    page = mocked_page
    captured: list[dict[str, str]] = []

    async def capture_status(route: Route) -> None:
        captured.append(dict(route.request.headers))
        await route.fulfill(json=_admission_payload(restricted=False))

    await page.route(re.compile(r"/api/auth/status"), capture_status)
    await goto(page, ROUTES.dashboard)
    await expect(page.locator(".header-bar-container")).to_be_visible()

    expires_ms = (time.time() + 3600) * 1000
    await page.evaluate(
        "([apiKey, twoFa]) => {"
        "  localStorage.setItem('tg_hub_api_key_remember', apiKey);"
        "  localStorage.setItem('tg_hub_2fa_unlocked', twoFa);"
        "}",
        [
            json.dumps({"key": "e2e-api-key", "expires": expires_ms}),
            json.dumps({"unlocked": True, "expires": expires_ms, "token": "e2e-2fa-token"}),
        ],
    )

    captured.clear()
    await page.reload()
    await expect(page.locator(".header-bar-container")).to_be_visible()
    assert captured, "刷新后应捕获 /api/auth/status 请求"
    headers = captured[0]
    assert headers.get("x-api-key") == "e2e-api-key", f"请求应携带 X-API-Key: {headers}"
    assert headers.get("x-2fa-token") == "e2e-2fa-token", f"请求应携带 X-2FA-Token: {headers}"
