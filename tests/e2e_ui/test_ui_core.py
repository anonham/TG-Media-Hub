"""E2E-UI-01 / 13 / 14 核心导航、表单保持与主题持久化 (本地-dummy 子集).

语义真源: docs/testing/E2E_GUIDE.md §4
"""

from __future__ import annotations

import re

import pytest
from playwright.async_api import Page, expect

from .helpers import ROUTES, api_get, goto


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_ui_01_global_navigation_and_smoke(page: Page) -> None:
    """E2E-UI-01 全局导航与页面健康冒烟."""
    pages = [
        (ROUTES.dashboard, re.compile(r"欢迎使用 Telegram Media Hub|总速率")),
        (ROUTES.create, re.compile(r"受限媒体下载")),
        (ROUTES.pikpak, re.compile(r"PikPak 账号鉴权")),
        (ROUTES.tasks, re.compile(r"瞬时总传输速率")),
        (ROUTES.accounts, re.compile(r"Userbot 账号管理")),
        (ROUTES.storage, re.compile(r"存储占用总览与磁盘容量")),
        (ROUTES.logs, re.compile(r"系统运行日志")),
        (ROUTES.settings, re.compile(r"应用设置")),
    ]
    main_content = page.locator(".app-main-wrapper main, main, .n-layout-content").first
    for hash_route, marker in pages:
        await goto(page, hash_route)
        # 懒加载路由 chunk 换装有窗口期 (入口分包后更明显): hash 同文档导航的
        # domcontentloaded 立即返回, main_text 可能在旧视图上误读 —
        # expect 自动重试直至目标视图真正渲染, 再读文本做内容断言
        await expect(main_content).to_contain_text(marker, timeout=10_000)
        text = await main_content.inner_text()
        assert len(text) > 50, f"页面 {hash_route} 主内容为空"

    # 版本一致性断言: 侧栏 (v 前缀) 与后端 app_version
    sidebar_version = await page.evaluate(
        r"""() => {
            const sider = document.querySelector(".n-layout-sider, aside");
            const m = (sider?.innerText ?? "").match(/v[\d.]+/);
            return m ? m[0] : (document.body.innerText.match(/v[\d.]+/)?.[0] ?? "");
        }"""
    )
    backend = await api_get(page.request, "/api/settings")
    assert sidebar_version, "未能提取侧栏版本号"
    assert sidebar_version.replace("v", "") == str(backend.get("app_version", "")).strip()


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_ui_13_create_form_keepalive_and_default_account(page: Page) -> None:
    """E2E-UI-13 创建表单 KeepAlive 与预设/账号选项一致性."""
    await goto(page, ROUTES.create)

    # 子视图 A 填值 (KeepAlive 按需挂载: async chunk 就绪前 textarea 尚不在 DOM, 须等待)
    textarea = page.get_by_placeholder("https://t.me/c/[频道ID]", exact=False)
    await expect(textarea).to_have_count(1)
    await textarea.fill("https://t.me/c/9000000010/16074 e2e_ui13_marker")

    # 切 B → 默认项断言 → 切回 A → 值保留 (KeepAlive)
    # 模式切换按钮为语义化 role="tab" (现代 Web P1 ARIA tabs), 不再是隐式 button role
    await page.get_by_role("tab", name=re.compile(r"本地文件夹发布")).click()
    await expect(page.get_by_text("系统自动调度 (默认负载均衡)").first).to_be_visible()

    await page.get_by_role("tab", name=re.compile(r"受限媒体下载")).click()
    await expect(textarea).to_have_value(re.compile(r"e2e_ui13_marker"))

    # 子视图 C 默认项
    await page.get_by_role("tab", name=re.compile(r"云端极速转发")).click()
    await expect(page.get_by_text("系统自动调度 (默认负载均衡)").first).to_be_visible()


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_ui_14_theme_switch_and_sidebar_collapse_persisted(page: Page) -> None:
    """E2E-UI-14 主题切换与侧栏折叠持久化."""
    await goto(page, ROUTES.settings)

    # 折叠侧栏 → 刷新 → 持久化
    collapse = page.get_by_role("button", name="折叠侧边栏")
    if await collapse.count() == 1:
        await collapse.click()
        await expect(page.get_by_role("button", name="展开侧边栏")).to_be_visible()
        persisted = await page.evaluate("() => localStorage.getItem('tg_hub_sidebar_collapsed')")
        assert persisted == "true"
        await page.reload()
        await page.wait_for_load_state("domcontentloaded")
        await expect(page.get_by_role("button", name="展开侧边栏")).to_be_visible()
        still_collapsed = await page.evaluate("() => localStorage.getItem('tg_hub_sidebar_collapsed')")
        assert still_collapsed == "true"

    # 主题切深色 → data-theme 跟随 → 切回跟随系统
    # (P0-P1 变更后默认主题为 'system': n-select 当前值显示"跟随系统 (System)"
    # 不可再按旧默认 light 假设定位显示文本 — 统一经 testid 打开下拉)
    theme_row = page.locator(".setting-row-item").filter(has_text="应用主题")
    theme_select = theme_row.locator("[data-testid='theme-select']")
    dropdown = page.locator(".n-select-dropdown, [class*='select-menu']")
    await theme_select.click()
    await expect(dropdown).to_be_visible()
    await dropdown.get_by_text("深色模式 (Dark)").click()
    await expect(page.locator("html")).to_have_attribute("data-theme", "dark")

    # 恢复跟随系统 (清理)
    await theme_select.click()
    await expect(dropdown).to_be_visible()
    await dropdown.get_by_text("跟随系统 (System)").click()
    await expect(page.locator("html")).not_to_have_attribute("data-theme", "dark")

    # 侧栏恢复展开 (清理)
    expand = page.get_by_role("button", name="展开侧边栏")
    if await expand.count() == 1:
        await expand.click()
