"""E2E-UI-10 / 11 存储跨页防误删与设置中心六标签保存/校验流 (本地-dummy 子集).

语义真源: docs/testing/E2E_GUIDE.md §4
"""

from __future__ import annotations

import re

import pytest
from playwright.async_api import Page, expect

from .helpers import ROUTES, api_get, goto, main_text, setting_value


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_ui_10_storage_cross_page_selection_and_cancel_safety(page: Page) -> None:
    """E2E-UI-10 存储页跨页选中/覆盖条/批量删除对话框仅到取消 (零删除安全)."""
    await goto(page, ROUTES.storage)
    # 存储页为 async chunk + 异步数据加载: 等待总览卡片渲染后再读主内容 (首读竞态加固)
    await expect(page.locator("#app main, main, .n-layout-content").first).to_contain_text(
        "存储占用总览与磁盘容量", timeout=15_000
    )
    text = await main_text(page)
    assert re.search(r"存储占用总览与磁盘容量", text)
    assert re.search(r"配额", text)

    # 第 1 页勾选两行 → 批量删除计数出现
    await page.locator("table .n-checkbox").nth(1).click()
    await page.locator("table .n-checkbox").nth(2).click()
    await expect(page.get_by_role("button", name=re.compile(r"批量删除 \(2\)"))).to_be_visible()

    # 跨页: 翻第 2 页再勾 1 项 → 计数 3 → 回翻第 1 页勾选保持
    await page.locator(".n-pagination").get_by_text("2", exact=True).click()
    await page.locator("table .n-checkbox").nth(1).click()
    await expect(page.get_by_role("button", name=re.compile(r"批量删除 \(3\)"))).to_be_visible()

    await page.locator(".n-pagination").get_by_text("1", exact=True).click()
    await expect(page.locator("table .n-checkbox--checked").first).to_be_visible()
    checked = await page.evaluate(
        """() => {
            return Array.from(document.querySelectorAll("table .n-checkbox"))
                .map((c, i) => (c.classList.contains("n-checkbox--checked") ? i : -1))
                .filter((i) => i >= 0);
        }"""
    )
    assert 1 in checked
    assert 2 in checked

    # 批量删除对话框: 清单含跨页项 → 立即取消 (绝不确认删除)
    await page.get_by_role("button", name=re.compile(r"批量删除 \(3\)")).click()
    dialog = page.locator(".n-modal")
    await expect(dialog).to_contain_text("确认批量删除")
    await expect(dialog).to_contain_text("跨页已选")
    await dialog.get_by_role("button", name="取消").click()
    await expect(dialog).to_have_count(0)

    # 取消后零删除断言 (预置的 65 个文件全量保持)
    overview = await api_get(page.request, "/api/storage/overview")
    assert overview.get("file_count") == 65


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_ui_11_settings_center_tabs_save_and_clamp(page: Page) -> None:
    """E2E-UI-11 设置中心六标签保存流与非法值钳制."""
    await goto(page, ROUTES.settings)

    # 六标签正常渲染 (语义化 role="tab", 现代 Web P1 ARIA tabs)
    for tab_name in ["通用设置", "下载管理", "频道转发", "本地发布", "PikPak 搬运", "关于与系统"]:
        await expect(page.get_by_role("tab", name=tab_name, exact=True)).to_be_visible()

    # PikPak 标签: 改值保存 → REST 复核 → 切签回显
    await page.get_by_role("tab", name="PikPak 搬运").click()
    input_el = page.locator(".setting-row-item").filter(has_text="pikpak_prefetch_concurrency").locator("input")
    await expect(input_el).to_be_visible()
    original = str(await setting_value(page.request, "pikpak_prefetch_concurrency"))
    bumped = str(min(int(original or "3") + 1, 16))
    await input_el.fill(bumped)
    await page.get_by_role("button", name="保存【PikPak 搬运】").click()
    await expect(page.locator(".n-message").filter(has_text="系统动态配置已保存").first).to_be_visible()
    # toast 自动消失后再进行下一步, 避免上一步 toast 残留被下一步误匹配 (全量语境时序竞态)
    await expect(page.locator(".n-message")).to_have_count(0, timeout=8_000)
    assert str(await setting_value(page.request, "pikpak_prefetch_concurrency")) == bumped

    # 非法值: 上限外数值被钳制
    await input_el.fill("99")
    await page.get_by_role("button", name="保存【PikPak 搬运】").click()
    await expect(page.locator(".n-message").filter(has_text="系统动态配置已保存").first).to_be_visible()
    await expect(page.locator(".n-message")).to_have_count(0, timeout=8_000)
    clamped = int(await setting_value(page.request, "pikpak_prefetch_concurrency"))
    assert clamped <= 16

    # 恢复原值 (清理)
    await input_el.fill(original)
    await page.get_by_role("button", name="保存【PikPak 搬运】").click()
    await expect(page.locator(".n-message").filter(has_text="系统动态配置已保存").first).to_be_visible()
    await expect(page.locator(".n-message")).to_have_count(0, timeout=8_000)
    assert str(await setting_value(page.request, "pikpak_prefetch_concurrency")) == original
