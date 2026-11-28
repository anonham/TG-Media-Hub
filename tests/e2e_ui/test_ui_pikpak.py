"""E2E-UI-04 / 05 / 12 固定 SKIP 用例 (环境红线与 FloodWait 自然不可触发口径).

语义真源: docs/testing/E2E_GUIDE.md §4
- UI-04: FloodWait 是预期产物，无法自然定时触发
- UI-05: 需运行中的 PikPak 发布任务 = 用户真实网盘文件 (隐私红线)
- UI-12: 网盘文件前置 + 任务取消清理依赖真实远端文件选择 (隐私红线)
"""

from __future__ import annotations

import re

import pytest
from playwright.async_api import Page

from .helpers import ROUTES, goto, main_text


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_ui_04_flood_wait_banner_countdown(page: Page) -> None:
    """E2E-UI-04 FloodWait 倒计时横幅闭环 (环境允许时执行)."""
    pytest.skip("FloodWait 无法自然触发 (预期产物非故障); 触发口径见 docs/testing/E2E_GUIDE.md")
    await goto(page, ROUTES.tasks)
    assert re.search(r"瞬时总传输速率", await main_text(page))


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_ui_05_pikpak_dual_speed_dashboard(page: Page) -> None:
    """E2E-UI-05 PikPak 双速率仪表盘与抽屉双曲线 (需远端测试文件)."""
    pytest.skip("需运行中的 PikPak 发布任务 = 用户真实网盘文件 (隐私红线); 预置远端测试目录后解除")
    await goto(page, ROUTES.pikpak)
    assert re.search(r"PikPak 账号鉴权", await main_text(page))


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_ui_12_pikpak_workbench_selector_and_payload_contract(page: Page) -> None:
    """E2E-UI-12 PikPak 工作台选择器显隐与提交载荷契约 (需远端测试文件)."""
    pytest.skip("需网盘测试文件供选择 (隐私红线); 提交载荷断言由 harness REST 侧复核")
    await goto(page, ROUTES.pikpak)
    assert re.search(r"发布配置", await main_text(page))
