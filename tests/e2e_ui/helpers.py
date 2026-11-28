"""UI E2E 纯异步辅助函数 (对齐原 TS 语义与 docs/testing/E2E_GUIDE.md)."""

from __future__ import annotations

import asyncio
import os
from typing import Any

from playwright.async_api import APIRequestContext, Page, expect

BACKEND_BASE_URL = os.environ.get("E2E_UI_BACKEND_URL", "http://127.0.0.1:8500").rstrip("/")


class ROUTES:
    dashboard = "/static/#/dashboard"
    create = "/static/#/tasks/create"
    pikpak = "/static/#/pikpak"
    tasks = "/static/#/tasks"
    accounts = "/static/#/accounts"
    storage = "/static/#/storage"
    logs = "/static/#/logs"
    settings = "/static/#/settings"


async def goto(page: Page, hash_route: str) -> None:
    """导航至目标 hash 路由并等待 DOMContentLoaded 与应用根节点就绪."""
    await page.goto(hash_route)
    await page.wait_for_load_state("domcontentloaded")
    await page.locator("#app").wait_for(state="visible")


async def main_text(page: Page) -> str:
    """读取主内容区文本 (用于路由渲染健康检查)."""
    return await page.evaluate(
        """() => {
            const main = document.querySelector(".app-main-wrapper main, main, .n-layout-content");
            return main ? main.innerText : "";
        }"""
    )


async def api_get(request: APIRequestContext, api_path: str) -> Any:
    """open-mode 后端 GET 复核."""
    resp = await request.get(f"{BACKEND_BASE_URL}{api_path}")
    assert resp.ok, f"GET {api_path} failed with status {resp.status}"
    return await resp.json()


async def api_post(request: APIRequestContext, api_path: str, body: Any = None) -> Any:
    """open-mode 后端 POST 请求."""
    resp = await request.post(f"{BACKEND_BASE_URL}{api_path}", data=body)
    assert resp.ok, f"POST {api_path} failed with status {resp.status}"
    return await resp.json()


async def api_delete(request: APIRequestContext, api_path: str) -> int:
    """open-mode 后端 DELETE 请求."""
    resp = await request.delete(f"{BACKEND_BASE_URL}{api_path}")
    return resp.status


async def setting_value(request: APIRequestContext, key: str) -> Any:
    """递归遍历获取系统配置项的值."""
    data = await api_get(request, "/api/settings")
    found: list[Any] = []

    def walk(obj: Any) -> None:
        if not isinstance(obj, dict):
            return
        for k, v in obj.items():
            if k == key:
                found.append(v)
            elif isinstance(v, dict):
                walk(v)

    walk(data)
    return found[0] if found else None


async def drawer_title(page: Page) -> str:
    """读取当前展开抽屉的摘要文本 (无抽屉时返回 '(no drawer)')."""
    return await page.evaluate(
        r"""() => {
            const d = document.querySelector('[data-testid="task-detail-drawer"], .n-drawer');
            return d ? d.innerText.replace(/\n+/g, ' | ').slice(0, 220) : '(no drawer)';
        }"""
    )


async def close_drawer(page: Page) -> None:
    """关闭当前展开的抽屉 (如果存在); Naive UI 关闭按钮为 .n-drawer-header__close > .n-base-close
    (无 "close" 可访问名; trap-focus=false 时 ESC 不生效), 关闭失败响亮报错.
    门禁打 .task-detail-drawer 外壳: loading 态下 .drawer-body testid 尚不存在."""
    drawer = page.locator(".task-detail-drawer")
    if await drawer.count() == 0:
        return
    close_btn = page.locator(".n-drawer-header__close, .n-base-close").first
    if await close_btn.count() >= 1:
        await close_btn.click()
    else:
        await page.keyboard.press("Escape")
    await expect(drawer).to_have_count(0, timeout=5000)


async def seed_publish_task(
    request: APIRequestContext,
    tag: str,
    ui_manifest: dict[str, Any],
    target_channel: str,
) -> str:
    """投放一个本地小样本发布任务 (6 × ~20 MB, 数分钟级运行窗口)."""
    if not target_channel or not target_channel.strip():
        raise RuntimeError("target_channel 不能为空 (禁止无显式测试频道投放任务)")
    folder = ui_manifest.get("folders", {}).get(tag)
    if not folder:
        raise RuntimeError(f"预置任务文件夹缺失: {tag}")
    data = await api_post(
        request,
        "/api/publish/folder",
        {
            "folder_path": folder,
            "target_channel": target_channel,
            "album_mode": False,
            "delay_seconds": 3.0,
            "send_placeholder_card": False,
            "oversize_policy": "skip_and_report",
            "force_republish": True,
        },
    )
    return str(data["task_id"])


async def ensure_running_task(
    request: APIRequestContext,
    prefer_id: str,
    ui_manifest: dict[str, Any],
    target_channel: str,
    registry: list[str] | None = None,
) -> str:
    """确保有一个 running 语义的观测任务: 指派任务已终局时补投并等待进入 running."""
    if not target_channel or not target_channel.strip():
        raise RuntimeError("target_channel 不能为空 (禁止无显式测试频道补投任务)")

    async def is_running(task_id: str) -> bool:
        try:
            d = await api_get(request, f"/api/tasks/{task_id}")
            return str(d.get("status")) == "running"
        except Exception:
            return False

    if prefer_id and await is_running(prefer_id):
        return prefer_id

    folder = ui_manifest.get("folders", {}).get("ui_task_a")
    if not folder:
        raise RuntimeError("预置任务文件夹缺失: ui_task_a")
    data = await api_post(
        request,
        "/api/publish/folder",
        {
            "folder_path": folder,
            "target_channel": target_channel,
            "album_mode": False,
            "delay_seconds": 3.0,
            "send_placeholder_card": False,
            "oversize_policy": "skip_and_report",
            "force_republish": True,
        },
    )
    new_id = str(data["task_id"])
    if registry is not None:
        registry.append(new_id)

    for _ in range(300):
        if await is_running(new_id):
            return new_id
        await asyncio.sleep(2.0)
    raise RuntimeError("补投任务 600s 内未进入 running 状态")


MOBILE_DOM_PROBE_JS = r"""() => {
  const vw = window.innerWidth;
  const vh = window.innerHeight;
  const docScrollWidth = document.documentElement.scrollWidth;
  const bodyScrollWidth = document.body.scrollWidth;

  function describeEl(el) {
    const tag = el.tagName.toLowerCase();
    const id = el.id ? '#' + el.id : '';
    const cls = typeof el.className === 'string' && el.className.trim()
      ? '.' + el.className.trim().split(/\s+/).slice(0, 3).join('.')
      : '';
    const text = (el.innerText || el.getAttribute('aria-label') || el.getAttribute('placeholder') || '')
      .replace(/\s+/g, ' ').trim().slice(0, 36);
    return `${tag}${id}${cls}${text ? `("${text}")` : ''}`;
  }

  function isVisible(el) {
    // 收起的移动端抽屉不可见，桌面侧栏仍需扫描。
    const sidebar = el.closest('aside.sidebar-container');
    if (sidebar && window.getComputedStyle(sidebar).visibility === 'hidden') {
      return false;
    }
    const style = window.getComputedStyle(el);
    if (style.display === 'none' || style.visibility === 'hidden' || parseFloat(style.opacity) === 0) {
      return false;
    }
    const r = el.getBoundingClientRect();
    return r.width > 0 && r.height > 0;
  }

  function isInsideIntentionalHorizontalScroller(el) {
    let cur = el.parentElement;
    while (cur && cur !== document.body) {
      const st = window.getComputedStyle(cur);
      if (st.overflowX === 'auto' || st.overflowX === 'scroll') {
        return true;
      }
      cur = cur.parentElement;
    }
    return false;
  }

  function hasClippingBoundaryToScroller(el) {
    const st = window.getComputedStyle(el);
    if (st.overflowX === 'hidden' || st.overflowX === 'clip') {
      return true;
    }
    let cur = el.parentElement;
    while (cur && cur !== document.body) {
      const cst = window.getComputedStyle(cur);
      if (cst.overflowX === 'auto' || cst.overflowX === 'scroll') {
        return false;
      }
      if (cst.overflowX === 'hidden' || cst.overflowX === 'clip') {
        return true;
      }
      cur = cur.parentElement;
    }
    return false;
  }

  const fixedOrViewportOverflows = [];
  const clippedContainerOverflows = [];
  const smallTouchTargets = [];

  const allEls = document.querySelectorAll('body *');
  for (const el of allEls) {
    if (!isVisible(el)) continue;
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();

    // 1) 固定定位或直接突破视口左右边界的元素 (即使 body overflow-x: hidden 也能捕获)
    const isFixed = style.position === 'fixed';
    const inScroller = !isFixed && isInsideIntentionalHorizontalScroller(el);
    if (!inScroller && (rect.right > vw + 2 || rect.left < -2)) {
      fixedOrViewportOverflows.push({
        el: describeEl(el),
        left: Math.round(rect.left),
        right: Math.round(rect.right),
        width: Math.round(rect.width),
        vw,
        position: style.position,
      });
    }

    // 2) 非有意滚动容器却发生内部横向撑爆 (被 overflow-x: hidden 裁切/隐藏的根因)
    const isIntentionalScroller =
      style.overflowX === 'auto' || style.overflowX === 'scroll' ||
      style.textOverflow === 'ellipsis' ||
      el.classList.contains('n-scrollbar-container') ||
      el.classList.contains('n-tabs-nav-scroll-wrapper') ||
      el.classList.contains('n-input__placeholder') ||
      el.classList.contains('n-base-selection-placeholder__inner') ||
      el.classList.contains('n-base-selection-overlay__wrapper') ||
      el.classList.contains('n-spin-body') ||
      el.classList.contains('n-base-loading') ||
      el.classList.contains('n-base-loading__transition-wrapper');
    if (
      !isIntentionalScroller &&
      (!inScroller || hasClippingBoundaryToScroller(el)) &&
      el.clientWidth > 0 &&
      el.scrollWidth > el.clientWidth + 4 &&
      ['DIV', 'HEADER', 'MAIN', 'SECTION', 'ARTICLE', 'FORM', 'SEARCH', 'NAV', 'UL', 'OL', 'PRE', 'TABLE'].includes(el.tagName)
    ) {
      clippedContainerOverflows.push({
        el: describeEl(el),
        clientWidth: Math.round(el.clientWidth),
        scrollWidth: Math.round(el.scrollWidth),
        overflowPx: Math.round(el.scrollWidth - el.clientWidth),
      });
    }

    // 3) 可交互元素触控热区扫描 (< 32px 视为高风险小热区)
    const isInteractive =
      el.matches('button, a[href], input:not([type="hidden"]), select, [role="button"], [role="tab"]') &&
      !el.closest('.n-pagination-item');
    if (isInteractive && (rect.height < 32 || rect.width < 32)) {
      smallTouchTargets.push({
        el: describeEl(el),
        width: Math.round(rect.width),
        height: Math.round(rect.height),
      });
    }
  }

  const dedup = (arr, keyFn) => {
    const seen = new Set();
    return arr.filter((item) => {
      const k = keyFn(item);
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
  };

  return {
    vw,
    vh,
    docScrollWidth,
    bodyScrollWidth,
    hasDocOverflow: docScrollWidth > vw + 1 || bodyScrollWidth > vw + 1,
    viewportOverflowCount: fixedOrViewportOverflows.length,
    viewportOverflows: dedup(fixedOrViewportOverflows, (x) => x.el).slice(0, 15),
    clippedOverflowCount: clippedContainerOverflows.length,
    clippedOverflows: dedup(clippedContainerOverflows, (x) => x.el).slice(0, 15),
    smallTouchTargetCount: smallTouchTargets.length,
    smallTouchTargets: dedup(smallTouchTargets, (x) => x.el).slice(0, 15),
  };
}"""
