"""移动端视口自动化诊断探针 (412×915 / 360×800).

复用 test_ui_mocked.py 的静态 SPA HTTP 处理器与 Pydantic 校验 Mock API 路由，
对全站 8 大路由及关键交互态（批量操作栏、详情抽屉、多 Tab 表单）执行：
1. 真实物理视口 + DPR=3.5 + Touch 仿真渲染
2. DOM 几何探针：检测文档溢出、固定定位溢出（含 overflow-x: hidden 掩盖下的容器截断）与过小触控热区
3. 留存高分辨率截图至 tests/.tmp/mobile_snapshots/<tag>/

用法:
  uv run python tests/e2e_ui/inspect_mobile_viewport.py --tag baseline
  uv run python tests/e2e_ui/inspect_mobile_viewport.py --tag after --assert-zero-overflow
"""

from __future__ import annotations

import argparse
import asyncio
import http.server
import json
import sys
import threading
from pathlib import Path
from typing import Any

from playwright.async_api import Page, async_playwright

_PACKAGE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _PACKAGE_DIR.parents[1]
_STATIC_DIR = _REPO_ROOT / "static"

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_TESTS_DIR = _REPO_ROOT / "tests"
if str(_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(_TESTS_DIR))

from tests.e2e_ui.conftest import resolve_cached_chromium  # noqa: E402
from tests.e2e_ui.helpers import MOBILE_DOM_PROBE_JS, ROUTES, close_drawer, goto  # noqa: E402
from tests.e2e_ui.test_ui_mocked import _handle_mock_api, _SPAHTTPRequestHandler  # noqa: E402

_REQUIRED_SCENES = (
    "01_dashboard",
    "02_tasks",
    "03_tasks_batch_bar",
    "04_tasks_drawer",
    "04b_tasks_drawer_params",
    "04c_tasks_drawer_diagnosis",
    "05_create_download",
    "06_create_publish",
    "07_create_forward",
    "08_pikpak",
    "09_accounts",
    "10_storage",
    "11_logs",
    "12_settings",
)


def _start_mock_spa_server() -> tuple[http.server.ThreadingHTTPServer, str]:
    if not (_STATIC_DIR / "index.html").exists():
        raise RuntimeError("static/index.html 不存在，请先执行 `cd frontend && pnpm run build`")
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _SPAHTTPRequestHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{port}"


async def _inspect_scene(page: Page, out_dir: Path, results: dict[str, Any], name: str, full_page: bool = True) -> None:
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_timeout(500)
    metrics = await page.evaluate(MOBILE_DOM_PROBE_JS)
    screenshot_path = out_dir / f"{name}.png"
    await page.screenshot(path=str(screenshot_path), full_page=full_page, animations="disabled", timeout=30_000)
    metrics["screenshot"] = str(screenshot_path)
    results["scenes"][name] = metrics


async def _probe_tasks_scenes(page: Page, out_dir: Path, results: dict[str, Any]) -> None:
    await goto(page, ROUTES.tasks)
    await page.locator(".task-center-page").wait_for(state="visible")
    await _inspect_scene(page, out_dir, results, "02_tasks")

    first_checkbox = page.locator(".task-center-page .n-checkbox").nth(1)
    await first_checkbox.wait_for(state="visible")
    await first_checkbox.click()
    await page.wait_for_timeout(300)
    await _inspect_scene(page, out_dir, results, "03_tasks_batch_bar", full_page=False)
    await first_checkbox.click()
    await page.wait_for_timeout(200)

    detail_trigger = page.locator(
        '[data-testid="task-detail-btn"] button, [data-testid="task-row"] .task-id-cell'
    ).first
    await detail_trigger.wait_for(state="visible")
    await detail_trigger.click()
    await page.locator('[data-testid="task-detail-drawer"]').wait_for(state="visible")
    await _inspect_scene(page, out_dir, results, "04_tasks_drawer", full_page=False)
    await page.locator('[data-testid="tab-params"]').click()
    await _inspect_scene(page, out_dir, results, "04b_tasks_drawer_params", full_page=False)
    await page.locator('[data-testid="tab-diagnosis"]').click()
    await _inspect_scene(page, out_dir, results, "04c_tasks_drawer_diagnosis", full_page=False)
    await close_drawer(page)


async def _probe_all_routes(page: Page, out_dir: Path, results: dict[str, Any]) -> None:
    await goto(page, ROUTES.dashboard)
    await page.locator(".dashboard-container").wait_for(state="visible")
    await _inspect_scene(page, out_dir, results, "01_dashboard")

    await _probe_tasks_scenes(page, out_dir, results)

    await goto(page, ROUTES.create)
    await page.locator(".task-create-container").wait_for(state="visible")
    await _inspect_scene(page, out_dir, results, "05_create_download")
    await page.locator('[data-testid="tab-publish"]').click()
    await _inspect_scene(page, out_dir, results, "06_create_publish")
    await page.locator('[data-testid="tab-forward"]').click()
    await _inspect_scene(page, out_dir, results, "07_create_forward")

    route_targets = [
        ("08_pikpak", ROUTES.pikpak, ".tree-toolbar"),
        ("09_accounts", ROUTES.accounts, ".accounts-view-container"),
        ("10_storage", ROUTES.storage, ".storage-view-container"),
        ("11_logs", ROUTES.logs, ".logs-view-container"),
        ("12_settings", ROUTES.settings, ".settings-page-wrapper"),
    ]
    for name, route, selector in route_targets:
        await goto(page, route)
        await page.locator(selector).wait_for(state="visible")
        await _inspect_scene(page, out_dir, results, name)


def _emit_report(results: dict[str, Any], out_dir: Path, assert_zero_overflow: bool) -> None:
    report_path = out_dir / "report.json"
    report_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

    vp = results["viewport"]
    print(f"\n=== 移动端视口诊断报告 [{results['tag']}] ({vp['width']}x{vp['height']} @ DPR={vp['dpr']}) ===")
    total_viewport_issues = 0
    total_clipped_issues = 0
    for scene, m in results["scenes"].items():
        vp_cnt = m["viewportOverflowCount"]
        cl_cnt = m["clippedOverflowCount"]
        sm_cnt = m["smallTouchTargetCount"]
        doc_ov = "DOC_OVERFLOW" if m["hasDocOverflow"] else "OK"
        total_viewport_issues += vp_cnt + (1 if m["hasDocOverflow"] else 0)
        total_clipped_issues += cl_cnt
        print(
            f"  {scene:<20} | {doc_ov:<12} | 视口溢出={vp_cnt:<2} | 容器被截断溢出={cl_cnt:<2} | 过小热区={sm_cnt:<2}"
        )
        for ov in m["viewportOverflows"][:5]:
            print(
                f"    [视口溢出] {ov['el']} (left={ov['left']}, right={ov['right']}, vw={ov['vw']}, pos={ov['position']})"
            )
        for cl in m["clippedOverflows"][:5]:
            print(
                f"    [容器撑爆] {cl['el']} (client={cl['clientWidth']}, scroll={cl['scrollWidth']}, +{cl['overflowPx']}px)"
            )

    print(f"\n报告与截图已存至: {out_dir}")

    missing_scenes = [s for s in _REQUIRED_SCENES if s not in results["scenes"]]
    if assert_zero_overflow and (missing_scenes or total_viewport_issues > 0 or total_clipped_issues > 0):
        raise SystemExit(
            f"断言失败: 缺失场景={missing_scenes}, 视口溢出={total_viewport_issues}, 容器撑爆={total_clipped_issues}"
        )


async def run_probe(width: int, height: int, dpr: float, tag: str, assert_zero_overflow: bool) -> dict[str, Any]:
    server, base_url = _start_mock_spa_server()
    out_dir = _REPO_ROOT / "tests" / ".tmp" / "mobile_snapshots" / f"{tag}_{width}x{height}"
    out_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, Any] = {"viewport": {"width": width, "height": height, "dpr": dpr}, "tag": tag, "scenes": {}}

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(executable_path=resolve_cached_chromium(), headless=True)
            try:
                context = await browser.new_context(
                    base_url=base_url,
                    viewport={"width": width, "height": height},
                    device_scale_factor=dpr,
                    is_mobile=True,
                    has_touch=True,
                    user_agent=(
                        "Mozilla/5.0 (Linux; Android 15; Mobile) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"
                    ),
                )
                try:
                    page = await context.new_page()
                    page.set_default_timeout(15_000)
                    await page.route("**/api/**", _handle_mock_api)
                    await _probe_all_routes(page, out_dir, results)
                finally:
                    await context.close()
            finally:
                await browser.close()
    finally:
        server.shutdown()
        server.server_close()

    _emit_report(results, out_dir, assert_zero_overflow)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="移动端视口 DOM 探针与截图工具")
    parser.add_argument("--width", type=int, default=412, help="CSS 逻辑宽度 (默认 412)")
    parser.add_argument("--height", type=int, default=915, help="CSS 逻辑高度 (默认 915)")
    parser.add_argument("--dpr", type=float, default=3.5, help="设备像素比 DPR (默认 3.5)")
    parser.add_argument("--tag", type=str, default="baseline", help="输出目录标签")
    parser.add_argument("--assert-zero-overflow", action="store_true", help="发现溢出时返回非零退出码")
    args = parser.parse_args()
    asyncio.run(run_probe(args.width, args.height, args.dpr, args.tag, args.assert_zero_overflow))


if __name__ == "__main__":
    main()
