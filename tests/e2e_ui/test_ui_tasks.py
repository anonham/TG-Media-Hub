"""E2E-UI-02 / 03 / 06 / 07 发布观测族与 E2E-UI-08 / 09 任务日志终端/错误面.

语义真源: docs/testing/E2E_GUIDE.md §4
- UI-02: 任务中心双量纲进度 (条目+字节单调性)
- UI-03: 速度/ETA/通道矩阵/DC 徽标 + 并发调节与 REST 复核
- UI-06: current_item 滚动与多任务抽屉状态隔离 (切页后抽屉确定性关闭)
- UI-07: 抽屉三 Tab 懒加载与保活
- UI-08: 日志终端关键词过滤/级别过滤与导出防重入
- UI-09: 创建表单错误面契约与表单保持
"""

from __future__ import annotations

import asyncio
import json
import re
import shutil
import time
import urllib.error
import urllib.request
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest
from e2e_live.harness.cleanup import published_message_ids
from playwright.async_api import Page, expect

from .conftest import _reader_creds, _userbot_session_candidates
from .helpers import (
    BACKEND_BASE_URL,
    MOBILE_DOM_PROBE_JS,
    ROUTES,
    api_delete,
    api_get,
    api_post,
    close_drawer,
    drawer_title,
    ensure_running_task,
    goto,
    seed_publish_task,
)

# 跨用例共享的发布观测状态
_live_state: dict[str, Any] = {
    "taskA": "",
    "taskB": "",
    "mark": None,
    "target_chat": "",
    "registry": [],
    "owned_ids": set(),
}


async def _open_running_task(page: Page, task_id: str) -> None:
    """打开指定任务的详情抽屉并校验任务 ID 存在于抽屉标题."""
    await close_drawer(page)
    search = page.get_by_placeholder("搜索 ID / 频道 / 路径...")
    if await search.count() == 1 and await search.input_value():
        await search.fill("")
        await search.press("Enter")

    row = page.locator("table tbody tr").filter(has_text=task_id)
    await expect(row).to_have_count(1)
    await row.get_by_role("button", name="详情").click()
    # 任务 ID 徽章在 n-drawer header 中 (不在 .drawer-body testid 元素内) — 断言打外壳
    drawer_shell = page.locator(".task-detail-drawer")
    await expect(drawer_shell).to_be_visible()
    # 抽屉内容换装存在窗口期: 重试等待目标任务 ID 真正渲染, 防止读到残留旧任务标题
    await expect(drawer_shell).to_contain_text(task_id, timeout=10_000)
    title = await drawer_title(page)
    assert task_id in title


async def _init_live_tasks(
    page: Page,
    tg_reader: Any,
    ui_manifest: dict[str, Any],
    ui_target_chat: str,
) -> None:
    chat_entity = await tg_reader.resolve_chat(ui_target_chat)
    _live_state["target_chat"] = ui_target_chat
    if _live_state["mark"] is None:
        _live_state["mark"] = await tg_reader.max_message_id(chat_entity)

    if not _live_state["taskA"]:
        task_a = await seed_publish_task(page.request, "ui_task_a", ui_manifest, ui_target_chat)
        _live_state["taskA"] = task_a
        _live_state["registry"].append(task_a)

    if not _live_state["taskB"]:
        task_b = await seed_publish_task(page.request, "ui_task_b", ui_manifest, ui_target_chat)
        _live_state["taskB"] = task_b
        _live_state["registry"].append(task_b)

    # 调度弹性: 等待至少一个任务进入 running
    for _ in range(60):
        a_status = ""
        b_status = ""
        try:
            a_data = await api_get(page.request, f"/api/tasks/{_live_state['taskA']}")
            a_status = str(a_data.get("status"))
        except Exception:
            a_status = ""
        try:
            b_data = await api_get(page.request, f"/api/tasks/{_live_state['taskB']}")
            b_status = str(b_data.get("status"))
        except Exception:
            b_status = ""
        if a_status == "running" or b_status == "running":
            break
        # 轮询等待后台任务进入 running 状态
        await asyncio.sleep(2.0)


async def _cleanup_live_tasks(
    tg_reader: Any,
    ui_target_chat: str,
    db_path: Path | None,
) -> None:
    """清理闭环: 任务取消先行 → 轮询终态 → 删任务行确认执行器退出 → 删除有任务回执的频道消息 (失败显式报错并保留未清理状态).

    HTTP 走 _http_request_sync (传输无关): 最后一个用例结束后 playwright RPC 通道可能已失效,
    同步 HTTP 不依赖浏览器传输, 保证清理必然可达后端.
    先删任务行作为执行器退出同步点 (delete_task 守卫要求 not is_task_running), 再删除已登记的回执消息,
    避免 cancel 后在途晚到消息漏删.
    """
    task_ids = list(dict.fromkeys(_live_state["registry"]))
    mark = _live_state.get("mark")
    if not task_ids and mark is None:
        return

    cleanup_errors: list[str] = []
    terminal_states = {"cancelled", "completed", "failed"}

    # 1. 任务取消先行 (Cancel-First Protocol)
    for tid in task_ids:
        try:
            code, _ = await asyncio.to_thread(_http_request_sync, "POST", f"/api/tasks/{tid}/cancel")
            # 404 = 任务行已不存在; 409 = 任务已终态 (cancel 幂等无害), 均不计入清理失败
            if code >= 400 and code not in (404, 409):
                cleanup_errors.append(f"取消任务 {tid} 失败 (HTTP {code})")
        except Exception as exc:
            cleanup_errors.append(f"取消任务 {tid} 异常: {exc}")

    # 2. 轮询等待任务全部进入终态 (确保后台线程不再向频道发新消息; 超时或查询失败显式记录)
    pending_ids: set[str] = set(task_ids)
    last_query_errors: dict[str, str] = {}
    for _ in range(30):
        if not pending_ids:
            break
        still_pending: set[str] = set()
        for tid in pending_ids:
            try:
                code, d = await asyncio.to_thread(_http_request_sync, "GET", f"/api/tasks/{tid}")
                if code == 404:
                    last_query_errors.pop(tid, None)
                    continue
                if code >= 400:
                    last_query_errors[tid] = f"HTTP {code}"
                    still_pending.add(tid)
                    continue
                status = str(d.get("status"))
                if status not in terminal_states:
                    last_query_errors[tid] = f"status={status}"
                    still_pending.add(tid)
                else:
                    last_query_errors.pop(tid, None)
            except Exception as exc:
                last_query_errors[tid] = str(exc)
                still_pending.add(tid)
        pending_ids = still_pending
        if not pending_ids:
            break
        await asyncio.sleep(1.0)

    if pending_ids:
        detail = ", ".join(f"{tid} ({last_query_errors.get(tid, 'unknown')})" for tid in sorted(pending_ids))
        cleanup_errors.append(f"等待任务进入终态超时 (30s): {detail}")

    # 3. 任务行物理删除 (同步点: delete_task 守卫确保执行器已退出调度器运行集合;
    #    仅移除已成功删除的任务 ID，失败项保留在 registry)
    remaining_ids: list[str] = []
    for tid in task_ids:
        try:
            if db_path is not None and mark is not None:
                ids = published_message_ids(db_path, tid, ui_target_chat, mark)
                _live_state["owned_ids"].update(ids)
            ok, del_err = await _delete_task_row_async(tid)
            if ok:
                if _live_state.get("taskA") == tid:
                    _live_state["taskA"] = ""
                if _live_state.get("taskB") == tid:
                    _live_state["taskB"] = ""
            else:
                remaining_ids.append(tid)
                cleanup_errors.append(f"删除任务行 {tid} 失败 ({del_err})")
        except Exception as exc:
            remaining_ids.append(tid)
            cleanup_errors.append(f"删除任务行 {tid} 异常: {exc}")

    _live_state["registry"][:] = remaining_ids

    # 保留失败的 ID，供会话级清理重试。外部服务没有对应的 scratch 回执。
    owned = _live_state["owned_ids"]
    if owned and tg_reader is not None:
        chat_entity = await tg_reader.resolve_chat(ui_target_chat)
        await tg_reader.delete_messages(chat_entity, sorted(owned))
        owned.clear()
    if owned:
        cleanup_errors.append(f"缺少 reader，待清理的本轮消息: {sorted(owned)}")
    if not remaining_ids and not owned:
        _live_state["target_chat"] = ""
        _live_state["mark"] = None

    if cleanup_errors:
        raise RuntimeError("实网任务/频道消息清理未完全成功: " + "; ".join(cleanup_errors))


def _http_request_sync(method: str, path: str, timeout: float = 10.0) -> tuple[int, dict[str, Any]]:
    """不依赖 Playwright Page 的同步 HTTP 辅助 (供会话级兜底清理使用)."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    req = urllib.request.Request(f"{BACKEND_BASE_URL}{path}", method=method)
    try:
        with opener.open(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
            data = json.loads(raw) if raw.strip().startswith("{") else {}
            return int(resp.status), data
    except urllib.error.HTTPError as err:
        return int(err.code), {}


async def _delete_task_row_async(tid: str) -> tuple[bool, str]:
    """删除单个任务行 (含 400 短重试, 不阻塞事件循环): 任务终态后调度器运行集合可能仍短暂持有
    executor 引用 (收尾未 pop), 删除守卫此时返回 400 —— 秒级窗口, 短退避重试即可。"""
    code = 0
    for attempt in range(4):
        code, _ = await asyncio.to_thread(_http_request_sync, "DELETE", f"/api/tasks/{tid}")
        if code in (200, 204, 404):
            return True, ""
        if attempt < 3:
            await asyncio.sleep(5.0)
    return False, f"HTTP {code}"


async def _fallback_cleanup_async(ui_scratch_dir: Path, db_path: Path | None) -> None:
    from e2e_live.harness import tg_reader as tg_reader_mod

    chat_ref = _live_state["target_chat"]
    reader = None
    session_copy = ui_scratch_dir / "ui_fallback_cleanup.session"
    try:
        if db_path is not None:
            candidates = _userbot_session_candidates(ui_scratch_dir)
            creds = _reader_creds()
            if candidates and creds is not None:
                shutil.copy2(candidates[0], session_copy)
                reader = tg_reader_mod.TgReader(session_copy, *creds)
    finally:
        try:
            await _cleanup_live_tasks(reader, chat_ref, db_path)
        finally:
            if reader is not None:
                await reader.aclose()
                session_copy.unlink(missing_ok=True)


def _is_last_live_test(request: pytest.FixtureRequest) -> bool:
    """判断当前测试是否为本次 pytest 执行流中最后一个运行的 live 观测用例."""
    live_test_names = {
        "test_ui_02_task_center_dual_metric_progress",
        "test_ui_03_speed_eta_channel_rack_and_concurrency",
        "test_ui_06_current_item_scroll_and_multitask_isolation",
        "test_ui_07_drawer_tabs_lazy_load_and_keepalive",
        "test_ui_19_mobile_real_backend_zero_overflow",
    }
    collected_live = [item.name for item in request.session.items if item.name in live_test_names]
    if not collected_live:
        return False
    return bool(request.node.name == collected_live[-1] or getattr(request.session, "shouldstop", False))


@pytest.fixture(scope="session")
def live_session_cleanup(
    ui_backend_server: Any,
    ui_scratch_dir: Path,
) -> Iterator[Path | None]:
    """会话级兜底清理夹具: 即使最后一个 live 用例在 page/tg_reader setup 阶段失败或跳过，也保证已投任务被回收."""
    data_dir = ui_backend_server.data_dir
    db_path = data_dir / "tg_service.db" if data_dir is not None else None
    try:
        yield db_path
    finally:
        if _live_state["registry"] or _live_state["mark"] is not None:
            asyncio.run(_fallback_cleanup_async(ui_scratch_dir, db_path))


@pytest.fixture
async def live_lifecycle(
    live_session_cleanup: Path | None,
    request: pytest.FixtureRequest,
    page: Page,
    tg_reader: Any,
    ui_target_chat: str,
) -> AsyncIterator[None]:
    """为单个或组合运行的 live 测试提供兜底清理，只清理有本轮归属回执的频道消息."""
    try:
        yield
    finally:
        if _is_last_live_test(request):
            # 收尾硬化: page.request RPC 在传输异常时可能永久挂起且取消不可达 —
            # asyncio.wait 限时, 超时放弃等待并响亮失败
            task = asyncio.create_task(_cleanup_live_tasks(tg_reader, ui_target_chat, live_session_cleanup))
            done, pending = await asyncio.wait({task}, timeout=180.0)
            if pending:
                pending.pop().cancel()
                raise RuntimeError("实网清理超时 (180s): 传输或后端无响应 (已放弃等待)")
            finished = next(iter(done))
            exc = None if finished.cancelled() else finished.exception()
            if exc is not None:
                raise exc


# ── 本地-dummy 用例 ─────────────────────────────────────────────────────────────


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_ui_09_error_surface_contract_and_form_retained(page: Page) -> None:
    """E2E-UI-09 错误面契约与表单保持."""
    await goto(page, ROUTES.create)

    # 切换至云端极速转发 (模式切换按钮为语义化 role="tab", 现代 Web P1 ARIA tabs)
    await page.get_by_role("tab", name=re.compile(r"云端极速转发")).click()

    src_input = page.get_by_placeholder("例如: https://t.me/c/[频道ID]/[消息ID]", exact=False)
    await expect(src_input).to_be_visible()
    await src_input.fill("definitely_invalid_channel_xyz_99")

    estimate = page.get_by_role("button", name=re.compile(r"测算消息范围"))
    await expect(estimate).to_be_enabled()
    await estimate.click()

    toast_msg = page.locator(".n-message").filter(has_text="测算失败").first
    await expect(toast_msg).to_be_visible(timeout=15_000)

    val = await src_input.evaluate("el => el.value")
    assert "definitely_invalid_channel_xyz_99" in val


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_ui_08_log_terminal_filter_and_export_contract(page: Page) -> None:
    """E2E-UI-08 日志终端关键词/级别过滤与导出防重入."""
    created = await api_post(
        page.request,
        "/api/download/link",
        {
            "links": "https://t.me/definitely_invalid_channel_xyz/999999",
            "threads": 4,
        },
    )
    task_id = str(created["task_id"])

    # 轮询终态 (≤60s)
    status = ""
    for _ in range(30):
        d = await api_get(page.request, f"/api/tasks/{task_id}")
        status = str(d.get("status"))
        if status in ("completed", "failed", "cancelled"):
            break
        await asyncio.sleep(2.0)
    assert status in ("completed", "failed")

    try:
        await goto(page, ROUTES.tasks)
        search = page.get_by_placeholder("搜索 ID / 频道 / 路径...")
        await search.fill(task_id)
        await search.press("Enter")
        row = page.locator("table tbody tr").filter(has_text=task_id)
        await expect(row).to_have_count(1)
        await row.get_by_role("button", name="详情").click()
        await expect(page.locator('[data-testid="task-detail-drawer"]')).to_be_visible()
        await page.get_by_text("📜 流式日志终端").click()
        await expect(
            page.locator('[data-testid="task-detail-drawer"] code, .n-drawer code, [class*="drawer"] code').first
        ).to_be_visible()

        kw = page.get_by_placeholder("搜索日志...")

        async def get_log_lines() -> int:
            return int(
                await page.evaluate(
                    """() => {
                        const code = document.querySelector('[data-testid="task-detail-drawer"], .n-drawer, [class*="drawer"]')?.querySelector("code");
                        return code ? code.innerText.split("\\n").filter((l) => l.trim()).length : -1;
                    }"""
                )
            )

        before = await get_log_lines()
        assert before >= 2

        async def _wait_log_lines(predicate, fail_msg: str) -> int:
            # playwright Python 无 expect.poll (JS API 语义) — 手动轮询等价实现
            count = await get_log_lines()
            deadline = time.monotonic() + 5.0
            while not predicate(count) and time.monotonic() < deadline:
                await asyncio.sleep(0.25)
                count = await get_log_lines()
            assert predicate(count), fail_msg.format(count=count)
            return count

        await kw.fill("下载")
        await _wait_log_lines(
            lambda n: n < before,
            f"关键词过滤未生效 (before={before}, narrowed={{count}})",
        )

        await kw.fill("")
        await kw.press("Backspace")
        await _wait_log_lines(
            lambda n: n == before,
            f"清除过滤未还原 (before={before}, restored={{count}})",
        )

        # 级别过滤档 (选择 WARN)
        await page.get_by_text("全部级别").click()
        dropdown = page.locator(".n-select-dropdown, [class*='select-menu']")
        await expect(dropdown).to_be_visible()
        await dropdown.get_by_text("WARN", exact=True).click()
        await _wait_log_lines(
            lambda n: n < before,
            f"WARN 级别过滤未生效 (before={before}, warn_lines={{count}})",
        )

        # 导出: 下载事件触发与文件名契约 task_log_<id>.txt
        export_btn = page.get_by_role("button", name="📥 导出 .txt")
        async with page.expect_download(timeout=10_000) as download_info:
            await export_btn.click()
            await export_btn.click()
        download = await download_info.value
        assert download.suggested_filename == f"task_log_{task_id}.txt"

        await close_drawer(page)
        await search.fill("")
        await search.press("Enter")

    finally:
        await page.request.post(f"{BACKEND_BASE_URL}/api/tasks/{task_id}/cancel")
        await api_delete(page.request, f"/api/tasks/{task_id}")


# ── 本地-live 发布观测族 (自投放任务 + 水位清理闭环) ─────────────────────────────────


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_ui_02_task_center_dual_metric_progress(
    page: Page,
    tg_reader: Any,
    ui_manifest: dict[str, Any],
    ui_target_chat: str,
    live_lifecycle: None,
) -> None:
    """E2E-UI-02 任务中心双量纲进度与看板渲染 (条目+字节单调性)."""
    await _init_live_tasks(page, tg_reader, ui_manifest, ui_target_chat)
    task_a = await ensure_running_task(
        page.request,
        _live_state["taskA"],
        ui_manifest,
        ui_target_chat,
        _live_state["registry"],
    )
    _live_state["taskA"] = task_a

    await goto(page, ROUTES.tasks)
    await expect(
        page.locator("table tbody tr").filter(has_text=re.compile(r"ui_task_a|ui_task_b")).first
    ).to_be_visible(timeout=15_000)
    samples: list[list[str]] = []
    for i in range(6):
        rows = await page.evaluate(
            r"""() => {
                return Array.from(document.querySelectorAll("table tbody tr"))
                    .map((r) => r.innerText.replace(/\n+/g, " "))
                    .filter((t) => /ui_task_a|ui_task_b/.test(t));
            }"""
        )
        samples.append(rows)
        if i < 5:
            await asyncio.sleep(5.0)

    # 双量纲 (字节 a/b 与条目 [x/y 项]) 同时展示且字节单调不减
    for sample in samples:
        assert len(sample) >= 1
        for row in sample:
            assert re.search(r"\d+(\.\d+)? (B|KB|MB|GB) \/ \d+(\.\d+)? (B|KB|MB|GB)", row)
            assert re.search(r"\[\d+\/\d+ 项\]", row)

    def bytes_of(row_text: str) -> float:
        m = re.search(r"([\d.]+) MB \/ ", row_text)
        return float(m.group(1)) if m else 0.0

    for which in (0, 1):
        series = [bytes_of(s[which]) for s in samples if which < len(s) and bytes_of(s[which]) > 0]
        for i in range(1, len(series)):
            assert series[i] >= series[i - 1] - 0.01


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_ui_03_speed_eta_channel_rack_and_concurrency(
    page: Page,
    tg_reader: Any,
    ui_manifest: dict[str, Any],
    ui_target_chat: str,
    live_lifecycle: None,
) -> None:
    """E2E-UI-03 速度/ETA/通道矩阵与 DC 徽标 + 并发调节."""
    await _init_live_tasks(page, tg_reader, ui_manifest, ui_target_chat)
    await goto(page, ROUTES.tasks)
    task_a = await ensure_running_task(
        page.request,
        _live_state["taskA"],
        ui_manifest,
        ui_target_chat,
        _live_state["registry"],
    )
    _live_state["taskA"] = task_a
    await _open_running_task(page, task_a)

    # DC 徽标随首个 progress 上报出现 (dc_id 为 0 时徽标不渲染): 轮询等待后再做元素断言
    drawer_shell = page.locator('[data-testid="task-detail-drawer"]')
    await expect(drawer_shell).to_contain_text(re.compile(r"🌐 DC \d+"), timeout=20_000)

    # 实时指标元素断言: 速率曲线 / ETA / Worker 矩阵 / DC 徽标 / 并发输入
    board = await page.evaluate(
        r"""() => {
            const d = document.querySelector('[data-testid="task-detail-drawer"], .n-drawer');
            const t = d ? d.innerText : "";
            return {
                speedCurve: /动态传输速率曲线/.test(t),
                eta: /预计剩余时间 \(ETA\)/.test(t),
                worker: Boolean(d?.querySelector(".channels-rack") || d?.querySelector("[title*='Worker']")),
                dc: /🌐 DC \d+/.test(t),
                concurrencyInput: /并发通道:/.test(t),
            };
        }"""
    )
    assert board["speedCurve"], "动态传输速率曲线未找到"
    assert board["eta"], "预计剩余时间 (ETA) 未找到"
    assert board["worker"], "Worker 矩阵未找到"
    assert board["dc"], "DC 徽标未找到"
    assert board["concurrencyInput"], "并发通道输入区未找到"

    # 并发调节 (点 + 按钮递增一档) → 响应拦截 → REST 复核 → 恢复原档
    drawer = page.locator('[data-testid="task-detail-drawer"]')
    concurrency_group = drawer.locator(".adjust-group").filter(has_text="并发通道:")
    await expect(concurrency_group).to_be_visible()
    input_el = concurrency_group.locator("input")
    initial_val = int((await input_el.input_value()) or "1")
    next_val = initial_val + 1

    async with page.expect_response(
        lambda r: "/concurrency" in r.url and r.status == 200,
        timeout=10_000,
    ):
        btns = concurrency_group.locator("button")
        if await btns.count() >= 2:
            await btns.last.click()
        else:
            await input_el.click()
            await page.keyboard.press("ArrowUp")

    # REST 复核: 并发值真实落库 (playwright Python 无 expect.poll — 手动轮询等价实现)
    async def get_rest_concurrency() -> int:
        detail = await api_get(page.request, f"/api/tasks/{task_a}")
        params = detail.get("params") or {}
        progress = detail.get("progress") or {}
        return int(params.get("upload_concurrency") or params.get("threads") or progress.get("concurrency_total") or 0)

    rest_val = await get_rest_concurrency()
    deadline = time.monotonic() + 5.0
    while rest_val != next_val and time.monotonic() < deadline:
        await asyncio.sleep(0.25)
        rest_val = await get_rest_concurrency()
    assert rest_val == next_val, f"并发值未落库 (expected={next_val}, got={rest_val})"

    # 恢复原档 (清理)
    await api_post(
        page.request,
        f"/api/tasks/{task_a}/concurrency",
        {
            "concurrency": initial_val,
            "upload_in_flight_per_connection": 2,
        },
    )
    await close_drawer(page)


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_ui_06_current_item_scroll_and_multitask_isolation(
    page: Page,
    tg_reader: Any,
    ui_manifest: dict[str, Any],
    ui_target_chat: str,
    live_lifecycle: None,
) -> None:
    """E2E-UI-06 current_item 滚动与多任务状态隔离."""
    await _init_live_tasks(page, tg_reader, ui_manifest, ui_target_chat)
    await goto(page, ROUTES.tasks)
    task_b = await ensure_running_task(
        page.request,
        _live_state["taskB"],
        ui_manifest,
        ui_target_chat,
        _live_state["registry"],
    )
    _live_state["taskB"] = task_b
    await _open_running_task(page, task_b)
    t1 = await drawer_title(page)
    assert task_b in t1

    # 切创建页再切回 → 抽屉确定性关闭 (无串台)
    # 同文档 hash 导航瞬时返回, 旧抽屉的离场动画/卸载存在窗口期 — 轮询等待稳定再断言
    await goto(page, ROUTES.create)
    await goto(page, ROUTES.tasks)
    title_after_nav = await drawer_title(page)
    deadline = time.monotonic() + 10.0
    while title_after_nav != "(no drawer)" and time.monotonic() < deadline:
        await asyncio.sleep(0.3)
        title_after_nav = await drawer_title(page)
    assert title_after_nav == "(no drawer)", f"切页后抽屉未确定性关闭: {title_after_nav}"

    task_a = await ensure_running_task(
        page.request,
        _live_state["taskA"],
        ui_manifest,
        ui_target_chat,
        _live_state["registry"],
    )
    _live_state["taskA"] = task_a
    await _open_running_task(page, task_a)
    t2 = await drawer_title(page)
    assert task_a in t2

    await _open_running_task(page, task_b)
    t3 = await drawer_title(page)
    assert task_b in t3


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_ui_07_drawer_tabs_lazy_load_and_keepalive(
    page: Page,
    tg_reader: Any,
    ui_manifest: dict[str, Any],
    ui_target_chat: str,
    live_lifecycle: None,
) -> None:
    """E2E-UI-07 抽屉三 Tab 懒加载保活并在用例结束执行全量清理闭环."""
    await _init_live_tasks(page, tg_reader, ui_manifest, ui_target_chat)
    await goto(page, ROUTES.tasks)
    task_b = await ensure_running_task(
        page.request,
        _live_state["taskB"],
        ui_manifest,
        ui_target_chat,
        _live_state["registry"],
    )
    _live_state["taskB"] = task_b
    await _open_running_task(page, task_b)

    async def get_drawer_text() -> str:
        return await page.evaluate(
            r"""() => {
                const d = document.querySelector('[data-testid="task-detail-drawer"], .n-drawer');
                return d ? d.innerText.replace(/\n+/g, " | ") : "";
            }"""
        )

    await page.get_by_text("📋 任务参数与元数据").click()
    await expect(
        page.locator('[data-testid="task-detail-drawer"]').filter(
            has_text=re.compile(r"任务类型 \| publish_folder|publish_folder")
        )
    ).to_be_visible()

    await page.get_by_text("🔍 专属诊断视图").click()
    await expect(
        page.locator('[data-testid="task-detail-drawer"]').filter(
            has_text=re.compile(r"超限跳过文件|失败文件|防重命中")
        )
    ).to_be_visible()

    await page.get_by_text("⚡ 实时监控看板").click()
    await expect(
        page.locator('[data-testid="task-detail-drawer"]').filter(has_text=re.compile(r"动态传输速率曲线"))
    ).to_be_visible()


# ── 真实后端移动端零溢出 (E2E-UI-19; ───────────────────────────


@pytest.mark.e2e_ui
@pytest.mark.asyncio
async def test_ui_19_mobile_real_backend_zero_overflow(
    page: Page, tg_reader: Any, ui_manifest: dict[str, Any], ui_target_chat: str, live_lifecycle: None
) -> None:
    """E2E-UI-19: 移动端视口 (≤768px) 连真实后端与真实观测任务的零溢出回归。

    mocked 套件已覆盖全路由探针 (mock 数据形态); 本用例补真实后端数据形态: 任务中心
    移动卡片流渲染 + 详情抽屉视口受控 (~88dvh), 探针复用 MOBILE_DOM_PROBE_JS。
    须排在其他 live 观测用例之后 (cleanup 经 live_lifecycle 在最后一个 live 用例收口)。
    """
    await _init_live_tasks(page, tg_reader, ui_manifest, ui_target_chat)
    await page.set_viewport_size({"width": 412, "height": 915})
    await goto(page, ROUTES.tasks)

    cards = page.locator('[data-testid="task-mobile-card"]')
    await expect(cards.first).to_be_visible(timeout=15_000)
    await page.wait_for_timeout(400)
    probe = await page.evaluate(MOBILE_DOM_PROBE_JS)
    assert not probe["hasDocOverflow"], f"[tasks_cards] 文档横向溢出: {probe['docScrollWidth']} > {probe['vw']}"
    assert probe["viewportOverflowCount"] == 0, f"[tasks_cards] 视口溢出元素: {probe['viewportOverflows']}"
    assert probe["clippedOverflowCount"] == 0, f"[tasks_cards] 容器撑爆元素: {probe['clippedOverflows']}"

    detail = cards.first.locator('[data-testid="task-detail-btn"] button')
    await detail.click()
    drawer = page.locator(".task-detail-drawer")
    await expect(drawer).to_be_visible()
    await expect(page.locator('[data-testid="task-detail-drawer"]')).to_be_visible(timeout=15_000)
    await page.wait_for_timeout(400)
    # 抽屉外壳在移动端为全屏容器 (88dvh 约束在内层内容), 断言走零溢出探针而非外框高度
    probe_drawer = await page.evaluate(MOBILE_DOM_PROBE_JS)
    assert probe_drawer["viewportOverflowCount"] == 0, (
        f"[tasks_drawer] 视口溢出元素: {probe_drawer['viewportOverflows']}"
    )
    assert probe_drawer["clippedOverflowCount"] == 0, f"[tasks_drawer] 容器撑爆元素: {probe_drawer['clippedOverflows']}"
    await close_drawer(page)

    await goto(page, ROUTES.dashboard)
    await expect(page.locator(".cockpit-card").first).to_be_visible(timeout=15_000)
    await page.wait_for_timeout(400)
    probe_dash = await page.evaluate(MOBILE_DOM_PROBE_JS)
    assert probe_dash["viewportOverflowCount"] == 0, f"[dashboard] 视口溢出元素: {probe_dash['viewportOverflows']}"
    assert probe_dash["clippedOverflowCount"] == 0, f"[dashboard] 容器撑爆元素: {probe_dash['clippedOverflows']}"
