"""韧性与会话治理 (E2E-LIVE-13, 20 + §7 ; 承 E2E-STRAT-06 与
E2E-MISC-01/02; E2E-LIVE-18 按 裁决转实机-人工)。

LIVE-13 断点续跑 realized on 本地发布通道 (计划书原文为 PikPak 任务; 执行期偏差
折算见计划书附录 A 谱系): 孤儿恢复/防重不重发/恢复日志提示三断言与介质无关,
PikPak 专属断点续传 (meta 分块续传) 由 R 层 tests/pipeline/test_pikpak_resume.py
锁定。恢复语义 (scheduler.recover_orphaned_tasks 实证): 重启后未完成任务默认转
paused, `auto_resume_tasks_on_startup=true` (system_settings, 经 PUT /api/settings
写入 scratch 库) 才自动重排 queued —— 用例取后者。

LIVE-20 会话治理: 相对 USERBOT_SESSION_NAME 归一落位 DATA_DIR (scratch 克隆即
实证) + 仓库根零残留 + `DELETE /api/accounts/{id}?delete_session_file=true` 四类
文件 (.session/-journal/-wal/-shm) 全清 —— 全程作用于 scratch 隔离副本, 真实
data/ 会话零触碰。
"""

import time
from pathlib import Path
from typing import Any

import httpx
import pytest

from e2e_live.harness import api_client, db_readonly, env_gate, local_samples, pikpak_probe, server_proc
from e2e_live.harness import tg_reader as tg_reader_mod
from e2e_live.harness.tg_reader import is_video_message

pytestmark = [pytest.mark.e2e_live]

_RESUME_FILES = 6  # 注入窗口保障: ≥1 项已发布时击杀, 剩余必然含孤儿


def _make_run_folder(live_cleanup, name: str) -> Path:
    """任务文件夹 (DATA_DIR 外, 发布扫描守卫红线; 与 test_03 同机制)。"""
    stamp = time.strftime("%Y%m%d_%H%M%S")
    folder = local_samples.samples_root() / "runs" / f"{name}_{stamp}"
    folder.mkdir(parents=True, exist_ok=True)
    live_cleanup.register_dir(folder)
    return folder


def _poll(live_api: api_client.LiveApiClient, task_id: str, predicate, timeout: float = 120.0) -> dict[str, Any]:
    """带 429 退避的任务轮询直至谓词满足 (attach 形态限流不可控)。"""
    deadline = time.monotonic() + timeout
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            resp = live_api.get(f"/api/tasks/{task_id}")
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 429:
                time.sleep(2.0)
                continue
            raise
        last = resp.json()
        if predicate(last):
            return last
        time.sleep(0.5)
    raise AssertionError(f"任务 {task_id} 在 {timeout}s 内未满足轮询条件, 最后状态 {last.get('status')!r}")


@pytest.mark.usefixtures("managed_live_server")
async def test_server_kill_restart_and_resume(
    live_server, live_api, tg_reader, target_chat, live_cleanup, live_db, scratch_data_dir
) -> None:
    """E2E-LIVE-13 (承 E2E-STRAT-06): 进程击杀 → 重启 → 孤儿任务自动恢复 + 防重不重发。

    断言: 任务运行中段整树击杀服务器 → 同 scratch/同端口重启 (锁接管) → auto_resume
    语义将孤儿任务重排 queued 并写入恢复日志 → 任务续跑至 completed → 已发布文件
    防重跳过 (日志) + 频道产物恰好 N 条无重复 + DB 终态 completed。
    """
    if shutil_which_ffmpeg() is None:
        pytest.skip("ffmpeg 不在 PATH, 击杀续跑样本无法制备")
    try:
        base = local_samples.ensure_v3_base()
    except (RuntimeError, OSError) as exc:
        pytest.skip(f"样本制备失败: {exc}")

    folder = _make_run_folder(live_cleanup, "resume")
    names = [f"res_{i:02d}.mp4" for i in range(1, _RESUME_FILES + 1)]
    for name in names:
        # v3_s (~4MB): 低速网络下击杀窗口内首个文件也能完成落库 (19.7MB 档在
        # 0.2 MB/s 下需 ~100s/文件, 120s 击杀窗口内可能 0 发布)
        local_samples.copy_into(folder, base["s"], name)

    # auto_resume 持久化进 scratch system_settings (重启后恢复语义取自动重排分支)
    settings_resp = live_api.put("/api/settings", json={"settings": {"auto_resume_tasks_on_startup": True}})
    assert settings_resp.status_code == 200, settings_resp.text

    try:
        chat = await tg_reader.resolve_chat(target_chat)
    except tg_reader_mod.ChatUnresolvableError as exc:
        pytest.skip(f"无 userbot 会话可解析测试频道 {target_chat}: {exc}")
    mark = await tg_reader.max_message_id(chat)
    live_cleanup.register_watermark(target_chat, mark)

    resp = live_api.post(
        "/api/publish/folder",
        json={
            "folder_path": str(folder),
            "target_channel": target_chat,
            "album_mode": False,
            "delay_seconds": 0.5,
            "send_placeholder_card": False,
            "oversize_policy": "skip_and_report",
            "upload_concurrency": 8,
            "upload_in_flight_per_connection": 2,
        },
    )
    assert resp.status_code == 200, resp.text
    task_id = resp.json()["task_id"]
    live_cleanup.register_task(task_id)

    # 运行中段击杀: 以 publish_history 落库计数为"已发布"权威信号 —— 进度
    # processed_items 在首文件上传中即报 1 (文件级计数语义, 包 4 实证), 不可作依据
    deadline = time.monotonic() + 300
    published = 0
    while time.monotonic() < deadline:
        row = db_readonly.fetch_one(
            live_db,
            "SELECT COUNT(*) AS n FROM publish_history WHERE file_path LIKE ?",
            (f"%{folder.name.lower()}%",),
        )
        published = int(row["n"]) if row else 0
        if published >= 1:
            break
        time.sleep(0.5)
    assert published >= 1, "击杀前应至少 1 个文件已发布入库 (publish_history)"
    in_flight = live_api.get(f"/api/tasks/{task_id}").json()
    assert in_flight.get("status") == "running", f"击杀时任务应处于 running: {in_flight.get('status')}"
    live_server.stop()  # 整树击杀 (trampoline 语义, 包 2 实证)

    # 同 scratch / 同端口重启: 锁接管 (进程死亡释放字节范围锁), live_api 沿用原端口
    restarted = server_proc.LiveServer(scratch_data_dir, live_server.port, live_server.host)
    restarted.start()
    try:
        restarted.wait_ready()
    except Exception:
        restarted.stop()
        raise
    # 进程接管: 会话夹具 teardown 停新进程 (原 proc 已死, stop() 幂等)
    live_server.proc = restarted.proc
    live_server.log_path = restarted.log_path

    # 重启后连接池里的死连接不可复用: 新建客户端轮询
    post = api_client.LiveApiClient(live_server.base_url)
    try:
        final = post.wait_terminal(task_id, timeout=1800)
        assert final["status"] == "completed", (
            f"孤儿任务应自动恢复并续跑至 completed: {final.get('status')} / {final.get('error_detail')}"
        )
        assert final["progress"]["processed_items"] == _RESUME_FILES, final["progress"]

        logs = pikpak_probe.logs_text(post, task_id)
        assert "服务启动自动恢复" in logs, "恢复日志缺失 (recover_orphaned_tasks auto_resume 分支)"
        # 断点续跑日志措辞: "⏩ [断点续传跳过] <file> (当前任务已发布)"
        assert "断点续传跳过" in logs, "已发布文件应被防重跳过 (断点续跑语义)"

        msgs = await tg_reader.collect_messages_after(chat, mark)
        videos = [m for m in msgs if is_video_message(m)]
        got_names = {(m.text or "").strip() for m in videos}
        assert len(videos) == _RESUME_FILES and got_names == set(names), (
            f"频道应恰 {_RESUME_FILES} 条视频且无重复发送: 实得 {len(videos)}, captions={sorted(got_names)}"
        )
        row = db_readonly.fetch_one(live_db, "SELECT status FROM task_queue WHERE id = ?", (task_id,))
        assert row is not None and row["status"] == "completed", dict(row)
    finally:
        post.close()


def shutil_which_ffmpeg() -> str | None:
    import shutil

    return shutil.which("ffmpeg")


def test_network_outage_resilience_manual_placeholder() -> None:
    """E2E-LIVE-18 (承 E2E-FLOW-03) — 用户裁决: 断网韧性转实机-人工。

    harness 不实现网络注入层; 断言口径不变 (REST + Telethon 只读 + 日志重连记录),
    执行形态转人工 —— 正文落 docs/testing/E2E_GUIDE.md, 本占位仅保留编号与指引。
    """
    pytest.skip(
        "E2E-LIVE-18 已裁决转实机-人工: 任务运行中切断出网 30s ×2 轮 (切片阶段/发送阶段), "
        "断言自动续传/无重复分卷/终态成功 —— 口径见 E2E_GUIDE"
    )


@pytest.mark.usefixtures("managed_live_server")
def test_session_path_normalization_and_account_cleanup(live_api, live_db, scratch_data_dir) -> None:
    """E2E-LIVE-20 (承 E2E-MISC-01/02): 会话路径归一 + 账号删除四类文件全清。

    断言: 相对 USERBOT_SESSION_NAME 的账号会话落位 DATA_DIR (scratch 克隆即实证) 且
    仓库根零残留 → DELETE /api/accounts/{id}?delete_session_file=true 删除非默认备用
    账号 → scratch 内该账号 .session/-journal/-wal/-shm 四类全清 + tg_accounts 行消亡。
    """
    # ---- 归一: 仓库根零残留 (harness 工作副本全在 scratch) ----
    root_sessions = [p.name for p in env_gate.REPO_ROOT.glob("*.session")]
    assert not root_sessions, f"仓库根不应有 session 残留 (会话应归一至 DATA_DIR): {root_sessions}"

    account_sessions = [p for p in scratch_data_dir.glob("*.session") if not p.name.startswith(("live_", "_live"))]
    assert account_sessions, "账号会话应归一落位 DATA_DIR (scratch 克隆)"

    # ---- 备用账号删除 (非默认; scratch 隔离副本, 真实 data/ 零触碰) ----
    listing = live_api.get("/api/accounts")
    assert listing.status_code == 200, listing.text
    accounts = listing.json()
    assert accounts, "scratch 库应含克隆账号"
    spare = next((a for a in accounts if not a.get("is_default")), None)
    if spare is None:
        pytest.skip("仅存在默认账号, 无备用账号可删 (E2E-LIVE-20 需 ≥2 账号)")
    session_name = Path(str(spare.get("session_file") or "")).name
    if session_name and not session_name.endswith(".session"):
        session_name = f"{session_name}.session"

    removed = live_api.delete(f"/api/accounts/{spare['id']}", params={"delete_session_file": True})
    assert removed.status_code == 200, removed.text

    # ---- 文件系统: 四类文件全清 (DATA_DIR 内) ----
    if session_name:
        for suffix in ("", "-journal", "-wal", "-shm"):
            leftover = scratch_data_dir / f"{session_name}{suffix}"
            assert not leftover.exists(), f"删除账号后应四类全清: {leftover} 仍存在"

    # ---- DB: 账号行消亡 ----
    row = db_readonly.fetch_one(live_db, "SELECT id FROM tg_accounts WHERE id = ?", (spare["id"],))
    assert row is None, f"tg_accounts 行应已删除: id={spare['id']}"

    # ---- 其余账号不受影响 (服务器仍存活可服务) ----
    remaining = live_api.get("/api/accounts")
    assert remaining.status_code == 200 and len(remaining.json()) == len(accounts) - 1, "其余账号应保持不变"
