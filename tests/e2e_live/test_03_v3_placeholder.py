"""占位卡与原位编辑回填真链路 E2E 测试 (E2E-LIVE-14 ~ 16)。

被测语义:
- 单文件传输失败 (本地通道) → 1:1 ⏳ Photo 失败卡 (海报 1280×720, 文案
  "⏳ 【补发排队】") + `placeholder_slots` backfillable 槽 (state=placeholder_sent,
  严禁写入 publish_history);
- 相册整组失败 → 1 张文本汇总卡 (group_summary 槽, 成员共享 msg_id, 结构上禁回填);
  任务终态 failed 即触发 finalize: group_summary 槽无阈值直接 ❌【相册补发终止】,
  backfillable 槽 fail_count ≥ 1+max_retries 才 ❌;
- 超限卡 (skip_and_report) = "⚠️ 【缺集说明】" 纯文本卡, 无槽;
- 失败卡 封顶: 每任务 10 张逐条 + 1 张 "⚠️ 【批量异常提示】" 溢出汇总卡
  (溢出卡无槽; 溢出计数文案为发送时点快照, 后续溢出仅计数不刷新文案);
- 重试回路: 失败文件重推 → `_try_backfill` CAS 命中 → 两段式 edit 原地回填
  (消息 ID 不变, caption 变更为文件名), 槽 published + publish_history 补记。

失败注入 (仅 harness 侧, 零产品代码改动): 任务 running 后 1.2s 移除磁盘上尚未
发送的文件 (TOCTOU) —— 列表快照已含该文件、发送时读取失败, 与真实"源文件消失"
故障同型; 成功档文件置前且体积足量 (~7.5 MiB), 注入窗口落在其上传中段。
任务文件夹必须在 DATA_DIR 之外 (发布入口扫描守卫拒绝内部运行数据目录, 包 5
实跑实证), 落在样本缓存根下并随会话清理账本回收。
"""

import shutil
import time
from pathlib import Path
from typing import Any

import httpx
import pytest

from e2e_live.harness import api_client, db_readonly, local_samples, pikpak_probe
from e2e_live.harness import tg_reader as tg_reader_mod
from e2e_live.harness.tg_reader import is_document_message, is_photo_message, is_video_message

pytestmark = [pytest.mark.e2e_live]

_INJECT_DELAY_S = 1.2  # running → 注入间隔 (窗口 = 列表快照之后、待删文件发送之前)


def _inject_source_loss(path: Path) -> None:
    """注入"源文件消失"故障 (Windows 兼容双形态): 优先 unlink; 文件正被服务器
    上传句柄占用时报 WinError 32 (包 5 实跑实证) → 降级原地清空 —— 后续分块读
    命中 EOF 短读, 与删除同属源数据丢失故障面 (引擎有 test_local_provider_short_read_error
    先例)。两种形态均不触碰产品代码。"""
    try:
        path.unlink()
    except PermissionError:
        with open(path, "r+b") as fh:
            fh.truncate(0)


@pytest.fixture(scope="module")
def v3_samples(live_gate) -> dict[str, Any]:
    """本地样本制备: ffmpeg 缺失或制备失败即整包 skip。"""
    if shutil.which("ffmpeg") is None:
        pytest.skip("ffmpeg 不在 PATH, 本地 V3 样本无法制备")
    try:
        base = local_samples.ensure_v3_base()
        jpegs = local_samples.ensure_album_jpegs(8)
    except (RuntimeError, OSError) as exc:
        pytest.skip(f"本地样本制备失败: {exc}")
    return {**base, "album": jpegs}


def _wait_running(live_api: api_client.LiveApiClient, task_id: str, timeout: float = 120.0) -> None:
    """等待任务进入 running (注入时序锚点: 列表快照已完成); attach 形态下外部
    服务器限流不可控, 429 按退避容忍。"""
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        try:
            resp = live_api.get(f"/api/tasks/{task_id}")
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 429:
                time.sleep(2.0)
                continue
            raise
        last = str(resp.json().get("status", ""))
        if last == "running":
            return
        if last in api_client.TERMINAL_STATUSES:
            raise AssertionError(f"任务在注入前已终态: {last}")
        time.sleep(0.5)
    raise AssertionError(f"任务 {timeout}s 内未进入 running (最后状态 {last})")


def _submit_folder_publish(live_api: api_client.LiveApiClient, folder: Path, target_chat: str, *, album: bool) -> str:
    resp = live_api.post(
        "/api/publish/folder",
        json={
            "folder_path": str(folder),
            "target_channel": target_chat,
            "album_mode": album,
            "delay_seconds": 0.5,
            "send_placeholder_card": True,
            "oversize_policy": "skip_and_report",
            "upload_concurrency": 8,
            "upload_in_flight_per_connection": 2,
        },
    )
    assert resp.status_code == 200, resp.text
    return str(resp.json()["task_id"])


def _slot_rows(live_db, sql: str, args: tuple) -> list[dict[str, Any]]:
    return [dict(r) for r in db_readonly.fetch_all(live_db, sql, args)]


def _norm_key(path: Path) -> str:
    """publish_history 查询键归一 (与写入侧 app.db.publish_repository.norm_file_path
    同口径: 反斜杠 → 正斜杠 + 全小写)。"""
    from app.db.publish_repository import norm_file_path

    return norm_file_path(str(path))


def _make_run_folder(live_cleanup, name: str) -> Path:
    """任务文件夹: 服务器可读且必须在 DATA_DIR 之外 —— 发布入口扫描守卫拒绝
    内部运行数据目录 ("禁止扫描内部数据库与运行数据目录", 包 5 实跑实证);
    落在样本缓存根下并由清理账本在会话结束回收。"""
    stamp = time.strftime("%Y%m%d_%H%M%S")
    folder = local_samples.samples_root() / "runs" / f"{name}_{stamp}"
    folder.mkdir(parents=True, exist_ok=True)
    live_cleanup.register_dir(folder)
    return folder


async def _resolve_chat_or_skip(tg_reader: tg_reader_mod.TgReader, target_chat: str) -> Any:
    try:
        return await tg_reader.resolve_chat(target_chat)
    except tg_reader_mod.ChatUnresolvableError as exc:
        pytest.skip(f"无 userbot 会话可解析测试频道 {target_chat}: {exc}")


async def test_placeholder_card_lifecycle_and_inplace_backfill(
    v3_samples, live_api, tg_reader, target_chat, live_cleanup, live_db
) -> None:
    """E2E-LIVE-14 (承 E2E-PUB-01/02 + VERIFICATION 场景 1/2): 占位卡全生命周期。

    断言链: 失败 → ⏳ Photo 卡紧随成功件 + backfillable 槽 + publish_history 零占位行
    → 重试 → 原消息 ID 不变原地变正式媒体 + 槽 published + publish_history 补记 +
    频道无新增孤儿媒体消息。
    """
    folder = _make_run_folder(live_cleanup, "lifecycle")
    ok_path = local_samples.copy_into(folder, v3_samples["a"], "v3_a1_ok.mp4")
    fail_path = local_samples.copy_into(folder, v3_samples["m"], "v3_z9_fail.mp4")

    chat = await _resolve_chat_or_skip(tg_reader, target_chat)
    mark1 = await tg_reader.max_message_id(chat)
    live_cleanup.register_watermark(target_chat, mark1)

    task_id = _submit_folder_publish(live_api, folder, target_chat, album=False)
    live_cleanup.register_task(task_id)
    _wait_running(live_api, task_id)
    time.sleep(_INJECT_DELAY_S)
    # 失败注入: 成功档 (52 MiB) 上传中段移除尚未开传的失败档文件 —— 消息顺序与
    # 计划书语义一致 (卡紧随成功件之后); 若该文件已被句柄占用, _inject_source_loss
    # 自动降级原地清空 (短读故障, 失败形态不变)
    _inject_source_loss(fail_path)
    final = live_api.wait_terminal(task_id, timeout=1200)
    assert final["status"] == "failed", (
        f"单文件失败任务终态应为 failed: {final.get('status')} / {final.get('error_detail')}"
    )

    msgs1 = await tg_reader.collect_messages_after(chat, mark1)
    videos = [m for m in msgs1 if is_video_message(m)]
    photos = [m for m in msgs1 if is_photo_message(m)]
    assert len(videos) == 1, f"应恰 1 条成功视频 (v3_a1_ok), 实得 {len(videos)}"
    assert len(photos) == 1, (
        f"应恰 1 张 ⏳ Photo 失败卡, 实得 {len(photos)}; texts={[(m.text or '')[:40] for m in msgs1]}"
    )
    card = photos[0]
    assert int(card.id) > int(videos[0].id), "占位卡应紧随成功件之后"
    assert "⏳ 【补发排队】" in (card.text or "") and "v3_z9_fail" in (card.text or ""), card.text

    logs = pikpak_probe.logs_text(live_api, task_id)
    assert "⏳ 已发送失败占位卡" in logs, logs[-1500:]

    slot = _slot_rows(
        live_db,
        "SELECT * FROM placeholder_slots WHERE task_id = ? AND slot_kind = 'backfillable'",
        (task_id,),
    )
    assert len(slot) == 1, f"应恰 1 个 backfillable 槽, 实得 {len(slot)}"
    slot = slot[0]
    # fail_count 首败为 0 (insert 默认, 仅槽复用时递增) —— J2 终态阈值按复用次数计
    assert slot["state"] == "placeholder_sent", dict(slot)
    assert int(slot["placeholder_msg_id"] or 0) == int(card.id), dict(slot)
    assert str(slot["file_path"]).endswith("v3_z9_fail.mp4"), dict(slot)
    # 红线: 占位信息严禁写入 publish_history (失败文件零入库); 查询键须与写入侧
    # 同口径归一 (norm_file_path: 正斜杠 + 小写)
    failed_hist = db_readonly.fetch_one(
        live_db, "SELECT id FROM publish_history WHERE file_path = ?", (_norm_key(fail_path),)
    )
    assert failed_hist is None, "失败文件不应出现在 publish_history"
    ok_hist = db_readonly.fetch_one(
        live_db,
        "SELECT tg_message_id FROM publish_history WHERE file_path = ? AND file_size = ?",
        (_norm_key(ok_path), ok_path.stat().st_size),
    )
    assert ok_hist is not None and int(ok_hist["tg_message_id"] or 0) > 0, "成功文件应已入 publish_history"

    # ---- 重试回路: 复活失败文件 → 原地回填 ----
    shutil.copy2(v3_samples["m"], fail_path)  # 同字节重放 (槽身份含 size)
    mark2 = await tg_reader.max_message_id(chat)
    retry = live_api.post(f"/api/tasks/{task_id}/retry")
    assert retry.status_code == 200, retry.text
    final2 = live_api.wait_terminal(task_id, timeout=1200)
    assert final2["status"] == "completed", (
        f"重试应全部成功 (ok 件防重跳过 + fail 件回填): {final2.get('status')} / {final2.get('error_detail')}"
    )

    msgs2 = await tg_reader.collect_messages_after(chat, mark2)
    new_media = [m for m in msgs2 if is_video_message(m) or is_photo_message(m) or is_document_message(m)]
    assert not new_media, f"原地回填不应产生新媒体消息 (孤儿检查): {[str(m.text)[:40] for m in new_media]}"
    refetched = await tg_reader.get_messages(chat, [int(card.id)])
    backfilled = refetched[0] if isinstance(refetched, list) else refetched
    assert backfilled is not None and is_video_message(backfilled), (
        f"原消息 {card.id} 应已被原地编辑为正式媒体, 实得形态: {type(backfilled)}"
    )
    assert (backfilled.text or "").strip() == "v3_z9_fail.mp4", (
        f"回填后 caption 基线应为文件名, 实得 {(backfilled.text or '')[:60]!r}"
    )
    slot2 = db_readonly.fetch_one(
        live_db, "SELECT state, real_msg_id FROM placeholder_slots WHERE id = ?", (slot["id"],)
    )
    assert slot2 is not None and slot2["state"] == "published", dict(slot2)
    assert int(slot2["real_msg_id"] or 0) == int(card.id), dict(slot2)
    hist2 = db_readonly.fetch_one(
        live_db,
        "SELECT tg_message_id FROM publish_history WHERE file_path = ? AND file_size = ?",
        (_norm_key(fail_path), fail_path.stat().st_size),
    )
    assert hist2 is not None and int(hist2["tg_message_id"] or 0) == int(card.id), (
        "回填成功后 publish_history 应补记且 tg_message_id = 原卡消息 ID"
    )
    for msg in [*msgs1, *msgs2]:
        live_cleanup.register_message(target_chat, int(msg.id))


async def test_album_group_summary_vs_per_item_cards(
    v3_samples, live_api, tg_reader, target_chat, live_cleanup, live_db
) -> None:
    """E2E-LIVE-15 (承 E2E-FLOW-02 消息形态半边 + V3 D2/R1): 相册整组失败汇总卡 vs
    非相册失败逐条卡 vs 超限缺集说明卡。

    混合文件夹 (8 图相册 + 成功视频 + 失败视频 + 超限 MKV): 相册组传输期整组失败 →
    恰 1 张汇总卡 + 8 个 group_summary 槽 (共享 msg_id, 任务终态 failed 后即 ❌ 终态化);
    非相册失败项 → 逐条 ⏳ backfillable 卡; 超限项 → "缺集说明" 纯文本卡且无槽。
    """
    folder = _make_run_folder(live_cleanup, "album_mix")
    for jpeg in v3_samples["album"]:
        local_samples.copy_into(folder, jpeg, jpeg.name)
    local_samples.copy_into(folder, v3_samples["a"], "v3_a1_ok.mp4")
    fail_path = local_samples.copy_into(folder, v3_samples["m"], "v3_m5_fail.mp4")
    big_path = local_samples.copy_into(folder, v3_samples["big"], "v3_big.mkv")

    chat = await _resolve_chat_or_skip(tg_reader, target_chat)
    mark = await tg_reader.max_message_id(chat)
    live_cleanup.register_watermark(target_chat, mark)

    task_id = _submit_folder_publish(live_api, folder, target_chat, album=True)
    live_cleanup.register_task(task_id)
    _wait_running(live_api, task_id)
    time.sleep(_INJECT_DELAY_S)
    for jpeg in v3_samples["album"]:
        _inject_source_loss(folder / jpeg.name)  # 相册组整组失败注入
    _inject_source_loss(fail_path)  # 非相册失败项注入
    final = live_api.wait_terminal(task_id, timeout=1800)
    assert final["status"] == "failed", f"相册整组失败任务终态应为 failed: {final.get('error_detail')}"

    logs = pikpak_probe.logs_text(live_api, task_id)
    assert "相册成员失败" in logs or "Album 发布失败" in logs, logs[-1500:]
    assert "⏳ 已发送失败占位卡" in logs, logs[-1500:]

    msgs = await tg_reader.collect_messages_after(chat, mark)
    videos = [m for m in msgs if is_video_message(m)]
    photos = [m for m in msgs if is_photo_message(m)]
    texts = [m for m in msgs if (m.text or "").strip() and not is_photo_message(m) and not is_video_message(m)]
    assert len(videos) == 1, f"应恰 1 条成功视频 (v3_a1_ok), 实得 {len(videos)}"
    assert len(photos) == 1, (
        f"非相册失败项应恰 1 张 ⏳ Photo 卡, 实得 {len(photos)}; texts={[(m.text or '')[:36] for m in texts]}"
    )
    assert "⏳ 【补发排队】" in (photos[0].text or ""), photos[0].text
    summary = [m for m in texts if "相册等待补发" in (m.text or "") or "相册补发终止" in (m.text or "")]
    assert len(summary) == 1, (
        f"相册整组失败应恰 1 张汇总卡 (严禁逐条连发), 实得 {len(summary)}; texts={[(m.text or '')[:36] for m in texts]}"
    )
    oversize_card = [m for m in texts if "缺集说明" in (m.text or "") and "v3_big" in (m.text or "")]
    assert len(oversize_card) == 1, f"超限项应恰 1 张缺集说明卡: {[(m.text or '')[:60] for m in texts]}"

    # ---- finalize 竞态等待: update_status 先于槽终态化 (publish_runner.py:118 vs 121) ----
    # 轮询 DB 至 group_summary 槽被 ❌ 终态化, 再回读消息断言 ❌ 文案
    deadline = time.monotonic() + 30
    group_rows: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        group_rows = _slot_rows(
            live_db,
            "SELECT * FROM placeholder_slots WHERE task_id = ? AND slot_kind = 'group_summary'",
            (task_id,),
        )
        if group_rows and all(r["state"] == "failed_terminal" for r in group_rows):
            break
        time.sleep(1.0)
    assert len(group_rows) == 8, f"8 个相册成员应共享 8 条 group_summary 槽, 实得 {len(group_rows)}"
    assert len({int(r["placeholder_msg_id"] or 0) for r in group_rows}) == 1, "组内成员应共享同一汇总卡 msg_id"
    assert all(r["state"] == "failed_terminal" for r in group_rows), [dict(r) for r in group_rows]
    refetched = await tg_reader.get_messages(chat, [int(group_rows[0]["placeholder_msg_id"] or 0)])
    summary_text = (refetched[0].text or "") if isinstance(refetched, list) and refetched else ""
    assert "【相册补发终止】" in summary_text, (
        f"任务终态 failed 后汇总卡应已被 finalize 编辑为 ❌ 终态文案, 实得 {summary_text[:60]!r}"
    )
    backfill_rows = _slot_rows(
        live_db,
        "SELECT * FROM placeholder_slots WHERE task_id = ? AND slot_kind = 'backfillable'",
        (task_id,),
    )
    assert len(backfill_rows) == 1 and backfill_rows[0]["state"] == "placeholder_sent", (
        f"非相册失败项应恰 1 个 backfillable 槽 (placeholder_sent): {[dict(r) for r in backfill_rows]}"
    )
    big_slot = db_readonly.fetch_one(live_db, "SELECT id FROM placeholder_slots WHERE file_path = ?", (str(big_path),))
    assert big_slot is None, "超限缺集说明卡不应产生占位槽 (纯文本终态)"
    # 诊断明细经 params_json 信封透出 (TaskResponse.params): failed_files 封顶 {items, total}
    params = final.get("params") or {}
    failed_items = (params.get("failed_files") or {}).get("items") or []
    failed_names = {d.get("file_name") for d in failed_items}
    assert "v3_m5_fail.mp4" in failed_names, f"失败明细应含 v3_m5_fail: {failed_names}"
    skipped_names = {s.get("file_name") for s in (params.get("skipped_files") or [])}
    assert "v3_big.mkv" in skipped_names, f"超限明细应含 v3_big.mkv: {skipped_names}"

    for msg in msgs:
        live_cleanup.register_message(target_chat, int(msg.id))


async def test_failure_card_cap_ten_plus_one(
    v3_samples, live_api, tg_reader, target_chat, live_cleanup, live_db
) -> None:
    """E2E-LIVE-16 (V3 D6): 失败卡 10+1 封顶真链路。

    14 个超限失败候选 (注入后 ≥11 项失败): 频道逐条 ⏳ Photo 卡恰 10 张 + 1 张溢出
    汇总卡 (无槽); DB backfillable 槽恰 10; 日志出现封顶提示。
    """
    folder = _make_run_folder(live_cleanup, "cap")
    names = [f"cap_{i:02d}.mp4" for i in range(1, 15)]
    for name in names:
        local_samples.copy_into(folder, v3_samples["m"], name)

    chat = await _resolve_chat_or_skip(tg_reader, target_chat)
    mark = await tg_reader.max_message_id(chat)
    live_cleanup.register_watermark(target_chat, mark)

    task_id = _submit_folder_publish(live_api, folder, target_chat, album=False)
    live_cleanup.register_task(task_id)
    _wait_running(live_api, task_id)
    time.sleep(_INJECT_DELAY_S)
    for name in names:
        _inject_source_loss(folder / name)  # 全员失败注入 (前 1~2 项可能已完成, 仍 ≥11 失败)
    final = live_api.wait_terminal(task_id, timeout=1800)
    assert final["status"] == "failed", f"全员失败任务终态应为 failed: {final.get('error_detail')}"

    failed_total = int(((final.get("params") or {}).get("failed_files") or {}).get("total") or 0)
    assert failed_total >= 11, f"注入后失败项应 ≥11 (超出封顶), 实得 {failed_total}"

    msgs = await tg_reader.collect_messages_after(chat, mark)
    photos = [m for m in msgs if is_photo_message(m)]
    videos = [m for m in msgs if is_video_message(m)]
    texts = [m for m in msgs if (m.text or "").strip() and not is_photo_message(m) and not is_video_message(m)]
    assert len(photos) == 10, f"逐条失败卡应恰 10 张, 实得 {len(photos)}; texts={[(m.text or '')[:36] for m in texts]}"
    assert all("⏳ 【补发排队】" in (m.text or "") for m in photos), "逐条卡均应为 ⏳ 待回填形态"
    overflow = [m for m in texts if "【批量异常提示】" in (m.text or "")]
    assert len(overflow) == 1, (
        f"应恰 1 张溢出汇总卡, 实得 {len(overflow)}; texts={[(m.text or '')[:40] for m in texts]}"
    )
    assert len(videos) <= 3, f"注入前完成发布的视频应 ≤3, 实得 {len(videos)}"

    logs = pikpak_probe.logs_text(live_api, task_id)
    assert "失败占位卡已达封顶" in logs, logs[-1500:]

    rows = _slot_rows(
        live_db,
        "SELECT state FROM placeholder_slots WHERE task_id = ? AND slot_kind = 'backfillable'",
        (task_id,),
    )
    assert len(rows) == 10, f"backfillable 槽应恰 10 (第 11+ 项失败无卡无槽), 实得 {len(rows)}"
    assert all(r["state"] == "placeholder_sent" for r in rows), "fail_count=1 < 1+max_retries, 不应被 finalize 终态化"
    for msg in msgs:
        live_cleanup.register_message(target_chat, int(msg.id))
