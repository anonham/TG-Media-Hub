"""五类任务真链路冒烟 (E2E-LIVE-02 ~ 06, ; 承 E2E-STRAT-01 ~ 05)。

断言三通道: REST 响应 + scratch 库只读直查 + Telethon 只读拉取; live 层
零"人工目测"断言。真实发送一律串行且走服务器自身节流; 各用例产物统一登记
会话清理账本, 会话结束删消息 → 删任务 (§3.2 清理口径)。
"""

import os
import time

import pytest

from e2e_live.harness import api_client, db_readonly, pikpak_probe, samples
from e2e_live.harness import tg_reader as tg_reader_mod
from e2e_live.harness.tg_reader import is_photo_message

pytestmark = [pytest.mark.e2e_live]


def test_download_link_real_chain(channel_stage, live_api, live_db, live_cleanup) -> None:
    """E2E-LIVE-02 (承 E2E-STRAT-01): download_link 真链路。

    REST 终态 (双量纲 processed_items/total_bytes) + DB 只读直查 + 下载字节数锚点。
    """
    chat_ref = channel_stage["chat_ref"]
    media_id = channel_stage["media1_id"]
    expected_size = channel_stage["media_size"]
    if chat_ref.startswith("-100"):
        link = f"https://t.me/c/{chat_ref[4:]}/{media_id}"
    elif "/" not in chat_ref and not chat_ref.startswith("@"):
        link = f"https://t.me/{chat_ref}/{media_id}"
    else:
        link = f"{chat_ref.rstrip('/')}/{media_id}"

    resp = live_api.post("/api/download/link", json={"links": link, "threads": 4})
    assert resp.status_code == 200, resp.text
    task_id = resp.json()["task_id"]
    live_cleanup.register_task(task_id)

    final = live_api.wait_terminal(task_id, timeout=600)
    assert final["status"] == "completed", final.get("error_detail")
    progress = final["progress"]
    assert progress["processed_items"] == 1, progress
    assert progress["total_bytes"] == expected_size, (
        f"下载字节数不符 (文档发送应字节保真): {progress['total_bytes']} != {expected_size}"
    )

    row = db_readonly.fetch_one(
        live_db,
        "SELECT status, task_type, processed_items, total_items FROM task_queue WHERE id = ?",
        (task_id,),
    )
    assert row is not None, "task_queue 缺少任务行"
    assert row["status"] == "completed" and row["task_type"] == "download_link"
    assert row["processed_items"] == 1 and row["total_items"] == 1


def test_download_range_skips_non_media(channel_stage, live_api, live_cleanup) -> None:
    """E2E-LIVE-03 (承 E2E-STRAT-02): download_range 真链路 —— 区间含非媒体消息。

    执行器条目语义 (实证): processed_items 计区间全消息 (含跳过的非媒体),
    processed_bytes 只计实际下载的媒体字节 —— 两者均与预检 inspect-range 精确对齐。
    """
    chat_ref = channel_stage["chat_ref"]
    start_id = min(channel_stage["media2_id"], channel_stage["text_id"])
    end_id = max(channel_stage["media2_id"], channel_stage["text_id"])

    inspect = live_api.post(
        "/api/download/inspect-range",
        json={"channel_identifier": chat_ref, "start_id": start_id, "end_id": end_id},
    )
    assert inspect.status_code == 200, inspect.text
    probe = inspect.json()
    assert probe["messages_with_media"] == 1 and probe["text_only_messages"] == 1, probe

    resp = live_api.post(
        "/api/download/range",
        json={"channel_identifier": chat_ref, "start_id": start_id, "end_id": end_id, "threads": 4},
    )
    assert resp.status_code == 200, resp.text
    task_id = resp.json()["task_id"]
    live_cleanup.register_task(task_id)

    final = live_api.wait_terminal(task_id, timeout=600)
    assert final["status"] == "completed", final.get("error_detail")
    progress = final["progress"]
    assert progress["processed_items"] == probe["total_range_messages"], (
        f"任务条目数应与预检全消息数对齐: probe={probe} final={final}"
    )
    assert progress["processed_bytes"] == probe["estimated_size_bytes"], (
        f"下载字节应与预检媒体体积对齐 (非媒体不计字节): probe={probe} final={final}"
    )


async def test_forward_channel_real_chain(channel_stage, live_api, tg_reader, live_cleanup) -> None:
    """E2E-LIVE-04 (承 E2E-STRAT-03): forward_channel 真链路 —— 回执计数 + Telethon
    水位断言消息到达且无重复。

    #58 同源转发防护: source==target 被产品拒绝 (防自我复制循环), 转发目标必须为
    专用独立频道 E2E_LIVE_FORWARD_TARGET (禁止回退共享 TRANSIT_CHAT_ID 并在
    teardown 做水位全删, 避免误删并发中转消息; 未配置即 skip)。
    """
    chat_ref = channel_stage["chat_ref"]
    media_id = channel_stage["media1_id"]
    forward_target = os.environ.get("E2E_LIVE_FORWARD_TARGET", "").strip()
    if not forward_target:
        pytest.skip(
            "未配置专用转发目标 E2E_LIVE_FORWARD_TARGET (#58 同源转发防护要求源/目标为不同频道, "
            "且禁止对共享 TRANSIT_CHAT_ID 登记水位清理)"
        )
    try:
        target_entity = await tg_reader.resolve_chat(forward_target)
    except tg_reader_mod.ChatUnresolvableError as exc:
        pytest.skip(f"userbot 会话不可解析转发目标 {forward_target}: {exc}")
    # 同实体预检: #58 产品护栏按解析后实体判同源 —— TRANSIT_CHAT_ID 与测试频道为
    # 同一频道时 (常见单频道部署), 该用例无法在单频道环境执行, skip 并给出配置指引
    source_entity = await tg_reader.resolve_chat(chat_ref)
    if getattr(source_entity, "id", None) == getattr(target_entity, "id", None):
        pytest.skip(
            f"转发目标 {forward_target} 与源频道解析为同一实体 (#58 同源转发会被产品拒绝); "
            "如需真链路转发验证, 请配置 E2E_LIVE_FORWARD_TARGET 指向另一个频道"
        )
    target_mark = await tg_reader.max_message_id(target_entity)
    live_cleanup.register_watermark(forward_target, target_mark)

    resp = live_api.post(
        "/api/forward/channel",
        json={
            "source_chat": chat_ref,
            "target_chat": forward_target,
            "start_id": media_id,
            "end_id": media_id,
            "batch_size": 10,
            "delay_seconds": 0.8,
        },
    )
    assert resp.status_code == 200, resp.text
    task_id = resp.json()["task_id"]
    live_cleanup.register_task(task_id)

    final = live_api.wait_terminal(task_id, timeout=900)
    pikpak_probe.completed_or_env_skip(live_api, final)

    receipt = live_api.get(f"/api/forward/receipt/{task_id}")
    assert receipt.status_code == 200, receipt.text
    data = receipt.json()["data"]
    assert data["forwarded"] == 1, data
    assert data["bad"] == 0, data
    assert data.get("dedup_skipped") in (0, None), data

    history = live_api.get(
        "/api/forward/history",
        params={"source_chat": chat_ref, "target_chat": forward_target, "source_msg_id": media_id},
    )
    history.raise_for_status()
    owned = {int(row["target_msg_id"]) for row in history.json()["data"] if row["task_id"] == task_id}
    assert len(owned) == 1 and min(owned) > target_mark, "缺少本轮转发任务的目标消息回执"
    for msg_id in owned:
        live_cleanup.register_message(forward_target, msg_id)
    new_ids = await tg_reader.collect_ids_after(target_entity, target_mark)
    assert owned.issubset(new_ids), f"频道中未找到本轮已转发消息 {owned}"


async def test_publish_folder_real_chain(tmp_path, live_api, tg_reader, target_chat, live_cleanup) -> None:
    """E2E-LIVE-05 (承 E2E-STRAT-04): publish_folder 真链路 —— 3 张小图逐条发送 +
    总结卡; 断言条目数、频道消息形态 (Telethon) 与消息总数。"""
    folder = tmp_path / "e2e_live_publish"
    for i in range(3):
        samples.write_sample_jpeg(folder / f"sample_{i}.jpg")

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

    final = live_api.wait_terminal(task_id, timeout=1200)
    pikpak_probe.completed_or_env_skip(live_api, final)
    assert final["progress"]["processed_items"] == 3, final["progress"]

    msgs = await tg_reader.collect_messages_after(chat, mark)
    photos = [m for m in msgs if is_photo_message(m)]
    # 纯文本卡 = 有文本且非图片 (图片消息自带文件名 caption, 不算卡)
    cards = [m for m in msgs if (m.text or "").strip() and not is_photo_message(m)]
    assert len(msgs) == 5 and len(photos) == 3 and len(cards) == 2, (
        f"频道应新增 3 图片 + 批次卡/任务总结卡 (V2 D4 双卡) = 5 条; 实得 {len(msgs)} 条, "
        f"photos={len(photos)}, cards={len(cards)}, "
        f"texts={[(m.text or '')[:60] for m in msgs]}"
    )
    newest = max(msgs, key=lambda m: m.id)
    assert (newest.text or "").strip(), "最后一条应为总结卡文本消息"


def _is_admission_rejected_fixture(item: dict) -> bool:
    """排除 E2E-LIVE-27 预置的无效 .ts/.d.ts 准入夹具, 避免 zero-disk 挑中必拒样本。"""
    name = str(item.get("name") or "").lower()
    return name.endswith(".d.ts") or (name.endswith(".ts") and "fake" in name)


async def test_pikpak_publish_zero_disk(live_api, tg_reader, target_chat, live_cleanup, scratch_data_dir) -> None:
    """E2E-LIVE-06 (承 E2E-STRAT-05 + VERIFICATION 场景 3): PikPak 零磁盘流式发布。

    断言: 任务完成 + 传输全程 scratch/downloads 零字节增量 (零盘承诺) + 频道产物
    到达。前置: PikPak logged_in 且配置 E2E_LIVE_PIKPAK_REMOTE_DIR 远端测试目录
    (auto_cleanup_mode=none, 不动网盘文件)。
    """
    auth = live_api.get("/api/pikpak/auth/status")
    assert auth.status_code == 200
    state = auth.json().get("state")
    if state != "logged_in":
        pytest.skip(f"PikPak 未就绪 (state={state}), 跳过零盘发布冒烟")

    remote_dir = os.environ.get("E2E_LIVE_PIKPAK_REMOTE_DIR", "").strip()
    if not remote_dir:
        pytest.skip("未配置 E2E_LIVE_PIKPAK_REMOTE_DIR (PikPak 远端测试目录路径)")
    folder_id = pikpak_probe.resolve_remote_folder(live_api, remote_dir)
    if folder_id is None:
        pytest.skip(f"PikPak 远端测试目录不存在: {remote_dir}")
    remote_files = pikpak_probe.list_remote_page(live_api, folder_id)
    remote_files = [i for i in remote_files if not i.get("is_dir") and not _is_admission_rejected_fixture(i)]
    if not remote_files:
        pytest.skip(f"PikPak 测试目录无可用合法文件 (此前运行可能已消费或仅有准入拒收夹具): {remote_dir}")
    picked = sorted(remote_files, key=lambda i: i.get("size_bytes") or 0)[:2]

    try:
        chat = await tg_reader.resolve_chat(target_chat)
    except tg_reader_mod.ChatUnresolvableError as exc:
        pytest.skip(f"无 userbot 会话可解析测试频道 {target_chat}: {exc}")
    mark = await tg_reader.max_message_id(chat)
    live_cleanup.register_watermark(target_chat, mark)
    downloads_dir = scratch_data_dir / "downloads"
    # 基线增量口径: downloads 在会话内跨用例持久 (前序下载用例的样本文件残留),
    # 零盘断言语义为"本传输窗口零新增" —— 按文件快照计算新增字节 (基线文件删除不抵消)
    baseline = pikpak_probe.downloads_snapshot(downloads_dir)

    resp = live_api.post(
        "/api/pikpak/publish",
        json={
            "remote_files": [
                {
                    "file_id": f["file_id"],
                    "name": f["name"],
                    "is_dir": False,
                    "size_bytes": f.get("size_bytes") or 0,
                    "parent_id": f.get("parent_id") or folder_id,
                }
                for f in picked
            ],
            "target_channel": target_chat,
            "auto_cleanup_mode": "none",
            "oversize_policy": "skip_and_report",
            "upload_concurrency": 8,
            "upload_in_flight_per_connection": 2,
            "send_delay_seconds": 0.5,
        },
    )
    assert resp.status_code == 200, resp.text
    task_id = resp.json()["task_id"]
    live_cleanup.register_task(task_id)

    deadline = time.monotonic() + 1800
    max_disk = 0
    status = ""
    snapshot: dict = {}
    while time.monotonic() < deadline:
        snap = live_api.get(f"/api/tasks/{task_id}")
        snap.raise_for_status()
        snapshot = snap.json()
        status = str(snapshot.get("status", ""))
        max_disk = max(max_disk, pikpak_probe.downloads_added_bytes(downloads_dir, baseline))
        if status in api_client.TERMINAL_STATUSES or status == "paused":
            break
        time.sleep(5)
    assert status in api_client.TERMINAL_STATUSES or status == "paused", f"PikPak 发布在时限内未达终态: {status}"
    pikpak_probe.completed_or_env_skip(
        live_api, {"id": task_id, "status": status, "error_detail": snapshot.get("error_detail")}
    )
    assert snapshot["progress"]["processed_items"] == len(picked), snapshot["progress"]
    assert max_disk == 0, f"零盘承诺被破坏: 传输期间 downloads 目录峰值 {max_disk} 字节"

    msgs = await tg_reader.collect_messages_after(chat, mark)
    assert len(msgs) >= len(picked), f"频道新增消息数应 ≥ 文件数: 实得 {len(msgs)}"
