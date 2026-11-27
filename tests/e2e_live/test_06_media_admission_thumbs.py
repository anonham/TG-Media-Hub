"""媒体封面 (thumbs) 与准入过滤真链路 E2E 测试 (E2E-LIVE-24 ~ 27)。

被测语义 (
- 视频 (本地与 PikPak 源) 在 send 与原地回填 edit 两条路径都应携带归一化封面
  (JPEG ≤320px, Pillow 归一化); 全黑片源回退占位海报资产;
- 本地通道: 伪 .ts (文本伪装) 在扫描期被过滤 ("非有效 MPEG-TS 视频格式"), 真 .ts
  与 .m4v 正常准入发布 (默认开启 auto_remux_mpegts_to_mp4 时, H.264/AAC
  真 .ts 在发布前零落盘重封装为 MP4, 与 .m4v 同以可流播视频卡片形态落库,
  publish_history 仍以原始 .ts file_path 入库);
- PikPak 通道: 无效远端 .ts 在上传/超限/归档之前被拒 (failed_files, Range 校验
  fail-open 时 skip), 不发包位卡、无归档副作用; 真远端 .ts 正常发布 (skip-if-absent)。

封面断言通道 = Telethon 只读 thumb 下载 + PIL 均值 (信息性): Telegram 会为带视频
属性的消息服务端自生成预览, 下载通道无法区分发送侧封面与平台预览 —— 回退/归一化
语义由离线产品测试 (test_thumbnail_workflow) 锁定, live 层只断言 thumbs 结构与规格。
样本由 harness 以 ffmpeg 制备 (幂等缓存), 注入失败与清理账本口径同 E2E-LIVE-14 ~ 16。
"""

import asyncio
import io
import os
import shutil
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest

from app.core.media_sniff import MPEG_TS_MIN_REJECTION_BYTES
from app.core.pipeline.engines.pikpak.engine import TS_FAIL_OPEN_LOG_PREFIX
from e2e_live.harness import api_client, db_readonly, local_samples, pikpak_probe, server_proc
from e2e_live.harness import tg_reader as tg_reader_mod
from e2e_live.harness.tg_reader import is_document_message, is_photo_message, is_video_message

pytestmark = [pytest.mark.e2e_live]

_THUMB_MAX_DIM = 400  # 归一化上界 ≤320; 放宽到 400 容纳 Telegram 服务端补充尺寸


@pytest.fixture(scope="module")
def thumb_samples(live_gate) -> dict[str, Path]:
    """封面用例样本: ffmpeg 缺失或制备失败即整包 skip。mid = 7.5MB 中档样本
    (回填注入用 —— 300KB 小样本在 8×2 并发下 1.2s 注入窗口内即完成发布)。"""
    if shutil.which("ffmpeg") is None:
        pytest.skip("ffmpeg 不在 PATH, 封面样本无法制备")
    try:
        clips = local_samples.ensure_thumb_clips()
        base = local_samples.ensure_v3_base()
    except (RuntimeError, OSError) as exc:
        pytest.skip(f"本地样本制备失败: {exc}")
    return {**clips, "mid": base["m"]}


@pytest.fixture(scope="module")
def admission_samples(live_gate) -> dict[str, Path]:
    """准入用例样本。"""
    if shutil.which("ffmpeg") is None:
        pytest.skip("ffmpeg 不在 PATH, 准入样本无法制备")
    try:
        return local_samples.ensure_admission_family()
    except (RuntimeError, OSError) as exc:
        pytest.skip(f"本地样本制备失败: {exc}")


def _wait_running(live_api: api_client.LiveApiClient, task_id: str, timeout: float = 120.0) -> None:
    """等待任务进入 running (注入时序锚点); attach 形态 429 按退避容忍。"""
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


def _submit_folder_publish(
    live_api: api_client.LiveApiClient, folder: Path, target_chat: str, *, album: bool, placeholder: bool = False
) -> str:
    resp = live_api.post(
        "/api/publish/folder",
        json={
            "folder_path": str(folder),
            "target_channel": target_chat,
            "album_mode": album,
            "delay_seconds": 0.5,
            "send_placeholder_card": placeholder,
            "oversize_policy": "skip_and_report",
            "upload_concurrency": 8,
            "upload_in_flight_per_connection": 2,
        },
    )
    assert resp.status_code == 200, resp.text
    return str(resp.json()["task_id"])


def _norm_key(path: Path) -> str:
    """publish_history 查询键归一 (与写入侧 norm_file_path 同口径)。"""
    from app.db.publish_repository import norm_file_path

    return norm_file_path(str(path))


def _make_run_folder(live_cleanup, name: str) -> Path:
    """任务文件夹必须在 DATA_DIR 之外 (发布入口扫描守卫), 落样本缓存根下随账本回收。"""
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


def _thumb_brightness(data: bytes | None) -> float:
    """thumb 灰度均值 (PIL): 回退海报为彩色资产, 全黑帧均值 ≈0。"""
    from PIL import Image, ImageStat

    assert data, "thumb 下载失败 (字节为空)"
    with Image.open(io.BytesIO(data)) as img:
        img.load()
        stat = ImageStat.Stat(img.convert("L"))
    return float(stat.mean[0])


def _assert_thumb_dimensions(msg: Any) -> None:
    """消息封面结构断言: thumbs 非空且尺寸不超归一化上界。"""
    video = getattr(msg, "video", None)
    assert video is not None, f"消息 {msg.id} 应为视频文档"
    thumbs = list(getattr(video, "thumbs", None) or [])
    assert thumbs, f"视频消息 {msg.id} 应携带封面 (thumbs 非空)"
    sized = [(int(t.w), int(t.h)) for t in thumbs if getattr(t, "w", None) and getattr(t, "h", None)]
    assert sized, f"thumbs 应含 PhotoSize 尺寸信息: {thumbs!r}"
    assert max(max(d) for d in sized) <= _THUMB_MAX_DIM, f"封面尺寸超出归一化上界: {sized}"


def _lock_file_exclusive(path: Path):
    """对整文件加 Windows 字节锁 (msvcrt.locking): 服务器进程任何读取立即命中
    锁冲突 → 发送必然失败 —— 确定性故障注入, 与上传速度/时序完全无关
    (8×2 并发提速后时序类注入无法命中, )。返回句柄供解锁。"""
    import msvcrt

    size = path.stat().st_size
    fh = open(path, "r+b")  # noqa: SIM115 — 锁句柄须跨发布生命周期持存, 重试回填前显式解锁关闭
    msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, max(size, 1))
    return fh


def _unlock_file(fh) -> None:
    """释放字节锁并关闭句柄 (重试回填前必须解锁)。"""
    import msvcrt

    try:
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, max(os.fstat(fh.fileno()).st_size, 1))
    except OSError:
        pass
    finally:
        fh.close()


async def _publish_and_collect(
    live_api: api_client.LiveApiClient,
    tg_reader: tg_reader_mod.TgReader,
    target_chat: str,
    live_cleanup,
    folder: Path,
    *,
    album: bool,
    placeholder: bool = False,
    budget_s: float = 900.0,
) -> tuple[str, dict[str, Any], list[Any], Any]:
    """提交发布 → 终态 → 频道水位增量收集 (登记任务/水位, 消息由调用方登记)。"""
    chat = await _resolve_chat_or_skip(tg_reader, target_chat)
    mark = await tg_reader.max_message_id(chat)
    live_cleanup.register_watermark(target_chat, mark)
    task_id = _submit_folder_publish(live_api, folder, target_chat, album=album, placeholder=placeholder)
    live_cleanup.register_task(task_id)
    final = live_api.wait_terminal(task_id, timeout=budget_s)
    msgs = await tg_reader.collect_messages_after(chat, mark)
    return task_id, final, msgs, chat


async def test_video_cover_on_send_path(thumb_samples, live_api, tg_reader, target_chat, live_cleanup, live_db) -> None:
    """E2E-LIVE-24: 正常视频发布 → send 路径携带归一化封面 + publish_history 入账。"""
    folder = _make_run_folder(live_cleanup, "thumbs_send")
    clip = local_samples.copy_into(folder, thumb_samples["bright"], "bright.mp4")

    _task_id, final, msgs, _chat = await _publish_and_collect(
        live_api, tg_reader, target_chat, live_cleanup, folder, album=False
    )
    assert final["status"] == "completed", (
        f"单视频发布应 completed: {final.get('status')} / {final.get('error_detail')}"
    )
    assert final["progress"]["processed_items"] == 1, final["progress"]

    videos = [m for m in msgs if is_video_message(m)]
    assert len(videos) == 1, f"应恰 1 条视频消息, 实得 {len(videos)}; msgs={[(m.text or '')[:40] for m in msgs]}"
    _assert_thumb_dimensions(videos[0])
    brightness = _thumb_brightness(await tg_reader.download_thumb(videos[0]))
    assert brightness > 40.0, f"正常内容封面不应为近黑帧 (均值 {brightness:.1f})"

    hist = db_readonly.fetch_one(
        live_db,
        "SELECT tg_message_id FROM publish_history WHERE file_path = ? AND file_size = ?",
        (_norm_key(clip), clip.stat().st_size),
    )
    assert hist is not None and int(hist["tg_message_id"] or 0) > 0, "发布成功应入 publish_history"
    for msg in msgs:
        live_cleanup.register_message(target_chat, int(msg.id))


@pytest.mark.skipif(sys.platform != "win32", reason="LIVE-25 故障注入依赖 Windows msvcrt 字节锁，非 Windows 跳过")
async def test_black_source_cover_fallback_send_and_backfill(
    thumb_samples, live_api, tg_reader, target_chat, live_cleanup, live_db
) -> None:
    """E2E-LIVE-25: 全黑片源 send 路径回退占位海报 (结构断言); 原地回填 edit 路径
    同样携带封面。

    Phase A: 全黑视频直发 → thumbs 结构断言 (像素亮度判别不可靠, 见下); Phase B:
    事件锚定源丢失 → 失败卡 → 复活重试 → 原消息 ID 原地编辑为正式视频且携带封面
    (editMessageMedia 路径)。
    """
    # ---- Phase A: send 路径回退 ----
    folder = _make_run_folder(live_cleanup, "thumbs_black")
    black_a = local_samples.copy_into(folder, thumb_samples["black"], "black_a.mp4")

    _task_id, final, msgs, _chat = await _publish_and_collect(
        live_api, tg_reader, target_chat, live_cleanup, folder, album=False
    )
    assert final["status"] == "completed", (
        f"全黑视频发布应 completed: {final.get('status')} / {final.get('error_detail')}"
    )
    videos = [m for m in msgs if is_video_message(m)]
    assert len(videos) == 1, f"应恰 1 条视频消息, 实得 {len(videos)}"
    _assert_thumb_dimensions(videos[0])
    # 像素亮度判别在此通道不可靠: Telegram 会为带视频属性的消息服务端自生成预览
    # (黑片源即纯黑预览), 与发送侧附加封面无法经下载通道区分 (
    # 离线 get_local_thumbnail(全黑片源) → 占位海报资产 ✓, 回退语义由
    # test_thumbnail_workflow 锁定; 此处只断言 thumbs 结构与规格)。
    brightness = _thumb_brightness(await tg_reader.download_thumb(videos[0]))
    print(f"[info] 全黑片源下载 thumb 均值 {brightness:.1f} (TG 自生成预览为 0.0)")
    hist_a = db_readonly.fetch_one(
        live_db,
        "SELECT tg_message_id FROM publish_history WHERE file_path = ? AND file_size = ?",
        (_norm_key(black_a), black_a.stat().st_size),
    )
    assert hist_a is not None and int(hist_a["tg_message_id"] or 0) > 0
    for msg in msgs:
        live_cleanup.register_message(target_chat, int(msg.id))

    # ---- Phase B: 回填 edit 路径 (失败档用 7.5MB 中档样本 + 整文件字节锁注入:
    #      确定性故障, 与上传速度/时序无关; 解锁后文件完好, 重试直接回填) ----
    folder_b = _make_run_folder(live_cleanup, "thumbs_backfill")
    fail_b = local_samples.copy_into(folder_b, thumb_samples["mid"], "backfill_fail.mp4")
    lock_fh = _lock_file_exclusive(fail_b)
    try:
        task_id_b, final_b, msgs_b, _chat_b = await _publish_and_collect(
            live_api, tg_reader, target_chat, live_cleanup, folder_b, album=False, placeholder=True
        )
    finally:
        _unlock_file(lock_fh)
    assert final_b["status"] == "failed", (
        f"锁定文件任务应 failed: {final_b.get('status')} / {final_b.get('error_detail')}"
    )
    cards = [m for m in msgs_b if is_photo_message(m)]
    assert len(cards) == 1 and "⏳ 【补发排队】" in (cards[0].text or ""), (
        f"应恰 1 张 ⏳ Photo 失败卡: {[(m.text or '')[:40] for m in msgs_b]}"
    )
    card_id = int(cards[0].id)
    for msg in msgs_b:
        live_cleanup.register_message(target_chat, int(msg.id))

    retry = live_api.post(f"/api/tasks/{task_id_b}/retry")
    assert retry.status_code == 200, retry.text
    final_b2 = live_api.wait_terminal(task_id_b, timeout=1200)
    assert final_b2["status"] == "completed", (
        f"重试回填应 completed: {final_b2.get('status')} / {final_b2.get('error_detail')}"
    )

    refetched = await tg_reader.get_messages(_chat_b, [card_id])
    backfilled = refetched[0] if isinstance(refetched, list) else refetched
    assert backfilled is not None and is_video_message(backfilled), f"原消息 {card_id} 应已被原地编辑为正式视频"
    _assert_thumb_dimensions(backfilled)
    brightness_b = _thumb_brightness(await tg_reader.download_thumb(backfilled))
    print(f"[info] 回填消息下载 thumb 均值 {brightness_b:.1f} (结构断言为主, 理由见 Phase A)")
    slot = db_readonly.fetch_one(
        live_db,
        "SELECT state, real_msg_id FROM placeholder_slots WHERE task_id = ? AND slot_kind = 'backfillable'",
        (task_id_b,),
    )
    assert slot is not None and slot["state"] == "published", dict(slot or {})
    assert int(slot["real_msg_id"] or 0) == card_id, dict(slot)


@contextmanager
def _enable_remux_for_live_test(live_api: api_client.LiveApiClient, *, external: bool) -> Iterator[None]:
    key = "auto_remux_mpegts_to_mp4"
    settings_resp = live_api.get("/api/settings")
    settings_resp.raise_for_status()
    previous = str(settings_resp.json().get("all_settings", {}).get(key, "")).strip().lower()
    if previous == "true":
        yield
        return
    if external:
        pytest.skip(f"外部 E2E 实例需预先启用 {key}")
    if previous != "false":
        pytest.skip(f"无法读到可恢复的 {key} 值")

    try:
        live_api.put("/api/settings", json={"settings": {key: "true"}}).raise_for_status()
        yield
    finally:
        live_api.put("/api/settings", json={"settings": {key: previous}}).raise_for_status()


async def test_local_admission_invalid_ts_filtered_and_ts_m4v_published(
    admission_samples, live_api, tg_reader, target_chat, live_cleanup, live_db, live_server
) -> None:
    """E2E-LIVE-26: 本地通道准入 —— 伪 .ts 扫描期过滤 (inspect 通道归因可见),
    真 .ts (auto_remux_mpegts_to_mp4 自动转封装为 MP4) 与 .m4v 均以可流播视频形态发布。"""
    folder = _make_run_folder(live_cleanup, "admission_local")
    genuine = local_samples.copy_into(folder, admission_samples["genuine_ts"], "b_real.ts")
    m4v = local_samples.copy_into(folder, admission_samples["m4v"], "c_clip.m4v")
    local_samples.copy_into(folder, admission_samples["fake_ts"], "a_fake.ts")

    # 预检扫描通道: 伪 .ts 进 ignored_files 且带 MPEG-TS 归因; 媒体清单恰 2 项视频
    inspect = live_api.post("/api/publish/inspect", json={"folder_path": str(folder)})
    assert inspect.status_code == 200, inspect.text
    data = inspect.json()
    ignored = {str(e.get("file_name")): str(e.get("reason", "")) for e in data.get("ignored_files", [])}
    assert "a_fake.ts" in ignored and "MPEG-TS" in ignored["a_fake.ts"], f"伪 .ts 应在预检扫描被过滤并归因: {ignored}"
    media_names = {str(e.get("file_name")) for e in data.get("media_files", [])}
    assert media_names == {"b_real.ts", "c_clip.m4v"}, f"媒体清单不应含伪 .ts: {media_names}"

    with _enable_remux_for_live_test(live_api, external=isinstance(live_server, server_proc.ExternalServer)):
        _task_id, final, msgs, _chat = await _publish_and_collect(
            live_api, tg_reader, target_chat, live_cleanup, folder, album=False
        )
        assert final["status"] == "completed", (
            f"真 .ts + .m4v 发布应 completed: {final.get('status')} / {final.get('error_detail')}"
        )
        assert final["progress"]["processed_items"] == 2, final["progress"]

        videos = [m for m in msgs if is_video_message(m)]
        documents = [m for m in msgs if is_document_message(m)]
        photos = [m for m in msgs if is_photo_message(m)]
        # 默认开启 auto_remux_mpegts_to_mp4, 真 .ts (H.264+AAC) 自动重封装为 MP4
        # 并保留原 caption ("b_real.ts"), 与 .m4v 同以可流播视频卡片落库
        video_captions = {(m.text or "").strip() for m in videos}
        assert len(videos) == 2 and video_captions == {"b_real.ts", "c_clip.m4v"}, (
            f"真 .ts (auto-remux) 与 .m4v 均应以视频形态发布, 实得 videos={[(m.text or '')[:40] for m in videos]}; "
            f"documents={[(m.text or '')[:40] for m in documents]}"
        )
        assert not documents, (
            f"开启 auto_remux 后真 .ts 不应退化为裸 document: {[(m.text or '')[:40] for m in documents]}"
        )
        assert not photos, f"不应产生占位卡: {[(m.text or '')[:40] for m in photos]}"

        for m in videos:
            _assert_thumb_dimensions(m)

        fake_hist = db_readonly.fetch_one(
            live_db, "SELECT id FROM publish_history WHERE file_path LIKE ?", ("%a_fake.ts",)
        )
        assert fake_hist is None, "伪 .ts 严禁入 publish_history"
        for path in (genuine, m4v):
            row = db_readonly.fetch_one(
                live_db,
                "SELECT tg_message_id FROM publish_history WHERE file_path = ? AND file_size = ?",
                (_norm_key(path), path.stat().st_size),
            )
            assert row is not None and int(row["tg_message_id"] or 0) > 0, f"{path.name} 应入 publish_history"
        for msg in msgs:
            live_cleanup.register_message(target_chat, int(msg.id))


def _pick_remote_ts_candidates(
    remote_files: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """从远端文件清单挑选 (fake, genuine) .ts 对: 命名含 fake 且 ≥816B 者视为有效伪造件; 任一缺失返回 None。"""
    ts_files = [
        f
        for f in remote_files
        if str(f.get("name", "")).lower().endswith(".ts") and not str(f.get("name", "")).lower().endswith(".d.ts")
    ]
    fakes = [
        f
        for f in ts_files
        if "fake" in str(f.get("name", "")).lower() and int(f.get("size_bytes") or 0) >= MPEG_TS_MIN_REJECTION_BYTES
    ]
    genuine = [f for f in ts_files if "fake" not in str(f.get("name", "")).lower()]
    if not fakes or not genuine:
        return None
    fake = sorted(fakes, key=lambda i: i.get("size_bytes") or 0)[0]
    genuine_pick = sorted(genuine, key=lambda i: i.get("size_bytes") or 0)[0]
    return fake, genuine_pick


async def test_pikpak_remote_ts_admission_gate(
    live_api, tg_reader, target_chat, live_cleanup, scratch_data_dir
) -> None:
    """E2E-LIVE-27 (skip-if-absent): PikPak 远端无效 .ts 在上传/超限/归档之前被拒
    (failed_files, 无占位卡), 真远端 .ts 正常发布。

    前置: 远端测试目录预置 *fake*.ts (≥816B 文本伪装件) 与真 .ts (ffmpeg mpegts) 各一;
    任一缺失即 skip (远端样本不随仓库制备, 同 E2E-LIVE-24 真实世界 .mov 先例)。
    """
    auth = live_api.get("/api/pikpak/auth/status")
    assert auth.status_code == 200
    state = auth.json().get("state")
    if state != "logged_in":
        pytest.skip(f"PikPak 未就绪 (state={state})")

    remote_dir = os.environ.get("E2E_LIVE_PIKPAK_REMOTE_DIR", "").strip()
    folder_id = pikpak_probe.resolve_remote_folder(live_api, remote_dir) if remote_dir else None
    if folder_id is None:
        pytest.skip(
            "PikPak 远端测试目录不可用 "
            f"(E2E_LIVE_PIKPAK_REMOTE_DIR={remote_dir or '未配置'}, resolve={'成功' if folder_id else '失败'})"
        )
    remote_files = [i for i in pikpak_probe.list_remote_page(live_api, folder_id) if not i.get("is_dir")]
    candidates = _pick_remote_ts_candidates(remote_files)
    if candidates is None:
        pytest.skip(
            f"PikPak 远端测试目录缺 .ts 准入样本 (须含 *fake*.ts 且 ≥{MPEG_TS_MIN_REJECTION_BYTES}B 伪装件与真 .ts 各一): "
            f"ts_names={[str(f.get('name')) for f in remote_files]}"
        )
    picked_fake, picked_genuine = candidates

    chat = await _resolve_chat_or_skip(tg_reader, target_chat)
    mark = await tg_reader.max_message_id(chat)
    live_cleanup.register_watermark(target_chat, mark)

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
                for f in (picked_fake, picked_genuine)
            ],
            "target_channel": target_chat,
            "auto_cleanup_mode": "none",
            "oversize_policy": "skip_and_report",
            # 同会话 zero-disk 用例按体积最小优先会先发布这对外样本, 必须绕过防重
            "force_republish": True,
            "send_delay_seconds": 0.5,
        },
    )
    assert resp.status_code == 200, resp.text
    task_id = resp.json()["task_id"]
    live_cleanup.register_task(task_id)
    snapshot = await asyncio.to_thread(live_api.wait_terminal, task_id, timeout=1200.0, poll_interval=5.0)

    fake_name = str(picked_fake.get("name"))
    genuine_name = str(picked_genuine.get("name"))
    params = snapshot.get("params") or {}
    failed_reasons = {
        str(d.get("file_name") or ""): str(d.get("reason") or "")
        for d in ((params.get("failed_files") or {}).get("items") or [])
    }
    logs = pikpak_probe.logs_text(live_api, task_id)
    # 仅以准入拒绝产物 failed_files 且 reason 明确为 MPEG-TS 过滤判定拒绝,
    # 避免无关上传失败或常规文件名命中掩盖门禁缺失; fail-open 降级时必须有本文件标记才 skip
    fake_rejected = fake_name in failed_reasons and "MPEG-TS" in failed_reasons[fake_name]
    fail_open_marker = f"{TS_FAIL_OPEN_LOG_PREFIX} [{fake_name}]"
    if not fake_rejected:
        if fail_open_marker in logs:
            pytest.skip(f"远端 TS 门禁触发 fail-open 降级 ({fail_open_marker}); 拒绝语义无法在当前网络条件验证")
        pytest.fail(
            f"无效 .ts ({fake_name}) 未被 MPEG-TS 准入门禁拒绝进 failed_files, 且任务日志无 fail-open 标记 "
            f"(疑似远端准入门禁缺失或回归): failed={failed_reasons}; logs={logs[-800:]}"
        )
    assert genuine_name not in failed_reasons, f"真 .ts 不应失败: failed={failed_reasons}; logs={logs[-600:]}"

    msgs = await tg_reader.collect_messages_after(chat, mark)
    videos = [m for m in msgs if is_video_message(m)]
    documents = [m for m in msgs if is_document_message(m)]
    photos = [m for m in msgs if is_photo_message(m)]
    # 默认开启 auto_remux_mpegts_to_mp4 后, 兼容编码的真 .ts 自动转封装为可流播视频;
    # 若远端真实样本为不支持 copy 的老编码并降级为 document, 也保持 caption 匹配容忍
    matched_media = [m for m in (*videos, *documents) if (m.text or "").strip() == genuine_name]
    assert len(matched_media) == 1 and len(msgs) == 1, (
        f"应恰 1 条真 .ts 消息, 实得 {[(m.text or '')[:40] for m in msgs]}; logs={logs[-600:]}"
    )
    assert not photos, f"无效 .ts 被拒严禁发包位卡: {[(m.text or '')[:40] for m in photos]}"
    for msg in msgs:
        live_cleanup.register_message(target_chat, int(msg.id))
