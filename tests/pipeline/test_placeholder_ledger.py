"""Pipeline Core: 占位卡与元数据卡任务级发卡幂等、占位槽存储账本与回填生命周期测试 (Placeholder Ledger & Lifecycle)

覆盖范围:
- 原 test_issue4_placeholder_idempotency: 占位卡与元数据卡任务级发卡幂等性
- 原 test_placeholder_cards: 失败占位卡生成、渲染、延迟与流控
- 原 test_placeholder_slots: PlaceholderSlotRepository 状态机 CAS 与级联清理
- 原 test_placeholder_backfill: 重试回填、占位槽翻转与两阶段编辑"""

import asyncio
import io
import os
import sqlite3
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.config import settings
from app.core.pipeline import placeholder as ph
from app.core.pipeline.engines import LocalDiskTransportEngine
from app.core.pipeline.filtering import BatchFilter
from app.core.pipeline.interfaces import ItemSource
from app.core.pipeline.models import DedupQuery, PublishBatch, PublishContext, PublishItem
from app.core.pipeline.pipeline import PublishPipeline
from app.core.pipeline.placeholder import PlaceholderCardSender, PlaceholderSlotCoordinator, summarize_failure_reason
from app.core.pipeline.runtime import BatchState, Counters
from app.core.scheduler import RetryableTaskError, UploadCancelledError, UploadPausedError
from app.db.publish_repository import PlaceholderSlotRepository as Slots
from app.db.task_repository import TaskQueueRepository
from app.services import publish_runner as pub_exec
from app.services.publisher_service import PublishFolderResult


# ==============================================================================
# Part: test_issue4_placeholder_idempotency.py
# ==============================================================================
@pytest.mark.asyncio
async def test_handle_oversize_sends_card_once_and_skips_on_second():
    mock_source = MagicMock()
    batch_filter = BatchFilter(mock_source)

    mock_client = MagicMock()
    mock_client.send_message = AsyncMock()

    context = PublishContext(
        task_id="task_test_placeholder",
        target_chat_id="-1009000000010",
        userbot_client=mock_client,
        send_placeholder_card=True,
        is_premium=False,
    )

    item = PublishItem(
        item_id="huge_video.mkv",
        name="huge_video.mkv",
        size_bytes=3 * 1024 * 1024 * 1024,  # 3.00 GiB (3072.00 MiB) > 2000 MiB (1.95 GiB) (non-premium)
    )

    b = BatchState()
    counters = Counters(total_files=1)
    all_skipped = []

    with patch("app.core.pipeline.filtering.submit_db_write") as mock_db_write:
        limit_bytes = 2000 * 1024 * 1024
        # First encounter: should send card
        ret1 = await batch_filter.handle_oversize(
            item, context, limit_bytes, is_pikpak=False, counters=counters, b=b, all_skipped=all_skipped
        )
        assert ret1 is True
        assert mock_client.send_message.call_count == 1
        assert "huge_video.mkv:non_premium_limit" in context.sent_placeholder_keys
        assert mock_db_write.called

        # Second encounter (e.g. On task retry/resume): should skip sending card
        ret2 = await batch_filter.handle_oversize(
            item, context, limit_bytes, is_pikpak=False, counters=counters, b=b, all_skipped=all_skipped
        )
        assert ret2 is True
        # Call count remains 1!
        assert mock_client.send_message.call_count == 1


# ==============================================================================
# Part: test_placeholder_cards.py
# ==============================================================================
TV_DIR = "/tv"


def _ctx(**kw):
    kw.setdefault("delay_seconds", 0.0)
    kw.setdefault("userbot_client", AsyncMock())
    return PublishContext(task_id="ph_t", target_chat_id=-100100, **kw)


def _item(name="e01.mkv", size=1000, path=None):
    return PublishItem(item_id=path or os.path.join(TV_DIR, name), name=name, size_bytes=size, source_type="local")


def _slot_of(name, size=1000):
    return Slots.get_by_identity("-100100", os.path.join(TV_DIR, name), size)


def _sent(msg_id=111, with_photo=True):
    m = MagicMock(id=msg_id)
    if with_photo:
        m.photo = MagicMock(name="input_photo")
    return m


# ---------------------------------------------------------------------------
# D5 Photo 形态 + 槽落库
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_failure_card_photo_form_and_slot():
    """失败卡 = Photo 媒体形态 + backfillable 槽落库。

    海报字节必须包装为带 .name 的命名流: Telethon 对裸 bytes 无文件名可判型 →
    is_image 恒 False → 以无名文档形态上传, Photo 卡静默失效 (live E2E 包 5 实证
    的真缺陷, 本断言即回归锁)。"""
    ctx = _ctx()
    ctx.userbot_client.send_file.return_value = _sent(111)
    sender = PlaceholderCardSender()

    await sender.send_failure_card(_item(), ctx, "连接超时 TimeoutError")

    assert ctx.userbot_client.send_file.await_count == 1
    payload = ctx.userbot_client.send_file.await_args.args[1]
    assert isinstance(payload, io.BytesIO) and payload.name == "placeholder_poster.jpg", (
        "海报字节须包装命名流 (裸 bytes 会被 Telethon 判为无名文档, Photo 形态失效)"
    )
    assert payload.getbuffer().nbytes > 0
    kwargs = ctx.userbot_client.send_file.await_args.kwargs
    assert kwargs.get("force_document") is False
    assert "补发排队" in kwargs.get("caption", "")
    slot = _slot_of("e01.mkv")
    assert slot is not None
    assert slot["slot_kind"] == "backfillable" and slot["state"] == "placeholder_sent"
    assert slot["placeholder_msg_id"] == 111
    assert slot["client_label"] == "Userbot"


# ---------------------------------------------------------------------------
# J6 槽复用 (跨运行防刷屏)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_slot_reuse_no_second_card():
    """同身份已有槽 → 不发新卡仅 fail_count 原子递增 (J6)"""
    ctx = _ctx()
    ctx.userbot_client.send_file.return_value = _sent(111)
    sender = PlaceholderCardSender()
    await sender.send_failure_card(_item(), ctx, "第一次")
    await sender.send_failure_card(_item(), ctx, "第二次")  # 新 sender 模拟跨运行重试

    assert ctx.userbot_client.send_file.await_count == 1, "槽复用绝不发新卡"
    slot = _slot_of("e01.mkv")
    assert slot["fail_count"] == 1 and slot["fail_reason"] == "第二次"


# ---------------------------------------------------------------------------
# 封顶 10 逐条 + 1 溢出汇总
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cap_10_plus_1_overflow():
    """D6: 前 10 张逐条 Photo 卡; 第 11 项触发 1 张溢出文本卡 (无槽终态); 后续仅计数"""
    ctx = _ctx()
    ctx.userbot_client.send_file.return_value = _sent(111)
    ctx.userbot_client.send_message.return_value = MagicMock(id=999)
    sender = PlaceholderCardSender()

    for i in range(10):
        await sender.send_failure_card(_item(f"f{i:02d}.mkv"), ctx, "x")
    assert ctx.userbot_client.send_file.await_count == 10

    await sender.send_failure_card(_item("f10.mkv"), ctx, "x")
    assert ctx.userbot_client.send_message.await_count == 1, "溢出发 1 张文本汇总卡"
    overflow_text = ctx.userbot_client.send_message.await_args.args[1]
    assert "超出单批逐条展示上限" in overflow_text and "批量异常提示" in overflow_text

    await sender.send_failure_card(_item("f11.mkv"), ctx, "x")
    assert ctx.userbot_client.send_message.await_count == 1, "溢出卡仅 1 张, 后续仅计数"
    assert ctx.userbot_client.send_file.await_count == 10

    rows = Slots.list_by_task("ph_t")
    assert len(rows) == 10, "溢出文件无槽 (无卡可回填, 重试走常规发送)"
    assert _slot_of("f10.mkv") is None


# ---------------------------------------------------------------------------
# R7 节流 + 开关/客户端守卫
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_card_send_sleeps_delay_each_time(monkeypatch):
    """R7: 每张卡发送前必须等待完整 delay_seconds, 严禁并发连发"""
    sleeps = []

    async def fake_sleep(_context, d):
        sleeps.append(d)

    monkeypatch.setattr(ph, "card_throttle_wait", fake_sleep)
    ctx = _ctx(delay_seconds=1.5)
    ctx.userbot_client.send_file.return_value = _sent(111)
    sender = PlaceholderCardSender()
    await sender.send_failure_card(_item(), ctx, "x")
    await sender.send_failure_card(_item(name="e02.mkv"), ctx, "x")

    assert sleeps == [1.5, 1.5]


# ---------------------------------------------------------------------------
# 暂停排水: 剩余占位卡快速发完后再暂停 (取消不排水)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pause_drains_pending_cards_then_raises(monkeypatch):
    """暂停落在第 2/3 张卡节流中 → 进入排水模式, 剩余卡以地板间隔快速发完 (零丢失);
    context.pause_drain 置位, 电平信号由下一个常规检查点收口 (此处以 check_signals 验证)"""
    import app.core.pipeline.runtime as rt

    paused = {"on": False}
    clock = [0.0]

    async def fake_wait(context, d):
        if paused["on"]:
            raise UploadPausedError("paused")
        clock[0] += d

    async def tick(delay):
        clock[0] += delay

    monkeypatch.setattr(rt.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(rt.asyncio, "sleep", tick)

    monkeypatch.setattr(rt, "wait_with_signals", fake_wait)
    ctx = _ctx(delay_seconds=1.5)
    sent_at = []

    async def send_card(*args, **kwargs):
        sent_at.append(clock[0])
        return _sent(111)

    ctx.userbot_client.send_file.side_effect = send_card
    sender = PlaceholderCardSender()

    await sender.send_failure_card(_item("d01.mkv"), ctx, "x")  # 正常完整节流
    previous_card_time = clock[0]
    paused["on"] = True  # 用户在第 2 张卡的节流期间按下暂停
    await sender.send_failure_card(_item("d02.mkv"), ctx, "x")  # 等待触发暂停 → 排水, 卡照发
    assert sent_at[1] - previous_card_time >= rt.CARD_DRAIN_FLOOR
    await sender.send_failure_card(_item("d03.mkv"), ctx, "x")  # 排水零丢失

    assert ctx.userbot_client.send_file.await_count == 3, "排水模式剩余占位卡零丢失"
    assert ctx.pause_drain is True
    slot = _slot_of("d03.mkv")
    assert slot is not None and slot["state"] == "placeholder_sent"

    # 电平信号收口: 下一个常规检查点必然 raise (批次循环顶/批间节奏等待)
    ctx.is_paused = lambda: True
    with pytest.raises(UploadPausedError):
        rt.check_signals(ctx)


@pytest.mark.asyncio
async def test_cancel_not_drained_immediate(monkeypatch):
    """取消不排水: 卡节流中取消 → UploadCancelledError 立即传播 (不被吞), 队列剩余卡不发"""
    import app.core.pipeline.runtime as rt

    cancelled = {"on": False}

    async def fake_wait(context, d):
        if cancelled["on"]:
            raise UploadCancelledError("cancelled")

    monkeypatch.setattr(rt, "wait_with_signals", fake_wait)

    ctx = _ctx(delay_seconds=1.5)
    ctx.userbot_client.send_file.return_value = _sent(111)
    sender = PlaceholderCardSender()

    await sender.send_failure_card(_item("c01.mkv"), ctx, "x")
    cancelled["on"] = True
    with pytest.raises(UploadCancelledError):
        await sender.send_failure_card(_item("c02.mkv"), ctx, "x")

    assert ctx.userbot_client.send_file.await_count == 1, "取消后剩余占位卡不发"
    assert ctx.pause_drain is False, "取消不进入排水模式"


def test_card_drain_floor_contract():
    """排水地板间隔契约: 0.7s (用户裁决) — 快速发完但保留防刷屏节奏"""
    import app.core.pipeline.runtime as rt

    assert rt.CARD_DRAIN_FLOOR == 0.7


@pytest.mark.asyncio
async def test_oversize_card_pause_drain_sends_card_then_propagates(monkeypatch):
    """缺集说明卡 (filtering.py Codex P2 收口): 暂停在卡节流中触发 → 卡仍快速发出
    (card_throttle_wait 不抛), pause_drain 置位; 后续检查点收口 raise"""
    import app.core.pipeline.runtime as rt

    paused = {"on": True}  # 等待期间暂停已按下

    async def fake_wait(context, d):
        if paused["on"]:
            raise UploadPausedError("paused")

    monkeypatch.setattr(rt, "wait_with_signals", fake_wait)
    monkeypatch.setattr(rt, "CARD_DRAIN_FLOOR", 0.01)

    mock_source = MagicMock()
    batch_filter = BatchFilter(mock_source)
    mock_client = MagicMock()
    mock_client.send_message = AsyncMock()

    context = PublishContext(
        task_id="task_drain_oversize",
        target_chat_id="-1009000000010",
        userbot_client=mock_client,
        send_placeholder_card=True,
        is_premium=False,
    )
    item = PublishItem(
        item_id="huge_video.mkv",
        name="huge_video.mkv",
        size_bytes=3 * 1024 * 1024 * 1024,
    )
    b = BatchState()
    counters = Counters(total_files=1)

    with patch("app.core.pipeline.filtering.submit_db_write"):
        ret = await batch_filter.handle_oversize(
            item, context, 2000 * 1024 * 1024, is_pikpak=False, counters=counters, b=b, all_skipped=[]
        )

    assert ret is True
    assert mock_client.send_message.call_count == 1, "排水模式缺集说明卡仍发出 (剩余延迟跳过)"
    assert context.pause_drain is True

    # 调用方 (批次循环顶) 的下一个 check_signals 收口
    context.is_paused = lambda: True
    with pytest.raises(UploadPausedError):
        rt.check_signals(context)


@pytest.mark.asyncio
async def test_execute_paused_never_completes(monkeypatch):
    """最终批通知后暂停: UploadPausedError 传播 → 任务绝不能以 completed 假终态收尾
    (覆盖 _execute_impl 收尾保险 + 顶层 except 落日志链路)"""
    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=1001)
    pipeline = _pipeline([_bf_item("e01.mkv", 1234)])
    ctx = _bf_ctx(mock_client)
    paused = {"on": False}
    ctx.is_paused = lambda: paused["on"]

    async def pause_after_notification(*args):
        paused["on"] = True

    notification = AsyncMock(side_effect=pause_after_notification)
    monkeypatch.setattr(pipeline.notifier, "notify_batch", notification)
    _mock_ft(monkeypatch, [MagicMock(name="handle") for _ in range(8)])
    _mock_userbot(monkeypatch, mock_client)

    with pytest.raises(UploadPausedError):
        await pipeline.execute(ctx)
    notification.assert_awaited_once()


@pytest.mark.asyncio
async def test_switch_off_no_cards():
    """D5 总开关关闭 → 超限卡与失败卡全关"""
    ctx = _ctx(send_placeholder_card=False)
    sender = PlaceholderCardSender()
    await sender.send_failure_card(_item(), ctx, "x")
    assert ctx.userbot_client.send_file.await_count == 0
    assert ctx.userbot_client.send_message.await_count == 0
    assert Slots.list_by_task("ph_t") == []


@pytest.mark.asyncio
async def test_missing_userbot_no_cards():
    """userbot 缺位 → 降级不发卡, 永不致命"""
    ctx = _ctx(userbot_client=None)
    sender = PlaceholderCardSender()
    await sender.send_failure_card(_item(), ctx, "x")
    assert Slots.list_by_task("ph_t") == []


# ---------------------------------------------------------------------------
# 资产缺失降级 + InputPhoto 缓存
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_asset_falls_back_to_text(monkeypatch):
    """海报资产缺失 (否决自愈写回) → 降级纯文本卡, 槽照常落库"""
    monkeypatch.setattr(settings, "PLACEHOLDER_ASSET_PATH", "Z:/nonexistent/poster.jpg")
    monkeypatch.setattr(ph, "_POSTER_CACHE", None)
    ctx = _ctx()
    ctx.userbot_client.send_message.return_value = MagicMock(id=222)
    sender = PlaceholderCardSender()

    await sender.send_failure_card(_item(), ctx, "x")

    assert ctx.userbot_client.send_file.await_count == 0
    assert ctx.userbot_client.send_message.await_count == 1
    slot = _slot_of("e01.mkv")
    assert slot is not None and slot["placeholder_msg_id"] == 222


@pytest.mark.asyncio
async def test_inputphoto_handle_cache_reuse():
    """首卡上传后缓存 sent_msg.photo, 第 2 张卡零上传直接复用"""
    ctx = _ctx()
    photo1 = MagicMock(name="input_photo_1")
    sent1 = _sent(111)
    sent1.photo = photo1
    ctx.userbot_client.send_file.return_value = sent1
    sender = PlaceholderCardSender()

    await sender.send_failure_card(_item(), ctx, "x")
    await sender.send_failure_card(_item(name="e02.mkv"), ctx, "x")

    assert ctx.userbot_client.send_file.await_count == 2
    second_payload = ctx.userbot_client.send_file.await_args.args[1]
    assert second_payload is photo1, "第 2 张卡零上传, 直接复用 InputPhoto 句柄"


@pytest.mark.asyncio
async def test_inputphoto_cache_expiry_reuploads():
    """缓存句柄失效 (FILE_REFERENCE_EXPIRED) → 丢弃缓存用新鲜字节重传一次"""
    ctx = _ctx()
    ctx.userbot_client.send_file.side_effect = [
        _sent(111),
        Exception("FILE_REFERENCE_EXPIRED"),
        _sent(222),
    ]
    sender = PlaceholderCardSender()

    await sender.send_failure_card(_item(), ctx, "x")
    await sender.send_failure_card(_item(name="e02.mkv"), ctx, "x")

    assert ctx.userbot_client.send_file.await_count == 3, "卡1 + 卡2(缓存失败) + 卡2(重传)"
    slot = _slot_of("e02.mkv")
    assert slot is not None and slot["placeholder_msg_id"] == 222


# ---------------------------------------------------------------------------
# 文案时态锁 + 简明原因归类
# ---------------------------------------------------------------------------


def test_copy_tense_lock_oq2():
    """⏳ 卡文案诚实口径: 被动时态, 禁进行时承诺 (逐文件失败无任务级自动重试)"""
    assert "正在" not in ph.PENDING_CARD_TEXT, "⏳ 卡禁止'正在补发'类进行时承诺"
    assert "任务重试后" in ph.PENDING_CARD_TEXT
    assert "正在" not in ph.GROUP_PENDING_TEXT
    assert "已停止自动补发" in ph.TERMINAL_CARD_TEXT
    assert "已停止调度" in ph.GROUP_TERMINAL_TEXT


def test_reason_classifier():
    """{简明原因} 四类归类映射, 兜底'传输异常', 不得直出异常文本"""
    assert summarize_failure_reason("FileNotFoundError: no such file or directory") == "源文件缺失"
    assert summarize_failure_reason("ConnectionResetError by peer") == "远端不可达"
    assert summarize_failure_reason("ReadTimeout: request timed out") == "连接超时"
    assert summarize_failure_reason("文件体积超过上限 too large") == "体积超限"
    assert summarize_failure_reason("") == "传输异常"
    assert summarize_failure_reason("某未知内部错误") == "传输异常"


# ---------------------------------------------------------------------------
# D3/J2: 终态卡 (确认不可发布分类直接 ❌ 形态)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_terminal_card_form_failed_terminal_slot():
    """terminal=True → ❌ 形态发卡 + 槽直接落 failed_terminal (not_found 分类)"""
    ctx = _ctx()
    ctx.userbot_client.send_file.return_value = _sent(333)
    sender = PlaceholderCardSender()

    await sender.send_failure_card(_item(), ctx, "PikPak not found: file gone", terminal=True)

    caption = ctx.userbot_client.send_file.await_args.kwargs["caption"]
    assert "传输中断归档" in caption and "源文件缺失" in caption, "简明原因归类映射"
    assert _slot_of("e01.mkv")["state"] == "failed_terminal"


# ---------------------------------------------------------------------------
# D2/R1: 整组失败汇总卡 (文本形态) + group_summary 槽
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_group_summary_card_and_slots():
    """整组失败 → 1 张文本汇总卡; 每成员 group_summary 槽共享汇总卡 msg_id;
    已有槽的成员仅 fail_count++ (UNIQUE 身份键)"""
    ctx = _ctx()
    ctx.userbot_client.send_file.return_value = _sent(111)
    ctx.userbot_client.send_message.return_value = MagicMock(id=777)
    sender = PlaceholderCardSender()

    await sender.send_failure_card(_item(name="a.jpg", size=10), ctx, "x")  # 早前单文件失败
    members = [_item(name="a.jpg", size=10), _item(name="b.jpg", size=20), _item(name="c.jpg", size=30)]
    await sender.send_group_summary_card(members, ctx, "组发失败")

    assert ctx.userbot_client.send_message.await_count == 1
    text = ctx.userbot_client.send_message.await_args.args[1]
    assert "相册等待补发" in text and "3" in text
    a = _slot_of("a.jpg", size=10)
    assert a["slot_kind"] == "backfillable" and a["fail_count"] == 1, "已有槽复用, 不建共享槽"
    row_b = _slot_of("b.jpg", size=20)
    row_c = _slot_of("c.jpg", size=30)
    assert row_b["slot_kind"] == "group_summary" and row_b["placeholder_msg_id"] == 777
    assert row_c["slot_kind"] == "group_summary" and row_c["placeholder_msg_id"] == 777


# ---------------------------------------------------------------------------
# reject/audit 拦截零发卡 (业务判死 ≠ 传输异常; filter_batch 层验证)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reject_never_send_cards_oq3():
    """reject/audit 拦截仅落 failed/诊断表 —— 发卡器零调用、频道端零占位卡"""
    from app.core.pipeline.filtering import BatchFilter
    from app.core.pipeline.models import DedupQuery
    from app.core.pipeline.runtime import BatchState, Counters

    class _RejSource:
        def reject_item(self, item, context):
            return "非媒体文件"

        def audit_block_reason(self, item, context):
            return None

        def dedup_query(self, item, context):
            return DedupQuery(local_path=item.item_id, size_bytes=item.size_bytes)

        def oversize_limit_bytes(self, item, context):
            return None

    batch = PublishBatch(
        batch_id="b_rej",
        folder_name="rej",
        is_folder_publish=False,
        items=[_item(name="note.txt"), _item(name="clip.mp4")],
    )
    ctx = _ctx()
    b = BatchState()
    counters = Counters(2)
    filt = BatchFilter(_RejSource())

    publishable = await filt.filter_batch(batch, ctx, False, counters, b, [])

    assert publishable == [] and len(b.failed) == 2, "reject 条目计入失败"
    assert ctx.userbot_client.send_file.await_count == 0, "reject 绝不发卡"
    assert ctx.userbot_client.send_message.await_count == 0
    assert Slots.list_by_task("ph_t") == []


# ==============================================================================
# Part: test_placeholder_slots.py
# ==============================================================================
def _mk(task_id, chat="-100123", path="/tv/e01.mkv", size=1024, kind="backfillable", msg_id=55):
    return Slots.insert_new_slot(
        chat,
        path,
        size,
        slot_kind=kind,
        placeholder_msg_id=msg_id,
        task_id=task_id,
        client_label="Userbot",
        sender_account_id=1,
    )


def test_unique_identity_conflict():
    """单点防重: 同 (chat, path, size) 二次插入必须抛 UNIQUE (调用方转入槽复用路径)"""
    _mk("t1")
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
        _mk("t2")


def test_cas_three_state_and_slot_kind_filter():
    """R5+三态守卫 + J11 slot_kind 双过滤"""
    slot = _mk("t1")
    _mk("t1", path="/tv/album/a.jpg", size=10, kind="group_summary")

    # group_summary 行结构上禁入回填 (即使共享汇总卡 msg_id)
    assert not Slots.cas_acquire_edit("-100123", "/tv/album/a.jpg", 10)

    # placeholder_sent → 获取成功; editing 态独占互斥 (不可被并发二次认领)
    assert Slots.cas_acquire_edit("-100123", "/tv/e01.mkv", 1024)
    assert not Slots.cas_acquire_edit("-100123", "/tv/e01.mkv", 1024)
    row = Slots.get_by_identity("-100123", "/tv/e01.mkv", 1024)
    assert row["state"] == "editing" and row["edit_attempted_at"] > 0

    # published / lost 终态不可获取
    Slots.mark_published(slot, real_msg_id=999)
    assert not Slots.cas_acquire_edit("-100123", "/tv/e01.mkv", 1024)

    slot2 = _mk("t1", path="/tv/e02.mkv", size=2048)
    Slots.mark_lost(slot2, "占位卡被删")
    assert not Slots.cas_acquire_edit("-100123", "/tv/e02.mkv", 2048)


def test_cas_failed_terminal_revival_oq5():
    """failed_terminal 槽纳入守卫集 —— 手动重试可原地复活为 published"""
    slot = _mk("t1", path="/tv/e03.mkv", size=3072)
    Slots.mark_failed_terminal(slot, "重试耗尽")
    assert Slots.get_by_identity("-100123", "/tv/e03.mkv", 3072)["state"] == "failed_terminal"

    assert Slots.cas_acquire_edit("-100123", "/tv/e03.mkv", 3072)
    Slots.mark_published(slot, real_msg_id=1234)
    row = Slots.get_by_identity("-100123", "/tv/e03.mkv", 3072)
    assert row["state"] == "published" and row["real_msg_id"] == 1234


def test_increment_fail_atomic():
    """J6 槽复用: 原子递增失败计数, 跨运行累加"""
    slot = _mk("t1", path="/tv/e04.mkv", size=4096)
    for i in range(4):
        Slots.increment_fail(slot, f"第{i + 1}次失败")
    row = Slots.get_by_identity("-100123", "/tv/e04.mkv", 4096)
    assert row["fail_count"] == 4
    assert row["fail_reason"] == "第4次失败"


def test_revert_editing_to_sent():
    """R5 启动自愈: 仅 editing 回滚, 其余态不动"""
    _mk("t1", path="/tv/e05.mkv", size=5120)
    _mk("t1", path="/tv/e06.mkv", size=6144)
    Slots.cas_acquire_edit("-100123", "/tv/e05.mkv", 5120)

    changed = Slots.revert_editing_to_sent()

    assert changed >= 1
    assert Slots.get_by_identity("-100123", "/tv/e05.mkv", 5120)["state"] == "placeholder_sent"
    assert Slots.get_by_identity("-100123", "/tv/e06.mkv", 6144)["state"] == "placeholder_sent"


def test_states_for_identities():
    """诊断富化查询: 命中返回 state 映射, 未命中跳过"""
    _mk("t1")
    s2 = _mk("t1", path="/tv/e07.mkv", size=7168)
    Slots.mark_published(s2, real_msg_id=1)

    got = Slots.states_for_identities(
        "-100123",
        [("/tv/e01.mkv", 1024), ("/tv/e07.mkv", 7168), ("/tv/none.mkv", 1)],
    )
    assert got == {"/tv/e01.mkv|1024": "placeholder_sent", "/tv/e07.mkv|7168": "published"}


def test_mark_flipped_only_group_summary():
    """翻转仅对 group_summary 行生效 (backfillable 行无视)"""
    gs = _mk("t1", path="/tv/album/b.jpg", size=20, kind="group_summary")
    bf = _mk("t1", path="/tv/e08.mkv", size=8192)

    Slots.mark_flipped(gs)
    assert Slots.get_by_identity("-100123", "/tv/album/b.jpg", 20)["state"] == "flipped"

    Slots.mark_flipped(bf)
    assert Slots.get_by_identity("-100123", "/tv/e08.mkv", 8192)["state"] == "placeholder_sent"


def test_cascade_delete_exclusionary_oq7():
    """delete_task 级联 —— 清除一切终态 (published/lost/failed_terminal/flipped),
    保留可行动态 (placeholder_sent/editing 支持跨任务槽复用)"""
    _mk("t1")  # placeholder_sent → 保留
    _mk("t1", path="/tv/e09.mkv", size=9216)
    assert Slots.cas_acquire_edit("-100123", "/tv/e09.mkv", 9216)
    published = _mk("t1", path="/tv/e10.mkv", size=10240)
    Slots.mark_published(published, real_msg_id=5)
    lost = _mk("t1", path="/tv/e11.mkv", size=11264)
    Slots.mark_lost(lost, "x")
    terminal = _mk("t1", path="/tv/e12.mkv", size=12288)
    Slots.mark_failed_terminal(terminal, "y")
    flipped = _mk("t1", path="/tv/album/c.jpg", size=30, kind="group_summary")
    Slots.mark_flipped(flipped)

    TaskQueueRepository.create("t1", "publish_folder", {"target_channel": "-100123"})
    TaskQueueRepository.delete("t1")

    kept = Slots.list_by_task("t1")
    assert {r["state"] for r in kept} <= {"placeholder_sent", "editing"}
    assert {(r["file_path"], r["file_size"]) for r in kept} == {
        ("/tv/e01.mkv", 1024),
        ("/tv/e09.mkv", 9216),
    }


def test_cascade_clear_finished_oq7():
    """clear_finished 批量路径同样收口 (偿还历史旁路缺口), 其它任务不受影响"""
    _mk("tA")  # placeholder_sent → 保留
    p1 = _mk("tA", path="/tv/e20.mkv", size=20480)
    Slots.mark_published(p1, real_msg_id=9)
    _mk("tB", path="/tv/e30.mkv", size=30720)

    TaskQueueRepository.create("tA", "publish_folder", {"target_channel": "-100123"})
    TaskQueueRepository.create("tB", "publish_folder", {"target_channel": "-100123"})
    TaskQueueRepository.update_status("tA", "completed")
    TaskQueueRepository.clear_finished()

    kept_a = Slots.list_by_task("tA")
    assert len(kept_a) == 1 and kept_a[0]["state"] == "placeholder_sent"
    assert len(Slots.list_by_task("tB")) == 1


# ==============================================================================
# Part: test_placeholder_backfill.py
# ==============================================================================
TV_DIR = os.path.join("D:", os.sep, "bf")
CHAT = "-100900050"


def _slot_path(name):
    return os.path.join(TV_DIR, name)


def _bf_item(name, size):
    return PublishItem(item_id=_slot_path(name), name=name, size_bytes=size, source_type="local")


def _bf_slot_of(name, size):
    return Slots.get_by_identity(CHAT, _slot_path(name), size)


def _insert_slot(name, size, *, kind="backfillable", msg_id, task_id="bf_t", fail_reason="旧失败", fail_count=0):
    """造数: 插入占位槽; 返回 slot_id (可选后续状态迁移)"""
    slot_id = Slots.insert_new_slot(
        CHAT,
        _slot_path(name),
        size,
        slot_kind=kind,
        placeholder_msg_id=msg_id,
        task_id=task_id,
        fail_reason=fail_reason,
        client_label="Userbot",
        sender_account_id=1,
    )
    for _ in range(fail_count):
        Slots.increment_fail(slot_id, fail_reason)
    return slot_id


class _Src(ItemSource):
    def __init__(self, batches):
        self.batches = batches

    async def get_batches(self, context):
        return self.batches


class _Clean:
    async def cleanup(self, items, context):
        return ""


class _Notif:
    async def notify_batch(self, batch, context, result):
        return None


def _pipeline(items):
    batch = PublishBatch(batch_id="b_bf", folder_name="bf", is_folder_publish=False, items=items)
    return PublishPipeline(_Src([batch]), LocalDiskTransportEngine(), _Clean(), _Notif())


def _bf_ctx(mock_client, task_id="bf_t"):
    return PublishContext(
        task_id=task_id,
        target_chat_id=-100900050,
        delay_seconds=0.0,
        album_mode=True,
        userbot_client=mock_client,
    )


def _mock_userbot(monkeypatch, mock_client):
    """preflight 无账号路径: context.userbot_client 回落 client_manager.userbot"""
    from types import SimpleNamespace

    import app.core.client_manager as cm_module

    if hasattr(mock_client, "is_connected") and isinstance(mock_client.is_connected, AsyncMock):
        mock_client.is_connected = MagicMock(return_value=True)
    if (
        hasattr(mock_client, "get_me")
        and isinstance(mock_client.get_me, AsyncMock)
        and not mock_client.get_me.return_value
    ):
        mock_client.get_me.return_value = SimpleNamespace(premium=False, id=1, username="userbot")

    monkeypatch.setattr(cm_module.client_manager, "userbot", mock_client)


def _mock_ft(monkeypatch, side_effect):
    """替换 upload_file_parallel (引擎函数级 import 于调用时解析, setattr 模块属性即生效)"""
    import app.core.fast_telethon as ft_module

    fake = AsyncMock(side_effect=side_effect)
    monkeypatch.setattr(ft_module, "upload_file_parallel", fake)
    return fake


# ---------------------------------------------------------------------------
# 原地回填: 两段式 F1 回归锁 + 成功路径
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backfill_two_stage_edit_and_slot_published(monkeypatch):
    """命中槽 + CAS → edit_item: edit_message 必须收已上传 handle (严禁路径直传, F1);
    attributes 经共享构建器 (视频保持流媒体播放器, F3); 槽 published + 正常入账。"""
    _insert_slot("e01.mkv", 1234, msg_id=55)
    mock_client = AsyncMock()
    handle = MagicMock(name="uploaded_handle")
    mock_client.edit_message.return_value = MagicMock(id=888)
    pipeline = _pipeline([_bf_item("e01.mkv", 1234)])
    ctx = _bf_ctx(mock_client)
    mock_ft = _mock_ft(monkeypatch, [handle for _ in range(8)])
    _mock_userbot(monkeypatch, mock_client)

    res = await pipeline.execute(ctx)

    assert res.published_count == 1
    edit_kwargs = mock_client.edit_message.await_args.kwargs
    edit_args = mock_client.edit_message.await_args.args
    assert edit_kwargs.get("file") is handle, "F1 回归锁: 必须收已上传 handle"
    assert not isinstance(edit_kwargs.get("file"), str), "严禁 file= 传本地路径 (丢断点续传)"
    assert edit_kwargs.get("text") == "e01.mkv", "caption 基线 = 文件名 (F3)"
    assert edit_kwargs.get("attributes"), "视频回填必须带 DocumentAttributeVideo (F3)"
    assert edit_kwargs.get("supports_streaming") is True
    assert edit_args[1] == 55, "编辑对象 = 占位卡 msg_id"
    assert mock_ft.await_count == 1, "回填上传恰好一次 (断点续传)"
    slot = _bf_slot_of("e01.mkv", 1234)
    assert slot["state"] == "published" and slot["real_msg_id"] == 888


@pytest.mark.asyncio
async def test_backfill_no_slot_normal_send(monkeypatch):
    """无槽 → 常规发送零额外副作用 (不触碰槽表, 不触发原位编辑)"""
    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=1001)
    pipeline = _pipeline([_bf_item("e01.mkv", 1234)])
    ctx = _bf_ctx(mock_client)
    _mock_ft(monkeypatch, [MagicMock(name="handle") for _ in range(8)])
    _mock_userbot(monkeypatch, mock_client)

    res = await pipeline.execute(ctx)

    assert res.published_count == 1
    assert _bf_slot_of("e01.mkv", 1234) is None
    mock_client.edit_message.assert_not_awaited()


# ---------------------------------------------------------------------------
# 编辑失败矩阵 (§4.2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backfill_edit_fail_deleted_fallback_published(monkeypatch):
    """卡被删 → lost + fallback 常规发送; 发送成功 → published (real_msg_id=新消息)"""
    _insert_slot("del.mkv", 777, msg_id=66)
    mock_client = AsyncMock()
    mock_client.edit_message.side_effect = Exception("MESSAGE_ID_INVALID: the message was deleted")
    mock_client.send_file.return_value = MagicMock(id=7771)
    pipeline = _pipeline([_bf_item("del.mkv", 777)])
    ctx = _bf_ctx(mock_client)
    _mock_ft(monkeypatch, [MagicMock(name="handle") for _ in range(8)])
    _mock_userbot(monkeypatch, mock_client)

    res = await pipeline.execute(ctx)

    assert res.published_count == 1, "回退常规发送成功"
    slot = _bf_slot_of("del.mkv", 777)
    assert slot["state"] == "published" and slot["real_msg_id"] == 7771


@pytest.mark.asyncio
async def test_backfill_edit_fail_window_expired_fallback_published(monkeypatch):
    """超 48h 编辑窗口 → 优雅降级: 常规发送成功后槽 published (⏳ 卡保留为历史标记)"""
    _insert_slot("win.mkv", 778, msg_id=67)
    mock_client = AsyncMock()
    mock_client.edit_message.side_effect = Exception("MESSAGE_EDIT_TIME_EXPIRED: too old")
    mock_client.send_file.return_value = MagicMock(id=7781)
    pipeline = _pipeline([_bf_item("win.mkv", 778)])
    ctx = _bf_ctx(mock_client)
    _mock_ft(monkeypatch, [MagicMock(name="handle") for _ in range(8)])
    _mock_userbot(monkeypatch, mock_client)

    res = await pipeline.execute(ctx)

    assert res.published_count == 1
    slot = _bf_slot_of("win.mkv", 778)
    assert slot["state"] == "published" and slot["real_msg_id"] == 7781


@pytest.mark.asyncio
async def test_backfill_edit_fail_flood_wait_raises_retryable(monkeypatch):
    """FloodWait → 复位 placeholder_sent + RetryableTaskError 上抛 (下轮重试可重获取占位槽)"""
    _insert_slot("flood.mkv", 779, msg_id=68)
    mock_client = AsyncMock()
    mock_client.edit_message.side_effect = Exception("FLOOD_WAIT_5: please wait")
    pipeline = _pipeline([_bf_item("flood.mkv", 779)])
    ctx = _bf_ctx(mock_client)
    _mock_ft(monkeypatch, [MagicMock(name="handle") for _ in range(8)])
    _mock_userbot(monkeypatch, mock_client)

    with pytest.raises(RetryableTaskError):
        await pipeline.execute(ctx)

    assert _bf_slot_of("flood.mkv", 779)["state"] == "placeholder_sent", "FloodWait 须释放 editing 锁供下轮重试"

    # 下轮重试应能顺利重获取并原地回填成功
    mock_client.edit_message.side_effect = None
    mock_client.edit_message.return_value = MagicMock(id=68)
    res = await pipeline.execute(_bf_ctx(mock_client))
    assert res.published_count == 1
    assert _bf_slot_of("flood.mkv", 779)["state"] == "published"


@pytest.mark.asyncio
async def test_backfill_claim_conflict_state_routing(monkeypatch):
    """CAS 抢占失败按槽现态分流: editing→无预算Retryable(15s), published→校验绑定防重, force/变更内容→补发, lost→补发"""
    from app.db.publish_repository import PublishHistoryRepository

    # 1. editing: 抛 RetryableTaskError(retry_after=15.0, count_retry=False, is_flood_wait=False) 且不触发 send_file
    _insert_slot("busy.mkv", 781, msg_id=71)
    assert Slots.cas_acquire_edit(CHAT, _slot_path("busy.mkv"), 781)
    mock_client = AsyncMock()
    _mock_ft(monkeypatch, [MagicMock(name="handle") for _ in range(8)])
    _mock_userbot(monkeypatch, mock_client)

    with pytest.raises(RetryableTaskError) as exc_info:
        await _pipeline([_bf_item("busy.mkv", 781)]).execute(_bf_ctx(mock_client))
    assert exc_info.value.retry_after == 15.0
    assert exc_info.value.count_retry is False
    assert exc_info.value.is_flood_wait is False
    mock_client.send_file.assert_not_awaited()
    mock_client.edit_message.assert_not_awaited()

    # 2. published (已绑定同一内容记录且非 force_republish): 短路跳过防重，不触发 send_file
    pub_slot = _insert_slot("done.mkv", 782, msg_id=72)
    Slots.mark_published(pub_slot, real_msg_id=720)
    PublishHistoryRepository.record(CHAT, _slot_path("done.mkv"), 782, file_hash="same_hash", tg_message_id=720)
    monkeypatch.setattr(
        _Src,
        "dedup_query",
        lambda self, item, ctx: DedupQuery(local_path=item.item_id, size_bytes=item.size_bytes, file_hash="same_hash"),
    )
    ctx_no_filter = _bf_ctx(mock_client)
    res_pub = await _pipeline([_bf_item("done.mkv", 782)]).execute(ctx_no_filter)
    # BatchFilter 会先防重跳过; 显式测试 _try_backfill 竞态命中 published 的兜底分支:
    res_try = await _pipeline([_bf_item("done.mkv", 782)])._try_backfill(_bf_item("done.mkv", 782), ctx_no_filter)
    assert (res_pub.published_count, res_pub.newly_published_count) == (1, 0)
    assert res_try == ("already_published", pub_slot, 720)
    mock_client.send_file.assert_not_awaited()

    # 2b. published 但 force_republish=True: 不得被 published 槽拦截，照常走 send_item
    ctx_force = _bf_ctx(mock_client)
    ctx_force.force_republish = True
    mock_client.send_file.return_value = MagicMock(id=721)
    res_force = await _pipeline([_bf_item("done.mkv", 782)]).execute(ctx_force)
    assert res_force.published_count == 1
    assert res_force.newly_published_count == 1
    mock_client.send_file.assert_awaited_once()
    mock_client.send_file.reset_mock()

    # 2c. published 但同路径同大小文件内容哈希变更: 不得被旧槽拦截，照常走 send_item
    hash_slot = _insert_slot("rehashed.mkv", 784, msg_id=74)
    Slots.mark_published(hash_slot, real_msg_id=740)
    PublishHistoryRepository.record(CHAT, _slot_path("rehashed.mkv"), 784, file_hash="old_hash_1", tg_message_id=740)
    pipe_hash = _pipeline([_bf_item("rehashed.mkv", 784)])
    monkeypatch.setattr(
        pipe_hash._batch_filter.source,
        "dedup_query",
        lambda item, ctx: type(
            "DQ",
            (),
            {
                "local_path": item.item_id,
                "size_bytes": item.size_bytes,
                "file_hash": "new_hash_2",
                "source_type": "local",
                "remote_file_id": "",
            },
        )(),
    )
    mock_client.send_file.return_value = MagicMock(id=741)
    res_hash = await pipe_hash.execute(_bf_ctx(mock_client))
    assert res_hash.published_count == 1
    assert res_hash.newly_published_count == 1
    mock_client.send_file.assert_awaited_once()
    mock_client.send_file.reset_mock()

    # 3. lost: 原占位卡已删，照常走 send_item 补发
    lost_slot = _insert_slot("gone.mkv", 783, msg_id=73)
    Slots.mark_lost(lost_slot, "deleted")
    mock_client.send_file.return_value = MagicMock(id=730)
    res_lost = await _pipeline([_bf_item("gone.mkv", 783)]).execute(_bf_ctx(mock_client))
    assert res_lost.published_count == 1
    assert res_lost.newly_published_count == 1
    mock_client.send_file.assert_awaited_once()


@pytest.mark.asyncio
async def test_window_fallback_send_cancelled_error_resets_slot(monkeypatch):
    """window 回退在 send_item 期间遇 asyncio.CancelledError 须复位 placeholder_sent，防卡死在 editing."""
    _insert_slot("cancel_win.mkv", 785, msg_id=75)
    mock_client = AsyncMock()
    mock_client.edit_message.side_effect = Exception("MESSAGE_EDIT_TIME_EXPIRED")
    mock_client.send_file.side_effect = asyncio.CancelledError()
    _mock_ft(monkeypatch, [MagicMock(name="handle") for _ in range(8)])
    _mock_userbot(monkeypatch, mock_client)

    with pytest.raises(asyncio.CancelledError):
        await _pipeline([_bf_item("cancel_win.mkv", 785)])._transport_single(
            _bf_item("cancel_win.mkv", 785), _bf_ctx(mock_client), Counters(1), BatchState(), is_pikpak=False
        )

    assert _bf_slot_of("cancel_win.mkv", 785)["state"] == "placeholder_sent"


@pytest.mark.asyncio
async def test_backfill_edit_fail_other_resets_then_fallback_published(monkeypatch):
    """其余回填失败 → 槽复位 placeholder_sent + 回退常规发送成功后 published"""
    _insert_slot("other.mkv", 880, msg_id=69)
    mock_client = AsyncMock()
    mock_client.edit_message.side_effect = RuntimeError("upload boom")
    mock_client.send_file.return_value = MagicMock(id=8801)
    pipeline = _pipeline([_bf_item("other.mkv", 880)])
    ctx = _bf_ctx(mock_client)
    _mock_ft(monkeypatch, [MagicMock(name="handle") for _ in range(8)])
    _mock_userbot(monkeypatch, mock_client)

    res = await pipeline.execute(ctx)

    assert res.published_count == 1
    slot = _bf_slot_of("other.mkv", 880)
    assert slot["state"] == "published" and slot["real_msg_id"] == 8801


@pytest.mark.asyncio
async def test_backfill_edit_fail_deleted_and_fallback_send_fails_marks_lost(monkeypatch):
    """卡被删 + 回退发送也失败 → 槽 lost + 常规失败归因 (占位卡复用路径发卡)"""
    _insert_slot("lost.mkv", 881, msg_id=70)
    mock_client = AsyncMock()
    mock_client.edit_message.side_effect = Exception("MESSAGE_ID_INVALID: gone")
    mock_client.send_file.side_effect = RuntimeError("send also boom")
    pipeline = _pipeline([_bf_item("lost.mkv", 881)])
    ctx = _bf_ctx(mock_client)
    _mock_ft(monkeypatch, [MagicMock(name="handle") for _ in range(8)])
    _mock_userbot(monkeypatch, mock_client)

    res = await pipeline.execute(ctx)

    assert len(res.failed_files) == 1
    assert _bf_slot_of("lost.mkv", 881)["state"] == "lost", "卡不存在且发送失败 → 无可回填目标"


# ---------------------------------------------------------------------------
# PikPak 引擎 edit_item 多分卷门槛 (属性统一计划包2: 门槛谓词与 send_item 对齐)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pikpak_edit_item_multi_part_still_rejected(monkeypatch):
    """size 超阈值 → split 产出多卷 → 仍拒原地回填 (多分卷从未走过 edit 路径,
    本用例补锁该语义, 防门槛放宽回归)"""
    from app.core.pipeline.engines import PikPakStreamTransportEngine

    engine = PikPakStreamTransportEngine(link_provider=AsyncMock())
    item = PublishItem(item_id="fid_big", name="big.mp4", size_bytes=1500, source_type="pikpak")
    ctx = PublishContext(
        task_id="gate_t", target_chat_id=-100900050, video_split_threshold_bytes=1000, userbot_client=AsyncMock()
    )
    _mock_ft(monkeypatch, [])  # 不应被触达

    with pytest.raises(RuntimeError, match="多分卷条目不支持原地回填"):
        await engine.edit_item(item, ctx, msg_id=91)


@pytest.mark.asyncio
async def test_pikpak_edit_item_size_equal_threshold_single_part_allowed(monkeypatch):
    """size 恰等于阈值 → split 单卷 → 不再误拒 (旧 size >= threshold 边界), 原地回填放行,
    attributes 单元素 Video 且 duration == item.duration_s (单卷整片语义)"""
    from app.core.pipeline.engines import PikPakStreamTransportEngine

    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/edge.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)
    item = PublishItem(item_id="fid_edge", name="edge.mp4", size_bytes=1000, duration_s=40.04, source_type="pikpak")
    mock_client = AsyncMock()
    mock_client.edit_message.return_value = MagicMock(id=92)
    ctx = PublishContext(
        task_id="gate_t", target_chat_id=-100900050, video_split_threshold_bytes=1000, userbot_client=mock_client
    )
    handle = MagicMock(name="uploaded_handle")
    _mock_ft(monkeypatch, [handle])

    outcome = await engine.edit_item(item, ctx, msg_id=55)

    assert outcome.message_ids == [92]
    edit_kwargs = mock_client.edit_message.await_args.kwargs
    assert edit_kwargs.get("file") is handle, "F1 两段式: 必须收已上传 handle"
    attrs = edit_kwargs.get("attributes")
    assert attrs and len(attrs) == 1
    assert type(attrs[0]).__name__ == "DocumentAttributeVideo"
    assert attrs[0].duration == 40, "单分卷可达域: duration = 整片时长 = 唯一消息真实时长"
    assert attrs[0].supports_streaming is True


@pytest.mark.asyncio
async def test_pikpak_edit_item_single_part_below_threshold_semantics(monkeypatch):
    """常规单卷域 (size < threshold) 回归锁: 整片时长语义 + supports_streaming 保持"""
    from app.core.pipeline.engines import PikPakStreamTransportEngine

    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value=["https://cdn.example.com/ok.mp4"]))
    item = PublishItem(
        item_id="fid_ok", name="ok.mp4", size_bytes=500, duration_s=68.5, width=1920, height=1080, source_type="pikpak"
    )
    mock_client = AsyncMock()
    mock_client.edit_message.return_value = MagicMock(id=93)
    ctx = PublishContext(
        task_id="gate_t", target_chat_id=-100900050, video_split_threshold_bytes=1000, userbot_client=mock_client
    )
    _mock_ft(monkeypatch, [MagicMock(name="handle") for _ in range(8)])

    await engine.edit_item(item, ctx, msg_id=56)

    attrs = mock_client.edit_message.await_args.kwargs.get("attributes")
    assert attrs and attrs[0].duration == 68
    assert attrs[0].w == 1920 and attrs[0].h == 1080


# ---------------------------------------------------------------------------
# finalize_terminal / 启动自愈 (R5) / 组发翻转 (R2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_finalize_terminal_threshold_and_group_summary(monkeypatch):
    """J2 阈值 (fail_count ≥ 1+max_retries) + group_summary 一并 ❌ 终态;
    未达阈值与已终态槽不动; 严禁删除消息 (仅 caption 编辑, 简明原因归类)"""
    _insert_slot("t4.mkv", 11, msg_id=11, task_id="fin_t", fail_count=4)  # 达阈值 → 终态
    _insert_slot("t1.mkv", 12, msg_id=12, task_id="fin_t", fail_count=1)  # 未达阈值 → 不动
    _insert_slot("t9.mkv", 13, msg_id=13, task_id="fin_t")  # 已终态 → 不动
    Slots.mark_published(_bf_slot_of("t9.mkv", 13)["id"], 1)
    _insert_slot("gs.jpg", 20, msg_id=14, task_id="fin_t", kind="group_summary")  # → 终态
    mock_userbot = AsyncMock()
    import app.core.client_manager as cm_module

    monkeypatch.setattr(cm_module.client_manager, "userbot", mock_userbot)

    # The persisted sender is account 1; terminal edits must resolve that account.
    monkeypatch.setattr(cm_module.client_manager, "get_account", lambda account_id=None: MagicMock(client=mock_userbot))

    coordinator = PlaceholderSlotCoordinator(max_retries=3)
    finalized = await coordinator.finalize_terminal("fin_t")

    assert finalized == 2
    assert _bf_slot_of("t4.mkv", 11)["state"] == "failed_terminal"
    assert _bf_slot_of("t1.mkv", 12)["state"] == "placeholder_sent"
    assert _bf_slot_of("t9.mkv", 13)["state"] == "published"
    assert _bf_slot_of("gs.jpg", 20)["state"] == "failed_terminal"
    texts = [c.kwargs.get("text", "") for c in mock_userbot.edit_message.await_args_list]
    assert any("传输中断归档" in t for t in texts), "1:1 终态文案 (简明原因归类)"
    assert any("相册补发终止" in t for t in texts), "汇总卡终态文案"
    mock_userbot.delete_messages.assert_not_called() if hasattr(mock_userbot, "delete_messages") else None


@pytest.mark.asyncio
async def test_finalize_terminal_without_userbot_still_flips_state(monkeypatch):
    """userbot 缺位 → 跳过 caption 编辑但 DB 状态照常翻转 (恢复后下次编辑补上)"""
    _insert_slot("noc.mkv", 15, msg_id=16, task_id="noc_t", fail_count=4)
    import app.core.client_manager as cm_module

    monkeypatch.setattr(cm_module.client_manager, "userbot", None)

    coordinator = PlaceholderSlotCoordinator(max_retries=3)
    finalized = await coordinator.finalize_terminal("noc_t")

    assert finalized == 1
    assert _bf_slot_of("noc.mkv", 15)["state"] == "failed_terminal"


def test_heal_editing_startup_r5():
    """R5: editing 孤儿槽 → placeholder_sent (启动自愈, 同步)"""
    _insert_slot("h1.mkv", 31, msg_id=15)
    Slots.cas_acquire_edit(CHAT, _slot_path("h1.mkv"), 31)

    coordinator = PlaceholderSlotCoordinator()
    changed = coordinator.heal_editing()

    assert changed >= 1
    assert _bf_slot_of("h1.mkv", 31)["state"] == "placeholder_sent"


@pytest.mark.asyncio
async def test_flip_after_group_send_r2(monkeypatch):
    """R2/组发成功 → 命中槽卡 best-effort 翻转 ✅; backfillable → published,
    group_summary → flipped; 无槽成员跳过; failed_terminal 卡也可复活"""
    _insert_slot("f1.jpg", 41, msg_id=21, task_id="flip_t")
    _insert_slot("f2.jpg", 42, msg_id=22, task_id="flip_t", kind="group_summary")
    _insert_slot("f3.jpg", 43, msg_id=23, task_id="flip_t")
    Slots.mark_failed_terminal(_bf_slot_of("f3.jpg", 43)["id"], "x")
    mock_client = AsyncMock()
    ctx = _bf_ctx(mock_client, task_id="flip_t")

    members = [
        (_bf_item("f1.jpg", 41), 900),
        (_bf_item("f2.jpg", 42), 901),
        (_bf_item("f3.jpg", 43), 902),
        (_bf_item("f4.jpg", 44), 903),  # 无槽 → 跳过
    ]
    coordinator = PlaceholderSlotCoordinator()
    flipped = await coordinator.flip_after_group_send(members, ctx)

    assert flipped == 3
    assert _bf_slot_of("f1.jpg", 41)["state"] == "published"
    assert _bf_slot_of("f2.jpg", 42)["state"] == "flipped"
    assert _bf_slot_of("f3.jpg", 43)["state"] == "published"
    assert _bf_slot_of("f4.jpg", 44) is None
    texts = [c.kwargs.get("text", "") for c in mock_client.edit_message.await_args_list]
    assert all("已补发" in t for t in texts)


@pytest.mark.asyncio
async def test_group_send_success_flips_slots_in_pipeline(monkeypatch):
    """管线级集成: 相册组发成功 → 编排层触发翻转 (含 group_summary 槽)"""
    _insert_slot("g1.jpg", 51, msg_id=31)
    _insert_slot("g2.jpg", 52, msg_id=32, kind="group_summary")
    mock_client = AsyncMock()
    mock_client.send_file.return_value = [MagicMock(id=910), MagicMock(id=911)]
    pipeline = _pipeline([_bf_item("g1.jpg", 51), _bf_item("g2.jpg", 52)])
    ctx = _bf_ctx(mock_client)
    _mock_ft(monkeypatch, [MagicMock(name="handle") for _ in range(8)])
    _mock_userbot(monkeypatch, mock_client)

    res = await pipeline.execute(ctx)

    assert res.published_count == 2
    assert _bf_slot_of("g1.jpg", 51)["state"] == "published"
    assert _bf_slot_of("g2.jpg", 52)["state"] == "flipped"


# ---------------------------------------------------------------------------
# 执行器: 终态化触发 + 诊断富化 (placeholder_state, 终态化后实况)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_executor_enriches_placeholder_state_and_finalizes(monkeypatch):
    """执行器置 failed 后: finalize → failed_files 信封富化 (占位状态反映终态化后实况)"""
    task_id = "ph_enrich"
    TaskQueueRepository.create(task_id, "publish_folder", {"target_channel": CHAT})
    _insert_slot("enrich.mkv", 61, msg_id=41, task_id=task_id)  # placeholder_sent → 富化
    _insert_slot("term.mkv", 62, msg_id=42, task_id=task_id, fail_count=4)  # 达阈值 → 终态化

    details = [
        {
            "file_name": "enrich.mkv",
            "reason": "boom",
            "client_label": "Userbot",
            "file_path": _slot_path("enrich.mkv"),
            "file_size": 61,
        },
        {
            "file_name": "term.mkv",
            "reason": "boom",
            "client_label": "Userbot",
            "file_path": _slot_path("term.mkv"),
            "file_size": 62,
        },
    ]
    res_mock = PublishFolderResult(0, 2, ["enrich.mkv: boom", "term.mkv: boom"])
    res_mock.failed_details = details

    async def fake_publish_folder(**kwargs):
        return res_mock

    monkeypatch.setattr(pub_exec.PublisherService, "publish_folder", staticmethod(fake_publish_folder))
    await pub_exec.run_publish_folder_task(task_id, _slot_path("fake"), CHAT, album_mode=False, delay_seconds=0.0)

    rec = TaskQueueRepository.get_by_id(task_id)
    envelope = rec["params"].get("failed_files")
    assert envelope is not None
    states = {d["file_name"]: d.get("placeholder_state") for d in envelope["items"]}
    assert states.get("enrich.mkv") == "placeholder_sent", "诊断富化: 占位状态列数据源"
    assert states.get("term.mkv") == "failed_terminal", "富化发生于终态化之后 (实况)"
    assert _bf_slot_of("term.mkv", 62)["state"] == "failed_terminal"


@pytest.mark.parametrize("draining", [False, True])
@pytest.mark.parametrize("cancel_at", [0.0, 0.25, 0.7])
async def test_cancel_during_card_drain_stops_remaining_cards(monkeypatch, draining, cancel_at):
    import app.core.pipeline.runtime as rt

    clock = [0.0]

    async def tick(delay):
        clock[0] += delay

    monkeypatch.setattr(rt.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(rt.asyncio, "sleep", tick)
    ctx = _ctx(delay_seconds=20.0)
    ctx.pause_drain = draining
    ctx.is_paused = lambda: True
    ctx.is_cancelled = lambda: clock[0] >= cancel_at
    sender = PlaceholderCardSender()

    async def send_remaining():
        for name in ("cancel01.mkv", "cancel02.mkv"):
            await sender.send_failure_card(_item(name), ctx, "x")

    with pytest.raises(UploadCancelledError):
        await send_remaining()
    assert clock[0] <= cancel_at + 0.25
    ctx.userbot_client.send_file.assert_not_awaited()
    ctx.userbot_client.send_message.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(("history_hash", "message_id"), [("", 900), ("same_hash", 901)])
async def test_published_slot_requires_content_and_message_binding(monkeypatch, history_hash, message_id):
    from app.db.publish_repository import PublishHistoryRepository

    slot_id = _insert_slot("binding.mkv", 789, msg_id=90)
    Slots.mark_published(slot_id, real_msg_id=900)
    PublishHistoryRepository.record(
        CHAT, _slot_path("binding.mkv"), 789, file_hash=history_hash, tg_message_id=message_id
    )
    item = _bf_item("binding.mkv", 789)
    pipeline = _pipeline([item])
    monkeypatch.setattr(
        pipeline._batch_filter.source,
        "dedup_query",
        lambda item, ctx: DedupQuery(local_path=item.item_id, size_bytes=item.size_bytes, file_hash="same_hash"),
    )
    assert await pipeline._try_backfill(item, _bf_ctx(AsyncMock())) is None


@pytest.mark.asyncio
async def test_window_fallback_archive_cancellation_keeps_slot_published(monkeypatch):
    _insert_slot("archive_cancel.mkv", 790, msg_id=91)
    client = AsyncMock()
    client.edit_message.side_effect = Exception("MESSAGE_EDIT_TIME_EXPIRED: too old")
    client.send_file.return_value = MagicMock(id=910)
    _mock_ft(monkeypatch, [MagicMock(name="handle") for _ in range(8)])
    _mock_userbot(monkeypatch, client)
    item = _bf_item("archive_cancel.mkv", 790)
    pipeline = _pipeline([item])

    async def cancel_archive(*args, **kwargs):
        slot = _bf_slot_of("archive_cancel.mkv", 790)
        assert slot["state"] == "published"
        assert slot["real_msg_id"] == 910
        raise asyncio.CancelledError()

    monkeypatch.setattr(pipeline, "_record_success", cancel_archive)
    with pytest.raises(asyncio.CancelledError):
        await pipeline._transport_single(item, _bf_ctx(client), Counters(1), BatchState(), is_pikpak=False)
    assert _bf_slot_of("archive_cancel.mkv", 790)["state"] == "published"


@pytest.mark.asyncio
async def test_oversize_cards_enforce_10_plus_1_cap_and_skip_failed_sends() -> None:
    """filter_batch 超限缺集说明卡遵循 10+1 封顶，失败发送不消耗额度."""
    from app.core.pipeline.placeholder import OVERFLOW_TEXT, PER_ITEM_CARD_CAP

    client = MagicMock()
    send_results: list[object] = [RuntimeError("transient"), *(MagicMock(id=i) for i in range(1, 20))]
    client.send_message = AsyncMock(side_effect=send_results)
    ctx = PublishContext(
        task_id="t_cap_ov",
        target_chat_id=-1001,
        userbot_client=client,
        send_placeholder_card=True,
        delay_seconds=0.0,
    )
    filt = BatchFilter(MagicMock())
    b = BatchState()
    counters = Counters(total_files=14)
    all_skipped: list = []

    with patch("app.core.pipeline.filtering.submit_db_write"):
        for idx in range(14):
            item = PublishItem(
                item_id=f"/tmp/big_{idx}.mkv",
                name=f"big_{idx}.mkv",
                size_bytes=3 * 1024 * 1024 * 1024,
            )
            await filt.handle_oversize(
                item, ctx, 2000 * 1024 * 1024, is_pikpak=False, counters=counters, b=b, all_skipped=all_skipped
            )

    # 第 1 次 send_message 失败不占名额；随后发出 10 张逐条卡 + 1 张溢出汇总卡 = 12 次调用 (11 次成功)
    assert client.send_message.await_count == 1 + PER_ITEM_CARD_CAP + 1
    assert client.send_message.await_args_list[-1].args[1] == OVERFLOW_TEXT
    assert len(ctx.sent_placeholder_keys) == PER_ITEM_CARD_CAP


@pytest.mark.asyncio
async def test_placeholder_terminalization_preserves_editing_and_reserves_identities() -> None:
    """终态化不覆盖 editing/published 槽；发卡前原子预占避免并发双发."""
    # mark_failed_terminal 保护 editing 态
    slot_edit = Slots.insert_new_slot("-10055", "/edit.mkv", 100, placeholder_msg_id=10, task_id="t_ed")
    assert Slots.cas_acquire_edit("-10055", "/edit.mkv", 100)
    assert Slots.mark_failed_terminal(slot_edit, "late fail") is False
    assert Slots.get_by_id(slot_edit)["state"] == "editing"

    # finalize_terminal 在 resolve_entity 期间若槽位进入 editing，不编辑消息且不置终态
    slot_race = Slots.insert_new_slot(
        "-10055", "/race.mkv", 200, placeholder_msg_id=20, task_id="t_fin_race", sender_account_id=1
    )
    Slots.increment_fail(slot_race, "1")
    Slots.increment_fail(slot_race, "2")
    Slots.increment_fail(slot_race, "3")
    Slots.increment_fail(slot_race, "4")
    client = AsyncMock()

    async def acquire_during_resolve(*args):
        Slots.cas_acquire_edit("-10055", "/race.mkv", 200)
        return -10055

    with (
        patch.object(PlaceholderSlotCoordinator, "_resolve_userbot", return_value=client),
        patch("app.core.entity_resolver.resolve_entity", side_effect=acquire_during_resolve),
    ):
        count = await PlaceholderSlotCoordinator(max_retries=3).finalize_terminal("t_fin_race")
    assert count == 0
    assert Slots.get_by_id(slot_race)["state"] == "editing"
    client.edit_message.assert_not_awaited()

    # 并发发卡触发 UNIQUE 预占冲突时直接复用既有槽，不发重复卡片
    ctx = PublishContext(
        task_id="t_resv", target_chat_id=-10055, userbot_client=client, delay_seconds=0.0, send_placeholder_card=True
    )
    sender = PlaceholderCardSender()
    orig_insert = Slots.insert_new_slot

    def racing_insert(*args, **kwargs):
        orig_insert("-10055", "/race_card.mkv", 300, placeholder_msg_id=30, task_id="t_other")
        raise sqlite3.IntegrityError("UNIQUE constraint failed")

    with patch.object(Slots, "insert_new_slot", side_effect=racing_insert):
        await sender.send_failure_card(
            PublishItem(item_id="/race_card.mkv", name="race_card.mkv", size_bytes=300, source_type="local"),
            ctx,
            "boom",
        )
    client.send_file.assert_not_awaited()
    client.send_message.assert_not_awaited()
    assert Slots.get_by_identity("-10055", "/race_card.mkv", 300)["fail_count"] == 1

    # 整组汇总卡并发预占冲突时复用既有槽且不重发汇总卡
    client.send_message.reset_mock()

    def racing_group_insert(chat, identity, size, **kwargs):
        orig_insert(chat, identity, size, slot_kind="group_summary", placeholder_msg_id=31, task_id="t_other")
        raise sqlite3.IntegrityError("UNIQUE constraint failed")

    with patch.object(Slots, "insert_new_slot", side_effect=racing_group_insert):
        await sender.send_group_summary_card(
            [PublishItem(item_id="/race_g1.jpg", name="race_g1.jpg", size_bytes=11, source_type="local")],
            ctx,
            "group boom",
        )
    client.send_message.assert_not_awaited()
    assert Slots.get_by_identity("-10055", "/race_g1.jpg", 11)["fail_count"] == 1


def test_startup_heal_deletes_stale_sending_reservations() -> None:
    """崩溃遗留的 sending 预占槽必须在启动自愈中删除, 其余状态不受影响."""
    stuck = Slots.insert_new_slot("-100heal", "cr54_stuck.mkv", 10, state="sending", task_id="t_crash")
    editing = Slots.insert_new_slot("-100heal", "cr54_editing.mkv", 20, placeholder_msg_id=50, task_id="t_ed")
    assert Slots.cas_acquire_edit("-100heal", "cr54_editing.mkv", 20)
    sent = Slots.insert_new_slot("-100heal", "cr54_sent.mkv", 30, placeholder_msg_id=60, task_id="t_ok")

    PlaceholderSlotCoordinator().heal_editing()

    assert Slots.get_by_id(stuck) is None
    assert Slots.get_by_id(editing) is not None and Slots.get_by_id(editing)["state"] == "placeholder_sent"
    assert Slots.get_by_id(sent) is not None and Slots.get_by_id(sent)["state"] == "placeholder_sent"


@pytest.mark.asyncio
async def test_single_card_activation_miss_reclaims_reservation() -> None:
    """激活返回 False 按未激活处理 —— 预留槽被回收, 身份不被滞留压制."""
    client = AsyncMock()
    client.send_file = AsyncMock(return_value=MagicMock(id=501))
    client.send_message = AsyncMock(return_value=MagicMock(id=502))
    ctx = PublishContext(
        task_id="t_act_miss",
        target_chat_id=-10077,
        userbot_client=client,
        delay_seconds=0.0,
        send_placeholder_card=True,
    )
    sender = PlaceholderCardSender()
    orig_insert = Slots.insert_new_slot
    reserved_ids: list[int] = []

    def spying_insert(*args, **kwargs):
        slot_id = orig_insert(*args, **kwargs)
        reserved_ids.append(slot_id)
        return slot_id

    with (
        patch.object(Slots, "insert_new_slot", side_effect=spying_insert),
        patch.object(Slots, "activate_reserved_slot", return_value=False),
    ):
        await sender.send_failure_card(
            PublishItem(item_id="/cr54_act_miss.mkv", name="act_miss.mkv", size_bytes=17, source_type="local"),
            ctx,
            "boom",
        )

    assert reserved_ids, "发卡前必须先预占 sending 槽"
    assert Slots.get_by_id(reserved_ids[0]) is None
    client.send_file.assert_awaited()


@pytest.mark.asyncio
async def test_group_summary_activation_miss_reclaims_member_reservation() -> None:
    """汇总卡成员激活失败/False 不滞留 sending —— 预留被回收, 身份解锁."""
    client = AsyncMock()
    client.send_message = AsyncMock(return_value=MagicMock(id=78))
    ctx = PublishContext(
        task_id="t_grp_miss",
        target_chat_id=-10088,
        userbot_client=client,
        delay_seconds=0.0,
        send_placeholder_card=True,
    )
    sender = PlaceholderCardSender()
    orig_insert = Slots.insert_new_slot
    reserved_ids: list[int] = []

    def spying_insert(*args, **kwargs):
        slot_id = orig_insert(*args, **kwargs)
        reserved_ids.append(slot_id)
        return slot_id

    with (
        patch.object(Slots, "insert_new_slot", side_effect=spying_insert),
        patch.object(Slots, "activate_reserved_slot", return_value=False),
    ):
        await sender.send_group_summary_card(
            [PublishItem(item_id="/cr54_grp_miss.mkv", name="grp_miss.mkv", size_bytes=19, source_type="local")],
            ctx,
            "group boom",
        )

    assert reserved_ids, "汇总卡发卡前必须先预占成员槽"
    assert Slots.get_by_id(reserved_ids[0]) is None
    client.send_message.assert_awaited()


@pytest.mark.asyncio
async def test_group_summary_insert_failure_degrades_to_untracked_send() -> None:
    """成员槽预占写入失败 (非 UNIQUE) 必须降级无槽发卡, 不得静默吞掉整组失败通知."""
    client = AsyncMock()
    client.send_message = AsyncMock(return_value=MagicMock(id=88))
    ctx = PublishContext(
        task_id="t_grp_ins_fail",
        target_chat_id=-10099,
        userbot_client=client,
        delay_seconds=0.0,
        send_placeholder_card=True,
    )
    sender = PlaceholderCardSender()

    def failing_insert(*args, **kwargs):
        raise RuntimeError("db down")

    with patch.object(Slots, "insert_new_slot", side_effect=failing_insert):
        await sender.send_group_summary_card(
            [
                PublishItem(
                    item_id="/cr54_grp_ins_fail.mkv", name="grp_ins_fail.mkv", size_bytes=23, source_type="local"
                )
            ],
            ctx,
            "group boom",
        )

    client.send_message.assert_awaited_once()
    assert Slots.get_by_identity("-10099", "/cr54_grp_ins_fail.mkv", 23) is None


@pytest.mark.asyncio
async def test_placeholder_edits_use_sender_account_and_wait_for_all_shared_members() -> None:
    """占位槽编辑按 sender_account_id 选客户端；共享汇总卡须全员成功后才翻 ✅."""
    from types import SimpleNamespace

    client_sender = AsyncMock()
    client_other = AsyncMock()
    sender_acc = SimpleNamespace(client=client_sender)

    Slots.insert_new_slot(
        "-10091",
        "/dir/x.mkv",
        50,
        slot_kind="backfillable",
        placeholder_msg_id=701,
        sender_account_id=10,
        task_id="t_ph_cross",
    )
    ctx_other = PublishContext(
        task_id="t_ph_cross",
        target_chat_id=-10091,
        account_id=20,
        userbot_client=client_other,
        send_placeholder_card=True,
    )
    with patch("app.core.client_manager.client_manager.get_account", return_value=sender_acc) as m_get:
        await PlaceholderCardSender().send_failure_card(
            PublishItem(item_id="/dir/x.mkv", name="x.mkv", size_bytes=50, source_type="local"),
            ctx_other,
            "not found",
            terminal=True,
        )
    m_get.assert_called_with(10)
    client_sender.edit_message.assert_awaited_once()
    client_other.edit_message.assert_not_awaited()

    client_sender.edit_message.reset_mock()
    Slots.insert_new_slot(
        "-10092",
        "/dir/a.jpg",
        10,
        slot_kind="group_summary",
        placeholder_msg_id=888,
        sender_account_id=10,
        task_id="t_gs",
    )
    Slots.insert_new_slot(
        "-10092",
        "/dir/b.jpg",
        20,
        slot_kind="group_summary",
        placeholder_msg_id=888,
        sender_account_id=10,
        task_id="t_gs",
    )
    item_a = PublishItem(item_id="/dir/a.jpg", name="a.jpg", size_bytes=10, source_type="local")
    item_b = PublishItem(item_id="/dir/b.jpg", name="b.jpg", size_bytes=20, source_type="local")
    ctx_gs = PublishContext(task_id="t_gs", target_chat_id=-10092, account_id=20, userbot_client=client_other)
    coord = PlaceholderSlotCoordinator()

    with patch("app.core.client_manager.client_manager.get_account", return_value=sender_acc):
        await coord.flip_after_group_send([(item_a, 9001)], ctx_gs)
        assert Slots.get_by_identity("-10092", "/dir/a.jpg", 10)["state"] == "flipped"
        assert Slots.get_by_identity("-10092", "/dir/b.jpg", 20)["state"] == "placeholder_sent"
        client_sender.edit_message.assert_not_awaited()

        await coord.flip_after_group_send([(item_b, 9002)], ctx_gs)
        assert Slots.get_by_identity("-10092", "/dir/b.jpg", 20)["state"] == "flipped"
        client_sender.edit_message.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", [UploadPausedError("pause"), RetryableTaskError("flood")])
async def test_try_backfill_resets_slot_on_pause_and_retryable(exc: Exception) -> None:
    """_try_backfill 遇 UploadPausedError / RetryableTaskError 须先将槽复位 placeholder_sent."""
    Slots.insert_new_slot(
        "-10093",
        "/dir/bf.mp4",
        123,
        slot_kind="backfillable",
        placeholder_msg_id=555,
        task_id="t_bf_rst",
    )

    class _DummyEngine:
        async def edit_item(self, item, context, msg_id):
            raise exc

    pipeline = PublishPipeline(source=MagicMock(), engine=_DummyEngine())
    ctx = PublishContext(task_id="t_bf_rst", target_chat_id=-10093)
    item = PublishItem(item_id="/dir/bf.mp4", name="bf.mp4", size_bytes=123, source_type="local")

    with pytest.raises(type(exc)):
        await pipeline._try_backfill(item, ctx)

    row = Slots.get_by_identity("-10093", "/dir/bf.mp4", 123)
    assert row is not None and row["state"] == "placeholder_sent"


@pytest.mark.parametrize("matching_context", [True, False])
async def test_recorded_sender_never_uses_default_client(matching_context):
    from types import SimpleNamespace

    from app.core.pipeline.placeholder import _resolve_slot_edit_client

    context_client = AsyncMock()
    ctx = PublishContext(
        task_id="task", target_chat_id=-1001, account_id=10 if matching_context else 20, userbot_client=context_client
    )
    manager = SimpleNamespace(get_account=MagicMock(return_value=SimpleNamespace(client=None)), userbot=AsyncMock())
    with patch("app.core.client_manager.client_manager", manager):
        assert _resolve_slot_edit_client(10, ctx) is (context_client if matching_context else None)


@pytest.mark.asyncio
async def test_filtering_oversize_card_key_uses_item_id():
    """同名不同路径的超限文件应各自发出占位卡."""
    bf = BatchFilter(MagicMock())
    client = MagicMock()
    client.send_message = AsyncMock()
    ctx = PublishContext(
        task_id="t_ck", target_chat_id=-1001, userbot_client=client, send_placeholder_card=True, delay_seconds=0.0
    )
    b = BatchState()
    counters = Counters(2)
    i1 = PublishItem(item_id="/dir1/dup.mkv", name="dup.mkv", size_bytes=3000 * 1024 * 1024)
    i2 = PublishItem(item_id="/dir2/dup.mkv", name="dup.mkv", size_bytes=3000 * 1024 * 1024)
    with patch("app.core.pipeline.filtering.submit_db_write"):
        await bf.handle_oversize(i1, ctx, 2000 * 1024 * 1024, is_pikpak=False, counters=counters, b=b, all_skipped=[])
        await bf.handle_oversize(i2, ctx, 2000 * 1024 * 1024, is_pikpak=False, counters=counters, b=b, all_skipped=[])
    assert client.send_message.await_count == 2


@pytest.mark.asyncio
async def test_single_send_reconciles_existing_group_summary_slot():
    """原相册失败建立的 group_summary 槽在后续普通单发成功后应同步翻转."""
    from app.core.pipeline.models import TransportOutcome

    Slots.insert_new_slot(
        "-100777",
        "/dir/p1.jpg",
        100,
        slot_kind="group_summary",
        placeholder_msg_id=888,
        task_id="t_rec",
    )
    item = PublishItem(item_id="/dir/p1.jpg", name="p1.jpg", size_bytes=100, source_type="local")
    engine = MagicMock()
    engine.send_item = AsyncMock(return_value=TransportOutcome(message_ids=[999], speed_mb_s=1.0))
    pipeline = PublishPipeline(source=MagicMock(), engine=engine)
    ctx = PublishContext(task_id="t_rec", target_chat_id=-100777)
    await pipeline._transport_single(item, ctx, Counters(1), BatchState(), is_pikpak=False)
    row = Slots.get_by_identity("-100777", "/dir/p1.jpg", 100)
    assert row is not None and row["state"] == "flipped"


@pytest.mark.asyncio
async def test_placeholder_terminal_reuse_cap_and_group_dedup(monkeypatch):
    """复用槽转终态、溢出文案无陈旧计数、写库失败仍计封顶、群组槽复用不重发."""
    from app.core.pipeline.placeholder import OVERFLOW_TEXT

    assert "{count}" not in OVERFLOW_TEXT
    client = AsyncMock()
    client.send_file = AsyncMock(return_value=MagicMock(id=501, photo=MagicMock()))
    client.send_message = AsyncMock(return_value=MagicMock(id=601))
    ctx = PublishContext(
        task_id="t_ph", target_chat_id=-100888, userbot_client=client, delay_seconds=0.0, send_placeholder_card=True
    )
    sender = PlaceholderCardSender()
    item = PublishItem(item_id="/tv/a.mkv", name="a.mkv", size_bytes=50, source_type="local")

    await sender.send_failure_card(item, ctx, "timeout")
    await sender.send_failure_card(item, ctx, "not found", terminal=True)
    row = Slots.get_by_identity("-100888", "/tv/a.mkv", 50)
    assert row is not None and row["state"] == "failed_terminal"
    client.edit_message.assert_awaited_once()

    before_cards = sender._per_item_cards
    with patch.object(Slots, "insert_new_slot", side_effect=RuntimeError("db err")):
        await sender.send_failure_card(
            PublishItem(item_id="/tv/b.mkv", name="b.mkv", size_bytes=50, source_type="local"), ctx, "err"
        )
    assert sender._per_item_cards == before_cards + 1

    m1 = PublishItem(item_id="/tv/g1.jpg", name="g1.jpg", size_bytes=10, source_type="local")
    m2 = PublishItem(item_id="/tv/g2.jpg", name="g2.jpg", size_bytes=20, source_type="local")
    await sender.send_group_summary_card([m1, m2], ctx, "fail1")
    await sender.send_group_summary_card([m1, m2], ctx, "fail2")
    assert client.send_message.await_count == 1
    assert Slots.get_by_identity("-100888", "/tv/g1.jpg", 10)["fail_count"] == 1


@pytest.mark.asyncio
async def test_placeholder_terminal_protects_published_and_retries_failed_edit():
    """published/flipped 槽不被 terminal 覆盖; edit 失败后的 failed_terminal 槽重入仍补改文案."""
    client = AsyncMock()
    ctx = PublishContext(
        task_id="t_ph_guard",
        target_chat_id=-100999,
        userbot_client=client,
        delay_seconds=0.0,
        send_placeholder_card=True,
    )
    sender = PlaceholderCardSender()

    pub_item = PublishItem(item_id="/tv/pub.mkv", name="pub.mkv", size_bytes=50, source_type="local")
    pub_slot_id = Slots.insert_new_slot("-100999", "/tv/pub.mkv", 50, slot_kind="backfillable", placeholder_msg_id=701)
    Slots.mark_published(pub_slot_id, 901)

    flip_item = PublishItem(item_id="/tv/flip.jpg", name="flip.jpg", size_bytes=60, source_type="local")
    flip_slot_id = Slots.insert_new_slot(
        "-100999", "/tv/flip.jpg", 60, slot_kind="group_summary", placeholder_msg_id=702
    )
    Slots.mark_flipped(flip_slot_id)

    await sender.send_failure_card(pub_item, ctx, "not found", terminal=True)
    await sender.send_failure_card(flip_item, ctx, "not found", terminal=True)
    assert Slots.get_by_identity("-100999", "/tv/pub.mkv", 50)["state"] == "published"
    assert Slots.get_by_identity("-100999", "/tv/flip.jpg", 60)["state"] == "flipped"
    client.edit_message.assert_not_awaited()

    retry_item = PublishItem(item_id="/tv/retry.mkv", name="retry.mkv", size_bytes=70, source_type="local")
    Slots.insert_new_slot("-100999", "/tv/retry.mkv", 70, slot_kind="backfillable", placeholder_msg_id=703)
    client.edit_message = AsyncMock(side_effect=[RuntimeError("temp tg error"), MagicMock()])
    await sender.send_failure_card(retry_item, ctx, "not found", terminal=True)
    assert Slots.get_by_identity("-100999", "/tv/retry.mkv", 70)["state"] == "failed_terminal"
    await sender.send_failure_card(retry_item, ctx, "not found", terminal=True)
    assert client.edit_message.await_count == 2


@pytest.mark.asyncio
async def test_group_summary_single_send_recovery_and_partial_terminal_state_machine():
    """单发逐条恢复经 Coordinator 仅在全员完成后翻 ✅，部分恢复终态文案准确且不递增已翻槽计数."""
    from app.core.pipeline.models import TransportOutcome
    from app.core.pipeline.placeholder import FLIPPED_TEXT, GROUP_PARTIAL_TERMINAL_TEXT

    chat = "-100778"
    Slots.insert_new_slot(chat, "/dir/gs1.jpg", 10, slot_kind="group_summary", placeholder_msg_id=889, task_id="t_gs2")
    Slots.insert_new_slot(chat, "/dir/gs2.jpg", 20, slot_kind="group_summary", placeholder_msg_id=889, task_id="t_gs2")
    Slots.insert_new_slot(chat, "/dir/gs3.jpg", 30, slot_kind="group_summary", placeholder_msg_id=889, task_id="t_gs2")

    item1 = PublishItem(item_id="/dir/gs1.jpg", name="gs1.jpg", size_bytes=10, source_type="local")
    item2 = PublishItem(item_id="/dir/gs2.jpg", name="gs2.jpg", size_bytes=20, source_type="local")
    item3 = PublishItem(item_id="/dir/gs3.jpg", name="gs3.jpg", size_bytes=30, source_type="local")

    client = AsyncMock()
    engine = MagicMock()
    engine.send_item = AsyncMock(
        side_effect=[
            TransportOutcome(message_ids=[1001], speed_mb_s=1.0),
            TransportOutcome(message_ids=[1002], speed_mb_s=1.0),
            TransportOutcome(message_ids=[1003], speed_mb_s=1.0),
        ]
    )
    pipeline = PublishPipeline(source=MagicMock(), engine=engine)
    ctx = PublishContext(
        task_id="t_gs2",
        target_chat_id=-100778,
        userbot_client=client,
        delay_seconds=0.0,
        send_placeholder_card=True,
    )

    await pipeline._transport_single(item1, ctx, Counters(3), BatchState(), is_pikpak=False)
    assert Slots.get_by_identity(chat, "/dir/gs1.jpg", 10)["state"] == "flipped"
    client.edit_message.assert_not_awaited()

    await PlaceholderCardSender().send_group_summary_card([item1, item2, item3], ctx, "partial err")
    assert Slots.get_by_identity(chat, "/dir/gs1.jpg", 10)["fail_count"] == 0
    assert Slots.get_by_identity(chat, "/dir/gs2.jpg", 20)["fail_count"] == 1

    await pipeline._transport_single(item2, ctx, Counters(3), BatchState(), is_pikpak=False)
    client.edit_message.assert_not_awaited()
    await pipeline._transport_single(item3, ctx, Counters(3), BatchState(), is_pikpak=False)
    client.edit_message.assert_awaited_once_with(-100778, 889, text=FLIPPED_TEXT)

    chat2 = "-100779"
    s1 = Slots.insert_new_slot(
        chat2, "/dir/p1.jpg", 10, slot_kind="group_summary", placeholder_msg_id=990, task_id="t_gs_term"
    )
    Slots.insert_new_slot(
        chat2, "/dir/p2.jpg", 20, slot_kind="group_summary", placeholder_msg_id=990, task_id="t_gs_term"
    )
    Slots.mark_flipped(s1)
    term_client = AsyncMock()
    with (
        patch.object(PlaceholderSlotCoordinator, "_resolve_userbot", return_value=term_client),
        patch("app.core.entity_resolver.resolve_entity", new_callable=AsyncMock, return_value=-100779),
    ):
        finalized = await PlaceholderSlotCoordinator().finalize_terminal("t_gs_term")
    assert finalized == 1
    term_client.edit_message.assert_awaited_once_with(-100779, 990, text=GROUP_PARTIAL_TERMINAL_TEXT)


@pytest.mark.asyncio
@pytest.mark.parametrize("group_send", [False, True])
async def test_terminal_summary_partial_recovery_refreshes_card(group_send):
    from app.core.pipeline.placeholder import FLIPPED_TEXT, GROUP_PARTIAL_TERMINAL_TEXT

    chat = "-100780"
    first = Slots.insert_new_slot(
        chat, "/dir/recover1.jpg", 10, slot_kind="group_summary", placeholder_msg_id=991, task_id="recover"
    )
    second = Slots.insert_new_slot(
        chat, "/dir/recover2.jpg", 20, slot_kind="group_summary", placeholder_msg_id=991, task_id="recover"
    )
    client = AsyncMock()
    coordinator = PlaceholderSlotCoordinator()
    with patch.object(coordinator, "_resolve_userbot", return_value=None):
        assert await coordinator.finalize_terminal("recover") == 2
    ctx = PublishContext(task_id="recover", target_chat_id=int(chat), userbot_client=client)
    items = [
        PublishItem(item_id="/dir/recover1.jpg", name="recover1.jpg", size_bytes=10),
        PublishItem(item_id="/dir/recover2.jpg", name="recover2.jpg", size_bytes=20),
    ]
    for index, item in enumerate(items):
        if group_send:
            await coordinator.flip_after_group_send([(item, 100 + index)], ctx)
        else:
            await coordinator.reconcile_post_send(item, ctx, 100 + index)
        expected = GROUP_PARTIAL_TERMINAL_TEXT if index == 0 else FLIPPED_TEXT
        client.edit_message.assert_awaited_with(int(chat), 991, text=expected)
    assert Slots.get_by_id(first)["state"] == "flipped"
    assert Slots.get_by_id(second)["state"] == "flipped"
    assert len(Slots.list_by_placeholder_msg_id(chat, 991)) == 2
    assert first != second


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_at", ["before", "resolve", "edit", "never"])
async def test_admission_finalization_does_not_terminalize_retried_slots(retry_at):
    task_id = "guarded_rejection"
    TaskQueueRepository.create(task_id, "publish", {}, status="queued")
    assert TaskQueueRepository.fail_queued_task(task_id, "rejection")
    Slots.insert_new_slot(
        "-100789", "/guarded.jpg", 10, slot_kind="group_summary", placeholder_msg_id=992, task_id=task_id
    )
    client = AsyncMock()

    def retry():
        TaskQueueRepository.update_status(task_id, "queued")

    async def resolve(*args):
        if retry_at == "resolve":
            retry()
        return -100789

    async def edit(*args, **kwargs):
        if retry_at == "edit":
            retry()

    client.edit_message.side_effect = edit
    if retry_at == "before":
        retry()
    with (
        patch.object(PlaceholderSlotCoordinator, "_resolve_userbot", return_value=client),
        patch("app.core.entity_resolver.resolve_entity", side_effect=resolve),
    ):
        count = await PlaceholderSlotCoordinator().finalize_terminal(task_id, expected_error="rejection")
    assert count == int(retry_at == "never")
    slot = Slots.get_by_identity("-100789", "/guarded.jpg", 10)
    assert slot["state"] == ("failed_terminal" if retry_at == "never" else "placeholder_sent")
    if retry_at in ("before", "resolve"):
        client.edit_message.assert_not_awaited()
    elif retry_at == "edit":
        client.edit_message.assert_awaited_once()
        assert TaskQueueRepository.get_by_id(task_id)["status"] == "queued"


def test_cas_acquire_edit_blocks_cross_task_failed_terminal_and_cleanup_deletes_it():
    """failed_terminal 仅限同任务原地复活，禁止跨任务复活；任务删除清理后不再残留可复活槽。"""
    TaskQueueRepository.create("t_owner_1", "publish_folder", {"target_channel": "-100123"})
    TaskQueueRepository.create("t_other_2", "publish_folder", {"target_channel": "-100123"})
    try:
        slot = _mk("t_owner_1", path="/tv/cross_task.mkv", size=4096)
        assert Slots.mark_failed_terminal(slot, "终端失败")

        assert Slots.cas_acquire_edit("-100123", "/tv/cross_task.mkv", 4096, task_id="t_other_2") is False
        assert Slots.get_by_id(slot)["state"] == "failed_terminal"

        assert Slots.cas_acquire_edit("-100123", "/tv/cross_task.mkv", 4096, task_id="t_owner_1") is True
        assert Slots.get_by_id(slot)["state"] == "editing"

        Slots.reset_to_sent(slot)
        assert Slots.mark_failed_terminal(slot, "再次终态")
        TaskQueueRepository.delete("t_owner_1")

        assert Slots.get_by_identity("-100123", "/tv/cross_task.mkv", 4096) is None
        assert Slots.cas_acquire_edit("-100123", "/tv/cross_task.mkv", 4096, task_id="t_other_2") is False
    finally:
        TaskQueueRepository.delete("t_owner_1")
        TaskQueueRepository.delete("t_other_2")
