"""Business Services: 频道消息转发引擎、断点续转闭环、中转桥梁 (Transit Bridge) 与会话失效感知测试 (Forward Service & Transit Bridge)

覆盖范围:
- 原 test_forward_engine: 转发引擎核心状态机、批量转发、相册组装与异常重试
- 原 test_forward_closure: 断点续转闭环验收、账本推进与已转消息跳过
- 原 test_transit_bridge: 受限频道启发式中转下载上传桥梁、相册原子转发与临时文件清理
- 原 test_session_invalidation: 账号会话失效感知、异常分类识别与广播通知"""

import asyncio
import os
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from _cm_seed import seed_default_account, unseed_accounts
from _safeio import safe_write_bytes
from telethon.errors import ChatWriteForbiddenError, FloodWaitError, RPCError, SlowModeWaitError
from telethon.tl.types import (
    Channel,
    DocumentAttributeFilename,
    MessageActionChatAddUser,
    MessageMediaDocument,
    MessageMediaPhoto,
    RestrictionReason,
)

from app.core.client_manager import TelegramBotClient, client_manager
from app.core.connection_manager import connection_manager
from app.core.runtime import active_task_controls
from app.core.session_errors import is_session_invalid_error, report_session_invalid
from app.db.account_repository import SystemSettingsRepository
from app.db.database import get_db
from app.db.download_repository import DownloadHistoryRepository
from app.db.forward_repository import ForwardHistoryRepository, ForwardLedgerRepository
from app.db.task_repository import TaskQueueRepository
from app.services.downloader_service import DownloaderService
from app.services.forward_service import (
    AdaptivePacer,
    ForwardCancelledError,
    ForwardPausedError,
    ForwardService,
    MessagePureFilter,
    _commit_batch,
    _flush_partial_ledger,
    _ForwardRunCtx,
    _ForwardRunState,
    _reject_same_peer_pair,
)
from app.services.transit_bridge_service import TransitBridgeService

# ==============================================================================
# Part: test_forward_engine.py
# ==============================================================================
# =========================================================================
# 1. 前置本地纯净度清洗器 (MessagePureFilter) 单元测试
# =========================================================================


def test_pure_filter_valid_normal_messages():
    # 纯文本正常消息
    msg_text = MagicMock(empty=False, action=None, media=None, message="正常文本消息", restriction_reason=None)
    ok, reason = MessagePureFilter.is_forwardable(msg_text)
    assert ok is True
    assert reason == "valid"

    # 图片正常消息
    msg_photo = MagicMock(empty=False, action=None, media=MessageMediaPhoto(), message="", restriction_reason=None)
    ok, reason = MessagePureFilter.is_forwardable(msg_photo)
    assert ok is True
    assert reason == "valid"

    # 视频正常消息
    msg_doc = MagicMock(
        empty=False, action=None, media=MessageMediaDocument(), message="视频描述", restriction_reason=None
    )
    ok, reason = MessagePureFilter.is_forwardable(msg_doc)
    assert ok is True
    assert reason == "valid"


def test_pure_filter_invalid_and_service_messages():
    # None
    assert MessagePureFilter.is_forwardable(None) == (False, "null_message")

    # empty=True
    msg_empty = MagicMock(empty=True)
    assert MessagePureFilter.is_forwardable(msg_empty) == (False, "empty_message")

    # 服务动作消息 (加群等)
    msg_action = MagicMock(empty=False, action=MessageActionChatAddUser(users=[123]), media=None, message="User joined")
    assert MessagePureFilter.is_forwardable(msg_action) == (False, "service_action")

    # 幽灵消息 (既无媒体也无文本)
    msg_ghost = MagicMock(empty=False, action=None, media=None, message="", raw_text="")
    assert MessagePureFilter.is_forwardable(msg_ghost) == (False, "ghost_no_content")


def test_pure_filter_tos_dead_vs_restricted_media():
    # 1. 全平台 TOS 死消息占位 (platform='all', reason='terms', media=None) -> 必须拦截！
    dead_reason = RestrictionReason(platform="all", reason="terms", text="This message is not available due to terms.")
    msg_dead_tos = MagicMock(
        empty=False, action=None, media=None, message="This message is not available", restriction_reason=[dead_reason]
    )
    ok, reason = MessagePureFilter.is_forwardable(msg_dead_tos)
    assert ok is False
    assert reason == "tos_dead_terms"

    # 2. 全平台 Copyright 死消息占位 (platform='all', reason='copyright', media=None) -> 必须拦截！
    dead_copy = RestrictionReason(platform="all", reason="copyright", text="Copyright violation.")
    msg_dead_copy = MagicMock(
        empty=False, action=None, media=None, message="Copyright removed", restriction_reason=[dead_copy]
    )
    ok, reason = MessagePureFilter.is_forwardable(msg_dead_copy)
    assert ok is False
    assert reason == "tos_dead_copyright"

    # 3. 局部平台受限但含有完整媒体文件 (platform='ios', reason='porn', media=Photo) -> 必须放行！
    ios_reason = RestrictionReason(platform="ios", reason="porn", text="Blocked on iOS")
    msg_ios_media = MagicMock(
        empty=False, action=None, media=MessageMediaPhoto(), message="正常成人合规资源", restriction_reason=[ios_reason]
    )
    ok, reason = MessagePureFilter.is_forwardable(msg_ios_media)
    assert ok is True
    assert reason == "valid"

    # 4. 局部平台受限但含有文本描述 (platform='android', reason='other', message="内容") -> 必须放行！
    android_reason = RestrictionReason(platform="android", reason="other", text="Blocked on Android")
    msg_android_text = MagicMock(
        empty=False, action=None, media=None, message="仅安卓分级受限文本", restriction_reason=[android_reason]
    )
    ok, reason = MessagePureFilter.is_forwardable(msg_android_text)
    assert ok is True
    assert reason == "valid"


# =========================================================================
# 2. 自适应动态节流器 (AdaptivePacer) 单元测试
# =========================================================================


def test_adaptive_pacer_dynamics():
    pacer = AdaptivePacer(base_delay=2.0, min_delay=0.5, max_delay=6.0)
    assert pacer.current_delay == 2.0

    # 模拟发生重试
    pacer.record_retry()
    assert pacer.current_delay == 2.8
    assert pacer.consecutive_success == 0

    # 模拟触发 FloodWait
    pacer.record_flood_wait()
    assert pacer.current_delay == 4.8

    # 模拟连续成功 5 次
    for _ in range(5):
        pacer.record_success()
    assert pacer.current_delay == 4.6

    for _ in range(5):
        pacer.record_success()
    assert pacer.current_delay == 4.4

    # 确保不超过上限和下限
    for _ in range(10):
        pacer.record_flood_wait()
    assert pacer.current_delay == 6.0  # max_delay


# =========================================================================
# 3. 频道与标识符解析测试
# =========================================================================


def test_parse_chat_identifier_formats():
    assert ForwardService.parse_chat_identifier("https://t.me/c/9000000010/1234") == -1009000000010
    assert ForwardService.parse_chat_identifier("https://t.me/sample_chan/10") == "sample_chan"
    assert ForwardService.parse_chat_identifier("-1009000000027") == -1009000000027
    assert ForwardService.parse_chat_identifier(-100123) == -100123
    assert ForwardService.parse_chat_identifier("test_group") == "test_group"


# =========================================================================
# 4. 二分隔离批次转发引擎与熔断机制测试
# =========================================================================


@pytest.mark.asyncio
async def test_forward_healthy_batch(tmp_dir):
    """测试全部健康的消息批次：1 次 RPC 批量提交成功"""
    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)

    # 模拟 10 条消息
    msgs = []
    for i in range(1, 11):
        m = MagicMock(
            id=i, empty=False, action=None, media=MessageMediaPhoto(), message=f"Msg {i}", restriction_reason=None
        )
        msgs.append(m)

    async def mock_iter(*args, **kwargs):
        for m in msgs:
            yield m

    mock_client.iter_messages = mock_iter
    mock_client.get_messages = AsyncMock(return_value=MagicMock(total=10))
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=1001))

    # 模拟 forward_messages 批量成功返回对应的转发对象
    async def mock_forward_messages(entity, messages, from_peer, **kwargs):
        return [MagicMock(id=1000 + mid) for mid in messages]

    mock_client.forward_messages = AsyncMock(side_effect=mock_forward_messages)

    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        count = await ForwardService.forward_channel_messages(
            source_chat="https://t.me/c/9000000010/1", target_chat="-100900037", batch_size=10, delay_seconds=0.01
        )
        assert count == 10
        # 10 条消息一次性批量转发，forward_messages 只被调用了 1 次！
        assert mock_client.forward_messages.call_count == 1


@pytest.mark.asyncio
async def test_forward_bisect_isolate_single_bad_message(tmp_dir):
    """测试包含 1 条损坏死消息的批次：二分算法精准隔离坏消息，放行其余健康消息"""
    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)

    # 模拟 8 条消息，其中 ID=5 是隐藏的损坏死消息
    msgs = []
    for i in range(1, 9):
        m = MagicMock(
            id=i, empty=False, action=None, media=MessageMediaPhoto(), message=f"Msg {i}", restriction_reason=None
        )
        msgs.append(m)

    async def mock_iter(*args, **kwargs):
        for m in msgs:
            yield m

    mock_client.iter_messages = mock_iter
    mock_client.get_messages = AsyncMock(return_value=MagicMock(total=8))
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=1001))

    # 模拟 forward_messages：如果批次包含 ID=5 则抛出 RPCError，单条发 ID=5 也抛出异常
    async def mock_forward_messages(entity, messages, from_peer, **kwargs):
        if 5 in messages:
            raise RPCError(request=None, message="MESSAGE_ID_INVALID")
        return [MagicMock(id=2000 + mid) for mid in messages]

    mock_client.forward_messages = AsyncMock(side_effect=mock_forward_messages)

    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        count = await ForwardService.forward_channel_messages(
            source_chat="-1009000000010", target_chat="-100900037", batch_size=8, delay_seconds=0.01
        )
        # 8 条中成功转发 7 条，隔离跳过 1 条 (ID=5)
        assert count == 7
        # 验证二分调用次数远小于逐条单发的 8+1 次
        assert mock_client.forward_messages.call_count > 1


@pytest.mark.asyncio
@pytest.mark.parametrize("wait_type", [FloodWaitError, SlowModeWaitError])
async def test_forward_flood_wait_circuit_breaker(wait_type):
    """测试 FloodWait 顶级熔断拦截与绝对休眠重试机制"""
    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)

    msgs = [
        MagicMock(id=1, empty=False, action=None, media=MessageMediaPhoto(), message="Msg 1", restriction_reason=None)
    ]

    async def mock_iter(*args, **kwargs):
        for m in msgs:
            yield m

    mock_client.iter_messages = mock_iter
    mock_client.get_messages = AsyncMock(return_value=MagicMock(total=1))
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=1001))

    # 第一次调用抛出 FloodWaitError(0.01s)，第二次成功
    calls = 0

    async def mock_forward_messages(entity, messages, from_peer, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            err = wait_type(request=None)
            err.seconds = 0.01
            raise err
        return [MagicMock(id=3001)]

    mock_client.forward_messages = AsyncMock(side_effect=mock_forward_messages)

    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        count = await ForwardService.forward_channel_messages(
            source_chat="-1009000000010", target_chat="-100900037", batch_size=1, delay_seconds=0.01
        )
        assert count == 1
        assert calls == 2


@pytest.mark.asyncio
async def test_forward_task_cancellation():
    """测试用户手动取消任务时的响应机制"""
    task_id = "test_cancel_task"
    active_task_controls[task_id] = {"cancelled": True, "paused": False}

    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)
    mock_client.get_messages = AsyncMock(return_value=MagicMock(total=1))
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=1001))

    msgs = [
        MagicMock(id=1, empty=False, action=None, media=MessageMediaPhoto(), message="Msg 1", restriction_reason=None)
    ]

    async def mock_iter(*args, **kwargs):
        for m in msgs:
            yield m

    mock_client.iter_messages = mock_iter

    with patch("app.core.client_manager.client_manager.userbot", mock_client), pytest.raises(ForwardCancelledError):
        await ForwardService.forward_channel_messages(
            source_chat="-1009000000010",
            target_chat="-100900037",
            batch_size=10,
            delay_seconds=0.01,
            task_id=task_id,
        )
    active_task_controls.pop(task_id, None)


# =========================================================================
# 5. 防重剔除与账本落库 (增量/全量模式) 测试
# =========================================================================


def _build_mock_client(msgs, forward_impl=None):
    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)

    async def mock_iter(*args, **kwargs):
        for m in msgs:
            yield m

    mock_client.iter_messages = mock_iter
    mock_client.get_messages = AsyncMock(return_value=MagicMock(total=len(msgs)))
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=1001))

    if forward_impl:
        mock_client.forward_messages = AsyncMock(side_effect=forward_impl)
    else:

        async def default_forward(entity, messages, from_peer, **kwargs):
            return [MagicMock(id=5000 + mid) for mid in messages]

        mock_client.forward_messages = AsyncMock(side_effect=default_forward)
    return mock_client


def _valid_msg(i):
    return MagicMock(
        id=i, empty=False, action=None, media=MessageMediaPhoto(), message=f"Msg {i}", restriction_reason=None
    )


@pytest.mark.asyncio
async def test_forward_dedup_skips_processed_messages():
    """增量模式: 已在 forward_history 的消息被剔除，不重发 (第二批转发 0 重复)"""
    SRC, TGT = -1009000000010, -100900037
    ForwardHistoryRepository.save_forward_batch(
        [
            (None, str(SRC), 1, str(TGT), 9001),
            (None, str(SRC), 2, str(TGT), 9002),
            (None, str(SRC), 3, str(TGT), 9003),
        ]
    )
    msgs = [_valid_msg(i) for i in range(1, 6)]
    mock_client = _build_mock_client(msgs)

    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        count = await ForwardService.forward_channel_messages(
            source_chat=str(SRC), target_chat=str(TGT), batch_size=10, delay_seconds=0.01
        )
    assert count == 2
    # 只发生 1 次 RPC，且仅携带未处理的 [4, 5]
    assert mock_client.forward_messages.call_count == 1
    forwarded_ids = mock_client.forward_messages.call_args.kwargs["messages"]
    assert forwarded_ids == [4, 5]


@pytest.mark.asyncio
async def test_forward_full_retransfer_forwards_all():
    """全量重转模式: 跳过防重剔除，全部消息重发 (仍写账本)"""
    SRC, TGT = -1009000000010, -100900037
    ForwardHistoryRepository.save_forward_batch(
        [
            (None, str(SRC), 1, str(TGT), 9001),
            (None, str(SRC), 2, str(TGT), 9002),
        ]
    )
    msgs = [_valid_msg(i) for i in range(1, 6)]
    mock_client = _build_mock_client(msgs)

    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        count = await ForwardService.forward_channel_messages(
            source_chat=str(SRC), target_chat=str(TGT), batch_size=10, delay_seconds=0.01, full_retransfer=True
        )
    assert count == 5
    forwarded_ids = mock_client.forward_messages.call_args.kwargs["messages"]
    assert forwarded_ids == [1, 2, 3, 4, 5]


@pytest.mark.asyncio
async def test_forward_filter_skips_persisted_to_ledger():
    """过滤跳过留痕 forward_skips (target=NULL)，覆盖区间写入 forward_coverage"""
    SRC, TGT = -1009000000010, -100900037
    msg_action = MagicMock(
        id=2,
        empty=False,
        action=MessageActionChatAddUser(users=[1]),
        media=None,
        message="joined",
        restriction_reason=None,
    )
    msg_ghost = MagicMock(id=3, empty=False, action=None, media=None, message="", raw_text="", restriction_reason=None)
    msg_empty = MagicMock(id=5, empty=True)
    msgs = [_valid_msg(1), msg_action, msg_ghost, _valid_msg(4), msg_empty]
    mock_client = _build_mock_client(msgs)

    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        count = await ForwardService.forward_channel_messages(
            source_chat=str(SRC), target_chat=str(TGT), batch_size=10, delay_seconds=0.01
        )
    assert count == 2

    conn = get_db()
    skip_rows = conn.execute(
        "SELECT msg_id, skip_type, target_chat FROM forward_skips WHERE source_chat = ? ORDER BY msg_id", (str(SRC),)
    ).fetchall()
    cov = conn.execute(
        "SELECT seg_start, seg_end FROM forward_coverage WHERE source_chat = ? AND target_chat = ?",
        (str(SRC), str(TGT)),
    ).fetchall()
    conn.close()
    assert [(r["msg_id"], r["skip_type"], r["target_chat"]) for r in skip_rows] == [
        (2, "service_action", None),
        (3, "ghost_no_content", None),
        (5, "empty_message", None),
    ]
    assert [(r["seg_start"], r["seg_end"]) for r in cov] == [(1, 5)]


# =========================================================================
# 6. 同 (源,目标) 并发防护测试
# =========================================================================


@pytest.mark.asyncio
async def test_forward_pair_lock_rejects_concurrent_same_pair():
    """进程内 pair lock: 同 (源,目标) 已有运行中任务时，第二次调用立即拒绝"""
    key = ("-1009000000010", "-100900037")
    ForwardService._pair_locks[key] = asyncio.Lock()
    lock = ForwardService._pair_locks[key]
    await lock.acquire()
    mock_client = _build_mock_client([])
    try:
        with (
            patch("app.core.client_manager.client_manager.userbot", mock_client),
            pytest.raises(RuntimeError, match="已有运行中的转发任务"),
        ):
            await ForwardService.forward_channel_messages(
                source_chat="-1009000000010", target_chat="-100900037", batch_size=10, delay_seconds=0.01
            )
    finally:
        lock.release()
    # 释放后 lock 处于未占用状态
    assert not ForwardService._pair_locks[key].locked()


@pytest.mark.asyncio
async def test_forward_pair_lock_evicted_after_completion(monkeypatch):
    """有界化: 任务结束释放并摘除 pair 锁与 target 锁条目, 不随历史 (源,目标) 对无限增长"""

    async def _fake_inner(cls, **kwargs):
        return 0

    monkeypatch.setattr(ForwardService, "_forward_channel_messages_inner", classmethod(_fake_inner))
    key = ("-1009000000010", "-100900037")
    mock_client = _build_mock_client([])
    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        await ForwardService.forward_channel_messages(
            source_chat="-1009000000010", target_chat="-100900037", batch_size=10, delay_seconds=0.01
        )
    assert key not in ForwardService._pair_locks
    assert "-100900037" not in ForwardService._target_locks


@pytest.mark.asyncio
async def test_forward_target_lock_rejects_concurrent_multi_source_same_target(monkeypatch):
    """需求 D: 多源向单目标并发时 (源A -> 目标X 与 源B -> 目标X)，目标级互斥锁必须拦截第二个任务，
    而向不同目标 (源B -> 目标Y) 可正常并行执行，且完成后锁字典有界清空。"""
    started_a = asyncio.Event()
    release_a = asyncio.Event()

    async def _slow_inner(cls, **kwargs):
        if kwargs.get("source_chat") == "-1009000000003":
            started_a.set()
            await release_a.wait()
        return 5

    monkeypatch.setattr(ForwardService, "_forward_channel_messages_inner", classmethod(_slow_inner))
    mock_client = _build_mock_client([])

    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        task_a = asyncio.create_task(
            ForwardService.forward_channel_messages(
                source_chat="-1009000000003", target_chat="-1009000000052", batch_size=10, delay_seconds=0.01
            )
        )
        await started_a.wait()

        try:
            # 不同源 (-1009000000026) 向同一目标 (-1009000000052) 并发转发 -> 必须立即拒绝
            with pytest.raises(RuntimeError, match="已有运行中的转发任务"):
                await ForwardService.forward_channel_messages(
                    source_chat="-1009000000026", target_chat="-1009000000052", batch_size=10, delay_seconds=0.01
                )

            # 不同源 (-1009000000026) 向不同目标 (-1009000000043) 转发 -> 不受阻塞正常完成
            res_other = await ForwardService.forward_channel_messages(
                source_chat="-1009000000026", target_chat="-1009000000043", batch_size=10, delay_seconds=0.01
            )
            assert res_other == 5
            assert "-1009000000043" not in ForwardService._target_locks
        finally:
            release_a.set()
            res_a = await task_a
            assert res_a == 5

    assert "-1009000000052" not in ForwardService._target_locks
    assert ("-1009000000003", "-1009000000052") not in ForwardService._pair_locks


# =========================================================================
# 7. 试转模式 (dry-run) 首批自动暂停测试
# =========================================================================


@pytest.mark.asyncio
async def test_forward_dry_run_auto_pauses_after_first_batch():
    """dry-run: 首个批次完成后自动置 paused 并退出让位 (Aria2 模型)，恢复后继续全量完成"""
    task_id = "dryrun_test"
    active_task_controls[task_id] = {"cancelled": False, "paused": False}
    msgs = [_valid_msg(i) for i in range(1, 26)]  # 25 条, batch_size=10 -> 3 批
    mock_client = _build_mock_client(msgs)

    try:
        with patch("app.core.client_manager.client_manager.userbot", mock_client):
            # 1. 首批试转: 完成第 1 批后触发暂停并抛出 ForwardPausedError 退出协程让出配额
            with pytest.raises(ForwardPausedError):
                await ForwardService.forward_channel_messages(
                    source_chat="-1009000000010",
                    target_chat="-100900037",
                    batch_size=10,
                    delay_seconds=0.01,
                    task_id=task_id,
                    dry_run=True,
                )

            assert active_task_controls.get(task_id, {}).get("paused") is True
            assert mock_client.forward_messages.call_count == 1  # 首批 10 条

            # 2. 模拟用户恢复任务: 从消息 11 继续全量转发
            active_task_controls[task_id]["paused"] = False
            remaining_count = await ForwardService.forward_channel_messages(
                source_chat="-1009000000010",
                target_chat="-100900037",
                start_id=11,
                end_id=25,
                batch_size=10,
                delay_seconds=0.01,
                task_id=task_id,
                dry_run=False,
            )
            assert remaining_count == 15
            assert mock_client.forward_messages.call_count == 3  # 1 + 2 = 3 批 (全量 25 条)
    finally:
        active_task_controls.pop(task_id, None)


# =========================================================================
# 8. Jitter 伪随机抖动与边界截断测试
# =========================================================================


@pytest.mark.asyncio
async def test_adaptive_pacer_jitter_distribution_and_bounds():
    """测试 AdaptivePacer 的拟人化随机抖动以及 [min_delay, max_delay] 严格钳位"""
    pacer = AdaptivePacer(base_delay=2.0, min_delay=0.5, max_delay=4.0, jitter_range=(0.85, 1.25))

    delays = []
    with patch("asyncio.sleep", new_callable=AsyncMock):
        for _ in range(50):
            d = await pacer.pace()
            delays.append(d)

    # 验证全部在理论抖动范围 [2.0 * 0.85, 2.0 * 1.25] 即 [1.70, 2.50] 之内
    for d in delays:
        assert 1.70 <= d <= 2.50
        assert 0.5 <= d <= 4.0

    # 验证具有随机波动性
    assert max(delays) - min(delays) > 0.1

    # 测试边界钳位截断
    pacer.current_delay = 10.0  # 超过 max_delay
    with patch("asyncio.sleep", new_callable=AsyncMock):
        d = await pacer.pace()
        assert d <= 4.0  # 钳位在 max_delay

    pacer.current_delay = 0.1  # 小于 min_delay
    with patch("asyncio.sleep", new_callable=AsyncMock):
        d = await pacer.pace()
        assert d >= 0.5  # 钳位在 min_delay


# =========================================================================
# 9. 默认静音转发 (silent=True) 测试
# =========================================================================


@pytest.mark.asyncio
async def test_forward_silent_param_passed_to_telethon():
    """测试调用 userbot.forward_messages 时显式携带 silent=True"""
    msgs = [_valid_msg(i) for i in range(1, 4)]
    mock_client = _build_mock_client(msgs)

    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        count = await ForwardService.forward_channel_messages(
            source_chat="-1009000000010", target_chat="-100900037", batch_size=10, delay_seconds=0.01
        )
    assert count == 3
    assert mock_client.forward_messages.call_count == 1
    assert mock_client.forward_messages.call_args.kwargs.get("silent") is True


# =========================================================================
# 10. Album-Aware 聚合机制测试
# =========================================================================


def _album_msg(i, grouped_id):
    return MagicMock(
        id=i,
        grouped_id=grouped_id,
        empty=False,
        action=None,
        media=MessageMediaPhoto(),
        message=f"Album Msg {i}",
        restriction_reason=None,
    )


@pytest.mark.asyncio
async def test_album_aware_batching_prevent_cross_batch_tearing():
    """
    相册跨批次对齐:
    独立消息 1~8 (8条) + 相册 9~12 (4条, grouped_id=100)
    batch_size=10
    预期:
    - 独立消息 1~8 在 Batch 1 提前提交 (8 条)
    - 相册消息 9~12 在 Batch 2 完整原子提交 (4 条)
    - 相册未被切分成 [9, 10] 和 [11, 12]
    """
    msgs = [_valid_msg(i) for i in range(1, 9)] + [_album_msg(i, 100) for i in range(9, 13)]
    mock_client = _build_mock_client(msgs)

    forwarded_batches = []

    async def capture_forward(entity, messages, from_peer, **kwargs):
        forwarded_batches.append(list(messages))
        return [MagicMock(id=5000 + mid) for mid in messages]

    mock_client.forward_messages = AsyncMock(side_effect=capture_forward)

    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        count = await ForwardService.forward_channel_messages(
            source_chat="-1009000000010", target_chat="-100900037", batch_size=10, delay_seconds=0.01
        )

    assert count == 12
    assert len(forwarded_batches) == 2
    assert forwarded_batches[0] == [1, 2, 3, 4, 5, 6, 7, 8]
    assert forwarded_batches[1] == [9, 10, 11, 12]


@pytest.mark.asyncio
async def test_album_aware_oversized_album_atomic_delivery():
    """
    超大相册 (10 张图, grouped_id=200) 与较小 batch_size (5)
    预期: 10 张图完整收集并在 1 次 RPC 中原子发送
    """
    msgs = [_album_msg(i, 200) for i in range(1, 11)]
    mock_client = _build_mock_client(msgs)

    forwarded_batches = []

    async def capture_forward(entity, messages, from_peer, **kwargs):
        forwarded_batches.append(list(messages))
        return [MagicMock(id=5000 + mid) for mid in messages]

    mock_client.forward_messages = AsyncMock(side_effect=capture_forward)

    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        count = await ForwardService.forward_channel_messages(
            source_chat="-1009000000010", target_chat="-100900037", batch_size=5, delay_seconds=0.01
        )

    assert count == 10
    assert len(forwarded_batches) == 1
    assert forwarded_batches[0] == list(range(1, 11))


@pytest.mark.asyncio
async def test_album_aware_with_filtered_dead_message_inside():
    """
    相册中夹杂被过滤的死消息:
    相册 ID 1, 2, 3 (grouped_id=300), 其中 ID 2 为 TOS 死消息 (terms)
    预期:
    - ID 2 被 MessagePureFilter 过滤跳过，记录入 forward_skips
    - ID 1, 3 仍作为同一相册原子提交 (1 次 RPC: [1, 3])
    """
    dead_reason = RestrictionReason(platform="all", reason="terms", text="TOS removed")
    msg_dead = MagicMock(
        id=2, grouped_id=300, empty=False, action=None, media=None, message="Dead msg", restriction_reason=[dead_reason]
    )
    msgs = [_album_msg(1, 300), msg_dead, _album_msg(3, 300)]
    mock_client = _build_mock_client(msgs)

    forwarded_batches = []

    async def capture_forward(entity, messages, from_peer, **kwargs):
        forwarded_batches.append(list(messages))
        return [MagicMock(id=5000 + mid) for mid in messages]

    mock_client.forward_messages = AsyncMock(side_effect=capture_forward)

    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        count = await ForwardService.forward_channel_messages(
            source_chat="-1009000000010", target_chat="-100900037", batch_size=10, delay_seconds=0.01
        )

    assert count == 2
    assert len(forwarded_batches) == 1
    assert forwarded_batches[0] == [1, 3]

    conn = get_db()
    skips = conn.execute("SELECT msg_id, skip_type FROM forward_skips WHERE msg_id = 2").fetchall()
    conn.close()
    assert len(skips) == 1
    assert skips[0]["skip_type"] == "tos_dead_terms"


# ==============================================================================
# Part: test_forward_closure.py
# ==============================================================================
SRC, TGT = -1009000000010, -100900037


def _build_mock_client(msgs):
    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)

    async def mock_iter(*args, **kwargs):
        for m in msgs:
            yield m

    mock_client.iter_messages = mock_iter
    mock_client.get_messages = AsyncMock(return_value=MagicMock(total=len(msgs)))
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=1001))

    async def default_forward(entity, messages, from_peer, **kwargs):
        return [MagicMock(id=5000 + mid) for mid in messages]

    mock_client.forward_messages = AsyncMock(side_effect=default_forward)
    return mock_client


def _valid_msg(i):
    return MagicMock(
        id=i, empty=False, action=None, media=MessageMediaPhoto(), message=f"Msg {i}", restriction_reason=None
    )


@pytest.mark.asyncio
async def test_same_range_second_run_zero_duplicates():
    """同一范围转发两次: 第二次 0 重复消息, final_summary 记录防重跳过"""
    msgs = [_valid_msg(i) for i in range(1, 6)]

    task1, task2 = "closure_run1", "closure_run2"
    TaskQueueRepository.create(task1, "forward_channel", {})
    TaskQueueRepository.create(task2, "forward_channel", {})

    with patch("app.core.client_manager.client_manager.userbot", _build_mock_client(msgs)):
        first = await ForwardService.forward_channel_messages(
            source_chat=str(SRC), target_chat=str(TGT), batch_size=10, delay_seconds=0.01, task_id=task1
        )
    with patch("app.core.client_manager.client_manager.userbot", _build_mock_client(msgs)):
        second = await ForwardService.forward_channel_messages(
            source_chat=str(SRC), target_chat=str(TGT), batch_size=10, delay_seconds=0.01, task_id=task2
        )

    assert first == 5
    assert second == 0  # 第二次 0 重复消息

    summary = (TaskQueueRepository.get_by_id(task2).get("params") or {}).get("final_summary") or {}
    assert summary.get("dedup_skipped") == 5
    assert summary.get("forwarded") == 0
    logs2 = [l["message"] for l in TaskQueueRepository.get_task_logs(task2, 0, 1000)[0]]
    assert any("防重" in m for m in logs2)


def test_compute_resume_start_gap_and_covered():
    """中断续转: 部分覆盖 -> gap 起点; 全覆盖 -> covered"""
    # 模拟首批只完成 ID 1~2 (覆盖区间 [1,2])
    ForwardLedgerRepository.commit_batch_records(
        task_id=None,
        source_chat=str(SRC),
        target_chat=str(TGT),
        mappings=[(1, 101), (2, 102)],
        skips=[],
        seg_start=1,
        seg_end=2,
    )
    resume_start, mode = ForwardService.compute_resume_start(str(SRC), str(TGT), 1, 5)
    assert mode == "gap"
    assert resume_start == 3  # 从首个未覆盖区间左端续转

    # 补齐 3~5 后全覆盖
    ForwardLedgerRepository.commit_batch_records(
        task_id=None,
        source_chat=str(SRC),
        target_chat=str(TGT),
        mappings=[(3, 103), (4, 104), (5, 105)],
        skips=[],
        seg_start=3,
        seg_end=5,
    )
    resume_start2, mode2 = ForwardService.compute_resume_start(str(SRC), str(TGT), 1, 5)
    assert mode2 == "covered"
    assert resume_start2 is None


def test_compute_resume_start_full_retransfer_ignores_coverage():
    """强制全量重转: 不采信既有覆盖, 一律按原始起点 fallback (重转请求不能被covered吞掉)"""
    ForwardLedgerRepository.commit_batch_records(
        task_id=None,
        source_chat=str(SRC),
        target_chat=str(TGT),
        mappings=[(1, 101), (2, 102)],
        skips=[],
        seg_start=1,
        seg_end=5,
    )
    resume_start, mode = ForwardService.compute_resume_start(str(SRC), str(TGT), None, 5, full_retransfer=True)
    assert mode == "fallback"
    assert resume_start is None  # 原始起点 (start_id 未指定)


def test_compute_resume_start_lower_bound_is_requested_range():
    """start 缺省时 w_start 取请求范围下界 (ID 1): 早于首个覆盖段的消息不得被误判 covered"""
    # 既有覆盖只到 [3,5]; 任务请求 start=None, end=5 → 请求范围 [1,5]
    ForwardLedgerRepository.commit_batch_records(
        task_id=None,
        source_chat=str(SRC),
        target_chat=str(TGT),
        mappings=[(3, 103), (4, 104), (5, 105)],
        skips=[],
        seg_start=3,
        seg_end=5,
    )
    resume_start, mode = ForwardService.compute_resume_start(str(SRC), str(TGT), None, 5)
    assert mode == "gap"
    assert resume_start == 1  # 旧实现以 coverage 下界 (3) 近似, 会漏掉 ID 1~2


@pytest.mark.asyncio
async def test_forward_partial_none_response_remains_uncovered():
    """Missing responses preserve confirmed mappings without permanent skips."""
    SRC2, TGT2 = -1009000000010, -100900037
    msgs = [_valid_msg(1), _valid_msg(2), _valid_msg(3)]
    mock_client = _build_mock_client(msgs)

    async def partial_forward(entity, messages, from_peer, **kwargs):
        # 3 条请求, ID 2 无效 → 原位返回 None
        return [MagicMock(id=5000 + m) if m != 2 else None for m in messages]

    mock_client.forward_messages = AsyncMock(side_effect=partial_forward)

    with (
        patch("app.core.client_manager.client_manager.userbot", mock_client),
        pytest.raises(RuntimeError, match="未返回消息映射"),
    ):
        await ForwardService.forward_channel_messages(
            source_chat=str(SRC2), target_chat=str(TGT2), batch_size=10, delay_seconds=0.01
        )

    conn = get_db()
    bad_rows = conn.execute(
        "SELECT msg_id FROM forward_skips WHERE source_chat = ? AND skip_type = 'bad_id'", (str(SRC2),)
    ).fetchall()
    conn.close()
    assert not bad_rows
    assert ForwardLedgerRepository.get_processed_ids(str(SRC2), str(TGT2), [1, 2, 3]) == {1, 3}
    assert ForwardLedgerRepository.get_coverage_segments(str(SRC2), str(TGT2)) == [(1, 1)]


@pytest.mark.asyncio
async def test_forward_retry_exhaustion_raises_not_bad_id():
    """瞬态错误重试耗尽: 上抛转任务失败, 绝不把有效消息永久记为 bad_id/covered"""
    SRC2, TGT2 = -1009000000010, -100900037
    msgs = [_valid_msg(1)]
    mock_client = _build_mock_client(msgs)

    async def always_busy(entity, messages, from_peer, **kwargs):
        raise RPCError(request=None, message="WorkerBusy: too busy", code=420)

    mock_client.forward_messages = AsyncMock(side_effect=always_busy)

    with (
        patch("app.core.client_manager.client_manager.userbot", mock_client),
        patch("app.services.forward_service.asyncio.sleep", new_callable=AsyncMock),
        pytest.raises(RuntimeError, match="重试耗尽"),
    ):
        await ForwardService.forward_channel_messages(
            source_chat=str(SRC2), target_chat=str(TGT2), batch_size=10, delay_seconds=0.01
        )

    conn = get_db()
    bad_rows = conn.execute(
        "SELECT msg_id FROM forward_skips WHERE source_chat = ? AND skip_type = 'bad_id'", (str(SRC2),)
    ).fetchall()
    cov_rows = conn.execute(
        "SELECT seg_start, seg_end FROM forward_coverage WHERE source_chat = ? AND target_chat = ?",
        (str(SRC2), str(TGT2)),
    ).fetchall()
    conn.close()
    assert bad_rows == []
    assert cov_rows == []  # 未发送 ID 保持未覆盖


@pytest.mark.asyncio
async def test_forward_channel_wide_rpc_error_propagates():
    """频道级权限错误 (ChatWriteForbiddenError) 按类型拦截: 直接上抛, 不落入 bad_id 隔离"""
    SRC2, TGT2 = -1009000000010, -100900037
    msgs = [_valid_msg(i) for i in range(1, 4)]
    mock_client = _build_mock_client(msgs)

    async def forbidden(entity, messages, from_peer, **kwargs):
        raise ChatWriteForbiddenError(request=None)

    mock_client.forward_messages = AsyncMock(side_effect=forbidden)

    with patch("app.core.client_manager.client_manager.userbot", mock_client), pytest.raises(ChatWriteForbiddenError):
        await ForwardService.forward_channel_messages(
            source_chat=str(SRC2), target_chat=str(TGT2), batch_size=10, delay_seconds=0.01
        )


def test_reject_same_peer_pair():
    """源=目标同会话 (含别名解析后同 peer) 直接拒绝, 防自我复制循环; 不支持比较的形态跳过"""
    from telethon.tl.types import PeerChannel

    with pytest.raises(RuntimeError, match="同一会话"):
        _reject_same_peer_pair(PeerChannel(123), PeerChannel(123))
    _reject_same_peer_pair(PeerChannel(123), PeerChannel(456))  # 不同 peer 不抛
    _reject_same_peer_pair(MagicMock(), MagicMock())  # 实体形态不支持比较 → 跳过不抛


@pytest.mark.asyncio
async def test_commit_batch_album_buffer_coverage_clamp():
    """相册缓冲未发送时覆盖段上界压缩至缓冲区之前: [1,2]已结算+skip 5 不得把缓冲 [3,4] 标 covered"""
    SRC2, TGT2 = -1009000000010, -100900037
    state = _ForwardRunState(
        task_id=None, parsed_source=SRC2, parsed_target=TGT2, batch_size=3, full_retransfer=False, dry_run=False
    )
    state.current_album_buffer = [3, 4]
    state.pending_skips = [(5, "empty_message", None, None)]
    ctx = _ForwardRunCtx(userbot=None, source_entity=None, target_entity=None, pacer=AdaptivePacer(base_delay=0))

    # 预写 1/2 的防重记录 → 批前剔除走防重-only 路径 (零 RPC), 复现
    ForwardHistoryRepository.save_forward_batch([(None, str(SRC2), 1, str(TGT2), 9001)])
    ForwardHistoryRepository.save_forward_batch([(None, str(SRC2), 2, str(TGT2), 9002)])

    await _commit_batch(state, ctx, [1, 2])

    conn = get_db()
    cov = conn.execute(
        "SELECT seg_start, seg_end FROM forward_coverage WHERE source_chat = ? AND target_chat = ?",
        (str(SRC2), str(TGT2)),
    ).fetchall()
    skips = conn.execute(
        "SELECT msg_id FROM forward_skips WHERE source_chat = ? AND skip_type = 'empty_message'", (str(SRC2),)
    ).fetchall()
    conn.close()
    # 旧实现区间 [1,5] 会把未发送的相册 3/4 标成已覆盖
    assert [(r["seg_start"], r["seg_end"]) for r in cov] == [(1, 2)]
    assert [r["msg_id"] for r in skips] == [5]  # 越界 skip 单独留痕, 防重按逐条记录兜底


@pytest.mark.asyncio
async def test_flush_partial_ledger_persists_left_subbatch():
    """二分左子批结算先落库: 右子批中断不再丢失已成功映射, 未发送余量不进覆盖段"""
    SRC2, TGT2 = -1009000000010, -100900037
    state = _ForwardRunState(
        task_id=None, parsed_source=SRC2, parsed_target=TGT2, batch_size=3, full_retransfer=False, dry_run=False
    )
    state.pending_mappings = [(1, 101)]
    await _flush_partial_ledger(state, [2])

    conn = get_db()
    hist = conn.execute(
        "SELECT source_msg_id FROM forward_history WHERE source_chat = ? AND target_chat = ?", (str(SRC2), str(TGT2))
    ).fetchall()
    bad = conn.execute(
        "SELECT msg_id FROM forward_skips WHERE source_chat = ? AND skip_type = 'bad_id'", (str(SRC2),)
    ).fetchall()
    cov = conn.execute(
        "SELECT seg_start, seg_end FROM forward_coverage WHERE source_chat = ? AND target_chat = ?",
        (str(SRC2), str(TGT2)),
    ).fetchall()
    conn.close()
    assert [r["source_msg_id"] for r in hist] == [1]
    assert [r["msg_id"] for r in bad] == [2]
    assert [(r["seg_start"], r["seg_end"]) for r in cov] == [(1, 2)]
    assert state.pending_mappings == []
    assert state.pending_skips == []


@pytest.mark.asyncio
async def test_scan_upper_bound_snapshot_applied_when_end_id_unset():
    """无 end_id 时以启动快照最新消息 ID 为扫描上界, 运行期新增消息不卷入本次任务"""
    msgs = [_valid_msg(i) for i in range(1, 4)]
    mock_client = _build_mock_client(msgs)
    # 上界快照 (limit=1) 返回可索引的最新消息; run_total (limit=0) 返回 total 结构
    mock_client.get_messages = AsyncMock(
        side_effect=lambda entity, limit=0, **kw: [MagicMock(id=3)] if limit == 1 else MagicMock(total=len(msgs))
    )
    captured: dict = {}

    async def mock_iter(entity, **kwargs):
        captured.update(kwargs)
        for m in msgs:
            yield m

    mock_client.iter_messages = mock_iter

    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        count = await ForwardService.forward_channel_messages(
            source_chat=str(SRC), target_chat=str(TGT), batch_size=10, delay_seconds=0.01
        )
    assert count == 3
    assert captured.get("max_id") == 4  # _build_iter_range_kwargs: end_id + 1


def test_orphan_cleanup_per_type_messages():
    """服务重启孤儿清理: 按任务类型给出差异化恢复提示 (不再对转发任务误导 .part 断点)"""
    TaskQueueRepository.create("orphan_dl", "download_range", {"channel": "c", "start_id": 1, "end_id": 2})
    TaskQueueRepository.create("orphan_fw", "forward_channel", {"source_chat": "s", "target_chat": "t"})
    TaskQueueRepository.create("orphan_pub", "publish_folder", {"folder_path": "f", "target_channel": "t"})
    for tid in ("orphan_dl", "orphan_fw", "orphan_pub"):
        TaskQueueRepository.update_status(tid, "running")

    # 孤儿恢复统一走调度器实现 (P1: 仓储重复版 cleanup_orphaned_tasks 已删, 语义对齐 recover_orphaned_tasks)
    from app.core.scheduler import task_scheduler
    from app.db.account_repository import SystemSettingsRepository

    SystemSettingsRepository.set("auto_resume_tasks_on_startup", "true")
    try:
        task_scheduler.recover_orphaned_tasks()
    finally:
        SystemSettingsRepository.set("auto_resume_tasks_on_startup", "false")

    dl_logs = [l["message"] for l in TaskQueueRepository.get_task_logs("orphan_dl", 0, 200)[0]]
    fw_logs = [l["message"] for l in TaskQueueRepository.get_task_logs("orphan_fw", 0, 200)[0]]
    pub_logs = [l["message"] for l in TaskQueueRepository.get_task_logs("orphan_pub", 0, 200)[0]]

    assert TaskQueueRepository.get_by_id("orphan_dl")["status"] == "queued"
    assert TaskQueueRepository.get_by_id("orphan_fw")["status"] == "queued"
    assert TaskQueueRepository.get_by_id("orphan_pub")["status"] == "queued"

    assert any(".part" in m for m in dl_logs)
    assert any("账本" in m for m in fw_logs)  # scheduler 版 hint: "转发账本已记录进度，将自动跳过已转发消息"
    assert not any(".part" in m for m in fw_logs)  # 转发任务不再出现误导性 .part 文案
    assert any("跳过" in m for m in pub_logs)


# =========================================================================
# 进度条比例修复测试: 分母=任务范围消息数 (而非整频道总数), 分子含防重跳过
# =========================================================================


@pytest.mark.asyncio
async def test_progress_ratio_uses_range_span_not_channel_total():
    """范围任务: 进度分母 = end-start+1, 而非整频道消息总数"""
    msgs = [_valid_msg(i) for i in range(1, 6)]
    mock_client = _build_mock_client(msgs)
    # 频道真实总数 999 >> 任务范围 1~5
    mock_client.get_messages = AsyncMock(return_value=MagicMock(total=999))

    task_id = "ratio_span"
    TaskQueueRepository.create(task_id, "forward_channel", {})
    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        count = await ForwardService.forward_channel_messages(
            source_chat=str(SRC),
            target_chat=str(TGT),
            start_id=1,
            end_id=5,
            batch_size=10,
            delay_seconds=0.01,
            task_id=task_id,
        )

    assert count == 5
    task = TaskQueueRepository.get_by_id(task_id)
    assert task["total_items"] == 5  # 分母是范围条目数 5, 而非频道总数 999
    assert task["processed_items"] == 5
    assert task["total_bytes"] == 0  # 物理量纲拆分: 纯转发任务无字节量纲
    assert task["progress_bytes"] == 0


@pytest.mark.asyncio
async def test_progress_counts_dedup_skipped():
    """重跑同范围: 防重跳过的消息计入分子, 进度不再贴 0 后跳 100"""
    ForwardHistoryRepository.save_forward_batch(
        [
            (None, str(SRC), 1, str(TGT), 9001),
            (None, str(SRC), 2, str(TGT), 9002),
        ]
    )
    msgs = [_valid_msg(i) for i in range(1, 6)]
    mock_client = _build_mock_client(msgs)

    task_id = "ratio_dedup"
    TaskQueueRepository.create(task_id, "forward_channel", {})
    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        count = await ForwardService.forward_channel_messages(
            source_chat=str(SRC),
            target_chat=str(TGT),
            start_id=1,
            end_id=5,
            batch_size=10,
            delay_seconds=0.01,
            task_id=task_id,
        )

    assert count == 3  # 实际转发 3 条
    task = TaskQueueRepository.get_by_id(task_id)
    assert task["processed_items"] == 5  # 分子 = 3 转发 + 2 防重跳过
    assert task["total_items"] == 5
    assert task["progress_bytes"] == 0
    assert task["total_bytes"] == 0


@pytest.mark.asyncio
async def test_progress_denominator_on_resume_is_remaining_span():
    """resume 快路径 (start 收窄到 gap 左端): 分母 = 剩余范围消息数"""
    ForwardLedgerRepository.commit_batch_records(
        task_id=None,
        source_chat=str(SRC),
        target_chat=str(TGT),
        mappings=[(1, 101), (2, 102)],
        skips=[],
        seg_start=1,
        seg_end=2,
    )
    msgs = [_valid_msg(i) for i in range(3, 6)]
    mock_client = _build_mock_client(msgs)

    task_id = "ratio_resume"
    TaskQueueRepository.create(task_id, "forward_channel", {})
    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        count = await ForwardService.forward_channel_messages(
            source_chat=str(SRC),
            target_chat=str(TGT),
            start_id=3,
            end_id=5,
            batch_size=10,
            delay_seconds=0.01,
            task_id=task_id,
        )

    assert count == 3
    task = TaskQueueRepository.get_by_id(task_id)
    assert task["total_items"] == 3  # 剩余 gap 3~5 的范围条目数
    assert task["processed_items"] == 3
    assert task["total_bytes"] == 0
    assert task["progress_bytes"] == 0


@pytest.mark.asyncio
async def test_progress_only_start_uses_newest_message_bound():
    """只有起点无终点: 用最新一条消息 ID 作为范围上界计算分母"""
    msgs = [_valid_msg(i) for i in (8, 9, 10)]
    mock_client = _build_mock_client(msgs)

    async def gm(entity, limit=0, ids=None, **kwargs):
        if limit == 0:
            return MagicMock(total=999)
        if limit == 1:
            return [MagicMock(id=10)]  # 频道最新消息 ID = 10
        return []

    mock_client.get_messages = AsyncMock(side_effect=gm)

    task_id = "ratio_start_only"
    TaskQueueRepository.create(task_id, "forward_channel", {})
    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        count = await ForwardService.forward_channel_messages(
            source_chat=str(SRC), target_chat=str(TGT), start_id=8, batch_size=10, delay_seconds=0.01, task_id=task_id
        )

    assert count == 3
    task = TaskQueueRepository.get_by_id(task_id)
    assert task["total_items"] == 3  # 10 - 8 + 1, 而非频道总数 999
    assert task["processed_items"] == 3
    assert task["total_bytes"] == 0
    assert task["progress_bytes"] == 0


# ==============================================================================
# Part: test_transit_bridge.py
# ==============================================================================
CHUNK = 512 * 1024


def _create_mock_message(
    msg_id: int = 100, chat_id: int = -1009000000010, file_size: int = 50 * 1024 * 1024, noforwards: bool = False
):
    doc = MagicMock()
    doc.size = file_size
    doc.dc_id = 2
    doc.attributes = [DocumentAttributeFilename(file_name="test_video.mp4")]
    doc.mime_type = "video/mp4"

    media = MagicMock(spec=MessageMediaDocument)
    media.document = doc

    msg = MagicMock()
    msg.id = msg_id
    msg.chat_id = chat_id
    msg.media = media
    msg.grouped_id = None
    msg.noforwards = noforwards
    msg.input_chat = MagicMock()
    return msg


def _create_mock_bot(bot_id: int, session_name: str, is_ready: bool = True):
    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)
    bot = TelegramBotClient(
        bot_id=bot_id,
        bot_token=f"token_{bot_id}",
        session_name=session_name,
        api_id=123456,
        api_hash="hash",
        is_active=True,
        bot_name=f"Bot #{bot_id}",
    )
    bot.client = mock_client
    bot.status = "ready" if is_ready else "unauthenticated"
    return bot


@pytest.mark.asyncio
async def test_heuristic_gating_conditions(monkeypatch):
    """测试启发式准入门控各边界条件 (开关、未配置、无 Bot、防环路、noforwards、文件体积)"""
    # 1. 模拟系统设置
    settings_dict = {
        "enable_transit_acceleration": "true",
        "transit_chat_id": "-1009000000052",
        "transit_min_file_size_mb": "20",
    }
    monkeypatch.setattr(SystemSettingsRepository, "get_bool", lambda k, d=False: settings_dict.get(k, "true") == "true")
    monkeypatch.setattr(SystemSettingsRepository, "get", lambda k, d=None: settings_dict.get(k, d))
    monkeypatch.setattr(SystemSettingsRepository, "get_int", lambda k, d=0: int(settings_dict.get(k, "20")))

    # 模拟 Bot 池
    bot1 = _create_mock_bot(1, "bot_pool_1", is_ready=True)
    monkeypatch.setattr(client_manager, "get_all_bots", lambda: [bot1])

    msg_large = _create_mock_message(msg_id=1, file_size=30 * 1024 * 1024)
    msg_small = _create_mock_message(msg_id=2, file_size=10 * 1024 * 1024)
    msg_noforwards = _create_mock_message(msg_id=3, file_size=30 * 1024 * 1024, noforwards=True)

    target_chat = MagicMock(noforwards=False)

    # 正常满血场景 -> True
    capable, reason = TransitBridgeService.is_transit_capable(None, target_chat, [msg_large])
    assert capable is True
    assert "满足中转借壳加速条件" in reason

    # 条件 1: 开关关闭 -> False
    settings_dict["enable_transit_acceleration"] = "false"
    capable, reason = TransitBridgeService.is_transit_capable(None, target_chat, [msg_large])
    assert capable is False
    assert "未启用" in reason
    settings_dict["enable_transit_acceleration"] = "true"

    # 条件 2: 中转群 ID 为空 -> False
    settings_dict["transit_chat_id"] = ""
    capable, reason = TransitBridgeService.is_transit_capable(None, target_chat, [msg_large])
    assert capable is False
    assert "未配置" in reason
    settings_dict["transit_chat_id"] = "-1009000000052"

    # 条件 3: 目标群即中转群 (防环路) -> False
    capable, reason = TransitBridgeService.is_transit_capable(None, "-1009000000052", [msg_large])
    assert capable is False
    assert "目标频道与中转群相同" in reason

    # 条件 4: 辅助 Bot 池无可用 Bot -> False
    monkeypatch.setattr(client_manager, "get_all_bots", list)
    capable, reason = TransitBridgeService.is_transit_capable(None, target_chat, [msg_large])
    assert capable is False
    assert "Bot 池暂无就绪" in reason
    monkeypatch.setattr(client_manager, "get_all_bots", lambda: [bot1])

    # 条件 5: noforwards=True -> False
    target_chat_restricted = MagicMock(noforwards=True)
    capable, reason = TransitBridgeService.is_transit_capable(None, target_chat_restricted, [msg_large])
    assert capable is False
    assert "防转发保护" in reason

    capable, reason = TransitBridgeService.is_transit_capable(None, target_chat, [msg_noforwards])
    assert capable is False
    assert "防转发保护" in reason

    # 条件 6: 文件体积 < 20MB -> False
    capable, reason = TransitBridgeService.is_transit_capable(None, target_chat, [msg_small])
    assert capable is False
    assert "低于中转加速阈值" in reason


@pytest.mark.asyncio
async def test_album_atomic_forward_and_cleanup(monkeypatch):
    """测试 Album 媒体列表原子一次性转发，以及 finally 中严格调用 delete_messages 强保洁"""
    monkeypatch.setattr(SystemSettingsRepository, "get_bool", lambda k, d=False: True)
    monkeypatch.setattr(SystemSettingsRepository, "get", lambda k, d=None: "-1009000000052")
    monkeypatch.setattr(SystemSettingsRepository, "get_int", lambda k, d=0: 20)

    bot1 = _create_mock_bot(1, "bot_pool_1", is_ready=True)
    bot2 = _create_mock_bot(2, "bot_pool_2", is_ready=True)
    monkeypatch.setattr(client_manager, "get_all_bots", lambda: [bot1, bot2])

    # 模拟主 Userbot 客户端
    mock_primary_client = AsyncMock()
    mock_transit_entity = MagicMock(id=999999999)

    async def fake_resolve_entity(client, chat_id):
        return mock_transit_entity

    monkeypatch.setattr("app.services.forward_service.ForwardService.resolve_entity", fake_resolve_entity)

    msg1 = _create_mock_message(msg_id=101, file_size=25 * 1024 * 1024)
    msg2 = _create_mock_message(msg_id=102, file_size=30 * 1024 * 1024)

    fwd_msg1 = MagicMock(id=801, media=msg1.media)
    fwd_msg2 = MagicMock(id=802, media=msg2.media)
    mock_primary_client.forward_messages.return_value = [fwd_msg1, fwd_msg2]

    # 进入上下文
    async with TransitBridgeService.transit_bridge_context(
        primary_client=mock_primary_client, source_chat="-1009000000010", messages=[msg1, msg2], task_id="test_task_1"
    ) as bridge:
        assert bridge is not None
        assert bridge.transit_chat == mock_transit_entity
        assert len(bridge.forwarded_messages) == 2
        # 客户端池为 1 Userbot + 2 Bots = 3 客户端
        assert len(bridge.clients) == 3
        assert bridge.ready_bots_count == 2
        assert bridge.source_to_forwarded[101] == fwd_msg1
        assert bridge.source_to_forwarded[102] == fwd_msg2

        # 验证原子转发时传入了全部消息 ID 列表
        mock_primary_client.forward_messages.assert_called_once_with(
            mock_transit_entity, [101, 102], from_peer=-1009000000010
        )

    # 退出上下文后：断言 delete_messages 强保洁被调用
    mock_primary_client.delete_messages.assert_called_once_with(mock_transit_entity, [801, 802])


@pytest.mark.asyncio
async def test_transit_partial_forward_failure_preserves_source_pairing(monkeypatch):
    """部分转发失败 (Telethon 原位返回 None): 先按位配对再剔除, 后续成员不得错配到错误媒体"""
    monkeypatch.setattr(SystemSettingsRepository, "get_bool", lambda k, d=False: True)
    monkeypatch.setattr(SystemSettingsRepository, "get", lambda k, d=None: "-1009000000052")
    monkeypatch.setattr(SystemSettingsRepository, "get_int", lambda k, d=0: 20)

    bot1 = _create_mock_bot(1, "bot_pool_1", is_ready=True)
    monkeypatch.setattr(client_manager, "get_all_bots", lambda: [bot1])

    mock_primary_client = AsyncMock()
    mock_transit_entity = MagicMock(id=999999999)
    monkeypatch.setattr(
        "app.services.forward_service.ForwardService.resolve_entity", AsyncMock(return_value=mock_transit_entity)
    )

    msg_a = _create_mock_message(msg_id=101, file_size=25 * 1024 * 1024)
    msg_b = _create_mock_message(msg_id=102, file_size=30 * 1024 * 1024)
    msg_c = _create_mock_message(msg_id=103, file_size=25 * 1024 * 1024)

    fwd_a = MagicMock(id=801, media=msg_a.media)
    fwd_c = MagicMock(id=803, media=msg_c.media)
    # 源 [A,B,C] → 结果 [fwdA, None, fwdC] (B 无效, 原位 None)
    mock_primary_client.forward_messages.return_value = [fwd_a, None, fwd_c]

    async with TransitBridgeService.transit_bridge_context(
        primary_client=mock_primary_client, source_chat="-1009000000010", messages=[msg_a, msg_b, msg_c]
    ) as bridge:
        assert bridge is not None
        # 旧实现先过滤 None 再 zip: B→fwdC (错配下载 C 的媒体), C 被丢弃
        assert bridge.source_to_forwarded[101] == fwd_a
        assert 102 not in bridge.source_to_forwarded
        assert bridge.source_to_forwarded[103] == fwd_c
        assert len(bridge.forwarded_messages) == 2


@pytest.mark.asyncio
async def test_cleanup_guaranteed_on_download_error_or_cancel(monkeypatch):
    """测试下载中途发生异常或中断时，中转群消息仍 100% 被清理 (零残留保障)"""
    monkeypatch.setattr(SystemSettingsRepository, "get_bool", lambda k, d=False: True)
    monkeypatch.setattr(SystemSettingsRepository, "get", lambda k, d=None: "-1009000000052")
    monkeypatch.setattr(SystemSettingsRepository, "get_int", lambda k, d=0: 20)

    bot1 = _create_mock_bot(1, "bot_pool_1", is_ready=True)
    monkeypatch.setattr(client_manager, "get_all_bots", lambda: [bot1])

    mock_primary_client = AsyncMock()
    mock_transit_entity = MagicMock(id=999999999)
    monkeypatch.setattr(
        "app.services.forward_service.ForwardService.resolve_entity", AsyncMock(return_value=mock_transit_entity)
    )

    msg1 = _create_mock_message(msg_id=201)
    fwd_msg1 = MagicMock(id=888, media=msg1.media)
    mock_primary_client.forward_messages.return_value = [fwd_msg1]

    # 模拟在 context 内部抛出异常
    entered_bridge = None

    async def _run_and_fail():
        nonlocal entered_bridge
        async with TransitBridgeService.transit_bridge_context(
            primary_client=mock_primary_client, source_chat="-1009000000010", messages=[msg1]
        ) as bridge:
            entered_bridge = bridge
            raise RuntimeError("Simulated Network Error")

    with pytest.raises(RuntimeError, match="Simulated Network Error"):
        await _run_and_fail()
    assert entered_bridge is not None

    # 尽管抛出异常，finally 保洁仍然执行
    mock_primary_client.delete_messages.assert_called_once_with(mock_transit_entity, [888])


@pytest.mark.asyncio
async def test_forward_restricted_graceful_fallback(monkeypatch):
    """测试遇到 ChatForwardsRestrictedError 等 Telegram 限制时，优雅捕获并返回 None 进行直连降级"""
    monkeypatch.setattr(SystemSettingsRepository, "get_bool", lambda k, d=False: True)
    monkeypatch.setattr(SystemSettingsRepository, "get", lambda k, d=None: "-1009000000052")
    monkeypatch.setattr(SystemSettingsRepository, "get_int", lambda k, d=0: 20)

    bot1 = _create_mock_bot(1, "bot_pool_1", is_ready=True)
    monkeypatch.setattr(client_manager, "get_all_bots", lambda: [bot1])

    mock_primary_client = AsyncMock()
    mock_transit_entity = MagicMock(id=999999999)
    monkeypatch.setattr(
        "app.services.forward_service.ForwardService.resolve_entity", AsyncMock(return_value=mock_transit_entity)
    )

    msg1 = _create_mock_message(msg_id=301)

    # 模拟转发时抛出 ChatForwardsRestrictedError
    class ChatForwardsRestrictedError(Exception):
        pass

    mock_primary_client.forward_messages.side_effect = ChatForwardsRestrictedError("CHAT_FORWARDS_RESTRICTED")

    async with TransitBridgeService.transit_bridge_context(
        primary_client=mock_primary_client, source_chat="-1009000000010", messages=[msg1]
    ) as bridge:
        # 捕获异常并不向外抛出，而是返回 None 引导自动降级
        assert bridge is None

    # 未产生转发消息，不调用删除
    mock_primary_client.delete_messages.assert_not_called()


@pytest.mark.asyncio
async def test_origin_metadata_preservation(tmp_path, monkeypatch):
    """测试在中转下载模式下，保存路径、防重历史库与断点记录严格使用 origin 原始频道与原始消息 ID"""
    from app.config import settings

    monkeypatch.setattr(settings, "DOWNLOAD_DIR", str(tmp_path))

    # 原始源频道与原始消息 ID
    origin_chat_id = "-1009000000021"
    origin_msg_id = 777
    file_name = "test_origin_preservation.mp4"

    # 中转群临时消息 (ID 9999)
    transit_chat_id = "-1009000000052"
    transit_msg_id = 9999

    msg = _create_mock_message(msg_id=transit_msg_id, chat_id=int(transit_chat_id), file_size=CHUNK)
    doc = msg.media.document
    doc.attributes = [DocumentAttributeFilename(file_name=file_name)]

    mock_client = AsyncMock()
    mock_client.get_messages.return_value = msg
    mock_client.session = MagicMock(dc_id=2, auth_key=b"12345678")
    mock_client.is_connected = MagicMock(return_value=True)

    # 模拟 download_file_parallel 成功返回真实路径 (路径含频道规范化标识 + origin_msg_id)
    expected_save_path = os.path.join(str(tmp_path), f"{origin_chat_id}_{origin_msg_id}_{file_name}")

    async def fake_download_parallel(**kwargs):
        out_path = kwargs["out_file_path"]
        safe_write_bytes(out_path, b"x" * CHUNK)
        return out_path

    monkeypatch.setattr("app.services.downloader_service.download_file_parallel", fake_download_parallel)

    # 记录调用前历史库未记录
    assert not DownloadHistoryRepository.is_downloaded(origin_chat_id, origin_msg_id)

    res_path = await DownloaderService.download_single_message(
        client=[mock_client],
        chat_id=transit_chat_id,
        message=msg,
        task_id="task_meta_test",
        origin_chat_id=origin_chat_id,
        origin_msg_id=origin_msg_id,
    )

    # 1. 验证文件保存路径严格使用 origin_msg_id (777_test_origin_preservation.mp4), 而不是 transit_msg_id (9999)
    assert res_path == expected_save_path
    assert os.path.exists(expected_save_path)
    assert f"{origin_msg_id}_{file_name}" in os.path.basename(res_path)
    assert "9999" not in os.path.basename(res_path)

    # 2. 验证防重历史记录库中落库的键为 origin_chat_id 和 origin_msg_id
    assert DownloadHistoryRepository.is_downloaded(origin_chat_id, origin_msg_id) is True
    assert DownloadHistoryRepository.is_downloaded(transit_chat_id, transit_msg_id) is False

    # 验证底层的 RPC get_messages 依然查询的是中转群临时消息 ID (9999)
    mock_client.get_messages.assert_called()
    called_args = mock_client.get_messages.call_args[1]
    assert called_args.get("ids") == transit_msg_id


@pytest.mark.asyncio
async def test_verify_transit_group_health_report(monkeypatch):
    """测试中转群健康检查探针在不同配置与连通状态下的报告输出"""
    # 场景 1: 未配置 TRANSIT_CHAT_ID
    monkeypatch.setattr(SystemSettingsRepository, "get", lambda k, d=None: "")
    rep = await TransitBridgeService.verify_transit_group_health()
    assert rep["status"] == "unconfigured"
    assert rep["userbot_accessible"] is False

    # 场景 2: 主 Userbot 离线
    monkeypatch.setattr(SystemSettingsRepository, "get", lambda k, d=None: "-1009000000052")
    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=False)
    rep2 = await TransitBridgeService.verify_transit_group_health(primary_client=mock_client)
    assert rep2["status"] == "error"

    # 场景 3: 完全健康 (主 Userbot + 所有 Bot 在群且为管理员)
    mock_client.is_connected = MagicMock(return_value=True)
    mock_client.get_permissions = AsyncMock(
        return_value=MagicMock(post_messages=True, delete_messages=True, is_admin=True)
    )
    mock_entity = MagicMock(title="Transit Group Alpha")

    async def fake_resolve(c, cid, fallback_peer=None):
        return mock_entity

    monkeypatch.setattr("app.services.forward_service.ForwardService.resolve_entity", fake_resolve)

    bot1 = _create_mock_bot(1, "bot_pool_1", is_ready=True)
    bot1.client.get_permissions = AsyncMock(return_value=MagicMock(is_admin=True))
    monkeypatch.setattr(client_manager, "get_all_bots", lambda: [bot1])

    rep3 = await TransitBridgeService.verify_transit_group_health(primary_client=mock_client)
    assert rep3["status"] == "healthy"
    assert rep3["userbot_accessible"] is True
    assert rep3["ready_bots_count"] == 1
    assert rep3["transit_chat_title"] == "Transit Group Alpha"
    assert rep3["bots_status"][0]["in_transit_group"] is True
    assert rep3["bots_status"][0]["is_admin"] is True

    # 场景 4: Bot 尚未加入中转群 (UserNotParticipantError)
    from telethon.errors import UserNotParticipantError

    bot1.client.get_permissions = AsyncMock(side_effect=UserNotParticipantError(None))
    rep4 = await TransitBridgeService.verify_transit_group_health(primary_client=mock_client)
    assert rep4["status"] == "degraded"
    assert rep4["ready_bots_count"] == 0
    assert rep4["bots_status"][0]["in_transit_group"] is False
    assert "尚未加入中转群" in rep4["bots_status"][0]["error"]


# ==============================================================================
# Part: test_session_invalidation.py
# ==============================================================================
SRC, TGT = -1009000000010, -100900037


def _make_unauthorized():
    from telethon.errors import UnauthorizedError

    try:
        return UnauthorizedError(request=None)
    except Exception:
        return RuntimeError("Unauthorized")


def _valid_msg(i):
    return MagicMock(
        id=i, empty=False, action=None, media=MessageMediaPhoto(), message=f"Msg {i}", restriction_reason=None
    )


def test_classifier_session_invalid_errors():
    assert is_session_invalid_error(_make_unauthorized())
    assert is_session_invalid_error(RuntimeError("AUTH_KEY_DUPLICATED: The authorization key has been invalidated"))
    assert is_session_invalid_error(RuntimeError("SessionRevokedError: session revoked by server"))
    assert is_session_invalid_error(RuntimeError("USER_DEACTIVATED"))


def test_classifier_non_session_errors():
    assert not is_session_invalid_error(TimeoutError("request timed out"))
    assert not is_session_invalid_error(RuntimeError("WORKERBUSY: too busy"))
    assert not is_session_invalid_error(RuntimeError("MESSAGE_ID_INVALID"))
    assert not is_session_invalid_error(FloodWaitErrorStub())


class FloodWaitErrorStub(Exception):
    def __str__(self):
        return "FloodWaitError: Too many requests"


@pytest.mark.asyncio
async def test_forward_session_invalid_marks_failed_and_broadcasts(monkeypatch):
    """转发中被登出 -> 任务 failed + 指引 + system_event 广播 + 引擎失效标记"""
    msgs = [_valid_msg(i) for i in range(1, 4)]

    async def unauthorized_impl(entity, messages, from_peer, **kwargs):
        raise _make_unauthorized()

    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)

    async def mock_iter(*args, **kwargs):
        for m in msgs:
            yield m

    mock_client.iter_messages = mock_iter
    mock_client.get_messages = AsyncMock(return_value=MagicMock(total=len(msgs)))
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=1001))
    mock_client.forward_messages = AsyncMock(side_effect=unauthorized_impl)

    task_id = "sess_invalid_1"
    TaskQueueRepository.create(task_id, "forward_channel", {})

    broadcast_mock = AsyncMock()
    monkeypatch.setattr(connection_manager, "broadcast", broadcast_mock)
    seed_default_account(client_manager, client=mock_client, auth_state="AUTHENTICATED")
    try:
        # mock 抛 UnauthorizedError (构造失败时 RuntimeError 兜底), 会话失效路径原样上抛
        from telethon.errors import UnauthorizedError

        with (
            patch(
                "app.services.forward_service._resolve_forward_pair",
                new=AsyncMock(return_value=(mock_client, MagicMock(id=1001), MagicMock(id=1002), 1)),
            ),
            pytest.raises((UnauthorizedError, RuntimeError)),
        ):
            await ForwardService.forward_channel_messages(
                source_chat=str(SRC),
                target_chat=str(TGT),
                batch_size=10,
                delay_seconds=0.01,
                task_id=task_id,
                account_id=2,
            )

        task = TaskQueueRepository.get_by_id(task_id)
        assert task["status"] == "failed"
        fail_logs, _, _ = TaskQueueRepository.get_task_logs(task_id, 0, 500)
        assert any("重新绑定账号" in l["message"] for l in fail_logs)
        assert client_manager.auth_state == "UNAUTHENTICATED"

        sent = [c.args[0] for c in broadcast_mock.call_args_list if c.args]
        system_events = [m for m in sent if m.get("type") == "system_event"]
        assert any(e.get("event") == "session_invalid" and e.get("account_id") == 1 for e in system_events)
        # 会话失效未被当作坏消息隔离 (无 bad_id skip 落库)
        from app.db.forward_repository import ForwardLedgerRepository

        receipt = ForwardLedgerRepository.get_task_receipt(task_id)
        assert receipt["bad_ids"] == []
        assert receipt["forwarded"] == 0
    finally:
        unseed_accounts(client_manager)


@pytest.mark.asyncio
async def test_forward_fallback_userbot_session_invalid_attributes_default_owner(monkeypatch):
    """未 mock _resolve_forward_pair 时, 请求账号回退默认 userbot 后会话失效仍准确归因默认账号而非任务 account_id"""
    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=1001))
    mock_client.get_messages = AsyncMock(return_value=MagicMock(total=1))
    mock_client.iter_messages = MagicMock(side_effect=lambda *a, **kw: _async_error_iter(_make_unauthorized()))

    task_id = "sess_invalid_fallback_1"
    TaskQueueRepository.create(task_id, "forward_channel", {}, account_id=2)

    broadcast_mock = AsyncMock()
    monkeypatch.setattr(connection_manager, "broadcast", broadcast_mock)
    seed_default_account(client_manager, client=mock_client, auth_state="AUTHENTICATED")
    try:
        from telethon.errors import UnauthorizedError

        with pytest.raises((UnauthorizedError, RuntimeError)):
            await ForwardService.forward_channel_messages(
                source_chat=str(SRC),
                target_chat=str(TGT),
                batch_size=10,
                delay_seconds=0.01,
                task_id=task_id,
                account_id=2,
            )

        sent = [c.args[0] for c in broadcast_mock.call_args_list if c.args]
        system_events = [m for m in sent if m.get("type") == "system_event" and m.get("event") == "session_invalid"]
        assert system_events
        assert system_events[0].get("account_id") == 1
        assert TaskQueueRepository.get_account_id(task_id) == 2
    finally:
        unseed_accounts(client_manager)


async def _async_error_iter(exc: BaseException):
    if False:
        yield None
    raise exc


@pytest.mark.asyncio
async def test_report_session_invalid_direct():
    """report_session_invalid 单独调用: 引擎标记失效 + system_event 广播"""
    broadcast_calls = []
    monkey_broadcast = AsyncMock(side_effect=lambda m: broadcast_calls.append(m))
    with patch.object(connection_manager, "broadcast", monkey_broadcast):
        await report_session_invalid(None, _make_unauthorized())
    assert client_manager.auth_state == "UNAUTHENTICATED"
    assert any(m.get("type") == "system_event" and m.get("event") == "session_invalid" for m in broadcast_calls)


async def test_partial_ledger_does_not_cover_unsent_prefix():
    state = _ForwardRunState(
        task_id=None,
        parsed_source=SRC,
        parsed_target=TGT,
        batch_size=4,
        full_retransfer=False,
        dry_run=False,
    )
    state.current_batch_ids = [1, 2, 3, 5]
    state.current_album_buffer = [6, 7]
    state.pending_mappings = [(1, 101), (2, 102)]
    state.pending_skips = [(4, "empty_message", None, None)]
    await _flush_partial_ledger(state, [])
    assert ForwardLedgerRepository.get_coverage_segments(str(SRC), str(TGT)) == [(1, 2)]
    assert ForwardLedgerRepository.get_processed_ids(str(SRC), str(TGT), [1, 2, 3, 4, 5]) == {1, 2, 4}


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (ValueError("Request was unsuccessful 5 time(s)"), "unknown"),
        (RPCError(request=None, message="MESSAGE_ID_INVALID"), "isolate"),
    ],
)
async def test_telethon_retry_exhaustion_classification(exc, expected):
    from app.services.forward_service import _handle_forward_send_failure

    state = _ForwardRunState(None, SRC, TGT, 4, False, False)
    from app.core.scheduler import FatalTaskError

    if expected == "unknown":
        with pytest.raises(FatalTaskError, match="转发结果未知") as caught:
            await _handle_forward_send_failure(state, exc)
        assert caught.value.__cause__ is exc
    else:
        assert await _handle_forward_send_failure(state, exc) == expected


@pytest.mark.asyncio
async def test_non_rpc_value_error_is_never_isolated():
    """未知非 RPC 异常 (非传输瞬态/非 Telethon 重试耗尽) 严禁误标坏消息隔离"""
    from app.services.forward_service import _handle_forward_send_failure

    state = _ForwardRunState(None, SRC, TGT, 4, False, False)
    with pytest.raises(ValueError, match="for another reason"):
        await _handle_forward_send_failure(state, ValueError("Request was unsuccessful for another reason"))


@pytest.mark.asyncio
async def test_forward_failover_reassign_passes_old_account_id(monkeypatch):
    import app.services.forward_service as fs

    state = SimpleNamespace(
        parsed_source="-100111", parsed_target="-100222", task_id="fwd_reassign_old", client_account_id=None
    )
    src_entity, tgt_entity = SimpleNamespace(id=-100111), SimpleNamespace(id=-100222)
    resolve_calls: list = []

    async def fake_resolve(client, ident, fallback_peer=None):
        resolve_calls.append(ident)
        if len(resolve_calls) == 1:
            raise RuntimeError("ChannelPrivateError: cannot access")
        return src_entity if ident == "-100111" else tgt_entity

    reassign_calls: list = []

    def fake_reassign(task_id, old_account_id, new_account_id):
        reassign_calls.append((task_id, old_account_id, new_account_id))

    alt_acc = SimpleNamespace(account_id=7, account_name="alt7")
    monkeypatch.setattr(fs.ForwardService, "resolve_entity", fake_resolve)
    monkeypatch.setattr("app.core.client_manager.is_channel_permission_error", lambda e: True)
    monkeypatch.setattr(client_manager, "find_failover_account", AsyncMock(return_value=alt_acc))
    monkeypatch.setattr(client_manager, "get_account", lambda account_id=None: SimpleNamespace(account_name="old3"))
    monkeypatch.setattr(fs, "_select_forward_userbot", AsyncMock(return_value=("ub2", 7)))
    monkeypatch.setattr("app.core.scheduler.task_scheduler.reassign_task_account", fake_reassign)
    monkeypatch.setattr(TaskQueueRepository, "update_account_id_async", AsyncMock())
    monkeypatch.setattr(fs.connection_manager, "broadcast", AsyncMock())
    monkeypatch.setattr(fs, "_log_task_event", AsyncMock())

    userbot, src, tgt, account_id = await fs._resolve_forward_pair(state, "ub1", "-100111", "-100222", 3, set())

    assert (userbot, account_id) == ("ub2", 7)
    assert reassign_calls == [("fwd_reassign_old", 3, 7)], "old 必须是真实旧账号 3"
    assert src is src_entity and tgt is tgt_entity


def _mk_forward_state(task_id):
    return SimpleNamespace(
        task_id=task_id,
        parsed_source=-1001,
        parsed_target=-1002,
        skipped_dead_count=1,
        bad_isolated_count=0,
        forwarded_count=3,
        dedup_skipped=2,
        filtered_reason_counts={},
        bad_id_list=[],
        run_total=6,
    )


@pytest.mark.asyncio
async def test_forward_completion_guard_skips_when_cancelled(monkeypatch):
    import app.services.forward_service as fs

    state = _mk_forward_state("fwd_cancelled_race")
    update_mock = AsyncMock(return_value=False)
    monkeypatch.setattr(TaskQueueRepository, "update_status_async", update_mock)
    log_mock = AsyncMock()
    monkeypatch.setattr(fs, "_log_task_event", log_mock)
    broadcast_mock = AsyncMock()
    monkeypatch.setattr(fs.connection_manager, "broadcast", broadcast_mock)

    await fs._summarize_forward_completion(state)

    assert update_mock.await_args.args == ("fwd_cancelled_race", "completed", ("running",))
    assert broadcast_mock.await_count == 0, "CAS 拒绝后不得发送完成广播"
    logged = log_mock.await_args.args
    assert "取消或暂停" in str(logged)
    logged_texts = [str(c.args) for c in log_mock.await_args_list]
    assert not any("云端极速转发完成" in t for t in logged_texts), "取消竞态下不得先残留成功庆祝日志"


@pytest.mark.asyncio
async def test_forward_completion_guard_passes_on_running(monkeypatch):
    import app.services.forward_service as fs

    state = _mk_forward_state("fwd_normal_done")
    monkeypatch.setattr(TaskQueueRepository, "update_status_async", AsyncMock(return_value=True))
    monkeypatch.setattr(TaskQueueRepository, "update_progress_async", AsyncMock())
    broadcast_mock = AsyncMock()
    monkeypatch.setattr(fs.connection_manager, "broadcast", broadcast_mock)

    await fs._summarize_forward_completion(state)

    assert broadcast_mock.await_count >= 2, "正常完成路径保持原有完成广播"


@pytest.mark.asyncio
async def test_verify_transit_health_reports_empty_pool_as_degraded(monkeypatch):
    import app.services.forward_service as fs

    monkeypatch.setattr(
        TransitBridgeService,
        "get_transit_settings",
        classmethod(lambda cls: {"enabled": True, "transit_chat_id": "-100555"}),
    )
    stub_client = SimpleNamespace(
        is_connected=lambda: True,
        get_permissions=AsyncMock(return_value=SimpleNamespace(is_creator=True, delete_messages=True)),
    )
    entity = SimpleNamespace(id=-100555, title="transit", broadcast=True)
    monkeypatch.setattr(fs.ForwardService, "resolve_entity", AsyncMock(return_value=entity))
    monkeypatch.setattr(client_manager, "get_all_bots", list)

    result = await TransitBridgeService.verify_transit_group_health(stub_client)

    assert result["status"] == "degraded", "空 Bot 池必须与 is_transit_capable 同判 degraded"
    assert "Bot 池为空" in result["message"]
    assert result["ready_bots_count"] == 0 and result["total_bots_count"] == 0
    assert result["issues"], "空池必须留下可操作 issues"
    assert "Bot 池为空" in result["issues"][0]


@pytest.fixture
def _clean_forward_ledger_and_locks():
    ForwardService._pair_locks.clear()
    ForwardService._target_locks.clear()
    conn = get_db()
    cur = conn.cursor()
    for tbl in ("task_queue", "task_logs", "forward_history", "forward_skips", "forward_coverage", "peer_aliases"):
        cur.execute(f"DELETE FROM {tbl}")
    conn.commit()
    conn.close()
    yield
    ForwardService._pair_locks.clear()
    ForwardService._target_locks.clear()


def _make_channel_peer(cid: int, username: str | None = None) -> Channel:
    return Channel(
        id=cid,
        title=f"Chan {cid}",
        photo=None,
        date=datetime(2026, 1, 1, tzinfo=UTC),
        username=username,
    )


@pytest.mark.asyncio
@pytest.mark.usefixtures("_clean_forward_ledger_and_locks")
@pytest.mark.parametrize(
    ("transport_exc_factory", "expected_calls"),
    [
        (lambda: ConnectionError("Cannot send requests while disconnected"), 4),
        (TimeoutError, 1),
        (lambda: OSError("Connection reset by peer"), 1),
    ],
)
async def test_forward_transport_errors_never_isolated_as_bad_id(monkeypatch, transport_exc_factory, expected_calls):
    monkeypatch.setattr("app.services.forward_service.asyncio.sleep", AsyncMock())
    src, tgt = "-1009000000010", "-1009000000020"
    msgs = [_valid_msg(1), _valid_msg(2), _valid_msg(3)]
    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)

    async def mock_iter(*args, **kwargs):
        for m in msgs:
            yield m

    mock_client.iter_messages = mock_iter
    mock_client.get_messages = AsyncMock(return_value=MagicMock(total=3))
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=1001))

    from app.core.scheduler import FatalTaskError

    calls = 0

    async def always_disconnect(entity, messages, from_peer, **kwargs):
        nonlocal calls
        calls += 1
        raise transport_exc_factory()

    mock_client.forward_messages = AsyncMock(side_effect=always_disconnect)

    with (
        patch("app.core.client_manager.client_manager.userbot", mock_client),
        pytest.raises(FatalTaskError if expected_calls == 1 else ConnectionError),
    ):
        await ForwardService.forward_channel_messages(
            source_chat=src, target_chat=tgt, batch_size=10, delay_seconds=0.01
        )

    assert calls == expected_calls
    conn = get_db()
    bad_rows = conn.execute("SELECT * FROM forward_skips WHERE skip_type = 'bad_id'").fetchall()
    cov_rows = conn.execute("SELECT * FROM forward_coverage").fetchall()
    conn.close()
    assert bad_rows == []
    assert cov_rows == []


@pytest.mark.asyncio
@pytest.mark.usefixtures("_clean_forward_ledger_and_locks")
async def test_forward_unexpected_non_rpc_error_propagates_without_isolation():
    src, tgt = "-1009000000010", "-1009000000020"
    msgs = [_valid_msg(1), _valid_msg(2)]
    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)

    async def mock_iter(*args, **kwargs):
        for m in msgs:
            yield m

    mock_client.iter_messages = mock_iter
    mock_client.get_messages = AsyncMock(return_value=MagicMock(total=2))
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=1001))
    mock_client.forward_messages = AsyncMock(side_effect=RuntimeError("unexpected client bug"))

    with (
        patch("app.core.client_manager.client_manager.userbot", mock_client),
        pytest.raises(RuntimeError, match="unexpected client bug"),
    ):
        await ForwardService.forward_channel_messages(
            source_chat=src, target_chat=tgt, batch_size=10, delay_seconds=0.01
        )

    assert mock_client.forward_messages.call_count == 1
    assert ForwardLedgerRepository.get_coverage_segments(src, tgt) == []
    assert ForwardLedgerRepository.query_skips(source_chat=src)[1] == 0


@pytest.mark.asyncio
@pytest.mark.usefixtures("_clean_forward_ledger_and_locks")
async def test_forward_real_rpc_error_still_bisects_and_isolates():
    """对照组: 确认的 Telegram RPCError 仍正常二分并隔离坏消息"""
    src, tgt = "-1009000000010", "-1009000000020"
    msgs = [_valid_msg(1), _valid_msg(2), _valid_msg(3)]
    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)

    async def mock_iter(*args, **kwargs):
        for m in msgs:
            yield m

    mock_client.iter_messages = mock_iter
    mock_client.get_messages = AsyncMock(return_value=MagicMock(total=3))
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=1001))

    async def fail_msg_2(entity, messages, from_peer, **kwargs):
        if 2 in messages:
            raise RPCError(request=None, message="MESSAGE_ID_INVALID")
        return [MagicMock(id=5000 + m) for m in messages]

    mock_client.forward_messages = AsyncMock(side_effect=fail_msg_2)

    with patch("app.core.client_manager.client_manager.userbot", mock_client):
        forwarded = await ForwardService.forward_channel_messages(
            source_chat=src, target_chat=tgt, batch_size=10, delay_seconds=0.01
        )

    assert forwarded == 2
    assert ForwardLedgerRepository.get_processed_ids(src, tgt, [1, 2, 3]) == {1, 2, 3}
    skips, total = ForwardLedgerRepository.query_skips(source_chat=src, skip_type="bad_id")
    assert total == 1
    assert skips[0]["msg_id"] == 2


@pytest.mark.asyncio
@pytest.mark.usefixtures("_clean_forward_ledger_and_locks")
async def test_forward_persists_mappings_before_success_log_write():
    src, tgt = "-1009000000010", "-1009000000020"
    msgs = [_valid_msg(1), _valid_msg(2), _valid_msg(3)]
    task_id = "fwd_log_fail"
    TaskQueueRepository.create(task_id, "forward_channel", {})

    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)

    async def mock_iter(*args, **kwargs):
        for m in msgs:
            yield m

    mock_client.iter_messages = mock_iter
    mock_client.get_messages = AsyncMock(return_value=MagicMock(total=3))
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=1001))
    mock_client.forward_messages = AsyncMock(
        side_effect=lambda entity, messages, from_peer, **kw: [MagicMock(id=6000 + m) for m in messages]
    )

    events: list[str] = []
    real_commit = ForwardLedgerRepository.commit_batch_records
    real_append = TaskQueueRepository.append_log_async

    def tracked_commit(*args, **kwargs):
        if kwargs.get("mappings"):
            events.append("commit_mappings")
        return real_commit(*args, **kwargs)

    async def failing_success_log(tid, message, *args, **kwargs):
        if "成功转发" in message:
            events.append("success_log_failed")
            raise OSError("ws/db log sink failed")
        return await real_append(tid, message, *args, **kwargs)

    with (
        patch("app.core.client_manager.client_manager.userbot", mock_client),
        patch.object(ForwardLedgerRepository, "commit_batch_records", side_effect=tracked_commit),
        patch.object(TaskQueueRepository, "append_log_async", side_effect=failing_success_log),
    ):
        forwarded = await ForwardService.forward_channel_messages(
            source_chat=src, target_chat=tgt, batch_size=10, delay_seconds=0.01, task_id=task_id
        )

    assert forwarded == 3
    assert events.index("commit_mappings") < events.index("success_log_failed")
    assert ForwardLedgerRepository.get_processed_ids(src, tgt, [1, 2, 3]) == {1, 2, 3}
    assert ForwardLedgerRepository.get_coverage_segments(src, tgt) == [(1, 3)]


@pytest.mark.asyncio
@pytest.mark.usefixtures("_clean_forward_ledger_and_locks")
async def test_target_lock_rejects_concurrent_different_spellings_of_same_channel():
    src1_ent = _make_channel_peer(9000000003, "src_one")
    src2_ent = _make_channel_peer(9000000007, "src_two")
    tgt_ent = _make_channel_peer(9000000020, "target_chan")

    def resolve_side_effect(ident):
        s = str(ident)
        if "src_one" in s or "9000000003" in s:
            return src1_ent
        if "src_two" in s or "9000000007" in s:
            return src2_ent
        if "target_chan" in s or "9000000020" in s:
            return tgt_ent
        raise ValueError(f"unknown {ident}")

    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)
    mock_client.get_entity = AsyncMock(side_effect=resolve_side_effect)

    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_inner(cls, **kwargs):
        started.set()
        await release.wait()
        return 1

    with (
        patch("app.core.client_manager.client_manager.userbot", mock_client),
        patch.object(ForwardService, "_forward_channel_messages_inner", classmethod(slow_inner)),
    ):
        t1 = asyncio.create_task(
            ForwardService.forward_channel_messages(
                source_chat="@src_one",
                target_chat="@target_chan",
                batch_size=10,
                delay_seconds=0.01,
            )
        )
        try:
            started_waiter = asyncio.create_task(started.wait())
            try:
                done, _ = await asyncio.wait({started_waiter, t1}, timeout=5.0, return_when=asyncio.FIRST_COMPLETED)
            finally:
                if not started_waiter.done():
                    started_waiter.cancel()
                    await asyncio.gather(started_waiter, return_exceptions=True)

            assert done, "t1 failed to start within timeout"
            if t1.done():
                await t1
                pytest.fail("t1 exited before reaching slow_inner")

            with pytest.raises(RuntimeError, match="已有运行中的转发任务"):
                await ForwardService.forward_channel_messages(
                    source_chat="-1009000000007",
                    target_chat="-1009000000020",
                    batch_size=10,
                    delay_seconds=0.01,
                )
        finally:
            release.set()
            if not t1.done():
                t1.cancel()
                await asyncio.gather(t1, return_exceptions=True)
            assert t1.done()
            if not t1.cancelled() and t1.exception() is None:
                assert t1.result() == 1

    assert ForwardService._target_locks == {}
    assert ForwardService._pair_locks == {}


@pytest.mark.asyncio
@pytest.mark.usefixtures("_clean_forward_ledger_and_locks")
async def test_resolution_failure_and_same_peer_rejection_leave_no_locks():
    same_ent = _make_channel_peer(9000000019, "same_chan")
    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)
    mock_client.get_entity = AsyncMock(side_effect=RuntimeError("resolve failed"))

    with (
        patch("app.core.client_manager.client_manager.userbot", mock_client),
        pytest.raises(RuntimeError, match="resolve failed"),
    ):
        await ForwardService.forward_channel_messages(
            source_chat="@src", target_chat="@tgt", batch_size=10, delay_seconds=0.01
        )
    assert ForwardService._target_locks == {}
    assert ForwardService._pair_locks == {}

    mock_client.get_entity = AsyncMock(return_value=same_ent)
    with (
        patch("app.core.client_manager.client_manager.userbot", mock_client),
        pytest.raises(RuntimeError, match="禁止同源转发"),
    ):
        await ForwardService.forward_channel_messages(
            source_chat="@same_chan", target_chat="-1009000000019", batch_size=10, delay_seconds=0.01
        )
    assert ForwardService._target_locks == {}
    assert ForwardService._pair_locks == {}


@pytest.mark.asyncio
@pytest.mark.usefixtures("_clean_forward_ledger_and_locks")
async def test_canonical_ledger_shared_across_username_and_numeric_id():
    from fastapi.testclient import TestClient

    from app.api.auth import issue_2fa_token
    from app.config import settings
    from app.main import app
    from app.services.forward_console_service import ForwardConsoleService

    src_ent = _make_channel_peer(9000000017, "MySrcChan")
    tgt_ent = _make_channel_peer(9000000018, "MyTgtChan")
    msgs = [_valid_msg(1), _valid_msg(2), _valid_msg(3)]

    def get_entity_impl(ident):
        s = str(ident)
        if "mysrcchan" in s.lower() or "9000000017" in s:
            return src_ent
        if "mytgtchan" in s.lower() or "9000000018" in s:
            return tgt_ent
        raise ValueError(f"unexpected {ident}")

    def make_client():
        c = AsyncMock()
        c.is_connected = MagicMock(return_value=True)
        c.get_entity = AsyncMock(side_effect=get_entity_impl)
        c.get_messages = AsyncMock(return_value=MagicMock(total=len(msgs)))

        async def mock_iter(*args, **kwargs):
            for m in msgs:
                yield m

        c.iter_messages = mock_iter
        c.forward_messages = AsyncMock(
            side_effect=lambda entity, messages, from_peer, **kw: [MagicMock(id=7000 + m) for m in messages]
        )
        return c

    with patch("app.core.client_manager.client_manager.userbot", make_client()):
        first = await ForwardService.forward_channel_messages(
            source_chat="@MySrcChan",
            target_chat="https://t.me/MyTgtChan",
            batch_size=10,
            delay_seconds=0.01,
        )
    assert first == 3

    assert ForwardLedgerRepository.get_coverage_segments("-1009000000017", "-1009000000018") == [(1, 3)]
    assert ForwardService.compute_resume_start("@mysrcchan", "-1009000000018", 1, 5) == (4, "gap")
    assert ForwardConsoleService.get_coverage_segments("https://t.me/mysrcchan", "@MYTGTCHAN") == [(1, 3)]

    client2 = make_client()
    with patch("app.core.client_manager.client_manager.userbot", client2):
        second = await ForwardService.forward_channel_messages(
            source_chat="-1009000000017",
            target_chat="-1009000000018",
            batch_size=10,
            delay_seconds=0.01,
        )
    assert second == 0
    client2.forward_messages.assert_not_awaited()

    headers = {}
    if settings.API_KEY:
        headers["X-API-Key"] = settings.API_KEY
        tok = issue_2fa_token()
        if tok:
            headers["X-2FA-Token"] = tok
    api_client = TestClient(app, client=("127.0.0.1", 50000))
    cov_resp = api_client.get(
        "/api/forward/coverage",
        params={"source_chat": "@mysrcchan", "target_chat": "https://t.me/mytgtchan"},
        headers=headers,
    )
    assert cov_resp.status_code == 200
    cov_data = cov_resp.json()["data"]
    assert cov_data["source_chat"] == "-1009000000017"
    assert cov_data["target_chat"] == "-1009000000018"
    assert cov_data["covered"] == [{"start": 1, "end": 3}]


async def test_forward_start_and_error_logs_hide_invites(monkeypatch):
    from app.services import forward_service as service

    state = service._ForwardRunState("invite-log", "+fakeSource", "-1234", 50, False, False)
    append = AsyncMock()
    monkeypatch.setattr(TaskQueueRepository, "append_log_async", append)
    await service._log_forward_startup(state, 50, 0)
    await service._log_task_event(state, "lookup https://t.me/joinchat/fakeTarget and +fakeSource")
    messages = [call.args[1] for call in append.await_args_list]
    assert any("-1234" in message for message in messages)
    assert all("fakeSource" not in message and "fakeTarget" not in message for message in messages)


@pytest.mark.asyncio
@pytest.mark.parametrize("stopped_status", ["paused", "cancelled"])
async def test_broadcast_forward_start_does_not_overwrite_stopped_status(monkeypatch, stopped_status):
    from app.services import forward_service as service

    task_id = f"fwd_start_stop_{stopped_status}"
    TaskQueueRepository.create(task_id, "forward_channel", {}, status=stopped_status)
    state = service._ForwardRunState(task_id, "-1001", "-1002", 50, False, False, run_total=10)
    progress_mock = AsyncMock()
    monkeypatch.setattr(TaskQueueRepository, "update_progress_async", progress_mock)

    expected_exc = service.ForwardCancelledError if stopped_status == "cancelled" else service.ForwardPausedError
    with pytest.raises(expected_exc):
        await service._broadcast_forward_start(state, 1, 10)

    assert TaskQueueRepository.get_status(task_id) == stopped_status
    progress_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_dry_run_auto_pause_preserves_cancelled_status(monkeypatch):
    from app.core.runtime import active_task_controls, set_task_control
    from app.services import forward_service as service

    task_id = "fwd_dry_cancel_race"
    TaskQueueRepository.create(task_id, "forward_channel", {}, status="running")
    state = service._ForwardRunState(
        task_id,
        "-1001",
        "-1002",
        50,
        False,
        dry_run=True,
        run_total=10,
        pending_skips=[(1, "dead", None, None)],
    )
    ctx = service._ForwardRunCtx(
        userbot=AsyncMock(), source_entity=MagicMock(), target_entity=MagicMock(), pacer=AsyncMock()
    )

    def commit_and_cancel(*_a, **_kw):
        TaskQueueRepository.update_status(task_id, "cancelled", from_statuses=("running",))
        set_task_control(task_id, cancelled=True)

    monkeypatch.setattr(service.ForwardLedgerRepository, "commit_batch_records", commit_and_cancel)
    monkeypatch.setattr(service.connection_manager, "broadcast", AsyncMock())
    try:
        with pytest.raises(service.ForwardCancelledError):
            await service._commit_batch(state, ctx, [])
        assert TaskQueueRepository.get_status(task_id) == "cancelled"
    finally:
        active_task_controls.pop(task_id, None)


@pytest.mark.asyncio
async def test_broadcast_forward_start_returns_false_on_failed_and_aborts_scan(monkeypatch):
    from app.services import forward_service as service

    task_id = "fwd_start_failed_abort"
    TaskQueueRepository.create(task_id, "forward_channel", {}, status="failed")
    try:
        state_failed = service._ForwardRunState(task_id, "-1001", "-1002", 50, False, False, run_total=10)
        progress_mock = AsyncMock()
        monkeypatch.setattr(TaskQueueRepository, "update_progress_async", progress_mock)

        assert await service._broadcast_forward_start(state_failed, 1, 10) is False
        progress_mock.assert_not_awaited()

        state_direct = service._ForwardRunState(None, "-1001", "-1002", 50, False, False, run_total=10)
        assert await service._broadcast_forward_start(state_direct, 1, 10) is True

        userbot = MagicMock()
        userbot.iter_messages = MagicMock(side_effect=AssertionError("不得扫描源频道"))
        monkeypatch.setattr(service, "_compute_forward_run_total", AsyncMock(return_value=10))
        monkeypatch.setattr(service, "_log_forward_startup", AsyncMock())

        forwarded = await service.ForwardService._forward_channel_messages_inner(
            source_chat="-1001",
            target_chat="-1002",
            start_id=1,
            end_id=10,
            batch_size=50,
            delay_seconds=0.0,
            task_id=task_id,
            full_retransfer=False,
            dry_run=False,
            account_id=None,
            _pre_resolved=(state_failed, userbot, MagicMock(), MagicMock(), 1),
        )
        assert forwarded == 0
        userbot.iter_messages.assert_not_called()
    finally:
        TaskQueueRepository.delete(task_id)
