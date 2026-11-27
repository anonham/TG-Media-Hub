"""execute_split_loop 分卷主循环契约测试 (characterization).

锁定统一分卷主循环的编排契约: 检查点幂等跳过 / JIT 索链 / oversize 重规划 /
非惩罚性保全 vs 物理回滚判定矩阵 / aclose 兜底。行为以当前实现为准 (重构安全网)。
"""

import contextlib
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.fmp4_pipe_uploader import PartOversizeError
from app.core.pikpak.taxonomy import (
    PikPakPublishRetryableError,
    PikPakPublishRiskControlError,
    PikPakPublishThrottledError,
)
from app.core.pipeline.engines.pikpak.split_loop import execute_split_loop, get_saved_checkpoints
from app.core.pipeline.models import PublishContext, PublishItem
from app.core.scheduler import UploadCancelledError, UploadPausedError
from app.db.task_repository import TaskQueueRepository

CKPT_MODULE = "app.core.pipeline.engines.pikpak.split_loop"


def _make_context(**kw: Any) -> PublishContext:
    return PublishContext(task_id="t_split", target_chat_id=-100123, **kw)


def _make_item() -> PublishItem:
    return PublishItem(item_id="file-1", name="movie.mp4", size_bytes=10_000, source_type="pikpak")


def _make_plan(part_index: int, total_parts: int) -> MagicMock:
    plan = MagicMock()
    plan.part_index = part_index
    plan.total_parts = total_parts
    plan.part_name = f"part{part_index:03d}.mp4"
    plan.envelope_length = 100
    return plan


def _make_channel(plans_by_call: list[list[MagicMock]]) -> MagicMock:
    channel = MagicMock()
    channel.channel_name = "vsse"
    channel.channel_label = "VSSE"
    channel.plan_volumes = MagicMock(side_effect=plans_by_call)
    channel.get_cached_volume_bytes = MagicMock(return_value=500)
    channel.prepare_part = AsyncMock()
    channel.handle_oversize = AsyncMock(return_value=len(plans_by_call[0]) + 1)
    channel.aclose = AsyncMock()
    return channel


def _repo_patches(row: dict[str, Any] | None = None) -> tuple[contextlib.ExitStack, MagicMock]:
    """统一 patch 面: 返回 (ExitStack, 回滚函数 mock) —— 回滚经 split_loop 模块函数直调
    。"""
    stack = contextlib.ExitStack()
    m_rollback = stack.enter_context(patch(f"{CKPT_MODULE}.delete_published_messages", new_callable=AsyncMock))
    for ctx in [
        patch.object(TaskQueueRepository, "get_by_id_async", new_callable=AsyncMock, return_value=row),
        patch.object(TaskQueueRepository, "record_part_checkpoint_async", new_callable=AsyncMock),
        patch.object(TaskQueueRepository, "clear_part_checkpoint_async", new_callable=AsyncMock),
        patch(f"{CKPT_MODULE}._inter_part_breathing_delay", new_callable=AsyncMock),
    ]:
        stack.enter_context(ctx)
    return stack, m_rollback


@pytest.mark.asyncio
async def test_happy_path_records_checkpoints_clears_and_breathes() -> None:
    """双卷成功: 逐卷记录检查点 → 卷间呼吸 → 终态清理 → 全量 message_ids 与速度透传。"""
    plans = [_make_plan(1, 2), _make_plan(2, 2)]
    channel = _make_channel([plans])
    channel.stream_part = AsyncMock(side_effect=[(111, 500, 1.5), (222, 600, 1.7)])
    context = _make_context(account_display="Userbot #1")
    with (
        patch.object(TaskQueueRepository, "get_by_id_async", new_callable=AsyncMock, return_value=None),
        patch.object(TaskQueueRepository, "record_part_checkpoint_async", new_callable=AsyncMock) as m_rec,
        patch.object(TaskQueueRepository, "clear_part_checkpoint_async", new_callable=AsyncMock) as m_clr,
        patch(f"{CKPT_MODULE}._inter_part_breathing_delay", new_callable=AsyncMock) as m_breath,
    ):
        outcome = await execute_split_loop(
            channel=channel,
            item=_make_item(),
            context=context,
            threshold=100,
            client=MagicMock(),
            capture_session=MagicMock(),
            link_provider=AsyncMock(),
            reporter=MagicMock(),
        )
    assert outcome.message_ids == [111, 222]
    assert outcome.speed_mb_s == 1.7
    assert outcome.client_label == "Userbot #1"
    assert m_rec.await_count == 2
    m_clr.assert_not_awaited()
    m_breath.assert_awaited_once()
    channel.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_checkpoint_hit_skips_stream_part_idempotently() -> None:
    """检查点命中: 已投递分卷零 CDN 跳过, record_part_sent 以 sent_attributes=None 记账。"""
    plans = [_make_plan(1, 2), _make_plan(2, 2)]
    channel = _make_channel([plans])
    channel.stream_part = AsyncMock(return_value=(222, 500, 1.0))
    row = {
        "params": {
            "pikpak_part_checkpoints": {
                "file-1": {"strategy": "vsse", "total_parts": 2, "threshold": 100, "delivered": {"1": 111}}
            }
        }
    }
    capture = MagicMock()
    stack, _ = _repo_patches(row=row)
    with stack:
        outcome = await execute_split_loop(
            channel=channel,
            item=_make_item(),
            context=_make_context(),
            threshold=100,
            client=MagicMock(),
            capture_session=capture,
            link_provider=AsyncMock(),
            reporter=MagicMock(),
        )
    assert channel.stream_part.await_count == 1
    capture.record_part_sent.assert_called_once()
    args, kwargs = capture.record_part_sent.call_args
    assert args[0] == "part001.mp4"
    assert kwargs["msg_id"] == 111
    assert kwargs["sent_attributes"] is None
    assert outcome.message_ids == [111, 222]


@pytest.mark.asyncio
async def test_prepare_part_jit_refresh_and_failure_tolerance() -> None:
    """JIT 索链: 仅 part_index>1 触发; prepare_part 异常告警后继续发布 (不熔断)。"""
    plans = [_make_plan(1, 2), _make_plan(2, 2)]
    channel = _make_channel([plans])
    channel.prepare_part = AsyncMock(side_effect=RuntimeError("link stale"))
    channel.stream_part = AsyncMock(side_effect=[(111, 500, 1.0), (222, 500, 1.0)])
    stack, _ = _repo_patches()
    with stack:
        outcome = await execute_split_loop(
            channel=channel,
            item=_make_item(),
            context=_make_context(),
            threshold=100,
            client=MagicMock(),
            capture_session=MagicMock(),
            link_provider=AsyncMock(),
            reporter=MagicMock(),
        )
    channel.prepare_part.assert_awaited_once()
    assert outcome.message_ids == [111, 222]


@pytest.mark.asyncio
async def test_oversize_replans_with_forced_parts_then_succeeds() -> None:
    """超限熔断: 丢弃 sink → 清检查点 → handle_oversize 返回强制分卷数 → 重规划后全量成功。"""
    first = [_make_plan(1, 2), _make_plan(2, 2)]
    second = [_make_plan(1, 3), _make_plan(2, 3), _make_plan(3, 3)]
    channel = _make_channel([first, second])
    channel.handle_oversize = AsyncMock(return_value=3)
    channel.stream_part = AsyncMock(
        side_effect=[PartOversizeError(2, 4096, 2048, "boom"), (911, 500, 1.0), (912, 500, 1.0), (913, 500, 1.0)]
    )
    capture = MagicMock()
    with (
        patch.object(TaskQueueRepository, "get_by_id_async", new_callable=AsyncMock, return_value=None),
        patch.object(TaskQueueRepository, "record_part_checkpoint_async", new_callable=AsyncMock),
        patch.object(TaskQueueRepository, "clear_part_checkpoint_async", new_callable=AsyncMock) as m_clr,
        patch(f"{CKPT_MODULE}._inter_part_breathing_delay", new_callable=AsyncMock),
    ):
        outcome = await execute_split_loop(
            channel=channel,
            item=_make_item(),
            context=_make_context(),
            threshold=100,
            client=MagicMock(),
            capture_session=capture,
            link_provider=AsyncMock(),
            reporter=MagicMock(),
            max_replan_attempts=3,
        )
    channel.handle_oversize.assert_awaited_once()
    assert channel.handle_oversize.await_args.kwargs["replan_count"] == 1
    capture.discard_part_sink.assert_called_once()
    m_clr.assert_awaited_once()  # 重规划时清理，发送成功后保留到文件入账
    assert outcome.message_ids == [911, 912, 913]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc",
    [
        UploadPausedError("paused"),
        PikPakPublishThrottledError("throttled", retry_after=30.0),
        PikPakPublishRiskControlError("risk"),
        PikPakPublishRetryableError("retryable"),
    ],
)
async def test_preserve_exceptions_keep_delivered_volumes(exc: Exception) -> None:
    """非惩罚性保全: 暂停/冷却/风控/可重试异常一律保留已发分卷, 严禁物理回滚。"""
    plans = [_make_plan(1, 2), _make_plan(2, 2)]
    channel = _make_channel([plans])
    channel.stream_part = AsyncMock(side_effect=[(111, 500, 1.0), exc])
    capture = MagicMock()
    stack, m_rollback = _repo_patches()
    with stack, pytest.raises(type(exc)):
        await execute_split_loop(
            channel=channel,
            item=_make_item(),
            context=_make_context(),
            threshold=100,
            client=MagicMock(),
            capture_session=capture,
            link_provider=AsyncMock(),
            reporter=MagicMock(),
        )
    m_rollback.assert_not_awaited()
    capture.mark_rolled_back.assert_not_called()
    capture.fail_pending_parts.assert_called_once()


@pytest.mark.asyncio
async def test_cancelled_and_generic_exceptions_physically_rollback() -> None:
    """严格回滚谓词: 显式取消与未知异常 → 物理删除已发孤儿消息 + 标记回滚 + 清检查点。"""
    for stream_exc in (UploadCancelledError("cancelled"), RuntimeError("boom")):
        plans = [_make_plan(1, 2), _make_plan(2, 2)]
        channel = _make_channel([plans])
        channel.stream_part = AsyncMock(side_effect=[(111, 500, 1.0), stream_exc])
        capture = MagicMock()
        client = MagicMock()
        stack, m_rollback = _repo_patches()
        with stack, pytest.raises(type(stream_exc)):
            await execute_split_loop(
                channel=channel,
                item=_make_item(),
                context=_make_context(),
                threshold=100,
                client=client,
                capture_session=capture,
                link_provider=AsyncMock(),
                reporter=MagicMock(),
            )
        m_rollback.assert_awaited_once_with(client, -100123, [111])
        capture.mark_rolled_back.assert_called_once()


@pytest.mark.asyncio
async def test_aclose_always_runs_and_swallows_errors() -> None:
    """aclose 兜底: 成功路径必调用; aclose 自身异常不影响 outcome 交付。"""
    plans = [_make_plan(1, 1)]
    channel = _make_channel([plans])
    channel.stream_part = AsyncMock(return_value=(311, 500, 1.0))
    channel.aclose = AsyncMock(side_effect=RuntimeError("close failed"))
    stack, _ = _repo_patches()
    with stack:
        outcome = await execute_split_loop(
            channel=channel,
            item=_make_item(),
            context=_make_context(),
            threshold=100,
            client=MagicMock(),
            capture_session=MagicMock(),
            link_provider=AsyncMock(),
            reporter=MagicMock(),
        )
    channel.aclose.assert_awaited_once()
    assert outcome.message_ids == [311]


@pytest.mark.asyncio
async def test_get_saved_checkpoints_validates_strategy_shape_and_threshold() -> None:
    """检查点读取: strategy/total_parts/threshold 一致才采信; 空任务/畸形参数 → 空表。"""
    row = {
        "params": {
            "pikpak_part_checkpoints": {
                "file-1": {"strategy": "vsse", "total_parts": 3, "threshold": 100, "delivered": {"1": 11, "2": 22}}
            }
        }
    }
    with patch.object(TaskQueueRepository, "get_by_id_async", new_callable=AsyncMock, return_value=row):
        assert await get_saved_checkpoints("t1", "file-1", "vsse", 3, 100) == {1: 11, 2: 22}
        assert await get_saved_checkpoints("t1", "file-1", "fmp4", 3, 100) == {}  # strategy 不一致
        assert await get_saved_checkpoints("t1", "file-1", "vsse", 2, 100) == {}  # total_parts 不一致
        assert await get_saved_checkpoints("t1", "file-1", "vsse", 3, 200) == {}  # threshold 不一致
    assert await get_saved_checkpoints("", "file-1", "vsse", 3, 100) == {}  # 空 task_id


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_read", [1, 2])
async def test_checkpoint_read_failure_preserves_delivery_until_retry(failure_read: int) -> None:
    from app.core.scheduler import RetryableTaskError

    channel = _make_channel([[_make_plan(1, 2), _make_plan(2, 2)]])
    channel.stream_part = AsyncMock()
    reads = [RuntimeError("metadata unavailable")] if failure_read == 1 else [None, RuntimeError("read unavailable")]
    with (
        patch.object(TaskQueueRepository, "get_by_id_async", new_callable=AsyncMock, side_effect=reads),
        patch.object(TaskQueueRepository, "record_part_checkpoint_async", new_callable=AsyncMock) as record,
        patch.object(TaskQueueRepository, "clear_part_checkpoint_async", new_callable=AsyncMock) as clear,
        patch(f"{CKPT_MODULE}.delete_published_messages", new_callable=AsyncMock) as delete,
        pytest.raises(RetryableTaskError),
    ):
        await execute_split_loop(
            channel=channel,
            item=_make_item(),
            context=_make_context(),
            threshold=100,
            client=MagicMock(),
            capture_session=MagicMock(),
            link_provider=AsyncMock(),
            reporter=MagicMock(),
        )
    channel.stream_part.assert_not_awaited()
    record.assert_not_awaited()
    clear.assert_not_awaited()
    delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_split_loop_resume_tracks_saved_ids_on_early_cancel() -> None:
    """恢复执行在首次分卷检查前即被取消时，仍必须回滚删除所有已保存分卷消息."""
    plans = [
        MagicMock(part_index=1, total_parts=2, part_name="p1.mp4"),
        MagicMock(part_index=2, total_parts=2, part_name="p2.mp4"),
    ]
    channel = MagicMock(
        channel_name="vsse",
        channel_label="VSSE",
        plan_volumes=MagicMock(return_value=plans),
        get_cached_volume_bytes=MagicMock(return_value=500),
        aclose=AsyncMock(),
    )
    del channel.get_plan_signature
    row = {
        "params": {
            "pikpak_part_checkpoints": {
                "file-1": {"strategy": "vsse", "total_parts": 2, "threshold": 100, "delivered": {"1": 101, "2": 102}}
            }
        }
    }
    ctx = PublishContext(task_id="t_r_cancel", target_chat_id=-1001, is_cancelled=lambda: True)
    client = MagicMock()
    with (
        patch.object(TaskQueueRepository, "get_by_id_async", new_callable=AsyncMock, return_value=row),
        patch.object(TaskQueueRepository, "clear_part_checkpoint_async", new_callable=AsyncMock),
        patch(
            "app.core.pipeline.engines.pikpak.split_loop.delete_published_messages",
            new_callable=AsyncMock,
            return_value=True,
        ) as m_del,
        pytest.raises(UploadCancelledError),
    ):
        await execute_split_loop(
            channel=channel,
            item=PublishItem(item_id="file-1", name="m.mp4", size_bytes=1000, source_type="pikpak"),
            context=ctx,
            threshold=100,
            client=client,
            capture_session=MagicMock(),
            link_provider=AsyncMock(),
            reporter=MagicMock(),
        )
    m_del.assert_awaited_once_with(client, -1001, [101, 102])


@pytest.mark.asyncio
async def test_split_loop_compensates_delivery_when_checkpoint_persist_fails() -> None:
    """stream_part 成功后若检查点持久化失败，先补偿删除当前分卷并转 RetryableTaskError."""
    from app.core.scheduler import RetryableTaskError

    plans = [
        MagicMock(part_index=1, total_parts=2, part_name="p1.mp4"),
        MagicMock(part_index=2, total_parts=2, part_name="p2.mp4"),
    ]
    channel = MagicMock(
        channel_name="vsse",
        channel_label="VSSE",
        plan_volumes=MagicMock(return_value=plans),
        get_cached_volume_bytes=MagicMock(return_value=500),
        prepare_part=AsyncMock(),
        stream_part=AsyncMock(side_effect=[(101, 500, 1.0), (202, 500, 1.0)]),
        aclose=AsyncMock(),
    )
    del channel.get_plan_signature
    client = MagicMock()
    with (
        patch.object(TaskQueueRepository, "get_by_id_async", new_callable=AsyncMock, return_value=None),
        patch.object(
            TaskQueueRepository,
            "record_part_checkpoint_async",
            new_callable=AsyncMock,
            side_effect=[None, RuntimeError("db locked"), RuntimeError("db locked"), None],
        ),
        patch.object(TaskQueueRepository, "clear_part_checkpoint_async", new_callable=AsyncMock) as m_clr,
        patch("app.core.pipeline.engines.pikpak.split_loop._inter_part_breathing_delay", new_callable=AsyncMock),
        patch(
            "app.core.pipeline.engines.pikpak.split_loop.delete_published_messages",
            new_callable=AsyncMock,
            return_value=True,
        ) as m_del,
        pytest.raises(RetryableTaskError),
    ):
        await execute_split_loop(
            channel=channel,
            item=PublishItem(item_id="file-1", name="m.mp4", size_bytes=1000, source_type="pikpak"),
            context=PublishContext(task_id="t_cp_fail", target_chat_id=-1001),
            threshold=100,
            client=client,
            capture_session=MagicMock(),
            link_provider=AsyncMock(),
            reporter=MagicMock(),
        )
    # 仅补偿删除未记账的 Part 2 (202)，Part 1 (101) 得以保全
    m_del.assert_awaited_once_with(client, -1001, [202])
    m_clr.assert_not_awaited()


@pytest.mark.asyncio
async def test_split_loop_preserves_checkpoint_when_replan_or_dispose_delete_fails() -> None:
    """_replan_after_oversize 与 _dispose_failed_delivery 在回滚删除失败时保留检查点."""
    from app.core.pipeline.engines.pikpak.split_loop import _dispose_failed_delivery, _replan_after_oversize
    from app.core.scheduler import RetryableTaskError

    store = AsyncMock()
    store.clear_part_checkpoint = AsyncMock()
    channel = MagicMock()
    channel.channel_name = "fmp4"
    channel.channel_label = "fMP4"
    channel.handle_oversize = AsyncMock(side_effect=RetryableTaskError("rollback failed"))
    ctx = PublishContext(task_id="t_sp_rb", target_chat_id=-1004)
    item = PublishItem(item_id="fid_1", name="m.mp4", size_bytes=100, source_type="pikpak")
    capture = MagicMock()

    with pytest.raises(RetryableTaskError):
        await _replan_after_oversize(
            channel=channel,
            context=ctx,
            item=item,
            capture_session=capture,
            client=AsyncMock(),
            plans=[],
            delivered_message_ids=[11],
            replan_count=1,
            max_replan_attempts=3,
            last_part_exc=None,
            task_store=store,
        )
    store.clear_part_checkpoint.assert_not_awaited()

    client = AsyncMock()
    with patch(
        "app.core.pipeline.engines.pikpak.split_loop.delete_published_messages",
        new_callable=AsyncMock,
        return_value=False,
    ) as m_del:
        await _dispose_failed_delivery(
            channel=channel,
            item=item,
            context=ctx,
            client=client,
            capture_session=capture,
            delivered_message_ids=[11],
            last_split_exc=RuntimeError("boom"),
            task_store=store,
        )
    m_del.assert_awaited_once_with(client, ctx.target_chat_id, [11])
    capture.mark_rolled_back.assert_not_called()
    store.clear_part_checkpoint.assert_not_awaited()


@pytest.mark.asyncio
async def test_split_loop_resume_uses_saved_total_parts_and_cleans_mismatch():
    """恢复执行时沿用保存的分卷数 forced_parts; 若仍不匹配则清理旧消息与检查点."""
    row = {
        "params": {
            "pikpak_part_checkpoints": {
                "file-1": {"strategy": "fmp4", "total_parts": 3, "threshold": 100, "delivered": {"1": 111}}
            }
        }
    }
    plans_3 = [MagicMock(part_index=i, total_parts=3, part_name=f"p{i}.mp4") for i in (1, 2, 3)]
    channel = MagicMock()
    channel.channel_name = "fmp4"
    channel.channel_label = "fMP4"
    channel.plan_volumes = MagicMock(return_value=plans_3)
    channel.get_cached_volume_bytes = MagicMock(return_value=100)
    channel.prepare_part = AsyncMock()
    channel.stream_part = AsyncMock(side_effect=[(222, 100, 1.0), (333, 100, 1.0)])
    channel.aclose = AsyncMock()

    ctx = PublishContext(task_id="t_res", target_chat_id=-100123)
    item = PublishItem(item_id="file-1", name="v.mp4", size_bytes=300, source_type="pikpak")
    with (
        patch.object(TaskQueueRepository, "get_by_id_async", new_callable=AsyncMock, return_value=row),
        patch.object(TaskQueueRepository, "record_part_checkpoint_async", new_callable=AsyncMock),
        patch.object(TaskQueueRepository, "clear_part_checkpoint_async", new_callable=AsyncMock),
        patch("app.core.pipeline.engines.pikpak.split_loop._inter_part_breathing_delay", new_callable=AsyncMock),
        patch("app.core.pipeline.engines.pikpak.split_loop.delete_published_messages", new_callable=AsyncMock) as m_del,
    ):
        out = await execute_split_loop(
            channel=channel,
            item=item,
            context=ctx,
            threshold=100,
            client=MagicMock(),
            capture_session=MagicMock(),
            link_provider=AsyncMock(),
            reporter=MagicMock(),
        )
    channel.plan_volumes.assert_called_once_with(3)
    m_del.assert_not_awaited()
    assert out.message_ids == [111, 222, 333]

    events: list[str] = []

    async def _trace_clear(*args, **kwargs):
        events.append("clear")

    async def _trace_delete(*args, **kwargs):
        events.append("delete")
        return True

    async def _trace_stream(*args, job=None, **kwargs):
        part = job if job is not None else args[0]
        events.append(f"stream:{part.part_index}")
        return (444, 150, 1.0) if part.part_index == 1 else (555, 150, 1.0)

    plans_2 = [MagicMock(part_index=i, total_parts=2, part_name=f"p{i}.mp4") for i in (1, 2)]
    channel.plan_volumes = MagicMock(return_value=plans_2)
    channel.stream_part = AsyncMock(side_effect=_trace_stream)
    client2 = MagicMock()
    with (
        patch.object(TaskQueueRepository, "get_by_id_async", new_callable=AsyncMock, return_value=row),
        patch.object(TaskQueueRepository, "record_part_checkpoint_async", new_callable=AsyncMock),
        patch.object(
            TaskQueueRepository, "clear_part_checkpoint_async", new_callable=AsyncMock, side_effect=_trace_clear
        ) as m_clr,
        patch("app.core.pipeline.engines.pikpak.split_loop._inter_part_breathing_delay", new_callable=AsyncMock),
        patch(
            "app.core.pipeline.engines.pikpak.split_loop.delete_published_messages",
            new_callable=AsyncMock,
            side_effect=_trace_delete,
        ) as m_del,
    ):
        out2 = await execute_split_loop(
            channel=channel,
            item=item,
            context=ctx,
            threshold=100,
            client=client2,
            capture_session=MagicMock(),
            link_provider=AsyncMock(),
            reporter=MagicMock(),
        )
    m_del.assert_awaited_once_with(client2, ctx.target_chat_id, [111], propagate_flood_wait=True)
    m_clr.assert_awaited_once()
    assert events.index("delete") < events.index("stream:1")
    assert events.index("clear") < events.index("stream:1")
    assert out2.message_ids == [444, 555]


@pytest.mark.asyncio
async def test_split_loop_keeps_checkpoint_when_delete_fails_and_closes_on_peek_cancel():
    """删消息失败不清理检查点; peek 期间取消仍调用 channel.aclose()."""
    import asyncio

    from app.core.scheduler import RetryableTaskError

    row = {
        "params": {
            "pikpak_part_checkpoints": {
                "file-1": {"strategy": "fmp4", "total_parts": 3, "threshold": 100, "delivered": {"1": 111}}
            }
        }
    }
    plans_2 = [MagicMock(part_index=i, total_parts=2, part_name=f"p{i}.mp4") for i in (1, 2)]
    channel = MagicMock()
    channel.channel_name = "fmp4"
    channel.channel_label = "fMP4"
    channel.plan_volumes = MagicMock(return_value=plans_2)
    channel.get_cached_volume_bytes = MagicMock(return_value=100)
    channel.stream_part = AsyncMock()
    channel.aclose = AsyncMock()

    client = MagicMock()
    client.delete_messages = AsyncMock(side_effect=RuntimeError("tg forbid delete"))
    ctx = PublishContext(task_id="t_keep_cp", target_chat_id=-1001)
    item = PublishItem(item_id="file-1", name="v.mp4", size_bytes=300, source_type="pikpak")

    with (
        patch.object(TaskQueueRepository, "get_by_id_async", new_callable=AsyncMock, return_value=row),
        patch.object(TaskQueueRepository, "clear_part_checkpoint_async", new_callable=AsyncMock) as m_clr,
        pytest.raises(RetryableTaskError),
    ):
        await execute_split_loop(
            channel=channel,
            item=item,
            context=ctx,
            threshold=100,
            client=client,
            capture_session=MagicMock(),
            link_provider=AsyncMock(),
            reporter=MagicMock(),
        )
    m_clr.assert_not_awaited()
    channel.stream_part.assert_not_awaited()
    channel.aclose.assert_awaited_once()

    channel2 = MagicMock()
    channel2.channel_name = "fmp4"
    channel2.aclose = AsyncMock()
    with (
        patch.object(
            TaskQueueRepository, "get_by_id_async", new_callable=AsyncMock, side_effect=asyncio.CancelledError()
        ),
        pytest.raises(asyncio.CancelledError),
    ):
        await execute_split_loop(
            channel=channel2,
            item=item,
            context=ctx,
            threshold=100,
            client=MagicMock(),
            capture_session=MagicMock(),
            link_provider=AsyncMock(),
            reporter=MagicMock(),
        )
    channel2.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_split_loop_delete_messages_flood_wait_preserves_retry_after():
    """分卷变更清理旧消息遇 FloodWaitError 时必须透传 retry_after 与因果链, 不得被吞."""
    from telethon.errors import FloodWaitError

    from app.core.scheduler import RetryableTaskError

    row = {
        "params": {
            "pikpak_part_checkpoints": {
                "file-1": {"strategy": "fmp4", "total_parts": 3, "threshold": 100, "delivered": {"1": 111}}
            }
        }
    }
    plans_2 = [MagicMock(part_index=i, total_parts=2, part_name=f"p{i}.mp4") for i in (1, 2)]
    channel = MagicMock()
    channel.channel_name = "fmp4"
    channel.channel_label = "fMP4"
    channel.plan_volumes = MagicMock(return_value=plans_2)
    channel.get_cached_volume_bytes = MagicMock(return_value=100)
    channel.prepare_part = AsyncMock()
    channel.stream_part = AsyncMock()
    channel.aclose = AsyncMock()

    fw = FloodWaitError(request=None)
    fw.seconds = 42
    mock_client = MagicMock()
    mock_client.delete_messages = AsyncMock(side_effect=fw)

    ctx = PublishContext(task_id="t_res_fw", target_chat_id=-100123)
    item = PublishItem(item_id="file-1", name="v.mp4", size_bytes=300, source_type="pikpak")
    with (
        patch.object(TaskQueueRepository, "get_by_id_async", new_callable=AsyncMock, return_value=row),
        patch.object(TaskQueueRepository, "clear_part_checkpoint_async", new_callable=AsyncMock) as m_clr,
        pytest.raises(RetryableTaskError) as exc_info,
    ):
        await execute_split_loop(
            channel=channel,
            item=item,
            context=ctx,
            threshold=100,
            client=mock_client,
            capture_session=MagicMock(),
            link_provider=AsyncMock(),
            reporter=MagicMock(),
        )
    assert exc_info.value.retry_after == 42.0
    assert exc_info.value.__cause__ is fw
    m_clr.assert_not_awaited()
