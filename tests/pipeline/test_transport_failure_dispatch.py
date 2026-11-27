"""PublishPipeline 失败分发表契约测试 (characterization).

锁定 _handle_transport_failure 的全分支语义: TransportError 解包归因 / 控制信号穿透 /
not_found 逐文件隔离 + 终态卡 / 分类学与未知异常上抛 / 本地会话失效上报 / 本地归因入账。
异常归因已单轨化 (引擎侧包装 TransportError, 挂载属性 legacy 双轨废除),
本套件同步覆盖引擎包装 → 编排解包的端到端归因链。
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.pikpak.taxonomy import PikPakPublishError
from app.core.pipeline.engines.exceptions import TransportError
from app.core.pipeline.engines.local_disk import LocalDiskTransportEngine
from app.core.pipeline.interfaces import CleanupStrategy, ItemSource, NotificationStrategy
from app.core.pipeline.models import PublishContext, PublishItem
from app.core.pipeline.pipeline import PublishPipeline
from app.core.pipeline.runtime import BatchState
from app.core.scheduler import UploadCancelledError, UploadPausedError
from app.db.task_repository import TaskQueueRepository

DISPATCH_MODULE = "app.core.pipeline.pipeline"


def _make_pipeline() -> PublishPipeline:
    source = MagicMock(spec=ItemSource)
    engine = MagicMock()
    cleanup = MagicMock(spec=CleanupStrategy)
    notifier = MagicMock(spec=NotificationStrategy)
    return PublishPipeline(source, engine, cleanup=cleanup, notifier=notifier)


def _make_context(**kw: Any) -> PublishContext:
    return PublishContext(task_id="t1", target_chat_id=-100, account_display="Userbot #1", **kw)


def _make_item() -> PublishItem:
    return PublishItem(item_id="f1", name="a.mp4", size_bytes=100, source_type="pikpak")


@pytest.mark.asyncio
async def test_transport_error_unwraps_control_signal() -> None:
    """TransportError(cause=UploadPausedError) → 解包后控制信号原样穿透 (暂停不被吞)。"""
    pipeline = _make_pipeline()
    pause = UploadPausedError("paused")
    exc = TransportError("wrapped", client_label="Bot #2", cause=pause)
    with pytest.raises(UploadPausedError) as ei:
        await pipeline._handle_transport_failure(exc, _make_item(), _make_context(), BatchState(), is_pikpak=True)
    assert ei.value is pause


@pytest.mark.asyncio
async def test_plain_control_signal_passes_through() -> None:
    """裸控制信号 (无包装) → 原样上抛。"""
    pipeline = _make_pipeline()
    with pytest.raises(UploadCancelledError):
        await pipeline._handle_transport_failure(
            UploadCancelledError("cancel"), _make_item(), _make_context(), BatchState(), is_pikpak=False
        )


@pytest.mark.asyncio
async def test_not_found_isolates_file_and_sends_terminal_card() -> None:
    """pikpak not_found: 失败入账 + 结构化明细 + ❌ 终态卡, 不上抛 (逐文件隔离)。"""
    pipeline = _make_pipeline()
    pipeline._emit_failure_card = AsyncMock()  # type: ignore[method-assign]
    b = BatchState()
    miss = PikPakPublishError("not_found", "源文件已删除")
    with (
        patch.object(TaskQueueRepository, "append_log_async", new_callable=AsyncMock) as m_log,
        patch(f"{DISPATCH_MODULE}.FailureLogger.log_failure") as m_fail,
    ):
        await pipeline._handle_transport_failure(miss, _make_item(), _make_context(), b, is_pikpak=True)
    assert len(b.failed) == 1
    assert "源文件已删除" in b.failed[0]
    assert b.failed_details[0]["client_label"] == "Userbot #1"
    pipeline._emit_failure_card.assert_awaited_once()
    assert pipeline._emit_failure_card.await_args.kwargs["terminal"] is True
    m_log.assert_awaited_once()
    m_fail.assert_not_called()  # not_found 走隔离, 不走本地归因


@pytest.mark.asyncio
async def test_pikpak_other_category_reraises_underlying_instance() -> None:
    """分类学其余类别: 原异常实例原样上抛 (调度器 isinstance 契约)。"""
    pipeline = _make_pipeline()
    err = PikPakPublishError("risk_control", "风控拦截")
    with pytest.raises(PikPakPublishError) as ei:
        await pipeline._handle_transport_failure(err, _make_item(), _make_context(), BatchState(), is_pikpak=True)
    assert ei.value is err


@pytest.mark.asyncio
async def test_pikpak_unknown_exception_raises_unwrapped_cause() -> None:
    """PikPak 未知异常: TransportError 解包 cause 上抛 (与旧 pikpak 路径语义一致)。"""
    pipeline = _make_pipeline()
    boom = RuntimeError("cdn gone")
    with pytest.raises(RuntimeError) as ei:
        await pipeline._handle_transport_failure(
            TransportError("wrapped", client_label="Bot #3", cause=boom),
            _make_item(),
            _make_context(),
            BatchState(),
            is_pikpak=True,
        )
    assert ei.value is boom


@pytest.mark.asyncio
async def test_local_failure_attributed_with_transport_error_label() -> None:
    """本地域: TransportError.client_label 优先归因 (Bot 池失败不再记主账号)。"""
    pipeline = _make_pipeline()
    exc = TransportError("wrapped", client_label="Bot Pool #2", cause=RuntimeError("disk full"))
    ctx = _make_context()
    with (
        patch.object(TaskQueueRepository, "append_log_async", new_callable=AsyncMock),
        patch(f"{DISPATCH_MODULE}.describe_upload_failure", return_value=RuntimeError("disk full")) as m_desc,
        patch(f"{DISPATCH_MODULE}.FailureLogger.log_failure") as m_fail,
    ):
        await pipeline._handle_transport_failure(exc, _make_item(), ctx, BatchState(), is_pikpak=False)
    m_desc.assert_called_once()
    assert m_desc.call_args.args[0] is exc.cause  # 解包后的根因参与归因
    assert m_fail.call_args.kwargs["client_type"] == "Bot Pool #2"
    assert m_fail.call_args.kwargs["task_id"] == "t1"


@pytest.mark.asyncio
async def test_local_send_item_wraps_unknown_error_with_mode_label() -> None:
    """引擎侧单轨化: 本地 send_item 未知异常包装为 TransportError, client_label=通道标签
    。"""
    ctx = _make_context()
    ctx.userbot_client = AsyncMock()
    item = _make_item()
    engine = LocalDiskTransportEngine()
    with (
        # local_disk 经 ClientRegistryPort 读单例 → patch 单例本体 (同一对象)
        patch("app.core.client_manager.client_manager") as m_cm,
        patch(
            "app.core.pipeline.engines.local_disk.ft_mod.upload_file_parallel",
            new_callable=AsyncMock,
            side_effect=RuntimeError("disk boom"),
        ),
    ):
        m_cm.userbot = AsyncMock()
        m_cm.bots = []
        with pytest.raises(TransportError) as ei:
            await engine.send_item(item, ctx)
    assert ei.value.client_label == "Userbot #1"
    assert isinstance(ei.value.cause, RuntimeError)
    assert str(ei.value.cause) == "disk boom"


@pytest.mark.asyncio
async def test_session_invalid_reported_then_raised() -> None:
    """本地域会话失效: 上报 report_session_invalid 后原样上抛 (穿透优先于归因)。"""
    pipeline = _make_pipeline()
    boom = RuntimeError("auth key unregistered")
    with (
        patch("app.core.session_errors.is_session_invalid_error", return_value=True),
        patch("app.core.session_errors.report_session_invalid", new_callable=AsyncMock) as m_report,
        patch(f"{DISPATCH_MODULE}.FailureLogger.log_failure") as m_fail,
        pytest.raises(RuntimeError),
    ):
        await pipeline._handle_transport_failure(boom, _make_item(), _make_context(), BatchState(), is_pikpak=False)
    m_report.assert_awaited_once()
    assert m_report.await_args.args[0] == "t1"
    m_fail.assert_not_called()


@pytest.mark.asyncio
async def test_transport_error_without_cause_attributed_as_itself() -> None:
    """无 cause 的 TransportError: 以自身参与归因, client_label 仍取结构化字段。"""
    pipeline = _make_pipeline()
    exc = TransportError("no cause", client_label="Bot #5")
    with (
        patch.object(TaskQueueRepository, "append_log_async", new_callable=AsyncMock),
        patch(f"{DISPATCH_MODULE}.describe_upload_failure", side_effect=lambda e, *_a: e),
        patch(f"{DISPATCH_MODULE}.FailureLogger.log_failure") as m_fail,
    ):
        await pipeline._handle_transport_failure(exc, _make_item(), _make_context(), BatchState(), is_pikpak=False)
    assert m_fail.call_args.args[2] is exc
    assert m_fail.call_args.kwargs["client_type"] == "Bot #5"


@pytest.mark.asyncio
async def test_bot_pool_session_invalid_uses_bot_failure_path():
    pipeline = _make_pipeline()
    boom = RuntimeError("auth key unregistered")
    with (
        patch("app.core.session_errors.report_session_invalid", new_callable=AsyncMock) as report,
        patch.object(TaskQueueRepository, "append_log_async", new_callable=AsyncMock),
        patch(f"{DISPATCH_MODULE}.FailureLogger.log_failure") as failure,
    ):
        await pipeline._handle_transport_failure(
            TransportError("wrapped", client_label="Bot Pool #2", cause=boom),
            _make_item(),
            _make_context(),
            BatchState(),
            is_pikpak=False,
        )
    report.assert_not_awaited()
    assert failure.call_args.kwargs["client_type"] == "Bot Pool #2"
