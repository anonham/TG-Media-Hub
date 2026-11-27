"""PikPak 保守风控与无人值守自愈架构测试

覆盖范围:
- 调度器三级阶梯无人值守自愈 (Tier 1: 15m -> Tier 2: 40m + 换链 + 降档 -> Tier 3: Safe-Pause)
- 进程级全局 PikPak 冷却保护锁 (_pikpak_cooldown_until)
- 单文件成功后限流阶梯计数复位 (throttle_strike_count = 0)
- 多分卷检查点持久化 (pikpak_part_checkpoints) 与清理
- 严格回滚谓词 (仅显式取消与动态重规划物理回滚，暂停/冷却/重试非惩罚性保全已发分卷)
"""

import asyncio
import os
import time
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from app.core.pikpak.taxonomy import (
    PikPakPublishRiskControlError,
    PikPakPublishThrottledError,
    wrap_pikpak_failure,
)
from app.core.pikpak.taxonomy import (
    PikPakRiskControlError as CliPikPakRiskControlError,
)
from app.core.pipeline import (
    PublishContext,
    PublishItem,
    PublishPipeline,
)
from app.core.pipeline.engines import PikPakStreamTransportEngine
from app.core.pipeline.engines.pikpak import single as pikpak_single
from app.core.pipeline.engines.pikpak.split_loop import get_saved_checkpoints
from app.core.scheduler import TaskScheduler, UploadCancelledError
from app.core.stream_chunk_provider import (
    PikPakRiskControlError as ProviderPikPakRiskControlError,
)
from app.core.stream_chunk_provider import (
    PikPakThrottledError,
)
from app.db.task_repository import TaskQueueRepository


@pytest.fixture(autouse=True)
def clean_scheduler_cooldown():
    """每个测试前后清理全局 PikPak 冷却锁"""
    yield


@pytest.mark.asyncio
async def test_pikpak_cooldown_global_lock():
    """测试全局 PikPak 冷却锁阻止同源任务派发，但不影响非 PikPak 任务"""
    scheduler = TaskScheduler(check_interval=0.1)
    scheduler.clear_pikpak_cooldown()

    # 1. 初始状态：无冷却
    assert scheduler.is_pikpak_in_cooldown() is False

    # 2. 设置冷却到未来 60 秒
    future_ts = time.time() + 60.0
    scheduler.set_pikpak_cooldown(future_ts)
    assert scheduler.is_pikpak_in_cooldown() is True

    # 3. 入队一个 pikpak_publish 任务和一个 download_link 任务
    t_pikpak = f"t_pk_{int(time.time() * 1000)}"
    t_down = f"t_dl_{int(time.time() * 1000)}"

    TaskQueueRepository.create(
        task_id=t_pikpak,
        task_type="pikpak_publish",
        status="queued",
        params={"target_channel": "-1001", "remote_files": []},
    )
    TaskQueueRepository.create(
        task_id=t_down,
        task_type="download_link",
        status="queued",
        params={"url": "https://example.com/test.bin"},
    )

    try:

        async def long_handler(task_id: str, params: dict, account_id: int):
            await asyncio.sleep(1.0)

        scheduler.register_executor("pikpak_publish", long_handler)
        scheduler.register_executor("download_link", long_handler)

        mock_acc = MagicMock(account_id=1, max_concurrency=5, is_flood_locked=lambda: False)
        with (
            patch.object(scheduler, "get_max_global_concurrency", return_value=5),
            patch(
                "app.core.client_manager.client_manager.get_available_account",
                new_callable=AsyncMock,
                return_value=mock_acc,
            ),
        ):
            await scheduler.schedule_once()

            # pikpak 任务因全局冷却锁未派发，download 任务成功派发运行
            assert t_pikpak not in scheduler._running_tasks
            assert t_down in scheduler._running_tasks

        # 4. 清理冷却锁后再次调度，pikpak 任务可成功派发
        scheduler.clear_pikpak_cooldown()
        assert scheduler.is_pikpak_in_cooldown() is False

        with (
            patch.object(scheduler, "get_max_global_concurrency", return_value=5),
            patch(
                "app.core.client_manager.client_manager.get_available_account",
                new_callable=AsyncMock,
                return_value=mock_acc,
            ),
        ):
            await scheduler.schedule_once()
            assert t_pikpak in scheduler._running_tasks

    finally:
        TaskQueueRepository.delete(t_pikpak)
        TaskQueueRepository.delete(t_down)
        scheduler.clear_pikpak_cooldown()


@pytest.mark.asyncio
async def test_pikpak_tiered_healing_level2_throttling():
    """测试 Level 2 频控 (PikPakThrottledError) 的三级阶梯无人值守自愈流转"""
    scheduler = TaskScheduler(check_interval=0.1)
    task_id = f"t_tier_l2_{int(time.time() * 1000)}"

    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        params={
            "target_channel": "-1001",
            "remote_files": [{"file_id": "f1"}],
            "upload_concurrency": 4,
            "send_delay_seconds": 3.0,
        },
    )

    try:
        # --- Tier 1 (首次触发 Level 2 频控) ---
        exc1 = PikPakThrottledError("HTTP 503 Service Unavailable", status=503, retry_after=5.0)
        await scheduler._handle_pikpak_tiered_cooldown(
            task_id=task_id,
            retry_count=0,
            max_retries=3,
            error_type="throttled",
            retry_after=exc1.retry_after,
            exc=exc1,
        )

        row1 = TaskQueueRepository.get_by_id(task_id)
        assert row1["status"] == "queued"
        assert row1["retry_count"] == 1
        assert row1["params"]["throttle_strike_count"] == 1
        # Tier 1 延迟约为 max(5, 900) + [0, 120] >= 900s
        assert row1["next_retry_at"] >= time.time() + 890.0
        assert scheduler.is_pikpak_in_cooldown() is True

        # --- Tier 2 (二次触发 Level 2 频控: 深度冷却 + 自动降档) ---
        exc2 = PikPakThrottledError("HTTP 503 Service Unavailable", status=503)
        await scheduler._handle_pikpak_tiered_cooldown(
            task_id=task_id,
            retry_count=1,
            max_retries=3,
            error_type="throttled",
            retry_after=None,
            exc=exc2,
        )

        row2 = TaskQueueRepository.get_by_id(task_id)
        assert row2["status"] == "queued"
        assert row2["retry_count"] == 2
        assert row2["params"]["throttle_strike_count"] == 2
        assert row2["params"]["auto_downshifted"] is True
        assert row2["params"]["pikpak_prefetch_concurrency"] == 1
        assert row2["params"]["upload_concurrency"] == 1
        assert row2["params"]["send_delay_seconds"] == 20.0
        # Tier 2 延迟约为 max(0, 2400) + [0, 180] >= 2400s
        assert row2["next_retry_at"] >= time.time() + 2390.0

        # --- Tier 3 (三次触发 Level 2 频控: Safe-Pause) ---
        exc3 = PikPakThrottledError("HTTP 429 Too Many Requests", status=429)
        await scheduler._handle_pikpak_tiered_cooldown(
            task_id=task_id,
            retry_count=2,
            max_retries=3,
            error_type="throttled",
            retry_after=None,
            exc=exc3,
        )

        row3 = TaskQueueRepository.get_by_id(task_id)
        # 绝不进入 failed，保留断点挂起为 paused
        assert row3["status"] == "paused"
        assert "风控安全挂起" in row3["error_detail"]

    finally:
        TaskQueueRepository.delete(task_id)
        scheduler.clear_pikpak_cooldown()


@pytest.mark.asyncio
async def test_pikpak_tiered_healing_level3_risk_control():
    """测试 Level 3a 数据面拦截直接进入 Tier 2，二次 Safe-Pause；Level 3b 控制面直接 Safe-Pause"""
    scheduler = TaskScheduler(check_interval=0.1)

    # 1. Level 3a 数据面 HTML 质询 (ProviderPikPakRiskControlError)
    t_data = f"t_tier_l3a_{int(time.time() * 1000)}"
    TaskQueueRepository.create(
        task_id=t_data,
        task_type="pikpak_publish",
        params={"target_channel": "-1001", "upload_concurrency": 2, "send_delay_seconds": 3.0},
    )

    try:
        # 首犯 Level 3a -> 直接进入 Tier 2 深度冷却与自动降档
        exc_data = ProviderPikPakRiskControlError("CDN 返回 text/html 验证页")
        await scheduler._handle_pikpak_tiered_cooldown(
            task_id=t_data,
            retry_count=0,
            max_retries=3,
            error_type="data_risk_control",
            retry_after=None,
            exc=exc_data,
        )

        row_d1 = TaskQueueRepository.get_by_id(t_data)
        assert row_d1["status"] == "queued"
        assert row_d1["params"]["data_risk_strike_count"] == 1
        assert row_d1["params"]["auto_downshifted"] is True
        assert row_d1["params"]["pikpak_prefetch_concurrency"] == 1
        assert row_d1["params"]["upload_concurrency"] == 1
        assert row_d1["params"]["send_delay_seconds"] >= 20.0
        assert row_d1["next_retry_at"] >= time.time() + 2390.0

        # 二次遭遇 Level 3a -> 直接 Safe-Pause
        await scheduler._handle_pikpak_tiered_cooldown(
            task_id=t_data,
            retry_count=1,
            max_retries=3,
            error_type="data_risk_control",
            retry_after=None,
            exc=exc_data,
        )

        row_d2 = TaskQueueRepository.get_by_id(t_data)
        assert row_d2["status"] == "paused"
        assert "风控安全挂起" in row_d2["error_detail"]

    finally:
        TaskQueueRepository.delete(t_data)
        scheduler.clear_pikpak_cooldown()

    # 2. Level 3b 控制面账号风控 (CliPikPakRiskControlError)
    t_ctrl = f"t_tier_l3b_{int(time.time() * 1000)}"
    TaskQueueRepository.create(
        task_id=t_ctrl,
        task_type="pikpak_publish",
        params={"target_channel": "-1001"},
    )

    try:
        exc_ctrl = CliPikPakRiskControlError("账号触发风控拦截")
        await scheduler._handle_pikpak_tiered_cooldown(
            task_id=t_ctrl,
            retry_count=0,
            max_retries=3,
            error_type="control_risk_control",
            retry_after=None,
            exc=exc_ctrl,
        )

        row_c = TaskQueueRepository.get_by_id(t_ctrl)
        assert row_c["status"] == "paused"
        assert "控制面风控拦截" in row_c["error_detail"]

    finally:
        TaskQueueRepository.delete(t_ctrl)
        scheduler.clear_pikpak_cooldown()


@pytest.mark.asyncio
async def test_task_repository_part_checkpoints():
    """测试 TaskQueueRepository 分卷检查点记录、去重与清理"""
    task_id = f"t_cp_{int(time.time() * 1000)}"
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        params={"target_channel": "-1001"},
    )

    try:
        # 1. 记录 Part 1 交付检查点
        await TaskQueueRepository.record_part_checkpoint_async(
            task_id=task_id,
            file_id="fid_999",
            strategy="vsse",
            total_parts=3,
            threshold=2044723200,
            part_index=1,
            msg_id=7701,
        )

        row = TaskQueueRepository.get_by_id(task_id)
        cp = row["params"]["pikpak_part_checkpoints"]["fid_999"]
        assert cp["strategy"] == "vsse"
        assert cp["total_parts"] == 3
        assert cp["threshold"] == 2044723200
        assert cp["delivered"] == {"1": 7701}

        # 2. 记录 Part 2 交付检查点
        await TaskQueueRepository.record_part_checkpoint_async(
            task_id=task_id,
            file_id="fid_999",
            strategy="vsse",
            total_parts=3,
            threshold=2044723200,
            part_index=2,
            msg_id=7702,
        )

        row = TaskQueueRepository.get_by_id(task_id)
        cp = row["params"]["pikpak_part_checkpoints"]["fid_999"]
        assert cp["delivered"] == {"1": 7701, "2": 7702}

        # 3. 读取检查点 (split_loop.get_saved_checkpoints 校验一致性)
        res_ok = await get_saved_checkpoints(
            task_id=task_id,
            file_id="fid_999",
            strategy="vsse",
            total_parts=3,
            threshold=2044723200,
        )
        assert res_ok == {1: 7701, 2: 7702}

        # 4. 校验不匹配 (如阈值或总卷数变动) 则返回空
        res_mismatch = await get_saved_checkpoints(
            task_id=task_id,
            file_id="fid_999",
            strategy="vsse",
            total_parts=4,
            threshold=2044723200,
        )
        assert res_mismatch == {}

        # 5. 清理检查点
        await TaskQueueRepository.clear_part_checkpoint_async(task_id, "fid_999")
        row_cleared = TaskQueueRepository.get_by_id(task_id)
        assert "fid_999" not in row_cleared["params"]["pikpak_part_checkpoints"]

    finally:
        TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_single_file_success_resets_strikes_and_checkpoints():
    """测试单文件成功交付后阶梯限流计数复位与该文件的检查点清理"""
    task_id = f"t_reset_{int(time.time() * 1000)}"
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        params={
            "target_channel": "-1001",
            "throttle_strike_count": 2,
            "data_risk_strike_count": 1,
            "pikpak_part_checkpoints": {
                "fid_1": {"strategy": "vsse", "total_parts": 2, "threshold": 2044723200, "delivered": {"1": 8801}}
            },
        },
    )

    try:
        pipeline = PublishPipeline(
            source=MagicMock(),
            engine=MagicMock(),
            cleanup=MagicMock(),
            notifier=MagicMock(),
        )

        item = PublishItem(
            item_id="fid_1",
            name="video.mp4",
            size_bytes=1000,
            parent_folder_id="root",
        )
        context = PublishContext(task_id=task_id, target_chat_id="-1001", source_type="pikpak")
        counters = MagicMock()
        b = MagicMock()
        b.successful = []

        with patch("app.db.publish_repository.PublishHistoryRepository.record"):
            await pipeline._record_success(
                item=item,
                context=context,
                counters=counters,
                b=b,
                msg_id=8802,
                is_pikpak=True,
            )

        row = TaskQueueRepository.get_by_id(task_id)
        # 单文件成功交付后清零限流计数
        assert row["params"]["throttle_strike_count"] == 0
        assert row["params"]["data_risk_strike_count"] == 0
        # 该文件的检查点被清理
        assert "fid_1" not in row["params"]["pikpak_part_checkpoints"]

    finally:
        TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_taxonomy_wrapping_and_classification():
    """测试分类学与包装函数对 PikPakThrottledError 和 RiskControl 的映射"""
    # 1. PikPakThrottledError -> PikPakPublishThrottledError (Retryable)
    th_exc = PikPakThrottledError("503", status=503, retry_after=12.0)
    wrapped_th = wrap_pikpak_failure(th_exc)
    assert isinstance(wrapped_th, PikPakPublishThrottledError)
    assert wrapped_th.category == "throttled"
    assert wrapped_th.retry_after == 12.0

    # 2. ProviderPikPakRiskControlError -> PikPakPublishRiskControlError (is_data_plane=True)
    dp_risk = ProviderPikPakRiskControlError("html response")
    wrapped_dp = wrap_pikpak_failure(dp_risk)
    assert isinstance(wrapped_dp, PikPakPublishRiskControlError)
    assert wrapped_dp.is_data_plane is True

    # 3. CliPikPakRiskControlError -> PikPakPublishRiskControlError (is_data_plane=False)
    ctrl_risk = CliPikPakRiskControlError("cli captcha")
    wrapped_ctrl = wrap_pikpak_failure(ctrl_risk)
    assert isinstance(wrapped_ctrl, PikPakPublishRiskControlError)
    assert wrapped_ctrl.is_data_plane is False

    # 4. classify_pikpak_failure 返回 throttled 分类的通用异常包装
    class MockThrottled(Exception):
        pass

    with patch("app.core.pikpak.taxonomy.classify_pikpak_failure", return_value=("throttled", "频控")):
        wrapped_mock = wrap_pikpak_failure(MockThrottled("503"))
        assert isinstance(wrapped_mock, PikPakPublishThrottledError)
        assert wrapped_mock.category == "throttled"


@pytest.mark.asyncio
async def test_vsse_checkpoint_skipping_and_strict_rollback():
    """测试 VSSE 分卷检查点命中时跳过已发分卷，以及严格回滚谓词 (暂停不删，取消才删)"""
    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value="http://cdn/video.mp4"))
    task_id = f"t_vsse_cp_{int(time.time() * 1000)}"

    # 1. 预设检查点：Part 1 已交付 (msg_id=9001)
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        params={
            "target_channel": "-1001",
            "pikpak_part_checkpoints": {
                "fid_vsse": {
                    "strategy": "vsse",
                    "total_parts": 2,
                    "threshold": 2044723200,
                    "delivered": {"1": 9001},
                }
            },
        },
    )

    try:
        item = PublishItem(item_id="fid_vsse", name="video.mp4", size_bytes=3000000000)
        context = PublishContext(task_id=task_id, target_chat_id="-1001", is_premium=False, delay_seconds=0.0)

        mock_plan1 = MagicMock(part_index=1, total_parts=2, envelope_length=1000, part_name="part1.mp4")
        mock_plan2 = MagicMock(part_index=2, total_parts=2, envelope_length=1000, part_name="part2.mp4")
        plans = [mock_plan1, mock_plan2]

        with (
            patch.object(
                engine, "_probe_and_precheck_stream_url", new_callable=AsyncMock, return_value="http://cdn/video.mp4"
            ),
            patch(
                "app.core.pipeline.engines.pikpak.engine.probe_vsse_envelope",
                new_callable=AsyncMock,
                return_value=(MagicMock(), plans, [b"hdr1", b"hdr2"]),
            ),
            patch.object(engine, "_fetch_engine_thumb", new_callable=AsyncMock, return_value=None),
            patch(
                "app.core.pipeline.engines.pikpak.vsse.stream_vsse_volume",
                new_callable=AsyncMock,
                return_value=(9002, 1000, 15.0),
            ) as mock_stream,
            patch(
                "app.core.pipeline.engines.pikpak.split_loop.delete_published_messages", new_callable=AsyncMock
            ) as mock_delete,
        ):
            # 执行 VSSE 发布
            outcome = await engine._publish_vsse_split_item(MagicMock(), item, 2044723200, context)

            # Part 1 命中检查点跳过，stream_vsse_volume 仅被 Part 2 调用 1 次
            assert mock_stream.call_count == 1
            assert outcome.message_ids == [9001, 9002]
            # 正常完成未调用删除
            mock_delete.assert_not_called()

            # 全部完成后检查点被清理
            row = TaskQueueRepository.get_by_id(task_id)
            assert "fid_vsse" not in (row["params"].get("pikpak_part_checkpoints") or {})

        # 2. 严格回滚测试：发生 UploadPausedError 暂停时严禁删除已发分卷
        with (
            patch.object(
                engine, "_probe_and_precheck_stream_url", new_callable=AsyncMock, return_value="http://cdn/video.mp4"
            ),
            patch(
                "app.core.pipeline.engines.pikpak.engine.probe_vsse_envelope",
                new_callable=AsyncMock,
                return_value=(MagicMock(), plans, [b"hdr1", b"hdr2"]),
            ),
            patch.object(engine, "_fetch_engine_thumb", new_callable=AsyncMock, return_value=None),
            patch(
                "app.core.pipeline.engines.pikpak.vsse.stream_vsse_volume",
                side_effect=[(9001, 1000, 10.0), PikPakThrottledError("503")],
            ),
            patch(
                "app.core.pipeline.engines.pikpak.split_loop.delete_published_messages", new_callable=AsyncMock
            ) as mock_delete_paused,
        ):
            with pytest.raises(PikPakThrottledError):
                await engine._publish_vsse_split_item(MagicMock(), item, 2044723200, context)

            # 非惩罚性保全: 503 频控中断严禁删除已发送的 Part 1
            mock_delete_paused.assert_not_called()

        # 2b. 严格回滚测试：发生分块预读瞬态异常 (ChunkFetchTransientError) 时同样严禁删除已发分卷
        from app.core.stream_chunk_provider import ChunkFetchTransientError

        await TaskQueueRepository.clear_part_checkpoint_async(task_id, item.item_id)
        with (
            patch.object(
                engine, "_probe_and_precheck_stream_url", new_callable=AsyncMock, return_value="http://cdn/video.mp4"
            ),
            patch(
                "app.core.pipeline.engines.pikpak.engine.probe_vsse_envelope",
                new_callable=AsyncMock,
                return_value=(MagicMock(), plans, [b"hdr1", b"hdr2"]),
            ),
            patch.object(engine, "_fetch_engine_thumb", new_callable=AsyncMock, return_value=None),
            patch(
                "app.core.pipeline.engines.pikpak.vsse.stream_vsse_volume",
                side_effect=[(9001, 1000, 10.0), ChunkFetchTransientError("read timeout")],
            ),
            patch(
                "app.core.pipeline.engines.pikpak.split_loop.delete_published_messages", new_callable=AsyncMock
            ) as mock_delete_transient,
        ):
            with pytest.raises(ChunkFetchTransientError):
                await engine._publish_vsse_split_item(MagicMock(), item, 2044723200, context)

            # 非惩罚性保全: 分块瞬断严禁删除已发送的 Part 1
            mock_delete_transient.assert_not_called()

        # 3. 严格回滚测试：用户显式取消任务时物理回滚已发分卷
        task_id_cancel = f"t_vsse_cancel_{int(time.time() * 1000)}"
        TaskQueueRepository.create(
            task_id=task_id_cancel, task_type="pikpak_publish", params={"target_channel": "-1001"}
        )
        try:
            # 开始执行 Part 1 成功，执行 Part 2 时触发取消
            is_cancelled_flag = False

            def check_cancelled():
                return is_cancelled_flag

            context_cancelled = PublishContext(
                task_id=task_id_cancel,
                target_chat_id="-1001",
                is_premium=False,
                is_cancelled=check_cancelled,
                delay_seconds=0.0,
            )

            async def stream_part(client, itm, plan, hdr, prov, ctx, capt, prior, th, meta, **kwargs):
                nonlocal is_cancelled_flag
                if plan.part_index == 1:
                    return 9001, 1000, 10.0
                # Part 2 执行时标记取消并抛出取消异常
                is_cancelled_flag = True
                raise UploadCancelledError("上传任务被取消")

            with (
                patch.object(
                    engine,
                    "_probe_and_precheck_stream_url",
                    new_callable=AsyncMock,
                    return_value="http://cdn/video.mp4",
                ),
                patch(
                    "app.core.pipeline.engines.pikpak.engine.probe_vsse_envelope",
                    new_callable=AsyncMock,
                    return_value=(MagicMock(), plans, [b"hdr1", b"hdr2"]),
                ),
                patch.object(engine, "_fetch_engine_thumb", new_callable=AsyncMock, return_value=None),
                patch("app.core.pipeline.engines.pikpak.vsse.stream_vsse_volume", side_effect=stream_part),
                patch(
                    "app.core.pipeline.engines.pikpak.split_loop.delete_published_messages", new_callable=AsyncMock
                ) as mock_delete_cancel,
            ):
                with pytest.raises(UploadCancelledError):
                    await engine._publish_vsse_split_item(MagicMock(), item, 2044723200, context_cancelled)

                # 取消时物理回滚 Part 1
                mock_delete_cancel.assert_called_once_with(ANY, -1001, [9001])
        finally:
            TaskQueueRepository.delete(task_id_cancel)

    finally:
        TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_fmp4_checkpoint_skipping_and_strict_rollback():
    """测试 fMP4 分卷检查点命中时跳过已发分卷，以及重规划与取消回滚"""
    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value="http://cdn/video.mkv"))
    task_id = f"t_fmp4_cp_{int(time.time() * 1000)}"

    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        params={
            "target_channel": "-1001",
            "pikpak_part_checkpoints": {
                "fid_fmp4": {
                    "strategy": "fmp4",
                    "total_parts": 2,
                    "threshold": 2044723200,
                    "delivered": {"1": 9501},
                }
            },
        },
    )

    try:
        item = PublishItem(item_id="fid_fmp4", name="video.mkv", size_bytes=3000000000)
        context = PublishContext(task_id=task_id, target_chat_id="-1001", is_premium=False, delay_seconds=0.0)

        mock_plan1 = MagicMock(part_index=1, total_parts=2, estimated_size=1000, part_name="part1.mp4")
        mock_plan2 = MagicMock(part_index=2, total_parts=2, estimated_size=1000, part_name="part2.mp4")
        plans = [mock_plan1, mock_plan2]

        with (
            patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
            patch(
                "app.core.pipeline.engines.pikpak.engine.probe_fmp4_media",
                new_callable=AsyncMock,
                return_value=(".mkv", True, 100.0, None),
            ),
            patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
            patch.object(engine, "_fetch_engine_thumb", new_callable=AsyncMock, return_value=None),
            patch(
                "app.core.pipeline.engines.pikpak.fmp4.stream_fmp4_volume",
                new_callable=AsyncMock,
                return_value=(9502, 1000, 20.0, "http://cdn/video.mkv"),
            ) as mock_fmp4_stream,
            patch(
                "app.core.pipeline.engines.pikpak.split_loop.delete_published_messages", new_callable=AsyncMock
            ) as mock_fmp4_delete,
        ):
            outcome = await engine._publish_fmp4_split_item(MagicMock(), item, 2044723200, context)

            # Part 1 跳过，仅调用 Part 2 传输 1 次
            assert mock_fmp4_stream.call_count == 1
            assert outcome.message_ids == [9501, 9502]
            mock_fmp4_delete.assert_not_called()

            # 全部完成后检查点被清理
            row = TaskQueueRepository.get_by_id(task_id)
            assert "fid_fmp4" not in (row["params"].get("pikpak_part_checkpoints") or {})

        # 严格回滚测试：发生中断时非惩罚性保全
        with (
            patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
            patch(
                "app.core.pipeline.engines.pikpak.engine.probe_fmp4_media",
                new_callable=AsyncMock,
                return_value=(".mkv", True, 100.0, None),
            ),
            patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
            patch.object(engine, "_fetch_engine_thumb", new_callable=AsyncMock, return_value=None),
            patch(
                "app.core.pipeline.engines.pikpak.fmp4.stream_fmp4_volume",
                side_effect=[(9501, 1000, 10.0, "http://cdn/video.mkv"), PikPakThrottledError("503")],
            ),
            patch(
                "app.core.pipeline.engines.pikpak.split_loop.delete_published_messages", new_callable=AsyncMock
            ) as mock_fmp4_delete_paused,
        ):
            with pytest.raises(PikPakThrottledError):
                await engine._publish_fmp4_split_item(MagicMock(), item, 2044723200, context)

            # 频控中断不删除
            mock_fmp4_delete_paused.assert_not_called()

        # 严格回滚测试：发生底层分块预读瞬态异常时同样严禁删除已发分卷
        from app.core.stream_chunk_provider import ChunkFetchTransientError

        await TaskQueueRepository.clear_part_checkpoint_async(task_id, item.item_id)
        with (
            patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
            patch(
                "app.core.pipeline.engines.pikpak.engine.probe_fmp4_media",
                new_callable=AsyncMock,
                return_value=(".mkv", True, 100.0, None),
            ),
            patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
            patch.object(engine, "_fetch_engine_thumb", new_callable=AsyncMock, return_value=None),
            patch(
                "app.core.pipeline.engines.pikpak.fmp4.stream_fmp4_volume",
                side_effect=[(9501, 1000, 10.0, "http://cdn/video.mkv"), ChunkFetchTransientError("chunk timeout")],
            ),
            patch(
                "app.core.pipeline.engines.pikpak.split_loop.delete_published_messages", new_callable=AsyncMock
            ) as mock_fmp4_delete_reset,
        ):
            with pytest.raises(ChunkFetchTransientError):
                await engine._publish_fmp4_split_item(MagicMock(), item, 2044723200, context)

            # 分块瞬态异常严禁删除已发送的 Part 1
            mock_fmp4_delete_reset.assert_not_called()

        # 严格回滚测试：取消时物理回滚
        task_id_fmp4_cancel = f"t_fmp4_cancel_{int(time.time() * 1000)}"
        TaskQueueRepository.create(
            task_id=task_id_fmp4_cancel, task_type="pikpak_publish", params={"target_channel": "-1001"}
        )
        try:
            is_fmp4_cancelled = False

            def check_fmp4_cancelled():
                return is_fmp4_cancelled

            context_fmp4_cancel = PublishContext(
                task_id=task_id_fmp4_cancel,
                target_chat_id="-1001",
                is_premium=False,
                is_cancelled=check_fmp4_cancelled,
                delay_seconds=0.0,
            )

            async def stream_fmp4_part(*args, **kwargs):
                nonlocal is_fmp4_cancelled
                plan = args[2] if len(args) > 2 else kwargs.get("plan")
                if plan and plan.part_index == 1:
                    return 9501, 1000, 10.0, "http://cdn/video.mkv"
                is_fmp4_cancelled = True
                raise UploadCancelledError("取消上传")

            with (
                patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
                patch(
                    "app.core.pipeline.engines.pikpak.engine.probe_fmp4_media",
                    new_callable=AsyncMock,
                    return_value=(".mkv", True, 100.0, None),
                ),
                patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
                patch.object(engine, "_fetch_engine_thumb", new_callable=AsyncMock, return_value=None),
                patch("app.core.pipeline.engines.pikpak.fmp4.stream_fmp4_volume", side_effect=stream_fmp4_part),
                patch(
                    "app.core.pipeline.engines.pikpak.split_loop.delete_published_messages", new_callable=AsyncMock
                ) as mock_fmp4_delete_cancel,
            ):
                with pytest.raises(UploadCancelledError):
                    await engine._publish_fmp4_split_item(MagicMock(), item, 2044723200, context_fmp4_cancel)

                mock_fmp4_delete_cancel.assert_called_once_with(ANY, -1001, [9501])
        finally:
            TaskQueueRepository.delete(task_id_fmp4_cancel)

    finally:
        TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_raw_checkpoint_skipping():
    """测试 Raw 分卷上传命中检查点跳过已投递分卷并清理检查点"""
    from app.core.stream_splitter import PartSpec

    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value="http://cdn/archive.zip"))
    task_id = f"t_raw_cp_{int(time.time() * 1000)}"

    threshold = 2044723200
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        params={
            "target_channel": "-1001",
            "pikpak_part_checkpoints": {
                "fid_raw": {
                    "strategy": "raw",
                    "total_parts": 2,
                    "threshold": threshold,
                    "delivered": {"1": 9801},
                }
            },
        },
    )

    try:
        item = PublishItem(item_id="fid_raw", name="archive.zip", size_bytes=3000000000)
        context = PublishContext(
            task_id=task_id,
            target_chat_id="-1001",
            is_premium=False,
            video_split_threshold_bytes=threshold,
            delay_seconds=0.0,
        )

        parts = [
            PartSpec(part_index=1, offset=0, length=1500000000),
            PartSpec(part_index=2, offset=1500000000, length=1500000000),
        ]

        mock_client = MagicMock()
        mock_sent = MagicMock(id=9802)
        mock_client.send_file = AsyncMock(return_value=mock_sent)
        mock_provider = MagicMock()
        mock_provider.total_bytes = 3000000000
        mock_provider.download_speed_mb_s = 25.0

        with patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()):
            # raw 分卷上传所有权已迁 single.upload_parts (engine._upload_parts 消亡)
            last_msg_id, last_speed = await pikpak_single.upload_parts(
                engine,
                client=mock_client,
                provider=mock_provider,
                item=item,
                parts=parts,
                context=context,
            )

            # Part 1 跳过，send_file 仅为 Part 2 调用 1 次
            assert mock_client.send_file.call_count == 1
            assert last_msg_id == 9802

            # 全部完成后检查点被清理
            row = TaskQueueRepository.get_by_id(task_id)
            assert "fid_raw" not in (row["params"].get("pikpak_part_checkpoints") or {})

    finally:
        TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_pikpak_tiered_cross_strike_escalation():
    """测试混部频控 (Throttle) 与数据面质询 (Data Risk) 的跨阶梯累加与 Tier 3 Safe-Pause 熔断"""
    scheduler = TaskScheduler(check_interval=0.1)
    task_id = f"t_tier_cross_{int(time.time() * 1000)}"

    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        params={
            "target_channel": "-1001",
            "upload_concurrency": 4,
            "send_delay_seconds": 3.0,
        },
    )

    try:
        # Strike 1: Level 2 频控 (503) -> Tier 1 (浅度冷却 15m)
        exc_th1 = PikPakThrottledError("HTTP 503", status=503)
        await scheduler._handle_pikpak_tiered_cooldown(
            task_id=task_id,
            retry_count=0,
            max_retries=3,
            error_type="throttled",
            retry_after=None,
            exc=exc_th1,
        )
        row1 = TaskQueueRepository.get_by_id(task_id)
        assert row1["status"] == "queued"
        assert row1["params"]["throttle_strike_count"] == 1
        assert row1["params"]["data_risk_strike_count"] == 0

        # Strike 2: Level 3a 数据面拦截 (HTML) -> Tier 2 (深度冷却 40m + 稳态降档)
        exc_dp = ProviderPikPakRiskControlError("HTML challenge")
        await scheduler._handle_pikpak_tiered_cooldown(
            task_id=task_id,
            retry_count=1,
            max_retries=3,
            error_type="data_risk_control",
            retry_after=None,
            exc=exc_dp,
        )
        row2 = TaskQueueRepository.get_by_id(task_id)
        assert row2["status"] == "queued"
        assert row2["params"]["throttle_strike_count"] == 1
        assert row2["params"]["data_risk_strike_count"] == 1
        assert row2["params"]["auto_downshifted"] is True
        assert row2["params"]["pikpak_prefetch_concurrency"] == 1

        # Strike 3: 再次遭遇 Level 2 频控 -> 累计达到 3 次总拦截，必须直接 Safe-Pause (Tier 3)
        exc_th2 = PikPakThrottledError("HTTP 503", status=503)
        await scheduler._handle_pikpak_tiered_cooldown(
            task_id=task_id,
            retry_count=2,
            max_retries=3,
            error_type="throttled",
            retry_after=None,
            exc=exc_th2,
        )
        row3 = TaskQueueRepository.get_by_id(task_id)
        assert row3["status"] == "paused"
        assert "风控安全挂起" in row3["error_detail"]
        assert row3["params"]["throttle_strike_count"] == 2
        assert row3["params"]["data_risk_strike_count"] == 1

    finally:
        TaskQueueRepository.delete(task_id)
        scheduler.clear_pikpak_cooldown()


@pytest.mark.asyncio
async def test_task_repository_malformed_checkpoint_params():
    """测试当 params 中 pikpak_part_checkpoints 为 None 或非法格式时的防御性容错"""
    task_id = f"t_malformed_cp_{int(time.time() * 1000)}"
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        params={"target_channel": "-1001", "pikpak_part_checkpoints": None},
    )

    try:
        # 记录检查点应自动修正非 dict 类型并成功写入
        await TaskQueueRepository.record_part_checkpoint_async(
            task_id=task_id,
            file_id="fid_corrupt",
            strategy="vsse",
            total_parts=2,
            threshold=1000,
            part_index=1,
            msg_id=12345,
        )

        row = TaskQueueRepository.get_by_id(task_id)
        assert isinstance(row["params"]["pikpak_part_checkpoints"], dict)
        assert row["params"]["pikpak_part_checkpoints"]["fid_corrupt"]["delivered"] == {"1": 12345}

        # 清理检查点
        await TaskQueueRepository.clear_part_checkpoint_async(task_id, "fid_corrupt")
        row2 = TaskQueueRepository.get_by_id(task_id)
        assert "fid_corrupt" not in row2["params"]["pikpak_part_checkpoints"]

    finally:
        TaskQueueRepository.delete(task_id)


@pytest.mark.asyncio
async def test_canary_resume_serial_probe():
    """测试金丝雀单块恢复 (Canary Resume):
    前 16 个分块 (8 MiB) 强制以单并发 (prefetch_concurrency = 1) 串行拉取探路，
    第 16 块之后平滑放开至目标并发度拉流。"""
    from telethon import types

    from app.config import settings
    from app.core.fast_telethon import upload_file_parallel

    chunk_size = 512 * 1024
    total_parts = 20
    total_bytes = total_parts * chunk_size

    in_flight_counts = []
    current_in_flight = 0
    in_flight_lock = asyncio.Lock()

    class MockCanaryProvider:
        def __init__(self):
            self.total_bytes = total_bytes
            self.file_id = "canary_fid"
            self.file_name = "canary.mp4"
            self.download_speed_mb_s = 20.0

        async def read_chunk(self, offset: int, size: int) -> bytes:
            nonlocal current_in_flight
            part_id = offset // chunk_size
            async with in_flight_lock:
                current_in_flight += 1
                in_flight_counts.append((part_id, current_in_flight))
            try:
                await asyncio.sleep(0.01)
                return b"X" * size
            finally:
                async with in_flight_lock:
                    current_in_flight -= 1

        async def aclose(self):
            pass

    client = AsyncMock()
    client.session = MagicMock(dc_id=4)
    client.upload_file = AsyncMock(
        return_value=types.InputFile(id=123, parts=total_parts, name="canary.mp4", md5_checksum="")
    )

    os.makedirs(settings.PIKPAK_META_DIR, exist_ok=True)
    meta_file = os.path.join(settings.PIKPAK_META_DIR, "canary.upload.meta")
    if os.path.exists(meta_file):
        os.remove(meta_file)
    provider = MockCanaryProvider()

    try:
        res = await upload_file_parallel(
            client,
            "pikpak://canary_fid",
            concurrency=4,
            prefetch_concurrency=4,
            in_flight_per_connection=1,
            chunk_provider=provider,
            meta_path=meta_file,
        )

        assert isinstance(res, types.InputFile)
        assert res.parts == total_parts

        # 校验前 16 个分块 (part_id 0..15) 在途并发数严格为 1
        canary_part_concurrencies = [count for pid, count in in_flight_counts if pid < 16]
        assert len(canary_part_concurrencies) == 16
        assert all(c == 1 for c in canary_part_concurrencies), (
            f"前 16 块金丝雀探针期间在途并发必须严格为 1: {canary_part_concurrencies}"
        )

        # 校验全部 20 块分块均已拉取
        assert len(in_flight_counts) == total_parts
    finally:
        if os.path.exists(meta_file):
            os.remove(meta_file)


@pytest.mark.asyncio
async def test_canary_resume_halts_immediately_on_throttle():
    """测试金丝雀探针遭遇 503 频控时 0 微观重试立即刹停"""
    from app.config import settings
    from app.core.fast_telethon import upload_file_parallel

    chunk_size = 512 * 1024
    total_parts = 20
    total_bytes = total_parts * chunk_size
    dispatched_parts = []

    class MockThrottleProvider:
        def __init__(self):
            self.total_bytes = total_bytes
            self.file_id = "throttle_fid"
            self.file_name = "throttle.mp4"
            self.download_speed_mb_s = 20.0

        async def read_chunk(self, offset: int, size: int) -> bytes:
            part_id = offset // chunk_size
            dispatched_parts.append(part_id)
            if part_id == 2:
                raise PikPakThrottledError("HTTP 503 Service Unavailable", status=503)
            return b"Y" * size

        async def aclose(self):
            pass

    client = AsyncMock()
    client.session = MagicMock(dc_id=4)
    os.makedirs(settings.PIKPAK_META_DIR, exist_ok=True)
    meta_file = os.path.join(settings.PIKPAK_META_DIR, "throttle.upload.meta")
    if os.path.exists(meta_file):
        os.remove(meta_file)
    provider = MockThrottleProvider()

    try:
        with pytest.raises(PikPakThrottledError):
            await upload_file_parallel(
                client,
                "pikpak://throttle_fid",
                concurrency=4,
                prefetch_concurrency=4,
                chunk_provider=provider,
                meta_path=meta_file,
            )

        # 确认为第 3 块 (part_id=2) 抛错后立即刹停，后续分块绝不发起预读
        assert 2 in dispatched_parts
        assert 3 not in dispatched_parts
    finally:
        if os.path.exists(meta_file):
            os.remove(meta_file)


@pytest.mark.asyncio
async def test_resume_task_clears_pikpak_cooldown():
    """测试用户手动恢复 Safe-Paused 的 PikPak 任务时清除全局冷却锁"""
    from app.api.tasks import resume_task
    from app.core.scheduler import task_scheduler

    task_id = f"t_resume_cd_{int(time.time() * 1000)}"
    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        status="paused",
        params={
            "target_channel": "-1001",
            "remote_files": [{"file_id": "f_res", "name": "res.mp4"}],
        },
    )

    try:
        # 设置全局冷却
        task_scheduler.set_pikpak_cooldown(time.time() + 900.0)
        assert task_scheduler.is_pikpak_in_cooldown() is True

        mock_acc = MagicMock(account_id=1, max_concurrency=5, is_flood_locked=lambda: False)
        with (
            patch.object(type(task_scheduler), "is_running", True),
            patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
            patch("app.db.publish_repository.PublishHistoryRepository.is_published", return_value=False),
        ):
            resp = await resume_task(task_id)
            assert resp["status"] == "queued"
            # 冷却锁已被清除
            assert task_scheduler.is_pikpak_in_cooldown() is False
    finally:
        TaskQueueRepository.delete(task_id)
        task_scheduler.clear_pikpak_cooldown()


@pytest.mark.asyncio
async def test_scheduler_startup_restores_unexpired_cooldown():
    """测试调度器启动时自动从 DB 恢复未到期的全局 PikPak 冷却锁 (服务重启保护)"""
    scheduler = TaskScheduler(check_interval=0.1)
    task_id = f"t_start_cd_{int(time.time() * 1000)}"
    cooldown_target = time.time() + 600.0

    TaskQueueRepository.create(
        task_id=task_id,
        task_type="pikpak_publish",
        status="queued",
        params={"target_channel": "-1001"},
    )
    # 设置 DB 中的 next_retry_at 为未来 600 秒
    TaskQueueRepository.mark_retry(task_id, 1, cooldown_target, "冷却中")

    try:
        assert scheduler.is_pikpak_in_cooldown() is False
        # 启动调度器应从 DB 恢复冷却锁
        await scheduler.start()
        assert scheduler.is_pikpak_in_cooldown() is True
        assert scheduler._pikpak_cooldown_until >= cooldown_target - 5.0
    finally:
        await scheduler.stop()
        TaskQueueRepository.delete(task_id)
        scheduler.clear_pikpak_cooldown()
