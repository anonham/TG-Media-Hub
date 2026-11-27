"""ProgressReporter 进度分发器契约测试 (characterization).

锁定统一进度计算的双路径 (plan 分卷比例 / prior_bytes 直加), tolerant/strict 双形态
分卷钩子, 控制信号前置检查, 与回调缺失零开销语义。
"""

from typing import Any
from unittest.mock import MagicMock

import pytest

from app.core.pipeline.engines._common import ProgressReporter
from app.core.pipeline.models import PublishContext, PublishItem
from app.core.scheduler import UploadCancelledError


def _make_item(size_bytes: int = 1000) -> PublishItem:
    return PublishItem(item_id="f1", name="a.mp4", size_bytes=size_bytes, source_type="pikpak")


def _make_context(callback=None, **kw: Any) -> PublishContext:
    ctx = PublishContext(task_id="t1", target_chat_id=-100, **kw)
    ctx.progress_callback = callback
    return ctx


class TestComputeFileFraction:
    def test_no_plan_uses_prior_bytes_and_item_size(self) -> None:
        reporter = ProgressReporter(_make_item(1000), _make_context())
        file_done, fraction, est_total = reporter.compute_file_fraction(50, 100)
        assert (file_done, fraction, est_total) == (50, 0.05, 1000)

    def test_no_plan_with_prior_bytes(self) -> None:
        reporter = ProgressReporter(_make_item(1000), _make_context())
        file_done, fraction, _ = reporter.compute_file_fraction(100, 100, prior_bytes=300)
        assert (file_done, fraction) == (400, 0.4)

    def test_no_plan_est_total_never_shrinks_below_done(self) -> None:
        reporter = ProgressReporter(_make_item(100), _make_context())
        file_done, fraction, est_total = reporter.compute_file_fraction(80, 100, prior_bytes=50)
        assert file_done == 130
        assert est_total == 130
        assert fraction == 1.0

    def test_plan_path_interpolates_across_volumes(self) -> None:
        reporter = ProgressReporter(_make_item(4000), _make_context())
        plan = MagicMock()
        plan.part_index = 2
        plan.total_parts = 4
        file_done, fraction, _ = reporter.compute_file_fraction(50, 100, plan=plan, prior_bytes=1000)
        assert file_done == 1050
        assert fraction == pytest.approx((1 + 0.5) / 4)

    def test_zero_total_is_safe(self) -> None:
        reporter = ProgressReporter(_make_item(1000), _make_context())
        file_done, fraction, _ = reporter.compute_file_fraction(0, 0)
        assert (file_done, fraction) == (0, 0.0)


class TestEmitProgress:
    def test_emits_overall_position_and_bytes(self) -> None:
        cb = MagicMock()
        ctx = _make_context(
            cb, completed_files_count=2, total_files_count=5, completed_bytes=3000, total_bytes_sum=9000
        )
        reporter = ProgressReporter(_make_item(1000), ctx, provider=None, log_label="VSSE")
        reporter.emit_progress(current_bytes=100, total_bytes=1000, speed=1.5, eta=3.0)
        cb.assert_called_once()
        args, kwargs = cb.call_args
        assert args[:5] == (3, 5, 1.5, 3.0, "a.mp4")  # cur_item=min(5, 2+1), tot_files
        assert kwargs["percent"] == pytest.approx((2 + 0.1) / 5 * 100)
        assert kwargs["total_bytes_done"] == 3100  # completed_bytes + file_done
        assert kwargs["total_bytes_all"] == 9000
        assert kwargs["concurrency"] == 1

    def test_tolerant_args_injects_dc_and_concurrency_total(self) -> None:
        cb = MagicMock()
        ctx = _make_context(cb, upload_concurrency=4)
        reporter = ProgressReporter(_make_item(1000), ctx)
        reporter.emit_progress(current_bytes=10, total_bytes=100, tolerant_args=True, dc_id=7, workers_total=0)
        kwargs = cb.call_args.kwargs
        assert kwargs["dc_id"] == 7
        assert kwargs["concurrency_total"] == 4  # workers_total 缺省回落 context.upload_concurrency

    def test_no_callback_is_noop(self) -> None:
        reporter = ProgressReporter(_make_item(1000), _make_context(None))
        reporter.emit_progress(current_bytes=10, total_bytes=100)  # 不抛即通过

    def test_pikpak_download_speed_preferred_over_provider(self) -> None:
        cb = MagicMock()
        provider = MagicMock()
        provider.download_speed_mb_s = 9.9
        ctx = _make_context(cb)
        reporter = ProgressReporter(_make_item(1000), ctx, provider=provider)
        reporter.emit_progress(current_bytes=10, total_bytes=100, pikpak_download_speed=3.3)
        assert cb.call_args.kwargs["pikpak_download_speed_mb_s"] == 3.3


class TestSplitPartHooks:
    def test_tolerant_hook_parses_workers_dc_workers_total(self) -> None:
        cb = MagicMock()
        reporter = ProgressReporter(_make_item(2000), _make_context(cb))
        plan = MagicMock()
        plan.part_index = 1
        plan.total_parts = 2
        hook = reporter.make_split_part_hook(plan, prior_bytes=0, tolerant_args=True)
        hook(100, 1000, 2.0, 1.0, 3, 8, 5)
        kwargs = cb.call_args.kwargs
        assert kwargs["concurrency"] == 3
        assert kwargs["dc_id"] == 8
        assert kwargs["concurrency_total"] == 5

    def test_strict_hook_uses_positional_contract(self) -> None:
        cb = MagicMock()
        reporter = ProgressReporter(_make_item(2000), _make_context(cb))
        plan = MagicMock()
        plan.part_index = 1
        plan.total_parts = 1
        hook = reporter.make_split_part_hook(plan, prior_bytes=0, tolerant_args=False)
        hook(100, 1000, 2.0, 1.0, "ignored-name", 4)
        kwargs = cb.call_args.kwargs
        assert kwargs["concurrency"] == 4
        assert "dc_id" not in kwargs  # strict 形态不下发 dc 细分

    def test_hook_raises_control_signal_before_emit(self) -> None:
        ctx = _make_context(MagicMock())
        ctx.is_cancelled = lambda: True
        reporter = ProgressReporter(_make_item(2000), ctx)
        plan = MagicMock()
        plan.part_index = 1
        plan.total_parts = 1
        hook = reporter.make_split_part_hook(plan, prior_bytes=0)
        with pytest.raises(UploadCancelledError):
            hook(1, 10, 0.0, 0.0, "name", 1)


class TestPartHook:
    def test_part_hook_uses_part_length_and_prior_bytes(self) -> None:
        cb = MagicMock()
        ctx = _make_context(cb, completed_files_count=1, total_files_count=3, completed_bytes=500, total_bytes_sum=3000)
        parts = [MagicMock(length=1000), MagicMock(length=2000)]
        reporter = ProgressReporter(_make_item(3000), ctx, provider=None, log_label="PikPak")
        hook = reporter.make_part_hook(parts, part_index=2, prior_bytes=1000)
        hook(500, 2000, 1.0, 2.0, 2, 4, 6)
        args, kwargs = cb.call_args
        assert args[:5] == (2, 3, 1.0, 2.0, "a.mp4")
        # total_bytes_done = completed_bytes(500) + file_done(prior 1000 + part_done 500) = 2000
        assert kwargs["total_bytes_done"] == 2000
        assert kwargs["total_bytes_all"] == 3000
        assert kwargs["concurrency_total"] == 6
