"""属性构建统一断言 (build_media_attributes 覆盖参数语义 + 双通道收编)。

对应 docs/architecture/PUBLISH_PIPELINE.md §2.2 (build_media_attributes 统一构建):
- 构建器单元: 覆盖优先 / 缺省回落 item 字段 / eligible=False 仍空 / 音频优先
  不回归 / 覆盖值 0 与 None 的区分语义;
- VSSE 通道级: 多卷场景每卷 attributes.duration == int(对应卷 plan.duration_sec)
  (既有契约测试只锁 supports_streaming 与宽高, duration 是统一前回归盲区);
- fMP4 通道级: 上传器实测时长优先于 plan 时长, 实测缺失回落 plan。
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.iso_bmff.fragmented import Fmp4PartPlan
from app.core.pipeline.engines import PikPakStreamTransportEngine
from app.core.pipeline.models import PublishItem
from app.core.pipeline.utils import build_media_attributes
from tests.pipeline.test_fmp4_engine_contract import _make_context as _fmp4_ctx
from tests.pipeline.test_vsse_engine_contract import (
    MemorySyntheticChunkProvider,
    _build_test_mp4_header,
)
from tests.pipeline.test_vsse_engine_contract import (
    _make_context as _vsse_ctx,
)


def _video_item(**kw) -> PublishItem:
    defaults = dict(
        item_id="D:\\tv\\e01.mp4",
        name="e01.mp4",
        size_bytes=1000,
        duration_s=40,
        width=640,
        height=360,
        source_type="local",
    )
    defaults.update(kw)
    return PublishItem(**defaults)


# ---------------------------------------------------------------------------
# 构建器单元: 覆盖参数语义
# ---------------------------------------------------------------------------


def test_builder_override_takes_priority_over_item_fields():
    attrs = build_media_attributes(_video_item(), duration_s=12.9, width=1920, height=1080)
    assert len(attrs) == 1
    assert attrs[0].duration == 12
    assert attrs[0].w == 1920
    assert attrs[0].h == 1080
    assert attrs[0].supports_streaming is True


def test_builder_none_override_falls_back_to_item_fields():
    attrs = build_media_attributes(_video_item(), duration_s=None, width=None, height=None)
    assert attrs[0].duration == 40
    assert attrs[0].w == 640
    assert attrs[0].h == 360


def test_builder_partial_override_mixes_with_item_fields():
    attrs = build_media_attributes(_video_item(), duration_s=7)
    assert attrs[0].duration == 7
    assert attrs[0].w == 640
    assert attrs[0].h == 360


def test_builder_zero_override_distinct_from_none():
    # 显式 0 不回落 item 字段 (分卷通道宽高链解析出 0 时必须如实记录 0)
    attrs = build_media_attributes(_video_item(), width=0, height=0)
    assert attrs[0].w == 0
    assert attrs[0].h == 0


def test_builder_eligible_false_returns_empty_even_with_overrides():
    assert build_media_attributes(_video_item(), eligible=False, duration_s=9, width=1, height=1) == []


def test_builder_audio_priority_not_regressed_by_duration_override():
    # 音频判定只读 item 扩展名: 音频条目即便传入视频时长覆盖也绝不误注 Video 属性
    item = PublishItem(item_id="D:\\m\\s.mp3", name="s.mp3", size_bytes=100, duration_s=180, source_type="local")
    attrs = build_media_attributes(item, duration_s=99)
    assert len(attrs) == 1
    assert type(attrs[0]).__name__ == "DocumentAttributeAudio"
    assert attrs[0].duration == 180


# ---------------------------------------------------------------------------
# VSSE 通道级: 每卷 duration 来自对应卷 plan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_vsse_per_part_duration_matches_plan_in_unified_builder():
    import dataclasses

    from app.core.iso_bmff.box_parser import parse_moov, scan_top_level_boxes
    from app.core.vsse_provider import plan_vsse_volumes

    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/dur.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    file_size = 3000 * 1024 * 1024
    item = PublishItem(
        item_id="fid_attr_dur",
        name="attr_dur.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="aac",
        video_codec="h264",
        source_type="pikpak",
    )
    header_bytes = _build_test_mp4_header(total_bytes=file_size)
    mock_provider = MemorySyntheticChunkProvider(
        file_id=item.item_id,
        file_name=item.name,
        total_bytes=file_size,
        header_bytes=header_bytes,
    )
    mock_client = AsyncMock()
    mock_client.send_file.side_effect = [MagicMock(id=301), MagicMock(id=302)]
    ctx = _vsse_ctx(userbot_client=mock_client, split_strategy="vsse_with_fallback")

    # 引擎用同一纯函数规划分卷 —— 测试侧先重算真实几何, 再替换各卷时长为不同值
    # (合成头两卷等时长, 等值断言无法区分"取对应卷"与"取固定值"; 仅替换 duration_sec,
    # 几何字段原样保留, 头部合成/封套 provider 不受影响)
    threshold = 2000 * 1024 * 1024
    boxes = scan_top_level_boxes(header_bytes, file_size)
    moov_header = next(b for b in boxes if b.box_type == b"moov")
    moov_bytes = header_bytes[moov_header.offset : moov_header.offset + moov_header.size]
    metadata = parse_moov(moov_bytes)
    base_plans = plan_vsse_volumes(meta=metadata, threshold_bytes=threshold, file_name=item.name)
    assert len(base_plans) == 2
    expected_plans = [
        dataclasses.replace(base_plans[0], duration_sec=111.5),
        dataclasses.replace(base_plans[1], duration_sec=222.25),
    ]

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
        patch("app.core.vsse_provider.plan_vsse_volumes", return_value=expected_plans),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_upload,
    ):
        mock_upload.side_effect = [MagicMock(), MagicMock()]
        res = await engine.send_item(item, ctx)

    assert res.message_ids == [301, 302]
    assert mock_client.send_file.call_count == 2
    for idx, plan in enumerate(expected_plans):
        kwargs = mock_client.send_file.call_args_list[idx].kwargs
        attrs = kwargs["attributes"]
        assert len(attrs) == 1
        assert attrs[0].duration == int(plan.duration_sec), f"part {idx + 1} duration 必须等于本卷 plan.duration_sec"
        assert attrs[0].supports_streaming is True
        assert kwargs["supports_streaming"] is True
        # 宽高链: item 无宽高 → stsd 0 → tkhd 回退 (与既有契约测试同源)
        assert attrs[0].w == 1920
        assert attrs[0].h == 1080


# ---------------------------------------------------------------------------
# fMP4 通道级: actual_dur 优先 + plan 回落
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fmp4_actual_duration_priority_and_plan_fallback():
    mock_link_fn = AsyncMock(return_value=["https://cdn.example.com/fmp4dur.mp4"])
    engine = PikPakStreamTransportEngine(link_provider=mock_link_fn)

    item = PublishItem(
        item_id="fid_fmp4_dur",
        name="fmp4_dur.mp4",
        size_bytes=3000 * 1024 * 1024,
        duration_s=2000.0,
        width=0,
        height=0,
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.send_file = AsyncMock(return_value=MagicMock(id=401))
    ctx = _fmp4_ctx(userbot_client=mock_client, split_strategy="fmp4_universal")

    plans = [
        Fmp4PartPlan(
            part_index=1, total_parts=2, start_sec=0, end_sec=1000, duration_sec=1000.0, part_name="d.part1.mp4"
        ),
        Fmp4PartPlan(
            part_index=2, total_parts=2, start_sec=1000, end_sec=2000, duration_sec=1000.5, part_name="d.part2.mp4"
        ),
    ]

    async def fake_stream(self_uploader, **kwargs):
        self_uploader.video_width = 1920
        self_uploader.video_height = 1080
        self_uploader.total_emitted_bytes = 1000
        # 第 1 卷: 实测时长存在 (优先); 第 2 卷: 实测缺失 (回落 plan)
        return MagicMock(), 999.5 if kwargs.get("start_sec", 0) == 0 else None

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
        patch("app.core.keyframe_prober.Mp4StssProber.probe_from_url", new_callable=AsyncMock, return_value=None),
        patch("app.core.pikpak.thumbnail.fetch_thumbnail", new_callable=AsyncMock, return_value=None),
        patch("app.core.pikpak.thumbnail.extract_frame_ffmpeg", new_callable=AsyncMock, return_value=None),
        patch("app.core.iso_bmff.fragmented.plan_video_fmp4_parts", return_value=plans),
        patch("app.core.fmp4_pipe_uploader.ZeroDiskFmp4StreamUploader.stream_to_telegram", new=fake_stream),
    ):
        await engine.send_item(item, ctx)

    assert mock_client.send_file.await_count == 2
    first = mock_client.send_file.await_args_list[0].kwargs
    second = mock_client.send_file.await_args_list[1].kwargs
    assert first["attributes"][0].duration == 999  # int(999.5) 实测优先
    assert second["attributes"][0].duration == 1000  # actual_dur None → plan 1000.5
    # 宽高链: item 0 → uploader 探测值采用 (与既有 契约同源)
    assert first["attributes"][0].w == 1920
    assert first["attributes"][0].h == 1080
