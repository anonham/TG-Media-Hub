"""本地视频消息属性探测测试。

- golden (真实 ffmpeg, 门控口径同既有 golden——不设 skip, 缺失即失败):
  MP4 探测回填 duration/宽高; MKV 仅 duration, 宽高保持缺省;
- 缓存: 同 (path,size,mtime) 二次调用不重探测, mtime 变更触发重探;
- 引擎级: send_item 送达 Telethon 的 attributes 带真实探测值 (mock client 捕获 kwargs);
- 降级: 探测异常 → 字段保持缺省 (现状语义) 且不抛出; 假路径不改写。
"""

import os
import subprocess
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telethon.tl.types import DocumentAttributeVideo

from app.core.keyframe_prober import CuesProbeResult
from app.core.pipeline.engines import LocalDiskTransportEngine
from app.core.pipeline.models import PublishContext, PublishItem


def _gen_video(path: str, w: int = 320, h: int = 240, dur: int = 5):
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            f"testsrc2=size={w}x{h}:rate=30:duration={dur}",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=44100",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-shortest",
            str(path),
        ],
        check=True,
    )


def _make_item(path, name=None):
    return PublishItem(
        item_id=str(path),
        name=name or os.path.basename(str(path)),
        size_bytes=os.path.getsize(str(path)),
        source_type="local",
    )


def _make_ctx(userbot_client=None):
    return PublishContext(task_id="T-PROBE", target_chat_id=-100999, userbot_client=userbot_client)


# ---------------------------------------------------------------------------
# golden: 真实探测器
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mp4_probe_backfills_metadata(tmp_path):
    video = tmp_path / "clip.mp4"
    _gen_video(str(video))
    item = _make_item(video)

    await LocalDiskTransportEngine()._ensure_local_video_metadata(item)

    assert item.duration_s is not None and abs(float(item.duration_s) - 5.0) <= 0.5
    assert item.width == 320
    assert item.height == 240


@pytest.mark.asyncio
async def test_mkv_probe_backfills_duration_and_dims(tmp_path):
    video = tmp_path / "clip.mkv"
    _gen_video(str(video))
    item = _make_item(video)

    await LocalDiskTransportEngine()._ensure_local_video_metadata(item)

    assert item.duration_s is not None and abs(float(item.duration_s) - 5.0) <= 0.5
    assert item.width == 320
    assert item.height == 240


# ---------------------------------------------------------------------------
# 缓存语义
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cache_hit_and_mtime_invalidation(tmp_path, monkeypatch):
    video = tmp_path / "cached.mp4"
    _gen_video(str(video))
    calls = []

    async def fake_probe(url, file_size=None):
        calls.append(url)
        return CuesProbeResult(duration_s=7.0, timescale=1000, has_cues=True, width=640, height=360)

    monkeypatch.setattr("app.core.keyframe_prober.Mp4StssProber.probe_from_url", fake_probe)
    engine = LocalDiskTransportEngine()

    item1 = _make_item(video)
    item2 = _make_item(video)
    await engine._ensure_local_video_metadata(item1)
    await engine._ensure_local_video_metadata(item2)
    assert len(calls) == 1  # 同 (path,size,mtime) 命中缓存
    assert item2.duration_s == 7.0 and item2.width == 640 and item2.height == 360

    # mtime 变更 → 缓存失效重探
    st = os.stat(video)
    os.utime(video, (st.st_atime, st.st_mtime + 10))
    item3 = _make_item(video)
    await engine._ensure_local_video_metadata(item3)
    assert len(calls) == 2


# ---------------------------------------------------------------------------
# 降级路径
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_probe_failure_degrades_silently(tmp_path, monkeypatch):
    video = tmp_path / "broken.mp4"
    _gen_video(str(video))

    async def boom(url, ext):
        raise RuntimeError("probe boom")

    monkeypatch.setattr("app.core.pipeline.engines.local_disk.probe_local_video_metadata", boom)
    item = _make_item(video)
    await LocalDiskTransportEngine()._ensure_local_video_metadata(item)  # 不得抛出
    assert item.duration_s is None and item.width is None and item.height is None


@pytest.mark.asyncio
async def test_fake_path_untouched():
    # 既有本地引擎测试的假路径形态: 探测直接跳过, 字段保持缺省 (E4 兼容前提)
    item = PublishItem(item_id="D:/fake/nope.mp4", name="nope.mp4", size_bytes=100, source_type="local")
    await LocalDiskTransportEngine()._ensure_local_video_metadata(item)
    assert item.duration_s is None and item.width is None and item.height is None


@pytest.mark.asyncio
async def test_unsupported_video_extension_skipped(tmp_path):
    video = tmp_path / "clip.avi"
    video.write_bytes(b"RIFF....")  # 内容无关: 扩展名已直接跳过探测
    item = _make_item(video)
    await LocalDiskTransportEngine()._ensure_local_video_metadata(item)
    assert item.duration_s is None


# ---------------------------------------------------------------------------
# 引擎级: send_item attributes 携带探测值
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_item_attributes_carry_probed_values(tmp_path):
    video = tmp_path / "attr.mp4"
    _gen_video(str(video))
    item = _make_item(video)

    mock_client = AsyncMock()
    sent = MagicMock()
    sent.id = 4242
    mock_client.send_file.return_value = sent
    ctx = _make_ctx(userbot_client=mock_client)

    with patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_ft:
        mock_ft.return_value = MagicMock()
        outcome = await LocalDiskTransportEngine().send_item(item, ctx)

    assert outcome.message_ids == [4242]
    kwargs = mock_client.send_file.call_args.kwargs
    video_attrs = [a for a in (kwargs.get("attributes") or []) if isinstance(a, DocumentAttributeVideo)]
    assert video_attrs, f"attributes 应含 DocumentAttributeVideo: {kwargs.get('attributes')}"
    va = video_attrs[0]
    assert abs(va.duration - 5) <= 1
    assert va.w == 320
    assert va.h == 240
    assert va.supports_streaming is True
