"""封面抽帧自适应 seek 测试。

- 单元: compute_thumb_seek_sec clamp 边界与非法输入回退;
- 命令捕获 (假 create_subprocess_exec): -ss 随 duration 变化、默认 5.0、官方封面优先不触 ffmpeg;
- 集成 (真实 ffmpeg + 本地 HTTP 服务): 黑场开场视频按新 seek 抽帧亮度 > 阈值、
  固定 1s 对照 < 阈值 —— 用户 "亮度 0 vs 123" 实验的自动化版 (亮度测量走 ffmpeg
  rawvideo, 不引入 Pillow)。
"""

import asyncio
import io
import subprocess

import pytest
from aiohttp import web
from PIL import Image

import app.core.pikpak.thumbnail as thumbnail_mod
from app.core.pikpak.expander import ParsedFileItem
from app.core.pikpak.thumbnail import (
    FFMPEG_THUMB_TIMEOUT_S,
    compute_thumb_seek_sec,
    extract_frame_ffmpeg,
    make_thumb_provider,
)


def _parsed_item(duration_s=None, name="movie.mp4"):
    return ParsedFileItem(
        file_id="F1",
        name=name,
        is_dir=False,
        size_bytes=123,
        audit={},
        thumbnail_link=None,
        duration_s=duration_s,
        width=None,
        height=None,
        parent_id="P1",
    )


# ---------------------------------------------------------------------------
# 单元: compute_thumb_seek_sec
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("duration_s", "expected"),
    [
        (None, 5.0),  # 未知 → 兜底
        ("", 5.0),  # 非法字符串
        ("abc", 5.0),  # 非法字符串
        (0, 5.0),  # 非正数
        ("-5", 5.0),  # 负数 (字符串)
        (10, 3.0),  # 0.1×10=1.0 → 下限 3.0
        (30, 3.0),  # 恰在下限
        ("45", 4.5),
        (60, 6.0),
        (400, 30.0),  # 0.1×400=40 → 上限 30.0
        (300.0, 30.0),
    ],
)
def test_compute_thumb_seek_sec(duration_s, expected):
    assert compute_thumb_seek_sec(duration_s) == expected


def test_thumb_timeout_constant():
    assert FFMPEG_THUMB_TIMEOUT_S == 12.0


# ---------------------------------------------------------------------------
# 命令捕获: -ss 自适应 / 默认值 / 官方封面优先
# ---------------------------------------------------------------------------


def _sample_jpeg():
    buf = io.BytesIO()
    Image.new("RGB", (24, 12), "orange").save(buf, format="JPEG")
    return buf.getvalue()


class _OkProc:
    def __init__(self, stdout=None):
        self.returncode = 0
        self._stdout = _sample_jpeg() if stdout is None else stdout

    async def communicate(self):
        return self._stdout, b""


@pytest.mark.asyncio
async def test_extract_frame_seek_default_and_explicit(monkeypatch):
    captured = []

    async def fake_exec(*args, **kwargs):
        captured.append(list(args))
        return _OkProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

    await extract_frame_ffmpeg("https://cdn.test/v.mp4", "F1")
    await extract_frame_ffmpeg("https://cdn.test/v.mp4", "F1", seek_sec=6.0)

    assert captured[0][captured[0].index("-ss") + 1] == "5.000"
    assert captured[1][captured[1].index("-ss") + 1] == "6.000"


@pytest.mark.asyncio
async def test_provider_seek_follows_duration(monkeypatch):
    item = _parsed_item(duration_s="60")  # 0.1×60 = 6.0s

    async def fake_fetch(_item, **kwargs):
        return None  # 官方封面缺失 → 走抽帧兜底

    monkeypatch.setattr(thumbnail_mod, "fetch_thumbnail", fake_fetch)
    captured = []

    async def fake_exec(*args, **kwargs):
        captured.append(list(args))
        return _OkProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

    provider = make_thumb_provider(item, url_provider=lambda: "https://cdn.test/v.mp4")
    assert await provider() == _sample_jpeg()
    assert captured[0][captured[0].index("-ss") + 1] == "6.000"


@pytest.mark.asyncio
async def test_provider_unknown_duration_uses_5s(monkeypatch):
    item = _parsed_item(duration_s=None)

    async def fake_fetch(_item, **kwargs):
        return None

    monkeypatch.setattr(thumbnail_mod, "fetch_thumbnail", fake_fetch)
    captured = []

    async def fake_exec(*args, **kwargs):
        captured.append(list(args))
        return _OkProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

    provider = make_thumb_provider(item, url_provider=lambda: "https://cdn.test/v.mp4")
    await provider()
    assert captured[0][captured[0].index("-ss") + 1] == "5.000"


@pytest.mark.asyncio
async def test_provider_official_cover_skips_ffmpeg(monkeypatch):
    item = _parsed_item(duration_s="60")

    async def fake_fetch(_item, **kwargs):
        return _sample_jpeg()

    monkeypatch.setattr(thumbnail_mod, "fetch_thumbnail", fake_fetch)

    async def boom(*args, **kwargs):
        raise AssertionError("官方封面存在时不得触 ffmpeg")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", boom)

    provider = make_thumb_provider(item, url_provider=lambda: "https://cdn.test/v.mp4")
    assert await provider() == _sample_jpeg()


# ---------------------------------------------------------------------------
# 集成: 黑场开场亮度实验 (真实 ffmpeg + 本地 HTTP)
# ---------------------------------------------------------------------------


def _measure_luma(jpeg_bytes: bytes) -> int:
    """ffmpeg 解码 JPEG → 1×1 灰度 → 单字节 Y (不引入 Pillow)。"""
    p = subprocess.run(
        [
            "ffmpeg",
            "-v",
            "error",
            "-f",
            "image2pipe",
            "-i",
            "pipe:0",
            "-vf",
            "scale=1:1",
            "-pix_fmt",
            "gray",
            "-f",
            "rawvideo",
            "pipe:1",
        ],
        input=jpeg_bytes,
        capture_output=True,
    )
    assert p.returncode == 0, p.stderr.decode("utf-8", "replace")
    assert len(p.stdout) == 1
    return p.stdout[0]


@pytest.mark.asyncio
async def test_black_fade_opening_seek_brightness(tmp_path):
    # 12s 白底视频, fade=in 持续 10s: t=1s 亮度 ~10%, t=3s (新 seek, clamp 下限) ~30%
    src = tmp_path / "fade.mp4"
    await asyncio.to_thread(
        subprocess.run,
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=c=white:size=320x180:rate=30:duration=12",
            "-vf",
            "fade=t=in:st=0:d=10",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            str(src),
        ],
        check=True,
    )

    async def handler(request):
        return web.Response(body=src.read_bytes())

    app = web.Application()
    app.router.add_get("/video.mp4", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        port = site._server.sockets[0].getsockname()[1]
        url = f"http://127.0.0.1:{port}/video.mp4"

        # 自适应 seek: 12s 时长 → clamp(1.2, 3, 30) = 3.0s
        assert compute_thumb_seek_sec(12) == 3.0

        new_thumb = await extract_frame_ffmpeg(url, "F1", seek_sec=3.0)
        assert new_thumb, "新 seek 抽帧应成功"
        old_thumb = await extract_frame_ffmpeg(url, "F1", seek_sec=1.0)
        assert old_thumb, "对照抽帧应成功"

        y_new = _measure_luma(new_thumb)
        y_old = _measure_luma(old_thumb)
        # 用户实测形态: 黑场开场在 1s 处近黑、后移后显著变亮 (0 vs 123)
        assert y_new >= 45, f"seek=3s 亮度应显著高于黑场: {y_new}"
        assert y_old <= 25, f"seek=1s 对照应仍处黑场: {y_old}"
        assert y_new > y_old
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_extract_frame_ffmpeg_kills_process_on_cancel(monkeypatch):
    """抽帧协程被取消时终止并回收 ffmpeg 子进程."""
    from unittest.mock import AsyncMock, MagicMock

    proc = AsyncMock()
    proc.returncode = None
    proc.communicate = AsyncMock(side_effect=[asyncio.CancelledError(), (b"", b"")])
    proc.kill = MagicMock()
    proc.wait = AsyncMock()

    async def fake_exec(*args, **kwargs):
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    with pytest.raises(asyncio.CancelledError):
        await extract_frame_ffmpeg("https://cdn.test/v.mp4", "F1")
    proc.kill.assert_called_once()
    assert proc.communicate.await_count == 2


@pytest.mark.asyncio
async def test_fetch_thumbnail_caps_body_and_redacts_error(caplog, monkeypatch):
    """封面下载流式限流且异常日志不打印带签名 URL."""
    import logging
    from unittest.mock import AsyncMock, MagicMock, patch

    monkeypatch.setattr(thumbnail_mod, "THUMBNAIL_MAX_BYTES", 10)

    class _FakeContent:
        async def iter_chunked(self, _n):
            yield b"123456"
            yield b"789012"

    class _FakeResp:
        def __init__(self):
            self.headers = {"Content-Type": "image/jpeg"}
            self.content = _FakeContent()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

    session = MagicMock()
    session.get = MagicMock(return_value=_FakeResp())
    base_item = _parsed_item()
    item = ParsedFileItem(**{**base_item.__dict__, "thumbnail_link": "https://cdn.test/t?sig=SECRET"})
    with patch("app.core.cdn_transport.get_cdn_session", new_callable=AsyncMock, return_value=session):
        assert await thumbnail_mod.fetch_thumbnail(item) is None

    session.get = MagicMock(side_effect=RuntimeError("https://cdn.test/t?sig=SECRET"))
    with (
        patch("app.core.cdn_transport.get_cdn_session", new_callable=AsyncMock, return_value=session),
        caplog.at_level(logging.DEBUG, logger="app.core.pikpak.thumbnail"),
    ):
        assert await thumbnail_mod.fetch_thumbnail(item) is None
    assert "SECRET" not in caplog.text
    assert "RuntimeError" in caplog.text
