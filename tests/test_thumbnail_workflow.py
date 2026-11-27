"""Offline cover selection and FFmpeg regressions."""

import asyncio
import io
import shutil
import subprocess
import time
from dataclasses import replace
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from PIL import Image, PngImagePlugin

from app.core import thumbnail_image
from app.core.pikpak import thumbnail
from app.core.pikpak.expander import ParsedFileItem


def _item(*, thumbnail_link=None, duration_s="1"):
    return ParsedFileItem(
        file_id="cover-file",
        name="clip.mp4",
        is_dir=False,
        size_bytes=123,
        audit={},
        thumbnail_link=thumbnail_link,
        duration_s=duration_s,
        width=None,
        height=None,
        parent_id=None,
    )


def _image_bytes(color, image_format="JPEG"):
    buf = io.BytesIO()
    Image.new("RGB", (640, 360), color).save(buf, format=image_format)
    return buf.getvalue()


def _assert_jpeg(data):
    assert data and len(data) < 200_000
    with Image.open(io.BytesIO(data)) as image:
        assert image.format == "JPEG"
        image.load()
        assert max(image.size) <= 320
        return image.copy()


def test_cover_rejects_excess_pixels_before_decoding(monkeypatch):
    buf = io.BytesIO()
    Image.new("1", (4096, 2049)).save(buf, format="PNG")
    decode = Mock(side_effect=AssertionError("oversized cover decoded"))
    monkeypatch.setattr(PngImagePlugin.PngImageFile, "load", decode)

    assert thumbnail_image.normalize_thumbnail_bytes(buf.getvalue()) is None
    decode.assert_not_called()


def test_cover_checks_multidimensional_view_byte_count(cover_jpeg):
    view = memoryview(cover_jpeg).cast("B", shape=(1, len(cover_jpeg)))

    assert thumbnail_image.normalize_thumbnail_bytes(view, max_input_bytes=view.nbytes - 1) is None
    assert thumbnail_image.normalize_thumbnail_bytes(view, max_input_bytes=view.nbytes) == cover_jpeg


def test_cover_rgba_resizes_before_compositing(monkeypatch):
    image = Image.new("RGBA", (640, 360))
    image.paste((255, 0, 0, 128), (0, 0, 320, 360))
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    to_rgb = Mock(wraps=thumbnail_image._to_rgb)
    monkeypatch.setattr(thumbnail_image, "_to_rgb", to_rgb)

    result = _assert_jpeg(thumbnail_image.normalize_thumbnail_bytes(buf.getvalue()))

    assert to_rgb.call_args.args[0].size == (320, 180)
    red, green, blue = result.getpixel((80, 90))
    assert 115 <= red <= 140 and green < 10 and blue < 10
    assert max(result.getpixel((240, 90))) < 10


@pytest.mark.parametrize(
    ("mode", "transparent", "opaque", "expected"),
    [
        ("RGB", (255, 0, 0), (0, 255, 0), (0, 255, 0)),
        ("L", 200, 100, (100, 100, 100)),
        ("I;16", 1000, 2000, (255, 255, 255)),
        ("P", 1, 2, (0, 255, 0)),
    ],
)
def test_cover_resamples_png_with_color_key_transparency(monkeypatch, mode, transparent, opaque, expected):
    image = Image.new(mode, (640, 360), opaque)
    if mode == "P":
        image.putpalette([0, 0, 0, 255, 0, 0, 0, 255, 0])
    image.paste(Image.new(mode, (320, 360), transparent), (0, 0))
    buf = io.BytesIO()
    image.save(buf, format="PNG", transparency=transparent)
    encode = Mock(wraps=thumbnail_image._encode_jpeg_under_limit)
    monkeypatch.setattr(thumbnail_image, "_encode_jpeg_under_limit", encode)

    result = _assert_jpeg(thumbnail_image.normalize_thumbnail_bytes(buf.getvalue()))

    assert result.size == (320, 180)
    assert max(result.getpixel((80, 90))) < 10
    assert all(abs(a - b) <= 10 for a, b in zip(result.getpixel((240, 90)), expected, strict=True))
    # Inspect the resampled border before JPEG chroma subsampling adds its own color bleed.
    if mode in ("RGB", "P"):
        assert encode.call_args.args[0].crop((152, 80, 168, 100)).getchannel("R").getextrema() == (0, 0)


def test_cover_normalizes_16_bit_grayscale_png():
    buf = io.BytesIO()
    Image.new("I;16", (640, 360), 110).save(buf, format="PNG")

    result = _assert_jpeg(thumbnail_image.normalize_thumbnail_bytes(buf.getvalue()))

    assert result.size == (320, 180)
    assert all(100 <= channel <= 120 for channel in result.getpixel((160, 90)))


def test_cover_resizes_before_exif_orientation(monkeypatch):
    image = Image.new("RGB", (640, 360), "blue")
    image.paste("red", (0, 0, 320, 360))
    image.getexif()[274] = 6  # 90 degrees clockwise
    buf = io.BytesIO()
    image.save(buf, format="JPEG", exif=image.getexif())
    transpose = Mock(wraps=thumbnail_image.ImageOps.exif_transpose)
    monkeypatch.setattr(thumbnail_image.ImageOps, "exif_transpose", transpose)

    result = _assert_jpeg(thumbnail_image.normalize_thumbnail_bytes(buf.getvalue()))

    assert transpose.call_args.args[0].size == (320, 180)
    assert result.size == (180, 320)
    assert result.getpixel((90, 80))[0] > 190
    assert result.getpixel((90, 240))[2] > 190
    assert result.getexif().get(274, 1) == 1


async def test_local_cover_file_check_timeout_uses_poster_without_ffmpeg(monkeypatch):
    async def stalled_check(*_args):
        await asyncio.Future()

    extract = AsyncMock()
    monkeypatch.setattr(thumbnail.asyncio, "to_thread", stalled_check)
    monkeypatch.setattr(thumbnail, "_extract_ffmpeg_with_fallback", extract)

    async with asyncio.timeout(2):
        result = await thumbnail.get_local_thumbnail("clip.mp4", deadline_s=0.1)

    assert result and result == thumbnail.load_normalized_placeholder_poster()
    extract.assert_not_called()


async def test_local_cover_file_check_propagates_cancellation(monkeypatch):
    check_started = asyncio.Event()

    async def stalled_check(*_args):
        check_started.set()
        await asyncio.Future()

    extract = AsyncMock()
    monkeypatch.setattr(thumbnail.asyncio, "to_thread", stalled_check)
    monkeypatch.setattr(thumbnail, "_extract_ffmpeg_with_fallback", extract)

    async with asyncio.timeout(2):
        task = asyncio.create_task(thumbnail.get_local_thumbnail("clip.mp4"))
        await check_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    extract.assert_not_called()


@pytest.mark.parametrize("listed", ["missing", "expired", "slow"])
async def test_official_cover_fallback_before_ffmpeg(monkeypatch, listed):
    release = asyncio.Event()
    requests = []

    async def handle(request):
        requests.append(request.path)
        if request.path == "/listed":
            if listed == "slow":
                await release.wait()
            return web.Response(status=403)
        return web.Response(body=_image_bytes("green", "PNG"), content_type="image/png")

    app = web.Application()
    app.router.add_get("/{name}", handle)
    monkeypatch.setattr(thumbnail, "LIST_IMAGE_STAGE_BUDGET_S", 0.25)
    async with TestServer(app) as server, aiohttp.ClientSession() as session:
        monkeypatch.setattr("app.core.cdn_transport.get_cdn_session", AsyncMock(return_value=session))
        lookup = AsyncMock(return_value=str(server.make_url("/refreshed")))
        data_url = AsyncMock()
        link = str(server.make_url("/listed")) if listed != "missing" else None
        provider = thumbnail.make_thumb_provider(
            _item(thumbnail_link=link), url_provider=data_url, thumbnail_link_provider=lookup
        )
        try:
            data = await provider()
            assert await provider() == data
            assert _assert_jpeg(data).size == (320, 180)
            lookup.assert_awaited_once_with("cover-file")
            data_url.assert_not_called()
            assert requests == (["/listed", "/refreshed"] if link else ["/refreshed"])
        finally:
            release.set()


async def test_valid_bytes_short_circuit_and_missing_video_uses_poster():
    lookup = AsyncMock(return_value=None)
    supplied = _image_bytes("orange")
    provider = thumbnail.make_thumb_provider(_item(), thumbnail_link_provider=lookup, provided_bytes=supplied)
    assert _assert_jpeg(await provider()).getpixel((100, 75))[0] > 200
    lookup.assert_not_called()

    provider = thumbnail.make_thumb_provider(_item(), thumbnail_link_provider=lookup, provided_bytes=b"broken")
    assert await provider() == thumbnail.load_normalized_placeholder_poster()
    lookup.assert_awaited_once_with("cover-file")

    lookup.reset_mock()
    provider = thumbnail.make_thumb_provider(
        replace(_item(duration_s=None), name="notes.txt"), thumbnail_link_provider=lookup
    )
    assert await provider() is None
    lookup.assert_not_called()


async def test_slow_cli_cleanup_does_not_block_ffmpeg_or_release_its_slot(monkeypatch, cover_jpeg):
    cleaning = asyncio.Event()
    release = asyncio.Event()
    semaphore = asyncio.Semaphore(2)

    async def stalled_lookup(_file_id):
        try:
            await asyncio.Future()
        finally:
            cleaning.set()
            await release.wait()

    async def extract(*_args, **_kwargs):
        await cleaning.wait()
        # One of the two slots remains held by the cancelled CLI's cleanup.
        async with semaphore:
            assert semaphore.locked()
        return cover_jpeg

    monkeypatch.setattr(thumbnail, "LOOKUP_STAGE_BUDGET_S", 0.1)
    monkeypatch.setattr(thumbnail, "_get_remote_semaphore", lambda: semaphore)
    monkeypatch.setattr(thumbnail, "extract_frame_ffmpeg", extract)
    provider = thumbnail.make_thumb_provider(
        _item(), url_provider=lambda: "https://cdn.example.test/clip.mp4", thumbnail_link_provider=stalled_lookup
    )
    try:
        assert await asyncio.wait_for(provider(), timeout=2) == cover_jpeg
    finally:
        release.set()
        await asyncio.sleep(0)
    async with asyncio.timeout(2), semaphore, semaphore:
        assert semaphore.locked()


def _ffmpeg(*args):
    return subprocess.run(
        ["ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin", *args], capture_output=True, timeout=20
    )


@pytest.mark.parametrize(("duration_s", "alternate"), [(20, 7.0), (None, 0.5), (1.5, 0.375)])
async def test_failed_seek_then_black_start_tries_unused_alternate(monkeypatch, cover_jpeg, duration_s, alternate):
    frame = AsyncMock(side_effect=[None, _image_bytes("black"), cover_jpeg])
    monkeypatch.setattr(thumbnail, "_run_ffmpeg_once", frame)
    primary = thumbnail.compute_thumb_seek_sec(duration_s)
    deadline = time.monotonic() + 12

    result = await thumbnail._extract_ffmpeg_with_fallback(
        "clip.mp4", "clip", seek_sec=primary, duration_s=duration_s, deadline=deadline
    )

    assert result == cover_jpeg
    assert [call.args[2] for call in frame.await_args_list] == [primary, 0.0, alternate]
    assert all(call.kwargs["deadline"] == deadline for call in frame.await_args_list)


async def test_failed_seek_then_black_start_skips_alternate_without_budget(monkeypatch):
    frame = AsyncMock(side_effect=[None, _image_bytes("black")])
    monkeypatch.setattr(thumbnail, "_run_ffmpeg_once", frame)
    monkeypatch.setattr(thumbnail, "_remaining_time", lambda _deadline: 1.0 if frame.await_count < 2 else 0.4)

    result = await thumbnail._extract_ffmpeg_with_fallback(
        "clip.mp4", "clip", seek_sec=5.0, duration_s=None, deadline=time.monotonic() + 12
    )

    assert result is None
    assert [call.args[2] for call in frame.await_args_list] == [5.0, 0.0]


async def test_unknown_short_clip_with_black_opening_gets_cover(tmp_path):
    if shutil.which("ffmpeg") is None:
        pytest.skip("FFmpeg is not installed")
    path = tmp_path / "short-black-intro.mp4"
    result = _ffmpeg(
        "-f",
        "lavfi",
        "-i",
        "color=black:size=320x180:rate=25:duration=0.8,drawbox=0:0:320:180:orange:t=fill:enable='gte(t,0.2)'",
        "-c:v",
        "libx264",
        "-threads",
        "1",
        str(path),
    )
    assert result.returncode == 0, result.stderr

    image = _assert_jpeg(await thumbnail.get_local_thumbnail(path))
    assert image.getpixel((160, 90))[0] > 200


@pytest.fixture(scope="module")
def portrait_clips(tmp_path_factory):
    if shutil.which("ffmpeg") is None:
        pytest.skip("FFmpeg is not installed")
    temp = tmp_path_factory.mktemp("cover-portrait")
    native = temp / "native.mp4"
    landscape = temp / "landscape.mp4"
    rotated = temp / "rotated.mp4"
    for path, spec in [
        (native, "color=blue:size=360x640:rate=25:duration=0.8,drawbox=0:320:360:320:red:t=fill"),
        (landscape, "color=blue:size=640x360:rate=25:duration=0.8,drawbox=0:0:320:360:red:t=fill"),
    ]:
        result = _ffmpeg("-f", "lavfi", "-i", spec, "-c:v", "libx264", "-threads", "1", str(path))
        assert result.returncode == 0, result.stderr
    result = _ffmpeg("-display_rotation", "90", "-i", str(landscape), "-c", "copy", str(rotated))
    if result.returncode:
        result = _ffmpeg("-y", "-i", str(landscape), "-c", "copy", "-metadata:s:v:0", "rotate=90", str(rotated))
    assert result.returncode == 0, result.stderr
    return {"native": native, "rotated": rotated}


@pytest.mark.parametrize("variant", ["native", "rotated"])
@pytest.mark.parametrize("remote", [False, True])
async def test_short_portrait_cover_orientation_and_tail_moov(monkeypatch, portrait_clips, variant, remote):
    path = portrait_clips[variant]
    if remote:
        payload = path.read_bytes()
        assert 0 < payload.find(b"mdat") < payload.find(b"moov")
        ranges = []
        cover_requested = asyncio.Event()
        release = asyncio.Event()

        async def slow_cover(_request):
            cover_requested.set()
            await release.wait()
            return web.Response()

        async def handle(request):
            ranges.append(request.headers.get("Range"))
            return web.FileResponse(path)

        app = web.Application()
        app.router.add_get("/clip.mp4", handle)
        app.router.add_get("/slow-cover", slow_cover)
        monkeypatch.setattr(thumbnail, "LIST_IMAGE_STAGE_BUDGET_S", 0.25)
        async with TestServer(app) as server, aiohttp.ClientSession() as session:
            monkeypatch.setattr("app.core.cdn_transport.get_cdn_session", AsyncMock(return_value=session))
            lookup = AsyncMock(return_value=None)
            provider = thumbnail.make_thumb_provider(
                _item(thumbnail_link=str(server.make_url("/slow-cover")), duration_s=None),
                url_provider=lambda: str(server.make_url("/clip.mp4")),
                thumbnail_link_provider=lookup,
            )
            try:
                data = await provider()
                assert cover_requested.is_set()
                lookup.assert_awaited_once_with("cover-file")
            finally:
                release.set()
        assert any(value and value.startswith("bytes=") for value in ranges)
    else:
        data = await thumbnail.get_local_thumbnail(path, duration_s=0.8)

    image = _assert_jpeg(data)
    assert image.size == (180, 320)
    r, _, b = image.getpixel((90, 80))
    assert b > 190 and r < 40
    r, _, b = image.getpixel((90, 240))
    assert r > 190 and b < 40


async def test_black_intro_retries_within_remaining_budget(tmp_path):
    if shutil.which("ffmpeg") is None:
        pytest.skip("FFmpeg is not installed")
    path = tmp_path / "black-intro.mp4"
    result = _ffmpeg(
        "-f",
        "lavfi",
        "-i",
        "color=black:size=320x180:rate=10:duration=9,drawbox=0:0:320:180:orange:t=fill:enable='gte(t,5)'",
        "-c:v",
        "libx264",
        "-threads",
        "1",
        str(path),
    )
    assert result.returncode == 0, result.stderr
    image = _assert_jpeg(await thumbnail.get_local_thumbnail(path, duration_s=9))
    assert image.getpixel((160, 80))[0] > 200


async def test_ffmpeg_queue_expiry_leaves_slots_and_skips_spawn(monkeypatch):
    semaphore = asyncio.Semaphore(0)
    monkeypatch.setattr(thumbnail, "_get_ffmpeg_semaphore", lambda: semaphore)
    spawn = AsyncMock()
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    provider = thumbnail.make_thumb_provider(
        _item(), url_provider=lambda: "https://cdn.example.test/clip.mp4", deadline_s=0.1
    )
    assert await provider() == thumbnail.load_normalized_placeholder_poster()
    spawn.assert_not_called()
    assert semaphore.locked()


@pytest.mark.parametrize("cancel", [False, True])
async def test_ffmpeg_stalled_http_process_reaped(monkeypatch, cancel):
    if shutil.which("ffmpeg") is None:
        pytest.skip("FFmpeg is not installed")
    spawned = []
    requested = asyncio.Event()
    release = asyncio.Event()
    create_proc = asyncio.create_subprocess_exec

    async def track_proc(*args, **kwargs):
        proc = await create_proc(*args, **kwargs)
        spawned.append(proc)
        return proc

    async def stall(_request):
        requested.set()
        await release.wait()
        return web.Response()

    app = web.Application()
    app.router.add_get("/clip.mp4", stall)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", track_proc)
    monkeypatch.setattr(thumbnail, "FFMPEG_THUMB_TIMEOUT_S", 1.5)
    async with TestServer(app) as server:
        task = asyncio.create_task(
            thumbnail.extract_frame_ffmpeg(str(server.make_url("/clip.mp4")), "stalled", seek_sec=0)
        )
        try:
            await asyncio.wait_for(requested.wait(), timeout=4)
            if cancel:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                assert await task is None
            assert len(spawned) == 1 and spawned[0].returncode is not None
        finally:
            release.set()
