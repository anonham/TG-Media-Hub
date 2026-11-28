"""Phase 1 media admission, display rotation, metadata enrichment, and remote TS verification tests."""

from __future__ import annotations

import asyncio
import gc
import json
import logging
import mimetypes
import struct
import subprocess
import weakref
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import ClientConnectionError, web
from telethon.extensions import BinaryReader
from telethon.tl.types import DocumentAttributeVideo

from app.api.storage import _resolve_media_file, storage_preview
from app.core import media_metadata as media_meta
from app.core.iso_bmff.box_parser import display_dimensions, parse_moov, rotation_degrees_from_matrix
from app.core.keyframe_prober import Mp4StssProber, TailMoovScanner, _parse_mkv_tracks_element, _parse_mkv_video_element
from app.core.media_metadata import (
    BudgetedRangeReader,
    ProbedMediaMetadata,
    _parse_ffprobe_json,
    apply_probed_metadata,
    probe_bytes_ffprobe,
    probe_local_ffprobe,
    probe_local_video_metadata,
    probe_remote_video_metadata,
    verify_remote_mpeg_ts_head,
)
from app.core.media_sniff import is_supported_local_media
from app.core.pipeline.engines import LocalDiskTransportEngine, PikPakStreamTransportEngine
from app.core.pipeline.engines._common import _backfill_media_metadata_to_item
from app.core.pipeline.engines.pikpak import engine as pik_engine
from app.core.pipeline.engines.pikpak import fmp4 as pik_fmp4
from app.core.pipeline.models import (
    OVERSIZE_POLICY_SKIP_AND_REPORT,
    PublishContext,
    PublishItem,
    TransportOutcome,
)
from app.core.pipeline.pipeline import PublishPipeline
from app.core.pipeline.sources import LocalFolderSource, PikPakSnapshotSource
from app.core.pipeline.utils import build_media_attributes
from app.core.scheduler import UploadCancelledError, UploadPausedError
from app.services.publisher_service import PublisherService
from app.services.storage_service import StorageService


def _run_ffmpeg(args: list[str]) -> None:
    subprocess.run(
        ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error", *args],
        check=True,
    )


def _gen_mp4_family(
    path: Path,
    *,
    w: int = 320,
    h: int = 240,
    dur: float = 2.0,
    display_rotation: int | None = None,
    sample_aspect_ratio: str | None = None,
) -> Path:
    target = path if display_rotation is None else path.with_name(f"_base_{path.name}")
    video_source = f"testsrc2=size={w}x{h}:rate=15:duration={dur}"
    if sample_aspect_ratio is not None:
        video_source += f",setsar={sample_aspect_ratio}"
    cmd = [
        "-f",
        "lavfi",
        "-i",
        video_source,
        "-f",
        "lavfi",
        "-i",
        f"sine=frequency=440:sample_rate=44100:duration={dur}",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-movflags",
        "+faststart",
        str(target),
    ]
    _run_ffmpeg(cmd)
    if display_rotation is not None:
        try:
            _run_ffmpeg(
                [
                    "-display_rotation",
                    str(display_rotation),
                    "-i",
                    str(target),
                    "-c",
                    "copy",
                    "-movflags",
                    "+faststart",
                    str(path),
                ]
            )
        except subprocess.CalledProcessError:
            # Older FFmpeg builds (< 5/6) use output stream rotate metadata instead of -display_rotation
            _run_ffmpeg(
                [
                    "-i",
                    str(target),
                    "-c",
                    "copy",
                    "-metadata:s:v:0",
                    f"rotate={display_rotation}",
                    "-movflags",
                    "+faststart",
                    str(path),
                ]
            )
        target.unlink(missing_ok=True)
    return path


def _make_synthetic_ts(packet_size: int = 188, num_packets: int = 16) -> bytes:
    buf = bytearray()
    for i in range(num_packets):
        pkt = bytearray(packet_size)
        pkt[0] = 0x47
        pkt[1] = 0x00
        pkt[2] = 0x11
        pkt[3] = 0x10 + (i % 16)
        buf.extend(pkt)
    return bytes(buf)


class _RangeHttpServer:
    """Controllable local loopback HTTP server for Range / TS / budget / concurrency tests."""

    def __init__(self, handler: Any) -> None:
        self._handler = handler
        self._runner: web.AppRunner | None = None
        self.base_url: str = ""

    async def __aenter__(self) -> _RangeHttpServer:
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self._handler)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        sockets = site._server.sockets  # type: ignore[union-attr]
        port = sockets[0].getsockname()[1]
        self.base_url = f"http://127.0.0.1:{port}"
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._runner is not None:
            await self._runner.cleanup()


def _make_byte_range_handler(
    payload: bytes,
    *,
    recorded_ranges: list[tuple[int, int]] | None = None,
    sent_bytes: list[int] | None = None,
    delay_s: float = 0.0,
    active_counter: dict[str, int] | None = None,
) -> Any:
    total = len(payload)

    async def _handle(request: web.Request) -> web.StreamResponse:
        if active_counter is not None:
            active_counter["cur"] += 1
            active_counter["max"] = max(active_counter["max"], active_counter["cur"])
        try:
            if delay_s > 0:
                await asyncio.sleep(delay_s)
            rng = request.headers.get("Range", "")
            if rng.startswith("bytes="):
                start_s, end_s = rng[6:].split("-", 1)
                start = int(start_s) if start_s else 0
                end = int(end_s) if end_s else total - 1
                end = min(end, total - 1)
                if recorded_ranges is not None:
                    recorded_ranges.append((start, end))
                chunk = payload[start : end + 1]
                if sent_bytes is not None:
                    sent_bytes.append(len(chunk))
                return web.Response(
                    status=206,
                    body=chunk,
                    headers={
                        "Content-Range": f"bytes {start}-{end}/{total}",
                        "Accept-Ranges": "bytes",
                        "Content-Type": "application/octet-stream",
                    },
                )
            if sent_bytes is not None:
                sent_bytes.append(total)
            return web.Response(status=200, body=payload)
        finally:
            if active_counter is not None:
                active_counter["cur"] -= 1

    return _handle


# ---------------------------------------------------------------------------
# 1. M4V admission, MIME, storage category & preview HTTP 206 Range
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_m4v_case_insensitive_admission_mime_storage_and_preview_range(tmp_path, monkeypatch):
    monkeypatch.setattr("app.config.settings.DOWNLOAD_DIR", str(tmp_path))
    m4v_lower = _gen_mp4_family(tmp_path / "movie.m4v", dur=1.5)
    m4v_upper = tmp_path / "UPPER_CLIP.M4V"
    m4v_upper.write_bytes(m4v_lower.read_bytes())

    assert is_supported_local_media(str(m4v_lower)) == (True, None)
    assert is_supported_local_media(str(m4v_upper)) == (True, None)
    assert mimetypes.guess_type(str(m4v_lower))[0] == "video/mp4"
    assert mimetypes.guess_type(str(m4v_upper))[0] == "video/mp4"
    assert StorageService.categorize_file("movie.m4v") == "video"
    assert StorageService.categorize_file("UPPER_CLIP.M4V") == "video"

    report = PublisherService.inspect_folder(str(tmp_path))
    assert report["videos"]["count"] == 2
    assert report["ignored_files"] == []

    batches = await LocalFolderSource(str(tmp_path)).get_batches(
        PublishContext(task_id="T-M4V", target_chat_id=-1001, folder_path=str(tmp_path))
    )
    assert len(batches) == 1
    assert {it.name for it in batches[0].items} == {"movie.m4v", "UPPER_CLIP.M4V"}
    assert all(it.mime_type == "video/mp4" for it in batches[0].items)

    safe_path, content_type = _resolve_media_file("UPPER_CLIP.M4V")
    assert safe_path == m4v_upper.resolve()
    assert content_type == "video/mp4"

    resp = await storage_preview("UPPER_CLIP.M4V", range="bytes=0-15")
    assert resp.status_code == 206
    assert resp.headers["Content-Type"] == "video/mp4"
    assert resp.headers["Content-Range"].startswith("bytes 0-15/")
    body = b"".join([chunk async for chunk in resp.body_iterator])
    assert len(body) == 16


@pytest.mark.asyncio
async def test_m4v_pikpak_single_publish_without_duration_sets_video_attrs_and_mime():
    item = PublishItem(
        item_id="fid-m4v",
        name="Clip.M4V",
        size_bytes=1024,
        source_type="pikpak",
        duration_s=None,
    )
    mock_client = AsyncMock()
    sent = MagicMock(id=777)
    mock_client.send_file.return_value = sent
    ctx = PublishContext(task_id="T-PP-M4V", target_chat_id=-1002, userbot_client=mock_client, source_type="pikpak")
    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value=["http://127.0.0.1:1/nope"]))

    with (
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()),
        patch(
            "app.core.pipeline.engines.pikpak.single.probe_remote_video_metadata",
            new_callable=AsyncMock,
            return_value=ProbedMediaMetadata(),
        ),
    ):
        outcome = await engine.send_item(item, ctx)

    assert outcome.message_ids == [777]
    kwargs = mock_client.send_file.call_args.kwargs
    assert kwargs["mime_type"] == "video/mp4"
    assert kwargs["supports_streaming"] is True
    v_attrs = [a for a in kwargs["attributes"] if isinstance(a, DocumentAttributeVideo)]
    assert len(v_attrs) == 1


# ---------------------------------------------------------------------------
# 2. Native portrait vs 90/270 rotation metadata (send + edit + muxer safety)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("ext", [".mp4", ".mov", ".m4v"])
@pytest.mark.parametrize("rotation", [90, 270])
async def test_rotated_mp4_mov_m4v_swaps_display_dims_for_send_and_edit(tmp_path, ext, rotation):
    video = _gen_mp4_family(tmp_path / f"rot{rotation}{ext}", w=320, h=240, dur=2.0, display_rotation=rotation)
    item = PublishItem(item_id=str(video), name=video.name, size_bytes=video.stat().st_size, source_type="local")

    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=101)
    mock_client.edit_message.return_value = MagicMock(id=101)
    ctx = PublishContext(task_id="T-ROT", target_chat_id=-1003, userbot_client=mock_client)
    engine = LocalDiskTransportEngine()

    with patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()):
        await engine.send_item(item, ctx)
        await engine.edit_item(item, ctx, msg_id=101)

    send_va = next(
        a for a in mock_client.send_file.call_args.kwargs["attributes"] if isinstance(a, DocumentAttributeVideo)
    )
    edit_va = next(
        a for a in mock_client.edit_message.call_args.kwargs["attributes"] if isinstance(a, DocumentAttributeVideo)
    )
    assert (send_va.w, send_va.h) == (240, 320)
    assert (edit_va.w, edit_va.h) == (240, 320)
    assert item.video_codec in {"avc1", "h264"}
    assert item.audio_codec in {"mp4a", "aac"}
    if ext == ".m4v":
        assert mock_client.send_file.call_args.kwargs["mime_type"] == "video/mp4"

    # Ensure ISO-BMFF muxer metadata remains encoded dimensions (320x240) and is NOT mutated
    raw = video.read_bytes()
    moov_pos = raw.find(b"moov") - 4
    moov_size = int.from_bytes(raw[moov_pos : moov_pos + 4], "big")
    meta = parse_moov(raw[moov_pos : moov_pos + moov_size])
    assert meta.video_track is not None
    assert int(meta.video_track.tkhd.width) == 320
    assert int(meta.video_track.tkhd.height) == 240
    assert rotation_degrees_from_matrix(meta.video_track.tkhd.matrix) in (90, 270)
    assert display_dimensions(320, 240, rotation_degrees_from_matrix(meta.video_track.tkhd.matrix)) == (240, 320)

    # _backfill_media_metadata_to_item handles partial dims coherently & preserves complete remote dims
    partial_item = PublishItem(item_id="p1", name="rot.mp4", size_bytes=100, source_type="pikpak", width=-1, height=240)
    _backfill_media_metadata_to_item(partial_item, meta)
    assert (partial_item.width, partial_item.height) == (240, 320)
    assert int(meta.video_track.tkhd.width) == 320  # unchanged

    complete_item = PublishItem(
        item_id="p2", name="rot.mp4", size_bytes=100, source_type="pikpak", width=1080, height=1920
    )
    _backfill_media_metadata_to_item(complete_item, meta)
    assert (complete_item.width, complete_item.height) == (1080, 1920)


def test_mp4_full_and_lightweight_probes_agree_on_tkhd_presentation_geometry(tmp_path):
    raw = _gen_mp4_family(tmp_path / "scaled.mp4", display_rotation=90).read_bytes()
    moov_start = raw.find(b"moov") - 4
    moov_size = int.from_bytes(raw[moov_start : moov_start + 4], "big")
    moov = bytearray(raw[moov_start : moov_start + moov_size])
    tkhd_start = moov.find(b"tkhd") - 4
    tkhd_size = int.from_bytes(moov[tkhd_start : tkhd_start + 4], "big")
    struct.pack_into(">I", moov, tkhd_start + tkhd_size - 8, 400 << 16)

    light = Mp4StssProber._parse_moov_keyframes(bytes(moov))
    full = parse_moov(bytes(moov))
    item = PublishItem(item_id="scaled", name="scaled.mp4", size_bytes=len(raw), width=-1, height=240)
    _backfill_media_metadata_to_item(item, full)

    assert (light.width, light.height) == (240, 400)
    assert (item.width, item.height) == (240, 400)
    entry = full.video_track.stbl.stsd.entries[0]
    assert (entry.width, entry.height) == (320, 240)


def test_fmp4_send_geometry_uses_a_valid_pair():
    item = PublishItem(item_id="fid", name="movie.mp4", size_bytes=100, width=720, height=-1)
    uploader = SimpleNamespace(video_width=854, video_height=480)
    plan = SimpleNamespace(part_index=1, total_parts=2, duration_sec=2.0)
    attrs = pik_fmp4.build_fmp4_send_kwargs(item, uploader, plan, 0.0, None)["attributes"]
    assert (attrs[0].w, attrs[0].h) == (854, 480)


@pytest.mark.asyncio
async def test_native_portrait_is_not_swapped_and_subsecond_duration_preserved(tmp_path):
    portrait = _gen_mp4_family(tmp_path / "native_portrait.mp4", w=240, h=320, dur=0.5)
    item = PublishItem(
        item_id=str(portrait), name=portrait.name, size_bytes=portrait.stat().st_size, source_type="local"
    )

    mock_client = AsyncMock()
    mock_client.send_file.return_value = MagicMock(id=202)
    ctx = PublishContext(task_id="T-PORT", target_chat_id=-1004, userbot_client=mock_client)

    with patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()):
        await LocalDiskTransportEngine().send_item(item, ctx)

    va = next(a for a in mock_client.send_file.call_args.kwargs["attributes"] if isinstance(a, DocumentAttributeVideo))
    assert (va.w, va.h) == (240, 320)
    assert va.duration > 0

    # Also verify build_media_attributes directly for subsecond float
    sub_attr = build_media_attributes(PublishItem(item_id="x.mp4", name="x.mp4", size_bytes=10, duration_s=0.4))[0]
    assert sub_attr.duration == pytest.approx(0.4, abs=0.01)
    decoded = BinaryReader(bytes(sub_attr)).tgread_object()
    assert decoded.duration == pytest.approx(0.4, abs=0.01)


# ---------------------------------------------------------------------------
# 3. Real MKV / WebM / AVI / WMV / FLV / TS metadata & attached-pic filtering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("ext", "vcodec", "acodec"),
    [
        (".mkv", "libx264", "aac"),
        (".webm", "libvpx", "libopus"),
        (".avi", "mpeg4", "mp3"),
        (".flv", "flv", "mp3"),
        (".wmv", "wmv2", "wmav2"),
        (".ts", "libx264", "aac"),
    ],
)
async def test_real_non_mp4_containers_populate_duration_and_dims(tmp_path, ext, vcodec, acodec):
    out = tmp_path / f"sample{ext}"
    _run_ffmpeg(
        [
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=320x240:rate=15:duration=2",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=44100:duration=2",
            "-c:v",
            vcodec,
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            acodec,
            "-shortest",
            str(out),
        ]
    )
    item = PublishItem(item_id=str(out), name=out.name, size_bytes=out.stat().st_size, source_type="local")
    await LocalDiskTransportEngine()._ensure_local_video_metadata(item)

    assert item.duration_s is not None and 1.0 <= float(item.duration_s) <= 3.5
    assert item.width == 320
    assert item.height == 240
    audio_codec = acodec.removeprefix("lib")
    assert item.audio_codec in {audio_codec, f"a_{audio_codec}"}


@pytest.mark.asyncio
async def test_ffprobe_sees_audio_and_later_default_video_track(tmp_path):
    path = tmp_path / "multiple.mkv"
    _run_ffmpeg(
        [
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=160x120:rate=10:duration=0.5",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=320x240:rate=10:duration=0.5",
            "-f",
            "lavfi",
            "-i",
            "sine=duration=0.5",
            "-map",
            "0:v",
            "-map",
            "1:v",
            "-map",
            "2:a",
            "-c:v",
            "mpeg4",
            "-c:a",
            "aac",
            "-disposition:v:0",
            "0",
            "-disposition:v:1",
            "default",
            str(path),
        ]
    )
    meta = await probe_local_ffprobe(str(path))
    assert (meta.width, meta.height, meta.video_codec, meta.audio_codec) == (320, 240, "mpeg4", "aac")


@pytest.mark.asyncio
async def test_local_metadata_cache_retries_empty_probe_and_retains_codecs(tmp_path):
    path = tmp_path / "retry.avi"
    path.write_bytes(b"sample")
    item = PublishItem(item_id=str(path), name=path.name, size_bytes=6, video_codec="existing")
    fresh_item = PublishItem(item_id=str(path), name=path.name, size_bytes=6)
    engine = LocalDiskTransportEngine()
    meta = ProbedMediaMetadata(duration_s=1.2, width=320, height=240, video_codec="mpeg4", audio_codec="mp3")
    with patch(
        "app.core.pipeline.engines.local_disk.probe_local_video_metadata",
        new_callable=AsyncMock,
        side_effect=[ProbedMediaMetadata(), meta],
    ) as probe:
        await engine._ensure_local_video_metadata(item)
        assert item.duration_s is None
        await engine._ensure_local_video_metadata(item)
        await engine._ensure_local_video_metadata(fresh_item)

    assert probe.await_count == 2
    assert (item.duration_s, item.width, item.height) == (1.2, 320, 240)
    assert (item.video_codec, item.audio_codec) == ("existing", "mp3")
    assert (fresh_item.video_codec, fresh_item.audio_codec) == ("mpeg4", "mp3")


@pytest.mark.asyncio
async def test_local_metadata_cache_retries_and_merges_partial_probes(tmp_path):
    path = tmp_path / "partial.mkv"
    path.write_bytes(b"test")
    engine = LocalDiskTransportEngine()
    results = [ProbedMediaMetadata(width=720, height=576, video_codec="h264"), ProbedMediaMetadata(duration_s=12)]
    items = [PublishItem(item_id=str(path), name=path.name, size_bytes=4) for _ in range(3)]
    with patch(
        "app.core.pipeline.engines.local_disk.probe_local_video_metadata", new=AsyncMock(side_effect=results)
    ) as probe:
        for item in items:
            await engine._ensure_local_video_metadata(item)

    assert probe.await_count == 2
    assert [(it.width, it.height, it.video_codec) for it in items] == [(720, 576, "h264")] * 3
    assert [it.duration_s for it in items] == [None, 12, 12]


def _ebml_element(element_id: int, payload: bytes | int) -> bytes:
    if isinstance(payload, int):
        payload = payload.to_bytes(max(1, (payload.bit_length() + 7) // 8), "big")
    assert len(payload) < 127  # single-byte size VINT for these small fixtures
    return element_id.to_bytes((element_id.bit_length() + 7) // 8, "big") + bytes([0x80 | len(payload)]) + payload


def _mkv_track(width: int, height: int, codec: bytes, *, default: bool = True, enabled: bool = True) -> bytes:
    video = _ebml_element(0xB0, width) + _ebml_element(0xBA, height)
    return _ebml_element(
        0xAE,
        _ebml_element(0x83, 1)
        + _ebml_element(0x88, int(default))
        + _ebml_element(0xB9, int(enabled))
        + _ebml_element(0x86, codec)
        + _ebml_element(0xE0, video),
    )


def test_mkv_display_dimensions_apply_cropping_and_per_axis_defaults():
    video = b"".join(
        _ebml_element(key, val)
        for key, val in [(0xB0, 1920), (0xBA, 1080), (0x54CC, 240), (0x54DD, 240), (0x54BB, 40), (0x54AA, 40)]
    )
    assert _parse_mkv_video_element(video) == (1440, 1000)
    assert _parse_mkv_video_element(video + _ebml_element(0x54B0, 1600)) == (1600, 1000)
    assert _parse_mkv_video_element(video + _ebml_element(0x54BA, 900)) == (1440, 900)
    assert _parse_mkv_video_element(video + _ebml_element(0x54B2, 3)) == (1920, 1080)


@pytest.mark.parametrize("display_unit", [1, 2, 3])
def test_mkv_display_aspect_ratio_uses_cropped_height_and_rejects_invalid_width(display_unit):
    video = b"".join(
        _ebml_element(key, val)
        for key, val in [(0xB0, 720), (0xBA, 580), (0x54BB, 2), (0x54AA, 2), (0x54B2, display_unit), (0x54BA, 9)]
    )
    assert _parse_mkv_video_element(video + _ebml_element(0x54B0, 16)) == (1024, 576)
    assert _parse_mkv_video_element(video + _ebml_element(0x54B0, 0)) == (720, 580)
    assert _parse_mkv_video_element(video + _ebml_element(0x54B0, 1 << 32)) == (720, 580)


def test_mkv_pixel_dimensions_reject_out_of_range_values():
    coded = _ebml_element(0xB0, 720) + _ebml_element(0xBA, 576)
    assert _parse_mkv_video_element(coded + _ebml_element(0x54B0, 1 << 31)) == (720, 576)
    assert _parse_mkv_video_element(coded + _ebml_element(0x54BA, 1 << 31)) == (720, 576)
    assert _parse_mkv_video_element(_ebml_element(0xB0, 1 << 31) + _ebml_element(0xBA, 576)) == (0, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("ratio", "rotation", "expected"),
    [("64/45", None, (1024, 576)), ("64/45", 90, (576, 1024)), ("1/1", None, (720, 576))],
)
async def test_mov_aperture_dimensions_match_local_metadata_and_full_moov(tmp_path, ratio, rotation, expected):
    path = _gen_mp4_family(
        tmp_path / "aperture.mov", w=720, h=576, dur=0.5, display_rotation=rotation, sample_aspect_ratio=ratio
    )
    with patch.object(media_meta, "probe_local_ffprobe", new=AsyncMock()) as fallback:
        local = await probe_local_video_metadata(str(path), ".mov")
    fallback.assert_not_awaited()
    assert (local.width, local.height) == expected

    data = path.read_bytes()
    moov = data.index(b"moov") - 4
    size = struct.unpack_from(">I", data, moov)[0]
    parsed = parse_moov(data[moov : moov + size])
    item = PublishItem(item_id="fid", name=path.name, size_bytes=len(data))
    _backfill_media_metadata_to_item(item, parsed)
    assert (item.width, item.height) == expected
    entry = parsed.video_track.stbl.stsd.entries[0]
    assert (entry.width, entry.height) == (720, 576)


@pytest.mark.asyncio
@pytest.mark.parametrize(("rotation", "expected"), [(None, (1024, 576)), (90, (576, 1024))])
async def test_local_and_pipe_ffprobe_scale_sample_aspect_ratio_before_rotation(tmp_path, rotation, expected):
    path = _gen_mp4_family(
        tmp_path / "anamorphic.mp4",
        w=720,
        h=576,
        dur=0.5,
        display_rotation=rotation,
        sample_aspect_ratio="64/45",
    )
    local = await probe_local_ffprobe(str(path))
    from_pipe = await probe_bytes_ffprobe(path.read_bytes())
    assert (local.width, local.height) == (from_pipe.width, from_pipe.height) == expected


@pytest.mark.parametrize("ratio", [None, "1:1", "N/A", "0:1", "1:0", "-1:2", "4000000:1"])
def test_ffprobe_unusable_or_square_sample_ratio_keeps_encoded_dimensions(ratio):
    stream = {"codec_type": "video", "width": 720, "height": 576, "sample_aspect_ratio": ratio}
    meta = _parse_ffprobe_json(json.dumps({"streams": [stream]}).encode())
    assert (meta.width, meta.height) == (720, 576)


def test_mkv_chooses_complete_enabled_default_track_and_keeps_complete_alternate():
    disabled = _mkv_track(640, 480, b"V_AV1", enabled=False)
    alternate = _mkv_track(320, 180, b"V_VP8", default=False)
    primary = _mkv_track(1920, 1080, b"V_VP9")
    assert _parse_mkv_tracks_element(disabled + alternate + primary) == (1920, 1080, "v_vp9", "")
    assert _parse_mkv_tracks_element(disabled + alternate + primary[:-1]) == (320, 180, "v_vp8", "")
    assert _parse_mkv_tracks_element(primary[:-1]) == (0, 0, "", "")


@pytest.mark.asyncio
async def test_mkv_nonfinite_duration_triggers_local_fallback(tmp_path):
    file = tmp_path / "nonfinite.mkv"
    info = _ebml_element(0x1549A966, _ebml_element(0x4489, struct.pack(">d", float("inf"))))
    tracks = _ebml_element(0x1654AE6B, _mkv_track(320, 240, b"V_VP9"))
    file.write_bytes(info + tracks)

    with patch.object(media_meta, "probe_local_ffprobe", new_callable=AsyncMock) as fallback:
        fallback.return_value = ProbedMediaMetadata(duration_s=2.0)
        meta = await probe_local_video_metadata(str(file), ".mkv")
    fallback.assert_awaited_once_with(str(file))
    assert (meta.duration_s, meta.width, meta.height) == (2.0, 320, 240)


def test_ffprobe_json_skips_attached_pic_and_applies_rotation_once():
    payload = b"""{
        "format": {"duration": "4.5"},
        "streams": [
            {"index": 0, "codec_type": "audio", "codec_name": "aac"},
            {"index": 1, "codec_type": "video", "codec_name": "mjpeg", "width": 600, "height": 600,
             "disposition": {"attached_pic": 1}},
            {"index": 2, "codec_type": "video", "codec_name": "h264", "width": 1920, "height": 1080,
             "disposition": {"default": 1, "attached_pic": 0},
             "side_data_list": [{"rotation": -90}]}
        ]
    }"""
    meta = _parse_ffprobe_json(payload)
    assert meta.duration_s == pytest.approx(4.5, abs=0.01)
    assert (meta.width, meta.height) == (1080, 1920)
    assert meta.video_codec == "h264"
    assert meta.audio_codec == "aac"


def test_apply_probed_metadata_retains_valid_fields():
    item = PublishItem(item_id="a.avi", name="a.avi", size_bytes=100, duration_s=12.0, width=0, height=0)
    apply_probed_metadata(item, ProbedMediaMetadata(duration_s=9.0, width=640, height=480))
    assert item.duration_s == 12.0
    assert (item.width, item.height) == (640, 480)


# ---------------------------------------------------------------------------
# 4. Controllable HTTP server: Remote TS verification + Remote probe budgets
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_remote_ts_head_verification_cap_valid_invalid_and_fail_open():
    valid_ts = _make_synthetic_ts(188, 30)  # 5640 bytes (> 2048)
    invalid_ts = b"export const x: number = 42;\n" * 200  # > 2048 bytes of TypeScript

    recorded_valid: list[tuple[int, int]] = []
    sent_valid: list[int] = []
    async with _RangeHttpServer(
        _make_byte_range_handler(valid_ts, recorded_ranges=recorded_valid, sent_bytes=sent_valid)
    ) as srv:
        verified, reason = await verify_remote_mpeg_ts_head(f"{srv.base_url}/ok.ts", file_size=len(valid_ts), ext=".ts")
        assert verified is True
        assert reason is None
        assert recorded_valid == [(0, 2047)]
        assert sum(sent_valid) <= 2048

    recorded_invalid: list[tuple[int, int]] = []
    sent_invalid: list[int] = []
    async with _RangeHttpServer(
        _make_byte_range_handler(invalid_ts, recorded_ranges=recorded_invalid, sent_bytes=sent_invalid)
    ) as srv:
        verified, reason = await verify_remote_mpeg_ts_head(
            f"{srv.base_url}/code.ts", file_size=len(invalid_ts), ext=".ts"
        )
        assert verified is False
        assert reason and "非有效 MPEG-TS" in reason
        assert recorded_invalid == [(0, 2047)]
        assert sum(sent_invalid) <= 2048

    # Non-206 (HTTP 200), 503, missing Content-Range, and short-read must all fail-open (None, None)
    async def _edge_handler(request: web.Request) -> web.StreamResponse:
        path = request.path
        if path == "/status200.ts":
            return web.Response(status=200, body=invalid_ts[:2048])
        if path == "/status503.ts":
            return web.Response(status=503, text="busy")
        if path == "/no_cr.ts":
            return web.Response(status=206, body=invalid_ts[:2048])
        if path == "/short.ts":
            return web.Response(
                status=206,
                body=invalid_ts[:100],
                headers={"Content-Range": f"bytes 0-99/{len(invalid_ts)}"},
            )
        if path == "/changed-size.ts":
            return web.Response(
                status=206,
                body=invalid_ts[:2048],
                headers={"Content-Range": f"bytes 0-2047/{len(invalid_ts) + 1}"},
            )
        return web.Response(status=404)

    async with _RangeHttpServer(_edge_handler) as srv:
        for path in ("/status200.ts", "/status503.ts", "/no_cr.ts", "/short.ts", "/changed-size.ts"):
            verified, reason = await verify_remote_mpeg_ts_head(
                f"{srv.base_url}{path}", file_size=len(invalid_ts), ext=".ts"
            )
            assert verified is None and reason is None


@pytest.mark.asyncio
async def test_remote_ts_head_with_invalid_utf8_tail_fails_open():
    """头部以本质无效的 UTF-8 字节结尾时不构成文本证据：无法判定 → 放行 (CR 6006788077)。"""
    payload = b"a" * 2047 + b"\xff" + b"\x00" * 4096  # > 2048 B，头部非完整文本亦非 TS 封包
    recorded: list[tuple[int, int]] = []
    async with _RangeHttpServer(_make_byte_range_handler(payload, recorded_ranges=recorded)) as srv:
        verified, reason = await verify_remote_mpeg_ts_head(
            f"{srv.base_url}/mixed.ts", file_size=len(payload), ext=".ts"
        )
    assert verified is None and reason is None
    assert recorded == [(0, 2047)]


@pytest.mark.asyncio
@pytest.mark.parametrize("cached_link_age_s", [0.0, 6.0])
async def test_pikpak_invalid_ts_skips_before_oversize_backfill_and_archive(monkeypatch, cached_link_age_s):
    """Invalid remote .ts (even with complete official metadata) is skipped before upload/oversize/backfill/archive."""
    invalid_ts = b"export interface Foo { bar: string }\n" * 150
    valid_ts = _make_synthetic_ts(188, 20)
    good_requests: list[tuple[str, str]] = []
    clock_s = 100.0
    monkeypatch.setattr(pik_engine, "time", SimpleNamespace(monotonic=lambda: clock_s))

    async def _handler(request: web.Request) -> web.StreamResponse:
        payload = invalid_ts if "bad" in request.path else valid_ts
        rng = request.headers.get("Range", "bytes=0-2047")
        if "good_video" in request.path:
            good_requests.append((request.query["link"], rng))
        s, e = rng[6:].split("-", 1)
        start, end = int(s), min(int(e) if e else len(payload) - 1, len(payload) - 1)
        return web.Response(
            status=206,
            body=payload[start : end + 1],
            headers={"Content-Range": f"bytes {start}-{end}/{len(payload)}"},
        )

    async with _RangeHttpServer(_handler) as srv:
        link_calls: list[list[str]] = []

        async def _link_provider(fids: list[str]) -> list[str]:
            link_calls.append(list(fids))
            fid = fids[0]
            return [f"{srv.base_url}/{fid}.ts?link={link_calls.count([fid])}"]

        async def _read_first_chunk(*args, chunk_provider, **kwargs):
            assert await chunk_provider.read_chunk(0, 16) == valid_ts[:16]
            return MagicMock()

        bad_oversize_item = {
            "file_id": "bad_oversize",
            "name": "oversize_code.ts",
            "size_bytes": len(invalid_ts),
            "is_dir": False,
            # Authoritative official metadata present — TS head check must STILL run!
            "duration_s": "120",
            "width": "1920",
            "height": "1080",
        }
        dts_item = {
            "file_id": "decl_dts",
            "name": "types.d.ts",
            "size_bytes": 500,
            "is_dir": False,
        }
        good_ts_item = {
            "file_id": "good_video",
            "name": "z_clip.ts",  # sorted after the invalid TS, so the send delay applies
            "size_bytes": len(valid_ts),
            "is_dir": False,
            "duration_s": "10",
            "width": "320",
            "height": "240",
        }

        mock_client = AsyncMock()
        mock_client.send_file.return_value = MagicMock(id=909)
        cleanup = MagicMock()
        cleanup.archive_single_item = AsyncMock(return_value=True)
        cleanup.cleanup = AsyncMock(return_value="")
        notifier = MagicMock()
        notifier.notify_batch = AsyncMock()
        notifier.notify_task_summary = AsyncMock()

        engine = PikPakStreamTransportEngine(link_provider=_link_provider)

        async def _send_delay(*args):
            nonlocal clock_s
            clock_s += cached_link_age_s

        pipeline = PublishPipeline(
            source=PikPakSnapshotSource(remote_files=[bad_oversize_item, dts_item, good_ts_item]),
            engine=engine,
            cleanup=cleanup,
            notifier=notifier,
        )

        ctx = PublishContext(
            task_id="T-TS-GATE",
            target_chat_id=-10099,
            userbot_client=mock_client,
            source_type="pikpak",
            delay_seconds=0.0,
            video_split_threshold_bytes=4000,
            oversize_policy=OVERSIZE_POLICY_SKIP_AND_REPORT,
            send_placeholder_card=True,
        )

        with (
            patch("app.core.pipeline.pipeline.phase1_preflight", new_callable=AsyncMock, return_value=None),
            patch("app.core.pipeline.pipeline.wait_with_signals", side_effect=_send_delay),
            patch.object(engine, "_make_thumb_provider", return_value=AsyncMock(return_value=b"fixed-thumbnail")),
            patch("app.core.fast_telethon.upload_file_parallel", side_effect=_read_first_chunk),
            patch(
                "app.db.publish_repository.PlaceholderSlotRepository.get_by_identity",
                return_value={
                    "id": 1,
                    "slot_kind": "backfillable",
                    "placeholder_msg_id": 55,
                    "state": "placeholder_sent",
                },
            ),
            patch("app.db.publish_repository.PlaceholderSlotRepository.cas_acquire_edit", return_value=False),
        ):
            result = await pipeline.execute(ctx)

        # bad_oversize_item & dts_item were skipped before oversize / backfill / archive
        assert result.skipped_files == []  # NO oversize skip/placeholder card for bad_oversize_item
        assert mock_client.send_message.await_count == 0  # NO placeholder card sent
        assert any("oversize_code.ts" in f for f in result.failed_files)
        assert any("types.d.ts" in f for f in result.failed_files)
        assert result.published_count == 1
        # Only good_video was archived; bad_oversize never triggered archive
        assert [call.args[0].item_id for call in cleanup.archive_single_item.await_args_list] == ["good_video"]
        # The media request reuses a fresh preflight link; an old link is replaced before upload.
        assert link_calls.count(["decl_dts"]) == 0
        expected_lookups = 2 if cached_link_age_s else 1
        assert link_calls.count(["good_video"]) == expected_lookups
        assert good_requests == [("1", "bytes=0-2047"), (str(expected_lookups), "bytes=0-15")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("body_size", "send_eof", "expected"),
    [(2047, True, None), (2048, True, False), (2049, True, None), (2048, False, None)],
)
async def test_ts_preflight_requires_exact_chunked_body(body_size, send_eof, expected):
    async def serve(request):
        assert request.headers["Range"] == "bytes=0-2047"
        response = web.StreamResponse(status=206, headers={"Content-Range": "bytes 0-2047/4096"})
        await response.prepare(request)
        await response.write(b"x" * min(body_size, 2048))
        if body_size > 2048:
            await asyncio.sleep(0.01)
            await response.write(b"x")
        if not send_eof:
            await asyncio.sleep(0.6)
        return response

    async with _RangeHttpServer(serve) as srv:
        result, reason = await verify_remote_mpeg_ts_head(f"{srv.base_url}/chunked.ts", file_size=4096, timeout_s=0.5)

    assert result is expected
    assert bool(reason) is (expected is False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "max_bytes", "expected", "ranges"),
    [
        pytest.param(_make_synthetic_ts(188, 3), 2048, None, [], id="three-packets"),
        pytest.param(_make_synthetic_ts(204, 8), 815, None, [(0, 814)], id="insufficient-budget-for-204-layout"),
        pytest.param(_make_synthetic_ts(188, 4), 2048, True, [(0, 751)], id="valid-188-layout"),
        pytest.param(_make_synthetic_ts(204, 4), 2048, True, [(0, 815)], id="valid-204-layout"),
        pytest.param(b"x" * 816, 2048, False, [(0, 815)], id="sufficient-invalid-sample"),
        pytest.param(bytes(1500) + _make_synthetic_ts(204, 8), 2048, None, [(0, 2047)], id="late-sync"),
        pytest.param(bytes(816), 2048, False, [(0, 815)], id="complete-invalid-binary"),
    ],
)
async def test_ts_preflight_distinguishes_short_samples_from_invalid_content(payload, max_bytes, expected, ranges):
    requests: list[tuple[int, int]] = []
    async with _RangeHttpServer(_make_byte_range_handler(payload, recorded_ranges=requests)) as srv:
        result, reason = await verify_remote_mpeg_ts_head(
            f"{srv.base_url}/clip.ts", file_size=len(payload), max_bytes=max_bytes
        )

    assert result is expected
    assert bool(reason) is (expected is False)
    assert requests == ranges


@pytest.mark.asyncio
async def test_remote_metadata_probe_budgets_concurrency_no_probe_when_complete_and_redaction(
    tmp_path, caplog, cover_jpeg
):
    rot_mp4 = _gen_mp4_family(tmp_path / "remote_rot.mp4", w=320, h=240, dur=2.0, display_rotation=90)
    mp4_bytes = rot_mp4.read_bytes()

    # 1) Complete official metadata -> ZERO probe requests
    complete_requests: list[tuple[int, int]] = []
    async with _RangeHttpServer(_make_byte_range_handler(mp4_bytes, recorded_ranges=complete_requests)) as srv:
        mock_client = AsyncMock()
        mock_client.send_file.return_value = MagicMock(id=301)
        engine = PikPakStreamTransportEngine(
            link_provider=AsyncMock(return_value=[f"{srv.base_url}/complete.mp4?sig=SUPER_SECRET_SIG"])
        )
        item = PublishItem(
            item_id="comp1",
            name="complete.mp4",
            size_bytes=len(mp4_bytes),
            source_type="pikpak",
            duration_s=10.0,
            width=1280,
            height=720,
            thumbnail_data=cover_jpeg,
        )
        ctx = PublishContext(task_id="T-COMP", target_chat_id=-1005, userbot_client=mock_client, source_type="pikpak")
        with patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()):
            await engine.send_item(item, ctx)
        assert complete_requests == []
        va = next(
            a for a in mock_client.send_file.call_args.kwargs["attributes"] if isinstance(a, DocumentAttributeVideo)
        )
        assert (va.w, va.h, va.duration) == (1280, 720, 10)

    # 2) Missing metadata -> bounded probe extracts rotated display dims (240x320) & duration, respects byte cap
    sent_bytes: list[int] = []
    async with _RangeHttpServer(_make_byte_range_handler(mp4_bytes, sent_bytes=sent_bytes)) as srv:
        res = await probe_remote_video_metadata(
            f"{srv.base_url}/rot.mp4?sig=SUPER_SECRET_SIG",
            ".mp4",
            file_size=len(mp4_bytes),
            max_bytes=256 * 1024,
            deadline_s=3.0,
        )
        assert (res.width, res.height) == (240, 320)
        assert 1.0 <= res.duration_s <= 3.0
        assert sum(sent_bytes) <= 256 * 1024

    # 3) Strict byte cap on oversized blob: bytes read never exceeds max_bytes
    big_blob = b"\x00" * (200 * 1024)
    capped_sent: list[int] = []
    async with _RangeHttpServer(_make_byte_range_handler(big_blob, sent_bytes=capped_sent)) as srv:
        res_cap = await probe_remote_video_metadata(
            f"{srv.base_url}/big.mp4?sig=SUPER_SECRET_SIG",
            ".mp4",
            file_size=len(big_blob),
            max_bytes=16 * 1024,
            deadline_s=3.0,
        )
        assert res_cap == ProbedMediaMetadata()
        assert sum(capped_sent) <= 16 * 1024

    # 4) Signed URLs stay out of probe logs.
    caplog.set_level(logging.DEBUG)
    async with _RangeHttpServer(_make_byte_range_handler(mp4_bytes)) as srv:
        url = f"{srv.base_url}/slow.mp4?sig=TOP_SECRET_NEVER_LOG"
        result = await probe_remote_video_metadata(url, ".mp4", file_size=len(mp4_bytes))
        assert (result.width, result.height) == (240, 320)

    app_log_text = "\n".join(r.getMessage() for r in caplog.records if r.name.startswith("app."))
    assert "TOP_SECRET_NEVER_LOG" not in app_log_text
    assert "SUPER_SECRET_SIG" not in app_log_text
    assert "TOP_SECRET_NEVER_LOG" not in caplog.text

    # 5) Cancel signal propagates immediately
    with pytest.raises(UploadCancelledError):
        await probe_remote_video_metadata(
            "http://127.0.0.1:9/x.mp4",
            ".mp4",
            file_size=100,
            is_cancelled=lambda: True,
        )


@pytest.mark.asyncio
async def test_remote_probe_queue_timeout_leaves_url_provider_uncalled():
    entered = asyncio.Queue()
    release = asyncio.Event()

    async def hold_slot():
        entered.put_nowait(None)
        await release.wait()
        return ""

    first = asyncio.create_task(probe_remote_video_metadata(hold_slot, ".mp4"))
    second = asyncio.create_task(verify_remote_mpeg_ts_head(hold_slot, file_size=2048))
    third_provider = AsyncMock(return_value="")
    try:
        await asyncio.wait_for(entered.get(), timeout=2)
        await asyncio.wait_for(entered.get(), timeout=2)
        assert await probe_remote_video_metadata(third_provider, ".mp4", deadline_s=0.15) == ProbedMediaMetadata()
        third_provider.assert_not_awaited()
        assert not first.done() and not second.done()
    finally:
        release.set()
        await asyncio.gather(first, second)


def test_remote_probe_semaphore_does_not_retain_closed_contended_loop():
    async def contend():
        sem = media_meta._get_remote_probe_semaphore()
        async with sem, sem:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(sem.acquire(), timeout=0.01)

    loop = asyncio.new_event_loop()
    loop_ref = weakref.ref(loop)
    try:
        loop.run_until_complete(contend())
    finally:
        loop.close()
    del loop
    gc.collect()
    assert loop_ref() is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("probe", "kwargs"),
    [
        (probe_remote_video_metadata, {"ext": ".mp4", "deadline_s": 0.2}),
        (verify_remote_mpeg_ts_head, {"file_size": 2048, "timeout_s": 0.2}),
    ],
)
@pytest.mark.parametrize(
    ("control", "error"), [("is_paused", UploadPausedError), ("is_cancelled", UploadCancelledError)]
)
async def test_timed_out_remote_probe_propagates_task_control(probe, kwargs, control, error):
    entered = asyncio.Event()
    signal = asyncio.Event()

    async def stalled_provider():
        entered.set()
        await asyncio.Future()

    task = asyncio.create_task(probe(stalled_provider, **kwargs, **{control: signal.is_set}))
    await asyncio.wait_for(entered.wait(), timeout=1)
    signal.set()
    with pytest.raises(error):
        await task


def _iso_box(kind: bytes, payload: bytes) -> bytes:
    return struct.pack(">I4s", len(payload) + 8, kind) + payload


def test_tail_moov_scan_shares_work_budget_across_candidates():
    moov = _iso_box(b"moov", _iso_box(b"mvhd", b""))
    other = _iso_box(b"moov", _iso_box(b"free", b""))
    window = moov + other
    scanner = TailMoovScanner(len(other))
    assert scanner.find(window, len(window)) is None
    assert scanner.exhausted
    assert scanner.find(moov, len(moov)) is None
    assert TailMoovScanner(len(window)).find(window, len(window)) == moov

    window = moov + other * 64
    scanner = TailMoovScanner(len(window))
    assert scanner.find(window, len(window)) is None
    assert scanner.exhausted


@pytest.mark.asyncio
async def test_normal_tail_moov_is_found_through_both_probes(tmp_path):
    path = tmp_path / "tail.mp4"
    _run_ffmpeg(["-f", "lavfi", "-i", "testsrc2=size=320x240:rate=10:duration=0.5", "-c:v", "libx264", str(path)])
    data = path.read_bytes()
    moov_start = data.index(b"moov") - 4
    tail_data = data[:moov_start] + _iso_box(b"free", bytes(131072)) + data[moov_start:]
    path.write_bytes(tail_data)
    local = await Mp4StssProber.probe_from_url(str(path))

    async with _RangeHttpServer(_make_byte_range_handler(tail_data)) as srv:
        remote = await probe_remote_video_metadata(f"{srv.base_url}/tail.mp4", ".mp4", file_size=len(tail_data))

    assert (local.width, local.height, local.duration_s) == (320, 240, 0.5)
    assert (remote.width, remote.height, remote.duration_s) == (320, 240, 0.5)


@pytest.mark.asyncio
async def test_zero_sized_moov_uses_known_eof(tmp_path):
    data = _gen_mp4_family(tmp_path / "zero.mov", dur=0.5).read_bytes()
    moov_start = data.index(b"moov") - 4
    moov_size = struct.unpack_from(">I", data, moov_start)[0]
    # A final size-zero box uses the end of this data as its end.
    payload = data[:moov_start] + bytes(4) + data[moov_start + 4 : moov_start + moov_size]
    async with _RangeHttpServer(_make_byte_range_handler(payload)) as srv:
        url = f"{srv.base_url}/zero.mov"
        found = await probe_remote_video_metadata(url, ".mov", file_size=len(payload))
        unknown_end = await probe_remote_video_metadata(url, ".mov", max_bytes=len(payload) - 1)
        capped = await probe_remote_video_metadata(url, ".mov", file_size=len(payload), max_bytes=len(payload) - 1)

    original = Mp4StssProber._parse_moov_keyframes(data[moov_start : moov_start + moov_size])
    assert (found.width, found.height, found.duration_s) == (320, 240, original.duration_s)
    assert found.duration_s > 0
    assert capped == unknown_end == ProbedMediaMetadata()


@pytest.mark.asyncio
async def test_range_error_message_redacts_complete_signed_url(caplog):
    url = "https://fixture.invalid/movie.mp4?sig=SYNTHETIC_SIGNED_URL_TOKEN"
    session = MagicMock()
    session.get.side_effect = ClientConnectionError(f"Fixture connection failed: {url}")

    caplog.set_level(logging.DEBUG, logger=media_meta.__name__)
    with patch.object(media_meta.cdn_mod, "get_cdn_session", new=AsyncMock(return_value=session)):
        assert await probe_remote_video_metadata(url, ".mp4", file_size=512) == ProbedMediaMetadata()

    emitted = [record.getMessage() for record in caplog.records if record.name == media_meta.__name__]
    assert any("Fixture connection failed" in msg and "fixture.invalid" in msg for msg in emitted)
    assert "SYNTHETIC_SIGNED_URL_TOKEN" not in caplog.text


@pytest.mark.asyncio
async def test_remote_ffprobe_reserves_time_for_process_cleanup():
    proc = MagicMock(returncode=None)
    loop = asyncio.get_running_loop()
    killed_at = None

    def kill():
        nonlocal killed_at
        killed_at = loop.time()

    async def communicate(*args, **kwargs):
        if killed_at is None:
            await asyncio.Event().wait()
        await asyncio.sleep(0.08)
        proc.returncode = -9
        return b"", b""

    proc.kill.side_effect = kill
    proc.communicate = AsyncMock(side_effect=communicate)
    deadline_s = 0.6
    async with _RangeHttpServer(_make_byte_range_handler(b"x" * 128)) as srv:
        with (
            patch.object(media_meta.shutil, "which", return_value="ffprobe"),
            patch.object(media_meta.asyncio, "create_subprocess_exec", new=AsyncMock(return_value=proc)),
        ):
            started = loop.time()
            result = await probe_remote_video_metadata(
                f"{srv.base_url}/slow.avi", ".avi", file_size=128, deadline_s=deadline_s
            )
            elapsed = loop.time() - started

    assert result == ProbedMediaMetadata()
    proc.kill.assert_called_once()
    assert proc.communicate.await_count == 2 and proc.returncode == -9
    assert killed_at is not None and killed_at - started < deadline_s - 0.05
    assert elapsed < deadline_s + 0.1
    semaphore = media_meta._get_remote_probe_semaphore()
    async with asyncio.timeout(0.5), semaphore, semaphore:
        assert semaphore.locked()  # Both slots were released.


@pytest.mark.asyncio
async def test_budgeted_range_reader_rejects_200_and_malformed_206_without_overreading():
    """Strict Range reader rejects HTTP 200, mismatched Content-Range/Length, and short bodies before overreading."""
    import time

    static_responses: dict[str, tuple[int, bytes, dict[str, str]]] = {
        "/http200": (200, b"X" * 4096, {}),
        "/no_cr": (206, b"Y" * 64, {}),
        "/bad_start": (206, b"Z" * 64, {"Content-Range": "bytes 10-73/4096"}),
        "/gzip_encoded": (206, b"B" * 64, {"Content-Range": "bytes 0-63/4096", "Content-Encoding": "gzip"}),
    }
    stream_cases: dict[str, tuple[dict[str, str], bytes]] = {
        "/bad_cl": ({"Content-Range": "bytes 0-63/4096", "Content-Length": "4096"}, b"A" * 64),
        "/short_body": ({"Content-Range": "bytes 0-63/4096"}, b"C" * 20),
    }

    async def _malformed_handler(request: web.Request) -> web.StreamResponse:
        path = request.path
        if path in static_responses:
            status, body, headers = static_responses[path]
            return web.Response(status=status, body=body, headers=headers)
        if path in stream_cases:
            headers, body = stream_cases[path]
            resp = web.StreamResponse(status=206, headers=headers)
            await resp.prepare(request)
            await resp.write(body)
            await resp.write_eof()
            return resp
        return web.Response(status=404)

    async with _RangeHttpServer(_malformed_handler) as srv:
        # HTTP 200 with 4096 bytes when requesting 64 bytes -> rejected before body read, budget reserved
        r200 = BudgetedRangeReader(f"{srv.base_url}/http200", max_bytes=64, deadline_monotonic=time.monotonic() + 3.0)
        assert await r200.fetch_range(0, 63) == b""
        assert r200.bytes_read == 0
        assert r200.bytes_reserved == 64
        assert r200.remaining_bytes == 0

        for path in ("/no_cr", "/bad_start", "/bad_cl", "/gzip_encoded"):
            rdr = BudgetedRangeReader(f"{srv.base_url}{path}", max_bytes=64, deadline_monotonic=time.monotonic() + 3.0)
            assert await rdr.fetch_range(0, 63) == b""
            assert rdr.bytes_read == 0
            assert rdr.remaining_bytes == 0

        # Short body: returns no data, reserves full 64B so subsequent read cannot exceed cumulative 64B cap
        r_short = BudgetedRangeReader(
            f"{srv.base_url}/short_body", max_bytes=64, deadline_monotonic=time.monotonic() + 3.0
        )
        assert await r_short.fetch_range(0, 63) == b""
        assert r_short.bytes_read <= 20
        assert r_short.remaining_bytes == 0
        # Second fetch is immediately blocked by exhausted reserved budget
        assert await r_short.fetch_range(0, 63) == b""
        assert r_short.requests_made == 1


@pytest.mark.asyncio
async def test_remote_ffprobe_pipe_prefix_and_truncated_ts_duration_guard(tmp_path):
    """Remote non-MP4 uses bounded 128 KiB prefix via ffprobe pipe:0; truncated TS does not invent full duration."""
    avi_path = tmp_path / "clip.avi"
    _run_ffmpeg(
        [
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=320x240:rate=15:duration=2",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=44100:duration=2",
            "-c:v",
            "mpeg4",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "mp3",
            "-shortest",
            str(avi_path),
        ]
    )
    avi_bytes = avi_path.read_bytes()
    avi_sent: list[int] = []
    async with _RangeHttpServer(_make_byte_range_handler(avi_bytes, sent_bytes=avi_sent)) as srv:
        meta = await probe_remote_video_metadata(
            f"{srv.base_url}/clip.avi",
            ".avi",
            file_size=len(avi_bytes),
            deadline_s=4.0,
        )
        assert (meta.width, meta.height) == (320, 240)
        assert 1.0 <= meta.duration_s <= 3.5
        assert sum(avi_sent) <= 131072

    # Real TS file larger than 128 KiB -> truncated prefix must yield dims only (duration_s == 0.0)
    ts_path = tmp_path / "long.ts"
    _run_ffmpeg(
        [
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=320x240:rate=15:duration=6",
            "-c:v",
            "libx264",
            "-b:v",
            "500k",
            "-pix_fmt",
            "yuv420p",
            str(ts_path),
        ]
    )
    ts_bytes = ts_path.read_bytes()
    assert len(ts_bytes) > 131072
    ts_sent: list[int] = []
    async with _RangeHttpServer(_make_byte_range_handler(ts_bytes, sent_bytes=ts_sent)) as srv:
        ts_meta = await probe_remote_video_metadata(
            f"{srv.base_url}/long.ts",
            ".ts",
            file_size=len(ts_bytes),
            deadline_s=4.0,
        )
        assert (ts_meta.width, ts_meta.height) == (320, 240)
        assert ts_meta.duration_s == 0.0
        assert sum(ts_sent) <= 131072


@pytest.mark.asyncio
async def test_jit_link_provider_bounded_by_semaphore_deadline_and_skipped_for_unsupported_ext():
    """JIT link provider runs inside shared semaphore + deadline; unsupported ext never calls provider."""
    called = 0

    async def _never_call() -> str:
        nonlocal called
        called += 1
        return "http://127.0.0.1:9/nope"

    # Unsupported extension -> zero provider calls
    assert (
        await probe_remote_video_metadata(_never_call, ".zip", file_size=100, deadline_s=1.0) == ProbedMediaMetadata()
    )
    assert called == 0

    # Hanging JIT link provider -> bounded by deadline (fail-open, does not hang)
    async def _slow_provider() -> str:
        await asyncio.sleep(5.0)
        return "http://127.0.0.1:9/slow.mp4"

    res_meta = await probe_remote_video_metadata(_slow_provider, ".mp4", file_size=100, deadline_s=0.2)
    assert res_meta == ProbedMediaMetadata()

    verified, reason = await verify_remote_mpeg_ts_head(_slow_provider, file_size=2048, ext=".ts", timeout_s=0.2)
    assert verified is None and reason is None

    # Shared concurrency cap across metadata probe + TS head verification includes JIT link lookup
    valid_ts = _make_synthetic_ts(188, 20)
    provider_active = {"cur": 0, "max": 0}
    async with _RangeHttpServer(_make_byte_range_handler(valid_ts)) as srv:

        async def _tracked_provider() -> str:
            provider_active["cur"] += 1
            provider_active["max"] = max(provider_active["max"], provider_active["cur"])
            try:
                await asyncio.sleep(0.15)
                return f"{srv.base_url}/ok.ts"
            finally:
                provider_active["cur"] -= 1

        ts_probes = [
            verify_remote_mpeg_ts_head(_tracked_provider, file_size=len(valid_ts), timeout_s=2.0) for _ in range(2)
        ]
        metadata_probes = [
            probe_remote_video_metadata(_tracked_provider, ".mp4", file_size=len(valid_ts), deadline_s=2.0)
            for _ in range(2)
        ]
        ts_one, meta_one, ts_two, meta_two = await asyncio.gather(
            ts_probes[0], metadata_probes[0], ts_probes[1], metadata_probes[1]
        )
        assert provider_active["max"] == 2
        assert ts_one == ts_two == (True, None)
        assert meta_one == meta_two == ProbedMediaMetadata()


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["vsse", "fmp4"])
async def test_vsse_fmp4_do_not_reuse_prefetched_ts_link(route):
    """Prefetched TS link is discarded by VSSE/fMP4 so split channels always start with a fresh link."""
    link_calls: list[list[str]] = []

    async def _link_provider(fids: list[str]) -> list[str]:
        link_calls.append(list(fids))
        return [f"http://127.0.0.1:9/fresh_{len(link_calls)}.ts"]

    engine = PikPakStreamTransportEngine(link_provider=_link_provider)
    engine._prefetched_links["fid-ts-big"] = ("http://127.0.0.1:9/stale_prefetched.ts", pik_engine.time.monotonic())
    item = PublishItem(item_id="fid-ts-big", name="video.ts", size_bytes=4096, source_type="pikpak")
    context = PublishContext(task_id="fresh-split", target_chat_id=-1001)
    outcome = TransportOutcome(message_ids=[7])
    fresh_url = "http://127.0.0.1:9/fresh_1.ts"

    with (
        patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock) as precheck,
        patch.object(engine, "_fetch_engine_thumb", new=AsyncMock(return_value=None)),
        patch.object(pik_engine, "probe_vsse_envelope", new=AsyncMock(return_value=(MagicMock(), [], []))) as vsse,
        patch.object(pik_engine, "probe_fmp4_media", new=AsyncMock(return_value=(".ts", False, 8, None))) as fmp4,
        patch.object(pik_engine, "execute_split_loop", new=AsyncMock(return_value=outcome)),
    ):
        publish = getattr(engine, f"_publish_{route}_split_item")
        assert await publish(AsyncMock(), item, 2048, context) is outcome

    precheck.assert_awaited_once_with(fresh_url)
    if route == "vsse":
        assert vsse.await_args.args[2]._inner._current_url == fresh_url
    else:
        assert fmp4.await_args.args[0] == fresh_url
    assert "fid-ts-big" not in engine._prefetched_links
    assert link_calls == [["fid-ts-big"]]


@pytest.mark.asyncio
async def test_pikpak_remote_ts_fail_open_records_task_log():
    """Fail-open remote TS verification records task-scoped log without caching link."""
    store = MagicMock()
    store.append_log = AsyncMock()
    engine = PikPakStreamTransportEngine(
        link_provider=AsyncMock(return_value=["http://127.0.0.1:9/fake_sample.ts"]),
        store=store,
    )
    item = PublishItem(item_id="fid-ts-fo", name="fake_sample.ts", size_bytes=2048, source_type="pikpak")

    with patch.object(pik_engine, "verify_remote_mpeg_ts_head", new=AsyncMock(return_value=(None, None))):
        allowed, reason = await engine.verify_remote_ts_item(
            item,
            PublishContext(task_id="T-TS-FO", target_chat_id=-1001),
        )
        assert allowed is True and reason is None
        store.append_log.assert_awaited_once_with(
            "T-TS-FO",
            f"{pik_engine.TS_FAIL_OPEN_LOG_PREFIX} [fake_sample.ts]",
        )
        assert "fid-ts-fo" not in engine._prefetched_links

        store.append_log.reset_mock()
        allowed_no_task, reason_no_task = await engine.verify_remote_ts_item(
            item,
            PublishContext(task_id="", target_chat_id=-1001),
        )
        assert allowed_no_task is True and reason_no_task is None
        store.append_log.assert_not_called()

        store.append_log = AsyncMock(side_effect=RuntimeError("db busy"))
        allowed_db_err, reason_db_err = await engine.verify_remote_ts_item(
            item,
            PublishContext(task_id="T-TS-FO", target_chat_id=-1001),
        )
        assert allowed_db_err is True and reason_db_err is None
