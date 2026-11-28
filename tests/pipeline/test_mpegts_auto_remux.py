"""Unit & real-ffmpeg tests for MPEG-TS (.ts/.m2ts/.mts) zero-disk MP4 auto-remux."""

import asyncio
import json
import shutil
import subprocess
from contextlib import suppress
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telethon.errors import FilePartsInvalidError
from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest
from telethon.tl.types import InputFile

from app.core.e2e_capture import E2ECaptureSession
from app.core.fmp4_pipe_uploader import PartOversizeError, PartTruncatedError, ZeroDiskFmp4StreamUploader
from app.core.media_metadata import ProbedMediaMetadata, needs_metadata_probe
from app.core.pikpak.taxonomy import PikPakTransientError
from app.core.pipeline.engines import LocalDiskTransportEngine, PikPakStreamTransportEngine
from app.core.pipeline.engines._common import ProgressReporter
from app.core.pipeline.engines._remux import (
    build_remux_video_attributes,
    decide_mpegts_remux,
    is_mpegts_remux_candidate,
    remux_output_filename,
)
from app.core.pipeline.engines._types import VolumeJob
from app.core.pipeline.engines.exceptions import TransportError
from app.core.pipeline.engines.pikpak.fmp4 import Fmp4SplitChannel, compute_fmp4_plan_signature, probe_fmp4_media
from app.core.pipeline.models import PublishContext, PublishItem
from app.core.scheduler import UploadPausedError
from app.core.stream_chunk_provider import ChunkFetchTransientError, PikPakLinkExpiredError
from app.services.pikpak_publisher_service import PikPakPublisherService
from app.services.publisher_service import PublisherService


def _make_context(**kwargs) -> PublishContext:
    defaults = {
        "task_id": "task_remux_01",
        "target_chat_id": -1009000000010,
        "auto_cleanup_mode": "none",
        "auto_remux_mpegts_to_mp4": True,
    }
    defaults.update(kwargs)
    return PublishContext(**defaults)


def test_remux_candidate_and_filename_helpers():
    ctx_on = _make_context(auto_remux_mpegts_to_mp4=True)
    ctx_off = _make_context(auto_remux_mpegts_to_mp4=False)

    assert is_mpegts_remux_candidate("clip.ts", ctx_on) is True
    assert is_mpegts_remux_candidate("CLIP.M2TS", ctx_on) is True
    assert is_mpegts_remux_candidate("video.mts", ctx_on) is True
    assert is_mpegts_remux_candidate("types.d.ts", ctx_on) is False
    assert is_mpegts_remux_candidate("movie.mp4", ctx_on) is False
    assert is_mpegts_remux_candidate("clip.ts", ctx_off) is False

    assert remux_output_filename("clip.ts") == "clip.mp4"
    assert remux_output_filename("CLIP.M2TS") == "CLIP.mp4"
    assert remux_output_filename("stream.mts") == "stream.mp4"


def test_decide_mpegts_remux_codec_matrix():
    h264_aac = PublishItem(item_id="1", name="a.ts", size_bytes=100, video_codec="h264", audio_codec="aac")
    dec = decide_mpegts_remux(h264_aac, ffmpeg_available=True, require_known_vcodec=True)
    assert dec.should_remux is True
    assert dec.copy_audio is True

    hevc_ac3 = PublishItem(item_id="2", name="b.ts", size_bytes=100, video_codec="hevc", audio_codec="ac3")
    dec2 = decide_mpegts_remux(hevc_ac3, ffmpeg_available=True, require_known_vcodec=True)
    assert dec2.should_remux is True
    assert dec2.copy_audio is False

    mpeg2 = PublishItem(item_id="3", name="c.ts", size_bytes=100, video_codec="mpeg2video", audio_codec="mp2")
    dec3 = decide_mpegts_remux(mpeg2, ffmpeg_available=True, require_known_vcodec=True)
    assert dec3.should_remux is False
    assert "mpeg2video" in dec3.skip_reason

    unknown = PublishItem(item_id="4", name="d.ts", size_bytes=100)
    assert decide_mpegts_remux(unknown, ffmpeg_available=True, require_known_vcodec=True).should_remux is False
    dec4 = decide_mpegts_remux(unknown, ffmpeg_available=True, require_known_vcodec=False)
    assert dec4.should_remux is True
    assert dec4.copy_audio is False

    assert decide_mpegts_remux(h264_aac, ffmpeg_available=False, require_known_vcodec=True).should_remux is False


def test_needs_metadata_probe_require_codecs():
    item = PublishItem(item_id="1", name="v.ts", size_bytes=1024, duration_s=100.0, width=1920, height=1080)
    assert needs_metadata_probe(item) is False
    assert needs_metadata_probe(item, require_codecs=True) is True

    item.video_codec = "h264"
    assert needs_metadata_probe(item, require_codecs=True) is True

    item.audio_codec = "aac"
    assert needs_metadata_probe(item, require_codecs=True) is False


def test_build_remux_video_attributes_prefers_valid_item_dims():
    item = PublishItem(item_id="1", name="v.ts", size_bytes=1024, duration_s=60.0, width=1920, height=1080)
    attrs = build_remux_video_attributes(item, 61.4, 1280, 720)
    assert len(attrs) == 1
    assert attrs[0].duration == 61
    assert attrs[0].w == 1920
    assert attrs[0].h == 1080
    assert attrs[0].supports_streaming is True

    item_no_dims = PublishItem(item_id="2", name="v.ts", size_bytes=1024)
    attrs2 = build_remux_video_attributes(item_no_dims, 12.0, 1280, 720)
    assert attrs2[0].w == 1280
    assert attrs2[0].h == 720


@pytest.mark.asyncio
async def test_pikpak_send_item_ts_auto_remux_and_identity_preserved():
    store = MagicMock()
    store.append_log = AsyncMock()
    store.get_by_id = AsyncMock(return_value=None)
    link_provider = AsyncMock(return_value=["https://cdn.pikpak/sample.ts"])
    engine = PikPakStreamTransportEngine(link_provider=link_provider, store=store)

    item = PublishItem(
        item_id="fid_ts_1",
        name="clip.ts",
        size_bytes=50 * 1024 * 1024,
        duration_s=120.0,
        width=1920,
        height=1080,
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.send_file = AsyncMock(return_value=MagicMock(id=777))
    ctx = _make_context(userbot_client=mock_client, source_type="pikpak")

    fake_input = MagicMock(name="InputFileBig")

    async def fake_probe(url_or_provider, ext, **kwargs):
        return ProbedMediaMetadata(duration_s=120.0, width=1920, height=1080, video_codec="h264", audio_codec="aac")

    with (
        patch("app.core.pipeline.engines.pikpak.single.shutil.which", return_value="/usr/bin/ffmpeg"),
        patch("app.core.pipeline.engines.pikpak.single.probe_remote_video_metadata", side_effect=fake_probe) as p_mock,
        patch.object(
            ZeroDiskFmp4StreamUploader,
            "stream_to_telegram",
            new_callable=AsyncMock,
            return_value=(fake_input, 120.2),
        ) as stream_mock,
        patch.object(engine, "_fetch_engine_thumb", new_callable=AsyncMock, return_value=b"thumb-jpeg"),
    ):
        outcome = await engine.send_item(item, ctx)

    assert outcome.message_ids == [777]
    p_mock.assert_awaited_once()
    assert stream_mock.await_args is not None
    skw = stream_mock.await_args.kwargs
    assert skw["file_name"] == "clip.mp4"
    assert skw["copy_audio"] is True
    assert skw["full_stream"] is True
    assert skw["force_single_input"] is True
    assert skw["pause_yield"] is True

    sf_kwargs = mock_client.send_file.await_args.kwargs
    assert sf_kwargs["caption"] == "clip.ts"
    assert sf_kwargs["mime_type"] == "video/mp4"
    assert sf_kwargs["supports_streaming"] is True
    assert sf_kwargs["attributes"][0].supports_streaming is True
    # Original identity keys must never be mutated
    assert item.name == "clip.ts"
    assert item.item_id == "fid_ts_1"


@pytest.mark.asyncio
async def test_pikpak_send_item_ts_incompatible_codec_falls_back_to_raw():
    store = MagicMock()
    store.append_log = AsyncMock()
    store.get_by_id = AsyncMock(return_value=None)
    engine = PikPakStreamTransportEngine(
        link_provider=AsyncMock(return_value=["https://cdn.pikpak/mpeg2.ts"]),
        store=store,
    )
    item = PublishItem(
        item_id="fid_mpeg2",
        name="legacy.ts",
        size_bytes=20 * 1024 * 1024,
        duration_s=60.0,
        width=720,
        height=480,
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.send_file = AsyncMock(return_value=MagicMock(id=888))
    ctx = _make_context(userbot_client=mock_client, source_type="pikpak")

    with (
        patch("app.core.pipeline.engines.pikpak.single.shutil.which", return_value="/usr/bin/ffmpeg"),
        patch(
            "app.core.pipeline.engines.pikpak.single.probe_remote_video_metadata",
            new_callable=AsyncMock,
            return_value=ProbedMediaMetadata(video_codec="mpeg2video", audio_codec="mp2"),
        ),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()),
        patch.object(engine, "_make_thumb_provider", return_value=None),
    ):
        outcome = await engine.send_item(item, ctx)

    assert outcome.message_ids == [888]
    sf_kwargs = mock_client.send_file.await_args.kwargs
    assert sf_kwargs["mime_type"] == "video/mp2t"
    store.append_log.assert_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [PikPakLinkExpiredError, ChunkFetchTransientError])
async def test_pikpak_remux_pause_propagates_and_source_read_retries(error_type):
    link_provider = AsyncMock(
        side_effect=[["https://cdn/1.ts"], PikPakTransientError("temporary link lookup failure"), ["https://cdn/2.ts"]]
    )
    engine = PikPakStreamTransportEngine(link_provider=link_provider)
    item = PublishItem(
        item_id="fid_retry",
        name="retry.ts",
        size_bytes=5 * 1024 * 1024,
        video_codec="h264",
        audio_codec="aac",
        duration_s=10.0,
        width=1280,
        height=720,
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.send_file = AsyncMock(return_value=MagicMock(id=999))
    ctx = _make_context(userbot_client=mock_client, source_type="pikpak")

    with (
        patch("app.core.pipeline.engines.pikpak.single.shutil.which", return_value="/usr/bin/ffmpeg"),
        patch.object(
            ZeroDiskFmp4StreamUploader,
            "stream_to_telegram",
            new_callable=AsyncMock,
            side_effect=[error_type("source read failed"), (MagicMock(), 10.0)],
        ) as stream_mock,
        patch.object(engine, "_fetch_engine_thumb", new_callable=AsyncMock, return_value=None),
    ):
        outcome = await engine.send_item(item, ctx)
    assert outcome.message_ids == [999]
    assert [call.kwargs["pikpak_url"] for call in stream_mock.await_args_list] == [
        "https://cdn/1.ts",
        "https://cdn/2.ts",
    ]
    assert all(call.kwargs["expected_input_bytes"] == item.size_bytes for call in stream_mock.await_args_list)
    assert link_provider.await_count == 3

    # Pause must propagate immediately without falling back to raw .ts
    engine2 = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value=["https://cdn/p.ts"]))
    with (
        patch("app.core.pipeline.engines.pikpak.single.shutil.which", return_value="/usr/bin/ffmpeg"),
        patch.object(
            ZeroDiskFmp4StreamUploader,
            "stream_to_telegram",
            new_callable=AsyncMock,
            side_effect=UploadPausedError("paused"),
        ),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as raw_mock,
    ):
        with pytest.raises(UploadPausedError):
            await engine2.send_item(item, ctx)
        raw_mock.assert_not_called()


@pytest.mark.asyncio
async def test_pikpak_edit_item_ts_auto_remux():
    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value=["https://cdn/edit.ts"]))
    item = PublishItem(
        item_id="fid_edit_ts",
        name="slot.mts",
        size_bytes=8 * 1024 * 1024,
        video_codec="h264",
        audio_codec="aac",
        duration_s=15.0,
        width=1920,
        height=1080,
        source_type="pikpak",
    )
    mock_client = AsyncMock()
    mock_client.edit_message = AsyncMock(return_value=MagicMock(id=4321))
    ctx = _make_context(userbot_client=mock_client, source_type="pikpak")

    with (
        patch("app.core.pipeline.engines.pikpak.single.shutil.which", return_value="/usr/bin/ffmpeg"),
        patch.object(
            ZeroDiskFmp4StreamUploader,
            "stream_to_telegram",
            new_callable=AsyncMock,
            return_value=(MagicMock(), 15.1),
        ) as stream_mock,
        patch.object(engine, "_fetch_engine_thumb", new_callable=AsyncMock, return_value=b"thumb"),
    ):
        outcome = await engine.edit_item(item, ctx, 4321)

    assert outcome.message_ids == [4321]
    assert stream_mock.await_args.kwargs["file_name"] == "slot.mp4"
    assert stream_mock.await_args.kwargs["auto_small_file_mode"] is True
    edit_kwargs = mock_client.edit_message.await_args.kwargs
    assert edit_kwargs["text"] == "slot.mts"
    assert edit_kwargs["supports_streaming"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("edit", [False, True])
async def test_pikpak_remux_refreshes_rejected_telegram_handle(edit: bool):
    item = PublishItem(
        item_id="clip",
        name="clip.ts",
        size_bytes=1000,
        video_codec="h264",
        audio_codec="aac",
        duration_s=8.0,
        width=320,
        height=240,
    )
    client = AsyncMock()
    deliver = client.edit_message if edit else client.send_file
    deliver.side_effect = [FilePartsInvalidError(request=None), MagicMock(id=71)]
    ctx = _make_context(userbot_client=client, source_type="pikpak")
    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value=["https://cdn/clip.ts"]))
    handles = [InputFile(id=number, parts=1, name="clip.mp4", md5_checksum="") for number in (1, 2)]

    with (
        patch("app.core.pipeline.engines.pikpak.single.shutil.which", return_value="/usr/bin/ffmpeg"),
        patch.object(engine, "_fetch_engine_thumb", new_callable=AsyncMock, return_value=None),
        patch.object(ZeroDiskFmp4StreamUploader, "stream_to_telegram", autospec=True) as stream,
    ):
        stream.side_effect = [(handle, 8.0) for handle in handles]
        outcome = await (engine.edit_item(item, ctx, 70) if edit else engine.send_item(item, ctx))

    assert outcome.message_ids == [71]
    assert [call.kwargs["file"] if edit else call.args[1] for call in deliver.await_args_list] == handles
    assert len({call.args[0].file_id for call in stream.await_args_list}) == 2


@pytest.mark.asyncio
async def test_local_disk_send_and_edit_ts_auto_remux_and_fallback(tmp_path: Path):
    ts_file = tmp_path / "local_clip.ts"
    ts_file.write_bytes(b"\x47" + b"\x00" * 400)

    mock_userbot = AsyncMock()
    mock_userbot.send_file = AsyncMock(return_value=MagicMock(id=1001))
    mock_userbot.edit_message = AsyncMock(return_value=MagicMock(id=1002))

    registry = MagicMock()
    registry.userbot = mock_userbot
    registry.bots = []
    store = MagicMock()
    store.append_log = AsyncMock()
    store.get_by_id = AsyncMock(return_value=None)

    engine = LocalDiskTransportEngine(registry=registry, store=store)
    ctx = _make_context(userbot_client=mock_userbot, source_type="local", bot_routing_threshold_bytes=1024)
    item = PublishItem(
        item_id=str(ts_file),
        name="local_clip.ts",
        size_bytes=ts_file.stat().st_size,
        video_codec="h264",
        audio_codec="aac",
        duration_s=20.0,
        width=1920,
        height=1080,
        source_type="local",
    )

    with (
        patch("app.core.pipeline.engines.local_disk.shutil.which", return_value="/usr/bin/ffmpeg"),
        patch.object(engine, "_ensure_local_video_metadata", new_callable=AsyncMock),
        patch.object(engine, "_resolve_local_video_thumb", new_callable=AsyncMock, return_value=b"thumb"),
        patch.object(
            ZeroDiskFmp4StreamUploader,
            "stream_to_telegram",
            new_callable=AsyncMock,
            return_value=(MagicMock(), 20.0),
        ) as stream_mock,
    ):
        send_out = await engine.send_item(item, ctx)
        edit_out = await engine.edit_item(item, ctx, 1002)

    assert send_out.message_ids == [1001]
    assert edit_out.message_ids == [1002]
    assert stream_mock.await_count == 2
    assert stream_mock.await_args.kwargs["auto_small_file_mode"] is True
    assert mock_userbot.send_file.await_args.kwargs["mime_type"] == "video/mp4"
    assert mock_userbot.send_file.await_args.kwargs["caption"] == "local_clip.ts"
    assert mock_userbot.edit_message.await_args.kwargs["supports_streaming"] is True

    # Missing ffmpeg falls back cleanly to raw .ts upload
    with (
        patch("app.core.pipeline.engines.local_disk.shutil.which", return_value=None),
        patch.object(engine, "_ensure_local_video_metadata", new_callable=AsyncMock),
        patch.object(engine, "_resolve_local_video_thumb", new_callable=AsyncMock, return_value=None),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()),
    ):
        fb_out = await engine.send_item(item, ctx)
    assert fb_out.message_ids == [1001]
    assert mock_userbot.send_file.await_args.kwargs["mime_type"] == "video/mp2t"


@pytest.mark.asyncio
@pytest.mark.parametrize("edit", [False, True])
@pytest.mark.parametrize("failure", ["invalid_parts", "invalid_parts_twice", "ambiguous"])
async def test_local_remux_retries_only_confirmed_part_rejections(tmp_path: Path, edit: bool, failure: str):
    ts_file = tmp_path / "clip.ts"
    ts_file.write_bytes(b"\x47" * 564)
    item = PublishItem(
        item_id=str(ts_file),
        name=ts_file.name,
        size_bytes=ts_file.stat().st_size,
        video_codec="h264",
        audio_codec="aac",
        duration_s=8.0,
    )
    client = AsyncMock()
    deliver = client.edit_message if edit else client.send_file
    invalid_parts = FilePartsInvalidError(request=None)
    deliver.side_effect = {
        "invalid_parts": [invalid_parts, MagicMock(id=71)],
        "invalid_parts_twice": [invalid_parts, FilePartsInvalidError(request=None)],
        "ambiguous": [TimeoutError("delivery timed out")],
    }[failure]
    registry = MagicMock(userbot=client, bots=[])
    engine = LocalDiskTransportEngine(registry=registry)
    ctx = _make_context(userbot_client=client, source_type="local", task_id=None)
    uploaded = [InputFile(id=number, parts=1, name="clip.mp4", md5_checksum="") for number in (1, 2)]

    with (
        patch("app.core.pipeline.engines.local_disk.shutil.which", return_value="/usr/bin/ffmpeg"),
        patch.object(engine, "_ensure_local_video_metadata", new_callable=AsyncMock),
        patch.object(engine, "_resolve_local_video_thumb", new_callable=AsyncMock, return_value=None),
        patch.object(ZeroDiskFmp4StreamUploader, "stream_to_telegram", autospec=True) as stream,
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as raw_upload,
    ):
        stream.side_effect = [(handle, 8.0) for handle in uploaded]
        operation = engine.edit_item(item, ctx, 70) if edit else engine.send_item(item, ctx)
        if failure == "invalid_parts":
            assert (await operation).message_ids == [71]
        else:
            with pytest.raises(TransportError):
                await operation

    raw_upload.assert_not_awaited()
    expected_attempts = 1 if failure == "ambiguous" else 2
    assert deliver.await_count == stream.await_count == expected_attempts
    assert len({call.args[0].file_id for call in stream.await_args_list}) == expected_attempts
    handles = [call.kwargs["file"] if edit else call.args[1] for call in deliver.await_args_list]
    assert handles == uploaded[:expected_attempts]


@pytest.mark.asyncio
async def test_local_disk_bot_pool_remux_uses_single_worker(tmp_path: Path):
    ts_file = tmp_path / "bot_clip.ts"
    ts_file.write_bytes(b"\x47" + b"\x00" * 400)

    mock_bot = AsyncMock()
    mock_bot.send_file = AsyncMock(return_value=MagicMock(id=2001))
    registry = MagicMock()
    registry.userbot = MagicMock()
    registry.bots = [mock_bot]
    registry.bot_index = 1
    registry.get_next_available_bot.return_value = mock_bot

    engine = LocalDiskTransportEngine(registry=registry)
    ctx = _make_context(source_type="local", bot_routing_threshold_bytes=10 * 1024 * 1024, upload_concurrency=4)
    item = PublishItem(
        item_id=str(ts_file),
        name="bot_clip.ts",
        size_bytes=ts_file.stat().st_size,
        video_codec="h264",
        audio_codec="aac",
        duration_s=5.0,
        width=640,
        height=480,
        source_type="local",
    )

    seen_workers: list[int] = []
    orig_init = ZeroDiskFmp4StreamUploader.__init__

    def _spy_init(self, client, max_workers=4, queue_size=12, in_flight_per_connection=1, capture_sink=None):
        seen_workers.append(max_workers)
        orig_init(
            self,
            client,
            max_workers=max_workers,
            queue_size=queue_size,
            in_flight_per_connection=in_flight_per_connection,
            capture_sink=capture_sink,
        )

    with (
        patch("app.core.pipeline.engines.local_disk.shutil.which", return_value="/usr/bin/ffmpeg"),
        patch.object(engine, "_ensure_local_video_metadata", new_callable=AsyncMock),
        patch.object(engine, "_resolve_local_video_thumb", new_callable=AsyncMock, return_value=None),
        patch.object(ZeroDiskFmp4StreamUploader, "__init__", _spy_init),
        patch.object(
            ZeroDiskFmp4StreamUploader,
            "stream_to_telegram",
            new_callable=AsyncMock,
            return_value=(MagicMock(), 5.0),
        ),
    ):
        out = await engine.send_item(item, ctx)

    assert out.message_ids == [2001]
    assert seen_workers == [1]


@pytest.mark.asyncio
async def test_facades_propagate_auto_remux_setting_to_publish_context(tmp_path: Path):
    captured: list[PublishContext] = []

    class _FakePipeline:
        def __init__(self, **kwargs):
            pass

        async def execute(self, context: PublishContext):
            captured.append(context)
            return MagicMock(
                published_count=0,
                total_files_count=0,
                failed_files=[],
                skipped_files=[],
                failed_items=[],
                placeholder_cards_sent=0,
                placeholder_overflow_sent=False,
            )

    with (
        patch("app.core.pipeline.PublishPipeline", _FakePipeline),
        patch("app.services.system_settings_service.SystemSettingsService.get_bool", return_value=False),
    ):
        await PublisherService.publish_folder(str(tmp_path), -100900008)
        await PikPakPublisherService.publish_pikpak_files(remote_files=[], target_chat_id=-100900008)

    assert len(captured) == 2
    assert captured[0].auto_remux_mpegts_to_mp4 is False
    assert captured[1].auto_remux_mpegts_to_mp4 is False

    captured.clear()
    with (
        patch("app.core.pipeline.PublishPipeline", _FakePipeline),
        patch("app.services.system_settings_service.SystemSettingsService.get_bool", return_value=True),
    ):
        await PublisherService.publish_folder(str(tmp_path), -100900008)
        await PikPakPublisherService.publish_pikpak_files(remote_files=[], target_chat_id=-100900008)

    assert len(captured) == 2
    assert captured[0].auto_remux_mpegts_to_mp4 is True
    assert captured[1].auto_remux_mpegts_to_mp4 is True


@pytest.fixture(scope="module")
def padded_mpegts_sample(tmp_path_factory: pytest.TempPathFactory) -> Path:
    if shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None:
        pytest.skip("ffmpeg/ffprobe unavailable on PATH")
    source = tmp_path_factory.mktemp("mpegts_remux") / "sample.ts"
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
            "testsrc=size=320x240:rate=25",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=880:sample_rate=44100",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=44100",
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-map",
            "2:a:0",
            "-t",
            "18.0",
            "-c:v",
            "libx264",
            "-g",
            "50",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-b:a",
            "96k",
            "-metadata:s:a:0",
            "language=eng",
            "-metadata:s:a:1",
            "language=jpn",
            "-muxrate",
            "8500000",
            "-f",
            "mpegts",
            str(source),
        ],
        check=True,
        timeout=30,
    )
    return source


def _probe_uploaded_mp4(out_mp4: Path, parts: list[SaveFilePartRequest | SaveBigFilePartRequest]) -> dict:
    out_mp4.write_bytes(b"".join(part.bytes for part in sorted(parts, key=lambda part: part.file_part)))
    probe_out = subprocess.check_output(
        ["ffprobe", "-v", "error", "-print_format", "json", "-show_format", "-show_streams", str(out_mp4)],
        text=True,
        timeout=15,
    )
    return json.loads(probe_out)


@pytest.mark.asyncio
async def test_real_ffmpeg_mpegts_full_stream_remux_produces_valid_fmp4(
    tmp_path: Path,
    padded_mpegts_sample: Path,
):
    source_size = (await asyncio.to_thread(padded_mpegts_sample.stat)).st_size
    assert source_size > 16 * 1024 * 1024
    client = AsyncMock(return_value=True)
    uploader = ZeroDiskFmp4StreamUploader(client=client, max_workers=1, queue_size=4)
    res_obj, real_dur = await uploader.stream_to_telegram(
        pikpak_url=str(padded_mpegts_sample),
        start_sec=0.0,
        end_sec=18.0,
        file_name="sample.mp4",
        copy_audio=True,
        full_stream=True,
        force_single_input=True,
        pause_yield=True,
        auto_small_file_mode=True,
        audio_copy_tracks=(0, 1),
    )

    assert isinstance(res_obj, InputFile)
    parts = [call.args[0] for call in client.await_args_list]
    assert len(parts) == res_obj.parts > 0
    assert all(isinstance(part, SaveFilePartRequest) and part.file_id == res_obj.id for part in parts)
    assert real_dur == pytest.approx(18.0, abs=0.2)
    assert uploader.video_width == 320
    assert uploader.video_height == 240
    assert res_obj.name == "sample.mp4"

    parsed = await asyncio.to_thread(_probe_uploaded_mp4, tmp_path / "remuxed.mp4", parts)
    assert [(stream["codec_type"], stream["codec_name"]) for stream in parsed["streams"]] == [
        ("video", "h264"),
        ("audio", "aac"),
        ("audio", "aac"),
    ]
    assert [stream["tags"]["language"] for stream in parsed["streams"] if stream["codec_type"] == "audio"] == [
        "eng",
        "jpn",
    ]
    assert float(parsed["format"]["duration"]) == pytest.approx(18.0, abs=0.2)


@pytest.mark.asyncio
async def test_local_remux_copies_aac_and_transcodes_secondary_ac3(tmp_path: Path, padded_mpegts_sample: Path):
    source = tmp_path / "mixed.ts"
    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(padded_mpegts_sample),
        "-map",
        "0",
        "-c",
        "copy",
        "-c:a:1",
        "ac3",
        "-t",
        "4",
        str(source),
    ]
    await asyncio.to_thread(subprocess.run, command, check=True, timeout=15)
    item = PublishItem(
        item_id=str(source),
        name=source.name,
        size_bytes=(await asyncio.to_thread(source.stat)).st_size,
        video_codec=" ",
        audio_codec=" ",
    )
    client = AsyncMock(return_value=True)
    client.send_file.return_value = MagicMock(id=71)
    engine = LocalDiskTransportEngine(registry=MagicMock(userbot=client, bots=[]))
    ctx = _make_context(userbot_client=client, source_type="local")
    with patch.object(engine, "_resolve_local_video_thumb", new_callable=AsyncMock, return_value=None):
        outcome = await engine.send_item(item, ctx)
    assert outcome.message_ids == [71]
    assert (item.video_codec, item.audio_codec, item.audio_codecs) == ("h264", "aac", ("aac", "ac3"))
    assert decide_mpegts_remux(item, ffmpeg_available=True, require_known_vcodec=True).audio_copy_tracks == (0,)

    handle = client.send_file.await_args.args[1]
    assert isinstance(handle, InputFile)
    parsed = await asyncio.to_thread(
        _probe_uploaded_mp4, tmp_path / "mixed.mp4", [call.args[0] for call in client.await_args_list]
    )
    assert [track["codec_name"] for track in parsed["streams"] if track["codec_type"] == "audio"] == ["aac", "aac"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["range", "chunked", "close", "truncated", "short_eof", "short_duration"])
async def test_remote_remux_accepts_finite_eof_and_rejects_incomplete_input(
    padded_mpegts_sample: Path,
    mode: str,
):
    payload = await asyncio.to_thread(padded_mpegts_sample.read_bytes)
    cutoff = (len(payload) // 3) // 188 * 188
    requests: list[bytes] = []
    headers = b"Connection: close\r\nContent-Type: video/mp2t\r\n"
    if mode in ("range", "truncated"):
        headers += f"Content-Range: bytes 0-{len(payload) - 1}/{len(payload)}\r\n".encode()
        headers += f"Content-Length: {len(payload)}\r\n".encode()
        body = payload[:cutoff] if mode == "truncated" else payload
        response = (b"HTTP/1.1 206 Partial Content\r\n", headers, b"\r\n", body)
    elif mode == "chunked":
        headers += b"Transfer-Encoding: chunked\r\n"
        response = (b"HTTP/1.1 200 OK\r\n", headers, f"\r\n{len(payload):x}\r\n".encode(), payload, b"\r\n0\r\n\r\n")
    else:
        body = payload[:cutoff] if mode in ("short_eof", "short_duration") else payload
        response = (b"HTTP/1.1 200 OK\r\n", headers, b"\r\n", body)

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            requests.append(await reader.readuntil(b"\r\n\r\n"))
            writer.writelines(response)
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            return
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()

    async with await asyncio.start_server(serve, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = AsyncMock(return_value=True)
        uploader = ZeroDiskFmp4StreamUploader(client=client, max_workers=1)
        operation = uploader.stream_to_telegram(
            pikpak_url=f"http://127.0.0.1:{port}/sample.ts",
            start_sec=0,
            end_sec=18.0,
            file_name="sample.mp4",
            copy_audio=True,
            full_stream=True,
            auto_small_file_mode=True,
            max_part_bytes=2 * 1024 * 1024,
            expected_input_bytes=None if mode == "short_duration" else len(payload),
        )
        if mode in ("truncated", "short_eof"):
            with pytest.raises(ChunkFetchTransientError, match="MPEG-TS HTTP"):
                await asyncio.wait_for(operation, timeout=25)
            client.assert_not_awaited()
        elif mode == "short_duration":
            with pytest.raises(PartTruncatedError):
                await asyncio.wait_for(operation, timeout=25)
            client.assert_not_awaited()
        else:
            handle, duration = await asyncio.wait_for(operation, timeout=25)
            assert isinstance(handle, InputFile)
            assert duration == pytest.approx(18.0, abs=0.2)
        assert len(requests) == 1
        assert b"\r\nrange: bytes=0-\r\n" in requests[0].lower()


@pytest.mark.asyncio
async def test_pikpak_remux_part_oversize_falls_back_to_split_when_enabled():
    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value=["https://cdn/huge.ts"]))
    item = PublishItem(
        item_id="fid_oversize",
        name="huge.ts",
        size_bytes=1900 * 1024 * 1024,
        video_codec="h264",
        audio_codec="aac",
        duration_s=1800.0,
        width=1920,
        height=1080,
        source_type="pikpak",
    )
    ctx = _make_context(
        userbot_client=AsyncMock(),
        source_type="pikpak",
        oversize_policy="split",
        split_strategy="vsse_with_fallback",
        task_limit_bytes=1920 * 1024 * 1024,
    )

    with (
        patch("app.core.pipeline.engines.pikpak.single.shutil.which", return_value="/usr/bin/ffmpeg"),
        patch.object(
            ZeroDiskFmp4StreamUploader,
            "stream_to_telegram",
            new_callable=AsyncMock,
            side_effect=PartOversizeError(1, 1921 * 1024 * 1024, 1920 * 1024 * 1024),
        ),
        patch.object(
            engine,
            "_publish_fmp4_split_item",
            new_callable=AsyncMock,
            return_value=MagicMock(message_ids=[3001, 3002]),
        ) as split_mock,
    ):
        outcome = await engine.send_item(item, ctx)

    assert outcome.message_ids == [3001, 3002]
    assert split_mock.await_args.args[2] == 1920 * 1024 * 1024
    assert split_mock.await_args.kwargs["estimated_output_bytes"] == 1921 * 1024 * 1024


def test_remux_overflow_volume_planner_uses_emitted_size():
    item = PublishItem(item_id="clip", name="clip.ts", size_bytes=1_000_000)
    channel = Fmp4SplitChannel(
        item=item,
        context=_make_context(),
        threshold=1_200_000,
        get_fresh_url=AsyncMock(),
        initial_url="https://cdn/clip.ts",
        ext=".ts",
        copy_audio=False,
        total_duration=60.0,
        keyframe_pts=None,
        thumb_bytes=None,
        estimated_output_bytes=2_300_000,
    )
    plans = channel.plan_volumes()
    assert len(plans) >= 3
    assert plans[-1].end_sec == 60.0
    assert item.size_bytes == 1_000_000


@pytest.mark.asyncio
@pytest.mark.parametrize("size_bytes", [0, -1])
async def test_pikpak_edit_item_rejects_invalid_size_before_loading_source(size_bytes: int):
    link_provider = AsyncMock()
    engine = PikPakStreamTransportEngine(link_provider=link_provider)
    item = PublishItem(
        item_id="fid_zero_size",
        name="empty.ts",
        size_bytes=size_bytes,
        video_codec="h264",
        audio_codec="aac",
        duration_s=10.0,
        width=1280,
        height=720,
        source_type="pikpak",
    )
    client = AsyncMock()
    ctx = _make_context(userbot_client=client, source_type="pikpak")

    with (
        patch.object(ZeroDiskFmp4StreamUploader, "stream_to_telegram", new_callable=AsyncMock) as remux_mock,
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as upload,
        pytest.raises(TransportError, match="文件大小未知或为零"),
    ):
        await engine.edit_item(item, ctx, 555)

    remux_mock.assert_not_awaited()
    upload.assert_not_awaited()
    link_provider.assert_not_awaited()
    client.edit_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_pikpak_mpegts_split_keeps_aac_and_transcodes_secondary_pcm(tmp_path: Path, padded_mpegts_sample: Path):
    source = tmp_path / "split.m2ts"
    await asyncio.to_thread(
        subprocess.run,
        [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(padded_mpegts_sample),
            "-map",
            "0",
            "-c",
            "copy",
            "-c:a:1",
            "pcm_bluray",
            "-ar:a:1",
            "48000",
            "-t",
            "4",
            str(source),
        ],
        check=True,
        timeout=15,
    )
    item = PublishItem(
        item_id="mixed",
        name=source.name,
        size_bytes=(await asyncio.to_thread(source.stat)).st_size,
        source_type="pikpak",
        duration_s=4.0,
    )
    context = _make_context(source_type="pikpak")
    metadata = ProbedMediaMetadata(audio_codec="aac", audio_codecs=("aac", "pcm_bluray"))
    with patch("app.core.pipeline.engines.pikpak.fmp4.probe_remote_video_metadata", return_value=metadata) as probe:
        ext, copy_audio, duration, keyframes = await probe_fmp4_media("https://cdn/clip.m2ts", item, context)
    probe.assert_awaited_once()
    channel = Fmp4SplitChannel(
        item=item,
        context=context,
        threshold=2 * 1024 * 1024,
        get_fresh_url=AsyncMock(return_value=str(source)),
        initial_url=str(source),
        ext=ext,
        copy_audio=copy_audio,
        total_duration=duration,
        keyframe_pts=keyframes,
        thumb_bytes=None,
    )
    plans = channel.plan_volumes(force_parts=2)
    assert len(plans) == 2
    assert channel.get_plan_signature(plans) != compute_fmp4_plan_signature(plans)
    client = AsyncMock(return_value=True)
    client.send_file.return_value = MagicMock(id=71)
    for plan in plans:
        job = VolumeJob(plan.part_index, len(plans), plan, 0, item.size_bytes // len(plans), plan.part_name)
        await channel.stream_part(job, client, context, E2ECaptureSession.inactive(), ProgressReporter(item, context))
        handle = client.send_file.await_args.args[1]
        parts = [entry.args[0] for entry in client.await_args_list if entry.args[0].file_id == handle.id]
        result = await asyncio.to_thread(_probe_uploaded_mp4, tmp_path / handle.name, parts)
        audio_tracks = [entry for entry in result["streams"] if entry["codec_type"] == "audio"]
        assert [(track["codec_name"], track["tags"]["language"]) for track in audio_tracks] == [
            ("aac", "eng"),
            ("aac", "jpn"),
        ]
