"""
Golden Test Suite for ZeroDisk FMP4 Pipe Uploader & Streaming Splitter

1. Test 1: Standard Web MP4 (H.264 + AAC, faststart)
2. Test 2: Rear moov + Open GOP (with B-frames, strict A/V PTS offset <= 0.05s)
3. Test 3: Non-standard multi-track MKV (filtering/transcoding to AAC with keyframe alignment & -noaccurate_seek)
4. Test 4: Extreme volume boundary (exact 10 * 512KB = 5,242,880 bytes, Lookahead contract via real stream_to_telegram)
5. Test 5: Local HTTP Range streaming integration test with aiohttp.web
6. Test 6: PartOversizeError hard-cap fuse test
7. Test 7: PartTruncatedError silent truncation guard test
8. Test 8: Four-duration Box tree synchronization test
"""

import asyncio
import itertools
import json
import os
import struct
import subprocess
import sys
from types import SimpleNamespace

import pytest

from app.core import keyframe_prober
from app.core.cdn_transport import (
    aclose_cdn_session,
    cdn_bulk_timeout,
    cdn_liveness_timeout,
    get_cdn_session,
)
from app.core.fmp4_pipe_uploader import (
    PART_SIZE,
    PartDurationInflatedError,
    PartOversizeError,
    PartTruncatedError,
    ZeroDiskFmp4StreamUploader,
    _Fmp4StreamSession,
    _inject_sidx_and_patch_durations,
)
from app.core.iso_bmff.fragmented import (
    RESERVED_SIDX_SPACE,
    build_sidx_and_free_box,
    patch_all_durations,
    plan_video_fmp4_parts,
)
from app.core.keyframe_prober import (
    MkvCuesProber,
    Mp4StssProber,
    _async_fetch_range,
    _async_get_content_length,
    _read_range_body,
)


def run_ffprobe_json(file_path: str):
    cmd = ["ffprobe", "-v", "error", "-print_format", "json", "-show_format", "-show_streams", file_path]
    p = subprocess.run(cmd, capture_output=True, text=True)
    assert p.returncode == 0, f"ffprobe failed: {p.stderr}"
    return json.loads(p.stdout)


def get_first_packet_pts(file_path: str, stream_type: str) -> float:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        f"{stream_type}:0",
        "-show_entries",
        "packet=pts_time",
        "-read_intervals",
        "%+#1",
        "-print_format",
        "json",
        file_path,
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    assert p.returncode == 0, f"ffprobe packet failed: {p.stderr}"
    data = json.loads(p.stdout)
    packets = data.get("packets", [])
    assert len(packets) > 0, f"No packets found for stream {stream_type}"
    return float(packets[0]["pts_time"])


@pytest.mark.asyncio
async def test_golden_1_standard_web_mp4(tmp_path):
    """Test 1: Standard Web MP4 (FastStart, H.264 + AAC)"""
    input_file = str(tmp_path / "web_standard.mp4")
    # Generate 3-second H.264 + AAC with +faststart
    cmd = [
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
        "sine=frequency=1000:sample_rate=44100",
        "-t",
        "3.0",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-movflags",
        "+faststart",
        input_file,
    ]
    subprocess.run(cmd, check=True)
    assert os.path.exists(input_file)

    collected_chunks = []

    async def sink(idx, tot, data):
        collected_chunks.append((idx, tot, data))

    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    res, dur = await uploader.stream_to_telegram(
        pikpak_url=input_file,
        start_sec=0.0,
        end_sec=3.0,
        file_name="web_standard.mp4",
        copy_audio=True,
        inspect_sink=sink,
    )

    assert len(collected_chunks) > 0
    assert dur > 0.0

    # Write collected chunks into single output to probe
    out_file = str(tmp_path / "out_test1.mp4")
    with open(out_file, "wb") as f:
        sorted_chunks = sorted(collected_chunks, key=lambda x: x[0])
        for _idx, _tot, chunk in sorted_chunks:
            f.write(chunk)

    probe = run_ffprobe_json(out_file)
    streams = probe.get("streams", [])
    assert len(streams) >= 2
    v_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
    a_stream = next((s for s in streams if s.get("codec_type") == "audio"), None)
    assert v_stream is not None and v_stream.get("codec_name") == "h264"
    assert a_stream is not None and a_stream.get("codec_name") == "aac"
    fmt = probe.get("format", {})
    assert float(fmt.get("duration", 0.0)) > 2.0


@pytest.mark.asyncio
async def test_golden_2_rear_moov_open_gop(tmp_path):
    """Test 2: Rear moov + Open GOP (with B-frames, intermediate cut point, strict A/V offset <= 0.05s)"""
    input_file = str(tmp_path / "rear_moov.mp4")
    # Generate 3-second with B-frames, moov placed at end
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=320x240:rate=50",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=1000:sample_rate=44100",
        "-t",
        "3.0",
        "-c:v",
        "libx264",
        "-bf",
        "2",
        "-g",
        "50",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        input_file,
    ]
    subprocess.run(cmd, check=True)

    collected_chunks = []

    async def sink(idx, tot, data):
        collected_chunks.append((idx, tot, data))

    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    res, dur = await uploader.stream_to_telegram(
        pikpak_url=input_file,
        start_sec=1.5,
        end_sec=3.0,
        file_name="rear_moov.mp4",
        copy_audio=True,
        inspect_sink=sink,
    )

    assert len(collected_chunks) > 0
    out_file = str(tmp_path / "out_test2.mp4")
    with open(out_file, "wb") as f:
        sorted_chunks = sorted(collected_chunks, key=lambda x: x[0])
        for _idx, _tot, chunk in sorted_chunks:
            f.write(chunk)

    probe = run_ffprobe_json(out_file)
    fmt = probe.get("format", {})
    assert float(fmt.get("duration", 0.0)) > 1.0

    # Strict A/V first frame PTS offset assertion
    v_pts = get_first_packet_pts(out_file, "v")
    a_pts = get_first_packet_pts(out_file, "a")
    offset = abs(v_pts - a_pts)
    assert offset <= 0.05, f"A/V PTS offset {offset:.4f}s exceeds 0.05s limit (v_pts={v_pts}, a_pts={a_pts})"


@pytest.mark.asyncio
async def test_golden_3_non_standard_mkv(tmp_path):
    """Test 3: Non-standard multi-track MKV with non-zero seek and content-based AV sync verification."""
    input_file = str(tmp_path / "sample.mkv")
    # Generate MKV with 2.0s GOP and linear chirp AC3 audio: f(t) = 200 + 100t
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=size=320x240:rate=25",
        "-f",
        "lavfi",
        "-i",
        "aevalsrc=0.9*sin(2*PI*(200*t+50*t*t)):s=48000:d=6.0",
        "-t",
        "6.0",
        "-c:v",
        "libx264",
        "-g",
        "50",
        "-keyint_min",
        "50",
        "-sc_threshold",
        "0",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "ac3",
        input_file,
    ]
    subprocess.run(cmd, check=True)

    collected_chunks = []

    async def sink(idx, tot, data):
        collected_chunks.append((idx, tot, data))

    # Transcode audio to AAC on non-zero cutpoint: start_sec=2.0 (aligns with keyframe), end_sec=4.0
    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    res, dur = await uploader.stream_to_telegram(
        pikpak_url=input_file,
        start_sec=2.0,
        end_sec=4.0,
        file_name="sample.mkv",
        copy_audio=False,
        inspect_sink=sink,
    )

    out_file = str(tmp_path / "out_test3.mp4")
    with open(out_file, "wb") as f:
        sorted_chunks = sorted(collected_chunks, key=lambda x: x[0])
        for _idx, _tot, chunk in sorted_chunks:
            f.write(chunk)

    probe = run_ffprobe_json(out_file)
    streams = probe.get("streams", [])
    a_stream = next((s for s in streams if s.get("codec_type") == "audio"), None)
    v_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
    assert a_stream is not None and a_stream.get("codec_name") == "aac"
    assert v_stream is not None and v_stream.get("codec_name") == "h264"

    # 1. Output duration must be near expected 2.0s (not doubled to 4.0s by N-1 bug)
    fmt = probe.get("format", {})
    real_dur = float(fmt.get("duration", 0.0))
    assert 1.7 <= real_dur <= 2.3, f"Duration {real_dur}s out of expected [1.7, 2.3]s"

    # 2. Content-based AV sync verification using decoded audio zero-crossing frequency
    # At start_sec = 2.0s, f(t) = 200 + 100*(2.0) = 400Hz.
    # If seek fell back to 0.0s (N-1), freq would be 200Hz.
    decode_cmd = [
        "ffmpeg",
        "-y",
        "-v",
        "error",
        "-i",
        out_file,
        "-t",
        "0.3",
        "-ac",
        "1",
        "-ar",
        "48000",
        "-c:a",
        "pcm_s16le",
        "-f",
        "s16le",
        "pipe:1",
    ]
    p_audio = subprocess.run(decode_cmd, stdout=subprocess.PIPE, check=True)
    raw_pcm = p_audio.stdout
    samples = struct.unpack(f"<{len(raw_pcm) // 2}h", raw_pcm)
    # Skip initial 1024 samples (AAC encoder MDCT priming delay frame) to prevent near-zero noise oscillations
    valid_samples = samples[1024:] if len(samples) > 1024 else samples
    dur = len(valid_samples) / 48000.0
    crossings = sum(
        1
        for i in range(len(valid_samples) - 1)
        if (valid_samples[i] <= 0 and valid_samples[i + 1] > 0) or (valid_samples[i] >= 0 and valid_samples[i + 1] < 0)
    )
    measured_freq = crossings / (2 * dur)
    t_audio_start = (measured_freq - 200) / 100
    assert abs(t_audio_start - 2.0) <= 0.10, (
        f"Audio content start {t_audio_start:.3f}s deviates too much from expected 2.0s"
    )


@pytest.mark.asyncio
async def test_golden_4_exact_volume_boundary(tmp_path):
    """Test 4: Extreme volume boundary (exact 10 * 512KB = 5,242,880 bytes via real production stream_to_telegram)"""
    target_bytes = 10 * PART_SIZE  # 5,242,880 bytes

    # 1. Generate base fmp4 stream
    raw_mp4 = str(tmp_path / "base.mp4")
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=size=160x120:rate=25",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=1000:sample_rate=44100",
        "-t",
        "1.0",
        "-c:v",
        "libx264",
        "-c:a",
        "aac",
        "-movflags",
        "frag_keyframe+empty_moov+default_base_moof+skip_trailer",
        "-f",
        "mp4",
        raw_mp4,
    ]
    subprocess.run(cmd, check=True)
    with open(raw_mp4, "rb") as f:
        base_data = f.read()

    # Calculate exact free box padding so that base + free + 64KB reserved sidx == exactly 5,242,880 bytes
    needed_pad = target_bytes - RESERVED_SIDX_SPACE - len(base_data)
    assert needed_pad >= 8, f"needed_pad ({needed_pad}) is too small"
    free_box = struct.pack(">I4s", needed_pad, b"free") + b"\x00" * (needed_pad - 8)
    exact_data = base_data + free_box

    exact_file = str(tmp_path / "exact_stream.mp4")
    with open(exact_file, "wb") as f:
        f.write(exact_data)

    collected_chunks = []

    async def sink(idx, tot, data):
        collected_chunks.append((idx, tot, data))

    # Real production uploader executing real stream_to_telegram lookahead logic
    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    py_code = "import sys; f=open(sys.argv[1], 'rb'); sys.stdout.buffer.write(f.read()); f.close()"
    custom_cmd = [sys.executable, "-u", "-c", py_code, exact_file]

    res, dur = await uploader.stream_to_telegram(
        pikpak_url="",
        start_sec=0.0,
        end_sec=1.0,
        file_name="exact_10parts.mp4",
        inspect_sink=sink,
        custom_cmd=custom_cmd,
    )

    assert len(collected_chunks) == 10, f"Expected exactly 10 parts, got {len(collected_chunks)}"
    sorted_chunks = sorted(collected_chunks, key=lambda x: x[0])
    for i in range(9):
        assert sorted_chunks[i][1] == -1, f"Part {i} declared total {sorted_chunks[i][1]}, expected -1"
    assert sorted_chunks[9][1] == 10, f"Part 9 declared total {sorted_chunks[9][1]}, expected 10"
    total_emitted = sum(len(c[2]) for c in sorted_chunks)
    assert total_emitted == target_bytes, f"Emitted bytes {total_emitted} != {target_bytes}"


@pytest.mark.asyncio
async def test_http_range_streaming(tmp_path):
    """Test 5: Integration test with local HTTP Range server (aiohttp.web) covering Range 206 and reconnect"""
    from aiohttp import ClientSession, web

    media_file = str(tmp_path / "stream_source.mp4")
    cmd = [
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
        "sine=frequency=1000:sample_rate=44100",
        "-t",
        "2.0",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-movflags",
        "+faststart",
        media_file,
    ]
    subprocess.run(cmd, check=True)

    file_size = os.path.getsize(media_file)
    reconnect_attempts = 0

    app = web.Application()

    async def handle_video(request):
        return web.FileResponse(media_file)

    async def handle_reconnect_video(request):
        nonlocal reconnect_attempts
        reconnect_attempts += 1
        # On first connection, send partial bytes and abruptly close connection to trigger FFmpeg HTTP reconnect
        if reconnect_attempts == 1:
            resp = web.StreamResponse(
                status=206,
                headers={
                    "Content-Type": "video/mp4",
                    "Accept-Ranges": "bytes",
                    "Content-Range": f"bytes 0-{file_size - 1}/{file_size}",
                    "Content-Length": str(file_size),
                },
            )
            await resp.prepare(request)
            with open(media_file, "rb") as f:
                head_chunk = f.read(min(8192, file_size // 2))
            await resp.write(head_chunk)
            if request.transport:
                request.transport.close()
            return resp
        # Subsequent requests (including Range resume) return FileResponse (HTTP 206)
        return web.FileResponse(media_file)

    app.router.add_get("/video.mp4", handle_video)
    app.router.add_get("/reconnect_video.mp4", handle_reconnect_video)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    url = f"http://127.0.0.1:{port}/video.mp4"
    reconnect_url = f"http://127.0.0.1:{port}/reconnect_video.mp4"

    try:
        # 1. Verify HTTP 206 Range capability
        async with ClientSession() as session, session.get(url, headers={"Range": "bytes=0-0"}) as resp:
            assert resp.status == 206
            assert resp.headers.get("Content-Range", "").startswith("bytes 0-0/")

        # 2. Stream through FFmpeg from local HTTP server with reconnect protection
        collected_chunks = []

        async def sink(idx, tot, chunk):
            collected_chunks.append((idx, tot, chunk))

        uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
        res, dur = await uploader.stream_to_telegram(
            pikpak_url=reconnect_url,
            start_sec=0.0,
            end_sec=2.0,
            file_name="reconnect_out.mp4",
            copy_audio=True,
            inspect_sink=sink,
        )

        assert len(collected_chunks) > 0
        assert dur > 0.0
        assert reconnect_attempts >= 2, f"Expected at least 1 reconnect (>=2 requests), got {reconnect_attempts}"

        out_file = str(tmp_path / "reconnect_probed.mp4")
        with open(out_file, "wb") as f:
            for _idx, _tot, chunk in sorted(collected_chunks, key=lambda x: x[0]):
                f.write(chunk)

        probe = run_ffprobe_json(out_file)
        fmt = probe.get("format", {})
        assert float(fmt.get("duration", 0.0)) > 1.0
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_part_oversize_error_fuse(tmp_path):
    """Test 6: PartOversizeError hard-cap fuse immediately kills FFmpeg"""
    sample_file = str(tmp_path / "oversize_src.mp4")
    cmd = [
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
        "sine=frequency=1000:sample_rate=44100",
        "-t",
        "3.0",
        "-c:v",
        "libx264",
        "-c:a",
        "aac",
        sample_file,
    ]
    subprocess.run(cmd, check=True)

    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    # Set max_part_bytes to 30KB so emitting stream (~110KB) will fuse
    fuse_limit = 30 * 1024
    with pytest.raises(PartOversizeError) as exc_info:
        await uploader.stream_to_telegram(
            pikpak_url=sample_file,
            start_sec=0.0,
            end_sec=3.0,
            file_name="oversize.mp4",
            copy_audio=True,
            max_part_bytes=fuse_limit,
            inspect_sink=lambda i, t, d: asyncio.sleep(0),
        )

    assert exc_info.value.max_part_bytes == fuse_limit
    assert exc_info.value.total_emitted_bytes > fuse_limit


@pytest.mark.asyncio
async def test_part_truncated_error(tmp_path):
    """Test 7: PartTruncatedError triggers when real_duration is shorter than expected"""
    sample_file = str(tmp_path / "trunc_src.mp4")
    # Only 1.0 second duration in file
    cmd = [
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
        "sine=frequency=1000:sample_rate=44100",
        "-t",
        "1.0",
        "-c:v",
        "libx264",
        "-c:a",
        "aac",
        sample_file,
    ]
    subprocess.run(cmd, check=True)

    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    # Expect 10.0 seconds, but file only has 1.0 second
    with pytest.raises(PartTruncatedError) as exc_info:
        await uploader.stream_to_telegram(
            pikpak_url=sample_file,
            start_sec=0.0,
            end_sec=10.0,
            file_name="trunc.mp4",
            copy_audio=True,
            inspect_sink=lambda i, t, d: asyncio.sleep(0),
        )

    assert exc_info.value.real_duration < 5.0
    assert exc_info.value.expected_duration == 10.0


def test_patch_all_durations_box_tree():
    """Test 8: Four-duration Box tree synchronization updates mvhd, tkhd, mdhd, and mehd"""
    # Build synthetic moov box:
    # moov
    #   mvhd (v0, timescale=1000, duration=0)
    #   trak
    #     tkhd (v0, duration=0)
    #     mdia
    #       mdhd (v0, timescale=90000, duration=0)
    #   mvex
    #     mehd (v0, duration=0)
    mvhd = (
        bytearray(struct.pack(">I4sB3sIIIIII", 108, b"mvhd", 0, b"\x00" * 3, 0, 0, 1000, 0, 0x00010000, 0x0100))
        + b"\x00" * 72
    )
    tkhd = bytearray(struct.pack(">I4sB3sIIIIII", 84, b"tkhd", 0, b"\x00" * 3, 0, 0, 1, 0, 0, 0)) + b"\x00" * 48
    mdhd = bytearray(struct.pack(">I4sB3sIIII", 32, b"mdhd", 0, b"\x00" * 3, 0, 0, 90000, 0)) + b"\x00" * 4
    mdia_body = mdhd
    mdia = struct.pack(">I4s", len(mdia_body) + 8, b"mdia") + mdia_body
    trak_body = tkhd + mdia
    trak = struct.pack(">I4s", len(trak_body) + 8, b"trak") + trak_body
    mehd = bytearray(struct.pack(">I4sB3sI", 16, b"mehd", 0, b"\x00" * 3, 0))
    mvex_body = mehd
    mvex = struct.pack(">I4s", len(mvex_body) + 8, b"mvex") + mvex_body
    moov_body = mvhd + trak + mvex
    moov = struct.pack(">I4s", len(moov_body) + 8, b"moov") + moov_body
    ftyp = struct.pack(">I4s4sI", 16, b"ftyp", b"isom", 512)

    buf = bytearray(ftyp + moov)

    # Dynamic offsets for verification
    mvhd_pos = buf.find(b"mvhd") - 4
    tkhd_pos = buf.find(b"tkhd") - 4
    mdhd_pos = buf.find(b"mdhd") - 4
    mehd_pos = buf.find(b"mehd") - 4

    # Patch duration for 12.5 seconds
    patch_all_durations(buf, 12.5)

    # Verify mvhd: timescale 1000 -> ticks 12.5 * 1000 = 12500
    mvhd_ticks = struct.unpack_from(">I", buf, mvhd_pos + 8 + 16)[0]
    assert mvhd_ticks == 12500

    # Verify tkhd: movie timescale 1000 -> ticks 12500
    tkhd_ticks = struct.unpack_from(">I", buf, tkhd_pos + 8 + 20)[0]
    assert tkhd_ticks == 12500

    # Verify mdhd: track timescale 90000 -> ticks 12.5 * 90000 = 1125000
    mdhd_ticks = struct.unpack_from(">I", buf, mdhd_pos + 8 + 16)[0]
    assert mdhd_ticks == 1125000

    # Verify mehd: movie timescale 1000 -> ticks 12500
    mehd_ticks = struct.unpack_from(">I", buf, mehd_pos + 8 + 4)[0]
    assert mehd_ticks == 12500


@pytest.mark.asyncio
async def test_mkv_cues_prober_timecode_scale_order(tmp_path):
    """Test 9: MkvCuesProber correctly parses Info element with custom TimecodeScale & Duration order."""
    # Synthesize EBML stream with Segment, SeekHead, Info (Duration first, then TimecodeScale=2,000,000 ns)
    # Segment ID: 0x18538067
    # Info ID: 0x1549A966
    # Duration ID: 0x4489, float 4 bytes: 50.0
    # TimecodeScale ID: 0x2AD7B1, uint 2,000,000 ns
    # Expected duration: (50.0 * 2000000) / 1e9 = 0.1s
    duration_elem = struct.pack(">H", 0x4489) + struct.pack("B", 0x84) + struct.pack(">f", 50.0)
    ts_bytes = (2000000).to_bytes(4, "big")
    ts_elem = struct.pack(">I", 0x2AD7B1)[1:] + struct.pack("B", 0x84) + ts_bytes  # 3 byte ID 0x2AD7B1
    info_body = duration_elem + ts_elem
    info_elem = struct.pack(">I", 0x1549A966) + struct.pack("B", 0x80 | len(info_body)) + info_body

    segment_body = info_elem
    segment_elem = struct.pack(">I", 0x18538067) + struct.pack("B", 0x80 | len(segment_body)) + segment_body

    ebml_file = str(tmp_path / "mock.mkv")
    with open(ebml_file, "wb") as f:
        f.write(segment_elem)

    res = await MkvCuesProber.probe_from_url(ebml_file)
    assert res.timescale == 2000000
    assert abs(res.duration_s - 0.1) < 0.001


def test_plan_video_fmp4_parts_monotonicity():
    """Test 10: plan_video_fmp4_parts maintains strict monotonicity and bounds with sparse/irregular cues."""
    # File duration 1000s, 3000MB, threshold 1000MB -> ~4 parts, target ~250s
    # Irregular cues with large gaps
    cues = [10.0, 350.0, 360.0, 950.0]
    plans = plan_video_fmp4_parts(
        total_bytes=3000 * 1024 * 1024,
        threshold_bytes=1000 * 1024 * 1024,
        total_duration_s=1000.0,
        keyframe_pts_s=cues,
        file_name="movie.mkv",
        safety_margin=0.90,
    )
    assert len(plans) >= 2
    for i in range(len(plans)):
        assert plans[i].duration_sec > 0, f"Plan {i} non-positive duration {plans[i].duration_sec}"
        assert plans[i].end_sec > plans[i].start_sec
        if i > 0:
            assert plans[i].start_sec == plans[i - 1].end_sec
    assert plans[0].start_sec == 0.0
    assert plans[-1].end_sec == 1000.0


@pytest.mark.asyncio
async def test_mp4_stss_prober(tmp_path):
    """Test 11: Mp4StssProber extracts keyframes, timescale, and dimensions from FastStart MP4."""
    mp4_file = str(tmp_path / "probe_sample.mp4")
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=640x360:rate=25",
        "-t",
        "4.0",
        "-c:v",
        "libx264",
        "-g",
        "25",
        "-keyint_min",
        "25",
        "-sc_threshold",
        "0",
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        mp4_file,
    ]
    subprocess.run(cmd, check=True)

    res = await Mp4StssProber.probe_from_url(mp4_file)
    assert res.has_cues is True
    assert res.duration_s >= 3.9
    assert res.width == 640
    assert res.height == 360
    assert len(res.keyframe_pts_s) >= 4
    for expected_k in [0.0, 1.0, 2.0, 3.0]:
        assert any(abs(pts - expected_k) <= 0.05 for pts in res.keyframe_pts_s)


@pytest.mark.asyncio
async def test_golden_11_rear_moov_large(tmp_path):
    """Test 11 / NF-10: Mp4StssProber correctly probes >1.5MB Rear-moov MP4 (non-faststart) and plans/slices."""
    large_mp4 = str(tmp_path / "large_rear.mp4")
    # Generate 6.0s noisy MP4 at 3Mbps without faststart (moov placed at end) -> file size ~2.3MB (>1.5MB)
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "nullsrc=size=640x480:rate=25",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=1000:sample_rate=44100",
        "-vf",
        "noise=alls=50:allf=t+u",
        "-t",
        "6.0",
        "-c:v",
        "libx264",
        "-b:v",
        "3M",
        "-maxrate",
        "3M",
        "-bufsize",
        "3M",
        "-g",
        "25",
        "-keyint_min",
        "25",
        "-sc_threshold",
        "0",
        "-c:a",
        "aac",
        large_mp4,
    ]
    subprocess.run(cmd, check=True)
    size = os.path.getsize(large_mp4)
    assert size > 1572864, f"Test file size {size} should exceed 1.5MB to trigger tail scan"

    res = await Mp4StssProber.probe_from_url(large_mp4, file_size=size)
    assert res.has_cues is True
    assert res.duration_s >= 5.9
    assert res.width == 640
    assert res.height == 480
    assert res.audio_codec == "aac"
    assert res.video_codec == "avc1"
    assert len(res.keyframe_pts_s) >= 5
    assert 0.0 in res.keyframe_pts_s
    assert any(abs(pts - 1.0) <= 0.05 for pts in res.keyframe_pts_s)

    # HTTP 双模对照: aiohttp TestServer 按 RFC 7233 返回 206 切片验证一致性
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    with open(large_mp4, "rb") as f:
        file_bytes = f.read()

    app_http = web.Application()

    async def range_handler(request: web.Request) -> web.Response:
        range_header = request.headers.get("Range")
        if not range_header or not range_header.startswith("bytes="):
            return web.Response(body=file_bytes, status=200)
        spec = range_header[6:].strip()
        parts = spec.split("-")
        start = int(parts[0]) if parts[0] else 0
        end = int(parts[1]) if len(parts) > 1 and parts[1] else len(file_bytes) - 1
        end = min(end, len(file_bytes) - 1)
        sliced = file_bytes[start : end + 1]
        headers = {
            "Content-Range": f"bytes {start}-{end}/{len(file_bytes)}",
            "Content-Length": str(len(sliced)),
            "Accept-Ranges": "bytes",
        }
        return web.Response(body=sliced, status=206, headers=headers)

    app_http.router.add_get("/large_rear.mp4", range_handler)
    server_http = TestServer(app_http)
    await server_http.start_server()
    try:
        http_url = f"http://127.0.0.1:{server_http.port}/large_rear.mp4"
        res_http = await Mp4StssProber.probe_from_url(http_url, file_size=size)
        assert res_http.has_cues is True, "HTTP 路径下 >1.5MB 后置 moov 必须成功探测"
        assert res_http.has_cues == res.has_cues
        assert res_http.duration_s == res.duration_s
        assert res_http.keyframe_pts_s == res.keyframe_pts_s
    finally:
        await server_http.close()

    # Verify plan_video_fmp4_parts works with the probed keyframes
    plans = plan_video_fmp4_parts(
        total_bytes=size,
        threshold_bytes=1048576,  # 1MB threshold forces 2 parts
        total_duration_s=res.duration_s,
        keyframe_pts_s=res.keyframe_pts_s,
        file_name="large_rear.mp4",
    )
    assert len(plans) >= 2
    assert plans[0].start_sec == 0.0
    assert plans[1].start_sec == plans[0].end_sec
    assert any(abs(plans[1].start_sec - pts) < 0.01 for pts in res.keyframe_pts_s)

    # Slice part 2 to verify streaming uploader handles the rear-moov file slice
    collected = []

    async def sink(idx, tot, data):
        collected.append((idx, tot, data))

    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    slice_res, actual_dur = await uploader.stream_to_telegram(
        pikpak_url=large_mp4,
        start_sec=plans[1].start_sec,
        end_sec=plans[1].end_sec,
        file_name=plans[1].part_name,
        copy_audio=True,
        inspect_sink=sink,
    )
    assert len(collected) > 0
    assert actual_dur > 0.5


@pytest.mark.asyncio
async def test_mkv_overshoot_seek_accuracy(tmp_path):
    """Test 12: MKV seek overshoot (+0.18s with -noaccurate_seek) targets exact keyframe and avoids GOP regression."""
    mkv_file = str(tmp_path / "overshoot_sample.mkv")
    # GOP of 50 frames (2.0s) at 25fps, total 6.0s -> keyframes at 0.0, 2.0, 4.0
    cmd = [
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
        "sine=frequency=1000:sample_rate=48000",
        "-t",
        "6.0",
        "-c:v",
        "libx264",
        "-g",
        "50",
        "-keyint_min",
        "50",
        "-sc_threshold",
        "0",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        mkv_file,
    ]
    subprocess.run(cmd, check=True)

    collected_chunks = []

    async def sink(idx, tot, data):
        collected_chunks.append((idx, tot, data))

    # Request slice exactly at keyframe 2.0s with duration 2.0s
    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    res, dur = await uploader.stream_to_telegram(
        pikpak_url=mkv_file,
        start_sec=2.0,
        end_sec=4.0,
        file_name="sample.mkv",
        copy_audio=True,
        inspect_sink=sink,
        is_mkv=True,
    )

    out_file = str(tmp_path / "out_overshoot.mp4")
    with open(out_file, "wb") as f:
        for _idx, _tot, chunk in sorted(collected_chunks, key=lambda x: x[0]):
            f.write(chunk)

    probe = run_ffprobe_json(out_file)
    fmt = probe.get("format", {})
    real_dur = float(fmt.get("duration", 0.0))
    assert 1.8 <= real_dur <= 2.25, (
        f"MKV seek failed: real_dur={real_dur}s (expected ~2.0s, N-1 bug would produce ~4.0s)"
    )


@pytest.mark.asyncio
async def test_part_duration_inflated_guard(tmp_path):
    """Test 13: Bidirectional duration check rejects parts whose duration is significantly longer than expected."""
    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    custom_cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=160x120:rate=25",
        "-t",
        "5.0",
        "-c:v",
        "libx264",
        "-movflags",
        "frag_keyframe+empty_moov+default_base_moof+skip_trailer",
        "-f",
        "mp4",
        "pipe:1",
    ]
    with pytest.raises(PartDurationInflatedError) as exc_info:
        await uploader.stream_to_telegram(
            pikpak_url="", start_sec=0.0, end_sec=2.0, file_name="inflated.mp4", custom_cmd=custom_cmd
        )
    assert isinstance(exc_info.value, PartDurationInflatedError)
    assert "inflated" in str(exc_info.value).lower() or exc_info.value.real_duration > 4.0


@pytest.mark.asyncio
async def test_mkv_cues_dynamic_length_reading(tmp_path):
    """Test 14: MkvCuesProber dynamically extends range read for large Cues elements (>64KB)."""
    cue_points = []
    for i in range(10000):
        c_time_val = (i * 1000).to_bytes(4, "big")
        c_time = struct.pack("B", 0xB3) + struct.pack("B", 0x84) + c_time_val
        c_point_body = c_time
        c_point = struct.pack("B", 0xBB) + struct.pack("B", 0x80 | len(c_point_body)) + c_point_body
        cue_points.append(c_point)
    cues_body = b"".join(cue_points)
    assert len(cues_body) > 65536, f"Cues body size {len(cues_body)} should exceed 64KB"

    c_len_vint = (0x200000 | len(cues_body)).to_bytes(3, "big")
    cues_elem = struct.pack(">I", 0x1C53BB6B) + c_len_vint + cues_body

    info_body = (
        struct.pack(">H", 0x4489)
        + struct.pack("B", 0x84)
        + struct.pack(">f", 1500.0)
        + struct.pack(">I", 0x2AD7B1)[1:]
        + struct.pack("B", 0x84)
        + (1000000).to_bytes(4, "big")
    )
    info_elem = struct.pack(">I", 0x1549A966) + struct.pack("B", 0x80 | len(info_body)) + info_body

    seek_cues_body = (
        struct.pack(">H", 0x53AB)
        + struct.pack("B", 0x84)
        + struct.pack(">I", 0x1C53BB6B)
        + struct.pack(">H", 0x53AC)
        + struct.pack("B", 0x84)
        + (0).to_bytes(4, "big")
    )
    seekhead_elem_tmp = (
        struct.pack(">I", 0x114D9B74)
        + struct.pack("B", 0x80 | (len(seek_cues_body) + 3))
        + struct.pack(">H", 0x4DBB)
        + struct.pack("B", 0x80 | len(seek_cues_body))
        + seek_cues_body
    )
    exact_cues_pos = len(seekhead_elem_tmp) + len(info_elem)

    seek_cues_body = (
        struct.pack(">H", 0x53AB)
        + struct.pack("B", 0x84)
        + struct.pack(">I", 0x1C53BB6B)
        + struct.pack(">H", 0x53AC)
        + struct.pack("B", 0x84)
        + exact_cues_pos.to_bytes(4, "big")
    )
    seek_cues_entry = struct.pack(">H", 0x4DBB) + struct.pack("B", 0x80 | len(seek_cues_body)) + seek_cues_body
    seekhead_elem = struct.pack(">I", 0x114D9B74) + struct.pack("B", 0x80 | len(seek_cues_entry)) + seek_cues_entry

    segment_body = seekhead_elem + info_elem + cues_elem
    seg_len_vint = struct.pack(">I", 0x10000000 | len(segment_body))
    segment_elem = struct.pack(">I", 0x18538067) + seg_len_vint + segment_body

    mkv_path = str(tmp_path / "large_cues.mkv")
    with open(mkv_path, "wb") as f:
        f.write(segment_elem)

    res = await MkvCuesProber.probe_from_url(mkv_path)
    assert res.has_cues is True
    assert len(res.keyframe_pts_s) == 10000


@pytest.mark.asyncio
async def test_unaligned_split_gets_noaccurate_seek(monkeypatch):
    """Test 15: Unaligned split & keyframe seeks inject -noaccurate_seek when start_sec > 0; start_sec == 0 strictly omits it."""
    plans = plan_video_fmp4_parts(
        total_bytes=1800,
        threshold_bytes=1000,
        total_duration_s=20.0,
        keyframe_pts_s=[0.0, 19.5],  # No cues in window [2.0, 10.0]
        file_name="unaligned_movie.mp4",
        force_num_parts=2,
    )
    assert len(plans) == 2
    assert plans[1].start_sec == 10.0
    assert plans[0].is_blind is False
    assert plans[1].is_blind is True  # Empty valid_cues triggers fallback to unaligned candidate, marked is_blind=True

    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    captured_cmds = []

    async def fake_create_subprocess_exec(*args, **kwargs):
        captured_cmds.append(list(args))
        raise RuntimeError("Stop after capturing command")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    # 默认命令路径会先跑 ffmpeg 能力预检 (同样走 create_subprocess_exec)——
    # 预置缓存跳过预检, 保证 captured_cmds 只含 4 条流命令
    monkeypatch.setattr("app.core.fmp4_pipe_uploader._FFMPEG_MIN_FRAG_DURATION_OK", True)

    # 1. start_sec > 0.0 with is_blind=True (unaligned fallback)
    with pytest.raises(RuntimeError, match="Stop after capturing command"):
        await uploader.stream_to_telegram(
            pikpak_url="http://example.com/unaligned_movie.mp4",
            start_sec=plans[1].start_sec,
            end_sec=plans[1].end_sec,
            file_name="unaligned_movie.mp4",
            is_blind=plans[1].is_blind,
            is_mkv=False,
        )

    # 2. start_sec > 0.0 with is_blind=False (aligned keyframe cut)
    with pytest.raises(RuntimeError, match="Stop after capturing command"):
        await uploader.stream_to_telegram(
            pikpak_url="http://example.com/unaligned_movie.mp4",
            start_sec=5.0,
            end_sec=10.0,
            file_name="unaligned_movie.mp4",
            is_blind=False,
            is_mkv=False,
        )

    # 3. start_sec == 0.0 (first part)
    with pytest.raises(RuntimeError, match="Stop after capturing command"):
        await uploader.stream_to_telegram(
            pikpak_url="http://example.com/unaligned_movie.mp4",
            start_sec=plans[0].start_sec,
            end_sec=plans[0].end_sec,
            file_name="unaligned_movie.mp4",
            is_blind=plans[0].is_blind,
            is_mkv=False,
        )

    # 4. MKV format with start_sec > 0.0: check 0.18s overshoot and -noaccurate_seek
    with pytest.raises(RuntimeError, match="Stop after capturing command"):
        await uploader.stream_to_telegram(
            pikpak_url="http://example.com/movie.mkv",
            start_sec=4.0,
            end_sec=8.0,
            file_name="movie.mkv",
            is_blind=False,
            is_mkv=True,
        )

    assert len(captured_cmds) == 4

    # Case 1: start_sec > 0.0, is_blind=True
    cmd1 = captured_cmds[0]
    assert "-noaccurate_seek" in cmd1, f"-noaccurate_seek must be in cmd1: {cmd1}"
    assert cmd1.index("-noaccurate_seek") < cmd1.index("-ss"), "-noaccurate_seek must precede -ss in cmd1"

    # Case 2: start_sec > 0.0, is_blind=False -> still unconditionally injected before -ss
    cmd2 = captured_cmds[1]
    assert "-noaccurate_seek" in cmd2, f"-noaccurate_seek must be unconditionally injected when is_blind=False: {cmd2}"
    assert cmd2.index("-noaccurate_seek") < cmd2.index("-ss"), "-noaccurate_seek must precede -ss in cmd2"

    # Case 3: start_sec == 0.0 -> strictly NOT injected
    cmd3 = captured_cmds[2]
    assert "-noaccurate_seek" not in cmd3, f"-noaccurate_seek must strictly NOT be in cmd3 when start_sec == 0: {cmd3}"
    assert "-ss" in cmd3
    ss_idx3 = cmd3.index("-ss")
    assert cmd3[ss_idx3 + 1] == "0.000"

    # Case 4: MKV start_sec > 0.0 -> seek_sec == start_sec + 0.18
    cmd4 = captured_cmds[3]
    assert "-noaccurate_seek" in cmd4
    assert cmd4.index("-noaccurate_seek") < cmd4.index("-ss")
    ss_idx4 = cmd4.index("-ss")
    assert cmd4[ss_idx4 + 1] == f"{4.0 + 0.18:.3f}"


@pytest.mark.asyncio
async def test_blind_cut_duration_inflation_within_gop_allowed():
    """Test 16: Blind cut duration inflation within 1 GOP is allowed (max_dur_allowance=30s), while is_blind=False rejects it."""
    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    custom_cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=160x120:rate=25",
        "-t",
        "5.0",
        "-c:v",
        "libx264",
        "-movflags",
        "frag_keyframe+empty_moov+default_base_moof+skip_trailer",
        "-f",
        "mp4",
        "pipe:1",
    ]

    # When is_blind=False (expected 2.0s), max_allowed is 2.0 + 2.0 = 4.0s. 5.0s > 4.0s triggers PartDurationInflatedError
    with pytest.raises(PartDurationInflatedError) as exc_info:
        await uploader.stream_to_telegram(
            pikpak_url="",
            start_sec=0.0,
            end_sec=2.0,
            file_name="blind_vs_unaligned.mp4",
            is_blind=False,
            custom_cmd=custom_cmd,
        )
    assert exc_info.value.real_duration > 4.0

    # When is_blind=True (expected 2.0s), max_dur_allowance = 30.0s -> max_allowed is 32.0s. 5.0s <= 32.0s is allowed!
    collected_chunks = []

    async def sink(idx, tot, data):
        collected_chunks.append(data)

    res, dur = await uploader.stream_to_telegram(
        pikpak_url="",
        start_sec=0.0,
        end_sec=2.0,
        file_name="blind_allowed.mp4",
        is_blind=True,
        inspect_sink=sink,
        custom_cmd=custom_cmd,
    )
    assert dur > 4.5
    assert len(collected_chunks) > 0


@pytest.mark.asyncio
async def test_unaligned_part_inherits_relaxed_duration_allowance():
    """Test 17: Unaligned part planned with empty valid_cues gets is_blind=True and is accepted when duration inflates within 30s."""
    plans = plan_video_fmp4_parts(
        total_bytes=1800,
        threshold_bytes=1000,
        total_duration_s=20.0,
        keyframe_pts_s=[0.0, 19.5],  # No cues in window [2.0, 10.0] -> plan[1] is unaligned
        file_name="unaligned_gop.mp4",
        force_num_parts=2,
    )
    unaligned_plan = plans[1]
    assert unaligned_plan.is_blind is True

    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    # Simulate FFmpeg output that was forward-aligned by 3 seconds (real duration 13.0s vs expected 10.0s)
    custom_cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=160x120:rate=25",
        "-t",
        "13.0",
        "-c:v",
        "libx264",
        "-movflags",
        "frag_keyframe+empty_moov+default_base_moof+skip_trailer",
        "-f",
        "mp4",
        "pipe:1",
    ]
    collected = []

    async def sink(idx, tot, data):
        collected.append(data)

    res, dur = await uploader.stream_to_telegram(
        pikpak_url="",
        start_sec=unaligned_plan.start_sec,
        end_sec=unaligned_plan.end_sec,
        file_name="unaligned_gop.mp4",
        is_blind=unaligned_plan.is_blind,
        inspect_sink=sink,
        custom_cmd=custom_cmd,
    )
    assert dur > 12.5
    assert len(collected) > 0


@pytest.mark.asyncio
async def test_large_file_rear_moov_tail_fetch_size(monkeypatch):
    """Test 18 / NF-14: Large files (>500MB) use 4MB initial tail fetch; smaller files use 1MB."""
    import aiohttp

    fetched_ranges = []

    class MockAiohttpResponse:
        def __init__(self, data: bytes, status: int = 206):
            self._data = data
            self.status = status
            self.headers = {"Content-Length": str(len(data))}

        async def read(self) -> bytes:
            return self._data

        @property
        def content(self):
            class _S:
                def __init__(s, data):
                    s._d = data

                async def iter_chunked(s, n):
                    for i in range(0, len(s._d), n):
                        yield s._d[i : i + n]

            return _S(self._data)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

    def mock_get(self, url, headers=None, timeout=None, **kwargs):
        range_header = (headers or {}).get("Range", "")
        if range_header and range_header.startswith("bytes="):
            parts = range_header.replace("bytes=", "").split("-")
            start, end = int(parts[0]), int(parts[1])
            fetched_ranges.append((start, end, end - start + 1))
            if start == 0:
                # ftyp (16 bytes) + mdat extending beyond head
                prefix = (
                    struct.pack(">I4s", 16, b"ftyp")
                    + b"isom"
                    + struct.pack(">I", 0)
                    + struct.pack(">I4s", 0x20000000, b"mdat")
                )
                data = prefix.ljust(end - start + 1, b"\x00")
            else:
                data = b"\x00" * (end - start + 1)
        else:
            data = b""

        return MockAiohttpResponse(data, status=206)

    def mock_head(self, url, timeout=None, **kwargs):
        return MockAiohttpResponse(b"", status=200)

    monkeypatch.setattr(aiohttp.ClientSession, "get", mock_get)
    monkeypatch.setattr(aiohttp.ClientSession, "head", mock_head)

    # 1. File > 500MB (e.g. 600MB)
    fetched_ranges.clear()
    size_large = 600 * 1024 * 1024
    await Mp4StssProber.probe_from_url("http://example.com/large.mp4", file_size=size_large)
    # Head fetch: (0, 131071)
    # Tail fetch: start = 600MB - 4MB = 629145600 - 4194304 = 624951296, size = 4MB (4194304)
    assert len(fetched_ranges) >= 2
    tail_fetch_large = fetched_ranges[1]
    expected_tail_size_large = 4 * 1024 * 1024
    assert tail_fetch_large[2] == expected_tail_size_large, (
        f"Large file tail fetch size was {tail_fetch_large[2]}, expected {expected_tail_size_large}"
    )
    assert tail_fetch_large[0] == size_large - expected_tail_size_large

    # 2. File <= 500MB (e.g. 300MB)
    fetched_ranges.clear()
    size_small = 300 * 1024 * 1024
    await Mp4StssProber.probe_from_url("http://example.com/small.mp4", file_size=size_small)
    assert len(fetched_ranges) >= 2
    tail_fetch_small = fetched_ranges[1]
    expected_tail_size_small = 1048576  # 1MB
    assert tail_fetch_small[2] == expected_tail_size_small, (
        f"Small file tail fetch size was {tail_fetch_small[2]}, expected {expected_tail_size_small}"
    )
    # 3. Exact boundary File == 500MB (500 * 1024 * 1024 bytes -> strictly 1MB initial tail fetch)
    fetched_ranges.clear()
    size_boundary = 500 * 1024 * 1024
    await Mp4StssProber.probe_from_url("http://example.com/boundary.mp4", file_size=size_boundary)
    assert len(fetched_ranges) >= 2
    tail_fetch_boundary = fetched_ranges[1]
    assert tail_fetch_boundary[2] == 1048576, (
        f"Exact 500MB file tail fetch size must strictly be 1MB, got {tail_fetch_boundary[2]}"
    )
    assert tail_fetch_boundary[0] == size_boundary - 1048576

    # 4. Boundary + 1 byte: File == 500MB + 1 (500 * 1024 * 1024 + 1 bytes -> strictly 4MB initial tail fetch)
    fetched_ranges.clear()
    size_boundary_plus1 = 500 * 1024 * 1024 + 1
    await Mp4StssProber.probe_from_url("http://example.com/boundary_plus1.mp4", file_size=size_boundary_plus1)
    assert len(fetched_ranges) >= 2
    tail_fetch_boundary_plus1 = fetched_ranges[1]
    assert tail_fetch_boundary_plus1[2] == 4 * 1024 * 1024, (
        f"500MB+1 file tail fetch size must strictly be 4MB, got {tail_fetch_boundary_plus1[2]}"
    )
    assert tail_fetch_boundary_plus1[0] == size_boundary_plus1 - (4 * 1024 * 1024)

    # 5. Small file <= 128KB (e.g. 100KB -> moov must be in head, tail fetch skipped)
    fetched_ranges.clear()
    size_tiny = 100 * 1024
    await Mp4StssProber.probe_from_url("http://example.com/tiny.mp4", file_size=size_tiny)
    assert len(fetched_ranges) == 1, "Files <= 128KB must only fetch head 128KB and skip tail fetch"


@pytest.mark.asyncio
async def test_fmp4_slice_structured_logging(caplog, monkeypatch):
    """Test 19 / R-5.3: fMP4 slice completion logs structured info with part, duration, bytes, and PTS alignment (both aligned & blind)."""
    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    custom_cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=160x120:rate=25",
        "-t",
        "2.0",
        "-c:v",
        "libx264",
        "-movflags",
        "frag_keyframe+empty_moov+default_base_moof+skip_trailer",
        "-f",
        "mp4",
        "pipe:1",
    ]
    collected = []

    async def sink(idx, tot, data):
        collected.append(data)

    import logging

    # 注意: 不要 monkeypatch "app".propagate —— app.main 被 import 过的进程里
    # dictConfig 已将其置 False, pytest catching_logs 会把捕获 handler 同时挂到
    # 该非传播 logger 上; 运行中翻回 True 会让同一条 record 被 handler 收两次。
    # 1. Aligned cut: is_blind=False, part_index=1 -> v_pts_aligned=True
    with caplog.at_level(logging.INFO, logger="app.core.fmp4_pipe_uploader"):
        res, dur = await uploader.stream_to_telegram(
            pikpak_url="",
            start_sec=0.0,
            end_sec=2.0,
            file_name="logged_part1.mp4",
            part_index=1,
            is_blind=False,
            inspect_sink=sink,
            custom_cmd=custom_cmd,
        )

    log_records = [r.message for r in caplog.records if "fMP4 切片完成" in r.message]
    assert len(log_records) == 1, f"Expected 1 completion log, got {log_records}"
    log_msg1 = log_records[0]
    assert "part=1" in log_msg1
    assert "v_pts_aligned=True" in log_msg1
    assert "bytes=" in log_msg1
    assert "duration=" in log_msg1

    # 2. Unaligned blind cut: is_blind=True, part_index=2 -> v_pts_aligned=False
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="app.core.fmp4_pipe_uploader"):
        res, dur = await uploader.stream_to_telegram(
            pikpak_url="",
            start_sec=0.0,
            end_sec=2.0,
            file_name="logged_part2.mp4",
            part_index=2,
            is_blind=True,
            inspect_sink=sink,
            custom_cmd=custom_cmd,
        )

    log_records2 = [r.message for r in caplog.records if "fMP4 切片完成" in r.message]
    assert len(log_records2) == 1, f"Expected 1 completion log, got {log_records2}"
    log_msg2 = log_records2[0]
    assert "part=2" in log_msg2
    assert "v_pts_aligned=False" in log_msg2
    assert "bytes=" in log_msg2
    assert "duration=" in log_msg2


@pytest.mark.asyncio
async def test_mp4_stss_prober_elst_parsing(tmp_dir):
    """Test 20 / NF-13: Mp4StssProber correctly parses edts/elst empty dwell edits and media_time offsets."""
    # 1. Synthetic test for _compute_keyframe_pts
    # 25 fps, timescale 1000, 100 frames (4 seconds, keyframes every 25 frames -> sample 1, 26, 51, 76)
    stts_entries = [(100, 40)]  # 100 samples * 40 ticks = 4000 ticks = 4.0s
    stss_samples = [1, 26, 51, 76]  # raw dts: 0, 1000, 2000, 3000 ticks

    # Standard without elst
    pts_std = Mp4StssProber._compute_keyframe_pts(stts_entries, stss_samples, timescale=1000)
    assert pts_std == [0.0, 1.0, 2.0, 3.0]

    # elst with empty dwell: 1500ms delay (dur=1500 in movie_timescale 1000, media_time=-1)
    # followed by media segment starting at media_time=0
    elst_dwell = [(1500, -1), (4000, 0)]
    pts_dwell = Mp4StssProber._compute_keyframe_pts(
        stts_entries, stss_samples, timescale=1000, elst_entries=elst_dwell, movie_timescale=1000
    )
    assert pts_dwell == [1.5, 2.5, 3.5, 4.5]

    # elst with media trim: starts at media_time=1000 ticks (1.0s)
    # First keyframe at 0 ticks should be trimmed out; remaining keyframes shift by -1.0s
    elst_trim = [(3000, 1000)]
    pts_trim = Mp4StssProber._compute_keyframe_pts(
        stts_entries, stss_samples, timescale=1000, elst_entries=elst_trim, movie_timescale=1000, first_cts=0
    )
    assert pts_trim == [0.0, 1.0, 2.0]

    # 2. End-to-end integration test with real MP4 generated with itsoffset delay
    td = os.path.join(tmp_dir, "elst")
    os.makedirs(td, exist_ok=True)
    src = os.path.join(td, "src.mp4")
    delayed = os.path.join(td, "delayed.mp4")
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
            "testsrc=duration=4:size=160x120:rate=25",
            "-c:v",
            "libx264",
            "-g",
            "25",
            "-bf",
            "2",
            "-movflags",
            "faststart",
            src,
        ],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-itsoffset",
            "1.5",
            "-i",
            src,
            "-c",
            "copy",
            "-movflags",
            "faststart",
            delayed,
        ],
        check=True,
        capture_output=True,
    )
    probe_cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v",
        "-show_entries",
        "packet=pts_time,flags",
        "-of",
        "csv=p=0",
        delayed,
    ]
    res = subprocess.run(probe_cmd, capture_output=True, text=True)
    kf_ffprobe = [float(l.split(",")[0]) for l in res.stdout.strip().split("\n") if "K" in l]

    res_probe = await Mp4StssProber.probe_from_url(delayed)
    assert res_probe.has_cues is True
    assert res_probe.keyframe_pts_s == kf_ffprobe


@pytest.mark.asyncio
async def test_fmp4_mkv_valid_cues_empty_degradation(tmp_dir):
    """Test 21 / MKV without Cues gracefully falls back to blind cut with -noaccurate_seek."""
    td = os.path.join(tmp_dir, "nocues")
    os.makedirs(td, exist_ok=True)
    mkv_path = os.path.join(td, "nocues.mkv")
    # Create MKV with cues disabled via -reserve_index_space 0
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
            "testsrc=duration=6:size=160x120:rate=25",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:duration=6",
            "-c:v",
            "libx264",
            "-g",
            "25",
            "-c:a",
            "aac",
            "-reserve_index_space",
            "0",
            mkv_path,
        ],
        check=True,
        capture_output=True,
    )

    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    collected = []

    async def sink(idx, tot, data):
        collected.append(data)

    # Execute blind cut slice on part 2 (start_sec=2.0, end_sec=4.0) with is_blind=True
    res, dur = await uploader.stream_to_telegram(
        pikpak_url=mkv_path,
        start_sec=2.0,
        end_sec=4.0,
        file_name="blind_part2.mp4",
        part_index=2,
        is_blind=True,
        copy_audio=False,
        inspect_sink=sink,
    )
    assert res is not None
    assert len(collected) > 0
    assert dur >= 1.5

    # Verify output is a valid playable MP4
    out_part = os.path.join(td, "out_part2.mp4")
    with open(out_part, "wb") as f:
        for c in collected:
            f.write(c)

    info = run_ffprobe_json(out_part)
    v_stream = next(s for s in info["streams"] if s["codec_type"] == "video")
    a_stream = next(s for s in info["streams"] if s["codec_type"] == "audio")
    assert v_stream["codec_name"] == "h264"
    assert a_stream["codec_name"] == "aac"


@pytest.mark.asyncio
async def test_fmp4_avi_container_slicing_and_upload(tmp_dir):
    """Test 22 / Non-probeable container (.avi) executes blind cut with -noaccurate_seek smoothly."""
    td = os.path.join(tmp_dir, "avi")
    os.makedirs(td, exist_ok=True)
    avi_path = os.path.join(td, "sample.avi")
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
            "testsrc=duration=5:size=160x120:rate=25",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:duration=5",
            "-c:v",
            "libx264",
            "-g",
            "25",
            "-c:a",
            "aac",
            avi_path,
        ],
        check=True,
        capture_output=True,
    )

    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    collected = []

    async def sink(idx, tot, data):
        collected.append(data)

    # Slice part 2 (2.0s to 4.0s) as blind cut from AVI container
    res, dur = await uploader.stream_to_telegram(
        pikpak_url=avi_path,
        start_sec=2.0,
        end_sec=4.0,
        file_name="avi_part2.mp4",
        part_index=2,
        is_blind=True,
        copy_audio=False,
        inspect_sink=sink,
    )
    assert res is not None
    assert len(collected) > 0
    assert dur >= 1.5

    out_part = os.path.join(td, "avi_out_part2.mp4")
    with open(out_part, "wb") as f:
        for c in collected:
            f.write(c)

    info = run_ffprobe_json(out_part)
    assert any(s["codec_type"] == "video" for s in info["streams"])
    assert any(s["codec_type"] == "audio" for s in info["streams"])


@pytest.mark.asyncio
async def test_async_fetch_range_local_file(tmp_path):
    """Test _async_fetch_range reads local files with correct offsets and boundary safety."""
    test_file = tmp_path / "sample.bin"
    sample_content = b"0123456789ABCDEF" * 16  # 256 bytes
    test_file.write_bytes(sample_content)

    # Regular range
    slice1 = await _async_fetch_range(str(test_file), 10, 19)
    assert slice1 == sample_content[10:20]

    # Out of range start
    slice_oob = await _async_fetch_range(str(test_file), 300, 400)
    assert slice_oob == b""

    # Non-existent file
    slice_missing = await _async_fetch_range(str(tmp_path / "missing.bin"), 0, 100)
    assert slice_missing == b""


@pytest.mark.asyncio
async def test_async_fetch_range_session_reuse(monkeypatch):
    """Test _async_fetch_range automatically reuses global cdn_transport session across calls."""
    import aiohttp

    calls = []

    class MockResp:
        status = 206

        def __init__(self, length):
            self.length = length

        async def read(self):
            return b"x" * self.length

        @property
        def content(self):
            class _S:
                def __init__(s, length):
                    s._l = length

                async def iter_chunked(s, n):
                    full = b"x" * s._l
                    for i in range(0, len(full), n):
                        yield full[i : i + n]

            return _S(self.length)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    def mock_get(self, url, headers=None, timeout=None, **kwargs):
        calls.append((id(self), url, headers))
        r = headers.get("Range", "")
        parts = r.replace("bytes=", "").split("-")
        expected = int(parts[1]) - int(parts[0]) + 1
        return MockResp(expected)

    monkeypatch.setattr(aiohttp.ClientSession, "get", mock_get)

    data1 = await _async_fetch_range("http://example.com/video.mp4", 0, 99)
    assert len(data1) == 100
    data2 = await _async_fetch_range("http://example.com/video.mp4", 100, 199)
    assert len(data2) == 100

    assert len(calls) == 2
    # 两次调用必须命中同一全局 Session 单例实例
    assert calls[0][0] == calls[1][0]


@pytest.mark.asyncio
async def test_async_get_content_length_head(monkeypatch):
    """Test _async_get_content_length parses Content-Length on HEAD and handles errors."""
    import aiohttp

    head_status = 200
    head_headers = {"Content-Length": "12345678"}

    class MockResp:
        def __init__(self):
            self.status = head_status
            self.headers = head_headers

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    def mock_head(self, url, timeout=None, **kwargs):
        return MockResp()

    monkeypatch.setattr(aiohttp.ClientSession, "head", mock_head)

    # Success 200
    size = await _async_get_content_length("http://example.com/media.mp4")
    assert size == 12345678

    # Non-200 status (e.g. 404)
    head_status = 404
    size_404 = await _async_get_content_length("http://example.com/media.mp4")
    assert size_404 == 0

    # Non-digit Content-Length
    head_status = 200
    head_headers = {"Content-Length": "invalid"}
    size_inv = await _async_get_content_length("http://example.com/media.mp4")
    assert size_inv == 0


@pytest.mark.asyncio
async def test_prober_network_error_graceful_degradation(monkeypatch):
    """Test MkvCuesProber and Mp4StssProber gracefully return has_cues=False on network errors."""
    import aiohttp

    def mock_get_fail(self, url, **kwargs):
        raise aiohttp.ClientConnectionError("Connection refused")

    monkeypatch.setattr(aiohttp.ClientSession, "get", mock_get_fail)

    mkv_res = await MkvCuesProber.probe_from_url("http://example.com/test.mkv")
    assert not mkv_res.has_cues
    assert mkv_res.duration_s == 0.0

    mp4_res = await Mp4StssProber.probe_from_url("http://example.com/test.mp4", file_size=1000000)
    assert not mp4_res.has_cues
    assert mp4_res.duration_s == 0.0


@pytest.mark.asyncio
async def test_cdn_bulk_timeout_semantics():
    """定向用例: 锁死 cdn_bulk_timeout 的 Socket 级流式超时语义 (total is None)."""
    t = cdn_bulk_timeout(sock_read=15.0, connect=5.0)
    assert t.total is None, "大块传输必须 total=None 以防大文件 moov 慢速传输被静默中断"
    assert t.sock_read == 15.0
    assert t.sock_connect == 5.0

    tl = cdn_liveness_timeout(total=10.0, connect=5.0)
    assert tl.total == 10.0, "存活探测必须设置硬总预算防止慢速滴流挂死"
    assert tl.sock_connect == 5.0


@pytest.mark.asyncio
async def test_async_fetch_range_206_eof_truncation_allowed():
    """定向用例: 206 响应在服务端 EOF 截断时合法返回前缀数据, 不得误杀 (基于真实分块 TestServer)."""
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    app = web.Application()

    async def handle_eof(request: web.Request) -> web.StreamResponse:
        resp = web.StreamResponse(status=206, headers={"Content-Range": "bytes 0-4/5", "Content-Length": "5"})
        await resp.prepare(request)
        await resp.write(b"short")
        await resp.write_eof()
        return resp

    app.router.add_get("/stream", handle_eof)
    server = TestServer(app)
    await server.start_server()
    try:
        url = f"http://127.0.0.1:{server.port}/stream"
        res = await _async_fetch_range(url, 0, 99)
        assert res == b"short", "EOF 截断下的可用切片数据必须正常返回"
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_async_fetch_range_chunked_reads():
    """P0 核心用例: 验证 _async_fetch_range 能够跨越多个 16KB 网络分块完整累积至 expected (方案 A)."""
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    full_data = b"0123456789ABCDEF" * 8192  # 131072 bytes (128KB)
    app = web.Application()

    async def handle_chunked(request: web.Request) -> web.StreamResponse:
        resp = web.StreamResponse(
            status=206,
            headers={
                "Content-Range": f"bytes 0-{len(full_data) - 1}/{len(full_data)}",
                "Content-Length": str(len(full_data)),
            },
        )
        await resp.prepare(request)
        chunk_size = 16 * 1024
        for i in range(0, len(full_data), chunk_size):
            await resp.write(full_data[i : i + chunk_size])
        await resp.write_eof()
        return resp

    app.router.add_get("/chunked", handle_chunked)
    server = TestServer(app)
    await server.start_server()
    try:
        url = f"http://127.0.0.1:{server.port}/chunked"
        # 请求 100KB (跨越多个 16KB 分块，若是旧 read(n) 则只返回第一块 16KB)
        expected_len = 100 * 1024
        res = await _async_fetch_range(url, 0, expected_len - 1)
        assert len(res) == expected_len, f"预期获取 {expected_len} 字节, 实际获取 {len(res)} 字节"
        assert res == full_data[:expected_len]
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_async_fetch_range_200_chunked_success():
    """验证 HTTP 200 (start=0 且响应体在安全阈值内) 时跨分块正确累积至 expected."""
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    body_data = b"HEADDATA" * 2048  # 16384 bytes
    app = web.Application()

    async def handle_200(request: web.Request) -> web.StreamResponse:
        resp = web.StreamResponse(status=200, headers={"Content-Length": str(len(body_data))})
        await resp.prepare(request)
        for i in range(0, len(body_data), 4096):
            await resp.write(body_data[i : i + 4096])
        await resp.write_eof()
        return resp

    app.router.add_get("/head_sample", handle_200)
    server = TestServer(app)
    await server.start_server()
    try:
        url = f"http://127.0.0.1:{server.port}/head_sample"
        # 请求前 8192 字节
        res = await _async_fetch_range(url, 0, 8191)
        assert len(res) == 8192
        assert res == body_data[:8192]
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_read_range_body_oversend_truncated_to_expected():
    """服务端超发时按 expected 精确截断, 且返回前缀内容正确 (覆盖 del buf[expected:] 原地截断路径)."""

    class MockStream:
        def __init__(self, data: bytes) -> None:
            self._data = data

        async def iter_chunked(self, n: int):
            for i in range(0, len(self._data), n):
                yield self._data[i : i + n]

    payload = bytes(range(256)) * 1200  # 307200 bytes
    expected = 200 * 1024
    res = await _read_range_body(MockStream(payload), expected)
    assert isinstance(res, bytes)
    assert len(res) == expected, f"超发场景应精确截断到 {expected} 字节, 实际 {len(res)}"
    assert res == payload[:expected]


@pytest.mark.asyncio
async def test_read_range_body_eof_short_returns_prefix():
    """服务端在 EOF 处短读时返回可用前缀, 不做补齐或报错."""

    class MockStream:
        def __init__(self, data: bytes) -> None:
            self._data = data

        async def iter_chunked(self, n: int):
            for i in range(0, len(self._data), n):
                yield self._data[i : i + n]

    payload = b"P" * (50 * 1024)
    res = await _read_range_body(MockStream(payload), 200 * 1024)
    assert res == payload, "EOF 短读必须原样返回可用前缀"


@pytest.mark.asyncio
async def test_read_range_body_memory_peak_within_two_x():
    """内存防回归: 累积缓冲 + 返回值, 瞬时峰值应 <= 2.5x expected.

    若退回 bytes(buf[:expected]) 写法, 峰值会升到 3x 触发失败。
    """
    import gc
    import tracemalloc

    class MockStream:
        def __init__(self, data: bytes) -> None:
            self._data = data

        async def iter_chunked(self, n: int):
            for i in range(0, len(self._data), n):
                yield self._data[i : i + n]

    # 预热一次小样本, 避免冷 worker 下首次路径/coverage 追踪污染峰值分配统计
    await _read_range_body(MockStream(b"W" * 4096), 4096)
    gc.collect()

    expected = 4 * 1024 * 1024
    payload = b"Z" * expected
    stream = MockStream(payload)

    tracemalloc.start()
    base = tracemalloc.get_traced_memory()[0]
    tracemalloc.reset_peak()
    try:
        res = await _read_range_body(stream, expected)
        peak = tracemalloc.get_traced_memory()[1] - base
    finally:
        tracemalloc.stop()

    assert len(res) == expected
    ratio = peak / expected
    assert ratio <= 2.5, f"瞬时内存峰值 {peak / 1024 / 1024:.1f}MB 达到 {ratio:.2f}x expected, 疑似出现额外切片副本"


@pytest.mark.asyncio
async def test_async_fetch_range_payload_error_degradation(monkeypatch):
    """定向用例: 传输中断引发 aiohttp ClientPayloadError 时安全降级为 b''."""
    import aiohttp

    class MockFaultStream:
        async def iter_chunked(self, n: int):
            raise aiohttp.ClientPayloadError("Response payload is not completed")
            yield b""

    class MockFaultResp:
        status = 206
        content = MockFaultStream()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr(aiohttp.ClientSession, "get", lambda *a, **k: MockFaultResp())

    res = await _async_fetch_range("http://example.com/video.mp4", 0, 99)
    assert res == b"", "传输中断 ClientPayloadError 必须安全返回 b''"


@pytest.mark.asyncio
async def test_async_fetch_range_200_oom_protection(monkeypatch):
    """定向用例: CDN 忽略 Range 返回 200 及超大 Content-Length 时拦截 OOM."""
    import aiohttp

    class MockOomStream:
        async def iter_chunked(self, n):
            raise AssertionError("严禁在大响应体 200 下读取流数据")
            yield b""

    class MockOomResp:
        status = 200
        content_length = 10 * 1024 * 1024 * 1024  # 10GB 远超 expected * 4
        content = MockOomStream()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr(aiohttp.ClientSession, "get", lambda *a, **k: MockOomResp())

    # 1. start != 0 收到 200 必须直接拒绝
    res_nonzero = await _async_fetch_range("http://example.com/video.mp4", 1000, 2000)
    assert res_nonzero == b""

    # 2. start == 0 且超大响应体必须防 OOM 放弃读取
    res_oom = await _async_fetch_range("http://example.com/video.mp4", 0, 99)
    assert res_oom == b""


@pytest.mark.asyncio
async def test_cdn_session_concurrent_singleton():
    """定向用例: 协程并发获取 CDN session 验证双检锁单例一致性."""
    import asyncio

    try:
        sessions = await asyncio.gather(*[get_cdn_session() for _ in range(10)])
        first_session = sessions[0]
        for s in sessions:
            assert s is first_session, "并发获取必须返回同一 ClientSession 单例"
            assert not s.closed
    finally:
        await aclose_cdn_session()


@pytest.mark.asyncio
async def test_mkv_prober_dual_mode_consistency(tmp_path):
    """结构性用例: MKV 尾部 Cues 在本地文件与 HTTP (EOF 截断) 双模下探测结果必须 100% 一致."""
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    def _build_tail_cues_mkv(cue_count: int = 10, padding_size: int = 1024) -> bytes:
        cue_points = []
        for i in range(cue_count):
            c_time_val = (i * 1000).to_bytes(4, "big")
            c_time = struct.pack("B", 0xB3) + struct.pack("B", 0x84) + c_time_val
            c_point_body = c_time
            c_point = struct.pack("B", 0xBB) + struct.pack("B", 0x80 | len(c_point_body)) + c_point_body
            cue_points.append(c_point)
        cues_body = b"".join(cue_points)

        c_len_vint = (
            struct.pack("B", 0x80 | len(cues_body))
            if len(cues_body) < 128
            else (0x4000 | len(cues_body)).to_bytes(2, "big")
        )
        cues_elem = struct.pack(">I", 0x1C53BB6B) + c_len_vint + cues_body

        info_body = (
            struct.pack(">H", 0x4489)
            + struct.pack("B", 0x84)
            + struct.pack(">f", 100.0)
            + struct.pack(">I", 0x2AD7B1)[1:]
            + struct.pack("B", 0x84)
            + (1000000).to_bytes(4, "big")
        )
        info_elem = struct.pack(">I", 0x1549A966) + struct.pack("B", 0x80 | len(info_body)) + info_body

        padding = b"\x00" * padding_size

        seek_cues_body = (
            struct.pack(">H", 0x53AB)
            + struct.pack("B", 0x84)
            + struct.pack(">I", 0x1C53BB6B)
            + struct.pack(">H", 0x53AC)
            + struct.pack("B", 0x84)
            + (0).to_bytes(4, "big")
        )
        seekhead_elem_tmp = (
            struct.pack(">I", 0x114D9B74)
            + struct.pack("B", 0x80 | (len(seek_cues_body) + 3))
            + struct.pack(">H", 0x4DBB)
            + struct.pack("B", 0x80 | len(seek_cues_body))
            + seek_cues_body
        )
        exact_cues_pos = len(seekhead_elem_tmp) + len(info_elem) + len(padding)

        seek_cues_body = (
            struct.pack(">H", 0x53AB)
            + struct.pack("B", 0x84)
            + struct.pack(">I", 0x1C53BB6B)
            + struct.pack(">H", 0x53AC)
            + struct.pack("B", 0x84)
            + exact_cues_pos.to_bytes(4, "big")
        )
        seek_cues_entry = struct.pack(">H", 0x4DBB) + struct.pack("B", 0x80 | len(seek_cues_body)) + seek_cues_body
        seekhead_elem = struct.pack(">I", 0x114D9B74) + struct.pack("B", 0x80 | len(seek_cues_entry)) + seek_cues_entry

        segment_body = seekhead_elem + info_elem + padding + cues_elem
        seg_len_vint = struct.pack(">I", 0x10000000 | len(segment_body))
        segment_elem = struct.pack(">I", 0x18538067) + seg_len_vint + segment_body
        return segment_elem

    mkv_bytes = _build_tail_cues_mkv(cue_count=10)
    assert len(mkv_bytes) < 65536, "验证样本总大小必须小于 64KB 探测窗口以触发 EOF 截断"

    # 1. 本地文件探测
    local_file = tmp_path / "tail_cues.mkv"
    local_file.write_bytes(mkv_bytes)
    res_local = await MkvCuesProber.probe_from_url(str(local_file))

    # 2. HTTP RFC 7233 真实截断服务端
    app = web.Application()

    async def range_handler(request: web.Request) -> web.Response:
        range_header = request.headers.get("Range")
        if not range_header or not range_header.startswith("bytes="):
            return web.Response(body=mkv_bytes, status=200)
        spec = range_header[6:].strip()
        parts = spec.split("-")
        start = int(parts[0]) if parts[0] else 0
        end = int(parts[1]) if len(parts) > 1 and parts[1] else len(mkv_bytes) - 1
        end = min(end, len(mkv_bytes) - 1)
        sliced = mkv_bytes[start : end + 1]
        headers = {
            "Content-Range": f"bytes {start}-{end}/{len(mkv_bytes)}",
            "Content-Length": str(len(sliced)),
            "Accept-Ranges": "bytes",
        }
        return web.Response(body=sliced, status=206, headers=headers)

    app.router.add_get("/stream.mkv", range_handler)
    server = TestServer(app)
    await server.start_server()
    try:
        http_url = f"http://127.0.0.1:{server.port}/stream.mkv"
        res_http = await MkvCuesProber.probe_from_url(http_url)

        # 核心双模一致性断言
        assert res_http.has_cues is True, "HTTP 路径下尾部 Cues 必须成功解析"
        assert res_local.has_cues is True, "本地路径下尾部 Cues 必须成功解析"
        assert res_http.keyframe_pts_s == res_local.keyframe_pts_s, "双模探测关键帧列表必须完全一致"
        assert len(res_http.keyframe_pts_s) == 10
        assert res_http.duration_s == res_local.duration_s
    finally:
        await server.close()


# -----------------------------------------------------------------------------
# E2E 捕获模式 capture_sink (docs/testing/E2E_GUIDE.md §5.1, D3/E1/E2)
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_capture_sink_alongside_real_pipeline_captures_final_bytes():
    """capture_sink 与真实入队/worker 管线并存 (client=None 干跑): 捕获的即最终上传字节。

    - 到达顺序非 0,1,2,... (part 0 sidx patch 后与尾块最后入队) → 定位写语义 == 排序拼接;
    - 除最高 idx 尾块外每块恒为 PART_SIZE; part 0 在捕获集内 (patch 后字节);
    - 与 inspect_sink 干跑双跑字节全等 (同 cmd FFmpeg 确定性)。
    """
    custom_cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=160x120:rate=25",
        "-t",
        "2.0",
        "-c:v",
        "libx264",
        "-movflags",
        "frag_keyframe+empty_moov+default_base_moof+skip_trailer",
        "-f",
        "mp4",
        "pipe:1",
    ]

    def _assemble(chunks):
        buf = bytearray()
        for idx, _tot, data in chunks:
            end = idx * PART_SIZE + len(data)
            if end > len(buf):
                buf.extend(b"\x00" * (end - len(buf)))
            buf[idx * PART_SIZE : end] = data
        return bytes(buf)

    # 1. capture 模式 (client=None: worker 跳过真实 MTProto 发送, 队列/哨兵/收尾全真实)
    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    captured = []

    def cap_sink(idx, tot, data):
        captured.append((idx, tot, data))

    res_obj, dur = await uploader.stream_to_telegram(
        pikpak_url="",
        start_sec=0.0,
        end_sec=2.0,
        file_name="cap_part1.mp4",
        part_index=1,
        capture_sink=cap_sink,
        custom_cmd=custom_cmd,
    )
    assert not uploader._error_event.is_set()
    assert res_obj is not None and dur > 0
    assert 0 in {idx for idx, _, _ in captured}, "part 0 (sidx/时长修补后的最终字节) 必须被捕获"

    max_idx = max(idx for idx, _, _ in captured)
    for idx, _tot, data in captured:
        if idx != max_idx:
            assert len(data) == PART_SIZE, "非尾块必须恒为 512 KiB"
    total_flags = {idx: tot for idx, tot, _ in captured}
    assert total_flags[max_idx] == len({idx for idx, _, _ in captured}), "尾块 total_parts 必须为真实总数"

    capture_bytes = _assemble(captured)

    # 2. inspect 干跑对照 (同 cmd): 字节必须完全一致
    inspect_chunks = []

    async def inspect_sink(idx, tot, data):
        inspect_chunks.append((idx, tot, data))

    uploader2 = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    await uploader2.stream_to_telegram(
        pikpak_url="",
        start_sec=0.0,
        end_sec=2.0,
        file_name="cap_part1.mp4",
        part_index=1,
        inspect_sink=inspect_sink,
        custom_cmd=custom_cmd,
    )
    assert capture_bytes == _assemble(inspect_chunks), "capture 与 inspect 捕获字节必须全等"


@pytest.mark.asyncio
async def test_enqueue_chunk_capture_ignores_sentinel_and_survives_sink_errors():
    """E1/E2: 终止哨兵 None 不触发捕获; sink 异常吞断告警不阻断入队。"""
    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=1, queue_size=8)
    captured = []

    def sink(idx, tot, data):
        captured.append((idx, tot, data))

    uploader._capture_sink = sink
    await uploader._enqueue_chunk((2, 3, b"CC"))
    await uploader._enqueue_chunk((0, -1, b"AAAA"))
    await uploader._enqueue_chunk(None)  # 终止哨兵: 不捕获、不抛

    assert captured == [(2, 3, b"CC"), (0, -1, b"AAAA")]
    assert uploader.chunk_queue.qsize() == 3

    def bad_sink(idx, tot, data):
        raise OSError("disk full simulation")

    uploader2 = ZeroDiskFmp4StreamUploader(client=None, capture_sink=bad_sink)
    await uploader2._enqueue_chunk((0, -1, b"x"))  # 不抛
    assert uploader2.chunk_queue.qsize() == 1


def test_plan_video_fmp4_parts_tiny_threshold_no_zero_division():
    """#1: 极小 threshold_bytes 不应因 safe_threshold 取整归零触发 ZeroDivisionError。"""
    plans = plan_video_fmp4_parts(
        total_bytes=10,
        threshold_bytes=1,
        total_duration_s=100.0,
        keyframe_pts_s=[0.0, 10.0, 20.0, 30.0, 50.0, 80.0],
        file_name="tiny_thresh.mp4",
        safety_margin=0.9,
    )
    assert len(plans) >= 2
    with pytest.raises(ValueError, match="safety_margin"):
        plan_video_fmp4_parts(
            total_bytes=100,
            threshold_bytes=50,
            total_duration_s=20.0,
            keyframe_pts_s=None,
            file_name="bad_margin.mp4",
            safety_margin=0.0,
        )


@pytest.mark.asyncio
async def test_fmp4_concurrent_floodwait_deduplicates_overlap(monkeypatch):
    """#3: 多 worker 并发遇同一波 FloodWait(50s) 时按非重叠时间窗口累加，不应误触 180s 熔断。"""
    from unittest.mock import AsyncMock, MagicMock

    from telethon.errors import FloodWaitError

    orig_sleep = asyncio.sleep
    fake_now = [1000.0]

    async def fast_sleep(delay):
        target = fake_now[0] + max(0.0, delay)
        await orig_sleep(0.02)
        fake_now[0] = max(fake_now[0], target)

    monkeypatch.setattr("app.core.fmp4_pipe_uploader.asyncio.sleep", fast_sleep)
    monkeypatch.setattr("app.core.fmp4_pipe_uploader.time.time", lambda: fake_now[0])

    client = AsyncMock()
    uploader = ZeroDiskFmp4StreamUploader(client=client, max_workers=4, queue_size=16)
    uploader.file_id = 12345
    uploader._worker_senders = {0: MagicMock(), 1: MagicMock(), 2: MagicMock(), 3: MagicMock()}

    worker_hits = {i: 0 for i in range(4)}
    arrived = 0
    all_in_flight = asyncio.Event()

    async def fake_send_part(sender, req):
        nonlocal arrived
        part_idx = req.file_part
        if worker_hits[part_idx] == 0:
            worker_hits[part_idx] += 1
            arrived += 1
            if arrived >= 4:
                all_in_flight.set()
            await asyncio.wait_for(all_in_flight.wait(), timeout=2.0)
            raise FloodWaitError(request=None, capture=50)
        return True

    monkeypatch.setattr(uploader, "_send_part_request", fake_send_part)

    for i in range(4):
        await uploader.chunk_queue.put((i, 4, b"x"))
    for _ in range(4):
        await uploader.chunk_queue.put(None)

    workers = [asyncio.create_task(uploader._upload_worker(i)) for i in range(4)]
    await asyncio.wait_for(asyncio.gather(*workers), timeout=5.0)
    assert not uploader._error_event.is_set()
    assert uploader._total_flood_wait < 60.0, f"并发重叠窗口不应重复累计 4*50s，实际={uploader._total_flood_wait}"


@pytest.mark.asyncio
async def test_fmp4_stream_cleans_up_subprocess_if_prepare_senders_fails(monkeypatch):
    """#4: _prepare_worker_senders 失败时必须终止已拉起的 FFmpeg 子进程并清理 stderr_task。"""
    from unittest.mock import AsyncMock, MagicMock

    class FakeProc:
        def __init__(self):
            self.returncode = None
            self.killed = False
            self.waited = False
            self.stdout = AsyncMock()
            self.stderr = AsyncMock()
            self.stderr.readline = AsyncMock(return_value=b"")

        def kill(self):
            self.killed = True
            self.returncode = -9

        async def wait(self):
            self.waited = True
            return self.returncode

    fake_proc = FakeProc()

    async def fake_exec(*args, **kwargs):
        return fake_proc

    monkeypatch.setattr("app.core.fmp4_pipe_uploader.asyncio.create_subprocess_exec", fake_exec)

    uploader = ZeroDiskFmp4StreamUploader(client=MagicMock(), max_workers=1, queue_size=4)
    monkeypatch.setattr(uploader, "_prepare_worker_senders", AsyncMock(side_effect=RuntimeError("sender init boom")))

    with pytest.raises(RuntimeError, match="sender init boom"):
        await uploader.stream_to_telegram(
            pikpak_url="http://example.com/v.mp4",
            start_sec=0.0,
            end_sec=10.0,
            file_name="fail.mp4",
            custom_cmd=["ffmpeg", "-i", "pipe:0"],
        )
    assert fake_proc.killed is True
    assert fake_proc.waited is True


@pytest.mark.asyncio
async def test_fmp4_send_part_request_rejects_false_return():
    """#15: fMP4 _send_part_request 遇到 Telegram 显式返回 False 时应抛出异常重试。"""
    from unittest.mock import AsyncMock, MagicMock

    uploader = ZeroDiskFmp4StreamUploader(client=AsyncMock(return_value=False), max_workers=1)
    req = MagicMock(file_part=3)
    with pytest.raises(RuntimeError, match="False"):
        await uploader._send_part_request(None, req)


@pytest.mark.asyncio
async def test_fmp4_truncated_moov_triggers_ffmpeg_stderr_classification(monkeypatch):
    """#5: moov/ext_len box body 半截断抛 IncompleteReadError 时，应落入 FFmpeg 退出码/stderr 分类而非裸异常穿透。"""
    from unittest.mock import AsyncMock

    from app.core.stream_chunk_provider import PikPakThrottledError

    moov_header = struct.pack(">I4s", 128, b"moov")
    reader = asyncio.StreamReader()
    reader.feed_data(moov_header + b"partial")
    reader.feed_eof()

    class FakeProc:
        def __init__(self):
            self.returncode = 1
            self.stdout = reader
            self.stderr = AsyncMock()
            self.stderr.readline = AsyncMock(side_effect=[b"HTTP error 429 Too Many Requests\n", b""])

        def kill(self):
            pass

        async def wait(self):
            return self.returncode

    async def fake_exec(*args, **kwargs):
        return FakeProc()

    monkeypatch.setattr("app.core.fmp4_pipe_uploader.asyncio.create_subprocess_exec", fake_exec)

    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=1, queue_size=4)
    with pytest.raises(PikPakThrottledError):
        await uploader.stream_to_telegram(
            pikpak_url="http://example.com/v.mp4",
            start_sec=0.0,
            end_sec=10.0,
            file_name="trunc.mp4",
            inspect_sink=AsyncMock(),
            custom_cmd=["ffmpeg", "-i", "pipe:0"],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "data",
    [
        b"partial",
        struct.pack(">I4s", 1, b"moov"),
        struct.pack(">I4s", 128, b"moov") + b"partial",
        struct.pack(">I4s", 128, b"mdat") + b"partial",
    ],
)
async def test_fmp4_incomplete_box_rejected_even_with_success_exit(monkeypatch, data):
    from unittest.mock import AsyncMock, MagicMock

    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    proc = MagicMock(returncode=0, stdout=reader)
    proc.stderr.readline = AsyncMock(return_value=b"")
    proc.wait = AsyncMock(return_value=0)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", AsyncMock(return_value=proc))
    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=1)
    with pytest.raises(asyncio.IncompleteReadError):
        await uploader.stream_to_telegram(
            pikpak_url="http://example.com/v.mp4",
            start_sec=0,
            end_sec=10,
            file_name="truncated.mp4",
            inspect_sink=AsyncMock(),
            custom_cmd=["ffmpeg"],
        )


@pytest.mark.asyncio
async def test_fmp4_cancellation_during_stderr_drain_propagates(monkeypatch):
    from unittest.mock import AsyncMock, MagicMock

    reader = asyncio.StreamReader()
    reader.feed_eof()
    proc = MagicMock(returncode=0, stdout=reader)

    async def blocked_stderr():
        await asyncio.Future()

    proc.stderr.readline = blocked_stderr
    proc.wait = AsyncMock(return_value=0)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", AsyncMock(return_value=proc))
    real_wait_for = asyncio.wait_for
    draining = asyncio.Event()

    async def observe_wait_for(awaitable, **kwargs):
        if kwargs.get("timeout") == 1.0:
            draining.set()
        return await real_wait_for(awaitable, **kwargs)

    monkeypatch.setattr(asyncio, "wait_for", observe_wait_for)
    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=1)
    task = asyncio.create_task(
        uploader.stream_to_telegram(
            pikpak_url="http://example.com/v.mp4",
            start_sec=0,
            end_sec=10,
            file_name="cancelled.mp4",
            inspect_sink=AsyncMock(),
            custom_cmd=["ffmpeg"],
        )
    )
    try:
        await real_wait_for(draining.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    proc.wait.assert_awaited()
    assert all(worker.done() for worker in uploader._workers)


@pytest.mark.asyncio
async def test_send_part_request_fallback_client_bounded_by_timeout(monkeypatch):
    """#3: sender=None 回退调用 self.client(req) 时同样受 30 秒超时保护。"""
    import app.core.fmp4_pipe_uploader as fmp4_mod

    observed_timeouts = []
    orig_wait_for = fmp4_mod.asyncio.wait_for

    async def spy_wait_for(aw, **kwargs):
        observed_timeouts.append(kwargs.get("timeout"))
        return await orig_wait_for(aw, timeout=0.05)

    monkeypatch.setattr(fmp4_mod.asyncio, "wait_for", spy_wait_for)

    async def hanging_client(_req):
        await asyncio.sleep(5.0)
        return True

    uploader = ZeroDiskFmp4StreamUploader(client=hanging_client, max_workers=1)
    with pytest.raises(TimeoutError):
        await uploader._send_part_request(None, object())
    assert observed_timeouts == [30.0]


@pytest.mark.asyncio
async def test_finalize_workers_aborts_when_cancelled():
    """#4: _finalize_workers 等待收尾期间若 is_cancelled() 置 True, 必须及时抛 CancelledError。"""
    gate = asyncio.Event()
    cancelled = False

    async def slow_worker():
        await gate.wait()

    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=1)
    worker = asyncio.create_task(slow_worker())

    async def trigger_cancel():
        nonlocal cancelled
        await asyncio.sleep(0.05)
        cancelled = True

    canceller = asyncio.create_task(trigger_cancel())
    try:
        with pytest.raises(asyncio.CancelledError):
            await uploader._finalize_workers([worker], is_cancelled=lambda: cancelled)
    finally:
        gate.set()
        await asyncio.gather(worker, canceller, return_exceptions=True)


def test_mp4_stss_prober_per_sample_ctts_and_esds_codec():
    """#5 & #8: Mp4StssProber 保留逐样本 ctts RLE 偏移计算 PTS, 并从 esds 区分 mp4a 内的 MP3/AAC。"""
    import struct

    from app.core.iso_bmff.box_synthesizer import make_box, make_full_box
    from app.core.keyframe_prober import Mp4StssProber

    # #8: timescale=1000, stts=[(10, 100)], stss=[1, 5], ctts=[(1, 200), (9, 200)], elst=[(1000, 200)]
    # 亦测非均一 ctts: sample 1 ctts=200 (PTS=200), sample 5 (DTS=400) ctts=200 -> PTS=600, elst media_time=200 -> [0.0, 0.4]
    pts = Mp4StssProber._compute_keyframe_pts(
        stts_entries=[(10, 100)],
        stss_samples=[1, 5],
        timescale=1000,
        elst_entries=[(1000, 200)],
        movie_timescale=1000,
        ctts_entries=[(1, 200), (4, 200), (5, 50)],
    )
    assert pts == [0.0, 0.4]

    # 端到端构造 moov 验证 _parse_moov_keyframes 读取完整 ctts v1 (含负偏移) 与 mp4a esds=0x6B
    v_stsd = make_full_box(
        b"stsd",
        0,
        0,
        struct.pack(">I", 1) + make_box(b"avc1", struct.pack(">6sH", b"\x00" * 6, 1) + b"\x00" * 70),
    )
    v_stts = make_full_box(b"stts", 0, 0, struct.pack(">III", 1, 10, 100))
    v_stss = make_full_box(b"stss", 0, 0, struct.pack(">III", 2, 1, 5))
    v_ctts = make_full_box(b"ctts", 1, 0, struct.pack(">I", 2) + struct.pack(">IiIi", 1, 200, 9, 400))
    v_stbl = make_box(b"stbl", v_stsd + v_stts + v_stss + v_ctts)
    v_minf = make_box(b"minf", v_stbl)
    v_mdhd = make_full_box(b"mdhd", 0, 0, struct.pack(">IIIIHH", 0, 0, 1000, 1000, 0x55C4, 0))
    v_hdlr = make_full_box(b"hdlr", 0, 0, struct.pack(">I4s12s", 0, b"vide", b"\x00" * 12) + b"V\x00")
    v_trak = make_box(b"trak", make_box(b"mdia", v_mdhd + v_hdlr + v_minf))

    dcd = bytes([0x04, 0x0D, 0x6B, 0x15, 0x00, 0x18, 0x00, 0x00, 0x01, 0xF4, 0x00, 0x00, 0x01, 0xF4, 0x00])
    es_desc = bytes([0x03, 3 + len(dcd), 0x00, 0x01, 0x00]) + dcd
    esds_box = make_full_box(b"esds", 0, 0, es_desc)
    mp4a_box = make_box(
        b"mp4a",
        struct.pack(">6sH", b"\x00" * 6, 1)
        + b"\x00" * 8
        + struct.pack(">HHHH", 2, 16, 0, 0)
        + struct.pack(">I", 48000 << 16)
        + esds_box,
    )
    a_stsd = make_full_box(b"stsd", 0, 0, struct.pack(">I", 1) + mp4a_box)
    a_stbl = make_box(b"stbl", a_stsd)
    a_minf = make_box(b"minf", a_stbl)
    a_mdhd = make_full_box(b"mdhd", 0, 0, struct.pack(">IIIIHH", 0, 0, 1000, 1000, 0x55C4, 0))
    a_hdlr = make_full_box(b"hdlr", 0, 0, struct.pack(">I4s12s", 0, b"soun", b"\x00" * 12) + b"A\x00")
    a_trak = make_box(b"trak", make_box(b"mdia", a_mdhd + a_hdlr + a_minf))

    mvhd = make_full_box(b"mvhd", 0, 0, struct.pack(">IIII", 0, 0, 1000, 1000) + b"\x00" * 80)
    moov = make_box(b"moov", mvhd + v_trak + a_trak)

    res = Mp4StssProber._parse_moov_keyframes(moov)
    assert res.audio_codec == "mp3"
    # Sample 1 PTS = 0+200 = 0.2s, Sample 5 PTS = 400+400 = 0.8s
    assert res.keyframe_pts_s == [0.2, 0.8]


@pytest.mark.asyncio
async def test_reencode_branch_keeps_channel_layout(monkeypatch):
    captured = []

    async def fake_exec(*args, **kwargs):
        captured.append(list(args))
        raise RuntimeError("Stop after capturing command")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr("app.core.fmp4_pipe_uploader._FFMPEG_MIN_FRAG_DURATION_OK", True)

    uploader = ZeroDiskFmp4StreamUploader(client=None)
    with pytest.raises(RuntimeError, match="Stop after capturing command"):
        await uploader.stream_to_telegram(
            pikpak_url="https://cdn.test/x.mp4",
            start_sec=0.0,
            end_sec=10.0,
            file_name="x.mp4",
            copy_audio=False,  # 触发 AAC 重编码分支
        )
    cmd = captured[-1]
    assert "-ac" not in cmd, f"重编码分支不得再降混声道: {cmd}"
    assert cmd[cmd.index("-c:a") + 1] == "aac"
    assert "192k" in cmd


def _box(kind, payload):
    return struct.pack(">I4s", len(payload) + 8, kind) + payload


@pytest.mark.parametrize("kind", [b"mvhd", b"mdhd", b"tkhd", b"mehd"])
@pytest.mark.parametrize("version", [0, 1])
def test_short_duration_box_does_not_overwrite_sibling(kind, version):
    short = _box(kind, bytes([version, 0, 0, 0]))
    sibling = _box(b"free", b"X" * 64)
    payload = short + sibling
    if kind == b"mdhd":
        payload = _box(b"trak", _box(b"mdia", payload))
    elif kind == b"tkhd":
        payload = _box(b"trak", payload)
    elif kind == b"mehd":
        payload = _box(b"mvex", payload)
    original = _box(b"moov", payload)
    actual = bytearray(original)
    patch_all_durations(actual, 10)
    assert actual == original


@pytest.mark.parametrize(
    ("fragments", "reserve"), [([(0x80000010, 90000, True)], 64), ([(0x40000000, 1, True)] * 2, 52)]
)
def test_sidx_overflow_preserves_reserved_free_box(fragments, reserve):
    with pytest.raises(ValueError, match="reference size"):
        build_sidx_and_free_box(fragments, 90000, reserve)
    original = _box(b"free", bytes(reserve - 8))
    actual = bytearray(original)
    _inject_sidx_and_patch_durations(
        actual,
        fragments_meta=fragments,
        video_timescale=90000,
        video_track_id=1,
        sidx_insert_offset=0,
        sidx_reserve_size=reserve,
        has_reserved_sidx=True,
        real_duration=1,
    )
    assert actual == original


def test_sparse_keyframes_do_not_leave_oversized_final_part():
    plans = plan_video_fmp4_parts(3000000, 1000000, 300, [0, 60, 120, 180], "video.mp4")
    assert plans[0].start_sec == 0
    assert plans[-1].end_sec == 300
    assert all(2 <= part.duration_sec <= 90 for part in plans)
    assert all(a.end_sec == b.start_sec for a, b in itertools.pairwise(plans))


def test_rounding_at_cut_window_bounds_returns_feasible_plan():
    plans = plan_video_fmp4_parts(9000000, 1000000, 1001, None, "video.mp4")
    assert len(plans) == 10
    assert plans[0].start_sec == 0
    assert plans[-1].end_sec == 1001
    assert all(2 <= part.duration_sec <= 100.1 + 1e-9 for part in plans)
    assert all(a.end_sec == b.start_sec for a, b in itertools.pairwise(plans))


@pytest.mark.parametrize(
    ("total_bytes", "threshold_bytes", "duration_s", "force_num_parts"),
    [
        (5000, 2000, 5.0, None),  # max_part_dur = 1.8s < 2.0s min-part guard
        (3000000, 1000000, 5.0, None),  # dense short high-bitrate clip
        (3000000, 1000000, 12.0, 8),  # aggressive force_num_parts dynamic retry
    ],
)
def test_infeasible_duration_bound_falls_back_gracefully(total_bytes, threshold_bytes, duration_s, force_num_parts):
    plans = plan_video_fmp4_parts(
        total_bytes,
        threshold_bytes,
        duration_s,
        None,
        "short.mp4",
        force_num_parts=force_num_parts,
    )
    assert len(plans) >= 1
    assert plans[0].start_sec == 0.0
    assert plans[-1].end_sec == duration_s
    assert all(part.duration_sec > 0 for part in plans)
    assert all(a.end_sec == b.start_sec for a, b in itertools.pairwise(plans))


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://alice:p@ss@cdn.example:8443/video", "https://cdn.example:8443/<redacted>"),
        ("https://alice:secret@[::1]:8080/video", "https://[::1]:8080/<redacted>"),
        ("https://:secret@/video", "<redacted-cdn-url>"),
    ],
)
def test_url_label_omits_credentials(url, expected):
    assert keyframe_prober._safe_url_for_log(url) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("moov_size", [2 * 1024 * 1024, 4 * 1024 * 1024 + 2, 6 * 1024 * 1024, 18 * 1024 * 1024])
async def test_tail_discovery_expands_to_limit(monkeypatch, moov_size):
    file_size = 2 * 1024**3
    moov = _box(b"moov", _box(b"mvhd", bytes(moov_size - 16)))
    moov_offset = file_size - len(moov)
    requests = []

    async def fetch(_url, start, end):
        requests.append((start, end))
        rel_start = start - moov_offset
        rel_end = end + 1 - moov_offset
        if rel_end <= 0:
            return b"\x00" * (end - start + 1)
        if rel_start < 0:
            return b"\x00" * (-rel_start) + moov[0:rel_end]
        return moov[rel_start:rel_end]

    monkeypatch.setattr(keyframe_prober, "_async_fetch_range", fetch)
    result = await keyframe_prober._find_moov_in_tail("synthetic", file_size)
    if moov_size <= 16 * 1024 * 1024:
        assert result == moov
    else:
        assert result is None
    assert requests == (
        [(file_size - 4 * 1024**2, file_size - 1)]
        if moov_size < 4 * 1024**2
        else [
            (file_size - 4 * 1024**2, file_size - 1),
            (file_size - 8 * 1024**2, file_size - 4 * 1024**2 - 1),
        ]
        if moov_size < 8 * 1024**2
        else [
            (file_size - 4 * 1024**2, file_size - 1),
            (file_size - 8 * 1024**2, file_size - 4 * 1024**2 - 1),
            (file_size - 16 * 1024**2, file_size - 8 * 1024**2 - 1),
        ]
    )


def _session(reader, cancelled):
    return _Fmp4StreamSession(
        uploader=SimpleNamespace(_error_event=asyncio.Event()),
        proc=None,
        proc_stdout=reader,
        file_name="video.mp4",
        part_index=1,
        max_part_bytes=None,
        progress_callback=None,
        is_cancelled=cancelled,
        is_paused=None,
        inspect_sink=None,
    )


@pytest.mark.asyncio
async def test_stalled_read_observes_cancel():
    session = _session(asyncio.StreamReader(), lambda: True)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(session._read_exact_or_eof(8), timeout=2)


@pytest.mark.asyncio
async def test_stalled_box_body_and_unbounded_read_observe_cancel():
    session_body = _session(asyncio.StreamReader(), lambda: True)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(session_body._passthrough_box_body(b"\x00\x00\x04\x08mdat", 1024), timeout=2)

    session_unbounded = _session(asyncio.StreamReader(), lambda: True)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(session_unbounded._drain_unbounded_box(b"\x00\x00\x00\x00mdat"), timeout=2)


@pytest.mark.asyncio
async def test_retry_keeps_buffered_bytes_and_partial_eof():
    reader = asyncio.StreamReader()
    reader.feed_data(b"ab")
    session = _session(reader, lambda: False)
    task = asyncio.create_task(session._read_exact_or_eof(4))
    await asyncio.sleep(0.6)
    reader.feed_data(b"cd")
    assert await asyncio.wait_for(task, timeout=2) == b"abcd"
    reader.feed_data(b"e")
    reader.feed_eof()
    assert await session._read_exact_or_eof(4, box_boundary=True) is None
    assert session.incomplete_read.partial == b"e"
    session.incomplete_read = None
    assert await session._read_exact_or_eof(4, box_boundary=True) is None
    assert session.incomplete_read is None


@pytest.mark.asyncio
async def test_prepare_ffmpeg_command_full_stream_and_aac_adtstoasc(monkeypatch):
    from app.core import fmp4_pipe_uploader as fmp4_mod

    monkeypatch.setattr(fmp4_mod, "_FFMPEG_MIN_FRAG_DURATION_OK", True)
    cmd, dur, _urls = await fmp4_mod._prepare_ffmpeg_command(
        pikpak_url="https://cdn.example.com/clip.ts",
        start_sec=0.0,
        end_sec=42.0,
        file_name="clip.mp4",
        copy_audio=True,
        custom_cmd=None,
        is_mkv=False,
        audio_input_url=None,
        full_stream=True,
        force_single_input=True,
    )
    assert "-ss" not in cmd
    assert "-t" not in cmd
    assert "-noaccurate_seek" not in cmd
    assert "-xerror" in cmd
    assert "-reconnect_at_eof" not in cmd
    assert "-reconnect_streamed" not in cmd
    assert "0:a?" in cmd
    assert "0:s?" in cmd
    assert cmd.count("-i") == 1
    assert cmd[cmd.index("-i") + 1] == "pipe:0"
    assert "-bsf:a" not in cmd
    assert cmd[cmd.index("-c:a:0") + 1] == "copy"
    assert cmd[cmd.index("-bsf:a:0") + 1] == "aac_adtstoasc"
    assert "-bsf:a:1" not in cmd
    assert dur == 42.0

    # Transcoded audio path must NOT pass -bsf:a aac_adtstoasc and full_stream keeps single input
    cmd2, _, _ = await fmp4_mod._prepare_ffmpeg_command(
        pikpak_url="https://cdn.example.com/clip.ts",
        start_sec=0.0,
        end_sec=0.0,
        file_name="clip.mp4",
        copy_audio=False,
        custom_cmd=None,
        is_mkv=False,
        audio_input_url=None,
        full_stream=True,
        force_single_input=True,
    )
    assert cmd2.count("-i") == 1
    assert "-bsf:a" not in cmd2
    assert "aac" in cmd2


@pytest.mark.asyncio
async def test_prepare_mpegts_split_maps_all_audio_on_dual_inputs(monkeypatch):
    from app.core import fmp4_pipe_uploader as fmp4_mod

    monkeypatch.setattr(fmp4_mod, "_FFMPEG_MIN_FRAG_DURATION_OK", True)
    cmd, duration, _urls = await fmp4_mod._prepare_ffmpeg_command(
        pikpak_url="https://cdn.example.com/clip.m2ts",
        start_sec=2.0,
        end_sec=4.0,
        file_name="clip.part002.mp4",
        copy_audio=False,
        custom_cmd=None,
        is_mkv=False,
        audio_input_url=None,
        audio_copy_tracks=(0,),
    )
    assert duration == 2.0
    assert cmd.count("-i") == 2
    assert "1:a?" in cmd
    assert "1:a:0?" not in cmd
    assert cmd[cmd.index("-c:a:0") + 1] == "copy"
    assert cmd[cmd.index("-bsf:a:0") + 1] == "aac_adtstoasc"
    assert "-bsf:a:1" not in cmd


@pytest.mark.asyncio
async def test_full_http_feeder_stops_after_ffmpeg_failure_and_preserves_reported_errors():
    from app.core.fmp4_pipe_uploader import _finish_full_http_source
    from app.core.stream_chunk_provider import PikPakLinkExpiredError

    waiting = asyncio.create_task(asyncio.Event().wait())
    await asyncio.wait_for(_finish_full_http_source(waiting, 1, lambda: None), 1.0)
    assert waiting.cancelled()

    async def expired():
        raise PikPakLinkExpiredError("410")

    failed = asyncio.create_task(expired())
    await asyncio.wait((failed,))
    with pytest.raises(PikPakLinkExpiredError, match="410"):
        await _finish_full_http_source(failed, 1, lambda: None)


def test_full_stream_progress_uses_estimate_independently_of_safety_limit(monkeypatch):
    from unittest.mock import MagicMock

    from app.core import fmp4_pipe_uploader as fmp4_mod

    clock = iter((1.0, 2.0))
    monkeypatch.setattr(fmp4_mod, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    proc = SimpleNamespace(returncode=None, kill=MagicMock())
    progress = MagicMock()
    mib = 1024 * 1024
    session = _Fmp4StreamSession(
        uploader=SimpleNamespace(last_speed=0.0, max_workers=1),
        proc=proc,
        proc_stdout=MagicMock(spec=asyncio.StreamReader),
        file_name="clip.mp4",
        part_index=1,
        max_part_bytes=10 * mib,
        progress_total_bytes=6 * mib,
        progress_callback=progress,
        is_cancelled=None,
        is_paused=None,
        inspect_sink=None,
    )
    session.update_speed_and_progress(mib)
    session.update_speed_and_progress(mib)
    progress.assert_called_with(2 * mib, 6 * mib, 1.0, 4.0, "clip.mp4", 1)
    with pytest.raises(PartOversizeError):
        session.update_speed_and_progress(9 * mib)
    proc.kill.assert_called_once_with()


@pytest.mark.asyncio
async def test_pause_yield_raises_upload_paused_error():
    from app.core.scheduler import UploadPausedError

    session = _Fmp4StreamSession(
        uploader=SimpleNamespace(_error_event=asyncio.Event()),
        proc=None,
        proc_stdout=asyncio.StreamReader(),
        file_name="clip.mp4",
        part_index=1,
        max_part_bytes=None,
        progress_callback=None,
        is_cancelled=lambda: False,
        is_paused=lambda: True,
        inspect_sink=None,
        pause_yield=True,
    )
    with pytest.raises(UploadPausedError):
        await session._wait_if_paused()


@pytest.mark.asyncio
@pytest.mark.parametrize("num_parts,expect_small", [(1, True), (3, True), (21, False)])
async def test_auto_small_file_mode_switches_between_save_file_part_and_big(num_parts, expect_small):
    from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest
    from telethon.tl.types import InputFile, InputFileBig

    from app.core import fmp4_pipe_uploader as fmp4_mod

    sent_reqs: list[object] = []

    class _DummyClient:
        async def __call__(self, req):
            sent_reqs.append(req)
            return True

    uploader = ZeroDiskFmp4StreamUploader(client=_DummyClient(), max_workers=1, queue_size=32)
    reader = asyncio.StreamReader()
    proc = SimpleNamespace(returncode=0, kill=lambda: None)
    session = _Fmp4StreamSession(
        uploader=uploader,
        proc=proc,
        proc_stdout=reader,
        file_name="clip.mp4",
        part_index=1,
        max_part_bytes=None,
        progress_callback=None,
        is_cancelled=None,
        is_paused=None,
        inspect_sink=None,
        auto_small_file_mode=True,
    )

    workers = [asyncio.create_task(uploader._upload_worker(0))]
    try:
        payload_chunk = b"x" * PART_SIZE
        for _ in range(num_parts - 1):
            session.out_buf.extend(payload_chunk)
            await session.flush_available_chunks()
        session.out_buf.extend(b"tail" * 64)
        total_parts = await session.finalize_tail_chunk()
        assert total_parts == num_parts
        await session.emit_part0_and_tail(total_parts)
        await uploader._finalize_workers(workers)
    finally:
        for w in workers:
            if not w.done():
                w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    assert uploader._use_small_file_parts is expect_small
    assert len(sent_reqs) == num_parts
    if expect_small:
        assert all(isinstance(r, SaveFilePartRequest) for r in sent_reqs)
        handle = fmp4_mod._make_input_file(uploader.file_id, num_parts, "clip.mp4")
        assert isinstance(handle, InputFile)
        assert handle.md5_checksum == ""
    else:
        assert all(isinstance(r, SaveBigFilePartRequest) for r in sent_reqs)
        handle = fmp4_mod._make_input_file_big(uploader.file_id, num_parts, "clip.mp4")
        assert isinstance(handle, InputFileBig)


def test_full_http_source_size_rejects_unknown_total_range_without_expected_bytes():
    from app.core.fmp4_pipe_uploader import _full_http_source_size
    from app.core.stream_chunk_provider import ChunkFetchTransientError

    resp = SimpleNamespace(
        status=206,
        content_type="video/mp2t",
        content_length=1048576,
        headers={
            "Content-Encoding": "identity",
            "Content-Range": "bytes 0-1048575/*",
        },
    )
    for bad_expected in (None, 0, 2097152):
        with pytest.raises(ChunkFetchTransientError):
            _full_http_source_size(resp, bad_expected)

    assert _full_http_source_size(resp, 1048576) == 1048576


@pytest.mark.asyncio
async def test_pipe_full_http_source_disables_trust_env(monkeypatch):
    from unittest.mock import AsyncMock, MagicMock

    import aiohttp

    from app.core.fmp4_pipe_uploader import _pipe_full_http_source

    captured_kwargs: dict = {}

    class _FakeContent:
        async def iter_chunked(self, _size: int):
            yield b"\x47" * 188

    class _FakeResponse:
        def __init__(self):
            self.status = 200
            self.content_type = "video/mp2t"
            self.content_length = 188
            self.headers = {"Content-Encoding": "identity"}
            self.content = _FakeContent()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    class _FakeSession:
        def __init__(self, **kwargs):
            captured_kwargs.update(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def get(self, *_args, **_kwargs):
            return _FakeResponse()

    monkeypatch.setattr(aiohttp, "ClientSession", _FakeSession)
    writer = SimpleNamespace(write=MagicMock(), drain=AsyncMock(), close=MagicMock())

    await _pipe_full_http_source("https://cdn.example.com/sample.ts", writer, expected_bytes=188)

    assert captured_kwargs["trust_env"] is False
    assert captured_kwargs["auto_decompress"] is False
    writer.write.assert_called_once_with(b"\x47" * 188)
    writer.close.assert_called_once_with()
