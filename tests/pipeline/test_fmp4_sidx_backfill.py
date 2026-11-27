"""
Pipeline and Unit Tests for fMP4 sidx Backfill, mvex/mehd Injection,
and Dual -i Stream Separation (WP-5).
"""

import asyncio
import shutil
import struct
import subprocess

import pytest

from app.core.fmp4_pipe_uploader import ZeroDiskFmp4StreamUploader
from app.core.iso_bmff.fragmented import (
    ensure_mvex_mehd_box,
    patch_all_durations,
)
from app.core.pikpak.thumbnail import extract_frame_ffmpeg

# ---------------------------------------------------------------------------
# Helpers for Box Assembly / Inspection
# ---------------------------------------------------------------------------


def _build_box(box_type: bytes, payload: bytes, is_64bit: bool = False) -> bytes:
    if is_64bit:
        size = len(payload) + 16
        return struct.pack(">I4sQ", 1, box_type, size) + payload
    size = len(payload) + 8
    return struct.pack(">I4s", size, box_type) + payload


def _find_child_boxes(parent_bytes: bytes, header_size: int = 8) -> list[tuple[bytes, int, int, bytes]]:
    """Returns list of (type, offset, size, payload) for child boxes."""
    results = []
    pos = header_size
    end = len(parent_bytes)
    while pos + 8 <= end:
        sz, tp = struct.unpack_from(">I4s", parent_bytes, pos)
        if sz == 1:
            if pos + 16 > end:
                break
            h_len = 16
            sz = struct.unpack_from(">Q", parent_bytes, pos + 8)[0]
        else:
            h_len = 8
        if sz < h_len or pos + sz > end:
            break
        results.append((tp, pos, sz, parent_bytes[pos + h_len : pos + sz]))
        pos += sz
    return results


# ---------------------------------------------------------------------------
# 1. ensure_mvex_mehd_box Unit Tests
# ---------------------------------------------------------------------------


def test_ensure_mvex_mehd_box_basic_injection():
    """Verify 16-byte mehd is injected at the start of mvex when missing."""
    mvhd_payload = b"\x00" * 100  # version 0 mvhd placeholder
    mvhd_box = _build_box(b"mvhd", mvhd_payload)

    trex_payload = struct.pack(">BBHIIIII", 0, 0, 0, 1, 1, 1000, 1000, 0)
    trex_box = _build_box(b"trex", trex_payload)

    mvex_box = _build_box(b"mvex", trex_box)
    orig_mvex_len = len(mvex_box)

    orig_moov = _build_box(b"moov", mvhd_box + mvex_box)
    orig_moov_len = len(orig_moov)

    # Execute injection
    new_moov = ensure_mvex_mehd_box(orig_moov)

    assert len(new_moov) == orig_moov_len + 16
    new_moov_sz = struct.unpack_from(">I", new_moov, 0)[0]
    assert new_moov_sz == orig_moov_len + 16

    # Verify children of moov
    children = _find_child_boxes(new_moov)
    child_map = {c[0]: c for c in children}
    assert b"mvex" in child_map

    mvex_info = child_map[b"mvex"]
    assert mvex_info[2] == orig_mvex_len + 16

    # Verify children of mvex
    mvex_children = _find_child_boxes(new_moov[mvex_info[1] : mvex_info[1] + mvex_info[2]])
    assert len(mvex_children) == 2
    assert mvex_children[0][0] == b"mehd"
    assert mvex_children[0][2] == 16
    assert mvex_children[1][0] == b"trex"

    # Verify mehd payload: ver=0, flags=0, duration=0
    mehd_payload = mvex_children[0][3]
    ver, flags, dur = struct.unpack(">B3sI", mehd_payload)
    assert ver == 0
    assert flags == b"\x00\x00\x00"
    assert dur == 0


def test_ensure_mvex_mehd_box_idempotent_when_already_present():
    """Verify that if mehd already exists inside mvex, moov is unchanged."""
    mehd_box = struct.pack(">I4sBBHI", 16, b"mehd", 0, 0, 0, 5000)
    trex_box = _build_box(b"trex", b"\x00" * 24)
    mvex_box = _build_box(b"mvex", mehd_box + trex_box)
    moov_box = _build_box(b"moov", mvex_box)

    result = ensure_mvex_mehd_box(moov_box)
    assert result == moov_box


def test_ensure_mvex_mehd_box_non_fragmented_unmodified():
    """Verify non-fragmented mp4 moov (no mvex) is unchanged."""
    mvhd_box = _build_box(b"mvhd", b"\x00" * 100)
    trak_box = _build_box(b"trak", b"\x00" * 50)
    moov_box = _build_box(b"moov", mvhd_box + trak_box)

    result = ensure_mvex_mehd_box(moov_box)
    assert result == moov_box


def test_ensure_mvex_mehd_box_malformed_inputs_safe():
    """Verify short/malformed byte inputs return safely without crashing."""
    assert ensure_mvex_mehd_box(b"") == b""
    assert ensure_mvex_mehd_box(b"\x00\x00") == b"\x00\x00"
    assert ensure_mvex_mehd_box(b"\x00\x00\x00\x08ftyp") == b"\x00\x00\x00\x08ftyp"
    corrupt_moov = struct.pack(">I4s", 100, b"moov") + b"\x00" * 10
    assert ensure_mvex_mehd_box(corrupt_moov) == corrupt_moov


def test_ensure_mvex_mehd_box_64bit_moov_and_mvex():
    """Verify 64-bit extended box size handling for moov and mvex."""
    trex_box = _build_box(b"trex", b"\x00" * 24)
    mvex_box_64 = _build_box(b"mvex", trex_box, is_64bit=True)
    moov_box_64 = _build_box(b"moov", mvex_box_64, is_64bit=True)

    orig_moov_len = len(moov_box_64)
    orig_mvex_len = len(mvex_box_64)

    result = ensure_mvex_mehd_box(moov_box_64)
    assert len(result) == orig_moov_len + 16

    # Check 64-bit moov size
    assert struct.unpack_from(">I", result, 0)[0] == 1
    new_moov_sz = struct.unpack_from(">Q", result, 8)[0]
    assert new_moov_sz == orig_moov_len + 16

    # Check 64-bit mvex size
    mvex_offset = 16
    assert struct.unpack_from(">I", result, mvex_offset)[0] == 1
    new_mvex_sz = struct.unpack_from(">Q", result, mvex_offset + 8)[0]
    assert new_mvex_sz == orig_mvex_len + 16


# ---------------------------------------------------------------------------
# 2. ensure_mvex_mehd_box + patch_all_durations Integration
# ---------------------------------------------------------------------------


def test_ensure_mehd_and_patch_all_durations_real_stream(tmp_path):
    """
    Generate a real fMP4 with FFmpeg (which naturally lacks mehd),
    inject mehd with ensure_mvex_mehd_box(), and verify patch_all_durations()
    synchronously updates mvhd, tkhd, mdhd, and mehd to exact duration.
    """
    if shutil.which("ffmpeg") is None:
        pytest.skip("ffmpeg not available on PATH")
    raw_mp4 = str(tmp_path / "synthetic_fmp4.mp4")
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
        "sine=frequency=1000:sample_rate=48000",
        "-t",
        "1.5",
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
        data = f.read()

    # Locate ftyp and moov
    pos = 0
    moov_start = -1
    moov_size = -1
    while pos + 8 <= len(data):
        sz, tp = struct.unpack_from(">I4s", data, pos)
        if sz == 1:
            sz = struct.unpack_from(">Q", data, pos + 8)[0]
        if tp == b"moov":
            moov_start = pos
            moov_size = sz
            break
        pos += sz

    assert moov_start >= 0
    raw_moov = data[moov_start : moov_start + moov_size]

    # Before injection: mvex contains no mehd
    mvex_children_before = _find_child_boxes(raw_moov[raw_moov.find(b"mvex") - 4 :])
    assert not any(c[0] == b"mehd" for c in mvex_children_before)

    # Inject mehd
    injected_moov = ensure_mvex_mehd_box(raw_moov)
    assert len(injected_moov) == len(raw_moov) + 16

    # Assemble test buffer
    buf = bytearray(data[:moov_start] + injected_moov + data[moov_start + moov_size :])

    # Target duration to patch
    target_sec = 8.765
    patch_all_durations(buf, target_sec)

    # Inspect all four duration locations in the patched buffer:
    # 1. moov -> mvhd
    mvhd_idx = buf.find(b"mvhd")
    assert mvhd_idx >= 0
    mv_ver = buf[mvhd_idx + 4]
    if mv_ver == 0:
        movie_timescale = struct.unpack_from(">I", buf, mvhd_idx + 16)[0]
        mvhd_duration_ticks = struct.unpack_from(">I", buf, mvhd_idx + 20)[0]
    else:
        movie_timescale = struct.unpack_from(">I", buf, mvhd_idx + 24)[0]
        mvhd_duration_ticks = struct.unpack_from(">Q", buf, mvhd_idx + 28)[0]
    mvhd_duration_sec = mvhd_duration_ticks / movie_timescale

    # 2. moov -> trak -> tkhd
    tkhd_idx = buf.find(b"tkhd")
    assert tkhd_idx >= 0
    tk_ver = buf[tkhd_idx + 4]
    if tk_ver == 0:
        tkhd_duration_ticks = struct.unpack_from(">I", buf, tkhd_idx + 24)[0]
    else:
        tkhd_duration_ticks = struct.unpack_from(">Q", buf, tkhd_idx + 32)[0]
    tkhd_duration_sec = tkhd_duration_ticks / movie_timescale

    # 3. moov -> trak -> mdia -> mdhd
    mdhd_idx = buf.find(b"mdhd")
    assert mdhd_idx >= 0
    md_ver = buf[mdhd_idx + 4]
    if md_ver == 0:
        track_timescale = struct.unpack_from(">I", buf, mdhd_idx + 16)[0]
        mdhd_duration_ticks = struct.unpack_from(">I", buf, mdhd_idx + 20)[0]
    else:
        track_timescale = struct.unpack_from(">I", buf, mdhd_idx + 24)[0]
        mdhd_duration_ticks = struct.unpack_from(">Q", buf, mdhd_idx + 28)[0]
    mdhd_duration_sec = mdhd_duration_ticks / track_timescale

    # 4. moov -> mvex -> mehd
    mvex_idx = buf.find(b"mvex")
    assert mvex_idx >= 0
    mehd_idx = buf.find(b"mehd", mvex_idx)
    assert mehd_idx >= 0
    me_ver = buf[mehd_idx + 4]
    if me_ver == 0:
        mehd_duration_ticks = struct.unpack_from(">I", buf, mehd_idx + 8)[0]
    else:
        mehd_duration_ticks = struct.unpack_from(">Q", buf, mehd_idx + 8)[0]
    mehd_duration_sec = mehd_duration_ticks / movie_timescale

    # Assert all four duration locations match target_sec precisely
    assert abs(mvhd_duration_sec - target_sec) < 0.001, f"mvhd {mvhd_duration_sec} != {target_sec}"
    assert abs(tkhd_duration_sec - target_sec) < 0.001, f"tkhd {tkhd_duration_sec} != {target_sec}"
    assert abs(mdhd_duration_sec - target_sec) < 0.001, f"mdhd {mdhd_duration_sec} != {target_sec}"
    assert abs(mehd_duration_sec - target_sec) < 0.001, f"mehd {mehd_duration_sec} != {target_sec}"


# ---------------------------------------------------------------------------
# 3. Dual -i Stream Separation Unit Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_uploader_dual_i_tuple_syntax(monkeypatch):
    """Verify passing pikpak_url as a tuple constructs dual -i command."""
    captured = []

    async def fake_exec(*args, **kwargs):
        captured.append(list(args))
        raise RuntimeError("Stop after capturing command")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr("app.core.fmp4_pipe_uploader._FFMPEG_MIN_FRAG_DURATION_OK", True)

    uploader = ZeroDiskFmp4StreamUploader(client=None)
    with pytest.raises(RuntimeError, match="Stop after capturing command"):
        await uploader.stream_to_telegram(
            pikpak_url=("http://127.0.0.1:8500/v_tok", "http://127.0.0.1:8500/a_tok"),
            start_sec=1.0,
            end_sec=10.0,
            file_name="movie.mkv",
            copy_audio=False,
        )

    cmd = captured[-1]
    # Check dual inputs
    i_indices = [idx for idx, val in enumerate(cmd) if val == "-i"]
    assert len(i_indices) == 2, f"Expected exactly 2 -i inputs in command: {cmd}"

    # Check input 0 (video)
    in0_url = cmd[i_indices[0] + 1]
    assert in0_url == "http://127.0.0.1:8500/v_tok"
    in0_options = cmd[: i_indices[0]]
    assert "-an" in in0_options
    assert "-sn" in in0_options
    assert "-dn" in in0_options
    assert in0_options[in0_options.index("-probesize") + 1] == "4194304"
    assert in0_options[in0_options.index("-analyzeduration") + 1] == "2000000"

    # Check input 1 (audio)
    in1_url = cmd[i_indices[1] + 1]
    assert in1_url == "http://127.0.0.1:8500/a_tok"
    in1_options = cmd[i_indices[0] + 2 : i_indices[1]]
    assert "-vn" in in1_options
    assert "-sn" in in1_options
    assert "-dn" in in1_options
    assert in1_options[in1_options.index("-probesize") + 1] == "1048576"
    assert in1_options[in1_options.index("-analyzeduration") + 1] == "2000000"

    # Check mapping and audio encoding
    assert "-map" in cmd
    assert cmd[cmd.index("-map") + 1] == "0:v:0"
    assert cmd[cmd.index("-map", cmd.index("-map") + 2) + 1] == "1:a:0?"
    assert "-ac" not in cmd  # No downmix
    assert cmd[cmd.index("-c:a") + 1] == "aac"
    assert "192k" in cmd


@pytest.mark.asyncio
async def test_uploader_dual_i_keyword_param(monkeypatch):
    """Verify passing audio_input_url keyword triggers dual -i command."""
    captured = []

    async def fake_exec(*args, **kwargs):
        captured.append(list(args))
        raise RuntimeError("Stop after capturing command")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr("app.core.fmp4_pipe_uploader._FFMPEG_MIN_FRAG_DURATION_OK", True)

    uploader = ZeroDiskFmp4StreamUploader(client=None)
    with pytest.raises(RuntimeError, match="Stop after capturing command"):
        await uploader.stream_to_telegram(
            pikpak_url="http://127.0.0.1:8500/v_stream",
            start_sec=0.0,
            end_sec=5.0,
            file_name="clip.mp4",
            copy_audio=True,  # With explicit audio_input_url, dual input is used even with copy_audio
            audio_input_url="http://127.0.0.1:8500/a_stream",
        )

    cmd = captured[-1]
    i_indices = [idx for idx, val in enumerate(cmd) if val == "-i"]
    assert len(i_indices) == 2
    assert cmd[i_indices[0] + 1] == "http://127.0.0.1:8500/v_stream"
    assert cmd[i_indices[1] + 1] == "http://127.0.0.1:8500/a_stream"
    assert cmd[cmd.index("-c:a") + 1] == "copy"


@pytest.mark.asyncio
async def test_uploader_dual_i_auto_http_transcode(monkeypatch):
    """Verify single remote HTTP URL automatically activates dual -i when copy_audio=False."""
    captured = []

    async def fake_exec(*args, **kwargs):
        captured.append(list(args))
        raise RuntimeError("Stop after capturing command")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr("app.core.fmp4_pipe_uploader._FFMPEG_MIN_FRAG_DURATION_OK", True)

    uploader = ZeroDiskFmp4StreamUploader(client=None)
    with pytest.raises(RuntimeError, match="Stop after capturing command"):
        await uploader.stream_to_telegram(
            pikpak_url="https://cdn.example.com/source.mkv",
            start_sec=0.0,
            end_sec=10.0,
            file_name="source.mkv",
            copy_audio=False,
        )

    cmd = captured[-1]
    i_indices = [idx for idx, val in enumerate(cmd) if val == "-i"]
    assert len(i_indices) == 2, "Remote HTTP + audio transcode must use dual -i to avoid ping-pong seek"
    assert cmd[i_indices[0] + 1] == "https://cdn.example.com/source.mkv"
    assert cmd[i_indices[1] + 1] == "https://cdn.example.com/source.mkv"


@pytest.mark.asyncio
async def test_uploader_single_i_fallback_for_local_or_copy_audio(monkeypatch):
    """Verify single -i is preserved when copy_audio=True or local file path."""
    captured = []

    async def fake_exec(*args, **kwargs):
        captured.append(list(args))
        raise RuntimeError("Stop after capturing command")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr("app.core.fmp4_pipe_uploader._FFMPEG_MIN_FRAG_DURATION_OK", True)

    uploader = ZeroDiskFmp4StreamUploader(client=None)

    # 1. Local file path
    with pytest.raises(RuntimeError):
        await uploader.stream_to_telegram(
            pikpak_url="D:/media/local.mp4",
            start_sec=0.0,
            end_sec=5.0,
            file_name="local.mp4",
            copy_audio=False,
        )
    cmd1 = captured[-1]
    assert cmd1.count("-i") == 1
    assert "-map" in cmd1 and cmd1[cmd1.index("-map") + 1] == "0:v:0"
    assert cmd1[cmd1.index("-map", cmd1.index("-map") + 2) + 1] == "0:a:0?"

    # 2. Remote HTTP with copy_audio=True
    with pytest.raises(RuntimeError):
        await uploader.stream_to_telegram(
            pikpak_url="https://cdn.example.com/stream.mp4",
            start_sec=0.0,
            end_sec=5.0,
            file_name="stream.mp4",
            copy_audio=True,
        )
    cmd2 = captured[-1]
    assert cmd2.count("-i") == 1


# ---------------------------------------------------------------------------
# 4. Real FFmpeg Dual -i Pipe Stream Execution Test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_uploader_real_ffmpeg_dual_i_pipe_execution(tmp_path):
    """
    Execute a real FFmpeg pipeline with dual -i input pointing to a local file,
    verify that ZeroDiskFmp4StreamUploader properly reads stdout, injects mehd,
    injects sidx, and yields valid playable fMP4 chunks.
    """
    if shutil.which("ffmpeg") is None:
        pytest.skip("ffmpeg not available on PATH")
    # 1. Create source file
    src_mp4 = str(tmp_path / "dual_source.mp4")
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
            "testsrc2=size=160x120:rate=25",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:sample_rate=48000",
            "-t",
            "2.0",
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
            src_mp4,
        ],
        check=True,
    )

    collected_chunks = []

    async def sink(part_idx, total_parts, data):
        collected_chunks.append((part_idx, total_parts, data))

    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    res, dur = await uploader.stream_to_telegram(
        pikpak_url=(src_mp4, src_mp4),
        start_sec=0.0,
        end_sec=2.0,
        file_name="dual_stream.mp4",
        copy_audio=True,
        inspect_sink=sink,
    )

    assert len(collected_chunks) > 0
    assert dur > 1.0

    part_0 = collected_chunks[0][2]
    # Check mehd exists in part 0
    mvex_pos = part_0.find(b"mvex")
    assert mvex_pos >= 0
    mehd_pos = part_0.find(b"mehd", mvex_pos)
    assert mehd_pos >= 0, "Part 0 moov -> mvex must contain mehd box"

    # Check sidx box exists in part 0
    sidx_pos = part_0.find(b"sidx")
    assert sidx_pos >= 0, "Part 0 must contain synthesized sidx box"


# ---------------------------------------------------------------------------
# 5. Remote Thumbnail Extraction Acceleration Test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_extract_frame_ffmpeg_audio_stripping_and_probesize(monkeypatch, cover_jpeg):
    """
    Verify that extract_frame_ffmpeg inserts -an, -sn, -dn, -probesize 1048576,
    and -analyzeduration 1000000 before -i to accelerate thumbnail extraction.
    """
    captured = []

    class _FakeProc:
        returncode = 0

        async def communicate(self):
            return cover_jpeg, b""

        def kill(self):
            pass

        async def wait(self):
            return 0

    async def fake_exec(*args, **kwargs):
        captured.append(list(args))
        return _FakeProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

    res = await extract_frame_ffmpeg("https://cdn.example.com/video.mp4", "file_123", seek_sec=8.5)
    assert res == cover_jpeg

    cmd = captured[-1]
    i_idx = cmd.index("-i")
    pre_i_opts = cmd[:i_idx]

    assert "-an" in pre_i_opts
    assert "-sn" in pre_i_opts
    assert "-dn" in pre_i_opts
    assert pre_i_opts[pre_i_opts.index("-probesize") + 1] == "1048576"
    assert pre_i_opts[pre_i_opts.index("-analyzeduration") + 1] == "1000000"
    assert pre_i_opts[pre_i_opts.index("-ss") + 1] == "8.500"


# ---------------------------------------------------------------------------
# 6. Hardening & Edge Cases Regression Tests
# ---------------------------------------------------------------------------


def test_ensure_mvex_mehd_box_corrupt_mvex_child_returns_unmodified():
    """Verify that if mvex has corrupted child boxes, moov is returned untouched."""
    # Child box claims size 100 but only 12 bytes exist in mvex
    corrupt_child = struct.pack(">I4sI", 100, b"trex", 1)
    mvex_box = _build_box(b"mvex", corrupt_child)
    moov_box = _build_box(b"moov", mvex_box)

    result = ensure_mvex_mehd_box(moov_box)
    assert result == moov_box


def test_patch_all_durations_overflow_safe():
    """Verify patch_all_durations clamps ticks to 0xFFFFFFFF for 32-bit fields without error."""
    mvhd_payload = bytearray(100)
    mvhd_payload[0] = 0  # version 0
    struct.pack_into(">I", mvhd_payload, 12, 90000)  # timescale = 90000
    mvhd_box = _build_box(b"mvhd", bytes(mvhd_payload))

    mehd_box = struct.pack(">I4sBBHI", 16, b"mehd", 0, 0, 0, 0)
    mvex_box = _build_box(b"mvex", mehd_box)
    moov_box = _build_box(b"moov", mvhd_box + mvex_box)

    buf = bytearray(moov_box)
    # Huge duration: 100,000 seconds * 90,000 = 9,000,000,000 (> 0xFFFFFFFF)
    patch_all_durations(buf, 100_000.0)

    # mvhd duration should be clamped to 0xFFFFFFFF
    mvhd_offset = buf.find(b"mvhd")
    dur_val = struct.unpack_from(">I", buf, mvhd_offset + 20)[0]
    assert dur_val == 0xFFFFFFFF

    # mehd duration should also be clamped to 0xFFFFFFFF
    mehd_offset = buf.find(b"mehd")
    mehd_dur = struct.unpack_from(">I", buf, mehd_offset + 8)[0]
    assert mehd_dur == 0xFFFFFFFF


@pytest.mark.asyncio
async def test_uploader_dual_i_rejects_empty_audio_url():
    """Verify ValueError is raised if dual stream mode has an empty audio url."""
    uploader = ZeroDiskFmp4StreamUploader(client=None)

    with pytest.raises(ValueError, match="audio_url is required"):
        await uploader.stream_to_telegram(
            pikpak_url=("http://127.0.0.1:8500/video", ""),
            start_sec=0.0,
            end_sec=5.0,
            file_name="clip.mp4",
        )

    with pytest.raises(ValueError, match="audio_url is required"):
        await uploader.stream_to_telegram(
            pikpak_url="http://127.0.0.1:8500/video",
            audio_input_url="",
            start_sec=0.0,
            end_sec=5.0,
            file_name="clip.mp4",
        )


@pytest.mark.asyncio
async def test_uploader_dual_i_sanitizes_audio_input_url_in_stderr(monkeypatch):
    """Verify audio_input_url is sanitized in exception messages when FFmpeg fails."""
    monkeypatch.setattr("app.core.fmp4_pipe_uploader._FFMPEG_MIN_FRAG_DURATION_OK", True)

    class _FailingProc:
        returncode = 1

        def __init__(self):
            err_msg = (
                b"Error opening input https://cdn.example.com/audio_secret_path?token=super_secret_123: "
                b"Server returned 404 Not Found\n"
            )
            self.stderr = asyncio.StreamReader()
            self.stderr.feed_data(err_msg)
            self.stderr.feed_eof()
            self.stdout = asyncio.StreamReader()
            self.stdout.feed_eof()

        async def wait(self):
            return 1

        def kill(self):
            pass

    async def fake_exec(*args, **kwargs):
        return _FailingProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

    uploader = ZeroDiskFmp4StreamUploader(client=None)
    with pytest.raises(Exception) as exc_info:
        await uploader.stream_to_telegram(
            pikpak_url="https://cdn.example.com/video_stream",
            audio_input_url="https://cdn.example.com/audio_secret_path?token=super_secret_123",
            start_sec=0.0,
            end_sec=5.0,
            file_name="test.mp4",
        )

    err_str = str(exc_info.value)
    assert "super_secret_123" not in err_str, f"audio_input_url secret leaked in exception: {err_str}"
    assert "audio_secret_path" not in err_str
