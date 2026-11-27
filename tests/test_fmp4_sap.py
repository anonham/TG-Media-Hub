"""fMP4 分片 SAP 不变量测试.

单元 (无 ffmpeg):
- parse_moof_video_fragment_info 三态 SAP 判定 (True/False/None) 与多 trun 首段锁定;
- build_sidx_and_free_box 按真实 SAP 写引用字第三字段 + 合并继承首段 sap + 预留空间数学;
- ffmpeg 能力预检 fail-fast 与缓存。

golden (真实 ffmpeg, 不传 custom_cmd 锁定默认命令):
- -min_frag_duration 分片全部关键帧起始: 每个视频 moof 的 first_sample_flags 存在且为
  契约哨兵值 0x02000000, 并与 ffprobe 关键帧集合交叉验证 (取证脚本 frag_sap_check.py
  的 flags→分片首样本映射逻辑移植于此——conftest 会清空 tests/.tmp, 原脚本为一次性产物);
- sidx 引用字 starts_with_SAP 如实标记 (全部 0x90000000);
- 注入产物 ffmpeg -v error 全解码零错误。
"""

import asyncio
import json
import struct
import subprocess

import pytest

import app.core.fmp4_pipe_uploader as fmp4_mod
from app.core.fmp4_pipe_uploader import ZeroDiskFmp4StreamUploader
from app.core.iso_bmff import (
    RESERVED_SIDX_SPACE,
    build_sidx_and_free_box,
    parse_moof_video_fragment_info,
    parse_moov_video_properties,
)

FFMPEG_FIRST_SAMPLE_FLAGS_SAP = 0x02000000


# ---------------------------------------------------------------------------
# 手造 moof 构造工具 (单元测试用, 无需 ffmpeg)
# ---------------------------------------------------------------------------


def _box(btype: bytes, payload: bytes) -> bytes:
    return struct.pack(">I4s", 8 + len(payload), btype) + payload


def _make_moof(track_id: int, trun_specs, tfhd_flags: int = 0x0):
    """构造最小 moof: mfhd + traf{ tfhd(track_id), trun... }。

    trun_specs: [(sample_durations, first_sample_flags_or_None), ...]
    tfhd 可带 default_sample_duration (flags 0x8) 供无逐样本时长的 trun 使用。
    """
    mfhd = _box(b"mfhd", struct.pack(">I", 1))
    tfhd_payload = bytearray(b"\x00" + tfhd_flags.to_bytes(3, "big") + struct.pack(">I", track_id))
    if tfhd_flags & 0x000008:
        tfhd_payload += struct.pack(">I", 100)  # default_sample_duration
    tfhd = _box(b"tfhd", bytes(tfhd_payload))
    truns = b""
    for durations, first_flags in trun_specs:
        flags = 0x000100 | (0x000004 if first_flags is not None else 0)
        payload = bytearray(b"\x00" + flags.to_bytes(3, "big") + struct.pack(">I", len(durations)))
        if first_flags is not None:
            payload += struct.pack(">I", first_flags)
        for d in durations:
            payload += struct.pack(">I", d)
        truns += _box(b"trun", bytes(payload))
    traf = _box(b"traf", tfhd + truns)
    return _box(b"moof", mfhd + traf)


# ---------------------------------------------------------------------------
# 单元: parse_moof_video_fragment_info
# ---------------------------------------------------------------------------


def test_parse_moof_info_sap_true():
    moof = _make_moof(1, [([1000, 1000, 1000], FFMPEG_FIRST_SAMPLE_FLAGS_SAP)])
    ticks, sap = parse_moof_video_fragment_info(moof, target_track_id=1)
    assert ticks == 3000
    assert sap is True


def test_parse_moof_info_sap_false_when_flags_absent():
    moof = _make_moof(1, [([1000, 1000], None)])
    ticks, sap = parse_moof_video_fragment_info(moof, target_track_id=1)
    assert ticks == 2000
    assert sap is False


@pytest.mark.parametrize("bad_value", [0x01010000, 0x01000000, 0x02010000, 0x00000000])
def test_parse_moof_info_sap_false_on_non_sap_value(bad_value):
    moof = _make_moof(1, [([1000], bad_value)])
    _, sap = parse_moof_video_fragment_info(moof, target_track_id=1)
    assert sap is False


def test_parse_moof_info_no_target_track_returns_none():
    moof = _make_moof(2, [([1000], FFMPEG_FIRST_SAMPLE_FLAGS_SAP)])  # 纯音频轨分片
    ticks, sap = parse_moof_video_fragment_info(moof, target_track_id=1)
    assert ticks == 0
    assert sap is None


def test_parse_moof_info_first_trun_latches_sap():
    # 首个视频 trun 无 first_sample_flags → False; 第二个 trun 的 SAP 值不得改写判定
    moof = _make_moof(1, [([1000], None), ([1000], FFMPEG_FIRST_SAMPLE_FLAGS_SAP)])
    ticks, sap = parse_moof_video_fragment_info(moof, target_track_id=1)
    assert ticks == 2000
    assert sap is False


def test_parse_moof_info_default_duration_without_per_sample():
    # trun 无逐样本时长 (flags 0x100 缺席) → sample_count × tfhd default_sample_duration
    mfhd = _box(b"mfhd", struct.pack(">I", 1))
    tfhd = _box(b"tfhd", b"\x00\x00\x00\x08" + struct.pack(">I", 1) + struct.pack(">I", 100))
    trun_payload = b"\x00\x00\x00\x00" + struct.pack(">I", 3)  # flags=0: 无逐样本字段
    trun = _box(b"trun", trun_payload)
    traf = _box(b"traf", tfhd + trun)
    moof = _box(b"moof", mfhd + traf)
    ticks, sap = parse_moof_video_fragment_info(moof, target_track_id=1)
    assert ticks == 300
    assert sap is False


# ---------------------------------------------------------------------------
# 单元: build_sidx_and_free_box
# ---------------------------------------------------------------------------


def _decode_sidx_refs(blob: bytes):
    size, btype = struct.unpack_from(">I4s", blob, 0)
    assert btype == b"sidx"
    assert blob[8] == 0  # version 0
    first_offset = struct.unpack_from(">I", blob, 24)[0]
    ref_count = struct.unpack_from(">H", blob, 30)[0]
    refs = []
    pos = 32
    for _ in range(ref_count):
        refs.append(struct.unpack_from(">III", blob, pos))
        pos += 12
    return first_offset, refs, size


def test_build_sidx_sap_three_states():
    frags = [(100, 1000, True), (200, 1000, False), (300, 1000, None)]
    blob = build_sidx_and_free_box(frags, timescale=90000, reserved_size=RESERVED_SIDX_SPACE)
    _, refs, sidx_size = _decode_sidx_refs(blob)
    assert len(refs) == 3
    assert refs[0][2] == 0x90000000  # sap=True → starts_with_SAP=1
    assert refs[1][2] == 0x00000000  # sap=False
    assert refs[2][2] == 0x00000000  # sap=None
    assert (refs[0][0], refs[0][1]) == (100, 1000)


def test_build_sidx_merge_inherits_first_segment_sap():
    # 4 条引用 (4*12+32=80 > 68-8=60) → 两两合并为 2 条 (2*12+32=56 ≤ 60)
    frags = [(10, 100, True), (20, 200, False), (30, 300, False), (40, 400, True)]
    blob = build_sidx_and_free_box(frags, timescale=1000, reserved_size=68)
    _, refs, _ = _decode_sidx_refs(blob)
    assert len(refs) == 2
    # (True,False) → 首段 True; (False,True) → 首段 False
    assert refs[0][:] == (30, 300, 0x90000000)
    assert refs[1][:] == (70, 700, 0x00000000)


def test_build_sidx_reserved_space_math_no_regression():
    frags = [(111, 222, True)]
    blob = build_sidx_and_free_box(frags, 90000, reserved_size=256)
    assert len(blob) == 256
    first_offset, refs, sidx_size = _decode_sidx_refs(blob)
    assert first_offset == 256 - sidx_size  # first_offset = free 大小, 指向首个 moof
    free_size, free_type = struct.unpack_from(">I4s", blob, sidx_size)
    assert free_type == b"free"
    assert free_size == 256 - sidx_size


# ---------------------------------------------------------------------------
# 单元: ffmpeg 能力预检
# ---------------------------------------------------------------------------


class _FakeProc:
    def __init__(self, stdout=b""):
        self.returncode = 0
        self._stdout = stdout

    async def communicate(self):
        return self._stdout, b""


@pytest.mark.asyncio
async def test_capability_precheck_fail_fast(monkeypatch):
    async def fake_exec(*args, **kwargs):
        return _FakeProc(stdout=b"-frag_duration <int> ...")  # 无 min_frag_duration

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(fmp4_mod, "_FFMPEG_MIN_FRAG_DURATION_OK", None)
    with pytest.raises(RuntimeError, match="min_frag_duration"):
        await fmp4_mod._ensure_ffmpeg_min_frag_duration_support()
    assert fmp4_mod._FFMPEG_MIN_FRAG_DURATION_OK is False  # 确定性缺失才缓存


@pytest.mark.asyncio
async def test_capability_precheck_exec_error_propagates_uncached(monkeypatch):
    # 预检进程自身异常 (ffmpeg 缺失等) 不缓存、原样上抛, 避免环境故障被永久误判为能力缺失
    async def fake_exec(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(fmp4_mod, "_FFMPEG_MIN_FRAG_DURATION_OK", None)
    with pytest.raises(RuntimeError, match="boom"):
        await fmp4_mod._ensure_ffmpeg_min_frag_duration_support()
    assert fmp4_mod._FFMPEG_MIN_FRAG_DURATION_OK is None


@pytest.mark.asyncio
async def test_capability_precheck_pass_and_cache(monkeypatch):
    calls = []

    async def fake_exec(*args, **kwargs):
        calls.append(list(args))
        return _FakeProc(stdout=b"-min_frag_duration <int> ...")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(fmp4_mod, "_FFMPEG_MIN_FRAG_DURATION_OK", None)
    await fmp4_mod._ensure_ffmpeg_min_frag_duration_support()
    await fmp4_mod._ensure_ffmpeg_min_frag_duration_support()
    assert len(calls) == 1  # 模块级缓存: 第二次不再起进程
    assert fmp4_mod._FFMPEG_MIN_FRAG_DURATION_OK is True


# ---------------------------------------------------------------------------
# 单元: 默认命令锁定 (假 create_subprocess_exec 捕获 argv)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_default_cmd_uses_min_frag_duration(monkeypatch):
    captured = []

    async def fake_exec(*args, **kwargs):
        captured.append(list(args))
        raise RuntimeError("Stop after capturing command")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(fmp4_mod, "_FFMPEG_MIN_FRAG_DURATION_OK", True)  # 预检走缓存
    uploader = ZeroDiskFmp4StreamUploader(client=None)
    with pytest.raises(RuntimeError, match="Stop after capturing command"):
        await uploader.stream_to_telegram(
            pikpak_url="https://cdn.test/x.mp4",
            start_sec=0.0,
            end_sec=10.0,
            file_name="x.mp4",
            copy_audio=True,
        )
    cmd = captured[-1]
    assert "-min_frag_duration" in cmd
    assert cmd[cmd.index("-min_frag_duration") + 1] == "2000000"
    assert "-frag_duration" not in cmd
    assert "frag_keyframe+empty_moov+default_base_moof+skip_trailer" in cmd


# ---------------------------------------------------------------------------
# golden: 真实 ffmpeg + 默认命令 (不传 custom_cmd)
# ---------------------------------------------------------------------------


def _gen_gop_src(path: str, gop_sec: int, total_s: int = 40):
    fps = 30
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
            f"testsrc2=size=320x180:rate={fps}:duration={total_s}",
            "-f",
            "lavfi",
            "-i",
            f"sine=frequency=440:sample_rate=44100:duration={total_s}",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-g",
            str(gop_sec * fps),
            "-keyint_min",
            str(gop_sec * fps),
            "-sc_threshold",
            "0",
            "-c:a",
            "aac",
            "-b:a",
            "96k",
            "-shortest",
            str(path),
        ],
        check=True,
    )


def _walk_top_level_boxes(data: bytes):
    pos = 0
    end = len(data)
    while pos + 8 <= end:
        size, btype = struct.unpack_from(">I4s", data, pos)
        hlen = 8
        if size == 1:
            size = struct.unpack_from(">Q", data, pos + 8)[0]
            hlen = 16
        elif size == 0:
            size = end - pos
        if size < hlen:
            break
        yield pos, size, btype
        pos += size


def _first_video_trun_info(moof: bytes, video_track_id: int):
    """移植自取证脚本 frag_sap_check.parse_moof_first_video_sample:
    返回首个目标轨 trun 的 (sample_count, raw_first_sample_flags_or_None)。"""
    pos = 8
    end = len(moof)
    b_size, b_type = struct.unpack_from(">I4s", moof, 0)
    if b_size == 1 and end >= 16:
        pos = 16
    while pos + 8 <= end:
        b_size, b_type = struct.unpack_from(">I4s", moof, pos)
        if b_size < 8 or pos + b_size > end:
            break
        if b_type == b"traf":
            t_pos = pos + 8
            t_end = pos + b_size
            is_target = False
            while t_pos + 8 <= t_end:
                sub_size, sub_type = struct.unpack_from(">I4s", moof, t_pos)
                if sub_size < 8 or t_pos + sub_size > t_end:
                    break
                if sub_type == b"tfhd":
                    track_id = struct.unpack_from(">I", moof, t_pos + 12)[0]
                    is_target = track_id == video_track_id
                elif sub_type == b"trun" and is_target:
                    flags = struct.unpack_from(">I", moof, t_pos + 8)[0] & 0xFFFFFF
                    sample_count = struct.unpack_from(">I", moof, t_pos + 12)[0]
                    entry_off = t_pos + 16
                    first_flags = None
                    if flags & 0x000001:
                        entry_off += 4
                    if flags & 0x000004:
                        first_flags = struct.unpack_from(">I", moof, entry_off)[0]
                    return sample_count, first_flags
                t_pos += sub_size
        pos += b_size
    return None, None


def _collect_video_fragments(blob: bytes):
    """返回 ([(duration_ticks, sap, raw_first_flags, sample_count)], sidx 引用, 视频轨 timescale)。"""
    video_track_id = 1
    video_timescale = 90000
    frags = []
    sidx_refs = None
    for pos, size, btype in _walk_top_level_boxes(blob):
        if btype == b"moov":
            video_track_id, video_timescale = parse_moov_video_properties(blob[pos : pos + size])[:2]
        elif btype == b"moof":
            moof = blob[pos : pos + size]
            ticks, sap = parse_moof_video_fragment_info(moof, target_track_id=video_track_id)
            sample_count, raw_flags = _first_video_trun_info(moof, video_track_id)
            frags.append((ticks, sap, raw_flags, sample_count))
        elif btype == b"sidx":
            ref_count = struct.unpack_from(">H", blob, pos + 30)[0]
            rpos = pos + 32
            sidx_refs = [struct.unpack_from(">III", blob, rpos + i * 12) for i in range(ref_count)]
    return frags, sidx_refs, video_timescale


def _ffprobe_video_keyframe_flags(file_path: str):
    p = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "packet=flags",
            "-of",
            "json",
            str(file_path),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    packets = json.loads(p.stdout)["packets"]
    return ["K" in pkt.get("flags", "") for pkt in packets]


@pytest.mark.asyncio
@pytest.mark.parametrize("gop_sec,min_frag_count", [(2, 18), (10, 4)])
async def test_golden_min_frag_duration_all_sap(tmp_path, gop_sec, min_frag_count):
    src = tmp_path / f"src_gop{gop_sec}.mp4"
    _gen_gop_src(str(src), gop_sec)

    collected = []

    async def sink(idx, tot, data):
        collected.append((idx, tot, bytes(data)))

    uploader = ZeroDiskFmp4StreamUploader(client=None, max_workers=2, queue_size=8)
    res, dur = await uploader.stream_to_telegram(
        pikpak_url=str(src),
        start_sec=0.0,
        end_sec=40.0,
        file_name=src.name,
        copy_audio=True,
        inspect_sink=sink,
    )
    assert collected
    assert dur > 0.0
    blob = b"".join(d for _, _, d in sorted(collected, key=lambda x: x[0]))

    frags, sidx_refs, video_timescale = _collect_video_fragments(blob)
    assert len(frags) >= min_frag_count

    # 1) 每个视频分片首样本 = SAP: first_sample_flags 存在且为契约哨兵值 (ffmpeg 只在
    #    分片首视频样本为关键帧时写该字段); 解析器判定 sap=True
    for ticks, sap, raw_flags, sample_count in frags:
        assert raw_flags == FFMPEG_FIRST_SAMPLE_FLAGS_SAP
        assert sap is True
        assert ticks > 0
        assert sample_count and sample_count > 0

    # 2) 与 ffprobe 关键帧集合交叉验证 (取证脚本算法): 每个分片首视频样本必为关键帧
    out_file = tmp_path / f"out_gop{gop_sec}.mp4"
    out_file.write_bytes(blob)
    key_flags = _ffprobe_video_keyframe_flags(str(out_file))
    running = 0
    for _ticks, _sap, _raw_flags, sample_count in frags:
        assert running < len(key_flags), "fragment sample map overrun"
        assert key_flags[running] is True, f"fragment starting at video sample {running} is not a keyframe"
        running += sample_count

    # 3) 分片时长 ≈ GOP (末片允许携带音频尾巴, 单独放宽)
    ts = video_timescale
    for ticks, _, _, _ in frags[:-1]:
        assert abs(ticks / ts - gop_sec) <= 0.75
    assert frags[-1][0] / ts <= gop_sec + 0.75

    # 4) sidx 引用字如实标记: 全部 starts_with_SAP=1, 条目数 = 分片数 (单产物不触发合并)
    assert sidx_refs is not None
    assert len(sidx_refs) == len(frags)
    assert all(r[2] == 0x90000000 for r in sidx_refs)

    # 5) 注入产物全解码零错误
    p = subprocess.run(
        ["ffmpeg", "-v", "error", "-i", str(out_file), "-f", "null", "-"],
        capture_output=True,
        text=True,
    )
    assert p.returncode == 0
    assert not p.stderr.strip()


@pytest.mark.parametrize(
    ("sample_flags", "expected_sap"),
    [
        (FFMPEG_FIRST_SAMPLE_FLAGS_SAP, True),
        (0x01010000, False),
    ],
)
def test_parse_moof_info_per_sample_flags_sap(sample_flags: int, expected_sap: bool):
    """When first_sample_flags (0x000004) is absent, read first sample's flags from per-sample flags (0x000400)."""
    mfhd = _box(b"mfhd", struct.pack(">I", 1))
    tfhd = _box(b"tfhd", b"\x00\x00\x00\x00" + struct.pack(">I", 1))
    # trun flags: data_offset (0x1) + duration (0x100) + size (0x200) + sample_flags (0x400)
    trun_flags = 0x000701
    trun_payload = bytearray(b"\x00" + trun_flags.to_bytes(3, "big") + struct.pack(">I", 2))
    trun_payload += struct.pack(">i", 0)  # data_offset
    # sample 1: duration=1000, size=500, flags=sample_flags
    trun_payload += struct.pack(">III", 1000, 500, sample_flags)
    # sample 2: duration=1000, size=200, flags=0x01010000
    trun_payload += struct.pack(">III", 1000, 200, 0x01010000)
    trun = _box(b"trun", bytes(trun_payload))
    moof = _box(b"moof", mfhd + _box(b"traf", tfhd + trun))
    ticks, sap = parse_moof_video_fragment_info(moof, target_track_id=1)
    assert ticks == 2000
    assert sap is expected_sap


@pytest.mark.parametrize("flags", [0x400, 0x004])
def test_parse_moof_info_does_not_read_flags_from_next_box(flags):
    tfhd = _box(b"tfhd", struct.pack(">II", 0, 1))
    trun = _box(b"trun", struct.pack(">II", flags, 1))
    # The following box's size looks exactly like SAP flags if read as a record.
    following = struct.pack(">I4s", FFMPEG_FIRST_SAMPLE_FLAGS_SAP, b"free")
    moof = _box(b"moof", _box(b"traf", tfhd + trun + following))
    assert parse_moof_video_fragment_info(moof) == (0, False)
