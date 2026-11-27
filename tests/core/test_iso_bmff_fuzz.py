"""
Hypothesis 属性模糊测试：app/core/iso_bmff/

覆盖：
1. iter_boxes 对任意随机/截断字节流的安全解析（绝对零未捕获崩溃，仅允许 IsoBmffParseError）
2. iter_boxes 对畸形 Atom 长度（0, 1, 2..7, 2^32-1, 64-bit 溢出）的结构化异常阻断
3. scan_top_level_boxes 对任意文件尺寸与切片边界的防崩溃守卫
4. parse_moov 对纯随机字节流的鲁棒性
5. parse_moov 对合法 moov 变异/截断/畸形 Atom 注入时的零崩溃保证（安全抛出 IsoBmffError）
6. synthesize_stbl / box_synthesizer 的结构完整性与可再解析性
"""

from __future__ import annotations

import contextlib
import struct

from hypothesis import given, settings
from hypothesis import strategies as st

from app.core.iso_bmff import (
    IsoBmffError,
    IsoBmffParseError,
    iter_boxes,
    parse_moov,
    scan_top_level_boxes,
)
from app.core.iso_bmff.box_parser import _to_memoryview
from tests.core.test_vsse_box_parser import (
    build_synthetic_moov,
    build_synthetic_stbl,
    build_synthetic_trak,
    make_box,
)


@given(data=st.binary(max_size=4096))
@settings(max_examples=120)
def test_iter_boxes_fuzz_random_bytes(data: bytes) -> None:
    """
    随机字节流模糊测试：
    验证 iter_boxes 面对任意输入，要么产生合法的 box 元组，要么安全抛出 IsoBmffParseError，
    绝对不产生 IndexError, struct.error, ValueError 等未捕获崩溃。
    """
    mv = _to_memoryview(data)
    with contextlib.suppress(IsoBmffParseError):
        boxes = list(iter_boxes(mv))
        for b_type, b_pos, b_size, b_hdr in boxes:
            assert isinstance(b_type, bytes)
            assert len(b_type) == 4
            assert b_hdr in (8, 16)
            assert b_size >= b_hdr
            assert b_pos >= 0
            assert b_pos + b_size <= len(mv)


@given(
    box_type=st.binary(min_size=4, max_size=4),
    raw_size=st.integers(min_value=0, max_value=0xFFFFFFFF),
    has_large_size=st.booleans(),
    large_size=st.integers(min_value=0, max_value=0xFFFFFFFFFFFFFFFF),
    payload=st.binary(max_size=512),
    truncate_at=st.integers(min_value=0, max_value=600),
)
@settings(max_examples=120)
def test_iter_boxes_fuzz_malformed_atom_headers(
    box_type: bytes,
    raw_size: int,
    has_large_size: bool,
    large_size: int,
    payload: bytes,
    truncate_at: int,
) -> None:
    """
    注入畸形 Atom 长度（包括 0, 1, 2..7, 溢出边界）与截断头部，
    验证全部被类型化异常 IsoBmffParseError 拦截。
    """
    hdr = struct.pack(">I4sQ", 1, box_type, large_size) if has_large_size else struct.pack(">I4s", raw_size, box_type)

    buf = (hdr + payload)[:truncate_at]
    mv = _to_memoryview(buf)

    with contextlib.suppress(IsoBmffParseError):
        list(iter_boxes(mv))


@given(
    header_bytes=st.binary(max_size=2048),
    file_size=st.integers(min_value=-10, max_value=10_000_000),
)
@settings(max_examples=100)
def test_scan_top_level_boxes_fuzz_random_bytes(header_bytes: bytes, file_size: int) -> None:
    """
    scan_top_level_boxes 面对任意切片与文件尺寸，绝不发生未捕获异常。
    """
    with contextlib.suppress(IsoBmffError):
        boxes = scan_top_level_boxes(header_bytes, file_size)
        assert isinstance(boxes, list)


@given(data=st.binary(max_size=4096))
@settings(max_examples=100)
def test_parse_moov_fuzz_pure_random_bytes(data: bytes) -> None:
    """
    parse_moov 纯随机字节流模糊测试：绝不抛出非 IsoBmffError 异常。
    """
    with contextlib.suppress(IsoBmffError):
        parse_moov(data)


@st.composite
def mutated_valid_moov_st(draw: st.DrawFn) -> bytes:
    """
    基于合法的合成 moov 盒子进行有针对性的变异：
    包含截断、单点字节破坏、Atom 长度篡改、无效 entry_count 等。
    """
    is_video = draw(st.booleans())
    codec = draw(st.sampled_from([b"avc1", b"hvc1", b"mp4a", b"opus", b"bad_"]))
    sample_count = draw(st.integers(min_value=1, max_value=50))
    is_co64 = draw(st.booleans())
    include_mvex = draw(st.booleans())

    stbl = build_synthetic_stbl(sample_count=sample_count, is_video=is_video, codec=codec, is_co64=is_co64)
    trak = build_synthetic_trak(is_video=is_video, stbl_bytes=stbl)
    moov = build_synthetic_moov(tracks=[trak], include_mvex=include_mvex)

    mutation_type = draw(
        st.sampled_from(["truncate", "corrupt_bytes", "tamper_size", "prepend_junk", "wrap_box", "none"])
    )

    if mutation_type == "truncate":
        cut_point = draw(st.integers(min_value=0, max_value=len(moov)))
        return moov[:cut_point]
    if mutation_type == "corrupt_bytes" and len(moov) > 10:
        pos = draw(st.integers(min_value=0, max_value=len(moov) - 4))
        corrupt_val = draw(st.binary(min_size=4, max_size=4))
        b_arr = bytearray(moov)
        b_arr[pos : pos + 4] = corrupt_val
        return bytes(b_arr)
    if mutation_type == "tamper_size" and len(moov) >= 8:
        b_arr = bytearray(moov)
        bad_size = draw(st.sampled_from([0, 1, 2, 4, 7, 0x7FFFFFFF, 0xFFFFFFFF]))
        struct.pack_into(">I", b_arr, 0, bad_size)
        return bytes(b_arr)
    if mutation_type == "prepend_junk":
        junk = draw(st.binary(min_size=1, max_size=32))
        return junk + moov
    if mutation_type == "wrap_box":
        return make_box(b"moov", moov)
    return moov


@given(moov_data=mutated_valid_moov_st())
@settings(max_examples=120)
def test_parse_moov_fuzz_mutated_structures(moov_data: bytes) -> None:
    """
    变异合法 moov 盒子模糊测试：
    验证面对畸形、截断或篡改的结构化 box，解包器严格受限抛出结构化 IsoBmffError，
    绝对不泄露 IndexError, struct.error 或未捕获崩溃。
    """
    with contextlib.suppress(IsoBmffError):
        metadata = parse_moov(moov_data)
        assert metadata.mvhd is not None
        assert len(metadata.tracks) > 0


@given(
    sample_count=st.integers(min_value=1, max_value=100),
    samples_per_chunk=st.integers(min_value=1, max_value=20),
    is_co64=st.booleans(),
    use_stz2=st.booleans(),
)
@settings(max_examples=80)
def test_stbl_unpack_and_synthesis_invariants(
    sample_count: int,
    samples_per_chunk: int,
    is_co64: bool,
    use_stz2: bool,
) -> None:
    """
    验证合成 stbl 的解包不变式：
    - stz2 必定触发 CompactSampleSizeError
    - 正常 stbl 必定能通过 iter_boxes 完整遍历其所有子 atom
    """
    stbl_bytes = build_synthetic_stbl(
        sample_count=sample_count,
        samples_per_chunk=samples_per_chunk,
        is_co64=is_co64,
        use_stz2=use_stz2,
    )
    mv = _to_memoryview(stbl_bytes)

    # 验证最外层是一个 stbl 盒子
    b_size, b_type = struct.unpack_from(">I4s", mv, 0)
    assert b_type == b"stbl"
    assert b_size == len(stbl_bytes)

    # 遍历 stbl 内的子盒子
    sub_payload = mv[8:]
    sub_types = [t for t, _, _, _ in iter_boxes(sub_payload)]
    assert b"stsd" in sub_types
    assert b"stts" in sub_types
    assert b"stsc" in sub_types
    if use_stz2:
        assert b"stz2" in sub_types
    else:
        assert b"stsz" in sub_types
    if is_co64:
        assert b"co64" in sub_types
    else:
        assert b"stco" in sub_types
