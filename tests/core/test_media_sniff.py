"""MPEG-TS 与 TypeScript 代码媒体特征嗅探单元测试。"""

import pytest

from app.core.media_sniff import (
    is_plain_utf8_text,
    is_supported_local_media,
    is_valid_mpeg_ts_buffer,
    is_valid_mpeg_ts_file,
)


def _make_synthetic_ts(packet_size: int = 188, num_packets: int = 5, start_offset: int = 0) -> bytes:
    """生成带有周期同步字 0x47 的模拟 TS 数据包。"""
    buf = bytearray(b"\x00" * start_offset)
    for i in range(num_packets):
        packet = bytearray(packet_size)
        # MPEG-TS 同步字节恒为 0x47
        packet[0] = 0x47
        # 携带包含 \x00 的典型 PID/头数据
        packet[1] = 0x00
        packet[2] = 0x11
        packet[3] = 0x10 + (i % 16)
        buf.extend(packet)
    return bytes(buf)


def test_is_plain_utf8_text_accepts_ascii_and_truncated_multibyte_tail():
    # 完整合法 UTF-8 (纯 ASCII 与含多字节注释)
    assert is_plain_utf8_text(b"const x = 1;\n" * 20)
    assert is_plain_utf8_text("/* Café */ const x = 1;\n".encode())
    # 固定长度读取恰好在合法多字节字符中间截断：允许 1-3 字节的不完整序列前缀
    assert is_plain_utf8_text(b"a" * 2046 + "你".encode()[:2])
    assert is_plain_utf8_text(b"a" * 10 + bytes.fromhex("f09f9880")[:3])  # 4 字节 emoji 截断于 3 字节
    assert is_plain_utf8_text(b"a" * 2047 + b"\xe4")


def test_is_plain_utf8_text_rejects_invalid_trailing_bytes():
    # (6006788077)：本质无效的字节不是截断序列，裁剪不得将其抹掉
    assert not is_plain_utf8_text(b"a" * 2047 + b"\xff")
    # 孤立续字节
    assert not is_plain_utf8_text(b"a" * 2047 + b"\x80")
    # overlong 3 字节前缀 (即使补全也非法)
    assert not is_plain_utf8_text(b"a" * 2047 + b"\xe0\x80")
    # 超出 U+10FFFF 的 4 字节前缀 (0xF4 0x90)
    assert not is_plain_utf8_text(b"a" * 2047 + b"\xf4\x90")
    # 控制符 / NUL 在解码前即被拒绝
    assert not is_plain_utf8_text(b"a" * 100 + b"\x00" + b"a" * 100)
    assert not is_plain_utf8_text(b"a" * 100 + b"\x01" + b"a" * 100)


def test_is_plain_utf8_text_rejects_prefix_only_buffer():
    # 仅含不完整前缀的极短缓冲不构成文本证据
    assert not is_plain_utf8_text(b"\xe4\xb8")


def test_is_valid_mpeg_ts_buffer_188():
    data = _make_synthetic_ts(188, 5)
    assert is_valid_mpeg_ts_buffer(data) is True


def test_is_valid_mpeg_ts_buffer_192():
    data = _make_synthetic_ts(192, 5)
    assert is_valid_mpeg_ts_buffer(data) is True


def test_is_valid_mpeg_ts_buffer_204():
    data = _make_synthetic_ts(204, 5)
    assert is_valid_mpeg_ts_buffer(data) is True


def test_is_valid_mpeg_ts_buffer_with_lead_offset():
    # 前置 12 字节的垃圾对齐数据
    data = _make_synthetic_ts(188, 5, start_offset=12)
    assert is_valid_mpeg_ts_buffer(data) is True


def test_is_valid_mpeg_ts_buffer_rejects_typescript_code():
    code = (
        b"import { ref, computed } from 'vue';\n\n"
        b"export interface UserInfo {\n"
        b"  id: number;\n"
        b"  name: string;\n"
        b"}\n\n"
        b"export function useUser() {\n"
        b"  const user = ref<UserInfo | null>(null);\n"
        b"  return { user };\n"
        b"}\n"
    )
    assert is_valid_mpeg_ts_buffer(code) is False


def test_is_valid_mpeg_ts_buffer_rejects_qt_xml():
    xml = (
        b'<?xml version="1.0" encoding="utf-8"?>\n'
        b"<!DOCTYPE TS>\n"
        b'<TS version="2.1" language="zh_CN">\n'
        b"<context>\n"
        b"    <name>MainWindow</name>\n"
        b"    <message>\n"
        b"        <source>Hello</source>\n"
        b"        <translation>\xe4\xbd\xa0\xe5\xa5\xbd</translation>\n"
        b"    </message>\n"
        b"</context>\n"
        b"</TS>\n"
    )
    assert is_valid_mpeg_ts_buffer(xml) is False


def test_is_valid_mpeg_ts_buffer_rejects_random_binary():
    # 包含 \x00 但没有规律 0x47 步进的随机二进制数据
    import random

    rng = random.Random(42)
    data = bytes(rng.getrandbits(8) for _ in range(1024))
    assert is_valid_mpeg_ts_buffer(data) is False


def test_is_valid_mpeg_ts_buffer_short():
    # 不足 4 个包长
    short_data = b"\x47\x00\x00\x10" * 10
    assert is_valid_mpeg_ts_buffer(short_data) is False


def test_is_valid_mpeg_ts_file(tmp_path):
    ts_video = tmp_path / "movie.ts"
    ts_video.write_bytes(_make_synthetic_ts(188, 6))
    assert is_valid_mpeg_ts_file(str(ts_video)) is True

    ts_code = tmp_path / "index.ts"
    ts_code.write_text("console.log('hello');", encoding="utf-8")
    assert is_valid_mpeg_ts_file(str(ts_code)) is False

    d_ts = tmp_path / "index.d.ts"
    d_ts.write_bytes(_make_synthetic_ts(188, 6))  # 即使二进制内容，后缀是 .d.ts 也拒绝
    assert is_valid_mpeg_ts_file(str(d_ts)) is False


def test_is_supported_local_media(tmp_path):
    # 1. 普通 MP4 视频
    mp4_file = tmp_path / "sample.mp4"
    mp4_file.write_bytes(b"dummy mp4")
    is_media, reason = is_supported_local_media(str(mp4_file))
    assert is_media is True
    assert reason is None

    # 2. 合法 TS 视频
    ts_file = tmp_path / "sample.ts"
    ts_file.write_bytes(_make_synthetic_ts(188, 6))
    is_media, reason = is_supported_local_media(str(ts_file))
    assert is_media is True
    assert reason is None

    # 3. TypeScript 源码文件
    code_file = tmp_path / "app.ts"
    code_file.write_text("export const x = 1;", encoding="utf-8")
    is_media, reason = is_supported_local_media(str(code_file))
    assert is_media is False
    assert "疑似文本源码" in str(reason)

    # 4. .d.ts 声明文件
    decl_file = tmp_path / "env.d.ts"
    decl_file.write_text("declare module '*.vue';", encoding="utf-8")
    is_media, reason = is_supported_local_media(str(decl_file))
    assert is_media is False
    assert ".d.ts" in str(reason)

    # 5. 非媒体文件 (.exe, .py)
    py_file = tmp_path / "main.py"
    py_file.write_text("print('hi')", encoding="utf-8")
    is_media, reason = is_supported_local_media(str(py_file))
    assert is_media is False
    assert "非有效媒体格式" in str(reason)


@pytest.mark.parametrize("extension", ["ts", "m2ts", "mts"])
def test_null_packets_before_video_are_accepted(tmp_dir, extension):
    from pathlib import Path

    from app.core.pipeline.utils import scan_folder_grouped
    from app.services.publisher_service import PublisherService

    # Three legal null packets occupy the first 564 bytes without any zero bytes.
    null_packet = b"\x47\x1f\xff\x10" + b"\xff" * 184
    recording = null_packet * 3 + _make_synthetic_ts()
    assert b"\x00" not in recording[:512]
    folder = Path(tmp_dir) / "recordings"
    folder.mkdir()
    path = folder / f"recording.{extension}"
    path.write_bytes(recording)

    assert is_valid_mpeg_ts_buffer(recording)
    assert is_supported_local_media(str(path)) == (True, None)
    assert scan_folder_grouped(str(folder)) == {str(folder): [str(path)]}
    report = PublisherService.inspect_folder(str(folder))
    assert report["videos"]["count"] == 1
    assert report["ignored_files"] == []


def test_mpeg_ts_without_any_zero_bytes():
    null_packet = b"\x47\x1f\xff\x10" + b"\xff" * 184
    assert is_valid_mpeg_ts_buffer(null_packet * 5)


def test_mpeg_ts_rejects_reserved_packet_header():
    invalid_packet = b"\x47\x1f\xff\x00" + b"\xff" * 184
    assert not is_valid_mpeg_ts_buffer(invalid_packet * 5)


def test_mpeg_ts_rejects_truncated_last_header():
    data = b"\xff" * 187 + _make_synthetic_ts(num_packets=4)
    assert not is_valid_mpeg_ts_buffer(data[: 188 * 4])


@pytest.mark.parametrize("stride", [188, 192, 204])
@pytest.mark.parametrize("prefix", [b"\xff" * 600, b"\x47\x1f\xff\x00" + b"\xff" * 596], ids=["junk", "invalid-header"])
def test_mpeg_ts_finds_later_valid_sequence(stride, prefix):
    assert is_valid_mpeg_ts_buffer(prefix + _make_synthetic_ts(stride, 4))


@pytest.mark.parametrize("stride", [188, 192, 204])
def test_mpeg_ts_skips_damaged_packet(stride):
    data = bytearray(_make_synthetic_ts(stride, 6))
    data[stride + 3] = 0  # The second packet has a reserved header.
    assert is_valid_mpeg_ts_buffer(bytes(data))


@pytest.mark.parametrize("stride", [188, 192, 204])
def test_aligned_typescript_is_rejected(tmp_path, stride):
    line = b"Global.foo(); //".ljust(stride - 1, b"x") + b"\n"
    source = line * 6
    assert not is_valid_mpeg_ts_buffer(source)
    path = tmp_path / "source.ts"
    path.write_bytes(source)
    is_media, reason = is_supported_local_media(str(path))
    assert not is_media
    assert "疑似文本源码" in reason


def test_binary_mpeg_ts_with_text_payload_is_accepted():
    packet = b"\x47\x1f\xff\x10" + b"Global.foo(); //".ljust(184, b"x")
    assert is_valid_mpeg_ts_buffer(packet * 5)


@pytest.mark.parametrize("stride", [188, 192, 204])
def test_aligned_typescript_with_utf8_comment_is_rejected(tmp_path, stride):
    line = b"/* Caf\xc3\xa9 */ Globa".ljust(stride - 1, b"x") + b"\n"
    source = line * 6
    assert not is_valid_mpeg_ts_buffer(source)
    path = tmp_path / "comment_utf8.ts"
    path.write_bytes(source)
    is_media, reason = is_supported_local_media(str(path))
    assert not is_media
    assert "疑似文本源码" in str(reason)
