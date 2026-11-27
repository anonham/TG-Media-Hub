"""媒体文件格式深度特征嗅探与防误判校验模块。

提供针对复合后缀 (如 .ts 兼属 MPEG Transport Stream 视频与 TypeScript 源码)
的微秒级无依赖二义性消除判定。
"""

import codecs
import os

from app.config import settings

TS_AMBIGUOUS_EXTENSIONS: tuple[str, ...] = (".ts", ".m2ts", ".mts")
_MPEG_TS_PACKET_SIZES = (188, 192, 204)
_MPEG_TS_MIN_SYNC_PACKETS = 4
MPEG_TS_MIN_PROBE_BYTES = min(_MPEG_TS_PACKET_SIZES) * _MPEG_TS_MIN_SYNC_PACKETS
MPEG_TS_MIN_REJECTION_BYTES = max(_MPEG_TS_PACKET_SIZES) * _MPEG_TS_MIN_SYNC_PACKETS
_UTF8_INCREMENTAL_DECODER = codecs.getincrementaldecoder("utf-8")


def is_plain_utf8_text(head: bytes) -> bool:
    """判断缓冲区是否为纯 UTF-8 文本源码 (含注释内多字节字符，不含控制符与 NUL)。"""
    if any((byte < 32 and byte not in (9, 10, 13)) or byte == 127 for byte in head):
        return False
    # 固定长度读取 (例如 2048 B) 可能在末尾截断多字节 UTF-8 字符：final=False 仅缓存末尾
    # 合法的不完整序列前缀，其余无效字节 (0xff、孤立续字节、overlong 前缀等) 直接判非文本
    try:
        text = _UTF8_INCREMENTAL_DECODER("strict").decode(head, final=False)
    except UnicodeDecodeError:
        return False
    return bool(text)


def is_valid_mpeg_ts_buffer(head: bytes, min_sync_packets: int = _MPEG_TS_MIN_SYNC_PACKETS) -> bool:
    """快速判定二进制头部是否符合 MPEG-TS (188/192/204 字节步进) 结构。

    - 周期同步字节校验：标准 MPEG-TS 封包长 188 字节，BDAV (M2TS) 长 192 字节，DVB/FEC 长 204 字节，
      每个封包首字节（或 M2TS 首包偏移后）严格以 0x47 (ASCII 'G') 同步字对齐。
    """
    if len(head) < min(_MPEG_TS_PACKET_SIZES) * min_sync_packets:
        return False

    # 拒绝纯 ASCII 与含多字节 UTF-8 注释的文本源码 (如 .ts TypeScript 注释中对齐的 'G')
    if is_plain_utf8_text(head):
        return False

    for stride in _MPEG_TS_PACKET_SIZES:
        needed = stride * min_sync_packets
        if len(head) < needed:
            continue
        max_start = len(head) - stride * (min_sync_packets - 1) - 3
        for start in range(max_start):
            # adaptation_field_control=0 is reserved, not a valid packet header.
            if all(
                head[start + i * stride] == 0x47 and head[start + i * stride + 3] & 0x30
                for i in range(min_sync_packets)
            ):
                return True
    return False


def is_valid_mpeg_ts_file(file_path: str) -> bool:
    """校验本地文件是否为合法的 MPEG-TS 视频文件。"""
    fname = os.path.basename(file_path).lower()
    if fname.endswith(".d.ts"):
        return False
    try:
        with open(file_path, "rb") as f:
            head = f.read(2048)
        return is_valid_mpeg_ts_buffer(head)
    except OSError:
        return False


def is_supported_local_media(file_path: str) -> tuple[bool, str | None]:
    """综合校验本地文件是否为受支持的媒体文件，返回 (is_media, ignore_reason)。"""
    fname = os.path.basename(file_path)
    lower = fname.lower()
    ext = os.path.splitext(lower)[1]

    if lower.endswith(".d.ts"):
        return False, "TypeScript 类型声明文件 (.d.ts)，已自动过滤"

    if ext in settings.IMAGE_EXTENSIONS or ext in settings.AUDIO_EXTENSIONS:
        return True, None

    if ext in settings.VIDEO_EXTENSIONS:
        if ext in TS_AMBIGUOUS_EXTENSIONS:
            if not is_valid_mpeg_ts_file(file_path):
                return False, f"非有效 MPEG-TS 视频格式 ({ext} 疑似文本源码或不合规封包)，已自动过滤"
            return True, None
        return True, None

    return False, f"非有效媒体格式 ({ext or '无后缀'})，已自动过滤"
