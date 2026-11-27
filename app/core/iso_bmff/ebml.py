"""
Matroska (EBML) 变长整数解码原语。

从 fmp4_pipe_uploader.MkvCuesProber 收拢: MKV Cues 关键帧
探测与 fMP4 流式上传共用同一套严格 1~8 字节 VINT 语义, 单一实现防双头漂移。
"""


def _read_ebml_vint(data: bytes, offset: int) -> tuple[int, int]:
    """读取 EBML 动态变长整数 (1~8 字节 VINT)。返回 (value, length)。"""
    if offset >= len(data):
        return -1, 0
    first_byte = data[offset]
    if first_byte == 0:
        return -1, 0
    mask = 0x80
    length = 1
    while not (first_byte & mask):
        mask >>= 1
        length += 1
    if offset + length > len(data):
        return -1, 0
    val = first_byte & (~mask)
    for i in range(1, length):
        val = (val << 8) | data[offset + i]
    return val, length


def _read_ebml_element_id(data: bytes, offset: int) -> tuple[int, int]:
    """读取 EBML 元素 ID (保留变长首字节标记位)。返回 (element_id, length)。"""
    if offset >= len(data):
        return -1, 0
    first_byte = data[offset]
    if first_byte == 0:
        return -1, 0
    mask = 0x80
    length = 1
    while not (first_byte & mask):
        mask >>= 1
        length += 1
    if offset + length > len(data):
        return -1, 0
    val = 0
    for i in range(length):
        val = (val << 8) | data[offset + i]
    return val, length
