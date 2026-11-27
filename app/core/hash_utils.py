"""流式文件哈希工具: 大文件必须分块读取, 禁止全量载入内存"""

import hashlib
from pathlib import Path

_DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1MB


def sha256_of_file(path: str | Path, chunk_size: int = _DEFAULT_CHUNK_SIZE) -> str:
    """流式计算文件 SHA-256 摘要 (hex)。

    以 chunk_size 块循环读取, 内存占用恒定; 用于下载成品落库 file_hash 与发布防重键。
    """
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(max(chunk_size, 1))
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
