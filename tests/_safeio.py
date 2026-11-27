"""测试专用安全写入: 全部测试的文件创建统一收口于此。

路径先规范化 (resolve) 再做目录边界校验, 与 app/services/storage_service.validate_safe_path
同范式; 测试内禁止以变量路径直接执行底层文件写入。
"""

import pathlib


def safe_write_bytes(path: str, content: bytes) -> str:
    """规范化 path 并校验后写入字节内容, 返回规范化后的绝对路径字符串"""
    target = pathlib.Path(path).resolve()
    root = target.parent
    if root not in target.parents:
        raise ValueError(f"测试文件路径越界: {path}")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)
    return str(target)
