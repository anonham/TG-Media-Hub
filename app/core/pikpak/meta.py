"""断点元数据沙箱命名原语 (自 pikpak_publisher_service 下沉; GC 复用同一原语)。

唯一实现 —— 禁止第三处硬编码: pipeline/utils.get_meta_path 现为本模块的委托薄层,
upload_meta_gc 与传输引擎 (engines) 均从本模块取名。
"""

import os
import re

from app.config import settings

META_SUFFIX = ".upload.meta"


def sanitize_meta_stem(raw: str) -> str:
    """文件名安全清洗: 仅保留 [A-Za-z0-9_-], 其余替换 '_' (防路径分隔符/非法字符注入)。"""
    cleaned = re.sub(r"[^A-Za-z0-9_-]", "_", str(raw))
    return cleaned or "_"


def meta_stem(task_id: str, item_id: str, segment_index: int) -> str:
    """{task_id}_{item_id}_{segment_index} (V5.1 §三.2: item_id 用 PikPak remote file_id,
    天然规避多任务共享同一 file_id 的 meta 并发竞争)。"""
    return f"{sanitize_meta_stem(task_id)}_{sanitize_meta_stem(item_id)}_{int(segment_index)}"


def meta_path(task_id: str, item_id: str, segment_index: int) -> str:
    """断点 meta 沙箱路径唯一命名助手: 必落在 settings.PIKPAK_META_DIR 下
    (引擎 Provider 分支对该路径做 trust_root 强校验, 越界即 ValueError)。"""
    return os.path.join(settings.PIKPAK_META_DIR, meta_stem(task_id, item_id, segment_index) + META_SUFFIX)
