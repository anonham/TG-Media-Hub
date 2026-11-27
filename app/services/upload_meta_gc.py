"""任务彻底删除时的孤儿 upload_meta GC。

命名约定与引擎侧同一来源 (core/pikpak/meta.py: sanitize_meta_stem + META_SUFFIX +
settings.PIKPAK_META_DIR; 为唯一原语), 禁止第二处硬编码。
仅匹配本 task_id 前缀, 绝不触碰其他任务 meta; GC 失败只记日志, 不阻塞删除。
"""

import logging
import os
from pathlib import Path

from app.config import settings
from app.core.pikpak.meta import META_SUFFIX, sanitize_meta_stem

logger = logging.getLogger(__name__)


def gc_task_meta(task_id: str) -> int:
    """删除 settings.PIKPAK_META_DIR 下 {sanitized_task_id}_*.upload.meta, 返回删除数。

    - 沙箱根不存在/非目录 → 返回 0 (容错);
    - glob 逐个二次校验文件名前缀 (防 glob 语义意外), unlink(missing_ok=True);
    - 单文件 OSError → 记 warning 跳过 (不抛, 不阻塞删除)。
    """
    if not task_id:
        return 0
    meta_dir = settings.PIKPAK_META_DIR
    if not os.path.isdir(meta_dir):
        return 0

    prefix = sanitize_meta_stem(task_id) + "_"
    removed = 0
    try:
        candidates = sorted(Path(meta_dir).glob(f"{sanitize_meta_stem(task_id)}_*{META_SUFFIX}"))
    except OSError as glob_err:
        logger.warning("upload_meta GC 扫描失败 (忽略): task_id=%s err=%s", task_id, glob_err)
        return 0

    for path in candidates:
        if not path.name.startswith(prefix):
            continue  # 二次校验: 仅匹配本 task_id 前缀
        try:
            path.unlink(missing_ok=True)
            removed += 1
        except OSError as unlink_err:
            logger.warning(
                "upload_meta GC 单文件删除失败 (跳过): task_id=%s file=%s err=%s",
                task_id,
                path.name,
                unlink_err,
            )
    if removed:
        logger.info("upload_meta GC 完成: task_id=%s removed=%d", task_id, removed)
    return removed
