"""PikPak 远端目录展开算法与快照解析 (自 pikpak_publisher_service/pikpak_service 下沉)。

核心定义算法, IO 由调用方注入: list_files_raw 以参数传入 (services 层提供 CLI 包装),
依赖方向不得反转 (core.pikpak 严禁 import app.services.*)。
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from app.core.scheduler import UploadCancelledError, UploadPausedError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ParsedFileItem:
    """`ls --raw` 解析产物 (字段映射表见 ; 字符串语义保持, 不强转)"""

    file_id: str
    name: str
    is_dir: bool
    size_bytes: int
    audit: dict
    thumbnail_link: str | None
    duration_s: str | None
    width: str | None
    height: str | None
    parent_id: str
    audio_codec: str | None = None
    video_codec: str | None = None


def items_from_snapshot(files: list | None) -> list[ParsedFileItem]:
    """params['remote_files'] 字典列表 → ParsedFileItem (缺省字段填默认值; file_id 必填)。"""
    items: list[ParsedFileItem] = []
    for raw in files or []:
        if not isinstance(raw, dict):
            continue
        file_id = str(raw.get("file_id") or "").strip()
        if not file_id:
            logger.warning("PikPak 快照条目缺少 file_id, 已跳过: %r", {k: raw.get(k) for k in ("name", "parent_id")})
            continue
        items.append(
            ParsedFileItem(
                file_id=file_id,
                name=str(raw.get("name") or file_id),
                is_dir=bool(raw.get("is_dir", False)),
                size_bytes=int(raw.get("size_bytes") or 0),
                audit=raw.get("audit") or {},
                thumbnail_link=raw.get("thumbnail_link") or None,
                duration_s=raw.get("duration_s"),
                width=raw.get("width"),
                height=raw.get("height"),
                parent_id=str(raw.get("parent_id") or ""),
                audio_codec=raw.get("audio_codec"),
                video_codec=raw.get("video_codec"),
            )
        )
    return items


async def _notify_truncation(
    *,
    task_id: str | None,
    max_files: int,
    kept_files: int,
    truncation_out: dict[str, Any] | None,
) -> None:
    from app.db.task_repository import TaskQueueRepository

    trunc_msg = (
        f"⚠️ 目录展开文件数超过单任务上限 ({max_files})，已截断保留前 {kept_files} 个文件，其余文件未纳入本次任务。"
    )
    logger.warning(trunc_msg)
    if task_id:
        await TaskQueueRepository.append_log_async(task_id, trunc_msg)
    if truncation_out is not None:
        truncation_out.update({"truncated": True, "max_files": max_files, "kept_files": kept_files})


notify_expansion_truncation = _notify_truncation


async def _collect_folder_level(
    folder_id: str,
    *,
    list_files_raw: Callable,
    remaining_budget: int,
    is_cancelled: Callable[[], bool] | None,
    is_paused: Callable[[], bool] | None,
    seen_file_ids: set[str] | None = None,
) -> tuple[list[ParsedFileItem], list[ParsedFileItem], bool]:
    """分页拉取单个目录层, 达到剩余唯一文件预算时立即停止翻页。"""
    level_files: list[ParsedFileItem] = []
    level_dirs: list[ParsedFileItem] = []
    level_seen_ids: set[str] = set()
    hit_limit = False
    page_token = None
    while True:
        if is_cancelled and is_cancelled():
            raise UploadCancelledError("任务在目录展开阶段已被取消")
        if is_paused and is_paused():
            raise UploadPausedError("任务在目录展开阶段已被暂停")

        items, next_page_token = await list_files_raw(folder_id, page_token=page_token)
        for item in items:
            if item.is_dir:
                level_dirs.append(item)
            elif (seen_file_ids is None or item.file_id not in seen_file_ids) and item.file_id not in level_seen_ids:
                level_seen_ids.add(item.file_id)
                level_files.append(item)

        if len(level_files) >= remaining_budget:
            if next_page_token or len(level_files) > remaining_budget:
                hit_limit = True
            break
        if not next_page_token:
            break
        page_token = next_page_token
    return level_files, level_dirs, hit_limit


async def _crawl_folder_recursive(
    folder_id: str,
    current_path: str,
    depth: int,
    *,
    list_files_raw: Callable,
    max_depth: int,
    max_files: int,
    visited_folder_ids: set[str],
    dir_tree: dict[str, str],
    expanded_files: list[ParsedFileItem],
    seen_file_ids: set[str],
    is_cancelled: Callable[[], bool] | None,
    is_paused: Callable[[], bool] | None,
) -> bool:
    """递归展开单个目录树, 返回是否因达到 max_files 而截断。"""
    from app.core.pipeline.utils import natural_sort_key

    if len(expanded_files) >= max_files:
        return True
    if depth > max_depth:
        logger.warning("递归深度超限 (depth=%d > %d), 终止下钻: %s", depth, max_depth, current_path)
        return False
    if folder_id in visited_folder_ids:
        logger.warning("检测到循环目录引用, 跳过: folder_id=%s path=%s", folder_id, current_path)
        return False
    visited_folder_ids.add(folder_id)
    dir_tree[folder_id] = current_path

    current_level_files, current_level_dirs, truncated = await _collect_folder_level(
        folder_id,
        list_files_raw=list_files_raw,
        remaining_budget=max_files - len(expanded_files),
        is_cancelled=is_cancelled,
        is_paused=is_paused,
        seen_file_ids=seen_file_ids,
    )

    current_level_files.sort(key=lambda x: (natural_sort_key(x.name), x.name))
    for f in current_level_files:
        if f.file_id in seen_file_ids:
            continue
        if len(expanded_files) >= max_files:
            logger.warning("已达到单任务最大文件数上限 (%d), 停止继续展开", max_files)
            return True
        seen_file_ids.add(f.file_id)
        expanded_files.append(f)

    current_level_dirs.sort(key=lambda x: (natural_sort_key(x.name), x.name))
    for d in current_level_dirs:
        if len(expanded_files) >= max_files:
            return True
        sub_truncated = await _crawl_folder_recursive(
            d.file_id,
            f"{current_path}/{d.name}",
            depth + 1,
            list_files_raw=list_files_raw,
            max_depth=max_depth,
            max_files=max_files,
            visited_folder_ids=visited_folder_ids,
            dir_tree=dir_tree,
            expanded_files=expanded_files,
            seen_file_ids=seen_file_ids,
            is_cancelled=is_cancelled,
            is_paused=is_paused,
        )
        truncated = truncated or sub_truncated
    return truncated


async def expand_remote_files(
    raw_items: list[ParsedFileItem],
    *,
    list_files_raw: Callable,
    task_id: str | None = None,
    is_cancelled: Callable[[], bool] | None = None,
    is_paused: Callable[[], bool] | None = None,
    max_files: int = 5000,
    truncation_out: dict[str, Any] | None = None,
) -> tuple:
    """后台单线程异步递归展开目录树 (严禁并发调用 list_files_raw 以免击穿 _CLI_SPAWN_LIMIT=3)。
    安全护栏: max_depth=10, visited_folder_ids 防死循环, 单任务上限 max_files=5000。

    list_files_raw 为注入参数 (services 层提供 `ls --raw` 分页包装, 签名
    `(folder_id, page_token=...) -> (items, next_page_token)`) —— 核心定义算法, IO 不进 core。

    返回 (叶子文件列表, dir_tree) —— dir_tree: {目录file_id: 相对路径} (包D 结构化归档素材,
    根规则目录 = 其名, 深层 = `父路径/名`; 仅包含被展开的目录)。"""
    from app.core.pipeline.utils import natural_sort_key
    from app.db.task_repository import TaskQueueRepository

    if not any(item.is_dir for item in (raw_items or [])):
        sorted_leaf = sorted(raw_items or [], key=lambda x: (natural_sort_key(x.name), x.name))
        if len(sorted_leaf) > max_files:
            sorted_leaf = sorted_leaf[:max_files]
            await _notify_truncation(
                task_id=task_id,
                max_files=max_files,
                kept_files=len(sorted_leaf),
                truncation_out=truncation_out,
            )
        return sorted_leaf, {}

    expanded_files: list[ParsedFileItem] = []
    seen_file_ids: set[str] = set()
    dir_tree: dict[str, str] = {}
    visited_folder_ids: set[str] = set()
    truncated = False

    sorted_raw_items = sorted(raw_items or [], key=lambda x: (natural_sort_key(x.name), x.name))
    for item in sorted_raw_items:
        if is_cancelled and is_cancelled():
            raise UploadCancelledError("任务在目录展开阶段已被取消")
        if is_paused and is_paused():
            raise UploadPausedError("任务在目录展开阶段已被暂停")
        if not item.is_dir and item.file_id in seen_file_ids:
            continue
        if len(expanded_files) >= max_files:
            truncated = True
            break

        if item.is_dir:
            if task_id:
                await TaskQueueRepository.append_log_async(task_id, f"🔍 正在后台递归展开目录 [{item.name}]...")
            dir_truncated = await _crawl_folder_recursive(
                item.file_id,
                item.name,
                1,
                list_files_raw=list_files_raw,
                max_depth=10,
                max_files=max_files,
                visited_folder_ids=visited_folder_ids,
                dir_tree=dir_tree,
                expanded_files=expanded_files,
                seen_file_ids=seen_file_ids,
                is_cancelled=is_cancelled,
                is_paused=is_paused,
            )
            truncated = truncated or dir_truncated
        else:
            seen_file_ids.add(item.file_id)
            expanded_files.append(item)

    if truncated:
        await _notify_truncation(
            task_id=task_id,
            max_files=max_files,
            kept_files=len(expanded_files),
            truncation_out=truncation_out,
        )

    if task_id:
        await TaskQueueRepository.append_log_async(task_id, f"✅ 目录展开完成: 共展开 {len(expanded_files)} 个媒体文件")
    return expanded_files, dir_tree
