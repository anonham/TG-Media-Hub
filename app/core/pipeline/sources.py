"""统一发布流水线数据源策略 (Sources)"""

import asyncio
import inspect
import logging
import mimetypes
import os
from collections.abc import Callable
from typing import Any

from app.core.pikpak.expander import ParsedFileItem, items_from_snapshot, notify_expansion_truncation
from app.core.pikpak.taxonomy import audit_blocked_reason
from app.core.pipeline.interfaces import ItemSource
from app.core.pipeline.models import DedupQuery, PublishBatch, PublishContext, PublishItem
from app.core.pipeline.utils import format_folder_tag, natural_sort_key, scan_folder_grouped
from app.db.download_repository import DownloadHistoryRepository
from app.db.task_repository import TaskQueueRepository

logger = logging.getLogger(__name__)


class LocalFolderSource(ItemSource):
    """本地文件夹数据源。

    扫描指定本地目录（或 context.folder_path / context.folder_name），
    调用 scan_folder_grouped 获取按子目录分组的有效媒体文件，
    归一化为 List[PublishBatch]，保留子目录分组，并设置 is_folder_publish 与 folder_name。
    """

    def __init__(self, folder_path: str | None = None):
        self.folder_path = folder_path or ""

    def _scan_local_batches_sync(self, context: PublishContext) -> list[PublishBatch]:
        folder = self.folder_path or getattr(context, "folder_path", "") or context.folder_name or ""
        if not folder:
            logger.warning("LocalFolderSource: 目录路径为空")
            return []
        try:
            from app.core.path_utils import validate_safe_folder_path

            folder = validate_safe_folder_path(folder)
        except ValueError as err:
            logger.warning("LocalFolderSource: 目录安全校验未通过: %s", err)
            return []

        grouped_files: dict[str, list[str]] = scan_folder_grouped(folder)
        if not grouped_files:
            logger.info("LocalFolderSource: 在目录 %s 中未找到可发布的媒体文件", folder)
            return []

        batches: list[PublishBatch] = []
        for subfolder, files in grouped_files.items():
            if not files:
                continue

            rel_dir = os.path.relpath(subfolder, folder).replace("\\", "/")
            if rel_dir == ".":
                rel_dir = ""

            if rel_dir:
                # 复合标头还原 (旧基线语义: #根目录 /子路径) —— 子目录批次标头
                # 保留根目录上下文, 频道检索不再丢失所属合集
                batch_folder_name = f"{format_folder_tag(folder)} /{rel_dir}"
                batch_id = rel_dir
            else:
                batch_folder_name = context.folder_name or format_folder_tag(folder)
                batch_id = os.path.basename(os.path.abspath(folder)) or "root"

            items: list[PublishItem] = []
            for file_path in files:
                fname = os.path.basename(file_path)
                try:
                    fsize = os.path.getsize(file_path)
                except OSError:
                    fsize = 0
                file_rel = os.path.relpath(file_path, folder).replace("\\", "/")
                mime, _ = mimetypes.guess_type(file_path)
                items.append(
                    PublishItem(
                        item_id=file_path,
                        name=fname,
                        size_bytes=fsize,
                        rel_path=file_rel,
                        parent_folder_id=subfolder,
                        source_type="local",
                        mime_type=mime,
                        is_dir=False,
                    )
                )

            batch = PublishBatch(
                batch_id=batch_id,
                folder_name=batch_folder_name,
                rel_path=rel_dir,
                is_folder_publish=True,
                items=items,
            )
            batches.append(batch)

        batches.sort(key=lambda b: (natural_sort_key(b.rel_path or ""), b.rel_path or ""))
        return batches

    async def get_batches(self, context: PublishContext) -> list[PublishBatch]:
        return await asyncio.to_thread(self._scan_local_batches_sync, context)

    # ---- Phase 3 hooks (V2 策略化) ----

    def dedup_query(self, item: PublishItem, context: PublishContext) -> DedupQuery:
        return DedupQuery(
            local_path=item.item_id,
            size_bytes=item.size_bytes,
            file_hash=DownloadHistoryRepository.get_hash_by_path(item.item_id),
            source_type="local",
        )

    def oversize_limit_bytes(self, item: PublishItem, context: PublishContext) -> int | None:
        return context.task_limit_bytes or None


class PikPakSnapshotSource(ItemSource):
    """PikPak 网盘快照数据源。

    解析 remote_files 快照列表为 List[PublishItem]，
    从 context.dir_tree 关联并挂载相对目录路径 (rel_path)，
    并依据 context.is_folder_publish 与 context.folder_name 封装为 PublishBatch。
    若包含未展开的目录条目 (is_dir=True)，自动调用展开器并扩充 dir_tree。
    """

    def __init__(
        self,
        remote_files: list[Any] | None = None,
        expander: Callable[..., Any] | None = None,
    ):
        self.remote_files = remote_files
        self.expander = expander

    async def get_batches(self, context: PublishContext) -> list[PublishBatch]:
        raw_files = self.remote_files
        if raw_files is None:
            raw_files = getattr(context, "remote_files", []) or []

        dir_tree = dict(context.dir_tree or {})

        # 如果存在目录条目，进行递归展开
        has_dir = any((isinstance(r, dict) and r.get("is_dir")) or getattr(r, "is_dir", False) for r in raw_files)
        if has_dir:
            expander = self.expander or getattr(context, "expander", None)
            if expander is None:
                # 无 services 动态解析兜底 —— 展开器为唯一注入来源 (门面装配)
                raise ValueError(
                    "PikPakSnapshotSource 含目录条目但缺少 expander 注入 "
                    "(门面应传绑定了 list_files_raw 的 expander, 例如 "
                    "functools.partial(core.pikpak.expander.expand_remote_files, list_files_raw=...))"
                )
            parsed_items = []
            for r in raw_files:
                if isinstance(r, dict):
                    parsed_items.extend(items_from_snapshot([r]))
                elif isinstance(r, ParsedFileItem):
                    parsed_items.append(r)
                elif isinstance(r, PublishItem):
                    parsed_items.append(
                        ParsedFileItem(
                            file_id=r.item_id,
                            name=r.name,
                            is_dir=r.is_dir,
                            size_bytes=r.size_bytes,
                            audit=r.audit or {},
                            thumbnail_link=r.thumbnail_data if isinstance(r.thumbnail_data, str) else None,
                            duration_s=str(r.duration_s) if r.duration_s else None,
                            width=str(r.width) if r.width else None,
                            height=str(r.height) if r.height else None,
                            parent_id=r.parent_folder_id,
                            audio_codec=r.audio_codec,
                            video_codec=r.video_codec,
                        )
                    )
                else:
                    parsed_items.extend(items_from_snapshot([r]))
            truncation_info: dict[str, Any] = {}
            exp_kwargs: dict[str, Any] = {
                "task_id": context.task_id,
                "is_cancelled": context.is_cancelled,
                "is_paused": context.is_paused,
            }
            try:
                sig = inspect.signature(expander)
                if "truncation_out" in sig.parameters or any(
                    p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
                ):
                    exp_kwargs["truncation_out"] = truncation_info
            except (TypeError, ValueError) as sig_err:
                logger.debug("探测 expander 签名失败 (按默认参数调用): %s", sig_err)
            expanded_items, new_tree = await expander(parsed_items, **exp_kwargs)
            if truncation_info.get("truncated"):
                context.expansion_truncated = True
                context.expansion_limit = int(truncation_info.get("max_files") or 5000)
            dir_tree.update(new_tree)
            context.dir_tree.update(new_tree)
            raw_files = expanded_items

        items: list[PublishItem] = []
        for raw in raw_files:
            if isinstance(raw, PublishItem):
                item = raw
            elif isinstance(raw, dict):
                fid = str(raw.get("file_id") or "").strip()
                if not fid:
                    continue
                item = PublishItem(
                    item_id=fid,
                    name=str(raw.get("name") or fid),
                    size_bytes=int(raw.get("size_bytes") or 0),
                    parent_folder_id=str(raw.get("parent_id") or ""),
                    source_type="pikpak",
                    audit=dict(raw.get("audit") or {}),
                    thumbnail_data=raw.get("thumbnail_link") or None,
                    duration_s=float(raw.get("duration_s") or 0.0) if raw.get("duration_s") is not None else 0.0,
                    width=int(raw.get("width") or 0) if raw.get("width") is not None else 0,
                    height=int(raw.get("height") or 0) if raw.get("height") is not None else 0,
                    is_dir=bool(raw.get("is_dir", False)),
                    audio_codec=raw.get("audio_codec"),
                    video_codec=raw.get("video_codec"),
                )
            else:
                fid = getattr(raw, "file_id", getattr(raw, "item_id", ""))
                if not fid:
                    continue
                item = PublishItem(
                    item_id=str(fid),
                    name=getattr(raw, "name", str(fid)),
                    size_bytes=int(getattr(raw, "size_bytes", 0)),
                    parent_folder_id=str(getattr(raw, "parent_id", getattr(raw, "parent_folder_id", "")) or ""),
                    source_type="pikpak",
                    audit=dict(getattr(raw, "audit", {}) or {}),
                    thumbnail_data=getattr(raw, "thumbnail_link", getattr(raw, "thumbnail_data", None)),
                    duration_s=float(getattr(raw, "duration_s", 0.0) or 0.0),
                    width=int(getattr(raw, "width", 0) or 0),
                    height=int(getattr(raw, "height", 0) or 0),
                    is_dir=bool(getattr(raw, "is_dir", False)),
                    audio_codec=getattr(raw, "audio_codec", None),
                    video_codec=getattr(raw, "video_codec", None),
                )

            # 挂载相对目录路径 (rel_path)
            if not item.rel_path:
                if item.parent_folder_id and item.parent_folder_id in dir_tree:
                    parent_rel = str(dir_tree[item.parent_folder_id] or "").strip().replace("\\", "/").rstrip("/")
                    item.rel_path = f"{parent_rel}/{item.name}" if parent_rel else item.name
                elif item.item_id in dir_tree:
                    item.rel_path = str(dir_tree[item.item_id] or "").strip().replace("\\", "/")
                elif context.is_folder_publish and context.folder_name:
                    folder_tag = context.folder_name.strip().replace("\\", "/").rstrip("/")
                    if item.parent_folder_id:
                        dir_tree[item.parent_folder_id] = folder_tag
                        context.dir_tree[item.parent_folder_id] = folder_tag
                    item.rel_path = f"{folder_tag}/{item.name}" if folder_tag else item.name
                else:
                    item.rel_path = item.name

            items.append(item)

        # (BUG-B 后端第二道防线): 载荷按 file_id 显式去重 —— 前端归因视图已结构性
        # 防重, 此处幂等兜底: 重复叶子只发布一次 (发布历史仅一条记录)。
        seen_fids = set()
        deduped_items: list[PublishItem] = []
        for item in items:
            if item.item_id in seen_fids:
                logger.warning("载荷含重复文件 (file_id=%s), 已按 file_id 去重, 只发布一次", item.item_id)
                if context.task_id:
                    await TaskQueueRepository.append_log_async(
                        context.task_id, f"⚠️ 载荷含重复文件 [{item.name}], 已按 file_id 去重"
                    )
                continue
            seen_fids.add(item.item_id)
            deduped_items.append(item)
        items = deduped_items

        if len(items) > 5000:
            # 平铺纯叶子发布 (及兜底): 在空 file_id 过滤与按 file_id 去重后执行 5000 上限截断，保持原始输入顺序
            items = items[:5000]
            if not context.expansion_truncated:
                await notify_expansion_truncation(
                    task_id=context.task_id,
                    max_files=5000,
                    kept_files=len(items),
                    truncation_out=None,
                )
            context.expansion_truncated = True
            context.expansion_limit = 5000

        if not items:
            return []

        # 按直接父目录分组多批次 (语义对齐 utils.scan_folder_grouped —— 每个直接
        # 含媒体的目录一个批次, 逐最低级文件夹发批次卡):
        # - parent 在 dir_tree 有映射 (含选中根本身) → 文件夹批次 (拥有批次卡);
        # - parent 无映射 (散装叶子/旧任务快照) → 散装批次 is_folder_publish=False (不发卡);
        # - 根级直含文件: parent = 选中根 (dir_tree 有根条目) → 拥有自己的批次卡。
        by_parent: dict[str, list[PublishItem]] = {}
        for item in items:
            by_parent.setdefault(item.parent_folder_id or "", []).append(item)

        default_name = context.folder_name or ("PikPak批量发布" if not context.is_folder_publish else "PikPak目录发布")
        batches: list[PublishBatch] = []
        for parent_id, group in by_parent.items():
            group.sort(key=lambda x: (natural_sort_key(x.name), x.name))
            rel = str(dir_tree.get(parent_id) or "").strip().replace("\\", "/").rstrip("/")
            if rel:
                leaf = rel.split("/")[-1].strip()
                batches.append(
                    PublishBatch(
                        batch_id=parent_id,
                        folder_name=format_folder_tag(leaf) if leaf else default_name,
                        rel_path=rel,
                        is_folder_publish=True,
                        items=group,
                    )
                )
            else:
                batches.append(
                    PublishBatch(
                        batch_id=parent_id or "pikpak_loose",
                        folder_name=default_name,
                        rel_path="",
                        is_folder_publish=False,
                        items=group,
                    )
                )
        batches.sort(key=lambda b: (natural_sort_key(b.rel_path or ""), b.rel_path or ""))
        return [b for b in batches if b.items]

    # ---- Phase 3 hooks (V2 策略化; audit 判定为 pikpak-only 钩子的唯一实现) ----

    def reject_item(self, item: PublishItem, context: PublishContext) -> str | None:
        if item.is_dir:
            return f"{item.name or item.item_id}: 目录不支持流式发布"
        if item.size_bytes <= 0:
            return f"{item.name or item.item_id}: 零字节文件, 跳过上传"
        name_lower = (item.name or item.item_id).lower()
        if name_lower.endswith(".d.ts"):
            return f"{item.name or item.item_id}: TypeScript 类型声明文件 (.d.ts)，跳过上传"
        return None

    def audit_block_reason(self, item: PublishItem, context: PublishContext) -> str | None:
        return audit_blocked_reason(item)

    def dedup_query(self, item: PublishItem, context: PublishContext) -> DedupQuery:
        return DedupQuery(source_type="pikpak", remote_file_id=item.item_id)

    def oversize_limit_bytes(self, item: PublishItem, context: PublishContext) -> int | None:
        return context.video_split_threshold_bytes or None
