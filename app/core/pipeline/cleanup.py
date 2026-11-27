"""统一发布流水线资产清理与善后策略 (Cleanup Strategies)


- 日志口径反转: 成功归档/删除摘要移出任务日志 (改由频道总结卡归档行呈现);
  迁移失败新增进任务日志 (可行动文案, 封顶: 前 10 条逐条 + 剩余计数一行防刷屏);
- delete 与 archive 完全同口径 (失败封顶 + 空目录删除对称适用, 同一安全栏);
- D2 空目录删除: 归档/删除成功后对选中根的后代目录自底向上检测变空即删 (rm 进
  回收站); 非媒体残留视为非空不删并记因; 永不删选中根本身 (rel 无 "/") 与网盘根
  (dir_id 空)。CLI 对空目录 rm 的回收站/配额语义待实机校准 (§5-1)。
"""

import logging
import os
from collections.abc import Callable, Sequence
from typing import Any

from app.core.pipeline.interfaces import CleanupStrategy
from app.core.pipeline.models import PublishContext, PublishItem

logger = logging.getLogger(__name__)

_FAILURE_LOG_CAP = 10


def _cap_failure_lines(failures: list[str]) -> list[str]:
    """封顶: 前 10 条逐条 + 剩余计数一行。"""
    lines = [f"⚠️ {f}" for f in failures[:_FAILURE_LOG_CAP]]
    if len(failures) > _FAILURE_LOG_CAP:
        lines.append(f"…(其余 {len(failures) - _FAILURE_LOG_CAP} 条迁移失败已省略)")
    return lines


async def _append_task_log(context: PublishContext, lines: list[str]) -> None:
    if not (context.task_id and lines):
        return
    from app.db.task_repository import TaskQueueRepository

    for line in lines:
        try:
            await TaskQueueRepository.append_log_async(context.task_id, line)
        except Exception as log_err:
            logger.debug("清理日志追加异常: %s", log_err)


def _collect_affected_dir_scope(
    dir_tree: dict[str, str], affected_items: Sequence[PublishItem]
) -> tuple[set[str], set[str]]:
    """收集当前批次受影响条目的父目录 ID 及其相对路径前缀集合, 避免跨批次全树重复列举。"""
    affected_fids: set[str] = set()
    affected_rels: set[str] = set()
    for item in affected_items:
        pid = str(item.parent_folder_id or "").strip()
        if pid:
            affected_fids.add(pid)
        rel = str(dir_tree.get(pid) or "").strip().replace("\\", "/").strip("/")
        if not rel and item.rel_path and "/" in item.rel_path:
            rel = os.path.dirname(item.rel_path).replace("\\", "/").strip("/")
        if rel:
            affected_rels.add(rel)
    return affected_fids, affected_rels


async def prune_empty_descendant_dirs(
    context: PublishContext,
    *,
    list_files_raw: Callable[..., Any],
    delete_files: Callable[..., Any],
    affected_items: Sequence[PublishItem] | None = None,
) -> tuple[int, list[str]]:
    """D2: 选中根的后代目录自底向上检测变空即删。

    候选 = dir_tree 中 rel_path 含 "/" 的条目 (排除选中根本身; 排除空 dir_id = 网盘根)。
    当传入 affected_items 时仅收敛到本批受影响条目的祖先目录; 已删目录同步自 context.dir_tree 移除。
    深层先删 → 父目录随之变空。非空 (含非媒体残留) 不删并记因; 单目录失败记因不抛。
    返回 (删除数, 跳过/失败明细)。
    """
    dir_tree = dict(context.dir_tree or {})
    candidates: list[tuple[str, str]] = [
        (str(fid), str(rel or "").strip().replace("\\", "/").rstrip("/"))
        for fid, rel in dir_tree.items()
        if str(fid or "").strip() and str(rel or "").strip()
    ]
    candidates = [(fid, rel) for fid, rel in candidates if "/" in rel]
    if affected_items is not None:
        affected_fids, affected_rels = _collect_affected_dir_scope(dir_tree, affected_items)
        candidates = [
            (fid, rel)
            for fid, rel in candidates
            if fid in affected_fids or any(ar == rel or ar.startswith(rel + "/") for ar in affected_rels)
        ]
    # 自底向上: 按路径深度降序 (深层先删, 父目录随之变空)
    candidates.sort(key=lambda pair: pair[1].count("/"), reverse=True)

    pruned = 0
    notes: list[str] = []
    for fid, rel in candidates:
        try:
            empty = True
            page_token = None
            while True:
                items, page_token = await list_files_raw(fid, page_token=page_token)
                if items:
                    empty = False
                    break
                if not page_token:
                    break
            if not empty:
                notes.append(f"目录 {rel} 非空 (残留条目), 跳过删除")
                continue
            await delete_files([fid])
            pruned += 1
            if isinstance(context.dir_tree, dict):
                context.dir_tree.pop(fid, None)
        except Exception as prune_err:
            notes.append(f"目录 {rel}: 空目录清理失败 ({prune_err})")
    return pruned, notes


class NoopCleanupStrategy(CleanupStrategy):
    """空清理策略。

    用于本地发布或 auto_cleanup_mode == "none" 场景，不执行任何删除或归档操作。
    """

    async def cleanup(self, items: list[PublishItem], context: PublishContext) -> str | None:
        return None


class PikPakStructuredArchiveStrategy(CleanupStrategy):
    """PikPak 结构化归档策略。

    按源目录相对路径在 [已发布归档] 下重建目录树后分组 mv。
    无 dir_tree 映射条目 (或旧任务) 回退根级平铺归档；
    单组结构化失败回退平铺，再失败文件保留原位；
    任何归档失败绝不影响任务终态（红线纪律）。
    返回归档摘要字符串（供频道总结卡归档行; D5: 不再写任务日志）。

    CLI IO 依赖一律构造注入 (resolve_archive_folder / archive_files_to_folder /
    archive_files / list_files_raw+delete_files 供空目录清理), 缺注入在使用点报错。
    """

    def __init__(
        self,
        resolve_archive_folder: Callable[..., Any] | None = None,
        archive_files_to_folder: Callable[..., Any] | None = None,
        archive_files: Callable[..., Any] | None = None,
        list_files_raw: Callable[..., Any] | None = None,
        delete_files: Callable[..., Any] | None = None,
    ):
        self._resolve_archive_folder = resolve_archive_folder
        self._archive_files_to_folder = archive_files_to_folder
        self._archive_files = archive_files
        self._list_files_raw = list_files_raw
        self._delete_files = delete_files
        self._archived_item_ids: set[str] = set()

    def _require_deps(self) -> None:
        missing = [
            name
            for name, val in (
                ("resolve_archive_folder", self._resolve_archive_folder),
                ("archive_files_to_folder", self._archive_files_to_folder),
                ("archive_files", self._archive_files),
            )
            if val is None
        ]
        if missing:
            raise RuntimeError(f"PikPakStructuredArchiveStrategy 缺少构造注入依赖: {', '.join(missing)} (门面装配缺失)")

    @staticmethod
    def _strict_moved(res: Any) -> int:
        """严格口径: moved 缺字段/非负 int 视为该组失败 (抛出走回退), 杜绝乐观计数
        —— CLI 异常响应不再被乐观地按条目数计入归档摘要。"""
        raw = res.get("moved") if isinstance(res, dict) else None
        if not isinstance(raw, int) or isinstance(raw, bool) or raw < 0:
            raise RuntimeError("归档响应缺少有效 moved 字段 (严格口径, 视为该组失败)")
        return raw

    async def archive_single_item(self, item: PublishItem, context: PublishContext) -> bool:
        """单文件即时移动：成功上传后立即将远端文件移动到对应归档目录。
        任何移动失败仅警告，严禁阻断后续文件发布。"""
        cleanup_mode = getattr(context, "auto_cleanup_mode", "archive")
        if cleanup_mode in ("none", "delete"):
            return False
        if not self._resolve_archive_folder or not self._archive_files:
            return False

        parent_id = item.parent_folder_id or ""
        dir_tree = context.dir_tree or {}
        rel = dir_tree.get(parent_id)
        if not rel:
            first_rel = item.rel_path or ""
            if "/" in first_rel:
                rel = os.path.dirname(first_rel).replace("\\", "/").strip("/")

        try:
            root_id = await self._resolve_archive_folder("")
            target_folder = root_id
            if rel and root_id and self._archive_files_to_folder:
                for seg in rel.split("/"):
                    seg_clean = seg.strip()
                    if seg_clean:
                        target_folder = await self._resolve_archive_folder(target_folder, seg_clean)
                res = await self._archive_files_to_folder(
                    [item.item_id], folder_id=target_folder, source_parent_id=parent_id
                )
                moved = self._strict_moved(res)
                if moved != 1:
                    raise RuntimeError(f"单文件归档确认数异常 (expected=1, moved={moved})")
            else:
                res = await self._archive_files([item.item_id], source_parent_id=parent_id)
                moved = self._strict_moved(res)
                if moved != 1:
                    raise RuntimeError(f"单文件平铺归档确认数异常 (expected=1, moved={moved})")
            self._archived_item_ids.add(item.item_id)
            logger.info("📦 单文件即时归档成功: %s -> %s", item.name, rel or "根目录")
            return True
        except Exception as e:
            logger.warning("单文件即时归档移动跳过 (将在收尾对账重试): file=%s err=%s", item.name, e)
            return False

    async def cleanup(self, items: list[PublishItem], context: PublishContext) -> str | None:
        if not items and (
            getattr(context, "cleanup_defer_unrelated_dirs", False)
            or not self._list_files_raw
            or not self._delete_files
        ):
            return None

        cleanup_mode = getattr(context, "auto_cleanup_mode", "archive")
        if cleanup_mode in ("none", "delete"):
            return None

        self._require_deps()

        # 终态兜底对账：过滤尚未即时归档移动的文件
        unarchived_items = [i for i in items if i.item_id not in self._archived_item_ids]
        already_moved_count = len(self._archived_item_ids.intersection({i.item_id for i in items}))
        archived_total = already_moved_count
        summary_parts: list[str] = []
        if already_moved_count > 0:
            summary_parts.append(f"即时归档 {already_moved_count}")
        failures: list[str] = []

        if unarchived_items:
            by_parent: dict[str, list[PublishItem]] = {}
            for item in unarchived_items:
                by_parent.setdefault(item.parent_folder_id or "", []).append(item)

            resolve_archive_folder = self._resolve_archive_folder
            archive_files_to_folder = self._archive_files_to_folder
            archive_files = self._archive_files
            assert resolve_archive_folder is not None
            assert archive_files_to_folder is not None
            assert archive_files is not None

            root_id = ""
            try:
                root_id = await resolve_archive_folder("")  # 幂等解析/创建根级 [已发布归档]
            except Exception as root_err:
                logger.warning("归档根目录解析失败, 全部回退平铺归档: %s", root_err)

            dir_tree = context.dir_tree or {}
            for parent_id, group_items in by_parent.items():
                rel = dir_tree.get(parent_id)
                if not rel and group_items:
                    first_rel = group_items[0].rel_path or ""
                    if "/" in first_rel:
                        rel = os.path.dirname(first_rel).replace("\\", "/").strip("/")
                moved = 0
                tag = ""
                try:
                    if rel and root_id:
                        parent = root_id
                        for seg in rel.split("/"):
                            seg_clean = seg.strip()
                            if seg_clean:
                                parent = await resolve_archive_folder(parent, seg_clean)
                        res = await archive_files_to_folder(
                            [i.item_id for i in group_items],
                            folder_id=parent,
                            source_parent_id=parent_id,
                        )
                        moved = self._strict_moved(res)
                        if moved == 0:
                            raise RuntimeError("结构化归档返回 moved=0")
                        if moved < len(group_items):
                            failures.append(
                                f"目录 {rel or parent_id}: 请求归档 {len(group_items)} 个文件，仅确认 {moved} 个"
                            )
                        tag = f"{rel}/ 补移 {moved}" if already_moved_count > 0 else f"{rel}/ {moved}"
                    else:
                        raise RuntimeError("无 dir_tree 映射, 走平铺路径")
                except Exception as struct_err:
                    logger.warning(
                        "结构化归档对账失败, 回退平铺 (文件保留原位, 不影响任务终态): parent=%s err=%s",
                        parent_id,
                        struct_err,
                    )
                    try:
                        res = await archive_files([i.item_id for i in group_items], source_parent_id=parent_id)
                        moved = self._strict_moved(res)
                        if moved == 0:
                            raise RuntimeError("平铺归档返回 moved=0")
                        if moved < len(group_items):
                            failures.append(
                                f"目录 {rel or parent_id}: 平铺补归档请求 {len(group_items)} 个文件，仅确认 {moved} 个"
                            )
                        tag = f"平铺补移 {moved}" if already_moved_count > 0 else f"平铺回退 {moved}"
                    except Exception as flat_err:
                        logger.warning("平铺归档对账回退亦失败 (文件保留原位): parent=%s err=%s", parent_id, flat_err)
                        failures.append(
                            f"目录 {rel or parent_id}: {len(group_items)} 个文件归档失败 "
                            f"(结构化: {struct_err}; 平铺回退: {flat_err}) —— 文件已保留原位"
                        )
                archived_total += moved
                if tag:
                    summary_parts.append(tag)

        # D5: 迁移失败进任务日志 (可行动文案 + 封顶); 成功摘要不再写任务日志 (卡片归档行呈现)
        if failures:
            context.cleanup_had_failures = True
        await _append_task_log(context, _cap_failure_lines(failures))

        # D2: 空目录删除 (与 delete 模式同口径; 失败/跳过记因并封顶)
        if self._list_files_raw and self._delete_files:
            try:
                scoped_items = items if getattr(context, "cleanup_defer_unrelated_dirs", False) else None
                pruned, prune_notes = await prune_empty_descendant_dirs(
                    context,
                    list_files_raw=self._list_files_raw,
                    delete_files=self._delete_files,
                    affected_items=scoped_items,
                )
            except Exception as prune_err:
                logger.warning("空目录清理异常 (不影响任务终态): %s", prune_err)
                pruned, prune_notes = 0, []
            prune_lines: list[str] = []
            if pruned:
                prune_lines.append(f"🧹 已清理 {pruned} 个变空的源目录 (移入回收站)")
            prune_lines.extend(_cap_failure_lines(prune_notes))
            await _append_task_log(context, prune_lines)

        if archived_total:
            return f"🧹 已归档 {archived_total} 个文件至 [已发布归档] ({' · '.join(summary_parts)})"
        return ""


class PikPakDeleteCleanupStrategy(CleanupStrategy):
    """PikPak 移入回收站清理策略。

    用于 auto_cleanup_mode == "delete" 场景，将已发布条目调用 CLI rm 移入回收站。
    逐组安全删除，异常仅记录，严禁向外抛出（红线纪律）。
    D5: 与 archive 完全同口径 —— 成功摘要移出任务日志、失败封顶进日志、空目录删除对称适用。
    """

    def __init__(
        self,
        delete_files: Callable[..., Any] | None = None,
        list_files_raw: Callable[..., Any] | None = None,
    ):
        self._delete_files = delete_files
        self._list_files_raw = list_files_raw

    async def cleanup(self, items: list[PublishItem], context: PublishContext) -> str | None:
        if not items and (
            getattr(context, "cleanup_defer_unrelated_dirs", False)
            or not self._list_files_raw
            or not self._delete_files
        ):
            return None

        cleanup_mode = getattr(context, "auto_cleanup_mode", "delete")
        if cleanup_mode in ("none", "archive"):
            return None

        if self._delete_files is None:
            raise RuntimeError("PikPakDeleteCleanupStrategy 缺少构造注入依赖: delete_files (门面装配缺失)")

        by_parent: dict[str, list[PublishItem]] = {}
        for item in items:
            by_parent.setdefault(item.parent_folder_id or "", []).append(item)

        deleted_total = 0
        failures: list[str] = []
        for parent_id, group in by_parent.items():
            try:
                await self._delete_files([i.item_id for i in group], parent_id=parent_id)
                deleted_total += len(group)
            except Exception as e:
                logger.warning("PikPak 删除文件失败 (文件保留原位): parent=%s err=%s", parent_id, e)
                failures.append(f"目录 {parent_id}: {len(group)} 个文件删除失败 ({e}) —— 文件已保留原位")

        if failures:
            context.cleanup_had_failures = True
        await _append_task_log(context, _cap_failure_lines(failures))

        if self._list_files_raw:
            try:
                scoped_items = items if getattr(context, "cleanup_defer_unrelated_dirs", False) else None
                pruned, prune_notes = await prune_empty_descendant_dirs(
                    context,
                    list_files_raw=self._list_files_raw,
                    delete_files=self._delete_files,
                    affected_items=scoped_items,
                )
            except Exception as prune_err:
                logger.warning("空目录清理异常 (不影响任务终态): %s", prune_err)
                pruned, prune_notes = 0, []
            prune_lines: list[str] = []
            if pruned:
                prune_lines.append(f"🧹 已清理 {pruned} 个变空的源目录 (移入回收站)")
            prune_lines.extend(_cap_failure_lines(prune_notes))
            await _append_task_log(context, prune_lines)

        if deleted_total:
            return f"🗑️ 已将 {deleted_total} 个文件移入回收站"
        return ""
