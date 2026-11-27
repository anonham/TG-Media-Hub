"""统一发布流水线 Phase 3 通用过滤策略 (V2 自 pipeline 拆出 —— 编排层瘦身)

拒绝/audit/防重 全部经 source hooks 单点治理; 超限治理双路径:
- skip_and_report → skipped_files + 占位卡 (文案与 pikpak 现状逐字对齐);
- strict_fail → 熔断 (pikpak 分类学 Fatal; 本地 core.scheduler FatalTaskError);
- split 为 PikPak 流式虚拟分卷专属概念, 已在门面映射为 skip (见 models 常量注释)。
本地超限历史顺序: 防重之前 (Phase 3 内); pikpak 超限历史顺序: 防重之后 (传输循环内) ——
两处共用本模块的 handle_oversize, 顺序保真。
"""

import logging
from typing import Any

from app.core.pikpak.taxonomy import PikPakPublishFatalError
from app.core.pipeline.interfaces import ItemSource
from app.core.pipeline.models import (
    OVERSIZE_POLICY_SKIP_AND_REPORT,
    OVERSIZE_POLICY_STRICT_FAIL,
    PublishBatch,
    PublishContext,
    PublishItem,
)
from app.core.pipeline.runtime import BatchState, Counters, card_throttle_wait, check_signals, emit_progress
from app.core.pipeline.utils import classify_oversize
from app.core.scheduler import FatalTaskError
from app.db.publish_repository import PublishHistoryRepository
from app.db.task_repository import TaskQueueRepository
from app.db.write_pool import submit_db_write
from app.models.publish import OversizedFileInfo

logger = logging.getLogger(__name__)


async def _log_dedup_skip(
    item: PublishItem,
    context: PublishContext,
    *,
    is_pikpak: bool,
    by_current_task: bool,
) -> None:
    """防重跳过日志分发 helper (抽离以收敛 filter_batch 圈复杂度)。"""
    if by_current_task:
        if is_pikpak:
            logger.info("PikPak 当前任务已发布 (断点跳过): file_id=%s name=%s", item.item_id, item.name)
            if context.task_id:
                await TaskQueueRepository.append_log_async(
                    context.task_id, f"⏩ [{item.name}] 当前任务已成功发布过，断点续传跳过"
                )
        else:
            skip_msg = f"⏩ [断点续传跳过] {item.name} (当前任务已发布)"
            logger.info(skip_msg)
            if context.task_id:
                await TaskQueueRepository.append_log_async(context.task_id, skip_msg)
    elif is_pikpak:
        logger.info("PikPak 防重命中, 跳过: file_id=%s name=%s", item.item_id, item.name)
        if context.task_id:
            dup_msg = f"⏭️ [{item.name}] 已发布过 (防重命中), 跳过"
            await TaskQueueRepository.append_log_async(context.task_id, dup_msg)
    else:
        skip_msg = f"⏩ [跳过已发布] {item.name}"
        logger.info(skip_msg)
        if context.task_id:
            await TaskQueueRepository.append_log_async(context.task_id, skip_msg)


class BatchFilter:
    """Phase 3 通用过滤器 (source hooks 的编排侧消费者)"""

    def __init__(self, source: ItemSource, placeholder_sender: Any | None = None) -> None:
        self.source = source
        self.placeholder_sender = placeholder_sender

    async def _send_oversize_placeholder_card(
        self,
        item: PublishItem,
        context: PublishContext,
        oversized_info: OversizedFileInfo,
    ) -> None:
        """发送超限缺集说明卡 (受每任务 10+1 封顶约束; 失败发送不占额度)."""
        from app.core.pipeline.placeholder import OVERFLOW_TEXT, PER_ITEM_CARD_CAP

        card_key = f"{item.item_id}:{oversized_info.oversize_kind}"
        if card_key in context.sent_placeholder_keys:
            logger.info("⏭️ 占位说明卡已发送过，跳过重复发卡: %s", card_key)
            return

        sender = self.placeholder_sender
        sent_count = sender.cards_sent_count() if sender is not None else context.placeholder_cards_sent
        if sent_count >= PER_ITEM_CARD_CAP:
            overflow_sent = sender.overflow_sent() if sender is not None else context.placeholder_overflow_sent
            if not overflow_sent:
                await card_throttle_wait(context, min(await context.get_current_send_delay_seconds(), 1.5))
                try:
                    await context.userbot_client.send_message(context.target_chat_id, OVERFLOW_TEXT)
                    if sender is not None:
                        sender.record_external_overflow_card()
                    else:
                        context.placeholder_overflow_sent = True
                except Exception as ex:
                    logger.warning("发送超限溢出汇总卡异常: %s", ex)
            elif sender is not None:
                sender.record_external_overflow_skip()
            if context.task_id:
                await TaskQueueRepository.append_log_async(
                    context.task_id, f"⏳ 失败占位卡已达封顶 ({PER_ITEM_CARD_CAP}), 后续超限仅计数: {item.name}"
                )
            return

        # 卡节流在 try 外: card_throttle_wait 的暂停异常从排水路径收口 (下一检查点),
        # 绝不能落入下方 except Exception 被当卡片失败吞掉
        await card_throttle_wait(context, min(await context.get_current_send_delay_seconds(), 1.5))
        try:
            placeholder_text = (
                f"⚠️ **【缺集说明】** 媒体文件 `{item.name}` "
                f"({oversized_info.file_size_mb:.2f} MiB) 超过单文件体积限制，已跳过上传。"
            )
            await context.userbot_client.send_message(context.target_chat_id, placeholder_text)
            if sender is not None:
                sender.record_external_per_item_card()
            else:
                context.placeholder_cards_sent += 1
            context.sent_placeholder_keys.add(card_key)
            if context.task_id:
                submit_db_write(
                    TaskQueueRepository.merge_params,
                    context.task_id,
                    {"sent_placeholder_keys": list(context.sent_placeholder_keys)},
                )
        except Exception as ex:
            logger.warning("发送缺集占位说明卡异常: %s", ex)

    async def handle_oversize(
        self,
        item: PublishItem,
        context: PublishContext,
        limit_bytes: int,
        *,
        is_pikpak: bool,
        counters: Counters,
        b: BatchState,
        all_skipped: list[Any],
        force_skip: bool = False,
    ) -> bool:
        """超限治理: strict_fail → 熔断; skip_and_report → skipped_files + 占位卡,
        返回 True (调用方 continue); split (pikpak 流式专属) → 不跳过, 返回 False 继续
        虚拟分卷传输。本地超限旧行为 (failed_files + FailureLogger SizeGuard + 任务 failed)
        已按 D3 废除 —— 超限是 skip 不是 failure, 不再写失败归因。

        force_skip (相册分支专用, D1): 超限图片无论任务策略一律占位卡/跳过 —— 相册为
        photo 形态, 与虚拟分卷 (split) 互斥, 绝不入组。"""
        if not force_skip and context.oversize_policy == OVERSIZE_POLICY_STRICT_FAIL:
            fail_msg = (
                f"文件 [{item.name}] ({item.size_bytes / (1024 * 1024):.2f} MiB) 超过单文件体积限制，严格模式终止任务"
            )
            if context.task_id:
                strict_msg = f"❌ {fail_msg}"
                await TaskQueueRepository.append_log_async(context.task_id, strict_msg)
            if is_pikpak:
                raise PikPakPublishFatalError("quota", fail_msg)
            raise FatalTaskError(fail_msg)

        if not force_skip and context.oversize_policy != OVERSIZE_POLICY_SKIP_AND_REPORT:
            # split (PikPak 流式默认): 引擎按 split_threshold 虚拟分卷续传, 绝不跳过
            return False

        limit_base = (
            context.task_limit_bytes if (is_pikpak and item.size_bytes > context.task_limit_bytes) else limit_bytes
        )
        oversize_kind, _ = classify_oversize(item.size_bytes, limit_base, context.is_premium)
        oversized_info = OversizedFileInfo(
            file_name=item.name,
            rel_path=f"pikpak://{item.parent_folder_id}/{item.name}" if is_pikpak else item.rel_path,
            file_size_bytes=item.size_bytes,
            file_size_mb=round(item.size_bytes / (1024 * 1024), 2),
            file_size_gb=round(item.size_bytes / (1024 * 1024 * 1024), 2),
            limit_mb=round(limit_base / (1024 * 1024), 2),
            exceeded_mb=round((item.size_bytes - limit_base) / (1024 * 1024), 2),
            oversize_kind=oversize_kind or ("premium_limit" if context.is_premium else "non_premium_limit"),
        )
        b.skipped.append(oversized_info)
        all_skipped.append(oversized_info)
        skip_msg = f"⏭️ [跳过超限文件] {item.name} ({oversized_info.file_size_mb:.2f} MiB 超过单文件体积限制)"
        logger.info(skip_msg)
        if context.task_id:
            await TaskQueueRepository.append_log_async(context.task_id, skip_msg)

        # 占位卡: 本地与 pikpak 语义/文案逐字对齐 (缺集说明卡; 严禁写入 PublishHistory);
        # force_skip 无论任务策略始终发卡 —— 占位卡/跳过为 明文口径
        if (context.send_placeholder_card or force_skip) and context.userbot_client:
            await self._send_oversize_placeholder_card(item, context, oversized_info)

        emit_progress(
            context,
            current=counters.published,
            total=counters.total_files,
            # 进度事件名保真: 本地旧实现为纯文件名, pikpak 旧实现带 ⏭️ 前缀
            name=item.name if not is_pikpak else f"⏭️ [跳过超限] {item.name}",
            percent=counters.percent(),
            force_db=is_pikpak,
            done_bytes=counters.done_bytes,
            all_bytes=context.total_bytes_sum,
            dl_speed=0.0 if is_pikpak else None,
            concurrency=(context.upload_concurrency or 2) if is_pikpak else None,
        )
        return True

    async def filter_batch(
        self,
        batch: PublishBatch,
        context: PublishContext,
        is_pikpak: bool,
        counters: Counters,
        b: BatchState,
        all_skipped: list[Any],
    ) -> list[PublishItem]:
        """逐条: reject_item → audit_block_reason → (本地)超限 → dedup_query 防重。
        pikpak 超限不在本循环 (历史顺序: 防重之后, 由传输循环调用 handle_oversize)。"""
        publishable: list[PublishItem] = []
        for item in batch.items:
            check_signals(context)

            reject_reason = self.source.reject_item(item, context)
            if reject_reason:
                b.failed.append(reject_reason)
                # 包1 (K1): 结构化失败明细 (诊断视图失败表) —— 全前缀匹配剥离 (非朴素
                # split(": "), 抗文件名含 ": ")。name 基准必须与 hook 文案构造逐字一致
                # (sources.reject_item 用 item.name or item.item_id); 不命中兜底 reason=全文
                # (未来 hook 文案形态变化时优雅降级, file_name 仍单独正确)。
                reject_name = item.name or item.item_id
                reject_prefix = f"{reject_name}: "
                reject_detail = (
                    reject_reason[len(reject_prefix) :] if reject_reason.startswith(reject_prefix) else reject_reason
                )
                b.failed_details.append(
                    {
                        "file_name": reject_name,
                        "reason": reject_detail,
                        "client_label": context.account_display,
                    }
                )
                continue

            block_reason = self.source.audit_block_reason(item, context)
            if block_reason:
                logger.warning(
                    "🚫 audit 拦截 (UI 标红数据源): file_id=%s name=%s reason=%s",
                    item.item_id,
                    item.name,
                    block_reason,
                )
                if context.task_id:
                    block_msg = f"🚫 跳过 [{item.name}] (audit 拦截): {block_reason}"
                    await TaskQueueRepository.append_log_async(context.task_id, block_msg)
                b.failed.append(f"{item.name}: {block_reason}")
                # 包1 (K1): block_reason 原始变量在分支作用域内 —— 免解析直填结构化明细
                # (与 reject 分支的前缀剥离路径对照)。
                b.failed_details.append(
                    {
                        "file_name": item.name,
                        "reason": block_reason,
                        "client_label": context.account_display,
                    }
                )
                continue

            if not is_pikpak:
                limit_bytes = self.source.oversize_limit_bytes(item, context)
                # 单文件超限谓词为 > (等值文件可不分卷直传) —— 与相册入组门槛 >= (D1 严格
                # "低于"语义, 见 pipeline._transport_pikpak / engines.send_album) 是两个有意
                # 不同的谓词, 勿"统一" (J2)。
                if limit_bytes and item.size_bytes > limit_bytes:
                    skipped = await self.handle_oversize(
                        item, context, limit_bytes, is_pikpak=False, counters=counters, b=b, all_skipped=all_skipped
                    )
                    if skipped:
                        continue

            dq = self.source.dedup_query(item, context)
            pub_info = PublishHistoryRepository.get_published_info(
                context.target_chat_id,
                dq.local_path,
                dq.size_bytes,
                dq.file_hash,
                source_type=dq.source_type,
                remote_file_id=dq.remote_file_id,
            )
            was_published = pub_info is not None
            # 任务级断点防御: 若该文件在当前任务自身生命周期内已成功发布并记入账本，
            # 无论任务初始是否勾选 force_republish，后续重试/恢复时均无条件跳过，杜绝二次重传刷屏
            already_published_by_current_task = bool(
                pub_info is not None and context.task_id and pub_info.task_id == context.task_id
            )

            if was_published and (already_published_by_current_task or not context.force_republish):
                # 包4 (J7): 防重命中明细聚合 (诊断视图防重表数据源); 计数语义不变
                b.dedup_hits.append({"file_name": item.name, "size_bytes": item.size_bytes})
                counters.published += 1
                b.published += 1
                counters.done_bytes += item.size_bytes
                context.completed_files_count = counters.published
                context.completed_bytes = counters.done_bytes
                await _log_dedup_skip(
                    item,
                    context,
                    is_pikpak=is_pikpak,
                    by_current_task=already_published_by_current_task,
                )
                emit_progress(
                    context,
                    current=counters.published,
                    total=counters.total_files,
                    name=item.name,
                    percent=counters.percent(),
                    force_db=False,
                    done_bytes=counters.done_bytes,
                    all_bytes=context.total_bytes_sum,
                )
                continue

            if was_published and context.task_id:
                republish_msg = (
                    f"🔁 [{item.name}] 已发布过, 强制重传 (忽略防重)"
                    if is_pikpak
                    else f"🔁 [强制重传] {item.name} (忽略已发布防重)"
                )
                logger.warning(republish_msg)
                await TaskQueueRepository.append_log_async(context.task_id, republish_msg)
            publishable.append(item)
        return publishable
