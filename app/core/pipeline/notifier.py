"""Telegram 频道终态告示策略"""

import asyncio
import logging

from app.core.entity_resolver import resolve_entity
from app.core.pipeline.interfaces import NotificationStrategy
from app.core.pipeline.models import PublishBatch, PublishContext, PublishResult
from app.db.task_repository import TaskQueueRepository

logger = logging.getLogger(__name__)


class TelegramChannelNotifier(NotificationStrategy):
    """Telegram 频道总结卡通知器。

    核心规则：
    1. 仅当 batch.is_folder_publish=True 时发送目录总结卡片；散装单文件发布直接跳过，杜绝刷屏；
    2. 若本批次没有新发布任何文件 (newly_published_count == 0) 且无失败文件，说明该文件夹此前已全量发布并已发送过总结，跳过向频道重复发消息；
    3. 目标频道实体解析采用全局 resolve_entity 工具链，自动拉取 get_dialogs 刷新 access_hash 缓存并带自动重试；
    4. 发送异常仅落日志，严禁抛出影响任务终态（红线纪律）。
    """

    async def notify_batch(self, batch: PublishBatch, context: PublishContext, result: PublishResult) -> None:
        # 规则 1: 仅文件夹打包发布才发送总结卡片
        if not batch.is_folder_publish:
            logger.info("批次 [%s] 非文件夹打包发布，跳过向频道发送总结卡片。", batch.batch_id)
            return

        folder_name = batch.folder_name or context.folder_name or "媒体合集"

        # 规则 2: 全部防重跳过且无失败与超限文件时，静默跳过 (严格对齐 test_publish_skip_summary.py)
        if result.newly_published_count == 0 and not result.failed_files and not result.skipped_files:
            skip_msg = f"⏩ 文件夹 [{folder_name}] 全部媒体已在历史中发布 ({result.published_count}/{result.total_files_count})，跳过发送重复总结消息。"
            logger.info(skip_msg)
            if context.task_id:
                try:
                    await TaskQueueRepository.append_log_async(context.task_id, skip_msg)
                except Exception as log_err:
                    logger.debug("记录跳过日志异常 (非致命忽略): %s", log_err)
            return

        pub_client = context.userbot_client
        if pub_client is None:
            if context.task_id:
                try:
                    await TaskQueueRepository.append_log_async(context.task_id, "⚠️ 总结卡未发送: 执行账号客户端未就绪")
                except Exception as log_err:
                    logger.debug("记录未就绪日志异常 (非致命忽略): %s", log_err)
            return

        # 构建总结卡文本
        tag_name = folder_name.strip().lstrip("#").strip() or "媒体合集"
        header_lines = [
            f"📁 **发布文件夹**: #{tag_name}",
            f"✅ **成功发布**: {result.published_count}/{result.total_files_count} 个媒体文件",
        ]
        if getattr(context, "expansion_truncated", False):
            limit = int(getattr(context, "expansion_limit", 0) or 5000)
            header_lines.append(f"⚠️ **目录展开截断**: 文件数超过单任务上限 ({limit})，仅处理前 {limit} 个文件")
        # 包1 (可选行): 相册形态信息 —— 仅当本批次有相册成功张数时呈现
        if getattr(result, "album_published_count", 0) > 0:
            header_lines.append(f"📷 **相册**: {result.album_published_count} 张 (合成发送)")
        if result.archive_summary:
            clean_summary = result.archive_summary.lstrip("🧹").strip()
            if clean_summary:
                if getattr(result, "archive_failed", False):
                    clean_summary += " (⚠️ 部分文件归档失败，已保留原位)"
                header_lines.append(f"🧹 **归档**: {clean_summary}")
        elif getattr(result, "archive_failed", False):
            header_lines.append("🧹 **归档**: ⚠️ 归档失败")

        if result.skipped_files:
            header_lines.append(f"⏭️ **超限跳过 ({len(result.skipped_files)} 个)**:")
            for sf in result.skipped_files[:10]:
                sf_name = getattr(sf, "file_name", None) or (sf.get("file_name") if isinstance(sf, dict) else str(sf))
                sf_mb = getattr(sf, "file_size_mb", 0) or (sf.get("file_size_mb", 0) if isinstance(sf, dict) else 0)
                header_lines.append(f"  • `{sf_name}` ({sf_mb:.1f}MiB, 超过单文件体积限制)")
            if len(result.skipped_files) > 10:
                header_lines.append(f"  ... (剩余 {len(result.skipped_files) - 10} 个超限跳过文件已截断)")

        if result.failed_files:
            header_lines.append(f"❌ **失败文件列表 ({len(result.failed_files)} 个)**:")
            for ff in result.failed_files[:10]:
                header_lines.append(f"  • {ff}")
            if len(result.failed_files) > 10:
                header_lines.append(f"  ... (剩余 {len(result.failed_files) - 10} 个失败文件已截断)")
        else:
            header_lines.append("❌ **失败文件数**: 0")

        summary_text = "\n".join(header_lines).strip()
        if len(summary_text) > 3900:
            summary_text = summary_text[:3850] + "\n...(超出消息长度限制，已截断)"

        target = context.target_chat_id
        sent_ok = False
        for attempt in range(2):
            try:
                # 实体解析预热：使用标准 resolve_entity，自动刷新私密群 access_hash 缓存
                entity = await resolve_entity(pub_client, target)
                await pub_client.send_message(entity, summary_text)
                sent_ok = True
                break
            except Exception as ex:
                if attempt == 0:
                    logger.debug("第 1 次发送总结卡异常，将在 1.0 秒后重试: %s", ex)
                    await asyncio.sleep(1.0)
                else:
                    logger.warning("发送频道总结卡片异常 (不影响任务终态): %s", ex)
                    if context.task_id:
                        try:
                            await TaskQueueRepository.append_log_async(context.task_id, f"⚠️ 总结卡发送失败: {ex}")
                        except Exception as log_err:
                            logger.debug("记录总结卡失败日志异常: %s", log_err)
        if sent_ok and context.task_id:
            try:
                await TaskQueueRepository.append_log_async(
                    context.task_id, f"📢 已向目标频道 [{context.target_channel_raw}] 发送发布总结卡片。"
                )
            except Exception as log_err:
                logger.debug("记录总结卡成功日志异常 (不影响已发送结果): %s", log_err)

    async def notify_task_summary(self, context: PublishContext, batch_reports: list[PublishResult]) -> None:
        """双卡制: 任务级汇总卡 —— 逐批次卡之后整任务一张, 聚合各批次成败/跳过/归档,
        全局 #folder_name 标头; 发送失败兜底落任务日志 (不影响任务终态)。"""
        if not batch_reports:
            return

        global_name = context.folder_name or "媒体合集"
        tag = global_name.strip().lstrip("#").strip() or "媒体合集"
        header_lines = [f"📊 **任务汇总**: #{tag}"]
        if getattr(context, "expansion_truncated", False):
            limit = int(getattr(context, "expansion_limit", 0) or 5000)
            header_lines.append(f"⚠️ **目录展开截断**: 文件数超过单任务上限 ({limit})，仅处理前 {limit} 个文件")
        total_pub = total_fail = total_skip = 0
        archive_failed = False
        for rep in batch_reports:
            head = (rep.folder_name or "批次").strip().lstrip("#").strip() or "批次"
            row = f"  • #{head}: ✅ {rep.published_count}"
            if rep.skipped_files:
                row += f" · ⏭️ 跳过 {len(rep.skipped_files)}"
            if rep.failed_files:
                row += f" · ❌ 失败 {len(rep.failed_files)}"
            if rep.archive_summary:
                row += f"\n    {rep.archive_summary}"
                if getattr(rep, "archive_failed", False):
                    row += " (⚠️ 部分归档失败)"
                    archive_failed = True
            elif getattr(rep, "archive_failed", False):
                row += "\n    🧹 归档失败 (文件保留原位)"
                archive_failed = True
            header_lines.append(row)
            total_pub += rep.published_count
            total_fail += len(rep.failed_files)
            total_skip += len(rep.skipped_files)
        total_line = f"合计: ✅ {total_pub}"
        if total_skip:
            total_line += f" · ⏭️ {total_skip}"
        if total_fail:
            total_line += f" · ❌ {total_fail}"
        if archive_failed:
            total_line += " · 🧹 归档失败"
        header_lines.append(total_line)

        summary_text = "\n".join(header_lines).strip()
        if len(summary_text) > 3900:
            summary_text = summary_text[:3850] + "\n...(超出消息长度限制，已截断)"

        pub_client = context.userbot_client
        if pub_client is None:
            if context.task_id:
                try:
                    await TaskQueueRepository.append_log_async(
                        context.task_id, "⚠️ 任务汇总卡未发送: 执行账号客户端未就绪"
                    )
                except Exception as log_err:
                    logger.debug("记录任务汇总卡未就绪日志异常: %s", log_err)
            return

        target = context.target_chat_id
        sent_ok = False
        for attempt in range(2):
            try:
                entity = await resolve_entity(pub_client, target)
                await pub_client.send_message(entity, summary_text)
                sent_ok = True
                break
            except Exception as ex:
                if attempt == 0:
                    logger.debug("第 1 次发送任务汇总卡异常，将在 1.0 秒后重试: %s", ex)
                    await asyncio.sleep(1.0)
                else:
                    logger.warning("发送任务汇总卡异常 (不影响任务终态): %s", ex)
                    if context.task_id:
                        try:
                            await TaskQueueRepository.append_log_async(context.task_id, f"⚠️ 任务汇总卡发送失败: {ex}")
                        except Exception as log_err:
                            logger.debug("记录任务汇总卡失败日志异常: %s", log_err)
        if sent_ok and context.task_id:
            try:
                await TaskQueueRepository.append_log_async(
                    context.task_id, f"📢 已向目标频道 [{context.target_channel_raw}] 发送任务汇总卡。"
                )
            except Exception as log_err:
                logger.debug("记录任务汇总卡成功日志异常 (不影响已发送结果): %s", log_err)
