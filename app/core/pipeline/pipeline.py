"""统一发布流水线编排核心 (PublishPipeline)

标准化 6 阶段生命周期 (V2: Phase 1/3 已拆出 preflight.py / filtering.py —— 编排层只保留
介质无关的传输循环与批次汇合):
Phase 1: Pre-flight (preflight.phase1_preflight; 探针经 channel_probe 注入)。
Phase 2: Source 批次划分。
Phase 3: 通用过滤 (filtering.BatchFilter; source hooks)。
Phase 4: 统一传输循环 (engine 返回 TransportOutcome; 失败单点 _handle_transport_failure
         策略表: not_found 逐文件隔离 / 分类学上抛 / 控制信号穿透 / 本地域归因)。
Phase 6: 批次清理; Phase 5: 批次告示。
"""

import asyncio
import logging
import time
from collections.abc import Callable
from typing import Any

from app.config import settings
from app.core.failure_logger import FailureLogger
from app.core.media_sniff import TS_AMBIGUOUS_EXTENSIONS
from app.core.pikpak.taxonomy import PikPakPublishError
from app.core.pipeline.cleanup import NoopCleanupStrategy
from app.core.pipeline.engines.album_delivery import UnconfirmedAlbumError
from app.core.pipeline.engines.exceptions import TransportError
from app.core.pipeline.filtering import BatchFilter
from app.core.pipeline.interfaces import (
    CleanupStrategy,
    ItemSource,
    MediaTransportEngine,
    NotificationStrategy,
)
from app.core.pipeline.models import (
    PublishContext,
    PublishItem,
    PublishResult,
    TransportOutcome,
)
from app.core.pipeline.notifier import TelegramChannelNotifier
from app.core.pipeline.placeholder import PlaceholderCardSender
from app.core.pipeline.preflight import phase1_preflight
from app.core.pipeline.runtime import BatchState, Counters, check_signals, emit_progress, wait_with_signals
from app.core.pipeline.utils import (
    _is_premium_lost_failure,
    _media_ext,
    describe_upload_failure,
    is_file_parts_invalid,
    is_image_item,
)
from app.core.scheduler import RetryableTaskError, UploadCancelledError, UploadPausedError
from app.db.download_repository import DownloadHistoryRepository
from app.db.publish_repository import PlaceholderSlotRepository, PublishHistoryRepository
from app.db.task_repository import TaskQueueRepository

logger = logging.getLogger(__name__)


class PublishPipeline:
    """统一发布流水线编排器 (Template Method)"""

    def __init__(
        self,
        source: ItemSource,
        engine: MediaTransportEngine,
        cleanup: CleanupStrategy | None = None,
        notifier: NotificationStrategy | None = None,
        channel_probe: Callable[..., Any] | None = None,
    ):
        self.source = source
        self.engine = engine
        self.cleanup = cleanup or NoopCleanupStrategy()
        self.notifier = notifier or TelegramChannelNotifier()
        # (J6 修订): 写权限探针由门面构造注入 (ChannelInspector.inspect_channel)。
        # app/core 禁止依赖 app.services (test_layering AST 守卫), 旧的动态 import/getattr
        # 洗白一并废除; 裸构造 (仅测试路径) 未注入时跳过写权限探针。
        self.channel_probe = channel_probe
        self._batch_filter = BatchFilter(source)
        # run 级发卡器 (J10) —— execute 时实例化; 直构/单测路径为 None 时不发卡
        self._placeholder_sender: PlaceholderCardSender | None = None
        # 占位槽协调器 (终态/翻转/自愈; 无状态, 随 execute 同步实例化)
        self._slot_coordinator: Any | None = None

    # ------------------------------------------------------------------
    # Phase 4: 统一传输循环
    # ------------------------------------------------------------------

    async def _handle_transport_failure(
        self, exc: Exception, item: PublishItem, context: PublishContext, b: BatchState, *, is_pikpak: bool
    ) -> None:
        """失败策略表 (单点): 控制信号穿透; 本地域会话失效穿透上报; pikpak not_found 逐文件
        隔离、其余分类学与未知异常上抛 (调度器处置, 与旧 pikpak 路径一致); 本地域异常归因
        入账后批内失败继续。"""
        client_label = context.account_display
        underlying: Exception = exc
        if isinstance(exc, TransportError):
            client_label = exc.client_label or client_label
            if exc.cause is not None and isinstance(exc.cause, Exception):
                underlying = exc.cause

        if isinstance(
            underlying, (UploadCancelledError, UploadPausedError, asyncio.CancelledError, RetryableTaskError)
        ):
            raise underlying

        if isinstance(underlying, PikPakPublishError):
            if underlying.category == "not_found":
                logger.warning("PikPak 文件缺失, 跳过该文件继续: %s", underlying.user_detail)
                if context.task_id:
                    miss_msg = f"❌ [{item.name or item.item_id}] {underlying.user_detail}"
                    await TaskQueueRepository.append_log_async(context.task_id, miss_msg)
                b.failed.append(f"{item.name or item.item_id}: {underlying.user_detail}")
                # 包4 (J7): 结构化失败明细 (诊断视图失败表; client_label = 主账号)
                b.failed_details.append(
                    {
                        "file_name": item.name or item.item_id,
                        "reason": str(underlying.user_detail),
                        "client_label": context.account_display,
                        "file_path": f"pikpak://{item.parent_folder_id}/{item.name}",
                        "file_size": item.size_bytes,
                    }
                )
                # 源文件缺失属"确认不可发布"分类 (J2) → 占位卡直接以
                # ❌ 终态形态发送 (槽 failed_terminal), 不再等待无意义的回填窗口
                await self._emit_failure_card(item, context, str(underlying.user_detail), terminal=True)
                return
            raise underlying
        if is_pikpak:
            # 未知异常 (非分类学域): 与旧 pikpak 传输路径语义一致 —— 原样上抛
            raise underlying

        from app.core.session_errors import is_session_invalid_error, report_session_invalid

        is_bot_pool = isinstance(exc, TransportError) and client_label.startswith("Bot Pool")
        if is_session_invalid_error(underlying) and not is_bot_pool:
            await report_session_invalid(context.task_id, underlying, account_id=getattr(context, "account_id", None))
            raise underlying
        failure_exc = describe_upload_failure(underlying, item.size_bytes, context.is_premium)
        if _is_premium_lost_failure(underlying, context.is_premium):
            logger.warning("⚠️ 文件 [%s] 上传失败疑似账号 Premium 权益失效: %s", item.name, underlying)
        logger.error("❌ 发布文件失败: %s -> %s", item.name, underlying)
        if context.task_id:
            fail_msg = f"❌ 发布失败: {item.name} -> {failure_exc}"
            await TaskQueueRepository.append_log_async(context.task_id, fail_msg)
        FailureLogger.log_failure(
            item.item_id,
            context.target_chat_id,
            failure_exc,
            client_type=client_label,
            task_id=context.task_id,
        )
        b.failed.append(item.name)
        # 包4 (J7): 结构化失败明细 (诊断视图失败表)
        b.failed_details.append(
            {
                "file_name": item.name,
                "reason": str(failure_exc),
                "client_label": client_label,
                "file_path": item.item_id,
                "file_size": item.size_bytes,
            }
        )
        # 单文件失败 → 1:1 ⏳ Photo 占位卡 (backfillable 槽);
        # FILE_PARTS_INVALID 清除断点后仍失败属"确认不可发布" (J2) → 直接终态形态
        await self._emit_failure_card(
            item,
            context,
            str(failure_exc),
            terminal=is_file_parts_invalid(exc),
        )

    async def _emit_failure_card(
        self, item: PublishItem, context: PublishContext, reason: str, *, terminal: bool = False
    ) -> None:
        """失败占位卡统一出口 (J12 双重防御 —— 发卡器内部吞异常, 此处再兜一层)"""
        sender = self._placeholder_sender
        if sender is None:
            return
        try:
            await sender.send_failure_card(item, context, reason, terminal=terminal)
        except Exception as card_err:
            logger.warning("失败占位卡发送异常 (不阻塞主流程): %s", card_err)

    async def _record_success(
        self,
        item: PublishItem,
        context: PublishContext,
        counters: Counters,
        b: BatchState,
        *,
        msg_id: int,
        is_pikpak: bool,
        dl_speed: float = 0.0,
        history_recorded: bool = False,
    ) -> None:
        if not history_recorded:
            if is_pikpak:
                history_path = f"pikpak://{item.parent_folder_id}/{item.name}"
                PublishHistoryRepository.record(
                    context.target_chat_id,
                    history_path,
                    item.size_bytes,
                    "",
                    source_type="pikpak",
                    remote_file_id=item.item_id,
                    tg_message_id=msg_id,
                    task_id=context.task_id or "",
                )
            else:
                PublishHistoryRepository.record(
                    context.target_chat_id,
                    item.item_id,
                    item.size_bytes,
                    DownloadHistoryRepository.get_hash_by_path(item.item_id),
                    tg_message_id=msg_id,
                    task_id=context.task_id or "",
                )
        counters.published += 1
        b.published += 1
        counters.newly += 1
        b.newly += 1
        b.successful.append(item)
        counters.done_bytes += item.size_bytes
        context.completed_files_count = counters.published
        context.completed_bytes = counters.done_bytes
        conc = (context.upload_concurrency or 2) if is_pikpak else None
        emit_progress(
            context,
            current=counters.published,
            total=counters.total_files,
            name=item.name,
            percent=counters.percent(),
            force_db=True,
            done_bytes=counters.done_bytes,
            all_bytes=context.total_bytes_sum,
            dl_speed=dl_speed or (0.0 if is_pikpak else None),
            concurrency=conc,
            concurrency_total=conc,
        )
        if is_pikpak and context.task_id:
            try:
                await TaskQueueRepository.clear_part_checkpoint_async(context.task_id, item.item_id)
                await TaskQueueRepository.merge_params_async(
                    context.task_id,
                    {
                        "throttle_strike_count": 0,
                        "data_risk_strike_count": 0,
                    },
                    reset_retry_count=True,
                )
            except Exception as reset_err:
                logger.debug("复位限流计数与检查点异常 (非致命): %s", reset_err)

        if self.cleanup:
            try:
                await self.cleanup.archive_single_item(item, context)
            except Exception as move_err:
                logger.warning("单文件即时归档移动异常 (不阻塞主流程): %s", move_err)

    async def _record_empty_outcome_failure(self, item: PublishItem, context: PublishContext, b: BatchState) -> None:
        """J5 (包1e): 引擎未返回有效消息 ID (message_ids 为空 / 末条为 0 —— padding、短
        sent_list、零体积空分卷等场景) —— 计入失败且不落 PublishHistory: 杜绝"未发送却
        入库 (tg_message_id=0)、未来被防重挡住"的静默丢失。批次继续, 不熔断。"""
        name = item.name or item.item_id
        if context.task_id:
            warn_msg = f"⚠️ [{name}] 发送未返回消息 ID，计入失败 (不落库, 可重试)"
            await TaskQueueRepository.append_log_async(context.task_id, warn_msg)
        b.failed.append(f"{name}: 发送未返回消息 ID (tg_message_id 缺失)")
        # 包4 (J7): 空 outcome 失败同样进结构化明细 (诊断视图失败表)
        b.failed_details.append(
            {
                "file_name": name,
                "reason": "发送未返回消息 ID (tg_message_id 缺失)",
                "client_label": context.account_display,
                "file_path": f"pikpak://{item.parent_folder_id}/{item.name}"
                if context.source_type == "pikpak"
                else item.item_id,
                "file_size": item.size_bytes,
            }
        )
        # 空 outcome 也是真实断裂 → 失败占位卡
        await self._emit_failure_card(item, context, "发送未返回消息 ID (tg_message_id 缺失)")

    async def _route_album_outcomes(
        self,
        items: list[PublishItem],
        outcomes: list[TransportOutcome],
        context: PublishContext,
        counters: Counters,
        b: BatchState,
        *,
        is_pikpak: bool,
    ) -> None:
        """相册逐项 outcome 统一路由。

                - message_ids 有效 → 正常入账;
                - message_ids 空 + error_detail 非空 → 逐项隔离失败: 成功子集非空时逐条 1:1 卡
        成功子集为 0 时整组合并 1 张汇总卡 (严禁逐条连发);
                - message_ids 空 + error_detail 空 → 既有 J5 哨兵 (padding/短列表) 语义不变。"""
        album_success = 0
        failed_with_detail: list[int] = []
        empty_outcomes: list[PublishItem] = []
        success_members: list[tuple[Any, ...]] = []
        retryable_detail: str | None = None
        retryable_wait_s = 0
        retryable_flood_marked: bool | None = None
        for idx, sitm in enumerate(items):
            outcome = outcomes[idx] if idx < len(outcomes) else None
            if outcome is not None and outcome.message_ids and outcome.message_ids[-1]:
                await self._record_success(
                    sitm,
                    context,
                    counters,
                    b,
                    msg_id=outcome.message_ids[-1],
                    is_pikpak=is_pikpak,
                    dl_speed=outcome.speed_mb_s,
                    history_recorded=outcome.history_recorded,
                )
                album_success += 1
                success_members.append((sitm, outcome.message_ids[-1]))
            elif outcome is not None and getattr(outcome, "retryable_error", False):
                if retryable_detail is None:
                    retryable_detail = outcome.error_detail or "FloodWait"
                retryable_wait_s = max(retryable_wait_s, int(getattr(outcome, "retry_after_s", 0) or 0))
                marked = bool(getattr(outcome, "flood_marked", False))
                retryable_flood_marked = (
                    marked if retryable_flood_marked is None else (retryable_flood_marked and marked)
                )
            elif outcome is not None and outcome.error_detail:
                failed_with_detail.append(idx)
            else:
                # J5: 该子项无有效消息 ID (padding/短 sent_list/D1 引擎二次拦截) → 计入失败零入库
                empty_outcomes.append(sitm)
        for sitm in empty_outcomes:
            await self._record_empty_outcome_failure(sitm, context, b)
        b.album_published += album_success
        if album_success and context.task_id:
            album_ok = f"✅ [相册发布成功] {album_success} 张图片"
            await TaskQueueRepository.append_log_async(context.task_id, album_ok)
        # (R2/): 组发成功后对命中槽成员 best-effort 翻转 ✅ (含终态卡复活)
        coordinator = self._slot_coordinator
        if coordinator is not None and success_members:
            try:
                await coordinator.flip_after_group_send(success_members, context)
            except Exception as flip_err:
                logger.warning("组发成功翻转异常 (不阻塞主流程): %s", flip_err)

        if retryable_detail is not None:
            raise RetryableTaskError(
                f"相册发送遇 FloodWait (已入账先送达成员): {retryable_detail}",
                retry_after=float(retryable_wait_s) if retryable_wait_s > 0 else None,
                flood_client_marked=bool(retryable_flood_marked),
            )

        if not failed_with_detail:
            return

        if not album_success:
            # R3/D2: 成功子集 == 0 → 整组失败, 合并 1 张汇总卡 (文本形态, R1), 逐条卡严禁
            sender = self._placeholder_sender
            if sender is not None:
                try:
                    await sender.send_group_summary_card(
                        [items[i] for i in failed_with_detail],
                        context,
                        outcomes[failed_with_detail[0]].error_detail,
                    )
                except Exception as card_err:
                    logger.warning("整组失败汇总卡发送异常: %s", card_err)

        for i in failed_with_detail:
            sitm = items[i]
            reason = outcomes[i].error_detail
            client_label = outcomes[i].client_label or context.account_display
            if context.task_id:
                await TaskQueueRepository.append_log_async(
                    context.task_id, f"❌ [{sitm.name or sitm.item_id}] 相册成员失败: {reason}"
                )
            if not is_pikpak:
                FailureLogger.log_failure(
                    sitm.item_id,
                    context.target_chat_id,
                    RuntimeError(reason) if isinstance(reason, str) else reason,
                    client_type=client_label,
                    task_id=context.task_id,
                )
            b.failed.append(f"{sitm.name or sitm.item_id}: {reason}")
            b.failed_details.append(
                {
                    "file_name": sitm.name or sitm.item_id,
                    "reason": reason,
                    "client_label": client_label,
                    # 占位槽身份字段 (诊断富化 states_for_identities 用)
                    "file_path": f"pikpak://{sitm.parent_folder_id}/{sitm.name}" if is_pikpak else sitm.item_id,
                    "file_size": sitm.size_bytes,
                }
            )
            # D2 (已裁决): 仅"部分成员失败、其余成功组发"场景才逐条发独立占位卡
            if album_success:
                await self._emit_failure_card(sitm, context, reason)

    async def _handle_backfill_claim_conflict(
        self,
        item: PublishItem,
        context: PublishContext,
        identity: str,
        size: int,
    ) -> tuple[str, int, Any] | None:
        """CAS 抢占失败后按槽现态分流: lost→补发, published→校验非强制重传且内容匹配后防重跳过, editing→无故障预算退避."""
        latest = PlaceholderSlotRepository.get_by_identity(context.target_chat_id, identity, size)
        if latest is None:
            return None
        state = latest.get("state")
        if state == "published":
            if context.force_republish:
                return None
            dq = self._batch_filter.source.dedup_query(item, context)
            pub_info = PublishHistoryRepository.get_published_info(
                context.target_chat_id,
                dq.local_path,
                dq.size_bytes,
                dq.file_hash,
                source_type=dq.source_type,
                remote_file_id=dq.remote_file_id,
            )
            if (
                pub_info is None
                or not pub_info.tg_message_id
                or pub_info.tg_message_id != int(latest.get("real_msg_id") or 0)
                or (dq.source_type != "pikpak" and (not dq.file_hash or pub_info.file_hash != dq.file_hash))
            ):
                return None
            return ("already_published", int(latest["id"]), int(latest.get("real_msg_id") or 0))
        if state == "editing":
            attempted_at = float(latest.get("edit_attempted_at") or 0.0)
            if attempted_at > 0 and (time.time() - attempted_at) >= 10.0:
                busy_msg = f"⏳ [{item.name}] 占位卡仍由其他任务回填中，等待重试结果"
                logger.warning(busy_msg)
                if context.task_id:
                    await TaskQueueRepository.append_log_async(context.task_id, busy_msg)
            else:
                logger.info("[%s] 占位卡正在回填中 (slot=%s)，退避 15s 等待", item.name, latest["id"])
            raise RetryableTaskError(
                f"[{item.name}] 占位卡正在回填中 (slot={latest['id']})",
                retry_after=15.0,
                count_retry=False,
                is_flood_wait=False,
            )
        return None

    async def _try_backfill(self, item: PublishItem, context: PublishContext) -> tuple[str, int, Any] | None:
        """(S4/): 发送前占位槽回填探测。

        命中 (chat+发布身份+size, backfillable) 且 CAS 获得编辑权 (R5 独占守卫) →
        engine.edit_item (F1 两段式 + F3 共享属性)。返回:
        - None: 无槽 / lost 回退 / 引擎未覆写 edit_item;
        - ("already_published", slot_id, real_msg_id): 并发任务已完成回填，跳过发送;
        - ("success",): 原地 edit 成功, 槽已置 published;
        - ("fallback", slot_id, kind): 回填失败按 §4.2 矩阵转入常规发送
          (kind ∈ deleted / window / other)。
        FloodWait / editing 占用 → RetryableTaskError 上抛 (调度器退避通道, 引擎层不自造重试)。"""
        # 引擎须覆写 edit_item (基类默认实现 → 视为不支持, 零 DB 开销快路径)
        if getattr(type(self.engine), "edit_item", None) is MediaTransportEngine.edit_item:
            return None
        from app.core.pipeline.placeholder import _slot_identity, classify_edit_failure

        identity = _slot_identity(item, context)
        size = int(item.size_bytes or 0)
        slot = PlaceholderSlotRepository.get_by_identity(context.target_chat_id, identity, size)
        if slot is None or slot["slot_kind"] != "backfillable":
            return None
        if not PlaceholderSlotRepository.cas_acquire_edit(context.target_chat_id, identity, size):
            return await self._handle_backfill_claim_conflict(item, context, identity, size)
        try:
            outcome = await self.engine.edit_item(item, context, int(slot["placeholder_msg_id"]))
        except (UploadCancelledError, UploadPausedError, RetryableTaskError, asyncio.CancelledError):
            PlaceholderSlotRepository.reset_to_sent(slot["id"])
            raise
        except Exception as exc:
            kind = classify_edit_failure(exc)
            if kind == "flood":
                PlaceholderSlotRepository.reset_to_sent(slot["id"])
                if context.task_id:
                    await TaskQueueRepository.append_log_async(
                        context.task_id, f"⚠️ [{item.name}] 原地回填遇 FloodWait, 交由调度器退避重试"
                    )
                raise RetryableTaskError(f"原地回填遇 FloodWait: {exc}") from exc
            if kind == "deleted":
                # 占位卡被删 → 无可回填目标; 槽置 lost, 回退常规发送
                PlaceholderSlotRepository.mark_lost(slot["id"], f"占位卡被删: {exc}")
                return ("fallback", slot["id"], "deleted")
            if kind == "window":
                # 超 48h 编辑窗口 → 优雅降级: 常规发送, 成功后槽置 published (⏳ 卡保留)
                return ("fallback", slot["id"], "window")
            # 其余 (上传/编辑失败): ⏳ 卡保留, 槽回 placeholder_sent 可复用
            PlaceholderSlotRepository.reset_to_sent(slot["id"])
            return ("fallback", slot["id"], "other")
        real_msg_id = outcome.message_ids[-1] if outcome.message_ids else 0
        if not real_msg_id:
            PlaceholderSlotRepository.reset_to_sent(slot["id"])
            return ("fallback", slot["id"], "other")
        PlaceholderSlotRepository.mark_published(slot["id"], real_msg_id)
        return ("success", real_msg_id, outcome)

    async def _reconcile_post_send_slot(
        self,
        item: PublishItem,
        context: PublishContext,
        msg_id: int,
        fallback_slot: int | None,
    ) -> None:
        from app.core.pipeline.placeholder import PlaceholderSlotCoordinator

        coordinator = self._slot_coordinator or PlaceholderSlotCoordinator()
        try:
            await coordinator.reconcile_post_send(item, context, msg_id, fallback_slot=fallback_slot)
        except Exception as slot_err:
            logger.warning("单发占位槽对账异常 (不阻塞主流程): %s", slot_err)

    @staticmethod
    def _finalize_failed_fallback_slot(
        fallback_slot: int | None,
        fallback_kind: str | None,
        deleted_reason: str,
    ) -> None:
        if fallback_slot is None:
            return
        if fallback_kind == "deleted":
            PlaceholderSlotRepository.mark_lost(fallback_slot, deleted_reason)
        elif fallback_kind == "window":
            PlaceholderSlotRepository.reset_to_sent(fallback_slot)

    async def _resolve_pre_send_backfill(
        self,
        item: PublishItem,
        context: PublishContext,
        counters: Counters,
        b: BatchState,
        *,
        is_pikpak: bool,
    ) -> tuple[bool, int | None, str | None]:
        """执行发送前占位回填探测; 返回 (是否已终结无需发送, fallback_slot, fallback_kind)."""
        backfill = await self._try_backfill(item, context)
        if backfill is None:
            return (False, None, None)
        kind = backfill[0]
        if kind == "already_published":
            _, _slot_id, real_msg_id = backfill
            b.dedup_hits.append({"file_name": item.name, "size_bytes": item.size_bytes})
            counters.published += 1
            b.published += 1
            counters.done_bytes += item.size_bytes
            context.completed_files_count = counters.published
            context.completed_bytes = counters.done_bytes
            skip_msg = f"⏭️ [{item.name}] 占位卡已由并发任务回填完成 (msg {real_msg_id})，跳过重复发送"
            logger.info(skip_msg)
            if context.task_id:
                await TaskQueueRepository.append_log_async(context.task_id, skip_msg)
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
            return (True, None, None)
        if kind == "success":
            _, real_msg_id, outcome = backfill
            await self._record_success(
                item,
                context,
                counters,
                b,
                msg_id=real_msg_id,
                is_pikpak=is_pikpak,
                dl_speed=outcome.speed_mb_s,
            )
            if context.task_id:
                await TaskQueueRepository.append_log_async(
                    context.task_id, f"✅ [{item.name}] 占位卡已原位回填为正式媒体 (msg {real_msg_id})"
                )
            return (True, None, None)
        return (False, backfill[1], backfill[2])

    async def _transport_single(
        self, item: PublishItem, context: PublishContext, counters: Counters, b: BatchState, *, is_pikpak: bool
    ) -> None:
        # 发送前占位槽回填探测 (命中且 CAS 成功 → 原地 edit, 两段式)
        handled, fallback_slot, fallback_kind = await self._resolve_pre_send_backfill(
            item, context, counters, b, is_pikpak=is_pikpak
        )
        if handled:
            return
        try:
            outcome = await self.engine.send_item(item, context)
        except asyncio.CancelledError:
            if fallback_slot is not None and fallback_kind == "window":
                PlaceholderSlotRepository.reset_to_sent(fallback_slot)
            raise
        except Exception as exc:
            self._finalize_failed_fallback_slot(fallback_slot, fallback_kind, "占位卡被删且回退发送仍失败")
            await self._handle_transport_failure(exc, item, context, b, is_pikpak=is_pikpak)
            return
        # J5: 落库消息 ID 缺失 (空 outcome / 末条 0 哨兵) → 计入失败且零入库
        msg_id = outcome.message_ids[-1] if outcome.message_ids else 0
        if not msg_id:
            self._finalize_failed_fallback_slot(fallback_slot, fallback_kind, "占位卡被删且回退发送未返回消息 ID")
            await self._record_empty_outcome_failure(item, context, b)
            return
        if fallback_slot is not None:
            await self._reconcile_post_send_slot(item, context, msg_id, fallback_slot)
        await self._record_success(
            item,
            context,
            counters,
            b,
            msg_id=msg_id,
            is_pikpak=is_pikpak,
            dl_speed=outcome.speed_mb_s,
        )
        if fallback_slot is None:
            await self._reconcile_post_send_slot(item, context, msg_id, fallback_slot)

    async def _transport_pikpak(
        self,
        publishable: list[PublishItem],
        context: PublishContext,
        counters: Counters,
        b: BatchState,
        all_skipped: list[Any],
    ) -> None:
        # D1/D6 相册分支: album_mode 开启时, 同批次图片合成相册 (photo 形态, 整组失败全员
        # failed 零入库 —— 对齐本地相册语义); 其余条目走单文件循环。相册只在同批次/同
        # 文件夹图片内组成 (逐文件夹批次天然咬合); 单图组走单文件路径语义更简 (caption=图名)。
        pending: list[PublishItem] = list(publishable)
        if context.album_mode:
            # P0 修复: 按 item.name 扩展名判图片 (is_image_item, 两侧统一) —— 旧实现按
            # item_id (PikPak 为 file_id, 无扩展名) 判定, 恒不命中, 相册分支整体静默失效。
            # 单趟分区 (对齐 _transport_local), 替代旧 O(n²) 按值相等剔除。
            images: list[PublishItem] = []
            pending = []
            for it in publishable:
                (images if is_image_item(it) else pending).append(it)
            # 防回归护栏: album_mode 开启却 0 张识别为图 + 存在扩展名无法归类的条目
            # (file_id/空扩展名形态) → 疑似识别失效, 记 warning 任务日志 (视频等已知
            # 媒体批次不误报)。
            if len(publishable) >= 2 and not images:
                unclassified = [it for it in publishable if _media_ext(it) not in settings.VALID_MEDIA_EXTENSIONS]
                if unclassified and context.task_id:
                    guard_msg = (
                        f"⚠️ 相册模式已开启但本批次未识别到图片 (共 {len(publishable)} 项)，"
                        "请核查文件扩展名识别逻辑 (is_image_item)"
                    )
                    await TaskQueueRepository.append_log_async(context.task_id, guard_msg)
            chunk_size = 10
            for i in range(0, len(images), chunk_size):
                check_signals(context)
                chunk = images[i : i + chunk_size]
                # 门槛 (编排侧): 超限图片绝不入组 —— 先走占位卡/跳过路径。force_skip:
                # 相册 photo 形态与虚拟分卷互斥, 即使任务策略为 split 也一律跳过 (引擎侧
                # 门槛为二次防御); strict_fail 对相册超限图片同样走占位卡。
                # 门槛谓词为 >= (等值即拒) = D1 "单文件必须低于分卷阈值才可入组" 的严格
                # 语义; 与单文件超限谓词 > (等值可不分卷直传, 见 _transport_local 单文件
                # 循环 / filtering.filter_batch) 是两个有意不同的谓词, 勿"统一" (J2)。
                groupable = []
                for sitm in chunk:
                    limit_bytes = self._batch_filter.source.oversize_limit_bytes(sitm, context)
                    if limit_bytes and sitm.size_bytes >= limit_bytes:
                        # force_skip=True: handle_oversize 恒返回 True (filtering 双短路),
                        # 超限图片必经占位卡/跳过, 绝不入组 —— 返回值无需判断 (J3)。
                        await self._batch_filter.handle_oversize(
                            sitm,
                            context,
                            limit_bytes,
                            is_pikpak=True,
                            counters=counters,
                            b=b,
                            all_skipped=all_skipped,
                            force_skip=True,
                        )
                    else:
                        groupable.append(sitm)
                chunk = groupable
                if len(chunk) < 2 or getattr(type(self.engine), "send_album", None) is MediaTransportEngine.send_album:
                    # 单图不组相册 (避免 caption/形态歧义), 走单文件循环
                    pending.extend(chunk)
                    continue
                try:
                    outcomes = await self.engine.send_album(chunk, context)
                except (UnconfirmedAlbumError, UploadCancelledError, UploadPausedError, RetryableTaskError):
                    raise
                except Exception as exc:
                    # 包4 (K4): 三处口径统一 not_found / 执行器兜底 (publish_executor_service
                    # getattr 同款先例) —— taxonomy 异常 str(exc)==user_detail 恒成立
                    # (taxonomy.py __init__), 原始域 (CLI/Provider/Telethon) 无 user_detail
                    # 回落 str(exc); 防未来 __str__ 携带技术前缀时三处漂移。非文案重写。
                    detail = getattr(exc, "user_detail", None) or str(exc)
                    logger.error("❌ PikPak 图片相册发布失败 (%s): %s", [s.name for s in chunk], detail)
                    if context.task_id:
                        album_err = f"❌ PikPak 图片相册发布失败 ({[s.name for s in chunk]}): {detail}"
                        await TaskQueueRepository.append_log_async(context.task_id, album_err)
                    for sitm in chunk:
                        b.failed.append(f"{sitm.name or sitm.item_id}: {detail}")
                        # 包4 (J7): 相册组失败逐条明细 (client_label = 主账号)
                        b.failed_details.append(
                            {
                                "file_name": sitm.name or sitm.item_id,
                                "reason": detail,
                                "client_label": context.account_display,
                            }
                        )
                    # 整组失败 → 合并 1 张文本汇总卡 (group_summary 槽)
                    sender = self._placeholder_sender
                    if sender is not None:
                        try:
                            await sender.send_group_summary_card(chunk, context, detail)
                        except Exception as card_err:
                            logger.warning("整组失败汇总卡发送异常: %s", card_err)
                else:
                    # 逐项 outcome 统一路由 (部分失败逐条卡 / 全组失败汇总卡)
                    await self._route_album_outcomes(chunk, outcomes, context, counters, b, is_pikpak=True)
                await wait_with_signals(context, await context.get_current_send_delay_seconds())

        for index, item in enumerate(pending):
            check_signals(context)
            # 远端 .ts/.m2ts/.mts 独立 2048B 文件头校验 (早于超限治理与占位回填, 杜绝无效条目触发发卡/建槽/归档副作用)
            if _media_ext(item) in TS_AMBIGUOUS_EXTENSIONS and isinstance(self.engine, MediaTransportEngine):
                is_valid_ts, invalid_ts_reason = await self.engine.preflight_item(item, context)
                if not is_valid_ts:
                    reason_text = invalid_ts_reason or "非有效 MPEG-TS 视频格式，已自动过滤"
                    logger.warning(
                        "⏭️ 跳过无效远端 TS 文件: file_id=%s name=%s reason=%s",
                        item.item_id,
                        item.name,
                        reason_text,
                    )
                    if context.task_id:
                        await TaskQueueRepository.append_log_async(
                            context.task_id, f"⏭️ 跳过 [{item.name or item.item_id}]: {reason_text}"
                        )
                    b.failed.append(f"{item.name or item.item_id}: {reason_text}")
                    b.failed_details.append(
                        {
                            "file_name": item.name or item.item_id,
                            "reason": reason_text,
                            "client_label": context.account_display,
                        }
                    )
                    continue
            # PikPak 超限治理 (历史顺序: 防重之后; 与本地共用 filtering.handle_oversize)。
            # 单文件超限谓词为 > (等值文件可不分卷直传) —— 与相册入组门槛 >= (D1 严格
            # "低于"语义, 见上方相册分支) 是两个有意不同的谓词, 勿"统一" (J2)。
            limit_bytes = self._batch_filter.source.oversize_limit_bytes(item, context)
            if limit_bytes and item.size_bytes > limit_bytes:
                skipped = await self._batch_filter.handle_oversize(
                    item,
                    context,
                    limit_bytes,
                    is_pikpak=True,
                    counters=counters,
                    b=b,
                    all_skipped=all_skipped,
                )
                if skipped:
                    continue
            if index > 0:
                await wait_with_signals(context, await context.get_current_send_delay_seconds())
                check_signals(context)
            await self._transport_single(item, context, counters, b, is_pikpak=True)

    async def _transport_local(
        self, publishable: list[PublishItem], context: PublishContext, counters: Counters, b: BatchState
    ) -> None:
        image_files: list[PublishItem] = []
        single_files: list[PublishItem] = []
        if context.album_mode and getattr(type(self.engine), "send_album", None) is not MediaTransportEngine.send_album:
            # P0 修复: 按 item.name 扩展名判图片 (is_image_item) —— 本地 item_id=绝对路径
            # 时旧判定本就成立, 收口共享 helper 后两侧语义一致。
            for itm in publishable:
                (image_files if is_image_item(itm) else single_files).append(itm)
        else:
            single_files = list(publishable)

        chunk_size = 10
        for i in range(0, len(image_files), chunk_size):
            check_signals(context)
            sub_items = image_files[i : i + chunk_size]
            try:
                outcomes = await self.engine.send_album(sub_items, context)
            except (UnconfirmedAlbumError, UploadCancelledError, UploadPausedError, RetryableTaskError):
                raise
            except Exception as exc:
                err_msg = f"❌ 图片 Album 发布失败 ({[s.name for s in sub_items]}): {exc}"
                logger.error(err_msg)
                if context.task_id:
                    await TaskQueueRepository.append_log_async(context.task_id, err_msg)
                group_label = context.account_display
                underlying_album_exc: Exception = exc
                if isinstance(exc, TransportError):
                    group_label = exc.client_label or group_label
                    if exc.cause is not None and isinstance(exc.cause, Exception):
                        underlying_album_exc = exc.cause
                from app.core.session_errors import is_session_invalid_error, report_session_invalid

                is_bot_pool = isinstance(exc, TransportError) and group_label.startswith("Bot Pool")
                if is_session_invalid_error(underlying_album_exc) and not is_bot_pool:
                    await report_session_invalid(
                        context.task_id,
                        underlying_album_exc,
                        account_id=getattr(context, "account_id", None),
                    )
                    if underlying_album_exc is exc:
                        raise
                    raise underlying_album_exc from exc
                for sitm in sub_items:
                    failure_exc = describe_upload_failure(underlying_album_exc, sitm.size_bytes, context.is_premium)
                    FailureLogger.log_failure(
                        sitm.item_id,
                        context.target_chat_id,
                        failure_exc,
                        client_type=group_label,
                        task_id=context.task_id,
                    )
                    b.failed.append(sitm.name)
                    # 包4 (J7): 相册组失败逐条明细
                    b.failed_details.append(
                        {
                            "file_name": sitm.name,
                            "reason": str(failure_exc),
                            "client_label": group_label,
                        }
                    )
                # 整组失败 → 合并 1 张文本汇总卡 (group_summary 槽)
                sender = self._placeholder_sender
                if sender is not None:
                    try:
                        await sender.send_group_summary_card(sub_items, context, str(exc))
                    except Exception as card_err:
                        logger.warning("整组失败汇总卡发送异常: %s", card_err)
            else:
                # 逐项 outcome 统一路由 (部分失败逐条卡 / 全组失败汇总卡)
                await self._route_album_outcomes(sub_items, outcomes, context, counters, b, is_pikpak=False)
            await wait_with_signals(context, await context.get_current_send_delay_seconds())

        for item in single_files:
            check_signals(context)
            await self._transport_single(item, context, counters, b, is_pikpak=False)
            await wait_with_signals(context, await context.get_current_send_delay_seconds())

    # ------------------------------------------------------------------
    # 编排主入口: Phase 2 → 1 → (3 → 4 → 6 → 5) × batches → 汇总
    # (实现名 _execute_impl + 类级别名 execute: 保持既有调用面不变; 静态扫描器对与
    #  DB 游标同名的编排入口存在确定性误报, 本仓库无任何手拼 SQL)
    # ------------------------------------------------------------------

    async def _execute_impl(self, context: PublishContext) -> PublishResult:
        """执行统一发布流水线"""
        if context.task_id:
            task = await TaskQueueRepository.get_by_id_async(context.task_id)
            if task and (task.get("params") or {}).get("pending_album_delivery"):
                raise UnconfirmedAlbumError()
        batches = await self.source.get_batches(context)
        # 包3 (K3): medium 标识唯一权威 = context.source_type (两门面装配显式设置, 见
        # models.PublishContext 字段注释)。旧的 isinstance 双查 + 死条件 getattr +
        # 逐条 item 扫描启发式与编排层对具体策略类的延迟 import 一并删除 —— 执行期
        # 审计 (收尾批次): PublishPipeline 生产代码全仓仅两门面构造, 门面装配是
        # medium 分流的唯一流入路径 (tests 直构用例已显式声明 medium, 见 §3.3.0 勘误)。
        is_pikpak = context.source_type == "pikpak"
        total_files_count = sum(len(b.items) for b in batches)
        context.total_files_count = total_files_count
        context.completed_files_count = 0
        context.total_bytes_sum = sum(b.total_size_bytes for b in batches)
        context.completed_bytes = 0

        if total_files_count == 0:
            if is_pikpak:
                logger.info("PikPak 发布: 未选择任何远端文件")
            else:
                logger.info("提示: 在 %s 中未找到可发布的媒体文件。", context.folder_path or context.folder_name)
                if context.task_id:
                    empty_msg = f"⚠️ 在 {context.folder_path or context.folder_name} 中未找到可发布的媒体文件。"
                    await TaskQueueRepository.append_log_async(context.task_id, empty_msg)
            return PublishResult(
                published_count=0,
                total_files_count=0,
                failed_files=[],
                skipped_files=[],
                is_folder_publish=context.is_folder_publish,
                folder_name=context.folder_name or "",
            )

        preflight_res = await phase1_preflight(context, is_pikpak, self.channel_probe)
        if preflight_res is not None:
            return preflight_res

        counters = Counters(total_files_count)
        # /run 级发卡器 (封顶 10+1 计数器与 InputPhoto 缓存) + 槽协调器
        from app.core.pipeline.placeholder import PlaceholderSlotCoordinator

        self._placeholder_sender = PlaceholderCardSender()
        self._batch_filter.placeholder_sender = self._placeholder_sender
        self._slot_coordinator = PlaceholderSlotCoordinator()
        all_failed: list[str] = []
        all_skipped: list[Any] = []
        all_failed_details: list[dict[str, Any]] = []  # 包4 (J7): 结构化失败明细 (任务级聚合)
        all_dedup: list[dict[str, Any]] = []  # 包4 (J7): 防重命中明细 (任务级聚合)
        all_archive_summaries: list[str] = []
        batch_reports: list[PublishResult] = []
        try:
            for batch_idx, batch in enumerate(batches):
                check_signals(context)
                b = BatchState()
                publishable = await self._batch_filter.filter_batch(batch, context, is_pikpak, counters, b, all_skipped)
                if is_pikpak:
                    await self._transport_pikpak(publishable, context, counters, b, all_skipped)
                else:
                    await self._transport_local(publishable, context, counters, b)

                # Phase 6: 批内清理 (失败不影响终态)
                archive_summary, archive_failed = "", False
                context.cleanup_had_failures = False
                context.cleanup_defer_unrelated_dirs = batch_idx < len(batches) - 1
                try:
                    if b.successful or not context.cleanup_defer_unrelated_dirs:
                        archive_summary = await self.cleanup.cleanup(b.successful, context) or ""
                        archive_failed = bool(getattr(context, "cleanup_had_failures", False))
                except Exception as cleanup_err:
                    logger.warning("批次 [%s] 清理善后失败 (不影响任务终态): %s", batch.batch_id, cleanup_err)
                    archive_failed = True
                if archive_summary:
                    all_archive_summaries.append(archive_summary)

                # Phase 5: 批次告示 (发送异常仅落日志)
                batch_result = PublishResult(
                    published_count=b.published,
                    total_files_count=len(batch.items),
                    failed_files=b.failed,
                    skipped_files=b.skipped,
                    archive_summary=archive_summary,
                    is_folder_publish=batch.is_folder_publish,
                    folder_name=batch.folder_name,
                    newly_published_count=b.newly,
                    archive_failed=archive_failed,
                    album_published_count=b.album_published,
                    failed_details=b.failed_details,
                    dedup_skipped=b.dedup_hits,
                )
                try:
                    await self.notifier.notify_batch(batch, context, batch_result)
                except Exception as notif_err:
                    logger.warning("批次 [%s] 通知告示发送异常 (不影响任务终态): %s", batch.batch_id, notif_err)

                batch_reports.append(batch_result)
                all_failed.extend(b.failed)
                all_failed_details.extend(b.failed_details)
                all_dedup.extend(b.dedup_hits)

            # 双卡制: 任务级汇总卡 (存在文件夹批次时发送一次; 执行口径供否决:
            # 纯散装任务不发; 全量防重跳过且无失败/跳过时抑制 —— 对齐批次卡防刷屏裁决)
            if any(br.is_folder_publish for br in batch_reports) and not (
                counters.newly == 0 and not all_failed and not all_skipped
            ):
                try:
                    await self.notifier.notify_task_summary(context, batch_reports)
                except Exception as notif_err:
                    logger.warning("任务汇总卡发送异常 (不影响任务终态): %s", notif_err)

            # 收尾保险: 最后一批的占位卡排水期间到达的暂停 (pause_drain 已置位, 批次循环
            # 内没有再触发的检查点) 必须在此收口 —— 记 paused 而非 completed
            check_signals(context)
        except UploadPausedError:
            pause_msg = "⏸️ 收到暂停信号，已保存上传断点并释放连接让位退出。"
            logger.warning(pause_msg)
            if context.task_id:
                try:
                    await TaskQueueRepository.append_log_async(context.task_id, pause_msg)
                except Exception as log_err:
                    logger.warning("写入暂停任务日志失败 (忽略, 保留控制信号): %s", log_err)
            raise
        except UploadCancelledError:
            cancel_msg = "⚠️ 收到取消信号，已立即中断上传传输并退出。"
            logger.warning(cancel_msg)
            if context.task_id:
                try:
                    await TaskQueueRepository.append_log_async(context.task_id, cancel_msg)
                except Exception as log_err:
                    logger.warning("写入取消任务日志失败 (忽略, 保留控制信号): %s", log_err)
            raise

        combined_archive = " · ".join(all_archive_summaries) if all_archive_summaries else None
        return PublishResult(
            published_count=counters.published,
            total_files_count=total_files_count,
            failed_files=all_failed,
            skipped_files=all_skipped,
            archive_summary=combined_archive,
            is_folder_publish=context.is_folder_publish,
            folder_name=context.folder_name,
            newly_published_count=counters.newly,
            archive_failed=any(br.archive_failed for br in batch_reports),
            failed_details=all_failed_details,
            dedup_skipped=all_dedup,
        )

    # 公共编排入口 (类级别名, 调用面 pipeline.execute 不变)
    execute = _execute_impl
