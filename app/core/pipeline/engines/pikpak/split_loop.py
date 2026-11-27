"""统一发布流水线传输引擎 - PikPak 零落盘流式传输分卷主循环 (Split Loop)"""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from functools import partial
from typing import Any

from telethon.errors import FloodWaitError
from telethon.tl.types import MessageEmpty

from app.core import fmp4_pipe_uploader as fmp4_mod
from app.core import stream_chunk_provider as scp_mod
from app.core.e2e_capture import E2ECaptureSession
from app.core.pikpak.taxonomy import (
    PikPakPublishRetryableError,
    PikPakPublishRiskControlError,
    PikPakPublishThrottledError,
    PikPakTransientError,
)
from app.core.pikpak.taxonomy import (
    PikPakRiskControlError as CliPikPakRiskControlError,
)
from app.core.pipeline.engines._common import (
    ProgressReporter,
    _inter_part_breathing_delay,
    _raise_if_control_signal,
)
from app.core.pipeline.engines._types import SplitChannel, SplitVolumePlan, VolumeJob
from app.core.pipeline.engines.part_delivery import UnconfirmedPartError, part_receipt_scope
from app.core.pipeline.interfaces import DEFAULT_TASK_STORE, TaskStorePort
from app.core.pipeline.models import PublishContext, PublishItem, TransportOutcome
from app.core.scheduler import RetryableTaskError, UploadCancelledError, UploadPausedError

logger = logging.getLogger(__name__)


async def delete_published_messages(
    client: Any,
    chat_id: Any,
    message_ids: list[int],
    *,
    propagate_flood_wait: bool = False,
) -> bool:
    """Atomic rollback helper: delete delivered orphan messages on failure. Returns True if all cleaned (or empty)."""
    if not message_ids:
        return True
    logger.warning("⚠️ 发布中断或失败，正在物理回滚清理已发送的 %d 条孤儿消息: %s", len(message_ids), message_ids)
    try:
        await client.delete_messages(chat_id, list(message_ids))
        remaining = await read_live_message_ids(client, chat_id, message_ids)
        if remaining:
            logger.warning("目标仍存在分卷消息: %s", remaining)
            return False
        logger.info("✅ 已成功清理孤儿消息: %s", message_ids)
        return True
    except FloodWaitError as del_fw:
        logger.warning("清理孤儿消息遇 FloodWait: %s", del_fw)
        if propagate_flood_wait:
            raise
        return False
    except Exception as del_err:
        logger.warning("清理孤儿消息失败: %s", del_err)
        return False


async def read_live_message_ids(client: Any, chat_id: Any, ids: list[int]) -> set[int]:
    """Telethon preserves the requested positions and returns None for missing IDs."""
    live: set[int] = set()
    for offset in range(0, len(ids), 100):
        batch = ids[offset : offset + 100]
        messages = await client.get_messages(chat_id, ids=batch)
        if not isinstance(messages, (list, tuple)) or len(messages) != len(batch):
            raise RetryableTaskError("无法核实分卷消息状态")
        for expected, msg in zip(batch, messages, strict=True):
            if msg is None or isinstance(msg, MessageEmpty):
                continue
            if msg.id != expected:
                raise RetryableTaskError("分卷消息回读 ID 不匹配")
            live.add(expected)
    return live


def _check_delivery_state(checkpoint: dict[str, Any]) -> None:
    if any(int(value) < 0 for value in (checkpoint.get("delivered") or {}).values()):
        raise UnconfirmedPartError()


async def _peek_checkpoint_resume_info(
    task_id: str,
    file_id: str,
    strategy: str,
    threshold: int,
    store: TaskStorePort | None = None,
) -> tuple[int | None, list[int], str | None]:
    """探测已保存的分卷检查点元数据, 返回 (saved_total_parts, delivered_msg_ids, saved_plan_signature)。

    仅当 strategy 与 threshold 一致且存在已投递消息时返回正整数 saved_total_parts,
    以便恢复执行时沿用重规划后的分卷数 (forced_parts), 避免重新规划回 N 卷造成重传与孤儿。"""
    if not task_id:
        return None, [], None
    try:
        row = await (store or DEFAULT_TASK_STORE).get_by_id(task_id)
        if not row:
            return None, [], None
        params = row.get("params") or {}
        checkpoints = params.get("pikpak_part_checkpoints") or {}
        file_cp = checkpoints.get(str(file_id))
        if not file_cp or not isinstance(file_cp, dict):
            return None, [], None
        _check_delivery_state(file_cp)
        if file_cp.get("strategy") == strategy and file_cp.get("threshold") == threshold:
            delivered = file_cp.get("delivered") or {}
            msg_ids = [int(v) for v in delivered.values() if v]
            total_parts = int(file_cp.get("total_parts") or 0)
            raw_sig = file_cp.get("plan_signature")
            plan_sig = raw_sig if isinstance(raw_sig, str) and raw_sig else None
            return (total_parts if total_parts > 0 else None), msg_ids, plan_sig
        live_ids = [int(v) for v in (file_cp.get("delivered") or {}).values() if int(v) > 0]
        if live_ids:
            return None, live_ids, "incompatible-plan"
    except UnconfirmedPartError:
        raise
    except Exception as exc:
        raise RetryableTaskError("无法读取分卷检查点元数据，保留检查点等待重试") from exc
    return None, [], None


async def get_saved_checkpoints(
    task_id: str,
    file_id: str,
    strategy: str,
    total_parts: int,
    threshold: int,
    store: TaskStorePort | None = None,
    *,
    plan_signature: str | None = None,
    client: Any = None,
    chat_id: Any = None,
) -> dict[int, int]:
    """读取任务持久化分卷检查点: 校验 strategy, total_parts, threshold 与切分边界签名一致性。

    持久层经 TaskStorePort 注入 (None → 默认单例适配器, 调用期经
    模块属性读取 TaskQueueRepository, 类级 patch 保持穿透)。
    """
    if not task_id:
        return {}
    try:
        row = await (store or DEFAULT_TASK_STORE).get_by_id(task_id)
        if not row:
            return {}
        params = row.get("params") or {}
        checkpoints = params.get("pikpak_part_checkpoints") or {}
        file_cp = checkpoints.get(str(file_id))
        if not file_cp or not isinstance(file_cp, dict):
            return {}
        _check_delivery_state(file_cp)
        raw_sig = file_cp.get("plan_signature")
        saved_sig = raw_sig if isinstance(raw_sig, str) and raw_sig else None
        if (
            file_cp.get("strategy") == strategy
            and file_cp.get("total_parts") == total_parts
            and file_cp.get("threshold") == threshold
            and saved_sig == plan_signature
        ):
            delivered = file_cp.get("delivered") or {}
            result = {int(k): int(v) for k, v in delivered.items() if int(v) > 0}
            if result and file_cp.get("rollback_started"):
                if client is None:
                    raise RetryableTaskError("需回读目标消息以核对分卷回滚")
                live = await read_live_message_ids(client, chat_id, list(result.values()))
                for part, msg in list(result.items()):
                    if msg not in live:
                        await (store or DEFAULT_TASK_STORE).forget_missing_part(task_id, file_id, part, msg)
                        del result[part]
            return result
        if any(int(v) > 0 for v in (file_cp.get("delivered") or {}).values()) and client is not None:
            raise UnconfirmedPartError()
    except UnconfirmedPartError:
        raise
    except Exception as exc:
        raise RetryableTaskError("无法读取分卷检查点，保留检查点等待重试") from exc
    return {}


_PRESERVE_DELIVERY_EXC_TYPES = (
    UnconfirmedPartError,
    FloodWaitError,
    UploadPausedError,
    scp_mod.PikPakThrottledError,
    PikPakPublishThrottledError,
    scp_mod.PikPakRiskControlError,
    CliPikPakRiskControlError,
    PikPakPublishRiskControlError,
    RetryableTaskError,
    PikPakPublishRetryableError,
    scp_mod.ChunkFetchTransientError,
    PikPakTransientError,
    scp_mod.PikPakLinkExpiredError,
)


def _should_preserve_delivered_volumes(context: PublishContext, last_split_exc: BaseException | None) -> bool:
    """非惩罚性保全谓词:
    暂停 / CDN 频控冷却 / 风控 / 瞬态重试一律保全已发分卷; 显式取消或致命错误 (含 Telegram send_file 连续 FloodWait 耗尽) 优先物理回滚。"""
    is_cancelled = bool(
        (context.is_cancelled and context.is_cancelled()) or isinstance(last_split_exc, UploadCancelledError)
    )
    is_paused = bool((context.is_paused and context.is_paused()) or isinstance(last_split_exc, UploadPausedError))
    is_preserve_exc = isinstance(last_split_exc, _PRESERVE_DELIVERY_EXC_TYPES)
    return (is_paused or is_preserve_exc) and not is_cancelled


async def _dispose_failed_delivery(
    *,
    channel: SplitChannel,
    item: PublishItem,
    context: PublishContext,
    client: Any,
    capture_session: E2ECaptureSession,
    delivered_message_ids: list[int],
    last_split_exc: BaseException | None,
    task_store: TaskStorePort,
) -> None:
    """失败处置: 严格回滚谓词 → 非惩罚性保全 / 物理回滚 +
    检查点清理, 末尾统一 fail_pending_parts。"""
    # 严格回滚谓词: 显式取消或致命不可恢复异常物理回滚; 暂停/冷却/风控/瞬态重试一律非惩罚性保全已发分卷
    if isinstance(last_split_exc, UnconfirmedPartError) or _should_preserve_delivered_volumes(context, last_split_exc):
        if delivered_message_ids:
            logger.info(
                "🛡️ %s 非惩罚性保全: 任务中断 (暂停/冷却/重试)，保全已交付的 %d 条分卷消息 (IDs: %s)",
                channel.channel_label,
                len(delivered_message_ids),
                delivered_message_ids,
            )
    elif delivered_message_ids:
        # 直调模块函数 (原经 channel.engine 属性 getattr 的测试兼容垫片废除)
        await task_store.begin_part_rollback(context.task_id, item.item_id)
        deleted_ok = await delete_published_messages(client, context.target_chat_id, delivered_message_ids)
        if deleted_ok:
            capture_session.mark_rolled_back()
            await task_store.clear_part_checkpoint(context.task_id, item.item_id)
        else:
            logger.warning(
                "⚠️ %s 回滚清理已交付分卷失败，保留检查点以便后续恢复或清理 (IDs: %s)",
                channel.channel_label,
                delivered_message_ids,
            )
    capture_session.fail_pending_parts(f"{channel.channel_name} publish aborted before volume completion")


async def _refresh_link_jit(channel: SplitChannel, plan_idx: int, plan: Any) -> None:
    """JIT 索链: 后续分卷开始前按需获取最新有效直链
    (防跨卷过期); 索链异常告警后继续发布 (不熔断)。"""
    if plan.part_index > 1:
        try:
            await channel.prepare_part(plan_idx, plan)
        except Exception as link_err:
            logger.warning("JIT 索取分卷直链异常: %s", link_err)


async def _replan_after_oversize(
    *,
    channel: SplitChannel,
    context: PublishContext,
    item: PublishItem,
    capture_session: E2ECaptureSession,
    client: Any,
    plans: Any,
    delivered_message_ids: list[int],
    replan_count: int,
    max_replan_attempts: int,
    last_part_exc: Exception | None,
    task_store: TaskStorePort,
) -> int:
    """超限熔断处置: 通道重规划 (回滚已发卷 + 强制分卷数 + 直链刷新) → 成功后清检查点。"""
    forced_parts = await channel.handle_oversize(
        client=client,
        context=context,
        capture_session=capture_session,
        plans=plans,
        delivered_message_ids=delivered_message_ids,
        replan_count=replan_count,
        max_replan_attempts=max_replan_attempts,
        exc=last_part_exc if last_part_exc is not None else RuntimeError("oversize"),
        item_id=item.item_id,
        task_store=task_store,
    )
    await task_store.clear_part_checkpoint(context.task_id, item.item_id)
    return forced_parts


def _resolve_plan_signature(channel: SplitChannel, plans: Sequence[SplitVolumePlan]) -> str | None:
    """读取通道提供的切分边界签名; 对测试桩/未实现通道返回 None (仅采信非空 str)。"""
    fn = getattr(channel, "get_plan_signature", None)
    if not callable(fn):
        return None
    raw_sig = fn(plans)
    return raw_sig if isinstance(raw_sig, str) and raw_sig else None


async def _reconcile_resume_plan_count(
    *,
    replan_count: int,
    saved_total_parts: int | None,
    saved_delivered_ids: list[int],
    saved_plan_signature: str | None = None,
    plan_signature: str | None = None,
    plans: Sequence[SplitVolumePlan],
    channel: SplitChannel,
    client: Any,
    context: PublishContext,
    item: PublishItem,
    task_store: TaskStorePort,
) -> list[int]:
    """恢复规划分卷数或切分边界签名与检查点不一致时先清理陈旧消息再清空检查点；删失败保留检查点并上抛重试."""
    count_mismatch = saved_total_parts is not None and len(plans) != saved_total_parts
    signature_mismatch = saved_plan_signature != plan_signature
    if replan_count == 0 and saved_delivered_ids and (count_mismatch or signature_mismatch):
        logger.warning(
            "⚠️ %s 恢复分卷计划与历史检查点不一致 (count: %d vs %s, signature_match=%s)，先清理 %d 条陈旧已发消息再重传",
            channel.channel_label,
            len(plans),
            saved_total_parts,
            not signature_mismatch,
            len(saved_delivered_ids),
        )
        try:
            await task_store.begin_part_rollback(context.task_id, item.item_id)
            deleted_ok = await delete_published_messages(
                client,
                context.target_chat_id,
                saved_delivered_ids,
                propagate_flood_wait=True,
            )
        except FloodWaitError as fw_err:
            wait_sec = float(getattr(fw_err, "seconds", 60) or 60)
            raise RetryableTaskError(
                f"分卷计划变更清理历史消息遇 FloodWait ({int(wait_sec)}s)，保留检查点等待重试",
                retry_after=wait_sec,
            ) from fw_err
        if deleted_ok is False:
            raise RetryableTaskError("分卷计划变更清理历史消息失败，保留检查点等待重试")
        await task_store.clear_part_checkpoint(context.task_id, item.item_id)
        return []
    return saved_delivered_ids


async def _persist_part_checkpoint(
    *,
    task_store: TaskStorePort,
    context: PublishContext,
    item: PublishItem,
    channel: SplitChannel,
    total_parts: int,
    threshold: int,
    part_index: int,
    msg_id: int,
    plan_signature: str | None,
) -> None:
    """落盘单卷检查点 (仅在存在非空 plan_signature 时透传该参数以兼容历史 7 参桩)."""
    if plan_signature is not None:
        await task_store.record_part_checkpoint(
            context.task_id,
            item.item_id,
            channel.channel_name,
            total_parts,
            threshold,
            part_index,
            msg_id,
            plan_signature=plan_signature,
        )
    else:
        await task_store.record_part_checkpoint(
            context.task_id,
            item.item_id,
            channel.channel_name,
            total_parts,
            threshold,
            part_index,
            msg_id,
        )


async def _persist_checkpoint_with_compensation(
    *,
    task_store: TaskStorePort,
    context: PublishContext,
    item: PublishItem,
    channel: SplitChannel,
    total_parts: int,
    threshold: int,
    part_index: int,
    msg_id: int,
    plan_signature: str | None,
    client: Any,
    delivered_message_ids: list[int],
) -> None:
    """持久化单卷检查点 (重试 1 次); 仍失败则补偿删除本次未记账分卷并转 RetryableTaskError."""
    if msg_id and msg_id not in delivered_message_ids:
        delivered_message_ids.append(msg_id)
    for cp_attempt in range(2):
        try:
            await _persist_part_checkpoint(
                task_store=task_store,
                context=context,
                item=item,
                channel=channel,
                total_parts=total_parts,
                threshold=threshold,
                part_index=part_index,
                msg_id=msg_id,
                plan_signature=plan_signature,
            )
            return
        except (UploadCancelledError, UploadPausedError, asyncio.CancelledError):
            raise
        except Exception as cp_err:
            if cp_attempt == 0:
                continue
            if msg_id:
                try:
                    await task_store.begin_part_rollback(context.task_id, item.item_id)
                except Exception as mark_err:
                    raise UnconfirmedPartError() from mark_err
                comp_ok = await delete_published_messages(client, context.target_chat_id, [msg_id])
                if comp_ok:
                    await _persist_part_checkpoint(
                        task_store=task_store,
                        context=context,
                        item=item,
                        channel=channel,
                        total_parts=total_parts,
                        threshold=threshold,
                        part_index=part_index,
                        msg_id=0,
                        plan_signature=plan_signature,
                    )
                    if msg_id in delivered_message_ids:
                        delivered_message_ids.remove(msg_id)
                    raise RetryableTaskError(
                        f"分卷 Part {part_index}/{total_parts} 检查点落盘失败，已回滚当前分卷等待重试: {cp_err}"
                    ) from cp_err
            raise UnconfirmedPartError() from cp_err


def _record_checkpoint_hit(
    *,
    channel: SplitChannel,
    plan_idx: int,
    plan: SplitVolumePlan,
    total_parts: int,
    cached_msg_id: int,
    delivered_message_ids: list[int],
    capture_session: E2ECaptureSession,
) -> int:
    """检查点命中幂等跳过记账, 返回本卷预估字节数."""
    if cached_msg_id not in delivered_message_ids:
        delivered_message_ids.append(cached_msg_id)
    vol_len = channel.get_cached_volume_bytes(plan_idx, plan)
    logger.info(
        "⏩ %s 检查点命中: 分卷 Part %d/%d 已成功投递 (msg_id=%d)，幂等跳过重发",
        channel.channel_label,
        plan.part_index,
        total_parts,
        cached_msg_id,
    )
    capture_session.record_part_sent(
        plan.part_name,
        msg_id=int(cached_msg_id),
        sent_attributes=None,
    )
    return vol_len


async def _reconcile_and_load_checkpoints(
    *,
    replan_count: int,
    saved_total_parts: int | None,
    saved_delivered_ids: list[int],
    saved_plan_signature: str | None,
    plan_signature: str | None,
    plans: Sequence[SplitVolumePlan],
    channel: SplitChannel,
    client: Any,
    context: PublishContext,
    item: PublishItem,
    threshold: int,
    task_store: TaskStorePort,
    delivered_message_ids: list[int],
) -> tuple[list[int], dict[int, int]]:
    """对账恢复分卷计划并加载有效检查点, 同步初始化 delivered_message_ids."""
    reconciled_ids = await _reconcile_resume_plan_count(
        replan_count=replan_count,
        saved_total_parts=saved_total_parts,
        saved_delivered_ids=saved_delivered_ids,
        saved_plan_signature=saved_plan_signature,
        plan_signature=plan_signature,
        plans=plans,
        channel=channel,
        client=client,
        context=context,
        item=item,
        task_store=task_store,
    )
    if replan_count == 0:
        delivered_message_ids[:] = list(dict.fromkeys(reconciled_ids)) if reconciled_ids else []
    if saved_delivered_ids and not reconciled_ids:
        return [], {}
    saved_checkpoints = await get_saved_checkpoints(
        context.task_id,
        item.item_id,
        channel.channel_name,
        len(plans),
        threshold,
        store=task_store,
        plan_signature=plan_signature,
        client=client,
        chat_id=context.target_chat_id,
    )
    if replan_count == 0:
        delivered_message_ids[:] = [msg for msg in delivered_message_ids if msg in saved_checkpoints.values()]
    return reconciled_ids, saved_checkpoints


async def execute_split_loop(
    *,
    channel: SplitChannel,
    item: PublishItem,
    context: PublishContext,
    threshold: int,
    client: Any,
    capture_session: E2ECaptureSession,
    link_provider: Callable[..., Awaitable[list[str]]],
    reporter: ProgressReporter,
    max_replan_attempts: int = 3,
    store: TaskStorePort | None = None,
) -> TransportOutcome:
    """通用分卷传输主循环模板。

    将 VSSE 虚拟分卷与 fMP4 动态分卷中同构的检查点记录与清理、
    幂等跳过已发分卷、JIT 跨卷索链刷新、分卷间呼吸等待、
    异常判定与非惩罚性保全/物理回滚逻辑统一收敛。
    持久层经 TaskStorePort 注入 (None → 默认单例适配器)。
    """
    task_store = store or DEFAULT_TASK_STORE
    delivered_message_ids: list[int] = []
    last_speed = 0.0
    prior_bytes = 0
    publish_success = False
    last_split_exc: BaseException | None = None
    last_part_exc: Exception | None = None
    replan_count = 0
    forced_parts: int | None = None

    try:
        saved_total_parts, saved_delivered_ids, saved_plan_signature = await _peek_checkpoint_resume_info(
            context.task_id, item.item_id, channel.channel_name, threshold, store=task_store
        )
        delivered_message_ids[:] = list(dict.fromkeys(saved_delivered_ids))
        if saved_total_parts and saved_total_parts > 1:
            forced_parts = saved_total_parts

        while replan_count <= max_replan_attempts:
            plans = channel.plan_volumes(forced_parts)
            plan_signature = _resolve_plan_signature(channel, plans)
            saved_delivered_ids, saved_checkpoints = await _reconcile_and_load_checkpoints(
                replan_count=replan_count,
                saved_total_parts=saved_total_parts,
                saved_delivered_ids=saved_delivered_ids,
                saved_plan_signature=saved_plan_signature,
                plan_signature=plan_signature,
                plans=plans,
                channel=channel,
                client=client,
                context=context,
                item=item,
                threshold=threshold,
                task_store=task_store,
                delivered_message_ids=delivered_message_ids,
            )

            oversize_triggered = False
            for plan_idx, plan in enumerate(plans):
                _raise_if_control_signal(context)

                # 检查点命中: 幂等跳过已成功交付的分卷 (零 CDN 请求)
                if plan.part_index in saved_checkpoints:
                    prior_bytes += _record_checkpoint_hit(
                        channel=channel,
                        plan_idx=plan_idx,
                        plan=plan,
                        total_parts=len(plans),
                        cached_msg_id=saved_checkpoints[plan.part_index],
                        delivered_message_ids=delivered_message_ids,
                        capture_session=capture_session,
                    )
                    continue

                # JIT 索链: 后续分卷开始前按需获取最新有效直链 (防跨卷过期)
                await _refresh_link_jit(channel, plan_idx, plan)

                job = VolumeJob(
                    part_index=plan.part_index,
                    total_parts=len(plans),
                    plan=plan,
                    prior_bytes=prior_bytes,
                    estimated_bytes=channel.get_cached_volume_bytes(plan_idx, plan),
                    part_name=plan.part_name,
                )

                try:
                    receipt = partial(
                        task_store.record_part_checkpoint,
                        context.task_id,
                        item.item_id,
                        channel.channel_name,
                        len(plans),
                        threshold,
                        plan.part_index,
                        plan_signature=plan_signature,
                    )
                    with part_receipt_scope(receipt if context.task_id else None):
                        msg_id, volume_bytes, volume_speed = await channel.stream_part(
                            job=job,
                            client=client,
                            context=context,
                            capture_session=capture_session,
                            reporter=reporter,
                        )
                except (fmp4_mod.PartOversizeError, fmp4_mod.PartDurationInflatedError) as pe:
                    last_part_exc = pe
                    logger.warning(
                        "⚡ Part %d/%d 触发 %s 熔断/重规划: %s",
                        plan.part_index,
                        plan.total_parts,
                        pe.__class__.__name__,
                        pe,
                    )
                    capture_session.discard_part_sink(plan.part_name)
                    oversize_triggered = True
                    break

                await _persist_checkpoint_with_compensation(
                    task_store=task_store,
                    context=context,
                    item=item,
                    channel=channel,
                    total_parts=len(plans),
                    threshold=threshold,
                    part_index=plan.part_index,
                    msg_id=msg_id,
                    plan_signature=plan_signature,
                    client=client,
                    delivered_message_ids=delivered_message_ids,
                )
                prior_bytes += volume_bytes
                last_speed = volume_speed

                if plan_idx < len(plans) - 1:
                    await _inter_part_breathing_delay(context, plan.part_index, len(plans), channel.channel_label)

            if oversize_triggered:
                replan_count += 1
                forced_parts = await _replan_after_oversize(
                    channel=channel,
                    context=context,
                    item=item,
                    capture_session=capture_session,
                    client=client,
                    plans=plans,
                    delivered_message_ids=delivered_message_ids,
                    replan_count=replan_count,
                    max_replan_attempts=max_replan_attempts,
                    last_part_exc=last_part_exc,
                    task_store=task_store,
                )
                prior_bytes = 0
                continue

            # 文件级历史提交后由 PublishPipeline 清理，跨越发送成功到入账之间的窗口。
            publish_success = True
            break

        return TransportOutcome(
            message_ids=delivered_message_ids,
            speed_mb_s=last_speed,
            client_label=context.account_display or "Userbot",
        )
    except BaseException as exc:
        last_split_exc = exc
        raise
    finally:
        try:
            await channel.aclose()
        except Exception as cl_err:
            logger.debug("关闭 channel 异常 (非致命): %s", cl_err)

        if not publish_success:
            # 保全/回滚谓词与失败处置下沉 _dispose_failed_delivery
            await _dispose_failed_delivery(
                channel=channel,
                item=item,
                context=context,
                client=client,
                capture_session=capture_session,
                delivered_message_ids=delivered_message_ids,
                last_split_exc=last_split_exc,
                task_store=task_store,
            )
