"""任务全生命周期编排服务 (Task Lifecycle Service)

封装任务恢复 (normal / steady 降档)、跳过当前文件并留置待回填占位卡、全局队列暂停/恢复、
单任务暂停/重试/取消/删除及并发热更新等领域编排逻辑，使 API 路由层保持薄控制器形态。
"""

import asyncio
import logging
from functools import partial
from typing import Any

from fastapi import HTTPException

from app.core.client_manager import client_manager, is_private_channel_identifier
from app.core.connection_manager import connection_manager
from app.core.entity_resolver import parse_chat_identifier, resolve_entity
from app.core.pipeline.placeholder import PENDING_CARD_TEXT, as_named_stream, load_static_poster
from app.core.runtime import (
    active_task_controls,
    requeue_task_and_clear_controls,
    resume_paused_executor,
    stop_task_with_controls,
)
from app.core.scheduler import TaskScheduler, task_scheduler
from app.core.task_strategy import StrategyRegistry
from app.db.account_repository import AccountRepository
from app.db.download_repository import DownloadBreakpointRepository
from app.db.publish_repository import PlaceholderSlotRepository, PublishHistoryRepository
from app.db.task_repository import DuplicateDownloadTaskError, TaskQueueRepository
from app.models.common import TaskActionResponse
from app.models.task import (
    BatchTaskActionRequest,
    BatchTaskActionResponse,
    BatchTaskActionResult,
    DownloadTaskConflictDetail,
    PrecheckAccessRequest,
    PrecheckAccessResponse,
    QueueResumeConflict,
    SkipCurrentFileRequest,
    UpdateTaskConcurrencyRequest,
)
from app.services.forward_service import ForwardService
from app.services.storage_service import StorageService
from app.services.upload_meta_gc import gc_task_meta

logger = logging.getLogger(__name__)


def _download_conflict_detail(conflict: DuplicateDownloadTaskError) -> DownloadTaskConflictDetail:
    message = "已有暂停的下载任务" if conflict.status == "paused" else "已有相同的下载任务"
    return DownloadTaskConflictDetail(
        code="duplicate_download_task",
        task_id=conflict.task_id,
        status=conflict.status,
        message=f"{message} (#{conflict.task_id})",
    )


def _download_retry_conflict(conflict: DuplicateDownloadTaskError) -> HTTPException:
    return HTTPException(status_code=409, detail=_download_conflict_detail(conflict).model_dump())


def _parse_precheck_target(chat_input: str) -> str | int:
    """precheck 目标归一: 统一委托共享 parse_chat_identifier。

    旧内联 rsplit 解析对 /s/ 网页预览链接取到 "s"、/joinchat/ 邀请链接取到 "joinchat"、
    畸形 /c/ 标识符会在 probe 前 int() 抛异常 —— 共享解析器全形态支持且不抛,
    与转发/下载链路的解析口径完全一致。"""
    return parse_chat_identifier(chat_input)


async def _precheck_explicit_account(account_id: int, target: str | int, chat_input: str) -> PrecheckAccessResponse:
    acc = client_manager._accounts.get(account_id)
    if not acc:
        db_acc = await AccountRepository.get_by_id_async(account_id)
        if not db_acc:
            return PrecheckAccessResponse(accessible=False, reason="指定执行的 Userbot 账号不存在")
        acc = client_manager.get_account(account_id)
        if not acc or acc.account_id != account_id:
            return PrecheckAccessResponse(accessible=False, reason="指定执行的 Userbot 账号不存在或未初始化")

    if not acc.is_ready() or not acc.client:
        return PrecheckAccessResponse(accessible=False, reason=f"Userbot 账号 [{acc.account_name}] 未连接或未完成鉴权")

    try:
        entity = await asyncio.wait_for(ForwardService.resolve_entity(acc.client, target), timeout=6.0)
        title = getattr(entity, "title", None) or getattr(entity, "first_name", None) or str(target)
        username = getattr(entity, "username", None)
        chat_id = str(getattr(entity, "id", target))
        is_restricted = bool(getattr(entity, "restricted", False) or getattr(entity, "noforwards", False))
        return PrecheckAccessResponse(
            accessible=True,
            chat_id=chat_id,
            title=f"✅ 账号 [{acc.account_name}] 具备访问权限 ({title})",
            username=username,
            is_restricted=is_restricted,
        )
    except Exception as e:
        err_msg = str(e)
        logger.warning("Dryrun 预检频道访问权限失败 (账号 %s, 目标 %s): %s", acc.account_name, chat_input, e)
        if "Cannot find any entity" in err_msg or "Could not find the input entity" in err_msg:
            reason = f"当前选择的 Userbot ({acc.account_name}) 尚未加入目标私有频道，请先加入或切换自动调度"
        elif "ChannelPrivateError" in type(e).__name__ or "private" in err_msg.lower():
            reason = f"当前选择的 Userbot ({acc.account_name}) 无权访问该私有频道 (未加入或已被移出)"
        elif "ChatAdminRequiredError" in type(e).__name__:
            reason = "访问该频道需要管理员权限"
        else:
            reason = f"访问探测失败: {e}"
        return PrecheckAccessResponse(accessible=False, reason=reason)


async def prepare_task_resume(task: dict[str, Any], *, persist: bool = True) -> dict[str, Any]:
    """统一的恢复/重试前置参数预处理与断点计算闭环 (委托至策略模式实现)"""
    task_id = str(task["id"])
    task_type = str(task.get("task_type") or "")
    params = task.get("params") or {}
    patch_data: dict[str, Any] = {}

    strategy = StrategyRegistry.get_optional(task_type)
    if strategy:
        is_completed, patch_data, msg = strategy.validate_and_prepare_resume(task)
        if is_completed:
            return {"completed": True, "message": msg}

    if patch_data and persist:
        await TaskQueueRepository.merge_params_async(task_id, patch_data)
        params.update(patch_data)

    return {"completed": False, "patch": patch_data}


def _pikpak_resume_patch(
    task_type: str | None,
    params: dict[str, Any],
    mode: str,
) -> dict[str, Any]:
    """计算恢复时的稳态降档参数或风控计数复位。"""
    if task_type != "pikpak_publish":
        return {}
    if mode == "steady":
        return {
            "pikpak_prefetch_concurrency": 1,
            "upload_concurrency": 1,
            "send_delay_seconds": max(float(params.get("send_delay_seconds") or 3.0), 20.0),
            "auto_downshifted": True,
            "safe_paused": False,
            "throttle_strike_count": 0,
            "data_risk_strike_count": 0,
        }
    if params.get("safe_paused") or params.get("throttle_strike_count") or params.get("data_risk_strike_count"):
        return {"safe_paused": False, "throttle_strike_count": 0, "data_risk_strike_count": 0}
    return {}


async def _apply_pikpak_resume_mode(
    task_id: str,
    task_type: str | None,
    params: dict[str, Any],
    mode: str,
    sched: TaskScheduler,
) -> None:
    """运行中执行器会重新读取已落库的稳态参数。"""
    if task_type != "pikpak_publish":
        return
    if patch := _pikpak_resume_patch(task_type, params, mode):
        await TaskQueueRepository.merge_params_async(task_id, patch)
    if mode == "steady":
        sched.update_download_threads(task_id, 1)
    await TaskQueueRepository.set_error_detail_async(task_id, "")


def _locate_skip_candidate(
    remote_files: list[Any],
    target_fid: str | None,
    target_chat_id: str,
    source_type: str,
) -> dict[str, Any]:
    current_file: dict[str, Any] | None = None
    if target_fid:
        for rf in remote_files:
            fid = rf.get("file_id") if isinstance(rf, dict) else getattr(rf, "file_id", None)
            if str(fid) == str(target_fid):
                current_file = rf if isinstance(rf, dict) else dict(rf.__dict__)
                break
        if not current_file:
            raise HTTPException(status_code=400, detail=f"未在任务待发布清单中找到指定文件 ID: {target_fid}")
        if PublishHistoryRepository.is_published(
            target_chat_id, "", 0, source_type=source_type, remote_file_id=str(target_fid)
        ):
            raise HTTPException(status_code=400, detail=f"文件 ID [{target_fid}] 已发布完成或已跳过，无需重复跳过")

    if not current_file and remote_files:
        for rf in remote_files:
            rf_dict = rf if isinstance(rf, dict) else dict(rf.__dict__)
            fid = rf_dict.get("file_id")
            if fid and not PublishHistoryRepository.is_published(
                target_chat_id, "", 0, source_type=source_type, remote_file_id=str(fid)
            ):
                current_file = rf_dict
                break

    if not current_file:
        raise HTTPException(status_code=400, detail="未在任务待发布清单中找到可跳过的未完成文件")
    return current_file


async def _ensure_skip_placeholder_slot(
    task_id: str,
    raw_acc_id: int | None,
    target_chat_id: str,
    slot_identity: str,
    file_name: str,
    file_size: int,
    skip_reason: str,
) -> int:
    existing_slot = PlaceholderSlotRepository.get_by_identity(target_chat_id, slot_identity, file_size)
    if existing_slot and existing_slot.get("placeholder_msg_id"):
        msg_id = int(existing_slot["placeholder_msg_id"])
        PlaceholderSlotRepository.increment_fail(existing_slot["id"], skip_reason)
        logger.info("复用既有占位卡 (msg_id: %d, 槽ID: %d), 避免重复发卡刷屏", msg_id, existing_slot["id"])
        return msg_id

    msg_id = 0
    acc = client_manager.get_account(raw_acc_id or None)
    client = acc.client if acc and acc.is_ready() else None
    if client:
        try:
            poster_data = await asyncio.to_thread(load_static_poster)
            card_caption = PENDING_CARD_TEXT.format(name=file_name)
            try:
                entity = await resolve_entity(client, target_chat_id)
            except Exception:
                entity = target_chat_id
            if poster_data:
                sent_msg = await client.send_file(entity, as_named_stream(poster_data), caption=card_caption)
            else:
                sent_msg = await client.send_message(entity, card_caption)
            msg_id = int(getattr(sent_msg, "id", 0) or 0)
            logger.info("已在目标频道 [%s] 成功留置跳过占位卡 (msg_id: %d)", target_chat_id, msg_id)
        except Exception as card_err:
            logger.warning("留置跳过占位卡异常 (非致命，降级无实体卡): %s", card_err)

    if msg_id:
        try:
            PlaceholderSlotRepository.insert_new_slot(
                target_chat_id,
                slot_identity,
                file_size,
                slot_kind="backfillable",
                state="placeholder_sent",
                placeholder_msg_id=msg_id,
                client_label=acc.account_name if acc else "Userbot",
                sender_account_id=acc.account_id if acc else 0,
                task_id=task_id,
                fail_reason=skip_reason,
            )
        except Exception as slot_err:
            logger.debug("登记跳过占位槽异常 (非致命): %s", slot_err)
    return msg_id


def _sync_purge_task_files_and_meta(
    tid: str, should_delete: bool, bp_paths_snapshot: dict[str, list[str]] | None = None
) -> tuple[list[str], list[dict[str, Any]]]:
    f_deleted: list[str] = []
    f_skipped: list[dict[str, Any]] = []
    if should_delete:
        # 断点行随任务行 DB 级联清除: 删除已预留时必须用删除前快照的路径清理文件
        bp_paths = (
            bp_paths_snapshot
            if bp_paths_snapshot is not None
            else DownloadBreakpointRepository.get_breakpoint_paths_by_task(tid)
        )
        targets = [p for p in bp_paths["part"] + bp_paths["meta"] if p]
        if targets:
            result = StorageService.delete_files(targets)
            f_deleted = list(result.get("deleted") or [])
            f_skipped = list(result.get("skipped") or [])
    try:
        gc_task_meta(tid)
    except Exception as gc_err:
        logger.warning("任务 %s upload_meta GC 失败 (忽略, 不阻塞删除): %s", tid, gc_err)
    return f_deleted, f_skipped


def _sync_batch_gc(tids: list[str]) -> None:
    for tid in tids:
        try:
            gc_task_meta(tid)
        except Exception as gc_err:
            logger.warning("任务 %s upload_meta GC 失败: %s", tid, gc_err)


async def _await_executor_drain(
    task_id: str,
    expected_status: str,
    sched: TaskScheduler,
    op_label: str,
    max_polls: int = 32,
) -> dict[str, Any]:
    """旧执行器尚未完全退出 (cancel/pause 只置标志不等待) 时有界等待收尾，超时或状态变更则 409 拒绝"""
    if sched.is_task_running(task_id):
        drained = False
        for _ in range(max_polls):
            if not sched.is_task_running(task_id):
                drained = True
                break
            await asyncio.sleep(0.25)
        if not drained and sched.is_task_running(task_id):
            raise HTTPException(
                status_code=409,
                detail=f"任务执行器尚未完全退出 (收尾中)，请稍后再试{op_label}操作",
            )
    latest = await TaskQueueRepository.get_by_id_async(task_id)
    if not latest or latest.get("status") != expected_status:
        raise HTTPException(status_code=409, detail=f"任务状态在{op_label}期间已变更，请刷新后重试")
    return latest


class TaskLifecycleService:
    """任务生命周期控制服务"""

    @staticmethod
    async def precheck_access(req: PrecheckAccessRequest) -> PrecheckAccessResponse:
        chat_input = (req.chat_identifier or "").strip()
        if not chat_input:
            return PrecheckAccessResponse(accessible=False, reason="目标频道标识不能为空")

        target = _parse_precheck_target(chat_input)
        if req.account_id is not None:
            return await _precheck_explicit_account(req.account_id, target, chat_input)

        ready_accounts = [
            a for a in client_manager.get_all_accounts() if a.is_active and a.is_ready() and not a.is_flood_locked()
        ]
        if not ready_accounts:
            return PrecheckAccessResponse(accessible=False, reason="账号池内暂无任何已连接且可用的活跃 Userbot 账号")

        accessible_accounts = []
        first_entity = None
        for cand in ready_accounts:
            try:
                if cand.client and cand.client.is_connected():
                    entity = await asyncio.wait_for(ForwardService.resolve_entity(cand.client, target), timeout=3.5)
                    if entity:
                        accessible_accounts.append(cand)
                        if not first_entity:
                            first_entity = entity
            except Exception as exc:
                logger.debug(
                    "候选账号 %s 探测目标 %s 解析失败 (非致命跳过): %s", getattr(cand, "id", None), target, exc
                )

        if accessible_accounts:
            first_acc = accessible_accounts[0]
            acc_count = len(accessible_accounts)
            title_val = getattr(first_entity, "title", None) or getattr(first_entity, "first_name", None) or str(target)
            username_val = getattr(first_entity, "username", None)
            is_restr = bool(getattr(first_entity, "restricted", False) or getattr(first_entity, "noforwards", False))
            chat_id_val = str(getattr(first_entity, "id", target))
            msg = (
                f"✅ 账号池中 [{first_acc.account_name}] 等 {acc_count} 个账号具备该私有频道访问权限，可安全提交"
                if is_private_channel_identifier(chat_input)
                else f"✅ 目标频道访问正常 ({title_val})，全账号池可达"
            )
            return PrecheckAccessResponse(
                accessible=True, chat_id=chat_id_val, title=msg, username=username_val, is_restricted=is_restr
            )

        fail_reason = (
            "❌ 账号池内暂无任何账号加入该私有频道"
            if is_private_channel_identifier(chat_input)
            else "❌ 账号池内暂无任何账号可访问该频道/群组 (请检查链接或网络)"
        )
        return PrecheckAccessResponse(accessible=False, reason=fail_reason)

    @staticmethod
    async def pause_queue() -> dict[str, Any]:
        task_ids = await TaskQueueRepository.get_ids_by_statuses_async(("running", "queued", "pending"))
        paused_count = 0
        for tid in task_ids:
            if not await asyncio.to_thread(stop_task_with_controls, tid, "paused", ("running", "queued", "pending")):
                continue
            await TaskQueueRepository.append_log_async(tid, "[调度器] 全局队列暂停：任务已安全转置为暂停，释放网络资源")
            await connection_manager.broadcast({"type": "task_update", "task_id": tid, "status": "paused"})
            paused_count += 1
        return {"status": "ok", "paused_count": paused_count, "message": f"已暂停整个队列 (共 {paused_count} 个任务)"}

    @staticmethod
    async def resume_queue(sched: TaskScheduler | None = None) -> dict[str, Any]:
        active_sched = sched or task_scheduler
        task_ids = await TaskQueueRepository.get_ids_by_statuses_async(("paused",))
        resumed_count = 0
        skipped_conflicts: list[QueueResumeConflict] = []
        for tid in task_ids:
            t_rec = await TaskQueueRepository.get_by_id_async(tid)
            if not t_rec or t_rec["status"] != "paused":
                continue
            version = t_rec.get("updated_at")
            prep_res = await prepare_task_resume(t_rec, persist=False)
            if prep_res.get("completed"):
                if await TaskQueueRepository.update_status_async(
                    tid, "completed", from_statuses=("paused",), expected_updated_at=version
                ):
                    await TaskQueueRepository.append_log_async(
                        tid, f"[调度器] 全局队列恢复：{prep_res.get('message', '任务已全部完成')}"
                    )
                    await connection_manager.broadcast({"type": "task_update", "task_id": tid, "status": "completed"})
                continue
            params_patch = prep_res.get("patch", {})
            if active_sched.is_task_running(tid):
                # 活执行器仍存活 (live-paused): 仅清除暂停标志原地续跑, 重排队会造成同任务双执行器
                if not await asyncio.to_thread(resume_paused_executor, tid, version, params_patch=params_patch):
                    continue
                await TaskQueueRepository.append_log_async(tid, "[调度器] 全局队列恢复：任务已解除暂停原地续跑")
                await connection_manager.broadcast({"type": "task_update", "task_id": tid, "status": "running"})
                resumed_count += 1
                continue
            try:
                requeued = await asyncio.to_thread(
                    requeue_task_and_clear_controls,
                    tid,
                    partial(
                        TaskQueueRepository.requeue_task_for_resume,
                        tid,
                        "paused",
                        params_patch=params_patch,
                        expected_updated_at=version,
                    ),
                )
            except DuplicateDownloadTaskError as conflict:
                skipped_conflicts.append(QueueResumeConflict(task_id=tid, conflict=_download_conflict_detail(conflict)))
                continue
            if not requeued:
                continue
            await TaskQueueRepository.append_log_async(tid, "[调度器] 全局队列恢复：任务已重新进入排队调度队列")
            await connection_manager.broadcast({"type": "task_update", "task_id": tid, "status": "queued"})
            resumed_count += 1

        if active_sched.is_running:
            active_sched.notify_new_task()
        message = f"已恢复队列调度 (共 {resumed_count} 个任务)"
        if skipped_conflicts:
            message += f"，{len(skipped_conflicts)} 个同键冲突任务保持暂停"
        return {
            "status": "ok",
            "resumed_count": resumed_count,
            "message": message,
            "skipped_conflicts": [item.model_dump() for item in skipped_conflicts],
        }

    @staticmethod
    async def pause_task(task_id: str, sched: TaskScheduler | None = None) -> dict[str, str]:
        active_sched = sched or task_scheduler
        task = await TaskQueueRepository.get_by_id_async(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="任务不存在")

        status = task.get("status")
        if status == "paused":
            if not await asyncio.to_thread(stop_task_with_controls, task_id, "paused", ("paused",)):
                raise HTTPException(status_code=409, detail="任务状态在暂停期间已变更，请刷新后重试")
            return {"status": "paused"}

        if status in ("queued", "pending"):
            if not await asyncio.to_thread(
                stop_task_with_controls, task_id, "paused", ("queued", "pending", "running")
            ):
                raise HTTPException(status_code=409, detail="任务状态在暂停期间已变更，请刷新后重试")
            active_sched.clear_slot_contention_wait(task_id)
            await TaskQueueRepository.append_log_async(task_id, "[调度器] 排队中的任务已手动暂停")
            await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "paused"})
            return {"status": "paused"}

        if status != "running":
            raise HTTPException(
                status_code=409, detail=f"当前状态为 {status} 的任务无法暂停 (仅运行中或排队中的任务可暂停)"
            )

        if not await asyncio.to_thread(stop_task_with_controls, task_id, "paused", ("running", "paused")):
            raise HTTPException(status_code=409, detail="任务状态在暂停期间已变更，请刷新后重试")
        active_sched.cleanup_finished_executor(task_id, task.get("account_id"))
        log_line = (
            "[调度器] 用户触发暂停操作，执行器将自行断开底层通道并让位"
            if active_sched.is_task_running(task_id)
            else "[调度器] 任务已置为暂停，保留已有断点"
        )
        await TaskQueueRepository.append_log_async(task_id, log_line)

        await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "paused"})
        return {"status": "paused"}

    @staticmethod
    async def resume_task(
        task_id: str,
        mode: str = "normal",
        sched: TaskScheduler | None = None,
        drain_polls: int = 32,
    ) -> dict[str, Any]:
        active_sched = sched or task_scheduler
        task = await TaskQueueRepository.get_by_id_async(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="任务不存在")

        status = task.get("status")
        task_type = str(task.get("task_type") or "")
        params = task.get("params") or {}

        # 1. 暂停中且后台协程仍存活: 清除 paused 标志即可继续。
        #    控制条目存在不足以证明执行器存活 (排队任务被暂停时条目仍可能残留) —— 必须以调度器
        #    运行集合为准, 否则会在位置 running 后无执行器认领, 任务永久卡 running。
        if status == "paused" and task_id in active_task_controls and active_sched.is_task_running(task_id):
            return await TaskLifecycleService._resume_live_task(task_id, task, mode, active_sched)

        # 2. 排队中任务: 清理冷却时间戳直接唤醒
        if status == "queued":
            if active_sched.is_waiting_for_slot_contention(task_id) is True:
                raise HTTPException(status_code=409, detail="任务正在等待占位卡释放，请等待自动复查")
            try:
                reset_ok = await TaskQueueRepository.reset_task_for_retry_async(
                    task_id,
                    from_statuses=("queued",),
                    expected_updated_at=task.get("updated_at"),
                    params_patch=_pikpak_resume_patch(task_type, params, mode),
                )
            except DuplicateDownloadTaskError as conflict:
                raise _download_retry_conflict(conflict) from conflict
            if not reset_ok:
                raise HTTPException(status_code=409, detail="任务状态在恢复期间已变更，无法重新入队")
            if task_type == "pikpak_publish":
                if mode == "steady":
                    active_sched.update_download_threads(task_id, 1)
                active_sched.clear_pikpak_cooldown()
            feedback_msg = "已以低速稳态恢复并重新排队调度" if mode == "steady" else "冷却已解除，任务已重新排队调度"
            await TaskQueueRepository.append_log_async(task_id, f"[调度器] {feedback_msg}")
            if active_sched.is_running:
                active_sched.notify_new_task()
            await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "queued"})
            return {"status": "queued", "message": feedback_msg}

        # 3. 已取消/失败/已释放协程的暂停任务
        if status in ("cancelled", "failed", "paused"):
            return await TaskLifecycleService._resume_inactive_task(
                task_id, task, mode, active_sched, drain_polls=drain_polls
            )

        # 4. 运行中任务在线切换稳态模式
        if status == "running":
            if mode == "steady" and task_type == "pikpak_publish":
                await _apply_pikpak_resume_mode(task_id, task_type, params, mode, active_sched)
                active_sched.clear_pikpak_cooldown()
                log_line = "[调度器] 用户在线触发低速稳态降档：已锁定单预取通道/单并发/20s发送延迟"
                await TaskQueueRepository.append_log_async(task_id, log_line)
                await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "running"})
                return {"status": "running", "message": "已将运行中任务在线切换至低速稳态模式"}
            return {"status": "running", "message": "任务已在运行中"}

        raise HTTPException(status_code=400, detail=f"当前状态 {status} 的任务不支持恢复")

    @staticmethod
    async def _resume_live_task(task_id: str, task: dict[str, Any], mode: str, sched: TaskScheduler) -> dict[str, str]:
        task_type = task.get("task_type")
        params_patch = _pikpak_resume_patch(task_type, task.get("params") or {}, mode)
        if not await asyncio.to_thread(
            resume_paused_executor,
            task_id,
            task.get("updated_at"),
            params_patch=params_patch,
            error_detail="" if task_type == "pikpak_publish" else None,
        ):
            raise HTTPException(status_code=409, detail="任务状态在恢复期间已变更，请刷新后重试")
        if task_type == "pikpak_publish":
            if mode == "steady":
                sched.update_download_threads(task_id, 1)
            sched.clear_pikpak_cooldown()
        log_msg = (
            "[调度器] 用户触发低速稳态恢复：已锁定单预取通道/单并发/20s发送延迟并恢复运行"
            if mode == "steady"
            else "[调度器] 任务已解除暂停并恢复运行"
        )
        await TaskQueueRepository.append_log_async(task_id, log_msg)
        await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "running"})
        return {
            "status": "running",
            "message": "任务已以低速稳态恢复运行" if mode == "steady" else "任务已恢复运行",
        }

    @staticmethod
    async def _resume_inactive_task(
        task_id: str,
        task: dict[str, Any],
        mode: str,
        active_sched: TaskScheduler,
        drain_polls: int = 32,
    ) -> dict[str, Any]:
        # 守卫必须早于 prepare_task_resume / active_task_controls.pop / 状态写入:
        # 若刚 cancel 的旧执行器仍在收尾, 抹掉 cancelled 标志或重排队会导致幽灵复活与双执行器并发
        expected_status = str(task.get("status") or "")
        task = await _await_executor_drain(task_id, expected_status, active_sched, "恢复", max_polls=drain_polls)
        version = task.get("updated_at")
        prep_res = await prepare_task_resume(task, persist=False)
        if prep_res.get("completed"):
            if not await TaskQueueRepository.update_status_async(
                task_id, "completed", from_statuses=(expected_status,), expected_updated_at=version
            ):
                raise HTTPException(status_code=409, detail="任务状态在恢复期间已变更，请刷新后重试")
            msg = str(prep_res.get("message") or "任务已全部完成")
            await TaskQueueRepository.append_log_async(task_id, f"[调度器] {msg}")
            await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "completed"})
            return {"status": "completed", "message": msg}

        task_type = str(task.get("task_type") or "")
        params = task.get("params") or {}
        raw_acc_id = task.get("account_id")

        params_patch = {**_pikpak_resume_patch(task_type, params, mode), **prep_res.get("patch", {})}
        params = {**params, **params_patch}

        acc = client_manager.get_account(raw_acc_id)
        acc_id = acc.account_id if acc else 0
        max_acc_concurrency = acc.max_concurrency if acc else 1
        max_global = active_sched.get_max_global_concurrency()

        cur_global = active_sched.get_running_task_count()
        cur_acc = active_sched.get_account_task_count(acc_id)
        has_capacity = (
            (cur_global < max_global) and (cur_acc < max_acc_concurrency) and not (acc and acc.is_flood_locked())
        )

        if not has_capacity:
            feedback_msg = (
                "当前并发已满，已将任务以低速稳态置于就绪队列首位，空闲时优先运行"
                if mode == "steady"
                else "当前并发已满，已将任务置于就绪队列首位，空闲时优先运行"
            )
        else:
            feedback_msg = (
                "任务已以低速稳态恢复运行并加入调度队列" if mode == "steady" else "任务已恢复运行并加入调度队列"
            )

        strategy = StrategyRegistry.get_optional(task_type)
        if not strategy:
            raise HTTPException(status_code=400, detail=f"任务类型 {task_type} 不支持恢复")

        try:
            strategy.validate_required_params(params)
        except ValueError as err:
            raise HTTPException(status_code=400, detail=str(err)) from err

        log_line = (
            f"[调度器] 用户触发低速稳态恢复：已锁定单预取通道/单上传并发/20s发送延迟并以金丝雀单块模式平滑恢复 ({feedback_msg})"
            if mode == "steady"
            else strategy.get_resume_log_message(feedback_msg, params)
        )
        try:
            requeued = await asyncio.to_thread(
                requeue_task_and_clear_controls,
                task_id,
                partial(
                    TaskQueueRepository.requeue_task_for_resume,
                    task_id,
                    expected_status,
                    params_patch=params_patch,
                    priority_floor=None if has_capacity else 50,
                    expected_updated_at=version,
                    error_detail="" if task_type == "pikpak_publish" else None,
                ),
            )
        except DuplicateDownloadTaskError as conflict:
            raise _download_retry_conflict(conflict) from conflict
        if not requeued:
            raise HTTPException(status_code=409, detail="任务状态在恢复期间已变更，请刷新后重试")
        await TaskQueueRepository.append_log_async(task_id, log_line)
        if active_sched.is_running:
            if task_type == "pikpak_publish":
                active_sched.clear_pikpak_cooldown()
            active_sched.notify_new_task()

        await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "queued"})
        return {"status": "queued", "message": feedback_msg, "queued_first": not has_capacity}

    @staticmethod
    async def skip_current_file(task_id: str, req: SkipCurrentFileRequest | None = None) -> TaskActionResponse:
        task = await TaskQueueRepository.get_by_id_async(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="任务不存在")

        status = str(task.get("status") or "")
        if status == "running":
            raise HTTPException(status_code=409, detail="当前任务正在运行中，请先暂停任务后再执行跳过当前文件操作")
        if status not in ("paused", "failed", "queued", "cancelled"):
            raise HTTPException(
                status_code=400,
                detail=f"当前状态 {status} 的任务不支持跳过文件操作 (仅暂停中/已失败/排队中任务支持)",
            )

        task_type = str(task.get("task_type") or "")
        params = task.get("params") or {}
        target_channel = params.get("target_channel") or params.get("target_chat")
        if not target_channel:
            raise HTTPException(status_code=400, detail="任务未配置目标频道，无法执行跳过与占位卡留置")

        target_chat_id = str(parse_chat_identifier(target_channel))
        source_type = "pikpak" if task_type == "pikpak_publish" else "local"
        current_file = _locate_skip_candidate(
            params.get("remote_files") or [],
            req.file_id if req and req.file_id else None,
            target_chat_id,
            source_type,
        )

        fid = str(current_file.get("file_id") or "")
        file_name = str(current_file.get("file_name") or current_file.get("name") or fid)
        file_size = int(
            current_file.get("file_size_bytes") or current_file.get("size_bytes") or current_file.get("file_size") or 0
        )
        parent_folder_id = str(current_file.get("parent_folder_id") or current_file.get("parent_id") or "")
        slot_identity = f"pikpak://{parent_folder_id}/{file_name}" if task_type == "pikpak_publish" else file_name
        skip_reason = req.reason if req and req.reason else "用户手动跳过"

        msg_id = await _ensure_skip_placeholder_slot(
            task_id,
            task.get("account_id"),
            target_chat_id,
            slot_identity,
            file_name,
            file_size,
            skip_reason,
        )

        PublishHistoryRepository.record(
            target_chat_id,
            slot_identity,
            file_size,
            "",
            source_type=source_type,
            remote_file_id=fid if fid else "",
            tg_message_id=msg_id,
            task_id=task_id,
        )

        if fid:
            await TaskQueueRepository.clear_part_checkpoint_async(task_id, fid)

        skipped_item = {
            "file_name": file_name,
            "rel_path": slot_identity,
            "file_size_bytes": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2),
            "oversize_kind": "manual_skip",
            "reason": skip_reason,
        }
        existing_skipped = list(params.get("skipped_files") or [])
        existing_skipped.append(skipped_item)
        await TaskQueueRepository.merge_params_async(task_id, {"skipped_files": existing_skipped})

        card_tip = f"并在频道留置 ⏳ 待回填占位卡 (msg #{msg_id})" if msg_id else "（未留置实体占位卡）"
        log_text = f"⏭️ [用户操作] 用户手动跳过文件 [{file_name}] {card_tip}，防重账本已生效，后续调度将自动跳过"
        await TaskQueueRepository.append_log_async(task_id, log_text)
        await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": status})

        return TaskActionResponse(
            task_id=task_id,
            status=status,
            message=f"已成功跳过文件 [{file_name}]" + (f" 并留置占位卡 (msg #{msg_id})" if msg_id else ""),
        )

    @staticmethod
    async def retry_task(
        task_id: str,
        sched: TaskScheduler | None = None,
        drain_polls: int = 32,
    ) -> dict[str, Any]:
        active_sched = sched or task_scheduler
        task = await TaskQueueRepository.get_by_id_async(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="任务不存在")

        status = str(task.get("status") or "")
        if status not in ("failed", "cancelled", "paused"):
            raise HTTPException(
                status_code=400,
                detail=f"当前状态为 {status} 的任务无法直接重试 (仅已失败、已取消或暂停中的任务支持重试)",
            )

        # 旧执行器尚未完全退出 (cancel/pause 只置标志不等待): 有界等待收尾后再准备恢复,
        # 防止收尾中的 cancelled 执行器被 prepare_task_resume 误写为 completed
        task = await _await_executor_drain(task_id, status, active_sched, "重试", max_polls=drain_polls)

        version = task.get("updated_at")
        prep_res = await prepare_task_resume(task, persist=False)
        if prep_res.get("completed"):
            if active_sched.is_task_running(task_id):
                raise HTTPException(status_code=409, detail="任务执行器尚未完全退出 (收尾中)，请稍后再试重试操作")
            completed_ok = await TaskQueueRepository.update_status_async(
                task_id, "completed", from_statuses=(status,), expected_updated_at=version
            )
            if not completed_ok:
                raise HTTPException(status_code=409, detail="任务状态在重试期间已变更，请刷新后重试")
            msg = str(prep_res.get("message") or "任务在指定范围内已全部完成")
            await TaskQueueRepository.append_log_async(task_id, f"[调度器] 重试检测：{msg}")
            await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "completed"})
            return {"status": "completed", "task_id": task_id, "message": msg}

        # 比较准备前的行版本，避免任务回到同名状态后由旧请求重新入队。
        if active_sched.is_task_running(task_id):
            raise HTTPException(status_code=409, detail="任务执行器正在运行或状态已变更，请刷新后重试")

        try:
            reset_ok = await asyncio.to_thread(
                requeue_task_and_clear_controls,
                task_id,
                partial(
                    TaskQueueRepository.retry_task,
                    task_id,
                    expected_status=status,
                    expected_updated_at=version,
                    params_patch=prep_res.get("patch", {}),
                ),
            )
        except DuplicateDownloadTaskError as conflict:
            raise _download_retry_conflict(conflict) from conflict
        if not reset_ok:
            raise HTTPException(status_code=409, detail="任务状态在重试期间已变更，无法重新入队")
        if task.get("task_type") == "pikpak_publish":
            active_sched.clear_pikpak_cooldown()
        await TaskQueueRepository.append_log_async(task_id, "🔄 用户触发重试操作，任务已重新进入排队调度队列...")
        if active_sched.is_running:
            active_sched.notify_new_task()

        await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "queued"})
        return {"status": "queued", "task_id": task_id, "message": "任务已重新进入排队调度"}

    @staticmethod
    async def cancel_task(task_id: str) -> dict[str, str]:
        task = await TaskQueueRepository.get_by_id_async(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="任务不存在")

        status = task.get("status")
        if status not in ("pending", "running", "paused", "queued"):
            raise HTTPException(
                status_code=409, detail=f"当前状态为 {status} 的任务无法取消 (仅运行中/暂停中/排队中的任务可取消)"
            )

        if not await asyncio.to_thread(
            stop_task_with_controls, task_id, "cancelled", ("pending", "running", "paused", "queued")
        ):
            raise HTTPException(status_code=409, detail="任务状态在取消期间已变更，请刷新后重试")
        task_scheduler.clear_slot_contention_wait(task_id)
        await TaskQueueRepository.append_log_async(task_id, "任务已被手动取消")
        await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "cancelled"})
        return {"status": "cancelled"}

    @staticmethod
    async def delete_task(task_id: str, delete_files: bool = False) -> dict[str, Any]:
        task = await TaskQueueRepository.get_by_id_async(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="任务不存在")

        status = task.get("status")
        # 删除守卫: 调度器运行集合中的执行器 (含 cancelled 收尾中) 一律拒绝删除 —— 删除会清掉
        # 取消标志与断点, 幽灵执行器将为已删除任务继续工作。控制条目仅在执行器确已退出
        # (陈旧泄漏) 时不阻塞删除, 行删除后统一 pop 兜底。
        if (
            status in ("running", "pending", "queued")
            or task_scheduler.is_task_running(task_id)
            or (status == "paused" and task_id in active_task_controls)
        ):
            raise HTTPException(
                status_code=400, detail="不能删除正在运行/暂停中的任务 (包括排队中任务)，请先【取消】任务后再删除"
            )

        # 先快照断点路径 (任务行删除会 DB 级联清掉断点行) → 原子预留删除 (409 时绝不触碰
        # 断点/meta, 旧顺序的"任务仍在但上传元数据已丢"残局不再存在) → 按快照清理文件与元数据
        bp_snapshot = DownloadBreakpointRepository.get_breakpoint_paths_by_task(task_id) if delete_files else None
        deleted = await TaskQueueRepository.delete_async(
            task_id, allowed_statuses=("completed", "cancelled", "failed", "paused")
        )
        if not deleted:
            raise HTTPException(
                status_code=409, detail="任务已被调度器认领运行或状态已变更，无法删除，请先【取消】任务后再删除"
            )
        try:
            files_deleted, files_skipped = await asyncio.to_thread(
                _sync_purge_task_files_and_meta, task_id, delete_files, bp_snapshot
            )
        except Exception as purge_err:
            logger.warning("任务 %s 删除后文件/元数据清理异常 (孤儿由 GC 兜底): %s", task_id, purge_err)
            files_deleted, files_skipped = [], []
        active_task_controls.pop(task_id, None)
        await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "deleted"})
        return {"status": "deleted", "files_deleted": files_deleted, "files_skipped": files_skipped}

    @staticmethod
    async def batch_action(req: BatchTaskActionRequest, sched: TaskScheduler | None = None) -> BatchTaskActionResponse:
        action = req.action.lower()
        if action not in ("pause", "resume", "cancel", "delete", "retry"):
            raise HTTPException(status_code=400, detail="action 仅支持 pause | resume | cancel | delete | retry")

        results: list[BatchTaskActionResult] = []
        success_count = 0
        for tid in req.task_ids:
            try:
                if action == "pause":
                    res_p = await TaskLifecycleService.pause_task(tid, sched=sched)
                    results.append(BatchTaskActionResult(task_id=tid, success=True, status=res_p["status"]))
                elif action == "resume":
                    res_r = await TaskLifecycleService.resume_task(tid, sched=sched, drain_polls=2)
                    results.append(BatchTaskActionResult(task_id=tid, success=True, status=res_r["status"]))
                elif action == "cancel":
                    res_c = await TaskLifecycleService.cancel_task(tid)
                    results.append(BatchTaskActionResult(task_id=tid, success=True, status=res_c["status"]))
                elif action == "delete":
                    await TaskLifecycleService.delete_task(tid, delete_files=bool(req.delete_files))
                    results.append(BatchTaskActionResult(task_id=tid, success=True, status="deleted"))
                else:
                    res_rt = await TaskLifecycleService.retry_task(tid, sched=sched, drain_polls=2)
                    results.append(BatchTaskActionResult(task_id=tid, success=True, status=res_rt["status"]))
                success_count += 1
            except HTTPException as he:
                if isinstance(he.detail, dict):
                    error = str(he.detail.get("message") or "任务操作失败")
                    conflict_id = he.detail.get("task_id")
                    if conflict_id and f"#{conflict_id}" not in error:
                        error = f"{error} (#{conflict_id})"
                else:
                    error = str(he.detail)
                results.append(BatchTaskActionResult(task_id=tid, success=False, error=error))
            except Exception as e:
                results.append(BatchTaskActionResult(task_id=tid, success=False, error=str(e)))

        return BatchTaskActionResponse(
            action=action, total=len(req.task_ids), success_count=success_count, results=results
        )

    @staticmethod
    async def clear_finished_tasks(sched: TaskScheduler | None = None) -> dict[str, str]:
        active_sched = sched or task_scheduler
        running_ids = sorted(tid for tid in active_sched.get_running_task_ids() if active_sched.is_task_running(tid))
        if running_ids:
            logger.info("clear_finished_tasks 跳过仍在收尾的运行中执行器: %s", running_ids)
        finished_ids = await TaskQueueRepository.clear_finished_async(exclude_ids=running_ids)
        for tid in finished_ids:
            if not active_sched.is_task_running(tid):
                active_task_controls.pop(tid, None)
        if finished_ids:
            try:
                await asyncio.to_thread(_sync_batch_gc, finished_ids)
            except Exception as e:
                logger.warning("批量 upload_meta GC 异常: %s", e)
        await connection_manager.broadcast({"type": "task_update", "status": "cleared"})
        return {"status": "cleared"}

    @staticmethod
    async def update_task_concurrency(
        task_id: str,
        req: UpdateTaskConcurrencyRequest,
        sched: TaskScheduler | None = None,
    ) -> dict[str, Any]:
        active_sched = sched or task_scheduler
        task = await TaskQueueRepository.get_by_id_async(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="任务不存在")

        task_type = str(task.get("task_type") or task.get("type") or "")
        strategy = StrategyRegistry.get_optional(task_type)
        max_c = strategy.max_concurrency_limit if strategy else 32
        if req.concurrency < 1 or req.concurrency > max_c:
            raise HTTPException(status_code=400, detail=f"{task_type} 任务并发通道数必须介于 1 到 {max_c} 之间")

        def _apply_strategy_concurrency(merger_task_type: str, params: dict[str, Any]) -> None:
            merger_strategy = StrategyRegistry.get_optional(merger_task_type)
            if merger_strategy:
                merger_strategy.apply_concurrency_update(params, req.concurrency, req.upload_in_flight_per_connection)
            else:
                params["threads"] = req.concurrency

        await TaskQueueRepository.update_concurrency_async(
            task_id, req.concurrency, req.upload_in_flight_per_connection, param_merger=_apply_strategy_concurrency
        )
        if task_type in ("download_link", "download_range") and (
            task.get("status") == "running" or active_sched.has_thread_allocation(task_id)
        ):
            active_sched.update_download_threads(task_id, req.concurrency)
        bcast: dict[str, Any] = {"type": "task_update", "task_id": task_id, "concurrency_total": req.concurrency}
        if req.upload_in_flight_per_connection is not None:
            bcast["upload_in_flight_per_connection"] = req.upload_in_flight_per_connection
        await connection_manager.broadcast(bcast)
        return {
            "task_id": task_id,
            "concurrency": req.concurrency,
            "upload_in_flight_per_connection": req.upload_in_flight_per_connection,
            "status": "updated",
        }
