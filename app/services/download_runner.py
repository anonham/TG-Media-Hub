"""下载任务编排 (Download Runner) —— D4 重组自 download_executor_service。

承载 run_download_links_task / run_download_range_task 两个执行入口 (策略 execute
唯一调用) 与其 ≤120 行协作函数组: 启动配置解析、账号故障转移、Tier2 联合舰队、
Tier1 中转桥下载、完成态收尾。进度回调经共享工厂 make_executor_progress_cb。
"""

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from telethon.errors import FloodWaitError

from app.config import settings
from app.core.connection_manager import connection_manager
from app.core.entity_resolver import normalize_download_chat_key
from app.core.executor_progress import make_executor_progress_cb
from app.core.runtime import (
    active_task_controls,
    is_task_cancelled,
    is_task_paused,
    mark_task_executor_exit,
    mark_task_pause_exit,
    release_runner_controls,
    try_claim_executor_entry,
)
from app.core.scheduler import TaskCancelledError, TaskPausedError, task_scheduler
from app.core.session_errors import is_session_invalid_error, report_session_invalid
from app.db.task_repository import TaskQueueRepository
from app.db.write_pool import submit_db_write
from app.services.downloader_service import DownloaderService, _verify_final_file_nonempty_sync

logger = logging.getLogger(__name__)

# 任务失败汇总日志最多展示的失败条目数 (完整逐条明细已在下载过程中实时写入任务日志)
_FAILED_ITEMS_LOG_LIMIT = 20

_PAUSED_LOG = "[调度器] 任务已安全暂停，断点已留存，已完全释放网络并发槽位让位"


async def _claim_download_terminal(task_id: str, target_status: str) -> bool:
    """尝试写入下载终态前先标记执行器退出；遇暂停冲突时转交调度器暂停收尾。"""
    mark_task_executor_exit(task_id)
    if await TaskQueueRepository.update_status_async(task_id, target_status, from_statuses=("running",)):
        return True
    controls = active_task_controls.get(task_id, {})
    if controls.get("cancelled"):
        return False
    if (
        controls.get("paused")
        or controls.get("resumed_in_place")
        or await asyncio.to_thread(TaskQueueRepository.get_status, task_id) == "paused"
    ):
        mark_task_pause_exit(task_id)
        raise TaskPausedError("任务收尾期间已暂停让位")
    return False


async def _finalize_failed_task(task_id: str, failed_items: list[tuple]):
    """部分条目下载失败: 任务整体置 failed 并汇总失败清单, 绝不伪造完成态"""
    if not await _claim_download_terminal(task_id, "failed"):
        return
    sample = "；".join(f"{ident}: {reason}" for ident, reason in failed_items[:_FAILED_ITEMS_LOG_LIMIT])
    more = (
        f" (仅展示前 {_FAILED_ITEMS_LOG_LIMIT} 条，共 {len(failed_items)} 条失败)"
        if len(failed_items) > _FAILED_ITEMS_LOG_LIMIT
        else ""
    )
    await TaskQueueRepository.append_log_async(
        task_id, f"❌ 任务失败: {len(failed_items)} 个条目未完成下载{more}: {sample}"
    )
    await TaskQueueRepository.append_log_async(
        task_id, "💡 失败条目已保留 .part/.meta 断点文件，可通过【重试】按钮从断点续传，已完成的文件会自动防重跳过。"
    )
    await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "failed"})


@dataclass
class _DownloadRunState:
    """单任务运行态计数器 (links/range 共用; contiguous_cursor_id 仅 range 使用)"""

    total_items: int
    cumulative_done_bytes: int = 0
    known_total_bytes: int = 0
    contiguous_cursor_id: int = 0
    failed_items: list[tuple] = field(default_factory=list)
    downloaded_sources: dict[tuple[str, int], str] = field(default_factory=dict)
    failed_sources: dict[tuple[str, int], tuple[str, str]] = field(default_factory=dict)

    def mark_source_downloaded(self, source_key: tuple[str, int], path: str, *, force: bool) -> None:
        self.failed_sources.pop(source_key, None)
        if force:
            self.downloaded_sources[source_key] = path

    async def has_valid_source_file(self, source_key: tuple[str, int]) -> bool:
        path = self.downloaded_sources.get(source_key)
        if path is None:
            return False
        if await asyncio.to_thread(_verify_final_file_nonempty_sync, path):
            return True
        self.downloaded_sources.pop(source_key, None)
        return False


def _source_message_key(chat_id: str | int, message: Any) -> tuple[str, int]:
    peer_id = getattr(message, "chat_id", None)
    return normalize_download_chat_key(peer_id if isinstance(peer_id, (str, int)) else chat_id), message.id


@dataclass
class _ClientCtx:
    """跨条目共享的可变客户端上下文 (账号故障转移时整体换血)"""

    task_id: str
    threads: int | None
    account_id: int | None
    primary_account_id: int | None = None
    primary_client: Any = None
    clients_pool: list[object] = field(default_factory=list)
    client_account_map: dict[Any, int | None] = field(default_factory=dict)
    bot_clients: set[Any] = field(default_factory=set)

    def __post_init__(self):
        self.attempted_accounts = {self.account_id} if self.account_id else set()


def _register_userbot_entries(mapping: dict[Any, int | None], entries: Any) -> None:
    if not isinstance(entries, (list, tuple, set)):
        return
    for entry in entries:
        c = getattr(entry, "client", None)
        acc_id = getattr(entry, "account_id", None)
        if c is not None and isinstance(acc_id, int) and not isinstance(acc_id, bool):
            mapping[c] = acc_id


def _sync_client_account_map(ctx: _ClientCtx, extra_userbots: list[Any] | None = None) -> None:
    """维护并发下载池内 client -> account_id (及 Bot 成员) 的归属映射."""
    from app.core.client_manager import client_manager

    get_accounts = getattr(client_manager, "get_all_accounts", None)
    if callable(get_accounts):
        _register_userbot_entries(ctx.client_account_map, get_accounts())

    get_bots = getattr(client_manager, "get_all_bots", None)
    bots = get_bots() if callable(get_bots) else None
    if isinstance(bots, (list, tuple, set)):
        for bot in bots:
            c = getattr(bot, "client", None)
            if c is not None:
                ctx.bot_clients.add(c)

    if extra_userbots:
        _register_userbot_entries(ctx.client_account_map, extra_userbots)

    for c in ctx.clients_pool or ():
        if c is not None and c not in ctx.client_account_map and c not in ctx.bot_clients:
            c_acc_id = getattr(c, "account_id", None)
            if isinstance(c_acc_id, int) and not isinstance(c_acc_id, bool):
                ctx.client_account_map[c] = c_acc_id

    if ctx.primary_client is not None:
        ctx.client_account_map[ctx.primary_client] = ctx.primary_account_id


def _refresh_clients_pool(ctx: _ClientCtx) -> None:
    from app.core.client_manager import client_manager

    ctx.clients_pool = client_manager.get_all_active_clients(ctx.account_id) or client_manager.get_all_clients()
    _sync_client_account_map(ctx)


def _lookup_account_id_from_manager(client_manager: Any, client: Any) -> int | None:
    resolve_fn = getattr(client_manager, "resolve_client_account_id", None)
    if callable(resolve_fn):
        resolved_id = resolve_fn(client)
        if isinstance(resolved_id, int) and not isinstance(resolved_id, bool):
            return resolved_id

    get_accounts = getattr(client_manager, "get_all_accounts", None)
    accounts = get_accounts() if callable(get_accounts) else None
    if isinstance(accounts, (list, tuple, set)):
        for acc in accounts:
            if getattr(acc, "client", None) is not None and acc.client == client:
                acc_id = getattr(acc, "account_id", None)
                if isinstance(acc_id, int) and not isinstance(acc_id, bool):
                    return acc_id
    return None


def _resolve_client_owner(ctx: _ClientCtx, client: Any) -> tuple[str, int | None]:
    """解析触发失败的 client 归属: ('bot', None) / ('account', account_id) / ('unknown', None)."""
    from app.core.client_manager import client_manager

    is_bot_fn = getattr(client_manager, "is_bot_client", None)
    if client in ctx.bot_clients or (callable(is_bot_fn) and is_bot_fn(client) is True):
        return ("bot", None)

    if client in ctx.client_account_map:
        return ("account", ctx.client_account_map[client])

    if ctx.primary_client is not None and client == ctx.primary_client:
        return ("account", ctx.primary_account_id)

    resolved_id = _lookup_account_id_from_manager(client_manager, client)
    if resolved_id is not None:
        return ("account", resolved_id)

    c_acc_id = getattr(client, "account_id", None)
    if isinstance(c_acc_id, int) and not isinstance(c_acc_id, bool):
        return ("account", c_acc_id)

    return ("unknown", None)


def _is_chunk_error_session_invalid(sub_exc: Any, sub_err: Any) -> bool:
    if isinstance(sub_exc, BaseException) and is_session_invalid_error(sub_exc):
        return True
    if isinstance(sub_err, BaseException):
        return is_session_invalid_error(sub_err)
    return sub_err is not None and is_session_invalid_error(RuntimeError(str(sub_err)))


def _collect_session_invalid_failing_clients(exc: BaseException) -> list[Any]:
    last_clients = getattr(exc, "last_clients", None)
    if not isinstance(last_clients, dict) or not last_clients:
        return []

    exc_map = getattr(exc, "last_exceptions", None)
    err_map = getattr(exc, "last_errors", None)
    exc_dict = exc_map if isinstance(exc_map, dict) else {}
    err_dict = err_map if isinstance(err_map, dict) else {}
    missing_offsets = getattr(exc, "missing_offsets", None)
    candidate_offsets = [off for off in (missing_offsets or list(last_clients.keys())) if off in last_clients]

    session_invalid_offsets = [
        off for off in candidate_offsets if _is_chunk_error_session_invalid(exc_dict.get(off), err_dict.get(off))
    ]

    # 无逐块会话失效证据时不回退到全部候选: 聚合消息可能被媒体可控文件名污染
    # (安全审查发现)，空列表交由调用方走保守主号回退 (纵深防御)
    failing_clients: list[Any] = []
    for off in session_invalid_offsets:
        c = last_clients.get(off)
        if c is not None and c not in failing_clients:
            failing_clients.append(c)
    return failing_clients


def _evict_failing_client(ctx: _ClientCtx, client: Any, active_clients: list[object] | None) -> None:
    if client in ctx.clients_pool:
        ctx.clients_pool = [c for c in ctx.clients_pool if c != client]
    if active_clients is not None and client in active_clients:
        active_clients[:] = [c for c in active_clients if c != client]
        if not active_clients and ctx.primary_client is not None:
            active_clients.append(ctx.primary_client)


async def _handle_download_session_invalid(
    ctx: _ClientCtx,
    exc: BaseException,
    active_clients: list[object] | None = None,
) -> bool:
    """归因并上报下载会话失效异常 (区分主 Userbot / 辅助 Userbot / Bot 池).

    返回 True 表示命中 Userbot 会话失效并已完成上报 (调用方应中止任务);
    返回 False 表示仅辅助 Bot 会话失效 (已就地标 invalid 并剔除, 不误伤主账号)。
    """
    from app.core.client_manager import client_manager

    failing_clients = _collect_session_invalid_failing_clients(exc)
    if not failing_clients:
        await report_session_invalid(ctx.task_id, exc, account_id=ctx.primary_account_id)
        return True

    failed_account_ids: list[int | None] = []
    bot_invalidated = False
    unknown_client_found = False

    for client in failing_clients:
        _evict_failing_client(ctx, client, active_clients)
        kind, acc_id = _resolve_client_owner(ctx, client)
        if kind == "bot":
            bot_invalidated = True
            invalidate_bot = getattr(client_manager, "invalidate_bot_by_client", None)
            if callable(invalidate_bot):
                invalidate_bot(client, str(exc))
        elif kind == "account":
            if acc_id not in failed_account_ids:
                failed_account_ids.append(acc_id)
        else:
            unknown_client_found = True

    if not failed_account_ids:
        if bot_invalidated and not unknown_client_found:
            return False
        failed_account_ids.append(ctx.primary_account_id)

    for idx, acc_id in enumerate(failed_account_ids):
        if acc_id is not None and acc_id != ctx.primary_account_id:
            task_scheduler.release_auxiliary_account(ctx.task_id, acc_id)
            invalidate_cache = getattr(client_manager, "invalidate_accessible_cache", None)
            if callable(invalidate_cache):
                invalidate_cache(account_id=acc_id)
        await report_session_invalid(ctx.task_id if idx == 0 else None, exc, account_id=acc_id)
    return True


async def _select_primary_client(ctx: _ClientCtx) -> None:
    from app.core.client_manager import client_manager

    # await get_client_for_task 期间若触发重连, 可能切换默认账号; 先锁定任务请求账号的归属快照
    owner = client_manager.get_account(ctx.account_id)
    ctx.primary_client = await client_manager.get_client_for_task(ctx.account_id, mode="userbot")
    if not ctx.primary_client:
        ctx.primary_client = client_manager.userbot
        owner = client_manager.get_account()
    ctx.primary_account_id = owner.account_id if owner else ctx.account_id
    _sync_client_account_map(ctx)


def _make_progress_cb(task_id: str, state: _DownloadRunState, idx: int, total: int, item_label: str):
    """条目级字节协议进度回调 (共享工厂; 惰性取值器保持基线计数闭包格子语义)"""
    return make_executor_progress_cb(
        task_id,
        submit_fn=submit_db_write,
        byte_accounting=True,
        item_label_fn=lambda: item_label,
        item_position_fn=lambda: (idx, total),
        done_base_fn=lambda: state.cumulative_done_bytes,
        known_total_fn=lambda: state.known_total_bytes,
    )


def _make_control_lambdas(task_id: str) -> tuple[Callable[[], bool], Callable[[], bool]]:
    return (lambda: is_task_cancelled(task_id), lambda: is_task_paused(task_id))


async def _check_pause(task_id: str, *, cursor: _DownloadRunState | None = None):
    """暂停信号检查: 置 paused + 落日志 (+ range 持久化连续游标) + 让位异常"""
    if active_task_controls.get(task_id, {}).get("paused"):
        mark_task_pause_exit(task_id)
        await TaskQueueRepository.update_status_async(task_id, "paused", from_statuses=("running", "paused"))
        if cursor is not None:
            await TaskQueueRepository.merge_params_async(task_id, {"contiguous_cursor_id": cursor.contiguous_cursor_id})
            await TaskQueueRepository.append_log_async(
                task_id, f"[调度器] 任务已安全暂停 (已连续完成至 #{cursor.contiguous_cursor_id})，{_PAUSED_LOG[6:]}"
            )
        else:
            await TaskQueueRepository.append_log_async(task_id, _PAUSED_LOG)
        raise TaskPausedError("任务已安全暂停让位")


async def _resolve_boot_config(
    task_id: str, threads: int | None, account_id: int | None, label: str
) -> tuple[int | None, int | None]:
    """启动配置回读: threads/账号未显式给出时从任务行取值 (非致命降级沿用入参)"""
    effective_threads = threads
    current_account_id = account_id
    if effective_threads is None or current_account_id is None:
        try:
            t_rec = await TaskQueueRepository.get_by_id_async(task_id)
            if t_rec:
                if effective_threads is None:
                    effective_threads = (t_rec.get("params") or {}).get("threads") or t_rec.get("concurrency_total")
                if current_account_id is None:
                    current_account_id = t_rec.get("account_id")
        except Exception as exc:
            logger.debug("获取%s初始配置异常，继续沿用传入参数 (非致命): %s", label, exc)
    return effective_threads, current_account_id


async def _promote_failover_account(ctx: _ClientCtx, alt_acc, link: str) -> None:
    """账号故障转移晋升: 落日志 + 调度器重绑 + DB 回写 + WS 广播 (调用方随后换血客户端)"""
    from app.core.client_manager import client_manager

    old_acc = client_manager.get_account(ctx.account_id)
    old_name = old_acc.account_name if old_acc else f"ID:{ctx.account_id}"
    await TaskQueueRepository.append_log_async(
        ctx.task_id, f"⚠️ 账号 [{old_name}] 无权访问私有频道，已自动无缝故障转移至账号 [{alt_acc.account_name}] 接替执行"
    )
    task_scheduler.reassign_task_account(ctx.task_id, ctx.account_id or 0, alt_acc.account_id)
    ctx.account_id = alt_acc.account_id
    ctx.attempted_accounts.add(ctx.account_id)
    await TaskQueueRepository.update_account_id_async(ctx.task_id, ctx.account_id)
    await connection_manager.broadcast({"type": "task_update", "task_id": ctx.task_id, "account_id": ctx.account_id})


async def _start_tier2_fleet(ctx: _ClientCtx, target: str | int) -> list[object]:
    """Tier2 跨账号联合并发舰队: 发现可达辅助 Userbot、借壳计数、执行者名单广播。

    返回 effective_clients (主号 + 辅助号); 单号可达时返回 [primary] (降级 Tier3 由调用方处理)。"""
    from app.core.client_manager import client_manager

    accessible_userbots = await client_manager.get_accessible_userbots(target)
    aux_userbots = [u for u in accessible_userbots if u.client and u.client != ctx.primary_client]
    effective_clients = [ctx.primary_client] + [u.client for u in aux_userbots]
    _sync_client_account_map(ctx, extra_userbots=accessible_userbots)
    for u in aux_userbots:
        task_scheduler.borrow_auxiliary_account(ctx.task_id, u.account_id)
    if len(effective_clients) > 1:
        participating = [u for u in accessible_userbots if u.client in effective_clients]
        acc_names = [u.account_name or f"Userbot #{u.account_id}" for u in participating]
        acc_ids = [u.account_id for u in participating]
        await TaskQueueRepository.merge_params_async(ctx.task_id, {"executor_names": acc_names, "account_ids": acc_ids})
        await connection_manager.broadcast(
            {"type": "task_update", "task_id": ctx.task_id, "executor_names": acc_names, "account_ids": acc_ids}
        )
        names_str = ", ".join(acc_names)
        await TaskQueueRepository.append_log_async(
            ctx.task_id,
            f"🚀 梯队 2 启动: 发现 {len(effective_clients)} 个在群 Userbot 账号 [{names_str}]，启用跨账号联合并发加速下载",
        )
    return effective_clients


async def _announce_tier1(task_id: str, account_id: int | None, bridge) -> tuple[str, list[str]]:
    """Tier1 中转桥启动通告: 执行者名单 (主号+就绪 Bots) 落 params + WS 广播。返回 (主号名, Bot 名列表) 供调用方拼日志。"""
    from app.core.client_manager import client_manager

    acc = client_manager.get_account(account_id)
    acc_name = acc.account_name if acc else "Primary Userbot"
    bot_names = [b.session_name for b in client_manager.get_all_bots() if b.is_ready()]
    exec_names = [acc_name] + bot_names
    await TaskQueueRepository.merge_params_async(task_id, {"executor_names": exec_names, "tier": "tier1_transit"})
    await connection_manager.broadcast(
        {"type": "task_update", "task_id": task_id, "executor_names": exec_names, "tier": "tier1_transit"}
    )
    return acc_name, bot_names


async def _transit_download_album(
    state: _DownloadRunState,
    ctx: _ClientCtx,
    bridge,
    album_msgs: list,
    origin_chat_id,
    idx: int,
    total: int,
    item_key: str,
    force: bool = False,
) -> None:
    """Tier1 中转桥批量下载 (相册成员逐一借壳; 取消中断、暂停让位、失败逐条记账)"""
    cancel_fn, pause_fn = _make_control_lambdas(ctx.task_id)
    for sub_msg in album_msgs:
        if active_task_controls.get(ctx.task_id, {}).get("cancelled"):
            break
        source_key = _source_message_key(origin_chat_id, sub_msg)
        if force and await state.has_valid_source_file(source_key):
            continue
        await _check_pause(ctx.task_id)
        sub_fname, sub_fsize = DownloaderService.get_file_name_and_size(sub_msg)
        sub_fsize = sub_fsize or 0
        if sub_fsize > 0:
            state.known_total_bytes += sub_fsize

        # 身份红线: message 必须取中转群转发消息 (凭证预取按中转群鉴权), origin_msg_id 取
        # 各成员自己的源消息 ID (整对象/共享锚点会污染历史防重与 .part 断点归属)
        fwd_msg = bridge.source_to_forwarded.get(sub_msg.id)
        if fwd_msg is None:
            if not active_task_controls.get(ctx.task_id, {}).get("cancelled"):
                state.failed_sources[source_key] = (
                    item_key,
                    f"消息 {sub_msg.id} 中转桥接失败 (无对应转发消息)",
                )
            continue

        progress_cb = _make_progress_cb(ctx.task_id, state, idx, total, f"{idx}/{total}")
        result_path = await DownloaderService.download_single_message(
            client=bridge.clients,
            chat_id=bridge.transit_chat,
            message=fwd_msg,
            task_id=ctx.task_id,
            progress_callback=progress_cb,
            is_cancelled=cancel_fn,
            is_paused=pause_fn,
            threads=ctx.threads,
            origin_chat_id=origin_chat_id,
            origin_msg_id=int(getattr(sub_msg, "id", 0) or 0),
            origin_grouped_id=getattr(sub_msg, "grouped_id", None),
            force=force,
        )
        await _check_pause(ctx.task_id)
        if result_path is not None:
            state.mark_source_downloaded(source_key, result_path, force=force)
            state.cumulative_done_bytes += max(progress_cb.received_bytes(), sub_fsize)
        elif not active_task_controls.get(ctx.task_id, {}).get("cancelled"):
            state.failed_sources[source_key] = (item_key, f"消息 {sub_msg.id} 下载未完成")


async def _download_album_members(
    state: _DownloadRunState,
    ctx: _ClientCtx,
    album_msgs: list,
    chat_id,
    idx: int,
    total: int,
    link: str,
    clients: list[object],
    with_progress: bool,
    force: bool = False,
) -> None:
    """Tier2/3 相册成员下载 (舰队联合并发; 接替重试形态无进度回调 —— 保留原语义)"""
    cancel_fn, pause_fn = _make_control_lambdas(ctx.task_id)
    for sub_msg in album_msgs:
        if active_task_controls.get(ctx.task_id, {}).get("cancelled"):
            break
        source_key = _source_message_key(chat_id, sub_msg)
        if force and await state.has_valid_source_file(source_key):
            continue
        await _check_pause(ctx.task_id)
        sub_fname, sub_fsize = DownloaderService.get_file_name_and_size(sub_msg)
        sub_fsize = sub_fsize or 0
        # 字节记账与回调解耦: with_progress=False (接替重试) 也必须计入分母/分子,
        # 否则重试全成功时会以 0 字节 100% 完成收尾
        if sub_fsize > 0:
            state.known_total_bytes += sub_fsize
        progress_cb = _make_progress_cb(ctx.task_id, state, idx, total, f"{idx}/{total}") if with_progress else None
        result_path = await DownloaderService.download_single_message(
            client=clients,
            chat_id=chat_id,
            message=sub_msg,
            task_id=ctx.task_id,
            progress_callback=progress_cb,
            is_cancelled=cancel_fn,
            is_paused=pause_fn,
            threads=ctx.threads,
            force=force,
        )
        # 暂停检查不依赖 with_progress: 暂停返回 None 会落入失败记账分支, 被误标 failed
        await _check_pause(ctx.task_id)
        if result_path is not None:
            state.mark_source_downloaded(source_key, result_path, force=force)
            if progress_cb is not None:
                state.cumulative_done_bytes += max(progress_cb.received_bytes(), sub_fsize)
            else:
                state.cumulative_done_bytes += sub_fsize
        elif not active_task_controls.get(ctx.task_id, {}).get("cancelled"):
            state.failed_sources[source_key] = (link, f"消息 {sub_msg.id} 下载未完成")


async def _finalize_download_completed(task_id: str, state: _DownloadRunState, completion_label: str) -> None:
    """完成态收尾: 状态 + 终态进度回写 + 双广播 (100% 全量完成)"""
    if not await _claim_download_terminal(task_id, "completed"):
        return
    final_bytes = state.cumulative_done_bytes
    await TaskQueueRepository.update_progress_async(
        task_id,
        final_bytes,
        final_bytes,
        state.total_items,
        state.total_items,
        0.0,
        0.0,
        f"[{state.total_items}/{state.total_items}] {completion_label}",
    )
    await TaskQueueRepository.append_log_async(task_id, f"🎉 {completion_label}！")
    await connection_manager.broadcast(
        {
            "type": "progress",
            "task_id": task_id,
            "progress": {
                "processed_bytes": final_bytes,
                "total_bytes": final_bytes,
                "processed_items": state.total_items,
                "total_items": state.total_items,
                "percent": 100.0,
                "speed_mb_s": 0.0,
                "eta_s": 0.0,
                "current_item": f"[{state.total_items}/{state.total_items}] {completion_label}",
                "concurrency": 0,
                "concurrency_total": 0,
                "dc_id": 0,
            },
        }
    )
    await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "completed"})


async def _raise_if_cancelled(task_id: str):
    """取消信号检查: 先幂等置终态再让位 (与原实现取消路径的 DB 回写语义一致)"""
    if active_task_controls.get(task_id, {}).get("cancelled"):
        await TaskQueueRepository.update_status_async(task_id, "cancelled")
        raise TaskCancelledError("任务已被用户取消")


async def _claim_and_mark_running(task_id: str) -> bool:
    """调度窗口自检并原子置位 running；已被取消/暂停时跳过执行且不覆盖终态。"""
    if not await asyncio.to_thread(try_claim_executor_entry, task_id):
        await TaskQueueRepository.append_log_async(
            task_id, "[执行器] 任务在调度窗口内已被用户取消/暂停，跳过执行且不覆盖终态"
        )
        return False
    if (
        not await TaskQueueRepository.update_status_async(
            task_id, "running", from_statuses=("queued", "pending", "running")
        )
        and await asyncio.to_thread(TaskQueueRepository.get_status, task_id) is not None
    ):
        await TaskQueueRepository.append_log_async(
            task_id, "[执行器] 任务在调度窗口内已被用户取消/暂停，跳过执行且不覆盖终态"
        )
        release_runner_controls(task_id)
        return False
    return True


async def run_download_links_task(
    task_id: str, links: list[str], threads: int | None = None, account_id: int | None = None, force: bool = False
):
    if not await _claim_and_mark_running(task_id):
        return
    await TaskQueueRepository.append_log_async(task_id, f"开始处理 {len(links)} 条下载链接...")
    state = _DownloadRunState(total_items=len(links))
    effective_threads, current_account_id = await _resolve_boot_config(task_id, threads, account_id, "")
    ctx = _ClientCtx(task_id=task_id, threads=effective_threads, account_id=current_account_id)

    try:
        from app.core.client_manager import client_manager, is_private_channel_identifier

        await _select_primary_client(ctx)
        if not ctx.primary_client:
            await report_session_invalid(
                task_id,
                RuntimeError("Telegram 会话未连接或未登录 (Unauthorized)"),
                account_id=ctx.account_id,
            )
            return
        _refresh_clients_pool(ctx)

        for idx, link in enumerate(links, start=1):
            await _raise_if_cancelled(task_id)
            await _check_pause(task_id)
            effective_clients: list[object] | None = None

            try:
                await TaskQueueRepository.append_log_async(task_id, f"正在解析链接 [{idx}/{state.total_items}]: {link}")
                chat_id, msg = await DownloaderService.resolve_message_from_link(ctx.primary_client, link)

                # 私有频道权限探测与故障转移 (账号未加入频道导致解析为空)
                if not msg and is_private_channel_identifier(link):
                    alt_acc = await client_manager.find_failover_account(
                        ctx.attempted_accounts, link, task_type="download_link"
                    )
                    if alt_acc:
                        await _promote_failover_account(ctx, alt_acc, link)
                        await _select_primary_client(ctx)
                        _refresh_clients_pool(ctx)
                        chat_id, msg = await DownloaderService.resolve_message_from_link(ctx.primary_client, link)

                if not msg or not msg.media:
                    await TaskQueueRepository.append_log_async(task_id, f"⚠️ 消息不存在或未包含媒体: {link}")
                    # 记账失败项: 只含不可解析链接的任务绝不能以 0 文件 100% 完成收尾
                    state.failed_items.append((link, "消息不存在或未包含媒体 (解析失败)"))
                    continue

                source_chat = chat_id or msg.chat_id
                album_msgs = await DownloaderService.get_album_messages(ctx.primary_client, source_chat, msg)
                if force:
                    album_msgs = [
                        member
                        for member in album_msgs
                        if not await state.has_valid_source_file(_source_message_key(source_chat, member))
                    ]
                    if not album_msgs:
                        continue
                await TaskQueueRepository.append_log_async(
                    task_id, f"检测到 {len(album_msgs)} 个关联媒体消息(Album)..."
                )

                is_priv = is_private_channel_identifier(link) or (
                    chat_id is not None and is_private_channel_identifier(chat_id)
                )

                # --- 🚀 梯队 1: 静默中转群桥接与多 Bot 借壳加速 ---
                if is_priv:
                    from app.services.transit_bridge_service import TransitBridgeService

                    can_transit, _ = TransitBridgeService.is_transit_capable(
                        ctx.primary_client, chat_id or msg.chat_id, album_msgs
                    )
                    if can_transit:
                        async with TransitBridgeService.transit_bridge_context(
                            primary_client=ctx.primary_client,
                            source_chat=chat_id or msg.chat_id,
                            messages=album_msgs,
                            task_id=task_id,
                        ) as bridge:
                            if bridge and bridge.forwarded_messages:
                                acc_name, bot_names = await _announce_tier1(task_id, ctx.account_id, bridge)
                                await TaskQueueRepository.append_log_async(
                                    task_id,
                                    f"🚀 梯队 1 启动 (中转加速): 消息已静默桥接至中转群，启用 1 Userbot + {bridge.ready_bots_count} Bots 极速借壳下载 [主号: {acc_name}, 辅助: {', '.join(bot_names)}]",
                                )
                                await _transit_download_album(
                                    state,
                                    ctx,
                                    bridge,
                                    album_msgs,
                                    chat_id or msg.chat_id,
                                    idx,
                                    state.total_items,
                                    link,
                                    force=force,
                                )
                                continue

                # --- 降级逻辑: 梯队 2 (多 Userbot 协同) 或 梯队 3 (单 Userbot 8-TCP) ---
                if is_priv:
                    if getattr(settings, "ENABLE_MULTI_USERBOT_DOWNLOAD", True):
                        effective_clients = await _start_tier2_fleet(ctx, chat_id or link)
                    else:
                        effective_clients = [ctx.primary_client]
                else:
                    effective_clients = ctx.clients_pool or [ctx.primary_client]
                await _download_album_members(
                    state,
                    ctx,
                    album_msgs,
                    chat_id or msg.chat_id,
                    idx,
                    state.total_items,
                    link,
                    effective_clients,
                    with_progress=True,
                    force=force,
                )

            except (TaskPausedError, TaskCancelledError):
                raise
            except Exception as e:
                if isinstance(e, FloodWaitError):
                    raise
                if is_session_invalid_error(e) and await _handle_download_session_invalid(
                    ctx, e, active_clients=effective_clients
                ):
                    return
                if await _handle_link_failure(state, ctx, link, e, force=force):
                    continue

        await _raise_if_cancelled(task_id)

        # 真实性保障: 存在失败条目时任务终态为 failed, 不再广播"全部下载完成"
        unresolved = [*state.failed_items, *state.failed_sources.values()]
        if unresolved:
            await _finalize_failed_task(task_id, unresolved)
            return

        await _finalize_download_completed(task_id, state, "全部下载完成")

    except (TaskPausedError, TaskCancelledError):
        raise
    except Exception as e:
        if isinstance(e, FloodWaitError):
            raise
        if is_session_invalid_error(e) and await _handle_download_session_invalid(ctx, e):
            return
        await TaskQueueRepository.append_log_async(task_id, f"❌ 任务异常中断: {e}")
        raise
    finally:
        release_runner_controls(task_id)


async def _handle_link_failure(
    state: _DownloadRunState, ctx: _ClientCtx, link: str, e: Exception, force: bool = False
) -> bool:
    """单链接失败处置 (FloodWait/会话失效由调用方先行拦截): 权限故障转移接替重试。

    返回 True = 已接替重试完成 (继续下一链接, 不记账); False = 常规记账。
    接替形态无进度回调、失败逐条记账 —— 保留原语义。
    """
    from app.core.client_manager import client_manager, is_channel_permission_error, is_private_channel_identifier

    if is_channel_permission_error(e):
        alt_acc = await client_manager.find_failover_account(ctx.attempted_accounts, link, task_type="download_link")
        if alt_acc:
            await _promote_failover_account(ctx, alt_acc, link)
            await _select_primary_client(ctx)
            _refresh_clients_pool(ctx)
            # 重试本条链接解析
            try:
                chat_id, msg = await DownloaderService.resolve_message_from_link(ctx.primary_client, link)
                if msg and msg.media:
                    album_msgs = await DownloaderService.get_album_messages(
                        ctx.primary_client, chat_id or msg.chat_id, msg
                    )
                    is_priv_retry = is_private_channel_identifier(link) or (
                        chat_id is not None and is_private_channel_identifier(chat_id)
                    )
                    if is_priv_retry:
                        if getattr(settings, "ENABLE_MULTI_USERBOT_DOWNLOAD", True):
                            effective_retry_clients = await _start_tier2_fleet(ctx, chat_id or link)
                        else:
                            effective_retry_clients = [ctx.primary_client]
                    else:
                        effective_retry_clients = ctx.clients_pool or [ctx.primary_client]
                    await _download_album_members(
                        state,
                        ctx,
                        album_msgs,
                        chat_id or msg.chat_id,
                        0,
                        state.total_items,
                        link,
                        effective_retry_clients,
                        with_progress=False,
                        force=force,
                    )
                    return True
                # 接替账号仍未解析到可下载媒体: 记账失败项, 不得静默当作已处理
                await TaskQueueRepository.append_log_async(ctx.task_id, f"❌ 接替账号仍未解析到可下载媒体: {link}")
                state.failed_items.append((link, "故障转移后消息不存在或未包含媒体"))
                return True
            except (TaskPausedError, TaskCancelledError):
                raise
            except FloodWaitError:
                raise
            except Exception as retry_err:
                if is_session_invalid_error(retry_err):
                    raise
                await TaskQueueRepository.append_log_async(ctx.task_id, f"❌ 接替账号执行失败: {retry_err}")
    await TaskQueueRepository.append_log_async(ctx.task_id, f"❌ 处理链接失败 {link}: {e}")
    state.failed_items.append((link, str(e)))
    return False


async def _resolve_range_entity(ctx: _ClientCtx, channel_identifier: str):
    """范围任务 Entity 解析 (私有频道权限异常自动换号故障转移)。

    会话失效统一上报后返回 None (任务终止); 其余解析异常原样上抛。"""
    from app.core.client_manager import client_manager, is_channel_permission_error
    from app.services.forward_service import ForwardService

    while True:
        try:
            parsed_c = ForwardService.parse_chat_identifier(channel_identifier)
            return await ForwardService.resolve_entity(ctx.primary_client, parsed_c)
        except Exception as pe:
            if is_session_invalid_error(pe):
                await report_session_invalid(ctx.task_id, pe, account_id=ctx.primary_account_id)
                return None
            if is_channel_permission_error(pe):
                alt_acc = await client_manager.find_failover_account(
                    ctx.attempted_accounts, channel_identifier, task_type="download_range"
                )
                if alt_acc:
                    await _promote_failover_account(ctx, alt_acc, channel_identifier)
                    await _select_primary_client(ctx)
                    _refresh_clients_pool(ctx)
                    continue
            raise pe


async def _setup_range_fleet(ctx: _ClientCtx, channel_identifier: str, is_priv_range: bool) -> list[object]:
    """范围任务客户端舰队 (任务级一次性): 私有频道 Tier2 联合 / 非私有全活跃池"""
    if is_priv_range:
        if getattr(settings, "ENABLE_MULTI_USERBOT_DOWNLOAD", True):
            return await _start_tier2_fleet(ctx, channel_identifier)
        return [ctx.primary_client]
    return ctx.clients_pool or [ctx.primary_client]


async def _range_cursor_advance(state: _DownloadRunState, task_id: str, m_id: int, idx: int) -> None:
    """连续游标推进: 仅当 m_id 紧接游标时前移, 每 10 条持久化一次 (断点续传口径)"""
    if m_id == state.contiguous_cursor_id + 1:
        state.contiguous_cursor_id = m_id
        if idx % 10 == 0:
            await TaskQueueRepository.merge_params_async(task_id, {"contiguous_cursor_id": state.contiguous_cursor_id})


async def _transit_download_single(
    state: _DownloadRunState,
    ctx: _ClientCtx,
    bridge,
    msg,
    msg_fsize: int,
    channel_identifier: str,
    idx: int,
    total: int,
    force: bool = False,
) -> None:
    """Tier1 中转桥单消息下载 (范围任务): 通告 + 借壳下载 + 暂停让位 + 游标推进 + 失败记账"""
    fwd_msg = bridge.source_to_forwarded.get(msg.id) or bridge.forwarded_messages[0]
    await _announce_tier1(ctx.task_id, ctx.account_id, bridge)
    await TaskQueueRepository.append_log_async(
        ctx.task_id,
        f"🚀 梯队 1 启动 (中转加速): 消息 ID {msg.id} 已桥接至中转群，启用 1 Userbot + {bridge.ready_bots_count} Bots 极速借壳下载",
    )
    progress_cb = _make_progress_cb(ctx.task_id, state, idx, total, f"ID:{msg.id} {idx}/{total}")
    cancel_fn, pause_fn = _make_control_lambdas(ctx.task_id)
    result_path = await DownloaderService.download_single_message(
        client=bridge.clients,
        chat_id=bridge.transit_chat,
        message=fwd_msg,
        task_id=ctx.task_id,
        progress_callback=progress_cb,
        is_cancelled=cancel_fn,
        is_paused=pause_fn,
        threads=ctx.threads,
        origin_chat_id=channel_identifier,
        origin_msg_id=msg.id,
        origin_grouped_id=getattr(msg, "grouped_id", None),
        force=force,
    )
    await _check_pause(ctx.task_id, cursor=state)
    if result_path is not None:
        state.cumulative_done_bytes += max(progress_cb.received_bytes(), msg_fsize)
        await _range_cursor_advance(state, ctx.task_id, msg.id, idx)
    elif not active_task_controls.get(ctx.task_id, {}).get("cancelled"):
        state.failed_items.append((f"ID {msg.id}", "下载未完成"))


async def _try_transit_single(
    state: _DownloadRunState,
    ctx: _ClientCtx,
    msg,
    msg_fsize: int,
    channel_identifier: str,
    idx: int,
    total: int,
    force: bool = False,
) -> bool:
    """Tier1 中转桥尝试: 可桥接则借壳下载并返回 True (调用方 continue 下一消息)"""
    from app.services.transit_bridge_service import TransitBridgeService

    can_transit, _ = TransitBridgeService.is_transit_capable(ctx.primary_client, channel_identifier, [msg])
    if not can_transit:
        return False
    async with TransitBridgeService.transit_bridge_context(
        primary_client=ctx.primary_client, source_chat=channel_identifier, messages=[msg], task_id=ctx.task_id
    ) as bridge:
        if bridge and bridge.forwarded_messages:
            await _transit_download_single(
                state, ctx, bridge, msg, msg_fsize, channel_identifier, idx, total, force=force
            )
            return True
    return False


async def run_download_range_task(
    task_id: str,
    channel_identifier: str,
    start_id: int = 1,
    end_id: int = 1,
    threads: int | None = None,
    account_id: int | None = None,
    force: bool = False,
):
    if not await _claim_and_mark_running(task_id):
        return
    await TaskQueueRepository.append_log_async(
        task_id, f"开始抓取频道 [{channel_identifier}] 消息范围 {start_id} ~ {end_id}..."
    )
    state = _DownloadRunState(total_items=max(0, end_id - start_id + 1), contiguous_cursor_id=start_id - 1)
    effective_threads, current_account_id = await _resolve_boot_config(task_id, threads, account_id, "批量")
    ctx = _ClientCtx(task_id=task_id, threads=effective_threads, account_id=current_account_id)
    effective_range_clients: list[object] = []

    try:
        from app.core.client_manager import is_private_channel_identifier

        await _select_primary_client(ctx)
        if not ctx.primary_client:
            await report_session_invalid(
                task_id,
                RuntimeError("Telegram 会话未连接或未登录 (Unauthorized)"),
                account_id=ctx.account_id,
            )
            return
        _refresh_clients_pool(ctx)

        # 解析 Entity，支持私有频道权限异常自动换号故障转移
        chat_entity = await _resolve_range_entity(ctx, channel_identifier)
        if chat_entity is None:
            return

        is_priv_range = is_private_channel_identifier(channel_identifier)
        effective_range_clients = await _setup_range_fleet(ctx, channel_identifier, is_priv_range)

        # 连续游标恢复 (断点续传口径: 取任务范围起点与已持久化游标的较大者)
        try:
            t_rec = await TaskQueueRepository.get_by_id_async(task_id)
            if t_rec:
                saved_cur = (t_rec.get("params") or {}).get("contiguous_cursor_id")
                if saved_cur is not None:
                    state.contiguous_cursor_id = max(state.contiguous_cursor_id, int(saved_cur))
        except Exception as exc:
            logger.debug("获取连续游标进度异常，沿用初始游标 (非致命): %s", exc)

        for idx, m_id in enumerate(range(start_id, end_id + 1), start=1):
            await _raise_if_cancelled(task_id)
            await _check_pause(task_id, cursor=state)

            try:
                msg = await ctx.primary_client.get_messages(chat_entity, ids=m_id)
                if not msg or not msg.media:
                    if m_id == state.contiguous_cursor_id + 1:
                        state.contiguous_cursor_id = m_id
                    continue

                msg_fname, msg_fsize = DownloaderService.get_file_name_and_size(msg)
                msg_fsize = msg_fsize or 0
                if msg_fsize > 0:
                    state.known_total_bytes += msg_fsize

                await TaskQueueRepository.append_log_async(task_id, f"正在下载 ID: {m_id} [{idx}/{state.total_items}]")

                progress_cb = _make_progress_cb(
                    task_id, state, idx, state.total_items, f"ID:{m_id} {idx}/{state.total_items}"
                )

                # --- 🚀 梯队 1: 静默中转群借壳加速 (若满足条件) ---
                if is_priv_range and await _try_transit_single(
                    state, ctx, msg, msg_fsize, channel_identifier, idx, state.total_items, force=force
                ):
                    continue

                # --- 降级逻辑: 梯队 2 (多 Userbot 协同舰队) 或 梯队 3 (单 Userbot 8-TCP) ---
                result_path = await DownloaderService.download_single_message(
                    client=effective_range_clients,
                    chat_id=channel_identifier,
                    message=msg,
                    task_id=task_id,
                    progress_callback=progress_cb,
                    is_cancelled=lambda: is_task_cancelled(task_id),
                    is_paused=lambda: is_task_paused(task_id),
                    threads=effective_threads,
                    force=force,
                )
                await _check_pause(task_id, cursor=state)

                if result_path is not None:
                    state.cumulative_done_bytes += max(progress_cb.received_bytes(), msg_fsize)
                    await _range_cursor_advance(state, task_id, m_id, idx)
                elif not active_task_controls.get(task_id, {}).get("cancelled"):
                    state.failed_items.append((f"ID {m_id}", "下载未完成"))

            except (TaskPausedError, TaskCancelledError):
                raise
            except Exception as e:
                if isinstance(e, FloodWaitError):
                    raise
                if is_session_invalid_error(e) and await _handle_download_session_invalid(
                    ctx, e, active_clients=effective_range_clients
                ):
                    return
                await TaskQueueRepository.append_log_async(task_id, f"❌ 下载失败 {m_id}: {e}")
                state.failed_items.append((f"ID {m_id}", str(e)))

        await _raise_if_cancelled(task_id)

        # 真实性保障: 存在失败消息时任务终态为 failed, 不再广播"全部范围下载完成"
        if state.failed_items:
            await _finalize_failed_task(task_id, state.failed_items)
            return

        await _finalize_download_completed(task_id, state, "全部范围下载完成")

    except (TaskPausedError, TaskCancelledError):
        raise
    except Exception as e:
        if isinstance(e, FloodWaitError):
            raise
        if is_session_invalid_error(e) and await _handle_download_session_invalid(
            ctx, e, active_clients=effective_range_clients
        ):
            return
        await TaskQueueRepository.append_log_async(task_id, f"❌ 任务异常中断: {e}")
        raise
    finally:
        release_runner_controls(task_id)
