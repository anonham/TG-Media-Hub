import asyncio
import logging
import random
import re
import time
from dataclasses import dataclass, field
from typing import Any, ClassVar

from telethon import TelegramClient
from telethon.errors import (
    ChatForwardsRestrictedError,
    ChatSendMediaForbiddenError,
    ChatWriteForbiddenError,
    FloodWaitError,
    RPCError,
)

from app.core.client_manager import client_manager
from app.core.connection_manager import connection_manager
from app.core.entity_resolver import (
    extract_entity_aliases,
    normalize_peer_alias_key,
    resolve_canonical_chat_key,
)
from app.core.intervals import compute_gaps
from app.core.runtime import mark_task_pause_exit, release_runner_controls, try_claim_executor_entry
from app.core.scheduler import TaskCancelledError, TaskPausedError
from app.core.session_errors import is_session_invalid_error, report_session_invalid
from app.db.forward_repository import ForwardLedgerRepository
from app.db.task_repository import TaskQueueRepository

logger = logging.getLogger(__name__)


class ForwardCancelledError(TaskCancelledError):
    """转发任务被用户取消"""


class ForwardPausedError(TaskPausedError):
    """转发任务被用户暂停 (Aria2 优雅让位)"""


# 频道级写权限/受限错误: 与具体消息无关, 二分隔离只会把整段范围误标 bad_id 永久跳过
# (ChatForwardsRestrictedError 在 Telethon 的 str(e) 为 "You can't forward messages from a
# protected chat", 字符串子串匹配不可靠, 必须按异常类型拦截)。命中即上抛: 任务失败可恢复,
# 未发送 ID 保持未覆盖, 由 resume/批前防重兜底。
_CHANNEL_WIDE_WRITE_ERRORS: tuple[type[Exception], ...] = (
    ChatWriteForbiddenError,
    ChatForwardsRestrictedError,
    ChatSendMediaForbiddenError,
)


class MessagePureFilter:
    """本地消息纯净度清洗器 (0ms, 0 RPC 开销)"""

    @staticmethod
    def is_forwardable(msg: Any) -> tuple[bool, str]:
        """
        本地判定消息是否可转发
        返回: (是否放行, 原因标识)
        """
        # 1. 基础空对象/空消息判断
        if not msg:
            return False, "null_message"
        if getattr(msg, "empty", False):
            return False, "empty_message"

        # 2. 系统动作与服务消息过滤 (加群、退群、置顶、频道迁移等不可转发消息)
        if getattr(msg, "action", None) is not None:
            return False, "service_action"

        has_text = bool(getattr(msg, "message", None) or getattr(msg, "raw_text", None))
        has_media = getattr(msg, "media", None) is not None

        # 3. 幽灵消息 (既无文本也无媒体)
        if not has_text and not has_media:
            return False, "ghost_no_content"

        # 4. 精准区分 TOS 死亡消息 与 平台局部受限资源
        restriction_reasons = getattr(msg, "restriction_reason", None)
        if restriction_reasons:
            for r in restriction_reasons:
                platform = str(getattr(r, "platform", "")).lower()
                reason = str(getattr(r, "reason", "")).lower()
                # 全平台 terms/copyright 封禁且无 media 的占位消息 -> 判定为真死消息
                if (platform in ["all", ""] or not platform) and reason in ["terms", "copyright"] and not has_media:
                    return False, f"tos_dead_{reason}"

        # 5. 局部平台受限 (如 platform='ios', reason='porn') 但有 media 或 text -> 予以放行
        return True, "valid"


class AdaptivePacer:
    """自适应节流控制器：根据 RPC 状态动态调节发包间隔，集成拟人化随机抖动"""

    def __init__(
        self,
        base_delay: float = 1.5,
        min_delay: float = 0.5,
        max_delay: float = 8.0,
        jitter_range: tuple[float, float] = (0.85, 1.25),
    ):
        self.min_delay = min(min_delay, float(base_delay))
        self.max_delay = max(max_delay, float(base_delay))
        self.base_delay = float(base_delay)
        self.current_delay = self.base_delay
        self.jitter_range = jitter_range
        self.consecutive_success = 0

    def record_success(self):
        self.consecutive_success += 1
        # 连续 5 次批次平稳转发，平滑缩减延迟，趋向 base_delay
        if self.consecutive_success >= 5 and self.current_delay > self.base_delay:
            self.current_delay = max(self.base_delay, round(self.current_delay - 0.2, 2))
            self.consecutive_success = 0

    def record_retry(self):
        self.consecutive_success = 0
        # 发生二分重试或瞬态抖动，适度增加发包间隔
        self.current_delay = min(self.max_delay, round(self.current_delay + 0.8, 2))

    def record_flood_wait(self):
        self.consecutive_success = 0
        # 触发 FloodWait 后，加重防风控延迟
        self.current_delay = min(self.max_delay, round(self.current_delay + 2.0, 2))

    async def pace(self) -> float:
        """
        引入拟人化伪随机抖动 (Jitter)，返回本次实际休眠时长 (秒)
        """
        jitter = random.uniform(self.jitter_range[0], self.jitter_range[1])
        actual_delay = max(self.min_delay, min(self.max_delay, self.current_delay * jitter))
        await asyncio.sleep(actual_delay)
        return actual_delay


class ForwardService:
    # 进程内 (源,目标) 与单目标频道并发防护: 同一目标频道任意时刻只允许一个转发协程写入
    _pair_locks: ClassVar[dict[tuple[str, str], asyncio.Lock]] = {}
    _target_locks: ClassVar[dict[str, asyncio.Lock]] = {}

    @staticmethod
    def parse_chat_identifier(chat_str: str | int) -> str | int:
        """解析群组/频道标识符 (向后兼容薄包装，委托至 app.core.entity_resolver)"""
        from app.core.entity_resolver import parse_chat_identifier

        return parse_chat_identifier(chat_str)

    @classmethod
    async def resolve_entity(cls, client: TelegramClient, chat_identifier: str | int, fallback_peer: Any = None) -> Any:
        """安全解析 Telegram Entity (向后兼容薄包装，委托至 app.core.entity_resolver)"""
        from app.core.entity_resolver import resolve_entity

        return await resolve_entity(client, chat_identifier, fallback_peer=fallback_peer)

    @classmethod
    async def inspect_channel_range(cls, source_chat: str, account_id: int | None = None) -> dict:
        """测算与确认源频道/群组的历史消息总数及起始与结束消息 ID"""
        parsed_source = cls.parse_chat_identifier(source_chat)
        acc, source_entity = await client_manager.probe_chat_access(parsed_source, account_id=account_id)
        if acc and acc.client and acc.client.is_connected():
            userbot = acc.client
        else:
            userbot = client_manager.userbot
            if not userbot or not userbot.is_connected():
                await client_manager.initialize()
                userbot = client_manager.userbot
            if source_entity is None and userbot:
                source_entity = await cls.resolve_entity(userbot, parsed_source)

        if not source_entity:
            source_entity = await cls.resolve_entity(userbot, parsed_source)

        canonical_source = resolve_canonical_chat_key(source_entity, source_chat)
        await asyncio.to_thread(_record_resolved_peer_aliases, source_entity, canonical_source, source_chat)

        # 提取最早一条消息的 ID
        oldest_msgs = await userbot.get_messages(source_entity, limit=1, reverse=True)
        min_id = oldest_msgs[0].id if oldest_msgs else 1

        # 提取最新一条消息的 ID
        newest_msgs = await userbot.get_messages(source_entity, limit=1)
        max_id = newest_msgs[0].id if newest_msgs else 1

        # 获得频道总消息数量
        res = await userbot.get_messages(source_entity, limit=0)
        total_count = getattr(res, "total", 0)

        return {
            "source_chat": source_chat,
            "parsed_source": canonical_source,
            "total_count": total_count,
            "min_id": min_id,
            "max_id": max_id,
        }

    @classmethod
    def canonicalize_chat_key(cls, chat_identifier: str | int) -> str:
        """解析群组/频道标识并优先映射为已缓存的规范 peer_id 串 (C-2)."""
        parsed = cls.parse_chat_identifier(chat_identifier)
        if isinstance(parsed, int):
            return str(parsed)
        parsed_str = str(parsed)
        if re.fullmatch(r"-?\d+", parsed_str.strip()):
            return parsed_str.strip()
        alias_key = normalize_peer_alias_key(parsed_str)
        if alias_key:
            cached = ForwardLedgerRepository.get_peer_id_by_alias(alias_key)
            if cached:
                return cached
        return parsed_str

    @classmethod
    def compute_resume_start(
        cls,
        source_chat: str,
        target_chat: str,
        start_id: int | None = None,
        end_id: int | None = None,
        full_retransfer: bool = False,
        allow_alias_cache: bool = True,
    ) -> tuple[int | None, str]:
        """
        resume 快路径: 计算 resume 时的最优起始消息 ID
        返回 (resume_start_id, mode):
        - 'fallback': 无覆盖区间记录(含纯存量库/未钉住的可变别名) → 用原始起点，后续逐批防重兜底
        - 'gap':      从首个未覆盖 gap 的左端点续转
        - 'covered':  指定范围已全覆盖 → resume_start 为 None，调用方直接完结任务
        """
        if full_retransfer:
            # 强制全量重转: 不采信既有账本覆盖, 一律从原始起点重扫 (批前防重由 full_retransfer 跳过)
            return start_id, "fallback"
        if allow_alias_cache:
            parsed_source = cls.canonicalize_chat_key(source_chat)
            parsed_target = cls.canonicalize_chat_key(target_chat)
        else:
            # 恢复快路径对从未完成实时解析的可变别名 (@username/+invite) 不采信离线缓存，
            # 防止 @username 易主后误用旧 peer 的 coverage 直接将新任务判为 completed
            raw_source = cls.parse_chat_identifier(source_chat)
            raw_target = cls.parse_chat_identifier(target_chat)
            if normalize_peer_alias_key(raw_source) or normalize_peer_alias_key(raw_target):
                return start_id, "fallback"
            parsed_source = str(raw_source)
            parsed_target = str(raw_target)
        segs = ForwardLedgerRepository.get_coverage_segments(parsed_source, parsed_target)
        if not segs:
            return start_id, "fallback"
        # start 缺省用请求范围下界 (ID 1): 若以 coverage 下界近似, 早于首个覆盖段的消息会被误判 covered;
        # end 缺省用极大哨兵 (整频道任务: 最后一段之后的消息都视为未覆盖)
        w_start = start_id if start_id else 1
        w_end = end_id if end_id else (1 << 62)
        gaps = compute_gaps(segs, w_start, w_end)
        if not gaps:
            return None, "covered"
        return gaps[0][0], "gap"

    @classmethod
    async def forward_channel_messages(
        cls,
        source_chat: str,
        target_chat: str,
        start_id: int | None = None,
        end_id: int | None = None,
        batch_size: int = 50,
        delay_seconds: float = 1.5,
        task_id: str | None = None,
        full_retransfer: bool = False,
        dry_run: bool = False,
        account_id: int | None = None,
    ) -> int:
        """
        云端极速批量转发入口：先解析源/目标实体并规范化为真实 peer_id (C-1/C-2)，
        再基于规范目标 peer_id 加排他并发锁，避免同一频道因 @username / -100... 不同写法绕过互斥与账本。
        """
        current_account_id = account_id
        attempted_accounts = {current_account_id} if current_account_id else set()

        from app.core.client_manager import client_manager

        userbot, client_account_id = await _select_forward_userbot(client_manager, current_account_id)
        if not userbot or not userbot.is_connected():
            await client_manager.initialize()
            userbot, client_account_id = await _select_forward_userbot(client_manager, current_account_id)

        state = _ForwardRunState(
            task_id=task_id,
            parsed_source=cls.parse_chat_identifier(source_chat),
            parsed_target=cls.parse_chat_identifier(target_chat),
            batch_size=batch_size,
            full_retransfer=full_retransfer,
            dry_run=dry_run,
            client_account_id=client_account_id,
        )

        # 实体解析与同会话拒绝必须先于加锁: 不同拼写共享同一目标锁, 解析失败/同会话绝不留锁
        userbot, source_entity, target_entity, current_account_id = await _resolve_forward_pair(
            state, userbot, source_chat, target_chat, current_account_id, attempted_accounts
        )
        source_key = resolve_canonical_chat_key(source_entity, source_chat)
        target_key = resolve_canonical_chat_key(target_entity, target_chat)
        await asyncio.to_thread(_record_resolved_peer_aliases, source_entity, source_key, source_chat)
        await asyncio.to_thread(_record_resolved_peer_aliases, target_entity, target_key, target_chat)
        if task_id:
            # 将经 Telegram 实时解析确认的权威 peer_id 钉入任务参数, 供后续 resume 快路径可信使用
            await asyncio.to_thread(
                TaskQueueRepository.merge_params,
                task_id,
                {"canonical_source": source_key, "canonical_target": target_key},
            )
        state.parsed_source = source_key
        state.parsed_target = target_key

        pair_key = (source_key, target_key)
        existing_pair = cls._pair_locks.get(pair_key)
        existing_target = cls._target_locks.get(target_key)
        if (existing_pair and existing_pair.locked()) or (existing_target and existing_target.locked()):
            raise RuntimeError(
                f"目标频道 ({target_key}) 已有运行中的转发任务 ({source_key} → {target_key})，"
                "同一目标频道禁止多任务并发写入，请等待其完成或取消后再试"
            )
        pair_lock = existing_pair or cls._pair_locks.setdefault(pair_key, asyncio.Lock())
        target_lock = existing_target or cls._target_locks.setdefault(target_key, asyncio.Lock())
        await pair_lock.acquire()
        await target_lock.acquire()
        try:
            return await cls._forward_channel_messages_inner(
                source_chat=source_chat,
                target_chat=target_chat,
                start_id=start_id,
                end_id=end_id,
                batch_size=batch_size,
                delay_seconds=delay_seconds,
                task_id=task_id,
                full_retransfer=full_retransfer,
                dry_run=dry_run,
                account_id=account_id,
                _pre_resolved=(state, userbot, source_entity, target_entity, current_account_id),
            )
        finally:
            if target_lock.locked():
                target_lock.release()
            if not target_lock.locked() and cls._target_locks.get(target_key) is target_lock:
                cls._target_locks.pop(target_key, None)
            if pair_lock.locked():
                pair_lock.release()
            # 有界化: 任务结束摘除 pair/target 锁条目, 防止随历史频道对无限增长;
            # release 与摘除之间无 await, 同事件循环内不存在并发窗口 (争用者只能走上方 locked() 拒绝路径)
            if not pair_lock.locked() and cls._pair_locks.get(pair_key) is pair_lock:
                cls._pair_locks.pop(pair_key, None)

    @classmethod
    async def _forward_channel_messages_inner(
        cls,
        source_chat: str,
        target_chat: str,
        start_id: int | None = None,
        end_id: int | None = None,
        batch_size: int = 50,
        delay_seconds: float = 1.5,
        task_id: str | None = None,
        full_retransfer: bool = False,
        dry_run: bool = False,
        account_id: int | None = None,
        _pre_resolved: tuple[Any, Any, Any, Any, int | None] | None = None,
    ) -> int:
        """
        云端极速批量转发 (编排): 信号检查 / 转发核心 (二分隔离) / 账本落库 / 相册缓冲四段协作。
        全过程采用【前置纯净度过滤 + 二分隔离批次转发 + FloodWait 顶级熔断 + 自适应节流】架构
        增量模式(默认)按批防重剔除已处理消息；full_retransfer=True 为全量重转(跳过剔除，仍写账本)
        dry_run=True 试转模式: 首个批次完成后自动置为 paused (复用现有暂停信号，不引入新状态)
        """
        if _pre_resolved is not None:
            state, userbot, source_entity, target_entity, current_account_id = _pre_resolved
        else:
            current_account_id = account_id
            attempted_accounts = {current_account_id} if current_account_id else set()

            from app.core.client_manager import client_manager

            userbot, client_account_id = await _select_forward_userbot(client_manager, current_account_id)
            if not userbot or not userbot.is_connected():
                await client_manager.initialize()
                userbot, client_account_id = await _select_forward_userbot(client_manager, current_account_id)

            state = _ForwardRunState(
                task_id=task_id,
                parsed_source=cls.parse_chat_identifier(source_chat),
                parsed_target=cls.parse_chat_identifier(target_chat),
                batch_size=batch_size,
                full_retransfer=full_retransfer,
                dry_run=dry_run,
                client_account_id=client_account_id,
            )

            # 1. 检查源实体与目标实体连通性 (支持权限异常自动故障转移)
            userbot, source_entity, target_entity, current_account_id = await _resolve_forward_pair(
                state, userbot, source_chat, target_chat, current_account_id, attempted_accounts
            )
            state.parsed_source = resolve_canonical_chat_key(source_entity, source_chat)
            state.parsed_target = resolve_canonical_chat_key(target_entity, target_chat)
            await asyncio.to_thread(_record_resolved_peer_aliases, source_entity, str(state.parsed_source), source_chat)
            await asyncio.to_thread(_record_resolved_peer_aliases, target_entity, str(state.parsed_target), target_chat)

        client_account_id = state.client_account_id if state.client_account_id is not None else current_account_id
        await _log_forward_startup(state, source_chat, target_chat, batch_size, delay_seconds)

        # 2. 计算本次任务的进度分母 run_total (任务范围消息数口径) 并立刻更新任务状态为 running
        state.run_total = await _compute_forward_run_total(userbot, source_entity, start_id, end_id)
        await _broadcast_forward_start(state, start_id, end_id)

        ctx = _ForwardRunCtx(
            userbot=userbot,
            source_entity=source_entity,
            target_entity=target_entity,
            pacer=AdaptivePacer(base_delay=delay_seconds),
        )

        # 构造 iter_messages 范围 (无 end_id 时以启动时最新消息为上界: reverse 流式扫描不再把
        # 运行期新增消息卷入本次任务, 同源副本也无法形成自我复制循环)
        snapshot_end_id = await _snapshot_scan_upper_bound(userbot, source_entity, end_id)
        kwargs = _build_iter_range_kwargs(start_id, snapshot_end_id)

        await _log_task_event(state, "⚡ 已开启【流式纯净检索 + 二分智能避障】模式，零硬盘占用，极速启动...")

        # 3. 流式迭代：前置纯净度过滤 + Album-Aware 缓冲分批提交二分转发
        try:
            async for msg in userbot.iter_messages(source_entity, **kwargs):
                await _process_scanned_message(state, ctx, msg)

            # 迭代结束，结算残留相册缓冲
            if state.active_grouped_id is not None or state.current_album_buffer:
                await _flush_album_buffer(state, ctx)

            # 转发最后一批剩余消息 (含纯过滤尾部: batch 为空但仍有待落账的 skip)
            if state.current_batch_ids or state.pending_skips:
                await _commit_batch(state, ctx, state.current_batch_ids)
                state.current_batch_ids = []

        except (ForwardCancelledError, ForwardPausedError):
            raise
        except Exception as e:
            # 账号会话失效 (转发中被登出): 统一置 failed + 全局事件引导重绑, 避免被误判为坏批次二分
            if is_session_invalid_error(e):
                await report_session_invalid(task_id, e, account_id=client_account_id)
                raise e
            if isinstance(e, FloodWaitError):
                raise e
            err_text = f"❌ 检索或转发频道消息异常: {e}"
            await _log_task_event(state, err_text, "ERROR")
            raise e

        # 4. 总结完成 (回执小结构: 成功 X / 策略过滤 Y / 坏消息隔离 Z / 防重跳过 W)
        # 收尾前最后信号检查: 末批节流休眠期间到达的暂停/取消不能被记成 completed
        await _check_task_signals(state)
        await _summarize_forward_completion(state)
        return state.forwarded_count


@dataclass
class _ForwardRunState:
    """单次转发的可变运行态 (信号检查/转发核心/账本落库/相册缓冲四协作段共享;
    原 560 行巨型闭包的显式化)。"""

    task_id: str | None
    parsed_source: Any
    parsed_target: Any
    batch_size: int
    full_retransfer: bool
    dry_run: bool
    client_account_id: int | None = None
    run_total: int = 0
    forwarded_count: int = 0
    skipped_dead_count: int = 0
    dedup_skipped: int = 0
    bad_isolated_count: int = 0
    dry_pause_done: bool = False
    current_batch_ids: list[int] = field(default_factory=list)
    current_album_buffer: list[int] = field(default_factory=list)
    active_grouped_id: int | None = None
    pending_mappings: list[tuple[int, int]] = field(default_factory=list)
    pending_skips: list[tuple] = field(default_factory=list)
    bad_id_list: list[int] = field(default_factory=list)
    filtered_reason_counts: dict[str, int] = field(default_factory=dict)


@dataclass
class _ForwardRunCtx:
    """转发运行上下文 (客户端/实体/节拍器)"""

    userbot: Any
    source_entity: Any
    target_entity: Any
    pacer: Any


def _record_resolved_peer_aliases(entity: Any, canonical_key: str, *raw_inputs: Any) -> None:
    """将非数字频道别名与实体 username 绑定到规范 peer_id，供离线/同步查询对齐主键."""
    if not re.fullmatch(r"-?\d+", str(canonical_key or "").strip()):
        return
    aliases = extract_entity_aliases(entity, *raw_inputs)
    if aliases:
        ForwardLedgerRepository.upsert_peer_aliases(aliases, str(canonical_key))


async def _log_task_event(state: _ForwardRunState, content: str, level: str = "INFO") -> None:
    """任务级明细统一落 task_logs 并经 append_log_async 广播 WS, 系统日志仅留 debug 追踪 (避免刷爆环形缓冲)"""
    logger.debug("[%s] [Task %s] %s", level, state.task_id, content)
    if state.task_id:
        await TaskQueueRepository.append_log_async(state.task_id, f"[{level}] {content}")


async def _check_task_signals(state: _ForwardRunState) -> None:
    """检查任务控制信号 (暂停与取消 - Aria2 立即让位模型)"""
    from app.core.runtime import active_task_controls

    if state.task_id:
        if active_task_controls.get(state.task_id, {}).get("cancelled", False):
            await _log_task_event(state, "🛑 任务已被用户手动取消！", "WARNING")
            await TaskQueueRepository.update_status_async(state.task_id, "cancelled")
            raise ForwardCancelledError("Task cancelled by user")

        if active_task_controls.get(state.task_id, {}).get("paused", False):
            mark_task_pause_exit(state.task_id)
            await _log_task_event(state, "⏸️ 任务已安全暂停：进度已记录至账本，已释放连接并让位", "INFO")
            await TaskQueueRepository.update_status_async(state.task_id, "paused", from_statuses=("running", "paused"))
            raise ForwardPausedError("Task paused by user")


def _record_mapping(state: _ForwardRunState, src_ids: list, res_objs: Any) -> list[tuple[int, int]]:
    """捕获 Telegram 目标群组生成的新消息 ID，缓存入待落库映射 (与 skip/覆盖区间同批事务写入)。

    返回本次成功建立映射的 [(source_id, target_id)]; Telethon 对部分无效 ID 在原位返回 None,
    计数与回执只认实际返回的结果。"""
    if not res_objs:
        return []
    res_list = res_objs if isinstance(res_objs, list) else [res_objs]
    new_mappings = [
        (s_id, res_list[idx].id)
        for idx, s_id in enumerate(src_ids)
        if idx < len(res_list) and res_list[idx] and hasattr(res_list[idx], "id")
    ]
    state.pending_mappings.extend(new_mappings)
    return new_mappings


async def _flush_partial_ledger(
    state: _ForwardRunState, settled_bad_ids: list[int], unresolved_ids: list[int] | None = None
) -> None:
    """二分左子批结算先落库: 右子批是可中断区 (FloodWait 休眠可暂停 / 会话失效可上抛),
    左子批已产生的映射与隔离记录必须先持久化, 中断恢复时不再重发已成功消息。
    仅提交已结算 ID 的覆盖区间, 未发送余量保持未覆盖 (resume 时批前防重逐条兜底)。"""
    if not state.pending_mappings and not state.pending_skips and not settled_bad_ids:
        return
    bad_skips = [
        (bad_id, "bad_id", "二分隔离出的不可转发坏消息", str(state.parsed_target)) for bad_id in settled_bad_ids
    ]
    seg_ids = [m[0] for m in state.pending_mappings] + [s[0] for s in state.pending_skips] + list(settled_bad_ids)
    if not seg_ids:
        return
    settled = set(seg_ids)
    unsettled = [
        i for i in state.current_batch_ids + state.current_album_buffer + (unresolved_ids or []) if i not in settled
    ]
    seg_end = min(max(seg_ids), min(unsettled) - 1) if unsettled else max(seg_ids)
    await asyncio.to_thread(
        ForwardLedgerRepository.commit_batch_records,
        task_id=state.task_id,
        source_chat=str(state.parsed_source),
        target_chat=str(state.parsed_target),
        mappings=list(state.pending_mappings),
        skips=list(state.pending_skips) + bad_skips,
        seg_start=min(seg_ids),
        seg_end=seg_end,
    )
    state.pending_mappings.clear()
    state.pending_skips.clear()


async def _snapshot_scan_upper_bound(userbot: Any, source_entity: Any, end_id: int | None) -> int | None:
    """无 end_id 时以启动时最新消息 ID 为扫描上界 (1 次廉价 RPC)。空频道/快照失败时保持无界行为。"""
    if end_id is not None:
        return end_id
    try:
        newest_msgs = await userbot.get_messages(source_entity, limit=1)
        newest_id = getattr(newest_msgs[0], "id", None) if newest_msgs else None
        return int(newest_id) if newest_id else None
    except (AttributeError, IndexError, TypeError, ValueError) as snap_err:
        logger.debug("扫描上界快照失败, 保持无界扫描 (非致命): %s", snap_err)
        return None


def _reject_same_peer_pair(source_entity: Any, target_entity: Any) -> None:
    """拒绝源=目标同会话转发: 反向扫描会把本次转发副本再度卷入, 造成自我复制循环。
    必须比较解析后的 peer 标识 (别名/用户名与数字 ID 指向同一会话时, 字符串比较无法拦截)。"""
    try:
        from telethon.utils import get_peer_id

        same_peer = get_peer_id(source_entity) == get_peer_id(target_entity)
    except Exception as cmp_err:
        logger.debug("同会话比较跳过 (实体形态不支持): %s", cmp_err)
        return
    if same_peer:
        raise RuntimeError("源频道与目标频道解析为同一会话，禁止同源转发 (会触发自我复制循环)")


async def _flood_wait_cooldown(state: _ForwardRunState, wait_time: float) -> None:
    """FloodWait 可中断熔断等待: 1s 粒度倒计时, 期间可响应暂停/取消,
    并向前端广播剩余秒数 (FloodWait 可视化)。"""
    flood_deadline = time.monotonic() + wait_time
    while True:
        await _check_task_signals(state)
        remaining = max(0.0, flood_deadline - time.monotonic())
        if remaining <= 0:
            break
        if state.task_id:
            processed_total = state.forwarded_count + state.skipped_dead_count + state.dedup_skipped
            flood_percent = (
                min(100.0, round((processed_total / state.run_total * 100), 1)) if state.run_total > 0 else 0.0
            )
            flood_item = f"⏳ FloodWait 熔断保护中，剩余 {int(remaining)}s 后自动重试..."
            await TaskQueueRepository.update_progress_async(
                state.task_id, 0, 0, processed_total, state.run_total, 0.0, 0.0, flood_item
            )
            await connection_manager.broadcast(
                {
                    "type": "progress",
                    "task_id": state.task_id,
                    "flood_wait_seconds": int(remaining),
                    "progress": {
                        "processed_bytes": 0,
                        "total_bytes": 0,
                        "processed_items": processed_total,
                        "total_items": state.run_total,
                        "percent": flood_percent,
                        "speed_mb_s": 0.0,
                        "eta_s": 0.0,
                        "current_item": flood_item,
                        "flood_wait_seconds": int(remaining),
                        "concurrency": 0,
                        "concurrency_total": 0,
                        "dc_id": 0,
                    },
                }
            )
        await asyncio.sleep(min(1.0, remaining))


async def _handle_forward_send_failure(state: "_ForwardRunState", e: Exception) -> str:
    """转发失败分类处置 (RPC 与通用异常共用):
    - 会话失效 / 频道级写权限受限 / 未知非 RPC 异常: 直接上抛 (严禁进入坏消息隔离)
    - 传输层瞬态故障 (ConnectionError/TimeoutError/OSError) 及节点拥塞: 返回 'retry'
    - 确认的 Telegram 消息/批次级 RPCError: 返回 'isolate' 交回二分隔离
    """
    # 账号会话失效直接向上传播 (由外层统一置 failed + 引导重绑), 严禁进入坏批次二分
    if is_session_invalid_error(e):
        raise e
    err_msg = str(e)
    # 频道级权限/受限错误按类型拦截: 与具体消息无关, 隔离会把整段范围永久误标 bad_id
    if isinstance(e, _CHANNEL_WIDE_WRITE_ERRORS):
        await _log_task_event(
            state, f"❌ 频道级权限/受限错误 ({type(e).__name__})，与具体消息无关，终止隔离并上抛: {e}", "ERROR"
        )
        raise e
    # 权限/群组禁止转发硬错误 (字符串匹配保留为类型拦截之外的兜底)
    if "ChatForwardsRestrictedError" in err_msg or "chat forwards restricted" in err_msg.lower():
        await _log_task_event(state, f"❌ 目标或源群组受限禁止直接转发 (ChatForwardsRestricted): {err_msg}", "ERROR")
        raise e
    # 明确未发出的断连错误 (请求尚未写入传输层): 安全重试 (最多 max_retries 次);
    # 发送结果未知的 TimeoutError / OSError / 断连中 reset 等严禁循环重发 (避免产生重复消息),
    # 直接上抛让任务进入 failed 并保留未覆盖区间, 不落 bad_id 隔离。
    if isinstance(e, ConnectionError) and "cannot send requests while disconnected" in err_msg.lower():
        return "retry"
    if isinstance(e, (ConnectionError, TimeoutError, OSError)):
        await _log_task_event(
            state,
            f"❌ 转发遭遇发送结果未知的传输异常 ({type(e).__name__}: {err_msg or 'timeout'})，禁止自动重发，终止本批待核查恢复",
            "ERROR",
        )
        raise e
    # 瞬态拥塞错误: 指数退避后可重试
    if isinstance(e, ValueError) and re.fullmatch(r"Request was unsuccessful \d+ time\(s\)", err_msg):
        return "retry"
    if any(term in err_msg.lower() for term in ["workerbusy", "too busy"]):
        return "retry"
    if isinstance(e, RPCError) and "timeout" in err_msg.lower():
        return "retry"
    if isinstance(e, RPCError):
        return "isolate"
    await _log_task_event(
        state, f"❌ 转发遭遇非 RPC 异常 ({type(e).__name__}: {err_msg})，终止隔离并上抛待恢复", "ERROR"
    )
    raise e


async def _retry_transient_forward(
    state: "_ForwardRunState", ctx: "_ForwardRunCtx", retry_count: int, err_msg: str
) -> None:
    """瞬态拥塞退避 (指数) + 熔断休眠期间保持信号响应"""
    backoff = 2.0 * retry_count + 1.0
    await _log_task_event(state, f"⏳ Telegram 节点繁忙 ({err_msg})，退避休眠 {backoff}s 后重试...", "WARNING")
    ctx.pacer.record_retry()
    await asyncio.sleep(backoff)
    await _check_task_signals(state)


async def _isolate_or_bisect(
    state: "_ForwardRunState", ctx: "_ForwardRunCtx", sub_ids: list[int], depth: int, err_msg: str
) -> tuple[int, list[int]]:
    """坏消息隔离段: 单条精准隔离; 多条二分左/右分治 (左子批结算先落库再进入可中断的右子批)"""
    if len(sub_ids) == 1:
        bad_id = sub_ids[0]
        await _log_task_event(
            state, f"🙈 [单条死消息隔离] 消息 ID {bad_id} 无法转发 ({err_msg})，已安全剔除跳过", "WARNING"
        )
        return 0, [bad_id]

    mid = len(sub_ids) // 2
    left_ids = sub_ids[:mid]
    right_ids = sub_ids[mid:]
    ctx.pacer.record_retry()
    await _log_task_event(
        state,
        f"⚠️ 子批次 (大小 {len(sub_ids)}, ID: {sub_ids[0]}~{sub_ids[-1]}) 转发受阻 ({err_msg})，启动二分隔离: 左 [{left_ids[0]}~{left_ids[-1]}] | 右 [{right_ids[0]}~{right_ids[-1]}]",
        "DEBUG",
    )
    l_succ, l_bads = await _forward_sub_batch(state, ctx, left_ids, depth + 1)
    await _flush_partial_ledger(state, l_bads, right_ids)
    r_succ, r_bads = await _forward_sub_batch(state, ctx, right_ids, depth + 1)
    return (l_succ + r_succ), (l_bads + r_bads)


async def _forward_sub_batch(
    state: _ForwardRunState, ctx: _ForwardRunCtx, sub_ids: list[int], depth: int = 0
) -> tuple[int, list[int]]:
    """转发核心 —— 二分隔离批次转发算法：
    - 整批成功: 1 次 RPC 批量入库，返回 (成功数量, [])
    - 遇 FloodWait: 顶级熔断休眠 (e.seconds + 2)，原批次直接重试，绝不降级/丢弃
    - 遇 WorkerBusy: 瞬态指数退避重试
    - 遇不可恢复 RPC 异常: N > 1 二分为 Left/Right 分治求解; N = 1 精准隔离坏消息
    """
    if not sub_ids:
        return 0, []

    await _check_task_signals(state)

    max_retries = 3
    retry_count = 0
    res_objs: Any = None
    last_transient_err: Exception | None = None

    while retry_count <= max_retries:
        try:
            res_objs = await ctx.userbot.forward_messages(
                entity=ctx.target_entity, messages=sub_ids, from_peer=ctx.source_entity, silent=True
            )
            break

        except FloodWaitError as e:
            # 顶级熔断拦截！绝对休眠，严禁轰炸
            wait_time = e.seconds + 2
            await _log_task_event(
                state,
                f"⏳ 触发 Telegram API 速率限制 (FloodWait: {e.seconds}s)，执行最高优先级熔断保护，休眠 {wait_time}s...",
                "WARNING",
            )
            ctx.pacer.record_flood_wait()
            await _flood_wait_cooldown(state, wait_time)
            # 休眠完成后原批次重试
            continue

        except RPCError as e:
            action = await _handle_forward_send_failure(state, e)
            if action == "isolate":
                return await _isolate_or_bisect(state, ctx, sub_ids, depth, str(e))
            last_transient_err = e
            retry_count += 1
            await _retry_transient_forward(state, ctx, retry_count, str(e))
            continue

        except Exception as e:
            action = await _handle_forward_send_failure(state, e)
            if action == "isolate":
                return await _isolate_or_bisect(state, ctx, sub_ids, depth, str(e))
            last_transient_err = e
            retry_count += 1
            await _retry_transient_forward(state, ctx, retry_count, str(e))
            continue

    if res_objs is None:
        # 传输层故障重试耗尽直接上抛: 断连/超时无法通过二分修复，且严禁落 bad_id 或放大退避
        if isinstance(last_transient_err, (ConnectionError, TimeoutError, OSError)):
            raise last_transient_err
        # 节点拥塞重试耗尽: 多条可二分降级 (子批各自有全新重试预算); 单条耗尽直接上抛转任务失败
        if len(sub_ids) > 1:
            return await _isolate_or_bisect(state, ctx, sub_ids, depth, "瞬态重试耗尽")
        raise RuntimeError(
            f"转发瞬态重试耗尽 (连续 {max_retries + 1} 次失败)，消息 ID {sub_ids[0]} 未转发，任务转失败待恢复重试"
        )

    # 发送成功后的记账与日志移出重试 try: 日志/账本失败绝不能触发重发
    # (Telethon 转发每次产生新随机 ID, 重放会在目标频道产生重复消息)
    return await _finalize_successful_forward(state, ctx, sub_ids, res_objs, depth)


async def _finalize_successful_forward(
    state: "_ForwardRunState", ctx: "_ForwardRunCtx", sub_ids: list[int], res_objs: Any, depth: int
) -> tuple[int, list[int]]:
    """Persist confirmed mappings before fallible log writes; unresolved responses remain retryable."""
    new_mappings = _record_mapping(state, sub_ids, res_objs)
    ctx.pacer.record_success()
    succ_src_ids = {m[0] for m in new_mappings}
    missing_ids = [s for s in sub_ids if s not in succ_src_ids]
    await _flush_partial_ledger(state, [], missing_ids or None)
    batch_str = f"ID: {sub_ids[0]} ~ {sub_ids[-1]}" if len(sub_ids) > 1 else f"ID: {sub_ids[0]}"
    try:
        if depth > 0:
            await _log_task_event(state, f"✅ [二分子批次] 成功转发 {len(new_mappings)} 条消息 ({batch_str})")
        else:
            await _log_task_event(state, f"✅ [批量秒转] 成功转发 {len(new_mappings)} 条消息 ({batch_str})")
        if missing_ids:
            await _log_task_event(
                state,
                f"⚠️ 本批 {len(missing_ids)}/{len(sub_ids)} 条消息 Telegram 未返回结果，保留未覆盖以便重试",
                "WARNING",
            )
    except Exception as log_err:
        logger.warning("转发成功明细日志写入失败 (映射已持久化，不阻断批结算): %s", log_err)
    if missing_ids:
        raise RuntimeError(f"Telegram 未返回消息映射，待恢复重试: {missing_ids}")
    return len(new_mappings), []


async def _commit_batch(state: _ForwardRunState, ctx: _ForwardRunCtx, batch_ids: list[int]) -> None:
    """账本落库段: 批前防重剔除 (增量模式) → 云端二分转发 → 账本单事务落库 → 进度广播 → 节流 → 试转暂停"""
    if not batch_ids and not state.pending_mappings and not state.pending_skips:
        return

    # 批前信号检查: 全防重剔除/纯 skip 提交不触发 RPC, 暂停/取消不能被记账流程淹没
    await _check_task_signals(state)

    # 1. 增量模式: 凑批提交前按批查询已处理集合，剔除已转发/已处置消息 (一批一次 DB 往返)
    ids_to_forward = list(batch_ids)
    if ids_to_forward and not state.full_retransfer:
        done_ids = await asyncio.to_thread(
            ForwardLedgerRepository.get_processed_ids,
            str(state.parsed_source),
            str(state.parsed_target),
            ids_to_forward,
        )
        if done_ids:
            ids_to_forward = [i for i in ids_to_forward if i not in done_ids]
            removed = len(batch_ids) - len(ids_to_forward)
            state.dedup_skipped += removed
            if removed == len(batch_ids):
                await _log_task_event(
                    state, f"♻️ [防重] 本批 {removed} 条消息均已处理过，跳过转发 (目标频道不会出现重复)"
                )
            elif removed > 0:
                await _log_task_event(state, f"♻️ [防重] 本批 {len(batch_ids)} 条中 {removed} 条已处理过，剔除后继续")

    # 2. 云端转发 (未全部防重剔除时)
    rpc_happened = False
    if ids_to_forward:
        rpc_happened = True
        succ, bads = await _forward_sub_batch(state, ctx, ids_to_forward, depth=0)
        state.forwarded_count += succ
        state.skipped_dead_count += len(bads)
        state.bad_isolated_count += len(bads)
        if bads and len(state.bad_id_list) < 500:
            state.bad_id_list.extend(bads[: 500 - len(state.bad_id_list)])
        for bad_id in bads:
            state.pending_skips.append((bad_id, "bad_id", "二分隔离出的不可转发坏消息", str(state.parsed_target)))

    # 3. 账本单事务落库: 成功映射 + 本批 skip + 覆盖区间 [seg_lo, seg_hi]
    #    区间端点覆盖 本批 ID + 自上次落库以来的过滤 skip ID (迭代单调递增，区间内的空洞即不存在/已处理消息)
    seg_ids = list(batch_ids) + [m[0] for m in state.pending_mappings] + [s[0] for s in state.pending_skips]
    if seg_ids:
        seg_start = min(seg_ids)
        seg_end = max(seg_ids)
        if state.current_album_buffer:
            # 相册缓冲尚未发送: 覆盖段上界压缩至缓冲区之前, 绝不让 [seg_start, seg_end] 把未发送
            # 的缓冲相册 ID 标成已覆盖 (否则 dry_run 暂停/中断后 resume 会跳过永不发送的相册)。
            # 缓冲区之后的越界 skip 单独记录在 forward_skips, 防重按逐条记录兜底, 不依赖段覆盖。
            buf_lo = min(state.current_album_buffer)
            if buf_lo > seg_start:
                seg_end = min(seg_end, buf_lo - 1)
        await asyncio.to_thread(
            ForwardLedgerRepository.commit_batch_records,
            task_id=state.task_id,
            source_chat=str(state.parsed_source),
            target_chat=str(state.parsed_target),
            mappings=list(state.pending_mappings),
            skips=list(state.pending_skips),
            seg_start=seg_start,
            seg_end=seg_end,
        )
        state.pending_mappings.clear()
        state.pending_skips.clear()

    # 广播任务进度 (分子=本次运行已处理消息数: 转存+过滤/隔离+防重跳过; 分母=本次任务范围消息数)
    await _broadcast_forward_batch_progress(state)

    # 仅实际发生过 RPC 时才执行批间节流
    if rpc_happened:
        await ctx.pacer.pace()

    # 试转模式: 首个批次完成后自动置为 paused (复用现有暂停信号，不引入新状态值)
    if state.dry_run and state.task_id and not state.dry_pause_done:
        from app.core.runtime import active_task_controls

        state.dry_pause_done = True
        active_task_controls.setdefault(state.task_id, {})
        active_task_controls[state.task_id]["paused"] = True
        await TaskQueueRepository.update_status_async(state.task_id, "paused")
        await connection_manager.broadcast({"type": "task_update", "task_id": state.task_id, "status": "paused"})
        await _log_task_event(state, "🎬 试转首批已完成，已自动暂停；请到目标频道验收消息后点击【恢复】继续全量转发")
        await _check_task_signals(state)


async def _process_scanned_message(state: "_ForwardRunState", ctx: "_ForwardRunCtx", msg: Any) -> None:
    """扫描单体处理: 信号检查 + 本地纯净度过滤 + Album-Aware 缓冲分批 (过滤类 skip 为
    目标无关处置, target_chat=NULL 跨目标复用)"""
    # 扫描期信号检查: 纯过滤/全防重路径不经过 _forward_sub_batch, 暂停/取消信号不能被淹没
    await _check_task_signals(state)
    # 本地纯净度判定 (0ms, 0 RPC)
    is_ok, reason = MessagePureFilter.is_forwardable(msg)
    if not is_ok:
        state.skipped_dead_count += 1
        state.filtered_reason_counts[reason] = state.filtered_reason_counts.get(reason, 0) + 1
        if msg is not None and getattr(msg, "id", None) is not None:
            state.pending_skips.append((msg.id, reason, None, None))
        # 过滤/死消息处理：若遇到不同 grouped_id 则结算上一相册
        msg_gid = getattr(msg, "grouped_id", None)
        if msg_gid is not None and state.active_grouped_id is not None and msg_gid != state.active_grouped_id:
            await _flush_album_buffer(state, ctx)
        return

    msg_gid = getattr(msg, "grouped_id", None)
    if msg_gid is not None:
        # 属于相册消息
        if state.active_grouped_id is not None and msg_gid != state.active_grouped_id:
            # 遇到新相册，先结算上一相册
            await _flush_album_buffer(state, ctx)
        state.active_grouped_id = msg_gid
        state.current_album_buffer.append(msg.id)
        return

    # 属于普通独立消息
    if state.active_grouped_id is not None:
        # 上一个相册完结，结算相册缓冲
        await _flush_album_buffer(state, ctx)

    state.current_batch_ids.append(msg.id)
    if len(state.current_batch_ids) >= state.batch_size:
        await _commit_batch(state, ctx, state.current_batch_ids)
        state.current_batch_ids = []


async def _broadcast_forward_batch_progress(state: "_ForwardRunState") -> None:
    """批后进度回写与 WS 广播 (分子=转存+过滤/隔离+防重跳过, 分母=本次任务范围消息数)"""
    if not state.task_id:
        return
    processed_total = state.forwarded_count + state.skipped_dead_count + state.dedup_skipped
    current_item_str = (
        f"已处理 {processed_total}/{state.run_total} "
        f"(转存 {state.forwarded_count}, 过滤/隔离 {state.skipped_dead_count}, 防重跳过 {state.dedup_skipped})"
    )
    percent = round((processed_total / state.run_total * 100), 1) if state.run_total > 0 else 0.0
    await TaskQueueRepository.update_progress_async(
        state.task_id, 0, 0, processed_total, state.run_total, 0.0, 0.0, current_item_str
    )
    await connection_manager.broadcast(
        {
            "type": "progress",
            "task_id": state.task_id,
            "progress": {
                "processed_bytes": 0,
                "total_bytes": 0,
                "processed_items": processed_total,
                "total_items": state.run_total,
                "percent": min(100.0, percent),
                "speed_mb_s": 0.0,
                "eta_s": 0.0,
                "current_item": current_item_str,
                "concurrency": 0,
                "concurrency_total": 0,
                "dc_id": 0,
            },
        }
    )


async def _flush_album_buffer(state: _ForwardRunState, ctx: _ForwardRunCtx) -> None:
    """相册缓冲段 —— Album-Aware 缓冲刷新与原子对齐：
    - 若当前批次已有消息，且加入当前相册后会超出 batch_size，则将当前批次提前独立提交，
      将当前相册作为新批次整体顺延，防止相册跨批次被生硬撕裂。
    - 若相册本身大小 >= batch_size (如 10 张大相册)，合并后立即整批提交。"""
    if not state.current_album_buffer:
        state.active_grouped_id = None
        return

    if state.current_batch_ids and (len(state.current_batch_ids) + len(state.current_album_buffer) > state.batch_size):
        await _commit_batch(state, ctx, state.current_batch_ids)
        state.current_batch_ids = list(state.current_album_buffer)
    else:
        state.current_batch_ids.extend(state.current_album_buffer)

    state.current_album_buffer = []
    state.active_grouped_id = None

    if len(state.current_batch_ids) >= state.batch_size:
        await _commit_batch(state, ctx, state.current_batch_ids)
        state.current_batch_ids = []


async def _select_forward_userbot(client_manager: Any, account_id: int | None) -> tuple[Any, int | None]:
    """解析转发任务实际使用的 userbot 及该客户端真实所属账号 ID (回退默认池时归属同步回落)。"""
    owner = client_manager.get_account(account_id)
    client = await client_manager.get_client_for_task(account_id, mode="userbot")
    if client:
        return client, owner.account_id if owner else account_id
    default_owner = client_manager.get_account()
    return client_manager.userbot, default_owner.account_id if default_owner else account_id


async def _resolve_forward_pair(
    state: "_ForwardRunState",
    userbot,
    source_chat: str,
    target_chat: str,
    current_account_id: int | None,
    attempted_accounts: set,
):
    """源/目标实体连通性解析 (权限异常自动故障转移换号; 会话失效统一上报后原样上抛)。

    返回 (userbot, source_entity, target_entity, current_account_id) —— 故障转移后
    userbot 与账号归属同步换血。"""
    from app.core.client_manager import client_manager, is_channel_permission_error

    while True:
        try:
            source_entity = await ForwardService.resolve_entity(userbot, state.parsed_source)
            target_entity = await ForwardService.resolve_entity(userbot, state.parsed_target)
            _reject_same_peer_pair(source_entity, target_entity)
            return userbot, source_entity, target_entity, current_account_id
        except Exception as e:
            # 账号会话失效: 统一置 failed + 全局事件引导重绑 (区别于普通实体解析失败)
            if is_session_invalid_error(e):
                err_account_id = state.client_account_id if state.client_account_id is not None else current_account_id
                await report_session_invalid(state.task_id, e, account_id=err_account_id)
                raise e
            if is_channel_permission_error(e):
                alt_acc = await client_manager.find_failover_account(
                    attempted_accounts, source_chat, task_type="forward_channel"
                )
                if alt_acc:
                    old_acc = client_manager.get_account(current_account_id)
                    old_name = old_acc.account_name if old_acc else f"ID:{current_account_id}"
                    await _log_task_event(
                        state,
                        f"⚠️ 账号 [{old_name}] 无权访问私有频道，已自动无缝故障转移至账号 [{alt_acc.account_name}] 接替执行",
                        "WARNING",
                    )
                    from app.core.scheduler import task_scheduler

                    # 先存旧账号再换血: reassign 的 old/new 必须是真实的前后账号,
                    # 传新账号自身会把原账号的配额计数永久虚高 (与 download_runner 口径一致)
                    old_account_id = current_account_id
                    current_account_id = alt_acc.account_id
                    attempted_accounts.add(current_account_id)
                    if state.task_id:
                        task_scheduler.reassign_task_account(state.task_id, old_account_id or 0, alt_acc.account_id)
                        await TaskQueueRepository.update_account_id_async(state.task_id, current_account_id)
                        await connection_manager.broadcast(
                            {"type": "task_update", "task_id": state.task_id, "account_id": current_account_id}
                        )
                    userbot, state.client_account_id = await _select_forward_userbot(client_manager, current_account_id)
                    continue

            err_text = f"❌ 无法连接或解析目标频道/群组: {e}"
            await _log_task_event(state, err_text, "ERROR")
            raise e


async def _compute_forward_run_total(userbot, source_entity, start_id: int | None, end_id: int | None) -> int:
    """进度分母口径 = 任务范围消息数 (与 /api/forward/estimate 的 span_count 一致)，而非整频道总数"""
    res = await userbot.get_messages(source_entity, limit=0)
    channel_total = getattr(res, "total", 0)
    if start_id and end_id:
        return end_id - start_id + 1
    if start_id:
        # 只有起点: 取最新一条消息 ID 作为范围上界 (1 次廉价 RPC)
        try:
            newest_msgs = await userbot.get_messages(source_entity, limit=1)
            newest_id = int(newest_msgs[0].id) if newest_msgs and hasattr(newest_msgs[0], "id") else int(start_id)
            return max(1, newest_id - int(start_id) + 1)
        except (ValueError, TypeError):
            return int(channel_total or 1)
    if end_id:
        # 只有终点: 从频道首条 (ID 1) 迭代到 end_id
        return end_id
    # 整频道任务: 频道总数即任务总数
    return channel_total


async def _broadcast_forward_start(state: _ForwardRunState, start_id: int | None, end_id: int | None) -> None:
    """转发启动态: running 置位 + 初始进度回写 + WS 广播"""
    if not state.task_id:
        return
    await TaskQueueRepository.update_status_async(state.task_id, "running")
    range_hint = f" (范围 ID {start_id or 1} ~ {end_id or '最新'})" if (start_id or end_id) else ""
    await TaskQueueRepository.update_progress_async(
        state.task_id,
        0,
        0,
        0,
        state.run_total,
        0.0,
        0.0,
        f"🚀 流式纯净转存已启动，预计处理 ~{state.run_total} 条消息{range_hint}...",
    )
    await connection_manager.broadcast(
        {
            "type": "progress",
            "task_id": state.task_id,
            "progress": {
                "processed_bytes": 0,
                "total_bytes": 0,
                "processed_items": 0,
                "total_items": state.run_total,
                "percent": 0.0,
                "speed_mb_s": 0.0,
                "eta_s": 0.0,
                "current_item": "🚀 流式纯净转存已启动...",
                "concurrency": 0,
                "concurrency_total": 0,
                "dc_id": 0,
            },
        }
    )


def _build_iter_range_kwargs(start_id: int | None, end_id: int | None) -> dict:
    """构造 iter_messages 范围 (reverse 流式; min/max 边界按任务范围收敛)"""
    kwargs: dict[str, Any] = {"reverse": True}
    min_id = (start_id - 1) if start_id and start_id > 1 else 0
    max_id = (end_id + 1) if end_id else 0
    if min_id > 0:
        kwargs["min_id"] = min_id
    if max_id > 0:
        kwargs["max_id"] = max_id
    return kwargs


async def _log_forward_startup(
    state: _ForwardRunState, source_chat: str, target_chat: str, batch_size: int, delay_seconds: float
) -> None:
    """启动横幅: 引擎初始化 + 源/目标解析 + 运行配置 (任务日志面)"""
    await _log_task_event(state, "🚀 开始初始化云端极速转发引擎 (防风控二分高鲁棒版本)...")
    await _log_task_event(state, f"📌 源频道/群组: {source_chat} (解析后: {state.parsed_source})")
    await _log_task_event(state, f"📌 目标频道/群组: {target_chat} (解析后: {state.parsed_target})")
    await _log_task_event(
        state, f"⚙️ 运行配置: 批次上限 {batch_size} 条 | 基准发包延迟 {delay_seconds}s | 纯净过滤+二分隔离: 开启"
    )


async def _summarize_forward_completion(state: _ForwardRunState) -> None:
    """总结完成段: 回执小结构 + 丢弃清单摘要 + 终态进度对齐 (已处理总数覆盖扫描区间)"""
    filtered_count = max(0, state.skipped_dead_count - state.bad_isolated_count)
    final_summary = {
        "forwarded": state.forwarded_count,
        "filtered": filtered_count,
        "bad": state.bad_isolated_count,
        "dedup_skipped": state.dedup_skipped,
    }
    if state.task_id:
        await asyncio.to_thread(TaskQueueRepository.merge_params, state.task_id, {"final_summary": final_summary})
        # 完成态写入加 CAS 守卫: 终段多次 await 期间用户取消/暂停落库的状态不得被无条件覆写
        completed = await TaskQueueRepository.update_status_async(state.task_id, "completed", ("running",))
        if not completed:
            await _log_task_event(state, "🛑 任务状态已被外部取消或暂停，跳过完成态写入与完成广播", "WARNING")
            return
    # 完成庆祝与丢弃摘要仅在终态写入成功 (或无任务行的直调场景) 后记录:
    # 取消竞态下不得先出现成功日志再出现拒绝警告 (批 A 复审)
    await _log_task_event(
        state,
        f"🎉 云端极速转发完成！共成功处理 {state.forwarded_count} 条有效消息 "
        f"(策略过滤 {filtered_count} 条, 坏消息隔离 {state.bad_isolated_count} 条, 防重跳过 {state.dedup_skipped} 条已处理消息)！",
    )
    # 丢弃清单摘要: 每条被跳过的消息均有原因可查 (过滤分类计数 + 坏消息 ID，逐条明细见上方日志与完成回执)
    if filtered_count > 0 and state.filtered_reason_counts:
        reason_digest = ", ".join(
            f"{k} ×{v}" for k, v in sorted(state.filtered_reason_counts.items(), key=lambda kv: -kv[1])
        )
        await _log_task_event(state, f"🗑️ 丢弃清单摘要 [策略过滤 {filtered_count} 条]: {reason_digest}")
    if state.bad_isolated_count > 0 and state.bad_id_list:
        ids_digest = ", ".join(str(i) for i in state.bad_id_list[:100])
        more = f" ...等共 {len(state.bad_id_list)} 条" if len(state.bad_id_list) > 100 else ""
        await _log_task_event(
            state,
            f"🗑️ 丢弃清单摘要 [坏消息隔离 {state.bad_isolated_count} 条]: ID {ids_digest}{more} (单条原因详见上方隔离日志)",
        )
    if state.task_id:
        summary_str = (
            f"全部云端秒转完成 (成功 {state.forwarded_count} 条, 过滤 {filtered_count} 条, "
            f"坏消息隔离 {state.bad_isolated_count} 条, 防重跳过 {state.dedup_skipped} 条)"
        )
        # 完成态保留真实比例并确保完全覆盖扫描区间:
        # 已处理总数包含 成功转发 + 策略过滤 + 坏消息隔离 + 防重跳过；
        # 正常完成意味着扫描区间完全覆盖，终态已处理数与区间总数 run_total 对齐
        calculated_processed = state.forwarded_count + filtered_count + state.bad_isolated_count + state.dedup_skipped
        final_processed = max(state.run_total, calculated_processed) if state.run_total > 0 else calculated_processed
        run_total = state.run_total if state.run_total > 0 else final_processed

        await TaskQueueRepository.update_progress_async(
            state.task_id, 0, 0, final_processed, run_total, 0.0, 0.0, summary_str
        )
        # 对齐 download/publish: 完成时广播 task_update，前端据此刷新任务列表并拉取回执
        await connection_manager.broadcast({"type": "task_update", "task_id": state.task_id, "status": "completed"})
        await connection_manager.broadcast(
            {
                "type": "progress",
                "task_id": state.task_id,
                "progress": {
                    "processed_bytes": 0,
                    "total_bytes": 0,
                    "processed_items": final_processed,
                    "total_items": run_total,
                    "percent": 100.0,
                    "speed_mb_s": 0.0,
                    "eta_s": 0.0,
                    "current_item": summary_str,
                    "concurrency": 0,
                    "concurrency_total": 0,
                    "dc_id": 0,
                },
                "summary": final_summary,
            }
        )


async def run_forward_channel_task(
    task_id: str,
    source_chat: str,
    target_chat: str,
    start_id: int | None = None,
    end_id: int | None = None,
    batch_size: int = 50,
    delay_seconds: float = 1.5,
    full_retransfer: bool = False,
    dry_run: bool = False,
    account_id: int | None = None,
):
    # dry_run 仅首次派发生效 (试转自动暂停后由用户恢复)；resume 重派发不传该参数即默认 False
    # 调度窗口自检 (统一范式见 runtime.try_claim_executor_entry): 已被用户取消/暂停则直接退出
    if not await asyncio.to_thread(try_claim_executor_entry, task_id):
        await TaskQueueRepository.append_log_async(
            task_id, "[执行器] 任务在调度窗口内已被用户取消/暂停，跳过执行且不覆盖终态"
        )
        return

    current_account_id = account_id
    if current_account_id is None:
        try:
            t_rec = await asyncio.to_thread(TaskQueueRepository.get_by_id, task_id)
            if t_rec:
                current_account_id = t_rec.get("account_id")
        except Exception as exc:
            logger.debug("获取转发任务执行账号异常，继续沿用默认账号 (非致命): %s", exc)

    try:
        await ForwardService.forward_channel_messages(
            source_chat=source_chat,
            target_chat=target_chat,
            start_id=start_id,
            end_id=end_id,
            batch_size=batch_size,
            delay_seconds=delay_seconds,
            task_id=task_id,
            full_retransfer=full_retransfer,
            dry_run=dry_run,
            account_id=current_account_id,
        )
    except ForwardCancelledError:
        logger.info("转发任务 [%s] 已被用户手动取消", task_id)
        raise
    except ForwardPausedError:
        logger.info("转发任务 [%s] 已安全暂停并释放槽位让位", task_id)
        raise
    except Exception as e:
        from telethon.errors import FloodWaitError

        if isinstance(e, FloodWaitError):
            raise
        # service 层已知失败路径已写日志; 此处仅兜底未被 service 处理的异常, 避免重复日志
        logger.error(f"❌ 极速转发后台任务异常 [{task_id}]: {e}")
        current = await asyncio.to_thread(TaskQueueRepository.get_by_id, task_id)
        if not current or current.get("status") not in ("failed", "paused", "cancelled"):
            await TaskQueueRepository.append_log_async(task_id, f"❌ 转发任务因异常中断: {e}")
        raise
    finally:
        release_runner_controls(task_id)
