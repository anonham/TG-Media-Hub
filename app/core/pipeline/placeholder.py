"""发布占位卡子系统

三件套:
- resolve_placeholder_media: 占位海报字节解析 (item.thumbnail_data → PikPak fetch_thumbnail
  增强来源 → 随应用分发的静态资产; 静态资产本地读, 不新增任何 URL 抓取面 —— §1 红线);
- PlaceholderCardSender: run 级发卡器 (J10) —— 失败 1:1 Photo 卡, 槽复用 fail_count++
  防跨重试刷屏 (J6), 封顶 10 逐条 + 1 溢出汇总, 发送前一律 sleep(delay_seconds) 顺序节流
  (严禁并发连发, R7), 一律 userbot, 异常就地吞并
  记日志不熔断主流程 (J12);
- PlaceholderSlotCoordinator: 终态/翻转/自愈协调入口 (包4 消费, 本包落骨架)。

文案为 已裁决定稿 (§3.3.1): 面向频道读者, 被动时态 —— ⏳ 卡绝不承诺"正在补发"
(逐文件失败不触发任务级自动重试, publish_runner.py 实证), {简明原因} 经
summarize_failure_reason 四类归类映射, 不得直出异常文本。
"""

import asyncio
import io
import logging
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

from app.config import settings
from app.core.pipeline.engines.exceptions import TransportError
from app.core.pipeline.models import PublishContext, PublishItem
from app.core.pipeline.runtime import card_throttle_wait
from app.db.publish_repository import PlaceholderSlotRepository

logger = logging.getLogger(__name__)


def as_named_stream(payload: Any) -> Any:
    """海报字节 → 带 .name 的 BytesIO。

    Telethon 对裸 bytes 无文件名可判型: utils.is_image 恒 False → 以无名文档形态
    上传, ⏳ 失败卡静默失去 Photo 形态 (文本消息不可媒体编辑, 原地回填承诺整体
    失效) —— live E2E 包 5 实证后收口。缓存句柄 (Photo 对象) 原样透传。"""
    if isinstance(payload, bytes):
        stream = io.BytesIO(payload)
        stream.name = "placeholder_poster.jpg"
        return stream
    return payload


_as_named_stream = as_named_stream


# ---------------------------------------------------------------------------
# 已裁决定稿文案 (被动时态; 不暴露运维 UI 名词)
# ---------------------------------------------------------------------------

PENDING_CARD_TEXT = "⏳ 【补发排队】`{name}`\n本条媒体未能按时送达，任务重试后将在原位刷新为正式媒体。"
TERMINAL_CARD_TEXT = "❌ 【传输中断归档】`{name}`\n该媒体经多次补发尝试仍未送达（{reason}），已停止自动补发。"
GROUP_PENDING_TEXT = (
    "⚠️ 【相册等待补发】\n"
    "本批次包含 {count} 项媒体的相册组遇到传输异常，重试成功后将在后方整组补发，届时本卡将标记完成。"
)
GROUP_TERMINAL_TEXT = "❌ 【相册补发终止】该相册组经多次重发均未送达，已停止调度。"
GROUP_PARTIAL_TERMINAL_TEXT = "❌ 【相册补发终止】该相册组部分成员经多次重发仍未送达，已停止调度。"
OVERFLOW_TEXT = "⚠️ 【批量异常提示】\n当前批次有多项媒体传输异常（超出单批逐条展示上限），请在控制台查看完整明细。"
# R2 (扩展): 组发成功后对命中槽卡的 best-effort 翻转文案
FLIPPED_TEXT = "✅ 已补发（见下方消息）"

_SUMMARY_PENDING_STATES = ("placeholder_sent", "editing", "failed_terminal")

# D6 (已裁决): 每任务(运行)最多 10 张逐条失败卡 + 1 张溢出汇总卡
PER_ITEM_CARD_CAP = 10

# 汇总卡正文列名上限 (超出以"等 N 项"收口, 防长名单刷屏)
SUMMARY_NAME_LIMIT = 10

# 简明原因四类归类: 关键字 → 展示文案; 兜底"传输异常"
_REASON_KEYWORDS = (
    (("not found", "找不到", "缺失", "no such file", "不存在"), "源文件缺失"),
    (("体积", "超限", "too large", "size"), "体积超限"),
    (("timeout", "timed out", "超时"), "连接超时"),
    (("connection", "network", "连接", "网络", "unreachable", "dns"), "远端不可达"),
)


def summarize_failure_reason(raw_reason: str) -> str:
    """异常文本 → 简明原因: 四类归类映射, 兜底'传输异常'; 不得直出异常文本"""
    text = (raw_reason or "").lower()
    for keywords, label in _REASON_KEYWORDS:
        if any(k in text for k in keywords):
            return label
    return "传输异常"


_POSTER_CACHE: bytes | None = None


def load_static_poster() -> bytes | None:
    """随应用分发的静态占位海报 (1280×720 16:9 JPEG ≤40KB, 暗底大字)。

    模块级单次装载; 资产缺失/读失败返回 None (调用方降级纯文本 ⏳ 卡, 永不致命)。
    红线: 仅本地文件读, 零 URL 抓取; 运行时零自愈写回 (否决 Base64 方案)。
    资产禁放 static/ (Vite 构建清仓), 落位 app/assets/ (J7)。
    """
    if _POSTER_CACHE is not None:
        return _POSTER_CACHE
    try:
        data = Path(settings.PLACEHOLDER_ASSET_PATH).read_bytes()
        if data:
            mod: Any = sys.modules[__name__]
            mod._POSTER_CACHE = data
            return data
    except Exception as asset_err:
        logger.warning("占位海报资产装载失败 (降级纯文本卡): %s", asset_err)
    return None


_load_static_poster = load_static_poster


def _slot_identity(item: PublishItem, context: PublishContext) -> str:
    """槽表 file_path = 发布身份逻辑路径 (R6 已裁决): 本地=绝对路径; PikPak=pikpak://
    {parent_folder_id}/{name} (先例 pipeline.py 的 publish_history 口径)。"""
    if context.source_type == "pikpak":
        return f"pikpak://{getattr(item, 'parent_folder_id', '')}/{item.name}"
    return item.item_id


def resolve_placeholder_media(item: PublishItem, context: PublishContext) -> bytes | None:
    """占位海报字节解析: item.thumbnail_data (若数据源已透传二进制字节) → 静态资产。

    PikPak 封面增强来源 (fetch_thumbnail, 既有 SSRF 口径) 需 PublishItem 携带
    thumbnail_link —— 当前 sources 未透传二进制缩略图字节, 缺失或为 URL 字符串时一律回落静态资产 (裁决口径)。"""
    data = getattr(item, "thumbnail_data", None)
    if isinstance(data, (bytes, bytearray, memoryview)) and data:
        return bytes(data)
    return _load_static_poster()


def _names_preview(members: list[Any]) -> str:
    names = [str(getattr(m, "name", None) or getattr(m, "item_id", "")) for m in members]
    shown = names[:SUMMARY_NAME_LIMIT]
    preview = "、".join(shown)
    if len(names) > SUMMARY_NAME_LIMIT:
        preview += f" 等 {len(names)} 项"
    return preview


def _is_not_modified_error(exc: BaseException) -> bool:
    """Telethon MESSAGE_NOT_MODIFIED 判定 (幂等文案更新不视作错误)."""
    try:
        from telethon import errors as tg_errors

        not_modified_cls = getattr(tg_errors, "MessageNotModifiedError", None)
        if not_modified_cls is not None and isinstance(exc, not_modified_cls):
            return True
    except Exception as imp_err:
        logger.debug("导入 telethon.errors 异常 (忽略): %s", imp_err)
    return "not modified" in str(exc).lower()


def _resolve_slot_edit_client(sender_account_id: int, context: PublishContext) -> Any:
    """按槽记录的发送账号解析编辑用 userbot 客户端; 同账号或未记录时回退 context.userbot_client。"""
    if sender_account_id:
        resolved = PlaceholderSlotCoordinator._resolve_userbot(sender_account_id)
        if resolved is not None:
            return resolved
        if int(getattr(context, "account_id", 0) or 0) == sender_account_id:
            return context.userbot_client
        return None
    return context.userbot_client or PlaceholderSlotCoordinator._resolve_userbot(0)


# 激活重试: 单行 UPDATE 的短退避, 覆盖预占→激活窗口内的瞬态 DB busy (WAL 写锁竞争)
_ACTIVATION_ATTEMPTS = 3
_ACTIVATION_RETRY_DELAY = 0.05


async def _activate_reserved_slot_with_retry(slot_id: int, *, msg_id: int, state: str) -> bool:
    """激活预占槽 (短暂重试); False = 重试耗尽或状态守卫未命中, 调用方按未激活处理并回收预留."""
    for attempt in range(_ACTIVATION_ATTEMPTS):
        try:
            if PlaceholderSlotRepository.activate_reserved_slot(slot_id, placeholder_msg_id=msg_id, state=state):
                return True
            logger.warning("占位槽 %d 激活未命中 (state != 'sending', 疑似并发回收), 按未激活处理", slot_id)
            return False
        except Exception as act_err:
            if attempt == _ACTIVATION_ATTEMPTS - 1:
                logger.warning("占位槽 %d 激活失败 (已重试 %d 次): %s", slot_id, _ACTIVATION_ATTEMPTS, act_err)
                return False
        await asyncio.sleep(_ACTIVATION_RETRY_DELAY)
    return False


class PlaceholderCardSender:
    """run 级发卡器 (J10): 封顶计数与 InputPhoto 句柄缓存随单次流水线运行生命周期。

    发卡一律经 context.userbot_client; 每张卡发送前 sleep(get_current_send_delay_seconds)
    顺序节流; 全部异常就地吞并 (J12) —— 卡是附属品, 不放大主流程失败。
    """

    def __init__(self) -> None:
        self._per_item_cards = 0
        self._overflow_count = 0
        self._overflow_card_sent = False
        # run 级 InputPhoto 句柄缓存 —— 首卡上传后复用 sent_msg.photo, 第 2+ 张
        # 卡零上传; FILE_REFERENCE_EXPIRED 由重试循环兜底重新上传。缓存不免节流。
        self._poster_handle: Any = None

    def cards_sent_count(self) -> int:
        return self._per_item_cards

    def overflow_sent(self) -> bool:
        return self._overflow_card_sent

    def record_external_per_item_card(self) -> None:
        self._per_item_cards += 1

    def record_external_overflow_card(self) -> None:
        self._overflow_count += 1
        self._overflow_card_sent = True

    def record_external_overflow_skip(self) -> None:
        self._overflow_count += 1

    @staticmethod
    async def _reconcile_reused_terminal_slot(
        existing: dict[str, Any],
        item: PublishItem,
        context: PublishContext,
        identity: str,
        reason: str,
    ) -> None:
        """复用槽终态化与文案对账: 已发布/已翻转/回填中/发送中槽保护不变; 其余槽条件落 failed_terminal 并对 backfillable 卡做幂等文案更新."""
        state = existing.get("state")
        if state in ("published", "flipped", "editing", "sending"):
            return
        if state != "failed_terminal" and not PlaceholderSlotRepository.mark_failed_terminal(existing["id"], reason):
            return
        if existing.get("slot_kind") == "backfillable" and int(existing.get("placeholder_msg_id") or 0) > 0:
            latest = PlaceholderSlotRepository.get_by_id(existing["id"])
            if latest is not None and latest.get("state") != "failed_terminal":
                return
            display_reason = summarize_failure_reason(reason)
            term_text = TERMINAL_CARD_TEXT.format(name=item.name or identity, reason=display_reason)
            edit_client = _resolve_slot_edit_client(int(existing.get("sender_account_id") or 0), context)
            if edit_client is not None:
                try:
                    await edit_client.edit_message(
                        context.target_chat_id, int(existing["placeholder_msg_id"]), text=term_text
                    )
                except Exception as edit_err:
                    if not _is_not_modified_error(edit_err):
                        logger.debug("复用占位槽更新终态文案失败 (非致命): %s", edit_err)

    # -- 对外入口 ---------------------------------------------------------

    async def _handle_overflow_card(self, context: PublishContext, identity: str) -> None:
        self._overflow_count += 1
        if not self._overflow_card_sent:
            # D6: 第 11+ 项失败 → 1 张溢出汇总卡 (纯文本, 无槽, 终态; 后续仅计数)
            await self._sleep_throttle(context)
            overflow_msg_id = await self._send_text_quietly(context, OVERFLOW_TEXT)
            if overflow_msg_id is not None:
                self._overflow_card_sent = True
        if context.task_id:
            from app.db.task_repository import TaskQueueRepository

            await TaskQueueRepository.append_log_async(
                context.task_id, f"⏳ 失败占位卡已达封顶 ({PER_ITEM_CARD_CAP}), 后续失败仅计数: {identity}"
            )

    async def _reserve_or_reuse_backfill_slot(
        self,
        item: PublishItem,
        context: PublishContext,
        identity: str,
        size: int,
        reason: str,
        *,
        terminal: bool,
    ) -> tuple[bool, int | None]:
        """预占 backfillable 槽位; 返回 (是否应立即中止发卡, reserved_slot_id)."""
        try:
            slot_id = PlaceholderSlotRepository.insert_new_slot(
                context.target_chat_id,
                identity,
                size,
                slot_kind="backfillable",
                state="sending",
                placeholder_msg_id=0,
                client_label=context.account_display or "Userbot",
                sender_account_id=int(context.account_id or 0),
                task_id=context.task_id or "",
                fail_reason=reason,
            )
            return False, slot_id
        except sqlite3.IntegrityError:
            existing = PlaceholderSlotRepository.get_by_identity(context.target_chat_id, identity, size)
            if existing is not None:
                PlaceholderSlotRepository.increment_fail(existing["id"], reason)
                if terminal:
                    await self._reconcile_reused_terminal_slot(existing, item, context, identity, reason)
                logger.info(
                    "占位槽已存在 (并发预占复用, 不发新卡): %s (fail_count=%d)", identity, existing["fail_count"] + 1
                )
            return True, None
        except Exception as slot_err:
            logger.warning("占位槽预占写入失败 (降级仅发卡并计入封顶): %s", slot_err)
            return False, None

    async def send_failure_card(
        self, item: PublishItem, context: PublishContext, reason: str, *, terminal: bool = False
    ) -> None:
        """1:1 失败占位卡 (backfillable 槽; terminal=True 时直接以 ❌ 终态形态发卡)。

        槽复用 (J6): 发卡前先原子预占身份槽 (撞 UNIQUE 自动转入槽复用 fail_count++), 避免并发双发无槽孤儿卡。"""
        if not self._guard(context):
            return
        chat = context.target_chat_id
        identity = _slot_identity(item, context)
        size = int(item.size_bytes or 0)
        existing = PlaceholderSlotRepository.get_by_identity(chat, identity, size)
        if existing is not None:
            PlaceholderSlotRepository.increment_fail(existing["id"], reason)
            if terminal:
                await self._reconcile_reused_terminal_slot(existing, item, context, identity, reason)
            logger.info("占位槽已存在 (复用, 不发新卡): %s (fail_count=%d)", identity, existing["fail_count"] + 1)
            return

        if self._per_item_cards >= PER_ITEM_CARD_CAP:
            await self._handle_overflow_card(context, identity)
            return

        reused, reserved_slot_id = await self._reserve_or_reuse_backfill_slot(
            item, context, identity, size, reason, terminal=terminal
        )
        if reused:
            return

        activated = False
        try:
            await self._sleep_throttle(context)
            display_reason = summarize_failure_reason(reason)
            text = (
                TERMINAL_CARD_TEXT.format(name=item.name or identity, reason=display_reason)
                if terminal
                else PENDING_CARD_TEXT.format(name=item.name or identity)
            )
            msg_id = await self._send_card(context, item, text)
            if msg_id is None:
                return
            self._per_item_cards += 1
            if reserved_slot_id is None:
                return
            activated = await _activate_reserved_slot_with_retry(
                reserved_slot_id,
                msg_id=msg_id,
                state="failed_terminal" if terminal else "placeholder_sent",
            )
        finally:
            if not activated and reserved_slot_id is not None:
                # 激活耗尽仍删除预留以解锁身份 (下次失败可重发新卡); 残留 = 已发卡无槽
                # 或下次一张重复卡 (装饰性), 优于滞留 sending 永久压制发卡
                try:
                    PlaceholderSlotRepository.delete_unactivated_slot(reserved_slot_id)
                except Exception as cleanup_err:
                    logger.debug("清理未激活占位槽异常 (非致命): %s", cleanup_err)

        if context.task_id:
            from app.db.task_repository import TaskQueueRepository

            await TaskQueueRepository.append_log_async(
                context.task_id, f"⏳ 已发送失败占位卡 ({'终态' if terminal else '待回填'}): {item.name or identity}"
            )

    @staticmethod
    def _increment_existing_summary_fails(
        existing_by_identity: dict[tuple[str, int], dict[str, Any]], reason: str
    ) -> None:
        for (identity, _size), existing in existing_by_identity.items():
            if existing.get("state") in ("published", "flipped"):
                continue
            try:
                PlaceholderSlotRepository.increment_fail(existing["id"], reason)
            except Exception as inc_err:
                logger.warning("汇总卡既有槽递增失败计数异常: %s (%s)", identity, inc_err)

    @staticmethod
    def _reserve_group_summary_slots(
        members: list[Any], context: PublishContext, reason: str
    ) -> tuple[dict[tuple[str, int], dict[str, Any]], list[tuple[int, str]], list[str]]:
        chat = context.target_chat_id
        existing_by_identity: dict[tuple[str, int], dict[str, Any]] = {}
        reserved_members: list[tuple[int, str]] = []
        # 预占写入失败 (非 UNIQUE 的 DB 故障/瞬态, 或 UNIQUE 撞后回查也无槽) 的成员身份:
        # 与 "已有槽复用" 严格区分, 调用方据此降级无槽发卡而非静默吞通知 (与单卡路径对齐)
        failed_identities: list[str] = []
        seen_member_keys: set[tuple[str, int]] = set()
        for member in members:
            identity = _slot_identity(member, context)
            size = int(member.size_bytes or 0)
            key = (identity, size)
            if key in seen_member_keys:
                continue
            seen_member_keys.add(key)
            try:
                existing = PlaceholderSlotRepository.get_by_identity(chat, identity, size)
            except Exception as lookup_err:
                logger.warning("查询汇总卡成员槽失败: %s (%s)", identity, lookup_err)
                existing = None
            if existing is not None:
                existing_by_identity[key] = existing
                continue
            try:
                reserved_slot_id = PlaceholderSlotRepository.insert_new_slot(
                    chat,
                    identity,
                    size,
                    slot_kind="group_summary",
                    state="sending",
                    placeholder_msg_id=0,
                    client_label=context.account_display or "Userbot",
                    sender_account_id=int(context.account_id or 0),
                    task_id=context.task_id or "",
                    fail_reason=reason,
                )
                reserved_members.append((reserved_slot_id, identity))
            except sqlite3.IntegrityError:
                existing = PlaceholderSlotRepository.get_by_identity(chat, identity, size)
                if existing is not None:
                    existing_by_identity[key] = existing
                else:
                    # UNIQUE 撞后回查无槽 (并发删除窗口): 按预占失败降级
                    failed_identities.append(identity)
            except Exception as slot_err:
                logger.warning("汇总卡成员槽预占写入失败: %s (%s)", identity, slot_err)
                failed_identities.append(identity)
        return existing_by_identity, reserved_members, failed_identities

    @staticmethod
    async def _activate_or_cleanup_group_slots(
        reserved_members: list[tuple[int, str]], *, msg_id: int | None, terminal: bool
    ) -> None:
        if msg_id:
            target_state = "failed_terminal" if terminal else "placeholder_sent"
            for reserved_slot_id, identity in reserved_members:
                activated = await _activate_reserved_slot_with_retry(
                    reserved_slot_id, msg_id=int(msg_id), state=target_state
                )
                if not activated:
                    # 激活耗尽删除该成员预留 (解锁身份, 下次失败重发新汇总卡), 不滞留 sending。
                    # 已知残留 (串3, 显式接受): 该成员自此无槽, 翻转对账 (_collect_group_flip_targets /
                    # _summary_card_recovery_text 按共享 msg_id 兄弟槽计算) 看不见它 —— 极窄 DB 故障窗口下
                    # 汇总卡可能在他成员回填后翻 ✅ 而此成员通知已丢失; 替代方案 (不回收滞留 sending 退回
                    # 压制 Bug / 强制 failed_terminal 既不可见也压制未来发卡 / 辅助表不成比例) 均更差
                    logger.warning("汇总卡成员槽激活未成, 回收预留: %s (slot_id=%d)", identity, reserved_slot_id)
                    try:
                        PlaceholderSlotRepository.delete_unactivated_slot(reserved_slot_id)
                    except Exception as cleanup_err:
                        logger.debug("清理未激活汇总槽异常 (非致命): %s", cleanup_err)
            return
        for reserved_slot_id, _identity in reserved_members:
            try:
                PlaceholderSlotRepository.delete_unactivated_slot(reserved_slot_id)
            except Exception as cleanup_err:
                logger.debug("清理未激活汇总槽异常 (非致命): %s", cleanup_err)

    async def send_group_summary_card(
        self, members: list[Any], context: PublishContext, reason: str, *, terminal: bool = False
    ) -> None:
        """整组失败汇总卡 (文本形态, 严禁逐条连发); 发卡前先原子预占各成员 group_summary 槽
        (共享汇总卡 msg_id, 结构上禁入回填 —— R2); 已有槽的成员仅 fail_count++; 预占写入失败时与单卡
        路径同构降级为无槽 best-effort 发卡, 不静默吞掉整组失败通知。"""
        if not self._guard(context) or not members:
            return
        existing_by_identity, reserved_members, failed_identities = self._reserve_group_summary_slots(
            members, context, reason
        )
        self._increment_existing_summary_fails(existing_by_identity, reason)

        if not reserved_members and not failed_identities:
            logger.info("整组失败所有成员均已有占位槽 (全部复用递增计数, 不重复发汇总卡): %s", _names_preview(members))
            return
        if failed_identities:
            # 串4: 预占写入失败 ≠ 成员已有槽 —— 降级无槽发送, 身份不受滞留槽压制,
            # 下次失败可重建槽; 绝不因落库瞬态中断静默丢失整组失败通知
            logger.warning("汇总卡成员槽预占失败, 降级无槽发卡 (下次失败可重建槽): %s", failed_identities)

        msg_id: int | None = None
        try:
            await self._sleep_throttle(context)
            text = (
                GROUP_TERMINAL_TEXT
                if terminal
                else GROUP_PENDING_TEXT.format(count=len(members), names=_names_preview(members))
            )
            msg_id = await self._send_text_quietly(context, text)
            if not msg_id:
                return
        finally:
            await self._activate_or_cleanup_group_slots(reserved_members, msg_id=msg_id, terminal=terminal)

        if context.task_id:
            from app.db.task_repository import TaskQueueRepository

            await TaskQueueRepository.append_log_async(
                context.task_id, f"⚠️ 已发送整组失败汇总卡 ({len(members)} 项): {_names_preview(members)}"
            )

    # -- 内部 -------------------------------------------------------------

    @staticmethod
    def _guard(context: PublishContext) -> bool:
        """D5 总开关 + D4 userbot 客户端守卫 (先例 filtering.py)"""
        return bool(getattr(context, "send_placeholder_card", False) and context.userbot_client)

    @staticmethod
    async def _sleep_throttle(context: PublishContext) -> None:
        """卡发送节奏 (R7): 必须 sleep(delay_seconds), 严禁并发/绕过防刷屏设施;
        暂停信号触发排水模式 (card_throttle_wait) —— 队列剩余卡以地板间隔快速发完后再暂停"""

        delay = float(await context.get_current_send_delay_seconds())
        if delay > 0:
            await card_throttle_wait(context, delay)

    async def _send_card(self, context: PublishContext, item: PublishItem, text: str) -> int | None:
        """Photo 媒体形态发卡; 海报缺失/上传失败降级纯文本卡。返回占位卡 msg_id
        (None=两种形态均失败)。InputPhoto 缓存命中先行, 异常丢弃缓存重传一次。"""
        client = context.userbot_client
        media = resolve_placeholder_media(item, context)
        if media is not None:
            for use_cache in (True, False):
                if use_cache and self._poster_handle is None:
                    continue
                try:
                    payload = self._poster_handle if use_cache else media
                    sent = await client.send_file(
                        context.target_chat_id,
                        _as_named_stream(payload),
                        caption=text,
                        force_document=False,
                    )
                    photo = getattr(sent, "photo", None)
                    if photo is not None:
                        self._poster_handle = photo
                    return int(getattr(sent, "id", 0) or 0) or None
                except Exception as send_err:
                    logger.warning("失败占位卡 (Photo 形态, cache=%s) 发送异常: %s", use_cache, send_err)
                    self._poster_handle = None
        return await self._send_text_quietly(context, text)

    @staticmethod
    async def _send_text_quietly(context: PublishContext, text: str) -> int | None:
        try:
            sent = await context.userbot_client.send_message(context.target_chat_id, text)
            return int(getattr(sent, "id", 0) or 0) or None
        except Exception as send_err:
            logger.warning("占位文本卡发送异常: %s", send_err)
            return None


def classify_edit_failure(exc: BaseException) -> str:
    """回填 edit 失败分类 (§4.2 矩阵): "deleted" 卡被删 / "window" 超 48h 编辑窗口 /
    "other" 其余 (回退常规发送)。getattr 安全构造 (异常类缺失时不致命)。
    SOTA 批2.1: 引擎侧异常已规范化为 TransportError 包装 —— 优先解包 cause 再判型。"""
    underlying: BaseException = exc
    if isinstance(exc, TransportError) and exc.cause is not None:
        underlying = exc.cause
    try:
        from telethon import errors as tg_errors
    except Exception:
        return "other"
    text = str(underlying).upper()
    flood_cls = getattr(tg_errors, "FloodWaitError", None)
    if flood_cls is not None and isinstance(underlying, flood_cls):
        return "flood"
    if "FLOOD_WAIT" in text:
        return "flood"
    window_cls = getattr(tg_errors, "MessageEditTimeExpiredError", None)
    if window_cls is not None and isinstance(underlying, window_cls):
        return "window"
    if "MESSAGE_EDIT_TIME_EXPIRED" in text:
        return "window"
    for name in ("MessageDeletedError", "MessageIdInvalidError", "MessageEmptyError"):
        cls = getattr(tg_errors, name, None)
        if cls is not None and isinstance(underlying, cls):
            return "deleted"
    if "MESSAGE_DELETED" in text or "MESSAGE_ID_INVALID" in text:
        return "deleted"
    return "other"


class PlaceholderSlotCoordinator:
    """占位槽协调器 (包4): 终态 finalize (executor/scheduler 调用)、启动自愈 (R5)、
    组发成功翻转 (R2)。落位 app/core (J9): core 禁 import services 而 scheduler 也要调用;
    客户端统一经 client_manager.userbot 兜底链。"""

    # J2: 终态阈值 = 首次失败 + max_retries (镜像调度器 max_retries=3 → 4)
    TERMINAL_FAIL_THRESHOLD_BASE = 1

    def __init__(self, *, max_retries: int = 3) -> None:
        self._max_retries = max(0, int(max_retries))

    @staticmethod
    def _resolve_userbot(sender_account_id: int = 0) -> Any:
        """D4 账号一致解析: 已记录发送账号必须精确匹配; 未记录时回退默认主账号。

        跨账号部署下编辑必须回到发送者本人 —— 其他账号对频道内他人消息无编辑权
        (甚至不可解析该频道), edit 全败会被逐行吞掉导致 ❌ 终态化静默失效 (live
        E2E 实证; slot.sender_account_id 防御字段即为此而设, 口径)。"""
        try:
            from app.core.client_manager import client_manager

            if sender_account_id:
                account = client_manager.get_account(int(sender_account_id))
                # TelegramAccountClient 是包装器 (状态机/记账), 内部 .client 才是
                # Telethon 客户端本体 (edit_message 等真实在 inner 上)
                inner = getattr(account, "client", None)
                if inner is not None:
                    return inner
            return None if sender_account_id else client_manager.userbot
        except Exception as client_err:
            logger.warning("userbot 客户端获取失败 (跳过占位卡编辑): %s", client_err)
            return None

    def heal_editing(self) -> int:
        """R5 启动自愈: editing 孤儿槽 → placeholder_sent; 崩溃遗留的 sending 预占槽一并
        删除 (同步 DB 操作, lifespan 调用: 单实例锁 + 无活跃任务, 不可能竞态活跃预占)。"""
        changed = PlaceholderSlotRepository.revert_editing_to_sent()
        if changed:
            logger.info("占位槽启动自愈: %d 个 editing 孤儿槽已回滚为 placeholder_sent", changed)
        removed = PlaceholderSlotRepository.delete_stale_sending_slots()
        if removed:
            logger.info("占位槽启动自愈: %d 个遗留 sending 预占槽已删除", removed)
        return changed

    @staticmethod
    def matches_terminal_rejection(task_id: str, expected_error: str) -> bool:
        from app.db.task_repository import TaskQueueRepository

        task = TaskQueueRepository.get_by_id(task_id)
        return bool(task and task.get("status") == "failed" and task.get("error_detail") == expected_error)

    async def _finalize_single_slot(self, row: dict[str, Any], task_id: str, expected_error: str | None) -> int:
        kind = row["slot_kind"]
        client = self._resolve_userbot(int(row["sender_account_id"] or 0))
        if client is not None and int(row["placeholder_msg_id"] or 0) > 0:
            if kind == "group_summary":
                siblings = PlaceholderSlotRepository.list_by_placeholder_msg_id(
                    row["target_chat_id"], int(row["placeholder_msg_id"])
                )
                has_flipped = any(s.get("state") in ("flipped", "published") for s in siblings)
                text = GROUP_PARTIAL_TERMINAL_TEXT if has_flipped else GROUP_TERMINAL_TEXT
            else:
                display_name = os.path.basename(row["file_path"] or "") or row["file_path"]
                text = TERMINAL_CARD_TEXT.format(name=display_name, reason=summarize_failure_reason(row["fail_reason"]))
            from app.core.entity_resolver import parse_chat_identifier, resolve_entity

            entity = await resolve_entity(client, parse_chat_identifier(row["target_chat_id"]))
            if expected_error is not None and not self.matches_terminal_rejection(task_id, expected_error):
                return -1
            latest_slot = PlaceholderSlotRepository.get_by_id(row["id"])
            if latest_slot is None or latest_slot.get("state") != "placeholder_sent":
                return 0
            try:
                await client.edit_message(entity, int(row["placeholder_msg_id"]), text=text)
            except Exception as edit_err:
                if _is_not_modified_error(edit_err):
                    logger.debug("占位卡终态文案已就位 (共享汇总卡幂等编辑): msg_id=%s", row["placeholder_msg_id"])
                else:
                    raise
        return int(self._mark_terminal_slot(row, task_id, expected_error))

    async def finalize_terminal(self, task_id: str, *, expected_error: str | None = None) -> int:
        """任务终态 failed → 占位槽终态化:
        - backfillable & placeholder_sent & fail_count ≥ 1+max_retries (J2) → ❌ 归档文案
        - group_summary & placeholder_sent → ❌【相册补发终止】
        全程异常吞并 (J12); 无 userbot 时跳过编辑仅翻转 DB 状态。返回终态化数量。"""
        if expected_error is not None and not self.matches_terminal_rejection(task_id, expected_error):
            return 0
        rows = PlaceholderSlotRepository.list_by_task(task_id)
        threshold = self.TERMINAL_FAIL_THRESHOLD_BASE + self._max_retries
        finalized = 0
        for row in rows:
            try:
                state = row["state"]
                kind = row["slot_kind"]
                eligible = (
                    kind == "backfillable" and state == "placeholder_sent" and int(row["fail_count"] or 0) >= threshold
                ) or (kind == "group_summary" and state == "placeholder_sent")
                if not eligible:
                    continue
                delta = await self._finalize_single_slot(row, task_id, expected_error)
                if delta < 0:
                    return finalized
                finalized += delta
            except Exception as slot_err:
                logger.warning("占位槽终态化失败 (id=%s): %s", row["id"], slot_err)
        if finalized:
            logger.info("任务 [%s] 终态化占位槽 %d 个 (❌ 归档文案)", task_id, finalized)
        return finalized

    @staticmethod
    def _mark_terminal_slot(row: dict[str, Any], task_id: str, expected_error: str | None) -> bool:
        if expected_error is not None:
            return PlaceholderSlotRepository.mark_failed_terminal_for_rejection(
                row["id"], task_id, expected_error, row["fail_reason"] or ""
            )
        return PlaceholderSlotRepository.mark_failed_terminal(row["id"], row["fail_reason"] or "")

    async def reconcile_post_send(
        self,
        item: PublishItem,
        context: PublishContext,
        real_msg_id: int,
        *,
        fallback_slot: int | None = None,
    ) -> None:
        """单条发送成功后的占位槽对账 (统一收口 backfillable 与 group_summary)。

        - fallback_slot 非空: 常规回退路径直接推进为 published (保留原 ⏳ 卡)。
        - group_summary: 先校验共享同一 placeholder_msg_id 的全部成员状态，仅当全部成员
          均已恢复/完成时翻转 Telegram 汇总卡文案，再原子推进当前成员槽状态为 flipped。
        - backfillable: best-effort 翻转卡文案并推进为 published。
        """
        if fallback_slot is not None:
            PlaceholderSlotRepository.mark_published(fallback_slot, int(real_msg_id or 0))
            return
        existing_slot = PlaceholderSlotRepository.get_by_identity(
            context.target_chat_id, _slot_identity(item, context), int(item.size_bytes or 0)
        )
        if existing_slot is None or existing_slot.get("state") not in _SUMMARY_PENDING_STATES:
            return
        slot_id = int(existing_slot["id"])
        msg_id = int(existing_slot.get("placeholder_msg_id") or 0)
        sender_id = int(existing_slot.get("sender_account_id") or 0)
        if existing_slot.get("slot_kind") == "group_summary":
            if msg_id > 0:
                await self._flip_completed_summary_cards({msg_id: sender_id}, context, {slot_id})
            PlaceholderSlotRepository.mark_flipped(slot_id)
        else:
            if msg_id > 0:
                await self._best_effort_flip_card(msg_id, sender_id, context)
            PlaceholderSlotRepository.mark_published(slot_id, int(real_msg_id or 0))

    async def flip_after_group_send(
        self, members_with_msgs: list[tuple[PublishItem, int]], context: PublishContext
    ) -> int:
        """R2 (扩展): 组发成功后对命中槽成员 best-effort 翻转 ✅ (失败静默, J12)。
        members_with_msgs: List[(item, real_msg_id)]; backfillable → published,
        group_summary → flipped (仅当共享同一汇总卡的全部成员均已成功时才编辑文案)。"""
        if not members_with_msgs:
            return 0
        summary_rows, backfill_rows, backfill_edits, summary_edits = self._collect_group_flip_targets(
            members_with_msgs, context
        )

        for msg_id, sender_id in backfill_edits:
            await self._best_effort_flip_card(msg_id, sender_id, context)

        completed_summary_ids = {int(row["id"]) for row in summary_rows}
        await self._flip_completed_summary_cards(summary_edits, context, completed_summary_ids)

        flipped = 0
        for row in summary_rows:
            try:
                PlaceholderSlotRepository.mark_flipped(int(row["id"]))
                flipped += 1
            except Exception as slot_err:
                logger.warning("组发翻转汇总槽落库失败 (id=%s): %s", row.get("id"), slot_err)
        for row, real_msg_id in backfill_rows:
            try:
                PlaceholderSlotRepository.mark_published(int(row["id"]), int(real_msg_id or 0))
                flipped += 1
            except Exception as slot_err:
                logger.warning("组发翻转回填槽落库失败 (id=%s): %s", row.get("id"), slot_err)
        return flipped

    @staticmethod
    def _collect_group_flip_targets(
        members_with_msgs: list[tuple[PublishItem, int]], context: PublishContext
    ) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], int]], list[tuple[int, int]], dict[int, int]]:
        summary_rows: list[dict[str, Any]] = []
        backfill_rows: list[tuple[dict[str, Any], int]] = []
        backfill_edits: list[tuple[int, int]] = []
        summary_edits: dict[int, int] = {}
        seen_slot_ids: set[int] = set()
        for member, real_msg_id in members_with_msgs:
            try:
                identity = _slot_identity(member, context)
                row = PlaceholderSlotRepository.get_by_identity(
                    context.target_chat_id, identity, int(member.size_bytes or 0)
                )
                if row is None or row["state"] not in ("placeholder_sent", "failed_terminal"):
                    continue
                slot_id = int(row["id"])
                if slot_id in seen_slot_ids:
                    continue
                seen_slot_ids.add(slot_id)
                msg_id = int(row["placeholder_msg_id"] or 0)
                sender_id = int(row.get("sender_account_id") or 0)
                if row["slot_kind"] == "group_summary":
                    summary_rows.append(row)
                    if msg_id > 0:
                        summary_edits.setdefault(msg_id, sender_id)
                else:
                    backfill_rows.append((row, int(real_msg_id or 0)))
                    if msg_id > 0:
                        backfill_edits.append((msg_id, sender_id))
            except Exception as slot_err:
                logger.warning("组发翻转槽读取失败: %s", slot_err)
        return summary_rows, backfill_rows, backfill_edits, summary_edits

    @staticmethod
    def _summary_card_recovery_text(target_chat_id: Any, msg_id: int, newly_completed_slot_ids: set[int]) -> str | None:
        try:
            siblings = PlaceholderSlotRepository.list_by_placeholder_msg_id(target_chat_id, msg_id)
            pending = [
                s
                for s in siblings
                if int(s["id"]) not in newly_completed_slot_ids and s.get("state") in _SUMMARY_PENDING_STATES
            ]
            if not pending:
                return FLIPPED_TEXT
            if any(s.get("state") == "failed_terminal" for s in pending):
                return GROUP_PARTIAL_TERMINAL_TEXT
            return None
        except Exception as lookup_err:
            logger.warning("查询共享汇总卡成员状态失败 (跳过翻转编辑): %s", lookup_err)
            return None

    async def _flip_completed_summary_cards(
        self,
        summary_edits: dict[int, int],
        context: PublishContext,
        newly_completed_slot_ids: set[int],
    ) -> None:
        for msg_id, sender_id in summary_edits.items():
            text = self._summary_card_recovery_text(context.target_chat_id, msg_id, newly_completed_slot_ids)
            if text is None:
                continue
            await self._best_effort_flip_card(msg_id, sender_id, context, text=text)

    @staticmethod
    async def _best_effort_flip_card(
        msg_id: int, sender_account_id: int, context: PublishContext, *, text: str = FLIPPED_TEXT
    ) -> None:
        client = _resolve_slot_edit_client(sender_account_id, context)
        if client is None:
            return
        try:
            await client.edit_message(context.target_chat_id, msg_id, text=text)
        except Exception as flip_err:
            if _is_not_modified_error(flip_err):
                logger.debug("占位卡翻转文案已就位 (msg_id=%s)", msg_id)
            else:
                logger.warning("占位卡翻转失败 (best-effort): %s", flip_err)
