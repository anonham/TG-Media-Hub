import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from telethon import TelegramClient

from app.config import settings
from app.db.account_repository import SystemSettingsRepository
from app.db.task_repository import TaskQueueRepository

logger = logging.getLogger(__name__)


@dataclass
class TransitBridgeResult:
    transit_chat: Any
    forwarded_messages: list[Any]
    clients: list[TelegramClient]
    ready_bots_count: int
    source_to_forwarded: dict[int, Any]


class TransitBridgeService:
    """Phase 3: Telegram 静默中转群桥接与多 Bot 借壳加速服务"""

    @staticmethod
    def get_transit_settings() -> dict[str, Any]:
        """统一读取中转加速配置 (数据库动态配置优先，回退至 settings/环境变量)"""
        enabled = SystemSettingsRepository.get_bool(
            "enable_transit_acceleration", getattr(settings, "ENABLE_TRANSIT_ACCELERATION", True)
        )
        chat_id = SystemSettingsRepository.get("transit_chat_id", getattr(settings, "TRANSIT_CHAT_ID", ""))
        if chat_id is None:
            chat_id = getattr(settings, "TRANSIT_CHAT_ID", "")
        chat_id_str = str(chat_id).strip()

        min_size_mb = SystemSettingsRepository.get_int(
            "transit_min_file_size_mb", getattr(settings, "TRANSIT_MIN_FILE_SIZE_MB", 20)
        )
        return {
            "enabled": enabled,
            "transit_chat_id": chat_id_str,
            "min_file_size_mb": max(0, min_size_mb),
            "min_file_size_bytes": max(0, min_size_mb) * 1024 * 1024,
        }

    @classmethod
    def is_transit_capable(
        cls,
        primary_client: TelegramClient | None,
        target_chat: Any,
        messages: Any | list[Any],
        total_bytes: int | None = None,
    ) -> tuple[bool, str]:
        """
        启发式准入决策门控 (零阻塞评估是否满足梯队 1 准入红线):
        1. 系统开关已开启且中转群 ID 有效
        2. 辅助 Bot 池可用 (活跃就绪 Bot 数量 >= 1)
        3. 目标源频道与中转群不能相同
        4. 目标频道与消息未限制转发 (noforwards is False)
        5. 文件体积达到阈值 (>= TRANSIT_MIN_FILE_SIZE_MB)
        """
        conf = cls.get_transit_settings()
        if not conf["enabled"]:
            return False, "中转加速功能未启用 (ENABLE_TRANSIT_ACCELERATION=False)"

        transit_chat_id = conf["transit_chat_id"]
        if not transit_chat_id:
            return False, "中转群 ID 未配置 (TRANSIT_CHAT_ID 为空)"

        # 检查是否为同群防环路
        target_chat_str = str(target_chat).strip()
        if target_chat_str == transit_chat_id:
            return False, "目标频道与中转群相同，跳过中转"

        # 检查 Bot 池就绪状态
        from app.core.client_manager import client_manager

        ready_bots = [b for b in client_manager.get_all_bots() if b.is_ready() and b.client]
        if not ready_bots:
            return False, "Bot 池暂无就绪的辅助 Bot，中转加速无增益"

        msg_list = messages if isinstance(messages, list) else [messages]
        if not msg_list or not any(msg_list):
            return False, "消息列表为空"

        # 检查源频道或消息受限保护状态 (noforwards)
        if getattr(target_chat, "noforwards", False):
            return False, "目标频道开启了防转发保护 (noforwards=True)"

        for m in msg_list:
            if m and getattr(m, "noforwards", False):
                return False, f"消息 ID {getattr(m, 'id', '?')} 开启了防转发保护 (noforwards=True)"

        # 检查文件体积门槛
        min_bytes = conf["min_file_size_bytes"]
        if total_bytes is not None:
            max_size = total_bytes
        else:
            from app.services.downloader_service import DownloaderService

            sizes = []
            for m in msg_list:
                if m and getattr(m, "media", None):
                    _, sz = DownloaderService.get_file_name_and_size(m)
                    sizes.append(sz)
            max_size = max(sizes) if sizes else 0

        if max_size < min_bytes:
            return False, f"文件体积 ({max_size / (1024 * 1024):.2f}MB) 低于中转加速阈值 ({conf['min_file_size_mb']}MB)"

        # Telegram Bot 账号上限: 超大文件辅助 Bot 无权下载，直接降级至 Userbot
        if max_size > settings.MAX_FILE_SIZE_BYTES:
            return (
                False,
                f"文件体积 ({max_size / (1024 * 1024):.2f}MB) 超出 Telegram Bot 协议限制，降级为 Userbot 原生通道下载",
            )

        return True, "满足中转借壳加速条件"

    @staticmethod
    def _pair_forward_results_preserved(msg_list: list[Any], forwarded: Any) -> tuple[list[Any], dict[int, Any]]:
        """转发结果保位配对: Telethon 对部分无效 ID 在原位返回 None, 必须先按原始位置 zip
        对齐源消息, 再剔除失败位 —— 先过滤会整体左移, 后续成员全部错配到错误媒体。"""
        raw_list = forwarded if isinstance(forwarded, list) else [forwarded]
        source_to_forwarded: dict[int, Any] = {}
        for src_m, fwd_m in zip(msg_list, raw_list, strict=False):
            if fwd_m and getattr(fwd_m, "id", None):
                source_to_forwarded[src_m.id] = fwd_m
        if len(source_to_forwarded) != len(msg_list):
            logger.warning("转发消息数量与原消息不符 (原: %d, 成功: %d)", len(msg_list), len(source_to_forwarded))
        filtered = [m for m in raw_list if m and getattr(m, "id", None)]
        return filtered, source_to_forwarded

    @classmethod
    @asynccontextmanager
    async def transit_bridge_context(
        cls,
        primary_client: TelegramClient,
        source_chat: Any,
        messages: Any | list[Any],
        task_id: str | None = None,
    ):
        """
        中转桥接异步上下文管理器:
        1. 批量原子转发目标消息/Album 到中转群
        2. 组装 [Userbot] + [活跃 Bots] 满血客户端池
        3. 记录日志与抛出映射上下文
        4. finally 强保洁机制：彻底删除中转群临时消息，杜绝孤儿消息残留
        """
        conf = cls.get_transit_settings()
        transit_chat_id = conf["transit_chat_id"]
        from app.core.client_manager import client_manager
        from app.services.forward_service import ForwardService

        parsed_transit = ForwardService.parse_chat_identifier(transit_chat_id)
        parsed_source = ForwardService.parse_chat_identifier(source_chat)

        msg_list = messages if isinstance(messages, list) else [messages]
        msg_list = [m for m in msg_list if m and getattr(m, "id", None)]
        if not msg_list:
            yield None
            return

        transit_chat_entity = None
        forwarded_list: list[Any] = []
        try:
            # 1. 解析中转群实体
            transit_chat_entity = await ForwardService.resolve_entity(primary_client, parsed_transit)
            if not transit_chat_entity:
                raise RuntimeError(f"无法解析中转群实体: {transit_chat_id}")

            # 2. 原子批量转发 (保留 Album Grouped 关联)
            msg_ids = [m.id for m in msg_list]
            forward_target = msg_ids if len(msg_ids) > 1 else msg_ids[0]

            logger.info("正在将消息 %s 从频道 %s 转发至中转群 %s...", msg_ids, source_chat, transit_chat_id)
            if task_id:
                await TaskQueueRepository.append_log_async(
                    task_id, f"🔄 [中转桥接] 正在将 {len(msg_ids)} 条消息原子转发至中转缓存群..."
                )

            forwarded = await primary_client.forward_messages(
                transit_chat_entity, forward_target, from_peer=parsed_source
            )

            forwarded_list, source_to_forwarded = cls._pair_forward_results_preserved(msg_list, forwarded)

            # 3. 组装 [Userbot] + [活跃 Bots] 借壳加速客户端池
            ready_bots = [b.client for b in client_manager.get_all_bots() if b.is_ready() and b.client]
            transit_clients = [primary_client] + ready_bots

            logger.info(
                "中转桥接就绪: 消息已转发至中转群 (临时 ID: %s), 激活 1 Userbot + %d Bots 联合下载",
                [m.id for m in forwarded_list],
                len(ready_bots),
            )

            bridge_result = TransitBridgeResult(
                transit_chat=transit_chat_entity,
                forwarded_messages=forwarded_list,
                clients=transit_clients,
                ready_bots_count=len(ready_bots),
                source_to_forwarded=source_to_forwarded,
            )

        except Exception as fwd_err:
            err_name = type(fwd_err).__name__
            err_str = str(fwd_err)
            if (
                "ChatForwardsRestricted" in err_name
                or "ChatSendMediaForbidden" in err_name
                or "CHAT_FORWARDS_RESTRICTED" in err_str
                or "FORWARDS_RESTRICTED" in err_str
            ):
                warn_msg = f"⚠️ [中转桥接受限] 目标频道限制内容转发 ({err_name})，静默降级至直连下载"
                logger.warning(warn_msg)
                if task_id:
                    await TaskQueueRepository.append_log_async(task_id, warn_msg)
            else:
                warn_msg = f"⚠️ [中转桥接异常] 转发至中转群失败 ({err_name}: {fwd_err})，自动降级至直连下载"
                logger.warning(warn_msg)
                if task_id:
                    await TaskQueueRepository.append_log_async(task_id, warn_msg)

            # 降级: 向调用者返回 None，通知外层进入 Phase 2/1 直连
            yield None
            return

        # 4. 强制生命周期保洁 (Guaranteed Cleanup)
        # 无论下载成功、中途报错、用户主动暂停/取消，必须在 finally 中清理中转群临时消息
        try:
            yield bridge_result
        finally:
            if forwarded_list and transit_chat_entity:
                del_ids = [m.id for m in forwarded_list if m and getattr(m, "id", None)]
                if del_ids:
                    try:
                        await primary_client.delete_messages(transit_chat_entity, del_ids)
                        logger.info("🧹 中转群临时消息强保洁已完成，已彻底删除消息 ID: %s", del_ids)
                        if task_id:
                            await TaskQueueRepository.append_log_async(
                                task_id, f"🧹 [中转群强保洁] 已彻底删除 {len(del_ids)} 条临时缓存消息 (零垃圾残留)"
                            )
                    except Exception as clean_err:
                        logger.warning("清理中转群临时消息失败 (非致命): %s", clean_err)

    @classmethod
    async def verify_transit_group_health(cls, primary_client: TelegramClient | None = None) -> dict[str, Any]:
        """
        中转群治理与健康度检查探针:
        1. 检查中转群 ID 配置是否有效
        2. 检查主 Userbot 对中转群的发信与删信权限
        3. 检查系统各辅助 Bot 是否均已在群内且可读取消息
        """
        from app.core.client_manager import client_manager
        from app.services.forward_service import ForwardService

        conf = cls.get_transit_settings()
        transit_chat_id = conf["transit_chat_id"]
        if not transit_chat_id:
            return {
                "status": "unconfigured",
                "message": "中转群 ID (TRANSIT_CHAT_ID) 未配置",
                "transit_chat_id": "",
                "userbot_accessible": False,
                "bots_status": [],
                "ready_bots_count": 0,
                "total_bots_count": len(client_manager.get_all_bots()),
                "issues": ["未配置 TRANSIT_CHAT_ID，中转加速将保持关闭"],
            }

        client = primary_client
        if not client or not client.is_connected():
            client = await client_manager.get_client_for_task(mode="userbot") or client_manager.userbot

        if not client or not client.is_connected():
            return {
                "status": "error",
                "message": "主 Userbot 客户端未连接，无法执行中转群健康检查",
                "transit_chat_id": transit_chat_id,
                "userbot_accessible": False,
                "bots_status": [],
                "ready_bots_count": 0,
                "total_bots_count": len(client_manager.get_all_bots()),
                "issues": ["主 Userbot 离线或未授权"],
            }

        parsed_transit = ForwardService.parse_chat_identifier(transit_chat_id)
        issues = []
        userbot_accessible = False
        chat_title = str(transit_chat_id)

        # 1. 探测 Userbot 是否具备中转群访问权限与发布/删信保洁权限
        entity = None
        userbot_can_post = True
        userbot_can_delete = True
        try:
            entity = await asyncio.wait_for(ForwardService.resolve_entity(client, parsed_transit), timeout=6.0)
            if entity:
                userbot_accessible = True
                chat_title = getattr(entity, "title", getattr(entity, "first_name", str(transit_chat_id)))
                # 进一步检查主 Userbot 在中转群的发信与删信权限
                try:
                    ub_perms = await asyncio.wait_for(client.get_permissions(entity, "me"), timeout=4.0)
                    if ub_perms:
                        # 频道: 精确 post_messages 发帖权; 群组: 管理员天然可发言, 普通成员按
                        # 群禁言/媒体禁发判定 —— is_admin 或创造者身份绝不等价于可发帖
                        if getattr(ub_perms, "is_creator", False):
                            userbot_can_post = True
                        elif getattr(entity, "broadcast", False):
                            userbot_can_post = bool(getattr(ub_perms, "post_messages", False))
                        elif getattr(ub_perms, "is_admin", False):
                            userbot_can_post = True
                        elif getattr(ub_perms, "is_banned", False) or not getattr(
                            ub_perms, "has_default_permissions", False
                        ):
                            userbot_can_post = False
                        else:
                            default_banned = getattr(entity, "default_banned_rights", None)
                            userbot_can_post = not (
                                default_banned is not None
                                and (
                                    getattr(default_banned, "send_messages", False)
                                    or getattr(default_banned, "send_media", False)
                                )
                            )
                        userbot_can_delete = bool(
                            getattr(ub_perms, "delete_messages", True)
                            or getattr(ub_perms, "is_admin", False)
                            or getattr(ub_perms, "is_creator", False)
                        )
                        if not userbot_can_post:
                            issues.append("⚠️ 主 Userbot 在中转群缺少发信/发布权限，无法执行桥接转发")
                        if not userbot_can_delete:
                            issues.append("⚠️ 主 Userbot 在中转群缺少删除消息权限，无法执行强保洁清理")
                except Exception as perm_err:
                    logger.debug("获取主 Userbot 权限异常 (非致命): %s", perm_err)
        except Exception as e:
            issues.append(f"主 Userbot 无法访问中转群: {e}")

        # 2. 探测各活跃 Bot 的可达性与群驻留状态
        all_bots = client_manager.get_all_bots()
        bots_status = []
        accessible_bot_count = 0

        for b in all_bots:
            b_info = {
                "bot_id": b.bot_id,
                "bot_name": b.bot_name,
                "session_name": b.session_name,
                "bot_username": b.bot_username,
                "is_ready": b.is_ready(),
                "in_transit_group": False,
                "is_admin": False,
                "error": None,
            }
            if not b.is_ready() or not b.client:
                b_info["error"] = "Bot 处于未就绪或熔断状态"
                bots_status.append(b_info)
                continue

            try:
                # 优先传入 Userbot 已成功解析出的实体作为 fallback_peer，共享私密频道的 access_hash
                b_entity = await asyncio.wait_for(
                    ForwardService.resolve_entity(
                        b.client, parsed_transit, fallback_peer=entity if userbot_accessible else None
                    ),
                    timeout=4.0,
                )
                if b_entity:
                    # 关键修改: 严禁调用 get_messages(b_entity, limit=1)！
                    # MTProto 协议严格禁止 Bot 在频道中发起 GetHistoryRequest，且清空历史的空频道更无消息。
                    # 正确做法: 调用 get_permissions('me')，直接向 Telegram 校验 Bot 是否在群及管理员权限
                    perms = await asyncio.wait_for(b.client.get_permissions(b_entity, "me"), timeout=4.0)
                    b_info["in_transit_group"] = True
                    b_info["is_admin"] = getattr(perms, "is_admin", False)
                    accessible_bot_count += 1
                else:
                    b_info["error"] = "未能解析中转群实体"
            except Exception as b_err:
                err_str = str(b_err)
                err_name = type(b_err).__name__
                if "UserNotParticipant" in err_name or "USER_NOT_PARTICIPANT" in err_str:
                    b_info["error"] = "Bot 尚未加入中转群"
                    issues.append(f"Bot [{b.session_name}] 尚未加入中转群，请将其拉入群并设为管理员")
                elif "ChannelPrivate" in err_name or "ChatAdminRequired" in err_name or "CHANNEL_PRIVATE" in err_str:
                    b_info["error"] = "Bot 无权访问该私有中转群 (需管理员身份)"
                    issues.append(f"Bot [{b.session_name}] 无法访问该私有中转群，请确认已将其添加为管理员")
                else:
                    b_info["error"] = str(b_err)
                    issues.append(f"Bot [{b.session_name}] 访问中转群失败: {b_err}")

            bots_status.append(b_info)

        # 3. 综合评定健康度 (必备能力失败也必须降级: 全 Bot 在群但 Userbot 不可发帖 ≠ healthy)
        if not userbot_accessible:
            overall_status = "error"
            message = "主 Userbot 无法访问中转群，请检查群 ID 或拉入 Userbot"
        elif not (userbot_can_post and userbot_can_delete):
            overall_status = "degraded"
            missing = "发信/发布" if not userbot_can_post else "删除消息"
            if not userbot_can_post and not userbot_can_delete:
                missing = "发信/发布与删除消息"
            message = f"主 Userbot 在中转群缺少{missing}权限，桥接转发/保洁无法执行，请授予管理员权限"
        elif accessible_bot_count == 0:
            # 空 Bot 池与「有配置但零在群」同判 degraded: is_transit_capable 对空池同样拒绝,
            # 探针若报 healthy 会与准入判定自相矛盾 (verify_transit.py 据此误报加速就绪)
            overall_status = "degraded"
            message = (
                "主 Userbot 正常，但 Bot 池为空未配置任何辅助 Bot，中转加速无法生效"
                if not all_bots
                else "主 Userbot 正常，但没有辅助 Bot 在中转群内，中转加速无法生效"
            )
            if not all_bots:
                # verify_transit.py 以 issues 为空判定整体健康 —— 空池必须留下可操作条目
                issues.append("Bot 池为空: 未配置任何辅助 Bot，请先在 Bot 管理页添加并启动 Bot")
        elif accessible_bot_count < len(all_bots):
            overall_status = "partially_healthy"
            message = f"中转群部分就绪: {accessible_bot_count}/{len(all_bots)} 个 Bot 在群内"
        else:
            overall_status = "healthy"
            message = f"中转群完全就绪: 主 Userbot 与全部 {accessible_bot_count} 个 Bot 均已在群"

        return {
            "status": overall_status,
            "message": message,
            "transit_chat_id": transit_chat_id,
            "transit_chat_title": chat_title,
            "userbot_accessible": userbot_accessible,
            "bots_status": bots_status,
            "ready_bots_count": accessible_bot_count,
            "total_bots_count": len(all_bots),
            "issues": issues,
        }
