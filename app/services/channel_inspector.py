import asyncio
import logging
from typing import Any

from telethon.tl.types import Chat, ChatAdminRights, ChatBannedRights, User

from app.core.client_manager import client_manager
from app.services.forward_service import ForwardService


class ChannelInspector:
    """Telegram 频道/群组探针与读写权限诊断服务"""

    @classmethod
    async def inspect_channel(
        cls, channel_identifier: str, preset_type: str = "target", account_id: int | None = None
    ) -> dict[str, Any]:
        """
        全面检测频道有效性、解析标准 ID 及读/写权限：
        1. 解析链接/ID/Username
        2. 获取 Entity 元数据 (Title, Username, 类型)
        3. 测试读权限 (能否检索消息)
        4. 测试写权限 (管理员权限 / 发言与发媒体权限)
        """
        parsed_chat = ForwardService.parse_chat_identifier(channel_identifier)

        userbot = None
        used_account_id = None
        used_account_name = None
        entity = None

        if account_id is not None:
            acc = client_manager.get_account(account_id)
            if not acc:
                return {
                    "channel_identifier": str(channel_identifier),
                    "parsed_chat_id": str(parsed_chat),
                    "channel_title": str(channel_identifier),
                    "channel_username": None,
                    "channel_type": "Unknown",
                    "is_accessible": False,
                    "is_restricted": False,
                    "has_read_permission": False,
                    "has_write_permission": False,
                    "recommended_name": str(channel_identifier),
                    "permission_summary": f"❌ 未找到 ID 为 {account_id} 的 Userbot 账号",
                    "error_detail": f"Account with ID {account_id} not found",
                    "account_id": account_id,
                    "account_name": f"Account #{account_id}",
                }
            if not acc.client or not acc.client.is_connected():
                try:
                    await acc.connect()
                except Exception as conn_err:
                    return {
                        "channel_identifier": str(channel_identifier),
                        "parsed_chat_id": str(parsed_chat),
                        "channel_title": str(channel_identifier),
                        "channel_username": None,
                        "channel_type": "Unknown",
                        "is_accessible": False,
                        "is_restricted": False,
                        "has_read_permission": False,
                        "has_write_permission": False,
                        "recommended_name": str(channel_identifier),
                        "permission_summary": f"❌ 指定账号 [{acc.account_name}] 连接失败: {conn_err}",
                        "error_detail": str(conn_err),
                        "account_id": acc.account_id,
                        "account_name": acc.account_name,
                    }
            if not (acc.is_ready() and acc.client and acc.client.is_connected()):
                return {
                    "channel_identifier": str(channel_identifier),
                    "parsed_chat_id": str(parsed_chat),
                    "channel_title": str(channel_identifier),
                    "channel_username": None,
                    "channel_type": "Unknown",
                    "is_accessible": False,
                    "is_restricted": False,
                    "has_read_permission": False,
                    "has_write_permission": False,
                    "recommended_name": str(channel_identifier),
                    "permission_summary": f"❌ 指定账号 [{acc.account_name}] 尚未完成登录鉴权 (状态: {acc.auth_state})",
                    "error_detail": f"Account {acc.account_name} is not ready (auth_state: {acc.auth_state})",
                    "account_id": acc.account_id,
                    "account_name": acc.account_name,
                }
            userbot = acc.client
            used_account_id = acc.account_id
            used_account_name = acc.account_name
        else:
            # 自动调度模式: 使用 probe_chat_access 并发探针自动探测所有活跃就绪账号
            probed_acc, probed_entity = await client_manager.probe_chat_access(parsed_chat, account_id=None)
            if probed_acc:
                userbot = probed_acc.client
                used_account_id = probed_acc.account_id
                used_account_name = probed_acc.account_name
                entity = probed_entity
            else:
                acc = client_manager.get_account()
                if acc:
                    if not acc.client or not acc.client.is_connected():
                        try:
                            await acc.connect()
                        except Exception as conn_err:
                            logging.debug("默认账号 [%s] 连接失败: %s", acc.account_name, conn_err)
                    if acc.is_ready() and acc.client and acc.client.is_connected():
                        userbot = acc.client
                        used_account_id = acc.account_id
                        used_account_name = acc.account_name
                if not userbot or not userbot.is_connected():
                    userbot = client_manager.userbot
                    if not userbot or not userbot.is_connected():
                        await client_manager.initialize()
                        userbot = client_manager.userbot
                    if userbot:
                        used_account_name = "全局默认 Userbot"

        if not userbot:
            raise RuntimeError("Telegram 客户端未初始化，请先完成账号登录绑定")

        # 1. 解析 Entity (若尚未通过探针解析，则安全解析；本地 Session 缺少 access_hash 时自动刷新)
        if entity is None:
            try:
                entity = await asyncio.wait_for(ForwardService.resolve_entity(userbot, parsed_chat), timeout=15.0)
            except Exception as e:
                return {
                    "channel_identifier": str(channel_identifier),
                    "parsed_chat_id": str(parsed_chat),
                    "channel_title": str(channel_identifier),
                    "channel_username": None,
                    "channel_type": "Unknown",
                    "is_accessible": False,
                    "is_restricted": False,
                    "has_read_permission": False,
                    "has_write_permission": False,
                    "recommended_name": str(channel_identifier),
                    "permission_summary": f"❌ 无法解析或访问该频道/群组: {e}",
                    "error_detail": str(e),
                    "account_id": used_account_id,
                    "account_name": used_account_name,
                }

        title = getattr(entity, "title", getattr(entity, "first_name", str(channel_identifier)))
        username = getattr(entity, "username", None)
        is_restricted = getattr(entity, "noforwards", False)

        channel_type = "Channel"
        is_broadcast = getattr(entity, "broadcast", False)
        is_megagroup = getattr(entity, "megagroup", False)

        if is_megagroup:
            channel_type = "Supergroup"
        elif is_broadcast:
            channel_type = "Channel"
        elif isinstance(entity, Chat):
            channel_type = "Group"
        elif isinstance(entity, User):
            channel_type = "PrivateChat"

        # 2. 读权限测试 (Read Test)
        has_read = False
        read_error = None
        try:
            await asyncio.wait_for(userbot.get_messages(entity, limit=1), timeout=8.0)
            has_read = True
        except Exception as e:
            read_error = str(e)

        # 3. 写权限测试 (Write / Post Test)
        has_write = False
        write_reason = ""
        admin_rights: ChatAdminRights | None = None

        # 如果是频道 (Broadcast Channel)，必须拥有 post_messages (change_info 仅可改简介, 不含发帖)
        if is_broadcast:
            admin_rights = getattr(entity, "admin_rights", None)
            is_creator = getattr(entity, "creator", False)
            if is_creator:
                has_write = True
                write_reason = "拥有者最高发布权限"
            elif admin_rights and admin_rights.post_messages:
                has_write = True
                write_reason = "管理员频道发布权限"
            else:
                has_write = False
                write_reason = "非频道管理员，无发帖权限"
        elif is_megagroup or isinstance(entity, Chat):
            # 群组：管理员不适用全员禁言; 普通成员按被封禁权利判定 (含媒体细分封禁)
            default_banned: ChatBannedRights | None = getattr(entity, "default_banned_rights", None)
            banned: ChatBannedRights | None = getattr(entity, "banned_rights", None)
            admin_rights = getattr(entity, "admin_rights", None)
            is_creator = getattr(entity, "creator", False)

            # post_messages 是广播频道专属权, Telethon 配置超级群管理员时会移除该位 ——
            # 群组管理员判定只看是否持有任一管理权
            has_any_admin_right = admin_rights is not None and any(
                value is True for value in vars(admin_rights).values()
            )

            if is_creator or has_any_admin_right:
                has_write = True
                write_reason = "群组管理权限"
            elif banned and (
                banned.send_messages
                or banned.send_media
                or getattr(banned, "send_videos", False)
                or getattr(banned, "send_photos", False)
                or getattr(banned, "send_audios", False)
                or getattr(banned, "send_docs", False)
            ):
                has_write = False
                write_reason = "被限制在该群组发送消息或媒体"
            elif default_banned and (
                default_banned.send_messages
                or default_banned.send_media
                or getattr(default_banned, "send_videos", False)
                or getattr(default_banned, "send_photos", False)
                or getattr(default_banned, "send_audios", False)
                or getattr(default_banned, "send_docs", False)
            ):
                has_write = False
                write_reason = "群组全员禁言或禁止发送媒体"
            else:
                has_write = True
                write_reason = "群成员正常发言权限"
        else:
            # 个人私聊或通用对话
            has_write = True
            write_reason = "私聊/收藏夹发送权限"

        # 生成权限摘要信息
        summary_parts = []
        if has_read:
            summary_parts.append("✅ 可读取")
        else:
            summary_parts.append(f"❌ 读取受限 ({read_error or '无权访问'})")

        if has_write:
            summary_parts.append(f"✅ 可写入 ({write_reason})")
        else:
            summary_parts.append(f"⚠️ 无法写入 ({write_reason})")

        if is_restricted:
            summary_parts.append("🛡️ 受限内容频道")

        recommended_name = f"{title}"

        return {
            "channel_identifier": str(channel_identifier),
            "parsed_chat_id": str(parsed_chat),
            "channel_title": title,
            "channel_username": username,
            "channel_type": channel_type,
            "is_accessible": True,
            "is_restricted": is_restricted,
            "has_read_permission": has_read,
            "has_write_permission": has_write,
            "recommended_name": recommended_name,
            "permission_summary": " | ".join(summary_parts),
            "error_detail": read_error if not has_read else None,
            "account_id": used_account_id,
            "account_name": used_account_name,
        }
