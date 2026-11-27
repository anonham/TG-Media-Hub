"""Telegram Entity 解析与权限探测基础设施 (Entity Resolver)

下沉纯函数与基础设施能力，消除 core -> services 反向依赖:
- parse_chat_identifier: 解析频道/群组/私密链接为标识符 (纯函数)
- is_private_channel_identifier: 判定频道标识是否为私有频道特征 (纯函数)
- resolve_entity: 安全解析 Telegram Entity，带私密群组 access_hash 刷新回退
- probe_write_permission: 轻量探测客户端对目标频道的发帖/写权限
"""

import inspect
import logging
import re
from collections.abc import Iterator
from typing import Any, overload

from telethon import TelegramClient
from telethon.tl.types import Channel, Chat, ChatAdminRights, ChatBannedRights

logger = logging.getLogger(__name__)

_PRIVATE_MESSAGE_LINK = re.compile(r"t\.me/c/(\d+)(?:/\d+)?/(\d+)(?=$|[/?#])")
_PUBLIC_MESSAGE_LINK = re.compile(r"t\.me/(?:s/)?([^/]+)(?:/\d+)?/(\d+)(?=$|[/?#])")


def iter_message_link_targets(link: str) -> Iterator[tuple[str | int, int]]:
    """提取消息链接的频道与消息 ID，话题链接取末段，优先尝试私有频道形式。"""
    if match := _PRIVATE_MESSAGE_LINK.search(link):
        yield int(f"-100{match[1]}"), int(match[2])
        return
    if match := _PUBLIC_MESSAGE_LINK.search(link):
        yield match[1], int(match[2])


@overload
def parse_chat_identifier(chat_str: str | int) -> str | int: ...


@overload
def parse_chat_identifier(chat_str: Any) -> Any: ...


def parse_chat_identifier(chat_str: Any) -> Any:
    """解析群组/频道标识符，支持数字 ID、Username 或 Telegram 链接；已解析的 Entity/Peer 原样透传"""
    if isinstance(chat_str, int):
        if chat_str > 0 and 10**8 <= chat_str < 10**12:
            return int(f"-100{chat_str}")
        return chat_str

    if chat_str is None:
        return ""

    if not isinstance(chat_str, str):
        return chat_str

    if not chat_str:
        return ""

    chat_str = str(chat_str).strip()

    # 1. 邀请链接 (t.me/+... 或 t.me/joinchat/...): 提取 +<hash> 统一邀请格式
    invite_match = re.search(r"t\.me/(?:\+|joinchat/)([^/?#]+)", chat_str)
    if invite_match:
        return f"+{invite_match.group(1)}"

    # 2. 私密频道链接: https://t.me/c/[频道ID]/[消息ID] -> -100XXXXXXXXXX
    private_match = re.search(r"t\.me/c/(\d+)", chat_str)
    if private_match:
        return int(f"-100{private_match.group(1)}")

    # 3. 公开频道/群组链接 (跳过 /s/ 网页预览前缀, 截断 query 与 fragment):
    # 例如 https://t.me/s/demo_channel/17354?start=1 -> demo_channel
    public_match = re.search(r"t\.me/(?:s/)?([^/?#]+)", chat_str)
    if public_match:
        return public_match.group(1)

    # 3. 9~12 位正整数 -> 自愈为 Telegram 频道格式 -100XXXXXXXXXX
    if re.match(r"^\d{9,12}$", chat_str):
        return int(f"-100{chat_str}")

    # 4. 数字字符串 ID (如 -100XXXXXXXXXX 或 普通负数)
    if re.match(r"^-?\d+$", chat_str):
        return int(chat_str)

    # 5. Username (包含或不包含 @)
    return chat_str


def normalize_peer_alias_key(chat_identifier: Any) -> str:
    """将非数字频道标识规范化为别名查询键 (去掉 @ 前缀; 用户名小写, +邀请哈希保留大小写).

    纯数字 ID / 空值返回空串 (无需经别名表二次映射).
    """
    parsed = parse_chat_identifier(chat_identifier)
    if isinstance(parsed, int):
        return ""
    s = str(parsed or "").strip()
    if not s or re.fullmatch(r"-?\d+", s):
        return ""
    if s.startswith("@"):
        s = s[1:]
    if not s:
        return ""
    if s.startswith("+") and not s[1:].isdigit():
        return s
    return s.lower()


def normalize_download_chat_key(chat: str | int) -> str:
    """统一下载目标键与历史记录的频道标识。"""
    parsed = parse_chat_identifier(chat)
    if isinstance(parsed, int):
        return str(parsed)
    return normalize_peer_alias_key(parsed) or parsed.strip()


def extract_entity_aliases(entity: Any, *raw_inputs: Any) -> list[str]:
    """从原始输入与已解析 Telethon Entity 提取去重后的非数字别名键列表."""
    aliases: list[str] = []
    seen: set[str] = set()

    def _add(raw: Any) -> None:
        key = normalize_peer_alias_key(raw)
        if key and key not in seen:
            seen.add(key)
            aliases.append(key)

    for raw in raw_inputs:
        if raw is not None:
            _add(raw)

    if entity is not None:
        username = getattr(entity, "username", None)
        if isinstance(username, str) and username.strip():
            _add(username)
        extra_usernames = getattr(entity, "usernames", None)
        if isinstance(extra_usernames, (list, tuple)):
            for item in extra_usernames:
                if getattr(item, "active", True):
                    u_name = getattr(item, "username", None)
                    if isinstance(u_name, str) and u_name.strip():
                        _add(u_name)
    return aliases


def resolve_canonical_chat_key(entity: Any, fallback: Any) -> str:
    """优先使用 Telethon get_peer_id(entity) 提取规范 peer ID 串; stub/mock 不支持时回退 parse_chat_identifier."""
    if entity is not None:
        try:
            from telethon.utils import get_peer_id

            return str(get_peer_id(entity))
        except (TypeError, ValueError) as err:
            logger.debug("resolve_canonical_chat_key 回退 parse_chat_identifier: %s", err)
    return str(parse_chat_identifier(fallback))


def is_private_channel_identifier(chat_str: str | int) -> bool:
    """检查频道标识是否为私有频道特征"""
    if not chat_str:
        return False
    if isinstance(chat_str, int):
        return chat_str < 0 or (10**8 <= chat_str < 10**12) or str(chat_str).startswith("-100")
    s = str(chat_str).strip()
    if "t.me/c/" in s:
        return True
    if re.match(r"^-100\d+$", s) or (s.startswith("-") and s[1:].isdigit()):
        return True
    if re.match(r"^\d{9,12}$", s):
        return True
    return False


async def resolve_entity(client: TelegramClient, chat_identifier: Any, fallback_peer: Any = None) -> Any:
    """安全解析 Telegram Entity，如遇到未缓存私密群组自动拉取 Dialogs 刷新 access_hash 缓存"""
    parsed = parse_chat_identifier(chat_identifier)
    # 规范化后的邀请码 (+<hash>) 需还原完整 t.me/+<hash> 才能被 Telethon parse_username 识别为邀请链接
    entity_target: Any = (
        f"https://t.me/{parsed}"
        if isinstance(parsed, str) and parsed.startswith("+") and not parsed[1:].isdigit()
        else parsed
    )
    try:
        return await client.get_entity(entity_target)
    except Exception:
        # 1. 若提供了已成功解析的 fallback_peer (例如 Userbot 共享的包含 access_hash 的 Entity/InputPeer)，优先尝试
        if fallback_peer is not None:
            try:
                return await client.get_entity(fallback_peer)
            except Exception as fb_exc:
                logger.warning("通过备用 Peer 解析实体失败 (%s): %s", fallback_peer, fb_exc)

        # 2. 自动拉取会话列表 (get_dialogs)，把账号所在的所有私密群/频道的 access_hash 刷入本地 Session 缓存
        # 注意: Bot 账号严禁调用 get_dialogs，否则 Telegram 会报 BotMethodInvalidError
        try:
            is_bot = False
            try:
                if hasattr(client, "is_bot"):
                    bot_fn = client.is_bot
                    if callable(bot_fn):
                        res = bot_fn()
                        if inspect.isawaitable(res):
                            is_bot = bool(await res)
                        else:
                            is_bot = bool(res)
                    else:
                        is_bot = bool(bot_fn)
            except Exception as is_bot_err:
                logger.debug("检测 client.is_bot 异常: %s", is_bot_err)
            if not is_bot and hasattr(client, "get_dialogs"):
                await client.get_dialogs(limit=50)
                return await client.get_entity(entity_target)
        except Exception as refresh_err:
            logger.warning("通过 get_dialogs 刷新缓存重新解析实体仍失败 (%s): %s", parsed, refresh_err)
        # 若二次重试仍无法解析，抛出原异常
        raise


async def probe_write_permission(client: TelegramClient, chat_identifier: Any) -> bool:
    """轻量探测客户端对目标频道的发帖/写权限"""
    if isinstance(chat_identifier, (Channel, Chat)):
        entity = chat_identifier
    else:
        try:
            entity = await resolve_entity(client, chat_identifier)
        except Exception as ex:
            logger.debug("probe_write_permission 解析 entity 失败 (%s): %s", chat_identifier, ex)
            return False

    is_broadcast = getattr(entity, "broadcast", False)
    is_megagroup = getattr(entity, "megagroup", False)
    admin_rights: ChatAdminRights | None = None

    if is_broadcast:
        admin_rights = getattr(entity, "admin_rights", None)
        is_creator = getattr(entity, "creator", False)
        if is_creator:
            return True
        if admin_rights and getattr(admin_rights, "post_messages", False):
            return True
        return False
    if is_megagroup or isinstance(entity, Chat):
        default_banned: ChatBannedRights | None = getattr(entity, "default_banned_rights", None)
        banned: ChatBannedRights | None = getattr(entity, "banned_rights", None)
        admin_rights = getattr(entity, "admin_rights", None)
        is_creator = getattr(entity, "creator", False)

        # 个人受限优先检查
        if banned and (getattr(banned, "send_messages", False) or getattr(banned, "send_media", False)):
            return False
        # 创建者与群管理员享有发布权限且不受 default_banned_rights 全员禁言约束
        if is_creator or admin_rights:
            return True
        # 普通成员受全员禁言/禁止发媒体约束
        if default_banned and (
            getattr(default_banned, "send_messages", False) or getattr(default_banned, "send_media", False)
        ):
            return False
        return True
    # 个人私聊或通用对话
    return True
