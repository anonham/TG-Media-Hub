import asyncio
import logging
import os
import re
import shutil
import stat
import time
from collections.abc import Callable
from typing import Any

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto
from telethon.utils import get_input_location

from app.config import settings
from app.core.entity_resolver import iter_message_link_targets, normalize_download_chat_key
from app.core.failure_logger import FailureLogger
from app.core.fast_telethon import download_file_parallel
from app.core.hash_utils import sha256_of_file
from app.core.session_errors import is_session_invalid_error
from app.db.download_repository import DownloadBreakpointRepository, DownloadHistoryRepository
from app.db.task_repository import TaskQueueRepository

logger = logging.getLogger(__name__)

_INVALID_FILENAME_CHARS = re.compile(r'[\\/:*?"<>|\x00-\x1f]')
MAX_INSPECT_RANGE_MESSAGES = 5000


def _reraise_task_level_resolution_error(e: Exception) -> None:
    """会话失效/FloodWait 必须上抛: 外层任务策略依赖它们做退避与重绑, 吞掉会被误判为链接失效"""
    if isinstance(e, FloodWaitError) or is_session_invalid_error(e):
        raise e


class AlbumDiscoveryError(RuntimeError):
    """关联 Album (grouped_id) 抓取失败或结果为空，禁止静默退化为单成员下载"""


def sanitize_filename(file_name: str) -> str:
    """清理来自 Telegram 的文件名: 防止路径穿越并移除 Windows 非法字符"""
    safe = os.path.basename(file_name.replace("\\", "/"))
    safe = _INVALID_FILENAME_CHARS.sub("_", safe).strip(" .")
    return safe or "download_file"


def _register_active_download_paths_sync(paths: tuple[str, ...]) -> tuple[str, ...]:
    from app.core.runtime import active_download_paths
    from app.services.storage_service import _active_path_key

    # os.replace 会替换成品目录项，本次登记需覆盖替换前后的真实路径。
    keys = tuple(dict.fromkeys((_active_path_key(paths[0], follow_leaf=False), *map(_active_path_key, paths))))
    for key in keys:
        active_download_paths.add(key)
    return keys


def _unregister_active_download_paths_sync(keys: tuple[str, ...]) -> None:
    from app.core.runtime import active_download_paths

    for key in keys:
        active_download_paths.release(key)


def _count_downloaded_media(chat_keys: tuple[str, ...], message_ids: list[int]) -> tuple[int, int]:
    valid = eligible = 0
    for msg_id in message_ids:
        has_file = False
        for chat in chat_keys:
            if chat and (record := DownloadHistoryRepository.get_valid_download_record(chat, msg_id)):
                has_file = True
                if record["album_completed"]:
                    eligible += 1
                    break
        valid += has_file
    return valid, eligible


def _verify_final_file_nonempty_sync(final_path: str | None) -> bool:
    if not final_path:
        return False
    try:
        info = os.stat(final_path)
    except OSError as exc:
        logger.debug("无法检查下载文件 %s: %s", final_path, exc)
        return False
    return stat.S_ISREG(info.st_mode) and info.st_size > 0


class DownloaderService:
    @staticmethod
    async def resolve_message_from_link(client: TelegramClient, link: str) -> tuple[str | int | None, Any | None]:
        """解析 Telegram 链接（支持公开频道、私有频道与 Telethon 自动机制）"""
        link = link.strip()
        if not link:
            return None, None

        for chat_id, msg_id in iter_message_link_targets(link):
            try:
                msg = await asyncio.wait_for(client.get_messages(chat_id, ids=msg_id), timeout=8.0)
                if msg:
                    return chat_id, msg
            except Exception as e:
                _reraise_task_level_resolution_error(e)
                logger.debug("Chat 链接解析失败: %s", e)

        # 后备方案：由 Telethon 自动原生解析
        try:
            res = await asyncio.wait_for(client.get_messages(link), timeout=8.0)
            if isinstance(res, list) and len(res) > 0:
                return link, res[0]
            if res:
                return link, res
        except Exception as e:
            _reraise_task_level_resolution_error(e)
            logger.warning(f"⚠️ 原生解析失败 {link}: {e}")

        return None, None

    @staticmethod
    async def get_album_messages(client: TelegramClient, chat_id: Any, message: Any) -> list[Any]:
        """如果消息属于 Album (grouped_id)，抓取同组的所有关联媒体消息 (带 5 秒超时保护)"""
        if not message or not getattr(message, "grouped_id", None):
            return [message] if message else []

        grouped_id = message.grouped_id
        msg_id = message.id

        min_search = max(0, msg_id - 10)
        max_search = msg_id + 10

        album_msgs = []
        try:
            target_entity = getattr(message, "input_chat", None)
            if target_entity is None:
                if isinstance(chat_id, int):
                    target_entity = chat_id
                elif isinstance(chat_id, str) and re.match(r"^-?\d+$", chat_id.strip()):
                    target_entity = int(chat_id.strip())
                else:
                    target_entity = chat_id
            msgs = await asyncio.wait_for(
                client.get_messages(target_entity, min_id=min_search, max_id=max_search), timeout=5.0
            )
            for m in msgs or ():
                if m and getattr(m, "grouped_id", None) == grouped_id:
                    album_msgs.append(m)
        except Exception as e:
            _reraise_task_level_resolution_error(e)
            logger.warning("⚠️ 关联 Album 抓取失败 (grouped_id=%s): %s", grouped_id, e)
            raise AlbumDiscoveryError(f"关联 Album (grouped_id={grouped_id}) 抓取失败: {e}") from e

        if not album_msgs:
            raise AlbumDiscoveryError(f"关联 Album (grouped_id={grouped_id}) 抓取结果为空，无法确认相册完整性")

        album_msgs.sort(key=lambda x: x.id)
        return album_msgs

    @staticmethod
    def get_file_name_and_size(message: Any, fallback_id: int | None = None) -> tuple[str, int]:
        """从 Telegram 消息精准导出文件名与文件字节大小（正确提取 Photo 大小）。

        fallback_id: 兜底文件名基准 ID (中转桥下载必须传源消息 ID) —— 传 message.id 会把
        中转群临时 ID 写进文件名, 桥接重发后 ID 漂移, 重试选中不同 .part/.meta 断点续传断裂。"""
        if not message or not message.media:
            return "unknown_file", 0

        base_id = fallback_id if fallback_id is not None else getattr(message, "id", 0)
        file_name = None
        file_size = 0

        if isinstance(message.media, MessageMediaDocument):
            doc = message.media.document
            file_size = getattr(doc, "size", 0) if doc else 0
            if doc and doc.attributes:
                for attr in doc.attributes:
                    if hasattr(attr, "file_name") and attr.file_name:
                        file_name = attr.file_name
                        break
            if not file_name:
                mime = getattr(doc, "mime_type", "")
                ext = ".mp4" if "video" in mime else (".mp3" if "audio" in mime else ".bin")
                file_name = f"doc_{base_id}{ext}"

        elif isinstance(message.media, MessageMediaPhoto):
            photo = message.media.photo
            if photo and hasattr(photo, "sizes") and photo.sizes:
                for s in reversed(photo.sizes):
                    sz = getattr(s, "size", 0)
                    if sz > 0:
                        file_size = sz
                        break
                    sizes_list = getattr(s, "sizes", None)
                    if sizes_list and isinstance(sizes_list, list):
                        file_size = max(sizes_list)
                        break
            file_name = f"photo_{base_id}.jpg"
        else:
            telethon_file = getattr(message, "file", None)
            if telethon_file:
                file_name = getattr(telethon_file, "name", None)
                file_size = getattr(telethon_file, "size", 0) or 0
            if not file_name:
                file_name = f"media_{base_id}.file"

        return file_name, file_size

    @classmethod
    async def download_single_message(
        cls,
        client: TelegramClient | list[TelegramClient],
        chat_id: Any,
        message: Any,
        task_id: str | None = None,
        progress_callback: Callable[..., Any] | None = None,
        is_cancelled: Callable[[], bool] | None = None,
        is_paused: Callable[[], bool] | None = None,
        threads: int | None = None,
        origin_chat_id: Any | None = None,
        origin_msg_id: int | None = None,
        force: bool = False,
        origin_grouped_id: str | int | None = None,
    ) -> str | None:
        """使用 FastTelethon 多并发下载单条消息的媒体文件，支持断点续传、防重校验与源元数据保护"""
        if not message or not message.media:
            if task_id:
                await TaskQueueRepository.append_log_async(
                    task_id, f"⚠️ 消息 {getattr(message, 'id', '?')} 中未找到可下载媒体。"
                )
            return None

        _cached_concurrency: int | None = None
        _last_concurrency_check: float = 0.0
        _CONCURRENCY_CACHE_TTL: float = 2.0  # 2.0 秒 TTL 短时内存缓存

        def get_current_download_concurrency() -> int:
            nonlocal _cached_concurrency, _last_concurrency_check
            now = time.monotonic()
            if _cached_concurrency is not None and (now - _last_concurrency_check < _CONCURRENCY_CACHE_TTL):
                return _cached_concurrency

            val = None
            # 动态解析并发线程数 (任务运行时变更优先 -> 初始入参 -> 系统设置 -> 全局静态设置)
            if task_id:
                try:
                    task_rec = TaskQueueRepository.get_by_id(task_id)
                    if task_rec:
                        candidate_th = (task_rec.get("params") or {}).get("threads")
                        if candidate_th is None:
                            candidate_th = task_rec.get("concurrency_total")
                        if candidate_th is not None and str(candidate_th).strip() != "" and int(candidate_th) > 0:
                            val = max(1, min(int(candidate_th), 32))
                except Exception as t_err:
                    logger.debug("动态获取任务下载并发配置异常: %s", t_err)
            if val is None and threads is not None:
                try:
                    val = max(1, min(int(threads), 32))
                except (ValueError, TypeError) as val_err:
                    logger.debug("转换传入 threads 参数为有效整型失败: %s", val_err)
            if val is None:
                from app.db.account_repository import SystemSettingsRepository

                try:
                    val = SystemSettingsRepository.get_int(
                        "download_parallel_connections", int(getattr(settings, "DOWNLOAD_PARALLEL_CONNECTIONS", 8))
                    )
                    val = max(1, min(val, 32))
                except Exception:
                    val = 8

            _cached_concurrency = val
            _last_concurrency_check = now
            return val

        effective_threads = get_current_download_concurrency()

        # 元数据解耦红线: origin_* 优先用于存储路径与防重历史, 绝不被中转群临时 ID 篡改
        effective_chat_id = origin_chat_id if origin_chat_id is not None else chat_id
        effective_msg_id = origin_msg_id if origin_msg_id is not None else message.id
        msg_id = effective_msg_id
        str_chat_id = normalize_download_chat_key(effective_chat_id) or str(effective_chat_id)
        target_msg_id = message.id  # 底层 Telegram 实体抓取/凭证预取的目标消息 ID (中转群临时 ID 或直连原 ID)

        file_name, total_bytes = cls.get_file_name_and_size(message, fallback_id=msg_id)
        safe_file_name = sanitize_filename(file_name)

        def _build_save_path() -> str:
            # 输出路径含频道规范化标识: 两个频道可能存在同 ID + 同名媒体, 纯 {msg_id}_{name}
            # 布局会让它们共享同一输出与 .part/.meta 断点 (互相覆盖/续传错位)
            return os.path.join(settings.DOWNLOAD_DIR, f"{str_chat_id}_{msg_id}_{safe_file_name}")

        # 数据库防重检查 (严格基于源频道与源消息 ID); 含磁盘 stat 与可能的失效行 DELETE, 移入线程避免阻塞循环
        downloaded_path = None
        if not force or task_id:
            try:
                # 强制任务续跑时，只复用本任务已成功落盘的文件。
                downloaded_path = await asyncio.to_thread(
                    DownloadHistoryRepository.get_valid_download_path,
                    str_chat_id,
                    msg_id,
                    task_id=task_id if force else None,
                    reused_by_task_id=task_id,
                )
            except OSError as exc:
                logger.debug("下载执行无法确认历史文件 %s/%s: %s", str_chat_id, msg_id, exc)
        if downloaded_path:
            skip_msg = f"⏩ [已跳过] 消息 ID {msg_id} ({safe_file_name}) 之前已被下载过。"
            logger.info(skip_msg)
            if task_id:
                await TaskQueueRepository.append_log_async(task_id, skip_msg)
            if progress_callback:
                progress_callback(
                    100, 100, 0.0, 0.0, file_name, concurrency=0, dc_id=0, concurrency_total=effective_threads
                )
            return downloaded_path  # 回传历史行真实落盘路径 (兼容旧命名布局)

        save_path = _build_save_path()
        part_path = save_path + ".part"
        meta_path = save_path + ".meta"

        # 断点归属登记 (供删除任务时精确清理) + 活跃路径登记 (存储删除守卫红线: 不得触碰使用中的文件)
        if task_id:
            DownloadBreakpointRepository.upsert(task_id, str_chat_id, msg_id, part_path, meta_path, save_path)
        active_path_keys = _register_active_download_paths_sync((save_path, part_path, meta_path))

        def progress_wrapper(current, total, speed, eta, concurrency=None, dc_id=0, concurrency_total=0):
            # 引擎心跳传 (活跃线程数 int, 总线程数 int); "n/N" 比率不再挤占 concurrency 整数列
            cur_th = get_current_download_concurrency()
            active_th = concurrency if concurrency is not None else cur_th
            tot_th = concurrency_total or cur_th
            if progress_callback:
                try:
                    progress_callback(current, total, speed, eta, file_name, active_th, dc_id, tot_th)
                except Exception as e:
                    logger.warning("下载进度回调异常: %s", e)

        try:
            # 日志写也纳入清理作用域: 注册已生效, 此处的失败/取消必须走 finally 释放活跃路径登记
            if task_id:
                await TaskQueueRepository.append_log_async(
                    task_id, f"📥 开始极速下载: {safe_file_name} (大小: {total_bytes / (1024 * 1024):.2f}MB)"
                )

            try:
                location = get_input_location(message.media)
            except Exception:
                location = message.media

            # 凭证预取/刷新的 peer 解析: 必须用无 access_hash 的裸标识 (message.chat_id),
            # 主号 input_chat 的 access_hash 属于主号账号缓存, 借给辅助 Bot 会预取失败
            # 被误剔除 (Telethon 文档: access hash 按账号隔离, input peer 原样透传不换 hash)
            target_chat = getattr(message, "chat_id", None)
            if target_chat is None:
                if isinstance(chat_id, int):
                    target_chat = chat_id
                elif isinstance(chat_id, str) and re.match(r"^-?\d+$", chat_id.strip()):
                    target_chat = int(chat_id.strip())
                else:
                    target_chat = chat_id

            client_locations = {}
            if isinstance(client, list):

                async def _prefetch_loc(c):
                    try:
                        msg_c = await asyncio.wait_for(c.get_messages(target_chat, ids=target_msg_id), timeout=5.0)
                        if msg_c and msg_c.media:
                            try:
                                loc = get_input_location(msg_c.media)
                                if isinstance(loc, tuple):
                                    loc = loc[1]
                                return c, loc
                            except Exception:
                                return c, msg_c.media
                    except Exception as ex:
                        logger.debug("多 Client 凭证预取异常: %s", ex)
                    return c, None

                prefetch_results = await asyncio.gather(*[_prefetch_loc(c) for c in client], return_exceptions=True)
                for item in prefetch_results:
                    if isinstance(item, tuple) and item[1] is not None:
                        client_locations[item[0]] = item[1]

            # 严格纯化客户端池：过滤出预取到自身专属凭证的 Client
            # 若辅助 Bot 无权访问目标频道（私有群组典型场景），彻底剔除，保底纯化为 [primary_client]
            if isinstance(client, list) and client:
                primary_client = client[0]
                valid_clients = [c for c in client if c in client_locations]
                if not valid_clients:
                    valid_clients = [primary_client]
            else:
                valid_clients = client

            async def refetch_location(target_c=None):
                c = target_c or (valid_clients[0] if isinstance(valid_clients, list) else valid_clients)
                try:
                    fresh_msg = await c.get_messages(target_chat, ids=target_msg_id)
                    if fresh_msg and fresh_msg.media:
                        try:
                            loc = get_input_location(fresh_msg.media)
                            if isinstance(loc, tuple):
                                loc = loc[1]
                        except Exception:
                            loc = fresh_msg.media
                        return loc
                except Exception as ref_err:
                    logger.warning(f"⚠️ 自动刷新消息 ID {target_msg_id} Location 异常: {ref_err}")

                # 阻断毒药回退：特定 Client 刷新失败直接返回 None，绝不借用主账号凭证冒充
                return None

            final_path = await download_file_parallel(
                client=valid_clients,
                location=location,
                out_file_path=save_path,
                total_bytes=total_bytes,
                progress_callback=progress_wrapper,
                concurrency=effective_threads,
                get_concurrency=get_current_download_concurrency,
                set_initial_bytes=getattr(progress_callback, "set_initial_bytes", None),
                is_cancelled=is_cancelled,
                is_paused=is_paused,
                raw_media=message.media,
                client_locations=client_locations,
                refetch_location=refetch_location,
            )

            if is_paused and is_paused():
                if task_id:
                    await TaskQueueRepository.append_log_async(
                        task_id,
                        f"⏸️ 任务已安全暂停: 消息 {msg_id} ({safe_file_name}) 断点已留存，底层连接已完全释放让位",
                    )
                return None

            if not (is_cancelled and is_cancelled()):
                # 双保险: 引擎异常路径已不可达此处, 仍校验目标文件真实存在且非空才写历史库
                if not _verify_final_file_nonempty_sync(final_path):
                    err_msg = f"❌ 下载消息 {msg_id} ({safe_file_name}) 失败: 目标文件不存在或为空，跳过入库"
                    logger.warning(err_msg)
                    if task_id:
                        await TaskQueueRepository.append_log_async(task_id, err_msg)
                    FailureLogger.log_failure(
                        file_path=final_path or safe_file_name,
                        target_chat_id=str_chat_id,
                        error_obj=RuntimeError("目标文件不存在或为空，下载不完整"),
                        client_type="download",
                        task_id=task_id,
                        log_filename="download_failures.jsonl",
                        file_size_mb=round(total_bytes / (1024 * 1024), 2) if total_bytes else None,
                    )
                    return None
                grouped_id = origin_grouped_id if origin_msg_id is not None else getattr(message, "grouped_id", None)
                # 流式计算成品 SHA-256 (1MB 分块, 恒定内存); 线程池执行避免大文件哈希阻塞事件循环
                file_hash = await asyncio.to_thread(sha256_of_file, final_path)
                await asyncio.to_thread(
                    DownloadHistoryRepository.record,
                    chat_id=str_chat_id,
                    msg_id=msg_id,
                    grouped_id=grouped_id,
                    file_name=safe_file_name,
                    file_path=final_path,
                    file_size=total_bytes,
                    file_hash=file_hash,
                    task_id=task_id,
                )
                # 成品已落防重库, 断点归属行随之清除 (断点文件本身已由引擎 rename 消除)
                if task_id:
                    DownloadBreakpointRepository.delete(task_id, str_chat_id, msg_id)
                success_msg = f"✅ 下载成功并存入本地与数据库: {safe_file_name} (sha256: {file_hash[:16]}…)"
                logger.info(success_msg)
                if task_id:
                    await TaskQueueRepository.append_log_async(task_id, success_msg)
                return final_path

        except Exception as e:
            # 账号会话失效直接向上传播, 由任务层统一置 failed + 引导重绑 (区别于普通单条下载失败);
            # FloodWaitError 同样上抛 —— download_runner 四处 isinstance(FloodWaitError) 拦截后走
            # 任务级熔断退避, 在此处吞掉会退化为无退避的单条失败
            if isinstance(e, FloodWaitError) or is_session_invalid_error(e):
                raise
            err_msg = f"❌ 下载消息 {msg_id} ({safe_file_name}) 失败: {e}"
            logger.warning(err_msg)
            if task_id:
                await TaskQueueRepository.append_log_async(task_id, err_msg)
            FailureLogger.log_failure(
                file_path=save_path or safe_file_name,
                target_chat_id=str_chat_id,
                error_obj=e,
                client_type="download",
                task_id=task_id,
                log_filename="download_failures.jsonl",
                file_size_mb=round(total_bytes / (1024 * 1024), 2) if total_bytes else None,
            )
        finally:
            # 无论成功/取消/失败/异常传播, 释放活跃路径登记 (否则会永久阻塞该文件的存储删除)
            _unregister_active_download_paths_sync(active_path_keys)

        return None

    @classmethod
    async def inspect_download_range(cls, channel_identifier: str, start_id: int, end_id: int) -> dict[str, Any]:
        """
        在执行范围下载前对目标频道与消息范围进行预检：
        1. 探测频道可用性与连通性
        2. 探测受限保护状态 (noforwards)
        3. 分析区间内的媒体总数、分布 (图片/视频/音频/文档) 及预估下载总容量
        4. 统计本地已下载防重消息数
        """
        from app.core.client_manager import client_manager
        from app.services.forward_service import ForwardService

        if start_id > end_id:
            start_id, end_id = end_id, start_id

        parsed_chat = ForwardService.parse_chat_identifier(channel_identifier)
        history_chat_keys = tuple(
            dict.fromkeys(
                (
                    normalize_download_chat_key(channel_identifier),
                    normalize_download_chat_key(parsed_chat),
                    str(parsed_chat),
                    channel_identifier,
                )
            )
        )
        userbot = client_manager.userbot
        if not userbot or not userbot.is_connected():
            await client_manager.initialize()
            userbot = client_manager.userbot

        if not userbot:
            raise RuntimeError("Telegram 引擎未初始化，请检查 Telegram 授权状态")

        # 1. 尝试获取 Entity (若本地 Session 缺少 access_hash 缓存则自动拉取 Dialogs 刷新)
        try:
            entity = await ForwardService.resolve_entity(userbot, parsed_chat)
        except Exception as e:
            return {
                "channel_identifier": str(channel_identifier),
                "parsed_chat_id": str(parsed_chat),
                "channel_title": str(channel_identifier),
                "channel_username": None,
                "is_accessible": False,
                "is_restricted": False,
                "channel_type": "Unknown",
                "start_id": start_id,
                "end_id": end_id,
                "total_range_messages": end_id - start_id + 1,
                "messages_with_media": 0,
                "text_only_messages": 0,
                "photos_count": 0,
                "videos_count": 0,
                "audios_count": 0,
                "documents_count": 0,
                "estimated_size_bytes": 0,
                "estimated_size_mb": 0.0,
                "estimated_size_gb": 0.0,
                "already_downloaded_count": 0,
                "all_media_downloaded": False,
                "history_check_complete": False,
                "warning_messages": [f"无法连接或解析目标频道: {e}"],
                "error_detail": str(e),
                "transit_capable": False,
                "transit_notice": None,
            }

        channel_title = getattr(entity, "title", getattr(entity, "first_name", str(channel_identifier)))
        channel_username = getattr(entity, "username", None)
        is_restricted = getattr(entity, "noforwards", False)

        channel_type = "Channel"
        if getattr(entity, "megagroup", False):
            channel_type = "Supergroup"
        elif getattr(entity, "broadcast", False):
            channel_type = "Channel"
        elif hasattr(entity, "participants_count"):
            channel_type = "Chat"

        total_range = end_id - start_id + 1

        messages_with_media = 0
        text_only_messages = 0
        photos_count = 0
        videos_count = 0
        audios_count = 0
        documents_count = 0
        estimated_size_bytes = 0
        already_downloaded_count = 0
        eligible_downloaded_count = 0
        history_check_complete = True
        warning_messages = []

        if is_restricted:
            warning_messages.append(
                "🛡️ 目标频道开启了防转发/受限内容保护 (Restricted Content / NoForwards)，本工具将启用突破限制下载模式"
            )

        # 分批检索消息元数据 (单次最多 100 条; 单次预检封顶防超大范围耗尽配额)
        chunk_size = 100
        scan_end_id = min(end_id, start_id + MAX_INSPECT_RANGE_MESSAGES - 1)
        if scan_end_id < end_id:
            history_check_complete = False
            warning_messages.append(
                f"⚠️ 预检范围共 {total_range} 条，超过单次预检上限 ({MAX_INSPECT_RANGE_MESSAGES} 条)，"
                f"已截断仅扫描 {start_id}~{scan_end_id}，剩余 {scan_end_id + 1}~{end_id} 未扫描"
            )
        for first_id in range(start_id, scan_end_id + 1, chunk_size):
            sub_ids = list(range(first_id, min(first_id + chunk_size, scan_end_id + 1)))
            try:
                msgs = await asyncio.wait_for(userbot.get_messages(entity, ids=sub_ids), timeout=15.0)
                if msgs is None or len(msgs) != len(sub_ids):
                    history_check_complete = False
                    if not msgs:
                        continue
                media_ids = []
                for m in msgs:
                    if not m:
                        continue
                    if not m.media:
                        if m.message:
                            text_only_messages += 1
                        continue

                    messages_with_media += 1
                    media_ids.append(m.id)
                    file_name, file_size = cls.get_file_name_and_size(m)
                    estimated_size_bytes += file_size

                    if isinstance(m.media, MessageMediaPhoto):
                        photos_count += 1
                    elif isinstance(m.media, MessageMediaDocument):
                        doc = m.media.document
                        mime = getattr(doc, "mime_type", "").lower()
                        if "video" in mime:
                            videos_count += 1
                        elif "audio" in mime or "ogg" in mime:
                            audios_count += 1
                        else:
                            documents_count += 1
                    else:
                        documents_count += 1
                if media_ids:
                    valid, eligible = await asyncio.to_thread(_count_downloaded_media, history_chat_keys, media_ids)
                    already_downloaded_count += valid
                    eligible_downloaded_count += eligible
            except Exception as e:
                history_check_complete = False
                warning_messages.append(f"获取消息区间 {sub_ids[0]}~{sub_ids[-1]} 元数据提示: {e}")

        estimated_mb = round(estimated_size_bytes / (1024 * 1024), 2)
        estimated_gb = round(estimated_size_bytes / (1024 * 1024 * 1024), 2)

        # 磁盘剩余空间预检: 按下载目录所在挂载点测算 (Windows/Linux 均适用); 检测失败不阻塞预检
        disk_free_bytes = 0
        disk_free_gb = 0.0
        disk_sufficient = True
        try:
            usage = shutil.disk_usage(settings.DOWNLOAD_DIR)
            disk_free_bytes = usage.free
            disk_free_gb = round(disk_free_bytes / (1024 * 1024 * 1024), 2)
            # 5% 安全余量: 覆盖文件系统开销与容量估算误差
            disk_sufficient = disk_free_bytes >= estimated_size_bytes * 1.05
            if not disk_sufficient:
                warning_messages.append(
                    f"⚠️ 磁盘剩余空间不足: 本次预估需要 ~{estimated_gb} GB (含 5% 安全余量)，"
                    f"下载目录所在磁盘仅剩 {disk_free_gb} GB，任务可能中途失败"
                )
        except Exception as e:
            warning_messages.append(f"磁盘剩余空间检测失败 (已跳过): {e}")

        # 中转借壳加速资格预测 (梯队 1 准入评估)
        transit_capable = False
        transit_notice = None
        try:
            from app.core.client_manager import client_manager, is_private_channel_identifier
            from app.services.transit_bridge_service import TransitBridgeService

            transit_conf = TransitBridgeService.get_transit_settings()
            is_priv = is_private_channel_identifier(channel_identifier) or is_private_channel_identifier(parsed_chat)
            if is_priv and not is_restricted and transit_conf["enabled"] and transit_conf["transit_chat_id"]:
                ready_bots = [b for b in client_manager.get_all_bots() if b.is_ready()]
                if ready_bots:
                    min_mb = transit_conf["min_file_size_mb"]
                    transit_capable = True
                    transit_notice = f"🚀 目标私密频道支持转发且中转群已就绪，大于 {min_mb}MB 的媒体将自动激活「梯队 1 · 中转借壳加速」 (1 Userbot + {len(ready_bots)} Bots 联合极速下载)"
        except Exception as te:
            logging.debug("中转加速资格预检异常: %s", te)

        return {
            "channel_identifier": str(channel_identifier),
            "parsed_chat_id": str(parsed_chat),
            "channel_title": channel_title,
            "channel_username": channel_username,
            "is_accessible": True,
            "is_restricted": is_restricted,
            "channel_type": channel_type,
            "start_id": start_id,
            "end_id": end_id,
            "total_range_messages": total_range,
            "messages_with_media": messages_with_media,
            "text_only_messages": text_only_messages,
            "photos_count": photos_count,
            "videos_count": videos_count,
            "audios_count": audios_count,
            "documents_count": documents_count,
            "estimated_size_bytes": estimated_size_bytes,
            "estimated_size_mb": estimated_mb,
            "estimated_size_gb": estimated_gb,
            "disk_free_bytes": disk_free_bytes,
            "disk_free_gb": disk_free_gb,
            "disk_sufficient": disk_sufficient,
            "already_downloaded_count": already_downloaded_count,
            "all_media_downloaded": history_check_complete
            and messages_with_media > 0
            and eligible_downloaded_count == messages_with_media,
            "history_check_complete": history_check_complete,
            "warning_messages": warning_messages,
            "error_detail": None,
            "transit_capable": transit_capable,
            "transit_notice": transit_notice,
        }
