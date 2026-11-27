import logging
import os
from collections.abc import Callable
from typing import Any

from app.config import settings
from app.core.client_manager import client_manager
from app.core.media_sniff import is_supported_local_media
from app.core.path_utils import walk_publish_files
from app.core.pipeline.models import (
    OVERSIZE_POLICY_SKIP_AND_REPORT,
    OVERSIZE_POLICY_SPLIT,
    PublishResult,
)

# 唯一实现在 core/pipeline/utils (下沉); 仅保留内部使用名, 不再对外 re-export
from app.core.pipeline.utils import (
    classify_oversize,
    format_folder_tag,
    natural_sort_key,
)
from app.services.channel_inspector import ChannelInspector

IMAGE_EXTENSIONS = settings.IMAGE_EXTENSIONS
VIDEO_EXTENSIONS = settings.VIDEO_EXTENSIONS
AUDIO_EXTENSIONS = settings.AUDIO_EXTENSIONS
VALID_MEDIA_EXTENSIONS = settings.VALID_MEDIA_EXTENSIONS

logger = logging.getLogger(__name__)


class PublishFolderResult(PublishResult):
    """发布文件夹结果容器类，继承自 PublishResult (及 int)，保持 100% 向后兼容"""


class PublisherService:
    @classmethod
    def inspect_folder(
        cls,
        folder_path: str,
        target_channel: str | None = None,
        account_premium: bool | None = None,
        effective_limit_bytes: int | None = None,
    ) -> dict[str, Any]:
        """
        前置深度扫描与合规检测本地文件夹：
        1. 严格过滤仅统计有效媒体文件（图片、视频、音频），自动归类被排除的非媒体文件
        2. 统计图片/视频/音频各自分类数量与容量
        3. 按当前账号动态上限 (Premium 4000 MiB (3.91 GiB) / 非 Premium 2000 MiB (1.95 GiB)) 预警超限文件清单并分类归因
        4. 预览防重已发布历史记录数与媒体列表
        """
        from app.db.download_repository import DownloadHistoryRepository
        from app.db.publish_repository import PublishHistoryRepository

        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"本地文件夹路径不存在: {folder_path}")

        total_scanned_files = 0
        all_media_files: list[str] = []
        ignored_files: list[dict[str, Any]] = []

        images_count = 0
        images_size_bytes = 0
        videos_count = 0
        videos_size_bytes = 0
        audios_count = 0
        audios_size_bytes = 0

        # 全量遍历以捕获媒体与非媒体
        for root, files in walk_publish_files(folder_path):
            for file in files:
                total_scanned_files += 1
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, folder_path).replace("\\", "/")
                ext = os.path.splitext(file)[1].lower()
                try:
                    fsize = os.path.getsize(full_path)
                except Exception:
                    fsize = 0

                is_media, ignore_reason = is_supported_local_media(full_path)
                if is_media:
                    all_media_files.append(full_path)
                    if ext in IMAGE_EXTENSIONS:
                        images_count += 1
                        images_size_bytes += fsize
                    elif ext in VIDEO_EXTENSIONS:
                        videos_count += 1
                        videos_size_bytes += fsize
                    elif ext in AUDIO_EXTENSIONS:
                        audios_count += 1
                        audios_size_bytes += fsize
                else:
                    ignored_files.append(
                        {
                            "file_name": file,
                            "rel_path": rel_path,
                            "file_size_mb": round(fsize / (1024 * 1024), 2),
                            "extension": ext or "无后缀",
                            "reason": ignore_reason or f"非有效媒体格式 ({ext or 'unknown'})，已自动过滤",
                        }
                    )

        all_media_files.sort(key=lambda p: (natural_sort_key(os.path.relpath(p, folder_path).replace("\\", "/")), p))
        total_media_files = len(all_media_files)
        total_media_size_bytes = sum(images_size_bytes + videos_size_bytes + audios_size_bytes for _ in [0])

        target_chat_id: int | str | None = None
        if target_channel:
            try:
                target_chat_id = int(target_channel)
            except ValueError:
                target_chat_id = target_channel

        valid_files_count = 0
        already_published_count = 0
        oversized_files: list[dict[str, Any]] = []
        media_files_list: list[dict[str, Any]] = []

        # 动态上限: 显式 effective_limit_bytes (调用方自带同源快照上限) 最优先; 仅传入
        # 账号级 Premium 快照时门槛与快照同源自洽派生 (—— 快照身份与全局
        # 引擎可能不一致, 不得混用全局引擎门槛, 否则 oversize_kind 标签与超限判定错位);
        # 二者皆缺省则整体回退全局引擎 (原行为不变)
        snapshot_supplied = account_premium is not None
        if account_premium is None:
            account_premium = bool(client_manager.is_premium)
        if effective_limit_bytes is not None:
            max_limit = effective_limit_bytes
        elif snapshot_supplied:
            max_limit = settings.PREMIUM_MAX_FILE_SIZE_BYTES if account_premium else settings.MAX_FILE_SIZE_BYTES
        else:
            max_limit = client_manager.effective_max_file_size_bytes()

        unavailable_files: list[dict[str, Any]] = []
        for file_path in all_media_files:
            try:
                file_size = os.path.getsize(file_path)
            except OSError:
                # 全量遍历与逐条巡检之间文件可能被移除/失去访问权: 单条跳过并单独上报,
                # 不得让单文件异常中止整个目录巡检 (第一遍 os.walk 读已兜底, 此处为第二遍读)
                unavailable_files.append(
                    {
                        "file_name": os.path.basename(file_path),
                        "rel_path": os.path.relpath(file_path, folder_path).replace("\\", "/"),
                        "reason": "文件大小不可读取 (可能已被移除或无访问权限)，已从巡检明细中跳过",
                    }
                )
                continue
            file_name = os.path.basename(file_path)
            rel_path = os.path.relpath(file_path, folder_path).replace("\\", "/")
            ext = os.path.splitext(file_path)[1].lower()

            if ext in IMAGE_EXTENSIONS:
                media_type = "image"
            elif ext in VIDEO_EXTENSIONS:
                media_type = "video"
            elif ext in AUDIO_EXTENSIONS:
                media_type = "audio"
            else:
                media_type = "unknown"

            is_oversized = file_size > max_limit
            is_published = False

            if is_oversized:
                oversize_kind, _ = classify_oversize(file_size, max_limit, account_premium)
                oversized_files.append(
                    {
                        "file_name": file_name,
                        "rel_path": rel_path,
                        "file_size_bytes": file_size,
                        "file_size_mb": round(file_size / (1024 * 1024), 2),
                        "file_size_gb": round(file_size / (1024 * 1024 * 1024), 2),
                        "limit_mb": round(max_limit / (1024 * 1024), 2),
                        "exceeded_mb": round((file_size - max_limit) / (1024 * 1024), 2),
                        "oversize_kind": oversize_kind,
                    }
                )
            else:
                valid_files_count += 1

            # 已下载成品的 SHA-256 (来自 download_history; 未计算过为 null, 不在预检时现算保证速度)
            file_hash = DownloadHistoryRepository.get_hash_by_path(file_path)

            if target_chat_id is not None and PublishHistoryRepository.is_published(
                target_chat_id, file_path, file_size, file_hash
            ):
                already_published_count += 1
                is_published = True

            media_files_list.append(
                {
                    "file_name": file_name,
                    "rel_path": rel_path,
                    "file_size_bytes": file_size,
                    "file_size_mb": round(file_size / (1024 * 1024), 2),
                    "media_type": media_type,
                    "is_oversized": is_oversized,
                    "is_already_published": is_published,
                    "file_hash": file_hash or None,
                }
            )

        return {
            "folder_path": folder_path,
            "total_scanned_files": total_scanned_files,
            "total_media_files": total_media_files,
            "total_media_size_bytes": total_media_size_bytes,
            "total_media_size_mb": round(total_media_size_bytes / (1024 * 1024), 2),
            "total_media_size_gb": round(total_media_size_bytes / (1024 * 1024 * 1024), 2),
            "valid_files_count": valid_files_count,
            "already_published_count": already_published_count,
            # 口径 = 未超限且未发布 (already_published 含 oversized 而有效集
            # 不含, 相减会把「已发布但超限」错误抵扣未发布有效文件 → 限额变化后欠计)
            "pending_publish_count": sum(
                1 for f in media_files_list if not f["is_oversized"] and not f["is_already_published"]
            ),
            "account_premium": account_premium,
            "effective_limit_mb": round(max_limit / (1024 * 1024), 2),
            "images": {
                "count": images_count,
                "size_bytes": images_size_bytes,
                "size_mb": round(images_size_bytes / (1024 * 1024), 2),
            },
            "videos": {
                "count": videos_count,
                "size_bytes": videos_size_bytes,
                "size_mb": round(videos_size_bytes / (1024 * 1024), 2),
            },
            "audios": {
                "count": audios_count,
                "size_bytes": audios_size_bytes,
                "size_mb": round(audios_size_bytes / (1024 * 1024), 2),
            },
            "ignored_files_count": len(ignored_files),
            "unavailable_files_count": len(unavailable_files),
            "oversized_files": oversized_files,
            "media_files": media_files_list,
            "ignored_files": ignored_files,
            "unavailable_files": unavailable_files,
            # 向后兼容
            "total_files": total_media_files,
            "total_size_bytes": total_media_size_bytes,
            "total_size_mb": round(total_media_size_bytes / (1024 * 1024), 2),
            "total_size_gb": round(total_media_size_bytes / (1024 * 1024 * 1024), 2),
        }

    @classmethod
    async def publish_folder(
        cls,
        folder_path: str,
        target_chat_id: str | int,
        album_mode: bool = True,
        delay_seconds: float = 3.0,
        progress_callback: Callable | None = None,
        is_cancelled: Callable[[], bool] | None = None,
        is_paused: Callable[[], bool] | None = None,
        task_id: str | None = None,
        account_id: int | None = None,
        upload_concurrency: int | None = None,
        upload_in_flight_per_connection: int | None = None,
        force_republish: bool = False,
        oversize_policy: str = OVERSIZE_POLICY_SKIP_AND_REPORT,
        send_placeholder_card: bool | None = None,
    ) -> PublishFolderResult:
        """批量发布本地文件夹内容到目标频道/群组 (门面薄层，组装 PublishPipeline 执行)。"""
        from app.core.pipeline import (
            DEFAULT_CLIENT_REGISTRY,
            DEFAULT_TASK_STORE,
            LocalDiskTransportEngine,
            LocalFolderSource,
            NoopCleanupStrategy,
            PublishContext,
            PublishPipeline,
            TelegramChannelNotifier,
        )
        from app.services.system_settings_service import SystemSettingsService

        # D3: split 为 PikPak 流式虚拟分卷专属概念 (VirtualStreamSplitter + JIT 索链) ——
        # 本地磁盘传输无虚拟分卷能力, split 请求值一律映射为 skip_and_report
        # (前端本地发布视图不暴露 split 选项; 本注释为该映射的强制说明位)。
        if oversize_policy == OVERSIZE_POLICY_SPLIT:
            oversize_policy = OVERSIZE_POLICY_SKIP_AND_REPORT

        # 占位卡开关派生单点 (与 PikPak 门面逐字对齐, 先例 pikpak_publisher_service.py:131-132):
        # None=未指定 → skip_and_report 默认发卡; 显式 True/False 一律尊重
        if send_placeholder_card is None:
            send_placeholder_card = oversize_policy == OVERSIZE_POLICY_SKIP_AND_REPORT

        auto_remux_mpegts = SystemSettingsService.get_bool(
            "auto_remux_mpegts_to_mp4",
            getattr(settings, "AUTO_REMUX_MPEGTS_TO_MP4", True),
        )

        folder_name = format_folder_tag(folder_path)

        existing_sent_keys = set()
        if task_id:
            try:
                from app.db.task_repository import TaskQueueRepository

                t_row = TaskQueueRepository.get_by_id(task_id)
                if t_row and t_row.get("params"):
                    existing_sent_keys = set(t_row["params"].get("sent_placeholder_keys") or [])
            except Exception as exc:
                logger.debug("读取本地发布已发送占位卡键列表异常 (非致命): %s", exc)

        context = PublishContext(
            task_id=task_id or "",
            target_chat_id=target_chat_id,
            target_channel_raw=str(target_chat_id),
            is_folder_publish=True,
            folder_name=folder_name,
            folder_path=folder_path,
            account_id=account_id,
            delay_seconds=delay_seconds,
            force_republish=force_republish,
            album_mode=album_mode,
            source_type="local",  # 包3 (K3): medium 标识显式声明 (编排层唯一判定来源)
            oversize_policy=oversize_policy,
            send_placeholder_card=send_placeholder_card,
            auto_remux_mpegts_to_mp4=auto_remux_mpegts,
            upload_concurrency=upload_concurrency,
            upload_in_flight_per_connection=upload_in_flight_per_connection,
            is_cancelled=is_cancelled,
            is_paused=is_paused,
            progress_callback=progress_callback,
            sent_placeholder_keys=existing_sent_keys,
        )

        pipeline = PublishPipeline(
            source=LocalFolderSource(folder_path=folder_path),
            # 端口显式装配 (默认=现状单例适配器; 为未来多账号编排留缝)
            engine=LocalDiskTransportEngine(
                registry=DEFAULT_CLIENT_REGISTRY,
                store=DEFAULT_TASK_STORE,
            ),
            cleanup=NoopCleanupStrategy(),
            notifier=TelegramChannelNotifier(),
            # 写权限探针构造注入 (类属性调用时读取 → 测试 monkeypatch 方法仍生效)
            channel_probe=ChannelInspector.inspect_channel,
        )

        res = await pipeline.execute(context)
        return PublishFolderResult(
            published_count=res.published_count,
            total_files_count=res.total_files_count,
            failed_files=res.failed_files,
            skipped_files=res.skipped_files,
            archive_summary=res.archive_summary,
            is_folder_publish=res.is_folder_publish,
            folder_name=res.folder_name,
            newly_published_count=res.newly_published_count,
            archive_failed=res.archive_failed,
            album_published_count=res.album_published_count,
            failed_details=res.failed_details,
            dedup_skipped=res.dedup_skipped,
        )
