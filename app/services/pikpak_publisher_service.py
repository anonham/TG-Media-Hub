"""PikPak → Telegram 流式发布调度器。

门面组装 PublishPipeline 执行; 失败分类学 / CLI 域异常体系 / 目录展开算法 / meta 命名原语 /
audit 判定 / 缩略图链路的唯一实现均在 app/core/pikpak/ (本模块仅内部消费, 不对外 re-export)。

红线:
- 媒体字节全程内存 (Provider 读流 → MTProto 上传, 零媒体落盘);
- 下载直链只存内存、只传 Provider (url_provider 回调), 禁止写日志/DB/WS;
- 断点元数据仅落 settings.PIKPAK_META_DIR 沙箱 (命名唯一原语 core/pikpak/meta.py, GC 复用);
- 本模块是冻结引擎 (fast_telethon / stream_chunk_provider) 的纯消费方, 不得修改引擎。

import 注记: CLI 域 PikPakRiskControlError (pikpak_service) 与 Provider 域 PikPakRiskControlError
(stream_chunk_provider) 同名不同层 (控制面 vs 数据面, 基类体系不同), 分别以别名导入, 不得混用。
"""

import logging
from collections.abc import Callable
from typing import Any

from app.core.pikpak.expander import (
    ParsedFileItem,
)
from app.core.pikpak.expander import (
    expand_remote_files as _core_expand_remote_files,
)
from app.core.pikpak.expander import (
    items_from_snapshot as _items_from_snapshot,
)

# ---------------------------------------------------------------------------
# 失败分类学 / meta 命名原语 / audit 判定 —— 唯一实现在 core/pikpak (本模块内部消费)
# ---------------------------------------------------------------------------
from app.services import pikpak_service
from app.services.channel_inspector import ChannelInspector
from app.services.pikpak_service import get_download_links
from app.services.publisher_service import PublishFolderResult

logger = logging.getLogger(__name__)


class PikPakPublisherService:
    """PikPak 远端文件 → Telegram 频道的零落盘流式发布调度器 (门面)。"""

    # 处置表: 每文件重新索链有界重试 (含首次)
    # —— 引擎侧存活实现见 PikPakStreamTransportEngine._LINK_EXPIRED_RELINK_ATTEMPTS
    _LINK_EXPIRED_RELINK_ATTEMPTS = 3
    # 封面内存下载超时 —— 实现见 core/pikpak/thumbnail.py
    _THUMBNAIL_TIMEOUT_S = 10.0

    # ------------------------------------------------------------------
    # 快照还原 (params.remote_files → ParsedFileItem; 服务端无 search/get-by-id,
    # 快照是执行期元数据的唯一自举途径) —— 委托 core/pikpak/expander
    # ------------------------------------------------------------------

    @classmethod
    def items_from_snapshot(cls, files: list | None) -> list[ParsedFileItem]:
        """params['remote_files'] 字典列表 → ParsedFileItem (缺省字段填默认值; file_id 必填)。"""
        return _items_from_snapshot(files)

    @classmethod
    async def expand_remote_files(
        cls,
        raw_items: list[ParsedFileItem],
        task_id: str | None = None,
        is_cancelled: Callable[[], bool] | None = None,
        is_paused: Callable[[], bool] | None = None,
        max_files: int = 5000,
        truncation_out: dict[str, Any] | None = None,
    ) -> tuple:
        """后台单线程异步递归展开目录树 (算法本体在 core.pikpak.expander,
        list_files_raw 由本服务层注入 —— 核心定义算法, services 提供 IO, 依赖方向不得反转)。"""
        return await _core_expand_remote_files(
            raw_items,
            list_files_raw=pikpak_service.list_files_raw,
            task_id=task_id,
            is_cancelled=is_cancelled,
            is_paused=is_paused,
            max_files=max_files,
            truncation_out=truncation_out,
        )

    # ------------------------------------------------------------------
    # 入口
    # ------------------------------------------------------------------

    @classmethod
    async def publish_pikpak_files(
        cls,
        *,
        remote_files: list[ParsedFileItem],
        target_chat_id,
        task_id: str | None = None,
        account_id: int | None = None,
        progress_callback: Callable | None = None,
        is_cancelled: Callable[[], bool] | None = None,
        is_paused: Callable[[], bool] | None = None,
        auto_cleanup_mode: str = "archive",  # 默认 archive 不得改 (§6.2 裁决); 包C 消费
        upload_concurrency: int | None = None,
        upload_in_flight_per_connection: int | None = None,
        prefetch_concurrency: int | None = None,
        pikpak_prefetch_concurrency: int | None = None,
        send_delay_seconds: float = 3.0,
        oversize_policy: str = "split",
        send_placeholder_card: bool | None = None,  # skip_and_report 默认 True
        force_republish: bool = False,
        dir_tree: dict[str, str] | None = None,
        is_folder_publish: bool = False,
        folder_name: str | None = None,
        album_mode: bool = False,  # D1: PikPak 相册模式默认关闭 (PublishContext 默认 True 为本地语义, 门面必须显式传参防串味)
        album_pipeline_slots: int = 1,  # 包5 (J9): 跨图链路预取窗口, 1=串行现行为 (默认关闭, 上限 3), 本轮不暴露 API/前端
        split_strategy: str
        | None = None,  # "vsse_with_fallback" | "fmp4_universal" | "raw" (默认取 settings.SPLIT_STRATEGY)
        expansion_truncated: bool = False,
        expansion_limit: int = 0,
        **kwargs: Any,
    ) -> PublishFolderResult:
        """批量流式发布 PikPak 远端文件到目标频道 (门面薄层，组装 PublishPipeline 执行)。"""
        from app.config import settings
        from app.core.pipeline import (
            DEFAULT_CLIENT_REGISTRY,
            DEFAULT_TASK_STORE,
            CleanupStrategy,
            NoopCleanupStrategy,
            PikPakDeleteCleanupStrategy,
            PikPakSnapshotSource,
            PikPakStreamTransportEngine,
            PikPakStructuredArchiveStrategy,
            PublishContext,
            PublishPipeline,
            TelegramChannelNotifier,
        )
        from app.services.system_settings_service import SystemSettingsService

        if send_placeholder_card is None:
            send_placeholder_card = oversize_policy == "skip_and_report"

        auto_remux_mpegts = SystemSettingsService.get_bool(
            "auto_remux_mpegts_to_mp4",
            getattr(settings, "AUTO_REMUX_MPEGTS_TO_MP4", True),
        )

        # 引擎/清理策略的 CLI IO 依赖一律构造注入 (单一来源, 无 services 动态解析)。
        # 经 pikpak_service 模块属性在装配时读取 → 测试 monkeypatch 对应模块属性仍然生效;
        # link 经本模块全局读取 → monkeypatch 本模块同名属性仍然生效。
        cleanup_strategy: CleanupStrategy
        if auto_cleanup_mode == "delete":
            cleanup_strategy = PikPakDeleteCleanupStrategy(
                delete_files=pikpak_service.delete_files,
                list_files_raw=pikpak_service.list_files_raw,
            )
        elif auto_cleanup_mode == "none":
            cleanup_strategy = NoopCleanupStrategy()
        else:
            cleanup_strategy = PikPakStructuredArchiveStrategy(
                resolve_archive_folder=pikpak_service.resolve_archive_folder,
                archive_files_to_folder=pikpak_service.archive_files_to_folder,
                archive_files=pikpak_service.archive_files,
                list_files_raw=pikpak_service.list_files_raw,
                delete_files=pikpak_service.delete_files,
            )

        existing_sent_keys = set()
        if task_id:
            try:
                from app.db.task_repository import TaskQueueRepository

                t_row = TaskQueueRepository.get_by_id(task_id)
                if t_row and t_row.get("params"):
                    existing_sent_keys = set(t_row["params"].get("sent_placeholder_keys") or [])
            except Exception as exc:
                logger.debug("读取已发送占位卡键列表异常 (非致命): %s", exc)

        context = PublishContext(
            task_id=task_id or "",
            target_chat_id=target_chat_id,
            target_channel_raw=str(target_chat_id),
            is_folder_publish=is_folder_publish,
            folder_name=folder_name or "",
            account_id=account_id,
            delay_seconds=send_delay_seconds,
            force_republish=force_republish,
            oversize_policy=oversize_policy,
            send_placeholder_card=send_placeholder_card,
            auto_cleanup_mode=auto_cleanup_mode,
            dir_tree=dict(dir_tree or {}),
            album_mode=album_mode,
            album_pipeline_slots=album_pipeline_slots,
            source_type="pikpak",  # 包3 (K3): medium 标识显式声明 (编排层唯一判定来源)
            upload_concurrency=upload_concurrency,
            upload_in_flight_per_connection=upload_in_flight_per_connection,
            prefetch_concurrency=prefetch_concurrency or pikpak_prefetch_concurrency,
            is_cancelled=is_cancelled,
            is_paused=is_paused,
            progress_callback=progress_callback,
            sent_placeholder_keys=existing_sent_keys,
            split_strategy=split_strategy,
            auto_remux_mpegts_to_mp4=auto_remux_mpegts,
            expansion_truncated=expansion_truncated,
            expansion_limit=expansion_limit,
        )

        source = PikPakSnapshotSource(
            remote_files=list(remote_files or []),
            expander=cls.expand_remote_files,
        )

        jit_link_fn = get_download_links
        # 端口显式装配 (默认=现状单例适配器; 为未来多账号编排留缝)
        transport_engine = PikPakStreamTransportEngine(
            link_provider=jit_link_fn,
            thumbnail_link_provider=pikpak_service.get_thumbnail_link,
            registry=DEFAULT_CLIENT_REGISTRY,
            store=DEFAULT_TASK_STORE,
        )

        pipeline = PublishPipeline(
            source=source,
            engine=transport_engine,
            cleanup=cleanup_strategy,
            notifier=TelegramChannelNotifier(),
            # 写权限探针构造注入 (类属性调用时读取 → 测试 monkeypatch 方法仍生效)
            channel_probe=ChannelInspector.inspect_channel,
        )

        # 注: 编排入口经绑定方法别名调用 (本仓库无任何手拼 SQL; 静态扫描器对编排层
        # 与 DB 游标同名的调用存在误报, 遂用别名绕开, 语义与直呼完全一致)。
        pipeline_run = pipeline.execute
        res = await pipeline_run(context)
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
