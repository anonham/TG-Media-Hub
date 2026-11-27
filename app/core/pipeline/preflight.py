"""统一发布流水线 Phase 1 Pre-flight (V2 自 pipeline 拆出 —— 编排层瘦身)

Target 规范化 / premium 快照 / 分卷阈值与并发计算 / 写权限前置熔断。
探针经 channel_probe 注入 (app/core 禁止依赖 services, test_layering AST 守卫);
探针异常与熔断处置分离 —— 归因日志失败不得吞熔断。
"""

import logging
from collections.abc import Callable
from typing import Any

from app.config import settings
from app.core.failure_logger import FailureLogger
from app.core.pikpak.taxonomy import PikPakPublishFatalError
from app.core.pipeline.models import PublishContext, PublishResult
from app.core.pipeline.utils import normalize_target_chat_id
from app.core.stream_splitter import derive_split_threshold
from app.db.account_repository import SystemSettingsRepository
from app.db.task_repository import TaskQueueRepository

logger = logging.getLogger(__name__)


async def phase1_preflight(
    context: PublishContext,
    is_pikpak: bool,
    channel_probe: Callable[..., Any] | None,
) -> PublishResult | None:
    """Phase 1: 返回 None = 预检通过继续; 返回 PublishResult = 本地权限熔断短路。"""
    if context.target_chat_id is not None and context.target_chat_id != "":
        context.target_chat_id = normalize_target_chat_id(context.target_chat_id)
        if not context.target_channel_raw:
            context.target_channel_raw = str(context.target_chat_id)

    from app.core.client_manager import client_manager

    explicit_account = context.account_id is not None
    if explicit_account:
        acc_id = int(context.account_id) if context.account_id is not None else 0
        acc = client_manager.get_account(acc_id) if acc_id > 0 else None
    else:
        acc = client_manager.get_account()
    if acc and acc.is_ready() and acc.client:
        context.userbot_client = acc.client
        if not explicit_account and getattr(acc, "account_id", None):
            context.account_id = acc.account_id
        await acc.probe_premium(force=True)
        context.is_premium = bool(acc.is_premium)
        context.task_limit_bytes = acc.effective_max_file_size_bytes()
        context.account_display = f"Userbot [{getattr(acc, 'account_name', context.account_id)}]"
    elif explicit_account:
        err_msg = (
            f"🛑 [账号预检失败] 指定执行账号 (ID={context.account_id}) 不存在、未登录或未就绪，"
            "已拒绝静默回退默认主账号。"
        )
        logger.warning(err_msg)
        if context.task_id:
            await TaskQueueRepository.append_log_async(context.task_id, err_msg)
        if is_pikpak:
            raise PikPakPublishFatalError("userbot", f"指定执行账号 (ID={context.account_id}) 不可用或未就绪")
        try:
            FailureLogger.log_failure(
                file_path=context.folder_path or context.folder_name or "",
                target_chat_id=context.target_chat_id,
                error_obj=RuntimeError(f"Requested account_id={context.account_id} unavailable"),
                client_type=f"Userbot [ID={context.account_id}]",
                task_id=context.task_id,
            )
        except Exception as log_err:
            logger.debug("账号预检失败归因日志写入失败 (不影响熔断): %s", log_err)
        return PublishResult(
            published_count=0,
            total_files_count=context.total_files_count,
            failed_files=[f"指定执行账号 (ID={context.account_id}) 不可用或未就绪"],
            is_folder_publish=context.is_folder_publish,
            folder_name=context.folder_name or "",
        )
    else:
        await client_manager.refresh_premium(force=True)
        context.userbot_client = client_manager.userbot
        if not context.account_id:
            default_acc = client_manager.get_account()
            default_id = getattr(default_acc, "account_id", None) if default_acc is not None else None
            if default_id:
                context.account_id = default_id
        context.is_premium = bool(client_manager.is_premium)
        context.task_limit_bytes = client_manager.effective_max_file_size_bytes()
        context.account_display = "Userbot [默认主账号]"

    if is_pikpak:
        active_acc = acc if acc is not None else client_manager.get_account(context.account_id)
        if context.userbot_client is None or (active_acc is not None and not active_acc.is_ready()):
            raise PikPakPublishFatalError("userbot")

    if is_pikpak:
        if not context.video_split_threshold_bytes:
            context.video_split_threshold_bytes = derive_split_threshold(context.is_premium)
        if not context.bot_routing_threshold_bytes:
            context.bot_routing_threshold_bytes = (
                SystemSettingsRepository.get_int(
                    "userbot_upload_threshold_mb", getattr(settings, "USERBOT_UPLOAD_THRESHOLD_MB", 30)
                )
                * 1024
                * 1024
            )
        if not context.task_limit_bytes:
            context.task_limit_bytes = (
                settings.PREMIUM_MAX_FILE_SIZE_BYTES if context.is_premium else settings.MAX_FILE_SIZE_BYTES
            )
        db_concurrency = SystemSettingsRepository.get_int(
            "pikpak_upload_concurrency", getattr(settings, "PIKPAK_UPLOAD_CONCURRENCY", 2)
        )
        db_in_flight = SystemSettingsRepository.get_int(
            "pikpak_upload_in_flight", getattr(settings, "PIKPAK_UPLOAD_IN_FLIGHT", 1)
        )
        is_explicit_concurrency = context.upload_concurrency is not None and not getattr(
            context.upload_concurrency, "_is_default_sentinel", False
        )
        val_c = None
        if is_explicit_concurrency and context.upload_concurrency is not None:
            try:
                val_c = max(1, min(int(context.upload_concurrency), 16))
            except (ValueError, TypeError):
                val_c = None
        if val_c is None:
            default_base = 4 if context.is_premium else 2
            val_c = (
                max(db_concurrency, default_base)
                if (context.is_premium and db_concurrency <= 2)
                else (db_concurrency or default_base)
            )
        context.upload_concurrency = max(1, min(int(val_c), 16))
        is_explicit_in_flight = context.upload_in_flight_per_connection is not None and not getattr(
            context.upload_in_flight_per_connection, "_is_default_sentinel", False
        )
        val_inf = None
        if is_explicit_in_flight and context.upload_in_flight_per_connection is not None:
            try:
                val_inf = max(1, min(int(context.upload_in_flight_per_connection), 4))
            except (ValueError, TypeError):
                val_inf = None
        context.upload_in_flight_per_connection = max(1, min(int(val_inf or db_in_flight or 1), 4))
        premium_banner = (
            f"👤 执行账号: {context.account_display} (Premium: {'✅ 是' if context.is_premium else '❌ 否'}) — "
            f"PikPak 分卷阈值 {context.video_split_threshold_bytes / (1024 * 1024):.0f} MiB, 上传并发 {context.upload_concurrency}×{context.upload_in_flight_per_connection}"
        )
    else:
        if not context.bot_routing_threshold_bytes:
            context.bot_routing_threshold_bytes = (
                SystemSettingsRepository.get_int(
                    "userbot_upload_threshold_mb", getattr(settings, "USERBOT_UPLOAD_THRESHOLD_MB", 30)
                )
                * 1024
                * 1024
            )
        if not context.video_split_threshold_bytes:
            context.video_split_threshold_bytes = derive_split_threshold(context.is_premium)
        if not context.task_limit_bytes:
            context.task_limit_bytes = (
                settings.PREMIUM_MAX_FILE_SIZE_BYTES if context.is_premium else settings.MAX_FILE_SIZE_BYTES
            )
        is_explicit_concurrency = context.upload_concurrency is not None and not getattr(
            context.upload_concurrency, "_is_default_sentinel", False
        )
        val_c = None
        if is_explicit_concurrency and context.upload_concurrency is not None:
            try:
                val_c = max(1, min(int(context.upload_concurrency), 16))
            except (ValueError, TypeError):
                val_c = None
        if val_c is None:
            default_base = 4 if context.is_premium else 2
            db_c = SystemSettingsRepository.get_int(
                "upload_parallel_connections", getattr(settings, "UPLOAD_PARALLEL_CONNECTIONS", 2)
            )
            val_c = max(db_c, default_base) if (context.is_premium and db_c <= 2) else (db_c or default_base)
        context.upload_concurrency = max(1, min(int(val_c or 2), 16))
        is_explicit_in_flight = context.upload_in_flight_per_connection is not None and not getattr(
            context.upload_in_flight_per_connection, "_is_default_sentinel", False
        )
        val_inf = None
        if is_explicit_in_flight and context.upload_in_flight_per_connection is not None:
            try:
                val_inf = max(1, min(int(context.upload_in_flight_per_connection), 4))
            except (ValueError, TypeError):
                val_inf = None
        if val_inf is None:
            val_inf = SystemSettingsRepository.get_int(
                "upload_in_flight_per_connection", getattr(settings, "UPLOAD_IN_FLIGHT_PER_CONNECTION", 2)
            )
        context.upload_in_flight_per_connection = max(1, min(int(val_inf or 2), 4))
        premium_banner = (
            f"👤 执行账号: {context.account_display} (Premium: {'✅ 是' if context.is_premium else '❌ 否'}) — "
            f"本次任务上传上限 {context.task_limit_bytes / (1024 * 1024):.0f} MiB"
        )

    logger.info(premium_banner)
    if context.task_id:
        await TaskQueueRepository.append_log_async(context.task_id, premium_banner)

    # 写权限探针 (channel_probe 由门面注入; 裸构造未注入时跳过 —— 仅测试路径)
    if channel_probe is None:
        logger.debug("PublishPipeline 未注入 channel_probe, 跳过写权限前置探针 (仅测试裸构造路径)")
        return None
    try:
        perm_check = await channel_probe(
            channel_identifier=str(context.target_chat_id),
            preset_type="target",
            account_id=context.account_id,
        )
    except PikPakPublishFatalError:
        raise
    except Exception as check_ex:
        logger.warning("目标频道发帖权限 Pre-flight 探测异常 (跳过硬熔断继续执行): %s", check_ex)
        return None

    if not perm_check.get("is_accessible", False) or not perm_check.get("has_write_permission", False):
        reason = perm_check.get("permission_summary") or perm_check.get("error_detail") or "缺少管理员发帖权限"
        if is_pikpak:
            abort_msg = (
                f"🛑 [PikPak 写权限前置熔断] 执行账号 ({context.account_display}) 对目标频道 "
                f"[{context.target_chat_id}] 无发帖权限: {reason}。任务已立即中止，避免无效拉流浪费！"
            )
            logger.warning(abort_msg)
            if context.task_id:
                await TaskQueueRepository.append_log_async(context.task_id, abort_msg)
            raise PikPakPublishFatalError("permission")

        abort_msg = (
            f"🛑 [写权限前置熔断] 执行账号 ({context.account_display}) 对目标频道 "
            f"[{context.target_chat_id}] 无发帖权限: {reason}。任务已立即中止，已为您避免无效的网络带宽上传浪费！"
        )
        logger.warning(abort_msg)
        if context.task_id:
            await TaskQueueRepository.append_log_async(context.task_id, abort_msg)
        try:
            FailureLogger.log_failure(
                file_path=context.folder_path or context.folder_name or "",
                target_chat_id=context.target_chat_id,
                error_obj=PermissionError(f"Chat admin privileges required: {reason}"),
                client_type=context.account_display,
                task_id=context.task_id,
            )
        except Exception as log_err:
            # 归因日志失败只降级记录, 绝不允许吞掉熔断本身
            logger.debug("熔断归因日志写入失败 (不影响熔断): %s", log_err)
        return PublishResult(
            published_count=0,
            total_files_count=context.total_files_count,
            failed_files=[f"目标频道权限不足: {reason}"],
            is_folder_publish=context.is_folder_publish,
            folder_name=context.folder_name or "",
        )
    return None
