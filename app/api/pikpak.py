"""PikPak 控制面路由。全部薄封装, 业务在 service / executor。

安全红线:
- 本文件不提供任何 token 修改 / 写 .env 端点 (路由集由 test_pikpak_api 固化枚举);
- 响应只透传安全字段, 下载直链 (get_download_links 产物) 永不进任何响应;
- 错误 detail 经 redact_text (防 CLI stderr 夹带敏感值)。
鉴权: 本 router 由 app/api/__init__.py 整组挂载双依赖 (verify_api_key + verify_2fa_session),
路由文件内零鉴权代码 —— 一处控制, 杜绝个别端点漏挂。
"""

import asyncio
import logging
import re
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException

from app.config import settings
from app.core.pikpak.taxonomy import (
    PikPakAuthError,
    PikPakCliTimeoutError,
    PikPakError,
    PikPakNotFoundError,
    PikPakQuotaError,
    audit_blocked_reason,
)
from app.core.redact import redact_text
from app.core.scheduler import task_scheduler
from app.models.common import TaskActionResponse
from app.models.pikpak import (
    PikPakAuthStatusResponse,
    PikPakDevicePollRequest,
    PikPakDevicePollResponse,
    PikPakDeviceStartResponse,
    PikPakFileItem,
    PikPakFileListResponse,
    PikPakInspectRequest,
    PikPakInspectResponse,
    PikPakPublishRequest,
    PikPakStorageSummary,
)
from app.services.pikpak_publisher_service import PikPakPublisherService
from app.services.pikpak_service import (
    DEFAULT_CLI_TIMEOUT_S,
    auth_status,
    classify_auth_failure,
    device_login_poll,
    device_login_start,
    invalidate_folder_cache,
    list_files_raw,
)
from app.services.system_settings_service import SystemSettingsService
from app.services.task_console_service import TaskConsoleService

logger = logging.getLogger(__name__)

router = APIRouter()

# 设备码轮询终止态 (CLI 返回 success 即成功; 其余终态值「待校准」—— 真实样本归档后修正, 不写死 UI 文案)
PIKPAK_DEVICE_TERMINAL_STATUSES = frozenset({"expired", "denied"})


def _pikpak_http_exc(e: Exception) -> HTTPException:
    """PikPakError → HTTP 状态码映射; detail 全文经 redact_text。"""
    if isinstance(e, PikPakAuthError):
        # 424 Failed Dependency: 明确标识为上游三方网盘凭据失效/未绑定，
        # 严禁返回 401 (401 专属于宿主 X-API-Key 门禁，避免前端 Axios 拦截器污染控制台凭据)
        status = 424
    elif isinstance(e, PikPakNotFoundError):
        status = 404
    elif isinstance(e, PikPakQuotaError):
        status = 429
    else:
        status = 502
    return HTTPException(status_code=status, detail=f"PikPak 错误: {redact_text(str(e))}")


def _to_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


_SIZE_RE = re.compile(r"^\s*([+-]?\d+(?:\.\d+)?)\s*([A-Za-z]+)?\s*$")

_SIZE_UNITS = {
    "b": 1,
    "byte": 1,
    "bytes": 1,
    "k": 1024,
    "kb": 1024,
    "kib": 1024,
    "m": 1024**2,
    "mb": 1024**2,
    "mib": 1024**2,
    "g": 1024**3,
    "gb": 1024**3,
    "gib": 1024**3,
    "t": 1024**4,
    "tb": 1024**4,
    "tib": 1024**4,
    "p": 1024**5,
    "pb": 1024**5,
    "pib": 1024**5,
}


def parse_size_to_bytes(val: Any) -> int | None:
    """健壮字节解析器: 支持 int/float/纯数字字符串及真实 CLI 带单位字符串 (如 '10.00 TB', '9.90 TB', '0 B', '512 MB')。"""
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        return max(0, int(val))
    if isinstance(val, str):
        text = val.strip()
        if not text:
            return None
        try:
            return max(0, int(text))
        except ValueError as e1:
            logger.debug("字符串无法直接转整型，尝试浮点转换: %s", e1)
        try:
            return max(0, int(float(text)))
        except ValueError as e2:
            logger.debug("字符串无法转浮点整型，尝试正则单位解析: %s", e2)
        match = _SIZE_RE.match(text)
        if match:
            num_str, unit = match.group(1), match.group(2)
            try:
                num = float(num_str)
                if num < 0:
                    return None
            except ValueError:
                return None
            if unit:
                mult = _SIZE_UNITS.get(unit.lower())
                if mult is None:
                    return None
                return int(num * mult)
            return int(num)
    return None


@router.get("/auth/status", response_model=PikPakAuthStatusResponse)
async def pikpak_auth_status(force: bool = False):
    """登录态三态查询 (恒 200): state=logged_in | not_bound | unreachable。

    认证域失败经探针交叉归因 (classify_auth_failure, memo 复用通常零 spawn): CLI 吐认证码
    但探针报网络/CLI 异常 → unreachable (网络瞬断被误报「未登录」缺陷的修复点);
    探针仍认证失败 → not_bound。transient/CLI 异常直达 → unreachable (错误本身即不可达
    实证, 不探针)。其他端点的 424/404/429/502 映射 (_pikpak_http_exc) 保持不变。"""
    token_source = "env" if settings.PIKPAK_TOKEN else "stored"
    try:
        data = await auth_status(force=force)
    except PikPakAuthError:
        verdict = await classify_auth_failure()
        if verdict == "transient":
            return PikPakAuthStatusResponse(
                logged_in=False,
                state="unreachable",
                state_reason="PikPak 暂时不可达 (探针网络/CLI 异常)",
                token_source=token_source,
            )
        return PikPakAuthStatusResponse(
            logged_in=False,
            state="not_bound",
            state_reason="PikPak 凭据域失败 (未绑定或会话已失效)",
            token_source=token_source,
        )
    except PikPakError as e:
        # transient 直达 / CLI 异常 → unreachable (原 502 双轨废除; 恒 200 三态契约)
        return PikPakAuthStatusResponse(
            logged_in=False,
            state="unreachable",
            state_reason=redact_text(str(e)),
            token_source=token_source,
        )
    storage = data.get("storage") or {}
    quotas = data.get("additional_quotas") or {}
    vip = data.get("vip") or {}
    return PikPakAuthStatusResponse(
        logged_in=bool(data.get("logged_in")),
        state="logged_in",
        state_reason=None,
        token_source=data.get("token_source") or ("env" if settings.PIKPAK_TOKEN else "stored"),
        user_name=data.get("name") or None,
        email=data.get("email") or None,
        vip_type=vip.get("type") if isinstance(vip, dict) else None,
        storage=PikPakStorageSummary(
            total_bytes=parse_size_to_bytes(storage.get("total")),
            used_bytes=parse_size_to_bytes(storage.get("used")),
            in_trash_bytes=parse_size_to_bytes(storage.get("in_trash")),
        )
        if storage
        else None,
        trash_duration_s=_to_int(quotas.get("trash_duration")),
    )


@router.post("/auth/device/start", response_model=PikPakDeviceStartResponse)
async def pikpak_device_start():
    """设备码登录第一步: 取 device_code/user_code (CLI 输出后即退出, 不自动轮询)。"""
    try:
        data = await device_login_start()
    except PikPakError as e:
        raise _pikpak_http_exc(e) from e
    interval = _to_int(data.get("interval")) or 5
    return PikPakDeviceStartResponse(
        device_code=str(data.get("device_code")),
        verification_uri=str(data.get("verification_uri")),
        user_code=str(data.get("user_code")),
        expires_in=_to_int(data.get("expires_in")),
        interval=interval,
    )


@router.post("/auth/device/poll", response_model=PikPakDevicePollResponse)
async def pikpak_device_poll(req: PikPakDevicePollRequest):
    """设备码登录第二步: 有界服务端轮询循环 (节奏/超时/终止判定在 API 层驱动)。
    success / 终止态 / 超时三种出口; 超时不视为失败, 前端可携带同 device_code 继续调用。"""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + req.max_wait_s
    last_status = ""
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            return PikPakDevicePollResponse(status=last_status, logged_in=False, timed_out=True)
        call_timeout = min(DEFAULT_CLI_TIMEOUT_S, remaining)
        try:
            data = await device_login_poll(req.device_code, timeout_s=call_timeout)
        except PikPakCliTimeoutError as e:
            if remaining <= DEFAULT_CLI_TIMEOUT_S or loop.time() >= deadline:
                return PikPakDevicePollResponse(status=last_status, logged_in=False, timed_out=True)
            raise _pikpak_http_exc(e) from e
        except PikPakError as e:
            raise _pikpak_http_exc(e) from e
        last_status = str(data.get("status") or "")
        if last_status == "success":
            account_raw = data.get("account")
            account = account_raw if isinstance(account_raw, dict) else (data if isinstance(data, dict) else {})
            return PikPakDevicePollResponse(
                status="success",
                logged_in=True,
                user_name=account.get("name") or None,
                email=account.get("email") or None,
            )
        if last_status in PIKPAK_DEVICE_TERMINAL_STATUSES:
            return PikPakDevicePollResponse(status=last_status, logged_in=False, timed_out=False)
        remaining = deadline - loop.time()
        if remaining <= 0:
            return PikPakDevicePollResponse(status=last_status, logged_in=False, timed_out=True)
        await asyncio.sleep(min(req.interval_s, remaining))


@router.get("/files/list", response_model=PikPakFileListResponse)
async def pikpak_file_list(
    folder_id: str = "",
    page_token: str | None = None,
    refresh: bool = False,
):
    """逐级目录浏览 (服务端已禁用 search, 无全局搜索); next_page_token 透传分页。

    refresh: 为 True 时强制失效当前目录 LRU/TTL 分页缓存并穿透调用 CLI，回源最新快照。
    """
    if refresh:
        invalidate_folder_cache(folder_id)
    try:
        items, next_token = await list_files_raw(folder_id, page_token, use_cache=not refresh)
    except PikPakError as e:
        raise _pikpak_http_exc(e) from e
    return PikPakFileListResponse(
        items=[
            PikPakFileItem(
                file_id=i.file_id,
                name=i.name,
                is_dir=i.is_dir,
                size_bytes=i.size_bytes,
                audit=i.audit or {},
                thumbnail_link=i.thumbnail_link,
                duration_s=i.duration_s,
                width=i.width,
                height=i.height,
                parent_id=i.parent_id,
            )
            for i in items
        ],
        next_page_token=next_token,
    )


@router.post("/inspect", response_model=PikPakInspectResponse)
async def pikpak_inspect(req: PikPakInspectRequest):
    """提交前预检: 目标频道写权限探测 + 选中文件 audit 汇总 (UI 标红数据源)。"""
    items = PikPakPublisherService.items_from_snapshot([f.model_dump() for f in req.remote_files])
    blocked_names = []
    for item in items:
        reason = audit_blocked_reason(item)
        if reason:
            blocked_names.append(item.name or item.file_id)

    perm = {}
    try:
        from app.services.channel_inspector import ChannelInspector

        perm = await ChannelInspector.inspect_channel(
            channel_identifier=str(req.target_channel),
            preset_type="target",
            account_id=req.account_id,
        )
    except Exception as ex:
        logger.warning("PikPak inspect 频道探测异常 (返回未授权态由前端拦截提交): %s", ex)

    return PikPakInspectResponse(
        accessible=bool(perm.get("is_accessible", False)),
        has_write_permission=bool(perm.get("has_write_permission", False)),
        chat_id=perm.get("parsed_chat_id") or perm.get("chat_id") or None,
        channel_title=perm.get("channel_title") or perm.get("title") or None,
        username=perm.get("channel_username") or perm.get("username") or None,
        permission_summary=perm.get("permission_summary") or perm.get("error_detail") or None,
        files_total=len(items),
        files_audit_blocked=len(blocked_names),
        audit_blocked_names=blocked_names,
        total_size_bytes=sum(i.size_bytes for i in items),
        warning="存在审核状态未通过的文件，提交后将被跳过" if blocked_names else None,
    )


@router.post("/publish", response_model=TaskActionResponse)
async def pikpak_publish(req: PikPakPublishRequest):
    """提交 PikPak 流式发布任务:
    落库 task_type='pikpak_publish' → 通知调度器扫描队列执行。"""
    task_id = str(uuid.uuid4())[:8]
    task_params = {
        "target_channel": req.target_channel,
        "remote_files": [f.model_dump() for f in req.remote_files],
        "auto_cleanup_mode": req.auto_cleanup_mode,
        "oversize_policy": req.oversize_policy,
        "send_placeholder_card": req.send_placeholder_card,
        "send_delay_seconds": req.send_delay_seconds,
        "force_republish": req.force_republish,
        "is_folder_publish": req.is_folder_publish,
        "folder_name": req.folder_name,
        "dir_tree": req.dir_tree or {},
        "album_mode": req.album_mode,
    }
    if req.upload_concurrency is not None:
        task_params["upload_concurrency"] = req.upload_concurrency
    if req.upload_in_flight_per_connection is not None:
        task_params["upload_in_flight_per_connection"] = req.upload_in_flight_per_connection
    prefetch_concurrency = req.pikpak_prefetch_concurrency or req.prefetch_concurrency
    if prefetch_concurrency is not None:
        task_params["pikpak_prefetch_concurrency"] = prefetch_concurrency

    split_strategy: str = (
        req.split_strategy
        if req.split_strategy
        else SystemSettingsService.get_str("pikpak_default_split_strategy", "vsse_with_fallback")
    )
    task_params["split_strategy"] = split_strategy

    default_concurrency = SystemSettingsService.get_int("pikpak_upload_concurrency", settings.PIKPAK_UPLOAD_CONCURRENCY)
    initial_files = [f for f in req.remote_files if not f.is_dir]
    initial_total_bytes = sum(int(f.size_bytes or 0) for f in initial_files) if initial_files else 0
    initial_total_items = len(initial_files)
    initial_current_item = f"[0/{initial_total_items}] 等待调度启动..." if initial_files else ""

    TaskConsoleService.create_task(
        task_id=task_id,
        task_type="pikpak_publish",
        params=task_params,
        account_id=req.account_id,
        priority=req.priority if req.priority is not None else 10,
        concurrency_total=req.upload_concurrency or default_concurrency,
        total_bytes=initial_total_bytes,
        total_items=initial_total_items,
        current_item=initial_current_item,
    )
    # D4: 派发统一 —— 任务落库后一律经调度器调度; 原 BackgroundTasks 即跑兜底通道已废除
    task_scheduler.notify_new_task()
    return {"status": "ok", "task_id": task_id}
