import math
from typing import Any

from fastapi import APIRouter, HTTPException

from app import APP_VERSION
from app.core.scheduler import task_scheduler
from app.models.settings import (
    SystemSettingItem,
    SystemSettingsGroupedResponse,
    SystemSettingUpdateRequest,
    TransitVerifyHealthResponse,
)
from app.services.system_settings_service import SystemSettingsService

router = APIRouter()


@router.get("", response_model=SystemSettingsGroupedResponse)
async def get_settings():
    """获取当前系统所有动态配置项，按分类结构化返回"""
    data = SystemSettingsService.get_all_grouped()
    grouped_categories = {}
    for cat, items in data["categories"].items():
        grouped_categories[cat] = [
            SystemSettingItem(
                key=it["key"],
                value=it["value"],
                category=it["category"],
                description=it.get("description"),
                updated_at=it["updated_at"],
            )
            for it in items
        ]

    return SystemSettingsGroupedResponse(
        categories=grouped_categories, all_settings=data["all_settings"], app_version=APP_VERSION
    )


_INT_SETTING_SPECS: dict[str, tuple[int, int | None, str]] = {
    "max_global_concurrency": (1, 20, "max_global_concurrency 必须为 1 到 20 之间的整数"),
    "userbot_upload_threshold_mb": (1, None, "userbot_upload_threshold_mb 必须为大于等于 1 的整数"),
    "transit_min_file_size_mb": (0, None, "transit_min_file_size_mb 必须为大于等于 0 的整数"),
    "download_parallel_connections": (1, 32, "download_parallel_connections 必须为 1 到 32 之间的整数"),
    "upload_parallel_connections": (1, 16, "upload_parallel_connections 必须为 1 到 16 之间的整数"),
    "pikpak_upload_concurrency": (1, 16, "pikpak_upload_concurrency 必须为 1 到 16 之间的整数"),
    "pikpak_upload_in_flight": (1, 4, "pikpak_upload_in_flight 必须为 1 到 4 之间的整数"),
    "pikpak_ring_buffer_mb": (10, 1024, "pikpak_ring_buffer_mb 必须为 10 到 1024 之间的整数"),
    "pikpak_prefetch_concurrency": (1, 16, "pikpak_prefetch_concurrency 必须为 1 到 16 之间的整数"),
}


_ENUM_SETTING_SPECS: dict[str, tuple[tuple[str, ...], str]] = {
    "pikpak_default_cleanup_mode": (
        ("none", "archive", "delete"),
        "pikpak_default_cleanup_mode 必须为 none / archive / delete 之一",
    ),
    "pikpak_default_oversize_policy": (
        ("skip_and_report", "split"),
        "pikpak_default_oversize_policy 必须为 skip_and_report / split 之一",
    ),
    "pikpak_default_split_strategy": (
        ("vsse_with_fallback", "fmp4_universal", "raw"),
        "pikpak_default_split_strategy 必须为 vsse_with_fallback / fmp4_universal / raw 之一",
    ),
}

_BOOL_SETTING_SPECS: dict[str, str] = {
    "auto_remux_mpegts_to_mp4": "auto_remux_mpegts_to_mp4 必须为 true 或 false",
}


def _parse_strict_int(v: Any) -> int:
    """严格解析整型配置值：拒绝布尔值、浮点数（含 1.5/NaN）与空白/小数字符串。"""
    if isinstance(v, (bool, float)):
        raise ValueError("boolean or float is not allowed for integer setting")
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            raise ValueError("empty string")
        return int(s)
    raise ValueError("unsupported type")


def _normalize_bool_setting(v: Any, err_msg: str) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, str) and v.strip().lower() in ("true", "false"):
        return v.strip().lower()
    raise HTTPException(status_code=400, detail=err_msg)


def _normalize_float_setting(v: Any, err_msg: str) -> float:
    try:
        if isinstance(v, bool):
            raise ValueError()
        val_float = float(v)
        if not math.isfinite(val_float) or val_float < 0:
            raise ValueError()
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail=err_msg) from None
    return val_float


def _normalize_proxy_url_setting(v: Any) -> str:
    proxy_str = str(v).strip() if v is not None else ""
    if proxy_str:
        from urllib.parse import urlparse

        parsed = urlparse(proxy_str)
        if parsed.scheme.lower() not in ("http", "https", "socks5", "socks5h") or not parsed.netloc:
            raise HTTPException(
                status_code=400,
                detail="pikpak_proxy_url 格式无效，必须以 http://, https:// 或 socks5:// 开头且包含主机与端口，或留空",
            )
    return proxy_str


def _validate_and_normalize_settings(raw_settings: dict[str, Any]) -> dict[str, Any]:
    """校验并规范化系统动态配置更新字典，返回可直接持久化的数据副本。"""
    normalized: dict[str, Any] = dict(raw_settings)
    for k, v in raw_settings.items():
        if k in _INT_SETTING_SPECS:
            min_val, max_val, err_msg = _INT_SETTING_SPECS[k]
            try:
                val_int = _parse_strict_int(v)
                if val_int < min_val or (max_val is not None and val_int > max_val):
                    raise ValueError()
            except (ValueError, TypeError):
                raise HTTPException(status_code=400, detail=err_msg) from None
            normalized[k] = val_int
        elif k == "max_storage_quota_gb":
            normalized[k] = _normalize_float_setting(v, "max_storage_quota_gb 必须为大于等于 0 的有限数字")
        elif k == "pikpak_proxy_url":
            normalized[k] = _normalize_proxy_url_setting(v)
        elif k in _ENUM_SETTING_SPECS:
            allowed, err_msg = _ENUM_SETTING_SPECS[k]
            if v not in allowed:
                raise HTTPException(status_code=400, detail=err_msg)
        elif k in _BOOL_SETTING_SPECS:
            normalized[k] = _normalize_bool_setting(v, _BOOL_SETTING_SPECS[k])
    return normalized


@router.put("", response_model=SystemSettingsGroupedResponse)
async def update_settings(req: SystemSettingUpdateRequest):
    """批量或单项更新系统动态配置 (支持热生效响应)"""
    if not req.settings:
        raise HTTPException(status_code=400, detail="配置更新字典不能为空")

    normalized = _validate_and_normalize_settings(req.settings)
    try:
        SystemSettingsService.set_batch(normalized)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # 若修改了 pikpak_proxy_url，立即失效控制面 CDN 传输层连接池以应用新代理
    if "pikpak_proxy_url" in req.settings:
        from app.core.cdn_transport import aclose_cdn_session

        await aclose_cdn_session()

    # 若全局并发发生变更，立即通知调度器唤醒
    if "max_global_concurrency" in req.settings:
        task_scheduler.notify_new_task()

    return await get_settings()


@router.post("/verify-transit", response_model=TransitVerifyHealthResponse)
async def verify_transit_settings():
    """触发中转群健康度检查探针，验证主 Userbot 与各 Bot 的群连通性"""
    from app.services.transit_bridge_service import TransitBridgeService

    report = await TransitBridgeService.verify_transit_group_health()
    return report
