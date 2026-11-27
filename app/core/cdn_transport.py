"""控制面统一 HTTP 传输层 (CDN Transport).

提供针对 PikPak CDN 直链的控制面 HTTP 会话单例管理器、流式与探活超时 Profile、
代理隔离与热生效、以及生命周期受控的优雅关停。

设计原则:
1. 双检锁单例 (asyncio.Lock): 保证并发安全与连接池复用;
2. 控制面代理隔离: trust_env=False, 严防宿主机环境变量 HTTP_PROXY/HTTPS_PROXY 污染控制面;
3. 专享代理注入: 联动系统配置 pikpak_proxy_url (支持 http, https, socks5, socks5h);
4. 双超时 Profile:
   - cdn_bulk_timeout: 用于大块数据流式拉取 (moov/cues/tail), total=None 允许大文件持续传输,
     仅限制 sock_connect 与 sock_read (防假死);
   - cdn_liveness_timeout: 用于探活 (precheck)、短资源 (HEAD/thumbnail), 施加硬总预算 total (防慢速滴流).
"""

import asyncio
import logging
import ssl
import sys
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlparse

import aiohttp

from app.config import settings

logger = logging.getLogger(__name__)


@dataclass
class CdnTransportState:
    """控制面 HTTP 连接池运行态容器 (替代裸 global 变量修改)"""

    session: aiohttp.ClientSession | None = None
    lock: asyncio.Lock | None = None
    session_proxy: str | None = None


cdn_transport_state = CdnTransportState()
_cdn_session: aiohttp.ClientSession | None = None
_cdn_lock: asyncio.Lock | None = None
_cdn_session_proxy: str | None = None


def _sync_cdn_module_attrs(
    session: aiohttp.ClientSession | None,
    lock: asyncio.Lock | None,
    proxy: str | None,
) -> None:
    cdn_transport_state.session = session
    cdn_transport_state.lock = lock
    cdn_transport_state.session_proxy = proxy
    mod: Any = sys.modules[__name__]
    mod._cdn_session = session
    mod._cdn_lock = lock
    mod._cdn_session_proxy = proxy


def cdn_bulk_timeout(sock_read: float = 15.0, connect: float = 5.0) -> aiohttp.ClientTimeout:
    """大块 Range 拉取: 允许持续传输, 仅约束建连/连接池等待与单次读取 (moov / cues / tail)."""
    return aiohttp.ClientTimeout(total=None, connect=connect, sock_connect=connect, sock_read=sock_read)


def cdn_liveness_timeout(total: float = 10.0, connect: float = 5.0) -> aiohttp.ClientTimeout:
    """存活探测与短资源: 硬总预算 (precheck / HEAD / thumbnail), 杜绝慢速滴流挂死."""
    return aiohttp.ClientTimeout(total=total, sock_connect=connect, sock_read=total)


def _get_effective_proxy_url() -> str | None:
    """获取当前生效的 pikpak_proxy_url (系统数据库配置优先于静态 settings)."""
    try:
        from app.db.account_repository import SystemSettingsRepository

        proxy_val = SystemSettingsRepository.get_str(
            "pikpak_proxy_url", getattr(settings, "PIKPAK_PROXY_URL", "") or ""
        )
    except Exception:
        proxy_val = getattr(settings, "PIKPAK_PROXY_URL", "") or ""
    clean = str(proxy_val).strip() if proxy_val else ""
    return clean or None


def _build_proxy_connector(proxy_url: str) -> aiohttp.BaseConnector:
    """构建控制面代理连接器: 原生兼容 http/https/socks4/socks5/socks5h."""
    from aiohttp_socks import ProxyConnector, ProxyType

    parsed = urlparse(proxy_url)
    scheme = (parsed.scheme or "").lower()
    common_kwargs: dict[str, Any] = {
        "limit": 32,
        "limit_per_host": 16,
        "keepalive_timeout": 60.0,
        "ttl_dns_cache": 300,
    }
    if scheme in ("socks5h", "https"):
        host = parsed.hostname
        if not host:
            raise ValueError(f"Empty host component in proxy URL: {proxy_url}")
        port = parsed.port
        if port is None and scheme == "https":
            port = 443
        if port is None:
            raise ValueError(f"Invalid port component in proxy URL: {proxy_url}")
        username = unquote(parsed.username) if parsed.username is not None else None
        password = unquote(parsed.password) if parsed.password is not None else None
        if scheme == "socks5h":
            return ProxyConnector(
                proxy_type=ProxyType.SOCKS5,
                host=host,
                port=port,
                username=username,
                password=password,
                rdns=True,
                **common_kwargs,
            )
        return ProxyConnector(
            proxy_type=ProxyType.HTTP,
            host=host,
            port=port,
            username=username,
            password=password,
            proxy_ssl=ssl.create_default_context(),
            **common_kwargs,
        )
    return ProxyConnector.from_url(proxy_url, **common_kwargs)


async def get_cdn_session() -> aiohttp.ClientSession:
    """获取全局控制面 aiohttp.ClientSession 单例 (双检锁懒加载与代理热重载)."""
    target_proxy = _get_effective_proxy_url()

    # 快速路径: 会话已存在、未关闭且绑定的代理与当前配置一致
    if _cdn_session is not None and not _cdn_session.closed and _cdn_session_proxy == target_proxy:
        return _cdn_session

    lock = _cdn_lock
    if lock is None:
        lock = asyncio.Lock()
        _sync_cdn_module_attrs(_cdn_session, lock, _cdn_session_proxy)

    async with lock:
        session = _cdn_session
        proxy = _cdn_session_proxy
        # 双检判断 1: 检查是否发生代理漂移 (配置被修改)，若漂移则主动关停并重建
        if session is not None and not session.closed and proxy != target_proxy:
            logger.info("检测到 CDN 传输层代理变更, 正在重构连接池")
            await session.close()
            session = None
            _sync_cdn_module_attrs(None, lock, proxy)

        # 双检判断 2: 懒加载创建连接池与会话
        if session is None or session.closed:
            connector: aiohttp.BaseConnector
            if target_proxy:
                connector = _build_proxy_connector(target_proxy)
            else:
                connector = aiohttp.TCPConnector(
                    limit=32,
                    limit_per_host=16,
                    keepalive_timeout=60.0,
                    ttl_dns_cache=300,
                )

            session = aiohttp.ClientSession(
                connector=connector,
                timeout=cdn_liveness_timeout(),
                trust_env=False,  # 控制面代理隔离红线: 严防环境变量污染
            )
            _sync_cdn_module_attrs(session, lock, target_proxy)

        return session


async def aclose_cdn_session() -> None:
    """生命周期优雅异步关停 (幂等)."""
    lock = _cdn_lock
    if lock is not None:
        async with lock:
            if _cdn_session is not None and not _cdn_session.closed:
                await _cdn_session.close()
            _sync_cdn_module_attrs(None, lock, None)
    else:
        if _cdn_session is not None and not _cdn_session.closed:
            await _cdn_session.close()
        _sync_cdn_module_attrs(None, None, None)


def close_cdn_session_sync() -> None:
    """生命周期同步安全关停 (专用于测试 teardown 与同步清理, 杜绝 Unclosed session 警告)."""
    if _cdn_session is not None and not _cdn_session.closed:
        session = _cdn_session
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            loop.create_task(session.close())
        else:
            try:
                asyncio.run(session.close())
            except Exception as exc:
                logger.debug("同步安全关停控制面 CDN 会话异常 (非致命): %s", exc)
    _sync_cdn_module_attrs(None, None, None)
