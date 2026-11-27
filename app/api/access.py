"""本机访问豁免仅用于回环直连部署，公网反代必须显式禁用豁免。"""

import ipaddress

from fastapi import Request, WebSocket

from app.config import settings

# 转发头是额外的拒绝信号，公网部署的边界由 CONSOLE_REQUIRE_API_KEY 确立。
_PROXY_MARKING_HEADERS = ("x-forwarded-for", "x-forwarded-proto", "x-real-ip", "forwarded")


def bind_is_loopback() -> bool:
    """进程配置的绑定地址是否为回环入口。"""
    host = (settings.HOST or "").strip().lower()
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _unwrap_client_ip(host: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    """解析对端 IP; IPv4-mapped IPv6 (::ffff:a.b.c.d, uvicorn 双栈 bind 的回环形态) 解包为 IPv4。"""
    try:
        ip = ipaddress.ip_address(host.strip())
    except ValueError:
        return None
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        return ip.ipv4_mapped
    return ip


def is_local_request(request: Request | WebSocket) -> bool:
    """HTTP 和 WS 共用本机豁免规则，未知来源按公网规则处理。"""
    if settings.CONSOLE_REQUIRE_API_KEY or not bind_is_loopback():
        return False
    if any(name in request.headers for name in _PROXY_MARKING_HEADERS):
        return False
    client = request.client
    host = client.host if client is not None else None
    ip = _unwrap_client_ip(host) if isinstance(host, str) else None
    return ip is not None and ip.is_loopback


def console_locked_for_request(request: Request | WebSocket) -> bool:
    """公网入口未配置 API_KEY 时的控制台锁定判定 (提示部署者完成配置)。"""
    return not settings.API_KEY and not is_local_request(request)
