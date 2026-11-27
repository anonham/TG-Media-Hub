"""控制面统一 CDN 传输层 (app.core.cdn_transport) 专项测试套件.

覆盖范围:
1. trust_env=False 宿主机环境变量物理隔离 (安全红线);
2. 基于 aiohttp-socks 的 HTTP / SOCKS5 代理连接池装配;
3. 数据库 pikpak_proxy_url 配置变更后的连接池主动与被动漂移重载;
4. 资源生命周期: aclose_cdn_session 与 close_cdn_session_sync 优雅安全关停;
5. 系统设置 API 更新 pikpak_proxy_url 时的会话热失效机制。
"""

import logging
from unittest.mock import patch

import aiohttp
import pytest
from _async_wait import wait_until

import app.core.cdn_transport as ct
from app.core.cdn_transport import (
    cdn_bulk_timeout,
    cdn_liveness_timeout,
    close_cdn_session_sync,
    get_cdn_session,
)


@pytest.mark.asyncio
async def test_cdn_session_trust_env_isolation(monkeypatch):
    """验证控制面代理隔离红线: 宿主机环境变量绝对禁止污染控制面 ClientSession."""
    # 模拟宿主机被注入全局代理环境变量
    monkeypatch.setenv("HTTP_PROXY", "http://evil-proxy.internal:8080")
    monkeypatch.setenv("HTTPS_PROXY", "http://evil-proxy.internal:8080")
    monkeypatch.setenv("ALL_PROXY", "socks5://evil-proxy.internal:1080")
    monkeypatch.setenv("http_proxy", "http://evil-proxy.internal:8080")
    monkeypatch.setenv("https_proxy", "http://evil-proxy.internal:8080")
    monkeypatch.setenv("all_proxy", "socks5://evil-proxy.internal:1080")

    # 确保数据库中未配置 pikpak_proxy_url
    monkeypatch.setattr(
        "app.db.account_repository.SystemSettingsRepository.get_str",
        lambda key, default="": "" if key == "pikpak_proxy_url" else default,
    )

    session = await get_cdn_session()
    assert session is not None
    assert not session.closed

    # 核心安全红线断言
    assert session._trust_env is False
    assert isinstance(session.connector, aiohttp.TCPConnector)
    assert ct._cdn_session_proxy is None


@pytest.mark.asyncio
async def test_cdn_session_http_proxy_connector(monkeypatch):
    """验证配置 HTTP 代理时，正确采用 ProxyConnector 注入连接池."""
    target_proxy = "http://127.0.0.1:7890"
    monkeypatch.setattr(
        "app.db.account_repository.SystemSettingsRepository.get_str",
        lambda key, default="": target_proxy if key == "pikpak_proxy_url" else default,
    )

    session = await get_cdn_session()
    assert session is not None
    assert not session.closed
    assert session._trust_env is False
    assert ct._cdn_session_proxy == target_proxy

    from aiohttp_socks import ProxyConnector

    assert isinstance(session.connector, ProxyConnector)
    assert session.connector.limit == 32
    assert session.connector.limit_per_host == 16


@pytest.mark.asyncio
async def test_cdn_session_socks5_proxy_connector(monkeypatch):
    """验证配置 SOCKS5 代理时，原生支持并通过 ProxyConnector 建立连接池."""
    target_proxy = "socks5://127.0.0.1:1080"
    monkeypatch.setattr(
        "app.db.account_repository.SystemSettingsRepository.get_str",
        lambda key, default="": target_proxy if key == "pikpak_proxy_url" else default,
    )

    session = await get_cdn_session()
    assert session is not None
    assert not session.closed
    assert ct._cdn_session_proxy == target_proxy

    from aiohttp_socks import ProxyConnector

    assert isinstance(session.connector, ProxyConnector)
    assert session.connector.limit == 32
    assert session.connector.limit_per_host == 16


@pytest.mark.asyncio
async def test_cdn_session_proxy_drift_hot_reload(monkeypatch):
    """验证代理配置变更 (漂移) 时，get_cdn_session 自动关闭旧连接池并重载新连接池."""
    current_proxy = ""

    def mock_get_str(key, default=""):
        if key == "pikpak_proxy_url":
            return current_proxy
        return default

    monkeypatch.setattr(
        "app.db.account_repository.SystemSettingsRepository.get_str",
        mock_get_str,
    )

    # 1. 首次获取: 无代理直连
    session_1 = await get_cdn_session()
    assert isinstance(session_1.connector, aiohttp.TCPConnector)
    assert ct._cdn_session_proxy is None

    # 2. 模拟用户通过 Web UI / DB 修改为专享 HTTP 代理
    current_proxy = "http://127.0.0.1:8888"

    # 3. 再次获取: 自动感知漂移并轮转会话
    session_2 = await get_cdn_session()
    assert session_1.closed is True
    assert session_2 is not session_1
    assert session_2.closed is False
    assert ct._cdn_session_proxy == "http://127.0.0.1:8888"
    from aiohttp_socks import ProxyConnector

    assert isinstance(session_2.connector, ProxyConnector)

    # 4. 再次修改为 SOCKS5 代理
    current_proxy = "socks5://127.0.0.1:1088"
    session_3 = await get_cdn_session()
    assert session_2.closed is True
    assert session_3 is not session_2
    assert ct._cdn_session_proxy == "socks5://127.0.0.1:1088"
    assert isinstance(session_3.connector, ProxyConnector)


def test_close_cdn_session_sync_safety():
    """同步单元测试场景: 验证在纯同步上下文无运行 loop 时调用 close_cdn_session_sync 绝对安全."""
    close_cdn_session_sync()
    assert ct._cdn_session is None
    assert ct._cdn_lock is None
    assert ct._cdn_session_proxy is None


@pytest.mark.asyncio
async def test_close_cdn_session_sync_in_running_loop():
    """异步测试场景: 验证在运行中事件循环内调用 close_cdn_session_sync 能安全调度释放."""
    session = await get_cdn_session()
    assert session is not None
    assert not session.closed

    # 执行同步安全关停
    close_cdn_session_sync()
    assert ct._cdn_session is None
    assert ct._cdn_lock is None
    assert ct._cdn_session_proxy is None

    # 有界轮询等待 loop.create_task 排空的 session.close() 完成
    await wait_until(lambda: session.closed is True, message="CDN session was not closed after sync shutdown")
    assert session.closed is True


def test_cdn_timeouts_profile():
    """验证两套超时 Profile 的契约参数."""
    bulk = cdn_bulk_timeout(sock_read=20.0, connect=6.0)
    assert bulk.total is None
    assert bulk.sock_read == 20.0
    assert bulk.sock_connect == 6.0

    liveness = cdn_liveness_timeout(total=12.0, connect=4.0)
    assert liveness.total == 12.0
    assert liveness.sock_read == 12.0
    assert liveness.sock_connect == 4.0


@pytest.mark.asyncio
async def test_settings_api_proxy_update_triggers_invalidation(monkeypatch):
    """验证设置接口更新 pikpak_proxy_url 时主动失效全局 CDN 传输层."""
    from app.api.settings import SystemSettingUpdateRequest, update_settings

    # 预设活跃会话
    session = await get_cdn_session()
    assert ct._cdn_session is not None

    req = SystemSettingUpdateRequest(settings={"pikpak_proxy_url": "http://127.0.0.1:9090"})

    with patch("app.db.account_repository.SystemSettingsRepository.set_batch"):
        with patch("app.api.settings.get_settings", return_value={"general": {}}):
            await update_settings(req)

    # 验证旧 session 已被 aclose_cdn_session 关闭并置 None
    assert ct._cdn_session is None
    assert session.closed is True


@pytest.mark.asyncio
async def test_cdn_proxy_drift_log_omits_proxy_urls(caplog):
    """验证代理配置变更重构连接池时，日志不输出含认证信息的代理 URL 原文."""
    with patch.object(ct, "_get_effective_proxy_url", return_value="http://u1:secretA@127.0.0.1:8080"):
        first_session = await get_cdn_session()
        assert first_session is not None

    with (
        caplog.at_level(logging.INFO, logger=ct.__name__),
        patch.object(ct, "_get_effective_proxy_url", return_value="http://u2:secretB@127.0.0.1:8081"),
    ):
        second_session = await get_cdn_session()
        assert second_session is not first_session

    assert any("检测到 CDN 传输层代理变更" in r.message for r in caplog.records)
    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "secretA" not in joined
    assert "secretB" not in joined
    assert "127.0.0.1:8080" not in joined
    assert "127.0.0.1:8081" not in joined
    await ct.aclose_cdn_session()


@pytest.mark.asyncio
async def test_cdn_bulk_timeout_sets_connect_and_preserves_total_none():
    """#1: cdn_bulk_timeout 须设置 connect 限制连接池等待, 同时保持 total=None."""
    bulk = cdn_bulk_timeout(sock_read=15.0, connect=5.0)
    assert bulk.total is None
    assert bulk.connect == 5.0
    assert bulk.sock_connect == 5.0
    assert bulk.sock_read == 15.0


@pytest.mark.asyncio
async def test_cdn_session_supports_https_and_socks5h_proxy_schemes(monkeypatch):
    """#2: 专享代理配置 https:// 与 socks5h:// 须正确装配 ProxyConnector 而非抛 ValueError."""
    from aiohttp_socks import ProxyConnector, ProxyType

    for proxy_url, expected_type, expect_rdns, expect_ssl in [
        ("socks5h://u:p@127.0.0.1:1080", ProxyType.SOCKS5, True, False),
        ("https://u:p@127.0.0.1:8443", ProxyType.HTTP, None, True),
    ]:
        await ct.aclose_cdn_session()
        monkeypatch.setattr(
            "app.db.account_repository.SystemSettingsRepository.get_str",
            lambda key, default="", _u=proxy_url: _u if key == "pikpak_proxy_url" else default,
        )
        session = await get_cdn_session()
        assert isinstance(session.connector, ProxyConnector)
        assert session.connector._proxy_type == expected_type
        assert session.connector._rdns is expect_rdns
        if expect_ssl:
            assert session.connector._proxy_ssl is not None
        else:
            assert session.connector._proxy_ssl is None
    await ct.aclose_cdn_session()
