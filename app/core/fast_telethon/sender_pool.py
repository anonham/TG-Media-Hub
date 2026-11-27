"""FastTelethon MTProtoSender 通道池: Socket 加固、DC RTT 探针与共享 Auth 独立建连.

纯搬迁自原单文件 app/core/fast_telethon.py (拆分重构, 零运行时逻辑改动)。
"""

import asyncio
import inspect
import logging
import socket
import sys
import time
from contextlib import suppress
from typing import Any

from telethon import TelegramClient, functions
from telethon import errors as telethon_errors
from telethon.errors import FloodWaitError
from telethon.network import MTProtoSender

logger = logging.getLogger(__name__)

FloodPremiumWaitError = getattr(telethon_errors, "FloodPremiumWaitError", FloodWaitError)


def _tune_sender_socket(sender: Any) -> None:
    """加固物理 Sender 套接字与缓冲区参数 (长肥管道优化)"""
    try:
        conn = getattr(sender, "_connection", None)
        if not conn:
            return
        writer = getattr(conn, "_writer", None)
        if not writer:
            return
        tr = getattr(writer, "transport", None)
        if tr:
            # StreamWriter 缓冲水位线：高水位 2MB，低水位 512KB
            if hasattr(tr, "set_write_buffer_limits"):
                with suppress(Exception):
                    tr.set_write_buffer_limits(high=2 * 1024 * 1024, low=512 * 1024)
            sock = tr.get_extra_info("socket")
            if sock:
                with suppress(Exception):
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                with suppress(Exception):
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2 * 1024 * 1024)
                tcp_congestion = getattr(socket, "TCP_CONGESTION", 13 if sys.platform.startswith("linux") else None)
                if tcp_congestion is not None:
                    with suppress(Exception):
                        sock.setsockopt(socket.IPPROTO_TCP, tcp_congestion, b"bbr")
    except Exception as e:
        logger.debug("套接字加固跳过: %s", e)


async def _probe_dc_endpoints(client: TelegramClient, candidates: list) -> list:
    """DC 端点探针：带代理守卫，无代理时并发测 RTT 选 Top 2"""
    if not candidates or len(candidates) <= 1:
        return candidates
    if getattr(client, "_proxy", None):
        return candidates

    unique_candidates = []
    seen = set()
    for c in candidates:
        key = (getattr(c, "ip_address", None), getattr(c, "port", None))
        if key not in seen:
            seen.add(key)
            unique_candidates.append(c)

    if len(unique_candidates) <= 1:
        return candidates

    async def _ping_ip(dc_opt: Any) -> tuple[float, Any]:
        t0 = time.perf_counter()
        try:
            _, writer = await asyncio.wait_for(asyncio.open_connection(dc_opt.ip_address, dc_opt.port), timeout=1.0)
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()
            return (time.perf_counter() - t0, dc_opt)
        except Exception:
            return (999.0, dc_opt)

    try:
        results = await asyncio.gather(*[_ping_ip(c) for c in unique_candidates], return_exceptions=True)
        valid = [r for r in results if isinstance(r, tuple) and r[0] < 900.0]
        if not valid:
            return candidates
        valid.sort(key=lambda x: x[0])
        return [x[1] for x in valid[:2]]
    except Exception as e:
        logger.debug("端点探测异常降级: %s", e)
        return candidates


def _build_client_connection(client: TelegramClient, dc_option: Any, dc_id: int) -> Any:
    conn_factory = getattr(client, "_connection", None)
    if not conn_factory or not callable(conn_factory):
        return None
    try:
        ip = getattr(dc_option, "ip_address", None)
        port = getattr(dc_option, "port", None)
        if not ip or not port:
            return None
        conn = conn_factory(
            ip,
            port,
            dc_id,
            loggers=getattr(client, "_log", None),
            proxy=getattr(client, "_proxy", None),
            local_addr=getattr(client, "_local_addr", None),
        )
    except Exception as e:
        logger.debug("创建 client._connection 异常: %s", e)
        return None
    if inspect.isawaitable(conn):
        close_fn = getattr(conn, "close", None)
        if callable(close_fn):
            with suppress(Exception):
                close_fn()
        return None
    if not hasattr(conn, "connect"):
        return None
    return conn


def _is_sender_connected(sender: Any) -> bool:
    if not sender:
        return False
    is_conn = getattr(sender, "is_connected", None)
    if not callable(is_conn):
        return bool(is_conn)
    conn_val = is_conn()
    if inspect.isawaitable(conn_val):
        close_fn = getattr(conn_val, "close", None)
        if callable(close_fn):
            with suppress(Exception):
                close_fn()
        return True
    return bool(conn_val)


async def _create_worker_sender_with_auth(
    client: TelegramClient, dc_id: int, auth_id: int, auth_bytes: bytes, dc_option: Any = None
) -> Any:
    """使用共享 auth 凭证为 Worker 建立真正独立的 TCP MTProtoSender (零 FloodWait)"""
    if not dc_option:
        dc_option = await client._get_dc(dc_id)

    conn = _build_client_connection(client, dc_option, dc_id)
    if not conn:
        return None
    sender = MTProtoSender(None, loggers=client._log)
    try:
        await asyncio.wait_for(sender.connect(conn), timeout=15.0)

        from telethon.tl import alltlobjects

        device_model = getattr(client, "_device_model", getattr(client, "device_model", "Desktop"))
        system_version = getattr(client, "_system_version", getattr(client, "system_version", "Windows 11 Pro 64bit"))
        app_version = getattr(client, "_app_version", getattr(client, "app_version", "5.4.1 x64"))
        lang_code = getattr(client, "_lang_code", getattr(client, "lang_code", "en"))

        req = functions.InvokeWithLayerRequest(
            alltlobjects.LAYER,
            functions.InitConnectionRequest(
                api_id=client.api_id,
                device_model=device_model,
                system_version=system_version,
                app_version=app_version,
                system_lang_code=lang_code,
                lang_pack="",
                lang_code=lang_code,
                query=functions.auth.ImportAuthorizationRequest(id=auth_id, bytes=auth_bytes),
            ),
        )
        fut = sender.send(req)
        await asyncio.wait_for(fut, timeout=15.0)
        sender.dc_id = dc_id
        _tune_sender_socket(sender)
        return sender
    except BaseException:
        if hasattr(sender, "disconnect"):
            with suppress(Exception):
                disc = sender.disconnect()
                if inspect.isawaitable(disc):
                    await asyncio.wait_for(disc, timeout=2.0)
        raise


async def _create_worker_sender_same_dc(client: TelegramClient, dc_id: int, dc_option: Any = None) -> Any:
    """为同 DC 主账号快速建立基于已有 Session AuthKey 的独立 TCP MTProtoSender"""
    if not dc_option:
        dc_option = await client._get_dc(dc_id)

    conn = _build_client_connection(client, dc_option, dc_id)
    if not conn:
        return None

    sender = MTProtoSender(client.session.auth_key, loggers=client._log)
    try:
        await asyncio.wait_for(sender.connect(conn), timeout=15.0)
        sender.dc_id = dc_id
        _tune_sender_socket(sender)
        return sender
    except BaseException:
        if hasattr(sender, "disconnect"):
            with suppress(Exception):
                disc = sender.disconnect()
                if inspect.isawaitable(disc):
                    await asyncio.wait_for(disc, timeout=2.0)
        raise


async def _get_worker_sender(client: TelegramClient, dc_id: int) -> Any:
    """为 Worker 借用 MTProtoSender 通道 (降级备用)"""
    if not dc_id or not client:
        return None

    try:
        if (
            client.session
            and getattr(client.session, "dc_id", None) == dc_id
            and getattr(client.session, "auth_key", None)
        ):
            return await _create_worker_sender_same_dc(client, dc_id)

        if hasattr(client, "_borrow_exported_sender"):
            sender = await asyncio.wait_for(client._borrow_exported_sender(dc_id), timeout=8.0)
            try:
                if sender:
                    _tune_sender_socket(sender)
            except BaseException:
                await client._return_exported_sender(sender)
                raise
            return sender
        if hasattr(client, "_create_exported_sender"):
            sender = await asyncio.wait_for(client._create_exported_sender(dc_id), timeout=8.0)
            if sender:
                _tune_sender_socket(sender)
            return sender
    except Exception as e:
        logger.warning(f"⚠️ [FastTelethon] 借用/创建 Sender 失败 (DC {dc_id}): {e}")
        return None
    return None


async def _close_worker_sender(client: TelegramClient, sender: Any, dc_id: int) -> None:
    """断开 Worker 通道，静默过滤非致命对端关闭连接异常 (TASK-001)"""
    if not sender:
        return
    try:
        # Telethon owns cached senders and reference counts each worker's borrow.
        if client and hasattr(client, "_borrowed_senders") and dc_id in client._borrowed_senders:
            cached_tuple = client._borrowed_senders[dc_id]
            cached_sender = cached_tuple[1] if isinstance(cached_tuple, tuple) else None
            if cached_sender is sender:
                await client._return_exported_sender(sender)
                return

        if hasattr(sender, "disconnect"):
            await sender.disconnect()
        elif client and hasattr(client, "_return_exported_sender"):
            await client._return_exported_sender(sender)
    except (ConnectionError, asyncio.IncompleteReadError, OSError) as ce:
        logger.debug("Worker 通道断开时遭遇预期对端重置 (静默忽略): %s", ce)
    except Exception as e:
        err_str = str(e).lower()
        if "0 bytes read" in err_str or "closed the connection" in err_str:
            logger.debug("Worker 通道断开时遭遇对端 0 字节关闭 (静默忽略): %s", e)
        else:
            logger.debug("断开 Worker 通道异常: %s", e)


async def _gather_worker_senders_with_cleanup(init_coros: list, clients: list[TelegramClient], dc_id: int) -> list[Any]:
    """并发预建 Worker Sender; gather 中途被取消 (CancelledError 等 BaseException) 时,
    先取消未完成任务并逐个关闭已建立的 sender, 再原样上抛 —— 防止 init 阶段连接泄漏。

    Exception 级单点失败由 init 协程内部降级为 None 槽位, 不会中断 gather, 故此处仅兜底
    BaseException 场景; 回收动作自身再失败也静默 (suppress), 绝不遮蔽原异常。
    """
    tasks = [asyncio.ensure_future(coro) for coro in init_coros]
    try:
        return await asyncio.gather(*tasks)
    except BaseException:
        for t in tasks:
            if not t.done():
                t.cancel()
        with suppress(BaseException):
            await asyncio.gather(*tasks, return_exceptions=True)
        for i, t in enumerate(tasks):
            if t.done() and not t.cancelled() and t.exception() is None and t.result():
                with suppress(Exception):
                    await _close_worker_sender(clients[i % len(clients)], t.result(), dc_id)
        raise


async def _init_worker_sender(
    i: int,
    clients: list[TelegramClient],
    dc_id: int,
    dc_options: list[Any],
    auth_cache: dict[Any, Any],
    *,
    target_client: TelegramClient | None = None,
    tune_socket: bool = False,
    log_tag: str = "FastTelethon",
) -> Any:
    """统一 Worker Sender 预建入口 (下载/上传引擎共用薄委托目标).

    融合两侧既有保护: 同 DC Session AuthKey 直连优先 -> 共享凭证建连且
    ExportAuthorizationRequest 失效自动重导重试 (原上传侧缺失, 唯一有意行为增强)
    -> 失败统一降级借用/重建路径; tune_socket=True 时建连/借用成功后套接字加固
    (上传侧形态)。
    """
    w_client = target_client or clients[i % len(clients)]
    try:
        w_dc_option = dc_options[i % len(dc_options)] if dc_options else None
        if (
            w_client.session
            and getattr(w_client.session, "dc_id", None) == dc_id
            and getattr(w_client.session, "auth_key", None)
        ):
            s = await _create_worker_sender_same_dc(w_client, dc_id, dc_option=w_dc_option)
        else:
            auth: Any = auth_cache.get(w_client)
            if not auth:
                # 首次跨 DC 导出须容忍 Telethon 内置 flood_sleep_threshold (默认 60s), 避免误截断短 FloodWait
                auth = await asyncio.wait_for(w_client(functions.auth.ExportAuthorizationRequest(dc_id)), timeout=75.0)
                auth_cache[w_client] = auth
            try:
                s = await _create_worker_sender_with_auth(w_client, dc_id, auth.id, auth.bytes, dc_option=w_dc_option)
            except Exception:
                auth = await asyncio.wait_for(w_client(functions.auth.ExportAuthorizationRequest(dc_id)), timeout=15.0)
                auth_cache[w_client] = auth
                s = await _create_worker_sender_with_auth(w_client, dc_id, auth.id, auth.bytes, dc_option=w_dc_option)
        if not s:
            s = await _get_worker_sender(w_client, dc_id)
        if s and tune_socket:
            _tune_sender_socket(s)
        return s
    except Exception as e:
        logger.warning(f"⚠️ [{log_tag}] 预建/重建 Sender {i + 1} 降级: {e}")
        s = await _get_worker_sender(w_client, dc_id)
        if s and tune_socket:
            _tune_sender_socket(s)
        return s


async def _prepare_download_auth_and_dc(clients: list[TelegramClient], dc_id: int) -> tuple[dict[Any, Any], list[Any]]:
    auth_cache: dict[Any, Any] = {}
    dc_options: list[Any] = []
    try:
        config = await clients[0](functions.help.GetConfigRequest())
        dc_options = [d for d in config.dc_options if d.id == dc_id and not d.ipv6]
    except Exception as e:
        logger.debug("获取 DC 配置网络异常: %s", e)

    for w_client in clients:
        if getattr(w_client.session, "dc_id", None) == dc_id and getattr(w_client.session, "auth_key", None):
            continue
        try:
            auth_cache[w_client] = await w_client(functions.auth.ExportAuthorizationRequest(dc_id))
        except Exception as e:
            logger.debug("导出 Worker 鉴权凭证异常: %s", e)
    return auth_cache, dc_options


async def _prepare_upload_dc_options(primary_client: TelegramClient, dc_id: int) -> list[Any]:
    try:
        config = await primary_client(functions.help.GetConfigRequest())
        use_ipv6 = getattr(primary_client, "_use_ipv6", False)
        raw_options = [
            d
            for d in config.dc_options
            if d.id == dc_id
            and bool(getattr(d, "ipv6", False)) == use_ipv6
            and not getattr(d, "cdn", False)
            and not getattr(d, "tcpo_only", False)
        ]
        if not raw_options:
            raw_options = [d for d in config.dc_options if d.id == dc_id and not getattr(d, "cdn", False)]
        dc_options_443 = [d for d in raw_options if getattr(d, "port", None) == 443]
        candidates = dc_options_443 if dc_options_443 else raw_options
        return await _probe_dc_endpoints(primary_client, candidates)
    except Exception as e:
        logger.debug("上传 DC 配置获取与端点探测异常: %s", e)
        return []
