import asyncio

import pytest
from _async_wait import wait_until
from fastapi.testclient import TestClient

from app.api.auth import issue_media_ticket
from app.config import settings
from app.core.connection_manager import connection_manager
from app.main import app

client = TestClient(app, client=("127.0.0.1", 50000))


def test_websocket_connection_and_broadcast():
    query = f"?ticket={issue_media_ticket()}" if settings.API_KEY else ""
    with client.websocket_connect(f"/ws/progress{query}") as websocket:
        # 发送测试消息通过 manager 广播
        test_msg = {"type": "progress", "task_id": "ws_t1", "percent": 75.5}

        # 运行广播逻辑
        asyncio.run(connection_manager.broadcast(test_msg))

        # 接收并校验
        data = websocket.receive_json()
        assert data["type"] == "progress"
        assert data["task_id"] == "ws_t1"
        assert data["percent"] == 75.5


@pytest.mark.asyncio
async def test_slow_client_isolation():
    """验证慢客户端发送阻塞不拖慢正常客户端的广播投递与接收"""
    from unittest.mock import AsyncMock

    fast_ws = AsyncMock()
    slow_ws = AsyncMock()

    slow_ws_event = asyncio.Event()
    received_by_fast = []

    async def fast_send(msg):
        received_by_fast.append(msg)

    async def slow_send(msg):
        await slow_ws_event.wait()

    fast_ws.send_json = AsyncMock(side_effect=fast_send)
    slow_ws.send_json = AsyncMock(side_effect=slow_send)

    await connection_manager.connect(fast_ws)
    await connection_manager.connect(slow_ws)

    for i in range(5):
        await connection_manager.broadcast({"seq": i})

    await wait_until(lambda: len(received_by_fast) == 5, message="fast ws did not receive all 5 broadcasts")

    # 正常客户端无需等待慢客户端，立即收到全部广播
    assert len(received_by_fast) == 5
    assert [m["seq"] for m in received_by_fast] == [0, 1, 2, 3, 4]

    slow_ws_event.set()
    connection_manager.disconnect(fast_ws)
    connection_manager.disconnect(slow_ws)


@pytest.mark.asyncio
async def test_queue_overflow_disconnect():
    """验证慢客户端积压非暂态消息超过队列上限时触发熔断剔除与断开"""
    from unittest.mock import AsyncMock

    slow_ws = AsyncMock()
    block_event = asyncio.Event()

    async def blocking_send(msg):
        await block_event.wait()

    slow_ws.send_json = AsyncMock(side_effect=blocking_send)
    await connection_manager.connect(slow_ws)
    assert slow_ws in connection_manager.active_connections

    # 超过容量上限触发溢出断开 (非暂态消息如 seq/task_update)
    for i in range(connection_manager.QUEUE_CAPACITY + 10):
        await connection_manager.broadcast({"seq": i})

    assert slow_ws not in connection_manager.active_connections

    block_event.set()
    await wait_until(lambda: slow_ws.close.called, message="overflow slow_ws was not closed")
    # 验证溢出慢客户端被以 1008 策略违规码明确关闭断开
    slow_ws.close.assert_called_with(code=1008)


@pytest.mark.asyncio
async def test_queue_overflow_drops_progress_without_disconnect():
    """验证慢客户端积压高频进度消息时，自动丢弃暂态帧保全连接而不触发断开"""
    from unittest.mock import AsyncMock

    slow_ws = AsyncMock()
    block_event = asyncio.Event()

    async def blocking_send(msg):
        await block_event.wait()

    slow_ws.send_json = AsyncMock(side_effect=blocking_send)
    await connection_manager.connect(slow_ws)
    assert slow_ws in connection_manager.active_connections

    # 广播超过容量上限的高频 progress 暂态消息
    for i in range(connection_manager.QUEUE_CAPACITY + 50):
        await connection_manager.broadcast({"type": "progress", "task_id": "t1", "percent": i % 100})

    # 连接仍保持活跃，未被 1008 剔除
    assert slow_ws in connection_manager.active_connections
    assert not slow_ws.close.called

    block_event.set()
    connection_manager.disconnect(slow_ws)


@pytest.mark.asyncio
async def test_dead_client_cleanup_and_no_task_leak():
    """验证死连接异常抛出时主动剔除并终结 sender，无孤儿协程泄漏"""
    from unittest.mock import AsyncMock

    dead_ws = AsyncMock()
    dead_ws.send_json = AsyncMock(side_effect=ConnectionResetError("Socket broken"))

    await connection_manager.connect(dead_ws)
    assert dead_ws in connection_manager.active_connections

    sender_task = connection_manager._connections[dead_ws][1]
    assert not sender_task.done()

    await connection_manager.broadcast({"type": "test"})
    await wait_until(
        lambda: dead_ws not in connection_manager.active_connections and sender_task.done() and dead_ws.close.called,
        message="dead ws cleanup did not converge",
    )

    assert dead_ws not in connection_manager.active_connections
    assert sender_task.done()
    assert dead_ws.close.called

    # 断言无孤儿协程 (asyncio.all_tasks 过滤)
    orphans = [t for t in asyncio.all_tasks() if t is sender_task and not t.done()]
    assert len(orphans) == 0


@pytest.mark.asyncio
async def test_endpoint_cascade_cancel_on_disconnect():
    """验证 disconnect 会级联取消该连接对应的 sender task"""
    from unittest.mock import AsyncMock

    ws = AsyncMock()
    await connection_manager.connect(ws)
    sender_task = connection_manager._connections[ws][1]
    assert not sender_task.done()

    connection_manager.disconnect(ws)
    await wait_until(sender_task.done, message="sender_task did not finish after disconnect")

    assert ws not in connection_manager.active_connections
    assert sender_task.done()
    assert sender_task.cancelled()


@pytest.mark.asyncio
async def test_broadcast_handles_closed_loop_and_runtime_error():
    """验证 broadcast 能够探测已关闭的外部 event loop 并安全清理连接，且防范 RuntimeError 崩溃"""
    from unittest.mock import AsyncMock, MagicMock

    ws = AsyncMock()
    mock_loop = MagicMock()
    mock_loop.is_running.return_value = False
    mock_loop.is_closed.return_value = True

    mock_task1 = MagicMock()
    mock_task1.done.return_value = True
    connection_manager._connections[ws] = (asyncio.Queue(10), mock_task1, mock_loop)
    assert ws in connection_manager.active_connections

    await connection_manager.broadcast({"type": "closed_loop_test"})
    # 已关闭 loop 对应的连接应被自动剔除，不泄露在活跃连接池
    assert ws not in connection_manager.active_connections

    # 模拟 call_soon_threadsafe 抛出 RuntimeError (如循环在调用瞬间关闭)
    ws2 = AsyncMock()
    mock_loop2 = MagicMock()
    mock_loop2.is_running.return_value = True
    mock_loop2.call_soon_threadsafe.side_effect = RuntimeError("Event loop is closed")

    mock_task2 = MagicMock()
    mock_task2.done.return_value = True
    connection_manager._connections[ws2] = (asyncio.Queue(10), mock_task2, mock_loop2)
    await connection_manager.broadcast({"type": "runtime_error_test"})
    assert ws2 not in connection_manager.active_connections


@pytest.mark.asyncio
async def test_duplicate_connect_cleans_up_previous():
    """验证同一个 websocket 实例重复 connect 会先清理旧连接，不产生未追踪任务"""
    from unittest.mock import AsyncMock

    ws = AsyncMock()
    await connection_manager.connect(ws)
    first_task = connection_manager._connections[ws][1]

    await connection_manager.connect(ws)
    second_task = connection_manager._connections[ws][1]

    await wait_until(first_task.done, message="previous sender task was not cancelled on reconnect")
    assert first_task.done()
    assert first_task.cancelled()
    assert not second_task.done()

    connection_manager.disconnect(ws)


@pytest.mark.parametrize(
    ("origins", "allowed"),
    [
        ([], True),
        (["http://localhost:8500"], True),
        (["https://ui.example"], True),
        (["null"], False),
        (["https://other.example"], False),
        (["https://ui.example", "https://other.example"], False),
    ],
)
def test_keyless_websocket_browser_origin(monkeypatch, origins, allowed):
    from httpx import Headers
    from starlette.websockets import WebSocketDisconnect

    monkeypatch.setattr(settings, "API_KEY", "")
    monkeypatch.setattr(settings, "CORS_ORIGINS", ["http://localhost:8500", "https://ui.example", "*"])
    headers = Headers([("origin", origin) for origin in origins])
    if allowed:
        with client.websocket_connect("/ws/progress", headers=headers):
            pass
    else:
        with pytest.raises(WebSocketDisconnect) as exc, client.websocket_connect("/ws/progress", headers=headers):
            pytest.fail("unexpectedly accepted")
        assert exc.value.code == 1008
