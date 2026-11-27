import asyncio
import logging
from collections.abc import Callable

from fastapi import WebSocket

from app.db.task_repository import TaskQueueRepository

logger = logging.getLogger(__name__)

# 可安全丢弃的高频暂态消息类型（前端在下一次进度/心跳 tick 会自动覆盖）
DROPPABLE_MESSAGE_TYPES = {"progress", "log", "task_log"}


class ConnectionManager:
    """全局 WebSocket 长连接与状态/日志广播管理器 (Per-Connection 发送队列模型)"""

    QUEUE_CAPACITY: int = 500

    def __init__(self):
        # websocket -> (queue, sender_task, loop)
        self._connections: dict[WebSocket, tuple[asyncio.Queue, asyncio.Task, asyncio.AbstractEventLoop]] = {}

    @property
    def active_connections(self) -> list[WebSocket]:
        return list(self._connections.keys())

    async def connect(self, websocket: WebSocket, *, may_send: Callable[[], bool] | None = None):
        await websocket.accept()
        if websocket in self._connections:
            self.disconnect(websocket)
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue(maxsize=self.QUEUE_CAPACITY)
        sender_task = loop.create_task(self._sender_loop(websocket, queue, may_send))
        self._connections[websocket] = (queue, sender_task, loop)
        self._update_metrics()

    def disconnect(self, websocket: WebSocket):
        conn_entry = self._connections.pop(websocket, None)
        if conn_entry is not None:
            _, sender_task, _ = conn_entry
            try:
                current_task = asyncio.current_task()
            except RuntimeError:
                current_task = None
            if sender_task is not current_task and not sender_task.done():
                sender_task.cancel()
            self._update_metrics()

    async def _sender_loop(
        self, websocket: WebSocket, queue: asyncio.Queue, may_send: Callable[[], bool] | None = None
    ):
        try:
            while True:
                message = await queue.get()
                try:
                    if may_send is not None and not may_send():
                        self.disconnect(websocket)
                        await self._safe_close(websocket)
                        break
                    await websocket.send_json(message)
                except Exception as e:
                    logger.debug("WebSocket 发送失败，移除连接: %s", e)
                    self.disconnect(websocket)
                    try:
                        await websocket.close()
                    except (Exception, asyncio.CancelledError) as close_err:
                        logger.debug("WebSocket 关闭异常 (非致命忽略): %s", close_err)
                    break
                finally:
                    queue.task_done()
        except asyncio.CancelledError:
            logger.debug("WebSocket sender loop 正常取消退出")

    async def broadcast(self, message: dict):
        if not self._connections:
            return

        current_loop = None
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError as rt_err:
            logger.debug("获取当前运行 loop 异常 (未在协程上下文): %s", rt_err)

        for websocket, (queue, _sender_task, conn_loop) in list(self._connections.items()):

            def _push(ws=websocket, q=queue, loop=conn_loop):
                msg_type = message.get("type") if isinstance(message, dict) else None
                try:
                    q.put_nowait(message)
                except asyncio.QueueFull:
                    # 优化背压：高频暂态消息在队列饱和时安全丢弃，保全长连接避免闪烁与重连风暴
                    if msg_type in DROPPABLE_MESSAGE_TYPES:
                        logger.warning(
                            "WebSocket 客户端 %s 消息队列满 (>%d)，丢弃高频暂态消息以保全长连接: type=%s",
                            ws,
                            self.QUEUE_CAPACITY,
                            msg_type,
                        )
                        return

                    logger.warning(
                        "WebSocket 客户端 %s 消息队列溢出 (>%d)，主动断开隔离慢客户端", ws, self.QUEUE_CAPACITY
                    )
                    self.disconnect(ws)
                    if loop.is_running():
                        loop.create_task(self._safe_close(ws))

            if current_loop == conn_loop:
                _push()
            elif conn_loop.is_running():
                try:
                    conn_loop.call_soon_threadsafe(_push)
                except RuntimeError:
                    self.disconnect(websocket)
            elif conn_loop.is_closed():
                self.disconnect(websocket)

    async def _safe_close(self, websocket: WebSocket):
        try:
            await websocket.close(code=1008)
        except (Exception, asyncio.CancelledError) as safe_close_err:
            logger.debug("WebSocket 安全断开流程忽略错误: %s", safe_close_err)

    def _update_metrics(self):
        try:
            from app.core.metrics import active_ws_connections

            active_ws_connections.set(len(self._connections))
        except Exception as metric_err:
            logger.debug("更新 WebSocket 在线连接指标失败 (非致命): %s", metric_err)


connection_manager = ConnectionManager()


def _dispatch_log_broadcast(payload: dict):
    return connection_manager.broadcast(payload)


TaskQueueRepository.register_log_broadcast_hook(_dispatch_log_broadcast)
