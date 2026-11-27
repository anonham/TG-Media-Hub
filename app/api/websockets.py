import asyncio
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.api.auth import verify_websocket_auth
from app.core.client_manager import client_manager
from app.core.connection_manager import connection_manager

logger = logging.getLogger(__name__)
router = APIRouter()


@router.websocket("/ws/progress")
async def websocket_endpoint(websocket: WebSocket):
    if not await verify_websocket_auth(websocket):
        return
    await connection_manager.connect(websocket, may_send=client_manager.is_default_account_admitted)
    try:
        while True:
            await websocket.receive_text()  # 心跳保持
    except (WebSocketDisconnect, asyncio.CancelledError) as ws_err:
        logger.debug("WebSocket 连接正常断开或取消: %s", ws_err)
    finally:
        connection_manager.disconnect(websocket)
