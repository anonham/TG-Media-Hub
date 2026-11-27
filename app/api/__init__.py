from fastapi import APIRouter, Depends

from app.api import (
    accounts,
    audit,
    auth,
    download,
    forward,
    logs,
    pikpak,
    presets,
    publish,
    storage,
    tasks,
    telegram_auth,
    websockets,
)
from app.api import (
    settings as settings_api,
)
from app.api.auth import verify_2fa_session, verify_api_key

api_router = APIRouter()
api_router.include_router(auth.router, prefix="/api/auth", tags=["Auth"])
api_router.include_router(
    accounts.accounts_router,
    prefix="/api/accounts",
    tags=["Accounts"],
    dependencies=[Depends(verify_api_key), Depends(verify_2fa_session)],
)
api_router.include_router(
    accounts.bots_router,
    prefix="/api/bots",
    tags=["Bots"],
    dependencies=[Depends(verify_api_key), Depends(verify_2fa_session)],
)
api_router.include_router(
    download.router,
    prefix="/api/download",
    tags=["Download"],
    dependencies=[Depends(verify_api_key), Depends(verify_2fa_session)],
)
api_router.include_router(
    publish.router,
    prefix="/api/publish",
    tags=["Publish"],
    dependencies=[Depends(verify_api_key), Depends(verify_2fa_session)],
)
api_router.include_router(
    forward.router,
    prefix="/api/forward",
    tags=["Forward"],
    dependencies=[Depends(verify_api_key), Depends(verify_2fa_session)],
)
api_router.include_router(
    presets.router,
    prefix="/api/presets",
    tags=["Presets"],
    dependencies=[Depends(verify_api_key), Depends(verify_2fa_session)],
)
api_router.include_router(
    tasks.router,
    prefix="/api/tasks",
    tags=["Tasks"],
    dependencies=[Depends(verify_api_key), Depends(verify_2fa_session)],
)
api_router.include_router(storage.router, prefix="/api/storage", tags=["Storage"])
api_router.include_router(
    logs.router, prefix="/api/logs", tags=["Logs"], dependencies=[Depends(verify_api_key), Depends(verify_2fa_session)]
)
api_router.include_router(
    audit.router,
    prefix="/api/audit",
    tags=["Audit"],
    dependencies=[Depends(verify_api_key), Depends(verify_2fa_session)],
)
api_router.include_router(
    settings_api.router,
    prefix="/api/settings",
    tags=["Settings"],
    dependencies=[Depends(verify_api_key), Depends(verify_2fa_session)],
)
# PikPak 整组双依赖挂载 (硬性约束 5: 一处控制, 路由文件内零鉴权代码, 杜绝个别端点漏挂)
api_router.include_router(
    pikpak.router,
    prefix="/api/pikpak",
    tags=["PikPak"],
    dependencies=[Depends(verify_api_key), Depends(verify_2fa_session)],
)
api_router.include_router(websockets.router, tags=["WebSockets"])
api_router.include_router(telegram_auth.router, prefix="/api/telegram", tags=["TelegramAuth"])

__all__ = ["api_router"]
