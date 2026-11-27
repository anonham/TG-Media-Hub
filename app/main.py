import asyncio
import copy
import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request, Security, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api import api_router
from app.api._login_helpers import NO_CACHE_HEADERS
from app.api.access import console_locked_for_request, is_local_request
from app.api.auth import (
    BootstrapAccessError,
    api_key_header,
    api_key_matches,
    rate_limiter_middleware,
    two_fa_header,
    verify_api_key,
)
from app.config import secure_file_permissions, settings
from app.core.cdn_transport import cdn_transport_state
from app.core.client_manager import client_manager
from app.core.connection_manager import connection_manager
from app.core.fmp4_pipe_uploader import fmp4_runtime_state
from app.core.log_setup import configure_logging, logging_state, shutdown_logging
from app.core.scheduler import task_scheduler
from app.core.single_instance import SingleInstanceLock, acquire_process_singleton_lock
from app.db.database import WAL_CHECKPOINT_INTERVAL_SECONDS, init_db, run_wal_checkpoint
from app.db.task_repository import TaskQueueRepository
from app.db.write_pool import write_pool_state
from app.services.pikpak_service import pikpak_service_state

# 应用日志体系 (console + data/logs/app.log 旋转文件 + 环形缓冲):
# 模块导入时配置, 覆盖 uvicorn app.main:app 直拉入口 (run.py 入口由 run.py 自行调用, 幂等)
configure_logging()
logger = logging.getLogger(__name__)


class SuppressExpectedCloseFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        if "0 bytes read on a total of 8 expected bytes" in msg or "Server closed the connection: 0 bytes read" in msg:
            return False
        return True


def _harden_session_permissions_sync() -> None:
    secure_file_permissions(settings.DB_PATH)
    for target_dir in (settings.BASE_DIR, settings.DATA_DIR):
        if os.path.exists(target_dir):
            for item in os.listdir(target_dir):
                if item.endswith(".session"):
                    secure_file_permissions(os.path.join(target_dir, item))


def _read_index_html_sync() -> str:
    index_path = os.path.join(settings.STATIC_DIR, "index.html")
    if os.path.exists(index_path):
        with open(index_path, encoding="utf-8") as f:
            return f.read()
    return "<h1>Telegram Web Service Running</h1>"


for _logger_name in ("telethon.network.mtprotosender", "telethon.network.connection.connection", "telethon.network"):
    logging.getLogger(_logger_name).addFilter(SuppressExpectedCloseFilter())


async def _run_protected_thread(pending: set[asyncio.Task[None]], fn, *args):
    """提交工作线程任务并登记至 pending 集合；即使外层被取消仍保留句柄以便退出前等待完成。"""
    task: asyncio.Task[None] = asyncio.create_task(asyncio.to_thread(fn, *args))
    pending.add(task)
    try:
        return await asyncio.shield(task)
    finally:
        if task.done():
            pending.discard(task)


async def _cleanup_lifespan(
    *,
    pending_worker_tasks: set[asyncio.Task[None]],
    wal_task: asyncio.Task[None] | None,
    db_initialized: bool,
    runtime_started: bool,
) -> None:
    """统一停机与异常退出的有序清理，确保在释放单实例锁前排空一切后台工作。"""
    if pending_worker_tasks:
        await asyncio.gather(*pending_worker_tasks, return_exceptions=True)

    if wal_task is not None:
        wal_task.cancel()
        try:
            await wal_task
        except (asyncio.CancelledError, Exception) as wal_exit_err:
            logger.debug("退出时等待 WAL 检查点任务终止: %s", wal_exit_err)

    if runtime_started:
        await client_manager.stop_startup_recovery()
        await task_scheduler.stop()

    if db_initialized:
        try:
            from app.db.write_pool import cancel_queued_and_wait_active_writes, drain_db_write_pool

            drained = await drain_db_write_pool(timeout=3.0)
            if not drained:
                logger.warning("退出时 DB 写池未在 3.0s 内排空；已触发方案 A 兜底（取消排队写入并等待在途写线程完成）")
                await cancel_queued_and_wait_active_writes()
        except Exception as e:
            logger.warning("退出时排空 DB 写池失败，尝试等待在途写入收口: %s", e)
            try:
                from app.db.write_pool import cancel_queued_and_wait_active_writes

                await cancel_queued_and_wait_active_writes()
            except Exception as cancel_err:
                logger.warning("退出时 DB 写池兜底收口失败: %s", cancel_err)

        try:
            await asyncio.to_thread(run_wal_checkpoint)
        except Exception as e:
            logger.warning("退出时执行最终 WAL 检查点失败: %s", e)

    if runtime_started:
        try:
            from app.services.telegram_login_service import telegram_login_service

            await telegram_login_service.shutdown()
        except Exception as login_shut_err:
            logger.warning("退出时清理登录挑战服务异常: %s", login_shut_err)
        await client_manager.disconnect_all()
        try:
            from app.core.cdn_transport import aclose_cdn_session

            await aclose_cdn_session()
        except Exception as e:
            logger.warning("退出时关闭 CDN 会话失败: %s", e)

    await asyncio.to_thread(shutdown_logging)


async def _await_shielded_cleanup(
    *,
    pending_worker_tasks: set[asyncio.Task[None]],
    wal_task: asyncio.Task[None] | None,
    db_initialized: bool,
    runtime_started: bool,
    lifespan_lock: SingleInstanceLock | None,
) -> None:
    """在任何取消信号下都等待清理任务完全结束后再释放单实例锁，最后再恢复向上传播 CancelledError。"""
    cleanup_task = asyncio.create_task(
        _cleanup_lifespan(
            pending_worker_tasks=pending_worker_tasks,
            wal_task=wal_task,
            db_initialized=db_initialized,
            runtime_started=runtime_started,
        )
    )
    caller_cancelled = False
    try:
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                caller_cancelled = True
        await cleanup_task
    finally:
        if lifespan_lock is not None:
            lifespan_lock.release()
    if caller_cancelled:
        raise asyncio.CancelledError


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Web 服务启动：初始化 SQLite 数据库、自动清理孤儿任务及动态客户端连接池与调度引擎...")
    # 启动期凭证校验: 必须先于任何初始化 (尤其下方会吞异常的 try 块) 快速失败, 一次性列出全部缺失键
    settings.validate_required()
    # PR1 迁移告警: TG_API_KEY 别名已移除 (两者并存时 validate_required 已放行至此), 提示清理残留
    if os.getenv("TG_API_KEY"):
        logger.warning("检测到已弃用的 TG_API_KEY 环境变量: 自本版本起不再生效, 请改用 API_KEY 并清理残留配置")
    # 单实例锁守卫: 覆盖 uvicorn app.main:app 直拉入口 (run.py 已持同路径锁时静默复用, 不重复加锁)
    lifespan_lock = acquire_process_singleton_lock(settings.LOCK_PATH)
    pending_worker_tasks: set[asyncio.Task[None]] = set()
    wal_task: asyncio.Task[None] | None = None
    db_initialized = False
    runtime_started = False
    try:
        init_db()
        db_initialized = True
        task_scheduler.recover_orphaned_tasks()

        # 占位槽启动自愈 —— 崩溃遗留的 editing 孤儿槽回滚 placeholder_sent
        # (幂等重 edit 无害; 与 recover_orphaned_tasks 同期执行, 此时无任何活跃任务)
        try:
            from app.core.pipeline.placeholder import PlaceholderSlotCoordinator

            PlaceholderSlotCoordinator().heal_editing()
        except Exception as heal_err:
            logger.warning("占位槽启动自愈跳过: %s", heal_err)

        # 任务日志保留修剪 (启动期, 无活跃任务, 单写者前提天然满足; 防长任务日志无界增长)
        try:
            pruned = await _run_protected_thread(
                pending_worker_tasks, TaskQueueRepository.prune_task_logs, settings.TASK_LOGS_RETENTION_PER_TASK
            )
            if pruned:
                logger.info(
                    "任务日志保留修剪完成: 共清理 %d 条历史日志 (每任务保留最新 %d 条)",
                    pruned,
                    settings.TASK_LOGS_RETENTION_PER_TASK,
                )
        except Exception as prune_err:
            logger.warning("任务日志保留修剪跳过: %s", prune_err)

        # 孤儿断点登记扫描 (此时无任何活跃任务, 天然安全; 只登记不删除)
        from app.services.storage_service import StorageService

        await _run_protected_thread(pending_worker_tasks, StorageService.scan_and_register)

        # 增强敏感文件读写权限隔离 (0600)
        await _run_protected_thread(pending_worker_tasks, _harden_session_permissions_sync)

        # 启动后台 WAL 定期 checkpoint 协程
        wal_task = asyncio.create_task(_wal_checkpoint_loop())

        # 解耦执行器注册：保证无论网络或账号是否就绪，调度执行器注册表均无条件就绪挂载
        from app.services.executor_registry import register_default_executors

        register_default_executors(task_scheduler)

        runtime_started = True
        try:
            await client_manager.initialize()
            if client_manager.is_default_account_admitted():
                await task_scheduler.start()
                task_scheduler.startup_degraded = False
                logger.info("✅ Telegram 连接池初始化结束，智能调度引擎已运行！")
            else:
                client_manager.startup_degraded = True
                task_scheduler.startup_degraded = True
                logger.warning("⚠️ 默认 Userbot 处于受限待绑定状态，服务已进入受限引导模式 (调度器暂不启动)")
        except Exception as e:
            logger.warning("⚠️ 初始化提示: %s", e)
            client_manager.startup_degraded = True
            task_scheduler.startup_degraded = True
        yield
        logger.info("🛑 正在停止调度引擎并关闭 Telegram 连接...")
    finally:
        await _await_shielded_cleanup(
            pending_worker_tasks=pending_worker_tasks,
            wal_task=wal_task,
            db_initialized=db_initialized,
            runtime_started=runtime_started,
            lifespan_lock=lifespan_lock,
        )


async def _wal_checkpoint_loop():
    """定期执行 SQLite PASSIVE 检查点，防止长效运行 WAL 文件无限增长"""
    while True:
        try:
            await asyncio.sleep(WAL_CHECKPOINT_INTERVAL_SECONDS)
            await asyncio.to_thread(run_wal_checkpoint)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning("定期 WAL 检查点执行异常: %s", e)


class ConsoleApp(FastAPI):
    def openapi(self) -> dict[str, Any]:
        previous_schema = self.openapi_schema
        schema = super().openapi()
        if schema is previous_schema:
            return schema
        both_headers: dict[str, list[str]] = {api_key_header.scheme_name: [], two_fa_header.scheme_name: []}
        separate_headers = [{name: scopes} for name, scopes in both_headers.items()]
        # FastAPI emits separate Security dependencies as OR. The console requires both headers.
        for path in schema["paths"].values():
            for operation in path.values():
                if not isinstance(operation, dict):
                    continue
                security = operation.get("security", [])
                if all(header in security for header in separate_headers):
                    operation["security"] = [both_headers, *(item for item in security if item not in separate_headers)]
        return schema


app = ConsoleApp(
    title="Telegram Restricted Download & Multi-Channel Publisher Web Service",
    description="支持突破限制保存内容下载、FastTelethon 多通道并发加速、断点续传与 4-Bot 轮询频道发布 Web 服务",
    version="1.0.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

# 挂载核心运行时状态容器至 FastAPI app.state (支持集中观测与依赖访问)
app.state.client_manager = client_manager
app.state.task_scheduler = task_scheduler
app.state.connection_manager = connection_manager
app.state.cdn_transport = cdn_transport_state
app.state.fmp4_runtime = fmp4_runtime_state
app.state.logging_runtime = logging_state
app.state.write_pool = write_pool_state
app.state.pikpak_service = pikpak_service_state


_RESTRICTED_EXACT_ALLOW_PATHS = {
    "/",
    "/favicon.ico",
    "/api/auth/status",
    "/api/telegram/status",
    "/api/telegram/bootstrap/auth",
    "/api/telegram/bootstrap/logout",
    "/api/telegram/native/send-code",
    "/api/telegram/native/resend-code",
    "/api/telegram/native/verify-code",
    "/api/telegram/native/password",
    "/api/telegram/qr/start",
    "/api/telegram/qr/status",
    "/api/telegram/qr/password",
    "/api/telegram/challenge/cancel",
}


def _is_path_allowed_in_restricted_mode(path: str) -> bool:
    if path in _RESTRICTED_EXACT_ALLOW_PATHS:
        return True
    return path.startswith("/static/")


@app.exception_handler(BootstrapAccessError)
async def bootstrap_access_exception_handler(_request: Request, exc: BootstrapAccessError):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "error_code": exc.error_code},
        headers=exc.headers,
    )


# 受限引导门禁 + Rate Limit 限流中间件
@app.middleware("http")
async def http_rate_limit_middleware(request: Request, call_next):
    path = request.scope["path"]
    root_path = request.scope.get("root_path", "")
    # 门禁使用与路由匹配一致的应用内路径，避免挂载前缀被当作白名单路径。
    if root_path and (path == root_path or path.startswith(root_path + "/")):
        path = path[len(root_path) :] or "/"
    # 集中检查默认 Userbot 业务准入: 早于 /metrics 放行、API_KEY 留空与旧 2FA/ticket 放行
    if not client_manager.is_default_account_admitted() and not _is_path_allowed_in_restricted_mode(path):
        return JSONResponse(
            status_code=503,
            content={
                "detail": "默认 Userbot 尚未完成绑定或验证，系统处于受限引导状态",
                "error_code": "telegram_bootstrap_required",
            },
            headers=NO_CACHE_HEADERS,
        )

    # PR1 公网入口控制台锁定门禁: API_KEY 留空仅覆盖可证明的本机直连 (绑定回环 + 对端回环 +
    # 无代理转发痕迹); 非本机来源未配置 API_KEY 时一律锁定并提示部署者完成配置。放行清单与
    # 受限引导一致 —— 受保护的首次绑定、最小公开状态、首页、静态资源与健康检查。
    if console_locked_for_request(request) and not _is_path_allowed_in_restricted_mode(path):
        return JSONResponse(
            status_code=503,
            content={
                "detail": "公网入口未配置 API_KEY，控制台已锁定：请在部署配置中设置 API_KEY 并重启服务",
                "error_code": "console_locked",
            },
            headers=NO_CACHE_HEADERS,
        )

    # 高频抓取只在 Key 校验通过后跳过限流，失败校验继续消耗请求预算。
    if (
        path == "/metrics"
        and request.method == "GET"
        and api_key_matches(request.headers.get(api_key_header.model.name))
    ):
        return await call_next(request)

    # BaseHTTPMiddleware 内 raise 的 HTTPException 不会被更内层的 ExceptionMiddleware
    # 捕获 (会直冲 ServerErrorMiddleware 变 500), 需就地转成 JSONResponse 返回 429
    try:
        rate_limiter_middleware(request)
    except HTTPException as exc:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    return await call_next(request)


# CORS 跨域安全中间件 —— 必须在 rate-limit 之后注册: Starlette 后注册者
# 位于外层, CORS 处于外层才能把限流 429 等中间件生成的响应也包上 CORS 头, 否则跨域
# 前端 (如 Vite dev) 收到的是不透明 CORS 错误而非 429
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 挂载静态资源目录
app.mount("/static", StaticFiles(directory=settings.STATIC_DIR), name="static")

# 引入路由模块
app.include_router(api_router)


@app.get("/metrics", include_in_schema=True, tags=["Metrics"], dependencies=[Depends(verify_api_key)])
async def metrics_endpoint():
    """Prometheus 运行时监控指标端点 (配置 API_KEY 后须携带 X-API-Key, 无需 2FA;
    本机免认证模式仅放行本机直连; 受限引导期与公网锁定态由前置门禁禁止暴露业务指标)"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ── 自管文档入口 (内置 docs_url/redoc_url/openapi_url 已关闭) ──────────
# 可达性: /docs /redoc 仅本机直连; /openapi.json 本机直达, 或 key 模式下凭 X-API-Key
# 程序化拉取。通过前置门禁后，公网匿名请求返回 404。


def _reject_non_local_docs_request(request: Request) -> None:
    if not is_local_request(request):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")


@app.get("/docs", include_in_schema=False)
async def swagger_ui_endpoint(request: Request):
    """Swagger UI (仅本机直连可达; 公网请求 404)"""
    _reject_non_local_docs_request(request)
    root_path = request.scope.get("root_path", "").rstrip("/")
    return get_swagger_ui_html(openapi_url=f"{root_path}/openapi.json", title=f"{app.title} - Swagger UI")


@app.get("/redoc", include_in_schema=False)
async def redoc_ui_endpoint(request: Request):
    """ReDoc (仅本机直连可达; 公网请求 404)"""
    _reject_non_local_docs_request(request)
    root_path = request.scope.get("root_path", "").rstrip("/")
    return get_redoc_html(openapi_url=f"{root_path}/openapi.json", title=f"{app.title} - ReDoc")


def _openapi_response(request: Request) -> JSONResponse:
    schema = app.openapi()
    root_path = request.scope.get("root_path", "").rstrip("/")
    if root_path:
        schema = copy.deepcopy(schema)
        servers = schema.setdefault("servers", [])
        if not any(server.get("url") == root_path for server in servers):
            servers.insert(0, {"url": root_path})
    return JSONResponse(schema)


@app.get("/openapi.json", include_in_schema=False)
async def openapi_schema_endpoint(request: Request, header_key: str | None = Security(api_key_header)):
    """OpenAPI schema (本机直达; key 模式下凭 X-API-Key 程序化拉取; 匿名公网 404)"""
    if is_local_request(request):
        return _openapi_response(request)
    if api_key_matches(header_key):
        return _openapi_response(request)
    if header_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="未经授权的控制台访问：API Key 无效或缺失")
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)


@app.get("/", response_class=HTMLResponse)
async def read_root():
    return await asyncio.to_thread(_read_index_html_sync)
