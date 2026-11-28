import io
import logging
import os
import shutil
import sys
import uuid
from pathlib import Path

import pytest
from PIL import Image

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from app.config import settings
from app.core.pipeline.models import BreathingPolicy, PublishContext
from app.db.database import init_db


@pytest.fixture
def cover_jpeg() -> bytes:
    """A decodable, non-black thumbnail that can pass through validation."""
    buf = io.BytesIO()
    Image.new("RGB", (32, 18), "orange").save(buf, format="JPEG")
    return buf.getvalue()


# 测试期零等待呼吸/退避策略默认注入 (杜绝单测内无意触发生产 15s+ 呼吸睡眠导致单用例耗时 50s+)
_FAST_TEST_BREATHING = BreathingPolicy(
    min_floor_s=0.0, jitter_s=0.0, poll_s=0.01, retry_backoff_s=(0.0, 0.0), enabled=False
)
_orig_publish_context_init = PublishContext.__init__


def _patched_publish_context_init(self, *args, **kwargs):
    kwargs.setdefault("breathing", _FAST_TEST_BREATHING)
    return _orig_publish_context_init(self, *args, **kwargs)


PublishContext.__init__ = _patched_publish_context_init

# 测试期日志重定向: 必须在 conftest 顶层 (收集阶段、任何测试模块 import app.main 之前) 改写,
# 否则首个测试模块导入即触发 app/main.py 的 configure_logging(), dictConfig 单次 guard 下
# RotatingFileHandler 会向真实 data/logs/app.log 落盘。落点沿用 tmp_dir 同款 os.makedirs 模式
# (继承父目录 ACL, 避免 tempfile.mkdtemp 在 Windows 沙箱下的严格 ACL 问题)。
_RUN_DIR = Path(_TESTS_DIR) / ".tmp" / f"run_{uuid.uuid4().hex}"
_TEST_LOG_DIR = str(_RUN_DIR / "logs")
os.makedirs(_TEST_LOG_DIR, exist_ok=True)
settings.LOG_DIR = _TEST_LOG_DIR

# 执行器注册单接线 (生产唯一注册点 = main lifespan; 测试进程走裸 TestClient 不触发 lifespan,
# 在此显式注册。register 为幂等覆盖语义, 各策略测试可按需自行重注册。)
from app.services.executor_registry import register_default_executors

register_default_executors()


def pytest_configure(config: pytest.Config) -> None:
    """Register StarletteDeprecationWarning filter only when the installed Starlette defines it."""
    import starlette.exceptions as starlette_exc

    if hasattr(starlette_exc, "StarletteDeprecationWarning"):
        config.addinivalue_line("filterwarnings", "ignore::starlette.exceptions.StarletteDeprecationWarning")


def pytest_sessionfinish(session, exitstatus):
    """套件结束: 先停日志监听线程 (flush 残余队列) 并 shutdown 全部 handler (释放 app.log 句柄) 再清理会话日志目录"""
    from app.core.log_setup import shutdown_logging

    shutdown_logging()
    logging.shutdown()
    shutil.rmtree(_RUN_DIR, ignore_errors=True)


@pytest.fixture
def fast_breathing() -> BreathingPolicy:
    """测试零等待呼吸/退避策略 (非 autouse —— 显式注入)。

    生产代码不再感知 PYTEST_CURRENT_TEST; dataclass 字段默认在类创建期固化,
    运行期 monkeypatch 字段无效 —— 故零等待由各测试上下文构造显式注入
    breathing=BreathingPolicy(enabled=False, retry_backoff_s=(0.0, 0.0)) 承载。
    """
    return BreathingPolicy(min_floor_s=0.0, jitter_s=0.0, poll_s=0.01, retry_backoff_s=(0.0, 0.0), enabled=False)


@pytest.fixture
def tmp_dir():
    """在项目 tests/.tmp 下创建可写临时目录

    注意: 不用 tempfile.mkdtemp, 其在 Windows 上会给子目录设置严格 ACL,
    在受限沙箱环境中后续无法写入; os.makedirs 创建的目录继承父目录 ACL。
    """
    import gc
    import time

    path = os.path.join(_RUN_DIR, f"test_{uuid.uuid4().hex[:12]}")
    os.makedirs(path)
    yield path
    gc.collect()
    for _ in range(3):
        try:
            shutil.rmtree(path, ignore_errors=False)
            break
        except Exception:
            time.sleep(0.05)


@pytest.fixture(autouse=True)
def setup_test_db(request, tmp_dir, monkeypatch):
    """为每个测试独立配置临时 SQLite 数据库并清空限流计数与存储目录"""
    from app.api.auth import reset_auth_runtime_state_for_tests
    from app.core.client_manager import client_manager
    from app.core.failure_logger import FailureLogger
    from app.core.scheduler import task_scheduler
    from app.db.database import close_all_thread_connections
    from app.db.write_pool import shutdown_db_write_pool_sync
    from app.services.telegram_login_service import telegram_login_service

    reset_auth_runtime_state_for_tests()
    telegram_login_service.reset_for_tests()
    client_manager._accounts.clear()
    client_manager._bots.clear()
    client_manager._pool_init_task = None
    client_manager._startup_initializing = False
    client_manager._shutting_down = False
    client_manager.startup_degraded = False
    task_scheduler._is_running = False
    task_scheduler.startup_degraded = False

    # 进程内测试沙箱钉回环绑定 (本机免认证的部署前提) —— 隔离开发者 .env 的 HOST=0.0.0.0,
    # 否则统一来源判定会把 TestClient 一律判为公网入口。非回环拓扑用例自行 monkeypatch 覆盖。
    monkeypatch.setattr(settings, "HOST", "127.0.0.1")
    monkeypatch.setattr(settings, "CONSOLE_REQUIRE_API_KEY", False)
    monkeypatch.setattr(settings, "DATA_DIR", os.path.join(tmp_dir, "data"))
    monkeypatch.setattr(settings, "DOWNLOAD_DIR", os.path.join(tmp_dir, "downloads"))
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", os.path.join(tmp_dir, "upload_meta"))
    monkeypatch.setattr(settings, "LOCK_PATH", os.path.join(tmp_dir, "tg_service.lock"))
    monkeypatch.setattr(settings, "SPLIT_STRATEGY", os.getenv("TEST_SPLIT_STRATEGY", "raw"))
    # E2E 捕获模式: 默认关 (回归保障), 捕获目录沙箱化 (即便误开也不污染真实 data/)
    monkeypatch.setattr(settings, "E2E_CAPTURE_MODE", False)
    monkeypatch.setattr(settings, "E2E_CAPTURE_DIR", os.path.join(tmp_dir, "e2e_capture"))
    os.makedirs(settings.DATA_DIR, exist_ok=True)
    os.makedirs(settings.DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(settings.PIKPAK_META_DIR, exist_ok=True)
    os.makedirs(settings.E2E_CAPTURE_DIR, exist_ok=True)

    test_db_path = os.path.join(tmp_dir, "test_tg_service.db")
    monkeypatch.setattr(settings, "DB_PATH", test_db_path)

    # E2E 测试自行编排 scratch 环境或真实后台服务，跳过进程内冗余 DB 初始化与账号灌入
    if request.node.get_closest_marker("e2e_ui") or request.node.get_closest_marker("e2e_live"):
        yield test_db_path
        FailureLogger.close_all()
        return

    init_db()
    yield test_db_path
    # 0. 等待调度器派发的执行器协程在沙箱 DB_PATH 上收尾 (有界 ~1s): 这些协程内经
    #    asyncio.to_thread 落地的 DB 写不归写池管辖, 若在 DB_PATH 复原后才执行会写进真实 data/tg_service.db
    import time

    for _ in range(20):
        if not task_scheduler._running_tasks:
            break
        time.sleep(0.05)
    # 1. 先排空并停用写线程池工作线程
    shutdown_db_write_pool_sync(timeout=0.5)
    # 2. 强力释放所有线程的 SQLite 连接句柄
    close_all_thread_connections()
    # 3. 释放 FailureLogger 打开的文件句柄
    FailureLogger.close_all()
    reset_auth_runtime_state_for_tests()
    telegram_login_service.reset_for_tests()
    client_manager._accounts.clear()
    client_manager._bots.clear()
    client_manager._pool_init_task = None
    client_manager._shutting_down = False
    task_scheduler._is_running = False


@pytest.fixture(autouse=True)
def admitted_business_api_unit_tests(request, monkeypatch):
    """给直接调用业务路由、未运行 lifespan 的单测提供已准入的运行前提。"""
    module = request.path.stem
    direct_route_test_modules = {
        "test_downloader_service",
        "test_forward_service",
        "test_pikpak_resume",
        "test_runtime_infrastructure",
        "test_task_strategies",
        "test_transaction_recovery",
        "test_upload_concurrency",
    }
    if request.path.parent.name != "api" and module not in direct_route_test_modules:
        return
    if module == "test_default_userbot_bootstrap" or request.node.originalname.startswith("test_lifespan_"):
        return
    if request.node.originalname in {
        "test_native_bootstrap_rejects_switching_bound_phone",
        "test_native_routes_require_deployer_token",
        "test_native_verify_success_mints_token",
    }:
        return
    from app.core.client_manager import client_manager

    monkeypatch.setattr(client_manager, "is_default_account_admitted", lambda: True)


@pytest.fixture(autouse=True)
def sandbox_telethon_sessions(tmp_dir, monkeypatch, request):
    """允许本用例的临时会话和 E2E 夹具已创建的 scratch 副本。"""
    import telethon

    roots = [Path(tmp_dir).resolve()]
    roots.extend(
        Path(request.getfixturevalue(fixture)).resolve()
        for fixture in ("tmp_path", "scratch_data_dir", "ui_scratch_dir")
        if fixture in request.fixturenames
    )
    orig_init = telethon.TelegramClient.__init__

    def patched_init(self, session, *args, **kwargs):
        if isinstance(session, (str, os.PathLike)) and session:
            path = Path(tmp_dir) / session
            if not str(path).endswith(".session"):
                path = Path(str(path) + ".session")
            path = path.resolve()
            if not any(path.is_relative_to(root) for root in roots):
                raise ValueError("Telegram session outside test sandbox")
            session = str(path)
        return orig_init(self, session, *args, **kwargs)

    monkeypatch.setattr(telethon.TelegramClient, "__init__", patched_init)


@pytest.fixture(autouse=True)
def ensure_foreign_key_accounts(monkeypatch):
    """自动夹具: 确保单测中任意虚拟 account_id 在写入 task_queue 前已在 tg_accounts 落盘, 满足 PRAGMA foreign_keys = ON"""
    from app.db.database import get_db
    from app.db.task_repository import TaskQueueRepository

    orig_create = TaskQueueRepository.create
    orig_acquire = TaskQueueRepository.acquire_task

    def _ensure_acc(acc_id):
        if acc_id is not None and isinstance(acc_id, int):
            try:
                conn = get_db()
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO tg_accounts
                    (id, account_name, phone, session_file, api_id, api_hash, is_active, is_default, status, max_concurrency, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 123456, 'hash_test', 1, 0, 'unauthenticated', 2, 0.0, 0.0)
                    """,
                    (acc_id, f"AutoTest Account {acc_id}", f"+{acc_id}", f"auto_test_{acc_id}.session"),
                )
                conn.commit()
            except Exception:
                pass

    def patched_create(*args, **kwargs):
        acc_id = kwargs.get("account_id")
        if acc_id is None and len(args) >= 4:
            acc_id = args[3]
        _ensure_acc(acc_id)
        return orig_create(*args, **kwargs)

    def patched_acquire(*args, **kwargs):
        acc_id = kwargs.get("account_id")
        if acc_id is None and len(args) >= 2:
            acc_id = args[1]
        _ensure_acc(acc_id)
        return orig_acquire(*args, **kwargs)

    monkeypatch.setattr(TaskQueueRepository, "create", patched_create)
    monkeypatch.setattr(TaskQueueRepository, "acquire_task", patched_acquire)


@pytest.fixture(autouse=True)
def reset_cdn_session():
    """每个用例隔离并安全释放全局控制面 CDN 会话, 防止跨事件循环污染与 ResourceWarning"""
    from app.core.cdn_transport import close_cdn_session_sync

    close_cdn_session_sync()
    yield
    close_cdn_session_sync()
