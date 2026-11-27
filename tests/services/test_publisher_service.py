"""Business Services: 本地/PikPak 发布核心服务、账号绑定、阈值路由与全跳过总结静默测试 (Publisher Service)

覆盖范围:
- 原 test_publish_threshold: 发布阈值路由 (userbot_upload_threshold_mb)、Bot 池与 Userbot 分流
- 原 test_publish_account_binding: 发布任务账号显式绑定、降级回退与权限预检
- 原 test_publish_skip_summary: 全量已发布媒体跳过频道总结静默与防重计数
- 原 test_pikpak_publisher_service: PikPak 发布服务核心流程、远端树解析、归档清理与相册构建"""

import asyncio
import os
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from _cm_seed import make_bot, seed_default_account
from _safeio import safe_write_bytes
from telethon.errors import ChannelPrivateError, ChatAdminRequiredError

from app.config import settings
from app.core.client_manager import client_manager
from app.core.failure_logger import FailureLogger
from app.core.pikpak import (
    FAILURE_DETAIL_TEXT,
    ParsedFileItem,
    PikPakPublishFatalError,
    PikPakPublishRetryableError,
)
from app.core.pikpak.thumbnail import extract_frame_ffmpeg as thumbnail_extract_frame_ffmpeg
from app.core.pipeline.cleanup import (
    NoopCleanupStrategy,
    PikPakDeleteCleanupStrategy,
    PikPakStructuredArchiveStrategy,
)
from app.core.pipeline.models import PublishContext, PublishItem
from app.core.scheduler import UploadCancelledError, UploadPausedError
from app.core.stream_chunk_provider import (
    BaseChunkProvider,
    ChunkFetchTransientError,
    PikPakLinkExpiredError,
)
from app.core.stream_splitter import derive_split_threshold
from app.db.account_repository import SystemSettingsRepository
from app.db.database import get_db
from app.db.publish_repository import PublishHistoryRepository, norm_file_path
from app.db.task_repository import TaskQueueRepository
from app.models.publish import PresetInspectResponse
from app.services import pikpak_publisher_service as pub
from app.services.channel_inspector import ChannelInspector
from app.services.pikpak_publisher_service import PikPakPublisherService
from app.services.pikpak_service import PikPakAuthError, PikPakNotFoundError, PikPakQuotaError
from app.services.publisher_service import PublisherService

# ==============================================================================
# Part: test_publish_threshold.py
# ==============================================================================
_MB = 1024 * 1024


def _publish_mocks(account_client: AsyncMock):
    """发布链路公共 mock: 绑定账号就绪可用 + 目标频道具备写权限 (参照 test_publish_account_binding 套路)"""
    mock_acc = MagicMock()
    mock_acc.account_id = 99
    mock_acc.account_name = "Userbot_99"
    mock_acc.is_ready.return_value = True
    mock_acc.is_premium = True
    mock_acc.effective_max_file_size_bytes.return_value = 4000 * _MB
    mock_acc.probe_premium = AsyncMock(return_value=True)
    mock_acc.client = account_client

    mock_inspect = AsyncMock(
        return_value={
            "channel_identifier": "-100888",
            "is_accessible": True,
            "has_write_permission": True,
            "permission_summary": "拥有者最高发布权限",
        }
    )
    return mock_acc, mock_inspect


@pytest.mark.asyncio
async def test_threshold_db_setting_routes_files(tmp_dir):
    """设置中心 userbot_upload_threshold_mb=1 (MB) 时: 超过 1MB 的相册/单文件走 Userbot, 未超过走 Bot 池。

    四个子目录各放一个文件, 单次任务内同时覆盖相册路径与单文件路径的双向分流。
    """
    SystemSettingsRepository.set("userbot_upload_threshold_mb", "1", "telegram", "超过该容量切换为 Userbot 发送 (MB)")

    safe_write_bytes(os.path.join(tmp_dir, "sub_album_large", "large_image.jpg"), b"0" * (2 * _MB))
    safe_write_bytes(os.path.join(tmp_dir, "sub_album_small", "small_image.jpg"), b"0" * (512 * 1024))
    safe_write_bytes(os.path.join(tmp_dir, "sub_single_large", "large_video.mp4"), b"0" * (2 * _MB))
    safe_write_bytes(os.path.join(tmp_dir, "sub_single_small", "small_audio.mp3"), b"0" * (512 * 1024))

    account_client = AsyncMock()
    account_client.send_file = AsyncMock()
    account_client.send_message = AsyncMock()
    mock_acc, mock_inspect = _publish_mocks(account_client)

    bot_client = AsyncMock()
    bot_client.send_file = AsyncMock()

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_ft_upload,
        patch.dict(client_manager._bots, {1: make_bot(bot_client)}, clear=True),
        patch.object(client_manager, "_bot_index", 0),
    ):
        mock_ft_upload.return_value = MagicMock()

        res = await PublisherService.publish_folder(
            folder_path=tmp_dir,
            target_chat_id="-100888",
            album_mode=True,
            delay_seconds=0.0,
            task_id="thr_routes_001",
        )

        assert res.published_count == 4
        assert res.failed_files == []
        # 相册两段化: 本地源按子目录分批, 每批单图走 send_album 单成员分支
        # (R3 强制单张 + 逐项预上传) —— large_image(2MB, Userbot) 与 small_image(0.5MB,
        # Bot 池) 各经 upload_file_parallel 预上传; 2MB 单视频走 Userbot send_item。
        # ft 合计 3 次; account send = large_image 单张 + large_video; bot send =
        # small_image 单张 (两段化) + small_audio 直发。路由语义不变: 大→Userbot, 小→Bot。
        assert mock_ft_upload.call_count == 3
        assert account_client.send_file.call_count == 2
        assert bot_client.send_file.call_count == 2


def test_threshold_db_missing_falls_back_to_env_default(tmp_dir):
    """动态项缺失时回退 settings.USERBOT_UPLOAD_THRESHOLD_MB (MB 语义, 默认 30)"""
    with patch.object(SystemSettingsRepository, "get", staticmethod(lambda key, default=None: None)):
        assert (
            SystemSettingsRepository.get_int("userbot_upload_threshold_mb", settings.USERBOT_UPLOAD_THRESHOLD_MB)
            == settings.USERBOT_UPLOAD_THRESHOLD_MB
            == 30
        )


@pytest.mark.asyncio
async def test_threshold_db_missing_routes_by_env_default(tmp_dir):
    """动态项缺失时按环境默认阈值 (30MB) 分流: 阈值 +1MB 的文件走 Userbot"""
    safe_write_bytes(
        os.path.join(tmp_dir, "sub_over_default", "over_threshold.mp4"),
        b"0" * (settings.USERBOT_UPLOAD_THRESHOLD_MB * _MB + _MB),
    )

    account_client = AsyncMock()
    account_client.send_file = AsyncMock()
    account_client.send_message = AsyncMock()
    mock_acc, mock_inspect = _publish_mocks(account_client)

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_ft_upload,
    ):
        mock_ft_upload.return_value = MagicMock()

        res = await PublisherService.publish_folder(
            folder_path=tmp_dir,
            target_chat_id="-100888",
            album_mode=False,
            task_id="thr_fallback_001",
        )

        assert res.published_count == 1
        assert res.failed_files == []
        assert mock_ft_upload.call_count == 1


def test_threshold_env_semantics_is_mb():
    """环境变量 USERBOT_UPLOAD_THRESHOLD 按 MB 解析: 设 30 等效 30MB (子进程隔离验证, env 优先于 .env)"""
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    code = "from app.config import settings; print(settings.USERBOT_UPLOAD_THRESHOLD_MB)"
    env = dict(os.environ)
    env["USERBOT_UPLOAD_THRESHOLD"] = "30"
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=env,
        cwd=repo_root,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip().endswith("30")


def test_threshold_env_seeds_fresh_bootstrap(tmp_dir):
    """全新临时库 + USERBOT_UPLOAD_THRESHOLD=100 时, _bootstrap_from_env 种子值跟随环境变量 (端到端, 无 mock)。

    子进程内先改写 settings.DB_PATH 再 init_db: get_db 每次按 settings.DB_PATH 重键线程本地连接缓存,
    全新解释器无历史连接, 不存在脏缓存路径。
    """
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    code = (
        "import os\n"
        "from app.config import settings\n"
        "settings.DB_PATH = os.environ['THRESHOLD_BOOTSTRAP_DB']\n"
        "from app.db.database import init_db\n"
        "init_db()\n"
        "from app.db.account_repository import SystemSettingsRepository\n"
        "print(SystemSettingsRepository.get_int('userbot_upload_threshold_mb'))\n"
    )
    env = dict(os.environ)
    env["USERBOT_UPLOAD_THRESHOLD"] = "100"
    env["THRESHOLD_BOOTSTRAP_DB"] = os.path.join(tmp_dir, "bootstrap_env.db")
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=env,
        cwd=repo_root,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip().endswith("100")


@pytest.mark.asyncio
async def test_settings_change_applies_to_next_task(tmp_dir):
    """设置中心修改阈值后无需重启, 对下一个发布任务生效: 1MB 时走 Userbot, 调成 999MB 后同文件走 Bot 池"""
    safe_write_bytes(os.path.join(tmp_dir, "sub_hot_change", "video_hot.mp4"), b"0" * (2 * _MB))

    account_client = AsyncMock()
    account_client.send_file = AsyncMock()
    account_client.send_message = AsyncMock()
    mock_acc, mock_inspect = _publish_mocks(account_client)

    bot_client = AsyncMock()
    bot_client.send_file = AsyncMock()

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_ft_upload,
        patch.dict(client_manager._bots, {1: make_bot(bot_client)}, clear=True),
        patch.object(client_manager, "_bot_index", 0),
    ):
        mock_ft_upload.return_value = MagicMock()

        SystemSettingsRepository.set(
            "userbot_upload_threshold_mb", "1", "telegram", "超过该容量切换为 Userbot 发送 (MB)"
        )
        res1 = await PublisherService.publish_folder(
            folder_path=tmp_dir,
            target_chat_id="-100888",
            album_mode=False,
            delay_seconds=0.0,
            task_id="thr_hot_001",
        )
        assert res1.published_count == 1
        assert res1.failed_files == []
        assert mock_ft_upload.call_count == 1  # 2MB > 1MB -> Userbot

        # 模拟设置中心热调大阈值 (复跑任务不重启进程; 更换目标频道以绕开发布防重记录)
        mock_ft_upload.reset_mock()
        bot_client.send_file.reset_mock()
        SystemSettingsRepository.set(
            "userbot_upload_threshold_mb", "999", "telegram", "超过该容量切换为 Userbot 发送 (MB)"
        )
        res2 = await PublisherService.publish_folder(
            folder_path=tmp_dir,
            target_chat_id="-100777",
            album_mode=False,
            delay_seconds=0.0,
            task_id="thr_hot_002",
        )
        assert res2.published_count == 1
        assert res2.failed_files == []
        assert mock_ft_upload.call_count == 0  # 2MB < 999MB -> Bot 池
        assert bot_client.send_file.call_count == 1


@pytest.mark.asyncio
async def test_threshold_read_once_per_task(tmp_dir):
    """阈值在任务开始时读取一次 (任务级快照), 单次 publish_folder 仅触发一次阈值读库"""
    safe_write_bytes(os.path.join(tmp_dir, "sub_snapshot", "video_snapshot.mp4"), b"0" * (512 * 1024))

    account_client = AsyncMock()
    account_client.send_file = AsyncMock()
    account_client.send_message = AsyncMock()
    mock_acc, mock_inspect = _publish_mocks(account_client)

    real_get_int = SystemSettingsRepository.get_int
    threshold_reads = []

    def counting_get_int(key, default=0):
        if key == "userbot_upload_threshold_mb":
            threshold_reads.append(key)
        return real_get_int(key, default)

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
        patch.object(SystemSettingsRepository, "get_int", staticmethod(counting_get_int)),
    ):
        res = await PublisherService.publish_folder(
            folder_path=tmp_dir, target_chat_id="-100888", album_mode=False, task_id="thr_once_001"
        )

        assert res.published_count == 1
        assert res.failed_files == []
        assert threshold_reads.count("userbot_upload_threshold_mb") == 1


# ==============================================================================
# Part: test_publish_account_binding.py
# ==============================================================================
def test_failure_logger_classify_telegram_permissions():
    """测试 FailureLogger 错误分类器精准识别 Telegram 远程权限拒绝 RPC，不再误判为 FILE_IO_ERROR"""
    # 1. 现场复现报错: Chat admin privileges are required to do that in the specified chat...
    err1 = Exception("Chat admin privileges are required to do that in the specified chat (caused by SendMediaRequest)")
    assert FailureLogger.classify_error(err1) == "TG_NO_PERMISSION"

    # 2. Telethon ChatAdminRequiredError
    err2 = ChatAdminRequiredError(request=None)
    assert FailureLogger.classify_error(err2) == "TG_NO_PERMISSION"

    # 3. ChannelPrivateError
    err3 = ChannelPrivateError(request=None)
    assert FailureLogger.classify_error(err3) == "TG_NO_PERMISSION"

    # 4. 其他常见 Telegram 权限错误文本
    assert FailureLogger.classify_error(Exception("CHAT_WRITE_FORBIDDEN")) == "TG_NO_PERMISSION"
    assert FailureLogger.classify_error(Exception("chat_send_media_forbidden")) == "TG_NO_PERMISSION"
    assert FailureLogger.classify_error(Exception("🛑 [写权限前置熔断] 执行账号无发帖权限")) == "TG_NO_PERMISSION"

    # 5. 本地文件系统 I/O 错误必须归为 FILE_IO_ERROR
    err_file1 = PermissionError("[Errno 13] Permission denied: '/tmp/test.mp4'")
    assert FailureLogger.classify_error(err_file1) == "FILE_IO_ERROR"

    err_file2 = FileNotFoundError("[Errno 2] No such file or directory: 'photo.jpg'")
    assert FailureLogger.classify_error(err_file2) == "FILE_IO_ERROR"

    err_file3 = OSError("access is denied on local disk")
    assert FailureLogger.classify_error(err_file3) == "FILE_IO_ERROR"


@pytest.mark.asyncio
async def test_scheduler_exec_publish_folder_passes_account_id():
    """验证调度器注册的 exec_publish_folder 能把 account_id 正确透传至 run_publish_folder_task"""
    from app.core.scheduler import TaskScheduler
    from app.services.executor_registry import register_default_executors

    with patch("app.services.publish_runner.run_publish_folder_task", new_callable=AsyncMock) as mock_run:
        scheduler = TaskScheduler()
        register_default_executors(scheduler)

        handler = scheduler._executors.get("publish_folder")
        assert handler is not None

        params = {
            "folder_path": "/fake/folder",
            "target_channel": "-100900008",
            "album_mode": True,
            "delay_seconds": 2.0,
        }
        await handler("task_123", params, account_id=42)

        # D4: execute 经表驱动 extract_run_params 以 kwargs 调用 runner
        mock_run.assert_called_once_with(
            "task_123",
            folder_path="/fake/folder",
            target_channel="-100900008",
            album_mode=True,
            delay_seconds=2.0,
            account_id=42,
        )


@pytest.mark.asyncio
async def test_scheduler_exec_download_executors_pass_account_id():
    """验证调度器注册的 download_link 与 download_range 执行器能正确透传 account_id 和 force"""
    from app.core.scheduler import TaskScheduler
    from app.services.executor_registry import register_default_executors

    with (
        patch("app.services.download_runner.run_download_links_task", new_callable=AsyncMock) as mock_links,
        patch("app.services.download_runner.run_download_range_task", new_callable=AsyncMock) as mock_range,
    ):
        scheduler = TaskScheduler()
        register_default_executors(scheduler)

        # 1. download_link
        handler_link = scheduler._executors.get("download_link")
        assert handler_link is not None
        await handler_link("task_link", {"links": ["https://t.me/c/1/2"], "threads": 8}, account_id=101)
        mock_links.assert_called_once_with(
            "task_link", links=["https://t.me/c/1/2"], threads=8, account_id=101, force=False
        )

        # 2. download_range
        handler_range = scheduler._executors.get("download_range")
        assert handler_range is not None
        await handler_range(
            "task_range",
            {"channel": "mychan", "start_id": 1, "end_id": 10, "threads": 16, "force": True},
            account_id=202,
        )
        mock_range.assert_called_once_with(
            "task_range", channel_identifier="mychan", start_id=1, end_id=10, threads=16, account_id=202, force=True
        )


@pytest.mark.asyncio
async def test_scheduler_exec_forward_channel_passes_account_id():
    """验证调度器注册的 forward_channel 执行器能正确透传 account_id"""
    from app.core.scheduler import TaskScheduler
    from app.services.executor_registry import register_default_executors

    with patch("app.services.forward_service.run_forward_channel_task", new_callable=AsyncMock) as mock_fwd:
        scheduler = TaskScheduler()
        register_default_executors(scheduler)

        handler = scheduler._executors.get("forward_channel")
        assert handler is not None
        params = {
            "source_chat": "-1001",
            "target_chat": "-1002",
            "start_id": 10,
            "end_id": 20,
            "batch_size": 30,
            "delay_seconds": 1.0,
            "full_retransfer": True,
            "dry_run": False,
        }
        await handler("task_fwd", params, account_id=303)
        mock_fwd.assert_called_once_with(
            "task_fwd",
            source_chat="-1001",
            target_chat="-1002",
            start_id=10,
            end_id=20,
            batch_size=30,
            delay_seconds=1.0,
            full_retransfer=True,
            dry_run=False,
            account_id=303,
        )


@pytest.mark.asyncio
async def test_publisher_service_preflight_guard_aborts_when_no_permission(tmp_dir):
    """验证后台 Pre-flight Guard: 当目标频道无发帖权限时，0 字节网络传输直接中止任务并熔断"""
    pub_dir = os.path.join(tmp_dir, "preflight_pub")
    os.makedirs(pub_dir, exist_ok=True)
    # 创建一个测试媒体文件
    safe_write_bytes(os.path.join(pub_dir, "test_video.mp4"), b"0" * (1024 * 100))  # 100KB

    # Mock ChannelInspector.inspect_channel 返回无写权限
    mock_inspect = AsyncMock(
        return_value={
            "channel_identifier": "-1009999",
            "is_accessible": True,
            "has_write_permission": False,
            "permission_summary": "非频道管理员，无发帖权限",
        }
    )

    with (
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_upload,
    ):
        res = await PublisherService.publish_folder(
            folder_path=pub_dir, target_chat_id="-1009999", task_id="test_preflight_001", account_id=None
        )

        # 验证发布被前置熔断：发布数为 0，并且未发生任何网络上传
        assert res.published_count == 0
        assert len(res.failed_files) > 0
        assert "目标频道权限不足" in res.failed_files[0]
        mock_upload.assert_not_called()


@pytest.mark.asyncio
async def test_publisher_service_uses_bound_account_client(tmp_dir):
    """验证 PublisherService 精确绑定指定的 account_id 客户端与动态 Premium 上限"""
    pub_dir = os.path.join(tmp_dir, "bound_pub")
    os.makedirs(pub_dir, exist_ok=True)
    safe_write_bytes(os.path.join(pub_dir, "test_large.mp4"), b"0" * (35 * 1024 * 1024))  # 35MB，触发 Userbot 上传阈值

    # 创建 Mock 账号
    mock_acc = MagicMock()
    mock_acc.account_id = 99
    mock_acc.account_name = "Userbot_99"
    mock_acc.is_ready.return_value = True
    mock_acc.is_premium = True
    mock_acc.effective_max_file_size_bytes.return_value = 4000 * 1024 * 1024
    mock_acc.probe_premium = AsyncMock(return_value=True)

    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)
    mock_client.send_file = AsyncMock()
    mock_client.send_message = AsyncMock()
    mock_acc.client = mock_client

    # Mock inspect_channel 具备写权限
    mock_inspect = AsyncMock(
        return_value={
            "channel_identifier": "-100888",
            "is_accessible": True,
            "has_write_permission": True,
            "permission_summary": "拥有者最高发布权限",
        }
    )

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_ft_upload,
    ):
        mock_ft_upload.return_value = MagicMock()

        res = await PublisherService.publish_folder(
            folder_path=pub_dir, target_chat_id="-100888", task_id="test_bound_acc_001", account_id=99
        )

        # 验证确实使用了该 mock_acc.client 进行并行上传和发送
        assert mock_ft_upload.call_count == 1
        assert mock_ft_upload.call_args[0][0] == mock_client
        assert mock_client.send_file.call_count == 1
        # 验证总结消息也是由绑定账号发送
        assert mock_client.send_message.call_count == 2
        assert res.published_count == 1


@pytest.mark.asyncio
async def test_client_manager_get_available_account_prefers_channel_admin():
    """验证调度器为 publish_folder 任务选号时，优先选择在目标频道具备管理员写权限的账号"""
    acc_non_admin = MagicMock()
    acc_non_admin.account_id = 1
    acc_non_admin.account_name = "Userbot_NonAdmin"
    acc_non_admin.is_active = True
    acc_non_admin.is_ready.return_value = True
    acc_non_admin.is_flood_locked.return_value = False
    acc_non_admin.active_task_count = 0
    acc_non_admin.max_concurrency = 2
    acc_non_admin.client = MagicMock()
    acc_non_admin.client.is_connected.return_value = True

    acc_admin = MagicMock()
    acc_admin.account_id = 2
    acc_admin.account_name = "Userbot_Admin"
    acc_admin.is_active = True
    acc_admin.is_ready.return_value = True
    acc_admin.is_flood_locked.return_value = False
    acc_admin.active_task_count = 0
    acc_admin.max_concurrency = 2
    acc_admin.client = MagicMock()
    acc_admin.client.is_connected.return_value = True

    async def mock_probe_write(client, target_chat):
        if client == acc_admin.client:
            return True
        return False

    with (
        patch.object(client_manager, "_accounts", {1: acc_non_admin, 2: acc_admin}),
        patch("app.core.account_selection.probe_write_permission", side_effect=mock_probe_write),
    ):
        selected_acc = await client_manager.get_available_account("publish_folder", target_chat="-100555")

        assert selected_acc is not None
        assert selected_acc.account_id == 2
        assert selected_acc.account_name == "Userbot_Admin"


@pytest.mark.asyncio
async def test_channel_inspector_respects_account_id():
    """验证 ChannelInspector.inspect_channel 支持指定 account_id 解析并调用其 client"""
    mock_acc = MagicMock()
    mock_acc.account_id = 77
    mock_acc.is_ready.return_value = True

    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)
    mock_entity = MagicMock()
    mock_entity.title = "测试频道"
    mock_entity.creator = True
    mock_entity.broadcast = True
    mock_entity.noforwards = False

    mock_client.get_messages = AsyncMock(return_value=[])
    mock_acc.client = mock_client

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.forward_service.ForwardService.resolve_entity", AsyncMock(return_value=mock_entity)),
    ):
        res = await ChannelInspector.inspect_channel("-100777", account_id=77)

        assert res["is_accessible"] is True
        assert res["has_write_permission"] is True
        assert res["channel_title"] == "测试频道"
        assert res["account_id"] == 77


@pytest.mark.asyncio
async def test_channel_inspector_no_fallback_on_unready_account():
    """验证 ChannelInspector 在指定账号不存在或未就绪时，坚决不向默认账号静默降级"""
    from app.services.channel_inspector import ChannelInspector

    with patch("app.core.client_manager.client_manager.get_account", return_value=None):
        res = await ChannelInspector.inspect_channel("-100777", account_id=999)
        assert res["is_accessible"] is False
        assert "未找到 ID 为 999 的 Userbot 账号" in res["permission_summary"]
        assert res["account_id"] == 999

        # 确保通过 Pydantic 校验
        validated = PresetInspectResponse(**res)
        assert validated.account_id == 999
        assert validated.has_write_permission is False


# D4 注销: test_background_fallback_compatible_with_legacy_callable_signature /
# test_background_fallback_uninspectable_or_positional_only_callable /
# test_download_and_publish_background_fallback_compatible_with_legacy_callable
# 三用例专测已废除的 BackgroundTasks 即跑兜底通道 (callable_supports_kwarg 探测形参
# + monkeypatch api 模块 run_* 直跑), 派发统一入 task_queue + scheduler 后机制不复存在。

# ==============================================================================
# Part: test_publish_skip_summary.py
# ==============================================================================
_MB = 1024 * 1024


def _publish_mocks(account_client: AsyncMock):
    mock_acc = MagicMock()
    mock_acc.account_id = 99
    mock_acc.account_name = "Userbot_99"
    mock_acc.is_ready.return_value = True
    mock_acc.is_premium = True
    mock_acc.effective_max_file_size_bytes.return_value = 4000 * _MB
    mock_acc.probe_premium = AsyncMock(return_value=True)
    mock_acc.client = account_client

    mock_inspect = AsyncMock(
        return_value={
            "channel_identifier": "-100888",
            "is_accessible": True,
            "has_write_permission": True,
            "permission_summary": "拥有发帖权限",
        }
    )
    return mock_acc, mock_inspect


@pytest.mark.asyncio
async def test_publish_folder_skips_summary_when_all_files_already_published(tmp_dir):
    """验证当文件夹内所有媒体文件均已在历史记录中发布过时：
    1. 不发送任何媒体文件 (send_file.call_count == 0)
    2. 跳过向频道发送总结消息 (send_message.call_count == 0)
    3. published_count 依然正确累加 (防重跳过计数正确)
    """
    sub_dir = os.path.join(tmp_dir, "batch_folder_all_done")
    os.makedirs(sub_dir, exist_ok=True)
    f1 = safe_write_bytes(os.path.join(sub_dir, "v1.mp4"), b"dummy content 1")
    f2 = safe_write_bytes(os.path.join(sub_dir, "v2.mp4"), b"dummy content 2")

    target_chat_id = "-100888"
    # 模拟先前已成功发布并入库
    PublishHistoryRepository.record(target_chat_id, f1, os.path.getsize(f1))
    PublishHistoryRepository.record(target_chat_id, f2, os.path.getsize(f2))

    account_client = AsyncMock()
    account_client.send_file = AsyncMock()
    account_client.send_message = AsyncMock()
    mock_acc, mock_inspect = _publish_mocks(account_client)

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock),
    ):
        res = await PublisherService.publish_folder(
            folder_path=sub_dir,
            target_chat_id=target_chat_id,
            album_mode=False,
            delay_seconds=0.01,
            task_id="skip_test_001",
        )

        assert res.published_count == 2
        assert res.failed_files == []
        # 无需重新上传
        assert account_client.send_file.call_count == 0
        # 核心断言：完全跳过发送总结消息，避免刷屏目标频道
        assert account_client.send_message.call_count == 0


@pytest.mark.asyncio
async def test_publish_folder_force_republish_bypasses_dedup(tmp_dir):
    """包C: force_republish=True 忽略已发布防重 → 文件重发 + 总结发送 + 发布历史幂等覆盖。"""
    sub_dir = os.path.join(tmp_dir, "force_republish_dir")
    os.makedirs(sub_dir, exist_ok=True)
    f1 = safe_write_bytes(os.path.join(sub_dir, "v1.mp4"), b"dummy content 1")

    target_chat_id = "-100888"
    PublishHistoryRepository.record(target_chat_id, f1, os.path.getsize(f1))
    TaskQueueRepository.create(
        task_id="force_test_001",
        task_type="publish_folder",
        params={"target_channel": target_chat_id, "folder_path": sub_dir},
    )

    account_client = AsyncMock()
    account_client.send_file = AsyncMock()
    account_client.send_message = AsyncMock()
    mock_acc, mock_inspect = _publish_mocks(account_client)

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock),
    ):
        res = await PublisherService.publish_folder(
            folder_path=sub_dir,
            target_chat_id=target_chat_id,
            album_mode=False,
            delay_seconds=0.01,
            task_id="force_test_001",
            force_republish=True,
        )

        assert res.published_count == 1
        assert account_client.send_file.call_count == 1, "force_republish 必须重发已发布文件"
        # 双卡制: 批次总结卡 + 任务汇总卡
        assert account_client.send_message.call_count == 2
        logs, _, _ = TaskQueueRepository.get_task_logs("force_test_001")
        assert any("强制重传" in e["message"] for e in logs)
    # 发布历史幂等 (record 覆盖式, 不重复行; 写入侧已归一, 查询按 norm_file_path 同口径)
    from app.db.database import get_db

    row = (
        get_db()
        .execute("SELECT COUNT(*) AS c FROM publish_history WHERE file_path=?", (norm_file_path(f1),))
        .fetchone()
    )
    assert row["c"] == 1


@pytest.mark.asyncio
async def test_publish_folder_multi_batch_suppresses_only_completed_batches(tmp_dir):
    """验证多子目录批量任务中：
    - sub_done: 全部已发布 -> 抑制总结
    - sub_new: 未发布 -> 正常发送媒体并发送总结
    """
    sub_done = os.path.join(tmp_dir, "sub_done")
    sub_new = os.path.join(tmp_dir, "sub_new")
    os.makedirs(sub_done, exist_ok=True)
    os.makedirs(sub_new, exist_ok=True)

    f_done1 = safe_write_bytes(os.path.join(sub_done, "d1.mp4"), b"content d1")
    f_done2 = safe_write_bytes(os.path.join(sub_done, "d2.mp4"), b"content d2")
    safe_write_bytes(os.path.join(sub_new, "n1.mp4"), b"content n1")

    target_chat_id = "-100888"
    PublishHistoryRepository.record(target_chat_id, f_done1, os.path.getsize(f_done1))
    PublishHistoryRepository.record(target_chat_id, f_done2, os.path.getsize(f_done2))

    account_client = AsyncMock()
    account_client.send_file = AsyncMock()
    account_client.send_message = AsyncMock()
    mock_acc, mock_inspect = _publish_mocks(account_client)

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock),
    ):
        res = await PublisherService.publish_folder(
            folder_path=tmp_dir,
            target_chat_id=target_chat_id,
            album_mode=False,
            delay_seconds=0.01,
            task_id="multi_batch_test_002",
        )

        assert res.published_count == 3
        assert res.failed_files == []
        # 仅新子目录的 1 个文件被发送
        assert account_client.send_file.call_count == 1
        # 仅新子目录发送了 1 张批次总结卡，已完成的 sub_done 未发批次卡
        #
        assert account_client.send_message.call_count == 2
        texts = [c.args[1] for c in account_client.send_message.call_args_list]
        batch_cards = [t for t in texts if "发布文件夹" in t]
        assert len(batch_cards) == 1
        assert "sub_new" in batch_cards[0]
        assert "sub_done" not in batch_cards[0]
        summary = [t for t in texts if "任务汇总" in t]
        assert len(summary) == 1


@pytest.mark.asyncio
async def test_publish_folder_sends_summary_on_partial_resume(tmp_dir):
    """验证断点续传场景（同一文件夹内部分已发布，部分未发布）：
    - 未发布的文件被成功发送后，正常向频道发送该文件夹的完整总结消息
    """
    sub_partial = os.path.join(tmp_dir, "sub_partial")
    os.makedirs(sub_partial, exist_ok=True)

    f_old = safe_write_bytes(os.path.join(sub_partial, "old.mp4"), b"old content")
    safe_write_bytes(os.path.join(sub_partial, "new.mp4"), b"new content")

    target_chat_id = "-100888"
    PublishHistoryRepository.record(target_chat_id, f_old, os.path.getsize(f_old))

    account_client = AsyncMock()
    account_client.send_file = AsyncMock()
    account_client.send_message = AsyncMock()
    mock_acc, mock_inspect = _publish_mocks(account_client)

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock),
    ):
        res = await PublisherService.publish_folder(
            folder_path=sub_partial,
            target_chat_id=target_chat_id,
            album_mode=False,
            delay_seconds=0.01,
            task_id="partial_resume_test_003",
        )

        assert res.published_count == 2
        assert res.failed_files == []
        # 新文件被发送
        assert account_client.send_file.call_count == 1
        # 文件夹在此次运行中完成了续传，因此向频道发送总结消息
        #
        assert account_client.send_message.call_count == 2
        texts = [c.args[1] for c in account_client.send_message.call_args_list]
        batch_card = [t for t in texts if "发布文件夹" in t][0]
        assert "2/2 个媒体文件" in batch_card


@pytest.mark.asyncio
async def test_publish_folder_sends_summary_when_folder_has_failures(tmp_dir):
    """D3 适配: 超限文件从 failed 切换为 skipped_files + 占位卡 —— 本次零成功发送时
    若存在超限跳过 (或失败)，依然向频道发送含超限清单的总结消息 (不被静默抑制)。"""
    sub_fail = os.path.join(tmp_dir, "sub_fail")
    os.makedirs(sub_fail, exist_ok=True)
    # 创建一个超大文件 (2MB)，而 mock 限制设为 1MB
    safe_write_bytes(os.path.join(sub_fail, "oversized.mp4"), b"0" * (2 * _MB))

    target_chat_id = "-100888"
    account_client = AsyncMock()
    account_client.send_file = AsyncMock()
    account_client.send_message = AsyncMock()
    mock_acc, mock_inspect = _publish_mocks(account_client)
    mock_acc.effective_max_file_size_bytes.return_value = 1 * _MB

    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
        patch("app.services.channel_inspector.ChannelInspector.inspect_channel", mock_inspect),
    ):
        res = await PublisherService.publish_folder(
            folder_path=sub_fail,
            target_chat_id=target_chat_id,
            album_mode=False,
            delay_seconds=0.01,
            task_id="fail_test_004",
        )

        assert res.published_count == 0
        # D3: 超限 → skipped_files (非 failed)
        assert len(res.skipped_files) == 1
        assert res.failed_files == []
        assert account_client.send_file.call_count == 0
        # 占位卡 (超限缺集说明) + 批次总结卡 (含超限清单) + 任务汇总卡 —— 三条频道消息
        assert account_client.send_message.call_count == 3
        texts = [c.args[1] for c in account_client.send_message.call_args_list]
        batch_card = [t for t in texts if "发布文件夹" in t][0]
        assert "超限跳过" in batch_card
        assert any("任务汇总" in t for t in texts)


# ==============================================================================
# Part: test_pikpak_publisher_service.py
# ==============================================================================
_MB = 1024 * 1024
TARGET = "-100888"


# ---------------------------------------------------------------------------
# 测试替身与夹具
# ---------------------------------------------------------------------------


class _StubProvider(BaseChunkProvider):
    """冻结 PikPakStreamChunkProvider 的测试替身: 构造签名一致, read_chunk 从内存缓冲读。

    read_behavior: 可注入的可调用 (offset, size) -> bytes (或协程), 可抛异常脚本化失败。
    """

    def __init__(self, file_id, file_name, total_bytes, url_provider, request_timeout=60.0, **kwargs):
        self.file_id = file_id
        self.file_name = file_name
        self.total_bytes = total_bytes
        self._url_provider = url_provider
        self.request_timeout = request_timeout
        self.read_calls = 0
        self.url_fetches = 0
        self.closed = False
        self.read_behavior = None

    async def read_chunk(self, offset, size):
        self.read_calls += 1
        if self.read_behavior is not None:
            result = self.read_behavior(offset, size)
            if asyncio.iscoroutine(result):
                result = await result
            return result
        if offset >= self.total_bytes or size <= 0:
            raise ValueError("out of range")
        return b"\xab" * (min(offset + size, self.total_bytes) - offset)

    async def aclose(self):
        self.closed = True


def _make_item(
    file_id="F1", name="video.mp4", size_bytes=5 * _MB, parent_id="P0", audit=None, duration_s="12", thumbnail_link=None
):
    return ParsedFileItem(
        file_id=file_id,
        name=name,
        is_dir=False,
        size_bytes=size_bytes,
        audit=audit if audit is not None else {"status": "OK"},
        thumbnail_link=thumbnail_link,
        duration_s=duration_s,
        width="1920",
        height="1080",
        parent_id=parent_id,
    )


@pytest.fixture
def publisher_env(tmp_dir, monkeypatch):
    """统一 mock 环境: meta 沙箱 / Userbot 替身 / 引擎与 JIT 索链 mock / 预检放行 / 替身 Provider。"""
    sandbox = os.path.join(tmp_dir, "upload_meta")
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", sandbox)

    client = AsyncMock()
    sent = MagicMock()
    sent.id = 4321
    client.send_file = AsyncMock(return_value=sent)

    engine_calls = []

    async def fake_upload_file_parallel(client_, file_path, **kwargs):
        engine_calls.append({"file_path": file_path, **kwargs})
        provider = kwargs.get("chunk_provider")
        if provider is not None and provider.total_bytes > 0:
            await provider.read_chunk(0, min(64, provider.total_bytes))  # 触发测量包装统计
        return f"media-{len(engine_calls)}"

    # 引擎经函数级 import 于调用时解析冻结引擎模块属性 → patch 唯一来源 = fast_telethon
    monkeypatch.setattr("app.core.fast_telethon.upload_file_parallel", fake_upload_file_parallel)

    link_calls = []

    async def fake_get_download_links(file_ids, **kwargs):
        link_calls.append(list(file_ids))
        return [f"https://cdn.fake.test/dl/{fid}?X-Sign=FAKESIGN&Expires=9999999999" for fid in file_ids]

    monkeypatch.setattr(pub, "get_download_links", fake_get_download_links)

    async def fake_inspect(*args, **kwargs):
        return {"is_accessible": True, "has_write_permission": True, "permission_summary": "拥有发帖权限"}

    monkeypatch.setattr("app.services.channel_inspector.ChannelInspector.inspect_channel", fake_inspect)

    _acc = seed_default_account(client_manager, client=client, is_premium=False)
    monkeypatch.setattr(client_manager, "_accounts", {_acc.account_id: _acc})
    refresh_mock = AsyncMock(return_value=False)
    monkeypatch.setattr(client_manager, "refresh_premium", refresh_mock, raising=False)
    # 冻结 Provider 同上 → patch 唯一来源 = stream_chunk_provider 模块属性
    monkeypatch.setattr("app.core.stream_chunk_provider.PikPakStreamChunkProvider", _StubProvider)
    # 单测中直链均为 fake.test 虚拟域，拦截真实 FFmpeg HTTP Range 抽帧进程杜绝单文件 12s 超时假死
    monkeypatch.setattr("app.core.pikpak.thumbnail.extract_frame_ffmpeg", AsyncMock(return_value=None))
    # 彻底拦截测试期真实 pikpak CLI 进程调用 (杜绝无凭证下单次操作 12s 进程超时挂死)
    monkeypatch.setattr("app.services.pikpak_service.resolve_archive_folder", AsyncMock(return_value="archive_root_id"))
    monkeypatch.setattr("app.services.pikpak_service.archive_files", AsyncMock(return_value={"moved": 1}))
    monkeypatch.setattr("app.services.pikpak_service.archive_files_to_folder", AsyncMock(return_value={"moved": 1}))
    monkeypatch.setattr("app.services.pikpak_service.delete_files", AsyncMock(return_value={"deleted": 1}))
    monkeypatch.setattr("app.services.pikpak_service.list_files_raw", AsyncMock(return_value=([], None)))
    monkeypatch.setattr("app.services.pikpak_service.get_thumbnail_link", AsyncMock(return_value=None))

    return SimpleNamespace(
        client=client,
        engine_calls=engine_calls,
        link_calls=link_calls,
        sandbox=sandbox,
        refresh_mock=refresh_mock,
    )


async def _publish(env, items, **kwargs):
    defaults = dict(target_chat_id=TARGET, task_id="t1")
    defaults.update(kwargs)
    return await PikPakPublisherService.publish_pikpak_files(remote_files=items, **defaults)


# ---------------------------------------------------------------------------
# 骨架用例 (包B)
# ---------------------------------------------------------------------------


async def test_requires_userbot_fatal_without_client(publisher_env, monkeypatch):
    monkeypatch.setattr(client_manager, "_accounts", {})
    with pytest.raises(PikPakPublishFatalError) as ei:
        await _publish(publisher_env, [_make_item()])
    assert ei.value.category == "userbot"
    assert "Userbot" in str(ei.value)


async def test_premium_snapshot_drives_threshold(publisher_env, monkeypatch):
    """指定账号 probe_premium(force=True) 快照驱动阈值联动: premium 1 Part / 非 premium 2 Part。"""
    mock_acc = MagicMock()
    mock_acc.is_ready.return_value = True
    mock_acc.client = publisher_env.client
    mock_acc.account_name = "acc1"
    mock_acc.probe_premium = AsyncMock()
    monkeypatch.setattr(client_manager, "get_account", lambda account_id=None: mock_acc, raising=False)

    over_non_premium = derive_split_threshold(False) + 1  # 超非 premium 阈值

    mock_acc.is_premium = True
    res = await _publish(publisher_env, [_make_item(size_bytes=over_non_premium)], account_id=7)
    assert res.published_count == 1
    assert len(publisher_env.engine_calls) == 1  # premium 阈值 3900MB → 单 Part
    mock_acc.probe_premium.assert_awaited_once_with(force=True)

    publisher_env.engine_calls.clear()
    mock_acc.is_premium = False
    res = await _publish(publisher_env, [_make_item(file_id="F2", size_bytes=over_non_premium)], account_id=7)
    assert res.published_count == 1
    assert len(publisher_env.engine_calls) == 2  # 非 premium 阈值 1950MB → 2 Part


async def test_default_account_refresh_premium_fallback(publisher_env, monkeypatch):
    """默认账号未就绪 (FLOOD_WAIT) → preflight 走 CM 级 refresh_premium(force=True) 回退分支。"""
    from app.core.client_manager import client_manager as _cm

    not_ready_acc = seed_default_account(
        client_manager,
        client=publisher_env.client,
        auth_state="FLOOD_WAIT",
    )
    monkeypatch.setattr(_cm, "get_account", lambda account_id=None: not_ready_acc)

    async def recover_account(*, force):
        assert force is True
        not_ready_acc.auth_state = "AUTHENTICATED"
        return False

    publisher_env.refresh_mock.side_effect = recover_account
    await _publish(publisher_env, [_make_item()])
    publisher_env.refresh_mock.assert_awaited_once_with(force=True)


async def test_audit_blocked_file_skipped(publisher_env, caplog):
    """audit 非通过 → 跳过 + 告警 (UI 标红数据源); audit 空缺宽容放行。"""
    import logging

    with caplog.at_level(logging.WARNING, logger="app.services.pikpak_publisher_service"):
        res = await _publish(publisher_env, [_make_item(audit={"status": "FAIL", "message": "risk"})])
    assert res.published_count == 0
    assert len(res.failed_files) == 1
    assert "审核状态未通过" in res.failed_files[0]
    assert publisher_env.engine_calls == []  # 引擎零调用
    assert any("audit 拦截" in r.message for r in caplog.records)

    # audit 字段缺失 → 宽容放行
    res = await _publish(publisher_env, [_make_item(file_id="F2", audit={})])
    assert res.published_count == 1
    assert len(publisher_env.engine_calls) == 1

    # audit.status == "STATUS_OK" (真实 CLI 样本, DEF-02 修复) → 正常放行
    publisher_env.engine_calls.clear()
    res = await _publish(publisher_env, [_make_item(file_id="F3", audit={"status": "STATUS_OK"})])
    assert res.published_count == 1
    assert len(publisher_env.engine_calls) == 1

    # audit.status == "OK" (既有兼容值) → 正常放行
    publisher_env.engine_calls.clear()
    res = await _publish(publisher_env, [_make_item(file_id="F4", audit={"status": "OK"})])
    assert res.published_count == 1
    assert len(publisher_env.engine_calls) == 1


async def test_dedup_hit_skips(publisher_env):
    """PikPak 防重命中 (source_type='pikpak' + remote_file_id) → 跳过且引擎零调用、不重复入库。"""
    PublishHistoryRepository.record(
        TARGET, "pikpak://P0/video.mp4", 5 * _MB, "", source_type="pikpak", remote_file_id="F1"
    )
    res = await _publish(publisher_env, [_make_item()])
    assert res.published_count == 1  # 防重跳过计入已发布 (对齐 publish_folder 计数语义)
    assert publisher_env.engine_calls == []
    conn = get_db()
    rows = conn.execute("SELECT COUNT(*) AS c FROM publish_history WHERE remote_file_id='F1'").fetchone()
    assert rows["c"] == 1  # 未重复入库


async def test_force_republish_bypasses_dedup(publisher_env):
    """包C: force_republish=True 忽略防重强制重传 (引擎重发 + 🔁 日志 + 发布历史幂等覆盖)。"""
    PublishHistoryRepository.record(
        TARGET, "pikpak://P0/video.mp4", 5 * _MB, "", source_type="pikpak", remote_file_id="F1"
    )
    TaskQueueRepository.create("t1", "pikpak_publish", {"target_channel": TARGET})
    res = await _publish(publisher_env, [_make_item()], force_republish=True)
    assert res.published_count == 1
    assert len(publisher_env.engine_calls) == 1, "force_republish 必须重发已发布文件"
    conn = get_db()
    rows = conn.execute("SELECT COUNT(*) AS c FROM publish_history WHERE remote_file_id='F1'").fetchone()
    assert rows["c"] == 1  # record 覆盖式幂等 (不重复行)
    logs, _, _ = TaskQueueRepository.get_task_logs("t1")
    assert any("强制重传" in e["message"] for e in logs)


async def test_structured_archive_grouped_mv(monkeypatch):
    """包D: archive 模式按 dir_tree 重建目录树后分组 mv (根级归档 + 逐级幂等 mkdir + 定点归档); 摘要落任务日志。
    活实现 = PikPakStructuredArchiveStrategy (构造注入), 原死代码 _run_structured_archive 已删。"""
    TaskQueueRepository.create("t-arch", "pikpak_publish", {"target_channel": TARGET})
    resolve_calls = []
    to_folder_calls = []
    flat_calls = []

    async def fake_resolve(parent_id="", name="已发布归档", **kw):
        resolve_calls.append((parent_id, name))
        return f"folder-{name}-{len(resolve_calls)}"

    async def fake_to_folder(file_ids, *, folder_id, source_parent_id="", **kw):
        to_folder_calls.append((list(file_ids), folder_id, source_parent_id))
        return {"archive_folder_id": folder_id, "moved": len(file_ids)}

    async def fake_flat(file_ids, *, source_parent_id="", **kw):
        flat_calls.append((list(file_ids), source_parent_id))
        return {"archive_folder_id": "flat", "moved": len(file_ids)}

    strategy = PikPakStructuredArchiveStrategy(
        resolve_archive_folder=fake_resolve,
        archive_files_to_folder=fake_to_folder,
        archive_files=fake_flat,
    )
    items = [
        PublishItem(item_id="FA", name="a.mp4", size_bytes=1, parent_folder_id="P0"),
        PublishItem(item_id="FB", name="b.mp4", size_bytes=2, parent_folder_id="P0"),
    ]
    ctx = PublishContext(
        task_id="t-arch",
        target_chat_id=TARGET,
        dir_tree={"P0": "Test/已发布"},
        auto_cleanup_mode="archive",
    )

    summary = await strategy.cleanup(items, ctx)

    assert resolve_calls[0] == ("", "已发布归档"), "先幂等解析根级 [已发布归档]"
    assert resolve_calls[1] == ("folder-已发布归档-1", "Test")
    assert resolve_calls[2] == ("folder-Test-2", "已发布")
    assert to_folder_calls == [(["FA", "FB"], "folder-已发布-3", "P0")], "分组定点 mv"
    assert flat_calls == []
    assert "Test/已发布/ 2" in summary
    logs, _, _ = TaskQueueRepository.get_task_logs("t-arch")
    # D5: 归档成功摘要移出任务日志 (改由频道总结卡归档行呈现)
    assert not any("已归档" in e["message"] for e in logs)


async def test_structured_archive_flat_fallback(monkeypatch):
    """包D: 无 dir_tree (裸文件/旧任务) → 根级平铺归档回退路径 (兼容既有语义)。
    活实现 = PikPakStructuredArchiveStrategy (构造注入)。"""
    TaskQueueRepository.create("t-arch2", "pikpak_publish", {"target_channel": TARGET})
    to_folder_calls = []
    flat_calls = []

    async def fake_resolve(parent_id="", name="已发布归档", **kw):
        return "folder-root"

    async def fake_to_folder(file_ids, *, folder_id, source_parent_id="", **kw):
        to_folder_calls.append(1)
        return {"archive_folder_id": folder_id, "moved": len(file_ids)}

    async def fake_flat(file_ids, *, source_parent_id="", **kw):
        flat_calls.append((list(file_ids), source_parent_id))
        return {"archive_folder_id": "flat", "moved": len(file_ids)}

    strategy = PikPakStructuredArchiveStrategy(
        resolve_archive_folder=fake_resolve,
        archive_files_to_folder=fake_to_folder,
        archive_files=fake_flat,
    )
    items = [PublishItem(item_id="FC", name="c.mp4", size_bytes=3, parent_folder_id="P0")]
    ctx = PublishContext(
        task_id="t-arch2",
        target_chat_id=TARGET,
        dir_tree={},
        auto_cleanup_mode="archive",
    )

    summary = await strategy.cleanup(items, ctx)

    assert to_folder_calls == []
    assert flat_calls == [(["FC"], "P0")]
    assert "平铺" in summary


async def test_multi_part_caption_sequential_meta_naming(publisher_env):
    """多 Part 严格顺序: caption [i/n]、meta 沙箱命名 {task}_{item}_{seg}、file_path 仅展示。"""
    threshold = derive_split_threshold(False)
    item = _make_item(size_bytes=threshold * 2 + 345)
    res = await _publish(publisher_env, [item])
    assert res.published_count == 1

    assert len(publisher_env.engine_calls) == 3
    expected_lengths = [threshold, threshold, 345]
    for i, call in enumerate(publisher_env.engine_calls, start=1):
        assert call["file_path"] == "pikpak://F1"  # 仅展示, 不参与 IO
        assert call["meta_path"] == os.path.join(publisher_env.sandbox, f"t1_F1_{i}.upload.meta")
        assert call["force_fresh"] is False
        # DEF-01 护栏: 切片 Provider 的 total_bytes 必须精确匹配分卷长度, 绝非全文件大小
        sliced_prov = call["chunk_provider"]
        assert sliced_prov.total_bytes == expected_lengths[i - 1]
        assert sliced_prov.file_id == f"F1_part00{i}"
        assert sliced_prov.file_name == f"video.mp4.part00{i}"
    send_calls = publisher_env.client.send_file.await_args_list
    assert len(send_calls) == 3
    assert [c.kwargs["caption"] for c in send_calls] == ["[1/3] video.mp4", "[2/3] video.mp4", "[3/3] video.mp4"]
    assert all(c.kwargs["supports_streaming"] is False for c in send_calls)  # 多分卷不开启流式属性


async def test_single_part_plain_caption_and_streaming_flag(publisher_env):
    """单 Part caption 用原名; supports_streaming 对单卷视频生效，非视频不生效。"""
    # 视频 (含 duration_s)
    await _publish(publisher_env, [_make_item()])
    send_calls = publisher_env.client.send_file.await_args_list
    assert len(send_calls) == 1
    assert send_calls[0].kwargs["caption"] == "video.mp4"  # 单 Part 无前缀
    assert send_calls[0].kwargs["supports_streaming"] is True
    assert "attributes" in send_calls[0].kwargs

    # 视频 (转存文件 duration_s 为 None, 依赖扩展名识别为视频流媒体)
    publisher_env.client.send_file.reset_mock()
    await _publish(publisher_env, [_make_item(file_id="F2", duration_s=None)])
    send_calls = publisher_env.client.send_file.await_args_list
    assert send_calls[0].kwargs["supports_streaming"] is True
    assert "attributes" in send_calls[0].kwargs

    # 非视频文件 (如压缩包)
    publisher_env.client.send_file.reset_mock()
    await _publish(publisher_env, [_make_item(file_id="F3", name="archive.zip", duration_s=None)])
    send_calls = publisher_env.client.send_file.await_args_list
    assert send_calls[0].kwargs["supports_streaming"] is False
    assert "attributes" not in send_calls[0].kwargs


async def test_file_parts_invalid_retries_force_fresh(publisher_env, monkeypatch):
    """FILE_PARTS_INVALID → 清断点全量重传该 Part 一次 (force_fresh=True); 非 FILE_PARTS_INVALID 不重试。"""
    captured = []
    calls = {"n": 0}

    async def flaky_upload(client_, file_path, **kwargs):
        calls["n"] += 1
        captured.append(kwargs)
        if calls["n"] == 1:
            raise Exception("TEL：FILE_PARTS_INVALID (part 3 missing)")
        return "media-ok"

    # 冻结引擎 patch 唯一来源 = fast_telethon 模块属性 (引擎函数级 import 调用时解析)
    monkeypatch.setattr("app.core.fast_telethon.upload_file_parallel", flaky_upload)
    res = await _publish(publisher_env, [_make_item()])
    assert res.published_count == 1
    assert calls["n"] == 2
    assert captured[0]["force_fresh"] is False
    assert captured[1]["force_fresh"] is True

    # 非 FILE_PARTS_INVALID 异常 → 不触发第二次
    calls["n"] = 0

    async def boom_upload(client_, file_path, **kwargs):
        calls["n"] += 1
        raise ValueError("boom")

    monkeypatch.setattr("app.core.fast_telethon.upload_file_parallel", boom_upload)
    with pytest.raises(ValueError):
        await _publish(publisher_env, [_make_item(file_id="F3")])
    assert calls["n"] == 1


async def test_cancel_midway_no_record(publisher_env):
    """取消: 第 2 Part 边界中断 → UploadCancelledError 上抛、无落库、后续 Part 不上传。"""
    threshold = derive_split_threshold(False)

    def is_cancelled():
        return len(publisher_env.engine_calls) >= 1

    with pytest.raises(UploadCancelledError):
        await _publish(publisher_env, [_make_item(size_bytes=threshold * 2 + 10)], is_cancelled=is_cancelled)
    assert len(publisher_env.engine_calls) == 1
    assert publisher_env.client.send_file.await_count == 1
    conn = get_db()
    rows = conn.execute("SELECT COUNT(*) AS c FROM publish_history WHERE remote_file_id='F1'").fetchone()
    assert rows["c"] == 0


async def test_pause_midway_no_record(publisher_env):
    """暂停: 引擎首次调用后翻转 → UploadPausedError 上抛、无落库。"""
    threshold = derive_split_threshold(False)

    def is_paused():
        return len(publisher_env.engine_calls) >= 1

    with pytest.raises(UploadPausedError):
        await _publish(publisher_env, [_make_item(size_bytes=threshold * 2 + 10)], is_paused=is_paused)
    assert len(publisher_env.engine_calls) == 1
    conn = get_db()
    rows = conn.execute("SELECT COUNT(*) AS c FROM publish_history WHERE remote_file_id='F1'").fetchone()
    assert rows["c"] == 0


async def test_record_and_progress_contract(publisher_env, monkeypatch):
    """落库断言 (source_type/remote_file_id/tg_message_id) + 进度契约 (items 主轴/force_db/双向流速扩展字段)。"""
    events = []

    def cb(
        current,
        total,
        speed,
        eta,
        file_name,
        concurrency=0,
        dc_id=0,
        concurrency_total=0,
        percent=None,
        force_db=False,
        pikpak_download_speed_mb_s=0.0,
        total_bytes_done=0,
        total_bytes_all=0,
    ):
        events.append(
            {
                "current": current,
                "total": total,
                "force_db": force_db,
                "dl": pikpak_download_speed_mb_s,
                "done": total_bytes_done,
                "all": total_bytes_all,
            }
        )

    # 专属引擎替身: 模拟引擎行为 —— 读块后回调 progress 钩子 (镜像真实引擎对 progress_callback 的调用)
    size = 5 * _MB

    async def engine_with_hook(client_, file_path, **kwargs):
        publisher_env.engine_calls.append({"file_path": file_path, **kwargs})
        provider = kwargs.get("chunk_provider")
        hook = kwargs.get("progress_callback")
        if provider is not None and provider.total_bytes > 0:
            await provider.read_chunk(0, min(64, provider.total_bytes))
        if hook is not None:
            hook(64, size, 1.5, 10.0, 2, 4, 2)
        return "media-hooked"

    monkeypatch.setattr("app.core.fast_telethon.upload_file_parallel", engine_with_hook)

    res = await _publish(publisher_env, [_make_item(size_bytes=size)], progress_callback=cb)
    assert res.published_count == 1

    conn = get_db()
    row = conn.execute(
        "SELECT file_path, file_size, source_type, remote_file_id, tg_message_id "
        "FROM publish_history WHERE remote_file_id='F1'"
    ).fetchone()
    assert row["source_type"] == "pikpak"
    assert row["remote_file_id"] == "F1"
    assert row["tg_message_id"] == 4321
    assert row["file_size"] == size
    assert row["file_path"] == "pikpak://P0/video.mp4"

    assert events, "进度回调未被调用"
    assert events[-1]["force_db"] is True  # 每文件完成 force_db 落库
    assert events[-1]["current"] == 1 and events[-1]["total"] == 1
    assert events[-1]["dl"] > 0  # PikPak 下行速率被测量包装统计
    assert events[-1]["done"] == size and events[-1]["all"] == size
    assert any(e["force_db"] is False for e in events[:-1])  # 分卷内为高频事件

    # 旧式回调 (无扩展 kwarg) → 自动省略不报错
    old_events = []

    def old_cb(
        current, total, speed, eta, file_name, concurrency=0, dc_id=0, concurrency_total=0, percent=None, force_db=False
    ):
        old_events.append(current)

    res = await _publish(publisher_env, [_make_item(file_id="F9")], progress_callback=old_cb)
    assert res.published_count == 1
    assert old_events


async def test_preflight_no_write_permission_fails_fast(publisher_env, monkeypatch):
    """预检熔断: 无写权限 → Fatal('permission') 快速失败, 引擎零调用。"""

    async def deny(*args, **kwargs):
        return {"is_accessible": True, "has_write_permission": False, "permission_summary": "非频道管理员，无发帖权限"}

    monkeypatch.setattr("app.services.channel_inspector.ChannelInspector.inspect_channel", deny)
    with pytest.raises(PikPakPublishFatalError) as ei:
        await _publish(publisher_env, [_make_item()])
    assert ei.value.category == "permission"
    assert publisher_env.engine_calls == []


# ---------------------------------------------------------------------------
# 处置行为 / auto_cleanup / 封面 (包C)
# ---------------------------------------------------------------------------


class _ScriptedProvider(BaseChunkProvider):
    """脚本化 Provider 替身: 首个 read_chunk 先索链 (镜像真实 Provider 懒加载语义) 再按序数决定抛错;
    序数 < fail_first_n 的实例恒抛 exc_factory, 其余正常返回内存字节。"""

    created = []
    fail_first_n = 0
    exc_factory = None

    def __init__(self, file_id, file_name, total_bytes, url_provider, request_timeout=60.0, **kwargs):
        self.file_id = file_id
        self.file_name = file_name
        self.total_bytes = total_bytes
        self._url_provider = url_provider
        self.read_calls = 0
        self.closed = False
        self._initialized = False
        self._broken = len(_ScriptedProvider.created) < _ScriptedProvider.fail_first_n
        _ScriptedProvider.created.append(self)

    async def read_chunk(self, offset, size):
        self.read_calls += 1
        if not self._initialized:
            self._initialized = True
            await self._url_provider()  # JIT 懒加载首链
            if self._broken:
                raise _ScriptedProvider.exc_factory()
        if offset >= self.total_bytes or size <= 0:
            raise ValueError("out of range")
        return b"\xcd" * (min(offset + size, self.total_bytes) - offset)

    async def aclose(self):
        self.closed = True


async def test_transient_wraps_retryable(publisher_env, monkeypatch):
    """引擎 L1/L2 尽力后仍瞬断 → Retryable('transient') 任务级上抛 (服务级不重试)。"""
    from app.core.scheduler import RetryableTaskError

    _ScriptedProvider.created = []
    _ScriptedProvider.fail_first_n = 999
    _ScriptedProvider.exc_factory = ChunkFetchTransientError
    monkeypatch.setattr("app.core.stream_chunk_provider.PikPakStreamChunkProvider", _ScriptedProvider)

    with pytest.raises(PikPakPublishRetryableError) as ei:
        await _publish(publisher_env, [_make_item()])
    assert ei.value.category == "transient"
    assert str(ei.value) == FAILURE_DETAIL_TEXT["transient"]
    assert isinstance(ei.value, RetryableTaskError)


async def test_link_expired_relink_success(publisher_env, monkeypatch):
    """直链过期 → 重新索链重建 Provider (旧 Provider 已销毁) → 引擎断点续传完成发布。"""
    _ScriptedProvider.created = []
    _ScriptedProvider.fail_first_n = 1  # 仅首个实例 (首次尝试) 恒抛
    _ScriptedProvider.exc_factory = PikPakLinkExpiredError
    monkeypatch.setattr("app.core.stream_chunk_provider.PikPakStreamChunkProvider", _ScriptedProvider)

    threshold = derive_split_threshold(False)
    item = _make_item(size_bytes=threshold * 2 + 10)
    res = await _publish(publisher_env, [item])
    assert res.published_count == 1
    assert len(publisher_env.link_calls) == 2  # 首链 + 换链 (懒加载语义, 每个 Provider 实例一次)
    assert _ScriptedProvider.created[0].closed is True  # 旧 Provider 已销毁


async def test_link_expired_bounded_exhaustion(publisher_env, monkeypatch):
    """恒过期 → 有界 3 次重新索链后 Retryable('link_expired'), 无无限循环。"""
    _ScriptedProvider.created = []
    _ScriptedProvider.fail_first_n = 999
    _ScriptedProvider.exc_factory = PikPakLinkExpiredError
    monkeypatch.setattr("app.core.stream_chunk_provider.PikPakStreamChunkProvider", _ScriptedProvider)

    with pytest.raises(PikPakPublishRetryableError) as ei:
        await _publish(publisher_env, [_make_item()])
    assert ei.value.category == "link_expired"
    assert len(publisher_env.link_calls) == PikPakPublisherService._LINK_EXPIRED_RELINK_ATTEMPTS


async def test_auth_error_wraps_fatal(publisher_env, monkeypatch):
    """凭据恢复单飞后仍失败 → Fatal('auth') 人工介入, 不自动重试。"""
    from app.core.scheduler import FatalTaskError

    async def auth_fail(file_ids, **kwargs):
        raise PikPakAuthError("TOKEN_EXPIRED")

    monkeypatch.setattr(pub, "get_download_links", auth_fail)
    # 替换为懒加载索链的脚本化替身, 使 JIT 索链在首块读取时被触发 (镜像真实 Provider 语义)
    _ScriptedProvider.created = []
    _ScriptedProvider.fail_first_n = 0
    monkeypatch.setattr("app.core.stream_chunk_provider.PikPakStreamChunkProvider", _ScriptedProvider)

    with pytest.raises(PikPakPublishFatalError) as ei:
        await _publish(publisher_env, [_make_item()])
    assert ei.value.category == "auth"
    assert "人工" in str(ei.value)
    assert isinstance(ei.value, FatalTaskError)


@pytest.mark.parametrize(
    "exc_cls,expected_category",
    [
        (PikPakQuotaError, "quota"),
        (PikPakNotFoundError, "not_found"),
    ],
)
async def test_quota_not_found_map_fatal(publisher_env, monkeypatch, exc_cls, expected_category):
    """Quota/NotFound → Fatal 且分类正确 (NotFound 由主循环逐文件隔离, 不中止整任务)。"""
    from app.core.scheduler import FatalTaskError

    async def fail(file_ids, **kwargs):
        if "F1" in file_ids:
            raise exc_cls("canned")
        return [f"https://cdn.fake.test/dl/{fid}?X-Sign=FAKESIGN" for fid in file_ids]

    monkeypatch.setattr(pub, "get_download_links", fail)
    _ScriptedProvider.created = []
    _ScriptedProvider.fail_first_n = 0
    monkeypatch.setattr("app.core.stream_chunk_provider.PikPakStreamChunkProvider", _ScriptedProvider)

    if expected_category == "not_found":
        # 逐文件隔离: 计入 failed_files 并继续, 第二个文件正常发布
        res = await _publish(publisher_env, [_make_item(), _make_item(file_id="F2")])
        assert res.published_count == 1
        assert len(res.failed_files) == 1
        assert "不存在" in res.failed_files[0]
    else:
        with pytest.raises(PikPakPublishFatalError) as ei:
            await _publish(publisher_env, [_make_item()])
        assert ei.value.category == "quota"
        assert isinstance(ei.value, FatalTaskError)


async def test_auto_cleanup_modes_and_ordering(publisher_env, monkeypatch):
    """默认 archive / 显式 delete / record 先于 cleanup / 失败与防重文件不清理。
    包D: archive 改为循环后结构化归档 (无 dir_tree → 平铺回退, source_parent_id 用真实父目录)。"""
    journal = []
    real_record = PublishHistoryRepository.record

    def spy_record(*a, **k):
        journal.append(("record", k.get("remote_file_id")))
        real_record(*a, **k)

    # PublishHistoryRepository 在服务内为类引用 (懒导入自 app.db.publish_repository), 直接 patch 类方法
    monkeypatch.setattr(PublishHistoryRepository, "record", spy_record)

    async def fake_archive(file_ids, *, source_parent_id="", **kw):
        journal.append(("archive", tuple(file_ids), source_parent_id))
        return {"archive_folder_id": "ARC", "moved": len(file_ids)}

    async def fake_delete(file_ids, *, parent_id="", **kw):
        journal.append(("delete", tuple(file_ids), parent_id))

    async def fake_resolve(parent_id="", name="已发布归档", **kw):
        journal.append(("resolve", parent_id, name))
        return "ARC-root"

    # cleanup 策略由门面装配注入 pikpak_service 模块属性 (装配时读取) → patch 源头即生效
    monkeypatch.setattr("app.services.pikpak_service.archive_files", fake_archive)
    monkeypatch.setattr("app.services.pikpak_service.resolve_archive_folder", fake_resolve)
    monkeypatch.setattr("app.services.pikpak_service.delete_files", fake_delete)

    # 1) 默认 archive 模式: 无 dir_tree → 平铺回退 (record 先于 cleanup, 真实父目录失效 LRU)
    res = await _publish(publisher_env, [_make_item()])
    assert res.published_count == 1
    assert ("archive", ("F1",), "P0") in journal
    assert all(j[0] != "delete" for j in journal)
    order = [j[0] for j in journal]
    assert order.index("record") < order.index("archive")

    # 2) 显式 delete 模式
    journal.clear()
    res = await _publish(publisher_env, [_make_item(file_id="F2")], auto_cleanup_mode="delete")
    assert res.published_count == 1
    assert ("delete", ("F2",), "P0") in journal
    assert all(j[0] != "archive" for j in journal)

    # 3) audit 拦截文件 → 不 record、不清理
    journal.clear()
    res = await _publish(
        publisher_env, [_make_item(file_id="F3", audit={"status": "BAD"})], auto_cleanup_mode="archive"
    )
    assert res.published_count == 0
    assert journal == []

    # 4) 防重命中文件 → 不清理 (已发布不再搬运)
    PublishHistoryRepository.record(TARGET, "pikpak://P0/x.mp4", 5 * _MB, "", source_type="pikpak", remote_file_id="F4")
    journal.clear()
    res = await _publish(publisher_env, [_make_item(file_id="F4")], auto_cleanup_mode="delete")
    assert res.published_count == 1
    assert journal == []


async def test_thumbnail_injection_and_content_type_reject(publisher_env, cover_jpeg):
    """有效封面首卷注入，缺链和非图片响应使用静态海报，后续卷无封面。"""
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    from app.core.thumbnail_image import load_normalized_placeholder_poster

    payload = cover_jpeg
    served = {"ct": "image/jpeg"}

    async def thumb_handler(request):
        return web.Response(body=payload, content_type=served["ct"])

    app = web.Application()
    app.router.add_get("/thumb", thumb_handler)
    server = TestServer(app)
    await server.start_server()
    try:
        thumb_url = f"http://127.0.0.1:{server.port}/thumb"
        threshold = derive_split_threshold(False)

        # 1) 有效 JPEG 首卷注入，后续卷无封面
        item = _make_item(size_bytes=threshold * 2 + 10, thumbnail_link=thumb_url)
        res = await _publish(publisher_env, [item])
        assert res.published_count == 1
        send_calls = publisher_env.client.send_file.await_args_list
        assert len(send_calls) == 3
        # DEF-03 护栏: 封面必须包装为具备合法 .name="thumbnail.jpg" 的 io.BytesIO 流
        thumb_obj = send_calls[0].kwargs.get("thumb")
        assert thumb_obj is not None
        assert hasattr(thumb_obj, "name") and thumb_obj.name == "thumbnail.jpg"
        assert hasattr(thumb_obj, "getvalue") and thumb_obj.getvalue() == payload
        assert "thumb" not in send_calls[1].kwargs
        assert "thumb" not in send_calls[2].kwargs

        # 2) text/html 拒绝作为封面，回落到静态海报
        publisher_env.client.send_file.reset_mock()
        publisher_env.engine_calls.clear()
        served["ct"] = "text/html"
        res = await _publish(publisher_env, [_make_item(file_id="F2", thumbnail_link=thumb_url)])
        assert res.published_count == 1
        thumb = publisher_env.client.send_file.await_args.kwargs["thumb"]
        assert thumb.getvalue() == load_normalized_placeholder_poster()

        # 3) thumbnail_link 缺失、补查和抽帧均无结果，回落到静态海报
        publisher_env.client.send_file.reset_mock()
        publisher_env.engine_calls.clear()
        res = await _publish(publisher_env, [_make_item(file_id="F3")])
        assert res.published_count == 1
        thumb = publisher_env.client.send_file.await_args.kwargs["thumb"]
        assert thumb.getvalue() == load_normalized_placeholder_poster()
    finally:
        await server.close()


async def test_concurrency_defaults_and_override(publisher_env):
    """并发默认消费稳健档 2×1 (config L150/L151), 显式参数覆写生效。"""
    await _publish(publisher_env, [_make_item()])
    last = publisher_env.engine_calls[-1]
    assert last["concurrency"] == settings.PIKPAK_UPLOAD_CONCURRENCY == 2
    assert last["in_flight_per_connection"] == settings.PIKPAK_UPLOAD_IN_FLIGHT == 1

    publisher_env.engine_calls.clear()
    await _publish(publisher_env, [_make_item(file_id="F2")], upload_concurrency=4, upload_in_flight_per_connection=2)
    last = publisher_env.engine_calls[-1]
    assert last["concurrency"] == 4
    assert last["in_flight_per_connection"] == 2


async def test_dynamic_concurrency_probe_updated(publisher_env, monkeypatch):
    """DEF-06: 动态并发调谐探针实时读取 DB task_queue params/concurrency_total。"""
    from app.db.task_repository import TaskQueueRepository

    TaskQueueRepository.create(
        "probe-task-1",
        "pikpak_publish",
        {"upload_concurrency": 6, "upload_in_flight_per_connection": 3, "upload_in_flight": 3},
    )

    concurrency_probed = []
    in_flight_probed = []

    async def probe_upload(client_, file_path, **kwargs):
        publisher_env.engine_calls.append({"file_path": file_path, **kwargs})
        get_c = kwargs.get("get_concurrency")
        get_inf = kwargs.get("get_in_flight_per_connection")
        if get_c:
            concurrency_probed.append(get_c())
        if get_inf:
            in_flight_probed.append(get_inf())
        return "media-probed"

    monkeypatch.setattr("app.core.fast_telethon.upload_file_parallel", probe_upload)
    await _publish(publisher_env, [_make_item(file_id="probe-F1")], task_id="probe-task-1")

    assert concurrency_probed == [6]
    assert in_flight_probed == [3]


# ---------------------------------------------------------------------------
# V2 增强与加固测试 (expand_remote_files, none 清理, 超限策略, ffmpeg 抽帧)
# ---------------------------------------------------------------------------


async def test_expand_remote_files_recursion_and_guards(monkeypatch):
    """递归展开目录树: 扁平化多级文件、环路跳过、深度上限截断。"""

    def _item(fid, name, is_dir, sz=0):
        return ParsedFileItem(
            file_id=fid,
            name=name,
            is_dir=is_dir,
            size_bytes=sz,
            audit={},
            thumbnail_link=None,
            duration_s=None,
            width=None,
            height=None,
            parent_id="",
        )

    canned_tree = {
        "D1": [
            _item("F1", "f1.mp4", False, 100),
            _item("D2", "sub", True, 0),
        ],
        "D2": [
            _item("F2", "f2.mp4", False, 200),
            _item("D1", "cycle_to_d1", True, 0),
        ],
    }

    async def fake_list_files_raw(folder_id="", page_token=None):
        return canned_tree.get(folder_id, []), None

    monkeypatch.setattr("app.services.pikpak_service.list_files_raw", fake_list_files_raw)

    root_items = [
        _item("D1", "root_dir", True, 0),
        _item("F0", "root_file.mp4", False, 50),
    ]

    expanded, dir_tree = await PikPakPublisherService.expand_remote_files(root_items)
    file_ids = [item.file_id for item in expanded]
    assert set(file_ids) == {"F0", "F1", "F2"}
    assert all(not item.is_dir for item in expanded)
    # 包D: dir_tree 落地 (结构化归档素材; 循环引用目录 D1 不重复登记)
    assert dir_tree == {"D1": "root_dir", "D2": "root_dir/sub"}


async def test_auto_cleanup_none_and_delete():
    """清理策略: mode='none' 零 CLI 调用 (Noop); mode='delete' 调用 delete_files。
    活实现 = NoopCleanupStrategy / PikPakDeleteCleanupStrategy (构造注入), 原死代码 _run_auto_cleanup 已删。"""
    delete_spy = AsyncMock()
    item = PublishItem(item_id="F_clean", name="c.mp4", size_bytes=1, parent_folder_id="P0")
    ctx_none = PublishContext(task_id="task-clean", target_chat_id=TARGET, auto_cleanup_mode="none")
    ctx_delete = PublishContext(task_id="task-clean", target_chat_id=TARGET, auto_cleanup_mode="delete")

    # 1. none → Noop 策略, 零 CLI 调用
    assert await NoopCleanupStrategy().cleanup([item], ctx_none) is None

    # 2. delete → 调用 delete_files (回收站), parent_id 透传
    summary = await PikPakDeleteCleanupStrategy(delete_files=delete_spy).cleanup([item], ctx_delete)
    delete_spy.assert_awaited_once_with(["F_clean"], parent_id="P0")
    assert summary == "🗑️ 已将 1 个文件移入回收站"


async def test_oversize_policy_skip_and_report(publisher_env):
    """oversize_policy='skip_and_report': 超限跳过, 记录 skipped_files; placeholder 开启时发消息但不入库。"""
    item = _make_item(file_id="BIG1", name="big_video.mp4", size_bytes=2100 * _MB)
    normal_item = _make_item(file_id="NORM1", name="normal.mp4", size_bytes=10 * _MB)

    res = await _publish(
        publisher_env,
        [item, normal_item],
        oversize_policy="skip_and_report",
        send_placeholder_card=True,
    )

    assert res.published_count == 1
    assert len(res.skipped_files) == 1
    sf = res.skipped_files[0]
    assert sf.file_name == "big_video.mp4"
    assert sf.file_size_mb == 2100.0
    assert sf.exceeded_mb == 100.0

    send_msg_calls = publisher_env.client.send_message.await_args_list
    assert len(send_msg_calls) == 1
    assert "big_video.mp4" in send_msg_calls[0].args[1]

    assert (
        PublishHistoryRepository.is_published(
            TARGET,
            f"pikpak://{item.parent_id}/{item.name}",
            item.size_bytes,
            source_type="pikpak",
            remote_file_id=item.file_id,
        )
        is False
    )


async def test_oversize_policy_strict_fail(publisher_env):
    """oversize_policy='strict_fail': 发现超限文件立即终止任务。"""
    item = _make_item(file_id="BIG2", name="huge.mp4", size_bytes=2100 * _MB)

    with pytest.raises(PikPakPublishFatalError) as ei:
        await _publish(
            publisher_env,
            [item],
            oversize_policy="strict_fail",
        )
    assert ei.value.category == "quota"
    assert "超过单文件体积限制" in str(ei.value)


async def test_oversize_threshold_boundary_and_progress_callback(publisher_env, monkeypatch):
    """验证 1950~2000 MiB (1.95 GiB) 临界文件对齐 derive_split_threshold 判定, 且触发平滑进度回报与流控保护。"""
    threshold = derive_split_threshold(False)
    boundary_item = _make_item(file_id="BOUND1", name="boundary_1960m.mp4", size_bytes=threshold + 10 * _MB)

    progress_events = []

    def on_progress(published, total, speed, eta, file_name, **kwargs):
        progress_events.append((published, total, file_name))

    sleep_calls = []
    real_sleep = asyncio.sleep

    async def fake_sleep(secs):
        sleep_calls.append(secs)
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    res = await _publish(
        publisher_env,
        [boundary_item],
        oversize_policy="skip_and_report",
        send_placeholder_card=True,
        send_delay_seconds=3.0,
        progress_callback=on_progress,
    )

    # 1. 临界文件正确命中超限并被跳过
    assert res.published_count == 0
    assert len(res.skipped_files) == 1
    assert res.skipped_files[0].file_name == "boundary_1960m.mp4"

    # 2. 发送占位卡前执行流控保护: 信号感知节流 (card_throttle_wait → wait_with_signals
    #    0.25s 粒度轮询, 暂停期间可即时让位/排水), 裸 asyncio.sleep(1.5) 已被替换
    assert 1.5 not in sleep_calls, "裸节流已替换为信号感知等待 (runtime.card_throttle_wait)"
    assert 0.25 in sleep_calls and sum(s for s in sleep_calls if s <= 0.25) >= 1.5

    # 3. 立即触发一次 progress_callback 汇报 ⏭️ [跳过超限]
    assert any(ev[2] == "⏭️ [跳过超限] boundary_1960m.mp4" for ev in progress_events)

    # 4. 占位消息坚决不入库
    assert (
        PublishHistoryRepository.is_published(
            TARGET,
            f"pikpak://{boundary_item.parent_id}/{boundary_item.name}",
            boundary_item.size_bytes,
            source_type="pikpak",
            remote_file_id=boundary_item.file_id,
        )
        is False
    )


async def test_extract_frame_ffmpeg_mock(monkeypatch, cover_jpeg):
    """FFmpeg HTTP Range 抽帧单元测试: 正常抽帧 / 超时优雅降级 / 非法直链。
    实现迁 core/pikpak/thumbnail.extract_frame_ffmpeg (原死代码类方法已删)。"""
    proc_mock = AsyncMock()
    proc_mock.returncode = 0
    proc_mock.communicate = AsyncMock(return_value=(cover_jpeg, b""))

    async def fake_create_proc(*args, **kwargs):
        return proc_mock

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_proc)

    jpeg_data = await thumbnail_extract_frame_ffmpeg("https://cdn.test/video.mp4", "F1")
    assert jpeg_data == cover_jpeg

    proc_timeout_mock = AsyncMock()
    proc_timeout_mock.returncode = None
    proc_timeout_mock.communicate = AsyncMock(side_effect=[TimeoutError(), (b"", b"")])
    proc_timeout_mock.kill = MagicMock()
    proc_timeout_mock.wait = AsyncMock()

    async def fake_create_proc_timeout(*args, **kwargs):
        return proc_timeout_mock

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_proc_timeout)

    timeout_res = await thumbnail_extract_frame_ffmpeg("https://cdn.test/video.mp4", "F1", seek_sec=0)
    assert timeout_res is None
    proc_timeout_mock.kill.assert_called_once()

    invalid_res = await thumbnail_extract_frame_ffmpeg("ftp://invalid.test/v.mp4", "F1")
    assert invalid_res is None


async def test_oversize_policy_skip_and_report_default_placeholder(publisher_env):
    """oversize_policy='skip_and_report' 未显式指定 send_placeholder_card 时默认发送占位卡。"""
    item = _make_item(file_id="DEF_BIG", name="default_oversize.mp4", size_bytes=2200 * _MB)

    res = await _publish(
        publisher_env,
        [item],
        oversize_policy="skip_and_report",
        # 未传 send_placeholder_card 参数，验证其默认开启
    )

    assert res.published_count == 0
    assert len(res.skipped_files) == 1
    assert res.skipped_files[0].file_name == "default_oversize.mp4"

    # 验证 client.send_message 默认被调用发送缺集说明
    send_msg_calls = publisher_env.client.send_message.await_args_list
    assert len(send_msg_calls) == 1
    assert "default_oversize.mp4" in send_msg_calls[0].args[1]
    assert "【缺集说明】" in send_msg_calls[0].args[1]


async def test_publish_pikpak_files_attaches_folder_contract(publisher_env):
    """包2: publish_pikpak_files 结果对象携带 is_folder_publish 与 folder_name 属性。"""
    res = await _publish(
        publisher_env,
        [_make_item(file_id="F_FLD", name="folder_item.mp4")],
        is_folder_publish=True,
        folder_name="测试合集目录",
    )
    assert res.published_count == 1
    assert getattr(res, "is_folder_publish", False) is True
    assert getattr(res, "folder_name", "") == "测试合集目录"

    # 空列表退出分支同样保留契约属性
    empty_res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=[],
        target_chat_id="-100888",
        is_folder_publish=True,
        folder_name="空合集",
    )
    assert empty_res.published_count == 0
    assert getattr(empty_res, "is_folder_publish", False) is True
    assert getattr(empty_res, "folder_name", "") == "空合集"


def test_publisher_service_inspect_folder_ts_filtering(tmp_dir):
    """验证 PublisherService.inspect_folder 能够正确识别真实的 MPEG-TS 视频，并将 TypeScript 源码、.d.ts 等过滤并记录到 ignored_files。"""
    # 1. 真实 MPEG-TS (188 字节 stride * 5 包，含空字节)
    real_ts = (b"\x47\x00\x11\x10" + b"\x00" * 184) * 5
    safe_write_bytes(os.path.join(tmp_dir, "video.ts"), real_ts)

    # 2. 真实 M2TS (192 字节 stride * 5 包，前导 4 字节 timestamp + 0x47 + 187)
    real_m2ts = (b"\x00\x00\x00\x00\x47\x00\x11\x10" + b"\x00" * 184) * 5
    safe_write_bytes(os.path.join(tmp_dir, "clip.m2ts"), real_m2ts)

    # 3. TypeScript 源码文件 (UTF-8 文本，无 \x00)
    safe_write_bytes(os.path.join(tmp_dir, "index.ts"), b"import { ref } from 'vue';\nexport const x = 1;")

    # 4. TypeScript 声明文件
    safe_write_bytes(os.path.join(tmp_dir, "types.d.ts"), b"export interface User { id: string; }")

    res = PublisherService.inspect_folder(tmp_dir, None)

    # 两个有效视频
    assert res["videos"]["count"] == 2
    video_names = {f["file_name"] for f in res["media_files"]}
    assert "video.ts" in video_names
    assert "clip.m2ts" in video_names

    # 源码和声明被归类到 ignored_files
    ignored_names = {f["file_name"] for f in res["ignored_files"]}
    assert "index.ts" in ignored_names
    assert "types.d.ts" in ignored_names

    # 验证 ignored_files 里的原因信息
    ts_ignored = next(f for f in res["ignored_files"] if f["file_name"] == "index.ts")
    assert "MPEG-TS" in ts_ignored["reason"]


def test_inspect_folder_pending_count_excludes_published_oversized(tmp_path, monkeypatch):
    """pending = 未超限且未发布 —— 「已发布但超限」不得抵扣未发布有效文件"""
    safe_write_bytes(str(tmp_path / "a_valid.mkv"), b"V" * 30)  # 30B < 50B 门槛 → 有效未发布
    safe_write_bytes(str(tmp_path / "b_over.mkv"), b"V" * 100)  # 100B > 50B → 超限 (但已发布)

    # inspect_folder 内函数级导入真仓 → 打桩在源模块类属性
    monkeypatch.setattr(
        "app.db.publish_repository.PublishHistoryRepository.is_published",
        lambda chat, path, size, h: path.endswith("b_over.mkv"),
    )
    monkeypatch.setattr("app.db.download_repository.DownloadHistoryRepository.get_hash_by_path", lambda _path: None)

    data = PublisherService.inspect_folder(str(tmp_path), target_channel="-100999", effective_limit_bytes=50)
    assert data["valid_files_count"] == 1
    assert data["already_published_count"] == 1
    assert data["pending_publish_count"] == 1, "已发布但超限的文件不得抵扣未发布有效文件 (修复前 max(0,1-1)=0)"


def test_inspect_folder_limit_follows_account_snapshot(tmp_path, monkeypatch):
    """传入 account_premium 快照而未传 effective_limit_bytes 时, 门槛必须与快照同源"""
    safe_write_bytes(str(tmp_path / "v.mkv"), b"V" * 1024)

    # 全局引擎口径打桩为哨兵值: 修复前 (快照传入仍取全局门槛) 会误用哨兵
    monkeypatch.setattr(client_manager, "effective_max_file_size_bytes", lambda *a, **k: 123 * 1024 * 1024)

    data_premium = PublisherService.inspect_folder(str(tmp_path), account_premium=True)
    assert data_premium["account_premium"] is True
    assert data_premium["effective_limit_mb"] == round(settings.PREMIUM_MAX_FILE_SIZE_BYTES / (1024 * 1024), 2)

    data_non_premium = PublisherService.inspect_folder(str(tmp_path), account_premium=False)
    assert data_non_premium["account_premium"] is False
    assert data_non_premium["effective_limit_mb"] == round(settings.MAX_FILE_SIZE_BYTES / (1024 * 1024), 2)


def test_inspect_folder_skips_unreadable_file_instead_of_aborting(tmp_path, monkeypatch):
    safe_write_bytes(str(tmp_path / "ok1.mkv"), b"V" * 10)
    safe_write_bytes(str(tmp_path / "gone.mkv"), b"V" * 10)
    safe_write_bytes(str(tmp_path / "ok2.mkv"), b"V" * 10)

    real_getsize = os.path.getsize

    def flaky_getsize(path, *args, **kwargs):
        if str(path).endswith("gone.mkv"):
            raise OSError("file disappeared between walk and inspect")
        return real_getsize(path, *args, **kwargs)

    monkeypatch.setattr("app.services.publisher_service.os.path.getsize", flaky_getsize)

    data = PublisherService.inspect_folder(str(tmp_path), None)

    media_names = {f["file_name"] for f in data["media_files"]}
    assert media_names == {"ok1.mkv", "ok2.mkv"}, "不可读文件不得中止整目录巡检"
    assert data["unavailable_files_count"] == 1
    assert data["unavailable_files"][0]["file_name"] == "gone.mkv"


def test_inspect_and_publish_prune_sensitive_descendants(tmp_path):
    from app.core.pipeline.utils import scan_folder_grouped

    private = tmp_path / "nested" / ".ssh"
    private.mkdir(parents=True)
    (private / "key.txt").write_bytes(b"private")
    (private / "private.mp4").write_bytes(b"private")
    (tmp_path / "ordinary.mp4").write_bytes(b"media")
    assert PublisherService.inspect_folder(str(tmp_path))["total_scanned_files"] == 1
    assert scan_folder_grouped(str(tmp_path)) == {str(tmp_path): [str(tmp_path / "ordinary.mp4")]}
