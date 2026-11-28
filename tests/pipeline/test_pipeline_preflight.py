"""Pipeline Pre-flight: Telegram Premium 会员上传并发自适应感知测试 (Requirement G)"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.pipeline.models import PublishContext
from app.core.pipeline.preflight import phase1_preflight
from app.db.account_repository import SystemSettingsRepository


def _make_mock_account(*, is_premium: bool, account_id: int = 101):
    mock_client = AsyncMock()
    mock_acc = MagicMock()
    mock_acc.account_id = account_id
    mock_acc.account_name = "PremAcc" if is_premium else "StdAcc"
    mock_acc.client = mock_client
    mock_acc.is_ready.return_value = True
    mock_acc.is_premium = is_premium
    mock_acc.probe_premium = AsyncMock(return_value=is_premium)
    mock_acc.effective_max_file_size_bytes.return_value = (4000 if is_premium else 2000) * 1024 * 1024
    return mock_acc


@pytest.mark.asyncio
async def test_preflight_non_premium_default_upload_concurrency():
    """非 Premium 会员且未显式指定并发时：保持系统默认基线 (2 通道，单连接 2 在途 = 共 4 在途分块)"""
    SystemSettingsRepository.set("upload_parallel_connections", "2")
    SystemSettingsRepository.set("upload_in_flight_per_connection", "2")
    mock_acc = _make_mock_account(is_premium=False)

    # 场景 1: upload_concurrency=None (门面默认透传)
    ctx_none = PublishContext(
        task_id="preflight_std_none",
        target_chat_id="-1009000000010",
        upload_concurrency=None,
        upload_in_flight_per_connection=None,
    )
    with patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc):
        res = await phase1_preflight(ctx_none, is_pikpak=False, channel_probe=None)
    assert res is None
    assert ctx_none.is_premium is False
    assert ctx_none.upload_concurrency == 2
    assert ctx_none.upload_in_flight_per_connection == 2
    assert ctx_none.upload_concurrency * ctx_none.upload_in_flight_per_connection == 4

    # 场景 2: 裸构造 PublishContext (使用默认哨兵)
    ctx_default = PublishContext(
        task_id="preflight_std_default",
        target_chat_id="-1009000000010",
    )
    with patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc):
        await phase1_preflight(ctx_default, is_pikpak=False, channel_probe=None)
    assert ctx_default.upload_concurrency == 2
    assert ctx_default.upload_in_flight_per_connection == 2


@pytest.mark.asyncio
async def test_preflight_premium_adaptive_upload_concurrency():
    """Premium 会员且未显式指定并发时：自适应提升至 4 通道 (单连接 2 在途 = 共 8 在途分块)，释放 4000 MiB (3.91 GiB) 潜力"""
    SystemSettingsRepository.set("upload_parallel_connections", "2")
    SystemSettingsRepository.set("upload_in_flight_per_connection", "2")
    mock_acc = _make_mock_account(is_premium=True)

    # 场景 1: upload_concurrency=None
    ctx_none = PublishContext(
        task_id="preflight_prem_none",
        target_chat_id="-1009000000010",
        upload_concurrency=None,
        upload_in_flight_per_connection=None,
    )
    with patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc):
        res = await phase1_preflight(ctx_none, is_pikpak=False, channel_probe=None)
    assert res is None
    assert ctx_none.is_premium is True
    assert ctx_none.upload_concurrency == 4
    assert ctx_none.upload_in_flight_per_connection == 2
    assert ctx_none.upload_concurrency * ctx_none.upload_in_flight_per_connection == 8
    assert ctx_none.task_limit_bytes == 4000 * 1024 * 1024

    # 场景 2: 裸构造 PublishContext (使用默认哨兵)
    ctx_default = PublishContext(
        task_id="preflight_prem_default",
        target_chat_id="-1009000000010",
    )
    with patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc):
        await phase1_preflight(ctx_default, is_pikpak=False, channel_probe=None)
    assert ctx_default.is_premium is True
    assert ctx_default.upload_concurrency == 4
    assert ctx_default.upload_in_flight_per_connection == 2


@pytest.mark.asyncio
async def test_preflight_explicit_upload_concurrency_and_clamping():
    """显式指定 upload_concurrency 时优先于自适应默认，并严格受 [1, 16] 安全钳制"""
    SystemSettingsRepository.set("upload_parallel_connections", "2")
    mock_prem = _make_mock_account(is_premium=True)

    # 1. Premium 账号但显式指定 upload_concurrency=2 -> 尊重显式值 2
    ctx_explicit_2 = PublishContext(
        task_id="preflight_exp_2",
        target_chat_id="-1009000000010",
        upload_concurrency=2,
    )
    with patch("app.core.client_manager.client_manager.get_account", return_value=mock_prem):
        await phase1_preflight(ctx_explicit_2, is_pikpak=False, channel_probe=None)
    assert ctx_explicit_2.upload_concurrency == 2

    # 2. 显式指定超上限 32 -> 钳制为 16
    ctx_clamp_high = PublishContext(
        task_id="preflight_clamp_high",
        target_chat_id="-1009000000010",
        upload_concurrency=32,
    )
    with patch("app.core.client_manager.client_manager.get_account", return_value=mock_prem):
        await phase1_preflight(ctx_clamp_high, is_pikpak=False, channel_probe=None)
    assert ctx_clamp_high.upload_concurrency == 16

    # 3. 显式指定欠下限 0 -> 钳制为 1
    ctx_clamp_low = PublishContext(
        task_id="preflight_clamp_low",
        target_chat_id="-1009000000010",
        upload_concurrency=0,
    )
    with patch("app.core.client_manager.client_manager.get_account", return_value=mock_prem):
        await phase1_preflight(ctx_clamp_low, is_pikpak=False, channel_probe=None)
    assert ctx_clamp_low.upload_concurrency == 1


@pytest.mark.asyncio
async def test_pikpak_preflight_rejects_unready_default_account_and_clamps_in_flight() -> None:
    """PikPak 默认账号未就绪须拦截；DB pikpak_upload_in_flight 须钳制在 [1, 4]."""
    from app.core.pikpak.taxonomy import PikPakPublishFatalError

    unready_acc = MagicMock()
    unready_acc.account_id = 1
    unready_acc.client = AsyncMock()
    unready_acc.is_ready.return_value = False

    ctx_unready = PublishContext(task_id="t_pf_unr", target_chat_id=-10095)
    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=unready_acc),
        patch("app.core.client_manager.client_manager.refresh_premium", new_callable=AsyncMock),
        patch("app.core.client_manager.client_manager.userbot", unready_acc.client),
        pytest.raises(PikPakPublishFatalError),
    ):
        await phase1_preflight(ctx_unready, is_pikpak=True, channel_probe=None)

    ready_acc = MagicMock()
    ready_acc.account_id = 1
    ready_acc.account_name = "Main"
    ready_acc.client = AsyncMock()
    ready_acc.is_ready.return_value = True
    ready_acc.is_premium = False
    ready_acc.probe_premium = AsyncMock(return_value=False)
    ready_acc.effective_max_file_size_bytes.return_value = 2000 * 1024 * 1024

    SystemSettingsRepository.set("pikpak_upload_in_flight", "99")
    ctx_clamp = PublishContext(task_id="t_pf_clmp", target_chat_id=-10095)
    with patch("app.core.client_manager.client_manager.get_account", return_value=ready_acc):
        await phase1_preflight(ctx_clamp, is_pikpak=True, channel_probe=None)
    assert ctx_clamp.upload_in_flight_per_connection == 4


@pytest.mark.asyncio
async def test_preflight_explicit_unavailable_account_fails_instead_of_fallback():
    """显式指定不可用 account_id 必须直接失败, 禁止静默回退默认主账号."""
    from app.core.pikpak.taxonomy import PikPakPublishFatalError
    from app.core.pipeline.models import PublishResult

    ctx_pk = PublishContext(task_id="t_pf_pk", target_chat_id=-1001, account_id=42)
    with (
        patch("app.core.client_manager.client_manager.get_account", return_value=None),
        pytest.raises(PikPakPublishFatalError, match="ID=42"),
    ):
        await phase1_preflight(ctx_pk, is_pikpak=True, channel_probe=None)

    ctx_local = PublishContext(task_id="t_pf_loc", target_chat_id=-1001, account_id=42, total_files_count=3)
    with patch("app.core.client_manager.client_manager.get_account", return_value=None):
        res = await phase1_preflight(ctx_local, is_pikpak=False, channel_probe=None)
    assert isinstance(res, PublishResult)
    assert res.published_count == 0
    assert "ID=42" in res.failed_files[0]


@pytest.mark.asyncio
async def test_preflight_reads_db_in_flight_for_default_context():
    """未显式传 upload_in_flight_per_connection 时应读取 DB 设置."""
    SystemSettingsRepository.set("pikpak_upload_in_flight", "4")
    mock_acc = _make_mock_account(is_premium=False, account_id=1)

    ctx = PublishContext(task_id="t_inf", target_chat_id=-1001)
    assert ctx.upload_in_flight_per_connection == 2
    with patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc):
        await phase1_preflight(ctx, is_pikpak=True, channel_probe=None)
    assert ctx.upload_in_flight_per_connection == 4
