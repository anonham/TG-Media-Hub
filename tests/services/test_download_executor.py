from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.runtime import active_task_controls
from app.core.scheduler import TaskPausedError
from app.services.download_runner import run_download_links_task


@pytest.mark.asyncio
async def test_run_download_links_progress_callback_no_name_error(tmp_path):
    task_id = "test_progress_task"
    link = "https://t.me/publicchannel/100"
    active_task_controls[task_id] = {"cancelled": False, "paused": False}

    mock_msg = MagicMock()
    mock_msg.id = 100
    mock_msg.chat_id = -100900004
    mock_msg.media = MagicMock()

    mock_client = MagicMock()
    mock_client.is_connected.return_value = True

    async def fake_download(*args, **kwargs):
        cb = kwargs.get("progress_callback")
        if cb:
            # invoke progress_cb as downloader_service / fast_telethon would
            cb(500, 1000, 1.5, 10.0, "test_file.mp4", concurrency=4, dc_id=2, concurrency_total=4)
        return str(tmp_path / "test_file.mp4")

    with (
        patch("app.services.download_runner.try_claim_executor_entry", return_value=True),
        patch("app.core.client_manager.client_manager.get_client_for_task", new=AsyncMock(return_value=mock_client)),
        patch("app.core.client_manager.client_manager.get_all_active_clients", return_value=[mock_client]),
        patch("app.core.client_manager.client_manager.userbot", mock_client),
        patch("app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock()),
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()),
        patch("app.services.download_runner.TaskQueueRepository.update_progress_async", new=AsyncMock()),
        patch("app.services.download_runner.submit_db_write") as mock_db_write,
        patch("app.services.download_runner.connection_manager.broadcast", new=AsyncMock()),
        patch(
            "app.services.downloader_service.DownloaderService.resolve_message_from_link",
            new=AsyncMock(return_value=(-100900004, mock_msg)),
        ),
        patch(
            "app.services.downloader_service.DownloaderService.get_album_messages",
            new=AsyncMock(return_value=[mock_msg]),
        ),
        patch(
            "app.services.downloader_service.DownloaderService.get_file_name_and_size",
            return_value=("test_file.mp4", 1000),
        ),
        patch("app.services.downloader_service.DownloaderService.download_single_message", side_effect=fake_download),
    ):
        await run_download_links_task(task_id, [link], threads=4)

        assert mock_db_write.called
        # Check that progress_cb passed item_text containing file name
        call_args = mock_db_write.call_args[0]
        assert "[1/1] test_file.mp4" in call_args


@pytest.mark.asyncio
async def test_run_download_links_task_pause_raises_task_paused_error(tmp_path):
    task_id = "test_pause_task"
    link = "https://t.me/publicchannel/101"
    active_task_controls[task_id] = {"cancelled": False, "paused": False}

    mock_msg = MagicMock()
    mock_msg.id = 101
    mock_msg.chat_id = -100900004
    mock_msg.media = MagicMock()

    mock_client = MagicMock()
    mock_client.is_connected.return_value = True

    async def fake_download_and_pause(*args, **kwargs):
        active_task_controls[task_id]["paused"] = True

    with (
        patch("app.services.download_runner.try_claim_executor_entry", return_value=True),
        patch("app.core.client_manager.client_manager.get_client_for_task", new=AsyncMock(return_value=mock_client)),
        patch("app.core.client_manager.client_manager.get_all_active_clients", return_value=[mock_client]),
        patch("app.core.client_manager.client_manager.userbot", mock_client),
        patch(
            "app.services.download_runner.TaskQueueRepository.update_status_async", new=AsyncMock()
        ) as mock_update_status,
        patch("app.services.download_runner.TaskQueueRepository.append_log_async", new=AsyncMock()),
        patch(
            "app.services.downloader_service.DownloaderService.resolve_message_from_link",
            new=AsyncMock(return_value=(-100900004, mock_msg)),
        ),
        patch(
            "app.services.downloader_service.DownloaderService.get_album_messages",
            new=AsyncMock(return_value=[mock_msg]),
        ),
        patch(
            "app.services.downloader_service.DownloaderService.get_file_name_and_size",
            return_value=("test_file.mp4", 1000),
        ),
        patch(
            "app.services.downloader_service.DownloaderService.download_single_message",
            side_effect=fake_download_and_pause,
        ),
        patch("app.services.download_runner._finalize_failed_task", new=AsyncMock()) as mock_finalize_failed,
    ):
        with pytest.raises(TaskPausedError):
            await run_download_links_task(task_id, [link], threads=4)

        # _finalize_failed_task should NOT have been called
        assert not mock_finalize_failed.called
        mock_update_status.assert_any_call(task_id, "paused", from_statuses=("running", "paused"))
