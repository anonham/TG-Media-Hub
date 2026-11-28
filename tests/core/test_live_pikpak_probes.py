"""Offline regression checks for live PikPak assertions."""

from unittest.mock import Mock

import pytest
from e2e_live.harness import pikpak_probe


@pytest.mark.parametrize("detail", ["PikPak HTTP 403 Forbidden", "本地目录缺少写入权限"])
def test_concrete_failure_is_reported_even_if_prior_log_mentions_chat_permissions(detail):
    api = Mock()
    api.get.return_value.json.return_value = {"logs": [{"message": "ChatWriteForbiddenError"}]}
    with pytest.raises(AssertionError, match=detail):
        pikpak_probe.completed_or_env_skip(api, {"id": "task", "status": "failed", "error_detail": detail})
    api.get.assert_not_called()


@pytest.mark.parametrize("detail", ["CHAT_WRITE_FORBIDDEN", "执行账号对目标频道无发帖权限"])
def test_channel_write_failure_skips(detail):
    with pytest.raises(pytest.skip.Exception, match="无发帖权限"):
        pikpak_probe.completed_or_env_skip(Mock(), {"id": "task", "status": "failed", "error_detail": detail})


@pytest.mark.parametrize(
    ("messages", "has_more", "skip"),
    [
        (["ChatWriteForbiddenError"], False, True),
        (["ChatWriteForbiddenError", "CDN: 403 Forbidden"], False, False),
        (["ChatWriteForbiddenError"], True, False),
    ],
)
def test_logs_fallback_requires_terminal_channel_error(messages, has_more, skip):
    api = Mock()
    api.get.return_value.status_code = 200
    api.get.return_value.json.return_value = {"logs": [{"message": m} for m in messages], "has_more": has_more}
    expected = pytest.skip.Exception if skip else AssertionError
    with pytest.raises(expected):
        pikpak_probe.completed_or_env_skip(api, {"id": "task", "status": "failed"})


def test_archive_preserves_an_unsubmitted_subdirectory(monkeypatch):
    from e2e_live import test_04_archive as archive

    drama = {"file_id": "drama", "name": "剧集A", "is_dir": True}
    season = {"file_id": "season", "name": "第一季", "is_dir": True}
    extras = {"file_id": "extras", "name": "花絮", "is_dir": True}
    leaf = {"file_id": "episode", "name": "01.mp4", "size_bytes": 128, "is_dir": False}
    tree = {"root": [drama], "drama": [season], "season": [leaf, extras]}
    monkeypatch.setenv("E2E_LIVE_PIKPAK_REMOTE_DIR", "/test")
    monkeypatch.setattr(pikpak_probe, "auth_state", lambda api: "logged_in")
    monkeypatch.setattr(pikpak_probe, "resolve_remote_folder", lambda api, path: "root")
    monkeypatch.setattr(pikpak_probe, "list_remote_page", lambda api, folder_id: tree[folder_id])
    stage = archive.archive_stage.__wrapped__(None, None)
    assert stage["unsubmitted_ids"] == {"extras"}
    tree["season"] = [extras]
    archive._assert_source_preserved(None, stage)
    tree["season"] = []
    with pytest.raises(AssertionError, match="extras"):
        archive._assert_source_preserved(None, stage)
