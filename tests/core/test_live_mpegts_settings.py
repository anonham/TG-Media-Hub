"""Offline coverage for the Live E2E settings guard."""

from unittest.mock import MagicMock, call

import pytest

from tests.e2e_live.test_06_media_admission_thumbs import _enable_remux_for_live_test

KEY = "auto_remux_mpegts_to_mp4"


@pytest.mark.parametrize(("external", "previous"), [(True, "false"), (True, None), (False, None)])
def test_live_remux_skips_when_it_cannot_toggle_safely(external, previous):
    api = MagicMock()
    api.get.return_value.json.return_value = {"all_settings": {KEY: previous} if previous is not None else {}}

    with pytest.raises(pytest.skip.Exception), _enable_remux_for_live_test(api, external=external):
        pytest.fail("This test should skip before running the Live E2E body")

    api.put.assert_not_called()


def test_live_remux_already_enabled_on_external_server_is_read_only():
    api = MagicMock()
    api.get.return_value.json.return_value = {"all_settings": {KEY: "true"}}
    reached_body = MagicMock()

    with _enable_remux_for_live_test(api, external=True):
        reached_body()

    reached_body.assert_called_once_with()
    api.put.assert_not_called()


@pytest.mark.parametrize("failure_on_enable", [False, True])
def test_live_remux_restores_original_setting_after_failure(failure_on_enable):
    api = MagicMock()
    api.get.return_value.json.return_value = {"all_settings": {KEY: "false"}}
    if failure_on_enable:
        api.put.side_effect = [RuntimeError("expected failure"), MagicMock()]

    with pytest.raises(RuntimeError, match="expected failure"), _enable_remux_for_live_test(api, external=False):
        raise RuntimeError("expected failure")

    assert api.put.call_args_list == [
        call("/api/settings", json={"settings": {KEY: "true"}}),
        call("/api/settings", json={"settings": {KEY: "false"}}),
    ]
