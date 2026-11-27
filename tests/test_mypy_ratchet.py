"""mypy 白名单棘轮测试 (只减不增).

pyproject ``[tool.mypy.overrides]`` 的 ``ignore_errors = true`` 模块清单即类型债台账:
模块清偿清零后从清单删除 (基线收紧, 本测试静默通过); 任何新增模块 (白名单增长) 必须
显式扩本文件的锁定集, 否则失败 —— 防止静默放宽类型检查范围。
"""

import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# 锁定基线: 22 模块 / 119 错。只允许减少。
FROZEN_WHITELIST = frozenset(
    {
        "app.api.download",
        "app.api.pikpak",
        "app.api.tasks",
        "app.core.cdn_transport",
        "app.core.client_manager",
        "app.core.e2e_capture",
        "app.core.e2e_compare",
        "app.core.entity_resolver",
        "app.core.executor_progress",
        "app.core.runtime",
        "app.core.scheduler",
        "app.services.channel_inspector",
        "app.services.download_runner",
        "app.services.downloader_service",
        "app.services.forward_service",
        "app.services.pikpak_publisher_service",
        "app.services.pikpak_service",
        "app.services.publish_runner",
        "app.services.publisher_service",
        "app.services.storage_service",
        "app.services.task_strategies.forward_channel_strategy",
    }
)


def test_mypy_error_whitelist_only_shrinks() -> None:
    """白名单模块只减不增: 新增模块须显式扩 FROZEN_WHITELIST (评审可见)。"""
    with open(PROJECT_ROOT / "pyproject.toml", "rb") as fh:
        data = tomllib.load(fh)
    overrides = data["tool"]["mypy"]["overrides"]
    modules: set[str] = set()
    for override in overrides:
        if override.get("ignore_errors") is True:
            modules.update(override.get("module", []))
    added = modules - FROZEN_WHITELIST
    assert not added, f"mypy ignore_errors 白名单新增模块须显式扩锁定集: {sorted(added)}"
