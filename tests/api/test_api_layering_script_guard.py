"""API 层依赖方向脚本守卫测试: 校验 scripts.lint_standards 与 AST 门禁一致。"""

from pathlib import Path

import pytest

import tests.api.test_api_layering as api_layering


@pytest.mark.parametrize(
    ("source", "forbidden"),
    [
        ("import app.db.tasks", True),
        ("from app.db.tasks import TaskRepository", True),
        ("from ..db.tasks import TaskRepository", True),
        ("from app import db", True),
        ("from .. import db", True),
        ("from app import db as database", True),
        ("def handler():\n    from .. import db", True),
        ("from app import services", False),
        ("from ..services import tasks", False),
        ("from . import tasks", False),
        ("import app.db_helpers", False),
    ],
)
@pytest.mark.parametrize("relative_path", ["app/api/tasks.py", "app/api/__init__.py", "app/api/nested/tasks.py"])
def test_import_guards_detect_db_imports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str, forbidden: bool, relative_path: str
) -> None:
    from scripts import lint_standards

    # Nested API packages need one extra level to reach app.
    if "nested" in relative_path:
        source = source.replace("from ..", "from ...")
    path = tmp_path / relative_path
    path.parent.mkdir(parents=True)
    path.write_text(source, encoding="utf-8")
    monkeypatch.setattr(api_layering, "_REPO_ROOT", tmp_path)
    monkeypatch.setattr(lint_standards, "REPO_ROOT", tmp_path)

    assert bool(api_layering._module_imports_db(path)) is forbidden
    assert bool(lint_standards.check_layering_boundaries()) is forbidden
    if forbidden:
        with pytest.raises(AssertionError, match="app/db"):
            api_layering.test_api_file_does_not_import_db_directly(path)
    else:
        api_layering.test_api_file_does_not_import_db_directly(path)
