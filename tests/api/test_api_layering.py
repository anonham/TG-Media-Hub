"""API 层依赖方向全量门禁: app/api/ 全目录严禁直连 app/db 仓库。

依赖方向契约: app/api → app/services → app/core → app/db。
全量 AST 扫描 app/api/**/*.py (含函数内延迟导入与相对导入), 零白名单豁免。"""

import ast
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_API_DIR = _REPO_ROOT / "app" / "api"
_API_PY_FILES = sorted(_API_DIR.rglob("*.py"))


def _module_imports_db(path: Path) -> list[str]:
    """AST 扫描模块内全部 (含函数级与相对导入) app.db 导入"""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    hits: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            hits.extend(
                alias.name for alias in node.names if alias.name == "app.db" or alias.name.startswith("app.db.")
            )
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if node.level:
                parts = path.relative_to(_REPO_ROOT).parts[:-1]
                if node.level > len(parts):
                    continue
                base_pkg = ".".join(parts[: len(parts) - node.level + 1])
                module = f"{base_pkg}.{module}" if module else base_pkg
            imports = [module, *(f"{module}.{alias.name}" for alias in node.names)]
            hits.extend(mod for mod in imports if mod == "app.db" or mod.startswith("app.db."))
    return hits


@pytest.mark.parametrize("py_path", _API_PY_FILES, ids=lambda p: p.relative_to(_API_DIR).as_posix())
def test_api_file_does_not_import_db_directly(py_path: Path) -> None:
    hits = _module_imports_db(py_path)
    rel = py_path.relative_to(_REPO_ROOT).as_posix()
    assert not hits, f"{rel} 直连 app/db 违反依赖方向 app/api→app/services→app/core→app/db: {hits}"


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
    monkeypatch.setitem(globals(), "_REPO_ROOT", tmp_path)
    monkeypatch.setattr(lint_standards, "REPO_ROOT", tmp_path)

    assert bool(_module_imports_db(path)) is forbidden
    assert bool(lint_standards.check_layering_boundaries()) is forbidden
    if forbidden:
        with pytest.raises(AssertionError, match="app/db"):
            test_api_file_does_not_import_db_directly(path)
    else:
        test_api_file_does_not_import_db_directly(path)
