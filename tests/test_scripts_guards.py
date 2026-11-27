"""Unit tests for repository guard, lint, and benchmark scripts."""

import importlib.util
import json
import runpy
import subprocess
import sys
import types
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_script(rel_path: str, module_name: str):
    path = REPO_ROOT / rel_path
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_lint_ratchet_empty_stdout_with_exit_one_raises():
    mod = _load_script("scripts/lint_ratchet.py", "lint_ratchet_mod")
    fake_proc = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="invalid ruff option")
    with patch.object(mod.subprocess, "run", return_value=fake_proc), pytest.raises(RuntimeError, match="无 JSON 输出"):
        mod.count_by_rule("app")


def test_lint_ratchet_corrupted_baseline_returns_two(tmp_path):
    mod = _load_script("scripts/lint_ratchet.py", "lint_ratchet_mod_bad_json")
    bad_baseline = tmp_path / "baseline.json"
    bad_baseline.write_text("{corrupt", encoding="utf-8")
    with (
        patch.object(mod, "BASELINE_PATH", bad_baseline),
        patch.object(mod, "count_by_rule", return_value={}),
        patch.object(sys, "argv", ["lint_ratchet.py"]),
    ):
        assert mod.main() == 2


def test_pyright_report_summary_rejects_zero_files_and_bad_root(tmp_path):
    mod = _load_script("scripts/pyright_report_summary.py", "pyright_summary_mod")

    zero_files = tmp_path / "zero.json"
    zero_files.write_text(
        json.dumps({"summary": {"errorCount": 0, "filesAnalyzed": 0}, "generalDiagnostics": []}),
        encoding="utf-8",
    )
    assert mod.main(["pyright_report_summary.py", str(zero_files)]) == 1

    bad_root = tmp_path / "list.json"
    bad_root.write_text("[]", encoding="utf-8")
    assert mod.main(["pyright_report_summary.py", str(bad_root)]) == 1

    valid = tmp_path / "valid.json"
    valid.write_text(
        json.dumps({"summary": {"errorCount": 0, "filesAnalyzed": 5}, "generalDiagnostics": []}),
        encoding="utf-8",
    )
    assert mod.main(["pyright_report_summary.py", str(valid)]) == 0


def test_lint_bare_except_records_syntax_errors_and_exits_two(tmp_path):
    mod = _load_script("scripts/lint_bare_except.py", "lint_bare_except_mod")
    bad_py = tmp_path / "broken.py"
    bad_py.write_text("def broken(:\n    pass\n", encoding="utf-8")
    parse_errors: list = []
    violations = mod.scan_file(str(bad_py), parse_errors=parse_errors)
    assert violations == []
    assert len(parse_errors) == 1

    good_py = tmp_path / "ok_early_return.py"
    good_py.write_text("def f():\n    try:\n        return 1\n    except ValueError:\n        return None\n")
    ok_errors: list = []
    assert mod.scan_file(str(good_py), parse_errors=ok_errors) == []
    assert ok_errors == []


def test_verify_docs_consistency_prefix_and_markdown_links(tmp_path):
    mod = _load_script("scripts/verify_docs_consistency.py", "verify_docs_mod")
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    (docs_dir / "bad.md").write_text("See /api/ta for tasks.\n", encoding="utf-8")

    fake_app = types.SimpleNamespace(openapi=lambda: {"paths": {"/api/tasks": {}, "/api/tasks/{task_id}": {}}})
    fake_main = types.SimpleNamespace(app=fake_app)
    with patch.dict(sys.modules, {"app.main": fake_main}):
        errs = mod.check_api_endpoints(str(docs_dir))
    assert any("/api/ta" in e for e in errs)

    (docs_dir / "bad.md").write_text(
        '```markdown\n[ignored](missing_in_code.md)\n```\n[Title](ok.md "hint")\n[Root](/docs/ok.md)\n',
        encoding="utf-8",
    )
    (docs_dir / "ok.md").write_text("# OK\n", encoding="utf-8")
    with patch.object(mod, "PROJECT_ROOT", str(tmp_path)):
        link_errs = mod.check_markdown_links(str(docs_dir))
    assert link_errs == []


def test_bench_upload_speed_parse_setting_tuples():
    mod = _load_script("scripts/benchmarks/bench_upload_speed.py", "bench_upload_mod")
    assert mod._parse_setting_tuples(types.SimpleNamespace(settings=None, concurrency=None, in_flight=None)) == [
        (4, 1),
        (6, 1),
        (8, 1),
        (8, 2),
    ]
    assert mod._parse_setting_tuples(types.SimpleNamespace(settings=None, concurrency=8, in_flight=2)) == [(8, 2)]
    assert mod._parse_setting_tuples(types.SimpleNamespace(settings="3*2,5", concurrency=8, in_flight=2)) == [
        (3, 2),
        (5, 1),
    ]


def test_bench_true_multi_tcp_creates_missing_download_dir_and_file(tmp_path):
    mod = _load_script("scripts/benchmarks/bench_true_multi_tcp.py", "bench_multi_tcp_mod")
    target_dir = tmp_path / "nested_downloads"
    with patch.object(mod.settings, "DOWNLOAD_DIR", str(target_dir)):
        fh = mod._open_benchmark_output_sync(16)
        try:
            assert (target_dir / "test_multi_tcp.mp4").exists()
        finally:
            fh.close()
    assert (target_dir / "test_multi_tcp.mp4").stat().st_size == 16


def test_e2e_compare_exits_two_on_manifest_io_error(tmp_path):
    mod = _load_script("scripts/e2e_compare.py", "e2e_compare_mod")
    missing_manifest = tmp_path / "missing_manifest.json"
    with (
        patch.object(sys, "argv", ["e2e_compare.py", "--manifest", str(missing_manifest)]),
        patch.object(mod, "run_compare", side_effect=OSError("cannot read")),
        pytest.raises(SystemExit) as exc_info,
    ):
        mod.main()
    assert exc_info.value.code == 2


@pytest.mark.parametrize("counts", [None, [], {"E": -1}, {"E": True}, {"E": 1.5}, {"E": "1"}])
@pytest.mark.parametrize("update", [False, True])
def test_lint_ratchet_rejects_invalid_nested_baseline_without_overwrite(tmp_path, counts, update):
    mod = _load_script("scripts/lint_ratchet.py", "invalid_nested_baseline")
    baseline = tmp_path / "baseline.json"
    original = json.dumps({"app": counts, "tests": {}})
    baseline.write_text(original)
    with (
        patch.object(mod, "BASELINE_PATH", baseline),
        patch.object(mod, "count_by_rule", return_value={}),
        patch.object(sys, "argv", ["lint_ratchet.py", *(["--update"] if update else [])]),
    ):
        assert mod.main() == 2
    assert baseline.read_text() == original


@pytest.mark.parametrize(
    ("content", "missing"),
    [
        ("```example```\n[Missing](missing.md)\n", True),
        ("```\n``` ```\n[Example](missing.md)\n```\n", False),
        ("~~~~ lang\n~~~\n[Example](missing.md)\n~~~~  \n[Missing](missing.md)", True),
        ("~~~ info`ok\n[Example](missing.md)\n~~~\n", False),
        ("    ```\n\n[Missing](missing.md)\n", True),
        ("\t```\n\n[Missing](missing.md)\n", True),
        ("   ```\n[Example](missing.md)\n   ```\n[Missing](missing.md)\n", True),
        ("```\n    ```\n[Example](missing.md)\n```\n", False),
        ("~~~\n    ~~~\n[Example](missing.md)\n~~~\n", False),
        ("- item\n  - nested\n\n    ```\n    [Example](missing.md)\n    ```\n[Missing](missing.md)\n", True),
        ("- item\n  - nested\n\n    ```\n    [Example](missing.md)\n    ```\n", False),
        ("- item\n  - nested\n\n    ```\n        ```\n    [Example](missing.md)\n    ```\n", False),
        ("- item\n  - nested\n\n        ```\n\n[Missing](missing.md)\n", True),
        ("- item\n\n    ```\n    [Example](missing.md)\n    ```\n", False),
        ("- ```\n  [Example](missing.md)\n  ```\n[Missing](missing.md)\n", True),
        ("- ```\n  example\n\n[Missing](missing.md)\n", True),
        ("- ~~~\n  [Example](ignored.md)\n\n[Missing](missing.md)\n", True),
        ("- ```\n  example\n- [Missing](missing.md)\n", True),
        ("- item\n  - ```\n    example\n\n  [Missing](missing.md)\n", True),
        ("- item\n\n    ```\n  [Example](missing.md)\n\n  more code\n", False),
        ("```\n- item\n\n[Example](missing.md)\n", False),
    ],
)
def test_markdown_fence_boundaries(tmp_path, content, missing):
    mod = _load_script("scripts/verify_docs_consistency.py", "fence_boundaries")
    (tmp_path / "doc.md").write_text(content)
    assert bool(mod.check_markdown_links(str(tmp_path))) is missing


@pytest.mark.parametrize(
    ("url", "override", "accepted"),
    [
        ("http://localhost:8500@attacker.example", "0", False),
        ("http://localhost:8500@attacker.example", "1", False),
        ("https://user@example.com", "0", False),
        ("http://localhost.evil", "0", False),
        ("http://example.com", "0", False),
        ("http://example.com", "1", True),
        ("http://localhost:8500", "0", True),
        ("http://127.0.0.1:8500", "0", True),
        ("http://[::1]:8500", "0", True),
        ("https://example.com", "0", True),
    ],
)
def test_accept_2fa_url_guard(url, override, accepted):
    import os

    wsl_vars = "BASE_URL/u:ALLOW_INSECURE_HTTP/u:API_KEY/u"
    wslenv = ":".join(filter(None, [os.environ.get("WSLENV"), wsl_vars]))
    # No key: accepted URLs stop at the key guard, before any network request.
    result = subprocess.run(
        ["bash", "scripts/accept_2fa.sh"],
        cwd=REPO_ROOT,
        env={**os.environ, "BASE_URL": url, "ALLOW_INSECURE_HTTP": override, "API_KEY": "", "WSLENV": wslenv},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert ("API_KEY" in result.stdout) is accepted


def test_bench_redirect_is_caught_by_health_check():
    import urllib.error
    import urllib.request

    mod = _load_script("scripts/benchmarks/bench_runner.py", "bench_redirect")
    req = urllib.request.Request("http://127.0.0.1/api/tasks")
    with pytest.raises(urllib.error.HTTPError):
        mod._NoRedirectHandler().redirect_request(req, None, 302, "Found", {}, "http://example.com")
    with (
        patch.object(mod, "_safe_request", return_value=req),
        patch.object(mod._OPENER, "open", side_effect=urllib.error.HTTPError(req.full_url, 302, "", {}, None)),
    ):
        assert mod.check_server_health() is False


def test_bench_https_pins_connection_and_preserves_tls_hostname():
    from unittest.mock import MagicMock

    mod = _load_script("scripts/benchmarks/bench_runner.py", "bench_pinned_https")
    with (
        patch.object(mod, "SERVER_URL", "https://internal.example:8443"),
        patch.object(mod.socket, "gethostbyname", return_value="127.0.0.1") as resolve,
    ):
        req = mod._safe_request("/api/tasks")
    resolve.assert_called_once_with("internal.example")
    context = MagicMock()
    handler = mod._PinnedHTTPSHandler(context=context)
    sock = MagicMock()
    with patch.object(mod.socket, "create_connection", return_value=sock) as connect:

        def open_connection(factory, request, **kwargs):
            conn = factory(request.host, context=kwargs["context"])
            conn.connect()

        with patch.object(handler, "do_open", side_effect=open_connection):
            handler.https_open(req)
    assert connect.call_args.args[0] == ("127.0.0.1", 8443)
    context.wrap_socket.assert_called_once_with(sock, server_hostname="internal.example")


def test_gitleaks_workflow_detects_merge_only_secret(tmp_path):
    import re
    import secrets
    import shutil

    gitleaks = shutil.which("gitleaks")
    if gitleaks is None:
        pytest.skip("Gitleaks CLI required for merge-history integration regression")

    def git(*args):
        return subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True, text=True).stdout.strip()

    git("init", "-b", "main")
    git("config", "user.email", "test@example.invalid")
    git("config", "user.name", "Regression Test")
    (tmp_path / "base").write_text("base")
    git("add", ".")
    git("commit", "-m", "base")
    base = git("rev-parse", "HEAD")
    git("checkout", "-b", "side")
    (tmp_path / "side").write_text("side")
    git("add", ".")
    git("commit", "-m", "side")
    git("checkout", "main")
    (tmp_path / "main").write_text("main")
    git("add", ".")
    git("commit", "-m", "main")
    git("merge", "--no-ff", "--no-commit", "side")
    # Synthetic, deliberately detectable token exists in neither parent.
    (tmp_path / "merge.env").write_text(f'api_key="{secrets.token_hex(32)}"\n')
    git("add", ".")
    git("commit", "-m", "merge-only fixture")
    head = git("rev-parse", "HEAD")

    def scan(options):
        return subprocess.run(
            [gitleaks, "git", "--redact", "--log-opts=" + options, "."],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        ).returncode

    assert scan(f"{base}..{head}") == 0  # Demonstrate the original blind spot.
    workflow = (REPO_ROOT / ".github/workflows/security.yml").read_text()
    options = re.findall(r'--log-opts="([^"]+)"', workflow)
    assert len(options) == 3
    for template in options:
        option = template
        for variable, value in (
            ("PR_BASE_SHA", base),
            ("PUSH_BEFORE_SHA", base),
            ("PR_HEAD_SHA", head),
            ("HEAD_SHA", head),
        ):
            option = option.replace("${" + variable + "}", value)
        assert scan(option) == 1


@pytest.mark.parametrize(
    ("rel_path", "use_main_argv"),
    [
        ("scripts/pyright_report_summary.py", ["pyright_report_summary.py", "--help"]),
        ("scripts/lint_standards.py", ["--help"]),
        ("scripts/lint_bare_except.py", ["--help"]),
        ("scripts/verify_docs_consistency.py", ["--help"]),
        ("scripts/generate_placeholder_poster.py", ["--help"]),
        ("scripts/verify_transit.py", None),
        ("scripts/benchmarks/bench_download_speed.py", None),
        ("scripts/benchmarks/bench_multi_client_speed.py", None),
        ("scripts/benchmarks/bench_true_multi_tcp.py", None),
        ("scripts/benchmarks/bench_runner.py", None),
    ],
)
def test_scripts_support_help_without_side_effects(rel_path, use_main_argv, monkeypatch, capsys):
    def _invoke():
        if use_main_argv is not None:
            mod = _load_script(rel_path, f"help_smoke_{Path(rel_path).stem}")
            mod.main(use_main_argv)
            return
        monkeypatch.setattr(sys, "argv", [Path(rel_path).name, "--help"])
        runpy.run_path(str(REPO_ROOT / rel_path), run_name="__main__")

    with pytest.raises(SystemExit) as exc_info:
        _invoke()
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "usage:" in captured.out


@pytest.mark.parametrize("layer", ["api", "services", "core"])
@pytest.mark.parametrize("primitive", ["get_db", "db_cursor", "db_immediate_tx"])
def test_lint_standards_rejects_db_primitives_outside_db_layer(tmp_path, monkeypatch, layer, primitive):
    mod = _load_script("scripts/lint_standards.py", f"lint_standards_prim_{layer}_{primitive}")
    target = tmp_path / "app" / layer / "sample.py"
    target.parent.mkdir(parents=True)
    target.write_text(f"from app.db.database import {primitive}\n", encoding="utf-8")
    monkeypatch.setattr(mod, "REPO_ROOT", tmp_path)

    errors = mod.check_layering_boundaries()
    assert any(f"app/{layer}" in err and primitive in err for err in errors)


def test_lint_standards_allows_core_repository_imports(tmp_path, monkeypatch):
    mod = _load_script("scripts/lint_standards.py", "lint_standards_core_repo_ok")
    target = tmp_path / "app" / "core" / "sample.py"
    target.parent.mkdir(parents=True)
    target.write_text("from app.db.task_repository import TaskQueueRepository\n", encoding="utf-8")
    monkeypatch.setattr(mod, "REPO_ROOT", tmp_path)

    assert mod.check_layering_boundaries() == []
