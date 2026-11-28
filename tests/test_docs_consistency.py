"""
Documentation Consistency & Symbol Integrity Guards.

This test module implements automated guards ensuring that technical documentation
remains 100% faithful to the codebase, preventing regressions such as:
- G1: Environment variables in app/config.py missing from .env.example (or undocumented variables).
- G2: Universal symbol & class verification: Mermaid classDiagram classes and backticked
      architectural components must exist in app/ AST (eliminating fabricated classes).
- G3: Universal API endpoint & HTTP method verification: All endpoint paths and methods
      referenced in docs/ must match real FastAPI OpenAPI routes (preventing fake routes & method mismatches).
- G4: Source file line number references ('xxx.py:line') in docs/ or README.md.
"""

import ast
import glob
import os
import re

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_g1_env_example_matches_config():
    """G1: .env.example keys and app/config.py environment variables must stay synchronized."""
    config_file = os.path.join(PROJECT_ROOT, "app", "config.py")
    example_file = os.path.join(PROJECT_ROOT, ".env.example")

    assert os.path.exists(config_file), "app/config.py not found"
    assert os.path.exists(example_file), ".env.example not found"

    config_content = open(config_file, encoding="utf-8").read()
    config_keys = set(re.findall(r'getenv\(\s*["\']([A-Z0-9_]+)["\']', config_content))
    config_keys.update(re.findall(r'_optional_\w+\(\s*["\']([A-Z0-9_]+)["\']', config_content))

    example_content = open(example_file, encoding="utf-8").read()
    # Matches active or commented assignment keys, e.g. FOO=... or # FOO=...
    example_keys = set(re.findall(r"^[#\s]*([A-Z0-9_]+)=", example_content, re.MULTILINE))

    # All keys accessed in config.py must be present/documented in .env.example
    missing_in_example = config_keys - example_keys
    assert not missing_in_example, (
        f"Environment variables in app/config.py missing from .env.example: {sorted(missing_in_example)}"
    )

    # Allowed auxiliary keys in .env.example that are consumed by subprocesses/CLI or tests
    known_auxiliary_keys = {
        "PIKPAK_DIR",
        "PIKPAK_NO_LOG",
        "PIKPAK_NO_UPDATE_NOTIFIER",
        "TEST_CHANNEL_LINK",
        "TEST_SAMPLE_FOLDER",
    }
    unexpected_in_example = example_keys - config_keys - known_auxiliary_keys
    assert not unexpected_in_example, (
        f"Undocumented/unexpected keys found in .env.example: {sorted(unexpected_in_example)}"
    )

    # All keys in .env.example must also be documented in docs/guides/CONFIGURATION.md
    cfg_doc = os.path.join(PROJECT_ROOT, "docs", "guides", "CONFIGURATION.md")
    assert os.path.exists(cfg_doc), "docs/guides/CONFIGURATION.md not found"
    with open(cfg_doc, encoding="utf-8") as f_cfg:
        cfg_doc_content = f_cfg.read()
    missing_in_cfg_doc = [k for k in sorted(example_keys) if f"`{k}`" not in cfg_doc_content]
    assert not missing_in_cfg_doc, (
        f"Environment variables in .env.example missing from docs/guides/CONFIGURATION.md: {missing_in_cfg_doc}"
    )


def test_g2_docs_symbols_and_classes_exist():
    """G2 (Universal): Major architecture classes, Mermaid classDiagrams, and components in docs/ must exist in app/."""
    app_symbols = set()
    for py in glob.glob(os.path.join(PROJECT_ROOT, "app", "**", "*.py"), recursive=True):
        content = open(py, encoding="utf-8", errors="ignore").read()
        try:
            tree = ast.parse(content, py)
            for node in ast.walk(tree):
                if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                    app_symbols.add(node.name)
        except Exception:
            pass

    # Standard libraries and external packages whitelist
    external_whitelist = {
        "FastAPI",
        "Telethon",
        "SQLite",
        "ThreadPoolExecutor",
        "ProcessPoolExecutor",
        "AsyncClient",
        "APIRouter",
        "HTTPException",
        "Depends",
        "WebSocket",
        "Uvicorn",
        "Caddy",
        "Nginx",
        "Docker",
        "Dockerfile",
        "Vite",
        "Pinia",
        "Vue",
        "BaseModel",
        "Enum",
        "InputPhoto",
        "Message",
        "TelegramClient",
        "PostgreSQL",
        "Redis",
        "Prometheus",
        "Linux",
        "Windows",
        "Unix",
        "Bash",
        "GitHub",
        "Git",
        "Pip",
        "ItemSource",
        "MediaTransportEngine",
        "CleanupStrategy",
        "NotificationStrategy",
    }

    # Regression blacklist of previously fabricated symbols
    banned_symbols = [
        "IPublishStrategy",
        "LocalDiskPublishStrategy",
        "PikPakStreamingPublishStrategy",
        "SingleWriterExecutor",
        "VsseProvider",
    ]

    docs = glob.glob(os.path.join(PROJECT_ROOT, "docs", "**", "*.md"), recursive=True)
    assert len(docs) > 0, "No markdown files found under docs/"

    class_diagram_pat = re.compile(r"```mermaid[\s\S]*?classDiagram([\s\S]*?)```")
    component_pat = re.compile(
        r"`([A-Z][a-zA-Z0-9]+(?:Service|Engine|Strategy|Provider|Manager|Middleware|Scheduler|Source|Prober|Notifier|Uploader|Coordinator|Executor))`"
    )

    errors = []

    for doc in docs:
        content = open(doc, encoding="utf-8").read()
        rel_path = os.path.relpath(doc, PROJECT_ROOT)

        # 1. Regression check against known banned symbols
        for banned in banned_symbols:
            if banned in content:
                errors.append(f"{rel_path}: Contains banned fabricated symbol '{banned}'")

        # 2. Universal check across all Mermaid classDiagram blocks in all documentation
        for block in class_diagram_pat.findall(content):
            for c in re.findall(r"\bclass\s+([A-Za-z0-9_]+)\b", block):
                if c not in app_symbols and c not in external_whitelist:
                    errors.append(f"{rel_path} classDiagram: Class '{c}' not found in app/ codebase")

        # 3. Universal check for backticked architectural component names
        if "plans" not in doc:  # Future evolution roadmaps in plans/ are exempt from present-code check
            for m in component_pat.finditer(content):
                symbol = m.group(1)
                if symbol not in app_symbols and symbol not in external_whitelist:
                    errors.append(f"{rel_path}: Component `{symbol}` not found in app/ codebase")

    assert not errors, "G2 Universal Symbol Integrity Violations:\n" + "\n".join(errors)


def test_g3_docs_api_endpoints_exist_in_openapi():
    """G3 (Universal): All API endpoint paths and HTTP methods referenced in docs/ must match real FastAPI routes."""
    from app.main import app

    openapi_spec = app.openapi()
    openapi_paths = set(openapi_spec.get("paths", {}).keys())

    # Build set of valid (METHOD, path) combinations
    valid_endpoints = set()
    for path, path_item in openapi_spec.get("paths", {}).items():
        for method in path_item.keys():
            valid_endpoints.add((method.upper(), path))

    # Blacklist of known fake route patterns
    banned_route_patterns = [
        r"/api/inspect/channel\b",
        r"/api/inspect/range\b",
        r"/api/pikpak/status\b",
        r"/api/pikpak/files\b(?!/list)",
        r"/api/pikpak/archive-target\b",
        r"GET\s+/api/storage/quota\b",
    ]

    table_pat = re.compile(r"\|\s*`?(GET|POST|PUT|DELETE|PATCH)`?\s*\|\s*`?(/api/[a-z0-9/{}_-]+)`?")
    inline_pat = re.compile(r"\b(GET|POST|PUT|DELETE|PATCH)\s+`?(/api/[a-z0-9/{}_-]+)`?")

    docs = glob.glob(os.path.join(PROJECT_ROOT, "docs", "**", "*.md"), recursive=True)
    errors = []

    for doc in docs:
        content = open(doc, encoding="utf-8").read()
        rel_path = os.path.relpath(doc, PROJECT_ROOT)

        # 1. Banned pattern check
        for pat in banned_route_patterns:
            if re.search(pat, content):
                errors.append(f"{rel_path}: Matches banned route pattern '{pat}'")

        # 2. Universal (METHOD, path) pair check (both table rows and inline mentions)
        method_path_pairs = set(table_pat.findall(content) + inline_pat.findall(content))
        for method, path in method_path_pairs:
            if (method, path) not in valid_endpoints:
                errors.append(f"{rel_path}: Invalid route or unsupported HTTP method '{method} {path}'")

        # 3. Path & prefix validation
        for m in re.finditer(r"/api/[a-z0-9/{}_-]+", content):
            route = m.group(0)
            if route in openapi_paths:
                continue
            is_prefix = any(p.startswith(route) for p in openapi_paths)
            if not is_prefix and route not in openapi_paths:
                errors.append(f"{rel_path}: Route '{route}' is neither a real path nor a registered prefix")

    assert not errors, "G3 Universal API & HTTP Method Violations:\n" + "\n".join(errors)


def test_g4_zero_line_numbers_in_docs_and_readme():
    """G4: Documentation must be free of unstable source line references (xxx.py/vue/js/ts/css:line)."""
    targets = [
        os.path.join(PROJECT_ROOT, "README.md"),
        os.path.join(PROJECT_ROOT, "CONTRIBUTING.md"),
        os.path.join(PROJECT_ROOT, "SECURITY.md"),
    ] + glob.glob(os.path.join(PROJECT_ROOT, "docs", "**", "*.md"), recursive=True)
    pat = re.compile(r"\b[a-zA-Z0-9_\-\./]+\.(?:py|vue|js|ts|css):[0-9]+\b")
    errors = []
    for t in targets:
        if not os.path.exists(t):
            continue
        content = open(t, encoding="utf-8", errors="ignore").read()
        matches = pat.findall(content)
        if matches:
            errors.append(f"{os.path.relpath(t, PROJECT_ROOT)}: {matches}")

    assert not errors, "Found source line number references in documentation:\n" + "\n".join(errors)


def test_g5_normative_standards_guard():
    """G5: Run normative standards guard verifying units, concurrency contracts, and doc symbols."""
    from scripts.lint_standards import (
        check_concurrency_contracts,
        check_config_units,
        check_configuration_doc_parity,
        check_docs_symbols,
        check_layering_boundaries,
        check_test_awareness_absence,
        check_upload_in_flight_absence,
        check_upload_limit_precision,
    )

    errors = []
    errors.extend(check_docs_symbols())
    errors.extend(check_config_units())
    errors.extend(check_concurrency_contracts())
    errors.extend(check_configuration_doc_parity())
    errors.extend(check_upload_limit_precision())
    errors.extend(check_upload_in_flight_absence())
    errors.extend(check_test_awareness_absence())
    errors.extend(check_layering_boundaries())

    assert not errors, f"Normative standards guard failed with {len(errors)} errors:\n" + "\n".join(errors)
