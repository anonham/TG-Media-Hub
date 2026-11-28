#!/usr/bin/env python
"""TG-Media-Hub 架构规范、单位度量与命名契约静态守卫脚本。

本脚本执行轻量级 AST 与文本契约校验，防止：
1. 存储度量混乱与虚假常量；
2. 并发变量别名与调用端 keyword 参数传递错误；
3. 文档中虚构类名与参数默认值失真。
"""

import argparse
import ast
import re
import sys
from pathlib import Path

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent


def check_docs_symbols() -> list[str]:
    """检查文档不得包含已知虚构或已废弃符号。"""
    banned_symbols = [
        "VsseStreamProvider",
        "SingleWriterExecutor",
        "IPublishStrategy",
        "LocalDiskPublishStrategy",
        "PikPakStreamingPublishStrategy",
        "VsseProvider",
    ]
    errors = []
    docs_dir = REPO_ROOT / "docs"
    for md in docs_dir.rglob("*.md"):
        content = md.read_text(encoding="utf-8", errors="ignore")
        for sym in banned_symbols:
            if sym in content:
                rel = md.relative_to(REPO_ROOT)
                errors.append(f"{rel}: 包含已废弃或虚构符号 '{sym}'")
    return errors


def check_config_units() -> list[str]:
    """检查核心配置常量与单位规范。"""
    errors = []
    config_py = REPO_ROOT / "app" / "config.py"
    if not config_py.exists():
        errors.append("app/config.py 不存在")
        return errors

    content = config_py.read_text(encoding="utf-8", errors="ignore")
    # 必须具备 typed MIB 常量
    for req in ["MAX_FILE_SIZE_MIB", "PREMIUM_MAX_FILE_SIZE_MIB"]:
        if req not in content:
            errors.append(f"app/config.py: 缺少规范化的 typed 常量 '{req}'")

    # 检查除 app/config.py 之外的代码禁止引用 TWO_GB
    two_gb_targets: list[Path] = []
    run_py = REPO_ROOT / "run.py"
    if run_py.exists():
        two_gb_targets.append(run_py)
    for py_dir in [REPO_ROOT / "app", REPO_ROOT / "scripts", REPO_ROOT / "tests"]:
        if py_dir.exists():
            two_gb_targets.extend(py_dir.rglob("*.py"))

    for py_file in two_gb_targets:
        rel = py_file.relative_to(REPO_ROOT).as_posix()
        if rel in {"app/config.py", "scripts/lint_standards.py"}:
            continue
        c = py_file.read_text(encoding="utf-8", errors="ignore")
        if re.search(r"\bTWO_GB\b", c):
            errors.append(f"{rel}: 禁止引用具有歧义的历史废弃常量 'TWO_GB'")

    return errors


def check_concurrency_contracts() -> list[str]:
    """检查执行器签名与 API 参数契约。"""
    errors = []

    # 1. 检查 publish.py 传参
    pub_api = REPO_ROOT / "app" / "api" / "publish.py"
    if pub_api.exists():
        content = pub_api.read_text(encoding="utf-8", errors="ignore")
        if 'pub_kwargs["upload_in_flight"]' in content:
            errors.append("app/api/publish.py: 存在错误传递非规范参数 pub_kwargs['upload_in_flight']")
        if "threads_for_display" in content:
            errors.append("app/api/publish.py: 存在将上传并发混称为 threads_for_display 的命名违规")

    # 2. 检查 publish_runner.py 签名
    exec_service = REPO_ROOT / "app" / "services" / "publish_runner.py"
    if exec_service.exists():
        content = exec_service.read_text(encoding="utf-8", errors="ignore")
        try:
            tree = ast.parse(content, filename=str(exec_service))
        except SyntaxError as err:
            errors.append(f"app/services/publish_runner.py: AST 解析失败 ({err})")
        else:
            func_args = {}
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    func_args[node.name] = [a.arg for a in (node.args.args + node.args.kwonlyargs)]

            for fname in ["run_publish_folder_task", "run_publish_pikpak_task"]:
                args = func_args.get(fname, [])
                if "upload_in_flight_per_connection" not in args:
                    errors.append(
                        f"app/services/publish_runner.py: {fname} 缺少规范参数 'upload_in_flight_per_connection'"
                    )

    return errors


def check_configuration_doc_parity() -> list[str]:
    """检查 CONFIGURATION.md 与代码默认值及变量完备性的一致性。"""
    errors = []
    cfg_doc = REPO_ROOT / "docs" / "guides" / "CONFIGURATION.md"
    if cfg_doc.exists():
        content = cfg_doc.read_text(encoding="utf-8", errors="ignore")
        # 1. 必须注明 UPLOAD_PARALLEL_CONNECTIONS 默认值为 2
        m = re.search(r"`UPLOAD_PARALLEL_CONNECTIONS`\s*\|\s*`int`\s*\|\s*否\s*\|\s*`(\d+)`", content)
        if m:
            val = m.group(1)
            if val != "2":
                errors.append(f"docs/guides/CONFIGURATION.md: UPLOAD_PARALLEL_CONNECTIONS 默认值应为 2, 实得 {val}")
        # 2. 必须注明 PIKPAK_PREFETCH_CONCURRENCY 默认值为 3
        m_pre = re.search(r"`PIKPAK_PREFETCH_CONCURRENCY`\s*\|\s*`int`\s*\|\s*否\s*\|\s*`(\d+)`", content)
        if m_pre:
            val_pre = m_pre.group(1)
            if val_pre != "3":
                errors.append(f"docs/guides/CONFIGURATION.md: PIKPAK_PREFETCH_CONCURRENCY 默认值应为 3, 实得 {val_pre}")
        # 3. 检查 TASK_LOGS_RETENTION_PER_TASK 已收录
        if "TASK_LOGS_RETENTION_PER_TASK" not in content:
            errors.append("docs/guides/CONFIGURATION.md: 漏列环境变量 'TASK_LOGS_RETENTION_PER_TASK'")
    return errors


def check_upload_limit_precision() -> list[str]:
    """检查系统关于普通和 Premium 上传限制的度量口径绝对规范：
    禁止将普通限制标为 2000MB、2GB 或 2GiB；必须严格指明 2000 MiB (1.95 GiB)。
    禁止将 Premium 限制标为 4000MB、4GB 或 4GiB；必须严格指明 4000 MiB (3.91 GiB)。
    """
    errors = []
    bad_patterns = [
        (
            re.compile(r"(?<!MAX_FILE_SIZE_)(?<!PREMIUM_MAX_FILE_SIZE_)2000\s*MB(?!_)", re.IGNORECASE),
            "禁止使用 '2000MB'，普通账号上传上限必须严格指明为 2000 MiB (1.95 GiB / 2097.15 MB)",
        ),
        (
            re.compile(r"(?<!PREMIUM_MAX_FILE_SIZE_)4000\s*MB(?!_)", re.IGNORECASE),
            "禁止使用 '4000MB'，Premium 账号上传上限必须严格指明为 4000 MiB (3.91 GiB / 4194.30 MB)",
        ),
        (
            re.compile(r"(?<![\d.])2\s*(?:GB|GiB)(?![\w.])", re.IGNORECASE),
            "禁止使用裸 '2GB' 或 '2GiB'，普通账号上传上限必须严格指明为 2000 MiB (1.95 GiB)",
        ),
        (
            re.compile(r"(?<![\d.])4\s*(?:GB|GiB)(?![\w.])", re.IGNORECASE),
            "禁止使用裸 '4GB' 或 '4GiB'，Premium 账号上传上限必须严格指明为 4000 MiB (3.91 GiB)",
        ),
    ]

    scan_files = []
    # 根目录下环境、说明文档与入口脚本 (tests/ 含 MP4 64-bit > 4GB 偏移与磁盘配额测试，不纳入上传上限文案检查)
    for root_file in [
        REPO_ROOT / ".env.example",
        REPO_ROOT / "run.py",
        *REPO_ROOT.glob("*.md"),
        *REPO_ROOT.glob("*.js"),
        *REPO_ROOT.glob("*.ts"),
    ]:
        if root_file.exists():
            scan_files.append(root_file)

    scan_dirs = [REPO_ROOT / "app", REPO_ROOT / "docs", REPO_ROOT / "scripts", REPO_ROOT / "frontend" / "src"]
    for s_dir in scan_dirs:
        if not s_dir.exists():
            continue
        for ext in ["*.py", "*.md", "*.vue", "*.js", "*.ts"]:
            scan_files.extend(s_dir.rglob(ext))

    for f in scan_files:
        rel = f.relative_to(REPO_ROOT).as_posix()
        if rel == "scripts/lint_standards.py":
            continue
        content = f.read_text(encoding="utf-8", errors="ignore")
        for pattern, msg in bad_patterns:
            for m in pattern.finditer(content):
                line_num = content[: m.start()].count("\n") + 1
                errors.append(f"{rel}:{line_num}: {msg}")
    return errors


def check_upload_in_flight_absence() -> list[str]:
    """检查除 API/DB 历史兼容别名以外，禁止代码中出现属性 context.upload_in_flight 或参数形参 upload_in_flight。"""
    errors = []
    # 1. 检查 context.upload_in_flight 属性调用
    context_pattern = re.compile(r"\bcontext\.upload_in_flight\b(?!_per_connection)")
    for py_file in (REPO_ROOT / "app").rglob("*.py"):
        content = py_file.read_text(encoding="utf-8", errors="ignore")
        for m in context_pattern.finditer(content):
            rel = py_file.relative_to(REPO_ROOT)
            line_num = content[: m.start()].count("\n") + 1
            errors.append(
                f"{rel}:{line_num}: 禁止引用废弃属性 'context.upload_in_flight'，必须使用 'context.upload_in_flight_per_connection'"
            )

    # 2. 检查函数形参定义及 kwargs 读取中的 upload_in_flight
    kwargs_pattern = re.compile(r"\bkwargs(?:\.get\(\s*|\[\s*)['\"]upload_in_flight['\"]")
    for py_file in (REPO_ROOT / "app").rglob("*.py"):
        content = py_file.read_text(encoding="utf-8", errors="ignore")
        for m in kwargs_pattern.finditer(content):
            rel = py_file.relative_to(REPO_ROOT)
            line_num = content[: m.start()].count("\n") + 1
            errors.append(
                f"{rel}:{line_num}: 禁止通过 kwargs 读取废弃参数 'upload_in_flight'，必须使用 'upload_in_flight_per_connection'"
            )
        try:
            tree = ast.parse(content, filename=str(py_file))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                all_args = node.args.args + node.args.kwonlyargs
                for arg in all_args:
                    if arg.arg == "upload_in_flight":
                        rel = py_file.relative_to(REPO_ROOT)
                        errors.append(
                            f"{rel}:{node.lineno}: 函数 '{node.name}' 存在废弃形参 'upload_in_flight'，必须使用规范形参 'upload_in_flight_per_connection'"
                        )

    return errors


def check_test_awareness_absence() -> list[str]:
    """生产代码 (app/ + run.py) 禁止感知测试环境与幽灵属性。

    测试零等待的唯一合法通道 = PublishContext.breathing 注入 (tests/conftest.py
    autouse fixture 注入零等待策略, 专项测试显式覆盖)。
    """
    errors: list[str] = []
    forbidden: list[tuple[str, str]] = [
        ("PYTEST_CURRENT_TEST", "禁止读取测试环境变量 (测试零等待经 conftest 注入 BreathingPolicy 承载)"),
        ("_testing_fast_breath", "禁止引用已废除幽灵属性 '_testing_fast_breath'"),
        ("_min_breathing_floor", "禁止引用已废除幽灵属性 '_min_breathing_floor'"),
        (
            'getattr(context, "send_delay_seconds"',
            "禁止以 getattr 读取幽灵属性 'send_delay_seconds' (生产链路字段 = context.delay_seconds)",
        ),
    ]
    targets = [*(REPO_ROOT / "app").rglob("*.py"), REPO_ROOT / "run.py"]
    for py_file in targets:
        content = py_file.read_text(encoding="utf-8", errors="ignore")
        for snippet, reason in forbidden:
            idx = content.find(snippet)
            if idx != -1:
                rel = py_file.relative_to(REPO_ROOT)
                line_num = content[:idx].count("\n") + 1
                errors.append(f"{rel}:{line_num}: {reason}")
    return errors


def check_layering_boundaries() -> list[str]:
    """检查严格单向分层架构边界: app/api -> app/services -> app/core -> app/db。
    1. app/db/ 严禁 import app.core / app.services / app.api;
    2. app/core/ 严禁 import app.services / app.api;
    3. app/services/ 严禁 import app.api;
    4. app/api/ 严禁直接导入任何 app.db.*;
    5. app/api/、app/services/ 与 app/core/ 严禁直接从 app.db.database 导入 get_db / db_cursor / db_immediate_tx。
    """
    errors: list[str] = []
    layer_rules = [
        ("db", ("app.core", "app.services", "app.api")),
        ("core", ("app.services", "app.api")),
        ("services", ("app.api",)),
        ("api", ("app.db",)),
    ]
    for layer_dir, forbidden_prefixes in layer_rules:
        target_dir = REPO_ROOT / "app" / layer_dir
        if not target_dir.exists():
            continue
        for py_file in target_dir.rglob("*.py"):
            content = py_file.read_text(encoding="utf-8", errors="ignore")
            try:
                tree = ast.parse(content, filename=str(py_file))
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if any(alias.name == p or alias.name.startswith(p + ".") for p in forbidden_prefixes):
                            rel = py_file.relative_to(REPO_ROOT)
                            errors.append(f"{rel}:{node.lineno}: 分层违规 (`app/{layer_dir}` 禁止导入 `{alias.name}`)")
                elif isinstance(node, ast.ImportFrom):
                    module = node.module or ""
                    if node.level:
                        parts = py_file.relative_to(REPO_ROOT).parts[:-1]
                        if node.level > len(parts):
                            continue
                        base_pkg = ".".join(parts[: len(parts) - node.level + 1])
                        module = f"{base_pkg}.{module}" if module else base_pkg
                    imports = [module, *(f"{module}.{alias.name}" for alias in node.names)]
                    for imported in imports:
                        if any(imported == p or imported.startswith(p + ".") for p in forbidden_prefixes):
                            rel = py_file.relative_to(REPO_ROOT)
                            errors.append(f"{rel}:{node.lineno}: 分层违规 (`app/{layer_dir}` 禁止从 `{imported}` 导入)")
                            break

    # 4. 控制器层、服务层与核心层禁止直接导入底层连接与事务函数
    db_consumer_dirs = [
        ("api", REPO_ROOT / "app" / "api"),
        ("services", REPO_ROOT / "app" / "services"),
        ("core", REPO_ROOT / "app" / "core"),
    ]
    for layer_name, target_dir in db_consumer_dirs:
        if not target_dir.exists():
            continue
        for py_file in target_dir.rglob("*.py"):
            content = py_file.read_text(encoding="utf-8", errors="ignore")
            try:
                tree = ast.parse(content, filename=str(py_file))
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module == "app.db.database":
                    for alias in node.names:
                        if alias.name in ("get_db", "db_cursor", "db_immediate_tx"):
                            rel = py_file.relative_to(REPO_ROOT)
                            errors.append(
                                f"{rel}:{node.lineno}: 分层违规 (`app/{layer_name}` 禁止直接导入底层 `{alias.name}`，必须经 Repository 调用)"
                            )
    return errors


def build_arg_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog="lint_standards.py",
        description="TG-Media-Hub 架构规范、单位度量与命名契约静态守卫",
    )


def main(argv: list[str] | None = None) -> int:
    build_arg_parser().parse_args(argv)
    print("🔍 运行 TG-Media-Hub 架构规范与命名契约守卫检查...")
    errors = []
    errors.extend(check_docs_symbols())
    errors.extend(check_config_units())
    errors.extend(check_concurrency_contracts())
    errors.extend(check_configuration_doc_parity())
    errors.extend(check_upload_limit_precision())
    errors.extend(check_upload_in_flight_absence())
    errors.extend(check_test_awareness_absence())
    errors.extend(check_layering_boundaries())

    if errors:
        print(f"❌ 契约守卫检查失败，发现 {len(errors)} 项违规：")
        for err in errors:
            print(f"  - {err}")
        return 1

    print("✅ 契约守卫检查全部通过：单位、并发契约及技术文档均完全符合规范！")
    return 0


if __name__ == "__main__":
    sys.exit(main())
