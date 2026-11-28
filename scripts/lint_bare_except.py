"""
scripts/lint_bare_except.py
TG-Media-Hub 裸吞异常 (Bare Exception Suppression) AST 检查门禁脚本

检查规则：
1. app/ 目录与 run.py 中所有 except 语句块禁止仅包含单个 `pass` 或 `...` (Ellipsis)
2. 异常处理应包含上下文日志 (如 logger.debug / logger.warning / logger.error) 或明确处理逻辑；合理的单条控制流早退 (`return` / `continue` / `break`) 不视为裸吞异常
3. 退出码：存在违规返回 1，发生读取或 AST 解析错误返回 2，全部合规返回 0
"""

import argparse
import ast
import os
import sys

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

TARGET_PATHS = ["app", "run.py"]
IGNORE_DIRS = {".conda", ".git", ".pytest_cache", "__pycache__", "dist", "build", "node_modules", ".venv-review"}


def is_bare_suppression(handler: ast.ExceptHandler) -> bool:
    """判定是否为无日志、无处理的裸吞异常"""
    if len(handler.body) == 1:
        stmt = handler.body[0]
        # 匹配 pass
        if isinstance(stmt, ast.Pass):
            return True
        # 匹配 ... (Ellipsis)
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant) and stmt.value.value is ...:
            return True
    return False


def scan_file(file_path: str, parse_errors: list[tuple[str, Exception]] | None = None):
    violations = []
    try:
        with open(file_path, encoding="utf-8") as f:
            content = f.read()
        tree = ast.parse(content, filename=file_path)
    except (OSError, SyntaxError, ValueError) as e:
        print(f"Error parsing {file_path}: {e}", file=sys.stderr)
        if parse_errors is not None:
            parse_errors.append((file_path, e))
        return violations

    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler):
            if is_bare_suppression(node):
                exc_type = ast.unparse(node.type) if node.type else "<bare except>"
                violations.append((file_path, node.lineno, exc_type))
    return violations


def build_arg_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog="lint_bare_except.py",
        description="扫描 app/ 与 run.py 中的裸吞异常 (except ...: pass) 违规",
    )


def main(argv: list[str] | None = None):
    build_arg_parser().parse_args(argv)
    all_violations = []
    parse_errors: list[tuple[str, Exception]] = []
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    for target in TARGET_PATHS:
        full_target = os.path.join(base_dir, target)
        if os.path.isfile(full_target):
            all_violations.extend(scan_file(full_target, parse_errors=parse_errors))
        elif os.path.isdir(full_target):
            for root, dirs, files in os.walk(full_target):
                dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
                for file in files:
                    if file.endswith(".py"):
                        p = os.path.join(root, file)
                        all_violations.extend(scan_file(p, parse_errors=parse_errors))

    print("==================================================")
    print("AST 裸吞异常扫描报告 (Target: app/, run.py)")
    print("==================================================")
    if parse_errors:
        print(f"❌ 扫描失败：{len(parse_errors)} 个文件读取或 AST 解析异常！", file=sys.stderr)
        for file_path, err in parse_errors:
            rel_path = os.path.relpath(file_path, base_dir)
            print(f"  - {rel_path}: {err}", file=sys.stderr)
        sys.exit(2)
    if all_violations:
        print(f"❌ 发现 {len(all_violations)} 处裸吞异常违规：\n")
        for file_path, lineno, exc_type in sorted(all_violations, key=lambda x: (x[0], x[1])):
            rel_path = os.path.relpath(file_path, base_dir)
            print(f"  - {rel_path}:{lineno} -> except {exc_type}: pass")
        print("\n治理要求：必须补齐分级日志 (logger.debug/logger.warning) 或精准重构。")
        sys.exit(1)
    else:
        print("✅ 扫描通过：全库 0 处裸吞异常，质量门禁 100% 达标！")
        sys.exit(0)


if __name__ == "__main__":
    main()
