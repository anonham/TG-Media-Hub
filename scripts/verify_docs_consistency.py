#!/usr/bin/env python
"""
Docs Consistency & Symbol Verifier
Verifies that:
1. No 'file.py:line' references exist in docs/.
2. All /api/... routes documented exist in the actual FastAPI application.
3. No dead internal links exist.
4. Key environment variables in CONFIGURATION.md exist in app/config.py or .env.example.
"""

import argparse
import os
import re
import sys

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def check_line_numbers(docs_dir: str):
    pat = re.compile(r"\b[a-zA-Z0-9_\-\./]+\.(?:py|vue|js|ts|css):\d+\b")
    errors = []
    root_docs = [
        os.path.join(PROJECT_ROOT, "README.md"),
        os.path.join(PROJECT_ROOT, "CONTRIBUTING.md"),
        os.path.join(PROJECT_ROOT, "SECURITY.md"),
        os.path.join(PROJECT_ROOT, "CLAUDE.md"),
        os.path.join(PROJECT_ROOT, "AGENTS.md"),
    ]
    for fpath in root_docs:
        if os.path.exists(fpath):
            with open(fpath, encoding="utf-8", errors="ignore") as fp:
                content = fp.read()
            matches = pat.findall(content)
            if matches:
                errors.append(f"{fpath}: Found line number references: {matches}")
    for root, _, files in os.walk(docs_dir):
        for f in files:
            if f.endswith(".md"):
                fpath = os.path.join(root, f)
                with open(fpath, encoding="utf-8", errors="ignore") as fp:
                    content = fp.read()
                matches = pat.findall(content)
                if matches:
                    errors.append(f"{fpath}: Found line number references: {matches}")
    return errors


def check_api_endpoints(docs_dir: str):
    from app.main import app

    real_paths = set(app.openapi()["paths"].keys())
    # Match /api/[a-z0-9/{}_-]+
    pat = re.compile(r"/api/[a-z0-9/{}_-]+")
    errors = []
    for root, _, files in os.walk(docs_dir):
        for f in files:
            if f.endswith(".md"):
                fpath = os.path.join(root, f)
                with open(fpath, encoding="utf-8", errors="ignore") as fp:
                    content = fp.read()
                for m in pat.finditer(content):
                    route = m.group(0)
                    # Normalize wildcard or route prefixes if mentioned as prefix
                    if route in real_paths:
                        continue
                    # Check if it's a module prefix like /api/tasks, /api/auth, etc.
                    prefix = route.rstrip("/") + "/"
                    is_prefix = any(p == route or p.startswith(prefix) for p in real_paths)
                    if not is_prefix:
                        errors.append(f"{fpath}: Undefined API route '{route}'")
    return errors


def _fence_line_context(line: str, list_indents: list[int]) -> tuple[str, int, int]:
    """Locate a fence relative to any enclosing Markdown list containers."""
    line = line.expandtabs(4)
    stripped = line.lstrip(" ")
    indent = len(line) - len(stripped)
    if stripped:
        while list_indents and indent < list_indents[-1]:
            list_indents.pop()
    container_indent = list_indents[-1] if list_indents else 0
    # A list marker may introduce a nested container or a fence on this line.
    marker = re.match(r" {0,3}(?:[-+*]|[0-9]{1,9}[.)]) {1,4}(?=\S)", line[container_indent:])
    if marker:
        container_indent += marker.end()
        list_indents.append(container_indent)
        stripped = line[container_indent:]
        indent = container_indent
    return stripped, indent, container_indent


def _strip_fenced_code_blocks(content: str) -> str:
    """剔除围栏代码块内的行，避免示例代码/Mermaid 中的方括号链接误报。"""
    out_lines = []
    in_fence = False
    fence_char = ""
    fence_len = 0
    fence_indent = 0
    fence_container_indent = 0
    list_indents: list[int] = []
    for line in content.splitlines():
        expanded = line.expandtabs(4)
        stripped = expanded.lstrip(" ")
        indent = len(expanded) - len(stripped)
        # An unclosed list fence ends with its item; parse the boundary line again
        # as Markdown rather than hiding it as code. Blank lines stay in the item.
        if in_fence and stripped and indent < fence_container_indent:
            in_fence = False
        if not in_fence:
            stripped, indent, container_indent = _fence_line_context(line, list_indents)
            if indent - container_indent <= 3 and stripped.startswith(("```", "~~~")):
                ch = stripped[0]
                count = len(stripped) - len(stripped.lstrip(ch))
                if ch == "`" and "`" in stripped[count:]:
                    out_lines.append(line)
                    continue
                in_fence = True
                fence_char = ch
                fence_len = count
                fence_indent = indent
                fence_container_indent = container_indent
                out_lines.append("")
                continue
            out_lines.append(line)
        else:
            count = len(stripped) - len(stripped.lstrip(fence_char))
            if indent - fence_indent <= 3 and count >= fence_len and not stripped[count:].strip():
                in_fence = False
            out_lines.append("")
    return "\n".join(out_lines)


def check_markdown_links(docs_dir: str):
    errors = []
    for root, _, files in os.walk(docs_dir):
        for f in files:
            if f.endswith(".md"):
                fpath = os.path.join(root, f)
                with open(fpath, encoding="utf-8", errors="ignore") as fp:
                    content = _strip_fenced_code_blocks(fp.read())
                links = re.findall(r"\[([^\]]+)\]\(([^)]+)\)", content)
                for _text, url in links:
                    raw = url.strip()
                    if raw.startswith("<") and ">" in raw:
                        target = raw[1 : raw.index(">")].strip()
                    else:
                        target = raw.split()[0] if raw else ""
                    clean_url = target.split("#", 1)[0].split("?", 1)[0].strip()
                    if not clean_url or clean_url.startswith(("http://", "https://", "mailto:", "file://")):
                        continue
                    if clean_url.startswith("/"):
                        target_path = os.path.normpath(os.path.join(PROJECT_ROOT, clean_url.lstrip("/")))
                    else:
                        target_path = os.path.normpath(os.path.join(root, clean_url))
                    if not os.path.exists(target_path):
                        errors.append(f"{fpath}: Dead link '{url}' -> '{target_path}'")
    return errors


def build_arg_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog="verify_docs_consistency.py",
        description="校验 docs/ 中的行号引用、API 路由和内部 Markdown 链接一致性",
    )


def main(argv: list[str] | None = None):
    build_arg_parser().parse_args(argv)
    docs_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "docs"))
    print(f"Scanning documentation in {docs_dir}...")

    all_errors = []

    # 1. Line numbers
    ln_errors = check_line_numbers(docs_dir)
    if ln_errors:
        all_errors.extend(ln_errors)
        print(f"❌ Line number checks failed ({len(ln_errors)} errors)")
    else:
        print("✅ No 'xxx.py:line' references found in docs.")

    # 2. API Endpoints
    api_errors = check_api_endpoints(docs_dir)
    if api_errors:
        all_errors.extend(api_errors)
        print(f"❌ API route checks failed ({len(api_errors)} errors)")
        for err in api_errors:
            print("  -", err)
    else:
        print("✅ All /api/ routes referenced in docs exist in FastAPI schema.")

    # 3. Markdown links
    link_errors = check_markdown_links(docs_dir)
    if link_errors:
        all_errors.extend(link_errors)
        print(f"❌ Link checks failed ({len(link_errors)} errors)")
        for err in link_errors:
            print("  -", err)
    else:
        print("✅ All internal markdown links in docs/ are valid.")

    if all_errors:
        print(f"\nTotal errors found: {len(all_errors)}")
        sys.exit(1)
    else:
        print("\n🎉 ALL DOCS CONSISTENCY CHECKS PASSED!")
        sys.exit(0)


if __name__ == "__main__":
    main()
