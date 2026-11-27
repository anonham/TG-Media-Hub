"""basedpyright 报告摘要与 0-error 门禁裁决器。

读取 `basedpyright --outputjson` 产物, 按 严重级 × 规则码 聚合打印分布,
并作为阻断门禁严格裁决: 当报告缺失、损坏或存在类型错误时退出码为 1 (阻断);
仅在报告完整解析且 errorCount == 0 时退出码为 0 (放行)。

用法: uv run python scripts/pyright_report_summary.py [basedpyright_report.json]
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

DEFAULT_REPORT = Path("basedpyright_report.json")


def _load_report(report_path: Path) -> tuple[dict, dict]:
    try:
        doc = json.loads(report_path.read_text(encoding="utf-8"))
    except FileNotFoundError as err:
        raise ValueError(f"报告文件不存在: {report_path}") from err
    except json.JSONDecodeError as json_err:
        raise ValueError(f"报告解析失败: {json_err}") from json_err

    if not isinstance(doc, dict):
        raise ValueError("报告根节点必须为 JSON 对象")
    summary = doc.get("summary")
    if not isinstance(summary, dict) or "errorCount" not in summary:
        raise ValueError("报告内容残缺, 缺少 summary 或 errorCount 字段")
    if int(summary.get("filesAnalyzed") or 0) <= 0:
        raise ValueError("basedpyright 未分析任何文件 (filesAnalyzed == 0)，疑似工具执行异常")
    return doc, summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pyright_report_summary.py",
        description="basedpyright 报告摘要与 0-error 门禁裁决器",
    )
    parser.add_argument(
        "report_path",
        nargs="?",
        default=str(DEFAULT_REPORT),
        help=f"basedpyright JSON 报告路径 (默认: {DEFAULT_REPORT})",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    raw_args = argv[1:] if argv is not None else sys.argv[1:]
    args = build_arg_parser().parse_args(raw_args)
    report_path = Path(args.report_path)
    try:
        doc, summary = _load_report(report_path)
    except ValueError as err:
        print(f"❌ 门禁阻断: {err}")
        return 1

    diagnostics = doc.get("generalDiagnostics", [])
    by_rule: dict[str, Counter[str]] = {}
    for diag in diagnostics:
        severity = str(diag.get("severity") or "unknown")
        rule = str(diag.get("rule") or "<no-rule>")
        by_rule.setdefault(severity, Counter())[rule] += 1
    print(
        f"📊 basedpyright 报告摘要: {summary.get('errorCount', 0)} errors / "
        f"{summary.get('warningCount', 0)} warnings / {summary.get('informationCount', 0)} info "
        f"({summary.get('filesAnalyzed', 0)} files)"
    )
    for severity in sorted(by_rule):
        print(f"=== {severity} ===")
        for rule, count in by_rule[severity].most_common():
            print(f"{count:5d}  {rule}")

    error_count = int(summary.get("errorCount", 0))
    if error_count > 0:
        print(f"\n❌ basedpyright 错误阻断门禁失败: 发现 {error_count} 处类型错误！")
        return 1
    print("\n✅ basedpyright 错误阻断门禁通过: 0 处类型错误！")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
