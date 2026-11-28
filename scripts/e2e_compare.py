#!/usr/bin/env python3
"""
scripts/e2e_compare.py — E2E 捕获证据离线对比 CLI (计划 D5, 断言 A1-A5)

用法:
    uv run python scripts/e2e_compare.py --manifest <manifest.json 路径>
    uv run python scripts/e2e_compare.py --task <task_id>            # 扫描
        settings.E2E_CAPTURE_DIR/<task_id>/*/manifest.json 逐个比对

输出: 各 manifest 同目录 compare_report.md (人读) + compare_report.json (机读);
退出码: 0 = 无 FAIL 级断言 (SKIP 降级不算失败), 1 = 存在 FAIL, 2 = 用法/IO 错误。
断言语义与降级矩阵详见 docs/testing/E2E_GUIDE.md §5.1。
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings
from app.core.e2e_compare import run_compare


def _collect_manifest_paths(args) -> list:
    if args.manifest:
        return [args.manifest]
    root = os.path.join(settings.E2E_CAPTURE_DIR, str(args.task))
    if not os.path.isdir(root):
        print(f"[e2e-compare] 任务目录不存在: {root}")
        sys.exit(2)
    found = []
    for entry in sorted(os.listdir(root)):
        m = os.path.join(root, entry, "manifest.json")
        if os.path.isfile(m):
            found.append(m)
    if not found:
        print(f"[e2e-compare] 任务目录下无 manifest: {root}")
        sys.exit(2)
    return found


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="E2E 捕获证据离线对比 (A1-A5)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--manifest", help="单 item manifest.json 路径")
    group.add_argument("--task", help="task_id (扫描 E2E_CAPTURE_DIR/<task_id>/*/manifest.json)")
    args = parser.parse_args()

    any_fail = False
    any_io_error = False
    for manifest_path in _collect_manifest_paths(args):
        print(f"\n[e2e-compare] 比对: {manifest_path}")
        try:
            _manifest, report, ok = run_compare(manifest_path)
        except (OSError, ValueError, KeyError) as e:
            print(f"  ❌ manifest 读取/解析异常: {e}")
            any_io_error = True
            continue
        for r in report.results:
            mark = {"PASS": "✅", "FAIL": "❌", "SKIP": "⏭️"}.get(r.status, "?")
            print(f"  {mark} {r.status:<4} {r.name:<28} {r.detail}")
        verdict = "FAIL" if not ok else "PASS (含 SKIP 降级项)"
        print(f"  判定: {verdict}  (报告: {os.path.join(os.path.dirname(manifest_path), 'compare_report.md')})")
        any_fail = any_fail or not ok

    if any_io_error:
        sys.exit(2)
    sys.exit(1 if any_fail else 0)


if __name__ == "__main__":
    main()
