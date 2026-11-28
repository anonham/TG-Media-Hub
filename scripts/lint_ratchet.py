#!/usr/bin/env python
"""ruff 违规计数棘轮门禁 (只减不增).

以规则码为单位, 比对当前违规计数与基线 (仓库根 lint_ratchet_baseline.json):
- 当前计数 > 基线 → 新增违规, 非零退出 (CI 阻塞);
- 当前计数 < 基线 → 允许通过 (清偿后用 --update 收紧基线, diff 入评审);
- 基线未登记的规则码按 0 处理 → 任何新规则码违规即回归。

分域计数: 生产域 (app/scripts/run.py) 与测试域 (tests) 互不阻塞, 可独立清偿。
本脚本自 起替代裸 `ruff check` 成为 CI 阻塞门禁 (存量违规由棘轮承载,
裸 check 无法在扩容规则集下保持绿); `ruff format --check` 仍为独立门禁不变。

用法:
    uv run python scripts/lint_ratchet.py            # 校验 (CI / 本地门禁)
    uv run python scripts/lint_ratchet.py --update   # 以当前计数重算基线 (diff 入评审)

ruff 版本升级或规则集调整可能使基线整体漂移: 重新 --update 并在 PR 中审阅 diff。
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BASELINE_PATH = PROJECT_ROOT / "lint_ratchet_baseline.json"

# 分域监管: 生产域与测试域分开计数, 清偿进度互不阻塞
DOMAINS: dict[str, list[str]] = {
    "app": ["app", "scripts", "run.py"],
    "tests": ["tests"],
}


def _extract_code(item: dict[str, Any]) -> str:
    code = item.get("code")
    if isinstance(code, dict):  # 兼容 ruff 新版嵌套形态 {"code": ..., "url": ...}
        return str(code.get("code", ""))
    return str(code or "")


def count_by_rule(domain: str) -> dict[str, int]:
    """运行 ruff JSON 输出并按规则码计数 (退出码 0/1 均为有效扫描结果)。"""
    cmd = [sys.executable, "-m", "ruff", "check", *DOMAINS[domain], "--output-format", "json"]
    proc = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if proc.returncode not in (0, 1):  # 0=无违规, 1=有违规; 其余为基础设施错误
        raise RuntimeError(f"ruff 异常退出 (code={proc.returncode}): {proc.stderr.strip()[:500]}")
    if proc.returncode == 1 and not proc.stdout.strip():
        raise RuntimeError(f"ruff 异常退出 (code=1 且无 JSON 输出): {proc.stderr.strip()[:500]}")
    try:
        diagnostics = json.loads(proc.stdout or "[]")
    except json.JSONDecodeError as err:
        raise RuntimeError(f"ruff JSON 解析失败: {err}") from err
    counts: dict[str, int] = {}
    for item in diagnostics:
        code = _extract_code(item)
        if code:
            counts[code] = counts.get(code, 0) + 1
    return dict(sorted(counts.items()))


def _render_counts(counts: dict[str, int]) -> str:
    return json.dumps(counts, indent=2, sort_keys=True) + "\n"


def _load_baseline(path: Path) -> dict[str, dict[str, int]]:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as err:
        raise RuntimeError(f"基线读取/解析失败 ({path}): {err}") from err
    if not isinstance(loaded, dict):
        raise RuntimeError(f"基线格式非法 ({path}): 顶层必须为 JSON 对象")
    for domain, counts in loaded.items():
        if not isinstance(counts, dict) or any(type(count) is not int or count < 0 for count in counts.values()):
            raise RuntimeError(f"基线格式非法 ({path}): {domain} 必须为非负整数计数对象")
    return loaded


def _update_baseline(current: dict[str, dict[str, int]]) -> int:
    old: dict[str, dict[str, int]] = {}
    if BASELINE_PATH.exists():
        old = _load_baseline(BASELINE_PATH)
    BASELINE_PATH.write_text(_render_counts(current), encoding="utf-8")
    for domain in DOMAINS:
        old_map = old.get(domain, {})
        cur_map = current[domain]
        for code in sorted(set(old_map) | set(cur_map)):
            before, after = old_map.get(code, 0), cur_map.get(code, 0)
            if before != after:
                print(f"  [{domain}] {code}: {before} -> {after}")
    print(f"✅ 基线已更新: {BASELINE_PATH}")
    return 0


def _verify_against_baseline(current: dict[str, dict[str, int]]) -> int:
    if not BASELINE_PATH.exists():
        print(f"❌ 基线不存在: {BASELINE_PATH} (先运行 --update 生成)")
        return 2
    baseline = _load_baseline(BASELINE_PATH)

    regressions = 0
    for domain in DOMAINS:
        base_map = baseline.get(domain, {})
        cur_map = current.get(domain, {})
        for code, count in cur_map.items():
            allowed = base_map.get(code, 0)
            if count > allowed:
                print(f"❌ [{domain}] {code}: 基线 {allowed} -> 当前 {count} (新增违规, 先清偿或评审后 --update)")
                regressions += 1
        summary = f"[{domain}] 当前 {sum(cur_map.values())} / 基线 {sum(base_map.values())}"
        tightenable = sum(1 for c, n in base_map.items() if cur_map.get(c, 0) < n)
        if tightenable:
            summary += f" (可收紧规则码 {tightenable} 个: 清偿后 --update)"
        print(summary)

    if regressions:
        print(f"\n❌ 棘轮门禁失败: {regressions} 个规则码出现新增违规")
        return 1
    print("\n✅ 棘轮门禁通过: 违规计数未超基线 (只减不增)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="ruff 违规计数棘轮门禁 (只减不增)")
    parser.add_argument("--update", action="store_true", help="以当前计数重算基线 (评审可见 diff)")
    args = parser.parse_args()

    try:
        current = {domain: count_by_rule(domain) for domain in DOMAINS}
        if args.update:
            return _update_baseline(current)
        return _verify_against_baseline(current)
    except RuntimeError as err:
        print(f"❌ {err}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
