"""SQLite task log append micro-benchmark (strict SLA verification).

Benchmarks `TaskQueueRepository.append_log` latency under a seeded 100k-row
`task_logs` table inside an isolated temporary database so no repository state
or root `*.db` files are touched.

Strict failures on shared CI hosts are advisory investigation signals, not merge
gates (the scheduled CI workflow always runs non-strict).

Usage:
    uv run python scripts/benchmarks/bench_sqlite_logs.py
    uv run python scripts/benchmarks/bench_sqlite_logs.py --strict
    uv run python scripts/benchmarks/bench_sqlite_logs.py --json
    uv run python scripts/benchmarks/bench_sqlite_logs.py --json-file bench_sqlite_logs.json
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

# 允许从任意工作目录直接运行：将仓库根加入模块搜索路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.config import settings
from app.db.database import close_all_thread_connections, get_db, init_db
from app.db.task_repository import TaskQueueRepository

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.platform == "win32" and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


@dataclass(slots=True)
class SqliteLogBenchResult:
    """SQLite append_log micro-benchmark summary."""

    name: str
    seeded_rows: int
    samples: int
    min_ms: float
    avg_ms: float
    median_ms: float
    p95_ms: float
    max_ms: float
    sla_min_ms: float
    sla_passed: bool


def _seed_logs(task_id: str, count: int) -> None:
    conn = get_db()
    conn.executemany(
        "INSERT INTO task_logs (task_id, seq, time, level, message, created_at) VALUES (?, ?, '12:00:00', 'info', ?, 0)",
        [(task_id, i, f"chunk {i % 256}/256 speed=8.2MB/s dc=2") for i in range(1, count + 1)],
    )
    conn.commit()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()


def run_append_log_benchmark(*, seeded_rows: int = 100_000, samples: int = 100) -> SqliteLogBenchResult:
    """Measure single-row TaskQueueRepository.append_log latency over seeded task_logs.

    The SLA gates on min — the robust lower-bound statistic on noisy hosts (timeit
    methodology); avg/median/p95 are reported for diagnostics but intentionally
    not gated, since scheduler jitter inflates them without reflecting code cost.
    """
    orig_data_dir = settings.DATA_DIR
    orig_db_path = settings.DB_PATH
    with tempfile.TemporaryDirectory(prefix="tg_bench_sqlite_") as tmp_dir:
        close_all_thread_connections()
        settings.DATA_DIR = tmp_dir
        settings.DB_PATH = os.path.join(tmp_dir, "bench.db")
        try:
            init_db()
            task_id = "bench_append_log"
            TaskQueueRepository.create(task_id, "download_range", {"channel": "c", "start_id": 1, "end_id": 2})
            _seed_logs(task_id, seeded_rows)
            TaskQueueRepository.append_log(task_id, "warmup")

            durations_ms: list[float] = []
            for i in range(samples):
                t0 = time.perf_counter()
                TaskQueueRepository.append_log(task_id, f"perf {i}")
                durations_ms.append((time.perf_counter() - t0) * 1000.0)

            logs, last_seq, _ = TaskQueueRepository.get_task_logs(task_id, after_seq=seeded_rows)
            if last_seq != seeded_rows + 1 + samples or len(logs) != samples + 1:
                raise AssertionError(f"Unexpected log sequence state: last_seq={last_seq}, len={len(logs)}")

            ordered = sorted(durations_ms)
            p95_idx = min(len(ordered) - 1, max(0, int(len(ordered) * 0.95) - 1))
            avg_ms = sum(durations_ms) / len(durations_ms)
            sla_min_ms = 1.0
            return SqliteLogBenchResult(
                name="sqlite_append_log_100k_rows",
                seeded_rows=seeded_rows,
                samples=samples,
                min_ms=ordered[0],
                avg_ms=avg_ms,
                median_ms=statistics.median(ordered),
                p95_ms=ordered[p95_idx],
                max_ms=ordered[-1],
                sla_min_ms=sla_min_ms,
                sla_passed=ordered[0] < sla_min_ms,
            )
        finally:
            close_all_thread_connections()
            settings.DATA_DIR = orig_data_dir
            settings.DB_PATH = orig_db_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SQLite append_log strict micro-benchmark runner")
    parser.add_argument("--seeded-rows", type=int, default=100_000, help="Pre-seeded historical log rows")
    parser.add_argument("--samples", type=int, default=100, help="Number of append_log samples")
    parser.add_argument("--json", action="store_true", help="Print JSON summary")
    parser.add_argument("--json-file", default=None, help="Write JSON summary to this file (table still printed)")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if SLA fails")
    args = parser.parse_args(argv)

    result = run_append_log_benchmark(seeded_rows=args.seeded_rows, samples=args.samples)
    if args.json_file:
        Path(args.json_file).write_text(json.dumps([asdict(result)], indent=2), encoding="utf-8")
    if args.json:
        print(json.dumps([asdict(result)], indent=2))
    else:
        status = "PASS" if result.sla_passed else "FAIL"
        print("SQLite Log Micro-Benchmark Results (isolated TemporaryDirectory):")
        print(
            f"  [{status}] {result.name}: avg={result.avg_ms:.3f}ms, min={result.min_ms:.3f}ms, "
            f"median={result.median_ms:.3f}ms, p95={result.p95_ms:.3f}ms, max={result.max_ms:.3f}ms "
            f"(SLA min < {result.sla_min_ms:.1f}ms)"
        )

    if args.strict and not result.sla_passed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
