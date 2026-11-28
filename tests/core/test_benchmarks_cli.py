"""Smoke tests for standalone micro-benchmark scripts under scripts/benchmarks/."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def _load_module(rel_path: str, module_name: str):
    target = REPO_ROOT / rel_path
    spec = importlib.util.spec_from_file_location(module_name, target)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_bench_iso_bmff_cli_json(capsys):
    """Verify bench_iso_bmff.py --json emits valid schema without requiring coverage/xdist."""
    mod = _load_module("scripts/benchmarks/bench_iso_bmff.py", "bench_iso_bmff_mod")
    rc = mod.main(["--suite", "uniform_stsz", "--warmup", "1", "--iterations", "2", "--json"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert len(payload) == 1
    entry = payload[0]
    assert entry["name"] == "uniform_stsz_350m"
    assert entry["iterations"] == 2
    assert entry["warmup"] == 1
    assert entry["min_ms"] >= 0.0
    assert entry["object_bytes"] < 1024
    assert entry["peak_bytes"] < 10240


def test_bench_sqlite_logs_cli_json(capsys):
    """Verify bench_sqlite_logs.py --json runs in an isolated temp DB and restores settings."""
    from app.config import settings

    saved_data_dir = settings.DATA_DIR
    saved_db_path = settings.DB_PATH
    mod = _load_module("scripts/benchmarks/bench_sqlite_logs.py", "bench_sqlite_logs_mod")
    rc = mod.main(["--seeded-rows", "100", "--samples", "5", "--json"])
    assert rc == 0
    assert saved_data_dir == settings.DATA_DIR
    assert saved_db_path == settings.DB_PATH

    payload = json.loads(capsys.readouterr().out)
    assert len(payload) == 1
    entry = payload[0]
    assert entry["name"] == "sqlite_append_log_100k_rows"
    assert entry["seeded_rows"] == 100
    assert entry["samples"] == 5
    assert entry["avg_ms"] >= 0.0
    assert entry["min_ms"] >= 0.0
    assert entry["sla_min_ms"] == 1.0


def test_bench_iso_bmff_cli_json_file(capsys, tmp_path):
    """Verify --json-file writes parseable JSON while stdout keeps the human table."""
    mod = _load_module("scripts/benchmarks/bench_iso_bmff.py", "bench_iso_bmff_mod")
    out_file = tmp_path / "bench_iso_bmff.json"
    rc = mod.main(["--suite", "uniform_stsz", "--warmup", "1", "--iterations", "2", "--json-file", str(out_file)])
    assert rc == 0
    stdout = capsys.readouterr().out
    assert "ISO-BMFF Micro-Benchmark Results" in stdout, "表格输出必须保留在 stdout"

    entry = json.loads(out_file.read_text(encoding="utf-8"))[0]
    assert entry["name"] == "uniform_stsz_350m"
    assert entry["iterations"] == 2
    assert entry["min_ms"] >= 0.0


def test_bench_sqlite_logs_cli_json_file(capsys, tmp_path):
    """Verify --json-file writes parseable JSON in an isolated temp DB and restores settings."""
    from app.config import settings

    saved_data_dir = settings.DATA_DIR
    saved_db_path = settings.DB_PATH
    mod = _load_module("scripts/benchmarks/bench_sqlite_logs.py", "bench_sqlite_logs_mod")
    out_file = tmp_path / "bench_sqlite_logs.json"
    rc = mod.main(["--seeded-rows", "100", "--samples", "5", "--json-file", str(out_file)])
    assert rc == 0
    assert saved_data_dir == settings.DATA_DIR
    assert saved_db_path == settings.DB_PATH
    stdout = capsys.readouterr().out
    assert "SQLite Log Micro-Benchmark Results" in stdout, "表格输出必须保留在 stdout"

    entry = json.loads(out_file.read_text(encoding="utf-8"))[0]
    assert entry["name"] == "sqlite_append_log_100k_rows"
    assert entry["samples"] == 5
    assert entry["sla_min_ms"] == 1.0


def test_bench_vsse_pipeline_cli_json_and_file(capsys, tmp_path):
    """Verify bench_vsse_pipeline.py supports both --json stdout and --json-file output."""
    mod = _load_module("scripts/benchmarks/bench_vsse_pipeline.py", "bench_vsse_pipeline_mod")
    rc = mod.main(["--suite", "ttfb_large", "--warmup", "1", "--iterations", "2", "--json"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert len(payload) == 1
    assert payload[0]["name"] == "vsse_ttfb_large_envelope"
    assert payload[0]["min_ms"] >= 0.0

    out_file = tmp_path / "bench_vsse_pipeline.json"
    rc = mod.main(["--suite", "streaming_memory", "--warmup", "1", "--iterations", "1", "--json-file", str(out_file)])
    assert rc == 0
    stdout = capsys.readouterr().out
    assert "VSSE Pipeline Micro-Benchmark Results" in stdout
    file_entry = json.loads(out_file.read_text(encoding="utf-8"))[0]
    assert file_entry["name"] == "vsse_streaming_50mib_o1"
    assert file_entry["sla_peak_mib"] == 5.0


@pytest.mark.parametrize(
    ("totals", "volumes", "expected"),
    [([80.0], [60.0], False), ([80.0], [50.0], False), ([80.0], [40.0], True), ([80.0, 110.0], [60.0, 40.0], False)],
)
async def test_multi_volume_sla_requires_both_limits_in_same_run(monkeypatch, totals, volumes, expected):
    mod = _load_module("scripts/benchmarks/bench_vsse_pipeline.py", "bench_vsse_sla_mod")

    async def fake_peak(fn):
        return 1.0

    async def fake_latencies(fn, *, warmup, iterations, results):
        results.extend(volumes)
        return totals

    monkeypatch.setattr(mod, "_measure_async_peak_mib", fake_peak)
    monkeypatch.setattr(mod, "_measure_async_latencies", fake_latencies)
    result = await mod.run_ttfb_multi_volume_benchmark(warmup=1, iterations=len(totals))
    assert result.sla_passed is expected


async def test_latency_measurement_retains_only_measured_results():
    mod = _load_module("scripts/benchmarks/bench_vsse_pipeline.py", "bench_vsse_measure_mod")
    values = iter([99.0, 10.0, 20.0])

    async def run_once():
        return next(values)

    results = []
    samples = await mod._measure_async_latencies(run_once, warmup=1, iterations=2, results=results)
    assert len(samples) == 2
    assert results == [10.0, 20.0]
