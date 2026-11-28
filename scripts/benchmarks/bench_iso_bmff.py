"""ISO-BMFF box parser micro-benchmarks (strict SLA verification).

Runs outside pytest coverage/xdist instrumentation and decouples tracemalloc
memory profiling from latency timing so wall-clock measurements reflect real
CPU cost without trace-hook distortion.

Strict failures on shared CI hosts are advisory investigation signals, not merge
gates (the scheduled CI workflow always runs non-strict).

Usage:
    uv run python scripts/benchmarks/bench_iso_bmff.py
    uv run python scripts/benchmarks/bench_iso_bmff.py --strict
    uv run python scripts/benchmarks/bench_iso_bmff.py --json
    uv run python scripts/benchmarks/bench_iso_bmff.py --json-file bench_iso_bmff.json
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import statistics
import struct
import sys
import time
import tracemalloc
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path

# 允许从任意工作目录直接运行：将仓库根加入模块搜索路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.core.iso_bmff import UniformStszTable, parse_moov
from app.core.iso_bmff.box_parser import CompactCttsTable

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.platform == "win32" and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


@dataclass(slots=True)
class IsoBmffBenchResult:
    """Single ISO-BMFF benchmark measurement summary."""

    name: str
    iterations: int
    warmup: int
    min_ms: float
    median_ms: float
    p95_ms: float
    max_ms: float
    sla_ms: float
    peak_bytes: int
    max_peak_bytes: int
    object_bytes: int | None
    sla_passed: bool


def _make_box(box_type: bytes, payload: bytes) -> bytes:
    return struct.pack(">I4s", len(payload) + 8, box_type) + payload


def _make_full_box(box_type: bytes, version: int, flags: int, payload: bytes) -> bytes:
    vf = (version << 24) | (flags & 0x00FFFFFF)
    return struct.pack(">I4sI", len(payload) + 12, box_type, vf) + payload


def _build_avc1_stsd_box() -> bytes:
    entry_payload = (
        struct.pack(">6sH", b"\x00" * 6, 1)
        + struct.pack(">HH", 0, 0)
        + struct.pack(">III", 0, 0, 0)
        + struct.pack(">HH", 1920, 1080)
        + struct.pack(">II", 0x00480000, 0x00480000)
        + struct.pack(">IH", 0, 1)
        + b"\x00" * 32
        + struct.pack(">hh", 24, -1)
    )
    entry_box = struct.pack(">I4s", len(entry_payload) + 8, b"avc1") + entry_payload
    return _make_full_box(b"stsd", 0, 0, struct.pack(">I", 1) + entry_box)


def _wrap_stbl_in_moov(stbl_bytes: bytes, *, duration: int = 10000, timescale: int = 1000) -> bytes:
    tkhd_payload = (
        struct.pack(">IIII", 0, 0, 1, 0)
        + struct.pack(">I", duration)
        + struct.pack(">8s", b"\x00" * 8)
        + struct.pack(">hhh", 0, 0, 0)
        + struct.pack(">2s", b"\x00" * 2)
        + struct.pack(">9i", 0x00010000, 0, 0, 0, 0x00010000, 0, 0, 0, 0x40000000)
        + struct.pack(">II", 1920 << 16, 1080 << 16)
    )
    tkhd_box = _make_full_box(b"tkhd", 0, 1, tkhd_payload)
    mdhd_box = _make_full_box(b"mdhd", 0, 0, struct.pack(">IIIIH2s", 0, 0, timescale, duration, 0x55C4, b"\x00\x00"))
    hdlr_box = _make_full_box(b"hdlr", 0, 0, struct.pack(">I4s12s", 0, b"vide", b"\x00" * 12) + b"TrackHandler\x00")
    minf_box = _make_box(b"minf", stbl_bytes)
    mdia_box = _make_box(b"mdia", mdhd_box + hdlr_box + minf_box)
    trak_box = _make_box(b"trak", tkhd_box + mdia_box)

    mvhd_payload = (
        struct.pack(">IIII", 0, 0, timescale, duration)
        + struct.pack(">IH", 0x00010000, 0x0100)
        + struct.pack(">10s", b"\x00" * 10)
        + struct.pack(">9i", 0x00010000, 0, 0, 0, 0x00010000, 0, 0, 0, 0x40000000)
        + struct.pack(">24s", b"\x00" * 24)
        + struct.pack(">I", 3)
    )
    mvhd_box = _make_full_box(b"mvhd", 0, 0, mvhd_payload)
    return _make_box(b"moov", mvhd_box + trak_box)


def _build_uniform_stsz_350m_moov(sample_count: int = 350_000_000, uniform_size: int = 4) -> bytes:
    stsd_box = _build_avc1_stsd_box()
    stts_box = _make_full_box(b"stts", 0, 0, struct.pack(">III", 1, sample_count, 1000))
    stsc_box = _make_full_box(b"stsc", 0, 0, struct.pack(">IIII", 1, 10, 1, 1))
    stsz_box = _make_full_box(b"stsz", 0, 0, struct.pack(">II", uniform_size, sample_count))
    co_box = _make_full_box(b"stco", 0, 0, struct.pack(">II", 1, 10000))
    stbl = _make_box(b"stbl", stsd_box + stts_box + stsc_box + stsz_box + co_box)
    return _wrap_stbl_in_moov(stbl)


def _build_compact_ctts_moov(entry_count: int = 648_000) -> bytes:
    ctts_payload = struct.pack(">I", entry_count) + (struct.pack(">Ii", 1, 1024) + struct.pack(">Ii", 1, -512)) * (
        entry_count // 2
    )
    ctts_box = _make_full_box(b"ctts", 1, 0, ctts_payload)
    stsd_box = _build_avc1_stsd_box()
    stts_box = _make_full_box(b"stts", 0, 0, struct.pack(">III", 1, entry_count, 1000))
    stsc_box = _make_full_box(b"stsc", 0, 0, struct.pack(">IIII", 1, 10, 1, 1))
    stsz_box = _make_full_box(b"stsz", 0, 0, struct.pack(">II", 1000, entry_count))
    co_box = _make_full_box(b"stco", 0, 0, struct.pack(">II", 1, 10000))
    stbl = _make_box(b"stbl", stsd_box + stts_box + ctts_box + stsc_box + stsz_box + co_box)
    return _wrap_stbl_in_moov(stbl)


def _measure_peak_memory(fn: Callable[[], object]) -> int:
    fn()  # Warm up lazy module/type caches before tracing allocations
    gc.collect()
    tracemalloc.start()
    try:
        fn()
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return peak_bytes


def _measure_latency_ms(fn: Callable[[], object], *, warmup: int, iterations: int) -> dict[str, float]:
    for _ in range(warmup):
        fn()
    samples_ms: list[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        samples_ms.append((time.perf_counter() - t0) * 1000.0)
    ordered = sorted(samples_ms)
    p95_idx = min(len(ordered) - 1, max(0, int(len(ordered) * 0.95) - 1))
    return {
        "min_ms": ordered[0],
        "median_ms": statistics.median(ordered),
        "p95_ms": ordered[p95_idx],
        "max_ms": ordered[-1],
    }


def run_uniform_stsz_benchmark(
    *, warmup: int = 3, iterations: int = 20, sample_count: int = 350_000_000
) -> IsoBmffBenchResult:
    """Benchmark UniformStszTable parsing for 350M samples (SLA: min < 1.0ms, peak < 10KiB)."""
    moov_bytes = _build_uniform_stsz_350m_moov(sample_count=sample_count)
    peak_bytes = _measure_peak_memory(lambda: parse_moov(moov_bytes))
    meta = parse_moov(moov_bytes)
    v_track = meta.video_track
    if v_track is None or not isinstance(v_track.stbl.stsz, UniformStszTable):
        raise AssertionError("Expected UniformStszTable on video track")
    object_bytes = sys.getsizeof(v_track.stbl.stsz)

    stats = _measure_latency_ms(lambda: parse_moov(moov_bytes), warmup=warmup, iterations=iterations)
    sla_ms = 1.0
    max_peak_bytes = 10 * 1024
    sla_passed = stats["min_ms"] < sla_ms and peak_bytes < max_peak_bytes and object_bytes < 1024
    return IsoBmffBenchResult(
        name="uniform_stsz_350m",
        iterations=iterations,
        warmup=warmup,
        min_ms=stats["min_ms"],
        median_ms=stats["median_ms"],
        p95_ms=stats["p95_ms"],
        max_ms=stats["max_ms"],
        sla_ms=sla_ms,
        peak_bytes=peak_bytes,
        max_peak_bytes=max_peak_bytes,
        object_bytes=object_bytes,
        sla_passed=sla_passed,
    )


def run_compact_ctts_benchmark(
    *, warmup: int = 1, iterations: int = 5, entry_count: int = 648_000
) -> IsoBmffBenchResult:
    """Benchmark CompactCttsTable parsing for 3h 60fps video (SLA: min < 100.0ms, peak < 15MiB)."""
    moov_bytes = _build_compact_ctts_moov(entry_count=entry_count)
    peak_bytes = _measure_peak_memory(lambda: parse_moov(moov_bytes))
    meta = parse_moov(moov_bytes)
    v_track = meta.video_track
    if v_track is None or not isinstance(v_track.stbl.ctts, CompactCttsTable):
        raise AssertionError("Expected CompactCttsTable on video track")

    stats = _measure_latency_ms(lambda: parse_moov(moov_bytes), warmup=warmup, iterations=iterations)
    sla_ms = 100.0
    max_peak_bytes = 15 * 1024 * 1024
    sla_passed = stats["min_ms"] < sla_ms and peak_bytes < max_peak_bytes
    return IsoBmffBenchResult(
        name="compact_ctts_3h_60fps",
        iterations=iterations,
        warmup=warmup,
        min_ms=stats["min_ms"],
        median_ms=stats["median_ms"],
        p95_ms=stats["p95_ms"],
        max_ms=stats["max_ms"],
        sla_ms=sla_ms,
        peak_bytes=peak_bytes,
        max_peak_bytes=max_peak_bytes,
        object_bytes=None,
        sla_passed=sla_passed,
    )


def run_suite(
    *,
    suite: str = "all",
    warmup: int | None = None,
    iterations: int | None = None,
) -> list[IsoBmffBenchResult]:
    """Execute selected ISO-BMFF micro-benchmarks."""
    results: list[IsoBmffBenchResult] = []
    if suite in ("all", "uniform_stsz"):
        results.append(
            run_uniform_stsz_benchmark(
                warmup=3 if warmup is None else warmup,
                iterations=20 if iterations is None else iterations,
            )
        )
    if suite in ("all", "compact_ctts"):
        results.append(
            run_compact_ctts_benchmark(
                warmup=1 if warmup is None else warmup,
                iterations=5 if iterations is None else iterations,
            )
        )
    return results


def _print_table(results: list[IsoBmffBenchResult]) -> None:
    print("ISO-BMFF Micro-Benchmark Results (tracemalloc decoupled from timing):")
    for r in results:
        status = "PASS" if r.sla_passed else "FAIL"
        peak_kib = r.peak_bytes / 1024.0
        obj_info = f", obj={r.object_bytes}B" if r.object_bytes is not None else ""
        print(
            f"  [{status}] {r.name}: min={r.min_ms:.3f}ms, median={r.median_ms:.3f}ms, "
            f"p95={r.p95_ms:.3f}ms, max={r.max_ms:.3f}ms (SLA < {r.sla_ms:.1f}ms) | "
            f"peak={peak_kib:.2f}KiB{obj_info}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="ISO-BMFF parser strict micro-benchmark runner")
    parser.add_argument("--suite", choices=["all", "uniform_stsz", "compact_ctts"], default="all")
    parser.add_argument("--warmup", type=int, default=None, help="Override warmup iterations")
    parser.add_argument("--iterations", type=int, default=None, help="Override measured iterations")
    parser.add_argument("--json", action="store_true", help="Print JSON summary")
    parser.add_argument("--json-file", default=None, help="Write JSON summary to this file (table still printed)")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any SLA fails")
    args = parser.parse_args(argv)

    results = run_suite(suite=args.suite, warmup=args.warmup, iterations=args.iterations)
    if args.json_file:
        Path(args.json_file).write_text(json.dumps([asdict(r) for r in results], indent=2), encoding="utf-8")
    if args.json:
        print(json.dumps([asdict(r) for r in results], indent=2))
    else:
        _print_table(results)

    if args.strict and any(not r.sla_passed for r in results):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
