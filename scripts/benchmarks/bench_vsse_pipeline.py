"""VSSE vNext pipeline micro-benchmarks (strict SLA verification).

Runs outside pytest coverage/xdist instrumentation and decouples tracemalloc
memory profiling from latency timing so wall-clock measurements reflect real
CPU cost without trace-hook distortion.

Strict failures on shared CI hosts are advisory investigation signals, not merge
gates (the scheduled CI workflow always runs non-strict).

Usage:
    uv run python scripts/benchmarks/bench_vsse_pipeline.py
    uv run python scripts/benchmarks/bench_vsse_pipeline.py --strict
    uv run python scripts/benchmarks/bench_vsse_pipeline.py --json
    uv run python scripts/benchmarks/bench_vsse_pipeline.py --json-file bench_vsse_pipeline.json
"""

from __future__ import annotations

import argparse
import array
import asyncio
import gc
import json
import logging
import os
import statistics
import sys
import time
import tracemalloc
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from unittest.mock import AsyncMock, patch

# 允许从任意工作目录直接运行：将仓库根加入模块搜索路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.core.iso_bmff.box_parser import (
    CttsEntry,
    Hdlr,
    Mdhd,
    Mp4Metadata,
    Mvhd,
    SampleEntry,
    SampleTable,
    StscEntry,
    Stsd,
    SttsEntry,
    Tkhd,
    Track,
)
from app.core.iso_bmff.box_synthesizer import synthesize_faststart_envelope_header
from app.core.pipeline.engines import PikPakStreamTransportEngine
from app.core.pipeline.models import BreathingPolicy, PublishContext, PublishItem, TransportOutcome
from app.core.stream_chunk_provider import BaseChunkProvider
from app.core.vsse_provider import DEFAULT_FTYP_BOX, VirtualEnvelopeChunkProvider, plan_vsse_volumes

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.platform == "win32" and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


@dataclass(slots=True)
class VssePipelineBenchResult:
    """Single VSSE pipeline benchmark measurement summary."""

    name: str
    iterations: int
    warmup: int
    min_ms: float
    median_ms: float
    p95_ms: float
    max_ms: float
    sla_ms: float | None
    peak_mib: float
    sla_peak_mib: float | None
    sla_passed: bool


class _ZeroByteStreamProvider(BaseChunkProvider):
    """Constant O(1) memory chunk provider returning zero bytes."""

    def __init__(self, total_bytes: int, file_id: str = "bench_zero", file_name: str = "zero.mp4"):
        self.file_id = file_id
        self.file_name = file_name
        self.total_bytes = total_bytes

    async def read_chunk(self, offset: int, size: int) -> bytes:
        if offset >= self.total_bytes:
            return b""
        return b"\x00" * min(size, self.total_bytes - offset)

    async def aclose(self) -> None:
        return None


class _MemoryHeaderChunkProvider(BaseChunkProvider):
    """In-memory chunk provider for circuit-breaker micro-benchmarking."""

    def __init__(self, file_id: str, file_name: str, total_bytes: int, header_bytes: bytes):
        self.file_id = file_id
        self.file_name = file_name
        self.total_bytes = total_bytes
        self._header = header_bytes

    async def read_chunk(self, offset: int, size: int) -> bytes:
        if offset < len(self._header):
            return self._header[offset : min(offset + size, len(self._header))]
        return b"\x00" * min(size, max(0, self.total_bytes - offset))

    async def aclose(self) -> None:
        return None


def _build_track(
    *,
    track_id: int,
    is_video: bool,
    sample_sizes: array.array[int],
    chunk_offsets: list[int],
    samples_per_chunk: int = 30,
    sample_delta: int = 1,
    timescale: int = 60,
    sync_samples: list[int] | None = None,
    ctts: list[CttsEntry] | None = None,
) -> Track:
    num_samples = len(sample_sizes)
    entry = (
        SampleEntry(format=b"avc1", data_ref_index=1, codec="avc1", width=1920, height=1080)
        if is_video
        else SampleEntry(format=b"mp4a", data_ref_index=1, codec="mp4a", channel_count=2, sample_rate=48000)
    )
    stbl = SampleTable(
        stsd=Stsd(version=0, flags=0, entry_count=1, entries=[entry]),
        stts=[SttsEntry(sample_count=num_samples, sample_delta=sample_delta)],
        stsc=[StscEntry(first_chunk=1, samples_per_chunk=samples_per_chunk, sample_description_index=1)],
        stsz=sample_sizes,
        chunk_offsets=array.array("Q", chunk_offsets),
        stss=array.array("I", sync_samples) if sync_samples is not None else None,
        ctts=ctts,
        default_sample_size=0,
        is_co64=True,
    )
    duration = num_samples * sample_delta
    tkhd = Tkhd(
        version=0,
        flags=7,
        creation_time=0,
        modification_time=0,
        track_id=track_id,
        duration=duration,
        layer=0,
        alternate_group=0,
        volume=0 if is_video else 0x0100,
        width=1920.0 if is_video else 0.0,
        height=1080.0 if is_video else 0.0,
        width_raw=(1920 << 16) if is_video else 0,
        height_raw=(1080 << 16) if is_video else 0,
    )
    mdhd = Mdhd(
        version=0,
        flags=0,
        creation_time=0,
        modification_time=0,
        timescale=timescale,
        duration=duration,
        language=0x55C4,
    )
    hdlr = Hdlr(
        handler_type=b"vide" if is_video else b"soun",
        name="VideoHandler" if is_video else "SoundHandler",
    )
    return Track(track_id=track_id, tkhd=tkhd, mdhd=mdhd, hdlr=hdlr, stbl=stbl, edts=None)


def _build_metadata(v_track: Track, a_track: Track | None = None) -> Mp4Metadata:
    tracks = [v_track] if a_track is None else [v_track, a_track]
    mvhd = Mvhd(
        version=0,
        flags=0,
        creation_time=0,
        modification_time=0,
        timescale=1000,
        duration=max(t.tkhd.duration for t in tracks),
        rate=0x00010000,
        volume=0x0100,
        matrix=(0x00010000, 0, 0, 0, 0x00010000, 0, 0, 0, 0x40000000),
        next_track_id=len(tracks) + 1,
    )
    return Mp4Metadata(mvhd=mvhd, tracks=tracks, udta_bytes=None)


async def _measure_async_peak_mib(fn: Callable[[], Awaitable[object]]) -> float:
    await fn()  # Warm up lazy caches before tracing allocations
    gc.collect()
    tracemalloc.start()
    try:
        await fn()
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return peak_bytes / (1024 * 1024)


async def _measure_async_latencies[T](
    fn: Callable[[], Awaitable[T]],
    *,
    warmup: int,
    iterations: int,
    results: list[T] | None = None,
) -> list[float]:
    for _ in range(warmup):
        await fn()
    samples_ms: list[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        result = await fn()
        samples_ms.append((time.perf_counter() - t0) * 1000)
        if results is not None:
            results.append(result)
    return samples_ms


def _summarize_result(
    *,
    name: str,
    samples_ms: list[float],
    warmup: int,
    sla_ms: float | None,
    peak_mib: float,
    sla_peak_mib: float | None,
) -> VssePipelineBenchResult:
    ordered = sorted(samples_ms)
    p95_idx = min(len(ordered) - 1, max(0, int(0.95 * (len(ordered) - 1))))
    min_ms = ordered[0]
    time_ok = sla_ms is None or min_ms < sla_ms
    mem_ok = sla_peak_mib is None or peak_mib < sla_peak_mib
    sla_passed = time_ok and mem_ok
    return VssePipelineBenchResult(
        name=name,
        iterations=len(samples_ms),
        warmup=warmup,
        min_ms=round(min_ms, 3),
        median_ms=round(statistics.median(ordered), 3),
        p95_ms=round(ordered[p95_idx], 3),
        max_ms=round(ordered[-1], 3),
        sla_ms=sla_ms,
        peak_mib=round(peak_mib, 3),
        sla_peak_mib=sla_peak_mib,
        sla_passed=sla_passed,
    )


async def run_ttfb_large_envelope_benchmark(*, warmup: int, iterations: int) -> VssePipelineBenchResult:
    """Measure planning + synthesis + first 512 KiB read TTFB on 60k video frames."""
    n_samples = 60_000
    n_chunks = (n_samples + 29) // 30
    meta = _build_metadata(
        _build_track(
            track_id=1,
            is_video=True,
            sample_sizes=array.array("I", [25000]) * n_samples,
            chunk_offsets=list(range(10000, 10000 + n_chunks * 750000, 750000)),
            sync_samples=list(range(1, n_samples + 1, 600)),
        )
    )

    async def _run_once() -> bytes:
        plans = plan_vsse_volumes(meta, threshold_bytes=500 * 1024 * 1024, file_name="large_benchmark.mp4")
        p1 = plans[0]
        hdr = synthesize_faststart_envelope_header(
            metadata=meta,
            ftyp_bytes=DEFAULT_FTYP_BOX,
            v_start=p1.v_start,
            v_end=p1.v_end,
            a_start=p1.a_start,
            a_end=p1.a_end,
            o_min=p1.o_min,
            o_max=p1.o_max,
            payload_len=p1.payload_len,
        )
        source = _ZeroByteStreamProvider(total_bytes=p1.o_min + p1.payload_len + 1024)
        provider = VirtualEnvelopeChunkProvider(
            file_id="bench_p1",
            file_name=p1.part_name,
            faststart_header=hdr,
            source_provider=source,
            envelope_start=p1.o_min,
            envelope_length=p1.payload_len,
        )
        first_chunk = await provider.read_chunk(0, 524288)
        assert first_chunk[4:8] == b"ftyp"
        return first_chunk

    peak_mib = await _measure_async_peak_mib(_run_once)
    samples_ms = await _measure_async_latencies(_run_once, warmup=warmup, iterations=iterations)
    return _summarize_result(
        name="vsse_ttfb_large_envelope",
        samples_ms=samples_ms,
        warmup=warmup,
        sla_ms=100.0,
        peak_mib=peak_mib,
        sla_peak_mib=15.0,
    )


async def run_ttfb_av_bframes_benchmark(*, warmup: int, iterations: int) -> VssePipelineBenchResult:
    """Measure TTFB with 60k B-frame video samples (ctts) + 60k audio samples."""
    n_samples = 60_000
    n_chunks = (n_samples + 29) // 30
    v_track = _build_track(
        track_id=1,
        is_video=True,
        sample_sizes=array.array("I", [25000]) * n_samples,
        chunk_offsets=list(range(10000, 10000 + n_chunks * 750000, 750000)),
        sync_samples=list(range(1, n_samples + 1, 600)),
        ctts=[CttsEntry(1, (i % 3) * 1000) for i in range(n_samples)],
    )
    a_track = _build_track(
        track_id=2,
        is_video=False,
        sample_sizes=array.array("I", [500]) * n_samples,
        chunk_offsets=list(range(12000, 12000 + n_chunks * 750000, 750000)),
    )
    meta = _build_metadata(v_track, a_track)

    async def _run_once() -> bytes:
        plans = plan_vsse_volumes(meta, threshold_bytes=500 * 1024 * 1024, file_name="large_av_bframes.mp4")
        p1 = plans[0]
        hdr = synthesize_faststart_envelope_header(
            metadata=meta,
            ftyp_bytes=DEFAULT_FTYP_BOX,
            v_start=p1.v_start,
            v_end=p1.v_end,
            a_start=p1.a_start,
            a_end=p1.a_end,
            o_min=p1.o_min,
            o_max=p1.o_max,
            payload_len=p1.payload_len,
        )
        source = _ZeroByteStreamProvider(total_bytes=p1.o_min + p1.payload_len + 1024)
        provider = VirtualEnvelopeChunkProvider(
            file_id="bench_av_p1",
            file_name=p1.part_name,
            faststart_header=hdr,
            source_provider=source,
            envelope_start=p1.o_min,
            envelope_length=p1.payload_len,
        )
        first_chunk = await provider.read_chunk(0, 524288)
        assert first_chunk[4:8] == b"ftyp"
        return first_chunk

    peak_mib = await _measure_async_peak_mib(_run_once)
    samples_ms = await _measure_async_latencies(_run_once, warmup=warmup, iterations=iterations)
    return _summarize_result(
        name="vsse_ttfb_av_bframes",
        samples_ms=samples_ms,
        warmup=warmup,
        sla_ms=100.0,
        peak_mib=peak_mib,
        sla_peak_mib=15.0,
    )


async def run_circuit_breaker_benchmark(*, warmup: int, iterations: int) -> VssePipelineBenchResult:
    """Measure circuit-breaker detection and fMP4 fallback latency on corrupt moov."""
    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value=["https://cdn.example.com/corrupt.mp4"]))
    file_size = 3000 * 1024 * 1024
    item = PublishItem(
        item_id="fid_corrupt_moov",
        name="corrupt.mp4",
        size_bytes=file_size,
        duration_s=1800,
        audio_codec="aac",
        source_type="pikpak",
    )
    header_bytes = b"\x00\x00\x00\x20ftypisom\x00\x00\x02\x00isomiso2mp41\x00\x00\x01\x00moovCORRUPTED_MOOV_PAYLOAD"
    mock_provider = _MemoryHeaderChunkProvider(item.item_id, item.name, file_size, header_bytes)
    ctx = PublishContext(
        task_id="bench_cb",
        target_chat_id=-1009000000010,
        target_channel_raw="-1009000000010",
        is_folder_publish=False,
        folder_name="",
        dir_tree={},
        auto_cleanup_mode="none",
        userbot_client=AsyncMock(),
        split_strategy="vsse_with_fallback",
        video_split_threshold_bytes=2000 * 1024 * 1024,
        upload_concurrency=8,
        breathing=BreathingPolicy(
            min_floor_s=0.0, jitter_s=0.0, poll_s=0.01, retry_backoff_s=(0.0, 0.0), enabled=False
        ),
    )

    async def _run_once() -> TransportOutcome:
        with (
            patch.object(engine, "_precheck_cdn_range", new_callable=AsyncMock),
            patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", return_value=mock_provider),
            patch.object(
                engine,
                "_publish_fmp4_split_item",
                new_callable=AsyncMock,
                return_value=TransportOutcome(message_ids=[901], speed_mb_s=15.0),
            ),
        ):
            return await engine.send_item(item, ctx)

    prev_disable = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        peak_mib = await _measure_async_peak_mib(_run_once)
        samples_ms = await _measure_async_latencies(_run_once, warmup=warmup, iterations=iterations)
    finally:
        logging.disable(prev_disable)
    return _summarize_result(
        name="vsse_circuit_breaker_corrupt_moov",
        samples_ms=samples_ms,
        warmup=warmup,
        sla_ms=100.0,
        peak_mib=peak_mib,
        sla_peak_mib=None,
    )


async def run_peak_memory_av_benchmark(*, warmup: int, iterations: int) -> VssePipelineBenchResult:
    """Measure peak traced memory for 100k+100k AV lifecycle (< 15 MiB SLA)."""

    async def _run_once() -> None:
        n_samples = 100_000
        n_chunks = (n_samples + 29) // 30
        v_track = _build_track(
            track_id=1,
            is_video=True,
            sample_sizes=array.array("I", [25000]) * n_samples,
            chunk_offsets=list(range(10000, 10000 + n_chunks * 750000, 750000)),
            sync_samples=list(range(1, n_samples + 1, 600)),
        )
        a_track = _build_track(
            track_id=2,
            is_video=False,
            sample_sizes=array.array("I", [500]) * n_samples,
            chunk_offsets=list(range(12000, 12000 + n_chunks * 750000, 750000)),
        )
        meta = _build_metadata(v_track, a_track)
        plans = plan_vsse_volumes(meta, threshold_bytes=500 * 1024 * 1024, file_name="mem_test.mp4")
        mock_source = _ZeroByteStreamProvider(total_bytes=4000 * 1024 * 1024)
        for p in plans:
            hdr = synthesize_faststart_envelope_header(
                metadata=meta,
                ftyp_bytes=DEFAULT_FTYP_BOX,
                v_start=p.v_start,
                v_end=p.v_end,
                a_start=p.a_start,
                a_end=p.a_end,
                o_min=p.o_min,
                o_max=p.o_max,
                payload_len=p.payload_len,
            )
            provider = VirtualEnvelopeChunkProvider(
                file_id=f"mem_part_{p.part_index}",
                file_name=p.part_name,
                faststart_header=hdr,
                source_provider=mock_source,
                envelope_start=p.o_min,
                envelope_length=p.payload_len,
            )
            await provider.read_chunk(0, 524288)
            await provider.read_chunk(max(0, len(hdr) - 262144), 524288)
            await provider.read_chunk(len(hdr) + 1024, 524288)

    peak_mib = await _measure_async_peak_mib(_run_once)
    samples_ms = await _measure_async_latencies(_run_once, warmup=warmup, iterations=iterations)
    return _summarize_result(
        name="vsse_peak_memory_100k_av",
        samples_ms=samples_ms,
        warmup=warmup,
        sla_ms=1000.0,
        peak_mib=peak_mib,
        sla_peak_mib=15.0,
    )


async def run_ttfb_multi_volume_benchmark(*, warmup: int, iterations: int) -> VssePipelineBenchResult:
    """Measure multi-volume sequential planning + first-chunk TTFB (< 100ms total, < 50ms/vol SLA)."""
    n_samples = 30_000
    n_chunks = (n_samples + 29) // 30
    meta = _build_metadata(
        _build_track(
            track_id=1,
            is_video=True,
            sample_sizes=array.array("I", [20000]) * n_samples,
            chunk_offsets=list(range(10000, 10000 + n_chunks * 600000, 600000)),
            samples_per_chunk=30,
            sample_delta=1,
            timescale=30,
            sync_samples=list(range(1, n_samples + 1, 300)),
        )
    )
    source = _ZeroByteStreamProvider(total_bytes=1000 * 1024 * 1024)

    async def _run_once() -> float:
        plans = plan_vsse_volumes(meta, threshold_bytes=200 * 1024 * 1024, file_name="multi_bench.mp4")
        max_vol_ms = 0.0
        for p in plans:
            t_vol = time.perf_counter()
            hdr = synthesize_faststart_envelope_header(
                metadata=meta,
                ftyp_bytes=DEFAULT_FTYP_BOX,
                v_start=p.v_start,
                v_end=p.v_end,
                a_start=p.a_start,
                a_end=p.a_end,
                o_min=p.o_min,
                o_max=p.o_max,
                payload_len=p.payload_len,
            )
            provider = VirtualEnvelopeChunkProvider(
                file_id=f"bench_multi_{p.part_index}",
                file_name=p.part_name,
                faststart_header=hdr,
                source_provider=source,
                envelope_start=p.o_min,
                envelope_length=p.payload_len,
            )
            first_chunk = await provider.read_chunk(0, 524288)
            assert first_chunk[4:8] == b"ftyp"
            max_vol_ms = max(max_vol_ms, (time.perf_counter() - t_vol) * 1000)
        return max_vol_ms

    peak_mib = await _measure_async_peak_mib(_run_once)
    max_vol_samples_ms: list[float] = []
    samples_ms = await _measure_async_latencies(
        _run_once, warmup=warmup, iterations=iterations, results=max_vol_samples_ms
    )
    result = _summarize_result(
        name="vsse_ttfb_multi_volume",
        samples_ms=samples_ms,
        warmup=warmup,
        sla_ms=100.0,
        peak_mib=peak_mib,
        sla_peak_mib=15.0,
    )
    # Keep the best-run policy, requiring both latency limits in the same run.
    result.sla_passed = result.sla_passed and any(
        total_ms < 100.0 and max_vol_ms < 50.0
        for total_ms, max_vol_ms in zip(samples_ms, max_vol_samples_ms, strict=True)
    )
    return result


async def run_streaming_constant_memory_benchmark(*, warmup: int, iterations: int) -> VssePipelineBenchResult:
    """Measure O(1) streaming peak memory across 50 MiB of virtual chunk reads (< 5 MiB SLA)."""
    hdr = DEFAULT_FTYP_BOX + b"\x00\x00\x00\x10free_padding"
    source = _ZeroByteStreamProvider(total_bytes=100 * 1024 * 1024)
    provider = VirtualEnvelopeChunkProvider(
        file_id="bench_leak_check",
        file_name="leak.mp4",
        faststart_header=hdr,
        source_provider=source,
        envelope_start=0,
        envelope_length=60 * 1024 * 1024,
    )

    async def _run_once() -> int:
        offset = 0
        total_read = 0
        chunk_size = 524288
        target_bytes = 50 * 1024 * 1024
        while total_read < target_bytes:
            chunk = await provider.read_chunk(offset, chunk_size)
            if not chunk:
                break
            total_read += len(chunk)
            offset += len(chunk)
        return total_read

    peak_mib = await _measure_async_peak_mib(_run_once)
    samples_ms = await _measure_async_latencies(_run_once, warmup=warmup, iterations=iterations)
    return _summarize_result(
        name="vsse_streaming_50mib_o1",
        samples_ms=samples_ms,
        warmup=warmup,
        sla_ms=None,
        peak_mib=peak_mib,
        sla_peak_mib=5.0,
    )


async def run_suite(suite: str, *, warmup: int, iterations: int) -> list[VssePipelineBenchResult]:
    """Run requested VSSE pipeline micro-benchmark suites."""
    runners: dict[str, Callable[[], Awaitable[VssePipelineBenchResult]]] = {
        "ttfb_large": lambda: run_ttfb_large_envelope_benchmark(warmup=warmup, iterations=iterations),
        "ttfb_multi": lambda: run_ttfb_multi_volume_benchmark(warmup=warmup, iterations=iterations),
        "ttfb_bframes": lambda: run_ttfb_av_bframes_benchmark(warmup=warmup, iterations=iterations),
        "circuit_breaker": lambda: run_circuit_breaker_benchmark(warmup=warmup, iterations=iterations),
        "peak_memory": lambda: run_peak_memory_av_benchmark(warmup=warmup, iterations=iterations),
        "streaming_memory": lambda: run_streaming_constant_memory_benchmark(warmup=warmup, iterations=iterations),
    }
    selected = list(runners.keys()) if suite == "all" else [suite]
    return [await runners[name]() for name in selected]


def _format_table(results: list[VssePipelineBenchResult]) -> str:
    header = (
        f"{'benchmark':<36} {'min_ms':>8} {'med_ms':>8} {'p95_ms':>8} "
        f"{'sla_ms':>8} {'peak_mib':>10} {'sla_mib':>8} {'status':>8}"
    )
    lines = ["VSSE Pipeline Micro-Benchmark Results", header, "-" * len(header)]
    for item in results:
        status = "PASS" if item.sla_passed else "FAIL"
        sla_ms_str = f"{item.sla_ms:>8.1f}" if item.sla_ms is not None else f"{'-':>8}"
        sla_mib_str = f"{item.sla_peak_mib:>8.1f}" if item.sla_peak_mib is not None else f"{'-':>8}"
        lines.append(
            f"{item.name:<36} {item.min_ms:>8.3f} {item.median_ms:>8.3f} {item.p95_ms:>8.3f} "
            f"{sla_ms_str} {item.peak_mib:>10.3f} {sla_mib_str} {status:>8}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run VSSE vNext pipeline micro-benchmarks.")
    parser.add_argument(
        "--suite",
        choices=[
            "all",
            "ttfb_large",
            "ttfb_multi",
            "ttfb_bframes",
            "circuit_breaker",
            "peak_memory",
            "streaming_memory",
        ],
        default="all",
        help="Which VSSE pipeline benchmark suite to run.",
    )
    parser.add_argument("--warmup", type=int, default=2, help="Warmup runs before timing.")
    parser.add_argument("--iterations", type=int, default=5, help="Timed iterations.")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any strict SLA threshold fails.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON output.")
    parser.add_argument(
        "--json-file",
        default=None,
        help="Optional file path to write machine-readable JSON results while keeping stdout table.",
    )
    args = parser.parse_args(argv)

    results = asyncio.run(run_suite(args.suite, warmup=args.warmup, iterations=args.iterations))
    payload = [asdict(r) for r in results]
    if args.json_file:
        out_path = Path(args.json_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(_format_table(results))

    if args.strict and any(not r.sla_passed for r in results):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
