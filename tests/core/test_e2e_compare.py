"""E2E 捕获离线工具单测: compare 引擎断言矩阵 + 降级。

对应 docs/testing/E2E_GUIDE.md §5.1; conftest 已将
E2E_CAPTURE_DIR 沙箱化到 tmp_dir。
"""

import hashlib
import json
import os
from unittest.mock import patch

from app.config import settings
from app.core.e2e_compare import compare_manifest, run_compare
from tests._safeio import safe_write_bytes

# ---------------------------------------------------------------------------
# 装具
# ---------------------------------------------------------------------------


def _write(root: str, rel: str, data: bytes) -> str:
    return safe_write_bytes(os.path.join(root, *rel.split("/")), data)


def _build_root(
    *,
    source_data: bytes = b"",
    source_status: str = "ok",
    part_specs: list = None,
    mirror_specs: list = None,
    rolled_back: bool = False,
    part_status: str = "sent",
) -> tuple:
    """落盘证据文件并返回 (manifest, root)。

    part_specs: [{name, data, channel, header_len, envelope_start, envelope_length, plan}]
    mirror_specs: [{name, data, document_size}] (data=None 表示不落镜像文件)
    """
    root = os.path.join(settings.E2E_CAPTURE_DIR, "task_cmp", "item_cmp")
    manifest = {
        "schema_version": 1,
        "task_id": "task_cmp",
        "item_id": "item_cmp",
        "target_chat_id": -1009000000010,
        "rolled_back": rolled_back,
        "source": {
            "name": "movie.mp4",
            "path": "source/movie.mp4" if source_status == "ok" else "",
            "size_bytes": len(source_data),
            "sha256": "",
            "status": source_status,
            "error": "",
        },
        "parts": [],
        "tg_mirror": [],
        "compare": {"status": "pending", "report": ""},
    }
    if source_status == "ok":
        _write(root, "source/movie.mp4", source_data)

    for spec in part_specs or []:
        _write(root, f"parts/{spec['name']}", spec["data"])
        part_entry = {
            "part_name": spec["name"],
            "channel": spec.get("channel", "vsse"),
            "plan": spec.get("plan") or {},
            "header_len": spec.get("header_len", 0),
            "envelope_start": spec.get("envelope_start", 0),
            "envelope_length": spec.get("envelope_length", 0),
            "path": f"parts/{spec['name']}",
            "bytes_written": len(spec["data"]),
            "sha256": "",
            "msg_id": spec.get("msg_id", 101),
            "status": spec.get("status", part_status),
            "error": "",
        }
        if "sent_attributes" in spec:  # 特性前存量 manifest 不含此键
            part_entry["sent_attributes"] = spec["sent_attributes"]
        manifest["parts"].append(part_entry)
    for spec in mirror_specs or []:
        entry = {
            "part_name": spec["name"],
            "msg_id": spec.get("msg_id", 101),
            "path": "",
            "size_bytes": 0,
            "document_size": spec.get("document_size", 0),
            "sha256": "",
            "status": "ok",
            "error": "",
        }
        if spec.get("data") is not None:
            _write(root, f"tg_mirror/{spec['name']}", spec["data"])
            entry["path"] = f"tg_mirror/{spec['name']}"
        manifest["tg_mirror"].append(entry)
    return manifest, root


def _result_map(report):
    return {(r.name, r.status) for r in report.results}


def _get(report, name_prefix):
    return next(r for r in report.results if r.name.startswith(name_prefix))


# ---------------------------------------------------------------------------
# A1 / A4
# ---------------------------------------------------------------------------


def test_a1_pass_when_mirror_identical():
    payload = os.urandom(300_000)
    manifest, root = _build_root(
        source_data=b"x",
        part_specs=[{"name": "p1.mp4", "data": payload, "channel": "fmp4"}],
        mirror_specs=[{"name": "p1.mp4", "data": payload, "document_size": len(payload)}],
    )
    report = compare_manifest(manifest, root)
    assert _get(report, "A1[").status == "PASS"
    assert _get(report, "A4[").status == "PASS"
    assert not report.has_fail


def test_a1_fail_when_mirror_differs_and_a4_size_double_check():
    payload = os.urandom(300_000)
    tampered = payload[:-1] + bytes([payload[-1] ^ 0xFF])
    manifest, root = _build_root(
        source_data=b"x",
        part_specs=[{"name": "p1.mp4", "data": payload, "channel": "fmp4"}],
        mirror_specs=[{"name": "p1.mp4", "data": tampered, "document_size": len(tampered)}],
    )
    report = compare_manifest(manifest, root)
    assert _get(report, "A1[").status == "FAIL"
    assert report.has_fail


def test_a4_fail_when_document_size_mismatches():
    payload = os.urandom(1000)
    manifest, root = _build_root(
        source_data=b"x",
        part_specs=[{"name": "p1.mp4", "data": payload, "channel": "fmp4"}],
        mirror_specs=[{"name": "p1.mp4", "data": payload, "document_size": len(payload) - 7}],
    )
    report = compare_manifest(manifest, root)
    a4 = _get(report, "A4[")
    assert a4.status == "FAIL" and "截断" in a4.detail


# ---------------------------------------------------------------------------
# A2 (VSSE)
# ---------------------------------------------------------------------------


def _pattern_source(size: int) -> bytes:
    return bytes(i % 251 for i in range(size))


def test_a2_pass_interval_and_completeness():
    src = _pattern_source(2 * 1024 * 1024)
    header = b"H" * 64
    env_start, env_len = 1024 * 1024, 512 * 1024
    part = header + src[env_start : env_start + env_len]
    manifest, root = _build_root(
        source_data=src,
        part_specs=[
            {
                "name": "v.part001.mp4",
                "data": part,
                "channel": "vsse",
                "header_len": len(header),
                "envelope_start": env_start,
                "envelope_length": env_len,
            }
        ],
    )
    report = compare_manifest(manifest, root)
    assert _get(report, "A2[").status == "PASS"
    assert _get(report, "A2-completeness[").status == "PASS"
    assert not report.has_fail


def test_a2_fail_on_payload_tamper():
    src = _pattern_source(2 * 1024 * 1024)
    header = b"H" * 64
    env_start, env_len = 1024 * 1024, 512 * 1024
    part = bytearray(header + src[env_start : env_start + env_len])
    part[len(header) + 12345] ^= 0x01  # payload 区间内翻转 1 字节
    manifest, root = _build_root(
        source_data=src,
        part_specs=[
            {
                "name": "v.part001.mp4",
                "data": bytes(part),
                "channel": "vsse",
                "header_len": len(header),
                "envelope_start": env_start,
                "envelope_length": env_len,
            }
        ],
    )
    report = compare_manifest(manifest, root)
    a2 = _get(report, "A2[")
    assert a2.status == "FAIL" and "偏移" in a2.detail


def test_a2_completeness_fail_when_part_truncated():
    src = _pattern_source(2 * 1024 * 1024)
    header = b"H" * 64
    env_start, env_len = 1024 * 1024, 512 * 1024
    part = (header + src[env_start : env_start + env_len])[:-100]  # tee 完备性破坏
    manifest, root = _build_root(
        source_data=src,
        part_specs=[
            {
                "name": "v.part001.mp4",
                "data": part,
                "channel": "vsse",
                "header_len": len(header),
                "envelope_start": env_start,
                "envelope_length": env_len,
            }
        ],
    )
    report = compare_manifest(manifest, root)
    assert _get(report, "A2-completeness[").status == "FAIL"


def test_a2_missing_source_skips_interval_but_still_judges_completeness():
    header = b"H" * 64
    part = header + b"P" * 512 * 1024
    manifest, root = _build_root(
        source_data=b"",
        source_status="failed",
        part_specs=[
            {
                "name": "v.part001.mp4",
                "data": part,
                "channel": "vsse",
                "header_len": len(header),
                "envelope_start": 0,
                "envelope_length": 512 * 1024,
            }
        ],
    )
    report = compare_manifest(manifest, root)
    assert _get(report, "A2[").status == "SKIP"
    assert _get(report, "A2-completeness[").status == "PASS"
    assert not report.has_fail


# ---------------------------------------------------------------------------
# A3 (fMP4, 弱断言)
# ---------------------------------------------------------------------------


def test_a3_skipped_when_ffprobe_unavailable(monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: None)
    payload = os.urandom(200_000)
    manifest, root = _build_root(
        source_data=b"x",
        part_specs=[
            {
                "name": "f.part001.mp4",
                "data": payload,
                "channel": "fmp4",
                "plan": {"duration_sec": 10.0, "end_sec": 10.0},
            }
        ],
        mirror_specs=[{"name": "f.part001.mp4", "data": payload, "document_size": len(payload)}],
    )
    report = compare_manifest(manifest, root)
    a3 = _get(report, "A3[")
    assert a3.status == "SKIP" and "ffprobe" in a3.detail
    assert not report.has_fail  # 弱断言缺失不 FAIL


# ---------------------------------------------------------------------------
# 降级矩阵 (附录 B) 与回滚
# ---------------------------------------------------------------------------


def test_degradation_no_mirror_evidence_skips_a1_a4():
    payload = os.urandom(1000)
    manifest, root = _build_root(
        source_data=b"x",
        part_specs=[{"name": "p1.mp4", "data": payload, "channel": "fmp4"}],
        mirror_specs=[],
    )
    report = compare_manifest(manifest, root)
    assert _get(report, "A1[").status == "SKIP"
    assert _get(report, "A4[").status == "SKIP"
    assert not report.has_fail


def test_rolled_back_skips_a1_a4():
    payload = os.urandom(1000)
    manifest, root = _build_root(
        source_data=b"x",
        rolled_back=True,
        part_specs=[{"name": "p1.mp4", "data": payload, "channel": "fmp4"}],
        mirror_specs=[{"name": "p1.mp4", "data": payload, "document_size": len(payload)}],
    )
    report = compare_manifest(manifest, root)
    assert _get(report, "A1[").status == "SKIP"
    assert _get(report, "A4[").status == "SKIP"


def test_failed_part_not_asserted_against_mirror():
    payload = os.urandom(1000)
    manifest, root = _build_root(
        source_data=b"x",
        part_specs=[{"name": "p1.mp4", "data": payload, "channel": "fmp4", "status": "failed"}],
        mirror_specs=[],
    )
    report = compare_manifest(manifest, root)
    assert _get(report, "A1[").status == "SKIP"


# ---------------------------------------------------------------------------
# A5 (消息实发 attributes 快照, 属性统一计划包1)
# ---------------------------------------------------------------------------


def test_a5_pass_when_sent_attributes_match_plan():
    # fmp4 通道: 避开 vsse A2 完备性对合成 manifest 的既有断言 (A1/A3/A4 与 A5 正交)
    payload = os.urandom(1000)
    manifest, root = _build_root(
        source_data=b"x",
        part_specs=[
            {
                "name": "p1.mp4",
                "data": payload,
                "channel": "fmp4",
                "plan": {"duration_sec": 40.04, "end_sec": 40.04},
                "sent_attributes": {"duration": 40, "w": 1920, "h": 1080, "supports_streaming": True},
            }
        ],
        mirror_specs=[{"name": "p1.mp4", "data": payload, "document_size": len(payload)}],
    )
    report = compare_manifest(manifest, root)
    a5 = _get(report, "A5[")
    assert a5.status == "PASS" and "duration=40" in a5.detail
    assert not report.has_fail


def test_a5_fmp4_uses_actual_dur_as_expectation():
    payload = os.urandom(1000)
    manifest, root = _build_root(
        source_data=b"x",
        part_specs=[
            {
                "name": "f1.mp4",
                "data": payload,
                "channel": "fmp4",
                "plan": {"duration_sec": 10.0, "end_sec": 10.0},
                "sent_attributes": {"duration": 9, "w": 0, "h": 0, "supports_streaming": True, "actual_dur": 9.9},
            }
        ],
        mirror_specs=[{"name": "f1.mp4", "data": payload, "document_size": len(payload)}],
    )
    report = compare_manifest(manifest, root)
    assert _get(report, "A5[").status == "PASS"  # 期望 = int(actual_dur or plan) = 9


def test_a5_fmp4_falls_back_to_plan_when_actual_dur_missing():
    payload = os.urandom(1000)
    manifest, root = _build_root(
        source_data=b"x",
        part_specs=[
            {
                "name": "f2.mp4",
                "data": payload,
                "channel": "fmp4",
                "plan": {"duration_sec": 10.5, "end_sec": 10.5},
                "sent_attributes": {"duration": 10, "w": 0, "h": 0, "supports_streaming": True, "actual_dur": None},
            }
        ],
        mirror_specs=[{"name": "f2.mp4", "data": payload, "document_size": len(payload)}],
    )
    report = compare_manifest(manifest, root)
    assert _get(report, "A5[").status == "PASS"


def test_a5_fail_on_duration_mismatch():
    payload = os.urandom(1000)
    manifest, root = _build_root(
        source_data=b"x",
        part_specs=[
            {
                "name": "p1.mp4",
                "data": payload,
                "channel": "vsse",
                "plan": {"duration_sec": 40.04},
                "sent_attributes": {"duration": 268, "w": 1920, "h": 1080, "supports_streaming": True},
            }
        ],
        mirror_specs=[{"name": "p1.mp4", "data": payload, "document_size": len(payload)}],
    )
    report = compare_manifest(manifest, root)
    a5 = _get(report, "A5[")
    assert a5.status == "FAIL" and "duration" in a5.detail
    assert report.has_fail


def test_a5_fail_when_supports_streaming_missing():
    payload = os.urandom(1000)
    manifest, root = _build_root(
        source_data=b"x",
        part_specs=[
            {
                "name": "p1.mp4",
                "data": payload,
                "channel": "vsse",
                "plan": {"duration_sec": 40.04},
                "sent_attributes": {"duration": 40, "w": 1920, "h": 1080},
            }
        ],
        mirror_specs=[{"name": "p1.mp4", "data": payload, "document_size": len(payload)}],
    )
    report = compare_manifest(manifest, root)
    a5 = _get(report, "A5[")
    assert a5.status == "FAIL" and "supports_streaming" in a5.detail


def test_a5_skip_for_legacy_manifest_without_snapshot():
    payload = os.urandom(1000)
    manifest, root = _build_root(
        source_data=b"x",
        part_specs=[
            {"name": "p1.mp4", "data": payload, "channel": "fmp4", "plan": {"duration_sec": 40.04, "end_sec": 40.04}}
        ],
        mirror_specs=[{"name": "p1.mp4", "data": payload, "document_size": len(payload)}],
    )
    report = compare_manifest(manifest, root)
    a5 = _get(report, "A5[")
    assert a5.status == "SKIP" and "sent_attributes" in a5.detail
    assert not report.has_fail


def test_a5_skip_for_failed_part_and_rollback():
    payload = os.urandom(1000)
    manifest, root = _build_root(
        source_data=b"x",
        rolled_back=True,
        part_specs=[
            {
                "name": "p1.mp4",
                "data": payload,
                "channel": "vsse",
                "plan": {"duration_sec": 40.04},
                "sent_attributes": {"duration": 40, "w": 0, "h": 0, "supports_streaming": True},
            }
        ],
        mirror_specs=[{"name": "p1.mp4", "data": payload, "document_size": len(payload)}],
    )
    report = compare_manifest(manifest, root)
    assert _get(report, "A5[").status == "SKIP"


# ---------------------------------------------------------------------------
# run_compare 落盘产物
# ---------------------------------------------------------------------------


def test_run_compare_writes_reports_and_backfills_sha(tmp_dir):
    payload = os.urandom(64_000)
    manifest, root = _build_root(
        source_data=b"x",
        part_specs=[{"name": "p1.mp4", "data": payload, "channel": "fmp4"}],
        mirror_specs=[{"name": "p1.mp4", "data": payload, "document_size": len(payload)}],
    )
    manifest_path = os.path.join(root, "manifest.json")
    safe_write_bytes(manifest_path, json.dumps(manifest, ensure_ascii=False).encode())

    _m, _r, ok = run_compare(manifest_path)

    assert ok is True
    assert os.path.isfile(os.path.join(root, "compare_report.md"))
    assert os.path.isfile(os.path.join(root, "compare_report.json"))
    with open(manifest_path, encoding="utf-8") as f:
        reloaded = json.load(f)
    assert reloaded["parts"][0]["sha256"] == hashlib.sha256(payload).hexdigest()
    assert reloaded["compare"]["status"] == "done"


def test_run_compare_fail_exit_semantics(tmp_dir):
    payload = os.urandom(64_000)
    tampered = payload[:-3] + b"ZZZ"
    manifest, root = _build_root(
        source_data=b"x",
        part_specs=[{"name": "p1.mp4", "data": payload, "channel": "fmp4"}],
        mirror_specs=[{"name": "p1.mp4", "data": tampered, "document_size": len(tampered)}],
    )
    manifest_path = os.path.join(root, "manifest.json")
    safe_write_bytes(manifest_path, json.dumps(manifest, ensure_ascii=False).encode())

    _m, _r, ok = run_compare(manifest_path)
    assert ok is False  # CLI 退出码非零的来源


def test_compare_manifest_excludes_discarded_fmp4_parts_from_a3_sum(tmp_path):
    """验证 compare_manifest 在统计 A3-sum 时真正排除 status=discarded 的重规划旧分卷"""
    for name in ("p_old.mp4", "p1.mp4", "p2.mp4"):
        (tmp_path / name).write_bytes(b"mp4data")

    manifest = {
        "parts": [
            {
                "channel": "fmp4",
                "status": "discarded",
                "part_name": "p_old",
                "path": "p_old.mp4",
                "plan": {"duration_sec": 15.0, "end_sec": 15.0},
            },
            {
                "channel": "fmp4",
                "status": "sent",
                "part_name": "p1",
                "path": "p1.mp4",
                "plan": {"duration_sec": 10.0, "end_sec": 10.0},
            },
            {
                "channel": "fmp4",
                "status": "sent",
                "part_name": "p2",
                "path": "p2.mp4",
                "plan": {"duration_sec": 10.0, "end_sec": 20.0},
            },
        ],
        "tg_mirror": [],
    }
    durations = {
        str(tmp_path / "p_old.mp4"): 15.0,
        str(tmp_path / "p1.mp4"): 10.0,
        str(tmp_path / "p2.mp4"): 10.0,
    }
    with patch("app.core.e2e_compare._ffprobe_duration", side_effect=lambda p: durations[p]):
        report = compare_manifest(manifest, str(tmp_path))

    a3_sum = [r for r in report.results if r.name == "A3-sum"]
    assert len(a3_sum) == 1
    assert a3_sum[0].status == "PASS"
    assert "20.00s" in a3_sum[0].detail


def test_compare_manifest_recomputes_mirror_sha256_on_every_run(tmp_path):
    """验证 A1 每次均重新计算镜像文件 sha256，不被旧缓存误导通过"""
    part_bytes = b"authentic-part-bytes"
    (tmp_path / "p1.mp4").write_bytes(part_bytes)
    (tmp_path / "m1.mp4").write_bytes(b"corrupted-mirror-bytes")
    stale_sha = hashlib.sha256(part_bytes).hexdigest()

    manifest = {
        "parts": [{"channel": "vsse", "status": "sent", "part_name": "p1", "path": "p1.mp4"}],
        "tg_mirror": [{"part_name": "p1", "path": "m1.mp4", "sha256": stale_sha, "document_size": len(part_bytes)}],
    }
    report = compare_manifest(manifest, str(tmp_path))
    a1 = [r for r in report.results if r.name == "A1[p1]"]
    assert len(a1) == 1
    assert a1[0].status == "FAIL"


def test_ranges_equal_rejects_equally_short_eof_reads(tmp_path):
    """#2: 两个文件同时在请求区间长度前遇到 EOF 时必须判为不等，不能误返回 True 或 IndexError。"""
    from app.core.e2e_compare import _ranges_equal

    fa = tmp_path / "a.bin"
    fb = tmp_path / "b.bin"
    fc = tmp_path / "c.bin"
    fa.write_bytes(b"ab")
    fb.write_bytes(b"ab")
    fc.write_bytes(b"ac")

    assert _ranges_equal(str(fa), 0, str(fb), 0, 4) == (False, 2)
    assert _ranges_equal(str(fa), 0, str(fc), 0, 4) == (False, 1)
