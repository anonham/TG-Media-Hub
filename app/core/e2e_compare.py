"""E2E 捕获证据离线对比引擎 (计划 D5 三层断言 A1-A4 + 附录 B 降级矩阵)。

纯逻辑 + 受控文件 IO (输入 manifest dict + 证据目录), 供 scripts/e2e_compare.py
CLI 组装与 tests/core/test_e2e_compare.py 单测复用。断言分层:

- A1 (必须全等): sha256(tg_mirror[i]) == sha256(parts[i]) —— MTProto 上传字节保真,
  本模式存在的核心理由 (含乱序上传/sidx 错位/末块截断的归因入口);
- A2 (必须相等, VSSE): parts[i][H:H+L] == source[o:o+L] 逐字节 (1 MiB 流式分块比对)
  + 产物完备性 len(parts[i]) == H + L; 缺 E1 → SKIP 源侧, 完备性仍判;
- A3 (弱断言, fMP4): ffprobe 时长 ≈ plan.duration_sec (±2%), Σ 各卷时长 ≈ 最终卷
  end_sec (±1%); ffprobe 缺失 → SKIPPED 不 FAIL;
- A4 (必须相等): tg_mirror document.size == parts[i] 字节数 (TG 侧静默截断双保险)。

降级语义 (附录 B): 任一证据缺失 → 对应断言 SKIP (无法判定), 不是 FAIL;
manifest.rolled_back=True → A1/A4 全 SKIP (消息已被物理回滚, 无镜像可言)。
"""

import hashlib
import json
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

_HASH_CHUNK = 1024 * 1024  # 流式比对/哈希块 (不做全文件 hash 于 A2, 哈希除外)
A3_DURATION_TOLERANCE = 0.02  # ±2%: ffprobe 时长 vs plan.duration_sec
A3_SUM_TOLERANCE = 0.01  # ±1%: Σ 各卷时长 vs 最终卷 end_sec


@dataclass
class AssertionResult:
    name: str  # e.g. "A1", "A2[part001]", "A2-completeness[part001]"
    status: str  # PASS | FAIL | SKIP
    detail: str = ""


@dataclass
class CompareReport:
    results: list[AssertionResult] = field(default_factory=list)
    manifest: dict | None = None

    @property
    def has_fail(self) -> bool:
        return any(r.status == "FAIL" for r in self.results)


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            block = f.read(_HASH_CHUNK)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def file_size(path: str) -> int:
    return os.path.getsize(path)


def _ffprobe_duration(path: str) -> float | None:
    """ffprobe 时长探测; 缺失/失败返回 None (A3 → SKIPPED, 弱断言不 FAIL)。"""
    if shutil.which("ffprobe") is None:
        return None
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                path,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return float(proc.stdout.strip())
    except (OSError, ValueError, subprocess.SubprocessError) as e:
        logger.debug("ffprobe 时长探测失败 (%s): %s", path, e)
        return None


def _ranges_equal(path_a: str, offset_a: int, path_b: str, offset_b: int, length: int) -> tuple[bool, int]:
    """两文件区间 1 MiB 流式比对; 返回 (相等, 首个不等绝对偏移)。"""
    with open(path_a, "rb") as fa, open(path_b, "rb") as fb:
        done = 0
        while done < length:
            n = min(_HASH_CHUNK, length - done)
            fa.seek(offset_a + done)
            fb.seek(offset_b + done)
            ba = fa.read(n)
            bb = fb.read(n)
            if len(ba) != n or len(bb) != n:
                limit = min(len(ba), len(bb))
                for i in range(limit):
                    if ba[i] != bb[i]:
                        return False, done + i
                return False, done + limit
            if ba != bb:
                for i in range(n):
                    if ba[i] != bb[i]:
                        return False, done + i
                return False, done
            done += n
    return True, -1


def compare_manifest(manifest: dict, root: str) -> CompareReport:
    """对单 item manifest 执行 A1-A4; 回填 parts[].sha256; 不写任何文件。"""
    report = CompareReport(manifest=manifest)
    rolled_back = bool(manifest.get("rolled_back"))
    source = manifest.get("source") or {}
    source_ok = source.get("status") == "ok" and source.get("path")
    source_path = os.path.join(root, source.get("path", "")) if source_ok else None
    mirror_by_name = {m.get("part_name"): m for m in manifest.get("tg_mirror", [])}

    fmp4_parts = [p for p in manifest.get("parts", []) if p.get("channel") == "fmp4" and p.get("status") != "discarded"]
    fmp4_duration_sum = 0.0
    fmp4_duration_probe_count = 0

    for part in manifest.get("parts", []):
        name = part.get("part_name", "?")
        channel = part.get("channel", "?")
        part_rel = part.get("path") or ""
        part_path = os.path.join(root, part_rel) if part_rel else None
        mirror = mirror_by_name.get(name) or {}

        # ---- A2 (VSSE): 区间比对 + 完备性 --------------------------------
        if channel == "vsse":
            header_len = int(part.get("header_len") or 0)
            env_start = int(part.get("envelope_start") or 0)
            env_len = int(part.get("envelope_length") or 0)
            if not part_path or not os.path.isfile(part_path):
                report.results.append(AssertionResult(f"A2[{name}]", "SKIP", "缺 E2 产物"))
            else:
                # 完备性: 无需源即可判定 (附录 B: SKIP 源侧时完备性仍判)
                actual_len = file_size(part_path)
                if actual_len == header_len + env_len:
                    report.results.append(
                        AssertionResult(
                            f"A2-completeness[{name}]", "PASS", f"len={actual_len} == H({header_len})+L({env_len})"
                        )
                    )
                else:
                    report.results.append(
                        AssertionResult(
                            f"A2-completeness[{name}]", "FAIL", f"len={actual_len} != H({header_len})+L({env_len})"
                        )
                    )
                if source_path and os.path.isfile(source_path):
                    eq, first_diff = _ranges_equal(part_path, header_len, source_path, env_start, env_len)
                    if eq:
                        report.results.append(
                            AssertionResult(f"A2[{name}]", "PASS", f"envelope[{env_start}:+{env_len}] 逐字节相等")
                        )
                    else:
                        report.results.append(
                            AssertionResult(f"A2[{name}]", "FAIL", f"首个不等偏移 {first_diff} (卷内)")
                        )
                else:
                    report.results.append(AssertionResult(f"A2[{name}]", "SKIP", "缺 E1 源快照"))

        # ---- 证据文件与镜像哈希 (A1/A4 公共前置) --------------------------
        if part_path and os.path.isfile(part_path):
            part["sha256"] = sha256_file(part_path)  # 回填 (schema: finalize 惰性补算)
        else:
            part_path = None

        mirror_rel = mirror.get("path") or ""
        mirror_path = os.path.join(root, mirror_rel) if mirror_rel else None
        if mirror_path and not os.path.isfile(mirror_path):
            mirror_path = None

        # ---- A1: 镜像与产物字节全等 --------------------------------------
        if part.get("status") != "sent":
            report.results.append(
                AssertionResult(f"A1[{name}]", "SKIP", f"part status={part.get('status')} (无对应消息)")
            )
        elif rolled_back:
            report.results.append(AssertionResult(f"A1[{name}]", "SKIP", "已回滚: 消息被物理删除"))
        elif part_path is None or mirror_path is None:
            report.results.append(AssertionResult(f"A1[{name}]", "SKIP", "缺 E2 产物或 E3 镜像"))
        else:
            part_sha = part.get("sha256") or sha256_file(part_path)
            mirror_sha = sha256_file(mirror_path)
            mirror["sha256"] = mirror_sha
            if part_sha == mirror_sha:
                report.results.append(AssertionResult(f"A1[{name}]", "PASS", part_sha[:16] + "…"))
            else:
                report.results.append(
                    AssertionResult(
                        f"A1[{name}]",
                        "FAIL",
                        f"parts={part_sha[:16]}… tg_mirror={mirror_sha[:16]}… (MTProto 传输链路失真)",
                    )
                )

        # ---- A4: TG document.size 双保险 ----------------------------------
        doc_size = mirror.get("document_size")
        if part.get("status") != "sent" or rolled_back:
            report.results.append(AssertionResult(f"A4[{name}]", "SKIP", "无消息或已回滚"))
        elif part_path is None or doc_size in (None, 0):
            report.results.append(AssertionResult(f"A4[{name}]", "SKIP", "缺 E2 产物或镜像元数据"))
        elif int(doc_size) == file_size(part_path):
            report.results.append(AssertionResult(f"A4[{name}]", "PASS", f"size={doc_size}"))
        else:
            report.results.append(
                AssertionResult(
                    f"A4[{name}]",
                    "FAIL",
                    f"document.size={doc_size} != parts 字节数={file_size(part_path)} (TG 侧截断嫌疑)",
                )
            )

        # ---- A5: 消息实发 attributes 快照 (消息标签层, 属性统一计划包1) ------
        # w/h 仅为记录项 (manifest 内无独立期望源), 硬断言 duration 与 supports_streaming
        sent_attrs = part.get("sent_attributes")
        if part.get("status") != "sent" or rolled_back:
            report.results.append(AssertionResult(f"A5[{name}]", "SKIP", "无消息或已回滚"))
        elif not sent_attrs:
            report.results.append(
                AssertionResult(f"A5[{name}]", "SKIP", "manifest 未记录 sent_attributes (特性前存量)")
            )
        else:
            plan_meta = part.get("plan") or {}
            plan_dur = float(plan_meta.get("duration_sec") or 0.0)
            if channel == "fmp4":
                expect_dur = int(float(sent_attrs.get("actual_dur") or plan_dur))
            else:
                expect_dur = int(plan_dur)
            if sent_attrs.get("supports_streaming") is not True:
                report.results.append(
                    AssertionResult(
                        f"A5[{name}]", "FAIL", f"supports_streaming={sent_attrs.get('supports_streaming')} (期望 True)"
                    )
                )
            elif sent_attrs.get("duration") != expect_dur:
                report.results.append(
                    AssertionResult(
                        f"A5[{name}]", "FAIL", f"实发 duration={sent_attrs.get('duration')} != 期望 {expect_dur}"
                    )
                )
            else:
                report.results.append(
                    AssertionResult(
                        f"A5[{name}]",
                        "PASS",
                        f"duration={sent_attrs.get('duration')} w={sent_attrs.get('w')} h={sent_attrs.get('h')}",
                    )
                )

        # ---- A3 (fMP4): ffprobe 元数据弱断言 ------------------------------
        if channel == "fmp4" and part_path is not None and part.get("status") != "discarded":
            plan = part.get("plan") or {}
            plan_expect_dur = float(plan.get("duration_sec") or 0.0)
            probed = _ffprobe_duration(part_path)
            if probed is None:
                report.results.append(AssertionResult(f"A3[{name}]", "SKIP", "ffprobe 不可用"))
            elif plan_expect_dur > 0 and abs(probed - plan_expect_dur) / plan_expect_dur <= A3_DURATION_TOLERANCE:
                report.results.append(
                    AssertionResult(f"A3[{name}]", "PASS", f"duration={probed:.2f}s ≈ {plan_expect_dur:.2f}s")
                )
                fmp4_duration_sum += probed
                fmp4_duration_probe_count += 1
            else:
                report.results.append(
                    AssertionResult(f"A3[{name}]", "FAIL", f"duration={probed} vs plan={plan_expect_dur}")
                )

    # A3 汇总: Σ 各卷时长 ≈ 最终卷 end_sec (±1%)
    probed_plans = [
        (float((p.get("plan") or {}).get("duration_sec") or 0.0), float((p.get("plan") or {}).get("end_sec") or 0.0))
        for p in fmp4_parts
    ]
    if fmp4_duration_probe_count == len(fmp4_parts) and len(fmp4_parts) >= 2 and probed_plans:
        total_expect = max(end for _, end in probed_plans)
        if total_expect > 0:
            if abs(fmp4_duration_sum - total_expect) / total_expect <= A3_SUM_TOLERANCE:
                report.results.append(
                    AssertionResult("A3-sum", "PASS", f"Σduration={fmp4_duration_sum:.2f}s ≈ {total_expect:.2f}s")
                )
            else:
                report.results.append(
                    AssertionResult(
                        "A3-sum", "FAIL", f"Σduration={fmp4_duration_sum:.2f}s vs {total_expect:.2f}s (±1% 超差)"
                    )
                )

    return report


def render_report_md(manifest: dict, report: CompareReport) -> str:
    lines = [
        "# E2E 捕获对比报告",
        "",
        f"- task/item: `{manifest.get('task_id')}` / `{manifest.get('item_id')}`",
        f"- 通道切换: {manifest.get('channel_switch') or '无'}",
        f"- 回滚: {'是 (消息已物理删除, A1/A4 全 SKIP)' if manifest.get('rolled_back') else '否'}",
        f"- 源快照: {(manifest.get('source') or {}).get('status', 'unknown')}",
        f"- 判定: {'**FAIL**' if report.has_fail else '**ALL PASS** (含 SKIP 降级项)'}",
        "",
        "| 断言 | 结果 | 说明 |",
        "|---|---|---|",
    ]
    for r in report.results:
        lines.append(f"| {r.name} | {r.status} | {r.detail} |")
    return "\n".join(lines) + "\n"


def run_compare(manifest_path: str, write_report: bool = True) -> tuple[dict, CompareReport, bool]:
    """加载 manifest → 比对 → 回写 manifest (sha256/compare 段) + 报告; 返回 (manifest, report, ok)。

    报告写 manifest 同目录 compare_report.md / compare_report.json。
    """
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)
    root = os.path.dirname(os.path.abspath(manifest_path))

    report = compare_manifest(manifest, root)
    ok = not report.has_fail

    if write_report:
        manifest.setdefault("compare", {})
        manifest["compare"]["status"] = "done" if ok else "fail"
        manifest["compare"]["report"] = "compare_report.md"
        md_path = os.path.join(root, "compare_report.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(render_report_md(manifest, report))
        json_path = os.path.join(root, "compare_report.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "ok": ok,
                    "results": [{"name": r.name, "status": r.status, "detail": r.detail} for r in report.results],
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        tmp = manifest_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        os.replace(tmp, manifest_path)
    return manifest, report, ok
