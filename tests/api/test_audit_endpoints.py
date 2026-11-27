"""审计端点读错误分类与异常 JSONL 行容错测试。"""

import json
import os

import pytest
from fastapi import HTTPException

from app.api.audit import _read_failure_entries
from app.config import settings


@pytest.fixture
def audit_log_dir(tmp_path, monkeypatch):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    monkeypatch.setattr(settings, "LOG_DIR", str(log_dir))
    return log_dir


def test_rotation_race_skips_missing_file(audit_log_dir, monkeypatch):
    """exists 探测与 open 之间被轮转移走 (FileNotFoundError) -> 跳过该档文件, 其余照常返回"""
    good = audit_log_dir / "download_failures.jsonl"
    good.write_text(json.dumps({"type": "FLOOD_WAIT", "task_id": "t1"}) + "\n", encoding="utf-8")

    real_exists = os.path.exists

    def fake_exists(path):
        # 谎报 .1 档存在 (轮转竞态窗口), 实际不存在 -> open 抛 FileNotFoundError
        if str(path).endswith("download_failures.jsonl.1"):
            return True
        return real_exists(path)

    monkeypatch.setattr(os.path, "exists", fake_exists)
    entries = _read_failure_entries("download_failures.jsonl")
    assert [e["task_id"] for e in entries] == ["t1"], "轮转竞态消失的档文件必须跳过而非吞结果或报错"


def test_unreadable_file_raises_explicit_error(audit_log_dir):
    """非消失类读失败 (目录伪装档文件 -> open 抛 OSError) -> 显式 500, 不再静默吞成空结果"""
    good = audit_log_dir / "download_failures.jsonl"
    good.write_text(json.dumps({"type": "NETWORK_TIMEOUT", "task_id": "t2"}) + "\n", encoding="utf-8")
    (audit_log_dir / "download_failures.jsonl.1").mkdir()

    with pytest.raises(HTTPException) as exc:
        _read_failure_entries("download_failures.jsonl")
    assert exc.value.status_code == 500
    assert "审计日志读取失败" in exc.value.detail


def test_entries_newest_first(audit_log_dir):
    """滚动归档从旧到新读取 + 整体 reverse -> 严格最新在前"""
    arch = audit_log_dir / "download_failures.jsonl.1"
    arch.write_text(json.dumps({"type": "FLOOD_WAIT", "task_id": "old"}) + "\n", encoding="utf-8")
    base = audit_log_dir / "download_failures.jsonl"
    base.write_text(json.dumps({"type": "OTHER_ERROR", "task_id": "new"}) + "\n", encoding="utf-8")

    entries = _read_failure_entries("download_failures.jsonl")
    assert [e["task_id"] for e in entries] == ["new", "old"]


def test_audit_skips_malformed_and_non_dict_jsonl_lines(tmp_path, monkeypatch):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    monkeypatch.setattr(settings, "LOG_DIR", str(log_dir))
    f = log_dir / "download_failures.jsonl"
    f.write_text(
        "{not-json\n[1, 2, 3]\n" + json.dumps({"type": "FLOOD_WAIT", "task_id": "ok_1"}) + "\n",
        encoding="utf-8",
    )
    entries = _read_failure_entries("download_failures.jsonl")
    assert [e["task_id"] for e in entries] == ["ok_1"]


def test_audit_skips_recursion_error_json_line(tmp_path, monkeypatch):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    monkeypatch.setattr(settings, "LOG_DIR", str(log_dir))
    deep_line = "[" * 1500 + "]" * 1500
    f = log_dir / "download_failures.jsonl"
    f.write_text(
        deep_line + "\n" + json.dumps({"type": "FLOOD_WAIT", "task_id": "ok_after_deep"}) + "\n",
        encoding="utf-8",
    )
    entries = _read_failure_entries("download_failures.jsonl")
    assert [e["task_id"] for e in entries] == ["ok_after_deep"]
