"""转发覆盖区间 API 与快路径测试: coverage/gaps 端点 / compute_resume_start / resume 分支 / 并发预检"""

import asyncio
import time
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.api.auth import issue_2fa_token
from app.config import settings
from app.db.forward_repository import ForwardHistoryRepository, ForwardLedgerRepository
from app.db.task_repository import TaskQueueRepository
from app.main import app
from app.services.forward_service import ForwardService

client = TestClient(app, client=("127.0.0.1", 50000))

SRC = "-1009000000010"
TGT = "-100900037"


def auth_headers():
    headers = {}
    if settings.API_KEY:
        headers["X-API-Key"] = settings.API_KEY
        token = issue_2fa_token()
        if token:
            headers["X-2FA-Token"] = token
    return headers


def _seed_ledger():
    """history: 1,2,3 (->TGT)；skip: 4 服务动作(NULL)、6 bad_id(->TGT)；coverage: [1,10]"""
    ForwardHistoryRepository.save_forward_batch(
        [
            (None, SRC, 1, TGT, 9001),
            (None, SRC, 2, TGT, 9002),
            (None, SRC, 3, TGT, 9003),
        ]
    )
    ForwardLedgerRepository.commit_batch_records(
        task_id=None,
        source_chat=SRC,
        target_chat=TGT,
        mappings=[],
        skips=[
            (4, "service_action", None, None),
            (6, "bad_id", "msg invalid", TGT),
        ],
        seg_start=1,
        seg_end=10,
    )


# =========================================================================
# compute_resume_start 四分支
# =========================================================================


def test_resume_start_fallback_without_coverage():
    resume_start, mode = ForwardService.compute_resume_start(SRC, TGT, 100, 500)
    assert (resume_start, mode) == (100, "fallback")


def test_resume_start_first_gap_left_edge():
    ForwardLedgerRepository.commit_batch_records(None, SRC, TGT, mappings=[], skips=[], seg_start=10, seg_end=20)
    resume_start, mode = ForwardService.compute_resume_start(SRC, TGT, 1, 100)
    assert mode == "gap"
    assert resume_start == 1  # 首个 gap = [1,9]

    ForwardLedgerRepository.commit_batch_records(None, SRC, TGT, mappings=[], skips=[], seg_start=1, seg_end=40)
    resume_start, mode = ForwardService.compute_resume_start(SRC, TGT, 1, 100)
    assert (resume_start, mode) == (41, "gap")  # 首个 gap 左端 = 已覆盖末尾 + 1


def test_resume_start_covered_when_range_full():
    ForwardLedgerRepository.commit_batch_records(None, SRC, TGT, mappings=[], skips=[], seg_start=1, seg_end=100)
    resume_start, mode = ForwardService.compute_resume_start(SRC, TGT, 1, 100)
    assert (resume_start, mode) == (None, "covered")


def test_resume_start_open_end_treats_beyond_as_gap():
    """end 缺省(整频道任务): 覆盖段之后的消息视为未覆盖，不能误判 covered"""
    ForwardLedgerRepository.commit_batch_records(None, SRC, TGT, mappings=[], skips=[], seg_start=1, seg_end=100)
    resume_start, mode = ForwardService.compute_resume_start(SRC, TGT, 1, None)
    assert mode == "gap"
    assert resume_start == 101


# =========================================================================
# GET /api/forward/coverage
# =========================================================================


def test_coverage_endpoint_aggregates_runs():
    _seed_ledger()
    resp = client.get("/api/forward/coverage", params={"source_chat": SRC, "target_chat": TGT}, headers=auth_headers())
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["covered"] == [{"start": 1, "end": 10}]
    assert data["forwarded_runs"] == [{"start": 1, "end": 3, "count": 3}]
    assert data["filtered_runs"] == [{"start": 4, "end": 4, "count": 1}]
    assert data["bad_runs"] == [{"start": 6, "end": 6, "count": 1}]
    assert data["stats"] == {"forwarded": 3, "filtered": 1, "bad": 1}
    assert data["truncated"] is False


def test_coverage_endpoint_empty_ledger():
    resp = client.get("/api/forward/coverage", params={"source_chat": SRC, "target_chat": TGT}, headers=auth_headers())
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["range"] is None
    assert data["covered"] == []
    assert data["stats"] == {"forwarded": 0, "filtered": 0, "bad": 0}


def test_coverage_no_window_uses_ledger_bounds_beyond_segments(monkeypatch):
    """P1 回归: 无窗口请求以 SQL 边界扩展 range, 且仅触发一次带窗口的 get_ledger_runs (无界预取已移除)"""
    _seed_ledger()
    # 账本记录超出 coverage 段 ([1,10]): history 999 -> 无窗口 range 应扩展到账本边界
    ForwardHistoryRepository.save_forward_batch([(None, SRC, 999, TGT, 9999)])

    calls = []
    real_runs = ForwardLedgerRepository.get_ledger_runs

    def runs_spy(source_chat, target_chat, *args):
        calls.append(args)
        return real_runs(source_chat, target_chat, *args)

    monkeypatch.setattr(ForwardLedgerRepository, "get_ledger_runs", runs_spy)
    resp = client.get("/api/forward/coverage", params={"source_chat": SRC, "target_chat": TGT}, headers=auth_headers())
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["range"] == {"min_id": 1, "max_id": 999}
    assert calls == [(1, 999)]  # 仅一次窗口查询 (w_min=1, w_max=999), 无界预取已移除


def test_coverage_no_window_matches_explicit_window():
    """无窗口响应与以 range 回填的显式窗口响应逐项相等 (边界 SQL 化后的行为等价锁)"""
    _seed_ledger()
    base = client.get(
        "/api/forward/coverage", params={"source_chat": SRC, "target_chat": TGT}, headers=auth_headers()
    ).json()["data"]
    assert base["range"] is not None
    windowed = client.get(
        "/api/forward/coverage",
        params={
            "source_chat": SRC,
            "target_chat": TGT,
            "min_id": base["range"]["min_id"],
            "max_id": base["range"]["max_id"],
        },
        headers=auth_headers(),
    ).json()["data"]
    assert windowed["forwarded_runs"] == base["forwarded_runs"]
    assert windowed["filtered_runs"] == base["filtered_runs"]
    assert windowed["bad_runs"] == base["bad_runs"]
    assert windowed["stats"] == base["stats"]
    assert windowed["truncated"] == base["truncated"]


# =========================================================================
# GET /api/forward/coverage/gaps
# =========================================================================


def test_gaps_endpoint_sorted_by_length_desc():
    ForwardLedgerRepository.commit_batch_records(None, SRC, TGT, mappings=[], skips=[], seg_start=1, seg_end=10)
    ForwardLedgerRepository.commit_batch_records(None, SRC, TGT, mappings=[], skips=[], seg_start=50, seg_end=60)
    resp = client.get(
        "/api/forward/coverage/gaps",
        params={"source_chat": SRC, "target_chat": TGT, "min_id": 1, "max_id": 100},
        headers=auth_headers(),
    )
    assert resp.status_code == 200
    data = resp.json()["data"]
    # gap: [61,100] (40) > [11,49] (39)，按长度降序
    assert data["gaps"] == [{"start": 61, "end": 100, "est_count": 40}, {"start": 11, "end": 49, "est_count": 39}]
    assert data["total_est_count"] == 79


def test_gaps_endpoint_empty_channel():
    resp = client.get(
        "/api/forward/coverage/gaps",
        params={"source_chat": SRC, "target_chat": TGT, "min_id": 1, "max_id": 100},
        headers=auth_headers(),
    )
    assert resp.status_code == 200
    assert resp.json()["data"]["gaps"] == [{"start": 1, "end": 100, "est_count": 100}]


# =========================================================================
# POST /api/forward/estimate
# =========================================================================


def test_estimate_with_explicit_range():
    _seed_ledger()  # history 1,2,3 + skip 4, 6 -> 已处理 5 条
    resp = client.post(
        "/api/forward/estimate",
        json={"source_chat": SRC, "target_chat": TGT, "start_id": 1, "end_id": 100, "batch_size": 30},
        headers=auth_headers(),
    )
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["span_count"] == 100
    assert data["already_processed"] == 5
    assert data["to_process"] == 95
    assert data["est_batches"] == 4  # ceil(95/30)
    assert data["inspected_total"] is None  # 显式起止未触发实测


def test_estimate_rejects_invalid_range():
    resp = client.post(
        "/api/forward/estimate",
        json={"source_chat": SRC, "target_chat": TGT, "start_id": 100, "end_id": 1, "batch_size": 30},
        headers=auth_headers(),
    )
    assert resp.status_code == 400


# =========================================================================
# GET /api/forward/receipt/{task_id}
# =========================================================================


def test_receipt_endpoint():
    task_id = TaskQueueRepository.create(
        task_id="receipttest1", task_type="forward_channel", params={"source_chat": SRC, "target_chat": TGT}
    )["id"]
    TaskQueueRepository.update_status(task_id, "completed")
    ForwardLedgerRepository.commit_batch_records(
        task_id=task_id,
        source_chat=SRC,
        target_chat=TGT,
        mappings=[(1, 9001), (2, 9002)],
        skips=[
            (3, "service_action", None, None),
            (4, "service_action", None, None),
            (5, "bad_id", "msg invalid", TGT),
        ],
        seg_start=1,
        seg_end=5,
    )
    TaskQueueRepository.merge_params(
        task_id, {"final_summary": {"forwarded": 2, "filtered": 2, "bad": 1, "dedup_skipped": 7}}
    )

    resp = client.get(f"/api/forward/receipt/{task_id}", headers=auth_headers())
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["forwarded"] == 2
    assert data["filtered"] == 2
    assert data["bad"] == 1
    assert data["dedup_skipped"] == 7
    assert data["bad_ids"] == [5]
    assert data["filtered_detail"] == [{"skip_type": "service_action", "count": 2, "sample_ids": [3, 4]}]


def test_receipt_falls_back_to_db_counts_without_final_summary():
    task_id = TaskQueueRepository.create(
        task_id="receipttest2", task_type="forward_channel", params={"source_chat": SRC, "target_chat": TGT}
    )["id"]
    ForwardLedgerRepository.commit_batch_records(
        task_id=task_id,
        source_chat=SRC,
        target_chat=TGT,
        mappings=[(10, 9010)],
        skips=[(11, "ghost_no_content", None, None)],
        seg_start=10,
        seg_end=11,
    )
    resp = client.get(f"/api/forward/receipt/{task_id}", headers=auth_headers())
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["forwarded"] == 1
    assert data["filtered"] == 1
    assert data["bad"] == 0
    assert data["dedup_skipped"] is None


def test_receipt_task_not_found():
    resp = client.get("/api/forward/receipt/nonexistent00", headers=auth_headers())
    assert resp.status_code == 404


# =========================================================================
# POST /api/tasks/{id}/resume (forward_channel 分支)
# =========================================================================


def _create_forward_task(status="cancelled", params=None):
    task_id = TaskQueueRepository.create(
        task_id="resumetest1",
        task_type="forward_channel",
        params=params
        or {
            "source_chat": SRC,
            "target_chat": TGT,
            "start_id": 1,
            "end_id": 100,
            "batch_size": 30,
            "delay_seconds": 0.2,
            "full_retransfer": False,
        },
    )["id"]
    TaskQueueRepository.update_status(task_id, status)
    return task_id


def test_resume_forward_dispatches_from_gap_left_edge():
    ForwardLedgerRepository.commit_batch_records(None, SRC, TGT, mappings=[], skips=[], seg_start=1, seg_end=40)
    task_id = _create_forward_task()

    resp = client.post(f"/api/tasks/{task_id}/resume", headers=auth_headers())
    assert resp.status_code == 200
    assert resp.json()["status"] == "queued"
    # D4: 断点补丁 (gap 左端 = 40 + 1) 持久化进 params 后统一入队, 由调度器拾起
    task = TaskQueueRepository.get_by_id(task_id)
    assert task["params"]["start_id"] == 41
    assert task["params"]["end_id"] == 100
    assert task["status"] == "queued"


def test_resume_forward_covered_completes_immediately():
    ForwardLedgerRepository.commit_batch_records(None, SRC, TGT, mappings=[], skips=[], seg_start=1, seg_end=100)
    task_id = _create_forward_task()
    resp = client.post(f"/api/tasks/{task_id}/resume", headers=auth_headers())
    assert resp.status_code == 200
    assert resp.json()["status"] == "completed"
    assert TaskQueueRepository.get_by_id(task_id)["status"] == "completed"


def test_resume_forward_missing_params_rejected():
    task_id = _create_forward_task(params={"source_chat": SRC})
    resp = client.post(f"/api/tasks/{task_id}/resume", headers=auth_headers())
    assert resp.status_code == 400


def test_resume_forward_legacy_params_supported():
    """旧版本任务的 params 无 full_retransfer 键，可正常恢复且默认增量模式"""
    ForwardLedgerRepository.commit_batch_records(None, SRC, TGT, mappings=[], skips=[], seg_start=1, seg_end=40)
    legacy_params = {
        "source_chat": SRC,
        "target_chat": TGT,
        "start_id": 1,
        "end_id": 100,
        "batch_size": 50,
        "delay_seconds": 1.5,
    }
    task_id = _create_forward_task(params=legacy_params)

    resp = client.post(f"/api/tasks/{task_id}/resume", headers=auth_headers())
    assert resp.status_code == 200
    # D4: 断点补丁持久化进 params (start_id 推进到 gap 左端); dry_run 恢复置 False (增量模式)
    # full_retransfer 键缺省不入参 (增量模式)
    task = TaskQueueRepository.get_by_id(task_id)
    assert task["params"]["start_id"] == 41
    assert "full_retransfer" not in task["params"]
    assert task["params"]["dry_run"] is False


# =========================================================================
# POST /api/forward/channel 并发预检
# =========================================================================


def test_channel_rejects_duplicate_active_pair():
    _create_forward_task(status="running")

    resp = client.post(
        "/api/forward/channel",
        json={"source_chat": SRC, "target_chat": TGT, "batch_size": 30, "delay_seconds": 1.5},
        headers=auth_headers(),
    )
    assert resp.status_code == 400
    assert "已有运行中" in resp.json()["detail"]


def test_channel_allows_same_source_different_target():
    """D4: 派发统一 —— 任务落库后入队等待调度器 (调度器未运行时保持 queued)"""
    _create_forward_task(status="running")

    resp = client.post(
        "/api/forward/channel",
        json={"source_chat": SRC, "target_chat": "-1009000000042", "batch_size": 30, "delay_seconds": 1.5},
        headers=auth_headers(),
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "success"
    # D4: 任务落库 (create 默认 pending) 等待调度器拾起; 调度器未运行时不直跑
    task = TaskQueueRepository.get_by_id(resp.json()["task_id"])
    assert task["status"] == "pending"
    assert task["params"]["target_chat"] == "-1009000000042"


def test_channel_rejects_delay_seconds_below_minimum_0_8():
    """需求 E: ForwardChannelRequest.delay_seconds 校验下限提升至 ge=0.8，传入 < 0.8 返回 422"""
    for invalid_delay in (0.1, 0.2, 0.5, 0.79):
        resp = client.post(
            "/api/forward/channel",
            json={
                "source_chat": SRC,
                "target_chat": "-1009000000040",
                "batch_size": 30,
                "delay_seconds": invalid_delay,
            },
            headers=auth_headers(),
        )
        assert resp.status_code == 422, f"delay_seconds={invalid_delay} 应触发 422 校验失败"

    resp_ok = client.post(
        "/api/forward/channel",
        json={"source_chat": SRC, "target_chat": "-1009000000041", "batch_size": 30, "delay_seconds": 0.8},
        headers=auth_headers(),
    )
    assert resp_ok.status_code == 200


@pytest.mark.asyncio
async def test_forward_channel_concurrent_submit_allows_only_one():
    created_ids: list[str] = []
    orig_create = TaskQueueRepository.create

    def slow_create(*args, **kwargs):
        time.sleep(0.05)
        res = orig_create(*args, **kwargs)
        created_ids.append(res["id"])
        return res

    from app.api.forward import forward_channel
    from app.models.forward import ForwardChannelRequest

    req = ForwardChannelRequest(source_chat="-100900004", target_chat="-100900028")
    with patch.object(TaskQueueRepository, "create", side_effect=slow_create):
        results = await asyncio.gather(forward_channel(req), forward_channel(req), return_exceptions=True)

    try:
        successes = [r for r in results if isinstance(r, dict)]
        failures = [r for r in results if isinstance(r, Exception)]
        assert len(successes) == 1
        assert len(failures) == 1
        winner = successes[0]["task_id"]
        assert created_ids == [winner]
        rejection = failures[0]
        assert isinstance(rejection, HTTPException)
        assert rejection.status_code == 400
        assert "已有运行中/暂停中的转发任务" in rejection.detail
        assert winner in rejection.detail
    finally:
        for tid in created_ids:
            TaskQueueRepository.delete(tid)


def test_forward_coverage_explicit_bounds_returns_ledger_only_runs():
    src, tgt = "-100900033", "-100900036"
    ForwardHistoryRepository.save_forward_batch([(None, src, 5, tgt, 5005), (None, src, 6, tgt, 5006)])

    resp = client.get(
        "/api/forward/coverage",
        params={"source_chat": src, "target_chat": tgt, "min_id": 1, "max_id": 10},
        headers=auth_headers(),
    )
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["range"] == {"min_id": 1, "max_id": 10}
    assert data["forwarded_runs"] == [{"start": 5, "end": 6, "count": 2}]
    assert data["stats"]["forwarded"] == 2


def test_forward_coverage_reversed_inferred_bounds_returns_empty():
    src, tgt = "-100900047", "-100900048"
    ForwardLedgerRepository.commit_batch_records(None, src, tgt, mappings=[], skips=[], seg_start=100, seg_end=200)

    resp = client.get(
        "/api/forward/coverage",
        params={"source_chat": src, "target_chat": tgt, "min_id": 300},
        headers=auth_headers(),
    )
    assert resp.status_code == 200
    assert resp.json()["data"]["range"] is None
    assert resp.json()["data"]["covered"] == []


def test_forward_api_duplicate_guard_rejects_same_username_after_canonical_pinning():
    ForwardLedgerRepository.upsert_peer_aliases(["src_alias"], "-1009000000017")
    ForwardLedgerRepository.upsert_peer_aliases(["tgt_alias"], "-1009000000018")
    TaskQueueRepository.create(
        "paused_canon_1",
        "forward_channel",
        {
            "source_chat": "@src_alias",
            "target_chat": "@tgt_alias",
            "canonical_source": "-1009000000017",
            "canonical_target": "-1009000000018",
        },
    )
    TaskQueueRepository.update_status("paused_canon_1", "paused")
    try:
        resp_alias = client.post(
            "/api/forward/channel",
            json={"source_chat": "@SRC_ALIAS", "target_chat": "https://t.me/tgt_alias"},
            headers=auth_headers(),
        )
        assert resp_alias.status_code == 400
        assert "paused_canon_1" in resp_alias.json()["detail"]

        resp_num = client.post(
            "/api/forward/channel",
            json={"source_chat": "-1009000000017", "target_chat": "-1009000000018"},
            headers=auth_headers(),
        )
        assert resp_num.status_code == 400
    finally:
        TaskQueueRepository.delete("paused_canon_1")


@pytest.mark.parametrize(
    ("page", "limit", "expected"), [(0, 1, 422), (1, -1, 422), (1, 501, 422), (1, 500, 200), (2**63, 1, 200)]
)
def test_forward_history_pagination_bounds(page, limit, expected):
    response = client.get("/api/forward/history", params={"page": page, "limit": limit}, headers=auth_headers())
    assert response.status_code == expected


@pytest.mark.asyncio
async def test_forward_history_canonicalizes_aliases_off_event_loop(monkeypatch):
    import threading

    from app.api.forward import get_forward_history
    from app.services.forward_console_service import ForwardConsoleService

    loop_tid = threading.get_ident()
    canon_threads: list[int] = []
    query_threads: list[int] = []

    def fake_canon(val):
        canon_threads.append(threading.get_ident())
        return f"canon:{val}"

    def fake_query(**_kwargs):
        query_threads.append(threading.get_ident())
        return []

    monkeypatch.setattr(ForwardService, "canonicalize_chat_key", staticmethod(fake_canon))
    monkeypatch.setattr(ForwardConsoleService, "query_history", staticmethod(fake_query))

    res = await get_forward_history(source_chat="@src_alias", target_chat="@tgt_alias")
    assert res == {"status": "success", "count": 0, "data": []}
    assert len(canon_threads) == 2 and all(t != loop_tid for t in canon_threads)
    assert len(query_threads) == 1 and query_threads[0] == canon_threads[0]
