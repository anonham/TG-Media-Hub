"""ForwardLedgerRepository 单元测试: 三表单事务原子性 / 幂等 / NULL target 去重 / 防重查询语义"""

import sqlite3

import pytest

from app.db.database import get_db
from app.db.forward_repository import ForwardHistoryRepository, ForwardLedgerRepository

SRC = "-1009000000010"
TGT_A = "-1009000000003"
TGT_B = "-1009000000026"


def _count_history() -> int:
    conn = get_db()
    n = conn.execute("SELECT COUNT(*) FROM forward_history").fetchone()[0]
    conn.close()
    return n


def _count_skips() -> int:
    conn = get_db()
    n = conn.execute("SELECT COUNT(*) FROM forward_skips").fetchone()[0]
    conn.close()
    return n


def _count_coverage() -> int:
    conn = get_db()
    n = conn.execute("SELECT COUNT(*) FROM forward_coverage").fetchone()[0]
    conn.close()
    return n


def _seed_processed():
    """预置: history 1,2 (->A)；skip: 1 服务动作(NULL), 3 bad_id(->A), 4 bad_id(->B), 5 ghost(->B)"""
    ForwardHistoryRepository.save_forward_batch(
        [
            (None, SRC, 1, TGT_A, 9001),
            (None, SRC, 2, TGT_A, 9002),
        ]
    )
    ForwardLedgerRepository.commit_batch_records(
        task_id=None,
        source_chat=SRC,
        target_chat=TGT_A,
        mappings=[],
        skips=[
            (1, "service_action", None, None),
            (3, "bad_id", "msg invalid", TGT_A),
            (4, "bad_id", "msg invalid", TGT_B),
            (5, "ghost_no_content", None, TGT_B),
        ],
    )


def test_commit_batch_records_writes_three_tables():
    ForwardLedgerRepository.commit_batch_records(
        task_id="t1",
        source_chat=SRC,
        target_chat=TGT_A,
        mappings=[(10, 9100), (11, 9101)],
        skips=[(9, "service_action", None, None)],
        seg_start=9,
        seg_end=11,
    )
    assert _count_history() == 2
    assert _count_skips() == 1
    assert _count_coverage() == 1
    conn = get_db()
    row = conn.execute(
        "SELECT seg_start, seg_end FROM forward_coverage WHERE source_chat = ? AND target_chat = ?", (SRC, TGT_A)
    ).fetchone()
    conn.close()
    assert (row["seg_start"], row["seg_end"]) == (9, 11)


def test_commit_atomicity_all_or_nothing():
    """覆盖区间表缺失(写入必失败)时，同事务的 history/skips 一并回滚"""
    conn = get_db()
    conn.execute("DROP TABLE forward_coverage")
    conn.commit()
    conn.close()

    with pytest.raises(sqlite3.OperationalError):
        ForwardLedgerRepository.commit_batch_records(
            task_id="t2",
            source_chat=SRC,
            target_chat=TGT_A,
            mappings=[(20, 9200)],
            skips=[(19, "ghost_no_content", None, None)],
            seg_start=19,
            seg_end=20,
        )
    assert _count_history() == 0
    assert _count_skips() == 0


def test_commit_idempotent_double_write():
    args = dict(
        task_id="t3",
        source_chat=SRC,
        target_chat=TGT_A,
        mappings=[(30, 9300), (31, 9301)],
        skips=[(29, "service_action", None, None)],
        seg_start=29,
        seg_end=31,
    )
    ForwardLedgerRepository.commit_batch_records(**args)
    ForwardLedgerRepository.commit_batch_records(**args)
    assert _count_history() == 2  # OR REPLACE 幂等
    assert _count_skips() == 1  # OR IGNORE 首条留痕
    assert _count_coverage() == 1  # 同区间合并不膨胀


def test_skip_null_target_unique_dedup():
    ForwardLedgerRepository.commit_batch_records(
        task_id=None,
        source_chat=SRC,
        target_chat=TGT_A,
        mappings=[],
        skips=[(40, "service_action", None, None), (40, "service_action", None, None)],
        seg_start=None,
        seg_end=None,
    )
    assert _count_skips() == 1
    # 同 msg 同 type 但 target 不同 -> 独立两行
    ForwardLedgerRepository.commit_batch_records(
        task_id=None,
        source_chat=SRC,
        target_chat=TGT_A,
        mappings=[],
        skips=[(41, "bad_id", "x", TGT_A), (41, "bad_id", "x", TGT_B)],
        seg_start=None,
        seg_end=None,
    )
    assert _count_skips() == 3


def test_get_processed_ids_semantics():
    _seed_processed()
    done_a = ForwardLedgerRepository.get_processed_ids(SRC, TGT_A, [1, 2, 3, 4, 5, 6])
    # 1,2 history(->A)；1 NULL 过滤 skip；3 bad_id(->A)；4 bad_id(->B) 不算；5 ghost(->B) 不算；6 未处理
    assert done_a == {1, 2, 3}

    done_b = ForwardLedgerRepository.get_processed_ids(SRC, TGT_B, [1, 2, 3, 4, 5, 6])
    # NULL 过滤 skip 跨目标复用；bad_id/显式 target skip 仅对同目标生效
    assert done_b == {1, 4, 5}

    assert ForwardLedgerRepository.get_processed_ids(SRC, TGT_A, []) == set()


def test_coverage_merge_converges_across_commits():
    ForwardLedgerRepository.commit_batch_records(None, SRC, TGT_A, mappings=[], skips=[], seg_start=10, seg_end=20)
    ForwardLedgerRepository.commit_batch_records(
        None, SRC, TGT_A, mappings=[], skips=[], seg_start=21, seg_end=30
    )  # 相邻合并
    assert _count_coverage() == 1
    ForwardLedgerRepository.commit_batch_records(None, SRC, TGT_A, mappings=[], skips=[], seg_start=100, seg_end=110)
    assert _count_coverage() == 2
    ForwardLedgerRepository.commit_batch_records(
        None, SRC, TGT_A, mappings=[], skips=[], seg_start=25, seg_end=105
    )  # 桥接两段
    assert _count_coverage() == 1

    conn = get_db()
    row = conn.execute(
        "SELECT seg_start, seg_end FROM forward_coverage WHERE source_chat = ? AND target_chat = ?", (SRC, TGT_A)
    ).fetchone()
    conn.close()
    assert (row["seg_start"], row["seg_end"]) == (10, 110)


def test_get_ledger_bounds_empty_returns_none():
    assert ForwardLedgerRepository.get_ledger_bounds(SRC, TGT_A) is None


def test_get_ledger_bounds_merges_history_and_skips():
    """history 100..300 (->A) + skips 50(NULL target), 400(bad_id->A): 边界取两表并集极值"""
    ForwardHistoryRepository.save_forward_batch(
        [
            (None, SRC, 100, TGT_A, 9100),
            (None, SRC, 300, TGT_A, 9300),
        ]
    )
    ForwardLedgerRepository.commit_batch_records(
        task_id=None,
        source_chat=SRC,
        target_chat=TGT_A,
        mappings=[],
        skips=[
            (50, "service_action", None, None),
            (400, "bad_id", "msg invalid", TGT_A),
        ],
    )
    assert ForwardLedgerRepository.get_ledger_bounds(SRC, TGT_A) == (50, 400)


def test_get_ledger_bounds_matches_get_ledger_runs_extremes():
    """与 get_ledger_runs 无窗口语义严格等价: 边界 == 三类列表并集的 (min, max);
    同时覆盖单侧表为空分支 (A 为 history+skip 混合, B 仅 skips 无 history)"""
    _seed_processed()
    for tgt in (TGT_A, TGT_B):
        ledger = ForwardLedgerRepository.get_ledger_runs(SRC, tgt)
        all_ids = ledger["forwarded"] + ledger["filtered"] + ledger["bad"]
        assert all_ids
        assert ForwardLedgerRepository.get_ledger_bounds(SRC, tgt) == (min(all_ids), max(all_ids))
