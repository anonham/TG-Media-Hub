"""
Hypothesis 属性测试：app/core/intervals.py

覆盖：
1. merge_intervals 交换律 (Commutativity)
2. merge_intervals 结合律 (Associativity)
3. merge_intervals 幂等性 (Idempotence)
4. merge_intervals 严格不相交与升序不变式 (Disjointness & Strict Ordering)
5. merge_intervals 离散点集全等性 (Point-set Equivalence)
6. compute_gaps 零空洞丢失与窗口完美划分 (Zero Holes Lost / Partition Completeness)
7. runs_from_ids 离散 ID 聚合属性与可逆性 (包含密集连续 run 与稀疏离散 ID)
"""

from __future__ import annotations

import random

from hypothesis import given, settings
from hypothesis import strategies as st

from app.core.intervals import Interval, compute_gaps, merge_intervals, runs_from_ids

# 限制适度范围以保证点集转换与笛卡尔验证高效
coord_st = st.integers(min_value=-1000, max_value=1000)
interval_st = st.tuples(coord_st, coord_st)
interval_list_st = st.lists(interval_st, max_size=40)


@st.composite
def structured_interval_clusters_st(draw: st.DrawFn) -> list[Interval]:
    """
    针对性生成重叠、嵌套、边界相邻、端点差 1 与端点差 2 的结构化区间集群。
    """
    base_intervals = draw(st.lists(st.tuples(coord_st, coord_st), min_size=1, max_size=10))
    clusters: list[Interval] = []
    for raw_s, raw_e in base_intervals:
        s, e = (raw_e, raw_s) if raw_s > raw_e else (raw_s, raw_e)
        clusters.append((s, e))
        # 变异方式：嵌套、重叠、边界差 1 相邻、端点差 2
        mode = draw(st.sampled_from(["nested", "overlap_right", "adjacent_left", "adjacent_right", "disjoint"]))
        if mode == "nested" and (e - s >= 2):
            clusters.append((s + 1, e - 1))
        elif mode == "overlap_right":
            clusters.append((s + (e - s) // 2, e + draw(st.integers(1, 20))))
        elif mode == "adjacent_left":
            clusters.append((s - draw(st.integers(1, 20)), s - 1))
        elif mode == "adjacent_right":
            clusters.append((e + 1, e + draw(st.integers(1, 20))))
        elif mode == "disjoint":
            clusters.append((e + 2, e + draw(st.integers(2, 20))))
    return clusters


@given(segs=interval_list_st)
@settings(max_examples=100)
def test_merge_intervals_idempotent(segs: list[Interval]) -> None:
    """幂等性: merge(merge(L)) == merge(L)"""
    once = merge_intervals(segs)
    twice = merge_intervals(once)
    assert once == twice


@given(segs=interval_list_st)
@settings(max_examples=100)
def test_merge_intervals_commutative(segs: list[Interval]) -> None:
    """交换律: 乱序置换输入区间列表，合并结果完全不变"""
    res1 = merge_intervals(segs)
    shuffled = list(segs)
    random.shuffle(shuffled)
    res2 = merge_intervals(shuffled)
    assert res1 == res2


@given(a=interval_list_st, b=interval_list_st)
@settings(max_examples=100)
def test_merge_intervals_associative(a: list[Interval], b: list[Interval]) -> None:
    """结合律: merge(a + b) == merge(merge(a) + merge(b))"""
    res_together = merge_intervals(a + b)
    res_split = merge_intervals(merge_intervals(a) + merge_intervals(b))
    assert res_together == res_split


@given(segs=st.one_of(interval_list_st, structured_interval_clusters_st()))
@settings(max_examples=100)
def test_merge_intervals_invariants(segs: list[Interval]) -> None:
    """
    不变式验证：
    1. 结果每个区间均合法 s <= e
    2. 结果严格递增且不相邻（端点差 >= 2）
    """
    res = merge_intervals(segs)
    for i, (s, e) in enumerate(res):
        assert s <= e, f"Invalid interval: ({s}, {e})"
        if i > 0:
            prev_s, prev_e = res[i - 1]
            assert prev_s < s, f"Intervals not sorted: {res[i - 1]} vs {res[i]}"
            assert s > prev_e + 1, f"Intervals overlapping or adjacent (gap < 2): {res[i - 1]} vs {res[i]}"


@given(segs=st.one_of(interval_list_st, structured_interval_clusters_st()))
@settings(max_examples=80)
def test_merge_intervals_point_set_equivalence(segs: list[Interval]) -> None:
    """点集等价性: 输入区间内的离散整数全集 与 合并后区间的离散整数全集 严格相等"""
    expected_points: set[int] = set()
    for s, e in segs:
        if e >= s:
            expected_points.update(range(s, e + 1))

    merged = merge_intervals(segs)
    actual_points: set[int] = set()
    for s, e in merged:
        actual_points.update(range(s, e + 1))

    assert actual_points == expected_points


@given(segs=st.one_of(interval_list_st, structured_interval_clusters_st()), w_start=coord_st, w_end=coord_st)
@settings(max_examples=100)
def test_compute_gaps_zero_holes_lost(segs: list[Interval], w_start: int, w_end: int) -> None:
    """
    零空洞丢失与窗口完美划分 (Partition Completeness):
    在窗口 [w_start, w_end] 内：
    1. 每个 gap 均位于窗口内且合法
    2. gaps 与输入区间交集为空 (无重叠)
    3. gaps 与输入区间并集完全覆盖 [w_start, w_end] (零遗漏)
    """
    gaps = compute_gaps(segs, w_start, w_end)

    if w_end < w_start:
        assert gaps == []
        return

    window_points = set(range(w_start, w_end + 1))
    covered_in_window: set[int] = set()
    for s, e in segs:
        if e >= s:
            clamped_s = max(s, w_start)
            clamped_e = min(e, w_end)
            if clamped_e >= clamped_s:
                covered_in_window.update(range(clamped_s, clamped_e + 1))

    gap_points: set[int] = set()
    for gs, ge in gaps:
        assert w_start <= gs <= ge <= w_end, f"Gap ({gs}, {ge}) out of window [{w_start}, {w_end}]"
        points = set(range(gs, ge + 1))
        # 验证 gaps 之间互斥
        assert gap_points.isdisjoint(points), "Gaps overlap with each other"
        gap_points.update(points)

    # 验证 gaps 与已覆盖区域严格互斥 (无重叠)
    assert gap_points.isdisjoint(covered_in_window), "Gaps overlap with covered segments"

    # 验证并集等于整个窗口 (零空洞丢失)
    assert gap_points | covered_in_window == window_points, (
        f"Window partition incomplete: lost {window_points - (gap_points | covered_in_window)}"
    )


@given(
    ids=st.one_of(
        st.lists(st.integers(min_value=-500, max_value=500), max_size=50),
        st.lists(st.integers(min_value=-100, max_value=100), min_size=2, max_size=30).map(
            lambda l: sorted(set(l + [x + 1 for x in l]))
        ),
    )
)
@settings(max_examples=100)
def test_runs_from_ids_properties(ids: list[int]) -> None:
    """runs_from_ids 属性验证: 结果自满足 merge_intervals 不变式，且完全展开回原 ID 集合"""
    runs = runs_from_ids(ids)
    assert merge_intervals(runs) == runs

    restored_ids: set[int] = set()
    for s, e in runs:
        restored_ids.update(range(s, e + 1))
    assert restored_ids == set(ids)
