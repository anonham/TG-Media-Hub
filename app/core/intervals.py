"""区间纯函数工具：覆盖区间合并 / 未覆盖 gap 计算 / 离散 ID 聚合为连续 run"""

from collections.abc import Iterable

Interval = tuple[int, int]


def merge_intervals(segs: Iterable[Interval]) -> list[Interval]:
    """
    合并重叠或相邻(端点差 1)的区间，返回升序不相交区间列表。
    例: [(1,10),(11,20),(30,40)] -> [(1,20),(30,40)]
    """
    items = sorted((int(s), int(e)) for s, e in segs if e >= s)
    merged: list[Interval] = []
    for s, e in items:
        if merged and s <= merged[-1][1] + 1:
            if e > merged[-1][1]:
                merged[-1] = (merged[-1][0], e)
        else:
            merged.append((s, e))
    return merged


def compute_gaps(segs: Iterable[Interval], w_start: int, w_end: int) -> list[Interval]:
    """
    计算窗口 [w_start, w_end] 内未被任何区间覆盖的 gap 列表(升序)。
    窗口外的区间部分被裁剪；w_end < w_start 时返回空。
    """
    if w_end < w_start:
        return []
    merged = merge_intervals((max(s, w_start), min(e, w_end)) for s, e in segs if e >= w_start and s <= w_end)
    gaps: list[Interval] = []
    cur = w_start
    for s, e in merged:
        if s > cur:
            gaps.append((cur, s - 1))
        cur = max(cur, e + 1)
    if cur <= w_end:
        gaps.append((cur, w_end))
    return gaps


def runs_from_ids(ids: Iterable[int]) -> list[Interval]:
    """
    将离散升序 ID 聚合为连续 run(相邻 ID 差 1 视为连续)，用于覆盖条渲染。
    输入无需有序、自动去重；返回升序区间列表。
    """
    unique: set[int] = {int(i) for i in ids}
    ordered = sorted(unique)
    runs: list[Interval] = []
    for i in ordered:
        if runs and i == runs[-1][1] + 1:
            runs[-1] = (runs[-1][0], i)
        else:
            runs.append((i, i))
    return runs
