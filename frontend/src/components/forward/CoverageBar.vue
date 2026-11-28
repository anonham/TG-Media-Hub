<template>
  <div class="coverage-bar-wrap">
    <div class="coverage-scale">
      <span>#{{ Number(rangeStart).toLocaleString() }}</span>
      <span class="coverage-hint">{{ coverageHint }}</span>
      <span>#{{ Number(rangeEnd).toLocaleString() }}</span>
    </div>
    <div class="coverage-bar" :class="{ 'is-loading': loading }">
      <div
        v-for="seg in segments"
        :key="`${seg.start}-${seg.end}`"
        class="coverage-seg"
        :class="`seg-${seg.kind}`"
        :style="{ flexGrow: seg.span }"
        role="button"
        tabindex="0"
        :aria-label="`#${seg.start} – #${seg.end} ×${seg.span.toLocaleString()} (${kindLabel(seg.kind)})`"
        :title="`#${seg.start} – #${seg.end} ×${seg.span.toLocaleString()} (${kindLabel(seg.kind)})`"
        @click="$emit('select', seg.start, seg.end)"
        @keydown.enter="$emit('select', seg.start, seg.end)"
        @keydown.space.prevent
        @keyup.space="$emit('select', seg.start, seg.end)"
      />
    </div>
    <div class="coverage-legend">
      <span class="legend-item"><i class="swatch swatch-forwarded"></i>已转发</span>
      <span class="legend-item"><i class="swatch swatch-filtered"></i>已过滤</span>
      <span class="legend-item"><i class="swatch swatch-bad"></i>坏消息隔离</span>
      <span class="legend-item"><i class="swatch swatch-covered-empty"></i>已处理(无内容)</span>
      <span class="legend-item"><i class="swatch swatch-uncovered"></i>未处理</span>
      <span v-if="truncated || segmentsTruncated" class="legend-item legend-warn">段过多已截断显示</span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

export interface IntervalItem {
  start?: number | string
  end?: number | string
  start_id?: number | string
  end_id?: number | string
  count?: number | null
}

export interface SegmentItem {
  start: number
  end: number
  span: number
  kind: string
}

const props = withDefaults(
  defineProps<{
    rangeStart?: number | null
    rangeEnd?: number | null
    coverage?: IntervalItem[]
    forwardedRuns?: IntervalItem[]
    filteredRuns?: IntervalItem[]
    badRuns?: IntervalItem[]
    loading?: boolean
    truncated?: boolean
  }>(),
  {
    rangeStart: null,
    rangeEnd: null,
    coverage: () => [],
    forwardedRuns: () => [],
    filteredRuns: () => [],
    badRuns: () => [],
    loading: false,
    truncated: false,
  },
)

defineEmits<{
  (e: 'select', start: number, end: number): void
}>()

const KIND_LABELS: Record<string, string> = {
  forwarded: '已转发',
  filtered: '已过滤',
  bad: '坏消息隔离',
  'covered-empty': '已处理·无内容/已删除',
  uncovered: '未处理',
}

function kindLabel(kind: string): string {
  return KIND_LABELS[kind] || kind
}

const MAX_SEGMENTS = 4000

// 裁剪至 [rs, re] 并按起点排序合并相邻/重叠区间 -> 严格不相交升序区间序列
function normalizeIntervals(rawList: IntervalItem[] | undefined, rs: number, re: number): [number, number][] {
  const clipped: [number, number][] = []
  for (const item of rawList || []) {
    const rawS = item.start ?? item.start_id
    const rawE = item.end ?? item.end_id
    const s = Math.max(Number(rawS), rs)
    const e = Math.min(Number(rawE), re)
    if (Number.isFinite(s) && Number.isFinite(e) && e >= s) {
      clipped.push([s, e])
    }
  }
  if (clipped.length <= 1) return clipped
  clipped.sort((a, b) => a[0] - b[0] || a[1] - b[1])
  const merged: [number, number][] = [clipped[0]]
  for (let i = 1; i < clipped.length; i++) {
    const prev = merged[merged.length - 1]
    const cur = clipped[i]
    if (cur[0] <= prev[1] + 1) {
      if (cur[1] > prev[1]) prev[1] = cur[1]
    } else {
      merged.push(cur)
    }
  }
  return merged
}

// 按优先级判定区间类型: forwarded > bad > filtered > covered-empty > uncovered
const KIND_PRIORITY: Record<string, number> = {
  uncovered: 0,
  'covered-empty': 1,
  filtered: 2,
  bad: 3,
  forwarded: 4,
}

// 将 run 列表与覆盖区间合成为互不重叠的分段序列 (排序 + 多路单调双指针扫描, O(N log N))
// 超出 MAX_SEGMENTS 时按桶降采样以完整保留 [rangeStart, rangeEnd] 区间并标示截断
const segmentResult = computed(() => {
  const rs = Number(props.rangeStart)
  const re = Number(props.rangeEnd)
  if (!Number.isFinite(rs) || !Number.isFinite(re) || re < rs) {
    return { segments: [] as SegmentItem[], truncated: false }
  }

  const fwd = normalizeIntervals(props.forwardedRuns, rs, re)
  const bad = normalizeIntervals(props.badRuns, rs, re)
  const flt = normalizeIntervals(props.filteredRuns, rs, re)
  const cov = normalizeIntervals(props.coverage, rs, re)

  const points = new Set<number>([rs, re + 1])
  for (const list of [fwd, bad, flt, cov]) {
    for (const [s, e] of list) {
      points.add(s)
      points.add(e + 1)
    }
  }
  const sorted = [...points].sort((a, b) => a - b)

  let fwdIdx = 0
  let badIdx = 0
  let fltIdx = 0
  let covIdx = 0

  const totalRaw = sorted.length - 1
  const isTruncated = totalRaw > MAX_SEGMENTS
  const bucketSize = isTruncated ? Math.ceil(totalRaw / MAX_SEGMENTS) : 1

  const segs: SegmentItem[] = []
  for (let startIdx = 0; startIdx < totalRaw; startIdx += bucketSize) {
    const endIdx = Math.min(startIdx + bucketSize, totalRaw)
    const bucketStart = sorted[startIdx]
    const bucketEnd = sorted[endIdx] - 1
    let kind = 'uncovered'

    for (let i = startIdx; i < endIdx; i++) {
      const s = sorted[i]
      const e = sorted[i + 1] - 1

      while (fwdIdx < fwd.length && fwd[fwdIdx][1] < s) fwdIdx++
      while (badIdx < bad.length && bad[badIdx][1] < s) badIdx++
      while (fltIdx < flt.length && flt[fltIdx][1] < s) fltIdx++
      while (covIdx < cov.length && cov[covIdx][1] < s) covIdx++

      let rawKind = 'uncovered'
      if (fwdIdx < fwd.length && fwd[fwdIdx][0] <= s && fwd[fwdIdx][1] >= e) {
        rawKind = 'forwarded'
      } else if (badIdx < bad.length && bad[badIdx][0] <= s && bad[badIdx][1] >= e) {
        rawKind = 'bad'
      } else if (fltIdx < flt.length && flt[fltIdx][0] <= s && flt[fltIdx][1] >= e) {
        rawKind = 'filtered'
      } else if (covIdx < cov.length && cov[covIdx][0] <= s && cov[covIdx][1] >= e) {
        rawKind = 'covered-empty'
      }

      if (KIND_PRIORITY[rawKind] > KIND_PRIORITY[kind]) {
        kind = rawKind
      }
    }

    const prev = segs[segs.length - 1]
    if (prev && prev.kind === kind && prev.end + 1 === bucketStart) {
      prev.end = bucketEnd
      prev.span = prev.end - prev.start + 1
    } else {
      segs.push({
        start: bucketStart,
        end: bucketEnd,
        span: bucketEnd - bucketStart + 1,
        kind,
      })
    }
  }
  return { segments: segs, truncated: isTruncated }
})

const segments = computed(() => segmentResult.value.segments)
const segmentsTruncated = computed(() => segmentResult.value.truncated)

const coverageHint = computed(() => {
  const rs = Number(props.rangeStart)
  const re = Number(props.rangeEnd)
  if (!Number.isFinite(rs) || !Number.isFinite(re) || re < rs) return ''
  const mergedCov = normalizeIntervals(props.coverage, rs, re)
  const covered = mergedCov.reduce((acc, [s, e]) => acc + (e - s + 1), 0)
  const total = re - rs + 1
  const pct = total > 0 ? Math.round((covered / total) * 100) : 0
  return `已覆盖 ${pct}%`
})
</script>

<style scoped>
.coverage-bar-wrap {
  display: flex;
  flex-direction: column;
  gap: 0.4rem;
  width: 100%;
}

.coverage-scale {
  display: flex;
  justify-content: space-between;
  font-size: 0.78rem;
  color: var(--text-secondary);
  font-family: monospace;
}

.coverage-hint {
  color: var(--info-color);
}

.coverage-bar {
  display: flex;
  width: 100%;
  height: 18px;
  border-radius: 5px;
  overflow: hidden;
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
}

.coverage-bar.is-loading {
  opacity: 0.5;
}

.coverage-seg {
  min-width: 0;
  flex-basis: 0;
  cursor: pointer;
  transition: filter 0.15s;
}

.coverage-seg:hover,
.coverage-seg:focus-visible {
  filter: brightness(1.4);
  outline: none;
}

/* 色觉无障碍: 颜色 + 纹理 + 亮度三重区分 */
.seg-forwarded {
  background: #22c55e;
}

.seg-filtered {
  background: repeating-linear-gradient(45deg, #64748b 0 4px, #475569 4px 8px);
}

.seg-bad {
  background: repeating-linear-gradient(135deg, #ef4444 0 3px, #7f1d1d 3px 6px);
}

.seg-covered-empty {
  background: rgba(100, 116, 139, 0.28);
}

.seg-uncovered {
  background: rgba(148, 163, 184, 0.05);
  box-shadow: inset 0 0 0 1px rgba(148, 163, 184, 0.35);
}

.coverage-legend {
  display: flex;
  flex-wrap: wrap;
  gap: 0.75rem;
  font-size: 0.75rem;
  color: var(--text-secondary);
}

.legend-item {
  display: inline-flex;
  align-items: center;
  gap: 0.3rem;
}

.legend-warn {
  color: var(--warning-color);
}

.swatch {
  display: inline-block;
  width: 14px;
  height: 10px;
  border-radius: 2px;
}

.swatch-forwarded {
  background: #22c55e;
}
.swatch-filtered {
  background: repeating-linear-gradient(45deg, #64748b 0 3px, #475569 3px 6px);
}
.swatch-bad {
  background: repeating-linear-gradient(135deg, #ef4444 0 2px, #7f1d1d 2px 4px);
}
.swatch-covered-empty {
  background: rgba(100, 116, 139, 0.28);
}
.swatch-uncovered {
  background: rgba(148, 163, 184, 0.05);
  box-shadow: inset 0 0 0 1px rgba(148, 163, 184, 0.5);
}
</style>
