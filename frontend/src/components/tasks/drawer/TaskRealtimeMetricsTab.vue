<template>
  <div class="tab-pane-content">
    <!-- 传输速率与动态曲线 (包3: pikpak_publish 双序列, 其余任务单序列) -->
    <div class="card glass-card section-box">
      <div class="box-header">
        <span class="box-title">动态传输速率曲线</span>
        <span class="font-mono current-speed-text" v-if="!isPikpakTask">
          {{ (task.progress.speed_mb_s || 0).toFixed(2) }} <span class="unit">MB/s</span>
        </span>
        <span class="font-mono current-speed-text" v-else>
          ↑ {{ (task.progress.speed_mb_s || 0).toFixed(2) }} / ↓
          {{ (task.progress.pikpak_download_speed_mb_s || 0).toFixed(2) }} <span class="unit">MB/s</span>
        </span>
      </div>

      <!-- 图例 (仅 pikpak 任务双序列) -->
      <div class="speed-legend" v-if="isPikpakTask">
        <span class="legend-item"><span class="legend-dot" style="background: var(--accent-color)"></span>TG 上传</span>
        <span class="legend-item"><span class="legend-dot" style="background: #38bdf8"></span>PikPak 拉流</span>
      </div>

      <!-- SVG 动态折线图 -->
      <div class="sparkline-container">
        <svg class="speed-svg" viewBox="0 0 400 90" preserveAspectRatio="none">
          <defs>
            <linearGradient id="speedGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stop-color="var(--accent-color)" stop-opacity="0.45" />
              <stop offset="100%" stop-color="var(--accent-color)" stop-opacity="0.0" />
            </linearGradient>
          </defs>
          <path :d="upAreaPath" fill="url(#speedGrad)" />
          <path
            :d="upLinePath"
            fill="none"
            stroke="var(--accent-color)"
            stroke-width="2.5"
            stroke-linecap="round"
            stroke-linejoin="round"
          />
          <path
            v-if="dlLinePath"
            :d="dlLinePath"
            fill="none"
            stroke="#38bdf8"
            stroke-width="2"
            stroke-linecap="round"
            stroke-linejoin="round"
          />
          <!-- 最新数据点光晕 -->
          <circle
            v-if="upPoints.length > 0"
            :cx="lastPointX"
            :cy="lastPointY"
            r="4.5"
            fill="var(--accent-color)"
            class="pulse-point"
          />
        </svg>
      </div>
    </div>

    <!-- 传输进度总览卡片 (三层分层看板) -->
    <div class="card glass-card section-box progress-hierarchy-card">
      <!-- 层级 1: 全任务总容量维度 -->
      <div class="hierarchy-tier tier-capacity">
        <div class="tier-header">
          <div class="tier-title-wrap">
            <span class="tier-badge">层级 1</span>
            <span class="tier-title">全任务总容量</span>
          </div>
          <div class="tier-meta font-mono">
            <span class="capacity-val">{{
              formattedCapacityProgress !== '-' ? formattedCapacityProgress : formattedProgress
            }}</span>
            <span class="progress-pct">{{ progressPercent }}%</span>
          </div>
        </div>
        <n-progress
          type="line"
          :class="{ 'running-progress-emerald': task.status === 'running' }"
          :percentage="progressPercent"
          :status="getProgressStatus(task.status)"
          :processing="task.status === 'running'"
          :height="10"
          class="progress-bar"
        />
        <div class="tier-sub-stats font-mono">
          <span>预计剩余时间 (ETA): {{ formatDurationLocalized(task.progress.eta_s || 0) }}</span>
          <span v-if="task.progress.speed_mb_s">传输速率: {{ (task.progress.speed_mb_s || 0).toFixed(2) }} MB/s</span>
        </div>
      </div>

      <!-- 层级 2: 条目计数统计维度 -->
      <div v-if="itemsProgressInfo && itemsProgressInfo.show" class="hierarchy-tier tier-items">
        <div class="tier-header">
          <div class="tier-title-wrap">
            <span class="tier-badge tier-badge-items">层级 2</span>
            <span class="tier-title">条目处理计数</span>
          </div>
          <div class="tier-meta font-mono">
            <span class="items-val">{{ itemsProgressInfo.text }}</span>
            <span class="items-pct">{{ itemsProgressInfo.percent }}%</span>
          </div>
        </div>
        <n-progress
          type="line"
          :percentage="itemsProgressInfo.percent"
          :show-indicator="false"
          :height="6"
          status="info"
        />
      </div>

      <!-- 层级 3: 当前活跃文件传输态 -->
      <div v-if="activeItemInfo" class="hierarchy-tier tier-active-item">
        <div class="tier-header">
          <div class="tier-title-wrap">
            <span class="tier-badge tier-badge-active">层级 3</span>
            <span class="tier-title">当前活跃文件</span>
          </div>
          <n-tag size="tiny" :type="task.status === 'running' ? 'success' : 'default'" round>
            {{ activeItemInfo.statusText }}
          </n-tag>
        </div>
        <div class="active-item-card font-mono">
          <div class="active-item-name text-truncate" :title="activeItemInfo.name">📄 {{ activeItemInfo.name }}</div>
        </div>
      </div>
    </div>

    <!-- FastTelethon 活跃条带与机房节点 (全宽横向流式布局) -->
    <div class="card glass-card section-box channel-rack-card">
      <div class="box-header">
        <div class="box-header-left">
          <span class="box-title"
            >FastTelethon
            {{ task.progress.concurrency_total || (task.task_type === 'publish_folder' ? 8 : 16) }} 通道</span
          >
          <span class="channel-badge font-mono">
            {{ task.progress.concurrency || 0 }} /
            {{ task.progress.concurrency_total || (task.task_type === 'publish_folder' ? 8 : 16) }} 线程
          </span>
        </div>
        <div class="box-header-right" title="Telegram 媒体专属分布式存储集群">
          <n-tag size="small" type="info" round class="font-mono dc-node-tag">
            🌐 {{ formatDcLocation(task.progress.dc_id) }}
          </n-tag>
        </div>
      </div>

      <p class="box-desc">
        {{ task.progress.concurrency_total || (isUploadTask ? 2 : 16) }} 并发 TCP 连接池多路复用分块吞吐矩阵
        <template v-if="isUploadTask">
          · 单连接在途: {{ currentTaskInFlight }} 块 (在途容量:
          {{ (task.progress.concurrency_total || 2) * currentTaskInFlight }} 请求)
        </template>
      </p>

      <!-- FloodWait 冷却倒计时 -->
      <div v-if="(task.progress.flood_wait_seconds || 0) > 0" class="flood-alert-box">
        ⏳ 触发 Telegram 频控保护，需等待 <b>{{ task.progress.flood_wait_seconds }}s</b>
      </div>

      <!-- Worker 通道横向条带化 (Channel Rack) -->
      <div class="channels-rack">
        <div
          v-for="ch in task.progress.concurrency_total || (isUploadTask ? 2 : 16)"
          :key="ch"
          class="channel-tile font-mono"
          :class="{ active: ch <= (task.progress.concurrency || 0) && task.status === 'running' }"
          :title="`Worker #${ch}: ${ch <= (task.progress.concurrency || 0) && task.status === 'running' ? '传输中' : '空闲'}`"
        >
          <span class="ch-led"></span>
          <span class="ch-num">#{{ ch }}</span>
        </div>
      </div>

      <!-- 实时动态调整并发与在途数 (横向一字排开) -->
      <div
        v-if="
          ['running', 'pending', 'paused'].includes(task.status) &&
          (['download_link', 'download_range'].includes(task.task_type) || isUploadTask)
        "
        class="concurrency-adjust-bar"
      >
        <div class="adjust-group">
          <span class="adjust-label">并发通道:</span>
          <n-input-number
            :value="currentTaskConcurrency"
            :min="minTaskConcurrency"
            :max="maxTaskConcurrency"
            :step="stepTaskConcurrency"
            size="small"
            :loading="updatingConcurrency"
            style="width: 105px"
            @update:value="(v) => v !== null && emit('update-concurrency', v)"
          />
        </div>

        <template v-if="isUploadTask">
          <div class="adjust-group">
            <span class="adjust-label">单连接在途:</span>
            <n-input-number
              :value="currentTaskInFlight"
              :min="1"
              :max="4"
              :step="1"
              size="small"
              :loading="updatingConcurrency"
              style="width: 95px"
              @update:value="(v) => v !== null && emit('update-in-flight', v)"
            />
          </div>
        </template>

        <span class="adjust-hint">
          (通道 {{ minTaskConcurrency }}~{{ maxTaskConcurrency }}<template v-if="isUploadTask">, 在途 1~4</template>)
        </span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
// 实时监控看板 Tab: 动态双序列 SVG 速率折线图 / 三层进度看板 /
// FastTelethon 通道矩阵 / 实时并发调节条。
// 并发调节的写入与单任务回填由父级 Drawer 统一持有 (两个 Tab 共用), 本组件仅展示与上报事件。
import { computed, onUnmounted, ref, watch } from 'vue'
import { NInputNumber, NProgress, NTag } from 'naive-ui'
import { formatBytes, formatDcLocation, formatDurationLocalized } from '@/utils/formatters'
import {
  calcTaskPercent,
  formatTaskProgress,
  getProgressStatus,
  isUploadTask as checkUploadTask,
} from '@/utils/taskHelpers'
import type { NormalizedTask } from '@/utils/taskNormalizer'

const props = defineProps<{
  task: NormalizedTask
  currentTaskConcurrency: number
  currentTaskInFlight: number
  minTaskConcurrency: number
  maxTaskConcurrency: number
  stepTaskConcurrency: number
  updatingConcurrency: boolean
}>()

const emit = defineEmits<{
  (e: 'update-concurrency', value: number): void
  (e: 'update-in-flight', value: number): void
}>()

const isPikpakTask = computed(() => props.task.task_type === 'pikpak_publish')
const isUploadTask = computed(() => checkUploadTask(props.task))

// ── 速度历史采样 (包3): 1s 时间窗取最新值, 替代"watch 值变化才推点" —— 值不变时曲线
// 停滞、高频变化时过密。pikpak_publish 任务双序列 (TG 上传 + PikPak 拉流), 其余单序列。
const SAMPLE_WINDOW_MS = 1000
const MAX_SPEED_POINTS = 30
const speedSeries = ref<number[]>([0])
const dlSeries = ref<number[]>([]) // 仅 pikpak_publish 任务采集
let samplerTimer: ReturnType<typeof setInterval> | null = null

function stopSpeedSampler() {
  if (samplerTimer) {
    clearInterval(samplerTimer)
    samplerTimer = null
  }
}

function pushSpeedSample() {
  const t = props.task
  if (!t) return
  speedSeries.value.push(Number(t.progress.speed_mb_s || 0))
  if (speedSeries.value.length > MAX_SPEED_POINTS) speedSeries.value.shift()
  if (isPikpakTask.value) {
    dlSeries.value.push(Number(t.progress.pikpak_download_speed_mb_s || 0))
    if (dlSeries.value.length > MAX_SPEED_POINTS) dlSeries.value.shift()
  }
}

function startSpeedSampler() {
  if (samplerTimer) return
  if (typeof document !== 'undefined' && document.hidden) return
  samplerTimer = setInterval(pushSpeedSample, SAMPLE_WINDOW_MS)
}

function handleVisibilityChange() {
  if (typeof document !== 'undefined' && document.hidden) {
    stopSpeedSampler()
  } else if (props.task.status === 'running') {
    pushSpeedSample()
    startSpeedSampler()
  }
}

function resetSpeedSeries() {
  speedSeries.value = [0]
  dlSeries.value = []
}

if (typeof document !== 'undefined') {
  document.addEventListener('visibilitychange', handleVisibilityChange)
}

watch(
  [() => props.task.id, () => props.task.status],
  ([, status]) => {
    if (status === 'running') {
      startSpeedSampler()
    } else {
      stopSpeedSampler()
    }
  },
  { immediate: true },
)

watch(
  () => props.task.id,
  () => {
    resetSpeedSeries()
  },
)

onUnmounted(() => {
  stopSpeedSampler()
  if (typeof document !== 'undefined') {
    document.removeEventListener('visibilitychange', handleVisibilityChange)
  }
})

// SVG 动态折线图计算 (参数化多序列)
// 包2 (K7): pikpak 任务双序列共用 Y 轴刻度 (max(TG上传序列+拉流序列, 下限 1)) —— 形状与
// 量级同时可比; 单序列任务 maxOverride 为 undefined → 走原 per-sequence max, 行为不变。
const sharedCurveMax = computed(() => {
  const vals = isPikpakTask.value ? [...speedSeries.value, ...dlSeries.value] : [...speedSeries.value]
  return Math.max(...vals, 1)
})

function seriesPoints(data: number[], maxOverride?: number) {
  const arr = data.length > 0 ? data : [0]
  const max = maxOverride ?? Math.max(...arr, 1)
  const width = 400
  const height = 80
  const step = width / Math.max(arr.length - 1, 1)
  return arr.map((val, idx) => ({ x: idx * step, y: height - (val / max) * (height - 10) }))
}

function linePathOf(pts: Array<{ x: number; y: number }>) {
  if (pts.length === 0) return ''
  return pts.reduce((acc, p, idx) => (idx === 0 ? `M ${p.x},${p.y}` : `${acc} L ${p.x},${p.y}`), '')
}

function areaPathOf(pts: Array<{ x: number; y: number }>, linePath: string) {
  if (pts.length === 0) return ''
  const last = pts[pts.length - 1]
  return `${linePath} L ${last.x},90 L 0,90 Z`
}

const upPoints = computed(() => seriesPoints(speedSeries.value, isPikpakTask.value ? sharedCurveMax.value : undefined))
const upLinePath = computed(() => linePathOf(upPoints.value))
const upAreaPath = computed(() => areaPathOf(upPoints.value, upLinePath.value))
const dlPoints = computed(() => (isPikpakTask.value ? seriesPoints(dlSeries.value, sharedCurveMax.value) : []))
const dlLinePath = computed(() => (dlPoints.value.length > 1 ? linePathOf(dlPoints.value) : ''))
const lastPointX = computed(() => (upPoints.value.length > 0 ? upPoints.value[upPoints.value.length - 1].x : 0))
const lastPointY = computed(() => (upPoints.value.length > 0 ? upPoints.value[upPoints.value.length - 1].y : 80))

// 进度与进度条 (展示 Helper 统一收敛至 utils/taskHelpers.ts)
const progressPercent = computed(() => calcTaskPercent(props.task))
const formattedProgress = computed(() => formatTaskProgress(props.task))

// 层级 1: 全任务容量进度 (驱动主显示)
const formattedCapacityProgress = computed(() => {
  const p = props.task.progress
  if (p.total_bytes > 0 || p.processed_bytes > 0) {
    return `${formatBytes(p.processed_bytes)} / ${formatBytes(p.total_bytes)}`
  }
  return '-'
})

// 层级 2: 条目计数统计
const itemsProgressInfo = computed(() => {
  const t = props.task
  const p = t.progress
  const curItems = p.processed_items
  const totItems = p.total_items

  if (t.task_type === 'forward_channel') {
    const summary = t.params?.final_summary as { forwarded?: number } | undefined
    if (t.status === 'completed' && summary && summary.forwarded !== undefined) {
      return {
        text: `${totItems || curItems || 0} / ${totItems || curItems || 0} 项已扫描 (成功 ${summary.forwarded} 条)`,
        percent: 100,
        show: true,
      }
    }
    const cur = curItems || 0
    const tot = totItems || 0
    const pct = tot > 0 ? Math.min(100, parseFloat(((cur / tot) * 100).toFixed(1))) : 0
    return {
      text: `${cur} / ${tot} 项已扫描`,
      percent: pct,
      show: tot > 0,
    }
  }

  if (totItems !== undefined && totItems !== null && totItems > 0) {
    const displayCur = t.status === 'completed' ? totItems : curItems !== null && curItems !== undefined ? curItems : 0
    const pct = Math.min(100, parseFloat(((displayCur / totItems) * 100).toFixed(1)))
    return {
      text: `${displayCur} / ${totItems} 项已处理`,
      current: displayCur,
      total: totItems,
      percent: pct,
      show: true,
    }
  }
  return null
})

// 层级 3: 当前活跃文件传输态
const activeItemInfo = computed(() => {
  const t = props.task
  if (!t?.progress.current_item) return null
  return {
    name: t.progress.current_item,
    statusText: t.status === 'running' ? '传输中' : t.status === 'completed' ? '已完成' : t.status,
  }
})
</script>

<style scoped>
.tab-pane-content {
  display: flex;
  flex-direction: column;
  gap: 1rem;
  padding-top: 0.5rem;
}
.section-box {
  padding: 1rem 1.25rem;
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}
.box-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.5rem;
}
.box-header-left {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.5rem 0.75rem;
  min-width: 0;
}
.box-header-right {
  display: flex;
  align-items: center;
}
.box-title {
  font-size: 0.95rem;
  font-weight: 700;
  color: var(--text-primary);
}
.box-desc {
  font-size: 0.75rem;
  color: var(--text-secondary);
  margin: 0;
}
.current-speed-text {
  font-size: 1.15rem;
  font-weight: 800;
  color: var(--accent-color);
}
/* 包3: 双速率曲线图例 */
.speed-legend {
  display: flex;
  gap: 1rem;
  font-size: 0.75rem;
  color: var(--text-secondary);
}
.legend-item {
  display: inline-flex;
  align-items: center;
  gap: 0.35rem;
}
.legend-dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  display: inline-block;
}
.sparkline-container {
  height: 90px;
  background: var(--bg-primary);
  border-radius: 8px;
  padding: 0.5rem;
  overflow: hidden;
  border: 1px solid var(--border-color);
}
.speed-svg {
  width: 100%;
  height: 100%;
}
.pulse-point {
  animation: pulse 1.5s infinite;
}
@keyframes pulse {
  0% {
    r: 4;
    opacity: 1;
  }
  50% {
    r: 6.5;
    opacity: 0.7;
  }
  100% {
    r: 4;
    opacity: 1;
  }
}
.progress-pct {
  font-weight: 700;
  font-size: 1.1rem;
  color: var(--text-primary);
}
/* 三层分层进度看板 */
.progress-hierarchy-card {
  display: flex;
  flex-direction: column;
  gap: 0.9rem;
}
.hierarchy-tier {
  display: flex;
  flex-direction: column;
  gap: 0.4rem;
}
.tier-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.4rem;
}
.tier-title-wrap {
  display: inline-flex;
  align-items: center;
  gap: 0.45rem;
}
.tier-badge {
  font-size: 0.68rem;
  padding: 1px 6px;
  border-radius: 4px;
  background: rgba(16, 185, 129, 0.15);
  color: var(--success-color);
  font-weight: 600;
}
.tier-badge-items {
  background: rgba(59, 130, 246, 0.15);
  color: var(--accent-color);
}
.tier-badge-active {
  background: rgba(245, 158, 11, 0.15);
  color: var(--warning-color);
}
.tier-title {
  font-size: 0.85rem;
  font-weight: 600;
  color: var(--text-primary);
}
.tier-meta {
  display: inline-flex;
  align-items: center;
  gap: 0.6rem;
}
.capacity-val,
.items-val {
  font-size: 0.85rem;
  color: var(--text-secondary);
}
.items-pct {
  font-size: 0.85rem;
  font-weight: 600;
  color: var(--accent-color);
}
.tier-sub-stats {
  display: flex;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 0.35rem;
  font-size: 0.75rem;
  color: var(--text-secondary);
  margin-top: 0.2rem;
}
.tier-items {
  padding-top: 0.6rem;
  border-top: 1px dashed var(--border-color);
}
.tier-active-item {
  padding-top: 0.6rem;
  border-top: 1px dashed var(--border-color);
}
.active-item-card {
  background: var(--bg-primary);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  padding: 0.5rem 0.75rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.8rem;
}
.active-item-name {
  color: var(--text-primary);
  flex: 1;
}
.text-truncate {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 100%;
  min-width: 0;
}
.channels-rack {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin: 0.25rem 0;
}
.channel-tile {
  flex: 1 1 calc(12.5% - 6px);
  min-width: 52px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 5px;
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  padding: 0.35rem 0.3rem;
  border-radius: 4px;
  font-size: 0.75rem;
  text-align: center;
  color: var(--text-secondary);
  transition: all 0.2s;
}
.channel-tile.active {
  background: var(--accent-light);
  border-color: var(--accent-color);
  color: var(--accent-color);
  font-weight: 700;
}
.ch-led {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: rgba(140, 140, 140, 0.35);
  display: inline-block;
  flex-shrink: 0;
}
.channel-tile.active .ch-led {
  background: var(--success-color);
  box-shadow: 0 0 6px rgba(16, 185, 129, 0.8);
}
.channel-badge {
  font-size: 0.8rem;
  color: var(--accent-color);
  font-weight: 700;
}
.channel-rack-card {
  width: 100%;
}
.dc-node-tag {
  font-size: 0.75rem;
}
.flood-alert-box {
  background: rgba(234, 179, 8, 0.15);
  border: 1px solid rgba(234, 179, 8, 0.4);
  color: var(--warning-color);
  padding: 0.5rem;
  border-radius: 6px;
  font-size: 0.75rem;
  text-align: center;
}
.concurrency-adjust-bar {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.8rem 1.2rem;
  margin-top: 12px;
  padding-top: 10px;
  border-top: 1px dashed var(--border-color);
  font-size: 0.85rem;
}
.adjust-group {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}
.adjust-label {
  color: var(--text-secondary);
  font-weight: 500;
  white-space: nowrap;
}
.adjust-hint {
  color: var(--text-muted);
  font-size: 0.75rem;
  white-space: normal;
}
</style>
