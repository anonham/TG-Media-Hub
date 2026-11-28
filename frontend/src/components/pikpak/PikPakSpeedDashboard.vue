<template>
  <div class="card glass-card">
    <div class="card-header">
      <div class="card-title">📊 双向实时速率 (TG 上行 ⇄ PikPak 下行)</div>
    </div>
    <n-empty
      v-if="activePikpakTasks.length === 0"
      description="暂无活跃的 PikPak 搬运任务（当前系统空闲）—— 任务运行中时此处实时显示双向流速"
      class="dash-empty"
    />
    <template v-else>
      <n-alert v-for="t in quotaAlertTasks" :key="'quota-' + t.id" type="error" :show-icon="true" class="quota-alert">
        ⚠️ 检测到配额/限速警报: {{ t.error_detail }}
      </n-alert>
      <div v-for="t in activePikpakTasks" :key="t.id" class="dash-task-row">
        <div class="dash-task-head">
          <n-tag size="small" :type="t.status === 'running' ? 'success' : 'default'" round>
            {{ t.status }}
          </n-tag>
          <span class="dash-task-id font-mono">#{{ t.id }}</span>
          <span class="dash-task-item text-secondary" :title="t.progress.current_item">
            {{ t.progress.current_item || '—' }}
          </span>
        </div>
        <div class="dash-gauges">
          <div class="gauge">
            <div class="gauge-label">
              <span>⬆ TG 上行</span>
              <span class="font-mono">{{ (t.progress.speed_mb_s ?? 0).toFixed(2) }} MB/s</span>
            </div>
            <n-progress type="line" :height="8" :show-indicator="false" :percentage="gaugePct(t.progress.speed_mb_s)" />
          </div>
          <div class="gauge">
            <div class="gauge-label">
              <span>⬇ PikPak 下行</span>
              <span class="font-mono">{{ (t.progress.pikpak_download_speed_mb_s ?? 0).toFixed(2) }} MB/s</span>
            </div>
            <n-progress
              type="line"
              :height="8"
              :show-indicator="false"
              :percentage="gaugePct(t.progress.pikpak_download_speed_mb_s)"
            />
          </div>
          <div class="dash-meta font-mono text-secondary">
            {{ t.progress.processed_items || 0 }} / {{ t.progress.total_items || 0 }} 项
            <template v-if="t.progress.total_bytes > 0">
              ({{ formatBytes(t.progress.processed_bytes) }} / {{ formatBytes(t.progress.total_bytes) }})
            </template>
            <template v-if="(t.progress.eta_s ?? 0) > 0">
              · ETA {{ formatDurationLocalized(t.progress.eta_s) }}</template
            >
          </div>
        </div>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
// 双向实时速率仪表盘: 数据源 taskStore 任务行 (双速字段缺失显示 0 容错)。
// 仅展示活跃中/执行中的 PikPak 搬运任务, running 置顶; 配额/限速警报行单独前置。
import { computed } from 'vue'
import { NAlert, NEmpty, NProgress, NTag } from 'naive-ui'
import { useTaskStore } from '@/stores/taskStore'
import { formatBytes, formatDurationLocalized } from '@/utils/formatters'
import type { NormalizedTask } from '@/utils/taskNormalizer'

const taskStore = useTaskStore()

const activePikpakTasks = computed(() => {
  const list = (taskStore.tasks || []).filter(
    (t) => t.task_type === 'pikpak_publish' && ['running', 'queued', 'pending'].includes(t.status),
  )
  const tier = (t: NormalizedTask) => (t.status === 'running' ? 0 : 1)
  return [...list].sort((a, b) => tier(a) - tier(b))
})

// 匹配后端 FAILURE_DETAIL_TEXT['quota'] 文案关键词; 后端文案演化时同步此常量
const QUOTA_ALERT_KEYWORDS = ['配额', '限速']

function isQuotaThrottled(errorDetail: string | null | undefined): boolean {
  const s = String(errorDetail || '')
  return QUOTA_ALERT_KEYWORDS.some((k) => s.includes(k))
}

const quotaAlertTasks = computed(() => activePikpakTasks.value.filter((t) => isQuotaThrottled(t.error_detail)))

const gaugeScale = computed(() => {
  let m = 1
  for (const t of activePikpakTasks.value) {
    if (t.status !== 'running') continue
    const sp = Number(t.progress.speed_mb_s) || 0
    const psp = Number(t.progress.pikpak_download_speed_mb_s) || 0
    m = Math.max(m, sp, psp)
  }
  return m
})

function gaugePct(speed: number | null | undefined): number {
  if (activePikpakTasks.value.length === 0) return 0
  return Math.min(100, Math.round(((Number(speed) || 0) / gaugeScale.value) * 100))
}
</script>

<style scoped>
.card {
  padding: 1.5rem;
}

.card-header {
  border-bottom: 1px solid var(--border-color);
  padding-bottom: 1rem;
  margin-bottom: 1.25rem;
}

.card-title {
  font-size: 1.15rem;
  font-weight: 600;
}

.text-secondary {
  color: var(--text-secondary);
}

.dash-empty {
  padding: 1.5rem 0;
}

.quota-alert {
  margin-bottom: 0.75rem;
}

.dash-task-row {
  border: 1px solid var(--border-color);
  border-radius: 10px;
  padding: 0.85rem;
  display: flex;
  flex-direction: column;
  gap: 0.6rem;
}

.dash-task-row + .dash-task-row {
  margin-top: 0.75rem;
}

.dash-task-head {
  display: flex;
  align-items: center;
  gap: 0.6rem;
}

.dash-task-id {
  font-size: 0.85rem;
}

.dash-task-item {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 0.82rem;
}

.dash-gauges {
  display: flex;
  flex-direction: column;
  gap: 0.45rem;
}

.gauge {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.gauge-label {
  display: flex;
  justify-content: space-between;
  font-size: 0.82rem;
}

.dash-meta {
  font-size: 0.78rem;
}
</style>
