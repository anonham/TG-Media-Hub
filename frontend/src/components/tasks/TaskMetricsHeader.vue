<template>
  <div class="metrics-summary-shell">
    <div class="metrics-summary-bar glass-card">
      <div data-testid="status-metric-card" class="summary-metric-card">
        <div class="metric-icon-wrap bg-blue">⚡</div>
        <div class="metric-text-wrap">
          <div class="metric-label">瞬时总传输速率</div>
          <div class="metric-value font-mono">
            {{ (totalRunningSpeed || 0).toFixed(2) }} <span class="unit">MB/s</span>
          </div>
        </div>
      </div>

      <button
        type="button"
        data-testid="status-metric-card"
        class="summary-metric-card clickable-card"
        @click="$emit('filter-status', 'running')"
      >
        <div class="metric-icon-wrap bg-emerald">▶️</div>
        <div class="metric-text-wrap">
          <div class="metric-label">运行中任务</div>
          <div class="metric-value font-mono text-emerald">
            {{ statusCounts.running || 0 }}
          </div>
        </div>
      </button>

      <button
        type="button"
        data-testid="status-metric-card"
        class="summary-metric-card clickable-card"
        @click="$emit('filter-status', 'queued')"
      >
        <div class="metric-icon-wrap bg-amber">⏳</div>
        <div class="metric-text-wrap">
          <div class="metric-label">排队调度中</div>
          <div class="metric-value font-mono text-amber">
            {{ statusCounts.queued || 0 }}
          </div>
        </div>
      </button>

      <button
        type="button"
        data-testid="status-metric-card"
        class="summary-metric-card clickable-card"
        @click="$emit('filter-status', 'paused')"
      >
        <div class="metric-icon-wrap bg-orange">⏸️</div>
        <div class="metric-text-wrap">
          <div class="metric-label">已暂停任务</div>
          <div class="metric-value font-mono text-orange">
            {{ statusCounts.paused || 0 }}
          </div>
        </div>
      </button>

      <button
        type="button"
        data-testid="status-metric-card"
        class="summary-metric-card clickable-card"
        @click="$emit('filter-status', 'failed')"
      >
        <div class="metric-icon-wrap bg-rose">❌</div>
        <div class="metric-text-wrap">
          <div class="metric-label">失败待重试</div>
          <div class="metric-value font-mono text-rose">
            {{ statusCounts.failed || 0 }}
          </div>
        </div>
      </button>

      <button
        type="button"
        data-testid="status-metric-card"
        class="summary-metric-card clickable-card"
        @click="$emit('filter-status', 'completed')"
      >
        <div class="metric-icon-wrap bg-purple">✅</div>
        <div class="metric-text-wrap">
          <div class="metric-label">已完成任务</div>
          <div class="metric-value font-mono text-purple">
            {{ statusCounts.completed || 0 }}
          </div>
        </div>
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
export interface TaskStatusCounts {
  running: number
  queued: number
  paused: number
  failed: number
  completed: number
  cancelled: number
}

defineProps<{
  totalRunningSpeed: number
  statusCounts: TaskStatusCounts
}>()

defineEmits<{
  (e: 'filter-status', status: string): void
}>()
</script>

<style scoped>
.metrics-summary-shell {
  width: 100%;
  container: metrics-bar / inline-size;
}

.metrics-summary-bar {
  display: grid;
  grid-template-columns: repeat(6, 1fr);
  gap: 1rem;
  padding: 1.25rem;
  width: 100%;
}

.summary-metric-card {
  display: flex;
  align-items: center;
  gap: 0.85rem;
  padding: 0.5rem 0.75rem;
  border-radius: 8px;
  background: var(--bg-card);
  border: 1px solid var(--border-color);
  transition: all 0.2s ease;
}

.clickable-card {
  cursor: pointer;
  width: 100%;
  text-align: left;
  font: inherit;
  color: inherit;
}

.clickable-card:focus-visible {
  outline: 2px solid var(--primary-color);
  outline-offset: 2px;
}

.clickable-card:hover {
  transform: translateY(-2px);
  border-color: var(--primary-color);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
}

.metric-icon-wrap {
  width: 36px;
  height: 36px;
  border-radius: 8px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 1.15rem;
  flex-shrink: 0;
}

.bg-blue {
  background: rgba(59, 130, 246, 0.15);
}

.bg-emerald {
  background: rgba(16, 185, 129, 0.15);
}

.bg-amber {
  background: rgba(245, 158, 11, 0.15);
}

.bg-orange {
  background: rgba(249, 115, 22, 0.15);
}

.bg-rose {
  background: rgba(244, 63, 94, 0.15);
}

.bg-purple {
  background: rgba(168, 85, 247, 0.15);
}

.metric-text-wrap {
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.metric-label {
  font-size: 0.76rem;
  color: var(--text-secondary);
  white-space: nowrap;
}

.metric-value {
  font-size: clamp(0.95rem, 0.78rem + 1.1cqi, 1.2rem);
  font-weight: 700;
  line-height: 1.3;
}

.metric-value .unit {
  font-size: 0.75rem;
  font-weight: 400;
  color: var(--text-secondary);
}

.text-emerald {
  color: #10b981;
}

.text-amber {
  color: #f59e0b;
}

.text-orange {
  color: #f97316;
}

.text-rose {
  color: #f43f5e;
}

.text-purple {
  color: #a855f7;
}

@container metrics-bar (max-width: 1060px) {
  .metrics-summary-bar {
    grid-template-columns: repeat(3, 1fr);
  }
}

@container metrics-bar (max-width: 610px) {
  .metrics-summary-bar {
    grid-template-columns: repeat(2, 1fr);
    gap: 0.6rem;
    padding: 0.75rem;
  }
  .summary-metric-card {
    gap: 0.55rem;
    padding: 0.5rem 0.6rem;
    min-width: 0;
  }
  .metric-icon-wrap {
    width: 30px;
    height: 30px;
    font-size: 0.95rem;
  }
  .metric-label {
    font-size: 0.72rem;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .metric-value {
    font-size: 1.05rem;
    white-space: nowrap;
  }
}

@container metrics-bar (max-width: 370px) {
  .metrics-summary-bar {
    gap: 0.45rem;
    padding: 0.6rem;
  }
  .summary-metric-card {
    gap: 0.45rem;
    padding: 0.45rem 0.5rem;
  }
  .metric-icon-wrap {
    width: 26px;
    height: 26px;
    font-size: 0.85rem;
  }
  .metric-value {
    font-size: 0.92rem;
    white-space: normal;
  }
  .metric-value .unit {
    font-size: 0.68rem;
  }
}
</style>
