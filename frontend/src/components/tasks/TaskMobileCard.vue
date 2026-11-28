<template>
  <article
    data-testid="task-mobile-card"
    class="task-mobile-card"
    :class="{
      'is-selected': isSelected,
      'is-active': isActive,
    }"
    @click="$emit('open-drawer', task.id)"
  >
    <!-- 顶部栏: 勾选 + ID + 类型 + 状态 -->
    <div class="card-top-row">
      <div class="card-id-group" @click.stop>
        <n-checkbox
          class="task-select-checkbox"
          :checked="isSelected"
          @update:checked="(chk: boolean) => $emit('select', task.id, chk)"
        >
          <span class="sr-only">选择任务 #{{ task.id }}</span>
        </n-checkbox>
        <span class="id-text font-mono" :title="task.id">#{{ task.id }}</span>
        <button
          type="button"
          class="copy-btn"
          title="复制任务 ID"
          aria-label="复制任务 ID"
          @click.stop="$emit('copy-id', task.id)"
        >
          📋
        </button>
      </div>

      <div class="card-tags-group">
        <n-tag size="small" :type="getTypeTagType(task.task_type)" round>
          {{ getTypeLabel(task.task_type) }}
        </n-tag>
        <template v-if="task.task_type === 'pikpak_publish' && health.isCooldown">
          <span :title="health.description">
            <n-tag size="small" type="warning" round class="status-tag status-tag-cooldown">
              <span class="pulse-dot-inline dot-warning"></span>
              {{ health.badgeText }}
            </n-tag>
          </span>
        </template>
        <template v-else-if="task.task_type === 'pikpak_publish' && health.isSafePaused">
          <span :title="health.description">
            <n-tag size="small" type="warning" round class="status-tag status-tag-safe-paused"> 🟠 安全挂起 </n-tag>
          </span>
        </template>
        <template v-else-if="task.task_type === 'pikpak_publish' && health.isAuthRequired">
          <span :title="health.description">
            <n-tag size="small" type="error" round class="status-tag"> 🔴 凭据失效 </n-tag>
          </span>
        </template>
        <template v-else>
          <n-tag
            size="small"
            :type="getStatusTagType(task.status)"
            round
            class="status-tag"
            :class="{ 'status-tag-running': task.status === 'running' }"
          >
            <span v-if="task.status === 'running'" class="pulse-dot-inline"></span>
            {{ task.status }}
          </n-tag>
        </template>
        <span
          v-if="task.task_type === 'pikpak_publish' && health.isAutoDownshifted"
          title="已在无人值守下自动降档为保守稳态 (单通道/20s发送延迟)"
        >
          <n-tag size="tiny" type="info" round class="downshift-badge font-mono"> 🛡️ 稳态降档 </n-tag>
        </span>
      </div>
    </div>

    <!-- 中部: 进度信息与状态条 -->
    <div class="card-progress-block">
      <div class="progress-meta font-mono">
        <span class="progress-meta-left">
          <span>{{ formatTaskProgress(task) }}</span>
          <span v-if="itemsBadge" class="progress-items-badge">{{ itemsBadge }}</span>
        </span>
        <span class="progress-pct">{{ calcTaskPercent(task) }}%</span>
      </div>

      <n-progress
        type="line"
        :class="{ 'running-progress-emerald': task.status === 'running' }"
        :percentage="calcTaskPercent(task)"
        :status="getProgressStatus(task.status)"
        :processing="task.status === 'running'"
        :show-indicator="false"
        :height="7"
      />

      <div
        v-if="task.task_type === 'pikpak_publish' && health.isCooldown"
        class="notice-bar cooldown-notice font-mono"
        :title="health.description"
      >
        ⏳ {{ health.label }}
      </div>
      <div
        v-else-if="task.task_type === 'pikpak_publish' && health.isSafePaused"
        class="notice-bar safe-paused-notice font-mono"
        :title="health.description"
      >
        🟠 [风控保护] {{ health.description }}
      </div>
      <div
        v-else-if="task.progress.current_item"
        class="current-item-sub font-mono"
        :title="task.progress.current_item"
      >
        {{ task.progress.current_item }}
      </div>
    </div>

    <!-- 遥测摘要行: 账号 + 速率/ETA + 并发/DC -->
    <div class="card-telemetry-row font-mono">
      <span class="account-tag-wrap" :title="accountTooltip">
        <n-tag size="tiny" :type="isMultiUserbot(task) ? 'success' : 'default'" round class="userbot-tag">
          {{ isMultiUserbot(task) ? '⚡' : '👤' }} {{ getTaskAccountDisplay(task, accountDisplayName) }}
        </n-tag>
      </span>

      <span v-if="task.status === 'running'" class="speed-badge text-emerald">
        ⚡ {{ (task.progress.speed_mb_s || 0).toFixed(2) }} MB/s
      </span>
      <span v-if="task.status === 'running'" class="eta-badge">
        {{
          task.progress.eta_s > 0 ? `ETA ${formatDurationLocalized(task.progress.eta_s, 'zh-CN', 'digital')}` : 'ETA --'
        }}
      </span>

      <span class="dc-badge">{{ formatDcLocation(task.progress.dc_id, true) }}</span>
    </div>

    <!-- 底部操作栏 -->
    <div class="card-footer-actions" @click.stop>
      <template v-if="task.status === 'running'">
        <span data-testid="task-pause-btn">
          <n-button size="small" type="warning" secondary @click="$emit('pause', task.id)"> 暂停 </n-button>
        </span>
        <span data-testid="task-cancel-btn">
          <n-button size="small" type="error" secondary @click="$emit('cancel', task)"> 取消 </n-button>
        </span>
      </template>

      <template v-else-if="task.task_type === 'pikpak_publish' && health.isCooldown">
        <span data-testid="task-resume-btn">
          <n-button size="small" type="success" secondary @click="$emit('resume', task.id)"> 恢复 </n-button>
        </span>
        <span data-testid="task-pause-btn">
          <n-button size="small" type="warning" secondary @click="$emit('pause', task.id)"> 暂停 </n-button>
        </span>
        <span data-testid="task-cancel-btn">
          <n-button size="small" type="error" secondary @click="$emit('cancel', task)"> 取消 </n-button>
        </span>
      </template>

      <template v-else-if="task.task_type === 'pikpak_publish' && health.isSafePaused">
        <n-button size="small" type="primary" secondary @click="$emit('resume', task.id, 'steady')">
          稳态恢复
        </n-button>
        <span data-testid="task-resume-btn">
          <n-button size="small" secondary @click="$emit('resume', task.id, 'normal')"> 恢复 </n-button>
        </span>
        <span data-testid="task-cancel-btn">
          <n-button size="small" type="error" secondary @click="$emit('cancel', task)"> 取消 </n-button>
        </span>
      </template>

      <template v-else-if="task.status === 'paused'">
        <span data-testid="task-resume-btn">
          <n-button size="small" type="primary" secondary @click="$emit('resume', task.id)"> 恢复 </n-button>
        </span>
        <span data-testid="task-cancel-btn">
          <n-button size="small" type="error" secondary @click="$emit('cancel', task)"> 取消 </n-button>
        </span>
      </template>

      <span v-else-if="task.status === 'queued'" data-testid="task-cancel-btn">
        <n-button size="small" type="error" secondary @click="$emit('cancel', task)"> 取消 </n-button>
      </span>

      <template v-else-if="task.status === 'failed' || task.status === 'cancelled'">
        <n-button size="small" type="primary" secondary @click="$emit('retry', task.id)"> 重试 </n-button>
        <n-button size="small" type="error" secondary @click="$emit('delete', task)"> 删除 </n-button>
      </template>

      <template v-else>
        <n-button size="small" secondary type="default" @click="$emit('delete', task)"> 删除 </n-button>
      </template>

      <span data-testid="task-detail-btn" class="detail-btn-wrap">
        <n-button size="small" type="info" secondary @click="$emit('open-drawer', task.id)"> 详情 </n-button>
      </span>
    </div>
  </article>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { NButton, NCheckbox, NProgress, NTag } from 'naive-ui'
import type { NormalizedTask } from '@/utils/taskNormalizer'
import {
  calcTaskPercent,
  formatTaskProgress,
  getProgressStatus,
  getStatusTagType,
  getTaskAccountDisplay,
  getTypeLabel,
  getTypeTagType,
  isMultiUserbot,
} from '@/utils/taskHelpers'
import { formatDcLocation, formatDurationLocalized } from '@/utils/formatters'
import { getPikPakChannelHealth } from '@/utils/pikpakHealth'

const props = defineProps<{
  task: NormalizedTask
  isSelected: boolean
  isActive: boolean
  currentTime: number
  accountDisplayName: (id: number | null | undefined) => string
}>()

defineEmits<{
  (e: 'select', id: string, checked: boolean): void
  (e: 'open-drawer', id: string): void
  (e: 'pause', id: string): void
  (e: 'resume', id: string, mode?: 'normal' | 'steady'): void
  (e: 'cancel', task: NormalizedTask): void
  (e: 'retry', id: string): void
  (e: 'delete', task: NormalizedTask): void
  (e: 'copy-id', id: string): void
}>()

const health = computed(() => getPikPakChannelHealth(props.task, props.currentTime))

const accountTooltip = computed(() => {
  if (isMultiUserbot(props.task)) {
    return `跨账号联合并发加速: ${getTaskAccountDisplay(props.task, props.accountDisplayName)}`
  }
  const name = getTaskAccountDisplay(props.task, props.accountDisplayName)
  return name === '自动调度' ? '系统自动负载均衡调度' : `执行账号: ${name}`
})

const itemsBadge = computed(() => {
  if (props.task.task_type === 'forward_channel') return null
  const p = props.task.progress
  const cur = p.processed_items
  const tot = p.total_items
  if (tot > 0) {
    const displayCur = props.task.status === 'completed' ? tot : cur !== null && cur !== undefined ? cur : 0
    return `[${displayCur}/${tot} 项]`
  }
  return null
})
</script>

<style scoped>
.task-mobile-card {
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-radius: 12px;
  padding: 0.85rem;
  display: flex;
  flex-direction: column;
  gap: 0.65rem;
  cursor: pointer;
  transition:
    background-color 0.15s ease,
    border-color 0.15s ease;
}

.task-mobile-card.is-selected {
  background-color: var(--table-row-selected, rgba(59, 130, 246, 0.08));
  border-color: var(--accent-color);
}

.task-mobile-card.is-active {
  background-color: var(--table-row-active, rgba(59, 130, 246, 0.12));
  border-color: var(--accent-color);
}

.card-top-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.5rem;
  flex-wrap: wrap;
}

.card-id-group {
  display: flex;
  align-items: center;
  gap: 0.45rem;
  min-width: 0;
}

.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip-path: inset(50%);
  white-space: nowrap;
  border: 0;
}

.task-select-checkbox :deep(.n-checkbox__label) {
  padding: 0;
}

.id-text {
  font-weight: 700;
  font-size: 0.88rem;
  color: var(--text-primary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 140px;
}

.copy-btn {
  background: transparent;
  border: none;
  cursor: pointer;
  padding: 4px 6px;
  font-size: 0.88rem;
  opacity: 0.75;
  min-width: 32px;
  min-height: 32px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
}

.card-tags-group {
  display: flex;
  align-items: center;
  gap: 0.35rem;
  flex-wrap: wrap;
}

.status-tag {
  display: inline-flex;
  align-items: center;
  gap: 0.3rem;
}

.pulse-dot-inline {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: #10b981;
  display: inline-block;
}

.pulse-dot-inline.dot-warning {
  background: #f59e0b;
}

.downshift-badge {
  font-size: 0.7rem;
}

.card-progress-block {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
}

.progress-meta {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.78rem;
  color: var(--text-secondary);
}

.progress-meta-left {
  display: inline-flex;
  align-items: center;
  gap: 0.35rem;
  min-width: 0;
  flex-wrap: wrap;
}

.progress-pct {
  font-weight: 700;
  color: var(--text-primary);
  flex-shrink: 0;
}

.current-item-sub {
  font-size: 0.75rem;
  color: var(--text-secondary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.notice-bar {
  font-size: 0.74rem;
  padding: 0.25rem 0.5rem;
  border-radius: 6px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.cooldown-notice {
  background: rgba(245, 158, 11, 0.12);
  color: var(--warning-color);
}

.safe-paused-notice {
  background: rgba(249, 115, 22, 0.12);
  color: #f97316;
}

.card-telemetry-row {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.4rem 0.65rem;
  font-size: 0.76rem;
  color: var(--text-secondary);
}

.account-tag-wrap {
  min-width: 0;
  max-width: 100%;
}

.account-tag-wrap :deep(.userbot-tag) {
  max-width: 100%;
  height: auto;
  min-height: 20px;
  padding-block: 2px;
}

.account-tag-wrap :deep(.userbot-tag .n-tag__content) {
  white-space: normal;
  overflow-wrap: anywhere;
  line-height: 1.3;
}

.speed-badge {
  font-weight: 600;
}

.text-emerald {
  color: #10b981;
}

.card-footer-actions {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.4rem;
  padding-top: 0.5rem;
  border-top: 1px dashed var(--border-color);
}

.detail-btn-wrap {
  margin-left: auto;
}
</style>
