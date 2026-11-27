<template>
  <tr
    data-testid="task-row"
    class="task-row"
    :class="{
      'is-selected': isSelected,
      'is-active': isActive,
    }"
    @click="$emit('open-drawer', task.id)"
  >
    <!-- 勾选列 -->
    <td class="checkbox-cell" @click.stop>
      <n-checkbox :checked="isSelected" @update:checked="(chk: boolean) => $emit('select', task.id, chk)" />
    </td>

    <!-- 任务 ID 列 -->
    <td class="task-id-cell">
      <div class="id-wrapper font-mono">
        <span class="id-text" :title="task.id">#{{ task.id }}</span>
        <button class="copy-btn" @click.stop="$emit('copy-id', task.id)" title="复制 ID">📋</button>
      </div>
    </td>

    <!-- 任务类型 Tag 列 -->
    <td>
      <n-tag size="small" :type="getTypeTagType(task.task_type)" round>
        {{ getTypeLabel(task.task_type) }}
      </n-tag>
    </td>

    <!-- 执行 Userbot 账号 Tag 列 -->
    <td>
      <span :title="accountTooltip">
        <n-tag
          size="small"
          :bordered="false"
          :type="isMultiUserbot(task) ? 'success' : 'default'"
          round
          class="font-mono userbot-tag"
          :class="{ 'is-multi': isMultiUserbot(task) }"
        >
          <template #icon>
            <span v-if="isMultiUserbot(task)">⚡</span>
            <span v-else>👤</span>
          </template>
          {{ getTaskAccountDisplay(task, accountDisplayName) }}
        </n-tag>
      </span>
    </td>

    <!-- 状态 Tag 列 (使用缓存的 health computed) -->
    <td>
      <div class="status-cell-wrapper">
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

        <!-- 稳态降档徽标 -->
        <span
          v-if="task.task_type === 'pikpak_publish' && health.isAutoDownshifted"
          title="已在无人值守下自动降档为保守稳态 (单通道/20s发送延迟)"
        >
          <n-tag size="tiny" type="info" round class="downshift-badge font-mono"> 🛡️ 稳态降档 </n-tag>
        </span>
      </div>
    </td>

    <!-- 实时进度列 -->
    <td class="progress-col">
      <div class="progress-col-content">
        <div class="progress-meta font-mono">
          <span class="progress-meta-left">
            <span>{{ formatTaskProgress(task) }}</span>
            <span v-if="itemsBadge" class="progress-items-badge">{{ itemsBadge }}</span>
          </span>
          <span>{{ calcTaskPercent(task) }}%</span>
        </div>
        <n-progress
          type="line"
          :class="{ 'running-progress-emerald': task.status === 'running' }"
          :percentage="calcTaskPercent(task)"
          :status="getProgressStatus(task.status)"
          :processing="task.status === 'running'"
          :show-indicator="false"
          :height="6"
        />
        <!-- 阶梯冷却倒计时说明条 -->
        <div
          v-if="task.task_type === 'pikpak_publish' && health.isCooldown"
          class="cooldown-notice-bar font-mono"
          :title="health.description"
        >
          <span class="cooldown-dot">⏳</span> {{ health.label }}
        </div>
        <!-- 安全挂起说明条 -->
        <div
          v-else-if="task.task_type === 'pikpak_publish' && health.isSafePaused"
          class="safe-paused-notice-bar font-mono text-truncate"
          :title="health.description"
        >
          <span>🟠 [风控保护]</span> {{ health.description }}
        </div>
        <div
          v-else-if="task.progress.current_item"
          class="current-item-sub font-mono text-truncate"
          :title="task.progress.current_item"
        >
          {{ task.progress.current_item }}
        </div>
      </div>
    </td>

    <!-- 实时速率 / ETA 列 -->
    <td>
      <div class="speed-eta-cell font-mono">
        <span
          v-if="task.status === 'running'"
          class="speed-val"
          :class="task.progress.speed_mb_s > 0 ? 'text-emerald' : 'text-secondary'"
        >
          ⚡ {{ (task.progress.speed_mb_s || 0).toFixed(2) }} MB/s
        </span>
        <span v-else class="text-secondary">-</span>
        <!-- PikPak 双速列 -->
        <span v-if="task.task_type === 'pikpak_publish' && task.status === 'running'" class="eta-val text-secondary">
          ⬇ {{ (task.progress.pikpak_download_speed_mb_s || 0).toFixed(2) }} MB/s (PikPak)
        </span>
        <span v-if="task.status === 'running'" class="eta-val text-secondary">
          {{
            task.progress.eta_s > 0
              ? `ETA: ${formatDurationLocalized(task.progress.eta_s, 'zh-CN', 'digital')}`
              : 'ETA: --'
          }}
        </span>
      </div>
    </td>

    <!-- 并发通道 / 在途 / DC 节点列 -->
    <td @click.stop>
      <div class="dc-threads-cell font-mono">
        <n-popover
          v-if="
            ['running', 'pending', 'paused'].includes(task.status) &&
            (['download_link', 'download_range'].includes(task.task_type) || isUploadTask(task))
          "
          trigger="click"
          placement="bottom"
        >
          <template #trigger>
            <span class="threads-adjust-trigger" title="点击调节实时并发与在途数">
              <template v-if="isUploadTask(task)">
                {{ task.progress.concurrency || 0 }}/{{ task.progress.concurrency_total || 2 }} 连接 ·
                {{ getTaskInFlight(task) }} 在途 ⚙️
              </template>
              <template v-else>
                {{ task.progress.concurrency || 0 }}/{{ task.progress.concurrency_total || 16 }} 通道 · - ⚙️
              </template>
            </span>
          </template>
          <div class="quick-concurrency-popover">
            <div class="popover-title">
              {{ isUploadTask(task) ? '上传通道与在途调节' : '实时并发调节 (4的倍数)' }}
            </div>
            <div class="popover-body">
              <div class="popover-row">
                <span class="popover-label">并发通道:</span>
                <n-input-number
                  :value="task.progress.concurrency_total || (isUploadTask(task) ? 2 : 16)"
                  :min="isUploadTask(task) ? 1 : 4"
                  :max="isUploadTask(task) ? 16 : 32"
                  :step="isUploadTask(task) ? 1 : 4"
                  size="small"
                  style="width: 105px"
                  @update:value="
                    (val: number | null) =>
                      $emit(
                        'update-concurrency',
                        task.id,
                        val || (isUploadTask(task) ? 2 : 16),
                        isUploadTask(task) ? 16 : 32,
                        isUploadTask(task),
                        task,
                      )
                  "
                />
                <span class="popover-hint"> ({{ isUploadTask(task) ? '1~16' : '4~32' }}) </span>
              </div>
              <div v-if="isUploadTask(task)" class="popover-row">
                <span class="popover-label">单连接在途:</span>
                <n-input-number
                  :value="getTaskInFlight(task)"
                  :min="1"
                  :max="4"
                  :step="1"
                  size="small"
                  style="width: 105px"
                  @update:value="(val: number | null) => $emit('update-inflight', task.id, val || 1, task)"
                />
                <span class="popover-hint"> (1~4) </span>
              </div>
            </div>
          </div>
        </n-popover>
        <span v-else>
          <template v-if="isUploadTask(task)">
            {{ task.progress.concurrency || 0 }}/{{ task.progress.concurrency_total || 2 }} 连接 ·
            {{ getTaskInFlight(task) }} 在途
          </template>
          <template v-else>
            {{ task.progress.concurrency || 0 }}/{{ task.progress.concurrency_total || 16 }} 通道 · -
          </template>
        </span>
        <span class="text-secondary">{{ formatDcLocation(task.progress.dc_id, true) }}</span>
      </div>
    </td>

    <!-- 操作按钮列 -->
    <td class="action-cell" @click.stop>
      <template v-if="task.status === 'running'">
        <span data-testid="task-pause-btn"
          ><n-button size="tiny" type="warning" secondary @click="$emit('pause', task.id)"> 暂停 </n-button></span
        >
        <span data-testid="task-cancel-btn"
          ><n-button size="tiny" type="error" secondary @click="$emit('cancel', task)"> 取消 </n-button></span
        >
      </template>

      <template v-else-if="task.task_type === 'pikpak_publish' && health.isCooldown">
        <span data-testid="task-resume-btn"
          ><n-button size="tiny" type="success" secondary @click="$emit('resume', task.id)"> 恢复 </n-button></span
        >
        <span data-testid="task-pause-btn"
          ><n-button size="tiny" type="warning" secondary @click="$emit('pause', task.id)"> 暂停 </n-button></span
        >
        <span data-testid="task-cancel-btn"
          ><n-button size="tiny" type="error" secondary @click="$emit('cancel', task)"> 取消 </n-button></span
        >
      </template>

      <template v-else-if="task.task_type === 'pikpak_publish' && health.isSafePaused">
        <n-button size="tiny" type="primary" secondary @click="$emit('resume', task.id, 'steady')"> 稳态恢复 </n-button>
        <span data-testid="task-resume-btn"
          ><n-button size="tiny" secondary @click="$emit('resume', task.id, 'normal')"> 恢复 </n-button></span
        >
        <span data-testid="task-cancel-btn"
          ><n-button size="tiny" type="error" secondary @click="$emit('cancel', task)"> 取消 </n-button></span
        >
      </template>

      <template v-else-if="task.status === 'paused'">
        <span data-testid="task-resume-btn"
          ><n-button size="tiny" type="primary" secondary @click="$emit('resume', task.id)"> 恢复 </n-button></span
        >
        <span data-testid="task-cancel-btn"
          ><n-button size="tiny" type="error" secondary @click="$emit('cancel', task)"> 取消 </n-button></span
        >
      </template>

      <span v-else-if="task.status === 'queued'" data-testid="task-cancel-btn">
        <n-button size="tiny" type="error" secondary @click="$emit('cancel', task)"> 取消 </n-button>
      </span>

      <template v-else-if="task.status === 'failed' || task.status === 'cancelled'">
        <n-button size="tiny" type="primary" secondary @click="$emit('retry', task.id)"> 重试 </n-button>
        <n-button size="tiny" type="error" secondary @click="$emit('delete', task)"> 删除 </n-button>
      </template>

      <template v-else>
        <n-button size="tiny" secondary type="default" @click="$emit('delete', task)"> 删除 </n-button>
      </template>

      <span data-testid="task-detail-btn"
        ><n-button size="tiny" type="info" secondary @click="$emit('open-drawer', task.id)"> 详情 </n-button></span
      >
    </td>
  </tr>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { NButton, NTag, NProgress, NCheckbox, NPopover, NInputNumber } from 'naive-ui'
import type { NormalizedTask } from '@/utils/taskNormalizer'
import {
  calcTaskPercent,
  formatTaskProgress,
  getTaskAccountDisplay,
  isMultiUserbot,
  getStatusTagType,
  getProgressStatus,
  getTypeLabel,
  getTypeTagType,
  isUploadTask,
  getTaskInFlight,
} from '@/utils/taskHelpers'
import { formatDurationLocalized, formatDcLocation } from '@/utils/formatters'
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
  (e: 'update-concurrency', taskId: string, val: number, maxVal: number, isUpload: boolean, task: NormalizedTask): void
  (e: 'update-inflight', taskId: string, val: number, task: NormalizedTask): void
}>()

// 核心优化：单行内仅计算一次 health，避免每秒全表高频重算
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
.task-row {
  cursor: pointer;
  transition: background-color 0.15s ease;
  content-visibility: auto;
  contain-intrinsic-size: auto 54px;
}

.task-row:hover {
  background-color: var(--table-row-hover);
}

.task-row.is-selected {
  background-color: var(--table-row-selected, rgba(59, 130, 246, 0.08));
}

.task-row.is-active {
  background-color: var(--table-row-active, rgba(59, 130, 246, 0.12));
}

.checkbox-cell {
  text-align: center;
}

.task-id-cell {
  font-size: 0.85rem;
}

.id-wrapper {
  display: flex;
  align-items: center;
  gap: 0.35rem;
}

.id-text {
  font-weight: 600;
  color: var(--text-primary);
  max-width: 95px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.copy-btn {
  background: none;
  border: none;
  cursor: pointer;
  padding: 0;
  font-size: 0.85rem;
  opacity: 0.6;
  transition: opacity 0.15s ease;
}

.copy-btn:hover {
  opacity: 1;
}

.userbot-tag {
  max-width: 140px;
}

.userbot-tag.is-multi {
  border: 1px solid rgba(16, 185, 129, 0.4);
}

.status-cell-wrapper {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.pulse-dot-inline {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background-color: #10b981;
  display: inline-block;
  margin-right: 4px;
  animation: pulse-dot 1.5s infinite ease-in-out;
}

.dot-warning {
  background-color: #f59e0b;
}

@keyframes pulse-dot {
  0%,
  100% {
    transform: scale(0.9);
    opacity: 0.8;
  }
  50% {
    transform: scale(1.3);
    opacity: 1;
  }
}

.progress-col {
  vertical-align: middle;
}

.progress-col-content {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
}

.progress-meta {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 0.78rem;
  color: var(--text-secondary);
}

.progress-meta-left {
  display: flex;
  align-items: center;
  gap: 0.35rem;
}

.progress-items-badge {
  font-size: 0.72rem;
  color: var(--accent-color);
}

.cooldown-notice-bar {
  font-size: 0.75rem;
  color: #f59e0b;
  display: flex;
  align-items: center;
  gap: 0.25rem;
}

.safe-paused-notice-bar {
  font-size: 0.75rem;
  color: #f97316;
}

.current-item-sub {
  font-size: 0.75rem;
  color: var(--text-secondary);
  max-width: 260px;
}

.text-truncate {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.speed-eta-cell {
  display: flex;
  flex-direction: column;
  gap: 0.2rem;
  font-size: 0.8rem;
}

.speed-val {
  font-weight: 600;
}

.eta-val {
  font-size: 0.75rem;
}

.dc-threads-cell {
  display: flex;
  flex-direction: column;
  gap: 0.2rem;
  font-size: 0.8rem;
}

.threads-adjust-trigger {
  cursor: pointer;
  text-decoration: underline dotted;
  transition: color 0.15s ease;
}

.threads-adjust-trigger:hover {
  color: var(--accent-color);
}

.action-cell {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 0.35rem;
  flex-wrap: wrap;
}

.quick-concurrency-popover {
  padding: 0.5rem;
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.popover-title {
  font-weight: 600;
  font-size: 0.85rem;
  margin-bottom: 0.25rem;
}

.popover-body {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.popover-row {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.popover-label {
  font-size: 0.8rem;
  color: var(--text-secondary);
  width: 70px;
}

.popover-hint {
  font-size: 0.75rem;
  color: var(--text-secondary);
}

.text-emerald {
  color: #10b981;
}

.text-secondary {
  color: var(--text-secondary);
}
</style>
