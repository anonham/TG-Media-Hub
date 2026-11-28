<template>
  <div class="tab-pane-content">
    <!-- 核心指标摘要 -->
    <div class="metadata-grid">
      <div class="meta-card">
        <div class="meta-label">任务类型</div>
        <div class="meta-value">{{ task.task_type }}</div>
      </div>
      <div class="meta-card">
        <div class="meta-label">并发通道数</div>
        <div class="meta-value font-mono">
          <n-input-number
            v-if="adjustable"
            :value="currentTaskConcurrency"
            :min="minTaskConcurrency"
            :max="maxTaskConcurrency"
            :step="stepTaskConcurrency"
            size="tiny"
            :loading="updatingConcurrency"
            class="meta-input-number"
            @update:value="(v) => v !== null && emit('update-concurrency', v)"
          />
          <span v-else
            >{{ task.progress.concurrency_total || (isUploadTask ? 2 : 16) }} {{ isUploadTask ? '连接' : '线程' }}</span
          >
        </div>
      </div>
      <div class="meta-card" v-if="isUploadTask">
        <div class="meta-label">单连接在途 (Pipelining)</div>
        <div class="meta-value font-mono">
          <n-input-number
            v-if="['running', 'pending', 'paused'].includes(task.status)"
            :value="currentTaskInFlight"
            :min="1"
            :max="4"
            :step="1"
            size="tiny"
            :disabled="updatingConcurrency"
            class="meta-input-number"
            @update:value="(v) => v !== null && emit('update-in-flight', v)"
          />
          <span v-else>{{ currentTaskInFlight }} 深度</span>
        </div>
      </div>
      <div class="meta-card">
        <div class="meta-label">调度优先级</div>
        <div class="meta-value font-mono">
          <n-tag
            size="small"
            :type="(task.priority ?? 10) > 15 ? 'error' : (task.priority ?? 10) > 10 ? 'warning' : 'default'"
          >
            P{{ task.priority || 10 }}
          </n-tag>
        </div>
      </div>
      <div class="meta-card">
        <div class="meta-label">执行 Userbot</div>
        <div class="meta-value font-mono" :title="isMultiUserbot ? '跨账号联合并发加速下载' : undefined">
          <n-tag v-if="isMultiUserbot" size="small" type="success" :bordered="false" round class="font-mono">
            ⚡ {{ assignedAccountLabel }}
          </n-tag>
          <n-tag
            v-else-if="assignedAccountLabel !== '系统智能调度'"
            size="small"
            :bordered="false"
            round
            class="font-mono"
          >
            👤 {{ assignedAccountLabel }}
          </n-tag>
          <span v-else>{{ assignedAccountLabel }}</span>
        </div>
      </div>
      <div class="meta-card">
        <div class="meta-label">重试次数</div>
        <div class="meta-value font-mono">{{ task.retry_count || 0 }} / {{ task.max_retries || 3 }}</div>
      </div>
      <div class="meta-card">
        <div class="meta-label">创建时间</div>
        <div class="meta-value font-mono">{{ formatDateTime(task.created_at) }}</div>
      </div>
      <div class="meta-card">
        <div class="meta-label">总已耗时</div>
        <div class="meta-value font-mono">{{ formatDuration(task.created_at, task.updated_at) }}</div>
      </div>
    </div>

    <!-- 错误信息提示卡片 -->
    <div v-if="task.error_detail || task.status === 'failed'" class="error-detail-card">
      <div class="error-header">
        <span class="error-icon">❌</span>
        <span class="error-title">异常诊断信息</span>
      </div>
      <div class="error-content font-mono">
        {{ task.error_detail || '任务执行过程出现未捕获异常导致失败，请查看流式日志获取排查线索。' }}
      </div>
    </div>

    <!-- 原始参数 JSON -->
    <div class="card glass-card section-box">
      <div class="box-header">
        <span class="box-title">结构化入参字典 (Parameters)</span>
        <n-button size="tiny" secondary type="primary" @click="copyJsonParams"> 📋 复制 JSON </n-button>
      </div>
      <!-- PikPak 发布参数摘要 (数据源 = 后端已解析的 task.params 对象; 前端从不解析 params_json 字符串) -->
      <div v-if="isPikpakTask" class="pikpak-params-summary">
        <div class="pikpak-summary-title">📦 PikPak 发布参数</div>
        <div class="pikpak-summary-grid">
          <div class="pikpak-summary-item">
            <span class="pikpak-summary-label">目标频道</span>
            <span class="pikpak-summary-value font-mono">{{ task.params?.target_channel || '—' }}</span>
          </div>
          <div class="pikpak-summary-item">
            <span class="pikpak-summary-label">发布文件</span>
            <span class="pikpak-summary-value">{{ pikpakFileCount }} 个</span>
          </div>
          <div class="pikpak-summary-item">
            <span class="pikpak-summary-label">发布后清理</span>
            <span class="pikpak-summary-value">{{
              CLEANUP_MODE_LABELS[String(task.params?.auto_cleanup_mode || '')] || task.params?.auto_cleanup_mode || '—'
            }}</span>
          </div>
          <div class="pikpak-summary-item">
            <span class="pikpak-summary-label">上传并发</span>
            <span class="pikpak-summary-value font-mono">{{ pikpakConcurrencyDisplay }}</span>
          </div>
          <div class="pikpak-summary-item">
            <span class="pikpak-summary-label">超限处理策略</span>
            <span class="pikpak-summary-value">{{
              OVERSIZE_POLICY_LABELS[String(task.params?.oversize_policy || '')] || task.params?.oversize_policy || '—'
            }}</span>
          </div>
          <div class="pikpak-summary-item" v-if="task.params?.split_strategy">
            <span class="pikpak-summary-label">切片策略</span>
            <span class="pikpak-summary-value font-mono">{{
              SPLIT_STRATEGY_LABELS[String(task.params?.split_strategy || '')] || task.params?.split_strategy
            }}</span>
          </div>
          <div class="pikpak-summary-item" v-if="task.params?.send_placeholder_card">
            <span class="pikpak-summary-label">缺集占位说明卡</span>
            <span class="pikpak-summary-value">开启</span>
          </div>
        </div>
        <div v-if="pikpakFileCount > 0" class="pikpak-summary-files">
          <span class="pikpak-summary-label">文件清单</span>
          <span class="pikpak-file-names font-mono">
            {{ pikpakFileNamesPreview
            }}<template v-if="pikpakFileCount > pikpakPreviewLimit"> 等 {{ pikpakFileCount }} 个文件</template>
          </span>
        </div>
      </div>

      <pre class="params-json-viewer font-mono">{{ formattedParamsJson }}</pre>
    </div>
  </div>
</template>

<script setup lang="ts">
// 任务参数与元数据 Tab: 核心指标网格 / 异常诊断卡 / PikPak 参数摘要 /
// 结构化入参 JSON 查看器。并发调节的写入由父级 Drawer 统一持有 (两个 Tab 共用)。
import { computed } from 'vue'
import { NButton, NInputNumber, NTag } from 'naive-ui'
import { useAccountStore } from '@/stores/accountStore'
import { copyTextToClipboard } from '@/utils/clipboard'
import { formatDateTime, formatDuration } from '@/utils/formatters'
import {
  getTaskAccountDisplay,
  isMultiUserbot as isTaskMultiUserbot,
  isUploadTask as checkUploadTask,
} from '@/utils/taskHelpers'
import { CLEANUP_MODE_LABELS, OVERSIZE_POLICY_LABELS, SPLIT_STRATEGY_LABELS } from '@/utils/publishConstants'
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

const accountStore = useAccountStore()

const isUploadTask = computed(() => checkUploadTask(props.task))

const adjustable = computed(
  () =>
    ['running', 'pending', 'paused'].includes(props.task.status) &&
    (['download_link', 'download_range'].includes(props.task.task_type) || isUploadTask.value),
)

const isMultiUserbot = computed(() => isTaskMultiUserbot(props.task))
const assignedAccountLabel = computed(() => getTaskAccountDisplay(props.task, accountStore.getAccountDisplayName))

const formattedParamsJson = computed(() => {
  if (!props.task?.params) return '{}'
  try {
    return JSON.stringify(props.task.params, null, 2)
  } catch {
    return String(props.task.params)
  }
})

function copyJsonParams() {
  copyTextToClipboard(formattedParamsJson.value, '任务入参 JSON 已复制到剪贴板')
}

// ── PikPak 发布参数摘要 (P4-T3; 数据源仅 task.params 对象, 不解析 params_json 字符串) ──
const isPikpakTask = computed(() => props.task.task_type === 'pikpak_publish')

const pikpakFileList = computed(() => {
  const rf = props.task?.params?.remote_files
  return Array.isArray(rf) ? rf : []
})

const pikpakFileCount = computed(() => pikpakFileList.value.length)
const pikpakPreviewLimit = 5

const pikpakFileNamesPreview = computed(() => {
  const names = pikpakFileList.value.slice(0, pikpakPreviewLimit).map((f) => f?.name || f?.file_id || '(未命名)')
  return names.join('、')
})

const pikpakConcurrencyDisplay = computed(() => {
  const p = props.task?.params || {}
  const c = p.upload_concurrency
  const f = p.upload_in_flight_per_connection
  if (!c && !f) return '默认（2 通道 × 1 在途）'
  return `${c || 2} 通道 × ${f || 1} 在途`
})
</script>

<style scoped>
.tab-pane-content {
  display: flex;
  flex-direction: column;
  gap: 1rem;
  padding-top: 0.5rem;
  width: 100%;
  container: params-tab / inline-size;
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
}
.box-title {
  font-size: 0.95rem;
  font-weight: 700;
  color: var(--text-primary);
}
/* 元数据样式 */
.metadata-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 0.75rem;
}
.meta-card {
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 0.65rem 0.85rem;
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  min-width: 0;
}
.meta-input-number {
  width: 100px;
  max-width: 100%;
}
.meta-label {
  font-size: 0.72rem;
  color: var(--text-secondary);
}
.meta-value {
  font-size: 0.85rem;
  font-weight: 600;
  color: var(--text-primary);
}
.error-detail-card {
  background: rgba(239, 68, 68, 0.1);
  border: 1px solid rgba(239, 68, 68, 0.3);
  border-radius: 8px;
  padding: 0.85rem;
  display: flex;
  flex-direction: column;
  gap: 0.4rem;
}
.error-header {
  display: flex;
  align-items: center;
  gap: 0.4rem;
  font-weight: 700;
  color: var(--danger-color);
  font-size: 0.85rem;
}
.error-content {
  font-size: 0.78rem;
  color: var(--danger-color);
  white-space: pre-wrap;
  word-break: break-all;
  max-height: 120px;
  overflow-y: auto;
}
.params-json-viewer {
  background: var(--bg-primary);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  padding: 0.85rem;
  font-size: 0.75rem;
  color: var(--text-primary);
  max-height: 240px;
  max-width: 100%;
  overflow-y: auto;
  overflow-x: auto;
  margin: 0;
}
/* PikPak 发布参数摘要 (P4-T3) */
.pikpak-params-summary {
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 0.75rem 0.85rem;
  margin-bottom: 0.75rem;
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}
.pikpak-summary-title {
  font-size: 0.85rem;
  font-weight: 600;
}
.pikpak-summary-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 0.45rem 1rem;
}
.pikpak-summary-item {
  display: flex;
  align-items: baseline;
  gap: 0.45rem;
  min-width: 0;
}
.pikpak-summary-label {
  font-size: 0.75rem;
  color: var(--text-secondary);
  flex-shrink: 0;
}
.pikpak-summary-value {
  font-size: 0.8rem;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.pikpak-summary-files {
  display: flex;
  align-items: baseline;
  gap: 0.45rem;
  border-top: 1px dashed var(--border-color);
  padding-top: 0.5rem;
}
.pikpak-file-names {
  font-size: 0.75rem;
  word-break: break-all;
}

@container params-tab (max-width: 560px) {
  .metadata-grid {
    grid-template-columns: repeat(2, 1fr);
  }
  .pikpak-summary-grid {
    grid-template-columns: 1fr;
  }
  .meta-input-number {
    width: 100%;
  }
}

@container params-tab (max-width: 340px) {
  .metadata-grid {
    grid-template-columns: 1fr;
  }
}
</style>
