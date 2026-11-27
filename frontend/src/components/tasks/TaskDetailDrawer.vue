<template>
  <n-drawer
    :show="show"
    :width="drawerWidth"
    placement="right"
    :trap-focus="false"
    :block-scroll="false"
    class="task-detail-drawer"
    @update:show="handleUpdateShow"
  >
    <n-drawer-content :native-scrollbar="false" closable>
      <!-- 自定义抽屉头部 -->
      <template #header>
        <div class="drawer-header-row">
          <div class="drawer-header-left">
            <n-tag :type="getTypeTagType(task?.task_type)" size="small" round>
              {{ getTypeLabel(task?.task_type) }}
            </n-tag>
            <button type="button" class="task-id-badge font-mono" title="点击复制任务 ID" @click="copyTaskId">
              #{{ task?.id || '-' }}
              <span class="copy-icon">📋</span>
            </button>
            <n-tag
              :type="getStatusTagType(task?.status)"
              size="small"
              round
              :class="{ 'status-tag-running': task?.status === 'running' }"
            >
              {{ task?.status || '-' }}
            </n-tag>
            <span
              v-if="task?.task_type === 'pikpak_publish' && pikpakHealth.status !== 'idle'"
              :title="pikpakHealth.description"
            >
              <n-tag :type="pikpakHealth.badgeType" size="small" round>
                {{ pikpakHealth.badgeText }}
              </n-tag>
            </span>
            <n-tag v-if="newlyPublishedCount !== null" type="info" size="small" round>
              本次新发 {{ newlyPublishedCount }} 个文件
            </n-tag>
            <n-tag v-if="newlyPublishedCount !== null || skippedFilesCount > 0" type="warning" size="small" round>
              跳过 {{ skippedFilesCount }} 个文件
            </n-tag>
          </div>

          <div class="drawer-header-actions" v-if="task">
            <template v-if="task.status === 'running'">
              <n-button size="tiny" type="warning" secondary @click="handlePause"> ⏸️ 暂停 </n-button>
              <n-button size="tiny" type="error" secondary @click="handleCancel"> ⏹️ 取消 </n-button>
            </template>
            <template v-else-if="task.task_type === 'pikpak_publish' && pikpakHealth.isCooldown">
              <n-button size="tiny" type="success" secondary @click="handleResume"> ▶️ 立即恢复 </n-button>
              <n-button size="tiny" type="warning" secondary @click="handlePause"> ⏸️ 暂停 </n-button>
              <n-button size="tiny" type="error" secondary @click="handleCancel"> ⏹️ 取消 </n-button>
            </template>
            <template v-else-if="task.task_type === 'pikpak_publish' && pikpakHealth.isSafePaused">
              <n-button size="tiny" type="primary" secondary @click="handleSteadyResume"> 🛡️ 稳态恢复 </n-button>
              <n-button size="tiny" secondary @click="handleResume"> ▶️ 原速恢复 </n-button>
              <n-button size="tiny" type="error" secondary @click="handleCancel"> ⏹️ 取消 </n-button>
            </template>
            <template v-else-if="task.status === 'paused'">
              <n-button size="tiny" type="primary" secondary @click="handleResume"> ▶️ 恢复 </n-button>
              <n-button size="tiny" type="error" secondary @click="handleCancel"> ⏹️ 取消 </n-button>
            </template>
            <template v-else-if="task.status === 'failed' || task.status === 'cancelled'">
              <n-button size="tiny" type="primary" secondary @click="handleRetry"> 🔄 重试 </n-button>
              <n-button size="tiny" type="error" secondary @click="handleDelete"> 🗑️ 删除 </n-button>
            </template>
            <template v-else>
              <n-button size="tiny" secondary type="default" @click="handleDelete"> 🗑️ 删除 </n-button>
            </template>
          </div>
        </div>
      </template>

      <!-- 抽屉主体内容区 -->
      <div v-if="loading && !task" class="drawer-loading">
        <n-spin size="large" description="正在加载任务元数据..." />
      </div>

      <div v-else-if="!task" class="drawer-empty">
        <n-empty description="未找到对应任务信息或任务已被删除">
          <template #extra>
            <n-button secondary type="primary" size="small" @click="handleBackToBoard"> 返回任务看板 </n-button>
          </template>
        </n-empty>
      </div>

      <div v-else class="drawer-body" data-testid="task-detail-drawer">
        <n-tabs v-model:value="activeTab" type="line" animated>
          <!-- Tab 1: 实时监控看板 -->
          <n-tab-pane name="metrics">
            <template #tab>
              <span data-testid="tab-metrics">⚡ 实时监控看板</span>
            </template>
            <TaskRealtimeMetricsTab
              :task="task"
              :current-task-concurrency="currentTaskConcurrency"
              :current-task-in-flight="currentTaskInFlight"
              :min-task-concurrency="minTaskConcurrency"
              :max-task-concurrency="maxTaskConcurrency"
              :step-task-concurrency="stepTaskConcurrency"
              :updating-concurrency="updatingConcurrency"
              @update-concurrency="handleUpdateConcurrency"
              @update-in-flight="handleUpdateInFlight"
            />
          </n-tab-pane>

          <!-- Tab 2: 任务参数与执行元数据 -->
          <n-tab-pane name="params">
            <template #tab>
              <span data-testid="tab-params">📋 任务参数与元数据</span>
            </template>
            <TaskParamsMetadataTab
              :task="task"
              :current-task-concurrency="currentTaskConcurrency"
              :current-task-in-flight="currentTaskInFlight"
              :min-task-concurrency="minTaskConcurrency"
              :max-task-concurrency="maxTaskConcurrency"
              :step-task-concurrency="stepTaskConcurrency"
              :updating-concurrency="updatingConcurrency"
              @update-concurrency="handleUpdateConcurrency"
              @update-in-flight="handleUpdateInFlight"
            />
          </n-tab-pane>

          <!-- Tab 3: 流式日志终端 (统一组件) -->
          <n-tab-pane name="logs">
            <template #tab>
              <span data-testid="tab-logs">📜 流式日志终端</span>
            </template>
            <div class="tab-pane-content">
              <TaskLogTerminal :rows="18" max-height="none" />
            </div>
          </n-tab-pane>

          <!-- Tab 4: 专属诊断 (display-directive=show: 首次激活懒加载, 之后保活不丢状态) -->
          <n-tab-pane name="diagnosis" display-directive="show">
            <template #tab>
              <span data-testid="tab-diagnosis">🔍 专属诊断视图</span>
            </template>
            <TaskDiagnosisTab :task="task" />
          </n-tab-pane>
        </n-tabs>
      </div>
    </n-drawer-content>
  </n-drawer>
</template>

<script setup lang="ts">
// 任务详情抽屉主文件: 抽屉壳 + 头部任务操作 + 并发调节写入单点;
// 四个 Tab 展示与数据内聚于 drawer/ 三子组件与 console/TaskLogTerminal。
import { ref, computed, watch, h } from 'vue'
import { NDrawer, NDrawerContent, NTabs, NTabPane, NTag, NButton, NSpin, NEmpty, NCheckbox } from 'naive-ui'
import { useTaskStore } from '@/stores/taskStore'
import { dialog } from '@/utils/discreteApi'
import { copyTextToClipboard } from '@/utils/clipboard'
import {
  getStatusTagType,
  getTaskInFlight,
  getTypeLabel,
  getTypeTagType,
  isUploadTask as checkUploadTask,
} from '@/utils/taskHelpers'
import { getPikPakChannelHealth } from '@/utils/pikpakHealth'
import { useMobileViewport } from '@/utils/viewport'
import type { NormalizedTask } from '@/utils/taskNormalizer'
import TaskLogTerminal from '@/components/console/TaskLogTerminal.vue'
import TaskRealtimeMetricsTab from '@/components/tasks/drawer/TaskRealtimeMetricsTab.vue'
import TaskParamsMetadataTab from '@/components/tasks/drawer/TaskParamsMetadataTab.vue'
import TaskDiagnosisTab from '@/components/tasks/drawer/TaskDiagnosisTab.vue'

const props = withDefaults(
  defineProps<{
    show?: boolean
    taskId?: string | null
  }>(),
  {
    show: false,
    taskId: null,
  },
)

const emit = defineEmits<{
  (e: 'update:show', value: boolean): void
  (e: 'close'): void
}>()

const taskStore = useTaskStore()

const activeTab = ref('metrics')
const loading = ref(false)
const { isMobileViewport } = useMobileViewport(768)
const drawerWidth = computed(() => (isMobileViewport.value ? '100%' : '680px'))

// 当前选中的任务实体 (优先本地 tasks 查找，其次单任务拉取回填；严格核对 id 防止串任务)
const singleTaskDetail = ref<NormalizedTask | null>(null)
const updatingConcurrency = ref(false)
let detailRequestSeq = 0
let concurrencyUpdateSeq = 0
let detailUpdateSeq = 0
let appliedDetailUpdateSeq = 0

const task = computed<NormalizedTask | null>(() => {
  if (!props.taskId) return null
  const storeTask = taskStore.tasks.find((t) => t.id === props.taskId)
  if (storeTask) return storeTask
  return singleTaskDetail.value?.id === props.taskId ? singleTaskDetail.value : null
})

const skippedFilesCount = computed(() => {
  const sf = task.value?.params?.skipped_files
  return Array.isArray(sf) ? sf.length : 0
})

const newlyPublishedCount = computed(() => {
  const current = task.value
  if (!current || (current.status !== 'completed' && current.status !== 'failed')) return null
  const count = current.params?.newly_published_count
  return typeof count === 'number' ? count : null
})

const pikpakHealth = computed(() => getPikPakChannelHealth(task.value))

watch(
  [() => props.taskId, () => props.show],
  async ([newId, isShow], prev) => {
    const reqSeq = ++detailRequestSeq
    if (prev && newId !== prev[0]) {
      concurrencyUpdateSeq += 1
      appliedDetailUpdateSeq = detailUpdateSeq
      updatingConcurrency.value = false
    }
    if (singleTaskDetail.value && singleTaskDetail.value.id !== newId) {
      singleTaskDetail.value = null
    }
    if (!newId || !isShow) {
      loading.value = false
      return
    }
    loading.value = true
    try {
      await taskStore.selectTask(newId)
      if (reqSeq !== detailRequestSeq || !props.show || props.taskId !== newId) return
      if (!taskStore.tasks.some((t) => t.id === newId)) {
        const detail = await taskStore.fetchTaskById(newId)
        if (reqSeq !== detailRequestSeq || !props.show || props.taskId !== newId) return
        singleTaskDetail.value = detail
      }
    } finally {
      if (reqSeq === detailRequestSeq) {
        loading.value = false
      }
    }
  },
  { immediate: true },
)

// ── 并发/在途调节 (指标与参数两 Tab 共用的写入单点) ──
const isUploadTask = computed(() => checkUploadTask(task.value))

const currentTaskConcurrency = computed(() => {
  if (!task.value) return 16
  const def = isUploadTask.value ? 2 : 16
  return Number(
    task.value.progress.concurrency_total || task.value.params?.threads || task.value.params?.upload_concurrency || def,
  )
})

const maxTaskConcurrency = computed(() => {
  if (!task.value) return 32
  return isUploadTask.value ? 16 : 32
})

const minTaskConcurrency = computed(() => {
  if (!task.value) return 1
  return isUploadTask.value ? 1 : 4
})

const stepTaskConcurrency = computed(() => {
  if (!task.value) return 1
  return isUploadTask.value ? 1 : 4
})

const currentTaskInFlight = computed(() => getTaskInFlight(task.value))

async function handleUpdateConcurrency(newVal: number) {
  if (!task.value || !newVal) return
  const targetTaskId = task.value.id
  const inFlight = currentTaskInFlight.value
  const max = maxTaskConcurrency.value
  const isUpload = isUploadTask.value
  let normalized: number
  if (isUpload) {
    normalized = Math.max(1, Math.min(Math.round(newVal), 16))
  } else {
    normalized = Math.round(newVal / 4) * 4
    normalized = Math.max(4, Math.min(normalized, max))
  }
  const seq = ++concurrencyUpdateSeq
  const updateSeq = ++detailUpdateSeq
  updatingConcurrency.value = true
  try {
    const success = await taskStore.updateTaskConcurrency(targetTaskId, normalized, inFlight)
    if (
      success &&
      updateSeq > appliedDetailUpdateSeq &&
      singleTaskDetail.value &&
      singleTaskDetail.value.id === targetTaskId
    ) {
      appliedDetailUpdateSeq = updateSeq
      singleTaskDetail.value.progress.concurrency_total = normalized
      if (!singleTaskDetail.value.params) singleTaskDetail.value.params = {}
      if (isUpload) {
        singleTaskDetail.value.params.upload_concurrency = normalized
      } else {
        singleTaskDetail.value.params.threads = normalized
      }
      singleTaskDetail.value.params.upload_in_flight_per_connection = inFlight
    }
  } finally {
    if (seq === concurrencyUpdateSeq) {
      updatingConcurrency.value = false
    }
  }
}

async function handleUpdateInFlight(newVal: number) {
  if (!task.value || !newVal) return
  const targetTaskId = task.value.id
  const concurrency = currentTaskConcurrency.value
  const isUpload = isUploadTask.value
  const val = Math.max(1, Math.min(Math.trunc(Number(newVal)), 4))
  const seq = ++concurrencyUpdateSeq
  const updateSeq = ++detailUpdateSeq
  updatingConcurrency.value = true
  try {
    const success = await taskStore.updateTaskConcurrency(targetTaskId, concurrency, val)
    if (
      success &&
      updateSeq > appliedDetailUpdateSeq &&
      singleTaskDetail.value &&
      singleTaskDetail.value.id === targetTaskId
    ) {
      appliedDetailUpdateSeq = updateSeq
      if (!singleTaskDetail.value.params) singleTaskDetail.value.params = {}
      singleTaskDetail.value.progress.concurrency_total = concurrency
      if (isUpload) {
        singleTaskDetail.value.params.upload_concurrency = concurrency
      } else {
        singleTaskDetail.value.params.threads = concurrency
      }
      singleTaskDetail.value.params.upload_in_flight_per_connection = val
    }
  } finally {
    if (seq === concurrencyUpdateSeq) {
      updatingConcurrency.value = false
    }
  }
}

function handlePause() {
  if (task.value) taskStore.pauseTask(task.value.id)
}
function handleResume() {
  if (task.value) taskStore.resumeTask(task.value.id)
}
function handleSteadyResume() {
  if (task.value) taskStore.resumeTask(task.value.id, 'steady')
}
function handleRetry() {
  if (task.value) taskStore.retryTask(task.value.id)
}
function handleCancel() {
  if (!task.value) return
  const targetId = task.value.id
  dialog.warning({
    title: '取消任务',
    content: `确定取消任务 [${task.value.task_type}] #${targetId} 吗？`,
    positiveText: '确认取消',
    negativeText: '放弃',
    onPositiveClick: () => {
      taskStore.cancelTask(targetId)
    },
  })
}

function handleDelete() {
  if (!task.value) return
  const targetId = task.value.id
  const isDownload = String(task.value.task_type || '').startsWith('download')
  const deleteFilesRef = ref(false)
  dialog.warning({
    title: '删除任务记录',
    content: () =>
      h('div', { style: 'display:flex;flex-direction:column;gap:10px;margin-top:4px' }, [
        h('span', `确定物理删除任务 #${targetId} 吗？操作不可恢复。`),
        isDownload
          ? h(
              NCheckbox,
              {
                size: 'small',
                checked: deleteFilesRef.value,
                'onUpdate:checked': (v: boolean) => {
                  deleteFilesRef.value = v
                },
              },
              { default: () => '同时删除关联的断点文件 (.part/.meta)' },
            )
          : null,
      ]),
    positiveText: '确认删除',
    negativeText: '放弃',
    onPositiveClick: () => {
      taskStore.deleteTask(targetId, isDownload && deleteFilesRef.value)
      handleUpdateShow(false)
    },
  })
}

function handleBackToBoard() {
  emit('update:show', false)
  emit('close')
}
function handleUpdateShow(val: boolean) {
  emit('update:show', val)
  if (!val) {
    emit('close')
  }
}
function copyTaskId() {
  if (!task.value) return
  copyTextToClipboard(task.value.id, `任务 ID #${task.value.id} 已复制到剪贴板`)
}
</script>

<style scoped>
.drawer-header-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.5rem;
  width: 100%;
  min-width: 0;
}
.drawer-header-left {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.5rem;
  min-width: 0;
}
.task-id-badge {
  /* span→button 语义化转换的 UA 样式复位: 按钮默认 buttontext 颜色与 13.33px 字号会泄漏 */
  font: inherit;
  color: inherit;
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  padding: 0.15rem 0.5rem;
  border-radius: 6px;
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  gap: 0.25rem;
  font-weight: 700;
  white-space: nowrap;
  flex-shrink: 0;
  transition: all 0.2s;
}
.task-id-badge:hover {
  border-color: var(--accent-color);
  color: var(--accent-color);
}
.copy-icon {
  font-size: 0.75rem;
}
.drawer-header-actions {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.4rem;
}
.drawer-body {
  display: flex;
  flex-direction: column;
  gap: 1rem;
  width: 100%;
  min-width: 0;
  container: task-panel / inline-size;
}

.tab-pane-content {
  display: flex;
  flex-direction: column;
  gap: 1rem;
  padding-top: 0.5rem;
}
.drawer-loading,
.drawer-empty {
  display: flex;
  justify-content: center;
  align-items: center;
  min-height: 350px;
}
</style>

<style>
.task-detail-drawer .n-drawer-header {
  border-bottom: 1px solid var(--border-color);
  padding: 1rem 1.25rem;
}

@media (max-width: 768px) {
  .task-detail-drawer .n-drawer-header {
    padding: calc(0.75rem + var(--safe-area-top)) 0.85rem 0.75rem;
  }
  .task-detail-drawer .n-drawer-body-content-wrapper {
    padding: 0.75rem 0.85rem calc(1rem + var(--safe-area-bottom));
  }
}
</style>
