<template>
  <div class="task-center-page">
    <!-- 1. 顶部全局监控指标卡片 -->
    <TaskMetricsHeader
      :total-running-speed="taskStore.totalRunningSpeed || 0"
      :status-counts="statusCounts"
      @filter-status="setStatusFilter"
    />

    <!-- 2. 主卡片：多维筛选与数据表格主控区 -->
    <div class="card glass-card main-table-card">
      <!-- 多维筛选工具栏 -->
      <TaskFilterToolbar
        v-model:filter-status="filterStatus"
        v-model:filter-task-type="filterTaskType"
        v-model:filter-account-id="filterAccountId"
        v-model:search-query="searchQuery"
        :status-filter-options="statusFilterOptions"
        :type-filter-options="typeFilterOptions"
        :account-filter-options="accountFilterOptions"
        :status-counts="statusCounts"
        @reset="resetFilters"
        @pause-queue="handlePauseQueue"
        @resume-queue="handleResumeQueue"
        @refresh="refreshTasks"
        @clear-finished="handleClearFinished"
        @create="router.push('/tasks/create')"
      />

      <!-- 移动端视图控制与全选条 -->
      <div v-if="isMobile && filteredTasks.length > 0" class="mobile-list-controls">
        <n-checkbox :checked="isAllSelected" :indeterminate="isIndeterminate" @update:checked="toggleSelectAll">
          全选本页
        </n-checkbox>
        <n-button size="tiny" secondary @click="forceTableOnMobile = !forceTableOnMobile">
          {{ forceTableOnMobile ? '📱 切换卡片视图' : '📊 切换表格视图' }}
        </n-button>
      </div>

      <!-- 空状态 -->
      <div v-if="filteredTasks.length === 0" class="empty-table-tip">
        <n-empty description="暂无符合筛选条件或调度队列中的任务记录" />
      </div>

      <!-- 移动端高密度卡片流 (<= 768px 默认启用) -->
      <div v-else-if="isMobile && !forceTableOnMobile" class="mobile-card-list" role="region" aria-label="任务卡片列表">
        <TaskMobileCard
          v-for="t in filteredTasks"
          :key="t.id"
          :task="t"
          :is-selected="selectedTaskIds.includes(t.id)"
          :is-active="t.id === drawerTaskId && showDrawer"
          :current-time="currentTime"
          :account-display-name="accountStore.getAccountDisplayName"
          @select="toggleSelectRow"
          @open-drawer="openDrawer"
          @pause="taskStore.pauseTask"
          @resume="(id: string, mode?: 'normal' | 'steady') => taskStore.resumeTask(id, mode)"
          @cancel="handleCancelTask"
          @retry="taskStore.retryTask"
          @delete="handleDeleteTask"
          @copy-id="copyId"
        />
      </div>

      <!-- 桌面端多维数据表格 -->
      <div v-else class="table-responsive-wrapper" tabindex="0" role="region" aria-label="任务数据表格">
        <table class="cockpit-table">
          <thead>
            <tr>
              <th scope="col" style="width: 44px; text-align: center">
                <n-checkbox
                  :checked="isAllSelected"
                  :indeterminate="isIndeterminate"
                  @update:checked="toggleSelectAll"
                />
              </th>
              <th scope="col" style="width: 140px">任务 ID</th>
              <th scope="col" style="width: 130px">任务类型</th>
              <th scope="col" style="width: 150px">执行 Userbot</th>
              <th scope="col" style="width: 100px">状态</th>
              <th scope="col" style="min-width: 200px">实时进度</th>
              <th scope="col" style="width: 130px">实时速率 / ETA</th>
              <th scope="col" style="width: 165px">并发 / 在途 / DC</th>
              <th scope="col" style="width: 170px; text-align: right">操作</th>
            </tr>
          </thead>
          <tbody>
            <TaskTableRow
              v-for="t in filteredTasks"
              :key="t.id"
              :task="t"
              :is-selected="selectedTaskIds.includes(t.id)"
              :is-active="t.id === drawerTaskId && showDrawer"
              :current-time="currentTime"
              :account-display-name="accountStore.getAccountDisplayName"
              @select="toggleSelectRow"
              @open-drawer="openDrawer"
              @pause="taskStore.pauseTask"
              @resume="(id: string, mode?: 'normal' | 'steady') => taskStore.resumeTask(id, mode)"
              @cancel="handleCancelTask"
              @retry="taskStore.retryTask"
              @delete="handleDeleteTask"
              @copy-id="copyId"
              @update-concurrency="handleQuickUpdateConcurrency"
              @update-inflight="handleQuickUpdateInFlight"
            />
          </tbody>
        </table>
      </div>

      <!-- 服务端驱动的真分页工具栏 -->
      <div class="pagination-footer" v-if="totalTasks > 0">
        <n-pagination
          :page="currentPage"
          :page-size="pageSize"
          :item-count="totalTasks"
          :page-sizes="[20, 50, 100]"
          :page-slot="isMobile ? 5 : 7"
          :show-size-picker="!isMobile"
          :show-quick-jumper="!isMobile"
          @update:page="handlePageChange"
          @update:page-size="handlePageSizeChange"
        >
          <template #prefix="{ itemCount }"> 共 {{ itemCount }} 项任务 </template>
        </n-pagination>
      </div>
    </div>

    <!-- 3. 底部浮动批量操作栏 (Batch Action Bar) -->
    <TaskBatchActionBar
      :selected-count="selectedTaskIds.length"
      @clear="clearSelection"
      @batch-pause="handleBatchPause"
      @batch-resume="handleBatchResume"
      @batch-retry="handleBatchRetry"
      @batch-cancel="handleBatchCancel"
      @batch-delete="handleBatchDelete"
    />

    <!-- 4. 全景侧滑抽屉 (Task Detail Drawer) -->
    <TaskDetailDrawer v-model:show="showDrawer" :task-id="drawerTaskId" @close="handleCloseDrawer" />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch, h } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { NButton, NEmpty, NCheckbox, NPagination, type SelectOption } from 'naive-ui'
import { useTaskStore } from '@/stores/taskStore'
import { useAccountStore } from '@/stores/accountStore'
import { message, dialog } from '@/utils/discreteApi'
import { copyTextToClipboard } from '@/utils/clipboard'
import { isUploadTask, getTaskInFlight } from '@/utils/taskHelpers'
import { useMobileViewport } from '@/utils/viewport'
import type { NormalizedTask } from '@/utils/taskNormalizer'

import TaskMetricsHeader from '@/components/tasks/TaskMetricsHeader.vue'
import TaskFilterToolbar from '@/components/tasks/TaskFilterToolbar.vue'
import TaskBatchActionBar from '@/components/tasks/TaskBatchActionBar.vue'
import TaskTableRow from '@/components/tasks/TaskTableRow.vue'
import TaskMobileCard from '@/components/tasks/TaskMobileCard.vue'
import TaskDetailDrawer from '@/components/tasks/TaskDetailDrawer.vue'

const route = useRoute()
const router = useRouter()

const taskStore = useTaskStore()
const accountStore = useAccountStore()
const { isMobileViewport: isMobile } = useMobileViewport(768)
const forceTableOnMobile = ref(false)

// 实时时间戳用于驱动冷却倒计时 (1秒更新一次)
const currentTime = ref(Date.now() / 1000)
let timeTimer: ReturnType<typeof setInterval> | null = null

// 多维筛选器状态: 默认与 taskStore.taskFilters 对齐 (解决跨页面状态分裂)
const filterStatus = ref(taskStore.taskFilters.status || 'all')
const filterTaskType = ref(taskStore.taskFilters.task_type || 'all')
const filterAccountId = ref<number | null>(taskStore.taskFilters.account_id ?? null)
const searchQuery = ref(taskStore.taskFilters.query || '')

// 抽屉状态
const showDrawer = ref(false)
const drawerTaskId = ref<string | null>(null)

// 多选表格行状态 (selectedTasksCache 缓存跨页勾选的任务快照)
const selectedTaskIds = ref<string[]>([])
const selectedTasksCache = new Map<string, NormalizedTask>()

function clearSelection() {
  selectedTaskIds.value = []
  selectedTasksCache.clear()
}

function getSelectedTask(id: string): NormalizedTask | null {
  return taskStore.tasks.find((item) => item.id === id) || selectedTasksCache.get(id) || null
}

// 筛选下拉选项
const statusFilterOptions = [
  { label: '全部状态', value: 'all' },
  { label: '运行中 (running)', value: 'running' },
  { label: '排队中 (queued/pending)', value: 'queued' },
  { label: '已暂停 (paused)', value: 'paused' },
  { label: '失败 (failed)', value: 'failed' },
  { label: '已完成 (completed)', value: 'completed' },
  { label: '已取消 (cancelled)', value: 'cancelled' },
]

const typeFilterOptions = [
  { label: '全部类型', value: 'all' },
  { label: '受限单链下载', value: 'download_link' },
  { label: '频道范围下载', value: 'download_range' },
  { label: '本地相册发布', value: 'publish_folder' },
  { label: '云端极速转发', value: 'forward_channel' },
  { label: 'PikPak 搬运', value: 'pikpak_publish' },
]

const accountFilterOptions = computed<SelectOption[]>(() => {
  const options: SelectOption[] = [{ label: '全部执行账号', value: '' }]
  for (const acc of accountStore.accounts) {
    options.push({
      label: accountStore.getAccountDisplayName(acc.id),
      value: acc.id,
    })
  }
  return options
})

// 各状态任务计数: 服务端全局 status_counts 单一数据源
const statusCounts = computed(() => {
  const sc = taskStore.statusCounts || {}
  return {
    running: sc.running || 0,
    queued: (sc.queued || 0) + (sc.pending || 0),
    paused: sc.paused || 0,
    failed: sc.failed || 0,
    completed: sc.completed || 0,
    cancelled: sc.cancelled || 0,
  }
})

const filteredTasks = computed(() => taskStore.tasks || [])

// 全选与半选状态
const isAllSelected = computed(() => {
  if (filteredTasks.value.length === 0) return false
  return filteredTasks.value.every((t) => selectedTaskIds.value.includes(t.id))
})

const isIndeterminate = computed(() => {
  const selectedInFiltered = filteredTasks.value.filter((t) => selectedTaskIds.value.includes(t.id)).length
  return selectedInFiltered > 0 && selectedInFiltered < filteredTasks.value.length
})

function toggleSelectAll(checked: boolean) {
  if (checked) {
    for (const t of filteredTasks.value) {
      selectedTasksCache.set(t.id, t)
    }
    const ids = new Set([...selectedTaskIds.value, ...filteredTasks.value.map((t) => t.id)])
    selectedTaskIds.value = Array.from(ids)
  } else {
    const currentFilteredIds = new Set(filteredTasks.value.map((t) => t.id))
    for (const id of currentFilteredIds) {
      selectedTasksCache.delete(id)
    }
    selectedTaskIds.value = selectedTaskIds.value.filter((id) => !currentFilteredIds.has(id))
  }
}

function toggleSelectRow(taskId: string, checked: boolean) {
  if (checked) {
    const t = filteredTasks.value.find((item) => item.id === taskId)
    if (t) selectedTasksCache.set(taskId, t)
    if (!selectedTaskIds.value.includes(taskId)) {
      selectedTaskIds.value.push(taskId)
    }
  } else {
    selectedTasksCache.delete(taskId)
    selectedTaskIds.value = selectedTaskIds.value.filter((id) => id !== taskId)
  }
}

watch(
  () => taskStore.tasks,
  (list) => {
    for (const t of list || []) {
      if (selectedTasksCache.has(t.id)) {
        selectedTasksCache.set(t.id, t)
      }
    }
  },
)

// 抽屉开启与关闭逻辑 (与 URL 同步)
function openDrawer(taskId: string) {
  drawerTaskId.value = taskId
  showDrawer.value = true
  router.replace({ query: { ...route.query, id: taskId } })
}

function handleCloseDrawer() {
  showDrawer.value = false
  drawerTaskId.value = null
  const newQuery = { ...route.query }
  delete newQuery.id
  router.replace({ query: newQuery })
}

// 顶部指标点击快捷过滤
function setStatusFilter(status: string) {
  filterStatus.value = status
}

function syncFilterQueryToUrl() {
  const nextQuery = { ...route.query }
  if (filterStatus.value && filterStatus.value !== 'all') {
    nextQuery.status = filterStatus.value
  } else {
    delete nextQuery.status
  }
  if (filterTaskType.value && filterTaskType.value !== 'all') {
    nextQuery.task_type = filterTaskType.value
  } else {
    delete nextQuery.task_type
  }
  if (filterAccountId.value !== null && filterAccountId.value !== undefined && String(filterAccountId.value) !== '') {
    nextQuery.account_id = String(filterAccountId.value)
  } else {
    delete nextQuery.account_id
  }

  const currentStatus = typeof route.query.status === 'string' ? route.query.status : undefined
  const currentTaskType = typeof route.query.task_type === 'string' ? route.query.task_type : undefined
  const currentAccountId = typeof route.query.account_id === 'string' ? route.query.account_id : undefined
  if (
    currentStatus !== nextQuery.status ||
    currentTaskType !== nextQuery.task_type ||
    currentAccountId !== nextQuery.account_id
  ) {
    router.replace({ query: nextQuery })
  }
}

function resetFilters() {
  clearSelection()
  filterStatus.value = 'all'
  filterTaskType.value = 'all'
  filterAccountId.value = null
  searchQuery.value = ''
  taskStore.taskFilters = {
    status: 'all',
    task_type: 'all',
    account_id: null,
    query: '',
  }
  taskStore.currentPage = 1
  syncFilterQueryToUrl()
  taskStore.loadTasks({ page: 1 })
}

// 服务端真分页联动
const currentPage = computed(() => taskStore.currentPage)
const pageSize = computed(() => taskStore.pageSize)
const totalTasks = computed(() => taskStore.totalTasks)

function handlePageChange(page: number) {
  taskStore.currentPage = page
  taskStore.loadTasks({ page })
}

function handlePageSizeChange(limit: number) {
  taskStore.pageSize = limit
  taskStore.currentPage = 1
  taskStore.loadTasks({ page: 1, limit })
}

let searchDebounceTimer: ReturnType<typeof setTimeout> | null = null

function startTimeTimer() {
  if (timeTimer) return
  timeTimer = setInterval(() => {
    currentTime.value = Date.now() / 1000
  }, 1000)
}

function stopTimeTimer() {
  if (timeTimer) {
    clearInterval(timeTimer)
    timeTimer = null
  }
}

function handleVisibilityChange() {
  if (typeof document !== 'undefined' && document.hidden) {
    stopTimeTimer()
  } else {
    currentTime.value = Date.now() / 1000
    startTimeTimer()
  }
}

onUnmounted(() => {
  if (searchDebounceTimer) {
    clearTimeout(searchDebounceTimer)
    searchDebounceTimer = null
  }
  stopTimeTimer()
  if (typeof document !== 'undefined') {
    document.removeEventListener('visibilitychange', handleVisibilityChange)
  }
})

watch([filterStatus, filterTaskType, filterAccountId], () => {
  clearSelection()
  taskStore.taskFilters = {
    status: filterStatus.value,
    task_type: filterTaskType.value,
    account_id: filterAccountId.value,
    query: searchQuery.value,
  }
  taskStore.currentPage = 1
  syncFilterQueryToUrl()
  taskStore.loadTasks({ page: 1 })
})

watch(searchQuery, () => {
  if (searchDebounceTimer) clearTimeout(searchDebounceTimer)
  searchDebounceTimer = setTimeout(() => {
    clearSelection()
    taskStore.taskFilters = {
      status: filterStatus.value,
      task_type: filterTaskType.value,
      account_id: filterAccountId.value,
      query: searchQuery.value,
    }
    taskStore.currentPage = 1
    taskStore.loadTasks({ page: 1 })
  }, 300)
})

function refreshTasks() {
  taskStore.loadTasks()
  accountStore.loadAccounts()
}

function copyId(id: string) {
  copyTextToClipboard(id, `任务 ID #${id} 已复制`)
}

// 全局队列控制处理
function handlePauseQueue() {
  dialog.warning({
    title: '暂停整个下载/处理队列',
    content: '将立即暂停当前所有正在运行及排队中的活跃任务，彻底释放底层网络连接与并发槽位。确定暂停吗？',
    positiveText: '确认暂停',
    negativeText: '取消',
    onPositiveClick: async () => {
      await taskStore.pauseQueue()
    },
  })
}

function handleResumeQueue() {
  dialog.info({
    title: '一键恢复整个队列',
    content: '将所有已暂停的任务重新加入排队就绪队列，调度器将按单账号与全局并发上限依次拉起。确定恢复吗？',
    positiveText: '确认恢复',
    negativeText: '取消',
    onPositiveClick: async () => {
      await taskStore.resumeQueue()
    },
  })
}

// 批量操作处理
async function runBatchAction(action: string, targets: string[], deleteFiles = false) {
  const result = await taskStore.batchAction(action, targets, deleteFiles)
  if (!result) return

  const failures = result.results.filter((item) => !item.success)
  if (action === 'resume' || action === 'retry') {
    const succeeded = new Set(result.results.filter((item) => item.success).map((item) => item.task_id))
    selectedTaskIds.value = selectedTaskIds.value.filter((id) => !succeeded.has(id))
    for (const id of succeeded) selectedTasksCache.delete(id)
  }
  if (failures.length) {
    dialog.warning({
      title: '批量操作未完成的任务',
      content: () =>
        h(
          'ul',
          { style: { overflowWrap: 'anywhere', maxHeight: '50vh', overflowY: 'auto' } },
          failures.map((item) => h('li', { key: item.task_id }, `#${item.task_id} · ${item.error || '请刷新后重试'}`)),
        ),
      positiveText: '关闭',
    })
  }
}

async function handleBatchPause() {
  const targets = selectedTaskIds.value.filter((id) => {
    const t = getSelectedTask(id)
    return t && t.status === 'running'
  })
  if (targets.length === 0) {
    message.warning('选中的任务中没有正在运行的任务')
    return
  }
  await runBatchAction('pause', targets)
}

async function handleBatchResume() {
  const targets = selectedTaskIds.value.filter((id) => {
    const t = getSelectedTask(id)
    return t && t.status === 'paused'
  })
  if (targets.length === 0) {
    message.warning('选中的任务中没有已暂停的任务')
    return
  }
  await runBatchAction('resume', targets)
}

async function handleBatchRetry() {
  const targets = selectedTaskIds.value.filter((id) => {
    const t = getSelectedTask(id)
    return t && (t.status === 'failed' || t.status === 'cancelled' || t.status === 'paused')
  })
  if (targets.length === 0) {
    message.warning('选中的任务中没有可重试的任务 (仅失败/取消/暂停任务支持)')
    return
  }
  await runBatchAction('retry', targets)
}

function handleBatchCancel() {
  const targets = selectedTaskIds.value.filter((id) => {
    const t = getSelectedTask(id)
    return t && (t.status === 'running' || t.status === 'paused' || t.status === 'queued')
  })
  if (targets.length === 0) {
    message.warning('选中的任务中没有运行/排队中的活跃任务')
    return
  }
  dialog.warning({
    title: '批量取消任务',
    content: `确定批量取消选中的 ${targets.length} 个活跃任务吗？`,
    positiveText: '确认取消',
    negativeText: '放弃',
    onPositiveClick: async () => {
      await runBatchAction('cancel', targets)
    },
  })
}

function handleBatchDelete() {
  const targets = [...selectedTaskIds.value]
  if (targets.length === 0) return
  const deleteFilesRef = ref(false)
  dialog.warning({
    title: '批量物理删除任务',
    content: () =>
      h('div', { style: 'display:flex;flex-direction:column;gap:10px;margin-top:4px' }, [
        h('span', `确定删除选中的 ${targets.length} 个任务记录吗？该操作不可撤销。`),
        h(
          NCheckbox,
          {
            size: 'small',
            checked: deleteFilesRef.value,
            'onUpdate:checked': (v: boolean) => {
              deleteFilesRef.value = v
            },
          },
          { default: () => '同时清理关联的断点碎片 (.part/.meta)' },
        ),
      ]),
    positiveText: '确认删除',
    negativeText: '取消',
    onPositiveClick: async () => {
      await runBatchAction('delete', targets, deleteFilesRef.value)
      clearSelection()
    },
  })
}

function handleCancelTask(t: NormalizedTask) {
  dialog.warning({
    title: '取消任务',
    content: `确定取消任务 [${t.task_type}] ID: ${t.id} 吗？已完成部分将保留断点。`,
    positiveText: '确认取消',
    negativeText: '继续运行',
    onPositiveClick: () => {
      taskStore.cancelTask(t.id)
    },
  })
}

function handleDeleteTask(t: NormalizedTask) {
  const isDownloadTask = String(t.task_type || '').startsWith('download')
  const deleteFilesRef = ref(false)
  dialog.warning({
    title: '删除任务',
    content: () =>
      h('div', { style: 'display:flex;flex-direction:column;gap:10px;margin-top:4px' }, [
        h('span', `确定删除任务 [${t.task_type}] #${t.id} 吗？`),
        isDownloadTask
          ? h(
              NCheckbox,
              {
                size: 'small',
                checked: deleteFilesRef.value,
                'onUpdate:checked': (v: boolean) => {
                  deleteFilesRef.value = v
                },
              },
              { default: () => '同时删除断点文件 (.part/.meta)' },
            )
          : null,
      ]),
    positiveText: '删除',
    negativeText: '取消',
    onPositiveClick: () => {
      taskStore.deleteTask(t.id, isDownloadTask && deleteFilesRef.value)
      toggleSelectRow(t.id, false)
    },
  })
}

function getTaskConcurrency(t: NormalizedTask | null) {
  if (!t) return 16
  const isUpload = isUploadTask(t)
  if (isUpload) {
    return t.progress.concurrency_total || Number(t.params?.upload_concurrency) || 2
  }
  return t.progress.concurrency_total || Number(t.params?.threads) || 16
}

async function handleQuickUpdateConcurrency(
  taskId: string,
  val: number,
  maxVal: number,
  isUpload: boolean = false,
  t: NormalizedTask | null = null,
) {
  if (!taskId || !val) return
  let normalized: number
  if (isUpload) {
    normalized = Math.max(1, Math.min(Math.round(val), maxVal || 16))
  } else {
    normalized = Math.max(4, Math.min(Math.round(val / 4) * 4, maxVal || 32))
  }
  const inFlight = isUpload && t ? getTaskInFlight(t) : null
  await taskStore.updateTaskConcurrency(taskId, normalized, inFlight)
}

async function handleQuickUpdateInFlight(taskId: string, val: number, t: NormalizedTask) {
  if (!taskId || !val) return
  const normalized = Math.max(1, Math.min(Math.round(val), 4))
  const concurrency = getTaskConcurrency(t)
  await taskStore.updateTaskConcurrency(taskId, concurrency, normalized)
}

function handleClearFinished() {
  dialog.warning({
    title: '清空已结束任务',
    content: '将删除所有已完成、已取消、已失败的任务记录。',
    positiveText: '清空',
    negativeText: '取消',
    onPositiveClick: () => {
      taskStore.clearFinishedTasks()
      clearSelection()
    },
  })
}

// 路由 Query 白名单防污染校验与自动修复 (SEC-001)
const VALID_STATUSES = new Set(['all', 'running', 'queued', 'pending', 'paused', 'failed', 'completed', 'cancelled'])
const VALID_TASK_TYPES = new Set([
  'all',
  'download_link',
  'download_range',
  'publish_folder',
  'forward_channel',
  'pikpak_publish',
])

function sanitizeStatus(val: unknown): string {
  if (!val || typeof val !== 'string') return 'all'
  const s = val.trim().toLowerCase()
  if (s === 'pending') return 'queued'
  return VALID_STATUSES.has(s) ? s : 'all'
}

function sanitizeTaskType(val: unknown): string {
  if (!val || typeof val !== 'string') return 'all'
  const t = val.trim().toLowerCase()
  return VALID_TASK_TYPES.has(t) ? t : 'all'
}

function sanitizeAccountId(val: unknown): number | null {
  if (val === null || val === undefined) return null
  const s = String(val).trim()
  if (/^\d+$/.test(s)) {
    const num = parseInt(s, 10)
    return num >= 0 ? num : null
  }
  return null
}

function isValidTaskId(val: unknown): val is string {
  if (!val || typeof val !== 'string') return false
  return /^[a-zA-Z0-9_-]{1,64}$/.test(val.trim())
}

// 挂载与深层链接响应
onMounted(() => {
  accountStore.loadAccounts()

  startTimeTimer()
  if (typeof document !== 'undefined') {
    document.addEventListener('visibilitychange', handleVisibilityChange)
  }

  // 路由 query 状态安全恢复与白名单清洗 (SEC-001)
  if (route.query.status) {
    const safeStatus = sanitizeStatus(route.query.status)
    filterStatus.value = safeStatus
  }
  if (route.query.account_id) {
    const safeAccId = sanitizeAccountId(route.query.account_id)
    filterAccountId.value = safeAccId
  }
  if (route.query.task_type) {
    const safeType = sanitizeTaskType(route.query.task_type)
    filterTaskType.value = safeType
  }
  if (route.query.id) {
    if (isValidTaskId(route.query.id)) {
      openDrawer(route.query.id)
    }
  }
})

// 监听 URL 变化 (支持浏览器 Back / Forward 前进后退)
watch(
  () => route.query.id,
  (newId) => {
    if (newId && isValidTaskId(newId)) {
      drawerTaskId.value = newId
      showDrawer.value = true
    } else {
      showDrawer.value = false
      drawerTaskId.value = null
    }
  },
)

watch(
  () => route.query.status,
  (newStatus) => {
    const safe = sanitizeStatus(newStatus)
    if (safe !== filterStatus.value) {
      filterStatus.value = safe
    }
  },
)

watch(
  () => route.query.account_id,
  (newAccId) => {
    const safe = sanitizeAccountId(newAccId)
    if (safe !== filterAccountId.value) {
      filterAccountId.value = safe
    }
  },
)

watch(
  () => route.query.task_type,
  (newType) => {
    const safe = sanitizeTaskType(newType)
    if (safe !== filterTaskType.value) {
      filterTaskType.value = safe
    }
  },
)
</script>

<style scoped>
.task-center-page {
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
  padding-block-end: var(--task-batch-clearance, 0px);
}

.main-table-card {
  padding: 1.25rem;
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.table-responsive-wrapper {
  overflow: auto;
  max-inline-size: 100%;
  max-block-size: min(720px, calc(100vh - 280px));
  container-type: inline-size;
}

.table-responsive-wrapper:focus-visible {
  outline: 2px solid var(--accent-color);
  outline-offset: -2px;
}

.table-responsive-wrapper :deep(.cockpit-table) {
  width: 100%;
  border-collapse: separate;
  border-spacing: 0;
  text-align: left;
  font-size: 0.85rem;
}

.table-responsive-wrapper :deep(.cockpit-table thead th) {
  position: sticky;
  inset-block-start: 0;
  z-index: 2;
  background: var(--bg-card-hover);
  color: var(--text-secondary);
  font-weight: 600;
  padding: 0.65rem 0.85rem;
  border-bottom: 1px solid var(--border-color);
  white-space: nowrap;
}

.table-responsive-wrapper :deep(.cockpit-table td) {
  padding: 0.75rem 0.85rem;
  border-bottom: 1px solid var(--border-color);
  color: var(--text-primary);
  vertical-align: middle;
}

.mobile-list-controls {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
  padding: 0.25rem 0;
}

.mobile-card-list {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.empty-table-tip {
  padding: 3rem 1rem;
  text-align: center;
}

.pagination-footer {
  display: flex;
  justify-content: flex-end;
  padding: 14px 18px;
  background: var(--bg-card-hover);
  border-top: 1px solid var(--border-color);
  border-radius: 0 0 12px 12px;
}

@media (max-width: 768px) {
  .main-table-card {
    padding: 0.85rem;
    gap: 0.75rem;
  }

  .pagination-footer {
    justify-content: center;
    padding: 10px;
  }

  .pagination-footer :deep(.n-pagination) {
    flex-wrap: wrap;
    justify-content: center;
    gap: 0.4rem;
  }
}
</style>
