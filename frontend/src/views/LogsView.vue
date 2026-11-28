<template>
  <div class="logs-view-container">
    <!-- 顶部 Tab 导航卡片 -->
    <div class="logs-header-card glass-card">
      <div class="header-tab-bar">
        <n-tabs v-model:value="activeTab" type="segment" animated>
          <n-tab name="system"> 🖥️ 系统运行日志 </n-tab>
          <n-tab name="tasks"> 📜 任务执行日志 </n-tab>
          <n-tab name="skips">
            🛡️ 转发清洗账本<template v-if="loadedTabs.skips"> ({{ logStore.forwardSkipsTotal }})</template>
          </n-tab>
          <n-tab name="failures">
            ⚠️ 失败审计<template v-if="loadedTabs.publish || loadedTabs.download">
              ({{ logStore.publishFailuresTotal + logStore.downloadFailuresTotal }})</template
            >
          </n-tab>
        </n-tabs>
      </div>
    </div>

    <!-- 1. 系统运行日志 -->
    <div v-show="activeTab === 'system'" class="system-logs-section">
      <div class="card glass-card">
        <div class="toolbar-row">
          <div class="filter-group">
            <n-select
              v-model:value="logStore.systemLogsLevel"
              :options="levelOptions"
              size="small"
              placeholder="日志级别"
              class="responsive-control-xs"
              @update:value="logStore.loadSystemLogs()"
            />
            <n-input
              v-model:value="logStore.systemLogsQuery"
              placeholder="检索日志内容..."
              size="small"
              clearable
              class="responsive-control-md"
              @keydown.enter="(e: KeyboardEvent) => onImeSafeEnter(e, () => logStore.loadSystemLogs())"
              @clear="logStore.loadSystemLogs()"
            />
            <n-button size="small" secondary @click="logStore.loadSystemLogs()"> 🔍 搜索 </n-button>
          </div>

          <div class="action-group">
            <n-button
              size="small"
              secondary
              type="primary"
              :loading="logStore.loadingSystemLogs"
              @click="logStore.loadSystemLogs()"
            >
              🔄 刷新
            </n-button>
          </div>
        </div>

        <!-- 系统日志输出控制台 -->
        <div class="log-stream-terminal" tabindex="0" role="region" aria-label="系统运行日志终端">
          <div v-if="logStore.loadingSystemLogs" class="empty-tip">正在拉取系统日志...</div>
          <div v-else-if="logStore.systemLogs.length === 0" class="empty-tip">暂无匹配的系统日志记录</div>
          <div v-else class="log-stream-rows">
            <div
              v-for="(log, idx) in logStore.systemLogs"
              :key="idx"
              class="log-stream-line"
              :class="getLogLevelClass(log.level)"
            >
              <span class="log-time font-mono">[{{ log.timestamp || '系统' }}]</span>
              <span class="log-level font-mono">[{{ (log.level || 'INFO').toUpperCase() }}]</span>
              <span class="log-msg">{{ log.message }}</span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- 2. 任务执行日志 -->
    <div v-show="activeTab === 'tasks'" class="task-logs-section">
      <div class="card glass-card">
        <div class="task-logs-header">
          <div class="task-selector-wrapper">
            <label class="selector-label">选择任务:</label>
            <n-select
              v-model:value="taskStore.selectedTaskId"
              :options="taskSelectOptions"
              placeholder="请选择需查看的任务"
              size="small"
              class="responsive-control-lg"
              @update:value="handleSelectTask"
            />
          </div>
        </div>

        <TaskLogTerminal show-title :rows="12" />
      </div>
    </div>

    <!-- 3. 转发清洗账本 -->
    <div v-show="activeTab === 'skips'" class="forward-skips-section">
      <div class="card glass-card">
        <div class="toolbar-row">
          <div class="filter-group">
            <n-input
              v-model:value="logStore.forwardSkipsFilters.source_chat"
              placeholder="源频道 ID/Username"
              size="small"
              clearable
              class="responsive-control-sm"
            />
            <n-input
              v-model:value="logStore.forwardSkipsFilters.target_chat"
              placeholder="目标频道 ID/Username"
              size="small"
              clearable
              class="responsive-control-sm"
            />
            <n-select
              v-model:value="logStore.forwardSkipsFilters.skip_type"
              :options="skipTypeOptions"
              placeholder="跳过类型"
              size="small"
              clearable
              class="responsive-control-sm"
            />
            <n-button size="small" secondary @click="logStore.loadForwardSkips(true)"> 🔍 过滤 </n-button>
          </div>

          <div class="action-group">
            <n-button
              size="small"
              secondary
              type="primary"
              :loading="logStore.loadingForwardSkips"
              @click="logStore.loadForwardSkips(true)"
            >
              🔄 刷新
            </n-button>
          </div>
        </div>

        <!-- 账本表格 -->
        <div class="table-responsive-wrapper" tabindex="0" role="region" aria-label="转发清洗跳过账本表格">
          <div v-if="logStore.loadingForwardSkips" class="empty-tip">正在查询转发清洗账本...</div>
          <div v-else-if="logStore.forwardSkips.length === 0" class="empty-tip">暂无转发跳过账本记录</div>
          <table v-else class="cockpit-table">
            <thead>
              <tr>
                <th scope="col">ID</th>
                <th scope="col">关联任务</th>
                <th scope="col">源频道</th>
                <th scope="col">消息 ID</th>
                <th scope="col">目标频道</th>
                <th scope="col">跳过类型</th>
                <th scope="col">判定原因</th>
                <th scope="col">记录时间</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="item in logStore.forwardSkips" :key="item.id">
                <td class="font-mono">#{{ item.id }}</td>
                <td class="font-mono">
                  <router-link
                    v-if="item.task_id"
                    :to="`/tasks?id=${item.task_id}`"
                    class="task-link-badge"
                    title="点击直达任务中心查看此任务详情"
                  >
                    #{{ item.task_id.slice(0, 8) }}
                  </router-link>
                  <span v-else class="text-secondary">-</span>
                </td>
                <td>{{ item.source_chat }}</td>
                <td class="font-mono">{{ item.msg_id }}</td>
                <td>{{ item.target_chat || '-' }}</td>
                <td>
                  <n-tag size="small" :type="getSkipTagType(item.skip_type)" round>
                    {{ item.skip_type }}
                  </n-tag>
                </td>
                <td class="text-secondary">{{ item.reason || '-' }}</td>
                <td class="font-mono text-secondary">{{ formatDateTime(item.created_at) }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- 分页栏 -->
        <div v-if="logStore.forwardSkipsTotal > logStore.forwardSkipsLimit" class="table-pagination-bar">
          <n-pagination
            v-model:page="logStore.forwardSkipsPage"
            :page-size="logStore.forwardSkipsLimit"
            :item-count="logStore.forwardSkipsTotal"
            show-quick-jumper
            @update:page="logStore.loadForwardSkips()"
          />
        </div>
      </div>
    </div>

    <!-- 4. 失败审计 (发布失败 + 下载失败) -->
    <div v-show="activeTab === 'failures'" class="publish-failures-section">
      <div class="card glass-card">
        <!-- 二级分类切换 -->
        <div class="failure-subtabs-row">
          <n-tabs v-model:value="failureSubTab" type="line" size="medium">
            <n-tab name="publish">
              📤 发布失败<template v-if="loadedTabs.publish"> ({{ logStore.publishFailuresTotal }})</template>
            </n-tab>
            <n-tab name="download">
              📥 下载失败<template v-if="loadedTabs.download"> ({{ logStore.downloadFailuresTotal }})</template>
            </n-tab>
          </n-tabs>
        </div>

        <!-- 4.1 发布失败面板 -->
        <div v-show="failureSubTab === 'publish'">
          <div class="toolbar-row">
            <div class="filter-group">
              <n-select
                v-model:value="logStore.publishFailuresFilters.type"
                :options="failureTypeOptions"
                placeholder="失败分类"
                size="small"
                clearable
                class="responsive-control-sm"
                @update:value="logStore.loadPublishFailures(true)"
              />
              <n-input
                v-model:value="logStore.publishFailuresFilters.chat_id"
                placeholder="目标频道 ID"
                size="small"
                clearable
                class="responsive-control-sm"
                @keydown.enter="(e: KeyboardEvent) => onImeSafeEnter(e, () => logStore.loadPublishFailures(true))"
                @clear="logStore.loadPublishFailures(true)"
              />
              <n-button size="small" secondary @click="logStore.loadPublishFailures(true)"> 🔍 过滤 </n-button>
            </div>

            <div class="action-group">
              <n-button
                size="small"
                secondary
                type="primary"
                :loading="logStore.loadingPublishFailures"
                @click="logStore.loadPublishFailures(true)"
              >
                🔄 刷新
              </n-button>
            </div>
          </div>

          <!-- 发布失败表格 -->
          <div class="table-responsive-wrapper" tabindex="0" role="region" aria-label="发布失败审计记录表格">
            <div v-if="logStore.loadingPublishFailures" class="empty-tip">正在查询发布失败审计记录...</div>
            <div v-else-if="logStore.publishFailures.length === 0" class="empty-tip">暂无发布失败审计记录</div>
            <table v-else class="cockpit-table">
              <thead>
                <tr>
                  <th scope="col">时间</th>
                  <th scope="col">关联任务</th>
                  <th scope="col">目标频道</th>
                  <th scope="col">文件</th>
                  <th scope="col">大小 (MB)</th>
                  <th scope="col">执行客户端</th>
                  <th scope="col">异常类型</th>
                  <th scope="col">失败原因</th>
                  <th scope="col">可重试</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(item, idx) in logStore.publishFailures" :key="idx">
                  <td class="font-mono text-secondary">{{ item.time || '-' }}</td>
                  <td class="font-mono">
                    <span
                      v-if="item.task_id"
                      class="task-link-badge"
                      role="button"
                      tabindex="0"
                      title="点击直达任务中心查看此任务详情"
                      @click="router.push(`/tasks?id=${item.task_id}`)"
                      @keydown.enter="router.push(`/tasks?id=${item.task_id}`)"
                      @keydown.space.prevent
                      @keyup.space="router.push(`/tasks?id=${item.task_id}`)"
                    >
                      #{{ item.task_id.slice(0, 8) }}
                    </span>
                    <span v-else class="text-secondary">-</span>
                  </td>
                  <td>{{ item.chat_id || '-' }}</td>
                  <td class="file-cell" :title="item.file">{{ item.file }}</td>
                  <td class="font-mono">{{ item.size_mb }}</td>
                  <td>{{ item.client || 'Unknown' }}</td>
                  <td>
                    <n-tag size="small" :type="getFailureTagType(item.type)" round>
                      {{ item.type }}
                    </n-tag>
                  </td>
                  <td class="reason-cell text-secondary" :title="item.reason">{{ item.reason }}</td>
                  <td>
                    <div class="retry-cell-group">
                      <n-tag size="small" :type="item.retryable ? 'success' : 'default'" round>
                        {{ item.retryable ? '支持' : '否' }}
                      </n-tag>
                      <n-button
                        v-if="item.retryable && item.task_id"
                        size="tiny"
                        type="primary"
                        secondary
                        @click="handleRetryFailure(item.task_id)"
                      >
                        立即重试
                      </n-button>
                    </div>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <!-- 发布失败分页栏 -->
          <div v-if="logStore.publishFailuresTotal > logStore.publishFailuresLimit" class="table-pagination-bar">
            <n-pagination
              v-model:page="logStore.publishFailuresPage"
              :page-size="logStore.publishFailuresLimit"
              :item-count="logStore.publishFailuresTotal"
              show-quick-jumper
              @update:page="logStore.loadPublishFailures()"
            />
          </div>
        </div>

        <!-- 4.2 下载失败面板 -->
        <div v-show="failureSubTab === 'download'">
          <div class="toolbar-row">
            <div class="filter-group">
              <n-select
                v-model:value="logStore.downloadFailuresFilters.type"
                :options="downloadFailureTypeOptions"
                placeholder="失败分类"
                size="small"
                clearable
                class="responsive-control-md"
                @update:value="logStore.loadDownloadFailures(true)"
              />
              <n-input
                v-model:value="logStore.downloadFailuresFilters.chat_id"
                placeholder="频道 ID"
                size="small"
                clearable
                class="responsive-control-sm"
                @keydown.enter="(e: KeyboardEvent) => onImeSafeEnter(e, () => logStore.loadDownloadFailures(true))"
                @clear="logStore.loadDownloadFailures(true)"
              />
              <n-input
                v-model:value="logStore.downloadFailuresFilters.task_id"
                placeholder="关联任务 ID"
                size="small"
                clearable
                class="responsive-control-sm"
                @keydown.enter="(e: KeyboardEvent) => onImeSafeEnter(e, () => logStore.loadDownloadFailures(true))"
                @clear="logStore.loadDownloadFailures(true)"
              />
              <n-button size="small" secondary @click="logStore.loadDownloadFailures(true)"> 🔍 过滤 </n-button>
            </div>

            <div class="action-group">
              <n-button
                size="small"
                secondary
                type="primary"
                :loading="logStore.loadingDownloadFailures"
                @click="logStore.loadDownloadFailures(true)"
              >
                🔄 刷新
              </n-button>
            </div>
          </div>

          <!-- 下载失败表格 -->
          <div class="table-responsive-wrapper" tabindex="0" role="region" aria-label="下载失败审计记录表格">
            <div v-if="logStore.loadingDownloadFailures" class="empty-tip">正在查询下载失败审计记录...</div>
            <div v-else-if="logStore.downloadFailures.length === 0" class="empty-tip">暂无下载失败审计记录</div>
            <table v-else class="cockpit-table">
              <thead>
                <tr>
                  <th scope="col">时间</th>
                  <th scope="col">关联任务</th>
                  <th scope="col">频道 ID</th>
                  <th scope="col">文件名</th>
                  <th scope="col">大小 (MB)</th>
                  <th scope="col">失败类型</th>
                  <th scope="col">失败原因</th>
                  <th scope="col">可重试</th>
                  <th scope="col">操作</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(item, idx) in logStore.downloadFailures" :key="idx">
                  <td class="font-mono text-secondary">{{ item.time || '-' }}</td>
                  <td class="font-mono">
                    <span
                      v-if="item.task_id"
                      class="task-link-badge"
                      role="button"
                      tabindex="0"
                      title="点击直达任务中心查看此任务详情"
                      @click="router.push(`/tasks?id=${item.task_id}`)"
                      @keydown.enter="router.push(`/tasks?id=${item.task_id}`)"
                      @keydown.space.prevent
                      @keyup.space="router.push(`/tasks?id=${item.task_id}`)"
                    >
                      #{{ item.task_id.slice(0, 8) }}
                    </span>
                    <span v-else class="text-secondary">-</span>
                  </td>
                  <td>{{ item.chat_id ?? '-' }}</td>
                  <td class="file-cell" :title="item.file">{{ item.file || '-' }}</td>
                  <td class="font-mono">{{ item.size_mb != null ? item.size_mb : '-' }}</td>
                  <td>
                    <n-tag size="small" :type="getFailureTagType(item.type)" round>
                      {{ item.type }}
                    </n-tag>
                  </td>
                  <td class="reason-cell text-secondary" :title="item.reason">{{ item.reason || '-' }}</td>
                  <td>
                    <n-tag size="small" :type="item.retryable ? 'success' : 'default'" round>
                      {{ item.retryable ? '支持' : '否' }}
                    </n-tag>
                  </td>
                  <td>
                    <n-button
                      v-if="item.retryable && item.task_id"
                      size="tiny"
                      type="primary"
                      secondary
                      @click="handleRetryFailure(item.task_id)"
                    >
                      立即重试
                    </n-button>
                    <span v-else class="text-secondary">-</span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <!-- 下载失败分页栏 -->
          <div v-if="logStore.downloadFailuresTotal > logStore.downloadFailuresLimit" class="table-pagination-bar">
            <n-pagination
              v-model:page="logStore.downloadFailuresPage"
              :page-size="logStore.downloadFailuresLimit"
              :item-count="logStore.downloadFailuresTotal"
              show-quick-jumper
              @update:page="logStore.loadDownloadFailures()"
            />
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { NTabs, NTab, NButton, NTag, NSelect, NInput, NPagination } from 'naive-ui'

import TaskLogTerminal from '@/components/console/TaskLogTerminal.vue'
import { useLogStore } from '@/stores/logStore'
import { useTaskStore } from '@/stores/taskStore'
import { dialog } from '@/utils/discreteApi'
import { formatDateTime } from '@/utils/formatters'
import { onImeSafeEnter } from '@/utils/ime'

const router = useRouter()
const logStore = useLogStore()
const taskStore = useTaskStore()

async function handleRetryFailure(taskId: string | number) {
  const success = await taskStore.retryTask(String(taskId))
  if (success) {
    if (failureSubTab.value === 'download') {
      logStore.loadDownloadFailures()
    } else {
      logStore.loadPublishFailures()
    }
    dialog.success({
      title: '重试成功',
      content: `任务 #${taskId} 已重新进入调度就绪队列，是否跳转至任务中心查看？`,
      positiveText: '前往任务中心',
      negativeText: '留在本页',
      onPositiveClick: () => {
        router.push(`/tasks?id=${taskId}`)
      },
    })
  }
}

const activeTab = ref('system')
const failureSubTab = ref('publish')

// 4 个日志 Tab 按首次激活懒加载，避免挂载瞬间 4 路并发请求
const loadedTabs = ref({
  system: false,
  skips: false,
  publish: false,
  download: false,
})

function ensureTabLoaded(tab = activeTab.value, subTab = failureSubTab.value) {
  if (tab === 'system' && !loadedTabs.value.system) {
    loadedTabs.value.system = true
    logStore.loadSystemLogs()
  } else if (tab === 'skips' && !loadedTabs.value.skips) {
    loadedTabs.value.skips = true
    logStore.loadForwardSkips()
  } else if (tab === 'failures') {
    if (subTab === 'publish' && !loadedTabs.value.publish) {
      loadedTabs.value.publish = true
      logStore.loadPublishFailures()
    } else if (subTab === 'download' && !loadedTabs.value.download) {
      loadedTabs.value.download = true
      logStore.loadDownloadFailures()
    }
  }
}

watch([activeTab, failureSubTab], ([tab, subTab]) => {
  ensureTabLoaded(tab, subTab)
})

const levelOptions = [
  { label: '全部级别', value: '' },
  { label: 'INFO 信息', value: 'info' },
  { label: 'WARNING 警告', value: 'warning' },
  { label: 'ERROR 错误', value: 'error' },
]

const skipTypeOptions = [
  { label: '全部类型', value: '' },
  { label: 'bad_id (坏消息/空洞)', value: 'bad_id' },
  { label: 'duplicate (防重跳过)', value: 'duplicate' },
  { label: 'filter (业务过滤)', value: 'filter' },
]

const failureTypeOptions = [
  { label: '全部异常', value: '' },
  { label: 'FLOOD_WAIT 频控', value: 'FLOOD_WAIT' },
  { label: 'NETWORK_TIMEOUT 超时', value: 'NETWORK_TIMEOUT' },
  { label: 'FILE_TOO_LARGE 超限', value: 'FILE_TOO_LARGE' },
  { label: 'OTHER_ERROR 其他', value: 'OTHER_ERROR' },
]

const downloadFailureTypeOptions = [
  { label: '全部异常', value: '' },
  { label: 'TG_NO_PERMISSION 权限不足', value: 'TG_NO_PERMISSION' },
  { label: 'DOWNLOAD_INCOMPLETE 下载未完成', value: 'DOWNLOAD_INCOMPLETE' },
  { label: 'TIMEOUT 超时', value: 'TIMEOUT' },
  { label: 'FLOOD_WAIT 频控', value: 'FLOOD_WAIT' },
  { label: 'NETWORK_TIMEOUT 网络超时', value: 'NETWORK_TIMEOUT' },
  { label: 'FILE_TOO_LARGE 超限', value: 'FILE_TOO_LARGE' },
  { label: 'FILE_IO_ERROR 磁盘IO', value: 'FILE_IO_ERROR' },
  { label: 'USER_CANCELLED 用户取消', value: 'USER_CANCELLED' },
  { label: 'OTHER_ERROR 其他', value: 'OTHER_ERROR' },
]

const taskSelectOptions = computed(() => {
  return (taskStore.tasks || []).map((t) => ({
    label: `[${t.task_type}] ID:${t.id} (${t.status})`,
    value: t.id,
  }))
})

function handleSelectTask(taskId: string | number) {
  taskStore.selectTask(String(taskId))
}

function getLogLevelClass(level = '') {
  const l = level.toLowerCase()
  if (l === 'error') return 'level-error'
  if (l === 'warning' || l === 'warn') return 'level-warn'
  return 'level-info'
}

function getSkipTagType(type = ''): 'info' | 'warning' | 'default' {
  if (type === 'duplicate') return 'info'
  if (type === 'bad_id') return 'warning'
  if (type === 'filter') return 'default'
  return 'default'
}

function getFailureTagType(type = ''): 'warning' | 'error' | 'info' | 'default' {
  if (type === 'FLOOD_WAIT') return 'warning'
  if (type === 'NETWORK_TIMEOUT' || type === 'TIMEOUT') return 'error'
  if (type === 'FILE_TOO_LARGE') return 'error'
  if (type === 'TG_NO_PERMISSION') return 'error'
  if (type === 'DOWNLOAD_INCOMPLETE') return 'warning'
  if (type === 'FILE_IO_ERROR') return 'error'
  if (type === 'USER_CANCELLED') return 'info'
  return 'default'
}

onMounted(() => {
  ensureTabLoaded()
})
</script>

<style scoped>
.failure-subtabs-row {
  margin-bottom: 0.5rem;
  border-bottom: 1px solid var(--border-color);
  padding-bottom: 0.25rem;
}

.logs-view-container {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
}

.logs-header-card {
  padding: 0.75rem 1.25rem;
}

.header-tab-bar {
  max-width: 680px;
  margin: 0 auto;
  overflow-x: auto;
  scrollbar-width: none;
  -webkit-overflow-scrolling: touch;
}

.header-tab-bar::-webkit-scrollbar {
  display: none;
}

.header-tab-bar :deep(.n-tabs-rail) {
  min-width: max-content;
}

.card {
  padding: 1.5rem;
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
}

.toolbar-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid var(--border-color);
  padding-bottom: 0.85rem;
  margin-bottom: 1.25rem;
  gap: 1rem;
}

.filter-group {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  flex-wrap: wrap;
}

.action-group {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

/* 终端样式 */
.log-stream-terminal {
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-radius: 0.75rem;
  padding: 1rem;
  font-family: 'JetBrains Mono', 'Fira Code', Consolas, monospace;
  min-height: 480px;
  max-height: 650px;
  overflow-y: auto;
  contain: content;
}

.log-stream-terminal:focus-visible {
  outline: 2px solid var(--accent-color);
  outline-offset: -2px;
}

.log-stream-rows {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
}

.log-stream-line {
  font-size: 0.82rem;
  line-height: 1.5;
  display: flex;
  flex-wrap: wrap;
  gap: 0.25rem 0.6rem;
  content-visibility: auto;
  contain-intrinsic-size: auto 24px;
}

.log-msg {
  flex: 1 1 200px;
  min-width: 0;
  word-break: break-word;
}

.log-time {
  color: var(--text-secondary);
  flex-shrink: 0;
}

.log-level {
  flex-shrink: 0;
  font-weight: 600;
}

.level-info .log-level {
  color: var(--accent-color);
}
.level-info .log-msg {
  color: var(--text-primary);
}

.level-warn .log-level {
  color: var(--warning-color);
}
.level-warn .log-msg {
  color: var(--warning-color);
}

.level-error .log-level {
  color: var(--danger-color);
}
.level-error .log-msg {
  color: var(--danger-color);
}

.empty-tip {
  padding: 3rem 0;
  text-align: center;
  color: var(--text-secondary);
  font-size: 0.88rem;
}

/* 任务日志区 */
.task-logs-header {
  display: flex;
  align-items: center;
  margin-bottom: 1rem;
}

.task-selector-wrapper {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.6rem;
  width: 100%;
}

.selector-label {
  font-size: 0.88rem;
  color: var(--text-secondary);
  white-space: nowrap;
}

/* 表格 */
.table-wrapper {
  overflow-x: auto;
}

.text-secondary {
  color: var(--text-secondary);
}

.retry-cell-group {
  display: flex;
  align-items: center;
  gap: 0.4rem;
}

.file-cell,
.reason-cell {
  max-width: 240px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.table-pagination-bar {
  display: flex;
  justify-content: flex-end;
  margin-top: 1.25rem;
}

@media (max-width: 768px) {
  .logs-header-card {
    padding: 0.65rem 0.75rem;
  }
  .toolbar-row {
    flex-direction: column;
    align-items: stretch;
  }
  .log-stream-terminal {
    min-height: 320px;
    padding: 0.75rem;
  }
  .table-pagination-bar {
    justify-content: center;
  }
  .table-pagination-bar :deep(.n-pagination) {
    flex-wrap: wrap;
    justify-content: center;
    gap: 0.4rem;
  }
}
</style>
