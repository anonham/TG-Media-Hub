<template>
  <div class="task-log-terminal">
    <div class="terminal-toolbar">
      <span v-if="showTitle" class="terminal-title">{{ titleText }}</span>

      <div class="toolbar-filters">
        <n-select v-model:value="logLevelFilter" :options="logLevelOptions" size="small" class="level-select" />
        <n-input v-model:value="searchQuery" placeholder="搜索日志..." size="small" clearable class="search-input" />
      </div>

      <div class="toolbar-actions">
        <n-button size="small" secondary :disabled="!renderedLogText" @click="copyCurrent"> 📋 复制 </n-button>
        <n-button
          size="small"
          secondary
          type="info"
          :loading="exporting"
          :disabled="!selectedTask"
          @click="exportCurrentLog"
        >
          📥 导出 .txt
        </n-button>
      </div>
    </div>

    <div class="terminal-output" :style="{ maxHeight }" tabindex="0" role="region" aria-label="实时任务日志输出终端">
      <n-log :log="renderedLogText" :rows="rows" :font-size="12" language="log" />
    </div>
  </div>
</template>

<script setup lang="ts">
// 统一日志终端: LogConsole 与 TaskDetailDrawer Tab3 两处重复终端
// 合并为单一可复用组件 —— 级别过滤 (消息启发式) + 关键词搜索 + 一键复制 + 全量 .txt 导出。
// 导出走服务端分页拉取 (不受环形缓冲 2000 条限制), 内置 loading 防重入;
// created_at 缺失判空, 修复导出头 1970/1/1 假时间。
import { computed, ref } from 'vue'
import { NLog, NInput, NButton, NSelect } from 'naive-ui'
import { useTaskStore } from '@/stores/taskStore'
import type { TaskLogEntry } from '@/stores/taskRealtime'
import { copyTextToClipboard, exportTextFile } from '@/utils/clipboard'
import { yieldToMain } from '@/utils/scheduler'
import { formatDateTime } from '@/utils/formatters'
import { errDetail } from '@/utils/errors'
import { message } from '@/utils/discreteApi'

const props = withDefaults(
  defineProps<{
    /** n-log 渲染行数 (驱动终端高度) */
    rows?: number
    /** 是否展示选中任务标题行 (LogsView 用, Drawer 内嵌不需要) */
    showTitle?: boolean
    /** 终端输出区最大高度 */
    maxHeight?: string
  }>(),
  { rows: 12, showTitle: false, maxHeight: '250px' },
)

const taskStore = useTaskStore()
const selectedTask = computed(() => taskStore.selectedTask)

const titleText = computed(() => {
  if (!selectedTask.value) return '📜 选中任务的完整日志输出:'
  return `📜 任务 [${selectedTask.value.task_type}] ID:${selectedTask.value.id} 历史日志 (${selectedTask.value.status}):`
})

// ── 过滤状态 ─────────────────────────────────────────────────
const logLevelFilter = ref('ALL')
const searchQuery = ref('')

const logLevelOptions = [
  { label: '全部级别', value: 'ALL' },
  { label: 'INFO', value: 'INFO' },
  { label: 'WARN', value: 'WARN' },
  { label: 'ERROR', value: 'ERROR' },
]

// 服务端日志无结构化 level 字段, 按消息内容启发式分级 (与既有终端口径一致)
function matchesLevel(entry: TaskLogEntry, filterLvl: string): boolean {
  if (filterLvl === 'ALL') return true
  const msg = String(entry.message || '')
  if (filterLvl === 'ERROR') {
    return msg.includes('❌') || msg.includes('失败') || /error/i.test(msg)
  }
  if (filterLvl === 'WARN') {
    return msg.includes('⚠️') || /warn/i.test(msg)
  }
  // INFO: 排除启发式命中的 WARN/ERROR
  return !(msg.includes('❌') || msg.includes('失败') || msg.includes('⚠️') || /error|warn/i.test(msg))
}

const filteredLogs = computed<TaskLogEntry[]>(() => {
  const logs = taskStore.currentLogs || []
  const query = searchQuery.value.trim().toLowerCase()
  const filterLvl = logLevelFilter.value
  return logs.filter((l) => {
    if (!matchesLevel(l, filterLvl)) return false
    if (
      query &&
      !String(l.message || '')
        .toLowerCase()
        .includes(query)
    )
      return false
    return true
  })
})

const renderedLogText = computed<string>(() => {
  if ((taskStore.currentLogs || []).length === 0) {
    return '[系统] 暂无选中的任务日志'
  }
  if (filteredLogs.value.length === 0) {
    return '[系统] 未找到匹配检索条件的日志'
  }
  return filteredLogs.value.map((l) => `[${l.time || '系统'}] ${l.message}`).join('\n')
})

function copyCurrent() {
  copyTextToClipboard(renderedLogText.value, '日志文本已复制到剪贴板')
}

// ── 全量导出 (服务端分页拉取, loading 防重入 ) ──────────
const exporting = ref(false)

async function exportCurrentLog() {
  const task = selectedTask.value
  if (!task) {
    message.warning('当前无选中的任务日志')
    return
  }
  if (exporting.value) return
  exporting.value = true
  try {
    const allLogs = await taskStore.fetchAllLogs(task.id)
    if (!allLogs.length) {
      message.warning('该任务暂无日志')
      return
    }
    // 让渡主线程执行权 (INP 优化): 避免大日志文本序列化阻塞 UI 交互渲染
    await yieldToMain()
    // created_at 缺失判空, 不再回退 1970/1/1
    let content = `任务类型: ${task.task_type}\n任务 ID: ${task.id}\n状态: ${task.status}\n创建时间: ${task.created_at ? formatDateTime(task.created_at) : '未知'}\n----------------------------------------\n`
    content += allLogs.map((l) => `[${l.time || '系统'}] ${l.message}`).join('\n')
    exportTextFile(content, `task_log_${task.id}.txt`)
    message.success(`任务 ${task.id} 日志文件已成功导出 (共 ${allLogs.length} 条)！`)
  } catch (err) {
    message.error(`拉取全量日志失败: ${errDetail(err)}`)
  } finally {
    exporting.value = false
  }
}
</script>

<style scoped>
.task-log-terminal {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  width: 100%;
  container: log-terminal / inline-size;
}

.terminal-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
  flex-wrap: wrap;
}

.terminal-title {
  font-size: 0.85rem;
  font-weight: 600;
  color: var(--text-secondary);
}

.toolbar-filters {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.5rem;
}

.level-select {
  width: 110px;
}

.search-input {
  width: 180px;
}

@container log-terminal (max-width: 620px) {
  .toolbar-filters {
    width: 100%;
  }
  .level-select {
    width: 105px;
    flex-shrink: 0;
  }
  .search-input {
    flex: 1 1 150px;
    width: auto;
  }
}

.toolbar-actions {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.terminal-output {
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-radius: 0.5rem;
  padding: 0.5rem;
  font-family: 'JetBrains Mono', monospace;
  overflow: hidden;
  contain: content;
}

.terminal-output:focus-visible {
  outline: 2px solid var(--accent-color);
  outline-offset: -2px;
}

:deep(.n-log) {
  background: transparent !important;
  color: var(--text-primary) !important;
  font-family: 'JetBrains Mono', monospace !important;
}

:deep(.n-log-loader) {
  color: var(--text-secondary);
}
</style>
