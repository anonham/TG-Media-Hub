<template>
  <div class="table-toolbar-shell">
    <div class="table-toolbar">
      <search class="toolbar-filters">
        <span class="filter-item filter-status" data-testid="task-filter-status">
          <n-select
            :value="filterStatus"
            :options="statusFilterOptions"
            placeholder="任务状态"
            size="small"
            @update:value="(val) => $emit('update:filterStatus', val)"
          />
        </span>
        <span class="filter-item filter-type" data-testid="task-filter-type">
          <n-select
            :value="filterTaskType"
            :options="typeFilterOptions"
            placeholder="任务类型"
            size="small"
            @update:value="(val) => $emit('update:filterTaskType', val)"
          />
        </span>
        <span class="filter-item filter-account" data-testid="task-filter-account">
          <n-select
            :value="filterAccountId"
            :options="accountFilterOptions"
            placeholder="执行账号"
            size="small"
            clearable
            @update:value="(val) => $emit('update:filterAccountId', val)"
          />
        </span>
        <n-button class="filter-reset-btn" size="small" secondary @click="$emit('reset')"> 重置 </n-button>
        <span class="filter-item filter-search" data-testid="task-search-input">
          <n-input
            :value="searchQuery"
            placeholder="搜索 ID / 频道 / 路径..."
            size="small"
            clearable
            @update:value="(val) => $emit('update:searchQuery', val)"
          >
            <template #prefix>🔍</template>
          </n-input>
        </span>
      </search>

      <div class="toolbar-actions">
        <n-button
          size="small"
          secondary
          type="warning"
          :disabled="statusCounts.running + statusCounts.queued === 0"
          @click="$emit('pause-queue')"
        >
          ⏸️ 暂停队列
        </n-button>
        <n-button
          size="small"
          secondary
          type="success"
          :disabled="statusCounts.paused === 0"
          @click="$emit('resume-queue')"
        >
          ▶️ 恢复队列
        </n-button>
        <n-button size="small" secondary type="primary" @click="$emit('refresh')"> 🔄 刷新列表 </n-button>
        <n-button
          size="small"
          secondary
          type="warning"
          :disabled="statusCounts.completed + statusCounts.cancelled + statusCounts.failed === 0"
          @click="$emit('clear-finished')"
        >
          🧹 清理已结束
        </n-button>
        <span data-testid="create-task-btn" @click="$emit('create')"
          ><n-button size="small" type="primary"> ➕ 新建任务 </n-button></span
        >
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { NSelect, NInput, NButton } from 'naive-ui'
import type { SelectOption } from 'naive-ui'
import type { TaskStatusCounts } from './TaskMetricsHeader.vue'

defineProps<{
  filterStatus: string
  filterTaskType: string
  filterAccountId: number | null
  searchQuery: string
  statusFilterOptions: SelectOption[]
  typeFilterOptions: SelectOption[]
  accountFilterOptions: SelectOption[]
  statusCounts: TaskStatusCounts
}>()

defineEmits<{
  (e: 'update:filterStatus', val: string): void
  (e: 'update:filterTaskType', val: string): void
  (e: 'update:filterAccountId', val: number | null): void
  (e: 'update:searchQuery', val: string): void
  (e: 'reset'): void
  (e: 'pause-queue'): void
  (e: 'resume-queue'): void
  (e: 'refresh'): void
  (e: 'clear-finished'): void
  (e: 'create'): void
}>()
</script>

<style scoped>
.table-toolbar-shell {
  width: 100%;
  container: filter-toolbar / inline-size;
}

.table-toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 1rem;
  flex-wrap: wrap;
  width: 100%;
  padding: 0.5rem 0 1rem;
  border-bottom: 1px solid var(--border-color);
}

.toolbar-filters {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  flex-wrap: wrap;
}

.filter-status {
  width: 130px;
}

.filter-type {
  width: 140px;
}

.filter-account {
  width: 170px;
}

.filter-search {
  width: 220px;
}

.filter-item :deep(.n-select),
.filter-item :deep(.n-input) {
  width: 100%;
}

.toolbar-actions {
  display: flex;
  align-items: center;
  gap: 0.65rem;
  flex-wrap: wrap;
}

@container filter-toolbar (max-width: 720px) {
  .table-toolbar {
    flex-direction: column;
    align-items: stretch;
    gap: 0.75rem;
  }

  .toolbar-filters {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0.5rem;
    width: 100%;
  }

  .filter-status,
  .filter-type,
  .filter-account {
    width: 100%;
  }

  .filter-search {
    grid-column: 1 / -1;
    width: 100%;
  }

  .filter-reset-btn {
    width: 100%;
  }

  .toolbar-actions {
    gap: 0.45rem;
    width: 100%;
  }
}
</style>
