<template>
  <header class="header-bar-container">
    <div class="header-left">
      <!-- 移动端汉堡菜单按钮 -->
      <button
        ref="mobileMenuBtnRef"
        type="button"
        class="mobile-menu-btn"
        aria-label="打开导航菜单"
        @click="$emit('open-mobile')"
      >
        <n-icon :size="22">
          <MenuOutline />
        </n-icon>
      </button>

      <!-- 动态面包屑与模块标题 (语义化 nav > ol > li，CSS 装饰分隔符) -->
      <nav class="breadcrumb-container" aria-label="页面位置面包屑">
        <ol class="breadcrumb-list">
          <li class="breadcrumb-item">
            <span class="breadcrumb-root">控制台</span>
          </li>
          <li class="breadcrumb-item" aria-current="page">
            <span class="breadcrumb-current">{{ currentTitle }}</span>
            <span v-if="currentSubTitle" class="breadcrumb-sub">({{ currentSubTitle }})</span>
          </li>
        </ol>
      </nav>
    </div>

    <!-- 中间全局传输速率与活跃摘要 -->
    <div class="header-center">
      <div class="transmission-summary-pill">
        <div class="summary-metric">
          <span class="metric-icon">⚡</span>
          <span class="metric-label">总速率:</span>
          <span class="metric-val font-mono">{{ currentSpeed.toFixed(2) }} MB/s</span>
        </div>
        <div class="summary-divider"></div>
        <div class="summary-metric">
          <span class="metric-icon">🚀</span>
          <span class="metric-label">活跃任务:</span>
          <span class="metric-val font-mono">{{ activeCount }}</span>
        </div>
      </div>
    </div>

    <!-- 右侧快捷操作区 -->
    <div class="header-right">
      <n-button type="primary" size="small" round class="create-task-btn" @click="handleCreateTask">
        <template #icon>
          <n-icon><AddOutline /></n-icon>
        </template>
        <span class="header-btn-text">新建任务</span>
      </n-button>

      <n-button secondary size="small" round :loading="refreshing" @click="handleGlobalRefresh">
        <template #icon>
          <n-icon><RefreshOutline /></n-icon>
        </template>
        <span class="header-btn-text">刷新</span>
      </n-button>

      <n-button
        v-if="authStore.isAuthRequired"
        secondary
        type="primary"
        size="small"
        round
        class="key-manage-btn"
        @click="authStore.openAuthModal('重置或管理您的 API Key 凭证')"
      >
        <template #icon>
          <n-icon><KeyOutline /></n-icon>
        </template>
        <span class="header-btn-text">密钥管理</span>
      </n-button>
    </div>
  </header>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { NButton, NIcon } from 'naive-ui'
import { MenuOutline, AddOutline, RefreshOutline, KeyOutline } from '@vicons/ionicons5'
import { useTaskStore } from '@/stores/taskStore'
import { useAccountStore } from '@/stores/accountStore'
import { useStorageStore } from '@/stores/storageStore'
import { useAuthStore } from '@/stores/authStore'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'
import { message } from '@/utils/discreteApi'

withDefaults(
  defineProps<{
    collapsed?: boolean
  }>(),
  {
    collapsed: false,
  },
)

defineEmits<{
  (e: 'open-mobile'): void
  (e: 'toggle-collapse'): void
}>()

const mobileMenuBtnRef = ref<HTMLButtonElement | null>(null)

function focusMobileMenuBtn() {
  mobileMenuBtnRef.value?.focus()
}

defineExpose({ focusMobileMenuBtn })

const route = useRoute()
const router = useRouter()

const taskStore = useTaskStore()
const accountStore = useAccountStore()
const storageStore = useStorageStore()
const authStore = useAuthStore()
const tgAuthStore = useTelegramAuthStore()

const refreshing = ref(false)

const currentTitle = computed(() => {
  return route.meta?.title || '控制台'
})

const currentSubTitle = computed(() => {
  return route.meta?.breadcrumb || ''
})

const currentSpeed = computed(() => {
  return taskStore.totalRunningSpeed || 0
})

// 活跃任务数: 服务端全局 status_counts, 不再遍历单页内存数组
const activeCount = computed(() => {
  return (taskStore.statusCounts?.running || 0) + (taskStore.statusCounts?.pending || 0)
})

function handleCreateTask() {
  router.push('/tasks/create')
}

async function handleGlobalRefresh() {
  refreshing.value = true
  try {
    const results = await Promise.allSettled([
      taskStore.loadTasks(),
      accountStore.loadAccounts(),
      accountStore.loadBots(),
      storageStore.loadOverview(),
      storageStore.loadMetrics(),
      tgAuthStore.fetchStatus(false),
    ])
    // 分流提示: store action 自身 toast 且不抛出, rejected 仅作防御;
    // 存储 2 项的失败经新增错误态字段观测, 不再"全部失败也报成功"
    const rejected = results.filter((r) => r.status === 'rejected').length
    const telegramStatus = results.at(-1)
    const statusFailed = telegramStatus?.status === 'fulfilled' && telegramStatus.value === false
    const failed =
      rejected +
      Number(Boolean(storageStore.overviewError)) +
      Number(Boolean(storageStore.metricsError)) +
      Number(statusFailed)
    if (failed === 0) {
      message.success('全局控制台数据已刷新')
    } else if (failed >= results.length) {
      message.error(`全局刷新失败: ${failed}/${results.length} 项数据源拉取失败`)
    } else {
      message.warning(`全局刷新完成，但 ${failed}/${results.length} 项数据源拉取失败`)
    }
  } catch (err) {
    console.error('全局刷新失败:', err)
    message.error('全局刷新执行异常')
  } finally {
    refreshing.value = false
  }
}
</script>

<style scoped>
.header-bar-container {
  min-height: calc(var(--header-height) + var(--safe-area-top));
  background: var(--bg-header);
  backdrop-filter: blur(16px);
  -webkit-backdrop-filter: blur(16px);
  border-bottom: 1px solid var(--border-color);
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--safe-area-top) max(1.5rem, var(--safe-area-right)) 0 max(1.5rem, var(--safe-area-left));
  position: sticky;
  top: 0;
  z-index: 100;
  gap: 1rem;
  view-transition-name: app-header;
}

.header-left {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  min-width: 0;
  flex: 1;
}

.mobile-menu-btn {
  display: none;
  background: transparent;
  border: 1px solid var(--border-color);
  color: var(--text-primary);
  border-radius: 0.5rem;
  width: 40px;
  height: 40px;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  flex-shrink: 0;
}

.breadcrumb-container {
  display: flex;
  align-items: center;
  min-width: 0;
  flex: 1;
}

.breadcrumb-list {
  display: flex;
  align-items: center;
  gap: 0.45rem;
  font-size: 0.88rem;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  min-width: 0;
  list-style: none;
  margin: 0;
  padding: 0;
}

.breadcrumb-item {
  display: inline-flex;
  align-items: center;
  gap: 0.45rem;
  min-width: 0;
}

.breadcrumb-item + .breadcrumb-item::before {
  /* 双声明渐进增强: 支持 content 备选文本语法的浏览器用后者 (装饰性分隔符对 AT 静音), 其余回退前者 */
  content: '/';
  content: '/' / '';
  color: var(--text-muted);
  flex-shrink: 0;
}

.breadcrumb-root {
  color: var(--text-secondary);
  flex-shrink: 0;
}

.breadcrumb-current {
  color: var(--text-primary);
  font-weight: 600;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.breadcrumb-sub {
  color: var(--text-secondary);
  font-size: 0.8rem;
  white-space: nowrap;
  flex-shrink: 0;
}

/* 全局传输摘要胶囊 */
.header-center {
  display: flex;
  align-items: center;
}

.transmission-summary-pill {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  background: var(--bg-pill-inactive);
  border: 1px solid var(--border-color);
  padding: 0.35rem 0.85rem;
  border-radius: 9999px;
  font-size: 0.82rem;
}

.summary-metric {
  display: flex;
  align-items: center;
  gap: 0.35rem;
}

.metric-icon {
  font-size: 0.9rem;
}

.metric-label {
  color: var(--text-secondary);
}

.metric-val {
  color: var(--accent-color);
  font-weight: 600;
}

.summary-divider {
  width: 1px;
  height: 12px;
  background: var(--border-color);
}

/* 右侧操作区 */
.header-right {
  display: flex;
  align-items: center;
  gap: 0.6rem;
}

.create-task-btn {
  background: var(--accent-gradient);
  border: none;
  box-shadow: 0 4px 12px rgba(37, 99, 235, 0.25);
}

@media (max-width: 900px) {
  .header-center {
    display: none;
  }
}

@media (max-width: 768px) {
  .mobile-menu-btn {
    display: flex;
  }
  .header-bar-container {
    padding: var(--safe-area-top) max(0.75rem, var(--safe-area-right)) 0 max(0.75rem, var(--safe-area-left));
    gap: 0.5rem;
  }
  .breadcrumb-sub {
    display: none;
  }
}

@media (max-width: 640px) {
  .header-btn-text {
    display: none;
  }
  .header-right {
    gap: 0.4rem;
  }
}

@media (max-width: 480px) {
  .key-manage-btn {
    display: none !important;
  }
  .breadcrumb-item:first-child {
    display: none;
  }
  .breadcrumb-item + .breadcrumb-item::before {
    display: none;
  }
}
</style>
