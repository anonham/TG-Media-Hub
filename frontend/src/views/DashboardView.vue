<template>
  <div class="dashboard-container">
    <!-- 顶部欢迎与系统概况 -->
    <div class="dashboard-header-card glass-card">
      <div class="header-left-col">
        <h2 class="welcome-title">
          <span>欢迎使用</span>
          <span class="gradient-text"> Telegram Media Hub</span>
        </h2>
        <p class="welcome-desc">基于 FastTelethon 多通道并发加速引擎与 4-Bot 轮询调度的企业级 Telegram 媒体资源枢纽</p>
      </div>
      <div class="header-right-col">
        <div class="engine-status-pill" :class="engineStatus.color" role="status">
          <span class="pulse-dot" aria-hidden="true"></span>
          <span>{{ engineStatus.text }}</span>
        </div>
      </div>
    </div>

    <!-- 4 大核心指标监控网格 -->
    <div class="metrics-overview-grid">
      <!-- 指标 1: 实时传输吞吐 -->
      <div class="metric-card glass-card">
        <div class="metric-card-top">
          <span class="metric-title">实时传输速率</span>
          <span class="metric-icon-badge speed-icon">⚡</span>
        </div>
        <div class="metric-main-value font-mono">
          {{ taskStore.totalRunningSpeed.toFixed(2) }} <span class="unit">MB/s</span>
        </div>
        <div class="metric-sub-detail">
          <span v-if="taskStore.currentMetrics.eta > 0">
            预计剩余: {{ formatDurationLocalized(taskStore.currentMetrics.eta) }}
          </span>
          <span v-else>当前通道: {{ taskStore.currentMetrics.threads }}</span>
        </div>
      </div>

      <!-- 指标 2: 任务队列概况 (独立语义化链接，支持键盘聚焦与快捷访问) -->
      <div class="metric-card glass-card">
        <router-link to="/tasks?status=running" class="metric-card-main-link clickable">
          <div class="metric-card-top">
            <span class="metric-title">任务队列调度</span>
            <span class="metric-icon-badge task-icon">📋</span>
          </div>
          <div class="metric-main-value font-mono">
            {{ activeTasksCount }} <span class="unit">活跃 / {{ totalTasksCount }} 总计</span>
          </div>
        </router-link>
        <div class="metric-sub-detail task-status-tags">
          <router-link to="/tasks?status=completed" class="tag-item success">
            已完成 {{ completedTasksCount }}
          </router-link>
          <router-link v-if="failedTasksCount > 0" to="/tasks?status=failed" class="tag-item error">
            失败 {{ failedTasksCount }}
          </router-link>
        </div>
      </div>

      <!-- 指标 3: 账号与 Bot 连接池 -->
      <div class="metric-card glass-card">
        <div class="metric-card-top">
          <span class="metric-title">账号与 Bot 连接池</span>
          <span class="metric-icon-badge account-icon">👥</span>
        </div>
        <div class="metric-main-value font-mono">
          {{ accountStore.activeAccounts.length }}/{{ accountStore.accounts.length }}
          <span class="unit">Userbot · {{ botStatusText }}</span>
        </div>
        <div class="metric-sub-detail">
          <span>可用总并发上限: {{ accountStore.totalConcurrency }} 线程</span>
          <span v-if="accountStore.restrictedCount > 0" class="warn-badge">
            ⚠️ {{ accountStore.restrictedCount }} 账号受限
          </span>
        </div>
      </div>

      <!-- 指标 4: 存储与磁盘水位 -->
      <div class="metric-card glass-card">
        <div class="metric-card-top">
          <span class="metric-title">存储与磁盘容量</span>
          <span class="metric-icon-badge storage-icon">💾</span>
        </div>
        <div class="metric-main-value font-mono">
          {{ formatBytes(storageStore.overview.total_bytes || 0) }}
          <span class="unit">已用 ({{ storageStore.overview.file_count || 0 }} 文件)</span>
        </div>
        <div class="metric-sub-detail">
          <span v-if="storageStore.metrics.effective_free_bytes">
            剩余可用: {{ formatBytes(storageStore.metrics.effective_free_bytes) }}
          </span>
          <span v-if="storageStore.breakpoints.orphan_count > 0" class="warn-badge">
            {{ storageStore.breakpoints.orphan_count }} 孤儿断点
          </span>
        </div>
      </div>
    </div>

    <!-- 快捷工作台操作卡片 -->
    <div class="quick-actions-card cockpit-card">
      <div class="section-title">快捷业务工作台传送门</div>
      <div class="quick-buttons-grid">
        <router-link to="/tasks/create?tab=download" custom v-slot="{ href, navigate }">
          <a :href="href" class="action-btn-item" @click="navigate">
            <div class="action-btn-icon bg-indigo">📥</div>
            <div class="action-btn-content">
              <div class="action-btn-title">受限媒体下载</div>
              <div class="action-btn-desc">突破 TG 限制抓取频道与单条链接媒体</div>
            </div>
          </a>
        </router-link>

        <router-link to="/tasks/create?tab=publish" custom v-slot="{ href, navigate }">
          <a :href="href" class="action-btn-item" @click="navigate">
            <div class="action-btn-icon bg-purple">📤</div>
            <div class="action-btn-content">
              <div class="action-btn-title">本地文件发布</div>
              <div class="action-btn-desc">按相册模式与速率限制发布至频道</div>
            </div>
          </a>
        </router-link>

        <router-link to="/tasks/create?tab=forward" custom v-slot="{ href, navigate }">
          <a :href="href" class="action-btn-item" @click="navigate">
            <div class="action-btn-icon bg-pink">⚡</div>
            <div class="action-btn-content">
              <div class="action-btn-title">云端极速转发</div>
              <div class="action-btn-desc">无损转存、防重账本与覆盖区间可视化</div>
            </div>
          </a>
        </router-link>

        <router-link to="/tasks" custom v-slot="{ href, navigate }">
          <a :href="href" class="action-btn-item" @click="navigate">
            <div class="action-btn-icon bg-blue">📊</div>
            <div class="action-btn-content">
              <div class="action-btn-title">任务监控中心</div>
              <div class="action-btn-desc">实时进度条、线程监控与虚拟日志流</div>
            </div>
          </a>
        </router-link>

        <router-link to="/accounts" custom v-slot="{ href, navigate }">
          <a :href="href" class="action-btn-item" @click="navigate">
            <div class="action-btn-icon bg-emerald">👥</div>
            <div class="action-btn-content">
              <div class="action-btn-title">账号与 Bot 管理</div>
              <div class="action-btn-desc">多 Userbot 凭证与 Bot 令牌池探活</div>
            </div>
          </a>
        </router-link>

        <router-link to="/storage" custom v-slot="{ href, navigate }">
          <a :href="href" class="action-btn-item" @click="navigate">
            <div class="action-btn-icon bg-amber">💾</div>
            <div class="action-btn-content">
              <div class="action-btn-title">存储与断点治理</div>
              <div class="action-btn-desc">HTTP 206 视频流式预览与孤儿清理</div>
            </div>
          </a>
        </router-link>
      </div>
    </div>

    <!-- 底部双栏：当前活跃传输任务速览 & 系统日志快速预览 -->
    <div class="dashboard-bottom-grid">
      <!-- 活跃任务速览 -->
      <div class="bottom-card cockpit-card">
        <div class="bottom-card-header">
          <span class="section-title">正在运行的传输任务</span>
          <n-button text type="primary" size="small" @click="navigateTo('/tasks')"> 查看全部任务 ➔ </n-button>
        </div>
        <div v-if="activeTasks.length === 0" class="empty-placeholder">当前调度队列中无运行中的传输任务</div>
        <div v-else class="active-tasks-list">
          <router-link
            v-for="t in activeTasks.slice(0, 4)"
            :key="t.id"
            :to="'/tasks?id=' + t.id"
            custom
            v-slot="{ href, navigate }"
          >
            <a :href="href" class="mini-task-item clickable" title="点击在任务中心查看此任务详情" @click="navigate">
              <div class="mini-task-left">
                <span class="mini-task-type">[{{ t.task_type }}]</span>
                <span class="mini-task-id font-mono">#{{ t.id.slice(0, 8) }}</span>
              </div>
              <div class="mini-task-status">
                <n-tag size="small" type="info" round>{{ t.status }}</n-tag>
              </div>
            </a>
          </router-link>
        </div>
      </div>

      <!-- 系统运行日志速览 -->
      <div class="bottom-card cockpit-card">
        <div class="bottom-card-header">
          <span class="section-title">系统最新动态日志</span>
          <n-button text type="primary" size="small" @click="navigateTo('/logs')"> 进入日志与审计中心 ➔ </n-button>
        </div>
        <div v-if="logStore.systemLogs.length === 0" class="empty-placeholder">暂无系统运行日志记录</div>
        <div v-else class="mini-logs-list">
          <div v-for="(l, idx) in logStore.systemLogs.slice(0, 5)" :key="idx" class="mini-log-item">
            <span class="mini-log-time font-mono">[{{ l.timestamp || '系统' }}]</span>
            <span class="mini-log-msg">{{ l.message }}</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { NButton, NTag } from 'naive-ui'
import { useTaskStore } from '@/stores/taskStore'
import { useAccountStore } from '@/stores/accountStore'
import { useStorageStore } from '@/stores/storageStore'
import { useLogStore } from '@/stores/logStore'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'
import { formatBytes, formatDurationLocalized } from '@/utils/formatters'

const router = useRouter()

const taskStore = useTaskStore()
const accountStore = useAccountStore()
const storageStore = useStorageStore()
const logStore = useLogStore()
const tgAuthStore = useTelegramAuthStore()

// 活跃任务列表仅用于"进行中任务"近况卡片 (取当前页快照); 各项计数一律读服务端
// 全局 status_counts / total
const activeTasks = computed(() => {
  return (taskStore.tasks || []).filter((t) => t.status === 'running' || t.status === 'pending')
})

const activeTasksCount = computed(() => {
  return (taskStore.statusCounts?.running || 0) + (taskStore.statusCounts?.pending || 0)
})
const totalTasksCount = computed(() => taskStore.totalTasks || 0)
const completedTasksCount = computed(() => taskStore.statusCounts?.completed || 0)
const failedTasksCount = computed(() => taskStore.statusCounts?.failed || 0)

const botStatusText = computed(() => {
  const startup = tgAuthStore.startupStatus
  if (startup?.phase === 'initializing') return `${startup.ready_bots}/${startup.total_bots} Bot`
  const total = accountStore.bots.filter((bot) => bot.is_active).length
  return `${accountStore.onlineBotsCount}/${total} Bot`
})

const engineStatus = computed(() => {
  const startup = tgAuthStore.startupStatus
  if (startup?.phase === 'initializing') {
    const detail = startup.total_bots > 0 ? ` · ${botStatusText.value} 就绪` : ''
    return { color: 'warning', text: `正在初始化${detail}` }
  }
  if (startup?.phase === 'degraded') {
    const issues = []
    if (!startup.scheduler_ready) issues.push('调度器未就绪')
    if (startup.unready_bots.length) issues.push(`${startup.unready_bots.join('、')} 未就绪`)
    return { color: 'warning', text: `Telethon 启动降级 · ${issues.join(' · ') || '客户端池初始化失败'}` }
  }
  if (startup?.phase === 'ready') {
    return tgAuthStore.isConnected && tgAuthStore.authState === 'AUTHENTICATED'
      ? { color: 'online', text: 'Telethon 引擎就绪 · 服务正常' }
      : { color: 'offline', text: '默认 Userbot 未就绪' }
  }
  if (tgAuthStore.startupDegraded) {
    return { color: 'warning', text: '正在获取 Telethon 初始化状态…' }
  }
  if (taskStore.wsStatus === 'connected' && tgAuthStore.authState === 'AUTHENTICATED') {
    return { color: 'online', text: 'Telethon 引擎就绪 · 服务正常' }
  }
  if (taskStore.wsStatus === 'connecting' || tgAuthStore.authState === 'UNKNOWN') {
    return { color: 'warning', text: '正在获取 Telethon 状态…' }
  }
  return { color: 'offline', text: '引擎通信离线' }
})

function navigateTo(path: string) {
  router.push(path)
}

onMounted(() => {
  accountStore.loadAccounts()
  accountStore.loadBots()
  storageStore.loadOverview()
  storageStore.loadMetrics()
  storageStore.loadBreakpoints()
  logStore.loadSystemLogs()
})
</script>

<style scoped>
.dashboard-container {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
  width: 100%;
  container: dashboard-root / inline-size;
}

/* 欢迎卡片 */
.dashboard-header-card {
  padding: 1.5rem 2rem;
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 1rem 1.5rem;
  container: hero-card / inline-size;
}

.header-left-col {
  flex: 1 1 280px;
  min-width: 0;
}

.header-right-col {
  min-width: 0;
  max-width: 100%;
  flex: 0 1 auto;
}

.welcome-title {
  font-size: clamp(1.15rem, 0.92rem + 1.8cqi, 1.5rem);
  font-weight: 700;
  margin-bottom: 0.35rem;
}

.gradient-text {
  background: var(--accent-gradient);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}

.welcome-desc {
  color: var(--text-secondary);
  font-size: 0.9rem;
  margin: 0;
}

.engine-status-pill {
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.4rem 1rem;
  border-radius: 9999px;
  font-size: 0.85rem;
  font-weight: 600;
  border: 1px solid transparent;
  max-width: 100%;
  white-space: normal;
  overflow-wrap: anywhere;
  line-height: 1.35;
  text-align: left;
}

.engine-status-pill.online {
  background: rgba(16, 185, 129, 0.15);
  color: var(--success-color);
  border-color: rgba(16, 185, 129, 0.3);
}

.engine-status-pill.warning {
  background: rgba(245, 158, 11, 0.15);
  color: var(--warning-color);
  border-color: rgba(245, 158, 11, 0.3);
}

.engine-status-pill.offline {
  background: rgba(239, 68, 68, 0.15);
  color: var(--danger-color);
  border-color: rgba(239, 68, 68, 0.3);
}

.pulse-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background-color: currentColor;
  box-shadow: 0 0 6px currentColor;
  flex-shrink: 0;
}

/* 4 大核心指标网格 */
.metrics-overview-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 1.25rem;
}

.metric-card {
  padding: 1.25rem;
  display: flex;
  flex-direction: column;
  gap: 0.6rem;
}

.metric-card-main-link {
  display: flex;
  flex-direction: column;
  gap: 0.6rem;
  min-width: 0;
}

.metric-card-top {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.metric-title {
  font-size: 0.85rem;
  color: var(--text-secondary);
  font-weight: 500;
}

.metric-icon-badge {
  width: 28px;
  height: 28px;
  border-radius: 8px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 0.95rem;
}

.speed-icon {
  background: rgba(99, 102, 241, 0.2);
  color: var(--indigo-color);
}
.task-icon {
  background: rgba(168, 85, 247, 0.2);
  color: var(--purple-color);
}
.account-icon {
  background: rgba(16, 185, 129, 0.2);
  color: var(--success-color);
}
.storage-icon {
  background: rgba(245, 158, 11, 0.2);
  color: var(--warning-color);
}

.metric-main-value {
  font-size: 1.35rem;
  font-weight: 700;
  color: var(--text-primary);
}

.metric-main-value .unit {
  font-size: 0.82rem;
  font-weight: 400;
  color: var(--text-secondary);
}

.metric-sub-detail {
  font-size: 0.78rem;
  color: var(--text-secondary);
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.task-status-tags {
  display: flex;
  gap: 0.4rem;
}

.tag-item {
  padding: 0.1rem 0.4rem;
  border-radius: 4px;
  font-size: 0.75rem;
  text-decoration: none;
  cursor: pointer;
}

.tag-item.success {
  background: rgba(16, 185, 129, 0.15);
  color: var(--success-color);
}
.tag-item.error {
  background: rgba(239, 68, 68, 0.15);
  color: var(--danger-color);
}

.warn-badge {
  color: var(--warning-color);
  font-weight: 600;
}

/* 快捷业务入口 */
.quick-actions-card {
  padding: 1.5rem;
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
}

.quick-buttons-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 1rem;
}

.clickable {
  cursor: pointer;
  text-decoration: none;
  color: inherit;
}

.action-btn-item {
  display: flex;
  align-items: center;
  gap: 1rem;
  padding: 1rem;
  border-radius: 12px;
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  cursor: pointer;
  transition: all 0.2s ease;
  text-decoration: none;
  color: inherit;
  min-width: 0;
}

.action-btn-item:hover {
  background: var(--bg-primary);
  border-color: var(--border-color-focus);
  transform: translateY(-2px);
  box-shadow: var(--card-shadow-hover);
}

.action-btn-icon {
  width: 44px;
  height: 44px;
  border-radius: 12px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 1.35rem;
  flex-shrink: 0;
}

.bg-indigo {
  background: rgba(37, 99, 235, 0.12);
}
.bg-purple {
  background: rgba(147, 51, 234, 0.12);
}
.bg-pink {
  background: rgba(236, 72, 153, 0.12);
}
.bg-blue {
  background: rgba(2, 132, 199, 0.12);
}
.bg-emerald {
  background: rgba(16, 185, 129, 0.12);
}
.bg-amber {
  background: rgba(245, 158, 11, 0.12);
}

.action-btn-content {
  display: flex;
  flex-direction: column;
  gap: 0.2rem;
  overflow: hidden;
  flex: 1;
  min-width: 0;
}

.action-btn-title {
  font-size: 0.95rem;
  font-weight: 600;
  color: var(--text-primary);
}

.action-btn-desc {
  font-size: 0.78rem;
  color: var(--text-secondary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

/* 底部双栏 */
.dashboard-bottom-grid {
  display: grid;
  /* minmax(0, 1fr): 1fr 轨道的自动最小值是内容 min-content —— 日志行 nowrap 长文本
     会把轨道撑爆视口 (移动端 412px 实测 614px), 强制轨道以容器宽度为准,
     内部由既有 nowrap/ellipsis 纪律接管溢出 */
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 1.25rem;
}

.bottom-card {
  padding: 1.25rem;
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.bottom-card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid var(--border-color);
  padding-bottom: 0.75rem;
}

.empty-placeholder {
  padding: 2rem 0;
  text-align: center;
  color: var(--text-secondary);
  font-size: 0.88rem;
}

.active-tasks-list,
.mini-logs-list {
  display: flex;
  flex-direction: column;
  gap: 0.6rem;
}

.mini-task-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0.6rem 0.8rem;
  background: var(--bg-card-hover);
  border-radius: 8px;
  border: 1px solid var(--border-color);
}

.mini-task-left {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.85rem;
  min-width: 0;
  overflow: hidden;
}

.mini-task-left > span {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mini-task-status {
  flex-shrink: 0;
}

.mini-task-type {
  color: var(--accent-color);
  font-weight: 600;
}

.mini-task-id {
  color: var(--text-secondary);
}

.mini-log-item {
  display: flex;
  gap: 0.6rem;
  font-size: 0.8rem;
  padding: 0.45rem 0.65rem;
  background: var(--bg-card-hover);
  border-radius: 8px;
  border: 1px solid var(--border-color);
  overflow: hidden;
}

.mini-log-time {
  color: var(--text-secondary);
  flex-shrink: 0;
}

.mini-log-msg {
  color: var(--text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

@container dashboard-root (max-width: 960px) {
  .metrics-overview-grid {
    grid-template-columns: repeat(2, 1fr);
  }
  .quick-buttons-grid {
    grid-template-columns: repeat(2, 1fr);
  }
  .dashboard-bottom-grid {
    grid-template-columns: minmax(0, 1fr);
  }
}

@container dashboard-root (max-width: 720px) {
  .dashboard-header-card {
    flex-direction: column;
    align-items: flex-start;
    padding: 1.15rem;
    gap: 0.85rem;
  }
  .header-left-col {
    flex: 0 1 auto;
  }
  .bottom-card-header {
    flex-wrap: wrap;
    gap: 0.5rem;
  }
}

@container dashboard-root (max-width: 580px) {
  .metrics-overview-grid {
    grid-template-columns: repeat(2, 1fr);
    gap: 0.75rem;
  }
  .metric-card {
    padding: 0.75rem 0.85rem;
    gap: 0.4rem;
    min-width: 0;
  }
  .metric-title {
    font-size: 0.78rem;
  }
  .metric-main-value {
    font-size: clamp(1rem, 0.88rem + 1cqi, 1.15rem);
  }
  .metric-main-value .unit {
    font-size: 0.72rem;
  }
  .metric-sub-detail {
    font-size: 0.7rem;
    flex-wrap: wrap;
    gap: 0.35rem;
  }
  .quick-buttons-grid {
    grid-template-columns: minmax(0, 1fr);
  }
}

@container dashboard-root (max-width: 350px) {
  .metrics-overview-grid {
    grid-template-columns: 1fr;
  }
}
</style>
