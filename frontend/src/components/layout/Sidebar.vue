<template>
  <div>
    <!-- 移动端遮罩蒙层 -->
    <div v-if="mobileOpen" class="sidebar-mobile-mask" @click="$emit('close-mobile')"></div>

    <!-- 侧边栏主体 -->
    <aside
      class="sidebar-container"
      :class="{
        'is-collapsed': collapsed,
        'mobile-open': mobileOpen,
      }"
      :inert="isMobileClosed ? true : undefined"
      :aria-hidden="isMobileClosed ? 'true' : undefined"
    >
      <!-- 品牌 Logo 头部 -->
      <router-link to="/dashboard" custom v-slot="{ href, navigate }">
        <a
          :href="href"
          class="brand-header"
          @click="
            (e) => {
              navigate(e)
              emit('close-mobile')
            }
          "
        >
          <div class="logo-badge">
            <span class="logo-icon">⚡</span>
          </div>
          <div v-show="!collapsed" class="brand-text-block">
            <span class="brand-title">tg-media-hub</span>
            <span class="version-tag">v{{ settingStore.appVersion }}</span>
          </div>
        </a>
      </router-link>

      <!-- 导航工作台列表 -->
      <nav class="nav-menu">
        <template v-for="item in menuItems" :key="item.path">
          <!-- 展开态导航项 -->
          <router-link v-if="!collapsed" :to="item.path" custom v-slot="{ href, navigate }">
            <a
              :href="href"
              class="nav-item"
              :class="{ active: isRouteActive(item.path) }"
              :aria-current="isRouteActive(item.path) ? 'page' : undefined"
              :data-testid="item.testId"
              @click="
                (e) => {
                  navigate(e)
                  handleMobileItemClick()
                }
              "
            >
              <div class="nav-icon-wrapper">
                <n-icon :size="20">
                  <component :is="item.icon" />
                </n-icon>
              </div>
              <span class="nav-label">{{ item.label }}</span>

              <!-- 角标提醒 -->
              <span v-if="item.badgeCount && item.badgeCount > 0" class="nav-badge" :class="item.badgeType || 'info'">
                {{ item.badgeCount }}
              </span>
              <span v-else-if="item.showDot" class="nav-dot" :class="item.badgeType || 'warning'"></span>
            </a>
          </router-link>

          <!-- 折叠态 Tooltip 导航项 -->
          <n-tooltip v-else placement="right" trigger="hover" :show-arrow="true">
            <template #trigger>
              <router-link :to="item.path" custom v-slot="{ href, navigate }">
                <a
                  :href="href"
                  class="nav-item collapsed-item"
                  :class="{ active: isRouteActive(item.path) }"
                  :aria-current="isRouteActive(item.path) ? 'page' : undefined"
                  :data-testid="item.testId"
                  @click="
                    (e) => {
                      navigate(e)
                      handleMobileItemClick()
                    }
                  "
                >
                  <div class="nav-icon-wrapper">
                    <n-icon :size="20">
                      <component :is="item.icon" />
                    </n-icon>
                  </div>
                  <!-- 折叠态简易角标 -->
                  <span v-if="item.badgeCount && item.badgeCount > 0" class="collapsed-badge">
                    {{ item.badgeCount }}
                  </span>
                  <span v-else-if="item.showDot" class="collapsed-dot"></span>
                </a>
              </router-link>
            </template>
            <span>{{ item.label }}</span>
          </n-tooltip>
        </template>
      </nav>

      <!-- 底部状态指示与控制区 -->
      <div class="sidebar-footer">
        <!-- 实时通信状态 -->
        <div class="footer-status-row" :title="wsStatusText">
          <div class="status-indicator-dot" :class="wsStatusClass"></div>
          <span v-show="!collapsed" class="status-text">{{ wsStatusText }}</span>
        </div>

        <!-- 底部快捷控制按钮组 -->
        <div class="footer-actions-row">
          <n-tooltip placement="top" trigger="hover">
            <template #trigger>
              <button
                class="footer-icon-btn"
                aria-label="密钥凭证管理"
                @click="
                  () => {
                    authStore.openAuthModal('API Key 访问控制与凭证管理')
                    handleMobileItemClick()
                  }
                "
              >
                <n-icon :size="16">
                  <LockClosedOutline />
                </n-icon>
              </button>
            </template>
            <span>密钥凭证管理</span>
          </n-tooltip>

          <!-- 折叠/展开切换按钮 (桌面端) -->
          <n-tooltip placement="top" trigger="hover">
            <template #trigger>
              <button
                class="footer-icon-btn collapse-toggle-btn"
                :aria-label="collapsed ? '展开侧边栏' : '折叠侧边栏'"
                @click="$emit('toggle-collapse')"
              >
                <n-icon :size="16">
                  <ChevronBackOutline v-if="!collapsed" />
                  <ChevronForwardOutline v-else />
                </n-icon>
              </button>
            </template>
            <span>{{ collapsed ? '展开侧边栏' : '折叠侧边栏' }}</span>
          </n-tooltip>
        </div>
      </div>
    </aside>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, type Component } from 'vue'
import { useRoute } from 'vue-router'
import { NIcon, NTooltip } from 'naive-ui'
import {
  SpeedometerOutline,
  AddCircleOutline,
  ListOutline,
  PeopleOutline,
  FolderOutline,
  DocumentTextOutline,
  SettingsOutline,
  CloudDownloadOutline,
  ChevronBackOutline,
  ChevronForwardOutline,
  LockClosedOutline,
} from '@vicons/ionicons5'

import { useTaskStore } from '@/stores/taskStore'
import { useAccountStore } from '@/stores/accountStore'
import { useAuthStore } from '@/stores/authStore'
import { useSettingStore } from '@/stores/settingStore'
import { useMobileViewport } from '@/utils/viewport'

interface MenuItem {
  path: string
  label: string
  icon: Component
  badgeCount?: number
  badgeType?: string
  showDot?: boolean
  testId?: string
}

const props = withDefaults(
  defineProps<{
    collapsed?: boolean
    mobileOpen?: boolean
  }>(),
  {
    collapsed: false,
    mobileOpen: false,
  },
)

const emit = defineEmits<{
  (e: 'toggle-collapse'): void
  (e: 'close-mobile'): void
}>()

const route = useRoute()

const taskStore = useTaskStore()
const accountStore = useAccountStore()
const authStore = useAuthStore()
const settingStore = useSettingStore()
const { isMobileViewport } = useMobileViewport(768)
const isMobileClosed = computed(() => isMobileViewport.value && !props.mobileOpen)

// 活跃任务总数: 服务端全局 status_counts, 不再遍历单页内存数组
const runningTasksCount = computed(() => {
  return (taskStore.statusCounts?.running || 0) + (taskStore.statusCounts?.pending || 0)
})

// 计算受限/警报账号数
const restrictedAccountCount = computed(() => {
  return accountStore.restrictedCount || 0
})

const menuItems = computed<MenuItem[]>(() => [
  {
    path: '/dashboard',
    label: '仪表盘总览',
    icon: SpeedometerOutline,
    testId: 'nav-dashboard',
  },
  {
    path: '/tasks/create',
    label: '任务发布工作台',
    icon: AddCircleOutline,
    testId: 'nav-create',
  },
  {
    path: '/pikpak',
    label: 'PikPak 搬运',
    icon: CloudDownloadOutline,
    testId: 'nav-pikpak',
  },
  {
    path: '/tasks',
    label: '任务监控中心',
    icon: ListOutline,
    badgeCount: runningTasksCount.value,
    badgeType: 'primary',
    testId: 'nav-tasks',
  },
  {
    path: '/accounts',
    label: '账号与 Bot 管理',
    icon: PeopleOutline,
    showDot: restrictedAccountCount.value > 0,
    badgeType: 'warning',
    testId: 'nav-accounts',
  },
  {
    path: '/storage',
    label: '存储资源管理器',
    icon: FolderOutline,
    testId: 'nav-storage',
  },
  {
    path: '/logs',
    label: '日志与审计中心',
    icon: DocumentTextOutline,
    testId: 'nav-logs',
  },
  {
    path: '/settings',
    label: '系统配置中心',
    icon: SettingsOutline,
    testId: 'nav-settings',
  },
])

function isRouteActive(path: string): boolean {
  if (path === '/dashboard') {
    return route.path === '/dashboard' || route.path === '/'
  }
  if (path === '/tasks') {
    return route.path === '/tasks'
  }
  return route.path.startsWith(path)
}

function handleMobileItemClick() {
  if (props.mobileOpen) {
    emit('close-mobile')
  }
}

function handleKeyDown(event: KeyboardEvent) {
  if (props.mobileOpen && event.key === 'Escape') {
    emit('close-mobile')
  }
}

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.addEventListener('keydown', handleKeyDown)
  }
})

onUnmounted(() => {
  if (typeof window !== 'undefined') {
    window.removeEventListener('keydown', handleKeyDown)
  }
})

const wsStatusClass = computed(() => {
  if (taskStore.wsStatus === 'connected') return 'status-online'
  if (taskStore.wsStatus === 'connecting') return 'status-warning'
  return 'status-offline'
})

const wsStatusText = computed(() => {
  if (taskStore.wsStatus === 'connected') return 'WebSocket 在线'
  if (taskStore.wsStatus === 'connecting') return '正在连接...'
  return '引擎通信离线'
})
</script>

<style scoped>
.sidebar-container {
  width: var(--sidebar-width);
  height: 100vh;
  height: 100dvh;
  position: fixed;
  top: 0;
  left: 0;
  z-index: 120;
  background: var(--bg-sidebar);
  backdrop-filter: blur(16px);
  -webkit-backdrop-filter: blur(16px);
  border-right: 1px solid var(--border-color);
  display: flex;
  flex-direction: column;
  transition:
    width 0.25s cubic-bezier(0.4, 0, 0.2, 1),
    transform 0.25s cubic-bezier(0.4, 0, 0.2, 1);
  overflow: hidden;
  padding: var(--safe-area-top) 0 var(--safe-area-bottom) var(--safe-area-left);
  user-select: none;
  view-transition-name: app-sidebar;
}

.sidebar-container.is-collapsed {
  width: var(--sidebar-collapsed-outer-width);
}

/* 品牌头部 */
.brand-header {
  height: var(--header-height);
  padding: 0 1.15rem;
  display: flex;
  align-items: center;
  gap: 0.75rem;
  border-bottom: 1px solid var(--border-color);
  cursor: pointer;
  flex-shrink: 0;
  text-decoration: none;
  color: inherit;
}

.logo-badge {
  width: 36px;
  height: 36px;
  border-radius: 10px;
  background: var(--accent-gradient);
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  box-shadow: 0 4px 12px rgba(37, 99, 235, 0.25);
}

.logo-icon {
  font-size: 1.25rem;
}

.brand-text-block {
  display: flex;
  flex-direction: column;
  overflow: hidden;
  white-space: nowrap;
}

.brand-title {
  font-size: 1.05rem;
  font-weight: 700;
  letter-spacing: -0.01em;
  color: var(--text-primary);
}

.version-tag {
  font-size: 0.68rem;
  color: var(--text-secondary);
  font-weight: 600;
}

/* 导航菜单区 */
.nav-menu {
  flex: 1;
  padding: 1rem 0.75rem;
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
  overflow-y: auto;
  overflow-x: hidden;
}

.nav-item {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 0.65rem 0.85rem;
  border-radius: 10px;
  color: var(--text-secondary);
  text-decoration: none;
  font-size: 0.9rem;
  font-weight: 500;
  transition: all 0.18s ease;
  position: relative;
  white-space: nowrap;
}

.nav-item:hover {
  color: var(--text-primary);
  background: var(--bg-card-hover);
}

.nav-item.active {
  color: var(--accent-color);
  background: var(--bg-pill-active);
  font-weight: 600;
  border: none;
  box-shadow: none;
}

.nav-item.active .nav-icon-wrapper {
  color: var(--accent-color);
}

.nav-icon-wrapper {
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  width: 24px;
}

.nav-label {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
}

.nav-badge {
  font-size: 0.72rem;
  font-weight: 700;
  padding: 0.1rem 0.5rem;
  border-radius: 9999px;
  background: var(--accent-light);
  color: var(--accent-color);
}

.nav-badge.primary {
  background: var(--accent-light);
  color: var(--accent-color);
}

.nav-dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
}

.nav-dot.warning {
  background-color: var(--warning-color);
  box-shadow: 0 0 6px rgba(245, 158, 11, 0.4);
}

/* 折叠态图标布局 */
.collapsed-item {
  justify-content: center;
  padding: 0.7rem 0;
}

.collapsed-badge {
  position: absolute;
  top: 4px;
  right: 6px;
  background: var(--accent-light);
  color: var(--accent-color);
  border: 1px solid var(--border-color-focus);
  font-size: 0.65rem;
  font-weight: 700;
  border-radius: 9999px;
  min-width: 16px;
  height: 16px;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 0 3px;
}

.collapsed-dot {
  position: absolute;
  top: 8px;
  right: 12px;
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--warning-color);
  box-shadow: 0 0 6px rgba(245, 158, 11, 0.4);
}

/* 底部状态栏 */
.sidebar-footer {
  padding: 0.75rem 0.85rem;
  border-top: 1px solid var(--border-color);
  display: flex;
  flex-direction: column;
  gap: 0.65rem;
  flex-shrink: 0;
}

.footer-status-row {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  padding: 0.45rem 0.6rem;
  border-radius: 8px;
  background: var(--bg-pill-inactive);
}

.status-indicator-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
}

.status-indicator-dot.status-online {
  background: var(--success-color);
  box-shadow: 0 0 6px rgba(16, 185, 129, 0.4);
}

.status-indicator-dot.status-warning {
  background: var(--warning-color);
  box-shadow: 0 0 6px rgba(245, 158, 11, 0.4);
}

.status-indicator-dot.status-offline {
  background: var(--danger-color);
  box-shadow: 0 0 6px rgba(239, 68, 68, 0.4);
}

.status-text {
  font-size: 0.78rem;
  color: var(--text-secondary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.footer-actions-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.footer-icon-btn {
  background: transparent;
  border: 1px solid transparent;
  color: var(--text-secondary);
  border-radius: 8px;
  width: 36px;
  height: 36px;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  transition: all 0.2s;
}

.footer-icon-btn:hover {
  color: var(--text-primary);
  background: var(--bg-card-hover);
  border-color: var(--border-color);
}

/* 移动端适配 */
.sidebar-mobile-mask {
  display: none;
}

@media (max-width: 768px) {
  .sidebar-mobile-mask {
    display: block;
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(0, 0, 0, 0.6);
    backdrop-filter: blur(4px);
    z-index: 119;
  }

  .sidebar-container {
    transform: translateX(-100%);
    visibility: hidden;
    width: var(--sidebar-width) !important;
  }

  .sidebar-container.mobile-open {
    transform: translateX(0);
    visibility: visible;
  }

  .collapse-toggle-btn {
    display: none;
  }
}
</style>
