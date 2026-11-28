import { createRouter, createWebHashHistory, type RouteRecordRaw } from 'vue-router'
import { nextTick } from 'vue'
import { useAuthStore } from '@/stores/authStore'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'

const routes: RouteRecordRaw[] = [
  {
    path: '/',
    redirect: '/dashboard',
  },
  {
    path: '/dashboard',
    name: 'Dashboard',
    component: () => import('@/views/DashboardView.vue'),
    meta: {
      title: '仪表盘总览',
      breadcrumb: '全局概览',
      icon: 'SpeedometerOutline',
    },
  },
  {
    path: '/tasks/create',
    name: 'TaskCreate',
    component: () => import('@/views/create/TaskCreateView.vue'),
    meta: {
      title: '任务发布工作台',
      breadcrumb: '极速发布与抓取',
      icon: 'AddCircleOutline',
    },
  },
  {
    path: '/tasks',
    name: 'TaskCenter',
    component: () => import('@/views/TaskCenterView.vue'),
    meta: {
      title: '任务监控中心',
      breadcrumb: '任务传输与实时控制台',
      icon: 'ListOutline',
    },
  },
  {
    path: '/accounts',
    name: 'Accounts',
    component: () => import('@/views/AccountsView.vue'),
    meta: {
      title: '账号与 Bot 管理',
      breadcrumb: 'Userbot 连接池 & Bot 组',
      icon: 'PeopleOutline',
    },
  },
  {
    path: '/storage',
    name: 'Storage',
    component: () => import('@/views/StorageView.vue'),
    meta: {
      title: '存储资源管理器',
      breadcrumb: '磁盘占用与断点治理',
      icon: 'FolderOutline',
    },
  },
  {
    path: '/logs',
    name: 'Logs',
    component: () => import('@/views/LogsView.vue'),
    meta: {
      title: '日志与审计中心',
      breadcrumb: '系统日志与转发账本',
      icon: 'DocumentTextOutline',
    },
  },
  {
    path: '/settings',
    name: 'Settings',
    component: () => import('@/views/SettingsView.vue'),
    meta: {
      title: '系统配置中心',
      breadcrumb: '系统动态参数',
      icon: 'SettingsOutline',
    },
  },
  {
    path: '/pikpak',
    name: 'PikPakPublish',
    component: () => import('@/views/PikPakPublishView.vue'),
    meta: {
      title: 'PikPak 搬运',
      breadcrumb: 'PikPak 网盘流式搬运',
      icon: 'CloudDownloadOutline',
    },
  },
  {
    path: '/:pathMatch(.*)*',
    name: 'NotFound',
    component: () => import('@/views/NotFoundView.vue'),
    meta: {
      title: '页面未找到',
      breadcrumb: '404',
    },
  },
]

const router = createRouter({
  history: createWebHashHistory(),
  routes,
  scrollBehavior() {
    return { top: 0 }
  },
})

router.beforeEach((to) => {
  const title = to.meta?.title ? `${to.meta.title} - tg-media-hub` : 'tg-media-hub 控制台'
  if (typeof document !== 'undefined') {
    document.title = title
  }
  // 鉴权与受限引导收口: 未通过默认主账号准入或需鉴权但无 API Key 时统一回落仪表盘,
  // 深层链接不得绕过引导/鉴权门禁提前进入业务视图
  const tgAuthStore = useTelegramAuthStore()
  if (
    to.name !== 'NotFound' &&
    (tgAuthStore.isRestricted || tgAuthStore.admissionPhase === 'error') &&
    to.path !== '/dashboard'
  ) {
    return { path: '/dashboard' }
  }
  const authStore = useAuthStore()
  if (
    to.name !== 'NotFound' &&
    authStore.isAuthChecked &&
    authStore.isAuthRequired &&
    !authStore.apiKey &&
    to.path !== '/dashboard'
  ) {
    return { path: '/dashboard' }
  }
  return true
})

// 原生 View Transitions API 渐进增强:
// 在支持合成器动画的现代浏览器下无缝过渡视图，在减弱动效偏好下安全降级
router.beforeResolve(async (to, from) => {
  const isReduceMotion =
    typeof document !== 'undefined' &&
    (document.documentElement.classList.contains('reduce-motion') ||
      window.matchMedia('(prefers-reduced-motion: reduce)').matches)

  if (
    to.path !== from.path &&
    typeof document !== 'undefined' &&
    'startViewTransition' in document &&
    !isReduceMotion
  ) {
    return new Promise<boolean>((resolve) => {
      ;(document as any).startViewTransition(async () => {
        resolve(true)
        await nextTick()
      })
    })
  }
  return true
})

export default router
