import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { darkTheme, type GlobalThemeOverrides } from 'naive-ui'

export type ThemeMode = 'light' | 'dark' | 'system' | 'auto'

// localStorage 访问可能因浏览器存储策略被拒 (SecurityError), typeof
// 检查接不住访问抛错 —— 而本模块在静态导入期读取偏好, 异常会中断导入链导致应用
// 白屏不挂载; 全部读写统一走安全封装, 持久化失败降级为内存态默认值
function safeStorageGet(key: string): string | null {
  try {
    if (typeof localStorage === 'undefined') return null
    return localStorage.getItem(key)
  } catch (err) {
    console.debug('localStorage 读取被拒绝 (浏览器存储策略), 使用默认值:', err)
    return null
  }
}

function safeStorageSet(key: string, value: string): void {
  try {
    if (typeof localStorage === 'undefined') return
    localStorage.setItem(key, value)
  } catch (err) {
    console.debug('localStorage 写入被拒绝 (浏览器存储策略), 仅保留内存态:', err)
  }
}

function readInitialThemeMode(): ThemeMode {
  return (safeStorageGet('tg_hub_theme_mode') as ThemeMode) || 'system'
}

function readInitialReduceMotion(): boolean {
  return safeStorageGet('tg_hub_reduce_motion') === 'true'
}

function readInitialSidebarCollapsed(): boolean {
  return safeStorageGet('tg_hub_sidebar_collapsed') === 'true'
}

// 模块级响应式状态真源: 供 discreteApi 在 Pinia 挂载前安全订阅同一组响应式计算属性,
// 避免 createDiscreteApi 在静态导入期同步 mount 内部宿主组件时因 Pinia 未激活而崩溃。
const themeMode = ref<ThemeMode>(readInitialThemeMode())
const reduceMotion = ref<boolean>(readInitialReduceMotion())

// 侧边栏折叠态: 设置页"侧边栏布局"的唯一状态真源,
// Sidebar / AppLayout / SettingsView 三处实时联动, 持久化沿用既有 localStorage 键
const sidebarCollapsed = ref<boolean>(readInitialSidebarCollapsed())

function setSidebarCollapsed(val: boolean): void {
  sidebarCollapsed.value = Boolean(val)
  safeStorageSet('tg_hub_sidebar_collapsed', sidebarCollapsed.value ? 'true' : 'false')
}

function toggleSidebar(): void {
  setSidebarCollapsed(!sidebarCollapsed.value)
}

// 系统暗黑偏好媒体查询
const systemPrefersDark = ref<boolean>(
  typeof window !== 'undefined' &&
    typeof window.matchMedia === 'function' &&
    window.matchMedia('(prefers-color-scheme: dark)').matches,
)

if (typeof window !== 'undefined' && typeof window.matchMedia === 'function') {
  const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)')
  mediaQuery.addEventListener('change', (e: MediaQueryListEvent) => {
    systemPrefersDark.value = e.matches
    applyThemeToDom()
  })
}

// 实际生效的是否为深色 (兼容 'system' 与历史 'auto' 两种跟随系统枚举值)
const isDark = computed<boolean>(() => {
  if (themeMode.value === 'system' || themeMode.value === 'auto') {
    return systemPrefersDark.value
  }
  return themeMode.value === 'dark'
})

// Naive UI 主题对象
export const naiveTheme = computed(() => {
  return isDark.value ? darkTheme : null
})

// Naive UI 设计变量覆写 (轻量蓝调、大圆角、现代质感)
export const naiveThemeOverrides = computed<GlobalThemeOverrides>(() => {
  if (isDark.value) {
    return {
      common: {
        primaryColor: '#3b82f6',
        primaryColorHover: '#60a5fa',
        primaryColorPressed: '#2563eb',
        primaryColorSuppl: '#1d4ed8',
        borderRadius: '10px',
        borderRadiusSmall: '6px',
        fontFamily: "'Inter', system-ui, -apple-system, sans-serif",
        cardColor: '#1e293b',
      },
      Button: {
        borderRadiusMedium: '8px',
        borderRadiusSmall: '6px',
      },
      Card: {
        borderRadius: '16px',
      },
    }
  }

  // 默认浅色规范 (对齐 Cockpit Tools 设计)
  return {
    common: {
      primaryColor: '#2563eb',
      primaryColorHover: '#3b82f6',
      primaryColorPressed: '#1d4ed8',
      primaryColorSuppl: '#60a5fa',
      infoColor: '#0284c7',
      successColor: '#10b981',
      warningColor: '#f59e0b',
      errorColor: '#ef4444',
      textColorBase: '#0f172a',
      textColor1: '#1e293b',
      textColor2: '#475569',
      textColor3: '#94a3b8',
      bodyColor: '#f4f6fb',
      cardColor: '#ffffff',
      modalColor: '#ffffff',
      popoverColor: '#ffffff',
      borderColor: 'rgba(0, 0, 0, 0.08)',
      borderRadius: '10px',
      borderRadiusSmall: '6px',
      fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
    },
    Button: {
      borderRadiusMedium: '8px',
      borderRadiusSmall: '6px',
      colorSecondary: '#f1f5f9',
      colorSecondaryHover: '#e2e8f0',
      textColorSecondary: '#334155',
    },
    Card: {
      borderRadius: '16px',
      borderColor: 'rgba(0, 0, 0, 0.06)',
    },
    Input: {
      borderRadius: '8px',
      color: '#f8fafc',
      colorFocus: '#ffffff',
      border: '1px solid rgba(0, 0, 0, 0.1)',
    },
    Select: {
      peers: {
        InternalSelection: {
          borderRadius: '8px',
          color: '#f8fafc',
        },
      },
    },
    Tabs: {
      tabBorderRadius: '8px',
    },
  }
})

function setThemeMode(mode: ThemeMode): void {
  themeMode.value = mode
  safeStorageSet('tg_hub_theme_mode', mode)
  applyThemeToDom()
}

function setReduceMotion(val: boolean): void {
  reduceMotion.value = val
  safeStorageSet('tg_hub_reduce_motion', String(val))
  applyThemeToDom()
}

function applyThemeToDom(): void {
  if (typeof document === 'undefined') return
  const root = document.documentElement

  if (isDark.value) {
    root.setAttribute('data-theme', 'dark')
  } else {
    root.removeAttribute('data-theme')
  }

  // 现代 Web 标准: 同步更新 <meta name="color-scheme">
  const metaColorScheme = document.querySelector('meta[name="color-scheme"]')
  if (metaColorScheme) {
    metaColorScheme.setAttribute(
      'content',
      themeMode.value === 'dark' || themeMode.value === 'light' ? themeMode.value : 'light dark',
    )
  }

  if (reduceMotion.value) {
    root.classList.add('reduce-motion')
  } else {
    root.classList.remove('reduce-motion')
  }
}

// 跨标签页状态自动同步 (storage 事件); 值经白名单校验, 键被移除 (newValue null) 时复位默认
const VALID_THEME_MODES: readonly ThemeMode[] = ['light', 'dark', 'system', 'auto']

if (typeof window !== 'undefined') {
  window.addEventListener('storage', (e: StorageEvent) => {
    if (e.key === 'tg_hub_theme_mode') {
      const next = e.newValue
      if (next === null || VALID_THEME_MODES.includes(next as ThemeMode)) {
        themeMode.value = (next as ThemeMode) || 'system'
        applyThemeToDom()
      }
    } else if (e.key === 'tg_hub_reduce_motion') {
      reduceMotion.value = e.newValue === 'true'
      applyThemeToDom()
    } else if (e.key === 'tg_hub_sidebar_collapsed') {
      sidebarCollapsed.value = e.newValue === 'true'
    }
  })
}

// 模块加载期先同步一次 DOM 主题属性
applyThemeToDom()

export const useThemeStore = defineStore('theme', () => {
  themeMode.value = readInitialThemeMode()
  reduceMotion.value = readInitialReduceMotion()
  sidebarCollapsed.value = readInitialSidebarCollapsed()

  // 初始化 DOM 属性
  applyThemeToDom()

  return {
    themeMode,
    reduceMotion,
    sidebarCollapsed,
    isDark,
    naiveTheme,
    naiveThemeOverrides,
    setThemeMode,
    setReduceMotion,
    setSidebarCollapsed,
    toggleSidebar,
    applyThemeToDom,
  }
})
