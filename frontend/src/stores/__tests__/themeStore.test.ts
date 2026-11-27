import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

// localStorage 访问可能因浏览器存储策略被拒 (SecurityError) —— 旧实现在
// 模块求值期裸读, 异常会中断静态导入链导致应用白屏不挂载; 安全封装后须优雅降级
describe('themeStore 存储策略拒绝降级 (#26)', () => {
  beforeEach(() => {
    vi.resetModules()
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  function deniedStorage() {
    const denied = () => {
      throw new DOMException('Access denied', 'SecurityError')
    }
    return { getItem: denied, setItem: denied, removeItem: denied }
  }

  it('localStorage 访问抛 SecurityError 时模块仍可导入且使用默认值', async () => {
    vi.stubGlobal('localStorage', deniedStorage())
    const mod = await import('../themeStore')
    setActivePinia(createPinia())
    const store = mod.useThemeStore()
    expect(store.themeMode).toBe('system')
    expect(store.reduceMotion).toBe(false)
  })

  it('写路径被拒时仅降级为内存态, 不抛异常且 UI 状态照常更新', async () => {
    vi.stubGlobal('localStorage', deniedStorage())
    const mod = await import('../themeStore')
    setActivePinia(createPinia())
    const store = mod.useThemeStore()
    expect(() => store.setThemeMode('dark')).not.toThrow()
    expect(store.themeMode).toBe('dark')
    expect(() => store.setReduceMotion(true)).not.toThrow()
    expect(store.reduceMotion).toBe(true)
    expect(() => store.setSidebarCollapsed(true)).not.toThrow()
    expect(store.sidebarCollapsed).toBe(true)
  })
})
