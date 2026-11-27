import { expect, it, vi } from 'vitest'
import { shallowMount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { nextTick } from 'vue'
import DashboardView from '../DashboardView.vue'
import { useAccountStore } from '@/stores/accountStore'
import { useLogStore } from '@/stores/logStore'
import { useStorageStore } from '@/stores/storageStore'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'

vi.mock('vue-router', () => ({ useRouter: () => ({ push: vi.fn() }) }))

it('shows initialization progress, degraded details and green readiness without a socket or navigation', async () => {
  const pinia = createPinia()
  setActivePinia(pinia)
  const accounts = useAccountStore()
  const storage = useStorageStore()
  vi.spyOn(accounts, 'loadAccounts').mockResolvedValue(undefined)
  vi.spyOn(accounts, 'loadBots').mockResolvedValue(undefined)
  vi.spyOn(storage, 'loadOverview').mockResolvedValue(undefined)
  vi.spyOn(storage, 'loadMetrics').mockResolvedValue(undefined)
  vi.spyOn(storage, 'loadBreakpoints').mockResolvedValue(undefined)
  vi.spyOn(useLogStore(), 'loadSystemLogs').mockResolvedValue(undefined)
  const telegram = useTelegramAuthStore()
  telegram.authState = 'AUTHENTICATED'
  telegram.isConnected = true
  telegram.startupStatus = {
    phase: 'initializing',
    scheduler_ready: false,
    ready_bots: 1,
    total_bots: 2,
    unready_bots: ['Auxiliary'],
  }

  const wrapper = shallowMount(DashboardView, {
    global: {
      plugins: [pinia],
      stubs: { RouterLink: { template: '<slot href="#/test" :navigate="navigate" />', methods: { navigate() {} } } },
    },
  })
  const status = wrapper.get('.engine-status-pill[role="status"]')
  expect(status.classes()).toContain('warning')
  expect(status.text()).toContain('正在初始化 · 1/2 Bot 就绪')

  telegram.startupStatus.phase = 'degraded'
  await nextTick()
  expect(status.classes()).toContain('warning')
  expect(status.text()).toContain('调度器未就绪 · Auxiliary 未就绪')

  telegram.startupStatus = { phase: 'ready', scheduler_ready: true, ready_bots: 2, total_bots: 2, unready_bots: [] }
  await nextTick()
  expect(status.classes()).toContain('online')
  expect(status.text()).toContain('Telethon 引擎就绪')

  telegram.isConnected = false
  await nextTick()
  expect(status.classes()).toContain('offline')
  wrapper.unmount()
  vi.restoreAllMocks()
})
