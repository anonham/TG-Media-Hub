import { beforeEach, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useSettingStore } from '../settingStore'
import { settingsApi } from '@/api'

vi.mock('@/api', () => ({ settingsApi: { getSettings: vi.fn(), updateSettings: vi.fn() } }))
vi.mock('@/utils/discreteApi', () => ({ message: { success: vi.fn(), error: vi.fn() } }))
beforeEach(() => {
  setActivePinia(createPinia())
  vi.clearAllMocks()
})

it('successful save clears loading and prevents a pending load from overwriting settings', async () => {
  let release!: (value: never) => void
  vi.mocked(settingsApi.getSettings).mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        release = resolve
      }),
  )
  vi.mocked(settingsApi.updateSettings).mockResolvedValueOnce({ data: { all_settings: { key: 'saved' } } } as never)
  const store = useSettingStore()
  const pending = store.loadSettings()
  expect(store.loadingSettings).toBe(true)
  await store.updateSettings({ key: 'saved' })
  expect(store.loadingSettings).toBe(false)
  release({ data: { all_settings: { key: 'stale' } } } as never)
  await pending
  expect(store.allSettings).toEqual({ key: 'saved' })
  expect(store.loadingSettings).toBe(false)
  expect(store.savingSettings).toBe(false)
})
