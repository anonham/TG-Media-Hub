import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { shallowMount, flushPromises } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { nextTick } from 'vue'
import PikPakFileBrowser from '../PikPakFileBrowser.vue'
import { usePikpakStore } from '@/stores/pikpakStore'
import { pikpakApi } from '@/api'
import type { PikPakFileItem } from '@/api/gen'

vi.mock('@/api', () => ({ pikpakApi: { fileList: vi.fn() } }))
vi.mock('@/utils/discreteApi', () => ({ message: { success: vi.fn(), info: vi.fn() } }))
beforeEach(() => {
  setActivePinia(createPinia())
  vi.mocked(pikpakApi.fileList)
    .mockReset()
    .mockResolvedValue({ data: { items: [] } } as never)
})
afterEach(() => vi.restoreAllMocks())

it('refetches an invalidated active folder and reuses navigation and refresh requests', async () => {
  const store = usePikpakStore()
  store.authStatus = { logged_in: true } as never
  const wrapper = shallowMount(PikPakFileBrowser)
  try {
    await flushPromises()
    const vm = wrapper.vm as unknown as {
      enterFolder: (item: PikPakFileItem) => Promise<void>
      refreshCurrent: () => Promise<void>
    }
    await vm.enterFolder({ file_id: 'active', name: 'active', is_dir: true, size_bytes: 0 })
    await flushPromises()
    expect(pikpakApi.fileList).toHaveBeenCalledTimes(2)
    store.clearCache()
    await flushPromises()
    expect(pikpakApi.fileList).toHaveBeenCalledTimes(3)
    expect(vi.mocked(pikpakApi.fileList).mock.lastCall?.[0]).toBe('active')
    await vm.refreshCurrent()
    await flushPromises()
    expect(pikpakApi.fileList).toHaveBeenCalledTimes(4)
    store.clearCache()
    const pending = store.fetchFolder('active')
    await nextTick()
    await pending
    await flushPromises()
    expect(pikpakApi.fileList).toHaveBeenCalledTimes(5)
  } finally {
    wrapper.unmount()
  }
})
