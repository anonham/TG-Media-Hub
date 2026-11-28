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

it('skips pruning on multi-page refresh and reconciles the refreshed folder after navigation', async () => {
  const store = usePikpakStore()
  store.authStatus = { logged_in: true } as never
  vi.mocked(pikpakApi.fileList).mockResolvedValueOnce({ data: { items: [], next_page_token: null } } as never)

  const wrapper = shallowMount(PikPakFileBrowser)
  try {
    await flushPromises()
    const vm = wrapper.vm as unknown as {
      enterFolder: (item: PikPakFileItem) => Promise<void>
      navigateTo: (idx: number) => Promise<void>
      refreshCurrent: () => Promise<void>
    }

    const folderA: PikPakFileItem = { file_id: 'folder-a', name: 'Folder A', is_dir: true, size_bytes: 0 }
    const page1File: PikPakFileItem = {
      file_id: 'f-p1',
      name: 'p1.mp4',
      is_dir: false,
      size_bytes: 10,
      parent_id: 'folder-a',
    }
    const page2File: PikPakFileItem = {
      file_id: 'f-p2',
      name: 'p2.mp4',
      is_dir: false,
      size_bytes: 20,
      parent_id: 'folder-a',
    }
    const rootFile: PikPakFileItem = {
      file_id: 'f-root',
      name: 'root.mp4',
      is_dir: false,
      size_bytes: 30,
      parent_id: '',
    }

    vi.mocked(pikpakApi.fileList).mockResolvedValueOnce({
      data: { items: [page1File], next_page_token: 'tok-2' },
    } as never)
    await vm.enterFolder(folderA)
    store.toggleFile(page2File)
    store.toggleFile(rootFile)

    // Multi-page refresh: page 2 selection must NOT be pruned
    vi.mocked(pikpakApi.fileList).mockResolvedValueOnce({
      data: { items: [page1File], next_page_token: 'tok-2' },
    } as never)
    await vm.refreshCurrent()
    expect(store.isSelected('f-p2')).toBe(true)

    // Navigate to root while single-page refresh of folderA is still pending
    let resolveFolderARefresh!: (v: unknown) => void
    vi.mocked(pikpakApi.fileList).mockImplementationOnce(
      () =>
        new Promise((r) => {
          resolveFolderARefresh = r
        }) as never,
    )
    const refreshPromise = vm.refreshCurrent()

    vi.mocked(pikpakApi.fileList).mockResolvedValueOnce({
      data: { items: [rootFile], next_page_token: null },
    } as never)
    await vm.navigateTo(0)

    // FolderA refresh completes with empty list; rootFile must stay selected while folderA selections prune
    resolveFolderARefresh({ data: { items: [], next_page_token: null } })
    await refreshPromise
    expect(store.isSelected('f-root')).toBe(true)
    expect(store.isSelected('f-p2')).toBe(false)
  } finally {
    wrapper.unmount()
  }
})
