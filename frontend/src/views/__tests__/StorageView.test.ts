import { expect, it, vi } from 'vitest'
import { flushPromises, shallowMount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import StorageView from '../StorageView.vue'
import { getStoragePreviewUrl } from '@/api/client'
import { useStorageStore } from '@/stores/storageStore'

vi.mock('@/api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/api/client')>()
  return {
    ...actual,
    getStoragePreviewUrl: vi.fn().mockResolvedValue('/api/storage/preview/sample.M4V?ticket=t1'),
    getStorageDownloadUrl: vi.fn().mockResolvedValue('/api/storage/download/sample.M4V?ticket=t1'),
  }
})

it('treats .m4v/.M4V files as previewable video and renders the existing video player', async () => {
  const pinia = createPinia()
  setActivePinia(pinia)
  const storage = useStorageStore()
  storage.files = [
    {
      abs_path: 'sample.M4V',
      rel_path: 'sample.M4V',
      file_name: 'sample.M4V',
      category: 'other',
      mime_type: 'application/octet-stream',
      mtime: 0,
      size_mb: 0,
      size_bytes: 1024,
    },
  ]
  vi.spyOn(storage, 'loadOverview').mockResolvedValue(undefined)
  vi.spyOn(storage, 'loadMetrics').mockResolvedValue(undefined)
  vi.spyOn(storage, 'loadBreakpoints').mockResolvedValue(undefined)
  vi.spyOn(storage, 'loadFiles').mockResolvedValue(undefined)

  const wrapper = shallowMount(StorageView, {
    global: {
      plugins: [pinia],
      stubs: {
        RouterLink: { template: '<a><slot /></a>' },
        Modal: { template: '<div v-if="show"><slot /></div>', props: ['show'] },
        Button: { template: '<button><slot /></button>' },
      },
    },
  })

  const vm = wrapper.vm as unknown as {
    isVideo: (path?: string) => boolean
    isPreviewable: (file: { path: string; category?: string }) => boolean
  }

  expect(vm.isVideo('clip.m4v')).toBe(true)
  expect(vm.isVideo('CLIP.M4V')).toBe(true)
  expect(vm.isPreviewable({ path: 'CLIP.M4V' })).toBe(true)

  expect(getStoragePreviewUrl).not.toHaveBeenCalled()
  const preview = wrapper.findAll('tbody .action-cell button').find((button) => button.text().includes('预览'))
  expect(preview).toBeDefined()
  await preview!.trigger('click')
  await flushPromises()
  expect(getStoragePreviewUrl).toHaveBeenCalledExactlyOnceWith('sample.M4V')

  const video = wrapper.find('video.preview-video-player')
  expect(video.exists()).toBe(true)
  expect(video.attributes('src')).toBe('/api/storage/preview/sample.M4V?ticket=t1')

  wrapper.unmount()
  vi.restoreAllMocks()
})
