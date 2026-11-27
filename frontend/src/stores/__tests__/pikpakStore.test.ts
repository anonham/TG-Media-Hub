import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { usePikpakStore, type DirSelectionRule } from '@/stores/pikpakStore'
import { pikpakApi } from '@/api'
import type { PikPakFileItem } from '@/api/gen'

vi.mock('@/api', () => ({
  pikpakApi: {
    authStatus: vi.fn(),
    deviceStart: vi.fn(),
    devicePoll: vi.fn(),
    fileList: vi.fn(),
    inspect: vi.fn(),
    publish: vi.fn(),
  },
  telegramAuthApi: {
    getStatus: vi.fn(),
  },
  authApi: {
    getStatus: vi.fn(),
  },
}))

vi.mock('@/utils/discreteApi', () => ({
  message: {
    success: vi.fn(),
    error: vi.fn(),
    info: vi.fn(),
    warning: vi.fn(),
  },
}))

describe('pikpakStore Unit Tests', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    vi.clearAllMocks()
    vi.useFakeTimers()
  })

  describe('fetchAuthStatus', () => {
    it('sets authStatus on successful response', async () => {
      const store = usePikpakStore()
      vi.mocked(pikpakApi.authStatus).mockResolvedValueOnce({
        data: {
          logged_in: true,
          state: 'logged_in',
          state_reason: null,
          email: 'user_123@test.com',
        },
      } as never)

      await store.fetchAuthStatus()
      expect(store.authStatus).not.toBeNull()
      expect(store.authStatus?.logged_in).toBe(true)
      expect(store.authStatus?.email).toBe('user_123@test.com')
      expect(store.authStatusLoading).toBe(false)
    })

    it('sets fallback unreachable state when API throws', async () => {
      const store = usePikpakStore()
      vi.mocked(pikpakApi.authStatus).mockRejectedValueOnce(new Error('Network error'))

      await store.fetchAuthStatus()
      expect(store.authStatus?.logged_in).toBe(false)
      expect(store.authStatus?.state).toBe('unreachable')
      expect(store.authStatus?.fetchError).toContain('Network error')
      expect(store.authStatusLoading).toBe(false)
    })
  })

  describe('Device Flow Lifecycle', () => {
    it('startDeviceLogin transitions to polling and stopDeviceFlow resets to idle', async () => {
      const store = usePikpakStore()
      vi.mocked(pikpakApi.deviceStart).mockResolvedValueOnce({
        data: {
          device_code: 'dev_code_1',
          user_code: 'USER-1234',
          verification_uri: 'https://mypikpak.com/device',
          interval: 5,
          expires_in: 300,
        },
      } as never)

      const started = await store.startDeviceLogin()
      expect(started).toBe(true)
      expect(store.deviceFlow.phase).toBe('polling')
      expect(store.deviceFlow.deviceCode).toBe('dev_code_1')
      expect(store.deviceFlow.userCode).toBe('USER-1234')

      store.stopDeviceFlow()
      expect(store.deviceFlow.phase).toBe('idle')
      expect(store.deviceFlow.deviceCode).toBe('')
      expect(store.deviceFlow.userCode).toBe('')
    })

    it('pollDeviceOnce transitions to success when logged_in is true', async () => {
      const store = usePikpakStore()
      store.deviceFlow = {
        phase: 'polling',
        deviceCode: 'dev_code_test',
        userCode: 'U-TEST',
        verificationUri: 'https://test.uri',
        interval: 5,
        expiresAt: Date.now() + 100000,
        lastStatus: '',
        errorMsg: '',
      }

      vi.mocked(pikpakApi.devicePoll).mockResolvedValueOnce({
        data: { logged_in: true, status: 'success' },
      } as never)
      vi.mocked(pikpakApi.authStatus).mockResolvedValueOnce({
        data: { logged_in: true, state: 'ok' },
      } as never)

      await store.pollDeviceOnce()
      expect(store.deviceFlow.phase).toBe('success')
      expect(store.deviceFlow.lastStatus).toBe('success')
    })
  })

  describe('fetchFolder & Cache Behavior', () => {
    it.each(['success', 'error'])('ignores obsolete root request %s after full invalidation', async (outcome) => {
      const store = usePikpakStore()
      let resolveOld!: (value: never) => void
      let rejectOld!: (error: Error) => void
      vi.mocked(pikpakApi.fileList).mockImplementationOnce(
        () =>
          new Promise((resolve, reject) => {
            resolveOld = resolve
            rejectOld = reject
          }),
      )
      const pending = store.fetchFolder()
      store.clearCache()
      vi.mocked(pikpakApi.fileList).mockResolvedValueOnce({ data: { items: [], next_page_token: 'new' } } as never)
      await store.fetchFolder()
      if (outcome === 'success') resolveOld({ data: { items: [], next_page_token: 'old' } } as never)
      else rejectOld(new Error('obsolete'))
      expect(await pending).toBeUndefined()
      expect(store.cacheEntry()?.nextPageToken).toBe('new')
      expect(store.cacheEntry()?.error).toBeNull()
    })

    it('fetches folder items and caches them', async () => {
      const store = usePikpakStore()
      const mockItems: PikPakFileItem[] = [
        { file_id: 'f1', name: 'video1.mp4', is_dir: false, size_bytes: 1024, parent_id: 'root' },
        { file_id: 'd1', name: 'folder1', is_dir: true, size_bytes: 0, parent_id: 'root' },
      ]
      vi.mocked(pikpakApi.fileList).mockResolvedValueOnce({
        data: { items: mockItems, next_page_token: 'page_2' },
      } as never)

      const entry = await store.fetchFolder('root')
      expect(entry).not.toBeNull()
      expect(entry?.items).toHaveLength(2)
      expect(entry?.nextPageToken).toBe('page_2')
      expect(store.cacheEntry('root')?.loaded).toBe(true)

      // Subsequent call within TTL returns cached entry without re-fetching
      const cached = await store.fetchFolder('root')
      expect(pikpakApi.fileList).toHaveBeenCalledTimes(1)
      expect(cached).toStrictEqual(entry)
    })

    it('loadMore appends new items to existing cache', async () => {
      const store = usePikpakStore()
      vi.mocked(pikpakApi.fileList)
        .mockResolvedValueOnce({
          data: {
            items: [{ file_id: 'f1', name: 'file1.mp4', is_dir: false, size_bytes: 100, parent_id: 'root' }],
            next_page_token: 'token_next',
          },
        } as never)
        .mockResolvedValueOnce({
          data: {
            items: [{ file_id: 'f2', name: 'file2.mp4', is_dir: false, size_bytes: 200, parent_id: 'root' }],
            next_page_token: null,
          },
        } as never)

      await store.fetchFolder('root')
      const moreEntry = await store.loadMore('root')
      expect(moreEntry?.items).toHaveLength(2)
      expect(moreEntry?.items[0].file_id).toBe('f1')
      expect(moreEntry?.items[1].file_id).toBe('f2')
      expect(moreEntry?.nextPageToken).toBeNull()
    })

    it('clearCache clears entries while respecting keepFolderId', async () => {
      const store = usePikpakStore()
      vi.mocked(pikpakApi.fileList).mockResolvedValue({ data: { items: [], next_page_token: null } } as never)
      await store.fetchFolder('dir1')
      await store.fetchFolder('dir2')
      expect(store.cacheEntry('dir1')).not.toBeNull()
      expect(store.cacheEntry('dir2')).not.toBeNull()

      store.clearCache('dir2')
      expect(store.cacheEntry('dir1')).toBeNull()
      expect(store.cacheEntry('dir2')).not.toBeNull()
    })
  })

  describe('Selection Rules & selectionView Aggregation', () => {
    it('retains the middle selected directory and its files in a nested tree', () => {
      const store = usePikpakStore()
      const file = { file_id: 'f', name: 'file', is_dir: false, size_bytes: 1, parent_id: 'B' }
      const rules: Pick<DirSelectionRule, 'dirId' | 'dirTree' | 'leaves'>[] = [
        { dirId: 'A', dirTree: { B: 'B', C: 'B/C' }, leaves: new Map([['f', file]]) },
        { dirId: 'B', dirTree: { C: 'B/C' }, leaves: new Map([['f', file]]) },
        { dirId: 'C', dirTree: {}, leaves: new Map() },
      ]
      store.selectionRules = rules.map((rule) => ({
        kind: 'dir' as const,
        dirName: rule.dirId,
        dirPath: rule.dirId,
        loading: false,
        foundCount: rule.leaves.size,
        aborted: false,
        ...rule,
      }))
      const root = store.selectionTree[0]
      expect(root.children.map((child) => child.dirId)).toEqual(['B'])
      expect(root.children[0].children.map((child) => child.dirId)).toEqual(['C'])
      expect(root.children[0].directLeaves.map((leaf) => leaf.fid)).toEqual(['f'])
    })

    it.each([false, true])('retries generation-invalidated directory walks (append=%s)', async (append) => {
      const store = usePikpakStore()
      const file: PikPakFileItem = { file_id: 'leaf', name: 'file', is_dir: false, size_bytes: 1 }
      let release!: (value: never) => void
      if (append)
        vi.mocked(pikpakApi.fileList).mockResolvedValueOnce({ data: { items: [], next_page_token: 'page2' } } as never)
      vi.mocked(pikpakApi.fileList).mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            release = resolve
          }),
      )
      const selecting = store.toggleDir({ file_id: 'dir', name: 'dir', is_dir: true, size_bytes: 0 })
      if (append) await vi.waitFor(() => expect(pikpakApi.fileList).toHaveBeenCalledTimes(2))
      store.clearCache()
      vi.mocked(pikpakApi.fileList).mockResolvedValueOnce({ data: { items: [file] } } as never)
      release({ data: { items: [] } } as never)
      await selecting
      expect(store.selectedFiles.has('leaf')).toBe(true)
      expect(store.selectionRules).toHaveLength(1)
      expect(pikpakApi.fileList).toHaveBeenCalledTimes(append ? 3 : 2)
    })

    it('does not retry a folder that is already loading', async () => {
      const store = usePikpakStore()
      let release!: (value: never) => void
      vi.mocked(pikpakApi.fileList).mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            release = resolve
          }),
      )
      const pending = store.fetchFolder('dir')
      await store.toggleDir({ file_id: 'dir', name: 'dir', is_dir: true, size_bytes: 0 })
      expect(pikpakApi.fileList).toHaveBeenCalledTimes(1)
      expect(store.selectionRules).toHaveLength(0)
      release({ data: { items: [] } } as never)
      await pending
    })

    it('toggles single files and computes selectionView', () => {
      const store = usePikpakStore()
      const item1: PikPakFileItem = { file_id: 'f1', name: 'clip1.mp4', is_dir: false, size_bytes: 1000 }
      const item2: PikPakFileItem = { file_id: 'f2', name: 'clip2.mp4', is_dir: false, size_bytes: 2000 }

      store.toggleFile(item1)
      store.toggleFile(item2)

      expect(store.selectedFiles.size).toBe(2)
      expect(store.selectionView.singles).toHaveLength(2)
      expect(store.selectionView.isFolderPublish).toBe(false)

      // Untoggle item1
      store.toggleFile(item1)
      expect(store.selectedFiles.size).toBe(1)
      expect(store.selectionView.singles).toHaveLength(1)
      expect(store.selectionView.singles[0].file_id).toBe('f2')
    })

    it('toggles directory and allows individual file exclusion', async () => {
      const store = usePikpakStore()
      const dirItem: PikPakFileItem = { file_id: 'd100', name: 'Season 1', is_dir: true, size_bytes: 0 }
      const f1: PikPakFileItem = {
        file_id: 'ep1',
        name: 'ep01.mp4',
        is_dir: false,
        size_bytes: 1000,
        parent_id: 'd100',
      }
      const f2: PikPakFileItem = {
        file_id: 'ep2',
        name: 'ep02.mp4',
        is_dir: false,
        size_bytes: 2000,
        parent_id: 'd100',
      }

      vi.mocked(pikpakApi.fileList).mockResolvedValueOnce({
        data: { items: [f1, f2], next_page_token: null },
      } as never)

      await store.toggleDir(dirItem)
      expect(store.selectionRules).toHaveLength(1)
      expect(store.selectedFiles.size).toBe(2)
      expect(store.selectionView.isFolderPublish).toBe(true)
      expect(store.selectionView.folderPublishName).toBe('Season 1')
      expect(store.selectionView.dirGroups).toHaveLength(1)

      // Exclude f1
      store.toggleFile(f1)
      expect(store.isSelected('ep1')).toBe(false)
      expect(store.selectedFiles.has('ep1')).toBe(false)
      expect(store.selectedFiles.has('ep2')).toBe(true)
      expect(store.selectedFiles.size).toBe(1)

      // Re-including f1
      store.toggleFile(f1)
      expect(store.isSelected('ep1')).toBe(true)
      expect(store.selectedFiles.size).toBe(2)
    })
  })
})
