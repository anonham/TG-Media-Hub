import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useForwardStore } from '@/stores/forwardStore'
import { forwardApi } from '@/api'

vi.mock('@/api', () => ({
  forwardApi: {
    getCoverage: vi.fn(),
    getGaps: vi.fn(),
    estimate: vi.fn(),
    getHistory: vi.fn(),
    getReceipt: vi.fn(),
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

describe('forwardStore Unit Tests', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    vi.clearAllMocks()
  })

  describe('loadCoverage & Sequence Guard', () => {
    it('populates coverage on successful response', async () => {
      const store = useForwardStore()
      vi.mocked(forwardApi.getCoverage).mockResolvedValueOnce({
        data: {
          code: 0,
          message: 'ok',
          data: {
            source_chat: '-100111',
            target_chat: '-100222',
            stats: { forwarded: 50 },
            forwarded_runs: [{ start: 1, end: 50, count: 50 }],
          },
        },
      } as never)

      await store.loadCoverage('-100111', '-100222', 1, 100)
      expect(store.coverage).not.toBeNull()
      expect(store.coverage?.stats?.forwarded).toBe(50)
      expect(store.coverageLoading).toBe(false)
      expect(store.coverageError).toBeNull()
    })

    it('ignores stale response when a newer loadCoverage is triggered (sequence guard)', async () => {
      const store = useForwardStore()
      let resolveFirst: (value: unknown) => void
      const firstPromise = new Promise((resolve) => {
        resolveFirst = resolve
      })

      vi.mocked(forwardApi.getCoverage)
        .mockReturnValueOnce(firstPromise as never)
        .mockResolvedValueOnce({
          data: {
            code: 0,
            message: 'ok',
            data: {
              source_chat: '-100111',
              target_chat: '-100222',
              stats: { forwarded: 99 },
              forwarded_runs: [],
            },
          },
        } as never)

      // Start call 1 (slow)
      const call1 = store.loadCoverage('-100111', '-100222', 1, 50)
      // Start call 2 (fast)
      await store.loadCoverage('-100111', '-100222', 1, 100)

      expect(store.coverage?.stats?.forwarded).toBe(99)

      // Now resolve call 1
      resolveFirst!({
        data: {
          code: 0,
          message: 'ok',
          data: {
            source_chat: '-100111',
            target_chat: '-100222',
            stats: { forwarded: 10 },
            forwarded_runs: [],
          },
        },
      })
      await call1

      // Stale call 1 data MUST NOT overwrite newer call 2 data
      expect(store.coverage?.stats?.forwarded).toBe(99)
    })
  })

  describe('loadGaps & Clear State', () => {
    it('populates gaps on success', async () => {
      const store = useForwardStore()
      vi.mocked(forwardApi.getGaps).mockResolvedValueOnce({
        data: {
          code: 0,
          message: 'ok',
          data: {
            gaps: [
              { start: 10, end: 20, est_count: 11 },
              { start: 50, end: 60, est_count: 11 },
            ],
          },
        },
      } as never)

      await store.loadGaps('-100111', '-100222')
      expect(store.gaps).toHaveLength(2)
      expect(store.gaps[0].est_count).toBe(11)
      expect(store.gapsLoading).toBe(false)
    })

    it('clearCoverage resets coverage, gaps and estimate', () => {
      const store = useForwardStore()
      store.coverage = { source_chat: '1', target_chat: '2' } as never
      store.gaps = [{ start_id: 1, end_id: 2, missing_count: 2 }] as never
      store.estimate = { total_messages: 5 } as never

      store.clearCoverage()
      expect(store.coverage).toBeNull()
      expect(store.gaps).toEqual([])
      expect(store.estimate).toBeNull()
    })

    it('clearGaps and clearEstimate preserve an in-flight loadCoverage response', async () => {
      const store = useForwardStore()
      let resolveCov!: (v: unknown) => void
      vi.mocked(forwardApi.getCoverage).mockReturnValueOnce(
        new Promise((r) => {
          resolveCov = r
        }) as never,
      )

      const pending = store.loadCoverage('-100111', '-100222')
      store.clearGaps()
      store.clearEstimate()

      resolveCov({
        data: {
          code: 0,
          message: 'ok',
          data: { source_chat: '-100111', target_chat: '-100222', stats: { forwarded: 42 }, forwarded_runs: [] },
        },
      })
      await pending
      expect(store.coverage?.stats?.forwarded).toBe(42)
    })
  })
})
