import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { usePresetStore } from '../presetStore'
import { presetsApi } from '@/api'
import { message } from '@/utils/discreteApi'

vi.mock('@/api', () => ({
  presetsApi: {
    list: vi.fn(),
    create: vi.fn(),
    delete: vi.fn(),
    inspect: vi.fn(),
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

describe('usePresetStore', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    vi.clearAllMocks()
  })

  it('loads presets and populates state', async () => {
    const mockData = [
      { id: 1, name: 'Target A', channel_id: '-100111', preset_type: 'target' },
      { id: 2, name: 'Source B', channel_id: '-100222', preset_type: 'source' },
    ]
    vi.mocked(presetsApi.list).mockResolvedValueOnce({ data: mockData } as never)

    const store = usePresetStore()
    await store.loadPresets()

    expect(presetsApi.list).toHaveBeenCalledWith(null)
    expect(store.presets).toHaveLength(2)
    expect(store.targetPresets).toHaveLength(1)
    expect(store.targetPresets[0].name).toBe('Target A')
    expect(store.sourcePresets).toHaveLength(1)
    expect(store.sourcePresets[0].name).toBe('Source B')
  })

  it('deduplicates concurrent in-flight load requests', async () => {
    let resolvePromise: (value: unknown) => void = () => {}
    const pendingPromise = new Promise((resolve) => {
      resolvePromise = resolve
    })
    vi.mocked(presetsApi.list).mockReturnValueOnce(pendingPromise as never)

    const store = usePresetStore()
    const p1 = store.loadPresets('target')
    const p2 = store.loadPresets('target')

    expect(presetsApi.list).toHaveBeenCalledTimes(1)
    resolvePromise({ data: [] })
    await Promise.all([p1, p2])
  })

  it('computes target and source preset options with fallback to all presets', async () => {
    const mockData = [
      { id: 1, name: 'Legacy Preset', channel_id: '-100333' }, // preset_type missing -> target fallback
    ]
    vi.mocked(presetsApi.list).mockResolvedValueOnce({ data: mockData } as never)

    const store = usePresetStore()
    await store.loadPresets()

    // targetPresets matches item with missing preset_type (treated as 'target')
    expect(store.targetPresetOptions).toEqual([{ label: 'Legacy Preset (-100333)', value: '-100333' }])
    // sourcePresets is empty, so sourcePresetOptions falls back to all presets
    expect(store.sourcePresetOptions).toEqual([{ label: 'Legacy Preset (-100333)', value: '-100333' }])
  })

  it('adds preset and displays success toast', async () => {
    vi.mocked(presetsApi.create).mockResolvedValueOnce({ data: {} } as never)
    vi.mocked(presetsApi.list).mockResolvedValueOnce({ data: [] } as never)

    const store = usePresetStore()
    const ok = await store.addPreset('New Target', '-100444', 'target', 'Channel Title')

    expect(ok).toBe(true)
    expect(presetsApi.create).toHaveBeenCalledWith({
      name: 'New Target',
      channel_id: '-100444',
      preset_type: 'target',
      title: 'Channel Title',
    })
    expect(message.success).toHaveBeenCalledWith('预设频道保存成功！')
    expect(presetsApi.list).toHaveBeenCalled()
  })

  it('deletes preset and displays info toast', async () => {
    vi.mocked(presetsApi.delete).mockResolvedValueOnce({ data: {} } as never)
    vi.mocked(presetsApi.list).mockResolvedValueOnce({ data: [] } as never)

    const store = usePresetStore()
    const ok = await store.deletePreset(10)

    expect(ok).toBe(true)
    expect(presetsApi.delete).toHaveBeenCalledWith(10)
    expect(message.info).toHaveBeenCalledWith('预设已删除')
    expect(presetsApi.list).toHaveBeenCalled()
  })

  it('inspects channel with optional accountId', async () => {
    const inspectResponse = { can_read: true, can_write: true, title: 'My Channel' }
    vi.mocked(presetsApi.inspect).mockResolvedValueOnce({ data: inspectResponse } as never)

    const store = usePresetStore()
    const result = await store.inspectPresetChannel('-100555', 'target', 42)

    expect(presetsApi.inspect).toHaveBeenCalledWith({
      channel_id: '-100555',
      preset_type: 'target',
      account_id: 42,
    })
    expect(result).toEqual(inspectResponse)
  })
})
