import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { nextTick } from 'vue'
import PresetManageModal from '../PresetManageModal.vue'
import { usePresetStore } from '@/stores/presetStore'

vi.mock('@/utils/discreteApi', () => ({
  message: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}))

function deferred<T>() {
  let resolve!: (val: T) => void
  const promise = new Promise<T>((r) => {
    resolve = r
  })
  return { promise, resolve }
}

const modalStubs = {
  NModal: { props: ['show'], template: '<div><slot /></div>' },
  NTabs: { props: ['value'], template: '<div><slot /></div>' },
  NTabPane: { props: ['name', 'tab'], template: '<div><slot /></div>' },
  NFormItem: { template: '<div><slot /></div>' },
  NInput: {
    props: ['value'],
    emits: ['update:value'],
    template: '<input :value="value" @input="$emit(\'update:value\', $event.target.value)" />',
  },
  NRadioGroup: { template: '<div><slot /></div>' },
  NRadioButton: { template: '<button><slot /></button>' },
  NButton: { template: '<button><slot /></button>' },
  NEmpty: { template: '<div />' },
  NPopconfirm: { template: '<div><slot name="trigger" /></div>' },
}

describe('PresetManageModal audit regressions', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('initializes form when mounted with show=true and invalidates probe metadata on channel change', async () => {
    const store = usePresetStore()
    const loadSpy = vi.spyOn(store, 'loadPresets').mockResolvedValue(undefined as never)
    vi.spyOn(store, 'inspectPresetChannel').mockResolvedValue({
      is_accessible: true,
      parsed_chat_id: '-100123',
      channel_title: 'Test Channel',
      recommended_name: 'Test Preset',
    } as never)

    const wrapper = mount(PresetManageModal, {
      props: { show: true, defaultType: 'source' },
      global: { stubs: modalStubs },
    })
    await flushPromises()

    const vm = wrapper.vm as unknown as {
      form: { presetType: string; channelId: string; name: string; title: string }
      probeResult: unknown
      handleProbe: () => Promise<void>
    }

    expect(loadSpy).toHaveBeenCalledTimes(1)
    expect(vm.form.presetType).toBe('source')

    vm.form.channelId = 't.me/test_chan'
    await vm.handleProbe()
    expect(vm.form.channelId).toBe('-100123')
    expect(vm.form.title).toBe('Test Channel')
    expect(vm.probeResult).not.toBeNull()

    vm.form.channelId = 'another_chan'
    await nextTick()
    expect(vm.probeResult).toBeNull()
    expect(vm.form.title).toBe('')

    wrapper.unmount()
  })

  it('discards stale probe responses after channel or modal session changes and emits submitted snapshot', async () => {
    const store = usePresetStore()
    vi.spyOn(store, 'loadPresets').mockResolvedValue(undefined as never)
    const firstProbe = deferred<unknown>()
    const secondProbe = deferred<unknown>()
    vi.spyOn(store, 'inspectPresetChannel')
      .mockReturnValueOnce(firstProbe.promise as never)
      .mockReturnValueOnce(secondProbe.promise as never)

    const saveDeferred = deferred<boolean>()
    const addSpy = vi.spyOn(store, 'addPreset').mockReturnValue(saveDeferred.promise as never)

    const wrapper = mount(PresetManageModal, {
      props: { show: true, defaultType: 'target' },
      global: { stubs: modalStubs },
    })

    const vm = wrapper.vm as unknown as {
      form: { presetType: string; channelId: string; name: string; title: string }
      probeResult: { parsed_chat_id?: string } | null
      probing: boolean
      handleProbe: () => Promise<void>
      handleSave: () => Promise<void>
    }

    vm.form.channelId = 'first_channel'
    const p1 = vm.handleProbe()

    vm.form.channelId = 'second_channel'
    const p2 = vm.handleProbe()

    secondProbe.resolve({
      is_accessible: true,
      parsed_chat_id: '-100222',
      channel_title: 'Second',
      recommended_name: 'Second Name',
    })
    await p2

    // Stale first probe arrives later: must NOT overwrite second probe state
    firstProbe.resolve({
      is_accessible: true,
      parsed_chat_id: '-100111',
      channel_title: 'First',
      recommended_name: 'First Name',
    })
    await p1

    expect(vm.form.channelId).toBe('-100222')
    expect(vm.form.title).toBe('Second')
    expect(vm.probeResult?.parsed_chat_id).toBe('-100222')
    expect(vm.probing).toBe(false)

    // Snapshot saved emit even if form is mutated before addPreset resolves
    vm.form.name = '  My Saved Preset  '
    const savingPromise = vm.handleSave()
    vm.form.name = 'Mutated After Submit'
    vm.form.title = 'Mutated Title'
    vm.form.channelId = 'mutated_channel'
    saveDeferred.resolve(true)
    await savingPromise

    expect(addSpy).toHaveBeenCalledWith('My Saved Preset', '-100222', 'target', 'Second')
    expect(wrapper.emitted('saved')?.[0]?.[0]).toEqual({
      presetType: 'target',
      channelId: '-100222',
      name: 'My Saved Preset',
      title: 'Second',
    })

    wrapper.unmount()
  })
})
