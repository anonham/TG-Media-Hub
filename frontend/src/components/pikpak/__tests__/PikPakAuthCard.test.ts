import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, shallowMount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { nextTick } from 'vue'
import PikPakAuthCard from '../PikPakAuthCard.vue'
import { usePikpakStore } from '@/stores/pikpakStore'

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

describe('PikPakAuthCard timer lifecycle regressions', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('does not reschedule reprobe after unmounting while fetchAuthStatus is in flight', async () => {
    const store = usePikpakStore()
    const inflight = deferred<unknown>()
    const fetchSpy = vi
      .spyOn(store, 'fetchAuthStatus')
      .mockImplementationOnce(async () => {
        store.authStatus = { logged_in: false, state: 'not_bound' } as never
        return store.authStatus as never
      })
      .mockImplementationOnce(() => inflight.promise as never)
      .mockResolvedValue({ logged_in: false, state: 'not_bound' } as never)

    const wrapper = shallowMount(PikPakAuthCard)
    await flushPromises()
    expect(fetchSpy).toHaveBeenCalledTimes(1)

    // Advance 5s to fire the first scheduled reprobe (second call is now in flight)
    await vi.advanceTimersByTimeAsync(5000)
    expect(fetchSpy).toHaveBeenCalledTimes(2)

    // Unmount before the second fetch resolves
    wrapper.unmount()
    store.authStatus = { logged_in: false, state: 'unreachable' } as never
    inflight.resolve(store.authStatus)
    await flushPromises()

    // Advancing well beyond max backoff must not trigger additional polls
    await vi.advanceTimersByTimeAsync(120000)
    expect(fetchSpy).toHaveBeenCalledTimes(2)
  })

  it('cancels the 900ms device success close timer when modal closes or unmounts', async () => {
    const store = usePikpakStore()
    vi.spyOn(store, 'fetchAuthStatus').mockResolvedValue({ logged_in: false, state: 'not_bound' } as never)
    vi.spyOn(store, 'startDeviceLogin').mockResolvedValue(undefined as never)
    const stopSpy = vi.spyOn(store, 'stopDeviceFlow').mockImplementation(() => {})

    const wrapper = shallowMount(PikPakAuthCard)
    await flushPromises()

    const vm = wrapper.vm as unknown as {
      deviceModalShow: boolean
      openDeviceModal: () => void
    }

    vm.openDeviceModal()
    expect(vm.deviceModalShow).toBe(true)

    store.deviceFlow.phase = 'success'
    await nextTick()

    // User closes and immediately reopens modal before 900ms elapses
    vm.deviceModalShow = false
    await nextTick()
    expect(stopSpy).toHaveBeenCalledTimes(1)

    vm.deviceModalShow = true
    await nextTick()

    await vi.advanceTimersByTimeAsync(1000)
    expect(vm.deviceModalShow).toBe(true)
    expect(stopSpy).toHaveBeenCalledTimes(1)

    wrapper.unmount()
  })

  it('refuses to render non-https verificationUri as a clickable anchor', async () => {
    const store = usePikpakStore()
    vi.spyOn(store, 'fetchAuthStatus').mockResolvedValue({ logged_in: false, state: 'not_bound' } as never)
    store.deviceFlow = {
      phase: 'polling',
      deviceCode: 'dc',
      userCode: 'UC',
      verificationUri: 'javascript:alert(1)',
      interval: 5,
      expiresAt: 0,
      lastStatus: '',
      errorMsg: '',
    }

    const wrapper = shallowMount(PikPakAuthCard, {
      global: {
        stubs: {
          NModal: { template: '<div><slot /></div>' },
          NButton: { template: '<button><slot /></button>' },
          NAlert: { template: '<div><slot /></div>' },
          NTag: { template: '<span><slot /></span>' },
          NSpin: { template: '<span />' },
        },
      },
    })
    await flushPromises()
    expect(wrapper.find('a[href]').exists()).toBe(false)
    wrapper.unmount()
  })
})
