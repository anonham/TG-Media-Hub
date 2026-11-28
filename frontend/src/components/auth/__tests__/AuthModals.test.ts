import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { NModal } from 'naive-ui'
import AuthModal from '../AuthModal.vue'
import TelegramAuthModal from '../TelegramAuthModal.vue'
import { useAuthStore } from '@/stores/authStore'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'

vi.mock('@/utils/discreteApi', () => ({
  message: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}))

describe('Auth components audit regressions', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('allows AuthModal dismissal in open mode without apiKey and blocks it when auth is required without apiKey', async () => {
    const authStore = useAuthStore()
    authStore.isAuthRequired = false
    authStore.apiKey = ''
    authStore.isModalVisible = true

    const NModalStub = {
      name: 'NModal',
      props: ['show', 'maskClosable'],
      emits: ['update:show'],
      template: '<div><slot /></div>',
    }
    const wrapper = mount(AuthModal, {
      global: {
        stubs: {
          NModal: NModalStub,
          NInput: { template: '<input />' },
          NInputGroup: { template: '<div><slot /></div>' },
          NButton: { template: '<button><slot /></button>' },
          NCheckbox: { template: '<input type="checkbox" />' },
        },
      },
    })

    const modal = wrapper.findComponent(NModal)
    expect(modal.exists()).toBe(true)
    await modal.vm.$emit('update:show', false)
    expect(authStore.isModalVisible).toBe(false)

    authStore.isAuthRequired = true
    authStore.isModalVisible = true
    await modal.vm.$emit('update:show', false)
    expect(authStore.isModalVisible).toBe(true)

    authStore.apiKey = 'valid-key'
    await modal.vm.$emit('update:show', false)
    expect(authStore.isModalVisible).toBe(false)

    wrapper.unmount()
  })

  it('preserves leading and trailing whitespace in Telegram 2FA password', async () => {
    const tgStore = useTelegramAuthStore()
    const verifySpy = vi.spyOn(tgStore, 'nativeVerifyCode').mockResolvedValue({
      status: 'ok',
      state: 'authenticated',
      authenticated: true,
      challenge_id: 'c-test',
      message: 'ok',
    })

    const wrapper = mount(TelegramAuthModal, {
      global: {
        stubs: {
          NModal: { template: '<div><slot /></div>' },
          NAlert: { template: '<div><slot /></div>' },
          NTabs: { template: '<div><slot /></div>' },
          NTabPane: { template: '<div><slot /></div>' },
          NFormItem: { template: '<div><slot /></div>' },
          NInput: { template: '<input />' },
          NButton: { template: '<button><slot /></button>' },
          NCollapse: { template: '<div><slot /></div>' },
          NCollapseItem: { template: '<div><slot /></div>' },
        },
      },
    })

    const vm = wrapper.vm as unknown as {
      nativeCode: string
      nativePassword: string
      handleNativeVerify: () => Promise<void>
    }

    vm.nativeCode = '12 345'
    vm.nativePassword = '  p@ss word  '
    await vm.handleNativeVerify()
    expect(verifySpy).toHaveBeenCalledWith('12345', '  p@ss word  ')

    vm.nativePassword = ''
    await vm.handleNativeVerify()
    expect(verifySpy).toHaveBeenLastCalledWith('12345', null)

    wrapper.unmount()
  })
})
