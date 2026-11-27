import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { nextTick } from 'vue'
import AppLayout from '../AppLayout.vue'
import Sidebar from '../Sidebar.vue'
import { useAuthStore } from '@/stores/authStore'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'
import { useTaskStore } from '@/stores/taskStore'

vi.mock('vue-router', () => ({
  useRoute: () => ({ path: '/dashboard', name: 'Dashboard', meta: {} }),
  useRouter: () => ({ push: vi.fn() }),
}))

const NTooltipStub = {
  template: '<div><slot name="trigger" /><slot /></div>',
}

const RouterLinkStub = {
  props: ['to'],
  template: '<slot href="#/test" :navigate="navigateSpy" />',
  setup() {
    return { navigateSpy: vi.fn() }
  },
}

function setViewportWidth(width: number) {
  Object.defineProperty(window, 'innerWidth', {
    configurable: true,
    writable: true,
    value: width,
  })
  window.dispatchEvent(new Event('resize'))
}

describe('Layout responsive and accessibility regressions', () => {
  const origInnerWidth = window.innerWidth

  beforeEach(() => {
    setActivePinia(createPinia())
  })

  afterEach(() => {
    Object.defineProperty(window, 'innerWidth', {
      configurable: true,
      writable: true,
      value: origInnerWidth,
    })
    vi.restoreAllMocks()
    document.body.innerHTML = ''
  })

  it('clears mobile drawer state on desktop resize and restores focus to menu button on mobile close', async () => {
    setViewportWidth(500)
    const authStore = useAuthStore()
    const tgAuthStore = useTelegramAuthStore()
    const taskStore = useTaskStore()
    vi.spyOn(authStore, 'checkAuthStatus').mockResolvedValue(true)
    vi.spyOn(tgAuthStore, 'fetchStatus').mockResolvedValue(true)
    vi.spyOn(taskStore, 'startPolling').mockImplementation(() => {})
    vi.spyOn(taskStore, 'stopPolling').mockImplementation(() => {})
    vi.spyOn(taskStore, 'initWebSocket').mockResolvedValue(undefined)

    const wrapper = mount(AppLayout, {
      attachTo: document.body,
      global: {
        stubs: {
          NButton: { template: '<button><slot /></button>' },
          NAlert: { template: '<div><slot /></div>' },
          NIcon: { template: '<i><slot /></i>' },
          NTooltip: NTooltipStub,
          RouterLink: RouterLinkStub,
          RouterView: { template: '<div />' },
        },
      },
    })

    const mainWrapper = wrapper.find('.app-main-wrapper')
    const menuBtn = wrapper.find('.mobile-menu-btn')
    const aside = wrapper.find('aside.sidebar-container')

    expect(mainWrapper.attributes('inert')).toBeUndefined()
    expect(aside.attributes('inert')).toBeDefined()
    expect(aside.attributes('aria-hidden')).toBe('true')

    await menuBtn.trigger('click')
    await nextTick()
    expect(mainWrapper.attributes('inert')).toBeDefined()
    expect(aside.attributes('inert')).toBeUndefined()
    expect(aside.attributes('aria-hidden')).toBeUndefined()

    await wrapper.find('.sidebar-mobile-mask').trigger('click')
    await nextTick()
    await nextTick()
    expect(mainWrapper.attributes('inert')).toBeUndefined()
    expect(aside.attributes('inert')).toBeDefined()
    expect(document.activeElement).toBe(menuBtn.element)

    // Reopen drawer on mobile, then resize viewport to desktop -> drawer clears & inert drops
    await menuBtn.trigger('click')
    await nextTick()
    expect(mainWrapper.attributes('inert')).toBeDefined()

    setViewportWidth(1024)
    await nextTick()
    expect(mainWrapper.attributes('inert')).toBeUndefined()
    expect(aside.attributes('inert')).toBeUndefined()

    wrapper.unmount()
  })

  it('closes mobile drawer when clicking a collapsed navigation item', async () => {
    setViewportWidth(500)
    const navigateSpy = vi.fn()
    const wrapper = mount(Sidebar, {
      props: {
        collapsed: true,
        mobileOpen: true,
      },
      global: {
        stubs: {
          NIcon: { template: '<i><slot /></i>' },
          NTooltip: NTooltipStub,
          RouterLink: {
            props: ['to'],
            setup() {
              return { navigate: navigateSpy }
            },
            template: '<slot href="#/test" :navigate="navigate" />',
          },
        },
      },
    })

    const collapsedItem = wrapper.find('.collapsed-item')
    expect(collapsedItem.exists()).toBe(true)
    await collapsedItem.trigger('click')

    expect(navigateSpy).toHaveBeenCalledTimes(1)
    expect(wrapper.emitted('close-mobile')).toHaveLength(1)

    wrapper.unmount()
  })

  it('keeps AppLayout hard-locked until stored API key passes verification even when 2FA is cached', async () => {
    const authStore = useAuthStore()
    const tgAuthStore = useTelegramAuthStore()
    const taskStore = useTaskStore()

    authStore.apiKey = 'stale-remembered-key'
    authStore.isAuthRequired = true
    authStore.isAuthChecked = true
    authStore.isKeyVerified = false
    tgAuthStore.admissionPhase = 'admitted'
    tgAuthStore.isWeb2FAUnlocked = true

    vi.spyOn(authStore, 'checkAuthStatus').mockResolvedValue(false)
    const startSpy = vi.spyOn(taskStore, 'startPolling').mockImplementation(() => {})
    const wsSpy = vi.spyOn(taskStore, 'initWebSocket').mockResolvedValue(undefined)

    const wrapper = mount(AppLayout, {
      global: {
        stubs: {
          NButton: { template: '<button><slot /></button>' },
          NAlert: { template: '<div><slot /></div>' },
          NIcon: { template: '<i><slot /></i>' },
          NTooltip: NTooltipStub,
          RouterLink: RouterLinkStub,
          RouterView: { template: '<div data-testid="router-view-mounted" />' },
        },
      },
    })

    expect(wrapper.find('.locked-placeholder').exists()).toBe(true)
    expect(wrapper.find('[data-testid="router-view-mounted"]').exists()).toBe(false)
    expect(startSpy).not.toHaveBeenCalled()
    expect(wsSpy).not.toHaveBeenCalled()

    wrapper.unmount()
  })
})
