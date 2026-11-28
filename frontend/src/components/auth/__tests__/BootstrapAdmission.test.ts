import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { defineComponent } from 'vue'
import App from '@/App.vue'
import router from '@/router'
import { authApi } from '@/api'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'
import type { AuthStatusResponse } from '@/api/gen'

const appLayoutMountSpy = vi.fn()

vi.mock('@/components/layout/AppLayout.vue', () => ({
  __esModule: true,
  default: defineComponent({
    name: 'AppLayout',
    setup() {
      appLayoutMountSpy()
      return {}
    },
    template: '<div data-testid="app-layout-mounted">AppLayout</div>',
  }),
}))

vi.mock('@/utils/discreteApi', () => ({
  message: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}))

vi.mock('@/api', () => ({
  authApi: {
    getStatus: vi.fn(),
    verify: vi.fn(),
  },
  telegramAuthApi: {
    getStatus: vi.fn(),
    bootstrapAuth: vi.fn(),
    bootstrapLogout: vi.fn(),
  },
}))

function seedCachedBrowserSecrets(): void {
  const future = Date.now() + 60 * 60 * 1000
  localStorage.setItem('tg_hub_api_key_remember', JSON.stringify({ key: 'cached-api-key-123', expires: future }))
  sessionStorage.setItem('tg_hub_api_key_session', 'cached-session-key-123')
  localStorage.setItem(
    'tg_hub_2fa_unlocked',
    JSON.stringify({ unlocked: true, token: 'cached-2fa-jwt-token', expires: future }),
  )
}

function authStatusResponse(payload: AuthStatusResponse) {
  return {
    data: payload,
    error: undefined,
    status: 200,
    statusText: 'OK',
    headers: {},
    config: {} as never,
  }
}

async function settleAsyncViews(): Promise<void> {
  await flushPromises()
  await vi.dynamicImportSettled()
  await flushPromises()
}

describe('BootstrapAdmission gate', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    setActivePinia(createPinia())
    vi.clearAllMocks()
    window.location.hash = '#/tasks'
  })

  afterEach(() => {
    vi.restoreAllMocks()
    window.location.hash = ''
  })

  it('prevents AppLayout mount with cached secrets and hash deep link while server admission is restricted', async () => {
    seedCachedBrowserSecrets()
    vi.mocked(authApi.getStatus).mockResolvedValue(
      authStatusResponse({
        auth_required: true,
        restricted: true,
        bootstrap_token_configured: true,
        bootstrap_authorized: false,
        default_phone_configured: false,
        default_phone_valid: true,
        binding_mode: 'first_bind',
      }),
    )

    await router.push('/tasks')
    await router.isReady()

    const wrapper = mount(App, {
      global: {
        plugins: [router],
        stubs: {
          NConfigProvider: { template: '<div><slot /></div>' },
          NGlobalStyle: { template: '<div />' },
        },
      },
    })

    await settleAsyncViews()

    const tgAuthStore = useTelegramAuthStore()
    expect(tgAuthStore.admissionPhase).toBe('restricted')
    expect(appLayoutMountSpy).not.toHaveBeenCalled()
    expect(wrapper.find('[data-testid="app-layout-mounted"]').exists()).toBe(false)
    expect(wrapper.find('#bootstrap-heading').exists()).toBe(true)

    // Attempting another hash deep link while restricted still blocks AppLayout and redirects.
    await router.push('/settings')
    await flushPromises()
    expect(router.currentRoute.value.path).toBe('/dashboard')
    expect(appLayoutMountSpy).not.toHaveBeenCalled()
    expect(wrapper.find('[data-testid="app-layout-mounted"]').exists()).toBe(false)

    wrapper.unmount()
  })

  it('keeps the bootstrap form available while the public console requires a Key', async () => {
    vi.mocked(authApi.getStatus).mockResolvedValueOnce(
      authStatusResponse({
        auth_required: false,
        restricted: true,
        console_locked: true,
        bootstrap_token_configured: true,
        bootstrap_authorized: false,
        binding_mode: 'first_bind',
      }),
    )

    const wrapper = mount(App, {
      global: {
        plugins: [router],
        stubs: {
          NConfigProvider: { template: '<div><slot /></div>' },
          NGlobalStyle: { template: '<div />' },
        },
      },
    })
    await settleAsyncViews()

    const tgAuthStore = useTelegramAuthStore()
    expect(tgAuthStore.admissionPhase).toBe('restricted')
    expect(wrapper.find('#bootstrap-deployer-token').exists()).toBe(true)
    expect(wrapper.get('.bootstrap-alert[role="status"]').text()).toContain('API_KEY')
    expect(appLayoutMountSpy).not.toHaveBeenCalled()

    vi.mocked(authApi.getStatus).mockResolvedValueOnce(
      authStatusResponse({ auth_required: false, restricted: false, console_locked: true }),
    )
    expect(await tgAuthStore.checkAdmission()).toBe(false)
    await settleAsyncViews()
    expect(tgAuthStore.admissionPhase).toBe('locked')
    expect(wrapper.find('#bootstrap-deployer-token').exists()).toBe(false)
    expect(appLayoutMountSpy).not.toHaveBeenCalled()

    vi.mocked(authApi.getStatus).mockResolvedValueOnce(
      authStatusResponse({ auth_required: true, restricted: false, console_locked: false }),
    )
    expect(await tgAuthStore.checkAdmission()).toBe(true)
    await settleAsyncViews()
    expect(appLayoutMountSpy).toHaveBeenCalledTimes(1)
    wrapper.unmount()
  })

  it('keeps AppLayout unmounted during checking/error and mounts only after explicit server admission', async () => {
    seedCachedBrowserSecrets()
    let resolveStatus!: (value: ReturnType<typeof authStatusResponse>) => void
    vi.mocked(authApi.getStatus).mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveStatus = resolve
        }),
    )

    const wrapper = mount(App, {
      global: {
        plugins: [router],
        stubs: {
          NConfigProvider: { template: '<div><slot /></div>' },
          NGlobalStyle: { template: '<div />' },
        },
      },
    })

    await flushPromises()
    const tgAuthStore = useTelegramAuthStore()
    expect(tgAuthStore.admissionPhase).toBe('checking')
    expect(appLayoutMountSpy).not.toHaveBeenCalled()
    expect(wrapper.find('[data-testid="app-layout-mounted"]').exists()).toBe(false)

    // Server explicitly returns restricted=true -> still no AppLayout mount.
    resolveStatus(
      authStatusResponse({
        auth_required: false,
        restricted: true,
        bootstrap_token_configured: true,
        bootstrap_authorized: false,
        default_phone_configured: false,
        default_phone_valid: true,
        binding_mode: 'rebind',
      }),
    )
    await flushPromises()
    expect(tgAuthStore.admissionPhase).toBe('restricted')
    expect(appLayoutMountSpy).not.toHaveBeenCalled()

    vi.mocked(authApi.getStatus).mockResolvedValueOnce(authStatusResponse({ auth_required: false }))
    expect(await tgAuthStore.checkAdmission()).toBe(false)
    await flushPromises()
    expect(tgAuthStore.admissionPhase).toBe('error')
    expect(appLayoutMountSpy).not.toHaveBeenCalled()

    // Explicit server admission arrives -> AppLayout mounts and deep link works.
    vi.mocked(authApi.getStatus).mockResolvedValueOnce(
      authStatusResponse({
        auth_required: false,
        restricted: false,
        bootstrap_token_configured: true,
        bootstrap_authorized: false,
        default_phone_configured: false,
        default_phone_valid: true,
        binding_mode: 'admitted',
      }),
    )
    const admitted = await tgAuthStore.checkAdmission()
    await flushPromises()

    expect(admitted).toBe(true)
    expect(tgAuthStore.admissionPhase).toBe('admitted')
    expect(appLayoutMountSpy).toHaveBeenCalledTimes(1)
    expect(wrapper.find('[data-testid="app-layout-mounted"]').exists()).toBe(true)

    await router.push('/tasks')
    await flushPromises()
    expect(router.currentRoute.value.path).toBe('/tasks')

    wrapper.unmount()
  })
})
