import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createApp, nextTick } from 'vue'
import { createPinia, setActivePinia } from 'pinia'
import { darkTheme } from 'naive-ui'
import { configProviderProps, message } from '../discreteApi'
import { useThemeStore } from '@/stores/themeStore'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'
import TelegramAuthModal from '@/components/auth/TelegramAuthModal.vue'
import { telegramAuthApi } from '@/api'

vi.mock('@/api', () => ({
  authApi: {
    getStatus: vi.fn(),
    verify: vi.fn(),
  },
  telegramAuthApi: {
    getStatus: vi.fn(),
    sendWeb2FA: vi.fn(),
    verifyWeb2FA: vi.fn(),
    nativeSendCode: vi.fn(),
    nativeVerifyCode: vi.fn(),
  },
}))

describe('discreteApi & Pinia initialization ordering', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    vi.clearAllMocks()
  })

  it('evaluates configProviderProps and calls message.success safely before Pinia is active', () => {
    setActivePinia(undefined)

    expect(() => configProviderProps.value).not.toThrow()
    expect(configProviderProps.value.theme).toBeNull()
    expect(() => {
      message.success('Pinia 挂载前调用不崩溃')
      message.error('错误提示不崩溃')
    }).not.toThrow()
  })

  it('reactively follows themeStore light/dark switching after Pinia is mounted', () => {
    setActivePinia(createPinia())
    const themeStore = useThemeStore()

    themeStore.setThemeMode('dark')
    expect(configProviderProps.value.theme).toBe(darkTheme)
    expect(configProviderProps.value.themeOverrides?.common?.cardColor).toBe('#1e293b')

    themeStore.setThemeMode('light')
    expect(configProviderProps.value.theme).toBeNull()
    expect(configProviderProps.value.themeOverrides?.common?.cardColor).toBe('#ffffff')
  })

  it('completes sendWeb2FA and returns true with real discreteApi message toast', async () => {
    setActivePinia(createPinia())
    vi.mocked(telegramAuthApi.sendWeb2FA).mockResolvedValueOnce({
      data: { status: 'ok', message: '验证码已发送至您的 Telegram 收藏夹 (Saved Messages)' },
    } as never)

    const tgAuthStore = useTelegramAuthStore()
    const ok = await tgAuthStore.sendWeb2FA()

    expect(ok).toBe(true)
    expect(telegramAuthApi.sendWeb2FA).toHaveBeenCalledTimes(1)
  })

  it('transitions TelegramAuthModal from send button to 6-digit OTP input box after sendWeb2FA', async () => {
    const pinia = createPinia()
    setActivePinia(pinia)
    vi.mocked(telegramAuthApi.sendWeb2FA).mockResolvedValueOnce({
      data: { status: 'ok', message: '验证码已发送至您的 Telegram 收藏夹 (Saved Messages)' },
    } as never)

    const tgAuthStore = useTelegramAuthStore()
    tgAuthStore.authState = 'AUTHENTICATED'
    tgAuthStore.isModalVisible = true

    const host = document.createElement('div')
    document.body.appendChild(host)
    const app = createApp(TelegramAuthModal)
    app.use(pinia)
    app.mount(host)
    await nextTick()

    const sendBtn = Array.from(document.querySelectorAll('button')).find((b) =>
      b.textContent?.includes('发送动态验证码'),
    )
    expect(sendBtn).toBeDefined()
    sendBtn!.click()

    await vi.waitFor(() => {
      const otpInput = document.querySelector('input[placeholder="输入 6 位动态验证码"]')
      expect(otpInput).not.toBeNull()
    })

    app.unmount()
    host.remove()
  })
})
