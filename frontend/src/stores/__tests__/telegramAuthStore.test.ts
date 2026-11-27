import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'
import { useAuthStore } from '@/stores/authStore'
import { authApi, telegramAuthApi } from '@/api'

vi.mock('@/api', () => ({
  telegramAuthApi: {
    getStatus: vi.fn(),
    sendWeb2FA: vi.fn(),
    verifyWeb2FA: vi.fn(),
    nativeSendCode: vi.fn(),
    nativeVerifyCode: vi.fn(),
    logout: vi.fn(),
    bootstrapAuth: vi.fn(),
    bootstrapLogout: vi.fn(),
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

describe('telegramAuthStore Unit Tests', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    setActivePinia(createPinia())
    vi.clearAllMocks()
  })

  describe('2FA Token Persistence & Expiration', () => {
    it('persists 2FA token on successful verification', async () => {
      const store = useTelegramAuthStore()
      vi.mocked(telegramAuthApi.verifyWeb2FA).mockResolvedValueOnce({
        data: { status: 'ok', token: 'jwt-auth-token-xyz' },
      } as never)

      const success = await store.verifyWeb2FA('password123')
      expect(success).toBe(true)
      expect(store.isWeb2FAUnlocked).toBe(true)
      expect(store.twoFAToken).toBe('jwt-auth-token-xyz')
      expect(store.get2FAToken()).toBe('jwt-auth-token-xyz')

      const saved = localStorage.getItem('tg_hub_2fa_unlocked')
      expect(saved).not.toBeNull()
      const parsed = JSON.parse(saved!)
      expect(parsed.unlocked).toBe(true)
      expect(parsed.token).toBe('jwt-auth-token-xyz')
      expect(parsed.expires).toBeGreaterThan(Date.now())
    })

    it('get2FAToken invalidates in-memory state on expired entry without deleting storage', () => {
      const expiredTime = Date.now() - 5000
      const expiredPayload = JSON.stringify({ unlocked: true, token: 'expired-token', expires: expiredTime })
      localStorage.setItem('tg_hub_2fa_unlocked', expiredPayload)

      const store = useTelegramAuthStore()
      expect(store.get2FAToken()).toBe('')
      expect(store.isWeb2FAUnlocked).toBe(false)
      expect(localStorage.getItem('tg_hub_2fa_unlocked')).toBe(expiredPayload)
    })

    it('passive read does not wipe a valid replacement credential written concurrently', () => {
      const expiredPayload = JSON.stringify({ unlocked: true, token: 'old', expires: Date.now() - 5000 })
      const replacementPayload = JSON.stringify({ unlocked: true, token: 'fresh', expires: Date.now() + 60000 })
      const store = useTelegramAuthStore()
      vi.spyOn(localStorage, 'getItem').mockImplementationOnce(() => {
        localStorage.setItem('tg_hub_2fa_unlocked', replacementPayload)
        return expiredPayload
      })

      expect(store.get2FAToken()).toBe('')
      expect(localStorage.getItem('tg_hub_2fa_unlocked')).toBe(replacementPayload)
    })

    it('clear2FAStorage clears token and local storage', () => {
      const store = useTelegramAuthStore()
      store.isWeb2FAUnlocked = true
      store.twoFAToken = 'valid-token'
      localStorage.setItem(
        'tg_hub_2fa_unlocked',
        JSON.stringify({ unlocked: true, token: 'valid-token', expires: Date.now() + 100000 }),
      )

      store.clear2FAStorage()
      expect(store.isWeb2FAUnlocked).toBe(false)
      expect(store.twoFAToken).toBe('')
      expect(localStorage.getItem('tg_hub_2fa_unlocked')).toBeNull()
    })
  })

  describe('Storage & Session invalidation', () => {
    it('reacts to cross-tab storage clearing on get2FAToken', () => {
      const store = useTelegramAuthStore()
      store.isWeb2FAUnlocked = true
      store.twoFAToken = 'token-tab-1'

      // Cross-tab storage cleared
      localStorage.removeItem('tg_hub_2fa_unlocked')
      expect(store.get2FAToken()).toBe('')
      expect(store.isWeb2FAUnlocked).toBe(false)
      expect(store.twoFAToken).toBe('')
    })

    it('reportSessionInvalid sets session message and dismissSessionInvalid clears it', () => {
      const store = useTelegramAuthStore()
      store.reportSessionInvalid('自定义失效消息')
      expect(store.sessionInvalidMessage).toBe('自定义失效消息')

      store.dismissSessionInvalid()
      expect(store.sessionInvalidMessage).toBe('')
    })
  })

  describe('Admission status races', () => {
    it.each(['admitted', 'failed', 'authStore'] as const)(
      'preserves a newer restricted transition when an older %s response arrives',
      async (scenario) => {
        const store = useTelegramAuthStore()
        let resolve!: (data: never) => void
        let reject!: (error: Error) => void
        vi.mocked(authApi.getStatus).mockImplementationOnce(
          () =>
            new Promise((ok, fail) => {
              resolve = ok
              reject = fail
            }),
        )
        const pending = scenario === 'authStore' ? useAuthStore().checkAuthStatus() : store.checkAdmission()
        store.enterRestrictedMode()
        if (scenario === 'failed') reject(new Error('stale request failed'))
        else resolve({ data: { restricted: false, auth_required: false } } as never)
        await pending
        expect(store.admissionPhase).toBe('restricted')
        expect(store.admissionError).toBe('')
      },
    )

    it('accepts a valid admission response even when a separate status request fails', async () => {
      const store = useTelegramAuthStore()
      let resolve!: (data: never) => void
      vi.mocked(authApi.getStatus).mockImplementationOnce(
        () =>
          new Promise((ok) => {
            resolve = ok
          }),
      )
      vi.mocked(telegramAuthApi.getStatus).mockRejectedValueOnce(new Error('status unavailable'))
      const pending = store.checkAdmission()
      const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
      await expect(store.fetchStatus()).resolves.toBe(false)
      consoleError.mockRestore()
      resolve({ data: { restricted: false, auth_required: false } } as never)
      await expect(pending).resolves.toBe(true)
      expect(store.admissionPhase).toBe('admitted')
    })

    it.each(['checkAdmission', 'checkAuthStatus'] as const)(
      'keeps bootstrap authorization when a pre-login %s response finishes late',
      async (statusReader) => {
        const store = useTelegramAuthStore()
        store.enterRestrictedMode()
        let resolve!: (data: never) => void
        vi.mocked(authApi.getStatus).mockImplementationOnce(
          () =>
            new Promise((ok) => {
              resolve = ok
            }),
        )
        const pending = statusReader === 'checkAdmission' ? store.checkAdmission() : useAuthStore().checkAuthStatus()
        vi.mocked(telegramAuthApi.bootstrapAuth).mockResolvedValueOnce({ data: { authorized: true } } as never)
        vi.mocked(telegramAuthApi.getStatus).mockResolvedValueOnce({
          data: { restricted: true, bootstrap_authorized: true },
        } as never)
        await expect(store.authenticateBootstrap('test-bootstrap-token')).resolves.toBe(true)
        expect(store.bootstrapAuthorized).toBe(true)

        resolve({
          data: { restricted: true, bootstrap_authorized: false, default_phone_configured: true, auth_required: false },
        } as never)
        await pending
        expect(store.bootstrapAuthorized).toBe(true)
        expect(store.defaultPhoneConfigured).toBe(true)
      },
    )

    it('ignores bootstrap authorization from both status reads started before logout', async () => {
      const store = useTelegramAuthStore()
      store.enterRestrictedMode()
      store.bootstrapAuthorized = true
      let resolveAuth!: (data: never) => void
      let resolveTelegram!: (data: never) => void
      vi.mocked(authApi.getStatus).mockImplementationOnce(
        () =>
          new Promise((ok) => {
            resolveAuth = ok
          }),
      )
      vi.mocked(telegramAuthApi.getStatus).mockImplementationOnce(
        () =>
          new Promise((ok) => {
            resolveTelegram = ok
          }),
      )
      vi.mocked(telegramAuthApi.bootstrapLogout).mockResolvedValueOnce({ data: { status: 'ok' } } as never)
      const reads = [store.checkAdmission(), store.fetchStatus()]
      await store.logoutBootstrap()
      const stale = { data: { restricted: true, bootstrap_authorized: true, auth_required: false } } as never
      resolveTelegram(stale)
      resolveAuth(stale)
      await Promise.all(reads)
      expect(store.isRestricted).toBe(true)
      expect(store.bootstrapAuthorized).toBe(false)
    })
  })
})
