import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useAuthStore } from '@/stores/authStore'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'
import { authApi } from '@/api'

vi.mock('@/api', () => ({
  authApi: {
    getStatus: vi.fn(),
    verify: vi.fn(),
  },
  telegramAuthApi: {
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

describe('authStore Unit Tests', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    setActivePinia(createPinia())
    vi.clearAllMocks()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('discards stale open-mode checkAuthStatus responses when enterRestrictedMode occurs', async () => {
    const tgAuth = useTelegramAuthStore()
    const auth = useAuthStore()

    let resolveStatus!: (val: unknown) => void
    vi.mocked(authApi.getStatus).mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveStatus = resolve
        }) as never,
    )

    const pending = auth.checkAuthStatus()
    tgAuth.enterRestrictedMode()
    auth.isAuthRequired = true

    resolveStatus({ data: { restricted: false, auth_required: false } })
    const result = await pending

    expect(result).toBe(false)
    expect(auth.isAuthRequired).toBe(true)
    expect(tgAuth.admissionPhase).toBe('restricted')
  })

  it('discards checkAuthStatus after verifyKey if restriction arrives mid-verification', async () => {
    localStorage.setItem('tg_hub_api_key_remember', JSON.stringify({ key: 'saved-key', expires: Date.now() + 60_000 }))
    const tgAuth = useTelegramAuthStore()
    const auth = useAuthStore()

    vi.mocked(authApi.getStatus).mockResolvedValueOnce({
      data: { restricted: false, auth_required: true },
    } as never)
    let resolveVerify!: (val: unknown) => void
    vi.mocked(authApi.verify).mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveVerify = resolve
        }) as never,
    )

    const pending = auth.checkAuthStatus()
    await vi.waitFor(() => expect(authApi.verify).toHaveBeenCalledTimes(1))
    tgAuth.enterRestrictedMode()
    resolveVerify({ status: 200 })

    expect(await pending).toBe(false)
    expect(auth.isKeyVerified).toBe(false)
    expect(tgAuth.admissionPhase).toBe('restricted')
  })

  it('does not count transport or 5xx errors as invalid key attempts', async () => {
    const auth = useAuthStore()
    vi.mocked(authApi.verify).mockRejectedValue(new Error('Network Error'))

    expect(await auth.submitAuthKey('candidate-key')).toBe(false)
    expect(await auth.submitAuthKey('candidate-key')).toBe(false)
    expect(await auth.submitAuthKey('candidate-key')).toBe(false)
    expect(auth.failedAttempts).toBe(0)
    expect(auth.isLocked).toBe(false)

    vi.mocked(authApi.verify).mockRejectedValueOnce({ response: { status: 401 } })
    expect(await auth.submitAuthKey('wrong-key')).toBe(false)
    expect(auth.failedAttempts).toBe(1)
  })

  it('discards stale invalid checkAuthStatus verification after submitAuthKey succeeds', async () => {
    localStorage.setItem('tg_hub_api_key_remember', JSON.stringify({ key: 'old-key', expires: Date.now() + 60_000 }))
    const auth = useAuthStore()
    vi.mocked(authApi.getStatus).mockResolvedValueOnce({
      data: { restricted: false, auth_required: true },
    } as never)

    let rejectOldVerify!: (err: unknown) => void
    vi.mocked(authApi.verify)
      .mockImplementationOnce(
        () =>
          new Promise((_, reject) => {
            rejectOldVerify = reject
          }) as never,
      )
      .mockResolvedValueOnce({ status: 200 } as never)

    const pendingCheck = auth.checkAuthStatus()
    await vi.waitFor(() => expect(authApi.verify).toHaveBeenCalledTimes(1))

    expect(await auth.submitAuthKey('new-key')).toBe(true)
    expect(auth.isKeyVerified).toBe(true)
    expect(auth.isModalVisible).toBe(false)

    rejectOldVerify({ response: { status: 401 } })
    expect(await pendingCheck).toBe(false)
    expect(auth.apiKey).toBe('new-key')
    expect(auth.isKeyVerified).toBe(true)
    expect(auth.isModalVisible).toBe(false)
  })

  it('discards stale valid checkAuthStatus verification after clearAuthKey', async () => {
    localStorage.setItem('tg_hub_api_key_remember', JSON.stringify({ key: 'old-key', expires: Date.now() + 60_000 }))
    const auth = useAuthStore()
    vi.mocked(authApi.getStatus).mockResolvedValueOnce({
      data: { restricted: false, auth_required: true },
    } as never)

    let resolveOldVerify!: (val: unknown) => void
    vi.mocked(authApi.verify).mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveOldVerify = resolve
        }) as never,
    )

    const pendingCheck = auth.checkAuthStatus()
    await vi.waitFor(() => expect(authApi.verify).toHaveBeenCalledTimes(1))

    auth.clearAuthKey()
    expect(auth.apiKey).toBe('')
    expect(auth.isKeyVerified).toBe(false)
    expect(auth.isModalVisible).toBe(true)

    resolveOldVerify({ status: 200 })
    expect(await pendingCheck).toBe(false)
    expect(auth.apiKey).toBe('')
    expect(auth.isKeyVerified).toBe(false)
    expect(auth.isModalVisible).toBe(true)
  })
})
