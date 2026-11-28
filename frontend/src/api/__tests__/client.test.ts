import { AxiosError } from 'axios'
import { createPinia, setActivePinia } from 'pinia'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { authApi } from '@/api'
import { client } from '@/api/client'
import { useAuthStore } from '@/stores/authStore'

vi.mock('@/utils/discreteApi', () => ({
  message: { error: vi.fn(), success: vi.fn() },
}))

describe('API Key prompt for Telegram login requests', () => {
  beforeEach(() => {
    localStorage.clear()
    setActivePinia(createPinia())
  })

  it.each([
    ['/api/telegram/native/send-code', undefined, true],
    ['/api/telegram/qr/start', undefined, true],
    ['/api/telegram/challenge/cancel', undefined, true],
    ['/api/telegram/qr/start', 'bootstrap_unauthorized', false],
    ['/api/telegram/native/verify-code', 'invalid_code', false],
    ['/api/telegram/bootstrap/auth', 'bootstrap_token_invalid', false],
  ] as const)('handles 401 for %s (%s)', async (url, errorCode, shouldPrompt) => {
    const handle401 = vi.spyOn(useAuthStore(), 'handle401Error').mockImplementation(() => {})
    const response = client.instance.post(
      url,
      {},
      {
        adapter: async (config) => {
          throw new AxiosError('Unauthorized', 'ERR_BAD_REQUEST', config, undefined, {
            config,
            data: { detail: 'Unauthorized', error_code: errorCode },
            headers: {},
            status: 401,
            statusText: 'Unauthorized',
          })
        },
      },
    )
    await expect(response).rejects.toMatchObject({ response: { status: 401 } })
    expect(handle401).toHaveBeenCalledTimes(shouldPrompt ? 1 : 0)
  })
})

describe('API Key verification 401 handling', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.setItem('tg_hub_api_key_session', 'saved-key')
    setActivePinia(createPinia())
  })

  it.each([
    { requestKey: 'saved-key', replacement: undefined, shouldPrompt: true },
    { requestKey: 'candidate-key', replacement: undefined, shouldPrompt: false },
    { requestKey: 'saved-key', replacement: 'new-key', shouldPrompt: false },
    { requestKey: 'saved-key', replacement: 'saved-key', shouldPrompt: false },
  ])(
    'handles a 401 for $requestKey with replacement $replacement',
    async ({ requestKey, replacement, shouldPrompt }) => {
      const auth = useAuthStore()
      const handle401 = vi.spyOn(auth, 'handle401Error')
      const oldRequest = Promise.withResolvers<void>()
      const response = Promise.withResolvers<void>()
      const adapter = client.getConfig().adapter
      let firstRequest = true
      client.setConfig({
        adapter: async (config) => {
          if (firstRequest) {
            firstRequest = false
            oldRequest.resolve()
            await response.promise
            throw new AxiosError('Unauthorized', 'ERR_BAD_REQUEST', config, undefined, {
              config,
              data: { detail: 'Unauthorized' },
              headers: {},
              status: 401,
              statusText: 'Unauthorized',
            })
          }
          return { config, data: {}, headers: {}, status: 200, statusText: 'OK' }
        },
      })

      try {
        const verification = authApi.verify(requestKey)
        await Promise.race([oldRequest.promise, verification])
        if (replacement) {
          expect(await auth.submitAuthKey(replacement)).toBe(true)
        }
        response.resolve()
        await expect(verification).rejects.toMatchObject({ response: { status: 401 } })
        expect(handle401).toHaveBeenCalledTimes(shouldPrompt ? 1 : 0)
        if (replacement) expect(auth.isKeyVerified).toBe(true)
      } finally {
        client.setConfig({ adapter })
        sessionStorage.clear()
        handle401.mockRestore()
      }
    },
  )
})
