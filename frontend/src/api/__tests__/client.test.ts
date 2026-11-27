import { AxiosError } from 'axios'
import { createPinia, setActivePinia } from 'pinia'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { client } from '@/api/client'
import { useAuthStore } from '@/stores/authStore'

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
