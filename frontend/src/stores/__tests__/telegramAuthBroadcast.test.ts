import { expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

vi.mock('@/api', () => ({ authApi: {} }))
vi.mock('@/utils/discreteApi', () => ({ message: { error: vi.fn() } }))

it('a clear broadcast preserves a credential saved after the receiver reads storage', async () => {
  class Channel {
    static current: Channel
    onmessage: ((event: MessageEvent) => void) | null = null
    constructor() {
      Channel.current = this
    }
  }
  vi.stubGlobal('BroadcastChannel', Channel)
  try {
    localStorage.clear()
    setActivePinia(createPinia())
    const { useTelegramAuthStore } = await import('../telegramAuthStore')
    const store = useTelegramAuthStore()
    store.isWeb2FAUnlocked = true
    store.twoFAToken = 'old'
    const credential = JSON.stringify({ token: 'new', expires: Date.now() + 100000 })
    vi.spyOn(localStorage, 'getItem').mockImplementationOnce(() => {
      localStorage.setItem('tg_hub_2fa_unlocked', credential)
      return null
    })
    Channel.current.onmessage?.({ data: { type: 'AUTH_CLEARED' } } as MessageEvent)
    expect(store.isWeb2FAUnlocked).toBe(false)
    expect(store.twoFAToken).toBe('')
    expect(localStorage.getItem('tg_hub_2fa_unlocked')).toBe(credential)
  } finally {
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
    localStorage.clear()
  }
})
