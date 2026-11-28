import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useThemeStore } from '@/stores/themeStore'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'
import { usePikpakStore } from '@/stores/pikpakStore'
import { errDetail, isAuthError } from '@/utils/errors'
import { pikpakApi, telegramAuthApi } from '@/api'

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
  pikpakApi: {
    authStatus: vi.fn(),
    fileList: vi.fn(),
    inspect: vi.fn(),
    publish: vi.fn(),
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

describe('Store core regressions', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    setActivePinia(createPinia())
    vi.clearAllMocks()
  })

  it('themeStore supports both "system" and legacy "auto" for system theme following', () => {
    const themeStore = useThemeStore()
    themeStore.setThemeMode('dark')
    expect(themeStore.isDark).toBe(true)

    themeStore.setThemeMode('light')
    expect(themeStore.isDark).toBe(false)

    // SettingsView passes 'system', which must follow systemPrefersDark (false in jsdom default)
    themeStore.setThemeMode('dark')
    themeStore.setThemeMode('system')
    expect(themeStore.themeMode).toBe('system')
    expect(themeStore.isDark).toBe(false)
  })

  it('telegramAuthStore invalidates expired or cleared 2FA token on get2FAToken() and storage events', async () => {
    const tgAuth = useTelegramAuthStore()
    vi.mocked(telegramAuthApi.verifyWeb2FA).mockResolvedValueOnce({
      data: { status: 'ok', token: 'jwt-token-valid-123' },
    } as never)

    await tgAuth.verifyWeb2FA('123456')
    expect(tgAuth.get2FAToken()).toBe('jwt-token-valid-123')
    expect(tgAuth.isWeb2FAUnlocked).toBe(true)

    // Simulate cross-tab logout removing localStorage item
    localStorage.removeItem('tg_hub_2fa_unlocked')
    window.dispatchEvent(new StorageEvent('storage', { key: 'tg_hub_2fa_unlocked', newValue: null }))

    expect(tgAuth.isWeb2FAUnlocked).toBe(false)
    expect(tgAuth.twoFAToken).toBe('')
    expect(tgAuth.get2FAToken()).toBe('')

    // Simulate expired token in localStorage while in-memory token was set
    vi.mocked(telegramAuthApi.verifyWeb2FA).mockResolvedValueOnce({
      data: { status: 'ok', token: 'jwt-token-soon-expired' },
    } as never)
    await tgAuth.verifyWeb2FA('654321')
    localStorage.setItem(
      'tg_hub_2fa_unlocked',
      JSON.stringify({ unlocked: true, expires: Date.now() - 1000, token: 'jwt-token-soon-expired' }),
    )
    expect(tgAuth.get2FAToken()).toBe('')
    expect(tgAuth.isWeb2FAUnlocked).toBe(false)
  })

  it('pikpakStore derives selectedFiles reactively from selectionView on toggleDir and exclusion', async () => {
    vi.mocked(pikpakApi.fileList).mockResolvedValueOnce({
      data: {
        items: [
          { file_id: 'f1', name: 'ep01.mp4', is_dir: false, size_bytes: 1024, parent_id: 'dir1' },
          { file_id: 'f2', name: 'ep02.mp4', is_dir: false, size_bytes: 2048, parent_id: 'dir1' },
        ],
        next_page_token: null,
      },
    } as never)

    const pikpak = usePikpakStore()
    await pikpak.toggleDir({ file_id: 'dir1', name: 'Season 1', is_dir: true, size_bytes: 0 })

    expect(pikpak.selectedFiles.size).toBe(2)
    expect(pikpak.selectedTotalBytes).toBe(3072)
    expect(pikpak.isSelected('f1')).toBe(true)

    // Exclude one file inside the selected directory
    pikpak.toggleFile({ file_id: 'f1', name: 'ep01.mp4', is_dir: false, size_bytes: 1024, parent_id: 'dir1' })
    expect(pikpak.selectedFiles.size).toBe(1)
    expect(pikpak.isSelected('f1')).toBe(false)
    expect(pikpak.selectedTotalBytes).toBe(2048)
  })

  it('errDetail formats FastAPI 422 array validation details cleanly', () => {
    const axios422 = {
      response: {
        data: {
          detail: [
            { loc: ['body', 'target_channel'], msg: 'Field required', type: 'missing' },
            { loc: ['body', 'upload_concurrency'], msg: 'Input should be less than or equal to 16' },
          ],
        },
      },
    }
    expect(errDetail(axios422)).toBe(
      'target_channel: Field required; upload_concurrency: Input should be less than or equal to 16',
    )
  })

  it('isAuthError correctly identifies 401 and 403 2FA errors to suppress duplicate toasts', () => {
    expect(isAuthError({ response: { status: 401 } })).toBe(true)
    expect(
      isAuthError({ response: { status: 403, data: { detail: '2FA 会话无效或已过期，请重新完成 Web 2FA 验证' } } }),
    ).toBe(true)
    expect(isAuthError({ response: { status: 403, data: { detail: 'Forbidden: other permission issue' } } })).toBe(
      false,
    )
    expect(isAuthError({ response: { status: 500, data: { detail: 'Internal Server Error' } } })).toBe(false)
    expect(isAuthError(new Error('Network error'))).toBe(false)
    expect(isAuthError(null)).toBe(false)
  })

  it('isAuthError exempts pikpak third-party 401 (gateway does not take over that domain)', () => {
    // 镜像 client.ts isThirdPartyAuth: /api/pikpak/* 的 401 不拉起 AuthModal, 错误须留给业务层
    expect(isAuthError({ response: { status: 401 }, config: { url: '/api/pikpak/file/list' } })).toBe(false)
    // 401 走 core 域仍视为网关接管
    expect(isAuthError({ response: { status: 401 }, config: { url: '/api/accounts' } })).toBe(true)
    // 403 2FA 契约不受域豁免影响 (拦截器对任意域均承接)
    expect(
      isAuthError({
        response: { status: 403, data: { detail: '2FA 会话无效或已过期，请重新完成 Web 2FA 验证' } },
        config: { url: '/api/pikpak/drive/list' },
      }),
    ).toBe(true)
  })
})
