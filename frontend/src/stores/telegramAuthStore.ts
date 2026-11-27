import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import type {
  AuthStatusResponse,
  NativeQrChallengeStartResponse,
  NativeQrStartRequest,
  NativeSendCodeResponse,
  NativeVerifyCodeResponse,
  QrChallengeStatusResponse,
  TelegramAuthStatusResponse,
  TelegramStartupStatus,
} from '@/api/gen'
import { authApi, telegramAuthApi } from '@/api'
import { message } from '@/utils/discreteApi'
import { errDetail } from '@/utils/errors'
import { useAuthStore } from './authStore'

const AUTH_CHANNEL_NAME = 'tg_hub_auth_channel'
const authChannel =
  typeof window !== 'undefined' && 'BroadcastChannel' in window ? new BroadcastChannel(AUTH_CHANNEL_NAME) : null
let storageListenerBound = false

interface TwoFALocalStorage {
  unlocked: boolean
  token: string
}

function load2FAStorage(): TwoFALocalStorage {
  if (typeof window === 'undefined') return { unlocked: false, token: '' }
  const saved2FA = localStorage.getItem('tg_hub_2fa_unlocked')
  if (saved2FA) {
    try {
      const data = JSON.parse(saved2FA)
      if (data.expires > Date.now()) {
        // 旧版本载荷无 token 字段: 视为已解锁但缺会话凭证, 首个业务请求 403 后触发重验补发
        return { unlocked: true, token: data.token || '' }
      } else {
        localStorage.removeItem('tg_hub_2fa_unlocked')
      }
    } catch {
      localStorage.removeItem('tg_hub_2fa_unlocked')
    }
  }
  return { unlocked: false, token: '' }
}

export const useTelegramAuthStore = defineStore('telegramAuth', () => {
  const authStore = useAuthStore()

  // 状态 (初始化时优先检查跨 Tab 共享的 2FA 凭据)
  const authState = ref<string>('UNKNOWN') // UNKNOWN, UNAUTHENTICATED, AUTHENTICATED
  const isConnected = ref<boolean>(false)
  const isModalVisible = ref<boolean>(false)
  const initial2FA = load2FAStorage()
  const isWeb2FAUnlocked = ref<boolean>(initial2FA.unlocked)
  // 服务端签发的 2FA 会话 JWT (请求头 X-2FA-Token), 与 unlocked 同载荷存于 localStorage
  const twoFAToken = ref<string>(initial2FA.token)
  // 账号 Premium 档位 (来自 /api/telegram/status)
  const isPremium = ref<boolean>(false)
  const uploadLimitMb = ref<number>(2000) // 单文件限制: 普通号 2000 MiB (1.95 GiB)，Premium 号 4000 MiB (3.91 GiB)
  const startupDegraded = ref<boolean>(false)
  const startupStatus = ref<TelegramStartupStatus | null>(null)
  let statusRequestId = 0
  let admissionGeneration = 0
  let bootstrapGeneration = 0

  // 原生验证挑战状态 (不再存储 Telegram phone_code_hash)
  const activeChallengeId = ref<string>('')
  const currentPhone = ref<string>('')

  // 会话失效全局横幅 (WS system_event 或状态探测发现被登出时置位)
  const sessionInvalidMessage = ref<string>('')

  // 默认账号受限引导和公网 Key 锁定可同时成立。
  const admissionPhase = ref<'checking' | 'restricted' | 'locked' | 'admitted' | 'error'>('checking')
  const admissionError = ref<string>('')
  const consoleLocked = ref(false)
  const isRestricted = computed<boolean>(() => admissionPhase.value === 'restricted')
  const bindingMode = ref<string>('admitted')
  const bootstrapTokenConfigured = ref<boolean>(false)
  const bootstrapAuthorized = ref<boolean>(false)
  const defaultPhoneConfigured = ref<boolean>(false)
  const defaultPhoneValid = ref<boolean>(true)

  function captureAdmissionRead() {
    return { admissionGeneration, bootstrapGeneration }
  }

  function syncAdmissionStatus(
    data: Partial<AuthStatusResponse & TelegramAuthStatusResponse> | null | undefined,
    read: ReturnType<typeof captureAdmissionRead>,
  ): void {
    if (!data || read.admissionGeneration !== admissionGeneration) return
    if (typeof data.bootstrap_token_configured === 'boolean') {
      bootstrapTokenConfigured.value = data.bootstrap_token_configured
    }
    if (read.bootstrapGeneration === bootstrapGeneration && typeof data.bootstrap_authorized === 'boolean') {
      bootstrapAuthorized.value = data.bootstrap_authorized
    }
    if (typeof data.default_phone_configured === 'boolean') {
      defaultPhoneConfigured.value = data.default_phone_configured
    }
    if (typeof data.default_phone_valid === 'boolean') {
      defaultPhoneValid.value = data.default_phone_valid
    }
    if (typeof data.binding_mode === 'string' && data.binding_mode) {
      bindingMode.value = data.binding_mode
    }
    if (typeof data.console_locked === 'boolean') {
      consoleLocked.value = data.console_locked
    }
    if (typeof data.restricted === 'boolean') {
      admissionPhase.value = data.restricted ? 'restricted' : consoleLocked.value ? 'locked' : 'admitted'
      admissionError.value = ''
    }
    if (data.restricted) startupStatus.value = null
  }

  function enterRestrictedMode(): void {
    statusRequestId += 1
    admissionGeneration += 1
    admissionPhase.value = 'restricted'
    admissionError.value = ''
    bootstrapAuthorized.value = false
    startupStatus.value = null
  }

  function enterLockedMode(): void {
    consoleLocked.value = true
    if (admissionPhase.value === 'restricted') return
    statusRequestId += 1
    admissionGeneration += 1
    admissionPhase.value = 'locked'
    admissionError.value = ''
    bootstrapAuthorized.value = false
    startupStatus.value = null
  }

  async function checkAdmission(): Promise<boolean> {
    const read = captureAdmissionRead()
    admissionPhase.value = 'checking'
    admissionError.value = ''
    try {
      const res = await authApi.getStatus()
      if (read.admissionGeneration !== admissionGeneration) return false
      const data = res.data
      if (typeof data?.restricted !== 'boolean') {
        throw new Error('服务端未返回默认账号准入状态')
      }
      authStore.isAuthRequired = data.auth_required
      consoleLocked.value = data.console_locked === true
      syncAdmissionStatus(data, read)
      return !data.restricted && !consoleLocked.value
    } catch (err: unknown) {
      if (read.admissionGeneration !== admissionGeneration) return false
      admissionPhase.value = 'error'
      admissionError.value = errDetail(err) || '无法获取服务准入状态，请检查后端服务连接'
      return false
    }
  }

  async function authenticateBootstrap(token: string): Promise<boolean> {
    const clean = token.trim()
    if (!clean) {
      throw new Error('请输入部署配置中的 TG_BOOTSTRAP_TOKEN')
    }
    await telegramAuthApi.bootstrapAuth(clean)
    bootstrapGeneration += 1
    bootstrapAuthorized.value = true
    await fetchStatus()
    return true
  }

  async function logoutBootstrap(): Promise<void> {
    try {
      await telegramAuthApi.bootstrapLogout()
    } finally {
      statusRequestId += 1
      bootstrapGeneration += 1
      bootstrapAuthorized.value = false
      activeChallengeId.value = ''
    }
  }

  function reportSessionInvalid(msg: string = 'Telegram 账号会话已失效，请重新绑定账号'): void {
    sessionInvalidMessage.value = msg
    message.error(msg)
  }

  function dismissSessionInvalid(): void {
    sessionInvalidMessage.value = ''
  }

  function openRebindModal(): void {
    sessionInvalidMessage.value = ''
    isModalVisible.value = true
  }

  // 跨 Tab 广播监听与 Storage 变化监听
  if (authChannel) {
    authChannel.onmessage = (event: MessageEvent) => {
      if (event.data?.type === '2FA_UNLOCKED') {
        isWeb2FAUnlocked.value = true
        twoFAToken.value = event.data.token || load2FAStorage().token
        isModalVisible.value = false
      } else if (event.data?.type === 'AUTH_CLEARED') {
        // 通知可能与本 Tab 的新验证交错 —— 发送方已清共享存储, 若其后
        // 本/他 Tab 完成重验则存储已有新凭证, 不得回删; 仅在存储确实为空时同步清理
        if (!load2FAStorage().token) {
          isWeb2FAUnlocked.value = false
          twoFAToken.value = ''
        }
      }
    }
  }

  if (typeof window !== 'undefined' && !storageListenerBound) {
    storageListenerBound = true
    window.addEventListener('storage', (e: StorageEvent) => {
      if (e.key === 'tg_hub_2fa_unlocked') {
        if (e.newValue) {
          try {
            const data = JSON.parse(e.newValue)
            if (data.expires > Date.now()) {
              isWeb2FAUnlocked.value = true
              twoFAToken.value = data.token || ''
              isModalVisible.value = false
            } else {
              isWeb2FAUnlocked.value = false
              twoFAToken.value = ''
            }
          } catch (err) {
            console.debug('跨 Tab 凭证同步解析降级:', err)
          }
        } else {
          isWeb2FAUnlocked.value = false
          twoFAToken.value = ''
        }
      }
    })
  }

  function persist2FAUnlock(token: string | null = null): void {
    isWeb2FAUnlocked.value = true
    isModalVisible.value = false
    if (token) twoFAToken.value = token
    const expires = Date.now() + 24 * 60 * 60 * 1000 // 24小时有效
    localStorage.setItem(
      'tg_hub_2fa_unlocked',
      JSON.stringify({
        unlocked: true,
        expires,
        token: twoFAToken.value,
      }),
    )
    if (authChannel) {
      try {
        authChannel.postMessage({ type: '2FA_UNLOCKED', expires, token: twoFAToken.value })
      } catch (err) {
        console.warn('BroadcastChannel 发送消息失败:', err)
      }
    }
  }

  function clear2FAStorage(): void {
    isWeb2FAUnlocked.value = false
    twoFAToken.value = ''
    localStorage.removeItem('tg_hub_2fa_unlocked')
    if (authChannel) {
      try {
        authChannel.postMessage({ type: 'AUTH_CLEARED' })
      } catch (err) {
        console.warn('BroadcastChannel 发送消息失败:', err)
      }
    }
  }

  function get2FAToken(): string {
    // 每次读取均通过 load2FAStorage 校验 24h 过期时间与跨 Tab 登出状态，杜绝返回已过期/已吊销的内存旧 Token
    const loaded = load2FAStorage()
    if (!loaded.unlocked) {
      isWeb2FAUnlocked.value = false
      twoFAToken.value = ''
      return ''
    }
    if (loaded.token) {
      twoFAToken.value = loaded.token
    }
    return twoFAToken.value
  }

  function handle403Error(msg: string = '2FA 会话已失效，请重新完成 Web 2FA 验证'): void {
    // 守卫: 任一鉴权弹窗已打开则不再重复弹窗或刷屏
    if (isModalVisible.value || authStore.isModalVisible) return
    clear2FAStorage()
    isModalVisible.value = true
    message.warning(msg)
  }

  async function fetchStatus(probePremium = true, signal?: AbortSignal): Promise<boolean> {
    if (!isRestricted.value && !authStore.apiKey && authStore.isAuthRequired) return false
    const requestId = ++statusRequestId
    const read = captureAdmissionRead()
    try {
      // API Key + 2FA 会话 token 由 client.ts 请求拦截器统一注入
      const res = await telegramAuthApi.getStatus(probePremium, signal)
      if (signal?.aborted || requestId !== statusRequestId) return false
      const data = res.data
      const prevState = authState.value
      syncAdmissionStatus(data, read)
      authState.value = data?.auth_state || 'UNKNOWN'
      isConnected.value = Boolean(data?.is_connected)
      // 账号在运行中被登出 (AUTHENTICATED → UNAUTHENTICATED): 触发全局横幅引导重绑
      if (prevState === 'AUTHENTICATED' && data?.auth_state === 'UNAUTHENTICATED' && !sessionInvalidMessage.value) {
        reportSessionInvalid('检测到 Telegram 登录状态已丢失，请重新绑定账号')
      }
      if (typeof data?.premium === 'boolean') {
        isPremium.value = data.premium
      }
      if (typeof data?.upload_limit_mb === 'number') {
        uploadLimitMb.value = data.upload_limit_mb
      }
      if (typeof data?.startup_degraded === 'boolean') {
        startupDegraded.value = data.startup_degraded
      }
      startupStatus.value = data?.startup ?? null
      return true
    } catch (err) {
      if (!signal?.aborted && requestId === statusRequestId) console.error('获取 Telegram 状态失败:', err)
      return false
    }
  }

  // ==== Web 2FA 逻辑 ====
  async function sendWeb2FA(): Promise<boolean> {
    try {
      const res = await telegramAuthApi.sendWeb2FA()
      message.success(res.data?.message || '动态验证码已发送至 Telegram')
      return true
    } catch (err: unknown) {
      message.error(errDetail(err) || '发送 Web 2FA 验证码失败')
      return false
    }
  }

  async function verifyWeb2FA(code: string): Promise<boolean> {
    try {
      const res = await telegramAuthApi.verifyWeb2FA(code)
      persist2FAUnlock(res.data?.token || null)
      message.success('Web 2FA 验证成功，控制台已解锁！')
      return true
    } catch (err: unknown) {
      message.error(errDetail(err) || 'Web 2FA 验证失败')
      return false
    }
  }

  // ==== 原生初次登录 / 恢复挑战逻辑 ====
  async function nativeSendCode(phone: string | null = null): Promise<NativeSendCodeResponse | null> {
    try {
      const res = await telegramAuthApi.nativeSendCode(phone)
      const data = (res.data || null) as NativeSendCodeResponse | null
      activeChallengeId.value = data?.challenge_id || ''
      currentPhone.value = phone || ''
      message.success(data?.message || 'Telegram 官方验证码已发送')
      return data
    } catch (err: unknown) {
      message.error(errDetail(err) || '发送原生验证码失败')
      throw err
    }
  }

  async function nativeResendCode(challengeId: string): Promise<NativeSendCodeResponse | null> {
    try {
      const res = await telegramAuthApi.nativeResendCode(challengeId)
      const data = (res.data || null) as NativeSendCodeResponse | null
      if (data?.challenge_id) {
        activeChallengeId.value = data.challenge_id
      }
      message.success(data?.message || '验证码已重新下发')
      return data
    } catch (err: unknown) {
      message.error(errDetail(err) || '重发验证码失败')
      throw err
    }
  }

  function applyNativeAuthenticated(data: { token?: string | null; message?: string | null }): void {
    authState.value = 'AUTHENTICATED'
    sessionInvalidMessage.value = ''
    activeChallengeId.value = ''
    if (data.token) {
      persist2FAUnlock(data.token)
    } else if (!isRestricted.value && authStore.isAuthRequired) {
      persist2FAUnlock(null)
    }
    message.success(data.message || '原生 Telegram 授权成功！')
  }

  async function nativeVerifyCode(
    code: string,
    password: string | null = null,
    challengeId: string | null = null,
  ): Promise<NativeVerifyCodeResponse | null> {
    const cid = challengeId || activeChallengeId.value
    try {
      const res = await telegramAuthApi.nativeVerifyCode({
        challenge_id: cid,
        code: code || null,
        password,
      })
      const data = (res.data || null) as NativeVerifyCodeResponse | null
      if (data?.authenticated) {
        applyNativeAuthenticated(data)
      }
      return data
    } catch (err: unknown) {
      message.error(errDetail(err) || '原生验证码或 2FA 密码错误')
      throw err
    }
  }

  async function nativeSubmitPassword(challengeId: string, password: string): Promise<NativeVerifyCodeResponse | null> {
    try {
      const res = await telegramAuthApi.nativeSubmitPassword({ challenge_id: challengeId, password })
      const data = (res.data || null) as NativeVerifyCodeResponse | null
      if (data?.authenticated) {
        applyNativeAuthenticated(data)
      }
      return data
    } catch (err: unknown) {
      message.error(errDetail(err) || '2FA 两步验证密码错误')
      throw err
    }
  }

  async function nativeQrStart(payload: NativeQrStartRequest): Promise<NativeQrChallengeStartResponse | null> {
    try {
      const res = await telegramAuthApi.nativeQrStart(payload)
      const data = res.data || null
      activeChallengeId.value = data?.challenge_id || ''
      if (payload.phone) {
        currentPhone.value = payload.phone
      }
      return data
    } catch (err: unknown) {
      message.error(errDetail(err) || '生成登录二维码失败')
      throw err
    }
  }

  async function nativeQrStatus(challengeId: string): Promise<QrChallengeStatusResponse | null> {
    const res = await telegramAuthApi.nativeQrStatus(challengeId)
    const data = (res.data || null) as QrChallengeStatusResponse | null
    if (data?.authenticated) {
      applyNativeAuthenticated(data)
    }
    return data
  }

  async function nativeQrPassword(challengeId: string, password: string): Promise<NativeVerifyCodeResponse | null> {
    try {
      const res = await telegramAuthApi.nativeQrPassword({ challenge_id: challengeId, password })
      const data = (res.data || null) as NativeVerifyCodeResponse | null
      if (data?.authenticated) {
        applyNativeAuthenticated(data)
      }
      return data
    } catch (err: unknown) {
      message.error(errDetail(err) || '2FA 两步验证密码错误')
      throw err
    }
  }

  async function nativeCancelChallenge(challengeId: string): Promise<void> {
    if (!challengeId) return
    if (activeChallengeId.value === challengeId) {
      activeChallengeId.value = ''
    }
    try {
      await telegramAuthApi.nativeCancelChallenge(challengeId)
    } catch {
      // Best-effort cancellation on close/switch
    }
  }

  return {
    authState,
    isConnected,
    isModalVisible,
    isWeb2FAUnlocked,
    twoFAToken,
    isPremium,
    uploadLimitMb,
    startupDegraded,
    startupStatus,
    activeChallengeId,
    currentPhone,
    sessionInvalidMessage,
    admissionPhase,
    admissionError,
    consoleLocked,
    isRestricted,
    bindingMode,
    bootstrapTokenConfigured,
    bootstrapAuthorized,
    defaultPhoneConfigured,
    defaultPhoneValid,
    captureAdmissionRead,
    syncAdmissionStatus,
    enterRestrictedMode,
    enterLockedMode,
    checkAdmission,
    authenticateBootstrap,
    logoutBootstrap,
    reportSessionInvalid,
    dismissSessionInvalid,
    openRebindModal,
    fetchStatus,
    sendWeb2FA,
    verifyWeb2FA,
    nativeSendCode,
    nativeResendCode,
    nativeVerifyCode,
    nativeSubmitPassword,
    nativeQrStart,
    nativeQrStatus,
    nativeQrPassword,
    nativeCancelChallenge,
    clear2FAStorage,
    get2FAToken,
    handle403Error,
  }
})
