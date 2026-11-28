import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { authApi } from '@/api'
import { message } from '@/utils/discreteApi'
import { useTelegramAuthStore } from './telegramAuthStore'

export const useAuthStore = defineStore('auth', () => {
  // 混合存储策略：优先检查带过期时间的 localStorage，再检查 sessionStorage
  const savedKey = typeof localStorage !== 'undefined' ? localStorage.getItem('tg_hub_api_key_remember') : null
  let initialKey = ''
  let initialRemember = false
  if (savedKey) {
    try {
      const data = JSON.parse(savedKey)
      if (data.expires > Date.now()) {
        initialKey = data.key
        initialRemember = true
      } else {
        localStorage.removeItem('tg_hub_api_key_remember')
      }
    } catch {
      localStorage.removeItem('tg_hub_api_key_remember')
    }
  }
  if (!initialKey && typeof sessionStorage !== 'undefined') {
    initialKey = sessionStorage.getItem('tg_hub_api_key_session') || ''
  }

  const apiKey = ref<string>(initialKey)
  const isKeyVerified = ref<boolean>(false)
  const isAuthRequired = ref<boolean>(false)
  const isAuthChecked = ref<boolean>(false)
  const isModalVisible = ref<boolean>(false)
  const modalMessage = ref<string>('')
  const rememberMe = ref<boolean>(initialRemember)

  // 防暴力破解状态
  const failedAttempts = ref<number>(0)
  const lockCountdown = ref<number>(0)
  let timerId: ReturnType<typeof setInterval> | null = null
  let credentialGeneration = 0

  const isLocked = computed<boolean>(() => lockCountdown.value > 0)

  function startLockdown(): void {
    lockCountdown.value = 30
    message.error('连续 3 次密码验证失败，已触发防爆破锁定 30 秒！')
    if (timerId) clearInterval(timerId)
    timerId = setInterval(() => {
      lockCountdown.value--
      if (lockCountdown.value <= 0) {
        if (timerId) clearInterval(timerId)
        timerId = null
        failedAttempts.value = 0
      }
    }, 1000)
  }

  async function checkAuthStatus(): Promise<boolean> {
    const tgAuthStore = useTelegramAuthStore()
    const read = tgAuthStore.captureAdmissionRead()
    const credGen = credentialGeneration
    try {
      const res = await authApi.getStatus()
      if (!tgAuthStore.isAdmissionReadCurrent(read) || credGen !== credentialGeneration) return false
      tgAuthStore.syncAdmissionStatus(res.data, read)
      isAuthRequired.value = Boolean(res.data?.auth_required)

      if (res.data?.restricted) {
        isKeyVerified.value = false
        isModalVisible.value = false
        isAuthChecked.value = false
        return false
      }

      if (!isAuthRequired.value) {
        isKeyVerified.value = false
        isModalVisible.value = false
        isAuthChecked.value = true
        return true
      }

      isAuthChecked.value = true
      if (apiKey.value) {
        const verifyOutcome = await verifyKey(apiKey.value)
        if (!tgAuthStore.isAdmissionReadCurrent(read) || credGen !== credentialGeneration) return false
        if (verifyOutcome === 'valid') {
          isKeyVerified.value = true
          isModalVisible.value = false
          return true
        } else {
          isKeyVerified.value = false
          openAuthModal(
            verifyOutcome === 'invalid' ? '保存的 API Key 已失效，请重新验证' : '鉴权服务暂时不可达，请稍后重试',
          )
          return false
        }
      } else {
        isKeyVerified.value = false
        openAuthModal('服务已开启身份验证，请输入 API Key 进入控制台')
        return false
      }
    } catch (err) {
      if (!tgAuthStore.isAdmissionReadCurrent(read) || credGen !== credentialGeneration) return false
      console.error('检查鉴权状态失败:', err)
      isAuthChecked.value = true
      return false
    }
  }

  async function verifyKey(key: string): Promise<'valid' | 'invalid' | 'error'> {
    try {
      const res = await authApi.verify(key)
      return res.status === 200 ? 'valid' : 'invalid'
    } catch (err: unknown) {
      const status = (err as { response?: { status?: number } } | null)?.response?.status
      return status === 401 || status === 403 ? 'invalid' : 'error'
    }
  }

  async function submitAuthKey(key: string, remember: boolean = false): Promise<boolean> {
    if (isLocked.value) return false
    const cleanKey = key.trim()
    if (!cleanKey) {
      message.warning('请输入有效的访问口令 (API Key)')
      return false
    }

    const outcome = await verifyKey(cleanKey)
    if (outcome === 'valid') {
      credentialGeneration += 1
      apiKey.value = cleanKey
      isKeyVerified.value = true
      rememberMe.value = remember

      if (remember) {
        localStorage.setItem(
          'tg_hub_api_key_remember',
          JSON.stringify({
            key: cleanKey,
            expires: Date.now() + 24 * 60 * 60 * 1000, // 24小时有效
          }),
        )
        sessionStorage.removeItem('tg_hub_api_key_session')
      } else {
        sessionStorage.setItem('tg_hub_api_key_session', cleanKey)
        localStorage.removeItem('tg_hub_api_key_remember')
      }

      failedAttempts.value = 0
      isModalVisible.value = false
      message.success('API Key 身份验证成功！')
      return true
    } else if (outcome === 'invalid') {
      failedAttempts.value++
      if (failedAttempts.value >= 3) {
        startLockdown()
      } else {
        message.error(`API Key 验证失败 (${failedAttempts.value}/3)`)
      }
      return false
    } else {
      message.error('无法连接鉴权服务，请检查网络或服务状态后重试')
      return false
    }
  }

  function clearAuthKey(): void {
    credentialGeneration += 1
    apiKey.value = ''
    isKeyVerified.value = false
    rememberMe.value = false
    localStorage.removeItem('tg_hub_api_key_remember')
    sessionStorage.removeItem('tg_hub_api_key_session')
    localStorage.removeItem('tg_hub_2fa_unlocked')
    if (typeof window !== 'undefined' && 'BroadcastChannel' in window) {
      try {
        const channel = new BroadcastChannel('tg_hub_auth_channel')
        channel.postMessage({ type: 'AUTH_CLEARED' })
        channel.close()
      } catch (err) {
        console.debug('BroadcastChannel 广播异常降级:', err)
      }
    }
    openAuthModal('已清除保存的凭证，请输入新 API Key')
  }

  function openAuthModal(msg: string = ''): void {
    modalMessage.value = msg
    isModalVisible.value = true
  }

  function handle401Error(): void {
    isKeyVerified.value = false
    if (isModalVisible.value) return // 已有弹窗打开则不再叠加
    if (isAuthRequired.value) {
      openAuthModal('访问口令失效或未经授权，请重新输入')
      message.error('401 未授权访问：请检查 API Key 凭证')
    }
  }

  function getCredentialGeneration(): number {
    return credentialGeneration
  }

  return {
    apiKey,
    isKeyVerified,
    isAuthRequired,
    isAuthChecked,
    isModalVisible,
    modalMessage,
    rememberMe,
    failedAttempts,
    lockCountdown,
    isLocked,
    checkAuthStatus,
    submitAuthKey,
    clearAuthKey,
    openAuthModal,
    handle401Error,
    getCredentialGeneration,
  }
})
