// Userbot 账号与 Bot 令牌池域 Store。
// userbotSelectOptions 为全站唯一下拉定义: 四个创建视图统一消费,
// 含 "系统自动调度" 默认项 (value=null) —— 修复 PikPakPublishView 漏掉该项的历史偏差。
import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import type {
  AccountCreateRequest,
  AccountResponse,
  AccountSendCodeResponse,
  AccountUpdateRequest,
  AccountVerifyCodeRequest,
  AccountVerifyCodeResponse,
  BotCreateRequest,
  BotPingResult,
  BotResponse,
  QrChallengeStartResponse,
  QrChallengeStatusResponse,
} from '@/api/gen'
import type { SelectOption } from 'naive-ui'
import { accountsApi, botsApi } from '@/api'
import { message } from '@/utils/discreteApi'
import { errDetail, isAuthError } from '@/utils/errors'
import { useTelegramAuthStore } from './telegramAuthStore'

// Userbot 执行账号下拉选项: value='' 表示系统自动调度 (默认负载均衡)
export type UserbotSelectOption = SelectOption

export const useAccountStore = defineStore('account', () => {
  const accounts = ref<AccountResponse[]>([])
  const bots = ref<BotResponse[]>([])
  const loadingAccounts = ref(false)
  const loadingBots = ref(false)
  const pingingBots = ref(false)
  const pingResults = ref<BotPingResult[]>([])
  let botListRequestId = 0

  // 计算属性
  const activeAccounts = computed(() => {
    return accounts.value.filter(
      (a) => a.is_active && a.status === 'authenticated' && (a.flood_wait_remaining_seconds || 0) <= 0,
    )
  })

  const restrictedAccounts = computed(() => {
    return accounts.value.filter(
      (a) => !a.is_active || a.status !== 'authenticated' || (a.flood_wait_remaining_seconds || 0) > 0,
    )
  })

  const restrictedCount = computed(() => {
    return restrictedAccounts.value.length
  })

  const totalConcurrency = computed(() => {
    return activeAccounts.value.reduce((sum, a) => sum + (a.max_concurrency || 1), 0)
  })

  function isBotOnline(bot: BotResponse): boolean {
    return bot.is_active && ['ready', 'authenticated', 'online'].includes(bot.status ?? '')
  }

  const onlineBotsCount = computed(() => bots.value.filter(isBotOnline).length)

  // 账号纯净消歧代号映射 (无手机号、同名自动追加 #ID 消歧)
  const accountDisplayNameMap = computed(() => {
    const map = new Map<number, string>()
    const nameCounts: Record<string, number> = {}
    for (const a of accounts.value) {
      const base = (a.account_name || '').trim() || `Userbot #${a.id}`
      nameCounts[base] = (nameCounts[base] || 0) + 1
    }
    for (const a of accounts.value) {
      const base = (a.account_name || '').trim() || `Userbot #${a.id}`
      if (nameCounts[base] > 1) {
        map.set(a.id, `${base} #${a.id}`)
      } else {
        map.set(a.id, base)
      }
    }
    return map
  })

  // 统一 Userbot 下拉选项: 首项恒为系统自动调度, 后接全部活跃账号
  const userbotSelectOptions = computed<UserbotSelectOption[]>(() => {
    const options: UserbotSelectOption[] = [{ label: '🤖 系统自动调度 (默认负载均衡)', value: '' }]
    for (const a of accounts.value) {
      if (a.is_active) {
        options.push({
          label: `${a.account_name} (${a.phone || 'ID:' + a.id})`,
          value: a.id,
        })
      }
    }
    return options
  })

  function getAccountDisplayName(accountId: number | null | undefined): string {
    if (!accountId) return '自动调度'
    if (accountDisplayNameMap.value.has(accountId)) {
      return accountDisplayNameMap.value.get(accountId) as string
    }
    return `ID: ${accountId}`
  }

  // Userbot Actions
  let loadAccountsInFlight: Promise<void> | null = null
  let loadAccountsGen = 0
  async function loadAccounts(): Promise<void> {
    // in-flight 去重锁: 并发调用共享同一 Promise, 防多视图同帧重复请求
    if (loadAccountsInFlight) return loadAccountsInFlight
    const gen = ++loadAccountsGen
    loadingAccounts.value = true
    let flight: Promise<void> | null = null
    flight = (async () => {
      try {
        const res = await accountsApi.list()
        // 变更后刷新已换代 —— 丢弃变更前的陈旧快照,
        // 防删除/创建/更新的刷新复用在途请求而展示旧列表
        if (gen !== loadAccountsGen) return
        accounts.value = res.data || []
      } catch (err) {
        if (gen !== loadAccountsGen) return
        console.error('加载 Userbot 账号列表失败:', err)
        if (!isAuthError(err)) {
          message.error(`加载 Userbot 失败: ${errDetail(err)}`)
        }
      } finally {
        if (gen === loadAccountsGen) loadingAccounts.value = false
        if (loadAccountsInFlight === flight) loadAccountsInFlight = null
      }
    })()
    loadAccountsInFlight = flight
    return flight
  }

  async function refreshAccountsAfterMutation(): Promise<void> {
    // 变更后强制新读取 —— 先失效在途去重锁, 旧响应再经代际守卫丢弃
    loadAccountsInFlight = null
    await loadAccounts()
  }

  async function createAccount(payload: AccountCreateRequest): Promise<AccountResponse> {
    try {
      const res = await accountsApi.create(payload)
      const data = res.data as AccountResponse
      message.success(`Userbot 账号 "${data.account_name}" 创建成功！`)
      await refreshAccountsAfterMutation()
      return data
    } catch (err) {
      message.error(`创建 Userbot 失败: ${errDetail(err)}`)
      throw err
    }
  }

  async function sendAccountCode(
    accountId: number,
    phone: string | null = null,
  ): Promise<AccountSendCodeResponse | null> {
    try {
      const payload = phone ? { phone } : {}
      const res = await accountsApi.sendCode(accountId, payload)
      const data = (res.data || null) as AccountSendCodeResponse | null
      message.success(data?.message || '验证码已下发至您的 Telegram 客户端')
      return data
    } catch (err) {
      message.error(`发送验证码失败: ${errDetail(err)}`)
      throw err
    }
  }

  async function resendAccountCode(accountId: number, challengeId: string): Promise<AccountSendCodeResponse | null> {
    try {
      const res = await accountsApi.resendCode(accountId, { challenge_id: challengeId })
      const data = (res.data || null) as AccountSendCodeResponse | null
      message.success(data?.message || '验证码已重新下发')
      return data
    } catch (err) {
      message.error(`重发验证码失败: ${errDetail(err)}`)
      throw err
    }
  }

  async function verifyAccountCode(
    accountId: number,
    payload: AccountVerifyCodeRequest,
  ): Promise<AccountVerifyCodeResponse | null> {
    try {
      const res = await accountsApi.verifyCode(accountId, payload)
      const data = (res.data || null) as AccountVerifyCodeResponse | null
      if (data?.authenticated) {
        message.success(data.message || '账号授权成功！')
        await refreshAccountsAfterMutation()
      }
      return data
    } catch (err) {
      message.error(`验证失败: ${errDetail(err)}`)
      throw err
    }
  }

  async function submitAccountPassword(
    accountId: number,
    challengeId: string,
    password: string,
  ): Promise<AccountVerifyCodeResponse | null> {
    try {
      const res = await accountsApi.submitPassword(accountId, { challenge_id: challengeId, password })
      const data = (res.data || null) as AccountVerifyCodeResponse | null
      if (data?.authenticated) {
        message.success(data.message || '账号授权成功！')
        await refreshAccountsAfterMutation()
      }
      return data
    } catch (err) {
      message.error(`2FA 密码校验失败: ${errDetail(err)}`)
      throw err
    }
  }

  async function startAccountQr(accountId: number): Promise<QrChallengeStartResponse | null> {
    try {
      const res = await accountsApi.startQr(accountId)
      return (res.data || null) as QrChallengeStartResponse | null
    } catch (err) {
      message.error(`生成登录二维码失败: ${errDetail(err)}`)
      throw err
    }
  }

  async function getAccountQrStatus(accountId: number, challengeId: string): Promise<QrChallengeStatusResponse | null> {
    const res = await accountsApi.getQrStatus(accountId, challengeId)
    const data = (res.data || null) as QrChallengeStatusResponse | null
    if (data?.authenticated) {
      message.success(data.message || '账号扫码授权成功！')
      await refreshAccountsAfterMutation()
    }
    return data
  }

  async function submitAccountQrPassword(
    accountId: number,
    challengeId: string,
    password: string,
  ): Promise<AccountVerifyCodeResponse | null> {
    try {
      const res = await accountsApi.submitQrPassword(accountId, { challenge_id: challengeId, password })
      const data = (res.data || null) as AccountVerifyCodeResponse | null
      if (data?.authenticated) {
        message.success(data.message || '账号扫码授权成功！')
        await refreshAccountsAfterMutation()
      }
      return data
    } catch (err) {
      message.error(`2FA 密码校验失败: ${errDetail(err)}`)
      throw err
    }
  }

  async function cancelAccountChallenge(accountId: number, challengeId: string): Promise<void> {
    if (!challengeId) return
    try {
      await accountsApi.cancelChallenge(accountId, { challenge_id: challengeId })
    } catch {
      // Best-effort cancellation on modal close/switch
    }
  }

  async function refreshAccount(accountId: number): Promise<unknown> {
    try {
      const res = await accountsApi.refresh(accountId)
      await refreshAccountsAfterMutation()
      const state = res.data?.auth_state
      if (state === 'AUTHENTICATED') {
        message.success('账号在线就绪，Premium 状态已刷新')
      } else if (state === 'FLOOD_WAIT') {
        message.warning('账号正在频控冷却，请等待冷却结束')
      } else if (state === 'INVALID') {
        message.warning('账号会话失效，请使用绑定手机号重新授权')
      } else {
        message.warning('账号暂未就绪，请稍后再次探活')
      }
      return res.data
    } catch (err) {
      message.error(`探测失败: ${errDetail(err)}`)
      throw err
    }
  }

  async function updateAccount(accountId: number, payload: AccountUpdateRequest): Promise<unknown> {
    try {
      const res = await accountsApi.update(accountId, payload)
      message.success('账号配置更新成功！')
      await refreshAccountsAfterMutation()
      return res.data
    } catch (err) {
      message.error(`更新账号失败: ${errDetail(err)}`)
      throw err
    }
  }

  async function deleteAccount(accountId: number, deleteSessionFile = false): Promise<boolean> {
    try {
      await accountsApi.delete(accountId, deleteSessionFile)
      message.info('账号已移出')
      await refreshAccountsAfterMutation()
      return true
    } catch (err) {
      message.error(`删除账号失败: ${errDetail(err)}`)
      throw err
    }
  }

  // Bot Token Pool Actions
  async function loadBots(): Promise<void> {
    const requestId = ++botListRequestId
    loadingBots.value = true
    try {
      const res = await botsApi.list()
      if (requestId === botListRequestId) bots.value = res.data || []
    } catch (err) {
      if (requestId !== botListRequestId) return
      console.error('加载 Bot 令牌池失败:', err)
      if (!isAuthError(err)) {
        message.error(`加载 Bot 池失败: ${errDetail(err)}`)
      }
    } finally {
      if (requestId === botListRequestId) loadingBots.value = false
    }
  }

  async function createBot(payload: BotCreateRequest): Promise<unknown> {
    try {
      const res = await botsApi.create(payload)
      const data = res.data as { bot_name?: string; bot_username?: string }
      message.success(`Bot "${data?.bot_name || data?.bot_username}" 加入令牌池成功！`)
      await loadBots()
      return data
    } catch (err) {
      message.error(`添加 Bot 失败: ${errDetail(err)}`)
      throw err
    }
  }

  async function pingBots(): Promise<unknown> {
    pingingBots.value = true
    try {
      const res = await botsApi.ping()
      const data = res.data as { results?: BotPingResult[]; online_count?: number; total?: number }
      pingResults.value = data?.results || []
      message.success(`Bot 健康探活完成: ${data?.online_count ?? 0}/${data?.total ?? 0} 在线`)
      await Promise.all([loadBots(), useTelegramAuthStore().fetchStatus(false)])
      return data
    } catch (err) {
      message.error(`Bot 探活异常: ${errDetail(err)}`)
      throw err
    } finally {
      pingingBots.value = false
    }
  }

  async function deleteBot(botId: number): Promise<boolean> {
    try {
      await botsApi.delete(botId)
      message.info('Bot 已移出令牌池')
      await loadBots()
      return true
    } catch (err) {
      message.error(`删除 Bot 失败: ${errDetail(err)}`)
      throw err
    }
  }

  return {
    accounts,
    bots,
    loadingAccounts,
    loadingBots,
    pingingBots,
    pingResults,
    activeAccounts,
    restrictedAccounts,
    restrictedCount,
    totalConcurrency,
    onlineBotsCount,
    isBotOnline,
    accountDisplayNameMap,
    userbotSelectOptions,
    getAccountDisplayName,
    loadAccounts,
    createAccount,
    sendAccountCode,
    resendAccountCode,
    verifyAccountCode,
    submitAccountPassword,
    startAccountQr,
    getAccountQrStatus,
    submitAccountQrPassword,
    cancelAccountChallenge,
    refreshAccount,
    updateAccount,
    deleteAccount,
    loadBots,
    createBot,
    pingBots,
    deleteBot,
  }
})
