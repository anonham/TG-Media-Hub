import { client } from './gen/client.gen'
import { createMediaTicketApiAuthMediaTicketPost } from './gen'
import { useAuthStore } from '@/stores/authStore'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'
import { message } from '@/utils/discreteApi'
import { errDetail } from '@/utils/errors'

// throwOnError 必须全局开启: gen 客户端默认把 AxiosError 作为正常值 resolve (不抛出),
// 会让全部调用方的 catch 分支静默失效; 开启后错误语义与原手写 axios 层一致。
// timeout 30000 对齐原 axios.create({ timeout: 30000 }), 长轮询请求按需在调用点覆盖。
client.setConfig({ timeout: 30000, throwOnError: true })

client.instance.interceptors.request.use((config) => {
  // 调用点显式携带 X-API-Key (如 authApi.verify 的候选口令探测) 时不得被默认凭证覆写
  if (!config.headers['X-API-Key']) {
    const authStore = useAuthStore()
    if (authStore.apiKey) {
      config.headers['X-API-Key'] = authStore.apiKey
    }
  }
  const tgAuthStore = useTelegramAuthStore()
  const tfaToken = tgAuthStore.get2FAToken()
  if (tfaToken) {
    config.headers['X-2FA-Token'] = tfaToken
  }
  return config
})

client.instance.interceptors.response.use(
  (response) => response,
  (error) => {
    const resp = error.response
    const url = error.config?.url || ''
    const respErrCode = (resp?.data as { error_code?: string } | undefined)?.error_code
    if (resp && resp.status === 503 && respErrCode === 'telegram_bootstrap_required') {
      const tgAuthStore = useTelegramAuthStore()
      tgAuthStore.enterRestrictedMode()
      return Promise.reject(error)
    }
    // 公网入口未配置 API_KEY —— 控制台整体锁定, 引导部署者完成部署配置
    if (resp && resp.status === 503 && respErrCode === 'console_locked') {
      const tgAuthStore = useTelegramAuthStore()
      tgAuthStore.enterLockedMode()
      return Promise.reject(error)
    }
    // 只有主系统核心鉴权/配置路由产生的 401 才触发控制台口令重新录入
    // 第三方集成域（如 /api/pikpak/*）、引导认证与 Telegram 登录挑战错误不得拉起 API Key 弹窗
    const isThirdPartyAuth = url.startsWith('/api/pikpak')
    const isBootstrapRoute =
      Boolean(respErrCode?.startsWith('bootstrap_')) || url.startsWith('/api/telegram/bootstrap/')
    const isTelegramChallengeError =
      Boolean(respErrCode) &&
      respErrCode !== 'api_key_invalid' &&
      (url.startsWith('/api/telegram/') || url.startsWith('/api/accounts/'))
    if (resp && resp.status === 401 && !isThirdPartyAuth && !isBootstrapRoute && !isTelegramChallengeError) {
      // API Key 失效: 引导重输访问口令
      const authStore = useAuthStore()
      authStore.handle401Error()
    } else if (resp && resp.status === 403 && String(resp.data?.detail || '').startsWith('2FA')) {
      // 2FA 会话失效 (detail 以 "2FA" 开头为后端契约): 引导重验
      const tgAuthStore = useTelegramAuthStore()
      tgAuthStore.handle403Error(resp.data.detail)
    }
    return Promise.reject(error)
  },
)

export { client }

// ── 请求卫生基建 ─────────────────────────────────────────
// latest-wins 竞态守卫: 列表类请求发起前 next() 取号, 响应抵达后 isCurrent(token)
// 判定 —— 过期说明已有更新的同类请求在途, 旧响应必须丢弃 (起接线
// storageStore.loadFiles 等易竞态入口)。
export interface RequestSequenceGuard {
  next(): number
  isCurrent(token: number): boolean
}

export function createRequestSequence(): RequestSequenceGuard {
  let seq = 0
  return {
    next: () => ++seq,
    isCurrent: (token: number) => token === seq,
  }
}

// ── 媒体流 ticket ─────────────────────────────
// 明文 Key/2FA 已从一切 URL 收口: 浏览器原生 <video>/<a> 标签无法携带自定义请求头,
// 改为"先取短时效 ticket、再拼 URL"的异步构造器; ticket TTL 内可重复消费, 兼容
// <video> 以同一 URL 发起多个 206 Range 分段请求。
export async function fetchMediaTicket(): Promise<string> {
  try {
    const res = await createMediaTicketApiAuthMediaTicketPost()
    const ticket = res.data?.ticket
    if (!ticket) {
      throw new Error('媒体 ticket 签发响应缺少 ticket 字段')
    }
    return ticket
  } catch (err) {
    // 硬规则 5: 取票失败必须 toast 并中止动作, 严禁静默回退明文凭证 URL
    const detail = errDetail(err)
    message.error(`媒体流鉴权取票失败: ${detail}`)
    throw err
  }
}

function buildMediaUrl(base: string, filePath: string, ticket: string): string {
  return `${base}/${encodeURIComponent(filePath).replace(/%2F/g, '/')}?ticket=${encodeURIComponent(ticket)}`
}

export async function getStoragePreviewUrl(filePath: string): Promise<string> {
  return buildMediaUrl('/api/storage/preview', filePath, await fetchMediaTicket())
}

export async function getStorageDownloadUrl(filePath: string): Promise<string> {
  return buildMediaUrl('/api/storage/download', filePath, await fetchMediaTicket())
}
