// 统一错误消息提取: api/client.ts 全局 throwOnError 后 reject 的是 AxiosError,
// 后端业务错误消息位于 response.data.detail; 兜底 err.message。
// Store 层 toast 文案的唯一取误入口。
export function errDetail(err: unknown, fallback = '未知错误'): string {
  const e = err as { response?: { data?: { detail?: unknown } }; message?: unknown } | null
  const detail = e?.response?.data?.detail
  if (typeof detail === 'string' && detail) return detail
  if (
    detail &&
    typeof detail === 'object' &&
    'message' in detail &&
    typeof detail.message === 'string' &&
    detail.message
  ) {
    return detail.message
  }
  if (Array.isArray(detail) && detail.length > 0) {
    const msgs = detail
      .map((item) => {
        if (typeof item === 'string') return item
        if (item && typeof item === 'object') {
          const msg = (item as { msg?: unknown }).msg
          const loc = (item as { loc?: unknown[] }).loc
          const field = Array.isArray(loc) && loc.length > 0 ? String(loc[loc.length - 1]) : ''
          if (typeof msg === 'string' && msg) return field ? `${field}: ${msg}` : msg
        }
        return ''
      })
      .filter(Boolean)
    if (msgs.length > 0) return msgs.join('; ')
  }
  if (typeof e?.message === 'string' && e.message) return e.message
  return fallback
}

export function errCode(err: unknown): string {
  const e = err as { response?: { data?: { error_code?: unknown } } } | null
  const code = e?.response?.data?.error_code
  return typeof code === 'string' ? code : ''
}

export function errRetryAfter(err: unknown): number | null {
  const e = err as { response?: { data?: { retry_after_seconds?: unknown } } } | null
  const sec = e?.response?.data?.retry_after_seconds
  return typeof sec === 'number' && Number.isFinite(sec) ? sec : null
}

/**
 * 判断是否为全局网关已接管的鉴权类错误 (401 API Key 缺失/失效 或 403 2FA 会话失效)。
 * 当全局拦截器 (client.ts) 触发对应模态弹窗后，业务 Store 应静默吸收此类错误，
 * 避免在认证弹窗背后同时弹出大量红色业务错误 Toast。
 *
 * 与 client.ts 的域契约对齐: pikpak 第三方域 (/api/pikpak/*) 的 401 被拦截器豁免
 * (isThirdPartyAuth, 不拉起 AuthModal 锁死主控制台)，因此该域的 401 不算"网关已接管"，
 * 必须保留给业务层自行呈现，此处返回 false。
 */
export function isAuthError(err: unknown): boolean {
  const e = err as {
    response?: { status?: number; data?: { detail?: unknown } }
    config?: { url?: string }
  } | null
  const status = e?.response?.status
  if (status === 401) {
    const url = e?.config?.url || ''
    if (!url.startsWith('/api/pikpak')) return true
  }
  if (status === 403) {
    const detail = e?.response?.data?.detail
    if (typeof detail === 'string' && detail.startsWith('2FA')) return true
  }
  return false
}
