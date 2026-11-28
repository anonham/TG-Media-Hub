// PikPak 上游信道健康度判定与风控自愈模型 (Milestone 4: 红绿灯与交互闭环)
// 契约基准: docs/architecture/STREAMING_AND_SPLITTING.md §1 (PikPak 上游信道状态与熔断模型)

export type ChannelHealthStatus =
  | 'direct' // 🟢 信道正常 (Direct Stream)
  | 'downshifted' // 🔵 降档稳态自愈中 (Auto-Downshifted)
  | 'cooldown' // 🟡 阶梯静默冷却中 (Tier 1 / Tier 2 Cooldown)
  | 'safe_paused' // 🟠 安全挂起保护 (Tier 3 Safe-Paused)
  | 'auth_required' // 🔴 凭据失效 (Auth Required)
  | 'idle' // ⚪ 非 PikPak / 空闲

export interface ChannelHealthInfo {
  status: ChannelHealthStatus
  tier?: 1 | 2 | 3
  remainingSeconds: number
  label: string
  badgeText: string
  badgeType: 'success' | 'info' | 'warning' | 'error' | 'default'
  dotClass: string
  icon: string
  description: string
  isCooldown: boolean
  isAutoDownshifted: boolean
  isSafePaused: boolean
  isAuthRequired: boolean
  throttleStrikes: number
  dataRiskStrikes: number
  deliveredPartsCount: number
}

/**
 * 将冷却剩余秒数格式化为可读倒计时 (如 "14分22秒" 或 "45秒")
 */
export function formatCooldownCountdown(seconds: number): string {
  const s = Math.max(0, Math.ceil(seconds))
  if (s <= 0) return '0秒'
  if (s < 60) return `${s}秒`
  const mins = Math.floor(s / 60)
  const remSec = s % 60
  if (mins < 60) {
    return remSec > 0 ? `${mins}分${remSec}秒` : `${mins}分钟`
  }
  const hours = Math.floor(mins / 60)
  const remMin = mins % 60
  return remMin > 0 ? `${hours}小时${remMin}分` : `${hours}小时`
}

export interface TaskLikeParams {
  throttle_strike_count?: number
  throttle_strikes?: number
  data_risk_strike_count?: number
  data_risk_strikes?: number
  auto_downshifted?: boolean
  pikpak_part_checkpoints?: Record<string, { delivered?: Record<string, unknown> }>
  safe_paused?: boolean
  cooldown_tier?: number
  [key: string]: unknown
}

export interface TaskLike {
  task_type?: string
  status?: string
  params?: TaskLikeParams | Record<string, unknown> | null
  next_retry_at?: number | null
  error_detail?: string | null
  cooldown_tier?: number | null
  [key: string]: unknown
}

/**
 * 判定任务的 PikPak 上游信道健康状态
 */
export function getPikPakChannelHealth(task: TaskLike | null | undefined, nowSeconds?: number): ChannelHealthInfo {
  const defaultIdle: ChannelHealthInfo = {
    status: 'idle',
    remainingSeconds: 0,
    label: '空闲',
    badgeText: '空闲',
    badgeType: 'default',
    dotClass: 'dot-idle',
    icon: '⚪',
    description: '',
    isCooldown: false,
    isAutoDownshifted: false,
    isSafePaused: false,
    isAuthRequired: false,
    throttleStrikes: 0,
    dataRiskStrikes: 0,
    deliveredPartsCount: 0,
  }

  if (!task || task.task_type !== 'pikpak_publish') {
    return defaultIdle
  }

  const params = (task.params && typeof task.params === 'object' ? task.params : {}) as TaskLikeParams
  const throttleStrikes = Number(params.throttle_strike_count ?? params.throttle_strikes ?? 0)
  const dataRiskStrikes = Number(params.data_risk_strike_count ?? params.data_risk_strikes ?? 0)
  const isAutoDownshifted = Boolean(params.auto_downshifted)

  // 统计多分卷已交付保全消息数
  let deliveredPartsCount = 0
  const checkpoints = params.pikpak_part_checkpoints
  if (checkpoints && typeof checkpoints === 'object') {
    for (const fileKey of Object.keys(checkpoints)) {
      const entry = checkpoints[fileKey]
      if (entry && typeof entry === 'object' && entry.delivered && typeof entry.delivered === 'object') {
        deliveredPartsCount += Object.keys(entry.delivered).length
      }
    }
  }

  const now = nowSeconds !== undefined ? nowSeconds : Date.now() / 1000
  const nextRetryAt = Number(task.next_retry_at || 0)
  const isCooldown = (task.status === 'queued' || task.status === 'pending') && nextRetryAt > now
  const remainingSeconds = isCooldown ? Math.max(0, Math.ceil(nextRetryAt - now)) : 0

  const errDetail = String(task.error_detail || '')

  // 1. 🔴 凭据失效判定
  const isAuthRequired =
    task.status === 'failed' &&
    (errDetail.includes('凭据') ||
      errDetail.includes('Auth') ||
      errDetail.includes('TOKEN_EXPIRED') ||
      errDetail.includes('鉴权') ||
      errDetail.includes('重新授权') ||
      errDetail.includes('登录') ||
      errDetail.includes('401') ||
      errDetail.toLowerCase().includes('unauthenticated'))

  if (isAuthRequired) {
    return {
      status: 'auth_required',
      remainingSeconds: 0,
      label: '凭据失效 (Auth Required)',
      badgeText: '🔴 凭据失效',
      badgeType: 'error',
      dotClass: 'dot-error',
      icon: '🔴',
      description: 'PikPak 登录令牌或凭据已失效，需重新完成设备码授权',
      isCooldown: false,
      isAutoDownshifted,
      isSafePaused: false,
      isAuthRequired: true,
      throttleStrikes,
      dataRiskStrikes,
      deliveredPartsCount,
    }
  }

  // 2. 🟠 安全挂起保护判定 (Tier 3 Safe-Paused)
  const isSafePaused =
    task.status === 'paused' &&
    (params.safe_paused === true ||
      errDetail.includes('风控安全挂起') ||
      errDetail.includes('Safe-Pause') ||
      errDetail.includes('控制面风控拦截') ||
      errDetail.includes('上游频控拦截已达') ||
      throttleStrikes >= 3 ||
      dataRiskStrikes >= 2 ||
      throttleStrikes + dataRiskStrikes >= 3)

  if (isSafePaused) {
    const desc = errDetail || '连续遭遇上游风控或需人工验证，系统已主动安全暂停以保护账号与 IP (断点与已发分卷均已保全)'
    return {
      status: 'safe_paused',
      tier: 3,
      remainingSeconds: 0,
      label: '安全挂起保护 (Safe-Paused)',
      badgeText: '🟠 安全挂起',
      badgeType: 'warning',
      dotClass: 'dot-safe-paused',
      icon: '🟠',
      description: desc,
      isCooldown: false,
      isAutoDownshifted,
      isSafePaused: true,
      isAuthRequired: false,
      throttleStrikes,
      dataRiskStrikes,
      deliveredPartsCount,
    }
  }

  // 3. 🟡 阶梯静默冷却判定 (Tier 1 / Tier 2 Cooldown)
  if (isCooldown) {
    const tier: 1 | 2 =
      task.cooldown_tier === 2 ||
      params.cooldown_tier === 2 ||
      dataRiskStrikes >= 1 ||
      throttleStrikes >= 2 ||
      errDetail.includes('Tier 2')
        ? 2
        : 1
    const countdownStr = formatCooldownCountdown(remainingSeconds)
    const label =
      tier === 2
        ? `Tier 2 深度冷却中: ${countdownStr} 后自动换链稳态续传`
        : `Tier 1 浅度冷却中: ${countdownStr} 后自动断点续传`
    const badgeText = `⏳ Tier ${tier} 冷却 (${countdownStr})`
    const description =
      tier === 2
        ? `CDN 频控/拦截加剧，处于 Tier 2 深度冷却中 (${countdownStr})，期满后强制换链跨 PoP 漂移并以保守稳态续传`
        : `CDN 边缘频控保护中 (${countdownStr})，期满后自动原位断点续传`

    return {
      status: 'cooldown',
      tier,
      remainingSeconds,
      label,
      badgeText,
      badgeType: 'warning',
      dotClass: 'dot-warning',
      icon: '🟡',
      description,
      isCooldown: true,
      isAutoDownshifted,
      isSafePaused: false,
      isAuthRequired: false,
      throttleStrikes,
      dataRiskStrikes,
      deliveredPartsCount,
    }
  }

  // 4. 🔵 降档稳态自愈中判定 (Auto-Downshifted)
  if (isAutoDownshifted && (task.status === 'running' || task.status === 'queued')) {
    return {
      status: 'downshifted',
      tier: 2,
      remainingSeconds: 0,
      label: '稳态自愈降档 (Auto-Downshifted)',
      badgeText: '🛡️ 稳态降档',
      badgeType: 'info',
      dotClass: 'dot-downshifted',
      icon: '🔵',
      description: '系统已在无人值守下自动收敛为单预取通道/单上传并发/20s发送延迟的保守稳态续传',
      isCooldown: false,
      isAutoDownshifted: true,
      isSafePaused: false,
      isAuthRequired: false,
      throttleStrikes,
      dataRiskStrikes,
      deliveredPartsCount,
    }
  }

  // 5. 🟢 信道正常直连传输
  if (task.status === 'running') {
    return {
      status: 'direct',
      remainingSeconds: 0,
      label: '信道正常 (Direct Stream)',
      badgeText: '🟢 直连传输',
      badgeType: 'success',
      dotClass: 'dot-direct',
      icon: '🟢',
      description: 'PikPak 边缘节点直连通道顺畅，全速流式发布中',
      isCooldown: false,
      isAutoDownshifted: false,
      isSafePaused: false,
      isAuthRequired: false,
      throttleStrikes,
      dataRiskStrikes,
      deliveredPartsCount,
    }
  }

  return defaultIdle
}
