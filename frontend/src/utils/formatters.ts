// 公共格式化工具: 全站唯一字节/时长/DC/时间戳格式化出口,
// 各视图手写的 5 套时间戳格式化与 PikPakPublishView 内置 formatBytes/formatSeconds 均已删除归此。

export function formatDcLocation(dcId: number | string | null | undefined, short = false): string {
  if (!dcId || dcId === 0) return '-'
  if (short) {
    const shortMap: Record<number, string> = {
      1: 'DC 1 (US)',
      2: 'DC 2 (EU)',
      3: 'DC 3 (US)',
      4: 'DC 4 (EU)',
      5: 'DC 5 (SG)',
    }
    return shortMap[Number(dcId)] || `DC ${dcId}`
  }
  const dcMap: Record<number, string> = {
    1: 'DC 1 (US West)',
    2: 'DC 2 (Europe)',
    3: 'DC 3 (US East)',
    4: 'DC 4 (Europe)',
    5: 'DC 5 (Singapore)',
  }
  return dcMap[Number(dcId)] || `DC ${dcId}`
}

/**
 * 格式化字节大小为易读文本 (基于 1024 二进制阶梯，采用行业通用 KB/MB/GB/TB 标识)。
 */
export function formatBytes(bytes: number | null | undefined, decimals = 2): string {
  if (!bytes || bytes <= 0 || !Number.isFinite(bytes)) return '0 B'
  const k = 1024
  const dm = decimals < 0 ? 0 : decimals
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.min(sizes.length - 1, Math.max(0, Math.floor(Math.log(bytes) / Math.log(k))))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i]
}

export function formatSeconds(seconds: number | null | undefined): string {
  if (!seconds || seconds <= 0) return '0s'
  const secs = Math.ceil(seconds)
  if (secs < 60) return `${secs}s`
  const mins = Math.floor(secs / 60)
  const remSecs = secs % 60
  if (mins < 60) return `${mins}m ${remSecs}s`
  const hours = Math.floor(mins / 60)
  const remMins = mins % 60
  return `${hours}h ${remMins}m ${remSecs}s`
}

// Intl.DurationFormat 实例缓存: ETA 逐秒刷新 × 任务行数渲染, 避免每次调用重建 formatter;
// 非法 locale 等构造异常同样记忆 (null), 后续调用直接走回退不再重复抛错
const durationFormatterCache = new Map<string, any>()

function getDurationFormatter(locale: string, style: 'long' | 'short' | 'narrow' | 'digital'): any | null {
  const cacheKey = `${locale}|${style}`
  if (durationFormatterCache.has(cacheKey)) {
    return durationFormatterCache.get(cacheKey)
  }
  let formatter: any = null
  try {
    if (typeof Intl !== 'undefined' && 'DurationFormat' in Intl) {
      formatter = new (Intl as any).DurationFormat(locale, { style })
    }
  } catch {
    formatter = null
  }
  durationFormatterCache.set(cacheKey, formatter)
  return formatter
}

/**
 * 现代标准时长本地化格式化器 (基于 Intl.DurationFormat 渐进增强, Baseline 2025-03)
 * 当浏览器环境支持 Intl.DurationFormat 时输出自然语言格式 (如 "2小时15分钟9秒",
 * style='digital' 输出 "2:15:09" 适配等宽窄列), 未支持或异常时自动平滑回退至
 * formatSeconds 紧凑格式。ETA 展示统一走此出口。
 */
export function formatDurationLocalized(
  seconds: number | null | undefined,
  locale = 'zh-CN',
  style: 'long' | 'short' | 'narrow' | 'digital' = 'short',
): string {
  if (!seconds || seconds <= 0) return '0s'
  const total = Math.ceil(seconds)
  const hours = Math.floor(total / 3600)
  const minutes = Math.floor((total % 3600) / 60)
  const secs = total % 60

  const formatter = getDurationFormatter(locale, style)
  if (formatter) {
    try {
      return formatter.format({ hours, minutes, seconds: secs })
    } catch {
      // format 阶段异常 (理论不可达): 落回退
    }
  }
  return formatSeconds(seconds)
}

/**
 * 秒级 Unix 时间戳 → 本地日期时间文本 (无值返回 '-', options 透传 Intl 配置)。
 */
export function formatDateTime(ts: number | string | null | undefined, options?: Intl.DateTimeFormatOptions): string {
  if (!ts) return '-'
  const d = new Date(Number(ts) * 1000)
  if (Number.isNaN(d.getTime())) return '-'
  return options ? d.toLocaleString(undefined, options) : d.toLocaleString()
}

/**
 * 任务耗时: created/updated 为秒级 Unix 时间戳, updated 缺省取当前时刻 (运行中任务)。
 */
export function formatDuration(created: number | null | undefined, updated?: number | null): string {
  if (!created) return '-'
  const end = updated || Date.now() / 1000
  const diff = Math.max(0, end - created)
  return formatSeconds(diff)
}
