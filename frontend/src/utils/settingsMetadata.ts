// 系统设置页静态元数据与校验规则。
// 展示目录只组织后端实际返回的键，不负责创建或默认化动态设置。
// 下拉选项统一取自 utils/publishConstants.ts。
import type { SelectOption } from 'naive-ui'
import { CLEANUP_MODE_OPTIONS, PIKPAK_OVERSIZE_POLICY_OPTIONS, SPLIT_STRATEGY_OPTIONS } from '@/utils/publishConstants'

export type SettingItemOption = SelectOption

export interface SettingItemMeta {
  tab?: string
  card?: string
  title?: string
  type?: 'timestamp' | 'switch'
  readOnly?: boolean
  options?: SettingItemOption[]
  min?: number
  max?: number
  step?: number
}

export interface SettingCardMeta {
  key: string
  title: string
  kind?: 'apiAuth' | 'explainer' | 'diagnostic'
  emptyText?: string
}

export interface SettingItem {
  key: string
  title: string
  description: string
  value?: unknown
  meta?: SettingItemMeta
}

export interface SettingCard {
  key: string
  title?: string
  kind?: 'apiAuth' | 'explainer' | 'diagnostic' | string
  emptyText?: string
  items: SettingItem[]
}

export const SETTING_META_MAP: Record<string, SettingItemMeta> = {
  max_global_concurrency: { tab: 'general', card: 'global', title: '系统最大并行任务数' },
  max_storage_quota_gb: { tab: 'general', card: 'global', title: '下载存储配额上限 (GB, 0 为不限制)' },
  last_storage_scan_at: { tab: 'general', card: 'global', title: '最近一次存储扫描时间戳', type: 'timestamp' },
  auto_remux_mpegts_to_mp4: {
    tab: 'general',
    card: 'global',
    title: 'MPEG-TS 自动转封装为 MP4',
    type: 'switch',
  },
  download_parallel_connections: { tab: 'download', card: 'downloadMatrix', title: '默认下载并发通道数' },
  download_chunk_size_kb: { tab: 'download', card: 'downloadMatrix', title: '默认下载分块大小 (KB)' },
  enable_transit_acceleration: { tab: 'download', card: 'transit', title: '启用多 Bot 联合中转加速', type: 'switch' },
  transit_chat_id: { tab: 'download', card: 'transit', title: '私有中转群组 ID (例如 -100xxxxxxxxxx)' },
  transit_min_file_size_mb: { tab: 'download', card: 'transit', title: '默认中转加速触发门槛 (MB)' },
  default_target_channel: { tab: 'forward', card: 'target', title: '默认转存私有频道 ID' },
  flood_wait_threshold_sec: { tab: 'forward', card: 'retry', title: '触发 FloodWait 自动休眠阈值 (秒)' },
  request_retry_delay_sec: { tab: 'forward', card: 'retry', title: '网络请求重试间隔 (秒)' },
  max_retry_attempts: { tab: 'forward', card: 'retry', title: '最大容错重试次数' },
  upload_parallel_connections: { tab: 'local_publish', card: 'publishChannels', title: '默认上传并发通道数' },
  upload_in_flight_per_connection: {
    tab: 'local_publish',
    card: 'publishChannels',
    title: '默认单连接在途分块数 (Pipelining 深度)',
  },
  upload_chunk_size_kb: { tab: 'local_publish', card: 'publishChannels', title: '默认上传分块大小 (KB)' },
  userbot_upload_threshold_mb: { tab: 'local_publish', card: 'routing', title: '默认 Userbot 上传分流阈值 (MB)' },
  bot_chunk_upload_limit_mb: { tab: 'local_publish', card: 'routing', title: 'Bot 分块上传最大体积 (MB)' },
  message_edit_delay_ms: { tab: 'local_publish', card: 'messageProgress', title: '消息进度刷新节流间隔 (毫秒)' },
  pikpak_upload_concurrency: { tab: 'pikpak', card: 'streaming', title: 'PikPak 默认上传并发通道数' },
  pikpak_upload_in_flight: { tab: 'pikpak', card: 'streaming', title: 'PikPak 默认单连接在途分块数' },
  pikpak_prefetch_concurrency: { tab: 'pikpak', card: 'streaming', title: 'PikPak 默认预拉取并发通道数' },
  pikpak_ring_buffer_mb: { tab: 'pikpak', card: 'network', title: 'PikPak 环形内存容灾缓冲区 (MB)' },
  pikpak_proxy_url: { tab: 'pikpak', card: 'network', title: 'PikPak 控制面专享代理地址' },
  pikpak_default_cleanup_mode: {
    tab: 'pikpak',
    card: 'policy',
    title: 'PikPak 任务完成默认清理模式',
    options: CLEANUP_MODE_OPTIONS,
  },
  pikpak_default_oversize_policy: {
    tab: 'pikpak',
    card: 'policy',
    title: 'PikPak 超限文件默认处理策略',
    options: PIKPAK_OVERSIZE_POLICY_OPTIONS,
  },
  pikpak_default_split_strategy: {
    tab: 'pikpak',
    card: 'policy',
    title: 'PikPak 默认流式切片引擎策略',
    options: SPLIT_STRATEGY_OPTIONS,
  },
  auto_reconnect: {
    tab: 'about',
    card: 'telegramCore',
    title: '断网自动重新连接',
    type: 'switch',
    readOnly: true,
  },
}

export const SETTING_CARD_LAYOUT: Record<string, SettingCardMeta[]> = {
  general: [
    { key: 'global', title: '全局调度与存储配额' },
    { key: 'apiAuth', title: 'API 凭证与鉴权', kind: 'apiAuth' },
    { key: 'other', title: '其他系统配置', emptyText: '当前没有其他未分类的动态配置项' },
  ],
  download: [
    { key: 'downloadMatrix', title: '下载并发与分块矩阵', emptyText: '当前版本未提供下载分块大小设置' },
    { key: 'transit', title: '⚡ 多 Bot 联合加速配置' },
    { key: 'transitDiagnostic', title: '中转集群连通性与权限诊断看板', kind: 'diagnostic' },
  ],
  forward: [
    { key: 'target', title: '默认转存私有频道' },
    { key: 'retry', title: '频控延迟与网络重试', emptyText: '当前版本未提供独立的频控/重试动态设置键' },
    { key: 'forwardExplainer', title: '纯净过滤模式说明', kind: 'explainer' },
  ],
  local_publish: [
    { key: 'publishChannels', title: '发布通道与在途分块数' },
    { key: 'routing', title: '文件分流阈值与相册模式', emptyText: '当前版本未提供额外分流设置' },
    { key: 'messageProgress', title: '消息进度刷新节流', emptyText: '当前版本未提供独立的进度刷新设置键' },
  ],
  pikpak: [
    { key: 'streaming', title: 'PikPak 流式传输与在途通道' },
    { key: 'network', title: '内存环形容灾与代理网络' },
    { key: 'policy', title: '默认归档清理与超限治理策略' },
  ],
}

export function formatItemTitle(key = ''): string {
  const titleMap: Record<string, string> = {
    max_global_concurrency: '系统最大并行任务数',
    max_concurrent_tasks: '单个批次并发任务数',
    download_parallel_connections: '默认下载并发通道数',
    upload_parallel_connections: '默认上传并发通道数',
    upload_in_flight_per_connection: '默认单连接在途分块数 (Pipelining 深度)',
    default_target_channel: '默认转存私有频道 ID',
    download_chunk_size_kb: '默认下载分块大小 (KB)',
    upload_chunk_size_kb: '默认上传分块大小 (KB)',
    max_storage_quota_gb: '下载存储配额上限 (GB, 0 为不限制)',
    enable_transit_acceleration: '启用多 Bot 联合中转加速',
    transit_chat_id: '私有中转群组 ID (例如 -100xxxxxxxxxx)',
    transit_min_file_size_mb: '默认中转加速触发门槛 (MB)',
    userbot_upload_threshold_mb: '默认 Userbot 上传分流阈值 (MB)',
    bot_chunk_upload_limit_mb: 'Bot 分块上传最大体积 (MB)',
    flood_wait_threshold_sec: '触发 FloodWait 自动休眠阈值 (秒)',
    message_edit_delay_ms: '消息进度刷新节流间隔 (毫秒)',
    request_retry_delay_sec: '网络请求重试间隔 (秒)',
    max_retry_attempts: '最大容错重试次数',
    auto_reconnect: '断网自动重新连接',
    last_storage_scan_at: '最近一次存储扫描时间戳',
    pikpak_upload_concurrency: 'PikPak 默认上传并发通道数',
    pikpak_upload_in_flight: 'PikPak 默认单连接在途分块数',
    pikpak_ring_buffer_mb: 'PikPak 环形内存容灾缓冲区 (MB)',
    pikpak_prefetch_concurrency: 'PikPak 默认预拉取并发通道数',
    pikpak_proxy_url: 'PikPak 控制面专享代理地址',
    pikpak_default_cleanup_mode: 'PikPak 任务完成默认清理模式',
    pikpak_default_oversize_policy: 'PikPak 超限文件默认处理策略',
    pikpak_default_split_strategy: 'PikPak 默认流式切片引擎策略',
    auto_remux_mpegts_to_mp4: 'MPEG-TS 自动转封装为 MP4',
  }
  return titleMap[key] || key
}

const ITEM_DESC_MAP: Record<string, string> = {
  download_parallel_connections: '新建下载任务默认分配的 TCP 连接通道数，推荐 8，范围 4~32，步长 4',
  upload_parallel_connections: '新建上传任务默认分配的并发通道数，推荐 2，范围 1~16',
  upload_in_flight_per_connection: '每个上传连接多路复用的在途并发分块数，推荐 2，范围 1~4',
  default_target_channel: '新建发布/转存任务时缺省绑定的 Telegram 私密频道 ID',
  max_global_concurrency: '允许同时处于 running 状态的任务总数上限',
  userbot_upload_threshold_mb: '超过该容量自动切换为 Userbot 发送 (MB)',
  transit_min_file_size_mb: '仅对超过该大小的文件启用集群加速，默认 20 MB；小文件直接下载更高效，避免转发开销',
  enable_transit_acceleration: '开启后符合条件的大文件自动触发集群多路并行拉取；异常或小文件自动降级直连',
  transit_chat_id:
    '用于多 Bot 临时分块传输的 Telegram 私有群 ID，格式形如 -100xxxxxxxxxx。要求：主账号具备发信与删信权限，各辅助 Bot 已入群',
  last_storage_scan_at: '系统最近一次执行本地磁盘存储配额与物理文件登记扫描的记录时间',
  pikpak_upload_concurrency: '新建 PikPak 发布任务默认分配的上传并发通道数，推荐 2，范围 1~16',
  pikpak_upload_in_flight: '新建 PikPak 发布任务单连接多路复用的在途分块数，推荐 1，范围 1~4',
  pikpak_ring_buffer_mb: '流式发布内存环形容灾缓冲区容量，推荐 50 MB，范围 10~1024 MB',
  pikpak_prefetch_concurrency: '新建 PikPak 发布任务 HTTP Range 预拉取并发通道数，推荐 3，范围 1~16',
  pikpak_proxy_url:
    '仅供 PikPak 控制面 (CLI 进程) 专享的代理地址 (例如 http://127.0.0.1:7890 或 socks5://127.0.0.1:1080，留空直连；二进制视频流 100% 物理直连不走代理)',
  pikpak_default_cleanup_mode:
    '发布成功后默认操作 (archive: 归档到根目录 [已发布归档], none: 保留原状, delete: 移入回收站)',
  pikpak_default_oversize_policy:
    '大文件超出单文件上限时的默认策略 (skip_and_report: 跳过并占位报告, split: 虚拟切片分卷)',
  pikpak_default_split_strategy:
    '超限视频文件切片分卷时的底层流式生成引擎 (vsse_with_fallback: VSSE 极速重构首选，零落盘零转码秒开; fmp4_universal: fMP4 全格式保底兼容; raw: 原始二进制硬切)',
  auto_remux_mpegts_to_mp4:
    '开启后，发布 .ts/.m2ts/.mts 视频前自动零落盘重封装为 MP4，解决 Telegram 剥离 mpegts 视频属性导致无法在线流式播放的问题（不兼容编码自动降级原样发送）',
}

export function formatItemDesc(item: { key?: string; description?: string } | null | undefined): string {
  if (!item) return ''
  return ITEM_DESC_MAP[item.key || ''] || item.description || '无系统级额外说明，修改后即时热生效'
}

export function isNumericKey(key = ''): boolean {
  return (
    key.includes('concurrency') ||
    key.includes('threshold') ||
    key.includes('quota') ||
    key.includes('delay') ||
    key.includes('limit') ||
    key.includes('items') ||
    key.includes('bytes') ||
    key.includes('size') ||
    key.includes('attempts') ||
    key.includes('flight') ||
    key.includes('buffer')
  )
}

export function getMinVal(key = ''): number {
  if (key === 'max_global_concurrency') return 1
  if (key === 'userbot_upload_threshold_mb') return 1
  if (key === 'download_parallel_connections') return 4
  if (key === 'upload_parallel_connections') return 1
  if (key === 'upload_in_flight_per_connection') return 1
  if (key === 'pikpak_upload_concurrency') return 1
  if (key === 'pikpak_upload_in_flight') return 1
  if (key === 'pikpak_ring_buffer_mb') return 10
  if (key === 'pikpak_prefetch_concurrency') return 1
  return 0
}

export function getMaxVal(key = ''): number {
  if (key === 'max_global_concurrency') return 20
  if (key === 'download_parallel_connections') return 32
  if (key === 'upload_parallel_connections') return 16
  if (key === 'upload_in_flight_per_connection') return 4
  if (key === 'pikpak_upload_concurrency') return 16
  if (key === 'pikpak_upload_in_flight') return 4
  if (key === 'pikpak_ring_buffer_mb') return 1024
  if (key === 'pikpak_prefetch_concurrency') return 16
  return 999999
}

export function getStepVal(key = ''): number {
  if (key === 'download_parallel_connections') return 4
  if (key === 'upload_parallel_connections') return 1
  if (key === 'upload_in_flight_per_connection') return 1
  if (key === 'pikpak_upload_concurrency') return 1
  if (key === 'pikpak_upload_in_flight') return 1
  if (key === 'pikpak_ring_buffer_mb') return 10
  if (key === 'pikpak_prefetch_concurrency') return 1
  return 1
}

/**
 * 单键保存前校验 (Scoped Validation): 返回错误消息, 通过返回 null。
 * 消息与边界与既有保存守卫逐字保持。
 */
export function validateSettingValue(key: string, rawValue: unknown): string | null {
  const v = String(rawValue)
  // 全串数值校验 —— parseInt/parseFloat 前缀解析会放过 '4.5' (→4)、
  // '4junk'、'Infinity' 等残缺/非有限值; Number + isInteger/isFinite 才是完整判定
  const num = v.trim() === '' ? NaN : Number(v.trim())
  const intVal = num
  const intBetween = (min: number, max: number, label: string): string | null => {
    if (!Number.isInteger(num) || num < min || num > max) {
      return `${label} 必须介于 ${min} 到 ${max} 之间`
    }
    return null
  }

  switch (key) {
    case 'max_global_concurrency':
      return v !== '' ? intBetween(1, 20, key) : null
    case 'download_parallel_connections': {
      if (v === '') return null
      if (!/^[+-]?[0-9]+$/.test(v.trim())) {
        return 'download_parallel_connections 必须介于 4 到 32 之间'
      }
      const err = intBetween(4, 32, key)
      if (err) return err
      if (intVal % 4 !== 0) {
        return 'download_parallel_connections 必须为 4 的倍数 (如 4, 8, 12, 16, 20, 24, 28, 32)'
      }
      return null
    }
    case 'upload_parallel_connections':
      return v !== '' ? intBetween(1, 16, key) : null
    case 'upload_in_flight_per_connection':
      return v !== '' ? intBetween(1, 4, key) : null
    case 'pikpak_upload_concurrency':
      return v !== '' ? intBetween(1, 16, key) : null
    case 'pikpak_upload_in_flight':
      return v !== '' ? intBetween(1, 4, key) : null
    case 'pikpak_ring_buffer_mb':
      return v !== '' ? intBetween(10, 1024, key) : null
    case 'pikpak_prefetch_concurrency':
      return v !== '' ? intBetween(1, 16, key) : null
    case 'pikpak_proxy_url': {
      const trimmed = v.trim()
      if (!trimmed) return null
      try {
        const u = new URL(trimmed)
        if (!['http:', 'https:', 'socks5:', 'socks5h:'].includes(u.protocol)) {
          return 'pikpak_proxy_url 协议必须为 http://, https:// 或 socks5://'
        }
      } catch {
        return 'pikpak_proxy_url 格式无效'
      }
      return null
    }
    case 'pikpak_default_cleanup_mode':
      if (!['none', 'archive', 'delete'].includes(v)) {
        return 'pikpak_default_cleanup_mode 必须为 none / archive / delete 之一'
      }
      return null
    case 'userbot_upload_threshold_mb':
      if (v !== '' && (!Number.isInteger(num) || num < 1)) {
        return 'userbot_upload_threshold_mb 必须为大于等于 1 的整数'
      }
      return null
    case 'max_storage_quota_gb':
      if (v !== '' && (!Number.isFinite(num) || num < 0)) {
        return 'max_storage_quota_gb 必须为大于等于 0 的数字'
      }
      return null
    case 'transit_min_file_size_mb':
      if (v !== '' && (!Number.isInteger(num) || num < 0)) {
        return 'transit_min_file_size_mb 必须为大于等于 0 的整数'
      }
      return null
    case 'pikpak_default_oversize_policy':
      if (!['skip_and_report', 'split'].includes(v)) {
        return 'pikpak_default_oversize_policy 必须为 skip_and_report / split 之一'
      }
      return null
    case 'pikpak_default_split_strategy':
      if (!['vsse_with_fallback', 'fmp4_universal', 'raw'].includes(v)) {
        return 'pikpak_default_split_strategy 必须为 vsse_with_fallback / fmp4_universal / raw 之一'
      }
      return null
    case 'auto_remux_mpegts_to_mp4':
      if (!['true', 'false'].includes(v)) {
        return 'auto_remux_mpegts_to_mp4 必须为 true 或 false'
      }
      return null
    default:
      return null
  }
}
