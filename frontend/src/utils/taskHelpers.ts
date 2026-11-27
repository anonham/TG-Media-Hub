// 任务展示 Helper 统一收敛: TaskCenterView 与 TaskDetailDrawer 此前各自
// 复制粘贴的进度/状态/账号/并发计算函数全部归此单点, 入参统一为 NormalizedTask
// (嵌套 progress 全字段保证有值)。
// 红线: 并发在途分块严格读取规范字段 upload_in_flight_per_connection (upload_in_flight
// /threads_for_display 为禁用旧名, 不做任何回退读取)。
import type { NormalizedTask } from '@/utils/taskNormalizer'
import { formatBytes } from '@/utils/formatters'

export type TaskTagType = 'default' | 'error' | 'primary' | 'info' | 'success' | 'warning'
export type ProgressStatus = 'default' | 'error' | 'success' | 'warning' | 'info'

/** 任务百分比: completed 终态无条件打满 100% → percent → 字节 → 条目 → 0 */
export function calcTaskPercent(t: NormalizedTask | null | undefined): number {
  if (!t) return 0
  if (t.status === 'completed') return 100
  const p = t.progress
  if (p.percent !== undefined && p.percent !== null && Number(p.percent) > 0) {
    return Math.min(100, Math.max(0, parseFloat(Number(p.percent).toFixed(1))))
  }
  if (p.total_bytes > 0) {
    return Math.min(100, Math.max(0, parseFloat(((p.processed_bytes / p.total_bytes) * 100).toFixed(1))))
  }
  if (p.total_items > 0) {
    return Math.min(100, Math.max(0, parseFloat(((p.processed_items / p.total_items) * 100).toFixed(1))))
  }
  return 0
}

/** 主进度文本: 转发任务走条目 + 结算摘要, 其余任务容量优先、条目兜底 */
export function formatTaskProgress(t: NormalizedTask | null | undefined): string {
  if (!t) return '-'
  const p = t.progress
  const curBytes = p.processed_bytes
  const totBytes = p.total_bytes
  const curItems = p.processed_items
  const totItems = p.total_items

  if (t.task_type === 'forward_channel') {
    const summary = t.params?.final_summary as { forwarded?: number } | undefined
    if (t.status === 'completed' && summary && summary.forwarded !== undefined) {
      return `${totItems || curItems}/${totItems || curItems} 已扫描 (成功 ${summary.forwarded} 条)`
    }
    return `${curItems} / ${totItems} 项`
  }
  // 全任务容量维度（驱动主显示）
  if (totBytes > 0 || curBytes > 0) {
    return `${formatBytes(curBytes)} / ${formatBytes(totBytes)}`
  }
  // 纯条目任务或尚未统计到字节
  if (totItems > 0 || curItems > 0) {
    return `${curItems} / ${totItems} 项`
  }
  return `${formatBytes(curBytes)} / ${formatBytes(totBytes)}`
}

/** 是否多账号联合执行 (account_ids / executor_names 任一 >1) */
export function isMultiUserbot(t: NormalizedTask | null | undefined): boolean {
  if (!t) return false
  if (Array.isArray(t.account_ids) && t.account_ids.length > 1) return true
  if (Array.isArray(t.executor_names) && t.executor_names.length > 1) return true
  return false
}

/**
 * 执行账号展示文本: account_ids 联合 > executor_names 兜底 > 单 account_id > 自动调度。
 * resolveName 注入 accountStore.getAccountDisplayName (唯一防重消歧代号来源), 保持本模块无 store 依赖。
 */
export function getTaskAccountDisplay(
  t: NormalizedTask | null | undefined,
  resolveName: (id: number) => string,
): string {
  if (!t) return '自动调度'
  // 1. 如果有 account_ids，优先使用 accountStore 的唯一防重消歧代号
  if (Array.isArray(t.account_ids) && t.account_ids.length > 0) {
    const names = t.account_ids.map((id) => resolveName(id))
    if (names.length > 1) {
      return `[${names.join(', ')}]`
    }
    return names[0]
  }
  // 2. 兜底 executor_names
  if (Array.isArray(t.executor_names) && t.executor_names.length > 0) {
    if (t.executor_names.length > 1) {
      return `[${t.executor_names.join(', ')}]`
    }
    return t.executor_names[0]
  }
  // 3. 兜底单 account_id
  if (t.account_id) {
    return resolveName(t.account_id)
  }
  return '自动调度'
}

export function getStatusTagType(status: string | null | undefined): TaskTagType {
  switch (status) {
    case 'running':
      return 'info'
    case 'completed':
      return 'success'
    case 'paused':
      return 'warning'
    case 'failed':
      return 'error'
    case 'cancelled':
      return 'default'
    default:
      return 'default'
  }
}

export function getProgressStatus(status: string | null | undefined): ProgressStatus {
  if (status === 'completed') return 'success'
  if (status === 'failed') return 'error'
  if (status === 'paused') return 'warning'
  return 'info'
}

export function getTypeLabel(type: string | null | undefined): string {
  const map: Record<string, string> = {
    download_link: '受限单链',
    download_range: '范围抓取',
    publish_folder: '相册发布',
    forward_channel: '云端转发',
    pikpak_publish: 'PikPak 搬运',
  }
  return map[type || ''] || type || '任务'
}

export function getTypeTagType(type: string | null | undefined): TaskTagType {
  if (type === 'forward_channel') return 'error'
  if (type === 'publish_folder') return 'warning'
  if (type === 'pikpak_publish') return 'primary'
  if (String(type).startsWith('download')) return 'info'
  return 'default'
}

/** 是否上传类任务 (并发语义: 通道=连接 而非 线程) */
export function isUploadTask(t: NormalizedTask | null | undefined): boolean {
  if (!t) return false
  return ['publish_folder', 'pikpak_publish'].includes(t.task_type)
}

/** 单连接在途分块数: 严格读取规范字段 upload_in_flight_per_connection, 缺省 2 (Pipelining 推荐值) */
export function getTaskInFlight(t: NormalizedTask | null | undefined): number {
  if (!t) return 2
  const inf = t.params?.upload_in_flight_per_connection
  return inf ? parseInt(String(inf)) : 2
}
