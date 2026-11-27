// 任务模型单一归一层: REST TaskResponse 与 WS progress 载荷的唯一入口转换器。
// 后端 TaskResponse 已单轨制收口: 进度元数据仅存在于嵌套 progress,
// 顶层扁平影子字段已从契约删除; 本模块负责补齐全字段保值语义 (数值恒 number, 严禁
// undefined 扩散到模板表达式)。
import type { DownloadTaskConflictResponse, TaskResponse } from '@/api/gen'

// 前端标准进度模型: 全字段保证有值
export interface NormalizedTaskProgress {
  processed_bytes: number
  total_bytes: number
  processed_items: number
  total_items: number
  percent: number
  speed_mb_s: number
  eta_s: number
  current_item: string
  concurrency: number
  concurrency_total: number
  dc_id: number
  pikpak_download_speed_mb_s: number
  flood_wait_seconds: number | null
}

// 前端标准任务模型: TaskResponse + 保证完整嵌套 progress;
// executor_names / account_ids 仅由 WS task_update 广播补写 (后端 TaskResponse 契约不含)
export type NormalizedTask = Omit<TaskResponse, 'progress'> & {
  progress: NormalizedTaskProgress
  executor_names?: string[]
  account_ids?: number[]
}

// 四大提交函数的统一返回契约: submitLinkDownload / submitRangeDownload /
// submitFolderPublish / submitChannelForward
export type TaskSubmitResult = {
  success: boolean
  taskId: string | null
  error?: string
  downloadConflict?: DownloadTaskConflictResponse['detail']
}

function toNum(value: unknown, fallback = 0): number {
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

/**
 * 唯一入口转换器: REST TaskResponse → NormalizedTask。幂等: 对已归一模型再归一, 结果不变。
 */
export function normalizeTask(raw: unknown): NormalizedTask {
  const src = (raw && typeof raw === 'object' ? raw : {}) as Record<string, unknown>
  const prog = (src.progress && typeof src.progress === 'object' ? src.progress : {}) as Record<string, unknown>
  const progress: NormalizedTaskProgress = {
    processed_bytes: toNum(prog.processed_bytes),
    total_bytes: toNum(prog.total_bytes),
    processed_items: toNum(prog.processed_items),
    total_items: toNum(prog.total_items),
    percent: toNum(prog.percent),
    speed_mb_s: toNum(prog.speed_mb_s),
    eta_s: toNum(prog.eta_s),
    current_item: String(prog.current_item ?? ''),
    concurrency: toNum(prog.concurrency),
    concurrency_total: toNum(prog.concurrency_total),
    dc_id: toNum(prog.dc_id),
    pikpak_download_speed_mb_s: toNum(prog.pikpak_download_speed_mb_s),
    flood_wait_seconds:
      prog.flood_wait_seconds === undefined || prog.flood_wait_seconds === null ? null : toNum(prog.flood_wait_seconds),
  }
  return { ...src, progress } as NormalizedTask
}

/**
 * 增量补丁合并器: 将 WS progress / task_update 载荷原地合并进 NormalizedTask。
 * 只写载荷中显式存在的字段 (缺省字段保留现值); WS 载荷契约为嵌套 progress only
 * (app/core/executor_progress.py), 不接受扁平形态。
 */
export function applyTaskProgressPatch(task: NormalizedTask, wsPayload: unknown): void {
  const data = (wsPayload && typeof wsPayload === 'object' ? wsPayload : {}) as Record<string, unknown>
  const prog = (data.progress && typeof data.progress === 'object' ? data.progress : {}) as Record<string, unknown>
  const p = task.progress
  const numericFields = [
    'processed_bytes',
    'total_bytes',
    'processed_items',
    'total_items',
    'percent',
    'speed_mb_s',
    'eta_s',
    'concurrency',
    'concurrency_total',
    'dc_id',
  ] as const
  for (const key of numericFields) {
    if (prog[key] !== undefined && prog[key] !== null) {
      p[key] = toNum(prog[key])
    }
  }
  if (prog.current_item !== undefined && prog.current_item !== null) {
    p.current_item = String(prog.current_item)
  }
  if (prog.pikpak_download_speed_mb_s !== undefined && prog.pikpak_download_speed_mb_s !== null) {
    p.pikpak_download_speed_mb_s = toNum(prog.pikpak_download_speed_mb_s)
  }
  const floodWait = prog.flood_wait_seconds !== undefined ? prog.flood_wait_seconds : data.flood_wait_seconds
  if (floodWait !== undefined && floodWait !== null) {
    p.flood_wait_seconds = toNum(floodWait)
  }
  if (data.status !== undefined && data.status !== null) {
    task.status = String(data.status)
  }
}
