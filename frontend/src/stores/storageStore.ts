import { defineStore } from 'pinia'
import { ref } from 'vue'
import { storageApi } from '@/api'
import { createRequestSequence } from '@/api/client'
import { message } from '@/utils/discreteApi'
import { errDetail, isAuthError } from '@/utils/errors'
import { formatBytes } from '@/utils/formatters'
import type {
  StorageOverviewResponse,
  StorageMetricsResponse,
  FileItemResponse,
  FileListResponse,
  StorageDeleteResponse,
  CleanOrphansResponse,
  StorageQuotaResponse,
  BreakpointItemResponse,
} from '@/api/gen/types.gen'

export interface StorageBreakpointsState {
  total: number
  orphan_count: number
  paired_count: number
  items: BreakpointItemResponse[]
}

export const useStorageStore = defineStore('storage', () => {
  const overview = ref<StorageOverviewResponse>({
    download_dir: 'downloads',
    total_bytes: 0,
    part_bytes: 0,
    final_bytes: 0,
    file_count: 0,
    final_count: 0,
    part_count: 0,
    orphan_part_count: 0,
    orphan_meta_count: 0,
    unregistered_final_count: 0,
    by_channel: [],
    scanned_at: null,
    disk_total_bytes: 0,
    disk_used_bytes: 0,
    disk_free_bytes: 0,
    disk_free_gb: 0,
    disk_used_percent: 0,
    max_storage_quota_gb: 0,
    max_storage_quota_bytes: 0,
    is_quota_active: false,
    effective_free_bytes: 0,
    effective_free_gb: 0,
    quota_used_percent: 0,
  })

  const metrics = ref<StorageMetricsResponse>({
    disk_total_bytes: 0,
    disk_used_bytes: 0,
    disk_free_bytes: 0,
    disk_free_gb: 0,
    disk_used_percent: 0,
    max_storage_quota_gb: 0,
    max_storage_quota_bytes: 0,
    is_quota_active: false,
    effective_free_bytes: 0,
    effective_free_gb: 0,
    quota_used_percent: 0,
    downloads_bytes: 0,
    downloads_mb: 0,
    db_size_bytes: 0,
    logs_size_bytes: 0,
    total_files_count: 0,
    active_downloads_locked: false,
    total_bytes: 0,
    used_bytes: 0,
    free_bytes: 0,
    percent_used: 0,
  })

  const files = ref<FileItemResponse[]>([])
  const filesTotal = ref<number>(0)
  const filesPage = ref<number>(1)
  const filesLimit = ref<number>(50)
  const filesCategory = ref<string>('')
  const filesQuery = ref<string>('')

  const breakpoints = ref<StorageBreakpointsState>({
    total: 0,
    orphan_count: 0,
    paired_count: 0,
    items: [],
  })

  const loadingOverview = ref<boolean>(false)
  const loadingMetrics = ref<boolean>(false)
  const loadingFiles = ref<boolean>(false)
  const scanning = ref<boolean>(false)
  const loadingBreakpoints = ref<boolean>(false)
  const cleaningOrphans = ref<boolean>(false)

  // 错误态字段: 概览/指标/断点/文件列表失败此前仅 console.error, 用户零感知
  const overviewError = ref<string | null>(null)
  const metricsError = ref<string | null>(null)
  const breakpointsError = ref<string | null>(null)
  const filesError = ref<string | null>(null)

  async function loadOverview(): Promise<void> {
    loadingOverview.value = true
    try {
      const res = await storageApi.getOverview()
      overview.value = (res.data as StorageOverviewResponse) || overview.value
      overviewError.value = null
    } catch (err: unknown) {
      overviewError.value = errDetail(err)
      if (!isAuthError(err)) {
        message.error(`获取存储总览失败: ${errDetail(err)}`)
      }
    } finally {
      loadingOverview.value = false
    }
  }

  async function loadMetrics(): Promise<void> {
    loadingMetrics.value = true
    try {
      const res = await storageApi.getMetrics()
      metrics.value = (res.data as StorageMetricsResponse) || metrics.value
      metricsError.value = null
    } catch (err: unknown) {
      metricsError.value = errDetail(err)
      if (!isAuthError(err)) {
        message.error(`获取磁盘指标失败: ${errDetail(err)}`)
      }
    } finally {
      loadingMetrics.value = false
    }
  }

  async function triggerScan(): Promise<StorageOverviewResponse | undefined> {
    scanning.value = true
    try {
      const res = await storageApi.scan()
      overview.value = (res.data as StorageOverviewResponse) || overview.value
      // 扫描成功刷新了 overview, 旧失败态必须一并清除 (此前仅刷新
      // metrics/breakpoints/files, overviewError 残留造成状态不一致)
      overviewError.value = null
      message.success('downloads/ 存储扫描完成，登记缓存已刷新！')
      await Promise.all([loadMetrics(), loadBreakpoints(), loadFiles()])
      return res.data as StorageOverviewResponse
    } catch (err: unknown) {
      message.error(`存储扫描失败: ${errDetail(err)}`)
      throw err
    } finally {
      scanning.value = false
    }
  }

  // 竞态守卫: 翻页/搜索/分类切换连发时仅采纳最新一次响应 (请求序号守卫基建接线)
  const filesSeq = createRequestSequence()

  async function loadFiles(resetPage: boolean = false): Promise<void> {
    const token = filesSeq.next()
    if (resetPage) {
      filesPage.value = 1
    }
    loadingFiles.value = true
    try {
      const params = {
        page: filesPage.value,
        limit: filesLimit.value,
        category: filesCategory.value || undefined,
        query: filesQuery.value.trim() || undefined,
      }
      const res = await storageApi.getFiles(params)
      if (!filesSeq.isCurrent(token)) return // 已被更新的请求取代, 丢弃过期响应
      const data = res.data as FileListResponse | FileItemResponse[] | undefined
      if (data && 'files' in data && data.files !== undefined) {
        files.value = data.files || []
        filesTotal.value = data.total || 0
      } else if (Array.isArray(data)) {
        files.value = data
        filesTotal.value = data.length
      }
      filesError.value = null
    } catch (err: unknown) {
      if (!filesSeq.isCurrent(token)) return // 过期请求的失败同样不落态
      filesError.value = errDetail(err)
      if (!isAuthError(err)) {
        message.error(`加载文件列表失败: ${errDetail(err)}`)
      }
    } finally {
      if (filesSeq.isCurrent(token)) loadingFiles.value = false
    }
  }

  async function deleteFiles(paths: string[]): Promise<StorageDeleteResponse | undefined> {
    if (!paths || paths.length === 0) return
    try {
      const res = await storageApi.deleteFiles(paths)
      const data = res.data as StorageDeleteResponse
      const deletedCount = data?.deleted?.length || 0
      const skipped = data?.skipped || []
      message.success(`已成功删除 ${deletedCount} 个文件`)
      if (skipped.length > 0) {
        message.warning(`${skipped.length} 个文件跳过删除: ${skipped[0].reason}`)
      }
      await Promise.all([loadOverview(), loadMetrics(), loadFiles(), loadBreakpoints()])
      return data
    } catch (err: unknown) {
      message.error(`删除文件失败: ${errDetail(err)}`)
      throw err
    }
  }

  async function loadBreakpoints(): Promise<void> {
    loadingBreakpoints.value = true
    try {
      const res = await storageApi.getBreakpoints()
      breakpoints.value = (res.data as StorageBreakpointsState) || {
        total: 0,
        orphan_count: 0,
        paired_count: 0,
        items: [],
      }
      breakpointsError.value = null
    } catch (err: unknown) {
      breakpointsError.value = errDetail(err)
      if (!isAuthError(err)) {
        message.error(`加载断点碎片失败: ${errDetail(err)}`)
      }
    } finally {
      loadingBreakpoints.value = false
    }
  }

  async function cleanOrphans(): Promise<CleanOrphansResponse | undefined> {
    cleaningOrphans.value = true
    try {
      const res = await storageApi.cleanOrphans()
      const data = res.data as CleanOrphansResponse
      const freed = formatBytes(data?.freed_bytes || 0)
      message.success(`孤儿断点清理完成：清理 ${data?.cleaned_count || 0} 个碎片，释放 ${freed} 磁盘空间！`)
      await Promise.all([loadOverview(), loadMetrics(), loadBreakpoints(), loadFiles()])
      return data
    } catch (err: unknown) {
      message.error(`清理孤儿断点失败: ${errDetail(err)}`)
      throw err
    } finally {
      cleaningOrphans.value = false
    }
  }

  const savingQuota = ref<boolean>(false)

  async function updateQuota(quotaGb: number): Promise<StorageQuotaResponse | undefined> {
    savingQuota.value = true
    try {
      const res = await storageApi.updateQuota(quotaGb)
      const data = res.data as StorageQuotaResponse
      message.success(data?.message || '存储配额已更新！')
      await Promise.all([loadOverview(), loadMetrics()])
      return data
    } catch (err: unknown) {
      message.error(`更新存储配额失败: ${errDetail(err)}`)
      throw err
    } finally {
      savingQuota.value = false
    }
  }

  return {
    overview,
    metrics,
    files,
    filesTotal,
    filesPage,
    filesLimit,
    filesCategory,
    filesQuery,
    breakpoints,
    loadingOverview,
    loadingMetrics,
    loadingFiles,
    overviewError,
    metricsError,
    filesError,
    breakpointsError,
    scanning,
    loadingBreakpoints,
    cleaningOrphans,
    savingQuota,
    loadOverview,
    loadMetrics,
    triggerScan,
    loadFiles,
    deleteFiles,
    loadBreakpoints,
    cleanOrphans,
    updateQuota,
  }
})
