import { ref } from 'vue'
import { defineStore } from 'pinia'
import { forwardApi } from '@/api'
import { createRequestSequence } from '@/api/client'
import { message } from '@/utils/discreteApi'
import { errDetail, isAuthError } from '@/utils/errors'
import type {
  ForwardCoverageData,
  ForwardGapItem,
  ForwardEstimateData,
  ForwardEstimateRequest,
  ForwardReceiptData,
  GetForwardCoverageApiForwardCoverageGetData,
  GetForwardCoverageGapsApiForwardCoverageGapsGetData,
} from '@/api/gen/types.gen'

export interface ForwardHistoryItem {
  id?: number | string
  source_chat?: string
  source_msg_id?: number
  target_chat?: string
  target_msg_id?: number
  forwarded_at?: number | string
  [key: string]: unknown
}

/**
 * 云端转发视图专属 store：覆盖区间可视化 / gap 下拉 / 提交前预估 / 映射反查分页 / 完成回执
 * 任务生命周期仍归 taskStore，此处只承载 ForwardView 的查询态
 */
export const useForwardStore = defineStore('forward', () => {
  // 请求竞态守卫
  const coverageSeq = createRequestSequence()
  const gapsSeq = createRequestSequence()
  const estimateSeq = createRequestSequence()
  const historySeq = createRequestSequence()
  const receiptSeq = createRequestSequence()

  // 覆盖可视化
  const coverage = ref<ForwardCoverageData | null>(null)
  const coverageLoading = ref<boolean>(false)
  const coverageError = ref<string | null>(null)
  const gaps = ref<ForwardGapItem[]>([])
  const gapsLoading = ref<boolean>(false)
  const gapsError = ref<string | null>(null)

  // 提交前预估
  const estimate = ref<ForwardEstimateData | null>(null)
  const estimateLoading = ref<boolean>(false)
  const estimateError = ref<string | null>(null)

  // 映射反查 (惰性 + 分页)
  const historyList = ref<ForwardHistoryItem[]>([])
  const historyPage = ref<number>(1)
  const historyPageSize = 50
  const historyHasMore = ref<boolean>(false)
  const historyLoading = ref<boolean>(false)
  const historyError = ref<string | null>(null)
  let lastHistoryParams: Record<string, unknown> = {}

  // 完成回执
  const receipt = ref<ForwardReceiptData | null>(null)
  const receiptLoading = ref<boolean>(false)
  const receiptError = ref<string | null>(null)

  function clearGaps(): void {
    gapsSeq.next()
    gapsLoading.value = false
    gaps.value = []
    gapsError.value = null
  }

  function clearEstimate(): void {
    estimateSeq.next()
    estimateLoading.value = false
    estimate.value = null
    estimateError.value = null
  }

  function clearCoverage(): void {
    // 推进三条序列使在途响应失效 (isCurrent 不再成立), 并复位 loading 旗 ——
    // 否则清除期间到达的 pending 响应会通过序号检查复活已清数据
    coverageSeq.next()
    coverageLoading.value = false
    coverage.value = null
    coverageError.value = null
    clearGaps()
    clearEstimate()
  }

  async function loadCoverage(
    source: string,
    target: string,
    minId: string | number | null = null,
    maxId: string | number | null = null,
  ): Promise<void> {
    const token = coverageSeq.next()
    const params: GetForwardCoverageApiForwardCoverageGetData['query'] = {
      source_chat: source,
      target_chat: target,
    }
    if (minId) params.min_id = parseInt(String(minId))
    if (maxId) params.max_id = parseInt(String(maxId))
    coverageLoading.value = true
    try {
      const res = await forwardApi.getCoverage(params)
      if (!coverageSeq.isCurrent(token)) return
      coverage.value = res.data?.data || null
      coverageError.value = null
    } catch (err: unknown) {
      if (!coverageSeq.isCurrent(token)) return
      // 覆盖条失败不再静默 (此前仅 console.error 用户零感知)
      coverageError.value = errDetail(err)
      if (!isAuthError(err)) {
        message.error(`查询转发覆盖区间失败: ${errDetail(err)}`)
      }
    } finally {
      if (coverageSeq.isCurrent(token)) {
        coverageLoading.value = false
      }
    }
  }

  async function loadGaps(
    source: string,
    target: string,
    minId: string | number | null = null,
    maxId: string | number | null = null,
  ): Promise<void> {
    const token = gapsSeq.next()
    const params: GetForwardCoverageGapsApiForwardCoverageGapsGetData['query'] = {
      source_chat: source,
      target_chat: target,
    }
    if (minId) params.min_id = parseInt(String(minId))
    if (maxId) params.max_id = parseInt(String(maxId))
    gapsLoading.value = true
    try {
      const res = await forwardApi.getGaps(params)
      if (!gapsSeq.isCurrent(token)) return
      gaps.value = res.data?.data?.gaps || []
      gapsError.value = null
    } catch (err: unknown) {
      if (!gapsSeq.isCurrent(token)) return
      gapsError.value = errDetail(err)
      if (!isAuthError(err)) {
        message.error(`查询未覆盖区间失败: ${errDetail(err)}`)
      }
    } finally {
      if (gapsSeq.isCurrent(token)) {
        gapsLoading.value = false
      }
    }
  }

  async function fetchEstimate(
    source: string,
    target: string,
    startId?: string | number | null,
    endId?: string | number | null,
    batchSize?: string | number | null,
  ): Promise<void> {
    const token = estimateSeq.next()
    const payload: ForwardEstimateRequest = {
      source_chat: source,
      target_chat: target,
      batch_size: parseInt(String(batchSize || 50)) || 50,
    }
    if (startId) payload.start_id = parseInt(String(startId))
    if (endId) payload.end_id = parseInt(String(endId))
    estimateLoading.value = true
    try {
      const res = await forwardApi.estimate(payload)
      if (!estimateSeq.isCurrent(token)) return
      estimate.value = res.data?.data || null
      estimateError.value = null
    } catch (err: unknown) {
      if (!estimateSeq.isCurrent(token)) return
      estimateError.value = errDetail(err)
      if (!isAuthError(err)) {
        message.error(`提交前预估失败: ${errDetail(err)}`)
      }
    } finally {
      if (estimateSeq.isCurrent(token)) {
        estimateLoading.value = false
      }
    }
  }

  async function searchHistory(params: Record<string, unknown> = {}): Promise<void> {
    const token = historySeq.next()
    historyLoading.value = true
    lastHistoryParams = params
    historyPage.value = 1
    try {
      const query = { ...params, page: historyPage.value, limit: historyPageSize }
      const res = await forwardApi.getHistory(query)
      if (!historySeq.isCurrent(token)) return
      const rows = (res.data?.data || []) as ForwardHistoryItem[]
      historyList.value = rows
      historyHasMore.value = rows.length >= historyPageSize
      historyError.value = null
    } catch (err: unknown) {
      if (!historySeq.isCurrent(token)) return
      historyError.value = errDetail(err)
      message.error(`查询转发历史失败: ${errDetail(err)}`)
    } finally {
      if (historySeq.isCurrent(token)) {
        historyLoading.value = false
      }
    }
  }

  async function loadMoreHistory(): Promise<void> {
    if (!historyHasMore.value || historyLoading.value) return
    const token = historySeq.next()
    historyLoading.value = true
    historyPage.value += 1
    try {
      const query = { ...lastHistoryParams, page: historyPage.value, limit: historyPageSize }
      const res = await forwardApi.getHistory(query)
      if (!historySeq.isCurrent(token)) return
      const rows = (res.data?.data || []) as ForwardHistoryItem[]
      // 代际守卫 —— 陈旧分页响应不得并入新搜索结果或改写页码
      historyList.value = historyList.value.concat(rows)
      historyHasMore.value = rows.length >= historyPageSize
      historyError.value = null
    } catch (err: unknown) {
      if (!historySeq.isCurrent(token)) return
      historyError.value = errDetail(err)
      historyPage.value -= 1
      message.error(`加载更多转发历史失败: ${errDetail(err)}`)
    } finally {
      if (historySeq.isCurrent(token)) {
        historyLoading.value = false
      }
    }
  }

  async function loadReceipt(taskId: string): Promise<void> {
    // 回执请求加序号守卫 (与 coverage/gaps/estimate 同口径), 防旧响应覆盖新回执
    const token = receiptSeq.next()
    receiptLoading.value = true
    try {
      const res = await forwardApi.getReceipt(taskId)
      if (!receiptSeq.isCurrent(token)) return
      receipt.value = res.data?.data || null
      receiptError.value = null
    } catch (err: unknown) {
      if (!receiptSeq.isCurrent(token)) return
      receiptError.value = errDetail(err)
      if (!isAuthError(err)) {
        message.error(`查询转发回执失败: ${errDetail(err)}`)
      }
    } finally {
      if (receiptSeq.isCurrent(token)) {
        receiptLoading.value = false
      }
    }
  }

  return {
    coverage,
    coverageLoading,
    coverageError,
    gaps,
    gapsLoading,
    gapsError,
    estimate,
    estimateLoading,
    estimateError,
    historyList,
    historyHasMore,
    historyLoading,
    historyError,
    receipt,
    receiptLoading,
    receiptError,
    clearCoverage,
    clearGaps,
    clearEstimate,
    loadCoverage,
    loadGaps,
    fetchEstimate,
    searchHistory,
    loadMoreHistory,
    loadReceipt,
  }
})
