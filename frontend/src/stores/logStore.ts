import { defineStore } from 'pinia'
import { ref } from 'vue'
import { logsApi, auditApi } from '@/api'
import { createRequestSequence } from '@/api/client'
import { message } from '@/utils/discreteApi'
import { errDetail, isAuthError } from '@/utils/errors'
import type {
  SystemLogItem,
  SystemLogsResponse,
  ForwardSkipItem,
  ForwardSkipsResponse,
  PublishFailureItem,
  PublishFailuresResponse,
  DownloadFailureItem,
  DownloadFailuresResponse,
} from '@/api/gen/types.gen'

export const useLogStore = defineStore('log', () => {
  // 1. 系统日志
  const systemLogs = ref<SystemLogItem[]>([])
  const systemLogsTotal = ref<number>(0)
  const systemLogsLimit = ref<number>(100)
  const systemLogsLevel = ref<string>('')
  const systemLogsQuery = ref<string>('')
  const loadingSystemLogs = ref<boolean>(false)

  // 2. 转发清洗跳过账本
  const forwardSkips = ref<ForwardSkipItem[]>([])
  const forwardSkipsTotal = ref<number>(0)
  const forwardSkipsPage = ref<number>(1)
  const forwardSkipsLimit = ref<number>(50)
  const forwardSkipsFilters = ref({
    source_chat: '',
    target_chat: '',
    skip_type: '',
    task_id: '',
  })
  const loadingForwardSkips = ref<boolean>(false)

  // 3. 发布失败审计日志
  const publishFailures = ref<PublishFailureItem[]>([])
  const publishFailuresTotal = ref<number>(0)
  const publishFailuresPage = ref<number>(1)
  const publishFailuresLimit = ref<number>(50)
  const publishFailuresFilters = ref({
    type: '',
    task_id: '',
    chat_id: '',
  })
  const loadingPublishFailures = ref<boolean>(false)

  // 4. 下载失败审计日志
  const downloadFailures = ref<DownloadFailureItem[]>([])
  const downloadFailuresTotal = ref<number>(0)
  const downloadFailuresPage = ref<number>(1)
  const downloadFailuresLimit = ref<number>(50)
  const downloadFailuresFilters = ref({
    type: '',
    task_id: '',
    chat_id: '',
  })
  const loadingDownloadFailures = ref<boolean>(false)

  // Actions
  // 四个数据集各自独立请求序号 —— 陈旧响应不得覆盖新结果
  const systemLogsSeq = createRequestSequence()
  const forwardSkipsSeq = createRequestSequence()
  const publishFailuresSeq = createRequestSequence()
  const downloadFailuresSeq = createRequestSequence()

  async function loadSystemLogs(): Promise<void> {
    const token = systemLogsSeq.next()
    loadingSystemLogs.value = true
    try {
      const params = {
        limit: systemLogsLimit.value,
        level: systemLogsLevel.value || undefined,
        query: systemLogsQuery.value.trim() || undefined,
      }
      const res = await logsApi.getSystemLogs(params)
      if (!systemLogsSeq.isCurrent(token)) return
      const data = res.data as SystemLogsResponse | undefined
      systemLogs.value = data?.logs || []
      systemLogsTotal.value = data?.total || 0
    } catch (err: unknown) {
      if (!systemLogsSeq.isCurrent(token)) return
      console.error('加载系统日志失败:', err)
      if (!isAuthError(err)) {
        message.error(`加载系统日志失败: ${errDetail(err)}`)
      }
    } finally {
      if (systemLogsSeq.isCurrent(token)) {
        loadingSystemLogs.value = false
      }
    }
  }

  async function loadForwardSkips(resetPage: boolean = false): Promise<void> {
    if (resetPage) {
      forwardSkipsPage.value = 1
    }
    const token = forwardSkipsSeq.next()
    loadingForwardSkips.value = true
    try {
      const f = forwardSkipsFilters.value
      const params = {
        page: forwardSkipsPage.value,
        limit: forwardSkipsLimit.value,
        source_chat: f.source_chat ? String(f.source_chat).trim() || undefined : undefined,
        target_chat: f.target_chat ? String(f.target_chat).trim() || undefined : undefined,
        skip_type: f.skip_type || undefined,
        task_id: f.task_id ? String(f.task_id).trim() || undefined : undefined,
      }
      const res = await auditApi.getForwardSkips(params)
      if (!forwardSkipsSeq.isCurrent(token)) return
      const data = res.data as ForwardSkipsResponse | undefined
      forwardSkips.value = data?.items || []
      forwardSkipsTotal.value = data?.total || 0
    } catch (err: unknown) {
      if (!forwardSkipsSeq.isCurrent(token)) return
      console.error('加载转发跳过账本失败:', err)
      if (!isAuthError(err)) {
        message.error(`加载转发账本失败: ${errDetail(err)}`)
      }
    } finally {
      if (forwardSkipsSeq.isCurrent(token)) {
        loadingForwardSkips.value = false
      }
    }
  }

  async function loadPublishFailures(resetPage: boolean = false): Promise<void> {
    if (resetPage) {
      publishFailuresPage.value = 1
    }
    const token = publishFailuresSeq.next()
    loadingPublishFailures.value = true
    try {
      const f = publishFailuresFilters.value
      const params = {
        page: publishFailuresPage.value,
        limit: publishFailuresLimit.value,
        type: f.type || undefined,
        task_id: f.task_id ? String(f.task_id).trim() || undefined : undefined,
        chat_id: f.chat_id ? String(f.chat_id).trim() || undefined : undefined,
      }
      const res = await auditApi.getPublishFailures(params)
      if (!publishFailuresSeq.isCurrent(token)) return
      const data = res.data as PublishFailuresResponse | undefined
      publishFailures.value = data?.items || []
      publishFailuresTotal.value = data?.total || 0
    } catch (err: unknown) {
      if (!publishFailuresSeq.isCurrent(token)) return
      console.error('加载发布失败审计失败:', err)
      if (!isAuthError(err)) {
        message.error(`加载发布审计失败: ${errDetail(err)}`)
      }
    } finally {
      if (publishFailuresSeq.isCurrent(token)) {
        loadingPublishFailures.value = false
      }
    }
  }

  async function loadDownloadFailures(resetPage: boolean = false): Promise<void> {
    if (resetPage) {
      downloadFailuresPage.value = 1
    }
    const token = downloadFailuresSeq.next()
    loadingDownloadFailures.value = true
    try {
      const f = downloadFailuresFilters.value
      const params = {
        page: downloadFailuresPage.value,
        limit: downloadFailuresLimit.value,
        type: f.type || undefined,
        task_id: f.task_id ? String(f.task_id).trim() || undefined : undefined,
        chat_id: f.chat_id ? String(f.chat_id).trim() || undefined : undefined,
      }
      const res = await auditApi.getDownloadFailures(params)
      if (!downloadFailuresSeq.isCurrent(token)) return
      const data = res.data as DownloadFailuresResponse | undefined
      downloadFailures.value = data?.items || []
      downloadFailuresTotal.value = data?.total || 0
    } catch (err: unknown) {
      if (!downloadFailuresSeq.isCurrent(token)) return
      console.error('加载下载失败审计失败:', err)
      if (!isAuthError(err)) {
        message.error(`加载下载审计失败: ${errDetail(err)}`)
      }
    } finally {
      if (downloadFailuresSeq.isCurrent(token)) {
        loadingDownloadFailures.value = false
      }
    }
  }

  return {
    systemLogs,
    systemLogsTotal,
    systemLogsLimit,
    systemLogsLevel,
    systemLogsQuery,
    loadingSystemLogs,
    forwardSkips,
    forwardSkipsTotal,
    forwardSkipsPage,
    forwardSkipsLimit,
    forwardSkipsFilters,
    loadingForwardSkips,
    publishFailures,
    publishFailuresTotal,
    publishFailuresPage,
    publishFailuresLimit,
    publishFailuresFilters,
    loadingPublishFailures,
    downloadFailures,
    downloadFailuresTotal,
    downloadFailuresPage,
    downloadFailuresLimit,
    downloadFailuresFilters,
    loadingDownloadFailures,
    loadSystemLogs,
    loadForwardSkips,
    loadPublishFailures,
    loadDownloadFailures,
  }
})
