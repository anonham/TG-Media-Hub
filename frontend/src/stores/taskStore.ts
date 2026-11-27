// 任务域主 Store: 仅保留任务 CRUD、控制、预检/提交/测算与列表查询。
// 实时通道拆至 taskRealtime.ts 协作模块, 预设频道拆至 presetStore.ts (不留旧名转发);
// 公共表面除预设成员与 batchXxx 语法糖外保持不变。
import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import { isAxiosError } from 'axios'
import type {
  BatchTaskActionRequest,
  DownloadLinkRequest,
  DownloadRangeRequest,
  DownloadTaskConflictResponse,
  ForwardChannelRequest,
  InspectFolderRequest,
  PublishFolderRequest,
  UpdateTaskConcurrencyRequest,
} from '@/api/gen'
import { downloadApi, forwardApi, publishApi, tasksApi } from '@/api'
import { createRequestSequence } from '@/api/client'
import { normalizeTask, type NormalizedTask, type TaskSubmitResult } from '@/utils/taskNormalizer'
import { useAuthStore } from './authStore'
import { usePresetStore } from './presetStore'
import { createTaskRealtime } from './taskRealtime'
import { message } from '@/utils/discreteApi'
import { errDetail } from '@/utils/errors'

export const useTaskStore = defineStore('task', () => {
  const authStore = useAuthStore()
  const presetStore = usePresetStore()

  const tasks = ref<NormalizedTask[]>([])
  const totalTasks = ref(0)
  // 全库级状态统计: 由 /api/tasks 响应 status_counts 直接驱动, 键集 = 状态枚举全集 + all
  const EMPTY_STATUS_COUNTS = {
    all: 0,
    pending: 0,
    queued: 0,
    running: 0,
    paused: 0,
    completed: 0,
    failed: 0,
    cancelled: 0,
  }
  const statusCounts = ref<Record<string, number>>({ ...EMPTY_STATUS_COUNTS })
  const currentPage = ref(1)
  const pageSize = ref(50)
  const taskFilters = ref({ status: 'all', task_type: 'all', account_id: null as number | null, query: '' })
  const selectedTaskId = ref<string | null>(null)
  const tasksReqSeq = createRequestSequence()
  // 实时通道协作模块装配: 传输层状态全部内聚在 taskRealtime.ts
  const {
    wsStatus,
    currentLogs,
    currentMetrics,
    fetchTaskLogs,
    fetchAllLogs,
    ensureLogsUpToDate,
    resetLogStream,
    resetMetrics,
    updateMetricsData,
    startPolling: startRealtimePolling,
    stopPolling,
    scheduleNextPoll,
    initWebSocket,
  } = createTaskRealtime({
    tasks,
    selectedTaskId,
    loadTasks,
    onTaskCompleted: () => {
      // 任务终态广播后清理 PikPak 远端目录缓存 (选区/已发布状态可能已变化)
      import('./pikpakStore')
        .then(({ usePikpakStore }) => usePikpakStore().clearCache())
        .catch((err) => console.error('PikPak 缓存清理失败:', err))
    },
  })

  const selectedTask = computed(() => {
    if (!selectedTaskId.value) return null
    return tasks.value.find((t) => t.id === selectedTaskId.value) || null
  })

  // 全局瞬时总传输速率 (实时求和所有 status === 'running' 活跃任务的 speed_mb_s)
  const totalRunningSpeed = computed(() => {
    let sum = 0
    for (const t of tasks.value) {
      if (t.status === 'running' && t.progress.speed_mb_s) sum += Number(t.progress.speed_mb_s) || 0
    }
    return parseFloat(sum.toFixed(2))
  })

  // 1. 任务列表加载 (带防重入保护、真分页下推, 最新请求胜出)
  async function loadTasks(customParams: Record<string, unknown> = {}) {
    if (authStore.isAuthRequired && !authStore.apiKey) return
    const token = tasksReqSeq.next()
    try {
      const queryParams: Record<string, unknown> = {}
      const page = customParams.page !== undefined ? customParams.page : currentPage.value
      const limit = customParams.limit !== undefined ? customParams.limit : pageSize.value
      if (page !== undefined && page !== null) queryParams.page = page
      if (limit !== undefined && limit !== null) queryParams.limit = limit

      const activeFilter: Record<string, unknown> = { ...taskFilters.value, ...customParams }
      if (activeFilter.status && activeFilter.status !== 'all') queryParams.status = activeFilter.status
      if (activeFilter.task_type && activeFilter.task_type !== 'all') queryParams.task_type = activeFilter.task_type
      if (activeFilter.account_id != null && activeFilter.account_id !== 'all')
        queryParams.account_id = activeFilter.account_id
      if (activeFilter.query && String(activeFilter.query).trim()) queryParams.query = String(activeFilter.query).trim()

      const res = await tasksApi.list(queryParams)
      if (!tasksReqSeq.isCurrent(token)) return
      // 核心类型安全防御: 统一解包数组或 TaskListResponse 分页对象，彻底根治白屏崩溃
      const raw = res.data
      const rawList: unknown[] = Array.isArray(raw) ? raw : (raw as { items?: unknown[] })?.items || []
      totalTasks.value = Array.isArray(raw) ? raw.length : ((raw as { total?: number })?.total ?? rawList.length)
      if (!Array.isArray(raw)) {
        const paged = raw as { page?: number; limit?: number; status_counts?: Record<string, number> }
        if (paged.page !== undefined) currentPage.value = paged.page
        if (paged.limit !== undefined) pageSize.value = paged.limit
        const sc = paged.status_counts
        statusCounts.value =
          sc && typeof sc === 'object' ? { ...EMPTY_STATUS_COUNTS, ...sc } : { ...EMPTY_STATUS_COUNTS }
      }

      // 单一归一入口: 扁平影子字段在此剥除, 下游只读嵌套 progress
      const prevById = new Map(tasks.value.map((t) => [t.id, t]))
      const taskData = rawList.map((t) => {
        const norm = normalizeTask(t)
        // REST 契约不含 WS-only 元数据 (executor_names / account_ids) ——
        // 列表刷新时按 id 回填, 防多账号指示与执行器名在提供它们的更新之后消失
        const prev = prevById.get(norm.id)
        if (prev) {
          if (!norm.executor_names?.length && prev.executor_names?.length) norm.executor_names = prev.executor_names
          if (!norm.account_ids?.length && prev.account_ids?.length) norm.account_ids = prev.account_ids
        }
        return norm
      })
      tasks.value = taskData

      if (taskData.length === 0) {
        selectedTaskId.value = null
        resetMetrics()
        resetLogStream()
        return
      }

      // 如果未选择任务或所选任务不存在，默认选择第一项
      if (!selectedTaskId.value || !taskData.some((t) => t.id === selectedTaskId.value)) {
        selectedTaskId.value = taskData[0].id
        await fetchTaskLogs(selectedTaskId.value, { reset: true })
      }

      const activeTask = taskData.find((t) => t.id === selectedTaskId.value)
      if (activeTask) {
        ensureLogsUpToDate(activeTask)
        // WS 断连时用 REST 快照持续刷新指标 (速率/ETA 不冻结); WS 在线时优先实时推送
        if (wsStatus.value !== 'connected') updateMetricsData(activeTask)
      }
    } catch (err) {
      console.error('获取任务列表异常:', err)
    }
  }

  // 2. 生命周期门面: 预设预热 + 实时通道启动 (AppLayout 为唯一启动点)
  function startPolling() {
    presetStore.loadPresets()
    startRealtimePolling()
  }

  async function selectTask(taskId: string) {
    selectedTaskId.value = taskId
    const task = tasks.value.find((t) => t.id === taskId)
    if (task) updateMetricsData(task)
    // 切换任务: 重置水位并拉取该任务日志 (初始页 + 追平)
    await fetchTaskLogs(taskId, { reset: true })
  }

  async function fetchTaskById(taskId: string): Promise<NormalizedTask | null> {
    if (!taskId) return null
    try {
      const res = await tasksApi.get(taskId)
      if (!res.data) return null
      const task = normalizeTask(res.data)
      const idx = tasks.value.findIndex((t) => t.id === taskId)
      if (idx >= 0) tasks.value[idx] = normalizeTask({ ...tasks.value[idx], ...task })
      else tasks.value.unshift(task)
      return task
    } catch (err) {
      console.error(`获取任务详情 ${taskId} 异常:`, err)
      return null
    }
  }

  // 3. 任务控制 API (refreshAfterAction = 刷新列表 + 重启动态轮询的公共收尾)
  async function refreshAfterAction() {
    await loadTasks()
    scheduleNextPoll()
  }

  async function controlTask(
    taskId: string,
    label: string,
    api: (id: string) => Promise<unknown>,
    toast: () => void,
  ): Promise<boolean> {
    try {
      await api(taskId)
      toast()
      await refreshAfterAction()
      return true
    } catch (err) {
      message.error(`${label}失败: ${errDetail(err)}`)
      return false
    }
  }

  function pauseTask(taskId: string) {
    return controlTask(taskId, '暂停任务', tasksApi.pause, () => message.warning('任务已暂停'))
  }
  function cancelTask(taskId: string) {
    return controlTask(taskId, '取消任务', tasksApi.cancel, () => message.info('任务已取消'))
  }
  async function handleRecoveryError(label: string, err: unknown): Promise<false> {
    const detail = errDetail(err)
    const status = (err as { response?: { status?: number } } | null)?.response?.status
    if (status === 409) {
      message.warning(detail)
      await refreshAfterAction()
    } else {
      message.error(`${label}失败: ${detail}`)
    }
    return false
  }

  async function retryTask(taskId: string) {
    try {
      const res = await tasksApi.retry(taskId)
      message.success(res.data?.message || '任务已重新进入调度队列')
      await refreshAfterAction()
      return true
    } catch (err) {
      return handleRecoveryError('重试任务', err)
    }
  }

  async function resumeTask(taskId: string, mode: 'normal' | 'steady' = 'normal') {
    try {
      const res = await tasksApi.resume(taskId, mode)
      const data = (res.data || {}) as { queued_first?: boolean; message?: string }
      if (data.queued_first) {
        message.warning(data.message || '当前并发已满，已将任务置于就绪队列首位，空闲时优先运行')
      } else if (data.message) {
        message.success(data.message)
      } else if (mode === 'steady') {
        message.success('已以低速稳态恢复运行')
      } else {
        message.success('任务已恢复运行')
      }
      await refreshAfterAction()
      return true
    } catch (err) {
      return handleRecoveryError('恢复任务', err)
    }
  }

  async function skipCurrentFile(taskId: string, fileId?: string | null, reason?: string) {
    try {
      const res = await tasksApi.skipCurrent(taskId, { file_id: fileId, reason })
      const data = (res.data || {}) as { message?: string }
      message.success(data.message || '已跳过当前文件并留置占位卡')
      await refreshAfterAction()
      return true
    } catch (err) {
      message.error(`跳过文件失败: ${errDetail(err)}`)
      return false
    }
  }

  async function pauseQueue() {
    return queueAction('暂停队列', tasksApi.queuePause, '已暂停整个队列', (m) => message.warning(m))
  }

  async function resumeQueue() {
    return queueAction('恢复队列', tasksApi.queueResume, '已恢复队列调度', (m) => message.success(m))
  }

  async function queueAction(
    label: string,
    api: () => Promise<{ data?: { message?: string } }>,
    fallback: string,
    toast: (msg: string) => void,
  ) {
    try {
      const res = await api()
      toast(res.data?.message || fallback)
      await refreshAfterAction()
      return res.data
    } catch (err) {
      message.error(`${label}失败: ${errDetail(err)}`)
      return null
    }
  }

  async function deleteTask(taskId: string, deleteFiles = false) {
    try {
      const res = await tasksApi.delete(taskId, deleteFiles)
      if (res.data?.files_deleted?.length) {
        message.info(`已同步删除 ${res.data.files_deleted.length} 个断点文件`)
      }
      for (const s of res.data?.files_skipped || []) {
        message.warning(`文件跳过删除: ${s.reason}`)
      }
      message.info('任务记录已删除')
      if (selectedTaskId.value === taskId) selectedTaskId.value = null
      await refreshAfterAction()
    } catch (err) {
      message.error(`删除任务失败: ${errDetail(err)}`)
    }
  }

  async function batchAction(action: string, taskIds: string[], deleteFiles = false) {
    if (!taskIds || taskIds.length === 0) return null
    try {
      const payload: BatchTaskActionRequest = { action, task_ids: taskIds }
      if (action === 'delete') payload.delete_files = deleteFiles
      const res = await tasksApi.batch(payload)
      const data = res.data
      const actionNames: Record<string, string> = {
        pause: '暂停',
        resume: '恢复',
        cancel: '取消',
        delete: '删除',
        retry: '重试',
      }
      const summary = `批量${actionNames[action] || action}完成，成功 ${data?.success_count ?? 0}/${data?.total ?? 0}`
      if (data?.results.some((result) => !result.success)) message.warning(summary)
      else message.success(summary)
      await refreshAfterAction()
      return data
    } catch (err) {
      message.error(`批量操作失败: ${errDetail(err)}`)
      return null
    }
  }

  async function clearFinishedTasks() {
    try {
      await tasksApi.clearFinished()
      message.success('已清理结束任务')
      selectedTaskId.value = null
      await refreshAfterAction()
    } catch (err) {
      message.error(`清理失败: ${errDetail(err)}`)
    }
  }

  // 4. 预检 / 测算 / 提交 API (统一返回契约 TaskSubmitResult; 公共管线: 提交 → toast + 选中新任务 → 刷新列表)
  async function precheckAccess(accountId: number | null, chatIdentifier: string) {
    try {
      const res = await tasksApi.precheckAccess({ account_id: accountId, chat_identifier: chatIdentifier })
      return res.data
    } catch (err) {
      console.error('权限预检失败:', err)
      return { accessible: false, reason: errDetail(err, '权限预检异常') }
    }
  }

  async function inspectDownloadRange(
    channelIdentifier: string,
    startId: number | string,
    endId: number | string,
    showSuccess = true,
  ) {
    try {
      const res = await downloadApi.inspectRange({
        channel_identifier: channelIdentifier,
        start_id: parseInt(String(startId)),
        end_id: parseInt(String(endId)),
      })
      if (showSuccess) message.success('频道消息范围预检完成！')
      return res.data
    } catch (err) {
      message.error(`范围预检失败: ${errDetail(err)}`)
      return null
    }
  }

  async function inspectFolder(
    folderPath: string,
    targetChannel: string | null = null,
    accountId: number | null = null,
  ) {
    try {
      const payload: InspectFolderRequest = { folder_path: folderPath }
      if (targetChannel) payload.target_channel = targetChannel
      if (accountId !== null && accountId !== undefined) payload.account_id = accountId
      const res = await publishApi.inspectFolder(payload)
      message.success('文件夹扫描与大小合规检测完成！')
      return res.data
    } catch (err) {
      message.error(`扫描失败: ${errDetail(err)}`)
      return null
    }
  }

  async function inspectChannelInfo(sourceChat: string) {
    try {
      const res = await forwardApi.inspect(sourceChat)
      message.success('频道测算完成！')
      return (res.data as { data?: unknown })?.data
    } catch (err) {
      message.error(`测算失败: ${errDetail(err)}`)
      return null
    }
  }

  // 5. 业务提交 API: 四大提交函数共享 submitTask 公共管线 (toast + 选中新任务 + 刷新列表)
  async function submitTask(
    request: () => Promise<{ data?: { task_id?: string } }>,
    successMsg: string,
  ): Promise<TaskSubmitResult> {
    try {
      const res = await request()
      const taskId = res.data?.task_id ?? null
      message.success(successMsg)
      selectedTaskId.value = taskId
      await refreshAfterAction()
      if (taskId) {
        // 补走共享选择操作 (selectTask 含日志流重置) —— 直接赋值会绕过
        // reset, 前一任务的水位会拒掉新任务的初始日志 (刷新页已含新任务时尤甚)
        await selectTask(taskId)
      }
      return { success: true, taskId }
    } catch (err) {
      if (isAxiosError<DownloadTaskConflictResponse>(err) && err.response?.status === 409) {
        const conflict = err.response.data?.detail
        if (
          conflict?.code === 'already_downloaded' ||
          (conflict?.code === 'duplicate_download_task' && conflict.task_id)
        ) {
          return { success: false, taskId: null, downloadConflict: conflict }
        }
      }
      const detail = errDetail(err)
      message.error(`提交失败: ${detail}`)
      return { success: false, taskId: null, error: detail }
    }
  }

  async function submitLinkDownload(
    links: string,
    accountId: number | null = null,
    priority: number | null = 10,
    threads: number | string | null = 8,
    force = false,
  ): Promise<TaskSubmitResult> {
    const payload: DownloadLinkRequest = { links }
    if (accountId != null) payload.account_id = accountId
    if (priority != null) payload.priority = priority
    if (threads) payload.threads = parseInt(String(threads))
    if (force) payload.force = true
    return submitTask(() => downloadApi.submitLink(payload), '链接极速下载任务提交成功！')
  }

  async function submitRangeDownload(
    channel: string,
    startId: number | string,
    endId: number | string,
    accountId: number | null = null,
    priority: number | null = 10,
    threads: number | string | null = 8,
    force = false,
  ): Promise<TaskSubmitResult> {
    const payload: DownloadRangeRequest = {
      channel_identifier: channel,
      start_id: parseInt(String(startId)),
      end_id: parseInt(String(endId)),
    }
    if (accountId != null) payload.account_id = accountId
    if (priority != null) payload.priority = priority
    if (threads) payload.threads = parseInt(String(threads))
    if (force) payload.force = true
    return submitTask(() => downloadApi.submitRange(payload), '范围抓取下载任务提交成功！')
  }

  async function submitFolderPublish(
    folder: string,
    target: string,
    albumMode: boolean,
    delay: number | string,
    accountId: number | null = null,
    priority: number | null = 10,
    uploadConcurrency: number | string | null = null,
    uploadInFlight: number | string | null = null,
    forceRepublish = false,
    oversizePolicy: string | null = null,
    sendPlaceholderCard = true,
  ): Promise<TaskSubmitResult> {
    const payload: PublishFolderRequest = {
      folder_path: folder,
      target_channel: target,
      album_mode: albumMode,
      delay_seconds: parseFloat(String(delay)),
      force_republish: Boolean(forceRepublish),
      send_placeholder_card: Boolean(sendPlaceholderCard),
    }
    if (accountId != null) payload.account_id = accountId
    if (priority != null) payload.priority = priority
    if (oversizePolicy) payload.oversize_policy = oversizePolicy as PublishFolderRequest['oversize_policy']
    if (uploadConcurrency != null && uploadConcurrency !== '') payload.upload_concurrency = Number(uploadConcurrency)
    if (uploadInFlight != null && uploadInFlight !== '')
      payload.upload_in_flight_per_connection = Number(uploadInFlight)
    return submitTask(() => publishApi.submitFolder(payload), '本地文件夹发布任务已启动！')
  }

  async function submitChannelForward(
    source: string,
    target: string,
    startId: number | string | null,
    endId: number | string | null,
    batchSize: number | string,
    delay: number | string,
    options: { fullRetransfer?: boolean; dryRun?: boolean; accountId?: number | null; priority?: number | null } = {},
  ): Promise<TaskSubmitResult> {
    const payload: ForwardChannelRequest = {
      source_chat: source,
      target_chat: target,
      batch_size: parseInt(String(batchSize)),
      delay_seconds: parseFloat(String(delay)),
    }
    if (startId) payload.start_id = parseInt(String(startId))
    if (endId) payload.end_id = parseInt(String(endId))
    if (options.fullRetransfer) payload.full_retransfer = true
    if (options.dryRun) payload.dry_run = true
    if (options.accountId != null) payload.account_id = options.accountId
    if (options.priority != null) payload.priority = options.priority
    return submitTask(() => forwardApi.submit(payload), '云端极速转发任务已启动！')
  }

  async function updateTaskConcurrency(
    taskId: string,
    concurrency: number | string,
    uploadInFlight: number | string | null = null,
  ): Promise<boolean> {
    try {
      const payload: UpdateTaskConcurrencyRequest = { concurrency: parseInt(String(concurrency)) }
      if (uploadInFlight != null && uploadInFlight !== '')
        payload.upload_in_flight_per_connection = parseInt(String(uploadInFlight))
      await tasksApi.updateConcurrency(taskId, payload)
      let msg = `任务并发数已更新为 ${concurrency}`
      if (payload.upload_in_flight_per_connection) msg += ` (在途分块数: ${payload.upload_in_flight_per_connection})`
      message.success(msg)
      // 本地先行补丁 (刷新列表前 UI 立即生效)
      const task = tasks.value.find((t) => t.id === taskId)
      if (task) {
        task.progress.concurrency_total = parseInt(String(concurrency))
        if (task.params) {
          const isUploadTask = ['publish_folder', 'pikpak_publish', 'publish'].includes(task.task_type)
          if (isUploadTask) {
            task.params.upload_concurrency = parseInt(String(concurrency))
            if (payload.upload_in_flight_per_connection) {
              task.params.upload_in_flight_per_connection = payload.upload_in_flight_per_connection as number
            }
          } else {
            task.params.threads = parseInt(String(concurrency))
          }
        }
      }
      await refreshAfterAction()
      return true
    } catch (err) {
      message.error(`调整并发失败: ${errDetail(err)}`)
      return false
    }
  }

  return {
    // 列表与统计
    tasks,
    totalTasks,
    statusCounts,
    currentPage,
    pageSize,
    taskFilters,
    selectedTaskId,
    selectedTask,
    totalRunningSpeed,
    // 实时通道与日志 (taskRealtime 协作模块表面)
    currentMetrics,
    currentLogs,
    wsStatus,
    fetchTaskLogs,
    fetchAllLogs,
    initWebSocket,
    scheduleNextPoll,
    // 生命周期与查询
    startPolling,
    stopPolling,
    loadTasks,
    selectTask,
    fetchTaskById,
    // 控制与批量
    pauseTask,
    resumeTask,
    skipCurrentFile,
    pauseQueue,
    resumeQueue,
    cancelTask,
    retryTask,
    deleteTask,
    batchAction,
    clearFinishedTasks,
    updateTaskConcurrency,
    // 预检 / 测算 / 提交
    precheckAccess,
    inspectDownloadRange,
    inspectFolder,
    inspectChannelInfo,
    submitLinkDownload,
    submitRangeDownload,
    submitFolderPublish,
    submitChannelForward,
  }
})
