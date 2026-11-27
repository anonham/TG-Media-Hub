// 任务实时通道协作模块: 自 taskStore 抽离的 WebSocket 状态机、
// 100ms 进度微批节流缓冲、三维自适应轮询调度器、增量日志环形缓冲与顶部实时指标模型。
// 非独立 Pinia Store —— 由 taskStore 以上下文注入方式实例化 (createTaskRealtime),
// 任务域公共表面保持单一 Store, 消费方零感知; 本模块持有全部传输层状态与鉴权联动。
import { ref, watch, type Ref } from 'vue'
import { tasksApi, authApi } from '@/api'
import { applyTaskProgressPatch, type NormalizedTask } from '@/utils/taskNormalizer'
import { useAccountStore } from './accountStore'
import { useAuthStore } from './authStore'
import { useTelegramAuthStore } from './telegramAuthStore'

// WS 单帧载荷 (app/api/websockets.py): type ∈ progress | log | task_update | system_event
type WsFrame = Record<string, unknown>

// 增量日志环形缓冲条目 (seq 为服务端水位序号, 服务端旧版本可能缺省)
export interface TaskLogEntry {
  seq?: number
  time: string
  message: string
}

// 顶部实时指标卡模型 (DashboardView 消费)
export interface TaskHeaderMetrics {
  name: string
  percent: number
  speed: number
  eta: number
  threads: string
  dc: number | string
  item: string
  status: string
  floodWait: number
}

export interface TaskRealtimeContext {
  /** 任务列表引用: 轮询活跃度判定 / task_update 原地补丁 / progress 微批回写 */
  tasks: Ref<NormalizedTask[]>
  /** 当前选中任务 id: progress 指标聚焦与 log 帧过滤 */
  selectedTaskId: Ref<string | null>
  /** 任务列表刷新 (轮询 tick 与 task_update / system_event 后重拉) */
  loadTasks: () => Promise<void>
  /** task_update 广播 status=completed 时的业务侧回调 (PikPak 缓存清理) */
  onTaskCompleted: () => void
}

export interface TaskRealtimeHandle {
  wsStatus: Ref<string>
  currentLogs: Ref<TaskLogEntry[]>
  currentMetrics: Ref<TaskHeaderMetrics>
  fetchTaskLogs: (taskId: string | null, options?: { reset?: boolean }) => Promise<void>
  fetchAllLogs: (taskId: string) => Promise<TaskLogEntry[]>
  ensureLogsUpToDate: (task: NormalizedTask) => void
  resetLogStream: () => void
  resetMetrics: () => void
  updateMetricsData: (data: unknown) => void
  startPolling: () => void
  stopPolling: () => void
  scheduleNextPoll: (delayOverride?: number | null) => void
  initWebSocket: () => Promise<void>
}

const STARTUP_POLL_INTERVAL_MS = 2000

const EMPTY_METRICS: TaskHeaderMetrics = {
  name: '当前无运行中的传输任务',
  percent: 0,
  speed: 0,
  eta: 0,
  threads: '-',
  dc: '-',
  item: '-',
  status: '',
  floodWait: 0,
}

function num(value: unknown, fallback = 0): number {
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

/**
 * 实时通道工厂: 在 taskStore setup 内调用 (pinia 已激活, 内部 store 访问合法)。
 */
export function createTaskRealtime(ctx: TaskRealtimeContext): TaskRealtimeHandle {
  const authStore = useAuthStore()
  const tgAuthStore = useTelegramAuthStore()

  const wsStatus = ref('disconnected')
  const isPolling = ref(false)
  let pollTimeout: ReturnType<typeof setTimeout> | null = null
  let startupPollTimeout: ReturnType<typeof setTimeout> | null = null
  let telegramStatusRequest: AbortController | null = null
  let awaitingStartupBots = false
  let startupEventNeedsBots = false
  let wsReconnectTimer: ReturnType<typeof setTimeout> | null = null
  let ws: WebSocket | null = null
  let isVisibilityListenerAttached = false

  // ── 增量日志环形缓冲 ─────────────────────────────────────────
  // 水位跟踪: 只拉取 seq > lastLogSeq 的差量, WS 推送与轮询兜底共用同一口径 (防重/防丢)
  const LOG_BUFFER_LIMIT = 2000
  const currentLogs = ref<TaskLogEntry[]>([])
  let newestSeq = 0
  let logsFetching = false
  let logsTaskId: string | null = null
  let logsGeneration = 0
  let bufferedSeqs = new Set<number>()

  function trimLogBuffer() {
    if (currentLogs.value.length > LOG_BUFFER_LIMIT) {
      const dropped = currentLogs.value.slice(0, currentLogs.value.length - LOG_BUFFER_LIMIT)
      for (const d of dropped) {
        if (d.seq !== undefined) bufferedSeqs.delete(d.seq)
      }
      currentLogs.value = currentLogs.value.slice(-LOG_BUFFER_LIMIT)
    }
  }

  function pushLogEntries(entries: WsFrame[]) {
    if (!Array.isArray(entries) || entries.length === 0) return
    const startIndex = currentLogs.value.length
    for (const e of entries) {
      const rawSeq = e.seq
      let seq: number | undefined
      if (rawSeq !== undefined && rawSeq !== null) {
        const seqNum = Number(rawSeq)
        if (Number.isFinite(seqNum)) {
          seq = seqNum
          newestSeq = Math.max(newestSeq, seqNum)
        }
      }
      if (seq !== undefined) {
        // 双源去重: WS 实时帧与 REST 回填页按缓冲现存 seq 精确去重,
        // 不再共用游标水位 —— 回填分页不会被实时帧的单帧推进整页拒收
        if (bufferedSeqs.has(seq)) continue
        bufferedSeqs.add(seq)
      }
      currentLogs.value.push({ seq, time: String(e.time || '系统'), message: String(e.message || '') })
    }
    // 交错合并乱序检出: 回填页晚于更高 seq 的实时帧到达时, 恢复 seq
    // 单调展示; 顺序到达 (WS 流常态) 零排序开销
    if (startIndex > 0 && startIndex < currentLogs.value.length) {
      const prevTail = currentLogs.value[startIndex - 1]
      const firstAdded = currentLogs.value[startIndex]
      if ((firstAdded.seq ?? Number.MAX_SAFE_INTEGER) < (prevTail.seq ?? Number.MAX_SAFE_INTEGER)) {
        currentLogs.value.sort((a, b) => (a.seq ?? Number.MAX_SAFE_INTEGER) - (b.seq ?? Number.MAX_SAFE_INTEGER))
      }
    }
    trimLogBuffer()
  }

  function resetLogStream() {
    newestSeq = 0
    bufferedSeqs = new Set()
    currentLogs.value = []
  }

  async function fetchTaskLogs(taskId: string | null, { reset = false }: { reset?: boolean } = {}) {
    if (!taskId) return
    // 同任务防重入 (不换代际, 在途请求继续追平)
    if (logsFetching && logsTaskId === taskId) return
    // 请求按任务+代际隔离 —— 跨任务切换/重置使在途旧响应整体作废,
    // 不再被全局单旗拒掉或串写他任务视图
    const gen = ++logsGeneration
    if (reset || logsTaskId !== taskId) {
      logsTaskId = taskId
      resetLogStream()
    }
    logsFetching = true
    try {
      let pages = 0
      let after = newestSeq
      while (pages < 12) {
        const res = await tasksApi.getLogs(taskId, after, 500)
        if (gen !== logsGeneration) return
        const data = (res.data || {}) as { logs?: WsFrame[]; has_more?: boolean; last_seq?: number }
        pushLogEntries(data.logs || [])
        pages += 1
        // 分页进度用服务端游标推进判定 (环形缓冲满载 trim 后长度不再
        // 增长, 旧实现的长度判定会让回填/追平每调用只拉 1 页); 游标不推进即退出防死循环
        const nextAfter = data.last_seq ?? after
        if (!data.has_more || nextAfter <= after) break
        after = nextAfter
      }
    } catch (err) {
      console.error('增量拉取任务日志失败:', err)
    } finally {
      if (gen === logsGeneration) logsFetching = false
    }
  }

  // 导出用: 分页拉取服务端全量日志 (不受环形缓冲 2000 条限制)
  async function fetchAllLogs(taskId: string): Promise<TaskLogEntry[]> {
    const all: TaskLogEntry[] = []
    let after = 0
    for (let i = 0; i < 200; i++) {
      const res = await tasksApi.getLogs(taskId, after, 2000)
      const data = (res.data || {}) as { logs?: TaskLogEntry[]; has_more?: boolean; last_seq?: number }
      all.push(...(data.logs || []))
      if (!data.has_more) break
      after = data.last_seq ?? 0
    }
    return all
  }

  // 日志差量兜底: 服务端水位超过本地已见最高 seq 时补拉 (覆盖 WS 断连期间丢失的日志)
  function ensureLogsUpToDate(task: NormalizedTask) {
    if ((task.last_log_seq || 0) > newestSeq) fetchTaskLogs(task.id)
  }

  // ── 顶部实时指标模型 ─────────────────────────────────────────
  const currentMetrics = ref<TaskHeaderMetrics>({ ...EMPTY_METRICS })

  function resetMetrics() {
    currentMetrics.value = { ...EMPTY_METRICS }
  }

  // 入参仅两种形态: NormalizedTask (REST) 或 WS progress 帧 —— 嵌套 progress 为唯一轨道
  function updateMetricsData(data: unknown) {
    if (!data || typeof data !== 'object') {
      resetMetrics()
      return
    }
    const d = data as Record<string, unknown>
    const prog = (d.progress && typeof d.progress === 'object' ? d.progress : {}) as Record<string, unknown>
    const currentBytes = num(prog.processed_bytes)
    const totalBytes = num(prog.total_bytes)
    const percent =
      prog.percent !== undefined && prog.percent !== null
        ? num(prog.percent)
        : totalBytes > 0
          ? (currentBytes / totalBytes) * 100
          : 0
    const speed = num(prog.speed_mb_s)
    const eta = num(prog.eta_s)
    const item = prog.current_item !== undefined && prog.current_item !== null ? String(prog.current_item) : '-'
    const concurrency = num(prog.concurrency)
    const concurrencyTotal = num(prog.concurrency_total)
    const isUpload = ['publish', 'publish_folder', 'pikpak_publish'].includes(
      typeof d.task_type === 'string' ? d.task_type : '',
    )
    const unitLabel = isUpload ? '连接' : '通道'
    const threads = concurrency
      ? concurrencyTotal
        ? `${concurrency}/${concurrencyTotal} ${unitLabel}`
        : `${concurrency} ${unitLabel}`
      : '-'
    const dc = num(prog.dc_id)
    const taskType = typeof d.task_type === 'string' && d.task_type ? `[${d.task_type}] ` : ''
    const status = typeof d.status === 'string' && d.status ? `(${d.status})` : ''
    const floodWait =
      prog.flood_wait_seconds !== undefined && prog.flood_wait_seconds !== null ? num(prog.flood_wait_seconds) : 0

    currentMetrics.value = {
      name: `任务 ${taskType}${item} ${status}`.trim(),
      percent: Math.min(100, Math.max(0, parseFloat(percent.toFixed(1)))),
      speed: parseFloat(speed.toFixed(2)),
      eta: Math.ceil(eta),
      threads,
      dc,
      item,
      status,
      floodWait,
    }
  }

  // ── WebSocket 传输进度微批节流缓冲 (100ms 防卡顿) ─────────
  const progressBuffer = new Map<string, WsFrame>()
  let progressThrottleTimer: ReturnType<typeof setTimeout> | null = null

  function flushProgressBuffer() {
    progressThrottleTimer = null
    if (progressBuffer.size === 0) return

    for (const [taskId, data] of progressBuffer.entries()) {
      const target = ctx.tasks.value.find((t) => t.id === taskId)
      if (target) {
        // 单一归一轨道: 缺省字段保留现值
        applyTaskProgressPatch(target, data)
      }
      if (!ctx.selectedTaskId.value || ctx.selectedTaskId.value === taskId) {
        // 先应用补丁再从完整任务记录派生头部指标 —— 局部帧 (仅 speed 等)
        // 不再把百分比/ETA/当前项/状态清零; 记录不在列表时保留旧帧直通降级
        updateMetricsData(target ?? data)
      }
    }
    progressBuffer.clear()
  }

  function handleProgressMessage(data: WsFrame) {
    if (!data || !data.task_id) return
    const key = String(data.task_id)
    const prev = progressBuffer.get(key)
    if (prev) {
      // 100ms 微批窗口内同任务多帧按字段合并 —— 嵌套 progress 逐键保留
      // 最新值, 防后帧把先帧的字段 (如 processed_bytes) 整帧顶掉
      progressBuffer.set(key, {
        ...prev,
        ...data,
        progress: {
          ...(prev.progress as Record<string, unknown> | undefined),
          ...(data.progress as Record<string, unknown> | undefined),
        },
      } as WsFrame)
    } else {
      progressBuffer.set(key, data)
    }
    if (!progressThrottleTimer) {
      progressThrottleTimer = setTimeout(flushProgressBuffer, 100)
    }
  }

  // ── 三维自适应调度引擎 ─────────────────────────────────────────
  // 维度 1: 页面可见性 (Page Visibility API: 切走后台挂起, 切回前台立即刷新)
  // 维度 2: WebSocket 状态自适应 (断连/重连降级为 4s 快速短轮询)
  // 维度 3: 业务繁忙度退避 (WS 在线时: 活跃任务 20s 水位兜底, 终态/空闲 60s 休眠轮询)
  function getNextPollDelay(): number {
    if (wsStatus.value !== 'connected') {
      return 4000 // 维度 2: WS 断连/重连中，4 秒快速短轮询降级接管
    }
    const hasActiveTask = ctx.tasks.value.some((t) => t.status === 'running' || t.status === 'pending')
    if (hasActiveTask) {
      return 20000 // 维度 3: WS 在线且有活跃任务，20 秒慢轮询做 last_log_seq 水位差量兜底
    }
    return 60000 // 维度 3: WS 在线且无活跃任务，60 秒极低频休眠轮询
  }

  function clearPollTimer() {
    if (pollTimeout) {
      clearTimeout(pollTimeout)
      pollTimeout = null
    }
  }

  function scheduleNextPoll(delayOverride: number | null = null) {
    clearPollTimer()
    if (!isPolling.value) return
    if (typeof document !== 'undefined' && document.hidden) {
      // 维度 1: 页面处于后台/锁屏，彻底挂起定时器
      return
    }

    const delay = delayOverride !== null ? delayOverride : getNextPollDelay()
    pollTimeout = setTimeout(async () => {
      await ctx.loadTasks()
      scheduleNextPoll()
    }, delay)
  }

  function handleVisibilityChange() {
    if (document.hidden) {
      // 维度 1: 切走标签页/锁屏，挂起轮询
      clearPollTimer()
    } else {
      // 维度 1: 切回前台，立即主动刷新一次并重启动态调度
      if (isPolling.value) {
        clearPollTimer()
        ctx.loadTasks().then(() => {
          scheduleNextPoll()
        })
      }
    }
  }

  function startPolling() {
    if (isPolling.value) return
    isPolling.value = true
    void refreshTelegramStatus()

    if (typeof document !== 'undefined' && !isVisibilityListenerAttached) {
      document.addEventListener('visibilitychange', handleVisibilityChange)
      isVisibilityListenerAttached = true
    }

    // 首次立即刷新并启动动态调度
    ctx.loadTasks().then(() => {
      scheduleNextPoll()
    })
  }

  function stopPolling() {
    isPolling.value = false
    clearPollTimer()
    stopStartupStatusRefresh()
    awaitingStartupBots = false
    startupEventNeedsBots = false

    if (typeof document !== 'undefined' && isVisibilityListenerAttached) {
      document.removeEventListener('visibilitychange', handleVisibilityChange)
      isVisibilityListenerAttached = false
    }

    if (wsReconnectTimer) {
      clearTimeout(wsReconnectTimer)
      wsReconnectTimer = null
    }
    // 停机即取消挂起的进度微批 flush (与缓冲一并丢弃), 不再应用延迟更新
    if (progressThrottleTimer) {
      clearTimeout(progressThrottleTimer)
      progressThrottleTimer = null
      progressBuffer.clear()
    }
    if (ws) {
      try {
        ws.close()
      } catch {
        /* 已失效连接的关闭异常可安全忽略 */
      }
      ws = null
    }
    wsStatus.value = 'disconnected'
  }

  function stopStartupStatusRefresh() {
    if (startupPollTimeout !== null) clearTimeout(startupPollTimeout)
    startupPollTimeout = null
    telegramStatusRequest?.abort()
    telegramStatusRequest = null
  }

  async function refreshTelegramStatus({ probePremium = false, refreshBots = false } = {}) {
    stopStartupStatusRefresh()
    const request = new AbortController()
    telegramStatusRequest = request
    const previousPhase = tgAuthStore.startupStatus?.phase
    if (previousPhase === 'initializing') awaitingStartupBots = true
    if (refreshBots) startupEventNeedsBots = true
    const updated = await tgAuthStore.fetchStatus(probePremium, request.signal)
    if (request.signal.aborted || !isPolling.value || tgAuthStore.isRestricted) return

    telegramStatusRequest = null
    const phase = tgAuthStore.startupStatus?.phase
    if (phase === 'initializing') awaitingStartupBots = true
    if (
      phase &&
      phase !== 'initializing' &&
      (awaitingStartupBots || (updated && (startupEventNeedsBots || previousPhase !== phase)))
    ) {
      awaitingStartupBots = false
      startupEventNeedsBots = false
      void useAccountStore().loadBots()
    } else if (phase === 'initializing' || (!updated && (!phase || startupEventNeedsBots))) {
      startupPollTimeout = setTimeout(() => void refreshTelegramStatus(), STARTUP_POLL_INTERVAL_MS)
    }
  }

  // ── WebSocket 状态机 ─────────────────────────────────────────
  // 上次握手使用的底层凭证指纹: 供 reconnectWebSocket 判定凭证是否真变化
  let wsHandshakeCreds = ''
  let wsInitInFlight = false

  function currentWsCreds(): string {
    return `${authStore.apiKey || ''}|${tgAuthStore.get2FAToken() || ''}`
  }

  async function initWebSocket() {
    // 停机 (stopPolling) 后不得再重建连接
    if (!isPolling.value) return
    if (authStore.isAuthRequired && !authStore.apiKey) return
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return
    if (wsInitInFlight) return
    wsInitInFlight = true

    try {
      // F9: WS 握手凭短时效 ticket —— 每次连接/重连先取新票, 明文
      // token/tfa_token query 形态已收口; 取票失败静默降级为轮询接管 (轮询走双
      // Header 鉴权不受影响), 不 toast 以防断连风暴刷屏。
      let ticket = ''
      try {
        const res = await authApi.issueMediaTicket()
        ticket = res.data?.ticket || ''
      } catch (err) {
        console.error('WS 握手取票失败:', err)
      }
      // 取票 await 期间停机 → 放弃建连
      if (!isPolling.value) return
      if (!ticket) {
        wsStatus.value = 'disconnected'
        scheduleNextPoll()
        return
      }

      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
      const host = window.location.host
      const wsUrl = `${protocol}//${host}/ws/progress?ticket=${encodeURIComponent(ticket)}`
      wsHandshakeCreds = currentWsCreds()

      if (ws) {
        try {
          ws.close()
        } catch {
          /* 旧连接关闭异常可安全忽略 */
        }
      }

      wsStatus.value = 'connecting'
      const socket = new WebSocket(wsUrl)
      ws = socket
      attachSocketHandlers(socket)
    } finally {
      wsInitInFlight = false
    }
  }

  function attachSocketHandlers(socket: WebSocket) {
    // 回调先验 socket 身份与生命周期 —— 凭证重连的新旧 socket 交错时,
    // 陈旧 close 事件不得改写当前连接状态、打开鉴权弹窗或重启连接
    const isCurrentSocket = () => ws === socket && isPolling.value
    socket.onopen = () => {
      if (!isCurrentSocket()) return
      wsStatus.value = 'connected'
      if (wsReconnectTimer) {
        clearTimeout(wsReconnectTimer)
        wsReconnectTimer = null
      }
      // 重连/首连后探测账号状态: 覆盖断线期间被登出的场景 (AUTHENTICATED→UNAUTHENTICATED 转变触发横幅)
      void refreshTelegramStatus({ probePremium: tgAuthStore.startupStatus?.phase !== 'initializing' })
      // 断连期间可能丢失日志推送, 按水位补拉差量
      if (ctx.selectedTaskId.value) fetchTaskLogs(ctx.selectedTaskId.value)
      // WS 握手成功切回低频轮询调度
      scheduleNextPoll()
    }

    socket.onmessage = (event: MessageEvent) => {
      if (!isCurrentSocket()) return
      try {
        const data = JSON.parse(event.data) as WsFrame
        if (data.type === 'progress') {
          handleProgressMessage(data)
        } else if (data.type === 'log') {
          if (!ctx.selectedTaskId.value || ctx.selectedTaskId.value === data.task_id) {
            // seq 水位去重: 与轮询差量补拉共用口径, 交错到达不重复不丢行
            // (服务端旧版本无 seq 字段时直接 push, 向后兼容)
            pushLogEntries([data])
          }
        } else if (data.type === 'task_update') {
          if (data.task_id) {
            const t = ctx.tasks.value.find((item) => item.id === data.task_id)
            if (t) {
              if (data.status) t.status = String(data.status)
              if (data.account_id !== undefined && data.account_id !== null) t.account_id = Number(data.account_id)
              if (data.executor_names) t.executor_names = data.executor_names as string[]
              if (data.account_ids) t.account_ids = data.account_ids as number[]
            }
          }
          if (data.status === 'completed') {
            ctx.onTaskCompleted()
          }
          ctx.loadTasks().then(() => {
            scheduleNextPoll()
          })
        } else if (data.type === 'system_event') {
          // 全局系统事件 (非任务域): 账号会话失效等, 触发顶部横幅引导
          if (data.event === 'telegram_startup_changed') {
            void refreshTelegramStatus({ refreshBots: true })
          } else if (data.event === 'session_invalid') {
            tgAuthStore.reportSessionInvalid(String(data.message || 'Telegram 账号会话已失效，请重新绑定账号'))
            ctx.loadTasks().then(() => {
              scheduleNextPoll()
            })
          }
        }
      } catch (err) {
        console.error('解析 WS 消息失败:', err)
      }
    }

    socket.onclose = (e: CloseEvent) => {
      if (!isCurrentSocket()) return
      wsStatus.value = 'disconnected'
      if (e && e.code === 1008) {
        // F9: WS 握手仅凭短时效 ticket, 1008 = 取到的票被服务端拒收 (凭证层失效),
        // 走 AuthModal 重录; 2FA 会话失效的重验引导由 REST 403 拦截器承接
        authStore.openAuthModal('WebSocket 鉴权失败：访问口令无效')
        return
      }
      // 自动重连退避 (重连时 initWebSocket 先取新 ticket, 天然消除陈旧凭证窗口)
      if (!wsReconnectTimer) {
        wsReconnectTimer = setTimeout(() => {
          wsReconnectTimer = null
          initWebSocket()
        }, 3000)
      }
      // WS 断开时，降级为 4 秒快速短轮询接管
      scheduleNextPoll()
    }

    socket.onerror = (err) => {
      if (!isCurrentSocket()) return
      console.error('WebSocket 异常:', err)
    }
  }

  // 凭证变更显式重连: 替代仅依赖 isReady 布尔间接信号 —— 修复 OPEN 态无法刷新
  // 凭证与跨 Tab 重解锁的陈旧凭证窗口; 凭证未变或生命周期未启动时 no-op。
  function reconnectWebSocket() {
    if (authStore.isAuthRequired && !authStore.apiKey) return
    if (!isPolling.value) return
    const creds = currentWsCreds()
    if (!creds.split('|')[0]) return
    if (
      ws &&
      (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) &&
      creds === wsHandshakeCreds
    ) {
      return
    }
    if (ws) {
      try {
        ws.close()
      } catch {
        /* 旧连接关闭异常可安全忽略 */
      }
      ws = null
    }
    initWebSocket()
  }

  watch(
    () => authStore.apiKey,
    (newKey) => {
      if (newKey) reconnectWebSocket()
    },
  )

  watch(
    () => tgAuthStore.twoFAToken,
    (newToken) => {
      if (newToken) reconnectWebSocket()
    },
  )

  return {
    wsStatus,
    currentLogs,
    currentMetrics,
    fetchTaskLogs,
    fetchAllLogs,
    ensureLogsUpToDate,
    resetLogStream,
    resetMetrics,
    updateMetricsData,
    startPolling,
    stopPolling,
    scheduleNextPoll,
    initWebSocket,
  }
}
