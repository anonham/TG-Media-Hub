import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import { ref } from 'vue'
import { createPinia, setActivePinia } from 'pinia'
import { createTaskRealtime } from '../taskRealtime'
import { botsApi, tasksApi, telegramAuthApi } from '@/api'
import { useTelegramAuthStore } from '../telegramAuthStore'
import { useAccountStore } from '../accountStore'

vi.mock('@/utils/discreteApi', () => ({
  message: { success: vi.fn(), error: vi.fn(), info: vi.fn(), warning: vi.fn() },
  dialog: { warning: vi.fn() },
}))

vi.mock('@/api', () => ({
  tasksApi: { getLogs: vi.fn() },
  authApi: { issueMediaTicket: vi.fn(async () => ({ data: { ticket: 't' } })) },
  telegramAuthApi: { getStatus: vi.fn(async () => ({ data: {} })) },
  botsApi: { list: vi.fn() },
}))

const mockedGetLogs = vi.mocked(tasksApi.getLogs)

interface FakeTask {
  status: string
}

function makeHandle(tasks: FakeTask[]) {
  setActivePinia(createPinia())
  const ctx = {
    tasks: ref(tasks as never[]),
    selectedTaskId: ref<string | null>(null),
    loadTasks: vi.fn(async () => {}),
    onTaskCompleted: vi.fn(),
  }
  const handle = createTaskRealtime(ctx)
  return { handle, ctx }
}

describe('taskRealtime 三档自适应轮询 (vitest 假时钟)', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
  })

  it('WS 断连 → 4s 快速短轮询降级接管', async () => {
    const { handle, ctx } = makeHandle([])
    handle.startPolling()
    await vi.advanceTimersByTimeAsync(0)
    expect(ctx.loadTasks).toHaveBeenCalledTimes(1)

    await vi.advanceTimersByTimeAsync(3999)
    expect(ctx.loadTasks).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1)
    expect(ctx.loadTasks).toHaveBeenCalledTimes(2)

    await vi.advanceTimersByTimeAsync(4000)
    expect(ctx.loadTasks).toHaveBeenCalledTimes(3)
    handle.stopPolling()
  })

  it('WS 在线且有活跃任务 → 20s 水位差量兜底', async () => {
    const { handle, ctx } = makeHandle([{ status: 'running' }])
    handle.wsStatus.value = 'connected'
    handle.startPolling()
    await vi.advanceTimersByTimeAsync(0)
    expect(ctx.loadTasks).toHaveBeenCalledTimes(1)

    await vi.advanceTimersByTimeAsync(19999)
    expect(ctx.loadTasks).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1)
    expect(ctx.loadTasks).toHaveBeenCalledTimes(2)
    handle.stopPolling()
  })

  it('WS 在线且无活跃任务 → 60s 极低频休眠轮询', async () => {
    const { handle, ctx } = makeHandle([{ status: 'completed' }])
    handle.wsStatus.value = 'connected'
    handle.startPolling()
    await vi.advanceTimersByTimeAsync(0)
    expect(ctx.loadTasks).toHaveBeenCalledTimes(1)

    await vi.advanceTimersByTimeAsync(59999)
    expect(ctx.loadTasks).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1)
    expect(ctx.loadTasks).toHaveBeenCalledTimes(2)
    handle.stopPolling()
  })

  it('pending 亦计为活跃任务; 终态族均走 60s 档', async () => {
    const pending = makeHandle([{ status: 'pending' }])
    pending.handle.wsStatus.value = 'connected'
    pending.handle.startPolling()
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(19999)
    expect(pending.ctx.loadTasks).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1)
    expect(pending.ctx.loadTasks).toHaveBeenCalledTimes(2)
    pending.handle.stopPolling()

    const cancelled = makeHandle([{ status: 'cancelled' }])
    cancelled.handle.wsStatus.value = 'connected'
    cancelled.handle.startPolling()
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(59999)
    expect(cancelled.ctx.loadTasks).toHaveBeenCalledTimes(1)
    cancelled.handle.stopPolling()
  })
})

describe('taskRealtime 日志流竞态', () => {
  class FakeWebSocket {
    static instances: FakeWebSocket[] = []
    onopen: (() => void) | null = null
    onmessage: ((event: MessageEvent) => void) | null = null
    onclose: ((e: CloseEvent) => void) | null = null
    onerror: ((err: unknown) => void) | null = null
    readyState = 1
    constructor(_url: string) {
      FakeWebSocket.instances.push(this)
    }
  }

  beforeEach(() => {
    setActivePinia(createPinia())
    mockedGetLogs.mockReset()
    vi.mocked(telegramAuthApi.getStatus)
      .mockReset()
      .mockResolvedValue({ data: {} } as never)
    vi.mocked(botsApi.list)
      .mockReset()
      .mockResolvedValue({ data: [] } as never)
    FakeWebSocket.instances = []
    vi.stubGlobal('WebSocket', FakeWebSocket as unknown as typeof WebSocket)
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
    vi.unstubAllGlobals()
  })

  function wsLog(seq: number, message: string) {
    return { type: 'log', seq, time: 't', message }
  }

  function startupStatus(phase: 'initializing' | 'ready' | 'degraded') {
    return {
      data: {
        auth_state: 'AUTHENTICATED',
        is_connected: true,
        startup: {
          phase,
          scheduler_ready: phase !== 'initializing',
          ready_bots: phase === 'ready' ? 1 : 0,
          total_bots: 1,
          unready_bots: [],
        },
      },
    } as never
  }

  it.each(['ready', 'degraded'] as const)(
    'stops polling and refreshes Bot list when startup becomes %s without a socket',
    async (phase) => {
      const { handle } = makeHandle([])
      const status = vi.mocked(telegramAuthApi.getStatus)
      status.mockResolvedValueOnce(startupStatus('initializing')).mockResolvedValueOnce(startupStatus(phase))

      handle.startPolling()
      await vi.advanceTimersByTimeAsync(0)
      expect(useTelegramAuthStore().startupStatus?.phase).toBe('initializing')
      expect(status).toHaveBeenCalledWith(false, expect.any(AbortSignal))
      expect(botsApi.list).not.toHaveBeenCalled()

      await vi.advanceTimersByTimeAsync(2000)
      expect(useTelegramAuthStore().startupStatus?.phase).toBe(phase)
      expect(botsApi.list).toHaveBeenCalledTimes(1)
      await vi.advanceTimersByTimeAsync(10000)
      expect(status).toHaveBeenCalledTimes(2)
      handle.stopPolling()
    },
  )

  it('reloads bots when a concurrent manual refresh records the terminal startup state', async () => {
    const { handle } = makeHandle([])
    const status = vi.mocked(telegramAuthApi.getStatus)
    let resolvePoll!: (data: never) => void
    status
      .mockResolvedValueOnce(startupStatus('initializing'))
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolvePoll = resolve
          }),
      )
      .mockResolvedValueOnce(startupStatus('ready'))

    handle.startPolling()
    await vi.advanceTimersByTimeAsync(2000)
    const accountStore = useAccountStore()
    vi.mocked(botsApi.list).mockResolvedValueOnce({
      data: [{ id: 1, status: 'unauthenticated', is_active: true }],
    } as never)
    await accountStore.loadBots()
    await useTelegramAuthStore().fetchStatus(false)
    expect(accountStore.onlineBotsCount).toBe(0)

    vi.mocked(botsApi.list).mockResolvedValueOnce({ data: [{ id: 1, status: 'ready', is_active: true }] } as never)
    resolvePoll(startupStatus('initializing'))
    await vi.advanceTimersByTimeAsync(10000)
    expect(useTelegramAuthStore().startupStatus?.phase).toBe('ready')
    expect(accountStore.onlineBotsCount).toBe(1)
    expect(botsApi.list).toHaveBeenCalledTimes(2)
    expect(status).toHaveBeenCalledTimes(3)
    handle.stopPolling()
  })

  it('updates immediately on startup completion and drops the older status response', async () => {
    const { handle } = makeHandle([])
    const status = vi.mocked(telegramAuthApi.getStatus)
    status.mockResolvedValueOnce(startupStatus('initializing'))
    let resolveOldStatus!: (data: never) => void
    status.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveOldStatus = resolve
        }),
    )
    status.mockResolvedValueOnce(startupStatus('ready'))
    vi.mocked(botsApi.list).mockResolvedValueOnce({ data: [{ id: 1, status: 'ready', is_active: true }] } as never)

    handle.startPolling()
    await handle.initWebSocket()
    await vi.advanceTimersByTimeAsync(2000)
    expect(useTelegramAuthStore().startupStatus?.phase).toBe('initializing')
    const socket = FakeWebSocket.instances.at(-1)!
    socket.onmessage?.(
      new MessageEvent('message', {
        data: JSON.stringify({ type: 'system_event', event: 'telegram_startup_changed' }),
      }),
    )
    await vi.advanceTimersByTimeAsync(0)
    expect(useTelegramAuthStore().startupStatus?.phase).toBe('ready')
    expect(useAccountStore().onlineBotsCount).toBe(1)
    expect(status.mock.calls[1][1]?.aborted).toBe(true)

    resolveOldStatus(startupStatus('initializing'))
    await vi.advanceTimersByTimeAsync(10000)
    expect(useTelegramAuthStore().startupStatus?.phase).toBe('ready')
    expect(status).toHaveBeenCalledTimes(3)
    handle.stopPolling()
  })

  it('回填进行中到达的 WS 帧与后续回填页零丢失且保持 seq 有序 (#15)', async () => {
    const { handle } = makeHandle([{ status: 'running' }])
    let releasePage1!: (v: unknown) => void
    handle.startPolling()
    await vi.advanceTimersByTimeAsync(0)
    await handle.initWebSocket()
    const socket = FakeWebSocket.instances.at(-1)!
    mockedGetLogs.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          releasePage1 = resolve
        }) as never,
    )
    const backfill = handle.fetchTaskLogs('task-1', { reset: true })
    // 回填第一页挂起期间, WS 实时帧 seq 501 先到 (旧实现会把水位抬到 501, 使 1-500 整页拒收并永久丢失)
    socket.onmessage?.({ data: JSON.stringify(wsLog(501, 'live')) } as MessageEvent)
    releasePage1({
      data: {
        task_id: 'task-1',
        logs: Array.from({ length: 500 }, (_, i) => wsLog(i + 1, `m${i + 1}`)),
        has_more: false,
        last_seq: 500,
      },
    })
    await backfill
    handle.stopPolling()

    const logs = handle.currentLogs.value
    expect(logs).toHaveLength(501)
    expect(logs.some((l) => l.seq === 1 && l.message === 'm1')).toBe(true)
    expect(logs.some((l) => l.seq === 500)).toBe(true)
    expect(logs.some((l) => l.message === 'live')).toBe(true)
    for (let i = 1; i < logs.length; i++) {
      expect(logs[i].seq ?? 0).toBeGreaterThan(logs[i - 1].seq ?? 0)
    }
    // 回填分页游标自 0 起, 不被实时帧顶飞
    expect(mockedGetLogs.mock.calls[0][1]).toBe(0)
  })

  it('sorts a late backfill before trimming a full buffer', async () => {
    const { handle } = makeHandle([])
    mockedGetLogs
      .mockResolvedValueOnce({
        data: {
          logs: Array.from({ length: 2000 }, (_, i) => wsLog(i + 1002, 'existing')),
          has_more: true,
          last_seq: 3001,
        },
      } as never)
      .mockResolvedValueOnce({
        data: {
          logs: Array.from({ length: 500 }, (_, i) => wsLog(i + 1, 'late')),
          has_more: false,
          last_seq: 500,
        },
      } as never)
    await handle.fetchTaskLogs('task-1', { reset: true })
    expect(handle.currentLogs.value.map((entry) => entry.seq)).toEqual(Array.from({ length: 2000 }, (_, i) => i + 1002))
  })

  it('跨任务切换使在途旧响应作废, 不串写新任务视图 (#16)', async () => {
    const { handle } = makeHandle([])
    let releaseA!: (v: unknown) => void
    mockedGetLogs.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          releaseA = resolve
        }) as never,
    )
    const pA = handle.fetchTaskLogs('task-a', { reset: true })
    mockedGetLogs.mockResolvedValueOnce({
      data: { task_id: 'task-b', logs: [wsLog(2, 'B 的日志')], has_more: false, last_seq: 2 },
    } as never)
    const pB = handle.fetchTaskLogs('task-b', { reset: true })
    releaseA({ data: { task_id: 'task-a', logs: [wsLog(1, 'A 的日志')] } })
    await Promise.all([pA, pB])

    const logs = handle.currentLogs.value
    expect(logs).toHaveLength(1)
    expect(logs[0].message).toBe('B 的日志')
    // A 的响应未写入 B 的视图, 也未推进 B 的水位
    expect(logs.some((l) => l.message === 'A 的日志')).toBe(false)
  })

  it('环形缓冲满载时分页仍按服务端游标推进 (#17)', async () => {
    const { handle } = makeHandle([])
    const page = (start: number, count: number, more: boolean) => ({
      data: {
        task_id: 'task-1',
        logs: Array.from({ length: count }, (_, i) => wsLog(start + i, `m${start + i}`)),
        has_more: more,
        // last_seq = 页内最大 seq (inclusive 语义, 与 fetchAllLogs 的游标推进一致)
        last_seq: start + count - 1,
      },
    })
    mockedGetLogs.mockResolvedValueOnce(page(1, 900, true) as never)
    mockedGetLogs.mockResolvedValueOnce(page(901, 900, true) as never)
    mockedGetLogs.mockResolvedValueOnce(page(1801, 900, false) as never)

    await handle.fetchTaskLogs('task-1', { reset: true })

    // 旧实现: 缓冲满 2000 后长度不再增长 → 每调用只拉 1 页; 新实现: 游标 0→900→1800 连续推进
    expect(mockedGetLogs).toHaveBeenCalledTimes(3)
    expect(mockedGetLogs.mock.calls[1][1]).toBe(900)
    expect(mockedGetLogs.mock.calls[2][1]).toBe(1800)
    // trim 后保留最新 2000 条 (seq 701-2700), 早于 701 的被裁剪属预期
    expect(handle.currentLogs.value).toHaveLength(2000)
    expect(handle.currentLogs.value.some((l) => l.seq === 2700)).toBe(true)
    expect(handle.currentLogs.value.some((l) => l.seq === 701)).toBe(true)
    expect(handle.currentLogs.value.some((l) => l.seq === 700)).toBe(false)
  })
})
