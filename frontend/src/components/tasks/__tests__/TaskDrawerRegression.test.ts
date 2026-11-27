import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { nextTick } from 'vue'
import TaskDiagnosisTab from '../drawer/TaskDiagnosisTab.vue'
import TaskDetailDrawer from '../TaskDetailDrawer.vue'
import TaskMetricsHeader from '../TaskMetricsHeader.vue'
import TaskBatchActionBar from '../TaskBatchActionBar.vue'
import { useTaskStore } from '@/stores/taskStore'
import { forwardApi, storageApi } from '@/api'
import { dialog } from '@/utils/discreteApi'
import type { NormalizedTask } from '@/utils/taskNormalizer'

vi.mock('vue-router', () => ({
  useRouter: () => ({ push: vi.fn() }),
}))

vi.mock('@/api', () => ({
  forwardApi: { getCoverage: vi.fn(), getReceipt: vi.fn() },
  storageApi: { getFiles: vi.fn() },
}))

vi.mock('@/utils/discreteApi', () => ({
  dialog: { warning: vi.fn(), success: vi.fn() },
  message: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}))

function deferred<T>() {
  let resolve!: (val: T) => void
  const promise = new Promise<T>((r) => {
    resolve = r
  })
  return { promise, resolve }
}

function makeTask(id: string, taskType = 'download_link', concurrency = 8): NormalizedTask {
  return {
    id,
    task_type: taskType,
    status: 'running',
    created_at: 1000,
    updated_at: 1000,
    params: { threads: concurrency, upload_concurrency: concurrency, source_chat: 'src', target_chat: 'tgt' },
    progress: {
      current_bytes: 0,
      total_bytes: 100,
      speed_mbps: 0,
      concurrency_total: concurrency,
      concurrency_active: 1,
    },
    logs: [],
  } as unknown as NormalizedTask
}

function setViewportWidth(width: number) {
  Object.defineProperty(window, 'innerWidth', {
    configurable: true,
    writable: true,
    value: width,
  })
  window.dispatchEvent(new Event('resize'))
}

describe('Task components drawer and batch regressions', () => {
  const origInnerWidth = window.innerWidth

  beforeEach(() => {
    setActivePinia(createPinia())
    vi.clearAllMocks()
    setViewportWidth(1024)
  })

  afterEach(() => {
    setViewportWidth(origInnerWidth)
    vi.restoreAllMocks()
  })

  it('captures skip-current taskId at dialog open and ignores stale diagnosis responses', async () => {
    const taskStore = useTaskStore()
    const skipSpy = vi.spyOn(taskStore, 'skipCurrentFile').mockResolvedValue(true as never)

    const files1 = deferred<{ data: unknown[] }>()
    const files2 = deferred<{ data: unknown[] }>()
    vi.mocked(storageApi.getFiles)
      .mockReturnValueOnce(files1.promise as never)
      .mockReturnValueOnce(files2.promise as never)

    const t1 = makeTask('task-1', 'download_link')
    const t2 = makeTask('task-2', 'download_link')

    const wrapper = mount(TaskDiagnosisTab, {
      props: { task: t1 },
      global: {
        stubs: {
          CoverageBar: { template: '<div />' },
          NButton: { template: '<button @click="$emit(\'click\')"><slot /></button>' },
          NTable: { template: '<table><slot /></table>' },
          NTag: { template: '<span><slot /></span>' },
        },
      },
    })

    const vm = wrapper.vm as unknown as {
      handlePromptSkipCurrent: () => void
      relatedFiles: Array<{ path: string }>
      loadingRelatedFiles: boolean
    }

    vm.handlePromptSkipCurrent()
    await wrapper.setProps({ task: t2 })

    // Confirm skip dialog AFTER task prop changed to task-2
    const warningOpts = vi.mocked(dialog.warning).mock.calls[0][0] as { onPositiveClick: () => Promise<void> }
    await warningOpts.onPositiveClick()
    expect(skipSpy).toHaveBeenCalledWith('task-1')

    // Resolve task-2 first, then stale task-1 later
    files2.resolve({ data: [{ path: '/t2/file.mp4' }] })
    await flushPromises()
    expect(vm.relatedFiles).toEqual([{ path: '/t2/file.mp4' }])
    expect(vm.loadingRelatedFiles).toBe(false)

    files1.resolve({ data: [{ path: '/t1/stale.mp4' }] })
    await flushPromises()
    expect(vm.relatedFiles).toEqual([{ path: '/t2/file.mp4' }])

    wrapper.unmount()
  })

  it('rejects stale forward diagnosis responses on task switch', async () => {
    const cov1 = deferred<{ data: { data: Record<string, unknown> } }>()
    const rec1 = deferred<{ data: { data: Record<string, unknown> } }>()
    vi.mocked(forwardApi.getCoverage).mockReturnValueOnce(cov1.promise as never)
    vi.mocked(forwardApi.getReceipt).mockReturnValueOnce(rec1.promise as never)
    vi.mocked(storageApi.getFiles).mockResolvedValue({ data: [] } as never)

    const wrapper = mount(TaskDiagnosisTab, {
      props: { task: makeTask('fwd-1', 'forward_channel') },
      global: {
        stubs: {
          CoverageBar: { template: '<div />' },
          NButton: { template: '<button><slot /></button>' },
          NTable: { template: '<table><slot /></table>' },
          NTag: { template: '<span><slot /></span>' },
        },
      },
    })

    const vm = wrapper.vm as unknown as {
      forwardCoverageData: unknown
      forwardReceipt: unknown
      loadingCoverage: boolean
    }

    expect(vm.loadingCoverage).toBe(true)
    await wrapper.setProps({ task: makeTask('dl-2', 'download_link') })
    expect(vm.loadingCoverage).toBe(false)

    cov1.resolve({
      data: {
        data: { range: { min_id: 1, max_id: 10 }, covered: [], forwarded_runs: [], filtered_runs: [], bad_runs: [] },
      },
    })
    rec1.resolve({ data: { data: { forwarded: 5 } } })
    await flushPromises()

    expect(vm.forwardCoverageData).toBeNull()
    expect(vm.forwardReceipt).toBeNull()
    expect(vm.loadingCoverage).toBe(false)

    wrapper.unmount()
  })

  it('scopes TaskDetailDrawer singleTaskDetail, concurrency writes, and drawerWidth reactively', async () => {
    const taskStore = useTaskStore()
    vi.spyOn(taskStore, 'selectTask').mockResolvedValue(undefined as never)

    const detail1 = makeTask('t1', 'download_link', 8)
    const detail2 = makeTask('t2', 'publish_folder', 2)
    const fetch1 = deferred<NormalizedTask>()
    const fetch2 = deferred<NormalizedTask>()
    vi.spyOn(taskStore, 'fetchTaskById')
      .mockResolvedValueOnce(detail1)
      .mockReturnValueOnce(fetch2.promise as never)
      .mockReturnValueOnce(fetch1.promise as never)

    const concDeferred = deferred<boolean>()
    vi.spyOn(taskStore, 'updateTaskConcurrency').mockReturnValue(concDeferred.promise as never)

    const wrapper = mount(TaskDetailDrawer, {
      props: { show: true, taskId: 't1' },
      global: {
        stubs: {
          NDrawer: { props: ['show', 'width'], template: '<div :data-width="width"><slot /></div>' },
          NDrawerContent: { template: '<div><slot name="header" /><slot /></div>' },
          NTabs: { template: '<div><slot /></div>' },
          NTabPane: { template: '<div><slot /></div>' },
          NTag: { template: '<span><slot /></span>' },
          NButton: { template: '<button><slot /></button>' },
          NSpin: { template: '<div><slot /></div>' },
          NEmpty: { template: '<div />' },
          TaskLogTerminal: { template: '<div />' },
          TaskRealtimeMetricsTab: { template: '<div />' },
          TaskParamsMetadataTab: { template: '<div />' },
          TaskDiagnosisTab: { template: '<div />' },
        },
      },
    })

    await flushPromises()
    const vm = wrapper.vm as unknown as {
      drawerWidth: string
      task: NormalizedTask | null
      singleTaskDetail: NormalizedTask | null
      handleUpdateConcurrency: (val: number) => Promise<void>
      updatingConcurrency: boolean
    }

    // Reactive drawerWidth across breakpoint
    expect(vm.drawerWidth).toBe('680px')
    setViewportWidth(600)
    await nextTick()
    expect(vm.drawerWidth).toBe('100%')

    expect(vm.task?.id).toBe('t1')

    // Start concurrency update on t1, then immediately switch to t2 before response
    const updatePromise = vm.handleUpdateConcurrency(16)
    await wrapper.setProps({ taskId: 't2' })

    // While t2 is still loading, t1 fallback detail must NOT leak into `task`
    expect(vm.task).toBeNull()

    fetch2.resolve(detail2)
    await flushPromises()
    expect(vm.task?.id).toBe('t2')

    // Now t1's concurrency write resolves: must NOT touch detail2's concurrency
    concDeferred.resolve(true)
    await updatePromise
    expect(vm.singleTaskDetail?.id).toBe('t2')
    expect(vm.singleTaskDetail?.progress.concurrency_total).toBe(2)

    wrapper.unmount()
  })

  it('invalidates pre-switch concurrency response after switching away and back to origin task', async () => {
    const taskStore = useTaskStore()
    vi.spyOn(taskStore, 'selectTask').mockResolvedValue(undefined as never)

    const detail1Initial = makeTask('t1', 'download_link', 8)
    const detail2 = makeTask('t2', 'publish_folder', 2)
    const detail1Reloaded = makeTask('t1', 'download_link', 4)

    vi.spyOn(taskStore, 'fetchTaskById')
      .mockResolvedValueOnce(detail1Initial)
      .mockResolvedValueOnce(detail2)
      .mockResolvedValueOnce(detail1Reloaded)

    const staleT1Write = deferred<boolean>()
    vi.spyOn(taskStore, 'updateTaskConcurrency').mockReturnValueOnce(staleT1Write.promise as never)

    const wrapper = mount(TaskDetailDrawer, {
      props: { show: true, taskId: 't1' },
      global: {
        stubs: {
          NDrawer: { props: ['show', 'width'], template: '<div :data-width="width"><slot /></div>' },
          NDrawerContent: { template: '<div><slot name="header" /><slot /></div>' },
          NTabs: { template: '<div><slot /></div>' },
          NTabPane: { template: '<div><slot /></div>' },
          NTag: { template: '<span><slot /></span>' },
          NButton: { template: '<button><slot /></button>' },
          NSpin: { template: '<div><slot /></div>' },
          NEmpty: { template: '<div />' },
          TaskLogTerminal: { template: '<div />' },
          TaskRealtimeMetricsTab: { template: '<div />' },
          TaskParamsMetadataTab: { template: '<div />' },
          TaskDiagnosisTab: { template: '<div />' },
        },
      },
    })

    await flushPromises()
    const vm = wrapper.vm as unknown as {
      singleTaskDetail: NormalizedTask | null
      handleUpdateConcurrency: (val: number) => Promise<void>
      updatingConcurrency: boolean
    }

    // Start concurrency write on t1, then switch t1 -> t2 -> t1 before it settles
    const stalePromise = vm.handleUpdateConcurrency(16)
    await wrapper.setProps({ taskId: 't2' })
    await flushPromises()
    expect(vm.singleTaskDetail?.id).toBe('t2')

    await wrapper.setProps({ taskId: 't1' })
    await flushPromises()
    expect(vm.singleTaskDetail?.id).toBe('t1')
    expect(vm.singleTaskDetail?.progress.concurrency_total).toBe(4)

    // Pre-switch t1 response arrives late: must NOT overwrite reloaded t1 detail
    staleT1Write.resolve(true)
    await stalePromise
    expect(vm.singleTaskDetail?.progress.concurrency_total).toBe(4)
    expect(vm.singleTaskDetail?.params?.threads).toBe(4)

    wrapper.unmount()
  })

  it.each([
    ['concurrency', 'concurrency', true, false, false],
    ['inFlight', 'inFlight', true, false, false],
    ['concurrency', 'inFlight', true, false, false],
    ['inFlight', 'concurrency', true, false, false],
    ['concurrency', 'concurrency', false, false, false],
    ['inFlight', 'inFlight', false, false, false],
    ['concurrency', 'inFlight', true, true, false],
    ['inFlight', 'concurrency', true, true, false],
    ['concurrency', 'inFlight', true, true, true],
    ['concurrency', 'inFlight', true, false, true],
  ] as const)(
    'orders detail writes: %s then %s (success: %s, older first: %s, fallback: %s)',
    async (olderKind, newerKind, newerSuccess, olderFirst, fallbackConcurrency) => {
      const taskStore = useTaskStore()
      vi.spyOn(taskStore, 'selectTask').mockResolvedValue(undefined as never)
      const detail = makeTask('task-1', 'publish_folder', 2)
      if (fallbackConcurrency) {
        detail.progress.concurrency_total = 0
        delete detail.params?.threads
        delete detail.params?.upload_concurrency
      }
      vi.spyOn(taskStore, 'fetchTaskById').mockResolvedValue(detail)
      const older = deferred<boolean>()
      const newer = deferred<boolean>()
      vi.spyOn(taskStore, 'updateTaskConcurrency').mockReturnValueOnce(older.promise).mockReturnValueOnce(newer.promise)

      const wrapper = mount(TaskDetailDrawer, {
        props: { show: true, taskId: 'task-1' },
        global: {
          stubs: {
            NDrawer: { props: ['show', 'width'], template: '<div :data-width="width"><slot /></div>' },
            NDrawerContent: { template: '<div><slot name="header" /><slot /></div>' },
            NTabs: { template: '<div><slot /></div>' },
            NTabPane: { template: '<div><slot /></div>' },
            NTag: { template: '<span><slot /></span>' },
            NButton: { template: '<button><slot /></button>' },
            NSpin: { template: '<div><slot /></div>' },
            NEmpty: { template: '<div />' },
            TaskLogTerminal: { template: '<div />' },
            TaskRealtimeMetricsTab: { template: '<div />' },
            TaskParamsMetadataTab: { template: '<div />' },
            TaskDiagnosisTab: { template: '<div />' },
          },
        },
      })

      await flushPromises()
      const vm = wrapper.vm as unknown as {
        task: NormalizedTask
        currentTaskConcurrency: number
        currentTaskInFlight: number
        handleUpdateConcurrency: (val: number) => Promise<void>
        handleUpdateInFlight: (val: number) => Promise<void>
        updatingConcurrency: boolean
      }
      const update = (kind: 'concurrency' | 'inFlight', value: number) =>
        kind === 'concurrency' ? vm.handleUpdateConcurrency(value) : vm.handleUpdateInFlight(value)
      const initialInFlight = vm.currentTaskInFlight
      const olderRequest = update(olderKind, 3)
      const newerRequest = update(newerKind, 4)
      expect(vm.updatingConcurrency).toBe(true)
      expect(taskStore.updateTaskConcurrency).toHaveBeenNthCalledWith(
        2,
        'task-1',
        newerKind === 'concurrency' ? 4 : 2,
        newerKind === 'inFlight' ? 4 : initialInFlight,
      )
      if (olderFirst) {
        older.resolve(true)
        await olderRequest
        expect(vm.updatingConcurrency).toBe(true)
        newer.resolve(newerSuccess)
        await newerRequest
      } else {
        newer.resolve(newerSuccess)
        await newerRequest
        expect(vm.updatingConcurrency).toBe(false)
        older.resolve(true)
        await olderRequest
      }

      expect(vm.currentTaskConcurrency).toBe(
        newerSuccess ? (newerKind === 'concurrency' ? 4 : 2) : olderKind === 'concurrency' ? 3 : 2,
      )
      expect(vm.currentTaskInFlight).toBe(
        newerSuccess
          ? newerKind === 'inFlight'
            ? 4
            : initialInFlight
          : olderKind === 'inFlight'
            ? 3
            : initialInFlight,
      )
      expect(vm.task.progress.concurrency_total).toBe(vm.currentTaskConcurrency)
      expect(vm.task.params?.upload_concurrency).toBe(vm.currentTaskConcurrency)
      expect(vm.task.params?.upload_in_flight_per_connection).toBe(vm.currentTaskInFlight)
      expect(vm.updatingConcurrency).toBe(false)
      wrapper.unmount()
    },
  )

  it('renders all 5 TaskMetricsHeader filter cards as native buttons and emits filter-status', async () => {
    const wrapper = mount(TaskMetricsHeader, {
      props: {
        totalRunningSpeed: 12.34,
        statusCounts: { running: 1, queued: 2, paused: 3, failed: 4, completed: 5, cancelled: 0 },
      },
    })

    const buttons = wrapper.findAll('button[data-testid="status-metric-card"]')
    expect(buttons).toHaveLength(5)
    for (const btn of buttons) {
      expect(btn.attributes('type')).toBe('button')
      await btn.trigger('click')
    }
    expect(wrapper.emitted('filter-status')).toEqual([['running'], ['queued'], ['paused'], ['failed'], ['completed']])

    wrapper.unmount()
  })

  it('renders TaskBatchActionBar and emits batch events', async () => {
    const wrapper = mount(TaskBatchActionBar, {
      props: { selectedCount: 3 },
      global: {
        stubs: {
          NButton: { template: '<button @click="$emit(\'click\')"><slot /></button>' },
        },
      },
    })
    const buttons = wrapper.findAll('button')
    expect(buttons.length).toBeGreaterThanOrEqual(6)
    await buttons[0].trigger('click')
    await buttons[1].trigger('click')
    expect(wrapper.emitted('clear')).toHaveLength(1)
    expect(wrapper.emitted('batch-pause')).toHaveLength(1)

    wrapper.unmount()
  })
})
