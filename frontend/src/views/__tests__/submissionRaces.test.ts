import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, type VueWrapper } from '@vue/test-utils'
import { nextTick, type Component } from 'vue'
import Download from '../create/DownloadView.vue'
import Forward from '../create/ForwardView.vue'
import Publish from '../create/PublishView.vue'
import PikPak from '../PikPakPublishView.vue'

const mocks = vi.hoisted(() => ({
  task: {
    precheckAccess: vi.fn(),
    submitLinkDownload: vi.fn(),
    submitRangeDownload: vi.fn(),
    inspectDownloadRange: vi.fn(),
    inspectChannelInfo: vi.fn(),
    inspectFolder: vi.fn(),
    submitChannelForward: vi.fn(),
    submitFolderPublish: vi.fn(),
  },
  preset: { loadPresets: vi.fn(), inspectPresetChannel: vi.fn() },
  forward: {
    clearCoverage: vi.fn(),
    clearGaps: vi.fn(),
    clearEstimate: vi.fn(),
    loadCoverage: vi.fn(),
    loadGaps: vi.fn(),
    fetchEstimate: vi.fn(),
  },
  pikpak: {
    inspectSelection: vi.fn(),
    publishPikpak: vi.fn(),
    selectedFiles: new Map([['file', { path: '/file' }]]),
    selectionView: { dirGroups: [], singles: [], combinedDirTree: {} },
    selectedTotalBytes: 1,
  },
  dialog: { warning: vi.fn(), success: vi.fn() },
  push: vi.fn(),
}))
vi.mock('@/stores/taskStore', () => ({ useTaskStore: () => mocks.task }))
vi.mock('@/stores/presetStore', () => ({ usePresetStore: () => mocks.preset }))
vi.mock('@/stores/forwardStore', () => ({ useForwardStore: () => mocks.forward }))
vi.mock('@/stores/accountStore', () => ({ useAccountStore: () => ({ loadAccounts: vi.fn() }) }))
vi.mock('@/stores/settingStore', () => ({ useSettingStore: () => ({ loadSettings: vi.fn(), settings: {} }) }))
vi.mock('@/stores/pikpakStore', () => ({ usePikpakStore: () => mocks.pikpak }))
vi.mock('vue-router', () => ({ useRouter: () => ({ push: mocks.push }) }))
vi.mock('@/utils/discreteApi', () => ({
  dialog: mocks.dialog,
  message: { warning: vi.fn(), error: vi.fn(), success: vi.fn() },
}))

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}
const wrappers: VueWrapper[] = []
function setup(view: Component) {
  // Exercise the real setup and lifecycle without rendering unrelated child controls.
  const wrapper = mount({ ...view, render: () => null })
  wrappers.push(wrapper)
  return { wrapper, state: (wrapper.vm.$ as unknown as { setupState: Record<string, any> }).setupState }
}
beforeEach(() => vi.resetAllMocks())
afterEach(() => {
  wrappers.splice(0).forEach((wrapper) => wrapper.unmount())
})

describe('submission race regressions', () => {
  it.each([
    ['Link', 'running', '已有相同的下载任务'],
    ['Range', 'paused', '已有暂停的下载任务'],
  ])('opens the original %s download after a %s conflict', async (kind, status, title) => {
    const { state } = setup(Download)
    Object.assign(state, { linksInput: 'https://t.me/test/1', channelInput: 'source', startId: 1, endId: 5 })
    const submit = kind === 'Link' ? mocks.task.submitLinkDownload : mocks.task.submitRangeDownload
    submit.mockResolvedValue({
      success: false,
      taskId: null,
      downloadConflict: { code: 'duplicate_download_task', task_id: 'deadbeef', status, message: title },
    })

    await state[`handle${kind}Submit`]()

    const warning = mocks.dialog.warning.mock.calls[0][0]
    expect(warning).toMatchObject({ title, positiveText: '查看任务' })
    if (status === 'paused') expect(warning.content).toContain('继续下载')
    expect(mocks.push).not.toHaveBeenCalled()
    warning.onPositiveClick()
    expect(mocks.push).toHaveBeenCalledExactlyOnceWith({ name: 'TaskCenter', query: { id: 'deadbeef' } })
    expect(mocks.dialog.success).not.toHaveBeenCalled()
    expect([state.linksInput, state.channelInput, state.startId, state.endId, state.submitting]).toEqual([
      'https://t.me/test/1',
      'source',
      1,
      5,
      false,
    ])
  })

  it.each(['Link', 'Range'])('guards repeated %s confirmation and dismissal during submission', async (kind) => {
    const { state } = setup(Download)
    Object.assign(state, {
      selectedAccountId: 1,
      linksInput: 'https://t.me/test/1',
      channelInput: 'source',
      startId: 1,
      endId: 5,
    })
    mocks.task.precheckAccess.mockResolvedValue({ accessible: false })
    const request = deferred<unknown>()
    const submit = kind === 'Link' ? mocks.task.submitLinkDownload : mocks.task.submitRangeDownload
    submit.mockReturnValue(request.promise)
    await state[`handle${kind}Submit`]()
    const warning = mocks.dialog.warning.mock.calls[0][0]
    const first = warning.onPositiveClick()
    expect(await warning.onPositiveClick()).toBe(false)
    for (const dismiss of ['onNegativeClick', 'onClose', 'onMaskClick', 'onEsc']) warning[dismiss]()
    expect(state.submitting).toBe(true)
    await state[`handle${kind}Submit`]()
    expect(submit).toHaveBeenCalledTimes(1)
    request.resolve({ success: false })
    await first
    expect(state.submitting).toBe(false)
  })

  it.each(['Link', 'Range'])('releases the %s lock when cancelled before confirmation', async (kind) => {
    const { state } = setup(Download)
    Object.assign(state, { selectedAccountId: 1, linksInput: 'link', channelInput: 'source', startId: 1, endId: 5 })
    mocks.task.precheckAccess.mockResolvedValue({ accessible: false })
    await state[`handle${kind}Submit`]()
    mocks.dialog.warning.mock.calls[0][0].onNegativeClick()
    expect(state.submitting).toBe(false)
  })

  it.each(['Link', 'Range'])('forces the original %s snapshot and guards dismissal while submitting', async (kind) => {
    const { state } = setup(Download)
    Object.assign(state, {
      linksInput: 'https://t.me/test/1',
      channelInput: 'source',
      startId: 1,
      endId: 5,
      selectedAccountId: 2,
      taskPriority: 13,
      downloadThreads: 24,
    })
    mocks.task.precheckAccess.mockResolvedValue({ accessible: true })
    const submit = kind === 'Link' ? mocks.task.submitLinkDownload : mocks.task.submitRangeDownload
    const request = deferred<unknown>()
    submit
      .mockResolvedValueOnce({
        success: false,
        taskId: null,
        downloadConflict: {
          code: 'already_downloaded',
          message: '目标文件均已下载到本地',
          task_id: 'history',
          file_count: 1,
        },
      })
      .mockReturnValueOnce(request.promise)

    await state[`handle${kind}Submit`]()
    const warning = mocks.dialog.warning.mock.calls[0][0]
    expect(warning).toMatchObject({ positiveText: '强制重新下载', negativeText: '前往存储管理查看' })
    expect(warning.content).toContain('#history')
    expect(state.submitting).toBe(true)

    Object.assign(state, {
      linksInput: 'https://t.me/new/2',
      channelInput: 'new',
      startId: 8,
      endId: 9,
      selectedAccountId: 3,
      taskPriority: 10,
      downloadThreads: 32,
    })
    const pending = warning.onPositiveClick()
    expect(await warning.onPositiveClick()).toBe(false)
    for (const dismiss of ['onNegativeClick', 'onClose', 'onMaskClick', 'onEsc']) warning[dismiss]()
    expect(state.submitting).toBe(true)
    await state[`handle${kind}Submit`]()
    expect(submit).toHaveBeenCalledTimes(2)
    const args = kind === 'Link' ? ['https://t.me/test/1'] : ['source', 1, 5]
    expect(submit).toHaveBeenNthCalledWith(2, ...args, 2, 13, 24, true)
    expect(mocks.task.precheckAccess).toHaveBeenCalledTimes(1)
    expect(mocks.push).not.toHaveBeenCalled()

    request.resolve({ success: true, taskId: 'forced' })
    await pending
    expect(state.submitting).toBe(false)
    expect([state.linksInput, state.channelInput, state.startId, state.endId]).toEqual([
      'https://t.me/new/2',
      'new',
      8,
      9,
    ])
  })

  it('checks current history before blocking a fully downloaded range with message gaps', async () => {
    const { state } = setup(Download)
    Object.assign(state, { channelInput: 'source', startId: 1, endId: 5, selectedAccountId: 2 })
    await nextTick()
    mocks.task.inspectDownloadRange.mockResolvedValue({
      is_accessible: true,
      history_check_complete: true,
      all_media_downloaded: true,
      messages_with_media: 2,
      already_downloaded_count: 2,
    })
    await state.handleInspectRange()
    await state.handleRangeSubmit()
    expect(mocks.task.inspectDownloadRange).toHaveBeenNthCalledWith(1, 'source', 1, 5)
    expect(mocks.task.inspectDownloadRange).toHaveBeenNthCalledWith(2, 'source', 1, 5, false)
    expect(mocks.task.submitRangeDownload).not.toHaveBeenCalled()
    expect(mocks.task.precheckAccess).not.toHaveBeenCalled()

    const warning = mocks.dialog.warning.mock.calls[0][0]
    warning.onNegativeClick()
    expect(mocks.push).toHaveBeenCalledExactlyOnceWith({ name: 'Storage' })
    expect(await warning.onPositiveClick()).toBe(false)
    expect([state.channelInput, state.startId, state.endId, state.submitting]).toEqual(['source', 1, 5, false])

    await state.handleRangeSubmit()
    mocks.task.submitRangeDownload.mockResolvedValue({ success: false })
    await mocks.dialog.warning.mock.calls[1][0].onPositiveClick()
    expect(mocks.task.submitRangeDownload).toHaveBeenCalledExactlyOnceWith('source', 1, 5, 2, 10, 16, true)
  })

  it.each([
    [true, 1],
    [false, 2],
    [true, 2],
  ])('submits normally when the refreshed history cannot prove every file is present', async (complete, count) => {
    const { state } = setup(Download)
    Object.assign(state, { channelInput: 'source', startId: 1, endId: 5 })
    await nextTick()
    const original = {
      is_accessible: true,
      history_check_complete: true,
      all_media_downloaded: true,
      messages_with_media: 2,
      already_downloaded_count: 2,
    }
    const refresh = deferred<typeof original>()
    mocks.task.inspectDownloadRange.mockResolvedValueOnce(original).mockReturnValueOnce(refresh.promise)
    await state.handleInspectRange()

    const submission = state.handleRangeSubmit()
    await state.handleRangeSubmit()
    expect(mocks.task.inspectDownloadRange).toHaveBeenCalledTimes(2)
    expect(mocks.task.submitRangeDownload).not.toHaveBeenCalled()
    refresh.resolve({
      ...original,
      history_check_complete: complete,
      all_media_downloaded: false,
      already_downloaded_count: count,
    })
    await submission
    expect(mocks.dialog.warning).not.toHaveBeenCalled()
    expect(mocks.task.submitRangeDownload).toHaveBeenCalledExactlyOnceWith('source', 1, 5, null, 10, 16, false)
    expect(state.submitting).toBe(false)
  })

  it('does not intercept a range changed immediately after inspection', async () => {
    const { state } = setup(Download)
    Object.assign(state, { channelInput: 'source', startId: 1, endId: 5 })
    await nextTick()
    mocks.task.inspectDownloadRange.mockResolvedValue({
      is_accessible: true,
      history_check_complete: true,
      all_media_downloaded: true,
      messages_with_media: 2,
      already_downloaded_count: 2,
    })
    await state.handleInspectRange()
    state.endId = 6
    await state.handleRangeSubmit()
    expect(mocks.dialog.warning).not.toHaveBeenCalled()
    expect(mocks.task.submitRangeDownload).toHaveBeenCalledExactlyOnceWith('source', 1, 6, null, 10, 16, false)
  })

  it.each(['a newer inspection', 'a range edit'])(
    'ignores a submit-time inspection invalidated by %s',
    async (change) => {
      const { state } = setup(Download)
      Object.assign(state, { channelInput: 'source', startId: 1, endId: 5 })
      await nextTick()
      const complete = {
        is_accessible: true,
        history_check_complete: true,
        all_media_downloaded: true,
        messages_with_media: 2,
        already_downloaded_count: 2,
      }
      const older = deferred<typeof complete>()
      mocks.task.inspectDownloadRange
        .mockResolvedValueOnce(complete)
        .mockReturnValueOnce(older.promise)
        .mockResolvedValueOnce({ ...complete, all_media_downloaded: false, already_downloaded_count: 1 })
      await state.handleInspectRange()

      const submission = state.handleRangeSubmit()
      if (change === 'a newer inspection') {
        await state.handleInspectRange()
        expect(state.rangeInspectResult.already_downloaded_count).toBe(1)
      } else {
        state.endId = 6
        await nextTick()
      }
      older.resolve(complete)
      await submission

      expect(mocks.dialog.warning).not.toHaveBeenCalled()
      expect(mocks.task.submitRangeDownload).toHaveBeenCalledExactlyOnceWith('source', 1, 5, null, 10, 16, false)
      expect(state.submitting).toBe(false)
    },
  )

  it('preserves range edits during channel inspection', async () => {
    const { state } = setup(Forward)
    Object.assign(state, { sourceChat: 'source', startId: 1, endId: 5 })
    const request = deferred<unknown>()
    mocks.task.inspectChannelInfo.mockReturnValue(request.promise)
    const pending = state.handleInspect()
    state.startId = 3
    request.resolve({ min_id: 10, max_id: 20 })
    await pending
    expect([state.startId, state.endId]).toEqual([3, 5])
  })

  it.each([false, true])('cleans the forward draft only when unchanged (edited: %s)', async (edited) => {
    const { state } = setup(Forward)
    Object.assign(state, { sourceChat: 'source', selectedTarget: 'target', startId: 1, endId: 5, selectedAccountId: 1 })
    const check = deferred<unknown>()
    mocks.task.precheckAccess.mockReturnValue(check.promise)
    mocks.task.submitChannelForward.mockResolvedValue({ success: true, taskId: 1 })
    const pending = state.handleSubmit()
    if (edited) Object.assign(state, { sourceChat: 'new', startId: 10, endId: 20 })
    check.resolve({ accessible: true })
    await pending
    expect([state.sourceChat, state.startId, state.endId]).toEqual(edited ? ['new', 10, 20] : ['', null, null])
    expect(mocks.task.submitChannelForward.mock.calls[0].slice(0, 4)).toEqual(['source', 'target', 1, 5])
  })

  it.each([false, true])(
    'sequences permission reports independently of submission (manual first: %s)',
    async (manualFirst) => {
      const { state } = setup(Publish)
      Object.assign(state, { folderPath: '/old', selectedTarget: 'target' })
      await nextTick()
      const older = deferred<unknown>()
      const newer = deferred<unknown>()
      mocks.preset.inspectPresetChannel.mockReturnValueOnce(older.promise).mockReturnValueOnce(newer.promise)
      mocks.task.submitFolderPublish.mockResolvedValue({ success: true, taskId: 1 })
      const first = manualFirst ? state.handleProbePermission() : state.handleSubmit()
      const second = manualFirst ? state.handleSubmit() : state.handleProbePermission()
      newer.resolve({ has_write_permission: manualFirst })
      await second
      older.resolve({ has_write_permission: !manualFirst })
      await first
      expect(state.channelInspectResult.has_write_permission).toBe(manualFirst)
      expect(mocks.task.submitFolderPublish).toHaveBeenCalledTimes(1)
    },
  )

  it.each([false, true])('cleans publish inputs only when unchanged (edited: %s)', async (edited) => {
    const { state } = setup(Publish)
    Object.assign(state, { folderPath: '/old', selectedTarget: 'target', uploadConcurrency: 2, uploadInFlight: 1 })
    await nextTick()
    const check = deferred<unknown>()
    mocks.preset.inspectPresetChannel.mockReturnValue(check.promise)
    mocks.task.submitFolderPublish.mockResolvedValue({ success: true, taskId: 1 })
    const pending = state.handleSubmit()
    if (edited) Object.assign(state, { folderPath: '/new', uploadConcurrency: 3, uploadInFlight: 2 })
    check.resolve({ has_write_permission: true })
    await pending
    expect([state.folderPath, state.uploadConcurrency, state.uploadInFlight]).toEqual(
      edited ? ['/new', 3, 2] : ['', null, null],
    )
  })

  it.each([
    { readable: 1, unavailable: 0 },
    { readable: 1, unavailable: 1 },
    { readable: 0, unavailable: 1 },
  ])(
    'applies the inspection outcome with $readable readable and $unavailable unavailable',
    async ({ readable, unavailable }) => {
      const { state } = setup(Publish)
      Object.assign(state, { folderPath: '/media', selectedTarget: 'target' })
      await nextTick()
      mocks.task.inspectFolder.mockResolvedValue({
        valid_files_count: readable,
        unavailable_files_count: unavailable,
        unavailable_files: unavailable ? [{ file_name: 'gone.mp4', rel_path: 'gone.mp4', reason: '不可读取' }] : [],
      })
      mocks.preset.inspectPresetChannel.mockResolvedValue({ has_write_permission: true })
      mocks.task.submitFolderPublish.mockResolvedValue({ success: false })

      await state.handleInspect()
      expect(state.unavailableFileCount).toBe(unavailable)
      await state.handleSubmit()
      expect(mocks.task.submitFolderPublish).toHaveBeenCalledTimes(readable ? 1 : 0)
    },
  )

  it.each(['handleInspect', 'handleSubmit'])('discards pending PikPak %s after unmount', async (handler) => {
    const { state, wrapper } = setup(PikPak)
    state.selectedTarget = 'target'
    await nextTick()
    const check = deferred<unknown>()
    mocks.pikpak.inspectSelection.mockReturnValue(check.promise)
    const pending = state[handler]()
    wrapper.unmount()
    check.resolve({ accessible: true, has_write_permission: true })
    await pending
    expect(state.inspectResult).toBeNull()
    expect(mocks.dialog.warning).not.toHaveBeenCalled()
  })

  it('keeps ForwardView loadCoverage active when range is omitted and clears on source/target change', async () => {
    vi.useFakeTimers()
    try {
      const { state } = setup(Forward)
      Object.assign(state, { sourceChat: 'src-a', selectedTarget: 'tgt-1' })
      await nextTick()
      expect(mocks.forward.clearCoverage).toHaveBeenCalledTimes(1)
      vi.clearAllMocks()

      await vi.advanceTimersByTimeAsync(600)

      expect(mocks.forward.loadCoverage).toHaveBeenCalledWith('src-a', 'tgt-1', null, null)
      expect(mocks.forward.clearGaps).toHaveBeenCalledTimes(1)
      expect(mocks.forward.clearEstimate).toHaveBeenCalledTimes(1)
      expect(mocks.forward.clearCoverage).not.toHaveBeenCalled()

      Object.assign(state, { selectedTarget: 'tgt-2' })
      await nextTick()
      expect(mocks.forward.clearCoverage).toHaveBeenCalledTimes(1)
    } finally {
      vi.useRealTimers()
    }
  })

  it('normalizes empty-string automatic account selection to null in PikPakPublishView', async () => {
    const { state } = setup(PikPak)
    state.selectedTarget = 'tgt-1'
    state.selectedAccountId = ''
    await nextTick()

    mocks.pikpak.inspectSelection.mockResolvedValue({
      accessible: true,
      has_write_permission: true,
      files_total: 1,
      files_ready: 1,
      files_audit_blocked: 0,
      audit_blocked_names: [],
    })
    mocks.pikpak.publishPikpak.mockResolvedValue({ success: false })
    await state.handleInspect()
    expect(mocks.pikpak.inspectSelection).toHaveBeenCalledWith('tgt-1', null)

    await state.handleSubmit()
    const warning = mocks.dialog.warning.mock.calls[0]?.[0]
    if (warning?.onPositiveClick) {
      await warning.onPositiveClick()
    }
    const snapshot = state.capturePublishSnapshot()
    expect(snapshot.accountId).toBeNull()
    expect('account_id' in snapshot.payload).toBe(false)
  })
})
