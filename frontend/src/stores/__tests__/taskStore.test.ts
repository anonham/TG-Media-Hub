import { AxiosError } from 'axios'
import { createPinia, setActivePinia } from 'pinia'
import { afterEach, expect, it, vi } from 'vitest'
import { downloadApi, tasksApi } from '@/api'
import { useTaskStore } from '@/stores/taskStore'
import { message } from '@/utils/discreteApi'

vi.mock('@/utils/discreteApi', () => ({
  message: { error: vi.fn(), success: vi.fn(), warning: vi.fn() },
}))

afterEach(() => {
  vi.restoreAllMocks()
  vi.clearAllMocks()
})

it('passes the download conflict through without a generic submission error', async () => {
  setActivePinia(createPinia())
  const detail = {
    code: 'duplicate_download_task',
    task_id: 'deadbeef',
    status: 'paused',
    message: '已有暂停的下载任务',
  }
  const error = Object.assign(new AxiosError('Conflict'), { response: { status: 409, data: { detail } } })
  vi.spyOn(downloadApi, 'submitLink').mockRejectedValueOnce(error)

  const result = await useTaskStore().submitLinkDownload('https://t.me/test/1')

  expect(result).toEqual({ success: false, taskId: null, downloadConflict: detail })
  expect(message.error).not.toHaveBeenCalled()
})

it('passes already-downloaded metadata through and submits force when requested', async () => {
  setActivePinia(createPinia())
  const detail = { code: 'already_downloaded', message: '目标文件均已下载到本地', file_count: 1, file_names: ['x.mp4'] }
  const error = Object.assign(new AxiosError('Conflict'), { response: { status: 409, data: { detail } } })
  const link = vi.spyOn(downloadApi, 'submitLink').mockRejectedValueOnce(error)
  const range = vi.spyOn(downloadApi, 'submitRange').mockRejectedValueOnce(error)
  const store = useTaskStore()

  const result = { success: false, taskId: null, downloadConflict: detail }
  expect(await store.submitLinkDownload('https://t.me/test/1', 2, 10, 8, true)).toEqual(result)
  expect(await store.submitRangeDownload('test', 1, 1, 2, 10, 8, true)).toEqual(result)
  expect(link).toHaveBeenCalledExactlyOnceWith({
    links: 'https://t.me/test/1',
    account_id: 2,
    priority: 10,
    threads: 8,
    force: true,
  })
  expect(range).toHaveBeenCalledExactlyOnceWith({
    channel_identifier: 'test',
    start_id: 1,
    end_id: 1,
    account_id: 2,
    priority: 10,
    threads: 8,
    force: true,
  })
  expect(message.error).not.toHaveBeenCalled()
})

it('displays the active task message on retry and preserves it in batch results', async () => {
  setActivePinia(createPinia())
  const detail = {
    code: 'duplicate_download_task',
    task_id: 'deadbeef',
    status: 'running',
    message: '已有相同的下载任务 (#deadbeef)',
  }
  const error = Object.assign(new AxiosError('Conflict'), { response: { status: 409, data: { detail } } })
  const batch = {
    action: 'retry',
    total: 1,
    success_count: 0,
    results: [{ task_id: 'original', success: false, error: detail.message }],
  }
  vi.spyOn(tasksApi, 'retry').mockRejectedValueOnce(error)
  vi.spyOn(tasksApi, 'batch').mockResolvedValueOnce({ data: batch } as Awaited<ReturnType<typeof tasksApi.batch>>)
  vi.spyOn(tasksApi, 'list').mockResolvedValue({ data: [] } as unknown as Awaited<ReturnType<typeof tasksApi.list>>)
  const store = useTaskStore()

  expect(await store.retryTask('original')).toBe(false)
  expect(message.warning).toHaveBeenCalledWith(detail.message)
  expect(await store.batchAction('retry', ['original'])).toEqual(batch)
  expect(message.warning).toHaveBeenCalledWith('批量重试完成，成功 0/1')
  expect(message.error).not.toHaveBeenCalled()
})
