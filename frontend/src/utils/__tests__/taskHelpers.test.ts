import { describe, expect, it } from 'vitest'
import {
  calcTaskPercent,
  formatTaskProgress,
  getProgressStatus,
  getStatusTagType,
  getTaskAccountDisplay,
  getTaskInFlight,
  getTypeLabel,
  getTypeTagType,
  isMultiUserbot,
  isUploadTask,
} from '../taskHelpers'
import { normalizeTask } from '../taskNormalizer'

describe('taskHelpers', () => {
  describe('calcTaskPercent', () => {
    it('returns 0 for null or undefined tasks', () => {
      expect(calcTaskPercent(null)).toBe(0)
      expect(calcTaskPercent(undefined)).toBe(0)
    })

    it('returns 100 unconditionally for completed tasks', () => {
      const task = normalizeTask({
        id: 't1',
        status: 'completed',
        progress: { percent: 0, processed_bytes: 0, total_bytes: 100 },
      })
      expect(calcTaskPercent(task)).toBe(100)
    })

    it('uses progress.percent when available and non-zero', () => {
      const task = normalizeTask({
        id: 't2',
        status: 'running',
        progress: { percent: 45.67, processed_bytes: 0, total_bytes: 0 },
      })
      expect(calcTaskPercent(task)).toBe(45.7)
    })

    it('falls back to byte ratio calculation when percent is 0', () => {
      const task = normalizeTask({
        id: 't3',
        status: 'running',
        progress: { percent: 0, processed_bytes: 250, total_bytes: 1000 },
      })
      expect(calcTaskPercent(task)).toBe(25)
    })

    it('falls back to item ratio calculation when bytes are 0', () => {
      const task = normalizeTask({
        id: 't4',
        status: 'running',
        progress: { percent: 0, processed_bytes: 0, total_bytes: 0, processed_items: 3, total_items: 10 },
      })
      expect(calcTaskPercent(task)).toBe(30)
    })

    it('clamps values between 0 and 100', () => {
      const taskOver = normalizeTask({
        id: 't5',
        status: 'running',
        progress: { percent: 120 },
      })
      expect(calcTaskPercent(taskOver)).toBe(100)
    })
  })

  describe('formatTaskProgress', () => {
    it('returns "-" for null tasks', () => {
      expect(formatTaskProgress(null)).toBe('-')
    })

    it('formats forward channel completed task with final summary', () => {
      const task = normalizeTask({
        id: 'fwd-1',
        task_type: 'forward_channel',
        status: 'completed',
        progress: { total_items: 50, processed_items: 50 },
        params: {
          final_summary: { forwarded: 48 },
        },
      })
      expect(formatTaskProgress(task)).toBe('50/50 已扫描 (成功 48 条)')
    })

    it('formats capacity tasks with byte strings', () => {
      const task = normalizeTask({
        id: 'dl-1',
        task_type: 'download_link',
        progress: { processed_bytes: 1048576, total_bytes: 10485760 },
      })
      expect(formatTaskProgress(task)).toBe('1 MB / 10 MB')
    })

    it('formats item-only tasks with item counts', () => {
      const task = normalizeTask({
        id: 'fwd-2',
        task_type: 'forward_channel',
        status: 'running',
        progress: { processed_items: 5, total_items: 20 },
      })
      expect(formatTaskProgress(task)).toBe('5 / 20 项')
    })
  })

  describe('isMultiUserbot', () => {
    it('detects multiple userbots by account_ids or executor_names', () => {
      expect(isMultiUserbot(null)).toBe(false)
      expect(isMultiUserbot(normalizeTask({ account_ids: [1, 2] }))).toBe(true)
      expect(isMultiUserbot(normalizeTask({ executor_names: ['BotA', 'BotB'] }))).toBe(true)
      expect(isMultiUserbot(normalizeTask({ account_ids: [1], executor_names: ['BotA'] }))).toBe(false)
    })
  })

  describe('getTaskAccountDisplay', () => {
    const mockResolver = (id: number) => `User_${id}`

    it('prioritizes account_ids over other identifiers', () => {
      const task = normalizeTask({
        account_ids: [101, 102],
        executor_names: ['Fallback'],
        account_id: 999,
      })
      expect(getTaskAccountDisplay(task, mockResolver)).toBe('[User_101, User_102]')
    })

    it('falls back to single executor_name or account_id', () => {
      const taskWithExec = normalizeTask({ executor_names: ['ExecutorA'] })
      expect(getTaskAccountDisplay(taskWithExec, mockResolver)).toBe('ExecutorA')

      const taskWithAccount = normalizeTask({ account_id: 202 })
      expect(getTaskAccountDisplay(taskWithAccount, mockResolver)).toBe('User_202')
    })

    it('defaults to 自动调度', () => {
      expect(getTaskAccountDisplay(null, mockResolver)).toBe('自动调度')
      expect(getTaskAccountDisplay(normalizeTask({}), mockResolver)).toBe('自动调度')
    })
  })

  describe('isUploadTask & getTaskInFlight', () => {
    it('identifies upload task types', () => {
      expect(isUploadTask(normalizeTask({ task_type: 'publish_folder' }))).toBe(true)
      expect(isUploadTask(normalizeTask({ task_type: 'pikpak_publish' }))).toBe(true)
      expect(isUploadTask(normalizeTask({ task_type: 'download_link' }))).toBe(false)
      expect(isUploadTask(normalizeTask({ task_type: 'forward_channel' }))).toBe(false)
    })

    it('reads upload_in_flight_per_connection strictly and defaults to 2', () => {
      expect(getTaskInFlight(null)).toBe(2)
      expect(getTaskInFlight(normalizeTask({}))).toBe(2)
      expect(
        getTaskInFlight(
          normalizeTask({
            params: { upload_in_flight_per_connection: 4 },
          }),
        ),
      ).toBe(4)
      expect(
        getTaskInFlight(
          normalizeTask({
            params: { upload_in_flight_per_connection: '3' },
          }),
        ),
      ).toBe(3)
    })
  })

  describe('tag and label helpers', () => {
    it('returns proper tag types for statuses', () => {
      expect(getStatusTagType('running')).toBe('info')
      expect(getStatusTagType('completed')).toBe('success')
      expect(getStatusTagType('paused')).toBe('warning')
      expect(getStatusTagType('failed')).toBe('error')
      expect(getStatusTagType('cancelled')).toBe('default')
      expect(getProgressStatus('completed')).toBe('success')
      expect(getProgressStatus('failed')).toBe('error')
      expect(getProgressStatus('paused')).toBe('warning')
      expect(getProgressStatus('running')).toBe('info')
    })

    it('returns proper type labels and tag types', () => {
      expect(getTypeLabel('download_link')).toBe('受限单链')
      expect(getTypeLabel('download_range')).toBe('范围抓取')
      expect(getTypeLabel('publish_folder')).toBe('相册发布')
      expect(getTypeLabel('forward_channel')).toBe('云端转发')
      expect(getTypeLabel('pikpak_publish')).toBe('PikPak 搬运')
      expect(getTypeTagType('forward_channel')).toBe('error')
      expect(getTypeTagType('publish_folder')).toBe('warning')
      expect(getTypeTagType('pikpak_publish')).toBe('primary')
      expect(getTypeTagType('download_link')).toBe('info')
    })
  })
})
