import { describe, expect, it } from 'vitest'
import { applyTaskProgressPatch, normalizeTask } from '../taskNormalizer'

describe('taskNormalizer', () => {
  describe('normalizeTask', () => {
    it('handles null and undefined input gracefully with safe defaults', () => {
      const normalized = normalizeTask(null)
      expect(normalized).toBeDefined()
      expect(normalized.progress).toEqual({
        processed_bytes: 0,
        total_bytes: 0,
        processed_items: 0,
        total_items: 0,
        percent: 0,
        speed_mb_s: 0,
        eta_s: 0,
        current_item: '',
        concurrency: 0,
        concurrency_total: 0,
        dc_id: 0,
        pikpak_download_speed_mb_s: 0,
        flood_wait_seconds: null,
      })
    })

    it('coerces stringified numeric progress values to number', () => {
      const raw = {
        id: 'test-task-1',
        task_type: 'download_link',
        status: 'running',
        progress: {
          processed_bytes: '1048576',
          total_bytes: '2097152',
          percent: '50.0',
          speed_mb_s: '12.5',
          concurrency: '4',
          dc_id: '2',
          flood_wait_seconds: '30',
        },
      }
      const normalized = normalizeTask(raw)
      expect(normalized.id).toBe('test-task-1')
      expect(normalized.progress.processed_bytes).toBe(1048576)
      expect(normalized.progress.total_bytes).toBe(2097152)
      expect(normalized.progress.percent).toBe(50.0)
      expect(normalized.progress.speed_mb_s).toBe(12.5)
      expect(normalized.progress.concurrency).toBe(4)
      expect(normalized.progress.dc_id).toBe(2)
      expect(normalized.progress.flood_wait_seconds).toBe(30)
    })

    it('is idempotent when re-normalizing an already normalized task', () => {
      const raw = {
        id: 'idempotent-task',
        task_type: 'publish_folder',
        status: 'running',
        progress: {
          processed_bytes: 500,
          total_bytes: 1000,
          percent: 50,
        },
      }
      const firstPass = normalizeTask(raw)
      const secondPass = normalizeTask(firstPass)
      expect(secondPass).toEqual(firstPass)
    })
  })

  describe('applyTaskProgressPatch', () => {
    it('applies progress patch in-place preserving unmentioned fields', () => {
      const task = normalizeTask({
        id: 'task-patch',
        status: 'running',
        progress: {
          processed_bytes: 100,
          total_bytes: 1000,
          dc_id: 2,
        },
      })

      applyTaskProgressPatch(task, {
        progress: {
          processed_bytes: 300,
          speed_mb_s: 15.2,
          current_item: 'video.mp4',
        },
      })

      expect(task.progress.processed_bytes).toBe(300)
      expect(task.progress.total_bytes).toBe(1000)
      expect(task.progress.speed_mb_s).toBe(15.2)
      expect(task.progress.dc_id).toBe(2)
      expect(task.progress.current_item).toBe('video.mp4')
    })

    it('updates task status and flood wait seconds from top-level or nested payload', () => {
      const task = normalizeTask({
        id: 'task-status-update',
        status: 'running',
      })

      applyTaskProgressPatch(task, {
        status: 'completed',
        flood_wait_seconds: 45,
      })

      expect(task.status).toBe('completed')
      expect(task.progress.flood_wait_seconds).toBe(45)
    })

    it('ignores null or undefined patch attributes without overwriting valid data', () => {
      const task = normalizeTask({
        id: 'task-guard',
        progress: {
          processed_bytes: 500,
        },
      })

      applyTaskProgressPatch(task, {
        progress: {
          processed_bytes: undefined,
          speed_mb_s: null,
        },
      })

      expect(task.progress.processed_bytes).toBe(500)
    })
  })
})
