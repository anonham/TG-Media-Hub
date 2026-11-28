import { describe, expect, it } from 'vitest'
import { formatCooldownCountdown, getPikPakChannelHealth } from '../pikpakHealth'
import { normalizeTask } from '../taskNormalizer'

describe('pikpakHealth', () => {
  describe('formatCooldownCountdown', () => {
    it('formats zero and negative seconds as 0秒', () => {
      expect(formatCooldownCountdown(0)).toBe('0秒')
      expect(formatCooldownCountdown(-10)).toBe('0秒')
    })

    it('formats seconds under one hour', () => {
      expect(formatCooldownCountdown(45)).toBe('45秒')
      expect(formatCooldownCountdown(125)).toBe('2分5秒')
      expect(formatCooldownCountdown(900)).toBe('15分钟')
    })

    it('formats seconds over one hour', () => {
      expect(formatCooldownCountdown(3600)).toBe('1小时')
      expect(formatCooldownCountdown(3665)).toBe('1小时1分')
    })
  })

  describe('getPikPakChannelHealth', () => {
    const createBasePikPakTask = () =>
      normalizeTask({
        id: 'task-123',
        task_type: 'pikpak_publish',
        status: 'running',
        progress: {
          percent: 50,
          processed_bytes: 100,
          total_bytes: 200,
          speed_mb_s: 1.5,
          concurrency: 2,
          concurrency_total: 2,
        },
        params: {},
      })

    it('returns idle status for non-pikpak tasks', () => {
      const folderTask = normalizeTask({
        id: 'task-f',
        task_type: 'publish_folder',
        status: 'running',
      })
      const health = getPikPakChannelHealth(folderTask)
      expect(health.status).toBe('idle')
      expect(health.badgeType).toBe('default')
      expect(health.badgeText).toBe('空闲')
    })

    it('returns idle status for null or undefined tasks', () => {
      expect(getPikPakChannelHealth(null).status).toBe('idle')
      expect(getPikPakChannelHealth(undefined).status).toBe('idle')
    })

    it('identifies auth_required on 401 or unauthenticated message', () => {
      const task401 = normalizeTask({
        id: 'task-401',
        task_type: 'pikpak_publish',
        status: 'failed',
        error_detail: 'PikPak API Error 401: unauthenticated token expired',
      })
      const health = getPikPakChannelHealth(task401)
      expect(health.status).toBe('auth_required')
      expect(health.isAuthRequired).toBe(true)
      expect(health.badgeType).toBe('error')
      expect(health.badgeText).toBe('🔴 凭据失效')
    })

    it('identifies cooldown with Tier 1 and remaining seconds', () => {
      const now = 1000
      const taskCooldown = normalizeTask({
        id: 'task-cd1',
        task_type: 'pikpak_publish',
        status: 'queued',
        next_retry_at: 1000 + 420,
        params: { throttle_strikes: 1 },
      })
      const health = getPikPakChannelHealth(taskCooldown, now)
      expect(health.status).toBe('cooldown')
      expect(health.isCooldown).toBe(true)
      expect(health.tier).toBe(1)
      expect(health.remainingSeconds).toBe(420)
      expect(health.badgeType).toBe('warning')
      expect(health.badgeText).toBe('⏳ Tier 1 冷却 (7分钟)')
    })

    it('identifies cooldown with Tier 2 when throttle strikes >= 2 or dataRisk >= 1', () => {
      const now = 2000
      const taskCooldownTier2 = normalizeTask({
        id: 'task-cd2',
        task_type: 'pikpak_publish',
        status: 'queued',
        next_retry_at: 2000 + 1200,
        params: { throttle_strikes: 2 },
      })
      const health = getPikPakChannelHealth(taskCooldownTier2, now)
      expect(health.status).toBe('cooldown')
      expect(health.isCooldown).toBe(true)
      expect(health.tier).toBe(2)
      expect(health.remainingSeconds).toBe(1200)
      expect(health.badgeText).toBe('⏳ Tier 2 冷却 (20分钟)')
    })

    it('identifies cooldown with Tier 2 when cooldown_tier property is 2', () => {
      const now = 3000
      const taskCooldownProp = normalizeTask({
        id: 'task-cd-prop',
        task_type: 'pikpak_publish',
        status: 'queued',
        next_retry_at: 3000 + 600,
        cooldown_tier: 2,
      })
      const health = getPikPakChannelHealth(taskCooldownProp, now)
      expect(health.status).toBe('cooldown')
      expect(health.tier).toBe(2)
      expect(health.remainingSeconds).toBe(600)
    })

    it('identifies safe_paused when paused with safe_paused flag or >= 3 strikes', () => {
      const taskSafePaused = normalizeTask({
        id: 'task-sp',
        task_type: 'pikpak_publish',
        status: 'paused',
        params: { safe_paused: true, throttle_strikes: 3, auto_downshifted: true },
      })
      const health = getPikPakChannelHealth(taskSafePaused)
      expect(health.status).toBe('safe_paused')
      expect(health.isSafePaused).toBe(true)
      expect(health.isAutoDownshifted).toBe(true)
      expect(health.badgeType).toBe('warning')
      expect(health.badgeText).toBe('🟠 安全挂起')
    })

    it('identifies downshifted when auto_downshifted is true', () => {
      const taskDownshifted = normalizeTask({
        id: 'task-ds',
        task_type: 'pikpak_publish',
        status: 'running',
        params: { auto_downshifted: true },
      })
      const health = getPikPakChannelHealth(taskDownshifted)
      expect(health.status).toBe('downshifted')
      expect(health.isAutoDownshifted).toBe(true)
      expect(health.badgeType).toBe('info')
      expect(health.badgeText).toBe('🛡️ 稳态降档')
    })

    it('defaults to direct mode for healthy running pikpak tasks', () => {
      const taskDirect = createBasePikPakTask()
      const health = getPikPakChannelHealth(taskDirect)
      expect(health.status).toBe('direct')
      expect(health.badgeType).toBe('success')
      expect(health.badgeText).toBe('🟢 直连传输')
    })
  })
})
