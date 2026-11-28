import { describe, expect, it } from 'vitest'
import {
  formatBytes,
  formatDateTime,
  formatDcLocation,
  formatDuration,
  formatDurationLocalized,
  formatSeconds,
} from '../formatters'

describe('formatters', () => {
  describe('formatDcLocation', () => {
    it('returns "-" for falsy or zero dcId', () => {
      expect(formatDcLocation(null)).toBe('-')
      expect(formatDcLocation(0)).toBe('-')
      expect(formatDcLocation(undefined)).toBe('-')
    })

    it('formats known DC locations in standard mode', () => {
      expect(formatDcLocation(1)).toBe('DC 1 (US West)')
      expect(formatDcLocation(2)).toBe('DC 2 (Europe)')
      expect(formatDcLocation(3)).toBe('DC 3 (US East)')
      expect(formatDcLocation(4)).toBe('DC 4 (Europe)')
      expect(formatDcLocation(5)).toBe('DC 5 (Singapore)')
    })

    it('formats known DC locations in short mode', () => {
      expect(formatDcLocation(1, true)).toBe('DC 1 (US)')
      expect(formatDcLocation(2, true)).toBe('DC 2 (EU)')
      expect(formatDcLocation(3, true)).toBe('DC 3 (US)')
      expect(formatDcLocation(4, true)).toBe('DC 4 (EU)')
      expect(formatDcLocation(5, true)).toBe('DC 5 (SG)')
    })

    it('falls back to "DC <id>" for unknown DCs', () => {
      expect(formatDcLocation(99)).toBe('DC 99')
      expect(formatDcLocation('8', true)).toBe('DC 8')
    })
  })

  describe('formatBytes', () => {
    it('handles 0 and empty inputs', () => {
      expect(formatBytes(0)).toBe('0 B')
      expect(formatBytes(null)).toBe('0 B')
      expect(formatBytes(undefined)).toBe('0 B')
    })

    it('formats various byte magnitudes accurately', () => {
      expect(formatBytes(500)).toBe('500 B')
      expect(formatBytes(1024)).toBe('1 KB')
      expect(formatBytes(1024 * 1024)).toBe('1 MB')
      expect(formatBytes(1024 * 1024 * 1024 * 2.5)).toBe('2.5 GB')
      expect(formatBytes(1024 * 1024 * 1024 * 1024 * 3.125)).toBe('3.13 TB')
    })

    it('respects decimals parameter', () => {
      expect(formatBytes(1024 * 1536, 1)).toBe('1.5 MB')
      expect(formatBytes(1024 * 1536, 0)).toBe('2 MB')
    })

    it('clamps out-of-range sub-byte and petabyte magnitudes within available units', () => {
      expect(formatBytes(0.5)).toBe('0.5 B')
      expect(formatBytes(1024 ** 5)).toBe('1024 TB')
      expect(formatBytes(-5)).toBe('0 B')
      expect(formatBytes(Number.NaN)).toBe('0 B')
    })
  })

  describe('formatSeconds', () => {
    it('returns "0s" for non-positive or falsy input', () => {
      expect(formatSeconds(0)).toBe('0s')
      expect(formatSeconds(-10)).toBe('0s')
      expect(formatSeconds(null)).toBe('0s')
    })

    it('formats seconds under 1 minute', () => {
      expect(formatSeconds(45)).toBe('45s')
      expect(formatSeconds(45.2)).toBe('46s')
    })

    it('formats minutes and seconds', () => {
      expect(formatSeconds(60)).toBe('1m 0s')
      expect(formatSeconds(135)).toBe('2m 15s')
    })

    it('formats hours, minutes and seconds', () => {
      expect(formatSeconds(3665)).toBe('1h 1m 5s')
      expect(formatSeconds(7200)).toBe('2h 0m 0s')
    })
  })

  describe('formatDurationLocalized', () => {
    // engines 要求 node>=24 且 CI 锁 24, 均内置 Intl.DurationFormat; 条件守卫兜底本地旧环境
    const supportsDurationFormat = typeof Intl !== 'undefined' && 'DurationFormat' in Intl

    it('returns "0s" for 0 or negative seconds', () => {
      expect(formatDurationLocalized(0)).toBe('0s')
      expect(formatDurationLocalized(-5)).toBe('0s')
    })

    it('formats natural language duration via zh-CN Intl.DurationFormat', () => {
      if (!supportsDurationFormat) return expect(formatDurationLocalized(3665)).toBe('1h 1m 5s')
      expect(formatDurationLocalized(3665)).toBe('1小时1分钟5秒')
      expect(formatDurationLocalized(60)).toBe('1分钟')
    })

    it('formats digital style for mono ETA columns', () => {
      if (!supportsDurationFormat) return expect(formatDurationLocalized(3665, 'zh-CN', 'digital')).toBe('1h 1m 5s')
      expect(formatDurationLocalized(3665, 'zh-CN', 'digital')).toBe('1:01:05')
    })

    it('falls back to formatSeconds on ill-formed locale', () => {
      // 'zh_CN' (下划线) 是非法 BCP-47 标签, 构造抛 RangeError → 回退; 结果应被缓存不重复抛错
      expect(formatDurationLocalized(45, 'zh_CN')).toBe('45s')
      expect(formatDurationLocalized(3665, 'zh_CN')).toBe('1h 1m 5s')
    })
  })

  describe('formatDateTime', () => {
    it('returns "-" for invalid or empty timestamp', () => {
      expect(formatDateTime(null)).toBe('-')
      expect(formatDateTime(0)).toBe('-')
      expect(formatDateTime(undefined)).toBe('-')
      expect(formatDateTime('invalid')).toBe('-')
    })

    it('formats valid Unix timestamp (seconds) into localized string', () => {
      const formatted = formatDateTime(1700000000)
      expect(formatted).not.toBe('-')
      expect(typeof formatted).toBe('string')
    })
  })

  describe('formatDuration', () => {
    it('returns "-" when created timestamp is absent', () => {
      expect(formatDuration(null)).toBe('-')
      expect(formatDuration(undefined)).toBe('-')
    })

    it('computes diff when created and updated timestamps are provided', () => {
      const created = 1700000000
      const updated = 1700000125
      expect(formatDuration(created, updated)).toBe('2m 5s')
    })
  })
})
