import { describe, expect, it } from 'vitest'

import { SETTING_META_MAP, formatItemDesc, formatItemTitle, validateSettingValue } from '../settingsMetadata'

// 全串数值校验回归 —— parseInt/parseFloat 前缀解析不再放过残缺值
describe('validateSettingValue strict numeric parsing', () => {
  it('rejects decimal input for integer settings (4.5 no longer truncates to 4)', () => {
    expect(validateSettingValue('download_parallel_connections', '4.5')).toContain('必须介于')
    expect(validateSettingValue('max_global_concurrency', '4.5')).toContain('必须介于')
    expect(validateSettingValue('userbot_upload_threshold_mb', '2.5')).toContain('整数')
    expect(validateSettingValue('transit_min_file_size_mb', '0.5')).toContain('整数')
  })

  it('rejects junk-suffixed input (4junk no longer parses as 4)', () => {
    expect(validateSettingValue('download_parallel_connections', '4junk')).toContain('必须介于')
    expect(validateSettingValue('max_global_concurrency', '8junk')).toContain('必须介于')
  })

  it.each(['0b100', '0o10', '0x8', '8e0'])('rejects non-decimal integer syntax %s', (value) => {
    expect(validateSettingValue('download_parallel_connections', value)).toContain('必须介于')
  })

  it('rejects Infinity for quota values', () => {
    expect(validateSettingValue('max_storage_quota_gb', 'Infinity')).toContain('大于等于 0 的数字')
  })

  it('preserves explicit empty-value behavior (null = 不校验, 交由后端默认)', () => {
    expect(validateSettingValue('max_global_concurrency', '')).toBeNull()
    expect(validateSettingValue('download_parallel_connections', '')).toBeNull()
    expect(validateSettingValue('max_storage_quota_gb', '')).toBeNull()
    expect(validateSettingValue('userbot_upload_threshold_mb', '')).toBeNull()
  })

  it('keeps valid values passing (含空格容忍 trim 语义)', () => {
    expect(validateSettingValue('download_parallel_connections', '8')).toBeNull()
    expect(validateSettingValue('max_global_concurrency', '20')).toBeNull()
    expect(validateSettingValue('max_storage_quota_gb', '1.5')).toBeNull()
    expect(validateSettingValue('userbot_upload_threshold_mb', '30')).toBeNull()
  })
})

describe('auto_remux_mpegts_to_mp4 metadata and validation', () => {
  it('registers switch metadata, title, and description', () => {
    expect(SETTING_META_MAP.auto_remux_mpegts_to_mp4).toEqual({
      tab: 'general',
      card: 'global',
      title: 'MPEG-TS 自动转封装为 MP4',
      type: 'switch',
    })
    expect(formatItemTitle('auto_remux_mpegts_to_mp4')).toBe('MPEG-TS 自动转封装为 MP4')
    expect(formatItemDesc({ key: 'auto_remux_mpegts_to_mp4' })).toContain('.ts/.m2ts/.mts')
  })

  it('validates boolean string values strictly', () => {
    expect(validateSettingValue('auto_remux_mpegts_to_mp4', 'true')).toBeNull()
    expect(validateSettingValue('auto_remux_mpegts_to_mp4', 'false')).toBeNull()
    expect(validateSettingValue('auto_remux_mpegts_to_mp4', 'invalid')).toContain('true 或 false')
  })
})
