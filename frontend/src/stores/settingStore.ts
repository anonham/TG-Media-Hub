import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { SystemSettingsGroupedResponse, SystemSettingItem, TransitVerifyHealthResponse } from '@/api/gen'
import { settingsApi } from '@/api'
import { createRequestSequence } from '@/api/client'
import { message } from '@/utils/discreteApi'
import { errDetail, isAuthError } from '@/utils/errors'

export interface TransitDiagnosticReport extends TransitVerifyHealthResponse {
  issues?: string[]
  ready_bots_count?: number
  total_bots_count?: number
  userbot_accessible?: boolean
}

export const useSettingStore = defineStore('setting', () => {
  const categories = ref<Record<string, SystemSettingItem[]>>({})
  const allSettings = ref<Record<string, string>>({})
  const loadingSettings = ref(false)
  const savingSettings = ref(false)
  // load/update 共用请求序号 —— 读取/保存交错时, 被取代的响应不得覆写
  // 较新状态; 保存成功后主动换代使在途读取失效 (保存失败时读取照常落库, 不丢数据)
  const settingsSeq = createRequestSequence()

  const transitReport = ref<TransitDiagnosticReport | null>(null)
  const probingTransit = ref(false)
  // 兜底 '--' 而非硬编码版本号 —— __APP_VERSION__ 注入失败时侧栏不得显示假版本
  const appVersion = ref(typeof __APP_VERSION__ !== 'undefined' ? __APP_VERSION__ : '--')

  async function loadSettings() {
    const token = settingsSeq.next()
    loadingSettings.value = true
    try {
      const res = await settingsApi.getSettings()
      if (!settingsSeq.isCurrent(token)) return
      const data = res.data as SystemSettingsGroupedResponse | undefined
      categories.value = data?.categories || {}
      allSettings.value = data?.all_settings || {}
      if (data?.app_version) {
        appVersion.value = data.app_version
      }
    } catch (err) {
      if (!settingsSeq.isCurrent(token)) return
      console.error('加载系统配置失败:', err)
      if (!isAuthError(err)) {
        message.error(`加载系统配置失败: ${errDetail(err)}`)
      }
    } finally {
      if (settingsSeq.isCurrent(token)) {
        loadingSettings.value = false
      }
    }
  }

  async function updateSettings(settingsMap: Record<string, unknown>) {
    savingSettings.value = true
    try {
      const res = await settingsApi.updateSettings(settingsMap)
      // 保存成功即换代 —— 使任何在途 loadSettings 的陈旧响应失效
      settingsSeq.next()
      loadingSettings.value = false
      const data = res.data as SystemSettingsGroupedResponse | undefined
      categories.value = data?.categories || {}
      allSettings.value = data?.all_settings || {}
      if (data?.app_version) {
        appVersion.value = data.app_version
      }
      message.success('系统动态配置已保存并即时热生效！')
      return res.data
    } catch (err) {
      message.error(`保存配置失败: ${errDetail(err)}`)
      throw err
    } finally {
      savingSettings.value = false
    }
  }

  async function verifyTransit() {
    probingTransit.value = true
    try {
      const res = await settingsApi.verifyTransit()
      const data = (res.data || null) as TransitDiagnosticReport | null
      transitReport.value = data
      if (data?.status === 'healthy') {
        message.success(`中转群探测完成: ${data.message || ''}`)
      } else if (data?.status === 'unconfigured') {
        message.warning(`中转群未配置: ${data.message || ''}`)
      } else {
        message.warning(`中转群探测提示: ${data?.message || ''}`)
      }
      return res.data
    } catch (err) {
      message.error(`中转群连通性探测失败: ${errDetail(err)}`)
      throw err
    } finally {
      probingTransit.value = false
    }
  }

  return {
    categories,
    allSettings,
    loadingSettings,
    savingSettings,
    transitReport,
    probingTransit,
    appVersion,
    loadSettings,
    updateSettings,
    verifyTransit,
  }
})
