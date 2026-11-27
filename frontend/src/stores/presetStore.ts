// 预设频道域 Store: 自 taskStore 全量迁出, 不留旧名转发 (硬规则 1)。
// 下拉选项 Getter 为全站唯一定义: 四个创建视图统一消费, 消除复制粘贴与
// PikPakPublishView 漏掉自动调度项的偏差; typed 分类为空时回退全量预设 (兼容
// preset_type 引入前的存量无类型条目, 与后端缺省 target 语义一致)。
import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import type { SelectOption } from 'naive-ui'
import type { PresetInspectRequest, PresetInspectResponse, PresetResponse } from '@/api/gen'
import { presetsApi } from '@/api'
import { message } from '@/utils/discreteApi'
import { errDetail } from '@/utils/errors'

// 预设频道下拉选项: label = "名称 (channel_id)", value = channel_id
export interface PresetSelectOption extends SelectOption {
  label: string
  value: string
}

export const usePresetStore = defineStore('preset', () => {
  const presets = ref<PresetResponse[]>([])

  const targetPresets = computed(() => presets.value.filter((p) => (p.preset_type || 'target') === 'target'))
  const sourcePresets = computed(() => presets.value.filter((p) => p.preset_type === 'source'))

  function toOptions(list: PresetResponse[]): PresetSelectOption[] {
    return list.map((p) => ({ label: `${p.name} (${p.channel_id})`, value: p.channel_id }))
  }

  const targetPresetOptions = computed(() =>
    toOptions(targetPresets.value.length > 0 ? targetPresets.value : presets.value),
  )
  const sourcePresetOptions = computed(() =>
    toOptions(sourcePresets.value.length > 0 ? sourcePresets.value : presets.value),
  )

  // 加载预设频道 (in-flight 去重锁 并发调用共享同一 Promise)
  let presetsInFlight: Promise<void> | null = null
  let presetsInFlightType: string | null = null
  let presetsGen = 0
  async function loadPresets(type: string | null = null): Promise<void> {
    if (presetsInFlight && presetsInFlightType === type) return presetsInFlight
    const gen = ++presetsGen
    presetsInFlightType = type
    let flight: Promise<void> | null = null
    flight = (async () => {
      try {
        const res = await presetsApi.list(type)
        // 变更后刷新已换代 —— 丢弃变更前的陈旧快照
        if (gen !== presetsGen) return
        presets.value = res.data || []
      } catch (err) {
        console.error('加载频道预设失败:', err)
      } finally {
        if (presetsInFlight === flight) {
          presetsInFlight = null
          presetsInFlightType = null
        }
      }
    })()
    presetsInFlight = flight
    return flight
  }

  async function refreshPresetsAfterMutation(): Promise<void> {
    // 变更后强制新读取 —— 先失效在途去重锁, 旧响应再经代际守卫丢弃
    presetsInFlight = null
    presetsInFlightType = null
    await loadPresets()
  }

  async function addPreset(name: string, channelId: string, presetType = 'target', title = ''): Promise<boolean> {
    try {
      await presetsApi.create({
        name,
        channel_id: channelId,
        preset_type: presetType,
        title: title || '',
      })
      message.success('预设频道保存成功！')
      await refreshPresetsAfterMutation()
      return true
    } catch (err) {
      message.error(`保存预设失败: ${errDetail(err)}`)
      return false
    }
  }

  async function deletePreset(presetId: number): Promise<boolean> {
    try {
      await presetsApi.delete(presetId)
      message.info('预设已删除')
      await refreshPresetsAfterMutation()
      return true
    } catch (err) {
      message.error(`删除预设失败: ${errDetail(err)}`)
      return false
    }
  }

  async function inspectPresetChannel(
    channelId: string,
    presetType = 'target',
    accountId: number | null = null,
  ): Promise<PresetInspectResponse | null> {
    try {
      const payload: PresetInspectRequest = {
        channel_id: channelId,
        preset_type: presetType,
      }
      if (accountId !== null && accountId !== undefined) {
        payload.account_id = accountId
      }
      const res = await presetsApi.inspect(payload)
      return res.data || null
    } catch (err) {
      message.error(`频道探针检测失败: ${errDetail(err)}`)
      return null
    }
  }

  return {
    presets,
    targetPresets,
    sourcePresets,
    targetPresetOptions,
    sourcePresetOptions,
    loadPresets,
    addPreset,
    deletePreset,
    inspectPresetChannel,
  }
})
