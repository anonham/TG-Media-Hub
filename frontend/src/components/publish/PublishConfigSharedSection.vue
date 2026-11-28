<template>
  <div class="shared-publish-config">
    <div>
      <label class="form-label">目标私密频道 / 群组 ID (选择预设或手动输入):</label>
      <div class="select-row">
        <n-select
          v-model:value="selectedTarget"
          :options="presetStore.targetPresetOptions"
          placeholder="选择预设目标频道，或直接输入 -100xxx / @username"
          class="flex-1"
          filterable
          tag
          clearable
        />
        <slot name="target-actions"></slot>
      </div>
    </div>

    <div class="checkbox-row">
      <n-checkbox v-model:checked="albumMode" size="small" />
      <span class="checkbox-label">
        <b>图片相册模式</b>（同一批次图片合成相册 photo
        形态发送，默认关闭；单文件超过分卷阈值不入组，走占位卡/跳过；整组失败全员失败零入库）
      </span>
    </div>

    <div class="checkbox-row">
      <n-checkbox v-model:checked="forceRepublish" size="small" />
      <span class="checkbox-label">
        <b>强制重传</b>（忽略「已发布」防重记录，对相同目标频道重复发送；重发文件将在总结卡中披露）
      </span>
    </div>

    <div class="checkbox-row">
      <n-checkbox v-model:checked="sendPlaceholderCard" size="small" />
      <span class="checkbox-label">
        <b>启用缺集与失败占位卡（推荐）</b
        >（当文件超限或传输异常时在频道发送占位说明卡；重试成功后尽量原位刷新为正式媒体，超过 48
        小时编辑窗口的旧卡将改为追加发送）
      </span>
    </div>

    <div class="grid-2col">
      <div>
        <label class="form-label">上传并发通道数 (1~16):</label>
        <n-input-number
          v-model:value="uploadConcurrency"
          :min="1"
          :max="16"
          :step="1"
          :placeholder="concurrencyPlaceholder"
          clearable
          style="width: 100%"
        />
      </div>
      <div>
        <label class="form-label">单连接在途分块数 (1~4):</label>
        <n-input-number
          v-model:value="uploadInFlight"
          :min="1"
          :max="4"
          :step="1"
          :placeholder="inFlightPlaceholder"
          clearable
          style="width: 100%"
        />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
// 共享发布配置区: PublishView (本地) 与 PikPakPublishView 表单的
// 交集字段单一来源 —— 目标频道预设选择 / 相册模式 / 强制重传 / 占位说明卡开关 /
// 上传并发通道数与单连接在途分块数 (规范字段 upload_in_flight_per_connection)。
// 文案与并发占位符的差异仅按 variant 区分, 布局与控件单一模板。
import { computed } from 'vue'
import { NCheckbox, NInputNumber, NSelect } from 'naive-ui'
import { usePresetStore } from '@/stores/presetStore'
import { useSettingStore } from '@/stores/settingStore'

const props = withDefaults(
  defineProps<{
    /** local=本地发布 (静态占位) / pikpak=PikPak 流式 (系统设置感知占位) */
    variant?: 'local' | 'pikpak'
  }>(),
  { variant: 'local' },
)

const selectedTarget = defineModel<string | null>('selectedTarget', { default: null })
const uploadConcurrency = defineModel<number | null>('uploadConcurrency', { default: null })
const uploadInFlight = defineModel<number | null>('uploadInFlight', { default: null })
const sendPlaceholderCard = defineModel<boolean>('sendPlaceholderCard', { default: true })
const albumMode = defineModel<boolean>('albumMode', { default: false })
const forceRepublish = defineModel<boolean>('forceRepublish', { default: false })

const presetStore = usePresetStore()
const settingStore = useSettingStore()

// 预设选项自举 (共享消费方; loadPresets 带 in-flight 去重, 多视图并发安全)
presetStore.loadPresets()

const concurrencyPlaceholder = computed(() => {
  if (props.variant === 'pikpak') {
    return settingStore.allSettings?.pikpak_upload_concurrency
      ? `默认 ${settingStore.allSettings.pikpak_upload_concurrency} (系统设置)`
      : '默认 2 (config 稳健档)'
  }
  return '默认 8 (1~2 低速安全档)'
})

const inFlightPlaceholder = computed(() => {
  if (props.variant === 'pikpak') {
    return settingStore.allSettings?.pikpak_upload_in_flight
      ? `默认 ${settingStore.allSettings.pikpak_upload_in_flight} (系统设置)`
      : '默认 1'
  }
  return '默认 2'
})
</script>

<style scoped>
.form-label {
  display: block;
  font-size: 0.88rem;
  font-weight: 500;
  margin-bottom: 0.4rem;
}

.select-row {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.5rem;
}

.flex-1 {
  flex: 1 1 260px;
  min-width: 0;
}

.checkbox-row {
  display: flex;
  align-items: flex-start;
  gap: 0.5rem;
}

.checkbox-label {
  font-size: 0.85rem;
  color: var(--text-secondary);
  line-height: 1.5;
}

.grid-2col {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 1rem;
}

@media (max-width: 768px) {
  .grid-2col {
    grid-template-columns: 1fr;
  }
}
</style>
