<template>
  <n-modal
    :show="show"
    @update:show="$emit('update:show', $event)"
    preset="card"
    title="⚙️ 频道预设与权限探针管理"
    :style="{ width: '90%', maxWidth: '640px' }"
    class="glass-modal"
  >
    <n-tabs type="segment" v-model:value="activeTab">
      <!-- Tab 1: 权限探针与新增预设 -->
      <n-tab-pane name="add" tab="➕ 新增预设与权限探针">
        <div class="modal-form">
          <n-form-item label="预设类型用途:">
            <n-radio-group v-model:value="form.presetType" name="preset-type">
              <n-radio-button value="target">🎯 目标频道 (转存/发布/转发目标)</n-radio-button>
              <n-radio-button value="source">📥 源频道 (范围下载/极速转发源)</n-radio-button>
            </n-radio-group>
          </n-form-item>

          <n-form-item label="Telegram 频道标识符 / 链接:">
            <div class="input-with-action">
              <n-input
                v-model:value="form.channelId"
                placeholder="例如: -100XXXXXXXXXX 或 https://t.me/c/[频道ID]/[消息ID] 或 my_channel"
                class="flex-1"
              />
              <n-button
                secondary
                type="info"
                :loading="probing"
                :disabled="!form.channelId.trim()"
                @click="handleProbe"
              >
                🔍 权限探针测试
              </n-button>
            </div>
          </n-form-item>

          <!-- 探针诊断结果卡片 -->
          <div v-if="probeResult" class="probe-result-card" :class="{ 'probe-error': !probeResult.is_accessible }">
            <div class="probe-header">
              <span class="probe-title">📊 <b>频道探针诊断报告</b></span>
              <span v-if="probeResult.is_accessible" class="badge badge-success">连通正常</span>
              <span v-else class="badge badge-error">访问失败</span>
            </div>

            <div v-if="probeResult.is_accessible" class="probe-details">
              <div class="probe-row">
                <span class="probe-label">频道名称:</span>
                <span class="probe-val"
                  ><b>{{ probeResult.channel_title }}</b></span
                >
              </div>
              <div class="probe-row">
                <span class="probe-label">标准 ID:</span>
                <span class="probe-val font-mono">{{ probeResult.parsed_chat_id }}</span>
              </div>
              <div class="probe-row">
                <span class="probe-label">频道类型:</span>
                <span class="probe-val">{{ probeResult.channel_type }}</span>
              </div>
              <div class="probe-row">
                <span class="probe-label">权限诊断:</span>
                <span class="probe-val">{{ probeResult.permission_summary }}</span>
              </div>
              <div v-if="probeResult.is_restricted" class="probe-tip-warning">
                🛡️ <b>受限内容保护</b>：该频道开启了防转发保护，下载时将启用突破限制加速通道。
              </div>
              <div v-if="form.presetType === 'target' && !probeResult.has_write_permission" class="probe-tip-error">
                ⚠️ <b>写入权限提示</b>：当前账号对该频道无发帖/管理员权限，发布可能失败，请确认是否已将
                Bot/账号设为管理员。
              </div>
            </div>

            <div v-else class="probe-error-detail">❌ {{ probeResult.permission_summary }}</div>
          </div>

          <n-form-item label="自定义预设备注名称:">
            <n-input v-model:value="form.name" placeholder="例如: 专属备份频道 / 资源分享总群" />
          </n-form-item>

          <div class="modal-footer-btn-row">
            <n-button @click="$emit('update:show', false)">关闭</n-button>
            <n-button
              type="primary"
              :loading="saving"
              :disabled="!form.name.trim() || !form.channelId.trim()"
              @click="handleSave"
            >
              💾 确认保存至预设库
            </n-button>
          </div>
        </div>
      </n-tab-pane>

      <!-- Tab 2: 已存预设列表 -->
      <n-tab-pane name="list" tab="📋 已存预设管理">
        <div class="preset-list-container">
          <div class="preset-filter-row">
            <n-radio-group v-model:value="filterType" size="small">
              <n-radio-button value="all">全部 ({{ presetStore.presets.length }})</n-radio-button>
              <n-radio-button value="target">🎯 目标频道 ({{ presetStore.targetPresets.length }})</n-radio-button>
              <n-radio-button value="source">📥 源频道 ({{ presetStore.sourcePresets.length }})</n-radio-button>
            </n-radio-group>
          </div>

          <div v-if="filteredPresets.length === 0" class="empty-state">暂无此类型的已存频道预设</div>

          <div v-else class="preset-items-wrapper">
            <div v-for="item in filteredPresets" :key="item.id" class="preset-item-card">
              <div class="preset-item-info">
                <div class="preset-item-header">
                  <span class="preset-name"
                    ><b>{{ item.name }}</b></span
                  >
                  <span class="badge" :class="item.preset_type === 'source' ? 'badge-info' : 'badge-primary'">
                    {{ item.preset_type === 'source' ? '📥 源频道' : '🎯 目标频道' }}
                  </span>
                </div>
                <div class="preset-item-details">
                  <span class="preset-id font-mono">ID: {{ item.channel_id }}</span>
                  <span v-if="item.title" class="preset-real-title">({{ item.title }})</span>
                </div>
              </div>
              <div class="preset-item-actions">
                <n-button size="small" type="error" secondary @click="handleDelete(item.id)"> 删除 </n-button>
              </div>
            </div>
          </div>
        </div>
      </n-tab-pane>
    </n-tabs>
  </n-modal>
</template>

<script setup lang="ts">
import { ref, reactive, computed, watch } from 'vue'
import { NModal, NTabs, NTabPane, NFormItem, NInput, NRadioGroup, NRadioButton, NButton } from 'naive-ui'
import { usePresetStore } from '@/stores/presetStore'

const props = withDefaults(
  defineProps<{
    show?: boolean
    defaultType?: string
  }>(),
  {
    show: false,
    defaultType: 'target',
  },
)

const emit = defineEmits<{
  (e: 'update:show', val: boolean): void
  (e: 'saved', form: { presetType: string; channelId: string; name: string; title: string }): void
}>()

const presetStore = usePresetStore()

const activeTab = ref('add')
const filterType = ref('all')
const probing = ref(false)
const saving = ref(false)
const probeResult = ref<any>(null)

const form = reactive({
  presetType: 'target',
  channelId: '',
  name: '',
  title: '',
})

let probeRequestSeq = 0
let suppressProbeInvalidation = false

function invalidateProbeState() {
  probeRequestSeq += 1
  probing.value = false
  probeResult.value = null
  form.title = ''
}

watch(
  [() => form.channelId, () => form.presetType],
  () => {
    if (suppressProbeInvalidation) return
    invalidateProbeState()
  },
  { flush: 'sync' },
)

watch(
  () => props.show,
  (newVal) => {
    probeRequestSeq += 1
    probing.value = false
    if (newVal) {
      suppressProbeInvalidation = true
      form.presetType = props.defaultType || 'target'
      form.channelId = ''
      form.name = ''
      form.title = ''
      suppressProbeInvalidation = false
      probeResult.value = null
      presetStore.loadPresets()
    }
  },
  { immediate: true },
)

const filteredPresets = computed(() => {
  if (filterType.value === 'all') return presetStore.presets
  return presetStore.presets.filter((p) => (p.preset_type || 'target') === filterType.value)
})

async function handleProbe() {
  const rawChannelId = form.channelId.trim()
  if (!rawChannelId) return
  const targetPresetType = form.presetType
  const reqSeq = ++probeRequestSeq
  probing.value = true
  probeResult.value = null
  try {
    const res = await presetStore.inspectPresetChannel(rawChannelId, targetPresetType)
    if (
      reqSeq !== probeRequestSeq ||
      !props.show ||
      form.channelId.trim() !== rawChannelId ||
      form.presetType !== targetPresetType
    ) {
      return
    }
    if (res) {
      probeResult.value = res
      if (res.is_accessible) {
        if (res.parsed_chat_id) {
          suppressProbeInvalidation = true
          form.channelId = res.parsed_chat_id
          suppressProbeInvalidation = false
        }
        form.title = res.channel_title || ''
        if (!form.name.trim() && res.channel_title) {
          form.name = res.recommended_name || res.channel_title
        }
      }
    }
  } finally {
    if (reqSeq === probeRequestSeq) {
      probing.value = false
    }
  }
}

async function handleSave() {
  const snapshot = {
    presetType: form.presetType,
    channelId: form.channelId.trim(),
    name: form.name.trim(),
    title: form.title || '',
  }
  if (!snapshot.name || !snapshot.channelId) return
  saving.value = true
  try {
    const success = await presetStore.addPreset(snapshot.name, snapshot.channelId, snapshot.presetType, snapshot.title)
    if (success) {
      emit('saved', snapshot)
      activeTab.value = 'list'
    }
  } finally {
    saving.value = false
  }
}

async function handleDelete(presetId: number | string) {
  await presetStore.deletePreset(Number(presetId))
}
</script>

<style scoped>
.modal-form {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  margin-top: 0.5rem;
}

.input-with-action {
  display: flex;
  flex-wrap: wrap;
  gap: 0.75rem;
  width: 100%;
}

.flex-1 {
  flex: 1 1 200px;
  min-width: 0;
}

.probe-result-card {
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 0.85rem 1rem;
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.probe-error {
  border-color: rgba(239, 68, 68, 0.3);
  background: rgba(239, 68, 68, 0.05);
}

.probe-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid var(--border-color);
  padding-bottom: 0.4rem;
}

.probe-details {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
  font-size: 0.88rem;
}

.probe-row {
  display: flex;
  gap: 0.5rem;
}

.probe-label {
  color: var(--text-secondary);
  min-width: 70px;
}

.probe-val {
  color: var(--text-primary);
}

.probe-tip-warning {
  margin-top: 0.35rem;
  padding: 0.4rem 0.6rem;
  background: rgba(245, 158, 11, 0.1);
  border-left: 3px solid var(--warning-color);
  border-radius: 4px;
  font-size: 0.82rem;
  color: var(--warning-color);
}

.probe-tip-error {
  margin-top: 0.35rem;
  padding: 0.4rem 0.6rem;
  background: rgba(239, 68, 68, 0.1);
  border-left: 3px solid var(--danger-color);
  border-radius: 4px;
  font-size: 0.82rem;
  color: var(--danger-color);
}

.probe-error-detail {
  color: var(--danger-color);
  font-size: 0.88rem;
}

.modal-footer-btn-row {
  display: flex;
  justify-content: flex-end;
  flex-wrap: wrap;
  gap: 0.75rem;
  margin-top: 0.75rem;
}

.preset-list-container {
  display: flex;
  flex-direction: column;
  gap: 1rem;
  margin-top: 0.5rem;
}

.preset-filter-row {
  display: flex;
}

.empty-state {
  text-align: center;
  padding: 2.5rem;
  color: var(--text-secondary);
}

.preset-items-wrapper {
  display: flex;
  flex-direction: column;
  gap: 0.6rem;
  max-height: 380px;
  overflow-y: auto;
}

.preset-item-card {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.65rem;
  padding: 0.75rem 1rem;
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-radius: 6px;
}

.preset-item-info {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  min-width: 0;
}

.preset-item-header {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.5rem;
}

.preset-name {
  font-size: 0.95rem;
}

.preset-item-details {
  display: flex;
  gap: 0.5rem;
  font-size: 0.82rem;
  color: var(--text-secondary);
}

.badge {
  display: inline-block;
  padding: 0.15rem 0.45rem;
  font-size: 0.75rem;
  font-weight: 500;
  border-radius: 4px;
}

.badge-primary {
  background: rgba(99, 102, 241, 0.15);
  color: var(--indigo-color);
}

.badge-info {
  background: rgba(14, 165, 233, 0.15);
  color: var(--info-color);
}

.badge-success {
  background: rgba(16, 185, 129, 0.15);
  color: var(--success-color);
}

.badge-error {
  background: rgba(239, 68, 68, 0.15);
  color: var(--danger-color);
}
</style>
