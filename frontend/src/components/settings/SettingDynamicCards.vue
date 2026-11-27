<template>
  <template v-for="card in cards" :key="card.key">
    <!-- API 凭证入口卡 -->
    <div v-if="card.kind === 'apiAuth'" class="settings-section">
      <div class="section-indicator">{{ card.title }}</div>
      <div class="cockpit-card setting-card-group">
        <div class="setting-row-item">
          <div class="setting-row-info">
            <div class="setting-row-title">API 访问控制与凭证</div>
            <div class="setting-row-desc">当前系统访问需要 API Key 认证，在此查看或重置您的密钥身份口令</div>
          </div>
          <div class="setting-row-action">
            <n-button
              type="primary"
              secondary
              round
              size="small"
              @click="authStore.openAuthModal('API Key 访问控制与凭证管理')"
            >
              <template #icon
                ><n-icon><KeyOutline /></n-icon
              ></template>
              密钥凭证管理
            </n-button>
          </div>
        </div>
      </div>
    </div>

    <!-- 转发纯净过滤说明卡 -->
    <div v-else-if="card.kind === 'explainer'" class="forward-explainer-card cockpit-card">
      <div class="explainer-header">
        <span class="explainer-icon">🧹</span>
        <span class="explainer-title">{{ card.title }}</span>
      </div>
      <div class="explainer-body">
        <p>转发前会跳过空消息、Telegram 系统服务消息和没有文本或媒体内容的消息，避免为无效内容发起转发请求。</p>
        <p>含媒体或文本的普通消息按 Telegram 原生转发流程处理；此过滤说明不代表系统会重写消息正文或广告尾缀。</p>
      </div>
    </div>

    <!-- 常规动态配置卡片 (diagnostic 由 TransitDiagnosticPanel 承载) -->
    <div
      v-else-if="card.kind !== 'diagnostic' && (card.key !== 'other' || card.items.length > 0)"
      class="settings-section"
    >
      <div class="section-indicator">{{ card.title }}</div>
      <!-- 中转加速原理说明卡 (与可编辑配置控件共同渲染) -->
      <div v-if="card.key === 'transit'" class="transit-explainer-card cockpit-card">
        <div class="explainer-header">
          <span class="explainer-icon">💡</span>
          <span class="explainer-title">功能原理与优势</span>
        </div>
        <div class="explainer-body">
          <p>
            <strong>Telegram 官方对单个主账号施加了严格的下行带宽与频控限制</strong>，而辅助 Bot 享有独立的 API
            带宽配额。
          </p>
          <p>
            开启本功能后，下载私密频道大文件时，主账号会将文件静默同步至私有中转群，调度所有就绪辅助 Bot
            与主账号组成<strong>“联合下载集群”</strong>分块多路拉取，下载速率提升
            <span class="highlight-boost">3~5 倍</span>！
          </p>
          <p>
            下载完成后系统自动物理清理中转群消息，做到<strong>阅后即焚、无痕安全</strong>。若未配置群组或较小文件，系统将智能降级为直连下载，不影响正常使用。
          </p>
        </div>
      </div>
      <div class="cockpit-card setting-card-group">
        <template v-if="card.items.length > 0">
          <div v-for="item in card.items" :key="item.key" class="setting-row-item">
            <div class="setting-row-info">
              <div class="setting-row-title">
                {{ item.title }}
                <span class="param-key-tag font-mono">({{ item.key }})</span>
              </div>
              <div class="setting-row-desc">{{ item.description }}</div>
            </div>
            <div class="setting-row-control">
              <n-tag v-if="item.meta?.type === 'timestamp'" size="small" :bordered="false" round type="info">
                🕒 {{ formatStorageScanTimestamp(formValues[item.key]) }}
              </n-tag>
              <n-switch
                v-else-if="item.meta?.type === 'switch'"
                :value="isEnabledValue(formValues[item.key])"
                :disabled="Boolean(item.meta?.readOnly)"
                @update:value="(value) => emit('update:value', item.key, value ? 'true' : 'false')"
              />
              <n-select
                v-else-if="item.meta?.options"
                :value="formValues[item.key] as string | number | null | undefined"
                :options="item.meta.options"
                size="small"
                class="responsive-control-md"
                @update:value="(val) => emit('update:value', item.key, val)"
              />
              <n-input-number
                v-else-if="isNumericKey(item.key)"
                :value="Number(formValues[item.key]) || 0"
                :min="getMinVal(item.key)"
                :max="getMaxVal(item.key)"
                :step="getStepVal(item.key)"
                size="small"
                class="responsive-control-md"
                @update:value="(val) => emit('update:value', item.key, val)"
              />
              <n-input
                v-else
                :value="formValues[item.key] != null ? String(formValues[item.key]) : ''"
                size="small"
                class="responsive-control-md"
                @update:value="(val) => emit('update:value', item.key, val)"
              />
            </div>
          </div>
        </template>
        <div v-else class="empty-hint">{{ card.emptyText || '当前版本没有由后端提供的配置项' }}</div>
      </div>
    </div>
  </template>
</template>

<script setup lang="ts">
// 动态配置卡片渲染器: 通用/业务 Tab 共用的卡片与控件行单一来源
// (apiAuth 入口 / 原理说明卡 / 常规键值行: timestamp/switch/select/number/input)。
import { NButton, NIcon, NInput, NInputNumber, NSelect, NSwitch, NTag } from 'naive-ui'
import { KeyOutline } from '@vicons/ionicons5'
import { useAuthStore } from '@/stores/authStore'
import { formatDateTime } from '@/utils/formatters'
import {
  type SettingCard,
  type SettingItem,
  type SettingItemMeta,
  isNumericKey,
  getMinVal,
  getMaxVal,
  getStepVal,
} from '@/utils/settingsMetadata'

export type { SettingCard, SettingItem, SettingItemMeta }

defineProps<{
  cards: SettingCard[]
  formValues: Record<string, string | number | boolean | null | undefined>
}>()

const emit = defineEmits<{
  (e: 'update:value', key: string, val: string | number | boolean | null): void
}>()

const authStore = useAuthStore()

function isEnabledValue(value: unknown): boolean {
  return value === true || ['true', '1', 'yes', 'on'].includes(String(value).toLowerCase())
}

function formatStorageScanTimestamp(val: unknown): string {
  if (!val || val === '0' || Number(val) === 0) return '尚未执行全量扫描'
  const num = Number(val)
  if (isNaN(num)) return String(val)
  return formatDateTime(num, { hour12: false })
}
</script>

<style scoped>
.param-key-tag {
  font-size: 0.75rem;
  color: var(--text-muted);
  margin-left: 0.4rem;
  font-weight: normal;
}
.empty-hint {
  padding: 3rem 1.5rem;
  text-align: center;
  color: var(--text-secondary);
  font-size: 0.95rem;
  display: flex;
  align-items: center;
  justify-content: center;
}
/* 多 Bot 联合加速原理与安全说明卡片 */
.transit-explainer-card {
  padding: 1.25rem 1.5rem;
  margin-bottom: 1.25rem;
  background: linear-gradient(135deg, rgba(59, 130, 246, 0.08) 0%, rgba(16, 185, 129, 0.06) 100%);
  border: 1px solid rgba(59, 130, 246, 0.2);
  border-radius: 10px;
}
.explainer-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 0.6rem;
}
.explainer-icon {
  font-size: 1.1rem;
}
.explainer-title {
  font-weight: 600;
  font-size: 0.95rem;
  color: var(--text-primary);
}
.explainer-body {
  font-size: 0.86rem;
  line-height: 1.6;
  color: var(--text-secondary);
}
.explainer-body p {
  margin: 0.35rem 0;
}
.highlight-boost {
  color: #10b981;
  font-weight: 700;
}
.forward-explainer-card {
  padding: 1.25rem 1.5rem;
  background: linear-gradient(135deg, rgba(59, 130, 246, 0.08) 0%, rgba(16, 185, 129, 0.06) 100%);
  border: 1px solid rgba(59, 130, 246, 0.2);
  border-radius: 10px;
}
</style>
