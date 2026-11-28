<template>
  <div class="settings-section">
    <div class="section-indicator">系统与架构概览</div>
    <div class="cockpit-card setting-card-group">
      <div class="setting-row-item">
        <div class="setting-row-info">
          <div class="setting-row-title">tg-media-hub 核心引擎</div>
          <div class="setting-row-desc">企业级 Telegram 媒体资源并发调度与多 Bot 负载枢纽</div>
        </div>
        <div class="setting-row-value font-mono">v{{ settingStore.appVersion }} (FastTelethon)</div>
      </div>
      <div class="setting-row-item">
        <div class="setting-row-info">
          <div class="setting-row-title">多通道架构</div>
          <div class="setting-row-desc">4-Bot 轮询上传分流 + 双通道并发下载 + 智能洪泛退避调度</div>
        </div>
        <div class="setting-row-value">
          <n-tag type="success" size="small" round>已就绪</n-tag>
        </div>
      </div>
      <div class="setting-row-item">
        <div class="setting-row-info">
          <div class="setting-row-title">前端设计语言</div>
          <div class="setting-row-desc">Cockpit Tools 现代纯净明亮规范 (基于 Vue 3 + Naive UI + Pinia)</div>
        </div>
        <div class="setting-row-value font-mono">Modern Light UI</div>
      </div>
      <div class="setting-row-item">
        <div class="setting-row-info">
          <div class="setting-row-title">Prometheus 监控端点</div>
          <div class="setting-row-desc">用于 Prometheus / Grafana 抓取服务与爬虫运行指标</div>
          <div class="path-display-box font-mono">
            {{ metricsEndpoint }}
          </div>
        </div>
        <div class="setting-row-action">
          <n-button secondary round size="small" @click="handleCopyMetrics">
            <template #icon>
              <n-icon><CopyOutline /></n-icon>
            </template>
            复制端点
          </n-button>
        </div>
      </div>
    </div>
  </div>

  <div class="settings-section">
    <div class="section-indicator">Telegram 底层核心参数</div>
    <div class="cockpit-card setting-card-group">
      <div v-if="formValues.auto_reconnect !== undefined" class="setting-row-item">
        <div class="setting-row-info">
          <div class="setting-row-title">
            断网自动重新连接 <span class="param-key-tag font-mono">(auto_reconnect)</span>
          </div>
          <div class="setting-row-desc">当前服务返回的底层连接恢复设置状态</div>
        </div>
        <div class="setting-row-control">
          <n-tag
            size="small"
            :bordered="false"
            round
            :type="isEnabledValue(formValues.auto_reconnect) ? 'success' : 'default'"
          >
            {{ isEnabledValue(formValues.auto_reconnect) ? '已启用' : '未启用' }}
          </n-tag>
        </div>
      </div>
      <div v-else class="empty-hint">当前版本未提供此动态设置项</div>
    </div>
  </div>

  <div class="settings-section">
    <div class="section-indicator">开源协议与项目信息</div>
    <div class="cockpit-card setting-card-group">
      <div class="setting-row-item">
        <div class="setting-row-info">
          <div class="setting-row-title">开源许可证</div>
          <div class="setting-row-desc">版权与使用条件以仓库 LICENSE 文件为准。</div>
        </div>
        <div class="setting-row-value font-mono">MIT License</div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
// 关于与系统 Tab: 系统概览 / Prometheus 端点复制 / 底层核心参数只读展示。
import { computed } from 'vue'
import { NButton, NIcon, NTag } from 'naive-ui'
import { CopyOutline } from '@vicons/ionicons5'
import { useSettingStore } from '@/stores/settingStore'
import { copyTextToClipboard } from '@/utils/clipboard'

defineProps<{
  formValues: Record<string, unknown>
}>()

const settingStore = useSettingStore()

const metricsEndpoint = computed(() => {
  if (typeof window !== 'undefined' && window.location) {
    return `${window.location.origin}/metrics`
  }
  return '/metrics'
})

async function handleCopyMetrics() {
  // 局域网 HTTP 非安全上下文由 clipboard.ts 自动回退 textarea 复制
  await copyTextToClipboard(metricsEndpoint.value, '端点地址已复制')
}

function isEnabledValue(value: unknown): boolean {
  return value === true || ['true', '1', 'yes', 'on'].includes(String(value).toLowerCase())
}
</script>

<style scoped>
.setting-row-value {
  font-size: 0.92rem;
  font-weight: 600;
  color: var(--text-primary);
}
.path-display-box {
  background: var(--bg-primary);
  border: 1px solid var(--border-color);
  padding: 0.45rem 0.75rem;
  border-radius: 6px;
  font-size: 0.82rem;
  color: var(--text-secondary);
  margin-top: 0.4rem;
  display: inline-block;
}
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
</style>
