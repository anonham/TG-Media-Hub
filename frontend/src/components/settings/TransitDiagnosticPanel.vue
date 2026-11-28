<template>
  <div class="settings-section">
    <div class="section-header-row diagnostic-header" style="margin-bottom: 0.6rem">
      <div class="section-indicator" style="margin-bottom: 0">中转集群连通性与权限诊断看板</div>
      <n-button
        type="primary"
        secondary
        size="small"
        round
        :loading="settingStore.probingTransit"
        @click="settingStore.verifyTransit()"
      >
        <template #icon>
          <n-icon><SearchOutline /></n-icon>
        </template>
        探测中转集群状态
      </n-button>
    </div>

    <div class="cockpit-card setting-card-group" style="padding: 1.25rem">
      <!-- 尚未探测提示 -->
      <div
        v-if="!settingStore.transitReport && !settingStore.probingTransit"
        class="empty-hint"
        style="padding: 1.5rem 0"
      >
        💡 点击右上角【探测中转集群状态】按钮，系统将实时测试主 Userbot 的发信/删信权限及各辅助 Bot 进群可读状态。
      </div>

      <!-- 正在探测 -->
      <div v-else-if="settingStore.probingTransit" class="loading-placeholder" style="padding: 1.5rem 0">
        <n-spin size="medium" />
        <span style="margin-left: 12px">正在探测中转集群连通性与 Bot 驻留状态...</span>
      </div>

      <!-- 探测结果看板 -->
      <div v-else-if="settingStore.transitReport" class="transit-diagnostic-board">
        <!-- 概览栏 -->
        <div class="diag-overview-card">
          <div class="diag-status-badge-col">
            <n-tag :type="getTransitStatusTagType(settingStore.transitReport.status)" size="large" round>
              {{ getTransitStatusText(settingStore.transitReport.status) }}
            </n-tag>
          </div>
          <div class="diag-info-col">
            <div class="diag-title">{{ settingStore.transitReport.transit_chat_title || '未获取到群组名称' }}</div>
            <div class="diag-desc font-mono">ID: {{ settingStore.transitReport.transit_chat_id || '-' }}</div>
          </div>
          <div class="diag-userbot-badge">
            <n-tag :type="settingStore.transitReport.userbot_accessible ? 'success' : 'error'" size="small" round>
              {{
                settingStore.transitReport.userbot_accessible
                  ? '主 Userbot: 正常连通与保洁'
                  : '主 Userbot: 无法访问群组'
              }}
            </n-tag>
          </div>
        </div>

        <!-- 辅助 Bot 令牌池驻留网格 -->
        <div class="diag-bots-section">
          <div class="diag-section-sub-title">
            🤖 辅助 Bot 令牌池驻留状态 ({{ settingStore.transitReport.ready_bots_count }}/{{
              settingStore.transitReport.total_bots_count
            }}
            已在群)
          </div>
          <div class="diag-bots-grid">
            <div
              v-for="bot in settingStore.transitReport.bots_status || []"
              :key="bot.bot_id"
              class="diag-bot-card"
              :class="{ 'in-group': bot.in_transit_group, 'not-in-group': !bot.in_transit_group }"
            >
              <div class="diag-bot-name">
                {{ bot.session_name || `Bot #${bot.bot_id}` }}
                <span v-if="bot.bot_username" class="diag-bot-user font-mono">@{{ bot.bot_username }}</span>
              </div>
              <div class="diag-bot-tags">
                <n-tag size="tiny" :type="bot.in_transit_group ? 'success' : 'error'" round>
                  {{ bot.is_admin ? '管理员在群' : bot.in_transit_group ? '已在群内' : '未在群内' }}
                </n-tag>
                <n-tag size="tiny" :type="bot.is_ready ? 'default' : 'warning'" round>
                  {{ bot.is_ready ? '已就绪' : '未就绪' }}
                </n-tag>
              </div>
              <div v-if="bot.error" class="diag-bot-error">
                {{ bot.error }}
              </div>
            </div>
          </div>
        </div>

        <!-- 排障指引 / 建议 -->
        <div
          v-if="settingStore.transitReport.issues && settingStore.transitReport.issues.length > 0"
          class="diag-issues-box"
        >
          <div class="diag-issues-header">⚠️ 诊断排障提示：</div>
          <ul class="diag-issues-list">
            <li v-for="(issue, idx) in settingStore.transitReport.issues" :key="idx">
              {{ issue }}
            </li>
          </ul>
        </div>
        <div v-else class="diag-success-box">
          🎉 中转集群配置完全就绪！下载私密频道大文件时将自动调度 1 个主账号 +
          {{ settingStore.transitReport.ready_bots_count }} 个辅助 Bot 组成联合集群高速拉取！
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
// 中转集群诊断看板: 探测触发 + 状态概览 + Bot 驻留网格 + 排障指引。
// 数据源 settingStore.transitReport / probingTransit, 组件只承载展示与探测触发。
import { NButton, NIcon, NSpin, NTag } from 'naive-ui'
import { SearchOutline } from '@vicons/ionicons5'
import { useSettingStore } from '@/stores/settingStore'

const settingStore = useSettingStore()

function getTransitStatusTagType(status: string): 'success' | 'warning' | 'error' | 'default' {
  const map: Record<string, 'success' | 'warning' | 'error' | 'default'> = {
    healthy: 'success',
    partially_healthy: 'warning',
    degraded: 'warning',
    unconfigured: 'default',
    error: 'error',
  }
  return map[status] || 'default'
}

function getTransitStatusText(status: string): string {
  const map: Record<string, string> = {
    healthy: '完全就绪 (Healthy)',
    partially_healthy: '部分就绪 (Partially Healthy)',
    degraded: '功能降级 (Degraded)',
    unconfigured: '未配置中转群 (Unconfigured)',
    error: '异常受阻 (Error)',
  }
  return map[status] || status
}
</script>

<style scoped>
.section-header-row {
  display: flex;
  gap: 1rem;
}
.diagnostic-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 1rem;
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
.loading-placeholder {
  padding: 3rem 1.5rem;
  text-align: center;
  color: var(--text-secondary);
  font-size: 0.95rem;
  display: flex;
  align-items: center;
  justify-content: center;
}
/* 中转群诊断看板 */
.transit-diagnostic-board {
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
}
.diag-overview-card {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 1.25rem;
  padding: 1rem 1.25rem;
  background: var(--bg-primary);
  border: 1px solid var(--border-color);
  border-radius: 8px;
}
.diag-info-col {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}
.diag-title {
  font-weight: 600;
  font-size: 1rem;
  color: var(--text-primary);
}
.diag-desc {
  font-size: 0.8rem;
  color: var(--text-muted);
}
.diag-bots-section {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}
.diag-section-sub-title {
  font-size: 0.9rem;
  font-weight: 600;
  color: var(--text-secondary);
}
.diag-bots-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
  gap: 0.75rem;
}
.diag-bot-card {
  padding: 0.85rem 1rem;
  background: var(--bg-primary);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  transition: all 0.2s ease;
}
.diag-bot-card.in-group {
  border-color: rgba(16, 185, 129, 0.3);
  background: rgba(16, 185, 129, 0.03);
}
.diag-bot-card.not-in-group {
  border-color: rgba(239, 68, 68, 0.25);
  background: rgba(239, 68, 68, 0.03);
}
.diag-bot-name {
  font-size: 0.88rem;
  font-weight: 600;
  color: var(--text-primary);
  display: flex;
  align-items: center;
  gap: 0.4rem;
}
.diag-bot-user {
  font-size: 0.78rem;
  color: var(--text-muted);
  font-weight: normal;
}
.diag-bot-tags {
  display: flex;
  gap: 0.4rem;
}
.diag-bot-error {
  font-size: 0.75rem;
  color: var(--danger-color);
  background: rgba(239, 68, 68, 0.1);
  padding: 0.25rem 0.5rem;
  border-radius: 4px;
  word-break: break-all;
}
.diag-issues-box {
  padding: 0.85rem 1rem;
  background: rgba(245, 158, 11, 0.08);
  border: 1px solid rgba(245, 158, 11, 0.25);
  border-radius: 6px;
  font-size: 0.85rem;
}
.diag-issues-header {
  font-weight: 600;
  color: #f59e0b;
  margin-bottom: 0.4rem;
}
.diag-issues-list {
  margin: 0;
  padding-left: 1.25rem;
  color: var(--text-secondary);
  line-height: 1.5;
}
.diag-success-box {
  padding: 0.85rem 1rem;
  background: rgba(16, 185, 129, 0.08);
  border: 1px solid rgba(16, 185, 129, 0.25);
  border-radius: 6px;
  color: #10b981;
  font-size: 0.85rem;
  line-height: 1.5;
}
</style>
