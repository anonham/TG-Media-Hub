<template>
  <div class="accounts-view-container">
    <!-- 顶部工作台卡片 -->
    <div class="accounts-header-card glass-card">
      <div class="header-tab-bar">
        <n-tabs v-model:value="activeTab" type="segment" animated>
          <n-tab name="userbots"> 👥 Userbot 账号管理 ({{ accountStore.accounts.length }}) </n-tab>
          <n-tab name="bots"> 🤖 Bot 令牌池 ({{ accountStore.bots.length }}) </n-tab>
        </n-tabs>
      </div>
    </div>

    <!-- 1. Userbot 账号管理 -->
    <div v-show="activeTab === 'userbots'" class="userbots-section">
      <div class="card glass-card">
        <div class="card-header-bar">
          <div class="title-block">
            <h3 class="section-title">Userbot 账号列表</h3>
            <p class="section-desc">管理突破限制下载与大文件（>2000 MiB (1.95 GiB)）上传的 Telegram 用户凭证连接池</p>
          </div>
          <div class="action-btn-group">
            <n-button type="primary" size="small" round @click="showAddAccountModal = true"> ➕ 添加 Userbot </n-button>
            <n-button
              secondary
              size="small"
              round
              :loading="accountStore.loadingAccounts"
              @click="accountStore.loadAccounts()"
            >
              🔄 刷新状态
            </n-button>
          </div>
        </div>

        <!-- 账号列表卡片网格 -->
        <div v-if="accountStore.accounts.length === 0" class="empty-placeholder">
          {{ accountStore.loadingAccounts ? '正在加载 Userbot 账号...' : '暂无 Userbot 账号，请点击右上角添加' }}
        </div>

        <div v-else class="accounts-grid">
          <div
            v-for="acc in accountStore.accounts"
            :key="acc.id"
            class="account-card"
            :class="{
              'is-active': acc.is_active,
              'is-floodwait': (acc.flood_wait_remaining_seconds || 0) > 0,
            }"
          >
            <!-- 账号头部信息 -->
            <div class="acc-top-row">
              <div class="acc-title-wrapper">
                <span class="acc-name">{{ acc.account_name }}</span>
                <span v-if="acc.is_default" class="default-badge">默认</span>
                <n-tag v-if="acc.is_premium" size="small" type="warning" round> ⭐ Premium </n-tag>
              </div>

              <!-- 状态标签 -->
              <n-tag size="small" :type="getAccountStatusTagType(acc)" round>
                {{ getAccountStatusText(acc) }}
              </n-tag>
            </div>

            <!-- 详细属性网格 -->
            <div class="acc-details-grid">
              <div class="acc-prop">
                <span class="prop-label">手机号码:</span>
                <span class="prop-val font-mono">{{ acc.phone || '-' }}</span>
              </div>
              <div class="acc-prop">
                <span class="prop-label">数据中心:</span>
                <span class="prop-val font-mono">{{ formatDcLocation(acc.dc_id) }}</span>
              </div>
              <div class="acc-prop">
                <span class="prop-label">并发上限:</span>
                <span class="prop-val font-mono">{{ acc.max_concurrency }} 线程</span>
              </div>
              <div class="acc-prop">
                <span class="prop-label">活跃任务:</span>
                <router-link
                  v-if="(acc.active_task_count || 0) > 0"
                  :to="`/tasks?account_id=${acc.id}`"
                  class="prop-val font-mono text-accent hover-underline"
                  title="点击在任务中心中过滤此账号的活跃任务"
                >
                  {{ acc.active_task_count }} 个
                  <span class="arrow-indicator">➔</span>
                </router-link>
                <span v-else class="prop-val font-mono">0 个</span>
              </div>
            </div>

            <!-- FloodWait 倒计时警告条 -->
            <div v-if="(acc.flood_wait_remaining_seconds || 0) > 0" class="flood-warning-bar">
              ⏳ 触发 Telegram 频控保护，剩余倒计时: {{ acc.flood_wait_remaining_seconds }}s
            </div>

            <!-- 底部操作按钮 -->
            <div class="acc-actions-row">
              <n-button v-if="acc.status !== 'authenticated'" size="tiny" type="primary" @click="openLoginModal(acc)">
                🔑 登录验证
              </n-button>
              <n-button size="tiny" secondary type="info" @click="accountStore.refreshAccount(acc.id)">
                🔍 探测
              </n-button>
              <n-button size="tiny" secondary type="default" @click="openEditModal(acc)"> ✏️ 编辑 </n-button>
              <n-button size="tiny" secondary type="error" @click="confirmDeleteAccount(acc)"> 🗑️ 删除 </n-button>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- 2. Bot 令牌池管理 -->
    <div v-show="activeTab === 'bots'" class="bots-section">
      <div class="card glass-card">
        <div class="card-header-bar">
          <div class="title-block">
            <h3 class="section-title">Bot 令牌池与轮询负载均衡</h3>
            <p class="section-desc">管理 4-Bot 轮询上传群组，单 Bot 20 并发，突破 TG 频控极速发布</p>
          </div>
          <div class="action-btn-group">
            <n-button type="primary" size="small" round @click="showAddBotModal = true"> ➕ 加入 Bot 令牌 </n-button>
            <n-button
              secondary
              type="warning"
              size="small"
              round
              :loading="accountStore.pingingBots"
              @click="accountStore.pingBots()"
            >
              ⚡ 批量 Ping 延迟
            </n-button>
            <n-button
              secondary
              type="info"
              size="small"
              round
              :loading="settingStore.probingTransit"
              @click="settingStore.verifyTransit()"
            >
              🚀 探测中转集群驻留
            </n-button>
            <n-button secondary size="small" round :loading="accountStore.loadingBots" @click="accountStore.loadBots()">
              🔄 刷新
            </n-button>
          </div>
        </div>

        <!-- Bot 与 Userbot 协同说明提示卡 -->
        <div class="collab-tip-banner">
          <div class="collab-tip-icon">💡</div>
          <div class="collab-tip-content">
            <span class="collab-tip-title">协同说明：</span>
            <span
              >Telegram 协议层面 Bot 与 Userbot 相互独立；本系统中
              <strong>Userbot</strong> 负责加入受限频道抓取下载，<strong>4-Bot 轮询池</strong>
              负责向目标频道并发发布。</span
            >
            <span class="collab-tip-highlight">
              ⚡ <strong>多 Bot 联合加速集群</strong>：将这 4 个 Bot 邀请进您在【系统设置 -> ⚡ 多 Bot
              联合加速】配置的私有中转群，即可在私密频道下载大文件时解锁 1 Userbot + 4 Bot
              联合满血并发拉取！可随时点击上方【探测中转集群驻留】检测状态。
            </span>
          </div>
        </div>

        <!-- Bot 列表网格 -->
        <div v-if="accountStore.bots.length === 0" class="empty-placeholder">
          {{ accountStore.loadingBots ? '正在加载 Bot 令牌池...' : '暂无 Bot 记录，请点击右上角加入 Bot' }}
        </div>

        <div v-else class="bots-grid">
          <div
            v-for="bot in accountStore.bots"
            :key="bot.id"
            class="bot-card"
            :class="{ 'is-online': accountStore.isBotOnline(bot) }"
          >
            <div class="bot-top-row">
              <div class="bot-info-title">
                <span class="bot-name">{{ bot.bot_name || `Bot #${bot.id}` }}</span>
                <span class="bot-username font-mono">
                  {{ bot.bot_username ? `@${bot.bot_username}` : '未获取到 Username' }}
                </span>
              </div>
              <div class="bot-tags-group">
                <n-tag
                  v-if="getBotTransitStatus(bot.id)"
                  size="small"
                  :type="getBotTransitStatus(bot.id)?.in_transit_group ? 'success' : 'warning'"
                  round
                >
                  {{
                    getBotTransitStatus(bot.id)?.is_admin
                      ? '🚀 中转群管理员'
                      : getBotTransitStatus(bot.id)?.in_transit_group
                        ? '🚀 中转群已就绪'
                        : '⚪ 未在群内'
                  }}
                </n-tag>
                <n-tag size="small" :type="bot.is_active ? 'success' : 'default'" round>
                  {{ bot.status }}
                </n-tag>
              </div>
            </div>

            <div class="bot-token-masked font-mono">Token: {{ maskToken(bot.bot_token_masked || '') }}</div>

            <div class="bot-details-row">
              <span class="bot-dc font-mono">数据中心: {{ formatDcLocation(bot.dc_id ?? 0) }}</span>
              <span
                v-if="getBotPing(bot.id)"
                class="bot-latency font-mono"
                :class="getLatencyClass(getBotPing(bot.id)!)"
              >
                📶 延迟: {{ getBotPing(bot.id) }}ms
              </span>
            </div>

            <div class="bot-actions-row">
              <n-button size="tiny" secondary type="error" @click="confirmDeleteBot(bot)"> 移出令牌池 </n-button>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- 弹窗 1: 添加 Userbot 弹窗 -->
    <n-modal
      v-model:show="showAddAccountModal"
      preset="card"
      title="➕ 添加 Userbot 账号凭证"
      style="width: 90%; max-width: 520px"
    >
      <n-form :model="newAccountForm" label-placement="top" size="small">
        <n-form-item label="账号标识名称 *" path="account_name">
          <n-input v-model:value="newAccountForm.account_name" placeholder="例如: VIP_Userbot_01" />
        </n-form-item>
        <n-form-item label="绑定的手机号 *" path="phone">
          <n-input v-model:value="newAccountForm.phone" placeholder="+XX XXXXXXXXX（带国际区号，必填）" />
        </n-form-item>
        <n-form-item label="Telegram API ID (可选，留空使用系统默认)" path="api_id">
          <n-input-number
            v-model:value="newAccountForm.api_id"
            :show-button="false"
            placeholder="例如: 1234567"
            style="width: 100%"
          />
        </n-form-item>
        <n-form-item label="Telegram API Hash (可选，留空使用系统默认)" path="api_hash">
          <n-input v-model:value="newAccountForm.api_hash" placeholder="例如: 0123456789abcdef..." />
        </n-form-item>
        <n-form-item label="最大任务并发数" path="max_concurrency">
          <n-input-number v-model:value="newAccountForm.max_concurrency" :min="1" :max="10" style="width: 100%" />
        </n-form-item>
      </n-form>
      <template #footer>
        <div style="display: flex; justify-content: flex-end; gap: 0.75rem">
          <n-button @click="showAddAccountModal = false">取消</n-button>
          <n-button type="primary" :loading="submittingAccount" @click="handleAddAccount">创建账号</n-button>
        </div>
      </template>
    </n-modal>

    <!-- 弹窗 2: Userbot 官方验证码与扫码登录弹窗 -->
    <n-modal
      v-model:show="showLoginModal"
      preset="card"
      title="🔑 Userbot 登录授权"
      style="width: 90%; max-width: 520px"
    >
      <div class="login-modal-content">
        <p class="login-tip">
          当前正在授权账号：<b>{{ currentLoginAccount?.account_name }}</b>
        </p>

        <TelegramChallengePanel
          mode="account"
          :account-id="currentLoginAccount?.id ?? null"
          :initial-phone="currentLoginAccount?.phone || ''"
          :phone-locked="Boolean(currentLoginAccount?.phone)"
          :active="showLoginModal"
          show-cancel-button
          @authenticated="showLoginModal = false"
          @cancel="showLoginModal = false"
        />
      </div>
    </n-modal>

    <!-- 弹窗 3: 编辑 Userbot 弹窗 -->
    <n-modal
      v-model:show="showEditAccountModal"
      preset="card"
      title="✏️ 修改 Userbot 配置"
      style="width: 90%; max-width: 480px"
    >
      <n-form :model="editAccountForm" label-placement="top" size="small">
        <n-form-item label="账号标识名称">
          <n-input v-model:value="editAccountForm.account_name" />
        </n-form-item>
        <n-form-item label="手机号">
          <n-input
            v-model:value="editAccountForm.phone"
            :disabled="editAccountForm.status === 'authenticated'"
            placeholder="+XX XXXXXXXXX"
          />
        </n-form-item>
        <n-form-item label="最大并发数">
          <n-input-number v-model:value="editAccountForm.max_concurrency" :min="1" :max="10" style="width: 100%" />
        </n-form-item>
        <n-form-item label="启用状态">
          <n-switch v-model:value="editAccountForm.is_active" />
        </n-form-item>
        <n-form-item label="设为默认主账号">
          <n-switch v-model:value="editAccountForm.is_default" />
        </n-form-item>
      </n-form>
      <template #footer>
        <div style="display: flex; justify-content: flex-end; gap: 0.75rem">
          <n-button @click="showEditAccountModal = false">取消</n-button>
          <n-button type="primary" :loading="updatingAccount" @click="handleUpdateAccount">保存修改</n-button>
        </div>
      </template>
    </n-modal>

    <!-- 弹窗 4: 添加 Bot 令牌弹窗 -->
    <n-modal
      v-model:show="showAddBotModal"
      preset="card"
      title="🤖 加入 Bot 令牌入池"
      style="width: 90%; max-width: 500px"
    >
      <div
        style="
          margin-bottom: 1rem;
          padding: 0.65rem 0.85rem;
          background: rgba(99, 102, 241, 0.08);
          border-radius: 6px;
          font-size: 0.82rem;
          color: var(--text-secondary);
          line-height: 1.5;
        "
      >
        💡 <strong>加速赋能建议</strong>：加入令牌池后，建议同步将此 Bot
        邀请进您在系统设置中配置的私有中转群。在下载私密频道大文件时，系统将自动调用此 Bot 参与联合分块多路高速拉取。
      </div>
      <n-form :model="newBotForm" label-placement="top" size="small">
        <n-form-item label="Bot Token *" path="bot_token">
          <n-input v-model:value="newBotForm.bot_token" placeholder="例如: 123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ" />
        </n-form-item>
        <n-form-item label="Bot 备注名称 (可选)" path="bot_name">
          <n-input v-model:value="newBotForm.bot_name" placeholder="例如: 发布分流 Bot #1" />
        </n-form-item>
      </n-form>
      <template #footer>
        <div style="display: flex; justify-content: flex-end; gap: 0.75rem">
          <n-button @click="showAddBotModal = false">取消</n-button>
          <n-button type="primary" :loading="submittingBot" @click="handleAddBot">加入令牌池</n-button>
        </div>
      </template>
    </n-modal>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { NTabs, NTab, NButton, NTag, NModal, NForm, NFormItem, NInput, NInputNumber, NSwitch } from 'naive-ui'

import TelegramChallengePanel from '@/components/auth/TelegramChallengePanel.vue'
import { useAccountStore } from '@/stores/accountStore'
import { useSettingStore } from '@/stores/settingStore'
import { dialog, message } from '@/utils/discreteApi'
import { formatDcLocation } from '@/utils/formatters'
import type { AccountResponse, BotResponse, TransitBotStatusItem } from '@/api/gen'

const accountStore = useAccountStore()
const settingStore = useSettingStore()

const activeTab = ref('userbots')

// 弹窗状态
const showAddAccountModal = ref(false)
const submittingAccount = ref(false)
const newAccountForm = ref({
  account_name: '',
  phone: '',
  api_id: null as number | null,
  api_hash: '',
  max_concurrency: 1 as number | null,
})

const showLoginModal = ref(false)
const currentLoginAccount = ref<AccountResponse | null>(null)

const showEditAccountModal = ref(false)
const updatingAccount = ref(false)
const editAccountForm = ref({
  id: null as number | null,
  account_name: '',
  phone: '',
  originalPhone: '',
  status: 'unauthenticated',
  max_concurrency: 1 as number | null,
  is_active: true,
  is_default: false,
})

const showAddBotModal = ref(false)
const submittingBot = ref(false)
const newBotForm = ref({
  bot_token: '',
  bot_name: '',
})

function getAccountStatusTagType(acc: AccountResponse): 'default' | 'warning' | 'success' | 'error' {
  if (!acc.is_active) return 'default'
  if ((acc.flood_wait_remaining_seconds || 0) > 0) return 'warning'
  if (acc.status === 'authenticated') return 'success'
  return 'error'
}

function getAccountStatusText(acc: AccountResponse) {
  if (!acc.is_active) return '已禁用'
  if ((acc.flood_wait_remaining_seconds || 0) > 0) return '频控冷却中'
  if (acc.status === 'authenticated') return '在线就绪'
  if (acc.status === 'invalid') return '会话失效，请重新授权'
  return '未认证'
}

function maskToken(token: string) {
  if (!token || token.length < 15) return token || '-'
  const parts = token.split(':')
  if (parts.length === 2) {
    return `${parts[0]}:******${parts[1].slice(-4)}`
  }
  return `${token.slice(0, 8)}******${token.slice(-4)}`
}

function getBotPing(botId: number): number | null {
  const item = (accountStore.pingResults || []).find((p) => p.bot_id === botId)
  return item ? (item.latency_ms ?? null) : null
}

function getLatencyClass(latency: number) {
  if (latency < 100) return 'latency-good'
  if (latency < 300) return 'latency-warn'
  return 'latency-bad'
}

function getBotTransitStatus(botId: number): TransitBotStatusItem | null {
  if (!settingStore.transitReport || !settingStore.transitReport.bots_status) return null
  return settingStore.transitReport.bots_status.find((b) => b.bot_id === botId) || null
}

// Userbot Actions
async function handleAddAccount() {
  if (!newAccountForm.value.account_name.trim()) {
    message.warning('请输入账号标识名称')
    return
  }
  const cleanPhone = newAccountForm.value.phone?.trim() || ''
  if (!cleanPhone) {
    message.warning('请输入绑定的手机号（带国际区号）')
    return
  }
  submittingAccount.value = true
  try {
    const created = await accountStore.createAccount({
      account_name: newAccountForm.value.account_name.trim(),
      phone: cleanPhone,
      api_id: newAccountForm.value.api_id || undefined,
      api_hash: newAccountForm.value.api_hash?.trim() || undefined,
      max_concurrency: newAccountForm.value.max_concurrency || 1,
    })
    showAddAccountModal.value = false
    newAccountForm.value = { account_name: '', phone: '', api_id: null, api_hash: '', max_concurrency: 1 }
    // 自动连续引导：如果新建账号尚未授权，立即自动弹出登录授权窗口
    if (created && created.status !== 'authenticated') {
      openLoginModal(created)
      message.info('Userbot 凭证已录入，请获取 Telegram 官方验证码完成授权绑定')
    }
  } catch {
    // 创建失败已由 accountStore toast, 此处吞掉 rejection 仅走 finally 复位 (硬规则 5 写法收敛)
  } finally {
    submittingAccount.value = false
  }
}

function openLoginModal(acc: AccountResponse) {
  currentLoginAccount.value = acc
  showLoginModal.value = true
}

function openEditModal(acc: AccountResponse) {
  editAccountForm.value = {
    id: acc.id,
    account_name: acc.account_name,
    phone: acc.phone || '',
    originalPhone: acc.phone || '',
    status: acc.status || 'unauthenticated',
    max_concurrency: acc.max_concurrency || 1,
    is_active: acc.is_active,
    is_default: acc.is_default,
  }
  showEditAccountModal.value = true
}

async function handleUpdateAccount() {
  if (editAccountForm.value.id === null) return
  const trimmedPhone = editAccountForm.value.phone?.trim() || ''
  if (editAccountForm.value.originalPhone && !trimmedPhone) {
    message.warning('已绑定账号的手机号不能为空')
    return
  }
  updatingAccount.value = true
  try {
    await accountStore.updateAccount(editAccountForm.value.id, {
      account_name: editAccountForm.value.account_name,
      phone: editAccountForm.value.status === 'authenticated' || !trimmedPhone ? undefined : trimmedPhone,
      max_concurrency: editAccountForm.value.max_concurrency || 1,
      is_active: editAccountForm.value.is_active,
      is_default: editAccountForm.value.is_default,
    })
    showEditAccountModal.value = false
  } catch {
    // 更新失败已由 accountStore toast
  } finally {
    updatingAccount.value = false
  }
}

function confirmDeleteAccount(acc: AccountResponse) {
  dialog.warning({
    title: '确认删除 Userbot 账号',
    content: `确定要从连接池中卸载账号 "${acc.account_name}" 吗？`,
    positiveText: '确认删除',
    negativeText: '取消',
    onPositiveClick: async () => {
      await accountStore.deleteAccount(acc.id, true)
    },
  })
}

// Bot Actions
async function handleAddBot() {
  if (!newBotForm.value.bot_token.trim()) {
    message.warning('请输入有效的 Bot Token')
    return
  }
  submittingBot.value = true
  try {
    await accountStore.createBot({
      bot_token: newBotForm.value.bot_token.trim(),
      bot_name: newBotForm.value.bot_name?.trim() || undefined,
    })
    showAddBotModal.value = false
    newBotForm.value = { bot_token: '', bot_name: '' }
  } catch {
    // 添加失败已由 accountStore toast
  } finally {
    submittingBot.value = false
  }
}

function confirmDeleteBot(bot: BotResponse) {
  dialog.warning({
    title: '确认移出 Bot',
    content: `确定要将 Bot "${bot.bot_name || bot.bot_username}" 移出令牌池吗？`,
    positiveText: '确认移出',
    negativeText: '取消',
    onPositiveClick: async () => {
      await accountStore.deleteBot(bot.id)
    },
  })
}

onMounted(() => {
  accountStore.loadAccounts()
  accountStore.loadBots()
})
</script>

<style scoped>
.accounts-view-container {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
  width: 100%;
  container: accounts-root / inline-size;
}

.accounts-header-card {
  padding: 0.75rem 1.25rem;
}

.header-tab-bar {
  max-width: 480px;
  margin: 0 auto;
}

.card {
  padding: 1.5rem;
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
}

.card-header-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid var(--border-color);
  padding-bottom: 1rem;
  margin-bottom: 1.25rem;
  gap: 1rem;
}

.title-block {
  /* .section-title 与 ::before 已提炼至 main.css
  display: flex  align-items: center  font-size: 1.15rem  font-weight: 700  color: var(--text-primary)  margin: 0}

.title-block h3 {
  content: ''  display: inline-block  width: 4px  height: 16px  background: var(--accent-color)  border-radius: 2px  margin-right: 8px; */
}

.section-desc {
  color: var(--text-secondary);
  font-size: 0.85rem;
  margin: 0.25rem 0 0;
}

.action-btn-group {
  display: flex;
  align-items: center;
  gap: 0.6rem;
}

.empty-placeholder {
  padding: 3rem 0;
  text-align: center;
  color: var(--text-secondary);
  font-size: 0.9rem;
}

/* Userbot 账号卡片网格: 响应式自适应卡片 */
.accounts-grid,
.bots-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(min(100%, 360px), 1fr));
  gap: 1.25rem;
}

.account-card {
  container: account-card / inline-size;
}

.account-card,
.bot-card {
  background: var(--bg-card);
  border: 1px solid var(--border-color);
  border-radius: 12px;
  padding: 1.25rem;
  display: flex;
  flex-direction: column;
  gap: 0.85rem;
  transition: all 0.2s ease;
  box-shadow: var(--card-shadow);
}

.account-card:hover,
.bot-card:hover {
  border-color: var(--border-color-focus);
  transform: translateY(-2px);
  box-shadow: var(--card-shadow-hover);
}

.acc-top-row,
.bot-top-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.45rem;
}

.acc-title-wrapper {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.5rem;
}

.acc-name,
.bot-name {
  font-size: 1.05rem;
  font-weight: 600;
  color: var(--text-primary);
}

.default-badge {
  font-size: 0.7rem;
  background: var(--accent-light);
  color: var(--accent-color);
  padding: 0.1rem 0.4rem;
  border-radius: 4px;
  font-weight: 600;
  white-space: nowrap;
  flex-shrink: 0;
}

.acc-details-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 0.5rem;
  font-size: 0.85rem;
  background: var(--bg-card-hover);
  padding: 0.75rem;
  border-radius: 8px;
  border: 1px solid var(--border-color);
}

.acc-prop {
  display: flex;
  align-items: center;
  gap: 0.4rem;
}

.prop-label {
  color: var(--text-secondary);
  white-space: nowrap;
  flex-shrink: 0;
}

.prop-val {
  color: var(--text-primary);
}

.clickable-prop {
  cursor: pointer;
}

.text-accent {
  color: var(--accent-color);
  font-weight: 700;
}

.hover-underline:hover {
  text-decoration: underline;
}

.arrow-indicator {
  font-size: 0.75rem;
  margin-left: 2px;
}

.flood-warning-bar {
  background: rgba(245, 158, 11, 0.12);
  border: 1px solid rgba(245, 158, 11, 0.25);
  color: var(--warning-color);
  padding: 0.4rem 0.6rem;
  border-radius: 6px;
  font-size: 0.8rem;
}

.acc-actions-row,
.bot-actions-row {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  flex-wrap: wrap;
  gap: 0.5rem;
  margin-top: auto;
}

/* Bot 专属样式 */
.bot-info-title {
  display: flex;
  flex-direction: column;
  gap: 0.2rem;
}

.bot-username {
  font-size: 0.82rem;
  color: var(--accent-color);
}

.bot-token-masked {
  font-size: 0.82rem;
  color: var(--text-secondary);
  background: var(--bg-card-hover);
  padding: 0.4rem 0.6rem;
  border-radius: 6px;
  border: 1px solid var(--border-color);
}

.bot-tags-group {
  display: flex;
  flex-wrap: wrap;
  gap: 0.35rem;
  align-items: center;
}

.bot-details-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.4rem;
  font-size: 0.82rem;
}

.bot-latency.latency-good {
  color: var(--success-color);
}
.bot-latency.latency-warn {
  color: var(--warning-color);
}
.bot-latency.latency-bad {
  color: var(--danger-color);
}

@container accounts-root (max-width: 820px) {
  .accounts-grid,
  .bots-grid {
    grid-template-columns: 1fr;
  }
}

@container account-card (max-width: 380px) {
  .acc-details-grid {
    grid-template-columns: 1fr;
  }
}

.collab-tip-banner {
  display: flex;
  align-items: flex-start;
  gap: 0.85rem;
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-left: 4px solid var(--accent-color);
  border-radius: 8px;
  padding: 0.85rem 1.1rem;
  margin-bottom: 1.25rem;
  font-size: 0.85rem;
  line-height: 1.5;
  color: var(--text-primary);
}

.collab-tip-icon {
  font-size: 1.2rem;
  flex-shrink: 0;
  line-height: 1.3;
}

.collab-tip-content {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.collab-tip-title {
  font-weight: 700;
  color: var(--accent-color);
}

.collab-tip-highlight {
  font-weight: 600;
  color: var(--warning-color);
}

.code-channel-tip {
  display: flex;
  align-items: flex-start;
  gap: 0.65rem;
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-left: 4px solid var(--accent-color);
  border-radius: 8px;
  padding: 0.65rem 0.85rem;
  margin-bottom: 1rem;
  font-size: 0.82rem;
  line-height: 1.5;
  color: var(--text-primary);
}

.code-channel-icon {
  font-size: 1.15rem;
  flex-shrink: 0;
  line-height: 1.3;
}

.code-channel-text {
  display: flex;
  flex-direction: column;
  gap: 0.2rem;
}
</style>
