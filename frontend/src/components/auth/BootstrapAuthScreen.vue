<template>
  <main class="bootstrap-screen">
    <section class="bootstrap-card" aria-labelledby="bootstrap-heading">
      <header class="bootstrap-header">
        <h1 id="bootstrap-heading" class="bootstrap-title">{{ modeLabel }}</h1>
        <p class="bootstrap-subtitle">请完成部署者认证和默认 Userbot 的 Telegram 验证。</p>
      </header>

      <div v-if="tgStore.consoleLocked" role="status" class="bootstrap-alert is-warning">
        <strong>公网控制台缺少 API Key</strong>
        <p>可以继续绑定默认 Userbot。控制台业务需在部署配置中设置 <code>API_KEY</code> 并重新启动服务后才能使用。</p>
      </div>

      <div v-if="!tgStore.bootstrapTokenConfigured" role="alert" class="bootstrap-alert is-warning">
        <strong>缺少有效的部署者引导密钥</strong>
        <p>
          请在服务端本地部署配置中设置高熵值的 <code>TG_BOOTSTRAP_TOKEN</code>（至少 16
          位随机字符），并重启服务后再进行绑定。
        </p>
      </div>

      <div
        v-else-if="tgStore.bindingMode === 'first_bind' && tgStore.defaultPhoneConfigured && !tgStore.defaultPhoneValid"
        role="alert"
        class="bootstrap-alert is-error"
      >
        <strong>默认手机号配置格式无效</strong>
        <p>
          部署配置中的 <code>TG_DEFAULT_PHONE</code> 格式非法。请修正为带国际区号的 E.164 格式（例如
          <code>+XX XXXXXXXXX</code>）或留空后重试。
        </p>
      </div>

      <form
        v-else-if="!tgStore.bootstrapAuthorized"
        class="bootstrap-token-form"
        method="POST"
        novalidate
        @submit.prevent="handleDeployerAuth"
      >
        <div class="field-group">
          <label for="bootstrap-deployer-token" class="field-label">部署者引导密钥 (TG_BOOTSTRAP_TOKEN)</label>
          <p id="bootstrap-token-hint" class="field-hint">输入服务端配置中的引导密钥。</p>
          <input
            id="bootstrap-deployer-token"
            v-model="deployerToken"
            type="password"
            name="bootstrap_token"
            class="field-input"
            autocomplete="off"
            autocorrect="off"
            autocapitalize="none"
            spellcheck="false"
            placeholder="输入部署配置中的 TG_BOOTSTRAP_TOKEN"
            :disabled="authorizing"
            aria-describedby="bootstrap-token-hint"
            required
          />
        </div>
        <p v-if="authError" role="alert" class="field-error">{{ authError }}</p>
        <button type="submit" class="primary-btn" :disabled="authorizing || !deployerToken.trim()">
          {{ authorizing ? '正在验证部署者凭证...' : '验证引导密钥并继续' }}
        </button>
      </form>

      <div v-else class="bootstrap-challenge-wrap">
        <div class="bootstrap-status-bar">
          <span class="status-pill">
            {{
              tgStore.bindingMode === 'rebind'
                ? '请用原先绑定的 Telegram 账号恢复。'
                : tgStore.defaultPhoneConfigured
                  ? '请输入部署配置指定的手机号。'
                  : '请输入要绑定的 Telegram 账号手机号。'
            }}
          </span>
          <button type="button" class="link-btn" @click="handleResetDeployerSession">更换引导密钥</button>
        </div>

        <TelegramChallengePanel
          mode="native"
          :phone-locked="false"
          :active="true"
          @authenticated="handleChallengeAuthenticated"
        />
      </div>
    </section>
  </main>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import TelegramChallengePanel from './TelegramChallengePanel.vue'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'
import { errDetail } from '@/utils/errors'

const tgStore = useTelegramAuthStore()

const deployerToken = ref('')
const authorizing = ref(false)
const authError = ref('')

const modeLabel = computed(() => {
  if (tgStore.bindingMode === 'rebind') return '恢复默认 Userbot'
  if (tgStore.bindingMode === 'unavailable') return '默认账号不可用'
  return '绑定默认 Userbot'
})

async function handleDeployerAuth(): Promise<void> {
  authError.value = ''
  const raw = deployerToken.value.trim()
  if (!raw) {
    authError.value = '请输入部署者引导密钥'
    return
  }
  authorizing.value = true
  try {
    await tgStore.authenticateBootstrap(raw)
    deployerToken.value = ''
  } catch (err: unknown) {
    authError.value = errDetail(err) || '引导密钥验证失败'
  } finally {
    authorizing.value = false
  }
}

async function handleResetDeployerSession(): Promise<void> {
  deployerToken.value = ''
  authError.value = ''
  await tgStore.logoutBootstrap()
}

async function handleChallengeAuthenticated(): Promise<void> {
  await tgStore.checkAdmission()
}
</script>

<style scoped>
.bootstrap-screen {
  min-height: 100vh;
  min-height: 100dvh;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 1.5rem;
}

.bootstrap-card {
  width: 100%;
  max-width: 540px;
  border-radius: 16px;
  padding: 2rem;
  border: 1px solid rgba(148, 163, 184, 0.28);
  background: var(--surface-card, rgba(255, 255, 255, 0.96));
  box-shadow: 0 20px 45px rgba(15, 23, 42, 0.12);
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
}

.bootstrap-header {
  display: flex;
  flex-direction: column;
  gap: 0.45rem;
}

.bootstrap-title {
  margin: 0;
  font-size: 1.35rem;
  font-weight: 700;
}

.bootstrap-subtitle {
  margin: 0;
  font-size: 0.92rem;
  line-height: 1.5;
  color: var(--text-secondary, #64748b);
}

.bootstrap-alert {
  padding: 0.95rem 1rem;
  border-radius: 10px;
  font-size: 0.9rem;
  line-height: 1.5;
}

.bootstrap-alert.is-warning {
  background: rgba(245, 158, 11, 0.12);
  border: 1px solid rgba(245, 158, 11, 0.35);
}

.bootstrap-alert.is-error {
  background: rgba(239, 68, 68, 0.12);
  border: 1px solid rgba(239, 68, 68, 0.35);
}

.bootstrap-alert p {
  margin: 0.35rem 0 0;
}

.bootstrap-token-form {
  display: flex;
  flex-direction: column;
  gap: 0.9rem;
}

.field-group {
  display: flex;
  flex-direction: column;
  gap: 0.4rem;
}

.field-label {
  font-weight: 600;
  font-size: 0.92rem;
}

.field-hint {
  margin: 0;
  font-size: 0.82rem;
  color: var(--text-secondary, #64748b);
}

.field-input {
  padding: 0.65rem 0.85rem;
  border-radius: 8px;
  border: 1px solid rgba(148, 163, 184, 0.45);
  font-size: 0.95rem;
}

.field-error {
  margin: 0;
  font-size: 0.85rem;
  color: #dc2626;
}

.primary-btn {
  padding: 0.65rem 1.2rem;
  border-radius: 8px;
  border: 0;
  background: #2563eb;
  color: #fff;
  font-weight: 600;
  cursor: pointer;
}

.primary-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.bootstrap-status-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
  margin-bottom: 0.9rem;
  font-size: 0.84rem;
  color: var(--text-secondary, #64748b);
}

.link-btn {
  background: transparent;
  border: 0;
  color: #2563eb;
  cursor: pointer;
  padding: 0;
  font-size: 0.84rem;
  white-space: nowrap;
}
</style>
