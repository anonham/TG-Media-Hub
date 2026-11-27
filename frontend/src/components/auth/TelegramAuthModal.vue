<template>
  <n-modal v-model:show="store.isModalVisible" :mask-closable="false" :close-on-esc="false">
    <div class="glass-modal-card">
      <div class="modal-header">
        <h2>{{ isNativeMode ? '🛡️ 初次部署: 绑定 Telegram' : '🔒 Web 2FA 安全拦截' }}</h2>
        <p class="subtitle">
          {{
            isNativeMode
              ? '系统检测到首次运行，需要绑定 Telegram 账号以生成底层凭证。'
              : '控制台已锁定。为了防止 API Key 泄露，我们需要验证您的物理设备。'
          }}
        </p>
      </div>

      <div class="modal-body">
        <!-- Mode A: Native Telegram Login (First Deployment / Rebind) -->
        <div v-if="isNativeMode" class="native-login-flow">
          <TelegramChallengePanel
            mode="native"
            :active="store.isModalVisible && isNativeMode"
            :initial-phone="store.currentPhone"
            @authenticated="store.isModalVisible = false"
          />
        </div>

        <!-- Mode B: Web 2FA OTP -->
        <div v-else class="web-2fa-flow">
          <form v-if="!otpSent" class="otp-request" @submit.prevent="handleWeb2FASend">
            <div class="info-box">
              点击下方按钮，系统将向您的 <b>Telegram 收藏夹 (Saved Messages)</b> 发送一个 6 位动态验证码。
            </div>
            <n-button
              attr-type="submit"
              type="primary"
              block
              size="large"
              class="action-btn"
              :loading="loading"
              :disabled="loading || countdown > 0"
            >
              {{ countdown > 0 ? `重新发送 (${countdown}s)` : '发送动态验证码' }}
            </n-button>
          </form>

          <form v-else class="otp-verify" @submit.prevent="handleWeb2FAVerify">
            <n-input
              v-model:value="otpCode"
              placeholder="输入 6 位动态验证码"
              size="large"
              class="mb-3"
              maxlength="6"
              :input-props="{ inputmode: 'numeric', autocomplete: 'one-time-code', pattern: '\\d*' }"
              @keydown.enter="(e: KeyboardEvent) => onImeSafeEnter(e, handleWeb2FAVerify)"
            />
            <n-button attr-type="submit" type="primary" block size="large" class="action-btn" :loading="loading">
              解锁控制台
            </n-button>
            <div class="resend-text">
              没有收到？
              <button type="button" class="link-btn" :disabled="loading || countdown > 0" @click="handleWeb2FASend">
                {{ countdown > 0 ? `重新发送 (${countdown}s)` : '重新发送' }}
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  </n-modal>
</template>

<script setup lang="ts">
import { ref, computed, watch, onUnmounted } from 'vue'
import { NModal, NInput, NButton } from 'naive-ui'
import TelegramChallengePanel from './TelegramChallengePanel.vue'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'
import { message } from '@/utils/discreteApi'
import { errDetail } from '@/utils/errors'
import { onImeSafeEnter } from '@/utils/ime'

const store = useTelegramAuthStore()

// 判断模式
const isNativeMode = computed(() => store.authState === 'UNAUTHENTICATED')

const loading = ref(false)

// 倒计时管理
const countdown = ref(0)
let timer: ReturnType<typeof setInterval> | null = null

function startCountdown(seconds = 60) {
  clearCountdown()
  countdown.value = seconds
  timer = setInterval(() => {
    if (countdown.value > 1) {
      countdown.value--
    } else {
      clearCountdown()
    }
  }, 1000)
}

function clearCountdown() {
  if (timer) {
    clearInterval(timer)
    timer = null
  }
  countdown.value = 0
}

// 弹窗关闭或组件卸载时彻底清除定时器
watch(
  () => store.isModalVisible,
  (visible) => {
    if (!visible) {
      clearCountdown()
    }
  },
)

onUnmounted(() => {
  clearCountdown()
})

// Native Mode compatibility refs (exposed for legacy unit assertions)
const nativeCode = ref('')
const nativePassword = ref('')

// Web 2FA Mode Data
const otpSent = ref(false)
const otpCode = ref('')

async function handleNativeVerify() {
  if (loading.value) return
  const cleanCode = nativeCode.value.replace(/\D/g, '')
  if (!cleanCode) return
  loading.value = true
  try {
    await store.nativeVerifyCode(cleanCode, nativePassword.value || null)
  } finally {
    loading.value = false
  }
}

defineExpose({
  nativeCode,
  nativePassword,
  handleNativeVerify,
})

async function handleWeb2FASend() {
  if (loading.value || countdown.value > 0) return
  loading.value = true
  try {
    const success = await store.sendWeb2FA()
    if (success) {
      otpSent.value = true
      startCountdown(60)
    }
  } catch (err: unknown) {
    message.error(errDetail(err) || '发送 Web 2FA 验证码失败')
  } finally {
    loading.value = false
  }
}

async function handleWeb2FAVerify() {
  if (loading.value) return
  if (!otpCode.value.trim()) return
  loading.value = true
  try {
    await store.verifyWeb2FA(otpCode.value.trim())
  } finally {
    loading.value = false
  }
  // 如果成功，store会自动关闭弹窗并设置解锁标记
}
</script>

<style scoped>
.glass-modal-card {
  background: var(--bg-card);
  backdrop-filter: blur(16px);
  border: 1px solid var(--border-color);
  border-radius: 16px;
  padding: 2rem;
  width: 92vw;
  max-width: 440px;
  max-height: 88dvh;
  overflow-y: auto;
  box-shadow: var(--card-shadow);
  color: var(--text-primary);
}

@media (max-width: 640px) {
  .glass-modal-card {
    padding: 1.25rem;
  }
}

.modal-header h2 {
  margin: 0 0 0.5rem 0;
  font-size: 1.35rem;
  font-weight: 700;
  color: var(--text-primary);
}

.subtitle {
  color: var(--text-secondary);
  font-size: 0.88rem;
  margin-bottom: 1.5rem;
  line-height: 1.5;
}

.action-btn {
  margin-top: 1.5rem;
}

.mb-3 {
  margin-bottom: 1rem;
}
.mt-2 {
  margin-top: 0.5rem;
}

.info-box {
  background: var(--bg-card-hover);
  border-left: 4px solid var(--accent-color);
  padding: 1rem;
  border-radius: 0 8px 8px 0;
  font-size: 0.88rem;
  line-height: 1.6;
  color: var(--text-primary);
}

.resend-text {
  text-align: center;
  margin-top: 1.5rem;
  font-size: 0.85rem;
  color: var(--text-secondary);
}

.resend-text .link-btn {
  background: none;
  border: none;
  padding: 0;
  font: inherit;
  color: var(--accent-color);
  cursor: pointer;
  text-decoration: underline;
}

.resend-text .link-btn:disabled {
  color: var(--text-muted);
  cursor: not-allowed;
  pointer-events: none;
  text-decoration: none;
}

.code-channel-box {
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-left: 3px solid var(--accent-color);
  padding: 0.65rem 0.85rem;
  border-radius: 6px;
  font-size: 0.82rem;
  line-height: 1.5;
  color: var(--text-primary);
}
</style>
