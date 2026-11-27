<template>
  <n-modal
    :show="authStore.isModalVisible"
    :mask-closable="!authStore.isAuthRequired || !!authStore.apiKey"
    :close-on-esc="false"
    preset="card"
    class="auth-modal-card"
    :style="{ width: '90%', maxWidth: '440px' }"
    @update:show="handleClose"
  >
    <div class="auth-header">
      <div class="auth-icon">🔑</div>
      <h3 class="auth-title">Web 控制台身份验证</h3>
      <p class="auth-subtitle">
        {{ authStore.modalMessage || '当前服务已开启 API Key 加固防护，请输入访问口令解封控制台' }}
      </p>
    </div>

    <form class="auth-body" @submit.prevent="submit">
      <div v-if="authStore.isLocked" class="lock-alert">
        ⚠️ 连续输入错误 3 次，触发防爆破锁定机制！请等待 <b>{{ authStore.lockCountdown }}</b> 秒后重试。
      </div>

      <div v-else-if="authStore.failedAttempts > 0" class="error-alert">
        ⚠️ 凭证校验失败！剩余可重试次数: {{ 3 - authStore.failedAttempts }} 次
      </div>

      <div class="auth-actions">
        <n-input-group>
          <n-input
            v-model:value="inputKey"
            type="password"
            show-password-on="click"
            size="large"
            placeholder="请输入 X-API-Key..."
            :input-props="{ autocomplete: 'current-password' }"
            @keydown.enter="(e: KeyboardEvent) => onImeSafeEnter(e, submit)"
            :disabled="authStore.isLocked"
          >
            <template #prefix>
              <span class="lock-icon">🔒</span>
            </template>
          </n-input>
          <n-button
            attr-type="submit"
            type="primary"
            size="large"
            :disabled="authStore.isLocked || !inputKey.trim()"
            :loading="loading"
          >
            {{ authStore.isLocked ? `${authStore.lockCountdown}s` : '验证' }}
          </n-button>
        </n-input-group>

        <div class="options-row">
          <n-checkbox v-model:checked="rememberMe"> 保持登录状态 (24小时) </n-checkbox>
        </div>

        <n-button
          v-if="authStore.apiKey"
          attr-type="button"
          type="error"
          secondary
          block
          @click="authStore.clearAuthKey()"
        >
          清除保存凭证
        </n-button>
      </div>
    </form>
  </n-modal>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { NModal, NInput, NInputGroup, NButton, NCheckbox } from 'naive-ui'
import { useAuthStore } from '@/stores/authStore'
import { onImeSafeEnter } from '@/utils/ime'

const authStore = useAuthStore()

const inputKey = ref('')
const rememberMe = ref(false)
const loading = ref(false)

async function submit() {
  if (loading.value || authStore.isLocked || !inputKey.value.trim()) return
  loading.value = true
  try {
    const success = await authStore.submitAuthKey(inputKey.value, rememberMe.value)
    if (success) {
      inputKey.value = ''
    }
  } finally {
    loading.value = false
  }
}

function handleClose(val: boolean) {
  if (!val && (!authStore.isAuthRequired || authStore.apiKey)) {
    authStore.isModalVisible = false
  }
}
</script>

<style scoped>
.auth-header {
  text-align: center;
  margin-bottom: 1.5rem;
}

.auth-icon {
  font-size: 2.8rem;
  line-height: 1;
  margin-bottom: 0.5rem;
}

.auth-title {
  font-size: 1.25rem;
  font-weight: 700;
  color: var(--text-primary);
}

.auth-subtitle {
  font-size: 0.85rem;
  color: var(--text-secondary);
  margin-top: 0.25rem;
}

.auth-body {
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
}

.lock-alert {
  background: rgba(239, 68, 68, 0.15);
  border: 1px solid rgba(239, 68, 68, 0.3);
  color: var(--danger-color);
  padding: 0.65rem 0.85rem;
  border-radius: 0.5rem;
  font-size: 0.85rem;
  line-height: 1.4;
}

.error-alert {
  background: rgba(245, 158, 11, 0.15);
  border: 1px solid rgba(245, 158, 11, 0.3);
  color: var(--warning-color);
  padding: 0.65rem 0.85rem;
  border-radius: 0.5rem;
  font-size: 0.85rem;
}

.auth-actions {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  margin-top: 0.5rem;
}

.options-row {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  padding: 0.2rem 0;
}
</style>

<style>
.auth-modal-card {
  max-height: 88dvh;
  overflow-y: auto;
}
</style>
