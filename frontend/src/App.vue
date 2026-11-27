<template>
  <n-config-provider :theme="themeStore.naiveTheme" :theme-overrides="themeStore.naiveThemeOverrides">
    <n-global-style />
    <div v-if="tgAuthStore.admissionPhase === 'checking'" class="bootstrap-gate-shell" role="status" aria-live="polite">
      <div class="bootstrap-gate-card">
        <div class="gate-spinner" aria-hidden="true" />
        <h2 class="gate-title">正在核验默认 Userbot 服务状态</h2>
      </div>
    </div>

    <div v-else-if="tgAuthStore.admissionPhase === 'error'" class="bootstrap-gate-shell" role="alert">
      <div class="bootstrap-gate-card">
        <div class="gate-icon" aria-hidden="true">⚠️</div>
        <h2 class="gate-title">无法获取服务准入状态</h2>
        <p class="gate-desc">{{ tgAuthStore.admissionError || '请确认后端服务已正常启动后重试。' }}</p>
        <button type="button" class="gate-retry-btn" @click="retryAdmission">重试连接</button>
      </div>
    </div>

    <BootstrapAuthScreen v-else-if="tgAuthStore.admissionPhase === 'restricted'" />

    <ConsoleLockScreen v-else-if="tgAuthStore.admissionPhase === 'locked'" />

    <AppLayout v-else />
  </n-config-provider>
</template>

<script setup lang="ts">
import { defineAsyncComponent, onMounted } from 'vue'
import { NConfigProvider, NGlobalStyle } from 'naive-ui'

import { useTelegramAuthStore } from '@/stores/telegramAuthStore'
import { useThemeStore } from '@/stores/themeStore'

const AppLayout = defineAsyncComponent(() => import('@/components/layout/AppLayout.vue'))
const BootstrapAuthScreen = defineAsyncComponent(() => import('@/components/auth/BootstrapAuthScreen.vue'))
const ConsoleLockScreen = defineAsyncComponent(() => import('@/components/auth/ConsoleLockScreen.vue'))

const themeStore = useThemeStore()
const tgAuthStore = useTelegramAuthStore()

async function retryAdmission(): Promise<void> {
  await tgAuthStore.checkAdmission()
}

onMounted(() => {
  void tgAuthStore.checkAdmission()
})
</script>

<style>
/* 全局基础重置与排版规范 */
html,
body,
#app {
  width: 100%;
  min-height: 100vh;
  margin: 0;
  padding: 0;
}

.bootstrap-gate-shell {
  min-height: 100vh;
  min-height: 100dvh;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 1.5rem;
}

.bootstrap-gate-card {
  width: 100%;
  max-width: 460px;
  padding: 2rem;
  border-radius: 14px;
  border: 1px solid rgba(148, 163, 184, 0.25);
  background: var(--surface-card, rgba(255, 255, 255, 0.92));
  text-align: center;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 0.85rem;
}

.gate-spinner {
  width: 32px;
  height: 32px;
  border-radius: 9999px;
  border: 3px solid rgba(148, 163, 184, 0.35);
  border-top-color: #2563eb;
  animation: gate-spin 0.8s linear infinite;
}

@keyframes gate-spin {
  to {
    transform: rotate(360deg);
  }
}

.gate-icon {
  font-size: 2rem;
}

.gate-title {
  margin: 0;
  font-size: 1.15rem;
  font-weight: 600;
}

.gate-desc {
  margin: 0;
  font-size: 0.9rem;
  line-height: 1.5;
  color: var(--text-secondary, #64748b);
}

.gate-retry-btn {
  margin-top: 0.5rem;
  padding: 0.55rem 1.25rem;
  border-radius: 8px;
  border: 0;
  background: #2563eb;
  color: #fff;
  font-weight: 600;
  cursor: pointer;
}
</style>
