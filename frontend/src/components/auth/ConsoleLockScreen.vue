<template>
  <main class="console-lock-screen">
    <section class="console-lock-card" aria-labelledby="console-lock-heading">
      <header class="console-lock-header">
        <div class="lock-icon" aria-hidden="true">🔒</div>
        <h1 id="console-lock-heading" class="lock-title">控制台已锁定</h1>
        <p class="lock-subtitle">检测到公网可达的部署入口，但服务端尚未配置 <code>API_KEY</code>。</p>
      </header>

      <div role="alert" class="lock-alert">
        <strong>部署者需要完成以下配置</strong>
        <ol class="lock-steps">
          <li>在部署环境的 <code>.env</code>（或系统环境变量）中设置强随机的 <code>API_KEY</code>；</li>
          <li>重启服务进程（Docker 部署执行 <code>docker compose up -d</code> 重建）；</li>
          <li>回到本页点击「重新检测」，使用 API Key 登录控制台。</li>
        </ol>
      </div>

      <p class="lock-note">
        仅本机直跑时，可以使用 <code>HOST=127.0.0.1</code> 和 <code>CONSOLE_REQUIRE_API_KEY=false</code>
        保留免认证。容器和公网反代需要 API Key。
      </p>

      <button type="button" class="primary-btn" :disabled="rechecking" @click="recheck">
        {{ rechecking ? '正在重新检测...' : '重新检测' }}
      </button>
    </section>
  </main>
</template>

<script setup lang="ts">
import { ref } from 'vue'

import { useTelegramAuthStore } from '@/stores/telegramAuthStore'

const tgStore = useTelegramAuthStore()
const rechecking = ref(false)

async function recheck(): Promise<void> {
  rechecking.value = true
  try {
    await tgStore.checkAdmission()
  } finally {
    rechecking.value = false
  }
}
</script>

<style scoped>
.console-lock-screen {
  min-height: 100vh;
  min-height: 100dvh;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 1.5rem;
}

.console-lock-card {
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
  text-align: center;
}

.console-lock-header {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 0.45rem;
}

.lock-icon {
  font-size: 2rem;
}

.lock-title {
  margin: 0;
  font-size: 1.35rem;
  font-weight: 700;
}

.lock-subtitle {
  margin: 0;
  font-size: 0.92rem;
  line-height: 1.5;
  color: var(--text-secondary, #64748b);
}

.lock-alert {
  padding: 0.95rem 1rem;
  border-radius: 10px;
  font-size: 0.9rem;
  line-height: 1.5;
  text-align: left;
  background: rgba(239, 68, 68, 0.1);
  border: 1px solid rgba(239, 68, 68, 0.35);
}

.lock-alert p {
  margin: 0.35rem 0 0;
}

.lock-steps {
  margin: 0.45rem 0 0;
  padding-left: 1.35rem;
  display: flex;
  flex-direction: column;
  gap: 0.3rem;
}

.lock-note {
  margin: 0;
  font-size: 0.82rem;
  line-height: 1.55;
  color: var(--text-secondary, #64748b);
  text-align: left;
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
</style>
