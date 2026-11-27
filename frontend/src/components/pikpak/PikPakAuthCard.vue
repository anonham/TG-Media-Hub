<template>
  <div class="card glass-card">
    <div class="card-header">
      <div class="card-title">☁️ PikPak 账号鉴权</div>
    </div>

    <!-- 冻结文案: token_source='env' 时禁用扫码登录, 全页不提供任何凭据编辑入口 -->
    <n-alert v-if="envTakeover" type="info" class="frozen-alert"> 当前由环境变量凭据接管 </n-alert>

    <div v-if="pikpakStore.authStatusLoading" class="auth-loading">
      <n-spin size="small" />
      <span class="text-secondary">正在获取 PikPak 鉴权状态…</span>
    </div>

    <template v-else-if="authStatus">
      <!-- 传输层兜底 (宿主不可达/401/网络断): 渲染「连接异常」警告条, 非业务三态 -->
      <div v-if="authStatus.fetchError" class="auth-block">
        <n-alert type="warning" :show-icon="true"> 连接异常: {{ authStatus.fetchError }} </n-alert>
        <n-button size="small" secondary class="retry-btn" :loading="pikpakStore.authStatusLoading" @click="retryAuth">
          重新检测
        </n-button>
      </div>

      <template v-else>
        <div class="auth-row">
          <n-tag :type="authTagType" round>
            {{ authTagText }}
          </n-tag>
          <n-tag v-if="authStatus.token_source === 'env'" type="info" size="small" round>env</n-tag>
          <span v-if="authStatus.user_name || authStatus.email" class="auth-account">
            {{ authStatus.user_name || '—' }}<template v-if="authStatus.email"> · {{ authStatus.email }}</template>
          </span>
          <span v-if="authStatus.state_reason" class="auth-state-reason text-secondary">
            {{ authStatus.state_reason }}
          </span>
          <n-tag v-if="authStatus.vip_type" size="small" type="warning" round>{{ authStatus.vip_type }}</n-tag>
          <span class="flex-spacer"></span>
          <n-button v-if="!envTakeover && !authStatus.logged_in" type="primary" @click="openDeviceModal">
            设备码登录绑定
          </n-button>
        </div>

        <div v-if="storage" class="storage-row font-mono">
          <div class="storage-label">
            网盘空间: {{ formatBytes(storage.used_bytes) }} / {{ formatBytes(storage.total_bytes) }}
            <template v-if="storage.in_trash_bytes != null">
              · 回收站占用 {{ formatBytes(storage.in_trash_bytes) }}</template
            >
          </div>
          <n-progress
            v-if="storage.total_bytes"
            type="line"
            :height="8"
            :show-indicator="false"
            :percentage="storagePercent"
          />
          <div v-if="authStatus.trash_duration_s" class="text-secondary trash-hint">
            回收站保留期约 {{ Math.max(1, Math.round(authStatus.trash_duration_s / 86400)) }} 天
          </div>
        </div>
      </template>
    </template>

    <!-- 设备码扫码登录弹窗 -->
    <n-modal
      v-model:show="deviceModalShow"
      preset="card"
      title="PikPak 设备码登录"
      class="device-modal"
      @after-leave="pikpakStore.stopDeviceFlow()"
    >
      <div class="device-flow">
        <template v-if="deviceFlow.phase === 'polling'">
          <div class="device-step">1️⃣ 打开授权页，输入以下用户码并确认授权:</div>
          <div class="user-code font-mono" title="点击复制" @click="copyUserCode">
            {{ deviceFlow.userCode || '—' }}
          </div>
          <div class="device-actions">
            <a
              v-if="safeVerificationUri"
              :href="safeVerificationUri"
              target="_blank"
              rel="noopener noreferrer"
              style="text-decoration: none"
            >
              <n-button type="primary"> 打开授权页 </n-button>
            </a>
            <n-button v-else type="primary" disabled> 打开授权页 </n-button>
            <n-button quaternary @click="copyUserCode">复制用户码</n-button>
          </div>
          <div class="poll-state">
            <n-spin size="small" />
            <span>等待授权确认…（每 {{ deviceFlow.interval || 5 }} 秒自动查询一次）</span>
          </div>
          <div v-if="deviceFlow.expiresAt" class="text-secondary expiry-hint">
            设备码有效期至: {{ formatExpiry(deviceFlow.expiresAt) }}
          </div>
        </template>

        <template v-else-if="deviceFlow.phase === 'success'">
          <n-alert type="success" :show-icon="true">PikPak 账号绑定成功，窗口即将关闭…</n-alert>
        </template>

        <template v-else-if="deviceFlow.phase === 'failed'">
          <n-alert type="error" :show-icon="true">
            授权未完成<template v-if="deviceFlow.lastStatus">（状态: {{ deviceFlow.lastStatus }}）</template>
          </n-alert>
          <div v-if="deviceFlow.errorMsg" class="text-secondary fail-hint">{{ deviceFlow.errorMsg }}</div>
          <div class="device-actions">
            <n-button type="primary" secondary @click="pikpakStore.startDeviceLogin()">重新发起授权</n-button>
          </div>
        </template>

        <template v-else>
          <div class="poll-state">
            <n-spin size="small" />
            <span>正在发起设备码登录…</span>
          </div>
        </template>
      </div>
    </n-modal>
  </div>
</template>

<script setup lang="ts">
// PikPak 鉴权卡: 三态状态展示 / 网盘空间进度条 / 三态自愈重探测 / 设备码扫码登录弹窗。
// 鉴权数据与设备码流全部来自 pikpakStore, 组件只承载展示与生命周期。
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { NAlert, NButton, NModal, NProgress, NSpin, NTag } from 'naive-ui'
import { usePikpakStore } from '@/stores/pikpakStore'
import { copyTextToClipboard } from '@/utils/clipboard'
import { formatBytes } from '@/utils/formatters'

const pikpakStore = usePikpakStore()

// ── 鉴权卡片 (三态: logged_in=已登录 | not_bound=未绑定 | unreachable=连接异常) ──
const authStatus = computed(() => pikpakStore.authStatus)
const envTakeover = computed(() => authStatus.value?.token_source === 'env')
const storage = computed(() => authStatus.value?.storage || null)
const storagePercent = computed(() => {
  const s = storage.value
  if (!s?.total_bytes) return 0
  const used = s.used_bytes || 0
  return Math.min(100, Math.max(0, parseFloat(((used / s.total_bytes) * 100).toFixed(1))))
})
const authTagType = computed(() => {
  if (authStatus.value?.logged_in) return 'success'
  return authStatus.value?.state === 'unreachable' ? 'error' : 'warning'
})
const authTagText = computed(() => {
  if (authStatus.value?.logged_in) return '已登录'
  return authStatus.value?.state === 'unreachable' ? '连接异常' : '未绑定'
})
const safeVerificationUri = computed(() => {
  const raw = pikpakStore.deviceFlow.verificationUri
  if (!raw) return ''
  try {
    const parsed = new URL(raw)
    return parsed.protocol === 'https:' ? parsed.href : ''
  } catch {
    return ''
  }
})

// ── 三态自愈: 非已登录态自动退避重探测 (5/15/30/60s 封顶; 设备码轮询期让位) ──
const REPROBE_DELAYS_S = [5, 15, 30, 60]
let reprobeIdx = 0
let reprobeTimer: ReturnType<typeof setTimeout> | null = null
let successCloseTimer: ReturnType<typeof setTimeout> | null = null
let isUnmounted = false

function stopReprobeTimer() {
  if (reprobeTimer) {
    clearTimeout(reprobeTimer)
    reprobeTimer = null
  }
}

function clearSuccessCloseTimer() {
  if (successCloseTimer) {
    clearTimeout(successCloseTimer)
    successCloseTimer = null
  }
}

function scheduleReprobe() {
  stopReprobeTimer()
  if (isUnmounted) return
  const delayS = REPROBE_DELAYS_S[Math.min(reprobeIdx, REPROBE_DELAYS_S.length - 1)]
  reprobeTimer = setTimeout(async () => {
    reprobeTimer = null
    if (isUnmounted) return
    // 设备码轮询期让位 (轮询成功出口自带 fetchAuthStatus(true)); loading 中防重入: 同档重排不进位
    if (pikpakStore.deviceFlow.phase === 'polling' || pikpakStore.authStatusLoading) {
      scheduleReprobe()
      return
    }
    await pikpakStore.fetchAuthStatus(true)
    if (isUnmounted) return
    if (pikpakStore.authStatus?.state !== 'logged_in') {
      reprobeIdx = Math.min(reprobeIdx + 1, REPROBE_DELAYS_S.length - 1)
      scheduleReprobe()
    }
    // logged_in: 后续收尾由 state watch 接管 (停表 + 归零)
  }, delayS * 1000)
}

async function retryAuth() {
  // 手动重试一律 force=true (穿透 60s 成功缓存); 链路节奏由 state watch 接管, 手动重试不重置档位
  await pikpakStore.fetchAuthStatus(true)
}

watch(
  () => authStatus.value?.state,
  (state) => {
    if (state === 'logged_in') {
      stopReprobeTimer()
      reprobeIdx = 0
      return
    }
    if (!isUnmounted && !reprobeTimer) scheduleReprobe()
  },
)

// ── 设备码登录 Modal ─────────────────────────────────────────
const deviceModalShow = ref(false)
const deviceFlow = computed(() => pikpakStore.deviceFlow)

watch(
  () => deviceFlow.value.phase,
  (phase) => {
    clearSuccessCloseTimer()
    if (phase === 'success' && !isUnmounted) {
      successCloseTimer = setTimeout(() => {
        successCloseTimer = null
        if (isUnmounted) return
        deviceModalShow.value = false
        pikpakStore.stopDeviceFlow()
      }, 900)
    }
  },
)

watch(deviceModalShow, (show) => {
  if (!show) {
    clearSuccessCloseTimer()
    pikpakStore.stopDeviceFlow()
  }
})

function openDeviceModal() {
  pikpakStore.startDeviceLogin()
  deviceModalShow.value = true
}

// 未绑定空态的绑定入口由 FileBrowser 发出 bind 事件, 父视图经此转发打开设备码弹窗
defineExpose({ openDeviceModal })

async function copyUserCode() {
  const code = deviceFlow.value.userCode
  if (!code) return
  await copyTextToClipboard(code, '用户码已复制')
}

function formatExpiry(ts: number): string {
  return new Date(ts).toLocaleTimeString()
}

// 鉴权探测最先执行 (教训: 任何前置异常不得阻断鉴权卡渲染)
onMounted(async () => {
  await pikpakStore.fetchAuthStatus()
})

onUnmounted(() => {
  isUnmounted = true
  stopReprobeTimer()
  clearSuccessCloseTimer()
  pikpakStore.stopDeviceFlow()
})
</script>

<style scoped>
.card {
  padding: 1.5rem;
}

.card-header {
  border-bottom: 1px solid var(--border-color);
  padding-bottom: 1rem;
  margin-bottom: 1.25rem;
}

.card-title {
  font-size: 1.15rem;
  font-weight: 600;
}

.text-secondary {
  color: var(--text-secondary);
}

.flex-spacer {
  flex: 1;
}

.frozen-alert {
  margin-bottom: 1rem;
}

/* 鉴权卡片 */
.auth-loading {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.auth-block {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.retry-btn {
  align-self: flex-start;
}

.auth-row {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  flex-wrap: wrap;
}

.auth-account {
  font-weight: 500;
}

.auth-state-reason {
  font-size: 0.85rem;
}

.storage-row {
  margin-top: 1rem;
  display: flex;
  flex-direction: column;
  gap: 0.4rem;
}

.storage-label {
  font-size: 0.85rem;
}

.trash-hint {
  font-size: 0.8rem;
}

/* 设备码登录 Modal */
.device-modal {
  max-width: 480px;
}

.device-flow {
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.device-step {
  font-size: 0.9rem;
}

.user-code {
  font-size: 2rem;
  letter-spacing: 0.35rem;
  text-align: center;
  font-weight: 700;
  padding: 1rem;
  border: 1px dashed var(--border-color);
  border-radius: 10px;
  cursor: pointer;
  user-select: all;
}

.device-actions {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 0.75rem;
}

.poll-state {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 0.75rem;
  font-size: 0.88rem;
}

.expiry-hint,
.fail-hint {
  text-align: center;
  font-size: 0.82rem;
}
</style>
