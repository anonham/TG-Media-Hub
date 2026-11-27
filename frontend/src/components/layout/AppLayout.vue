<template>
  <div class="app-layout-root" :class="{ 'sidebar-collapsed': isSidebarCollapsed }">
    <!-- 左侧可折叠 SaaS 侧边栏 -->
    <Sidebar
      :collapsed="isSidebarCollapsed"
      :mobile-open="isMobileDrawerOpen"
      @toggle-collapse="toggleSidebar"
      @close-mobile="isMobileDrawerOpen = false"
    />

    <!-- 右侧主体内容容器 (包含顶栏与视口; 移动端侧边栏展开时通过 inert 隔离背景) -->
    <div class="app-main-wrapper" :inert="isMobileViewport && isMobileDrawerOpen ? true : undefined">
      <!-- 顶部 Header 状态栏 -->
      <HeaderBar
        ref="headerBarRef"
        :collapsed="isSidebarCollapsed"
        @toggle-collapse="toggleSidebar"
        @open-mobile="isMobileDrawerOpen = true"
      />

      <!-- Telegram 账号会话失效全局横幅 (WS system_event / 探测触发) -->
      <div v-if="tgAuthStore.sessionInvalidMessage" class="banner-container">
        <n-alert type="error" closable class="session-invalid-banner" @close="tgAuthStore.dismissSessionInvalid()">
          <template #header>⚠️ Telegram 账号会话已失效</template>
          {{ tgAuthStore.sessionInvalidMessage }}
          <n-button size="small" type="error" class="rebind-btn" @click="tgAuthStore.openRebindModal()">
            重新绑定账号
          </n-button>
        </n-alert>
      </div>

      <!-- 业务视口：鉴权就绪后渲染 SPA 路由视图 (2FA 会话失效仅弹窗引导, 不整页锁定) -->
      <main v-if="!isHardLocked" class="main-viewport-content">
        <router-view v-slot="{ Component, route }">
          <transition :name="supportsViewTransition ? undefined : 'fade-transform'" mode="out-in">
            <component :is="Component" :key="route.name || route.path" />
          </transition>
        </router-view>
      </main>

      <!-- API Key 层缺失时的安全锁定保护蒙层 -->
      <div v-else class="locked-placeholder">
        <div class="locked-card glass-card">
          <div class="locked-icon">🔒</div>
          <h3>控制台已进入安全防护锁定</h3>
          <p>请在弹出的认证窗口中输入 API Key 与 Telegram 动态口令以解锁控制台</p>
          <div class="locked-btn-row">
            <n-button
              v-if="authStore.isAuthRequired && (!authStore.apiKey || !authStore.isKeyVerified)"
              type="primary"
              secondary
              @click="authStore.openAuthModal('请输入 API Key 解锁控制台')"
            >
              🔑 输入 API Key 访问口令
            </n-button>
            <n-button
              v-else-if="!tgAuthStore.isWeb2FAUnlocked"
              type="primary"
              secondary
              @click="tgAuthStore.isModalVisible = true"
            >
              🛡️ 输入 Telegram 动态验证码
            </n-button>
          </div>
        </div>
      </div>
    </div>

    <!-- 🔒 毛玻璃 API Key 与 Telegram 2FA 身份验证弹窗 (按需懒加载，压降首屏主包体积) -->
    <AuthModal v-if="authStore.isModalVisible" />
    <TelegramAuthModal v-if="tgAuthStore.isModalVisible" />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, nextTick, onMounted, onUnmounted, watch, defineAsyncComponent } from 'vue'
import { NButton, NAlert } from 'naive-ui'

import Sidebar from './Sidebar.vue'
import HeaderBar from './HeaderBar.vue'

const AuthModal = defineAsyncComponent(() => import('@/components/auth/AuthModal.vue'))
const TelegramAuthModal = defineAsyncComponent(() => import('@/components/auth/TelegramAuthModal.vue'))

import { useAuthStore } from '@/stores/authStore'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'
import { useThemeStore } from '@/stores/themeStore'
import { useTaskStore } from '@/stores/taskStore'
import { useMobileViewport } from '@/utils/viewport'

const isMobileDrawerOpen = ref(false)
const headerBarRef = ref<InstanceType<typeof HeaderBar> | null>(null)
const { isMobileViewport } = useMobileViewport(768)

watch(isMobileViewport, (mobile) => {
  if (!mobile && isMobileDrawerOpen.value) {
    isMobileDrawerOpen.value = false
  }
})

watch(isMobileDrawerOpen, async (open, prevOpen) => {
  if (!open && prevOpen && isMobileViewport.value) {
    await nextTick()
    headerBarRef.value?.focusMobileMenuBtn()
  }
})

const supportsViewTransition = computed(() => {
  return typeof document !== 'undefined' && 'startViewTransition' in document && !themeStore.reduceMotion
})

const authStore = useAuthStore()
const tgAuthStore = useTelegramAuthStore()
const themeStore = useThemeStore()
const taskStore = useTaskStore()

// 侧边栏折叠态唯一真源在 themeStore: Sidebar/HeaderBar/SettingsView 三处实时联动
const isSidebarCollapsed = computed(() => themeStore.sidebarCollapsed)
const toggleSidebar = themeStore.toggleSidebar

const isReady = computed(() => {
  if (tgAuthStore.isRestricted || tgAuthStore.admissionPhase === 'error') return false
  if (!authStore.isAuthChecked) return false
  if (!authStore.isAuthRequired) return true
  return !!authStore.apiKey && authStore.isKeyVerified && tgAuthStore.isWeb2FAUnlocked
})

// 会话首次 2FA 解锁追踪: 区分"初次登录向导准入"与"运行期 2FA 会话过期软重验"
// (ref 初值已覆盖首帧状态, watch 无需 immediate)
const hasEverUnlocked = ref(tgAuthStore.isWeb2FAUnlocked)

watch(
  () => tgAuthStore.isWeb2FAUnlocked,
  (unlocked) => {
    if (unlocked) {
      hasEverUnlocked.value = true
    }
  },
)

// 鉴权准入与硬锁定门禁:
// 1. 若无需鉴权 (!isAuthRequired)，恒放行;
// 2. 缺失 API Key 时整页锁定;
// 3. 初次进入/会话从未解锁过 2FA 时，整页锁定并等待 Telegram 动态验证码输入，
//    杜绝未解锁前业务视图抢跑挂载导致并发 403 错误风暴;
// 4. 一旦首轮成功解锁 2FA (hasEverUnlocked)，后续运行期 2FA 过期不再销毁业务 DOM，
//    仅由全局拦截器弹出重验窗口并降级轮询。
const isHardLocked = computed(() => {
  if (tgAuthStore.isRestricted || tgAuthStore.admissionPhase === 'error') return true
  if (!authStore.isAuthChecked) return true
  if (!authStore.isAuthRequired) return false
  if (!authStore.apiKey || !authStore.isKeyVerified) return true
  return !hasEverUnlocked.value && !tgAuthStore.isWeb2FAUnlocked
})

async function initTelegramAuth() {
  // open-mode (未启用 API_KEY) 下后端 2FA 会话校验恒放行 (verify_2fa_session 对空配置
  // 返回真), 前端不再拦截 —— 否则开放部署被 TG 验证码门禁完全锁死 (Agent-UI 首轮实证)
  if (!authStore.isAuthRequired) return
  const tgPassed = await tgAuthStore.fetchStatus()
  if (tgPassed && !tgAuthStore.isWeb2FAUnlocked) {
    tgAuthStore.isModalVisible = true
  }
}

// 监听 API Key 变更 (登录后触发 Telegram 2FA 探测; 凭证注销时重置 2FA 首通态)
watch(
  () => (authStore.apiKey && authStore.isKeyVerified ? authStore.apiKey : ''),
  (newKey) => {
    if (newKey) {
      initTelegramAuth()
    } else if (!authStore.apiKey) {
      hasEverUnlocked.value = false
    }
  },
)

// 鉴权层优先级防叠加 (评审 P2 收口): API Key 层是 2FA 层的外层门 —— AuthModal 一经
// 开启 (handle401Error / WS 1008 / 清除凭证 / 密钥管理入口) 即收起 2FA 重验弹窗,
// 避免 handle403Error 的双向守卫覆盖不到的反向叠层。AuthModal 关闭后 2FA 重验窗
// 由下一次 403 拦截或锁定占位卡按钮重新拉起。
watch(
  () => authStore.isModalVisible,
  (visible) => {
    if (visible) {
      tgAuthStore.isModalVisible = false
    }
  },
)

// 监听完全就绪态 (API Key + Telegram 2FA): 就绪即启动轮询与 WS;
// 仅硬锁定才全停 —— 2FA 失效走只读轮询降级, WS 由 4003 分支停连并引导重验
watch(
  isReady,
  (ready) => {
    if (ready) {
      taskStore.startPolling()
      taskStore.initWebSocket()
    } else if (isHardLocked.value) {
      taskStore.stopPolling()
    }
  },
  { immediate: true },
)

onUnmounted(() => taskStore.stopPolling())

onMounted(async () => {
  const passed = await authStore.checkAuthStatus()
  if (passed || !authStore.isAuthRequired) {
    await initTelegramAuth()
  }
})
</script>

<style scoped>
.app-layout-root {
  --sidebar-collapsed-outer-width: calc(var(--sidebar-collapsed-width) + var(--safe-area-left));
  display: flex;
  min-height: 100vh;
  min-height: 100dvh;
  width: 100%;
}

.app-main-wrapper {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-height: 100vh;
  min-height: 100dvh;
  min-width: 0;
  margin-left: var(--sidebar-width);
  transition: margin-left 0.25s cubic-bezier(0.4, 0, 0.2, 1);
  overflow-x: clip;
}

.app-layout-root.sidebar-collapsed .app-main-wrapper {
  margin-left: var(--sidebar-collapsed-outer-width);
}

.banner-container {
  padding: 1rem max(1.5rem, var(--safe-area-right)) 0 max(1.5rem, var(--safe-area-left));
  max-width: 1400px;
  width: 100%;
  margin: 0 auto;
}

.session-invalid-banner {
  width: 100%;
}

.rebind-btn {
  margin-left: 12px;
}

.main-viewport-content {
  flex: 1;
  max-width: 1400px;
  width: 100%;
  margin: 0 auto;
  padding: 1.5rem max(1.5rem, var(--safe-area-right)) max(1.5rem, var(--safe-area-bottom))
    max(1.5rem, var(--safe-area-left));
  container: main-viewport / inline-size;
  view-transition-name: app-main-viewport;
}

.locked-placeholder {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 4rem 1.5rem;
}

.locked-card {
  text-align: center;
  padding: 3rem 2rem;
  max-width: 480px;
  width: 100%;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 1rem;
  border-radius: 1rem;
}

.locked-icon {
  font-size: 3rem;
}

.locked-card h3 {
  font-size: 1.3rem;
  font-weight: 600;
  margin: 0;
}

.locked-card p {
  color: var(--text-secondary);
  font-size: 0.95rem;
  margin: 0;
  line-height: 1.5;
}

.locked-btn-row {
  margin-top: 1rem;
}

@media (max-width: 768px) {
  .app-main-wrapper {
    margin-left: 0 !important;
  }
  .banner-container {
    padding: 0.75rem max(0.5rem, var(--safe-area-right)) 0 max(0.5rem, var(--safe-area-left));
  }
  .main-viewport-content {
    padding: 0.75rem max(0.5rem, var(--safe-area-right)) calc(1.25rem + var(--safe-area-bottom))
      max(0.5rem, var(--safe-area-left));
  }
  .locked-placeholder {
    padding: 2rem 0.75rem;
  }
  .locked-card {
    padding: 2rem 1.25rem;
  }
}
</style>
