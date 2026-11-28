<template>
  <div class="task-create-container">
    <!-- 顶部模式选择器 -->
    <div class="create-header-card glass-card">
      <div class="tab-control-wrapper">
        <div class="tab-buttons" role="tablist" aria-label="任务模式切换" @keydown="handleTablistArrowNav">
          <button
            type="button"
            role="tab"
            id="create-tab-download"
            :aria-selected="currentTab === 'download'"
            :tabindex="currentTab === 'download' ? 0 : -1"
            data-testid="tab-download"
            class="mode-btn"
            :class="{ active: currentTab === 'download' }"
            @click="switchTab('download')"
          >
            <span class="mode-icon">📥</span>
            <div class="mode-text">
              <span class="mode-title">受限媒体下载</span>
              <span class="mode-sub">单条/批量链接与范围消息抓取</span>
            </div>
          </button>

          <button
            type="button"
            role="tab"
            id="create-tab-publish"
            :aria-selected="currentTab === 'publish'"
            :tabindex="currentTab === 'publish' ? 0 : -1"
            data-testid="tab-publish"
            class="mode-btn"
            :class="{ active: currentTab === 'publish' }"
            @click="switchTab('publish')"
          >
            <span class="mode-icon">📤</span>
            <div class="mode-text">
              <span class="mode-title">本地文件夹发布</span>
              <span class="mode-sub">相册合并与 4-Bot 轮询上传</span>
            </div>
          </button>

          <button
            type="button"
            role="tab"
            id="create-tab-forward"
            :aria-selected="currentTab === 'forward'"
            :tabindex="currentTab === 'forward' ? 0 : -1"
            data-testid="tab-forward"
            class="mode-btn"
            :class="{ active: currentTab === 'forward' }"
            @click="switchTab('forward')"
          >
            <span class="mode-icon">⚡</span>
            <div class="mode-text">
              <span class="mode-title">云端极速转发</span>
              <span class="mode-sub">跨频道秒传与覆盖区间可视化</span>
            </div>
          </button>
        </div>
      </div>
    </div>

    <!-- 业务表单区域 (KeepAlive 按需挂载: 仅激活的子视图发起初始化请求) -->
    <div class="tab-content-wrapper" role="tabpanel" :aria-labelledby="`create-tab-${currentTab}`">
      <KeepAlive>
        <component :is="activeTabComponent" />
      </KeepAlive>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch, defineAsyncComponent, type Component } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { handleTablistArrowNav } from '@/utils/tablistNavigation'

const DownloadView = defineAsyncComponent(() => import('./DownloadView.vue'))
const PublishView = defineAsyncComponent(() => import('./PublishView.vue'))
const ForwardView = defineAsyncComponent(() => import('./ForwardView.vue'))

const route = useRoute()
const router = useRouter()

type TabKey = 'download' | 'publish' | 'forward'
const currentTab = ref<TabKey>('download')

// v-show 三视图同帧挂载 → 3 倍预设/账号请求; 改为 KeepAlive 单实例按需挂载,
// 切换标签保留表单状态, 离开页面再进入也只重挂当前激活视图
const tabComponents: Record<TabKey, Component> = {
  download: DownloadView,
  publish: PublishView,
  forward: ForwardView,
}
const activeTabComponent = computed(() => tabComponents[currentTab.value] || DownloadView)

function switchTab(tab: TabKey) {
  currentTab.value = tab
  router.replace({ query: { ...route.query, tab } })
}

function isValidTab(val: unknown): val is TabKey {
  return typeof val === 'string' && (val === 'download' || val === 'publish' || val === 'forward')
}

onMounted(() => {
  if (isValidTab(route.query.tab)) {
    currentTab.value = route.query.tab
  }
})

watch(
  () => route.query.tab,
  (newTab) => {
    if (isValidTab(newTab)) {
      currentTab.value = newTab
    }
  },
)
</script>

<style scoped>
.task-create-container {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
  width: 100%;
  container: create-root / inline-size;
}

.create-header-card {
  padding: 0.75rem;
}

.tab-buttons {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 0.75rem;
}

.mode-btn {
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-radius: 12px;
  padding: 0.85rem 1.25rem;
  display: flex;
  align-items: center;
  gap: 0.85rem;
  min-width: 0;
  cursor: pointer;
  transition: all 0.2s ease;
  text-align: left;
}

.mode-btn:hover {
  background: var(--bg-primary);
  border-color: var(--border-color-focus);
}

.mode-btn.active {
  background: var(--accent-light);
  border-color: var(--accent-color);
  box-shadow: var(--card-shadow);
}

.mode-icon {
  font-size: 1.6rem;
  flex-shrink: 0;
}

.mode-text {
  display: flex;
  flex-direction: column;
  gap: 0.2rem;
  overflow: hidden;
}

.mode-title {
  font-size: 0.95rem;
  font-weight: 600;
  color: var(--text-primary);
}

.mode-btn.active .mode-title {
  color: var(--accent-color);
  font-weight: 700;
}

.mode-sub {
  font-size: 0.75rem;
  color: var(--text-secondary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.tab-content-wrapper {
  display: flex;
  flex-direction: column;
}

@container create-root (max-width: 756px) {
  .tab-buttons {
    grid-template-columns: 1fr;
  }
}
</style>
