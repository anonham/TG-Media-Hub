<template>
  <div class="settings-page-wrapper">
    <div class="settings-top-header">
      <div class="header-title-col">
        <h1 class="page-title">应用设置</h1>
      </div>
      <div class="segmented-pill-container" role="tablist" aria-label="设置分类标签页" @keydown="handleTablistArrowNav">
        <button
          v-for="tab in tabList"
          :key="tab.key"
          :id="`settings-tab-${tab.key}`"
          type="button"
          role="tab"
          :aria-selected="currentTab === tab.key"
          :tabindex="currentTab === tab.key ? 0 : -1"
          class="segmented-pill-item"
          :class="{ active: currentTab === tab.key }"
          @click="currentTab = tab.key"
        >
          {{ tab.label }}
        </button>
      </div>
      <div class="header-action-col">
        <n-button
          v-if="currentTab !== 'about'"
          secondary
          size="small"
          round
          :loading="settingStore.loadingSettings"
          @click="settingStore.loadSettings()"
        >
          <template #icon>
            <n-icon><RefreshOutline /></n-icon>
          </template>
          重新加载
        </n-button>
        <n-button
          v-if="currentTab !== 'about'"
          type="primary"
          size="small"
          round
          :loading="settingStore.savingSettings"
          @click="handleSaveSettings"
        >
          <template #icon>
            <n-icon><SaveOutline /></n-icon>
          </template>
          保存【{{ currentTabLabel }}】
        </n-button>
      </div>
    </div>

    <!-- 主体内容卡片区域 -->
    <div class="settings-content-body">
      <!-- 1. 通用偏好 Tab -->
      <div
        v-if="currentTab === 'general'"
        role="tabpanel"
        aria-labelledby="settings-tab-general"
        class="settings-tab-pane"
      >
        <div class="settings-section">
          <div class="section-indicator">通用界面偏好</div>
          <div class="cockpit-card setting-card-group">
            <div class="setting-row-item">
              <div class="setting-row-info">
                <div class="setting-row-title">应用主题</div>
                <div class="setting-row-desc">切换深色、浅色模式或跟随系统首选项自动适配</div>
              </div>
              <div class="setting-row-control">
                <span data-testid="theme-select">
                  <n-select
                    :value="themeStore.themeMode"
                    :options="themeOptions"
                    size="small"
                    class="responsive-control-md"
                    @update:value="themeStore.setThemeMode"
                  />
                </span>
              </div>
            </div>
            <div class="setting-row-item">
              <div class="setting-row-info">
                <div class="setting-row-title">减少动画</div>
                <div class="setting-row-desc">降低页面淡入、弹层过渡、阴影、模糊和平滑滚动，仅保留必要加载反馈</div>
              </div>
              <div class="setting-row-control">
                <n-switch :value="themeStore.reduceMotion" @update:value="themeStore.setReduceMotion" />
              </div>
            </div>
            <!-- 侧边栏布局: 唯一真源 themeStore.sidebarCollapsed, Sidebar/AppLayout/设置页三处实时联动 -->
            <div class="setting-row-item">
              <div class="setting-row-info">
                <div class="setting-row-title">侧边栏布局</div>
                <div class="setting-row-desc">切换侧边栏展开/折叠形态，实时生效并跨页同步</div>
              </div>
              <div class="setting-row-control">
                <n-select
                  :value="themeStore.sidebarCollapsed ? 'collapsed' : 'standard'"
                  :options="sidebarOptions"
                  size="small"
                  class="responsive-control-md"
                  @update:value="(v) => themeStore.setSidebarCollapsed(v === 'collapsed')"
                />
              </div>
            </div>
          </div>
        </div>

        <!-- 存储目录 -->
        <div class="settings-section">
          <div class="section-indicator">备份与存储目录</div>
          <div class="cockpit-card setting-card-group">
            <div class="setting-row-item">
              <div class="setting-row-info">
                <div class="setting-row-title">备份目录 (只读)</div>
                <div class="setting-row-desc">
                  目录由后端环境变量 DATA_DIR 与 DOWNLOAD_DIR 控制，如需迁移请修改部署配置。
                </div>
                <div class="path-display-box font-mono">
                  {{ storageStore.overview.download_dir || 'downloads' }}
                </div>
              </div>
            </div>
          </div>
        </div>

        <div v-if="settingStore.loadingSettings" class="loading-placeholder cockpit-card">
          <n-spin size="medium" />
          <span style="margin-left: 12px">正在读取系统动态配置...</span>
        </div>
        <SettingDynamicCards
          v-else
          :cards="currentSettingCards"
          :form-values="formValues"
          @update:value="handleSettingValueUpdate"
        />
      </div>

      <!-- 2. 业务 Tab 动态配置卡片 (下载管理 / 频道转发 / 本地发布 / PikPak 搬运) -->
      <div
        v-else-if="currentTab !== 'about'"
        role="tabpanel"
        :aria-labelledby="`settings-tab-${currentTab}`"
        class="settings-tab-pane"
      >
        <div v-if="settingStore.loadingSettings" class="loading-placeholder cockpit-card">
          <n-spin size="medium" />
          <span style="margin-left: 12px">正在读取系统动态配置...</span>
        </div>
        <template v-else>
          <SettingDynamicCards
            :cards="currentSettingCards"
            :form-values="formValues"
            @update:value="handleSettingValueUpdate"
          />
          <!-- 中转集群诊断看板仅属于下载管理模块 -->
          <TransitDiagnosticPanel v-if="currentTab === 'download'" />
        </template>
      </div>

      <!-- 3. 关于 Tab -->
      <div v-else role="tabpanel" aria-labelledby="settings-tab-about" class="settings-tab-pane">
        <AboutSystemPanel :form-values="formValues" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
// 系统配置中心主视图: 页面骨架 + 界面偏好真值编辑 + 保存守卫;
// 静态元数据/校验规则在 utils/settingsMetadata.ts, 动态卡渲染器/中转诊断看板/关于面板
// 内聚于 components/settings/ 三子组件。
import { ref, computed, watch, onMounted } from 'vue'
import { NButton, NIcon, NSelect, NSwitch, NSpin } from 'naive-ui'
import { RefreshOutline, SaveOutline } from '@vicons/ionicons5'
import { useSettingStore } from '@/stores/settingStore'
import { useThemeStore } from '@/stores/themeStore'
import { useStorageStore } from '@/stores/storageStore'
import { message } from '@/utils/discreteApi'
import {
  SETTING_META_MAP,
  SETTING_CARD_LAYOUT,
  formatItemTitle,
  formatItemDesc,
  isNumericKey,
  validateSettingValue,
} from '@/utils/settingsMetadata'
import type { SystemSettingItem } from '@/api/gen'
import SettingDynamicCards from '@/components/settings/SettingDynamicCards.vue'
import TransitDiagnosticPanel from '@/components/settings/TransitDiagnosticPanel.vue'
import AboutSystemPanel from '@/components/settings/AboutSystemPanel.vue'
import { handleTablistArrowNav } from '@/utils/tablistNavigation'

const settingStore = useSettingStore()
const themeStore = useThemeStore()
const storageStore = useStorageStore()

const currentTab = ref('general')
const tabList = [
  { key: 'general', label: '通用设置' },
  { key: 'download', label: '下载管理' },
  { key: 'forward', label: '频道转发' },
  { key: 'local_publish', label: '本地发布' },
  { key: 'pikpak', label: 'PikPak 搬运' },
  { key: 'about', label: '关于与系统' },
]
const currentTabLabel = computed(() => tabList.find((t) => t.key === currentTab.value)?.label || '配置')

const themeOptions = [
  { label: '跟随系统 (System)', value: 'system' },
  { label: '深色模式 (Dark)', value: 'dark' },
  { label: '浅色模式 (Light)', value: 'light' },
]
const sidebarOptions = [
  { label: '标准展开布局', value: 'standard' },
  { label: '经典折叠布局', value: 'collapsed' },
]

// 动态参数表单管理
const formValues = ref<Record<string, string | number | boolean | null>>({})

function handleSettingValueUpdate(key: string, val: string | number | boolean | null) {
  formValues.value[key] = val
}

function syncFormValues() {
  const map: Record<string, string | number | boolean | null> = {}
  for (const [k, v] of Object.entries(settingStore.allSettings || {})) {
    map[k] = isNumericKey(k) ? Number(v) : v
  }
  // Level 1: PikPak 默认超限策略三级安全兜底注入
  map.pikpak_default_oversize_policy =
    (map.pikpak_default_oversize_policy as string) ||
    settingStore.allSettings?.pikpak_default_oversize_policy ||
    'skip_and_report'
  map.pikpak_default_split_strategy =
    (map.pikpak_default_split_strategy as string) ||
    settingStore.allSettings?.pikpak_default_split_strategy ||
    'vsse_with_fallback'
  formValues.value = map
}

watch(
  () => settingStore.allSettings,
  () => {
    syncFormValues()
  },
  { deep: true },
)

const settingCategoryItems = computed(() => {
  const byKey: Record<string, SystemSettingItem> = {}
  for (const items of Object.values(settingStore.categories || {})) {
    for (const item of items || []) byKey[item.key] = item
  }
  return byKey
})

const currentSettingCards = computed(() => {
  const tab = currentTab.value
  const settings = settingStore.allSettings || {}
  return (SETTING_CARD_LAYOUT[tab] || []).map((card) => {
    if (card.kind) return { ...card, items: [] }
    const items = Object.entries(settings)
      .filter(([key]) =>
        SETTING_META_MAP[key]
          ? SETTING_META_MAP[key].tab === tab && SETTING_META_MAP[key].card === card.key
          : tab === 'general' && card.key === 'other',
      )
      .map(([key, value]) => {
        const meta = SETTING_META_MAP[key] || {}
        const sourceItem = settingCategoryItems.value[key] || {}
        return {
          key,
          value,
          meta,
          title: meta.title || formatItemTitle(key),
          description: formatItemDesc({ key, description: sourceItem.description || undefined }),
        }
      })
    return { ...card, items }
  })
})

function getScopedTabKeys(tabKey: string): Set<string> {
  const keys = new Set<string>()
  for (const [k, meta] of Object.entries(SETTING_META_MAP)) {
    if (meta.tab === tabKey && !meta.readOnly) keys.add(k)
  }
  if (tabKey === 'general') {
    for (const k of Object.keys(settingStore.allSettings || {})) {
      if (!SETTING_META_MAP[k]) keys.add(k)
    }
    for (const k of Object.keys(formValues.value || {})) {
      if (!SETTING_META_MAP[k]) keys.add(k)
    }
  }
  return keys
}

async function handleSaveSettings() {
  const scopedKeys = getScopedTabKeys(currentTab.value)
  const tabPayload: Record<string, string> = {}
  for (const k of scopedKeys) {
    const v = formValues.value[k]
    if (v === undefined || v === null) continue
    tabPayload[k] = String(v)
  }
  // 局部条件校验统一收敛至 settingsMetadata.validateSettingValue (Scoped Validation)
  if (tabPayload.pikpak_proxy_url !== undefined) {
    tabPayload.pikpak_proxy_url = tabPayload.pikpak_proxy_url.trim()
  }
  for (const [k, v] of Object.entries(tabPayload)) {
    const err = validateSettingValue(k, v)
    if (err) {
      message.error(err)
      return
    }
  }
  try {
    await settingStore.updateSettings(tabPayload)
  } catch {
    // 保存失败已由 settingStore toast, 此处吞掉 rejection 仅走 finally 复位 (硬规则 5 写法收敛)
  }
}

onMounted(async () => {
  await settingStore.loadSettings()
  syncFormValues()
})
</script>

<style scoped>
.settings-page-wrapper {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
  width: 100%;
  max-width: 1200px;
  margin: 0 auto;
  padding-bottom: 3rem;
  container: settings-shell / inline-size;
}
.settings-top-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
  flex-wrap: wrap;
  padding: 0.5rem 0 1rem;
}
.page-title {
  font-size: 1.75rem;
  font-weight: 700;
  color: #1e3a8a; /* 稍深的大标题蓝调 */
  letter-spacing: -0.02em;
  margin: 0;
}
[data-theme='dark'] .page-title {
  color: #93c5fd;
}
.header-action-col {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.75rem;
}
.settings-content-body {
  display: flex;
  flex-direction: column;
  gap: 2rem;
}
.settings-tab-pane {
  display: flex;
  flex-direction: column;
  gap: 2rem;
}
.settings-section {
  display: flex;
  flex-direction: column;
}
.setting-card-group {
  overflow: hidden;
}
.path-display-box {
  background: var(--bg-primary);
  border: 1px solid var(--border-color);
  padding: 0.45rem 0.75rem;
  border-radius: 6px;
  font-size: 0.82rem;
  color: var(--text-secondary);
  margin-top: 0.4rem;
  display: inline-block;
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
@container settings-shell (max-width: 780px) {
  .settings-top-header {
    flex-direction: column;
    align-items: flex-start;
  }
  .setting-row-item {
    flex-direction: column;
    align-items: flex-start;
    gap: 1rem;
  }
  .setting-row-control,
  .setting-row-action {
    width: 100%;
  }
}
</style>
