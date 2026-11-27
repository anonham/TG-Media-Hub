<template>
  <!-- 2. 网盘树浏览器 -->
  <div class="card glass-card">
    <div class="card-header tree-header">
      <div class="card-title">🗂️ PikPak 网盘浏览</div>
      <div v-if="authStatus?.state === 'logged_in'" class="tree-toolbar">
        <n-tag size="small" type="info" round>已选 {{ pikpakStore.selectedFiles.size }} 个文件</n-tag>
        <n-button size="small" secondary :loading="currentLoading" :disabled="currentLoading" @click="refreshCurrent">
          <n-icon :size="14" class="toolbar-icon"><RefreshOutline /></n-icon>
          刷新
        </n-button>
      </div>
    </div>

    <!-- 红线: 服务端已禁用 search, 本 UI 不提供任何全局搜索入口 -->

    <!-- 非已登录空态 (三态分文案: not_bound=尚未绑定 / unreachable=暂时无法连接, 含传输层兜底) -->
    <div
      v-if="!pikpakStore.authStatusLoading && authStatus && authStatus.state !== 'logged_in'"
      class="unauth-empty-state"
    >
      <n-empty :description="emptyStateText" size="large" class="tree-empty">
        <template #icon>
          <n-icon :size="48" class="text-secondary"><CloudOfflineOutline /></n-icon>
        </template>
        <template #extra>
          <div class="unauth-actions">
            <span class="text-secondary">{{ emptyStateHint }}</span>
            <span v-if="authStatus.state_reason" class="unauth-reason text-secondary">
              {{ authStatus.state_reason }}
            </span>
            <div class="unauth-buttons">
              <n-button
                v-if="!envTakeover && authStatus.state === 'not_bound'"
                type="primary"
                class="bind-action-btn"
                @click="openDeviceModal"
              >
                立即绑定网盘账号
              </n-button>
              <n-button secondary class="reprobe-btn" :loading="pikpakStore.authStatusLoading" @click="retryAuth">
                重新检测
              </n-button>
            </div>
          </div>
        </template>
      </n-empty>
    </div>

    <!-- 已登录正常浏览态 -->
    <template v-else>
      <div class="breadcrumb-row">
        <template v-for="(crumb, idx) in breadcrumbStack" :key="crumb.id">
          <a
            href="#"
            role="button"
            class="crumb"
            :class="{ current: idx === breadcrumbStack.length - 1 }"
            :aria-current="idx === breadcrumbStack.length - 1 ? 'location' : undefined"
            @click.prevent="navigateTo(idx)"
            @keydown.space.prevent
            @keyup.space="navigateTo(idx)"
          >
            {{ crumb.name }}
          </a>
          <span v-if="idx < breadcrumbStack.length - 1" class="crumb-sep">/</span>
        </template>
      </div>

      <n-alert v-if="currentError" type="error" :show-icon="true" class="tree-error">
        目录加载失败: {{ currentError }}
      </n-alert>

      <n-spin :show="currentLoading">
        <n-empty
          v-if="!currentLoading && !currentError && currentItems.length === 0"
          description="此目录为空"
          class="tree-empty"
        />
        <div class="file-list">
          <div
            v-for="item in currentItems"
            :key="item.file_id"
            class="file-row"
            :class="{ 'is-dir': item.is_dir, 'audit-blocked': isAuditBlocked(item) }"
            :role="item.is_dir ? 'button' : undefined"
            :tabindex="item.is_dir ? 0 : undefined"
            @click="item.is_dir && enterFolder(item)"
            @keydown.enter="item.is_dir && enterFolder(item)"
            @keydown.space.prevent
            @keyup.space="item.is_dir && enterFolder(item)"
          >
            <span @click.stop>
              <n-checkbox
                size="small"
                :checked="item.is_dir ? pikpakStore.dirCheckState(item).checked : pikpakStore.isSelected(item.file_id)"
                :indeterminate="item.is_dir ? pikpakStore.dirCheckState(item).indeterminate : false"
                @update:checked="() => (item.is_dir ? pikpakStore.toggleDir(item) : pikpakStore.toggleFile(item))"
              />
            </span>
            <n-icon :size="18" class="row-icon" :class="{ 'dir-icon': item.is_dir }">
              <FolderOutline v-if="item.is_dir" />
              <DocumentTextOutline v-else />
            </n-icon>
            <img
              v-if="item.thumbnail_link && !thumbFailed[item.file_id]"
              :src="item.thumbnail_link"
              class="thumb"
              alt=""
              loading="lazy"
              decoding="async"
              width="46"
              height="30"
              @error="onThumbError(item)"
            />
            <span class="file-name" :title="auditTooltip(item)">{{ item.name }}</span>
            <span
              v-if="item.is_dir && pikpakStore.dirCheckState(item).loading"
              class="dir-walk-hint text-secondary font-mono"
            >
              正在展开… 已发现 {{ walkFoundCount(item) }} 个
            </span>
            <n-tag v-if="isAuditBlocked(item)" size="small" type="error" round>审核未通过</n-tag>
            <span class="file-meta font-mono">
              <template v-if="!item.is_dir">{{ formatBytes(item.size_bytes) }}</template>
              <span v-if="item.duration_s">{{ item.duration_s }}s</span>
              <span v-if="item.width && item.height">{{ item.width }}×{{ item.height }}</span>
            </span>
          </div>
        </div>
      </n-spin>

      <div v-if="currentEntry?.nextPageToken" class="load-more-row">
        <n-button size="small" secondary :loading="currentLoading" @click="pikpakStore.loadMore(currentFolderId)">
          加载更多
        </n-button>
      </div>
    </template>
  </div>

  <!-- 3. 已选清单 (Windows 风格三态树形) -->
  <div class="card glass-card">
    <div class="card-header tree-header">
      <div class="card-title">✅ 已选发布清单</div>
      <n-button
        v-if="pikpakStore.selectedFiles.size > 0"
        size="small"
        quaternary
        type="error"
        @click="pikpakStore.clearSelection()"
      >
        清空
      </n-button>
    </div>
    <n-empty
      v-if="pikpakStore.selectedFiles.size === 0"
      description="尚未选择文件 —— 在上方浏览网盘并勾选待发布的远端文件"
      class="tree-empty"
    />
    <div v-else class="selected-list">
      <!-- Windows 风格树形展示 (目录节点三态: 全选/半选/未选, 子树包含关系可视化) ——
           选择语义零改动: 目录勾选仅在全选态可整体取消 (级联移除规则), 叶子勾选走既有排除/恢复机制 -->
      <template
        v-for="(row, idx) in selectedTreeRows"
        :key="row.type === 'dir' ? `dir-${idx}-${row.node.dirId}` : `file-${idx}-${row.leaf.fid}`"
      >
        <div
          v-if="row.type === 'dir'"
          class="file-row selected-row group-file-row"
          :style="{ paddingLeft: 8 + row.depth * 18 + 'px' }"
        >
          <n-checkbox
            size="small"
            :checked="row.node.checkState === 'checked'"
            :indeterminate="row.node.checkState === 'indeterminate'"
            :disabled="row.node.loading"
            @update:checked="onDirCheckboxClick(row.node)"
          />
          <n-icon :size="16" class="row-icon dir-icon"><FolderOutline /></n-icon>
          <span class="file-name" :title="row.node.dirName">📁 {{ row.node.dirName }}</span>
          <span class="file-meta font-mono">{{ row.node.selectedCount }}/{{ row.node.allLeaves.length }}</span>
          <n-button size="tiny" quaternary type="error" @click="pikpakStore.removeDirRule(row.node.dirId)">
            移除
          </n-button>
        </div>
        <div v-else class="file-row selected-row group-file-row" :style="{ paddingLeft: 8 + row.depth * 18 + 'px' }">
          <n-checkbox
            size="small"
            :checked="row.leaf.selected"
            @update:checked="() => pikpakStore.toggleFile(row.leaf.item)"
          />
          <n-icon :size="16" class="row-icon"><DocumentTextOutline /></n-icon>
          <span class="file-name" :title="row.leaf.name">{{ row.leaf.name }}</span>
          <span class="file-meta font-mono">{{ formatBytes(row.leaf.item.size_bytes) }}</span>
        </div>
      </template>
      <!-- 单独勾选文件 (不属于任何目录规则) -->
      <div v-for="item in selectedSingles" :key="item.file_id" class="file-row selected-row">
        <n-icon :size="16" class="row-icon"><DocumentTextOutline /></n-icon>
        <span class="file-name" :title="item.name">{{ item.name }}</span>
        <span class="file-meta font-mono">{{ formatBytes(item.size_bytes) }}</span>
        <n-button size="tiny" quaternary type="error" @click="pikpakStore.toggleFile(item)"> 移除 </n-button>
      </div>
      <div class="selected-summary font-mono">
        共 {{ selectedFileCount }} 个文件 · 合计 {{ formatBytes(pikpakStore.selectedTotalBytes) }}
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
// PikPak 网盘文件浏览器: 远端目录树浏览 / 面包屑导航 / Windows 风格三态已选清单。
// 浏览缓存与选择规则全部由 pikpakStore 持有, 组件只承载交互与展示。
import { computed, onMounted, ref, watch } from 'vue'
import { NAlert, NButton, NCheckbox, NEmpty, NIcon, NSpin, NTag } from 'naive-ui'
import { CloudOfflineOutline, DocumentTextOutline, FolderOutline, RefreshOutline } from '@vicons/ionicons5'
import type { PikPakFileItem } from '@/api/gen'
import { usePikpakStore, type SelectionTreeNode, type DirSelectionRule } from '@/stores/pikpakStore'
import { message } from '@/utils/discreteApi'
import { formatBytes } from '@/utils/formatters'

const pikpakStore = usePikpakStore()

// 未绑定空态的绑定按钮 → 父视图转发至 AuthCard 打开设备码弹窗 (弹窗单例归 AuthCard)
const emit = defineEmits(['bind'])

const authStatus = computed(() => pikpakStore.authStatus)
const envTakeover = computed(() => authStatus.value?.token_source === 'env')
const emptyStateText = computed(() =>
  authStatus.value?.state === 'unreachable' ? '暂时无法连接 PikPak' : '尚未绑定 PikPak 网盘账号',
)
const emptyStateHint = computed(() =>
  authStatus.value?.state === 'unreachable'
    ? 'PikPak 暂时不可达，正在自动重试；也可手动重新检测。'
    : '请先完成 PikPak 账号授权绑定后，方可浏览网盘文件并提交发布任务。',
)

async function retryAuth() {
  await pikpakStore.fetchAuthStatus(true)
}

function openDeviceModal() {
  emit('bind')
}

// ── 树浏览器 ─────────────────────────────────────────────────
// audit 通过态常量 —— 已按真实 CLI 样本校准 (
// status='STATUS_OK', message='Normal resource', title='')。兼容保留罐头值 'OK'。
// 后端 AUDIT_PASS_STATUSES 的同源校准属 app\ 冻结范围, 待人工裁决后另行修正。
// 权威拦截判定以提交前 /inspect 的 audit_blocked_names 为准, 标红仅为数据源可信度提示。
const AUDIT_PASS_STATUSES_CALIBRATED = ['OK', 'STATUS_OK']

function isAuditBlocked(item: { audit?: { status?: string } } | null | undefined): boolean {
  const s = item?.audit?.status
  return Boolean(s) && !AUDIT_PASS_STATUSES_CALIBRATED.includes(String(s).toUpperCase())
}

function auditTooltip(item: { name?: string; audit?: { status?: string; message?: string; title?: string } }): string {
  if (!isAuditBlocked(item)) return item.name || ''
  const a = item.audit || {}
  return a.message || a.title || '审核状态未通过'
}

const thumbFailed = ref<Record<string, boolean>>({})

function onThumbError(item: { file_id: string }) {
  // onerror 兜底: 缩略图加载失败即隐藏, 不占位不重试
  thumbFailed.value = { ...thumbFailed.value, [item.file_id]: true }
}

const breadcrumbStack = ref([{ id: '', name: '根目录' }])
const currentFolderId = computed(() => breadcrumbStack.value[breadcrumbStack.value.length - 1]?.id || '')
const currentEntry = computed(() => pikpakStore.cacheEntry(currentFolderId.value))
// A full invalidation removes the active entry. Navigation and refresh install a
// loading entry synchronously, so the watcher reuses those requests.
watch(currentEntry, (entry, previous) => {
  if (!entry && previous && pikpakStore.authStatus?.logged_in) {
    void pikpakStore.fetchFolder(currentFolderId.value)
  }
})
const currentItems = computed(() => currentEntry.value?.items || [])
const currentLoading = computed(() => currentEntry.value?.loading || false)
const currentError = computed(() => currentEntry.value?.error || null)

async function enterFolder(item: PikPakFileItem) {
  if (!item?.is_dir) return
  breadcrumbStack.value = [...breadcrumbStack.value, { id: item.file_id, name: item.name || '' }]
  await pikpakStore.fetchFolder(item.file_id)
}

async function navigateTo(idx: number) {
  if (idx === breadcrumbStack.value.length - 1) return
  breadcrumbStack.value = breadcrumbStack.value.slice(0, idx + 1)
  await pikpakStore.fetchFolder(currentFolderId.value)
}

function preserveValidSelections(freshItems: PikPakFileItem[] = []) {
  const prunedCount = pikpakStore.pruneSelectionForFolder(currentFolderId.value, freshItems)
  if (prunedCount > 0) {
    message.info(`已自动移除 ${prunedCount} 个远端已不存在的勾选文件`)
  }
}

async function refreshCurrent() {
  if (currentLoading.value) return
  // 包2 (J6) + 包6 (K5): 刷新语义 = 全量失效 (其余目录将在进入时按 TTL 重新拉取),
  // 但保留当前目录旧条目作 loading 占位 —— 手动刷新瞬间列表不闪空, 新数据到达后
  // 原位更新。所选清单快照 (规则层持有 leaves) 与 phase3 §9-1 "所见即所发"契约
  // 不受影响, preserveValidSelections 清洗保留。
  pikpakStore.clearCache(currentFolderId.value)
  const res = await pikpakStore.fetchFolder(currentFolderId.value, { force: true })
  if (res) {
    const currentName = breadcrumbStack.value[breadcrumbStack.value.length - 1]?.name || '根目录'
    message.success(`已刷新 ${currentName}，其余目录将在进入时重新拉取`)
    preserveValidSelections(res.items || [])
  }
}

// ── 已选清单 (级联归因视图: 目录组 + 单独勾选) ────────────────
const selectedSingles = computed(() => pikpakStore.selectionView.singles)

interface SelectedTreeRowDir {
  type: 'dir'
  node: SelectionTreeNode
  depth: number
}
interface SelectedTreeRowFile {
  type: 'file'
  leaf: { fid: string; name: string; selected: boolean; item: PikPakFileItem }
  depth: number
}
type SelectedTreeRow = SelectedTreeRowDir | SelectedTreeRowFile

// 树形展示行 (扁平化带缩进深度; 目录行三态 + 叶子行勾选)
const selectedTreeRows = computed<SelectedTreeRow[]>(() => {
  const rows: SelectedTreeRow[] = []
  const walk = (node: SelectionTreeNode, depth: number) => {
    rows.push({ type: 'dir', node, depth })
    for (const leaf of node.directLeaves) rows.push({ type: 'file', leaf, depth: depth + 1 })
    for (const child of node.children) walk(child, depth + 1)
  }
  for (const root of pikpakStore.selectionTree) walk(root, 0)
  return rows
})

function onDirCheckboxClick(node: SelectionTreeNode) {
  if (node.checkState === 'checked') {
    // 全选态整体取消 = 移除该目录规则 (级联, 更深规则保留 —— 与目录行"移除"一致)
    pikpakStore.toggleDir({ file_id: node.dirId, name: node.dirName, is_dir: true })
  } else {
    message.info('子树按目录整体选择中；如需调整请先取消该目录后重新展开')
  }
}
const selectedFileCount = computed(() => pikpakStore.selectedFiles.size)

function walkFoundCount(item: PikPakFileItem) {
  const rule = pikpakStore.selectionRules.find((r) => r.kind === 'dir' && r.dirId === item.file_id) as
    | DirSelectionRule
    | undefined
  return rule?.foundCount || 0
}

// 根目录拉取: 挂载时已登录则直接取; 登录态翻转 (绑定成功) 时自动补拉
onMounted(() => {
  if (pikpakStore.authStatus?.logged_in && !pikpakStore.cacheEntry('')?.loaded) {
    pikpakStore.fetchFolder('')
  }
})

watch(
  () => pikpakStore.authStatus?.logged_in,
  (isLoggedIn, oldVal) => {
    if (isLoggedIn && !oldVal && !pikpakStore.cacheEntry('')?.loaded) {
      pikpakStore.fetchFolder('')
    }
  },
)
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

.tree-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.tree-toolbar {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.toolbar-icon {
  margin-right: 4px;
  vertical-align: -2px;
}

.text-secondary {
  color: var(--text-secondary);
}

/* 树浏览器 */
.breadcrumb-row {
  display: flex;
  align-items: center;
  gap: 0.35rem;
  margin-bottom: 0.9rem;
  flex-wrap: wrap;
}

.crumb {
  color: var(--text-secondary);
  cursor: pointer;
  font-size: 0.88rem;
  text-decoration: none;
}

.crumb:hover {
  color: var(--accent-color, #2563eb);
}

.crumb.current {
  color: var(--text-primary, #0f172a);
  font-weight: 600;
  cursor: default;
}

.crumb-sep {
  color: var(--text-secondary);
  font-size: 0.85rem;
}

.tree-error {
  margin-bottom: 0.9rem;
}

.tree-empty {
  padding: 1.5rem 0;
}

.file-list {
  display: flex;
  flex-direction: column;
  min-height: 60px;
}

.file-row {
  display: flex;
  align-items: center;
  gap: 0.7rem;
  padding: 0.55rem 0.6rem;
  border-radius: 8px;
  cursor: default;
  transition: background 0.15s ease;
}

.file-row:hover {
  background: var(--bg-card-hover, rgba(100, 116, 139, 0.08));
}

.file-row.is-dir {
  cursor: pointer;
}

.file-row.audit-blocked .file-name {
  color: #ef4444;
  font-weight: 500;
}

.row-icon {
  flex-shrink: 0;
  color: var(--text-secondary);
}

.row-icon.dir-icon {
  color: #f59e0b;
}

.thumb {
  width: 46px;
  height: 30px;
  object-fit: cover;
  border-radius: 4px;
  flex-shrink: 0;
  background: rgba(100, 116, 139, 0.15);
}

.file-name {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 0.9rem;
}

.file-meta {
  display: flex;
  align-items: center;
  gap: 0.7rem;
  font-size: 0.8rem;
  color: var(--text-secondary);
  flex-shrink: 0;
}

.load-more-row {
  display: flex;
  justify-content: center;
  padding-top: 0.9rem;
}

/* 已选清单 */
.selected-list {
  display: flex;
  flex-direction: column;
  gap: 0.15rem;
}

.group-file-row {
  padding-left: 1.6rem;
}

.selected-row {
  padding: 0.4rem 0.6rem;
}

.dir-walk-hint {
  font-size: 0.8rem;
}

.selected-summary {
  padding: 0.6rem;
  font-size: 0.85rem;
  color: var(--text-secondary);
  border-top: 1px dashed var(--border-color);
}

.unauth-empty-state {
  padding: 2.5rem 1rem;
  display: flex;
  justify-content: center;
}

.unauth-actions {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 0.75rem;
  margin-top: 0.5rem;
}

.unauth-reason {
  font-size: 0.85rem;
  max-width: 28rem;
  text-align: center;
}

.unauth-buttons {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.bind-action-btn {
  margin-top: 0.25rem;
}
</style>
