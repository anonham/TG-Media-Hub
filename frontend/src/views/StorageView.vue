<template>
  <div class="storage-view-container">
    <!-- 1. 存储总览与磁盘容量卡片 -->
    <div class="card glass-card">
      <div class="card-header-bar">
        <div class="title-block">
          <h3 class="section-title">存储占用总览与磁盘容量</h3>
          <p class="section-desc">下载目录: {{ storageStore.overview.download_dir || 'downloads' }}</p>
        </div>
        <div class="action-btn-group">
          <n-button secondary size="small" round @click="openQuotaModal">
            ⚙️ 存储配额:
            {{ storageStore.metrics.is_quota_active ? `${storageStore.metrics.max_storage_quota_gb} GB` : '未限制' }}
          </n-button>
          <n-button
            secondary
            type="warning"
            size="small"
            round
            :loading="storageStore.cleaningOrphans"
            @click="confirmCleanOrphans"
          >
            🧹 一键清理孤儿断点
          </n-button>
          <n-button
            type="primary"
            size="small"
            round
            :loading="storageStore.scanning"
            @click="storageStore.triggerScan()"
          >
            🔄 重新扫描 downloads/
          </n-button>
        </div>
      </div>

      <!-- 统计指标卡片网格 -->
      <div class="storage-stats-grid">
        <div class="stat-box">
          <span class="stat-num font-mono">{{ formatBytes(storageStore.overview.total_bytes || 0) }}</span>
          <span class="stat-text">下载目录占用</span>
        </div>
        <div class="stat-box" :class="{ 'is-quota-box': storageStore.metrics.is_quota_active }">
          <span class="stat-num font-mono">
            {{
              formatBytes(
                storageStore.metrics.effective_free_bytes ??
                  storageStore.overview.effective_free_bytes ??
                  storageStore.metrics.disk_free_bytes ??
                  storageStore.metrics.free_bytes ??
                  0,
              )
            }}
          </span>
          <span class="stat-text">
            有效可用空间
            <small v-if="storageStore.metrics.is_quota_active" class="quota-badge"> (受配额限制) </small>
            <small v-else class="quota-badge"> (物理磁盘) </small>
          </span>
        </div>
        <div class="stat-box">
          <span class="stat-num font-mono">{{ formatBytes(storageStore.overview.part_bytes || 0) }}</span>
          <span class="stat-text">断点碎片 (文件数: {{ storageStore.overview.file_count || 0 }})</span>
        </div>
        <div class="stat-box" :class="{ 'has-warning': storageStore.breakpoints.orphan_count > 0 }">
          <span class="stat-num font-mono">{{ storageStore.breakpoints.orphan_count || 0 }}</span>
          <span class="stat-text">孤儿断点待清理</span>
        </div>
      </div>

      <!-- 存储水位与配额进度条 -->
      <div
        v-if="
          storageStore.metrics.disk_total_bytes ||
          storageStore.metrics.total_bytes ||
          storageStore.overview.disk_total_bytes
        "
        class="disk-metrics-block"
      >
        <!-- 模式 A: 启用了存储配额 (展示下载配额使用率，并在副文本标明物理磁盘底座) -->
        <template v-if="storageStore.metrics.is_quota_active">
          <div class="disk-meta-row">
            <span class="quota-title"> <b>下载存储配额水位</b> ({{ storageStore.metrics.quota_used_percent }}%) </span>
            <span class="font-mono quota-info-text">
              已用 {{ formatBytes(storageStore.overview.total_bytes || storageStore.metrics.downloads_bytes || 0) }} /
              配额上限 {{ storageStore.metrics.max_storage_quota_gb }} GB (有效剩余可用
              {{ formatBytes(storageStore.metrics.effective_free_bytes || 0) }})
            </span>
          </div>
          <n-progress
            type="line"
            :percentage="storageStore.metrics.quota_used_percent || 0"
            :color="getProgressColor(storageStore.metrics.quota_used_percent || 0)"
            processing
          />
          <div class="disk-host-subline font-mono">
            <span>🖥️ 宿主机底层物理磁盘:</span>
            <span>
              物理已用 {{ formatBytes(storageStore.metrics.disk_used_bytes || storageStore.metrics.used_bytes || 0) }} /
              物理总量
              {{ formatBytes(storageStore.metrics.disk_total_bytes || storageStore.metrics.total_bytes || 0) }}
              (物理剩余 {{ formatBytes(storageStore.metrics.disk_free_bytes || storageStore.metrics.free_bytes || 0) }},
              总使用率 {{ storageStore.metrics.disk_used_percent || storageStore.metrics.percent_used || 0 }}%)
            </span>
          </div>
        </template>

        <!-- 模式 B: 未设配额 (展示宿主机物理磁盘使用率，提示未受限) -->
        <template v-else>
          <div class="disk-meta-row">
            <span>
              <b>宿主机磁盘使用率</b> ({{
                storageStore.metrics.disk_used_percent || storageStore.metrics.percent_used || 0
              }}%)
              <small class="text-secondary ml-1">— 当前未设存储配额上限，直接受物理磁盘可用空间约束</small>
            </span>
            <span class="font-mono">
              已用 {{ formatBytes(storageStore.metrics.disk_used_bytes || storageStore.metrics.used_bytes || 0) }} /
              总共
              {{ formatBytes(storageStore.metrics.disk_total_bytes || storageStore.metrics.total_bytes || 0) }}
              (剩余可用
              {{
                formatBytes(
                  storageStore.metrics.effective_free_bytes ||
                    storageStore.metrics.disk_free_bytes ||
                    storageStore.metrics.free_bytes ||
                    0,
                )
              }})
            </span>
          </div>
          <n-progress
            type="line"
            :percentage="storageStore.metrics.disk_used_percent || storageStore.metrics.percent_used || 0"
            :color="getProgressColor(storageStore.metrics.disk_used_percent || storageStore.metrics.percent_used || 0)"
            processing
          />
        </template>
      </div>
    </div>

    <!-- 2. 媒体资源文件管理器 -->
    <div class="card glass-card">
      <div class="files-toolbar-header">
        <div class="filter-tabs">
          <n-tabs
            v-model:value="storageStore.filesCategory"
            type="line"
            size="small"
            @update:value="handleCategoryChange"
          >
            <n-tab name="">全部文件</n-tab>
            <n-tab name="video">🎬 视频</n-tab>
            <n-tab name="image">🖼️ 图片</n-tab>
            <n-tab name="audio">🎵 音频</n-tab>
            <n-tab name="document">📄 文档</n-tab>
            <n-tab name="parts">🧩 断点碎片</n-tab>
            <n-tab name="final">✅ 已完成成品</n-tab>
          </n-tabs>
        </div>

        <div class="toolbar-search-actions">
          <n-input
            v-model:value="storageStore.filesQuery"
            placeholder="搜索文件名 / 路径..."
            size="small"
            clearable
            class="files-search-input"
            @keydown.enter="(e: KeyboardEvent) => onImeSafeEnter(e, () => storageStore.loadFiles(true))"
            @clear="storageStore.loadFiles(true)"
          />
          <n-button size="small" secondary @click="storageStore.loadFiles(true)"> 🔍 查询 </n-button>
          <n-button v-if="selectedFilePaths.length > 0" size="small" type="error" secondary @click="confirmBatchDelete">
            批量删除 ({{ selectedFilePaths.length }})
          </n-button>
        </div>
      </div>

      <!-- 文件列表表格 (响应式区域容器与无障碍声明) -->
      <div class="table-responsive-wrapper" tabindex="0" role="region" aria-label="下载存储媒体资源列表">
        <div v-if="storageStore.loadingFiles" class="empty-placeholder">正在加载文件列表...</div>
        <div v-else-if="storageStore.files.length === 0" class="empty-placeholder">暂无匹配的媒体文件</div>
        <table v-else class="cockpit-table">
          <thead>
            <tr>
              <th scope="col" style="width: 40px">
                <n-checkbox
                  :checked="isAllSelected"
                  :indeterminate="isIndeterminate"
                  @update:checked="toggleSelectAll"
                />
              </th>
              <th scope="col">文件名 / 相对路径</th>
              <th scope="col">分类</th>
              <th scope="col">文件大小</th>
              <th scope="col">修改时间</th>
              <th scope="col">关联任务</th>
              <th scope="col" style="text-align: right">操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="file in storageStore.files" :key="getFilePath(file)">
              <td>
                <n-checkbox
                  :checked="selectedFilePaths.includes(getFilePath(file))"
                  @update:checked="(chk) => toggleSelectFile(getFilePath(file), chk)"
                />
              </td>
              <td class="file-name-cell">
                <span class="file-icon">{{ getFileIcon(file) }}</span>
                <span class="file-path-text" :title="getFilePath(file)">{{ getFilePath(file) }}</span>
              </td>
              <td>
                <n-tag size="small" :type="getCategoryTagType(file.category)" round>
                  {{ file.category || 'other' }}
                </n-tag>
              </td>
              <td class="font-mono">{{ formatBytes(getFileSize(file)) }}</td>
              <td class="font-mono text-secondary">{{ formatDateTime(file.mtime) }}</td>
              <td class="font-mono">
                <router-link
                  v-if="file.task_id"
                  :to="`/tasks?id=${file.task_id}`"
                  class="task-link-badge"
                  title="点击直达任务中心查看此任务详情"
                >
                  #{{ file.task_id.slice(0, 8) }}
                </router-link>
                <span v-else class="text-secondary">-</span>
              </td>
              <td class="action-cell">
                <n-button v-if="isPreviewable(file)" size="tiny" type="primary" secondary @click="openPreview(file)">
                  📺 预览
                </n-button>
                <n-button
                  size="tiny"
                  secondary
                  type="info"
                  :loading="downloadingPaths.has(getFilePath(file))"
                  @click="handleDownload(file)"
                >
                  📥 下载
                </n-button>
                <n-button size="tiny" secondary type="error" @click="confirmDeleteSingle(file)"> 🗑️ </n-button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- 分页栏 -->
      <div v-if="storageStore.filesTotal > storageStore.filesLimit" class="table-pagination-bar">
        <n-pagination
          v-model:page="storageStore.filesPage"
          :page-size="storageStore.filesLimit"
          :item-count="storageStore.filesTotal"
          show-quick-jumper
          @update:page="storageStore.loadFiles()"
        />
      </div>
    </div>

    <!-- 3. 媒体在线流式播放 / 预览弹窗 (HTTP 206 Partial Content) -->
    <n-modal
      v-model:show="showPreviewModal"
      preset="card"
      :title="`📺 在线媒体预览: ${getFilePath(currentPreviewFile)}`"
      style="width: 90%; max-width: 800px"
      @after-leave="closePreview"
    >
      <div v-if="currentPreviewFile && previewUrl" class="preview-content-box">
        <!-- 视频播放器 (src 为取票后构造的短时效 ticket URL) -->
        <video
          v-if="currentPreviewFile.category === 'video' || isVideo(getFilePath(currentPreviewFile))"
          :src="previewUrl"
          controls
          :autoplay="shouldAutoplay"
          class="preview-video-player"
        ></video>
        <div
          v-if="/\.(ts|m2ts|mts)$/i.test(getFilePath(currentPreviewFile))"
          class="text-secondary"
          style="font-size: 0.8rem; margin-top: 4px; text-align: center"
        >
          提示：若当前浏览器未内置 MPEG-TS 解码器无法正常播放，可直接下载后使用本地播放器查看。
        </div>

        <!-- 图片查看器 -->
        <img
          v-else-if="currentPreviewFile.category === 'image' || isImage(getFilePath(currentPreviewFile))"
          :src="previewUrl"
          class="preview-image"
          alt="Preview"
          loading="lazy"
          decoding="async"
        />

        <!-- 音频播放器 -->
        <audio
          v-else-if="currentPreviewFile.category === 'audio' || isAudio(getFilePath(currentPreviewFile))"
          :src="previewUrl"
          controls
          :autoplay="shouldAutoplay"
          class="preview-audio-player"
        ></audio>

        <div class="preview-meta-info font-mono">
          <span>文件大小: {{ formatBytes(getFileSize(currentPreviewFile)) }}</span>
          <span>HTTP 206 流式分块传输就绪</span>
        </div>
      </div>
    </n-modal>

    <!-- 3. 存储配额设置快捷弹窗 -->
    <n-modal
      v-model:show="showQuotaModal"
      preset="card"
      title="⚙️ 设置下载存储上限配额"
      style="width: 90%; max-width: 500px"
    >
      <div class="quota-modal-content">
        <p class="quota-tip-text">
          您可以为
          <code>downloads/</code>
          目录指定一个最大存储配额。系统在计算剩余可用空间时，<strong>严格取配额剩余量与宿主机物理磁盘可用量的最小值</strong>，绝不超过物理可用空间。
        </p>

        <div class="quota-physical-info font-mono">
          <span>宿主机物理磁盘实际可用空间：</span>
          <b class="text-primary">{{
            formatBytes(
              storageStore.metrics.disk_free_bytes ||
                storageStore.metrics.free_bytes ||
                storageStore.overview.disk_free_bytes ||
                0,
            )
          }}</b>
        </div>

        <n-form label-placement="top" size="small" style="margin-top: 1.25rem">
          <n-form-item label="存储配额上限 (GB)">
            <n-input-number
              v-model:value="quotaGbInput"
              :min="0"
              :max="1000000"
              :precision="1"
              style="width: 100%"
              placeholder="输入存储配额 (GB)，填 0 为不限制"
            >
              <template #suffix>GB</template>
            </n-input-number>
            <template #feedback>
              <span class="text-muted">提示：填入 0 或清空表示不设上限，直接按宿主机底层物理磁盘可用空间计算。</span>
            </template>
          </n-form-item>
        </n-form>
      </div>
      <template #footer>
        <div style="display: flex; justify-content: flex-end; gap: 0.75rem">
          <n-button @click="showQuotaModal = false">取消</n-button>
          <n-button type="primary" :loading="storageStore.savingQuota" @click="handleSaveQuota"> 保存配额 </n-button>
        </div>
      </template>
    </n-modal>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch, h } from 'vue'
import {
  NButton,
  NTabs,
  NTab,
  NTag,
  NInput,
  NInputNumber,
  NForm,
  NFormItem,
  NCheckbox,
  NPagination,
  NProgress,
  NModal,
} from 'naive-ui'

import { useStorageStore } from '@/stores/storageStore'
import { useThemeStore } from '@/stores/themeStore'
import { getStoragePreviewUrl, getStorageDownloadUrl } from '@/api/client'
import { dialog } from '@/utils/discreteApi'
import { formatBytes, formatDateTime } from '@/utils/formatters'
import { onImeSafeEnter } from '@/utils/ime'
import type { FileItemResponse } from '@/api/gen/types.gen'

const storageStore = useStorageStore()
const themeStore = useThemeStore()

const shouldAutoplay = computed(() => {
  if (themeStore.reduceMotion) return false
  if (
    typeof window !== 'undefined' &&
    window.matchMedia &&
    window.matchMedia('(prefers-reduced-motion: reduce)').matches
  ) {
    return false
  }
  return true
})

const selectedFilePaths = ref<string[]>([])

const showPreviewModal = ref(false)
const currentPreviewFile = ref<FileItemResponse | null>(null)
// F9: 媒体 URL 异步构造 —— 预览/下载先取短时效 ticket 再拼 URL,
// 明文 token/two_fa_token 已从一切 URL 收口
const previewUrl = ref('')
const downloadingPaths = ref<Set<string>>(new Set())

const showQuotaModal = ref(false)
const quotaGbInput = ref<number | null>(0)

function openQuotaModal() {
  quotaGbInput.value = storageStore.metrics.max_storage_quota_gb || 0
  showQuotaModal.value = true
}

async function handleSaveQuota() {
  try {
    await storageStore.updateQuota(Number(quotaGbInput.value) || 0)
    showQuotaModal.value = false
  } catch {
    // 失败已由 store 层 toast; 保留弹窗供修正后重试 (修复 unhandled rejection)
  }
}

function getProgressColor(percent: number) {
  if (percent >= 90) return '#ef4444'
  if (percent >= 80) return '#f59e0b'
  return '#3b82f6'
}

function getFilePath(file: FileItemResponse | null | undefined): string {
  if (!file) return ''
  return file.path || file.rel_path || file.file_name || ''
}

function getFileSize(file: FileItemResponse | null | undefined): number {
  if (!file) return 0
  return file.size ?? file.size_bytes ?? 0
}

const isAllSelected = computed(() => {
  return (
    storageStore.files.length > 0 && storageStore.files.every((f) => selectedFilePaths.value.includes(getFilePath(f)))
  )
})

const isIndeterminate = computed(() => {
  if (storageStore.files.length === 0) return false
  const selectedOnPage = storageStore.files.filter((f) => selectedFilePaths.value.includes(getFilePath(f))).length
  return selectedOnPage > 0 && selectedOnPage < storageStore.files.length
})

// 跨页保留选中集 —— 全选/取消全选仅合并或移除当前页路径
function toggleSelectAll(checked: boolean) {
  const currentPagePaths = storageStore.files.map((f) => getFilePath(f)).filter(Boolean)
  if (checked) {
    selectedFilePaths.value = Array.from(new Set([...selectedFilePaths.value, ...currentPagePaths]))
  } else {
    const currentPageSet = new Set(currentPagePaths)
    selectedFilePaths.value = selectedFilePaths.value.filter((p) => !currentPageSet.has(p))
  }
}

function toggleSelectFile(path: string, checked: boolean) {
  if (checked) {
    if (!selectedFilePaths.value.includes(path)) selectedFilePaths.value.push(path)
  } else {
    selectedFilePaths.value = selectedFilePaths.value.filter((p) => p !== path)
  }
}

function handleCategoryChange() {
  selectedFilePaths.value = []
  storageStore.loadFiles(true)
}

function getFileIcon(file: FileItemResponse | null | undefined) {
  const cat = file?.category
  if (cat === 'video') return '🎬'
  if (cat === 'image') return '🖼️'
  if (cat === 'audio') return '🎵'
  if (cat === 'document') return '📄'
  if (cat === 'parts' || cat === 'part' || cat === 'meta') return '🧩'
  return '📁'
}

function getCategoryTagType(cat: string): 'primary' | 'success' | 'info' | 'warning' | 'default' {
  if (cat === 'video') return 'primary'
  if (cat === 'image') return 'success'
  if (cat === 'audio') return 'info'
  if (cat === 'parts' || cat === 'part' || cat === 'meta') return 'warning'
  return 'default'
}

function isVideo(path = '') {
  return !/\.d\.ts$/i.test(path) && /\.(mp4|m4v|mkv|webm|mov|avi|flv|ts|m2ts|mts)$/i.test(path)
}

function isImage(path = '') {
  return /\.(jpg|jpeg|png|gif|webp|svg)$/i.test(path)
}

function isAudio(path = '') {
  return /\.(mp3|flac|ogg|m4a|wav|aac)$/i.test(path)
}

function isPreviewable(file: FileItemResponse | null | undefined) {
  const cat = file?.category
  const path = getFilePath(file)
  return cat === 'video' || cat === 'image' || cat === 'audio' || isVideo(path) || isImage(path) || isAudio(path)
}

let previewRequestGen = 0

watch(showPreviewModal, (open) => {
  if (!open) {
    previewRequestGen += 1
  }
})

async function openPreview(file: FileItemResponse) {
  const gen = ++previewRequestGen
  currentPreviewFile.value = file
  showPreviewModal.value = true
  previewUrl.value = ''
  try {
    const url = await getStoragePreviewUrl(getFilePath(file))
    if (gen !== previewRequestGen || !showPreviewModal.value) return
    previewUrl.value = url
  } catch {
    if (gen !== previewRequestGen) return
    // 取票失败已 toast (硬规则 5), 中止预览
    showPreviewModal.value = false
    currentPreviewFile.value = null
  }
}

function closePreview() {
  previewRequestGen += 1
  currentPreviewFile.value = null
  previewUrl.value = ''
}

async function handleDownload(file: FileItemResponse) {
  const path = getFilePath(file)
  if (!path || downloadingPaths.value.has(path)) return
  downloadingPaths.value = new Set([...downloadingPaths.value, path])
  // 在用户点击的同步阶段创建受控空白页并隔离 opener，防止异步取票后失去 transient activation 被拦截
  const popup = typeof window !== 'undefined' ? window.open('', '_blank') : null
  if (popup) {
    popup.opener = null
  }
  try {
    const url = await getStorageDownloadUrl(path)
    if (popup && !popup.closed) {
      popup.location.href = url
    } else {
      dialog.info({
        title: '📥 下载链接已就绪',
        content: () =>
          h('div', { style: 'display:flex;flex-direction:column;gap:8px' }, [
            h('span', `文件 "${path}" 的临时下载链接已生成（短期有效）：`),
            h(
              'a',
              {
                href: url,
                target: '_blank',
                rel: 'noopener noreferrer',
                style: 'color: var(--accent-color); text-decoration: underline; word-break: break-all;',
              },
              '点击立即下载文件',
            ),
          ]),
        positiveText: '关闭',
      })
    }
  } catch {
    if (popup && !popup.closed) {
      popup.close()
    }
    // 取票失败已 toast (硬规则 5), 中止打开
  } finally {
    const next = new Set(downloadingPaths.value)
    next.delete(path)
    downloadingPaths.value = next
  }
}

function confirmDeleteSingle(file: FileItemResponse) {
  const path = getFilePath(file)
  if (!path) return
  dialog.warning({
    title: '确认删除文件',
    content: `确定要删除文件 "${path}" 吗？此操作不可恢复。`,
    positiveText: '删除',
    negativeText: '取消',
    onPositiveClick: async () => {
      await storageStore.deleteFiles([path])
    },
  })
}

// 批量删除确认弹窗列出完整选中清单 (含跨页不可见项)
function confirmBatchDelete() {
  const paths = [...selectedFilePaths.value]
  if (paths.length === 0) return
  const currentPageSet = new Set(storageStore.files.map((f) => getFilePath(f)))
  const offPageCount = paths.filter((p) => !currentPageSet.has(p)).length

  dialog.warning({
    title: '确认批量删除',
    content: () =>
      h('div', { style: 'display: flex; flex-direction: column; gap: 0.6rem;' }, [
        h(
          'div',
          { style: 'font-size: 0.88rem; color: var(--text-primary);' },
          offPageCount > 0
            ? `确定要永久删除已选中的 ${paths.length} 个文件吗？（其中 ${offPageCount} 个文件位于其他分页或筛选视图之外）`
            : `确定要永久删除已选中的 ${paths.length} 个文件吗？此操作不可恢复：`,
        ),
        h(
          'ul',
          {
            class: 'font-mono',
            style:
              'margin: 0; padding: 0.5rem 0.75rem 0.5rem 1.5rem; max-height: 220px; overflow-y: auto; background: var(--bg-card-hover); border: 1px solid var(--border-color); border-radius: 8px; font-size: 0.78rem; color: var(--text-secondary);',
          },
          paths.map((p) =>
            h(
              'li',
              {
                key: p,
                style: 'word-break: break-all; padding: 2px 0;',
              },
              currentPageSet.has(p) ? p : `${p} (跨页已选)`,
            ),
          ),
        ),
      ]),
    positiveText: '确认删除',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        await storageStore.deleteFiles(paths)
        selectedFilePaths.value = []
      } catch {
        // 失败已由 store 层 toast; 保留选中集供重试 (修复 unhandled rejection)
      }
    },
  })
}

function confirmCleanOrphans() {
  dialog.warning({
    title: '确认清理孤儿断点',
    content: `确定要扫描并清理全部无活跃任务关联的孤儿 .part / .meta 碎片吗？`,
    positiveText: '一键清理',
    negativeText: '取消',
    onPositiveClick: async () => {
      await storageStore.cleanOrphans()
    },
  })
}

onMounted(() => {
  storageStore.loadOverview()
  storageStore.loadMetrics()
  storageStore.loadBreakpoints()
  storageStore.loadFiles()
})
</script>

<style scoped>
.storage-view-container {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
  width: 100%;
  container: storage-root / inline-size;
}

.card {
  padding: 1.5rem;
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
}

.card-header-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid var(--border-color);
  padding-bottom: 1rem;
  margin-bottom: 1.25rem;
  gap: 1rem;
}

.section-desc {
  color: var(--text-secondary);
  font-size: 0.85rem;
  margin: 0.25rem 0 0;
}

.action-btn-group {
  display: flex;
  align-items: center;
  gap: 0.6rem;
}

/* 统计卡片网格 */
.storage-stats-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 1rem;
  margin-bottom: 1.25rem;
}

.stat-box {
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-radius: 12px;
  padding: 1rem;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 0.3rem;
  box-shadow: var(--card-shadow);
}

.stat-box.has-warning {
  border-color: rgba(245, 158, 11, 0.4);
  background: rgba(245, 158, 11, 0.08);
}

.stat-num {
  font-size: 1.25rem;
  font-weight: 700;
  color: var(--text-primary);
}

.stat-text {
  font-size: 0.78rem;
  color: var(--text-secondary);
  display: flex;
  align-items: center;
  justify-content: center;
  flex-wrap: wrap;
  text-align: center;
  gap: 0.25rem 0.35rem;
}

.stat-box.is-quota-box {
  border-color: rgba(59, 130, 246, 0.35);
  background: rgba(59, 130, 246, 0.05);
}

.quota-badge {
  color: var(--accent-color, #3b82f6);
  font-size: 0.72rem;
  font-weight: 600;
}

.disk-metrics-block {
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-radius: 12px;
  padding: 1.1rem;
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.disk-meta-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 0.84rem;
  color: var(--text-secondary);
  flex-wrap: wrap;
  gap: 0.5rem;
}

.quota-title {
  color: var(--text-primary);
}

.disk-host-subline {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.78rem;
  color: var(--text-secondary);
  opacity: 0.85;
  border-top: 1px dashed var(--border-color);
  padding-top: 0.5rem;
  margin-top: 0.2rem;
  flex-wrap: wrap;
}

.quota-modal-content {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.quota-tip-text {
  font-size: 0.85rem;
  line-height: 1.5;
  color: var(--text-secondary);
  margin: 0;
}

.quota-physical-info {
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 0.6rem 0.85rem;
  font-size: 0.82rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

/* 媒体文件管理器 */
.files-toolbar-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid var(--border-color);
  padding-bottom: 0.75rem;
  margin-bottom: 1rem;
  gap: 1rem;
  min-width: 0;
}

.filter-tabs {
  min-width: 0;
  max-width: 100%;
}

.toolbar-search-actions {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.5rem;
}

.files-search-input {
  width: 220px;
}

.files-table-wrapper {
  overflow-x: auto;
}

.file-name-cell {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  max-width: 320px;
}

.file-icon {
  font-size: 1.1rem;
  flex-shrink: 0;
}

.file-path-text {
  white-space: pre;
  overflow: hidden;
  text-overflow: ellipsis;
  color: var(--text-primary);
}

.text-secondary {
  color: var(--text-secondary);
}

.action-cell {
  text-align: right;
  display: flex;
  justify-content: flex-end;
  gap: 0.4rem;
}

.table-pagination-bar {
  display: flex;
  justify-content: flex-end;
  margin-top: 1.25rem;
}

.empty-placeholder {
  padding: 3rem 0;
  text-align: center;
  color: var(--text-secondary);
}

/* 预览弹窗 */
.preview-content-box {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 1rem;
}

.preview-video-player {
  width: 100%;
  max-height: 480px;
  background: #000;
  border-radius: 0.5rem;
}

.preview-image {
  max-width: 100%;
  max-height: 500px;
  border-radius: 0.5rem;
  object-fit: contain;
}

.preview-audio-player {
  width: 100%;
}

.preview-meta-info {
  display: flex;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 0.4rem;
  width: 100%;
  font-size: 0.8rem;
  color: var(--text-secondary);
}

@container storage-root (max-width: 820px) {
  .storage-stats-grid {
    grid-template-columns: repeat(2, 1fr);
  }
  .files-toolbar-header {
    flex-direction: column;
    align-items: stretch;
  }
}

@container storage-root (max-width: 720px) {
  .files-search-input {
    flex: 1 1 180px;
    width: auto;
  }
  .table-pagination-bar {
    justify-content: center;
  }
  .table-pagination-bar :deep(.n-pagination) {
    flex-wrap: wrap;
    justify-content: center;
    gap: 0.4rem;
  }
}

@container storage-root (max-width: 350px) {
  .storage-stats-grid {
    grid-template-columns: 1fr;
  }
}
</style>
