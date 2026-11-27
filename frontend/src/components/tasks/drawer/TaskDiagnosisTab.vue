<template>
  <div class="tab-pane-content">
    <!-- 1. 云端转发专属诊断 -->
    <template v-if="task.task_type === 'forward_channel'">
      <div class="card glass-card section-box">
        <div class="box-header">
          <span class="box-title">覆盖区间条 (CoverageBar)</span>
        </div>
        <div class="forward-info-subline font-mono">
          <span
            >源频道: <b>{{ task.params?.source_chat || '-' }}</b></span
          >
          <span
            >目标: <b>{{ task.params?.target_chat || '-' }}</b></span
          >
        </div>
        <CoverageBar
          v-if="forwardCoverageData"
          :range-start="forwardCoverageData.min_id"
          :range-end="forwardCoverageData.max_id"
          :coverage="forwardCoverageData.covered"
          :forwarded-runs="forwardCoverageData.forwarded_runs"
          :filtered-runs="forwardCoverageData.filtered_runs"
          :bad-runs="forwardCoverageData.bad_runs"
          :loading="loadingCoverage"
        />
        <div v-else class="empty-diag-tip">
          {{ loadingCoverage ? '正在分析覆盖区间...' : '暂无覆盖区间数据或未测算' }}
        </div>
      </div>

      <!-- 转发回执明细 -->
      <div v-if="forwardReceipt" class="card glass-card section-box">
        <div class="box-header">
          <span class="box-title">转发结算清单</span>
        </div>
        <div class="receipt-stats-grid font-mono">
          <div class="r-item success">
            <div class="r-num">{{ forwardReceipt.forwarded || 0 }}</div>
            <div class="r-label">成功转发</div>
          </div>
          <div class="r-item warning">
            <div class="r-num">{{ forwardReceipt.filtered || 0 }}</div>
            <div class="r-label">策略清洗</div>
          </div>
          <div class="r-item error">
            <div class="r-num">{{ forwardReceipt.bad || 0 }}</div>
            <div class="r-label">坏消息隔离</div>
          </div>
          <div class="r-item info">
            <div class="r-num">{{ forwardReceipt.dedup_skipped ?? '-' }}</div>
            <div class="r-label">防重跳过</div>
          </div>
        </div>

        <div v-if="forwardReceipt.bad_ids?.length" class="bad-ids-block">
          <span class="block-sub-title">坏消息 ID 隔离列表:</span>
          <div class="bad-tags-wrap">
            <n-tag v-for="bId in forwardReceipt.bad_ids" :key="bId" size="small" type="error" round> #{{ bId }} </n-tag>
          </div>
        </div>
      </div>
    </template>

    <!-- 2. 受限下载专属诊断 -->
    <template v-else-if="task.task_type === 'download_link' || task.task_type === 'download_range'">
      <div class="card glass-card section-box">
        <div class="box-header">
          <span class="box-title">断点与落盘文件反查 (.part / .meta)</span>
          <n-button size="tiny" secondary type="primary" :loading="loadingRelatedFiles" @click="loadRelatedFiles">
            🔄 刷新落盘
          </n-button>
        </div>
        <div v-if="relatedFiles.length === 0" class="empty-diag-tip">
          {{ loadingRelatedFiles ? '正在扫描落盘文件...' : '当前任务暂无登记落盘文件或断点已合并完成' }}
        </div>
        <div v-else class="files-mini-list">
          <div v-for="f in relatedFiles" :key="f.rel_path || f.path || f.file_name" class="file-mini-row font-mono">
            <span class="file-kind-badge" :class="f.category">{{ f.category }}</span>
            <span class="file-path-span" :title="f.rel_path || f.path || ''">{{ f.rel_path || f.path }}</span>
            <span class="file-size-span">{{ formatBytes(f.size || f.size_bytes || 0) }}</span>
          </div>
        </div>
      </div>
    </template>

    <!-- 3. 发布类任务统一诊断 (包4: 本地 publish_folder + pikpak_publish 共用三表) -->
    <template v-else-if="isPublishTask">
      <!-- 0. PikPak 专属: 上游信道健康度与风控自愈控制台 (Milestone 4) -->
      <div v-if="task.task_type === 'pikpak_publish'" class="card glass-card section-box pikpak-risk-box">
        <div class="box-header">
          <div class="header-with-badge">
            <span class="box-title">🛡️ PikPak 上游信道健康度与风控自愈</span>
            <n-tag :type="channelHealth.badgeType" size="small" round>
              {{ channelHealth.badgeText }}
            </n-tag>
          </div>
          <div class="header-health-meta font-mono">
            <span class="health-meta-label">状态:</span>
            <span class="health-meta-val" :class="channelHealth.dotClass">{{ channelHealth.label }}</span>
          </div>
        </div>

        <div v-if="channelHealth.description" class="risk-description-banner" :class="channelHealth.status">
          <span class="desc-icon">{{ channelHealth.icon }}</span>
          <span class="desc-text">{{ channelHealth.description }}</span>
        </div>

        <!-- 四格风控指标 -->
        <div class="risk-stats-grid font-mono">
          <div
            class="r-item"
            :class="{ warning: channelHealth.throttleStrikes > 0, error: channelHealth.throttleStrikes >= 3 }"
          >
            <div class="r-num">{{ channelHealth.throttleStrikes }}/3</div>
            <div class="r-label">频控拦截 (Throttle)</div>
          </div>
          <div
            class="r-item"
            :class="{ warning: channelHealth.dataRiskStrikes > 0, error: channelHealth.dataRiskStrikes >= 2 }"
          >
            <div class="r-num">{{ channelHealth.dataRiskStrikes }}/2</div>
            <div class="r-label">数据质询 (Data Risk)</div>
          </div>
          <div class="r-item" :class="{ info: channelHealth.isAutoDownshifted }">
            <div class="r-num">{{ channelHealth.isAutoDownshifted ? '稳态降档' : '标准并发' }}</div>
            <div class="r-label">传输通道档位</div>
          </div>
          <div class="r-item success">
            <div class="r-num">{{ channelHealth.deliveredPartsCount }}</div>
            <div class="r-label">保全分卷检查点</div>
          </div>
        </div>

        <!-- 人机交互控制入口 (Human-in-the-Loop) -->
        <div class="risk-actions-bar">
          <div class="actions-left">
            <span class="actions-hint font-mono">人机协同恢复控制:</span>
          </div>
          <div class="actions-right">
            <n-button
              v-if="task.status === 'running' || channelHealth.isCooldown"
              type="warning"
              size="small"
              secondary
              :loading="isActionRunning"
              @click="handlePause"
            >
              ⏸️ 暂停任务
            </n-button>
            <n-button type="primary" size="small" secondary :loading="isActionRunning" @click="handleSteadyResume">
              🛡️ 低速稳态恢复 (推荐)
            </n-button>
            <n-button size="small" secondary :loading="isActionRunning" @click="handleNormalResume">
              ▶️ 原速恢复
            </n-button>
            <span :title="task.status === 'running' ? '当前任务正在运行中，请先暂停后再跳过文件' : undefined">
              <n-button
                type="warning"
                size="small"
                secondary
                :disabled="task.status === 'running'"
                :loading="isActionRunning"
                @click="handlePromptSkipCurrent"
              >
                ⏭️ 跳过当前文件
              </n-button>
            </span>
            <n-button
              v-if="channelHealth.isAuthRequired"
              type="error"
              size="small"
              secondary
              @click="handleNavigateAccounts"
            >
              🔑 重新授权
            </n-button>
          </div>
        </div>
      </div>

      <div class="card glass-card section-box">
        <div class="box-header">
          <span class="box-title">⏭️ 超限跳过文件 ({{ skippedFilesCount }})</span>
        </div>
        <div v-if="skippedFilesCount > 0" class="table-responsive-wrapper">
          <n-table size="small" :bordered="false" :single-line="false">
            <thead>
              <tr>
                <th>文件名</th>
                <th>文件体积</th>
                <th>单文件限制</th>
                <th>超限差值</th>
                <th>处理策略</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="sf in skippedFiles" :key="sf.file_id || sf.file_name">
                <td class="font-mono" style="word-break: break-all">{{ sf.file_name }}</td>
                <td class="font-mono">
                  {{ (sf.file_size_mb || (sf.file_size_bytes ? sf.file_size_bytes / (1024 * 1024) : 0)).toFixed(1) }}
                  MiB
                </td>
                <td class="font-mono">
                  {{
                    sf.limit_mb
                      ? `${sf.limit_mb.toFixed(0)} MiB (${(sf.limit_mb / 1024).toFixed(2)} GiB)`
                      : STANDARD_LIMIT_LABEL
                  }}
                </td>
                <td class="font-mono" style="color: #f0a020">
                  +{{ (sf.exceeded_mb ?? sf.oversize_delta_mb ?? 0).toFixed(1) }} MiB
                </td>
                <td>
                  <n-tag size="tiny" type="warning" round>
                    {{
                      sf.oversize_kind === 'premium_limit' || sf.oversize_kind === 'protocol_hard_cap'
                        ? `平台最高限制 ${PREMIUM_LIMIT_LABEL}`
                        : sf.oversize_kind === 'non_premium_limit'
                          ? `普通限制 ${STANDARD_LIMIT_LABEL}`
                          : sf.oversize_type || '体积超限'
                    }}
                  </n-tag>
                </td>
              </tr>
            </tbody>
          </n-table>
        </div>
        <div v-else class="empty-diag-tip">无</div>
      </div>

      <div class="card glass-card section-box">
        <div class="box-header">
          <span class="box-title">❌ 失败文件 ({{ failedTotal }})</span>
        </div>
        <div v-if="failedDetails.length > 0" class="table-responsive-wrapper">
          <n-table size="small" :bordered="false" :single-line="false">
            <thead>
              <tr>
                <th>文件名</th>
                <th>归因</th>
                <th>客户端</th>
                <th>占位状态</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(ff, i) in failedDetails" :key="i">
                <td class="font-mono" style="word-break: break-all">{{ ff.file_name }}</td>
                <td style="word-break: break-all">{{ ff.reason }}</td>
                <td class="font-mono">{{ ff.client_label || '—' }}</td>
                <td class="font-mono">{{ placeholderStateLabel(ff.placeholder_state) }}</td>
              </tr>
            </tbody>
          </n-table>
        </div>
        <div v-else class="empty-diag-tip">无</div>
        <div v-if="failedTotal > failedDetails.length" class="diag-cap-note font-mono">
          共 {{ failedTotal }} 条，仅显示前 {{ failedDetails.length }} 条
        </div>
      </div>

      <div class="card glass-card section-box">
        <div class="box-header">
          <span class="box-title">🔁 防重命中 ({{ dedupTotal }})</span>
        </div>
        <div v-if="dedupSkipped.length > 0" class="table-responsive-wrapper">
          <n-table size="small" :bordered="false" :single-line="false">
            <thead>
              <tr>
                <th>文件名</th>
                <th>体积</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(d, i) in dedupSkipped" :key="i">
                <td class="font-mono" style="word-break: break-all">{{ d.file_name }}</td>
                <td class="font-mono">{{ formatBytes(d.size_bytes || 0) }}</td>
              </tr>
            </tbody>
          </n-table>
        </div>
        <div v-else class="empty-diag-tip">无</div>
        <div v-if="dedupTotal > dedupSkipped.length" class="diag-cap-note font-mono">
          共 {{ dedupTotal }} 条，仅显示前 {{ dedupSkipped.length }} 条
        </div>
      </div>

      <!-- 本地发布说明卡 (仅 publish_folder 保留) -->
      <div v-if="task.task_type === 'publish_folder'" class="card glass-card section-box">
        <div class="box-header">
          <span class="box-title">4-Bot 轮询调度与上传安全机制</span>
        </div>
        <div class="publish-diag-content">
          <div class="diag-bullet">
            🤖 <b>4-Bot 轮询防封控</b>: 小于 30 MiB 的媒体由 Bot Pool 轮询发包，降低单一 Bot 请求频次。
          </div>
          <div class="diag-bullet">
            👤 <b>Userbot 自动接管</b>: 大于 30 MiB 媒体文件自动提升为由 Userbot 直发，支持突破
            {{ STANDARD_LIMIT_LABEL }} / {{ PREMIUM_LIMIT_LABEL }} 限制。
          </div>
          <div class="diag-bullet font-mono">
            📁 目标发布目录: <b>{{ task.params?.folder_path || '-' }}</b>
          </div>
          <div class="diag-bullet font-mono">
            🎯 目标频道: <b>{{ task.params?.target_channel || '-' }}</b>
          </div>
        </div>
      </div>
    </template>

    <!-- 其他任务类型通用提示 -->
    <div v-else class="empty-diag-tip">当前任务类型无特殊专项诊断数据</div>
  </div>
</template>

<script setup lang="ts">
// 专属诊断 Tab: 转发覆盖区间与结算回执 / 下载断点落盘反查 /
// 发布类任务三表 (超限跳过 / 失败文件 / 防重命中)。
// 数据自加载: 任务 id 变化即拉取 (父面板以 display-directive="show" 保活本组件,
// 首次激活前不发起请求, 切换 Tab 不丢状态)。
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { NButton, NTable, NTag } from 'naive-ui'
import CoverageBar from '@/components/forward/CoverageBar.vue'
import { forwardApi, storageApi } from '@/api'
import { formatBytes } from '@/utils/formatters'
import { STANDARD_LIMIT_LABEL, PREMIUM_LIMIT_LABEL } from '@/utils/publishConstants'
import type { NormalizedTask } from '@/utils/taskNormalizer'
import type {
  FileItemResponse,
  FileListResponse,
  ForwardCoverageResponse,
  ForwardReceiptData,
  ForwardReceiptResponse,
  ForwardRunInterval,
} from '@/api/gen'
import { getPikPakChannelHealth } from '@/utils/pikpakHealth'
import { useTaskStore } from '@/stores/taskStore'
import { dialog } from '@/utils/discreteApi'
import { useRouter } from 'vue-router'

const props = defineProps<{ task: NormalizedTask }>()

const taskStore = useTaskStore()
const router = useRouter()
const isActionRunning = ref(false)
const currentTime = ref(Date.now() / 1000)
let timer: ReturnType<typeof setInterval> | null = null

onMounted(() => {
  timer = setInterval(() => {
    currentTime.value = Date.now() / 1000
  }, 1000)
})

onUnmounted(() => {
  if (timer) clearInterval(timer)
})

const channelHealth = computed(() => getPikPakChannelHealth(props.task, currentTime.value))

async function handlePause() {
  isActionRunning.value = true
  try {
    await taskStore.pauseTask(props.task.id)
  } finally {
    isActionRunning.value = false
  }
}

async function handleSteadyResume() {
  isActionRunning.value = true
  try {
    await taskStore.resumeTask(props.task.id, 'steady')
  } finally {
    isActionRunning.value = false
  }
}

async function handleNormalResume() {
  isActionRunning.value = true
  try {
    await taskStore.resumeTask(props.task.id, 'normal')
  } finally {
    isActionRunning.value = false
  }
}

function handlePromptSkipCurrent() {
  const targetTaskId = props.task.id
  dialog.warning({
    title: '跳过当前文件确认',
    content:
      '确定要跳过当前未完成文件吗？系统将在目标 Telegram 频道留置 ⏳ 待回填占位卡，并将该文件记入防重账本，以便流水线继续处理后续文件。',
    positiveText: '确认跳过并留置占位卡',
    negativeText: '取消',
    onPositiveClick: async () => {
      isActionRunning.value = true
      try {
        await taskStore.skipCurrentFile(targetTaskId)
      } finally {
        isActionRunning.value = false
      }
    },
  })
}

function handleNavigateAccounts() {
  router.push('/accounts')
}

interface ForwardCoverageInfo {
  min_id?: number
  max_id?: number
  covered: Array<Record<string, number>>
  forwarded_runs: ForwardRunInterval[]
  filtered_runs: ForwardRunInterval[]
  bad_runs: ForwardRunInterval[]
}

export interface FailedFileItem {
  file_name?: string
  reason?: string
  client_label?: string
  placeholder_state?: string
  [key: string]: unknown
}

export interface DedupSkippedItem {
  file_name?: string
  size_bytes?: number
  [key: string]: unknown
}

// ── 转发覆盖数据与回执 ──
const forwardCoverageData = ref<ForwardCoverageInfo | null>(null)
const forwardReceipt = ref<ForwardReceiptData | null>(null)
const loadingCoverage = ref(false)

// ── 下载落盘文件反查 ──
const relatedFiles = ref<FileItemResponse[]>([])
const loadingRelatedFiles = ref(false)

const isPublishTask = computed(() => ['publish_folder', 'pikpak_publish'].includes(props.task.task_type))

const skippedFiles = computed(() => {
  const sf = props.task?.params?.skipped_files
  return Array.isArray(sf) ? sf : []
})
const skippedFilesCount = computed(() => skippedFiles.value.length)

const failedDetails = computed<FailedFileItem[]>(() => {
  const ff = props.task?.params?.failed_files as { items?: FailedFileItem[]; total?: number } | undefined
  return ff && Array.isArray(ff.items) ? ff.items : []
})
const failedTotal = computed(() => {
  const ff = props.task?.params?.failed_files as { items?: FailedFileItem[]; total?: number } | undefined
  return ff?.total ?? failedDetails.value.length
})

// ── 失败表"占位状态"列 (执行器按占位槽实时状态富化 placeholder_state) ──
function placeholderStateLabel(state: string | unknown) {
  const labels: Record<string, string> = {
    placeholder_sent: '⏳ 占位待回填',
    editing: '🔄 回填中',
    published: '✅ 已回填',
    failed_terminal: '❌ 终态',
    lost: '⚠️ 已失效',
    flipped: '✅ 已补发',
  }
  return (typeof state === 'string' && labels[state]) || '—'
}

const dedupSkipped = computed<DedupSkippedItem[]>(() => {
  const ds = props.task?.params?.dedup_skipped as { items?: DedupSkippedItem[]; total?: number } | undefined
  return ds && Array.isArray(ds.items) ? ds.items : []
})
const dedupTotal = computed(() => {
  const ds = props.task?.params?.dedup_skipped as { items?: DedupSkippedItem[]; total?: number } | undefined
  return ds?.total ?? dedupSkipped.value.length
})

let diagnosisRequestSeq = 0
let relatedFilesRequestSeq = 0

async function fetchRelatedFiles(targetTaskId: string, diagSeq: number) {
  const reqSeq = ++relatedFilesRequestSeq
  loadingRelatedFiles.value = true
  try {
    const res = await storageApi.getFiles({ task_id: targetTaskId, limit: 100 })
    if (reqSeq !== relatedFilesRequestSeq || diagSeq !== diagnosisRequestSeq || props.task.id !== targetTaskId) {
      return
    }
    const data = res.data as FileListResponse | FileItemResponse[] | undefined
    if (Array.isArray(data)) {
      relatedFiles.value = data
    } else {
      relatedFiles.value = data?.files || []
    }
  } catch (err) {
    if (reqSeq === relatedFilesRequestSeq && diagSeq === diagnosisRequestSeq && props.task.id === targetTaskId) {
      console.error('拉取落盘文件异常:', err)
    }
  } finally {
    if (reqSeq === relatedFilesRequestSeq && diagSeq === diagnosisRequestSeq) {
      loadingRelatedFiles.value = false
    }
  }
}

function loadRelatedFiles() {
  return fetchRelatedFiles(props.task.id, diagnosisRequestSeq)
}

async function loadDiagnosisData() {
  const diagSeq = ++diagnosisRequestSeq
  const tType = props.task.task_type
  const tid = props.task.id

  if (tType === 'forward_channel') {
    const params = props.task.params || {}
    const src = params.source_chat as string | undefined
    const tgt = params.target_chat as string | undefined
    if (src && tgt) {
      loadingCoverage.value = true
      try {
        const [covRes, recRes] = await Promise.allSettled([
          forwardApi.getCoverage({ source_chat: src, target_chat: tgt }),
          forwardApi.getReceipt(tid),
        ])
        if (diagSeq !== diagnosisRequestSeq || props.task.id !== tid) {
          return
        }
        if (covRes.status === 'fulfilled') {
          const resData = covRes.value.data as ForwardCoverageResponse | undefined
          if (resData?.data) {
            const d = resData.data
            forwardCoverageData.value = {
              min_id: d.range?.min_id,
              max_id: d.range?.max_id,
              covered: (d.covered || []) as Array<Record<string, number>>,
              forwarded_runs: d.forwarded_runs || [],
              filtered_runs: d.filtered_runs || [],
              bad_runs: d.bad_runs || [],
            }
          }
        }
        if (recRes.status === 'fulfilled') {
          const resData = recRes.value.data as ForwardReceiptResponse | undefined
          if (resData?.data) {
            forwardReceipt.value = resData.data
          }
        }
      } catch (err) {
        if (diagSeq === diagnosisRequestSeq && props.task.id === tid) {
          console.error('拉取转发诊断失败:', err)
        }
      } finally {
        if (diagSeq === diagnosisRequestSeq) {
          loadingCoverage.value = false
        }
      }
    }
  } else if (tType === 'download_link' || tType === 'download_range') {
    fetchRelatedFiles(tid, diagSeq)
  }
}

watch(
  () => props.task.id,
  () => {
    diagnosisRequestSeq += 1
    relatedFilesRequestSeq += 1
    loadingCoverage.value = false
    loadingRelatedFiles.value = false
    forwardCoverageData.value = null
    forwardReceipt.value = null
    relatedFiles.value = []
    loadDiagnosisData()
  },
  { immediate: true },
)
</script>

<style scoped>
.tab-pane-content {
  display: flex;
  flex-direction: column;
  gap: 1rem;
  padding-top: 0.5rem;
  width: 100%;
  container: diagnosis-tab / inline-size;
}
.section-box {
  padding: 1rem 1.25rem;
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}
.box-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.box-title {
  font-size: 0.95rem;
  font-weight: 700;
  color: var(--text-primary);
}
/* 专属诊断 */
.forward-info-subline {
  display: flex;
  flex-wrap: wrap;
  gap: 0.45rem 1rem;
  font-size: 0.8rem;
  color: var(--text-secondary);
}
.receipt-stats-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 0.5rem;
  text-align: center;
}
.r-item {
  background: var(--bg-primary);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  padding: 0.5rem;
  min-width: 0;
}
.r-num {
  font-size: 1.2rem;
  font-weight: 800;
}
.r-item.success .r-num {
  color: #10b981;
}
.r-item.warning .r-num {
  color: #f59e0b;
}
.r-item.error .r-num {
  color: #ef4444;
}
.r-item.info .r-num {
  color: #3b82f6;
}
.r-label {
  font-size: 0.72rem;
  color: var(--text-secondary);
}
.bad-ids-block {
  display: flex;
  flex-direction: column;
  gap: 0.4rem;
  margin-top: 0.5rem;
}
.bad-tags-wrap {
  display: flex;
  flex-wrap: wrap;
  gap: 0.3rem;
  max-height: 80px;
  overflow-y: auto;
}
.files-mini-list {
  display: flex;
  flex-direction: column;
  gap: 0.4rem;
  max-height: 180px;
  overflow-y: auto;
}
.file-mini-row {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.75rem;
  background: var(--bg-primary);
  padding: 0.35rem 0.5rem;
  border-radius: 4px;
  border: 1px solid var(--border-color);
}
.file-kind-badge {
  font-size: 0.68rem;
  padding: 0.1rem 0.3rem;
  border-radius: 3px;
  background: var(--accent-light);
  color: var(--accent-color);
}
.file-path-span {
  flex: 1;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.file-size-span {
  color: var(--text-secondary);
}
.publish-diag-content {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  font-size: 0.8rem;
}
.diag-bullet {
  line-height: 1.5;
}
.empty-diag-tip {
  text-align: center;
  color: var(--text-secondary);
  font-size: 0.85rem;
  padding: 1.5rem;
}
/* 包4: 封顶信封截断提示 */
.diag-cap-note {
  font-size: 0.72rem;
  color: var(--text-secondary);
  text-align: right;
}

/* Milestone 4: PikPak 风控自愈面板专属样式 */
.pikpak-risk-box {
  border-left: 4px solid var(--accent-color, #18a058);
}
.header-with-badge {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}
.header-health-meta {
  font-size: 0.8rem;
  display: flex;
  align-items: center;
  gap: 0.35rem;
}
.risk-description-banner {
  padding: 0.6rem 0.85rem;
  border-radius: 6px;
  font-size: 0.82rem;
  line-height: 1.45;
  display: flex;
  align-items: flex-start;
  gap: 0.5rem;
  background: rgba(24, 160, 88, 0.1);
  border: 1px solid rgba(24, 160, 88, 0.2);
}
.risk-description-banner.cooldown {
  background: rgba(245, 158, 11, 0.1);
  border-color: rgba(245, 158, 11, 0.25);
  color: #f59e0b;
}
.risk-description-banner.safe_paused {
  background: rgba(249, 115, 22, 0.12);
  border-color: rgba(249, 115, 22, 0.3);
  color: #ea580c;
}
.risk-description-banner.auth_required {
  background: rgba(239, 68, 68, 0.1);
  border-color: rgba(239, 68, 68, 0.25);
  color: #ef4444;
}
.risk-description-banner.downshifted {
  background: rgba(59, 130, 246, 0.1);
  border-color: rgba(59, 130, 246, 0.25);
  color: #3b82f6;
}
.risk-stats-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 0.5rem;
  text-align: center;
}
.risk-actions-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
  padding-top: 0.5rem;
  border-top: 1px dashed var(--border-color, rgba(255, 255, 255, 0.1));
  flex-wrap: wrap;
}
.actions-right {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  flex-wrap: wrap;
}
.actions-hint {
  font-size: 0.8rem;
  color: var(--text-secondary);
}

@container diagnosis-tab (max-width: 560px) {
  .receipt-stats-grid,
  .risk-stats-grid {
    grid-template-columns: repeat(2, 1fr);
  }
}
</style>
