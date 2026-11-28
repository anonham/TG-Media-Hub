<template>
  <div class="view-container">
    <div class="card glass-card">
      <div class="card-header">
        <div class="card-title">⚡ 频道/群组云端极速转发搬运</div>
      </div>

      <div class="form-group">
        <!-- 执行配置: 账号选择与优先级 (REQ-006 置顶首屏) -->
        <div class="grid-2col exec-config-block">
          <div>
            <label class="form-label">指定执行 Userbot 账号:</label>
            <n-select
              v-model:value="selectedAccountId"
              :options="userbotOptions"
              placeholder="系统自动调度 (默认负载均衡)"
              clearable
            />
          </div>
          <div>
            <label class="form-label">调度优先级 (1~20, 默认 10):</label>
            <div class="slider-row">
              <n-slider v-model:value="taskPriority" :min="1" :max="20" class="flex-1" />
              <n-input-number
                :value="taskPriority"
                @update:value="(v) => (taskPriority = v ?? 10)"
                :min="1"
                :max="20"
                size="small"
                style="width: 80px"
              />
            </div>
          </div>
        </div>

        <div>
          <label class="form-label"
            >源频道/群组 (支持 Username 如 demo_channel、私密频道 ID 如 -100XXXXXXXXXX 或消息链接):</label
          >
          <div class="input-row">
            <n-input
              v-model:value="sourceChat"
              placeholder="例如: https://t.me/c/[频道ID]/[消息ID] 或 -100XXXXXXXXXX 或 demo_sharehub"
              class="flex-1"
            />
            <n-select
              v-if="sourcePresetOptions.length > 0"
              v-model:value="sourceChat"
              :options="sourcePresetOptions"
              placeholder="选择源频道预设"
              class="responsive-control-md"
            />
            <n-button secondary type="info" :loading="inspecting" :disabled="!sourceChat.trim()" @click="handleInspect">
              🔍 测算消息范围
            </n-button>
            <n-button secondary type="primary" @click="openPresetModal('source')"> ⚙️ 源预设 </n-button>
          </div>

          <!-- 测算结果 Banner -->
          <div v-if="inspectResult" class="inspect-banner">
            📊 <b>频道测算结果</b>: 找到消息约 <b>{{ inspectResult.total_count }}</b> 条 | 历史 ID 范围:
            <b>{{ inspectResult.min_id }}</b> ~ <b>{{ inspectResult.max_id }}</b> (已自动填入 ID 范围框)
          </div>
        </div>

        <div>
          <label class="form-label">目标私密频道 / 群组 ID (选择预设或手动输入后回车):</label>
          <div class="input-row">
            <n-select
              v-model:value="selectedTarget"
              :options="targetPresetOptions"
              placeholder="选择目标频道预设，或直接输入 -100xxx / @username"
              class="flex-1"
              filterable
              tag
            />
            <n-button secondary type="primary" @click="openPresetModal('target')"> ⚙️ 目标预设 </n-button>
          </div>
        </div>

        <div class="grid-2col">
          <div>
            <label class="form-label">起始消息 ID (必填，点击测算可自动填入):</label>
            <n-input-number v-model:value="startId" placeholder="例如: 1" :min="1" class="w-full" />
          </div>
          <div>
            <label class="form-label">结束消息 ID (必填，点击测算可自动填入):</label>
            <n-input-number v-model:value="endId" placeholder="例如: 5000" :min="1" class="w-full" />
          </div>
        </div>

        <!-- 未覆盖 Gap 快速回填 + 覆盖区间可视化 -->
        <div>
          <label class="form-label">未覆盖区间 (Gap) 快速回填 (选填，基于该源→目标的已转覆盖记录):</label>
          <div class="input-row">
            <n-select
              v-model:value="gapValue"
              :options="gapOptions"
              placeholder="选择一个未覆盖区间，自动回填起止 ID"
              class="flex-1"
              clearable
              :loading="forwardStore.gapsLoading"
              @update:value="onGapSelect"
            />
          </div>
          <div v-if="forwardStore.coverage && coverageRange.min_id != null" class="coverage-panel">
            <CoverageBar
              :range-start="coverageRange.min_id"
              :range-end="coverageRange.max_id"
              :coverage="forwardStore.coverage.covered || []"
              :forwarded-runs="forwardStore.coverage.forwarded_runs || []"
              :filtered-runs="forwardStore.coverage.filtered_runs || []"
              :bad-runs="forwardStore.coverage.bad_runs || []"
              :loading="forwardStore.coverageLoading"
              :truncated="forwardStore.coverage.truncated"
              @select="onCoverageSelect"
            />
          </div>
          <div v-else-if="sourceChat.trim() && selectedTarget && !forwardStore.coverageLoading" class="empty-hint">
            该源→目标暂无覆盖记录，完成一次转发后将生成覆盖可视化
          </div>
        </div>

        <div class="grid-2col">
          <div>
            <label class="form-label">单批转发消息条数 (Batch Size):</label>
            <n-select v-model:value="batchSize" :options="batchOptions" class="w-full" />
          </div>
          <div>
            <label class="form-label">批次发包风控延迟 (秒/每批):</label>
            <n-input-number v-model:value="delaySeconds" :min="0.8" :step="0.5" placeholder="1.5" class="w-full" />
          </div>
        </div>

        <!-- 转发模式: 增量防重 (默认) / 全量重转 / 试转 -->
        <div>
          <label class="form-label">转发模式:</label>
          <div class="mode-row">
            <n-radio-group v-model:value="forwardMode">
              <n-radio-button value="incremental">增量模式 (自动跳过已处理消息)</n-radio-button>
              <n-radio-button value="full">全量重转模式</n-radio-button>
            </n-radio-group>
            <n-checkbox v-if="forwardMode === 'full'" v-model:checked="confirmFull">
              我已知晓目标频道将出现重复消息
            </n-checkbox>
            <n-checkbox v-else v-model:checked="dryRun">
              🎬 试转模式 (首批转完自动暂停，验收后点「恢复」继续全量)
            </n-checkbox>
          </div>
          <div v-if="forwardMode === 'full'" class="warning-box">
            ⚠️ 全量重转将忽略防重账本，对目标频道重复转发范围内所有消息；处置账本仍会记录。
          </div>
          <div v-else-if="dryRun" class="inspect-banner">
            🎬 试转模式: 仅转发首个批次后自动暂停，请到目标频道验收后再恢复全量转发。
          </div>
        </div>

        <!-- 提交前预估 -->
        <div v-if="forwardStore.estimate" class="inspect-banner">
          📈 <b>提交前预估</b>: 范围约 <b>{{ forwardStore.estimate.span_count.toLocaleString() }}</b> 条 | 已处理将跳过
          <b>{{ forwardStore.estimate.already_processed.toLocaleString() }}</b> 条 | 本次将处理约
          <b>{{ forwardStore.estimate.to_process.toLocaleString() }}</b> 条 | 预计
          <b>{{ forwardStore.estimate.est_batches }}</b> 批
        </div>

        <div class="btn-wrapper btn-group-row">
          <n-button
            secondary
            type="info"
            size="medium"
            :loading="prechecking"
            :disabled="!sourceChat.trim()"
            @click="handleManualPrecheck"
          >
            🔍 权限预检
          </n-button>
          <n-button
            type="primary"
            size="medium"
            :loading="submitting || checkingAccess"
            :disabled="
              submitting ||
              checkingAccess ||
              !sourceChat.trim() ||
              !selectedTarget ||
              startId === null ||
              endId === null ||
              (forwardMode === 'full' && !confirmFull)
            "
            @click="handleSubmit"
          >
            <template #icon>🚀</template>
            启动云端极速转发
          </n-button>
        </div>
      </div>
    </div>

    <!-- 已转发消息 ID 定位与查询卡片 (默认折叠，展开才触发首次查询) -->
    <div class="card glass-card margin-top-card">
      <div class="card-header card-header-row">
        <div class="card-title">🔍 已转发消息 ID 定位与反查</div>
        <n-button secondary size="small" type="info" @click="toggleHistory">
          {{ historyExpanded ? '📂 收起' : '📂 展开查询' }}
        </n-button>
      </div>

      <div v-if="historyExpanded" class="form-group">
        <div class="grid-3col">
          <div>
            <label class="form-label">源频道 ID / 标识符 (选填):</label>
            <n-input v-model:value="searchSourceChat" placeholder="例如: -100XXXXXXXXXX" />
          </div>
          <div>
            <label class="form-label">源消息 ID (选填):</label>
            <n-input-number v-model:value="searchSourceMsgId" placeholder="例如: 1738" :min="1" class="w-full" />
          </div>
          <div>
            <label class="form-label">目标群组 ID (选填):</label>
            <n-input v-model:value="searchTargetChat" placeholder="例如: -100XXXXXXXXXX" />
          </div>
        </div>

        <div class="btn-wrapper">
          <n-button secondary type="info" :loading="forwardStore.historyLoading" @click="handleSearchHistory">
            🔍 查询对应 Telegram 消息 ID
          </n-button>
        </div>

        <!-- 查询结果表格: 滚动触底自动加载下一页 -->
        <div v-if="forwardStore.historyList.length > 0" class="history-scroll" @scroll="onHistoryScroll">
          <table class="cockpit-table">
            <thead>
              <tr>
                <th>源频道</th>
                <th>源消息 ID</th>
                <th>目标群组</th>
                <th>目标 Telegram 消息 ID</th>
                <th>转发时间</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="item in forwardStore.historyList" :key="item.id">
                <td>{{ item.source_chat }}</td>
                <td>
                  <span class="badge badge-source">ID: {{ item.source_msg_id }}</span>
                </td>
                <td>{{ item.target_chat }}</td>
                <td>
                  <span class="badge badge-target">ID: {{ item.target_msg_id }}</span>
                </td>
                <td>{{ formatDateTime(item.forwarded_at) }}</td>
              </tr>
            </tbody>
          </table>
          <div v-if="forwardStore.historyLoading" class="load-hint">加载中...</div>
          <div v-else-if="!forwardStore.historyHasMore" class="load-hint">— 没有更多了 —</div>
        </div>
        <div
          v-else-if="searched && !forwardStore.historyLoading && forwardStore.historyList.length === 0"
          class="empty-hint"
        >
          ⚠️ 未找到符合条件的已转发消息映射记录
        </div>
      </div>
      <div v-else class="empty-hint">映射反查默认收起；点击「展开查询」后才会首次请求历史记录 (不进页面自动查询)</div>
    </div>

    <!-- 转发任务完成回执卡片 -->
    <div v-if="forwardStore.receipt" class="card glass-card margin-top-card">
      <div class="card-header card-header-row">
        <div class="card-title">🧾 最近转发任务回执</div>
        <span class="badge badge-source">任务 #{{ forwardStore.receipt.task_id }}</span>
      </div>

      <div class="receipt-grid">
        <div class="receipt-stat">
          <div class="receipt-num num-success">{{ forwardStore.receipt.forwarded }}</div>
          <div class="receipt-label">成功转发</div>
        </div>
        <div class="receipt-stat">
          <div class="receipt-num num-info">{{ forwardStore.receipt.filtered }}</div>
          <div class="receipt-label">策略过滤</div>
        </div>
        <div class="receipt-stat">
          <div class="receipt-num num-error">{{ forwardStore.receipt.bad }}</div>
          <div class="receipt-label">坏消息隔离</div>
        </div>
        <div class="receipt-stat">
          <div class="receipt-num num-warning">{{ forwardStore.receipt.dedup_skipped ?? '-' }}</div>
          <div class="receipt-label">防重跳过</div>
        </div>
      </div>

      <div v-if="forwardStore.receipt.bad_ids?.length" class="receipt-section">
        <div class="receipt-section-title">坏消息 ID 清单 (已隔离跳过)</div>
        <div class="badge-list">
          <span v-for="id in forwardStore.receipt.bad_ids" :key="id" class="badge badge-error">ID: {{ id }}</span>
        </div>
      </div>

      <div v-if="forwardStore.receipt.filtered_detail?.length" class="receipt-section">
        <div
          class="receipt-section-title receipt-toggle"
          role="button"
          tabindex="0"
          @click="receiptDetailExpanded = !receiptDetailExpanded"
          @keydown.enter="receiptDetailExpanded = !receiptDetailExpanded"
          @keydown.space.prevent
          @keyup.space="receiptDetailExpanded = !receiptDetailExpanded"
        >
          {{ receiptDetailExpanded ? '▾' : '▸' }} 策略过滤明细 ({{
            forwardStore.receipt.filtered_detail.length
          }}
          类，点击{{ receiptDetailExpanded ? '收起' : '展开' }})
        </div>
        <div v-if="receiptDetailExpanded" class="badge-list badge-list-column">
          <span v-for="(d, idx) in forwardStore.receipt.filtered_detail" :key="idx" class="receipt-detail-item">
            {{ skipTypeLabel(String(d.skip_type || '')) }} × {{ Number(d.count) || 0 }}
            <span class="receipt-sample font-mono"
              >样本 ID: {{ ((d.sample_ids as number[] | undefined) || []).join(', ') }}</span
            >
          </span>
        </div>
      </div>
    </div>

    <!-- 预设管理 Modal -->
    <PresetManageModal v-model:show="showPresetModal" :default-type="presetModalType" @saved="onPresetSaved" />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { useRouter } from 'vue-router'
import { NInput, NSelect, NInputNumber, NButton, NRadioGroup, NRadioButton, NCheckbox, NSlider } from 'naive-ui'
import { useTaskStore } from '@/stores/taskStore'
import { useForwardStore } from '@/stores/forwardStore'
import { useAccountStore } from '@/stores/accountStore'
import { usePresetStore } from '@/stores/presetStore'
import { dialog, message } from '@/utils/discreteApi'
import { formatDateTime } from '@/utils/formatters'
import { errDetail } from '@/utils/errors'
import PresetManageModal from '@/components/presets/PresetManageModal.vue'
import CoverageBar from '@/components/forward/CoverageBar.vue'

const router = useRouter()
const taskStore = useTaskStore()
const forwardStore = useForwardStore()
const accountStore = useAccountStore()
const presetStore = usePresetStore()

const prechecking = ref(false)

const sourceChat = ref('')
const selectedTarget = ref<string | null>(null)
const startId = ref<number | null>(null)
const endId = ref<number | null>(null)
const batchSize = ref<number | null>(50)
const delaySeconds = ref<number | null>(1.5)

const selectedAccountId = ref<number | string | null>(null)
const taskPriority = ref<number>(10)

// 统一下拉 Getter: Userbot 选项含系统自动调度项, 预设选项由 presetStore 标准化提供
const userbotOptions = computed(() => accountStore.userbotSelectOptions)

// 转发模式: incremental 增量防重(默认) / full 全量重转(需确认勾选)
const forwardMode = ref('incremental')
const confirmFull = ref(false)
const dryRun = ref(false)
const receiptDetailExpanded = ref(false)

const SKIP_TYPE_LABELS: Record<string, string> = {
  null_message: '空消息',
  empty_message: '空消息',
  service_action: '服务动作消息',
  ghost_no_content: '幽灵消息 (无内容)',
  tos_dead_terms: 'TOS 封禁 (terms)',
  tos_dead_copyright: '版权封禁 (copyright)',
}

function skipTypeLabel(t: string): string {
  return SKIP_TYPE_LABELS[t] || t
}

const inspecting = ref(false)
interface ChannelInspectInfo {
  min_id: number
  max_id: number
  total_count?: number
  [key: string]: unknown
}

const inspectResult = ref<ChannelInspectInfo | null>(null)
const submitting = ref(false)
const checkingAccess = ref(false)

const showPresetModal = ref(false)
const presetModalType = ref('target')

// 映射反查: 默认折叠，展开才触发首次查询
const searchSourceChat = ref('')
const searchSourceMsgId = ref<number | null>(null)
const searchTargetChat = ref('')
const searched = ref(false)
const historyExpanded = ref(false)

// 未覆盖 gap 下拉选中值 ("start:end" 形式，仅用于回填)
const gapValue = ref<string | null>(null)

const batchOptions = [
  { label: '30 条 / 批 (推荐大频道，有效避开 WorkerBusy 报错)', value: 30 },
  { label: '50 条 / 批 (推荐平衡)', value: 50 },
  { label: '80 条 / 批 (快速转发)', value: 80 },
  { label: '100 条 / 批 (极限最高速率)', value: 100 },
]

// 源/目标预设选项均回退全量预设 (兼容 preset_type 引入前的存量条目)
const sourcePresetOptions = computed(() => presetStore.sourcePresetOptions)

const targetPresetOptions = computed(() => presetStore.targetPresetOptions)

// 覆盖条渲染窗口: 优先服务端返回的 range，其次测算结果，最后手动起止
const coverageRange = computed(() => {
  const c = forwardStore.coverage
  if (c?.range) return c.range
  if (inspectResult.value) return { min_id: inspectResult.value.min_id, max_id: inspectResult.value.max_id }
  if (startId.value && endId.value) return { min_id: startId.value, max_id: endId.value }
  return { min_id: null, max_id: null }
})

const gapOptions = computed(() =>
  forwardStore.gaps.map((g) => ({
    label: `#${g.start.toLocaleString()} – #${g.end.toLocaleString()}（约 ${g.est_count.toLocaleString()} 条未处理）`,
    value: `${g.start}:${g.end}`,
  })),
)

onMounted(() => {
  presetStore.loadPresets()
  accountStore.loadAccounts()
})

function showSubmitSuccessModal(taskId: string | number) {
  dialog.success({
    title: '任务已成功加入调度队列',
    content: `云端极速转发任务 ID: #${taskId} 已创建，系统调度器已接管并按优先级 P${taskPriority.value} 排队运行。`,
    positiveText: '进入任务中心查看详情',
    negativeText: '继续发布',
    onPositiveClick: () => {
      router.push(`/tasks?id=${taskId}`)
    },
  })
}

// 表单要素变化 → 防抖刷新覆盖条 / gap 下拉 / 提交前预估 (不再自动查映射历史)
let coverageTimer: ReturnType<typeof setTimeout> | null = null
watch([sourceChat, selectedTarget, startId, endId, batchSize], (_newVals, oldVals) => {
  if (coverageTimer) {
    clearTimeout(coverageTimer)
    coverageTimer = null
  }
  const src = sourceChat.value?.trim()
  if (!src || !selectedTarget.value) {
    forwardStore.clearCoverage()
    return
  }
  if (oldVals && (oldVals[0]?.trim() !== src || oldVals[1] !== selectedTarget.value)) {
    forwardStore.clearCoverage()
  }
  coverageTimer = setTimeout(() => {
    const minId = startId.value || inspectResult.value?.min_id || null
    const maxId = endId.value || inspectResult.value?.max_id || null
    forwardStore.loadCoverage(src, selectedTarget.value!, minId, maxId)
    if (minId && maxId) {
      forwardStore.loadGaps(src, selectedTarget.value!, minId, maxId)
    } else {
      forwardStore.clearGaps()
    }
    if (startId.value && endId.value) {
      forwardStore.fetchEstimate(src, selectedTarget.value!, startId.value, endId.value, batchSize.value ?? 50)
    } else {
      forwardStore.clearEstimate()
    }
  }, 600)
})

// 组件卸载清理防抖定时器: 离开页面后不再触发已失效的覆盖查询
onUnmounted(() => {
  if (coverageTimer) clearTimeout(coverageTimer)
})

function openPresetModal(type: string) {
  presetModalType.value = type
  showPresetModal.value = true
}

function onPresetSaved(preset: { presetType?: string; channelId?: string }) {
  if (preset.presetType === 'source' && preset.channelId) {
    sourceChat.value = preset.channelId
  } else if (preset.channelId) {
    selectedTarget.value = preset.channelId
  }
}

let formRevision = 0
watch(
  [
    sourceChat,
    selectedTarget,
    startId,
    endId,
    batchSize,
    delaySeconds,
    selectedAccountId,
    taskPriority,
    forwardMode,
    confirmFull,
    dryRun,
  ],
  () => {
    formRevision += 1
  },
  { flush: 'sync' },
)

let inspectGen = 0

async function handleInspect() {
  const reqSource = sourceChat.value.trim()
  const reqStart = startId.value
  const reqEnd = endId.value
  if (!reqSource) return
  const gen = ++inspectGen
  inspecting.value = true
  inspectResult.value = null
  try {
    const data = (await taskStore.inspectChannelInfo(reqSource)) as ChannelInspectInfo | undefined
    if (gen !== inspectGen || sourceChat.value.trim() !== reqSource) return
    if (data) {
      inspectResult.value = data
      if (startId.value === reqStart && endId.value === reqEnd) {
        startId.value = data.min_id
        endId.value = data.max_id
      }
    }
  } finally {
    if (gen === inspectGen) {
      inspecting.value = false
    }
  }
}

function onGapSelect(val: string | null) {
  if (!val) return
  const [s, e] = val.split(':').map(Number)
  startId.value = s
  endId.value = e
  gapValue.value = null
}

function onCoverageSelect(start: number, end: number) {
  startId.value = start
  endId.value = end
}

interface ForwardSubmitSnapshot {
  revision: number
  sourceChat: string
  targetChat: string
  startId: number
  endId: number
  batchSize: number
  delaySeconds: number
  fullRetransfer: boolean
  dryRun: boolean
  accountId: number | null
  priority: number
}

async function doSubmitForward(snapshot: ForwardSubmitSnapshot) {
  if (submitting.value) return
  submitting.value = true
  try {
    const res = await taskStore.submitChannelForward(
      snapshot.sourceChat,
      snapshot.targetChat,
      snapshot.startId,
      snapshot.endId,
      snapshot.batchSize,
      snapshot.delaySeconds,
      {
        fullRetransfer: snapshot.fullRetransfer,
        dryRun: snapshot.dryRun,
        accountId: snapshot.accountId,
        priority: snapshot.priority,
      },
    )
    if (res?.success && res.taskId) {
      if (formRevision === snapshot.revision) {
        sourceChat.value = ''
        startId.value = null
        endId.value = null
        inspectResult.value = null
        forwardMode.value = 'incremental'
        confirmFull.value = false
        dryRun.value = false
        receiptDetailExpanded.value = false
        forwardStore.clearCoverage()
      }
      showSubmitSuccessModal(res.taskId)
    }
  } finally {
    submitting.value = false
  }
}

async function handleManualPrecheck() {
  const target = sourceChat.value.trim()
  if (!target) {
    message.warning('请先输入源频道标识符、Username 或链接')
    return
  }
  prechecking.value = true
  try {
    const res = await taskStore.precheckAccess(selectedAccountId.value ? Number(selectedAccountId.value) : null, target)
    if (res?.accessible) {
      message.success(res.title || '✅ 源频道/群组访问权限正常，可安全提交')
    } else {
      dialog.warning({
        title: '⚠️ 权限预检未通过',
        content: `探测目标: ${target}\n\n原因: ${res?.reason || '无权访问或未加入该频道'}\n\n请确认账号已加入目标私密频道，或切换至拥有权限的其他账号 / 系统自动调度。`,
        positiveText: '我知道了',
      })
    }
  } catch (err: unknown) {
    message.error(`权限预检异常: ${errDetail(err)}`)
  } finally {
    prechecking.value = false
  }
}

async function handleSubmit() {
  if (submitting.value || checkingAccess.value) return
  const trimmedSource = sourceChat.value.trim()
  if (!trimmedSource || !selectedTarget.value) {
    message.warning('请先填写源频道和目标频道')
    return
  }
  if (startId.value === null || endId.value === null) {
    message.warning('请先填写起始消息 ID 和结束消息 ID（可点击“测算”自动填入）')
    return
  }
  if (forwardMode.value === 'full' && !confirmFull.value) {
    message.warning('请先勾选确认全量重转模式的重复消息提示')
    return
  }

  // 在首个 await 前冻结本次提交快照，避免预检等待期间模式或表单篡改绕过校验
  const isFull = forwardMode.value === 'full'
  const snapshot: ForwardSubmitSnapshot = {
    revision: formRevision,
    sourceChat: trimmedSource,
    targetChat: selectedTarget.value,
    startId: startId.value,
    endId: endId.value,
    batchSize: batchSize.value ?? 50,
    delaySeconds: delaySeconds.value ?? 1.5,
    fullRetransfer: isFull,
    dryRun: !isFull && dryRun.value,
    accountId: selectedAccountId.value ? Number(selectedAccountId.value) : null,
    priority: taskPriority.value ?? 10,
  }

  // REQ-005: 账号访问权限预检 (Dryrun)
  if (snapshot.accountId !== null) {
    checkingAccess.value = true
    let checkRes
    try {
      checkRes = await taskStore.precheckAccess(snapshot.accountId, snapshot.sourceChat)
    } catch (err: unknown) {
      message.error(`权限预检异常: ${errDetail(err)}`)
      return
    } finally {
      checkingAccess.value = false
    }
    if (checkRes && checkRes.accessible === false) {
      dialog.warning({
        title: '⚠️ 源频道访问预检警告',
        content: `当前选择的 Userbot 账号可能无法访问该源频道/群组。\n\n原因: ${checkRes.reason || '无权访问或未加入'}\n\n是否仍要强制创建此转发任务？`,
        positiveText: '仍要创建',
        negativeText: '取消',
        onPositiveClick: async () => {
          await doSubmitForward(snapshot)
        },
      })
      return
    }
  }

  await doSubmitForward(snapshot)
}

// 选中的转发任务完成时自动拉取回执 (幂等保护：监听原始状态字段，避免因对象引用变动导致每轮轮询重复拉取)
watch(
  [() => taskStore.selectedTaskId, () => taskStore.selectedTask?.status, () => taskStore.selectedTask?.task_type],
  ([taskId, status, taskType]) => {
    if (taskId && taskType === 'forward_channel' && status === 'completed') {
      if (forwardStore.receipt?.task_id !== taskId && !forwardStore.receiptLoading) {
        forwardStore.loadReceipt(String(taskId))
      }
    }
  },
  { immediate: true },
)

function toggleHistory() {
  historyExpanded.value = !historyExpanded.value
  if (historyExpanded.value && !searched.value) {
    handleSearchHistory()
  }
}

async function handleSearchHistory() {
  const params: Record<string, unknown> = {}
  if (searchSourceChat.value.trim()) params.source_chat = searchSourceChat.value.trim()
  if (searchSourceMsgId.value) params.source_msg_id = searchSourceMsgId.value
  if (searchTargetChat.value.trim()) params.target_chat = searchTargetChat.value.trim()
  searched.value = true
  await forwardStore.searchHistory(params)
}

function onHistoryScroll(e: Event) {
  const el = e.target as HTMLElement | null
  if (el && el.scrollTop + el.clientHeight >= el.scrollHeight - 40) {
    forwardStore.loadMoreHistory()
  }
}
</script>

<style scoped>
.view-container {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
  width: 100%;
  container: forward-root / inline-size;
}

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

.form-group {
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
}

.form-label {
  font-size: 0.88rem;
  font-weight: 500;
  color: var(--text-secondary);
  margin-bottom: 0.4rem;
  display: block;
}

.input-row {
  display: flex;
  flex-wrap: wrap;
  gap: 0.65rem;
}

.flex-1 {
  flex: 1 1 240px;
  min-width: 0;
}

.grid-2col {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1rem;
}

.exec-config-block {
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 0.85rem 1rem;
}

.slider-row {
  display: flex;
  align-items: center;
  gap: 0.85rem;
}

.grid-3col {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr;
  gap: 1rem;
}

.w-full {
  width: 100%;
}

.inspect-banner {
  margin-top: 0.75rem;
  padding: 0.75rem 1rem;
  background: rgba(14, 165, 233, 0.1);
  border: 1px solid rgba(14, 165, 233, 0.2);
  border-radius: 6px;
  font-size: 0.9rem;
  color: var(--info-color);
}

.coverage-panel {
  margin-top: 0.75rem;
  padding: 0.75rem 1rem;
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-radius: 6px;
}

.mode-row {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.75rem;
}

.warning-box {
  margin-top: 0.6rem;
  padding: 0.6rem 0.9rem;
  background: rgba(245, 158, 11, 0.1);
  border: 1px solid rgba(245, 158, 11, 0.25);
  border-left: 3px solid var(--warning-color);
  border-radius: 6px;
  font-size: 0.85rem;
  color: var(--warning-color);
}

.card-header-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 1rem;
}

.history-scroll {
  margin-top: 1rem;
  max-height: 320px;
  overflow-y: auto;
  overflow-x: auto;
}

.load-hint {
  text-align: center;
  padding: 0.6rem;
  font-size: 0.8rem;
  color: var(--text-secondary);
}

.receipt-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 1rem;
  margin-bottom: 1rem;
}

.receipt-stat {
  text-align: center;
  padding: 0.9rem 0.5rem;
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-radius: 8px;
}

.receipt-num {
  font-size: 1.6rem;
  font-weight: 700;
  font-family: 'JetBrains Mono', 'Fira Code', Consolas, monospace;
}

.num-success {
  color: var(--success-color);
}
.num-info {
  color: var(--info-color);
}
.num-error {
  color: var(--danger-color);
}
.num-warning {
  color: var(--warning-color);
}

.receipt-label {
  margin-top: 0.3rem;
  font-size: 0.8rem;
  color: var(--text-secondary);
}

.receipt-section {
  margin-top: 0.75rem;
}

.receipt-section-title {
  font-size: 0.85rem;
  font-weight: 500;
  color: var(--text-secondary);
}

.receipt-toggle {
  cursor: pointer;
  user-select: none;
}

.receipt-toggle:hover {
  color: var(--accent-color);
}

.badge-list {
  display: flex;
  flex-wrap: wrap;
  gap: 0.4rem;
  margin-top: 0.5rem;
}

.badge-list-column {
  flex-direction: column;
  align-items: flex-start;
}

.badge-error {
  background: rgba(239, 68, 68, 0.15);
  color: var(--danger-color);
}

.receipt-detail-item {
  font-size: 0.85rem;
  color: var(--text-primary);
}

.receipt-sample {
  color: var(--text-secondary);
  font-size: 0.78rem;
}

.btn-wrapper {
  margin-top: 0.5rem;
  display: flex;
  justify-content: flex-end;
}

.margin-top-card {
  margin-top: 0.5rem;
}

.badge {
  display: inline-block;
  padding: 0.2rem 0.5rem;
  font-size: 0.75rem;
  font-weight: 600;
  border-radius: 4px;
}

.badge-source {
  background: rgba(99, 102, 241, 0.15);
  color: var(--indigo-color);
}

.badge-target {
  background: rgba(16, 185, 129, 0.15);
  color: var(--success-color);
}

.empty-hint {
  text-align: center;
  padding: 2rem;
  color: var(--text-secondary);
  font-size: 0.9rem;
}

.btn-group-row {
  display: flex;
  flex-wrap: wrap;
  gap: 0.65rem;
  justify-content: flex-end;
  align-items: center;
}

@container forward-root (max-width: 720px) {
  .grid-2col,
  .grid-3col {
    grid-template-columns: 1fr;
  }
}

@container forward-root (max-width: 580px) {
  .receipt-grid {
    grid-template-columns: repeat(2, 1fr);
    gap: 0.65rem;
  }
}
</style>
