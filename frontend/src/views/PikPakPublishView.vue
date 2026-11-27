<template>
  <div class="view-container">
    <PikPakAuthCard ref="authCardRef" />
    <PikPakFileBrowser @bind="authCardRef?.openDeviceModal()" />

    <!-- 4. 发布配置 -->
    <div class="card glass-card">
      <div class="card-header">
        <div class="card-title">🚚 发布配置</div>
      </div>

      <div class="form-group">
        <PublishConfigSharedSection
          v-model:selected-target="selectedTarget"
          v-model:upload-concurrency="uploadConcurrency"
          v-model:upload-in-flight="uploadInFlight"
          v-model:send-placeholder-card="sendPlaceholderCard"
          v-model:album-mode="albumMode"
          v-model:force-republish="forceRepublish"
          variant="pikpak"
        />

        <div>
          <label class="form-label">指定执行 Userbot 账号 (可选):</label>
          <n-select
            v-model:value="selectedAccountId"
            :options="userbotOptions"
            placeholder="系统自动调度 (默认负载均衡)"
            clearable
          />
        </div>

        <div>
          <label class="form-label">发布成功后网盘清理模式:</label>
          <n-radio-group v-model:value="autoCleanupMode" name="auto-cleanup-mode">
            <n-radio-button v-for="opt in cleanupRadioOptions" :key="opt.value" :value="opt.value">
              {{ opt.label }}
            </n-radio-button>
          </n-radio-group>
          <div v-if="autoCleanupMode === 'none'" class="text-secondary cleanup-hint">
            ℹ️ 保留原状: 发布完成后不对网盘文件做任何移动或删除
          </div>
          <div v-else-if="autoCleanupMode === 'archive'" class="text-secondary cleanup-hint">
            📁 归档模式: 归档到根目录 [已发布归档] 文件夹，不释放网盘存储空间
          </div>
          <div v-else-if="autoCleanupMode === 'delete'" class="text-secondary cleanup-hint">
            ⚠️ 删除模式: 文件发布成功后将移入网盘回收站，根据 VIP 规则保留或释放空间
          </div>
        </div>

        <div>
          <label class="form-label">超限文件处理策略:</label>
          <n-radio-group v-model:value="oversizePolicy" name="oversize-policy">
            <n-radio-button v-for="opt in PIKPAK_OVERSIZE_POLICY_OPTIONS" :key="opt.value" :value="opt.value">
              {{ opt.label }}
            </n-radio-button>
          </n-radio-group>
          <div v-if="oversizePolicy === 'skip_and_report'" class="text-secondary cleanup-hint">
            ⏭️ 跳过实际上传，在原时序发送缺集说明占位卡，并在总结卡出具异常清单
          </div>
          <div v-else-if="oversizePolicy === 'split'" class="text-secondary cleanup-hint">
            📦 支持零落盘流式切片，各分卷具备完整视频索引，在 Telegram 客户端 100% 原生可播且支持进度条秒开拖动
          </div>
        </div>

        <!-- 切片策略单选组 (当选择切片分卷时展开) -->
        <div v-if="oversizePolicy === 'split'" class="split-strategy-panel">
          <label class="form-label">流式切片引擎策略:</label>
          <n-radio-group v-model:value="splitStrategy" name="split-strategy">
            <n-radio-button value="vsse_with_fallback">VSSE 极速重构 (默认推荐)</n-radio-button>
            <n-radio-button value="fmp4_universal">fMP4 全能保底 (FFmpeg)</n-radio-button>
            <n-radio-button value="raw">原始二进制硬切</n-radio-button>
          </n-radio-group>
          <div v-if="splitStrategy === 'vsse_with_fallback'" class="text-secondary cleanup-hint">
            ⚡ 纯 Python 零 CPU 占用，秒级切片规划；遇非标布局自动熔断降级至 fMP4
          </div>
          <div v-else-if="splitStrategy === 'fmp4_universal'" class="text-secondary cleanup-hint">
            🎬 基于 FFmpeg 管道零落盘流式切片，全格式自适应保底可播
          </div>
          <div v-else-if="splitStrategy === 'raw'" class="text-secondary cleanup-hint">
            📦 原始二进制物理分卷，不重封装视频 (仅供文件归档备份)
          </div>
        </div>

        <div>
          <label class="form-label">文件间发送延迟 (秒, ≥0.5):</label>
          <n-input-number
            v-model:value="sendDelaySeconds"
            :min="0.5"
            :step="0.5"
            placeholder="3.0"
            style="width: 100%"
          />
        </div>

        <div class="btn-wrapper">
          <n-button secondary type="info" :loading="inspecting" :disabled="!canSubmitBase" @click="handleInspect">
            🔍 提交前预检 (写权限 + audit 汇总)
          </n-button>
          <n-button
            type="primary"
            size="medium"
            :loading="pikpakStore.publishSubmitting"
            :disabled="!canSubmitBase"
            @click="handleSubmit"
          >
            🚀 提交 PikPak 发布任务
          </n-button>
        </div>

        <!-- 预检结果卡片 (audit_blocked_names 为权威拦截判定数据源 —— §8-1②) -->
        <div v-if="inspectResult" class="inspect-card">
          <div class="inspect-title">📋 提交前预检结果</div>
          <div class="inspect-badges">
            <n-tag :type="inspectResult.accessible ? 'success' : 'error'" size="small" round>
              {{ inspectResult.accessible ? '频道可达' : '频道不可达' }}
            </n-tag>
            <n-tag :type="inspectResult.has_write_permission ? 'success' : 'error'" size="small" round>
              {{ inspectResult.has_write_permission ? '具备发帖权限' : '缺少发帖权限' }}
            </n-tag>
            <n-tag size="small" round>
              {{ inspectResult.files_total }} 个文件 · {{ formatBytes(inspectResult.total_size_bytes) }}
            </n-tag>
            <n-tag v-if="(inspectResult.files_audit_blocked || 0) > 0" type="error" size="small" round>
              audit 拦截 {{ inspectResult.files_audit_blocked }} 个
            </n-tag>
          </div>
          <div
            v-if="pikpakStore.selectionView.dirGroups.length > 0"
            class="text-secondary inspect-sub"
            style="color: #f59e0b; margin-top: 6px"
          >
            💡 提示：所选的
            {{ pikpakStore.selectionView.dirGroups.length }} 个文件夹将在任务提交后由后台自动递归展开并发布
          </div>
          <div
            v-if="inspectResult.channel_title || inspectResult.permission_summary"
            class="text-secondary inspect-sub"
          >
            <template v-if="inspectResult.channel_title">{{ inspectResult.channel_title }}</template>
            <template v-if="inspectResult.permission_summary"> · {{ inspectResult.permission_summary }}</template>
          </div>
          <div v-if="(inspectResult.files_audit_blocked || 0) > 0" class="blocked-list">
            <div class="blocked-title">审核状态未通过的文件（提交将被跳过 —— 请在已选清单中移除后重试）:</div>
            <div v-for="name in inspectResult.audit_blocked_names" :key="name" class="blocked-name font-mono">
              • {{ name }}
            </div>
          </div>
          <div v-if="inspectResult.warning" class="text-secondary inspect-sub">{{ inspectResult.warning }}</div>
        </div>
      </div>
    </div>

    <!-- 5. 双向实时速率仪表盘 -->
    <PikPakSpeedDashboard />
  </div>
</template>

<script setup lang="ts">
// PikPak 流式发布主视图: 发布表单状态 + 预检/提交流程 + 确认对话框。
// 鉴权/网盘浏览/共享配置/速率仪表盘分别内聚于 components/pikpak 与 components/publish 子组件。
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { useRouter } from 'vue-router'
import { NButton, NInputNumber, NRadioButton, NRadioGroup, NSelect, NTag } from 'naive-ui'
import type { PikPakInspectResponse, PikPakPublishRequest } from '@/api/gen'
import { usePikpakStore } from '@/stores/pikpakStore'
import { useAccountStore } from '@/stores/accountStore'
import { useSettingStore } from '@/stores/settingStore'
import { dialog, message } from '@/utils/discreteApi'
import { formatBytes } from '@/utils/formatters'
import {
  CLEANUP_MODE_LABELS,
  OVERSIZE_POLICY_LABELS,
  SPLIT_STRATEGY_LABELS,
  PIKPAK_OVERSIZE_POLICY_OPTIONS,
} from '@/utils/publishConstants'
import PikPakAuthCard from '@/components/pikpak/PikPakAuthCard.vue'
import PikPakFileBrowser from '@/components/pikpak/PikPakFileBrowser.vue'
import PikPakSpeedDashboard from '@/components/pikpak/PikPakSpeedDashboard.vue'
import PublishConfigSharedSection from '@/components/publish/PublishConfigSharedSection.vue'

const router = useRouter()
const pikpakStore = usePikpakStore()
const accountStore = useAccountStore()
const settingStore = useSettingStore()

const authCardRef = ref<InstanceType<typeof PikPakAuthCard> | null>(null)
const userbotOptions = computed(() => accountStore.userbotSelectOptions)

// ── 发布表单状态 (照 PublishView 表单范式: 手动守卫 + clamp, 无 naive-ui form rule) ──
const selectedTarget = ref<string | null>(null)
const selectedAccountId = ref<number | null>(null)

let userChangedCleanup = false
let userChangedOversize = false
let userChangedSplitStrategy = false

const autoCleanupMode = ref<'none' | 'archive' | 'delete'>(
  (settingStore.allSettings?.pikpak_default_cleanup_mode as 'none' | 'archive' | 'delete') || 'archive',
)
const oversizePolicy = ref<'skip_and_report' | 'split' | 'strict_fail'>(
  (settingStore.allSettings?.pikpak_default_oversize_policy as 'skip_and_report' | 'split' | 'strict_fail') ||
    'skip_and_report',
)
const splitStrategy = ref<'vsse_with_fallback' | 'fmp4_universal' | 'raw'>(
  (settingStore.allSettings?.pikpak_default_split_strategy as 'vsse_with_fallback' | 'fmp4_universal' | 'raw') ||
    'vsse_with_fallback',
)
const sendPlaceholderCard = ref(true)
const forceRepublish = ref(false)
const albumMode = ref(false)
const sendDelaySeconds = ref<number | null>(3.0)
const uploadConcurrency = ref<number | null>(null)
const uploadInFlight = ref<number | null>(null)
const inspecting = ref(false)
const inspectResult = ref<PikPakInspectResponse | null>(null)

const cleanupRadioOptions = ['none', 'archive', 'delete'].map((v) => ({ label: CLEANUP_MODE_LABELS[v], value: v }))

watch(
  () => settingStore.allSettings,
  (s) => {
    if (!s) return
    if (!userChangedCleanup && s.pikpak_default_cleanup_mode) {
      autoCleanupMode.value = s.pikpak_default_cleanup_mode as 'none' | 'archive' | 'delete'
    }
    if (!userChangedOversize && s.pikpak_default_oversize_policy) {
      oversizePolicy.value = s.pikpak_default_oversize_policy as 'skip_and_report' | 'split' | 'strict_fail'
    }
    if (!userChangedSplitStrategy && s.pikpak_default_split_strategy) {
      splitStrategy.value = s.pikpak_default_split_strategy as 'vsse_with_fallback' | 'fmp4_universal' | 'raw'
    }
  },
  { immediate: true },
)

watch(autoCleanupMode, () => {
  userChangedCleanup = true
})
watch(oversizePolicy, () => {
  userChangedOversize = true
})
watch(splitStrategy, () => {
  userChangedSplitStrategy = true
})

const canSubmitBase = computed(
  () =>
    pikpakStore.selectedFiles.size > 0 &&
    Boolean(selectedTarget.value) &&
    !pikpakStore.publishSubmitting &&
    !inspecting.value,
)

let inspectGen = 0
let inspectCallId = 0
let isMounted = true

function currentSelectionKey(): string {
  return [...pikpakStore.selectedFiles.keys()].sort().join('\n')
}

watch([selectedTarget, selectedAccountId, currentSelectionKey], () => {
  inspectGen += 1
  inspectResult.value = null
})

interface PikPakPublishSnapshot {
  target: string
  accountId: number | null
  selectionKey: string
  confirmText: string
  payload: PikPakPublishRequest
}

function normalizeAccountId(val: unknown): number | null {
  if (val === '' || val === null || val === undefined) return null
  const num = Number(val)
  return Number.isFinite(num) ? num : null
}

function capturePublishSnapshot(): PikPakPublishSnapshot {
  const target = selectedTarget.value || ''
  const accountId = normalizeAccountId(selectedAccountId.value)
  const selectionKey = currentSelectionKey()
  const effConcurrency = uploadConcurrency.value ? Math.max(1, Math.min(Math.round(uploadConcurrency.value), 16)) : null
  const effInFlight = uploadInFlight.value ? Math.max(1, Math.min(Math.round(uploadInFlight.value), 4)) : null

  const dirGroups = pikpakStore.selectionView.dirGroups.map((group) => ({
    dirPath: group.rule.dirPath,
    fileCount: group.files.size,
  }))
  const singleCount = pikpakStore.selectionView.singles.length
  const totalFiles = pikpakStore.selectedFiles.size
  const totalBytes = pikpakStore.selectedTotalBytes

  const cleanupMode = autoCleanupMode.value
  const policy = oversizePolicy.value
  const strategy = splitStrategy.value
  const placeholderCard = sendPlaceholderCard.value
  const delaySeconds = Math.max(0.5, Number(sendDelaySeconds.value) || 3.0)
  const republish = forceRepublish.value

  const payload: PikPakPublishRequest = {
    remote_files: [...pikpakStore.selectedFiles.values()],
    target_channel: target,
    auto_cleanup_mode: cleanupMode,
    oversize_policy: policy,
    split_strategy: policy === 'split' ? strategy : null,
    send_placeholder_card: placeholderCard,
    send_delay_seconds: delaySeconds,
    force_republish: republish,
    is_folder_publish: pikpakStore.selectionView.isFolderPublish,
    folder_name: pikpakStore.selectionView.folderPublishName || null,
    dir_tree: { ...pikpakStore.selectionView.combinedDirTree },
    album_mode: albumMode.value,
  }
  if (accountId !== null && accountId !== undefined) {
    payload.account_id = accountId
  }
  if (effConcurrency) payload.upload_concurrency = effConcurrency
  if (effInFlight) payload.upload_in_flight_per_connection = effInFlight

  const lines = [
    `目标频道: ${target}`,
    `发布内容: 共 ${totalFiles} 个文件 (来自 ${dirGroups.length} 个目录勾选 + ${singleCount} 个单独勾选, 合计 ${formatBytes(totalBytes)})`,
    `发布后清理: ${CLEANUP_MODE_LABELS[cleanupMode] || cleanupMode}`,
    `超限处理策略: ${OVERSIZE_POLICY_LABELS[policy] || policy}` +
      (policy === 'split' ? ` [${SPLIT_STRATEGY_LABELS[strategy] || strategy}]` : ''),
    `占位说明卡: ${placeholderCard ? '已启用 (遇异常发暗底海报，48h 内原位编辑自愈)' : '已关闭'}`,
    `发送延迟: ${delaySeconds}s · 并发: ${effConcurrency || '默认 2'} 通道 × ${effInFlight || '默认 1'} 在途`,
  ]
  for (const group of dirGroups) {
    lines.push(`  · 📁 ${group.dirPath} — ${group.fileCount} 个文件`)
  }
  if (cleanupMode === 'delete') {
    lines.push('⚠️ 删除模式警示: 文件发布成功后将移入回收站 (配额不立即释放)')
  }
  if (republish) {
    lines.push('⚠️ 强制重传已开启: 已发布过的文件将被重复发送 (刷屏/风控风险), 实际重传清单以总结卡披露为准')
  }

  return {
    target,
    accountId,
    selectionKey,
    confirmText: lines.join('\n'),
    payload,
  }
}

function isSnapshotStale(snapshot: PikPakPublishSnapshot): boolean {
  return (
    (selectedTarget.value || '') !== snapshot.target ||
    normalizeAccountId(selectedAccountId.value) !== snapshot.accountId ||
    currentSelectionKey() !== snapshot.selectionKey
  )
}

// ── 提交流程 (三出口全部阻断: 权限失败 / audit 拦截 / 预检请求异常) ──
async function runInspect(target: string, accountId: number | null, selectionKey: string) {
  if (!isMounted || !target) return null
  const gen = ++inspectGen
  const check = await pikpakStore.inspectSelection(target, accountId)
  if (
    !isMounted ||
    gen !== inspectGen ||
    (selectedTarget.value || '') !== target ||
    normalizeAccountId(selectedAccountId.value) !== accountId ||
    currentSelectionKey() !== selectionKey
  ) {
    return null
  }
  inspectResult.value = check
  return check
}

async function handleInspect() {
  if (!selectedTarget.value || pikpakStore.selectedFiles.size === 0) {
    message.warning('请先勾选待发布文件并填写目标频道')
    return
  }
  const target = selectedTarget.value
  const accountId = normalizeAccountId(selectedAccountId.value)
  const selectionKey = currentSelectionKey()
  const callId = ++inspectCallId
  inspecting.value = true
  try {
    const check = await runInspect(target, accountId, selectionKey)
    if (check) {
      message.success('预检完成')
    }
  } finally {
    if (callId === inspectCallId) {
      inspecting.value = false
    }
  }
}

async function handleSubmit() {
  if (pikpakStore.publishSubmitting || inspecting.value) return
  if (pikpakStore.selectedFiles.size === 0) {
    message.warning('请先勾选待发布的远端文件')
    return
  }
  if (!selectedTarget.value) {
    message.warning('请先选择或输入目标频道')
    return
  }

  // 在预检前捕获不可变完整请求快照，确保预检、确认弹窗与最终提交载荷一致
  const snapshot = capturePublishSnapshot()

  // 1. 提交前 /inspect 预检 (写权限 + audit 汇总)
  const callId = ++inspectCallId
  inspecting.value = true
  let check: PikPakInspectResponse | null = null
  try {
    check = await runInspect(snapshot.target, snapshot.accountId, snapshot.selectionKey)
  } finally {
    if (callId === inspectCallId) {
      inspecting.value = false
    }
  }
  if (!isMounted) return
  if (isSnapshotStale(snapshot)) {
    inspectResult.value = null
    message.warning('目标频道、账号或已选文件在预检期间已变更，请重新提交')
    return
  }
  if (!check) {
    message.error('预检请求失败，已阻止提交')
    return
  }
  if (!check.accessible || !check.has_write_permission) {
    message.warning('🛑 目标频道不可达或缺少发帖权限，已阻止提交 (详见预检结果)')
    return
  }
  if ((check.files_audit_blocked ?? 0) > 0) {
    message.warning(
      `🛑 存在 ${check.files_audit_blocked} 个审核状态未通过的文件，已阻止提交 —— 请在已选清单中移除后重试`,
    )
    return
  }

  // 2. 确认对话框 (基于冻结快照展示，含 auto_cleanup 模式展示与删除模式额外警示)
  dialog.warning({
    title: '确认提交 PikPak 发布任务',
    content: snapshot.confirmText,
    contentStyle: { whiteSpace: 'pre-line' },
    positiveText: '确认发布',
    negativeText: '取消',
    onPositiveClick: () => doPublish(snapshot),
  })
}

async function doPublish(snapshot: PikPakPublishSnapshot) {
  const res = await pikpakStore.publishPikpak(snapshot.payload)
  // 路由跳转由组件消费返回结果触发，且仅在视图仍挂载时跳转
  if (res?.success && res.taskId && isMounted) {
    router.push('/tasks?id=' + res.taskId)
  }
}

onMounted(() => {
  isMounted = true
  accountStore.loadAccounts()
  settingStore.loadSettings()
})

onUnmounted(() => {
  isMounted = false
  inspectGen += 1
})
</script>

<style scoped>
.view-container {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
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
.text-secondary {
  color: var(--text-secondary);
}
/* 发布配置 */
.form-group {
  display: flex;
  flex-direction: column;
  gap: 1.1rem;
}
.form-label {
  display: block;
  font-size: 0.88rem;
  font-weight: 500;
  margin-bottom: 0.4rem;
}
.cleanup-hint {
  font-size: 0.8rem;
  margin-top: 0.4rem;
}
.split-strategy-panel {
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 0.75rem 1rem;
  margin-top: -0.3rem;
}
.btn-wrapper {
  display: flex;
  align-items: center;
  gap: 0.9rem;
  flex-wrap: wrap;
}
.inspect-card {
  border: 1px solid var(--border-color);
  border-radius: 10px;
  padding: 1rem;
  display: flex;
  flex-direction: column;
  gap: 0.6rem;
}
.inspect-title {
  font-size: 0.92rem;
  font-weight: 600;
}
.inspect-badges {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  flex-wrap: wrap;
}
.inspect-sub {
  font-size: 0.82rem;
}
.blocked-list {
  border-top: 1px dashed var(--border-color);
  padding-top: 0.6rem;
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}
.blocked-title {
  font-size: 0.85rem;
  color: #ef4444;
  font-weight: 500;
}
.blocked-name {
  font-size: 0.82rem;
  color: #ef4444;
}
</style>
