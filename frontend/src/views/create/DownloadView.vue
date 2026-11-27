<template>
  <div class="view-container">
    <div class="card glass-card">
      <div class="card-header">
        <div class="card-title">📥 突破受限媒体保存下载</div>
      </div>

      <!-- 执行配置: 账号选择、并发线程与优先级 -->
      <div class="grid-3col exec-config-block">
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
          <label class="form-label">下载并发线程数 (4~32，4的倍数):</label>
          <n-input-number
            v-model:value="downloadThreads"
            :min="4"
            :max="32"
            :step="4"
            placeholder="默认 16"
            clearable
            style="width: 100%"
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
              class="priority-input"
            />
          </div>
        </div>
      </div>

      <n-tabs type="segment" class="subtabs" v-model:value="activeSubtab">
        <!-- 模式 1: 链接粘贴模式 -->
        <n-tab-pane name="links" tab="🔗 链接粘贴模式">
          <div class="form-group">
            <label class="form-label">粘贴 Telegram 消息链接 (支持单条或多行私密/公开链接):</label>
            <n-input
              v-model:value="linksInput"
              type="textarea"
              placeholder="https://t.me/c/[频道ID]/[消息ID]&#10;https://t.me/demo_channel/16074"
              :rows="5"
            />

            <div class="btn-wrapper btn-group-row">
              <n-button
                secondary
                type="info"
                size="medium"
                :loading="precheckingLink"
                :disabled="!linksInput.trim()"
                @click="handleManualPrecheckLink"
              >
                🔍 权限预检
              </n-button>
              <n-button
                type="primary"
                size="medium"
                :loading="submitting"
                :disabled="submitting || !linksInput.trim()"
                @click="handleLinkSubmit"
              >
                <template #icon>⚡</template>
                开始并发极速下载
              </n-button>
            </div>
          </div>
        </n-tab-pane>

        <!-- 模式 2: 频道消息 ID 范围模式 -->
        <n-tab-pane name="range" tab="📊 频道消息 ID 范围模式">
          <div class="form-group">
            <!-- 源频道选择与输入 -->
            <div>
              <label class="form-label">目标下载源频道 (支持选择预设或手动输入 Username / ID / 链接):</label>
              <div class="input-row">
                <n-input
                  v-model:value="channelInput"
                  placeholder="例如: -100XXXXXXXXXX 或 demo_channel 或 https://t.me/c/[频道ID]"
                  class="flex-1"
                />
                <n-select
                  v-if="sourcePresetOptions.length > 0"
                  v-model:value="channelInput"
                  :options="sourcePresetOptions"
                  placeholder="快速选择源频道预设"
                  class="responsive-control-md"
                />
                <n-button secondary type="primary" @click="showPresetModal = true"> ⚙️ 管理/新增预设 </n-button>
              </div>
            </div>

            <div class="grid-2col">
              <div>
                <label class="form-label">起始消息 ID:</label>
                <n-input-number v-model:value="startId" placeholder="100" :min="1" class="w-full" />
              </div>
              <div>
                <label class="form-label">结束消息 ID:</label>
                <n-input-number v-model:value="endId" placeholder="200" :min="1" class="w-full" />
              </div>
            </div>

            <div class="action-btn-row">
              <n-button
                secondary
                type="info"
                :loading="precheckingRange"
                :disabled="!channelInput.trim()"
                @click="handleManualPrecheckRange"
              >
                🔍 权限预检
              </n-button>

              <n-button
                secondary
                type="info"
                :loading="inspecting"
                :disabled="!channelInput.trim() || !startId || !endId"
                @click="handleInspectRange"
              >
                🔍 预检频道与消息范围
              </n-button>

              <n-button
                type="primary"
                size="medium"
                :loading="submitting"
                :disabled="submitting || !channelInput.trim() || !startId || !endId"
                @click="handleRangeSubmit"
              >
                <template #icon>🚀</template>
                批量抓取下载
              </n-button>
            </div>

            <!-- 范围预检报告卡片 -->
            <div
              v-if="rangeInspectResult"
              class="range-inspect-card"
              :class="{ 'inspect-error': !rangeInspectResult.is_accessible }"
            >
              <div class="inspect-header-row">
                <div class="inspect-title-group">
                  <span class="inspect-main-title">📊 <b>频道消息范围预检报告</b></span>
                  <span class="inspect-channel-name"
                    ><b>{{ rangeInspectResult.channel_title }}</b></span
                  >
                  <span class="badge" :class="rangeInspectResult.is_accessible ? 'badge-success' : 'badge-error'">
                    {{ rangeInspectResult.is_accessible ? '连通正常' : '访问失败' }}
                  </span>
                  <span v-if="rangeInspectResult.is_restricted" class="badge badge-warning"> 🛡️ 受限内容保护频道 </span>
                  <span v-if="rangeInspectResult.transit_capable" class="badge badge-success">
                    🚀 梯队 1 · 多 Bot 联合加速已就绪
                  </span>
                </div>
                <div class="inspect-range-tag">
                  消息范围: ID <b>{{ rangeInspectResult.start_id }}</b> ~ <b>{{ rangeInspectResult.end_id }}</b> (共
                  {{ rangeInspectResult.total_range_messages }} 条)
                </div>
              </div>

              <div v-if="rangeInspectResult.is_accessible" class="range-stats-container">
                <!-- 统计卡片网格 -->
                <div class="stats-grid">
                  <div class="stat-card">
                    <div class="stat-icon">🎬</div>
                    <div class="stat-info">
                      <div class="stat-label">视频资源</div>
                      <div class="stat-value">
                        <b>{{ rangeInspectResult.videos_count }}</b> 个
                      </div>
                    </div>
                  </div>

                  <div class="stat-card">
                    <div class="stat-icon">📸</div>
                    <div class="stat-info">
                      <div class="stat-label">图片相册</div>
                      <div class="stat-value">
                        <b>{{ rangeInspectResult.photos_count }}</b> 张
                      </div>
                    </div>
                  </div>

                  <div class="stat-card">
                    <div class="stat-icon">🎵</div>
                    <div class="stat-info">
                      <div class="stat-label">音频音乐</div>
                      <div class="stat-value">
                        <b>{{ rangeInspectResult.audios_count }}</b> 首
                      </div>
                    </div>
                  </div>

                  <div class="stat-card">
                    <div class="stat-icon">📄</div>
                    <div class="stat-info">
                      <div class="stat-label">文档/其他</div>
                      <div class="stat-value">
                        <b>{{ rangeInspectResult.documents_count }}</b> 个
                      </div>
                    </div>
                  </div>
                </div>

                <!-- 汇总指示条 -->
                <div class="summary-bar">
                  <div class="summary-item">
                    📦 预估下载容量: <b>{{ rangeInspectResult.estimated_size_gb }} GiB</b> ({{
                      rangeInspectResult.estimated_size_mb
                    }}
                    MiB)
                  </div>
                  <div class="summary-item">
                    💽 磁盘剩余空间:
                    <b>{{ rangeInspectResult.disk_free_gb }} GiB</b>
                    <span class="badge" :class="rangeInspectResult.disk_sufficient ? 'badge-success' : 'badge-error'">
                      {{ rangeInspectResult.disk_sufficient ? '容量充足' : '容量不足' }}
                    </span>
                  </div>
                  <div class="summary-item">
                    ✨ 含媒体消息: <b>{{ rangeInspectResult.messages_with_media }}</b> /
                    {{ rangeInspectResult.total_range_messages }} 条
                  </div>
                  <div class="summary-item" v-if="rangeInspectResult.already_downloaded_count > 0">
                    ⏩ 本地已有有效文件: <b>{{ rangeInspectResult.already_downloaded_count }}</b> 条
                  </div>
                </div>

                <!-- 梯队 1 中转借壳加速预测提示 -->
                <div v-if="rangeInspectResult.transit_notice" class="transit-notice-box">
                  {{ rangeInspectResult.transit_notice }}
                </div>

                <!-- 警告或建议提示 -->
                <div
                  v-if="rangeInspectResult.warning_messages && rangeInspectResult.warning_messages.length > 0"
                  class="warning-box"
                >
                  <div v-for="(warn, wIdx) in rangeInspectResult.warning_messages" :key="wIdx">
                    {{ warn }}
                  </div>
                </div>

                <!-- 快捷确认按钮 -->
                <div class="confirm-btn-row">
                  <n-button
                    type="primary"
                    secondary
                    :loading="submitting"
                    :disabled="submitting"
                    @click="handleRangeSubmit"
                  >
                    ⚡ 确认预检无误，立即开始批量下载
                  </n-button>
                </div>
              </div>

              <div v-else class="range-error-box">
                ❌ 预检失败原因: {{ rangeInspectResult.error_detail || '无法获取频道或消息元数据' }}
              </div>
            </div>
          </div>
        </n-tab-pane>
      </n-tabs>
    </div>

    <!-- 预设管理 Modal -->
    <PresetManageModal v-model:show="showPresetModal" default-type="source" @saved="onPresetSaved" />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { NTabs, NTabPane, NInput, NInputNumber, NSelect, NButton, NSlider } from 'naive-ui'
import { useTaskStore } from '@/stores/taskStore'
import { useAccountStore } from '@/stores/accountStore'
import { usePresetStore } from '@/stores/presetStore'
import { dialog, message } from '@/utils/discreteApi'
import { errDetail } from '@/utils/errors'
import type { AlreadyDownloadedDetail, DownloadTaskConflictDetail, InspectDownloadRangeResponse } from '@/api/gen'
import PresetManageModal from '@/components/presets/PresetManageModal.vue'

const router = useRouter()
const taskStore = useTaskStore()
const accountStore = useAccountStore()
const presetStore = usePresetStore()

const activeSubtab = ref<'links' | 'range'>('links')
const linksInput = ref('')
const channelInput = ref('')
const startId = ref<number | null>(null)
const endId = ref<number | null>(null)
const submitting = ref(false)
const precheckingLink = ref(false)
const precheckingRange = ref(false)

const selectedAccountId = ref<number | string | null>(null)
const taskPriority = ref<number>(10)
const downloadThreads = ref<number | null>(16)

const inspecting = ref(false)
const rangeInspectResult = ref<InspectDownloadRangeResponse | null>(null)
const showPresetModal = ref(false)

// 统一下拉 Getter: Userbot 选项含系统自动调度项, 预设选项由 presetStore 标准化提供
const userbotOptions = computed(() => accountStore.userbotSelectOptions)

const sourcePresetOptions = computed(() => presetStore.sourcePresetOptions)

let rangeInspectGen = 0
let rangeInspectCallId = 0
let inspectedRange: RangeTarget | null = null

// 当频道或 ID 范围变化时，重置预检结果并作废旧的异步请求
watch([channelInput, startId, endId], () => {
  rangeInspectGen += 1
  rangeInspectResult.value = null
  inspectedRange = null
})

onMounted(() => {
  presetStore.loadPresets()
  accountStore.loadAccounts()
})

function onPresetSaved(preset: { presetType?: string; channelId?: string }) {
  if (preset.presetType === 'source' && preset.channelId) {
    channelInput.value = preset.channelId
  }
}

function showSubmitSuccessModal(taskId: string | number) {
  dialog.success({
    title: '任务已成功加入调度队列',
    content: `任务 ID: #${taskId} 已创建，系统调度器已接管并按优先级 P${taskPriority.value} 排队运行。`,
    positiveText: '进入任务中心查看详情',
    negativeText: '继续发布',
    onPositiveClick: () => {
      router.push(`/tasks?id=${taskId}`)
    },
  })
}

function showActiveDownloadConflict(conflict: DownloadTaskConflictDetail) {
  dialog.warning({
    title: conflict.message,
    content: conflict.status === 'paused' ? '可以在原任务详情中继续下载。' : '可以在任务详情中查看进度。',
    positiveText: '查看任务',
    negativeText: '关闭',
    onPositiveClick: () => {
      router.push({ name: 'TaskCenter', query: { id: conflict.task_id } })
    },
  })
}

function showAlreadyDownloaded(conflict: AlreadyDownloadedDetail, forceSubmit: () => Promise<void>) {
  submitting.value = true
  let actionTaken = false
  function dismiss() {
    if (actionTaken) return
    actionTaken = true
    submitting.value = false
  }
  dialog.warning({
    title: conflict.message,
    content: `本地已有 ${conflict.file_count ?? 0} 个文件。${conflict.task_id ? `历史任务 #${conflict.task_id}。` : ''}`,
    positiveText: '强制重新下载',
    negativeText: '前往存储管理查看',
    onPositiveClick: async () => {
      if (actionTaken) return false
      actionTaken = true
      await forceSubmit()
    },
    onNegativeClick: () => {
      if (actionTaken) return false
      dismiss()
      void router.push({ name: 'Storage' })
    },
    onClose: dismiss,
    onMaskClick: dismiss,
    onEsc: dismiss,
  })
}

interface LinkSubmitSnapshot {
  links: string
  accountId: number | null
  priority: number
  threads: number
}

async function doSubmitLink(snapshot: LinkSubmitSnapshot, force = false) {
  submitting.value = true
  let res
  try {
    res = await taskStore.submitLinkDownload(
      snapshot.links,
      snapshot.accountId,
      snapshot.priority,
      snapshot.threads,
      force,
    )
  } finally {
    submitting.value = false
  }
  if (res?.success && res.taskId) {
    if (linksInput.value.trim() === snapshot.links) linksInput.value = ''
    showSubmitSuccessModal(res.taskId)
  } else if (res?.downloadConflict?.code === 'already_downloaded') {
    showAlreadyDownloaded(res.downloadConflict, () => doSubmitLink(snapshot, true))
  } else if (res?.downloadConflict) {
    showActiveDownloadConflict(res.downloadConflict)
  }
}

async function handleManualPrecheckLink() {
  const firstLine = linksInput.value.trim().split('\n')[0].trim()
  if (!firstLine) {
    message.warning('请先输入至少一条 Telegram 下载链接')
    return
  }
  precheckingLink.value = true
  try {
    const res = await taskStore.precheckAccess(
      selectedAccountId.value ? Number(selectedAccountId.value) : null,
      firstLine,
    )
    if (res?.accessible) {
      message.success(res.title || '✅ 目标频道/群组访问权限正常，可安全提交')
    } else {
      dialog.warning({
        title: '⚠️ 权限预检未通过',
        content: `探测目标: ${firstLine}\n\n原因: ${res?.reason || '无权访问或未加入该频道'}\n\n请确认账号已加入目标私密频道，或切换至拥有权限的其他账号 / 系统自动调度。`,
        positiveText: '我知道了',
      })
    }
  } catch (err: unknown) {
    message.error(`权限预检异常: ${errDetail(err)}`)
  } finally {
    precheckingLink.value = false
  }
}

async function handleLinkSubmit() {
  if (submitting.value) return
  const trimmedLinks = linksInput.value.trim()
  if (!trimmedLinks) return

  const effectiveThreads = downloadThreads.value
    ? Math.max(4, Math.min(Math.round(downloadThreads.value / 4) * 4, 32))
    : 16
  const snapshot: LinkSubmitSnapshot = {
    links: trimmedLinks,
    accountId: selectedAccountId.value ? Number(selectedAccountId.value) : null,
    priority: taskPriority.value ?? 10,
    threads: effectiveThreads,
  }

  // 同步锁定提交状态，防止 precheckAccess 等待期间并发重入
  submitting.value = true

  // REQ-005: 账号访问权限预检 (Dryrun)
  if (snapshot.accountId !== null) {
    const firstLine = trimmedLinks.split('\n')[0].trim()
    if (firstLine) {
      let checkRes
      try {
        checkRes = await taskStore.precheckAccess(snapshot.accountId, firstLine)
      } catch (err: unknown) {
        submitting.value = false
        message.error(`权限预检异常: ${errDetail(err)}`)
        return
      }
      if (checkRes && checkRes.accessible === false) {
        let confirmPending = false
        let confirmationStarted = false
        dialog.warning({
          title: '⚠️ 目标频道访问预检警告',
          content: `当前选择的 Userbot 账号可能无法访问目标链接中的频道/群组。\n\n原因: ${checkRes.reason || '无权访问或未加入'}\n\n是否仍要强制创建此下载任务？`,
          positiveText: '仍要创建',
          negativeText: '取消',
          onPositiveClick: async () => {
            if (confirmPending) return false
            confirmPending = true
            confirmationStarted = true
            try {
              await doSubmitLink(snapshot)
            } finally {
              confirmPending = false
            }
          },
          onNegativeClick: () => {
            if (!confirmationStarted) submitting.value = false
          },
          onClose: () => {
            if (!confirmationStarted) submitting.value = false
          },
          onMaskClick: () => {
            if (!confirmationStarted) submitting.value = false
          },
          onEsc: () => {
            if (!confirmationStarted) submitting.value = false
          },
        })
        return
      }
    }
  }

  await doSubmitLink(snapshot)
}

async function handleManualPrecheckRange() {
  const target = channelInput.value.trim()
  if (!target) {
    message.warning('请先输入目标源频道 Username、ID 或链接')
    return
  }
  precheckingRange.value = true
  try {
    const res = await taskStore.precheckAccess(selectedAccountId.value ? Number(selectedAccountId.value) : null, target)
    if (res?.accessible) {
      message.success(res.title || '✅ 目标频道/群组访问权限正常，可安全提交')
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
    precheckingRange.value = false
  }
}

async function handleInspectRange() {
  const trimmedChannel = channelInput.value.trim()
  const reqStart = startId.value
  const reqEnd = endId.value
  if (!trimmedChannel || !reqStart || !reqEnd) return
  await inspectRange({ channel: trimmedChannel, startId: reqStart, endId: reqEnd })
}

async function inspectRange(range: RangeTarget, showSuccess = true) {
  const gen = ++rangeInspectGen
  const callId = ++rangeInspectCallId
  inspecting.value = true
  try {
    const res = showSuccess
      ? await taskStore.inspectDownloadRange(range.channel, range.startId, range.endId)
      : await taskStore.inspectDownloadRange(range.channel, range.startId, range.endId, false)
    if (gen !== rangeInspectGen || !isCurrentRange(range)) return null
    rangeInspectResult.value = res ?? null
    inspectedRange = res ? range : null
    return res
  } finally {
    if (callId === rangeInspectCallId) {
      inspecting.value = false
    }
  }
}

interface RangeTarget {
  channel: string
  startId: number
  endId: number
}

interface RangeSubmitSnapshot extends RangeTarget {
  accountId: number | null
  priority: number
  threads: number
}

function isCurrentRange(range: RangeTarget) {
  return range.channel === channelInput.value.trim() && range.startId === startId.value && range.endId === endId.value
}

function isFullyDownloaded(
  report: InspectDownloadRangeResponse | null | undefined,
): report is InspectDownloadRangeResponse {
  return !!(report?.is_accessible && report.all_media_downloaded)
}

async function doSubmitRange(snapshot: RangeSubmitSnapshot, force = false) {
  submitting.value = true
  let res
  try {
    res = await taskStore.submitRangeDownload(
      snapshot.channel,
      snapshot.startId,
      snapshot.endId,
      snapshot.accountId,
      snapshot.priority,
      snapshot.threads,
      force,
    )
  } finally {
    submitting.value = false
  }
  if (res?.success && res.taskId) {
    if (isCurrentRange(snapshot)) {
      channelInput.value = ''
      startId.value = null
      endId.value = null
      rangeInspectResult.value = null
    }
    showSubmitSuccessModal(res.taskId)
  } else if (res?.downloadConflict?.code === 'already_downloaded') {
    showAlreadyDownloaded(res.downloadConflict, () => doSubmitRange(snapshot, true))
  } else if (res?.downloadConflict) {
    showActiveDownloadConflict(res.downloadConflict)
  }
}

async function handleRangeSubmit() {
  if (submitting.value) return
  const trimmedChannel = channelInput.value.trim()
  const reqStart = startId.value
  const reqEnd = endId.value
  if (!trimmedChannel || reqStart === null || reqEnd === null || !reqStart || !reqEnd) return

  const effectiveThreads = downloadThreads.value
    ? Math.max(4, Math.min(Math.round(downloadThreads.value / 4) * 4, 32))
    : 16
  const snapshot: RangeSubmitSnapshot = {
    channel: trimmedChannel,
    startId: reqStart,
    endId: reqEnd,
    accountId: selectedAccountId.value ? Number(selectedAccountId.value) : null,
    priority: taskPriority.value ?? 10,
    threads: effectiveThreads,
  }

  // 同步锁定提交状态，防止重新检查历史或访问权限时并发重入。
  submitting.value = true
  if (inspectedRange && isCurrentRange(inspectedRange) && isFullyDownloaded(rangeInspectResult.value)) {
    try {
      const freshReport = await inspectRange(snapshot, false)
      if (isFullyDownloaded(freshReport)) {
        showAlreadyDownloaded(
          {
            code: 'already_downloaded',
            message: '目标文件均已下载到本地',
            file_count: freshReport.messages_with_media,
          },
          () => doSubmitRange(snapshot, true),
        )
        return
      }
    } catch (err: unknown) {
      message.error(`范围预检失败: ${errDetail(err)}`)
    }
  }

  // REQ-005: 账号访问权限预检 (Dryrun)
  if (snapshot.accountId !== null) {
    let checkRes
    try {
      checkRes = await taskStore.precheckAccess(snapshot.accountId, snapshot.channel)
    } catch (err: unknown) {
      submitting.value = false
      message.error(`权限预检异常: ${errDetail(err)}`)
      return
    }
    if (checkRes && checkRes.accessible === false) {
      let confirmPending = false
      let confirmationStarted = false
      dialog.warning({
        title: '⚠️ 目标频道访问预检警告',
        content: `当前选择的 Userbot 账号可能无法访问该频道/群组。\n\n原因: ${checkRes.reason || '无权访问或未加入'}\n\n是否仍要强制创建此范围抓取任务？`,
        positiveText: '仍要创建',
        negativeText: '取消',
        onPositiveClick: async () => {
          if (confirmPending) return false
          confirmPending = true
          confirmationStarted = true
          try {
            await doSubmitRange(snapshot)
          } finally {
            confirmPending = false
          }
        },
        onNegativeClick: () => {
          if (!confirmationStarted) submitting.value = false
        },
        onClose: () => {
          if (!confirmationStarted) submitting.value = false
        },
        onMaskClick: () => {
          if (!confirmationStarted) submitting.value = false
        },
        onEsc: () => {
          if (!confirmationStarted) submitting.value = false
        },
      })
      return
    }
  }

  await doSubmitRange(snapshot)
}
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

.subtabs {
  margin-top: 0.75rem;
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
  padding-top: 0.5rem;
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

.grid-3col {
  display: grid;
  grid-template-columns: 1.2fr auto 1.2fr;
  gap: 1.25rem;
  align-items: center;
}

@media (max-width: 1024px) {
  .grid-3col {
    grid-template-columns: 1fr;
  }
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

.priority-input {
  flex: 0 0 75px;
}

.w-full {
  width: 100%;
}

.action-btn-row {
  display: flex;
  justify-content: flex-end;
  flex-wrap: wrap;
  gap: 0.65rem;
}

.btn-wrapper {
  margin-top: 0.5rem;
  display: flex;
  justify-content: flex-end;
}

.range-inspect-card {
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 1.2rem;
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.inspect-error {
  border-color: rgba(239, 68, 68, 0.3);
  background: rgba(239, 68, 68, 0.05);
}

.inspect-header-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.5rem;
  border-bottom: 1px solid var(--border-color);
  padding-bottom: 0.6rem;
}

.inspect-title-group {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  flex-wrap: wrap;
}

.inspect-main-title {
  font-size: 1rem;
}

.inspect-channel-name {
  font-size: 1rem;
  color: var(--accent-color);
}

.inspect-range-tag {
  font-size: 0.85rem;
  color: var(--text-secondary);
}

.range-stats-container {
  display: flex;
  flex-direction: column;
  gap: 1rem;
  min-inline-size: 0;
  container: range-stats / inline-size;
}

.stats-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  gap: 0.75rem;
  min-inline-size: 0;
}

@container range-stats (min-width: 18rem) {
  .stats-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@container range-stats (min-width: 40rem) {
  .stats-grid {
    grid-template-columns: repeat(4, minmax(0, 1fr));
  }
}

.stat-card {
  background: var(--bg-card);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  padding: 0.75rem 1rem;
  display: flex;
  align-items: center;
  gap: 0.75rem;
  min-inline-size: 0;
}

.stat-icon {
  font-size: 1.6rem;
  flex: 0 0 auto;
}

.stat-info {
  flex: 1 1 auto;
  min-inline-size: 0;
}

.stat-label {
  font-size: 0.82rem;
  color: var(--text-secondary);
  overflow-wrap: anywhere;
}

.stat-value {
  font-size: 1.05rem;
  color: var(--text-primary);
  overflow-wrap: anywhere;
}

.summary-bar {
  display: flex;
  gap: 1.25rem;
  flex-wrap: wrap;
  background: var(--bg-card);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  padding: 0.6rem 0.8rem;
  font-size: 0.88rem;
}

.summary-item b {
  color: var(--accent-color);
}

.transit-notice-box {
  padding: 0.55rem 0.85rem;
  background: rgba(16, 185, 129, 0.1);
  border-left: 3px solid #10b981;
  border-radius: 4px;
  font-size: 0.85rem;
  color: #10b981;
  line-height: 1.4;
}

.warning-box {
  padding: 0.5rem 0.8rem;
  background: rgba(245, 158, 11, 0.1);
  border-left: 3px solid #f59e0b;
  border-radius: 4px;
  font-size: 0.85rem;
  color: var(--warning-color);
}

.confirm-btn-row {
  display: flex;
  justify-content: flex-end;
}

.range-error-box {
  color: var(--danger-color);
  font-size: 0.88rem;
}

.badge {
  display: inline-block;
  padding: 0.15rem 0.45rem;
  font-size: 0.75rem;
  font-weight: 500;
  border-radius: 4px;
}

.badge-success {
  background: rgba(16, 185, 129, 0.15);
  color: #10b981;
}

.badge-warning {
  background: rgba(245, 158, 11, 0.15);
  color: var(--warning-color);
}

.badge-error {
  background: rgba(239, 68, 68, 0.15);
  color: var(--danger-color);
}

.btn-group-row {
  display: flex;
  flex-wrap: wrap;
  gap: 0.65rem;
  justify-content: flex-end;
  align-items: center;
}
</style>
