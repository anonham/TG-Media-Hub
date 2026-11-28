<template>
  <div class="view-container">
    <div class="card glass-card">
      <div class="card-header">
        <div class="card-title">📤 本地媒体发布到 Telegram 频道</div>
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
          <label class="form-label">本地媒体文件夹绝对路径:</label>
          <div class="input-row">
            <n-input
              v-model:value="folderPath"
              placeholder="例如: C:\Downloads\ChannelFolder 或 /path/to/folder"
              class="flex-1"
            />
            <n-button secondary type="info" :loading="inspecting" :disabled="!folderPath.trim()" @click="handleInspect">
              🔍 文件夹测试与媒体过滤
            </n-button>
          </div>
        </div>

        <!-- 深度测试与媒体分类结果卡片 -->
        <div v-if="inspectResult" class="inspect-card">
          <div class="inspect-header-row">
            <span class="inspect-main-title">📊 <b>文件夹媒体测试报告</b></span>
            <div class="inspect-badges">
              <span class="badge" :class="inspectResult.account_premium ? 'badge-success' : 'badge-warning'">
                {{
                  inspectResult.account_premium
                    ? `👑 Premium ✅ 上限 ${inspectResult.effective_limit_mb} MiB`
                    : `👤 非 Premium · 上限 ${inspectResult.effective_limit_mb} MiB`
                }}
              </span>
              <span
                v-if="inspectResult.target_channel_info"
                class="badge"
                :class="inspectResult.target_channel_info.has_write_permission ? 'badge-success' : 'badge-error'"
              >
                {{
                  inspectResult.target_channel_info.has_write_permission
                    ? `🟢 频道发帖权限: 正常 (${inspectResult.target_channel_info.permission_summary})`
                    : `🔴 无发帖权限: ${inspectResult.target_channel_info.permission_summary}`
                }}
              </span>
              <span class="badge badge-success">✅ 有效媒体: {{ inspectResult.valid_files_count }} 个</span>
              <span v-if="inspectResult.already_published_count > 0" class="badge badge-info"
                >⏩ 历史已发: {{ inspectResult.already_published_count }} 个</span
              >
              <span class="badge badge-warning">⏳ 待发布: {{ pendingPublishCount }} 个</span>
              <span v-if="inspectResult.ignored_files_count > 0" class="badge badge-warning"
                >🚫 过滤非媒体: {{ inspectResult.ignored_files_count }} 个</span
              >
            </div>
          </div>

          <!-- 分类统计网格卡片 -->
          <div class="category-grid">
            <div class="category-card">
              <div class="cat-icon">📸</div>
              <div class="cat-content">
                <div class="cat-name">图片相册</div>
                <div class="cat-val">
                  <b>{{ inspectResult.images?.count || 0 }}</b> 张
                </div>
                <div class="cat-sub">{{ inspectResult.images?.size_mb || 0 }} MiB</div>
              </div>
            </div>

            <div class="category-card">
              <div class="cat-icon">🎬</div>
              <div class="cat-content">
                <div class="cat-name">视频文件</div>
                <div class="cat-val">
                  <b>{{ inspectResult.videos?.count || 0 }}</b> 个
                </div>
                <div class="cat-sub">{{ inspectResult.videos?.size_mb || 0 }} MiB</div>
              </div>
            </div>

            <div class="category-card">
              <div class="cat-icon">🎵</div>
              <div class="cat-content">
                <div class="cat-name">原生音频</div>
                <div class="cat-val">
                  <b>{{ inspectResult.audios?.count || 0 }}</b> 首
                </div>
                <div class="cat-sub">{{ inspectResult.audios?.size_mb || 0 }} MiB</div>
              </div>
            </div>

            <div class="category-card">
              <div class="cat-icon">📦</div>
              <div class="cat-content">
                <div class="cat-name">媒体总容量</div>
                <div class="cat-val">
                  <b>{{ inspectResult.total_media_size_gb || 0 }}</b> GiB
                </div>
                <div class="cat-sub">共 {{ inspectResult.total_media_files || 0 }} 个媒体</div>
              </div>
            </div>
          </div>

          <div class="audio-tip-banner">
            🎵 <b>Telegram 原生播放支持</b>：音频文件 (.mp3, .m4a, .flac, .ogg, .wav 等)
            将自动注入播放元数据，在频道中提供可流式拖动进度条的原生播放器控件。
          </div>

          <!-- 若存在超限文件 (按账号动态上限判定) -->
          <details v-if="unavailableFileCount > 0" class="oversized-alert unavailable-files">
            <summary>有 {{ unavailableFileCount }} 个文件无法读取，已从本次巡检跳过</summary>
            <ul v-if="inspectResult.unavailable_files?.length">
              <li v-for="item in inspectResult.unavailable_files" :key="item.rel_path">
                <strong>{{ item.file_name }}</strong>
                <div>{{ item.rel_path }}</div>
                <div>{{ item.reason }}</div>
              </li>
            </ul>
            <p v-if="inspectFoundNoReadableMedia">请修复文件权限或路径后重新巡检。</p>
          </details>

          <div v-if="inspectResult.oversized_files && inspectResult.oversized_files.length > 0" class="oversized-alert">
            <div class="oversized-title">
              ⚠️
              <b
                >检测到 {{ inspectResult.oversized_files.length }} 个超限文件 (超过当前账号上限
                {{ inspectResult.effective_limit_mb }} MiB /
                {{ ((inspectResult.effective_limit_mb ?? 0) / 1024).toFixed(2) }} GiB)</b
              >:
            </div>
            <div class="oversized-desc">发布时将<b>自动拦截并跳过</b>以下文件，并在任务控制台和频道总结中标记：</div>
            <div v-if="premiumFixableCount > 0" class="oversized-hint">
              👑 开通 Telegram Premium（上限提升至 {{ PREMIUM_LIMIT_LABEL }}）后，下方 {{ premiumFixableCount }} 个「非
              Premium 上限」文件即可正常上传。
            </div>
            <div class="oversized-list">
              <div v-for="item in inspectResult.oversized_files" :key="item.rel_path" class="oversized-item">
                <span class="oversized-file-name">📄 {{ item.file_name }}</span>
                <span class="oversized-file-badge"
                  >{{ item.file_size_gb }} GiB (超出 +{{ item.exceeded_mb }} MiB) ·
                  {{ oversizeKindLabel(item.oversize_kind) }}</span
                >
              </div>
            </div>
          </div>

          <!-- 媒体文件清单预览表格与过滤 Tab -->
          <div class="preview-table-container">
            <div class="preview-tabs-row">
              <n-radio-group v-model:value="previewTab" size="small">
                <n-radio-button value="all">全部有效媒体 ({{ inspectResult.media_files?.length || 0 }})</n-radio-button>
                <n-radio-button value="image">📸 图片 ({{ inspectResult.images?.count || 0 }})</n-radio-button>
                <n-radio-button value="video">🎬 视频 ({{ inspectResult.videos?.count || 0 }})</n-radio-button>
                <n-radio-button value="audio">🎵 音频 ({{ inspectResult.audios?.count || 0 }})</n-radio-button>
                <n-radio-button value="ignored" v-if="inspectResult.ignored_files_count > 0"
                  >🚫 过滤非媒体 ({{ inspectResult.ignored_files_count }})</n-radio-button
                >
              </n-radio-group>
            </div>

            <!-- 媒体列表 -->
            <div v-if="previewTab !== 'ignored'" class="media-table-wrapper">
              <table class="cockpit-table">
                <thead>
                  <tr>
                    <th>文件名称</th>
                    <th>相对路径</th>
                    <th>类型</th>
                    <th>大小</th>
                    <th>发布状态</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="item in displayedMediaFiles" :key="item.rel_path">
                    <td class="file-cell">
                      <span class="type-icon">{{
                        item.media_type === 'image' ? '📸' : item.media_type === 'video' ? '🎬' : '🎵'
                      }}</span>
                      <span class="file-name-text">{{ item.file_name }}</span>
                    </td>
                    <td class="rel-cell font-mono">{{ item.rel_path }}</td>
                    <td>
                      <span
                        class="badge"
                        :class="
                          item.media_type === 'image'
                            ? 'badge-primary'
                            : item.media_type === 'video'
                              ? 'badge-info'
                              : 'badge-audio'
                        "
                      >
                        {{ item.media_type === 'image' ? '图片' : item.media_type === 'video' ? '视频' : '原生音频' }}
                      </span>
                    </td>
                    <td>{{ item.file_size_mb }} MiB</td>
                    <td>
                      <span v-if="item.is_oversized" class="badge badge-error">⚠️ 超限跳过</span>
                      <span v-else-if="item.is_already_published" class="badge badge-info">⏩ 历史已发</span>
                      <span v-else class="badge badge-warning">⏳ 待发布</span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>

            <!-- 过滤非媒体列表 -->
            <div v-else class="media-table-wrapper">
              <table class="cockpit-table">
                <thead>
                  <tr>
                    <th>非媒体文件名称</th>
                    <th>相对路径</th>
                    <th>大小</th>
                    <th>过滤原因</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="item in inspectResult.ignored_files" :key="item.rel_path">
                    <td class="file-cell">📄 {{ item.file_name }}</td>
                    <td class="rel-cell font-mono">{{ item.rel_path }}</td>
                    <td>{{ item.file_size_mb }} MiB</td>
                    <td>
                      <span class="badge badge-warning">{{ item.reason }}</span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <!-- 共享发布配置区 (目标频道/相册/强制重传/占位卡/并发与在途) —— PublishView 与 PikPakPublishView 单一来源 -->
        <PublishConfigSharedSection
          v-model:selected-target="selectedTarget"
          v-model:upload-concurrency="uploadConcurrency"
          v-model:upload-in-flight="uploadInFlight"
          v-model:send-placeholder-card="sendPlaceholderCard"
          v-model:album-mode="albumMode"
          v-model:force-republish="forceRepublish"
          variant="local"
        >
          <template #target-actions>
            <n-button
              secondary
              type="info"
              :loading="probingPermission"
              :disabled="!selectedTarget"
              @click="handleProbePermission"
            >
              🔍 权限预检
            </n-button>
            <n-button secondary type="primary" @click="showPresetModal = true"> ⚙️ 管理/新增预设 </n-button>
          </template>
        </PublishConfigSharedSection>

        <!-- 目标频道发帖权限体检报告卡片 (仿媒体检测卡片设计，替代模态弹窗) -->
        <div v-if="channelInspectResult" class="inspect-card channel-inspect-card">
          <div class="inspect-header-row">
            <div class="header-left">
              <span class="inspect-main-title">📡 <b>目标频道权限体检报告</b></span>
            </div>
            <div class="inspect-badges">
              <span class="badge badge-info">
                👤 执行账号: {{ channelInspectResult.account_name || currentAccountDisplayName }}
              </span>
              <span class="badge badge-primary">
                {{ channelTypeLabel(channelInspectResult.channel_type) }}
              </span>
              <span v-if="channelInspectResult.is_restricted" class="badge badge-warning"> 🛡️ 受限内容 </span>
              <span class="badge" :class="channelInspectResult.has_write_permission ? 'badge-success' : 'badge-error'">
                {{ channelInspectResult.has_write_permission ? '🟢 拥有发帖权限' : '🔴 无发帖权限' }}
              </span>
              <n-button text size="tiny" class="close-card-btn" @click="channelInspectResult = null"> ✕ 关闭 </n-button>
            </div>
          </div>

          <!-- 四宫格诊断信息 -->
          <div class="category-grid">
            <div class="category-card">
              <div class="cat-icon">📢</div>
              <div class="cat-content">
                <div class="cat-name">目标频道/群组</div>
                <div class="cat-val title-ellipsis" :title="channelInspectResult.channel_title">
                  <b>{{ channelInspectResult.channel_title || selectedTarget }}</b>
                </div>
                <div class="cat-sub font-mono">{{ channelInspectResult.parsed_chat_id || selectedTarget }}</div>
              </div>
            </div>

            <div class="category-card">
              <div class="cat-icon">👤</div>
              <div class="cat-content">
                <div class="cat-name">检测执行账号</div>
                <div class="cat-val">
                  <b>{{ channelInspectResult.account_name || currentAccountDisplayName }}</b>
                </div>
                <div class="cat-sub">
                  {{ selectedAccountId ? `指定绑定 ID #${selectedAccountId}` : '系统默认主账号' }}
                </div>
              </div>
            </div>

            <div class="category-card">
              <div class="cat-icon">{{ channelInspectResult.has_read_permission ? '👁️' : '🚫' }}</div>
              <div class="cat-content">
                <div class="cat-name">消息读取与访问</div>
                <div class="cat-val">
                  <span :class="channelInspectResult.has_read_permission ? 'text-success' : 'text-error'">
                    {{ channelInspectResult.has_read_permission ? '正常可读' : '无法访问' }}
                  </span>
                </div>
                <div class="cat-sub">
                  {{ channelInspectResult.has_read_permission ? '可检索历史消息' : '读取受阻/非成员' }}
                </div>
              </div>
            </div>

            <div class="category-card">
              <div class="cat-icon">{{ channelInspectResult.has_write_permission ? '✍️' : '🔒' }}</div>
              <div class="cat-content">
                <div class="cat-name">发帖写权限</div>
                <div class="cat-val">
                  <span :class="channelInspectResult.has_write_permission ? 'text-success' : 'text-error'">
                    {{ channelInspectResult.has_write_permission ? '具备权限' : '受限拦截' }}
                  </span>
                </div>
                <div class="cat-sub">{{ channelInspectPermissionNote }}</div>
              </div>
            </div>
          </div>

          <!-- 诊断指引与结论横幅 -->
          <div v-if="!channelInspectResult.has_write_permission" class="oversized-alert channel-perm-alert">
            <div class="oversized-title">
              🛑 <b>发帖权限不足警报</b>：账号 [{{ channelInspectResult.account_name || currentAccountDisplayName }}]
              对目标频道/群组缺少发帖权限！
            </div>
            <div class="oversized-desc">
              Telegram 诊断明细:
              <b>{{
                channelInspectResult.permission_summary ||
                channelInspectResult.error_detail ||
                '非频道管理员，无法发送消息或媒体'
              }}</b>
            </div>
            <div class="oversized-hint">
              👉 <b>建议解决方案</b>：<br />
              1. 若您有其他已是管理员的 Userbot 账号（如拥有者账号），请在上方<b>「指定执行 Userbot 账号」</b
              >下拉框中切换选择该账号；<br />
              2. 或在 Telegram 客户端中打开该频道设置，将当前账号添加为管理员并开启<b>「发布消息 (Post Messages)」</b
              >权限。
            </div>
          </div>

          <div v-else class="audio-tip-banner channel-perm-success">
            ✅ <b>权限体检全部通过</b>：账号 [{{ channelInspectResult.account_name || currentAccountDisplayName }}]
            拥有向频道 [{{ channelInspectResult.channel_title }}] 发布媒体与相册的完整写权限，可正常发布！
          </div>
        </div>

        <div class="checkbox-row policy-row">
          <span class="switch-label"><b>超限文件处理策略:</b></span>
          <!-- D3: 本地发布仅暴露 跳过并占位报告 / 严格中断; split 为 PikPak 流式虚拟分卷
               专属概念, 本地传输无分卷能力, 不暴露 (后端按 skip_and_report 执行) -->
          <n-select
            v-model:value="oversizePolicy"
            size="small"
            class="responsive-control-md"
            :options="LOCAL_OVERSIZE_POLICY_OPTIONS"
          />
        </div>

        <div>
          <label class="form-label">发布风控延迟 (秒/次):</label>
          <n-input-number
            :value="delaySeconds"
            @update:value="(v) => (delaySeconds = v ?? 3.0)"
            :min="0.5"
            :step="0.5"
            placeholder="3.0"
            style="width: 100%"
          />
        </div>

        <div class="btn-wrapper">
          <n-button
            type="primary"
            size="medium"
            :loading="submitting"
            :disabled="!folderPath.trim() || !selectedTarget || inspectFoundNoReadableMedia"
            @click="handleSubmit"
          >
            <template #icon>🚀</template>
            开始批量上传发布
          </n-button>
        </div>
      </div>
    </div>

    <!-- 预设管理 Modal -->
    <PresetManageModal v-model:show="showPresetModal" default-type="target" @saved="onPresetSaved" />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useRouter } from 'vue-router'
import { NInput, NSelect, NInputNumber, NButton, NRadioGroup, NRadioButton, NSlider } from 'naive-ui'
import { useTaskStore } from '@/stores/taskStore'
import { useAccountStore } from '@/stores/accountStore'
import { usePresetStore } from '@/stores/presetStore'
import { dialog, message } from '@/utils/discreteApi'
import { LOCAL_OVERSIZE_POLICY_OPTIONS, PREMIUM_LIMIT_LABEL, oversizeKindLabel } from '@/utils/publishConstants'
import type { InspectFolderResponse, MediaItemInfo, OversizedFileInfo } from '@/api/gen'
import PresetManageModal from '@/components/presets/PresetManageModal.vue'
import PublishConfigSharedSection from '@/components/publish/PublishConfigSharedSection.vue'

const router = useRouter()
const taskStore = useTaskStore()
const accountStore = useAccountStore()
const presetStore = usePresetStore()

const folderPath = ref('')
const selectedTarget = ref<string | null>(null)
const albumMode = ref(true)
const forceRepublish = ref(false)
// D3: 本地超限策略选择器 (不暴露 split —— PikPak 流式专属, 本地映射为 skip)
const oversizePolicy = ref('skip_and_report')
// 占位卡开关 (缺集超限卡 + 失败卡共用总开关, 默认开启)
const sendPlaceholderCard = ref(true)
const delaySeconds = ref(3.0)
const uploadConcurrency = ref<number | null>(null)
const uploadInFlight = ref<number | null>(null)
const submitting = ref(false)

const selectedAccountId = ref<number | string | null>(null)
const taskPriority = ref<number>(10)
const probingPermission = ref(false)
const channelInspectResult = ref<any>(null)

const currentAccountDisplayName = computed(() => {
  if (selectedAccountId.value) {
    const acc = accountStore.accounts.find((a) => a.id === selectedAccountId.value)
    return acc ? acc.account_name : `账号 #${selectedAccountId.value}`
  }
  return '系统默认主账号'
})

const channelInspectPermissionNote = computed(() => {
  if (!channelInspectResult.value) return ''
  if (channelInspectResult.value.has_write_permission) {
    return channelInspectResult.value.permission_summary?.includes('拥有者') ? '拥有者最高发布权限' : '管理员发帖权限'
  }
  return '非管理员无法发帖'
})

function channelTypeLabel(type: string): string {
  if (type === 'Channel') return '📢 广播频道'
  if (type === 'Supergroup') return '👥 超级群组'
  if (type === 'Group') return '👥 普通群组'
  if (type === 'PrivateChat') return '💬 私聊会话'
  return type || '频道/群组'
}

// 统一下拉 Getter: Userbot 选项含系统自动调度项, 预设选项由 presetStore 标准化提供
const userbotOptions = computed(() => accountStore.userbotSelectOptions)

const inspecting = ref(false)
const inspectResult = ref<InspectFolderResponse | null>(null)
type FolderInspectionContext = { folder: string; target: string | null; accountId: number | null }
type FolderInspectionRun = {
  context: FolderInspectionContext
  promise: Promise<InspectFolderResponse | null>
}
const inspectResultContext = ref<FolderInspectionContext | null>(null)
const pendingFolderInspections = new Map<string, FolderInspectionRun>()
const previewTab = ref('all')

function sameInspectionContext(a: FolderInspectionContext | null, b: FolderInspectionContext): boolean {
  return a !== null && a.folder === b.folder && a.target === b.target && a.accountId === b.accountId
}

function inspectionContextKey(context: FolderInspectionContext): string {
  return JSON.stringify([context.folder, context.target, context.accountId])
}

const unavailableFileCount = computed(
  () => inspectResult.value?.unavailable_files_count ?? inspectResult.value?.unavailable_files?.length ?? 0,
)
function inspectionFoundNoReadableMedia(context: FolderInspectionContext): boolean {
  const result = inspectResult.value
  return sameInspectionContext(inspectResultContext.value, context) && folderInspectionHasNoReadableMedia(result)
}

function folderInspectionHasNoReadableMedia(result: InspectFolderResponse | null): boolean {
  const unavailableCount = result?.unavailable_files_count ?? result?.unavailable_files?.length ?? 0
  return unavailableCount > 0 && result?.valid_files_count === 0 && !result.oversized_files?.length
}

const inspectFoundNoReadableMedia = computed(() =>
  inspectionFoundNoReadableMedia({
    folder: folderPath.value.trim(),
    target: selectedTarget.value,
    accountId: selectedAccountId.value ? Number(selectedAccountId.value) : null,
  }),
)

const showPresetModal = ref(false)

const displayedMediaFiles = computed<MediaItemInfo[]>(() => {
  if (!inspectResult.value?.media_files) return []
  if (previewTab.value === 'all') return inspectResult.value.media_files
  return inspectResult.value.media_files.filter((m: MediaItemInfo) => m.media_type === previewTab.value)
})

// 超限文件中「开会员即可解」的数量 (oversize_kind 由后端按动态上限分类)
const premiumFixableCount = computed(
  () =>
    (inspectResult.value?.oversized_files || []).filter(
      (f: OversizedFileInfo) => f.oversize_kind === 'non_premium_limit',
    ).length,
)

// oversizeKindLabel 已收敛至 utils/publishConstants.ts

// 待发布有效文件数量 (由后端接口或前端计算兜底)
const pendingPublishCount = computed(() => {
  if (inspectResult.value?.pending_publish_count !== undefined) {
    return inspectResult.value.pending_publish_count
  }
  const totalValid = inspectResult.value?.valid_files_count || 0
  const already = inspectResult.value?.already_published_count || 0
  return Math.max(0, totalValid - already)
})

let permissionProbeGen = 0
let permissionProbeCallId = 0
let folderInspectGen = 0
let folderInspectCallId = 0

watch(folderPath, () => {
  folderInspectGen += 1
  inspectResult.value = null
  inspectResultContext.value = null
})

watch(selectedTarget, () => {
  permissionProbeGen += 1
  channelInspectResult.value = null
  if (folderPath.value.trim() && inspectResult.value) {
    handleInspect()
  }
})

watch(selectedAccountId, () => {
  permissionProbeGen += 1
  channelInspectResult.value = null
  if (folderPath.value.trim() && inspectResult.value) {
    handleInspect()
  }
})

onMounted(() => {
  accountStore.loadAccounts()
})

function onPresetSaved(preset: { presetType?: string; channelId?: string }) {
  if (preset.presetType === 'target' && preset.channelId) {
    selectedTarget.value = preset.channelId
  }
}

function showSubmitSuccessModal(taskId: string | number) {
  dialog.success({
    title: '任务已成功加入调度队列',
    content: `本地媒体发布任务 ID: #${taskId} 已创建，系统调度器已接管并按优先级 P${taskPriority.value} 排队运行。`,
    positiveText: '进入任务中心查看详情',
    negativeText: '继续发布',
    onPositiveClick: () => {
      router.push(`/tasks?id=${taskId}`)
    },
  })
}

async function handleProbePermission() {
  const reqTarget = selectedTarget.value
  if (!reqTarget) return
  const reqAccountId = selectedAccountId.value ? Number(selectedAccountId.value) : undefined
  const gen = ++permissionProbeGen
  const callId = ++permissionProbeCallId
  probingPermission.value = true
  try {
    const res = await presetStore.inspectPresetChannel(reqTarget, 'target', reqAccountId)
    const currentAccountId = selectedAccountId.value ? Number(selectedAccountId.value) : undefined
    if (gen !== permissionProbeGen || selectedTarget.value !== reqTarget || currentAccountId !== reqAccountId) {
      return
    }
    if (res) {
      channelInspectResult.value = res
      if (res.has_write_permission) {
        message.success(`✅ 权限预检通过: ${res.channel_title || reqTarget}`)
      } else {
        message.warning(`⚠️ 权限警告: ${res.permission_summary || '无发帖权限，请查看体检报告'}`)
      }
    }
  } finally {
    if (callId === permissionProbeCallId) {
      probingPermission.value = false
    }
  }
}

async function handleInspect() {
  const reqFolder = folderPath.value.trim()
  if (!reqFolder) return
  const reqTarget = selectedTarget.value
  const reqAccountId = selectedAccountId.value ? Number(selectedAccountId.value) : null
  const context = { folder: reqFolder, target: reqTarget, accountId: reqAccountId }
  const contextKey = inspectionContextKey(context)
  const gen = ++folderInspectGen
  const callId = ++folderInspectCallId
  inspecting.value = true

  const inspection = (async () => {
    const res = await taskStore.inspectFolder(reqFolder, reqTarget, reqAccountId)
    const currentAccountId = selectedAccountId.value ? Number(selectedAccountId.value) : null
    if (
      gen !== folderInspectGen ||
      folderPath.value.trim() !== reqFolder ||
      selectedTarget.value !== reqTarget ||
      currentAccountId !== reqAccountId
    ) {
      return res ?? null
    }
    inspectResult.value = res ?? null
    inspectResultContext.value = res ? context : null
    return res ?? null
  })()
  const run = { context, promise: inspection }
  pendingFolderInspections.set(contextKey, run)
  try {
    await inspection
  } finally {
    if (pendingFolderInspections.get(contextKey) === run) {
      pendingFolderInspections.delete(contextKey)
    }
    if (callId === folderInspectCallId) {
      inspecting.value = false
    }
  }
}

async function handleSubmit() {
  if (submitting.value) return
  const reqFolder = folderPath.value.trim()
  const reqTarget = selectedTarget.value
  if (!reqFolder || !reqTarget) return
  const inspectionContext = {
    folder: reqFolder,
    target: reqTarget,
    accountId: selectedAccountId.value ? Number(selectedAccountId.value) : null,
  }
  if (inspectionFoundNoReadableMedia(inspectionContext)) return
  const inspectionKey = inspectionContextKey(inspectionContext)
  const inspectionAtSubmit = pendingFolderInspections.get(inspectionKey) ?? null
  const resultAtSubmit = sameInspectionContext(inspectResultContext.value, inspectionContext)
    ? inspectResult.value
    : null

  // 在权限检查 await 前冻结提交快照，确保通过预检的目标与账号即最终提交目标
  const snapshot = {
    folder: reqFolder,
    folderInput: folderPath.value,
    uploadConcurrency: uploadConcurrency.value,
    uploadInFlight: uploadInFlight.value,
    target: reqTarget,
    albumMode: albumMode.value,
    delaySeconds: delaySeconds.value,
    accountId: selectedAccountId.value ? Number(selectedAccountId.value) : null,
    priority: taskPriority.value ?? 10,
    effectiveUploadConcurrency: uploadConcurrency.value
      ? Math.max(1, Math.min(Math.round(uploadConcurrency.value), 16))
      : null,
    effectiveUploadInFlight: uploadInFlight.value ? Math.max(1, Math.min(Math.round(uploadInFlight.value), 4)) : null,
    forceRepublish: forceRepublish.value,
    oversizePolicy: oversizePolicy.value,
    sendPlaceholderCard: sendPlaceholderCard.value,
  }

  submitting.value = true
  try {
    // Pre-submit Guard: 提交前快速写权限校验拦截 (核心防呆，展开体检卡片，绝无弹窗打扰)
    try {
      const gen = ++permissionProbeGen
      const permCheck = await presetStore.inspectPresetChannel(
        snapshot.target,
        'target',
        snapshot.accountId ?? undefined,
      )
      if (permCheck) {
        const currentAccountId = selectedAccountId.value ? Number(selectedAccountId.value) : null
        if (
          gen === permissionProbeGen &&
          selectedTarget.value === snapshot.target &&
          currentAccountId === snapshot.accountId
        ) {
          channelInspectResult.value = permCheck
        }
        if (permCheck.has_write_permission === false) {
          message.warning('🛑 目标频道缺少发帖权限，已为您展开权限体检报告，请按指引处理')
          return
        }
      }
    } catch (err) {
      console.warn('提交前权限检测异常，跳过硬阻断', err)
    }

    // Keep the inspection tied to this submission even if the user edits the form during the permission check.
    const matchingInspection = inspectionAtSubmit ?? pendingFolderInspections.get(inspectionKey)
    const inspectionResultForSubmit = matchingInspection ? await matchingInspection.promise : resultAtSubmit
    if (folderInspectionHasNoReadableMedia(inspectionResultForSubmit)) {
      message.warning('该文件夹没有可读取的媒体文件，请修复路径或权限后重新巡检')
      return
    }

    const res = await taskStore.submitFolderPublish(
      snapshot.folder,
      snapshot.target,
      snapshot.albumMode,
      snapshot.delaySeconds,
      snapshot.accountId,
      snapshot.priority,
      snapshot.effectiveUploadConcurrency,
      snapshot.effectiveUploadInFlight,
      snapshot.forceRepublish,
      snapshot.oversizePolicy,
      snapshot.sendPlaceholderCard,
    )
    if (res?.success && res.taskId) {
      if (folderPath.value === snapshot.folderInput) {
        folderPath.value = ''
        inspectResult.value = null
        inspectResultContext.value = null
      }
      if (uploadConcurrency.value === snapshot.uploadConcurrency) uploadConcurrency.value = null
      if (uploadInFlight.value === snapshot.uploadInFlight) uploadInFlight.value = null
      showSubmitSuccessModal(res.taskId)
    }
  } finally {
    submitting.value = false
  }
}
</script>

<style scoped>
.unavailable-files {
  overflow-wrap: anywhere;
}

.unavailable-files summary {
  cursor: pointer;
  font-weight: 600;
}

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

.select-row,
.input-row {
  display: flex;
  flex-wrap: wrap;
  gap: 0.65rem;
}

.flex-1 {
  flex: 1 1 240px;
  min-width: 0;
}

.policy-row {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.5rem 0.75rem;
}

.w-half {
  width: 50%;
}

.grid-2col {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1rem;
}

.grid-3col {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 1rem;
}

@media (max-width: 900px) {
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

.inspect-card {
  background: var(--bg-card-hover);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 1.2rem;
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.inspect-header-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.5rem;
}

.inspect-main-title {
  font-size: 1rem;
}

.category-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 0.75rem;
}

.category-card {
  background: var(--bg-card);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  padding: 0.75rem 1rem;
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.cat-icon {
  font-size: 1.6rem;
}

.cat-name {
  font-size: 0.82rem;
  color: var(--text-secondary);
}

.cat-val {
  font-size: 1.05rem;
  color: var(--text-primary);
}

.cat-sub {
  font-size: 0.75rem;
  color: var(--text-secondary);
}

.audio-tip-banner {
  padding: 0.5rem 0.8rem;
  background: rgba(14, 165, 233, 0.1);
  border-left: 3px solid var(--info-color);
  border-radius: 4px;
  font-size: 0.85rem;
  color: var(--info-color);
}

.oversized-alert {
  background: rgba(239, 68, 68, 0.1);
  border: 1px solid rgba(239, 68, 68, 0.3);
  border-radius: 6px;
  padding: 0.8rem 1rem;
  font-size: 0.88rem;
}

.oversized-list {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
  margin-top: 0.5rem;
}

.oversized-hint {
  margin-top: 0.5rem;
  color: var(--warning-color);
}

.oversized-item {
  display: flex;
  justify-content: space-between;
  background: var(--bg-card);
  padding: 0.3rem 0.6rem;
  border-radius: 4px;
}

.preview-table-container {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  margin-top: 0.5rem;
}

.preview-tabs-row {
  display: flex;
}

.media-table-wrapper {
  max-height: 300px;
  overflow-y: auto;
  border: 1px solid var(--border-color);
  border-radius: 6px;
}

.media-table-wrapper :deep(.cockpit-table) {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.85rem;
  text-align: left;
}

.media-table-wrapper :deep(.cockpit-table th) {
  background: var(--bg-card-hover);
  padding: 0.6rem 0.8rem;
  color: var(--text-secondary);
  font-weight: 500;
  position: sticky;
  top: 0;
  z-index: 1;
}

.media-table-wrapper :deep(.cockpit-table td) {
  padding: 0.5rem 0.8rem;
  border-bottom: 1px solid var(--border-color);
}

.file-cell {
  display: flex;
  align-items: center;
  gap: 0.4rem;
  max-width: 260px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.rel-cell {
  max-width: 240px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  color: var(--text-secondary);
}

.rel-cell {
  /* font-mono 由 main.css 全局提供 */
}

.checkbox-row {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.btn-wrapper {
  margin-top: 0.5rem;
  display: flex;
  justify-content: flex-end;
}

.badge {
  display: inline-block;
  padding: 0.15rem 0.45rem;
  font-size: 0.75rem;
  font-weight: 500;
  border-radius: 4px;
}

.badge-primary {
  background: rgba(99, 102, 241, 0.15);
  color: var(--indigo-color);
}

.badge-info {
  background: rgba(14, 165, 233, 0.15);
  color: var(--info-color);
}

.badge-audio {
  background: rgba(236, 72, 153, 0.15);
  color: var(--pink-color);
}

.badge-success {
  background: rgba(16, 185, 129, 0.15);
  color: var(--success-color);
}

.badge-warning {
  background: rgba(245, 158, 11, 0.15);
  color: var(--warning-color);
}

.badge-error {
  background: rgba(239, 68, 68, 0.15);
  color: var(--danger-color);
}

.channel-inspect-card {
  margin-top: 0.25rem;
  border-left: 3px solid var(--info-color);
  animation: fadeIn 0.2s ease-in-out;
}

.channel-perm-alert {
  margin-top: 0.25rem;
}

.channel-perm-success {
  margin-top: 0.25rem;
  background: rgba(16, 185, 129, 0.1);
  border-left: 3px solid var(--success-color);
  color: var(--success-color);
}

.text-success {
  color: var(--success-color);
  font-weight: 600;
}

.text-error {
  color: var(--danger-color);
  font-weight: 600;
}

.title-ellipsis {
  max-width: 180px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.close-card-btn {
  margin-left: 0.5rem;
  color: var(--text-secondary);
  cursor: pointer;
}

.close-card-btn:hover {
  color: var(--text-primary);
}

@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(-4px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

@media (max-width: 900px) {
  .category-grid {
    grid-template-columns: repeat(2, 1fr);
  }
}
</style>
