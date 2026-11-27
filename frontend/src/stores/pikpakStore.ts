import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type {
  PikPakAuthStatusResponse,
  PikPakFileItem,
  PikPakFileListResponse,
  PikPakInspectResponse,
  PikPakPublishRequest,
} from '@/api/gen'
import { pikpakApi } from '@/api'
import { useTaskStore } from '@/stores/taskStore'
import { message } from '@/utils/discreteApi'
import { errDetail } from '@/utils/errors'

export type PikPakAuthStatus = PikPakAuthStatusResponse & {
  fetchError?: string
}

export interface PikPakDeviceFlow {
  phase: 'idle' | 'polling' | 'success' | 'failed'
  deviceCode: string
  userCode: string
  verificationUri: string
  interval: number
  expiresAt: number
  lastStatus: string
  errorMsg: string
}

export interface FolderCacheEntry {
  items: PikPakFileItem[]
  nextPageToken: string | null
  loading: boolean
  loaded: boolean
  loadedAt?: number
  error: string | null
}

export interface DirSelectionRule {
  kind: 'dir'
  dirId: string
  dirName: string
  dirPath: string
  leaves: Map<string, PikPakFileItem>
  dirTree: Record<string, string>
  loading: boolean
  foundCount: number
  aborted: boolean
  /** 遍历触及深度/文件数上限被截断 —— 选区不完整, UI 需明示 */
  truncated?: boolean
}

export interface FileSelectionRule {
  kind: 'file'
  fileId: string
  item: PikPakFileItem
}

export type SelectionRule = DirSelectionRule | FileSelectionRule

export interface DirGroupItem {
  rule: DirSelectionRule
  files: Map<string, PikPakFileItem>
}

export interface SelectionViewResult {
  selected: Map<string, PikPakFileItem>
  dirGroups: DirGroupItem[]
  singles: PikPakFileItem[]
  isFolderPublish: boolean
  folderPublishName: string | null
  combinedDirTree: Record<string, string>
}

export interface SelectionTreeNode {
  dirId: string
  dirName: string
  loading: boolean
  directLeaves: Array<{ fid: string; name: string; selected: boolean; item: PikPakFileItem }>
  children: SelectionTreeNode[]
  allLeaves: Array<{ fid: string; name: string; selected: boolean; item: PikPakFileItem }>
  selectedCount: number
  checkState: 'checked' | 'indeterminate' | 'unchecked'
}

export const usePikpakStore = defineStore('pikpak', () => {
  // ── 鉴权状态 ─────────────────────────────────────────────
  const authStatus = ref<PikPakAuthStatus | null>(null)
  const authStatusLoading = ref(false)

  async function fetchAuthStatus(force = false) {
    authStatusLoading.value = true
    try {
      const res = await pikpakApi.authStatus(force)
      authStatus.value = (res.data as PikPakAuthStatus) || null
    } catch (err: unknown) {
      authStatus.value = {
        logged_in: false,
        state: 'unreachable',
        state_reason: null,
        fetchError: errDetail(err, '连接异常'),
      }
    } finally {
      authStatusLoading.value = false
    }
  }

  // ── 设备码登录流 ──
  const deviceFlow = ref<PikPakDeviceFlow>({
    phase: 'idle',
    deviceCode: '',
    userCode: '',
    verificationUri: '',
    interval: 5,
    expiresAt: 0,
    lastStatus: '',
    errorMsg: '',
  })
  let devicePollTimer: ReturnType<typeof setTimeout> | null = null
  // 设备流代际: stop / 重新 start 即递增, 使在途 devicePoll/deviceStart 响应作废
  let deviceFlowGen = 0

  function clearPollTimer() {
    if (devicePollTimer) {
      clearTimeout(devicePollTimer)
      devicePollTimer = null
    }
  }

  function stopDeviceFlow() {
    // 代际递增使全部在途 devicePoll / deviceStart 响应作废
    deviceFlowGen += 1
    clearPollTimer()
    deviceFlow.value = {
      phase: 'idle',
      deviceCode: '',
      userCode: '',
      verificationUri: '',
      interval: 5,
      expiresAt: 0,
      lastStatus: '',
      errorMsg: '',
    }
  }

  function scheduleNextPoll(gen: number) {
    if (gen !== deviceFlowGen) return
    clearPollTimer()
    devicePollTimer = setTimeout(pollDeviceOnce, Math.max(1, deviceFlow.value.interval || 5) * 1000)
  }

  async function startDeviceLogin(): Promise<boolean> {
    stopDeviceFlow()
    const gen = ++deviceFlowGen
    try {
      const res = await pikpakApi.deviceStart()
      // deviceStart 在途期间被 stop / 新 start 取代 → 放弃
      if (gen !== deviceFlowGen) return false
      const data = (res.data || {}) as {
        device_code?: string
        user_code?: string
        verification_uri?: string
        interval?: number
        expires_in?: number
      }
      deviceFlow.value = {
        phase: 'polling',
        deviceCode: data.device_code || '',
        userCode: data.user_code || '',
        verificationUri: data.verification_uri || '',
        interval: data.interval || 5,
        expiresAt: data.expires_in ? Date.now() + data.expires_in * 1000 : 0,
        lastStatus: '',
        errorMsg: '',
      }
      scheduleNextPoll(gen)
      return true
    } catch (err: unknown) {
      if (gen !== deviceFlowGen) return false
      deviceFlow.value = {
        ...deviceFlow.value,
        phase: 'failed',
        errorMsg: errDetail(err, '设备码获取失败'),
      }
      return false
    }
  }

  async function pollDeviceOnce() {
    const flow = deviceFlow.value
    if (flow.phase !== 'polling' || !flow.deviceCode) return
    const gen = deviceFlowGen
    try {
      const res = await pikpakApi.devicePoll({
        device_code: flow.deviceCode,
        interval_s: Math.min(30, Math.max(0.5, flow.interval || 5)),
        max_wait_s: 30,
      })
      // 响应晚于 stopDeviceFlow / 新的 startDeviceLogin → 整体作废,
      // 不得复活旧流程或重启轮询
      if (gen !== deviceFlowGen) return
      const data = (res.data || {}) as { logged_in?: boolean; status?: string; timed_out?: boolean }
      if (data.logged_in) {
        deviceFlow.value = { ...flow, phase: 'success', lastStatus: data.status || 'success' }
        message.success('PikPak 账号绑定成功')
        await fetchAuthStatus(true)
        return
      }
      if (data.timed_out) {
        if (!flow.expiresAt || Date.now() < flow.expiresAt) {
          scheduleNextPoll(gen)
          return
        }
        deviceFlow.value = { ...flow, phase: 'failed', errorMsg: '设备码已失效，请重新发起授权' }
        return
      }
      deviceFlow.value = { ...flow, phase: 'failed', lastStatus: data.status || '' }
    } catch (err: unknown) {
      if (gen !== deviceFlowGen) return
      deviceFlow.value = {
        ...deviceFlow.value,
        phase: 'failed',
        errorMsg: errDetail(err, '授权状态查询失败'),
      }
    }
  }

  // ── 目录树懒加载缓存 ──
  const FOLDER_CACHE_TTL_MS = 45_000
  const folderCache = ref<Record<string, FolderCacheEntry>>({})
  let folderCacheGeneration = 0

  function cacheEntry(folderId?: string | null): FolderCacheEntry | null {
    return folderCache.value[folderId || ''] || null
  }

  // undefined marks generation invalidation; null retains loading/error semantics.
  async function fetchFolder(
    folderId?: string | null,
    { append = false, force = false } = {},
  ): Promise<FolderCacheEntry | null | undefined> {
    const key = folderId || ''
    const existing = folderCache.value[key]
    if (existing?.loading) return null
    const cacheFresh = existing?.loaded && Date.now() - (existing.loadedAt || 0) < FOLDER_CACHE_TTL_MS
    if (existing && !append && !force && cacheFresh) return existing
    const generation = folderCacheGeneration
    const pageToken = append ? existing?.nextPageToken || null : null
    folderCache.value = {
      ...folderCache.value,
      [key]: {
        items: existing?.items || [],
        nextPageToken: existing?.nextPageToken || null,
        loading: true,
        loaded: existing?.loaded || false,
        error: null,
      },
    }
    try {
      const res = await pikpakApi.fileList(key, pageToken, force)
      if (generation !== folderCacheGeneration) return undefined
      const data = (res.data || {}) as PikPakFileListResponse
      const newItems = Array.isArray(data.items) ? data.items : []
      const entry: FolderCacheEntry = {
        items: append ? [...(existing?.items || []), ...newItems] : newItems,
        nextPageToken: data.next_page_token || null,
        loading: false,
        loaded: true,
        loadedAt: Date.now(),
        error: null,
      }
      folderCache.value = { ...folderCache.value, [key]: entry }
      return entry
    } catch (err: unknown) {
      if (generation !== folderCacheGeneration) return undefined
      folderCache.value = {
        ...folderCache.value,
        [key]: {
          items: existing?.items || [],
          nextPageToken: existing?.nextPageToken || null,
          loading: false,
          loaded: existing?.loaded || false,
          error: errDetail(err, '目录加载失败'),
        },
      }
      const e = err as { response?: { status?: number } }
      const status = e.response?.status
      if ((status === 424 || status === 502) && !authStatusLoading.value) {
        fetchAuthStatus(true)
      }
      return null
    }
  }

  function clearCache(keepFolderId: string | null = null) {
    // null = 全量清空 (含根目录) —— 旧实现把缺省 null 归一成 '' 而
    // 保留根目录, 任务终态回调用此形态失效远端目录状态时根条目 45s TTL 内陈旧
    if (keepFolderId === null) {
      folderCacheGeneration += 1
      folderCache.value = {}
      return
    }
    const keepKey = keepFolderId || ''
    folderCache.value = Object.fromEntries(Object.entries(folderCache.value).filter(([k]) => k === keepKey))
  }

  function loadMore(folderId: string | null) {
    return fetchFolder(folderId, { append: true })
  }

  // ── 选择规则层与单一派生已选清单 ──
  const publishSubmitting = ref(false)
  const selectionRules = ref<SelectionRule[]>([])
  const excludedFileIds = ref<Set<string>>(new Set())

  const MAX_WALK_FILES = 5000
  const MAX_WALK_DEPTH = 10

  function findDirRule(dirId: string): DirSelectionRule | null {
    return (selectionRules.value.find((r) => r.kind === 'dir' && r.dirId === dirId) as DirSelectionRule) || null
  }

  const selectionView = computed<SelectionViewResult>(() => {
    const excluded = excludedFileIds.value
    const dirRules = (selectionRules.value.filter((r) => r.kind === 'dir') as DirSelectionRule[]).sort(
      (a, b) => a.dirPath.split('/').length - b.dirPath.split('/').length,
    )
    const attribution = new Map<string, DirSelectionRule>()
    for (const rule of dirRules) {
      for (const fid of rule.leaves.keys()) attribution.set(fid, rule)
    }
    const selected = new Map<string, PikPakFileItem>()
    const dirGroups: DirGroupItem[] = []
    for (const rule of dirRules) {
      const files = new Map<string, PikPakFileItem>()
      for (const [fid, item] of rule.leaves) {
        if (attribution.get(fid) !== rule || excluded.has(fid)) continue
        files.set(fid, item)
        selected.set(fid, item)
      }
      if (files.size > 0) dirGroups.push({ rule, files })
    }
    const singles: PikPakFileItem[] = []
    for (const rule of selectionRules.value) {
      if (rule.kind !== 'file' || excluded.has(rule.fileId)) continue
      if (selected.has(rule.fileId)) continue
      selected.set(rule.fileId, rule.item)
      singles.push(rule.item)
    }
    const isFolderPublish = dirGroups.length > 0
    let folderPublishName: string | null = null
    if (dirGroups.length === 1) {
      folderPublishName = dirGroups[0].rule.dirName
    } else if (dirGroups.length > 1) {
      folderPublishName = `${dirGroups[0].rule.dirName} 等 ${dirGroups.length} 个目录`
    }
    const combinedDirTree: Record<string, string> = {}
    for (const group of dirGroups) {
      if (group.rule && group.rule.dirTree) {
        for (const [dirId, rel] of Object.entries(group.rule.dirTree)) {
          if (!(dirId in combinedDirTree)) combinedDirTree[dirId] = rel
        }
      }
    }
    return { selected, dirGroups, singles, isFolderPublish, folderPublishName, combinedDirTree }
  })

  const selectedFiles = computed<Map<string, PikPakFileItem>>(() => selectionView.value.selected)

  const selectedTotalBytes = computed(() => {
    let sum = 0
    for (const item of selectedFiles.value.values()) sum += item.size_bytes || 0
    return sum
  })

  function isSelected(fileId: string): boolean {
    return selectedFiles.value.has(fileId)
  }

  const selectionTree = computed<SelectionTreeNode[]>(() => {
    const dirRules = selectionRules.value.filter((r) => r.kind === 'dir') as DirSelectionRule[]
    const childrenOf = (rule: DirSelectionRule) => {
      const direct = dirRules.filter((r) => r !== rule && rule.dirTree && rule.dirTree[r.dirId])
      // 只保留直接子选中目录 —— 已被其他选中子目录包含的后代剔除,
      // 防嵌套选择 (A→B→C) 下 C 被渲染两次且文件计数翻倍
      return direct.filter((r) => !direct.some((o) => o !== r && o.dirTree && o.dirTree[r.dirId]))
    }
    const roots = dirRules.filter((r) => !dirRules.some((o) => o !== r && o.dirTree && o.dirTree[r.dirId]))
    const buildNode = (rule: DirSelectionRule): SelectionTreeNode => {
      const childRules = childrenOf(rule)
      const childFids = new Set<string>()
      for (const c of childRules) for (const fid of c.leaves.keys()) childFids.add(fid)
      const childRelSet = new Set(childRules.map((r) => rule.dirTree[r.dirId]))
      const directLeaves: Array<{ fid: string; name: string; selected: boolean; item: PikPakFileItem }> = []
      for (const [fid, item] of rule.leaves) {
        if (childFids.has(fid)) continue
        const parentRel = (rule.dirTree && item.parent_id && rule.dirTree[item.parent_id]) || rule.dirName
        if (childRelSet.has(parentRel)) continue
        directLeaves.push({ fid, name: item.name || '', selected: selectedFiles.value.has(fid), item })
      }
      const children = childRules.map((r) => buildNode(r))
      const allLeaves = [...directLeaves]
      for (const c of children) for (const l of c.allLeaves) allLeaves.push(l)
      const selectedCount = allLeaves.filter((l) => l.selected).length
      const checkState: 'checked' | 'indeterminate' | 'unchecked' =
        allLeaves.length === 0 || selectedCount === 0
          ? 'unchecked'
          : selectedCount === allLeaves.length
            ? 'checked'
            : 'indeterminate'
      return {
        dirId: rule.dirId,
        dirName: rule.dirName,
        loading: !!rule.loading,
        directLeaves,
        children,
        allLeaves,
        selectedCount,
        checkState,
      }
    }
    return roots.map(buildNode)
  })

  async function walkSubtree(
    dirItem: PikPakFileItem,
    rule: DirSelectionRule,
  ): Promise<{ leaves: Map<string, PikPakFileItem>; truncated: boolean }> {
    const leaves = new Map<string, PikPakFileItem>()
    const dirTree: Record<string, string> = { [dirItem.file_id]: dirItem.name || '' }
    const visitedFolderIds = new Set<string>()
    let truncated = false
    async function fetchWalkFolder(folderId: string, append = false) {
      let entry = await fetchFolder(folderId, { append })
      while (entry === undefined && !rule.aborted) {
        entry = await fetchFolder(folderId, { append })
      }
      return entry
    }
    async function crawl(folderId: string, currentPath: string, depth: number) {
      if (rule.aborted) return
      if (depth > MAX_WALK_DEPTH || leaves.size >= MAX_WALK_FILES) {
        // 显式标记截断 —— 不再把残缺遍历当作完整目录选区呈现
        truncated = true
        return
      }
      if (visitedFolderIds.has(folderId)) return
      visitedFolderIds.add(folderId)
      let entry = await fetchWalkFolder(folderId)
      while (entry && entry.nextPageToken) {
        entry = await fetchWalkFolder(folderId, true)
      }
      if (!entry) throw new Error('目录拉取失败')
      for (const item of entry.items || []) {
        if (rule.aborted) return
        if (item.is_dir) {
          const subPath = `${currentPath}/${item.name}`
          dirTree[item.file_id] = subPath
          await crawl(item.file_id, subPath, depth + 1)
        } else if (!leaves.has(item.file_id)) {
          // 限额在叶子入循环内逐个检查 —— 平铺大目录不再绕过入口检查
          if (leaves.size >= MAX_WALK_FILES) {
            truncated = true
            return
          }
          leaves.set(item.file_id, item)
          rule.foundCount = leaves.size
        }
      }
    }
    rule.dirTree = dirTree
    await crawl(dirItem.file_id, dirItem.name || '', 1)
    if (rule.aborted) throw new Error('aborted')
    rule.dirTree = dirTree
    return { leaves, truncated }
  }

  async function toggleDir(item: PikPakFileItem) {
    if (!item || !item.file_id) return
    const existing = findDirRule(item.file_id)
    if (existing) {
      if (existing.loading) {
        existing.aborted = true
        return
      }
      removeDirRule(item.file_id)
      return
    }
    const initialRule: DirSelectionRule = {
      kind: 'dir',
      dirId: item.file_id,
      dirName: item.name || '',
      dirPath: item.name || '',
      leaves: new Map(),
      dirTree: { [item.file_id]: item.name || '' },
      loading: true,
      foundCount: 0,
      aborted: false,
    }
    selectionRules.value = [...selectionRules.value, initialRule]
    // 通过 findDirRule 获取 Vue 响应式 Proxy，确保递归遍历与完成赋值全部触发响应式依赖通知
    const reactiveRule = findDirRule(item.file_id) || initialRule
    try {
      const { leaves, truncated } = await walkSubtree(item, reactiveRule)
      excludedFileIds.value = new Set([...excludedFileIds.value].filter((id) => !leaves.has(id)))
      reactiveRule.leaves = leaves
      reactiveRule.loading = false
      reactiveRule.truncated = truncated
      selectionRules.value = [...selectionRules.value]
      if (truncated) {
        // 截断必须显式上报, 不允许静默呈现残缺选区
        message.warning(
          `目录 ${item.name || item.file_id} 超过遍历上限 (深度 ${MAX_WALK_DEPTH} 层 / 文件 ${MAX_WALK_FILES} 个)，仅载入部分内容`,
        )
      }
    } catch (err: unknown) {
      selectionRules.value = selectionRules.value.filter((r) => !(r.kind === 'dir' && r.dirId === item.file_id))
      if (reactiveRule.aborted) {
        message.info(`已取消展开目录 ${item.name}`)
      } else {
        message.warning(`目录展开失败: ${item.name} (${errDetail(err)})`)
      }
    }
  }

  function removeDirRule(dirId: string) {
    selectionRules.value = selectionRules.value.filter((r) => !(r.kind === 'dir' && r.dirId === dirId))
  }

  function toggleFile(item: PikPakFileItem) {
    if (!item || !item.file_id || item.is_dir) return
    if (selectedFiles.value.has(item.file_id)) {
      const covered = selectionRules.value.some((r) => r.kind === 'dir' && r.leaves.has(item.file_id))
      if (covered) {
        excludedFileIds.value = new Set([...excludedFileIds.value, item.file_id])
      } else {
        selectionRules.value = selectionRules.value.filter((r) => !(r.kind === 'file' && r.fileId === item.file_id))
      }
    } else {
      excludedFileIds.value = new Set([...excludedFileIds.value].filter((id) => id !== item.file_id))
      if (!selectionRules.value.some((r) => r.kind === 'dir' && r.leaves.has(item.file_id))) {
        selectionRules.value = [...selectionRules.value, { kind: 'file', fileId: item.file_id, item }]
      }
    }
  }

  function dirCheckState(dirItem: PikPakFileItem) {
    const rule = findDirRule(dirItem.file_id)
    if (!rule) return { checked: false, indeterminate: false, loading: false }
    if (rule.loading) return { checked: false, indeterminate: false, loading: true }
    const group = selectionView.value.dirGroups.find((g) => g.rule === rule)
    const effective = group ? group.files.size : 0
    if (effective === 0) return { checked: false, indeterminate: rule.leaves.size > 0, loading: false }
    if (effective >= rule.leaves.size) return { checked: true, indeterminate: false, loading: false }
    return { checked: false, indeterminate: true, loading: false }
  }

  function pruneSelectionForFolder(folderId: string, freshItems: PikPakFileItem[] = []) {
    const freshIds = new Set((freshItems || []).map((i) => i.file_id))
    let pruned = 0
    for (const rule of selectionRules.value) {
      if (rule.kind === 'dir') {
        for (const [fid, item] of Array.from(rule.leaves.entries())) {
          if (item.parent_id === folderId && !freshIds.has(fid)) {
            rule.leaves.delete(fid)
            excludedFileIds.value.delete(fid)
            pruned++
          }
        }
      } else if (rule.item?.parent_id === folderId && !freshIds.has(rule.fileId)) {
        selectionRules.value = selectionRules.value.filter((r) => r !== rule)
        pruned++
      }
    }
    if (pruned > 0) {
      excludedFileIds.value = new Set(excludedFileIds.value)
      selectionRules.value = [...selectionRules.value]
    }
    return pruned
  }

  function clearSelection() {
    selectionRules.value = []
    excludedFileIds.value = new Set()
  }

  // ── 预检与发布 ───────────────────────────────────────────
  async function inspectSelection(
    targetChannel: string,
    accountId: number | null = null,
  ): Promise<PikPakInspectResponse | null> {
    if (!targetChannel) return null
    try {
      const payload: { target_channel: string; remote_files: PikPakFileItem[]; account_id?: number } = {
        target_channel: targetChannel,
        remote_files: [...selectedFiles.value.values()],
      }
      if (accountId !== null && accountId !== undefined) payload.account_id = accountId
      const res = await pikpakApi.inspect(payload)
      return res.data || null
    } catch (err: unknown) {
      message.error(`预检失败: ${errDetail(err)}`)
      return null
    }
  }

  async function publishPikpak(payload: PikPakPublishRequest): Promise<{ success: boolean; taskId?: string }> {
    if (publishSubmitting.value) return { success: false }
    publishSubmitting.value = true
    try {
      const res = await pikpakApi.publish(payload)
      const taskId = res.data?.task_id
      message.success(`PikPak 发布任务已创建: ${taskId}`)
      clearSelection()
      try {
        const taskStore = useTaskStore()
        await taskStore.loadTasks()
        // 走共享选择操作 (含日志流重置), 防旧任务水位毒害新任务日志
        if (taskId) {
          await taskStore.selectTask(taskId)
        } else {
          taskStore.selectedTaskId = null
        }
        taskStore.scheduleNextPoll()
      } catch (err: unknown) {
        console.debug('任务中心联动导航降级:', err)
      }
      return { success: true, taskId }
    } catch (err: unknown) {
      message.error(`提交失败: ${errDetail(err)}`)
      return { success: false }
    } finally {
      publishSubmitting.value = false
    }
  }

  return {
    authStatus,
    authStatusLoading,
    fetchAuthStatus,
    deviceFlow,
    startDeviceLogin,
    pollDeviceOnce,
    stopDeviceFlow,
    folderCache,
    cacheEntry,
    fetchFolder,
    loadMore,
    clearCache,
    selectedFiles,
    selectedTotalBytes,
    isSelected,
    selectionRules,
    selectionView,
    toggleDir,
    toggleFile,
    dirCheckState,
    selectionTree,
    removeDirRule,
    pruneSelectionForFolder,
    clearSelection,
    publishSubmitting,
    inspectSelection,
    publishPikpak,
  }
})
