// 领域调用面: 唯一允许组件/Store 触达的 HTTP 出口。
// 全部函数基于 openapi_snapshot.json 自动生成的强类型 SDK (./gen, 严禁手工编辑),
// 严禁在本文件之外手写 URL 或直接 import axios —— 生成器重跑时这里随之收敛。
import './client'
import { useAuthStore } from '@/stores/authStore'
import {
  listAccountsApiAccountsGet,
  createAccountApiAccountsPost,
  sendAccountCodeApiAccountsAccountIdSendCodePost,
  resendAccountCodeApiAccountsAccountIdResendCodePost,
  verifyAccountCodeApiAccountsAccountIdVerifyCodePost,
  submitAccountPasswordApiAccountsAccountIdPasswordPost,
  startAccountQrApiAccountsAccountIdQrStartPost,
  getAccountQrStatusApiAccountsAccountIdQrStatusGet,
  submitAccountQrPasswordApiAccountsAccountIdQrPasswordPost,
  cancelAccountChallengeApiAccountsAccountIdChallengeCancelPost,
  refreshAccountStatusApiAccountsAccountIdRefreshPost,
  updateAccountApiAccountsAccountIdPut,
  deleteAccountApiAccountsAccountIdDelete,
  listBotsApiBotsGet,
  createBotApiBotsPost,
  pingBotsApiBotsPingPost,
  deleteBotApiBotsBotIdDelete,
  storageOverviewApiStorageOverviewGet,
  storageScanApiStorageScanPost,
  storageFilesApiStorageFilesGet,
  deleteStorageFilesApiStorageFilesDelete,
  storageBreakpointsApiStorageBreakpointsGet,
  storageCleanOrphansApiStorageCleanOrphansPost,
  storageMetricsApiStorageMetricsGet,
  updateStorageQuotaPut,
  getSystemLogsApiLogsSystemGet,
  getForwardSkipsApiAuditForwardSkipsGet,
  getPublishFailuresApiAuditPublishFailuresGet,
  getDownloadFailuresApiAuditDownloadFailuresGet,
  getSettingsApiSettingsGet,
  updateSettingsApiSettingsPut,
  verifyTransitSettingsApiSettingsVerifyTransitPost,
  getTasksApiTasksGet,
  getTaskApiTasksTaskIdGet,
  getTaskLogsApiTasksTaskIdLogsGet,
  pauseTaskApiTasksTaskIdPausePost,
  resumeTaskApiTasksTaskIdResumePost,
  steadyResumeTaskApiTasksTaskIdSteadyResumePost,
  skipCurrentFileApiTasksTaskIdSkipCurrentPost,
  retryTaskApiTasksTaskIdRetryPost,
  cancelTaskApiTasksTaskIdCancelPost,
  deleteTaskApiTasksTaskIdDelete,
  batchTaskActionApiTasksBatchPost,
  clearFinishedTasksApiTasksDelete,
  pauseQueueApiTasksQueuePausePost,
  resumeQueueApiTasksQueueResumePost,
  precheckChannelAccessApiTasksPrecheckAccessPost,
  updateTaskConcurrencyApiTasksTaskIdConcurrencyPost,
  getPresetsApiPresetsGet,
  addPresetApiPresetsPost,
  deletePresetApiPresetsPresetIdDelete,
  inspectChannelApiPresetsInspectPost,
  downloadLinksApiDownloadLinkPost,
  downloadRangeApiDownloadRangePost,
  inspectDownloadRangeApiDownloadInspectRangePost,
  publishFolderApiPublishFolderPost,
  inspectFolderApiPublishInspectPost,
  inspectChannelApiForwardInspectPost,
  forwardChannelApiForwardChannelPost,
  getForwardCoverageApiForwardCoverageGet,
  getForwardCoverageGapsApiForwardCoverageGapsGet,
  estimateForwardApiForwardEstimatePost,
  getForwardHistoryApiForwardHistoryGet,
  getForwardReceiptApiForwardReceiptTaskIdGet,
  pikpakAuthStatusApiPikpakAuthStatusGet,
  pikpakDeviceStartApiPikpakAuthDeviceStartPost,
  pikpakDevicePollApiPikpakAuthDevicePollPost,
  pikpakFileListApiPikpakFilesListGet,
  pikpakInspectApiPikpakInspectPost,
  pikpakPublishApiPikpakPublishPost,
  getAuthStatusApiAuthStatusGet,
  verifyAuthTokenApiAuthVerifyGet,
  createMediaTicketApiAuthMediaTicketPost,
  authenticateBootstrapDeployerApiTelegramBootstrapAuthPost,
  logoutBootstrapDeployerApiTelegramBootstrapLogoutPost,
  getTelegramStatusApiTelegramStatusGet,
  sendWeb2FaApiTelegramWeb2FaSendPost,
  verifyWeb2FaApiTelegramWeb2FaVerifyPost,
  nativeSendCodeApiTelegramNativeSendCodePost,
  nativeResendCodeApiTelegramNativeResendCodePost,
  nativeVerifyCodeApiTelegramNativeVerifyCodePost,
  nativeSubmitPasswordApiTelegramNativePasswordPost,
  nativeQrStartApiTelegramQrStartPost,
  nativeQrStatusApiTelegramQrStatusGet,
  nativeQrPasswordApiTelegramQrPasswordPost,
  nativeCancelChallengeApiTelegramChallengeCancelPost,
} from './gen'

import type {
  AccountCreateRequest,
  AccountSendCodeRequest,
  AccountVerifyCodeRequest,
  AccountUpdateRequest,
  BotCreateRequest,
  ChallengeActionRequest,
  ChallengePasswordRequest,
  StorageDeleteRequest,
  SystemSettingUpdateRequest,
  BatchTaskActionRequest,
  SkipCurrentFileRequest,
  PrecheckAccessRequest,
  UpdateTaskConcurrencyRequest,
  PresetCreateRequest,
  PresetInspectRequest,
  DownloadLinkRequest,
  DownloadRangeRequest,
  InspectDownloadRangeRequest,
  PublishFolderRequest,
  InspectFolderRequest,
  InspectChannelRequest,
  ForwardChannelRequest,
  ForwardEstimateRequest,
  PikPakInspectRequest,
  PikPakPublishRequest,
  PikPakDevicePollRequest,
  NativeQrStartRequest,
  NativeSendCodeRequest,
  NativeVerifyCodeRequest,
  GetTasksApiTasksGetData,
  StorageFilesApiStorageFilesGetData,
  GetSystemLogsApiLogsSystemGetData,
  GetForwardSkipsApiAuditForwardSkipsGetData,
  GetPublishFailuresApiAuditPublishFailuresGetData,
  GetDownloadFailuresApiAuditDownloadFailuresGetData,
  GetForwardCoverageApiForwardCoverageGetData,
  GetForwardCoverageGapsApiForwardCoverageGapsGetData,
  GetForwardHistoryApiForwardHistoryGetData,
} from './gen/types.gen'

type QueryOrRecord<T> = NonNullable<T>
type BodyOrRecord<T> = T

// ==========================================
// 1. Userbot 账号接口集 (/api/accounts)
// ==========================================
export const accountsApi = {
  list() {
    return listAccountsApiAccountsGet()
  },
  create(data: BodyOrRecord<AccountCreateRequest>) {
    return createAccountApiAccountsPost({ body: data as AccountCreateRequest })
  },
  sendCode(accountId: number | string, data: BodyOrRecord<AccountSendCodeRequest> = {}) {
    return sendAccountCodeApiAccountsAccountIdSendCodePost({
      path: { account_id: Number(accountId) },
      body: data as AccountSendCodeRequest,
    })
  },
  resendCode(accountId: number | string, data: BodyOrRecord<ChallengeActionRequest>) {
    return resendAccountCodeApiAccountsAccountIdResendCodePost({
      path: { account_id: Number(accountId) },
      body: data as ChallengeActionRequest,
    })
  },
  verifyCode(accountId: number | string, data: BodyOrRecord<AccountVerifyCodeRequest>) {
    return verifyAccountCodeApiAccountsAccountIdVerifyCodePost({
      path: { account_id: Number(accountId) },
      body: data as AccountVerifyCodeRequest,
    })
  },
  submitPassword(accountId: number | string, data: BodyOrRecord<ChallengePasswordRequest>) {
    return submitAccountPasswordApiAccountsAccountIdPasswordPost({
      path: { account_id: Number(accountId) },
      body: data as ChallengePasswordRequest,
    })
  },
  startQr(accountId: number | string) {
    return startAccountQrApiAccountsAccountIdQrStartPost({
      path: { account_id: Number(accountId) },
    })
  },
  getQrStatus(accountId: number | string, challengeId: string) {
    return getAccountQrStatusApiAccountsAccountIdQrStatusGet({
      path: { account_id: Number(accountId) },
      query: { challenge_id: challengeId },
    })
  },
  submitQrPassword(accountId: number | string, data: BodyOrRecord<ChallengePasswordRequest>) {
    return submitAccountQrPasswordApiAccountsAccountIdQrPasswordPost({
      path: { account_id: Number(accountId) },
      body: data as ChallengePasswordRequest,
    })
  },
  cancelChallenge(accountId: number | string, data: BodyOrRecord<ChallengeActionRequest>) {
    return cancelAccountChallengeApiAccountsAccountIdChallengeCancelPost({
      path: { account_id: Number(accountId) },
      body: data as ChallengeActionRequest,
    })
  },
  refresh(accountId: number | string) {
    return refreshAccountStatusApiAccountsAccountIdRefreshPost({
      path: { account_id: Number(accountId) },
    })
  },
  update(accountId: number | string, data: BodyOrRecord<AccountUpdateRequest>) {
    return updateAccountApiAccountsAccountIdPut({
      path: { account_id: Number(accountId) },
      body: data as AccountUpdateRequest,
    })
  },
  delete(accountId: number | string, deleteSessionFile: boolean = false) {
    return deleteAccountApiAccountsAccountIdDelete({
      path: { account_id: Number(accountId) },
      query: { delete_session_file: deleteSessionFile },
    })
  },
}

// ==========================================
// 2. Bot 令牌池接口集 (/api/bots)
// ==========================================
export const botsApi = {
  list() {
    return listBotsApiBotsGet()
  },
  create(data: BodyOrRecord<BotCreateRequest>) {
    return createBotApiBotsPost({ body: data as BotCreateRequest })
  },
  ping() {
    return pingBotsApiBotsPingPost()
  },
  delete(botId: number | string) {
    return deleteBotApiBotsBotIdDelete({ path: { bot_id: Number(botId) } })
  },
}

// ==========================================
// 3. 存储资源管理器接口集 (/api/storage)
// 媒体流 URL 构造器见 api/client.ts (getStoragePreviewUrl / getStorageDownloadUrl)
// ==========================================
export const storageApi = {
  getOverview() {
    return storageOverviewApiStorageOverviewGet()
  },
  scan() {
    return storageScanApiStorageScanPost()
  },
  getFiles(params: QueryOrRecord<StorageFilesApiStorageFilesGetData['query']> = {}) {
    return storageFilesApiStorageFilesGet({ query: params as StorageFilesApiStorageFilesGetData['query'] })
  },
  deleteFiles(paths: string[] = []) {
    const payload: StorageDeleteRequest = { paths }
    return deleteStorageFilesApiStorageFilesDelete({ body: payload })
  },
  getBreakpoints() {
    return storageBreakpointsApiStorageBreakpointsGet()
  },
  cleanOrphans() {
    return storageCleanOrphansApiStorageCleanOrphansPost()
  },
  getMetrics() {
    return storageMetricsApiStorageMetricsGet()
  },
  updateQuota(quotaGb: number) {
    return updateStorageQuotaPut({ body: { quota_gb: quotaGb } })
  },
}

// ==========================================
// 4. 日志中心接口集 (/api/logs)
// ==========================================
export const logsApi = {
  getSystemLogs(params: QueryOrRecord<GetSystemLogsApiLogsSystemGetData['query']> = {}) {
    return getSystemLogsApiLogsSystemGet({ query: params as GetSystemLogsApiLogsSystemGetData['query'] })
  },
}

// ==========================================
// 5. 审计与账本接口集 (/api/audit)
// ==========================================
export const auditApi = {
  getForwardSkips(params: QueryOrRecord<GetForwardSkipsApiAuditForwardSkipsGetData['query']> = {}) {
    return getForwardSkipsApiAuditForwardSkipsGet({
      query: params as GetForwardSkipsApiAuditForwardSkipsGetData['query'],
    })
  },
  getPublishFailures(params: QueryOrRecord<GetPublishFailuresApiAuditPublishFailuresGetData['query']> = {}) {
    return getPublishFailuresApiAuditPublishFailuresGet({
      query: params as GetPublishFailuresApiAuditPublishFailuresGetData['query'],
    })
  },
  getDownloadFailures(params: QueryOrRecord<GetDownloadFailuresApiAuditDownloadFailuresGetData['query']> = {}) {
    return getDownloadFailuresApiAuditDownloadFailuresGet({
      query: params as GetDownloadFailuresApiAuditDownloadFailuresGetData['query'],
    })
  },
}

// ==========================================
// 6. 系统配置接口集 (/api/settings)
// ==========================================
export const settingsApi = {
  getSettings() {
    return getSettingsApiSettingsGet()
  },
  updateSettings(settingsMap: SystemSettingUpdateRequest['settings'] | Record<string, unknown>) {
    return updateSettingsApiSettingsPut({ body: { settings: settingsMap as SystemSettingUpdateRequest['settings'] } })
  },
  verifyTransit() {
    return verifyTransitSettingsApiSettingsVerifyTransitPost()
  },
}

// ==========================================
// 7. 任务调度与控制接口集 (/api/tasks)
// ==========================================
export const tasksApi = {
  list(params: QueryOrRecord<GetTasksApiTasksGetData['query']> = {}) {
    return getTasksApiTasksGet({ query: params as GetTasksApiTasksGetData['query'] })
  },
  get(taskId: string) {
    return getTaskApiTasksTaskIdGet({ path: { task_id: taskId } })
  },
  getLogs(taskId: string, afterSeq: number = 0, limit: number = 200) {
    return getTaskLogsApiTasksTaskIdLogsGet({
      path: { task_id: taskId },
      query: { after_seq: afterSeq, limit },
    })
  },
  pause(taskId: string) {
    return pauseTaskApiTasksTaskIdPausePost({ path: { task_id: taskId } })
  },
  resume(taskId: string, mode: string = 'normal') {
    return resumeTaskApiTasksTaskIdResumePost({
      path: { task_id: taskId },
      query: { mode },
    })
  },
  steadyResume(taskId: string) {
    return steadyResumeTaskApiTasksTaskIdSteadyResumePost({
      path: { task_id: taskId },
    })
  },
  skipCurrent(taskId: string, data?: BodyOrRecord<SkipCurrentFileRequest>) {
    return skipCurrentFileApiTasksTaskIdSkipCurrentPost({
      path: { task_id: taskId },
      body: (data || {}) as SkipCurrentFileRequest,
    })
  },
  retry(taskId: string) {
    return retryTaskApiTasksTaskIdRetryPost({ path: { task_id: taskId } })
  },
  cancel(taskId: string) {
    return cancelTaskApiTasksTaskIdCancelPost({ path: { task_id: taskId } })
  },
  delete(taskId: string, deleteFiles: boolean = false) {
    return deleteTaskApiTasksTaskIdDelete({
      path: { task_id: taskId },
      query: deleteFiles ? { delete_files: true } : {},
    })
  },
  batch(data: BodyOrRecord<BatchTaskActionRequest>) {
    return batchTaskActionApiTasksBatchPost({ body: data as BatchTaskActionRequest })
  },
  clearFinished() {
    return clearFinishedTasksApiTasksDelete()
  },
  queuePause() {
    return pauseQueueApiTasksQueuePausePost()
  },
  queueResume() {
    return resumeQueueApiTasksQueueResumePost()
  },
  precheckAccess(data: BodyOrRecord<PrecheckAccessRequest>) {
    return precheckChannelAccessApiTasksPrecheckAccessPost({ body: data as PrecheckAccessRequest })
  },
  updateConcurrency(taskId: string, data: BodyOrRecord<UpdateTaskConcurrencyRequest>) {
    return updateTaskConcurrencyApiTasksTaskIdConcurrencyPost({
      path: { task_id: taskId },
      body: data as UpdateTaskConcurrencyRequest,
    })
  },
}

// ==========================================
// 8. 预设频道接口集 (/api/presets)
// ==========================================
export const presetsApi = {
  list(type: string | null = null) {
    return getPresetsApiPresetsGet({ query: type ? { type } : {} })
  },
  create(data: BodyOrRecord<PresetCreateRequest>) {
    return addPresetApiPresetsPost({ body: data as PresetCreateRequest })
  },
  delete(presetId: number | string) {
    return deletePresetApiPresetsPresetIdDelete({ path: { preset_id: Number(presetId) } })
  },
  inspect(data: BodyOrRecord<PresetInspectRequest>) {
    return inspectChannelApiPresetsInspectPost({ body: data as PresetInspectRequest })
  },
}

// ==========================================
// 9. 任务发布业务接口集
// ==========================================
export const downloadApi = {
  submitLink(data: BodyOrRecord<DownloadLinkRequest>) {
    return downloadLinksApiDownloadLinkPost({ body: data as DownloadLinkRequest })
  },
  submitRange(data: BodyOrRecord<DownloadRangeRequest>) {
    return downloadRangeApiDownloadRangePost({ body: data as DownloadRangeRequest })
  },
  inspectRange(data: BodyOrRecord<InspectDownloadRangeRequest>) {
    return inspectDownloadRangeApiDownloadInspectRangePost({ body: data as InspectDownloadRangeRequest })
  },
}

export const publishApi = {
  submitFolder(data: BodyOrRecord<PublishFolderRequest>) {
    return publishFolderApiPublishFolderPost({ body: data as PublishFolderRequest })
  },
  inspectFolder(data: BodyOrRecord<InspectFolderRequest>) {
    return inspectFolderApiPublishInspectPost({ body: data as InspectFolderRequest })
  },
}

export const forwardApi = {
  inspect(sourceChat: string) {
    const payload: InspectChannelRequest = { source_chat: sourceChat }
    return inspectChannelApiForwardInspectPost({ body: payload })
  },
  submit(data: BodyOrRecord<ForwardChannelRequest>) {
    return forwardChannelApiForwardChannelPost({ body: data as ForwardChannelRequest })
  },
  getCoverage(params: QueryOrRecord<GetForwardCoverageApiForwardCoverageGetData['query']>) {
    return getForwardCoverageApiForwardCoverageGet({
      query: params as GetForwardCoverageApiForwardCoverageGetData['query'],
    })
  },
  getGaps(params: QueryOrRecord<GetForwardCoverageGapsApiForwardCoverageGapsGetData['query']>) {
    return getForwardCoverageGapsApiForwardCoverageGapsGet({
      query: params as GetForwardCoverageGapsApiForwardCoverageGapsGetData['query'],
    })
  },
  estimate(data: BodyOrRecord<ForwardEstimateRequest>) {
    return estimateForwardApiForwardEstimatePost({ body: data as ForwardEstimateRequest })
  },
  getHistory(params: QueryOrRecord<GetForwardHistoryApiForwardHistoryGetData['query']> = {}) {
    return getForwardHistoryApiForwardHistoryGet({
      query: params as GetForwardHistoryApiForwardHistoryGetData['query'],
    })
  },
  getReceipt(taskId: string) {
    return getForwardReceiptApiForwardReceiptTaskIdGet({ path: { task_id: taskId } })
  },
}

// ==========================================
// 10. PikPak 流式发布接口集 (/api/pikpak)
// 双依赖 (X-API-Key + X-2FA-Token) 由 client.ts 拦截器统一注入;
// 403 (detail 以 "2FA" 开头) 自动走既有重验引导分支, 本组零特殊处理。
// auth/status 恒 200 三态契约 (state=logged_in|not_bound|unreachable): 业务失败不抛传输
// 错误, 仅宿主不可达/401 走 reject (store 侧 fetchError 传输层兜底)。
// ==========================================
export const pikpakApi = {
  authStatus(force: boolean = false) {
    return pikpakAuthStatusApiPikpakAuthStatusGet({ query: force ? { force: true } : {} })
  },
  deviceStart() {
    return pikpakDeviceStartApiPikpakAuthDeviceStartPost()
  },
  devicePoll(payload: PikPakDevicePollRequest & { max_wait_s?: number }) {
    // 服务端本轮长轮询会挂起 max_wait_s 秒, 全局 30s 超时会先掐断未完成授权的
    // 轮询, 使 store 侧 timed_out 正常过期分支不可达 —— 按等待时长覆盖超时并留传输余量。
    const waitS = Number(payload?.max_wait_s) || 0
    return pikpakDevicePollApiPikpakAuthDevicePollPost({
      body: payload,
      timeout: waitS * 1000 + 5000,
    })
  },
  fileList(folderId: string | null, pageToken: string | null = null, refresh: boolean = false) {
    const query: { folder_id?: string; page_token?: string | null; refresh?: boolean } = {
      folder_id: folderId || '',
    }
    if (pageToken) query.page_token = pageToken
    if (refresh) query.refresh = true
    return pikpakFileListApiPikpakFilesListGet({ query })
  },
  inspect(payload: BodyOrRecord<PikPakInspectRequest>) {
    return pikpakInspectApiPikpakInspectPost({ body: payload as PikPakInspectRequest })
  },
  publish(payload: BodyOrRecord<PikPakPublishRequest>) {
    return pikpakPublishApiPikpakPublishPost({ body: payload as PikPakPublishRequest })
  },
}

// ==========================================
// 11. 控制台口令与 Telegram 会话鉴权接口集
// ==========================================
export const authApi = {
  getStatus() {
    return getAuthStatusApiAuthStatusGet()
  },
  verify(key: string) {
    return verifyAuthTokenApiAuthVerifyGet({
      headers: { 'X-API-Key': key },
      authVerificationGeneration: useAuthStore().getCredentialGeneration(),
    })
  },
  issueMediaTicket() {
    return createMediaTicketApiAuthMediaTicketPost()
  },
}

export const telegramAuthApi = {
  bootstrapAuth(bootstrapToken: string) {
    return authenticateBootstrapDeployerApiTelegramBootstrapAuthPost({
      headers: { 'X-Bootstrap-Token': bootstrapToken },
    })
  },
  bootstrapLogout() {
    return logoutBootstrapDeployerApiTelegramBootstrapLogoutPost()
  },
  getStatus(probePremium = true, signal?: AbortSignal) {
    return getTelegramStatusApiTelegramStatusGet({ query: probePremium ? undefined : { probe_premium: false }, signal })
  },
  sendWeb2FA() {
    return sendWeb2FaApiTelegramWeb2FaSendPost()
  },
  verifyWeb2FA(code: string) {
    return verifyWeb2FaApiTelegramWeb2FaVerifyPost({ body: { code } })
  },
  nativeSendCode(phone: string | null = null) {
    const payload: NativeSendCodeRequest = phone ? { phone } : {}
    return nativeSendCodeApiTelegramNativeSendCodePost({ body: payload })
  },
  nativeResendCode(challengeId: string) {
    const payload: ChallengeActionRequest = { challenge_id: challengeId }
    return nativeResendCodeApiTelegramNativeResendCodePost({ body: payload })
  },
  nativeVerifyCode(data: BodyOrRecord<NativeVerifyCodeRequest>) {
    return nativeVerifyCodeApiTelegramNativeVerifyCodePost({ body: data as NativeVerifyCodeRequest })
  },
  nativeSubmitPassword(data: BodyOrRecord<ChallengePasswordRequest>) {
    return nativeSubmitPasswordApiTelegramNativePasswordPost({ body: data as ChallengePasswordRequest })
  },
  nativeQrStart(payload: NativeQrStartRequest) {
    return nativeQrStartApiTelegramQrStartPost({ body: payload })
  },
  nativeQrStatus(challengeId: string) {
    return nativeQrStatusApiTelegramQrStatusGet({ query: { challenge_id: challengeId } })
  },
  nativeQrPassword(data: BodyOrRecord<ChallengePasswordRequest>) {
    return nativeQrPasswordApiTelegramQrPasswordPost({ body: data as ChallengePasswordRequest })
  },
  nativeCancelChallenge(challengeId: string) {
    const payload: ChallengeActionRequest = { challenge_id: challengeId }
    return nativeCancelChallengeApiTelegramChallengeCancelPost({ body: payload })
  },
}
