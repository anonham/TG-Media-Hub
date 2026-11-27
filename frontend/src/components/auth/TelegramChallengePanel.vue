<template>
  <div class="tg-challenge-panel">
    <!-- 无障碍礼貌状态播报 -->
    <div class="visually-hidden" aria-live="polite" role="status">{{ politeStatus }}</div>

    <!-- 登录方式切换 (验证码 vs 扫码) -->
    <div class="method-tablist" role="tablist" aria-label="选择 Telegram 登录方式">
      <button
        :id="`${uid}-tab-otp`"
        type="button"
        role="tab"
        class="method-tab"
        :class="{ 'is-active': method === 'otp' }"
        :aria-selected="method === 'otp'"
        :tabindex="method === 'otp' ? 0 : -1"
        :disabled="submitting"
        @click="switchMethod('otp')"
        @keydown="onTabKeydown"
      >
        📱 验证码登录
      </button>
      <button
        :id="`${uid}-tab-qr`"
        type="button"
        role="tab"
        class="method-tab"
        :class="{ 'is-active': method === 'qr' }"
        :aria-selected="method === 'qr'"
        :tabindex="method === 'qr' ? 0 : -1"
        :disabled="submitting"
        @click="switchMethod('qr')"
        @keydown="onTabKeydown"
      >
        📷 扫码登录 (QR)
      </button>
    </div>

    <!-- 关键错误/状态横幅 -->
    <div v-if="bannerError" role="alert" class="challenge-alert">
      <span>{{ bannerError }}</span>
    </div>

    <!-- 方式 A: OTP 验证码登录 -->
    <div v-if="method === 'otp'" role="tabpanel" :aria-labelledby="`${uid}-tab-otp`" class="challenge-tabpanel">
      <!-- 步骤 1: 输入手机号 / 发起下发 -->
      <form v-if="otpStep === 'phone'" method="POST" novalidate @submit.prevent="handleSendOtp">
        <div class="field-group">
          <label :for="`${uid}-phone`" class="field-label"> 手机号码 <span aria-hidden="true">*</span> </label>
          <p :id="`${uid}-phone-hint`" class="field-hint">
            {{
              phoneLocked
                ? '已锁定账号绑定的国际手机号，点击下方按钮下发官方验证码。'
                : '请输入带国际区号的手机号（例如 +XX XXXXXXXXX）。'
            }}
          </p>
          <input
            :id="`${uid}-phone`"
            ref="phoneInputRef"
            v-model="phone"
            type="tel"
            name="phone"
            class="field-input"
            autocomplete="tel"
            inputmode="tel"
            enterkeyhint="next"
            autocorrect="off"
            autocapitalize="none"
            spellcheck="false"
            placeholder="+XX XXXXXXXXX"
            :disabled="phoneLocked || sending"
            :aria-invalid="phoneError ? 'true' : 'false'"
            :aria-describedby="phoneDescribedBy"
            required
            @input="onPhoneInput"
            @blur="onPhoneBlur"
            @keydown.enter="(e: KeyboardEvent) => onImeSafeEnter(e, handleSendOtp)"
          />
          <p v-if="phoneError" :id="`${uid}-phone-err`" class="field-error">{{ phoneError }}</p>
        </div>

        <button type="submit" class="primary-btn" :disabled="sending">
          {{ sending ? '正在下发验证码...' : '发送官方验证码' }}
        </button>
      </form>

      <!-- 步骤 2: 输入验证码 (+ 条件 2FA 云端密码) -->
      <form v-else method="POST" novalidate @submit.prevent="handleOtpSubmit">
        <div class="channel-info-box">
          <p class="channel-title"><strong>投递渠道：</strong>{{ deliveryChannelLabel }}</p>
          <p v-if="codeLength" class="channel-meta">请查收 {{ codeLength }} 位数字验证码（支持保留前导零）。</p>
          <p v-else class="channel-meta">请查收 Telegram 官方下发的数字验证码。</p>
        </div>

        <div v-if="challengeState !== 'password_required'" class="field-group">
          <label :for="`${uid}-otp`" class="field-label"> Telegram 验证码 <span aria-hidden="true">*</span> </label>
          <p :id="`${uid}-otp-hint`" class="field-hint">
            请输入通过{{ deliveryChannelLabel }}收到的{{ codeLength ? ` ${codeLength} 位` : '' }}数字验证码。
          </p>
          <input
            :id="`${uid}-otp`"
            ref="otpInputRef"
            v-model="otpCode"
            type="text"
            name="otp_code"
            class="field-input otp-input"
            inputmode="numeric"
            autocomplete="one-time-code"
            autocorrect="off"
            autocapitalize="none"
            spellcheck="false"
            enterkeyhint="done"
            :maxlength="codeLength ?? 16"
            :pattern="codeLength ? `\\d{${codeLength}}` : '\\d+'"
            placeholder="输入数字验证码"
            :disabled="submitting"
            :aria-invalid="otpError ? 'true' : 'false'"
            :aria-describedby="otpDescribedBy"
            required
            @input="onOtpInput"
            @blur="onOtpBlur"
            @keydown.enter="(e: KeyboardEvent) => onImeSafeEnter(e, handleOtpSubmit)"
          />
          <p v-if="otpError" :id="`${uid}-otp-err`" class="field-error">{{ otpError }}</p>
        </div>

        <!-- 条件展示两步验证密码 (由服务端 password_required 触发) -->
        <div v-if="challengeState === 'password_required'" class="field-group">
          <label :for="`${uid}-otp-pwd`" class="field-label">
            两步验证 (2FA) 云端密码 <span aria-hidden="true">*</span>
          </label>
          <p :id="`${uid}-otp-pwd-hint`" class="field-hint">
            该账号已开启 Telegram 云端两步验证密码，请输入密码完成最终授权。
          </p>
          <div class="password-row">
            <input
              :id="`${uid}-otp-pwd`"
              ref="pwdInputRef"
              v-model="password"
              :type="showPassword ? 'text' : 'password'"
              name="password"
              class="field-input"
              autocomplete="current-password"
              autocorrect="off"
              autocapitalize="none"
              spellcheck="false"
              enterkeyhint="done"
              placeholder="输入 2FA 云端密码"
              :disabled="submitting"
              :aria-invalid="passwordError ? 'true' : 'false'"
              :aria-describedby="pwdDescribedBy"
              required
              @input="onPasswordInput"
              @blur="onPasswordBlur"
              @keydown.enter="(e: KeyboardEvent) => onImeSafeEnter(e, handleOtpSubmit)"
            />
            <button
              type="button"
              class="toggle-pwd-btn"
              :aria-pressed="showPassword"
              :aria-label="showPassword ? '隐藏密码' : '显示密码'"
              @click="showPassword = !showPassword"
            >
              {{ showPassword ? '隐藏' : '显示' }}
            </button>
          </div>
          <p v-if="passwordError" :id="`${uid}-otp-pwd-err`" class="field-error">{{ passwordError }}</p>
        </div>

        <button type="submit" class="primary-btn" :disabled="submitting || challengeState === 'expired'">
          {{
            submitting ? '正在校验...' : challengeState === 'password_required' ? '提交密码完成授权' : '验证并完成登录'
          }}
        </button>

        <!-- 仅当服务端允许 fallback 渠道时显示重发与倒计时 -->
        <div v-if="challengeState === 'code_sent'" class="resend-section">
          <template v-if="canResend">
            <button
              type="button"
              class="link-action-btn"
              :disabled="sending || submitting || resendCountdown > 0"
              @click="handleResendOtp"
            >
              {{
                resendCountdown > 0
                  ? `重新发送 (${resendCountdown}s · ${nextChannelLabel})`
                  : `通过${nextChannelLabel}重新发送`
              }}
            </button>
          </template>
          <p v-else class="no-fallback-hint">Telegram 未提供备用重发渠道；如未收到可等待超时或切换扫码登录。</p>
        </div>

        <div class="secondary-actions">
          <button type="button" class="secondary-btn" :disabled="submitting" @click="resetToPhoneStep">重新发起</button>
          <button
            v-if="showCancelButton"
            type="button"
            class="secondary-btn"
            :disabled="submitting"
            @click="handleUserCancel"
          >
            取消
          </button>
        </div>
      </form>
    </div>

    <!-- 方式 B: QR 扫码登录 -->
    <div v-else role="tabpanel" :aria-labelledby="`${uid}-tab-qr`" class="challenge-tabpanel qr-panel">
      <div v-if="challengeState === 'password_required'" class="qr-password-stage">
        <div class="channel-info-box">
          <p class="channel-title"><strong>扫码已确认</strong></p>
          <p class="channel-meta">该账号启用了两步验证 (2FA)，请输入云端密码完成授权。</p>
        </div>
        <form method="POST" novalidate @submit.prevent="handleQrPasswordSubmit">
          <div class="field-group">
            <label :for="`${uid}-qr-pwd`" class="field-label">
              两步验证 (2FA) 云端密码 <span aria-hidden="true">*</span>
            </label>
            <p :id="`${uid}-qr-pwd-hint`" class="field-hint">请输入您的 Telegram 云端两步验证密码。</p>
            <div class="password-row">
              <input
                :id="`${uid}-qr-pwd`"
                ref="pwdInputRef"
                v-model="password"
                :type="showPassword ? 'text' : 'password'"
                name="qr_password"
                class="field-input"
                autocomplete="current-password"
                autocorrect="off"
                autocapitalize="none"
                spellcheck="false"
                enterkeyhint="done"
                placeholder="输入 2FA 云端密码"
                :disabled="submitting"
                :aria-invalid="passwordError ? 'true' : 'false'"
                :aria-describedby="qrPwdDescribedBy"
                required
                @input="onPasswordInput"
                @blur="onPasswordBlur"
                @keydown.enter="(e: KeyboardEvent) => onImeSafeEnter(e, handleQrPasswordSubmit)"
              />
              <button
                type="button"
                class="toggle-pwd-btn"
                :aria-pressed="showPassword"
                :aria-label="showPassword ? '隐藏密码' : '显示密码'"
                @click="showPassword = !showPassword"
              >
                {{ showPassword ? '隐藏' : '显示' }}
              </button>
            </div>
            <p v-if="passwordError" :id="`${uid}-qr-pwd-err`" class="field-error">{{ passwordError }}</p>
          </div>
          <button type="submit" class="primary-btn" :disabled="submitting">
            {{ submitting ? '正在验证...' : '提交密码完成授权' }}
          </button>
        </form>
      </div>

      <div v-else-if="needsQrPhoneConfirmation && !challengeId" class="qr-phone-stage">
        <div class="channel-info-box">
          <p class="channel-title"><strong>确认扫码账号的手机号</strong></p>
        </div>
        <form method="POST" novalidate @submit.prevent="startQrFlow">
          <div class="field-group">
            <label :for="`${uid}-qr-phone`" class="field-label"> 手机号码 <span aria-hidden="true">*</span> </label>
            <p :id="`${uid}-qr-phone-hint`" class="field-hint">{{ qrPhonePrompt }}</p>
            <input
              :id="`${uid}-qr-phone`"
              ref="phoneInputRef"
              v-model="phone"
              type="tel"
              name="qr_phone"
              class="field-input"
              autocomplete="tel"
              inputmode="tel"
              enterkeyhint="done"
              autocorrect="off"
              autocapitalize="none"
              spellcheck="false"
              placeholder="+XX XXXXXXXXX"
              :disabled="qrStarting"
              :aria-invalid="phoneError ? 'true' : 'false'"
              :aria-describedby="qrPhoneDescribedBy"
              required
              @input="onPhoneInput"
              @blur="onPhoneBlur"
              @keydown.enter="(e: KeyboardEvent) => onImeSafeEnter(e, startQrFlow)"
            />
            <p v-if="phoneError" :id="`${uid}-qr-phone-err`" class="field-error">{{ phoneError }}</p>
          </div>
          <div class="secondary-actions">
            <button type="submit" class="primary-btn" :disabled="qrStarting">
              {{ qrStarting ? '正在生成...' : '确认并生成二维码' }}
            </button>
            <button
              v-if="showCancelButton"
              type="button"
              class="secondary-btn"
              :disabled="qrStarting"
              @click="handleUserCancel"
            >
              取消
            </button>
          </div>
        </form>
      </div>

      <div v-else class="qr-scan-stage">
        <div class="channel-info-box">
          <p class="channel-title"><strong>手机扫码授权</strong></p>
          <p v-if="qrPhoneHint" class="channel-meta">
            请先切换到手机号为 <strong>{{ qrPhoneHint }}</strong> 的 Telegram 账号。
          </p>
          <p class="channel-meta">打开手机 Telegram → 设置 → 设备 → 关联桌面设备，扫描下方本地生成的二维码。</p>
        </div>

        <div class="qr-preview-wrap">
          <div v-if="qrStarting" class="qr-loading">正在生成安全登录二维码...</div>
          <TelegramQrCode
            v-else-if="qrUrl && qrRemainingSeconds > 0 && challengeState === 'qr_pending'"
            :value="qrUrl"
          />
          <div v-else class="qr-expired-box">
            <p>{{ challengeState === 'expired' ? '二维码已过期' : '二维码尚未生成或已失效' }}</p>
          </div>
        </div>

        <p v-if="challengeState === 'qr_pending'" class="qr-countdown">
          二维码剩余有效期：<strong>{{ qrRemainingSeconds }}s</strong>
        </p>

        <div class="secondary-actions">
          <button type="button" class="primary-btn" :disabled="qrStarting || submitting" @click="startQrFlow">
            {{ qrStarting ? '正在生成...' : '刷新二维码' }}
          </button>
          <button
            v-if="canChangeQrPhone"
            type="button"
            class="secondary-btn"
            :disabled="qrStarting || submitting"
            @click="resetQrPhoneStep"
          >
            更换手机号
          </button>
          <button
            v-if="showCancelButton"
            type="button"
            class="secondary-btn"
            :disabled="submitting"
            @click="handleUserCancel"
          >
            取消
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onUnmounted, ref, useId, watch } from 'vue'
import TelegramQrCode from './TelegramQrCode.vue'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'
import { useAccountStore } from '@/stores/accountStore'
import { errCode, errDetail, errRetryAfter } from '@/utils/errors'
import { isAxiosError } from 'axios'
import { onImeSafeEnter } from '@/utils/ime'

const props = withDefaults(
  defineProps<{
    mode: 'native' | 'account'
    accountId?: number | null
    initialPhone?: string
    phoneLocked?: boolean
    active?: boolean
    showCancelButton?: boolean
  }>(),
  {
    accountId: null,
    initialPhone: '',
    phoneLocked: false,
    active: true,
    showCancelButton: false,
  },
)

const emit = defineEmits<{
  authenticated: []
  cancel: []
}>()

const uid = useId()
const tgStore = useTelegramAuthStore()
const accountStore = useAccountStore()

const CHANNEL_LABELS: Record<string, string> = {
  app: 'Telegram 客户端官方消息',
  sms: '手机短信',
  call: '语音电话',
  flash_call: '闪信来电',
  missed_call: '未接来电验证',
  fragment_sms: 'Fragment 虚拟号短信',
  firebase_sms: 'Firebase 推送短信',
  email: '绑定邮箱',
  unknown: 'Telegram 官方渠道',
}

const method = ref<'otp' | 'qr'>('otp')
const otpStep = ref<'phone' | 'code'>('phone')
const challengeId = ref('')
const challengeState = ref<string>('idle')
const phone = ref(props.initialPhone || '')
const otpCode = ref('')
const password = ref('')
const showPassword = ref(false)

const deliveryChannel = ref('unknown')
const codeLength = ref<number | null>(null)
const nextDeliveryChannel = ref<string | null>(null)
const canResend = ref(false)
const resendCountdown = ref(0)

const qrUrl = ref('')
const qrPhoneHint = ref('')
const qrRemainingSeconds = ref(0)
const qrStarting = ref(false)

const sending = ref(false)
const submitting = ref(false)

const phoneError = ref('')
const otpError = ref('')
const passwordError = ref('')
const bannerError = ref('')
const bannerErrorCode = ref('')
const politeStatus = ref('')

const phoneTouched = ref(false)
const otpTouched = ref(false)
const passwordTouched = ref(false)

const phoneInputRef = ref<HTMLInputElement | null>(null)
const otpInputRef = ref<HTMLInputElement | null>(null)
const pwdInputRef = ref<HTMLInputElement | null>(null)

let sessionSeq = 0
type ChallengeOwner = { mode: typeof props.mode; accountId: typeof props.accountId }
let challengeOwner: ChallengeOwner | null = null
let resendTimer: ReturnType<typeof setInterval> | null = null
let qrCountdownTimer: ReturnType<typeof setInterval> | null = null
let qrPollTimer: ReturnType<typeof setTimeout> | null = null

const deliveryChannelLabel = computed(() => CHANNEL_LABELS[deliveryChannel.value] || 'Telegram 官方渠道')
const nextChannelLabel = computed(() =>
  nextDeliveryChannel.value ? CHANNEL_LABELS[nextDeliveryChannel.value] || '备用渠道' : '备用渠道',
)

const phoneDescribedBy = computed(() => (phoneError.value ? `${uid}-phone-hint ${uid}-phone-err` : `${uid}-phone-hint`))
const qrPhoneDescribedBy = computed(() =>
  phoneError.value ? `${uid}-qr-phone-hint ${uid}-qr-phone-err` : `${uid}-qr-phone-hint`,
)
const needsQrPhoneConfirmation = computed(() => props.mode === 'native' && !props.phoneLocked)
const canChangeQrPhone = computed(
  () => needsQrPhoneConfirmation.value && tgStore.bindingMode === 'first_bind' && !tgStore.defaultPhoneConfigured,
)
const qrPhonePrompt = computed(() => {
  if (tgStore.bindingMode === 'rebind') return '请输入已绑定账号带国际区号的手机号。'
  if (tgStore.defaultPhoneConfigured) return '请输入部署配置指定的手机号，包含国际区号。'
  return '请输入要用来扫码的 Telegram 账号手机号，包含国际区号。'
})
const otpDescribedBy = computed(() => (otpError.value ? `${uid}-otp-hint ${uid}-otp-err` : `${uid}-otp-hint`))
const pwdDescribedBy = computed(() =>
  passwordError.value ? `${uid}-otp-pwd-hint ${uid}-otp-pwd-err` : `${uid}-otp-pwd-hint`,
)
const qrPwdDescribedBy = computed(() =>
  passwordError.value ? `${uid}-qr-pwd-hint ${uid}-qr-pwd-err` : `${uid}-qr-pwd-hint`,
)

function clearTimers(): void {
  if (resendTimer) {
    clearInterval(resendTimer)
    resendTimer = null
  }
  if (qrCountdownTimer) {
    clearInterval(qrCountdownTimer)
    qrCountdownTimer = null
  }
  if (qrPollTimer) {
    clearTimeout(qrPollTimer)
    qrPollTimer = null
  }
}

function startResendCountdown(seconds: number): void {
  if (resendTimer) {
    clearInterval(resendTimer)
    resendTimer = null
  }
  if (!canResend.value || seconds <= 0) {
    resendCountdown.value = 0
    return
  }
  resendCountdown.value = Math.ceil(seconds)
  resendTimer = setInterval(() => {
    if (resendCountdown.value > 1) {
      resendCountdown.value -= 1
    } else {
      resendCountdown.value = 0
      if (resendTimer) {
        clearInterval(resendTimer)
        resendTimer = null
      }
    }
  }, 1000)
}

function startQrCountdown(seconds: number): void {
  if (qrCountdownTimer) {
    clearInterval(qrCountdownTimer)
    qrCountdownTimer = null
  }
  qrRemainingSeconds.value = Math.max(0, Math.ceil(seconds))
  if (qrRemainingSeconds.value <= 0) return
  qrCountdownTimer = setInterval(() => {
    if (qrRemainingSeconds.value > 1) {
      qrRemainingSeconds.value -= 1
    } else {
      qrRemainingSeconds.value = 0
      if (qrCountdownTimer) {
        clearInterval(qrCountdownTimer)
        qrCountdownTimer = null
      }
    }
  }, 1000)
}

function currentOwner(): ChallengeOwner {
  return { mode: props.mode, accountId: props.accountId }
}

function isCurrentStart(seq: number, owner: ChallengeOwner, targetMethod: 'otp' | 'qr'): boolean {
  return (
    seq === sessionSeq &&
    props.active &&
    method.value === targetMethod &&
    owner.mode === props.mode &&
    owner.accountId === props.accountId
  )
}

async function cancelChallenge(owner: ChallengeOwner, cid: string): Promise<void> {
  if (owner.mode === 'native') {
    await tgStore.nativeCancelChallenge(cid)
  } else if (owner.accountId) {
    await accountStore.cancelAccountChallenge(owner.accountId, cid)
  }
}

async function bestEffortCancelActiveChallenge(): Promise<void> {
  const cid = challengeId.value
  const owner = challengeOwner
  const terminal =
    challengeState.value === 'authenticated' ||
    challengeState.value === 'expired' ||
    challengeState.value === 'cancelled' ||
    challengeState.value === 'failed'
  challengeId.value = ''
  challengeOwner = null
  const nativeQrCanRefresh =
    owner?.mode === 'native' && method.value === 'qr' && ['expired', 'failed'].includes(challengeState.value)
  if (cid && owner && (!terminal || nativeQrCanRefresh)) await cancelChallenge(owner, cid)
}

function resetLocalState(preservePhone = false): void {
  clearTimers()
  if (!preservePhone) {
    method.value = 'otp'
    phone.value = props.initialPhone || ''
  }
  otpStep.value = 'phone'
  challengeState.value = 'idle'
  otpCode.value = ''
  password.value = ''
  showPassword.value = false
  deliveryChannel.value = 'unknown'
  codeLength.value = null
  nextDeliveryChannel.value = null
  canResend.value = false
  resendCountdown.value = 0
  qrUrl.value = ''
  qrPhoneHint.value = ''
  qrRemainingSeconds.value = 0
  qrStarting.value = false
  sending.value = false
  submitting.value = false
  phoneError.value = ''
  otpError.value = ''
  passwordError.value = ''
  bannerError.value = ''
  bannerErrorCode.value = ''
  phoneTouched.value = false
  otpTouched.value = false
  passwordTouched.value = false
}

function teardownSession(preservePhone = false): void {
  sessionSeq += 1
  void bestEffortCancelActiveChallenge()
  resetLocalState(preservePhone)
}

watch(
  () => props.initialPhone,
  (val) => {
    if (method.value === 'otp' && !challengeId.value && otpStep.value === 'phone') {
      phone.value = val || ''
    }
  },
)

watch(
  () => [props.active, props.mode, props.accountId] as const,
  ([active], [prevActive]) => {
    if (!active) {
      if (prevActive) {
        teardownSession(false)
      }
      return
    }
    teardownSession(false)
  },
)

onUnmounted(() => {
  teardownSession(false)
})

function validatePhoneField(): boolean {
  const clean = phone.value.trim()
  if (!clean) {
    phoneError.value = '请输入带国际区号的手机号'
    return false
  }
  phoneError.value = ''
  return true
}

function validateOtpField(): boolean {
  const clean = otpCode.value.replace(/\D/g, '')
  if (!clean) {
    otpError.value = '请输入 Telegram 数字验证码'
    return false
  }
  if (codeLength.value && clean.length !== codeLength.value) {
    otpError.value = `请输入完整的 ${codeLength.value} 位数字验证码`
    return false
  }
  otpError.value = ''
  return true
}

function validatePasswordField(): boolean {
  if (!password.value || !password.value.trim()) {
    passwordError.value = '请输入两步验证 (2FA) 云端密码'
    return false
  }
  passwordError.value = ''
  return true
}

function onPhoneInput(): void {
  phoneError.value = ''
  bannerError.value = ''
}
function onPhoneBlur(): void {
  phoneTouched.value = true
  validatePhoneField()
}
function onOtpInput(): void {
  otpError.value = ''
  bannerError.value = ''
}
function onOtpBlur(): void {
  otpTouched.value = true
  validateOtpField()
}
function onPasswordInput(): void {
  passwordError.value = ''
  bannerError.value = ''
}
function onPasswordBlur(): void {
  passwordTouched.value = true
  validatePasswordField()
}

function applyServerError(err: unknown): void {
  const code = errCode(err)
  const retry = errRetryAfter(err)
  const detail = errDetail(err)
  bannerErrorCode.value = code
  bannerError.value = detail
  if (code === 'invalid_code') {
    otpError.value = detail
    void nextTick(() => otpInputRef.value?.focus())
  } else if (code === 'invalid_password') {
    passwordError.value = detail
    void nextTick(() => pwdInputRef.value?.focus())
  } else if (code === 'challenge_expired') {
    challengeState.value = 'expired'
    clearTimers()
  } else if (code === 'delivery_unsupported') {
    challengeId.value = ''
    challengeOwner = null
    challengeState.value = 'failed'
    otpStep.value = 'phone'
    otpCode.value = ''
    canResend.value = false
    clearTimers()
  } else if (code === 'resend_cooldown' && retry && retry > 0) {
    startResendCountdown(retry)
  }
}

function applySentCodeResponse(data: {
  challenge_id: string
  delivery_channel?: string
  code_length?: number | null
  next_delivery_channel?: string | null
  can_resend?: boolean
  resend_wait_seconds?: number
  phone?: string
}): void {
  challengeId.value = data.challenge_id
  if (data.phone) phone.value = data.phone
  challengeState.value = 'code_sent'
  otpStep.value = 'code'
  deliveryChannel.value = data.delivery_channel || 'unknown'
  codeLength.value = typeof data.code_length === 'number' ? data.code_length : null
  nextDeliveryChannel.value = data.next_delivery_channel || null
  canResend.value = Boolean(data.can_resend && data.next_delivery_channel)
  startResendCountdown(canResend.value ? data.resend_wait_seconds || 0 : 0)
  politeStatus.value = `验证码已通过${deliveryChannelLabel.value}发送`
  void nextTick(() => otpInputRef.value?.focus())
}

async function handleSendOtp(): Promise<void> {
  if (sending.value || submitting.value) return
  if (!validatePhoneField()) {
    await nextTick(() => phoneInputRef.value?.focus())
    return
  }
  const seq = ++sessionSeq
  const owner = currentOwner()
  sending.value = true
  bannerError.value = ''
  bannerErrorCode.value = ''
  try {
    const cleanPhone = phone.value.trim()
    const res =
      owner.mode === 'native'
        ? await tgStore.nativeSendCode(cleanPhone)
        : owner.accountId
          ? await accountStore.sendAccountCode(owner.accountId, cleanPhone)
          : null
    if (!isCurrentStart(seq, owner, 'otp')) {
      if (res) void cancelChallenge(owner, res.challenge_id)
      return
    }
    if (res) {
      challengeOwner = owner
      applySentCodeResponse(res)
    }
  } catch (err) {
    if (seq !== sessionSeq || !props.active) return
    applyServerError(err)
  } finally {
    if (seq === sessionSeq) sending.value = false
  }
}

async function handleResendOtp(): Promise<void> {
  if (sending.value || submitting.value || !challengeId.value || !canResend.value || resendCountdown.value > 0) return
  const seq = sessionSeq
  sending.value = true
  bannerError.value = ''
  bannerErrorCode.value = ''
  try {
    const res =
      props.mode === 'native'
        ? await tgStore.nativeResendCode(challengeId.value)
        : props.accountId
          ? await accountStore.resendAccountCode(props.accountId, challengeId.value)
          : null
    if (seq !== sessionSeq || !props.active || method.value !== 'otp') return
    if (res) {
      applySentCodeResponse(res)
    }
  } catch (err) {
    if (seq !== sessionSeq || !props.active) return
    applyServerError(err)
  } finally {
    if (seq === sessionSeq) sending.value = false
  }
}

async function handleOtpSubmit(): Promise<void> {
  if (submitting.value || !challengeId.value) return
  if (challengeState.value === 'password_required') {
    if (!validatePasswordField()) {
      await nextTick(() => pwdInputRef.value?.focus())
      return
    }
  } else if (!validateOtpField()) {
    await nextTick(() => otpInputRef.value?.focus())
    return
  }

  const seq = sessionSeq
  submitting.value = true
  bannerError.value = ''
  bannerErrorCode.value = ''
  try {
    const cleanCode = otpCode.value.replace(/\D/g, '')
    const rawPassword = password.value ? password.value : null
    const res =
      challengeState.value === 'password_required'
        ? props.mode === 'native'
          ? await tgStore.nativeSubmitPassword(challengeId.value, password.value)
          : props.accountId
            ? await accountStore.submitAccountPassword(props.accountId, challengeId.value, password.value)
            : null
        : props.mode === 'native'
          ? await tgStore.nativeVerifyCode(cleanCode, rawPassword, challengeId.value)
          : props.accountId
            ? await accountStore.verifyAccountCode(props.accountId, {
                challenge_id: challengeId.value,
                code: cleanCode,
                password: rawPassword,
              })
            : null

    if (seq !== sessionSeq || !props.active) return
    if (res?.authenticated) {
      challengeState.value = 'authenticated'
      clearTimers()
      politeStatus.value = 'Telegram 账号授权成功'
      emit('authenticated')
      return
    }
    if (res?.state === 'password_required') {
      challengeState.value = 'password_required'
      otpError.value = ''
      politeStatus.value = '请输入两步验证云端密码'
      await nextTick(() => pwdInputRef.value?.focus())
    }
  } catch (err) {
    if (seq !== sessionSeq || !props.active) return
    applyServerError(err)
  } finally {
    if (seq === sessionSeq) submitting.value = false
  }
}

async function resetToPhoneStep(): Promise<void> {
  teardownSession(true)
  method.value = 'otp'
  otpStep.value = 'phone'
}

async function resetQrPhoneStep(): Promise<void> {
  teardownSession(true)
  await nextTick(() => phoneInputRef.value?.focus())
}

function isQrNetworkError(err: unknown): boolean {
  return errCode(err) === 'network_failure' || (isAxiosError(err) && !err.response && err.code !== 'ERR_CANCELED')
}

async function startQrFlow(): Promise<void> {
  if (qrStarting.value) return
  const refreshChallengeId = props.mode === 'native' ? challengeId.value : ''
  if (!refreshChallengeId && needsQrPhoneConfirmation.value && !validatePhoneField()) {
    await nextTick(() => phoneInputRef.value?.focus())
    return
  }
  const seq = ++sessionSeq
  const owner = currentOwner()
  const previousQrUrl = qrUrl.value
  const previousQrDeadline = Date.now() + qrRemainingSeconds.value * 1000
  clearTimers()
  qrStarting.value = true
  bannerError.value = ''
  bannerErrorCode.value = ''
  passwordError.value = ''
  password.value = ''
  qrUrl.value = ''

  try {
    if (!refreshChallengeId) await bestEffortCancelActiveChallenge()
    if (!isCurrentStart(seq, owner, 'qr')) return
    const payload = refreshChallengeId
      ? { refresh_challenge_id: refreshChallengeId }
      : { phone: phone.value.trim() || null }
    const res =
      owner.mode === 'native'
        ? await tgStore.nativeQrStart(payload)
        : owner.accountId
          ? await accountStore.startAccountQr(owner.accountId)
          : null
    if (!isCurrentStart(seq, owner, 'qr')) {
      if (res) void cancelChallenge(owner, res.challenge_id)
      return
    }
    if (res) {
      challengeOwner = owner
      challengeId.value = res.challenge_id
      qrUrl.value = res.qr_url
      qrPhoneHint.value =
        'target_phone_hint' in res && typeof res.target_phone_hint === 'string' ? res.target_phone_hint : ''
      challengeState.value = 'qr_pending'
      startQrCountdown(res.remaining_seconds || 30)
      politeStatus.value = '登录二维码已就绪，请使用手机 Telegram 扫描'
      scheduleQrPoll(seq)
    }
  } catch (err) {
    if (!isCurrentStart(seq, owner, 'qr')) return
    if (refreshChallengeId && isQrNetworkError(err)) {
      applyServerError(err)
      qrUrl.value = previousQrUrl
      challengeState.value = 'qr_pending'
      startQrCountdown((previousQrDeadline - Date.now()) / 1000)
      void pollQrStatus(seq, true)
      return
    }
    challengeState.value = 'failed'
    applyServerError(err)
    const sourceUnavailable = ['challenge_not_found', 'challenge_invalid', 'challenge_stale', 'challenge_cancelled']
    if (refreshChallengeId && sourceUnavailable.includes(bannerErrorCode.value)) {
      void bestEffortCancelActiveChallenge()
      qrPhoneHint.value = ''
      await nextTick(() => phoneInputRef.value?.focus())
    }
  } finally {
    if (seq === sessionSeq) qrStarting.value = false
  }
}

function scheduleQrPoll(seq: number, retryTransient = false): void {
  if (qrPollTimer) {
    clearTimeout(qrPollTimer)
    qrPollTimer = null
  }
  if (seq !== sessionSeq || !props.active || method.value !== 'qr' || challengeState.value !== 'qr_pending') {
    return
  }
  qrPollTimer = setTimeout(() => {
    void pollQrStatus(seq, retryTransient)
  }, 1500)
}

async function pollQrStatus(seq: number, retryTransient = false): Promise<void> {
  if (
    seq !== sessionSeq ||
    !props.active ||
    method.value !== 'qr' ||
    challengeState.value !== 'qr_pending' ||
    !challengeId.value
  ) {
    return
  }
  try {
    const res =
      props.mode === 'native'
        ? await tgStore.nativeQrStatus(challengeId.value)
        : props.accountId
          ? await accountStore.getAccountQrStatus(props.accountId, challengeId.value)
          : null
    if (seq !== sessionSeq || !props.active || method.value !== 'qr') return
    if (!res) return

    if (res.authenticated || res.state === 'authenticated') {
      challengeState.value = 'authenticated'
      clearTimers()
      politeStatus.value = '扫码登录授权成功'
      emit('authenticated')
      return
    }
    if (res.state === 'password_required') {
      challengeState.value = 'password_required'
      clearTimers()
      politeStatus.value = '扫码已确认，请输入两步验证密码'
      await nextTick(() => pwdInputRef.value?.focus())
      return
    }
    if (res.state === 'expired' || res.state === 'cancelled' || res.state === 'failed') {
      challengeState.value = res.state
      clearTimers()
      bannerErrorCode.value = res.error_code || ''
      bannerError.value = res.message || '扫码登录未完成，请刷新二维码重试'
      return
    }
    if (typeof res.remaining_seconds === 'number') {
      startQrCountdown(res.remaining_seconds)
    }
    scheduleQrPoll(seq)
  } catch (err) {
    if (seq !== sessionSeq || !props.active) return
    if (retryTransient && qrRemainingSeconds.value > 0 && isQrNetworkError(err)) {
      scheduleQrPoll(seq, true)
      return
    }
    clearTimers()
    challengeState.value = 'failed'
    applyServerError(err)
  }
}

async function handleQrPasswordSubmit(): Promise<void> {
  if (submitting.value || !challengeId.value) return
  if (!validatePasswordField()) {
    await nextTick(() => pwdInputRef.value?.focus())
    return
  }
  const seq = sessionSeq
  submitting.value = true
  bannerError.value = ''
  bannerErrorCode.value = ''
  try {
    const res =
      props.mode === 'native'
        ? await tgStore.nativeQrPassword(challengeId.value, password.value)
        : props.accountId
          ? await accountStore.submitAccountQrPassword(props.accountId, challengeId.value, password.value)
          : null
    if (seq !== sessionSeq || !props.active) return
    if (res?.authenticated) {
      challengeState.value = 'authenticated'
      clearTimers()
      politeStatus.value = '扫码登录授权成功'
      emit('authenticated')
    }
  } catch (err) {
    if (seq !== sessionSeq || !props.active) return
    applyServerError(err)
  } finally {
    if (seq === sessionSeq) submitting.value = false
  }
}

async function switchMethod(target: 'otp' | 'qr'): Promise<void> {
  if (method.value === target || submitting.value) return
  teardownSession(true)
  method.value = target
  if (target === 'qr' && props.active) {
    if (needsQrPhoneConfirmation.value) {
      await nextTick(() => phoneInputRef.value?.focus())
      return
    }
    await startQrFlow()
  }
}

function onTabKeydown(e: KeyboardEvent): void {
  if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
    e.preventDefault()
    void switchMethod(method.value === 'otp' ? 'qr' : 'otp')
  }
}

function handleUserCancel(): void {
  teardownSession(false)
  emit('cancel')
}

defineExpose({
  method,
  otpStep,
  challengeId,
  challengeState,
  phone,
  otpCode,
  password,
  handleSendOtp,
  handleResendOtp,
  handleOtpSubmit,
  startQrFlow,
  handleQrPasswordSubmit,
  switchMethod,
  teardownSession,
})
</script>

<style scoped>
.tg-challenge-panel {
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.visually-hidden {
  position: absolute !important;
  clip-path: inset(50%) !important;
  overflow: hidden !important;
  width: 1px !important;
  height: 1px !important;
  margin: -1px !important;
  padding: 0 !important;
  border: 0 !important;
  white-space: nowrap !important;
}

.method-tablist {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 0.5rem;
  padding: 0.25rem;
  border-radius: 10px;
  background: var(--bg-card-hover, rgba(128, 128, 128, 0.1));
  border: 1px solid var(--border-color, rgba(128, 128, 128, 0.2));
}

.method-tab {
  min-height: 40px;
  padding: 0.45rem 0.75rem;
  border: none;
  border-radius: 8px;
  background: transparent;
  color: var(--text-secondary, #6b7280);
  font: inherit;
  font-size: 0.9rem;
  font-weight: 600;
  cursor: pointer;
}

.method-tab.is-active {
  background: var(--bg-card, #ffffff);
  color: var(--text-primary, #111827);
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.12);
}

.method-tab:focus-visible,
.primary-btn:focus-visible,
.secondary-btn:focus-visible,
.link-action-btn:focus-visible,
.toggle-pwd-btn:focus-visible,
.field-input:focus-visible {
  outline: 3px solid var(--accent-color, #2563eb);
  outline-offset: 2px;
}

.challenge-alert {
  padding: 0.65rem 0.85rem;
  border-radius: 8px;
  border: 1px solid rgba(239, 68, 68, 0.45);
  background: rgba(239, 68, 68, 0.1);
  color: #dc2626;
  font-size: 0.86rem;
  line-height: 1.45;
}

.channel-info-box {
  background: var(--bg-card-hover, rgba(128, 128, 128, 0.08));
  border: 1px solid var(--border-color, rgba(128, 128, 128, 0.2));
  border-left: 3px solid var(--accent-color, #2563eb);
  border-radius: 6px;
  padding: 0.65rem 0.85rem;
  margin-bottom: 1rem;
  font-size: 0.84rem;
  line-height: 1.5;
}

.channel-title,
.channel-meta {
  margin: 0;
}

.channel-meta {
  color: var(--text-secondary, #6b7280);
  margin-top: 0.2rem;
}

.field-group {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
  margin-bottom: 1rem;
}

.field-label {
  font-size: 0.9rem;
  font-weight: 600;
  color: var(--text-primary);
}

.field-hint {
  margin: 0;
  font-size: 0.82rem;
  color: var(--text-secondary);
  line-height: 1.4;
}

.field-input {
  width: 100%;
  min-height: 48px;
  padding: 0.6rem 0.8rem;
  box-sizing: border-box;
  border-radius: 8px;
  border: 1px solid var(--border-color, #6b7280);
  background: var(--bg-input, var(--bg-card, transparent));
  color: var(--text-primary);
  font: inherit;
  font-size: max(1rem, 1em);
}

.field-input[aria-invalid='true'] {
  border-color: #dc2626;
}

.otp-input {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  letter-spacing: 0.25em;
}

.password-row {
  display: flex;
  gap: 0.5rem;
  align-items: stretch;
}

.password-row .field-input {
  flex: 1;
}

.toggle-pwd-btn {
  min-height: 48px;
  min-width: 60px;
  padding: 0 0.75rem;
  border-radius: 8px;
  border: 1px solid var(--border-color, #6b7280);
  background: var(--bg-card-hover, transparent);
  color: var(--text-primary);
  font: inherit;
  font-size: 0.85rem;
  cursor: pointer;
}

.field-error {
  margin: 0;
  font-size: 0.82rem;
  color: #dc2626;
}

.primary-btn {
  width: 100%;
  min-height: 48px;
  padding: 0.65rem 1rem;
  border: none;
  border-radius: 8px;
  background: var(--accent-color, #2563eb);
  color: #ffffff;
  font: inherit;
  font-size: 0.95rem;
  font-weight: 600;
  cursor: pointer;
}

.primary-btn:disabled {
  opacity: 0.55;
  cursor: not-allowed;
}

.secondary-actions {
  display: flex;
  gap: 0.6rem;
  margin-top: 0.75rem;
}

.secondary-btn {
  flex: 1;
  min-height: 44px;
  padding: 0.5rem 0.9rem;
  border-radius: 8px;
  border: 1px solid var(--border-color, #6b7280);
  background: transparent;
  color: var(--text-primary);
  font: inherit;
  font-size: 0.9rem;
  cursor: pointer;
}

.secondary-btn:disabled {
  opacity: 0.55;
  cursor: not-allowed;
}

.resend-section {
  margin-top: 0.85rem;
  text-align: center;
}

.link-action-btn {
  background: none;
  border: none;
  padding: 0.35rem 0.5rem;
  min-height: 36px;
  font: inherit;
  font-size: 0.86rem;
  color: var(--accent-color, #2563eb);
  text-decoration: underline;
  cursor: pointer;
}

.link-action-btn:disabled {
  color: var(--text-muted, #9ca3af);
  text-decoration: none;
  cursor: not-allowed;
}

.no-fallback-hint {
  margin: 0;
  font-size: 0.8rem;
  color: var(--text-secondary);
}

.qr-scan-stage {
  display: flex;
  flex-direction: column;
  align-items: stretch;
  gap: 0.75rem;
}

.qr-preview-wrap {
  display: flex;
  justify-content: center;
  align-items: center;
  min-height: 224px;
}

.qr-loading,
.qr-expired-box {
  padding: 2rem 1rem;
  text-align: center;
  font-size: 0.88rem;
  color: var(--text-secondary);
}

.qr-countdown {
  margin: 0;
  text-align: center;
  font-size: 0.86rem;
  color: var(--text-secondary);
}
</style>
