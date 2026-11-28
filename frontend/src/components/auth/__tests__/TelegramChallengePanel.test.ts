import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { nextTick } from 'vue'
import TelegramChallengePanel from '../TelegramChallengePanel.vue'
import { useTelegramAuthStore } from '@/stores/telegramAuthStore'
import { useAccountStore } from '@/stores/accountStore'

vi.mock('@/utils/discreteApi', () => ({
  message: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
  dialog: { warning: vi.fn() },
}))

function deferred<T>() {
  let resolve!: (val: T) => void
  const promise = new Promise<T>((r) => {
    resolve = r
  })
  return { promise, resolve }
}

describe('TelegramChallengePanel UI & race regressions', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    localStorage.clear()
    setActivePinia(createPinia())
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('uses a single numeric text OTP input preserving leading zeros and hides countdown without fallback', async () => {
    const tgStore = useTelegramAuthStore()
    const sendSpy = vi.spyOn(tgStore, 'nativeSendCode').mockResolvedValue({
      status: 'ok',
      challenge_id: 'cid-otp-1',
      method: 'otp',
      state: 'code_sent',
      delivery_channel: 'sms',
      code_length: 5,
      next_delivery_channel: null,
      can_resend: false,
      resend_wait_seconds: 0,
      expires_at: Date.now() / 1000 + 300,
      message: '已通过短信发送',
    })
    const verifySpy = vi.spyOn(tgStore, 'nativeVerifyCode').mockResolvedValue({
      status: 'ok',
      state: 'authenticated',
      authenticated: true,
      challenge_id: 'cid-otp-1',
      message: 'ok',
      token: 'jwt-ok',
    })

    const wrapper = mount(TelegramChallengePanel, {
      props: { mode: 'native', active: true, initialPhone: '+12025550117' },
    })

    await wrapper.find('form').trigger('submit.prevent')
    await nextTick()
    expect(sendSpy).toHaveBeenCalledWith('+12025550117')

    const otpInputs = wrapper.findAll('input[autocomplete="one-time-code"]')
    expect(otpInputs).toHaveLength(1)
    expect(otpInputs[0].attributes('type')).toBe('text')
    expect(otpInputs[0].attributes('inputmode')).toBe('numeric')
    expect(wrapper.text()).toContain('手机短信')
    expect(wrapper.text()).toContain('5 位')
    expect(wrapper.text()).toContain('未提供备用重发渠道')

    await otpInputs[0].setValue('00421')
    await wrapper.find('form').trigger('submit.prevent')
    expect(verifySpy).toHaveBeenCalledWith('00421', null, 'cid-otp-1')
    expect(wrapper.emitted('authenticated')).toHaveLength(1)

    wrapper.unmount()
  })

  it('renders QR locally as SVG, transitions to conditional 2FA password, and stops polling on close', async () => {
    const accountStore = useAccountStore()
    const qrStartSpy = vi.spyOn(accountStore, 'startAccountQr').mockResolvedValue({
      status: 'ok',
      challenge_id: 'cid-qr-1',
      account_id: 2,
      method: 'qr',
      state: 'qr_pending',
      qr_url: 'tg://login?token=local_only_token',
      expires_at: Date.now() / 1000 + 30,
      remaining_seconds: 30,
      message: '请扫码',
    })
    const qrPollSpy = vi.spyOn(accountStore, 'getAccountQrStatus').mockResolvedValue({
      status: 'password_required',
      challenge_id: 'cid-qr-1',
      account_id: 2,
      state: 'password_required',
      authenticated: false,
      remaining_seconds: 120,
      message: '请输入 2FA 密码',
    })
    const pwdSpy = vi.spyOn(accountStore, 'submitAccountQrPassword').mockResolvedValue({
      status: 'ok',
      state: 'authenticated',
      authenticated: true,
      challenge_id: 'cid-qr-1',
      message: 'ok',
      account_id: 2,
    })
    const cancelSpy = vi.spyOn(accountStore, 'cancelAccountChallenge').mockResolvedValue()

    const wrapper = mount(TelegramChallengePanel, {
      props: { mode: 'account', accountId: 2, initialPhone: '+12025550119', phoneLocked: true, active: true },
    })

    // Switch to QR tab -> local <svg> rendered, no external <img>
    await wrapper.findAll('[role="tab"]')[1].trigger('click')
    await flushPromises()
    expect(qrStartSpy).toHaveBeenCalledWith(2)
    expect(wrapper.find('svg.qr-svg').exists()).toBe(true)
    expect(wrapper.find('img').exists()).toBe(false)

    // Poll runs and transitions to password_required
    await vi.advanceTimersByTimeAsync(1500)
    await flushPromises()
    expect(qrPollSpy).toHaveBeenCalledWith(2, 'cid-qr-1')
    const pwdInput = wrapper.find('input[autocomplete="current-password"]')
    expect(pwdInput.exists()).toBe(true)

    // Closing modal before submitting cancels active challenge
    await wrapper.setProps({ active: false })
    await flushPromises()
    expect(cancelSpy).toHaveBeenCalledWith(2, 'cid-qr-1')

    // Re-open, trigger QR + password submission
    await wrapper.setProps({ active: true })
    await wrapper.findAll('[role="tab"]')[1].trigger('click')
    await flushPromises()
    await vi.advanceTimersByTimeAsync(1500)
    await flushPromises()
    await wrapper.find('input[autocomplete="current-password"]').setValue('  my pass  ')
    await wrapper.find('form').trigger('submit.prevent')
    await flushPromises()
    expect(pwdSpy).toHaveBeenCalledWith(2, 'cid-qr-1', '  my pass  ')
    expect(wrapper.emitted('authenticated')).toHaveLength(1)

    wrapper.unmount()
  })

  it('discards stale async send-code responses when switching accounts', async () => {
    const accountStore = useAccountStore()
    const pending = deferred<{
      status: string
      account_id: number
      phone: string
      challenge_id: string
      method: string
      state: string
      delivery_channel: string
      can_resend: boolean
      resend_wait_seconds: number
      expires_at: number
      message: string
    }>()
    vi.spyOn(accountStore, 'sendAccountCode').mockReturnValue(pending.promise as never)
    const cancelSpy = vi.spyOn(accountStore, 'cancelAccountChallenge').mockResolvedValue()

    const wrapper = mount(TelegramChallengePanel, {
      props: { mode: 'account', accountId: 1, initialPhone: '+12025550117', active: true },
    })
    const sendPromise = wrapper.find('form').trigger('submit.prevent')

    await wrapper.setProps({ accountId: 2, initialPhone: '+12025550119' })
    pending.resolve({
      status: 'ok',
      account_id: 1,
      phone: '+12025550117',
      challenge_id: 'stale-cid',
      method: 'otp',
      state: 'code_sent',
      delivery_channel: 'app',
      can_resend: false,
      resend_wait_seconds: 0,
      expires_at: 9999999999,
      message: 'ok',
    })
    await sendPromise
    await nextTick()

    expect((wrapper.vm as unknown as { challengeId: string }).challengeId).toBe('')
    expect((wrapper.vm as unknown as { otpStep: string }).otpStep).toBe('phone')
    expect(cancelSpy).toHaveBeenCalledWith(1, 'stale-cid')

    wrapper.unmount()
  })

  it('keeps a reopened QR challenge when cancellation of its predecessor finishes', async () => {
    const accountStore = useAccountStore()
    const cancel = deferred<void>()
    const cancelSpy = vi.spyOn(accountStore, 'cancelAccountChallenge').mockReturnValue(cancel.promise)
    const qr = {
      status: 'ok',
      account_id: 1,
      method: 'qr',
      state: 'qr_pending',
      qr_url: 'tg://login?token=local_only_token',
      expires_at: Date.now() / 1000 + 30,
      remaining_seconds: 30,
      message: '请扫码',
    }
    const startSpy = vi
      .spyOn(accountStore, 'startAccountQr')
      .mockResolvedValueOnce({ ...qr, challenge_id: 'old-qr' })
      .mockResolvedValueOnce({ ...qr, account_id: 2, challenge_id: 'new-qr' })
    const wrapper = mount(TelegramChallengePanel, {
      props: { mode: 'account', active: true, accountId: 1 },
    })
    const vm = wrapper.vm as unknown as { challengeId: string; startQrFlow(): Promise<void> }

    await wrapper.findAll('[role="tab"]')[1].trigger('click')
    await flushPromises()
    const refresh = vm.startQrFlow()
    // A parallel refresh must stop before it can create another challenge.
    await vm.startQrFlow()
    expect(startSpy).toHaveBeenCalledTimes(1)
    expect(cancelSpy).toHaveBeenCalledWith(1, 'old-qr')
    await wrapper.setProps({ active: false })
    await wrapper.setProps({ active: true, accountId: 2 })
    await wrapper.findAll('[role="tab"]')[1].trigger('click')
    await flushPromises()
    expect(vm.challengeId).toBe('new-qr')

    cancel.resolve()
    await refresh
    expect(startSpy).toHaveBeenCalledTimes(2)
    expect(vm.challengeId).toBe('new-qr')
    expect(wrapper.find('svg.qr-svg').exists()).toBe(true)
    wrapper.unmount()
  })

  it('requires explicit phone before generating a QR for the deployment-configured target', async () => {
    const tgStore = useTelegramAuthStore()
    tgStore.bindingMode = 'first_bind'
    tgStore.defaultPhoneConfigured = true
    const qrStartSpy = vi.spyOn(tgStore, 'nativeQrStart').mockResolvedValue({
      status: 'ok',
      challenge_id: 'cid-nat-qr',
      account_id: 1,
      method: 'qr',
      state: 'qr_pending',
      qr_url: 'tg://login?token=native_bootstrap_qr',
      target_phone_hint: '•••• 1111',
      expires_at: Date.now() / 1000 + 30,
      remaining_seconds: 30,
      message: '请扫码',
    })

    const wrapper = mount(TelegramChallengePanel, {
      props: { mode: 'native', active: true, initialPhone: '', phoneLocked: false },
    })

    // Switching to QR with empty phone stays on phone prompt and makes zero QR API calls
    await wrapper.findAll('[role="tab"]')[1].trigger('click')
    await flushPromises()
    expect(qrStartSpy).not.toHaveBeenCalled()
    expect(wrapper.find('input[name="qr_phone"]').exists()).toBe(true)

    // Submitting empty shows validation error without RPC
    await wrapper.find('form').trigger('submit.prevent')
    await flushPromises()
    expect(qrStartSpy).not.toHaveBeenCalled()
    expect(wrapper.text()).toContain('请输入带国际区号的手机号')

    // Supplying phone submits nativeQrStart with trimmed phone and renders SVG QR
    await wrapper.find('input[name="qr_phone"]').setValue('+12025550117')
    await wrapper.find('form').trigger('submit.prevent')
    await flushPromises()
    expect(qrStartSpy).toHaveBeenCalledWith({ phone: '+12025550117' })
    expect(wrapper.find('svg.qr-svg').exists()).toBe(true)
    expect(wrapper.text()).toContain('•••• 1111')
    expect(wrapper.text()).not.toContain('更换手机号')

    wrapper.unmount()
  })

  it('resumes the original QR after a network failure and claims its scan', async () => {
    const store = useTelegramAuthStore()
    const start = vi.spyOn(store, 'nativeQrStart').mockResolvedValueOnce({
      status: 'ok',
      challenge_id: 'qr-original',
      qr_url: 'tg://login?token=original',
      expires_at: Date.now() / 1000 + 30,
      remaining_seconds: 30,
      message: '请扫码',
    })
    start.mockRejectedValueOnce({ response: { data: { error_code: 'network_failure', detail: '网络异常' } } })
    const poll = vi
      .spyOn(store, 'nativeQrStatus')
      .mockResolvedValueOnce({
        status: 'ok',
        challenge_id: 'qr-original',
        state: 'qr_pending',
        authenticated: false,
        remaining_seconds: 20,
        message: '等待扫码',
      })
      .mockResolvedValueOnce({
        status: 'ok',
        challenge_id: 'qr-original',
        state: 'authenticated',
        authenticated: true,
        message: '完成',
      })
    const wrapper = mount(TelegramChallengePanel, {
      props: { mode: 'native', active: true, initialPhone: '+12025550117', phoneLocked: true },
    })
    await wrapper.findAll('[role="tab"]')[1].trigger('click')
    await flushPromises()
    await wrapper.find('.secondary-actions .primary-btn').trigger('click')
    await flushPromises()
    expect(start).toHaveBeenLastCalledWith({ refresh_challenge_id: 'qr-original' })
    expect(poll).toHaveBeenCalledWith('qr-original')
    expect(wrapper.find('svg.qr-svg').exists()).toBe(true)
    await vi.advanceTimersByTimeAsync(1500)
    expect(poll).toHaveBeenCalledTimes(2)
    expect(wrapper.emitted('authenticated')).toHaveLength(1)
    wrapper.unmount()
  })

  it('confirms the replacement phone after OTP and refreshes the expired QR by challenge ID', async () => {
    const tgStore = useTelegramAuthStore()
    tgStore.bindingMode = 'first_bind'
    const oldPhone = '+12025550117'
    const newPhone = '+12025550119'
    vi.spyOn(tgStore, 'nativeSendCode').mockResolvedValue({
      status: 'ok',
      challenge_id: 'otp-old-phone',
      expires_at: Date.now() / 1000 + 300,
      message: '验证码已下发',
    })
    const cancelSpy = vi.spyOn(tgStore, 'nativeCancelChallenge').mockResolvedValue(undefined)
    const qr = {
      status: 'ok',
      qr_url: 'tg://login?token=test_qr_token',
      target_phone_hint: '•••• 2222',
      expires_at: Date.now() / 1000 + 30,
      remaining_seconds: 1,
      message: '请扫码',
    }
    const start = vi.spyOn(tgStore, 'nativeQrStart')
    start.mockResolvedValueOnce({ ...qr, challenge_id: 'qr-confirmed' })
    start.mockResolvedValueOnce({ ...qr, challenge_id: 'qr-refreshed' })
    const poll = vi.spyOn(tgStore, 'nativeQrStatus')
    poll.mockResolvedValueOnce({
      challenge_id: 'qr-confirmed',
      state: 'expired',
      message: '二维码已过期',
    })
    poll.mockResolvedValueOnce({
      challenge_id: 'qr-refreshed',
      state: 'authenticated',
      authenticated: true,
      message: '扫码登录授权成功',
    })

    const wrapper = mount(TelegramChallengePanel, { props: { mode: 'native', initialPhone: oldPhone } })
    await wrapper.get('form').trigger('submit.prevent')
    await flushPromises()
    await wrapper.findAll('[role="tab"]')[1].trigger('click')
    expect(cancelSpy).toHaveBeenCalledWith('otp-old-phone')
    expect(start).not.toHaveBeenCalled()
    const field = wrapper.get<HTMLInputElement>('input[name="qr_phone"]')
    expect(field.element.value).toBe(oldPhone)
    await field.setValue(newPhone)
    await wrapper.setProps({ initialPhone: '+12025550120' })
    await wrapper.get('form').trigger('submit.prevent')
    await flushPromises()
    expect(start).toHaveBeenNthCalledWith(1, { phone: newPhone })
    expect(wrapper.text()).toContain('•••• 2222')
    expect(wrapper.text()).toContain('更换手机号')

    await vi.advanceTimersByTimeAsync(1500)
    expect(wrapper.text()).toContain('二维码已过期')
    await wrapper.get('.qr-scan-stage .primary-btn').trigger('click')
    await flushPromises()
    expect(start).toHaveBeenNthCalledWith(2, { refresh_challenge_id: 'qr-confirmed' })
    expect(cancelSpy).not.toHaveBeenCalledWith('qr-confirmed')
    expect(wrapper.text()).toContain('•••• 2222')
    await vi.advanceTimersByTimeAsync(1500)
    expect(poll).toHaveBeenLastCalledWith('qr-refreshed')
    expect(wrapper.emitted('authenticated')).toHaveLength(1)

    wrapper.unmount()
  })
})
