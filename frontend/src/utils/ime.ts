/**
 * 输入法安全回车判定 (W3C / Modern Web Guidance: ime-safe-enter-submit)
 *
 * 中文/日文/韩文等输入法在候选词确认时会触发 key === 'Enter' 事件。
 * 原生规范定义 KeyboardEvent.isComposing 为 true，同时在部分浏览器/旧 WebKit 中
 * 会上报 event.keyCode === 229 (VK_PROCESSKEY)。
 * 结合两者可精确避免输入拼音选词敲回车时发生意外表单提交或搜索触发。
 */
export function isImeComposing(event: KeyboardEvent): boolean {
  return Boolean(event.isComposing || event.keyCode === 229)
}

/**
 * 包装回车事件处理器，当处于输入法组字期间时静默跳过；
 * 仅在非组字状态下调用 event.preventDefault() 阻止 textarea 换行或 form 隐式重复提交，
 * 并调用真实业务逻辑 (W3C / Modern Web Guidance: ime-safe-enter-submit)。
 */
export function onImeSafeEnter(event: KeyboardEvent, handler: () => void | Promise<void>): void | Promise<void> {
  if (event.key !== 'Enter') return
  if (isImeComposing(event)) return
  event.preventDefault()
  // 透传异步 handler 的结果 —— 丢弃会让调用方无法 await/捕获 rejection,
  // 产生 unhandled promise rejection
  return handler()
}
