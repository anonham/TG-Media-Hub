/**
 * ARIA tabs 键盘方向导航 (WAI-ARIA APG tabs pattern 补全, 现代 Web P3)
 *
 * 绑定在 role="tablist" 容器的 @keydown 上: ArrowLeft / ArrowRight 在各
 * role="tab" 间循环移动焦点并自动激活 (selection follows focus, 标签数量少
 * 同步激活代价可忽略), Home / End 跳转首尾。
 * 其余按键 (含 Tab 键) 不拦截 —— Tab 键保持浏览器默认的 tablist 焦点进出。
 */
const NAV_KEYS = new Set(['ArrowLeft', 'ArrowRight', 'Home', 'End'])

export function handleTablistArrowNav(event: KeyboardEvent): void {
  if (!NAV_KEYS.has(event.key)) return
  const container = event.currentTarget
  if (!(container instanceof HTMLElement)) return
  const tabs = Array.from(container.querySelectorAll<HTMLButtonElement>('[role="tab"]'))
  if (tabs.length === 0) return
  const activeIndex = tabs.findIndex((tab) => tab === document.activeElement)
  if (activeIndex < 0) return

  let nextIndex = activeIndex
  if (event.key === 'ArrowLeft') nextIndex = activeIndex > 0 ? activeIndex - 1 : tabs.length - 1
  if (event.key === 'ArrowRight') nextIndex = activeIndex < tabs.length - 1 ? activeIndex + 1 : 0
  if (event.key === 'Home') nextIndex = 0
  if (event.key === 'End') nextIndex = tabs.length - 1

  event.preventDefault()
  const next = tabs[nextIndex]
  if (!next) return
  next.focus()
  if (nextIndex !== activeIndex) {
    next.click()
  }
}
