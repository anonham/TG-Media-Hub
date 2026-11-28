/**
 * 让渡主线程执行权 (INP 交互延迟优化):
 * 优先调用现代浏览器 scheduler.yield() 将剩余微任务排入渲染管线前列,
 * 降级使用 Promise + setTimeout(0) 兜底。
 */
export async function yieldToMain(): Promise<void> {
  if (typeof window !== 'undefined' && 'scheduler' in window && 'yield' in (window as any).scheduler) {
    return await (window as any).scheduler.yield()
  }
  return new Promise((resolve) => setTimeout(resolve, 0))
}
