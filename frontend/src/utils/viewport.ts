import { onMounted, onUnmounted, ref } from 'vue'

/**
 * 响应式视口断点订阅，保持与 CSS `@media (max-width: 768px)` 一致并在卸载时清理监听。
 */
export function useMobileViewport(breakpointPx = 768) {
  const query = `(max-width: ${breakpointPx}px)`
  const readIsMobile = (): boolean => {
    if (typeof window === 'undefined') return false
    return window.innerWidth <= breakpointPx
  }

  const isMobileViewport = ref(readIsMobile())
  let mediaQueryList: MediaQueryList | null = null

  const handleMediaChange = (event: MediaQueryListEvent) => {
    isMobileViewport.value = event.matches
  }

  const handleResize = () => {
    isMobileViewport.value = readIsMobile()
  }

  onMounted(() => {
    if (typeof window === 'undefined') return
    isMobileViewport.value = readIsMobile()
    if (typeof window.matchMedia === 'function') {
      mediaQueryList = window.matchMedia(query)
      mediaQueryList.addEventListener('change', handleMediaChange)
    }
    window.addEventListener('resize', handleResize)
  })

  onUnmounted(() => {
    if (mediaQueryList) {
      mediaQueryList.removeEventListener('change', handleMediaChange)
      mediaQueryList = null
    }
    if (typeof window !== 'undefined') {
      window.removeEventListener('resize', handleResize)
    }
  })

  return { isMobileViewport }
}
