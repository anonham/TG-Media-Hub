// 安全剪贴板与文本导出统一封装。
// 局域网 HTTP (http://192.168.x.x) 下 navigator.clipboard 为 undefined 或 writeText
// 被拒 (非安全上下文), 必须回退 <textarea> + execCommand —— 替换此前 6 处分散实现,
// 消除"局域网点击复制直接崩溃"的隐患。
import { message } from '@/utils/discreteApi'

function execCommandCopy(text: string): boolean {
  const textArea = document.createElement('textarea')
  textArea.value = text
  textArea.style.position = 'fixed'
  textArea.style.left = '-9999px'
  textArea.style.top = '-9999px'
  textArea.setAttribute('readonly', '')
  // 记录焦点来源, 复制后恢复 —— 移除临时 textarea 不会自动还焦点,
  // 键盘用户会丢失复制前的位置
  const previousActive = document.activeElement
  document.body.appendChild(textArea)
  textArea.focus()
  textArea.select()
  const successful = document.execCommand('copy')
  document.body.removeChild(textArea)
  if (previousActive instanceof HTMLElement) {
    previousActive.focus()
  }
  return successful
}

/**
 * 复制文本到剪贴板: 优先 Clipboard API, 非安全上下文自动回退 execCommand。
 * 成功且有 successMsg 时 toast 成功; 失败统一 toast 提示手动复制 (硬规则 5, 不静默)。
 * @returns 是否复制成功 (供调用方分支, 如失败保持弹窗开启)
 */
export async function copyTextToClipboard(text: string, successMsg?: string): Promise<boolean> {
  try {
    if (navigator?.clipboard?.writeText) {
      try {
        await navigator.clipboard.writeText(text)
        if (successMsg) message.success(successMsg)
        return true
      } catch {
        // 非安全上下文 / 权限拒绝 → 落入 execCommand 回退
      }
    }
    if (execCommandCopy(text)) {
      if (successMsg) message.success(successMsg)
      return true
    }
  } catch (err) {
    console.error('复制到剪贴板失败:', err)
  }
  message.error('复制失败，请手动复制')
  return false
}

/**
 * 导出 UTF-8 文本文件 (Blob + 临时 <a> 下载, 用后即释放 objectURL)。
 * 成功 toast 交由调用方 (各自携带条数/文件名等上下文)。
 */
export function exportTextFile(content: string, filename: string): void {
  const blob = new Blob([content], { type: 'text/plain;charset=utf-8' })
  const a = document.createElement('a')
  a.href = URL.createObjectURL(blob)
  a.download = filename
  a.click()
  URL.revokeObjectURL(a.href)
}
