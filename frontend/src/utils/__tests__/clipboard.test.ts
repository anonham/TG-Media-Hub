import { afterEach, describe, expect, it, vi } from 'vitest'
import { copyTextToClipboard } from '../clipboard'

vi.mock('@/utils/discreteApi', () => ({
  message: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}))

describe('clipboard utility', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    document.body.innerHTML = ''
  })

  it('removes temporary textarea and restores focus when execCommand throws', async () => {
    const btn = document.createElement('button')
    document.body.appendChild(btn)
    btn.focus()
    expect(document.activeElement).toBe(btn)

    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: undefined,
    })
    Object.defineProperty(document, 'execCommand', {
      configurable: true,
      value: vi.fn(() => {
        throw new Error('execCommand blocked')
      }),
    })

    const result = await copyTextToClipboard('hello')
    expect(result).toBe(false)
    expect(document.querySelector('textarea')).toBeNull()
    expect(document.activeElement).toBe(btn)
  })
})
