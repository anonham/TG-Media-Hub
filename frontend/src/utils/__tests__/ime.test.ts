import { describe, expect, it, vi } from 'vitest'
import { isImeComposing, onImeSafeEnter } from '../ime'

describe('ime utility tests', () => {
  it('detects isComposing = true as IME active', () => {
    const event = new KeyboardEvent('keydown', { key: 'Enter' })
    Object.defineProperty(event, 'isComposing', { value: true })
    expect(isImeComposing(event)).toBe(true)
  })

  it('detects keyCode = 229 as IME active', () => {
    const event = new KeyboardEvent('keydown', { key: 'Enter', keyCode: 229 } as any)
    expect(isImeComposing(event)).toBe(true)
  })

  it('returns false for normal Enter key without IME', () => {
    const event = new KeyboardEvent('keydown', { key: 'Enter' })
    expect(isImeComposing(event)).toBe(false)
  })

  it('blocks handler and does not preventDefault when IME is composing', () => {
    const handler = vi.fn()
    const event = new KeyboardEvent('keydown', { key: 'Enter', cancelable: true })
    Object.defineProperty(event, 'isComposing', { value: true })

    onImeSafeEnter(event, handler)
    expect(handler).not.toHaveBeenCalled()
    expect(event.defaultPrevented).toBe(false)
  })

  it('triggers handler and prevents default implicit submission on clean Enter', () => {
    const handler = vi.fn()
    const event = new KeyboardEvent('keydown', { key: 'Enter', cancelable: true })

    onImeSafeEnter(event, handler)
    expect(handler).toHaveBeenCalledTimes(1)
    expect(event.defaultPrevented).toBe(true)
  })

  it('ignores non-Enter keys and does not preventDefault', () => {
    const handler = vi.fn()
    const event = new KeyboardEvent('keydown', { key: 'Tab', cancelable: true })

    onImeSafeEnter(event, handler)
    expect(handler).not.toHaveBeenCalled()
    expect(event.defaultPrevented).toBe(false)
  })
})
