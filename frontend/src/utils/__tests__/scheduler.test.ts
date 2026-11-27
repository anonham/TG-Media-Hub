import { describe, expect, it, vi } from 'vitest'
import { yieldToMain } from '../scheduler'

describe('scheduler utility tests', () => {
  it('falls back to setTimeout when scheduler.yield is unavailable', async () => {
    const originalScheduler = (window as any).scheduler
    delete (window as any).scheduler

    let order = 0
    const p = yieldToMain().then(() => {
      order = 1
    })

    expect(order).toBe(0)
    await p
    expect(order).toBe(1)

    ;(window as any).scheduler = originalScheduler
  })

  it('uses native scheduler.yield when available', async () => {
    const mockYield = vi.fn().mockResolvedValue(undefined)
    ;(window as any).scheduler = { yield: mockYield }

    await yieldToMain()
    expect(mockYield).toHaveBeenCalledTimes(1)

    delete (window as any).scheduler
  })
})
