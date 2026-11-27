import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import CoverageBar from '../CoverageBar.vue'

describe('CoverageBar component regressions', () => {
  it('preserves full range up to rangeEnd and shows truncation warning when raw boundary segments exceed 4000', () => {
    // 4100 disjoint single-ID forwarded intervals across 1..8200 produce > 8000 boundary points (> MAX_SEGMENTS=4000)
    const forwardedRuns = Array.from({ length: 4100 }, (_, i) => ({
      start: i * 2 + 1,
      end: i * 2 + 1,
    }))

    const wrapper = mount(CoverageBar, {
      props: {
        rangeStart: 1,
        rangeEnd: 8200,
        forwardedRuns,
        coverage: [{ start: 1, end: 8200 }],
        truncated: false,
      },
    })

    const segEls = wrapper.findAll('.coverage-seg')
    expect(segEls.length).toBeGreaterThan(0)
    expect(segEls.length).toBeLessThanOrEqual(4000)

    const lastAria = segEls[segEls.length - 1].attributes('aria-label') || ''
    expect(lastAria).toContain('#8200')
    expect(wrapper.find('.legend-warn').exists()).toBe(true)

    wrapper.unmount()
  })

  it('keeps dense segments interactive via click and keyboard events', async () => {
    const forwardedRuns = Array.from({ length: 400 }, (_, i) => ({
      start: i * 2 + 1,
      end: i * 2 + 1,
    }))
    const wrapper = mount(CoverageBar, {
      props: {
        rangeStart: 1,
        rangeEnd: 800,
        forwardedRuns,
        coverage: [{ start: 1, end: 800 }],
      },
    })

    const segEls = wrapper.findAll('.coverage-seg')
    expect(segEls.length).toBe(800)
    await segEls[0].trigger('click')
    await segEls[1].trigger('keydown.enter')
    expect(wrapper.emitted('select')).toEqual([
      [1, 1],
      [2, 2],
    ])

    wrapper.unmount()
  })
})
