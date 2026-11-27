import { describe, expect, it } from 'vitest'
import { handleTablistArrowNav } from '../tablistNavigation'

function buildTablist(count: number): { container: HTMLElement; buttons: HTMLButtonElement[] } {
  const container = document.createElement('div')
  container.setAttribute('role', 'tablist')
  const buttons: HTMLButtonElement[] = []
  for (let i = 0; i < count; i += 1) {
    const btn = document.createElement('button')
    btn.setAttribute('role', 'tab')
    btn.textContent = `tab-${i}`
    container.appendChild(btn)
    buttons.push(btn)
  }
  document.body.appendChild(container)
  return { container, buttons }
}

function pressKey(container: HTMLElement, key: string): KeyboardEvent {
  const event = new KeyboardEvent('keydown', { key, bubbles: true, cancelable: true })
  container.dispatchEvent(event)
  return event
}

describe('tablistNavigation', () => {
  it('ArrowRight moves focus forward and auto-activates (selection follows focus)', () => {
    const { container, buttons } = buildTablist(3)
    const clicked: string[] = []
    buttons[1].addEventListener('click', () => clicked.push('tab-1'))
    container.addEventListener('keydown', handleTablistArrowNav)
    buttons[0].focus()

    const event = pressKey(container, 'ArrowRight')

    expect(document.activeElement).toBe(buttons[1])
    expect(clicked).toEqual(['tab-1'])
    expect(event.defaultPrevented).toBe(true)
  })

  it('ArrowLeft wraps from first tab to last', () => {
    const { container, buttons } = buildTablist(3)
    container.addEventListener('keydown', handleTablistArrowNav)
    buttons[0].focus()

    pressKey(container, 'ArrowLeft')

    expect(document.activeElement).toBe(buttons[2])
  })

  it('ArrowRight wraps from last tab to first', () => {
    const { container, buttons } = buildTablist(3)
    container.addEventListener('keydown', handleTablistArrowNav)
    buttons[2].focus()

    pressKey(container, 'ArrowRight')

    expect(document.activeElement).toBe(buttons[0])
  })

  it('Home and End jump to first and last tab', () => {
    const { container, buttons } = buildTablist(3)
    container.addEventListener('keydown', handleTablistArrowNav)
    buttons[1].focus()

    pressKey(container, 'Home')
    expect(document.activeElement).toBe(buttons[0])

    pressKey(container, 'End')
    expect(document.activeElement).toBe(buttons[2])
  })

  it('does not swallow Tab key or unrelated keys', () => {
    const { container, buttons } = buildTablist(3)
    container.addEventListener('keydown', handleTablistArrowNav)
    buttons[0].focus()

    const tabEvent = pressKey(container, 'Tab')
    expect(document.activeElement).toBe(buttons[0])
    expect(tabEvent.defaultPrevented).toBe(false)
  })

  it('no-op when focus is outside any tab (does not preventDefault)', () => {
    const { container } = buildTablist(3)
    container.addEventListener('keydown', handleTablistArrowNav)

    const event = pressKey(container, 'ArrowRight')

    expect(document.activeElement).not.toBe(null)
    expect(event.defaultPrevented).toBe(false)
  })
})
