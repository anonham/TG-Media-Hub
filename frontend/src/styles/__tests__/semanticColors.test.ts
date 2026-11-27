import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'

function srgbToLinear(channel: number): number {
  const c = channel / 255
  return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4
}

function parseHexRgb(hex: string): [number, number, number] {
  const clean = hex.trim().replace(/^#/, '')
  if (!/^[0-9a-fA-F]{6}$/.test(clean)) {
    throw new Error(`Expected 6-digit hex color, got "${hex}"`)
  }
  return [
    Number.parseInt(clean.slice(0, 2), 16),
    Number.parseInt(clean.slice(2, 4), 16),
    Number.parseInt(clean.slice(4, 6), 16),
  ]
}

function relativeLuminance(hex: string): number {
  const [r, g, b] = parseHexRgb(hex)
  return 0.2126 * srgbToLinear(r) + 0.7152 * srgbToLinear(g) + 0.0722 * srgbToLinear(b)
}

function compositeOver(fgHex: string, alpha: number, bgHex: string): string {
  const [fr, fg, fb] = parseHexRgb(fgHex)
  const [br, bg, bb] = parseHexRgb(bgHex)
  const r = Math.round(fr * alpha + br * (1 - alpha))
  const g = Math.round(fg * alpha + bg * (1 - alpha))
  const b = Math.round(fb * alpha + bb * (1 - alpha))
  return `#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b.toString(16).padStart(2, '0')}`
}

function contrastRatio(hexA: string, hexB: string): number {
  const lA = relativeLuminance(hexA)
  const lB = relativeLuminance(hexB)
  const [lighter, darker] = lA >= lB ? [lA, lB] : [lB, lA]
  return (lighter + 0.05) / (darker + 0.05)
}

function extractTopLevelBlock(css: string, selector: string): string {
  const start = css.indexOf(selector)
  if (start === -1) {
    throw new Error(`Selector "${selector}" not found`)
  }
  const openBrace = css.indexOf('{', start)
  if (openBrace === -1) {
    throw new Error(`Opening brace for "${selector}" not found`)
  }
  let depth = 0
  for (let i = openBrace; i < css.length; i += 1) {
    if (css[i] === '{') depth += 1
    else if (css[i] === '}') {
      depth -= 1
      if (depth === 0) {
        return css.slice(openBrace + 1, i)
      }
    }
  }
  throw new Error(`Unclosed block for "${selector}"`)
}

function readToken(block: string, tokenName: string): string {
  const escaped = tokenName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const match = block.match(new RegExp(`${escaped}\\s*:\\s*(#[0-9a-fA-F]{6})\\s*;`))
  if (!match || !match[1]) {
    throw new Error(`Token "${tokenName}" not found in CSS block`)
  }
  return match[1]
}

function extractStyleSection(vueSource: string): string {
  const matches = [...vueSource.matchAll(/<style(?:\s+[^>]*)?>([\s\S]*?)<\/style>/g)]
  return matches.map((m) => m[1] ?? '').join('\n')
}

const SEMANTIC_TOKENS: Array<{ token: string; tintBase: string }> = [
  { token: '--info-color', tintBase: '#0ea5e9' },
  { token: '--success-color', tintBase: '#10b981' },
  { token: '--warning-color', tintBase: '#f59e0b' },
  { token: '--danger-color', tintBase: '#ef4444' },
  { token: '--indigo-color', tintBase: '#6366f1' },
  { token: '--pink-color', tintBase: '#ec4899' },
  { token: '--purple-color', tintBase: '#a855f7' },
]

const P2_VUE_COMPONENTS = [
  'components/auth/AuthModal.vue',
  'components/tasks/drawer/TaskRealtimeMetricsTab.vue',
  'components/tasks/drawer/TaskParamsMetadataTab.vue',
  'views/create/ForwardView.vue',
  'views/create/PublishView.vue',
  'components/presets/PresetManageModal.vue',
  'components/forward/CoverageBar.vue',
  'views/DashboardView.vue',
]

function resolveFrontendSrcDir(): string {
  const direct = resolve(process.cwd(), 'src')
  if (existsSync(resolve(direct, 'styles/main.css'))) {
    return direct
  }
  return resolve(process.cwd(), 'frontend/src')
}

describe('P2 semantic status colors & WCAG AA contrast', () => {
  const srcDir = resolveFrontendSrcDir()
  const mainCss = readFileSync(resolve(srcDir, 'styles/main.css'), 'utf-8')
  const lightBlock = extractTopLevelBlock(mainCss, ':root {')
  const darkBlock = extractTopLevelBlock(mainCss, "[data-theme='dark'] {")

  it('defines all semantic tokens in both light and dark themes with >= 4.5:1 contrast', () => {
    for (const { token, tintBase } of SEMANTIC_TOKENS) {
      const lightFg = readToken(lightBlock, token)
      const lightSurfaces = ['#ffffff', '#f8fafc', compositeOver(tintBase, 0.15, '#f8fafc')]
      for (const bg of lightSurfaces) {
        expect(contrastRatio(lightFg, bg), `light ${token} (${lightFg}) on ${bg}`).toBeGreaterThanOrEqual(4.5)
      }

      const darkFg = readToken(darkBlock, token)
      const darkSurfaces = ['#1e293b', '#334155', compositeOver(tintBase, 0.15, '#334155')]
      for (const bg of darkSurfaces) {
        expect(contrastRatio(darkFg, bg), `dark ${token} (${darkFg}) on ${bg}`).toBeGreaterThanOrEqual(4.5)
      }
    }
  })

  it('includes a forced-colors system token fallback in main.css', () => {
    expect(mainCss).toMatch(/@media\s*\(\s*forced-colors\s*:\s*active\s*\)/)
  })

  it('removes hardcoded weak/raw semantic text hex colors from all 8 guarded Vue components', () => {
    const bannedHex = /#(38bdf8|fbbf24|f87171|eab308|34d399|f472b6|818cf8|c084fc)\b/i
    const bannedRawTextColors = /(?:color|border-left)\s*:[^;\n]*#(10b981|f59e0b|3b82f6|ef4444|0ea5e9)\b/i

    for (const relPath of P2_VUE_COMPONENTS) {
      const source = readFileSync(resolve(srcDir, relPath), 'utf-8')
      const style = extractStyleSection(source)
      expect(style, `${relPath} still contains banned weak hex color`).not.toMatch(bannedHex)
      expect(style, `${relPath} still contains raw semantic text/border hex color`).not.toMatch(bannedRawTextColors)
    }
  })
})
