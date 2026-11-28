// 发布域常量唯一真源: 限额术语与超限处理策略文案的全站唯一定义,
// SettingsView / PublishView / PikPakPublishView / TaskDetailDrawer 四处消费。
// 限额术语受 scripts/lint_standards.py 强制扫描 —— 严禁折行或改写口径。

/** 普通账号单文件上传上限 (规范口径, 严禁小数 MB / 裸 GB 简写) */
export const STANDARD_LIMIT_LABEL = '2000 MiB (1.95 GiB)'

/** Premium 账号单文件上传上限 (规范口径) */
export const PREMIUM_LIMIT_LABEL = '4000 MiB (3.91 GiB)'

export interface PublishSelectOption {
  label: string
  value: string
  [key: string]: unknown
}

// ── 发布后网盘清理模式 ───────────────────────────────────────
export const CLEANUP_MODE_LABELS: Record<string, string> = {
  none: '保留原状',
  archive: '归档到根目录 [已发布归档]',
  delete: '移入回收站',
}

export const CLEANUP_MODE_OPTIONS: PublishSelectOption[] = [
  { label: CLEANUP_MODE_LABELS.archive, value: 'archive' },
  { label: CLEANUP_MODE_LABELS.none, value: 'none' },
  { label: CLEANUP_MODE_LABELS.delete, value: 'delete' },
]

// ── 超限文件处理策略 ─────────────────────────────────────────
// split = PikPak 零落盘流式切片专属 (各分卷带完整视频索引, 原生可播);
// 本地发布无分卷能力不暴露该选项 (后端按 skip_and_report 执行), PikPak 不暴露 strict_fail。
export const OVERSIZE_POLICY_LABELS: Record<string, string> = {
  skip_and_report: '跳过并占位报告 (推荐)',
  split: '可播切片分卷 (Playable Split)',
  strict_fail: '严格报错中断',
}

export const LOCAL_OVERSIZE_POLICY_OPTIONS: PublishSelectOption[] = [
  { label: OVERSIZE_POLICY_LABELS.skip_and_report, value: 'skip_and_report' },
  { label: OVERSIZE_POLICY_LABELS.strict_fail, value: 'strict_fail' },
]

export const PIKPAK_OVERSIZE_POLICY_OPTIONS: PublishSelectOption[] = [
  { label: OVERSIZE_POLICY_LABELS.skip_and_report, value: 'skip_and_report' },
  { label: OVERSIZE_POLICY_LABELS.split, value: 'split' },
]

// ── 流式切片引擎策略 (oversize_policy = split 时生效) ────────
export const SPLIT_STRATEGY_LABELS: Record<string, string> = {
  vsse_with_fallback: 'VSSE 极速重构 (推荐)',
  fmp4_universal: 'fMP4 全能保底',
  raw: '原始二进制硬切',
}

/** 长描述形态 (系统设置项下拉用) */
export const SPLIT_STRATEGY_OPTIONS: PublishSelectOption[] = [
  { label: 'VSSE 极速重构 (纯 Python 零 CPU 占用，非标自动降级，推荐)', value: 'vsse_with_fallback' },
  { label: 'fMP4 全能保底 (全格式自适应高兼容)', value: 'fmp4_universal' },
  { label: '原始二进制硬切 (不可播)', value: 'raw' },
]

/**
 * 超限分类短标签 (诊断三表/媒体测试报告共用):
 * premium_limit / protocol_hard_cap = 平台硬上限, non_premium_limit = 开通 Premium 可解。
 */
export function oversizeKindLabel(kind: string | null | undefined): string {
  if (kind === 'premium_limit' || kind === 'protocol_hard_cap') {
    return `超过 Telegram 平台最高上限 ${PREMIUM_LIMIT_LABEL}`
  }
  return `非 Premium 账号上限（开通会员可解）`
}
