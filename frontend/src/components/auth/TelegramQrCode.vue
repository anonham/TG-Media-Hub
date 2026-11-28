<template>
  <div class="qr-frame" role="img" :aria-label="label">
    <svg
      v-if="qrData"
      class="qr-svg"
      :viewBox="`0 0 ${viewSize} ${viewSize}`"
      shape-rendering="crispEdges"
      aria-hidden="true"
    >
      <rect x="0" y="0" :width="viewSize" :height="viewSize" fill="#ffffff" />
      <path :d="qrPath" fill="#111827" />
    </svg>
    <p v-else class="qr-empty">暂无有效二维码</p>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { encode } from 'uqr'

const props = withDefaults(
  defineProps<{
    value: string
    label?: string
    margin?: number
  }>(),
  {
    label: 'Telegram 登录二维码',
    margin: 2,
  },
)

const qrData = computed(() => {
  const raw = (props.value || '').trim()
  if (!raw) return null
  return encode(raw)
})

const viewSize = computed(() => {
  if (!qrData.value) return 0
  return qrData.value.size + props.margin * 2
})

const qrPath = computed(() => {
  if (!qrData.value) return ''
  const parts: string[] = []
  const m = props.margin
  const matrix = qrData.value.data
  for (let y = 0; y < matrix.length; y++) {
    const row = matrix[y]
    for (let x = 0; x < row.length; x++) {
      if (row[x]) {
        parts.push(`M${x + m},${y + m}h1v1h-1z`)
      }
    }
  }
  return parts.join('')
})
</script>

<style scoped>
.qr-frame {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 0.75rem;
  background: #ffffff;
  border: 1px solid var(--border-color, #d1d5db);
  border-radius: 12px;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.06);
}

.qr-svg {
  width: 200px;
  height: 200px;
  max-width: 100%;
  display: block;
}

.qr-empty {
  margin: 0;
  padding: 2rem 1rem;
  font-size: 0.85rem;
  color: #6b7280;
}
</style>
