<template>
  <transition name="slide-up">
    <div v-if="selectedCount > 0" ref="barElement" class="batch-action-floating-bar glass-card">
      <div class="batch-bar-left">
        <span class="selected-count-badge">
          已勾选 <b>{{ selectedCount }}</b> 个任务
        </span>
        <n-button text size="tiny" @click="$emit('clear')" class="clear-select-btn"> 取消全选 </n-button>
      </div>

      <div class="batch-bar-actions">
        <n-button size="small" secondary type="warning" @click="$emit('batch-pause')"> ⏸️ 批量暂停 </n-button>
        <n-button size="small" secondary type="primary" @click="$emit('batch-resume')"> ▶️ 批量恢复 </n-button>
        <n-button size="small" secondary type="info" @click="$emit('batch-retry')"> 🔄 批量重试失败 </n-button>
        <n-button size="small" secondary type="error" @click="$emit('batch-cancel')"> ⏹️ 批量取消 </n-button>
        <n-button size="small" type="error" @click="$emit('batch-delete')"> 🗑️ 批量删除 </n-button>
      </div>
    </div>
  </transition>
</template>

<script setup lang="ts">
import { useTemplateRef, watch } from 'vue'
import { NButton } from 'naive-ui'

defineProps<{
  selectedCount: number
}>()

defineEmits<{
  (e: 'clear'): void
  (e: 'batch-pause'): void
  (e: 'batch-resume'): void
  (e: 'batch-retry'): void
  (e: 'batch-cancel'): void
  (e: 'batch-delete'): void
}>()

const barElement = useTemplateRef<HTMLDivElement>('barElement')

watch(
  barElement,
  (bar, _, onCleanup) => {
    if (!bar) return

    const scrollerStyle = document.documentElement.style
    const updateClearance = () => {
      scrollerStyle.setProperty('--task-batch-clearance', `calc(${bar.offsetHeight}px + var(--task-batch-bottom))`)
    }
    const observer = new ResizeObserver(updateClearance)
    updateClearance()
    observer.observe(bar, { box: 'border-box' })

    onCleanup(() => {
      observer.disconnect()
      scrollerStyle.removeProperty('--task-batch-clearance')
    })
  },
  { flush: 'post' },
)
</script>

<style scoped>
:global(html) {
  --task-batch-bottom: 2rem;
  scroll-padding-block-end: var(--task-batch-clearance, 0px);
}

.batch-action-floating-bar {
  position: fixed;
  bottom: var(--task-batch-bottom);
  left: 50%;
  transform: translateX(-50%);
  z-index: 99;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 2rem;
  max-width: calc(100vw - 1.5rem);
  padding: 0.85rem 1.75rem;
  border-radius: 9999px;
  background: var(--bg-card);
  backdrop-filter: blur(16px);
  -webkit-backdrop-filter: blur(16px);
  border: 1px solid var(--accent-color);
  box-shadow: 0 12px 32px rgba(0, 0, 0, 0.2);
}

.batch-bar-left {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  white-space: nowrap;
}

.selected-count-badge {
  font-size: 0.9rem;
  color: var(--text-primary);
}

.selected-count-badge b {
  color: var(--accent-color);
  font-size: 1.05rem;
}

.clear-select-btn {
  color: var(--text-secondary);
}

.batch-bar-actions {
  display: flex;
  align-items: center;
  gap: 0.65rem;
  flex-wrap: nowrap;
}

.slide-up-enter-active,
.slide-up-leave-active {
  transition: all 0.3s cubic-bezier(0.16, 1, 0.3, 1);
}

.slide-up-enter-from,
.slide-up-leave-to {
  opacity: 0;
  transform: translate(-50%, 20px);
}

@media (max-width: 768px) {
  :global(html) {
    --task-batch-bottom: calc(0.75rem + var(--safe-area-bottom));
  }

  .batch-action-floating-bar {
    width: calc(100vw - 1.5rem - var(--safe-area-left) - var(--safe-area-right));
    max-width: 520px;
    border-radius: 16px;
    padding: 0.75rem 1rem;
    flex-direction: column;
    align-items: stretch;
    gap: 0.65rem;
  }

  .batch-bar-left {
    justify-content: space-between;
  }

  .batch-bar-actions {
    flex-wrap: wrap;
    justify-content: flex-start;
    gap: 0.45rem;
  }
}
</style>
