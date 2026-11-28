<template>
  <div class="notfound-container">
    <div class="notfound-card glass-card">
      <div class="notfound-code font-mono">404</div>
      <h3>页面不存在</h3>
      <p>
        请求的路径 <code class="notfound-path font-mono">{{ route.fullPath }}</code> 没有对应的控制台视图
      </p>
      <div class="notfound-actions">
        <n-button type="primary" secondary @click="router.replace('/dashboard')"> 返回仪表盘 </n-button>
        <n-button quaternary @click="router.back()"> 返回上一页 </n-button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { useRoute, useRouter } from 'vue-router'
import { NButton } from 'naive-ui'

const route = useRoute()
const router = useRouter()
</script>

<style scoped>
.notfound-container {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 60vh;
}

.notfound-card {
  text-align: center;
  padding: 3rem 4rem;
  border-radius: 1rem;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 0.75rem;
}

.notfound-code {
  font-size: 4rem;
  font-weight: 700;
  line-height: 1;
  color: var(--accent-color);
  letter-spacing: 0.1em;
}

.notfound-card h3 {
  margin: 0;
  font-size: 1.2rem;
  font-weight: 600;
}

.notfound-card p {
  margin: 0;
  color: var(--text-secondary);
}

.notfound-path {
  padding: 0.1rem 0.4rem;
  border-radius: 6px;
  background: var(--bg-card-hover);
  word-break: break-all;
}

.notfound-actions {
  margin-top: 0.75rem;
  display: flex;
  gap: 0.75rem;
}
</style>
