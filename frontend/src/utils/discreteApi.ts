import { computed } from 'vue'
import { getActivePinia } from 'pinia'
import { createDiscreteApi } from 'naive-ui'
import { naiveTheme, naiveThemeOverrides, useThemeStore } from '@/stores/themeStore'

// 全局弹层 (message/dialog/notification/loadingBar) 唯一出口。
// configProviderProps 用 computed 动态绑定 themeStore, 使弹层实时跟随浅色/深色主题切换
//。
// 注意: createDiscreteApi 在模块静态导入期就会同步 mount 内部宿主组件并求值 configProviderProps.value,
// 此时 main.js 尚未执行 app.use(pinia); 若无守卫直调 useThemeStore() 会抛 reading '_s' 并致
// message 实例为 undefined。未激活 Pinia 时直接读取与 themeStore 同源的模块级 computed 建立响应式追踪。
export const configProviderProps = computed(() => {
  if (getActivePinia()) {
    const themeStore = useThemeStore()
    return {
      theme: themeStore.naiveTheme,
      themeOverrides: themeStore.naiveThemeOverrides,
    }
  }
  return {
    theme: naiveTheme.value,
    themeOverrides: naiveThemeOverrides.value,
  }
})

const { message, notification, dialog, loadingBar } = createDiscreteApi(
  ['message', 'notification', 'dialog', 'loadingBar'],
  { configProviderProps },
)

export { message, notification, dialog, loadingBar }
