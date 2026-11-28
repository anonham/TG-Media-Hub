import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import Components from 'unplugin-vue-components/vite'
import { NaiveUiResolver } from 'unplugin-vue-components/resolvers'
import { fileURLToPath, URL } from 'node:url'
import pkg from './package.json'

const backendHttpTarget =
  process.env.VITE_BACKEND_URL || process.env.E2E_UI_BACKEND_URL || 'http://127.0.0.1:8500'
const backendWsTarget = backendHttpTarget.replace(/^http/, 'ws')

export default defineConfig({
  base: '/static/',
  define: {
    __APP_VERSION__: JSON.stringify(pkg.version)
  },
  plugins: [
    vue({
      template: {
        compilerOptions: {
          isCustomElement: (tag) => tag === 'search'
        }
      }
    }),
    Components({
      resolvers: [NaiveUiResolver()]
    })
  ],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url))
    },
    extensions: ['.mjs', '.js', '.ts', '.jsx', '.tsx', '.json', '.vue']
  },
  build: {
    outDir: fileURLToPath(new URL('../static', import.meta.url)),
    emptyOutDir: true,
    chunkSizeWarningLimit: 350,
    rollupOptions: {
      output: {
        manualChunks(id) {
          const norm = id.replace(/\\/g, '/')
          if (norm.includes('/node_modules/')) {
            if (norm.includes('/@vicons/')) {
              return 'icons-vendor'
            }
            if (norm.includes('/axios/') || norm.includes('/@hey-api/')) {
              return 'http-vendor'
            }
            if (
              norm.includes('/date-fns/') ||
              norm.includes('/async-validator/') ||
              norm.includes('/seemly/') ||
              norm.includes('/evtd/') ||
              norm.includes('/treemate/') ||
              norm.includes('/css-render/') ||
              norm.includes('/@css-render/')
            ) {
              return 'ui-deps-vendor'
            }
            if (
              norm.includes('/vue/') ||
              norm.includes('/@vue/') ||
              norm.includes('/vue-router/') ||
              norm.includes('/pinia/')
            ) {
              return 'vue-vendor'
            }
          }
          if (norm.includes('/src/api/gen/')) {
            return 'api-sdk'
          }
        }
      }
    }
  },
  server: {
    host: '127.0.0.1',
    port: 5173,
    proxy: {
      '/api': {
        target: backendHttpTarget,
        changeOrigin: true
      },
      '/ws': {
        target: backendWsTarget,
        ws: true,
        changeOrigin: true
      }
    }
  }
})
