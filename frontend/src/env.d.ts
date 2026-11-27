/// <reference types="vite/client" />

declare global {
  const __APP_VERSION__: string
}

import 'vue'

declare module 'vue' {
  export interface GlobalComponents {
    search: any
  }
  interface HTMLAttributes {
    'data-testid'?: string
  }
  interface ButtonHTMLAttributes {
    'data-testid'?: string
  }
  interface ReservedProps {
    'data-testid'?: string
  }
}
