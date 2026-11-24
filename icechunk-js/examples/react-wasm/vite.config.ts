import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  // pre-bundling would separate icechunk's module from the .wasm it loads
  // from next to itself (`new URL(..., import.meta.url)`)
  optimizeDeps: {
    exclude: ['@earthmover/icechunk'],
  },
})
