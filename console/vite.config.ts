import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': 'http://localhost:8080',
    },
  },
  preview: {
    proxy: {
      '/api': 'http://localhost:8080',
    },
  },
  test: {
    environment: 'jsdom',
    globals: true,
    // Playwright's own specs (e2e/) are a different test runner with an
    // incompatible test() global; Vitest's default glob would otherwise
    // try to collect them too.
    exclude: ['**/node_modules/**', 'e2e/**'],
  },
});
