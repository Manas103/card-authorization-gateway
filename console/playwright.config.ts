import { defineConfig } from '@playwright/test';

/**
 * Browser-level proof that the console opens an incident on its triggering
 * segment and decline code, using Playwright's own bundled Chromium,
 * headless, launched and closed by Playwright itself. Assumes the Spring
 * Boot backend is already running on :8080 with its demo incidents seeded
 * (the same precondition the Java E2E suites in this portfolio place on
 * their own backend); `webServer` below builds and serves the console
 * itself and tears it down when the run ends.
 */
export default defineConfig({
  testDir: './e2e',
  timeout: 30_000,
  use: {
    baseURL: 'http://localhost:4319',
  },
  webServer: {
    command: 'npm run build && npm run preview -- --port 4319 --strictPort',
    port: 4319,
    reuseExistingServer: false,
    timeout: 60_000,
  },
});
