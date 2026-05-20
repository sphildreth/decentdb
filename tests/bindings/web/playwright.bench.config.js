const path = require("node:path");
const playwrightTestPackage = path.resolve(__dirname, "../../..", "bindings/web/node_modules/@playwright/test");
const { defineConfig } = require(playwrightTestPackage);

const ROOT_DIR = path.resolve(__dirname, "../..", "..");

/** @type {import('@playwright/test').PlaywrightTestConfig} */
module.exports = defineConfig({
  testDir: __dirname,
  testMatch: /transport-bench\.spec\.js$/,
  outputDir: path.resolve(ROOT_DIR, ".tmp/playwright-web-bench"),
  timeout: 180_000,
  fullyParallel: false,
  workers: 1,
  use: {
    baseURL: "http://127.0.0.1:4173",
    trace: "retain-on-failure",
  },
  webServer: {
    command: "python -m http.server 4173 --bind 127.0.0.1",
    cwd: ROOT_DIR,
    port: 4173,
    reuseExistingServer: true,
    timeout: 120_000,
  },
  projects: [
    {
      name: "chromium",
      use: {
        browserName: "chromium",
      },
    },
  ],
});
