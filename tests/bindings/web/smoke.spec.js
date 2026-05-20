const path = require("node:path");
const { test, expect } = require(path.resolve(__dirname, "../../..", "bindings/web/node_modules/@playwright/test"));

test("web package OPFS smoke: create/open/query/reopen + export/import + checkpoint/persist", async ({ page }) => {
  await page.goto("/tests/bindings/web/smoke.html", { waitUntil: "domcontentloaded" });
  try {
    await page.waitForFunction(() => typeof globalThis.__runDecentDBWebSmoke === "function", null, { timeout: 10_000 });
  } catch (error) {
    const output = await page.locator("#output").textContent().catch(() => "");
    throw new Error(
      `Smoke harness did not load in the browser. Ensure bindings/web/dist/index.js, bindings/web/dist/worker.js, and the wasm bundle are built before running tests.${output ? `\nBrowser output:\n${output}` : ""}`
    );
  }

  const resultEnvelope = await page.evaluate(async () => {
    try {
      return {
        ok: true,
        result: await globalThis.__runDecentDBWebSmoke({
          scenarioId: "playwright",
          seedId: 4242,
          path: `decentdb-web-smoke-playwright.ddb`,
          importPath: `decentdb-web-smoke-playwright-import.ddb`,
        }),
      };
    } catch (error) {
      return {
        ok: false,
        error: {
          name: error instanceof Error ? error.name : "Error",
          message: error instanceof Error ? error.message : String(error),
          code: error && typeof error === "object" && "code" in error ? error.code : undefined,
          details: error && typeof error === "object" && "details" in error ? error.details : undefined,
        },
      };
    }
  });

  if (!resultEnvelope.ok) {
    const output = await page.locator("#output").textContent().catch(() => "");
    throw new Error(`Browser smoke failed: ${JSON.stringify(resultEnvelope.error)}${output ? `\nBrowser output:\n${output}` : ""}`);
  }
  const result = resultEnvelope.result;

  expect(result.path).toContain("decentdb-web-smoke-playwright.ddb");
  expect(result.createdRows).toHaveLength(1);
  expect(result.reopenedRows).toHaveLength(1);
  expect(result.importedRows).toHaveLength(1);
  expect(result.createdRows[0]).toEqual({
    id: 4242,
    name: "scenario:playwright",
  });
  expect(result.reopenedRows).toEqual(result.createdRows);
  expect(result.importedRows).toEqual(result.createdRows);
  expect(result.exportedSize).toBeGreaterThan(0);
  expect(result.checkpointBytes).toBeGreaterThanOrEqual(0);
  expect(typeof result.persisted).toBe("boolean");
});
