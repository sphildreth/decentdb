const path = require("node:path");
const { test, expect } = require(path.resolve(__dirname, "../../..", "bindings/web/node_modules/@playwright/test"));

test("web package transport benchmark: binary versus JSON result transport", async ({ page }) => {
  await page.goto("/tests/bindings/web/transport-bench.html", { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => typeof globalThis.__runDecentDBWebTransportBench === "function", null, { timeout: 10_000 });

  const result = await page.evaluate(async () => globalThis.__runDecentDBWebTransportBench({
    scenarioId: "playwright",
    rowCount: 10_000,
  }));

  expect(result.binary.rowCount).toBe(result.rowCount);
  expect(result.json.rowCount).toBe(result.rowCount);
  expect(result.binary.first).toEqual(result.json.first);
  expect(result.binary.last).toEqual(result.json.last);
  expect(result.binary.durationMs).toBeGreaterThan(0);
  expect(result.json.durationMs).toBeGreaterThan(0);
  expect(result.binary.wasmMemoryAfter).toBeGreaterThanOrEqual(result.binary.wasmMemoryBefore);
  expect(result.json.wasmMemoryAfter).toBeGreaterThanOrEqual(result.json.wasmMemoryBefore);
  expect(result.binary.wasmMemoryAfter - result.binary.wasmMemoryBefore)
    .toBeLessThanOrEqual(result.json.wasmMemoryAfter - result.json.wasmMemoryBefore);

  console.log(JSON.stringify(result, null, 2));
});
