# Instructions

- Following Playwright test failed.
- Explain why, be concise, respect Playwright best practices.
- Provide a snippet of code with the fix, if possible.

# Test info

- Name: smoke.spec.js >> web package OPFS smoke: create/open/query/reopen + export/import + checkpoint/persist
- Location: ../../tests/bindings/web/smoke.spec.js:4:1

# Error details

```
Error: Smoke harness did not load in the browser. Ensure bindings/web/dist/index.js, bindings/web/dist/worker.js, and the wasm bundle are built before running tests.
Browser output:
ready
```

# Page snapshot

```yaml
- generic [active] [ref=e1]:
  - heading "@decentdb/web browser smoke" [level=1] [ref=e2]
  - paragraph [ref=e3]: Browser smoke for worker bootstrap, WASM loading, OPFS open, writes, and reads.
  - button "Run smoke" [ref=e4]
  - generic [ref=e5]: ready
```

# Test source

```ts
  1  | const path = require("node:path");
  2  | const { test, expect } = require(path.resolve(__dirname, "../../..", "bindings/web/node_modules/@playwright/test"));
  3  | 
  4  | test("web package OPFS smoke: create/open/query/reopen + export/import + checkpoint/persist", async ({ page }) => {
  5  |   await page.goto("/tests/bindings/web/smoke.html", { waitUntil: "domcontentloaded" });
  6  |   try {
  7  |     await page.waitForFunction(() => typeof globalThis.__runDecentDBWebSmoke === "function", null, { timeout: 10_000 });
  8  |   } catch (error) {
  9  |     const output = await page.locator("#output").textContent().catch(() => "");
> 10 |     throw new Error(
     |           ^ Error: Smoke harness did not load in the browser. Ensure bindings/web/dist/index.js, bindings/web/dist/worker.js, and the wasm bundle are built before running tests.
  11 |       `Smoke harness did not load in the browser. Ensure bindings/web/dist/index.js, bindings/web/dist/worker.js, and the wasm bundle are built before running tests.${output ? `\nBrowser output:\n${output}` : ""}`
  12 |     );
  13 |   }
  14 | 
  15 |   const result = await page.evaluate(async () =>
  16 |     globalThis.__runDecentDBWebSmoke({
  17 |       scenarioId: "playwright",
  18 |       seedId: 4242,
  19 |       path: `decentdb-web-smoke-playwright.ddb`,
  20 |       importPath: `decentdb-web-smoke-playwright-import.ddb`,
  21 |     })
  22 |   );
  23 | 
  24 |   expect(result.path).toContain("decentdb-web-smoke-playwright.ddb");
  25 |   expect(result.createdRows).toHaveLength(1);
  26 |   expect(result.reopenedRows).toHaveLength(1);
  27 |   expect(result.importedRows).toHaveLength(1);
  28 |   expect(result.createdRows[0]).toEqual({
  29 |     id: 4242,
  30 |     name: "scenario:playwright",
  31 |   });
  32 |   expect(result.reopenedRows).toEqual(result.createdRows);
  33 |   expect(result.importedRows).toEqual(result.createdRows);
  34 |   expect(result.exportedSize).toBeGreaterThan(0);
  35 |   expect(result.checkpointBytes).toBeGreaterThanOrEqual(0);
  36 |   expect(typeof result.persisted).toBe("boolean");
  37 | });
  38 | 
```