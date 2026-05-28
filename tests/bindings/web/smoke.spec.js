const path = require("node:path");
const { test, expect } = require(path.resolve(__dirname, "../../..", "bindings/web/node_modules/@playwright/test"));

test("web package OPFS smoke: probe + owner diagnostics + create/open/query/reopen + export/import", async ({ page }) => {
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
  expect(result.blobBytes).toEqual([1, 2, 3, 4]);
  expect(result.probe.supported).toBe(true);
  expect(result.probe.runtime.dedicatedWorker).toBe(true);
  expect(result.probe.runtime.broadcastChannel).toBe(true);
  expect(result.probe.runtime.webLocks).toBe(true);
  expect(result.probe.storage.opfsDirectory).toBe(true);
  expect(result.browserRuntimeView.rowCount).toBe(1);
  expect(result.browserOwnerView.rowCount).toBe(1);
  expect(result.browserStorageView.rowCount).toBe(1);
  expect(result.browserSyncView.rowCount).toBe(1);
  expect(result.dbMetadata.parserProfile).toBe("browser-app-v2");
  expect(result.dbMetadata.protocolVersion).toBe(2);
  expect(result.dbMetadata.capabilities.transactions).toBe(true);
  expect(result.dbMetadata.capabilities.statementPaging).toBe(true);
  expect(result.dbMetadata.capabilities.branchSnapshots).toBe(false);
  expect(result.txRows).toEqual([{ id: 4242, name: "committed" }]);
  expect(result.firstPreparedRow).toEqual(result.createdRows[0]);
  expect(result.preparedPage.rows).toEqual(result.createdRows);
  expect(result.preparedPage.done).toBe(true);
  expect(result.closedStatementCode).toBe("ERR_BROWSER_STATEMENT_CLOSED");
  expect(result.syncRun.status).toBe("deferred");
  expect(result.syncOrder).toEqual(["apply", "ack"]);
  expect(result.applyAck.apply).toEqual({ outcome: "applied" });
  expect(result.applyAck.ack).toEqual({ ok: true });
  expect(result.metrics.ownerRuntime).toBe("dedicated-worker");
  expect(result.metrics.coordinationModel).toBe("broadcastchannel-weblocks-dedicated-owner");
  expect(result.metrics.protocolVersion).toBe(2);
  expect(result.metrics.capabilities.changesetApply).toBe(true);
});
