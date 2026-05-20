const path = require("node:path");
const { test, expect } = require(path.resolve(__dirname, "../../..", "bindings/web/node_modules/@playwright/test"));

async function openDb(page, dbPath) {
  return page.evaluate(async (targetPath) => {
    const mod = await import("../../../bindings/web/dist/index.js");
    const db = await mod.open({
      path: targetPath,
      mode: "openOrCreate",
      workerUrl: new URL("../../../bindings/web/dist/worker.js", window.location.href).toString(),
      wasmUrl: new URL("../../../bindings/web/dist/decentdb_wasm.js", window.location.href).toString(),
      resultTransport: "binary",
    });
    globalThis.__decentdbMultitabHandle = db;
    return {
      ownerId: db.ownerId,
      runtime: db.ownerRuntime,
    };
  }, dbPath);
}

test("multi-tab: one owner per logical path and recovery after original owner tab closes", async ({ context, page }) => {
  const second = await context.newPage();
  const dbPath = `decentdb-web-multitab-${Date.now()}.ddb`;

  await page.goto("/tests/bindings/web/smoke.html", { waitUntil: "domcontentloaded" });
  await second.goto("/tests/bindings/web/smoke.html", { waitUntil: "domcontentloaded" });

  const firstOpen = await openDb(page, dbPath);
  expect(firstOpen.runtime).toBe("dedicated-worker");

  await page.evaluate(async () => {
    const db = globalThis.__decentdbMultitabHandle;
    await db.exec("CREATE TABLE IF NOT EXISTS mt(id INT64 PRIMARY KEY, name TEXT)");
    await db.exec("DELETE FROM mt");
    await db.exec("INSERT INTO mt(id, name) VALUES ($1, $2)", [1, "owner-tab"]);
  });

  const secondOpen = await openDb(second, dbPath);
  expect(secondOpen.ownerId).toBe(firstOpen.ownerId);

  const fromSecond = await second.evaluate(async () => {
    const db = globalThis.__decentdbMultitabHandle;
    const result = await db.query("SELECT id, name FROM mt ORDER BY id ASC");
    return result.rows;
  });
  expect(fromSecond).toEqual([{ id: 1, name: "owner-tab" }]);

  await page.close();

  const afterCloseRows = await second.evaluate(async () => {
    const db = globalThis.__decentdbMultitabHandle;
    await db.exec("INSERT INTO mt(id, name) VALUES ($1, $2)", [2, "remaining-tab"]);
    const result = await db.query("SELECT id, name FROM mt ORDER BY id ASC");
    await db.close();
    return result.rows;
  });
  expect(afterCloseRows).toEqual([
    { id: 1, name: "owner-tab" },
    { id: 2, name: "remaining-tab" },
  ]);
});

test("explicit unsupported behavior: missing BroadcastChannel fails fast", async ({ page }) => {
  await page.goto("/tests/bindings/web/smoke.html", { waitUntil: "domcontentloaded" });
  const result = await page.evaluate(async () => {
    const mod = await import("../../../bindings/web/dist/index.js");
    const previous = globalThis.BroadcastChannel;
    try {
      globalThis.BroadcastChannel = undefined;
      await mod.open({
        path: `decentdb-web-unsupported-${Date.now()}.ddb`,
        skipRuntimeProbe: true,
      });
      return { ok: true };
    } catch (error) {
      return {
        ok: false,
        code: error && typeof error === "object" && "code" in error ? error.code : undefined,
        message: error && typeof error === "object" && "message" in error ? error.message : String(error),
      };
    } finally {
      globalThis.BroadcastChannel = previous;
    }
  });

  expect(result.ok).toBe(false);
  expect(result.code).toBe("ERR_BROWSER_COORDINATION_UNAVAILABLE");
});
