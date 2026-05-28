import { DecentDBWebError, open, probeRuntime } from "../../../bindings/web/dist/index.js";

const output = document.getElementById("output");
const button = document.getElementById("run");

const DB_TABLE = "smoke";

function log(message) {
  if (!output) {
    return;
  }
  output.textContent = `${output.textContent}\n${message}`;
}

function clearOutput() {
  if (output) {
    output.textContent = "";
  }
}

function normalizeUrl(url) {
  return url instanceof URL ? url.toString() : String(url);
}

function withDatabaseUrls(overrides = {}) {
  const base = window.location.href;
  return {
    workerUrl: normalizeUrl(overrides.workerUrl ?? new URL("../../../bindings/web/dist/worker.js", base)),
    wasmUrl: normalizeUrl(overrides.wasmUrl ?? new URL("../../../bindings/web/dist/decentdb_wasm.js", base)),
  };
}

function formatRows(rows) {
  return rows.map((row) => `${row.id}:${row.name}`).join(",");
}

async function closeIfOpen(db) {
  if (!db) {
    return;
  }
  try {
    await db.close();
  } catch {
    // best effort close in shared-page cleanup paths
  }
}

async function runSmokeScenario(options = {}) {
  const scenarioId = options.scenarioId ?? "manual";
  const path = options.path ?? `decentdb-web-smoke-${scenarioId}.ddb`;
  const importPath = options.importPath ?? `decentdb-web-smoke-import-${scenarioId}.ddb`;
  const { workerUrl, wasmUrl } = withDatabaseUrls(options);

  const seedId = options.seedId ?? 1_000_001;
  const probe = await probeRuntime({
    wasmUrl,
    resultTransport: "binary",
  });
  let db = null;
  let reopenedDb = null;
  let importDb = null;

  try {
    log("opening primary database");
    db = await open({
      path,
      mode: options.mode ?? "openOrCreate",
      workerUrl,
      wasmUrl,
      resultTransport: "binary",
    });

    await db.exec(`CREATE TABLE IF NOT EXISTS ${DB_TABLE}(id INT64 PRIMARY KEY, name TEXT)`);
    await db.exec(`DELETE FROM ${DB_TABLE}`);
    await db.exec(`INSERT INTO ${DB_TABLE}(id, name) VALUES ($1, $2)`, [seedId, `scenario:${scenarioId}`]);
    await db.exec("CREATE TABLE IF NOT EXISTS smoke_blob(id INT64 PRIMARY KEY, body BLOB)");
    await db.exec("DELETE FROM smoke_blob");
    await db.exec("INSERT INTO smoke_blob(id, body) VALUES ($1, $2)", [seedId, new Uint8Array([1, 2, 3, 4])]);
    const created = await db.query(`SELECT id, name FROM ${DB_TABLE} ORDER BY id ASC`);
    const blobResult = await db.query("SELECT body FROM smoke_blob WHERE id = $1", [seedId]);
    const blobBytes = Array.from(blobResult.rows[0]?.body ?? []);
    const browserRuntimeView = await db.query("SELECT * FROM sys.browser_runtime");
    const browserOwnerView = await db.query("SELECT * FROM sys.browser_owner");
    const browserStorageView = await db.query("SELECT * FROM sys.browser_storage");
    const browserSyncView = await db.query("SELECT * FROM sys.browser_sync");
    const dbMetadata = {
      parserProfile: db.parserProfile,
      protocolVersion: db.protocolVersion,
      capabilities: db.capabilities,
    };

    await db.exec("CREATE TABLE IF NOT EXISTS smoke_tx(id INT64 PRIMARY KEY, name TEXT)");
    await db.exec("DELETE FROM smoke_tx");
    await db.transaction(async (tx) => {
      await tx.exec("INSERT INTO smoke_tx(id, name) VALUES ($1, $2)", [seedId, "committed"]);
      const savepoint = await tx.savepoint("smoke_sp");
      await tx.exec("INSERT INTO smoke_tx(id, name) VALUES ($1, $2)", [seedId + 1, "rolled-back"]);
      await tx.rollbackToSavepoint(savepoint);
      await tx.releaseSavepoint(savepoint);
    });
    const txRows = await db.query("SELECT id, name FROM smoke_tx ORDER BY id ASC");

    const stmt = await db.prepare(`SELECT id, name FROM ${DB_TABLE} ORDER BY id ASC`);
    const firstPreparedRow = await stmt.step();
    await stmt.reset();
    const preparedPage = await stmt.page(10);
    await stmt.clearBindings();
    await stmt.close();
    let closedStatementCode = null;
    try {
      await stmt.step();
    } catch (error) {
      closedStatementCode = error && typeof error === "object" && "code" in error ? error.code : null;
    }

    await db.sync.configurePeer({
      name: "cloud",
      endpoint: "https://sync.example.invalid",
    });
    const syncRun = await db.sync.run({
      peer: "cloud",
      direction: "both",
      timeoutMs: 1000,
    });
    const originalFetch = globalThis.fetch;
    const syncOrder = [];
    globalThis.fetch = async () => {
      syncOrder.push("ack");
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    };
    let applyAck;
    try {
      applyAck = await db.sync.applyAndAckShape({
        peer: "cloud",
        tenantId: "tenant_42",
        subjectId: "user_123",
        clientReplicaId: "web_123",
        message: {
          shape_id: "tenant_42_tasks_v1",
          shape_sequence: 7,
          source_high_watermark: 11,
          changeset: { changeset_id: "changeset:test" },
        },
        apply: async () => {
          syncOrder.push("apply");
          return { outcome: "applied" };
        },
      });
    } finally {
      globalThis.fetch = originalFetch;
    }
    const metrics = await db.metrics();
    let missingTableDiagnostic = null;
    try {
      await db.query("SELECT * FROM no_such_web_table");
      throw new Error("expected missing-table query to fail");
    } catch (error) {
      if (!(error instanceof DecentDBWebError)) {
        throw error;
      }
      if (
        error.nativeCode !== 5 ||
        error.subcode !== "sql.relation_not_found" ||
        error.diagnostic?.relation !== "no_such_web_table"
      ) {
        throw new Error(`unexpected diagnostic: ${JSON.stringify(error.toPayload())}`);
      }
      missingTableDiagnostic = error.toPayload();
    }
    const checkpointResult = await db.checkpoint();
    const exported = await db.export();
    const persisted = await db.persist();
    log(`primary rows=${formatRows(created.rows)}`);
    log(`checkpoint truncatedWalBytes=${checkpointResult.truncatedWalBytes ?? 0}`);
    log(`export size=${exported.size}`);
    await db.close();
    db = null;

    log("reopen primary database");
    reopenedDb = await open({
      path,
      mode: "open",
      workerUrl,
      wasmUrl,
      resultTransport: "json",
    });
    const reopened = await reopenedDb.query(`SELECT id, name FROM ${DB_TABLE} ORDER BY id ASC`);
    log(`reopen rows=${formatRows(reopened.rows)}`);

    log("run import path");
    importDb = await open({
      path: importPath,
      mode: "openOrCreate",
      workerUrl,
      wasmUrl,
    });
    await importDb.exec(`DROP TABLE IF EXISTS ${DB_TABLE}`);
    await importDb.import(exported.bytes);
    const imported = await importDb.query(`SELECT id, name FROM ${DB_TABLE} ORDER BY id ASC`);
    log(`import rows=${formatRows(imported.rows)}`);

    return {
      path,
      importPath,
      seedId,
      createdRows: created.rows,
      reopenedRows: reopened.rows,
      importedRows: imported.rows,
      checkpointBytes: checkpointResult.truncatedWalBytes ?? 0,
      exportedSize: exported.size,
      persisted,
      blobBytes,
      probe,
      metrics,
      browserRuntimeView,
      browserOwnerView,
      browserStorageView,
      browserSyncView,
      dbMetadata,
      txRows: txRows.rows,
      firstPreparedRow,
      preparedPage,
      closedStatementCode,
      missingTableDiagnostic,
      syncRun,
      applyAck,
      syncOrder,
    };
  } finally {
    await closeIfOpen(db);
    await closeIfOpen(reopenedDb);
    await closeIfOpen(importDb);
  }
}

globalThis.__runDecentDBWebSmoke = async function (options) {
  clearOutput();
  return runSmokeScenario(options);
};

button?.addEventListener("click", async () => {
  clearOutput();
  log("starting");
  try {
    const result = await runSmokeScenario({
      scenarioId: "manual",
      seedId: Date.now(),
      path: "decentdb-web-smoke.ddb",
      importPath: "decentdb-web-smoke-import.ddb",
    });
    log(`open/reopen/export/import smoke passed`);
    log(`created=${result.createdRows.length} reopen=${result.reopenedRows.length} import=${result.importedRows.length}`);
    log(`checkpoint truncatedWalBytes=${result.checkpointBytes}`);
    log(`export size=${result.exportedSize}`);
    log(`persisted=${result.persisted}`);
  } catch (error) {
    if (error instanceof DecentDBWebError) {
      log(`web failure: ${error.code}: ${error.message}`);
      if (error.details) {
        log(error.details);
      }
      return;
    }
    if (error instanceof Error) {
      log(`open failure: ${error.name}: ${error.message}`);
    } else {
      log(`open failure: ${String(error)}`);
    }
  }
});
