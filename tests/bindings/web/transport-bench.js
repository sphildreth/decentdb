import { open } from "../../../bindings/web/dist/index.js";

const output = document.getElementById("output");
const TABLE = "transport_bench";

function log(message) {
  if (!output) {
    return;
  }
  output.textContent = `${output.textContent}\n${message}`;
}

function normalizeUrl(url) {
  return url instanceof URL ? url.toString() : String(url);
}

function urls() {
  const base = window.location.href;
  return {
    workerUrl: normalizeUrl(new URL("../../../bindings/web/dist/worker.js", base)),
    wasmUrl: normalizeUrl(new URL("../../../bindings/web/dist/decentdb_wasm.js", base)),
  };
}

function valuesSql(start, count) {
  const rows = [];
  for (let index = 0; index < count; index += 1) {
    const id = start + index;
    rows.push(`(${id}, 'value-${String(id).padStart(5, "0")}-abcdefghijklmnopqrstuvwxyz')`);
  }
  return rows.join(",");
}

async function seed(db, rowCount) {
  await db.exec(`DROP TABLE IF EXISTS ${TABLE}`);
  await db.exec(`CREATE TABLE ${TABLE}(id INT64 PRIMARY KEY, name TEXT)`);
  const chunkSize = 250;
  for (let start = 1; start <= rowCount; start += chunkSize) {
    const count = Math.min(chunkSize, rowCount - start + 1);
    await db.exec(`INSERT INTO ${TABLE}(id, name) VALUES ${valuesSql(start, count)}`);
  }
  await db.checkpoint();
}

async function runOne(transport, options) {
  const { workerUrl, wasmUrl } = urls();
  const path = `decentdb-web-transport-bench-${options.scenarioId}-${transport}.ddb`;
  let db = null;
  try {
    const coldOpenStart = performance.now();
    db = await open({
      path,
      mode: "openOrCreate",
      workerUrl,
      wasmUrl,
      resultTransport: transport,
    });
    const coldOpenMs = performance.now() - coldOpenStart;
    await seed(db, options.rowCount);
    const firstQueryStart = performance.now();
    await db.query("SELECT 1 AS ok");
    const firstQueryMs = performance.now() - firstQueryStart;

    const prepared = await db.prepare(`SELECT id, name FROM ${TABLE} WHERE id = $1`);
    const preparedStart = performance.now();
    for (let index = 1; index <= options.lookupCount; index += 1) {
      await prepared.bind([index]);
      const row = await prepared.step();
      if (!row || row.id !== index) {
        throw new Error(`prepared lookup returned wrong row for ${index}`);
      }
      await prepared.reset();
    }
    await prepared.close();
    const preparedLookupMs = performance.now() - preparedStart;

    const insertStart = performance.now();
    await db.exec("CREATE TABLE IF NOT EXISTS bench_insert(id INT64 PRIMARY KEY, name TEXT)");
    await db.exec("DELETE FROM bench_insert");
    await db.transaction(async (tx) => {
      for (let index = 1; index <= options.insertCount; index += 1) {
        await tx.exec("INSERT INTO bench_insert(id, name) VALUES ($1, $2)", [index, `insert-${index}`]);
      }
    });
    const insertTransactionMs = performance.now() - insertStart;

    const before = await db.metrics();
    const start = performance.now();
    const result = await db.query(`SELECT id, name FROM ${TABLE} ORDER BY id ASC`);
    const durationMs = performance.now() - start;
    const after = await db.metrics();
    const exportStart = performance.now();
    const exported = await db.export();
    const exportMs = performance.now() - exportStart;
    await db.close();
    db = null;

    const warmOpenStart = performance.now();
    db = await open({
      path,
      mode: "open",
      workerUrl,
      wasmUrl,
      resultTransport: transport,
    });
    const warmOpenMs = performance.now() - warmOpenStart;

    const importPath = `${path}.import`;
    let importDb = null;
    const importStart = performance.now();
    try {
      importDb = await open({
        path: importPath,
        mode: "openOrCreate",
        workerUrl,
        wasmUrl,
        resultTransport: transport,
      });
      await importDb.import(exported.bytes);
    } finally {
      if (importDb) {
        await importDb.close();
      }
    }
    const importMs = performance.now() - importStart;

    return {
      transport,
      rowCount: result.rowCount,
      durationMs,
      coldOpenMs,
      warmOpenMs,
      firstQueryMs,
      preparedLookupMs,
      insertTransactionMs,
      exportMs,
      importMs,
      exportedSize: exported.size,
      wasmMemoryBefore: before.wasmMemoryBytes,
      wasmMemoryAfter: after.wasmMemoryBytes,
      jsHeapBefore: before.jsHeapBytes,
      jsHeapAfter: after.jsHeapBytes,
      first: result.rows[0],
      last: result.rows[result.rows.length - 1],
    };
  } finally {
    if (db) {
      await db.close();
    }
  }
}

async function assetSizes() {
  const { workerUrl, wasmUrl } = urls();
  const [workerBytes, wasmJsBytes] = await Promise.all([
    fetch(workerUrl).then(async (response) => (await response.arrayBuffer()).byteLength),
    fetch(wasmUrl).then(async (response) => (await response.arrayBuffer()).byteLength),
  ]);
  return {
    workerBytes,
    wasmBindgenJsBytes: wasmJsBytes,
  };
}

globalThis.__runDecentDBWebTransportBench = async function (options = {}) {
  if (output) {
    output.textContent = "";
  }
  const rowCount = options.rowCount ?? 2_000;
  const lookupCount = options.lookupCount ?? 100;
  const insertCount = options.insertCount ?? 100;
  const scenarioId = options.scenarioId ?? Date.now().toString();
  const sizes = await assetSizes();
  const binary = await runOne("binary", { rowCount, lookupCount, insertCount, scenarioId });
  log(`binary rows=${binary.rowCount} durationMs=${binary.durationMs.toFixed(2)} wasmMemoryAfter=${binary.wasmMemoryAfter ?? "unknown"}`);
  const json = await runOne("json", { rowCount, lookupCount, insertCount, scenarioId });
  log(`json rows=${json.rowCount} durationMs=${json.durationMs.toFixed(2)} wasmMemoryAfter=${json.wasmMemoryAfter ?? "unknown"}`);
  return {
    rowCount,
    lookupCount,
    insertCount,
    assetSizes: sizes,
    binary,
    json,
    improvementRatio: json.durationMs > 0 ? binary.durationMs / json.durationMs : null,
  };
};
