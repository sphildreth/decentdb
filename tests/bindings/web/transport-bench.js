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
    db = await open({
      path,
      mode: "openOrCreate",
      workerUrl,
      wasmUrl,
      resultTransport: transport,
    });
    await seed(db, options.rowCount);
    const before = await db.metrics();
    const start = performance.now();
    const result = await db.query(`SELECT id, name FROM ${TABLE} ORDER BY id ASC`);
    const durationMs = performance.now() - start;
    const after = await db.metrics();
    return {
      transport,
      rowCount: result.rowCount,
      durationMs,
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

globalThis.__runDecentDBWebTransportBench = async function (options = {}) {
  if (output) {
    output.textContent = "";
  }
  const rowCount = options.rowCount ?? 2_000;
  const scenarioId = options.scenarioId ?? Date.now().toString();
  const binary = await runOne("binary", { rowCount, scenarioId });
  log(`binary rows=${binary.rowCount} durationMs=${binary.durationMs.toFixed(2)} wasmMemoryAfter=${binary.wasmMemoryAfter ?? "unknown"}`);
  const json = await runOne("json", { rowCount, scenarioId });
  log(`json rows=${json.rowCount} durationMs=${json.durationMs.toFixed(2)} wasmMemoryAfter=${json.wasmMemoryAfter ?? "unknown"}`);
  return {
    rowCount,
    binary,
    json,
    improvementRatio: json.durationMs > 0 ? binary.durationMs / json.durationMs : null,
  };
};
