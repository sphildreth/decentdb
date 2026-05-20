# WASM / Browser

`@decentdb/web` runs DecentDB inside a browser Dedicated Worker. The worker
loads the Rust engine compiled to `wasm32-unknown-unknown`, owns the database
handle, and stores database bytes in OPFS through synchronous access handles.

The application-facing API is async TypeScript:

```ts
import { open } from "@decentdb/web";

const db = await open({
  path: "todos.ddb",
  mode: "openOrCreate",
  wasmUrl: new URL("./decentdb_wasm.js", import.meta.url).toString(),
});

await db.exec("CREATE TABLE IF NOT EXISTS todos(id INT64 PRIMARY KEY, title TEXT)");
await db.exec("INSERT INTO todos(id, title) VALUES ($1, $2)", [1, "offline"]);

const result = await db.query("SELECT id, title FROM todos WHERE id = $1", [1]);
console.log(result.rows);

console.log(await db.metrics());

await db.close();
```

## Runtime Model

- The main thread talks to a Dedicated Worker over a typed RPC protocol.
- The worker prepares OPFS sync access handles before opening the engine.
- The Rust engine remains synchronous internally.
- One worker owns one logical database handle.
- Operations on a handle are serialized by the worker.

Browser v1 does not provide cross-tab or cross-worker write coordination. Use a
single shared `Database` instance per logical OPFS database path.

## Compatibility

| Environment | Status |
|---|---|
| Chromium with OPFS synchronous access handles in a Dedicated Worker | Supported and covered by automated smoke/benchmark tests |
| Browsers without OPFS synchronous access handles | Unsupported for v1 |
| Service workers, shared workers, multi-tab write ownership | Unsupported for v1 |
| Node.js | Use native bindings instead of `@decentdb/web` |

## Build Shape

The package expects a wasm-bindgen web build of the `decentdb` crate:

```bash
cargo build -p decentdb --target wasm32-unknown-unknown --release
wasm-bindgen \
  ../../target/wasm32-unknown-unknown/release/decentdb.wasm \
  --target web \
  --out-dir dist \
  --out-name decentdb_wasm
```

Then build the TypeScript package:

```bash
cd bindings/web
npm install
npm run build
```

Pass the emitted `decentdb_wasm.js` URL through `open({ wasmUrl })` when the
default colocated worker layout does not match your bundler.

## API

`open(options)` returns a `Database`.

Options:

- `path`: logical OPFS database path.
- `mode`: `openOrCreate`, `open`, or `create`.
- `workerUrl`: optional custom worker module URL.
- `wasmUrl`: optional custom wasm-bindgen JavaScript module URL.
- `resultTransport`: optional `binary` or `json`; `binary` is the default.

`Database` methods:

- `exec(sql, params?)`
- `query(sql, params?)`
- `prepare(sql)`
- `checkpoint()`
- `export()`
- `import(bytes)`
- `persist()`
- `metrics()`
- `close()`

`Statement` methods:

- `bind(params)`
- `step()`
- `close()`

Prepared statement example:

```ts
const stmt = await db.prepare("SELECT id, title FROM todos WHERE id = $1");
await stmt.bind([1]);
const row = await stmt.step();
await stmt.close();
```

## SQL Parameters

Use DecentDB positional parameters (`$1`, `$2`, ...). The browser v1 bridge
accepts JSON-compatible parameter values: `null`, booleans, numbers, and strings.
Binary and native semantic values will be expanded in later browser binding
slices.

## Result Transport

The worker uses a compact binary result frame by default. The frame carries
column names, affected-row count, and typed cell values in a transferable byte
buffer before decoding rows on the main-thread side. This avoids row-by-row JSON
serialization for large reads.

Set `resultTransport: "json"` only for debugging or compatibility comparisons.

Use `metrics()` when validating browser performance-sensitive changes. It
returns available worker-side samples such as current WASM linear-memory bytes
and pages; Chrome may also report worker JS heap usage.

## Persistence And Durability

OPFS is the primary browser persistence backend. DecentDB maps WAL/data flushes
to OPFS sync access handle `flush()` calls, but browser storage is still subject
to browser policy, quota pressure, profile clearing, and eviction behavior.

Important boundaries:

- OPFS durability is not identical to native filesystem power-loss guarantees.
- `persist()` requests persistent storage where the browser supports it, but it
  is not an unconditional retention guarantee.
- `export()` checkpoints first and returns a checkpointed database byte image.
- `import(bytes)` replaces the database image and clears the WAL for that
  browser handle.
- Applications with important data should sync or export explicitly.

Backup and restore example:

```ts
const backup = await db.export();
await downloadBlob(new Blob([backup.bytes]), "todos.ddb");

const restoredBytes = await selectedFile.arrayBuffer();
await db.import(restoredBytes);
```

## Current Limitations

- Browser SQL parsing currently uses a small wasm-target parser because the
  native `pg_query` C parser does not build for `wasm32-unknown-unknown`.
  Native DecentDB keeps the full parser path.
- The browser v1 parser is suitable for local-first application bootstrap and
  smoke workflows: `CREATE TABLE`, `DROP TABLE`, `INSERT ... VALUES`, `DELETE`,
  and basic `SELECT` with simple `WHERE` and `ORDER BY`.
- Cross-tab writes, Shared Worker coordination, service worker use, and
  multi-worker WAL sharing are out of scope for v1.

## Browser Smoke

The repo includes a real Chromium OPFS smoke:

```bash
cd bindings/web
npm ci
npm run build
cd ../..
cargo build -p decentdb --target wasm32-unknown-unknown --release
wasm-bindgen target/wasm32-unknown-unknown/release/decentdb.wasm \
  --target web \
  --out-dir bindings/web/dist \
  --out-name decentdb_wasm
cd bindings/web
npm run browser:install
npm run browser:smoke
```

Run the transport benchmark when changing result encoding or decoding:

```bash
npm run browser:bench
```

The benchmark compares binary and JSON result transports on the same large
result shape and reports query time plus WASM memory samples.

## Frontend Integration Notes

Vanilla TypeScript keeps one module-level handle:

```ts
import { open, type Database } from "@decentdb/web";

let db: Database | undefined;

export async function getDb() {
  db ??= await open({ path: "app.ddb" });
  return db;
}

addEventListener("pagehide", () => void db?.close());
```

React should open once for the app shell and close during cleanup:

```tsx
useEffect(() => {
  let disposed = false;
  let handle: Database | undefined;

  open({ path: "app.ddb" }).then((db) => {
    if (disposed) void db.close();
    else handle = db;
  });

  return () => {
    disposed = true;
    void handle?.close();
  };
}, []);
```

Vue and Svelte follow the same lifecycle rule: create one browser `Database`
for the logical OPFS path, share it through app context/store state, and close it
from `onUnmounted` or `onDestroy` only when the owning app instance is leaving.

## Troubleshooting

- `ERR_BROWSER_WASM_EXPORT_NOT_AVAILABLE`: the worker could not import the
  wasm-bindgen module or it did not export `decentdbOpen`.
- `OPFS getDirectory() is unavailable`: run in a browser/worker environment that
  supports OPFS synchronous access handles.
- Open failures in development hot reload usually mean more than one worker is
  trying to own the same logical database path. Close old handles and reload.
