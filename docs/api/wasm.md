# WASM / Browser

`@decentdb/web` runs DecentDB inside a browser Dedicated Worker. The worker
loads the Rust engine compiled to `wasm32-unknown-unknown`, owns the database
handle, and stores database bytes in OPFS through synchronous access handles.
Production browser ownership is coordinated per logical path: one active
Dedicated Worker owns OPFS handles, Web Locks prevent competing owners, and
other tabs route requests to that owner over BroadcastChannel.

The application-facing API is async TypeScript:

```ts
import { open, probeRuntime } from "@decentdb/web";

const probe = await probeRuntime();
if (!probe.supported) {
  console.error(probe.errors);
  throw new Error("DecentDB browser runtime is unsupported here");
}

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

- The main thread talks to a Dedicated Worker owner over a typed RPC protocol.
- The worker prepares OPFS sync access handles before opening the engine.
- The Rust engine remains synchronous internally.
- One owner runtime is active per logical OPFS database path.
- Web Locks guard owner creation so two write-capable owners do not open the
  same path.
- BroadcastChannel discovers the owner and routes non-owner tab requests through
  it.
- Operations on a handle are serialized by the owner worker.
- If an owner tab disappears, later requests recover by opening a new owner and
  replaying WAL through normal engine open.

Service workers cannot own DecentDB browser database handles. They should wake
or notify an application page; sync-capable work must route through an active
page-owned runtime.

## Compatibility

| Environment | Status |
|---|---|
| Chromium-family desktop browsers with Dedicated Worker, BroadcastChannel, Web Locks, and OPFS sync access handles | Tier 1 when CI-covered |
| Chrome and Edge branded channels | Tier 1 release targets; run `browser:smoke:chrome` / `browser:smoke:edge` when installed |
| Firefox | Candidate tier until the same ownership, OPFS, recovery, and performance checks are promoted |
| Browsers without OPFS sync access handles, BroadcastChannel, or Web Locks | Unsupported |
| Service workers, private/ephemeral storage modes with failed probes, disabled worker/storage APIs | Unsupported |
| Node.js | Use native bindings instead of `@decentdb/web` |

Support is capability-gated, not user-agent-gated. DecentDB does not silently
fall back to IndexedDB, localStorage, or in-memory storage under the browser
durability contract.

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
- `openTimeoutMs`: optional owner discovery/request timeout.
- `skipRuntimeProbe`: advanced testing escape hatch; production code should keep
  startup probing enabled.

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

The module also exports `probeRuntime(options?)`, which returns a structured
support report covering worker/coordination primitives, OPFS capability,
persistence/quota information, parser profile, result transport, and stable
browser error payloads.

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

Use DecentDB positional parameters (`$1`, `$2`, ...). The browser bridge accepts
`null`, booleans, numbers, strings, `bigint`, `Uint8Array`, `ArrayBuffer`, and
tagged semantic values such as `{ kind: "bytes", base64 }`,
`{ kind: "decimal", scaled, scale }`, `{ kind: "uuid", bytes }`,
`{ kind: "dateDays", value }`, `{ kind: "timeMicros", value }`,
`{ kind: "timestampMicros", value }`, `{ kind: "timestampTzMicros", value }`,
and `{ kind: "interval", months, days, micros }`.

## Result Transport

The worker uses a compact binary result frame by default. The frame carries
column names, affected-row count, and typed cell values in a transferable byte
buffer before decoding rows on the main-thread side. This avoids row-by-row JSON
serialization for large reads.

Set `resultTransport: "json"` only for debugging or compatibility comparisons.

Use `metrics()` when validating browser performance-sensitive changes. It
returns available worker-side samples such as current WASM linear-memory bytes,
pages, owner id/runtime, attached client count, coordination model, parser
profile, sync-deferred status, quota/usage estimates, persistent-storage state,
and Chrome JS heap samples when exposed.

Browser-only system views are intercepted by the web runtime:

```sql
SELECT * FROM sys.browser_runtime;
SELECT * FROM sys.browser_owner;
SELECT * FROM sys.browser_storage;
SELECT * FROM sys.browser_sync;
```

These views report browser runtime state without adding native hot-path cost.

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

- Browser SQL parsing currently uses the `browser-app-v1` wasm-target parser
  profile because the
  native `pg_query` C parser does not build for `wasm32-unknown-unknown`.
  Native DecentDB keeps the full parser path.
- The browser parser profile is suitable for local-first application bootstrap
  and smoke workflows: `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`,
  `DROP INDEX`, `INSERT ... VALUES`, `UPDATE`, `DELETE`, and basic `SELECT`
  with simple boolean/comparison `WHERE`, `ORDER BY`, `LIMIT`, and `OFFSET`.
- Browser sync exposes an owner-routed API shell, but production relay transport
  remains deferred to the production sync relay phase and returns explicit
  deferred results/errors.

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

Optional tier/candidate runs:

```bash
npm run browser:smoke:chrome
npm run browser:smoke:edge
npm run browser:smoke:candidate
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
from `onUnmounted` or `onDestroy` only when the app instance is leaving. Multiple
tabs can open the same path; the runtime routes through one active owner.

## Troubleshooting

- `ERR_BROWSER_WASM_EXPORT_NOT_AVAILABLE`: the worker could not import the
  wasm-bindgen module or it did not export `decentdbOpen`.
- `ERR_BROWSER_COORDINATION_UNAVAILABLE`: Dedicated Worker, BroadcastChannel, or
  Web Locks are missing.
- `ERR_BROWSER_OPFS_UNAVAILABLE` or
  `ERR_BROWSER_SYNC_ACCESS_HANDLE_UNAVAILABLE`: run in a browser/worker
  environment that supports OPFS synchronous access handles.
- `ERR_BROWSER_OWNER_TIMEOUT` or `ERR_BROWSER_OWNER_STALE`: retry the operation;
  if the previous owner disappeared, the next request can recover ownership and
  replay WAL through normal open.
- `ERR_BROWSER_SERVICE_WORKER_UNSUPPORTED`: open the database from an
  application page/runtime owner instead of a service worker.
- `ERR_BROWSER_SYNC_DEFERRED`: browser sync API shape is present, but production
  sync transport is deferred to the sync relay work.
