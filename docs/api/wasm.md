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
- `beginTransaction()`, `commitTransaction()`, `rollbackTransaction()`
- `transaction(callback, options?)`
- `savepoint(name?)`, `releaseSavepoint(name)`, `rollbackToSavepoint(name)`
- `checkpoint()`
- `export()`
- `import(bytes)`
- `persist()`
- `metrics()`
- `close()`

The module also exports `probeRuntime(options?)`, which returns a structured
support report covering worker/coordination primitives, OPFS capability,
persistence/quota information, protocol version, parser profile, capability
flags, result transport, and stable browser error payloads. `open()` returns the
same protocol/capability metadata on the `Database` handle.

`Statement` methods:

- `bind(params)`
- `step()`
- `reset()`
- `clearBindings()`
- `page(pageSize?)`
- `iterate(pageSize?)` and async iteration
- `close()`

Prepared statement example:

```ts
const stmt = await db.prepare("SELECT id, title FROM todos WHERE id = $1");
await stmt.bind([1]);
const row = await stmt.step();
await stmt.reset();
for await (const row of stmt.iterate(100)) {
  console.log(row);
}
await stmt.close();
```

Closed database and statement handles fail with stable browser errors
(`ERR_BROWSER_DB_CLOSED` and `ERR_BROWSER_STATEMENT_CLOSED`). `import()` and
`close()` reject active prepared statements or active transactions instead of
silently tearing them down.

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

## Row Ownership

Browser rows are copied into JavaScript-owned objects before they cross the
worker boundary. `query()`, `Statement.step()`, `Statement.page()`, and
`Statement.iterate()` do not expose borrowed pointers into WASM linear memory or
native engine buffers.

Callers may retain returned row objects after stepping to another row, resetting
or closing the statement, and closing the database. Blob-like values are decoded
as JavaScript-owned byte arrays; mutating a returned array mutates only that
caller-owned copy.

Borrowed row-view APIs are intentionally not exposed in `browser-app-v2`.
If a future browser row-view API is added, it must document when borrowed memory
is invalidated and must include retaining-values tests for step, page, reset,
statement close, and database close.

Use `metrics()` when validating browser performance-sensitive changes. It
returns available worker-side samples such as current WASM linear-memory bytes,
pages, owner id/runtime, attached client count, coordination model, parser
profile, sync transport status, quota/usage estimates, persistent-storage
state, and Chrome JS heap samples when exposed.

Browser-only system views are intercepted by the web runtime:

```sql
SELECT * FROM sys.browser_runtime;
SELECT * FROM sys.browser_owner;
SELECT * FROM sys.browser_storage;
SELECT * FROM sys.browser_sync;
```

These views report browser runtime state without adding native hot-path cost.

## Production Relay Sync

The browser package exposes owner-routed relay helpers under `db.sync`:

```ts
await db.sync.configurePeer({
  name: "relay",
  endpoint: "https://relay.example.com",
  token,
  headers: {
    "x-decentdb-tenant": "tenant_42",
    "x-decentdb-subject": "user_123",
    "x-decentdb-roles": "user",
    "x-decentdb-shapes": "tenant_42_tasks_v1",
  },
});

const snapshot = await db.sync.shapeSnapshot({
  peer: "relay",
  shapeId: "tenant_42_tasks_v1",
  clientReplicaId: "web_123",
});

const subscription = db.sync.subscribeShape({
  peer: "relay",
  shapeId: "tenant_42_tasks_v1",
  clientReplicaId: "web_123",
  onMessage(message) {
    void db.sync.applyAndAckShape({
      peer: "relay",
      message,
      tenantId: "tenant_42",
      subjectId: "user_123",
      clientReplicaId: "web_123",
    });
  },
});
```

HTTP helpers use normal fetch headers. WebSocket subscriptions pass short-lived
principal context in the stream URL because browser WebSocket APIs cannot set
custom headers; deploy relay streams over TLS.

`applyAndAckShape()` applies the delivered public changeset through the local
engine first, waits for that transaction to succeed, and only then sends the
relay ack. Applications that intentionally ack before local apply must do that
manually and own the documented data-loss risk.

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
  browser handle; imports require one attached client, no active statements, and
  no active transaction.
- `metrics()` and `sys.browser_storage` report last checkpoint/export/import
  timestamps and storage pressure when browser quota estimates are available.
- Applications with important data should sync or export explicitly.

Backup and restore example:

```ts
const backup = await db.export();
await downloadBlob(new Blob([backup.bytes]), "todos.ddb");

const restoredBytes = await selectedFile.arrayBuffer();
await db.import(restoredBytes);
```

## SQL Profile (Parser)

- Browser runtime exposes `parserProfile: "browser-app-v2"` from
  `probeRuntime()`, `open()`, `metrics()`, and `sys.browser_runtime`.
- `browser-app-v2` expands the in-repo wasm parser. No parser dependency or
  native `pg_query` wasm port was added.
- Supported for browser workflows: `CREATE TABLE` (including primary key,
  `NOT NULL`, `UNIQUE`, simple `CHECK`, `REFERENCES`, and `DEFAULT` values),
  `DROP TABLE`, `CREATE INDEX`, `DROP INDEX`, `INSERT ... VALUES`,
  `INSERT ... SELECT`, `UPDATE`, `DELETE`, and `SELECT` with `WHERE`, `ORDER BY`,
  `LIMIT`, and `OFFSET`.
- Generated columns, CTEs, joins beyond the current browser profile, Lua
  extension SQL, and native-only platform features are explicit browser-profile
  deferrals.
- Unsupported-by-browser SQL surfaces still return stable browser SQL errors:
  `ERR_BROWSER_SQL_UNSUPPORTED`, `ERR_BROWSER_SQL_PARSE`, and
  `ERR_BROWSER_SQL_PROFILE_MISMATCH`.
- Browser branch/snapshot workflow APIs and browser TDE open options are
  deferred in this release and exposed as `branchSnapshots: false` and
  `browserTdeOpenOptions: false` in capability metadata.
- Browser sync relay helpers require an application-hosted production relay.
  The legacy `sync.run()` peer-to-peer workflow remains a compatibility shell
  for builds that have not configured a relay.

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

Run the browser benchmark guardrail when changing startup, result encoding,
statement paging, import/export, or worker protocol paths:

```bash
npm run browser:bench
```

The benchmark records cold open, warm reopen, first query, prepared point
lookups, transaction insert batches, large-result binary and JSON decoding,
export/import, package asset sizes, and WASM/JS memory samples. The release
guardrail fails when the binary large-result path is materially slower than JSON
or when startup/query times exceed the broad browser threshold in
`transport-bench.spec.js`; tighter release thresholds should be set from the
recorded CI baseline for the target browser channel.

## Framework Recipes

### Static ESM

Keep one module-level handle and serve `worker.js`, `decentdb_wasm.js`, and the
generated `.wasm` asset from the same static origin:

```ts
import { open, type Database } from "@decentdb/web";

let db: Database | undefined;

export async function getDb() {
  db ??= await open({ path: "app.ddb" });
  return db;
}

addEventListener("pagehide", () => void db?.close());
```

### Vite

Copy the built `bindings/web/dist` assets into `public/decentdb/` or serve them
from your package asset pipeline, then pass explicit URLs:

```ts
await open({
  path: "app.ddb",
  workerUrl: new URL("/decentdb/worker.js", location.origin).toString(),
  wasmUrl: new URL("/decentdb/decentdb_wasm.js", location.origin).toString(),
});
```

### Next.js

Use `@decentdb/web` only from client components or dynamic imports with SSR
disabled. Keep the handle in a client-side provider and close it on `pagehide`.

```tsx
"use client";

import { useEffect, useState } from "react";
import type { Database } from "@decentdb/web";

export function DecentDbProvider() {
  const [db, setDb] = useState<Database>();

  useEffect(() => {
    let handle: Database | undefined;
    void import("@decentdb/web").then(async ({ open }) => {
      handle = await open({ path: "app.ddb" });
      setDb(handle);
    });
    return () => void handle?.close();
  }, []);

  return null;
}
```

For the pages router, use the same dynamic import from `useEffect`; do not open
the browser runtime during `getServerSideProps`, route handlers, or middleware.

### SvelteKit

Open from `onMount` only, with SSR imports guarded by the browser lifecycle:

```ts
import { onMount } from "svelte";

onMount(() => {
  let db;
  import("@decentdb/web").then(async ({ open }) => {
    db = await open({ path: "app.ddb" });
  });
  return () => void db?.close();
});
```

### Electron And Tauri Webviews

Use the renderer/webview process, not the main process, and verify the webview
actually exposes Dedicated Worker, BroadcastChannel, Web Locks, and OPFS sync
access handles with `probeRuntime()`. If a webview lacks those capabilities,
use a native binding in the host process instead of falling back to weaker
browser storage.

### React

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
- `ERR_BROWSER_SQL_PARSE`, `ERR_BROWSER_SQL_UNSUPPORTED`, or
  `ERR_BROWSER_SQL_PROFILE_MISMATCH`: the SQL is invalid, outside
  `browser-app-v2`, or requires a newer/different browser SQL profile.
- `ERR_BROWSER_DB_CLOSED` or `ERR_BROWSER_STATEMENT_CLOSED`: create a fresh
  handle or statement instead of reusing a closed one.
- `ERR_BROWSER_ACTIVE_STATEMENTS` or `ERR_BROWSER_TRANSACTION_ACTIVE`: close
  statements and commit/rollback before closing or importing.
- `ERR_BROWSER_BRANCH_UNSUPPORTED`: branch/snapshot workflows are native-only in
  this browser profile.
- `ERR_BROWSER_SERVICE_WORKER_UNSUPPORTED`: open the database from an
  application page/runtime owner instead of a service worker.
- `ERR_BROWSER_SYNC_PEER_NOT_CONFIGURED`: configure the relay with
  `db.sync.configurePeer()` before calling relay helpers.
- `ERR_BROWSER_SYNC_PRINCIPAL_REQUIRED`: provide relay principal context for a
  WebSocket shape subscription.
- `ERR_BROWSER_SYNC_WEBSOCKET`: the relay stream could not be opened or was
  closed by the browser/runtime.
