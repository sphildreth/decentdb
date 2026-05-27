# @decentdb/web

Browser binding for DecentDB using a Dedicated Worker, WASM, OPFS, Web Locks,
and BroadcastChannel owner routing.

```ts
import { open, probeRuntime } from "@decentdb/web";

const probe = await probeRuntime();
if (!probe.supported) {
  throw new Error(probe.errors.map((error) => error.code).join(", "));
}

const db = await open({
  path: "app.ddb",
  mode: "openOrCreate",
  wasmUrl: new URL("./decentdb_wasm.js", import.meta.url).toString(),
  resultTransport: "binary",
});

await db.exec("CREATE TABLE IF NOT EXISTS notes(id INT64 PRIMARY KEY, body TEXT)");
await db.exec("INSERT INTO notes(id, body) VALUES ($1, $2)", [1, "hello"]);
const result = await db.query("SELECT id, body FROM notes");
await db.transaction(async (tx) => {
  await tx.exec("INSERT INTO notes(id, body) VALUES ($1, $2)", [2, "tx"]);
});
const metrics = await db.metrics();
await db.close();
```

## Runtime

- Main thread API is async.
- One Dedicated Worker owner owns the DecentDB handle for each logical path.
- Web Locks prevent competing browser owners for the same path.
- BroadcastChannel lets other tabs discover and route requests to the owner.
- The Rust engine remains synchronous inside WASM.
- OPFS sync access handles provide the browser VFS.
- Writes from all attached tabs are serialized through the owner.
- Service workers cannot own database handles.
- The browser sync API is owner-routed. Production relay helpers support
  `relayHello`, HTTP shape snapshot/pull/ack, and WebSocket shape
  subscriptions from supported page/worker contexts.

`metrics()` reports owner id/runtime, attached clients, protocol version,
capability flags, parser profile, quota/usage, persistence state, checkpoint/
export/import timestamps, and WASM memory samples. Browser system views are
also available through `query("SELECT * FROM sys.browser_runtime")`,
`sys.browser_owner`, `sys.browser_storage`, and `sys.browser_sync`.

## Build

Build the TypeScript package:

```bash
npm install
npm run build
```

Build the Rust wasm module separately with `wasm-bindgen --target web`, then
serve/pass the generated `decentdb_wasm.js` through `open({ wasmUrl })`.

## Browser test coverage

From the package root:

```bash
cd bindings/web
npm ci
npm run build
```

Build the wasm-bindgen artifact, then run the deterministic Playwright OPFS
smoke suite:

```bash
cd ../..
cargo build -p decentdb --target wasm32-unknown-unknown --release
wasm-bindgen target/wasm32-unknown-unknown/release/decentdb.wasm \
  --target web \
  --out-dir bindings/web/dist \
  --out-name decentdb_wasm
cd bindings/web
```

```bash
npm run browser:smoke
```

Optional browser matrix entries:

```bash
npm run browser:smoke:chrome
npm run browser:smoke:edge
npm run browser:smoke:candidate
```

Run the browser benchmark guardrail when changing the worker protocol, startup,
statement paging, import/export, or result decoding path:

```bash
npm run browser:bench
```

If the environment does not have browser binaries installed:

```bash
npm run browser:install
```

This runs `tests/bindings/web/smoke.spec.js` and
`tests/bindings/web/multitab.spec.js` with a real browser and OPFS-backed
storage assertions for capability probes, owner diagnostics, multi-tab routing,
create/open/query/reopen, binary and JSON result transports, export/import,
checkpoint, and persist coverage.

`browser:bench` runs `tests/bindings/web/transport-bench.spec.js` and reports
cold open, warm reopen, first query, prepared lookup, insert transaction,
export/import, binary-vs-JSON result transport timings, package asset sizes, and
WASM memory samples.

## Supported Browser SQL Profile

`@decentdb/web` exposes `browser-app-v2` through probe/runtime metadata.

Supported browser SQL includes:

- `CREATE TABLE` with ordinary columns, `NOT NULL`, `PRIMARY KEY`, `UNIQUE`,
  simple `CHECK`, `DEFAULT` values, and simple `REFERENCES` clauses.
- `DROP TABLE`, `CREATE INDEX`, `DROP INDEX`.
- `INSERT ... VALUES` and `INSERT ... SELECT` (for migration/import workflows).
- `UPDATE`, `DELETE`, and basic `SELECT` with boolean/comparison `WHERE`,
  `ORDER BY`, `LIMIT`, and `OFFSET`.

Unsupported grammar and parser profile cases return stable browser SQL errors:
`ERR_BROWSER_SQL_UNSUPPORTED`, `ERR_BROWSER_SQL_PARSE`,
`ERR_BROWSER_SQL_PROFILE_MISMATCH`.

The TypeScript API includes transactions, savepoints, prepared statement
`reset()`/`clearBindings()`/`page()`/async iteration, import/export, metrics,
and production relay helpers. Browser branch/snapshot workflows and browser TDE
open options are explicitly deferred and reported as disabled capability flags.

Relay subscriptions should apply locally before acking:

```ts
db.sync.subscribeShape({
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

Unsupported runtime modes fail explicitly with stable browser error codes such
as `ERR_BROWSER_COORDINATION_UNAVAILABLE`, `ERR_BROWSER_OPFS_UNAVAILABLE`,
`ERR_BROWSER_OWNER_TIMEOUT`, and `ERR_BROWSER_SERVICE_WORKER_UNSUPPORTED`.

OPFS persistence is browser-managed storage, not a replacement for explicit
sync/export of important data.
