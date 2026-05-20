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

`metrics()` reports owner id/runtime, attached clients, parser profile,
quota/usage, persistence state, and WASM memory samples. Browser system views
are also available through `query("SELECT * FROM sys.browser_runtime")`,
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

Run the large-result transport benchmark when changing the worker protocol or
result decoding path:

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
binary-vs-JSON result transport timings plus WASM memory samples.

## Current Limits

The browser `browser-app-v1` wasm parser is intentionally scoped because the
native `pg_query` C parser is not available on `wasm32-unknown-unknown`.
Browser workflows cover `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`,
`DROP INDEX`, `INSERT ... VALUES`, `UPDATE`, `DELETE`, and basic `SELECT` with
simple boolean/comparison `WHERE`, `ORDER BY`, `LIMIT`, and `OFFSET`; native
DecentDB continues to provide the broader SQL surface.

Unsupported runtime modes fail explicitly with stable browser error codes such
as `ERR_BROWSER_COORDINATION_UNAVAILABLE`, `ERR_BROWSER_OPFS_UNAVAILABLE`,
`ERR_BROWSER_OWNER_TIMEOUT`, and `ERR_BROWSER_SERVICE_WORKER_UNSUPPORTED`.

OPFS persistence is browser-managed storage, not a replacement for explicit
sync/export of important data.
