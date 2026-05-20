# @decentdb/web

Browser binding for DecentDB using a Dedicated Worker, WASM, and OPFS.

```ts
import { open } from "@decentdb/web";

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
- A Dedicated Worker owns the DecentDB handle.
- The Rust engine remains synchronous inside WASM.
- OPFS sync access handles provide the browser VFS.
- Writes are serialized through the worker; cross-tab write coordination is out
  of scope for v1.

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

Run the large-result transport benchmark when changing the worker protocol or
result decoding path:

```bash
npm run browser:bench
```

If the environment does not have browser binaries installed:

```bash
npm run browser:install
```

This runs `tests/bindings/web/smoke.spec.js` with a real browser and OPFS-backed
storage assertions for create/open/query/reopen, binary and JSON result
transports, export/import, checkpoint, and persist coverage.

`browser:bench` runs `tests/bindings/web/transport-bench.spec.js` and reports
binary-vs-JSON result transport timings plus WASM memory samples.

## Current Limits

The browser v1 wasm parser is intentionally scoped because the native `pg_query`
C parser is not available on `wasm32-unknown-unknown`. Browser workflows cover
`CREATE TABLE`, `DROP TABLE`, `INSERT ... VALUES`, `DELETE`, and basic `SELECT`
with simple `WHERE` and `ORDER BY`; native DecentDB continues to provide the
broader SQL surface.

OPFS persistence is browser-managed storage, not a replacement for explicit
sync/export of important data.
