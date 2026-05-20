# @decentdb/web

Browser binding for DecentDB using a Dedicated Worker, WASM, and OPFS.

```ts
import { open } from "@decentdb/web";

const db = await open({
  path: "app.ddb",
  mode: "openOrCreate",
  wasmUrl: new URL("./decentdb_wasm.js", import.meta.url).toString(),
});

await db.exec("CREATE TABLE IF NOT EXISTS notes(id INT64 PRIMARY KEY, body TEXT)");
await db.exec("INSERT INTO notes(id, body) VALUES ($1, $2)", [1, "hello"]);
const result = await db.query("SELECT id, body FROM notes");
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
npm install
npm run build
```

Run the deterministic Playwright OPFS smoke suite:

```bash
npm run browser:smoke
```

If the environment does not have browser binaries installed:

```bash
npm run browser:install
```

This runs `tests/bindings/web/smoke.spec.js` with a real browser and OPFS-backed
storage assertions for create/open/query/reopen, export/import, checkpoint, and
persist coverage.

## Current Limits

The initial wasm parser is intentionally narrow because the native `pg_query`
C parser is not available on `wasm32-unknown-unknown`. Simple browser workflows
cover `CREATE TABLE`, `INSERT ... VALUES`, and basic `SELECT`; native DecentDB
continues to provide the broader SQL surface.

OPFS persistence is browser-managed storage, not a replacement for explicit
sync/export of important data.
