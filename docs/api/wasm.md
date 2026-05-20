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

`Database` methods:

- `exec(sql, params?)`
- `query(sql, params?)`
- `prepare(sql)`
- `checkpoint()`
- `export()`
- `import(bytes)`
- `persist()`
- `close()`

`Statement` methods:

- `bind(params)`
- `step()`
- `close()`

## SQL Parameters

Use DecentDB positional parameters (`$1`, `$2`, ...). The initial browser bridge
accepts JSON-compatible parameter values: `null`, booleans, numbers, and strings.
Binary and native semantic values will be expanded in later browser binding
slices.

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

## Current Limitations

- Browser SQL parsing currently uses a small wasm-target parser because the
  native `pg_query` C parser does not build for `wasm32-unknown-unknown`.
  Native DecentDB keeps the full parser path.
- The initial wasm parser is suitable for smoke use and simple browser examples:
  `CREATE TABLE`, `INSERT ... VALUES`, and basic `SELECT`.
- Cross-tab writes, Shared Worker coordination, service worker use, and
  multi-worker WAL sharing are out of scope for v1.
- Large-result binary transport is not yet implemented; current worker transport
  returns JSON-compatible rows.

## Troubleshooting

- `ERR_BROWSER_WASM_EXPORT_NOT_AVAILABLE`: the worker could not import the
  wasm-bindgen module or it did not export `decentdbOpen`.
- `OPFS getDirectory() is unavailable`: run in a browser/worker environment that
  supports OPFS synchronous access handles.
- Open failures in development hot reload usually mean more than one worker is
  trying to own the same logical database path. Close old handles and reload.
