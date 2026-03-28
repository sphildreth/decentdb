# Node.js bindings

DecentDB ships two in-tree Node packages under `bindings/node/`:

- `bindings/node/decentdb` — the `decentdb-native` N-API addon plus a thin `Database` / `Statement` wrapper
- `bindings/node/knex-decentdb` — a Knex dialect that rewrites Knex `?` placeholders to DecentDB's `$N` parameter style

## Native library requirement

Both packages load the DecentDB shared library at runtime.

Build it from the repository root:

```bash
cargo build -p decentdb
```

Then point Node at the shared library with `DECENTDB_NATIVE_LIB_PATH`:

- Linux: `target/debug/libdecentdb.so`
- macOS: `target/debug/libdecentdb.dylib`
- Windows: `target/debug/decentdb.dll`

## Native package quick start

```js
const { Database, timestampMicros } = require('decentdb-native');

const db = Database.openOrCreate('app.ddb');

db.exec('CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT, created_at TIMESTAMP)');
db.exec('INSERT INTO users VALUES ($1, $2, $3)', [1, 'Ada', timestampMicros(Date.now())]);

const rows = db.exec('SELECT id, name FROM users ORDER BY id').rows;
console.log(rows);
console.log(db.inTransaction);
console.log(Database.abiVersion(), Database.version());

db.close();
```

Important parameter note:

- `decentdb-native` expects DecentDB-native placeholders: `$1`, `$2`, ...
- it intentionally rejects JDBC/SQLite-style `?` placeholders
- use `knex-decentdb` when you want automatic `?` rewriting

## Open modes

The native wrapper now exposes explicit open-mode helpers:

- `Database.openOrCreate(path)`
- `Database.openExisting(path)`
- `Database.create(path)`
- `new Database({ path, mode: 'openOrCreate' | 'open' | 'create' })`

Unsupported arbitrary open-option strings are rejected instead of being silently ignored.

## Native wrapper API highlights

`Database`:

- `exec(sql, bindings?)`
- `execAsync(sql, bindings?)`
- `prepare(sql)`
- `beginTransaction()`, `commitTransaction()`, `rollbackTransaction()`
- `checkpoint()`
- `saveAs(destPath)`
- `inTransaction`
- `listTables()`, `getTableColumns(name)`, `listIndexes()`
- `getTableDdl(name)`, `listViewsInfo()`, `listViews()`, `getViewDdl(name)`, `listTriggers()`
- instance getters: `abiVersion`, `engineVersion`
- static helpers: `Database.abiVersion()`, `Database.version()`, `Database.evictSharedWal(path)`

`Statement`:

- `bindAll([...])`
- typed bind support for `null`, `bigint`, safe-integer `number`, `boolean`, `string`, `Buffer` / `Uint8Array`, `{ unscaled, scale }`, `Date`, and `timestampMicros(...)`
- `step()`
- `stepRowView()`
- `rowArray()`
- `rowsAffected()`
- `columnNames()`
- `fetchRowsI64TextF64(maxRows)` / `fetchRowsI64TextF64Number(maxRows)`
- `reBindInt64Execute(value)`
- `reBindTextInt64Execute(text, value)`
- `reBindInt64TextExecute(value, text)`

Both `Database` and `Statement` also register a `FinalizationRegistry` safety net so dropped objects do not permanently leak native handles, but explicit `close()` / `finalize()` is still preferred.

## Knex usage

```js
const knex = require('knex');
const { Client_DecentDB } = require('knex-decentdb');

const db = knex({
  client: Client_DecentDB,
  connection: { filename: 'app.ddb' },
  useNullAsDefault: true,
});
```

`knex-decentdb` now safely skips `?` inside:

- single-quoted strings
- double-quoted identifiers/strings
- `--` line comments
- `/* ... */` block comments

## Validation commands

Native package tests:

```bash
cd bindings/node/decentdb
DECENTDB_NATIVE_LIB_PATH=/absolute/path/to/target/debug/libdecentdb.so npm test
```

Knex package tests:

```bash
cd bindings/node/knex-decentdb
DECENTDB_NATIVE_LIB_PATH=/absolute/path/to/target/debug/libdecentdb.so npm test
```

Benchmarks:

```bash
cd bindings/node/decentdb
DECENTDB_NATIVE_LIB_PATH=/absolute/path/to/target/debug/libdecentdb.so npm run benchmark:fetch -- --count 100000 --point-reads 5000 --fetchmany-batch 1024 --db-prefix node_native_bench_fetch

cd ../knex-decentdb
DECENTDB_NATIVE_LIB_PATH=/absolute/path/to/target/debug/libdecentdb.so npm run benchmark:fetch -- --count 100000 --point-reads 5000 --fetchmany-batch 1024 --db-prefix node_knex_bench_fetch
```

Smoke path:

```bash
cargo build -p decentdb
bash tests/bindings/node/build.sh
node tests/bindings/node/smoke.js
```

## Current limitations

- generic async iteration still dispatches one libuv worker per row via `stmtNextAsync`; large scans should prefer bulk fetch helpers where possible
- generic result-handle APIs (`ddb_db_execute` + `ddb_result_*`) are not exposed yet
- generic batch APIs (`ddb_stmt_execute_batch_i64`, `ddb_stmt_execute_batch_typed`) are not exposed yet
- the Knex fast path is still benchmark-oriented for specific `(int64, text, float64)` shapes
- DecentDB's engine contract is still one writer / many readers per process; do not share one writable connection across uncontrolled concurrent writers
