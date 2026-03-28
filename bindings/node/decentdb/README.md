# decentdb-native

Node.js bindings for DecentDB via N-API.

## Highlights

- explicit open modes: `Database.openOrCreate()`, `openExisting()`, `create()`
- sync `exec()` and promise-based `execAsync()`
- native prepared statements via `Database.prepare()` / `Statement`
- schema helpers: tables, columns, indexes, table DDL, views, view DDL, triggers
- transaction helpers plus `db.inTransaction`
- version helpers: `Database.abiVersion()`, `Database.version()`
- timestamp binding via `Statement.bindTimestamp(...)` or `timestampMicros(...)`
- re-execute helpers for common keyed DML patterns
- GC safety net via `FinalizationRegistry`

## Quick start

```js
const { Database, timestampMicros } = require('decentdb-native');

const db = Database.openOrCreate('app.ddb');
db.exec('CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT, created_at TIMESTAMP)');
db.exec('INSERT INTO users VALUES ($1, $2, $3)', [1, 'Ada', timestampMicros(Date.now())]);
console.log(db.exec('SELECT id, name FROM users').rows);
db.close();
```

DecentDB-native uses engine-native placeholders: `$1`, `$2`, ...

## Runtime library

Build the shared library from the repository root:

```bash
cargo build -p decentdb
```

Set `DECENTDB_NATIVE_LIB_PATH` to the built library if auto-detection does not find it.

## Tests and benchmark

```bash
DECENTDB_NATIVE_LIB_PATH=/absolute/path/to/target/debug/libdecentdb.so npm test
DECENTDB_NATIVE_LIB_PATH=/absolute/path/to/target/debug/libdecentdb.so npm run benchmark:fetch -- --count 100000 --point-reads 5000 --fetchmany-batch 1024 --db-prefix node_native_bench_fetch
```

## Limitations

- generic async iteration still uses one worker dispatch per row
- generic result-handle and generic batch APIs are still not wrapped
- one writer / many readers remains the engine-level concurrency contract
