# decentdb-native

Node.js bindings for DecentDB via N-API.

- Native layer: N-API addon (C) dynamically loading `libdecentdb`
- API shape: `Database` + `Statement` wrapper classes
- Concurrency: Non-blocking async execution via N-API thread pool
- Parameters: Postgres-style positional (`$1`, `$2`, ...)

## Features

- Postgres-style parameters (`$1`, `$2`, ...)
- Streaming async iteration (`for await (const row of stmt.rows())`)
- Async execution (`db.execAsync`)
- Synchronous execution (`db.exec`)
- Full transaction support (BEGIN/COMMIT/ROLLBACK)
- Decimal type support (`{ unscaled: BigInt, scale: number }`)
- Blob/Buffer support
- Schema introspection (`listTables`, `getTableColumns`, `listIndexes`)
- WAL checkpoint (`db.checkpoint()`)
- Auto-increment for INTEGER PRIMARY KEY columns

## Quick Start

```js
const { Database } = require('decentdb-native');

const db = new Database({ path: 'my.db' });

db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');
db.exec('INSERT INTO users (name, email) VALUES ($1, $2)', ['Alice', 'alice@example.com']);

const { rows } = db.exec('SELECT * FROM users');
console.log(rows); // [[1n, 'Alice', 'alice@example.com']]

db.close();
```

### Async iteration

```js
const stmt = db.prepare('SELECT * FROM users');
for await (const row of stmt.rows()) {
  console.log(row);
}
stmt.finalize();
```

### Transactions

```js
db.exec('BEGIN');
db.exec('INSERT INTO users (name, email) VALUES ($1, $2)', ['Bob', 'bob@example.com']);
db.exec('COMMIT');
```

### Schema introspection

```js
db.listTables();                // ['users']
db.getTableColumns('users');    // [{ name: 'id', type: 'BIGINT', ... }, ...]
db.listIndexes();               // [{ name: '...', table: 'users', ... }]
```

## Build

From this directory:

```sh
npm install
npm run build
```

## Runtime native library

The addon dynamically loads the DecentDB native library at runtime.

Set `DECENTDB_NATIVE_LIB_PATH` to an absolute path to `libdecentdb.so` (Linux), `libdecentdb.dylib` (macOS), or `decentdb.dll` (Windows).

### Example (Linux)

1. Build the library from the repo root:
   ```sh
   cargo build -p decentdb
   ```
2. Run tests:
   ```sh
   DECENTDB_NATIVE_LIB_PATH=/absolute/path/to/target/debug/libdecentdb.so npm test
   ```

## Testing

```sh
DECENTDB_NATIVE_LIB_PATH=/path/to/libdecentdb.so npm test
```

All tests use Node.js built-in test runner (`node:test`) — no additional test dependencies required.
