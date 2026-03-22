# Node.js Bindings

DecentDB's Node.js integrations live under `bindings/node/`:

- `bindings/node/decentdb`: N-API native addon + JS wrapper (`Database`, `Statement`)
- `bindings/node/knex-decentdb`: Knex client/dialect with automatic placeholder rewriting

## Build

```bash
# Build the native C library
cargo build_lib

# Build the Node addon
cd bindings/node/decentdb
npm install
npm run build

# Set the library path
export DECENTDB_NATIVE_LIB_PATH=$PWD/../../../build/libc_api.so
```

## Database API

```javascript
const { Database } = require('decentdb');

const db = new Database({ path: '/tmp/sample.ddb' });

// DDL
db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)');

// INSERT with auto-increment (omit id column)
db.exec('INSERT INTO users (name, email) VALUES ($1, $2)', ['Alice', 'alice@example.com']);
db.exec('INSERT INTO users (name, email) VALUES ($1, $2)', ['Bob', 'bob@example.com']);

// SELECT
const { rows } = db.exec('SELECT id, name, email FROM users ORDER BY id');
for (const [id, name, email] of rows) {
  console.log(`id=${id} name=${name} email=${email}`);
}

db.close();
```

### Async Iteration

```javascript
const db = new Database({ path: '/tmp/sample.ddb' });
const stmt = db.prepare('SELECT * FROM users');

for await (const row of stmt.rows()) {
  console.log(row);
}

stmt.finalize();
```

### Transactions

```javascript
db.exec('BEGIN');
db.exec('INSERT INTO users (name) VALUES ($1)', ['Carol']);
db.exec('COMMIT');
// or db.exec('ROLLBACK');
```

### Supported Bind Types

| JavaScript Type | DecentDB Type |
|----------------|---------------|
| `null` / `undefined` | NULL |
| `bigint` | INT64 |
| `number` (safe integer) | INT64 |
| `number` (float) | FLOAT64 |
| `boolean` | BOOL |
| `string` | TEXT |
| `Buffer` / `Uint8Array` | BLOB |
| `{ unscaled: bigint, scale: number }` | DECIMAL |

### TIMESTAMP values

TIMESTAMP columns are stored as microseconds since Unix epoch (UTC). When reading results, the addon returns TIMESTAMP values as a number (milliseconds since epoch); wrap with `new Date(ms)`.

To bind a TIMESTAMP parameter, pass a `bigint` microsecond value:

```javascript
db.exec('CREATE TABLE events (id INTEGER PRIMARY KEY, occurred_at TIMESTAMP)');
const micros = BigInt(Date.now()) * 1000n;
db.exec('INSERT INTO events (occurred_at) VALUES ($1)', [micros]);

const { rows } = db.exec('SELECT occurred_at FROM events');
console.log(new Date(rows[0][0]).toISOString());
```

### Checkpoint

```javascript
db.checkpoint();  // flush WAL to main database file
```

### In-Memory Databases

Use `:memory:` for an ephemeral in-memory database (case-insensitive):

```javascript
const db = new Database({ path: ':memory:' });
db.exec('CREATE TABLE cache (key TEXT PRIMARY KEY, val TEXT)');
db.exec("INSERT INTO cache (key, val) VALUES ($1, $2)", ['k1', 'hello']);
// Data is lost when the database is closed
db.close();
```

### SaveAs (Export to Disk)

Export any open database — including `:memory:` — to a new on-disk file:

```javascript
const db = new Database({ path: ':memory:' });
db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)');
db.exec('INSERT INTO items (id, name) VALUES ($1, $2)', [1, 'widget']);

db.saveAs('/tmp/snapshot.ddb');
db.close();
```

`saveAs` performs a full checkpoint, then copies all pages atomically. The destination must not already exist.

### Schema Introspection

```javascript
// List all tables
const tables = db.listTables();  // ['users', 'orders']

// Get column metadata
const cols = db.getTableColumns('users');
// [{ name: 'id', type: 'INT64', primary_key: true, not_null: true }, ...]

// List all indexes
const indexes = db.listIndexes();
// [{ name: 'idx_users_email', table: 'users', columns: ['email'], unique: false }, ...]
```

## Knex Integration

The `knex-decentdb` package provides a Knex dialect:

```javascript
const knex = require('knex');

const db = knex({
  client: require('knex-decentdb'),
  connection: { filename: '/tmp/sample.ddb' }
});

await db.schema.createTable('users', (table) => {
  table.integer('id').primary();
  table.text('name').notNullable();
  table.text('email');
});

await db('users').insert({ name: 'Alice', email: 'alice@example.com' });
const users = await db('users').select('*');
```

The Knex dialect automatically rewrites `?` placeholders to `$N`.

## Parameter Style

DecentDB uses Postgres-style positional parameters (`$1`, `$2`, ...). The `decentdb` package rejects unquoted `?` with a clear error. Use `knex-decentdb` for automatic conversion.
