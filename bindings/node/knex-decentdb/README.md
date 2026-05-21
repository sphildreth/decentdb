# knex-decentdb

Knex client/dialect for DecentDB.

## What it does

- opens DecentDB connections through `decentdb-native`
- rewrites Knex `?` placeholders to DecentDB `$N` placeholders
- skips placeholders inside string literals, `--` comments, and `/* ... */` block comments
- supports transactions through the native DB transaction helpers

## Usage

```js
const knex = require('knex');
const { Client_DecentDB } = require('knex-decentdb');

const db = knex({
  client: Client_DecentDB,
  connection: { filename: 'app.ddb' },
  useNullAsDefault: true,
});
```

## Queued writes

Queue options are forwarded to `decentdb-native`:

```js
const db = knex({
  client: Client_DecentDB,
  connection: {
    filename: 'app.ddb',
    writeQueueEnabled: true,
    writeQueueCapacity: 128,
    writeQueueDefaultTimeoutMs: 1000,
  },
  useNullAsDefault: true,
});
```

Unbound write SQL uses the native queued execution helper when queue mode is
enabled. Parameterized Knex queries still use prepared statements directly until
the DecentDB C ABI exposes queued prepared-statement execution.

## Lifecycle

Call `await db.destroy()` when the Knex instance is no longer needed, including
after failed transactions. This closes the native DecentDB connection held by
the Knex pool and releases associated database/WAL file handles.

## Benchmark

```bash
npm run benchmark:fetch -- --count 100000 --point-reads 5000 --fetchmany-batch 1024 --insert-batch 1024 --db-prefix node_knex_bench_fetch
```

## Current limitations

- the current fast path is still specialized for benchmark-shaped `(int64, text, float64)` inserts and fetches
- queued execution currently applies only to unbound write SQL
- Knex schema-builder customization for DecentDB-specific SQL types is still minimal
