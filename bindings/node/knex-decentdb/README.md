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

## Benchmark

```bash
npm run benchmark:fetch -- --count 100000 --point-reads 5000 --fetchmany-batch 1024 --insert-batch 1024 --db-prefix node_knex_bench_fetch
```

## Current limitations

- the current fast path is still specialized for benchmark-shaped `(int64, text, float64)` inserts and fetches
- Knex schema-builder customization for DecentDB-specific SQL types is still minimal
