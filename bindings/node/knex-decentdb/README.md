# knex-decentdb

Knex client/dialect for DecentDB.

Provides automatic binding conversion (`?` -> `$N`) and transaction support.

## Usage

```js
const knex = require('knex');
const { Client_DecentDB } = require('knex-decentdb');

const db = knex({
  client: Client_DecentDB,
  connection: { filename: '/path/to.ddb' },
  useNullAsDefault: true
});

await db.schema.createTable('users', t => {
  t.increments('id');
  t.string('name');
});

await db('users').insert({ name: 'Alice' });
```

## Parameter style

DecentDB’s engine requires Postgres-style positional parameters (`$1, $2, ...`).
This client automatically rewrites Knex’s `?` placeholders to `$N`, respecting string literals and comments.

## Benchmark

Run the Knex-level fair benchmark from this package:

```sh
npm run benchmark:fetch -- --count 100000 --point-reads 5000 --fetchmany-batch 1024 --insert-batch 1024 --db-prefix node_knex_bench_fetch
```

Supported options:

- `--engine <all|decentdb|sqlite>`
- `--count <n>`
- `--point-reads <n>`
- `--fetchmany-batch <n>`
- `--insert-batch <n>`
- `--point-seed <n>`
- `--db-prefix <prefix>` (DecentDB writes `.ddb`, SQLite writes `.db`)
- `--keep-db`
