# knex-decentdb

Knex client/dialect for DecentDB.

Provides automatic binding conversion (`?` -> `$N`) and transaction support.

## Usage

```js
const knex = require('knex');
const { Client_DecentDB } = require('knex-decentdb');

const db = knex({
  client: Client_DecentDB,
  connection: { filename: '/path/to.db' },
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
