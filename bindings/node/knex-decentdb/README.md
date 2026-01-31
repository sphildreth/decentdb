# knex-decentdb

Knex client/dialect for DecentDB.

This package is a scaffold.

## Usage (goal state)

```js
const knex = require('knex');
const { Client_DecentDB } = require('knex-decentdb');

const db = knex({
  client: Client_DecentDB,
  connection: { filename: '/path/to.db' }
});
```

## Parameter style

DecentDB’s engine requires Postgres-style positional parameters (`$1, $2, ...`).
This client rewrites Knex’s `?` placeholders to `$N`.
