/**
 * Example: Basic DecentDB usage with Node.js.
 *
 * Build the native library first:
 *   nim c -d:release --mm:arc --threads:on --app:lib --out:libdecentdb.so src/c_api.nim
 *
 * Build the Node.js addon:
 *   cd bindings/node/decentdb && npm run build
 *
 * Then run:
 *   DECENTDB_NATIVE_LIB_PATH=/path/to/libdecentdb.so node example.js
 */
'use strict';

const fs = require('node:fs');
const { Database } = require('../../bindings/node/decentdb');

const DB_PATH = 'example.ddb';

// Clean up any previous run.
try { fs.unlinkSync(DB_PATH); } catch {}
try { fs.unlinkSync(DB_PATH + '-wal'); } catch {}

const db = new Database({ path: DB_PATH });

// Create a table.
db.exec(`CREATE TABLE users (
  id    INTEGER PRIMARY KEY,
  name  TEXT NOT NULL,
  email TEXT UNIQUE
)`);

// Insert rows. DecentDB uses Postgres-style $1, $2, ... parameters.
const users = [
  ['Alice', 'alice@example.com'],
  ['Bob', 'bob@example.com'],
  ['Carol', 'carol@example.com'],
];

for (const [name, email] of users) {
  db.exec('INSERT INTO users (name, email) VALUES ($1, $2)', [name, email]);
}

// Query all users (synchronous).
const { rows } = db.exec('SELECT id, name, email FROM users ORDER BY id');
console.log('All users:');
for (const [id, name, email] of rows) {
  console.log(`  id=${id}  name=${name}  email=${email}`);
}

// Async query with streaming iteration.
async function main() {
  const stmt = db.prepare('SELECT name FROM users WHERE email = $1');
  stmt.bindAll(['bob@example.com']);
  for await (const row of stmt.rows()) {
    console.log(`\nLookup by email: ${row[0]}`);
  }
  stmt.finalize();

  // Transaction example.
  db.exec('BEGIN');
  db.exec('INSERT INTO users (name, email) VALUES ($1, $2)', ['Dave', 'dave@example.com']);
  db.exec('COMMIT');

  const { rows: countRows } = db.exec('SELECT count(*) FROM users');
  console.log(`\nTotal users after transaction: ${countRows[0][0]}`);

  // Schema introspection.
  console.log(`\nTables: ${JSON.stringify(db.listTables())}`);

  const columns = db.getTableColumns('users');
  console.log('Columns:');
  for (const col of columns) {
    const flags = [
      col.primary_key ? 'PK' : '',
      col.not_null ? 'NOT NULL' : '',
      col.unique ? 'UNIQUE' : '',
    ].filter(Boolean).join('  ');
    console.log(`  ${col.name} (${col.type})${flags ? '  ' + flags : ''}`);
  }

  db.close();

  // Clean up.
  try { fs.unlinkSync(DB_PATH); } catch {}
  try { fs.unlinkSync(DB_PATH + '-wal'); } catch {}

  console.log('\nDone.');
}

main();
