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

  // ── Window Functions ──
  db.exec(`CREATE TABLE scores (
    id    INTEGER PRIMARY KEY,
    name  TEXT NOT NULL,
    dept  TEXT NOT NULL,
    score INTEGER NOT NULL
  )`);

  const scores = [
    ['Alice', 'eng', 95], ['Bob', 'eng', 95],
    ['Carol', 'eng', 80], ['Dave', 'sales', 90],
    ['Eve', 'sales', 85],
  ];
  for (const [name, dept, score] of scores) {
    db.exec('INSERT INTO scores (name, dept, score) VALUES ($1, $2, $3)', [name, dept, score]);
  }

  console.log('\n── Window Functions ──');

  // ROW_NUMBER
  const rn = db.exec(`
    SELECT name, dept, score,
           ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS rn
    FROM scores ORDER BY dept, score DESC`);
  console.log('\nROW_NUMBER (ranking within department):');
  for (const [name, dept, score, rnVal] of rn.rows) {
    console.log(`  ${name.padEnd(6)}  dept=${dept.padEnd(5)}  score=${score}  rn=${rnVal}`);
  }

  // RANK
  const rank = db.exec(`
    SELECT name, score,
           RANK() OVER (ORDER BY score DESC) AS rank
    FROM scores ORDER BY score DESC, name`);
  console.log('\nRANK (with gaps for ties):');
  for (const [name, score, r] of rank.rows) {
    console.log(`  ${name.padEnd(6)}  score=${score}  rank=${r}`);
  }

  // DENSE_RANK
  const dr = db.exec(`
    SELECT name, score,
           DENSE_RANK() OVER (ORDER BY score DESC) AS dr
    FROM scores ORDER BY score DESC, name`);
  console.log('\nDENSE_RANK (no gaps):');
  for (const [name, score, d] of dr.rows) {
    console.log(`  ${name.padEnd(6)}  score=${score}  dense_rank=${d}`);
  }

  // LAG
  const lag = db.exec(`
    SELECT name, score,
           LAG(score, 1, 0) OVER (ORDER BY score DESC) AS prev_score
    FROM scores ORDER BY score DESC`);
  console.log('\nLAG (previous score):');
  for (const [name, score, prev] of lag.rows) {
    console.log(`  ${name.padEnd(6)}  score=${score}  prev_score=${prev}`);
  }

  // LEAD
  const lead = db.exec(`
    SELECT name, score,
           LEAD(score) OVER (PARTITION BY dept ORDER BY score DESC) AS next_score
    FROM scores ORDER BY dept, score DESC`);
  console.log('\nLEAD (next score in dept):');
  for (const [name, score, next] of lead.rows) {
    console.log(`  ${name.padEnd(6)}  score=${score}  next_score=${next ?? 'NULL'}`);
  }

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
