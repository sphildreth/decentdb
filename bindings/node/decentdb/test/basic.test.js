'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { Database } = require('..');

const DB_PATH = 'test_basic.db';

function cleanup() {
  try { fs.unlinkSync(DB_PATH); } catch {}
  try { fs.unlinkSync(DB_PATH + '-wal'); } catch {}
}

test('Database basic operations', async (t) => {
  cleanup();
  
  const db = new Database({ path: DB_PATH });
  
  // Create table
  db.exec('CREATE TABLE foo (id BIGINT, txt TEXT, b BLOB)');
  
  // Insert
  db.exec('INSERT INTO foo VALUES ($1, $2, $3)', [1n, 'hello', Buffer.from([1, 2, 3])]);
  db.exec('INSERT INTO foo VALUES ($1, $2, $3)', [2n, 'world', null]);
  
  // Async exec
  const res = await db.execAsync('SELECT * FROM foo ORDER BY id');
  assert.equal(res.rows.length, 2);
  assert.equal(res.rows[0][0], 1n);
  assert.equal(res.rows[0][1], 'hello');
  assert.deepEqual(res.rows[0][2], Buffer.from([1, 2, 3]));
  
  assert.equal(res.rows[1][0], 2n);
  assert.equal(res.rows[1][1], 'world');
  assert.equal(res.rows[1][2], null);
  
  db.close();
});

test('Parameter validation', (t) => {
  cleanup();
  const db = new Database({ path: DB_PATH });
  
  assert.throws(() => {
    db.prepare('SELECT ?');
  }, /DecentDB uses Postgres-style placeholders/);
  
  // Should allow ? in strings
  const stmt = db.prepare("SELECT 'What?'");
  stmt.finalize();
  
  db.close();
});

test('Streaming and async iteration', async (t) => {
  cleanup();
  const db = new Database({ path: DB_PATH });
  db.exec('CREATE TABLE nums (val BIGINT)');
  
  const count = 100;
  for (let i = 0; i < count; i++) {
    db.exec('INSERT INTO nums VALUES ($1)', [BigInt(i)]);
  }
  
  const stmt = db.prepare('SELECT val FROM nums ORDER BY val');
  let seen = 0;
  for await (const row of stmt.rows()) {
    assert.equal(row[0], BigInt(seen));
    seen++;
  }
  assert.equal(seen, count);
  stmt.finalize();
  db.close();
});

test('Transaction rollback', async (t) => {
  cleanup();
  const db = new Database({ path: DB_PATH });
  db.exec('CREATE TABLE t (id BIGINT)');
  
  db.exec('BEGIN');
  db.exec('INSERT INTO t VALUES (1)');
  db.exec('ROLLBACK');
  
  const res = await db.execAsync('SELECT count(*) FROM t');
  assert.equal(res.rows[0][0], 0n);
  
  db.close();
  cleanup();
});
