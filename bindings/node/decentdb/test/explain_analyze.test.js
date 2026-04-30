'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Database } = require('..');

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'decentdb-explain-'));
  return {
    dbPath: path.join(dir, 'test.ddb'),
    cleanup() {
      fs.rmSync(dir, { recursive: true, force: true });
    },
  };
}

test('EXPLAIN ANALYZE returns plan with actual metrics', async (t) => {
  const { dbPath, cleanup } = tmpDb();
  const db = new Database({ path: dbPath });
  db.exec('CREATE TABLE t (id BIGINT, name TEXT)');
  db.exec('INSERT INTO t VALUES ($1, $2)', [1n, 'Alice']);
  db.exec('INSERT INTO t VALUES ($1, $2)', [2n, 'Bob']);
  db.exec('INSERT INTO t VALUES ($1, $2)', [3n, 'Charlie']);

  const res = await db.execAsync('EXPLAIN ANALYZE SELECT * FROM t');
  assert.ok(res.rows.length > 0, 'expected plan output');

  const planText = res.rows.map(r => r[0]).join('\n');
  assert.ok(planText.includes('Project'), 'expected Project in plan');
  assert.ok(planText.includes('Actual Rows: 3'), 'expected Actual Rows: 3');
  assert.ok(planText.includes('Actual Time:'), 'expected Actual Time');
  assert.ok(planText.includes('ms'), 'expected ms unit');

  db.close();
  cleanup();
});

test('EXPLAIN without ANALYZE has no actual metrics', async (t) => {
  const { dbPath, cleanup } = tmpDb();
  const db = new Database({ path: dbPath });
  db.exec('CREATE TABLE t (id BIGINT)');

  const res = await db.execAsync('EXPLAIN SELECT * FROM t');
  const planText = res.rows.map(r => r[0]).join('\n');
  assert.ok(planText.includes('Project'), 'expected Project in plan');
  assert.ok(!planText.includes('Actual Rows:'), 'should not have Actual Rows');
  assert.ok(!planText.includes('Actual Time:'), 'should not have Actual Time');

  db.close();
  cleanup();
});

test('EXPLAIN ANALYZE with empty table shows zero rows', async (t) => {
  const { dbPath, cleanup } = tmpDb();
  const db = new Database({ path: dbPath });
  db.exec('CREATE TABLE t (id BIGINT)');

  const res = await db.execAsync('EXPLAIN ANALYZE SELECT * FROM t');
  const planText = res.rows.map(r => r[0]).join('\n');
  assert.ok(planText.includes('Actual Rows: 0'), 'expected Actual Rows: 0');

  db.close();
  cleanup();
});

test('EXPLAIN ANALYZE with filter shows filtered row count', async (t) => {
  const { dbPath, cleanup } = tmpDb();
  const db = new Database({ path: dbPath });
  db.exec('CREATE TABLE t (id BIGINT)');
  for (let i = 1; i <= 10; i++) {
    db.exec('INSERT INTO t VALUES ($1)', [BigInt(i)]);
  }

  const res = await db.execAsync('EXPLAIN ANALYZE SELECT * FROM t WHERE id > 5');
  const planText = res.rows.map(r => r[0]).join('\n');
  assert.ok(planText.includes('Actual Rows: 5'), 'expected Actual Rows: 5');

  db.close();
  cleanup();
});
