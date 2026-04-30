'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Database } = require('..');

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'decentdb-node-datatypes-'));
  return {
    dbPath: path.join(dir, 'test.ddb'),
    cleanup() {
      fs.rmSync(dir, { recursive: true, force: true });
    },
  };
}

test('Boolean support', async (t) => {
  const { dbPath, cleanup } = tmpDb();
  const db = new Database({ path: dbPath });
  db.exec('CREATE TABLE t_bool (b BOOL)');
  
  db.exec('INSERT INTO t_bool VALUES ($1)', [true]);
  db.exec('INSERT INTO t_bool VALUES ($1)', [false]);
  
  const res = await db.execAsync('SELECT b FROM t_bool');
  assert.equal(res.rows.length, 2);
  assert.strictEqual(res.rows[0][0], true);
  assert.strictEqual(res.rows[1][0], false);
  
  db.close();
  cleanup();
});

test('UUID support (as Blob)', async (t) => {
  const { dbPath, cleanup } = tmpDb();
  const db = new Database({ path: dbPath });
  db.exec('CREATE TABLE t_uuid (u UUID)');
  
  const uuid1 = Buffer.alloc(16);
  uuid1[0] = 0xAA;
  uuid1[15] = 0xBB;
  
  db.exec('INSERT INTO t_uuid VALUES ($1)', [uuid1]);
  
  const res = await db.execAsync('SELECT u FROM t_uuid');
  assert.equal(res.rows.length, 1);
  const val = res.rows[0][0];
  
  assert.ok(Buffer.isBuffer(val));
  assert.equal(val.length, 16);
  assert.equal(val[0], 0xAA);
  assert.equal(val[15], 0xBB);
  
  db.close();
  cleanup();
});

test('Float64 support', async (t) => {
  const { dbPath, cleanup } = tmpDb();
  const db = new Database({ path: dbPath });
  db.exec('CREATE TABLE t_float (id BIGINT, v FLOAT)');

  const vals = [0.0, 1.0, -1.0, 3.141592653589793, 1.7976931348623157e+308, 5e-324];
  for (let i = 0; i < vals.length; i++) {
    db.exec('INSERT INTO t_float VALUES ($1, $2)', [BigInt(i), vals[i]]);
  }

  const res = await db.execAsync('SELECT v FROM t_float ORDER BY id');
  assert.equal(res.rows.length, vals.length);
  for (let i = 0; i < vals.length; i++) {
    assert.equal(res.rows[i][0], vals[i]);
  }

  db.close();
  cleanup();
});

test('NULL support', async (t) => {
  const { dbPath, cleanup } = tmpDb();
  const db = new Database({ path: dbPath });
  db.exec('CREATE TABLE t_null (id BIGINT, i INT, t TEXT, b BOOL)');

  db.exec('INSERT INTO t_null VALUES ($1, $2, $3, $4)', [1n, null, null, null]);

  const res = await db.execAsync('SELECT i, t, b FROM t_null WHERE id = 1');
  assert.equal(res.rows.length, 1);
  assert.strictEqual(res.rows[0][0], null);
  assert.strictEqual(res.rows[0][1], null);
  assert.strictEqual(res.rows[0][2], null);

  db.close();
  cleanup();
});
