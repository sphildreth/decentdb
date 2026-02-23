'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { Database } = require('..');

const DB_PATH = 'test_decimal.db';

function cleanup() {
  try { fs.unlinkSync(DB_PATH); } catch {}
  try { fs.unlinkSync(DB_PATH + '-wal'); } catch {}
}

test('Decimal type support', async (t) => {
  cleanup();
  
  const db = new Database({ path: DB_PATH });
  
  // Create table
  db.exec('CREATE TABLE decimals (d DECIMAL(18, 9))');
  
  const vals = [
    { unscaled: 0n, scale: 9 }, // 0.000000000
    { unscaled: 1000000000n, scale: 9 }, // 1.000000000
    { unscaled: -1000000000n, scale: 9 }, // -1.000000000
    { unscaled: 123456789012n, scale: 9 }, // 123.456789012
  ];

  for (const v of vals) {
    db.exec('INSERT INTO decimals VALUES ($1)', [v]);
  }
  
  const res = await db.execAsync('SELECT d FROM decimals');
  assert.equal(res.rows.length, vals.length);
  
  for (let i = 0; i < vals.length; i++) {
    const row = res.rows[i];
    const val = vals[i];
    
    assert.equal(typeof row[0], 'object');
    assert.equal(row[0].unscaled, val.unscaled);
    assert.equal(row[0].scale, val.scale);
  }
  
  db.close();
  cleanup();
});

test('Decimal scale coercion', async (t) => {
  cleanup();
  const db = new Database({ path: DB_PATH });
  db.exec('CREATE TABLE t (d DECIMAL(18, 2))');
  
  // Insert 1 (scale 0)
  db.exec('INSERT INTO t VALUES ($1)', [{ unscaled: 1n, scale: 0 }]);
  
  const res = await db.execAsync('SELECT d FROM t');
  // Expect 1.00 (scale 2, unscaled 100)
  assert.equal(res.rows[0][0].scale, 2);
  assert.equal(res.rows[0][0].unscaled, 100n);
  
  db.close();
  cleanup();
});
