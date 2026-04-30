'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Database } = require('..');

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'decentdb-node-decimal-'));
  return {
    dbPath: path.join(dir, 'test.ddb'),
    cleanup() {
      fs.rmSync(dir, { recursive: true, force: true });
    },
  };
}

test('Decimal type support', async (t) => {
  const { dbPath, cleanup } = tmpDb();
  const db = new Database({ path: dbPath });
  
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
  const { dbPath, cleanup } = tmpDb();
  const db = new Database({ path: dbPath });
  db.exec('CREATE TABLE t (d DECIMAL(18, 2))');
  
  // Insert 1 (scale 0)
  db.exec('INSERT INTO t VALUES ($1)', [{ unscaled: 1n, scale: 0 }]);
  
  const res = await db.execAsync('SELECT d FROM t');
  // The Rust rewrite preserves bound decimal scale in result values.
  // Older native behavior coerced to declared column scale for DECIMAL(p,s).
  // Accept either representation so bindings stay compatible across engines.
  const d = res.rows[0][0];
  if (d.scale === 2) {
    assert.equal(d.unscaled, 100n);
  } else {
    assert.equal(d.scale, 0);
    assert.equal(d.unscaled, 1n);
  }
  
  db.close();
  cleanup();
});
