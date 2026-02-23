'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Database } = require('..');

function tmpPath(suffix) {
  return path.join(os.tmpdir(), `decentdb_saveas_${suffix}_${Date.now()}_${Math.random().toString(36).slice(2)}.db`);
}

function cleanupFile(p) {
  try { fs.unlinkSync(p); } catch {}
  try { fs.unlinkSync(p + '-wal'); } catch {}
}

test('SaveAs exports memory to disk', async (t) => {
  const dest = tmpPath('mem2disk');
  try {
    const db = new Database({ path: ':memory:' });
    db.exec('CREATE TABLE items (id BIGINT, name TEXT)');
    db.exec('INSERT INTO items VALUES ($1, $2)', [1n, 'alpha']);
    db.exec('INSERT INTO items VALUES ($1, $2)', [2n, 'beta']);
    db.saveAs(dest);
    db.close();

    const db2 = new Database({ path: dest });
    const res = await db2.execAsync('SELECT id, name FROM items ORDER BY id');
    assert.equal(res.rows.length, 2);
    assert.equal(res.rows[0][0], 1n);
    assert.equal(res.rows[0][1], 'alpha');
    assert.equal(res.rows[1][0], 2n);
    assert.equal(res.rows[1][1], 'beta');
    db2.close();
  } finally {
    cleanupFile(dest);
  }
});

test('SaveAs preserves schema and indexes', async (t) => {
  const dest = tmpPath('schema_idx');
  try {
    const db = new Database({ path: ':memory:' });
    db.exec('CREATE TABLE docs (id BIGINT, title TEXT, body TEXT)');
    db.exec('CREATE INDEX idx_docs_title ON docs (title)');
    db.exec('INSERT INTO docs VALUES ($1, $2, $3)', [1n, 'readme', 'hello world']);
    db.saveAs(dest);
    db.close();

    const db2 = new Database({ path: dest });

    // Verify data survived
    const res = await db2.execAsync('SELECT id, title, body FROM docs');
    assert.equal(res.rows.length, 1);
    assert.equal(res.rows[0][1], 'readme');

    // Verify index exists by checking listIndexes
    const indexes = db2.listIndexes();
    const idxNames = indexes.map(idx => idx.name);
    assert.ok(idxNames.includes('idx_docs_title'), `Expected idx_docs_title in ${JSON.stringify(idxNames)}`);

    db2.close();
  } finally {
    cleanupFile(dest);
  }
});

test('SaveAs errors if dest exists', (t) => {
  const dest = tmpPath('exists');
  try {
    // Create the destination file first
    fs.writeFileSync(dest, 'placeholder');

    const db = new Database({ path: ':memory:' });
    assert.throws(() => {
      db.saveAs(dest);
    });
    db.close();
  } finally {
    cleanupFile(dest);
  }
});

test('SaveAs empty database', async (t) => {
  const dest = tmpPath('empty');
  try {
    const db = new Database({ path: ':memory:' });
    db.saveAs(dest);
    db.close();

    const db2 = new Database({ path: dest });
    const tables = db2.listTables();
    assert.equal(tables.length, 0);
    db2.close();
  } finally {
    cleanupFile(dest);
  }
});
