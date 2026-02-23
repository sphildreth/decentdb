'use strict';

const { describe, it, after } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { Database } = require('..');

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'decentdb-test-'));
  const dbPath = path.join(dir, 'test.ddb');
  const db = new Database({ path: dbPath });
  return { db, dir };
}

describe('Schema introspection', () => {
  it('listTables returns empty array for new db', () => {
    const { db, dir } = tmpDb();
    try {
      const tables = db.listTables();
      assert.deepStrictEqual(tables, []);
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('listTables returns created tables', () => {
    const { db, dir } = tmpDb();
    try {
      db.exec('CREATE TABLE alpha (id INTEGER PRIMARY KEY)');
      db.exec('CREATE TABLE beta (id INTEGER PRIMARY KEY, name TEXT)');
      const tables = db.listTables();
      assert.strictEqual(tables.length, 2);
      assert.ok(tables.includes('alpha'));
      assert.ok(tables.includes('beta'));
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('getTableColumns returns column metadata', () => {
    const { db, dir } = tmpDb();
    try {
      db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)');
      const cols = db.getTableColumns('users');
      assert.strictEqual(cols.length, 3);
      assert.strictEqual(cols[0].name, 'id');
      assert.strictEqual(cols[0].primary_key, true);
      assert.strictEqual(cols[1].name, 'name');
      assert.strictEqual(cols[1].not_null, true);
      assert.strictEqual(cols[2].name, 'email');
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('listIndexes returns index metadata', () => {
    const { db, dir } = tmpDb();
    try {
      db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL)');
      db.exec('CREATE INDEX idx_items_name ON items (name)');
      const indexes = db.listIndexes();
      assert.ok(indexes.length >= 1);
      const found = indexes.find(idx => idx.name === 'idx_items_name');
      assert.ok(found, 'expected to find idx_items_name');
      assert.strictEqual(found.table, 'items');
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe('Checkpoint', () => {
  it('checkpoint does not throw', () => {
    const { db, dir } = tmpDb();
    try {
      db.exec('CREATE TABLE chk (id INTEGER PRIMARY KEY, v TEXT)');
      db.exec("INSERT INTO chk (v) VALUES ($1)", ['hello']);
      assert.doesNotThrow(() => db.checkpoint());
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe('Auto-increment', () => {
  it('INSERT without id auto-assigns incrementing ids', () => {
    const { db, dir } = tmpDb();
    try {
      db.exec('CREATE TABLE auto (id INTEGER PRIMARY KEY, val TEXT)');
      db.exec("INSERT INTO auto (val) VALUES ($1)", ['a']);
      db.exec("INSERT INTO auto (val) VALUES ($1)", ['b']);
      const { rows } = db.exec('SELECT id, val FROM auto ORDER BY id');
      assert.strictEqual(rows.length, 2);
      const [id1] = rows[0];
      const [id2] = rows[1];
      assert.ok(id1 < id2, `auto-increment IDs should increase: ${id1}, ${id2}`);
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('explicit id is preserved', () => {
    const { db, dir } = tmpDb();
    try {
      db.exec('CREATE TABLE auto2 (id INTEGER PRIMARY KEY, val TEXT)');
      db.exec("INSERT INTO auto2 (id, val) VALUES ($1, $2)", [100, 'x']);
      db.exec("INSERT INTO auto2 (val) VALUES ($1)", ['y']);
      const { rows } = db.exec('SELECT id, val FROM auto2 ORDER BY id');
      assert.strictEqual(rows.length, 2);
      // First row should have explicit id 100
      assert.strictEqual(Number(rows[0][0]), 100);
      // Second row should have auto-assigned id > 0
      assert.ok(Number(rows[1][0]) > 0);
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
