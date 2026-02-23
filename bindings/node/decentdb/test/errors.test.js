'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { Database } = require('..');

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'decentdb-err-'));
  const dbPath = path.join(dir, 'test.ddb');
  const db = new Database({ path: dbPath });
  return { db, dir };
}

describe('Error handling', () => {
  it('throws on invalid SQL', () => {
    const { db, dir } = tmpDb();
    try {
      assert.throws(() => db.exec('NOT VALID SQL'), /error|Error/i);
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('throws on prepare of invalid SQL', () => {
    const { db, dir } = tmpDb();
    try {
      assert.throws(() => db.prepare('SELECT FROM'), /error|Error/i);
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('throws on table that does not exist', () => {
    const { db, dir } = tmpDb();
    try {
      assert.throws(() => db.exec('SELECT * FROM nonexistent'), /error|Error/i);
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('throws on duplicate table creation', () => {
    const { db, dir } = tmpDb();
    try {
      db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)');
      assert.throws(() => db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)'), /error|Error/i);
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('throws on unique constraint violation', () => {
    const { db, dir } = tmpDb();
    try {
      db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT UNIQUE)');
      db.exec("INSERT INTO t (name) VALUES ($1)", ['Alice']);
      assert.throws(
        () => db.exec("INSERT INTO t (name) VALUES ($1)", ['Alice']),
        /error|Error/i
      );
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('throws on NOT NULL constraint violation', () => {
    const { db, dir } = tmpDb();
    try {
      db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)');
      assert.throws(
        () => db.exec("INSERT INTO t (name) VALUES ($1)", [null]),
        /error|Error/i
      );
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('throws on foreign key violation', () => {
    const { db, dir } = tmpDb();
    try {
      db.exec('CREATE TABLE parent (id INTEGER PRIMARY KEY)');
      db.exec('CREATE TABLE child (id INTEGER PRIMARY KEY, pid BIGINT REFERENCES parent(id))');
      assert.throws(
        () => db.exec("INSERT INTO child (pid) VALUES ($1)", [999n]),
        /error|Error/i
      );
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe('Statement lifecycle', () => {
  it('statement reset allows re-execution', () => {
    const { db, dir } = tmpDb();
    try {
      db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)');
      db.exec("INSERT INTO t (v) VALUES ($1)", ['a']);
      db.exec("INSERT INTO t (v) VALUES ($1)", ['b']);

      const stmt = db.prepare('SELECT v FROM t ORDER BY id');
      // Step once to consume first row
      assert.equal(stmt.step(), true);
      const row1 = stmt.rowArray();
      assert.equal(row1[0], 'a');

      stmt.reset();

      // After reset we can iterate all rows again
      const rows2 = [];
      while (stmt.step()) {
        rows2.push(stmt.rowArray());
      }
      assert.equal(rows2.length, 2);
      assert.equal(rows2[0][0], 'a');
      assert.equal(rows2[1][0], 'b');

      stmt.finalize();
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('rows_affected returns count for DML', () => {
    const { db, dir } = tmpDb();
    try {
      db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)');
      db.exec("INSERT INTO t (v) VALUES ($1)", ['a']);
      db.exec("INSERT INTO t (v) VALUES ($1)", ['b']);
      db.exec("INSERT INTO t (v) VALUES ($1)", ['c']);

      const { rowsAffected } = db.exec("DELETE FROM t WHERE v = $1", ['b']);
      assert.equal(rowsAffected, 1n);
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
