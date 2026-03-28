'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Database, timestampMicros } = require('..');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function makeDb() {
  return new Database({ path: ':memory:' });
}

// ---------------------------------------------------------------------------
// Version helpers
// ---------------------------------------------------------------------------
test('abiVersion returns a positive integer', () => {
  const v = Database.abiVersion();
  assert.equal(typeof v, 'number');
  assert.ok(v > 0, `expected abiVersion > 0, got ${v}`);
});

test('version returns a non-empty string', () => {
  const v = Database.version();
  assert.equal(typeof v, 'string');
  assert.ok(v.length > 0, `expected non-empty version string, got "${v}"`);
});

// ---------------------------------------------------------------------------
// inTransaction
// ---------------------------------------------------------------------------
test('inTransaction reflects transaction state', () => {
  const db = makeDb();
  assert.equal(db.inTransaction, false);
  db.beginTransaction();
  assert.equal(db.inTransaction, true);
  db.rollbackTransaction();
  assert.equal(db.inTransaction, false);
  db.close();
});

test('create/openExisting helpers honor explicit open modes', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'decentdb-node-modes-'));
  const dbPath = path.join(dir, 'modes.ddb');
  try {
    const created = Database.create(dbPath);
    created.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    created.close();

    const reopened = Database.openExisting(dbPath);
    assert.deepEqual(reopened.listTables(), ['t']);
    reopened.close();
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// Timestamp binding
// ---------------------------------------------------------------------------
test('bind timestamp as BigInt microseconds and read back as ms number', () => {
  const db = makeDb();
  db.exec('CREATE TABLE ts_test (id INTEGER PRIMARY KEY, ts TIMESTAMP)');
  // 2024-01-01T00:00:00.000Z = 1704067200000 ms = 1704067200000000 µs
  const epochMs = 1704067200000;
  const epochMicros = BigInt(epochMs) * 1000n;
  const stmt = db.prepare('INSERT INTO ts_test (id, ts) VALUES ($1, $2)');
  stmt.reset();
  stmt.bindAll([1]);
  stmt.bindTimestamp(2, epochMicros);
  stmt.step();
  stmt.finalize();

  const rows = db.exec('SELECT ts FROM ts_test WHERE id = $1', [1]).rows;
  assert.equal(rows.length, 1);
  // Engine returns timestamp as ms float
  assert.equal(Number(rows[0][0]), epochMs);
  db.close();
});

test('bind timestamp as Number (ms) via bindAll timestampMicros helper', () => {
  const db = makeDb();
  db.exec('CREATE TABLE ts2 (id INTEGER PRIMARY KEY, ts TIMESTAMP)');
  const epochMs = 1704067200000;
  db.exec('INSERT INTO ts2 (id, ts) VALUES ($1, $2)', [2, timestampMicros(epochMs)]);
  const rows = db.exec('SELECT ts FROM ts2 WHERE id = $1', [2]).rows;
  assert.equal(rows.length, 1);
  assert.equal(Number(rows[0][0]), epochMs);
  db.close();
});

test('bind timestamp directly from Date', () => {
  const db = makeDb();
  db.exec('CREATE TABLE ts3 (id INTEGER PRIMARY KEY, ts TIMESTAMP)');
  const when = new Date('2024-02-03T04:05:06.000Z');
  db.exec('INSERT INTO ts3 (id, ts) VALUES ($1, $2)', [3, when]);
  const rows = db.exec('SELECT ts FROM ts3 WHERE id = $1', [3]).rows;
  assert.equal(rows.length, 1);
  assert.equal(Number(rows[0][0]), when.getTime());
  db.close();
});

// ---------------------------------------------------------------------------
// stepRowView (fused step + row)
// ---------------------------------------------------------------------------
test('stepRowView returns row arrays and null when done', () => {
  const db = makeDb();
  db.exec('CREATE TABLE sv (id INTEGER PRIMARY KEY, val TEXT)');
  db.exec('INSERT INTO sv VALUES ($1, $2)', [1, 'hello']);
  db.exec('INSERT INTO sv VALUES ($1, $2)', [2, 'world']);

  const stmt = db.prepare('SELECT id, val FROM sv ORDER BY id');
  const row1 = stmt.stepRowView();
  assert.ok(Array.isArray(row1), 'first call should return array');
  assert.equal(row1.length, 2);
  assert.equal(row1[1], 'hello');

  const row2 = stmt.stepRowView();
  assert.ok(Array.isArray(row2));
  assert.equal(row2[1], 'world');

  const done = stmt.stepRowView();
  assert.equal(done, null, 'no more rows should return null');

  stmt.finalize();
  db.close();
});

// ---------------------------------------------------------------------------
// reBindInt64Execute (fast re-execute helper)
// ---------------------------------------------------------------------------
test('reBindInt64Execute updates a row and returns affected count', () => {
  const db = makeDb();
  db.exec('CREATE TABLE rb (id INTEGER PRIMARY KEY, val TEXT)');
  db.exec('INSERT INTO rb VALUES ($1, $2)', [1, 'original']);

  const stmt = db.prepare('DELETE FROM rb WHERE id = $1');
  const affected = stmt.reBindInt64Execute(1n);
  assert.equal(affected, 1n);

  const rows = db.exec('SELECT COUNT(*) FROM rb').rows;
  assert.equal(Number(rows[0][0]), 0);
  stmt.finalize();
  db.close();
});

test('reBindInt64Execute accepts safe number (coerced to BigInt)', () => {
  const db = makeDb();
  db.exec('CREATE TABLE rb2 (id INTEGER PRIMARY KEY, val TEXT)');
  db.exec('INSERT INTO rb2 VALUES ($1, $2)', [42, 'row']);

  const stmt = db.prepare('DELETE FROM rb2 WHERE id = $1');
  const affected = stmt.reBindInt64Execute(42);
  assert.equal(affected, 1n);
  stmt.finalize();
  db.close();
});

// ---------------------------------------------------------------------------
// reBindTextInt64Execute / reBindInt64TextExecute
// ---------------------------------------------------------------------------
test('reBindTextInt64Execute updates matching row', () => {
  const db = makeDb();
  db.exec('CREATE TABLE rbt (id INTEGER PRIMARY KEY, name TEXT)');
  db.exec('INSERT INTO rbt VALUES ($1, $2)', [1, 'alice']);

  const stmt = db.prepare('UPDATE rbt SET name = $1 WHERE id = $2');
  const affected = stmt.reBindTextInt64Execute('bob', 1n);
  assert.equal(affected, 1n);

  const rows = db.exec('SELECT name FROM rbt WHERE id = $1', [1]).rows;
  assert.equal(rows[0][0], 'bob');
  stmt.finalize();
  db.close();
});

test('reBindInt64TextExecute updates matching row', () => {
  const db = makeDb();
  db.exec('CREATE TABLE rib (id INTEGER PRIMARY KEY, name TEXT)');
  db.exec('INSERT INTO rib VALUES ($1, $2)', [7, 'carol']);

  const stmt = db.prepare('UPDATE rib SET id = $1 WHERE name = $2');
  const affected = stmt.reBindInt64TextExecute(77n, 'carol');
  assert.equal(affected, 1n);

  const rows = db.exec('SELECT id FROM rib WHERE name = $1', ['carol']).rows;
  assert.equal(Number(rows[0][0]), 77);
  stmt.finalize();
  db.close();
});

test('table/view/trigger DDL helpers expose schema details', () => {
  const db = makeDb();
  db.exec('CREATE TABLE parent (id INTEGER PRIMARY KEY)');
  db.exec('CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), name TEXT)');
  db.exec('CREATE VIEW child_names AS SELECT id, name FROM child');
  db.exec('CREATE TABLE audit_log (msg TEXT)');
  db.exec(
    "CREATE TRIGGER child_ai AFTER INSERT ON child FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log VALUES (''changed'')')",
  );

  assert.match(db.getTableDdl('child'), /CREATE TABLE "child"/);
  assert.deepEqual(db.listViews(), ['child_names']);
  assert.match(db.getViewDdl('child_names'), /CREATE VIEW "child_names"/);
  assert.equal(db.listTriggers()[0].name, 'child_ai');
  db.close();
});

// ---------------------------------------------------------------------------
// FinalizationRegistry (smoke test: verify no crash on GC without close)
// ---------------------------------------------------------------------------
test('Database without explicit close does not throw on GC (smoke)', async () => {
  // Create and abandon a database. The FinalizationRegistry will clean up on GC.
  // We can't reliably force GC in all environments, but we can at least verify
  // the registry callback doesn't crash when invoked.
  (() => {
    const db = new Database({ path: ':memory:' });
    db.exec('CREATE TABLE t (x INT)');
    // Let db go out of scope without close().
  })();

  if (global.gc) {
    global.gc();
    // Give the event loop a tick to process finalizers.
    await new Promise((r) => setImmediate(r));
  }
  // If we reach here without an uncaught exception, the test passes.
});

// ---------------------------------------------------------------------------
// positionBindings block comment safety (via index.js _hasUnquotedQuestionMark)
// ---------------------------------------------------------------------------
test('prepare does not false-positive on ? inside block comment', () => {
  // The _hasUnquotedQuestionMark check should skip block comment content.
  // This test is a smoke check via the knex positionBindings module directly.
  const { positionBindings } = require('../../knex-decentdb/src/positionBindings');

  const sql = "SELECT /* what is ? */ id FROM t WHERE id = ?";
  const result = positionBindings(sql);
  // The ? inside /* */ should be copied verbatim; the trailing ? should become $1.
  assert.ok(result.includes('/* what is ? */'), 'block comment content preserved');
  assert.ok(result.endsWith('= $1'), 'placeholder after comment rewritten');
});

test('positionBindings handles nested ? in string literals', () => {
  const { positionBindings } = require('../../knex-decentdb/src/positionBindings');
  const sql = "SELECT '?' as q, id FROM t WHERE id = ?";
  const result = positionBindings(sql);
  assert.ok(result.includes("'?'"), 'string literal ? preserved');
  assert.ok(result.includes('= $1'), 'placeholder rewritten');
  assert.equal((result.match(/\$\d+/g) || []).length, 1);
});
