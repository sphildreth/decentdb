'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Database } = require('..');

async function trimProcessHeap() {
  if (typeof global.gc !== 'function') {
    return;
  }

  for (let i = 0; i < 3; i++) {
    global.gc();
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

async function runLeakIteration(dbPath) {
  const db = new Database({ path: dbPath });
  try {
    const result = await db.execAsync('SELECT COUNT(*) FROM leak_probe');
    assert.equal(result.rows[0][0], 1n);
  } finally {
    db.close();
  }
}

test('Repeated open/query/close keeps RSS bounded', async (t) => {
  if (process.platform !== 'linux') {
    t.skip('RSS regression is Linux-only');
  }
  if (typeof global.gc !== 'function') {
    t.skip('requires node --expose-gc');
  }

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'decentdb-node-leak-'));
  const dbPath = path.join(tmpDir, 'memory-leak.ddb');

  t.after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  {
    const db = new Database({ path: dbPath });
    try {
      db.exec('CREATE TABLE leak_probe (id BIGINT PRIMARY KEY, payload TEXT)');
      db.exec('INSERT INTO leak_probe (id, payload) VALUES ($1, $2)', [1n, 'probe']);
    } finally {
      db.close();
    }
  }

  for (let i = 0; i < 25; i++) {
    await runLeakIteration(dbPath);
  }

  await trimProcessHeap();
  const before = process.memoryUsage().rss;

  for (let i = 0; i < 160; i++) {
    await runLeakIteration(dbPath);
    if (i % 10 === 0) {
      await trimProcessHeap();
    }
  }

  await trimProcessHeap();
  const after = process.memoryUsage().rss;
  const diff = after - before;

  assert.ok(
    diff < 12 * 1024 * 1024,
    `RSS grew by ${diff} bytes (before=${before}, after=${after})`,
  );
});
