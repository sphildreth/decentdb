'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const { Client_DecentDB } = require('..');
const knex = require('knex');

const DB_PATH = 'test_knex.db';

function cleanup() {
  for (const path of [DB_PATH, `${DB_PATH}.wal`, `${DB_PATH}.wal-idx`, `${DB_PATH}.sync-journal`, `${DB_PATH}-wal`]) {
    try { fs.unlinkSync(path); } catch {}
  }
}

test('Knex basic operations', async (t) => {
  cleanup();

  const k = knex({
    client: Client_DecentDB,
    connection: {
      filename: DB_PATH
    },
    useNullAsDefault: true
  });

  try {
    await k.schema.createTable('users', (t) => {
      t.bigInteger('id');
      t.string('name');
    });

    await k('users').insert({ id: 1, name: 'Alice' });
    await k('users').insert({ id: 2, name: 'Bob' });

    const rows = await k('users').select('*').orderBy('id');
    assert.equal(rows.length, 2);
    assert.equal(rows[0].name, 'Alice');
    assert.equal(rows[1].name, 'Bob');
  } catch (err) {
    console.error('TEST FAILED:', err);
    throw err;
  } finally {
    await k.destroy();
  }
});

test('Knex transaction', async (t) => {
  cleanup();

  const k = knex({
    client: Client_DecentDB,
    connection: {
      filename: DB_PATH
    },
    useNullAsDefault: true
  });

  try {
    await k.schema.createTable('bank', (t) => {
      t.bigInteger('balance');
    });

    await k.transaction(async (trx) => {
      await trx('bank').insert({ balance: 100 });
      // rollback implicitly on error, or commit on success
    });

    const rows = await k('bank').select('*');
    assert.equal(rows.length, 1);
    assert.equal(rows[0].balance, 100n);

    try {
      await k.transaction(async (trx) => {
        await trx('bank').insert({ balance: 200 });
        throw new Error('fail');
      });
    } catch (e) {
      // expected
    }

    const rows2 = await k('bank').select('*');
    assert.equal(rows2.length, 1); // Should still be 1
  } finally {
    await k.destroy();
    cleanup();
  }
});

test('Knex forwards write queue options for unbound writes', async (t) => {
  cleanup();

  const k = knex({
    client: Client_DecentDB,
    connection: {
      filename: DB_PATH,
      writeQueueEnabled: true,
      writeQueueCapacity: 16,
      writeQueueDefaultTimeoutMs: 1000
    },
    useNullAsDefault: true
  });

  try {
    await k.raw('CREATE TABLE queued_knex (id INT64 PRIMARY KEY, name TEXT)');
    await k.raw("INSERT INTO queued_knex VALUES (1, 'queued')");
    const rows = await k.raw('SELECT name FROM queued_knex WHERE id = 1');
    assert.equal(rows.rows[0].name, 'queued');
  } finally {
    await k.destroy();
    cleanup();
  }
});

test('Knex transaction closes pool after failure', async (t) => {
  cleanup();

  const k = knex({
    client: Client_DecentDB,
    connection: {
      filename: DB_PATH
    },
    useNullAsDefault: true
  });

  try {
    await k.schema.createTable('bank', (t) => {
      t.bigInteger('balance');
    });

    await assert.rejects(
      k.transaction(async (trx) => {
        await trx('bank').insert({ balance: 200 });
        throw new Error('fail');
      }),
      /fail/
    );
  } finally {
    await k.destroy();
    cleanup();
  }
});
