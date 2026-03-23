'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const { Client_DecentDB } = require('..');
const knex = require('knex');

const DB_PATH = 'test_knex.db';

function cleanup() {
  try { fs.unlinkSync(DB_PATH); } catch {}
  try { fs.unlinkSync(DB_PATH + '-wal'); } catch {}
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
  }
  
  await k.destroy();
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
  
  await k.destroy();
});
