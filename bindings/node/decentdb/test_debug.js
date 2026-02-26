'use strict';
const { Database } = require('.');
const assert = require('assert');
const fs = require('fs');

try { fs.unlinkSync('debug.db'); } catch {}
const db = new Database({ path: 'debug.db' });
db.exec('CREATE TABLE foo (id BIGINT, txt TEXT, b BLOB)');
db.exec('INSERT INTO foo VALUES ($1, $2, $3)', [1n, 'hello', Buffer.from([1, 2, 3])]);

const resSync = db.exec('SELECT * FROM foo');
console.log('Sync:', resSync.rows[0]);
