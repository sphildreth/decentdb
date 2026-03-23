'use strict';

const { positionBindings } = require('./positionBindings');

// This is intentionally minimal: a small custom Knex client that uses the
// DecentDB N-API addon. It should be treated as a scaffold.
//
// Knex does not provide a stable public API for custom dialects, so most
// third-party clients import from internal paths.
let Client;
try {
  // Inherit from Postgres client to get $1 binding generation and valid SQL for DDL.
  Client = require('knex/lib/dialects/postgres');
} catch (e) {
  // Fallback to base client if postgres dialect is moved/hidden
  Client = require('knex/lib/client');
}

const { Database } = require('decentdb-native');

class Client_DecentDB extends (Client ?? class {}) {
  constructor(config = {}) {
    super(config);
    this.dialect = 'decentdb';
    this.driverName = 'decentdb-native';
    if (!this.pool) {
        this.initializePool(config);
    }
  }

  _driver() {
    // Knex calls this to get the underlying driver.
    return { Database };
  }

  // Knex calls this to open a new connection.
  async acquireRawConnection() {
    const conn = this.config && this.config.connection ? this.config.connection : {};
    const filename = conn.filename || conn.database || conn.path;
    if (!filename) {
      throw new Error('DecentDB connection requires { filename }');
    }
    const options = conn.options || null;
    return new Database({ path: filename, options });
  }

  async destroyRawConnection(connection) {
    if (connection && typeof connection.close === 'function') {
      connection.close();
    }
  }

  // Convert Knex-style `?` placeholders into `$N`.
  positionBindings(sql) {
    return positionBindings(sql);
  }

  // Used to explicitly validate a connection
  async validateConnection(connection) {
    return true;
  }

  // Run a single query.
  async _query(connection, obj) {
    const sql = this.positionBindings(obj.sql);
    const bindings = obj.bindings || [];

    const stmt = connection.prepare(sql);
    try {
      stmt.bindAll(bindings);
      
      const colNames = stmt.columnNames();
      const rows = [];
      
      for await (const row of stmt.rows()) {
        const rowObj = {};
        for (let i = 0; i < row.length; i++) {
          rowObj[colNames[i]] = row[i];
        }
        rows.push(rowObj);
      }
      
      // Mimic pg response format
      obj.response = { 
        rows, 
        rowCount: stmt.rowsAffected() 
      };
      return obj;
    } finally {
      stmt.finalize();
    }
  }

  async _stream(connection, obj, stream, options) {
    const sql = this.positionBindings(obj.sql);
    const bindings = obj.bindings || [];

    const stmt = connection.prepare(sql);
    try {
      stmt.bindAll(bindings);
      const colNames = stmt.columnNames();
      for await (const row of stmt.rows()) {
        const rowObj = {};
        for (let i = 0; i < row.length; i++) {
          rowObj[colNames[i]] = row[i];
        }
        stream.write(rowObj);
      }
      stream.end();
    } catch (err) {
      stream.emit('error', err);
    } finally {
      stmt.finalize();
    }
  }

  processResponse(obj, runner) {
    if (obj.method === 'raw') return obj.response;
    if (obj.method === 'insert' || obj.method === 'update' || obj.method === 'del') {
       if (obj.response.rows.length > 0) return obj.response.rows;
       return obj.response.rowCount;
    }
    return obj.response.rows;
  }
}

module.exports = {
  Client_DecentDB
};
