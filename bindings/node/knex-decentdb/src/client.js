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
    const db = new Database({ path: filename, options });
    db.__decentStmtCache = new Map();
    return db;
  }

  async destroyRawConnection(connection) {
    if (connection && connection.__decentStmtCache instanceof Map) {
      for (const stmt of connection.__decentStmtCache.values()) {
        stmt.finalize();
      }
      connection.__decentStmtCache.clear();
    }
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
    const normalizedControl = normalizeControlSql(sql);
    if (normalizedControl && bindings.length === 0) {
      if (normalizedControl === 'BEGIN') {
        connection.beginTransaction();
      } else if (normalizedControl === 'COMMIT') {
        connection.commitTransaction();
      } else if (normalizedControl === 'ROLLBACK') {
        connection.rollbackTransaction();
      }
      obj.response = { rows: [], rowCount: 0n };
      return obj;
    }

    let stmt;
    if (connection.__decentStmtCache instanceof Map) {
      stmt = connection.__decentStmtCache.get(sql);
      if (!stmt) {
        stmt = connection.prepare(sql);
        connection.__decentStmtCache.set(sql, stmt);
      }
    } else {
      stmt = connection.prepare(sql);
    }

    try {
      let hasRow;
      if (typeof stmt.stepWithParams === 'function') {
        hasRow = stmt.stepWithParams(bindings);
      } else {
        stmt.reset();
        stmt.clearBindings();
        stmt.bindAll(bindings);
        hasRow = stmt.step();
      }

      const colNames = stmt.columnNames();
      const rows = [];
      while (hasRow) {
        const row = stmt.rowArray();
        const rowObj = {};
        for (let i = 0; i < row.length; i++) {
          rowObj[colNames[i]] = row[i];
        }
        rows.push(rowObj);
        hasRow = stmt.step();
      }

      // Mimic pg response format
      obj.response = {
        rows,
        rowCount: stmt.rowsAffected()
      };
      return obj;
    } finally {
      if (!(connection.__decentStmtCache instanceof Map)) {
        stmt.finalize();
      }
    }
  }

  _stream(connection, obj, stream, options) {
    const sql = this.positionBindings(obj.sql);
    const bindings = obj.bindings || [];

    let stmt;
    if (connection.__decentStmtCache instanceof Map) {
      const cacheKey = `${sql}::stream`;
      stmt = connection.__decentStmtCache.get(cacheKey);
      if (!stmt) {
        stmt = connection.prepare(sql);
        connection.__decentStmtCache.set(cacheKey, stmt);
      }
    } else {
      stmt = connection.prepare(sql);
    }

    return new Promise((resolve, reject) => {
      stream.on('error', reject);
      stream.on('end', resolve);

      try {
        let hasRow;
        if (typeof stmt.stepWithParams === 'function') {
          hasRow = stmt.stepWithParams(bindings);
        } else {
          stmt.reset();
          stmt.clearBindings();
          stmt.bindAll(bindings);
          hasRow = stmt.step();
        }
        const colNames = stmt.columnNames();
        while (hasRow) {
          const row = stmt.rowArray();
          const rowObj = {};
          for (let i = 0; i < row.length; i++) {
            rowObj[colNames[i]] = row[i];
          }
          stream.write(rowObj);
          hasRow = stmt.step();
        }
      } catch (err) {
        stream.emit('error', err);
      } finally {
        if (!(connection.__decentStmtCache instanceof Map)) {
          stmt.finalize();
        }
      }
      stream.end();
    });
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

function normalizeControlSql(sql) {
  if (typeof sql !== 'string') return null;
  const trimmed = sql.trim().replace(/;+$/g, '').trim().toUpperCase().replace(/\s+/g, ' ');
  if (trimmed === 'BEGIN' || trimmed === 'BEGIN TRANSACTION' || trimmed === 'START TRANSACTION') {
    return 'BEGIN';
  }
  if (trimmed === 'COMMIT' || trimmed === 'END' || trimmed === 'END TRANSACTION') {
    return 'COMMIT';
  }
  if (trimmed === 'ROLLBACK' || trimmed === 'ROLLBACK TRANSACTION') {
    return 'ROLLBACK';
  }
  return null;
}

module.exports = {
  Client_DecentDB
};
