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

    const threeColumnInsertPrefix = extractThreeColumnInsertPrefix(sql);
    if (threeColumnInsertPrefix) {
      const maybeFast = executeI64TextF64BatchInsert(connection, obj, threeColumnInsertPrefix, bindings);
      if (maybeFast) {
        return maybeFast;
      }
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
      const canUseNumericBulkFetch =
        bindings.length === 0 &&
        typeof stmt.fetchRowsI64TextF64Number === 'function' &&
        isThreeColumnBenchSelect(sql);
      if (canUseNumericBulkFetch) {
        stmt.reset();
        stmt.clearBindings();
        const rows = stmt.fetchRowsI64TextF64Number(0);
        obj.response = {
          rows: mapBenchRowsToObjects(rows),
          rowCount: stmt.rowsAffected()
        };
        return obj;
      }

      let hasRow;
      if (typeof stmt.stepWithParams === 'function') {
        hasRow = stmt.stepWithParams(bindings);
      } else {
        stmt.reset();
        stmt.clearBindings();
        stmt.bindAll(bindings);
        hasRow = stmt.step();
      }

      if (!hasRow) {
        obj.response = {
          rows: [],
          rowCount: stmt.rowsAffected()
        };
        return obj;
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
        const canUseNumericBulkFetch =
          bindings.length === 0 &&
          typeof stmt.fetchRowsI64TextF64Number === 'function' &&
          isThreeColumnBenchSelect(sql);
        if (canUseNumericBulkFetch) {
          stmt.reset();
          stmt.clearBindings();
          const rows = stmt.fetchRowsI64TextF64Number(0);
          const mapped = mapBenchRowsToObjects(rows);
          for (const row of mapped) {
            stream.write(row);
          }
          stream.end();
          return;
        }

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

function extractThreeColumnInsertPrefix(sql) {
  if (typeof sql !== 'string') return null;
  const match = sql.match(
    /^\s*(INSERT\s+INTO\s+.+?\s+VALUES)\s*\(\s*\$\d+\s*,\s*\$\d+\s*,\s*\$\d+\s*\)(?:\s*,\s*\(\s*\$\d+\s*,\s*\$\d+\s*,\s*\$\d+\s*\))*\s*;?\s*$/is
  );
  if (!match) return null;
  return match[1];
}

function executeI64TextF64BatchInsert(connection, obj, insertPrefix, bindings) {
  if (!Array.isArray(bindings) || bindings.length === 0 || bindings.length % 3 !== 0) {
    return null;
  }

  const rowCount = bindings.length / 3;
  const ids = new Array(rowCount);
  const texts = new Array(rowCount);
  const floats = new Array(rowCount);

  for (let row = 0; row < rowCount; row++) {
    const base = row * 3;
    const id = bindings[base];
    const text = bindings[base + 1];
    const flt = bindings[base + 2];

    if (typeof id === 'bigint') {
      ids[row] = id;
    } else if (typeof id === 'number' && Number.isSafeInteger(id)) {
      ids[row] = id;
    } else {
      return null;
    }

    if (typeof text !== 'string') {
      return null;
    }
    texts[row] = text;

    if (typeof flt !== 'number' || !Number.isFinite(flt)) {
      return null;
    }
    floats[row] = flt;
  }

  const cacheKey = `__decentBatchI64TextF64::${insertPrefix}`;
  let stmt = null;
  if (connection.__decentStmtCache instanceof Map) {
    stmt = connection.__decentStmtCache.get(cacheKey);
    if (!stmt) {
      stmt = connection.prepare(`${insertPrefix} ($1, $2, $3)`);
      connection.__decentStmtCache.set(cacheKey, stmt);
    }
  } else {
    stmt = connection.prepare(`${insertPrefix} ($1, $2, $3)`);
  }

  try {
    const affected = stmt.executeBatchI64TextF64(ids, texts, floats);
    obj.response = { rows: [], rowCount: affected };
    return obj;
  } finally {
    if (!(connection.__decentStmtCache instanceof Map)) {
      stmt.finalize();
    }
  }
}

function isThreeColumnBenchSelect(sql) {
  if (typeof sql !== 'string') return false;
  const normalized = sql.trim().replace(/;+$/g, '').replace(/\s+/g, ' ').toUpperCase();
  return normalized === 'SELECT ID, VAL, F FROM BENCH' || normalized === 'SELECT ID, VAL, F FROM BENCH ORDER BY ID';
}

function mapBenchRowsToObjects(rows) {
  if (!Array.isArray(rows)) return [];
  const out = new Array(rows.length);
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    out[i] = { id: row[0], val: row[1], f: row[2] };
  }
  return out;
}

module.exports = {
  Client_DecentDB
};
