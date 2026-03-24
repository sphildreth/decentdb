'use strict';

const path = require('node:path');
const fs = require('node:fs');

// Avoid extra deps like node-gyp-build; keep the scaffold minimal.
// When built, the addon lives at ./build/Release/decentdb_native.node
const addonPath = path.join(__dirname, 'build', 'Release', 'decentdb_native.node');

let native = null;
function normalizeTableList(parsed) {
  if (!Array.isArray(parsed)) return [];
  if (parsed.length === 0) return [];
  if (typeof parsed[0] === 'string') return parsed;
  return parsed
    .map((entry) => (entry && typeof entry.name === 'string' ? entry.name : null))
    .filter((name) => name !== null);
}

function normalizeColumns(parsed) {
  if (Array.isArray(parsed)) return parsed;
  if (!parsed || !Array.isArray(parsed.columns)) return [];
  return parsed.columns.map((col) => {
    const out = {
      name: col.name,
      type: col.type ?? col.column_type,
      not_null: col.not_null ?? !col.nullable,
      unique: col.unique ?? false,
      primary_key: col.primary_key ?? false,
    };
    if (col.foreign_key && typeof col.foreign_key === 'object') {
      out.ref_table = col.foreign_key.table;
      out.ref_column = col.foreign_key.column;
      out.ref_on_delete = col.foreign_key.on_delete;
      out.ref_on_update = col.foreign_key.on_update;
    }
    return out;
  });
}

function normalizeIndexes(parsed) {
  if (!Array.isArray(parsed)) return [];
  return parsed.map((idx) => ({
    ...idx,
    table: idx.table ?? idx.table_name ?? '',
  }));
}

function inferNativeLibPath() {
  const explicit = process.env.DECENTDB_NATIVE_LIB_PATH;
  if (explicit && explicit.length > 0) {
    return explicit;
  }

  const candidates = [
    path.join(__dirname, '..', '..', '..', 'target', 'release', 'libdecentdb.so'),
    path.join(__dirname, '..', '..', '..', 'target', 'release', 'libdecentdb.dylib'),
    path.join(__dirname, '..', '..', '..', 'target', 'release', 'decentdb.dll'),
    path.join(__dirname, '..', '..', '..', 'target', 'debug', 'libdecentdb.so'),
    path.join(__dirname, '..', '..', '..', 'target', 'debug', 'libdecentdb.dylib'),
    path.join(__dirname, '..', '..', '..', 'target', 'debug', 'decentdb.dll'),
    path.join(process.cwd(), 'target', 'release', 'libdecentdb.so'),
    path.join(process.cwd(), 'target', 'release', 'libdecentdb.dylib'),
    path.join(process.cwd(), 'target', 'release', 'decentdb.dll'),
    path.join(process.cwd(), 'target', 'debug', 'libdecentdb.so'),
    path.join(process.cwd(), 'target', 'debug', 'libdecentdb.dylib'),
    path.join(process.cwd(), 'target', 'debug', 'decentdb.dll'),
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return null;
}

function loadNative() {
  if (native) return native;
  const inferred = inferNativeLibPath();
  if (inferred) {
    process.env.DECENTDB_NATIVE_LIB_PATH = inferred;
  }
  // Defer the require() so users can install the package before native build.
  native = require(addonPath);
  return native;
}

class Database {
  constructor({ path, options } = {}) {
    if (!path || typeof path !== 'string') {
      throw new TypeError('Database requires { path: string }');
    }
    this._native = loadNative();
    this._handle = this._native.dbOpen(path, options ?? null);
  }

  close() {
    if (!this._handle) return;
    this._native.dbClose(this._handle);
    this._handle = null;
  }

  prepare(sql) {
    if (!this._handle) throw new Error('Database is closed');
    if (typeof sql !== 'string') throw new TypeError('sql must be a string');
    
    // Check for unquoted ? placeholders
    if (this._hasUnquotedQuestionMark(sql)) {
      throw new Error("DecentDB uses Postgres-style placeholders ($1, $2, ...), not '?'. Use 'knex-decentdb' for automatic conversion.");
    }

    return new Statement(this, sql);
  }

  _hasUnquotedQuestionMark(sql) {
    let inString = false;
    let quoteChar = '';
    for (let i = 0; i < sql.length; i++) {
      const c = sql[i];
      if (inString) {
        if (c === quoteChar) {
          if (i + 1 < sql.length && sql[i+1] === quoteChar) {
            i++; 
          } else {
            inString = false;
          }
        }
      } else {
        if (c === "'" || c === '"') {
          inString = true;
          quoteChar = c;
        } else if (c === '?') {
          return true;
        } else if (c === '-' && i + 1 < sql.length && sql[i+1] === '-') {
          i += 2;
          while (i < sql.length && sql[i] !== '\n') i++;
        }
      }
    }
    return false;
  }

  exec(sql, bindings) {
    const stmt = this.prepare(sql);
    try {
      stmt.bindAll(bindings);
      const rows = [];
      while (stmt.step()) {
        rows.push(stmt.rowArray());
      }
      return { rows, rowsAffected: stmt.rowsAffected() };
    } finally {
      stmt.finalize();
    }
  }

  async execAsync(sql, bindings) {
    const stmt = this.prepare(sql);
    try {
      stmt.bindAll(bindings);
      const rows = [];
      for await (const row of stmt.rows()) {
        rows.push(row);
      }
      return { rows, rowsAffected: stmt.rowsAffected() };
    } finally {
      stmt.finalize();
    }
  }

  checkpoint() {
    if (!this._handle) throw new Error('Database is closed');
    this._native.dbCheckpoint(this._handle);
  }

  saveAs(destPath) {
    if (!this._handle) throw new Error('Database is closed');
    if (typeof destPath !== 'string') throw new TypeError('destPath must be a string');
    this._native.dbSaveAs(this._handle, destPath);
  }

  listTables() {
    if (!this._handle) throw new Error('Database is closed');
    const json = this._native.dbListTablesJson(this._handle);
    return normalizeTableList(JSON.parse(json));
  }

  getTableColumns(tableName) {
    if (!this._handle) throw new Error('Database is closed');
    if (typeof tableName !== 'string') throw new TypeError('tableName must be a string');
    const json = this._native.dbGetTableColumnsJson(this._handle, tableName);
    return normalizeColumns(JSON.parse(json));
  }

  listIndexes() {
    if (!this._handle) throw new Error('Database is closed');
    const json = this._native.dbListIndexesJson(this._handle);
    return normalizeIndexes(JSON.parse(json));
  }
}

class Statement {
  constructor(db, sql) {
    if (typeof sql !== 'string') throw new TypeError('sql must be a string');
    this._db = db;
    this._native = db._native;
    this._handle = this._native.stmtPrepare(db._handle, sql);
  }

  finalize() {
    if (!this._handle) return;
    this._native.stmtFinalize(this._handle);
    this._handle = null;
  }

  reset() {
    this._native.stmtReset(this._handle);
  }

  clearBindings() {
    this._native.stmtClearBindings(this._handle);
  }

  bindAll(bindings) {
    if (bindings == null) return;
    if (!Array.isArray(bindings)) {
      throw new TypeError('bindings must be an array (positional)');
    }

    for (let i = 0; i < bindings.length; i++) {
      const index1 = i + 1;
      const v = bindings[i];
      if (v === null || v === undefined) {
        this._native.stmtBindNull(this._handle, index1);
      } else if (typeof v === 'bigint') {
        this._native.stmtBindInt64(this._handle, index1, v);
      } else if (typeof v === 'number') {
        // Conservatively bind numbers as float64 unless they are safe integers.
        if (Number.isSafeInteger(v)) {
          if (typeof this._native.stmtBindInt64Number === 'function') {
            this._native.stmtBindInt64Number(this._handle, index1, v);
          } else {
            this._native.stmtBindInt64(this._handle, index1, BigInt(v));
          }
        } else {
          this._native.stmtBindFloat64(this._handle, index1, v);
        }
      } else if (typeof v === 'boolean') {
        this._native.stmtBindBool(this._handle, index1, v);
      } else if (typeof v === 'string') {
        this._native.stmtBindText(this._handle, index1, v);
      } else if (Buffer.isBuffer(v) || v instanceof Uint8Array) {
        this._native.stmtBindBlob(this._handle, index1, Buffer.from(v));
      } else if (typeof v === 'object' && v !== null && typeof v.unscaled === 'bigint' && typeof v.scale === 'number') {
        this._native.stmtBindDecimal(this._handle, index1, v.unscaled, v.scale);
      } else {
        throw new TypeError(`Unsupported binding type at $${index1}: ${typeof v}`);
      }
    }
  }

  stepWithParams(bindings) {
    return this._native.stmtStepWithParams(this._handle, bindings);
  }

  step() {
    return this._native.stmtStep(this._handle);
  }

  // Decodes the current row via decentdb_row_view (single native call).
  rowArray() {
    return this._native.stmtRowArray(this._handle);
  }

  rowsAffected() {
    return this._native.stmtRowsAffected(this._handle);
  }

  columnNames() {
    return this._native.stmtColumnNames(this._handle);
  }

  rows() {
    const self = this;
    return {
      [Symbol.asyncIterator]() {
        return {
          async next() {
            const row = await self._native.stmtNextAsync(self._handle);
            if (row === null) {
              return { done: true, value: undefined };
            }
            return { done: false, value: row };
          }
        };
      }
    };
  }

  async allAsync() {
    const rows = [];
    for await (const row of this.rows()) {
      rows.push(row);
    }
    return rows;
  }
}

module.exports = {
  Database,
  _loadNative: loadNative
};
