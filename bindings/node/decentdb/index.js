'use strict';

const path = require('node:path');
const fs = require('node:fs');

// Avoid extra deps like node-gyp-build; keep the scaffold minimal.
// When built, the addon lives at ./build/Release/decentdb_native.node
const addonPath = path.join(__dirname, 'build', 'Release', 'decentdb_native.node');

let native = null;

/*
 * FinalizationRegistry safety net: if a Database or Statement is garbage-
 * collected without an explicit close()/finalize() call, the native handle
 * is closed here. This is a last resort; explicit cleanup is still strongly
 * preferred.
 */
const _dbRegistry = new FinalizationRegistry((handle) => {
  try {
    if (handle && native) native.dbClose(handle);
  } catch (_) { /* best-effort GC cleanup */ }
});

const _stmtRegistry = new FinalizationRegistry((handle) => {
  try {
    if (handle && native) native.stmtFinalize(handle);
  } catch (_) { /* best-effort GC cleanup */ }
});
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

function normalizeViews(parsed) {
  if (!Array.isArray(parsed)) return [];
  return parsed.map((view) => ({
    ...view,
    name: view?.name ?? '',
  }));
}

function normalizeTriggers(parsed) {
  if (!Array.isArray(parsed)) return [];
  return parsed.map((trigger) => ({
    ...trigger,
    name: trigger?.name ?? '',
    targetName: trigger?.targetName ?? trigger?.target_name ?? '',
  }));
}

function normalizeOpenOptions({ mode, options } = {}) {
  const legacy = options == null ? '' : String(options).trim();
  if (mode != null && legacy !== '') {
    throw new TypeError('Use either mode or options, not both');
  }

  const requested =
    mode != null
      ? String(mode)
      : legacy === 'mode=open' || legacy === 'mode=create' || legacy === 'mode=openOrCreate'
        ? legacy.slice('mode='.length)
        : legacy === ''
          ? 'openOrCreate'
          : null;

  switch (requested) {
    case 'openOrCreate':
      return 'mode=openOrCreate';
    case 'open':
      return 'mode=open';
    case 'create':
      return 'mode=create';
    default:
      throw new TypeError(
        "Unsupported native open options. Use mode: 'openOrCreate', 'open', or 'create'.",
      );
  }
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

function normalizeControlSql(sql) {
  if (typeof sql !== 'string') return null;
  const trimmed = sql.trim().replace(/;+$/g, '').trim().toUpperCase().replace(/\s+/g, ' ');
  if (trimmed === 'BEGIN' || trimmed === 'BEGIN TRANSACTION') return 'BEGIN';
  if (trimmed === 'START TRANSACTION') return 'BEGIN';
  if (trimmed === 'COMMIT' || trimmed === 'END' || trimmed === 'END TRANSACTION') return 'COMMIT';
  if (trimmed === 'ROLLBACK' || trimmed === 'ROLLBACK TRANSACTION') return 'ROLLBACK';
  return null;
}

class Database {
  constructor({ path, mode, options } = {}) {
    if (!path || typeof path !== 'string') {
      throw new TypeError('Database requires { path: string }');
    }
    this._native = loadNative();
    this.path = path;
    this._handle = this._native.dbOpen(path, normalizeOpenOptions({ mode, options }));
    _dbRegistry.register(this, this._handle, this);
  }

  static create(path) {
    return new Database({ path, mode: 'create' });
  }

  static openExisting(path) {
    return new Database({ path, mode: 'open' });
  }

  static openOrCreate(path) {
    return new Database({ path, mode: 'openOrCreate' });
  }

  close() {
    if (!this._handle) return;
    _dbRegistry.unregister(this);
    this._native.dbClose(this._handle);
    this._handle = null;
  }

  get abiVersion() {
    return Database.abiVersion();
  }

  get engineVersion() {
    return Database.version();
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
        } else if (c === '/' && i + 1 < sql.length && sql[i + 1] === '*') {
          i += 2;
          while (i + 1 < sql.length && !(sql[i] === '*' && sql[i + 1] === '/')) i++;
          if (i + 1 < sql.length) i++;
        }
      }
    }
    return false;
  }

  exec(sql, bindings) {
    const control = normalizeControlSql(sql);
    if (control && (bindings == null || (Array.isArray(bindings) && bindings.length === 0))) {
      if (control === 'BEGIN') this._native.dbBeginTransaction(this._handle);
      if (control === 'COMMIT') this._native.dbCommitTransaction(this._handle);
      if (control === 'ROLLBACK') this._native.dbRollbackTransaction(this._handle);
      return { rows: [], rowsAffected: 0n };
    }
    const stmt = this.prepare(sql);
    try {
      const rows = [];
      stmt.reset();
      stmt.clearBindings();
      stmt.bindAll(bindings);
      while (true) {
        const row = stmt.stepRowView();
        if (row === null) break;
        rows.push(row);
      }
      return { rows, rowsAffected: stmt.rowsAffected() };
    } finally {
      stmt.finalize();
    }
  }

  async execAsync(sql, bindings) {
    const control = normalizeControlSql(sql);
    if (control && (bindings == null || (Array.isArray(bindings) && bindings.length === 0))) {
      if (control === 'BEGIN') this._native.dbBeginTransaction(this._handle);
      if (control === 'COMMIT') this._native.dbCommitTransaction(this._handle);
      if (control === 'ROLLBACK') this._native.dbRollbackTransaction(this._handle);
      return { rows: [], rowsAffected: 0n };
    }
    const stmt = this.prepare(sql);
    try {
      const rows = [];
      stmt.reset();
      stmt.clearBindings();
      stmt.bindAll(bindings);
      while (true) {
        const row = stmt.stepRowView();
        if (row === null) break;
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

  beginTransaction() {
    if (!this._handle) throw new Error('Database is closed');
    this._native.dbBeginTransaction(this._handle);
  }

  commitTransaction() {
    if (!this._handle) throw new Error('Database is closed');
    this._native.dbCommitTransaction(this._handle);
  }

  rollbackTransaction() {
    if (!this._handle) throw new Error('Database is closed');
    this._native.dbRollbackTransaction(this._handle);
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

  getTableDdl(tableName) {
    if (!this._handle) throw new Error('Database is closed');
    if (typeof tableName !== 'string') throw new TypeError('tableName must be a string');
    if (typeof this._native.dbGetTableDdl !== 'function') {
      throw new Error('dbGetTableDdl not available in this build');
    }
    return this._native.dbGetTableDdl(this._handle, tableName);
  }

  listViewsInfo() {
    if (!this._handle) throw new Error('Database is closed');
    if (typeof this._native.dbListViewsJson !== 'function') {
      throw new Error('dbListViewsJson not available in this build');
    }
    return normalizeViews(JSON.parse(this._native.dbListViewsJson(this._handle)));
  }

  listViews() {
    return this.listViewsInfo().map((view) => view.name);
  }

  getViewDdl(viewName) {
    if (!this._handle) throw new Error('Database is closed');
    if (typeof viewName !== 'string') throw new TypeError('viewName must be a string');
    if (typeof this._native.dbGetViewDdl !== 'function') {
      throw new Error('dbGetViewDdl not available in this build');
    }
    return this._native.dbGetViewDdl(this._handle, viewName);
  }

  listTriggers() {
    if (!this._handle) throw new Error('Database is closed');
    if (typeof this._native.dbListTriggersJson !== 'function') {
      throw new Error('dbListTriggersJson not available in this build');
    }
    return normalizeTriggers(JSON.parse(this._native.dbListTriggersJson(this._handle)));
  }

  get inTransaction() {
    if (!this._handle) throw new Error('Database is closed');
    if (typeof this._native.dbInTransaction !== 'function') return false;
    return this._native.dbInTransaction(this._handle);
  }

  static evictSharedWal(dbPath) {
    const n = loadNative();
    if (typeof n.dbEvictSharedWal !== 'function') {
      throw new Error('dbEvictSharedWal not available in this build');
    }
    n.dbEvictSharedWal(dbPath);
  }

  static abiVersion() {
    const n = loadNative();
    if (typeof n.ddbAbiVersion !== 'function') return 0;
    return n.ddbAbiVersion();
  }

  static version() {
    const n = loadNative();
    if (typeof n.ddbVersion !== 'function') return '';
    return n.ddbVersion();
  }
}

class Statement {
  constructor(db, sql) {
    if (typeof sql !== 'string') throw new TypeError('sql must be a string');
    this._db = db;
    this._native = db._native;
    this._handle = this._native.stmtPrepare(db._handle, sql);
    _stmtRegistry.register(this, this._handle, this);
  }

  finalize() {
    if (!this._handle) return;
    _stmtRegistry.unregister(this);
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
      } else if (v instanceof Date) {
        if (typeof this._native.stmtBindTimestampMicros !== 'function') {
          throw new Error('stmtBindTimestampMicros not available in this build');
        }
        this._native.stmtBindTimestampMicros(this._handle, index1, BigInt(v.getTime()) * 1000n);
      } else if (typeof v === 'object' && v !== null && typeof v.unscaled === 'bigint' && typeof v.scale === 'number') {
        this._native.stmtBindDecimal(this._handle, index1, v.unscaled, v.scale);
      } else if (typeof v === 'object' && v !== null && v._isTimestampMicros === true && typeof v.micros === 'bigint') {
        // Explicit TIMESTAMP_MICROS object: { _isTimestampMicros: true, micros: BigInt }
        if (typeof this._native.stmtBindTimestampMicros === 'function') {
          this._native.stmtBindTimestampMicros(this._handle, index1, v.micros);
        } else {
          throw new Error('stmtBindTimestampMicros not available in this build');
        }
      } else {
        throw new TypeError(`Unsupported binding type at $${index1}: ${typeof v}`);
      }
    }
  }

  stepWithParams(bindings) {
    return this._native.stmtStepWithParams(this._handle, bindings);
  }

  executeBatchI64TextF64(ids, texts, floats) {
    if (typeof this._native.stmtExecuteBatchI64TextF64 !== 'function') {
      throw new Error('Native batch insert API is unavailable in this build');
    }
    return this._native.stmtExecuteBatchI64TextF64(this._handle, ids, texts, floats);
  }

  step() {
    return this._native.stmtStep(this._handle);
  }

  bindTimestampMicros(index, micros) {
    if (typeof this._native.stmtBindTimestampMicros !== 'function') {
      throw new Error('stmtBindTimestampMicros not available in this build');
    }
    this._native.stmtBindTimestampMicros(this._handle, index, micros);
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

  fetchRowsI64TextF64(maxRows) {
    if (typeof this._native.stmtFetchRowsI64TextF64 !== 'function') {
      throw new Error('Native batch fetch API is unavailable in this build');
    }
    return this._native.stmtFetchRowsI64TextF64(this._handle, maxRows);
  }

  fetchRowsI64TextF64Number(maxRows) {
    if (typeof this._native.stmtFetchRowsI64TextF64Number !== 'function') {
      throw new Error('Native numeric batch fetch API is unavailable in this build');
    }
    return this._native.stmtFetchRowsI64TextF64Number(this._handle, maxRows);
  }

  /**
   * Fused step + row-view. Returns the next row as an array, or null if done.
   * More efficient than separate step()/rowArray() for generic iteration.
   */
  stepRowView() {
    if (typeof this._native.stmtStepRowView !== 'function') {
      // Fallback: use step + rowArray
      const hasRow = this._native.stmtStep(this._handle);
      if (!hasRow) return null;
      return this._native.stmtRowArray(this._handle);
    }
    return this._native.stmtStepRowView(this._handle);
  }

  /**
   * Reset and rebind a single INT64 parameter, then execute (step to completion).
   * Designed for fast UPDATE/DELETE by primary key.
   * Returns affected rows as BigInt.
   */
  reBindInt64Execute(value) {
    if (typeof this._native.stmtReBindInt64Execute !== 'function') {
      throw new Error('stmtReBindInt64Execute not available in this build');
    }
    const v = typeof value === 'number' ? BigInt(value) : value;
    return this._native.stmtReBindInt64Execute(this._handle, v);
  }

  /**
   * Reset and rebind (text, int64) parameters, then execute.
   * Returns affected rows as BigInt.
   */
  reBindTextInt64Execute(textValue, intValue) {
    if (typeof this._native.stmtReBindTextInt64Execute !== 'function') {
      throw new Error('stmtReBindTextInt64Execute not available in this build');
    }
    const iv = typeof intValue === 'number' ? BigInt(intValue) : intValue;
    return this._native.stmtReBindTextInt64Execute(this._handle, textValue, iv);
  }

  /**
   * Reset and rebind (int64, text) parameters, then execute.
   * Returns affected rows as BigInt.
   */
  reBindInt64TextExecute(intValue, textValue) {
    if (typeof this._native.stmtReBindInt64TextExecute !== 'function') {
      throw new Error('stmtReBindInt64TextExecute not available in this build');
    }
    const iv = typeof intValue === 'number' ? BigInt(intValue) : intValue;
    return this._native.stmtReBindInt64TextExecute(this._handle, iv, textValue);
  }

  /**
   * Bind a TIMESTAMP_MICROS value directly. Pass microseconds since epoch as
   * a BigInt, or milliseconds since epoch as a Number (auto-scaled ×1000).
   */
  bindTimestamp(index1, value) {
    if (typeof this._native.stmtBindTimestampMicros !== 'function') {
      throw new Error('stmtBindTimestampMicros not available in this build');
    }
    return this._native.stmtBindTimestampMicros(this._handle, index1, value);
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
  /**
   * Create a TIMESTAMP_MICROS binding value.
   * - Pass a BigInt for microseconds since Unix epoch (already µs).
   * - Pass a Number for milliseconds since Unix epoch (auto-scaled ×1000 to µs).
   */
  timestampMicros(value) {
    if (typeof value === 'number') {
      return { _isTimestampMicros: true, micros: BigInt(Math.round(value * 1000)) };
    }
    return { _isTimestampMicros: true, micros: value };
  },
  _loadNative: loadNative
};
