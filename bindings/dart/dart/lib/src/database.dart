import 'dart:collection';
import 'dart:convert';
import 'dart:ffi';

import 'package:ffi/ffi.dart';

import 'errors.dart';
import 'native_bindings.dart';
import 'schema.dart';
import 'statement.dart';
import 'types.dart';

/// Open mode used by [Database._openWith].
enum _OpenMode { create, open, openOrCreate }

/// An open DecentDB database.
///
/// Obtain via [Database.open], [Database.create], [Database.openExisting], or
/// [Database.memory].  Call [close] when done.  If [close] is not called before
/// the object is garbage collected, a Dart [Finalizer] will release the native
/// handle automatically.
class Database {
  Database._(this._bindings, Pointer<DdbDb> dbPtr, {int stmtCacheCapacity = 128})
      : _dbPtr = dbPtr,
        _cap = stmtCacheCapacity,
        _lru = LinkedHashMap<String, Statement>() {
    _finalizer.attach(
      this,
      (bindings: _bindings, ptr: dbPtr),
      detach: this,
    );
  }

  // GC-safety finalizer: runs if close() is never called.
  static final _finalizer =
      Finalizer<({NativeBindings bindings, Pointer<DdbDb> ptr})>((token) {
    final slot = calloc<Pointer<DdbDb>>()..value = token.ptr;
    try {
      token.bindings.dbFree(slot);
    } finally {
      calloc.free(slot);
    }
  });

  final NativeBindings _bindings;
  Pointer<DdbDb>? _dbPtr;

  // Prepared-statement LRU cache (D4).
  final LinkedHashMap<String, Statement> _lru;
  final int _cap;
  int _hits = 0;
  int _misses = 0;
  bool _perfWarningEmitted = false;
  void Function(PerformanceWarning)? _onPerformanceWarning;

  void _checkOpen() {
    if (_dbPtr == null || _dbPtr == nullptr) {
      throw StateError('Database is closed');
    }
  }

  Never _throwStatus(int status, String fallback) {
    final msgPtr = _bindings.lastErrorMessage();
    final msg = msgPtr == nullptr ? fallback : msgPtr.toDartString();
    throw DecentDbException(ErrorCode.fromCode(status), msg);
  }

  // ---------------------------------------------------------------------------
  // Factory constructors
  // ---------------------------------------------------------------------------

  static NativeBindings _resolveBindings(
      String? libraryPath, NativeBindings? bindings) {
    return bindings ??
        NativeBindings.load(libraryPath ?? NativeBindings.defaultLibraryName());
  }

  static Database _openWith(
    _OpenMode mode,
    String path, {
    String? libraryPath,
    NativeBindings? bindings,
    int stmtCacheCapacity = 128,
  }) {
    final nb = _resolveBindings(libraryPath, bindings);
    final nativePath = path.toNativeUtf8();
    final outDb = calloc<Pointer<DdbDb>>();
    try {
      final int status;
      switch (mode) {
        case _OpenMode.create:
          status = nb.dbCreate(nativePath, outDb);
        case _OpenMode.open:
          status = nb.dbOpen(nativePath, outDb);
        case _OpenMode.openOrCreate:
          status = nb.dbOpenOrCreate(nativePath, outDb);
      }
      if (status != ddbOk) {
        final msgPtr = nb.lastErrorMessage();
        final msg = msgPtr == nullptr
            ? 'Failed to open database at $path'
            : msgPtr.toDartString();
        throw DecentDbException(ErrorCode.fromCode(status), msg);
      }
      return Database._(nb, outDb.value, stmtCacheCapacity: stmtCacheCapacity);
    } finally {
      calloc.free(outDb);
      calloc.free(nativePath);
    }
  }

  /// Open or create a database at [path].
  ///
  /// The [options] parameter is reserved for future ABI extension; passing a
  /// non-empty string currently throws [ArgumentError].
  ///
  /// [stmtCacheCapacity] sets the maximum number of prepared statements held
  /// in the internal LRU cache.  Pass `0` to disable caching.
  static Database open(
    String path, {
    String? options,
    String? libraryPath,
    NativeBindings? bindings,
    int stmtCacheCapacity = 128,
  }) {
    if (options != null && options.trim().isNotEmpty) {
      throw ArgumentError(
        'Database.open(options: ...) is not exposed by the current stable ddb_* ABI.',
      );
    }
    return _openWith(
      _OpenMode.openOrCreate,
      path,
      libraryPath: libraryPath,
      bindings: bindings,
      stmtCacheCapacity: stmtCacheCapacity,
    );
  }

  /// Create a new database at [path], failing if it already exists.
  static Database create(
    String path, {
    String? libraryPath,
    NativeBindings? bindings,
    int stmtCacheCapacity = 128,
  }) =>
      _openWith(_OpenMode.create, path,
          libraryPath: libraryPath,
          bindings: bindings,
          stmtCacheCapacity: stmtCacheCapacity);

  /// Open an existing database at [path], failing if it does not exist.
  static Database openExisting(
    String path, {
    String? libraryPath,
    NativeBindings? bindings,
    int stmtCacheCapacity = 128,
  }) =>
      _openWith(_OpenMode.open, path,
          libraryPath: libraryPath,
          bindings: bindings,
          stmtCacheCapacity: stmtCacheCapacity);

  /// Open an in-memory database (not persisted to disk).
  static Database memory({
    String? libraryPath,
    NativeBindings? bindings,
    int stmtCacheCapacity = 128,
  }) =>
      open(':memory:',
          libraryPath: libraryPath,
          bindings: bindings,
          stmtCacheCapacity: stmtCacheCapacity);

  /// Evict the shared WAL cache entry for an on-disk database [path].
  ///
  /// This should only be used after all handles for that path are closed.
  static void evictSharedWal(
    String path, {
    String? libraryPath,
    NativeBindings? bindings,
  }) {
    final nb = _resolveBindings(libraryPath, bindings);
    final nativePath = path.toNativeUtf8();
    try {
      final status = nb.evictSharedWal(nativePath);
      if (status != ddbOk) {
        final msgPtr = nb.lastErrorMessage();
        final msg = msgPtr == nullptr
            ? 'Failed to evict shared WAL for $path'
            : msgPtr.toDartString();
        throw DecentDbException(ErrorCode.fromCode(status), msg);
      }
    } finally {
      calloc.free(nativePath);
    }
  }

  // ---------------------------------------------------------------------------
  // Properties
  // ---------------------------------------------------------------------------

  /// Engine version string reported by the native library.
  String get engineVersion {
    final versionPtr = _bindings.version();
    return versionPtr == nullptr ? 'unknown' : versionPtr.toDartString();
  }

  /// `true` if a transaction is currently open on this database.
  bool get inTransaction {
    _checkOpen();
    final outFlag = calloc<Uint8>();
    try {
      final status = _bindings.dbInTransaction(_dbPtr!, outFlag);
      if (status != ddbOk)
        _throwStatus(status, 'Failed to query transaction state');
      return outFlag.value != 0;
    } finally {
      calloc.free(outFlag);
    }
  }

  // ---------------------------------------------------------------------------
  // Statement API
  // ---------------------------------------------------------------------------

  /// Register a callback that receives [PerformanceWarning] notifications.
  ///
  /// Fires at most once per [Database] lifetime, when the statement cache hit
  /// rate falls below 50% after 100 cumulative prepares.
  set onPerformanceWarning(void Function(PerformanceWarning)? sink) {
    _onPerformanceWarning = sink;
  }

  /// Cache statistics: `hits`, `misses`, `size`, `capacity`.
  Map<String, int> get stmtCacheStats => {
        'hits': _hits,
        'misses': _misses,
        'size': _lru.length,
        'capacity': _cap,
      };

  /// Finalize every cached statement and reset cache counters.
  void clearStmtCache() {
    for (final stmt in _lru.values) {
      if (!stmt.isDisposed) stmt.dispose();
    }
    _lru.clear();
    _hits = 0;
    _misses = 0;
    _perfWarningEmitted = false;
  }

  void _maybeEmitPerfWarning() {
    if (_perfWarningEmitted) return;
    final total = _hits + _misses;
    if (total >= 100 && _hits / total < 0.5) {
      _perfWarningEmitted = true;
      final pct = (_hits * 100 ~/ total);
      _onPerformanceWarning?.call(PerformanceWarning(
        'stmt cache hit rate $pct% over $total prepares — '
        'consider raising stmtCacheCapacity or reusing Statement handles.',
      ));
    }
  }

  /// Prepare [sql] as a native statement.
  ///
  /// When the statement cache is enabled (capacity > 0), repeated calls with
  /// the same SQL return a cached handle (reset and cleared) without
  /// re-parsing. The [Database] owns cached statements; callers must **not**
  /// call [Statement.dispose] on them — doing so marks the handle as disposed
  /// and the cache will create a fresh one on the next call.
  ///
  /// When the cache is disabled (capacity == 0), callers **must** call
  /// [Statement.dispose] to avoid native handle leaks.
  Statement prepare(String sql) {
    _checkOpen();
    if (_cap > 0) {
      final cached = _lru.remove(sql);
      if (cached != null && !cached.isDisposed) {
        _lru[sql] = cached; // move to MRU end
        cached.resetForReuse();
        _hits++;
        _maybeEmitPerfWarning();
        return cached;
      }
      // Cache miss (or previously disposed cached entry).
      _misses++;
      final stmt = Statement.fromSql(_bindings, _dbPtr!, sql);
      _lru[sql] = stmt;
      if (_lru.length > _cap) {
        // Evict the least-recently-used entry.
        final oldestKey = _lru.keys.first;
        final evicted = _lru.remove(oldestKey)!;
        if (!evicted.isDisposed) evicted.dispose();
      }
      _maybeEmitPerfWarning();
      return stmt;
    }
    _misses++;
    _maybeEmitPerfWarning();
    return Statement.fromSql(_bindings, _dbPtr!, sql);
  }

  /// Prepare [sql] directly, bypassing the cache. Used by convenience helpers
  /// that manage their own statement lifecycle with [Statement.dispose].
  Statement _rawPrepare(String sql) =>
      Statement.fromSql(_bindings, _dbPtr!, sql);

  /// Execute [sql] with no parameters; returns the number of affected rows.
  int execute(String sql) {
    _checkOpen();
    final stmt = _rawPrepare(sql);
    try {
      return stmt.execute();
    } finally {
      stmt.dispose();
    }
  }

  /// Execute [sql] through the one-shot native `ddb_db_execute` path.
  ///
  /// This bypasses prepared statements and is required for SQL transaction
  /// control statements such as `SAVEPOINT`, `RELEASE`, and `ROLLBACK TO`.
  int executeDirect(String sql) {
    _checkOpen();
    final nativeSql = sql.toNativeUtf8();
    final outResult = calloc<Pointer<DdbResult>>();
    final outAffected = calloc<Uint64>();
    try {
      final status = _bindings.dbExecute(
        _dbPtr!,
        nativeSql,
        nullptr.cast<DdbValue>(),
        0,
        outResult,
      );
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to execute SQL');
      }

      final affectedStatus = _bindings.resultAffectedRows(
        outResult.value,
        outAffected,
      );
      if (affectedStatus != ddbOk) {
        _throwStatus(affectedStatus, 'Failed to read affected rows');
      }
      return outAffected.value;
    } finally {
      _bindings.resultFree(outResult); // best-effort; errors here must not mask a primary exception
      calloc.free(outAffected);
      calloc.free(outResult);
      calloc.free(nativeSql);
    }
  }

  /// Execute [sql] with positional [params]; returns affected rows.
  int executeWithParams(String sql, List<Object?> params) {
    _checkOpen();
    final stmt = _rawPrepare(sql);
    try {
      stmt.bindAll(params);
      return stmt.execute();
    } finally {
      stmt.dispose();
    }
  }

  /// Execute [sql] and return all result rows.
  List<Row> query(String sql, [List<Object?> params = const []]) {
    _checkOpen();
    final stmt = _rawPrepare(sql);
    try {
      stmt.bindAll(params);
      return stmt.query();
    } finally {
      stmt.dispose();
    }
  }

  // ---------------------------------------------------------------------------
  // Transaction API
  // ---------------------------------------------------------------------------

  /// Begin an explicit transaction.
  void begin() {
    _checkOpen();
    final status = _bindings.dbBeginTransaction(_dbPtr!);
    if (status != ddbOk) _throwStatus(status, 'Failed to begin transaction');
  }

  /// Commit the current transaction.
  void commit() {
    _checkOpen();
    final outLsn = calloc<Uint64>();
    try {
      final status = _bindings.dbCommitTransaction(_dbPtr!, outLsn);
      if (status != ddbOk) _throwStatus(status, 'Failed to commit transaction');
    } finally {
      calloc.free(outLsn);
    }
  }

  /// Roll back the current transaction.
  void rollback() {
    _checkOpen();
    final status = _bindings.dbRollbackTransaction(_dbPtr!);
    if (status != ddbOk)
      _throwStatus(status, 'Failed to roll back transaction');
  }

  // ---------------------------------------------------------------------------
  // Savepoint API
  // ---------------------------------------------------------------------------

  /// Validates [name] for use as a SQL identifier in savepoint commands.
  ///
  /// Rejects empty strings, names longer than 128 characters, names containing
  /// double-quote characters, and names containing control characters (code
  /// unit < 0x20).
  void _assertValidIdent(String name) {
    if (name.isEmpty) {
      throw ArgumentError.value(name, 'name', 'Savepoint name must not be empty');
    }
    if (name.length > 128) {
      throw ArgumentError.value(
          name, 'name', 'Savepoint name must be 128 characters or fewer');
    }
    for (var i = 0; i < name.length; i++) {
      final c = name.codeUnitAt(i);
      if (c == 0x22) {
        throw ArgumentError.value(
            name, 'name', 'Savepoint name must not contain double-quote characters');
      }
      if (c < 0x20) {
        throw ArgumentError.value(
            name, 'name', 'Savepoint name must not contain control characters');
      }
    }
  }

  /// Create a savepoint named [name] within the current transaction.
  ///
  /// Savepoints follow standard SQL semantics: multiple savepoints may be
  /// nested, and each is uniquely identified by [name]. To undo work back to
  /// this savepoint use [rollbackToSavepoint]; to permanently keep the work
  /// use [releaseSavepoint].
  void savepoint(String name) {
    _checkOpen();
    _assertValidIdent(name);
    executeDirect('SAVEPOINT "$name"');
  }

  /// Release (commit) the savepoint named [name].
  ///
  /// The work done since the savepoint was created becomes part of the
  /// enclosing transaction. If [name] was created multiple times in a nested
  /// stack, only the innermost matching savepoint is released.
  void releaseSavepoint(String name) {
    _checkOpen();
    _assertValidIdent(name);
    executeDirect('RELEASE SAVEPOINT "$name"');
  }

  /// Roll back the current transaction to the savepoint named [name].
  ///
  /// All work done after the savepoint was created is discarded.  The
  /// savepoint itself is not destroyed — it can be reused or released
  /// afterwards.
  void rollbackToSavepoint(String name) {
    _checkOpen();
    _assertValidIdent(name);
    executeDirect('ROLLBACK TO SAVEPOINT "$name"');
  }

  /// Run [action] inside a transaction.  Commits on success; rolls back on
  /// any exception and rethrows.
  T transaction<T>(T Function() action) {
    begin();
    try {
      final result = action();
      commit();
      return result;
    } catch (error, stackTrace) {
      rollback();
      Error.throwWithStackTrace(error, stackTrace);
    }
  }

  // ---------------------------------------------------------------------------
  // Schema
  // ---------------------------------------------------------------------------

  /// Access schema metadata (tables, indexes, views, triggers).
  Schema get schema {
    _checkOpen();
    return Schema.fromNative(_bindings, _dbPtr!);
  }

  // ---------------------------------------------------------------------------
  // Maintenance
  // ---------------------------------------------------------------------------

  /// Force a WAL checkpoint.
  void checkpoint() {
    _checkOpen();
    final status = _bindings.dbCheckpoint(_dbPtr!);
    if (status != ddbOk) _throwStatus(status, 'Failed to checkpoint database');
  }

  /// Returns a snapshot of the engine's current pager / WAL / cache state.
  ///
  /// Useful for diagnostics and observability. The returned
  /// [StorageStateSnapshot] captures all fields emitted by the engine at the
  /// moment of the call; [StorageStateSnapshot.rawJson] preserves the full
  /// JSON text for forward compatibility.
  StorageStateSnapshot inspectStorageState() {
    _checkOpen();
    final out = calloc<Pointer<Utf8>>();
    try {
      final status = _bindings.dbInspectStorageStateJson(_dbPtr!, out);
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to inspect storage state');
      }
      final rawJson = out.value == nullptr ? '{}' : out.value.toDartString();
      final freeStatus = _bindings.stringFree(out);
      if (freeStatus != ddbOk) {
        _throwStatus(freeStatus, 'Failed to free storage state JSON');
      }
      final decoded = jsonDecode(rawJson);
      if (decoded is! Map<String, Object?>) {
        throw FormatException(
            'Storage state JSON is not an object: $rawJson');
      }
      return StorageStateSnapshot.fromJson(decoded, rawJson: rawJson);
    } finally {
      calloc.free(out);
    }
  }

  /// Save a snapshot of the current database state to [destPath].
  void saveAs(String destPath) {
    _checkOpen();
    final nativePath = destPath.toNativeUtf8();
    try {
      final status = _bindings.dbSaveAs(_dbPtr!, nativePath);
      if (status != ddbOk)
        _throwStatus(status, 'Failed to save database as $destPath');
    } finally {
      calloc.free(nativePath);
    }
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  /// Close the database and release the native handle.
  ///
  /// After calling [close], no other methods may be used.  Calling [close]
  /// on an already-closed database is a no-op.  All cached statements are
  /// finalized before the database handle is released.
  void close() {
    if (_dbPtr == null || _dbPtr == nullptr) return;
    clearStmtCache(); // D4: finalize cached statements before closing DB
    _finalizer.detach(this);
    final slot = calloc<Pointer<DdbDb>>()..value = _dbPtr!;
    try {
      final status = _bindings.dbFree(slot);
      if (status != ddbOk) _throwStatus(status, 'Failed to close database');
      _dbPtr = nullptr;
    } finally {
      calloc.free(slot);
    }
  }
}
