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
  Database._(this._bindings, Pointer<DdbDb> dbPtr) : _dbPtr = dbPtr {
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
        NativeBindings.load(
            libraryPath ?? NativeBindings.defaultLibraryName());
  }

  static Database _openWith(
    _OpenMode mode,
    String path, {
    String? libraryPath,
    NativeBindings? bindings,
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
      return Database._(nb, outDb.value);
    } finally {
      calloc.free(outDb);
      calloc.free(nativePath);
    }
  }

  /// Open or create a database at [path].
  ///
  /// The [options] parameter is reserved for future ABI extension; passing a
  /// non-empty string currently throws [ArgumentError].
  static Database open(
    String path, {
    String? options,
    String? libraryPath,
    NativeBindings? bindings,
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
    );
  }

  /// Create a new database at [path], failing if it already exists.
  static Database create(
    String path, {
    String? libraryPath,
    NativeBindings? bindings,
  }) =>
      _openWith(_OpenMode.create, path,
          libraryPath: libraryPath, bindings: bindings);

  /// Open an existing database at [path], failing if it does not exist.
  static Database openExisting(
    String path, {
    String? libraryPath,
    NativeBindings? bindings,
  }) =>
      _openWith(_OpenMode.open, path,
          libraryPath: libraryPath, bindings: bindings);

  /// Open an in-memory database (not persisted to disk).
  static Database memory({String? libraryPath, NativeBindings? bindings}) =>
      open(':memory:', libraryPath: libraryPath, bindings: bindings);

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
      if (status != ddbOk) _throwStatus(status, 'Failed to query transaction state');
      return outFlag.value != 0;
    } finally {
      calloc.free(outFlag);
    }
  }

  // ---------------------------------------------------------------------------
  // Statement API
  // ---------------------------------------------------------------------------

  /// Prepare [sql] as a native statement.  Caller must call [Statement.dispose].
  Statement prepare(String sql) {
    _checkOpen();
    return Statement.fromSql(_bindings, _dbPtr!, sql);
  }

  /// Execute [sql] with no parameters; returns the number of affected rows.
  int execute(String sql) {
    final stmt = prepare(sql);
    try {
      return stmt.execute();
    } finally {
      stmt.dispose();
    }
  }

  /// Execute [sql] with positional [params]; returns affected rows.
  int executeWithParams(String sql, List<Object?> params) {
    final stmt = prepare(sql);
    try {
      stmt.bindAll(params);
      return stmt.execute();
    } finally {
      stmt.dispose();
    }
  }

  /// Execute [sql] and return all result rows.
  List<Row> query(String sql, [List<Object?> params = const []]) {
    final stmt = prepare(sql);
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
    if (status != ddbOk) _throwStatus(status, 'Failed to roll back transaction');
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

  /// Save a snapshot of the current database state to [destPath].
  void saveAs(String destPath) {
    _checkOpen();
    final nativePath = destPath.toNativeUtf8();
    try {
      final status = _bindings.dbSaveAs(_dbPtr!, nativePath);
      if (status != ddbOk) _throwStatus(status, 'Failed to save database as $destPath');
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
  /// on an already-closed database is a no-op.
  void close() {
    if (_dbPtr == null || _dbPtr == nullptr) return;
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
