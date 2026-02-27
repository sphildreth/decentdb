import 'dart:ffi';
import 'dart:io' show Platform;

import 'package:ffi/ffi.dart';

import 'native_bindings.dart';
import 'statement.dart';
import 'schema.dart';
import 'errors.dart';
import 'types.dart';

/// A DecentDB database connection.
///
/// ## Basic Usage
///
/// ```dart
/// final db = Database.open('mydata.ddb');
/// db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
/// db.execute("INSERT INTO users VALUES (1, 'Alice')");
///
/// final stmt = db.prepare('SELECT * FROM users WHERE id = \$1');
/// stmt.bindInt64(1, 1);
/// for (final row in stmt.query()) {
///   print(row);
/// }
/// stmt.dispose();
/// db.close();
/// ```
///
/// ## Thread Safety
///
/// A Database handle must only be used from one thread/isolate at a time.
/// DecentDB uses a single-writer, multiple-reader model:
///
/// - **Writes** (INSERT/UPDATE/DELETE/DDL): serialize on a single isolate.
/// - **Reads** (SELECT): may run concurrently from separate statement handles,
///   but each statement handle must be used from one isolate only.
///
/// For Flutter apps, run database operations in a dedicated isolate and
/// communicate results back to the UI isolate via ports/messages.
class Database {
  final NativeBindings _bindings;
  Pointer<DecentdbDb>? _dbPtr;

  Database._(this._bindings, this._dbPtr);

  void _checkOpen() {
    if (_dbPtr == null || _dbPtr == nullptr) {
      throw StateError('Database is closed');
    }
  }

  void _throwLastError() {
    final errCode = _bindings.lastErrorCode(_dbPtr!);
    final msgPtr = _bindings.lastErrorMessage(_dbPtr!);
    final msg = msgPtr == nullptr ? 'Unknown error' : msgPtr.toDartString();
    throw DecentDbException(ErrorCode.fromCode(errCode), msg);
  }

  // -------------------------------------------------------------------------
  // Factory constructors
  // -------------------------------------------------------------------------

  /// Open or create a database at [path].
  ///
  /// [options] is an optional query string for configuration:
  /// - `cache_pages=N` or `cache_mb=N` — page cache size
  ///
  /// [libraryPath] is the path to the native library. If omitted, uses the
  /// platform default name (resolved via system library search paths).
  ///
  /// Throws [DecentDbException] on failure.
  static Database open(
    String path, {
    String? options,
    String? libraryPath,
    NativeBindings? bindings,
  }) {
    final b = bindings ??
        NativeBindings.load(libraryPath ?? NativeBindings.defaultLibraryName());

    final pathPtr = path.toNativeUtf8();
    final optPtr = options?.toNativeUtf8() ?? nullptr;
    try {
      final dbPtr = b.open(pathPtr, optPtr.cast<Utf8>());
      if (dbPtr == nullptr) {
        // Error on a null handle; check global error.
        final errCode = b.lastErrorCode(nullptr);
        final msgPtr = b.lastErrorMessage(nullptr);
        final msg = msgPtr == nullptr ? 'Failed to open database' : msgPtr.toDartString();
        throw DecentDbException(ErrorCode.fromCode(errCode), msg);
      }
      return Database._(b, dbPtr);
    } finally {
      calloc.free(pathPtr);
      if (optPtr != nullptr) calloc.free(optPtr);
    }
  }

  /// Open an in-memory database (no persistence).
  static Database memory({
    String? libraryPath,
    NativeBindings? bindings,
  }) {
    return open(':memory:', libraryPath: libraryPath, bindings: bindings);
  }

  /// The engine version string (e.g. "1.6.0").
  String get engineVersion {
    final ptr = _bindings.engineVersion();
    return ptr == nullptr ? 'unknown' : ptr.toDartString();
  }

  // -------------------------------------------------------------------------
  // SQL execution
  // -------------------------------------------------------------------------

  /// Execute a SQL statement with no result rows (DDL, INSERT, UPDATE, DELETE).
  ///
  /// Returns the number of affected rows (0 for DDL).
  ///
  /// For parameterized queries, use [prepare] instead.
  int execute(String sql) {
    _checkOpen();
    final stmt = prepare(sql);
    try {
      return stmt.execute();
    } finally {
      stmt.dispose();
    }
  }

  /// Execute a SQL statement with parameters and return affected rows.
  int executeWithParams(String sql, List<Object?> params) {
    _checkOpen();
    final stmt = prepare(sql);
    try {
      stmt.bindAll(params);
      return stmt.execute();
    } finally {
      stmt.dispose();
    }
  }

  /// Execute a SELECT and return all result rows.
  ///
  /// For large results, use [prepare] + [Statement.nextPage] for paging.
  List<Row> query(String sql, [List<Object?> params = const []]) {
    _checkOpen();
    final stmt = prepare(sql);
    try {
      stmt.bindAll(params);
      return stmt.query();
    } finally {
      stmt.dispose();
    }
  }

  /// Prepare a SQL statement for execution.
  ///
  /// The caller is responsible for calling [Statement.dispose] when done.
  Statement prepare(String sql) {
    _checkOpen();
    final sqlPtr = sql.toNativeUtf8();
    final outStmt = calloc<Pointer<DecentdbStmt>>();
    try {
      final rc = _bindings.prepare(_dbPtr!, sqlPtr, outStmt);
      if (rc != 0) {
        _throwLastError();
      }
      final stmtPtr = outStmt.value;
      if (stmtPtr == nullptr) {
        _throwLastError();
      }
      return Statement.fromNative(_bindings, _dbPtr!, stmtPtr);
    } finally {
      calloc.free(sqlPtr);
      calloc.free(outStmt);
    }
  }

  // -------------------------------------------------------------------------
  // Transactions
  // -------------------------------------------------------------------------

  /// Begin an explicit transaction.
  void begin() {
    _checkOpen();
    final rc = _bindings.begin(_dbPtr!);
    if (rc != 0) _throwLastError();
  }

  /// Commit the active transaction.
  void commit() {
    _checkOpen();
    final rc = _bindings.commit(_dbPtr!);
    if (rc != 0) _throwLastError();
  }

  /// Rollback the active transaction.
  void rollback() {
    _checkOpen();
    final rc = _bindings.rollback(_dbPtr!);
    if (rc != 0) _throwLastError();
  }

  /// Execute [action] inside a transaction.
  ///
  /// Automatically commits on success, rolls back on exception.
  T transaction<T>(T Function() action) {
    begin();
    try {
      final result = action();
      commit();
      return result;
    } catch (e) {
      rollback();
      rethrow;
    }
  }

  // -------------------------------------------------------------------------
  // Schema introspection
  // -------------------------------------------------------------------------

  /// Access schema introspection methods.
  Schema get schema => Schema.fromNative(_bindings, _dbPtr!);

  // -------------------------------------------------------------------------
  // Maintenance
  // -------------------------------------------------------------------------

  /// Flush the WAL to the main database file.
  void checkpoint() {
    _checkOpen();
    final rc = _bindings.checkpoint(_dbPtr!);
    if (rc != 0) _throwLastError();
  }

  /// Export the database to a new file at [destPath].
  void saveAs(String destPath) {
    _checkOpen();
    final pathPtr = destPath.toNativeUtf8();
    try {
      final rc = _bindings.saveAs(_dbPtr!, pathPtr);
      if (rc != 0) _throwLastError();
    } finally {
      calloc.free(pathPtr);
    }
  }

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  /// Close the database and release all native resources.
  void close() {
    if (_dbPtr != null && _dbPtr != nullptr) {
      _bindings.close(_dbPtr!);
      _dbPtr = null;
    }
  }
}
