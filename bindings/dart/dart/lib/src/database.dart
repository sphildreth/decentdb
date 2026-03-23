import 'dart:ffi';

import 'package:ffi/ffi.dart';

import 'errors.dart';
import 'native_bindings.dart';
import 'schema.dart';
import 'statement.dart';
import 'types.dart';

class Database {
  Database._(this._bindings, this._dbPtr);

  final NativeBindings _bindings;
  Pointer<DdbDb>? _dbPtr;

  void _checkOpen() {
    if (_dbPtr == null || _dbPtr == nullptr) {
      throw StateError('Database is closed');
    }
  }

  Never _throwStatus(int status, String fallback) {
    final messagePtr = _bindings.lastErrorMessage();
    final message =
        messagePtr == nullptr ? fallback : messagePtr.toDartString();
    throw DecentDbException(ErrorCode.fromCode(status), message);
  }

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

    final nativeBindings = bindings ??
        NativeBindings.load(libraryPath ?? NativeBindings.defaultLibraryName());
    final nativePath = path.toNativeUtf8();
    final outDb = calloc<Pointer<DdbDb>>();
    try {
      final status = nativeBindings.dbOpenOrCreate(nativePath, outDb);
      if (status != ddbOk) {
        final messagePtr = nativeBindings.lastErrorMessage();
        final message = messagePtr == nullptr
            ? 'Failed to open database'
            : messagePtr.toDartString();
        throw DecentDbException(ErrorCode.fromCode(status), message);
      }
      return Database._(nativeBindings, outDb.value);
    } finally {
      calloc.free(outDb);
      calloc.free(nativePath);
    }
  }

  static Database memory({String? libraryPath, NativeBindings? bindings}) =>
      open(':memory:', libraryPath: libraryPath, bindings: bindings);

  String get engineVersion {
    final versionPtr = _bindings.version();
    return versionPtr == nullptr ? 'unknown' : versionPtr.toDartString();
  }

  Statement prepare(String sql) {
    _checkOpen();
    return Statement.fromSql(_bindings, _dbPtr!, sql);
  }

  int execute(String sql) {
    final statement = prepare(sql);
    try {
      return statement.execute();
    } finally {
      statement.dispose();
    }
  }

  int executeWithParams(String sql, List<Object?> params) {
    final statement = prepare(sql);
    try {
      statement.bindAll(params);
      return statement.execute();
    } finally {
      statement.dispose();
    }
  }

  List<Row> query(String sql, [List<Object?> params = const []]) {
    final statement = prepare(sql);
    try {
      statement.bindAll(params);
      return statement.query();
    } finally {
      statement.dispose();
    }
  }

  void begin() {
    _checkOpen();
    final status = _bindings.dbBeginTransaction(_dbPtr!);
    if (status != ddbOk) {
      _throwStatus(status, 'Failed to begin transaction');
    }
  }

  void commit() {
    _checkOpen();
    final outLsn = calloc<Uint64>();
    try {
      final status = _bindings.dbCommitTransaction(_dbPtr!, outLsn);
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to commit transaction');
      }
    } finally {
      calloc.free(outLsn);
    }
  }

  void rollback() {
    _checkOpen();
    final status = _bindings.dbRollbackTransaction(_dbPtr!);
    if (status != ddbOk) {
      _throwStatus(status, 'Failed to roll back transaction');
    }
  }

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

  Schema get schema {
    _checkOpen();
    return Schema.fromNative(_bindings, _dbPtr!);
  }

  void checkpoint() {
    _checkOpen();
    final status = _bindings.dbCheckpoint(_dbPtr!);
    if (status != ddbOk) {
      _throwStatus(status, 'Failed to checkpoint database');
    }
  }

  void saveAs(String destPath) {
    _checkOpen();
    final nativePath = destPath.toNativeUtf8();
    try {
      final status = _bindings.dbSaveAs(_dbPtr!, nativePath);
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to save database as $destPath');
      }
    } finally {
      calloc.free(nativePath);
    }
  }

  void close() {
    if (_dbPtr == null || _dbPtr == nullptr) {
      return;
    }
    final slot = calloc<Pointer<DdbDb>>()..value = _dbPtr!;
    try {
      final status = _bindings.dbFree(slot);
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to close database');
      }
      _dbPtr = nullptr;
    } finally {
      calloc.free(slot);
    }
  }
}
