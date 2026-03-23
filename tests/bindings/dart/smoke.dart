import 'dart:ffi';
import 'dart:io';

import 'package:ffi/ffi.dart';

final class DdbApi {
  DdbApi(this.library)
      : lastError = library.lookupFunction<_last_error_native, _last_error_dart>('ddb_last_error_message'),
        openOrCreate = library.lookupFunction<_open_native, _open_dart>('ddb_db_open_or_create'),
        execute = library.lookupFunction<_execute_native, _execute_dart>('ddb_db_execute'),
        resultFree = library.lookupFunction<_result_free_native, _result_free_dart>('ddb_result_free'),
        rowCount = library.lookupFunction<_row_count_native, _row_count_dart>('ddb_result_row_count'),
        dbFree = library.lookupFunction<_db_free_native, _db_free_dart>('ddb_db_free');

  final DynamicLibrary library;
  final _last_error_dart lastError;
  final _open_dart openOrCreate;
  final _execute_dart execute;
  final _result_free_dart resultFree;
  final _row_count_dart rowCount;
  final _db_free_dart dbFree;
}

typedef _last_error_native = Pointer<Utf8> Function();
typedef _last_error_dart = Pointer<Utf8> Function();
typedef _open_native = Uint32 Function(Pointer<Utf8>, Pointer<Pointer<Void>>);
typedef _open_dart = int Function(Pointer<Utf8>, Pointer<Pointer<Void>>);
typedef _execute_native = Uint32 Function(
  Pointer<Void>,
  Pointer<Utf8>,
  Pointer<Void>,
  IntPtr,
  Pointer<Pointer<Void>>,
);
typedef _execute_dart = int Function(
  Pointer<Void>,
  Pointer<Utf8>,
  Pointer<Void>,
  int,
  Pointer<Pointer<Void>>,
);
typedef _result_free_native = Uint32 Function(Pointer<Pointer<Void>>);
typedef _result_free_dart = int Function(Pointer<Pointer<Void>>);
typedef _row_count_native = Uint32 Function(Pointer<Void>, Pointer<IntPtr>);
typedef _row_count_dart = int Function(Pointer<Void>, Pointer<IntPtr>);
typedef _db_free_native = Uint32 Function(Pointer<Pointer<Void>>);
typedef _db_free_dart = int Function(Pointer<Pointer<Void>>);

const ddbOk = 0;
const ddbErrSql = 5;

void main() {
  final root = File(Platform.script.toFilePath()).parent.parent.parent.parent.path;
  final library = DynamicLibrary.open('$root/target/debug/libdecentdb.so');
  final api = DdbApi(library);

  final dbSlot = calloc<Pointer<Void>>();
  final resultSlot = calloc<Pointer<Void>>();
  final rows = calloc<IntPtr>();
  try {
    check(api, api.openOrCreate(':memory:'.toNativeUtf8(), dbSlot), 'open_or_create');
    exec(api, dbSlot.value, resultSlot, 'CREATE TABLE smoke (id INT64 PRIMARY KEY, name TEXT)');
    freeResult(api, resultSlot);
    exec(api, dbSlot.value, resultSlot, "INSERT INTO smoke (id, name) VALUES (1, 'dart-smoke')");
    freeResult(api, resultSlot);
    exec(api, dbSlot.value, resultSlot, 'SELECT id, name FROM smoke');
    check(api, api.rowCount(resultSlot.value, rows), 'row count');
    if (rows.value != 1) {
      throw StateError('expected 1 row, got ${rows.value}');
    }
    freeResult(api, resultSlot);

    final status = api.execute(
      dbSlot.value,
      'SELECT * FROM nope'.toNativeUtf8(),
      nullptr,
      0,
      resultSlot,
    );
    if (status != ddbErrSql) {
      throw StateError('expected SQL error, got $status');
    }
    final error = api.lastError().toDartString();
    if (!error.contains('nope')) {
      throw StateError('unexpected error message: $error');
    }

    check(api, api.dbFree(dbSlot), 'free db');
  } finally {
    calloc.free(rows);
    calloc.free(resultSlot);
    calloc.free(dbSlot);
  }
}

void exec(DdbApi api, Pointer<Void> db, Pointer<Pointer<Void>> resultSlot, String sql) {
  final nativeSql = sql.toNativeUtf8();
  try {
    check(api, api.execute(db, nativeSql, nullptr, 0, resultSlot), sql);
  } finally {
    malloc.free(nativeSql);
  }
}

void freeResult(DdbApi api, Pointer<Pointer<Void>> resultSlot) {
  check(api, api.resultFree(resultSlot), 'free result');
}

void check(DdbApi api, int status, String context) {
  if (status != ddbOk) {
    final ptr = api.lastError();
    final error = ptr == nullptr ? '' : ptr.toDartString();
    throw StateError('$context failed with status $status: $error');
  }
}
