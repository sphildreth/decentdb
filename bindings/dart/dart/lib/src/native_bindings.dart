import 'dart:ffi';
import 'dart:io' show Platform;

import 'package:ffi/ffi.dart';

const int expectedAbiVersion = 1;
const int ddbOk = 0;
const int ddbTagNull = 0;
const int ddbTagInt64 = 1;
const int ddbTagFloat64 = 2;
const int ddbTagBool = 3;
const int ddbTagText = 4;
const int ddbTagBlob = 5;
const int ddbTagDecimal = 6;
const int ddbTagUuid = 7;
const int ddbTagTimestampMicros = 8;

final class DdbDb extends Opaque {}

final class DdbResult extends Opaque {}

/// Opaque handle for a native prepared statement (ddb_stmt_t).
final class DdbStmt extends Opaque {}

final class DdbValue extends Struct {
  @Uint32()
  external int tag;

  @Uint8()
  external int boolValue;

  @Array(7)
  external Array<Uint8> reserved0;

  @Int64()
  external int int64Value;

  @Double()
  external double float64Value;

  @Int64()
  external int decimalScaled;

  @Uint8()
  external int decimalScale;

  @Array(7)
  external Array<Uint8> reserved1;

  external Pointer<Uint8> data;

  @IntPtr()
  external int len;

  @Array(16)
  external Array<Uint8> uuidBytes;

  @Int64()
  external int timestampMicros;
}

// ---------------------------------------------------------------------------
// Global / utility
// ---------------------------------------------------------------------------

typedef _AbiVersionC = Uint32 Function();
typedef _AbiVersionDart = int Function();

typedef _VersionC = Pointer<Utf8> Function();
typedef _VersionDart = Pointer<Utf8> Function();

typedef _LastErrorMessageC = Pointer<Utf8> Function();
typedef _LastErrorMessageDart = Pointer<Utf8> Function();

typedef _ValueDisposeC = Uint32 Function(Pointer<DdbValue> value);
typedef _ValueDisposeDart = int Function(Pointer<DdbValue> value);

typedef _StringFreeC = Uint32 Function(Pointer<Pointer<Utf8>> value);
typedef _StringFreeDart = int Function(Pointer<Pointer<Utf8>> value);

// ---------------------------------------------------------------------------
// Database open / close
// ---------------------------------------------------------------------------

typedef _DbPathOutC = Uint32 Function(
  Pointer<Utf8> path,
  Pointer<Pointer<DdbDb>> outDb,
);
typedef _DbPathOutDart = int Function(
  Pointer<Utf8> path,
  Pointer<Pointer<DdbDb>> outDb,
);

typedef _DbFreeC = Uint32 Function(Pointer<Pointer<DdbDb>> db);
typedef _DbFreeDart = int Function(Pointer<Pointer<DdbDb>> db);

// ---------------------------------------------------------------------------
// Database ops
// ---------------------------------------------------------------------------

typedef _DbExecuteC = Uint32 Function(
  Pointer<DdbDb> db,
  Pointer<Utf8> sql,
  Pointer<DdbValue> params,
  IntPtr paramsLen,
  Pointer<Pointer<DdbResult>> outResult,
);
typedef _DbExecuteDart = int Function(
  Pointer<DdbDb> db,
  Pointer<Utf8> sql,
  Pointer<DdbValue> params,
  int paramsLen,
  Pointer<Pointer<DdbResult>> outResult,
);

typedef _DbPrepareC = Uint32 Function(
  Pointer<DdbDb> db,
  Pointer<Utf8> sql,
  Pointer<Pointer<DdbStmt>> outStmt,
);
typedef _DbPrepareDart = int Function(
  Pointer<DdbDb> db,
  Pointer<Utf8> sql,
  Pointer<Pointer<DdbStmt>> outStmt,
);

typedef _DbSimpleC = Uint32 Function(Pointer<DdbDb> db);
typedef _DbSimpleDart = int Function(Pointer<DdbDb> db);

typedef _DbCommitTransactionC = Uint32 Function(
  Pointer<DdbDb> db,
  Pointer<Uint64> outLsn,
);
typedef _DbCommitTransactionDart = int Function(
  Pointer<DdbDb> db,
  Pointer<Uint64> outLsn,
);

typedef _DbInTransactionC = Uint32 Function(
  Pointer<DdbDb> db,
  Pointer<Uint8> outFlag,
);
typedef _DbInTransactionDart = int Function(
  Pointer<DdbDb> db,
  Pointer<Uint8> outFlag,
);

typedef _DbSaveAsC = Uint32 Function(
  Pointer<DdbDb> db,
  Pointer<Utf8> destPath,
);
typedef _DbSaveAsDart = int Function(
  Pointer<DdbDb> db,
  Pointer<Utf8> destPath,
);

typedef _DbStringOutC = Uint32 Function(
  Pointer<DdbDb> db,
  Pointer<Pointer<Utf8>> outValue,
);
typedef _DbStringOutDart = int Function(
  Pointer<DdbDb> db,
  Pointer<Pointer<Utf8>> outValue,
);

typedef _DbNamedStringOutC = Uint32 Function(
  Pointer<DdbDb> db,
  Pointer<Utf8> name,
  Pointer<Pointer<Utf8>> outValue,
);
typedef _DbNamedStringOutDart = int Function(
  Pointer<DdbDb> db,
  Pointer<Utf8> name,
  Pointer<Pointer<Utf8>> outValue,
);

// ---------------------------------------------------------------------------
// Result accessors (ddb_db_execute result path)
// ---------------------------------------------------------------------------

typedef _ResultFreeC = Uint32 Function(Pointer<Pointer<DdbResult>> result);
typedef _ResultFreeDart = int Function(Pointer<Pointer<DdbResult>> result);

typedef _ResultRowCountC = Uint32 Function(
  Pointer<DdbResult> result,
  Pointer<IntPtr> outRows,
);
typedef _ResultRowCountDart = int Function(
  Pointer<DdbResult> result,
  Pointer<IntPtr> outRows,
);

typedef _ResultColumnCountC = Uint32 Function(
  Pointer<DdbResult> result,
  Pointer<IntPtr> outColumns,
);
typedef _ResultColumnCountDart = int Function(
  Pointer<DdbResult> result,
  Pointer<IntPtr> outColumns,
);

typedef _ResultAffectedRowsC = Uint32 Function(
  Pointer<DdbResult> result,
  Pointer<Uint64> outRows,
);
typedef _ResultAffectedRowsDart = int Function(
  Pointer<DdbResult> result,
  Pointer<Uint64> outRows,
);

typedef _ResultColumnNameCopyC = Uint32 Function(
  Pointer<DdbResult> result,
  IntPtr columnIndex,
  Pointer<Pointer<Utf8>> outName,
);
typedef _ResultColumnNameCopyDart = int Function(
  Pointer<DdbResult> result,
  int columnIndex,
  Pointer<Pointer<Utf8>> outName,
);

typedef _ResultValueCopyC = Uint32 Function(
  Pointer<DdbResult> result,
  IntPtr rowIndex,
  IntPtr columnIndex,
  Pointer<DdbValue> outValue,
);
typedef _ResultValueCopyDart = int Function(
  Pointer<DdbResult> result,
  int rowIndex,
  int columnIndex,
  Pointer<DdbValue> outValue,
);

// ---------------------------------------------------------------------------
// Native prepared statement API (ddb_stmt_t)
// ---------------------------------------------------------------------------

typedef _StmtFreeC = Uint32 Function(Pointer<Pointer<DdbStmt>> stmt);
typedef _StmtFreeDart = int Function(Pointer<Pointer<DdbStmt>> stmt);

typedef _StmtSimpleC = Uint32 Function(Pointer<DdbStmt> stmt);
typedef _StmtSimpleDart = int Function(Pointer<DdbStmt> stmt);

typedef _StmtBindNullC = Uint32 Function(
  Pointer<DdbStmt> stmt,
  IntPtr index,
);
typedef _StmtBindNullDart = int Function(
  Pointer<DdbStmt> stmt,
  int index,
);

typedef _StmtBindInt64C = Uint32 Function(
  Pointer<DdbStmt> stmt,
  IntPtr index,
  Int64 value,
);
typedef _StmtBindInt64Dart = int Function(
  Pointer<DdbStmt> stmt,
  int index,
  int value,
);

typedef _StmtBindFloat64C = Uint32 Function(
  Pointer<DdbStmt> stmt,
  IntPtr index,
  Double value,
);
typedef _StmtBindFloat64Dart = int Function(
  Pointer<DdbStmt> stmt,
  int index,
  double value,
);

typedef _StmtBindBoolC = Uint32 Function(
  Pointer<DdbStmt> stmt,
  IntPtr index,
  Uint8 value,
);
typedef _StmtBindBoolDart = int Function(
  Pointer<DdbStmt> stmt,
  int index,
  int value,
);

typedef _StmtBindTextC = Uint32 Function(
  Pointer<DdbStmt> stmt,
  IntPtr index,
  Pointer<Uint8> value,
  IntPtr byteLen,
);
typedef _StmtBindTextDart = int Function(
  Pointer<DdbStmt> stmt,
  int index,
  Pointer<Uint8> value,
  int byteLen,
);

typedef _StmtBindBlobC = Uint32 Function(
  Pointer<DdbStmt> stmt,
  IntPtr index,
  Pointer<Uint8> data,
  IntPtr byteLen,
);
typedef _StmtBindBlobDart = int Function(
  Pointer<DdbStmt> stmt,
  int index,
  Pointer<Uint8> data,
  int byteLen,
);

typedef _StmtBindDecimalC = Uint32 Function(
  Pointer<DdbStmt> stmt,
  IntPtr index,
  Int64 scaled,
  Uint8 scale,
);
typedef _StmtBindDecimalDart = int Function(
  Pointer<DdbStmt> stmt,
  int index,
  int scaled,
  int scale,
);

typedef _StmtBindTimestampMicrosC = Uint32 Function(
  Pointer<DdbStmt> stmt,
  IntPtr index,
  Int64 timestampMicros,
);
typedef _StmtBindTimestampMicrosDart = int Function(
  Pointer<DdbStmt> stmt,
  int index,
  int timestampMicros,
);

typedef _StmtStepC = Uint32 Function(
  Pointer<DdbStmt> stmt,
  Pointer<Uint8> outHasRow,
);
typedef _StmtStepDart = int Function(
  Pointer<DdbStmt> stmt,
  Pointer<Uint8> outHasRow,
);

typedef _StmtColumnCountC = Uint32 Function(
  Pointer<DdbStmt> stmt,
  Pointer<IntPtr> outColumns,
);
typedef _StmtColumnCountDart = int Function(
  Pointer<DdbStmt> stmt,
  Pointer<IntPtr> outColumns,
);

typedef _StmtColumnNameCopyC = Uint32 Function(
  Pointer<DdbStmt> stmt,
  IntPtr columnIndex,
  Pointer<Pointer<Utf8>> outName,
);
typedef _StmtColumnNameCopyDart = int Function(
  Pointer<DdbStmt> stmt,
  int columnIndex,
  Pointer<Pointer<Utf8>> outName,
);

typedef _StmtAffectedRowsC = Uint32 Function(
  Pointer<DdbStmt> stmt,
  Pointer<Uint64> outRows,
);
typedef _StmtAffectedRowsDart = int Function(
  Pointer<DdbStmt> stmt,
  Pointer<Uint64> outRows,
);

typedef _StmtValueCopyC = Uint32 Function(
  Pointer<DdbStmt> stmt,
  IntPtr columnIndex,
  Pointer<DdbValue> outValue,
);
typedef _StmtValueCopyDart = int Function(
  Pointer<DdbStmt> stmt,
  int columnIndex,
  Pointer<DdbValue> outValue,
);

// ---------------------------------------------------------------------------
// NativeBindings
// ---------------------------------------------------------------------------

class NativeBindings {
  NativeBindings._(this._lib)
      : abiVersion =
            _lib.lookupFunction<_AbiVersionC, _AbiVersionDart>('ddb_abi_version'),
        version = _lib.lookupFunction<_VersionC, _VersionDart>('ddb_version'),
        lastErrorMessage = _lib
            .lookupFunction<_LastErrorMessageC, _LastErrorMessageDart>(
                'ddb_last_error_message'),
        valueDispose = _lib.lookupFunction<_ValueDisposeC, _ValueDisposeDart>(
            'ddb_value_dispose'),
        stringFree = _lib
            .lookupFunction<_StringFreeC, _StringFreeDart>('ddb_string_free'),
        dbCreate = _lib.lookupFunction<_DbPathOutC, _DbPathOutDart>(
            'ddb_db_create'),
        dbOpen =
            _lib.lookupFunction<_DbPathOutC, _DbPathOutDart>('ddb_db_open'),
        dbOpenOrCreate = _lib.lookupFunction<_DbPathOutC, _DbPathOutDart>(
            'ddb_db_open_or_create'),
        dbFree = _lib.lookupFunction<_DbFreeC, _DbFreeDart>('ddb_db_free'),
        dbExecute =
            _lib.lookupFunction<_DbExecuteC, _DbExecuteDart>('ddb_db_execute'),
        dbPrepare =
            _lib.lookupFunction<_DbPrepareC, _DbPrepareDart>('ddb_db_prepare'),
        dbCheckpoint = _lib.lookupFunction<_DbSimpleC, _DbSimpleDart>(
            'ddb_db_checkpoint'),
        dbBeginTransaction = _lib.lookupFunction<_DbSimpleC, _DbSimpleDart>(
            'ddb_db_begin_transaction'),
        dbCommitTransaction = _lib.lookupFunction<_DbCommitTransactionC,
            _DbCommitTransactionDart>('ddb_db_commit_transaction'),
        dbRollbackTransaction = _lib.lookupFunction<_DbSimpleC, _DbSimpleDart>(
            'ddb_db_rollback_transaction'),
        dbInTransaction = _lib.lookupFunction<_DbInTransactionC,
            _DbInTransactionDart>('ddb_db_in_transaction'),
        dbSaveAs =
            _lib.lookupFunction<_DbSaveAsC, _DbSaveAsDart>('ddb_db_save_as'),
        dbListTablesJson = _lib.lookupFunction<_DbStringOutC, _DbStringOutDart>(
            'ddb_db_list_tables_json'),
        dbDescribeTableJson =
            _lib.lookupFunction<_DbNamedStringOutC, _DbNamedStringOutDart>(
                'ddb_db_describe_table_json'),
        dbGetTableDdl =
            _lib.lookupFunction<_DbNamedStringOutC, _DbNamedStringOutDart>(
                'ddb_db_get_table_ddl'),
        dbListIndexesJson =
            _lib.lookupFunction<_DbStringOutC, _DbStringOutDart>(
                'ddb_db_list_indexes_json'),
        dbListViewsJson = _lib.lookupFunction<_DbStringOutC, _DbStringOutDart>(
            'ddb_db_list_views_json'),
        dbGetViewDdl =
            _lib.lookupFunction<_DbNamedStringOutC, _DbNamedStringOutDart>(
                'ddb_db_get_view_ddl'),
        dbListTriggersJson =
            _lib.lookupFunction<_DbStringOutC, _DbStringOutDart>(
                'ddb_db_list_triggers_json'),
        resultFree = _lib
            .lookupFunction<_ResultFreeC, _ResultFreeDart>('ddb_result_free'),
        resultRowCount =
            _lib.lookupFunction<_ResultRowCountC, _ResultRowCountDart>(
                'ddb_result_row_count'),
        resultColumnCount =
            _lib.lookupFunction<_ResultColumnCountC, _ResultColumnCountDart>(
                'ddb_result_column_count'),
        resultAffectedRows =
            _lib.lookupFunction<_ResultAffectedRowsC, _ResultAffectedRowsDart>(
                'ddb_result_affected_rows'),
        resultColumnNameCopy =
            _lib.lookupFunction<_ResultColumnNameCopyC, _ResultColumnNameCopyDart>(
                'ddb_result_column_name_copy'),
        resultValueCopy =
            _lib.lookupFunction<_ResultValueCopyC, _ResultValueCopyDart>(
                'ddb_result_value_copy'),
        stmtFree =
            _lib.lookupFunction<_StmtFreeC, _StmtFreeDart>('ddb_stmt_free'),
        stmtReset = _lib.lookupFunction<_StmtSimpleC, _StmtSimpleDart>(
            'ddb_stmt_reset'),
        stmtClearBindings = _lib.lookupFunction<_StmtSimpleC, _StmtSimpleDart>(
            'ddb_stmt_clear_bindings'),
        stmtBindNull = _lib.lookupFunction<_StmtBindNullC, _StmtBindNullDart>(
            'ddb_stmt_bind_null'),
        stmtBindInt64 =
            _lib.lookupFunction<_StmtBindInt64C, _StmtBindInt64Dart>(
                'ddb_stmt_bind_int64'),
        stmtBindFloat64 =
            _lib.lookupFunction<_StmtBindFloat64C, _StmtBindFloat64Dart>(
                'ddb_stmt_bind_float64'),
        stmtBindBool =
            _lib.lookupFunction<_StmtBindBoolC, _StmtBindBoolDart>(
                'ddb_stmt_bind_bool'),
        stmtBindText =
            _lib.lookupFunction<_StmtBindTextC, _StmtBindTextDart>(
                'ddb_stmt_bind_text'),
        stmtBindBlob =
            _lib.lookupFunction<_StmtBindBlobC, _StmtBindBlobDart>(
                'ddb_stmt_bind_blob'),
        stmtBindDecimal =
            _lib.lookupFunction<_StmtBindDecimalC, _StmtBindDecimalDart>(
                'ddb_stmt_bind_decimal'),
        stmtBindTimestampMicros = _lib.lookupFunction<
                _StmtBindTimestampMicrosC, _StmtBindTimestampMicrosDart>(
            'ddb_stmt_bind_timestamp_micros'),
        stmtStep =
            _lib.lookupFunction<_StmtStepC, _StmtStepDart>('ddb_stmt_step'),
        stmtColumnCount =
            _lib.lookupFunction<_StmtColumnCountC, _StmtColumnCountDart>(
                'ddb_stmt_column_count'),
        stmtColumnNameCopy =
            _lib.lookupFunction<_StmtColumnNameCopyC, _StmtColumnNameCopyDart>(
                'ddb_stmt_column_name_copy'),
        stmtAffectedRows =
            _lib.lookupFunction<_StmtAffectedRowsC, _StmtAffectedRowsDart>(
                'ddb_stmt_affected_rows'),
        stmtValueCopy =
            _lib.lookupFunction<_StmtValueCopyC, _StmtValueCopyDart>(
                'ddb_stmt_value_copy');

  // ignore: unused_field – kept so DynamicLibrary stays live and symbols remain resolved
  final DynamicLibrary _lib;

  // Global
  final _AbiVersionDart abiVersion;
  final _VersionDart version;
  final _LastErrorMessageDart lastErrorMessage;
  final _ValueDisposeDart valueDispose;
  final _StringFreeDart stringFree;

  // DB open/close
  final _DbPathOutDart dbCreate;
  final _DbPathOutDart dbOpen;
  final _DbPathOutDart dbOpenOrCreate;
  final _DbFreeDart dbFree;

  // DB ops
  final _DbExecuteDart dbExecute;
  final _DbPrepareDart dbPrepare;
  final _DbSimpleDart dbCheckpoint;
  final _DbSimpleDart dbBeginTransaction;
  final _DbCommitTransactionDart dbCommitTransaction;
  final _DbSimpleDart dbRollbackTransaction;
  final _DbInTransactionDart dbInTransaction;
  final _DbSaveAsDart dbSaveAs;

  // Schema
  final _DbStringOutDart dbListTablesJson;
  final _DbNamedStringOutDart dbDescribeTableJson;
  final _DbNamedStringOutDart dbGetTableDdl;
  final _DbStringOutDart dbListIndexesJson;
  final _DbStringOutDart dbListViewsJson;
  final _DbNamedStringOutDart dbGetViewDdl;
  final _DbStringOutDart dbListTriggersJson;

  // ddb_db_execute result accessors
  final _ResultFreeDart resultFree;
  final _ResultRowCountDart resultRowCount;
  final _ResultColumnCountDart resultColumnCount;
  final _ResultAffectedRowsDart resultAffectedRows;
  final _ResultColumnNameCopyDart resultColumnNameCopy;
  final _ResultValueCopyDart resultValueCopy;

  // Native prepared statement
  final _StmtFreeDart stmtFree;
  final _StmtSimpleDart stmtReset;
  final _StmtSimpleDart stmtClearBindings;
  final _StmtBindNullDart stmtBindNull;
  final _StmtBindInt64Dart stmtBindInt64;
  final _StmtBindFloat64Dart stmtBindFloat64;
  final _StmtBindBoolDart stmtBindBool;
  final _StmtBindTextDart stmtBindText;
  final _StmtBindBlobDart stmtBindBlob;
  final _StmtBindDecimalDart stmtBindDecimal;
  final _StmtBindTimestampMicrosDart stmtBindTimestampMicros;
  final _StmtStepDart stmtStep;
  final _StmtColumnCountDart stmtColumnCount;
  final _StmtColumnNameCopyDart stmtColumnNameCopy;
  final _StmtAffectedRowsDart stmtAffectedRows;
  final _StmtValueCopyDart stmtValueCopy;

  static NativeBindings load(String path) {
    final bindings = NativeBindings._(DynamicLibrary.open(path));
    final abi = bindings.abiVersion();
    if (abi != expectedAbiVersion) {
      throw StateError(
        'DecentDB ABI version mismatch: expected $expectedAbiVersion, got $abi.',
      );
    }
    return bindings;
  }

  static String defaultLibraryName() {
    if (Platform.isLinux) return 'libdecentdb.so';
    if (Platform.isMacOS) return 'libdecentdb.dylib';
    if (Platform.isWindows) return 'decentdb.dll';
    throw UnsupportedError('Unsupported platform: ${Platform.operatingSystem}');
  }
}
