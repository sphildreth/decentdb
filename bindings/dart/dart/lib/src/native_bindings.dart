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

typedef _DbOpenOrCreateC = Uint32 Function(
  Pointer<Utf8> path,
  Pointer<Pointer<DdbDb>> outDb,
);
typedef _DbOpenOrCreateDart = int Function(
  Pointer<Utf8> path,
  Pointer<Pointer<DdbDb>> outDb,
);

typedef _DbFreeC = Uint32 Function(Pointer<Pointer<DdbDb>> db);
typedef _DbFreeDart = int Function(Pointer<Pointer<DdbDb>> db);

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

typedef _DbCheckpointC = Uint32 Function(Pointer<DdbDb> db);
typedef _DbCheckpointDart = int Function(Pointer<DdbDb> db);

typedef _DbBeginTransactionC = Uint32 Function(Pointer<DdbDb> db);
typedef _DbBeginTransactionDart = int Function(Pointer<DdbDb> db);

typedef _DbCommitTransactionC = Uint32 Function(
  Pointer<DdbDb> db,
  Pointer<Uint64> outLsn,
);
typedef _DbCommitTransactionDart = int Function(
  Pointer<DdbDb> db,
  Pointer<Uint64> outLsn,
);

typedef _DbRollbackTransactionC = Uint32 Function(Pointer<DdbDb> db);
typedef _DbRollbackTransactionDart = int Function(Pointer<DdbDb> db);

typedef _DbSaveAsC = Uint32 Function(Pointer<DdbDb> db, Pointer<Utf8> destPath);
typedef _DbSaveAsDart = int Function(Pointer<DdbDb> db, Pointer<Utf8> destPath);

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

typedef _ResultFreeC = Uint32 Function(Pointer<Pointer<DdbResult>> result);
typedef _ResultFreeDart = int Function(Pointer<Pointer<DdbResult>> result);

typedef _ResultRowCountC = Uint32 Function(
    Pointer<DdbResult> result, Pointer<IntPtr> outRows);
typedef _ResultRowCountDart = int Function(
    Pointer<DdbResult> result, Pointer<IntPtr> outRows);

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

class NativeBindings {
  NativeBindings._(this._lib)
      : abiVersion = _lib
            .lookupFunction<_AbiVersionC, _AbiVersionDart>('ddb_abi_version'),
        version = _lib.lookupFunction<_VersionC, _VersionDart>('ddb_version'),
        lastErrorMessage =
            _lib.lookupFunction<_LastErrorMessageC, _LastErrorMessageDart>(
                'ddb_last_error_message'),
        valueDispose = _lib.lookupFunction<_ValueDisposeC, _ValueDisposeDart>(
            'ddb_value_dispose'),
        stringFree = _lib
            .lookupFunction<_StringFreeC, _StringFreeDart>('ddb_string_free'),
        dbOpenOrCreate =
            _lib.lookupFunction<_DbOpenOrCreateC, _DbOpenOrCreateDart>(
                'ddb_db_open_or_create'),
        dbFree = _lib.lookupFunction<_DbFreeC, _DbFreeDart>('ddb_db_free'),
        dbExecute =
            _lib.lookupFunction<_DbExecuteC, _DbExecuteDart>('ddb_db_execute'),
        dbCheckpoint = _lib.lookupFunction<_DbCheckpointC, _DbCheckpointDart>(
            'ddb_db_checkpoint'),
        dbBeginTransaction =
            _lib.lookupFunction<_DbBeginTransactionC, _DbBeginTransactionDart>(
                'ddb_db_begin_transaction'),
        dbCommitTransaction = _lib.lookupFunction<_DbCommitTransactionC,
            _DbCommitTransactionDart>('ddb_db_commit_transaction'),
        dbRollbackTransaction = _lib.lookupFunction<_DbRollbackTransactionC,
            _DbRollbackTransactionDart>('ddb_db_rollback_transaction'),
        dbSaveAs =
            _lib.lookupFunction<_DbSaveAsC, _DbSaveAsDart>('ddb_db_save_as'),
        dbListTablesJson = _lib.lookupFunction<_DbStringOutC, _DbStringOutDart>(
          'ddb_db_list_tables_json',
        ),
        dbDescribeTableJson =
            _lib.lookupFunction<_DbNamedStringOutC, _DbNamedStringOutDart>(
          'ddb_db_describe_table_json',
        ),
        dbGetTableDdl =
            _lib.lookupFunction<_DbNamedStringOutC, _DbNamedStringOutDart>(
                'ddb_db_get_table_ddl'),
        dbListIndexesJson =
            _lib.lookupFunction<_DbStringOutC, _DbStringOutDart>(
          'ddb_db_list_indexes_json',
        ),
        dbListViewsJson = _lib.lookupFunction<_DbStringOutC, _DbStringOutDart>(
          'ddb_db_list_views_json',
        ),
        dbGetViewDdl =
            _lib.lookupFunction<_DbNamedStringOutC, _DbNamedStringOutDart>(
                'ddb_db_get_view_ddl'),
        dbListTriggersJson =
            _lib.lookupFunction<_DbStringOutC, _DbStringOutDart>(
          'ddb_db_list_triggers_json',
        ),
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
        resultColumnNameCopy = _lib.lookupFunction<_ResultColumnNameCopyC,
            _ResultColumnNameCopyDart>('ddb_result_column_name_copy'),
        resultValueCopy =
            _lib.lookupFunction<_ResultValueCopyC, _ResultValueCopyDart>(
                'ddb_result_value_copy');

  final DynamicLibrary _lib;
  final _AbiVersionDart abiVersion;
  final _VersionDart version;
  final _LastErrorMessageDart lastErrorMessage;
  final _ValueDisposeDart valueDispose;
  final _StringFreeDart stringFree;
  final _DbOpenOrCreateDart dbOpenOrCreate;
  final _DbFreeDart dbFree;
  final _DbExecuteDart dbExecute;
  final _DbCheckpointDart dbCheckpoint;
  final _DbBeginTransactionDart dbBeginTransaction;
  final _DbCommitTransactionDart dbCommitTransaction;
  final _DbRollbackTransactionDart dbRollbackTransaction;
  final _DbSaveAsDart dbSaveAs;
  final _DbStringOutDart dbListTablesJson;
  final _DbNamedStringOutDart dbDescribeTableJson;
  final _DbNamedStringOutDart dbGetTableDdl;
  final _DbStringOutDart dbListIndexesJson;
  final _DbStringOutDart dbListViewsJson;
  final _DbNamedStringOutDart dbGetViewDdl;
  final _DbStringOutDart dbListTriggersJson;
  final _ResultFreeDart resultFree;
  final _ResultRowCountDart resultRowCount;
  final _ResultColumnCountDart resultColumnCount;
  final _ResultAffectedRowsDart resultAffectedRows;
  final _ResultColumnNameCopyDart resultColumnNameCopy;
  final _ResultValueCopyDart resultValueCopy;

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
