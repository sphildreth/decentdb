import 'dart:ffi';
import 'dart:io' show Platform;

import 'package:ffi/ffi.dart';

/// Expected ABI version. Must match `decentdb_abi_version()` from the native
/// library at load time.
const int expectedAbiVersion = 1;

// ---------------------------------------------------------------------------
// Native type definitions
// ---------------------------------------------------------------------------

/// Opaque database handle.
typedef DecentdbDb = Void;

/// Opaque statement handle.
typedef DecentdbStmt = Void;

/// decentdb_value_view struct layout for row_view / step_with_params_row_view.
final class DecentdbValueView extends Struct {
  @Int32()
  external int kind;

  @Int32()
  external int isNull;

  @Int64()
  external int int64Val;

  @Double()
  external double float64Val;

  external Pointer<Uint8> bytes;

  @Int32()
  external int bytesLen;

  @Int32()
  external int decimalScale;
}

// ---------------------------------------------------------------------------
// Native function typedefs (C signatures)
// ---------------------------------------------------------------------------

// ABI version
typedef _AbiVersionC = Int32 Function();
typedef _AbiVersionDart = int Function();

// Engine version
typedef _EngineVersionC = Pointer<Utf8> Function();
typedef _EngineVersionDart = Pointer<Utf8> Function();

// Database lifecycle
typedef _OpenC = Pointer<DecentdbDb> Function(
    Pointer<Utf8> path, Pointer<Utf8> options);
typedef _OpenDart = Pointer<DecentdbDb> Function(
    Pointer<Utf8> path, Pointer<Utf8> options);

typedef _CloseC = Int32 Function(Pointer<DecentdbDb> db);
typedef _CloseDart = int Function(Pointer<DecentdbDb> db);

// Error reporting
typedef _LastErrorCodeC = Int32 Function(Pointer<DecentdbDb> db);
typedef _LastErrorCodeDart = int Function(Pointer<DecentdbDb> db);

typedef _LastErrorMessageC = Pointer<Utf8> Function(Pointer<DecentdbDb> db);
typedef _LastErrorMessageDart = Pointer<Utf8> Function(Pointer<DecentdbDb> db);

// Transaction control
typedef _BeginC = Int32 Function(Pointer<DecentdbDb> db);
typedef _BeginDart = int Function(Pointer<DecentdbDb> db);

typedef _CommitC = Int32 Function(Pointer<DecentdbDb> db);
typedef _CommitDart = int Function(Pointer<DecentdbDb> db);

typedef _RollbackC = Int32 Function(Pointer<DecentdbDb> db);
typedef _RollbackDart = int Function(Pointer<DecentdbDb> db);

// Prepare
typedef _PrepareC = Int32 Function(Pointer<DecentdbDb> db,
    Pointer<Utf8> sql, Pointer<Pointer<DecentdbStmt>> outStmt);
typedef _PrepareDart = int Function(Pointer<DecentdbDb> db,
    Pointer<Utf8> sql, Pointer<Pointer<DecentdbStmt>> outStmt);

// Bind
typedef _BindNullC = Int32 Function(Pointer<DecentdbStmt> stmt, Int32 index);
typedef _BindNullDart = int Function(Pointer<DecentdbStmt> stmt, int index);

typedef _BindInt64C = Int32 Function(
    Pointer<DecentdbStmt> stmt, Int32 index, Int64 v);
typedef _BindInt64Dart = int Function(
    Pointer<DecentdbStmt> stmt, int index, int v);

typedef _BindBoolC = Int32 Function(
    Pointer<DecentdbStmt> stmt, Int32 index, Int32 v);
typedef _BindBoolDart = int Function(
    Pointer<DecentdbStmt> stmt, int index, int v);

typedef _BindFloat64C = Int32 Function(
    Pointer<DecentdbStmt> stmt, Int32 index, Double v);
typedef _BindFloat64Dart = int Function(
    Pointer<DecentdbStmt> stmt, int index, double v);

typedef _BindTextC = Int32 Function(
    Pointer<DecentdbStmt> stmt, Int32 index, Pointer<Utf8> utf8, Int32 len);
typedef _BindTextDart = int Function(
    Pointer<DecentdbStmt> stmt, int index, Pointer<Utf8> utf8, int len);

typedef _BindBlobC = Int32 Function(
    Pointer<DecentdbStmt> stmt, Int32 index, Pointer<Uint8> data, Int32 len);
typedef _BindBlobDart = int Function(
    Pointer<DecentdbStmt> stmt, int index, Pointer<Uint8> data, int len);

typedef _BindDecimalC = Int32 Function(
    Pointer<DecentdbStmt> stmt, Int32 index, Int64 unscaled, Int32 scale);
typedef _BindDecimalDart = int Function(
    Pointer<DecentdbStmt> stmt, int index, int unscaled, int scale);

typedef _BindDateTimeC = Int32 Function(
    Pointer<DecentdbStmt> stmt, Int32 index, Int64 microsUtc);
typedef _BindDateTimeDart = int Function(
    Pointer<DecentdbStmt> stmt, int index, int microsUtc);

// Step / Reset / Clear / Finalize
typedef _StepC = Int32 Function(Pointer<DecentdbStmt> stmt);
typedef _StepDart = int Function(Pointer<DecentdbStmt> stmt);

typedef _ResetC = Int32 Function(Pointer<DecentdbStmt> stmt);
typedef _ResetDart = int Function(Pointer<DecentdbStmt> stmt);

typedef _ClearBindingsC = Int32 Function(Pointer<DecentdbStmt> stmt);
typedef _ClearBindingsDart = int Function(Pointer<DecentdbStmt> stmt);

typedef _FinalizeC = Void Function(Pointer<DecentdbStmt> stmt);
typedef _FinalizeDart = void Function(Pointer<DecentdbStmt> stmt);

// Column metadata
typedef _ColumnCountC = Int32 Function(Pointer<DecentdbStmt> stmt);
typedef _ColumnCountDart = int Function(Pointer<DecentdbStmt> stmt);

typedef _ColumnNameC = Pointer<Utf8> Function(
    Pointer<DecentdbStmt> stmt, Int32 col);
typedef _ColumnNameDart = Pointer<Utf8> Function(
    Pointer<DecentdbStmt> stmt, int col);

typedef _ColumnTypeC = Int32 Function(Pointer<DecentdbStmt> stmt, Int32 col);
typedef _ColumnTypeDart = int Function(Pointer<DecentdbStmt> stmt, int col);

// Column accessors
typedef _ColumnIsNullC = Int32 Function(Pointer<DecentdbStmt> stmt, Int32 col);
typedef _ColumnIsNullDart = int Function(Pointer<DecentdbStmt> stmt, int col);

typedef _ColumnInt64C = Int64 Function(Pointer<DecentdbStmt> stmt, Int32 col);
typedef _ColumnInt64Dart = int Function(Pointer<DecentdbStmt> stmt, int col);

typedef _ColumnFloat64C = Double Function(Pointer<DecentdbStmt> stmt, Int32 col);
typedef _ColumnFloat64Dart = double Function(Pointer<DecentdbStmt> stmt, int col);

typedef _ColumnTextC = Pointer<Utf8> Function(
    Pointer<DecentdbStmt> stmt, Int32 col, Pointer<Int32> outLen);
typedef _ColumnTextDart = Pointer<Utf8> Function(
    Pointer<DecentdbStmt> stmt, int col, Pointer<Int32> outLen);

typedef _ColumnBlobC = Pointer<Uint8> Function(
    Pointer<DecentdbStmt> stmt, Int32 col, Pointer<Int32> outLen);
typedef _ColumnBlobDart = Pointer<Uint8> Function(
    Pointer<DecentdbStmt> stmt, int col, Pointer<Int32> outLen);

typedef _ColumnDecimalUnscaledC = Int64 Function(
    Pointer<DecentdbStmt> stmt, Int32 col);
typedef _ColumnDecimalUnscaledDart = int Function(
    Pointer<DecentdbStmt> stmt, int col);

typedef _ColumnDecimalScaleC = Int32 Function(
    Pointer<DecentdbStmt> stmt, Int32 col);
typedef _ColumnDecimalScaleDart = int Function(
    Pointer<DecentdbStmt> stmt, int col);

typedef _ColumnDateTimeC = Int64 Function(
    Pointer<DecentdbStmt> stmt, Int32 col);
typedef _ColumnDateTimeDart = int Function(Pointer<DecentdbStmt> stmt, int col);

// Row view
typedef _RowViewC = Int32 Function(Pointer<DecentdbStmt> stmt,
    Pointer<Pointer<DecentdbValueView>> outValues, Pointer<Int32> outCount);
typedef _RowViewDart = int Function(Pointer<DecentdbStmt> stmt,
    Pointer<Pointer<DecentdbValueView>> outValues, Pointer<Int32> outCount);

// Rows affected
typedef _RowsAffectedC = Int64 Function(Pointer<DecentdbStmt> stmt);
typedef _RowsAffectedDart = int Function(Pointer<DecentdbStmt> stmt);

// Maintenance
typedef _CheckpointC = Int32 Function(Pointer<DecentdbDb> db);
typedef _CheckpointDart = int Function(Pointer<DecentdbDb> db);

typedef _SaveAsC = Int32 Function(
    Pointer<DecentdbDb> db, Pointer<Utf8> destPath);
typedef _SaveAsDart = int Function(
    Pointer<DecentdbDb> db, Pointer<Utf8> destPath);

// Memory
typedef _FreeC = Void Function(Pointer<Void> p);
typedef _FreeDart = void Function(Pointer<Void> p);

// Schema introspection
typedef _ListTablesJsonC = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Int32> outLen);
typedef _ListTablesJsonDart = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Int32> outLen);

typedef _ListTablesInfoJsonC = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Int32> outLen);
typedef _ListTablesInfoJsonDart = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Int32> outLen);

typedef _GetTableColumnsJsonC = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Utf8> table, Pointer<Int32> outLen);
typedef _GetTableColumnsJsonDart = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Utf8> table, Pointer<Int32> outLen);

typedef _GetTableDdlC = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Utf8> table, Pointer<Int32> outLen);
typedef _GetTableDdlDart = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Utf8> table, Pointer<Int32> outLen);

typedef _ListIndexesJsonC = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Int32> outLen);
typedef _ListIndexesJsonDart = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Int32> outLen);

typedef _ListViewsJsonC = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Int32> outLen);
typedef _ListViewsJsonDart = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Int32> outLen);

typedef _ListViewsInfoJsonC = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Int32> outLen);
typedef _ListViewsInfoJsonDart = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Int32> outLen);

typedef _GetViewDdlC = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Utf8> view, Pointer<Int32> outLen);
typedef _GetViewDdlDart = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Utf8> view, Pointer<Int32> outLen);

typedef _ListTriggersJsonC = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Int32> outLen);
typedef _ListTriggersJsonDart = Pointer<Utf8> Function(
    Pointer<DecentdbDb> db, Pointer<Int32> outLen);

// ---------------------------------------------------------------------------
// Native bindings holder
// ---------------------------------------------------------------------------

/// Low-level FFI bindings to the DecentDB native library.
///
/// Use [NativeBindings.open] to load the library and resolve all symbols.
/// Most users should prefer the high-level [Database] API instead.
class NativeBindings {
  final DynamicLibrary _lib;

  // ABI
  late final _AbiVersionDart abiVersion;
  late final _EngineVersionDart engineVersion;

  // Lifecycle
  late final _OpenDart open;
  late final _CloseDart close;

  // Error
  late final _LastErrorCodeDart lastErrorCode;
  late final _LastErrorMessageDart lastErrorMessage;

  // Transactions
  late final _BeginDart begin;
  late final _CommitDart commit;
  late final _RollbackDart rollback;

  // Statements
  late final _PrepareDart prepare;
  late final _BindNullDart bindNull;
  late final _BindInt64Dart bindInt64;
  late final _BindBoolDart bindBool;
  late final _BindFloat64Dart bindFloat64;
  late final _BindTextDart bindText;
  late final _BindBlobDart bindBlob;
  late final _BindDecimalDart bindDecimal;
  late final _BindDateTimeDart bindDateTime;
  late final _StepDart step;
  late final _ResetDart reset;
  late final _ClearBindingsDart clearBindings;
  late final _FinalizeDart finalize;

  // Columns
  late final _ColumnCountDart columnCount;
  late final _ColumnNameDart columnName;
  late final _ColumnTypeDart columnType;
  late final _ColumnIsNullDart columnIsNull;
  late final _ColumnInt64Dart columnInt64;
  late final _ColumnFloat64Dart columnFloat64;
  late final _ColumnTextDart columnText;
  late final _ColumnBlobDart columnBlob;
  late final _ColumnDecimalUnscaledDart columnDecimalUnscaled;
  late final _ColumnDecimalScaleDart columnDecimalScale;
  late final _ColumnDateTimeDart columnDateTime;

  // Row view
  late final _RowViewDart rowView;

  // Results
  late final _RowsAffectedDart rowsAffected;

  // Maintenance
  late final _CheckpointDart checkpoint;
  late final _SaveAsDart saveAs;

  // Memory
  late final _FreeDart free;

  // Schema
  late final _ListTablesJsonDart listTablesJson;
  late final _ListTablesInfoJsonDart listTablesInfoJson;
  late final _GetTableColumnsJsonDart getTableColumnsJson;
  late final _GetTableDdlDart getTableDdl;
  late final _ListIndexesJsonDart listIndexesJson;
  late final _ListViewsJsonDart listViewsJson;
  late final _ListViewsInfoJsonDart listViewsInfoJson;
  late final _GetViewDdlDart getViewDdl;
  late final _ListTriggersJsonDart listTriggersJson;

  NativeBindings._(this._lib) {
    abiVersion =
        _lib.lookupFunction<_AbiVersionC, _AbiVersionDart>('decentdb_abi_version');
    engineVersion =
        _lib.lookupFunction<_EngineVersionC, _EngineVersionDart>('decentdb_engine_version');

    open = _lib.lookupFunction<_OpenC, _OpenDart>('decentdb_open');
    close = _lib.lookupFunction<_CloseC, _CloseDart>('decentdb_close');

    lastErrorCode =
        _lib.lookupFunction<_LastErrorCodeC, _LastErrorCodeDart>('decentdb_last_error_code');
    lastErrorMessage =
        _lib.lookupFunction<_LastErrorMessageC, _LastErrorMessageDart>('decentdb_last_error_message');

    begin = _lib.lookupFunction<_BeginC, _BeginDart>('decentdb_begin');
    commit = _lib.lookupFunction<_CommitC, _CommitDart>('decentdb_commit');
    rollback = _lib.lookupFunction<_RollbackC, _RollbackDart>('decentdb_rollback');

    prepare = _lib.lookupFunction<_PrepareC, _PrepareDart>('decentdb_prepare');
    bindNull =
        _lib.lookupFunction<_BindNullC, _BindNullDart>('decentdb_bind_null');
    bindInt64 =
        _lib.lookupFunction<_BindInt64C, _BindInt64Dart>('decentdb_bind_int64');
    bindBool =
        _lib.lookupFunction<_BindBoolC, _BindBoolDart>('decentdb_bind_bool');
    bindFloat64 =
        _lib.lookupFunction<_BindFloat64C, _BindFloat64Dart>('decentdb_bind_float64');
    bindText =
        _lib.lookupFunction<_BindTextC, _BindTextDart>('decentdb_bind_text');
    bindBlob =
        _lib.lookupFunction<_BindBlobC, _BindBlobDart>('decentdb_bind_blob');
    bindDecimal =
        _lib.lookupFunction<_BindDecimalC, _BindDecimalDart>('decentdb_bind_decimal');
    bindDateTime =
        _lib.lookupFunction<_BindDateTimeC, _BindDateTimeDart>('decentdb_bind_datetime');

    step = _lib.lookupFunction<_StepC, _StepDart>('decentdb_step');
    reset = _lib.lookupFunction<_ResetC, _ResetDart>('decentdb_reset');
    clearBindings =
        _lib.lookupFunction<_ClearBindingsC, _ClearBindingsDart>('decentdb_clear_bindings');
    finalize =
        _lib.lookupFunction<_FinalizeC, _FinalizeDart>('decentdb_finalize');

    columnCount =
        _lib.lookupFunction<_ColumnCountC, _ColumnCountDart>('decentdb_column_count');
    columnName =
        _lib.lookupFunction<_ColumnNameC, _ColumnNameDart>('decentdb_column_name');
    columnType =
        _lib.lookupFunction<_ColumnTypeC, _ColumnTypeDart>('decentdb_column_type');
    columnIsNull =
        _lib.lookupFunction<_ColumnIsNullC, _ColumnIsNullDart>('decentdb_column_is_null');
    columnInt64 =
        _lib.lookupFunction<_ColumnInt64C, _ColumnInt64Dart>('decentdb_column_int64');
    columnFloat64 =
        _lib.lookupFunction<_ColumnFloat64C, _ColumnFloat64Dart>('decentdb_column_float64');
    columnText =
        _lib.lookupFunction<_ColumnTextC, _ColumnTextDart>('decentdb_column_text');
    columnBlob =
        _lib.lookupFunction<_ColumnBlobC, _ColumnBlobDart>('decentdb_column_blob');
    columnDecimalUnscaled =
        _lib.lookupFunction<_ColumnDecimalUnscaledC, _ColumnDecimalUnscaledDart>('decentdb_column_decimal_unscaled');
    columnDecimalScale =
        _lib.lookupFunction<_ColumnDecimalScaleC, _ColumnDecimalScaleDart>('decentdb_column_decimal_scale');
    columnDateTime =
        _lib.lookupFunction<_ColumnDateTimeC, _ColumnDateTimeDart>('decentdb_column_datetime');

    rowView =
        _lib.lookupFunction<_RowViewC, _RowViewDart>('decentdb_row_view');
    rowsAffected =
        _lib.lookupFunction<_RowsAffectedC, _RowsAffectedDart>('decentdb_rows_affected');

    checkpoint =
        _lib.lookupFunction<_CheckpointC, _CheckpointDart>('decentdb_checkpoint');
    saveAs = _lib.lookupFunction<_SaveAsC, _SaveAsDart>('decentdb_save_as');

    free = _lib.lookupFunction<_FreeC, _FreeDart>('decentdb_free');

    listTablesJson =
        _lib.lookupFunction<_ListTablesJsonC, _ListTablesJsonDart>('decentdb_list_tables_json');
    listTablesInfoJson =
        _lib.lookupFunction<_ListTablesInfoJsonC, _ListTablesInfoJsonDart>('decentdb_list_tables_info_json');
    getTableColumnsJson =
        _lib.lookupFunction<_GetTableColumnsJsonC, _GetTableColumnsJsonDart>('decentdb_get_table_columns_json');
    getTableDdl =
        _lib.lookupFunction<_GetTableDdlC, _GetTableDdlDart>('decentdb_get_table_ddl');
    listIndexesJson =
        _lib.lookupFunction<_ListIndexesJsonC, _ListIndexesJsonDart>('decentdb_list_indexes_json');
    listViewsJson =
        _lib.lookupFunction<_ListViewsJsonC, _ListViewsJsonDart>('decentdb_list_views_json');
    listViewsInfoJson =
        _lib.lookupFunction<_ListViewsInfoJsonC, _ListViewsInfoJsonDart>('decentdb_list_views_info_json');
    getViewDdl =
        _lib.lookupFunction<_GetViewDdlC, _GetViewDdlDart>('decentdb_get_view_ddl');
    listTriggersJson =
        _lib.lookupFunction<_ListTriggersJsonC, _ListTriggersJsonDart>('decentdb_list_triggers_json');
  }

  /// Load the native library from [path] and resolve all function symbols.
  ///
  /// Throws [DecentDbException] if the ABI version does not match.
  static NativeBindings load(String path) {
    final lib = DynamicLibrary.open(path);
    final bindings = NativeBindings._(lib);

    final version = bindings.abiVersion();
    if (version != expectedAbiVersion) {
      throw StateError(
        'DecentDB ABI version mismatch: expected $expectedAbiVersion, got $version. '
        'Update native library or Dart package.',
      );
    }
    return bindings;
  }

  /// Resolve the default library name for the current platform.
  static String defaultLibraryName() {
    if (Platform.isLinux) return 'libc_api.so';
    if (Platform.isMacOS) return 'libc_api.dylib';
    if (Platform.isWindows) return 'c_api.dll';
    throw UnsupportedError('Unsupported platform: ${Platform.operatingSystem}');
  }
}
