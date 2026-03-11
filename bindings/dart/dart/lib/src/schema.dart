import 'dart:convert';
import 'dart:ffi';

import 'package:ffi/ffi.dart';

import 'native_bindings.dart';
import 'errors.dart';
import 'types.dart';

/// Schema introspection for a DecentDB database.
///
/// All methods return Dart objects parsed from the JSON payloads returned by the
/// native `decentdb_list_*_json` / `decentdb_get_*_json` functions.
class Schema {
  final NativeBindings _bindings;
  final Pointer<DecentdbDb> _dbPtr;

  /// @nodoc — Internal constructor. Use [Database.schema] instead.
  Schema.fromNative(this._bindings, this._dbPtr);

  void _throwIfError() {
    final errCode = _bindings.lastErrorCode(_dbPtr);
    if (errCode != 0) {
      final msgPtr = _bindings.lastErrorMessage(_dbPtr);
      final msg = msgPtr == nullptr ? 'Unknown error' : msgPtr.toDartString();
      throw DecentDbException(ErrorCode.fromCode(errCode), msg);
    }
  }

  /// Helper: call a native JSON function, parse, free the buffer.
  String _callJsonFunc(
    Pointer<Utf8> Function(Pointer<DecentdbDb>, Pointer<Int32>) fn,
  ) {
    final lenPtr = calloc<Int32>();
    try {
      final ptr = fn(_dbPtr, lenPtr);
      if (ptr == nullptr) {
        _throwIfError();
        return '[]';
      }
      final len = lenPtr.value;
      final result = ptr.toDartString(length: len);
      _bindings.free(ptr.cast<Void>());
      return result;
    } finally {
      calloc.free(lenPtr);
    }
  }

  String _callStringByName(
    String name,
    Pointer<Utf8> Function(
      Pointer<DecentdbDb>,
      Pointer<Utf8>,
      Pointer<Int32>,
    )
    fn,
  ) {
    final namePtr = name.toNativeUtf8();
    final lenPtr = calloc<Int32>();
    try {
      final ptr = fn(_dbPtr, namePtr, lenPtr);
      if (ptr == nullptr) {
        _throwIfError();
        return '';
      }
      final len = lenPtr.value;
      final result = ptr.toDartString(length: len);
      _bindings.free(ptr.cast<Void>());
      return result;
    } finally {
      calloc.free(namePtr);
      calloc.free(lenPtr);
    }
  }

  /// List all table names in the database.
  List<String> listTables() {
    final json = _callJsonFunc(_bindings.listTablesJson);
    return (jsonDecode(json) as List).cast<String>();
  }

  /// List detailed metadata for all tables.
  List<TableInfo> listTablesInfo() {
    final json = _callJsonFunc(_bindings.listTablesInfoJson);
    return (jsonDecode(json) as List)
        .map((e) => TableInfo.fromJson(e as Map<String, dynamic>))
        .toList();
  }

  /// Get column metadata for a table or view.
  List<ColumnInfo> getTableColumns(String tableName) {
    final json = _callStringByName(tableName, _bindings.getTableColumnsJson);
    return (jsonDecode(json) as List)
        .map((e) => ColumnInfo.fromJson(e as Map<String, dynamic>))
        .toList();
  }

  /// Get canonical CREATE TABLE DDL for a table.
  String? getTableDdl(String tableName) {
    final result = _callStringByName(tableName, _bindings.getTableDdl);
    return result.isEmpty ? null : result;
  }

  /// List all indexes in the database.
  List<IndexInfo> listIndexes() {
    final json = _callJsonFunc(_bindings.listIndexesJson);
    return (jsonDecode(json) as List)
        .map((e) => IndexInfo.fromJson(e as Map<String, dynamic>))
        .toList();
  }

  /// List all view names in the database.
  List<String> listViews() {
    final json = _callJsonFunc(_bindings.listViewsJson);
    return (jsonDecode(json) as List).cast<String>();
  }

  /// List detailed metadata for all views.
  List<ViewInfo> listViewsInfo() {
    final json = _callJsonFunc(_bindings.listViewsInfoJson);
    return (jsonDecode(json) as List)
        .map((e) => ViewInfo.fromJson(e as Map<String, dynamic>))
        .toList();
  }

  /// Get the canonical SELECT body for a view.
  String? getViewDdl(String viewName) {
    final result = _callStringByName(viewName, _bindings.getViewDdl);
    return result.isEmpty ? null : result;
  }

  /// List detailed metadata for all triggers.
  List<TriggerInfo> listTriggers() {
    final json = _callJsonFunc(_bindings.listTriggersJson);
    return (jsonDecode(json) as List)
        .map((e) => TriggerInfo.fromJson(e as Map<String, dynamic>))
        .toList();
  }
}
