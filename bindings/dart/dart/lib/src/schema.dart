import 'dart:convert';
import 'dart:ffi';

import 'package:ffi/ffi.dart';

import 'errors.dart';
import 'native_bindings.dart';
import 'schema_snapshot.dart';
import 'types.dart';

class Schema {
  const Schema.fromNative(this._bindings, this._dbPtr);

  final NativeBindings _bindings;
  final Pointer<DdbDb> _dbPtr;

  Never _throwStatus(int status, String fallback) {
    final messagePtr = _bindings.lastErrorMessage();
    final message =
        messagePtr == nullptr ? fallback : messagePtr.toDartString();
    throw DecentDbException(ErrorCode.fromCode(status), message);
  }

  String _callDbString(
    int Function(Pointer<DdbDb>, Pointer<Pointer<Utf8>>) fn,
    String fallback,
  ) {
    final out = calloc<Pointer<Utf8>>();
    try {
      final status = fn(_dbPtr, out);
      if (status != ddbOk) {
        _throwStatus(status, fallback);
      }
      final value = out.value == nullptr ? '' : out.value.toDartString();
      final freeStatus = _bindings.stringFree(out);
      if (freeStatus != ddbOk) {
        _throwStatus(freeStatus, 'Failed to free native string');
      }
      return value;
    } finally {
      calloc.free(out);
    }
  }

  String _callNamedString(
    String name,
    int Function(Pointer<DdbDb>, Pointer<Utf8>, Pointer<Pointer<Utf8>>) fn,
    String fallback,
  ) {
    final nativeName = name.toNativeUtf8();
    final out = calloc<Pointer<Utf8>>();
    try {
      final status = fn(_dbPtr, nativeName, out);
      if (status != ddbOk) {
        _throwStatus(status, fallback);
      }
      final value = out.value == nullptr ? '' : out.value.toDartString();
      final freeStatus = _bindings.stringFree(out);
      if (freeStatus != ddbOk) {
        _throwStatus(freeStatus, 'Failed to free native string');
      }
      return value;
    } finally {
      calloc.free(out);
      calloc.free(nativeName);
    }
  }

  List<TableInfo> listTablesInfo() {
    final json =
        _callDbString(_bindings.dbListTablesJson, 'Failed to list tables');
    return (jsonDecode(json) as List)
        .map((value) => TableInfo.fromJson(value as Map<String, dynamic>))
        .toList();
  }

  List<String> listTables() =>
      listTablesInfo().map((table) => table.name).toList();

  TableInfo describeTable(String name) {
    final json = _callNamedString(
      name,
      _bindings.dbDescribeTableJson,
      'Failed to describe table $name',
    );
    return TableInfo.fromJson(jsonDecode(json) as Map<String, dynamic>);
  }

  List<ColumnInfo> getTableColumns(String name) => describeTable(name).columns;

  String getTableDdl(String name) => _callNamedString(
      name, _bindings.dbGetTableDdl, 'Failed to get DDL for $name');

  List<IndexInfo> listIndexes() {
    final json =
        _callDbString(_bindings.dbListIndexesJson, 'Failed to list indexes');
    return (jsonDecode(json) as List)
        .map((value) => IndexInfo.fromJson(value as Map<String, dynamic>))
        .toList();
  }

  List<ViewInfo> listViewsInfo() {
    final json =
        _callDbString(_bindings.dbListViewsJson, 'Failed to list views');
    return (jsonDecode(json) as List)
        .map((value) => ViewInfo.fromJson(value as Map<String, dynamic>))
        .toList();
  }

  List<String> listViews() => listViewsInfo().map((view) => view.name).toList();

  String getViewDdl(String name) => _callNamedString(
      name, _bindings.dbGetViewDdl, 'Failed to get view DDL for $name');

  List<TriggerInfo> listTriggers() {
    final json =
        _callDbString(_bindings.dbListTriggersJson, 'Failed to list triggers');
    return (jsonDecode(json) as List)
        .map((value) => TriggerInfo.fromJson(value as Map<String, dynamic>))
        .toList();
  }

  SchemaSnapshot getSchemaSnapshot() {
    final json = _callDbString(
      _bindings.dbGetSchemaSnapshotJson,
      'Failed to get schema snapshot',
    );
    return SchemaSnapshot.fromJson(jsonDecode(json) as Map<String, dynamic>);
  }
}
