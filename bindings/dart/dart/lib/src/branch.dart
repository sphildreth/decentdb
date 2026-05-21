import 'dart:convert';
import 'dart:ffi';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'errors.dart';
import 'native_bindings.dart';
import 'statement.dart';
import 'types.dart';

/// Public DecentDB branch and named-snapshot workflow API.
///
/// Obtain from [Database.branchWorkflow].
class BranchWorkflow {
  BranchWorkflow.fromNative(this._bindings, this._dbPtr);

  final NativeBindings _bindings;
  final Pointer<DdbDb> _dbPtr;

  /// Create a retained named snapshot of the current durable `main` state.
  NamedSnapshot createSnapshot(String name) {
    final response = _executeJson(<String, Object?>{
      'op': 'snapshot_create',
      'name': name,
    });
    return NamedSnapshot.fromJson(_asMap(response));
  }

  /// List retained named snapshots.
  List<NamedSnapshot> listSnapshots() {
    final response = _executeJson(<String, Object?>{'op': 'snapshot_list'});
    return _asMapList(response)
        .map(NamedSnapshot.fromJson)
        .toList(growable: false);
  }

  /// Delete a retained named snapshot by [name].
  bool deleteSnapshot(String name) {
    final response = _executeJson(<String, Object?>{
      'op': 'snapshot_delete',
      'name': name,
    });
    return _asMap(response)['deleted'] == true;
  }

  /// Create a branch from `main`, another branch, a named snapshot, or a head.
  BranchInfo createBranch(String name, {String? from}) {
    final response = _executeJson(<String, Object?>{
      'op': 'branch_create',
      'name': name,
      if (from != null) 'from': from,
    });
    return BranchInfo.fromJson(_asMap(response));
  }

  /// List branches.
  List<BranchInfo> listBranches() {
    final response = _executeJson(<String, Object?>{'op': 'branch_list'});
    return _asMapList(response)
        .map(BranchInfo.fromJson)
        .toList(growable: false);
  }

  /// Delete a non-main branch by [name].
  bool deleteBranch(String name) {
    final response = _executeJson(<String, Object?>{
      'op': 'branch_delete',
      'name': name,
    });
    return _asMap(response)['deleted'] == true;
  }

  /// Rename a non-main branch.
  bool renameBranch(String name, String newName) {
    final response = _executeJson(<String, Object?>{
      'op': 'branch_rename',
      'name': name,
      'new_name': newName,
    });
    return _asMap(response)['renamed'] == true;
  }

  /// Add a named commit marker to a non-main branch.
  BranchLogEntry commitBranch(String name, String message) {
    final response = _executeJson(<String, Object?>{
      'op': 'branch_commit',
      'name': name,
      'message': message,
    });
    return BranchLogEntry.fromJson(_asMap(response));
  }

  /// Return branch head history, newest first.
  List<BranchLogEntry> branchLog(String name) {
    final response = _executeJson(<String, Object?>{
      'op': 'branch_log',
      'name': name,
    });
    return _asMapList(response)
        .map(BranchLogEntry.fromJson)
        .toList(growable: false);
  }

  /// Compare two refs: `main`, branch name, named snapshot, or head ID.
  BranchDiffReport diff(String leftRef, String rightRef) {
    final response = _executeJson(<String, Object?>{
      'op': 'branch_diff',
      'left': leftRef,
      'right': rightRef,
    });
    return BranchDiffReport.fromJson(_asMap(response));
  }

  /// Restore a non-main branch to another branch, named snapshot, or head ID.
  BranchRestoreReport restore(
    String branchName,
    String targetRef, {
    bool dryRun = true,
  }) {
    final response = _executeJson(<String, Object?>{
      'op': 'branch_restore',
      'name': branchName,
      'target': targetRef,
      'dry_run': dryRun,
    });
    return BranchRestoreReport.fromJson(_asMap(response));
  }

  /// Merge clean primary-key row changes from [sourceBranch] into [targetRef].
  BranchMergeReport merge(
    String sourceBranch,
    String targetRef, {
    bool dryRun = true,
  }) {
    final response = _executeJson(<String, Object?>{
      'op': 'branch_merge',
      'source': sourceBranch,
      'target': targetRef,
      'dry_run': dryRun,
    });
    return BranchMergeReport.fromJson(_asMap(response));
  }

  /// Execute one SQL statement on [branchName] and return the full native result.
  BranchExecutionResult executeSql(
    String branchName,
    String sql, [
    List<Object?> params = const <Object?>[],
  ]) {
    final nativeBranch = branchName.toNativeUtf8();
    final nativeSql = sql.toNativeUtf8();
    final nativeParams = _NativeParamBuffer(params);
    final outResult = calloc<Pointer<DdbResult>>();
    try {
      final status = _bindings.dbExecuteOnBranch(
        _dbPtr,
        nativeBranch,
        nativeSql,
        nativeParams.pointer,
        nativeParams.length,
        outResult,
      );
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to execute SQL on branch $branchName');
      }
      return _readResult(outResult.value);
    } finally {
      if (outResult.value != nullptr) {
        _bindings.resultFree(outResult);
      }
      nativeParams.dispose();
      calloc.free(outResult);
      calloc.free(nativeSql);
      calloc.free(nativeBranch);
    }
  }

  /// Execute one SQL statement on [branchName] and return a bounded first page.
  ResultPage querySql(
    String branchName,
    String sql, {
    List<Object?> params = const <Object?>[],
    int pageSize = 100,
  }) {
    if (pageSize <= 0) throw RangeError.range(pageSize, 1, null, 'pageSize');
    return executeSql(branchName, sql, params).firstPage(pageSize);
  }

  Object? _executeJson(Map<String, Object?> request) {
    final requestPtr = jsonEncode(request).toNativeUtf8();
    final outJson = calloc<Pointer<Utf8>>();
    try {
      final status = _bindings.dbBranchExecuteJson(_dbPtr, requestPtr, outJson);
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to execute branch workflow request');
      }
      final rawJson =
          outJson.value == nullptr ? 'null' : outJson.value.toDartString();
      final freeStatus = _bindings.stringFree(outJson);
      if (freeStatus != ddbOk) {
        _throwStatus(freeStatus, 'Failed to free branch workflow response');
      }
      return jsonDecode(rawJson);
    } finally {
      calloc.free(outJson);
      calloc.free(requestPtr);
    }
  }

  BranchExecutionResult _readResult(Pointer<DdbResult> result) {
    final rowCount = calloc<IntPtr>();
    final columnCount = calloc<IntPtr>();
    final affectedRows = calloc<Uint64>();
    try {
      var status = _bindings.resultColumnCount(result, columnCount);
      if (status != ddbOk) _throwStatus(status, 'Failed to read column count');
      status = _bindings.resultRowCount(result, rowCount);
      if (status != ddbOk) _throwStatus(status, 'Failed to read row count');
      status = _bindings.resultAffectedRows(result, affectedRows);
      if (status != ddbOk) _throwStatus(status, 'Failed to read affected rows');

      final columns = _readColumnNames(result, columnCount.value);
      final rows = <Row>[];
      final value = calloc<DdbValue>();
      try {
        for (var row = 0; row < rowCount.value; row++) {
          final values = List<Object?>.filled(columnCount.value, null);
          for (var col = 0; col < columnCount.value; col++) {
            final status = _bindings.resultValueCopy(result, row, col, value);
            if (status != ddbOk) {
              _throwStatus(status, 'Failed to read result value');
            }
            try {
              values[col] = _decodeOwnedValue(value.ref);
            } finally {
              final disposeStatus = _bindings.valueDispose(value);
              if (disposeStatus != ddbOk) {
                _throwStatus(disposeStatus, 'Failed to dispose result value');
              }
            }
          }
          rows.add(Row(columns, values));
        }
      } finally {
        calloc.free(value);
      }

      return BranchExecutionResult(
        columns: columns,
        rows: List<Row>.unmodifiable(rows),
        affectedRows: affectedRows.value,
      );
    } finally {
      calloc.free(rowCount);
      calloc.free(columnCount);
      calloc.free(affectedRows);
    }
  }

  List<String> _readColumnNames(Pointer<DdbResult> result, int count) {
    final columns = List<String>.filled(count, '');
    final outName = calloc<Pointer<Utf8>>();
    try {
      for (var i = 0; i < count; i++) {
        final status = _bindings.resultColumnNameCopy(result, i, outName);
        if (status != ddbOk) {
          _throwStatus(status, 'Failed to read column name');
        }
        columns[i] =
            outName.value == nullptr ? '' : outName.value.toDartString();
        final freeStatus = _bindings.stringFree(outName);
        if (freeStatus != ddbOk) {
          _throwStatus(freeStatus, 'Failed to free column name');
        }
      }
    } finally {
      calloc.free(outName);
    }
    return List<String>.unmodifiable(columns);
  }

  Never _throwStatus(int status, String fallback) {
    final msgPtr = _bindings.lastErrorMessage();
    final msg = msgPtr == nullptr ? fallback : msgPtr.toDartString();
    throw DecentDbException(ErrorCode.fromCode(status), msg);
  }
}

/// Result from [BranchWorkflow.executeSql].
class BranchExecutionResult {
  const BranchExecutionResult({
    required this.columns,
    required this.rows,
    required this.affectedRows,
  });

  final List<String> columns;
  final List<Row> rows;
  final int affectedRows;

  bool get returnsRows => columns.isNotEmpty;

  ResultPage firstPage(int pageSize) {
    if (pageSize <= 0) throw RangeError.range(pageSize, 1, null, 'pageSize');
    final pageRows = rows.length <= pageSize
        ? rows
        : List<Row>.unmodifiable(rows.take(pageSize));
    return ResultPage(columns, pageRows, rows.length <= pageSize);
  }
}

class BranchInfo {
  const BranchInfo({
    required this.branchId,
    required this.name,
    required this.currentHeadId,
    required this.baseHeadId,
    required this.createdAtMicros,
    required this.updatedAtMicros,
    required this.deletedAtMicros,
  });

  final String branchId;
  final String name;
  final String? currentHeadId;
  final String? baseHeadId;
  final int createdAtMicros;
  final int updatedAtMicros;
  final int? deletedAtMicros;

  bool get isMain => name == 'main';

  DateTime get createdAt =>
      DateTime.fromMicrosecondsSinceEpoch(createdAtMicros, isUtc: true);

  DateTime get updatedAt =>
      DateTime.fromMicrosecondsSinceEpoch(updatedAtMicros, isUtc: true);

  factory BranchInfo.fromJson(Map<String, Object?> json) {
    return BranchInfo(
      branchId: json['branch_id']! as String,
      name: json['name']! as String,
      currentHeadId: json['current_head_id'] as String?,
      baseHeadId: json['base_head_id'] as String?,
      createdAtMicros: json['created_at_micros']! as int,
      updatedAtMicros: json['updated_at_micros']! as int,
      deletedAtMicros: json['deleted_at_micros'] as int?,
    );
  }
}

class NamedSnapshot {
  const NamedSnapshot({
    required this.snapshotId,
    required this.name,
    required this.branchId,
    required this.headId,
    required this.snapshotLsn,
    required this.createdAtMicros,
  });

  final String snapshotId;
  final String name;
  final String branchId;
  final String headId;
  final int snapshotLsn;
  final int createdAtMicros;

  DateTime get createdAt =>
      DateTime.fromMicrosecondsSinceEpoch(createdAtMicros, isUtc: true);

  factory NamedSnapshot.fromJson(Map<String, Object?> json) {
    return NamedSnapshot(
      snapshotId: json['snapshot_id']! as String,
      name: json['name']! as String,
      branchId: json['branch_id']! as String,
      headId: json['head_id']! as String,
      snapshotLsn: json['snapshot_lsn']! as int,
      createdAtMicros: json['created_at_micros']! as int,
    );
  }
}

class BranchLogEntry {
  const BranchLogEntry({
    required this.headId,
    required this.branchId,
    required this.parentHeadId,
    required this.message,
    required this.createdAtMicros,
    required this.sql,
  });

  final String headId;
  final String branchId;
  final String? parentHeadId;
  final String? message;
  final int createdAtMicros;
  final String? sql;

  DateTime get createdAt =>
      DateTime.fromMicrosecondsSinceEpoch(createdAtMicros, isUtc: true);

  factory BranchLogEntry.fromJson(Map<String, Object?> json) {
    return BranchLogEntry(
      headId: json['head_id']! as String,
      branchId: json['branch_id']! as String,
      parentHeadId: json['parent_head_id'] as String?,
      message: json['message'] as String?,
      createdAtMicros: json['created_at_micros']! as int,
      sql: json['sql'] as String?,
    );
  }
}

class BranchDiffReport {
  const BranchDiffReport({
    required this.leftRef,
    required this.rightRef,
    required this.tableCount,
    required this.changedTableCount,
    required this.addedRowCount,
    required this.updatedRowCount,
    required this.deletedRowCount,
    required this.tables,
  });

  final String leftRef;
  final String rightRef;
  final int tableCount;
  final int changedTableCount;
  final int addedRowCount;
  final int updatedRowCount;
  final int deletedRowCount;
  final List<BranchTableDiff> tables;

  factory BranchDiffReport.fromJson(Map<String, Object?> json) {
    return BranchDiffReport(
      leftRef: json['left_ref']! as String,
      rightRef: json['right_ref']! as String,
      tableCount: json['table_count']! as int,
      changedTableCount: json['changed_table_count']! as int,
      addedRowCount: json['added_row_count']! as int,
      updatedRowCount: json['updated_row_count']! as int,
      deletedRowCount: json['deleted_row_count']! as int,
      tables: _asMapList(json['tables'])
          .map(BranchTableDiff.fromJson)
          .toList(growable: false),
    );
  }
}

class BranchTableDiff {
  const BranchTableDiff({
    required this.table,
    required this.status,
    required this.schemaChanged,
    required this.added,
    required this.updated,
    required this.deleted,
    required this.message,
  });

  final String table;
  final String status;
  final bool schemaChanged;
  final List<BranchRowDiff> added;
  final List<BranchRowDiff> updated;
  final List<BranchRowDiff> deleted;
  final String? message;

  factory BranchTableDiff.fromJson(Map<String, Object?> json) {
    return BranchTableDiff(
      table: json['table']! as String,
      status: json['status']! as String,
      schemaChanged: json['schema_changed']! as bool,
      added: _asMapList(json['added'])
          .map(BranchRowDiff.fromJson)
          .toList(growable: false),
      updated: _asMapList(json['updated'])
          .map(BranchRowDiff.fromJson)
          .toList(growable: false),
      deleted: _asMapList(json['deleted'])
          .map(BranchRowDiff.fromJson)
          .toList(growable: false),
      message: json['message'] as String?,
    );
  }
}

class BranchRowDiff {
  const BranchRowDiff({
    required this.primaryKey,
    required this.before,
    required this.after,
  });

  final List<String> primaryKey;
  final List<String>? before;
  final List<String>? after;

  factory BranchRowDiff.fromJson(Map<String, Object?> json) {
    return BranchRowDiff(
      primaryKey:
          ((json['primary_key'] as List?) ?? const <Object?>[]).cast<String>(),
      before: (json['before'] as List?)?.cast<String>(),
      after: (json['after'] as List?)?.cast<String>(),
    );
  }
}

class BranchRestoreReport {
  const BranchRestoreReport({
    required this.branch,
    required this.targetRef,
    required this.dryRun,
    required this.previousHeadId,
    required this.targetHeadId,
    required this.newHeadId,
    required this.changedTableCount,
    required this.addedRowCount,
    required this.updatedRowCount,
    required this.deletedRowCount,
  });

  final String branch;
  final String targetRef;
  final bool dryRun;
  final String? previousHeadId;
  final String targetHeadId;
  final String? newHeadId;
  final int changedTableCount;
  final int addedRowCount;
  final int updatedRowCount;
  final int deletedRowCount;

  factory BranchRestoreReport.fromJson(Map<String, Object?> json) {
    return BranchRestoreReport(
      branch: json['branch']! as String,
      targetRef: json['target_ref']! as String,
      dryRun: json['dry_run']! as bool,
      previousHeadId: json['previous_head_id'] as String?,
      targetHeadId: json['target_head_id']! as String,
      newHeadId: json['new_head_id'] as String?,
      changedTableCount: json['changed_table_count']! as int,
      addedRowCount: json['added_row_count']! as int,
      updatedRowCount: json['updated_row_count']! as int,
      deletedRowCount: json['deleted_row_count']! as int,
    );
  }
}

class BranchMergeReport {
  const BranchMergeReport({
    required this.source,
    required this.target,
    required this.dryRun,
    required this.clean,
    required this.baseHeadId,
    required this.tableCount,
    required this.appliedChangeCount,
    required this.conflictCount,
    required this.applied,
    required this.conflicts,
  });

  final String source;
  final String target;
  final bool dryRun;
  final bool clean;
  final String baseHeadId;
  final int tableCount;
  final int appliedChangeCount;
  final int conflictCount;
  final List<BranchMergeChange> applied;
  final List<BranchMergeConflict> conflicts;

  factory BranchMergeReport.fromJson(Map<String, Object?> json) {
    return BranchMergeReport(
      source: json['source']! as String,
      target: json['target']! as String,
      dryRun: json['dry_run']! as bool,
      clean: json['clean']! as bool,
      baseHeadId: json['base_head_id']! as String,
      tableCount: json['table_count']! as int,
      appliedChangeCount: json['applied_change_count']! as int,
      conflictCount: json['conflict_count']! as int,
      applied: _asMapList(json['applied'])
          .map(BranchMergeChange.fromJson)
          .toList(growable: false),
      conflicts: _asMapList(json['conflicts'])
          .map(BranchMergeConflict.fromJson)
          .toList(growable: false),
    );
  }
}

class BranchMergeChange {
  const BranchMergeChange({
    required this.table,
    required this.primaryKey,
    required this.operation,
  });

  final String table;
  final List<String> primaryKey;
  final String operation;

  factory BranchMergeChange.fromJson(Map<String, Object?> json) {
    return BranchMergeChange(
      table: json['table']! as String,
      primaryKey:
          ((json['primary_key'] as List?) ?? const <Object?>[]).cast<String>(),
      operation: json['operation']! as String,
    );
  }
}

class BranchMergeConflict {
  const BranchMergeConflict({
    required this.table,
    required this.primaryKey,
    required this.conflictType,
    required this.message,
  });

  final String table;
  final List<String> primaryKey;
  final String conflictType;
  final String message;

  factory BranchMergeConflict.fromJson(Map<String, Object?> json) {
    return BranchMergeConflict(
      table: json['table']! as String,
      primaryKey:
          ((json['primary_key'] as List?) ?? const <Object?>[]).cast<String>(),
      conflictType: json['conflict_type']! as String,
      message: json['message']! as String,
    );
  }
}

class _NativeParamBuffer {
  _NativeParamBuffer(List<Object?> params)
      : length = params.length,
        pointer = params.isEmpty
            ? nullptr.cast<DdbValue>()
            : calloc<DdbValue>(params.length) {
    for (var i = 0; i < params.length; i++) {
      _writeValue((pointer + i).ref, params[i]);
    }
  }

  final int length;
  final Pointer<DdbValue> pointer;
  final List<void Function()> _release = <void Function()>[];

  void _writeValue(DdbValue slot, Object? value) {
    if (value == null) {
      slot.tag = ddbTagNull;
    } else if (value is int) {
      slot
        ..tag = ddbTagInt64
        ..int64Value = value;
    } else if (value is bool) {
      slot
        ..tag = ddbTagBool
        ..boolValue = value ? 1 : 0;
    } else if (value is double) {
      slot
        ..tag = ddbTagFloat64
        ..float64Value = value;
    } else if (value is String) {
      final data = value.toNativeUtf8();
      _release.add(() => calloc.free(data));
      slot
        ..tag = ddbTagText
        ..data = data.cast<Uint8>()
        ..len = data.length;
    } else if (value is UuidValue) {
      slot.tag = ddbTagUuid;
      for (var i = 0; i < 16; i++) {
        slot.uuidBytes[i] = value.bytes[i];
      }
    } else if (value is Uint8List) {
      if (value.isEmpty) {
        slot
          ..tag = ddbTagBlob
          ..data = nullptr.cast<Uint8>()
          ..len = 0;
      } else {
        final data = malloc<Uint8>(value.length);
        data.asTypedList(value.length).setAll(0, value);
        _release.add(() => malloc.free(data));
        slot
          ..tag = ddbTagBlob
          ..data = data
          ..len = value.length;
      }
    } else if (value is DateTime) {
      slot
        ..tag = ddbTagTimestampMicros
        ..timestampMicros = value.toUtc().microsecondsSinceEpoch;
    } else if (value is DecimalValue) {
      slot
        ..tag = ddbTagDecimal
        ..decimalScaled = value.scaled
        ..decimalScale = value.scale;
    } else {
      throw ArgumentError('Unsupported branch bind type: ${value.runtimeType}');
    }
  }

  void dispose() {
    for (final release in _release.reversed) {
      release();
    }
    if (pointer != nullptr) {
      calloc.free(pointer);
    }
  }
}

Object? _decodeOwnedValue(DdbValue value) {
  switch (value.tag) {
    case ddbTagNull:
      return null;
    case ddbTagInt64:
      return value.int64Value;
    case ddbTagFloat64:
      return value.float64Value;
    case ddbTagBool:
      return value.boolValue != 0;
    case ddbTagText:
      if (value.data == nullptr || value.len == 0) return '';
      return value.data.cast<Utf8>().toDartString(length: value.len);
    case ddbTagBlob:
    case ddbTagGeometry:
    case ddbTagGeography:
      if (value.data == nullptr || value.len == 0) return Uint8List(0);
      return Uint8List.fromList(value.data.asTypedList(value.len));
    case ddbTagDecimal:
      return DecimalValue(value.decimalScaled, value.decimalScale);
    case ddbTagUuid:
      final bytes = Uint8List(16);
      for (var i = 0; i < 16; i++) {
        bytes[i] = value.uuidBytes[i];
      }
      return bytes;
    case ddbTagTimestampMicros:
      return DateTime.fromMicrosecondsSinceEpoch(
        value.timestampMicros,
        isUtc: true,
      );
    case ddbTagEnum:
      return DecentDBEnumValue(value.enumTypeId, value.enumLabelId);
    case ddbTagIpAddr:
      return _formatIpAddr(value.ipFamily, value.ipCidrAddrBytes);
    case ddbTagCidr:
      return '${_formatIpAddr(value.ipFamily, value.ipCidrAddrBytes)}/${value.cidrPrefixLen}';
    case ddbTagDate:
      return DateTime.fromMicrosecondsSinceEpoch(
        value.dateDays * 86400000000,
        isUtc: true,
      );
    case ddbTagTime:
      return Duration(microseconds: value.timeMicros);
    case ddbTagTimestamptzMicros:
      return DateTime.fromMicrosecondsSinceEpoch(
        value.timestamptzMicros,
        isUtc: true,
      );
    case ddbTagInterval:
      return DecentDBIntervalValue(
        value.intervalMonths,
        value.intervalDays,
        value.intervalMicros,
      );
    case ddbTagMacaddr:
      return _formatMacAddr(value.ipFamily, value.ipCidrAddrBytes);
    default:
      throw StateError('Unsupported DecentDB value tag ${value.tag}');
  }
}

String _formatMacAddr(int length, Array<Uint8> bytes) {
  if (length != 6 && length != 8) {
    return '<invalid-macaddr-length-$length>';
  }
  return List.generate(
    length,
    (index) => bytes[index].toRadixString(16).padLeft(2, '0'),
  ).join(':');
}

String _formatIpAddr(int family, Array<Uint8> bytes) {
  if (family == 4) {
    return '${bytes[0]}.${bytes[1]}.${bytes[2]}.${bytes[3]}';
  }
  if (family == 6) {
    final groups = <String>[];
    for (var i = 0; i < 16; i += 2) {
      groups.add(((bytes[i] << 8) | bytes[i + 1]).toRadixString(16));
    }
    return groups.join(':');
  }
  return '<invalid-ip-family-$family>';
}

Map<String, Object?> _asMap(Object? value) {
  return Map<String, Object?>.from(value! as Map);
}

List<Map<String, Object?>> _asMapList(Object? value) {
  return ((value as List?) ?? const <Object?>[])
      .map((item) => Map<String, Object?>.from(item! as Map))
      .toList(growable: false);
}
