import 'dart:ffi';
import 'dart:math' as math;
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'errors.dart';
import 'native_bindings.dart';
import 'types.dart';

class Row {
  const Row(this.columns, this.values);

  final List<String> columns;
  final List<Object?> values;

  Object? operator [](String name) {
    final index = columns.indexOf(name);
    if (index < 0) {
      throw ArgumentError('Unknown column: $name');
    }
    return values[index];
  }

  Object? at(int index) => values[index];

  @override
  String toString() {
    final pairs = <String>[];
    for (var index = 0; index < columns.length; index++) {
      pairs.add('${columns[index]}: ${values[index]}');
    }
    return 'Row(${pairs.join(', ')})';
  }
}

class ResultPage {
  const ResultPage(this.columns, this.rows, this.isLast);

  final List<String> columns;
  final List<Row> rows;
  final bool isLast;
}

class Statement {
  Statement.fromSql(this._bindings, this._dbPtr, this._sql);

  final NativeBindings _bindings;
  final Pointer<DdbDb> _dbPtr;
  final String _sql;
  final List<Object?> _params = <Object?>[];

  bool _disposed = false;
  List<String> _columnNames = const [];
  List<Row>? _rows;
  int _affectedRows = 0;
  int _cursor = 0;
  int _currentRow = -1;

  void _checkNotDisposed() {
    if (_disposed) {
      throw StateError('Statement has been disposed');
    }
  }

  Never _throwStatus(int status, String fallback) {
    final messagePtr = _bindings.lastErrorMessage();
    final message =
        messagePtr == nullptr ? fallback : messagePtr.toDartString();
    throw DecentDbException(ErrorCode.fromCode(status), message);
  }

  void _ensureIndex(int index) {
    if (index < 1) {
      throw RangeError.range(index, 1, null, 'index');
    }
    while (_params.length < index) {
      _params.add(null);
    }
  }

  void _invalidateExecution() {
    _rows = null;
    _columnNames = const [];
    _affectedRows = 0;
    _cursor = 0;
    _currentRow = -1;
  }

  void bindNull(int index) {
    _checkNotDisposed();
    _ensureIndex(index);
    _params[index - 1] = null;
    _invalidateExecution();
  }

  void bindInt64(int index, int value) {
    _checkNotDisposed();
    _ensureIndex(index);
    _params[index - 1] = value;
    _invalidateExecution();
  }

  void bindBool(int index, bool value) {
    _checkNotDisposed();
    _ensureIndex(index);
    _params[index - 1] = value;
    _invalidateExecution();
  }

  void bindFloat64(int index, double value) {
    _checkNotDisposed();
    _ensureIndex(index);
    _params[index - 1] = value;
    _invalidateExecution();
  }

  void bindText(int index, String value) {
    _checkNotDisposed();
    _ensureIndex(index);
    _params[index - 1] = value;
    _invalidateExecution();
  }

  void bindBlob(int index, Uint8List value) {
    _checkNotDisposed();
    _ensureIndex(index);
    _params[index - 1] = value;
    _invalidateExecution();
  }

  void bindDecimal(int index, int scaled, int scale) {
    _checkNotDisposed();
    _ensureIndex(index);
    _params[index - 1] = DecimalValue(scaled, scale);
    _invalidateExecution();
  }

  void bindDateTime(int index, DateTime value) {
    _checkNotDisposed();
    _ensureIndex(index);
    _params[index - 1] = value.toUtc();
    _invalidateExecution();
  }

  void bind(int index, Object? value) {
    if (value == null) {
      bindNull(index);
      return;
    }
    if (value is int) {
      bindInt64(index, value);
      return;
    }
    if (value is bool) {
      bindBool(index, value);
      return;
    }
    if (value is double) {
      bindFloat64(index, value);
      return;
    }
    if (value is String) {
      bindText(index, value);
      return;
    }
    if (value is Uint8List) {
      bindBlob(index, value);
      return;
    }
    if (value is DateTime) {
      bindDateTime(index, value);
      return;
    }
    if (value is DecimalValue) {
      bindDecimal(index, value.scaled, value.scale);
      return;
    }
    throw ArgumentError('Unsupported bind type: ${value.runtimeType}');
  }

  void bindAll(List<Object?> params) {
    for (var index = 0; index < params.length; index++) {
      bind(index + 1, params[index]);
    }
  }

  int get columnCount => _columnNames.length;

  List<String> get columnNames => List.unmodifiable(_columnNames);

  void _ensureExecuted() {
    if (_rows != null) {
      return;
    }
    _run();
  }

  void _run() {
    _checkNotDisposed();
    final sqlPtr = _sql.toNativeUtf8();
    final resultSlot = calloc<Pointer<DdbResult>>();
    final encoded = _EncodedParams.fromValues(_params);
    try {
      final status = _bindings.dbExecute(
        _dbPtr,
        sqlPtr,
        encoded.values,
        encoded.length,
        resultSlot,
      );
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to execute SQL');
      }
      try {
        final decoded = _decodeResult(resultSlot.value);
        _columnNames = decoded.columns;
        _rows = decoded.rows;
        _affectedRows = decoded.affectedRows;
      } finally {
        final freeStatus = _bindings.resultFree(resultSlot);
        if (freeStatus != ddbOk) {
          _throwStatus(freeStatus, 'Failed to free query result');
        }
      }
      _cursor = 0;
      _currentRow = -1;
    } finally {
      encoded.dispose();
      calloc.free(resultSlot);
      calloc.free(sqlPtr);
    }
  }

  _DecodedResult _decodeResult(Pointer<DdbResult> resultPtr) {
    final rowCountPtr = calloc<IntPtr>();
    final columnCountPtr = calloc<IntPtr>();
    final affectedRowsPtr = calloc<Uint64>();
    try {
      var status = _bindings.resultRowCount(resultPtr, rowCountPtr);
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to read row count');
      }
      status = _bindings.resultColumnCount(resultPtr, columnCountPtr);
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to read column count');
      }
      status = _bindings.resultAffectedRows(resultPtr, affectedRowsPtr);
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to read affected rows');
      }

      final rowCount = rowCountPtr.value;
      final columnCount = columnCountPtr.value;
      final columns = List<String>.generate(columnCount, (columnIndex) {
        final outName = calloc<Pointer<Utf8>>();
        try {
          final nameStatus = _bindings.resultColumnNameCopy(
            resultPtr,
            columnIndex,
            outName,
          );
          if (nameStatus != ddbOk) {
            _throwStatus(nameStatus, 'Failed to read column name');
          }
          final name =
              outName.value == nullptr ? '' : outName.value.toDartString();
          final freeStatus = _bindings.stringFree(outName);
          if (freeStatus != ddbOk) {
            _throwStatus(freeStatus, 'Failed to free column name');
          }
          return name;
        } finally {
          calloc.free(outName);
        }
      });

      final rows = <Row>[];
      for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        final values = <Object?>[];
        for (var columnIndex = 0; columnIndex < columnCount; columnIndex++) {
          final valuePtr = calloc<DdbValue>();
          try {
            final valueStatus = _bindings.resultValueCopy(
              resultPtr,
              rowIndex,
              columnIndex,
              valuePtr,
            );
            if (valueStatus != ddbOk) {
              _throwStatus(valueStatus, 'Failed to copy cell value');
            }
            values.add(_decodeValue(valuePtr.ref));
            final disposeStatus = _bindings.valueDispose(valuePtr);
            if (disposeStatus != ddbOk) {
              _throwStatus(disposeStatus, 'Failed to dispose copied value');
            }
          } finally {
            calloc.free(valuePtr);
          }
        }
        rows.add(Row(columns, values));
      }

      return _DecodedResult(columns, rows, affectedRowsPtr.value);
    } finally {
      calloc.free(rowCountPtr);
      calloc.free(columnCountPtr);
      calloc.free(affectedRowsPtr);
    }
  }

  Object? _decodeValue(DdbValue value) {
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
        if (value.data == nullptr || value.len == 0) {
          return '';
        }
        return value.data.cast<Utf8>().toDartString(length: value.len);
      case ddbTagBlob:
        if (value.data == nullptr || value.len == 0) {
          return Uint8List(0);
        }
        return Uint8List.fromList(value.data.asTypedList(value.len));
      case ddbTagDecimal:
        return DecimalValue(value.decimalScaled, value.decimalScale);
      case ddbTagUuid:
        return Uint8List.fromList(
          List<int>.generate(16, (index) => value.uuidBytes[index]),
        );
      case ddbTagTimestampMicros:
        return DateTime.fromMicrosecondsSinceEpoch(
          value.timestampMicros,
          isUtc: true,
        );
      default:
        throw StateError('Unsupported DecentDB value tag ${value.tag}');
    }
  }

  List<Row> query() {
    _invalidateExecution();
    _run();
    return List<Row>.unmodifiable(_rows!);
  }

  ResultPage nextPage(int pageSize) {
    if (pageSize <= 0) {
      throw RangeError.range(pageSize, 1, null, 'pageSize');
    }
    _ensureExecuted();
    final rows = _rows!;
    final end = math.min(_cursor + pageSize, rows.length);
    final pageRows = rows.sublist(_cursor, end);
    _cursor = end;
    return ResultPage(
      List.unmodifiable(_columnNames),
      List<Row>.unmodifiable(pageRows),
      end >= rows.length,
    );
  }

  bool step() {
    _ensureExecuted();
    if (_cursor >= _rows!.length) {
      _currentRow = -1;
      return false;
    }
    _currentRow = _cursor;
    _cursor += 1;
    return true;
  }

  Row readRow() {
    _checkNotDisposed();
    if (_rows == null || _currentRow < 0 || _currentRow >= _rows!.length) {
      throw StateError('No current row; call step() first');
    }
    return _rows![_currentRow];
  }

  int execute() {
    _invalidateExecution();
    _checkNotDisposed();
    final sqlPtr = _sql.toNativeUtf8();
    final resultSlot = calloc<Pointer<DdbResult>>();
    final encoded = _EncodedParams.fromValues(_params);
    try {
      final status = _bindings.dbExecute(
        _dbPtr,
        sqlPtr,
        encoded.values,
        encoded.length,
        resultSlot,
      );
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to execute SQL');
      }
      final affectedRowsPtr = calloc<Uint64>();
      try {
        final affectedStatus =
            _bindings.resultAffectedRows(resultSlot.value, affectedRowsPtr);
        if (affectedStatus != ddbOk) {
          _throwStatus(affectedStatus, 'Failed to read affected rows');
        }
        _affectedRows = affectedRowsPtr.value;
      } finally {
        calloc.free(affectedRowsPtr);
      }

      final freeStatus = _bindings.resultFree(resultSlot);
      if (freeStatus != ddbOk) {
        _throwStatus(freeStatus, 'Failed to free query result');
      }
      _cursor = 0;
      _currentRow = -1;
    } finally {
      encoded.dispose();
      calloc.free(resultSlot);
      calloc.free(sqlPtr);
    }
    return _affectedRows;
  }

  void reset() {
    _checkNotDisposed();
    _invalidateExecution();
  }

  void clearBindings() {
    _checkNotDisposed();
    _params.clear();
    _invalidateExecution();
  }

  void dispose() {
    _disposed = true;
    _invalidateExecution();
  }
}

class _DecodedResult {
  const _DecodedResult(this.columns, this.rows, this.affectedRows);

  final List<String> columns;
  final List<Row> rows;
  final int affectedRows;
}

class _EncodedParams {
  _EncodedParams(this.values, this._allocatedPointers, this.length);

  final Pointer<DdbValue> values;
  final List<Pointer<Void>> _allocatedPointers;
  final int length;

  factory _EncodedParams.fromValues(List<Object?> values) {
    if (values.isEmpty) {
      return _EncodedParams(Pointer<DdbValue>.fromAddress(0), const [], 0);
    }
    final nativeValues = calloc<DdbValue>(values.length);
    final allocations = <Pointer<Void>>[];
    for (var index = 0; index < values.length; index++) {
      _writeValue(
          nativeValues.elementAt(index).ref, values[index], allocations);
    }
    return _EncodedParams(nativeValues, allocations, values.length);
  }

  static void _writeValue(
    DdbValue out,
    Object? value,
    List<Pointer<Void>> allocations,
  ) {
    if (value == null) {
      out.tag = ddbTagNull;
      return;
    }
    if (value is int) {
      out.tag = ddbTagInt64;
      out.int64Value = value;
      return;
    }
    if (value is bool) {
      out.tag = ddbTagBool;
      out.boolValue = value ? 1 : 0;
      return;
    }
    if (value is double) {
      out.tag = ddbTagFloat64;
      out.float64Value = value;
      return;
    }
    if (value is String) {
      final utf8 = value.toNativeUtf8();
      allocations.add(utf8.cast<Void>());
      out.tag = ddbTagText;
      out.data = utf8.cast<Uint8>();
      out.len = utf8.length;
      return;
    }
    if (value is Uint8List) {
      final data = calloc<Uint8>(value.length);
      allocations.add(data.cast<Void>());
      data.asTypedList(value.length).setAll(0, value);
      out.tag = ddbTagBlob;
      out.data = data;
      out.len = value.length;
      return;
    }
    if (value is DecimalValue) {
      out.tag = ddbTagDecimal;
      out.decimalScaled = value.scaled;
      out.decimalScale = value.scale;
      return;
    }
    if (value is DateTime) {
      out.tag = ddbTagTimestampMicros;
      out.timestampMicros = value.toUtc().microsecondsSinceEpoch;
      return;
    }
    throw ArgumentError('Unsupported parameter type: ${value.runtimeType}');
  }

  void dispose() {
    for (final pointer in _allocatedPointers) {
      calloc.free(pointer);
    }
    if (values != Pointer<DdbValue>.fromAddress(0)) {
      calloc.free(values);
    }
  }
}
