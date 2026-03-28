import 'dart:ffi';
import 'dart:math' as math;
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'errors.dart';
import 'native_bindings.dart';
import 'types.dart';

// ---------------------------------------------------------------------------
// Row
// ---------------------------------------------------------------------------

/// A single decoded result row with O(1) named-column lookup.
class Row {
  /// Construct a row with an automatically-built column index.
  Row(List<String> columns, List<Object?> values)
      : this._indexed(
          columns,
          {for (var i = 0; i < columns.length; i++) columns[i]: i},
          values,
        );

  Row._indexed(this.columns, this._columnIndex, this.values);

  final List<String> columns;
  final List<Object?> values;
  final Map<String, int> _columnIndex;

  /// Returns the value for [name], or throws [ArgumentError] if unknown.
  Object? operator [](String name) {
    final index = _columnIndex[name];
    if (index == null) {
      throw ArgumentError('Unknown column: $name');
    }
    return values[index];
  }

  /// Returns the value at zero-based [index].
  Object? at(int index) => values[index];

  @override
  String toString() {
    final pairs = <String>[];
    for (var i = 0; i < columns.length; i++) {
      pairs.add('${columns[i]}: ${values[i]}');
    }
    return 'Row(${pairs.join(', ')})';
  }
}

// ---------------------------------------------------------------------------
// ResultPage
// ---------------------------------------------------------------------------

class ResultPage {
  const ResultPage(this.columns, this.rows, this.isLast);

  final List<String> columns;
  final List<Row> rows;
  final bool isLast;
}

// ---------------------------------------------------------------------------
// Statement  (backed by ddb_stmt_t)
// ---------------------------------------------------------------------------

/// A prepared statement wrapping a native [ddb_stmt_t] handle.
///
/// Obtain via [Database.prepare]. Callers are responsible for calling
/// [dispose] when done; failing to do so leaks the native statement.
///
/// Typical single-execution pattern:
/// ```dart
/// final stmt = db.prepare('SELECT * FROM t WHERE id = $1');
/// stmt.bindInt64(1, 42);
/// final rows = stmt.query();
/// stmt.dispose();
/// ```
///
/// Reuse pattern (avoids repeated SQL parsing):
/// ```dart
/// final stmt = db.prepare('INSERT INTO t VALUES ($1, $2)');
/// for (final row in data) {
///   stmt.reset();
///   stmt.clearBindings();
///   stmt.bindAll(row);
///   stmt.execute();
/// }
/// stmt.dispose();
/// ```
class Statement {
  Statement._(this._bindings, this._stmtPtr);

  /// Prepare [sql] against [dbPtr].  Throws [DecentDbException] on invalid SQL.
  factory Statement.fromSql(
    NativeBindings bindings,
    Pointer<DdbDb> dbPtr,
    String sql,
  ) {
    final sqlPtr = sql.toNativeUtf8();
    final outStmt = calloc<Pointer<DdbStmt>>();
    try {
      final status = bindings.dbPrepare(dbPtr, sqlPtr, outStmt);
      if (status != ddbOk) {
        final msgPtr = bindings.lastErrorMessage();
        final msg = msgPtr == nullptr
            ? 'Failed to prepare statement'
            : msgPtr.toDartString();
        throw DecentDbException(ErrorCode.fromCode(status), msg);
      }
      return Statement._(bindings, outStmt.value);
    } finally {
      calloc.free(outStmt);
      calloc.free(sqlPtr);
    }
  }

  final NativeBindings _bindings;
  Pointer<DdbStmt>? _stmtPtr;
  bool _disposed = false;

  // Cached execution results
  List<String> _columnNames = const [];
  Map<String, int> _columnIndex = const {};
  List<Row>? _rows;
  int _affectedRows = 0;
  int _cursor = 0;
  int _currentRow = -1;

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  void _checkNotDisposed() {
    if (_disposed) throw StateError('Statement has been disposed');
  }

  Never _throwStatus(int status, String fallback) {
    final msgPtr = _bindings.lastErrorMessage();
    final msg = msgPtr == nullptr ? fallback : msgPtr.toDartString();
    throw DecentDbException(ErrorCode.fromCode(status), msg);
  }

  void _invalidateExecution() {
    _rows = null;
    _affectedRows = 0;
    _cursor = 0;
    _currentRow = -1;
  }

  /// Reset the native step cursor (keeps bindings), then wipe cached rows.
  void _nativeReset() {
    if (_stmtPtr == null) return;
    final status = _bindings.stmtReset(_stmtPtr!);
    if (status != ddbOk) _throwStatus(status, 'Failed to reset statement');
  }

  /// Fetch column names from the native stmt; must be called after prepare
  /// (or after the first step for SELECT) and before iterating rows.
  void _loadColumnMetadata() {
    final countPtr = calloc<IntPtr>();
    try {
      final status = _bindings.stmtColumnCount(_stmtPtr!, countPtr);
      if (status != ddbOk) _throwStatus(status, 'Failed to get column count');
      final count = countPtr.value;
      if (count == 0) {
        _columnNames = const [];
        _columnIndex = const {};
        return;
      }
      final names = List<String>.filled(count, '');
      final outName = calloc<Pointer<Utf8>>();
      try {
        for (var i = 0; i < count; i++) {
          final nameStatus =
              _bindings.stmtColumnNameCopy(_stmtPtr!, i, outName);
          if (nameStatus != ddbOk) {
            _throwStatus(nameStatus, 'Failed to get column name');
          }
          names[i] = outName.value == nullptr ? '' : outName.value.toDartString();
          final freeStatus = _bindings.stringFree(outName);
          if (freeStatus != ddbOk) {
            _throwStatus(freeStatus, 'Failed to free column name');
          }
        }
      } finally {
        calloc.free(outName);
      }
      _columnNames = List.unmodifiable(names);
      _columnIndex = {for (var i = 0; i < names.length; i++) names[i]: i};
    } finally {
      calloc.free(countPtr);
    }
  }

  /// Step and collect all rows.  Caller must call [_nativeReset] first.
  void _fetchAll() {
    _checkNotDisposed();
    _loadColumnMetadata();

    final colCount = _columnNames.length;
    final outHasRow = calloc<Uint8>();
    // Reuse a single DdbValue allocation across all cells to reduce GC pressure.
    final valPtr = colCount > 0 ? calloc<DdbValue>() : nullptr.cast<DdbValue>();
    final rows = <Row>[];

    try {
      while (true) {
        final status = _bindings.stmtStep(_stmtPtr!, outHasRow);
        if (status != ddbOk) _throwStatus(status, 'Failed to step statement');
        if (outHasRow.value == 0) break;

        final values = List<Object?>.filled(colCount, null);
        for (var col = 0; col < colCount; col++) {
          final copyStatus =
              _bindings.stmtValueCopy(_stmtPtr!, col, valPtr);
          if (copyStatus != ddbOk) {
            _throwStatus(copyStatus, 'Failed to copy cell value');
          }
          values[col] = _decodeValue(valPtr.ref);
          final disposeStatus = _bindings.valueDispose(valPtr);
          if (disposeStatus != ddbOk) {
            _throwStatus(disposeStatus, 'Failed to dispose cell value');
          }
        }
        rows.add(Row._indexed(_columnNames, _columnIndex, values));
      }
    } finally {
      calloc.free(outHasRow);
      if (valPtr != nullptr) calloc.free(valPtr);
    }

    _rows = rows;
    _cursor = 0;
    _currentRow = -1;

    final affectedPtr = calloc<Uint64>();
    try {
      final status = _bindings.stmtAffectedRows(_stmtPtr!, affectedPtr);
      if (status != ddbOk) _throwStatus(status, 'Failed to get affected rows');
      _affectedRows = affectedPtr.value;
    } finally {
      calloc.free(affectedPtr);
    }
  }

  void _ensureExecuted() {
    if (_rows != null) return;
    _nativeReset();
    _fetchAll();
  }

  // ---------------------------------------------------------------------------
  // Bind API
  // ---------------------------------------------------------------------------

  /// Bind SQL NULL at 1-based [index].
  void bindNull(int index) {
    _checkNotDisposed();
    _invalidateExecution();
    final status = _bindings.stmtBindNull(_stmtPtr!, index);
    if (status != ddbOk) _throwStatus(status, 'Failed to bind null');
  }

  /// Bind an integer at 1-based [index].
  void bindInt64(int index, int value) {
    _checkNotDisposed();
    _invalidateExecution();
    final status = _bindings.stmtBindInt64(_stmtPtr!, index, value);
    if (status != ddbOk) _throwStatus(status, 'Failed to bind int64');
  }

  /// Bind a boolean at 1-based [index].
  void bindBool(int index, bool value) {
    _checkNotDisposed();
    _invalidateExecution();
    final status = _bindings.stmtBindBool(_stmtPtr!, index, value ? 1 : 0);
    if (status != ddbOk) _throwStatus(status, 'Failed to bind bool');
  }

  /// Bind a floating-point value at 1-based [index].
  void bindFloat64(int index, double value) {
    _checkNotDisposed();
    _invalidateExecution();
    final status = _bindings.stmtBindFloat64(_stmtPtr!, index, value);
    if (status != ddbOk) _throwStatus(status, 'Failed to bind float64');
  }

  /// Bind a text value at 1-based [index].
  void bindText(int index, String value) {
    _checkNotDisposed();
    _invalidateExecution();
    final utf8 = value.toNativeUtf8();
    try {
      final byteLen = utf8.length; // byte count, excluding null terminator
      final status = _bindings.stmtBindText(
          _stmtPtr!, index, utf8.cast<Uint8>(), byteLen);
      if (status != ddbOk) _throwStatus(status, 'Failed to bind text');
    } finally {
      calloc.free(utf8);
    }
  }

  /// Bind a blob value at 1-based [index].
  void bindBlob(int index, Uint8List value) {
    _checkNotDisposed();
    _invalidateExecution();
    if (value.isEmpty) {
      final status =
          _bindings.stmtBindBlob(_stmtPtr!, index, nullptr.cast<Uint8>(), 0);
      if (status != ddbOk) _throwStatus(status, 'Failed to bind blob');
      return;
    }
    final data = calloc<Uint8>(value.length);
    try {
      data.asTypedList(value.length).setAll(0, value);
      final status =
          _bindings.stmtBindBlob(_stmtPtr!, index, data, value.length);
      if (status != ddbOk) _throwStatus(status, 'Failed to bind blob');
    } finally {
      calloc.free(data);
    }
  }

  /// Bind a decimal value at 1-based [index].
  void bindDecimal(int index, int scaled, int scale) {
    _checkNotDisposed();
    _invalidateExecution();
    final status =
        _bindings.stmtBindDecimal(_stmtPtr!, index, scaled, scale);
    if (status != ddbOk) _throwStatus(status, 'Failed to bind decimal');
  }

  /// Bind a [DateTime] (converted to UTC microseconds) at 1-based [index].
  void bindDateTime(int index, DateTime value) {
    _checkNotDisposed();
    _invalidateExecution();
    final micros = value.toUtc().microsecondsSinceEpoch;
    final status =
        _bindings.stmtBindTimestampMicros(_stmtPtr!, index, micros);
    if (status != ddbOk) _throwStatus(status, 'Failed to bind timestamp');
  }

  /// Polymorphic bind: dispatches to the typed method based on [value]'s type.
  void bind(int index, Object? value) {
    if (value == null) return bindNull(index);
    if (value is int) return bindInt64(index, value);
    if (value is bool) return bindBool(index, value);
    if (value is double) return bindFloat64(index, value);
    if (value is String) return bindText(index, value);
    if (value is Uint8List) return bindBlob(index, value);
    if (value is DateTime) return bindDateTime(index, value);
    if (value is DecimalValue) return bindDecimal(index, value.scaled, value.scale);
    throw ArgumentError('Unsupported bind type: ${value.runtimeType}');
  }

  /// Bind each element of [params] at 1-based positions.
  void bindAll(List<Object?> params) {
    for (var i = 0; i < params.length; i++) {
      bind(i + 1, params[i]);
    }
  }

  // ---------------------------------------------------------------------------
  // Column metadata
  // ---------------------------------------------------------------------------

  int get columnCount => _columnNames.length;
  List<String> get columnNames => List.unmodifiable(_columnNames);

  // ---------------------------------------------------------------------------
  // Execution API
  // ---------------------------------------------------------------------------

  /// Execute and return all result rows.
  ///
  /// Always re-executes from the beginning (implicit reset + step loop).
  List<Row> query() {
    _checkNotDisposed();
    _nativeReset();
    _invalidateExecution();
    _fetchAll();
    return List<Row>.unmodifiable(_rows!);
  }

  /// Execute a DML statement and return the number of affected rows.
  ///
  /// For reuse, call [reset] and re-bind before calling [execute] again.
  int execute() {
    _checkNotDisposed();
    _invalidateExecution();
    _loadColumnMetadata();

    final outHasRow = calloc<Uint8>();
    try {
      while (true) {
        final status = _bindings.stmtStep(_stmtPtr!, outHasRow);
        if (status != ddbOk) _throwStatus(status, 'Failed to execute statement');
        if (outHasRow.value == 0) break;
        // DML should not produce rows; skip any that appear.
      }
    } finally {
      calloc.free(outHasRow);
    }

    final affectedPtr = calloc<Uint64>();
    try {
      final status = _bindings.stmtAffectedRows(_stmtPtr!, affectedPtr);
      if (status != ddbOk) _throwStatus(status, 'Failed to get affected rows');
      _affectedRows = affectedPtr.value;
    } finally {
      calloc.free(affectedPtr);
    }
    return _affectedRows;
  }

  // ---------------------------------------------------------------------------
  // Streaming / pagination API
  // ---------------------------------------------------------------------------

  /// Advance the row cursor, executing if needed.  Returns `true` while rows
  /// remain; `false` when exhausted.
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

  /// Return the current row after a successful [step] call.
  Row readRow() {
    _checkNotDisposed();
    if (_rows == null || _currentRow < 0 || _currentRow >= _rows!.length) {
      throw StateError('No current row; call step() first');
    }
    return _rows![_currentRow];
  }

  /// Return the next [pageSize] rows, executing if needed.
  ResultPage nextPage(int pageSize) {
    if (pageSize <= 0) throw RangeError.range(pageSize, 1, null, 'pageSize');
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

  // ---------------------------------------------------------------------------
  // Reset / clear / dispose
  // ---------------------------------------------------------------------------

  /// Reset the step cursor (keeps bound parameters).  Prepare to re-execute.
  void reset() {
    _checkNotDisposed();
    _nativeReset();
    _invalidateExecution();
  }

  /// Clear all bound parameters.
  void clearBindings() {
    _checkNotDisposed();
    final status = _bindings.stmtClearBindings(_stmtPtr!);
    if (status != ddbOk) _throwStatus(status, 'Failed to clear bindings');
    _invalidateExecution();
  }

  /// Release the native statement handle.
  void dispose() {
    if (_disposed) return;
    _disposed = true;
    _invalidateExecution();
    if (_stmtPtr != null) {
      final slot = calloc<Pointer<DdbStmt>>()..value = _stmtPtr!;
      try {
        _bindings.stmtFree(slot);
      } finally {
        calloc.free(slot);
        _stmtPtr = null;
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Value decoder (shared utility)
// ---------------------------------------------------------------------------

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
      if (value.data == nullptr || value.len == 0) return '';
      return value.data.cast<Utf8>().toDartString(length: value.len);
    case ddbTagBlob:
      if (value.data == nullptr || value.len == 0) return Uint8List(0);
      return Uint8List.fromList(value.data.asTypedList(value.len));
    case ddbTagDecimal:
      return DecimalValue(value.decimalScaled, value.decimalScale);
    case ddbTagUuid:
      final bytes = Uint8List(16);
      for (var i = 0; i < 16; i++) bytes[i] = value.uuidBytes[i];
      return bytes;
    case ddbTagTimestampMicros:
      return DateTime.fromMicrosecondsSinceEpoch(
        value.timestampMicros,
        isUtc: true,
      );
    default:
      throw StateError('Unsupported DecentDB value tag ${value.tag}');
  }
}
