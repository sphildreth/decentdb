import 'dart:collection';
import 'dart:ffi';
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

/// Token passed to the GC finalizer for safe native cleanup.
typedef _StmtToken = ({NativeBindings bindings, Pointer<DdbStmt> ptr});

/// A prepared statement wrapping a native [ddb_stmt_t] handle.
///
/// Obtain via [Database.prepare]. Callers are responsible for calling
/// [dispose] when done; failing to do so leaks the native statement handle
/// (though a Dart [Finalizer] provides a GC-safety net).
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
  Statement._(this._bindings, Pointer<DdbStmt> stmtPtr) : _stmtPtr = stmtPtr {
    _finalizer.attach(
      this,
      (bindings: _bindings, ptr: stmtPtr),
      detach: this,
    );
  }

  // GC-safety finalizer: runs if dispose() is never called.
  static final _finalizer = Finalizer<_StmtToken>((token) {
    final slot = calloc<Pointer<DdbStmt>>()..value = token.ptr;
    try {
      token.bindings.stmtFree(slot);
    } finally {
      calloc.free(slot);
    }
  });

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
  bool _columnMetadataLoaded = false; // D7: persists across reset/bind cycles
  bool _executionPrimed = false;
  bool _streamExhausted = false;
  int _affectedRows = 0;
  Row? _currentRow;

  /// `true` if [dispose] has been called.
  bool get isDisposed => _disposed;

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
    _executionPrimed = false;
    _streamExhausted = false;
    _affectedRows = 0;
    _currentRow = null;
  }

  /// Reset the native step cursor (keeps bindings), then wipe cached rows.
  void _nativeReset() {
    if (_stmtPtr == null) return;
    final status = _bindings.stmtReset(_stmtPtr!);
    if (status != ddbOk) _throwStatus(status, 'Failed to reset statement');
  }

  /// Fetch column names from the native stmt; must be called after prepare
  /// (or after the first step for SELECT) and before iterating rows.
  ///
  /// Column names do not change between resets within the same prepared
  /// statement, so this method short-circuits on subsequent calls (D7).
  void _loadColumnMetadata() {
    if (_columnMetadataLoaded) return; // D7: only load once per statement lifetime
    final countPtr = calloc<IntPtr>();
    try {
      final status = _bindings.stmtColumnCount(_stmtPtr!, countPtr);
      if (status != ddbOk) _throwStatus(status, 'Failed to get column count');
      final count = countPtr.value;
      if (count == 0) {
        _columnNames = const [];
        _columnIndex = const {};
        _columnMetadataLoaded = true;
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
          names[i] =
              outName.value == nullptr ? '' : outName.value.toDartString();
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
      _columnMetadataLoaded = true; // D7: mark as cached
    } finally {
      calloc.free(countPtr);
    }
  }

  void _primeStreamingExecution() {
    if (_executionPrimed) {
      return;
    }
    _nativeReset();
    _loadColumnMetadata();
    _executionPrimed = true;
    _streamExhausted = false;
    _currentRow = null;
  }

  Row _decodeSingleRowView(Pointer<DdbValueView> valuesPtr, int columnCount) {
    if (_columnNames.length != columnCount) {
      throw StateError(
        'Column shape changed during statement execution '
        '(expected ${_columnNames.length}, got $columnCount)',
      );
    }
    final values = List<Object?>.filled(columnCount, null);
    for (var col = 0; col < columnCount; col++) {
      values[col] = _decodeValueView((valuesPtr + col).ref);
    }
    return Row._indexed(_columnNames, _columnIndex, values);
  }

  List<Row> _decodeRowViews(
    Pointer<DdbValueView> valuesPtr,
    int rowCount,
    int columnCount,
  ) {
    if (_columnNames.length != columnCount) {
      throw StateError(
        'Column shape changed during statement execution '
        '(expected ${_columnNames.length}, got $columnCount)',
      );
    }
    final rows = <Row>[];
    for (var rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      final start = rowIndex * columnCount;
      final values = List<Object?>.filled(columnCount, null);
      for (var col = 0; col < columnCount; col++) {
        values[col] = _decodeValueView((valuesPtr + start + col).ref);
      }
      rows.add(Row._indexed(_columnNames, _columnIndex, values));
    }
    return rows;
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
      final status =
          _bindings.stmtBindText(_stmtPtr!, index, utf8.cast<Uint8>(), byteLen);
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
    final data = malloc<Uint8>(value.length);
    try {
      data.asTypedList(value.length).setAll(0, value);
      final status =
          _bindings.stmtBindBlob(_stmtPtr!, index, data, value.length);
      if (status != ddbOk) _throwStatus(status, 'Failed to bind blob');
    } finally {
      malloc.free(data);
    }
  }

  /// Bind a decimal value at 1-based [index].
  void bindDecimal(int index, int scaled, int scale) {
    _checkNotDisposed();
    _invalidateExecution();
    final status = _bindings.stmtBindDecimal(_stmtPtr!, index, scaled, scale);
    if (status != ddbOk) _throwStatus(status, 'Failed to bind decimal');
  }

  /// Bind a [DateTime] (converted to UTC microseconds) at 1-based [index].
  void bindDateTime(int index, DateTime value) {
    _checkNotDisposed();
    _invalidateExecution();
    final micros = value.toUtc().microsecondsSinceEpoch;
    final status = _bindings.stmtBindTimestampMicros(_stmtPtr!, index, micros);
    if (status != ddbOk) _throwStatus(status, 'Failed to bind timestamp');
  }

  /// Bind a UUID value at 1-based [index].
  ///
  /// [bytes] must be exactly 16 bytes in canonical UUID byte order; throws
  /// [ArgumentError] otherwise.
  void bindUuid(int index, Uint8List bytes) {
    _checkNotDisposed();
    _invalidateExecution();
    if (bytes.length != 16) {
      throw ArgumentError(
          'UUID bytes must be exactly 16, got ${bytes.length}');
    }
    final data = malloc<Uint8>(16);
    try {
      data.asTypedList(16).setAll(0, bytes);
      final status = _bindings.stmtBindUuid(_stmtPtr!, index, data);
      if (status != ddbOk) _throwStatus(status, 'Failed to bind UUID');
    } finally {
      malloc.free(data);
    }
  }

  /// Polymorphic bind: dispatches to the typed method based on [value]'s type.
  void bind(int index, Object? value) {
    if (value == null) return bindNull(index);
    if (value is int) return bindInt64(index, value);
    if (value is bool) return bindBool(index, value);
    if (value is double) return bindFloat64(index, value);
    if (value is String) return bindText(index, value);
    // UuidValue must be checked before Uint8List so bare Uint8List stays BLOB.
    if (value is UuidValue) return bindUuid(index, value.bytes);
    if (value is Uint8List) return bindBlob(index, value);
    if (value is DateTime) return bindDateTime(index, value);
    if (value is DecimalValue)
      return bindDecimal(index, value.scaled, value.scale);
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

  /// The column names for the current result set.
  ///
  /// Returns the cached unmodifiable list directly — no wrapper allocation.
  List<String> get columnNames => _columnNames;

  // ---------------------------------------------------------------------------
  // Execution API
  // ---------------------------------------------------------------------------

  /// Execute and return all result rows.
  ///
  /// Always re-executes from the beginning (implicit reset + step loop).
  List<Row> query() {
    _checkNotDisposed();
    _invalidateExecution();
    _primeStreamingExecution();
    const internalPageSize = 256;
    final rows = <Row>[];
    while (true) {
      final page = nextPage(internalPageSize);
      rows.addAll(page.rows);
      if (page.isLast) {
        break;
      }
    }
    return UnmodifiableListView(rows); // D8: zero-copy wrap
  }

  /// Execute a DML statement and return the number of affected rows.
  ///
  /// For reuse, call [reset] and re-bind before calling [execute] again.
  int execute() {
    _checkNotDisposed();
    _invalidateExecution();
    _nativeReset();

    final outHasRow = calloc<Uint8>();
    try {
      while (true) {
        final status = _bindings.stmtStep(_stmtPtr!, outHasRow);
        if (status != ddbOk)
          _throwStatus(status, 'Failed to execute statement');
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

  /// Execute a one-column INT64 batch in one native call.
  int executeBatchInt64(List<int> values) {
    _checkNotDisposed();
    _invalidateExecution();
    if (values.isEmpty) {
      _affectedRows = 0;
      return 0;
    }

    final valuesPtr = malloc<Int64>(values.length);
    final outAffected = calloc<Uint64>();
    try {
      for (var i = 0; i < values.length; i++) {
        valuesPtr[i] = values[i];
      }
      final status = _bindings.stmtExecuteBatchI64(
        _stmtPtr!,
        values.length,
        valuesPtr,
        outAffected,
      );
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to execute INT64 batch');
      }
      _affectedRows = outAffected.value;
      return _affectedRows;
    } finally {
      malloc.free(valuesPtr);
      calloc.free(outAffected);
    }
  }

  /// Execute a `(INT64, TEXT, FLOAT64)` batch in one native call.
  int executeBatchI64TextF64(List<(int, String, double)> rows) {
    _checkNotDisposed();
    _invalidateExecution();
    if (rows.isEmpty) {
      _affectedRows = 0;
      return 0;
    }

    final valuesI64 = malloc<Int64>(rows.length);
    final valuesTextPtrs = malloc<Pointer<Utf8>>(rows.length);
    final valuesTextLens = malloc<IntPtr>(rows.length);
    final valuesF64 = malloc<Double>(rows.length);
    final outAffected = calloc<Uint64>();
    final allocatedText = <Pointer<Utf8>>[];
    try {
      for (var i = 0; i < rows.length; i++) {
        final row = rows[i];
        valuesI64[i] = row.$1;
        valuesF64[i] = row.$3;
        final text = row.$2.toNativeUtf8();
        allocatedText.add(text);
        valuesTextPtrs[i] = text;
        valuesTextLens[i] = text.length;
      }
      final status = _bindings.stmtExecuteBatchI64TextF64(
        _stmtPtr!,
        rows.length,
        valuesI64,
        valuesTextPtrs,
        valuesTextLens,
        valuesF64,
        outAffected,
      );
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to execute INT64/TEXT/FLOAT64 batch');
      }
      _affectedRows = outAffected.value;
      return _affectedRows;
    } finally {
      for (final ptr in allocatedText) {
        calloc.free(ptr);
      }
      malloc.free(valuesI64);
      malloc.free(valuesTextPtrs);
      malloc.free(valuesTextLens);
      malloc.free(valuesF64);
      calloc.free(outAffected);
    }
  }

  /// Execute a typed batch in one native call using an `i`/`t`/`f` signature.
  ///
  /// Sizes are computed upfront from [signature] and [rows].length so C
  /// buffers are allocated once and written directly (single-pass, no
  /// intermediate Dart lists). Uses [malloc] for write-only buffers (D5/D6).
  int executeBatchTyped(String signature, List<List<Object?>> rows) {
    _checkNotDisposed();
    _invalidateExecution();
    if (signature.isEmpty) {
      throw ArgumentError.value(signature, 'signature', 'must not be empty');
    }
    if (rows.isEmpty) {
      _affectedRows = 0;
      return 0;
    }

    final sig = signature.codeUnits;

    // Validate signature characters and compute per-type totals upfront.
    var iPerRow = 0, tPerRow = 0, fPerRow = 0;
    for (var col = 0; col < sig.length; col++) {
      switch (sig[col]) {
        case 0x69:
          iPerRow++;
          break;
        case 0x74:
          tPerRow++;
          break;
        case 0x66:
          fPerRow++;
          break;
        default:
          throw ArgumentError.value(
            signature,
            'signature',
            'contains unsupported type at column $col: '
                '${String.fromCharCode(sig[col])}',
          );
      }
    }

    final iTotal = rows.length * iPerRow;
    final tTotal = rows.length * tPerRow;
    final fTotal = rows.length * fPerRow;

    final signaturePtr = signature.toNativeUtf8();
    Pointer<Int64> valuesI64 = nullptr.cast<Int64>();
    Pointer<Double> valuesF64 = nullptr.cast<Double>();
    Pointer<Pointer<Utf8>> valuesTextPtrs = nullptr.cast<Pointer<Utf8>>();
    Pointer<IntPtr> valuesTextLens = nullptr.cast<IntPtr>();
    final outAffected = calloc<Uint64>();
    final allocatedText = <Pointer<Utf8>>[];
    try {
      if (iTotal > 0) valuesI64 = malloc<Int64>(iTotal);
      if (fTotal > 0) valuesF64 = malloc<Double>(fTotal);
      if (tTotal > 0) {
        valuesTextPtrs = malloc<Pointer<Utf8>>(tTotal);
        valuesTextLens = malloc<IntPtr>(tTotal);
      }

      // Single pass: write directly into C buffers.
      var iIdx = 0, tIdx = 0, fIdx = 0;
      for (var rowIndex = 0; rowIndex < rows.length; rowIndex++) {
        final row = rows[rowIndex];
        if (row.length != sig.length) {
          throw ArgumentError.value(
            row,
            'rows[$rowIndex]',
            'expected ${sig.length} values, got ${row.length}',
          );
        }
        for (var col = 0; col < sig.length; col++) {
          final value = row[col];
          switch (sig[col]) {
            case 0x69:
              if (value is! int) {
                throw ArgumentError.value(
                  value,
                  'rows[$rowIndex][$col]',
                  'expected int for signature column i',
                );
              }
              valuesI64[iIdx++] = value;
              break;
            case 0x74:
              if (value is! String) {
                throw ArgumentError.value(
                  value,
                  'rows[$rowIndex][$col]',
                  'expected String for signature column t',
                );
              }
              final text = value.toNativeUtf8();
              allocatedText.add(text);
              valuesTextPtrs[tIdx] = text;
              valuesTextLens[tIdx] = text.length;
              tIdx++;
              break;
            case 0x66:
              if (value is! num) {
                throw ArgumentError.value(
                  value,
                  'rows[$rowIndex][$col]',
                  'expected num for signature column f',
                );
              }
              valuesF64[fIdx++] = value.toDouble();
              break;
            default:
              throw StateError('Unsupported signature code ${sig[col]}');
          }
        }
      }

      final status = _bindings.stmtExecuteBatchTyped(
        _stmtPtr!,
        rows.length,
        signaturePtr,
        valuesI64,
        valuesF64,
        valuesTextPtrs,
        valuesTextLens,
        outAffected,
      );
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to execute typed batch');
      }
      _affectedRows = outAffected.value;
      return _affectedRows;
    } finally {
      for (final ptr in allocatedText) {
        calloc.free(ptr);
      }
      if (iTotal > 0) malloc.free(valuesI64);
      if (fTotal > 0) malloc.free(valuesF64);
      if (tTotal > 0) {
        malloc.free(valuesTextPtrs);
        malloc.free(valuesTextLens);
      }
      calloc.free(signaturePtr);
      calloc.free(outAffected);
    }
  }

  /// Reset, bind first parameter as INT64, execute, and return affected rows.
  int rebindInt64Execute(int value) {
    _checkNotDisposed();
    _invalidateExecution();
    final outAffected = calloc<Uint64>();
    try {
      final status =
          _bindings.stmtRebindInt64Execute(_stmtPtr!, value, outAffected);
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to rebind INT64 and execute');
      }
      _affectedRows = outAffected.value;
      return _affectedRows;
    } finally {
      calloc.free(outAffected);
    }
  }

  /// Reset, bind `(TEXT, INT64)`, execute, and return affected rows.
  int rebindTextInt64Execute(String text, int value) {
    _checkNotDisposed();
    _invalidateExecution();
    final outAffected = calloc<Uint64>();
    final textPtr = text.toNativeUtf8();
    try {
      final status = _bindings.stmtRebindTextInt64Execute(
        _stmtPtr!,
        textPtr,
        textPtr.length,
        value,
        outAffected,
      );
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to rebind TEXT/INT64 and execute');
      }
      _affectedRows = outAffected.value;
      return _affectedRows;
    } finally {
      calloc.free(textPtr);
      calloc.free(outAffected);
    }
  }

  /// Reset, bind `(INT64, TEXT)`, execute, and return affected rows.
  int rebindInt64TextExecute(int value, String text) {
    _checkNotDisposed();
    _invalidateExecution();
    final outAffected = calloc<Uint64>();
    final textPtr = text.toNativeUtf8();
    try {
      final status = _bindings.stmtRebindInt64TextExecute(
        _stmtPtr!,
        value,
        textPtr,
        textPtr.length,
        outAffected,
      );
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to rebind INT64/TEXT and execute');
      }
      _affectedRows = outAffected.value;
      return _affectedRows;
    } finally {
      calloc.free(textPtr);
      calloc.free(outAffected);
    }
  }

  // ---------------------------------------------------------------------------
  // Fused bind+step helpers
  // ---------------------------------------------------------------------------

  /// Bind INT64 at [index], then step one row in a single FFI call.
  bool bindInt64Step(int index, int value) {
    _checkNotDisposed();
    _invalidateExecution();

    final outValues = calloc<Pointer<DdbValueView>>();
    final outColumns = calloc<IntPtr>();
    final outHasRow = calloc<Uint8>();
    try {
      final status = _bindings.stmtBindInt64StepRowView(
        _stmtPtr!,
        index,
        value,
        outValues,
        outColumns,
        outHasRow,
      );
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to bind INT64 and step');
      }

      _loadColumnMetadata();
      _executionPrimed = true;

      if (outHasRow.value == 0) {
        _streamExhausted = true;
        _currentRow = null;
        return false;
      }
      _currentRow = _decodeSingleRowView(outValues.value, outColumns.value);
      return true;
    } finally {
      calloc.free(outValues);
      calloc.free(outColumns);
      calloc.free(outHasRow);
    }
  }

  /// Bind INT64 at [index], then step one `(INT64, TEXT, FLOAT64)` row in a
  /// single FFI call. Returns the decoded triple or `null` when exhausted.
  (int, String, double)? bindInt64StepI64TextF64(int index, int value) {
    _checkNotDisposed();
    _invalidateExecution();

    final outInt64 = calloc<Int64>();
    final outTextData = calloc<Pointer<Uint8>>();
    final outTextLen = calloc<IntPtr>();
    final outFloat64 = calloc<Double>();
    final outHasRow = calloc<Uint8>();
    try {
      final status = _bindings.stmtBindInt64StepI64TextF64(
        _stmtPtr!,
        index,
        value,
        outInt64,
        outTextData,
        outTextLen,
        outFloat64,
        outHasRow,
      );
      if (status != ddbOk) {
        _throwStatus(status, 'Failed to bind INT64 and step I64/Text/F64');
      }
      if (outHasRow.value == 0) {
        return null;
      }
      final textData = outTextData.value;
      final textLen = outTextLen.value;
      final text = textData == nullptr || textLen == 0
          ? ''
          : textData.cast<Utf8>().toDartString(length: textLen);
      return (outInt64.value, text, outFloat64.value);
    } finally {
      calloc.free(outInt64);
      calloc.free(outTextData);
      calloc.free(outTextLen);
      calloc.free(outFloat64);
      calloc.free(outHasRow);
    }
  }

  // ---------------------------------------------------------------------------
  // Streaming / pagination API
  // ---------------------------------------------------------------------------

  /// Advance the row cursor, executing if needed.  Returns `true` while rows
  /// remain; `false` when exhausted.
  bool step() {
    _checkNotDisposed();
    _primeStreamingExecution();
    if (_streamExhausted) {
      _currentRow = null;
      return false;
    }

    final outValues = calloc<Pointer<DdbValueView>>();
    final outColumns = calloc<IntPtr>();
    final outHasRow = calloc<Uint8>();
    try {
      final status = _bindings.stmtStepRowView(
          _stmtPtr!, outValues, outColumns, outHasRow);
      if (status != ddbOk) _throwStatus(status, 'Failed to step statement');
      if (outHasRow.value == 0) {
        _streamExhausted = true;
        _currentRow = null;
        return false;
      }
      _currentRow = _decodeSingleRowView(outValues.value, outColumns.value);
      return true;
    } finally {
      calloc.free(outValues);
      calloc.free(outColumns);
      calloc.free(outHasRow);
    }
  }

  /// Return the current row after a successful [step] call.
  Row readRow() {
    _checkNotDisposed();
    if (_currentRow == null) {
      throw StateError('No current row; call step() first');
    }
    return _currentRow!;
  }

  /// Return the next [pageSize] rows, executing if needed.
  ResultPage nextPage(int pageSize) {
    if (pageSize <= 0) throw RangeError.range(pageSize, 1, null, 'pageSize');
    _checkNotDisposed();
    _primeStreamingExecution();
    _currentRow = null; // nextPage invalidates any step() row.

    final outValues = calloc<Pointer<DdbValueView>>();
    final outRows = calloc<IntPtr>();
    final outColumns = calloc<IntPtr>();
    try {
      final status = _bindings.stmtFetchRowViews(
        _stmtPtr!,
        0,
        pageSize,
        outValues,
        outRows,
        outColumns,
      );
      if (status != ddbOk) _throwStatus(status, 'Failed to fetch page');

      final fetchedRows = outRows.value;
      final fetchedColumns = outColumns.value;
      final pageRows = fetchedRows == 0
          ? const <Row>[]
          : _decodeRowViews(outValues.value, fetchedRows, fetchedColumns);
      final isLast = fetchedRows < pageSize;
      _streamExhausted = isLast;

      return ResultPage(
        List.unmodifiable(_columnNames),
        List<Row>.unmodifiable(pageRows),
        isLast,
      );
    } finally {
      calloc.free(outValues);
      calloc.free(outRows);
      calloc.free(outColumns);
    }
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

  /// Reset and clear bindings in one step without re-fetching column metadata.
  ///
  /// Equivalent to [reset] + [clearBindings] but avoids the redundant metadata
  /// reload. Column names are cached across resets (D7) so this is safe.
  /// Used by the prepared-statement cache inside [Database.prepare].
  void resetForReuse() {
    _checkNotDisposed();
    _nativeReset();
    final status = _bindings.stmtClearBindings(_stmtPtr!);
    if (status != ddbOk) _throwStatus(status, 'Failed to clear bindings');
    _invalidateExecution();
  }

  /// Returns a [Stream] that yields each result row.
  ///
  /// Internally calls [nextPage] with [pageSize] rows per FFI round-trip.
  /// Starting a new iteration after the stream completes requires calling
  /// [reset] first. The stream is single-subscription.
  ///
  /// [pageSize] must be positive; 0 or negative values throw [ArgumentError].
  Stream<Row> rows({int pageSize = 256}) async* {
    if (pageSize <= 0) {
      throw ArgumentError.value(pageSize, 'pageSize', 'must be > 0');
    }
    while (true) {
      final page = nextPage(pageSize);
      for (final row in page.rows) {
        yield row;
      }
      if (page.isLast) return;
    }
  }

  /// Release the native statement handle.
  ///
  /// Idempotent — subsequent calls are safe no-ops. The Finalizer is detached
  /// before [stmtFree] runs to prevent any double-free if the GC fires
  /// concurrently.
  void dispose() {
    if (_disposed) return;
    _disposed = true;
    _finalizer.detach(this); // D9: prevent double-free
    _columnMetadataLoaded = false; // D7: reset so re-prepare can reload
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

Object? _decodeValueView(DdbValueView value) {
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
      for (var i = 0; i < 16; i++) {
        bytes[i] = value.uuidBytes[i];
      }
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
