import 'dart:ffi';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'native_bindings.dart';
import 'errors.dart';
import 'types.dart';

/// A row of query results with typed column access.
class Row {
  /// Column names.
  final List<String> columns;

  /// Column values as Dart objects.
  ///
  /// Values are: `null`, `int`, `bool`, `double`, `String`, `Uint8List`,
  /// `DateTime`, or `({int unscaled, int scale})` for decimals.
  final List<Object?> values;

  const Row(this.columns, this.values);

  /// Access a column value by name.
  Object? operator [](String name) {
    final idx = columns.indexOf(name);
    if (idx < 0) throw ArgumentError('Unknown column: $name');
    return values[idx];
  }

  /// Access a column value by 0-based index.
  Object? at(int index) => values[index];

  @override
  String toString() {
    final pairs = <String>[];
    for (var i = 0; i < columns.length; i++) {
      pairs.add('${columns[i]}: ${values[i]}');
    }
    return 'Row(${pairs.join(", ")})';
  }
}

/// A page of query results.
class ResultPage {
  /// Column names (same for all rows).
  final List<String> columns;

  /// Rows in this page.
  final List<Row> rows;

  /// Whether this is the last page (no more rows).
  final bool isLast;

  const ResultPage(this.columns, this.rows, this.isLast);
}

/// A prepared statement with parameter binding and result streaming.
///
/// Statements must be disposed when no longer needed to free native resources.
///
/// ## Lifecycle
///
/// ```dart
/// final stmt = db.prepare('SELECT * FROM users WHERE id = $1');
/// stmt.bindInt64(1, 42);
/// for (final row in stmt.query()) { ... }
/// stmt.dispose();
/// ```
///
/// ## Thread Safety
///
/// Statement handles are NOT safe for concurrent use. Use one statement per
/// isolate. The parent [Database] handle can be shared if access is serialized.
class Statement {
  final NativeBindings _bindings;
  final Pointer<DecentdbDb> _dbPtr;
  Pointer<DecentdbStmt>? _stmtPtr;
  List<String>? _columnNames;

  /// @nodoc — Internal constructor. Use [Database.prepare] instead.
  Statement.fromNative(this._bindings, this._dbPtr, this._stmtPtr);

  void _checkNotDisposed() {
    if (_stmtPtr == null || _stmtPtr == nullptr) {
      throw StateError('Statement has been disposed');
    }
  }

  void _checkResult(int rc) {
    if (rc != 0) {
      final errCode = _bindings.lastErrorCode(_dbPtr);
      final msgPtr = _bindings.lastErrorMessage(_dbPtr);
      final msg = msgPtr == nullptr ? 'Unknown error' : msgPtr.toDartString();
      throw DecentDbException(ErrorCode.fromCode(errCode), msg);
    }
  }

  // -------------------------------------------------------------------------
  // Bind parameters (1-based index matching $1..$N)
  // -------------------------------------------------------------------------

  /// Bind NULL to parameter at [index] (1-based).
  void bindNull(int index) {
    _checkNotDisposed();
    _checkResult(_bindings.bindNull(_stmtPtr!, index));
  }

  /// Bind an integer to parameter at [index] (1-based).
  void bindInt64(int index, int value) {
    _checkNotDisposed();
    _checkResult(_bindings.bindInt64(_stmtPtr!, index, value));
  }

  /// Bind a boolean to parameter at [index] (1-based).
  void bindBool(int index, bool value) {
    _checkNotDisposed();
    _checkResult(_bindings.bindBool(_stmtPtr!, index, value ? 1 : 0));
  }

  /// Bind a double to parameter at [index] (1-based).
  void bindFloat64(int index, double value) {
    _checkNotDisposed();
    _checkResult(_bindings.bindFloat64(_stmtPtr!, index, value));
  }

  /// Bind a text string to parameter at [index] (1-based).
  void bindText(int index, String value) {
    _checkNotDisposed();
    final utf8 = value.toNativeUtf8();
    try {
      _checkResult(_bindings.bindText(_stmtPtr!, index, utf8, utf8.length));
    } finally {
      calloc.free(utf8);
    }
  }

  /// Bind a blob to parameter at [index] (1-based).
  void bindBlob(int index, Uint8List value) {
    _checkNotDisposed();
    if (value.isEmpty) {
      final nullPtr = Pointer<Uint8>.fromAddress(0);
      _checkResult(_bindings.bindBlob(_stmtPtr!, index, nullPtr, 0));
      return;
    }
    final ptr = calloc<Uint8>(value.length);
    try {
      ptr.asTypedList(value.length).setAll(0, value);
      _checkResult(_bindings.bindBlob(_stmtPtr!, index, ptr, value.length));
    } finally {
      calloc.free(ptr);
    }
  }

  /// Bind a decimal value to parameter at [index] (1-based).
  ///
  /// The decimal is represented as [unscaled] * 10^-[scale].
  void bindDecimal(int index, int unscaled, int scale) {
    _checkNotDisposed();
    _checkResult(_bindings.bindDecimal(_stmtPtr!, index, unscaled, scale));
  }

  /// Bind a DateTime to parameter at [index] (1-based).
  ///
  /// The value is stored as microseconds since Unix epoch UTC.
  void bindDateTime(int index, DateTime value) {
    _checkNotDisposed();
    _checkResult(
      _bindings.bindDateTime(_stmtPtr!, index, value.microsecondsSinceEpoch),
    );
  }

  /// Bind a dynamic value to parameter at [index] (1-based).
  ///
  /// Accepts: `null`, `int`, `bool`, `double`, `String`, `Uint8List`, `DateTime`.
  void bind(int index, Object? value) {
    if (value == null) {
      bindNull(index);
    } else if (value is int) {
      bindInt64(index, value);
    } else if (value is bool) {
      bindBool(index, value);
    } else if (value is double) {
      bindFloat64(index, value);
    } else if (value is String) {
      bindText(index, value);
    } else if (value is Uint8List) {
      bindBlob(index, value);
    } else if (value is DateTime) {
      bindDateTime(index, value);
    } else {
      throw ArgumentError('Unsupported bind type: ${value.runtimeType}');
    }
  }

  /// Bind all parameters from a list (1-based indexing: params[0] → $1).
  void bindAll(List<Object?> params) {
    for (var i = 0; i < params.length; i++) {
      bind(i + 1, params[i]);
    }
  }

  // -------------------------------------------------------------------------
  // Column metadata (available after prepare, before first step)
  // -------------------------------------------------------------------------

  /// Number of result columns. Returns 0 for non-SELECT statements.
  int get columnCount {
    _checkNotDisposed();
    return _bindings.columnCount(_stmtPtr!);
  }

  /// Column names for the result set.
  List<String> get columnNames {
    if (_columnNames != null) return _columnNames!;
    _checkNotDisposed();
    final count = columnCount;
    _columnNames = List.generate(count, (i) {
      final ptr = _bindings.columnName(_stmtPtr!, i);
      return ptr == nullptr ? 'column$i' : ptr.toDartString();
    });
    return _columnNames!;
  }

  // -------------------------------------------------------------------------
  // Stepping and row reading
  // -------------------------------------------------------------------------

  /// Step to the next row. Returns `true` if a row is available.
  ///
  /// Returns `false` when no more rows (SELECT done or DML executed).
  /// Throws [DecentDbException] on error.
  bool step() {
    _checkNotDisposed();
    final rc = _bindings.step(_stmtPtr!);
    if (rc < 0) {
      final errCode = _bindings.lastErrorCode(_dbPtr);
      final msgPtr = _bindings.lastErrorMessage(_dbPtr);
      final msg = msgPtr == nullptr ? 'Step error' : msgPtr.toDartString();
      throw DecentDbException(ErrorCode.fromCode(errCode), msg);
    }
    return rc == 1;
  }

  /// Read the current row's values (call after [step] returns true).
  Row readRow() {
    _checkNotDisposed();
    final names = columnNames;
    final count = names.length;
    final values = List<Object?>.filled(count, null);

    for (var i = 0; i < count; i++) {
      if (_bindings.columnIsNull(_stmtPtr!, i) != 0) {
        values[i] = null;
        continue;
      }
      final type = ColumnType.fromCode(_bindings.columnType(_stmtPtr!, i));
      switch (type) {
        case ColumnType.vkInt64:
          values[i] = _bindings.columnInt64(_stmtPtr!, i);
          break;
        case ColumnType.vkBool:
          values[i] = _bindings.columnInt64(_stmtPtr!, i) != 0;
          break;
        case ColumnType.vkFloat64:
          values[i] = _bindings.columnFloat64(_stmtPtr!, i);
          break;
        case ColumnType.vkText:
          final lenPtr = calloc<Int32>();
          try {
            final ptr = _bindings.columnText(_stmtPtr!, i, lenPtr);
            if (ptr == nullptr) {
              values[i] = '';
            } else {
              final len = lenPtr.value;
              values[i] = ptr.toDartString(length: len);
            }
          } finally {
            calloc.free(lenPtr);
          }
          break;
        case ColumnType.vkBlob:
          final lenPtr = calloc<Int32>();
          try {
            final ptr = _bindings.columnBlob(_stmtPtr!, i, lenPtr);
            if (ptr == nullptr) {
              values[i] = Uint8List(0);
            } else {
              final len = lenPtr.value;
              values[i] = Uint8List.fromList(ptr.asTypedList(len));
            }
          } finally {
            calloc.free(lenPtr);
          }
          break;
        case ColumnType.vkDecimal:
          final unscaled = _bindings.columnDecimalUnscaled(_stmtPtr!, i);
          final scale = _bindings.columnDecimalScale(_stmtPtr!, i);
          values[i] = (unscaled: unscaled, scale: scale);
          break;
        case ColumnType.vkDateTime:
          final micros = _bindings.columnDateTime(_stmtPtr!, i);
          values[i] = DateTime.fromMicrosecondsSinceEpoch(micros, isUtc: true);
          break;
        case ColumnType.vkNull:
          values[i] = null;
          break;
      }
    }
    return Row(names, values);
  }

  // -------------------------------------------------------------------------
  // Convenience query methods
  // -------------------------------------------------------------------------

  /// Execute and return all result rows.
  ///
  /// Resets the statement first. For large results, prefer [queryCursor].
  List<Row> query() {
    _checkNotDisposed();
    reset();
    final rows = <Row>[];
    while (step()) {
      rows.add(readRow());
    }
    return rows;
  }

  /// Execute and return results one page at a time.
  ///
  /// Each call returns up to [pageSize] rows. Returns a [ResultPage] with
  /// `isLast = true` when no more rows remain.
  ///
  /// The statement is NOT automatically reset; call [reset] to re-execute.
  ResultPage nextPage(int pageSize) {
    _checkNotDisposed();
    final names = columnNames;
    final rows = <Row>[];
    var done = false;

    for (var i = 0; i < pageSize; i++) {
      if (!step()) {
        done = true;
        break;
      }
      rows.add(readRow());
    }
    return ResultPage(names, rows, done);
  }

  /// Execute a DML statement and return the number of affected rows.
  int execute() {
    _checkNotDisposed();
    step(); // Execute the statement
    return _bindings.rowsAffected(_stmtPtr!);
  }

  /// Reset the statement for re-execution with the same or new bindings.
  void reset() {
    _checkNotDisposed();
    _checkResult(_bindings.reset(_stmtPtr!));
  }

  /// Clear all parameter bindings (set to NULL).
  void clearBindings() {
    _checkNotDisposed();
    _checkResult(_bindings.clearBindings(_stmtPtr!));
  }

  /// Release native resources. The statement cannot be used after this call.
  void dispose() {
    if (_stmtPtr != null && _stmtPtr != nullptr) {
      _bindings.finalize(_stmtPtr!);
      _stmtPtr = null;
    }
  }
}
