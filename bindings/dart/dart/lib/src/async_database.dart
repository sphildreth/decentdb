import 'dart:async';
import 'dart:isolate';

import 'database.dart';
import 'errors.dart';
import 'statement.dart';
import 'types.dart';

/// Thrown when an operation is attempted on a closed or crashed [AsyncDatabase].
final class AsyncDatabaseClosed implements Exception {
  const AsyncDatabaseClosed([this.message = 'database has been closed']);

  final String message;

  @override
  String toString() => 'AsyncDatabaseClosed: $message';
}

// ---------------------------------------------------------------------------
// Request messages (sent from main isolate → worker isolate)
// ---------------------------------------------------------------------------

sealed class _Req {
  const _Req(this.id);
  final int id;
}

class _OpenReq extends _Req {
  const _OpenReq(super.id, this.path, this.libraryPath, this.cap);
  final String path;
  final String? libraryPath;
  final int cap;
}

class _ExecuteReq extends _Req {
  const _ExecuteReq(super.id, this.sql, this.params);
  final String sql;
  final List<Object?> params;
}

class _PrepareReq extends _Req {
  const _PrepareReq(super.id, this.sql);
  final String sql;
}

class _StmtBindAllReq extends _Req {
  const _StmtBindAllReq(super.id, this.stmtId, this.params);
  final int stmtId;
  final List<Object?> params;
}

class _StmtExecuteReq extends _Req {
  const _StmtExecuteReq(super.id, this.stmtId);
  final int stmtId;
}

class _StmtQueryReq extends _Req {
  const _StmtQueryReq(super.id, this.stmtId);
  final int stmtId;
}

class _StmtResetReq extends _Req {
  const _StmtResetReq(super.id, this.stmtId);
  final int stmtId;
}

class _StmtDisposeReq extends _Req {
  const _StmtDisposeReq(super.id, this.stmtId);
  final int stmtId;
}

class _CloseReq extends _Req {
  const _CloseReq(super.id);
}

// ---------------------------------------------------------------------------
// Worker isolate entry point
// ---------------------------------------------------------------------------

// Response wire format: 3-element List<dynamic>:
//   [id, true,  value]         — success
//   [id, false, errorMessage]  — failure

/// Top-level entry function for the worker isolate.
void _workerEntry(List<dynamic> args) {
  final handshakeSend = args[0] as SendPort;
  final responseSend = args[1] as SendPort;

  Database? db;
  final stmts = <int, Statement>{};
  var nextStmtId = 0;

  final recv = ReceivePort();
  handshakeSend.send(recv.sendPort); // handshake

  recv.listen((msg) {
    if (msg is! _Req) {
      recv.close();
      return;
    }

    try {
      switch (msg) {
        case _OpenReq(
          id: final id,
          path: final p,
          libraryPath: final lp,
          cap: final _,
        ):
          // Disable the Database-level stmt cache in the worker.  Each
          // AsyncStatement gets its own native handle via nextStmtId; caching
          // at the Database layer would alias handles for the same SQL.
          db = Database.open(p, libraryPath: lp, stmtCacheCapacity: 0);
          responseSend.send([id, true, null]);

        case _ExecuteReq(id: final id, sql: final sql, params: final params):
          final n = params.isEmpty
              ? db!.execute(sql)
              : db!.executeWithParams(sql, params);
          responseSend.send([id, true, n]);

        case _PrepareReq(id: final id, sql: final sql):
          // prepare() on a stmtCacheCapacity==0 db creates a fresh Statement.
          final stmt = db!.prepare(sql);
          final sid = nextStmtId++;
          stmts[sid] = stmt;
          responseSend.send([id, true, sid]);

        case _StmtBindAllReq(
          id: final id,
          stmtId: final sid,
          params: final params
        ):
          stmts[sid]!.bindAll(params);
          responseSend.send([id, true, null]);

        case _StmtExecuteReq(id: final id, stmtId: final sid):
          final n = stmts[sid]!.execute();
          responseSend.send([id, true, n]);

        case _StmtQueryReq(id: final id, stmtId: final sid):
          // Row contains only sendable types (int/double/bool/String/Uint8List/
          // DecimalValue/DateTime — all deep-copy safely across isolates).
          final rows = stmts[sid]!.query();
          responseSend.send([id, true, rows]);

        case _StmtResetReq(id: final id, stmtId: final sid):
          stmts[sid]!.reset();
          stmts[sid]!.clearBindings();
          responseSend.send([id, true, null]);

        case _StmtDisposeReq(id: final id, stmtId: final sid):
          stmts.remove(sid)?.dispose();
          responseSend.send([id, true, null]);

        case _CloseReq(id: final id):
          for (final s in stmts.values) {
            if (!s.isDisposed) s.dispose();
          }
          stmts.clear();
          db?.close();
          db = null;
          responseSend.send([id, true, null]);
          recv.close();
      }
    } catch (e) {
      responseSend.send([msg.id, false, e.toString()]);
    }
  });
}

// ---------------------------------------------------------------------------
// AsyncDatabase
// ---------------------------------------------------------------------------

/// An [AsyncDatabase] wraps a synchronous [Database] in a dedicated worker
/// [Isolate], keeping all FFI calls off the caller's event loop.
///
/// Obtain via [AsyncDatabase.open].  Must be closed with [close] when done.
///
/// Example:
/// ```dart
/// final db = await AsyncDatabase.open(':memory:', libraryPath: libPath);
/// await db.execute('CREATE TABLE t (id INT64 PRIMARY KEY, name TEXT)');
/// final stmt = await db.prepare(r'INSERT INTO t VALUES ($1, $2)');
/// await stmt.bindAll([1, 'alice']);
/// await stmt.execute();
/// await stmt.reset();
/// await stmt.dispose();
/// await db.close();
/// ```
final class AsyncDatabase {
  AsyncDatabase._(
    this._workerPort,
    this._responsePort,
    this._isolate,
  );

  final SendPort _workerPort;
  final ReceivePort _responsePort;
  final Isolate _isolate;

  final Map<int, Completer<dynamic>> _pending = {};
  int _nextId = 0;
  bool _closed = false;

  // ---------------------------------------------------------------------------
  // Factory constructor
  // ---------------------------------------------------------------------------

  /// Open [path] asynchronously (or `:memory:` for in-memory).
  ///
  /// The worker isolate owns the native database handle; all FFI calls run
  /// in that isolate and do not block the caller's event loop.
  static Future<AsyncDatabase> open(
    String path, {
    String? libraryPath,
  }) async {
    final responsePort = ReceivePort();
    final handshakePort = ReceivePort();
    final exitPort = ReceivePort();
    final errorPort = ReceivePort();

    final isolate = await Isolate.spawn<List<dynamic>>(
      _workerEntry,
      [handshakePort.sendPort, responsePort.sendPort],
      onExit: exitPort.sendPort,
      onError: errorPort.sendPort,
      errorsAreFatal: false,
    );

    // Receive the worker's SendPort.
    final workerSendPort = await handshakePort.first as SendPort;
    handshakePort.close();

    final db = AsyncDatabase._(workerSendPort, responsePort, isolate);

    // Route response messages to pending completers.
    responsePort.listen((msg) {
      if (msg is! List || msg.length != 3) return;
      final id = msg[0] as int;
      final success = msg[1] as bool;
      final value = msg[2];
      final completer = db._pending.remove(id);
      if (completer == null) return;
      if (success) {
        completer.complete(value);
      } else {
        completer.completeError(
          DecentDbException(ErrorCode.internal, value as String? ?? 'unknown'),
        );
      }
    });

    // Clean up on unexpected isolate exit or crash.
    exitPort.listen((_) {
      db._drainOnClose();
      exitPort.close();
      errorPort.close();
    });
    errorPort.listen((_) {
      db._drainOnClose();
    });

    // Send the open request.
    await db._send(_OpenReq(db._nextId++, path, libraryPath, 0));

    return db;
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  Future<dynamic> _send(_Req req) {
    if (_closed) return Future.error(const AsyncDatabaseClosed());
    final completer = Completer<dynamic>();
    _pending[req.id] = completer;
    _workerPort.send(req);
    return completer.future;
  }

  void _drainOnClose() {
    if (_closed) return;
    _closed = true;
    const error = AsyncDatabaseClosed();
    for (final c in _pending.values) {
      if (!c.isCompleted) c.completeError(error);
    }
    _pending.clear();
    _responsePort.close();
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /// Execute [sql] and return the number of affected rows.
  Future<int> execute(String sql, [List<Object?> params = const []]) async {
    final result = await _send(_ExecuteReq(_nextId++, sql, params));
    return result as int;
  }

  /// Prepare [sql] and return an [AsyncStatement] backed by the worker isolate.
  ///
  /// The caller must call [AsyncStatement.dispose] when done to release the
  /// native handle in the worker isolate.
  Future<AsyncStatement> prepare(String sql) async {
    final stmtId = await _send(_PrepareReq(_nextId++, sql));
    return AsyncStatement._(this, stmtId as int);
  }

  /// Close the database and terminate the worker isolate.
  ///
  /// All in-flight requests that have not yet completed will throw
  /// [AsyncDatabaseClosed].  Calling [close] on an already-closed database
  /// is a no-op.
  Future<void> close() async {
    if (_closed) return;
    // Register the close completer before marking closed.
    final closeId = _nextId++;
    final completer = Completer<dynamic>();
    _pending[closeId] = completer;
    _workerPort.send(_CloseReq(closeId));

    try {
      await completer.future;
    } finally {
      _drainOnClose();
      _isolate.kill(priority: Isolate.immediate);
    }
  }
}

// ---------------------------------------------------------------------------
// AsyncStatement
// ---------------------------------------------------------------------------

/// An asynchronous prepared statement backed by the worker isolate of an
/// [AsyncDatabase].
///
/// Obtain via [AsyncDatabase.prepare].  Must be disposed with [dispose] when
/// done to release the native handle in the worker isolate.
final class AsyncStatement {
  AsyncStatement._(this._db, this._stmtId);

  final AsyncDatabase _db;
  final int _stmtId;

  /// Bind [params] positionally; uses the same dispatch as [Statement.bindAll].
  Future<void> bindAll(List<Object?> params) async {
    await _db._send(_StmtBindAllReq(_db._nextId++, _stmtId, params));
  }

  /// Execute a DML statement; returns the number of affected rows.
  Future<int> execute() async {
    final result =
        await _db._send(_StmtExecuteReq(_db._nextId++, _stmtId));
    return result as int;
  }

  /// Execute a SELECT statement and return all result rows.
  Future<List<Row>> query() async {
    final result =
        await _db._send(_StmtQueryReq(_db._nextId++, _stmtId));
    return (result as List).cast<Row>();
  }

  /// Reset the statement and clear all bindings for reuse.
  Future<void> reset() async {
    await _db._send(_StmtResetReq(_db._nextId++, _stmtId));
  }

  /// Release the native statement handle in the worker isolate.
  Future<void> dispose() async {
    await _db._send(_StmtDisposeReq(_db._nextId++, _stmtId));
  }
}
