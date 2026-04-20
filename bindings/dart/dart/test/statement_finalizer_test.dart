import 'package:decentdb/decentdb.dart';
import 'package:test/test.dart';

import 'test_utils.dart';

void main() {
  late String libPath;

  setUpAll(() {
    libPath = findNativeLib();
  });

  // D9: Finalizer prevents double-free when caller forgets dispose()
  test('dispose then drop is safe and idempotent (D9)', () {
    final db = Database.open(':memory:', libraryPath: libPath);
    try {
      db.execute('CREATE TABLE t (id INT64 PRIMARY KEY)');
      final stmt = db.prepare('SELECT id FROM t');
      stmt.dispose();
      // Calling dispose() a second time must be a no-op (not crash).
      stmt.dispose();
    } finally {
      db.close();
    }
  });

  test('isDisposed reflects statement lifecycle (D9)', () {
    final db = Database.open(':memory:', libraryPath: libPath);
    try {
      db.execute('CREATE TABLE t (id INT64 PRIMARY KEY)');
      final stmt = db.prepare('SELECT id FROM t');
      expect(stmt.isDisposed, isFalse);
      stmt.dispose();
      expect(stmt.isDisposed, isTrue);
    } finally {
      db.close();
    }
  });

  test('orphaned statement does not crash the process (D9)', () async {
    final db = Database.open(':memory:', libraryPath: libPath);
    try {
      db.execute('CREATE TABLE t (id INT64 PRIMARY KEY)');
      // Create and orphan a statement — the Finalizer should handle cleanup.
      // We cannot deterministically trigger GC, but we verify no crash occurs
      // after the DB is closed with a dangling prepare.
      db.prepare('SELECT id FROM t');
      // Do NOT call stmt.dispose(). DB.close() calls clearStmtCache first
      // so the underlying DB is freed safely.
    } finally {
      db.close();
    }
    // If we reach here without a crash, the test passes.
  });
}
