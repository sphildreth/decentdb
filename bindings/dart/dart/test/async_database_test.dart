import 'dart:async';

import 'package:decentdb/decentdb.dart';
import 'package:test/test.dart';

import 'test_utils.dart';

void main() {
  late String libPath;

  setUpAll(() {
    libPath = findNativeLib();
  });

  test('executes, prepares, and queries', () async {
    final db = await AsyncDatabase.open(':memory:', libraryPath: libPath);
    addTearDown(db.close);

    await db.execute(
      'CREATE TABLE t (id INT64 PRIMARY KEY, name TEXT)',
    );

    // Insert via prepared statement.
    final ins = await db.prepare(r'INSERT INTO t VALUES ($1, $2)');
    await ins.bindAll([1, 'alice']);
    await ins.execute();
    await ins.reset();
    await ins.bindAll([2, 'bob']);
    await ins.execute();
    await ins.dispose();

    // Select via prepared statement.
    final sel = await db.prepare('SELECT id, name FROM t ORDER BY id');
    final rows = await sel.query();
    await sel.dispose();

    expect(rows.length, 2);
    expect(rows[0]['id'], 1);
    expect(rows[0]['name'], 'alice');
    expect(rows[1]['id'], 2);
    expect(rows[1]['name'], 'bob');
  });

  test('main isolate remains responsive during long query', () async {
    final db = await AsyncDatabase.open(':memory:', libraryPath: libPath);
    addTearDown(db.close);

    // Build enough rows so the worker is busy for a moment without making the
    // binding smoke suite sensitive to parallel pre-commit load.
    await db.execute('CREATE TABLE t (id INT64 PRIMARY KEY, v TEXT)');
    final ins = await db.prepare(r'INSERT INTO t VALUES ($1, $2)');
    const rowCount = 2000;
    for (var i = 0; i < rowCount; i++) {
      await ins.bindAll([i, 'x' * 64]);
      await ins.execute();
      await ins.reset();
    }
    await ins.dispose();

    // Fire a long query on the async db and simultaneously measure main-isolate
    // responsiveness with a Timer.
    var timerFires = 0;
    final timer =
        Timer.periodic(const Duration(milliseconds: 1), (_) => timerFires++);

    final sel =
        await db.prepare('SELECT id FROM t WHERE v LIKE \'x%\'');
    final rows = await sel.query();
    await sel.dispose();
    timer.cancel();

    expect(rows.length, rowCount);
    // The timer should have fired at least a few times while the query was
    // running, proving the main isolate remained responsive.
    expect(timerFires, greaterThanOrEqualTo(1));
  });

  test('close terminates the worker and returns cleanly', () async {
    final db = await AsyncDatabase.open(':memory:', libraryPath: libPath);
    await db.execute('CREATE TABLE t (id INT64 PRIMARY KEY)');
    // Should complete without error.
    await expectLater(db.close(), completes);
    // Second close is a no-op.
    await expectLater(db.close(), completes);
  });

  test('operations after close throw AsyncDatabaseClosed', () async {
    final db = await AsyncDatabase.open(':memory:', libraryPath: libPath);
    await db.close();

    expect(
      () => db.execute('SELECT 1'),
      throwsA(isA<AsyncDatabaseClosed>()),
    );
    expect(
      () => db.prepare('SELECT 1'),
      throwsA(isA<AsyncDatabaseClosed>()),
    );
  });
}
