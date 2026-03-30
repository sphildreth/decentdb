import 'dart:io';

import 'package:decentdb/decentdb.dart';
import 'package:test/test.dart';

String findNativeLib() {
  final envPath = Platform.environment['DECENTDB_NATIVE_LIB'];
  if (envPath != null && envPath.isNotEmpty) {
    return envPath;
  }

  var dir = Directory.current;
  for (var i = 0; i < 10; i++) {
    for (final candidateName in [
      'target/debug/libdecentdb.so',
      'target/debug/libdecentdb.dylib',
      'target/debug/decentdb.dll',
    ]) {
      final candidate = File('${dir.path}/$candidateName');
      if (candidate.existsSync()) {
        return candidate.path;
      }
    }
    dir = dir.parent;
  }

  throw StateError(
    'Cannot find DecentDB native library. '
    'Set DECENTDB_NATIVE_LIB or run from the repo root after `cargo build -p decentdb`.',
  );
}

void main() {
  late Database db;
  late String libPath;

  setUpAll(() {
    libPath = findNativeLib();
  });

  setUp(() {
    db = Database.open(':memory:', libraryPath: libPath);
  });

  tearDown(() {
    db.close();
  });

  test('executeBatchInt64 inserts all values', () {
    db.execute('CREATE TABLE t (id INT64 PRIMARY KEY)');
    final stmt = db.prepare(r'INSERT INTO t (id) VALUES ($1)');
    try {
      final affected = stmt.executeBatchInt64([1, 2, 3, 4, 5]);
      expect(affected, 5);
    } finally {
      stmt.dispose();
    }

    final ids = db.query('SELECT id FROM t ORDER BY id').map((r) => r['id']);
    expect(ids, orderedEquals([1, 2, 3, 4, 5]));
  });

  test('executeBatchI64TextF64 inserts typed triples', () {
    db.execute(
        'CREATE TABLE t (id INT64 PRIMARY KEY, name TEXT, score FLOAT64)');
    final stmt = db.prepare(r'INSERT INTO t VALUES ($1, $2, $3)');
    try {
      final affected = stmt.executeBatchI64TextF64([
        (10, 'ten', 10.0),
        (11, 'eleven', 11.5),
      ]);
      expect(affected, 2);
    } finally {
      stmt.dispose();
    }

    final rows = db.query('SELECT id, name, score FROM t ORDER BY id');
    expect(rows.map((r) => r['id']).toList(), [10, 11]);
    expect(rows.map((r) => r['name']).toList(), ['ten', 'eleven']);
    expect(rows.map((r) => r['score']).toList(), [10.0, 11.5]);
  });

  test('executeBatchTyped validates shape and inserts mixed rows', () {
    db.execute(
      'CREATE TABLE t (id INT64 PRIMARY KEY, name TEXT, score FLOAT64, qty INT64)',
    );
    final stmt = db.prepare(r'INSERT INTO t VALUES ($1, $2, $3, $4)');
    try {
      final affected = stmt.executeBatchTyped('itfi', [
        [1, 'a', 1.25, 10],
        [2, 'b', 2.5, 20],
      ]);
      expect(affected, 2);

      expect(
        () => stmt.executeBatchTyped('it', [
          [1],
        ]),
        throwsArgumentError,
      );
      expect(
        () => stmt.executeBatchTyped('ix', [
          [1, 'bad'],
        ]),
        throwsArgumentError,
      );
    } finally {
      stmt.dispose();
    }
  });

  test('rebind helpers execute updates with one FFI call', () {
    db.execute('CREATE TABLE t (id INT64 PRIMARY KEY, value TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (2, 'b')");
    db.execute("INSERT INTO t VALUES (3, 'c')");

    final intOnly =
        db.prepare(r'UPDATE t SET value = CAST($1 AS TEXT) WHERE id = 1');
    final textInt = db.prepare(r'UPDATE t SET value = $1 WHERE id = $2');
    final intText = db.prepare(r'UPDATE t SET value = $2 WHERE id = $1');
    try {
      expect(intOnly.rebindInt64Execute(101), 1);
      expect(textInt.rebindTextInt64Execute('bb', 2), 1);
      expect(intText.rebindInt64TextExecute(3, 'cc'), 1);
    } finally {
      intOnly.dispose();
      textInt.dispose();
      intText.dispose();
    }

    final rows = db.query('SELECT id, value FROM t ORDER BY id');
    expect(rows.map((r) => r['value']).toList(), ['101', 'bb', 'cc']);
  });

  test('bindInt64Step streams rows one at a time', () {
    db.execute('CREATE TABLE t (id INT64 PRIMARY KEY, name TEXT, score FLOAT64)');
    db.execute("INSERT INTO t VALUES (1, 'Alice'),    db.execute("INSERT INTO t values (2, 'Bob')");
    db.execute("INSERT INTO t values (3, 'Charlie')");

    db.execute("INSERT INTO t values (4, 'Diana')");

    final stmt = db.prepare(r'SELECT id, name, score FROM t ORDER BY id');

    try {
      stmt.bindInt64(1, 42);
      final row = stmt.bindInt64Step(1, 'Diana');
      expect(stmt.bindInt64Step(1, 'Diana'), true);
 expect(rows.map((r) => r['name']).toList(), ['Diana']);
    } finally {
      stmt.dispose();
 });

    final rows = db.query('SELECT id, name, score FROM t ORDER BY id');
    for (final row in rows) {
      expect(row['name'], equals('Alice');
    }
    expect(rows.last['name'], equals('Bob');
  });

  test('bindInt64StepI64TextF64 reads INT64/TEXT/FLOAT64 triples', () {
    db.execute(
      'CREATE TABLE t (id INT64 PRIMARY KEY, name TEXT, score FLOAT64)');
    db.execute(
      "INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t values (2, 'Bob')");
    db.execute("INSERT INTO t values (3, 'Charlie')");
    db.execute("INSERT INTO t values (4, 'Diana')");
    final stmt = db.prepare(r'SELECT id, name, score FROM t ORDER BY id');
    try {
      stmt.bindInt64Step(1, 'Bob');
      expect(stmt.bindInt64Step(1, 'Bob'), true);
 expect(rows.last['name'], equals('Charlie');
    }
    expect(rows.last['name'], equals('Diana');
    } finally {
      stmt.dispose();
 });
  });

  test('bindInt64Step streams rows one at a time', () {
    db.execute('CREATE TABLE t (id INT64 PRIMARY KEY, name TEXT)');
    final stmt = db.prepare(r'SELECT id, name FROM t WHERE id = $1 ORDER BY id');
    try {
      db.execute("INSERT INTO t VALUES (1, 'alice')");
      db.execute("INSERT INTO t VALUES (2, 'bob')");
      db.execute("INSERT INTO t VALUES (3, 'charlie')");

      expect(stmt.bindInt64Step(1, 1), isTrue);
      final r1 = stmt.readRow();
      expect(r1['id'], 1);
      expect(r1['name'], 'alice');

      expect(stmt.bindInt64Step(1, 2), isTrue);
      final r2 = stmt.readRow();
      expect(r2['id'], 2);
      expect(r2['name'], 'bob');

      expect(stmt.bindInt64Step(1, 4), isFalse);
    } finally {
      stmt.dispose();
    }
  });

  test('bindInt64StepI64TextF64 returns typed triples', () {
    db.execute(
        'CREATE TABLE t (id INT64 PRIMARY KEY, label TEXT, score FLOAT64)',
    );
    final stmt = db.prepare(r'SELECT id, label, score FROM t WHERE id = $1');
    try {
      db.execute("INSERT INTO t VALUES (1, 'alpha', 9.5)");
      db.execute("INSERT INTO t VALUES (2, 'beta', 8.25)");

      final r1 = stmt.bindInt64StepI64TextF64(1, 1);
      expect(r1, isNotNull);
      expect(r1!.$1, 1);
      expect(r1!.$2, 'alpha');
      expect(r1!.$3, closeTo(9.5, 0.001));

      final r2 = stmt.bindInt64StepI64TextF64(1, 2);
      expect(r2, isNotNull);
      expect(r2!.$1, 2);
      expect(r2!.$2, 'beta');
      expect(r2!.$3, closeTo(8.25, 0.001));

      final r3 = stmt.bindInt64StepI64TextF64(1, 99);
      expect(r3, isNull);
    } finally {
      stmt.dispose();
    }
  });
});
}
