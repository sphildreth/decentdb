import 'dart:typed_data';

import 'package:decentdb/decentdb.dart';
import 'package:test/test.dart';

import 'test_utils.dart';

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
    db.execute('CREATE TABLE t (id INT64 PRIMARY KEY, name TEXT)');
    final stmt =
        db.prepare(r'SELECT id, name FROM t WHERE id = $1 ORDER BY id');
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
      expect(r1.$2, 'alpha');
      expect(r1.$3, closeTo(9.5, 0.001));

      final r2 = stmt.bindInt64StepI64TextF64(1, 2);
      expect(r2, isNotNull);
      expect(r2!.$1, 2);
      expect(r2.$2, 'beta');
      expect(r2.$3, closeTo(8.25, 0.001));

      final r3 = stmt.bindInt64StepI64TextF64(1, 99);
      expect(r3, isNull);
    } finally {
      stmt.dispose();
    }
  });

  // D5/D6: malloc-backed batch helpers -------------------------------------------

  test('bindBlob round-trips a 1 MiB blob exactly (D5)', () {
    db.execute('CREATE TABLE blobs (id INT64 PRIMARY KEY, data BLOB)');
    final data = Uint8List.fromList(
        List.generate(1024 * 1024, (i) => i & 0xFF));
    final stmt = db.prepare(r'INSERT INTO blobs VALUES ($1, $2)');
    try {
      stmt.bindInt64(1, 1);
      stmt.bindBlob(2, data);
      stmt.execute();
    } finally {
      stmt.dispose();
    }

    final row = db.query('SELECT data FROM blobs WHERE id = 1').first;
    final result = row['data'] as List<int>;
    expect(result.length, 1024 * 1024);
    expect(result, orderedEquals(data));
  });

  test('executeBatchTyped preserves row order across 1k rows (D6)', () {
    db.execute('CREATE TABLE t (id INT64 PRIMARY KEY, name TEXT)');
    final stmt = db.prepare(r'INSERT INTO t VALUES ($1, $2)');
    const n = 1000;
    try {
      stmt.executeBatchTyped(
        'it',
        [for (var i = 0; i < n; i++) [i, 'row_$i']],
      );
    } finally {
      stmt.dispose();
    }

    final rows = db.query('SELECT id, name FROM t ORDER BY id');
    expect(rows.length, n);
    expect(rows.first['id'], 0);
    expect(rows.last['id'], n - 1);
    expect(rows.last['name'], 'row_${n - 1}');
  });

  test('executeBatchTyped handles a mix of short and long strings (D6)', () {
    db.execute('CREATE TABLE t (id INT64 PRIMARY KEY, name TEXT)');
    final stmt = db.prepare(r'INSERT INTO t VALUES ($1, $2)');
    try {
      final rows = [
        [1, ''],
        [2, 'x' * 10000],
        [3, 'hello'],
      ];
      stmt.executeBatchTyped('it', rows);
    } finally {
      stmt.dispose();
    }

    final result = db.query('SELECT id, name FROM t ORDER BY id');
    expect(result[0]['name'], '');
    expect((result[1]['name'] as String).length, 10000);
    expect(result[2]['name'], 'hello');
  });

  test('executeBatchTyped with 0 rows is a no-op (D6)', () {
    db.execute('CREATE TABLE t (id INT64 PRIMARY KEY, name TEXT)');
    final stmt = db.prepare(r'INSERT INTO t VALUES ($1, $2)');
    try {
      final affected = stmt.executeBatchTyped('it', []);
      expect(affected, 0);
    } finally {
      stmt.dispose();
    }

    final rows = db.query('SELECT id FROM t');
    expect(rows, isEmpty);
  });

  // D8: zero-copy column names / query() ----------------------------------------

  test('columnNames returns an unmodifiable view (D8)', () {
    db.execute('CREATE TABLE t (a INT64, b TEXT, c FLOAT64)');
    final stmt = db.prepare('SELECT a, b, c FROM t');
    try {
      stmt.step(); // primes execution and loads column metadata
      final names = stmt.columnNames;
      expect(names, orderedEquals(['a', 'b', 'c']));
      expect(() => (names as List).clear(), throwsUnsupportedError);
    } finally {
      stmt.dispose();
    }
  });

  test('query() result list is unmodifiable (D8)', () {
    db.execute('CREATE TABLE t (id INT64 PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    final stmt = db.prepare('SELECT id FROM t');
    try {
      final result = stmt.query();
      expect(result.length, 1);
      expect(() => (result as List).clear(), throwsUnsupportedError);
    } finally {
      stmt.dispose();
    }
  });
}
