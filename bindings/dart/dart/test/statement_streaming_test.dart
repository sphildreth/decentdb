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
    db.execute(
      'CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT, active BOOL)',
    );
  });

  tearDown(() {
    db.close();
  });

  void seed(int count) {
    db.transaction(() {
      for (var i = 1; i <= count; i++) {
        db.execute("INSERT INTO items VALUES ($i, 'item_$i', ${i % 2 == 0})");
      }
    });
  }

  test('nextPage paginates across large result sets', () {
    seed(600);
    final stmt = db.prepare('SELECT id FROM items ORDER BY id');
    try {
      var seen = 0;
      while (true) {
        final page = stmt.nextPage(128);
        seen += page.rows.length;
        if (page.isLast) {
          break;
        }
      }
      expect(seen, 600);
    } finally {
      stmt.dispose();
    }
  });

  test('mixed step then nextPage does not duplicate rows', () {
    seed(7);
    final stmt = db.prepare('SELECT id FROM items ORDER BY id');
    try {
      expect(stmt.step(), isTrue);
      expect(stmt.readRow()['id'], 1);
      final page = stmt.nextPage(3);
      expect(page.rows.map((row) => row['id']).toList(), [2, 3, 4]);
      expect(stmt.step(), isTrue);
      expect(stmt.readRow()['id'], 5);
    } finally {
      stmt.dispose();
    }
  });

  test('mixed nextPage then step continues from page end', () {
    seed(6);
    final stmt = db.prepare('SELECT id FROM items ORDER BY id');
    try {
      final page = stmt.nextPage(4);
      expect(page.rows.map((row) => row['id']).toList(), [1, 2, 3, 4]);
      expect(page.isLast, isFalse);

      expect(stmt.step(), isTrue);
      expect(stmt.readRow()['id'], 5);
      expect(stmt.step(), isTrue);
      expect(stmt.readRow()['id'], 6);
      expect(stmt.step(), isFalse);
    } finally {
      stmt.dispose();
    }
  });

  test('readRow validity and invalidation rules are enforced', () {
    seed(3);
    final stmt = db.prepare('SELECT id FROM items ORDER BY id');
    try {
      expect(() => stmt.readRow(), throwsStateError);
      expect(stmt.step(), isTrue);
      expect(stmt.readRow()['id'], 1);
      stmt.nextPage(1);
      expect(() => stmt.readRow(), throwsStateError);
      expect(stmt.step(), isTrue);
      expect(stmt.readRow()['id'], 3);
    } finally {
      stmt.dispose();
    }
  });

  test('reset/rebind/clearBindings invalidate streaming state', () {
    seed(4);
    final stmt =
        db.prepare(r'SELECT id FROM items WHERE active = $1 ORDER BY id');
    try {
      stmt.bindBool(1, true);
      expect(stmt.step(), isTrue);
      expect(stmt.readRow()['id'], 2);

      stmt.reset();
      expect(() => stmt.readRow(), throwsStateError);

      stmt.clearBindings();
      stmt.bindBool(1, false);
      final page = stmt.nextPage(10);
      expect(page.rows.map((row) => row['id']).toList(), [1, 3]);
      expect(() => stmt.readRow(), throwsStateError);
    } finally {
      stmt.dispose();
    }
  });

  test('empty result sets are handled cleanly', () {
    final stmt = db.prepare('SELECT id FROM items ORDER BY id');
    try {
      expect(stmt.step(), isFalse);
      final page = stmt.nextPage(10);
      expect(page.rows, isEmpty);
      expect(page.isLast, isTrue);
    } finally {
      stmt.dispose();
    }
  });

  // D7: column metadata caching ---------------------------------------------------

  test('column metadata is cached across reset/bind/execute cycles (D7)', () {
    seed(5);
    final stmt = db.prepare(r'SELECT id, name FROM items WHERE id = $1');
    try {
      // First step primes execution and loads metadata.
      stmt.bindInt64(1, 1);
      stmt.step();
      final names1 = stmt.columnNames;
      expect(names1, orderedEquals(['id', 'name']));

      // Reset and re-execute; columnNames must be identical object (cached).
      stmt.reset();
      stmt.bindInt64(1, 2);
      stmt.step();
      final names2 = stmt.columnNames;
      expect(identical(names1, names2), isTrue,
          reason: 'columnNames should return the same cached list');
    } finally {
      stmt.dispose();
    }
  });

  test('column metadata is fresh after dispose and re-prepare (D7)', () {
    seed(3);
    final stmt1 = db.prepare(r'SELECT id, name FROM items WHERE id = $1');
    final names1 = () {
      stmt1.bindInt64(1, 1);
      stmt1.step(); // loads metadata via _primeStreamingExecution
      final n = stmt1.columnNames;
      stmt1.dispose();
      return n;
    }();

    final stmt2 = db.prepare(r'SELECT id, name FROM items WHERE id = $1');
    try {
      stmt2.bindInt64(1, 1);
      stmt2.step();
      final names2 = stmt2.columnNames;
      // Different objects but same content.
      expect(identical(names1, names2), isFalse);
      expect(names2, orderedEquals(['id', 'name']));
    } finally {
      stmt2.dispose();
    }
  });

  // D11: rows() async* stream ----------------------------------------------------

  test('rows() yields all rows in order (D11)', () async {
    seed(100);
    final stmt = db.prepare('SELECT id FROM items ORDER BY id');
    try {
      final ids = <int>[];
      await for (final row in stmt.rows()) {
        ids.add(row['id'] as int);
      }
      expect(ids.length, 100);
      expect(ids.first, 1);
      expect(ids.last, 100);
    } finally {
      stmt.dispose();
    }
  });

  test('rows(pageSize: 1) yields each row individually (D11)', () async {
    seed(5);
    final stmt = db.prepare('SELECT id FROM items ORDER BY id');
    try {
      final ids = <int>[];
      await for (final row in stmt.rows(pageSize: 1)) {
        ids.add(row['id'] as int);
      }
      expect(ids, orderedEquals([1, 2, 3, 4, 5]));
    } finally {
      stmt.dispose();
    }
  });

  test('rows(pageSize: 0) throws ArgumentError (D11)', () async {
    final stmt = db.prepare('SELECT id FROM items ORDER BY id');
    try {
      await expectLater(
        stmt.rows(pageSize: 0).first,
        throwsA(isA<ArgumentError>()),
      );
    } finally {
      stmt.dispose();
    }
  });

  test('breaking out of await-for stops iteration and allows reuse (D11)',
      () async {
    seed(20);
    final stmt = db.prepare('SELECT id FROM items ORDER BY id');
    try {
      final ids = <int>[];
      await for (final row in stmt.rows()) {
        ids.add(row['id'] as int);
        if (ids.length == 5) break;
      }
      expect(ids, orderedEquals([1, 2, 3, 4, 5]));

      // After break, reset and re-query should work.
      stmt.reset();
      final all = stmt.query();
      expect(all.length, 20);
    } finally {
      stmt.dispose();
    }
  });
}
