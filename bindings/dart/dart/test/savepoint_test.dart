import 'package:decentdb/decentdb.dart';
import 'package:test/test.dart';

import 'test_utils.dart';

void main() {
  late String libPath;
  late Database db;

  setUpAll(() {
    libPath = findNativeLib();
  });

  setUp(() {
    db = Database.open(':memory:', libraryPath: libPath);
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
  });

  tearDown(() {
    db.close();
  });

  group('Savepoints', () {
    test('savepoint + rollbackToSavepoint restores intermediate state', () {
      db.begin();
      db.execute("INSERT INTO t VALUES (1, 'outer')");
      db.savepoint('sp1');
      db.execute("INSERT INTO t VALUES (2, 'inner')");

      // Roll back to sp1 — row 2 should disappear.
      db.rollbackToSavepoint('sp1');

      final rows = db.query('SELECT id FROM t').toList();
      expect(rows, hasLength(1));
      expect(rows.first['id'], equals(1));

      db.commit();
    });

    test(
        'savepoint + releaseSavepoint commits intermediate work when outer txn commits',
        () {
      db.begin();
      db.execute("INSERT INTO t VALUES (1, 'outer')");
      db.savepoint('sp1');
      db.execute("INSERT INTO t VALUES (2, 'inner')");
      db.releaseSavepoint('sp1');
      db.commit();

      final rows = db.query('SELECT id FROM t ORDER BY id').toList();
      expect(rows, hasLength(2));
      expect(rows[0]['id'], equals(1));
      expect(rows[1]['id'], equals(2));
    });

    group('rejects invalid savepoint names', () {
      test('empty name', () {
        expect(() => db.savepoint(''), throwsA(isA<ArgumentError>()));
      });

      test('name containing a double-quote', () {
        expect(
            () => db.savepoint('bad"name'), throwsA(isA<ArgumentError>()));
      });

      test('name containing a newline', () {
        expect(
            () => db.savepoint('bad\nname'), throwsA(isA<ArgumentError>()));
      });

      test('name longer than 128 characters', () {
        final longName = 'a' * 129;
        expect(
            () => db.savepoint(longName), throwsA(isA<ArgumentError>()));
      });

      test('name of exactly 128 characters is accepted', () {
        final maxName = 'a' * 128;
        db.begin();
        expect(() => db.savepoint(maxName), returnsNormally);
        db.rollback();
      });
    });
  });
}
