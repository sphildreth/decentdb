import 'dart:io';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:decentdb/decentdb.dart';

/// Resolve the path to libc_api.so from environment or repo layout.
String findNativeLib() {
  final envPath = Platform.environment['DECENTDB_NATIVE_LIB'];
  if (envPath != null && envPath.isNotEmpty) return envPath;

  // Walk up from this test file to find the repo root build/ directory.
  var dir = Directory.current;
  for (var i = 0; i < 10; i++) {
    final candidate = File('${dir.path}/build/libc_api.so');
    if (candidate.existsSync()) return candidate.path;
    final candidateDylib = File('${dir.path}/build/libc_api.dylib');
    if (candidateDylib.existsSync()) return candidateDylib.path;
    final candidateDll = File('${dir.path}/build/c_api.dll');
    if (candidateDll.existsSync()) return candidateDll.path;
    dir = dir.parent;
  }
  throw StateError(
    'Cannot find DecentDB native library. '
    'Set DECENTDB_NATIVE_LIB or run from the repo root after `nimble build_lib`.',
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

  group('Database lifecycle', () {
    test('open and close in-memory database', () {
      // db is already open from setUp
      expect(db.engineVersion, isNotEmpty);
    });

    test('open file database and close', () {
      final tmpDir = Directory.systemTemp.createTempSync('decentdb_dart_');
      final dbPath = '${tmpDir.path}/test.ddb';
      try {
        final fileDb = Database.open(dbPath, libraryPath: libPath);
        fileDb.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
        fileDb.close();

        // Reopen and verify
        final fileDb2 = Database.open(dbPath, libraryPath: libPath);
        final tables = fileDb2.schema.listTables();
        expect(tables, contains('t'));
        fileDb2.close();
      } finally {
        tmpDir.deleteSync(recursive: true);
      }
    });

    test('operations on closed database throw', () {
      final db2 = Database.open(':memory:', libraryPath: libPath);
      db2.close();
      expect(() => db2.execute('SELECT 1'), throwsStateError);
    });
  });

  group('DDL', () {
    test('CREATE TABLE', () {
      db.execute(
        'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)',
      );
      final tables = db.schema.listTables();
      expect(tables, contains('users'));
    });

    test('CREATE INDEX', () {
      db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)');
      db.execute('CREATE INDEX idx_items_name ON items (name)');
      final indexes = db.schema.listIndexes();
      expect(indexes.any((i) => i.name == 'idx_items_name'), isTrue);
    });

    test('CREATE VIEW', () {
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      db.execute('CREATE VIEW v AS SELECT id, val FROM t');
      final views = db.schema.listViews();
      expect(views, contains('v'));
    });
  });

  group('DML', () {
    setUp(() {
      db.execute(
        'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score FLOAT)',
      );
    });

    test('INSERT and SELECT', () {
      db.execute("INSERT INTO users VALUES (1, 'Alice', 95.5)");
      db.execute("INSERT INTO users VALUES (2, 'Bob', 87.3)");
      final rows = db.query('SELECT id, name, score FROM users ORDER BY id');
      expect(rows.length, 2);
      expect(rows[0]['id'], 1);
      expect(rows[0]['name'], 'Alice');
      expect(rows[1]['name'], 'Bob');
    });

    test('UPDATE returns affected rows', () {
      db.execute("INSERT INTO users VALUES (1, 'Alice', 95.5)");
      db.execute("INSERT INTO users VALUES (2, 'Bob', 87.3)");
      final affected = db.executeWithParams(
        'UPDATE users SET score = \$1 WHERE id = \$2',
        [100.0, 1],
      );
      expect(affected, 1);
    });

    test('DELETE returns affected rows', () {
      db.execute("INSERT INTO users VALUES (1, 'Alice', 95.5)");
      db.execute("INSERT INTO users VALUES (2, 'Bob', 87.3)");
      final affected = db.execute('DELETE FROM users WHERE id = 1');
      expect(affected, 1);
    });
  });

  group('Prepared statements', () {
    setUp(() {
      db.execute(
        'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)',
      );
    });

    test('bind and execute with reuse', () {
      final stmt = db.prepare('INSERT INTO items VALUES (\$1, \$2, \$3)');
      try {
        for (var i = 1; i <= 5; i++) {
          stmt.reset();
          stmt.clearBindings();
          stmt.bindInt64(1, i);
          stmt.bindText(2, 'item_$i');
          stmt.bindBool(3, i.isEven);
          stmt.execute();
        }
      } finally {
        stmt.dispose();
      }

      final rows = db.query('SELECT COUNT(*) as cnt FROM items');
      expect(rows[0]['cnt'], 5);
    });

    test('bindAll convenience', () {
      final stmt = db.prepare('INSERT INTO items VALUES (\$1, \$2, \$3)');
      try {
        stmt.bindAll([1, 'test', true]);
        stmt.execute();
      } finally {
        stmt.dispose();
      }

      final rows = db.query('SELECT name FROM items WHERE id = 1');
      expect(rows[0]['name'], 'test');
    });

    test('query resets before re-executing', () {
      db.execute("INSERT INTO items VALUES (1, 'one', true)");
      db.execute("INSERT INTO items VALUES (2, 'two', false)");

      final stmt = db.prepare(
        'SELECT id FROM items WHERE active = \$1 ORDER BY id',
      );
      try {
        stmt.bindBool(1, true);

        final first = stmt.query();
        final second = stmt.query();

        expect(first.map((row) => row['id']).toList(), [1]);
        expect(second.map((row) => row['id']).toList(), [1]);
      } finally {
        stmt.dispose();
      }
    });

    test('bind NULL', () {
      final stmt = db.prepare('INSERT INTO items VALUES (\$1, \$2, \$3)');
      try {
        stmt.bindAll([1, null, null]);
        stmt.execute();
      } finally {
        stmt.dispose();
      }

      final rows = db.query('SELECT name, active FROM items WHERE id = 1');
      expect(rows[0]['name'], isNull);
      expect(rows[0]['active'], isNull);
    });

    test('disposed statement throws', () {
      final stmt = db.prepare('SELECT 1');
      stmt.dispose();
      expect(() => stmt.step(), throwsStateError);
    });
  });

  group('Cursor paging', () {
    setUp(() {
      db.execute('CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)');
      db.transaction(() {
        for (var i = 1; i <= 100; i++) {
          db.execute('INSERT INTO nums VALUES ($i, ${i * 10})');
        }
      });
    });

    test('page through results', () {
      final stmt = db.prepare('SELECT id, val FROM nums ORDER BY id');
      try {
        var totalRows = 0;
        var pageCount = 0;

        while (true) {
          final page = stmt.nextPage(25);
          if (page.rows.isEmpty && page.isLast) break;
          pageCount++;
          totalRows += page.rows.length;
          if (page.isLast) break;
          expect(page.rows.length, 25);
        }

        expect(totalRows, 100);
        expect(pageCount, 4);
      } finally {
        stmt.dispose();
      }
    });

    test('small last page', () {
      final stmt = db.prepare('SELECT id FROM nums ORDER BY id');
      try {
        final page1 = stmt.nextPage(60);
        expect(page1.rows.length, 60);
        expect(page1.isLast, isFalse);

        final page2 = stmt.nextPage(60);
        expect(page2.rows.length, 40);
        expect(page2.isLast, isTrue);
      } finally {
        stmt.dispose();
      }
    });
  });

  group('Transactions', () {
    setUp(() {
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    });

    test('commit persists data', () {
      db.begin();
      db.execute("INSERT INTO t VALUES (1, 'a')");
      db.commit();
      final rows = db.query('SELECT val FROM t WHERE id = 1');
      expect(rows[0]['val'], 'a');
    });

    test('rollback discards data', () {
      db.execute("INSERT INTO t VALUES (1, 'before')");
      db.begin();
      db.execute("UPDATE t SET val = 'during' WHERE id = 1");
      db.rollback();
      final rows = db.query('SELECT val FROM t WHERE id = 1');
      expect(rows[0]['val'], 'before');
    });

    test('transaction() helper commits on success', () {
      db.transaction(() {
        db.execute("INSERT INTO t VALUES (1, 'committed')");
      });
      final rows = db.query('SELECT val FROM t WHERE id = 1');
      expect(rows[0]['val'], 'committed');
    });

    test('transaction() helper rolls back on exception', () {
      db.execute("INSERT INTO t VALUES (1, 'original')");
      try {
        db.transaction(() {
          db.execute("UPDATE t SET val = 'changed' WHERE id = 1");
          throw Exception('deliberate');
        });
      } catch (_) {}
      final rows = db.query('SELECT val FROM t WHERE id = 1');
      expect(rows[0]['val'], 'original');
    });
  });

  group('Schema introspection', () {
    setUp(() {
      db.execute('''
        CREATE TABLE users (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          email TEXT UNIQUE,
          score FLOAT
        )
      ''');
      db.execute('CREATE INDEX idx_users_name ON users (name)');
      db.execute('CREATE VIEW user_names AS SELECT id, name FROM users');
    });

    test('listTables', () {
      final tables = db.schema.listTables();
      expect(tables, contains('users'));
    });

    test('getTableColumns', () {
      final cols = db.schema.getTableColumns('users');
      expect(cols.length, 4);

      final idCol = cols.firstWhere((c) => c.name == 'id');
      expect(idCol.primaryKey, isTrue);

      final nameCol = cols.firstWhere((c) => c.name == 'name');
      expect(nameCol.notNull, isTrue);

      final emailCol = cols.firstWhere((c) => c.name == 'email');
      expect(emailCol.unique, isTrue);
    });

    test('getTableColumns for views', () {
      final cols = db.schema.getTableColumns('user_names');
      expect(cols.length, 2);
      expect(cols.map((c) => c.name).toList(), ['id', 'name']);
    });

    test('listIndexes', () {
      final indexes = db.schema.listIndexes();
      final idx = indexes.firstWhere((i) => i.name == 'idx_users_name');
      expect(idx.table, 'users');
      expect(idx.columns, ['name']);
      expect(idx.kind, 'btree');
    });

    test('listViews', () {
      final views = db.schema.listViews();
      expect(views, contains('user_names'));
    });

    test('getViewDdl', () {
      final ddl = db.schema.getViewDdl('user_names');
      expect(ddl, isNotNull);
      expect(ddl!, contains('SELECT'));
    });

    test('getViewDdl throws for missing view', () {
      expect(
        () => db.schema.getViewDdl('missing_view'),
        throwsA(isA<DecentDbException>()),
      );
    });
  });

  group('Data types', () {
    test('integer types', () {
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
      db.execute('INSERT INTO t VALUES (1, 9223372036854775807)');
      final rows = db.query('SELECT val FROM t');
      expect(rows[0]['val'], 9223372036854775807);
    });

    test('float types', () {
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val FLOAT)');
      db.execute('INSERT INTO t VALUES (1, 3.14159)');
      final rows = db.query('SELECT val FROM t');
      expect((rows[0]['val'] as double), closeTo(3.14159, 0.0001));
    });

    test('text types', () {
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      final stmt = db.prepare('INSERT INTO t VALUES (\$1, \$2)');
      stmt.bindAll([1, 'Hello, 世界! 🌍']);
      stmt.execute();
      stmt.dispose();
      final rows = db.query('SELECT val FROM t');
      expect(rows[0]['val'], 'Hello, 世界! 🌍');
    });

    test('blob types', () {
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val BLOB)');
      final data = Uint8List.fromList([0, 1, 2, 255, 128, 64]);
      final stmt = db.prepare('INSERT INTO t VALUES (\$1, \$2)');
      stmt.bindInt64(1, 1);
      stmt.bindBlob(2, data);
      stmt.execute();
      stmt.dispose();
      final rows = db.query('SELECT val FROM t');
      expect(rows[0]['val'], data);
    });

    test('decimal types', () {
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val DECIMAL(10,2))');
      final insert = db.prepare('INSERT INTO t VALUES (\$1, \$2)');
      try {
        insert.bindInt64(1, 1);
        insert.bindDecimal(2, 12345, 2);
        insert.execute();
      } finally {
        insert.dispose();
      }

      final stmt = db.prepare('SELECT val FROM t');
      try {
        final rows = stmt.query();
        final decimal = rows[0]['val'] as ({int unscaled, int scale});
        expect(decimal.unscaled, 12345);
        expect(decimal.scale, 2);
      } finally {
        stmt.dispose();
      }
    });

    test('datetime types', () {
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TIMESTAMP)');
      final expected = DateTime.utc(2024, 1, 2, 3, 4, 5, 678, 901);
      final insert = db.prepare('INSERT INTO t VALUES (\$1, \$2)');
      try {
        insert.bindInt64(1, 1);
        insert.bindDateTime(2, expected);
        insert.execute();
      } finally {
        insert.dispose();
      }

      final stmt = db.prepare('SELECT val FROM t');
      try {
        final rows = stmt.query();
        expect(rows[0]['val'], expected);
      } finally {
        stmt.dispose();
      }
    });

    test('compact bool and integer result kinds', () {
      final rows = db.query(
        'SELECT TRUE AS t, FALSE AS f, 1 AS one, 0 AS zero',
      );
      expect(rows[0]['t'], isTrue);
      expect(rows[0]['f'], isFalse);
      expect(rows[0]['one'], 1);
      expect(rows[0]['zero'], 0);
    });

    test('boolean types', () {
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val BOOLEAN)');
      final stmt = db.prepare('INSERT INTO t VALUES (\$1, \$2)');
      stmt.bindAll([1, true]);
      stmt.execute();
      stmt.reset();
      stmt.clearBindings();
      stmt.bindAll([2, false]);
      stmt.execute();
      stmt.dispose();
      final rows = db.query('SELECT id, val FROM t ORDER BY id');
      expect(rows[0]['val'], true);
      expect(rows[1]['val'], false);
    });

    test('NULL handling', () {
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
      db.execute('INSERT INTO t VALUES (1, NULL)');
      final rows = db.query('SELECT val FROM t');
      expect(rows[0]['val'], isNull);
    });
  });

  group('Error handling', () {
    test('invalid SQL throws DecentDbException', () {
      expect(
        () => db.execute('NOT VALID SQL'),
        throwsA(isA<DecentDbException>()),
      );
    });

    test('constraint violation throws', () {
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
      db.execute('INSERT INTO t VALUES (1)');
      expect(
        () => db.execute('INSERT INTO t VALUES (1)'),
        throwsA(isA<DecentDbException>()),
      );
    });

    test('error message is descriptive', () {
      try {
        db.execute('SELECT * FROM nonexistent');
        fail('Should have thrown');
      } on DecentDbException catch (e) {
        expect(e.message, isNotEmpty);
        expect(e.toString(), contains('DecentDbException'));
      }
    });
  });

  group('EXPLAIN', () {
    test('EXPLAIN returns plan lines', () {
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
      final rows = db.query('EXPLAIN SELECT * FROM t WHERE id = 1');
      expect(rows, isNotEmpty);
      expect(rows[0].columns, contains('query_plan'));
    });
  });

  group('Maintenance', () {
    test('checkpoint succeeds', () {
      db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
      // checkpoint on in-memory db is a no-op but should not error
      db.checkpoint();
    });

    test('saveAs exports database', () {
      final tmpDir = Directory.systemTemp.createTempSync('decentdb_dart_');
      try {
        db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
        db.execute("INSERT INTO t VALUES (1, 'hello')");
        final destPath = '${tmpDir.path}/export.ddb';
        db.saveAs(destPath);
        expect(File(destPath).existsSync(), isTrue);

        // Verify exported db
        final db2 = Database.open(destPath, libraryPath: libPath);
        final rows = db2.query('SELECT val FROM t');
        expect(rows[0]['val'], 'hello');
        db2.close();
      } finally {
        tmpDir.deleteSync(recursive: true);
      }
    });
  });
}
