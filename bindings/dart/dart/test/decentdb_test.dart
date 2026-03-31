import 'dart:io';
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

  group('database lifecycle', () {
    test('open and close in-memory database', () {
      expect(db.engineVersion, isNotEmpty);
    });

    test('open file database and reopen it', () {
      final tempDir = Directory.systemTemp.createTempSync('decentdb_dart_');
      final dbPath = '${tempDir.path}/test.ddb';
      try {
        final fileDb = Database.open(dbPath, libraryPath: libPath);
        fileDb.execute('CREATE TABLE t (id INT64 PRIMARY KEY)');
        fileDb.close();

        final reopened = Database.open(dbPath, libraryPath: libPath);
        expect(reopened.schema.listTables(), contains('t'));
        reopened.close();
      } finally {
        tempDir.deleteSync(recursive: true);
      }
    });

    test('create and openExisting expose distinct open modes', () {
      final tempDir =
          Directory.systemTemp.createTempSync('decentdb_dart_modes_');
      final dbPath = '${tempDir.path}/modes.ddb';
      try {
        final created = Database.create(dbPath, libraryPath: libPath);
        created.execute('CREATE TABLE t (id INT64 PRIMARY KEY)');
        created.close();

        final reopened = Database.openExisting(dbPath, libraryPath: libPath);
        expect(reopened.schema.listTables(), contains('t'));
        reopened.close();
      } finally {
        tempDir.deleteSync(recursive: true);
      }
    });

    test('rejects unsupported native open options', () {
      expect(
        () => Database.open(':memory:',
            libraryPath: libPath, options: 'cache_mb=8'),
        throwsArgumentError,
      );
    });

    test('operations on closed database throw', () {
      final closed = Database.open(':memory:', libraryPath: libPath);
      closed.close();
      expect(() => closed.execute('SELECT 1'), throwsStateError);
    });
  });

  group('DML and query execution', () {
    setUp(() {
      db.execute(
        'CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT NOT NULL, score FLOAT64)',
      );
    });

    test('insert and query rows', () {
      db.execute("INSERT INTO users VALUES (1, 'Alice', 95.5)");
      db.execute("INSERT INTO users VALUES (2, 'Bob', 87.0)");

      final rows = db.query('SELECT id, name, score FROM users ORDER BY id');
      expect(rows, hasLength(2));
      expect(rows[0]['id'], 1);
      expect(rows[0]['name'], 'Alice');
      expect(rows[1]['name'], 'Bob');
    });

    test('executeWithParams returns affected rows', () {
      db.execute("INSERT INTO users VALUES (1, 'Alice', 95.5)");

      final affected = db.executeWithParams(
        r'UPDATE users SET score = $1 WHERE id = $2',
        [100.0, 1],
      );

      expect(affected, 1);
      expect(db.query('SELECT score FROM users WHERE id = 1').single['score'],
          100.0);
    });

    test('saveAs exports the current database', () {
      db.execute("INSERT INTO users VALUES (1, 'Alice', 95.5)");
      final tempDir = Directory.systemTemp.createTempSync('decentdb_export_');
      final exportPath = '${tempDir.path}/copy.ddb';
      try {
        db.saveAs(exportPath);
        final copy = Database.open(exportPath, libraryPath: libPath);
        expect(
            copy.query('SELECT COUNT(*) AS cnt FROM users').single['cnt'], 1);
        copy.close();
      } finally {
        tempDir.deleteSync(recursive: true);
      }
    });
  });

  group('statement wrapper', () {
    setUp(() {
      db.execute(
        'CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT, active BOOL)',
      );
    });

    test('bind and execute with reuse', () {
      final stmt = db.prepare(r'INSERT INTO items VALUES ($1, $2, $3)');
      try {
        for (var i = 1; i <= 3; i++) {
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

      expect(db.query('SELECT COUNT(*) AS cnt FROM items').single['cnt'], 3);
    });

    test('query resets before re-executing', () {
      db.execute("INSERT INTO items VALUES (1, 'one', true)");
      db.execute("INSERT INTO items VALUES (2, 'two', false)");

      final stmt =
          db.prepare(r'SELECT id FROM items WHERE active = $1 ORDER BY id');
      try {
        stmt.bindBool(1, true);
        expect(stmt.query().map((row) => row['id']).toList(), [1]);
        expect(stmt.query().map((row) => row['id']).toList(), [1]);
      } finally {
        stmt.dispose();
      }
    });

    test('nextPage returns bounded pages', () {
      db.transaction(() {
        for (var i = 1; i <= 10; i++) {
          db.execute("INSERT INTO items VALUES ($i, 'item_$i', true)");
        }
      });

      final stmt = db.prepare('SELECT id FROM items ORDER BY id');
      try {
        final page1 = stmt.nextPage(4);
        final page2 = stmt.nextPage(4);
        final page3 = stmt.nextPage(4);

        expect(page1.rows.map((row) => row['id']).toList(), [1, 2, 3, 4]);
        expect(page2.rows.map((row) => row['id']).toList(), [5, 6, 7, 8]);
        expect(page3.rows.map((row) => row['id']).toList(), [9, 10]);
        expect(page3.isLast, isTrue);
      } finally {
        stmt.dispose();
      }
    });

    test('nextPage exact boundary reports last page on follow-up fetch', () {
      db.transaction(() {
        for (var i = 1; i <= 4; i++) {
          db.execute("INSERT INTO items VALUES ($i, 'item_$i', true)");
        }
      });

      final stmt = db.prepare('SELECT id FROM items ORDER BY id');
      try {
        final page = stmt.nextPage(4);
        expect(page.rows.map((row) => row['id']).toList(), [1, 2, 3, 4]);
        expect(page.isLast, isFalse);
        final tail = stmt.nextPage(4);
        expect(tail.rows, isEmpty);
        expect(tail.isLast, isTrue);
      } finally {
        stmt.dispose();
      }
    });

    test('mixed step then nextPage does not duplicate rows', () {
      db.transaction(() {
        for (var i = 1; i <= 6; i++) {
          db.execute("INSERT INTO items VALUES ($i, 'item_$i', true)");
        }
      });

      final stmt = db.prepare('SELECT id FROM items ORDER BY id');
      try {
        expect(stmt.step(), isTrue);
        expect(stmt.readRow()['id'], 1);

        final page = stmt.nextPage(3);
        expect(page.rows.map((row) => row['id']).toList(), [2, 3, 4]);
      } finally {
        stmt.dispose();
      }
    });

    test('mixed nextPage then step advances from page end', () {
      db.transaction(() {
        for (var i = 1; i <= 6; i++) {
          db.execute("INSERT INTO items VALUES ($i, 'item_$i', true)");
        }
      });

      final stmt = db.prepare('SELECT id FROM items ORDER BY id');
      try {
        final page = stmt.nextPage(4);
        expect(page.rows.map((row) => row['id']).toList(), [1, 2, 3, 4]);
        expect(page.isLast, isFalse);

        expect(stmt.step(), isTrue);
        expect(stmt.readRow()['id'], 5);
      } finally {
        stmt.dispose();
      }
    });

    test('readRow validity follows step and page invalidation rules', () {
      db.execute("INSERT INTO items VALUES (1, 'one', true)");
      db.execute("INSERT INTO items VALUES (2, 'two', true)");

      final stmt = db.prepare('SELECT id FROM items ORDER BY id');
      try {
        expect(() => stmt.readRow(), throwsStateError);
        expect(stmt.step(), isTrue);
        expect(stmt.readRow()['id'], 1);
        stmt.nextPage(1);
        expect(() => stmt.readRow(), throwsStateError);
      } finally {
        stmt.dispose();
      }
    });

    test('reset and rebind invalidate streaming state', () {
      db.execute("INSERT INTO items VALUES (1, 'one', true)");
      db.execute("INSERT INTO items VALUES (2, 'two', true)");
      db.execute("INSERT INTO items VALUES (3, 'three', false)");

      final stmt =
          db.prepare(r'SELECT id FROM items WHERE active = $1 ORDER BY id');
      try {
        stmt.bindBool(1, true);
        expect(stmt.step(), isTrue);
        expect(stmt.readRow()['id'], 1);

        stmt.reset();
        expect(() => stmt.readRow(), throwsStateError);

        stmt.clearBindings();
        stmt.bindBool(1, false);
        final page = stmt.nextPage(5);
        expect(page.rows.map((row) => row['id']).toList(), [3]);
      } finally {
        stmt.dispose();
      }
    });

    test('streaming handles empty and large result sets', () {
      final emptyStmt = db.prepare('SELECT id FROM items ORDER BY id');
      try {
        expect(emptyStmt.step(), isFalse);
        final emptyPage = emptyStmt.nextPage(10);
        expect(emptyPage.rows, isEmpty);
        expect(emptyPage.isLast, isTrue);
      } finally {
        emptyStmt.dispose();
      }

      db.transaction(() {
        for (var i = 1; i <= 600; i++) {
          db.execute("INSERT INTO items VALUES ($i, 'item_$i', true)");
        }
      });

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

    test('step/readRow iterate over decoded rows', () {
      db.execute("INSERT INTO items VALUES (1, 'one', true)");
      db.execute("INSERT INTO items VALUES (2, 'two', false)");

      final stmt = db.prepare('SELECT id, name FROM items ORDER BY id');
      try {
        expect(stmt.step(), isTrue);
        expect(stmt.readRow()['name'], 'one');
        expect(stmt.step(), isTrue);
        expect(stmt.readRow()['name'], 'two');
        expect(stmt.step(), isFalse);
      } finally {
        stmt.dispose();
      }
    });

    test('disposed statement throws', () {
      final stmt = db.prepare('SELECT 1');
      stmt.dispose();
      expect(() => stmt.query(), throwsStateError);
    });

    test('executeBatchInt64 inserts many rows', () {
      final stmt = db.prepare(r'INSERT INTO items (id) VALUES ($1)');
      try {
        final affected = stmt.executeBatchInt64([11, 12, 13, 14]);
        expect(affected, 4);
      } finally {
        stmt.dispose();
      }

      final ids = db
          .query('SELECT id FROM items ORDER BY id')
          .map((row) => row['id'])
          .toList();
      expect(ids, [11, 12, 13, 14]);
    });

    test('executeBatchI64TextF64 inserts triple rows', () {
      db.execute('ALTER TABLE items ADD COLUMN score FLOAT64');
      final stmt = db.prepare(
        r'INSERT INTO items (id, name, score) VALUES ($1, $2, $3)',
      );
      try {
        final affected = stmt.executeBatchI64TextF64([
          (1, 'one', 1.25),
          (2, 'two', 2.5),
          (3, 'three', 3.75),
        ]);
        expect(affected, 3);
      } finally {
        stmt.dispose();
      }

      final rows = db.query('SELECT id, name, score FROM items ORDER BY id');
      expect(rows.map((row) => row['id']).toList(), [1, 2, 3]);
      expect(rows.map((row) => row['name']).toList(), ['one', 'two', 'three']);
      expect(rows.map((row) => row['score']).toList(), [1.25, 2.5, 3.75]);
    });

    test('executeBatchTyped inserts rows with mixed signature', () {
      db.execute('ALTER TABLE items ADD COLUMN score FLOAT64');
      db.execute('ALTER TABLE items ADD COLUMN qty INT64');
      final stmt = db.prepare(
        r'INSERT INTO items (id, name, score, qty) VALUES ($1, $2, $3, $4)',
      );
      try {
        final affected = stmt.executeBatchTyped('itfi', [
          [1, 'one', 1.5, 10],
          [2, 'two', 2.25, 20],
          [3, 'three', 3.75, 30],
        ]);
        expect(affected, 3);
      } finally {
        stmt.dispose();
      }

      final rows =
          db.query('SELECT id, name, score, qty FROM items ORDER BY id');
      expect(rows.map((row) => row['id']).toList(), [1, 2, 3]);
      expect(rows.map((row) => row['qty']).toList(), [10, 20, 30]);
    });

    test('rebind fast paths update rows repeatedly', () {
      db.execute('CREATE TABLE counters (id INT64 PRIMARY KEY, value TEXT)');
      db.execute("INSERT INTO counters VALUES (1, 'a')");
      db.execute("INSERT INTO counters VALUES (2, 'b')");
      db.execute("INSERT INTO counters VALUES (3, 'c')");

      final int64Only = db.prepare(
          r'UPDATE counters SET value = CAST($1 AS TEXT) WHERE id = 1');
      final textInt64 =
          db.prepare(r'UPDATE counters SET value = $1 WHERE id = $2');
      final int64Text =
          db.prepare(r'UPDATE counters SET value = $2 WHERE id = $1');
      try {
        expect(int64Only.rebindInt64Execute(101), 1);
        expect(textInt64.rebindTextInt64Execute('bb', 2), 1);
        expect(int64Text.rebindInt64TextExecute(3, 'cc'), 1);
      } finally {
        int64Only.dispose();
        textInt64.dispose();
        int64Text.dispose();
      }

      final rows = db.query('SELECT id, value FROM counters ORDER BY id');
      expect(rows.map((row) => row['value']).toList(), ['101', 'bb', 'cc']);
    });
  });

  group('transactions', () {
    setUp(() {
      db.execute('CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)');
    });

    test('commit persists data', () {
      expect(db.inTransaction, isFalse);
      db.begin();
      expect(db.inTransaction, isTrue);
      db.execute("INSERT INTO t VALUES (1, 'a')");
      db.commit();
      expect(db.inTransaction, isFalse);
      expect(db.query('SELECT val FROM t WHERE id = 1').single['val'], 'a');
    });

    test('rollback discards data', () {
      db.execute("INSERT INTO t VALUES (1, 'before')");
      db.begin();
      db.execute("UPDATE t SET val = 'during' WHERE id = 1");
      db.rollback();
      expect(
          db.query('SELECT val FROM t WHERE id = 1').single['val'], 'before');
    });

    test('transaction helper commits and rolls back', () {
      db.transaction(() {
        db.execute("INSERT INTO t VALUES (1, 'committed')");
      });
      expect(db.query('SELECT val FROM t WHERE id = 1').single['val'],
          'committed');

      db.execute("INSERT INTO t VALUES (2, 'original')");
      expect(
        () => db.transaction<void>(() {
          db.execute("UPDATE t SET val = 'changed' WHERE id = 2");
          throw StateError('boom');
        }),
        throwsStateError,
      );
      expect(
          db.query('SELECT val FROM t WHERE id = 2').single['val'], 'original');
    });
  });

  group('schema metadata', () {
    setUp(() {
      db.execute('CREATE TABLE parent (id INT64 PRIMARY KEY)');
      db.execute('INSERT INTO parent VALUES (1)');
      db.execute(
        "CREATE TABLE child (id INT64 PRIMARY KEY, parent_id INT64 NOT NULL REFERENCES parent(id) ON DELETE CASCADE, name TEXT DEFAULT 'anon')",
      );
      db.execute('CREATE INDEX idx_child_parent ON child (parent_id)');
      db.execute('CREATE VIEW child_names AS SELECT id, name FROM child');
      db.execute('CREATE TABLE audit_log (msg TEXT)');
      db.execute(
        "CREATE TRIGGER child_ai AFTER INSERT ON child FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log VALUES (''changed'')')",
      );
      db.execute("INSERT INTO child VALUES (1, 1, 'Ada')");
    });

    test('listTables and describeTable expose catalog state', () {
      expect(db.schema.listTables(),
          containsAll(['parent', 'child', 'audit_log']));

      final child = db.schema.describeTable('child');
      expect(child.rowCount, 1);
      expect(child.primaryKeyColumns, ['id']);
      expect(child.columns.map((column) => column.name),
          containsAll(['id', 'parent_id', 'name']));

      final parentColumn =
          child.columns.firstWhere((column) => column.name == 'parent_id');
      expect(parentColumn.foreignKey, isNotNull);
      expect(parentColumn.foreignKey!.referencedTable, 'parent');
      expect(parentColumn.foreignKey!.onDelete, 'CASCADE');

      expect(db.schema.getTableColumns('child').length, child.columns.length);
      expect(db.schema.getTableDdl('child'), contains('CREATE TABLE "child"'));
    });

    test('listIndexes, listViews, and listTriggers expose metadata', () {
      final indexes = db.schema.listIndexes();
      final childIndex =
          indexes.firstWhere((index) => index.name == 'idx_child_parent');
      expect(childIndex.tableName, 'child');

      final views = db.schema.listViewsInfo();
      final childView = views.firstWhere((view) => view.name == 'child_names');
      expect(childView.dependencies, contains('child'));
      expect(db.schema.getViewDdl('child_names'),
          contains('SELECT id, name FROM child'));

      final triggers = db.schema.listTriggers();
      final childTrigger =
          triggers.firstWhere((trigger) => trigger.name == 'child_ai');
      expect(childTrigger.targetName, 'child');
      expect(childTrigger.actionSql, contains('changed'));
    });

    test('getSchemaSnapshot exposes rich metadata contract', () {
      final snapshot = db.schema.getSchemaSnapshot();
      expect(snapshot.snapshotVersion, 1);
      expect(snapshot.schemaCookie, greaterThanOrEqualTo(0));

      final child =
          snapshot.tables.firstWhere((table) => table.name == 'child');
      expect(child.ddl, contains('CREATE TABLE "child"'));
      expect(child.rowCount, 1);
      expect(child.columns.map((column) => column.name),
          containsAll(['id', 'parent_id', 'name']));

      final view =
          snapshot.views.firstWhere((entry) => entry.name == 'child_names');
      expect(view.ddl, contains('CREATE VIEW "child_names"'));

      final index = snapshot.indexes
          .firstWhere((entry) => entry.name == 'idx_child_parent');
      expect(index.kind, 'btree');
      expect(index.ddl, contains('CREATE INDEX'));

      final trigger =
          snapshot.triggers.firstWhere((entry) => entry.name == 'child_ai');
      expect(trigger.targetKind, 'table');
      expect(trigger.timing, 'after');
      expect(trigger.events, contains('insert'));
      expect(trigger.ddl, contains('CREATE TRIGGER'));
    });
  });

  group('data types', () {
    test('typed scalar values round-trip', () {
      db.execute(
        'CREATE TABLE typed (id INT64 PRIMARY KEY, score FLOAT64, active BOOL, note TEXT)',
      );

      final insert = db.prepare(r'INSERT INTO typed VALUES ($1, $2, $3, $4)');
      try {
        insert.bindInt64(1, 1);
        insert.bindFloat64(2, 12.5);
        insert.bindBool(3, true);
        insert.bindText(4, 'hello');
        insert.execute();
      } finally {
        insert.dispose();
      }

      final row = db.query('SELECT score, active, note FROM typed').single;
      expect(row['score'], 12.5);
      expect(row['active'], true);
      expect(row['note'], 'hello');
    });

    test('blob, decimal, and timestamp values round-trip', () {
      db.execute(
        'CREATE TABLE typed_more (id INT64 PRIMARY KEY, payload BLOB, amount DECIMAL, created_at TIMESTAMP)',
      );

      final insert = db.prepare(
        r'INSERT INTO typed_more VALUES ($1, $2, $3, $4)',
      );
      try {
        insert.bindInt64(1, 1);
        insert.bindBlob(2, Uint8List.fromList([1, 2, 3, 4]));
        insert.bindDecimal(3, 12345, 2);
        insert.bindDateTime(
          4,
          DateTime.fromMicrosecondsSinceEpoch(1711540800123456, isUtc: true),
        );
        insert.execute();
      } finally {
        insert.dispose();
      }

      final row = db
          .query(
            'SELECT payload, amount, created_at FROM typed_more',
          )
          .single;
      expect(row['payload'], Uint8List.fromList([1, 2, 3, 4]));
      expect(row['amount'], const DecimalValue(12345, 2));
      expect(
        row['created_at'],
        DateTime.fromMicrosecondsSinceEpoch(1711540800123456, isUtc: true),
      );
    });
  });

  group('error handling', () {
    test('unknown error codes do not silently map to internal', () {
      expect(() => ErrorCode.fromCode(999), throwsStateError);
    });
  });

  group('maintenance helpers', () {
    test('evictSharedWal succeeds for closed on-disk database', () {
      final tempDir =
          Directory.systemTemp.createTempSync('decentdb_dart_evict_');
      final dbPath = '${tempDir.path}/evict.ddb';
      try {
        final fileDb = Database.open(dbPath, libraryPath: libPath);
        fileDb.execute('CREATE TABLE t (id INT64 PRIMARY KEY)');
        fileDb.close();

        expect(
          () => Database.evictSharedWal(dbPath, libraryPath: libPath),
          returnsNormally,
        );
      } finally {
        tempDir.deleteSync(recursive: true);
      }
    });
  });
}
