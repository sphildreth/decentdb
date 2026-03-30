/// DecentDB Dart binding smoke test.
///
/// Validates core binding behaviour using the high-level `decentdb` package.
///
/// Run from the repository root:
///   cargo build -p decentdb
///   cd tests/bindings/dart
///   dart pub get
///   DECENTDB_NATIVE_LIB=../../../target/debug/libdecentdb.so dart run smoke.dart
import 'dart:io';
import 'dart:typed_data';

import 'package:decentdb/decentdb.dart';

String _findNativeLib() {
  final env = Platform.environment['DECENTDB_NATIVE_LIB'];
  if (env != null && env.isNotEmpty) return env;
  var dir = File(Platform.script.toFilePath()).parent;
  for (var i = 0; i < 10; i++) {
    for (final name in [
      'target/debug/libdecentdb.so',
      'target/debug/libdecentdb.dylib',
      'target/debug/decentdb.dll',
    ]) {
      final f = File('${dir.path}/$name');
      if (f.existsSync()) return f.path;
    }
    dir = dir.parent;
  }
  throw StateError(
    'Cannot find DecentDB native library. Set DECENTDB_NATIVE_LIB.',
  );
}

void _check(bool condition, String message) {
  if (!condition) throw StateError('smoke: FAIL – $message');
}

void main() {
  final libPath = _findNativeLib();
  print('smoke: using library $libPath');

  // -------------------------------------------------------------------------
  // 1. Basic open / version / close
  // -------------------------------------------------------------------------
  final db = Database.open(':memory:', libraryPath: libPath);
  _check(db.engineVersion.isNotEmpty, 'engineVersion should be non-empty');
  print('smoke: engineVersion = ${db.engineVersion}');

  // -------------------------------------------------------------------------
  // 2. DDL
  // -------------------------------------------------------------------------
  db.execute(
    'CREATE TABLE smoke (id INT64 PRIMARY KEY, name TEXT NOT NULL)',
  );
  db.execute('CREATE TABLE smoke_audit (msg TEXT)');
  db.execute(
    "CREATE VIEW smoke_view AS SELECT id, name FROM smoke",
  );
  db.execute(
    "CREATE TRIGGER smoke_ai AFTER INSERT ON smoke FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO smoke_audit VALUES (''inserted'')')",
  );

  // -------------------------------------------------------------------------
  // 3. Parameterised insert via prepared statement
  // -------------------------------------------------------------------------
  final insert = db.prepare(r'INSERT INTO smoke VALUES ($1, $2)');
  try {
    final affected = insert.executeBatchTyped('it', [
      [1, 'dart-smoke'],
      [2, 'reuse'],
    ]);
    _check(
        affected == 2, 'executeBatchTyped should affect 2 rows, got $affected');
  } finally {
    insert.dispose();
  }

  // -------------------------------------------------------------------------
  // 4. Query and named-column access (O(1) via Map lookup)
  // -------------------------------------------------------------------------
  final rows = db.query('SELECT id, name FROM smoke ORDER BY id');
  _check(rows.length == 2, 'expected 2 rows, got ${rows.length}');
  _check(rows[0]['id'] == 1, 'row[0].id should be 1, got ${rows[0]['id']}');
  _check(rows[0]['name'] == 'dart-smoke', 'row[0].name mismatch');
  _check(rows[1]['id'] == 2, 'row[1].id should be 2');
  _check(rows[1]['name'] == 'reuse', 'row[1].name mismatch');

  // -------------------------------------------------------------------------
  // 5. step() / readRow() streaming API
  // -------------------------------------------------------------------------
  final stepStmt = db.prepare('SELECT id, name FROM smoke ORDER BY id');
  try {
    var count = 0;
    while (stepStmt.step()) {
      final row = stepStmt.readRow();
      _check(row['id'] != null, 'step row id is null');
      count++;
    }
    _check(count == 2, 'step() visited $count rows, expected 2');
  } finally {
    stepStmt.dispose();
  }

  // -------------------------------------------------------------------------
  // 6. Pagination via nextPage()
  // -------------------------------------------------------------------------
  final pageStmt = db.prepare('SELECT id FROM smoke ORDER BY id');
  try {
    final page1 = pageStmt.nextPage(1);
    final page2 = pageStmt.nextPage(1);
    final page3 = pageStmt.nextPage(1);
    _check(page1.rows.length == 1, 'page1 should have 1 row');
    _check(page2.rows.length == 1, 'page2 should have 1 row');
    _check(page2.isLast == false,
        'page2 should not report last at exact boundary');
    _check(page3.rows.isEmpty, 'page3 should be empty');
    _check(page3.isLast, 'page3 should be last');
  } finally {
    pageStmt.dispose();
  }

  // -------------------------------------------------------------------------
  // 7. Explicit transaction – rollback
  // -------------------------------------------------------------------------
  _check(!db.inTransaction, 'should not be in a transaction initially');
  db.begin();
  _check(db.inTransaction, 'should be in a transaction after begin()');
  db.execute("INSERT INTO smoke VALUES (99, 'will-be-rolled-back')");
  db.rollback();
  _check(!db.inTransaction, 'should not be in a transaction after rollback()');

  final cnt = db.query('SELECT COUNT(*) AS n FROM smoke').single['n'] as int;
  _check(cnt == 2, 'expected 2 rows after rollback, got $cnt');

  // -------------------------------------------------------------------------
  // 8. transaction() helper – commit path
  // -------------------------------------------------------------------------
  db.transaction(() {
    db.execute("INSERT INTO smoke VALUES (3, 'committed')");
  });
  final cnt2 = db.query('SELECT COUNT(*) AS n FROM smoke').single['n'] as int;
  _check(cnt2 == 3, 'expected 3 rows after committed transaction, got $cnt2');

  // -------------------------------------------------------------------------
  // 9. transaction() helper – rollback on exception
  // -------------------------------------------------------------------------
  try {
    db.transaction<void>(() {
      db.execute("INSERT INTO smoke VALUES (4, 'boom')");
      throw StateError('intentional failure');
    });
  } on StateError {
    // expected
  }
  final cnt3 = db.query('SELECT COUNT(*) AS n FROM smoke').single['n'] as int;
  _check(cnt3 == 3, 'expected 3 rows after failed transaction, got $cnt3');

  // -------------------------------------------------------------------------
  // 10. Error handling – bad SQL produces DecentDbException with sql error code
  // -------------------------------------------------------------------------
  try {
    db.execute('SELECT * FROM no_such_table_xyz');
    _check(false, 'expected DecentDbException for unknown table');
  } on DecentDbException catch (e) {
    _check(
      e.code == ErrorCode.sql,
      'expected ErrorCode.sql, got ${e.code}: ${e.message}',
    );
    _check(
      e.message.contains('no_such_table_xyz'),
      'error message should reference the missing table: ${e.message}',
    );
  }

  // -------------------------------------------------------------------------
  // 11. Data types – blob, decimal, timestamp
  // -------------------------------------------------------------------------
  db.execute(
    'CREATE TABLE typed (id INT64 PRIMARY KEY, blob BLOB, dec DECIMAL, ts TIMESTAMP)',
  );
  final typedInsert = db.prepare(r'INSERT INTO typed VALUES ($1, $2, $3, $4)');
  final blobIn = Uint8List.fromList([0xDE, 0xAD, 0xBE, 0xEF]);
  final decIn = const DecimalValue(99999, 3);
  final tsIn =
      DateTime.fromMicrosecondsSinceEpoch(1711540800000000, isUtc: true);
  try {
    typedInsert.bindInt64(1, 1);
    typedInsert.bindBlob(2, blobIn);
    typedInsert.bindDecimal(3, decIn.scaled, decIn.scale);
    typedInsert.bindDateTime(4, tsIn);
    typedInsert.execute();
  } finally {
    typedInsert.dispose();
  }

  final typedRow =
      db.query('SELECT blob, dec, ts FROM typed WHERE id = 1').single;
  _check(
    (typedRow['blob'] as Uint8List).toString() == blobIn.toString(),
    'blob round-trip failed',
  );
  _check(typedRow['dec'] == decIn, 'decimal round-trip failed');
  _check(typedRow['ts'] == tsIn, 'timestamp round-trip failed');

  // -------------------------------------------------------------------------
  // 12. Schema introspection + rich schema snapshot
  // -------------------------------------------------------------------------
  final tables = db.schema.listTables();
  _check(tables.contains('smoke'), 'smoke table not in schema');
  final smokeInfo = db.schema.describeTable('smoke');
  _check(smokeInfo.columns.any((c) => c.name == 'id'), 'id column not found');
  _check(smokeInfo.columns.any((c) => c.name == 'name'), 'name column missing');
  final snapshot = db.schema.getSchemaSnapshot();
  _check(snapshot.snapshotVersion == 1, 'snapshot version should be 1');
  _check(
    snapshot.tables.any((table) => table.name == 'smoke'),
    'schema snapshot missing smoke table',
  );
  _check(
    snapshot.views.any((view) => view.name == 'smoke_view'),
    'schema snapshot missing smoke_view',
  );
  _check(
    snapshot.triggers.any((trigger) => trigger.name == 'smoke_ai'),
    'schema snapshot missing smoke_ai trigger',
  );

  // -------------------------------------------------------------------------
  // Cleanup
  // -------------------------------------------------------------------------
  db.close();

  print('smoke: all checks passed');
}
