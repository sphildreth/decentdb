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
        'CREATE TABLE docs (id INT64 PRIMARY KEY, title TEXT, body TEXT)');
    db.execute(
      'CREATE INDEX idx_docs_search ON docs USING fulltext (title, body) '
      "WITH (prefix = '2,3')",
    );
    db.execute(
      "INSERT INTO docs VALUES (1, 'Embedded database search', "
      "'DecentDB adds rust database search search primitives.')",
    );
    db.execute(
      "INSERT INTO docs VALUES (2, 'Rust database notes', "
      "'Durable local database storage.')",
    );
    db.execute(
      "INSERT INTO docs VALUES (3, 'Calendar entry', "
      "'Lunch and project planning.')",
    );
  });

  tearDown(() {
    db.close();
  });

  test('runs full-text search and BM25 ranking through the binding', () {
    final rows = db.query(
      "SELECT id, title, bm25('idx_docs_search') AS rank "
      'FROM docs '
      "WHERE fulltext_match('idx_docs_search', \$1) "
      'ORDER BY rank DESC, id',
      ['database OR search'],
    );

    expect(rows.map((row) => row['id']).toList(), [1, 2]);
    expect(rows.first['title'], 'Embedded database search');
    expect(rows.first['rank'], isA<double>());
    expect(
        rows.first['rank'] as double, greaterThan(rows[1]['rank'] as double));

    final prefixRows = db.query(
      "SELECT id FROM docs WHERE fulltext_match('idx_docs_search', \$1) "
      'ORDER BY id',
      ['dec*'],
    );
    expect(prefixRows.map((row) => row['id']).toList(), [1]);
  });
}
