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

  test(
      'rich schema snapshot includes tables/views/indexes/triggers and temp objects',
      () {
    db.execute('CREATE TABLE parent (id INT64 PRIMARY KEY, label TEXT)');
    db.execute('CREATE TABLE audit_log (msg TEXT)');
    db.execute(
      'CREATE TABLE metrics ('
      'id INT64 PRIMARY KEY, '
      'parent_id INT64 REFERENCES parent(id) ON UPDATE CASCADE ON DELETE SET NULL, '
      'qty INT64 NOT NULL CONSTRAINT ck_qty CHECK (qty > 0), '
      'price FLOAT64 NOT NULL CHECK (price >= 0), '
      'total FLOAT64 GENERATED ALWAYS AS (qty * price) STORED, '
      "label TEXT GENERATED ALWAYS AS ('id:' || CAST(id AS TEXT)) VIRTUAL"
      ')',
    );
    db.execute(
      'CREATE INDEX idx_metrics_parent_partial ON metrics(parent_id) WHERE qty > 1',
    );
    db.execute(
      'CREATE VIEW metrics_view AS SELECT id, parent_id, total FROM metrics',
    );
    db.execute(
      "CREATE TRIGGER metrics_ai AFTER INSERT ON metrics FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log VALUES (''changed'')')",
    );
    db.execute(
      "CREATE TRIGGER metrics_view_ioi INSTEAD OF INSERT ON metrics_view FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log VALUES (''view'')')",
    );
    db.execute(
        'CREATE TEMP TABLE temp_metrics (id INT64 PRIMARY KEY, note TEXT)');
    db.execute(
        'CREATE TEMP VIEW temp_metrics_view AS SELECT id, note FROM temp_metrics');
    db.execute('INSERT INTO parent VALUES (1, \'root\')');
    db.execute(
        'INSERT INTO metrics (id, parent_id, qty, price) VALUES (1, 1, 2, 10.5)');

    final snapshot = db.schema.getSchemaSnapshot();

    expect(snapshot.snapshotVersion, 1);
    expect(snapshot.tables, isNotEmpty);
    expect(snapshot.views, isNotEmpty);
    expect(snapshot.indexes, isNotEmpty);
    expect(snapshot.triggers, isNotEmpty);

    final tableNames = snapshot.tables.map((t) => t.name).toList();
    expect(tableNames, containsAll(['metrics', 'temp_metrics']));

    final metrics = snapshot.tables.firstWhere((t) => t.name == 'metrics');
    expect(metrics.ddl, contains('CREATE TABLE "metrics"'));
    expect(metrics.rowCount, 1);
    expect(metrics.primaryKeyColumns, ['id']);
    expect(metrics.foreignKeys, hasLength(1));
    expect(metrics.foreignKeys.single.onUpdate, 'CASCADE');
    expect(metrics.foreignKeys.single.onDelete, 'SET NULL');

    final totalCol = metrics.columns.firstWhere((c) => c.name == 'total');
    final labelCol = metrics.columns.firstWhere((c) => c.name == 'label');
    final priceCol = metrics.columns.firstWhere((c) => c.name == 'price');
    expect(totalCol.generatedSql, isNotNull);
    expect(totalCol.generatedStored, isTrue);
    expect(labelCol.generatedSql, isNotNull);
    expect(labelCol.generatedStored, isFalse);

    final qtyCol = metrics.columns.firstWhere((c) => c.name == 'qty');
    expect(qtyCol.checks, isNotEmpty);
    expect(
      qtyCol.checks.any((c) => c.expressionSql.contains('qty > 0')),
      isTrue,
    );
    expect(
      priceCol.checks.any((c) => c.expressionSql.contains('price >= 0')),
      isTrue,
    );

    final tempTable =
        snapshot.tables.firstWhere((t) => t.name == 'temp_metrics');
    expect(tempTable.temporary, isTrue);

    final tempView =
        snapshot.views.firstWhere((v) => v.name == 'temp_metrics_view');
    expect(tempView.temporary, isTrue);
    expect(tempView.ddl, contains('CREATE TEMP VIEW'));

    final partialIndex = snapshot.indexes
        .firstWhere((idx) => idx.name == 'idx_metrics_parent_partial');
    expect(partialIndex.predicateSql, isNotNull);
    expect(partialIndex.predicateSql, contains('qty > 1'));

    final tableTrigger =
        snapshot.triggers.firstWhere((t) => t.name == 'metrics_ai');
    final viewTrigger =
        snapshot.triggers.firstWhere((t) => t.name == 'metrics_view_ioi');
    expect(tableTrigger.targetKind, 'table');
    expect(tableTrigger.events, contains('insert'));
    expect(viewTrigger.targetKind, 'view');
    expect(viewTrigger.events, contains('insert'));
  });

  test('schema snapshot collections are deterministically ordered by name', () {
    db.execute('CREATE TABLE z_tbl (id INT64 PRIMARY KEY)');
    db.execute('CREATE TABLE a_tbl (id INT64 PRIMARY KEY)');
    db.execute('CREATE VIEW z_view AS SELECT id FROM z_tbl');
    db.execute('CREATE VIEW a_view AS SELECT id FROM a_tbl');
    db.execute('CREATE INDEX z_idx ON z_tbl(id)');
    db.execute('CREATE INDEX a_idx ON a_tbl(id)');
    db.execute('CREATE TABLE trigger_audit (msg TEXT)');
    db.execute(
      "CREATE TRIGGER z_tr AFTER INSERT ON z_tbl FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO trigger_audit VALUES (''z'')')",
    );
    db.execute(
      "CREATE TRIGGER a_tr AFTER INSERT ON a_tbl FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO trigger_audit VALUES (''a'')')",
    );

    final snapshot = db.schema.getSchemaSnapshot();

    final tableNames = snapshot.tables.map((t) => t.name).toList();
    final viewNames = snapshot.views.map((v) => v.name).toList();
    final indexNames = snapshot.indexes.map((i) => i.name).toList();
    final triggerNames = snapshot.triggers.map((t) => t.name).toList();

    expect(tableNames, equals([...tableNames]..sort()));
    expect(viewNames, equals([...viewNames]..sort()));
    expect(indexNames, equals([...indexNames]..sort()));
    expect(triggerNames, equals([...triggerNames]..sort()));
  });
}
