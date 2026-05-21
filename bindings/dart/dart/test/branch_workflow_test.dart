import 'dart:io';
import 'dart:typed_data';

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
    db.execute(
      'CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT NOT NULL)',
    );
    db.execute("INSERT INTO items VALUES (1, 'main')");
  });

  tearDown(() {
    db.close();
  });

  test('manages snapshots and branches through the public Dart API', () {
    final workflow = db.branchWorkflow;

    final baseline = workflow.createSnapshot('baseline');
    expect(baseline.name, 'baseline');
    expect(
      workflow.listSnapshots().map((snapshot) => snapshot.name),
      contains('baseline'),
    );

    final scratch = workflow.createBranch('scratch', from: 'baseline');
    expect(scratch.name, 'scratch');
    expect(scratch.currentHeadId, isNotNull);
    expect(
      workflow.listBranches().map((branch) => branch.name),
      containsAll(<String>['main', 'scratch']),
    );

    final write = workflow.executeSql(
      'scratch',
      r'INSERT INTO items VALUES ($1, $2)',
      <Object?>[2, 'branch'],
    );
    expect(write.affectedRows, 1);
    expect(write.returnsRows, isFalse);

    final firstPage = workflow.querySql(
      'scratch',
      r'SELECT id, name FROM items WHERE id >= $1 ORDER BY id',
      params: <Object?>[1],
      pageSize: 1,
    );
    expect(firstPage.rows.map((row) => row['id']).toList(), <int>[1]);
    expect(firstPage.isLast, isFalse);

    final branchRows = workflow.executeSql(
      'scratch',
      'SELECT id, name FROM items ORDER BY id',
    );
    expect(branchRows.rows.map((row) => row['name']).toList(), <String>[
      'main',
      'branch',
    ]);
    expect(
      db.query('SELECT COUNT(*) AS cnt FROM items WHERE id = 2').single['cnt'],
      0,
    );

    final diff = workflow.diff('main', 'scratch');
    expect(diff.rightRef, 'scratch');
    expect(diff.addedRowCount, 1);
    expect(diff.tables.where((table) => table.table == 'items'), isNotEmpty);

    final commit = workflow.commitBranch('scratch', 'checkpoint branch state');
    expect(commit.message, 'checkpoint branch state');
    expect(
      workflow.branchLog('scratch').map((entry) => entry.message),
      contains('checkpoint branch state'),
    );

    final restoreDryRun = workflow.restore(
      'scratch',
      'baseline',
      dryRun: true,
    );
    expect(restoreDryRun.dryRun, isTrue);
    expect(restoreDryRun.branch, 'scratch');

    final mergeDryRun = workflow.merge('scratch', 'main', dryRun: true);
    expect(mergeDryRun.dryRun, isTrue);
    expect(mergeDryRun.source, 'scratch');
    expect(mergeDryRun.target, 'main');

    expect(workflow.renameBranch('scratch', 'renamed'), isTrue);
    expect(
      workflow.listBranches().map((branch) => branch.name),
      contains('renamed'),
    );
    expect(workflow.deleteBranch('renamed'), isTrue);
    expect(workflow.deleteSnapshot('baseline'), isTrue);
  });

  test('surfaces native branch workflow errors', () {
    final workflow = db.branchWorkflow;

    expect(
      () => workflow.createBranch('bad', from: 'missing-ref'),
      throwsA(
        isA<DecentDbException>().having(
          (error) => error.message,
          'message',
          contains("unknown branch, snapshot, or head 'missing-ref'"),
        ),
      ),
    );
  });

  test('branch SQL supports Dart bind types across branch replay', () {
    db.execute('''
      CREATE TABLE typed_values (
        id INT64 PRIMARY KEY,
        maybe_null TEXT,
        active BOOL,
        score FLOAT64,
        note TEXT,
        payload BLOB,
        amount DECIMAL,
        created_at TIMESTAMP,
        uid UUID
      )
    ''');

    final workflow = db.branchWorkflow;
    workflow.createSnapshot('typed-baseline');
    workflow.createBranch('typed-branch', from: 'typed-baseline');

    final payload = Uint8List.fromList(<int>[1, 2, 3, 4]);
    final createdAt = DateTime.fromMicrosecondsSinceEpoch(
      1711540800123456,
      isUtc: true,
    );
    final uuid = UuidValue(
      Uint8List.fromList(List<int>.generate(16, (index) => index + 1)),
    );

    workflow.executeSql(
      'typed-branch',
      r'''
        INSERT INTO typed_values
          (id, maybe_null, active, score, note, payload, amount, created_at, uid)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      ''',
      <Object?>[
        1,
        null,
        true,
        12.5,
        "O'Brien",
        payload,
        const DecimalValue(12345, 2),
        createdAt,
        uuid,
      ],
    );

    final row = workflow
        .executeSql(
          'typed-branch',
          'SELECT * FROM typed_values WHERE id = 1',
        )
        .rows
        .single;

    expect(row['maybe_null'], isNull);
    expect(row['active'], true);
    expect(row['score'], 12.5);
    expect(row['note'], "O'Brien");
    expect(row['payload'], payload);
    expect(row['amount'], const DecimalValue(12345, 2));
    expect(row['created_at'], createdAt);
    expect(row['uid'], uuid.bytes);
  });

  test('can use a file database for branch workflow state', () {
    final tempDir = Directory.systemTemp.createTempSync('decentdb_branch_');
    final path = '${tempDir.path}/branches.ddb';
    try {
      final fileDb = Database.open(path, libraryPath: libPath);
      fileDb.execute('CREATE TABLE t (id INT64 PRIMARY KEY)');
      final snapshot = fileDb.branchWorkflow.createSnapshot('persisted');
      expect(snapshot.name, 'persisted');
      fileDb.close();

      final reopened = Database.open(path, libraryPath: libPath);
      expect(
        reopened.branchWorkflow.listSnapshots().map((item) => item.name),
        contains('persisted'),
      );
      reopened.close();
    } finally {
      tempDir.deleteSync(recursive: true);
    }
  });
}
