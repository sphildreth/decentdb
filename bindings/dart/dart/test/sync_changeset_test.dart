import 'dart:io';

import 'package:decentdb/decentdb.dart';
import 'package:test/test.dart';

import 'test_utils.dart';

void main() {
  late String libPath;

  setUpAll(() {
    libPath = findNativeLib();
  });

  test('changeset wrappers round-trip and apply before relay ack', () async {
    final tempDir = Directory.systemTemp.createTempSync('decentdb_sync_dart_');
    final srcPath = '${tempDir.path}/src.ddb';
    final dstPath = '${tempDir.path}/dst.ddb';
    Database? src;
    Database? dst;

    try {
      src = Database.open(srcPath, libraryPath: libPath);
      dst = Database.open(dstPath, libraryPath: libPath);
      for (final db in [src, dst]) {
        db.execute('CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)');
      }

      expect(src.sync.initReplica('src')['replica_id'], 'src');
      expect(dst.sync.initReplica('dst')['replica_id'], 'dst');

      src.execute("INSERT INTO items VALUES (1, 'one')");
      final changeset = src.sync.createChangeset({
        'source': {
          'kind': 'checkpoint',
          'peer': 'dst',
          'since_sequence': 0,
        },
      });
      expect(changeset['changeset_version'], 1);
      expect(changeset['records'], isA<List>());

      final inspection = src.sync.inspectChangeset(
        changeset,
        options: {'check_local_compatibility': true},
      );
      expect(inspection['valid_envelope'], true);
      expect(inspection['record_count'], 1);

      var acknowledged = false;
      final apply = await dst.sync.applyBeforeAck(changeset, () {
        expect(
          dst!.query('SELECT COUNT(*) AS cnt FROM items').single['cnt'],
          1,
        );
        acknowledged = true;
      });
      expect(acknowledged, isTrue);
      expect(apply['outcome'], 'applied');
      expect(apply['rows_applied'], 1);
      expect(
        dst.query('SELECT name FROM items WHERE id = 1').single['name'],
        'one',
      );

      final replay = dst.sync.applyChangeset(changeset);
      expect(replay['outcome'], 'already_applied');

      final inverse = src.sync.invertChangeset(changeset);
      final records = inverse['records'] as List<Object?>;
      expect((records.single as Map)['operation'], 'delete');
    } finally {
      src?.close();
      dst?.close();
      tempDir.deleteSync(recursive: true);
    }
  });
}
