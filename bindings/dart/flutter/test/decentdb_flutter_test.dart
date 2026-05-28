import 'dart:typed_data';

import 'package:decentdb_flutter/decentdb_flutter.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test(
    'buildOpenOptions redacts key-provider conflicts and zeroes copies',
    () async {
      final key = Uint8List.fromList(<int>[1, 2, 3, 4]);
      final options = await DecentDbMobile.buildOpenOptions(
        options: 'write_queue_enabled=true',
        keyProvider: StaticDecentDbKeyProvider(key),
      );

      expect(options, 'write_queue_enabled=true;encryption_key_hex=01020304');
      expect(
        DecentDbMobile.openOptionsSummary(options),
        'write_queue_enabled=true;encryption_key_hex=<redacted>',
      );

      await expectLater(
        DecentDbMobile.buildOpenOptions(
          options: 'encryption_key=plain',
          keyProvider: StaticDecentDbKeyProvider(key),
        ),
        throwsArgumentError,
      );
    },
  );

  test('databaseSetPaths returns the v1 mobile sidecar set', () {
    expect(DecentDbMobile.databaseSetPaths('/app/data/app.ddb'), <String>[
      '/app/data/app.ddb',
      '/app/data/app.ddb.wal',
      '/app/data/app.ddb.sync-journal',
      '/app/data/app.ddb.coord',
    ]);
  });

  test('diagnostics redact options and include support metadata', () {
    final diagnostics = DecentDbMobile.diagnostics(
      databasePath: '/app/data/app.ddb',
      options: 'encryption_key=plain;process_coordination=required',
    ).toJson();

    expect(diagnostics['package'], DecentDbMobile.packageName);
    expect(diagnostics['open_options'], isNot(contains('plain')));
    expect(
      diagnostics['database_set'],
      contains('/app/data/app.ddb.sync-journal'),
    );
  });
}
