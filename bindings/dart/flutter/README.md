# DecentDB Flutter Mobile Package

`decentdb_flutter` is the Flutter mobile companion to the in-tree `decentdb`
Dart FFI package. The Dart package stays authoritative for the public database
API; this package adds mobile storage helpers, key-provider wiring, and native
artifact registration for Android and iOS.

## Supported mobile targets

- Android API 26+, `arm64-v8a` and `x86_64`
- iOS 15+, device `arm64` and simulator `x86_64`

Android loads `libdecentdb.so` with `DynamicLibrary.open('libdecentdb.so')`.
iOS uses process-linked symbols through `DynamicLibrary.process()` when the
XCFramework/static library is linked into the Flutter app.

## Native artifacts

Build candidate artifacts from the repository root:

```bash
bindings/dart/scripts/build_mobile_android.sh --strict
bindings/dart/scripts/build_mobile_ios.sh --strict
```

Copy Android libraries into:

- `android/src/main/jniLibs/arm64-v8a/libdecentdb.so`
- `android/src/main/jniLibs/x86_64/libdecentdb.so`

Copy the iOS XCFramework into:

- `ios/Frameworks/decentdb.xcframework`

Or install both platforms from the default artifact directories:

```bash
bindings/dart/scripts/install_mobile_artifacts.sh
```

Record package-size guardrails with:

```bash
bindings/dart/scripts/mobile_benchmark_guardrails.sh
```

The `Mobile Native Artifacts` GitHub Actions workflow builds unsigned candidate
artifacts and uploads zip files for inspection or release packaging.

## Open a database

```dart
import 'package:decentdb_flutter/decentdb_flutter.dart';

final db = await DecentDbMobile.openAppDatabase('app.ddb');
try {
  db.execute('CREATE TABLE IF NOT EXISTS items (id INT64 PRIMARY KEY, name TEXT)');
} finally {
  db.close();
}
```

Encrypted opens use an app-owned secure key provider:

```dart
final db = await DecentDbMobile.openAppDatabase(
  'app.ddb',
  keyProvider: MyKeychainBackedProvider(),
);
```

Never log raw open options. Use `DecentDbMobile.openOptionsSummary(options)` or
`redactSensitiveOpenOptions(options)` before diagnostics.

## Database file set

The v1 mobile backup/delete set is:

- `app.ddb`
- `app.ddb.wal`
- `app.ddb.sync-journal`
- `app.ddb.coord` when process coordination is enabled

Use `DecentDbMobile.databaseSetPaths(path)` to enumerate these paths.
Use `DecentDbMobile.noBackupDatabasePath(name)` for device-local replicas or
sync-derived caches when the host app also configures the needed platform
backup-exclusion policy.

Reactive watch wrappers are not exposed in the first mobile package. Use query
refreshes after writes until the watch lifecycle is documented and tested for
mobile foreground/background behavior.

## Reference app

`example/` contains a small Flutter app that opens an encrypted app-private
database, creates schema, uses prepared statements and transactions, checkpoints
and closes during lifecycle pauses, exports a copy with `saveAs`, and exercises
the sync status path. It is intentionally built on the same public helpers used
by package consumers.
