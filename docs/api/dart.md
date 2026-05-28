# Dart binding

DecentDB ships two Dart-facing layers:

- `bindings/dart/dart/` — the packaged `decentdb` Dart API
- `bindings/dart/flutter/` — the `decentdb_flutter` mobile package for
  Flutter apps on Android and iOS
- `tests/bindings/dart/` — the smoke-validation package used in CI and repository validation

## Native library requirement

Build the shared library from the repository root:

```bash
cargo build -p decentdb
```

The Dart package loads the built shared library from `DECENTDB_NATIVE_LIB` or an explicit `libraryPath`:

- Linux: `target/debug/libdecentdb.so`
- macOS: `target/debug/libdecentdb.dylib`
- Windows: `target/debug/decentdb.dll`

For Flutter/Dart desktop packaging, GitHub Releases also publish small
platform-native archives that contain just the FFI library:

- `decentdb-dart-native-<tag>-Linux-x64.tar.gz`
- `decentdb-dart-native-<tag>-Linux-arm64.tar.gz`
- `decentdb-dart-native-<tag>-macOS-arm64.tar.gz`
- `decentdb-dart-native-<tag>-Windows-x64.zip`

Each archive extracts to the platform-native library file
(`libdecentdb.so`, `libdecentdb.dylib`, or `decentdb.dll`) so desktop apps can
bundle it directly.

## Flutter Mobile Package

`decentdb_flutter` is the first-class mobile package. It keeps the SQL/database
API in the pure Dart `decentdb` package and adds mobile packaging, path,
key-provider, sidecar, lifecycle, and diagnostics helpers.

Initial mobile support tiers:

| Platform | Target | Tier |
|---|---|---|
| Android Flutter app-private storage | API 26+, `arm64-v8a` device, `x86_64` emulator | Tier 2 until a documented real-device lane exists |
| iOS Flutter app-private Application Support storage | iOS 15+, arm64 device, x86_64 simulator | Tier 2 until a documented real-device lane exists |
| Android widgets/services/providers sharing one DB | Multiprocess mobile access | Candidate/unsupported until separately tested |
| iOS app extensions sharing one DB | App group storage | Candidate/unsupported until separately tested |

The mobile package uses default native loading in the happy path:

- Android: `DynamicLibrary.open('libdecentdb.so')` from the standard
  Flutter/Gradle native library layout.
- iOS: `DynamicLibrary.process()` for the static/XCFramework link model.

Android/iOS candidate artifacts are built by:

```bash
bindings/dart/scripts/build_mobile_android.sh --strict
bindings/dart/scripts/build_mobile_ios.sh --strict
bindings/dart/scripts/check_mobile_artifacts.sh --platform android --artifact-root target/mobile-artifacts/android --strict
bindings/dart/scripts/check_mobile_artifacts.sh --platform ios --artifact-root target/mobile-artifacts/ios --strict
```

Install built artifacts into the mobile package layout with:

```bash
bindings/dart/scripts/install_mobile_artifacts.sh
```

The dedicated `Mobile Native Artifacts` GitHub Actions workflow builds unsigned
candidate Android and iOS artifacts on tag pushes and manual dispatch.
Use `bindings/dart/scripts/mobile_benchmark_guardrails.sh` to record artifact
size guardrails and the placeholder runtime metrics that device/simulator
benchmark lanes fill in once accepted baselines exist.

### Mobile Quick Start

```dart
import 'package:decentdb_flutter/decentdb_flutter.dart';

final db = await DecentDbMobile.openAppDatabase('app.ddb');
try {
  db.execute('CREATE TABLE IF NOT EXISTS items (id INT64 PRIMARY KEY, name TEXT)');
} finally {
  db.close();
}
```

The v1 mobile database-set helper returns the files that app backup, restore,
export, and delete workflows must treat together:

```dart
final path = await DecentDbMobile.appDatabasePath('app.ddb');
final files = DecentDbMobile.databaseSetPaths(path);
```

That set is `app.ddb`, `app.ddb.wal`, `app.ddb.sync-journal`, and
`app.ddb.coord` when process coordination is enabled. Future authoritative
sidecars require a spec or ADR update before mobile backup/restore obligations
change.

`DecentDbMobile.noBackupDatabasePath(name)` returns an app-private device-local
subdirectory path for replicas or caches that should not be restored. The host
app is still responsible for any platform-specific backup-exclusion flag.

### Mobile Lifecycle

Mobile apps should use one owning isolate per native database handle. The
recommended async pattern is `AsyncDatabase`; its worker isolate owns the
native handle and all pending futures fail with `AsyncDatabaseClosed` if the
worker closes or terminates before replying.

On background/inactive transitions, stop admitting new app writes, finish or
cancel in-flight work, optionally checkpoint, and close when no OS-approved
background task is active. After process kill or isolate restart, old
`Database`, `AsyncDatabase`, `Statement`, and `AsyncStatement` objects are
invalid; reopen the database and recreate statements.

Reactive watch/change-stream wrappers are not exposed by the Flutter mobile
package yet. The native C ABI has watch foundations, but mobile background,
close, and restart semantics need dedicated lifecycle tests before the package
claims a stable reactive mobile API.

### Mobile Diagnostics

Use redacted summaries for logging and support bundles:

```dart
final summary = DecentDbMobile.openOptionsSummary(
  'encryption_key_hex=001122;process_coordination=required',
);
```

`DecentDbAbiMismatchException` reports the expected ABI, loaded ABI, artifact
source when known, and recovery guidance. Align the `decentdb`,
`decentdb_flutter`, and native artifact versions when this error appears.

## Quick start

```dart
import 'package:decentdb/decentdb.dart';

void main() {
  final db = Database.open(
    'app.ddb',
    libraryPath: '/absolute/path/to/libdecentdb.so',
  );

  db.execute('CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT NOT NULL)');

  final insert = db.prepare(r'INSERT INTO users VALUES ($1, $2)');
  insert.bindAll([1, 'Ada']);
  insert.execute();
  insert.dispose();

  final rows = db.query('SELECT id, name FROM users ORDER BY id');
  print(rows.single['name']);

  db.close();
}
```

## Open modes and lifecycle

The Dart wrapper now exposes distinct open modes backed by the stable C ABI:

- `Database.open(path, ...)` — open-or-create
- `Database.create(path, ...)` — create-only
- `Database.openExisting(path, ...)` — open-only
- `Database.memory(...)`
- `Database.close()`
- `Database.inTransaction`

The package also installs a Dart `Finalizer` so a leaked `Database` handle is still released if the object is garbage-collected, but callers should still close explicitly.

Local on-disk databases can require cross-process WAL coordination through
typed open parameters:

```dart
final db = Database.open(
  'app.ddb',
  processCoordination: ProcessCoordinationMode.required,
  processCoordinationTimeoutMs: 30000,
);
```

## Statement API

`Statement` is now backed by native prepared statements (`ddb_stmt_t`), not by re-sending the SQL text for every execution.

Available operations include:

- `bindNull`, `bindInt64`, `bindBool`, `bindFloat64`, `bindText`, `bindBlob`, `bindDecimal`, `bindDateTime`
- `bindAll([...])`
- `reset()` and `clearBindings()`
- `execute()`
- `query()`
- `step()` / `readRow()`
- `nextPage(pageSize)`
- `dispose()`

Supported high-level bind values in `bindAll(...)`:

- `null`
- `int`
- `bool`
- `double`
- `String`
- `Uint8List`
- `DateTime`
- `DecimalValue`

### Result type mapping

Rows returned from `query()`, `step()` / `readRow()`, and `nextPage()` decode
native values into Dart objects:

| DecentDB type | Dart result value |
|---|---|
| `INT64` | `int` |
| `FLOAT64` | `double` |
| `BOOL` | `bool` |
| `TEXT` | `String` |
| `BLOB`, `UUID`, `GEOMETRY`, `GEOGRAPHY` | `Uint8List` |
| `DECIMAL` | `DecimalValue` |
| `TIMESTAMP` | UTC `DateTime` |
| `ENUM` | `DecentDBEnumValue(typeId, labelId)` |
| `IPADDR` / `INET` | canonical `String` |
| `CIDR` | canonical `String` |
| `DATE` | UTC `DateTime` at midnight |
| `TIME` | `Duration` since midnight |
| `TIMESTAMPTZ` | UTC `DateTime` |
| `INTERVAL` | `DecentDBIntervalValue(months, days, microseconds)` |
| `MACADDR` / `MACADDR8` | canonical lowercase `String` |

String parameters can be used for typed semantic columns when the SQL target
column is known.

Rows use an O(1) column-name index map, so `row['column_name']` no longer performs a linear scan.

### Streaming and pagination

`step()` and `nextPage()` stream from native row-view buffers without materializing the full result set in Dart:

```dart
final stmt = db.prepare('SELECT id, name FROM users ORDER BY id');

while (stmt.step()) {
  final row = stmt.readRow();
  print(row['name']);
}

stmt.dispose();
```

```dart
final stmt = db.prepare('SELECT id, name FROM users ORDER BY id');

while (true) {
  final page = stmt.nextPage(128);
  for (final row in page.rows) {
    print(row['name']);
  }
  if (page.isLast) break;
}

stmt.dispose();
```

`query()` still returns all rows but internally chunks at 256 rows via the streaming path. `nextPage()` invalidates any row from a prior `step()` call, and vice versa. Binding, resetting, or clearing bindings also invalidates streaming state.

### Batch execution

Batch helpers execute many rows in a single FFI call, which is significantly faster than per-row bind/execute loops:

```dart
final stmt = db.prepare(r'INSERT INTO users VALUES ($1, $2)');
db.transaction(() {
  stmt.executeBatchTyped('it', [
    [1, 'Alice'],
    [2, 'Bob'],
    [3, 'Charlie'],
  ]);
});
stmt.dispose();
```

Available batch methods:

- `executeBatchInt64(List<int> values)` — one-column INT64 batch
- `executeBatchI64TextF64(List<(int, String, double)> rows)` — `(INT64, TEXT, FLOAT64)` triple batch
- `executeBatchTyped(String signature, List<List<Object?>> rows)` — mixed-type batch using an `i`/`t`/`f` signature string

### Re-execute helpers

Re-execute helpers combine reset, bind, and execute into a single FFI call for hot DML loops:

```dart
final stmt = db.prepare(r'UPDATE counters SET val = $1 WHERE id = 1');
stmt.rebindInt64Execute(42);
stmt.dispose();
```

Available re-execute methods:

- `rebindInt64Execute(int value)` — reset, bind INT64 at position 1, execute
- `rebindTextInt64Execute(String text, int value)` — reset, bind `(TEXT, INT64)`, execute
- `rebindInt64TextExecute(int value, String text)` — reset, bind `(INT64, TEXT)`, execute

### Fused bind+step helpers

For extremely hot query paths, fused helpers combine binding and stepping into a single FFI boundary crossing:

```dart
final stmt = db.prepare('SELECT id, name, score FROM t WHERE id = $1');

// Single-row lookup returning a primitive tuple
final result = stmt.bindInt64StepI64TextF64(1, 42); 
if (result != null) {
  print('Name: ${result.$2}');
}

stmt.dispose();
```

Available fused methods:

- `bindInt64Step(int index, int value)` — bind INT64 and stream one row view (returns `true` if row available, use `readRow()`)
- `bindInt64StepI64TextF64(int index, int value)` — bind INT64 and return a strongly-typed `(int, String, double)?` tuple directly

## Schema helpers

The packaged wrapper exposes:

- `db.schema.listTables()` / `listTablesInfo()`
- `db.schema.describeTable(name)` / `getTableColumns(name)`
- `db.schema.getTableDdl(name)`
- `db.schema.listIndexes()`
- `db.schema.listViews()` / `listViewsInfo()`
- `db.schema.getViewDdl(name)`
- `db.schema.listTriggers()`
- `db.schema.getToolingMetadata()`
- `db.schema.describeQueryContract(sql)`

### Rich schema snapshot

`getSchemaSnapshot()` returns the complete schema in one call with rich typed metadata:

```dart
final snapshot = db.schema.getSchemaSnapshot();
print('v${snapshot.snapshotVersion}, cookie=${snapshot.schemaCookie}');

for (final table in snapshot.tables) {
  print('Table ${table.name} (temp=${table.temporary}, rows=${table.rowCount})');
  print('  DDL: ${table.ddl}');

  for (final fk in table.foreignKeys) {
    print('  FK: ${fk.columns} -> ${fk.referencedTable}(${fk.referencedColumns})');
  }

  for (final column in table.columns) {
    if (column.generatedSql != null) {
      print('  Generated: ${column.name} = ${column.generatedSql} (${column.generatedStored ? "STORED" : "VIRTUAL"})');
    }
    for (final check in column.checks) {
      print('  Check: ${check.name ?? "<unnamed>"}: ${check.expressionSql}');
    }
  }
}
```

The snapshot model includes:

- `SchemaSnapshot` — top-level container with `tables`, `views`, `indexes`, `triggers`
- `SchemaTableInfo` — DDL, row count, primary key columns, foreign keys, check constraints, generated columns
- `SchemaViewInfo` — DDL, SQL text, column names, dependencies
- `SchemaIndexInfo` — DDL, kind, uniqueness, partial-index predicate, include columns
- `SchemaTriggerInfo` — DDL, target kind, timing, events, event mask, for-each-row flag
- `SchemaCheckConstraintInfo` — optional name and expression SQL

All collections are deterministically ordered by name.

## Branch and snapshot workflow

`Database.branchWorkflow` exposes native named-snapshot and branch workflows
through the stable C ABI. Branch-local SQL can target `main` or any named branch
and supports typed positional parameters.

```dart
final workflow = db.branchWorkflow;

final baseline = workflow.createSnapshot('baseline');
final scratch = workflow.createBranch('scratch', from: baseline.name);

final write = workflow.executeSql(
  scratch.name,
  r'INSERT INTO users VALUES ($1, $2)',
  [3, 'Carol'],
);
print('affected=${write.affectedRows}');

final page = workflow.querySql(
  scratch.name,
  r'SELECT id, name FROM users WHERE id >= $1 ORDER BY id',
  params: [1],
  pageSize: 100,
);
for (final row in page.rows) {
  print("${row['id']}: ${row['name']}");
}

final diff = workflow.diff('main', scratch.name);
print('added=${diff.addedRowCount}, updated=${diff.updatedRowCount}');

final preview = workflow.merge(scratch.name, 'main', dryRun: true);
print('clean=${preview.clean}, conflicts=${preview.conflictCount}');
```

Available operations:

- `createSnapshot(name)` / `listSnapshots()` / `deleteSnapshot(name)`
- `createBranch(name, from: ref)` / `listBranches()` / `deleteBranch(name)`
- `renameBranch(name, newName)`
- `commitBranch(name, message)` / `branchLog(name)`
- `diff(leftRef, rightRef)`
- `restore(branchName, targetRef, dryRun: true)`
- `merge(sourceBranch, targetRef, dryRun: true)`
- `executeSql(branchName, sql, [params])`
- `querySql(branchName, sql, params: [...], pageSize: n)`

Branch references can be `main`, a branch name, a named snapshot, or a head ID
where the operation supports historical refs. `restore` and `merge` default to
dry-run mode so callers can inspect the effect before mutating branch state.

Branch SQL parameters support `null`, `int`, `bool`, `double`, `String`,
`Uint8List`, `DateTime`, `DecimalValue`, and `UuidValue`. Results use the same
`Row` and `ResultPage` shapes as normal queries; `executeSql` returns a
`BranchExecutionResult` with `columns`, `rows`, `affectedRows`, `returnsRows`,
and `firstPage(pageSize)`.

The branch model types are:

- `BranchInfo` — branch identity, current/base head IDs, timestamps, and
  `isMain`
- `NamedSnapshot` — retained snapshot identity, source branch/head, LSN, and
  timestamp
- `BranchLogEntry` — head history entries with parent head, optional message,
  optional SQL, and timestamp
- `BranchDiffReport`, `BranchTableDiff`, and `BranchRowDiff` — row-level
  primary-key diff summaries
- `BranchRestoreReport` — dry-run or applied restore summary
- `BranchMergeReport`, `BranchMergeChange`, and `BranchMergeConflict` — merge
  preview/application summaries and conflict details

## WAL maintenance

```dart
Database.evictSharedWal(
  '/path/to/database.ddb',
  libraryPath: '/path/to/libdecentdb.so',
);
```

Evicts the shared WAL cache entry for an on-disk database. Call only after all handles for that path are closed.

## Validation commands

Package suite:

```bash
bindings/dart/scripts/run_tests.sh
```

Manual package validation:

```bash
cargo build -p decentdb
cd bindings/dart/dart
dart analyze lib/ test/ benchmarks/
DECENTDB_NATIVE_LIB=../../../target/debug/libdecentdb.so dart test --reporter expanded
```

Smoke path:

```bash
cargo build -p decentdb
cd tests/bindings/dart
dart pub get
dart run smoke.dart
```

Console example:

```bash
cd bindings/dart/examples/console
dart pub get
DECENTDB_NATIVE_LIB=../../../../target/debug/libdecentdb.so dart run main.dart
```

Benchmark:

```bash
cd bindings/dart/dart
dart pub get
DECENTDB_NATIVE_LIB=../../../target/debug/libdecentdb.so dart run benchmarks/bench_fetch.dart --count 100000 --point-reads 5000 --fetchmany-batch 1024 --db-prefix dart_bench_fetch
```

## Notes
- `Database.open(options: ...)` passes native open options through the stable C
  ABI, including cross-process coordination and write-queue options
- `Database.executeQueued(sql)` and `Database.writeQueueMetrics()` expose the
  engine-owned write queue for self-contained queued writes
- the example under `bindings/dart/examples/flutter_desktop/` is still a desktop-oriented reference rather than a real Flutter SDK app
- DecentDB remains a one-writer / many-readers engine; keep that concurrency model in mind when sharing database handles across isolates or threads
