# DecentDB Dart Binding

The in-tree Dart package wraps the stable Rust `ddb_*` C ABI with a small,
idiomatic Dart API for desktop and CLI applications.

Flutter mobile apps should use the sibling `bindings/dart/flutter`
`decentdb_flutter` package. It delegates SQL/database work to this Dart package
and adds Android/iOS packaging, app-private path helpers, key-provider wiring,
redacted diagnostics, mobile lifecycle guidance, and reference examples.

## What is covered today

- `Database.open()` / `Database.create()` / `Database.openExisting()` /
  `Database.memory()` / `Database.close()`
- `Database.inTransaction` query helper backed by `ddb_db_in_transaction`
- `Database.evictSharedWal(path)` maintenance helper for shared WAL cache cleanup
- `Database` `Finalizer` – the native handle is released by the GC if `close()`
  is never called
- One-shot `execute()`, `executeWithParams()`, and `query()`
- Native prepared statements (`ddb_stmt_t`) backing every `Statement` object –
  SQL is compiled once and the query plan is reused across executions
- `Statement.step()` and `Statement.nextPage()` stream rows from the native cursor
  without a Dart-side full-result backing store
- Fast paths for high-throughput workloads:
  `executeBatchInt64`, `executeBatchI64TextF64`, `executeBatchTyped`,
  `rebindInt64Execute`, `rebindTextInt64Execute`, `rebindInt64TextExecute`
- Efficient row decoding: a single `DdbValue` allocation is reused for every
  cell in a result set; a shared `Map<String, int>` index is built once per
  result and shared across all rows for O(1) named-column access via `row['col']`
- Typed bind methods call `ddb_stmt_bind_*` directly:
  `bindNull`, `bindInt64`, `bindBool`, `bindFloat64`, `bindText`, `bindBlob`,
  `bindDecimal`, `bindDateTime`
- `Statement.reset()` / `clearBindings()` / `dispose()` map to native
  `ddb_stmt_reset` / `ddb_stmt_clear_bindings` / `ddb_stmt_free`
- Transaction helpers: `begin()`, `commit()`, `rollback()`, `transaction()`
- Maintenance helpers: `checkpoint()` and `saveAs()`
- Schema metadata via `Schema.listTables()`, `describeTable()`, `listIndexes()`,
  `listViews()`, `getTableDdl()`, `getViewDdl()`, and `listTriggers()`
- Rich schema metadata via `Schema.getSchemaSnapshot()` with typed Dart models
  (`SchemaSnapshot`, `SchemaTableInfo`, `SchemaColumnInfo`, `SchemaViewInfo`,
  `SchemaIndexInfo`, `SchemaTriggerInfo`, `SchemaCheckConstraintInfo`)
- Stable tooling metadata and query contracts via `Schema.getToolingMetadata()`
  and `Schema.describeQueryContract(sql)`
- Native branch and named-snapshot workflows via `Database.branchWorkflow`,
  including snapshot list/create/delete, branch list/create/delete/rename,
  branch commit/log/diff/restore/merge, and branch-local SQL execution/query
  with typed positional parameters
- Sync JSON and public changeset helpers via `Database.sync`, including
  status/init, changeset create/inspect/apply/invert, and apply-before-ack
  ordering for relay clients
- Mobile-neutral default native loading for Android/iOS package layouts plus
  typed native-load and ABI-mismatch exceptions
- Redacted open-option diagnostics for `encryption_key`, `encryption_key_hex`,
  `tde_key`, and `tde_key_hex`
- `ErrorCode.fromCode` throws `StateError` on unrecognised codes
- `sqlite3` moved to `dev_dependencies` (only used by the benchmark)

## Build the native library

From the repository root:

```bash
cargo build -p decentdb
```

The Rust `cdylib` is emitted to:

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

You can also use the helper script:

```bash
bindings/dart/scripts/build_native.sh
```

For mobile CI artifact candidates, there are optional helper scripts:

```bash
bindings/dart/scripts/build_mobile_android.sh
bindings/dart/scripts/build_mobile_ios.sh
bindings/dart/scripts/check_mobile_artifacts.sh
bindings/dart/scripts/install_mobile_artifacts.sh
bindings/dart/scripts/mobile_benchmark_guardrails.sh
```

These scripts build candidate Android/iOS mobile artifacts, write a `version.txt`
file with build metadata, emit a `checksums.sha256` index for uploaded
packaged outputs, install artifacts into the Flutter package layout, and record
initial mobile artifact-size guardrails.

## Run the Dart package tests

```bash
bindings/dart/scripts/run_tests.sh
```

That script builds the shared library, runs `dart pub get`, and executes the
package suite in `bindings/dart/dart/test/decentdb_test.dart`.

## Run the Dart benchmark

From the repository root:

```bash
cargo build -p decentdb --release
cd bindings/dart/dart
dart pub get
DECENTDB_NATIVE_LIB=../../../target/release/libdecentdb.so dart run benchmarks/bench_fetch.dart --count 100000 --point-reads 5000 --fetchmany-batch 1024 --db-prefix dart_bench_fetch
```

Benchmark CLI options:

- `--engine <all|decentdb|sqlite>`
- `--count <n>`
- `--point-reads <n>`
- `--fetchmany-batch <n>`
- `--point-seed <n>`
- `--db-prefix <prefix>` (DecentDB writes `.ddb`, SQLite writes `.db`)
- `--keep-db`

## Quick start

```dart
import 'package:decentdb/decentdb.dart';

void main() {
  final db = Database.open(
    'mydata.ddb',
    libraryPath: '/absolute/path/to/libdecentdb.so',
  );

  db.execute('CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT NOT NULL)');
  db.execute("INSERT INTO users VALUES (1, 'Alice')");

  final rows = db.query('SELECT id, name FROM users ORDER BY id');
  for (final row in rows) {
    print("${row['id']}: ${row['name']}");
  }

  db.close();
}
```

## Parameter binding and paging

```dart
final insert = db.prepare(r'INSERT INTO users VALUES ($1, $2)');
insert.bindAll([2, 'Bob']);
insert.execute();
insert.dispose();

final select = db.prepare('SELECT id, name FROM users ORDER BY id');
while (true) {
  final page = select.nextPage(100);
  for (final row in page.rows) {
    print(row['name']);
  }
  if (page.isLast) break;
}
select.dispose();
```

Supported Dart bind values in the tested wrapper path are:

- `null`
- `int`
- `bool`
- `double`
- `String`
- `Uint8List`
- `DateTime`
- `DecimalValue`

Semantic result values decode to Dart-native shapes:

- `ENUM` -> `DecentDBEnumValue(typeId, labelId)`
- `IPADDR`, `CIDR`, `MACADDR` -> canonical `String`
- `DATE`, `TIMESTAMPTZ` -> UTC `DateTime`
- `TIME` -> `Duration`
- `INTERVAL` -> `DecentDBIntervalValue(months, days, microseconds)`

## Branch and snapshot workflow

```dart
final workflow = db.branchWorkflow;

final baseline = workflow.createSnapshot('baseline');
final branch = workflow.createBranch('scratch', from: baseline.name);

workflow.executeSql(
  branch.name,
  r'INSERT INTO users VALUES ($1, $2)',
  [3, 'Carol'],
);

final page = workflow.querySql(
  branch.name,
  'SELECT id, name FROM users ORDER BY id',
  pageSize: 100,
);
for (final row in page.rows) {
  print('${row['id']}: ${row['name']}');
}

final diff = workflow.diff('main', branch.name);
print('Changed rows: ${diff.addedRowCount + diff.updatedRowCount + diff.deletedRowCount}');

final mergePreview = workflow.merge(branch.name, 'main', dryRun: true);
print('Merge clean: ${mergePreview.clean}');
```

## Sync changesets

The Dart package exposes the stable C ABI sync JSON and public changeset entry
points through `Database.sync`:

```dart
db.sync.initReplica('mobile-a');
final changeset = db.sync.createChangeset({
  'source': {
    'kind': 'checkpoint',
    'peer': 'relay',
    'since_sequence': 0,
  },
});

await db.sync.applyBeforeAck(changeset, () async {
  await relay.ack(changeset);
});
```

`applyBeforeAck` preserves the relay invariant: local durable apply happens
before acknowledgement. If the process dies before ack, the relay can redeliver.

## Schema metadata

```dart
final tables = db.schema.listTables();
final users = db.schema.describeTable('users');
final ddl = db.schema.getTableDdl('users');
final indexes = db.schema.listIndexes();
final views = db.schema.listViewsInfo();
final triggers = db.schema.listTriggers();
```

For full metadata fidelity (checks, FKs, generated columns, canonical DDL, temp
objects), use:

```dart
final snapshot = db.schema.getSchemaSnapshot();
for (final table in snapshot.tables) {
  print('${table.name} temp=${table.temporary} rows=${table.rowCount}');
  for (final check in table.checks) {
    print('  check: ${check.name ?? '<unnamed>'} => ${check.expressionSql}');
  }
}
```

## Flutter desktop notes

Bundle the Rust shared library with your application and pass the resolved path
into `Database.open(..., libraryPath: ...)`. See
`bindings/dart/examples/flutter_desktop/main.dart` for a minimal reference.

## Current limitations

- `Database.open(options: ...)`, `Database.create(options: ...)`, and
  `Database.openExisting(options: ...)` pass native open options through the
  stable C ABI. Typed coordination parameters include
  `processCoordination: ProcessCoordinationMode.required` and
  `processCoordinationTimeoutMs: 30000`. Queue options include `write_queue_enabled`,
  `write_queue_capacity`, `write_queue_default_timeout_ms`,
  `write_queue_strict_group_commit`, `write_queue_max_batch`, and
  `write_queue_max_group_delay_us`.
- `Database.executeQueued(sql)` and `Database.writeQueueMetrics()` expose the
  engine-owned write queue for self-contained queued writes.
- The package uses the stable C ABI from `include/decentdb.h`; the reference
  header under `bindings/dart/native/decentdb.h` includes that file so the two
  surfaces stay in sync.
- Two C ABI fused bind+step helpers are not wrapped yet:
  `ddb_stmt_bind_int64_step_row_view` and
  `ddb_stmt_bind_int64_step_i64_text_f64`.
