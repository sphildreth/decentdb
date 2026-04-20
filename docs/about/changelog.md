# Changelog

All notable changes to DecentDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.2.2] - UNRELEASED

### Added (Dart/Flutter binding)

- **UUID bind** (`UuidValue`): `Statement.bindAll` and `Statement.bindUuid` accept `UuidValue` and bind it via the native UUID path instead of a generic BLOB.
- **`inspectStorageState`**: `Database.inspectStorageState()` exposes WAL and cache metrics without requiring a snapshot handle.
- **Savepoint helpers**: `Database.savepoint()`, `Database.releaseSavepoint()`, and `Database.rollbackToSavepoint()` with a `transaction()` overload that wraps work in a named savepoint instead of a bare BEGIN/COMMIT.
- **LRU prepared-statement cache**: `Database.prepare()` now returns a cached `Statement` keyed by SQL (capacity configurable via `stmtCacheCapacity` on all factory methods, default 128). `stmtCacheStats`, `clearStmtCache()`, and `onPerformanceWarning` allow observability and tuning.
- **`PerformanceWarning`** exception class fires (at most once per database) when the statement-cache hit rate falls below 50 % after 100 prepares.
- **Zero-copy batch binding**: `executeBatchTyped`, `executeBatchInt64`, and `executeBatchI64TextF64` use `malloc` for write-only bulk arrays instead of zero-filling with `calloc`.
- **Single-pass `executeBatchTyped`**: rewrote from multi-pass type-probe to single-pass dispatch, reducing FFI call count for mixed-type bulk inserts.
- **Column-metadata caching**: `Statement._loadColumnMetadata` is called at most once per prepared statement; the result is reused across resets and rebinds (eliminates ~30k redundant FFI calls in point-read workloads).
- **Unmodifiable result views**: `columnNames` and `query()` return `UnmodifiableListView` to prevent accidental mutation.
- **`Statement` finalizer**: a `dart:core` `Finalizer<_StmtToken>` calls `stmtFree` on GC'd statements; `dispose()` detaches it to prevent double-free.
- **`isDisposed` getter** on `Statement` for safe cache-lookup checks.
- **`rows()` streaming**: `Statement.rows({int pageSize = 100})` returns a lazy `Stream<Row>` backed by `nextPage`, suitable for large result sets.
- **`AsyncDatabase` and `AsyncStatement`**: an isolate-backed wrapper that moves all FFI calls to a dedicated worker isolate, keeping the caller's event loop unblocked.

### Fixed

- Made WAL page versions cheaply shareable with `Arc<[u8]>` so snapshot reads stop cloning full WAL-resident page buffers on every access.
- Eliminated repeated base-page materialization during WAL recovery by reusing per-page pending reconstruction state across long delta chains.
- Stopped deep-cloning read-mostly execution runtime state by Arc-sharing catalog, table, temp-table, and runtime-index maps with copy-on-write mutation.
- Avoided full dataset clones on CTE alias resolution by sharing dataset row storage and rewriting only column-binding metadata.
- Bounded the per-session cached payload map with an LRU cap from `DbConfig` and Arc-shared temp schema state to avoid repeated deep clones across statements.
- Documented C ABI ownership and free responsibilities for database, statement, and result handles in both the public header and Rust rustdoc.
- Fixed the Go binding BLOB bind path to satisfy cgo pointer rules by pinning Go byte slices during the native call, with a large-blob regression test and strict cgo validation guidance.
- Made Go `DB.Close()` idempotent and detached finalizers on explicit close to prevent double-free races, with direct regression coverage.
- Added Python statement-cache hit/miss stats, a close-time `PerformanceWarning` for low cache hit rates, and documented the new observability surface.
- `CREATE VIEW` no longer executes the SELECT body to derive output column names. A syntactic resolver walks the projection AST instead, falling back to execution only for `*`/`tbl.*` wildcards. Eliminates the cost of running large JOIN/aggregate view bodies during DDL (~94 % faster for the Dart `console_complex` view-creation step) and avoids unintended side effects of the SELECT during view definition.

## [2.2.1] - 2026-04-19

### Fixed

- .NET GUID/UUID parameter binding so native prepared statements, ADO.NET commands, and EF Core modification batches preserve UUID semantics instead of binding GUID values as raw blobs.
- Mixed-case identifier handling across Rust execution/runtime paths used by EF-created schemas, including raw SQL DML resolution and dirty-table persistence tracking after append-only and row-update writes.
- Planner/executor now uses the indexed equi-join fast path for `LEFT JOIN` on an indexed (or rowid-alias) right-side column, not only `INNER JOIN`. Previously LEFT JOINs with a valid B-tree or rowid probe fell back to an O(n·m) nested loop, causing multi-second regressions on mid-sized joined workloads. NULL-extended row semantics are preserved for non-matching left rows and for left rows whose join key is NULL.

### Added

- Regression coverage for typed UUID binding through the C ABI plus ADO.NET and EF Core indexed GUID equality queries.
- Regression coverage for mixed-case EF-created tables remaining reachable from unquoted raw SQL after `EnsureCreated()`.
- Regression coverage for the indexed `LEFT JOIN` fast path: a scaling/perf guard (`indexed_left_join_scales_linearly_not_quadratically`) plus correctness tests covering NULL-extended non-matches (`indexed_left_join_preserves_null_extended_rows`) and multi-row right-side matches (`indexed_left_join_handles_multi_match_right_side`).

## [2.2.0] - 2026-04-18

### Fixed

- EF Core literal generation and `HasData` coverage for provider-converted types, including `DateTime`, `DateTimeOffset`, `DateOnly`, `TimeOnly`, `TimeSpan`, `decimal` scale normalization, and NodaTime literal mappings.
- ADO.NET parameter and reader behavior for additional scalar types (`sbyte`, `char`, unsigned integer overflow guards), with explicit fail-fast behavior where DecentDB SQL literal syntax is not yet available (BLOB `HasData` literals).

### Added

- Expanded .NET regression coverage for literal executability contracts, comprehensive `HasData` matrix scenarios (including nullable cases), and 4 MB BLOB round-trip verification.
- ADR 0134 documenting the engine-level BLOB literal parsing gap and a proposed parser function path.

## [2.1.0] - 2026-04-01

### Fixed

- Decimal `MIN`/`MAX` aggregate ordering in the Rust engine now compares `DECIMAL` values natively during aggregate-extreme evaluation, including mixed decimal scales.
- EF Core decimal aggregate regression for grouped `Max(decimal)` projection shapes by aligning provider behavior with native engine decimal comparison support.
- .NET decimal parameter scale handling during command binding and EF modification batching so configured scale metadata is applied consistently before native decimal binding.
- Foreign key DDL validation for self-referencing and composite-key EF Core schemas so `CREATE TABLE` and migration-generated constraints now validate correctly.
- EF Core migration SQL generation for rename-table, rename-column, alter-column-type, and drop-index operations, plus explicit `NotSupportedException` savepoint behavior instead of leaking later SQL execution errors.
- Rust engine decimal comparison and `SUM`/`AVG` aggregate paths so mixed numeric comparisons and decimal aggregates execute without the earlier EF Core showcase workarounds.
- EF Core translation for `DateTime`, `DateOnly`, and `TimeOnly` member access in predicates, including nullable member-access shapes used by the showcase.
- Fault-injection WAL write classification for compound page-frame plus commit-frame appends so `wal.write_commit` failpoints and crash/reopen coverage continue to target the durable publish boundary after the write-path batching optimization.
- Rust table-payload append-only overflow persistence after checkpoint compaction now patches the on-disk row-count header before appending, preventing duplicate primary-key rows and the overnight memory-safety corruption failure on reopen.

### Added

- Dedicated standalone `decentdb-migrate` CLI tool to seamlessly upgrade databases from unsupported legacy format versions (e.g., Nim-era v3) to the current format.
- `decentdb-cli` now detects legacy format versions and provides a helpful message directing the user to the `decentdb-migrate` tool.
- Exported `DB_FORMAT_VERSION` from the core engine to identify the target database version.
- Added structured error code `DDB_ERR_UNSUPPORTED_FORMAT_VERSION` (8) to the C ABI and mapped it across all language bindings (Dart, Java, Python, Go, Node.js, .NET).
- Provider-specific EF Core window-function LINQ support via `EF.Functions`, covering ranking functions and value window functions rendered as `OVER (...)` SQL.
- Expanded .NET showcase coverage for temporal member predicates, composite foreign keys, and window functions so the sample now exercises the newly supported EF Core surface directly.
- Expanded .NET EF Core validation and docs for server-side set operations, `ExecuteUpdateAsync`, `ExecuteDeleteAsync`, `AsAsyncEnumerable()`, and explicit constraint/savepoint failure contracts.
- EF Core `UseDecentDB(DecentDBConnectionStringBuilder)` overloads so typed connection-string setup can be shared directly between ADO.NET and EF Core configuration.
- Lightweight .NET performance-sanity coverage and showcase guidance for projection-vs-tracked reads, `AsNoTracking`, split-query includes, keyset pagination, async streaming, and bulk mutation rowcount checks.
- Fast Rust regression coverage for the checkpointed append-only overflow primary-key corruption path, wired into `scripts/do-pre-commit-checks.py`.
- Dart binding rich schema snapshot: `Schema.getSchemaSnapshot()` returns a typed model layer covering tables, views, indexes, triggers, check constraints, foreign keys, generated columns, temp-object metadata, and canonical DDL in one call.
- Rust engine rich schema snapshot model (`SchemaSnapshot` and related structs in `metadata.rs`) with a single authoritative builder path in `db.rs` and deterministic name-ordered collections.
- C ABI function `ddb_db_get_schema_snapshot_json` for one-shot schema snapshot JSON retrieval over the stable ABI.
- Dart binding streaming statement refactor: `step()` and `nextPage()` now stream from native row-view buffers without materializing the full result set in Dart. `query()` internally chunks at 256 rows via the streaming path.
- Dart binding fast-path wrappers: `executeBatchInt64`, `executeBatchI64TextF64`, `executeBatchTyped`, `rebindInt64Execute`, `rebindTextInt64Execute`, `rebindInt64TextExecute`, `Database.evictSharedWal`, and fused bind+step helpers `bindInt64Step` and `bindInt64StepI64TextF64`.
- Dart binding test suite expanded with `schema_snapshot_test.dart`, `statement_streaming_test.dart`, and `fast_paths_test.dart` covering rich schema, streaming semantics, and batch/re-execute fast paths.
- Dart benchmark (`bench_fetch.dart`) updated to measure streaming `step()` and `nextPage()` paths instead of the old hidden full-fetch implementation.
- Regression coverage for decimal aggregate correctness:
  - Rust SQL test coverage for `MIN`/`MAX` over `DECIMAL`.
  - EF Core query-shape coverage for grouped decimal aggregate projections.
  - EF Core aggregate-shape coverage across grouped and ungrouped projections for core scalar types.
  - EF Core nullable aggregate-shape coverage, including mixed-null and all-null aggregate inputs.

### Changed

- Decent Bench (`decent-bench`) now uses the rich upstream schema snapshot API instead of narrow v2 types, resolving the metadata regression from the v1-to-v2 migration.
- Decent Bench native library resolver now looks for Rust v2 library names (`libdecentdb.so`, `libdecentdb.dylib`, `decentdb.dll`).
- Dart binding examples updated to demonstrate `executeBatchTyped` for bulk inserts instead of manual per-row bind/execute loops.
- Design and binding review documentation updated to reflect the completed Dart binding v2 surface.
- Rust durable-write hot paths now avoid eager post-commit transaction-state cloning, skip redundant explicit-transaction stale-index rebuilds on prepared fast-path writes, batch WAL page frames with the commit control frame, and move owned page buffers into write-transaction staging to reduce extra copy overhead during overflow and root persistence.

## [2.0.1] - 2026-03-28

### Fixed

- Python binding cursor semantics: `Connection.execute(...)` once again returns a fresh cursor per call instead of mutating a single shared cursor object across successive executions on the same connection.
- Release automation validation for the Python binding suite and Java FFM smoke path so the GitHub release workflow matches the supported dependency/toolchain setup.
- Memory-safety nightly stress coverage to use valid SQL, fail fast on unexpected database errors, and keep the working set bounded with periodic checkpoints instead of drifting into self-inflicted I/O failures.
- Benchmark asset refresh automation on protected `main` branches by pushing the generated asset commit to `automation/benchmark-assets` and opening/updating a PR (or surfacing a manual PR URL) instead of attempting a direct protected-branch push.

### Changed

- Docs deployment now publishes from docs changes merged to `main`, which keeps benchmark-asset refreshes compatible with pull-request-only repository rules without reintroducing the earlier double-publish path.

## [2.0.0] - 2026-03-28

> **v2.0.0 marks the first release of the Rust-native DecentDB engine.**
> The original DecentDB engine was written in [Nim](https://nim-lang.org/).
> For v2.0.0 the engine core was rewritten from scratch in Rust, delivering
> substantially improved memory safety, ACID durability guarantees, a stable
> C ABI (`include/decentdb.h`), and a fully updated binding ecosystem
> (.NET, Python, Go, Java/JDBC/DBeaver, Node.js, Dart). The v1.x Nim-era
> releases are preserved in repository history but are no longer maintained.

### Added

- SQL window enhancements: `NTILE`, `PERCENT_RANK`, `CUME_DIST`, aggregate window functions, and frame-aware execution for `ROWS` and supported `RANGE` bounds.
- Statistical and ordered-set aggregates: `ARRAY_AGG`, `MEDIAN`, `PERCENTILE_CONT ... WITHIN GROUP`, and `PERCENTILE_DISC ... WITHIN GROUP`.
- Trigonometric math functions: `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `PI`, `DEGREES`, `RADIANS`, and `COT`.
- Conditional scalar functions: `GREATEST`, `LEAST`, and `IIF`.
- DML enhancements: `UPDATE ... RETURNING`, `DELETE ... RETURNING`, and `TRUNCATE TABLE` with `RESTART IDENTITY`, `CONTINUE IDENTITY`, and `CASCADE`.
- Subquery comparison operators: `expr op ANY/SOME (subquery)` and `expr op ALL (subquery)`.
- Regex comparison operators: `~`, `~*`, `!~`, `!~*`.
- Extended date/time functions: `DATE_TRUNC`, `DATE_PART`, `DATE_DIFF`, `LAST_DAY`, `NEXT_DAY`, `MAKE_DATE`, `MAKE_TIMESTAMP`, `TO_TIMESTAMP`, `AGE`, and timestamp `INTERVAL` arithmetic.
- Extended string functions: `CONCAT`, `CONCAT_WS`, `POSITION`, `INITCAP`, `ASCII`, `REGEXP_REPLACE`, `SPLIT_PART`, `STRING_TO_ARRAY`, `QUOTE_IDENT`, `QUOTE_LITERAL`, `MD5`, and `SHA256`.
- Query features slice (S9): standalone `VALUES` query bodies, `VALUES` table sources with alias column naming, `CREATE TABLE ... AS SELECT` (including `WITH NO DATA` and `IF NOT EXISTS`), row-value `IN (VALUES ...)`, and `LATERAL` subqueries/table-function joins.
- DDL enhancements slice (S11): `ALTER TABLE ... RENAME TO ...` and `ALTER TABLE ADD/DROP CONSTRAINT` for named `CHECK`, named `FOREIGN KEY`, and named `UNIQUE` constraints with existing-row validation.
- Utility commands slice (S12): SQL-level `PRAGMA` support for `page_size`, `cache_size`, `integrity_check`, `database_list`, and `table_info(table)` with read/query behavior and constrained assignment semantics.
- DDL enhancements slice (S11) completion: `GENERATED ALWAYS AS (...) VIRTUAL` columns with parser support, read-time computation semantics, persistence metadata support, and `table_ddl` rendering of `VIRTUAL` vs `STORED`.
- S13 increment: covering index syntax `CREATE INDEX ... INCLUDE (...)` for BTREE key-column indexes, including parser normalization, catalog persistence, metadata APIs, and SQL dump rendering.
- S13 increment: `CREATE SCHEMA` with catalog namespace registration, `IF NOT EXISTS` semantics, and persistence across restart/checkpoint.

### Changed

- Refreshed repository documentation to present DecentDB as the current Rust engine and binding ecosystem.
- Clarified release/versioning docs around the current public `v2.x` release line.
