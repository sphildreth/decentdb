# Changelog

All notable changes to DecentDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.6.0] - [UNRELEASED] [WIP] [CURRENT WORK BRANCH VERSION]

### Added

- Added `decentdb serve`, a CLI-hosted local HTTP API and lightweight Web
  Console with embedded offline assets, transparent localhost auth,
  read-only mode, schema/table/index/view/trigger inspection, SQL and EXPLAIN
  execution, result limits, request limits, query history, CSV export, optional
  CORS, JSON request logging, and remote-bind safety checks.
- Enhanced `decentdb describe` and the REPL `.d <table>` command to show
  foreign key references, including table-level foreign key constraints.
- Completed the interactive SQL shell ergonomics roadmap slice with a version
  banner, `help`/quit aliases, topic-specific help, schema inspection commands
  (`.tables`, `.dt`, `.d <table>`, `.schema`, `.indexes`, `.views`), function
  listing (`.df`), output controls (`.mode`, `.headers`, `.nullvalue`,
  `.width`, `.timer`), file workflows (`.read`, `.output`, `.once`, `.import`,
  `.export`), explain helpers (`.explain`, `.plan`, `.explain-analyze`),
  positional parameter helpers (`.param`), repeat-last-SQL (`.g`), session
  history (`.s`), and branch creation/checkout helpers.
- Added WASM/browser support: the `decentdb` crate now checks for
  `wasm32-unknown-unknown`, exposes wasm-bindgen browser exports, and includes
  an OPFS-backed VFS intended for Dedicated Worker use.
- Added the `@decentdb/web` TypeScript binding with an async worker-owned API,
  OPFS host bridge, explicit `wasmUrl` loading, `exec`/`query`/`prepare`,
  binary result transport, checkpoint, import/export, persistence helper,
  worker metrics, automated Chromium OPFS browser smoke coverage, and scheduled
  browser transport benchmark coverage.
- Added ADR 0161 documenting the browser WASM/OPFS runtime and its one-worker
  ownership model.
- Added the WASM / Browser API documentation page.
- Completed the production browser runtime phase with ADR 0165, `probeRuntime()`,
  stable browser error codes, Dedicated Worker owner coordination through Web
  Locks and BroadcastChannel, multi-tab owner routing and recovery coverage,
  explicit service-worker exclusion, owner/quota/persistence diagnostics through
  `metrics()` and `sys.browser_*` views, tagged browser parameters, an expanded
  `browser-app-v1` SQL profile, and tier/candidate Playwright browser matrix
  scripts.
- Added concurrent-write benchmark coverage: native benchmark hooks for direct
  single-writer and read-under-write regression slices, executable queued-write
  single-writer and reader-under-writer scenarios, a shared cross-binding
  concurrent-write scenario definition, a grouped-commit fault-injection test
  plan, and pre-commit check keys for the new slices.
- Added the engine-owned queued write path with strict durable group commit,
  bounded admission, timeout and cancellation-before-run errors, Rust and C ABI
  queue metrics, C ABI status codes and execution entry points, and ADR 0162.
- Added the Write Concurrency user guide and C ABI documentation for queued
  writes, queue configuration, queue metrics, and the distinction between
  strict group commit and async commit.
- Added queued-write binding coverage across Python, Go, Dart, Node, .NET
  native, Java/JDBC status/config mapping, Knex unbound writes, and C ABI smoke
  tests for maintained binding surfaces.
- Added built-in operational metrics `sys.*` inspection views:
  `sys.wal_metrics`, `sys.write_queue_metrics`, `sys.storage_metrics`, and
  canonical `sys.sync_status` while preserving `sys_sync_*` compatibility
  inspection names. Added deterministic Rust regression tests and SQL API
  documentation plus ADR 0163 covering column contracts, metric snapshots, and
  lifecycle semantics.
- Added in-process reactive subscriptions and change streams with Rust watch
  handles, table/range/query/change-stream watch kinds, bounded lag reporting,
  post-commit LSN events, queued/sync/branch source tagging, C ABI JSON watch
  handles, Python and Go direct watch helpers, `sys.reactive_metrics`,
  `sys.reactive_subscriptions`, ADR 0164, and a full implementation spec for
  the reactive contract.

### Changed

- Gated the native C-backed `pg_query` parser out of `wasm32-unknown-unknown`
  builds and added a documented wasm parser for the browser v1 SQL subset.

### Fixed

- Fixed `decentdb-migrate` v10 → v11 upgrades for databases with an existing
  `<db>.wal` sidecar by standardizing migration tooling on the engine's
  append-`.wal` sidecar convention, copying the v10 WAL forward, and upgrading
  any page-1 database-header frames inside the copied WAL so a later checkpoint
  cannot restore a legacy v10 header.

## [2.5.1] - 2026-05-19

### Changed

- Updated the .NET benchmark harness to use the same tuned durable read profile as the native benchmark (`64MB` cache, resident row sources after commit, paged row storage disabled, and WAL auto-checkpoint disabled), primary-key point-read schema parity for SQLite and DecentDB, parameterized batched Dapper inserts, larger EF Core `SaveChanges` batches, and only like-for-like paired provider comparisons.

### Fixed

- Fixed .NET `Cache Size` and related native connection options so they are passed through the C ABI at open time instead of being parsed and ignored by the managed binding.
- Fixed C ABI owned-value disposal for native `GEOMETRY` and `GEOGRAPHY` result values so sanitizer runs no longer report leaked copied spatial cells.
- Fixed the Node Knex lifecycle tests to always close pools after success or failure and clean up DecentDB WAL sidecar files between tests, avoiding hung nightly binding lifecycle runs after transaction failures.
  

## [2.5.0] - 2026-05-18

### Changed

- Extracted CTE and query-scope utility functions from `exec/mod.rs` into `exec/cte.rs` (~470 lines), improving module cohesion.

### Fixed

- Fixed single-row `ENUM` inserts and updates after reopen so column label
  metadata is used during write coercion instead of falling through to the
  metadata-free generic cast path.
- Removed three stale `#[ignore]` test annotations: the zero-byte WAL design-choice test, the `INSERT DEFAULT VALUES` missing-feature test, and the decimal negative-precision ordering test (the underlying sortable encoding was already correct, validated by the now-active test).

### Added

- **Stable schema and query-contract metadata for tooling:** added
  `Db::get_tooling_metadata()`, `Db::describe_query_contract(sql)`, C ABI JSON
  helpers, deterministic schema fingerprints, native type metadata, query
  parameter/result-column contracts, and binding exposure across Python, Go,
  .NET, Node.js, Java/JDBC, and Dart.
- C ABI open-with-options entry points (`ddb_db_create_with_options`,
  `ddb_db_open_with_options`, and `ddb_db_open_or_create_with_options`) for
  open-time tuning from bindings, including cache size, paged row-source
  residency, paged row storage, persistent primary-key index, and WAL
  auto-checkpoint thresholds.
- **Binding-native semantic data types:** added compact native storage and C ABI
  value tags for `ENUM`, `IPADDR`/`INET`, `CIDR`, `DATE`, `TIME`,
  `TIMESTAMPTZ`, `INTERVAL`, and `MACADDR`/`MACADDR8`, including SQL casts,
  format-version migration support, dump/sync/tooling metadata coverage, and
  typed result decoding across Python, Go, .NET, Node.js, Java/JDBC, and Dart.
  `ENUM` stores stable label ids plus persisted catalog label metadata so row
  values are not tied to mutable label strings.
- **Native geospatial types and spatial indexes:** added `GEOMETRY` and
  `GEOGRAPHY` column types with EWKB-backed storage, WKB/WKT/GeoJSON
  conversion functions, core `ST_*` accessors/predicates/measurements,
  indexed spatial filters, distance ordering via `<->`, point-in-polygon
  spatial joins, C ABI bind/read support, and binding updates across Python,
  Go, .NET, Node.js, Java JNI, and Dart.
- **Branch, diff, restore, and time-travel workflows:** added named snapshots,
  read-only historical execution, branch metadata, branch-local writes,
  branch commit/log markers, primary-key row diffs, guarded restore,
  constrained merge, CLI and REPL support, Rust APIs, and a C ABI JSON bridge.
- 12 proptest-based property tests for WAL delta encoding (roundtrip, no-op, size bounds, determinism, corruption rejection) and WAL frame format (page/commit/checkpoint roundtrips, header identity, invalid frame types, encoded-len consistency).
- 4 crash-recovery edge-case tests: checkpoint survival, uncommitted transaction discard, WAL growth-and-truncation cycle, and SQL-level transaction isolation.
- `wal_fuzz` standalone binary exercising 6 WAL-corruption strategies across 75+ DB states to verify recovery never panics.

## [2.4.2] - 2026-05-04

### Fixed

- Fixed shutdown-time ownership that caused the nightly memory-safety Valgrind workflow to report possible leaks after C ABI and Python open/query/close probes. In-memory database drop now runs its final checkpoint synchronously so `ddb_db_free` does not return while a drop checkpoint thread still owns WAL and pager state.
- Pruned stale weak entries from the database open-lock and OS VFS path-lock registries as handles close, avoiding process-lifetime registry allocations being reported as possible leaks by Memcheck.
- Hardened background checkpoint worker shutdown so worker-context teardown cannot attempt to join its own thread.

## [2.4.1] - 2026-04-30

### Fixed

- Fixed deferred paged-row UPDATE validation so all foreign-key parent tables needed by row validation are materialized, including unchanged FK columns. This resolves the DecentDB.EntityFrameworkCore regression seen by downstream apps merge workflows after upgrading to 2.4.0.
- Improved .NET EF Core modification-batch diagnostics by surfacing the native DecentDB statement error instead of replacing it with a generic `Step failed` message.
- Fixed aggregate SELECTs over joins, including Entity Framework Core `COUNT(*)` join queries, so aggregate projections fall through to grouped evaluation instead of the non-aggregate join fast paths.
- Fixed base-table join filtering so `WHERE` predicates are evaluated against the full join scope before projection. This resolves filtered joins whose predicate references columns not present in the final projection.
- Fixed deferred DELETE dependency analysis for transitive self-referential `ON DELETE CASCADE` relationships so dependency traversal terminates and loads each affected table once.
- Fixed shared file-backed WAL checkpoint races that could corrupt paged table manifests or overflow payload reads during concurrent writer plus rapid open/close reader workloads. Implicit automatic, on-open, and drop-time checkpoints are now skipped for shared WAL handles until cross-handle pager-cache invalidation is coordinated; explicit checkpoints remain available.
- Fixed large WAL checkpointing so exact VFS reads and writes retry legal partial I/O results instead of treating them as fatal short reads/writes. CLI maintenance paths (`checkpoint`, `exec --checkpoint`, `import`, `bulk-load`, `save-as`, `vacuum`, and `doctor --fix`) now perform pure WAL flushes without running the optional pre-checkpoint payload compaction pass, avoiding multi-GB compaction appends during maintenance flushes.
- Fixed deferred-table index freshness reporting so `rebuild-indexes` persists fresh catalog metadata and subsequent `list-indexes` / `doctor` runs do not report rebuilt indexes as stale simply because table row payloads are still lazily materialized.
- Fixed PostgreSQL dump overwrite imports in the Python tools by evicting any shared WAL registry entry and removing the `.wal` sidecar before recreating the destination database.

## [2.4.0] - 2026-04-30

### Added

- Expanded .NET regression coverage with new ADO.NET and EF Core suites targeting common provider failure modes: parameter-collection shape/errors, maintenance path resolution and cleanup behavior, design-time model-factory metadata extraction, window/sql-expression translator contracts, modification-batch bind/execute fallback paths, and broad SaveChanges type-matrix binding coverage.
- **Background incremental checkpoint worker (ADR 0058):** auto-checkpoint work moved off the writer commit hot path onto a dedicated `decentdb-checkpoint` thread. Writers signal the worker via a `Condvar`; the worker also wakes on a periodic timeout to drain WAL even for workloads that just barely cross a threshold and then go idle. Opt-in via `DbConfig::background_checkpoint_worker` (default `true` when any auto-checkpoint threshold is configured).
- **Per-engine allocator scaffold (ADR 0142):** new `crates/decentdb/src/alloc.rs` introduces a per-engine allocator forwarder defaulting to the global allocator. Provides the abstraction needed for future per-database arena/jemalloc integration without changing call sites.
- **Paged on-disk WAL index sidecar scaffold (ADR 0141):** new `wal/index_sidecar.rs` and supporting `WalVersionPayload` enum (`Resident`/`OnDisk`) introduce the type machinery required to spill WAL-resident page versions to a sidecar file. Currently always-resident; spill path will be wired in a follow-up.
- **Storage-state memory instrumentation (ADR 0143 Phase A):** `Db::inspect_storage_state_json` now reports four new fields — `tables_in_memory_bytes`, `rows_in_memory_count`, `loaded_table_count`, and `deferred_table_count` — sourced from new helpers `EngineRuntime::table_memory_totals`, `TableData::approximate_heap_bytes`, and `Value::approximate_heap_bytes`. Enables external memory probes without parsing platform-specific RSS.
- **Per-commit delta payload recycler (M6) and `SmallVec` WAL index (M7):** writer-side commit path reuses encoded-delta scratch buffers across commits, and `WalIndex` now stores per-page version chains in a `SmallVec` to avoid heap allocations for the common single-version case.
- **Linux heap-release hook (ADR 0138):** opt-in `DbConfig::release_freed_after_checkpoint` calls `malloc_trim` on glibc Linux after a successful checkpoint to return freed arena pages to the OS. No-op on non-glibc/non-Linux targets.
- New helper `Value::approximate_heap_bytes()` returns a best-effort byte estimate of heap-allocated payload (`Text`, `Blob`, `Json`, etc.) for memory-accounting callers.
- **Per-table on-demand loading scaffold (ADR 0143 Phase B):** New `materialize_deferred_tables_with_store` now accepts an optional `filter: Option<&BTreeSet<String>>` parameter to materialize only specified tables instead of all deferred tables. Added `load_deferred_tables_filtered` for targeted loading. Statement-target analysis via `statement_referenced_tables()` extracts table names from `SELECT`/`INSERT`/`UPDATE`/`DELETE` statements. Targeted index rebuild via `mark_indexes_stale_for_table` + `rebuild_stale_indexes` after per-table load.
- **Deferred table materialization is now the default (ADR 0143 Phase B follow-up):** the per-table loader remains wired through the strict whitelist analyzer `safe_referenced_tables` (`crates/decentdb/src/sql/ast.rs`), but the old concurrent-checkpoint race is now closed by pinning one WAL snapshot across the runtime refresh and first-use overflow payload reads. Plain `SELECT`/`UPDATE`/`DELETE`/`INSERT … VALUES` statements whose table set is provably exhaustive load only the referenced tables; CTEs, set operations, `INSERT … SELECT`, FROM-subqueries/functions, view/CTE references, and any expression-level subquery (`InSubquery`/`CompareSubquery`/`ScalarSubquery`/`Exists`) still conservatively fall back to `ensure_all_tables_loaded`. Set `DbConfig::defer_table_materialization = false` to restore eager-at-open loading.
- **Persistent primary-key locator index (ADR 0144, soak-flagged):** new `DbConfig::persistent_pk_index` persists a row-id locator B+Tree (`pk_index_root`) per table so deferred `WHERE id = ?` reads can decode one row directly from the overflow payload instead of materializing `Vec<StoredRow>` for the whole table. The format version is now 9, checkpoint compaction skips payloads with live locator roots, and `decentdb-migrate` backfills missing roots during format-8 → format-9 upgrades.
- **Phase D row-source seam + streamed paged deferred scan slice (ADR 0145):** `EngineRuntime.tables` now stores a `TableRowSource` abstraction instead of exposing `Arc<TableData>` directly, the executor has a `TableRowIter` / visible-row-source layer for several simple scan, join, and runtime-index read paths, and non-transaction deferred reads can now answer plain `COUNT(*)`, grouped `COUNT(*)`, paged deferred `MIN`/`MAX`, plain `SELECT *` / `alias.*` sequential scans, the simplest filtered-projection queries, single-table expression projections with `ORDER BY` / `LIMIT`, and the narrow grouped numeric aggregate fast path from persisted table bytes without promoting the table to resident `Vec<StoredRow>`. The latest follow-up streams grouped-count / grouped-sum / projection fast paths chunk-by-chunk from persisted state instead of hydrating all paged chunk payloads up front; legacy deferred `MIN`/`MAX` now deliberately falls back to the safer snapshot-pinned load path until the broader iterator path is hardened. Broader scan/aggregate migration is still follow-up work.
- **Phase D paged-persistence follow-up (ADR 0145):** format 10 now introduces a table-level paged manifest behind `DbConfig::paged_row_storage` (default `true`). Existing legacy tables can be backfilled into that manifest on open, dirty tables written under the flag now persist behind bounded **multi-chunk** paged manifests, paged tables retain deferred primary-key point lookups through chunk-aware locator roots, pure insert growth on already-paged tables appends new chunks without rewriting existing chunk payloads, update/delete persistence now rewrites only changed chunks while preserving untouched chunk pointers, checkpoint compaction now rewrites large paged chunk payloads without breaking chunk-aware locator roots, and `decentdb-migrate` can copy v9 databases forward to the new format. True tombstones and fully streaming paged mutation semantics are still follow-up work.
- **Auto-checkpoint on open (ADR 0143):** new `DbConfig::auto_checkpoint_on_open_mb` (default `16` MB). When the on-disk WAL exceeds the threshold at `Db::open`, the engine performs a synchronous checkpoint immediately after `WalHandle::acquire` and before `EngineRuntime::load_from_storage`. This drops the resident WAL page-version index — which can be hundreds of MB on a large uncheckpointed WAL — before the runtime is loaded, dramatically reducing post-open RSS. Validated via `.tmp/decentdb_memory_reopen_probe.py`: at scale 5 (~321 MB DB + ~384 MB WAL), open Δ dropped from ~775 MB to ~6 MB and peak RSS from ~1089 MB to ~662 MB.
- **Doctor / Advisor / Introspection v1:** new `decentdb doctor` command and Rust report model produce deterministic Markdown or JSON health reports covering header/open compatibility, storage/WAL state, fragmentation, schema/index metadata, optional logical index verification, CI-friendly `--fail-on` thresholds, path redaction modes, recommendation suppression, and constrained `--fix` actions for safe checkpoint and index-rebuild maintenance.

### Fixed

- .NET SQL parameter rewriting for EF-style multi-row parameter names (for example `@p0_0`, `@p1_1`, `@p10_11`) so provider-generated names bind correctly without false positional rewrites.
- .NET prepared-command reuse now consistently resets and clears bindings after successful non-query execution, reducing native statement-retention pressure in repeated prepared insert/update workloads.
- .NET native text binding path now uses pooled/stack-based UTF-8 encoding buffers instead of per-call byte-array allocations, reducing allocation churn on high-frequency bind paths.
- **WAL writer / background-checkpoint race (regression introduced by ADR 0058):** `commit_pages_if_latest` now distinguishes a benign WAL-end-LSN advance caused by the background checkpoint worker (which bumps `WalShared::checkpoint_epoch` and may truncate `wal_end_lsn` to `0`) from a real concurrent writer commit. Previously, a long-running write transaction whose commit happened to land just after a background checkpoint truncated the WAL would spuriously fail with `transaction conflict: WAL advanced from N to 0`. The OCC guard for true multi-connection writer conflicts (ADR 0023) is preserved by checking the checkpoint epoch alongside the WAL end LSN.
- **Active-reader checkpoint safety for deferred scans:** checkpoint now retains snapshot-visible WAL versions while readers are active instead of pruning them mid-flight, and legacy deferred `SELECT MIN/MAX(...)` falls back to the safer snapshot-pinned load path. This removes the remaining parallel pre-commit `overflow payload length mismatch` failure seen in the Python writer/reader interleave stress test.

### Documentation

- New ADRs: 0058 (background incremental checkpoint worker), 0138 (Linux heap-release on checkpoint), 0141 (paged on-disk WAL index), 0142 (per-engine allocator), 0143 (on-disk row-scan executor, phased plan).

## [2.3.0] - 2026-04-20

### Added

- WAL group-commit mode (`WalSyncMode::AsyncCommit { interval_ms }`): commits return as soon as the WAL frame is written, while a per-WAL background thread fsyncs on the configured interval. Default `WalSyncMode::Full` is unchanged. New `Db::sync()` provides an explicit durability barrier (no-op for non-AsyncCommit modes). Trades a bounded recovery window (last `interval_ms` of acked commits may be lost on crash) for higher write throughput. See ADR 0135.

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
- Eliminated cross-table COW contamination on UPDATE/DELETE/INSERT by wrapping each table's `TableData` in its own `Arc`, so the first write in a transaction now clones only the targeted table's row vector instead of every table's rows. Yields ~3× faster DELETE p50 (132 µs → 42 µs) and ~1.3× faster UPDATE p50 (116 µs → 87 µs) on the Python `bench_complex` workload, with a 44× speedup on a worst-case micro-probe (mutating a small table while a large unrelated table sits in the catalog).
- Extended the per-entry `Arc` copy-on-write pattern to the runtime index map so the first write to an indexed column in a transaction clones only the targeted index instead of every `RuntimeIndex` (BTREE keys + trigram postings) in the database. Closes a latent regression that would have appeared on workloads updating indexed columns on databases with many or large secondary indexes.
- ADR 0136 (proposed): chunked row storage for finer-grained copy-on-write, targeting the remaining UPDATE-latency gap to SQLite by reducing per-transaction first-write cloning from O(rows in mutated table) to O(rows in mutated chunk).
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
