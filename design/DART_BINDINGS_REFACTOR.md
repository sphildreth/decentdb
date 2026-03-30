# Dart Bindings Refactor Plan

**Date:** 2026-03-29
**Status:** Active Plan
**Scope:** DecentDB Rust metadata layer, C ABI schema surface, Dart bindings, and downstream Decent Bench adoption sequencing

## Purpose

This document is the implementation plan for bringing the DecentDB Dart
bindings up to a v2-quality standard without losing downstream tooling features.

This is not a quick compatibility patch for `decent-bench`.

This plan is explicitly upstream-first:

1. fix the DecentDB engine metadata projection
2. fix the C ABI surface
3. fix the Dart binding model and execution semantics
4. only then adapt `decent-bench` to the richer upstream surface

The goal is to end with a feature-rich, engine-owned, performant Dart binding
that is a first-class citizen of the DecentDB v2 stack.

## Why This Exists

The current state is split across two distinct problems:

1. The rich schema metadata contract expected by `decent-bench` is not fully
   exposed by the DecentDB v2 Dart binding.
2. The current Dart `Statement` implementation still materializes entire result
   sets before paging, which is a real performance and behavior bug.

The result is exactly the failure mode seen when moving from DecentDB 1.8.1 to
2.0.2:

- `decent-bench` expects rich metadata fields that no longer exist in the
  current Dart type layer
- some rich metadata is still present inside the engine but is not surfaced
  through the stable binding contract
- paging/streaming behavior in Dart is weaker than the app contract requires

The right fix is not to parse DDL in Flutter or add a Decent Bench-specific
shim. The right fix is to restore the rich metadata surface and the streaming
execution semantics at the authoritative upstream layers.

## Design Inputs

This plan is constrained by the following documents:

- `AGENTS.md`
- `design/PRD.md`
- `design/SPEC.md`
- `design/TESTING_STRATEGY.md`
- `design/BINDING_REVIEW_FOR_V2.md`
- `design/adr/0115-dart-flutter-ffi-binding.md`
- `design/adr/0116-schema-introspection-surface.md`
- `design/adr/0129-rich-schema-snapshot-contract-for-bindings.md`
- `design/adr/0130-dart-streaming-statement-and-paging-semantics.md`

## Non-Negotiable Design Constraints

This document uses the 7 PRD pillars, sometimes informally referred to as the
project’s seven tenets, as hard constraints.

### 1. ACID Compliance is Forefront

This refactor must not change WAL semantics, transaction semantics, recovery
semantics, file format layout, or durability behavior. No schema-binding work is
allowed to take shortcuts that compromise correctness.

Implication for this plan:

- no on-disk format changes are part of this work
- no metadata caching layer is allowed to become a hidden source of stale truth
- schema snapshot data must be built from current engine/catalog state

### 2. Uncompromising Performance

The binding surface must stop paying unnecessary FFI and allocation costs for
common workflows.

Implication for this plan:

- use a one-shot schema snapshot for tooling loads
- stop materializing full result sets in `nextPage()` and `step()`
- use the existing row-view and batch C ABI fast paths where they materially
  help

### 3. Minimal Disk Footprint

This refactor must not duplicate catalog state on disk or add storage-only
metadata just to satisfy bindings.

Implication for this plan:

- richer binding metadata must be projected from existing engine metadata
- canonical DDL is generated, not stored redundantly

### 4. World-Class Documentation

The Dart binding refactor must ship with aligned ADRs, design docs, public API
docs, examples, and test-backed behavior notes.

Implication for this plan:

- create ADRs before implementation on the new contract decisions
- update binding docs and examples as part of the final slices

### 5. Best-in-Class Tooling & Bindings

The Dart binding is a first-class DecentDB product surface, not a convenience
afterthought.

Implication for this plan:

- the C ABI remains authoritative
- the Dart API may be idiomatic, but it cannot invent a parallel native
  contract
- schema/tooling needs must be satisfied upstream, not in downstream shims

### 6. Fantastic CLI Experience

Although this plan is Dart-focused, the engine-owned rich schema types should be
reusable by other product surfaces, including the CLI.

Implication for this plan:

- use Rust metadata types that can be shared by future CLI/schema inspection
  commands
- do not design a Dart-only schema concept

### 7. Fast Developer Feedback Loop

The work must be sliced so that PR validation stays fast and nightly/optional
checks absorb the heavier coverage and benchmark work.

Implication for this plan:

- keep each slice independently testable
- keep PR-fast Dart tests targeted
- keep heavier benchmark comparison out of the mandatory fast gate

## Fixed Decisions

The following decisions are already made by this plan and must not be
re-litigated during implementation:

- Do not patch `decent-bench` first.
- Do not parse DDL in Dart or Flutter to recover missing metadata.
- Do not create a Decent Bench-specific compatibility shim in the DecentDB Dart
  package.
- Do not add a second native contract just for Dart.
- Do not make `Statement.nextPage()` or `Statement.step()` materialize the full
  result set.
- Do not synthesize fake `CHECK` constraint names.
- Do not make downstream application code the source of truth for generated
  columns, trigger timing, temp-object state, or canonical DDL.
- Do not let the narrow list/describe helpers and the rich schema surface drift
  independently.

## Current Gap Summary

### Gap A: Rich schema metadata is incomplete at the binding boundary

Current v2 Dart types do not provide the complete metadata shape needed by
schema-heavy tooling:

- no one-shot schema snapshot
- no rich typed `CHECK` metadata with optional names
- no generated-column expression/storage metadata in the public Dart surface
- no full canonical DDL surface for every schema object
- no complete trigger semantics surface matching downstream needs
- temporary-object metadata is incomplete across the binding layers

### Gap B: Current Dart paging is not actually paging

`bindings/dart/dart/lib/src/statement.dart` currently fetches all rows into
memory before:

- `step()`
- `readRow()`
- `nextPage()`

That is not acceptable for a desktop tooling binding that needs to inspect large
tables safely.

### Gap C: The Dart binding still does not cover the most valuable remaining C ABI fast paths

The Dart binding review still shows missing or underused high-value functions:

- batch execution helpers
- re-execute helpers
- row-view fast paths
- shared WAL eviction

These are exactly the functions that help a binding be both feature-rich and
performant.

### Gap D: Downstream docs and compatibility baselines still point at the v1 world

`decent-bench` still assumes the v1 metadata and native-library layout. That
must be updated, but only after the upstream binding contract is corrected.

## Slice Map

Status legend:

- `Completed`: done in the current planning pass
- `Planned`: ready to implement in order
- `Blocked`: intentionally waits on an earlier slice

| Slice | Title | Status | Primary Repo | Depends On | Outcome |
|---|---|---|---|---|---|
| S0 | Planning, ADRs, and decision lock | Completed | `decentdb` | none | This document plus ADR-0129 and ADR-0130 |
| S1 | Rich Rust schema snapshot model | Planned | `decentdb` | S0 | Authoritative rich metadata types and builders in Rust |
| S2 | C ABI schema snapshot and compatibility projection | Planned | `decentdb` | S1 | Stable JSON contract for rich schema tooling |
| S3 | Dart rich schema API and model layer | Planned | `decentdb` | S2 | `Schema.getSchemaSnapshot()` and typed snapshot models |
| S4 | Dart streaming statement refactor | Planned | `decentdb` | S0 | True streaming `step()` and `nextPage()` |
| S5 | Dart ABI coverage parity and fast-path wrappers | Planned | `decentdb` | S4 | Batch, re-execute, eviction, and full declaration coverage |
| S6 | Dart tests, smoke coverage, and benchmarks | Planned | `decentdb` | S3, S4, S5 | Confidence that the refactor is correct and materially better |
| S7 | Decent Bench adoption and native library resolution update | Planned | `decent-bench` | S3, S4, S5 | Downstream app uses the rich upstream surface without feature loss |
| S8 | Docs, compatibility-line update, and release notes | Planned | both | S6, S7 | Docs and stated support match implemented reality |

## Implementation Rules By Slice

### S0. Planning, ADRs, and Decision Lock

**Status:** Completed

This slice is the current change set.

Deliverables:

- `design/DART_BINDINGS_REFACTOR.md`
- `design/adr/0129-rich-schema-snapshot-contract-for-bindings.md`
- `design/adr/0130-dart-streaming-statement-and-paging-semantics.md`

Exit criteria:

- the refactor is documented in slices
- implementation decisions are fixed enough that coding models do not need to
  improvise architecture

### S1. Rich Rust Schema Snapshot Model

**Status:** Planned

**Files to change**

- `crates/decentdb/src/metadata.rs`
- `crates/decentdb/src/db.rs`
- `crates/decentdb/src/catalog/schema.rs` only if projection helpers need
  already-present metadata surfaced more directly

**Required implementation**

1. Add the new rich metadata structs in `crates/decentdb/src/metadata.rs`:
   - `CheckConstraintInfo`
   - `SchemaColumnInfo`
   - `SchemaTableInfo`
   - `SchemaViewInfo`
   - `SchemaIndexInfo`
   - `SchemaTriggerInfo`
   - `SchemaSnapshot`
2. Derive `serde::Serialize` on the new snapshot structs. Do not add new
   dependencies; use the already-present `serde` / `serde_json`.
3. Keep the existing narrow metadata structs (`TableInfo`, `ColumnInfo`,
   `IndexInfo`, `ViewInfo`, `TriggerInfo`) in place for compatibility.
4. Add a single Rust builder path in `crates/decentdb/src/db.rs` that constructs
   `SchemaSnapshot` from engine/catalog state.
5. Reuse existing canonical DDL renderers in `crates/decentdb/src/db.rs` for:
   - tables
   - views
   - triggers
6. Add canonical index DDL generation in the same engine-owned layer if it is
   not already available.
7. Project generated-column metadata from the authoritative catalog model into
   `SchemaColumnInfo`:
   - `generated_sql`
   - `generated_stored`
8. Project `CHECK` constraints as typed objects with:
   - `name`
   - `expression_sql`
9. Preserve `row_count` semantics without introducing full table scans. Use the
   same cheap/authoritative source used by the existing table metadata path.
10. Order snapshot collections deterministically exactly as defined in ADR-0129.

**Do not do**

- do not change file format or catalog persistence layout
- do not add raw SQL text storage to the on-disk catalog
- do not synthesize constraint names
- do not duplicate the snapshot building logic in more than one Rust path

**Tests**

- add Rust unit coverage for snapshot objects containing:
  - named and unnamed `CHECK` constraints
  - foreign keys with actions
  - generated virtual/stored columns
  - temporary tables and views
  - partial indexes
  - triggers on tables and views
- add deterministic ordering assertions

**Exit criteria**

- one authoritative `SchemaSnapshot` builder exists
- all required rich fields exist in Rust types
- Rust tests prove the snapshot contains the full metadata contract

### S2. C ABI Schema Snapshot and Compatibility Projection

**Status:** Planned

**Files to change**

- `include/decentdb.h`
- `crates/decentdb/src/c_api.rs`

**Required implementation**

1. Add the new C ABI function declaration:
   - `ddb_db_get_schema_snapshot_json(ddb_db_t *db, char **out_json)`
2. Implement that function in `crates/decentdb/src/c_api.rs` using the Rust
   `SchemaSnapshot` builder from S1.
3. Serialize the snapshot through `serde_json::to_string`.
4. Keep ownership rules aligned with the existing string-returning APIs:
   - allocation on the Rust/C side
   - caller releases with `ddb_string_free()`
5. Refactor the existing JSON schema helpers so they project from the same Rust
   metadata builders rather than hand-assembling divergent JSON structures.
6. Preserve existing `ddb_db_get_view_ddl()` semantics:
   - it continues to return the canonical SELECT body for compatibility
   - full `CREATE VIEW ... AS ...` text lives in the rich snapshot `ddl` field

**Do not do**

- do not remove or rename the existing narrow C ABI functions
- do not create separate ad hoc JSON builders for narrow and rich schema paths

**Tests**

- add/expand C ABI tests in `crates/decentdb/src/c_api.rs` that parse the JSON
  returned by:
  - `ddb_db_get_schema_snapshot_json`
  - the existing narrow helpers
- assert that rich snapshot JSON contains the required fields
- assert compatibility semantics of `ddb_db_get_view_ddl()`

**Exit criteria**

- `include/decentdb.h` documents the new schema snapshot function
- rich schema JSON can be loaded in one C ABI call
- narrow and rich JSON surfaces are proven not to drift

### S3. Dart Rich Schema API and Model Layer

**Status:** Planned

**Files to change**

- `bindings/dart/dart/lib/src/native_bindings.dart`
- `bindings/dart/dart/lib/src/schema.dart`
- `bindings/dart/dart/lib/src/schema_snapshot.dart` (new file)
- `bindings/dart/dart/lib/decentdb.dart`

**Required implementation**

1. Add the FFI declaration for `ddb_db_get_schema_snapshot_json` in
   `bindings/dart/dart/lib/src/native_bindings.dart`.
2. Add a new public typed model layer in
   `bindings/dart/dart/lib/src/schema_snapshot.dart`:
   - `SchemaSnapshot`
   - `SchemaTableInfo`
   - `SchemaColumnInfo`
   - `SchemaViewInfo`
   - `SchemaIndexInfo`
   - `SchemaTriggerInfo`
   - `SchemaCheckConstraintInfo`
3. Reuse the existing `ForeignKeyInfo` type from `types.dart` for FK entries.
4. Add `Schema.getSchemaSnapshot()` in `bindings/dart/dart/lib/src/schema.dart`.
5. Keep the existing narrow schema APIs unchanged:
   - `listTablesInfo`
   - `describeTable`
   - `getTableDdl`
   - `listIndexes`
   - `listViewsInfo`
   - `getViewDdl`
   - `listTriggers`
6. Export the new snapshot models from `bindings/dart/dart/lib/decentdb.dart`.
7. Parse the JSON contract exactly as defined by ADR-0129. Keep JSON field names
   in snake_case on the wire and idiomatic Dart names in the object model.

**Do not do**

- do not retrofit the existing narrow `TableInfo` / `IndexInfo` / `TriggerInfo`
  classes into the rich contract
- do not force downstream tooling to recover rich semantics from the narrow
  classes
- do not parse DDL in Dart

**Tests**

- add Dart package tests for `getSchemaSnapshot()` covering:
  - tables
  - views
  - indexes
  - triggers
  - temp objects
  - generated columns
  - named and unnamed checks
- verify that narrow schema APIs still work after the refactor

**Exit criteria**

- Dart has a first-class rich schema API
- narrow APIs still compile and behave as before
- downstream tooling can stop depending on lossy field projections

### S4. Dart Streaming Statement Refactor

**Status:** Planned

**Files to change**

- `bindings/dart/dart/lib/src/native_bindings.dart`
- `bindings/dart/dart/lib/src/statement.dart`

**Required implementation**

1. Declare the row-view functions in `native_bindings.dart`:
   - `ddb_stmt_row_view`
   - `ddb_stmt_step_row_view`
   - `ddb_stmt_fetch_row_views`
   - `ddb_stmt_fetch_rows_i64_text_f64`
2. Replace the current `_fetchAll()`-driven cursor behavior in
   `bindings/dart/dart/lib/src/statement.dart`.
3. `Statement.query()` remains the full-materialization API, but it must
   internally materialize by reading chunked pages from the streaming path.
4. Use an internal `query()` chunk size of `256`.
5. `Statement.step()` must:
   - advance one row
   - decode that row immediately from the borrowed row-view buffer
   - store the decoded `Row` as the current row
6. `Statement.nextPage(pageSize)` must:
   - fetch at most `pageSize` rows
   - decode them immediately
   - never read past the requested page
7. `Statement.readRow()` remains valid only after a successful `step()`.
8. `nextPage()` invalidates the current row.
9. `bind*`, `bindAll`, `reset`, and `clearBindings` must invalidate streaming
   state consistently.
10. `execute()` continues to exhaust DML statements without storing result rows.

**Do not do**

- do not keep `_rows` as the backing store for streaming behavior
- do not expose borrowed raw row-view pointers publicly
- do not duplicate rows when users mix `step()` and `nextPage()`

**Tests**

- add Dart tests for:
  - repeated `nextPage()` pagination across large result sets
  - mixed `step()` then `nextPage()` semantics
  - mixed `nextPage()` then `step()` semantics
  - `readRow()` validity rules
  - reset/rebind/clear invalidation
  - empty result sets
  - large result sets without full materialization semantics

**Exit criteria**

- `nextPage()` is genuinely paged
- `step()` is genuinely streaming
- `query()` still works, but the full fetch path is explicit rather than hidden

### S5. Dart ABI Coverage Parity and Fast-Path Wrappers

**Status:** Planned

**Files to change**

- `bindings/dart/dart/lib/src/native_bindings.dart`
- `bindings/dart/dart/lib/src/statement.dart`
- `bindings/dart/dart/lib/src/database.dart`

**Required implementation**

1. Add the remaining missing C ABI declarations in `native_bindings.dart`:
   - `ddb_stmt_execute_batch_i64`
   - `ddb_stmt_execute_batch_i64_text_f64`
   - `ddb_stmt_execute_batch_typed`
   - `ddb_stmt_rebind_int64_execute`
   - `ddb_stmt_rebind_text_int64_execute`
   - `ddb_stmt_rebind_int64_text_execute`
   - `ddb_evict_shared_wal`
2. Add public Dart wrappers in `Statement`:
   - `executeBatchInt64(List<int> values)`
   - `executeBatchI64TextF64(List<(int, String, double)> rows)`
   - `executeBatchTyped(String signature, List<List<Object?>> rows)`
   - `rebindInt64Execute(int value)`
   - `rebindTextInt64Execute(String text, int value)`
   - `rebindInt64TextExecute(int value, String text)`
3. Add a public static helper in `Database`:
   - `Database.evictSharedWal(String path, {String? libraryPath, NativeBindings? bindings})`
4. Keep raw row-view functions internal only.
5. Do not design a second public result-set API in this slice. That is separate
   work and not required for Decent Bench parity.

**Do not do**

- do not add wrappers that expose borrowed row-view pointer lifetimes publicly
- do not defer high-value ABI declarations because the binding “works without
  them”

**Tests**

- add Dart tests for:
  - batch insert helpers
  - re-execute helpers
  - shared WAL eviction happy-path API behavior where testable

**Exit criteria**

- Dart declares the full high-value v2 C ABI surface
- the public Dart API exposes the performance-critical helpers that are worth
  exposing safely

### S6. Dart Tests, Smoke Coverage, and Benchmarks

**Status:** Planned

**Files to change**

- `bindings/dart/dart/test/decentdb_test.dart`
- `bindings/dart/dart/test/schema_snapshot_test.dart` (new file)
- `bindings/dart/dart/test/statement_streaming_test.dart` (new file)
- `bindings/dart/dart/test/fast_paths_test.dart` (new file)
- `bindings/dart/dart/test/memory_leak_test.dart`
- `tests/bindings/dart/smoke.dart`
- `bindings/dart/dart/benchmarks/bench_fetch.dart`

**Required implementation**

1. Keep `decentdb_test.dart` for baseline API coverage.
2. Move rich schema coverage into `schema_snapshot_test.dart`.
3. Move streaming and cursor-behavior coverage into
   `statement_streaming_test.dart`.
4. Move batch/re-execute coverage into `fast_paths_test.dart`.
5. Update `tests/bindings/dart/smoke.dart` so the smoke test covers:
   - `getSchemaSnapshot()`
   - `nextPage()`
   - at least one fast-path helper
6. Update `bench_fetch.dart` to measure the new streaming behavior rather than
   benchmarking the old hidden full-fetch implementation.

**Do not do**

- do not push heavyweight long-running performance validation into the default
  fast PR gate
- do not leave the benchmark measuring the old semantics after S4 lands

**Validation commands**

- `cargo check`
- `cargo clippy --all-targets --all-features`
- `cargo test`
- `cd bindings/dart/dart && dart test`
- `cd tests/bindings/dart && dart run smoke.dart`

**Exit criteria**

- Dart package tests cover rich schema, streaming semantics, and fast paths
- smoke coverage catches binding regressions at the repository level
- the benchmark reflects the new design rather than the old bug

### S7. Decent Bench Adoption and Native Library Resolution Update

**Status:** Planned

**Primary repo:** `/home/steven/source/decent-bench`

All paths in this slice are relative to `/home/steven/source/decent-bench`.

**Files to change**

- `apps/decent-bench/lib/features/workspace/infrastructure/decentdb_bridge.dart`
- `apps/decent-bench/lib/features/workspace/infrastructure/native_library_resolver.dart`
- `apps/decent-bench/test/features/workspace/infrastructure/decentdb_bridge_smoke_test.dart`
- `design/PRD.md`
- `design/SPEC.md`
- `design/adr/0003-pinned-decentdb-sql-capability-baseline.md`
- `README.md`

**Required implementation**

1. Switch `decentdb_bridge.dart` to the rich upstream snapshot API rather than
   the narrow v2 types.
2. Preserve the existing Decent Bench schema domain model unless a downstream
   simplification is clearly beneficial and does not lose information.
3. Map upstream rich fields directly; do not recover missing fields from DDL.
4. Keep planner/statistics workflows intact:
   - `ANALYZE`
   - `EXPLAIN`
   - `EXPLAIN ANALYZE`
5. Update native library resolution to accept Rust v2 library names:
   - `libdecentdb.so`
   - `libdecentdb.dylib`
   - `decentdb.dll`
6. Keep `DECENTDB_NATIVE_LIB` as the highest-priority explicit override.

**Do not do**

- do not add a local `decent-bench` metadata parser to compensate for upstream
  gaps
- do not downgrade or remove schema-browser metadata expectations

**Validation commands**

- `cd /home/steven/source/decent-bench/apps/decent-bench && flutter analyze`
- `cd /home/steven/source/decent-bench/apps/decent-bench && flutter test`

**Acceptance cases**

- schema browser still renders tables, views, indexes, triggers, temp objects,
  checks, foreign keys, generated columns, and canonical DDL
- planner/statistics smoke cases still pass
- Linux launch works with the Rust-native shared library path

### S8. Docs, Compatibility-Line Update, and Release Notes

**Status:** Planned

For the Decent Bench files in this slice, paths are relative to
`/home/steven/source/decent-bench`.

**Files to change**

- `design/BINDING_REVIEW_FOR_V2.md`
- `bindings/dart/README.md`
- `bindings/dart/examples/console/main.dart`
- `bindings/dart/examples/console_complex/main.dart`
- `bindings/dart/examples/flutter_desktop/main.dart`
- `design/PRD.md`
- `design/SPEC.md`
- `design/adr/0003-pinned-decentdb-sql-capability-baseline.md`
- `README.md`

**Required implementation**

1. Update the v2 binding review so the Dart section reflects implemented
   reality after the refactor.
2. Update binding docs to show:
   - `Schema.getSchemaSnapshot()`
   - real paging with `nextPage()`
   - batch helpers where appropriate
3. Update examples so they demonstrate the intended v2 surface, not the old
   narrow assumptions.
4. Update Decent Bench’s documented compatibility line from the old v1 baseline
   to the new upstream-supported v2 baseline.

**Exit criteria**

- docs match code
- the repo no longer claims a thinner or older Dart surface than is actually
  implemented

## Cross-Slice Acceptance Requirements

The entire refactor is only done when all of the following are true:

- Rust rich schema metadata is engine-owned and canonical
- the C ABI exposes a stable one-shot schema snapshot JSON contract
- Dart exposes a rich typed snapshot API
- Dart paging is actually streaming
- Dart covers the remaining high-value v2 fast paths
- Decent Bench works without feature loss in schema metadata, DDL display, or
  planner/statistics workflows
- documentation and compatibility statements match implemented behavior

## Explicit Out-of-Scope Items

These are intentionally excluded from this plan unless a later ADR/doc adds
them:

- on-disk catalog format changes
- WAL or durability changes
- a new public raw row-view Dart API
- a new public high-level Dart `ResultSet` redesign
- Dart mobile packaging work
- a Decent Bench-specific upstream fork surface

## Recommended Execution Order

Implementation should proceed in this exact order:

1. S1 rich Rust schema snapshot model
2. S2 C ABI schema snapshot
3. S3 Dart rich schema API
4. S4 Dart streaming statement refactor
5. S5 Dart ABI coverage parity and fast-path wrappers
6. S6 tests, smoke coverage, and benchmark alignment
7. S7 Decent Bench adoption
8. S8 docs and compatibility-line updates

Do not start S7 before S3, S4, and S5 are complete enough to provide a stable
upstream target.

## Handoff Notes For Coding Models

If a coding model picks up one of these slices later, the expected behavior is:

- implement the slice exactly as written
- do not re-open the architecture unless code reality proves a documented
  assumption false
- if a documented assumption is false, update this plan and the affected ADR
  before continuing
- prefer shared builders and projections over duplicating logic in parallel code
  paths
- keep the C ABI authoritative and the Dart surface idiomatic

The intended end state is not “Decent Bench works again.” The intended end state
is “DecentDB ships a strong Dart binding that Decent Bench can trust.”
