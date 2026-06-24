# ADR 0196: Persisted DML and Cascade Delete Performance Path

**Date:** 2026-06-24
**Status:** Proposed

## Context

The June 2026 SQLite comparison work found five current benchmark gaps that
cannot be closed by local executor micro-optimizations alone. The current
target list is from `.tmp/perf-validate/20260624-171109`:

| Benchmark target | SQLite | DecentDB | Gap |
|---|---:|---:|---:|
| Showdown GLM52 native defaults `Bulk DELETE` | 0.002926s | 0.487579s | 166.629x |
| Showdown GLM52 scale `UPDATE RETURNING` | 0.002675s | 0.211947s | 79.247x |
| Showdown GLM52 scale `Bulk DELETE` | 0.002769s | 0.152178s | 54.964x |
| MovieDB scratch `Cascade delete batch` | 0.118674s | 2.364792s | 19.927x |
| Showdown GLM52 native defaults `INSERT RETURNING` | 0.029628s | 0.393055s | 13.266x |

Evidence:

- `.tmp/perf-validate/20260624-171109`
- `.tmp/opt20260624/check_targets.py`
- `.tmp/opt20260624/REPORT.md`

The target checker confirms that all five rows are still in
`SQLite better at:` sections. This ADR records the next design path so future
implementation work does not repeat benchmark-only attempts or weaken durable
defaults.

The evidence supports four root-cause families:

1. **Paged-table DML reload and rewrite cost.** The durable default profile uses
   `paged_row_storage = true`, `defer_table_materialization = true`, and
   `retain_paged_row_sources_after_commit = false`. After commit, the next DML
   statement can reload and decode table data before it can delete, upsert, or
   return mutated rows. For delete-heavy paths, commit can also rewrite more
   table data than the small mutation requires.
2. **Runtime fulltext index copy-on-write cost.** The Showdown `movies` table
   has a large fulltext search index. Profiling showed the 500-row resident
   bulk delete spending about 132ms in `idx_movies_search_ft` maintenance
   because the first small write clones a large runtime fulltext index. ADR 0197
   records the required base-plus-overlay design.
3. **RETURNING mutation overhead.** `UPDATE RETURNING` and `INSERT RETURNING`
   repeatedly execute compatible DML inside explicit transactions. The engine
   needs cached RETURNING DML plans, direct projection, and transaction-local
   vectorized mutation state rather than rebuilding and materializing work for
   each execution. ADR 0198 records that design.
4. **Resident cascade delete O(n) compaction.** `embedded_fast()` intentionally
   uses resident row storage (`paged_row_storage = false`) to avoid the
   autocommit reload cliff from ADR 0195. Cascade delete currently performs
   repeated full-table `retain_rows` passes over large child tables instead of
   recording logical tombstones and batching physical child-table work. ADR 0199
   records the required transaction-local cascade batching design.

The tempting benchmark shortcuts are explicitly not acceptable:

- ADR 0037 and ADR 0184 prohibit weakening default durability.
- ADR 0184 rejects making tuned durable options the default.
- ADR 0195 defines `embedded_fast()` as an explicit preset, not a global
  default flip.
- Benchmark rows must be interpreted by section: rows under
  `SQLite better at:` have SQLite as the first value and DecentDB as the second.

## Decision

Adopt a storage/executor optimization track with four coordinated directions:

1. **Persisted paged-row DML.** Add executor paths that can apply eligible
   `DELETE`, `UPDATE`, `UPSERT`, and `INSERT ... RETURNING` operations directly
   against persisted paged row sources when phase timing proves table
   materialization or persisted-row-source work is material to the gap.
2. **Runtime fulltext overlays.** Implement the ADR 0197 base-plus-overlay
   fulltext runtime representation so small DML does not clone large fulltext
   indexes.
3. **Vectorized RETURNING DML.** Implement the ADR 0198 prepared-plan,
   direct-projection, and transaction-local vectorized execution path for
   repeated RETURNING DML.
4. **Batched cascade deletes.** Add a cascade-delete execution model that
   accumulates child-row deletions per child table and applies one checked,
   batched physical operation per table when semantics permit it, following the
   ADR 0199 row-change delta model.

This ADR does not authorize a durability downgrade. `WalSyncMode::Full` remains
the durable default. Any async, normal, batched, or relaxed sync behavior must
remain opt-in and separately documented.

This ADR also does not authorize a public profile default change. Existing
`Default`, `balanced()`, `tuned_durable()`, and `embedded_fast()` semantics
remain intact unless a later ADR changes them.

### Persisted Paged-Row DML Contract

The executor may operate directly on a persisted `TablePageManifest` when it
can prove that doing so preserves ordinary DML semantics.

The initial scope is deliberately narrow:

- primary-key, row-id, unique-index, or planner-proven row-id target sets;
- predicates whose matching row IDs can be produced without full table
  materialization;
- `DELETE` operations that can be represented by tombstones or chunk-local
  rewrites;
- `INSERT ... RETURNING` and `UPSERT ... RETURNING` rows that can be assembled
  from inserted values, updated values, defaults, generated values already
  computed by the ordinary mutation path, and affected row locators;
- fallback to the existing resident/materialized path whenever constraints,
  triggers, policies, masks, sync capture, branch state, or expression
  evaluation cannot be proven equivalent.

Required guardrails:

- no bypass of foreign-key checks, triggers, row policies, column masks,
  generated columns, sync mutation recording, branch/snapshot visibility, or
  stale-index handling;
- no persistent format change in the first implementation phase;
- if the implementation needs new row payload layout, per-row diff encoding,
  persistent locator format changes, or manifest versioning, it must stop for a
  follow-up storage-format ADR and the `decentdb-migrate` obligations from ADR
  0131;
- all direct persisted DML must have an ordinary materialized fallback.

The first implementation phase should prefer existing durable structures:

- `PersistedPagedTableManifest`;
- `TablePageManifest`;
- chunk payloads;
- tombstoned row ID sets;
- overlay payloads;
- existing runtime and persistent indexes.

### Batched Cascade Delete Contract

Cascade delete may batch child removals when all of these are true:

- all parent deletions belong to one statement or one internally atomic cascade
  operation;
- the child target row IDs are gathered through the same FK matching semantics
  used by the existing path;
- restrict/no-action checks still run before destructive effects become
  visible;
- trigger ordering and row counts match the existing behavior or the operation
  falls back;
- sync mutation recording sees the same logical child deletions;
- branch and snapshot visibility are preserved.

For resident tables, a batch delete should avoid repeated full-table compaction.
Acceptable first implementations include:

- one `retain_rows` pass per affected child table after collecting all child row
  IDs;
- a temporary row-ID deletion set that is applied once per child table;
- a later chunked resident row storage design only after a separate ADR accepts
  its memory and hot-read trade-offs.

For paged tables, the preferred first path is to reuse the persisted paged-row
DML machinery and apply tombstones or chunk-local rewrites for affected child
row IDs.

### Durability and Group Commit

Closing any remaining mutation gap by changing durable defaults is rejected.
The default must continue to acknowledge a commit only after the existing
durable sync requirements are satisfied.

Strict group commit remains governed by ADR 0037 and ADR 0162. It may be a
future way to amortize fsync cost without weakening `WalSyncMode::Full`, but it
is not part of this ADR's first implementation scope. If the benchmark target
requires strict group commit rather than persisted DML work, update the group
commit ADRs first.

### Benchmark Comparability Boundary

Benchmark labeling and profile comparability must remain explicit. This ADR's
storage and cascade work should be measured in the current benchmark lanes, but
it must not silently redefine those lanes. Any change that splits full-durable
and reduced-sync charts, adds commit-inclusive metrics next to execute-only
metrics, changes backup/export sync policy, or changes which schema/index
variants are considered canonical belongs in a separate benchmark/profile
comparability design note or ADR.

## Rationale

The benchmark evidence shows that the largest gaps are dominated by structural
work: per-statement table reloads, row-value decoding, whole-table rewrite or
compaction, and fsync costs. A local fast path that only changes the final
delete operation does not remove the prepare/reload cost. A profile tweak can
hide the reload but violates the existing configuration contract.

Operating on persisted paged row sources addresses the durable default profile
without changing the default profile. It allows eligible DML to touch the
affected row IDs and chunks instead of turning small mutations into full table
materialization exercises. It is also a candidate path for RETURNING workloads,
but that work must be driven by phase timing rather than assumed reload costs.

Batched cascade delete addresses the resident profile without changing
`embedded_fast()` back to paged row storage. It preserves the read-focused
choice from ADR 0195 while avoiding repeated O(n) passes over the same child
tables.

Keeping group commit out of this first ADR avoids mixing three separate
concerns: durable sync policy, persisted-row-source DML, and FK cascade
execution. That separation makes correctness testing and performance attribution
clearer.

## Alternatives Considered

1. **Make SQLite-comparable sync modes the default.** Rejected. This weakens
   DecentDB's durable default and conflicts with ADR 0037 and ADR 0184.
2. **Make `embedded_fast()` or tuned durable options the default.** Rejected.
   This changes memory residency and paged-row behavior for callers who chose
   the durable default.
3. **Keep adding resident delete fast paths only.** Rejected as insufficient.
   The measured gaps remain above 2x because reload, prepare, cascade, and sync
   costs remain.
4. **Adopt a new persistent row format immediately.** Rejected for the first
   phase. It may be needed later, but it requires a separate format ADR,
   migration parser work, crash tests, and compatibility analysis.
5. **Batch cascade deletes by skipping FK/trigger/sync details.** Rejected.
   Cascade performance work must preserve logical behavior, not only final row
   counts.
6. **Change benchmark interpretation or acceptance criteria.** Rejected.
   A row is fixed only when a fresh full benchmark run places it under
   `DecentDB better at:`.

## Consequences

### Positive

- Targets the measured root causes without weakening durability.
- Gives implementation agents a bounded design path and fallback rules.
- Keeps profile semantics stable.
- Enables focused tests around persisted-row-source mutation and cascade
  batching.

### Negative

- More executor complexity: each optimized path needs conservative proof and
  fallback.
- The first persisted-DML phase may not close every shape of `UPSERT` or
  `INSERT RETURNING` if the remaining cost is strict fsync.
- Batched cascade delete must account for trigger ordering and sync capture,
  which may limit fast-path eligibility.
- If existing manifest/tombstone/overlay structures are insufficient, a later
  storage-format ADR will be required.

## Implementation Phases

### Phase 0: Benchmark and Acceptance Harness

- Keep `.tmp/opt*/check_targets.py` style parsing in the workflow.
- Add or promote a durable target checker that reports winner, section,
  DecentDB value, SQLite value, and ratio for the five benchmark rows.
- Require fresh baseline and final full benchmark runs for performance claims.

### Phase 1: Persisted Paged DELETE

- Add a direct persisted-paged delete path for row-id sets produced by primary
  key, unique index, or proven row-id predicates.
- Apply tombstones or chunk-local rewrites without full table materialization.
- Preserve index invalidation, FK checks, triggers, sync tracking, branch state,
  and result row counts.
- Add tests comparing the optimized path to the existing materialized path.

### Phase 2: Persisted Paged INSERT/UPSERT RETURNING

- First add phase timing for `UPSERT` and `INSERT RETURNING` that separates
  conflict lookup, row construction, result materialization, persistence,
  commit sync, and binding overhead.
- Avoid table reload for inserted or updated rows when RETURNING values can be
  assembled from mutation inputs and affected row locators.
- Preserve conflict detection, generated/default values, constraints, and
  returned row order.
- Fall back for expressions or policies that require ordinary row materialization.

### Phase 3: Batched Cascade Deletes

- Accumulate child row IDs per child table during one cascade operation.
- Run restrict/no-action checks before applying destructive changes.
- Apply one resident or persisted-paged deletion batch per child table.
- Preserve trigger ordering, affected row counts, and sync mutation capture.

### Phase 4: Strict Durable Fsync Amortization Review

- If `UPSERT` remains dominated by `WalSyncMode::Full` after Phases 1-3, reopen
  ADR 0037/0162 work rather than changing this ADR.
- Any strict group commit implementation must keep durable acknowledgement
  semantics and add crash-injection coverage.

## Validation Requirements

Correctness:

- unit tests for persisted-paged delete/update/insert returning eligibility and
  fallback;
- differential tests comparing optimized and materialized DML paths;
- FK cascade tests for restrict, no-action, set-null/default where supported,
  delete cascade, triggers, indexes, and sync mutation tracking;
- branch/snapshot visibility tests when optimized DML runs on branch state;
- crash/recovery tests for any path that changes persistence ordering;
- stale-index and constraint-violation rollback tests.

Performance:

- microbenchmarks that split prepare, execute, result materialization, persist,
  checkpoint, and Python binding overhead where relevant;
- the five target rows must move under `DecentDB better at:` in a fresh
  `python scripts/benchmark_runner.py --report-only` run;
- targets with less than a 1.10x DecentDB advantage require at least three
  confirming full benchmark runs;
- result equivalence must remain `ok` in benchmark logs.

Required developer checks before merging implementation:

```bash
cargo fmt --check
cargo check -p decentdb
cargo test -p decentdb --lib
cargo clippy -p decentdb --all-targets --all-features -- -D warnings
python scripts/do-pre-commit-checks.py --mode fast
```

Run broader binding and crash validation when changed code touches C ABI,
Python benchmark surfaces, FK cascade semantics, WAL/checkpoint ordering, or
branch/snapshot behavior.

## Acceptance Criteria

This ADR's implementation track is complete only when a fresh full benchmark
run shows all five current target rows under `DecentDB better at:`:

1. Showdown GLM52 native defaults `Bulk DELETE`
2. Showdown GLM52 scale `UPDATE RETURNING`
3. Showdown GLM52 scale `Bulk DELETE`
4. MovieDB scratch `Cascade delete batch`
5. Showdown GLM52 native defaults `INSERT RETURNING`

The adjacent Showdown GLM52 scale `INSERT RETURNING` row shares the same root
cause as native-default `INSERT RETURNING`; ADR 0198 requires it to move under
`DecentDB better at:` as well.

The final implementation report must include:

- full benchmark log directory;
- parsed target-checker output;
- before/after table for all five targets;
- microbenchmark phase breakdowns;
- root cause and fix mapping per target;
- exact files changed;
- exact validation commands;
- remaining risks and variance notes.

## References

- `design/PRD.md`
- `design/TESTING_STRATEGY.md`
- `design/adr/0037-group-commit-wal-batching.md`
- `design/adr/0131-legacy-format-migrations.md`
- `design/adr/0143-on-disk-row-scan-executor.md`
- `design/adr/0145-paged-table-row-source.md`
- `design/adr/0162-engine-owned-write-queue-strict-group-commit.md`
- `design/adr/0184-default-fast-planner-and-runtime-contract.md`
- `design/adr/0195-embedded-fast-profile-and-resident-read-fast-path.md`
- `design/adr/0197-fulltext-runtime-index-delta-overlays.md`
- `design/adr/0198-vectorized-returning-dml-execution.md`
- `design/adr/0199-transaction-local-cascade-delete-batching.md`
- `.tmp/perf-validate/20260624-171109`
- `.tmp/opt20260624/REPORT.md`
