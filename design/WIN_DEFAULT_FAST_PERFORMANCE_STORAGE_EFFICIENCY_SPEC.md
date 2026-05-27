# Default-Fast Performance And Storage Efficiency

**Date:** 2026-05-27
**Status:** Proposed
**Future Version:** vNext
**Roadmap:** [`FUTURE_WINS.md`](FUTURE_WINS.md)
**Document Type:** Implementation SPEC
**Audience:** Core engine maintainers, planner and executor maintainers, WAL and
storage maintainers, binding maintainers, benchmark maintainers, documentation
authors, coding agents

**Governing ADRs:**

- [`adr/0184-default-fast-planner-and-runtime-contract.md`](adr/0184-default-fast-planner-and-runtime-contract.md)

**Required follow-up ADRs before implementation:**

- Persistent page compression, key-prefix compression, page-layout changes, or
  any file-format/version change.
- WAL format changes, checkpoint semantic changes, or recovery-order changes.
- Stable C ABI or maintained-binding streaming result contracts that alter
  result ownership, cancellation, or lifetime semantics.
- Major parser, planner, statistics, or storage dependency additions.
- New unsafe storage, VFS, or memory-mapping behavior beyond already accepted
  FFI/VFS boundaries.

**Related inputs:**

- [`FUTURE_WINS.md`](FUTURE_WINS.md)
- [`BENCHMARKING_GUIDE.md`](BENCHMARKING_GUIDE.md)
- [`PRD.md`](PRD.md)
- [`SPEC.md`](SPEC.md)
- [`TESTING_STRATEGY.md`](TESTING_STRATEGY.md)
- [`adr/0131-legacy-format-migrations.md`](adr/0131-legacy-format-migrations.md)
- [`adr/0143-on-disk-row-scan-executor.md`](adr/0143-on-disk-row-scan-executor.md)
- [`adr/0144-persistent-primary-key-index.md`](adr/0144-persistent-primary-key-index.md)
- [`adr/0145-paged-table-row-source.md`](adr/0145-paged-table-row-source.md)
- [`adr/0162-engine-owned-write-queue-strict-group-commit.md`](adr/0162-engine-owned-write-queue-strict-group-commit.md)
- [`adr/0163-operational-sys-metrics.md`](adr/0163-operational-sys-metrics.md)
- [`docs/user-guide/performance.md`](../docs/user-guide/performance.md)
- [`docs/api/configuration.md`](../docs/api/configuration.md)
- [`data/bench_summary.json`](../data/bench_summary.json)

---

## 1. Executive Summary

DecentDB's tuned durable profile is competitive in the release benchmark assets,
but users experience the engine through defaults: opening a database, preparing
and running ordinary statements, reading through bindings, checkpointing without
manual tuning, and keeping local files small enough for desktop, browser, and
mobile apps.

This win makes the durable default profile fast by default without weakening
durability, hiding unsafe modes, or conflating tuned benchmark results with
normal application behavior.

The work is intentionally measurement driven. The first implementation phase
must focus on changes that do not require persistent-format changes:

- default configuration and checkpoint policy tuning;
- cold-open and first-query path reduction;
- planner/executor use of existing index metadata, especially covering indexes;
- prepared-statement and binding hot-path improvements;
- memory-bounded result access where existing materialization creates measured
  pressure;
- release benchmark guardrails that distinguish default durable, tuned durable,
  native, binding, browser, and mobile profiles.

Page compression, key-prefix compression, layout rewrites, and WAL semantic
changes are not first-phase defaults. They require separate ADRs and accepted
benchmarks that prove the durability/recovery complexity is worth it.

## 2. Product Goals

- Reduce the visible gap between `decentdb_default_durable` and
  `decentdb_tuned_durable` benchmark profiles.
- Keep durable defaults durable: no default switch from full durable WAL sync to
  weaker acknowledgement semantics.
- Make cold open and first query predictable for large local databases.
- Keep file size plus WAL size competitive for normal OLTP app datasets.
- Improve common prepared-statement read paths without forcing application
  authors to learn engine-specific tuning.
- Make maintained bindings and WASM paths fast enough that benchmark claims are
  not native-only artifacts.
- Preserve explicit profile names in release assets so default and tuned results
  cannot be confused.
- Add performance Doctor findings only where DecentDB can attach concrete
  evidence and safe remediation guidance.

## 3. Non-Goals

- No durability downgrade for the default profile.
- No file-format change without a separate ADR and migration parser coverage.
- No page/key/layout compression work until benchmark profiles identify the
  storage waste and an ADR accepts the encoding and recovery behavior.
- No general OLAP, columnar, time-series, or foreign-data storage mode.
- No broad binding rewrite unless a measured hot path proves it is necessary.
- No always-on tracing that adds unbounded hot-path overhead.
- No Doctor advice based only on generic tuning folklore.

## 4. Current Baseline

The current release benchmark summary was aggregated on 2026-05-22 and uses the
`single_thread_prepared_statement_oltp_with_concurrent_read_extension` profile.
The default DecentDB profile is:

```text
decentdb_default_durable:wal_sync_full cache_size_mb=4
```

The tuned DecentDB profile is:

```text
decentdb_tuned_durable:wal_sync_full cache_size_mb=64 retain_paged_row_sources_after_commit=true paged_row_storage=false wal_autocheckpoint=0
```

Representative current p95 values:

| Metric | Default Durable | Tuned Durable | SQLite WAL FULL |
|---|---:|---:|---:|
| Point read p95 | 0.0169598 ms | 0.001907 ms | 0.0034974 ms |
| Range scan p95 | 0.6764458 ms | 0.012146 ms | 0.011377 ms |
| Join p95 | 0.0311028 ms | 0.0030208 ms | 0.0037916 ms |
| Aggregate p95 | 0.1431072 ms | 0.0258068 ms | 0.0374144 ms |
| Durable commit p95 | 0.9252148 ms | 0.3016636 ms | 0.3197528 ms |
| Concurrent read p95 | 0.0402664 ms | 0.0055302 ms | 0.023315 ms |
| Database size after checkpoint | 3.6914 MiB | 3.2110 MiB | 2.1953 MiB |
| Insert rows/sec | 1,617,064 | 1,787,297 | 2,266,284 |

This baseline shows three separate problems:

- some default gaps are caused by conservative memory/default choices;
- some default gaps are caused by planner/executor paths that do not exploit
  existing metadata broadly enough;
- some storage gaps may require deeper layout work, but should not be guessed
  before the cheap, non-format work is measured.

## 5. Definition Of Done

This win is complete only when all of these are true:

- `decentdb_default_durable` release benchmarks improve against the accepted
  baseline without weakening `WalSyncMode::Full`.
- Release assets include native default durable, native tuned durable, and at
  least one maintained binding profile for the targeted hot paths.
- Browser and mobile guardrails exist for any work that claims startup,
  first-query, result materialization, or package/runtime improvements on those
  surfaces.
- Cold-open and first-query benchmarks exist for at least small, medium, and
  large persisted databases.
- Storage-size benchmarks report main database, WAL, and combined size after
  checkpoint.
- Covering-index execution uses existing metadata where accepted by ADR 0184,
  with tests proving correct fallback when a query is not safely covered.
- `ANALYZE` and planner statistics remain optional for correctness and useful
  for performance; no ordinary app workflow requires ritual tuning before
  basic indexed queries are fast.
- User docs describe default performance expectations and tuning knobs without
  implying unsafe durability settings.
- Doctor performance/storage findings, if added, include evidence fields and
  safe recommendations.

## 6. Benchmark Contract

### 6.1 Required Profiles

Release performance assets must keep at least these profiles distinct:

| Profile | Purpose |
|---|---|
| `decentdb_default_durable` | User-visible safe default. Must use full durable WAL sync. |
| `decentdb_tuned_durable` | Explicit high-memory durable tuning profile. |
| `sqlite_wal_full` | Durable SQLite comparison, with SQLite-specific tuning named. |
| `duckdb` | Embedded analytical engine comparison, with durability caveats named. |
| maintained binding profile | Application-facing latency through at least one stable binding. |
| browser/mobile profile | Required when a change claims browser/mobile startup, query, or memory benefits. |

Profile names must appear in JSON metadata and generated release charts. Tuned
results must never replace default results in release-facing assets.

### 6.2 Workloads

The native benchmark suite must continue to cover:

- prepared single-row insert loop inside one explicit transaction;
- prepared point lookup with value materialization;
- prepared single-row durable autocommit insert p95;
- prepared indexed join lookup;
- prepared ordered range scan;
- prepared count/sum aggregate;
- concurrent prepared point lookups;
- final size after checkpoint.

New default-fast work should add:

- cold open;
- first query after open;
- first prepared query after open;
- checkpoint latency and post-checkpoint file size;
- WAL growth under long readers;
- large-result materialization memory;
- binding prepared-statement round trip latency;
- WASM binary and JSON result transport where changed.

### 6.3 Baseline Policy

Every performance change must say which baseline it targets:

- default durable native;
- tuned durable native;
- binding path;
- browser/WASM;
- mobile;
- storage size;
- cold-open/first-query.

Any regression accepted for one metric must be paired with an explicit product
reason and a compensating win. Durable commit p95, correctness, and recovery
behavior are not acceptable regression sinks for read benchmark wins.

## 7. Implementation Tracks

### 7.1 Default Configuration And Checkpoint Policy

Evaluate safe default changes before deeper format work:

- default page cache size;
- page-pool sizing;
- cached payload limits;
- auto-checkpoint thresholds;
- background checkpoint worker behavior;
- open-time checkpoint threshold;
- row-source retention after write commits.

Accepted changes must preserve documented memory bounds. If a default increases
resident memory, the benchmark and docs must state the new behavior and the
reason it is appropriate for embedded app defaults.

### 7.2 Cold Open And First Query

Optimize open and first-use behavior around:

- catalog decode;
- WAL recovery and WAL index construction;
- open-time checkpoint decisions;
- deferred table materialization;
- persistent primary-key locator availability;
- lazy runtime index construction where correctness permits;
- prepared-statement first execution after schema-cookie validation.

Cold-open changes must preserve crash recovery semantics and cross-process
coordination rules.

### 7.3 Planner And Executor Hot Paths

The planner/executor work is governed by ADR 0184.

Priority work:

- exploit `INCLUDE (...)` metadata for covering B+Tree queries where row
  policies, masks, projection expressions, and stale-index state make it safe;
- extend indexed projection and indexed join paths to avoid base-row
  materialization when all required values are available from the index entry;
- keep row-id point lookups fast under the default paged-row-storage profile;
- improve aggregate/range scans where the planner can prove a narrow row-source
  path is sufficient;
- keep `EXPLAIN` and `EXPLAIN ANALYZE` honest about selected fast paths.

### 7.4 Statistics And Plan Reuse

`ANALYZE` already records table row counts and index key cardinality. This win
should make that data more useful without making it mandatory ritual:

- use stats where present for index-vs-scan choices;
- keep reasonable heuristic behavior when stats are absent;
- invalidate stale plans on schema-cookie changes;
- avoid adaptive behavior that makes prepared-statement latency unpredictable;
- record enough benchmark evidence before adding new persisted statistics.

New persisted statistics fields require format/migration analysis before
implementation.

### 7.5 Binding And Result Materialization Hot Paths

Native performance wins only matter if maintained bindings can reach them.

Target areas:

- prepared-statement execution through the C ABI;
- statement reset/clear/reuse behavior;
- row-view APIs that avoid per-cell allocations;
- large-result paging or streaming where current materialization creates
  measured memory pressure;
- WASM binary result transport and async iteration.

Any broad stable C ABI result streaming contract requires a follow-up ADR before
implementation. Binding-only internal batching may be implemented without a new
ADR when it does not change public lifetime or ownership semantics.

### 7.6 Storage Efficiency

First-phase storage efficiency should focus on existing mechanisms:

- checkpoint policy and WAL truncation;
- freelist and trailing-page trimming;
- avoiding unnecessary table/index rewrites;
- index payload density where no format change is required;
- measuring main database size and WAL size separately.

Potential later work:

- page-level compression;
- key-prefix compression;
- table/index page layout changes;
- alternative overflow payload layout;
- persistent covering-index payload encoding.

Each later item requires a separate ADR with:

- exact on-disk encoding;
- format-version impact;
- recovery and torn-write behavior;
- checkpoint and WAL interaction;
- TDE interaction;
- migration-parser obligations;
- benchmark data proving the added complexity is justified.

### 7.7 Doctor And Diagnostics

Doctor may add performance/storage findings only after runtime surfaces can
explain them. Acceptable examples:

- WAL file is large and active readers are retaining history;
- checkpoint is blocked by process reader slots;
- database has high freelist fragmentation and a safe vacuum workflow exists;
- index is stale or verification fails;
- cache is undersized based on observed cache metrics, not only file size.

Doctor must not recommend unsafe durability downgrades.

## 8. Validation Matrix

Minimum validation for each implementation slice:

| Change Area | Required Validation |
|---|---|
| Default config | Native benchmark before/after, docs update, memory-bound check |
| WAL/checkpoint tuning | WAL recovery tests, checkpoint tests, Doctor WAL checks, benchmark |
| Cold open | open/reopen tests, crash recovery tests, cold-open benchmark |
| Planner/executor | targeted SQL tests, `EXPLAIN` coverage, benchmark hot path |
| Covering indexes | projection/join/range tests, policy/mask fallback tests, stale-index tests |
| `ANALYZE`/stats | stats persistence tests, no-stats fallback tests, plan tests |
| Binding hot path | C ABI tests plus impacted binding smoke/benchmark |
| WASM/mobile | browser/mobile benchmark guardrails and lifecycle smoke |
| Storage format | ADR, migration parser, crash/recovery, compatibility tests |

Standard Rust validation remains:

```bash
cargo fmt --check
cargo check -p decentdb
cargo lint
cargo t -p decentdb -- <targeted-filter>
```

## 9. Documentation Requirements

Update user-facing docs when public behavior changes:

- `docs/user-guide/performance.md` for defaults, tuning, and benchmark
  interpretation;
- `docs/api/configuration.md` for open-time options and default values;
- binding docs for prepared-statement and streaming/page APIs;
- `docs/api/wasm.md` and Dart/mobile docs when browser/mobile profiles are
  affected;
- benchmark docs and release assets when profile names or workloads change.

## 10. Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Default changes improve benchmark charts but increase embedded memory too much | Require memory-bound reporting and profile-specific docs. |
| Planner fast path skips policy, mask, generated-column, or stale-index checks | ADR 0184 requires safe fallback and tests for constrained cases. |
| Adaptive stats create unpredictable prepared-statement latency | Keep first phase heuristic and schema-cookie invalidation based. |
| Compression/layout work delays practical wins | Keep format-changing storage work out of first phase until measured. |
| Binding benchmarks lag native improvements | Require maintained-binding profiles for application-facing claims. |
| Doctor emits vague tuning advice | Require structured evidence and safe remediation before adding findings. |

## 11. Rollout Plan

1. Land this spec and ADR 0184.
2. Add benchmark slices for cold open, first query, storage size, and binding
   prepared-statement paths.
3. Run baseline measurements and record accepted targets.
4. Implement no-format default tuning and checkpoint policy changes.
5. Implement ADR 0184 planner/executor improvements, starting with covering
   index cases that can be proven safe.
6. Update docs and release benchmark assets.
7. Evaluate whether remaining storage-size gaps justify a concrete compression
   or layout ADR.

## 12. Open Questions

- What default cache size gives the best cross-platform balance for desktop,
  mobile, browser, and small server embedders?
- Should DecentDB expose named default profiles, such as `balanced` and
  `low_memory`, or only open-time knobs?
- Which maintained binding should be the first release-blocking application
  latency profile?
- What cold-open database sizes should define small, medium, and large release
  guardrails?
- Does persistent covering-index payload storage become necessary after
  runtime-only covering paths are measured?
