# Default-Fast Performance And Storage Efficiency

**Date:** 2026-05-27
**Status:** Proposed
**Future Version:** vNext
**Roadmap:** [`FUTURE_WINS.md`](FUTURE_WINS.md)
**Document Type:** Implementation SPEC
**Audience:** Core engine maintainers, planner and executor maintainers, WAL and
storage maintainers, binding maintainers, benchmark maintainers, documentation
authors, coding agents

`vNext` means the first release bucket after 2.7.0 only after this scope is
explicitly accepted. It is not a promise that every follow-up listed here lands
in that release.

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
- [`data/bench_summary.json`](../data/bench_summary.json) as the current
  release benchmark snapshot. Section 4 captures the baseline values this spec
  references; the JSON file may change as new benchmark runs are accepted.

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

- Reduce the visible gap between `decentdb_balanced_durable` and
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
This is the frozen planning baseline until rollout step 3 recaptures an
accepted baseline on the expanded benchmark suite. If step 3 recaptures the
baseline, update this section before judging implementation patches.

The existing JSON names the current default row `decentdb_default_durable`.
Within this spec, that row is the baseline for the canonical release profile
`decentdb_balanced_durable`:

```text
decentdb_balanced_durable baseline: wal_sync_full cache_size_mb=4
```

The tuned DecentDB profile is:

```text
decentdb_tuned_durable:wal_sync_full cache_size_mb=64 retain_paged_row_sources_after_commit=true paged_row_storage=false wal_autocheckpoint=0
```

Representative current p95 values:

| Metric | Balanced Baseline | Tuned Durable | SQLite WAL FULL |
|---|---:|---:|---:|
| Point read p95 | 0.0169598 ms | 0.001907 ms | 0.0034974 ms |
| Range scan p95 | 0.6764458 ms | 0.012146 ms | 0.011377 ms |
| Join p95 | 0.0311028 ms | 0.0030208 ms | 0.0037916 ms |
| Aggregate p95 | 0.1431072 ms | 0.0258068 ms | 0.0374144 ms |
| Durable commit p95 | 0.9252148 ms | 0.3016636 ms | 0.3197528 ms |
| Concurrent read p95 | 0.0402664 ms | 0.0055302 ms | 0.023315 ms |
| Database size after checkpoint | 3.6914 MiB | 3.2110 MiB | 2.1953 MiB |
| Insert rows/sec | 1,617,064 | 1,787,297 | 2,266,284 |

This baseline shows five separate problems:

- some default gaps are caused by conservative memory/default choices;
- some default gaps are caused by planner/executor paths that do not exploit
  existing metadata broadly enough;
- concurrent reads are materially behind the tuned profile and should improve
  through the same cache, row-source retention, and planner/executor work;
- insert throughput is behind tuned DecentDB and SQLite; first-phase work should
  measure whether checkpoint/cache tuning closes this before proposing write
  format changes;
- some storage gaps may require deeper layout work, but should not be guessed
  before the cheap, non-format work is measured. The tuned profile is still
  larger than SQLite in this snapshot, so phase-1 storage work may improve WAL
  and avoidable rewrite behavior without fully closing the file-size gap.

## 5. Definition Of Done

This win is complete only when all of these are true:

- `decentdb_balanced_durable` release benchmarks improve against the accepted
  baseline without weakening `WalSyncMode::Full`.
- Release assets include native `decentdb_balanced_durable`, native
  `decentdb_tuned_durable`, and at least one maintained binding profile for the
  targeted hot paths.
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

### 5.1 Success Targets

Before implementation changes begin, benchmark maintainers must run the expanded
benchmark slices from section 6 and record accepted target thresholds. Those
targets may refine the provisional bands below, but they must be committed to
the spec or an accepted benchmark note before performance patches are judged.

Ratio targets are valid only when the default profile improves. A target is not
met by making the tuned profile slower. Tuned durable, SQLite, and DuckDB
comparison profiles must remain within accepted benchmark noise or be explicitly
rebaselined with a reason before ratio-based success is claimed.

Provisional phase-1 targets:

| Metric Category | Target Band |
|---|---|
| Point reads | Reduce default/tuned p95 ratio from about 9x to 4x or better, with `decentdb_balanced_durable` point-read p95 no worse than 0.008 ms on the accepted baseline lane. |
| Concurrent reads | Reduce default/tuned p95 ratio from about 7x to 4x or better, with `decentdb_balanced_durable` concurrent-read p95 no worse than 0.023 ms on the accepted baseline lane. |
| Range scans | Reduce default/tuned p95 ratio by at least 3x from the current baseline, with `decentdb_balanced_durable` range-scan p95 no worse than 0.25 ms. |
| Aggregates | Reduce default/tuned p95 ratio by at least 3x from the current baseline, with `decentdb_balanced_durable` aggregate p95 no worse than 0.05 ms. |
| Joins | Reduce default/tuned p95 ratio by at least 2x from the current baseline, with `decentdb_balanced_durable` join p95 no worse than 0.016 ms. |
| Durable commit p95 | Improve default p95 by at least 20%, with `decentdb_balanced_durable` commit p95 no worse than 0.75 ms and without weakening `WalSyncMode::Full`. |
| Insert throughput | Improve default throughput by at least 10%, with `decentdb_balanced_durable` insert throughput at or above 1,775,000 rows/sec on the accepted baseline lane, or explicitly defer the remaining gap with benchmark evidence that it requires format/write-path work. |
| Storage size | Do not regress `decentdb_balanced_durable` combined main database plus WAL size after checkpoint; improve that combined size by at least 5% from the accepted baseline where existing checkpoint/freelist/rewrite mechanisms can do so. The current denominator is 3.6914 MiB. |
| Cold open and first query | Establish accepted p95 baselines for small, medium, and large databases, then improve targeted `decentdb_balanced_durable` profiles by at least 20%. |

If a target is infeasible after benchmark evidence, the spec must be updated
with the measured reason and the follow-up design path, such as a format ADR.

## 6. Benchmark Contract

### 6.1 Required Profiles

Release performance assets must keep at least these profiles distinct:

| Profile | Purpose |
|---|---|
| `decentdb_balanced_durable` | User-visible safe default candidate. Must use full durable WAL sync. |
| `decentdb_low_memory_durable` | Explicit constrained-host profile, initially preserving the current 4 MiB cache behavior unless benchmarks justify a different low-memory value. |
| `decentdb_tuned_durable` | Explicit high-memory durable tuning profile. |
| `sqlite_wal_full` | Durable SQLite comparison, with SQLite-specific tuning named. |
| `duckdb` | Embedded analytical engine comparison, with explicit engine-default durability and threading caveats named. |
| Python binding profile | First release-blocking application-facing latency profile. |
| browser/mobile profile | Required when a change claims browser/mobile startup, query, or memory benefits. |

Profile names must appear in JSON metadata and generated release charts. Tuned
results must never replace default results in release-facing assets.

Release assets must describe competitor durability settings precisely enough
that readers can compare latency and safety together. For DuckDB, the metadata
must state the engine-default durability mode used by the benchmark and any
threading limitation, such as single-threaded execution or non-`Send`
connections. Until the benchmark records exact DuckDB PRAGMAs/settings and
durability guarantees, the DuckDB row must be labeled as `duckdb_engine_default`
and not presented as a full-sync WAL peer.

The first maintained binding profile is Python. Python should exercise the
stable C ABI prepared-statement and result APIs directly and report both
prepared-statement execution latency and result access/materialization cost.
After Python, add Node or Dart based on whether browser or mobile work is in the
same release bucket.

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

The canonical concurrent-read workload is four reader threads, each executing
25,000 prepared point lookups over a deterministic permutation. It is read-only:
no writer is active during the baseline concurrent-read measurement, and reader
threads keep their prepared statements alive for the duration of the run. Any
active-writer concurrency claim requires a separately named mixed workload with
the writer cadence, commit mode, reader lifetime, and checkpoint behavior
recorded in metadata.

New default-fast work should add:

- cold open;
- first query after open;
- first prepared query after open;
- checkpoint latency and post-checkpoint file size;
- WAL growth under long readers;
- large-result materialization memory;
- binding prepared-statement round trip latency;
- WASM binary and JSON result transport where changed.

Cold-open and first-query benchmarks must define their cache protocol. Each
accepted benchmark must state whether it uses a fresh process, warm process,
OS-page-cache eviction such as `posix_fadvise(DONTNEED)` where available,
isolated temporary storage, or an intentionally warm cache. Release charts must
not mix cold and warm results under one label.

Cold-open fixtures must use a stable OLTP-ish schema shape, not only row counts:

- one primary table with an `INT64` primary key, at least six scalar columns,
  and one variable-width `TEXT` or `BLOB` payload column;
- one child table with a foreign key to the primary table;
- primary-key indexes plus at least three secondary B+Tree indexes, including
  one composite index and one `INCLUDE (...)` covering-index candidate;
- medium and large fixtures must include overflow pages by making at least 10%
  of payload values larger than one page payload;
- small fixtures use the same schema at 10,000 rows, medium fixtures use
  1,000,000 rows, and large fixtures use 10,000,000 rows or a fixed 1-2 GiB
  database, whichever is more practical for release validation.

### 6.3 Baseline Policy

Every performance change must say which baseline it targets:

- `decentdb_balanced_durable` native;
- tuned durable native;
- binding path;
- browser/WASM;
- mobile;
- storage size;
- cold-open/first-query.

Any regression accepted for one metric must be paired with an explicit product
reason and a compensating win. Durable commit p95, correctness, and recovery
behavior are not acceptable regression sinks for read benchmark wins.

### 6.4 Regression Guardrails

Default-fast benchmarks are enforced in three lanes:

| Lane | Scope | Enforcement |
|---|---|---|
| PR targeted | Required for patches that claim performance, storage, cache, planner, executor, binding, WAL, or checkpoint improvements. | Blocks the patch when a targeted p95 latency regresses by more than 10%, throughput regresses by more than 10%, storage size regresses by more than 5%, or memory regresses by more than 10% without an accepted explanation. |
| Nightly full | Runs the expanded native default/tuned, storage, cold-open, and selected binding suite. | Opens or updates a benchmark regression issue owned by benchmark maintainers and the touched area owner. |
| Release blocking | Runs the full accepted profile matrix before publishing release assets. | Blocks release if any accepted target regresses beyond threshold without an approved baseline update. |

Benchmark maintainers own harness reliability and baseline updates. Area
maintainers own triage for regressions in their subsystem. If noise makes a
threshold unreliable, the benchmark must be fixed or relabeled before it can be
used as a release-blocking gate.

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

The default page cache size is the leading first-phase hypothesis. The current
benchmark gap compares a 4 MiB default profile with a 64 MiB tuned profile, so
the first benchmark slice must sweep at least 4, 8, 16, 24, 32, and 64 MiB
under the same durable settings before changing deeper executor or storage code
for read latency. The first serious default candidate is 16 MiB. Move the balanced
default to 32 MiB only if benchmarks show a clear cross-workload win over
16 MiB across point, concurrent, range, join, aggregate, browser/mobile, and
memory profiles.

If the accepted default cache grows, DecentDB must also keep an explicit
low-memory profile or documented open option for constrained hosts. The
low-memory profile should initially preserve the current 4 MiB behavior.
Browser and mobile benchmark lanes are binding constraints for default
increases, not after-the-fact documentation work.

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

Deferred table materialization refers to the ADR 0143 open-time behavior where
table row data stays unloaded until a statement needs it. Cold-open benchmarks
must report whether first-query latency includes deferred table load work.

Open-time checkpoint tuning must be measured separately from query execution so
first-query wins are not hiding longer open latency or surprise checkpoint work.

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

Concurrent-read improvements are not a separate concurrency model change. They
should fall out of this track through better cache defaults, less repeated
row-source loading, narrower covered index reads, and lower prepared-statement
overhead. If benchmark evidence points instead to lock contention or reader
retention semantics, that requires a follow-up ADR before changing the
one-writer/many-readers contract.

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

Named profile helpers are accepted, but open-time knobs remain the authoritative
configuration contract. Each helper maps to one canonical release benchmark
profile:

| Helper | Canonical Release Profile | Direction |
|---|---|---|
| `balanced` | `decentdb_balanced_durable` | Default durable profile, using the accepted cache-size result from the sweep. Start from a 16 MiB candidate. |
| `low_memory` | `decentdb_low_memory_durable` | Constrained-host durable profile, initially 4 MiB unless benchmark evidence changes it. |
| `tuned_durable` | `decentdb_tuned_durable` | Explicit high-memory durable profile for benchmark and power-user tuning. |

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

The binding benchmark must report both prepared-statement execution latency and
result access cost. A native-only improvement is not sufficient evidence for an
application-facing claim if the maintained binding still spends most time in
FFI conversion, allocation, or full-result materialization.

### 7.6 Storage Efficiency

First-phase storage efficiency should focus on existing mechanisms:

- checkpoint policy and WAL truncation;
- freelist and trailing-page trimming;
- avoiding unnecessary table/index rewrites;
- index payload density where no format change is required;
- measuring main database size and WAL size separately.

The current baseline suggests phase-1 work may not fully close the file-size gap
with SQLite because the tuned DecentDB profile is still materially larger.
Phase 1 should therefore be honest about what it can fix: excess WAL retention,
avoidable rewrites, freelist/trailing-page waste, and checkpoint behavior. Page
density, key-prefix encoding, and compression belong in later ADR-backed work if
the measured residual gap is still product-significant.

TDE interaction must be explicit even when no behavior changes are intended.
Default cache-size changes should not extend encryption key lifetime beyond the
open database handle, should not add key material to metrics or benchmark
metadata, and should not change re-key behavior. Checkpoint or WAL changes must
run at least one TDE-enabled recovery/checkpoint smoke before release because
TDE wraps database and WAL bytes on the same logical paths.

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
- cache is undersized based on observed cache metrics, not only file size;
- page-cache hit/miss ratio or eviction rate shows working-set pressure, once
  those metrics are exposed through stable runtime surfaces.

Doctor must not recommend unsafe durability downgrades.

## 8. Validation Matrix

Minimum validation for each implementation slice:

| Change Area | Required Validation |
|---|---|
| Default config | Native benchmark before/after, docs update, memory-bound check including peak RSS/heap, steady-state RSS/heap after checkpoint, and configured cache/default profile reporting |
| WAL/checkpoint tuning | WAL recovery tests, checkpoint tests, Doctor WAL checks, benchmark |
| Cold open | open/reopen tests, crash recovery tests, cold-open benchmark |
| Planner/executor | targeted SQL tests, `EXPLAIN` coverage, benchmark hot path |
| Covering indexes | projection/join/range tests, policy/mask fallback tests, stale-index tests |
| `ANALYZE`/stats | stats persistence tests, no-stats fallback tests, plan tests |
| Binding hot path | C ABI tests plus impacted binding smoke/benchmark |
| WASM/mobile | browser/mobile benchmark guardrails and lifecycle smoke |
| Future storage format phase | ADR, migration parser, crash/recovery, compatibility tests |

Memory-bound checks must include any platform surface affected by the default
change. Native checks report RSS where available; browser checks report WASM
heap and JS-visible memory where available; mobile checks report process memory
from the accepted device/simulator lane. If a platform cannot provide a stable
absolute memory reading, it must still report relative growth versus baseline.

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
| Cold-open benchmarks are not reproducible | Require explicit cold/warm cache protocol labels before accepting results. |

## 11. Rollout Plan

ADR dependency: if ADR 0184 is delayed, rejected, or reopened in a branch,
planner/executor work from sections 7.3 and 7.4 is blocked. Benchmark harness
work, profile naming, cache sweeps, binding benchmarks, and documentation can
still proceed because they do not depend on the ADR's covering-index contract.

1. Land this spec and ADR 0184.
2. Add benchmark slices for cold open, first query, storage size, and binding
   prepared-statement paths.
3. Run baseline measurements and record accepted targets. This is a gate before
   default tuning or planner/executor changes are judged complete.
4. Run the 4/8/16/24/32/64 MiB cache sweep, using 16 MiB as the first balanced
   default candidate and preserving an explicit low-memory profile.
5. Add Python as the first maintained binding benchmark profile.
6. Add cold-open fixtures for small, medium, and large databases using the
   accepted row-count/size targets in section 12.
7. Implement no-format default tuning and checkpoint policy changes.
8. Implement ADR 0184 planner/executor improvements, starting with covering
   index cases that can be proven safe.
9. Update docs and release benchmark assets.
10. Evaluate whether remaining storage-size gaps justify a concrete compression
   or layout ADR.

## 12. Accepted Recommendations

- Use 16 MiB as the first balanced default cache-size candidate. Still run the
  full 4/8/16/24/32/64 MiB sweep, keep 4 MiB available as `low_memory`, and move
  to 32 MiB only if it is clearly better across the full benchmark and memory
  matrix.
- Add named profile helpers while keeping open-time knobs authoritative. The
  accepted profile names are `balanced`, `low_memory`, and `tuned_durable`.
- Use Python as the first release-blocking maintained binding latency profile.
  Add Node or Dart next depending on whether browser or mobile work is in the
  same release bucket.
- Use three cold-open fixtures: small at 10,000 rows, medium at 1,000,000 rows,
  and large at 10,000,000 rows or a fixed 1-2 GiB database, whichever is more
  practical for release validation. Small and medium should run in regular CI
  once stable; large may be release/nightly only.
- Defer persistent covering-index payload storage. First implement runtime-only
  covering-index execution with existing metadata. If residual base-row fetch
  overhead remains product-significant, write a focused format ADR for
  persistent covering payloads.
