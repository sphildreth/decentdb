# DecentDB Performance Improvements Roadmap

**Date:** 2026-03-24  
**Audience:** engine/core contributors  
**Purpose:** identify and prioritize core-engine performance work to make DecentDB a class-leading embedded database without sacrificing durability correctness.

## 1. Objective

DecentDB should not only occasionally beat competitors on selected runs, but do so **consistently** across repeated benchmark reruns under the fairness contract. The main engine objective is to widen margin on:

- durable single-row auto-commit latency (`commit_p95_ms`)
- prepared single-row insert throughput (`insert_rows_per_sec`)
- prepared point-read latency (`read_p95_ms`)

while preserving strengths in:

- small disk footprint
- join latency
- ACID durability semantics (`WAL` + sync behavior)

## 2. Current Position (Context, Not a Contract)

Recent runs show DecentDB can meet or beat SQLite in some categories, but run-to-run variance remains non-trivial. This indicates the current architecture is near a local ceiling in specific paths, especially durable commit and write persistence internals.

The key strategic point: **micro-optimizations help, but architectural changes are the long pole** for stable leadership.

## 3. Non-Negotiable Constraints

All slices below assume:

- no weakening of durability (`fsync` semantics stay intact)
- no benchmark manipulation
- no broad `unsafe` rewrites without ADR-level justification
- no large dependency additions without ADR + approval
- C-ABI and binding compatibility remain intact

## 4. Prioritization Model

Slices are ordered by estimated impact on production-like benchmark outcomes:

- **Impact on commit/insert/read** (weighted highest)
- **Breadth** (how many hot paths benefit)
- **Durability risk** (lower risk gets higher execution priority)
- **Implementation leverage** (enables future slices)

Estimated impact levels:

- **Very High**: likely double-digit % movement in one or more key metrics
- **High**: likely meaningful and repeatable single-digit to low double-digit %
- **Medium**: useful but secondary/compounding gains
- **Low**: niche or long-tail

## 5. Slices Ordered by Estimated Impact

## Slice 1 (Very High): Page-Resident Table Persistence (Phase1 Table B+Tree Ownership)

### Why this is #1

Current persistence still pays structural costs from manifest-managed table payload blobs. Even with append fast paths, write and read behavior is fundamentally constrained by row-blob mechanics. Moving table storage to page-resident table B+Trees is the largest lever for durable write latency, insert throughput, and read latency.

### Expected impact

- **commit_p95_ms:** large and more stable reduction
- **insert_rows_per_sec:** substantial increase due to direct page updates
- **read_p95_ms:** better point-read locality and lower decode overhead

### Core work

- Promote `crates/decentdb/src/btree/table.rs` from optional/runtime structures to persistence owner.
- Store row payloads in page-backed table B+Tree nodes (overflow only for large varlen values).
- Make manifest track schema/catalog metadata, not full table payload serialization.
- Replace table payload rewrite/append paths with page-level mutations.

### Main files/modules

- `crates/decentdb/src/btree/table.rs`
- `crates/decentdb/src/exec/mod.rs` (persistence flow)
- `crates/decentdb/src/db.rs` (commit staging mechanics)
- `crates/decentdb/src/record/*` (row layout/overflow boundaries)

### Risks

- correctness risk during migration of existing persisted format
- crash-recovery invariants must be re-proven for page-resident table state

### Validation gates

- crash/failpoint tests expanded for table B+Tree mutations
- no regressions in storage-phase durability tests
- repeated benchmark runs demonstrate stable advantage, not isolated wins

---

## Slice 2 (Very High): Commit Pipeline Lock and Staging Simplification

### Why this is #2

Autocommit single-row inserts are sensitive to lock churn and staging overhead around each commit. Any unnecessary mutex contention, map operations, or staging copies directly inflate `commit_p95_ms`.

### Expected impact

- **commit_p95_ms:** meaningful reduction and lower jitter
- secondary insert throughput improvement

### Core work

- Introduce a writer-held commit context to avoid repeated lock/unlock around staged page writes.
- Replace general-purpose structures in commit staging path where deterministic order can be preserved with lower overhead.
- Continue reducing hot-path allocations and copies in WAL frame preparation and transaction staging.
- Keep failpoint semantics stable for test determinism.

### Main files/modules

- `crates/decentdb/src/db.rs`
- `crates/decentdb/src/wal/writer.rs`
- `crates/decentdb/src/wal/mod.rs`

### Risks

- deadlock/order risk if lock graph changes are not explicitly reviewed
- failpoint behavior drift if write sequencing changes accidentally

### Validation gates

- `storage_phase1_tests` must remain clean
- `vfs_classification_tests` and faulty VFS tests remain deterministic
- repeated p95 commit runs show reduced spread

---

## Slice 3 (High): Catalog/Manifest Delta Persistence (Beyond Template Patching)

### Why this is #3

Template patching reduces encode overhead, but manifest persistence still rewrites a monolithic blob. A delta format (or segmented metadata pages) avoids paying full catalog-payload rewrite cost on routine DML.

### Expected impact

- **commit_p95_ms:** moderate reduction, especially for schema-rich databases
- improved scaling with many tables/indexes

### Core work

- Replace full manifest rewrite with small, deterministic metadata updates.
- Store per-table persisted state in fixed-page metadata records (or manifest segments).
- Keep compatibility decoder to support old format versions.

### Main files/modules

- `crates/decentdb/src/exec/mod.rs` (manifest encode/decode/persist)
- `crates/decentdb/src/storage/*` (metadata page design support)
- `design/adr/*` (format change ADR required)

### Risks

- on-disk format evolution complexity
- migration and recovery complexity

### Validation gates

- backward compatibility tests with old files
- corruption detection still strong (checksums/versioning)

---

## Slice 4 (High): Point-Read Path De-Materialization

### Why this is #4

Point reads are already fast, but class-leading performance requires minimizing row/value materialization for trivial projections and primary-key lookups.

### Expected impact

- **read_p95_ms:** consistent micro-latency reduction
- lower allocator pressure on mixed read/write workloads

### Core work

- Fast path for single-row/single-column projections without full row cloning.
- Delay `Value` heap allocations until required by query shape.
- Reuse decode scratch buffers across repeated prepared executions.
- Specialize common `SELECT col FROM t WHERE pk = $1` and similar patterns.

### Main files/modules

- `crates/decentdb/src/exec/mod.rs`
- `crates/decentdb/src/exec/row.rs`
- `crates/decentdb/src/record/row.rs`

### Risks

- subtle semantic regressions in expression evaluation or NULL behavior

### Validation gates

- relational phase tests for expression correctness remain green
- benchmark read p95 improves without join regressions

---

## Slice 5 (High): Insert Executor Hot-Path Specialization

### Why this is #5

Prepared insert is already optimized, but additional gains remain in validation/index-update path selection and branch predictability.

### Expected impact

- **insert_rows_per_sec:** moderate uplift
- slight **commit_p95_ms** improvement from less pre-commit work

### Core work

- Expand prepared insert specialization for common table/index shapes.
- Eliminate avoidable generic validation dispatch on known-safe plans.
- Reduce transient container creation per row (especially in index update pipeline).
- Keep stale-index rebuild checks off the autocommit critical path when state epoch proves unchanged.

### Main files/modules

- `crates/decentdb/src/exec/dml.rs`
- `crates/decentdb/src/exec/mod.rs`
- `crates/decentdb/src/db.rs`

### Risks

- correctness regressions around constraints/conflict behavior

### Validation gates

- constraint and ON CONFLICT tests remain unchanged and passing
- prepared statement invalidation behavior remains correct after schema change

---

## Slice 6 (Medium): WAL Recovery/Index Maintenance Efficiency

### Why this is #6

Steady-state commit path is primary, but reducing WAL index/recovery overhead improves reopen time, checkpoint behavior, and long-run consistency.

### Expected impact

- indirect improvements to long-running benchmark stability
- improved operational behavior under checkpoint-heavy scenarios

### Core work

- Tighten WAL index pruning and version retention policy when readers are absent.
- Reduce data cloning in recovery/index population paths.
- Keep frame format simple while avoiding redundant metadata updates.

### Main files/modules

- `crates/decentdb/src/wal/index.rs`
- `crates/decentdb/src/wal/recovery.rs`
- `crates/decentdb/src/wal/checkpoint.rs`

### Risks

- snapshot visibility bugs if pruning boundaries are wrong

### Validation gates

- shared WAL cross-connection tests remain green
- checkpoint + reader-held snapshot tests remain green

---

## Slice 7 (Medium): Page Cache and Snapshot Read Path Tuning

### Why this is #7

With faster persistence paths, cache behavior and snapshot page visibility checks become a larger relative cost for read latency and mixed workloads.

### Expected impact

- moderate read and mixed workload gains
- smoother tail latency under sustained load

### Core work

- tune page cache lookup/update path for hot pages
- reduce repeated WAL-vs-pager branching overhead on common snapshot cases
- evaluate compact metadata in cache keys/state structures

### Main files/modules

- `crates/decentdb/src/storage/cache.rs`
- `crates/decentdb/src/storage/pager.rs`
- `crates/decentdb/src/db.rs` (read path)

### Risks

- cache invalidation/coherency bugs are high-severity

### Validation gates

- pager/cache determinism tests remain intact
- no stale-read regressions across checkpoints

---

## Slice 8 (Medium): Allocator and Buffer Reuse Program

### Why this is #8

Micro-allocation churn still accumulates in hot loops. Systematic buffer reuse lowers allocator pressure and jitter.

### Expected impact

- small-to-moderate improvement across all key metrics
- especially useful for p95 stability

### Core work

- audit hot allocations in commit/read/insert loops
- promote scratch buffers to reusable per-handle/per-writer state where safe
- prefer in-place patching over rebuild + clone patterns

### Main files/modules

- `crates/decentdb/src/db.rs`
- `crates/decentdb/src/exec/mod.rs`
- `crates/decentdb/src/wal/writer.rs`
- `crates/decentdb/src/record/*`

### Risks

- accidental state leakage between operations if reuse boundaries are wrong

### Validation gates

- correctness tests + randomized sequences + fuzz harness spot checks

---

## Slice 9 (Medium): Benchmark Stability and Diagnostics Upgrade

### Why this is #9

Class-leading status requires confidence and reproducibility. Better observability makes regressions obvious and accelerates optimization iterations.

### Expected impact

- does not directly speed engine, but increases optimization quality and confidence

### Core work

- benchmark rerun automation with summary statistics (median/p95/min/max/stddev).
- preserve nanosecond precision output and expose more internal counters in debug/bench mode:
  - staged pages per commit
  - manifest bytes written
  - table payload bytes appended/rewritten
  - WAL bytes/commit
- provide benchmark comparison report script for PR workflows.

### Main files/modules

- `crates/decentdb/benches/embedded_compare.rs`
- `data/bench_summary.json` generation pipeline
- `scripts/` reporting helpers

### Risks

- benchmark harness drift if not tied to fairness contract definitions

### Validation gates

- exact workload metadata still recorded
- comparability checks fail fast on incompatible benchmark settings

---

## Slice 10 (Lower Immediate Impact, Long-Term): Advanced Data Layout and CPU-Level Optimizations

### Why this is lower immediate impact

SIMD/scalar tuning and byte-level micro-optimizations matter, but should follow the larger architectural and persistence wins above.

### Candidate ideas

- branchless decode helpers for common fixed-width columns
- vectorized predicate evaluation for narrow read patterns
- compile-time layout tuning for page/node structs
- selective `#[inline]`/`#[cold]` hot-cold path separation informed by measurement

### Risks

- complexity increase without proportional wins if done too early

### Validation gates

- benchmark and code-size tradeoff review for each micro-optimization

## 6. Suggested Execution Sequence

1. complete Slice 1 architecture foundation milestones (incremental rollout)
2. parallelize Slice 2 and Slice 5 on current architecture where low-risk
3. deliver Slice 3 format evolution with migration tests
4. fold in Slice 4 read de-materialization once persistent table layout is stable
5. continuously apply Slice 8 and Slice 9 for compounding gains and visibility

## 7. Performance Governance

For each slice PR:

- state target metric(s) and hypothesis
- include before/after numbers from comparable command runs
- report if gain is single-run only or repeatable across reruns
- explicitly mention any tradeoff in footprint, complexity, or maintainability
- include rollback plan when changing persistence formats

## 8. Definition of Success

DecentDB is “class-leading” when all are true:

- durable commit p95 leads SQLite with repeatable margin across reruns
- insert throughput leads or is statistically tied with SQLite while preserving durability
- read p95 remains leading with no join/file-size regression
- crash/failpoint/correctness suites stay clean
- improvements are sustained over time, not one-off benchmark spikes

