# SQL Gap Plan: Close Commit Latency Gap vs SQLite
**Date:** 2026-02-09
**Status:** Planning document (no implementation)

## Objective

Reduce the `commit_p95_ms` gap between DecentDB and SQLite in the embedded comparison benchmarks while preserving:

- Durable ACID writes by default (fsync-on-commit).
- Snapshot isolation and reader correctness.
- No statistically meaningful regressions in other tracked metrics (`read_p95_ms`, `join_p95_ms`, `insert_rows_per_sec`).

Baseline (from `benchmarks/embedded_compare/data/bench_summary.json`, run_id `20260208_223353`):

| Metric | DecentDB | SQLite | Gap |
|---|---:|---:|---:|
| `commit_p95_ms` | 0.03723 | 0.009397 | ~3.96x |
| `read_p95_ms` | 0.001332 | 0.002014 | DecentDB faster |
| `join_p95_ms` | 0.393489 | 0.399029 | ~parity |
| `insert_rows_per_sec` | 151,837 | 111,136 | DecentDB faster |

The commit latency microbenchmark is `runDecentDBCommitLatency` / `runSqliteCommitLatency` in `benchmarks/embedded_compare/run_benchmarks.nim`.

## Constraints (Non-Negotiables)

1. **Durability-by-default stays**: synchronous `fsync`/`fdatasync` on every commit remains the default (see `design/PRD.md`, `design/SPEC.md`).
2. **No semantics weakening for benchmarks**: no async commit, no “unsafe” mode as default.
3. **Reader correctness**: snapshot reads must remain correct under concurrent readers (single writer + many readers model).
4. **Format changes require an ADR**: any WAL/db format change must follow ADR workflow, bump versions, and add compatibility tests (repo policy in `AGENTS.md`; see also `design/adr/README.md`).
5. **Group commit remains deferred**: ADR `design/adr/0037-group-commit-wal-batching.md` defers group commit to post-1.0.

## What The Reviews Agree On (Likely Contributors)

These items are repeatedly identified across `design/SQL_GAP_*.md` reviews, and most are visible in current code:

1. **Benchmark timing asymmetry (fix first)**
   - DecentDB includes parameter marshaling inside the timed region:
     `toBytes("value" & $i)` occurs after `t0` in `runDecentDBCommitLatency` (`benchmarks/embedded_compare/run_benchmarks.nim`).
   - SQLite constructs the string before `t0` (`runSqliteCommitLatency`).
   - This can inflate the apparent engine gap; normalize before attributing wins.

2. **WAL “double write” per commit**
   - `src/wal/wal.nim` `WalWriter.commit` appends frames, then updates the WAL header at offset 0 (`writeWalHeader`), then calls `fsync`:
     `writeWalHeader(writer.wal, uint64(newEnd))` is in the commit critical path.
   - `design/SPEC.md` documents a WAL header field `wal_end_offset` (format v8) and that recovery scans only up to it.
   - Updating the header at file start plus appending frames at the file end forces fsync to cover disjoint regions.

3. **VFS write path flushes too often**
   - `src/vfs/os_vfs.nim` currently calls `flushFile(file.file)` on every `write`/`writeStr`, and `fsync` also flushes.
   - For commit-latency microbenchmarks, redundant flushing can be meaningful at microsecond scale.

4. **Non-WAL overhead above “append+fsync”**
   - Even simple `UPDATE` runs generic engine paths: metadata lookups, trigger discovery, FK checks, index evaluation, etc.
   - Specific hotspot called out in reviews: parent FK restrict checks currently scan catalog via `referencingChildren` (`src/engine.nim`) rather than using a reverse map.

5. **WAL index maintenance/version retention**
   - WAL index stores page versions and is updated on every commit (`src/wal/wal.nim`).
   - When there are no active readers, retaining older versions is often unnecessary but can still add work and memory churn.

## Plan

### Phase 0 (P0): Measurement + Fairness Guardrails

Goal: make sure we are measuring DecentDB, not benchmark harness artifacts, and get a component breakdown so we optimize the right thing.

**Tasks**

- [ ] **Normalize commit benchmark timing boundaries**
  - Update `benchmarks/embedded_compare/run_benchmarks.nim` so DecentDB and SQLite include equivalent parameter construction/binding overhead inside or outside `t0..t1`.
  - Preferred: move `toBytes(...)` outside the timed region for DecentDB to match SQLite’s current structure, then consider a second run mode that times “full app path” for both engines.

- [ ] **Add commit latency breakdown instrumentation**
  - Add per-iteration breakdown (or aggregated counters) for the DecentDB commit benchmark:
    - parameter marshaling
    - statement execution (pre-commit)
    - WAL frame encode/write
    - WAL header update
    - fsync
    - WAL index update + publish (`walEnd.store`)
  - Keep it behind a compile-time flag (or benchmark-only code) to avoid perturbing production code.

- [ ] **Define a stable benchmark protocol**
  - Median-of-N runs (N>=5) for comparison.
  - Ensure `--data-dir` points to real disk (the runner already warns about tmpfs; see `benchmarks/embedded_compare/run_benchmarks.nim`).
  - Record run manifest details (machine, filesystem, mount, power governor) consistent with `design/COMPARISON_BENCHMARK_PLAN.md` “Fairness Contract”.

**Exit criteria**

- We can state “top 2 contributors to `commit_p95_ms`” with numbers.
- `commit_p95_ms` comparison is apples-to-apples across engines.

### Phase 1 (P0/P1): Low-Risk Wins (No Format Changes)

Goal: reduce syscall/CPU overhead without altering WAL/db formats or weakening durability semantics.

**P0: VFS syscall reduction**

- [ ] **Remove per-write `flushFile`**
  - In `src/vfs/os_vfs.nim`, remove `flushFile(file.file)` from `write`/`writeStr`.
  - Keep `flushFile` inside `fsync` (where it belongs for buffered IO).
  - Validate with crash-injection tests that rely on buffered writes (see `design/TESTING_STRATEGY.md`).

**P0: WAL buffer/mmap path sanity**

- [ ] **Verify mmap header mapping is always used when mmap write path is enabled**
  - `writeWalHeader` has an mmap-header fast path (`wal.mmapHeaderPtr != nil`).
  - Ensure the “mmap write path” (ADR `design/adr/0067-wal-mmap-write-path.md`) actually eliminates the header write syscall in the common case.
  - If header isn’t mapped today, consider mapping just the 32-byte header region independently (no format change) and validate cross-platform behavior.

- [ ] **Make frameBuffer reuse allocation-stable**
  - `src/wal/wal.nim` uses `frameBuffer.setLen(totalLen)` which can reallocate when growing.
  - Make capacity growth strategy explicit to avoid realloc churn across iterations (especially for small commits).
  - Confirm with Phase 0 breakdown that allocations are on the hot path before investing heavily.

**P1: Reduce generic statement overhead for the commit microbench**

- [ ] **Prepared-statement “write profile”**
  - For prepared `UPDATE` statements, precompute per-table flags:
    - does the table have triggers for the event?
    - does it have CHECK/UNIQUE constraints?
    - does it have FKs or act as a parent for any FKs?
    - do any secondary indexes need maintenance for the updated columns?
  - If the profile indicates “simple update”, take a minimal fast path that avoids repeated catalog scans and trigger discovery.

- [ ] **Reverse FK lookup cache**
  - Replace the catalog scan in `src/engine.nim` `referencingChildren` with a reverse map in catalog metadata:
    - key: `(parentTable, parentColumn)`
    - value: list of referencing child FK entries
  - Keep the map maintained on DDL changes (CREATE/DROP TABLE/INDEX as applicable).
  - Ensure correctness for FK enforcement (RESTRICT/NO ACTION) remains unchanged.

- [ ] **Trigger “fast gate”**
  - Add a cheap table-level `hasTriggersForMask`/`hasAnyTriggers` gate so common tables with no triggers avoid scanning trigger metadata.

**Exit criteria**

- `commit_p95_ms` improves measurably (median-of-5 runs) with no regressions in other tracked metrics beyond noise thresholds.

### Phase 2 (P1/P2): WAL Index + Commit Path Restructuring (No On-Disk Format Change)

Goal: reduce commit-time work that is currently paid on every transaction, especially for hot-updated pages and no-reader scenarios.

**Tasks**

- [ ] **WAL index pruning keyed to active readers**
  - If there are no active readers, retain only the latest version per page in the in-memory WAL index.
  - If readers exist, retain only versions needed for the oldest active snapshot (min snapshot LSN).
  - Add targeted tests for snapshot correctness and recovery behavior with long-running readers (see `design/SPEC.md` snapshot rules; `design/TESTING_STRATEGY.md` concurrency/crash testing).

- [ ] **Evaluate commit-critical-path ordering**
  - `WalWriter.commit` updates the WAL index after `fsync`. Confirm via Phase 0 breakdown whether index update is a meaningful fraction of latency.
  - If it is, evaluate safe reordering or batching that does not violate:
    - readers only seeing data after durable commit (publication via `walEnd.store(moRelease)`)
    - writer “read-your-writes” behavior

### Phase 3 (ADR Required): Remove WAL Header Rewrite From Commit

This is the highest potential impact item, but it crosses into recovery semantics and possibly format changes. It should be attempted only if Phases 1–2 leave a material gap.

**Background**

- Current WAL format (SPEC v8) uses `wal_end_offset` in the fixed header and recovery scans only up to it.
- `WalWriter.commit` writes frames then rewrites the header with the new end offset, then fsyncs.
- Reviews argue this “double write” is a key differentiator vs SQLite’s append-only WAL behavior.

**Option A: Recovery scans physical file + invariants (no checksum; likely ADR for semantics)**

- [ ] ADR: “WAL recovery ignores `wal_end_offset` for correctness; header becomes an optimization”
  - Recovery scans frames until a short read or an invariant violation, and uses commit markers to determine last committed LSN.
  - Header updates could then be:
    - removed from the commit path (append-only commit), or
    - batched (written at checkpoint or periodically) purely as an optimization.
  - Risks: accepting garbage frames in preallocated/unwritten regions if invariants are insufficient; requires extensive crash-injection coverage.

**Option B: Append-only end-of-log validation with lightweight checksums (format change)**

- [ ] ADR: “Reintroduce frame (or commit-marker) validation to eliminate per-commit header writes”
  - Use the reserved trailer field (`frame_checksum`) as a validity signal for end-of-log detection.
  - Must directly address ADR `design/adr/0064-wal-frame-checksum-removal.md` (CRC32C cost was too high):
    - consider a cheaper checksum (xxhash32/64) or checksum only over header + a small payload prefix
    - or checksum only commit frames (not page frames) if that’s sufficient to bound recovery safely
  - Requires WAL format version bump and compatibility tests (open old WAL, recover; open new WAL, recover; mixed cases if applicable).

**Exit criteria**

- `commit_p95_ms` approaches SQLite (target: <= 1.5x gap) without any durability/correctness regression under crash-injection and differential tests.

## Validation and Gating

For each change:

1. Run embedded compare benchmarks repeatedly (>=5 runs) on the same host/disk and compare median metrics.
2. Reject changes that cause statistically meaningful regressions in non-commit metrics.
3. Run:
   - Nim unit tests for the touched modules.
   - Crash-injection tests for any WAL/commit/recovery-path changes.
   - Differential tests as applicable (per `design/TESTING_STRATEGY.md`).

Suggested guardrails (per-change, median-of-5 runs):

- `commit_p95_ms`: must improve.
- `read_p95_ms`: must not worsen beyond noise (target <= +2%).
- `join_p95_ms`: must not worsen beyond noise (target <= +2%).
- `insert_rows_per_sec`: must not decrease beyond noise (target >= -2%).

## Explicitly Deferred / Out Of Scope For This Plan

- Group commit / WAL batching policies as default (post-1.0 per `design/adr/0037-group-commit-wal-batching.md`).
- Weakening default durability (async commits).
- Multi-process concurrency / shared-memory locking changes.

## Source Reviews Consolidated

- `design/SQL_GAP_GPT53CODEX.md`
- `design/SQL_GAP_MM21.md`
- `design/SQL_GAP_G3PRO.md`
- `design/SQL_GAP_K25.md`

