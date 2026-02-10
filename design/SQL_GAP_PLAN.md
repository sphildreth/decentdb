# SQL Gap Plan: Close Insert Throughput Gap vs SQLite
**Date:** 2026-02-09
**Status:** Planning document (no implementation)

## Objective

Reduce the `insert_rows_per_sec` gap between DecentDB and SQLite in the embedded comparison benchmarks while preserving:

- Durable ACID writes by default (fsync-on-commit).
- Snapshot isolation and reader correctness.
- No statistically meaningful regressions in other tracked metrics (`commit_p95_ms`, `read_p95_ms`, `join_p95_ms`).

Baseline (from `benchmarks/embedded_compare/data/bench_summary.json`, run_id `20260210_003117`, durability `safe`, real disk `--data-dir=.bench_data`):

| Metric | DecentDB | SQLite | Gap |
|---|---:|---:|---:|
| `insert_rows_per_sec` | 286,722 | 1,129,688 | ~3.94x (SQLite faster) |
| `commit_p95_ms` | 3.027207 | 3.017667 | ~parity |
| `read_p95_ms` | 0.001222 | 0.001993 | DecentDB faster |
| `join_p95_ms` | 0.409328 | 0.440357 | DecentDB slightly faster |

The insert throughput microbenchmark is `runDecentDBInsert` / `runSqliteInsert` in `benchmarks/embedded_compare/run_benchmarks.nim`.

## Fairness Contract (Non-Negotiable)

We do **not** “game” or “cheat” the benchmarks. Improvements must come from DecentDB internals.

- No DecentDB-only benchmark loop optimizations (e.g., reusing buffers/params/latency arrays only for DecentDB).
- Any benchmark harness changes must apply equally to all engines or be clearly documented as measuring a different layer.
- Keep durability comparable (`--durability=safe`), and keep the benchmark disk-backed (`--data-dir=.bench_data`).
- Target end state: DecentDB **outperforms SQLite fairly and squarely** on the same workload.

## Constraints (Non-Negotiables)

1. **Durability-by-default stays**: synchronous `fsync`/`fdatasync` on every commit remains the default (see `design/PRD.md`, `design/SPEC.md`).
2. **No semantics weakening for benchmarks**: no async commit, no “unsafe” mode as default.
3. **Reader correctness**: snapshot reads must remain correct under concurrent readers (single writer + many readers model).
4. **Format changes require an ADR**: any WAL/db format change must follow ADR workflow, bump versions, and add compatibility tests (repo policy in `AGENTS.md`; see also `design/adr/README.md`).
5. **Group commit remains deferred**: ADR `design/adr/0037-group-commit-wal-batching.md` defers group commit to post-1.0.

## What The Reviews Agree On (Likely Contributors)

These items are repeatedly identified across `design/SQL_GAP_*.md` reviews, and most are visible in current code. Some are commit-latency specific; for inserts we expect the dominant costs to sit in the engine → storage → pager → B-tree insert path.

1. **Benchmark timing asymmetry (fix first)**
  - Any timing boundary differences between engines can dominate at microsecond scales.
  - Do not attribute wins until we have confirmed that per-iteration work (parameter creation/binding, statement execution, commit) is comparable.

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

### Phase 0 (P0): Measurement + “No Cheating” Guardrails

Goal: keep the comparison apples-to-apples while we iterate quickly on real engine improvements.

**Tasks**

- [ ] **Keep insert benchmark workload symmetric across engines**
  - If we ever adjust iteration structure, it must apply to all engines or be explicitly a different benchmark.
  - Avoid DecentDB-only reuse tricks; those are not allowed as wins.

- [ ] **Use breakdown instrumentation to identify the next bottleneck**
  - Run with `-d:bench_breakdown` periodically to see where time is going (engine vs storage vs pager vs B-tree).
  - Use aggregated counters (not per-iteration logging) to avoid perturbing timings.

**Exit criteria**

- We can name the top contributors to insert time with numbers.
- We have a stable baseline run_id captured in this doc.

### Phase 1 (P0/P1): Low-Risk Insert Throughput Wins (No Format Changes)

Goal: reduce per-insert CPU/allocations on the hot path without altering WAL/db formats or weakening durability semantics.

**Likely high ROI areas (based on current insert baseline)**

- B-tree insert path: avoid decoding/encoding and minimize page-touching on sequential workloads.
- Pager/write path: ensure copy-on-write upgrades and cache entry mutations are minimal.
- Engine insert execution: minimize per-row allocations when there is no `RETURNING`.

**P0: VFS syscall reduction (guardrail for later)**

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

**P1: Reduce generic statement overhead in insert hot path**

- [ ] **Prepared-statement “write profile”**
  - For prepared write statements (INSERT/UPDATE/DELETE), precompute per-table flags:
    - does the table have triggers for the event?
    - does it have CHECK/UNIQUE constraints?
    - does it have FKs or act as a parent for any FKs?
    - do any secondary indexes need maintenance for the updated columns?
  - If the profile indicates “simple insert”, take a minimal fast path that avoids repeated catalog scans and trigger discovery.

- [ ] **Reverse FK lookup cache**
  - Replace the catalog scan in `src/engine.nim` `referencingChildren` with a reverse map in catalog metadata:
    - key: `(parentTable, parentColumn)`
    - value: list of referencing child FK entries
  - Keep the map maintained on DDL changes (CREATE/DROP TABLE/INDEX as applicable).
  - Ensure correctness for FK enforcement (RESTRICT/NO ACTION) remains unchanged.

- [ ] **Trigger “fast gate”**
  - Add a cheap table-level `hasTriggersForMask`/`hasAnyTriggers` gate so common tables with no triggers avoid scanning trigger metadata.

**Exit criteria**

- `insert_rows_per_sec` improves measurably (median-of-5 runs) with no regressions in other tracked metrics beyond noise thresholds.

### Phase 2 (P1/P2): Storage/Pager/B-tree Structural Wins (No On-Disk Format Change)

Goal: reduce page mutations and data movement during insert-heavy workloads while preserving snapshot semantics.

**Tasks**

- [ ] **Minimize root/metadata writes on sequential inserts**
  - For the common case “append to rightmost leaf,” avoid touching internal nodes unless the tree height changes.
  - Ensure any cached-rightmost-leaf optimization remains correct under splits and under concurrent readers.

- [ ] **Avoid redundant page header writes**
  - Where safe, update only the necessary fields and avoid rewriting large page buffers when changes are localized.

- [ ] **Validate lock ordering and pin/unpin correctness**
  - Performance wins must not introduce deadlocks, hangs, or reader-writer correctness regressions.

### Phase 3 (ADR Required): Bigger Swings (Only If Needed)

These are high-impact but higher-risk items that may require format or semantic changes. Do not implement without following ADR workflow.

**Candidate A: WAL header rewrite removal from commit**

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

- `insert_rows_per_sec` approaches SQLite and keeps improving beyond parity (target: exceed SQLite fairly) without any durability/correctness regression under crash-injection and differential tests.

## Validation and Gating

For each change:

1. Run embedded compare benchmarks repeatedly (>=5 runs) on the same host/disk and compare median metrics.
2. Reject changes that cause statistically meaningful regressions in non-commit metrics.
3. Run:
   - Nim unit tests for the touched modules.
   - Crash-injection tests for any WAL/commit/recovery-path changes.
   - Differential tests as applicable (per `design/TESTING_STRATEGY.md`).

Suggested guardrails (per-change, median-of-5 runs):

- `insert_rows_per_sec`: must improve.
- `read_p95_ms`: must not worsen beyond noise (target <= +2%).
- `join_p95_ms`: must not worsen beyond noise (target <= +2%).
- `commit_p95_ms`: must not worsen beyond noise (target <= +2%).

## Explicitly Deferred / Out Of Scope For This Plan

- Group commit / WAL batching policies as default (post-1.0 per `design/adr/0037-group-commit-wal-batching.md`).
- Weakening default durability (async commits).
- Multi-process concurrency / shared-memory locking changes.

## Source Reviews Consolidated

- `design/SQL_GAP_GPT53CODEX.md`
- `design/SQL_GAP_MM21.md`
- `design/SQL_GAP_G3PRO.md`
- `design/SQL_GAP_K25.md`

