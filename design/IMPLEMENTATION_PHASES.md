# DecentDb Implementation Phases
**Date:** 2026-01-28
**Status:** Living document

This document turns `design/PRD.md`, `design/SPEC.md`, and `design/TESTING_STRATEGY.md` into a phased, low-decision implementation plan for coding agents.

North Star (from `AGENTS.md`):
- Priority #1: durable ACID writes
- Priority #2: fast reads
- MVP: single process, one writer, many concurrent reader threads

## How To Use This Document
- Pick the *earliest* phase with an unchecked box in the phase map.
- Within that phase, pick the *earliest* unchecked item in the phase checklist.
- Implement only what that item says (no extra features).
- Add/extend tests in the same PR.

## Phase Map (Update This As Work Lands)
Instructions to update the phase map:
- When a phase is complete, change `- [ ]` to `- [x]`.
- In the phase header, set `Status:` to `Done` and add `Completed:` with the date + PR/commit.
- If a phase’s scope changes, update *both* the phase checklist and the acceptance tests for that phase.
- Do not delete completed items; only append.

- [x] Phase 0: Foundations (project + test harness + VFS)
- [ ] Phase 1: DB File + Pager + Page Cache (read/write pages)
- [ ] Phase 2: Records + Overflow Pages + B+Tree Read Path
- [ ] Phase 3: WAL + Transactions + Recovery + Snapshot Reads
- [ ] Phase 4: B+Tree Write Path + Catalog + SQL/Exec MVP
- [ ] Phase 5: Constraints + Foreign Keys + Trigram Search (v1)
- [ ] Phase 6: Checkpointing + Bulk Load + Performance + Hardening

---

## Phase 0: Foundations (project + test harness + VFS)
Status: Done
Completed: 2026-01-28 (local change)

Goal: Establish deterministic tests, failure injection, and a stable set of core abstractions.

Deliverables:
- Nim module skeletons (see `design/SPEC.md` §2) with minimal compileable stubs.
- Python harness skeleton (see `design/TESTING_STRATEGY.md` §4).
- A test-only Faulty VFS with failpoints and partial-write injection.

Checklist:
- [x] Create/confirm module layout: `vfs/`, `pager/`, `wal/`, `btree/`, `record/`, `catalog/`, `sql/`, `planner/`, `exec/`, `search/`.
- [x] Implement `vfs/` interface: open/read/write/fsync/close + intra-process locking.
- [x] Implement Faulty VFS hooks (test-only): partial writes, injected errors, dropped fsync, labeled failpoints; log all fault decisions for replay.
- [x] Establish error type conventions and codes (see `design/SPEC.md` §13): `ERR_IO`, `ERR_CORRUPTION`, `ERR_CONSTRAINT`, `ERR_TRANSACTION`, `ERR_SQL`, `ERR_INTERNAL`.
- [x] Add minimal engine entrypoint used by tests (CLI or test binary): open DB, exec SQL, close.
- [x] Create Python harness layout: `tests/harness/runner.py`, `tests/harness/scenarios/`, `tests/harness/postgres_ref/`, `tests/harness/datasets/`.
- [x] Add seed logging + deterministic rerun plumbing for property/fuzz style tests.

Acceptance tests (must pass before Phase 1 starts):
- Unit: Faulty VFS exercises (partial write, dropped fsync, failpoint triggers) are deterministic and replayable.
- Harness: a trivial “open/close DB” scenario runs from Python and returns structured results.

Non-goals:
- No persistent file formats yet.
- No SQL semantics beyond a stubbed command path.

---

## Phase 1: DB File + Pager + Page Cache (read/write pages)
Status: Not started
Completed:

Goal: Create/open a database file, read/write fixed-size pages safely, and manage a small page cache.

Deliverables:
- DB header on page 1 as specified (see `design/SPEC.md` §3.2).
- Pager that reads/writes pages by page id.
- Page cache with pin/unpin, dirty tracking, and simple eviction.
- Freelist allocation/freeing.

Checklist:
- [ ] Implement DB header layout (128 bytes) + CRC-32C checksum; validate on open.
- [ ] Implement DB create/open rules: magic, format version, page size, schema cookie.
- [ ] Implement pager read/write of pages (default 4096 bytes) with bounds checks.
- [ ] Implement page cache: pin/unpin, per-page latch, global eviction lock, simple clock/LRU.
- [ ] Implement freelist (MVP may be a single chain): allocate/free pages; update header freelist pointers.
- [ ] Add deterministic page-level tests: roundtrip reads/writes, eviction correctness, freelist allocate/free.

Acceptance tests:
- Unit: pager page roundtrip, cache eviction correctness, freelist allocate/free (see `design/TESTING_STRATEGY.md` §2.1).
- Corruption: header checksum mismatch fails open with `ERR_CORRUPTION`.

Non-goals:
- No B+Tree yet.
- No WAL yet.

---

## Phase 2: Records + Overflow Pages + B+Tree Read Path
Status: Not started
Completed:

Goal: Encode/decode rows and support reading tables/indexes via B+Tree traversal.

Deliverables:
- Record format (typed fields + varint lengths) (see `design/SPEC.md` §2.1 record/).
- Mandatory overflow pages for large TEXT/BLOB (see `design/SPEC.md` §2.1 and `design/TESTING_STRATEGY.md` §2.1).
- B+Tree read traversal + cursor iteration for table and secondary index.

Checklist:
- [ ] Implement record encoding/decoding for MVP types: NULL, INT64, BOOL, FLOAT64, TEXT (UTF-8), BLOB.
- [ ] Implement overflow page chain read/write and pointer encoding in records.
- [ ] Define B+Tree page layouts for internal/leaf pages (do not change without ADR; changes are persistent format).
- [ ] Implement B+Tree lookup and in-order cursor iteration (table + index).
- [ ] Add unit tests: type boundaries, overflow chain roundtrip, btree search correctness, cursor ordering, split-friendly read invariants (even before writes exist).

Acceptance tests:
- Unit: record encode/decode + overflow chain tests.
- Unit: B+Tree read path can traverse a constructed tree and iterate in key order.

Non-goals:
- No B+Tree writes in this phase.
- No SQL execution pipeline yet.

---

## Phase 3: WAL + Transactions + Recovery + Snapshot Reads
Status: Not started
Completed:

Goal: Durable commits, crash recovery, and snapshot isolation for multiple concurrent readers.

Deliverables:
- WAL frame format (PAGE/COMMIT/CHECKPOINT) with checksums and monotonic LSN (see `design/SPEC.md` §4.1).
- Snapshot reads using `snapshot_lsn` captured at transaction start (see `design/SPEC.md` §4.2).
- Recovery on open: scan WAL from last checkpoint, ignore torn frames, apply up to last commit boundary (see `design/SPEC.md` §4.5).
- Active reader tracking (snapshot LSNs) to support safe truncation later (see `design/SPEC.md` §4.3).

Checklist:
- [ ] Implement WAL append of frames with checksum validation and torn-write detection.
- [ ] Implement `wal_end_lsn` as `AtomicU64` updated only after frame is fully written and indexed.
- [ ] Implement in-memory `walIndex` overlay for reads: page_id -> latest frame at/before snapshot.
- [ ] Implement transactions: BEGIN/COMMIT/ROLLBACK (single writer), snapshot reads for readers.
- [ ] Implement crash recovery: scan/validate, build walIndex view, stop at last COMMIT boundary.
- [ ] Add crash-injection failpoints around WAL writes/commit/fsync (see `design/TESTING_STRATEGY.md` §2.3).

Acceptance tests:
- Crash-injection: committed visible, uncommitted not visible, no corruption across all WAL failpoints.
- Torn write: partial WAL frame write is ignored on recovery.
- Concurrency: multiple readers observe stable snapshots while writer commits.

Non-goals:
- No checkpointing/truncation automation yet (that is Phase 6 hardening).
- No SQL planner/executor semantics beyond what’s needed to exercise transactions in tests.

---

## Phase 4: B+Tree Write Path + Catalog + SQL/Exec MVP
Status: Not started
Completed:

Goal: Make the database useful: DDL/DML, table/index metadata, and a minimal query engine.

Deliverables:
- B+Tree insert/update/delete with node splits (see `design/SPEC.md` §17.1).
- Catalog system tables storing schema metadata + schema cookie increments (see `design/SPEC.md` §2.1 catalog/ and §15).
- SQL subset MVP: CREATE/DROP TABLE/INDEX, SELECT/INSERT/UPDATE/DELETE, ORDER BY, LIMIT/OFFSET, parameters `$1..$n`.
- Volcano execution engine operators (see `design/SPEC.md` §2.1 exec/).

Checklist (implement in order):
- [ ] B+Tree write primitives: insert + split (merge/rebalance is post-MVP).
- [ ] Row storage: table rows addressed by rowid/PK; secondary index entries point to rowid.
- [ ] Catalog: create system tables; store table/column/index metadata; maintain schema cookie.
- [ ] SQL parsing decision:
  - Default: libpg_query FFI (see `design/SPEC.md` §6.1). If adding a new dependency, create/confirm an ADR.
- [ ] Binder: name resolution (tables/columns), type checking for MVP types.
- [ ] Planner (rule-based): TableScan vs IndexSeek, Filter, Project; NestedLoopJoin scaffolding.
- [ ] Exec operators: TableScan, IndexSeek, Filter, Project, Limit/Offset.
- [ ] Add Sort operator with spill-to-disk (external merge sort) if needed for ORDER BY (see `design/SPEC.md` §2.1 and §11).
- [ ] Implement JOINs (INNER/LEFT) on equality predicates with NestedLoopJoin + index on inner side.
- [ ] Implement aggregates: COUNT/SUM/AVG/MIN/MAX with GROUP BY and HAVING.
- [ ] Implement statement-level rollback semantics on errors (see `design/SPEC.md` §13.2).

Acceptance tests:
- Unit: btree insert/search invariants, cursor ordering, split cases (see `design/TESTING_STRATEGY.md` §2.1).
- Black-box SQL (Python): basic DDL/DML works; parameters `$1..` work; ORDER BY/LIMIT/OFFSET correct.
- Differential (Python): deterministic queries in the supported subset match PostgreSQL results.
- Resource leaks: verify temp sort files deleted after tests that spill (see `design/TESTING_STRATEGY.md` §2.5).

Non-goals:
- No trigram index yet.
- No checkpointing automation yet.

---

## Phase 5: Constraints + Foreign Keys + Trigram Search (v1)
Status: Not started
Completed:

Goal: Enforce relational integrity and accelerate `LIKE '%pattern%'` for selected TEXT columns.

Deliverables:
- Constraint enforcement: NOT NULL, UNIQUE/PK (via indexes).
- Foreign keys enforced at statement time with RESTRICT/NO ACTION (see `design/SPEC.md` §7.2).
- Trigram inverted index: postings B+Tree, delta+varint compression, query-time intersection + verification, and broad-pattern guardrails (see `design/SPEC.md` §8).

Checklist:
- [ ] Implement NOT NULL/UNIQUE checks and error messages with context.
- [ ] Implement FK enforcement at statement time for INSERT/UPDATE/DELETE.
- [ ] Enforce/auto-create supporting indexes for FK checks (parent key index required; create child index if missing; naming `fk_<table>_<column>_idx`).
- [ ] Implement trigram canonicalization and generation (uppercase normalization per SPEC).
- [ ] Implement postings encode/decode: sorted rowids, delta encoding, varints.
- [ ] Implement postings storage: B+Tree keyed by trigram -> postings blob.
- [ ] Implement query evaluation: rarest-first intersection, candidate cap/thresholding, final substring verification.
- [ ] Implement guardrails for broad patterns and for patterns shorter than 3 chars.

Acceptance tests:
- Unit: trigram generation + postings encode/decode + intersection correctness (see `design/TESTING_STRATEGY.md` §2.1).
- Property: “index results == scan results” for LIKE patterns over generated datasets.
- Constraint tests: FK RESTRICT/NO ACTION correctness and error context.
- Differential: LIKE semantics for supported patterns match PostgreSQL for deterministic cases (document any intentional divergence).

Non-goals:
- No ranking/full-text search.
- No CASCADE/SET NULL (post-MVP unless explicitly added).

---

## Phase 6: Checkpointing + Bulk Load + Performance + Hardening
Status: Not started
Completed:

Goal: Keep WAL bounded, make bulk ingest fast, and harden concurrency/performance to the PRD targets.

Deliverables:
- Checkpoint protocol with reader protection and conditional WAL truncation (see `design/SPEC.md` §4.3).
- Bulk load API with deferred durability (see `design/SPEC.md` §4.4).
- Benchmarks and regression thresholds in CI (see `design/TESTING_STRATEGY.md` §6).
- Memory budgets and spill behavior enforced (see `design/SPEC.md` §11 and §14).

Checklist:
- [ ] Implement checkpoint protocol: block new writers, copy committed pages, write CHECKPOINT frame, truncate only up to `min(active_reader_snapshot_lsn)`.
- [ ] Implement reader tracking and reporting (long-running reader warnings, configurable timeouts; see WAL growth prevention notes).
- [ ] Implement configurable checkpoint triggers (timeout / size thresholds).
- [ ] Implement bulk load API with batching and configurable fsync interval; crash loses all progress (no partial commits).
- [ ] Implement benchmark suite and CI regression checks (P50/P95 thresholds per testing strategy).
- [ ] Performance passes for target workload heuristics (see `design/SPEC.md` §9) and reduce per-row allocations.
- [ ] Expand crash-injection coverage to include checkpoint write paths.

Acceptance tests:
- Crash-injection: checkpoint paths do not corrupt DB; committed state preserved.
- WAL truncation safety: active reader snapshots prevent truncation past required LSN.
- Benchmarks: establish baseline and enforce regression thresholds in CI.

Non-goals:
- Multi-process concurrency.
- Online vacuum/compaction beyond minimal hooks.

---

## Cross-Cutting Rules (Applies To Every Phase)
- Persistent-format changes (DB header, page layouts, WAL frames, postings format) require an ADR + version bump + compatibility tests.
- “Correctness before features”: do not merge feature work without tests (`design/TESTING_STRATEGY.md` §1, §7).
- Prefer incremental PRs that complete exactly one checklist item.
