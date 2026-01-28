# DecentDb Testing Strategy (Python-first + Engine Unit Tests)
**Date:** 2026-01-27  
**Status:** Draft (v0.1)

This document expands the testing requirements from PRD/SPEC into an actionable plan.

## 1. Guiding principles
- **Correctness before features**: no feature merges without tests.
- **Determinism**: every failing test must be reproducible (seeded randomness, captured logs).
- **Layered defense**: unit + property + crash + differential testing.
- **Faults are features**: we intentionally simulate partial writes, dropped fsync, and crashes.

## 2. Test pyramid
### 2.1 Fast unit tests (Nim)
Runs on every PR.
- pager:
  - page read/write roundtrip
  - cache eviction correctness
  - freelist allocate/free
- WAL:
  - frame encode/decode + checksum
  - commit marker semantics
- btree:
  - insert/search invariants
  - cursor ordering
  - split cases (random inserts)
- record:
  - type encode/decode
  - boundary values
- search:
  - trigram generation canonicalization
  - postings encode/decode
  - intersection correctness

### 2.2 Property tests
- Model-based checks: compare engine behavior against a simplified in-memory model for a subset (optional but powerful).
- Random operation sequences:
  - create table, insert/update/delete, index create, select with filters
- Invariants:
  - “select via index == select via scan”
  - “btree keys strictly ordered”
  - “FK constraints never violated”
- Seeds:
  - record seed on failure
  - rerun with same seed in CI- Long-running transactions:
  - Test snapshot isolation under concurrent writes
  - Verify readers see consistent snapshots
  - Test transaction rollback on errors
### 2.3 Crash-injection tests (Python)
Core suite for ACID durability.
- Define failpoints in engine (or FaultyVFS) at:
  - wal write frame
  - wal write commit record
  - wal fsync
  - db file page write during checkpoint
  - db file fsync (if applicable)
- For each failpoint:
  1) run a scripted transaction scenario
  2) crash at failpoint (kill process)
  3) reopen
  4) assert database invariants and expected visibility

### 2.4 Differential tests vs PostgreSQL (Python)
For supported subset only.
- Load identical data into PostgreSQL and DecentDb
- Execute deterministic SQL
- Compare:
  - row counts
  - ordered results
  - NULL handling
  - string matching for LIKE patterns (define exact semantics)

### 2.5 Resource leak tests
- File handle leaks:
  - Track all file opens/closes
  - Assert all handles closed after each test
  - Use OS-level tools (lsof, handle.exe) in CI
- Memory leaks:
  - Use Nim's memory tracking (--gc:arc --gc:stats)
  - Run tests with leak detection enabled
  - Assert no memory growth across repeated operations
- Lock deadlocks:
  - Inject random delays in lock acquisition
  - Detect deadlocks via timeout
  - Verify deadlock recovery (transaction abort)

## 3. Faulty VFS design (must-have)
Implement a test-only VFS layer in Nim that can be toggled on.
Capabilities:
- partial write injection: write only first N bytes
- error injection on read/write/fsync
- failpoint triggers by label or counter
- optional: simulated “fsync lies” (returns success but does not flush)

All failpoint decisions must be logged so Python can reproduce.

## 4. Harness structure (Python)
Suggested layout:
- `tests/harness/runner.py` — runs engine CLI or embedded test binary
- `tests/harness/scenarios/` — declarative scenarios
- `tests/harness/postgres_ref/` — utilities to run same scenario in PostgreSQL
- `tests/harness/datasets/` — generators for artists/albums/tracks shapes

Scenario DSL (example):
- create schema
- insert N entities
- run query
- assert rows/ordering
- inject crash at failpoint X
- reopen and reassert

## 5. Coverage and CI
- Coverage target: set a baseline early and raise gradually.
- CI:
  - PR: unit + small property suite on all OSes
  - nightly: extended crash suite + long property runs + fuzz (if adopted)

## 6. Performance regression testing
### 6.1 Benchmark suite
Run microbenchmarks on every PR:
- PK point lookup (1000 iterations)
- FK join expansion (artist→albums→tracks, 100 iterations)
- Substring search with trigram index (100 iterations)
- Bulk load (10k records)

### 6.2 Regression detection
- Compare P50/P95 latencies against baseline (last successful main branch run)
- Thresholds:
  - Point lookup: fail if P95 increases > 20%
  - FK join: fail if P95 increases > 20%
  - Substring search: fail if P95 increases > 20%
  - Bulk load: fail if time increases > 15%
- If regression detected:
  - Mark PR as failing
  - Provide before/after metrics
  - Require investigation before merge

### 6.3 Benchmark storage
- Store benchmark results in CI artifacts
- Track trends over time (graphs in CI dashboard)
- Allow manual baseline updates with justification

## 7. "Definition of Done" for any PR
- New functionality includes unit tests
- Crash-sensitive changes include crash tests or justify why not
- No flaky tests; if randomness exists, it is seeded and logged

## 8. Error handling tests
### 8.1 Error propagation
- Test error codes at each layer (SQL, planner, exec, storage)
- Verify error messages include sufficient context
- Test transaction rollback on errors:
  - Statement-level errors: verify only current statement rolled back
  - Transaction-level errors: verify entire transaction rolled back

### 8.2 Constraint violation tests
- FK violations: test RESTRICT/NO ACTION on delete/update
- Unique constraint violations: test duplicate key insertion
- NOT NULL violations: test NULL insertion into non-nullable columns
- Verify error messages include table/column and violating value

### 8.3 Corruption recovery tests
- Inject checksum mismatches in WAL frames
- Inject checksum mismatches in main DB pages
- Verify database marked as read-only
- Test recovery from backup (if available)
