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
  - rerun with same seed in CI

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

## 6. “Definition of Done” for any PR
- New functionality includes unit tests
- Crash-sensitive changes include crash tests or justify why not
- No flaky tests; if randomness exists, it is seeded and logged
