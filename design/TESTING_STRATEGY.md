# DecentDB Testing Strategy (Python-first + Engine Unit Tests)

**Date:** 2026-01-27  
**Status:** Draft (v0.1)

This document expands the testing requirements from PRD/SPEC into an actionable plan.

## 1. Guiding principles

- **Correctness before features**: no feature merges without tests.
- **Determinism**: every failing test must be reproducible (seeded randomness, captured logs).
- **Layered defense**: unit + property + crash + differential testing.
- **Faults are features**: we intentionally simulate partial writes, dropped fsync, and crashes.

## 2. Test pyramid

### 2.1 Fast unit tests (Rust)

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
  - overflow chain implementation (write/read large blob)
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
- Long-running transactions:
  - Test snapshot isolation under concurrent writes
  - Verify readers see consistent snapshots
  - Test transaction rollback on errors
- Race condition testing:
  - Concurrent reader/writer scenarios (see ADR-0026)
  - Randomized scheduling to expose timing-dependent bugs
  - Stress testing with many simultaneous operations
  - Deadlock detection and prevention verification

### 2.3 Crash-injection tests (Python)

Core suite for ACID durability.

- Define failpoints in engine (or FaultyVFS) at:
  - wal write frame
  - wal write commit record
  - wal fsync
  - db file page write during checkpoint
  - db file fsync (if applicable)
- For each failpoint:
  1. run a scripted transaction scenario
  2. crash at failpoint (kill process)
  3. reopen
  4. assert database invariants and expected visibility

**Torn Write Tests:**

- Partial WAL frame writes (write only N bytes of M-byte frame)
- Verify: recovery ignores incomplete frames
- Test scenarios:
  - Crash mid-frame during multi-page transaction
  - Crash between frame header and payload
  - Crash during commit frame write
- Assert: committed transactions visible, uncommitted not visible, no corruption

### 2.4 Differential tests vs PostgreSQL (Python)

For supported subset only.

**PostgreSQL Version:** Target PostgreSQL 15.x for compatibility testing. CI should test against PG14, PG15, and PG16 to ensure broad compatibility.

- Load identical data into PostgreSQL and DecentDB
- Execute deterministic SQL
- Compare:
  - row counts
  - ordered results
  - NULL handling
  - string matching for LIKE patterns (define exact semantics)

### 2.5 Resource leak tests

- File handle leaks:
  - Track all file opens/closes
  - **Verify sort temp files deleted**: after spill tests, ensure `temp_dir` is empty
  - Assert all handles closed after each test
  - Use OS-level tools (lsof, handle.exe) in CI
- Memory leaks:
  - Use Rust tools like Valgrind or Miri for leak detection
  - Run tests with leak detection enabled
  - Assert no memory growth across repeated operations
  - Test long-running operations for gradual memory accumulation
  - Verify connection-scoped resource cleanup
- Lock deadlocks:
  - Inject random delays in lock acquisition
  - Detect deadlocks via timeout
  - Verify deadlock recovery (transaction abort)
  - Test lock ordering consistency to prevent circular waits

### 2.6 Test Data Generation Specifications

**Dataset Requirements:**

1. **Sequential ID Dataset:**
   - Row IDs starting from 1, incrementing by 1
   - Used for: B+Tree structure validation, basic query testing
   - Size: 1k, 10k, 100k, 1M rows

2. **Sparse/Deleted Data Dataset:**
   - Sequential insert followed by 30% random deletes
   - Used for: B+Tree structure integrity, freelist testing
   - Size: 100k rows with 30k deletes

3. **Unicode Text Dataset:**
   - Text fields with mixed ASCII and multi-byte UTF-8 characters
   - Include: Latin (é, ñ, ü), Cyrillic, CJK characters
   - Used for: Trigram index testing, string comparison, LIKE patterns
   - Size: 10k rows

4. **Edge Case Dataset:**
   - Empty strings, NULL values, max-length text, boundary numeric values
   - Used for: Type validation, boundary testing, NULL handling
   - Size: 1k rows

5. **Music Library Dataset (Reference):**
   - Structure matching MusicBrainz schema (artist, album, track)
   - 25k artists, 80k albums, 9.5M tracks
   - Used for: Performance benchmarks, FK constraint testing

## 3. Faulty VFS design (must-have)

Implement a test-only VFS layer in Rust that can be toggled on.
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
- Coverage reporting (Rust unit tests):
  - Run `cargo tarpaulin` or `cargo llvm-cov`.
  - Output: `build/coverage/summary.txt` and `build/coverage/summary.json`.
  - Notes: coverage is collected via LLVM; treat the numbers as a trend signal for test completeness.
- CI:
  - PR: unit + small property suite on all OSes
  - nightly: extended crash suite + long property runs + fuzz (if adopted)

## 5.5 Binding Integration Testing

Language bindings (Python, .NET, Go, Java, Node, Dart) provide FFI access to the C ABI. Binding tests verify:
- Correct marshalling of types across language/C boundaries
- API completeness and usability
- Resource lifecycle (connection open/close, result cleanup)
- Error handling propagation

### 5.5.1 Binding Test Coverage

**Comprehensive Test Suites (Run on Overnight CI):**
- **Python** (`bindings/python/tests/`): 25+ test files covering:
  - DB-API 2.0 compliance (test_basic.py)
  - SQLAlchemy dialect (test_sqlalchemy.py)
  - Concurrency stress (test_concurrency_stress.py)
  - Memory leak detection (test_memory_leak.py)
  - Data type coverage (test_datatypes.py, test_decimal.py)
  - Edge cases and relationships (test_edge_cases.py, test_relationships.py)
  - ~10-15 minutes runtime
- **.NET** (`bindings/dotnet/tests/`): 30+ test files covering:
  - ADO.NET layer (AdoNetLayerTests.cs)
  - Entity Framework Core (DbSetEdgeCaseTests.cs, DecentDBContextTests.cs)
  - Dapper ORM integration (DapperIntegrationTests.cs)
  - SQLite compatibility (SqliteCompatibilityTests.cs)
  - Transaction handling (TransactionTests.cs)
  - Data types and window functions (all data types, window functions)
  - ~10-15 minutes runtime

**Smoke Tests (PR and Nightly CI):**
- **Go, Java, Node, Dart**: Quick smoke tests only (create table, insert, select, error handling)
  - Go: `tests/bindings/go/smoke.go`
  - Java: `tests/bindings/java/Smoke.java`
  - Node: `tests/bindings/node/smoke.js`
  - Dart: `tests/bindings/dart/smoke.dart`
  - Combined runtime: ~2-3 minutes
  - Rationale: These bindings are thinner FFI wrappers; comprehensive suites would require significant community ownership. Smoke tests catch regressions in FFI ABI layer quickly.

### 5.5.2 Platform Strategy

**Linux only** for overnight binding tests (nightly-extended.yml):
- Bindings are FFI wrappers—correctness is determined by C ABI stability and Rust engine correctness
- Core engine is already cross-platform tested (PR fast runs ubuntu-latest)
- FFI ABI breakage typically manifests identically across all OSes
- Overhead reduction: ~1-2 hours saved per nightly run
- Multi-OS binding testing reserved for:
  - Manual weekly runs (optional)
  - Release candidate validation
  - Specific binding platform-specific issues

### 5.5.3 CI Integration

**PR Fast (every commit to main, every PR):**
- Build cdylib (release-mode Rust library)
- Run Python smoke tests
- Run .NET smoke tests
- Run Go, Java, Node, Dart smoke tests
- Combined: ~5 minutes

**Nightly Extended (once daily):**
- Build cdylib
- Run full Python binding test suite (all 25+ tests)
- Run full .NET binding test suite (all 30+ tests)
- Run smoke tests for Go, Java, Node, Dart
- Crash injection & soak tests for core engine (existing)
- Combined delta: ~20-30 minutes

### 5.5.4 Binding Test Failure Response

- Binding test failures block merge to main
- If a binding test failure is environment-specific (e.g., OS-specific), document the issue and re-enable on fix
- Any third-party ORM test failure (SQLAlchemy, Dapper, Entity Framework Core) requires investigation—likely indicates a regression in SQL semantics or result marshalling

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
  - Point lookup: fail if P95 increases > 10% (tight threshold for critical path)
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
