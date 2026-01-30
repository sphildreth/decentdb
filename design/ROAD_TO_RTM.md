# DecentDb Road to RTM (Release to Manufacturing) v1.0.0
**Status:** Draft  
**Target:** Production-ready 1.0.0 release

---

## Executive Summary

Based on comprehensive analysis of PRD.md, SPEC.md, TESTING_STRATEGY.md, and the current codebase (Nim source + Python test harness), DecentDb is approximately **85-90% code complete** for a 1.0.0 release.

### Current State
- **Core Engine**: ✅ Feature complete (storage, SQL execution, transactions)
- **SQL Subset**: ✅ 95% complete (missing IN operator, partial ILIKE)
- **Testing Infrastructure**: ⚠️ 75% complete (Phase 1 infrastructure complete, Phase 2+ pending)
- **Documentation**: ⚠️ 30% complete

### Critical Path to RTM
The primary blocker is the **testing infrastructure** as defined in TESTING_STRATEGY.md. While unit tests are comprehensive, the mandatory crash-injection, differential, and property-based testing layers are significantly under-implemented.

---

## Phase 1: Testing Infrastructure Completion (CRITICAL BLOCKER)

**Success Criteria:** All TESTING_STRATEGY.md requirements implemented and passing in CI

### 1.1 Crash-Injection Test Suite (Python Harness)

**Current State:** Only 1 basic scenario (`open_close.json`) exists. The FaultyVFS implementation is complete, but test scenarios are missing.

**Required Scenarios:**

- [x] **WAL Frame Write Failure**
  - File: `tests/harness/scenarios/wal_frame_fail.json`
  - Test: Transaction with multiple pages, crash mid-frame write
  - Verify: No corruption, uncommitted data not visible
  - Implementation: Add failpoint trigger in runner.py, execute multi-page INSERT, kill at frame N

- [x] **WAL Commit Record Failure**
  - File: `tests/harness/scenarios/wal_commit_fail.json`
  - Test: Crash during commit frame write
  - Verify: Transaction not visible (no commit marker = not committed)
  - Implementation: Set failpoint after N frames but before commit frame

- [x] **WAL Fsync Drop**
  - File: `tests/harness/scenarios/wal_fsync_drop.json`
  - Test: Simulate "fsync lies" (FaultyVFS capability exists)
  - Verify: Recovery handles partial durability correctly
  - Implementation: Use FaultyVFS with fsync injection enabled

- [x] **Checkpoint Page Write Failure**
  - File: `tests/harness/scenarios/checkpoint_page_fail.json`
  - Test: Crash during checkpoint page copy to main DB
  - Verify: Database remains consistent, WAL intact for recovery
  - Implementation: Set failpoint during checkpoint phase

- [x] **Checkpoint Fsync Failure**
  - File: `tests/harness/scenarios/checkpoint_fsync_fail.json`
  - Test: Drop fsync during checkpoint
  - Verify: Next recovery handles interrupted checkpoint
  - Implementation: Use FaultyVFS fsync injection

- [x] **Torn Write - Partial Frame Header**
  - File: `tests/harness/scenarios/torn_write_header.json`
  - Test: Write only N bytes of frame header (where N < full header size)
  - Verify: Recovery detects incomplete frame via checksum, ignores it
  - Implementation: FaultyVFS partial write with exact byte count

- [x] **Torn Write - Partial Frame Payload**
  - File: `tests/harness/scenarios/torn_write_payload.json`
  - Test: Write full header but only partial payload
  - Verify: Recovery detects size mismatch, ignores frame
  - Implementation: FaultyVFS with payload_size - 1 bytes written

- [x] **Torn Write - Partial Commit Frame**
  - File: `tests/harness/scenarios/torn_write_commit.json`
  - Test: Crash mid-commit frame write
  - Verify: Transaction treated as uncommitted
  - Implementation: FaultyVFS during commit marker write

- [x] **Multi-Transaction Crash**
  - File: `tests/harness/scenarios/multi_txn_crash.json`
  - Test: 3 transactions (committed, in-progress, failed), crash randomly
  - Verify: Only committed transaction visible
  - Implementation: Complex scenario with checkpoints

- [x] **Reader During Checkpoint Crash**
  - File: `tests/harness/scenarios/reader_checkpoint_crash.json`
  - Test: Active reader at snapshot LSN, checkpoint runs, crash mid-checkpoint
  - Verify: Reader can complete, recovery consistent
  - Implementation: Requires multi-threading simulation in Python

**Implementation Notes:**
- [x] Extend `tests/harness/runner.py` to support failpoint configuration
- [x] Add `--failpoint` CLI flag to decentdb for test mode
- [x] Create scenario JSON format supporting failpoint specification
- [x] Implement Python test orchestrator that:
  1. Creates fresh database
  2. Sets specific failpoint via env var or CLI
  3. Executes SQL workload
  4. Kills process at failpoint
  5. Reopens database
  6. Verifies invariants via assertions

### 1.2 Torn Write Detection Tests

SPEC section 4.1 requires torn write detection. Tests implemented in `tests/nim/test_wal.nim` covering frame format with payload_size and checksums.

- [x] **Frame Header Truncation Test**
  - Verify: 8-byte header, write only 4 bytes
  - Expected: Recovery identifies as incomplete, ignores

- [x] **Payload Size Mismatch Test**
  - Write header with payload_size=4096, write only 2048 bytes
  - Verify: Recovery detects mismatch via checksum failure

- [x] **Corrupted Checksum Test**
  - Write valid frame, corrupt 1 bit in payload
  - Verify: Recovery detects via CRC-32C mismatch

- [x] **Cross-Frame Corruption Test**
  - Write frame N, crash, write frame N+1 overlapping frame N's location
  - Verify: Recovery uses latest valid LSN

### 1.3 Differential Test Suite vs PostgreSQL

**Current State:** Complete implementation with 25+ test cases in `tests/harness/differential_runner.py` with PostgreSQL adapter at `tests/harness/postgres_ref/__init__.py`

**Required Tests:**

- [x] **DDL Differential Tests**
  - CREATE TABLE with all column types (INT, TEXT, BOOL, FLOAT, BLOB)
  - CREATE INDEX (BTree and trigram)
  - DROP TABLE/DROP INDEX
  - Verify: Schema matches between DecentDb and PostgreSQL

- [x] **DML Differential Tests - INSERT**
  - Single row insert
  - Multi-row insert (if supported)
  - Insert with NULL values
  - Insert with explicit PK
  - Insert with default rowid
  - Verify: Row counts and content match

- [x] **DML Differential Tests - SELECT**
  - SELECT * with ORDER BY
  - SELECT with WHERE equality
  - SELECT with WHERE range (<, >, <=, >=)
  - SELECT with WHERE LIKE patterns (multiple patterns)
  - SELECT with WHERE AND/OR combinations
  - SELECT with LIMIT/OFFSET
  - Verify: Result sets identical (order, values, NULL handling)

- [x] **DML Differential Tests - UPDATE**
  - Update single row
  - Update multiple rows with WHERE
  - Update with subquery (if supported)
  - Verify: Updated data matches

- [x] **DML Differential Tests - DELETE**
  - Delete single row
  - Delete with WHERE clause
  - Delete with CASCADE (post-MVP, skip for now)
  - Verify: Remaining rows match

- [x] **JOIN Differential Tests**
  - INNER JOIN on single column
  - LEFT JOIN on single column
  - Multiple JOINs (3+ tables)
  - JOIN with WHERE predicates
  - JOIN with aggregate functions
  - Verify: Join results match PostgreSQL exactly

- [x] **Aggregate Differential Tests**
  - COUNT(*) vs COUNT(col) NULL handling
  - SUM, AVG, MIN, MAX
  - GROUP BY single column
  - GROUP BY multiple columns
  - GROUP BY with HAVING
  - Verify: Aggregation results match (watch for floating point precision)

- [x] **Transaction Differential Tests**
  - BEGIN...COMMIT visibility
  - BEGIN...ROLLBACK isolation
  - Concurrent transaction isolation (simulate with separate connections)
  - Verify: Transaction semantics match

- [x] **Constraint Differential Tests**
  - NOT NULL violation handling
  - UNIQUE constraint violation
  - PRIMARY KEY uniqueness
  - FOREIGN KEY enforcement (RESTRICT)
  - Verify: Error behavior matches PostgreSQL

**Implementation Requirements:**
- [x] Create `tests/harness/differential_runner.py` (complete with 25+ test cases)
- [x] Create `tests/harness/postgres_ref/__init__.py` (PostgreSQL adapter)
- [x] Implement dual-database setup:
  ```python
  # Load identical schema into both databases
  # Execute identical SQL on both
  # Compare results with tolerance for floating point
  ```
- Handle PostgreSQL connection (PGDATABASE env var or connection string)
- Handle DecentDb CLI execution
- Normalize NULL representations (Python None vs SQL NULL)
- Handle type conversions (PostgreSQL may return different types)
- CI integration: Run against PostgreSQL 14, 15, 16

### 1.4 Property-Based Tests

TESTING_STRATEGY.md section 2.2 requires property-based testing. Implemented in `tests/harness/property_runner.py` with 5 property tests.

- [x] **Index-Scan Equivalence Property**
  - Generate random tables with random data
  - Create random indexes
  - For random queries: SELECT via index == SELECT via table scan
  - Implementation: Use Hypothesis (Python) or custom generator

- [x] **BTree Ordering Property**
  - Insert random sequence of keys
  - Property: Cursor iteration always returns sorted order
  - Test with duplicate keys, negative numbers, large values

- [x] **Foreign Key Invariant Property**
  - Random schema with FK relationships
  - Random insert/update/delete operations
  - Property: FK constraint never violated at commit
  - Note: Testing at statement time per ADR-0009

- [x] **ACID Durability Property**
  - Random transaction sequences
  - Simulate crashes at random points
  - Property: Committed data survives, uncommitted does not
  - Implementation: Combine with crash-injection harness

- [x] **Snapshot Isolation Property**
  - Multiple concurrent readers
  - Writer making changes
  - Property: Readers see consistent snapshot from start time
  - Implementation: Multi-threaded test harness

### 1.5 Resource Leak Tests

TESTING_STRATEGY.md section 2.5 requires leak detection. Implemented in `tests/harness/leak_runner.py` with 4 leak tests.

- [x] **File Handle Leak Test**
  - Repeatedly open/close database
  - Verify: OS file handles (lsof) return to baseline
  - Implementation: Use `lsof -p $PID | grep dbfile | wc -l`

- [x] **Sort Temp File Cleanup Test**
  - Execute queries forcing external sort (large ORDER BY)
  - Verify: `temp_dir` is empty after query completes
  - Implementation: List temp_dir before/after

- [x] **Memory Leak Test**
  - Run unit tests with garbage collection statistics enabled
  - Verify: No memory growth across repeated operations
  - Implementation: Nim compiler flags + test wrapper

- [x] **WAL File Growth Test**
  - Long-running readers holding old snapshots
  - Verify: WAL size managed via checkpointing
  - Implementation: Monitor WAL file size over time

### 1.6 Long-Running Reader Tests

ADR-0024 (WAL Growth Prevention) requires testing. Implemented in `tests/harness/reader_runner.py` with 4 reader tests.

- [x] **Reader Timeout Test**
  - Start reader with snapshot
  - Hold snapshot past checkpoint_timeout_sec threshold
  - Verify: Reader forced to close or timeout
  - Implementation: Python test with threading

- [x] **WAL Truncation with Active Readers**
  - Multiple readers at different snapshot LSNs
  - Run checkpoint
  - Verify: WAL truncated only up to minimum reader LSN
  - Implementation: Monitor WAL file, verify partial truncation

---

## Phase 2: Feature Completion (HIGH PRIORITY)

**Success Criteria:** SPEC section 6.2 SQL subset fully implemented

### 2.1 SQL IN Operator Implementation

SPEC 6.2 lists IN operator as "optional MVP" but it's commonly needed.

- [ ] **Parser Support**
  - File: `src/sql/sql.nim` or via libpg_query
  - Verify: `WHERE col IN (val1, val2, val3)` parses correctly
  - AST node for IN expression

- [ ] **Binder Support**
  - File: `src/sql/binder.nim`
  - Type checking for IN list elements
  - Verify all values compatible with column type

- [ ] **Planner Support**
  - File: `src/planner/planner.nim`
  - Convert IN to OR expression or special operator
  - Index usage: Can use index seek for `pk IN (1,2,3)`

- [ ] **Executor Support**
  - File: `src/exec/exec.nim`
  - Evaluate IN against row values
  - Handle NULL in IN list (3-valued logic)
  - Optimize: Use hash set for large IN lists

- [ ] **Tests**
  - Unit tests for IN with various types
  - Differential tests vs PostgreSQL
  - Edge cases: IN with NULL, empty IN list, IN with subquery (future)

### 2.2 ILIKE Full Implementation

Current state: Parser recognizes ILIKE, executor partially handles it.

- [ ] **Verify Parser**
  - File: `src/sql/sql.nim` (line 268 shows ILIKE support)
  - Confirm ILIKE token flows through to planner

- [ ] **Verify Executor**
  - File: `src/exec/exec.nim` (line 225-228 shows LIKE/ILIKE handling)
  - Confirm case-insensitive matching works
  - Test with Unicode characters (uppercase/lowercase conversions)

- [ ] **Trigram Index Integration**
  - File: `src/planner/planner.nim`
  - ILIKE should use trigram index same as LIKE (case-insensitive by design)
  - Verify: `WHERE col ILIKE '%pattern%'` uses trigram seek

- [ ] **Differential Tests**
  - Compare ILIKE behavior with PostgreSQL
  - Edge cases: Turkish I problem, Unicode case folding

### 2.3 HAVING Clause (if not fully implemented)

SPEC 6.2 mentions HAVING with aggregates.

- [ ] **Verify Parser**
  - Check if HAVING is parsed correctly

- [ ] **Verify Planner/Executor**
  - HAVING executes after aggregation
  - Can reference aggregate results

- [ ] **Tests**
  - Differential tests with PostgreSQL

---

## Phase 3: Performance & Hardening (MEDIUM PRIORITY)

**Success Criteria:** All PRD performance targets met and validated

### 3.1 Performance Benchmark Suite

PRD section 8.4 defines performance targets.

- [ ] **Point Lookup Benchmark**
  - Target: P95 < 10ms on 9.5M tracks
  - Dataset: Music library with 9.5M tracks
  - Query: `SELECT * FROM track WHERE id = ?`
  - Implementation: `tests/bench/point_lookup_bench.nim`

- [ ] **FK Join Expansion Benchmark**
  - Target: P95 < 100ms
  - Query: Artist → Albums → Tracks expansion
  - Implementation: `tests/bench/fk_join_bench.nim`

- [ ] **Trigram Search Benchmark**
  - Target: P95 < 200ms
  - Query: `WHERE name LIKE '%pattern%'` with trigram index
  - Test various pattern lengths and selectivities
  - Implementation: `tests/bench/trigram_search_bench.nim`

- [ ] **Bulk Load Benchmark**
  - Target: 100k records < 20 seconds
  - Test with deferred durability
  - Implementation: `tests/bench/bulk_load_bench.nim`

- [ ] **Transaction Insert Benchmark**
  - Target: < 1ms per row with fsync-on-commit
  - Implementation: `tests/bench/txn_insert_bench.nim`

- [ ] **Crash Recovery Time Benchmark**
  - Target: < 5 seconds for 100MB database
  - Create 100MB database, crash it, measure recovery
  - Implementation: `tests/bench/recovery_bench.nim`

- [ ] **CI Integration**
  - Run benchmarks on every PR (track trends)
  - Fail if regression > 10%
  - Store historical results

### 3.2 Memory Budget Validation

SPEC section 11 defines memory budgets.

- [ ] **Peak Memory Monitoring**
  - Verify: Peak memory during queries ≤ 2x cache size
  - Implementation: Track max RSS during test runs

- [ ] **Query Memory Limit**
  - Verify: Query aborts if exceeds max_query_memory (64MB default)
  - Test with deliberately large sorts/joins

- [ ] **Sort Spill Validation**
  - External merge sort spills at 16MB buffer
  - Verify: Sort completes without OOM on large datasets
  - Verify: Spill files cleaned up

### 3.3 WAL Size Management

- [ ] **Auto-Checkpoint Triggers**
  - Checkpoint when WAL reaches checkpointBytes threshold
  - Checkpoint when checkpointMs elapsed
  - Verify: Triggers work as configured

- [ ] **WAL Size Monitoring**
  - Log warnings when WAL grows unexpectedly
  - Alert on checkpoint failures

### 3.4 B+Tree Space Management

SPEC section 17 requirements.

- [ ] **Page Utilization Monitoring**
  - Track utilization per B+Tree
  - Verify: Can detect low utilization (< 50%)

- [ ] **Compaction Trigger**
  - Rebuild B+Tree when utilization drops
  - Verify: Space recovery works
  - Note: Merge/rebalance is post-MVP, compaction is MVP

---

## Phase 4: Documentation (HIGH PRIORITY)

**Success Criteria:** Complete documentation suite

### 4.1 API Reference Documentation

- [ ] **Public API Documentation**
  - File: `docs/api.md`
  - Document all public procs in `src/decentdb.nim`
  - Include examples for each operation

- [ ] **Error Codes Reference**
  - File: `docs/error_codes.md`
  - Document ERR_IO, ERR_CORRUPTION, ERR_CONSTRAINT, etc.
  - Include troubleshooting guidance

- [ ] **Configuration Options**
  - File: `docs/configuration.md`
  - Document all config options (page_size, cache_size_mb, etc.)
  - Performance tuning guidance

### 4.2 User Guides

- [ ] **Getting Started Guide**
  - File: `docs/getting_started.md`
  - Installation instructions
  - First database creation
  - Basic CRUD examples

- [ ] **SQL Reference**
  - File: `docs/sql_reference.md`
  - Supported SQL subset
  - Examples for each statement type
  - Known limitations (vs PostgreSQL)

- [ ] **Performance Tuning Guide**
  - File: `docs/performance_tuning.md`
  - Cache size recommendations
  - Index design guidelines
  - Checkpoint tuning
  - Bulk load best practices

- [ ] **Migration Guide**
  - File: `docs/migration.md`
  - v1 → v2 format upgrade process
  - Export/import procedures
  - Backup recommendations

### 4.3 Architecture Documentation

- [ ] **High-Level Architecture**
  - File: `docs/architecture.md`
  - Module overview
  - Data flow diagrams
  - Concurrency model explanation

- [ ] **File Format Specification**
  - File: `docs/file_format.md`
  - Detailed page layouts
  - WAL frame format
  - Header structure

- [ ] **Testing Documentation**
  - File: `docs/testing.md`
  - How to run tests
  - How to add new tests
  - Crash-injection testing guide

### 4.4 CLI Documentation

- [ ] **CLI Reference**
  - File: `docs/cli.md`
  - Document all commands (exec, list-tables, describe, etc.)
  - Examples for common operations

- [ ] **Troubleshooting Guide**
  - Common errors and solutions
  - Recovery procedures
  - Performance debugging

---

## Phase 5: Release Engineering (MEDIUM PRIORITY)

**Success Criteria:** Release-ready artifacts

### 5.1 Version Management

- [ ] **Version Bump**
  - Update version constant in `src/decentdb.nim` (currently "0.0.1")
  - Set to "1.0.0" for RTM

- [ ] **Changelog Creation**
  - File: `CHANGELOG.md`
  - List all features in 1.0.0
  - Document breaking changes
  - Credit contributors

### 5.2 Build & Distribution

- [ ] **Cross-Platform Builds**
  - Linux (x64, ARM64)
  - Windows (x64)
  - macOS (x64, ARM64/M1)
  - CI pipeline produces all artifacts

- [ ] **Package Managers**
  - Nimble package definition
  - GitHub Releases with binaries
  - Installation instructions

- [ ] **Docker Image**
  - Dockerfile for containerized usage
  - Multi-arch support
  - Published to registry

### 5.3 CI/CD Hardening

- [ ] **Nightly CI**
  - Extended crash tests run nightly
  - Differential tests against PostgreSQL
  - Performance benchmark tracking

- [ ] **Release Automation**
  - Automated release notes generation
  - Binary signing (if applicable)
  - Artifact publishing

---

## Appendix A: Test Data Requirements

Per TESTING_STRATEGY.md section 2.6, these datasets must be available:

- [ ] **Sequential ID Dataset** (1k, 10k, 100k, 1M rows)
  - File: `tests/data/sequential_*.csv`
  - Simple numeric and text data

- [ ] **Sparse/Deleted Dataset** (100k rows, 30k deletes)
  - File: `tests/data/sparse_deleted.db` (pre-generated)
  - For B+Tree integrity testing

- [ ] **Unicode Text Dataset** (10k rows)
  - File: `tests/data/unicode_text.csv`
  - Latin (é, ñ, ü), Cyrillic, CJK characters
  - For trigram index testing

- [ ] **Edge Case Dataset** (1k rows)
  - File: `tests/data/edge_cases.csv`
  - Empty strings, NULLs, max-length text, boundary numerics

- [ ] **Music Library Dataset** (25k artists, 80k albums, 9.5M tracks)
  - File: `tests/data/music_library.db` (or generation script)
  - Reference workload for performance testing
  - Script: `tests/data/generate_music_library.py`

---

## Appendix B: ADR Compliance Checklist

Verify all ADRs are implemented:

- [x] ADR-0001: Page Size (4096 default, configurable)
- [x] ADR-0002: WAL Commit Record Format
- [x] ADR-0003: Snapshot LSN Atomicity
- [x] ADR-0004: WAL Checkpoint Strategy
- [x] ADR-0005: SQL Parameterization ($1, $2 style)
- [x] ADR-0006: Foreign Key Index Creation
- [x] ADR-0007: Trigram Postings Storage Strategy
- [x] ADR-0008: Trigram Pattern Length Guardrails
- [x] ADR-0009: Foreign Key Enforcement Timing (statement-time)
- [x] ADR-0010: Error Handling Strategy
- [x] ADR-0011: Memory Management Strategy
- [x] ADR-0012: B+Tree Space Management
- [x] ADR-0013: Index Statistics Strategy
- [x] ADR-0014: Performance Targets
- [x] ADR-0015: Testing Strategy Enhancements
- [x] ADR-0016: Database Header Checksum
- [x] ADR-0017: Bulk Load API Design
- [x] ADR-0018: Checkpointing Reader Count Mechanism
- [x] ADR-0019: WAL Retention for Active Readers
- [x] ADR-0020: Overflow Pages for BLOBs
- [x] ADR-0021: Sort Buffer Memory Limits
- [x] ADR-0022: External Merge Sort
- [x] ADR-0023: Isolation Level Specification
- [x] ADR-0024: WAL Growth Prevention (Long Readers)
- [x] ADR-0025: Memory Leak Prevention Strategy
- [x] ADR-0026: Race Condition Testing Strategy
- [x] ADR-0027: Bulk Load API Specification
- [x] ADR-0028: Summary of Design Document Updates
- [x] ADR-0029: Freelist Page Format
- [x] ADR-0030: Record Format
- [x] ADR-0031: Overflow Page Format
- [x] ADR-0032: BTree Page Layout
- [x] ADR-0033: WAL Frame Format
- [x] ADR-0035: SQL Parser libpg_query
- [x] ADR-0036: Catalog Constraints & Index Metadata

Pending ADRs (post-1.0.0):
- [ ] ADR-003: CLI Engine Enhancements (partially done)
- [ ] ADR-0037: Group Commit / WAL Batching (post-1.0.0)
- [ ] ADR-0038: Cost-Based Optimization (explicitly post-1.0.0)

---

## Summary: RTM Readiness Criteria

DecentDb 1.0.0 is ready for release when:

### Must Have (Blocking)
1. ✅ All core engine modules implemented and unit tested
2. ⬜ **Crash-injection test suite complete (10+ scenarios)**
3. ⬜ **Differential test suite vs PostgreSQL (all SQL operations)**
4. ⬜ **Property-based tests for invariants**
5. ⬜ **IN operator implemented**
6. ⬜ **Performance benchmarks passing**
7. ⬜ **Complete API and user documentation**

### Should Have (Highly Recommended)
1. ⬜ ILIKE fully verified with differential tests
2. ⬜ Memory leak tests passing
3. ⬜ Long-running reader tests
4. ⬜ Sort temp file cleanup verification
5. ⬜ Cross-platform CI builds
6. ⬜ Changelog and release notes

### Nice to Have
1. ⬜ Docker image
2. ⬜ Package manager distribution
3. ⬜ Video tutorial / demo
4. ⬜ Community contribution guidelines

---

**Current Assessment:**
- Core Engine: **90% complete** ✅
- Testing Infrastructure: **75% complete** ⚠️ (Phase 1 complete, integration pending)
- Documentation: **30% complete** ⬜
- Release Engineering: **20% complete** ⬜

---

**Document Version History:**
- v0.1: Initial gap analysis based on PRD/SPEC/codebase review
