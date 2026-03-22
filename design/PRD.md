# DecentDB PRD (Product Requirements Document)
**Date:** 2026-01-27  
**Status:** Released (v1.0)

> This document describes the 1.0 baseline scope.

## 1. Product summary
DecentDB is an embedded, single-machine relational database engine focused on:
- **Durable ACID writes** (priority #1)
- **Fast reads** (priority #2)
- **Single writer + many concurrent readers** (single process, multi-threaded)
- **PostgreSQL-like SQL syntax** (subset, “good enough” for common CRUD + joins)
- **Efficient substring search** for user-facing “contains” queries (e.g., `LIKE '%FOO%'`) on selected text columns
- **WAL-based durability** (Write-Ahead Log) for ACID compliance

The initial target workload is music-library style data:
- ~25k artists, ~80k albums, ~9.5M tracks (initial)
- Stress goal: scalable design assumptions that can grow toward tens of millions of rows (e.g., MusicBrainz-scale), without requiring a redesign.

The project emphasizes **testing and correctness from day 1**, using a **Python-driven test harness** plus engine-level unit and property tests.

## 2. Goals
### 2.1 Functional goals (1.0 baseline)
1. **Relational core**
   - Tables with typed columns (baseline types: NULL, INT64, BOOL, FLOAT64, TEXT (UTF-8), BLOB)
   - Primary keys (rowid or explicit PK)
   - Foreign keys with enforcement (`RESTRICT` / `NO ACTION`; optional `CASCADE` later)
   - Secondary indexes (B+Tree)

2. **Transactions & durability**
   - ACID semantics with **WAL-based** persistence
   - `BEGIN`, `COMMIT`, `ROLLBACK`
   - **Isolation level**: Snapshot Isolation (see ADR-0023)
   - Crash-safe: committed transactions survive crash; uncommitted do not.

3. **Concurrency**
   - **Single writer at a time**
   - Multiple concurrent readers across threads in the same process
   - Snapshot reads: readers see a stable snapshot as of transaction start.

4. **SQL subset (PostgreSQL-like syntax)**
   - DDL: `CREATE TABLE`, `CREATE INDEX`, `DROP TABLE`, `DROP INDEX`
   - DML: `SELECT`, `INSERT`, `UPDATE`, `DELETE`
   - Aggregate functions: `COUNT(*)`, `COUNT(col)`, `SUM(col)`, `AVG(col)`, `MIN(col)`, `MAX(col)` with `GROUP BY`
   - Predicates: `=`, `!=`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `IN` (optional), `LIKE`
   - `LEFT JOIN`, `INNER JOIN`
   - `ORDER BY`, `LIMIT`, `OFFSET`
   - Parameters: positional `$1, $2, ...` (Postgres-style)

5. **Substring search acceleration**
    - **Trigram inverted index** for chosen TEXT columns to accelerate `LIKE '%pattern%'`
    - Guardrails for "too broad" patterns (e.g., articles like "THE", "LA"):
      - posting-list frequency thresholds
      - minimum effective pattern length rules
      - optional requirement of additional filters for short patterns

6. **Data portability (1.0 baseline)**
    - Database file is portable across platforms (little-endian format)
    - Export to SQL dump format for backup/migration
    - Import from SQL dump or CSV

7. **Bulk load API (1.0 baseline)**
    - Dedicated high-throughput API for loading large datasets
    - Configurable durability options (see ADR-0027)
    - Maintains snapshot isolation for concurrent readers
    - Performance target: 100k records in < 20 seconds

### 2.3 Non-functional goals (1.0 baseline)
- Cross-platform: Linux/Windows/macOS
- Deterministic tests: reproducible failure cases with seeded randomness
- Measurable performance (tested on reference hardware: Intel i5-8400 or equivalent, 16GB RAM, NVMe SSD):
  - point lookups: P95 < 10ms on target dataset (9.5M tracks)
  - FK-join queries (artist→albums→tracks): P95 < 100ms
  - substring search with trigram index: P95 < 200ms
  - writes should be durable by default (fsync on commit)
  - bulk load: 100k records in < 20 seconds using dedicated bulk_load() API

## 4. Non-goals (1.0 baseline)
- Multi-process concurrency (future)
- Full PostgreSQL semantics, system catalogs, extensions, or wire protocol
- Advanced query optimizer (cost-based, statistics-driven)
- Full-text ranking, stemming, language analyzers (beyond trigram index)
- Advanced ALTER TABLE operations (RENAME COLUMN, MODIFY COLUMN, ADD CONSTRAINT)
- Replication / clustering
- Encrypted storage (future optional)

## 5. Known Limitations
- **Foreign Key Enforcement Timing**: FK constraints are enforced at statement time rather than transaction commit time, which differs from the SQL standard (see ADR-0009 and SPEC section 7.2)

## 6. Critical Gap Addressed: Aggregate Functions
**Status:** Added to baseline requirements (COUNT, SUM, AVG, MIN, MAX with GROUP BY)

Rationale: Essential for any practical embedded database use case including basic analytics and reporting queries.

## 7. Target users & use cases
### 7.1 Primary user
A developer building an embedded application needing relational integrity and fast local queries.

### 7.2 Core use cases
1. CRUD-heavy app with normalized schema and FK joins
2. Interactive search using `LIKE '%…%'` on a few text columns (artist/album/track fields)
3. Read-heavy workloads with occasional writes (single writer thread)

## 8. Representative queries (acceptance targets)
The engine should efficiently handle these patterns:

### 8.1 Join + contains predicates
```sql
SELECT a.id, a.name, al.name, t.trackNumber, t.name
FROM artist a
LEFT JOIN album al ON (a.id = al.artistId)
LEFT JOIN track t on (al.id = t.albumId)
WHERE al.name like '%COLDSPRING%'
AND a.name like '%JOEL%'
ORDER BY a.name, al.name, t.trackNumber;
```

### 8.2 Point lookup + ordered expansion
```sql
SELECT a.id, a.name, al.name, t.trackNumber, t.name
FROM artist a
LEFT JOIN album al ON (a.id = al.artistId)
LEFT JOIN track t on (al.id = t.albumId)
WHERE a.Id = 143
ORDER BY a.name, al.name, t.trackNumber;
```

### 8.3 Track title search + joins
```sql
SELECT a.id, a.name, al.name, t.trackNumber, t.name
FROM artist a
LEFT JOIN album al ON (a.id = al.artistId)
LEFT JOIN track t on (al.id = t.albumId)
WHERE t.title like '%SHE%'
AND al.name like '%COLDSPRING%'
AND a.name like '%JOEL%'
ORDER BY a.name, al.name, t.trackNumber;
```

### 8.4 Performance targets (acceptance criteria)
- Point lookup by primary key: P95 < 10ms
- FK join expansion (artist→albums→tracks): P95 < 100ms
- Substring search with trigram index: P95 < 200ms
- Bulk load (100k records): < 20 seconds using `bulk_load()` API with deferred durability
- Normal transaction insert: < 1ms per row (with fsync-on-commit)
- Crash recovery time: < 5 seconds for 100MB database

## 9. Milestones (phased delivery)

**Checkpointing Timeline Note:** Basic checkpointing is introduced in M3 (WAL + transactions) for WAL size management, but automatic checkpointing with reader coordination is fully hardened in M6.

### M0 — Project skeleton + testing foundation
- Repo layout, build, CI on all OSes
- Python harness scaffold
- Unit test framework decisions locked
- “Faulty I/O” test harness skeleton for crash/partial write simulation

### M1 — Pager + file format + page cache (read-only)
- Open/create DB file
- Read/write fixed-size pages via pager
- Cache with pin/unpin and eviction policy (simple clock/LRU)
- Deterministic page-level tests

### M2 — B+Tree read path
- Table and index B+Tree read traversal
- Record encode/decode
- Cursor iteration

### M3 — WAL + transactions + recovery (single writer)
- WAL append frames, checksums
- Commit marker + fsync policy
- Recovery on open
- Snapshot reads

### M4 — B+Tree write path + DDL/DML subset
- Insert/update/delete basic operations
- **Overflow pages** for BLOB/Large TEXT support
- Create table/index metadata
- Simple SELECT execution pipeline

### M5 — Constraints + FKs + trigram index (v1 search)
- FK enforcement (`RESTRICT`)
- Trigram index build and query integration
- Guardrails for broad queries
- Benchmark suite and regression thresholds

### M6 — Harden + performance passes
- Bulk loader mode (optional post-1.0)
- Improved join ordering heuristics
- Checkpointing + WAL size management
- Expanded SQL subset as needed

## 10. Quality bar (must-have)
### 10.1 Correctness requirements
- ACID: committed data survives crash in all tested scenarios
- Readers always see consistent snapshots
- FKs and constraints enforced correctly

### 10.2 Testing requirements (critical)
- Unit tests for every core module (pager, WAL, B+Tree, execution)
- Property-based tests for invariants
- Crash-injection tests for WAL correctness
- Differential testing of SQL subset vs PostgreSQL for deterministic queries (Python harness)

## 11. Success metrics
- Import/load and query performance on target dataset sizes
- P95 latency on representative queries
- Crash-recovery time bounds
- Test suite runtime and coverage targets:
  - Unit tests: fast (< 1–2 minutes) and run on every PR
  - Extended fuzz/crash suites in nightly CI

## 12. Risks and mitigations
- **Index bloat (trigrams):** mitigate with posting list compression and frequency guards
- **WAL growth:** checkpoints + size thresholds
- **Planner limitations:** rule-based heuristics + targeted indexes
- **Testing complexity:** invest early in faulty I/O and deterministic replay

## 13. Out of scope future roadmap (post-1.0)
- Multi-process locking/shmem
- PostgreSQL wire protocol compatibility (Npgsql)
- Advanced DDL operations (RENAME TABLE, RENAME COLUMN, MODIFY COLUMN type changes, ADD CONSTRAINT)
- Advanced search (tokenization + language-aware features)
- Background checkpointing and compaction
