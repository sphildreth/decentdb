# DecentDb PRD (Product Requirements Document)
**Date:** 2026-01-27  
**Status:** Draft (v0.1)

## 1. Product summary
DecentDb is an embedded, single-machine relational database engine focused on:
- **Durable ACID writes** (priority #1)
- **Fast reads** (priority #2)
- **Single writer + many concurrent readers** (MVP: single process, multi-threaded)
- **PostgreSQL-like SQL syntax** (subset, “good enough” for common CRUD + joins)
- **Efficient substring search** for user-facing “contains” queries (e.g., `LIKE '%FOO%'`) on selected text columns

The initial target workload is music-library style data:
- ~25k artists, ~80k albums, ~9.5M tracks (initial)
- Stress goal: scalable design assumptions that can grow toward tens of millions of rows (e.g., MusicBrainz-scale), without requiring a redesign.

The project emphasizes **testing and correctness from day 1**, using a **Python-driven test harness** plus engine-level unit and property tests.

## 2. Goals
### 2.1 Functional goals (MVP)
1. **Relational core**
   - Tables with typed columns (MVP types: NULL, INT64, BOOL, FLOAT64, TEXT (UTF-8), BLOB)
   - Primary keys (rowid or explicit PK)
   - Foreign keys with enforcement (MVP: `RESTRICT` / `NO ACTION`; optional `CASCADE` later)
   - Secondary indexes (B+Tree)

2. **Transactions & durability**
   - ACID semantics with **WAL-only** (Write-Ahead Log)
   - `BEGIN`, `COMMIT`, `ROLLBACK`
   - Crash-safe: committed transactions survive crash; uncommitted do not.

3. **Concurrency**
   - **Single writer at a time**
   - Multiple concurrent readers across threads in the same process
   - Snapshot reads: readers see a stable snapshot as of transaction start.

4. **SQL subset (PostgreSQL-like syntax)**
   - DDL: `CREATE TABLE`, `CREATE INDEX`
   - DML: `SELECT`, `INSERT`, `UPDATE`, `DELETE`
   - Predicates: `=`, `!=`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `IN` (optional MVP), `LIKE`
   - `LEFT JOIN`, `INNER JOIN`
   - `ORDER BY`, `LIMIT`, `OFFSET`
   - Parameters (positional and/or named; pick one for MVP and document it)

5. **Substring search acceleration**
   - **Trigram inverted index** for chosen TEXT columns to accelerate `LIKE '%pattern%'`
   - Guardrails for “too broad” patterns (e.g., articles like “THE”, “LA”):
     - posting-list frequency thresholds
     - minimum effective pattern length rules
     - optional requirement of additional filters for short patterns

### 2.2 Non-functional goals (MVP)
- Cross-platform: Linux/Windows/macOS
- Deterministic tests: reproducible failure cases with seeded randomness
- Measurable performance:
  - point lookups and FK-join queries should be “snappy”
  - writes should be durable by default (fsync on commit)

## 3. Non-goals (MVP)
- Multi-process concurrency (future)
- Full PostgreSQL semantics, system catalogs, extensions, or wire protocol
- Advanced query optimizer (cost-based, statistics-driven)
- Full-text ranking, stemming, language analyzers (beyond trigram index)
- Online schema migrations / ALTER TABLE beyond minimal needs
- Replication / clustering
- Encrypted storage (future optional)

## 4. Target users & use cases
### 4.1 Primary user
A developer building an embedded application needing relational integrity and fast local queries.

### 4.2 Core use cases
1. CRUD-heavy app with normalized schema and FK joins
2. Interactive search using `LIKE '%…%'` on a few text columns (artist/album/track fields)
3. Read-heavy workloads with occasional writes (single writer thread)

## 5. Representative queries (acceptance targets)
The engine should efficiently handle these patterns:

### 5.1 Join + contains predicates
```sql
SELECT a.id, a.name, al.name, t.trackNumber, t.name
FROM artist a
LEFT JOIN album al ON (a.id = al.artistId)
LEFT JOIN track t on (al.id = t.albumId)
WHERE al.name like '%COLDSPRING%'
AND a.name like '%JOEL%'
ORDER BY a.name, al.name, t.trackNumber;
```

### 5.2 Point lookup + ordered expansion
```sql
SELECT a.id, a.name, al.name, t.trackNumber, t.name
FROM artist a
LEFT JOIN album al ON (a.id = al.artistId)
LEFT JOIN track t on (al.id = t.albumId)
WHERE a.Id = 143
ORDER BY a.name, al.name, t.trackNumber;
```

### 5.3 Track title search + joins
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

## 6. MVP milestones (phased delivery)
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
- Create table/index metadata
- Simple SELECT execution pipeline

### M5 — Constraints + FKs + trigram index (v1 search)
- FK enforcement (`RESTRICT`)
- Trigram index build and query integration
- Guardrails for broad queries
- Benchmark suite and regression thresholds

### M6 — Harden + performance passes
- Bulk loader mode (optional MVP+)
- Improved join ordering heuristics
- Checkpointing + WAL size management
- Expanded SQL subset as needed

## 7. Quality bar (must-have)
### 7.1 Correctness requirements
- ACID: committed data survives crash in all tested scenarios
- Readers always see consistent snapshots
- FKs and constraints enforced correctly

### 7.2 Testing requirements (critical)
- Unit tests for every core module (pager, WAL, B+Tree, execution)
- Property-based tests for invariants
- Crash-injection tests for WAL correctness
- Differential testing of SQL subset vs PostgreSQL for deterministic queries (Python harness)

## 8. Success metrics
- Import/load and query performance on target dataset sizes
- P95 latency on representative queries
- Crash-recovery time bounds
- Test suite runtime and coverage targets:
  - Unit tests: fast (< 1–2 minutes) and run on every PR
  - Extended fuzz/crash suites in nightly CI

## 9. Risks and mitigations
- **Index bloat (trigrams):** mitigate with posting list compression and frequency guards
- **WAL growth:** checkpoints + size thresholds
- **Planner limitations:** rule-based heuristics + targeted indexes
- **Testing complexity:** invest early in faulty I/O and deterministic replay

## 10. Out of scope future roadmap (post-MVP)
- Multi-process locking/shmem
- PostgreSQL wire protocol compatibility (Npgsql)
- Additional DDL (`ALTER TABLE`)
- Advanced search (tokenization + language-aware features)
- Background checkpointing and compaction
