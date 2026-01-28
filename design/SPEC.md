# DecentDb SPEC (Engineering Specification)
**Date:** 2026-01-27  
**Status:** Draft (v0.1)

## 1. Overview
This document defines the MVP engineering design for DecentDb:
- Embedded DB engine in **Nim**
- Strong correctness via **Python-driven testing harness** + unit/property/crash tests
- ACID via **WAL-only** design
- Storage: **paged file + B+Trees**, with **trigram inverted index** for substring searches

MVP scope: single process, multi-threaded readers, single writer.

---

## 2. Module architecture
### 2.1 Engine modules (Nim)
1. **vfs/**
   - OS file I/O abstraction: open/read/write/fsync/lock (intra-process lock only MVP)
   - “Faulty VFS” hooks for tests (partial writes, dropped fsync, crash points)

2. **pager/**
   - Fixed-size page manager (default page size: 4096 bytes)
   - Page cache (pin/unpin, dirty tracking, eviction)
   - Page allocation + freelist

3. **wal/**
   - WAL file append and recovery
   - Frame checksums, commit markers
   - WAL index (in-memory map pageId -> latest frame offset for fast reads)

4. **btree/**
   - B+Tree implementation for tables and secondary indexes
   - Cursors and iterators
   - Node split handling (merge/rebalance optional post-MVP)

5. **record/**
   - Record encoding/decoding: varint lengths + typed fields
   - Overflow pages for large TEXT/BLOB (optional MVP+; allow inline with max threshold initially)

6. **catalog/**
   - System tables and schema management
   - Stores table/column/index/fk metadata
   - Schema versioning cookie

7. **sql/**
   - Parse SQL (Postgres-like) via embedded parser or FFI (see §6)
   - Bind/resolve names
   - Lower into logical plan

8. **planner/**
   - Rule-based planner:
     - IndexSeek when predicates match indexed columns
     - NestedLoopJoin + index on inner side
     - Prefer selective predicates first (text index candidates)

9. **exec/**
   - Volcano (iterator) engine operators:
     - TableScan, IndexSeek, Filter, Project
     - NestedLoopJoin (MVP)
     - Sort (in-memory), Limit/Offset
   - Row materialization in reusable buffers (avoid per-row heap alloc)

10. **search/**
   - Trigram index build + query evaluation
   - Posting list storage format (see §8)

### 2.2 Testing modules (Python)
- `tests/harness/` orchestrates:
  - black-box SQL tests
  - crash injection scenarios
  - differential checks vs PostgreSQL for subset
  - dataset generators and benchmarks

---

## 3. File layout and formats
### 3.1 Files
- `dbfile` — main database file (paged)
- `dbfile.wal` — write-ahead log (append-only)
- (optional future) `dbfile.lock` / shared-memory region for multi-process

### 3.2 Main DB header (page 1)
Store:
- magic bytes + format version
- page size
- schema cookie (increment on schema change)
- root page ids for system catalog B+Trees
- freelist head pointer + count
- last checkpoint LSN / WAL checkpoint info (optional MVP)

### 3.3 Page types
- B+Tree internal page
- B+Tree leaf page
- Overflow page (optional MVP+)
- Freelist trunk/leaf (or single freelist chain for MVP)

---

## 4. Transactions, durability, and recovery (WAL-only)
### 4.1 WAL frame format (MVP)
Each frame appends:
- `page_id` (u32)
- `page_size` bytes page image
- `frame_checksum` (u64)
- `lsn` (u64 monotonically increasing)
- `commit_flag` (bool) OR a dedicated commit record

Commit rule:
- A transaction is committed when a commit marker is durably written.
- Default durability: `fsync(wal)` on commit.

### 4.2 Snapshot reads
On read transaction start:
- capture `snapshot_lsn = wal_end_lsn` (AtomicU64)
- When reading page P:
  - if wal has a version of P with `lsn <= snapshot_lsn`, return latest such frame
  - else return page from main db file

Maintain in-memory `walIndex: page_id -> list/latest frame offsets` (store latest; optionally chain for multiple versions during long read txns).

### 4.3 Checkpointing (MVP)
- Manual or opportunistic checkpoint:
  - Only checkpoint when no active readers (simplest MVP)
  - Copy latest committed page images from WAL back to main db file
  - Truncate WAL afterward

Future: incremental checkpoint while readers exist.

### 4.4 Crash recovery
On open:
- scan WAL from last checkpoint
- validate frame checksums
- apply frames up to last commit boundary into walIndex view
- DB becomes readable immediately using WAL overlay
- optional: perform checkpoint soon after open

---

## 5. Concurrency model (single process)
### 5.1 Writer
- Exactly one active writer transaction at a time
- Recommended: a single “writer thread” owning write txn state (actor pattern)

### 5.2 Readers
- Multiple concurrent readers
- Each reader uses snapshot_lsn and does not block on writer except for brief schema locks

### 5.3 Locks and latches (MVP)
- `schemaLock`: RW lock around catalog changes
- Page cache: per-page latch + global eviction lock
- WAL append: mutex around append operations (or single writer thread)

Multi-process locking is out of scope for MVP.

---

## 6. SQL parsing & compatibility (Postgres-like)
### 6.1 Parser choice
MVP recommendation:
- Use a Postgres-compatible parser via FFI (e.g., `libpg_query`) to accept familiar syntax.
- Normalize parse trees into DecentDb’s internal AST immediately.

Alternative:
- Use Nim-native `parsesql` for faster iteration, then migrate to libpg_query later.

### 6.2 Supported SQL subset (MVP)
- DDL: `CREATE TABLE`, `CREATE INDEX`
- DML: `SELECT`, `INSERT`, `UPDATE`, `DELETE`
- Joins: `LEFT JOIN`, `INNER JOIN` on equality predicates
- Filters: basic comparisons, boolean ops, `LIKE`
- Ordering: `ORDER BY` (multi-column), `LIMIT`, `OFFSET`

### 6.3 Parameterization
Pick one:
- `$1, $2, ...` positional (Postgres style) OR
- named parameters `:name` (common in app layers)
Document and test thoroughly.

---

## 7. Relational features
### 7.1 Primary keys
- Default rowid PK if none specified
- Unique index enforces PK uniqueness

### 7.2 Foreign keys
- Enforced at statement time or commit time (choose one; MVP: statement time)
- Requires index on referenced parent key
- Requires index on child FK columns (auto-create if missing, or require explicitly)

MVP actions:
- `RESTRICT` / `NO ACTION` on delete/update
Optional later:
- `CASCADE`, `SET NULL`

---

## 8. Trigram substring search index
### 8.1 Why trigrams
`LIKE '%pattern%'` cannot use a normal B+Tree index efficiently. Trigrams allow:
- candidate retrieval via inverted index
- intersection of posting lists
- final verification using actual substring match

### 8.2 Index data model
For each indexed TEXT column:
- Normalize to uppercase (configurable; MVP assumes uppercase inputs are already normalized)
- Generate trigrams across a canonical form (define whitespace/punctuation handling)

Store:
- `trigram` (3-byte token or packed u32)
- postings list: sorted list of row ids (or record ids)
- optional payload: store `albumId` with `trackId` for fast join pruning

### 8.3 Query evaluation
Given pattern:
- if `len(pattern) < 3`: do not use trigram (require additional filters or fall back with cap)
- generate trigrams of pattern
- order trigrams by increasing postings frequency
- intersect progressively until candidate set <= threshold
- verify each candidate by substring match

### 8.4 Broad-pattern guardrails
Maintain postings count per trigram.
- If rarest trigram count exceeds a threshold (e.g., >100k), require additional predicate or cap results.
- Provide an engine setting for thresholds.

### 8.5 Storage format for postings (MVP)
- Store postings lists in a dedicated B+Tree keyed by trigram:
  - key: trigram
  - value: compressed postings blob (delta-encoded varints)
- Updates:
  - For MVP, allow “append segments” per trigram and merge occasionally (LSM-like just for postings), OR
  - maintain small in-memory buffers per trigram and flush in batches during transaction commit.

Pick the simplest that keeps write amplification acceptable.

---

## 9. Planner rules for target workload
For queries like artist→album→track:
- If `artist.id = ?`:
  - drive from artist PK seek
  - then album index `(artistId, name, id)`
  - then track index `(albumId, trackNumber, id)`
  - avoid full sort by streaming in index order

For contains predicates:
- Use trigram index to produce candidate sets first, then join outward.
- Prefer driving from the most selective candidate set (smallest).

---

## 10. Testing strategy (critical)
### 10.1 Test layers
1. **Pure unit tests (Nim)**
   - Pager read/write, freelist correctness
   - WAL frame encoding, checksum verification
   - B+Tree invariants (ordering, search correctness, cursor iteration)
   - Trigram generation and postings encode/decode

2. **Property tests (Nim + Python)**
   - Random sequences of operations preserve invariants
   - “index results == scan results”
   - “FK never violated” under random mutations

3. **Crash-injection tests (Python harness + Faulty VFS)**
   - Kill at every critical write step during commit
   - Reopen and verify:
     - committed txns visible
     - uncommitted txns not visible
     - no structural corruption

4. **Differential tests (Python)**
   - For supported SQL subset, run query on:
     - DecentDb
     - PostgreSQL
   - Compare result sets for deterministic queries (no timestamps/random)

### 10.2 Faulty VFS requirements
Test hooks must simulate:
- partial writes (write N bytes then fail)
- re-ordered writes (optional)
- dropped fsync (simulate fsync “success” without durability)
- abrupt crash at labeled “failpoints”

### 10.3 CI requirements
- Run fast unit/property suites on every PR (Linux/Windows/macOS)
- Nightly jobs run extended crash + fuzz suites
- Coverage reporting target (project-specific; track trend)

---

## 11. Benchmarks and performance budgets
Define microbenchmarks:
- PK point lookup latency
- FK join expansion latency (artist->albums->tracks)
- Contains search (trigram) latency with typical patterns
- Bulk load throughput (MVP+)

Track:
- P50/P95 latency
- WAL size growth
- checkpoint time
- recovery time after crash

---

## 12. Future compatibility: Npgsql / PostgreSQL wire protocol
Not MVP. If pursued:
- Implement pgwire subset as a server endpoint
- Add minimal catalog responses for clients
- Maintain dialect compatibility with libpg_query parser

