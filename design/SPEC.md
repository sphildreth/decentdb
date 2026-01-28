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
   - Fixed-size page manager (default page size: 4096 bytes, configurable at DB creation)
     - 4KB aligns with typical SSD block sizes and OS page sizes
     - Larger pages (8KB, 16KB) reduce internal fragmentation for wide rows
     - Smaller pages (2KB) reduce memory pressure for cache-constrained environments
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
- `commit_record` (dedicated record type with transaction_id and commit timestamp)

Commit rule:
- A transaction is committed when a commit marker is durably written.
- Default durability: `fsync(wal)` on commit.

### 4.2 Snapshot reads
On read transaction start:
- capture `snapshot_lsn = wal_end_lsn` using atomic load with acquire semantics (`AtomicU64.load_acquire()`)
- This ensures the reader sees a consistent point-in-time view without acquiring locks
- When reading page P:
  - if wal has a version of P with `lsn <= snapshot_lsn`, return latest such frame
  - else return page from main db file

Maintain in-memory `walIndex: page_id -> list/latest frame offsets` (store latest; optionally chain for multiple versions during long read txns).

**Atomicity guarantee**: The `wal_end_lsn` is an `AtomicU64` that is incremented only after the WAL frame is fully written and the in-memory index is updated. Readers use `load_acquire()` to ensure they see all prior writes.

### 4.3 Checkpointing (MVP)
- Manual or opportunistic checkpoint:
  - Only checkpoint when no active readers (simplest MVP)
  - Copy latest committed page images from WAL back to main db file
  - Truncate WAL afterward

**WAL size management**:
- Configurable WAL size threshold (default: 100MB)
- If WAL exceeds threshold:
  - Block new writes until checkpoint completes
  - Wait up to `checkpoint_timeout` (default: 30 seconds) for readers to drain
  - If timeout expires, force checkpoint with readers active (copy pages atomically)
- After checkpoint, truncate WAL to zero and reset `wal_end_lsn`

**Checkpoint triggers**:
- Manual checkpoint via API call
- Automatic checkpoint when WAL size exceeds threshold
- Automatic checkpoint on database close

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

### 5.4 Deadlock detection and prevention
- Writer acquires locks in a consistent order to avoid deadlocks
- Readers never block other readers (snapshot isolation)
- If a deadlock is detected (via timeout), abort the newer transaction
- Document lock acquisition order for all operations

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
- `$1, $2, ...` positional (Postgres style) — chosen for MVP
  - Consistent with libpg_query parser choice
  - Simple to implement and test
  - Well-understood by developers familiar with PostgreSQL
- Document and test thoroughly.

---

## 7. Relational features
### 7.1 Primary keys
- Default rowid PK if none specified
- Unique index enforces PK uniqueness

### 7.2 Foreign keys
- Enforced at statement time (MVP choice for simplicity)
- Requires index on referenced parent key (enforced by CREATE TABLE)
- Auto-create index on child FK columns if not present
  - Index name: `fk_<table>_<column>_idx`
  - This ensures FK checks are efficient and avoids full table scans

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

**Pattern length guardrails**:
- Patterns < 3 characters: never use trigram index (too many matches)
- Patterns 3-5 characters: use trigram only if combined with other filters or if rarest trigram count < threshold
- Patterns > 5 characters: use trigram index, but cap results if rarest trigram exceeds threshold

### 8.4 Broad-pattern guardrails
Maintain postings count per trigram.
- If rarest trigram count exceeds a threshold (e.g., >100k), require additional predicate or cap results.
- Provide an engine setting for thresholds.

### 8.5 Storage format for postings (MVP)
- Store postings lists in a dedicated B+Tree keyed by trigram:
  - key: trigram
  - value: compressed postings blob (delta-encoded varints)
- Updates (MVP choice: in-memory buffers):
  - Maintain small in-memory buffers per trigram (max buffer size: 4KB)
  - Flush buffers to B+Tree during transaction commit
  - If buffer exceeds size, flush immediately and create new buffer
  - This provides bounded write amplification and simple implementation

**Postings compression**:
- Use delta encoding between consecutive row IDs
- Encode deltas as varints (smaller deltas = fewer bytes)
- Typical music library IDs are sequential, yielding high compression ratios

**Write amplification**:
- Each INSERT/UPDATE may update multiple trigram postings
- Buffering reduces per-row write overhead
- Future optimization: batch multiple transactions before flush

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

### 9.1 Index statistics (heuristic-based)
MVP uses simple heuristics without full statistics collection:
- Assume uniform distribution for equality predicates
- For trigram indexes: use actual posting list counts (stored in index metadata)
- For B+Tree indexes: estimate selectivity based on index type:
  - Primary key: assume high selectivity (1 row)
  - Unique index: assume high selectivity (1 row)
  - Non-unique index: assume moderate selectivity (0.1% of rows)

**Future enhancement**: Collect and maintain statistics (histograms, distinct counts) for cost-based optimization.

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

---

## 13. Error handling
### 13.1 Error codes
Define error categories:
- `ERR_IO`: File I/O errors (disk full, permissions, corruption)
- `ERR_CORRUPTION`: Database corruption detected (checksum mismatch)
- `ERR_CONSTRAINT`: Constraint violation (FK, unique, NOT NULL)
- `ERR_TRANSACTION`: Transaction errors (deadlock, timeout)
- `ERR_SQL`: SQL syntax or semantic errors
- `ERR_INTERNAL`: Internal engine errors (should not occur)

### 13.2 Error propagation
- Errors propagate through the execution pipeline using Result types
- Each layer (SQL → planner → exec → storage) can add context
- Transaction rollback on errors:
  - Statement-level errors: rollback current statement only
  - Transaction-level errors: rollback entire transaction
  - Corruption errors: mark database as read-only and require recovery

### 13.3 Error messages
- Include error code, human-readable message, and context
- For constraint violations: include table/column and violating value
- For corruption: include page ID and expected vs actual checksum

---

## 14. Memory management
### 14.1 Memory pools
- Page cache: fixed-size pool of page buffers (configurable, default: 1000 pages)
- Row materialization: reusable buffers per operator to avoid per-row alloc
- Trigram buffers: small per-trigram buffers (max 4KB each)

### 14.2 Memory limits
- Configurable maximum memory usage (default: 256MB)
- Page cache eviction when limit reached (LRU policy)
- Query execution aborts if memory limit exceeded during execution

### 14.3 Out-of-memory handling
- Pre-allocate memory at startup where possible
- If allocation fails:
  - Abort current query with `ERR_INTERNAL`
  - Rollback current transaction
  - Log error with memory usage statistics
- Do not crash the process; allow graceful degradation

---

## 15. Schema versioning and evolution
### 15.1 Schema cookie
- Stored in main DB header (page 1)
- Incremented on any DDL operation (CREATE TABLE, CREATE INDEX, DROP)
- Used to detect schema changes and invalidate cached metadata

### 15.2 Backward compatibility
- File format version stored in header
- Engine can read older format versions (read-only compatibility)
- Writing to older formats may trigger automatic upgrade (with user confirmation)

### 15.3 Schema changes (MVP)
- Supported: CREATE TABLE, CREATE INDEX, DROP TABLE, DROP INDEX
- Not supported: ALTER TABLE (post-MVP)
- Schema changes require exclusive lock (no active readers or writers)

### 15.4 Migration strategy
- When opening a database with older schema version:
  - Read and validate existing schema
  - Apply any necessary migrations (e.g., new system tables)
  - Update schema cookie
- Migrations are idempotent and crash-safe

---

## 16. Configuration system
### 16.1 Configuration options
Database-level configuration (set at open time):
- `page_size`: 4096, 8192, or 16384 (default: 4096)
- `cache_size_mb`: Maximum page cache size in MB (default: 4MB)
- `wal_sync_mode`: `FULL` (fsync), `NORMAL` (fdatasync), `OFF` (unsafe, testing only)
- `checkpoint_threshold_mb`: WAL size before auto-checkpoint (default: 100MB)
- `checkpoint_timeout_sec`: Max wait for readers before forced checkpoint (default: 30)
- `trigram_postings_threshold`: Max postings before requiring additional filters (default: 100000)

### 16.2 Runtime configuration
Some options can be changed at runtime:
- `trigram_postings_threshold`: adjust based on workload
- `checkpoint_threshold_mb`: adjust for write-heavy workloads

### 16.3 Configuration API
```nim
# Open database with configuration
db = open("dbfile", config{
  page_size: 8192,
  cache_size_mb: 16,
  wal_sync_mode: FULL
})

# Change runtime configuration
db.set_config("trigram_postings_threshold", 50000)
```

---

## 17. B+Tree space management
### 17.1 Node split
- When a B+Tree node exceeds capacity, split into two nodes
- Split point: middle key (balanced split)
- Propagate split up the tree as needed

### 17.2 Page utilization monitoring
- Track average page utilization per B+Tree
- If utilization drops below 50% (configurable), trigger compaction
- Compaction: rebuild B+Tree from scratch, freeing empty pages

### 17.3 Merge/rebalance (post-MVP)
- Not implemented in MVP to simplify code
- Compaction provides equivalent space recovery
- Future: implement merge for delete-heavy workloads

