# DecentDB SPEC (Engineering Specification)
**Date:** 2026-01-27  
**Status:** Released (v1.0)

> This document describes the 1.0 baseline scope.

## 1. Overview
This document defines the baseline engineering design for DecentDB:
- Embedded DB engine in **Nim**
- Strong correctness via **Python-driven testing harness** + unit/property/crash tests
- ACID via **WAL-based** design
- Storage: **paged file + B+Trees**, with **trigram inverted index** for search
- **Mandatory Overflow Pages** for BLOB/Large TEXT support

Current scope (1.0): single process, multi-threaded readers, single writer.

---

## 2. Module architecture
### 2.1 Engine modules (Nim)
1. **vfs/**
  - OS file I/O abstraction: open/read/write/fsync/lock (intra-process lock only)
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
   - Frame trailer (checksum reserved; LSN derived from offsets in v6), commit markers
   - WAL index (in-memory map pageId -> latest frame offset for fast reads)

4. **btree/**
   - B+Tree implementation for tables and secondary indexes
   - Cursors and iterators
  - Node split handling (merge/rebalance optional post-1.0)

5. **record/**
   - Record encoding/decoding: varint lengths + typed fields
  - Overflow pages for large TEXT/BLOB (baseline requirement - see ADR-0020)

6. **catalog/**
   - System tables and schema management
   - Stores table/column/index/fk metadata (including constraints and index kind)
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
    - NestedLoopJoin (1.0 baseline)
     - Sort (External Merge Sort capable, see ADR-0022), Limit/Offset
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
- magic bytes (16 bytes: "DECENTDB" + padding)
- format version (u32)
- page size (u32)
- header checksum (CRC-32C of header fields)
- schema cookie (increment on schema change)
- root page ids for system catalog B+Trees
- freelist head pointer + count
- last checkpoint LSN / WAL checkpoint info

**Header Layout (128 bytes total):**
```
Offset  Size  Field
0       16    Magic bytes
16      4     Format version
20      4     Page size
24      4     Header checksum (CRC-32C)
28      4     Schema cookie
32      4     Root page ID for catalog
36      4     Root page ID for freelist
40      4     Freelist head pointer
44      4     Freelist count
48      8     Last checkpoint LSN
56      72    Reserved
```

See ADR-0016 for checksum calculation details.

**Format version notes:**
- v2 adds catalog-encoded column constraints (NOT NULL/UNIQUE/PK/FK) and index metadata (kind + unique flag + optional partial-index predicate SQL).
- v5 removes per-frame WAL CRC32C validation (checksum field reserved, written as zero).
- v6 removes per-frame WAL LSN trailer; LSNs are derived from frame end offsets.
- v7 removes WAL `payload_size` field; payload sizes are derived from frame type and page size.
- v8 adds a fixed WAL header with a logical end offset (`wal_end_offset`).
- v1 databases are not auto-migrated; open fails with `ERR_CORRUPTION` until upgraded.

### 3.4 Catalog record encoding (v2)
Catalog records are stored in a B+Tree keyed by CRC-32C of record names.

**Table record:**
- kind = `"table"`
- name
- root page id
- next row id
- columns encoded as `name:type[:flags]` with `;` separators
  - flags: `nn`, `unique`, `pk`, `ref=parent_table.parent_column`

**Index record:**
- kind = `"index"`
- name
- table
- column
- root page id
- kind (`"btree"` or `"trigram"`)
- unique flag (0/1)

**View record (1.0):**
- kind = `"view"`
- name
- sql text (canonical defining `SELECT`)
- resolved output columns (`;`-delimited)
- dependencies (`;`-delimited, normalized object names)

Compatibility note:
- View records are an additive catalog extension in 1.0 and do not require a DB header format-version bump.

### 3.3 Page types
- B+Tree internal page
- B+Tree leaf page
- Overflow page (baseline requirement)
- Freelist trunk/leaf (or single freelist chain for 1.0)

---

## 4. Transactions, durability, and recovery (WAL-only)
### 4.1 WAL frame format (1.0 baseline)
Each frame appends:
- `frame_type` (u8): 0=page, 1=commit, 2=checkpoint
- `page_id` (u32, valid for page frames)
- payload (page image or commit metadata)
- `frame_checksum` (u64, **reserved**, written as 0 in format v5)

**LSN (format v6):**
- LSNs are derived from WAL byte offsets (frame end offset).
- `wal_end_lsn` is the WAL end offset after the last committed frame.

**WAL Header (format v8):**
- Fixed 32-byte header at file offset 0:
  - `magic` (8 bytes): `"DDBWAL01"`
  - `header_version` (u32): `1`
  - `page_size` (u32)
  - `wal_end_offset` (u64): logical end offset of the last committed frame (0 if none)
  - `reserved` (u64): 0
- WAL frames start at offset `WalHeaderSize` (32 bytes).
- Recovery scans only up to `wal_end_offset` (not physical file size).
- Checkpoint truncation reduces WAL to header-only and resets `wal_end_offset` to 0.

**Payload size (format v7):**
- PAGE frames: `pageSize`
- COMMIT frames: 0 bytes
- CHECKPOINT frames: 8 bytes (`checkpoint_lsn`)

**Frame Types:**
- **PAGE (0)**: Contains modified page data
  - `page_id`: ID of the page
  - `payload`: Full page image
- **COMMIT (1)**: Transaction commit marker
  - `page_id`: 0 (unused)
  - `payload`: transaction_id (u64), timestamp (u64)
- **CHECKPOINT (2)**: Checkpoint completion marker
  - `page_id`: 0 (unused)
  - `payload`: checkpoint_lsn (u64)

Commit rule:
- A transaction is committed when a COMMIT frame is durably written.
- Default durability: `fsync(wal)` on commit.

**Torn Write Detection (format v5):**
- Frame header includes `payload_size` for validation
- Recovery ignores incomplete frames (short reads / size truncation)
- Frame type and basic invariants are validated (e.g., page_id != 0 for page frames)

### 4.2 Snapshot reads
On read transaction start:
- capture `snapshot_lsn = wal_end_lsn` using atomic load with acquire semantics (`AtomicU64.load_acquire()`)
- This ensures the reader sees a consistent point-in-time view without acquiring locks
- When reading page P:
  - if wal has a version of P with `lsn <= snapshot_lsn`, return latest such frame
  - else return page from main db file

Maintain in-memory `walIndex: page_id -> list/latest frame offsets` (store latest; optionally chain for multiple versions during long read txns).

**Atomicity guarantee**: The `wal_end_lsn` is an `AtomicU64` that is incremented only after the WAL frame is fully written and the in-memory index is updated. Readers use `load_acquire()` to ensure they see all prior writes.

### 4.3 Checkpointing and WAL Retention
Checkpointed pages are copied to the main database file.

**Reader Protection Rule (ADR-0019):**
- The WAL must **never** be truncated if it contains frames required by an active reader (i.e., frame LSN > `min(active_reader_snapshot_lsn)`).
- If a reader falls far behind, the WAL file may grow indefinitely until the reader finishes.

**Checkpoint Protocol:**
1. Set `checkpoint_pending` flag to block new write transactions.
2. Copy committed pages to main DB file (up to last commit).
3. Determine `safe_truncate_lsn = min(active_readers_snapshot_lsn)`.
   - If no readers, `safe_truncate_lsn = last_commit_lsn`.
4. Write CHECKPOINT frame to WAL.
5. **Conditionally Truncate:**
   - If `safe_truncate_lsn` allows, truncate the WAL file to header-only.
   - If readers are blocking truncation, the WAL remains large until the next checkpoint opportunistically truncates it.
6. Clear `checkpoint_pending` flag.

**Forced Checkpoint (Timeout):**
- If we must checkpoint while readers are active, we proceed with the copy based on the `writer`'s LSN, but we **skip the WAL truncation** step for any portion needed by readers.

**WAL Growth Prevention (see ADR-0024):**
- Implement reader tracking to monitor active readers and their snapshot LSNs
- Introduce configurable timeout for long-running readers to prevent indefinite WAL growth
- Log warnings when readers hold snapshots for extended periods
- Optionally force WAL truncation with appropriate safeguards when readers become too stale

### 4.4 Bulk Load API
Dedicated API for high-throughput data loading with deferred durability.

**Key characteristics:**
- Holds single-writer lock for entire duration
- Batches rows and fsyncs only at configured intervals
- Readers see consistent pre-load state (snapshot isolation)
- Crash during bulk load loses all progress (no partial commits)

See ADR-0017 for detailed design.

**Configuration:**
- `batch_size`: Rows per batch (default: 10000)
- `sync_interval`: Batches between fsync (default: 10)
- `disable_indexes`: Skip index updates during load (default: true, rebuild after)
- `checkpoint_on_complete`: Checkpoint after load finishes (default: true)

### 4.5 Crash recovery
On open:
- scan WAL from last checkpoint
- validate frame invariants (type, payload_size, lsn)
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

### 5.3 Locks and latches (1.0 baseline)
- `schemaLock`: RW lock around catalog changes
- Page cache: per-page latch + global eviction lock
- WAL append: mutex for serializing frame writes (enforces single-writer)

**Note:** The WAL append mutex enforces the single-writer constraint at the storage layer. While the architecture supports only one writer transaction at a time by design, the mutex provides defense-in-depth against programming errors.

Multi-process locking is out of scope for 1.0.

### 5.4 Deadlock detection and prevention
- Writer acquires locks in a consistent order to avoid deadlocks
- Readers never block other readers (snapshot isolation)
- If a deadlock is detected (via timeout), abort the newer transaction
- Document lock acquisition order for all operations

### 5.5 Isolation Levels (LOW-001)
DecentDB implements **Snapshot Isolation (SI)** as the default and only isolation level.

**Semantics:**
- **Snapshot Acquisition**: A transaction acquires its snapshot (`snapshot_lsn`) at the beginning of the first statement (or `BEGIN` if explicit).
- **Visibility**: Queries see all transactions committed before `snapshot_lsn`. They do *not* see uncommitted changes from other transactions or changes committed after `snapshot_lsn`.
- **Own Writes**: A transaction always sees its own uncommitted modifications.

**Anomalies:**
- **Dirty Reads**: Prevented. Readers never see uncommitted data.
- **Non-Repeatable Reads**: Prevented. Repeated reads within a transaction return the same data (unless modified by the transaction itself).
- **Phantoms**: Prevented. Range scans are consistent with the snapshot.
- **Write Skew**: Possible. Two concurrent transactions can modify disjoint rows based on a consistent snapshot state that they both invalidate. (e.g., "maintain at least one doctor on call"). This distinguishes SI from full Serializability.

---

## 6. SQL parsing & compatibility (Postgres-like)
### 6.1 Parser choice
1.0 recommendation:
- Use a Postgres-compatible parser via FFI (e.g., `libpg_query`) to accept familiar syntax.
- Normalize parse trees into DecentDB’s internal AST immediately.

Alternative:
- Use Nim-native `parsesql` for faster iteration, then migrate to libpg_query later.

### 6.2 Supported SQL subset (1.0 baseline)
- DDL: `CREATE TABLE`, `CREATE INDEX`, `CREATE TRIGGER`, `DROP TABLE`, `DROP INDEX`, `DROP TRIGGER`, `CREATE VIEW`, `DROP VIEW`, `ALTER VIEW ... RENAME TO ...`
- DML: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `EXPLAIN`, `EXPLAIN ANALYZE`
- INSERT RETURNING subset:
  - `INSERT ... RETURNING *`
  - `INSERT ... RETURNING <expr[, ...]>`
  - applies to plain INSERT and `INSERT ... ON CONFLICT ...` paths
- UPSERT subset:
  - `INSERT ... ON CONFLICT DO NOTHING` with optional conflict target:
    - no target (`ON CONFLICT DO NOTHING`)
    - column-list target (`ON CONFLICT (col[, ...]) DO NOTHING`)
    - constraint/index-name target (`ON CONFLICT ON CONSTRAINT name DO NOTHING`)
  - `INSERT ... ON CONFLICT ... DO UPDATE` with explicit target:
    - column-list target (`ON CONFLICT (col[, ...]) DO UPDATE SET ...`)
    - constraint/index-name target (`ON CONFLICT ON CONSTRAINT name DO UPDATE SET ...`)
    - optional `WHERE` on `DO UPDATE`
    - expression scope: target table columns and `EXCLUDED.col`
    - unqualified columns in `DO UPDATE` expressions bind to target table
- Aggregate functions: `COUNT(*)`, `COUNT(col)`, `SUM(col)`, `AVG(col)`, `MIN(col)`, `MAX(col)` with `GROUP BY` and `HAVING`
- Scalar functions: `COALESCE`, `NULLIF`, `LENGTH`, `LOWER`, `UPPER`, `TRIM`
- Expression forms: searched/simple `CASE`, `CAST(expr AS type)` (narrow matrix)
- Common Table Expressions (CTE): non-recursive `WITH ...` for `SELECT`
  - CTE names resolve in declaration order and can shadow catalog objects in the statement scope
  - v0 CTE body restrictions: `GROUP BY`/`HAVING`, `ORDER BY`, and `LIMIT/OFFSET` inside CTE bodies are not supported
- Set operations: `UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT`
- Window functions (v0 subset): `ROW_NUMBER() OVER (...)`
  - `PARTITION BY` optional
  - `ORDER BY` inside `OVER (...)` required in 1.0
  - supported only in `SELECT` projection items
- Joins: `LEFT JOIN`, `INNER JOIN` on equality predicates
- Filters: basic comparisons, boolean ops, `BETWEEN`, `IN (...)`, `EXISTS (SELECT ...)` (non-correlated), `LIKE`/`ILIKE` (with `ESCAPE`), string concatenation (`||`)
- CHECK constraints in `CREATE TABLE` (column-level and table-level)
- AFTER trigger subset:
  - events: `INSERT`, `UPDATE`, `DELETE`
  - `FOR EACH ROW` on base tables
  - action form: `EXECUTE FUNCTION decentdb_exec_sql('<single DML SQL>')`
- INSTEAD OF trigger subset:
  - events: `INSERT`, `UPDATE`, `DELETE`
  - `FOR EACH ROW` on views
  - action form: `EXECUTE FUNCTION decentdb_exec_sql('<single DML SQL>')`
- Partial index subset: `CREATE INDEX ... WHERE <indexed_column> IS NOT NULL` for single-column BTREE indexes
- Expression index subset: `CREATE INDEX ... ((<expr>))` for single-expression BTREE indexes
  - allowed expression forms in 1.0:
    - direct column reference
    - `LOWER(col)`, `UPPER(col)`, `TRIM(col)`, `LENGTH(col)`
    - `CAST(col AS INT64|FLOAT64|TEXT|BOOL)`
  - restrictions:
    - exactly one index expression
    - `UNIQUE` expression indexes are not supported
    - partial expression indexes are not supported
- NULL semantics: SQL three-valued logic for `NOT`/`AND`/`OR`, comparisons with `NULL`, `IN (...)`, and `LIKE`/`ILIKE`
  - Predicate results in `WHERE`: only `TRUE` keeps a row; both `FALSE` and `NULL` filter out
- Ordering: `ORDER BY` (multi-column), `LIMIT`, `OFFSET`
- Explicitly unsupported in 1.0 baseline:
  - `WITH RECURSIVE`
  - `INTERSECT ALL`, `EXCEPT ALL`
  - Advanced window functions beyond `ROW_NUMBER()` (e.g., `RANK`, `DENSE_RANK`, `LAG`, frame clauses)
  - `FOR EACH STATEMENT` triggers
  - `NEW`/`OLD` row references in trigger actions
  - targetless `INSERT ... ON CONFLICT DO UPDATE ...` (without conflict target)
  - `UPDATE ... RETURNING`
  - `DELETE ... RETURNING`
  - Partial indexes beyond the v0 subset (`UNIQUE` partial indexes, trigram partial indexes, multi-column partial indexes, arbitrary predicates)
  - Expression indexes beyond the v0 subset (multi-expression keys, unsupported functions/operators, `UNIQUE`, or partial forms)

### 6.3 Parameterization
- `$1, $2, ...` positional (Postgres style) — chosen for the 1.0 baseline
  - Consistent with libpg_query parser choice
  - Simple to implement and test
  - Well-understood by developers familiar with PostgreSQL
- Document and test thoroughly.

---

## 7. Relational features
### 7.1 Primary keys
- Default rowid PK if none specified
- Unique index enforces PK uniqueness
- `INTEGER PRIMARY KEY` implies `NOT NULL`
- `INTEGER PRIMARY KEY` columns support auto-increment: if the column is omitted from an `INSERT`, DecentDB automatically assigns the next sequential ID (see ADR-0036, ADR-0092)
- Explicit values are still accepted and update the internal counter

### 7.2 Foreign keys
- Enforced at statement time (**Note:** differs from SQL standard which enforces at transaction commit)
- Requires index on referenced parent key (enforced by CREATE TABLE)
- Auto-create index on child FK columns if not present
  - Index name: `fk_<table>_<column>_idx`
  - This ensures FK checks are efficient and avoids full table scans

**Statement-time enforcement means:**
- Each INSERT/UPDATE/DELETE statement validates FK constraints immediately
- Violations cause immediate error and statement rollback
- This differs from PostgreSQL/MySQL which defer validation to COMMIT

1.0 actions:
- `ON DELETE`: `RESTRICT` / `NO ACTION`, `CASCADE`, `SET NULL`
- `ON UPDATE`: `RESTRICT` / `NO ACTION`, `CASCADE`, `SET NULL`

Current 1.0 limitations:
- `ON DELETE SET NULL` and `ON UPDATE SET NULL` require nullable child FK columns.
- Deferred constraint checking (transaction-commit time) is not supported.

### 7.3 CHECK constraints
- Supported in `CREATE TABLE` as column or table constraints.
- Enforced at statement-time on `INSERT` and `UPDATE` (including `INSERT ... ON CONFLICT DO UPDATE`).
- SQL semantics: CHECK fails only when the expression evaluates to `FALSE`; `TRUE` and `NULL` pass.
- v0 restrictions:
  - CHECK expressions must reference only columns in the same row/table.
  - CHECK does not allow parameters, aggregate functions, or `EXISTS` in 1.0.
  - `ALTER TABLE ... ADD CONSTRAINT CHECK` is not supported.

---

## 8. Trigram substring search index
### 8.1 Why trigrams
`LIKE '%pattern%'` cannot use a normal B+Tree index efficiently. Trigrams allow:
- candidate retrieval via inverted index
- intersection of posting lists
- final verification using actual substring match

### 8.2 Index data model
For each indexed TEXT column:
- Normalize to uppercase (configurable; 1.0 assumes uppercase inputs are already normalized)
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

**Case Sensitivity Rules:**
- Trigram index stores uppercase-normalized trigrams
- Patterns are uppercased before query processing
- `LIKE '%abc%'` and `LIKE '%ABC%'` use the same trigram index entries
- Case-insensitive matching is the default for trigram-indexed columns
- For case-sensitive search, use B+Tree index with exact match predicates

**Pattern length guardrails**:
- Patterns < 3 characters: never use trigram index (too many matches)
- Patterns 3-5 characters: use trigram only if combined with other filters or if rarest trigram count < threshold
- Patterns > 5 characters: use trigram index, but cap results if rarest trigram exceeds threshold

### 8.4 Broad-pattern guardrails
Maintain postings count per trigram.
- If rarest trigram count exceeds a threshold (e.g., >100k), require additional predicate or cap results.
- Provide an engine setting for thresholds.

### 8.5 Storage format for postings (1.0 baseline)
- Store postings lists in a dedicated B+Tree keyed by trigram:
  - key: trigram
  - value: compressed postings blob (delta-encoded varints)
- Updates (1.0 choice: in-memory buffers):
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
1.0 uses simple heuristics without full statistics collection:
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
     - DecentDB
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
- Bulk load throughput

Track:
- P50/P95 latency
- WAL size growth
- checkpoint time
- recovery time after crash

**Memory Usage Budgets:**
- Peak memory during queries: must not exceed 2x configured cache size
- Query execution memory: abort if exceeds `max_query_memory` (default: 64MB)
- Sort operations: **External Merge Sort** (ADR-0022). Spills to disk if `sort_buffer_size` (default: 16MB) is exceeded. Max 64 spill files (approx 1GB capacity with default buffer).
- Join operations: use nested-loop with index, materialize only when necessary
- Memory tracking: report in benchmark results, fail if >20% over budget

---

## 12. Future compatibility: Npgsql / PostgreSQL wire protocol
Not planned for 1.0. If pursued:
- Implement pgwire subset as a server endpoint
- Add minimal catalog responses for clients
- Maintain dialect compatibility with libpg_query parser

---

## 13. Error handling
### 13.1 Error codes
Define error categories:
- `ERR_IO`: File I/O errors (disk full, permissions, corruption)
- `ERR_CORRUPTION`: Database corruption detected (invalid frame/header, checksum mismatch when applicable)
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
- For corruption: include page ID and invariant details (checksum mismatch when applicable)

---

## 14. Memory management
### 14.1 Memory pools
- Page cache: fixed-size pool of page buffers (configurable, default: 1000 pages)
- Row materialization: reusable buffers per operator to avoid per-row alloc
- Trigram buffers: small per-trigram buffers (max 4KB each)
- Sort buffers: managed pool for external merge sort operations (see ADR-0025)

### 14.2 Memory limits
- Configurable maximum memory usage (default: 256MB)
- Page cache eviction when limit reached (LRU policy)
- Query execution aborts if memory limit exceeded during execution
- Per-query memory limits to prevent single queries from consuming all memory

### 14.3 Out-of-memory handling
- Pre-allocate memory at startup where possible
- If allocation fails:
  - Abort current query with `ERR_INTERNAL`
  - Rollback current transaction
  - Log error with memory usage statistics
- Do not crash the process; allow graceful degradation

### 14.4 Memory leak prevention and monitoring
- Track memory allocations with tags for different subsystems
- Periodic scanning for unreleased resources
- Integration with leak detection tools during testing
- Connection-scoped cleanup for per-connection resources (see ADR-0025)

---

## 15. Schema versioning and evolution
### 15.1 Schema cookie
- Stored in main DB header (page 1)
- Incremented on any DDL operation (CREATE/DROP/ALTER TABLE/INDEX/VIEW)
- Used to detect schema changes and invalidate cached metadata

### 15.2 Backward compatibility
- File format version stored in header
- Engine can read older format versions (read-only compatibility)
- Writing to older formats may trigger automatic upgrade (with user confirmation)

### 15.3 Schema changes (1.0 baseline)
- Supported: CREATE TABLE, CREATE INDEX, CREATE TRIGGER, DROP TABLE, DROP INDEX, DROP TRIGGER, ALTER TABLE
- ALTER TABLE operations: ADD COLUMN, DROP COLUMN, RENAME COLUMN, ALTER COLUMN TYPE
  - Current v0 limitation: ALTER TABLE operations are rejected on tables that define CHECK constraints
  - Current v0 limitation: ALTER TABLE operations are rejected on tables that define expression indexes
  - `RENAME COLUMN` is rejected when dependent views exist
  - `ALTER COLUMN TYPE` supports only `INT64`, `FLOAT64`, `TEXT`, `BOOL` source/target kinds
  - `ALTER COLUMN TYPE` is rejected for PRIMARY KEY columns, FK child columns, and columns referenced by foreign keys
- Trigger operations:
  - `AFTER` triggers on `INSERT`/`UPDATE`/`DELETE` for base tables (`FOR EACH ROW`)
  - `INSTEAD OF` triggers on `INSERT`/`UPDATE`/`DELETE` for views (`FOR EACH ROW`)
  - Trigger action must be `EXECUTE FUNCTION decentdb_exec_sql('<single DML SQL>')` in 1.0
  - `FOR EACH STATEMENT` triggers and `NEW`/`OLD` row references are not supported in 1.0
- Not supported (post-1.0): ADD CONSTRAINT
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
- `wal_sync_mode`: `FULL` (fsync), `NORMAL` (fdatasync), `TESTING_ONLY_UNSAFE_NOSYNC` (requires compile flag)
- `checkpoint_timeout_sec`: Max wait for readers before forced checkpoint (default: 30)
- `trigram_postings_threshold`: Max postings before requiring additional filters (default: 100000)
- `temp_dir`: Directory for temporary sort files (default: system temp)

**Safety Note:** `TESTING_ONLY_UNSAFE_NOSYNC` requires the engine to be compiled with `-d:allowUnsafeSyncMode` flag. This mode provides no durability guarantees and must never be used in production.

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

### 17.3 Merge/rebalance (post-1.0)
- Not implemented in 1.0 to simplify code
- Compaction provides equivalent space recovery
- Future: implement merge for delete-heavy workloads
