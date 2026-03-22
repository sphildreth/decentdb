# ROAD_TO_RUST_PLAN Review - M27
**Date:** 2026-03-22
**Reviewer:** Coding Agent (M27)
**Documents Reviewed:**
- `design/ROAD_TO_RUST_PLAN.md` (source document)
- `design/PRD.md` (product requirements)
- `design/TESTING_STRATEGY.md` (testing requirements)
- 20+ ADRs (Architecture Decision Records)
- `docs/architecture/wal.md`, `storage.md`, `btree.md`
- `crates/decentdb/src/lib.rs` (current stub implementation)

---

## Executive Summary

The ROAD_TO_RUST_PLAN is a well-structured, bottom-up rewrite plan that correctly prioritizes storage foundations (Phase 1) before query processing (Phase 3) and ecosystem parity (Phase 4). The 4-phase approach aligns with the 7 pillars defined in PRD.md. However, the plan has **significant gaps and inconsistencies** that could lead to confusion during implementation:

1. **Stale ADR references** (ADR-0032 is superseded by ADR-0035)
2. **Missing dependency review** (flate2 crate for compression requires an ADR)
3. **Ambiguous concurrency model** (single-process, one writer, multiple readers needs explicit locking invariants)
4. **Incomplete error handling strategy** (no error code catalog)
5. **Insufficient WAL truncation coordination details**
6. **No mention of transaction isolation levels**

These gaps are mostly correctable with targeted ADRs or plan amendments. The core architecture is sound.

---

## 1. Alignment with PRD Pillars

The 7 pillars in PRD.md are well-addressed by the plan:

| Pillar | Plan Coverage | Notes |
|--------|---------------|-------|
| 1. ACID Compliance | ✅ Strong | WAL (1.3), Crash Recovery (1.4), Checkpointing (1.4) address durability. Snapshot isolation via LSN (ADR-0003) |
| 2. Uncompromising Performance | ✅ Good | Zero-copy via `FileExt::read_at` (ADR-0119), lock-free snapshots, LRU cache (1.2) |
| 3. Minimal Disk Footprint | ✅ Good | `#[repr(C)]` pages, varint encoding (2.1), compact BTree (ADR-0035) |
| 4. World-Class Documentation | ⚠️ Weak | Plan mentions rustdoc tests but no dedicated docs slice |
| 5. Best-in-Class Toolings & Bindings | ✅ Good | C-ABI boundary (4.1), Python wiring (4.2), FFI panic safety (ADR-0118) |
| 6. Fantastic CLI Experience | ✅ Good | CLI slice (4.3) with REPL |
| 7. Fast Developer Feedback Loop | ⚠️ Missing | PR checks < 10 min mentioned in PRD but no CI implementation plan |

**Assessment:** The plan correctly prioritizes ACID and performance (Pillars 1 & 2), which is appropriate for a database engine. Documentation (Pillar 4) is underspecified.

---

## 2. Phase 1: Foundation (Storage & I/O) - Detailed Analysis

### Slice 1.1: VFS & Fault Injection ✅

**Strengths:**
- Correct use of `FileExt::read_at` for concurrent reads without Mutex (ADR-0119)
- `FaultyVfs` for crash-injection testing aligns with TESTING_STRATEGY.md §3

**Issues:**
- **None identified** - this is a well-scoped slice with clear directives

**References:**
- ADR-0119: Virtual File System (VFS) and Thread-Safe I/O in Rust

---

### Slice 1.2: Pager & LRU Page Cache ✅

**Strengths:**
- `#[repr(C)]` or `#[repr(align(4096))]` for page size guarantees
- Pin/unpin semantics correctly prevent eviction of in-use pages
- `RwLock` for cache-level synchronization

**Issues:**
1. **No mention of cache size limits**: ADR-0011 specifies fixed-size memory pools with configurable limits. The slice should specify how to enforce memory bounds (what happens when the cache is full and all pages are pinned?)

2. **Ambiguous lock granularity**: The plan says "The cache itself needs a lock for eviction, but page contents should ideally allow concurrent reads." This "ideally" is vague. Need explicit invariant: *Can multiple threads read the same page concurrently?* With `FileExt::read_at` yes, but with in-memory cache copies, no.

3. **Dirty page handling unclear**: Who is responsible for writing dirty pages to WAL before eviction? The pager or the caller?

**References:**
- ADR-0011: Memory Management Strategy
- `docs/architecture/storage.md` §"Page Cache"

---

### Slice 1.3: WAL Writer & Reader ✅⚠️

**Strengths:**
- `AtomicU64` snapshot LSN with acquire/release semantics (ADR-0003)
- `WalIndex` for O(1) page lookup by `page_id`
- fsync on commit for durability

**Issues:**

1. **WAL format inconsistency**: 
   - `docs/architecture/wal.md` shows: `[Type: 1 byte][PageId: 4 bytes][PayloadLen: 4 bytes][Payload: N bytes][Checksum: 8 bytes][LSN: 8 bytes]` = 17 bytes header + payload + 8 bytes checksum + 8 bytes LSN = 25 + N bytes per frame
   - ADR-0033 says: "fixed WAL frame header with type, page_id, payload_size, followed by payload, CRC-32C checksum, and LSN" and "adds 16 bytes of trailer per frame"
   - The plan references ADR-0033 but the total frame overhead is unclear (25 bytes? 16 bytes?).
   - **Action Required:** Clarify the exact byte layout in the plan or reference ADR-0033 with explicit header size.

2. **WAL header format mismatch**:
   - ADR-0068 introduces a "logical end offset" in the WAL header for mmap-based appends
   - But ADR-0119 explicitly says "Do not use mmap for the primary page cache"
   - This creates confusion: Is mmap used for WAL or not? ADR-0068 and ADR-0119 seem contradictory.
   - **Action Required:** Clarify WAL write path: is it pure `write_at` + `fsync` (no mmap), or is mmap used for WAL only?

3. **Missing: WAL frame type enumeration**:
   - Slice 1.3 should define the frame types explicitly: PAGE (0), COMMIT (1), CHECKPOINT (2) as shown in `docs/architecture/wal.md`
   - Missing: ABORT frame type? Savepoint frames?

4. **Missing: WAL buffering/batching**:
   - The plan shows direct VFS calls per frame
   - TESTING_STRATEGY.md §2.3 mentions crash-injection at "wal write frame" and "wal fsync"
   - No mention of write buffering (batch multiple frames before fsync)
   - ADR-0037 (group commit) exists but is not referenced

**References:**
- ADR-0003: Snapshot LSN Atomicity
- ADR-0033: WAL Frame Format
- ADR-0068: WAL header with logical end offset
- ADR-0119: VFS (precludes mmap for page cache)
- ADR-0037: Group commit WAL batching

---

### Slice 1.4: Crash Recovery & Checkpointing ⚠️

**Strengths:**
- WAL scanning for recovery
- Torn write detection (checksum validation)
- Reader-protection rule (ADR-0019)

**Issues:**

1. **WAL truncation coordination is underspecified**:
   - The plan says: "Do not truncate the WAL if `min(active_reader_lsn) < wal_end_lsn`"
   - ADR-0019 says: "Defer entire WAL truncation if any reader needs any part of it"
   - But the plan doesn't explain **how** the checkpoint process coordinates with active readers:
     - Is there a reader registry?
     - How is `min(active_reader_lsn)` tracked atomically?
     - What happens when a reader crashes without deregistering? (Orphaned reader preventing WAL truncation)
   - **Risk:** A "toxic" long-running reader can fill up disk, as warned in ADR-0019

2. **Recovery process incomplete**:
   - The plan says scan from offset 0 to `wal_end_offset`
   - But what if the WAL header is corrupt? Need header validation first
   - What about media recovery (checksum failures on pages during replay)?

3. **Checkpoint scheduling not addressed**:
   - Who triggers checkpoints? Background thread? On-demand?
   - ADR-0058 (background incremental checkpoint worker) is not referenced

**References:**
- ADR-0019: WAL Retention for Active Readers
- ADR-0058: Background incremental checkpoint worker

---

## 3. Phase 2: Data Structures - Detailed Analysis

### Slice 2.1: Record Encoding ⚠️

**Strengths:**
- Comprehensive `Value` enum (Null, Int64, Float64, Bool, Text, Blob, Decimal, Uuid, Timestamp)
- Leb128 varint encoding for space efficiency
- UUID as 16 raw bytes (not 36-char string)
- UTF-8 validity guarantee for Text
- proptest for property-based testing

**Issues:**

1. **ZigZag encoding not mentioned**:
   - `docs/architecture/storage.md` says: "INT64: ZigZag + varint-encoded uint64"
   - The plan only says "Leb128 Varint encoding" which is for unsigned integers
   - For signed integers (Int64), ZigZag encoding is required to map negative values to small unsigned numbers for efficient varint storage
   - **Action Required:** Specify ZigZag encoding for Int64

2. **Decimal and Timestamp encoding unclear**:
   - The plan says: "Timestamp and Decimal are backed by i64 and should also leverage varint encoding where mathematically safe/appropriate, or fixed 8-bytes if required for sortability"
   - This "where mathematically safe/appropriate" is ambiguous
   - Sortability is critical for indexes - if varint is used for Timestamp, ordering must be preserved
   - ADR-0091 (decimal/uuid implementation) and ADR-0114 (native datetime) exist but aren't referenced

3. **Lexicographic sortability not guaranteed**:
   - The plan says: "Ensure encoded byte arrays sort lexicographically correctly where possible, or document where index keys require specialized encoding"
   - This exception clause is dangerous - SQL requires ORDER BY on indexed columns to return sorted results
   - **Action Required:** Define a total ordering for all types or specify which types cannot be lexicographically sorted

4. **Compression not specified for inline values**:
   - `docs/architecture/storage.md` mentions "opportunistically-compressed TEXT/BLOB payloads"
   - The plan doesn't mention compression for inline values, only for overflow pages (Slice 2.3)

**References:**
- ADR-0091: Decimal Uuid Implementation
- ADR-0114: Native datetime timestamp type
- `docs/architecture/storage.md` §"Record Format"
- `docs/user-guide/data-types.md`

---

### Slice 2.2: B+Tree Node Layout & Traversal ⚠️

**Strengths:**
- Binary search within page
- Cursor for forward/backward traversal
- `#[repr(C, packed)]` or explicit byte-slice parsing

**Issues:**

1. **Stale ADR reference - ADR-0032 is superseded**:
   - The plan references ADR-0032 for byte layouts
   - ADR-0032 says: "Status: Superseded by ADR 0035 (FormatVersion 4)"
   - ADR-0035 defines the current FormatVersion 4 with varint-encoded cells
   - **Action Required:** Update reference from ADR-0032 to ADR-0035

2. **ADR-0035 details not reflected in plan**:
   - ADR-0035 specifies: `[key: Varint] [control: Varint] [payload...]` for leaf cells
   - Internal cells: `[key: Varint] [child: Varint]`
   - Control field: `is_overflow = (control & 1)`, `value = (control >> 1)`
   - The plan's Slice 2.2 doesn't mention control field encoding

3. **Prefix compression not implemented**:
   - ADR-0035 mentions: "Prefix Compression: Rejected (Phase 2)" - this is correct for now
   - But ADR-0062 (BTree prefix compression) exists and is deferred
   - The plan should note this is intentionally deferred

4. **Delta key encoding mentioned in docs but not plan**:
   - `docs/architecture/btree.md` says: "if PageFlagDeltaKeys is set, this is stored as the delta from the previous key"
   - The plan doesn't mention delta encoding at all

**References:**
- ADR-0032: B+Tree Page Layout (SUPERSEDED)
- ADR-0035: Compact B+Tree Page Layout v2 (CURRENT)
- ADR-0062: BTree prefix compression (deferred)
- `docs/architecture/btree.md`

---

### Slice 2.3: B+Tree Mutations ⚠️

**Strengths:**
- Insert, update, delete implementation
- Node splitting with even key distribution
- Overflow pages for large values

**Issues:**

1. **Compression dependency requires ADR**:
   - The plan says: "Integrate `zlib` compression (via `flate2` crate) for any Text or Blob that gets pushed to an overflow page"
   - AGENTS.md §5 says: "Adding large 3rd-party dependencies without discussion" requires an ADR
   - `flate2` is a well-known, mature crate but the dependency policy should be followed
   - **Action Required:** Either create an ADR for flate2 or remove compression from the plan

2. **ADR-0031 typo**:
   - ADR-0031 says: "Mirustal header overhead" - this is a typo for "Minimal"
   - The plan references ADR-0031 but should note the typo

3. **Overflow page format inconsistency**:
   - ADR-0031 says: "Each page begins with a 4-byte next pointer and 4-byte data length"
   - `docs/architecture/storage.md` says: "Overflow page: [next_page: 4 bytes][chunk_len: 4 bytes][chunk_bytes...]"
   - These are consistent (next + len + data)

4. **Delete/Update not fully specified**:
   - Does delete mark pages as free immediately or at checkpoint?
   - ADR-0029 (freelist page format) should be referenced
   - The plan mentions "Delete" but not tombstone handling or MVCC

**References:**
- ADR-0031: Overflow Page Format
- ADR-0029: Freelist page format
- ADR-0020: Overflow page format (file doesn't exist - typo?)

---

### Slice 2.4: Trigram Index Storage ⚠️

**Strengths:**
- Trigram generation from strings
- Posting lists as delta-encoded varints
- Posting list intersection

**Issues:**

1. **ADR-0007 not referenced**:
   - ADR-0007 (trigram postings storage strategy) exists and should be the primary reference
   - The plan says "Create `src/search.rs`" but should reference ADR-0007

2. **Trigram generation canonicalization unclear**:
   - How are trigrams generated? All 3-char substrings? Only word-boundary trigrams?
   - ADR-0008 (trigram pattern length guardrails) exists
   - TESTING_STRATEGY.md §2.1 mentions "trigram generation canonicalization" as a test case

3. **Posting list format details missing**:
   - Delta-encoded varints: what delta? (Row ID delta is standard)
   - Compression: zlib? LZ4? Raw delta?
   - ADR-0063 (trigram postings paging format) exists but isn't referenced

**References:**
- ADR-0007: Trigram postings storage strategy
- ADR-0008: Trigram pattern length guardrails
- ADR-0063: Trigram postings paging format

---

## 4. Phase 3: Relational Core - Detailed Analysis

### Slice 3.1: System Catalog ✅

**Strengths:**
- Schema management
- Schema version tracking (cookie)

**Issues:**

1. **Catalog schema not defined**:
   - What tables/stored procedures exist in the system catalog?
   - `sqlite_master` equivalent? Postgres `pg_*` equivalent?
   - ADR-0070 (views catalog and semantics) exists but isn't referenced

2. **Schema migration not addressed**:
   - When format version changes (e.g., FormatVersion 3 → 4 in ADR-0035), how is migration performed?
   - ADR-0035 says: "No automatic migration provided (requires rebuild)"
   - This should be explicitly stated

**References:**
- ADR-0070: Views catalog and semantics
- ADR-0036-catalog-constraints-index-metadata.md (catalog constraints)

---

### Slice 3.2: SQL Parsing ✅

**Strengths:**
- libpg_query for Postgres-compatible syntax
- Strongly-typed Rust Statement enum from AST JSON

**Issues:**

1. **libpg_query FFI complexity underestimated**:
   - ADR-0035 says: "Adds a C dependency that must be available at build time"
   - The plan doesn't mention:
     - How to obtain libpg_query (submodule? system library? vendored?)
     - Build system integration for the C library
     - Thread safety of the parser (is it safe to parse from multiple threads simultaneously?)

2. **AST JSON → Rust enum mapping not specified**:
   - libpg_query returns protobuf/JSON - need a translation layer
   - How comprehensive is the Statement enum? All of Postgres SQL?
   - PRD.md Pillar 5 says: Python, .NET, Go, Java, Node, Dart - doesn't specify SQL coverage

3. **Postgres-specific features not scoped**:
   - What Postgres features are supported? CTEs? Window functions? Recursive CTEs?
   - ADR-0078 (CTE non-recursive scope) and ADR-0107 (recursive CTE execution) exist

**References:**
- ADR-0035-sql-parser-libpg-query.md
- ADR-0078: CTE non-recursive scope v0
- ADR-0107: Recursive CTE execution

---

### Slice 3.3: Volcano Query Planner ⚠️

**Strengths:**
- Iterator-based execution model
- Plan tree definition
- Buffer reuse for minimal allocations

**Issues:**

1. **Volcano model may not be optimal for Rust**:
   - Volcano (pull-based iterators) is classic but creates one virtual function call per row
   - Rust's async/await or channels-based parallelism could be more efficient
   - However, for a 1.0.0, Volcano is acceptable as it's well-understood

2. **Planning strategy unclear**:
   - Rule-based? Cost-based (ADR-0112 exists)?
   - Index selection logic?
   - Join reordering for multi-table queries?

3. **No mention of transaction isolation levels**:
   - READ COMMITTED? SNAPSHOT (SERIALIZABLE)?
   - ADR-0023 (isolation level specification) exists but isn't referenced
   - The "snapshot LSN" mechanism implies SNAPSHOT isolation, but it's not explicitly stated

**References:**
- ADR-0023: Isolation level specification
- ADR-0038: Cost-based optimization deferred
- ADR-0112: Cost-based optimizer with stats

---

### Slice 3.4: DML Execution & Constraints ⚠️

**Strengths:**
- INSERT, UPDATE, DELETE implementation
- NOT NULL, UNIQUE, FK constraints
- Statement-level rollback

**Issues:**

1. **Statement-level rollback complexity**:
   - The plan says: "If an insert fails a constraint on row 5, rows 1-4 must be reverted without aborting the entire transaction"
   - This is non-trivial for bulk inserts
   - How are partial rollbacks implemented? Savepoints?
   - ADR-0110 (savepoints) exists but isn't referenced

2. **FK constraint enforcement timing unclear**:
   - ADR-0009 (FK enforcement timing) exists
   - Deferred vs immediate enforcement?

3. **ON DELETE/UPDATE actions not specified**:
   - CASCADE? SET NULL? RESTRICT? NO ACTION?
   - ADR-0081 (FK on delete actions v0) exists

4. **Conflict resolution missing**:
   - INSERT ON CONFLICT (UPSERT)?
   - ADR-0075 (insert on conflict do nothing) and ADR-0076 (insert on conflict do update) exist

**References:**
- ADR-0009: Foreign key enforcement timing
- ADR-0081: Foreign key on delete actions v0
- ADR-0110: Savepoints
- ADR-0075: Insert on conflict do nothing v0
- ADR-0076: Insert on conflict do update v0

---

## 5. Phase 4: Ecosystem Parity - Detailed Analysis

### Slice 4.1: C-ABI Boundary ⚠️

**Strengths:**
- `catch_unwind` on every extern "C" function (ADR-0118)
- C-compatible error codes
- Thread-local error buffer

**Issues:**

1. **Error code catalog missing**:
   - The plan mentions `DDB_ERR_PANIC` but doesn't define the full error code enum
   - `docs/api/error-codes.md` exists but what codes are implemented?
   - Need a comprehensive error code catalog (out of memory, not found, constraint violation, etc.)

2. **API surface not documented**:
   - What functions are exported?
   - How does the C API map to Rust internal types?
   - ADR-0039 (dotnet C API design) exists - should be generalized for all bindings

3. **Pointer lifetime management unclear**:
   - Who owns returned pointers? Borrowed data vs copied data?
   - ADR-0118 says: "Raw pointers must be explicitly converted using Box::into_raw and Box::from_raw"
   - But what about string results? Blob results? Memory ownership?

**References:**
- ADR-0118: Rust FFI & Panic Safety Strategy
- ADR-0039: Dotnet C API design
- `docs/api/error-codes.md`

---

### Slice 4.2: Python Crash/Differential Test Wiring ⚠️

**Strengths:**
- Hooking existing Python test harness to Rust `.so`
- Crash-injection tests overnight

**Issues:**

1. **Test harness doesn't exist**:
   - `tests/harness/` was mentioned but `glob` found no Python files
   - TESTING_STRATEGY.md §4 defines a harness structure but it's not implemented
   - The plan assumes the harness exists but it may need to be built

2. **Differential testing scope unclear**:
   - TESTING_STRATEGY.md §2.4 says compare against PostgreSQL
   - What SQL subset? How are differences handled (e.g., different NULL semantics)?

3. **CI integration not specified**:
   - Where do these tests run? GitHub Actions?
   - PRD says PR checks must complete < 10 min - overnight tests are fine, but basic smoke tests?

**References:**
- TESTING_STRATEGY.md §2.3, §2.4
- `docs/development/testing.md`

---

### Slice 4.3: DecentDB CLI ✅

**Strengths:**
- Clap for CLI argument parsing
- Subcommands: exec, repl, import, export, checkpoint, save-as
- REPL with rustyline/reedline

**Issues:**

1. **No mention of interactive transactions in REPL**:
   - Does REPL support BEGIN/COMMIT?
   - Autocommit mode?

2. **Import/Export formats not specified**:
   - CSV? JSON? PostgreSQL copy protocol?
   - ADR-0027 (bulk load API) and ADR-0027-bulk-load-api-specification exist

**References:**
- ADR-0027: Bulk load API specification
- `docs/api/cli-reference.md`

---

### Slice 4.4: 1.0.0 Release Verification ⚠️

**Strengths:**
- CI pipeline verification
- Benchmark comparison against Nim engine
- Final docs

**Issues:**

1. **No benchmark suite defined**:
   - ADR-0014 specifies: Point lookup P95 < 10ms, FK join P95 < 100ms, etc.
   - No mention of how to measure these

2. **Migration path from Nim engine not addressed**:
   - How do existing .ddb files migrate?
   - Is the Rust engine format-compatible with Nim?

3. **Release criteria not defined**:
   - What constitutes "done"? All slices complete? All tests pass?
   - PRD Pillar 7 says PR checks < 10 min - is there a CI matrix?

---

## 6. Cross-Cutting Concerns

### 6.1 Concurrency Model

**Current State:** The plan says "single process with one writer and multiple concurrent reader threads" (AGENTS.md §1).

**Issues:**
1. **Writer serialization not explicit**: Who ensures only one writer? A mutex? A channel?
2. **Reader/writer coordination not specified**: Do writers block readers? Do readers block writers?
3. **ADR-0055 (thread safety and snapshot context)** exists but isn't referenced

**Risk:** High - concurrent access bugs are subtle and can cause data corruption.

**Recommendation:** Add a dedicated slice or section on the concurrency control module (who holds locks, when, and for how long).

---

### 6.2 Dependency Management

**Current State:** AGENTS.md §5 requires ADRs for "Adding large 3rd-party dependencies."

**Identified dependencies in the plan:**
1. `flate2` (for compression) - **NO ADR**
2. `lru` crate (optional, for LRU cache) - mentioned but may write custom
3. `libpg_query` (C library for SQL parsing) - ADR-0035 acknowledges this
4. `clap` (CLI) - Already in workspace dependencies
5. `proptest` (property testing) - Already in dev-dependencies
6. `rustyline` or `reedline` (REPL) - mentioned but not in Cargo.toml

**Risk:** Medium - flate2 is a well-known crate but dependency proliferation should be monitored.

---

### 6.3 Error Handling

**Current State:** ADR-0010 (error handling strategy) exists but isn't referenced in the plan.

**Issues:**
1. No unified error type (`anyhow` vs `thiserror` - both are in workspace dependencies)
2. No error propagation strategy (should errors be recoverable?)
3. No mention of corruption detection and read-only mode

**Recommendation:** Specify error handling strategy explicitly.

---

### 6.4 Metrics & Observability

**Current State:** Not mentioned anywhere.

**Missing:**
1. How to monitor cache hit rates?
2. How to debug WAL growth?
3. How to profile query execution?

ADR-0045 (dotnet SQL observability) exists but is .NET-specific.

---

## 7. Summary of Issues by Severity

### Critical (Must Fix Before Implementation)

| Issue | Location | Description |
|-------|----------|-------------|
| Stale ADR reference | Slice 2.2 | ADR-0032 is superseded by ADR-0035 |
| ZigZag encoding missing | Slice 2.1 | Int64 requires ZigZag for signed varint |
| WAL format inconsistency | Slice 1.3 | ADR-0033 vs docs architecture discrepancy |
| mmap contradiction | Slice 1.3 | ADR-0119 says no mmap, ADR-0068 enables it for WAL |
| Compression dependency | Slice 2.3 | flate2 crate added without ADR |
| Concurrency model vague | All | Single-writer multi-reader not explicitly locked |

### High (Should Fix Before Phase Complete)

| Issue | Location | Description |
|-------|----------|-------------|
| Cache eviction policy | Slice 1.2 | Memory limits and eviction triggers unclear |
| Reader registry | Slice 1.4 | WAL truncation coordination incomplete |
| Error code catalog | Slice 4.1 | DDB_ERR_PANIC referenced but no full enum |
| Transaction isolation | Slice 3.3 | SNAPSHOT assumed but not stated |
| API surface | Slice 4.1 | C-ABI function signatures not documented |

### Medium (Nice to Have for 1.0.0)

| Issue | Location | Description |
|-------|----------|-------------|
| Delta key encoding | Slice 2.2 | Mentioned in docs but not plan |
| Prefix compression | Slice 2.2 | ADR-0062 deferred but should note |
| FK actions | Slice 3.4 | ON DELETE/UPDATE not specified |
| REPL transactions | Slice 4.3 | REPL autocommit vs manual |
| Observability | All | No metrics/observability mentioned |

---

## 8. Recommendations

### 8.1 Immediate Actions

1. **Update ADR references in plan**: Replace ADR-0032 with ADR-0035 throughout
2. **Clarify WAL architecture**: Resolve mmap contradiction between ADR-0068 and ADR-0119
3. **Specify ZigZag encoding**: Add to Slice 2.1 Record Encoding
4. **Create ADR for flate2**: Or remove compression from overflow pages
5. **Define concurrency invariants**: Explicit locking strategy for single-writer multi-reader

### 8.2 Plan Enhancements

1. Add a "Concurrency Control" section to Phase 1 (before Slice 1.2)
2. Add error code catalog to Phase 4 Slice 4.1
3. Add "Observability" slice to Phase 3 or 4
4. Specify differential testing SQL subset
5. Add migration strategy for Nim → Rust format

### 8.3 Process Improvements

1. Cross-reference all ADRs explicitly in plan slices
2. Add "Definition of Done" checklist to each slice
3. Add "Dependencies" section to each slice (what crates/ADRs are required)
4. Add a glossary for terms (LSN, Latch, Pin, etc.)

---

## 9. References

### Primary Documents
- `design/ROAD_TO_RUST_PLAN.md` - Source document under review
- `design/PRD.md` - Product requirements (7 pillars)
- `design/TESTING_STRATEGY.md` - Testing requirements

### Architecture Decision Records (by ADR number)
- ADR-0003: Snapshot LSN Atomicity
- ADR-0011: Memory Management Strategy
- ADR-0014: Performance Targets
- ADR-0019: WAL Retention for Active Readers
- ADR-0023: Isolation Level Specification
- ADR-0026: Race Condition Testing Strategy
- ADR-0031: Overflow Page Format
- ADR-0032: B+Tree Page Layout (SUPERSEDED)
- ADR-0033: WAL Frame Format
- ADR-0035: Compact B+Tree Page Layout v2
- ADR-0035-sql-parser-libpg-query.md: SQL Parser Choice
- ADR-0055: Thread Safety and Snapshot Context
- ADR-0058: Background Incremental Checkpoint Worker
- ADR-0068: WAL Header End Offset
- ADR-0118: Rust FFI & Panic Safety Strategy
- ADR-0119: VFS and Thread-Safe I/O

### Documentation
- `docs/architecture/wal.md`
- `docs/architecture/storage.md`
- `docs/architecture/btree.md`
- `docs/development/testing.md`
- `docs/api/error-codes.md`

---

*End of Review*
