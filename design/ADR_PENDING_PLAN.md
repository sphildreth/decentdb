# Deferred ADR Implementation Plan

**Date:** 2026-02-08
**Purpose:** Track deferred ADRs so they are not lost after the 1.0.0 release.

This document captures all ADRs that were reviewed during the 1.0.0 release process
and assigned **Deferred** status. Each entry explains what the ADR proposes, why it
was deferred, its dependencies, and an implementation priority for post-1.0 work.

---

## Priority Legend

| Priority | Meaning |
|----------|---------|
| **P1 — Next** | Implement before 1.1.0; directly impacts database size or correctness |
| **P2 — Soon** | Implement when concurrency/performance work begins |
| **P3 — Later** | Implement when the need arises or as opportunistic improvements |

---

## P1 — Storage Efficiency (Database Size Reduction)

DecentDB databases are currently ~50% larger than SQLite for comparable data. The
three ADRs below directly address the largest contributors to that overhead. They
should be implemented together as a coordinated storage optimization effort.

### ADR-0061: Typed/Comparable Index Key Encoding for TEXT/BLOB

- **File:** `design/adr/0061-typed-index-key-encoding-text-blob.md`
- **Status:** Deferred
- **Problem:** TEXT and BLOB index keys are stored as CRC32C hashes (8-byte `uint64`).
  This causes three issues:
  1. **Hash collisions** — the engine must post-verify every index lookup by reading the
     actual row, adding I/O overhead
  2. **No range queries** — hash keys have no ordering semantics, so `WHERE name > 'M'`
     cannot use an index
  3. **Duplicated data** — the original value is stored in the B+Tree leaf AND the hash
     is stored as the key, wasting space
- **Proposed solution:** Replace CRC32C hash keys with a typed, comparable binary
  encoding that preserves sort order. This enables direct equality/range comparisons
  in the B+Tree without post-verification.
- **Dependencies:** ADR-0032 (B+Tree layout), ADR-0020 (overflow pages)
- **Impact on DB size:** Eliminates duplicate storage of TEXT/BLOB values in indexes.
  Estimated **15–25% size reduction** for databases with TEXT-heavy indexes.
- **Risks:**
  - Persistent format change (requires format version bump + migration path)
  - Larger keys in B+Tree nodes may reduce fanout (mitigated by ADR-0062)
  - More expensive key comparisons (byte-by-byte vs. integer compare)
- **Implementation notes:**
  - `src/storage/storage.nim` `indexKeyFromValue()` — replace CRC32C with typed encoding
  - `src/engine.nim` — remove post-verification workaround for TEXT/BLOB indexes
  - `src/btree/btree.nim` — update key comparison to handle variable-length typed keys

### ADR-0062: B+Tree Prefix Compression

- **File:** `design/adr/0062-btree-prefix-compression.md`
- **Status:** Deferred
- **Problem:** B+Tree pages store each key independently. For indexes on columns with
  shared prefixes (e.g., URLs, file paths, email addresses), this wastes significant
  space and reduces page fanout, increasing tree depth and I/O.
- **Proposed solution:** Store a shared prefix per B+Tree page and delta-encode
  individual keys relative to that prefix. This reduces per-key storage and improves
  fanout (more keys per page = shallower tree = fewer reads).
- **Dependencies:** ADR-0032 (B+Tree layout), ADR-0035 (B+Tree layout v2)
- **Impact on DB size:** Estimated **5–15% size reduction** depending on key
  distribution. Most effective on TEXT indexes with common prefixes.
- **Risks:**
  - Persistent format change (page layout change, requires version bump)
  - Increased complexity in page split/merge operations
  - Decode overhead on every page read (mitigated by in-memory decompression)
- **Implementation notes:**
  - `src/btree/btree.nim` — page encoding/decoding, cell format
  - Should be implemented **after** ADR-0061 since typed TEXT keys will benefit most
    from prefix compression
  - Consider per-page prefix vs. per-node prefix tradeoffs

### ADR-0063: Trigram Postings Paging/Streaming Storage Format

- **File:** `design/adr/0063-trigram-postings-paging-format.md`
- **Status:** Deferred
- **Problem:** High-frequency trigrams produce large postings lists that are currently
  loaded entirely into memory. The existing bounded decode (`decodePostingsUpTo`) with
  scan fallback is a workaround, not a durable solution. Large postings also consume
  significant B+Tree leaf space.
- **Proposed solution:** Page/stream trigram postings across multiple storage pages
  instead of storing them monolithically in a single B+Tree value. This reduces memory
  usage for high-frequency trigrams and allows incremental reads.
- **Dependencies:** ADR-0007 (trigram storage), ADR-0052 (trigram durability — Accepted)
- **Impact on DB size:** Reduces overhead from oversized B+Tree values for common
  trigrams. Estimated **5–10% size reduction** for databases with trigram indexes on
  large text columns.
- **Risks:**
  - More complex storage/rebuild/checkpoint semantics for trigram data
  - Requires changes to trigram query execution (streaming reads vs. full decode)
  - Persistent format change
- **Implementation notes:**
  - `src/search/search.nim` — `encodePostingsSorted()`, `decodePostings()`,
    `decodePostingsUpTo()`, `getTrigramPostings()`
  - Consider overflow page chains for postings > page size
  - Delta-varint encoding is already in place; paging adds a chunking layer on top

### Combined size reduction estimate

| ADR | Mechanism | Size reduction |
|-----|-----------|---------------|
| 0061 | Eliminate duplicate TEXT/BLOB storage in indexes | 15–25% |
| 0062 | Prefix compression reduces per-key overhead | 5–15% |
| 0063 | Paged trigram postings reduce oversized values | 5–10% |
| **Total** | | **~25–40% reduction** |

These three changes together should bring DecentDB's database size to within 10–20%
of SQLite for typical workloads, closing the current ~50% gap.

---

## P2 — Concurrency and Performance

These ADRs prepare the engine for improved concurrency under the existing
one-writer / many-readers model. They are not required for correctness at the
current concurrency level but will become important as usage scales.

### ADR-0051: Freelist Atomicity During Checkpoint

- **File:** `design/adr/0051-freelist-atomicity.md`
- **Status:** Deferred
- **Problem:** Freelist head updates need atomicity guarantees for future multi-reader
  support. Current single-threaded guarantees are in place (rollbackLock,
  txnAllocatedPages tracking), but the design explicitly defers multi-reader safety.
- **Why deferred:** The ADR itself states "Defer implementation until 1.x." Current
  single-process single-writer model does not expose the race conditions described.
- **Dependencies:** ADR-0029 (freelist format), ADR-0018 (reader management)
- **Blocked by:** Nothing — can be implemented independently

### ADR-0053: Fine-Grained WAL Locking Strategy

- **File:** `design/adr/0053-fine-grained-wal-locking.md`
- **Status:** Deferred
- **Problem:** Single coarse-grained WAL mutex serializes all operations. Under high
  concurrency, this becomes a bottleneck.
- **Proposed solution:** Decompose into `appendLock` → `indexLock` → `syncLock` with
  strict ordering. Enable group commit batching before fsync.
- **Current state:** `indexLock` is implemented; `appendLock` and `syncLock` are not.
  The generic `lock` field still serves as the primary mutex.
- **Why deferred:** Limited benefit in current single-writer model. Becomes important
  if/when multi-writer or high-throughput single-writer scenarios arise.
- **Dependencies:** None

### ADR-0054: Lock Contention Improvements

- **File:** `design/adr/0054-lock-contention-improvements.md`
- **Status:** Deferred
- **Problem:** Contention on catalog lock (schemaLock) and pager cache locks during
  mixed read/write workloads.
- **Current state:** Page cache sharding with splitmix64 and per-shard locks is
  implemented (ADR-0059 — Accepted). RWLock and Copy-on-Write catalog are NOT
  implemented.
- **Why deferred:** Sharding addressed the most impactful contention source. RWLock
  and COW catalog are incremental improvements.
- **Dependencies:** None

### ADR-0055: Thread-Safety Contract and Snapshot Context Handling

- **File:** `design/adr/0055-thread-safety-and-snapshot-context.md`
- **Status:** Deferred
- **Problem:** Threading semantics for Db connections need explicit contracts and
  enforcement. Currently documented as "one connection per thread" but not enforced
  in code.
- **Why deferred:** Current usage model works; enforcement is a hardening task.
- **Dependencies:** ADR-0019 (WAL retention), ADR-0023 (isolation levels)

### ADR-0057: Transactional Freelist/Header Updates

- **File:** `design/adr/0057-transactional-freelist-header-updates.md`
- **Status:** Deferred
- **Problem:** `allocatePage`/`freePage` mutations and header fsync occur outside the
  WAL during transactions. A crash after header fsync but before WAL commit can reflect
  uncommitted structural state.
- **Current mitigation:** In-memory transaction tracking (`beginTxnPageTracking`,
  `rollbackTxnPageAllocations`) handles rollback, but this is not crash-safe.
- **Why deferred:** In practice, the window for this crash scenario is extremely narrow
  and has not been observed. Full WAL-logging of freelist operations adds significant
  complexity and WAL traffic.
- **Dependencies:** ADR-0051 (freelist atomicity)

### ADR-0058: Background/Incremental Checkpoint Worker

- **File:** `design/adr/0058-background-incremental-checkpoint-worker.md`
- **Status:** Deferred
- **Problem:** Synchronous checkpointing causes latency spikes. Long-running readers
  delay WAL truncation.
- **Why deferred:** Current checkpoint triggers (bytes, time, memory threshold) provide
  adequate behavior for most workloads. Background checkpointing adds threading
  complexity.
- **Dependencies:** ADR-0004 (checkpoint strategy), ADR-0019 (WAL retention)

---

## P3 — Future Considerations

No ADRs currently fall in this category. If new optimization proposals arise post-1.0,
they should be added here with the same structure.

---

## Dependency Graph

```
ADR-0061 (Typed Key Encoding)
  └── ADR-0062 (Prefix Compression) — benefits most from typed keys
        └── (no further deps)

ADR-0063 (Trigram Postings Paging)
  └── ADR-0052 (Trigram Durability) ✅ Accepted

ADR-0051 (Freelist Atomicity)
  └── ADR-0057 (Transactional Freelist/Header)
        └── (no further deps)

ADR-0053 (Fine-Grained WAL Locking)
ADR-0054 (Lock Contention)
ADR-0055 (Thread-Safety Contract)
ADR-0058 (Background Checkpoint)
  — All independent; can be implemented in any order
```

---

## Recommended Implementation Order

1. **ADR-0061** — Typed key encoding (foundation for ADR-0062; largest size win)
2. **ADR-0062** — Prefix compression (builds on 0061; second-largest size win)
3. **ADR-0063** — Trigram postings paging (independent; third size win)
4. **ADR-0053** — Fine-grained WAL locking (when concurrency work begins)
5. **ADR-0054** — Lock contention improvements (after 0053)
6. **ADR-0051** — Freelist atomicity (when multi-reader hardening begins)
7. **ADR-0057** — Transactional freelist/header (after 0051)
8. **ADR-0055** — Thread-safety contract (documentation + enforcement)
9. **ADR-0058** — Background checkpoint (when checkpoint latency becomes an issue)
