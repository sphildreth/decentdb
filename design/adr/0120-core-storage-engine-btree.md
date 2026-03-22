# ADR-0120: Core Storage Engine Architecture (B+Tree vs. LSM-Tree)

## Status
Accepted

## Context
As DecentDB undergoes a ground-up rewrite in Rust with the intent to function as a general-purpose embedded database (aiming to compete with or replace SQLite), the foundational storage architecture must be explicitly chosen and documented. The two primary candidates for durable disk storage are **B+Trees** (used by SQLite, PostgreSQL) and **LSM-Trees** (Log-Structured Merge Trees, used by RocksDB, LevelDB).

DecentDB's primary design goals (as outlined in the 7 Pillars) include:
1. **Uncompromising ACID durability**
2. **Fast, predictable reads** (single-writer / multi-reader concurrency)
3. **Small disk footprint and memory efficiency**
4. **General-purpose relational (SQL) access patterns**, which rely heavily on fast range scans and localized secondary index lookups.

## Decision
We will use a **B+Tree** architecture (specifically, an optimized paged B+Tree) as the core storage engine for DecentDB, rejecting the LSM-Tree architecture.

### Rationale
1. **Read Performance & Predictability:** LSM-Trees suffer from read amplification because a single read might need to check the MemTable and multiple levels of disk-based SSTables. For a general-purpose SQL database, predictable $O(\log_b N)$ read latency is critical. A B+Tree guarantees a deterministic number of page fetches per lookup.
2. **Range Queries:** Relational databases frequently execute range scans (e.g., `WHERE age BETWEEN 20 AND 30` or table scans). B+Trees store all data in linked leaf nodes, making sequential range scans optimal. LSM-Trees must perform a merge-sort across multiple SSTable levels on the fly during a range scan, which degrades performance.
3. **Resource Contention (Compaction):** LSM-Trees require background threads to continuously compact and merge SSTables. This background I/O and CPU usage can cause unpredictable latency spikes and resource starvation in constrained embedded environments (e.g., mobile devices, edge nodes). B+Trees perform localized page splits, which are deterministic and bounded.
4. **Caching Synergy:** The fixed-size 4KB/8KB B+Tree pages map perfectly onto DecentDB's internal LRU Page Cache and the OS-level page cache.

### Optimizations
To counteract the traditional weaknesses of B+Trees (write amplification and bloat), DecentDB will employ:
- **Varint Encoding:** Heavy use of LEB128/ZigZag encoding for integers, decimals, and timestamps to maximize the branching factor (keys per page).
- **Out-of-band Overflow Pages:** Large `TEXT` and `BLOB` values (>512 bytes) will be pushed to a separate chain of overflow pages and automatically compressed via `zlib`, preserving the density of the main B-Tree nodes.
- **WAL + Lock-free Reads:** The use of `read_at` (ADR-0119) and a WAL index will allow concurrent readers to bypass read locks entirely, solving a major SQLite concurrency limitation.

## Consequences
- **Positive:** Read performance will be highly predictable. Memory usage will be tightly bounded by the LRU cache size rather than unbounded MemTables. Range queries will be natively fast.
- **Negative:** Write amplification will be higher than an LSM-Tree, as modifying a single byte may require flushing an entire 4KB page to the WAL.
- **Negative:** Fragmentation can occur over time as pages split and merge, potentially requiring an eventual `VACUUM` command to reclaim optimal density.