# DecentDB vs LevelDB: When to Choose Which

This document helps developers decide between **DecentDB** and **LevelDB** for embedded storage workloads. These two systems operate at different abstraction levels, so the choice is less about features and more about what you want to build *on top* of your storage layer.

> **Versions compared:** DecentDB 2.0.0 vs LevelDB 1.23 (as of 2024).
>
> **See also:** [SQL Feature Matrix](sql-feature-matrix.md) for DecentDB's full SQL surface, and [DecentDB vs SQLite](decentdb-vs-sqlite.md) for the comparison with SQLite.

## They Operate at Different Layers

- **LevelDB** is a **key-value storage engine**. It stores byte-string keys and byte-string values. It has no concept of tables, columns, SQL, schemas, indexes, or queries. It is a building block.
- **DecentDB** is a **relational database**. It stores typed rows in tables with columns, supports SQL queries, indexes, joins, constraints, and transactions. It is a complete product.

LevelDB was created at Google and inspired many derivatives including RocksDB, HyperLevelDB, and others. If you want DecentDB's feature set on top of LevelDB, you would need to build it yourself.

## At a Glance

| Dimension | DecentDB | LevelDB |
|-----------|----------|---------|
| **Abstraction level** | Relational database (SQL) | Key-value storage engine |
| **Data model** | Tables, rows, columns, types | Arbitrary key-value byte pairs |
| **Query language** | SQL | None (get/put/delete API) |
| **Indexing** | B-tree secondary indexes, trigram, expression, covering | Single sorted key-space; secondary indexes must be built manually |
| **Durability** | WAL + fsync-on-commit, always | WAL + configurable sync |
| **Architecture** | B-tree | LSM-tree (Log-Structured Merge-tree) |
| **Write path** | In-place page updates | Append-only memtable, background compaction |
| **Concurrency** | One writer, many concurrent reader threads | Single-threaded (thread-safe but serialized) |
| **Transactions** | Full ACID (SQL-level) | Atomic batches (WriteBatch) |
| **Snapshots** | Transaction-level consistency | Point-in-time snapshots |
| **Compression** | None (planned) | Snappy (default), Zstd |
| **Compaction** | None (B-tree manages space in-place) | Background compaction is central to the design |
| **Bindings** | C ABI, Rust, Python, .NET, Go, Java, Node.js, Dart | C++, C, Java, Go, Python, Node.js, and many others |
| **License** | MIT or Apache-2.0 | BSD-3-Clause |
| **Binary size** | ~2-3 MB | ~200-400 KB |
| **Platform support** | Tier 1 Rust platforms | Windows, macOS, Linux, mobile |
| **Implementation language** | Rust | C++ |

## Architectural Differences

### B-tree (DecentDB) vs LSM-tree (LevelDB)

This is the fundamental design difference. It affects write throughput, read latency, space amplification, and operational behavior.

**DecentDB (B-tree):**
- Writes update pages in place (via WAL for durability)
- Reads are direct index seeks — O(log n) B-tree traversal
- Predictable read latency, no background work interfering
- Write amplification from page-level WAL + checkpoint

**LevelDB (LSM-tree):**
- Writes go to an in-memory memtable, then flush to sorted SST files on disk
- Background compaction merges SST files in levels
- Good write throughput (sequential I/O pattern)
- Reads may need to check multiple SST levels
- Compaction causes periodic I/O and space amplification

```
DecentDB write path:
  SQL INSERT → B-tree page lookup → WAL append → in-place page update → fsync

LevelDB write path:
  Put(key, value) → memtable → WAL append → memtable full → flush to L0 SST
  → background compaction merges L0 → L1 → L2 → ...
```

### LevelDB vs RocksDB

LevelDB is the predecessor to RocksDB. Key differences:

| Feature | LevelDB | RocksDB |
|---------|---------|---------|
| **Thread safety** | Single-threaded writes | Concurrent writes |
| **Compaction** | Single-threaded | Multi-threaded |
| **Features** | Core LSM | Many extensions (transactions, column families, etc.) |
| **Complexity** | Simpler, smaller | More features, larger |
| **Origin** | Google | Facebook |

If you're considering LevelDB, also consider whether RocksDB's additional features would benefit you.

### When the LSM-tree wins

LSM-trees excel at write-heavy workloads where:
- You are ingesting large volumes of key-value pairs
- Keys are roughly sequential or you don't need instant read-after-write visibility
- You can tolerate background compaction I/O
- Your read workload is primarily point lookups

### When the B-tree wins

B-trees excel at:
- Mixed read/write workloads where read latency must be predictable
- Range scans (sequential key reads from B-tree leaves)
- Workloads where background compaction interference is unacceptable
- Scenarios where space amplification from compaction is a concern

## When LevelDB Is the Better Fit

### 1. You need a key-value store, not SQL

LevelDB gives you `Get(key)`, `Put(key, value)`, and `Delete(key)`. There is no query language, no `WHERE` clause, no `JOIN`. If your data model is naturally key-value, LevelDB's simpler API is an advantage.

```cpp
// LevelDB: Simple key-value operations
leveldb::DB* db;
leveldb::Options options;
options.create_if_missing = true;
leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);

std::string value;
db->Get(leveldb::ReadOptions(), "key1", &value);
db->Put(leveldb::WriteOptions(), "key2", "value2");
db->Delete(leveldb::WriteOptions(), "key1");
```

### 2. You need a small, focused dependency

LevelDB is ~20K lines of C++. It does one thing (key-value storage) and does it well. If you want minimal complexity and a small attack surface, LevelDB is attractive.

### 3. You need built-in compression

LevelDB supports Snappy compression by default, with Zstd also available:

```cpp
leveldb::Options options;
options.compression = leveldb::kSnappyCompression;  // Default
// or
options.compression = leveldb::kZstdCompression;
options.zstd_compression_level = 3;
```

DecentDB does not currently have built-in compression.

### 4. You need point-in-time snapshots

LevelDB provides consistent snapshots:

```cpp
// Create snapshot
const leveldb::Snapshot* snapshot = db->GetSnapshot();

// Read from snapshot (sees database state at snapshot creation)
leveldb::ReadOptions options;
options.snapshot = snapshot;
std::string value;
db->Get(options, "key1", &value);

// Release snapshot when done
db->ReleaseSnapshot(snapshot);
```

### 5. You need atomic batch writes

```cpp
// LevelDB: Atomic batch
leveldb::WriteBatch batch;
batch.Delete("key1");
batch.Put("key2", "value2");
batch.Put("key3", "value3");
db->Write(leveldb::WriteOptions(), &batch);  // All-or-nothing
```

## When DecentDB Is the Better Fit

### 1. You want SQL, not a key-value API

```sql
-- DecentDB: Complex query in one statement
SELECT u.name, COUNT(o.id) as order_count, SUM(o.total) as total_spent
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.region = 'West'
  AND o.created_at > '2024-01-01'
GROUP BY u.id, u.name
HAVING COUNT(o.id) > 5
ORDER BY total_spent DESC
LIMIT 10;
```

With LevelDB, you would need to:
- Design key encoding for users and orders
- Build and maintain secondary indexes manually
- Implement the join logic in application code
- Handle all the edge cases yourself

### 2. You need typed data and constraints

```sql
-- DecentDB: Schema with constraints
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    total DECIMAL(10,2) NOT NULL CHECK (total >= 0)
);
```

LevelDB stores raw bytes. Type checking, constraints, and referential integrity are your responsibility.

### 3. You need secondary indexes

```sql
-- DecentDB: Automatic index management
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_created ON orders(created_at);
```

With LevelDB, you would maintain separate databases for each index and keep them consistent manually.

### 4. You need concurrent readers

DecentDB supports multiple concurrent reader threads. LevelDB's reads are serialized (though thread-safe).

### 5. You want predictable read latency

B-tree reads have predictable latency. LSM-tree reads may need to check multiple levels, and compaction can cause I/O spikes.

### 6. You want to avoid compaction tuning

LevelDB requires tuning compaction settings for optimal performance. DecentDB's B-tree has no background compaction to tune.

## Performance Characteristics

| Workload | DecentDB (B-tree) | LevelDB (LSM) |
|----------|-------------------|---------------|
| Point lookup by key | ~microseconds | ~microseconds (may check multiple levels) |
| Range scan | Efficient (sequential leaf reads) | May check multiple SST levels |
| Write throughput | Good (in-place updates) | Good (append-only) |
| Write amplification | Moderate (page-level) | Higher (compaction) |
| Read latency variance | Low (predictable) | Variable (compaction) |
| Space amplification | Low (in-place) | Higher (multiple SST levels) |
| Compression | Not yet supported | Snappy/Zstd built-in |

## Decision Matrix

| Your Requirement | Choose |
|------------------|--------|
| SQL queries, JOINs, aggregations | DecentDB |
| Key-value with range scans | LevelDB |
| Typed data with constraints | DecentDB |
| Secondary indexes | DecentDB (automatic) |
| Minimal binary size | LevelDB |
| Built-in compression | LevelDB |
| Concurrent readers | DecentDB |
| Predictable read latency | DecentDB |
| Write-heavy key-value workload | LevelDB |
| No compaction tuning | DecentDB |
| Point-in-time snapshots | LevelDB |

## Summary

- Choose **LevelDB** when you need a simple, compact key-value store with built-in compression and snapshots.
- Choose **DecentDB** when you need SQL queries, typed data, constraints, concurrent readers, and predictable read latency.

Both are excellent embedded storage options. The question is whether you want to build on a key-value foundation (LevelDB) or start with a complete relational database (DecentDB).

## See Also

- [DecentDB vs RocksDB](decentdb-vs-rocksdb.md) — LevelDB's more feature-rich successor
- [DecentDB vs LMDB](decentdb-vs-lmdb.md) — B+tree key-value alternative
