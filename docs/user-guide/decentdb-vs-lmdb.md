# DecentDB vs LMDB: When to Choose Which

This document helps developers decide between **DecentDB** and **LMDB** (Lightning Memory-Mapped Database) for embedded storage workloads. Both use B-tree variants and prioritize read performance, but they operate at different abstraction levels.

> **Versions compared:** DecentDB 2.0.0 vs LMDB 0.9.33 (as of 2024).
>
> **See also:** [SQL Feature Matrix](sql-feature-matrix.md) for DecentDB's full SQL surface, and [DecentDB vs SQLite](decentdb-vs-sqlite.md) for the comparison with SQLite.

## They Operate at Different Layers

- **LMDB** is a **key-value store** with an ordered-map interface. It stores arbitrary byte-string keys and values. It has no concept of tables, columns, SQL, schemas, or queries beyond key lookup and range scans.
- **DecentDB** is a **relational database**. It stores typed rows in tables with columns, supports SQL queries, indexes, joins, constraints, and transactions.

If you want DecentDB's feature set on top of LMDB, you would need to build the SQL layer, query planner, type system, and constraint enforcement yourself.

## At a Glance

| Dimension | DecentDB | LMDB |
|-----------|----------|------|
| **Abstraction level** | Relational database (SQL) | Key-value store |
| **Data model** | Tables, rows, columns, types | Arbitrary key-value byte pairs |
| **Query language** | SQL | None (get/put/del/range API) |
| **Indexing** | B-tree secondary indexes, trigram, expression, covering | Single sorted key-space per database; multiple named databases |
| **Durability** | WAL + fsync-on-commit, always | Copy-on-write with configurable sync |
| **Architecture** | B-tree | B+tree with copy-on-write |
| **Memory model** | Page cache (configurable) | Memory-mapped (OS-managed) |
| **Concurrency** | One writer, many concurrent reader threads (single process) | One writer, many readers (multi-process safe) |
| **Transactions** | Full ACID (SQL-level) | ACID with MVCC |
| **Background work** | None (B-tree manages space in-place) | None (no compaction, no logs) |
| **Bindings** | C ABI, Rust, Python, .NET, Go, Java, Node.js, Dart | C, C++, Python, Rust, Go, Java, Node.js, Ruby, and many others |
| **License** | MIT or Apache-2.0 | OpenLDAP Public License (permissive) |
| **Binary size** | ~2-3 MB | ~64 KB |
| **Platform support** | Tier 1 Rust platforms | Unix, Linux, Windows, macOS, mobile |
| **Implementation language** | Rust | C |

## Architectural Similarities

Both databases share important design principles:

### B-tree Family

- **DecentDB** uses a B-tree for table storage and indexes
- **LMDB** uses a B+tree with copy-on-write semantics

Both provide O(log n) lookups and efficient range scans. This is fundamentally different from LSM-tree databases like RocksDB or LevelDB.

### Read-Optimized Design

Both prioritize read performance:

- **DecentDB**: B-tree pages cached in memory, direct index seeks
- **LMDB**: Memory-mapped files enable zero-copy reads directly from OS page cache

### No Background Compaction

Unlike LSM-tree databases, neither requires background compaction:

- **DecentDB**: In-place updates, space managed within B-tree pages
- **LMDB**: Copy-on-write creates new pages, old pages remain valid for active readers

## Key Differences

### Memory Management

**LMDB** uses memory-mapped files:
- OS manages the page cache automatically
- Zero-copy reads: API returns pointers directly into mapped memory
- Database size limited by virtual address space (128 TB on 64-bit systems)
- No application-level cache tuning needed

**DecentDB** uses an explicit page cache:
- Configurable cache size
- Application controls memory usage
- Works well on 32-bit systems without address space limits
- Requires tuning for optimal performance

### Cross-Process Concurrency

**LMDB** supports multi-process access:
- Multiple processes can open the same database
- Readers and writers from different processes coordinate via shared memory
- MVCC ensures readers see consistent snapshots

**DecentDB** is single-process:
- One process owns the database file
- Multiple threads within that process can read concurrently
- Simpler model, no cross-process coordination overhead

### File Format Portability

**LMDB** files are architecture-dependent:
- Not portable between 32-bit and 64-bit systems
- Not portable between different endianness
- Must export/import when moving between architectures

**DecentDB** uses a portable file format:
- Same file works across supported platforms
- Architecture-independent layout

### Transaction Model

**LMDB** uses MVCC with copy-on-write:
- Readers never block writers
- Writers never block readers
- Each read transaction sees a consistent snapshot
- Write transactions are serialized (one at a time)
- No transaction log needed (copy-on-write provides durability)

**DecentDB** uses WAL-based transactions:
- Readers and writers can run concurrently
- Write-ahead log ensures durability
- Checkpointing keeps WAL bounded
- Full SQL transaction semantics (BEGIN/COMMIT/ROLLBACK)

## When LMDB Is the Better Fit

### 1. You need a key-value store, not SQL

If your data model is naturally key-value and you don't need queries, joins, or constraints, LMDB's simpler API is an advantage.

```c
// LMDB: Simple key-value operations
MDB_val key, value;
key.mv_data = "user:123";
key.mv_size = 8;
value.mv_data = user_data;
value.mv_size = user_data_len;
mdb_put(txn, dbi, &key, &value, 0);
```

### 2. You need zero-copy reads

LMDB returns pointers directly into memory-mapped files. For large values, this avoids copying data into application buffers.

```c
// LMDB: Zero-copy read
MDB_val key, value;
mdb_get(txn, dbi, &key, &value);
// value.mv_data points directly into the memory-mapped file
process_data(value.mv_data, value.mv_size);  // No copy needed
```

### 3. You need multi-process access

If multiple independent processes need to access the same database concurrently, LMDB's cross-process concurrency model is designed for this.

### 4. You want minimal code size

At ~64 KB of object code, LMDB is one of the smallest embedded databases available. If binary size is a critical constraint, LMDB is hard to beat.

### 5. You want OS-managed caching

LMDB relies on the OS page cache. There's no cache tuning, no buffer pool management, and no application-level memory accounting.

## When DecentDB Is the Better Fit

### 1. You need SQL queries

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

With LMDB, you would need to:
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CHECK (email LIKE '%@%')
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    total DECIMAL(10,2) NOT NULL CHECK (total >= 0)
);
```

LMDB stores raw bytes. Type checking, constraints, and referential integrity are your responsibility.

### 3. You need secondary indexes

```sql
-- DecentDB: Automatic index management
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_created ON orders(created_at);
```

With LMDB, you would maintain separate databases for each index and keep them consistent manually.

### 4. You need a portable file format

If your database files might be moved between different architectures (e.g., x86_64 to ARM), DecentDB's portable format avoids export/import steps.

### 5. You want a single-process, multi-threaded model

If your application is a single process with multiple threads, DecentDB's simpler concurrency model avoids the complexity of cross-process coordination.

## Performance Characteristics

| Workload | DecentDB | LMDB |
|----------|----------|------|
| Point lookup by key | ~microseconds | ~microseconds (zero-copy) |
| Range scan | Efficient (B-tree) | Efficient (B+tree) |
| Write throughput | Good (WAL + in-place) | Good (copy-on-write) |
| Read latency | Predictable | Predictable (OS-managed) |
| Memory control | Explicit (cache size) | Implicit (OS page cache) |
| Large value reads | Copy required | Zero-copy possible |

## Decision Matrix

| Your Requirement | Choose |
|------------------|--------|
| SQL queries, joins, aggregations | DecentDB |
| Key-value with range scans | LMDB |
| Multi-process access | LMDB |
| Single-process, multi-threaded | Either (DecentDB for SQL) |
| Zero-copy large value reads | LMDB |
| Typed data with constraints | DecentDB |
| Secondary indexes | DecentDB (automatic) |
| Minimal binary size | LMDB |
| Portable file format | DecentDB |
| OS-managed caching | LMDB |

## Summary

- Choose **LMDB** when you need a fast, compact key-value store with multi-process support and zero-copy reads.
- Choose **DecentDB** when you need SQL queries, typed data, constraints, and automatic index management.

Both are excellent choices for embedded storage. The question is whether you want to build on a key-value foundation (LMDB) or start with a complete relational database (DecentDB).
