# DecentDB vs RocksDB: When to Choose Which

This document helps developers decide between **DecentDB** and **RocksDB** for embedded storage workloads. These two systems operate at different abstraction levels, so the choice is less about features and more about what you want to build *on top* of your storage layer.

> **Versions compared:** DecentDB 2.0.0 vs RocksDB 9.x (as of 2024).
>
> **See also:** [SQL Feature Matrix](sql-feature-matrix.md) for DecentDB's full SQL surface, and [DecentDB vs SQLite](decentdb-vs-sqlite.md) for the comparison with SQLite.

## They Operate at Different Layers

This is the most important thing to understand before comparing:

- **RocksDB** is a **key-value storage engine**. It stores byte-string keys and byte-string values. It has no concept of tables, columns, SQL, schemas, indexes, or queries. It is a building block.
- **DecentDB** is a **relational database**. It stores typed rows in tables with columns, supports SQL queries, indexes, joins, constraints, and transactions. It is a complete product.

Systems like CockroachDB, TiKV, Pebble, and YugabyteDB use RocksDB (or its derivatives) as the storage layer and build a distributed database on top. If you want DecentDB's feature set on top of RocksDB, you would need to build it yourself.

## At a Glance

| Dimension | DecentDB | RocksDB |
|-----------|----------|---------|
| **Abstraction level** | Relational database (SQL) | Key-value storage engine |
| **Data model** | Tables, rows, columns, types | Arbitrary key-value byte pairs |
| **Query language** | SQL | None (get/put/scan/delete API) |
| **Indexing** | B-tree secondary indexes, trigram, expression, covering | Single sorted key-space; secondary indexes must be built manually |
| **Durability** | WAL + fsync-on-commit, always | WAL + configurable sync policies |
| **Architecture** | B-tree | LSM-tree (Log-Structured Merge-tree) |
| **Write path** | In-place page updates | Append-only memtable, background compaction |
| **Concurrency** | One writer, many concurrent reader threads | Single-process; thread-safe concurrent access |
| **Transactions** | Full ACID (SQL-level) | ACID at key-value level (`WriteBatch`, optimistic/pessimistic txn) |
| **Compaction** | None (B-tree manages space in-place) | Background compaction is central to the design |
| **Bindings** | C ABI, Rust, Python, .NET, Go, Java, Node.js, Dart | C++, C, Java, Go, Python, and many others |
| **License** | MIT or Apache-2.0 | Apache-2.0 or GPLv2 |
| **Binary size** | ~2-3 MB | ~5-10 MB |
| **Platform support** | Tier 1 Rust platforms | Windows, macOS, Linux, mobile |
| **Implementation language** | Rust | C++ |

## Architectural Differences

### B-tree (DecentDB) vs LSM-tree (RocksDB)

This is the fundamental design difference. It affects write throughput, read latency, space amplification, and operational behavior.

**DecentDB (B-tree):**
- Writes update pages in place (via WAL for durability)
- Reads are direct index seeks -- O(log n) B-tree traversal
- Predictable read latency, no background work interfering
- Write amplification from page-level WAL + checkpoint

**RocksDB (LSM-tree):**
- Writes go to an in-memory memtable, then flush to sorted SST files on disk
- Background compaction merges SST files in levels
- Excellent write throughput (sequential I/O pattern)
- Reads may need to check multiple SST levels + bloom filters
- Compaction causes periodic I/O spikes and space amplification

```
DecentDB write path:
  SQL INSERT → B-tree page lookup → WAL append → in-place page update → fsync

RocksDB write path:
  put(key, value) → memtable → WAL append → memtable full → flush to L0 SST
  → background compaction merges L0 → L1 → L2 → ...
```

### When the LSM-tree wins

LSM-trees excel at write-heavy workloads where:
- You are ingesting large volumes of key-value pairs
- Keys are roughly sequential or you don't need instant read-after-write visibility
- You can tolerate background compaction I/O
- Your read workload is primarily point lookups (bloom filters help)

### When the B-tree wins

B-trees excel at:
- Mixed read/write workloads where read latency must be predictable
- Range scans (sequential key reads from B-tree leaves)
- Workloads where background compaction interference is unacceptable
- Scenarios where space amplification from compaction is a concern

### Performance Characteristics

| Workload | DecentDB (B-tree) | RocksDB (LSM) |
|----------|-------------------|---------------|
| Point lookup by key | ~microseconds | ~microseconds (with bloom filter) |
| Range scan | Efficient (sequential leaf reads) | May check multiple SST levels |
| Write throughput | Good (in-place updates) | Excellent (append-only) |
| Write amplification | Moderate (page-level) | Higher (compaction) |
| Read latency variance | Low (predictable) | Variable (compaction spikes) |
| Space amplification | Low (in-place) | Higher (multiple SST levels) |

## When DecentDB Is the Better Fit

### 1. You want SQL, not a key-value API

RocksDB gives you `Get(key)`, `Put(key, value)`, and `Delete(key)`. There is no query language, no `WHERE` clause, no `JOIN`, no `GROUP BY`, no aggregation. If you want to find "all orders from users in California with total > $100 placed last month," you must design and maintain the indexes yourself.

```sql
-- DecentDB: one query
SELECT o.id, o.total, u.name
FROM orders o
JOIN users u ON o.user_id = u.id
WHERE u.state = 'CA'
  AND o.total > 100
  AND o.created_at >= '2024-11-01';
```

With RocksDB, you would need to:
1. Design a key encoding scheme (e.g., `order:{id}` for primary data)
2. Build secondary indexes manually (e.g., `idx:state:CA:{user_id}` for state lookups)
3. Build composite indexes for range queries (e.g., `idx:orders:date:{timestamp}:{order_id}`)
4. Implement the join logic in application code
5. Maintain consistency between primary data and secondary indexes on every write

### 2. You need multi-column indexes with range queries

DecentDB supports composite B-tree indexes for multi-column range predicates. RocksDB has a single sorted key-space -- all "secondary indexes" must be hand-encoded.

```sql
-- DecentDB: composite index for efficient range scan
CREATE INDEX idx_orders_user_date ON orders(user_id, created_at);

-- Efficient: uses the composite index
SELECT * FROM orders
WHERE user_id = 42 AND created_at >= '2024-01-01' AND created_at < '2024-02-01';
```

With RocksDB, you would encode this as a compound key like `order_by_user_date:{user_id}:{timestamp}:{order_id}` and use `Iterator::Seek` with upper/lower bounds. It works, but you are responsible for the encoding, the decode logic, and keeping it consistent.

### 3. You need enforced constraints

DecentDB enforces `PRIMARY KEY`, `UNIQUE`, `NOT NULL`, `CHECK`, and `FOREIGN KEY` constraints in the engine. RocksDB has no constraint system -- every validation must be implemented in application code.

```sql
-- DecentDB: constraints enforced by the engine
CREATE TABLE accounts (
  id INT PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  balance DECIMAL(10,2) CHECK (balance >= 0)
);

INSERT INTO accounts (email, balance) VALUES ('alice@test.com', -50);
-- ERROR: CHECK constraint violation

INSERT INTO accounts (email) VALUES (NULL);
-- ERROR: NOT NULL violation
```

### 4. You need schema evolution (ALTER TABLE)

DecentDB supports `ALTER TABLE ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, `ALTER COLUMN TYPE`, and `RENAME TO`. RocksDB has no schema -- it stores opaque byte pairs. Schema evolution is entirely your responsibility, including migration of existing data and versioning of value formats.

```sql
-- DecentDB: schema changes are handled by the engine
ALTER TABLE users ADD COLUMN phone TEXT;
ALTER TABLE users RENAME COLUMN name TO full_name;
ALTER TABLE users ALTER COLUMN age TYPE TEXT;
```

With RocksDB, you would need to:
1. Define a serialization format (Protocol Buffers, MessagePack, JSON, custom)
2. Version your value schemas
3. Write a migration that reads every key, deserializes, adds the field, reserializes, and writes back
4. Handle backward compatibility during the migration

### 5. You need joins, aggregates, and window functions

DecentDB provides a full query engine with joins, aggregates, window functions, CTEs, and set operations. RocksDB provides none of these.

```sql
-- DecentDB: analytical query on transactional data
SELECT
  u.name,
  COUNT(o.id) AS order_count,
  SUM(o.amount) AS total_spent,
  ROW_NUMBER() OVER (ORDER BY SUM(o.amount) DESC) AS spending_rank
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.name
HAVING COUNT(o.id) > 5
ORDER BY total_spent DESC;
```

With RocksDB, you would iterate over keys with a prefix scan, aggregate in application code, and implement ranking logic yourself.

### 6. You need guaranteed fsync-on-commit durability by default

DecentDB fsyncs on every commit with no opt-out. RocksDB's `sync` option on `WriteOptions` defaults to `false` -- you must explicitly enable it for durability, and even then the WAL sync behavior is configurable.

```sql
-- DecentDB: always durable
BEGIN;
UPDATE inventory SET qty = qty - 1 WHERE sku = 'WIDGET';
COMMIT;  -- guaranteed fsync
```

```cpp
// RocksDB: must explicitly request sync
rocksdb::WriteOptions options;
options.sync = true;  // NOT the default
db->Put(options, key, value);
```

### 7. You need predictable read latency without compaction interference

RocksDB background compaction causes periodic I/O spikes that can affect read latency. DecentDB's B-tree has no background compaction -- read latency is consistent.

This matters for latency-sensitive applications (API backends, real-time systems) where a p99 spike from compaction is unacceptable.

### 8. You need crash-safety verification built in

DecentDB ships with FaultyVFS and WAL failpoint hooks for deterministic crash and torn-write testing. RocksDB has `fault_injection_test` in its test suite, but it is not a first-class user-facing feature for application-level testing.

## When RocksDB Is the Better Fit

### 1. You need maximum write throughput

LSM-trees are optimized for write-heavy workloads. RocksDB's append-only memtable + sequential SST writes achieve higher raw write throughput than B-tree in-place page updates, especially on SSDs.

If your workload is "bulk ingest millions of key-value pairs per second and occasionally read by key," RocksDB's architecture is designed for exactly this.

### 2. You are building your own database

RocksDB is the most widely-used embedded storage engine in the database industry. If you are building a distributed database, a time-series engine, a graph store, or any system that needs a durable, sorted key-value foundation, RocksDB is battle-tested for this role.

Notable systems built on RocksDB:
- CockroachDB (via Pebble, a RocksDB-inspired Go engine)
- TiKV (distributed transactional key-value store)
- YugabyteDB (distributed SQL)
- Kafka Streams (state stores)
- MyRocks (MySQL storage engine)

### 3. You need fine-grained control over compaction

RocksDB gives you extensive control over compaction strategy, level sizes, compression per level, rate limiting, and scheduling. If you understand your I/O patterns and want to tune the engine for your specific workload, RocksDB exposes the knobs.

```cpp
// RocksDB: tune compaction
rocksdb::Options options;
options.num_levels = 7;
options.level0_file_num_compaction_trigger = 4;
options.compression_per_level = {
  rocksdb::kNoCompression,    // L0: fast writes
  rocksdb::kSnappyCompression, // L1-L2: fast reads
  rocksdb::kZSTD              // L3+: space savings
};
options.rate_limiter.reset(rocksdb::NewGenericRateLimiter(64 * 1048576));  // 64 MB/s
```

DecentDB does not expose tuning knobs for its internal storage.

### 4. You need column families for logical data separation

RocksDB column families let you partition data within one database instance, each with its own compaction settings, comparators, and prefix extractors. This is useful for workloads with different access patterns per data type.

```cpp
// RocksDB: separate column families with different settings
rocksdb::ColumnFamilyOptions fast_write_cf;
fast_write_cf.compression = rocksdb::kNoCompression;

rocksdb::ColumnFamilyOptions compact_cf;
compact_cf.compression = rocksdb::kZSTD;

std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs = {
  {"events", fast_write_cf},
  {"users", compact_cf}
};
```

DecentDB separates data by tables, but does not allow per-table storage tuning.

### 5. You need a narrow API surface for raw storage

RocksDB provides a focused key-value API: get, put, delete, iterate, batch write, snapshots. If you want raw storage without a query parser, planner, executor, type system, or SQL layer, RocksDB gives you exactly that. Note that the codebase itself is substantial (~300K+ lines of C++), but the API surface you interact with is minimal.

### 6. You need snapshots for consistent reads across many keys

RocksDB provides lightweight snapshots that give a consistent view of the entire key-space at a point in time, without copying data. This is useful for backup, replication, or consistent read pipelines.

```cpp
// RocksDB: consistent snapshot across all keys
const rocksdb::Snapshot* snapshot = db->GetSnapshot();
rocksdb::ReadOptions options;
options.snapshot = snapshot;
// ... iterate over keys with consistent view ...
db->ReleaseSnapshot(snapshot);
```

### 7. You need merge operators for atomic read-modify-write

RocksDB merge operators let you define custom associative operations (like increment, append, set-union) that are applied atomically. This avoids read-modify-write races for counters and accumulators.

```cpp
// RocksDB: atomic increment via merge
db->Merge(write_options, "counter_key", "1");  // atomic increment
// Later: db->Get() returns the accumulated value
```

With DecentDB, you use standard SQL:

```sql
-- DecentDB: atomic increment via UPDATE
UPDATE counters SET value = value + 1 WHERE key = 'page_views';
```

Both work, but RocksDB's merge operator avoids the read-modify-write round-trip for simple operations.

### 8. You need cross-process access to the same data

RocksDB can be opened by multiple processes simultaneously (with appropriate locking). DecentDB is single-process.

## Side-by-Side Examples

### Simple key-value read/write

```sql
-- DecentDB: SQL with schema
CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO config (key, value) VALUES ('theme', 'dark');
SELECT value FROM config WHERE key = 'theme';
```

```cpp
// RocksDB: raw key-value API
db->Put(write_options, "config:theme", "dark");
std::string value;
db->Get(read_options, "config:theme", &value);
```

Both work. The difference is what happens when you need more than key-value access.

### Range scan

```sql
-- DecentDB: range scan with index
CREATE INDEX idx_events_ts ON events(timestamp);
SELECT * FROM events
WHERE timestamp >= '2024-01-01' AND timestamp < '2024-02-01'
ORDER BY timestamp;
```

```cpp
// RocksDB: manual prefix scan with encoded keys
rocksdb::ReadOptions options;
auto it = db->NewIterator(options);
for (it->Seek("event:2024-01"); it->Valid() && it->key().starts_with("event:2024-01"); it->Next()) {
    // decode key and value manually
}
delete it;
```

### Multi-key transaction

```sql
-- DecentDB: ACID transfer
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

```cpp
// RocksDB: WriteBatch (atomic but no SQL-level transaction semantics)
rocksdb::WriteBatch batch;
batch.Put("account:1:balance", "400");  // new balance after -100
batch.Put("account:2:balance", "600");  // new balance after +100
db->Write(write_options, &batch);
// Note: must read-then-write manually, no WHERE clause
```

RocksDB's `WriteBatch` is atomic (all-or-nothing) but does not provide SQL-level isolation semantics, constraint checking, or the query planner to resolve the update expression.

For more complex transaction scenarios, RocksDB also offers `TransactionDB` with pessimistic locking and snapshot isolation, but you still manage all schema, constraints, and query logic in application code.

## Summary Decision Matrix

| Your situation | Recommendation |
|----------------|----------------|
| You need SQL queries, joins, aggregates | **DecentDB** |
| You need a schema with types and constraints | **DecentDB** |
| You need enforced foreign keys and unique constraints | **DecentDB** |
| You need multi-column indexes with range scans | **DecentDB** |
| You need fsync-on-commit durability by default | **DecentDB** |
| You need predictable read latency (no compaction spikes) | **DecentDB** |
| You need triggers, savepoints, views | **DecentDB** |
| You need built-in crash-injection testing | **DecentDB** |
| You are building your own database on a KV engine | **RocksDB** |
| You need maximum raw write throughput | **RocksDB** |
| You need fine-grained compaction tuning | **RocksDB** |
| You need column families with per-family settings | **RocksDB** |
| You need a minimal C++ storage dependency | **RocksDB** |
| You need merge operators for atomic accumulators | **RocksDB** |
| You need lightweight cross-process snapshots | **RocksDB** |
| You need cross-process access to the same data | **RocksDB** |

## A Note on "Why Not Both?"

Some systems use a key-value engine *and* a relational database side by side. For example, RocksDB for high-throughput event ingestion with DecentDB for the queryable application state. This is a valid architecture when your write path and read path have fundamentally different requirements.

If you are choosing a single embedded engine and you need SQL, DecentDB gives you the complete stack. If you need a raw storage foundation to build on, RocksDB gives you the most proven starting point in the industry.
