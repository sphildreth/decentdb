# DecentDB vs sled: When to Choose Which

This document helps developers decide between **DecentDB** and **sled** for embedded storage workloads in the Rust ecosystem. Both are written in Rust, but they serve different purposes.

> **Versions compared:** DecentDB 2.0.0 vs sled 0.34.x (as of 2024).
>
> **See also:** [SQL Feature Matrix](sql-feature-matrix.md) for DecentDB's full SQL surface, and [DecentDB vs SQLite](decentdb-vs-sqlite.md) for the comparison with SQLite.

## They Operate at Different Layers

- **sled** is a **key-value store** with an API similar to `BTreeMap`. It stores arbitrary keys and values with ACID transactions. It has no concept of tables, columns, SQL, schemas, or queries beyond key lookup and range scans.
- **DecentDB** is a **relational database**. It stores typed rows in tables with columns, supports SQL queries, indexes, joins, constraints, and transactions.

If you want DecentDB's feature set on top of sled, you would need to build the SQL layer, query planner, type system, and constraint enforcement yourself.

## At a Glance

| Dimension | DecentDB | sled |
|-----------|----------|------|
| **Abstraction level** | Relational database (SQL) | Key-value store |
| **Data model** | Tables, rows, columns, types | Arbitrary key-value pairs |
| **Query language** | SQL | None (get/insert/remove/scan API) |
| **Indexing** | B-tree secondary indexes, trigram, expression, covering | Single sorted key-space |
| **Durability** | WAL + fsync-on-commit, always | Configurable (flush/flush_async) |
| **Architecture** | B-tree | B-link tree (lock-free) |
| **Concurrency** | One writer, many concurrent reader threads | Lock-free reads, concurrent writes |
| **Transactions** | Full ACID (SQL-level) | ACID (atomic batches, compare-and-swap) |
| **Event notifications** | No | Yes (watch_prefix) |
| **Background work** | None (B-tree manages space in-place) | Background segment merging |
| **Bindings** | C ABI, Rust, Python, .NET, Go, Java, Node.js, Dart | Rust only |
| **License** | MIT or Apache-2.0 | MIT or Apache-2.0 |
| **Binary size** | ~2-3 MB | ~500 KB - 1 MB |
| **Platform support** | Tier 1 Rust platforms | Tier 1 Rust platforms |
| **Implementation language** | Rust | Rust |

## Both Are Rust-Native

This is a key similarity. Both databases:

- Are written in pure Rust
- Leverage Rust's type system for safety
- Avoid `unsafe` code where possible
- Support async runtimes (DecentDB via async bindings, sled natively)
- Target the same platforms (Tier 1 Rust targets)

The choice isn't about language ecosystem fit—it's about abstraction level and features.

## Key Differences

### Data Model

**sled** is a key-value store:

```rust
// sled: Simple key-value operations
let tree = sled::open("/tmp/mydb")?;

tree.insert("user:123", b"John Doe")?;
tree.insert("user:456", b"Jane Smith")?;

// Range scan
for item in tree.range("user:100".."user:200") {
    let (key, value) = item?;
    println!("{:?} => {:?}", key, value);
}
```

**DecentDB** is a relational database:

```sql
-- DecentDB: Structured data with SQL
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE
);

INSERT INTO users (id, name, email) VALUES 
    (123, 'John Doe', 'john@example.com'),
    (456, 'Jane Smith', 'jane@example.com');

SELECT * FROM users WHERE id BETWEEN 100 AND 200;
```

### Query Capabilities

**sled** provides:
- Point lookups (`get`)
- Range scans (`range`, `scan_prefix`)
- Atomic compare-and-swap (`compare_and_swap`)
- Batch operations

**DecentDB** provides:
- Full SQL query language
- JOINs (INNER, LEFT, RIGHT)
- Aggregations (COUNT, SUM, AVG, etc.)
- GROUP BY and HAVING
- Subqueries
- Window functions (planned)

### Event Notifications

**sled** has built-in event subscriptions:

```rust
// sled: Watch for changes to keys with a prefix
let tree = sled::open("mydb")?;
let mut subscriber = tree.watch_prefix("user:");

// In an async context
while let Some(event) = (&mut subscriber).await {
    println!("Event: {:?}", event);
}
```

**DecentDB** does not have built-in change notifications. You would implement this at the application layer.

### Maturity and Stability

**DecentDB**:
- Stable 2.0.0 release
- Stable file format
- Production-ready for embedded workloads

**sled**:
- 0.34.x release (pre-1.0)
- API stability not guaranteed
- Active development continues
- Some known edge cases and performance quirks

### Cross-Language Bindings

**DecentDB** provides a stable C ABI with bindings for:
- Python, .NET, Go, Java, Node.js, Dart
- Any language that can call C functions

**sled** is Rust-only. If you need to access the database from other languages, you would need to build your own FFI layer.

## When sled Is the Better Fit

### 1. You need a simple key-value store

If your data model is naturally key-value and you don't need SQL, sled's API is straightforward:

```rust
let db = sled::open("mydb")?;
db.insert("config:theme", b"dark")?;
db.insert("config:language", b"en")?;

let theme = db.get("config:theme")?;
```

### 2. You need event notifications

sled's `watch_prefix` provides real-time notifications when keys change:

```rust
let mut sub = db.watch_prefix("cache:");
db.insert("cache:item1", b"value1")?;  // Triggers event
```

### 3. You need compare-and-swap semantics

```rust
// Atomic CAS operation
let old_value = db.get("counter")?;
let result = db.compare_and_swap(
    "counter",
    old_value,           // Expected current value
    Some(b"42"),         // New value
)?;
if result.is_ok() {
    println!("Updated successfully");
}
```

### 4. You're building a Rust-only application

If your entire stack is Rust and you don't need cross-language access, sled's pure-Rust API is ergonomic and idiomatic.

### 5. You want a smaller binary

sled's binary footprint is smaller than DecentDB's, which includes a full SQL parser, planner, and executor.

## When DecentDB Is the Better Fit

### 1. You need SQL queries

```sql
-- DecentDB: Complex analytical query
SELECT 
    DATE(created_at) as day,
    COUNT(*) as orders,
    SUM(total) as revenue
FROM orders
WHERE created_at >= '2024-01-01'
GROUP BY DATE(created_at)
ORDER BY day DESC;
```

With sled, you would need to:
- Design key encoding for efficient querying
- Implement aggregation logic in application code
- Handle all edge cases manually

### 2. You need typed data and constraints

```sql
-- DecentDB: Schema with validation
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price DECIMAL(10,2) NOT NULL CHECK (price > 0),
    stock INTEGER NOT NULL DEFAULT 0 CHECK (stock >= 0)
);
```

sled stores raw bytes. Type safety and validation are your responsibility.

### 3. You need secondary indexes

```sql
-- DecentDB: Multiple indexes for different access patterns
CREATE INDEX idx_products_name ON products(name);
CREATE INDEX idx_products_price ON products(price);
```

With sled, you would maintain separate trees for each index and keep them consistent.

### 4. You need cross-language access

DecentDB's C ABI enables access from Python, .NET, Go, Java, Node.js, and Dart. sled is Rust-only.

### 5. You need a stable, production-ready release

DecentDB 2.0.0 has a stable file format and API. sled is still pre-1.0 with potential API changes.

### 6. You need JOINs

```sql
-- DecentDB: Join across tables
SELECT o.id, u.name, o.total
FROM orders o
JOIN users u ON o.user_id = u.id
WHERE o.total > 100;
```

With sled, you would implement join logic in application code, potentially with multiple round trips.

## Performance Characteristics

| Workload | DecentDB | sled |
|----------|----------|------|
| Point lookup | ~microseconds | ~microseconds |
| Range scan | Efficient (B-tree) | Efficient (B-link tree) |
| Write throughput | Good (WAL) | Good (lock-free) |
| Concurrent reads | Excellent | Excellent (lock-free) |
| Concurrent writes | Single writer | Multiple writers |
| Event notifications | Not supported | Built-in |

## Decision Matrix

| Your Requirement | Choose |
|------------------|--------|
| SQL queries, JOINs, aggregations | DecentDB |
| Simple key-value with range scans | sled |
| Event notifications (watch_prefix) | sled |
| Typed data with constraints | DecentDB |
| Secondary indexes | DecentDB (automatic) |
| Compare-and-swap operations | sled |
| Cross-language access | DecentDB |
| Rust-only application | Either |
| Smaller binary size | sled |
| Stable 1.0+ release | DecentDB |
| Multiple concurrent writers | sled |

## Summary

- Choose **sled** when you need a pure-Rust key-value store with event notifications, compare-and-swap, and a simple API.
- Choose **DecentDB** when you need SQL queries, typed data, constraints, JOINs, and cross-language bindings.

Both are excellent Rust-native options. The choice depends on whether you need the full power of SQL (DecentDB) or the simplicity of a key-value interface (sled).
