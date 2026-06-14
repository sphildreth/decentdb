# Performance Tuning

Tips for optimizing DecentDB performance.

## Cache Configuration

The page cache is critical for read performance.

```bash
# Default durable profile: 4MB
decentdb exec --db=my.ddb --sql="SELECT 1"

# Explicit balanced profile behavior: 4096 pages = 16MB
decentdb exec --db=my.ddb --sql="SELECT 1" --cachePages=4096

# Explicit low-memory profile behavior: 1024 pages = 4MB
decentdb exec --db=my.ddb --sql="SELECT 1" --cachePages=1024

# Or use megabytes
decentdb exec --db=my.ddb --sql="SELECT 1" --cacheMb=64  # 64MB
```

Recommendations:
- Small datasets (< 1GB): default 4MB cache, or 16MB with
  `DbConfig::balanced()` when the host has memory headroom
- Medium datasets (1-10GB): 16-64MB cache
- Large datasets (> 10GB): 64-256MB cache

The default profile keeps `WalSyncMode::Full`; DecentDB does not weaken durable
commit acknowledgement to improve benchmark charts. The explicit high-memory
tuned profile used in release comparisons is `64MB` plus row-source retention
and checkpoint settings for hot read workloads.

Default file-backed opens avoid work that is unnecessary until a workload
actually needs it: the background checkpoint worker starts on the first
auto-checkpoint threshold hit, and reactive subscription state is created only
when a watch API is used.

Default `process_coordination=auto` keeps the byte-range writer/checkpoint lock
as the correctness mechanism, but it avoids per-commit sidecar writes for
writer-owner diagnostics. Use `process_coordination=required` when
cross-process diagnostic persistence is more important than the default-fast
single-process hot path.

## Indexing Strategy

### Primary Keys

Always define primary keys. DecentDB automatically creates an index.

A single INT64 `PRIMARY KEY` column supports auto-assignment — omit the column from INSERT and DecentDB assigns the next sequential ID (`INT`/`INTEGER`/`INT64`/`BIGINT` are synonyms here).

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,  -- Automatically indexed, auto-assigned when omitted
    name TEXT
);

-- id is auto-assigned (1, 2, 3, ...)
INSERT INTO users (name) VALUES ('Alice');
INSERT INTO users (name) VALUES ('Bob');

-- Explicit id also works; counter advances past it
INSERT INTO users VALUES (100, 'Carol');
INSERT INTO users (name) VALUES ('Dave');  -- id = 101
```

### Foreign Keys

Foreign keys are automatically indexed for efficient joins.

```sql
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES users(id)  -- Auto-indexed
);
```

### Trigram Indexes

Use trigram indexes for substring and `%pattern%` search:

```sql
CREATE INDEX idx_name_trgm ON users USING trigram(name);

-- Fast substring search
SELECT * FROM users WHERE name LIKE '%john%';
```

### Full-Text Indexes

Use full-text indexes for tokenized document search, phrase search, prefix
search, and BM25 ranking:

```sql
CREATE INDEX idx_docs_search
ON docs USING fulltext(title, body)
WITH (prefix = '2,3');

SELECT id, bm25('idx_docs_search') AS rank
FROM docs
WHERE fulltext_match('idx_docs_search', 'database OR search')
ORDER BY rank DESC
LIMIT 20;
```

Choose trigram search for arbitrary substrings and full-text search for
keyword/document relevance.

### Expression Indexes

Index a computed expression for faster lookups:

```sql
-- Index on lowercase name for case-insensitive search
CREATE INDEX idx_name_lower ON users((LOWER(name)));

-- Query automatically uses the index
SELECT * FROM users WHERE LOWER(name) = 'alice';
```

Supported expressions: `LOWER(col)`, `UPPER(col)`, `TRIM(col)`, `LENGTH(col)`, `CAST(col AS type)`.

### Partial Indexes

Index only rows that satisfy a condition:

```sql
-- Index only non-null emails
CREATE INDEX idx_email_nonnull ON users(email) WHERE email IS NOT NULL;
```

### Covering Indexes

Use `INCLUDE (...)` for narrow projections that should be answered from an
index without fetching the base row:

```sql
CREATE INDEX idx_users_email_cover ON users(email) INCLUDE (name);

SELECT name FROM users WHERE email = 'ada@example.com';
```

Covering execution is conservative. It is used only for fresh B+Tree indexes
when projected values are available from index key/include metadata and row
policies, masks, generated columns, partial-index predicates, and transaction
state do not require the ordinary base-row path.

### When to Index

DO index:
- Primary keys (automatic)
- Foreign keys (automatic)
- Columns in WHERE clauses
- Columns in ORDER BY
- Columns in JOIN conditions

DON'T index:
- Columns with low cardinality (e.g., boolean flags)
- Columns that are rarely queried
- Very small tables

## Bulk Loading

For large imports, use bulk load:

```bash
# Much faster than individual inserts
decentdb bulk-load --db=my.ddb --table=users --input=users.csv
```

Bulk load options:
- `--disableIndexes` - Skip index updates during load (rebuild after)
- `--durability=deferred` - Batch fsync operations
- `--batchSize=10000` - Rows per batch

## Query Optimization

### Use Indexes

```sql
-- Fast: uses index
SELECT * FROM users WHERE id = 42;

-- Slow: full table scan (unless name is indexed)
SELECT * FROM users WHERE name = 'Alice';
```

### Limit Results

```sql
-- Good: returns only what you need
SELECT * FROM logs ORDER BY created_at DESC LIMIT 100;

-- Bad: fetches entire table
SELECT * FROM logs ORDER BY created_at DESC;
```

### Avoid Sorting Large Datasets

Sorting millions of rows requires external merge sort (spills to disk).

If possible:
- Use indexes for ORDER BY
- Filter before sorting
- Limit results

## WAL and Checkpointing

### Checkpoint Strategy

Checkpoints write WAL data to the main database file:

```bash
# Manual checkpoint
decentdb checkpoint --db=my.ddb

# (Optional) Override checkpoint policy for this exec invocation
# Note: if you pass any checkpoint/reader flag, exec overrides the engine defaults.
decentdb exec --db=my.ddb --checkpointBytes=10000000 --sql="SELECT 1"
```

### Durability vs Performance

Trade-off between safety and speed:

DecentDB commits are durable (fsync-on-commit) by default.

For higher throughput ingestion, use bulk load durability modes:

```bash
# Default bulk-load mode (good throughput, still durable)
decentdb bulk-load --db=my.ddb --table=logs --input=logs.csv --durability=deferred

# Fastest (unsafe): may lose recent batches on crash
decentdb bulk-load --db=my.ddb --table=logs --input=logs.csv --durability=none
```

## Monitoring

Check database statistics:

```bash
# Database info
decentdb info --db=my.ddb

# Shows:
# - Page size
# - Cache usage
# - WAL size
# - Active readers
```

## Plan Cache

DecentDB ships a connection-local plan cache that reuses parsed
parameterized statements and reusable prepared plans across calls
within a single `Db` handle. The cache is **enabled by default** with
a conservative 256 KiB total budget. See
`design/WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md` and
ADR 0190-0194 for the full contract.

Cache configuration:

```rust
use decentdb::{Db, DbConfig};

let mut config = DbConfig::default();
config.with_plan_cache(|c| {
    c.enabled = true;
    c.max_size_bytes = 512 * 1024;  // 512 KiB
});
let db = Db::open("app.ddb", config)?;
```

C ABI / binding open options:

```text
plan_cache_enabled=true|false
plan_cache_max_bytes=<bytes>
```

The default-on behavior is additive: existing binaries that do not
set the options get the cache. To opt out, set
`plan_cache_enabled=false`.

Literal one-shot SQL follows the existing narrow parser cache path instead of
entering the generalized plan cache. Parameterized parsed statements use
second-use admission, and prepared-plan entries admit on the first miss. This
keeps one-shot overhead bounded while preserving the repeated-preparation win.

Diagnostics:

```sql
SELECT * FROM sys.plan_cache;
SELECT * FROM sys.plan_cache_summary;
PRAGMA flush_plan_cache;  -- evict and reset counters
```

The cache is invalidated on DDL, temp-schema, policy/mask changes,
branch operations, extension changes, and `PRAGMA flush_plan_cache`.
`SET AUDIT CONTEXT` does not affect the cache (ADR 0192).

WASM/browser note: the budget is *per connection*. Multi-worker
browser apps should set `plan_cache_max_bytes` to
`low_memory_budget / N_workers`.

Native validation commands:

```bash
cargo bench -p decentdb --bench plan_cache
cd benchmarks/rust-baseline
cargo run --release --bin rust-baseline -- --plan-cache-benchmark --out-dir ../../.tmp/rust-baseline-plan-cache
```

## Prepared-write hot paths and aggregates

Prepared insert execution prefers direct/default hot paths for supported prepared
shapes and avoids unnecessary `INCLUDE (...)` payload work when no projected
payload is needed. Reusing prepared statements for repeated writes keeps this
path hot.

For Rust callers inserting many rows inside one explicit transaction,
`SqlTransaction::prepared_batch` keeps the validated prepared statement and
simple positional INSERT plan live for the batch. Refill one mutable parameter
buffer per row and call `PreparedStatementBatch::execute_mut` to avoid per-row
schema validation and fast-path resolution.

Plain persistent-table `SELECT COUNT(*) FROM table` reads and simple integer
primary-key projection reads stay on metadata or row-id lookup paths when no
security policy, temp table, view, expression projection, or additional filter
requires the full SQL executor. Other SQL shapes fall back to the normal parser
and planner.

Deferred table materialization now preserves valid index state for supported
single-table statements while avoiding full row materialization. Scalar integer
aggregate fast paths can scan encoded/persisted payload columns directly for common
aggregations when safe.

## Compaction

Over time, indexes may become fragmented. Rebuild them:

```bash
# Rebuild a specific index
decentdb rebuild-index --db=my.ddb --index=idx_users_name

# Or rebuild all indexes
decentdb rebuild-indexes --db=my.ddb
```

## Common Bottlenecks

1. **Cache too small** - Frequent page evictions
2. **Missing indexes** - Full table scans
3. **Large sorts** - Spilling to disk
4. **Long-running readers** - WAL file growth
5. **No checkpointing** - Large WAL files slow recovery
