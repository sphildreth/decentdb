# Performance Tuning

Tips for optimizing DecentDB performance.

## Cache Configuration

The page cache is critical for read performance.

```bash
# Default: 1024 pages = 4MB
decentdb exec --db=my.ddb --sql="SELECT 1" --cachePages=4096  # 16MB

# Or use megabytes
decentdb exec --db=my.ddb --sql="SELECT 1" --cacheMb=64  # 64MB
```

Recommendations:
- Small datasets (< 1GB): 4-16MB cache
- Medium datasets (1-10GB): 16-64MB cache
- Large datasets (> 10GB): 64-256MB cache

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

Use trigram indexes for text search:

```sql
CREATE INDEX idx_name_trgm ON users USING trigram(name);

-- Fast substring search
SELECT * FROM users WHERE name LIKE '%john%';
```

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
