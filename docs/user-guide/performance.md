# Performance Tuning

Tips for optimizing DecentDb performance.

## Cache Configuration

The page cache is critical for read performance.

```bash
# Default: 1024 pages = 4MB
decentdb exec --db=my.db --sql="SELECT 1" --cachePages=4096  # 16MB

# Or use megabytes
decentdb exec --db=my.db --sql="SELECT 1" --cacheMb=64  # 64MB
```

Recommendations:
- Small datasets (< 1GB): 4-16MB cache
- Medium datasets (1-10GB): 16-64MB cache
- Large datasets (> 10GB): 64-256MB cache

## Indexing Strategy

### Primary Keys

Always define primary keys. DecentDb automatically creates an index.

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,  -- Automatically indexed
    name TEXT
);
```

### Foreign Keys

Foreign keys are automatically indexed for efficient joins.

```sql
CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT REFERENCES users(id)  -- Auto-indexed
);
```

### Trigram Indexes

Use trigram indexes for text search:

```sql
CREATE INDEX idx_name_trgm ON users USING trigram(name);

-- Fast substring search
SELECT * FROM users WHERE name LIKE '%john%';
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
decentdb bulk-load --db=my.db --table=users --file=users.csv
```

Bulk load options:
- `--disable-indexes` - Skip index updates during load (rebuild after)
- `--durability=deferred` - Batch fsync operations
- `--batch-size=10000` - Rows per batch

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
decentdb exec --db=my.db --checkpoint

# Configure auto-checkpoint thresholds
decentdb exec --db=my.db --sql="PRAGMA checkpoint_threshold=10000000"  # 10MB
```

### Durability vs Performance

Trade-off between safety and speed:

```sql
-- Full durability (default): fsync on every commit
PRAGMA wal_sync_mode = FULL;

-- Normal: fdatasync (faster, still safe)
PRAGMA wal_sync_mode = NORMAL;

-- Testing only: no fsync
PRAGMA wal_sync_mode = OFF;
```

## Monitoring

Check database statistics:

```bash
# Database info
decentdb exec --db=my.db --dbInfo --verbose

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
decentdb rebuild-index --db=my.db --index=idx_users_name

# Or rebuild all indexes
decentdb exec --db=my.db --sql="PRAGMA rebuild_all_indexes"
```

## Common Bottlenecks

1. **Cache too small** - Frequent page evictions
2. **Missing indexes** - Full table scans
3. **Large sorts** - Spilling to disk
4. **Long-running readers** - WAL file growth
5. **No checkpointing** - Large WAL files slow recovery
