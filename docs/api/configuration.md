# Configuration

DecentDB can be configured at database creation and runtime.

## Database Configuration

Configuration is set when opening the database:

```rust
import decentdb/engine

# With default settings
let db = openDb("myapp.ddb")

# With custom cache size
let db2 = openDb("myapp.ddb", cachePages = 4096)
```

### Cache Size

The page cache keeps frequently accessed pages in memory.

**Configuration:**
- CLI: `--cachePages=<n>` or `--cacheMb=<n>`
- Rust API: `openDb(path, cachePages = n)`
- Default: 1024 pages (4MB with 4KB pages)

**Recommendations:**

| Database Size | Cache Size | Pages |
|--------------|------------|-------|
| < 100 MB | 4-16 MB | 1K-4K |
| 100 MB - 1 GB | 16-64 MB | 4K-16K |
| 1-10 GB | 64-256 MB | 16K-64K |
| > 10 GB | 256+ MB | 64K+ |

**Example:**
```bash
# Small database
decentdb exec --db=small.ddb --sql="SELECT 1" --cachePages=1024

# Large database
decentdb exec --db=large.ddb --sql="SELECT 1" --cacheMb=256
```

## Durability and Checkpointing

DecentDB uses a write-ahead log (WAL) and performs an `fsync()` on commit by default for durable ACID writes.

### Durability

DecentDB exposes a narrow SQL-level PRAGMA subset (`page_size`, `cache_size`, `integrity_check`, `database_list`). There is no SQL-level PRAGMA to change durability for normal DML (`INSERT`/`UPDATE`/`DELETE`). For high-throughput ingestion, use bulk-load durability modes instead:

- CLI: `decentdb bulk-load --durability=full|deferred|none` (default: `deferred`)
- Rust: `BulkLoadOptions.durability = dmFull|dmDeferred|dmNone`

### Checkpointing

- Manual checkpoint: `decentdb checkpoint --db=my.ddb`
- The engine configures sensible defaults at open (see `setCheckpointConfig` in the WAL module).

From the CLI you can override checkpoint/reader behavior for a single `exec` invocation:

```bash
decentdb exec --db=my.ddb \
  --checkpointBytes=67108864 \
  --readerWarnMs=60000 \
  --readerTimeoutMs=300000 \
  --forceTruncateOnTimeout \
  --sql="SELECT 1"
```

Note: if you pass *any* of the checkpoint/reader flags, the CLI overrides the engine defaults for that process (unset values become `0` / `false`).

## Bulk Load Configuration

Configure bulk loading behavior:

```rust
var opts = defaultBulkLoadOptions()

-- Rows per batch
opts.batchSize = 10000

-- Batches between fsync
opts.syncInterval = 10

-- Skip index updates during load
opts.disableIndexes = true

-- Checkpoint after load completes
opts.checkpointOnComplete = true

-- Durability mode
opts.durability = dmDeferred  -- dmFull, dmDeferred, dmNone
```

## Page Size

DecentDB currently uses a fixed 4096-byte page size (this is part of the on-disk format).

## Runtime Configuration

### Getting Current Settings

```bash
# Database info
decentdb info --db=my.ddb

# Include schema details (tables, columns, indexes)
decentdb info --db=my.ddb --schema-summary

# Shows:
# - Page size
# - Cache capacity
# - WAL LSN
# - Active readers
# - (optional) Schema summary (tables, columns, indexes)
```

### Checkpoint/Reader Settings (CLI)

DecentDB supports a limited SQLite-compatible PRAGMA subset:
- `PRAGMA page_size`
- `PRAGMA cache_size`
- `PRAGMA integrity_check`
- `PRAGMA database_list`
- `PRAGMA table_info(<table>)`

Assignment form is accepted only with constrained behavior:
- `PRAGMA page_size = <current_value>` is a no-op; changing page size requires reopening with `DbConfig.page_size`.
- `PRAGMA cache_size = <current_value>` is a no-op; changing cache size requires reopening with `DbConfig.cache_size_mb`.

To override checkpoint/reader settings for a single `exec` invocation, use:
- `--checkpointBytes`
- `--checkpointMs`
- `--readerWarnMs`
- `--readerTimeoutMs`
- `--forceTruncateOnTimeout`

For embedded Rust usage, call `setCheckpointConfig(db.wal, ...)` after opening the database.
## Configuration File

Create `~/.decentdb/config` for default settings:

```
# Default database path
db = ~/myapp.ddb

# Default cache size (either key is supported)
cacheMb = 64
# cachePages = 16384
```

Settings are overridden by command-line options.

## Performance Tuning

### For Read-Heavy Workloads

```bash
# Large cache
decentdb exec --db=my.ddb --sql="SELECT * FROM large_table" --cacheMb=256

# Create indexes for frequent queries
```

### For Write-Heavy Workloads

```rust
var opts = defaultBulkLoadOptions()
opts.durability = dmDeferred
opts.disableIndexes = true
opts.checkpointOnComplete = true
```

### For Mixed Workloads

```bash
# Balanced settings
decentdb exec --db=my.ddb --sql="..." --cacheMb=64
```

## Best Practices

1. **Set cache size based on data size**
   - Rule of thumb: 10-20% of database size

2. **Use deferred durability for bulk loads**
   - Much faster for large imports
   - Risk: may lose last batch on crash

3. **Checkpoint regularly**
   - Prevents WAL from growing too large
   - Improves recovery time

4. **Monitor performance**
   - Check stats regularly
   - Adjust cache if hit rate is low

5. **Test configuration changes**
   - Measure before and after
   - Use representative workload

## Configuration Examples

### Small Embedded Device

```bash
# Minimal memory usage
decentdb exec --db=embedded.ddb --sql="..." --cachePages=256  # 1MB
```

### Development/Testing

```bash
decentdb exec --db=dev.ddb --sql="..." --cacheMb=32
```

### In-Memory (Caching/Testing)

```bash
# Ephemeral in-memory database — no disk I/O
decentdb exec --db=:memory: --sql="CREATE TABLE cache (key TEXT PRIMARY KEY, val TEXT)"
```

In-memory databases are fully transactional, but do not write to disk. Use `save-as` to persist a snapshot to disk when needed.

### Production Server

```bash
# Larger cache (durability is fsync-on-commit by default)
decentdb exec --db=prod.ddb --sql="..." --cacheMb=256

# Optional: explicitly set checkpoint/reader policy for this invocation
# (see note above about overriding defaults)
decentdb exec --db=prod.ddb --sql="SELECT 1" --cacheMb=256 \
  --checkpointBytes=67108864 --readerWarnMs=60000 --readerTimeoutMs=300000 --forceTruncateOnTimeout
```

### Bulk Data Import

```rust
var opts = defaultBulkLoadOptions()
opts.batchSize = 50000
opts.syncInterval = 5
opts.disableIndexes = true
cachePages = 8192  # 32MB
```

## File Permissions

On POSIX systems (Linux, macOS), DecentDB creates database and WAL files with
mode `0600` (owner read/write only). This prevents other users on the same
machine from reading the database contents.

To use different permissions, set the desired umask before opening the database,
or change permissions on the files after creation.

## Resource Limits

DecentDB enforces the following internal limits:

| Resource | Limit | Notes |
|----------|-------|-------|
| SQL text length | 1 MB | Rejected at `prepare()` with `ERR_SQL` |
| AST node count | 10,000 | Prevents excessively complex queries |
| CTE/view expansion depth | 16 | Prevents infinite recursion |
| Trigger recursion depth | 16 | Prevents infinite trigger chains |
| Bind text/blob size | ~2 GB | Limited by `int32` byte length parameter |

The following resources are **not** limited by default:

| Resource | Notes |
|----------|-------|
| Query result set size | Use `LIMIT` to bound large queries |
| JOIN cardinality | Cartesian products can exhaust memory |
| Subquery nesting depth | Deep nesting may exhaust stack |

For an embedded single-process database these are lower risk than for a networked
server, but callers should use `LIMIT` clauses and validate input complexity.
