# Configuration

DecentDB can be configured at database creation and runtime.

## Database Configuration

Configuration is set when opening the database:

```nim
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
- Nim API: `openDb(path, cachePages = n)`
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

## WAL Configuration

The Write-Ahead Log (WAL) ensures durability.

### Sync Mode

Controls how often data is synced to disk.

**Modes:**
- **FULL** (default): fsync on every commit. Maximum durability, slowest.
- **NORMAL**: fdatasync on commit. Good balance.
- **OFF** (testing only): No fsync. Fastest, unsafe.

```sql
PRAGMA wal_sync_mode = FULL;
PRAGMA wal_sync_mode = NORMAL;
```

### Checkpoint Threshold

When to automatically checkpoint the WAL:

```sql
-- Checkpoint when WAL reaches 10MB
PRAGMA checkpoint_threshold = 10000000;
```

Default: 1MB

### Checkpoint Timeout

Maximum time to wait for readers before forcing checkpoint:

```sql
-- Wait up to 60 seconds for readers
PRAGMA checkpoint_timeout = 60;
```

Default: 30 seconds

## Bulk Load Configuration

Configure bulk loading behavior:

```nim
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
opts.durability = dmDeferred  -- dmFull, dmNormal, dmNone
```

## Page Size

Set at database creation (cannot be changed):

Valid sizes: 2048, 4096, 8192, 16384 bytes

Default: 4096 bytes

**Considerations:**
- Smaller pages (2KB): Less memory usage, more I/O
- Larger pages (16KB): Better for large rows, less I/O
- 4KB is optimal for most workloads

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

### PRAGMA Commands

```sql
-- Get setting
PRAGMA wal_sync_mode;

-- Set setting
PRAGMA wal_sync_mode = NORMAL;
```

Available PRAGMAs:
- `wal_sync_mode` - Durability level
- `checkpoint_threshold` - Auto-checkpoint size
- `checkpoint_timeout` - Reader timeout
- `stats` - Database statistics
- `integrity_check` - Verify database

## Configuration File

Create `~/.decentdb/config` for default settings:

```
# Default database path
db = ~/myapp.ddb

# Default cache size
cacheMb = 64

# Default output format
format = json
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

```nim
var opts = defaultBulkLoadOptions()
opts.durability = dmDeferred
opts.disableIndexes = true
opts.checkpointOnComplete = true
```

### For Mixed Workloads

```bash
# Balanced settings
decentdb exec --db=my.ddb --sql="..." --cacheMb=64

# Normal durability
PRAGMA wal_sync_mode = NORMAL;
```

## Environment Variables

- `DECENTDB_CACHE_MB` - Default cache size in MB
- `DECENTDB_WAL_SYNC` - Default WAL sync mode
- `DECENTDB_FORMAT` - Default output format

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
# Fast but less safe
decentdb exec --db=dev.ddb --sql="..." --cacheMb=32
PRAGMA wal_sync_mode = NORMAL;
```

### Production Server

```bash
# Safe and fast
decentdb exec --db=prod.ddb --sql="..." --cacheMb=256
PRAGMA wal_sync_mode = FULL;
PRAGMA checkpoint_threshold = 10000000;  # 10MB
```

### Bulk Data Import

```nim
var opts = defaultBulkLoadOptions()
opts.batchSize = 50000
opts.syncInterval = 5
opts.disableIndexes = true
cachePages = 8192  # 32MB
```
