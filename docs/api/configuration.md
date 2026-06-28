# Configuration

DecentDB is configured primarily when a database handle is created or opened.
Runtime SQL PRAGMAs exist for compatibility and inspection, but durability,
cache size, process coordination, encryption, and most storage behavior are
open-time settings.

## Database Configuration

Configuration is set when opening or creating the database:

```rust
use decentdb::{Db, DbConfig};

// With default settings
let db = Db::open_or_create("myapp.ddb", DbConfig::default())?;

// With custom cache size
let mut config = DbConfig::default();
config.cache_size_mb = 16;
let db2 = Db::open_or_create("myapp.ddb", config)?;
# Ok::<(), decentdb::DbError>(())
```

### Cache Size

The page cache keeps frequently accessed pages in memory.

**Configuration:**
- CLI: `--cachePages=<n>` or `--cacheMb=<n>`
- Rust API: `DbConfig.cache_size_mb`
- C ABI / binding option strings: `cache_size=<pages>`,
  `cache_size=<n>MB`, or `cache_size=<n>GB`
- Default durable/low-memory profile: 1024 pages (4MB with 4KB pages)
- Explicit balanced profile: 4096 pages (16MB with 4KB pages)

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
decentdb exec --db=small.ddb --sql="SELECT 1"

# Constrained host / low-memory profile behavior
decentdb exec --db=small.ddb --sql="SELECT 1" --cachePages=1024

# Large database
decentdb exec --db=large.ddb --sql="SELECT 1" --cacheMb=256
```

Rust callers can also start from named durable profiles:

```rust
use decentdb::DbConfig;

let balanced = DbConfig::balanced();
let low_memory = DbConfig::low_memory();
let tuned = DbConfig::tuned_durable();
let embedded_fast = DbConfig::embedded_fast();
# let _ = (balanced, low_memory, tuned, embedded_fast);
```

Named profiles keep full WAL sync unless the caller explicitly overrides
`DbConfig.wal_sync_mode` or passes `wal_sync_mode` / `synchronous` in an option
string. `embedded_fast` is the recommended opt-in profile for single-process
embedded applications with a hot working set and repeated small writes.
`tuned_durable` is the higher-memory benchmark/power-user profile. Both are
explicit because they raise memory use and change row-source/checkpoint
behavior.

C ABI and binding option strings can use the same profiles:

```text
profile=embedded_fast
profile=tuned_durable;cache_size=128MB
```

Explicit options in the same string override profile values. .NET connection
strings expose this as `Performance Profile=embedded_fast`.

## Durability and Checkpointing

DecentDB uses a write-ahead log (WAL). The default `WalSyncMode::Full` performs
a WAL sync before each commit is acknowledged, which is the durable ACID write
path.

Rust callers configure the WAL sync policy with `DbConfig.wal_sync_mode`:

```rust
use decentdb::{Db, DbConfig, WalSyncMode};

let config = DbConfig {
    wal_sync_mode: WalSyncMode::Full,
    ..DbConfig::default()
};
let db = Db::open_or_create("app.ddb", config)?;
# Ok::<(), decentdb::DbError>(())
```

C ABI and binding option strings accept:

```text
wal_sync_mode=full
wal_sync_mode=normal
wal_sync_mode=async_commit:10
synchronous=full
```

`async_commit:<milliseconds>` acknowledges commits after their WAL frames are
written and uses a background fsync thread. Use `Db::sync()` as an explicit
durability barrier in that mode. Clean handle close also performs a final
flush. The named durable profiles do not select async commit.

## Cross-Process WAL Coordination

Local on-disk databases coordinate multiple native OS processes by default when
the VFS supports byte-range file locks. Coordination uses a rebuildable
`<database>.coord` sidecar to serialize writers/checkpoints, retain WAL frames
for readers in other processes, and publish WAL/checkpoint generation changes.

Rust configuration:

```rust
use decentdb::{Db, DbConfig, ProcessCoordinationMode};

let config = DbConfig {
    process_coordination: ProcessCoordinationMode::Required,
    process_coordination_timeout_ms: 30_000,
    ..DbConfig::default()
};

let db = Db::open("app.ddb", config)?;
# Ok::<(), decentdb::DbError>(())
```

C ABI and binding open options:

```text
process_coordination=auto
process_coordination=required
process_coordination=single_process_unsafe
process_coordination_timeout_ms=30000
plan_cache_enabled=true|false
plan_cache_max_bytes=<bytes>
```

The plan cache options are additive: old binaries that do not set them
get the new default behavior (connection-local plan caching enabled,
default 256 KiB). To opt out, set `plan_cache_enabled=false`. See
`design/_archive/WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md` and ADR 0190-0194
for the full contract.

Use `required` for applications that must fail when cross-process protection is
unavailable. Use `single_process_unsafe` only for known single-process or
immutable inspection workflows.

The `.coord` sidecar is rebuildable from the durable database header and WAL.
Default opens therefore avoid fsyncing sidecar metadata; byte-range locks remain
the correctness mechanism for writer/checkpoint serialization.

In `auto` mode, current-writer diagnostics are maintained in process memory so
each durable commit does not need an extra sidecar metadata write. `required`
mode persists those diagnostics for applications that prefer cross-process
operational visibility over the default-fast commit path.

## Local Transparent Data Encryption (TDE)

TDE is configured at create/open time with `DbConfig::encryption` or C ABI open
options. The database file, WAL, and sync journal are encrypted through the VFS
layer when a key is supplied.

```rust
use decentdb::{Db, DbConfig, DbEncryptionConfig};

let config = DbConfig {
    encryption: Some(DbEncryptionConfig::from_key_bytes(
        b"application-managed high entropy key bytes",
    )?),
    ..DbConfig::default()
};

let db = Db::create("secure.ddb", config.clone())?;
let reopened = Db::open("secure.ddb", config)?;
# Ok::<(), decentdb::DbError>(())
```

C ABI and binding callers can use open options:

```text
encryption_key_hex=00112233445566778899aabbccddeeff
encryption_key=development-only-text-key
```

`encryption_key_hex` / `tde_key_hex` decode hex bytes. `encryption_key` /
`tde_key` use the UTF-8 bytes of the option value. Avoid logging option strings
that contain key material.

TDE v1 provides local encryption-at-rest confidentiality. Key storage, key
rotation, and authenticated page/chunk encryption are separate follow-up
concerns; keep production keys in a platform key store, secure enclave, KMS, or
equivalent secret manager.

### Durability

DecentDB exposes SQLite-style compatibility PRAGMAs for common tooling probes,
but SQL-level PRAGMAs do not weaken durability for normal DML
(`INSERT`/`UPDATE`/`DELETE`). `PRAGMA journal_mode` reports the engine's
WAL-only mode, and `PRAGMA synchronous` accepts only no-op assignments that
match the database's open-time WAL sync configuration. For high-throughput
ingestion, use explicit transactions or the bulk-load API/CLI. Bulk-load
durability still follows the database handle's open-time WAL sync mode.

### Checkpointing

- Manual checkpoint: `decentdb checkpoint --db=my.ddb`
- Programmatic checkpoint: `Db::checkpoint()` / `ddb_db_checkpoint`
- Default size-triggered auto-checkpointing wakes after 4096 dirty page
  versions or 64 MiB of WAL growth, gated by active readers and shared-handle
  safety.

Rust callers configure checkpoint policy with `DbConfig`:

```rust
use decentdb::{Db, DbConfig};

let config = DbConfig {
    wal_checkpoint_threshold_pages: 8192,
    wal_checkpoint_threshold_bytes: 128 * 1024 * 1024,
    checkpoint_timeout_sec: 30,
    background_checkpoint_worker: true,
    auto_checkpoint_on_open_mb: 16,
    ..DbConfig::default()
};

let db = Db::open_or_create("my.ddb", config)?;
# Ok::<(), decentdb::DbError>(())
```

C ABI and binding option strings can set the size triggers:

```text
wal_autocheckpoint=4096
wal_checkpoint_threshold_pages=8192
wal_checkpoint_threshold_bytes=134217728
```

`wal_autocheckpoint=0` disables both page and byte threshold triggers for that
handle. Checkpoints fold WAL frames into the database file and can truncate the
WAL when no active readers need retained versions; committed data is already
durable under the default full WAL sync mode.

## Bulk Load Configuration

Configure Rust bulk loading behavior:

```rust
use decentdb::{BulkLoadOptions, Db, DbConfig, Value};

let db = Db::open_or_create("bulk.ddb", DbConfig::default())?;
let rows = vec![vec![Value::Int64(1), Value::Text("Ada".to_string())]];
let options = BulkLoadOptions {
    batch_size: 10_000,
    sync_interval: 10,
    disable_indexes: false,
    checkpoint_on_complete: true,
};
db.bulk_load_rows("users", &["id", "name"], &rows, options)?;
# Ok::<(), decentdb::DbError>(())
```

The CLI exposes the same option names as camel-case flags:

```bash
decentdb bulk-load --db=bulk.ddb --table=users --input=users.csv \
  --batchSize=10000 --syncInterval=10 --noCheckpoint
```

## Page Size

DecentDB supports 4096, 8192, and 16384-byte page sizes at database creation.
The default is 4096 bytes. Existing database files keep their creation-time page
size; changing it requires creating a new database and migrating data.

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

### SQL PRAGMA Compatibility

DecentDB supports a safe SQLite-compatible PRAGMA subset for configuration
inspection, schema discovery, and common ORM/tooling setup probes:

- Storage and integrity: `page_size`, `cache_size`, `integrity_check`,
  `quick_check`, `database_list`
- Safety mode probes: `foreign_keys`, `journal_mode`, `synchronous`,
  `encoding`, `locking_mode`, `temp_store`
- WAL maintenance: `wal_checkpoint`
- Application metadata: `schema_version`, `user_version`, `application_id`
- Timeout tuning for queued writes: `busy_timeout`
- Introspection: `table_info(table)`, `table_xinfo(table)`, `table_list`,
  `index_list(table)`, `index_info(index)`, `index_xinfo(index)`,
  `foreign_key_list(table)`

Assignment form is accepted only when it is safe:

- `PRAGMA page_size = <current_value>` is a no-op; changing page size requires
  reopening with `DbConfig.page_size`.
- `PRAGMA cache_size = <current_value>` is a no-op; changing cache size
  requires reopening with `DbConfig.cache_size_mb`.
- `PRAGMA foreign_keys = ON` is a no-op because DecentDB always enforces
  foreign keys. `OFF` is rejected.
- `PRAGMA journal_mode = WAL` is a no-op and returns `wal`; other journal
  modes are rejected.
- `PRAGMA synchronous = FULL|NORMAL|OFF` succeeds only when the requested value
  matches the database's open-time WAL sync mode. It is not a runtime durability
  downgrade.
- `PRAGMA encoding = UTF-8`, `PRAGMA locking_mode = NORMAL`, and
  `PRAGMA temp_store = DEFAULT|FILE|0|1` are accepted as safe compatibility
  no-ops.
- `PRAGMA user_version = <signed 32-bit integer>` and
  `PRAGMA application_id = <signed 32-bit integer>` are durable,
  transactional application metadata values.
- `PRAGMA busy_timeout = <milliseconds>` sets the connection-local timeout used
  by queued writes when an individual queued call does not provide its own
  timeout.

Known unsafe or unsupported PRAGMAs are rejected with explicit SQL errors
instead of being silently ignored. Examples include `read_uncommitted`,
`ignore_check_constraints`, `defer_foreign_keys`, `journal_mode = OFF`, and
`temp_store = MEMORY`.

Checkpoint policy is open-time engine configuration. The CLI `exec` command
supports `--checkpoint` to run a manual checkpoint after execution, but it does
not expose per-invocation reader timeout or checkpoint threshold flags.

### Lua Extension Trust

Lua extension execution is configured when a database connection is opened.
Installed and enabled packages are inert unless the current connection allows
the package by name and exact content hash.

**Configuration:**

- CLI: `--allow-extension=<name@sha256:hash>` on `exec` and `repl`
- CLI development override: `--allow-unsigned-extensions`
- Rust API: `DbConfig::extension_trust_anchors`
- Rust API development override:
  `DbConfig::extension_unsigned_development_mode = true`
- C ABI open options:
  `allow_extension=<name@sha256:hash>` and
  `allow_unsigned_extensions=true`

Trust entries may include Ed25519 publisher keys:

```text
text_tools@sha256:7b3f...@publisher-key-1@base64:...
```

The unsigned-development override is intended only for local authoring and test
databases. Production applications should validate signed packages and open
connections with explicit package-hash allowlists.

## Configuration File

Some deployments keep wrapper-level defaults in `~/.decentdb/config`:

```
# Default database path
db = ~/myapp.ddb

# Default cache size used by custom wrappers around the CLI
cacheMb = 16
# cachePages = 4096
```

The current in-tree CLI does not read this file directly; treat this as an
application-level convention unless your wrapper implements it.

## Performance Tuning

### For Read-Heavy Workloads

```bash
# Large cache
decentdb exec --db=my.ddb --sql="SELECT * FROM large_table" --cacheMb=256

# Create indexes for frequent queries
```

### For Write-Heavy Workloads

```rust
let options = BulkLoadOptions {
    batch_size: 50_000,
    sync_interval: 10,
    disable_indexes: false,
    checkpoint_on_complete: true,
};
```

### For Mixed Workloads

```bash
# Balanced settings
decentdb exec --db=my.ddb --sql="..."
```

## Best Practices

1. **Set cache size based on data size**
   - Rule of thumb: 10-20% of database size

2. **Use explicit transactions or bulk load for large imports**
   - Avoid per-row autocommit overhead
   - Keep the default full WAL sync when crash durability matters

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
# Constrained memory usage
decentdb exec --db=embedded.ddb --sql="..." --cachePages=1024  # 4MB
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

# Optional: run a manual checkpoint after the statement
decentdb exec --db=prod.ddb --sql="SELECT 1" --cacheMb=256 --checkpoint
```

### Bulk Data Import

```rust
let mut config = DbConfig::default();
config.cache_size_mb = 32;

let options = BulkLoadOptions {
    batch_size: 50_000,
    sync_interval: 5,
    disable_indexes: false,
    checkpoint_on_complete: true,
};
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
