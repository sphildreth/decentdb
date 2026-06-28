# Performance Tuning

This guide covers the tuning knobs that matter most for DecentDB performance:
cache size, row storage mode, statement reuse, WAL sync policy, checkpointing,
and query shape. DecentDB defaults are durability-first. The highest-throughput
configuration can outperform SQLite-style embedded workloads, but it does so by
accepting a bounded post-crash durability window and, optionally, a
single-process-only coordination mode.

## Start with a Profile

Use a named profile first, then override only the few settings your workload
actually needs.

| Profile | Durability | Good For | Main Tradeoff |
|---|---:|---|---|
| `default` | Full fsync per commit | General-purpose durable opens | Small 4 MiB cache |
| `balanced` | Full fsync per commit | Durable apps with modest memory headroom | 16 MiB cache |
| `low_memory` | Full fsync per commit | Constrained devices | Fewer cached pages |
| `embedded_fast` | Full fsync per commit | Hot single-application workloads | More resident memory, less automatic checkpointing |
| `tuned_durable` | Full fsync per commit | High-memory durable benchmarks and services | Higher memory use |

The named profiles do not weaken commit durability. To get SQLite
`PRAGMA synchronous=NORMAL`-style write latency, you must explicitly choose a
relaxed WAL sync mode such as `wal_sync_mode=async_commit:10`.

Recommended starting points:

- Durable production app: `profile=embedded_fast;cache_size=64MB`
- Read-mostly durable app: `profile=tuned_durable;cache_size=128MB`
- SQLite NORMAL-style speed comparison:
  `profile=embedded_fast;cache_size=64MB;wal_sync_mode=async_commit:10;wal_autocheckpoint=0`
- Single-process benchmark or appliance:
  add `process_coordination=single_process_unsafe` only when one OS process can
  open the database.

## Durability Ladder

DecentDB can trade durability latency for throughput, but the settings are not
equivalent.

| Setting | What Commit Means | Crash Risk | When to Use |
|---|---|---|---|
| `wal_sync_mode=full` | WAL commit record is fsynced before commit returns | Durable against OS crash after commit returns | Default, safest production mode |
| `wal_sync_mode=normal` | WAL is still fsynced per commit, with reduced metadata sync | Lower sync overhead, still not SQLite NORMAL-like | Lower-risk tuning when full metadata sync is too costly |
| `wal_sync_mode=async_commit:10` | WAL frame is written, then background fsync runs about every 10 ms | The last interval of acknowledged commits can be lost after OS crash or power loss | High-throughput embedded workloads that can tolerate replaying recent work |

`async_commit` does not make commits partially visible. Atomicity, consistency,
and isolation remain intact. The tradeoff is durability timing: a successful
commit may not yet be on stable storage. Call `Db::sync()` after critical
batches in Rust to wait for the async WAL flusher. Bindings that do not expose
`Db::sync()` should checkpoint at controlled boundaries and validate that this
matches their recovery requirements.

Do not use test-only no-sync modes for application data. They exist for engine
tests and benchmarks that intentionally disable durability.

## Fast Embedded Recipe

Use this when you want DecentDB configured like a fast embedded database engine
and you can tolerate losing the most recent few milliseconds of acknowledged
commits after an OS crash or power loss.

Native option string used by C ABI-based bindings:

```text
profile=embedded_fast;cache_size=64MB;wal_sync_mode=async_commit:10;wal_autocheckpoint=0;process_coordination=single_process_unsafe
```

What each option does:

- `profile=embedded_fast` keeps hot table data resident, disables paged row
  storage by default, and keeps full sync unless overridden.
- `cache_size=64MB` gives the page cache room for common read working sets.
- `wal_sync_mode=async_commit:10` removes per-commit fsync latency and flushes
  in the background roughly every 10 ms.
- `wal_autocheckpoint=0` disables automatic checkpoint thresholds so latency
  does not move into the middle of a write-heavy benchmark or request path.
  Run checkpoints at controlled times.
- `process_coordination=single_process_unsafe` skips cross-process writer
  coordination overhead. Use it only when your deployment guarantees a single
  process opens the database file.

Python example:

```python
import decentdb

options = (
    "profile=embedded_fast;"
    "cache_size=64MB;"
    "wal_sync_mode=async_commit:10;"
    "wal_autocheckpoint=0;"
    "process_coordination=single_process_unsafe"
)

conn = decentdb.connect("app.ddb", options=options, stmt_cache_size=512)
cur = conn.cursor()

cur.execute("BEGIN")
for item_id, value in rows:
    cur.execute(
        "INSERT INTO items (id, value) VALUES (?, ?) "
        "ON CONFLICT (id) DO UPDATE SET value = excluded.value",
        (item_id, value),
    )
cur.execute("COMMIT")

# Run checkpoint work at an application boundary instead of during a hot path.
# Rust callers can use Db::sync() before checkpointing when they need a pure
# async_commit durability barrier.
conn.checkpoint()
```

Rust example:

```rust
use decentdb::{Db, DbConfig, ProcessCoordinationMode, WalSyncMode};

let mut config = DbConfig::embedded_fast();
config.cache_size_mb = 64;
config.wal_sync_mode = WalSyncMode::AsyncCommit { interval_ms: 10 };
config.process_coordination = ProcessCoordinationMode::SingleProcessUnsafe;
config.wal_checkpoint_threshold_pages = 0;
config.wal_checkpoint_threshold_bytes = 0;

let db = Db::open_or_create("app.ddb", config)?;
// Run work...
db.sync()?;       // explicit durability barrier for async_commit
db.checkpoint()?; // controlled checkpoint point
```

This profile is the first one to try when comparing against SQLite configured
with WAL mode and `PRAGMA synchronous=NORMAL`. It is not the same durability
contract as DecentDB's default profile or SQLite `synchronous=FULL`.

## Fair SQLite Comparisons

Benchmark results are only meaningful when the durability contracts are
comparable.

- DecentDB default `wal_sync_mode=full` should be compared to SQLite settings
  that fsync each committed transaction, not SQLite WAL/NORMAL.
- SQLite WAL/NORMAL usually avoids fsyncing every commit. The DecentDB
  comparison point is `wal_sync_mode=async_commit:<N>`, plus an explicit
  `Db::sync()` or checkpoint at the same application boundary.
- `process_coordination=single_process_unsafe` is comparable only to a
  deployment where the benchmark controls the only process with database access.
- If auto-checkpointing is disabled for one engine, checkpoint the other engine
  at the same logical point before measuring recovery, file size, or shutdown
  behavior.

## Cache Configuration

The page cache is the main read-performance knob. A cache that fits hot indexes
and frequently read table pages prevents repeated decoding and disk reads.

```bash
# Default durable behavior: 4 MiB cache.
decentdb exec --db=my.ddb --sql="SELECT 1"

# Explicit balanced behavior: 4096 pages = 16 MiB with 4 KiB pages.
decentdb exec --db=my.ddb --sql="SELECT 1" --cachePages=4096

# Or use megabytes.
decentdb exec --db=my.ddb --sql="SELECT 1" --cacheMb=64
```

Recommendations:

- Small datasets under 1 GiB: default 4 MiB cache, or 16 MiB with
  `DbConfig::balanced()` when the host has memory headroom.
- Medium datasets from 1 to 10 GiB: 16 to 64 MiB cache.
- Large datasets over 10 GiB: 64 to 256 MiB cache, then measure miss rate and
  resident memory pressure.

Default file-backed opens avoid work that is unnecessary until a workload
actually needs it: the background checkpoint worker starts on the first
auto-checkpoint threshold hit, and reactive subscription state is created only
when a watch API is used.

Default `process_coordination=auto` keeps the byte-range writer/checkpoint lock
as the correctness mechanism, but avoids per-commit sidecar writes for
writer-owner diagnostics. Use `process_coordination=required` when
cross-process diagnostic persistence is more important than the default-fast
single-process hot path.

## Row Storage and Hot Data

Two options heavily affect hot embedded workloads:

```text
retain_paged_row_sources_after_commit=true
paged_row_storage=false
```

`retain_paged_row_sources_after_commit=true` keeps decoded row sources available
after commit so repeated statements avoid reloading the same table data.

`paged_row_storage=false` favors resident in-memory table data. This improves
many hot read/write paths and avoids deferred-table reload cost, but it uses
more memory. Keep paged row storage enabled for very large cold tables or when
memory pressure matters more than single-process latency.

The `embedded_fast` and `tuned_durable` profiles set the hot-data options for
you while keeping full commit sync unless you override `wal_sync_mode`.

## Indexing Strategy

### Primary Keys

Always define primary keys. DecentDB automatically creates an index.

A single INT64 `PRIMARY KEY` column supports auto-assignment. Omit the column
from `INSERT` and DecentDB assigns the next sequential ID (`INT`, `INTEGER`,
`INT64`, and `BIGINT` are synonyms here).

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT
);

-- id is auto-assigned (1, 2, 3, ...)
INSERT INTO users (name) VALUES ('Alice');
INSERT INTO users (name) VALUES ('Bob');

-- Explicit id also works; counter advances past it.
INSERT INTO users VALUES (100, 'Carol');
INSERT INTO users (name) VALUES ('Dave');  -- id = 101
```

### Foreign Keys

Foreign keys are automatically indexed for efficient joins and cascades.

```sql
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES users(id)
);
```

### Trigram Indexes

Use trigram indexes for substring and `%pattern%` search:

```sql
CREATE INDEX idx_name_trgm ON users USING trigram(name);

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
CREATE INDEX idx_name_lower ON users((LOWER(name)));

SELECT * FROM users WHERE LOWER(name) = 'alice';
```

Supported expressions: `LOWER(col)`, `UPPER(col)`, `TRIM(col)`,
`LENGTH(col)`, and `CAST(col AS type)`.

### Partial Indexes

Index only rows that satisfy a condition:

```sql
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

Index:

- Primary keys, which are automatic.
- Foreign keys, which are automatic.
- Columns in `WHERE` clauses.
- Columns in `ORDER BY` when sorting large result sets.
- Columns in join conditions.
- Narrow projections that can be covered with `INCLUDE (...)`.

Avoid indexes on:

- Low-cardinality columns such as boolean flags unless paired with a selective
  partial predicate.
- Columns that are rarely queried.
- Very small tables.
- Heavy write paths where the index is not used by reads.

## Query Optimization

Use indexed predicates:

```sql
-- Fast: uses primary-key lookup.
SELECT * FROM users WHERE id = 42;

-- Slow unless name is indexed.
SELECT * FROM users WHERE name = 'Alice';
```

Limit result sets:

```sql
SELECT * FROM logs ORDER BY created_at DESC LIMIT 100;
```

Avoid sorting large datasets without an index. External merge sort can spill to
disk. Prefer an index that matches the `ORDER BY`, filter before sorting, and
apply `LIMIT` when the application only needs the first page.

Use parameterized SQL instead of unique literal SQL strings in hot loops. This
keeps the parser cache, plan cache, and binding-level statement cache hot:

```python
stmt = "SELECT name FROM users WHERE id = ?"
for user_id in ids:
    cur.execute(stmt, (user_id,))
    cur.fetchone()
```

## Bulk Loading and Write Batches

For large imports, prefer bulk load or one explicit transaction around many
statements. Autocommit turns each row into its own commit and pays the WAL sync
policy each time.

```bash
decentdb bulk-load --db=my.ddb --table=users --input=users.csv
```

Bulk load options:

- `--disableIndexes` skips index updates during load. Rebuild indexes after the
  import.
- `--batchSize=10000` controls rows per batch.
- `--syncInterval=10` is accepted by the bulk-load API and must be greater than
  zero.
- `--noCheckpoint` skips the post-load checkpoint; use it only when you have a
  later checkpoint plan.

For normal application writes:

```sql
BEGIN;
INSERT INTO events (id, payload) VALUES (1, 'a');
INSERT INTO events (id, payload) VALUES (2, 'b');
INSERT INTO events (id, payload) VALUES (3, 'c');
COMMIT;
```

With `wal_sync_mode=async_commit:<N>`, call Rust `Db::sync()` after a critical
batch when you need a pure WAL durability barrier. Bindings currently expose
checkpoint operations as the portable maintenance boundary; use them after
imports, before shutdown, or whenever deferred durability must be folded back
into the database file.

## WAL and Checkpointing

Checkpoints write committed WAL data back to the main database file. They also
bound recovery time and WAL file growth.

```bash
# Manual checkpoint.
decentdb checkpoint --db=my.ddb

# Run SQL and checkpoint before the process exits.
decentdb exec --db=my.ddb --sql="CREATE INDEX ix_logs_time ON logs(created_at)" --checkpoint
```

Disable auto-checkpointing only when you have a controlled checkpoint plan:

```text
wal_autocheckpoint=0
```

This can improve tail latency during a benchmark or ingest window, but the WAL
can grow and recovery work is deferred. Run `Db::checkpoint()`,
`conn.checkpoint()`, `ddb_db_checkpoint`, or `PRAGMA wal_checkpoint(TRUNCATE)` at
application boundaries such as after an import, before shutdown, or during a
maintenance window.

## Plan Cache

DecentDB ships a connection-local plan cache that reuses parsed parameterized
statements and reusable prepared plans across calls within a single `Db` handle.
The cache is enabled by default with a conservative 256 KiB total budget. See
`design/WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md` and ADR 0190-0194 for
the full contract.

Cache configuration:

```rust
use decentdb::{Db, DbConfig};

let mut config = DbConfig::default();
config.with_plan_cache(|c| {
    c.enabled = true;
    c.max_size_bytes = 512 * 1024;
});
let db = Db::open("app.ddb", config)?;
```

C ABI / binding open options:

```text
plan_cache_enabled=true|false
plan_cache_max_bytes=<bytes>
```

The default-on behavior is additive: existing binaries that do not set the
options get the cache. To opt out, set `plan_cache_enabled=false`.

Literal one-shot SQL follows the existing narrow parser cache path instead of
entering the generalized plan cache. Parameterized parsed statements use
second-use admission, and prepared-plan entries admit on the first miss. This
keeps one-shot overhead bounded while preserving the repeated-preparation win.

Diagnostics:

```sql
SELECT * FROM sys.plan_cache;
SELECT * FROM sys.plan_cache_summary;
PRAGMA flush_plan_cache;
```

The cache is invalidated on DDL, temp-schema, policy/mask changes, branch
operations, extension changes, and `PRAGMA flush_plan_cache`. `SET AUDIT
CONTEXT` does not affect the cache (ADR 0192).

WASM/browser note: the budget is per connection. Multi-worker browser apps
should set `plan_cache_max_bytes` to `low_memory_budget / N_workers`.

Native validation commands:

```bash
cargo bench -p decentdb --bench plan_cache
cd benchmarks/rust-baseline
cargo run --release --bin rust-baseline -- --plan-cache-benchmark --out-dir ../../.tmp/rust-baseline-plan-cache
```

## Prepared Writes and Fast Aggregates

Prepared insert execution prefers direct/default hot paths for supported
prepared shapes and avoids unnecessary `INCLUDE (...)` payload work when no
projected payload is needed. Reusing prepared statements for repeated writes
keeps this path hot.

For Rust callers inserting many rows inside one explicit transaction,
`SqlTransaction::prepared_batch` keeps the validated prepared statement and
simple positional `INSERT` plan live for the batch. Refill one mutable parameter
buffer per row and call `PreparedStatementBatch::execute_mut` to avoid per-row
schema validation and fast-path resolution.

Plain persistent-table `SELECT COUNT(*) FROM table` reads and simple integer
primary-key projection reads stay on metadata or row-id lookup paths when no
security policy, temp table, view, expression projection, or additional filter
requires the full SQL executor. Other SQL shapes fall back to the normal parser
and planner.

Deferred table materialization preserves valid index state for supported
single-table statements while avoiding full row materialization. Scalar integer
aggregate fast paths can scan encoded/persisted payload columns directly for
common aggregations when safe.

## Compaction

Over time, indexes may become fragmented. Rebuild them:

```bash
decentdb rebuild-index --db=my.ddb --index=idx_users_name
decentdb rebuild-indexes --db=my.ddb
```

## Monitoring

Check database statistics:

```bash
decentdb info --db=my.ddb
```

Use this to inspect page size, cache usage, WAL size, and active readers.
Long-running readers can keep old WAL versions alive and delay truncation.

## Common Bottlenecks

1. **Autocommit write loops**: batch writes in an explicit transaction or use
   bulk load.
2. **Full durability compared to SQLite NORMAL**: use a fair sync policy for
   benchmarks; use `async_commit` only when the durability window is acceptable.
3. **Cache too small**: increase `cache_size` until the hot working set fits.
4. **Deferred table reloads**: use `embedded_fast` or hot-data options for
   repeated access to resident working sets.
5. **Missing indexes**: add primary, foreign-key, predicate, sort, covering,
   trigram, or full-text indexes that match real query shapes.
6. **Unique literal SQL strings**: parameterize queries and keep statement
   caches enabled.
7. **Large sorts**: use index-backed ordering, filter early, and limit results.
8. **Long-running readers**: close stale cursors and reader connections so WAL
   checkpoints can advance.
9. **No checkpoint plan**: disabling auto-checkpointing requires manual
   checkpoints at controlled boundaries.
