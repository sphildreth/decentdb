# Write-Ahead Log (WAL)

The WAL ensures ACID durability and enables snapshot isolation.

## Overview

All changes are written to the WAL before being applied to the main database file.

**Benefits:**
- Durability: Committed data survives crashes
- Performance: Sequential writes are fast
- Concurrency: Readers see stable snapshots
- Recovery: Fast replay on startup

## WAL Architecture

### File Format

```
WAL File: [32-byte WAL header][Frame1][Frame2]...[FrameN]

Frame:
  [Type: 1 byte]
  [PageId: 4 bytes]
  [Payload: fixed size for frame type]
  [Trailer: 8 bytes, reserved]
```

Frame Types:
- **0 (PAGE)**: Modified page data
- **1 (COMMIT)**: Transaction commit marker
- **2 (CHECKPOINT)**: Checkpoint completion
- **3 (PAGE_DELTA)**: Delta-encoded page update

### Logical Sequence Number (LSN)

Monotonically increasing ID for each frame:
- Used for ordering during recovery
- Captured by readers for snapshot isolation
- Stored as atomic 64-bit value

## Write Path

### Transaction Flow

```
1. BEGIN
   └─ Acquire write lock

2. INSERT/UPDATE/DELETE
   └─ Modify pages in cache or runtime state
   └─ Buffer full-page or delta WAL frames

3. COMMIT
   └─ Write buffered frames and COMMIT frame to WAL
   └─ Sync WAL according to WalSyncMode
   └─ Release write lock
   └─ Return success

4. (Later) CHECKPOINT
   └─ Copy committed pages to main DB
   └─ Append CHECKPOINT frame
   └─ Truncate WAL if safe
```

### Page Modification

When a page is modified:

1. Apply changes to the cached page or higher-level runtime state
2. Buffer either a full-page frame or a page-delta frame
3. On commit, write the buffered frames and commit marker to the WAL
4. Sync according to the configured `WalSyncMode`

### Commit Durability

**FULL Mode (Default):**
```
write(WAL frames)
fsync(WAL file)
return success
```

Guarantees data is on disk before commit returns.

**NORMAL Mode:**
```
write(WAL frames)
sync WAL data
return success
```

Reduced sync overhead compared with full metadata sync. This still performs a
per-commit data sync and is not the same latency contract as SQLite
`synchronous=NORMAL` in WAL mode.

**ASYNC_COMMIT Mode:**
```
write(WAL frames)
return success
background flusher fsyncs on the configured interval
```

Higher throughput for embedded workloads that can tolerate losing the latest
acknowledged commits inside the configured interval after an OS crash or power
loss. Use `Db::sync()` as an explicit durability barrier.

**DEFERRED Mode (Bulk Load):**
```
write(WAL frames)  // Batch many frames
fsync(WAL file)    // Periodically
```

Faster for bulk operations, risk of losing last batch.

## Cross-Process Coordination

For local on-disk databases, DecentDB can coordinate WAL ownership across native
OS processes through a rebuildable `<database>.coord` sidecar. The sidecar is
not authoritative data; it records database identity, WAL/checkpoint
generations, lock-owner metadata, and a fixed reader-slot table that can be
rebuilt from the database header and WAL.

Coordination preserves the existing one-writer/many-readers model:

- writer and checkpoint operations acquire OS byte-range locks through the VFS;
- reader transactions register a sidecar slot with their snapshot LSN;
- checkpoints skip copyback and truncation while local or process reader slots
  are active, then copy back and truncate once readers drain;
- each process refreshes its local WAL index when another process publishes a
  newer WAL or checkpoint generation;
- stale reader slots are reclaimed only when lock liveness can be proven.

The default `process_coordination=auto` enables this path when the VFS supports
local file locks. `process_coordination=required` fails open on unsupported
VFSes, and `single_process_unsafe` skips sidecar registration for callers that
know no other native process is concurrently using the file.

## Snapshot Isolation

### Reader Snapshots

Each reader captures the current `wal_end_lsn` at start:

```
Reader starts: snapshot_lsn = 1000

Writer commits: adds frames 1001, 1002, 1003
                commit frame at 1004

Reader sees: all frames <= 1000
Reader does NOT see: frames 1001-1004
```

### Consistent View

When reading page N:

1. Check if WAL has page N with LSN <= snapshot_lsn
2. If yes, use WAL version (newer)
3. If no, use main DB version
4. Result: Consistent point-in-time view

### No Locking Needed

Readers don't acquire locks:
- Atomic LSN read provides snapshot
- WAL is append-only
- Old frames not overwritten
- Multiple readers, different snapshots

## Recovery

### Startup Process

```
1. Read database header
2. Get last_checkpoint_lsn from header
3. Scan WAL from that LSN
4. For each frame:
   a. Verify checksum
   b. If valid and committed, apply
   c. Build in-memory page index
5. Database is ready
```

### Torn Write Detection

Incomplete frames are detected via:
- WAL logical-end and frame-size validation
- Size mismatch or incomplete frame body (partial write)
- Invalid frame type

Frames beyond the durable logical end are ignored during recovery; malformed
frames inside the durable range are treated as corruption.

### Recovery Time

Typical recovery:
- Small WAL (< 10MB): < 1 second
- Medium WAL (10-100MB): 1-5 seconds
- Large WAL (> 100MB): 5-30 seconds

Recovery time proportional to WAL size since last checkpoint.

## Checkpointing

### What is Checkpointing?

Copy committed pages from WAL to main database file.

### When to Checkpoint

**Automatic:**
- WAL reaches the configured page or byte threshold
- a background checkpoint worker usually handles threshold-triggered work

**Manual:**
```bash
decentdb checkpoint --db=my.ddb
```

### Checkpoint Process

```
1. Acquire the checkpoint lock
2. Determine the latest committed safe LSN
3. Copy eligible committed pages to the main DB when no readers need older WAL
4. Write a durable CHECKPOINT frame to WAL
5. Determine safe truncate point
   (min snapshot LSN of all readers)
6. Truncate WAL if possible
7. Publish the new checkpoint generation
```

### Reader Coordination

WAL cannot be truncated past `min_reader_lsn`:

```
Reader A: snapshot_lsn = 500 (still active)
Reader B: snapshot_lsn = 800 (still active)
Writer:   last_commit = 1000

Checkpoint copies pages 1-1000 to main DB
Safe truncate point: 500 (min of reader LSNs)
Can truncate frames 1-499
Must keep frames 500-1000 for readers
```

### Forced Checkpoint

If readers hold snapshots too long:

```
1. Wait for checkpoint_timeout (default: 30s)
2. Copy pages up to writer's LSN
3. Log warning about blocking readers
4. Skip WAL truncation
```

### SQL with Checkpoint

Execute SQL and immediately checkpoint in a single command:

```bash
# Create index and checkpoint to main database file
decentdb exec --db=my.ddb --sql="CREATE INDEX ix_name ON users(name)" --checkpoint
```

This is useful when:
- Creating indexes that must survive WAL deletion
- Performing DDL operations that should be immediately persisted
- Ensuring data is in the main file before archival/backup

The response includes `checkpoint_lsn` showing where the data was persisted.

## WAL Size Management

### Growth Scenarios

**Normal:**
- Writes happen
- Checkpoint triggers periodically
- WAL stays bounded around the configured checkpoint thresholds

**With Active Readers:**
- Readers hold old snapshots
- Checkpoint can't truncate past their LSN
- WAL grows until readers finish

**Solution:**
- Don't hold long-running transactions
- Use streaming for large exports
- Monitor WAL size

### Monitoring

```bash
# Check WAL size
ls -lh my.ddb.wal

# Database stats
decentdb info --db=my.ddb
# Shows: WAL LSN, active readers
```

## Configuration

DecentDB supports safe SQLite-compatible PRAGMA probes for common WAL and
configuration questions. `PRAGMA journal_mode` reports `wal`,
`PRAGMA synchronous` reports the open-time sync mode, and
`PRAGMA wal_checkpoint(...)` maps to a WAL-only checkpoint operation. The
embedding API and CLI checkpoint command may also run optional payload
compaction maintenance. Checkpoint and reader-retention policy are still
configured through API/CLI settings; PRAGMA assignment does not weaken
durability.

### Checkpointing

```bash
# Manual checkpoint
decentdb checkpoint --db=my.ddb

# Or checkpoint after a specific exec
decentdb exec --db=my.ddb --sql="CREATE INDEX ..." --checkpoint

# Native/binding option string example for threshold tuning.
wal_checkpoint_threshold_bytes=67108864;wal_checkpoint_threshold_pages=4096
```

For embedded Rust usage, set `DbConfig::wal_checkpoint_threshold_pages` and
`DbConfig::wal_checkpoint_threshold_bytes` before opening the database.

### Durability

Commits are durable (full WAL sync on commit) by default. For regular SQL,
choose `WalSyncMode::Normal` or `WalSyncMode::AsyncCommit` only when that
durability tradeoff is acceptable. CLI bulk-load writes use the database
handle's open-time WAL sync mode and expose batch, index-maintenance, and
post-load checkpoint options.

## Best Practices

1. **Checkpoint regularly**
   - Keeps WAL size manageable
   - Faster recovery time
   - Reclaims disk space

2. **Avoid long-running readers**
   - Don't hold transactions open
   - Stream large queries
   - Use pagination

3. **Size checkpoint threshold appropriately**
   - Smaller: More frequent I/O, smaller WAL
   - Larger: Less I/O, larger WAL
   - Default 64MB is good for most workloads

4. **Monitor WAL growth**
   - Set up alerts if WAL > 100MB
   - Indicates reader or checkpoint issue

5. **Use bulk load intentionally**
   - `--batchSize` controls application-level batch sizing
   - `--disableIndexes` can reduce index-maintenance cost during load
   - `--noCheckpoint` defers post-load checkpoint work until your maintenance
     window

6. **Checkpoint after large DDL or maintenance operations**
   - Use `--checkpoint` with CREATE INDEX to persist immediately
   - Keeps subsequent recovery and backup operations bounded
   - Example: `decentdb exec --db=my.ddb --sql="CREATE INDEX ..." --checkpoint`

## Troubleshooting

### WAL Keeps Growing

**Check:**
```bash
# Active readers
decentdb info --db=my.ddb
# Shows "Active readers: N"

# Long-running queries?
# Check application for open transactions
```

**Solution:**
- Close idle connections
- Reduce transaction duration
- Force checkpoint: `decentdb checkpoint --db=...` (or `decentdb exec --checkpoint ...` for a checkpoint after specific SQL)

### Slow Recovery

**Cause:** Large WAL since last checkpoint

**Solution:**
- Checkpoint more frequently
- Use smaller `wal_checkpoint_threshold_bytes` or
  `wal_checkpoint_threshold_pages` values for long-running embedded usage
- Monitor with: `decentdb stats --db=...`

### Corruption After Crash

**Check:**
```bash
# Verify on-disk structure
decentdb verify-header --db=my.ddb

# Verify an index (repeat per index as needed)
decentdb verify-index --db=my.ddb --index=idx_users_name
```

**If corrupted:**
- Restore from backup
- Export data if partially readable
- Check hardware (disk issues)

## Further Reading

- [Storage Engine](storage.md) - Page format
- [Configuration](../api/configuration.md) - WAL settings
- [Transactions](../user-guide/transactions.md) - ACID details
