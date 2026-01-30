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
WAL File: [Frame1][Frame2]...[FrameN]

Frame:
  [Type: 1 byte]
  [PageId: 4 bytes]
  [PayloadLen: 4 bytes]
  [Payload: N bytes]
  [Checksum: 8 bytes]
  [LSN: 8 bytes]
```

Frame Types:
- **0 (PAGE)**: Modified page data
- **1 (COMMIT)**: Transaction commit marker
- **2 (CHECKPOINT)**: Checkpoint completion

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
   └─ Modify pages in cache
   └─ Append PAGE frames to WAL (in memory)

3. COMMIT
   └─ Append COMMIT frame
   └─ fsync WAL to disk
   └─ Release write lock
   └─ Return success

4. (Later) CHECKPOINT
   └─ Copy committed pages to main DB
   └─ Append CHECKPOINT frame
   └─ Truncate WAL if safe
```

### Page Modification

When a page is modified:

1. Copy original page to WAL buffer
2. Apply changes to cached page
3. Mark page as dirty
4. On commit, write WAL buffer to disk

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
fdatasync(WAL file)
return success
```

Slightly faster, still very safe.

**DEFERRED Mode (Bulk Load):**
```
write(WAL frames)  // Batch many frames
fsync(WAL file)    // Periodically
```

Faster for bulk operations, risk of losing last batch.

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
- Checksum mismatch
- Size mismatch (partial write)
- Invalid frame type

Corrupt frames are skipped during recovery.

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
- WAL reaches threshold size (default: 1MB)
- Time since last checkpoint (configurable)

**Manual:**
```bash
decentdb exec --db=my.db --checkpoint
```

### Checkpoint Process

```
1. Block new write transactions
2. Copy all committed pages to main DB
3. Write CHECKPOINT frame to WAL
4. Determine safe truncate point
   (min snapshot LSN of all readers)
5. Truncate WAL if possible
6. Unblock writers
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

## WAL Size Management

### Growth Scenarios

**Normal:**
- Writes happen
- Checkpoint triggers periodically
- WAL stays bounded (2x checkpoint threshold)

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
ls -lh my.db.wal

# Database stats
decentdb exec --db=my.db --dbInfo
# Shows: WAL LSN, active readers
```

### WAL Archive (Future)

For very large databases, archived WAL segments could be:
- Compressed
- Stored remotely
- Used for point-in-time recovery

## Configuration

### Checkpoint Threshold

```sql
-- Checkpoint when WAL reaches 10MB
PRAGMA checkpoint_threshold = 10000000;
```

### Checkpoint Timeout

```sql
-- Wait up to 60 seconds for readers
PRAGMA checkpoint_timeout = 60;
```

### Sync Mode

```sql
-- Maximum durability
PRAGMA wal_sync_mode = FULL;

-- Balanced (recommended)
PRAGMA wal_sync_mode = NORMAL;
```

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
   - Default 1MB is good for most

4. **Monitor WAL growth**
   - Set up alerts if WAL > 100MB
   - Indicates reader or checkpoint issue

5. **Use appropriate durability mode**
   - FULL: Critical data, slowest
   - NORMAL: Good balance
   - DEFERRED: Bulk loads only

## Troubleshooting

### WAL Keeps Growing

**Check:**
```bash
# Active readers
decentdb exec --db=my.db --dbInfo
# Shows "Active readers: N"

# Long-running queries?
# Check application for open transactions
```

**Solution:**
- Close idle connections
- Reduce transaction duration
- Force checkpoint: `PRAGMA checkpoint;`

### Slow Recovery

**Cause:** Large WAL since last checkpoint

**Solution:**
- Checkpoint more frequently
- Lower checkpoint_threshold
- Monitor with: `PRAGMA stats;`

### Corruption After Crash

**Check:**
```bash
# Verify integrity
decentdb exec --db=my.db --sql="PRAGMA integrity_check"
```

**If corrupted:**
- Restore from backup
- Export data if partially readable
- Check hardware (disk issues)

## Further Reading

- [Storage Engine](storage.md) - Page format
- [Configuration](../../api/configuration.md) - WAL settings
- [Transactions](../../user-guide/transactions.md) - ACID details
