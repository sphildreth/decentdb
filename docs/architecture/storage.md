# Storage Engine

The storage engine manages how data is organized on disk.

## Page-Based Storage

DecentDb uses a page-based storage model where all data is stored in fixed-size pages.

### Page Size

Default: **4096 bytes (4KB)**

Valid sizes: 2048, 4096, 8192, 16384 bytes

Set at database creation and cannot be changed.

**Why 4KB?**
- Matches most OS page sizes
- Matches SSD block sizes
- Good balance of I/O efficiency and memory usage
- Low internal fragmentation for typical rows

### Page Types

1. **Database Header Page** (Page 1)
   - Magic number, format version
   - Page size, schema cookie
   - Root page pointers
   - Freelist head
   - Checksums

2. **B+Tree Internal Pages**
   - Store routing keys
   - Point to child pages
   - Compact varint-based cell format

3. **B+Tree Leaf Pages**
   - Store actual data
   - Variable-length values
   - Overflow pointers for large data

4. **Overflow Pages**
   - Store values too large to fit inline in a B+Tree leaf
   - Chained for very large values
   - Separate allocation from B+Tree

5. **Freelist Pages**
   - Track free pages
   - Reused for new allocations
   - Reduces file growth

## Record Format

### Row Storage

Rows are stored as key-value pairs in B+Tree leaves:

```
Key: RowId (uint64; stored as varint in leaf cells)
Value: Encoded row data
```

### Encoding

Row values are encoded as a small self-describing record:

```
[field_count: varint]
   repeated field_count times:
      [kind: u8]
      [payload_len: varint]
      [payload_bytes]
```

This is defined in `src/record/record.nim` and ADR 0030.

**Value kinds (payload encoding):**
- NULL: empty payload
- BOOL: 1 byte (0/1)
- INT64: ZigZag + varint-encoded `uint64` (compact for small signed ints)
- FLOAT64: 8 bytes (IEEE 754, little-endian)
- TEXT/BLOB: raw bytes
- TEXT/BLOB overflow pointers: 8-byte payload containing `[overflow_page_id u32][overflow_len u32]`

Some builds may also store opportunistically-compressed TEXT/BLOB payloads and transparently decompress on decode.

### Overflow Handling

DecentDB uses overflow pages in two places:

1) **Record value overflow** (large TEXT/BLOB values stored out-of-line)
2) **B+Tree value overflow** (very large encoded records stored out-of-line)

#### Record value overflow

Large `TEXT`/`BLOB` payloads may be stored as an overflow chain. In that case, the record stores a small overflow pointer payload:

```
[overflow_page_id: 4 bytes]
[overflow_len: 4 bytes]
```

#### Overflow page layout

Overflow pages store chunks of bytes and link to the next page:

```
Overflow page: [next_page: 4 bytes][chunk_len: 4 bytes][chunk_bytes...]
```

For record value overflow, `overflow_len` is the total length of the logical value bytes.

#### B+Tree value overflow

If the *entire encoded record* does not fit inline in a leaf cell, the leaf cell can store an overflow page id and the record bytes are stored out-of-line as an overflow chain.

## B+Tree Structure

### Node Layout

**Leaf Page:**
```
[page_type: 1 byte]
[cell_count: 2 bytes]
[next_leaf: 4 bytes]
[cells...]
```

Each cell (FormatVersion 4):
```
[key: varint]
[control: varint]
[inline_payload: bytes]   (only when control indicates inline)
```

`control` packs both “inline length” and “overflow pointer”:
- `is_overflow = control & 1`
- `value = control >> 1`
- If `is_overflow == 0`: `value` is the inline payload length in bytes
- If `is_overflow == 1`: `value` is the overflow page id (no inline payload)

**Internal Page:**
```
[page_type: 1 byte]
[cell_count: 2 bytes]
[right_child: 4 bytes]
[cells...]
```

Each cell (FormatVersion 4):
```
[key: varint]
[child_page: varint]
```

### Tree Operations

**Search:**
1. Start at root page
2. If internal page, find child pointer for key range
3. Repeat until leaf page
4. Scan leaf for exact key

**Insert:**
1. Find target leaf page
2. If space available, insert sorted
3. If full, split leaf into two pages
4. Update parent with new separator key
5. If root splits, create new root level

**Delete:**
1. Find target leaf page
2. Remove cell
3. Re-encode remaining cells
4. Note: Merge not implemented (Post-MVP)

**Split:**
- Split point: Middle of sorted keys
- Creates two roughly equal pages
- Updates parent with separator key
- May cascade up to root

### Fanout

With 4KB pages:
- Internal nodes: ~340 keys (12 bytes each)
- Leaf nodes: ~50-100 entries (depends on value size)
- Tree height: Usually 2-3 levels
- Can store millions of rows with 2-3 disk reads

## Page Cache

The pager maintains an in-memory cache of recently used pages.

### Cache Management

**Pin/Unpin:**
- Pages in active use are "pinned"
- Pinned pages are not evicted
- Operations must unpin when done

**Dirty Tracking:**
- Modified pages marked "dirty"
- Written to WAL immediately
- Written to main file at checkpoint

**Eviction:**
- Unpinned, clean pages can be evicted
- Simple policy: First unpinned found
- Future: LRU or clock algorithm

### Cache Configuration

```nim
# Default: 1024 pages = 4MB
let db = openDb("my.db", cachePages = 4096)  # 16MB
```

**Sizing Guidelines:**
- Small DB (< 100MB): 1K-4K pages
- Medium DB (100MB - 1GB): 4K-16K pages  
- Large DB (> 1GB): 16K+ pages
- Aim for 20-30% of working set in cache

## Free Space Management

### Freelist

Tracks pages that are no longer in use:

```
Freelist trunk page:
  [next_trunk: 4 bytes]
  [page_count: 4 bytes]
  [page_ids...]
```

**Allocation:**
1. Check freelist first
2. If empty, extend file
3. Return page ID

**Deallocation:**
1. Add page to freelist
2. Clear page content
3. Available for reuse

### B+Tree Compaction

Pages with low utilization can be rebuilt:

```bash
# Rebuild index to reclaim space
decentdb rebuild-index --db=my.db --index=idx_name
```

This:
1. Scans all entries
2. Builds new compact tree
3. Frees old pages to freelist

## Storage Durability

### Page Write Order

1. Write dirty pages to WAL
2. fsync WAL
3. Mark transaction committed
4. (Later) Copy to main file at checkpoint

### Checksums

Each page has a CRC-32C checksum:
- Verified on every read
- Detects corruption immediately
- Fail fast for data integrity

### Recovery

On open:
1. Read database header
2. Verify header checksum
3. Scan WAL from last checkpoint
4. Apply committed frames
5. Discard uncommitted frames
6. Database ready

## Storage Statistics

Monitor storage health:

```bash
# Database stats
decentdb exec --db=my.db --dbInfo --verbose

# Shows:
# - Page size
# - Total pages
# - Cache usage
# - WAL size
# - Free pages
```

## Best Practices

1. **Choose appropriate page size**
   - 4KB for most workloads
   - 8KB if many large rows
   - 2KB for memory-constrained

2. **Size cache appropriately**
   - Monitor hit rate
   - Increase if many disk reads
   - Balance with other app memory

3. **Checkpoint regularly**
   - Prevents large WAL
   - Faster recovery
   - Reclaims WAL space

4. **Monitor page utilization**
   - Rebuild indexes if < 50%
   - Check after bulk deletes

5. **Use overflow for large data**
   - Don't store huge blobs in main table
   - Consider file storage with paths

## Further Reading

- [B+Tree Details](btree.md) - Node structure, splits
- [WAL & Recovery](wal.md) - Durability, checkpoints
- [Configuration](../../api/configuration.md) - Cache settings
