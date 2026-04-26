# ADR 0146: Cached Payload Append Path Optimization
**Date:** 2026-04-26
**Status:** Accepted

## Context

The engine's append-only persistence path (`append_uncompressed_with_first_page_patch`)
reads the **entire** table payload from disk on every commit, even when a cached copy
already exists in `EngineRuntime::cached_payloads`. For tables with millions of rows,
this causes quadratic slowdown: each commit re-reads hundreds of megabytes from the
overflow chain to append a few kilobytes of new rows.

Measured impact (5M rows, 50k rows/batch):
- Baseline: **9,437 rows/sec**
- Single-transaction (no commits): **794,592 rows/sec**
- 1M rows (smaller table): **1,020,902 rows/sec**

The root cause is that the append path calls `read_overflow()` to fetch the previous
payload from disk, then patches the row count and appends new rows. Meanwhile,
`EngineRuntime` already holds an `Arc<Vec<u8>>` of the same payload in its LRU cache,
populated by the previous commit's `cache_payload_insert`.

A previous attempt to use the cached payload failed because:
1. `Arc::clone` of a 500KB-700MB buffer is as expensive as reading from the OS page cache
2. The `rewrite_overflow_cached` skip calculation was incorrect for append operations
   (it didn't account for the patched row count in the first page)
3. The overflow chain cache was invalidated on every append, forcing full rewrites

## Decision

### 1. Take ownership from the cache instead of cloning

Replace `cached_payload()` (which clones the `Arc`) with `take_cached_payload()`
(which removes the entry from the cache and returns the `Arc` with refcount = 1).
This allows `Arc::try_unwrap` to succeed, giving us an owned `Vec<u8>` without
copying the payload bytes.

The cache is repopulated at the end of the commit with the new payload, so the next
commit will again find a cached entry.

### 2. Rebuild the chain cache instead of invalidating it

After a successful append, rebuild the overflow chain cache from the new pointer
instead of removing it. This allows subsequent commits to use
`rewrite_overflow_cached` with correct page IDs.

### 3. Use `rewrite_overflow_cached` with correct skip calculation

When appending, the unchanged prefix is everything before the appended rows.
The skip count is:

```
unchanged_bytes = old_payload_len (before append)
chunk_cap = page_size - OVERFLOW_HEADER_SIZE
skip = unchanged_bytes / chunk_cap
```

This is correct because:
- The row count patch is in the first page's header (bytes 8-12 after magic)
- The first page is always rewritten when its row count changes
- `rewrite_overflow_cached` skips full pages, not byte ranges
- The row count patch is within the first page's header, so page 0 is never skipped

### 4. Fallback to disk read when cache is empty

If the cache doesn't contain the payload (first commit, cache eviction, or
`cached_payloads_max_entries == 0`), fall back to the existing `read_overflow`
path. This preserves correctness for all existing workloads.

## Rationale

This optimization targets the dominant bottleneck for large-table append workloads
without changing the on-disk format, WAL semantics, or public API. It reuses
existing infrastructure (cached payloads, chain caches, cached rewrite) and only
changes the order of operations and ownership semantics.

The key insight is that the cache already exists and is already populated — it just
wasn't being used by the append path. By taking ownership instead of cloning, we
avoid the memory copy that made the previous attempt ineffective.

## Alternatives Considered

### Wait for full paged row storage (ADR 0145)
Rejected. ADR 0145 is default-off and requires format changes. This optimization
works with the current format and can be enabled immediately.

### Use `mmap` for the table payload
Rejected. Would require significant VFS changes and doesn't solve the append
amplification problem — it would just make the reads cheaper.

### Batch commits to reduce append frequency
Rejected. Changes the durability contract and doesn't solve the underlying O(N)
problem for large tables.

## Trade-offs

### Positive
- Eliminates O(N) disk reads per commit for append-only workloads
- No on-disk format change, no WAL changes, no API changes
- Reuses existing cached payload and chain cache infrastructure
- Falls back gracefully when cache is unavailable

### Negative
- The cache entry is consumed on each commit (taken, not cloned). If another
  code path expects the cache to persist across commits, it would need updating.
  Current callers only use it for the update-splice path, which also consumes it.
- Slightly more complex control flow in the append path.

## Implementation Plan

1. Add `take_cached_payload()` method to `EngineRuntime`
2. Modify the cached payload fetch condition to include append-only case
3. Replace `append_uncompressed_with_first_page_patch` path with:
   a. Take cached payload from cache
   b. Patch row count in-place
   c. Append new rows to the Vec
   d. Use `rewrite_overflow_cached` with correct skip
   e. Rebuild chain cache after commit
4. Add tests for multi-commit append correctness
5. Benchmark 5M row insert with 50k batch size

## References

- `crates/decentdb/src/exec/mod.rs` — `persist_to_db`, append path
- `crates/decentdb/src/record/overflow.rs` — `append_uncompressed_with_first_page_patch`, `rewrite_overflow_cached`
- `design/2026-04-25.ENGINE-MEMORY-WORK.md` — memory work context
- ADR 0145 — Paged Table Row Source (related but not required for this fix)
