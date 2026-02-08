## Trigram Postings Chunked Storage Format
**Date:** 2026-02-02
**Status:** Accepted

### Decision
Chunk trigram postings into fixed-size B+Tree entries (~400 bytes per chunk) keyed by
`(trigram << 16) | chunk_id`. This avoids oversized inline values and distributes
large postings lists across multiple B+Tree leaf entries.

### Implementation
- Key format: `trigramChunkKey(trigram, chunkId) = (uint64(trigram) << 16) | uint64(chunkId)`
- Chunk threshold: `PostingsChunkThreshold = 400` bytes (below B+Tree inline limit)
- Chunk splitting respects varint boundaries (scans for byte < 0x80)
- `loadPostings` iterates chunk_id 0..N until key not found
- `storePostings` splits encoded postings into chunks and cleans up old chunks
- `buildTrigramIndexForColumn` uses chunked key format

### Rationale
High-frequency trigrams can produce very large postings lists. Chunking keeps each
B+Tree entry within the inline value limit, avoiding overflow pages and enabling
efficient page-level caching.

### Trade-offs
- Slightly more complex store/load logic vs simpler single-key approach
- Multiple B+Tree lookups per trigram vs single lookup (mitigated by sequential keys)

### References
- design/adr/0007-trigram-postings-storage-strategy.md
- design/adr/0052-trigram-durability.md
