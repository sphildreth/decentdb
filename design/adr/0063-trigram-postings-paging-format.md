## Trigram Postings Paging / Streaming Storage Format
**Date:** 2026-02-02
**Status:** Deferred

### Decision
Draft a design for paging/streaming trigram postings to avoid decoding large postings lists into memory.

### Rationale
High-frequency trigrams can produce very large postings lists. Even with bounded decoding and fallback-to-scan heuristics, a durable solution may require a postings format that supports streaming iteration and paging/overflow storage.

### Alternatives Considered
- Status quo with bounded decode and scan fallback.
- Chunked postings pages referenced by trigram key.
- Separate postings table/index with overflow pages.

### Trade-offs
- Lower memory spikes vs more complex storage/rebuild/checkpoint semantics.

### References
- design/adr/0007-trigram-postings-storage-strategy.md
- design/adr/0052-trigram-durability.md
