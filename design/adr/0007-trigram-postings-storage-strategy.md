# Trigram Postings Storage Strategy
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Use in-memory buffers per trigram (max 4KB) that flush to B+Tree on transaction commit.

### Rationale
- Bounded write amplification (buffer limits per-trigram size)
- Simple implementation compared to LSM-style merge
- Flush on commit ensures durability
- Delta-encoded varints provide good compression for sequential IDs

### Alternatives Considered
- LSM-style append segments: More complex, requires merge logic
- Direct B+Tree updates: High write amplification

### Trade-offs
- **Pros**: Bounded writes, simple, good compression
- **Cons**: In-memory buffers consume memory, flush on commit adds latency

### References
- SPEC.md §8.5 (Storage format for postings)
