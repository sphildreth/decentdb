## Typed/Comparable Index Key Encoding for TEXT/BLOB
**Date:** 2026-02-02
**Status:** Proposed

### Decision
Draft a design to replace CRC32C-hash keys for TEXT/BLOB btree indexes with a typed, comparable encoding that supports correct equality and ordering/range semantics.

### Rationale
Hash-keyed TEXT/BLOB indexes can suffer collisions. Even with post-verification for equality constraints, ordering and range semantics are undefined/incorrect for hashed keys. A typed/comparable encoding is required for durable correctness.

### Alternatives Considered
- Keep CRC32C key and mandate post-verification for equality operations only (interim mitigation).
- Store a prefix (and length) in the key for partial ordering with fallback to row-compare.
- Store full value bytes (or an overflow reference) as the btree key.

### Trade-offs
- Correct semantics vs potential page bloat and more expensive comparisons.
- Likely a persistent format change if btree key format changes.

### References
- design/adr/0032-btree-page-layout.md
- design/adr/0020-overflow-pages-for-blobs.md
