## WAL-safe Page Cache Eviction / Flush Pipeline
**Date:** 2026-02-02
**Status:** Proposed

### Decision
Draft a design that guarantees dirty page eviction/flushes cannot write uncommitted bytes into the main DB file, and define how eviction interacts with commit/checkpoint.

### Rationale
If dirty pages created during an uncommitted transaction can be flushed to the DB file, atomicity is violated. A complete solution likely requires defining an explicit flush pipeline and/or routing dirty evictions through WAL.

### Alternatives Considered
- Forbid eviction of dirty pages during a transaction (simple, but can cause “cache full” failures).
- Always flush dirty evictions to WAL (set a flush handler) and only checkpoint into the DB file.
- Introduce per-transaction private dirty buffers and commit-time reconciliation.

### Trade-offs
- Strong correctness guarantees vs memory pressure and complexity.

### References
- design/adr/0019-wal-retention-for-active-readers.md
- design/adr/0010-error-handling-strategy.md
