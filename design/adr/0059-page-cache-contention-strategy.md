## Page Cache Contention Strategy
**Date:** 2026-02-02
**Status:** Proposed

### Decision
Draft a strategy for addressing page cache contention without changing correctness invariants, or explicitly document and ADR any locking/invariant changes required.

### Rationale
The current concurrency model is one writer with multiple concurrent readers. Hot locks in the pager/page cache can dominate performance. Any change to locking granularity or invariants can affect correctness and must be explicitly designed.

### Alternatives Considered
- Status quo with measurement-only (counters/benchmarks).
- Increase sharding, reduce critical sections, and avoid global locks.
- Reader-optimized data structures with per-page locks.

### Trade-offs
- Performance gains vs risk of subtle races and broken snapshot semantics.

### References
- design/adr/0055-thread-safety-and-snapshot-context.md
- design/adr/0018-checkpointing-reader-count-mechanism.md
