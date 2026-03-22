# WAL Checkpoint Strategy
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Implement WAL size-based checkpointing with configurable thresholds and forced checkpoint timeout.

### Rationale
- Prevents unbounded WAL growth
- Configurable threshold allows tuning for different workloads
- Timeout prevents indefinite blocking if readers are long-lived
- Forced checkpoint with readers active ensures progress

### Alternatives Considered
- Checkpoint only when no readers: Can block indefinitely
- Time-based checkpointing: Doesn't account for WAL size
- Manual checkpoint only: Too much operational burden

### Trade-offs
- **Pros**: Bounded WAL size, configurable, ensures progress
- **Cons**: Forced checkpoint may be slower, requires careful implementation

### References
- SPEC.md §4.3 (Checkpointing)
