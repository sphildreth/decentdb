## Isolation Level Specification
**Date:** 2026-01-28
**Status:** Accepted

### Decision

DecentDb will implement **Snapshot Isolation** as its default isolation level, which provides stronger guarantees than READ_COMMITTED but simpler implementation than SERIALIZABLE.

### Rationale

1. **Snapshot Isolation** provides a good balance between performance and consistency for the target use cases
2. It prevents the three phenomena that occur under READ_COMMITTED:
   - Dirty reads
   - Non-repeatable reads  
   - Phantom reads (within the snapshot)
3. It's simpler to implement than SERIALIZABLE which would require complex conflict detection
4. The WAL-based snapshot read mechanism already supports this model
5. For the music library use case, snapshot isolation provides sufficient consistency

### Alternatives Considered

1. **READ_COMMITTED**: Simpler but allows phantom reads, which could lead to inconsistent results in analytical queries
2. **SERIALIZABLE**: Provides strongest consistency but adds significant complexity with limited benefit for embedded use cases
3. **Custom isolation level**: Would create incompatibility with existing SQL standards

### Trade-offs

**Pros:**
- Prevents all three ANSI isolation anomalies
- Compatible with MVCC/Snapshot read architecture already planned
- Good performance characteristics for read-heavy workloads
- Reasonable complexity for MVP implementation

**Cons:**
- Still allows write skews (not prevented by snapshot isolation)
- May have higher memory overhead than READ_COMMITTED
- Could cause more transaction conflicts/retries in write-heavy scenarios

### References

- ADR-0003 (Snapshot LSN Atomicity) - foundational for snapshot isolation
- ADR-0004 (WAL Checkpoint Strategy) - relates to snapshot consistency
- PostgreSQL documentation on snapshot isolation