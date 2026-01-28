## Handling WAL Growth with Long-Running Readers
**Date:** 2026-01-28
**Status:** Accepted

### Decision

Implement a mechanism to prevent indefinite WAL growth when long-running readers are active by introducing reader tracking and timeout policies.

### Rationale

The current checkpointing mechanism has a critical flaw: if a reader holds a snapshot LSN that is significantly behind the current WAL position, the WAL cannot be truncated and may grow indefinitely. This creates a resource exhaustion vulnerability.

### Solution Components

1. **Reader Tracking**: Maintain a registry of active readers with their snapshot LSNs
2. **Reader Timeout**: Implement configurable timeout for long-running transactions
3. **Forced Checkpointing**: Allow checkpointing with WAL truncation even when readers exist, with appropriate warnings

### Implementation Details

1. **Reader Registry**: A thread-safe data structure tracking:
   - Reader ID
   - Snapshot LSN
   - Creation timestamp
   - Associated connection/session

2. **Timeout Policy**: Configurable timeout (default: 300 seconds) after which long-running readers receive warnings and eventually get terminated

3. **Checkpoint Logic Enhancement**:
   ```nim
   # Enhanced checkpoint protocol
   safe_truncate_lsn = min(active_readers_snapshot_lsn)
   
   # If oldest reader is too old, warn and potentially force truncate
   if oldest_reader_age > timeout_warning_threshold:
     log.warning("Long-running reader preventing WAL truncation")
     
   if oldest_reader_age > timeout_abort_threshold:
     # Option to terminate old readers or force truncate with warning
     log.error("Forcing checkpoint despite long-running readers")
     safe_truncate_lsn = writer_lsn  # Force truncation
   ```

### Alternatives Considered

1. **No timeout policy**: Would allow indefinite WAL growth, leading to potential disk exhaustion
2. **Automatic reader termination**: Too aggressive, might interrupt legitimate long-running queries
3. **WAL partitioning**: Complex solution that would require significant architectural changes

### Trade-offs

**Pros:**
- Prevents resource exhaustion from long-running readers
- Maintains data consistency during forced truncation
- Configurable to accommodate legitimate long-running queries
- Simple implementation relative to alternatives

**Cons:**
- May terminate long-running queries unexpectedly if timeout is too aggressive
- Adds complexity to transaction management
- Potential for data inconsistency if not implemented carefully

### References

- ADR-0004 (WAL Checkpoint Strategy) - foundational for this decision
- ADR-0019 (WAL Retention for Active Readers) - related to reader protection