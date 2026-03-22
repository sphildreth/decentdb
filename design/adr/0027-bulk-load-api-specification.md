## Bulk Load API Specification
**Date:** 2026-01-28
**Status:** Accepted

### Decision

Define a comprehensive bulk load API that provides high-throughput data loading with configurable durability options and proper isolation from concurrent readers.

### Rationale

The bulk load functionality was mentioned in the PRD but lacked detailed specification. A well-designed bulk load API is essential for the target use case of importing large datasets (like the music library with 9.5M tracks) efficiently while maintaining database consistency.

### API Design

#### Function Signature
```nim
proc bulkLoad*(db: Database, tableName: string, data: seq[Row], options: BulkLoadOptions): BulkLoadResult

type BulkLoadOptions = object
  batchSize*: int          # Rows per batch (default: 10000)
  syncInterval*: int       # Batches between fsync (default: 10) 
  disableIndexes*: bool    # Skip index updates during load (default: true)
  checkpointOnComplete*: bool # Checkpoint after load (default: true)
  durability*: DurabilityMode # FULL, DEFERRED, NONE (for testing)

type DurabilityMode = enum
  FULL = "full"           # fsync after each batch
  DEFERRED = "deferred"   # fsync only at end
  TEST_ONLY_NO_SYNC = "none" # No fsyncs (unsafe, testing only)
```

#### Behavior
1. Acquires exclusive writer lock for duration
2. Processes data in batches to manage memory usage
3. Maintains snapshot isolation for concurrent readers
4. Can skip index updates during load for performance, with index rebuild after
5. Reports progress and handles partial failures gracefully

#### Error Handling
- If crash occurs during bulk load, all progress is lost (no partial commits)
- Validation occurs before load begins
- Transaction rollback on any error during load

### Alternatives Considered

1. **Simple batch insert**: Would not provide the performance benefits of true bulk loading
2. **External bulk loading tool**: Would complicate deployment and reduce integration
3. **Streaming API**: Would be more complex but provide similar benefits

### Trade-offs

**Pros:**
- Significant performance improvement for large data loads
- Maintains ACID properties during load
- Configurable durability options for different use cases
- Proper isolation from concurrent operations

**Cons:**
- Exclusive lock prevents concurrent writes during load
- More complex implementation than simple batch inserts
- Requires careful memory management to avoid OOM errors

### References

- PRD section on bulk load API
- SPEC section 4.4 on Bulk Load API