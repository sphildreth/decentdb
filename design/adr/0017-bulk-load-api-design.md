# Bulk Load API Design
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Provide a dedicated `bulk_load()` API that batches inserts with deferred durability for high-throughput data loading, separate from normal transaction semantics.

### Rationale
- Normal transactions with fsync-on-commit achieve ~100-500 rows/sec
- Target workload (MusicBrainz import) requires loading millions of rows
- 100k rows in 30 seconds = 3,333 rows/sec, requiring 6-10x improvement
- Deferred durability during bulk load is acceptable if user explicitly opts in

### API Design
```nim
type BulkLoadConfig = object
  batch_size: int           # Rows per batch (default: 10000)
  sync_interval: int        # Batches between fsync (default: 10)
  disable_indexes: bool     # Skip index updates during load (default: true)
  disable_triggers: bool    # Skip FK checks during load (default: false)
  checkpoint_on_complete: bool  # Checkpoint after load (default: true)

proc bulk_load*(
  db: Database,
  table: string,
  rows: Iterator[Row],
  config: BulkLoadConfig = defaultBulkLoadConfig()
): Result[BulkLoadStats, Error]
```

### Semantics
1. **Deferred durability**: fsync only every `sync_interval` batches, not per transaction
2. **Single writer lock held** for entire duration (no concurrent writes)
3. **Readers unaffected**: Use snapshot isolation, see consistent pre-load state
4. **Crash behavior**: If crash during bulk load, all progress is lost (no partial commits)
5. **Post-load validation**: If `disable_indexes=true`, rebuild indexes after load completes
6. **FK enforcement**: Optional - can disable for trusted data sources, validate after

### Performance Targets
- With `disable_indexes=true`: 10,000+ rows/sec (10x improvement)
- With indexes enabled: 5,000+ rows/sec (5x improvement)
- 100k rows should complete in < 20 seconds on reference hardware

### Recovery After Crash
If crash occurs during bulk load:
1. Database remains consistent (last committed state before bulk load)
2. User must restart bulk load from beginning
3. No partial data exposure (bulk load uses separate transaction scope)

### Alternatives Considered
- **Single large transaction**: WAL would grow unbounded, recovery would be slow
- **Multiple normal transactions**: Still too slow due to fsync overhead
- **wal_sync_mode=OFF**: Too dangerous, affects all transactions

### Trade-offs
- **Pros**: 5-10x throughput improvement, explicit API signals intent, safer than global sync mode
- **Cons**: Crash loses progress, requires exclusive writer lock, more complex implementation

### References
- PRD.md §5.4 (Bulk load performance target)
- SPEC.md §4.1 (WAL durability modes)
