# Freelist Atomicity During Checkpoint
**Date:** 2026-02-01
**Status:** Deferred

## Decision
Document the current single-threaded freelist design and define the atomicity requirements for the future multi-reader phase. The freelist must maintain ACID guarantees during checkpoint operations, specifically ensuring that:
1. Freelist operations are serialized with WAL commits
2. Pages freed during a transaction are not reused until the transaction commits and is checkpointed
3. Readers never see partially-updated freelist state during checkpoint

## Rationale
The current single-process, single-writer model with WAL provides implicit serialization, but we need to establish invariants now to ensure safe evolution toward multi-reader concurrency in the 1.x timeframe. Without clear atomicity rules, future multi-reader implementations risk:
- Readers seeing inconsistent freelist state mid-checkpoint
- Pages being allocated that were freed by uncommitted transactions
- Lost updates during concurrent freelist modifications

## Current State (0.x)

### Single-Threaded Guarantees
- **Writer serialization**: All modifications go through a single WAL writer
- **Freelist cache in pager**: The `Pager` maintains in-memory freelist state during transactions
- **Checkpoint ordering**: Checkpoint only writes pages with LSN ≤ safeLsn, ensuring committed state is preserved

### Checkpoint Interaction
```
1. WAL commit completes → pages marked dirty in cache
2. Checkpoint identifies dirty pages ≤ safeLsn  
3. Checkpoint writes pages to database file
4. Checkpoint updates header (including freelist_head, freelist_count)
5. Readers during checkpoint see: WAL overlay > cache > database file
```

## Future Requirements (1.x+)

### Atomicity Guarantees
1. **Freelist head updates must be atomic**: The `freelist_head` and `freelist_count` fields in the database header must be updated together atomically
2. **Page reuse must be delayed**: Pages freed by transaction T1 cannot be allocated to transaction T2 until:
   - T1 commits
   - T1's changes are checkpointed  
   - All readers with snapshot < T1's commit LSN have completed
3. **Checkpoint must preserve freelist consistency**: During checkpoint, the freelist pages themselves may be written, but the invariants must hold:
   - No allocated page appears in the freelist
   - No free page is missing from the freelist
   - Header fields match the actual freelist state

### Proposed Concurrency Model
When multi-reader support is added:
1. Use the existing `rollbackLock` to prevent readers from seeing freelist changes during rollback
2. Extend `dirtySinceCheckpoint` tracking to include freelist pages
3. Consider a separate `freelistLock` for high-contention scenarios

## Trade-offs

### Option A: Status Quo (Single-Threaded)
- **Pros**: Simple, no locking overhead, correct in current architecture
- **Cons**: Will require refactoring for 1.x multi-reader support

### Option B: Add Freelist-Only Lock Now
- **Pros**: Cleaner migration path to 1.x, explicit documentation of critical section
- **Cons**: Slight overhead (one additional lock acquisition per allocate/free), premature optimization for current use case

### Option C: Full MVCC for Freelist (1.x)
- **Pros**: Maximum concurrency, no writer-reader conflicts on allocations
- **Cons**: Complex to implement, requires versioned freelist pages, may be overkill

**Selected**: Document Option A invariants now, defer implementation until 1.x ADR. The `rollbackLock` added in CRIT-004 already provides the necessary synchronization primitive for preventing readers from seeing partial freelist state during rollback.

## References
- `design/adr/0029-freelist-page-format.md` - Freelist storage format
- `design/adr/0018-checkpointing-reader-count-mechanism.md` - Reader management during checkpoint
- `design/adr/0004-wal-checkpoint-strategy.md` - Checkpoint ordering guarantees
- CRIT-004 (rollback cache atomicity) - Related rollback locking
- `src/pager/pager.nim` - Freelist implementation (`allocatePage`, `freePage`)
