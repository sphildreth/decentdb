# Trigram Index Delta Durability Model

**Date:** 2026-02-01
**Status:** Proposed

## Decision

Move trigram index delta flushing from `commitTransaction` to `checkpoint` operation. This changes the durability model such that:

1. **Transaction commit** only flushes B+Tree index changes to WAL (immediate durability)
2. **Trigram delta changes** are held in-memory and flushed only during checkpoint
3. **Crash recovery** will replay WAL entries but may need to rebuild trigram indexes from B+Tree data if deltas were not checkpointed

## Rationale

Currently, every transaction commit flushes trigram delta changes (`flushTrigramDeltas`) before committing the WAL. This provides full durability but has significant performance impact:

- Trigram delta operations involve scanning delta buffers and updating posting lists
- For write-heavy workloads with text columns, this dominates commit latency
- Since trigram indexes are secondary (derived) structures, they can be reconstructed from the B+Tree data

Moving trigram flush to checkpoint provides:
- **Lower commit latency** - commits only need to flush core data structures
- **Amortized trigram cost** - batch multiple transactions' trigram changes
- **Configurable durability** - checkpoint frequency controls trigram durability window
- **Simpler recovery model** - WAL contains only primary data, trigram indexes are soft state

## User-Visible Semantics

This is a **breaking change** to user-visible durability semantics:

| Scenario | Old Behavior | New Behavior |
|----------|-------------|--------------|
| Transaction commits | Trigram changes durable immediately | Trigram changes durable at next checkpoint |
| Crash before checkpoint | Trigram data recovered from WAL | Trigram data may be stale; can be rebuilt |
| LIKE '%pattern%' query | Always sees latest data | May not see un-checkpointed changes temporarily |

**Mitigation:** If needed, users can force checkpoint after critical writes.

## Alternatives Considered

### 1. Keep current model (flush on every commit)
- **Pros**: Full durability, simple recovery
- **Cons**: Poor write performance, commit latency spikes

### 2. Async trigram flush with fsync
- **Pros**: Non-blocking commit
- **Cons**: Complex async coordination, potential durability gaps

### 3. Configurable durability per-transaction
- **Pros**: Flexibility for different workloads
- **Cons**: Complex API, hard to test all combinations

### 4. Checkpoint-based flush (selected)
- **Pros**: Simple, predictable, batching benefits
- **Cons**: Temporary durability gap, needs rebuild capability

## Trade-offs

**Pros:**
- Significantly improved write throughput
- Lower and more predictable commit latency
- Reduced WAL size (no trigram entries in WAL)
- Simpler commit path

**Cons:**
- Temporary inconsistency between B+Tree and trigram indexes after crash
- Requires trigram rebuild capability in recovery/repair tools
- May cause temporary incorrect query results after crash (until rebuild)

## Implementation Plan

1. Modify `commitTransaction` in `src/engine.nim` to skip `flushTrigramDeltas`
2. Modify `checkpointDb` in `src/engine.nim` to call `flushTrigramDeltas` before checkpoint
3. Add trigram rebuild capability to recovery (future work)
4. Update documentation (SPEC.md) with new durability semantics

## References

- ACTION-PLAN.md MED-003
- SPEC.md §12 (Trigram Indexes)
