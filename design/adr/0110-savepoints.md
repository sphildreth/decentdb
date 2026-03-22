# ADR-0110: Savepoints (SAVEPOINT / RELEASE / ROLLBACK TO)

## Status
Accepted

## Context
DecentDB needs support for SQL savepoints within a transaction:
- `SAVEPOINT name` — create a named savepoint
- `RELEASE SAVEPOINT name` — discard a savepoint (merge changes into parent)
- `ROLLBACK TO SAVEPOINT name` — undo changes back to the named savepoint

## Decision

### Approach: In-Memory Snapshot Stack
Since DecentDB uses a single-writer model with WAL, savepoints are implemented as a stack of in-memory state snapshots rather than WAL-level markers:

1. **SAVEPOINT name**: Pushes a snapshot of the current catalog state (table metadata, view metadata) and the current WAL write position (frame count) onto a named stack.

2. **RELEASE SAVEPOINT name**: Pops the named savepoint from the stack. Changes remain in the current transaction. This is a no-op beyond stack cleanup.

3. **ROLLBACK TO SAVEPOINT name**: Restores catalog state from the snapshot and discards any WAL frames written after the savepoint. The savepoint itself remains active (can be rolled back to again).

### WAL Integration
- On ROLLBACK TO, any WAL frames written after the savepoint's frame position are logically discarded by truncating the writer's frame list.
- The pager's dirty page cache is also rolled back to the snapshot state.
- Since DecentDB is single-writer, there are no concurrent write conflicts.

### Limitations (v1)
- Savepoints only work within an explicit transaction (BEGIN...COMMIT).
- Nested savepoints are supported (stack-based).
- ROLLBACK TO does not roll back B+Tree structural changes; it rolls back page-level changes via the pager cache.

## Consequences
- No WAL format changes (frames are simply not committed on rollback).
- No persistent format changes.
- Catalog snapshot is shallow-copy of tables/views OrderedTables (acceptable cost for correctness).
