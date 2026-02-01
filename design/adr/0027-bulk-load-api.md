# Bulk Load API (0.x Baseline)
**Date:** 2026-01-28
**Status:** Accepted

## Context
DecentDb needs a dedicated bulk load path to ingest large datasets efficiently (PRD target: 100k rows in < 20s) while preserving:
- Correctness (constraints honored)
- Snapshot isolation for readers
- WAL-based durability by default (fsync on commit)

The engine already exposes a Nim-level `bulkLoad(db, tableName, rows, options, wal)` entry point (`src/engine.nim`), but the behavior and durability modes must be explicitly defined.

## Decision
Define `bulkLoad` as a batch-oriented loader with explicit durability modes and optional index build specialization.

### API surface (Nim)
`bulkLoad(db, tableName, rows, options = defaultBulkLoadOptions(), wal = db.wal)`

`BulkLoadOptions`:
- `batchSize`: number of rows per batch (validation + insert loop)
- `syncInterval`: batch interval used for durability pacing (meaning depends on `durability`)
- `disableIndexes`: if true, skip index maintenance during row inserts, then rebuild affected indexes at the end
- `checkpointOnComplete`: if true and WAL is used, run a checkpoint after the final commit
- `durability`:
  - `dmFull`: WAL commit + fsync at least once per batch
  - `dmDeferred`: WAL commit + fsync every `syncInterval` batches (and once at the end)
  - `dmNone`: no WAL usage (non-durable); WAL overlay is disabled for the connection/session

### Durability semantics
If `wal != nil` and `durability != dmNone`, bulk load uses WAL transactions to maintain snapshot isolation:
- Rows are inserted using the normal write path (page cache dirties pages).
- Batches are committed to the WAL based on `durability`:
  - `dmFull`: commit each batch.
  - `dmDeferred`: commit every `syncInterval` batches.
- A final commit is always performed at the end if any dirty pages remain.
- `checkpointOnComplete` triggers `checkpoint(wal, pager)` after the final commit.

If `wal == nil` or `durability == dmNone`, bulk load may flush dirty pages directly to the database file (`flushAll`) per the configured policy:
- `dmFull`: flush/fsync every batch
- `dmDeferred`: flush/fsync every `syncInterval` batches
- `dmNone`: no flushing guarantees (best-effort)

**Important:** If `durability == dmNone` and a WAL exists, `walOverlayEnabled` is disabled for the session because WAL snapshots are no longer consistent with direct DB-file writes.

### Index behavior
If `disableIndexes == true`:
- Row inserts skip index maintenance.
- At the end, affected indexes are rebuilt.
- The final WAL commit (when enabled) happens *after* index rebuild, so rebuilt indexes are durable/visible to WAL snapshot reads.

If `disableIndexes == false`:
- Indexes are maintained incrementally during insertion.

## Rationale
- Ensures WAL overlay remains correct when WAL is present (readers don’t accidentally see stale WAL versions over newer DB-file pages).
- Makes durability tradeoffs explicit and testable.
- Keeps implementation “boring”: batch loop + optional index rebuild + WAL commit cadence.

## Consequences / follow-ups
- `dmDeferred` currently still uses fsync-per-commit, just less frequently (commit cadence). A future ADR may define additional opt-in policies (e.g., group commit).
- Larger-than-memory loads may require further specialization (external sorting for index rebuilds, reduced GC pressure).

