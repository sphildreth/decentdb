# 19. WAL Retention for Active Readers

Date: 2026-01-28

## Status

Accepted

## Context

The initial design for Checkpointing allowing for a "Forced Checkpoint" that would truncate the WAL even if readers were active. The assumption was that readers could "continue" with old pages. However, in a WAL-based MVCC/Snapshot Isolation system, the "old pages" live **in the WAL frames** that shadow the main database file.

If the WAL is truncated (deleted/overwritten) while a reader is active:
1. The reader may need page P (version LSN 100).
2. The main DB file has page P (version LSN 200, from the checkpoint).
3. The WAL had the LSN 100 version, but it is now gone.
4. The reader reads LSN 200, violating Snapshot Isolation (Consistency), or fails with a confusing IO error.

## Decision

We will change the Checkpoint and Log Management logic to **never truncate WAL frames required by active readers**.

1.  **Read Tracking**: The system already tracks `active_readers`. We must also track `min_reader_snapshot_lsn`.
2.  **Checkpoint Logic**:
    *   The Checkpoint process copies valid committed pages to the main DB file as usual.
    *   However, the **WAL Truncation** step is conditional.
    *   Calculate `safe_truncate_lsn = min(active_readers_snapshot_lsn)`. If no readers, this is the current commit LSN.
    *   The WAL can only be discarded/reused *up to* the frame preceding the `safe_truncate_lsn`.
3.  **WAL Grwoth**:
    *   This implies that if a reader is open for a very long time (hours) while heavy writes occur, the WAL file will continue to grow indefinitely (or until disk exhaustion), even if checkpoints are happening.
    *   This is a standard trade-off in MVCC systems (Postgres "VACUUM" issues, Oracle ORA-01555).
    *   For MVP, we accept the risk of disk growth to ensure Correctness.

## Consequences

*   **Correctness**: Readers strictly guaranteed Snapshot Isolation.
*   **Performance/Risk**: A "toxic" long-running reader can prevent WAL cleanup, filling the disk. We should add monitoring or "max transaction age" timeouts in the future.
*   **Complexity**: Checkpoint logic needs to calculate the safe truncation point. WAL management needs to handle "partial" truncation or simply defer the entire file truncation until all blocking readers are gone. For MVP simplicity: **Defer entire WAL truncation** if any reader needs any part of it.
