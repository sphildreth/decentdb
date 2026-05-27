# Cross-Process Reader Retention And WAL Refresh
**Date:** 2026-05-27
**Status:** Accepted

### Decision

Cross-process readers must participate in WAL retention through reader slots in
the coordination sidecar. Checkpoint truncation must consider every active
reader slot owned by every process before discarding WAL frames.

Reader slots record snapshot LSN, owner process metadata, connection/reader
generation, checkpoint generation observed at read start, heartbeat information,
and state flags. A reader slot is owned by a byte-range lock. Slot records are
diagnostic and retention metadata; they must not contain user data, SQL text,
TDE key material, indexed terms, or audit context values.

The begin-read protocol must register a conservative slot before depending on a
snapshot LSN. Checkpointers must treat initializing slots as retention blockers.
This prevents a reader from capturing an old LSN while a checkpoint concurrently
sees no active readers and truncates required WAL frames.

Checkpoint safe truncation must use the minimum of:

- active cross-process reader snapshot LSNs;
- active in-process reader snapshot LSNs;
- branch retained snapshot LSN;
- sync/shape retention blockers;
- any other engine-owned WAL retention source.

Every process keeps a local WAL index. Before beginning a read transaction,
beginning a write transaction, checkpointing, or reporting WAL diagnostics, the
process must compare its local generation with the coordination sidecar and
refresh if another process appended WAL frames, checkpointed, truncated, or
recovered the WAL.

Refresh must validate WAL frames and commit markers. Coordination sidecar
metadata is a publication and discovery mechanism, not a substitute for WAL
integrity checks.

Stale reader slots may be cleaned only after the engine proves the slot lock is
not held by the owner process. A long-running reader in a live process is a
valid retention blocker, not a stale slot.

### Rationale

ADR 0019 established that WAL frames required by active readers must never be
truncated. That invariant cannot stop at a process boundary. Without
cross-process reader retention, a CLI checkpoint could truncate frames needed by
an application process and break snapshot isolation.

The existing in-memory WAL index is also process-local. If Process A commits
new frames, Process B's WAL index is stale until it refreshes. Correctness
requires a cheap generation check and a safe incremental refresh path before
Process B uses the WAL for reads, writes, or checkpoints.

Reader slot ownership by OS lock gives crash cleanup a reliable signal. Slot
metadata alone is not enough because a crashed process can leave stale bytes
behind. A free slot lock plus owner-token validation lets DecentDB distinguish a
dead reader from a merely old reader.

### Alternatives Considered

1. **Block checkpoints whenever more than one process is attached.** Rejected.
   Safe but too limiting; WAL files would grow unnecessarily.
2. **Use heartbeat timestamps alone for stale detection.** Rejected. A paused or
   overloaded live process could be misclassified and lose snapshot protection.
3. **Keep independent WAL indexes and rescan full WAL on every query.** Rejected.
   Correct but too slow for hot read paths.
4. **Trust sidecar WAL end without validating WAL frames.** Rejected. Sidecar
   corruption or crash windows must not make invalid WAL data visible.
5. **Truncate WAL based only on the current checkpointer's process-local
   readers.** Rejected. Violates ADR 0019 for readers in other processes.
6. **Auto-clear old readers by age.** Rejected. Long-running readers are valid
   even when they block truncation. Doctor should report them; the engine should
   not silently break them.

### Trade-offs

- Begin-read now has coordination overhead for on-disk databases.
- Checkpoint must scan cross-process reader slots.
- Reader slot exhaustion becomes a possible runtime error.
- WAL refresh adds complexity around external checkpoint/truncation detection.
- Processes with very long readers can still grow WAL files, but now Doctor can
  identify the blocker.
- Crash cleanup must be platform-tested carefully because lock release behavior
  is part of the correctness story.

### Consequences

- Reader lifecycle code must be refactored so sidecar registration and existing
  in-process reader guards are updated together.
- WAL checkpoint code must merge retention sources from process sidecar,
  in-process guards, branch, and sync.
- WAL index code needs generation-aware incremental refresh and rebuild paths.
- Crash/fault tests must cover commit-before-publish,
  publish-before-return, checkpoint-before-truncate, and stale-reader cleanup
  windows.
- Doctor and `sys.*` surfaces should expose active and stale reader slots.

### References

- `design/WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md`
- `design/adr/0018-checkpointing-reader-count-mechanism.md`
- `design/adr/0019-wal-retention-for-active-readers.md`
- `design/adr/0117-shared-wal-registry.md`
- `design/adr/0141-paged-on-disk-wal-index.md`
- `design/adr/0156-branch-checkpoint-retention-and-garbage-collection.md`
- `design/adr/0168-sync-shape-streaming-subscriptions.md`

