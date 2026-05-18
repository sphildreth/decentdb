# ADR 0156: Branch Checkpoint, Retention, And Garbage Collection
**Date:** 2026-05-18
**Status:** Accepted

## Context

Branch, snapshot, restore, and time-travel workflows require DecentDB to retain
historical database states after the writer advances the default branch. ADR
0019 already prevents WAL truncation while active readers need old versions, but
active readers are temporary. Named snapshots and branches are durable and may
pin state for days or months.

Keeping all historical branch state in the WAL would make the WAL an unbounded
history store and would mix crash-recovery lifecycle with product-level history
retention. Branching needs durable reachability tracking and explicit garbage
collection independent of ordinary reader retention.

## Decision

DecentDB will make checkpoint and page reclamation branch-aware by tracking
reachability from:

1. live branch heads
2. live named snapshots
3. active readers
4. in-flight writer transactions
5. WAL frames still required for crash recovery

### Retained state ownership

The initial design keeps retained historical page versions in the normal
database storage domain. Branch-local writes use copy-on-write page allocation:
old pages remain valid as long as at least one branch head, snapshot, or active
reader can reach them. A separate history sidecar is rejected for the first
implementation unless ADR 0154 proves the existing file layout cannot safely
represent retained page reachability.

### Checkpoint rules

Checkpoint may copy committed pages to the main database file as today, but it
must not place a page on the reclaimable free list if that page is reachable
from any live root manifest or active reader snapshot. WAL truncation remains
governed by ADR 0019 for active readers and crash recovery. Branch history must
not force WAL retention after a checkpoint has made the required page versions
durably reachable through root manifests.

### Garbage collection rules

Physical reclamation is a separate best-effort operation. Deleting a branch or
snapshot removes a reachability root, but pages become reusable only after the
branch GC pass proves they are unreachable from every live root. GC must be
idempotent and crash-safe:

- a crash before GC commits leaves old pages retained
- a crash after GC commits leaves only pages proven unreachable reclaimed
- branch heads and snapshots are never partially deleted
- main branch data is never reclaimed because a non-default branch was deleted

### Retention policy

The first retention policy is explicit and conservative:

- snapshots are retained until deleted
- branches are retained until deleted
- deleted branches/snapshots may leave reclaimable history until GC runs
- no automatic age-based snapshot deletion in the first implementation
- no automatic branch deletion in the first implementation

Future retention policies may add TTLs, storage budgets, or pruning hints, but
those policies must never delete named history silently.

### Diagnostics

Doctor and inspection surfaces must report retained branch history before the
feature is considered complete:

```text
branch history retained: 1.8 GB
oldest pinned snapshot: before-sync-2026-05-18
oldest pinned branch: migration-test
pages reclaimable after deleting branch: 1.2 GB
```

Planned inspection surfaces:

```sql
SELECT * FROM sys_branch_retention;
SELECT * FROM sys_branch_gc_candidates;
```

The exact names may be aligned with future `sys.*` virtual table naming, but
the information must be queryable and available through `doctor`.

## Rationale

Branch history is product data, not transient crash-recovery state. Treating it
as durable page reachability keeps WAL lifecycle focused on recovery and active
reader isolation while allowing long-lived snapshots and branches.

The conservative retention policy favors correctness and user trust over
automatic cleanup. Disk growth is visible and diagnosable rather than hidden.

## Alternatives Considered

1. **Retain all branch history in the WAL.** Rejected. WAL files are checkpoint
   and crash-recovery structures, not durable history archives. This would cause
   unbounded WAL growth and fragile recovery behavior.
2. **Copy a whole database file per branch.** Rejected. This makes branch create
   database-size-scale and holds the writer lock too long.
3. **Use OS reflinks.** Rejected. Reflinks are platform- and filesystem-specific
   and do not provide a portable DecentDB contract.
4. **Aggressive automatic pruning.** Rejected for the first implementation.
   Users must not lose named restore points because a hidden retention policy
   fired.

## Trade-offs

**Positive:**
- branch create and snapshot create can remain metadata-scale operations
- checkpoint can still bound WAL size after historical pages are durable
- doctor can explain retained disk usage
- no OS-specific file-copy primitive is required

**Negative:**
- page reclamation becomes reachability-based and more complex
- long-lived branches can retain substantial disk space
- GC requires crash tests and internal consistency checks
- first implementation may leave reclaimable pages in place until explicit GC

## Implementation Notes

1. Branch GC must use root manifests as the reachability source of truth.
2. GC must be safe to rerun after interruption.
3. Branch/snapshot deletion should be metadata-only; physical cleanup follows
   through GC.
4. Existing table-list and schema-introspection APIs must hide internal branch
   metadata tables.
5. Support bundles should include branch retention metadata once branch support
   is user-visible.

## References

- `design/WIN_BRANCH_DIFF_RESTORE_TIME_TRAVEL_IMPLEMENTATION_GUIDE.md`
- `design/adr/0019-wal-retention-for-active-readers.md`
- `design/adr/0137-size-based-auto-checkpoint-trigger.md`
- `design/adr/0153-branch-metadata-identity-and-user-surface.md`
- `design/adr/0154-branch-root-manifest-and-copy-on-write-storage.md`
- `design/adr/0155-branch-aware-wal-commit-records-and-recovery.md`
