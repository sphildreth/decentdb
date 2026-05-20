# ADR 0155: Branch-Aware WAL Commit Records and Recovery
**Date:** 2026-05-18
**Status:** Accepted

## Context

Branch-local writes, branch logs, restore, and time-travel workflows must not
weaken existing WAL correctness guarantees.

Current WAL behavior already establishes key invariants:

- `wal_end_lsn` is a global atomic snapshot boundary (ADR 0003).
- Recovery trusts only fully-valid WAL frames/checksums and ignores torn tails.
- Active-reader retention is LSN-based and independent of higher-level features
  (ADR 0019).
- WAL payload/version evolution is supported through discriminated WAL versions
  (ADR 0140).
- Async commit allows acknowledged-but-not-yet-fsynced commits (ADR 0135).

ADR C must define how branch head movement is represented in WAL commits and how
crash recovery reconstructs branch state deterministically.

## Decision

1. **Branch-aware metadata lives in commit records, not in per-page frame
   headers.**
   - Add a branch-aware commit payload variant that includes at least:
     - `branch_id`
     - `parent_branch_head_lsn` (expected current head before apply)
     - `base_root_manifest_id`
     - `new_root_manifest_id`
   - Existing data-frame forms (full-page and delta frames) remain page-oriented
     and do not carry branch identifiers.

2. **WAL frames are root-manifest-qualified at commit scope, not
   branch-qualified per frame.**
   - A commit's frame set is interpreted as producing `new_root_manifest_id`
     from `base_root_manifest_id`.
   - The branch association is applied by the commit record that advances a
     branch head to that new manifest.
   - This keeps frame encoding shared and avoids duplicating branch tags across
     every frame.

3. **Branch head advance is atomic with commit visibility.**
   - A branch head update is part of the same logical WAL commit record as the
     page changes.
   - The writer must publish the branch-head state change before the release
     store that advances global `wal_end_lsn`.
   - Readers that load `wal_end_lsn` with acquire semantics cannot observe a
     committed LSN without its corresponding branch-head metadata.

4. **Crash recovery order is deterministic and LSN-ordered.**
   - Step 1: scan WAL up to the last structurally valid, checksum-valid frame.
   - Step 2: rebuild page/WAL-version visibility structures in LSN order
     (including delta reconstruction semantics from ADR 0132).
   - Step 3: apply only valid commit records in LSN order.
   - Step 4: for each branch-aware commit record, validate
     `parent_branch_head_lsn` against the recovered branch head and then advance
     the branch head to `new_root_manifest_id` at that commit LSN.
   - Incomplete transactions or commits past a torn/invalid tail are ignored.

5. **Validation/checksum implications are additive and conservative.**
   - Existing per-frame checksum and frame-structure validation remains
     unchanged.
   - Branch metadata is protected by the commit frame checksum like other commit
     payload fields.
   - Recovery must reject a branch-aware commit record if metadata is
     inconsistent (for example, impossible parent-head transition), and treat
     the database as corrupted rather than guessing.

6. **`wal_end_lsn` remains a single global atomic across all branches.**
   - There is no per-branch `wal_end_lsn`.
   - Every committed branch write advances the same global LSN sequence.
   - Branch head selection for reads/time-travel is constrained by
     `head_commit_lsn <= snapshot_lsn` using this global LSN domain.
   - In `AsyncCommit` mode (ADR 0135), acknowledged branch commits may be lost
     on power loss exactly as other commits may be; recovery still returns a
     consistent state at the last durable valid WAL point.

## Consequences

- Branch state reconstruction is crash-safe without introducing out-of-band
  branch-head durability paths.
- WAL frame encoding stays stable for full/delta page frames; branch awareness
  is concentrated in commit metadata.
- Recovery and diagnostics gain explicit parent-head validation, improving
  corruption detection.
- Retention and GC policy for long-lived branch history remains ADR D scope;
  this ADR only fixes commit/recovery semantics.

## References

- `design/adr/0002-wal-commit-record-format.md`
- `design/adr/0003-snapshot-lsn-atomicity.md`
- `design/adr/0019-wal-retention-for-active-readers.md`
- `design/adr/0132-delta-wal-frames-for-small-page-edits.md`
- `design/adr/0135-async-commit-wal-group-commit.md`
- `design/adr/0140-walversion-discriminated-payload.md`
