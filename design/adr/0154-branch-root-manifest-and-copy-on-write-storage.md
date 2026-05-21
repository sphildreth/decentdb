# ADR 0154: Branch Root Manifest And Copy-On-Write Storage

**Date:** 2026-05-18  
**Status:** Accepted

## Context

Branch, diff, restore, and time-travel workflows must preserve DecentDB's
existing durability and snapshot guarantees.

This ADR decides:

- how a branch head points at database state
- root manifest shape
- how branch writes allocate new pages
- how existing pages are shared
- whether file format version changes are required
- interaction with B+Tree table/index roots

This decision must align with:

- ADR 0003 (snapshot LSN atomicity)
- ADR 0019 (WAL retention for active readers)
- ADR 0120 (B+Tree storage engine)
- ADR 0136 (chunked row COW in memory; no on-disk format change)

## Decision

### 1. Branch heads point to immutable root manifests

Each branch head references one durable root manifest identifier. A branch head
advance installs a new manifest reference; it does not mutate prior manifests.

Minimum branch head fields:

- `branch_id`
- `head_id`
- `parent_head_id` (nullable for genesis)
- `root_manifest_id`
- `commit_lsn`
- `created_at`
- `message` (optional)

### 2. Root manifest shape is global-with-per-object roots

The root manifest is a single logical record for a full database view, with
per-object B+Tree roots inside it.

Required manifest fields:

- `manifest_id`
- `manifest_version`
- `catalog_root_page_id`
- `table_roots` (`table_object_id -> root_page_id`)
- `index_roots` (`index_object_id -> root_page_id`)
- `sequence_state` (autoincrement/sequence durable state)
- `schema_cookie`
- `metadata_version`
- `commit_lsn`

Rationale:

- A single manifest gives one durable branch head target.
- Per-object roots avoid whole-database rewrites when one table/index changes.

### 3. Branch writes use page-level copy-on-write

For writes on branch `B`:

- Start from `B`'s current root manifest.
- For each modified B+Tree (table or index), allocate new pages for the changed
  path(s) from leaf to root.
- Never mutate a page reachable from another committed branch head or snapshot.
- Build a new manifest that points to new roots for changed objects and reuses
  existing roots for unchanged objects.
- Atomically advance `B`'s head to the new manifest at commit.

This preserves snapshot readers and enables cheap branch creation by metadata
only (shared roots until first write).

### 4. Existing pages are shared until divergence

Page sharing is explicit and expected:

- Branch creation shares all root pointers from the source manifest.
- Unchanged table/index roots remain shared across branches and snapshots.
- Only modified object paths allocate new pages.

Reachability and reclamation policy is defined by checkpoint/GC decisions in ADR
C and ADR D.

### 5. No file format version bump is required for this phase

ADR B does not require a top-level `.ddb` file format version bump.

The root manifest and branch-head metadata are introduced using existing durable
metadata mechanisms and page types already supported by the B+Tree-based engine.
If a future implementation needs new incompatible on-disk page layouts, that
must be handled by a separate ADR and migration path.

### 6. Interaction with B+Tree table and index roots

ADR 0120 remains authoritative: table and index storage stays B+Tree based.

Root manifest entries are the branch-specific root page IDs for those B+Trees:

- one root per logical table B+Tree
- one root per logical secondary/primary index B+Tree

On commit, modified table and index roots must advance together in the new
manifest so each branch head represents a single transactionally consistent
database state.

### 7. No OS reflink dependency

Branching and branch writes do not depend on filesystem reflinks, hardlinks, or
per-branch database file copies. All branching behavior is provided by DecentDB
metadata plus pager/WAL-managed page allocation.

## Consequences

- Branch creation can remain metadata-only and fast.
- Snapshot/time-travel opens can resolve state by manifest without replaying
  arbitrary branch history from WAL alone.
- Disk usage will grow with branch divergence and retained manifests, requiring
  branch-aware GC/retention policy (ADR D).
- Crash atomicity details for manifest install and branch-head advance are
  required in ADR C.

## Out of Scope

- WAL frame/commit record encoding and recovery order (ADR C).
- Branch/snapshot retention policy and page reclamation algorithm (ADR D).
- Diff/restore/merge semantics (ADR E).
- Sync behavior for branch-local writes (ADR F).

## References

- `design/adr/0003-snapshot-lsn-atomicity.md`
- `design/adr/0019-wal-retention-for-active-readers.md`
- `design/adr/0120-core-storage-engine-btree.md`
- `design/adr/0136-chunked-row-storage-for-coarse-grained-cow.md`
