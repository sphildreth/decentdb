# ADR 0158: Branch And Local-First Sync Interaction
**Date:** 2026-05-18
**Status:** Accepted

## Context

DecentDB now has native local-first sync with durable change journals, scoped
replication, peer/session metadata, conflict workflows, retention tooling, and
doctor surfaces. Branch, diff, restore, and time-travel workflows intersect
with sync because branch-local writes, preflight imports, merges, and restores
can all change what peers see.

The first branch implementation must preserve sync correctness without trying
to replicate branch graphs or branch metadata.

## Decision

The first branch implementation treats sync as a default-branch feature.

### Default branch

The default branch is `main`. Existing sync behavior applies to `main` only.

### Branch metadata

Branch metadata, snapshots, branch heads, root manifests, and branch commit
markers are local-only. They are not captured in the sync journal and are not
replicated to peers.

### Branch-local writes

Writes to non-default branches do not create sync journal records. They are
local sandbox writes until merged into a sync-enabled branch.

### Sync import preflight

`sync import --branch <name>` is allowed as a preflight workflow after branch
writes exist. Imported rows applied to a non-default branch are branch-local and
do not create outbound sync journal records from that branch.

The intended workflow is:

```bash
decentdb branch create sync-preflight --db app.ddb --from main
decentdb sync import --db app.ddb --branch sync-preflight --input incoming.json
decentdb diff --db app.ddb --from main --to sync-preflight
decentdb merge --db app.ddb sync-preflight --into main --dry-run
decentdb merge --db app.ddb sync-preflight --into main
```

### Merge into sync-enabled branch

Merging into `main` applies logical changes through the normal executor. If
sync is enabled on `main`, merge-generated inserts, updates, and deletes produce
ordinary sync journal records as if the application executed those writes
directly.

### Restore of sync-enabled branch

Restoring `main` when sync is enabled is a history-rewrite operation from the
point of view of peers. It requires an explicit override:

```bash
decentdb restore \
  --db app.ddb \
  --branch main \
  --to before-sync \
  --confirm \
  --allow-sync-history-rewrite
```

Restore must create an automatic pre-restore snapshot by default and must add a
sync doctor warning until the operator acknowledges or resolves the sync impact.

### Sync doctor

Sync doctor should report:

- branch-local changes that have not been merged
- sync-enabled branch restore warnings
- preflight branches created from sync imports
- retained branch history that can affect sync support bundles

## Rationale

Keeping sync on `main` only makes the first branch implementation useful without
turning sync into branch graph replication. Branch metadata is a local workflow
surface, not peer state.

Merge into `main` through normal logical DML preserves the existing sync journal
contract. Peers see the final accepted changes, not every local preflight or
agent-sandbox experiment.

Restore is intentionally guarded because peers generally expect monotonic
replica history. An explicit override prevents accidental data rewrites from
being silently exported.

## Alternatives Considered

1. **Replicate branch metadata and branch-local writes.** Rejected for the first
   implementation. This turns local workflow branches into distributed branch
   graph replication and greatly increases protocol complexity.
2. **Disable sync when branch support is enabled.** Rejected. Sync preflight is
   one of the strongest branch workflows.
3. **Record branch-local writes in the outbound sync journal.** Rejected.
   Peers should not receive sandbox writes unless they are explicitly merged.
4. **Allow unguarded restore of `main`.** Rejected. Restore can appear to peers
   as mass updates/deletes or non-monotonic state.

## Trade-offs

**Positive:**
- preserves existing sync journal semantics for `main`
- supports sync preflight workflows
- avoids distributed branch replication in v1
- keeps branch experiments local until merge

**Negative:**
- branch-local writes are not replicated
- peers cannot inspect branch graphs
- restore of sync-enabled `main` requires extra operator intent
- merge into `main` can produce a large sync batch

## Implementation Notes

1. Sync capture must check the target branch and skip non-default branches.
2. Sync CLI commands that mutate data must accept `--branch` only after
   branch-local writes exist.
3. Sync journal records do not include branch IDs in the first implementation.
4. Merge into `main` must use the same sync mutation capture paths as ordinary
   DML.
5. Restore warnings should be exposed through `sync doctor` and branch doctor
   surfaces.

## References

- `design/WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`
- `design/adr/0147-local-sync-journal-foundation.md`
- `design/adr/0148-sync-http-transport-and-peer-management.md`
- `design/adr/0149-scoped-sync-v1.md`
- `design/adr/0150-sync-conflict-resolution-workflows.md`
- `design/adr/0151-sync-operational-hardening.md`
- `design/adr/0152-dotnet-sync-sdk-json-bridge.md`
- `design/adr/0153-branch-metadata-identity-and-user-surface.md`
- `design/adr/0157-branch-diff-restore-and-merge-semantics.md`
