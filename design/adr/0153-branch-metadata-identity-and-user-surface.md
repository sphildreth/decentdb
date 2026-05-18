# ADR 0153: Branch Metadata, Identity, and User Surface
**Date:** 2026-05-18
**Status:** Accepted

## Context

FUTURE_WINS #1 (branch, diff, restore, and time-travel workflows) requires a
stable user and metadata contract before storage/WAL implementation details are
chosen. Section 8 of
`design/WIN_BRANCH_DIFF_RESTORE_TIME_TRAVEL_IMPLEMENTATION_GUIDE.md` requires
ADR A to define identity, default branch behavior, checkout scope, branch
commit semantics, naming, metadata catalog direction, and compatibility for
existing databases.

This ADR intentionally does not decide root-manifest layout, WAL encoding,
checkpoint retention/GC, or merge conflict algorithms; those are covered by ADR
B through ADR F.

## Decision

1. **Branch and snapshot identity are split into stable IDs and mutable names.**
   - Branches and snapshots each have an internal immutable ID and a user-facing
     unique name.
   - Branch and snapshot names are case-sensitive identifiers and must be unique
     within their own namespace.
   - User commands and public APIs address branches/snapshots by name.
   - Internal IDs are used for durable metadata linkage and are not part of the
     initial public binding contract.

2. **The default branch name is `main`.**
   - Persistent databases have exactly one default branch named `main`.
   - Existing command/API calls that do not specify a branch operate on `main`.
   - New branch-aware flags and APIs may override this per command/session.

3. **Checkout is session/connection-scoped, not process-global.**
   - CLI `exec`/`repl` branch selection applies only to that invocation/session.
   - Interactive `.checkout` changes only the current REPL session branch.
   - The engine does not persist a global "current branch" pointer in database
     metadata.

4. **Branch commit markers are metadata annotations, not SQL transaction
   commits.**
   - SQL `COMMIT` keeps its existing transaction meaning.
   - A branch commit marker names the current branch head with optional message
     metadata for history/audit workflows.
   - Branch commit markers are optional and not required before merge in this
     phase.
   - Initial CLI naming is `decentdb branch commit ...`; no top-level
     `decentdb commit` command is introduced.

5. **CLI/API naming is branch-explicit and Git-familiar without overloading SQL
   semantics.**
   - Branch selection flag: `--branch <name>`.
   - Time-travel selectors: `--as-of <snapshot-or-revision-name>` and
     `--as-of-lsn <lsn>` for read-only opens.
   - Lifecycle verbs: `branch create|list|delete|rename|commit|log`,
     `snapshot create|list|delete`, plus top-level `diff`, `restore`, and
     `merge`.
   - Rust/binding API names should mirror these concepts (`branch_*`,
     `snapshot_*`, `diff`, `restore`) and avoid ambiguous generic names such as
     `commit()` for branch markers.

6. **Inspection surfaces are standardized on `sys.*` names, with compatibility
   aliases allowed where needed.**
   - Canonical inspection surfaces are:
     - `sys.branches`
     - `sys.branch_heads`
     - `sys.snapshots`
     - `sys.branch_retention`
     - `sys.branch_log`
   - If an adapter layer still exposes underscore-style names (for example
     `sys_branches`), they are compatibility aliases only and must map to the
     canonical `sys.*` contract.

7. **Internal metadata catalog names use hidden `__decentdb_*` tables and are
   not public API.**
   - Branching metadata is stored in internal catalog tables prefixed with
     `__decentdb_` and filtered from ordinary schema listings, matching existing
     internal-table policy.
   - Initial table set is:
     - `__decentdb_branches`
     - `__decentdb_branch_heads`
     - `__decentdb_snapshots`
     - `__decentdb_root_manifests` (logical placeholder; structure finalized by
       ADR B)
     - `__decentdb_page_refs` (logical placeholder; structure finalized by ADR
       D)
   - Exact physical schemas may evolve in ADR B/D, but names and hidden-catalog
     intent are fixed by this ADR.

8. **Existing databases are upgraded lazily and compatibly.**
   - Databases without branch metadata are treated as legacy single-branch
     databases equivalent to `main`.
   - Branch metadata tables are created lazily on first branch/snapshot/restore/
     time-travel operation requiring them.
   - Bootstrap creates `main` anchored to the database's current durable head at
     upgrade time.
   - Legacy non-branch-aware reads/writes continue to work unchanged and target
     `main`.

## Consequences

- User behavior is explicit and predictable across CLI, REPL, and bindings.
- Branch operations can be introduced incrementally without breaking existing
  clients.
- Internal metadata remains implementation-private while stable inspection
  surfaces are exposed.
- Root/WAL/GC complexity remains deferred to ADR B/C/D with no conflict on
  naming or identity semantics.

## Out of Scope

- Root manifest storage format and branch write copy-on-write mechanics (ADR B)
- WAL commit record changes and crash recovery ordering (ADR C)
- Retention reclamation and GC algorithms (ADR D)
- Diff/restore/merge conflict semantics (ADR E)
- Sync interaction policy beyond naming compatibility assumptions (ADR F)

## References

- `design/WIN_BRANCH_DIFF_RESTORE_TIME_TRAVEL_IMPLEMENTATION_GUIDE.md`
- `design/FUTURE_WINS.md`
- `design/adr/0147-local-sync-journal-foundation.md`
