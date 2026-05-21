# ADR 0157: Branch Diff, Restore, And Merge Semantics
**Date:** 2026-05-18
**Status:** Accepted

## Context

DecentDB branch workflows need user-facing operations that compare states,
restore known-good points, and eventually merge branch-local changes. These
operations must use database semantics, not text-file Git semantics. They must
respect primary keys, constraints, triggers, generated columns, transaction
atomicity, and the single-writer model.

Diff, restore, and merge are separate capabilities, but they share one semantic
foundation: each operation compares or moves named branch heads and immutable
snapshots.

## Decision

DecentDB will define diff, restore, and merge around branch heads, snapshots,
and root manifests.

### Diff inputs

Diff accepts any two named states:

- branch name
- snapshot name
- branch head ID
- retained LSN only when the corresponding root manifest is still retained

CLI examples:

```bash
decentdb diff --db app.ddb --from main --to migration-test
decentdb diff --db app.ddb --from before-sync --to main
```

### Schema diff

Schema diff reports catalog-level differences:

- tables added or removed
- columns added, removed, renamed, or changed
- type, nullability, default, and generated-expression changes
- constraint changes
- index changes
- trigger and view changes

Schema diff is available even when row diff is unsupported for one or more
tables.

### Row diff

Precise row diff requires a stable primary key. For primary-key tables, DecentDB
reports:

- inserted rows
- deleted rows
- updated rows
- changed columns for updated rows

Tables without primary keys are marked as unsupported for precise row diff in
the first implementation. Approximate hash-based diff is deferred and must
require an explicit user flag if added later.

JSON diff output is part of the stable tooling surface. Human table output may
evolve, but JSON fields must be versioned before external tools depend on them.

### Restore

Restore has two safe modes:

1. create a new branch from a snapshot/head
2. move an existing branch head to a snapshot/head

Moving an existing branch head requires:

- `--dry-run` support
- explicit `--confirm` for destructive restore
- no active write transaction on the target branch
- an automatic pre-restore snapshot unless explicitly disabled
- a report of data that becomes unreachable
- crash-safe branch head movement

Restoring a sync-enabled branch requires ADR 0158 guardrails.

### Merge

The first merge implementation is a constrained three-way merge:

```text
base B
├── target main at T
└── source migration-test at S
```

Merge computes:

- diff `B -> S`
- diff `B -> T`
- conflicts between source and target changes
- an apply plan for source changes into the target branch

The first merge supports only clean primary-key row changes. Schema merge is
rejected unless the source and target schemas are identical. Future trivial
additive schema merge can be designed later.

The only conflict policy in the first implementation is `stop`.

Conflicts include:

- same row updated in both branches
- row deleted in one branch and updated in the other
- same primary key inserted in both branches with different values
- source row change violates target constraints
- source and target schemas are incompatible

Clean merge applies source changes as one target-branch write transaction
through normal logical DML paths. Constraints, triggers, generated columns, and
sync capture for the target branch behave like ordinary application writes.

### Rebase

Rebase is out of scope. It may be reconsidered after constrained merge is
correct, observable, and well tested.

## Rationale

Primary-key row diff gives deterministic, explainable behavior for application
tables. Rejecting no-primary-key row diff avoids surprising hash heuristics in
the first release.

Restore is intentionally conservative because moving a branch head can make
newer data unreachable. Automatic pre-restore snapshots provide a recovery
point and make support workflows easier.

Merge must go through normal logical write paths so DecentDB preserves existing
constraint, trigger, generated column, and sync behavior. Physical row copying
would bypass too many engine semantics.

## Alternatives Considered

1. **Full Git-like merge.** Rejected for the first release. Relational schema,
   constraints, triggers, and generated values make arbitrary merge/rebase much
   harder than text merge.
2. **Hash-based row diff for all tables.** Deferred. It can be useful for audit
   reports, but it is not precise enough for merge semantics.
3. **Physical merge that installs source pages into the target.** Rejected.
   This would bypass logical validation, constraints, triggers, and sync.
4. **Restore without automatic pre-restore snapshot.** Rejected. The default
   must be recoverable and supportable.

## Trade-offs

**Positive:**
- deterministic diff for primary-key tables
- conservative restore defaults
- merge is atomic and uses existing write semantics
- conflict reporting is understandable

**Negative:**
- tables without primary keys do not get precise row diff initially
- first merge cannot handle schema divergence
- merge can be slower than page-level physical apply
- automatic pre-restore snapshots increase retained history until cleaned up

## Implementation Notes

1. Diff should produce stable JSON for tools before broad binding exposure.
2. Merge base must be stored or derivable from branch metadata.
3. `branch commit` markers are optional for merge; head IDs are authoritative.
4. Merge dry-run should be implemented before merge apply.
5. Restore and merge must be covered by fault-injection tests around branch head
   movement and target transaction commit.

## References

- `design/adr/0153-branch-metadata-identity-and-user-surface.md`
- `design/adr/0154-branch-root-manifest-and-copy-on-write-storage.md`
- `design/adr/0155-branch-aware-wal-commit-records-and-recovery.md`
- `design/adr/0158-branch-sync-interaction.md`
