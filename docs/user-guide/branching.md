# Branching, Diff, Restore, And Time Travel

DecentDB supports named snapshots and branch-scoped workflows for local testing,
migration rehearsal, support investigation, and agent sandboxes.

## Snapshots

A named snapshot records the current durable `main` state and keeps the required
history available until the snapshot is deleted.

```bash
decentdb snapshot create --db app.ddb --name before-migration
decentdb exec --db app.ddb --as-of before-migration --sql "SELECT COUNT(*) FROM users"
```

Time-travel execution is read-only. Use a branch when you want to write against
a retained point.

## Branches

Create a branch from `main`, another branch, a snapshot, or a head ID:

```bash
decentdb branch create --db app.ddb --name migration-test --from before-migration
decentdb exec --db app.ddb --branch migration-test --sql "UPDATE users SET plan = 'trial' WHERE id = 1"
decentdb exec --db app.ddb --sql "SELECT plan FROM users WHERE id = 1"
```

The branch write is isolated from `main`. Use `decentdb repl --branch <name>` for
interactive branch-scoped sessions.

## Logs And Commit Markers

Branch writes create branch heads. Commit markers let tools and humans label a
branch state without changing rows:

```bash
decentdb branch commit --db app.ddb --name migration-test --message "validated trial-plan rewrite"
decentdb branch log --db app.ddb --name migration-test
```

## Diff

Diff compares two refs: `main`, a branch name, a named snapshot, or a head ID.
Primary-key row changes are reported as added, updated, and deleted rows.

```bash
decentdb branch diff --db app.ddb --left main --right migration-test --format json
```

Tables without primary keys and unsupported schema changes are reported
explicitly instead of guessed.

## Restore

Restore moves a non-`main` branch head to a branch, snapshot, or head target.
Run a dry-run first, then confirm:

```bash
decentdb branch restore --db app.ddb --name migration-test --to before-migration --dry-run
decentdb branch restore --db app.ddb --name migration-test --to before-migration --confirm
```

The old branch heads remain in the log, so the operation is auditable.

## Merge

Merge applies clean primary-key row changes from a source branch into `main` or
another branch. It uses the source branch's recorded base head for a three-way
row comparison and stops on conflicts.

```bash
decentdb branch merge --db app.ddb --source migration-test --target main --dry-run
decentdb branch merge --db app.ddb --source migration-test --target main --confirm
```

The first merge release is intentionally constrained: it supports identical
schemas and primary-key row inserts, updates, and deletes. Update/update,
delete/update, update/delete, duplicate insert, missing primary key, and schema
conflicts are reported and not applied.
