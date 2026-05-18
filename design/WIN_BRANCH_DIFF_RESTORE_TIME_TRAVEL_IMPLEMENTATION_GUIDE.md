# DecentDB Branch, Diff, Restore, And Time-Travel Implementation Guide

**Date:** 2026-05-18
**Status:** Proposed
**Audience:** Core engine developers, pager/WAL/checkpoint maintainers, SQL planner/executor maintainers, CLI maintainers, binding maintainers, documentation authors, coding agents
**Related roadmap item:** Branch, diff, restore, and time-travel workflows in `design/FUTURE_WINS.md`
**Related documents:** `design/PRD.md`, `design/SPEC.md`, `design/TESTING_STRATEGY.md`, `design/adr/0003-snapshot-lsn-atomicity.md`, `design/adr/0019-wal-retention-for-active-readers.md`, `design/adr/0136-chunked-row-storage-for-coarse-grained-cow.md`, `design/adr/0147-local-sync-journal-foundation.md`

---

## 1. Executive Summary

This document defines a proposed product and implementation plan for native
branch, diff, restore, and time-travel workflows in DecentDB.

The feature should let users treat a DecentDB database as an application data
workspace:

- create cheap database branches
- run risky writes or migrations on a branch
- inspect row/schema differences
- restore a branch to a known-good point
- open historical read-only views
- eventually merge a branch back into another branch with explicit conflict
  detection

The product goal is not to clone Git inside DecentDB. The goal is to provide a
database-native versioning workflow that is familiar enough for users who know
Git, while preserving DecentDB's core guarantees:

- durable ACID writes
- single process, one writer, many readers
- predictable snapshot isolation
- WAL-based crash recovery
- stable embedding and binding contracts

This is a large storage and product feature. It should not be implemented from
the existing `FUTURE_WINS.md` paragraph alone. It requires ADRs before code.

The recommended first implementation is deliberately conservative:

1. Define branch metadata, snapshot identity, retention, and CLI/API semantics.
2. Implement named immutable snapshots and read-only time-travel first.
3. Add cheap branch creation as metadata plus retained root/page history.
4. Add branch-local writes through copy-on-write root manifests.
5. Add diff and restore workflows.
6. Add constrained merge only after branch-local writes and diff are correct.

Full arbitrary relational merge and rebase should be deferred until the simpler
branch workflow proves correct, durable, and operationally understandable.

---

## 2. Product Thesis

The differentiator is not:

> "DecentDB can copy a database file."

The intended differentiator is:

> **DecentDB lets applications create safe, inspectable, branchable relational
> data workflows inside an embedded SQL engine.**

This matters because modern embedded applications increasingly need:

- safe local migration rehearsal
- offline sync preflight checks
- user-visible undo/restore points
- support bundles that preserve exact data state
- AI-agent sandboxes for generated data repair or schema migration scripts
- reproducible bug reports with data diffs instead of screenshots

SQLite has backups and changesets. RocksDB has snapshots and checkpoints. Dolt
has Git-like SQL versioning but is not a SQLite-style embedded engine. The
interesting DecentDB lane is:

> embedded SQL deployment plus first-class branch/diff/restore/time-travel
> workflows.

---

## 3. Why This Needs A Dedicated Plan

This feature crosses several architectural boundaries:

- persistent metadata
- WAL commit records
- checkpoint and page retention
- reader snapshot semantics
- branch-local write isolation
- CLI and binding contracts
- sync interaction
- crash recovery
- storage garbage collection

The single-writer model does not make branching impossible, but it does require
careful design. Branch creation should be a short metadata write, not a whole
database file copy under the writer lock. Branch writes and merges still
serialize through the existing one-writer model.

The feature must also avoid misleading Git analogies. Users will expect words
like `branch`, `checkout`, `diff`, `commit`, and `merge`, but database commits,
transaction isolation, constraints, triggers, and sync metadata are not text
files. The surface must be familiar without hiding database-specific rules.

---

## 4. Goals

### 4.1 Primary Goals

- Provide cheap named branches from a durable database state.
- Provide immutable named snapshots and historical read-only opens.
- Provide branch-aware `exec`, `repl`, and embedded API access.
- Provide row-level and schema-level diffs between branches/snapshots.
- Provide safe restore workflows with dry-run and explicit confirmation.
- Provide constrained merge workflows with conflict reports.
- Preserve DecentDB's one-writer / many-reader model.
- Preserve crash-safe recovery for branch metadata and branch writes.
- Make retention and disk growth visible through doctor/inspection surfaces.
- Keep user-facing behavior explicit enough for support and coding agents.

### 4.2 Secondary Goals

- Provide branch-aware support bundles.
- Allow migration tooling to run against branch sandboxes.
- Allow local-first sync imports to be rehearsed before affecting `main`.
- Allow AI agents to operate on a branch and produce diffs for human review.
- Provide a future path for Decent Bench visualization.

### 4.3 Non-Goals For The First Implementation

- Full Git compatibility.
- Textual Git storage, pack files, or Git object interoperability.
- Arbitrary relational rebase.
- Arbitrary automatic conflict resolution.
- Multi-writer concurrency.
- Cross-process branch writes before cross-process WAL coordination exists.
- OS-specific reflink dependence.
- Branch replication across sync peers.
- Loadable extension support.

---

## 5. User Model

The user-facing model should include familiar concepts with database-specific
definitions.

| Concept | Meaning In DecentDB |
|---|---|
| Branch | A named mutable line of database history with a current head. |
| Snapshot | An immutable named point-in-time view of a branch head. |
| Checkout | Select the branch used by a connection/session/command. It should not silently retarget every process. |
| Transaction commit | Existing SQL `COMMIT`; persists a transaction to the current branch head. |
| Branch commit | A named revision marker/message for the current branch head. It does not replace SQL transaction commit. |
| Diff | Schema and row changes between two branch heads or snapshots. |
| Restore | Move a branch head back to a snapshot or create a new branch from that snapshot. |
| Merge | Apply changes from one branch into another with validation and conflict detection. |
| Rebase | Replay branch changes on top of a newer target head. Deferred. |

### 5.1 The `commit` Naming Problem

Users coming from Git will expect `commit`, but SQL already has `COMMIT`.

The recommended rule:

- SQL `COMMIT` always means transaction commit.
- A branch commit is a metadata operation that names the current branch head.
- Avoid a top-level `decentdb commit` command at first.
- Prefer namespaced commands such as `decentdb branch commit`.

Example:

```bash
decentdb branch commit migration-test --db app.ddb --message "Normalize customer email"
```

This should create a named revision marker for the current head of
`migration-test`. It should reject if there is an open SQL transaction.

---

## 6. Real-World Workflows

### 6.1 Local-First Sync Preflight

A field-service app receives a sync batch from a remote peer. Before applying it
to the user's working data, the app creates a branch:

```bash
decentdb branch create sync-preflight --db app.ddb --from main
decentdb sync import --db app.ddb --branch sync-preflight --input incoming.batch.json
decentdb diff --db app.ddb --from main --to sync-preflight --format table
decentdb merge --db app.ddb sync-preflight --into main --dry-run
decentdb merge --db app.ddb sync-preflight --into main
```

Value:

- inspect unexpected deletes before they affect `main`
- show conflicts to a user or support engineer
- keep a reproducible branch when something looks wrong
- apply the final merge through the normal single-writer path

### 6.2 Migration Rehearsal

A desktop app needs to upgrade a local schema and backfill derived data.

```bash
decentdb branch create migration-v3 --db customer.ddb --from main
decentdb exec --db customer.ddb --branch migration-v3 --sql "ALTER TABLE customers ADD COLUMN normalized_email TEXT"
decentdb exec --db customer.ddb --branch migration-v3 --sql "UPDATE customers SET normalized_email = lower(email)"
decentdb diff --db customer.ddb --from main --to migration-v3 --schema --rows
decentdb branch commit migration-v3 --db customer.ddb --message "v3 migration rehearsal"
```

If validation passes, the app can merge or rerun the migration on `main`
depending on the accepted merge semantics.

### 6.3 AI Agent Sandbox

A coding agent is asked to repair inconsistent local data. The app creates an
agent branch:

```bash
decentdb branch create agent-repair-2026-05-18 --db app.ddb --from main
decentdb exec --db app.ddb --branch agent-repair-2026-05-18 --sql-file repair.sql
decentdb diff --db app.ddb --from main --to agent-repair-2026-05-18 --format json
```

The user reviews the diff before applying anything to `main`.

### 6.4 Support Snapshot

A support workflow creates a branch before running diagnostics:

```bash
decentdb snapshot create before-support --db app.ddb --branch main
decentdb doctor --db app.ddb --branch main --format markdown
decentdb restore --db app.ddb --branch main --to before-support --dry-run
```

Value:

- exact reproducibility
- safe diagnosis
- rollback point if a repair operation makes things worse

### 6.5 User-Facing Undo

A local app can create named restore points before destructive operations:

```bash
decentdb snapshot create before-bulk-delete --db library.ddb
decentdb exec --db library.ddb --sql "DELETE FROM tracks WHERE missing = true"
decentdb diff --db library.ddb --from before-bulk-delete --to main
decentdb restore --db library.ddb --branch main --to before-bulk-delete
```

---

## 7. Proposed User Surface

This section is a product sketch, not final syntax. The CLI/API surface must be
settled in an ADR before implementation.

### 7.1 CLI Commands

Branch lifecycle:

```bash
decentdb branch list --db app.ddb
decentdb branch create migration-test --db app.ddb --from main
decentdb branch delete migration-test --db app.ddb
decentdb branch rename migration-test migration-v3 --db app.ddb
```

Session/command selection:

```bash
decentdb exec --db app.ddb --branch migration-test --sql "SELECT count(*) FROM users"
decentdb repl --db app.ddb --branch migration-test
```

Interactive shell:

```text
decentdb> .branch
main

decentdb> .checkout migration-test
decentdb[migration-test]> SELECT count(*) FROM users;
```

Snapshots:

```bash
decentdb snapshot create before-sync --db app.ddb --branch main
decentdb snapshot list --db app.ddb
decentdb snapshot delete before-sync --db app.ddb
```

Branch commit marker:

```bash
decentdb branch commit migration-test --db app.ddb --message "Backfill normalized email"
decentdb branch log migration-test --db app.ddb
```

Diff:

```bash
decentdb diff --db app.ddb --from main --to migration-test
decentdb diff --db app.ddb --from before-sync --to main --table work_orders --format json
decentdb diff --db app.ddb --from main --to migration-test --schema
```

Restore:

```bash
decentdb restore --db app.ddb --branch main --to before-sync --dry-run
decentdb restore --db app.ddb --branch main --to before-sync --confirm
decentdb branch create restored-copy --db app.ddb --from before-sync
```

Merge:

```bash
decentdb merge --db app.ddb migration-test --into main --dry-run
decentdb merge --db app.ddb migration-test --into main --conflict-policy stop
```

Time-travel reads:

```bash
decentdb exec --db app.ddb --as-of before-sync --sql "SELECT * FROM work_orders"
decentdb exec --db app.ddb --branch main --as-of-lsn 12345 --sql "SELECT count(*) FROM events"
```

### 7.2 SQL Surface

The first implementation can be CLI/API-first. SQL syntax can follow once the
metadata model is stable.

Potential SQL shape:

```sql
CREATE BRANCH migration_test FROM main;
CREATE SNAPSHOT before_sync FROM main;

SELECT * FROM sys.branches;
SELECT * FROM sys.snapshots;
SELECT * FROM sys.branch_log WHERE branch_name = 'main';
SELECT * FROM sys.diff('main', 'migration_test');
```

Potential time-travel query shape:

```sql
SELECT *
FROM work_orders AS OF SNAPSHOT 'before_sync'
WHERE status = 'open';
```

Guardrail: do not introduce SQL grammar changes until the CLI/API semantics and
storage model are accepted.

### 7.3 Rust API Sketch

```rust
let db = Db::open("app.ddb")?;

db.branch_create("migration-test", BranchCreateOptions::from("main"))?;

let mut branch = db.open_branch("migration-test")?;
branch.execute("UPDATE users SET active = false WHERE last_seen < $1")?;

let diff = db.branch_diff("main", "migration-test", DiffOptions::default())?;
let report = db.merge_dry_run("migration-test", "main")?;
```

### 7.4 Binding API Direction

Initial binding exposure should be small:

- list branches
- create/delete branch
- open connection on branch
- list snapshots
- create/delete snapshot
- diff as JSON
- restore dry-run/report

Avoid exposing low-level page/root identifiers through bindings.

---

## 8. Required ADRs

This feature requires ADRs before implementation.

### ADR A: Branch Metadata, Identity, And User Surface

Must define:

- branch identifiers
- snapshot identifiers
- default branch behavior
- session-scoped checkout behavior
- branch commit marker semantics
- CLI/API/SQL naming
- hidden metadata tables or catalog records
- compatibility behavior for old databases

### ADR B: Root Manifest And Copy-On-Write Storage Strategy

Must define:

- how a branch head points at database state
- whether the root is a global manifest or per-table/per-index root set
- how branch writes allocate new pages
- how existing pages are shared between branches
- whether format version changes are required
- how this interacts with B+Tree table and index roots

### ADR C: WAL Commit Records And Crash Recovery For Branches

Must define:

- branch-aware WAL frame/commit metadata
- atomicity of branch head updates
- recovery order after crash
- checksum/validation implications
- whether WAL frames are branch-qualified or root-manifest-qualified
- how branch commits interact with existing `wal_end_lsn`

### ADR D: Checkpoint, Retention, And Garbage Collection

Must define:

- how branch snapshots pin pages and WAL history
- when checkpoint may reclaim old versions
- branch/snapshot retention policy
- doctor warnings for retained history
- disk growth guardrails
- branch deletion cleanup

### ADR E: Diff, Restore, And Merge Semantics

Must define:

- schema diff model
- row diff model
- primary-key requirement for precise row diffs
- tables without primary keys
- restore safety
- merge base tracking
- conflict definitions
- allowed conflict policies
- constraint and trigger behavior during merge

### ADR F: Sync Interaction

Must define:

- whether branch-local writes are captured by sync
- whether sync is restricted to the default branch initially
- how sync import/export behaves with `--branch`
- whether merge into `main` emits normal sync journal records
- whether branch metadata itself ever syncs

Recommended initial decision:

- Sync is enabled for `main` only unless explicitly configured later.
- Branch metadata is local-only.
- Branch-local writes do not replicate.
- Merging into a sync-enabled branch records normal sync changes.

---

## 9. Storage Architecture Direction

### 9.1 Baseline Constraints

DecentDB currently prioritizes:

- durable single-writer commits
- lock-free snapshot capture using LSNs
- WAL retention for active readers
- B+Tree-backed persisted storage
- page cache correctness
- stable C ABI and bindings

Branching cannot break these constraints.

### 9.2 Branch Creation Should Be Metadata-Only

Creating a branch should not copy the database file.

Target behavior:

```text
1. acquire writer lock
2. capture source branch head/root manifest
3. insert branch metadata
4. pin the source root/page history
5. commit and fsync metadata
6. release writer lock
```

The writer lock should be held only for a short metadata transaction.

### 9.3 Root Manifest

The preferred model is a branch head pointing at a durable root manifest:

```text
branch_head
  branch_id
  head_id
  parent_head_id
  root_manifest_id
  commit_lsn
  created_at
  optional_message
```

The root manifest should identify the database state needed to open a snapshot:

```text
root_manifest
  catalog_root
  table_roots
  index_roots
  sequence/autoincrement state
  schema cookie
  metadata version
```

The exact root shape depends on current storage internals and must be settled in
ADR B.

### 9.4 Copy-On-Write Writes

Branch-local writes should use copy-on-write behavior:

```text
main head H1
  table users root page 100

branch migration-test created from H1
  users root page 100 shared

write users on migration-test
  allocate new/modified pages
  produce new users root page 220
  branch head advances to H2
  main still points at page 100
```

Important rules:

- never mutate a page still reachable from another branch head or snapshot
- branch commit atomically installs a new root manifest for that branch
- readers keep using the root manifest they opened
- branch deletion releases reachability only after no readers need it

### 9.5 Why WAL Retention Alone Is Not Enough

DecentDB already retains WAL frames for active readers. Branches are different:

- active readers are temporary
- branch snapshots can be durable and long-lived
- a branch may need old page versions for days or months

Keeping all historical branch state only in the WAL would create unbounded WAL
growth and fragile recovery behavior.

The accepted design should provide durable page-version reachability and garbage
collection, not just "never truncate the WAL while branches exist."

### 9.6 Checkpoint And Garbage Collection

Checkpoint must become branch-aware.

The checkpoint/GC system needs to answer:

- which pages are reachable from live branch heads?
- which pages are reachable from named snapshots?
- which pages are needed by active readers?
- which WAL frames are needed for crash recovery?
- which old page versions can be reclaimed?

Doctor should report:

```text
branch history retained: 1.8 GB
oldest pinned snapshot: before-sync-2026-05-18
oldest pinned branch: migration-test
pages reclaimable after deleting branch: 1.2 GB
```

### 9.7 File Layout

Initial implementation should prefer the existing `.ddb` file plus normal WAL
sidecar unless ADR B proves a sidecar is necessary.

Avoid:

- OS-specific reflink requirements
- directory trees of per-branch database files
- hidden temporary copies that make durability unclear

If a sidecar is needed for retained historical pages, it must be documented as
part of the database's durable state and covered by backup/support workflows.

---

## 10. Diff Semantics

### 10.1 Diff Inputs

Diff should accept:

- branch name
- snapshot name
- branch head ID
- LSN if supported and retained

Examples:

```bash
decentdb diff --db app.ddb --from main --to migration-test
decentdb diff --db app.ddb --from before-sync --to main
```

### 10.2 Schema Diff

Schema diff should report:

- added/dropped/renamed tables
- added/dropped/renamed columns
- type/nullability/default changes
- constraint changes
- index changes
- trigger/view changes

Schema diff should be available even when row diff is disabled.

### 10.3 Row Diff

Precise row diff should use primary keys.

For each table with a stable primary key:

- added rows
- deleted rows
- modified rows
- changed columns per modified row

Tables without primary keys require an explicit decision:

Recommended first behavior:

- mark row diff as unsupported for tables without a primary key
- allow an approximate hash-based diff only with an explicit flag

### 10.4 Diff Output

Human table output:

```text
table       op       key      columns
----------  -------  -------  -------------------------
users       insert   id=42    name,email
users       update   id=7     status,updated_at
orders      delete   id=99    *
```

JSON output should be stable enough for tooling:

```json
{
  "from": "main",
  "to": "migration-test",
  "tables": [
    {
      "name": "users",
      "inserted": 1,
      "updated": 1,
      "deleted": 0,
      "changes": [
        {
          "op": "update",
          "primary_key": {"id": 7},
          "columns": {
            "status": {"from": "open", "to": "closed"}
          }
        }
      ]
    }
  ]
}
```

---

## 11. Restore Semantics

Restore is dangerous and must default to safe behavior.

### 11.1 Preferred Restore Modes

Create a new branch from a snapshot:

```bash
decentdb branch create restored-copy --db app.ddb --from before-sync
```

Dry-run moving a branch head:

```bash
decentdb restore --db app.ddb --branch main --to before-sync --dry-run
```

Actually move a branch head:

```bash
decentdb restore --db app.ddb --branch main --to before-sync --confirm
```

### 11.2 Restore Guardrails

Restore should:

- require a clean target branch state
- reject if the target has active write transaction
- warn if target branch is sync-enabled
- create an automatic pre-restore snapshot unless explicitly disabled
- support `--dry-run`
- explain data that would become unreachable
- be crash-safe

### 11.3 Restore And Sync

Restoring a sync-enabled branch can look like deletes/updates to sync peers or
can violate expectations about monotonic history.

Recommended first behavior:

- restoring `main` when sync is enabled requires `--allow-sync-history-rewrite`
- restore creates a sync doctor warning
- branch-created restore copies are safe and do not affect sync

This needs ADR F.

---

## 12. Merge Semantics

Merge should be constrained and explicit.

### 12.1 Merge Inputs

A merge needs:

- source branch head
- target branch head
- merge base head

```text
base B
├── target main at T
└── source migration-test at S
```

Merge computes:

- diff B -> S
- diff B -> T
- conflicts between source changes and target changes
- apply plan from S into T

### 12.2 Conflict Definitions

Conflicts include:

- same row updated in both branches
- row deleted in target and updated in source
- row deleted in source and updated in target
- same primary key inserted in both branches with different values
- schema changed incompatibly in both branches
- source changes violate target constraints
- source changes fire triggers that would produce additional changes

### 12.3 First Merge Capability

Recommended first merge support:

- dry-run required in examples and docs
- conflict policy default is `stop`
- only primary-key tables support row merge
- no automatic schema merge except trivial additive changes
- constraints validated before final commit
- merge applies as one target branch write transaction

Example:

```bash
decentdb merge --db app.ddb migration-test --into main --dry-run
decentdb merge --db app.ddb migration-test --into main --conflict-policy stop
```

### 12.4 Merge Output

Dry-run output:

```text
merge base: h-100
source: migration-test@h-130
target: main@h-145

applicable changes:
  users: 12 inserts, 4 updates, 0 deletes
  orders: 0 inserts, 2 updates, 0 deletes

conflicts:
  none
```

Conflict output:

```text
conflict table=users key=id=42 type=update/update
  target changed: email
  source changed: email
```

### 12.5 Rebase

Rebase means replaying a branch's changes on top of a newer target branch head.

Example:

```text
main@100
└── migration-test changes

main moves to 150 because sync/import/new local writes happen

rebase migration-test onto main@150
```

This is useful for long-running migration or agent branches, but it is harder
than merge because replay order, constraints, triggers, generated values, and
schema changes can all change results.

Recommended decision:

- do not implement rebase in the first release
- document it as a possible future feature after merge exists

---

## 13. Time-Travel Reads

Time-travel should start read-only.

### 13.1 Supported Inputs

- named snapshot
- branch commit marker
- branch head ID
- retained LSN
- timestamp only if commit timestamps are durable and monotonic enough for the
  intended meaning

### 13.2 CLI Examples

```bash
decentdb exec --db app.ddb --as-of before-sync --sql "SELECT count(*) FROM events"
decentdb repl --db app.ddb --as-of before-sync
```

`--as-of` opens a read-only view. Writes should fail clearly.

### 13.3 SQL Examples

SQL-level time travel is useful but can be deferred:

```sql
SELECT *
FROM work_orders AS OF SNAPSHOT 'before-sync'
WHERE status = 'open';
```

Adding parser syntax should wait until CLI/API time travel is already proven.

---

## 14. Single-Writer Implications

Branching must preserve the single-writer model.

| Operation | Writer impact |
|---|---|
| create branch | short metadata write transaction |
| create snapshot | short metadata write transaction |
| read branch | reader snapshot; no writer lock after open |
| write branch | normal single-writer transaction against selected branch |
| branch commit marker | short metadata write transaction |
| diff | read-only unless it materializes cached reports |
| restore | write transaction moving a branch head |
| merge | write transaction applying source changes into target |
| delete branch | metadata write plus later GC |

The key design requirement:

> branch creation must be cheap and metadata-driven.

It must not copy the whole database while holding the writer lock.

---

## 15. Internal Metadata Direction

Exact names are provisional. Internal metadata should be hidden from ordinary
schema listings, like sync metadata.

Possible internal structures:

```text
__decentdb_branch
  branch_id
  name
  current_head_id
  base_head_id
  created_at
  updated_at
  deleted_at

__decentdb_branch_head
  head_id
  branch_id
  parent_head_id
  root_manifest_id
  commit_lsn
  message
  created_at

__decentdb_snapshot
  snapshot_id
  name
  branch_id
  head_id
  created_at
  retention_policy

__decentdb_root_manifest
  root_manifest_id
  schema_cookie
  catalog_root
  table_root_manifest
  index_root_manifest

__decentdb_page_ref
  page_id
  ref_kind
  ref_id
  maybe_epoch
```

Implementation should avoid exposing this schema as stable public API at first.
Expose stable views instead:

```sql
SELECT * FROM sys_branches;
SELECT * FROM sys_branch_heads;
SELECT * FROM sys_snapshots;
SELECT * FROM sys_branch_retention;
```

Naming should align with future `sys.*` virtual table direction if that feature
lands first.

---

## 16. Interaction With Existing Features

### 16.1 Local-First Sync

Recommended initial behavior:

- default branch is sync-enabled
- branch-local writes are local-only unless explicit branch sync support exists
- merge into a sync-enabled branch records ordinary sync mutations
- branch metadata does not sync
- sync doctor reports branch retention and branch-local unsynced changes

Open question:

- Should `sync import --branch preflight` be allowed before branch writes are
  sync-aware? The product workflow is strong, but ADR F must define whether the
  imported changes should be captured in the source branch's sync journal or
  treated as branch-local only.

### 16.2 Triggers

Branch writes should run triggers exactly like normal writes.

Merge needs a specific decision:

- replay source DML and allow triggers to fire, or
- apply physical row changes and suppress triggers

Recommended first decision:

- merge applies logical DML through the normal executor so constraints and
  triggers behave like application writes
- if this creates surprising secondary changes, surface them in merge dry-run
  before final merge

### 16.3 Generated Columns

Generated columns should be recomputed through normal write paths during branch
writes and merges. Diff should show stored generated column differences if the
values are persisted.

### 16.4 Foreign Keys And Constraints

Branch-local writes use normal constraint enforcement.

Merge should validate all target constraints before commit and abort on failure.

### 16.5 Temporary Tables

Temporary tables are session-scoped and should not be branchable persistent
state. Diff/restore/merge should ignore temp objects.

### 16.6 In-Memory Databases

`:memory:` can support the branch API for tests and temporary workflows, but
durability claims apply only to persistent databases.

### 16.7 Bindings

Bindings should expose high-level branch APIs only after CLI behavior is stable.
The C ABI should avoid leaking internal root/page concepts.

---

## 17. Implementation Slices

Each slice must include tests and docs. Do not call a slice complete until its
definition of done is fully satisfied.

### Slice 0: Research, ADRs, And Storage Prototype

Status: `TODO`

Deliverables:

- ADR A through ADR F accepted or explicitly split/merged
- prototype root manifest representation
- prototype branch metadata stored durably
- explicit decision on file format version impact
- crash-recovery sketch validated by a small prototype

Definition of done:

- no production feature flag exposed
- ADRs accepted
- prototype proves branch create can be metadata-only
- risks and rejected alternatives documented

### Slice 1: Named Snapshots And Read-Only Time Travel

Status: `TODO`

Deliverables:

- create/list/delete named snapshots
- open read-only snapshot by name/head/retained LSN
- CLI `snapshot` commands
- `exec --as-of`
- read-only enforcement
- checkpoint retention aware of named snapshots
- doctor warning for retained history

Definition of done:

- crash tests for snapshot create/delete
- read snapshot remains stable while `main` advances
- checkpoint does not reclaim needed historical state
- deleting snapshot permits eventual reclamation
- docs and examples complete

### Slice 2: Branch Metadata And Branch-Scoped Reads

Status: `TODO`

Deliverables:

- create/list/delete/rename branches
- branch head metadata
- `exec --branch`
- `repl --branch`
- interactive shell `.branch` and `.checkout`
- branch visible in status/diagnostics

Definition of done:

- branch create is metadata-only
- branch read sees source state at fork point
- main can advance after branch creation
- branch read remains stable
- CLI and REPL docs updated

### Slice 3: Branch-Local Writes

Status: `TODO`

Deliverables:

- write transactions against non-default branch
- copy-on-write root/page behavior
- branch head advances atomically
- main and branch diverge safely
- crash recovery restores correct branch heads

Definition of done:

- writing branch does not alter main
- writing main does not alter branch
- concurrent readers preserve snapshot isolation
- WAL recovery handles branch commit records
- checkpoint/GC does not corrupt branch state

### Slice 4: Branch Commit Markers And Logs

Status: `TODO`

Deliverables:

- `branch commit --message`
- `branch log`
- named head/revision identifiers
- branch commit metadata in diagnostics

Definition of done:

- branch commit marker does not conflict with SQL `COMMIT`
- open SQL transaction rejection tested
- branch log survives reopen and crash recovery

### Slice 5: Diff

Status: `TODO`

Deliverables:

- schema diff
- primary-key row diff
- table filter
- JSON/table output
- diff against branch, snapshot, or head ID

Definition of done:

- add/update/delete row cases covered
- schema change cases covered
- tables without primary keys handled explicitly
- diff output stable enough for tooling
- docs include realistic examples

### Slice 6: Restore

Status: `TODO`

Deliverables:

- create branch from snapshot/head
- restore branch head to snapshot/head
- dry-run report
- automatic pre-restore snapshot
- sync-enabled branch guardrails

Definition of done:

- restore is crash-safe
- restore cannot silently destroy unreviewed state
- dry-run explains impact
- doctor surfaces restore/retention state
- docs warn about sync interactions

### Slice 7: Constrained Merge

Status: `TODO`

Deliverables:

- merge dry-run
- three-way row diff using merge base
- conflict report
- conflict policy `stop`
- apply clean primary-key row changes into target branch
- reject unsupported schema conflicts

Definition of done:

- clean non-overlapping row merges succeed
- update/update conflicts stop
- delete/update conflicts stop
- duplicate insert conflicts stop
- constraint violations stop before commit
- merge is atomic and crash-safe

### Slice 8: Bindings, Support Bundles, And Decent Bench Readiness

Status: `TODO`

Deliverables:

- stable C ABI wrappers for high-level branch operations
- .NET first binding surface
- JSON diff output for tools
- support bundle includes branch/snapshot metadata
- Decent Bench design notes for branch graph and diff UI

Definition of done:

- binding smoke tests
- docs for at least .NET
- support bundle examples
- Decent Bench handoff notes complete

---

## 18. Validation Strategy

### 18.1 Unit Tests

- branch metadata insert/update/delete
- branch name validation
- snapshot identity validation
- root manifest serialization
- branch head compare/update
- diff classification
- merge conflict classification

### 18.2 Integration Tests

- branch create then main write
- branch write then main read
- main write then branch read
- branch write then branch reopen
- snapshot read after checkpoint
- delete branch then GC candidate appears
- restore branch to snapshot
- clean merge
- conflicting merge

### 18.3 Crash And Fault Injection

Required crash points:

- after branch metadata write before commit
- after root manifest write before branch head update
- after branch head update before WAL sync
- during branch-local page writes
- during restore
- during merge
- during branch delete

Expected result:

- database opens or fails with a clear corruption/error state
- branch heads are either old or new, never half-applied
- main branch is not corrupted by failed branch operations

### 18.4 Checkpoint And GC Tests

- checkpoint while branch pins old pages
- checkpoint after deleting branch
- long-lived reader plus branch snapshot
- named snapshot preventing reclamation
- doctor output for retained pages

### 18.5 Sync Tests

- sync import into default branch
- sync import into preflight branch if supported
- branch-local write does not replicate unless explicitly supported
- merge into sync-enabled branch records expected journal records
- restore warning for sync-enabled branch

### 18.6 Performance Tests

- branch create latency independent of database size
- snapshot create latency independent of database size
- branch read overhead
- branch write overhead compared to main write
- diff performance on large primary-key tables
- checkpoint overhead with branch pins
- disk growth under retained branch history

Target:

- branch create should be metadata-scale, not database-size-scale
- branch-local point read should remain within a small constant factor of main
  branch point read
- branch-local writes may be slower initially but must remain bounded and
  predictable

---

## 19. Operational And Documentation Requirements

### 19.1 Doctor And Diagnostics

Doctor should detect:

- branch history preventing cleanup
- snapshots older than retention policy
- branch metadata inconsistency
- orphaned root manifests
- failed/incomplete merge records
- large retained history
- sync-enabled branch restore risk

### 19.2 User Docs

Required user docs:

- Branching overview
- Branch CLI reference
- Diff examples
- Restore guide
- Merge guide with conflict examples
- Time-travel reads
- Sync preflight workflow
- Migration rehearsal workflow
- AI agent sandbox workflow
- Operational retention and cleanup

### 19.3 API Docs

Required API docs:

- Rust branch APIs
- CLI command reference
- .NET branch APIs after binding exposure
- JSON output contracts for diff/merge/doctor

---

## 20. Open Questions

These must be resolved before implementation:

1. Does this require a file format version bump?
2. Is the root manifest global or per-table/per-index?
3. Are old page versions retained inside the main `.ddb`, in the WAL, or in a
   new sidecar?
4. What is the exact default branch name: `main`, `default`, or something else?
5. Is checkout ever persisted as database default state, or always
   connection/session-scoped?
6. Are branch-local writes included in sync journals?
7. Can `sync import --branch` exist in the first release?
8. Should branch commit markers be mandatory before merge, or optional?
9. How are tables without primary keys diffed?
10. Are schema merges supported at all in the first merge release?
11. What retention policy prevents branch history from filling disks?
12. What is the minimum C ABI surface for branch-aware bindings?

---

## 21. Recommended Initial Decisions

These are the starting recommendations for ADR discussion:

1. Default branch name is `main`.
2. Checkout is session/connection-scoped by default.
3. Branch creation is metadata-only.
4. Snapshots are immutable.
5. Time-travel is read-only in the first release.
6. Branch-local writes use copy-on-write roots.
7. Diff requires primary keys for precise row diffs.
8. Merge is deferred until branch writes and diff are stable.
9. First merge supports only clean primary-key row changes.
10. Rebase is out of scope.
11. Sync applies to `main` only at first.
12. Merge into `main` emits ordinary sync journal changes if sync is enabled.
13. Restore of sync-enabled `main` requires explicit override.
14. Doctor must ship with branch retention diagnostics.
15. No OS reflink dependency.

---

## 22. Go / No-Go Criteria

Proceed only if the team is willing to accept:

- persistent storage design changes
- WAL/checkpoint changes
- new retention and GC complexity
- several ADRs before implementation
- broad crash testing
- careful user education around branch vs SQL transaction semantics

Do not proceed if the desired product is merely:

- backup/restore convenience
- file copy automation
- SQLite session-extension parity

This feature is worth doing only if DecentDB wants branchable relational data to
be part of its identity.

---

## 23. Summary Recommendation

This feature is a genuine DecentDB differentiator, but it is also one of the
largest storage features on the roadmap.

The best path is not to start by building `merge`. The best path is:

1. accepted ADRs
2. named snapshots
3. read-only time travel
4. branch metadata
5. branch-local copy-on-write writes
6. diff
7. restore
8. constrained merge

If those steps are executed carefully, DecentDB would have a compelling feature
that few embedded SQL engines can match: safe, inspectable, local branchable
application data.
