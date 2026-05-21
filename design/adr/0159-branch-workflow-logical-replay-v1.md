# ADR 0159: Branch Workflow Logical Replay V1

**Status:** Accepted
**Date:** 2026-05-18

## Context

ADR 0154 described a future page/root copy-on-write storage model for branch
writes. During implementation, the first complete branch workflow was delivered
through durable branch-head metadata plus SQL replay entries linked to the head
that introduced each branch-local write.

This keeps the user-facing workflow small and auditable while avoiding a file
format bump and broad pager/WAL branch-record changes in the first release.

## Decision

The first shipped branch workflow uses logical SQL replay for non-`main` branch
writes:

- branch creation records a base branch head at the source retained LSN
- non-`main` branch writes execute against a materialized branch state
- successful branch writes append a SQL replay entry linked to a new branch head
- branch materialization walks the current head ancestry and replays linked SQL
  entries in order
- restore creates a restore head that points at the target head
- diff and merge operate on materialized refs

The C ABI exposes the feature through `ddb_db_branch_execute_json`; it does not
expose root/page internals.

## Consequences

Benefits:

- no database file format bump
- no new unsafe storage ownership model
- branch history is human-readable and easy to diagnose
- branch restore and fork-from-branch can be represented by head ancestry
- branch workflows are available to CLI, Rust, and C ABI users now

Trade-offs:

- branch-local writes are slower than direct page/root copy-on-write for large
  branch histories
- SQL replay is constrained to SQL that DecentDB can re-execute deterministically
- branch-local parameterized writes are rejected in this release because replay
  stores SQL text, not a parameter log
- page-level COW remains a possible future storage optimization, but it is no
  longer required for the first complete product workflow

## Validation

The implementation includes tests for:

- named snapshots and read-only time travel
- branch-local write isolation
- branch write survival after reopen
- fork-from-branch replay
- branch commit/log
- primary-key row diff
- branch restore dry-run and confirmed restore
- clean merge into `main`
- update/update merge conflict stop
- CLI branch workflow coverage
- C ABI branch JSON smoke coverage

## References

- ADR 0154: Branch Root Manifest And Copy-On-Write Storage
- ADR 0157: Branch Diff, Restore, And Merge Semantics
