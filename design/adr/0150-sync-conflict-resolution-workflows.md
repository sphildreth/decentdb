# ADR 0150: Sync Conflict Resolution Workflows
**Date:** 2026-05-17
**Status:** Accepted

## Context

Slices 1-4 made sync durable, inspectable, transportable, and scope-aware.
Conflicts were recorded, but operators still needed first-class workflows to
inspect, resolve, reopen, and configure how imports behave when conflicts
occur.

## Decision

1. **Safe default remains record-and-continue.**
   - The default conflict policy is `record`.
   - Conflicting remote records are not silently applied.
   - Imports continue after recording unresolved conflicts.

2. **Conflict policies are explicit.**
   - Supported v1 policies are `record`, `stop`, `last_writer_wins`, and
     `origin_priority`.
   - `stop` records the first conflict and fails the import/session clearly.
   - `last_writer_wins` is explicit remote-wins-on-conflict for v1; it does
     not infer causality from wall-clock timestamps.
   - `origin_priority` applies remote only when the configured replica
     precedence ranks the remote replica ahead of the local replica.

3. **Manual resolution is part of the engine contract.**
   - Conflicts can be shown individually, listed including resolved entries,
     resolved as keep-local, resolved by applying the remote record, and
     reopened.
   - Applying a remote conflict uses sync capture suppression so resolution
     does not create a new local sync journal record.

4. **Conflict records carry structured workflow fields.**
   - The conflict table keeps the existing columns and adds nullable columns
     for resolution, resolved timestamp, resolver, note, policy name, and
     local record JSON.
   - Existing tables are upgraded in place with guarded `ALTER TABLE ADD
     COLUMN` statements.
   - `local_row_json` remains for compatibility and mirrors local record JSON
     when a local row is available.

5. **CLI and SQL inspection are first-class.**
   - CLI commands cover conflict listing, show, resolve, reopen, and policy
     get/set.
   - `sync run --conflict-policy` and `sync serve --conflict-policy` allow
     per-session policy overrides.
   - `sys_sync_conflicts` remains available and `sys_sync_conflict_policy`
     exposes the configured policy.

## Consequences

- Conflicts are now operational objects, not passive diagnostics.
- Silent last-writer-wins behavior remains impossible unless explicitly
  configured.
- v1 policy semantics are deterministic and intentionally conservative.
- Richer merge strategies, per-table/per-scope policies, causality tracking,
  and custom merge handlers remain future work.

## References

- `design/WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`
- `design/FUTURE_WINS.md`
- `crates/decentdb/src/sync.rs`
- `crates/decentdb/src/db.rs`
- `crates/decentdb-cli/src/commands/mod.rs`
