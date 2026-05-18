# ADR 0152: .NET Sync SDK JSON Bridge
**Date:** 2026-05-17  
**Status:** Accepted

## Context

Slice 7 makes local-first sync pleasant to use from the flagship .NET SDK
while the sync surface is still evolving. The engine already exposes durable
sync primitives for status, peers, scopes, sessions, conflicts, retention,
doctoring, and manual batch exchange. What the SDK needs is a compact bridge
that preserves strong typing above the ABI without forcing many per-operation
native entry points.

The .NET binding also needs to stay fully in-process. Shelling out to the CLI
would make SDK integration harder to reason about, slower, and less portable.

## Decision

1. **Slice 7 uses one JSON request/response ABI for sync.**
   - The engine exposes a single C ABI entry point for sync operations:
     `ddb_db_sync_execute_json`.
   - Callers pass a UTF-8 JSON request object and receive a UTF-8 JSON
     response string.
   - This keeps the C ABI compact while sync v1 continues to evolve.

2. **Typed SDK objects live above the bridge.**
   - The .NET SDK deserializes the bridge payloads into strongly typed request
     and response models.
   - Other languages can build their own typed facades on top of the same JSON
     bridge without expanding the ABI surface immediately.

3. **Sync stays engine-local for v1.**
   - The flagship .NET SDK covers local engine sync operations, operational
     inspection, peer/scope/session workflows, conflict workflows, prune, and
     manual batch export/import.
   - Built-in HTTP transport parity for `sync run` remains future adapter work
     and is not part of this slice.

## Rationale

- A single JSON bridge is easier to keep stable while sync request/response
  shapes are still settling.
- Strongly typed .NET models preserve ergonomic SDK usage without duplicating
  the engine contract across many bespoke native functions.
- The bridge avoids shelling out to the CLI and keeps the SDK fully
  in-process.

## Consequences

- The C ABI stays small and easier to audit.
- The .NET SDK can offer a representative sync surface immediately, including
  agent-friendly raw JSON helpers.
- Future bindings can reuse the same bridge pattern instead of inventing new
  per-operation ABI functions.
- HTTP transport adapter work for `sync run` remains outside the v1 SDK
  polish slice.

## References

- `design/WIN01_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`
- `design/FUTURE_WINS.md`
- `crates/decentdb/src/c_api.rs`
- `crates/decentdb/src/db.rs`
- `bindings/dotnet/src/DecentDB.Native`
