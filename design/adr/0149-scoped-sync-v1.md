# ADR 0149: Scoped Sync v1
**Date:** 2026-05-17  
**Status:** Accepted

## Context

Slice 4 adds partial replication on top of the local-first sync foundation.
The engine already captures durable journals, exports and imports batches,
tracks peers and sessions, and exposes HTTP sync endpoints through the CLI.
The missing piece is a safe, deterministic v1 scope model that lets a peer
replicate only an explicit subset of tables and rows.

The design goal for v1 is strict safety and predictability, not full SQL
expressiveness. Future broader filtering is intentionally deferred.

## Decision

1. **Scopes are first-class sync catalog objects.**
   - A scope names an explicit table-inclusion set and an optional row filter.
   - The engine persists scopes in `__decentdb_sync_scopes`.
   - Peer bindings live in `__decentdb_sync_peer_scopes`.

2. **v1 row filters are intentionally narrow and deterministic.**
   - Supported predicates are only:
     - `<column> = <literal>`
     - `<column> != <literal>`
     - `<column> <> <literal>`
     - `<column> > <literal>`
     - `<column> >= <literal>`
     - `<column> < <literal>`
     - `<column> <= <literal>`
     - `<column> IN (<literal>, ...)`
     - `<column> IS NULL`
     - `<column> IS NOT NULL`
   - Predicates are combined by case-insensitive `AND` only.
   - Literals are restricted to integer, single-quoted text, `NULL`, `true`,
     and `false`.
   - Unsupported constructs are rejected up front, including `OR`, subqueries,
     `EXISTS`, functions, aggregates, window functions, JSON path operators,
     arithmetic, row values, and dotted/cross-table references.

3. **Delete safety is stricter than the broad future spec.**
   - If a scope uses a row filter, every referenced filter column must be part
     of the primary key for every included table.
   - This is required because sync journal delete records only carry the
     primary-key image.
   - The stricter rule prevents tombstone leakage or misclassification during
     scoped export/import.

4. **Peer bindings are scope-specific.**
   - A peer can bind to at most one scope at a time.
   - `sync run` uses the local peer binding when present:
     - push uses scoped export
     - pull uses scoped import
   - `sync serve --scope <name>` applies the same scope on the server side for
     `/changes` and `/import`.
   - Scoped export annotates batches with `source_high_watermark` so peers can
     advance past scanned out-of-scope journal records without leaking them.

5. **The engine remains HTTP-free.**
   - No HTTP dependency is added to `crates/decentdb`.
   - The CLI continues to own transport bytes and the dev server.

## Rationale

- Table inclusion is simple to reason about, easy to inspect, and maps well to
  tenant- or workload-based replication.
- Deterministic AND-only filters are enough for v1 and avoid creating an
  under-specified query language inside replication code.
- Requiring filter columns to be primary-key columns keeps delete capture and
  apply semantics safe without inventing richer tombstone payloads.
- Scope-aware peer bindings fit the existing peer catalog and CLI workflow
  without broadening the core transport surface.

## Consequences

- Scoped replication is available for tenants and other bounded subsets, with
  explicit validation and clear failure messages for unsupported definitions.
- Scoped export/import remains deterministic and testable from SQL, Rust, and
  the CLI.
- Broad filter expressiveness, including `OR` and general SQL predicates,
  remains backlog work for a future slice.

## References

- `design/WIN01_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`
- `design/FUTURE_WINS.md`
- `crates/decentdb/src/sync.rs`
- `crates/decentdb/src/db.rs`
- `crates/decentdb-cli/src/commands/mod.rs`
