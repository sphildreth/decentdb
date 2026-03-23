# ADR-0123: Phase 1 Table B+Tree Foundation

## Status
Accepted

## Context
DecentDB's current Phase 0 runtime still keeps table rows in memory and persists them via manifest-managed table payload blobs. That bridge architecture removed the worst commit write amplification, but it is still not true page-backed table storage and it does not move the engine toward the accepted B+Tree-centered 1.0 design.

The repository already has accepted decisions for:
- compact variable-length B+Tree pages with sequential cell parsing (`ADR-0035`)
- compact record encoding (`ADR-0030`)
- integer primary key / rowid semantics (`ADR-0036`)

We need an incremental Phase 1 step that starts using those accepted storage semantics without attempting a risky all-at-once migration of the SQL runtime and persistence layers.

## Decision
We will introduce a reusable **table-row B+Tree foundation** that wraps the existing generic `Btree<S>` and stores:

1. **Keys:** signed logical row ids using the accepted integer-PK / rowid ordering semantics.
2. **Values:** encoded row payloads using the existing `record::row::Row` format.
3. **Pages:** the already accepted compact varint B+Tree page layout with sequential parsing.

### Scope of this slice
- Add a `TableBtree<S: PageStore>` wrapper with typed row APIs (`insert_row`, `get_row`, `delete_row`, cursors).
- Preserve signed `i64` row ordering by mapping row ids into sortable `u64` B+Tree keys.
- Keep the underlying generic B+Tree implementation unchanged for now.
- Do **not** yet wire the SQL runtime or manifest persistence onto this abstraction in the same slice.

### Explicit non-goals for this slice
- No slotted-page or slot-directory table layout.
- No new table root pointers in the on-disk catalog yet.
- No WAL/checkpoint rule changes.
- No attempt to replace the current generic rebuild-oriented B+Tree writer in the same change.

## Rationale
This gives the repository a concrete, typed storage abstraction for page-backed table rows while staying aligned with the accepted page, record, and rowid decisions. It also creates a narrow seam for later work: once the runtime can target `TableBtree`, the remaining persistence migration becomes a matter of root-page/catalog ownership and incremental mutation behavior rather than inventing yet another row container.

Using the existing compact B+Tree page layout is important. The Rust rewrite's current design documents explicitly reject stale slotted-page assumptions for this storage path, so the new foundation must build on the accepted sequentially parsed varint-cell layout instead of introducing a competing page format.

## Alternatives Considered
### Jump directly to full runtime integration
Rejected for this slice. That would mix page-format, runtime, catalog, WAL, and migration concerns into one change.

### Add a separate slotted-page table format
Rejected. The accepted Rust design direction already standardizes the compact B+Tree page layout with sequential parsing for this engine generation.

### Keep using raw `Btree<Vec<u8>>` calls everywhere
Rejected. That would leave signed rowid ordering, row encoding, and cursor decoding duplicated across future callers.

## Trade-offs
- Positive: starts the Phase 1 storage path with low risk and strong testability.
- Positive: keeps future runtime integration aligned with accepted record/page/rowid ADRs.
- Negative: this slice alone does not improve runtime persistence performance because the SQL engine does not use it yet.
- Negative: it still inherits the current generic B+Tree writer behavior, which rebuilds page images eagerly and must be improved in later slices.

## References
- `design/PRD.md`
- `design/adr/0120-core-storage-engine-btree.md`
- `design/adr/0035-btree-page-layout-v2.md`
- `design/adr/0030-record-format.md`
- `design/adr/0036-integer-primary-key.md`
- `crates/decentdb/src/btree/`
