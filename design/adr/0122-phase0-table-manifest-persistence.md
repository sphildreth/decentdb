# ADR-0122: Phase 0 Table-Manifest Persistence

## Status
Accepted

## Context
DecentDB's current Phase 0 storage path persists the entire `EngineRuntime` as a single `DDBSTATE1` overflow payload behind the catalog root page. That design was useful for bootstrapping SQL semantics, but it makes commit cost scale with total database size rather than the amount of data changed by the current statement or transaction.

After removing the major execution-path bottlenecks, the benchmark now reaches roughly `218k rows/sec` for the 100k-row explicit-transaction insert workload, but durable autocommit inserts still spend about `130 ms` at p95 because each commit rewrites the whole runtime blob. This violates the PRD's performance and predictability goals even though WAL durability and correctness remain intact.

We need an intermediate step that materially reduces write amplification without taking on the full Phase 1 B+Tree/slotted-page rewrite in a single change.

## Decision
We will replace the single runtime overflow blob with a **manifest payload plus per-table row payloads** while keeping the in-memory `EngineRuntime` model for now.

1. The root-page overflow pointer will reference a compact manifest payload rather than a full row-bearing runtime dump.
2. The manifest payload will store catalog metadata plus, for each table, the overflow pointer and checksum for that table's row payload.
3. Each table's row payload will be stored in its own overflow chain.
4. Commits will always rewrite the manifest payload, but they will only rewrite row payloads for tables marked dirty by the current work.
5. The engine must continue to **read** the legacy `DDBSTATE1` payload format so older databases remain openable. New writes may migrate the database to the manifest format.

## Rationale
This change attacks the actual remaining bottleneck: whole-database rewrite amplification on commit. Rewriting only the touched table keeps the existing WAL, pager, overflow pages, and SQL execution model intact while making commit cost proportional to changed tables instead of all rows in the database.

This is intentionally a bridge architecture, not the end state. It improves Phase 0 enough to unblock performance work while still preserving the accepted long-term direction in ADR-0120 toward page-backed B+Tree tables and incremental index maintenance.

## Alternatives Considered
### Keep the single runtime blob and continue micro-optimizing
Rejected. The remaining benchmark gap is structural, not parser/index bookkeeping. More clone, cache, or compression tuning would not remove the whole-runtime rewrite cost.

### Jump directly to full page-backed B+Tree table storage
Rejected for this slice. That remains the right long-term target, but it is too large and risky for the next incremental performance step.

### Persist per-index blobs in addition to per-table row blobs
Deferred. Secondary indexes are still runtime artifacts in Phase 0 and are rebuilt or maintained in memory. Adding durable per-index blobs now would add complexity without addressing the main benchmark bottleneck first.

## Trade-offs
- Positive: durable commit work becomes proportional to dirty tables instead of total database size.
- Positive: the change preserves current WAL semantics, transaction behavior, and the in-memory execution engine.
- Negative: this introduces dirty-table tracking and manifest compatibility logic, which increases persistence-path complexity.
- Negative: this is still not true page-backed table storage; row payloads remain serialized into overflow chains rather than slotted B+Tree pages.
- Negative: the first write after opening a legacy single-blob database may need to rewrite all tables to migrate into the manifest format.

## References
- `design/PRD.md`
- `design/adr/0020-overflow-pages-for-blobs.md`
- `design/adr/0031-overflow-page-format.md`
- `design/adr/0120-core-storage-engine-btree.md`
- `crates/decentdb/src/exec/mod.rs`
