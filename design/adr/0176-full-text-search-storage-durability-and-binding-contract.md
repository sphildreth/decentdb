# Full-Text Search Storage, Durability, And Binding Contract
**Date:** 2026-05-26
**Status:** Accepted

### Decision

DecentDB full-text search will store FTS data as engine-owned derived secondary
index state. The index must be maintained through the normal single-writer
transaction path, must be planner-visible, and must be rebuildable from the base
table.

The FTS storage model will include these logical structures:

- term dictionary with term bytes, term ids, document frequency, and total term
  frequency
- postings keyed by term id and row id, including term frequency and token
  positions
- document statistics keyed by row id, including indexed document length and
  optional per-field lengths
- index metadata containing analyzer config, document count, average document
  length, build generation, dirty/rebuild state, and verification metadata

The implementation should reuse existing B+Tree and overflow-page mechanisms
where practical. Large postings lists must be chunked and must not be stored as
one unbounded blob per term.

FTS indexes must never be used silently if recovery or verification determines
that they may be stale or corrupt. Recovery may mark an FTS index as needing
rebuild if the implementation proves that this preserves base-table durability
and query correctness. Queries against a stale FTS index must either trigger an
explicitly accepted rebuild path or fail with a clear error.

Bindings will consume FTS through ordinary SQL and scalar result values. A
dedicated FTS C ABI is not required for v1. C ABI changes are limited to ABI
versioning, metadata, or helper functions if implementation proves they are
needed. Binding packages must add smoke tests and examples, but must not
implement tokenization or ranking.

### Rationale

FTS data is large, derived from table contents, and expensive to duplicate in
primary WAL payloads. Treating FTS as a derived secondary index keeps the base
table as the source of truth while still allowing persisted search structures,
fast ranked lookup, and explicit repair.

The storage decision follows these product constraints:

- committed base table rows remain the durable source of truth
- FTS query results must match committed table contents
- derived index corruption must be detectable and repairable
- index maintenance must honor transaction commit and rollback
- WAL and checkpoint behavior must remain understandable
- WASM/mobile portability must not depend on external native search components

The binding decision keeps the C ABI stable and avoids duplicating search logic
across languages. If the SQL surface is correct, every binding can prepare the
same statements and read rank values as ordinary `FLOAT64` columns.

### Alternatives Considered

- **Fully WAL-log every postings mutation as a primary data record.** Rejected as
  the default direction because it risks large write amplification. It may still
  be used internally if a later storage design proves it is the simplest correct
  implementation.
- **In-memory FTS only.** Rejected. Rebuilding on every open is unacceptable for
  application databases with large local corpora.
- **Hidden SQL tables as the public storage model.** Rejected. It exposes too
  much implementation detail and makes planner integration harder.
- **One opaque serialized FTS blob per index.** Rejected. It has poor update,
  verification, and large-index behavior.
- **Mandatory external search library.** Rejected for portability, auditability,
  and long-term storage ownership reasons.
- **Dedicated per-binding FTS APIs.** Rejected. They would increase drift and
  make SQL examples less portable.

### Trade-offs

- Derived index state requires careful stale/corrupt detection and rebuild
  tooling.
- Persisting analyzer identifiers and versions is required to avoid silent
  semantic drift across releases.
- Transactional maintenance adds write cost to indexed text updates.
- Phrase and prefix support require position and dictionary data, increasing
  index size.
- If the new index kind requires an on-disk format version bump, ADR 0131
  requires a read-only migration parser update in `decentdb-migrate`.

### References

- `design/WIN_FULL_TEXT_SEARCH_BM25_SPEC.md`
- `design/adr/0175-native-full-text-search-query-surface-and-ranking.md`
- `design/adr/0007-trigram-postings-storage-strategy.md`
- `design/adr/0052-trigram-durability.md`
- `design/adr/0063-trigram-postings-paging-format.md`
- `design/adr/0131-legacy-format-migrations.md`
- `include/decentdb.h`

