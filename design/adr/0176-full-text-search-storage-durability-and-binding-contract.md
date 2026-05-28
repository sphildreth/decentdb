# Full-Text Search Storage, Durability, And Binding Contract
**Date:** 2026-05-26
**Status:** Accepted

### Decision

DecentDB full-text search will store FTS data as engine-owned derived secondary
index state. The index must be maintained through the normal single-writer
transaction path, must be planner-visible, and must be rebuildable from the base
table.

The catalog must add `IndexKind::FullText` plus typed full-text options metadata
reachable from `IndexSchema`. Analyzer configuration must not live only in
rendered SQL text or in ad hoc per-call parsing. Structured introspection and
tooling metadata must expose the full-text index kind and normalized analyzer
options.

The FTS storage model will include these logical structures:

- term dictionary with term bytes, term ids, document frequency, and total term
  frequency
- postings keyed by term id and row id, including term frequency and token
  positions
- document statistics keyed by row id, including indexed document length,
  optional per-field lengths, and enough state to handle `NULL -> text` and
  `text -> NULL` transitions
- index metadata containing analyzer config, document count, average document
  length, build generation, dirty/rebuild state, and verification metadata

The logical storage key families are:

- `(index_id, TYPE_META)`
- `(index_id, TYPE_TERM, term_bytes)`
- `(index_id, TYPE_PREFIX, prefix_bytes)`
- `(index_id, TYPE_POSTING, term_id, chunk_start_row_id)`
- `(index_id, TYPE_DOC_STAT, row_id)`

Postings chunks are keyed by term id and starting row id, with compressed payloads
that include row-id deltas, document generation, term frequency, and positions.
This keeps high-frequency terms chunked and allows query execution to stream
postings without loading a full list into memory.

`NULL` indexed text values contribute no tokens and are not indexed as a literal
string. Ranking document count and average document length count only rows with
at least one indexed token.

The implementation should reuse existing B+Tree and overflow-page mechanisms
where practical. Large postings lists must be chunked and must not be stored as
one unbounded blob per term.

FTS indexes must never be used silently if recovery or verification determines
that they may be stale or corrupt. Recovery may mark an FTS index as needing
rebuild if the implementation proves that this preserves base-table durability
and query correctness. Queries against a stale FTS index must either trigger an
explicitly accepted rebuild path or fail with a clear error.

FTS maintenance must satisfy DecentDB read-your-writes semantics. V1 should use
transaction-local FTS delta overlays merged with persistent postings at query
time. Checkpoint-only materialization that makes FTS results lag behind visible
base table rows is not acceptable.

Deletes and indexed-column updates do not need to physically purge obsolete row
ids from postings chunks synchronously. Query execution must filter candidates
through base-table visibility and current document statistics. Updates that
preserve row id require document-generation filtering so an old posting for the
same row id cannot match the new document content. `ALTER INDEX ... REBUILD`
compacts obsolete postings.

`ALTER INDEX ... REBUILD` is synchronous in v1. It uses the normal single-writer
path, blocks other writes, builds replacement FTS state from a consistent
snapshot, and atomically swaps the rebuilt index at commit. Partially rebuilt
state must never be visible. `ALTER INDEX ... VERIFY` is synchronous and
read-only from the user's perspective.

FTS data must be stored through the same database, WAL, and sync-journal byte
paths as other engine data. TDE therefore encrypts FTS term dictionaries,
postings, document statistics, and metadata at rest. FTS must not create
plaintext sidecar files. Verification, Doctor, and default error messages must
not print raw indexed terms.

FTS follows branch-visible base table state. Branch-local writes must either
maintain branch-local FTS generations or mark that branch's FTS index stale;
they must not mutate another branch's visible FTS state. Branch diff, restore,
and sync changesets treat FTS as derived state and capture base table changes
rather than postings or term dictionary mutations.

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
- same-transaction FTS reads must match same-transaction table visibility
- obsolete postings left behind by deletes or same-row updates must be filtered
  by visibility and document generation
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
- **Eager purge of every obsolete posting during DELETE/UPDATE.** Rejected as a
  required v1 behavior because it creates random-write amplification. Rebuild can
  compact obsolete entries.
- **Checkpoint-only FTS materialization.** Rejected for v1 if it causes
  same-transaction or committed FTS reads to lag behind visible base table rows.
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
- Transaction-local delta overlays and generation filtering add executor
  complexity, but they preserve DecentDB's visibility contract while avoiding
  unnecessary random posting rewrites.
- Phrase and prefix support require position and dictionary data, increasing
  index size.
- Branch-aware derived state may force stale-index marking or branch-local
  generations until a later storage ADR proves safe sharing.
- The new index kind and persisted FTS options require an on-disk format version
  bump. ADR 0131 requires a read-only migration parser update in
  `decentdb-migrate`.

### References

- `design/WIN_FULL_TEXT_SEARCH_BM25_SPEC.md`
- `design/adr/0175-native-full-text-search-query-surface-and-ranking.md`
- `design/adr/0007-trigram-postings-storage-strategy.md`
- `design/adr/0052-trigram-durability.md`
- `design/adr/0063-trigram-postings-paging-format.md`
- `design/adr/0131-legacy-format-migrations.md`
- `include/decentdb.h`
