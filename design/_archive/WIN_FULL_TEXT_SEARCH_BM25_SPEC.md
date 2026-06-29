# Full-Text Search With BM25 Ranking

**Date:** 2026-05-26
**Status:** Implemented; retained as design and acceptance reference
**Future Version:** vNext
**Roadmap:** [`FUTURE_WINS.md`](../FUTURE_WINS.md)
**Document Type:** Implementation SPEC
**Audience:** Core engine developers, SQL planner/executor maintainers, storage
maintainers, binding maintainers, documentation authors, benchmark maintainers,
coding agents

**Governing ADRs:**

- [`adr/0175-native-full-text-search-query-surface-and-ranking.md`](../adr/0175-native-full-text-search-query-surface-and-ranking.md)
- [`adr/0176-full-text-search-storage-durability-and-binding-contract.md`](../adr/0176-full-text-search-storage-durability-and-binding-contract.md)

**Implementation status, 2026-05-26:** The engine implements `USING fulltext`
indexes, persisted analyzer config, `fulltext_match`, `bm25`, phrase/prefix
querying, write-path maintenance, planner prefiltering, rebuild/verify SQL,
tooling metadata, documentation, and regression coverage. Follow-up work should
be filed as performance, hybrid-search, fuzzy-search, or binding-helper items
rather than reopening this roadmap win.

**Related inputs:**

- [`FUTURE_WINS.md`](../FUTURE_WINS.md)
- [`docs/design/spec.md`](../../docs/design/spec.md)
- [`adr/0007-trigram-postings-storage-strategy.md`](../adr/0007-trigram-postings-storage-strategy.md)
- [`adr/0008-trigram-pattern-length-guardrails.md`](../adr/0008-trigram-pattern-length-guardrails.md)
- [`adr/0052-trigram-durability.md`](../adr/0052-trigram-durability.md)
- [`adr/0063-trigram-postings-paging-format.md`](../adr/0063-trigram-postings-paging-format.md)
- [`adr/0112-cost-based-optimizer-with-stats.md`](../adr/0112-cost-based-optimizer-with-stats.md)
- [`STABLE_TOOLING_METADATA_CONTRACT.md`](../STABLE_TOOLING_METADATA_CONTRACT.md)

---

## 1. Executive Summary

DecentDB already has a native trigram index for substring search. That is useful
for `LIKE '%term%'`, but it does not replace full-text search. Application
databases also need tokenized keyword search, phrase search, prefix search, and
ranked results for notes, messages, documents, help content, local knowledge
bases, and offline application data.

This spec defines the target for DecentDB native full-text search (FTS) with
BM25 ranking. The product goal is not to clone SQLite FTS5 syntax. The goal is
to provide a first-class DecentDB index mode that is portable, durable,
query-planner aware, easy to use from every binding, and competitive with
SQLite FTS5 and DuckDB FTS on representative embedded application workloads.

The feature is not done when the Rust engine can run one search query. It is
done when the SQL surface, index storage, transaction behavior, recovery
semantics, binding tests, user documentation, and benchmark coverage are all
complete.

## 2. Product Goals

- Native full-text index mode over one or more `TEXT` columns.
- BM25 ranking returned as an ordinary numeric SQL result.
- Tokenized term search, phrase search, and prefix search.
- Persisted analyzer policy covering tokenizer, case folding, stopwords,
  stemming, and prefix rules.
- Planner integration for FTS predicates, scalar filters, ranking, and top-k
  queries.
- Incremental maintenance through normal `INSERT`, `UPDATE`, `DELETE`, and
  transaction paths.
- Query consistency with committed base table contents.
- Rebuild, verify, and recovery semantics that never silently use a stale or
  corrupt FTS index.
- Binding-friendly behavior through ordinary SQL, prepared statements, and
  scalar result types.
- Representative benchmarks against SQLite FTS5 and DuckDB FTS.

## 3. Non-Goals

- No virtual-table-only user model.
- No arbitrary native extension or external search engine dependency.
- No network search service or server process.
- No vector/HNSW search in this win. Hybrid search is a later roadmap item.
- No fuzzy matching, spelling suggestions, highlighting, or snippet generation
  in the first required slice. These may be later FTS-adjacent slices.
- No language-specific binding implementation of the search engine.
- No role/user authorization model. Existing embedded policies and audit
  context remain the local security boundary.

## 4. Current Context

DecentDB has these relevant foundations:

- `CREATE INDEX ... USING trigram (...)` for substring search.
- Secondary index metadata and planner paths for B+Tree and trigram indexes.
- WAL, checkpoint, recovery, branch, and sync infrastructure that must remain
  coherent with any derived index.
- Stable C ABI SQL execution and prepared statement APIs used by maintained
  bindings.
- Query contract and tooling metadata surfaces that should expose new FTS
  indexes once shipped.
- Local security v1: TDE, row policies, projection masks, audit context, and
  audit tables.

Trigram code can inform postings compression, rebuild patterns, and planner
guardrails, but FTS must be a separate index kind. Trigram tokens are fixed
three-byte substrings. FTS terms are analyzer-produced lexical tokens with term
frequency, document length, positions, and ranking statistics.

## 5. User-Facing SQL Surface

The target SQL surface is:

```sql
CREATE INDEX idx_docs_search
ON docs USING fulltext (title, body)
WITH (
  tokenizer = 'unicode',
  language = 'simple',
  stopwords = 'none',
  stemming = 'none',
  prefix = '2,3'
);

SELECT id, title, bm25('idx_docs_search') AS rank
FROM docs
WHERE fulltext_match('idx_docs_search', $1)
ORDER BY rank DESC
LIMIT 20;
```

### 5.1 DDL

FTS is a native index mode:

```sql
CREATE INDEX index_name
ON table_name USING fulltext (column_name [, ...])
[WITH (option = value [, ...])];

DROP INDEX index_name;
ALTER INDEX index_name REBUILD;
ALTER INDEX index_name VERIFY;
```

Initial DDL rules:

- `fulltext` is the only accepted v1 access-method keyword. `fts`,
  `full_text`, and `gin` must not alias to FTS. The existing `gin` alias remains
  trigram-only compatibility behavior until a separate ADR changes it.
- Indexed expressions are out of scope for v1. FTS keys must be plain `TEXT`
  columns.
- Multi-column FTS indexes are supported by treating selected columns as fields
  in one document.
- `UNIQUE`, `INCLUDE`, and partial FTS indexes are out of scope for v1.
- Analyzer options are persisted in catalog metadata and cannot silently change
  after index creation.
- Unknown `WITH` options, invalid option types, duplicate options, and unsupported
  analyzer values are rejected at DDL time with `DbErrorCode::Sql`; they are not
  silently ignored.
- If analyzer changes are later supported, they require `ALTER INDEX ... REBUILD`
  semantics.

#### Indexed Value Semantics

- `NULL` indexed column values contribute no tokens. They are not indexed as the
  literal text `NULL`.
- Empty strings and strings that analyze to zero tokens contribute no tokens.
- Multi-column FTS indexes analyze fields in declared column order. Field
  boundaries are hard phrase boundaries in v1, so a quoted phrase cannot match
  across `title` and `body` unless a later ADR explicitly adds cross-field phrase
  behavior.
- V1 implements hard field boundaries with a persisted analyzer position gap:
  `FTS_FIELD_POSITION_GAP = 256`. If `title` produces positions `0, 1`, then the
  first `body` token starts at `1 + 256 = 257`. Phrase matching still checks
  consecutive positions, so phrases cannot cross fields without any special
  multi-field phrase logic. Document length counts real tokens only, not gap
  positions.
- Document length is the sum of analyzed tokens from non-NULL fields.
- Ranking corpus statistics (`N` and `avgdl`) count only rows whose indexed
  document length is greater than zero. The implementation may still store
  zero-length document-stat rows for verification and update bookkeeping.
- `NULL -> text`, `text -> NULL`, and `text -> text` updates must be covered by
  incremental maintenance tests.

### 5.2 Query Predicate

The v1 predicate is parser-compatible SQL:

```sql
WHERE fulltext_match(index_name_text, query_expression)
```

Rules:

- `index_name_text` must be a string literal naming a full-text index visible to
  the query. Identifiers are intentionally not accepted in v1 so the syntax stays
  unambiguous through the existing PostgreSQL parser.
- The FTS index's base table must be present in the query scope.
- `query_expression` is a normal SQL expression that evaluates to `TEXT`.
- Prepared statement parameters are supported.
- `fulltext_match(...)` returns a boolean predicate and can be combined with
  scalar filters.
- `fulltext_match(...)` is supported in the `WHERE` clause of a `SELECT` query
  block, including query blocks inside CTEs, subqueries, and `EXISTS`.
- `fulltext_match(...)` in `JOIN ON`, `HAVING`, `CHECK`, generated-column
  expressions, triggers, and partial-index predicates is out of scope for v1.
- A future ADR may add `index_name MATCH query_expression` as syntactic sugar.
  V1 must not patch `libpg_query`; if the sugar is added later, `MATCH` must be
  a contextual keyword only in the FTS predicate position.

Example:

```sql
SELECT id, title
FROM docs
WHERE fulltext_match('idx_docs_search', $1)
  AND tenant_id = $2
ORDER BY bm25('idx_docs_search') DESC
LIMIT 10;
```

### 5.3 Ranking

`bm25(index_name_text)` returns `FLOAT64`.

Rules:

- Higher scores sort as better matches.
- `index_name_text` must be a string literal naming the same full-text index used
  by a compatible `fulltext_match(index_name_text, ...)` predicate in the same
  query block.
- `bm25(...)` is valid only in the `SELECT` list and `ORDER BY` for v1. It is
  not valid in `WHERE`, `JOIN ON`, `GROUP BY`, or aggregate arguments.
- CTEs, subqueries, and views are validated per query block. A `bm25(...)` call
  in an outer query cannot use a `fulltext_match(...)` predicate from an inner
  query block, and the reverse is also invalid.
- A CTE or subquery may project `bm25('idx') AS rank`; an outer query may then
  sort or filter on that projected `rank` column as an ordinary `FLOAT64`.
- Views may contain literal FTS query predicates, but parameterized FTS through a
  view definition is out of scope for v1. Applications should use a CTE/subquery
  when the FTS query string is a runtime parameter.
- The same-block validation is a semantic analysis/planning error before row
  execution begins.
- Invalid scope returns `DbErrorCode::Sql` with the stable message prefix
  `FTS semantic error: bm25 requires fulltext_match in the same query block`.
- Default parameters are `k1 = 1.2` and `b = 0.75`.
- A future extension may accept explicit `k1` and `b` arguments, but v1 should
  keep the stable one-argument form.
- Multi-column indexes rank the concatenated indexed fields in v1. Full BM25F
  field weighting can be a later compatible enhancement if benchmarks justify it.

The accepted formula is:

```text
score(q, d) = sum over terms t in q:
  idf(t) * (tf(t,d) * (k1 + 1)) /
           (tf(t,d) + k1 * (1 - b + b * dl(d) / avgdl))

idf(t) = ln(1 + (N - df(t) + 0.5) / (df(t) + 0.5))
```

where `N` is non-empty indexed document count, `df` is document frequency, `tf`
is term frequency, `dl` is indexed-token count for the document, and `avgdl` is
average non-empty document length for the index.

This is the non-negative IDF variant commonly used by Lucene-style BM25:

```text
idf(t) = ln(1 + (N - df(t) + 0.5) / (df(t) + 0.5))
```

It is chosen deliberately to avoid negative scores for high-frequency terms.
It is not the BM25+ delta variant. Benchmark comparisons against SQLite FTS5 and
DuckDB FTS must document ranking formula differences where they affect result
ordering.

### 5.4 Query Language

The initial FTS query language should support:

- bare terms: `database`
- quoted phrases: `"embedded database"`
- whitespace-separated terms as `AND`
- explicit `OR`
- unary exclusion: `-draft`
- suffix prefix terms when enabled: `dece*`
- escaping quotes and operator characters with backslash

Out of scope for v1:

- `NEAR`
- nested proximity expressions
- regex
- fuzzy edit-distance syntax
- user-defined query parsers

V1 escape rules:

- SQL string escaping happens before the FTS query parser sees the query text.
- Inside quoted phrases, `\"` represents a literal double quote and `\\`
  represents a literal backslash.
- Outside quoted phrases, backslash escapes the next FTS operator character.
  Required escaped operator characters are `"`, `\`, `*`, and leading `-`.
- A trailing unpaired backslash is an `FTS query error:`.
- An empty query, a query that becomes empty after analysis, or a query with only
  excluded terms is an `FTS query error:` in v1.

Invalid FTS query syntax must produce a normal SQL error with enough context for
binding tests and application diagnostics.

### 5.5 Error Surface

FTS v1 uses the existing engine error taxonomy. It does not add new stable
numeric error codes unless a later C ABI ADR expands the public error model.

Binding tests should assert the existing numeric code plus stable message
prefixes:

| Condition | Error Code | Stable Message Prefix |
|---|---:|---|
| invalid `WITH` option or value | `DbErrorCode::Sql` / 5 | `FTS DDL error:` |
| unknown full-text index | `DbErrorCode::Sql` / 5 | `FTS semantic error:` |
| `fulltext_match` references a table not in scope | `DbErrorCode::Sql` / 5 | `FTS semantic error:` |
| `bm25` without same-block match predicate | `DbErrorCode::Sql` / 5 | `FTS semantic error:` |
| invalid FTS query syntax | `DbErrorCode::Sql` / 5 | `FTS query error:` |
| prefix query when prefix is disabled or too short | `DbErrorCode::Sql` / 5 | `FTS query error:` |
| stale index requiring rebuild | `DbErrorCode::Sql` / 5 | `FTS index requires rebuild:` |
| corrupt FTS storage detected by verify/query | `DbErrorCode::Corruption` / 2 | `FTS index corruption:` |

The C ABI continues to surface these through the normal status return and
`ddb_last_error_message()` path.

## 6. Analyzer Policy

Analyzer configuration is part of the persisted index definition.

Minimum analyzer policy:

| Option | v1 Requirement |
|---|---|
| `tokenizer` | `unicode` tokenizer implemented in Rust and portable to WASM/mobile |
| `language` | `simple` required; `english` may be added only through a pure-Rust portable implementation |
| `stopwords` | `none` required; built-in language list optional but must be persisted by name/version |
| `stemming` | `none` required; language stemming optional but must be deterministic and versioned |
| `prefix` | disabled by default; comma-separated auxiliary prefix index lengths such as `'2,3'` |
| case folding | deterministic Unicode-aware lowercasing policy |
| diacritics | `preserve` by default; `remove` optional only if implemented through a stable, versioned, portable table |

Analyzer changes are compatibility-sensitive. Any implementation must persist a
stable analyzer identifier and version so a future DecentDB release can detect
when an FTS index needs rebuild because tokenization behavior changed.

Prefix semantics:

- `prefix = 'none'` or omitting `prefix` disables prefix queries.
- `prefix = '2,3'` stores auxiliary prefix entries of exactly length 2 and
  exactly length 3 for each indexed token whose length is at least that prefix
  length. This follows the SQLite FTS5 convention for the option format.
- `prefix = '3'` stores only 3-character prefix entries.
- A query term ending in `*` is allowed only when its prefix length is at least
  the smallest configured prefix length.
- The executor uses the longest configured prefix length less than or equal to
  the query prefix length, then filters matching terms by the full query prefix.
  For example, `dece*` with `prefix = '2,3'` uses the `dec` auxiliary prefix
  entries and filters terms that begin with `dece`.
- V1 should reject more than three configured prefix lengths, lengths below 1,
  lengths above 8, duplicate lengths, and non-numeric values with
  `FTS DDL error:`.
- Prefix indexes increase storage size. Benchmarks must report the storage and
  ingest cost of prefix-enabled indexes separately from the default no-prefix
  configuration.

## 7. Storage Model

FTS index storage is engine-owned derived secondary index data.

### 7.1 Catalog Representation

The implementation must add an explicit full-text index kind and a typed options
representation to the catalog.

Required catalog changes:

- Add `IndexKind::FullText`.
- Add a typed full-text options/config field reachable from `IndexSchema`. The
  preferred shape is an enum or optional struct such as
  `IndexOptions::FullText(FullTextIndexConfig)` rather than ad hoc string
  parsing at every call site.
- `FullTextIndexConfig` must include analyzer id/version, tokenizer, language,
  stopwords, stemming, case-folding policy, diacritic policy, prefix lengths,
  field position gap, and any ranking defaults that become persistent.
- Existing BTree, trigram, and spatial indexes must round-trip with empty/default
  options.
- Catalog serialization must have deterministic field ordering and explicit
  versioning.
- Adding `IndexKind::FullText` and persisted full-text options is a database
  format change. Phase 2 must bump the format version and update the
  `decentdb-migrate` read-only parser per ADR 0131 before integration tests rely
  on persistent FTS databases.

Required introspection contract:

- `list_indexes`, `sys.*` index surfaces, SQL dump rendering, and tooling
  metadata must expose FTS index kind and analyzer options.
- SQL dump output must render the original semantic DDL, including `USING
  fulltext` and normalized `WITH (...)` options.
- Bindings should rely on tooling/query metadata for structured analyzer
  options. SQLite-master-style SQL text may be exposed for compatibility, but it
  is not the only stable introspection surface.

### 7.2 Runtime And Persistent Structures

Required logical structures:

- term dictionary: normalized term bytes, term id, document frequency, total term
  frequency
- postings: term id to row id entries with term frequency and positions
- document table: row id to indexed document length, field lengths, deletion
  generation, and optional per-field metadata
- index metadata: analyzer config, document count, average document length,
  build generation, dirty/rebuild state, checksum or equivalent verification
  marker

Implementation should reuse existing B+Tree and overflow-page mechanisms where
that keeps the storage model simple and portable. Large postings lists must be
chunked; one unbounded blob per term is not acceptable.

Logical key namespace:

- FTS should use typed, byte-stable keys in ordinary engine-owned B+Trees rather
  than inventing custom page formats.
- Keys must include a stable `index_id` or equivalent internal index storage id.
  If the catalog does not already expose a stable index id, Phase 2 must add one
  or explicitly justify a canonical-name encoding.
- Recommended key families:
  - `(index_id, TYPE_META)` -> index metadata and verification marker
  - `(index_id, TYPE_TERM, term_bytes)` -> `(term_id, doc_freq, total_term_freq)`
  - `(index_id, TYPE_PREFIX, prefix_bytes)` -> compressed term-id list or term-id
    range metadata for prefix lookup
  - `(index_id, TYPE_POSTING, term_id, chunk_start_row_id)` -> compressed
    postings chunk
  - `(index_id, TYPE_DOC_STAT, row_id)` -> `(doc_generation, doc_len,
    field_lens_array)`
- Postings chunk payloads should use varint/delta encoding for row ids,
  document generations, term frequency, and positions. Encoding must support
  streaming one chunk at a time.
- `chunk_start_row_id` is the first row id represented in the postings chunk and
  keeps high-frequency terms from requiring one huge postings value.
- Postings entries must include enough document-generation information to
  distinguish current row content from obsolete entries left behind by updates.

Verification marker requirements:

- Phase 2 must define the concrete verification marker before storage code
  lands.
- The minimum acceptable marker is a per-index metadata checksum or digest over
  the analyzer config, document-count statistics, average-length numerator, term
  count, structure root identifiers, and build generation.
- `ALTER INDEX ... VERIFY` must recompute the metadata marker and also validate
  B+Tree/postings chunk structure and document-stat reachability.
- Query execution is not required to recompute full-index checksums on every
  query.

The catalog and storage changes require a format-version bump. ADR 0131 applies,
and the `decentdb-migrate` read-only migration parser must be updated in Phase 2.

Implementation impact checklist:

- DDL access-method parsing in `exec/ddl.rs` must accept only `fulltext` for FTS.
- Every `IndexKind` match arm in DDL, DML maintenance, planner, constraints,
  introspection, dump rendering, runtime clone/rebuild, and tests must handle
  `FullText` explicitly.
- Full-text indexes are not constraint indexes and must be excluded from unique,
  foreign-key, and `ON CONFLICT` enforcement paths.

### 7.3 Storage And Scaling Guidance

FTS storage is expected to be large. The implementation should optimize for
predictable scaling rather than hiding the cost.

Initial design targets:

- Default no-prefix FTS index size should be measured and reported as a ratio of
  indexed UTF-8 text bytes. The implementation should aim for the practical
  range of roughly `0.5x` to `2.0x` indexed text size for normal note/message
  corpora, while documenting cases that exceed it.
- Prefix-enabled indexes must report separate size and ingest overhead because
  prefix entries can materially increase index size.
- Search memory should be proportional to query terms, postings chunk buffers,
  candidate sets, and top-k heap size. It must not require loading all postings
  for a high-frequency term into memory at once.
- Rebuild memory should stream base rows and postings construction in bounded
  batches. A simple first implementation may use larger temporary maps only for
  small corpora, but the v1 completion bar requires a bounded path for the
  benchmark sizes in section 13.
- Postings chunks should target page-friendly encoded payloads, normally in the
  `512` byte to `4 KiB` range, and must support postings lists that span many
  chunks.
- Benchmarks must include index-size, ingest/update/delete cost, search latency,
  rebuild time, and verify time.

## 8. Transaction, Recovery, And Rebuild Semantics

Required transaction behavior:

- Inserted rows become searchable at the same transaction visibility boundary as
  the base table rows.
- Updated indexed columns logically remove old tokens and add new tokens at the
  same visibility boundary.
- Deleted rows are removed from search results at the same visibility boundary.
- Rollback reverts all FTS maintenance.
- Same-transaction reads follow DecentDB's normal read-your-writes rules.
- Row policies filter FTS candidates before rows are returned.

FTS must not copy trigram's checkpoint-only pending-op visibility if that would
make committed or same-transaction FTS reads lag behind base table visibility.
V1 should use transaction-local FTS delta overlays rather than eager temporary
index copies:

- The active write transaction keeps in-memory FTS additions and logical
  removals keyed by index, term id or analyzed term, row id, and document
  generation.
- FTS query execution streams candidates from persistent postings and merges
  them with the transaction-local overlay.
- Rollback drops the overlay.
- Commit applies the overlay metadata needed for future readers without
  requiring every obsolete posting entry to be physically removed immediately.

This overlay direction minimizes write amplification during a transaction while
preserving read-your-writes. If implementation proves an alternative is simpler,
it must preserve the same visibility contract and be documented with a follow-up
ADR note before landing.

Deletion and update cleanup:

- V1 does not need to eagerly purge every obsolete row id from existing postings
  chunks during `DELETE` or indexed-column `UPDATE`.
- Query execution must filter posting candidates through base-table visibility
  and current document statistics.
- Same-row updates require generation filtering: a posting entry for `(row_id,
  old_generation)` must not match the current `(row_id, new_generation)`.
  Base-table visibility alone is not enough for updates that preserve row ids.
- Missing or deleted document statistics cause the candidate to be ignored.
- `ALTER INDEX ... REBUILD` compacts away obsolete postings. Background vacuum
  for FTS postings is a later feature.

Required recovery behavior:

- Reopen after clean shutdown preserves FTS results and ranking stats.
- Crash recovery must not silently use an index that may be missing committed
  changes.
- If the FTS index is derived and repairable, recovery may mark it
  `needs_rebuild`; queries against that index must either rebuild explicitly or
  fail with a clear error.
- `ALTER INDEX ... REBUILD` reconstructs FTS data from the base table.
- `ALTER INDEX ... VERIFY` validates term/document statistics and postings
  reachability without changing query results.
- Doctor integration should report stale, corrupt, or missing FTS derived data.

`ALTER INDEX ... REBUILD` and `ALTER INDEX ... VERIFY` semantics:

- Both operations are synchronous in v1.
- `REBUILD` is a write operation. It takes the normal single-writer path, blocks
  other writes, builds replacement FTS state from a consistent base-table
  snapshot, and atomically swaps the rebuilt index at commit. Partially rebuilt
  state is never visible.
- Readers that already hold a snapshot before rebuild continue using their
  snapshot. New readers after the rebuild commit use the rebuilt index. If the
  only visible index is stale and no rebuild has committed, FTS queries fail with
  `FTS index requires rebuild:`.
- `REBUILD` on a fresh index is valid and forces a deterministic rebuild; it is
  not an error.
- `REBUILD` cannot change analyzer options in v1.
- `VERIFY` is read-only from the user's perspective and reports corruption or
  stale metadata without repairing it.
- Background rebuild and progress reporting are out of scope for v1. A later
  runtime-tracing/Doctor extension may expose progress.

TDE and diagnostics:

- FTS term dictionary, postings, document statistics, and metadata must be stored
  in the same database/WAL/sync-journal byte paths as other persistent engine
  data so TDE encrypts them at rest.
- FTS must not create plaintext sidecar files.
- `VERIFY`, Doctor findings, and error messages must not print raw indexed terms
  by default. Diagnostics may report counts, row ids where already visible, term
  ids, hashes, and structural locations. Any future verbose term-dump tool must
  be explicit and documented as sensitive.

### 8.1 Branch And Sync Interaction

FTS is derived index state over branch-visible base table rows.

Rules:

- FTS query results must follow the currently checked-out branch snapshot.
- Branch creation may share existing FTS derived data only while the branch's
  indexed base-table state is identical to the source branch.
- The first branch-local write that affects an indexed column must either
  maintain a branch-local FTS generation or mark that branch's FTS index stale
  until rebuilt. It must not mutate another branch's visible FTS state.
- Branch diff, restore, and logical sync changesets should treat FTS data as
  derived state. They should capture base table changes, not postings or term
  dictionary mutations.
- Merge/replay into a branch must either apply FTS maintenance through the normal
  write path or mark affected FTS indexes stale and require rebuild.
- If branch-aware FTS sharing requires new root manifests or format semantics, a
  follow-up ADR is required before implementation.

## 9. Planner And Executor Integration

The planner must recognize `fulltext_match('index_name', expr)` as an FTS
access path.

Required planner behavior:

- Use FTS candidate retrieval before evaluating expensive row predicates.
- Combine FTS with scalar filters, row policies, joins, `ORDER BY`, and `LIMIT`.
- Recognize `ORDER BY bm25('index_name') DESC LIMIT N` as a top-k search shape.
- Preserve deterministic results for tied scores by requiring a stable secondary
  ordering in tests where exact order matters.
- Include FTS access in `EXPLAIN`.
- Surface planner stats through existing or extended metadata where useful.

Required executor behavior:

- Phrase search verifies token positions, not string contains.
- Prefix search consults the term dictionary and applies configured prefix
  limits.
- `bm25('index_name')` uses the matched query terms and persisted statistics.
- FTS results remain ordinary rows and ordinary scalar values to the rest of the
  execution pipeline.

### 9.1 Costing And Statistics

FTS planning must integrate with the cost model rather than always preferring
the FTS index.

Required direction:

- When the FTS query string is a literal or otherwise known at plan time, estimate
  selectivity from term dictionary document frequencies.
- For `AND` terms, use the rarest positive term as the first selectivity bound
  and refine with additional term frequencies where practical.
- For `OR` terms, estimate the union with capped summed document frequencies.
- Phrase predicates should start from the rarest phrase term and add a phrase
  verification cost.
- Prefix predicates should use prefix dictionary statistics when present; if
  unavailable, use a conservative high-cardinality estimate.
- When the query string is a parameter and not known at plan time, use a
  conservative heuristic and allow runtime fallback only if the executor can do
  so without changing result semantics.
- FTS scan cost must include postings reads, candidate row verification, row
  policy checks, scalar post-filters, rank computation, and top-k sorting.
- `ANALYZE` should refresh or validate FTS summary statistics if they are not
  already maintained transactionally. At minimum, metadata should expose document
  count, average document length, term count, and average/max postings length.
- If these costing rules require changes beyond ADR 0112's planner contract, add
  a follow-up optimizer ADR before implementation.

## 10. Binding And API Responsibilities

FTS should primarily be available through ordinary SQL. A dedicated search ABI is
not required for v1.

Common binding requirements:

- Prepared statements can bind FTS query strings.
- Ranking values read as normal floating-point columns.
- FTS DDL and DML work through existing execute/query APIs.
- Binding smoke tests cover create, insert, search, phrase search, ranking, and
  update/delete maintenance.
- Binding docs include at least one ranked-search example.
- Query contract metadata exposes `bm25(...)` as `FLOAT64`.
- Tooling metadata exposes full-text indexes and analyzer options.

Binding-specific expected additions:

| Binding | Required Work |
|---|---|
| C/C++ | Header docs/examples only unless a new metadata helper is needed; existing prepare/execute APIs should run FTS SQL. |
| Rust | Public examples and rustdoc for FTS DDL/query; optional typed helper only if it does not hide SQL semantics. |
| Python | DB-API smoke test; SQLAlchemy dialect docs/helper for `fulltext_match` and `bm25` if the dialect owns expression helpers. |
| .NET | ADO.NET/Dapper examples; EF Core function translations if EF provider is maintained in this branch. |
| Go | `database/sql` smoke test and sqlc documentation showing raw SQL with `$N` parameters. |
| Java | JDBC smoke test and result-type validation for rank columns. |
| Node | Native addon smoke test; Knex raw/query-builder guidance if available. |
| Dart/Flutter | FFI smoke test and mobile/browser-safe example guidance. |
| WASM/TypeScript | Worker/package smoke test once browser SQL parity includes the FTS parser and index code. |

Bindings must not implement tokenization or ranking themselves.

## 11. Documentation Updates

Documentation is part of the feature definition.

Required docs:

- `docs/user-guide/indexes.md`: full-text index DDL, options, limitations.
- `docs/user-guide/sql-reference.md`: `fulltext_match`, `bm25`,
  rebuild/verify syntax.
- `docs/user-guide/sql-feature-matrix.md`: FTS support and SQLite/DuckDB notes.
- `docs/user-guide/performance.md`: when to use B+Tree, trigram, and FTS indexes.
- `docs/user-guide/decentdb-vs-sqlite.md`: update FTS comparison once shipped.
- `docs/user-guide/decentdb-vs-duckdb.md`: update FTS comparison once shipped.
- `docs/api/sql-functions.md`: `bm25` and any FTS helper functions.
- `docs/api/configuration.md`: analyzer-related database config if exposed.
- `docs/api/rust-api.md`: Rust examples.
- `docs/api/c-cpp.md`: C ABI examples using prepared statements.
- Binding docs and package examples for maintained bindings.
- `docs/about/changelog.md`: Unreleased entry when implementation lands.

Do not claim FTS support in user-facing comparison pages until the feature is
implemented, tested, and documented.

## 12. Testing Strategy

### 12.1 Unit Tests

- Unicode tokenization, case folding, diacritic policy, stopwords, stemming.
- FTS query parser and error reporting.
- BM25 formula, term frequency, document length, and average length.
- Postings encoding/decoding, chunk boundaries, sorted row ids, positions.
- Prefix term dictionary lookup.
- Analyzer config persistence and compatibility checks.

Minimum unit coverage:

- at least 20 tokenizer/analyzer cases covering ASCII, Unicode letters,
  punctuation, case folding, diacritics policy, empty input, and long tokens
- at least 20 FTS query parser cases covering valid terms, phrases, `OR`,
  exclusion, prefix, escaping, and invalid syntax
- postings round-trip tests for empty, singleton, multi-entry, multi-chunk, and
  at least 100 randomized/proptest-style row-id sets
- BM25 golden tests for single-term, multi-term, zero-match, short-document, and
  long-document cases

### 12.2 SQL Integration Tests

- Create/drop/reopen FTS indexes.
- Single-term, multi-term, OR, exclusion, phrase, and prefix queries.
- `ORDER BY bm25('index') DESC LIMIT N`.
- Tied-score deterministic secondary ordering.
- Multi-column FTS indexes.
- Scalar filters combined with `fulltext_match`.
- Joins where the FTS base table is aliased.
- `INSERT`, `UPDATE`, `DELETE`, rollback, and transaction visibility.
- `ALTER INDEX ... REBUILD` and `ALTER INDEX ... VERIFY`.
- `EXPLAIN` includes FTS access paths.

### 12.3 Recovery And Correctness Tests

- Reopen after clean shutdown.
- Crash after base table write before/during/after FTS maintenance.
- Crash during rebuild.
- Corrupt or missing FTS index data detected by verify/Doctor.
- TDE database with FTS index remains encrypted and searchable with the key.
- TDE negative at-rest check: a database and WAL containing a known unique
  indexed term must not expose that term in plaintext bytes when opened with TDE
  enabled.
- Row policies filter FTS results.
- Column masks apply to projected columns without corrupting ranking.

Minimum recovery coverage:

- at least 8 crash/reopen scenarios covering insert, update, delete, rollback,
  commit before FTS persistence, commit after FTS persistence, rebuild start, and
  rebuild commit
- at least 3 stale/corrupt index scenarios covering missing term data, mismatched
  document stats, and invalid postings chunk structure

### 12.4 Binding Tests

Each maintained binding should have a focused smoke test:

1. open database
2. create table and FTS index
3. insert several rows
4. run parameterized ranked search
5. assert row order and rank type
6. update/delete rows
7. reopen and search again

Minimum binding coverage is one smoke test per maintained binding package that
has an existing local smoke-test harness. Tests may skip only under the
repository's normal missing-toolchain conventions.

### 12.5 Differential And Property Tests

- Compare token/query behavior against a frozen DecentDB expected corpus.
- Differential benchmark/query checks against SQLite FTS5 where semantics align.
- Property tests for postings invariants: sorted ids, no duplicates after update,
  valid positions, and no deleted row returned.

## 13. Benchmark Plan

Benchmarks must be added before claiming completion.

Required workloads:

- notes corpus: at least 10,000 short title/body documents
- messages corpus: at least 100,000 short rows with updates/deletes
- documentation corpus: at least 1,000 longer documents or 5 MiB of text with
  phrase and prefix queries
- scalar-filtered search: tenant/category/date filters plus FTS
- top-k search: `ORDER BY bm25(...) DESC LIMIT 10/20/100`
- ingest/update/delete maintenance cost
- cold reopen and verify/rebuild cost

Comparison engines:

- SQLite FTS5
- DuckDB FTS extension
- DecentDB trigram baseline where substring comparison is relevant
- full scan baseline

Initial target posture:

- FTS search should be materially faster than full scan on indexed corpora.
- Warm top-k ranked search should be in the same practical class as SQLite FTS5
  before marketing claims are updated.
- Maintenance cost must be visible in benchmark output rather than hidden under
  bulk-load-only scenarios.
- Release assets should include at least one FTS chart once the feature ships.

## 14. Implementation Phases

### Phase 0: Spec, ADRs, And Benchmark Fixtures

Status: this document plus ADR 0175 and ADR 0176 establish the initial contract.

Acceptance:

- Roadmap points to this spec and governing ADRs.
- Benchmark fixture schemas and corpora meeting section 13 minimum sizes are
  selected or generated deterministically.
- Parser/storage follow-up decisions are captured before code begins.

### Phase 1: Analyzer, Query Parser, And Ranking Core

Scope:

- portable tokenizer/analyzer module
- FTS query parser
- BM25 scoring unit module
- no persistent index yet

Phase 1 produces unit-testable modules and may include an in-memory harness for
developer validation. It does not need to expose SQL DDL/query execution, catalog
metadata, or persistent storage.

Acceptance:

- unit tests for analyzer/query/ranking pass
- analyzer config structs can serialize/deserialize deterministically
- FTS-specific error prefixes from section 5.5 are covered
- prefix option parsing follows section 6 exactly
- no new native dependencies

### Phase 2: Catalog And Persistent FTS Index Storage

Scope:

- `IndexKind::FullText`
- typed `IndexSchema` full-text options/config metadata
- analyzer metadata in catalog/introspection/tooling metadata
- term dictionary, postings, document stats, and index metadata storage
- rebuild from base table
- database format-version bump and `decentdb-migrate` read-only parser update

Acceptance:

- create/drop/reopen FTS indexes
- rebuild produces deterministic postings and stats
- DDL rejects unsupported access-method aliases and invalid `WITH` options
- dump/introspection round-trips `USING fulltext` and normalized analyzer options
- format-version bump is implemented and migration parser coverage exists

### Phase 3: Transactional Maintenance And Recovery

Scope:

- insert/update/delete maintenance
- transaction-local FTS delta overlay
- rollback and transaction visibility
- crash/reopen checks
- verify and stale-index detection

Acceptance:

- no committed row is silently missing from FTS results after recovery
- same-transaction FTS reads see the transaction's own writes
- updates that preserve row id do not match obsolete terms from older document
  generations
- verify reports corrupt/stale index data
- rebuild repairs derived index state
- rebuild concurrency follows the synchronous atomic-swap rules in section 8

### Phase 4: Planner, Executor, Phrase/Prefix, And BM25 SQL

Scope:

- `fulltext_match('index', query)`
- `bm25('index')`
- phrase and prefix execution
- scalar filter and join integration
- `EXPLAIN`
- top-k optimization

Acceptance:

- ranked search integration tests pass
- phrase search uses positions
- planner chooses FTS access path where applicable
- rank is returned as ordinary `FLOAT64`
- invalid `bm25` scope and invalid FTS query syntax return the section 5.5 error
  prefixes

### Phase 5: Binding, Metadata, And Documentation Completion

Scope:

- binding smoke tests
- query contract metadata
- tooling metadata
- user/API docs
- changelog

Acceptance:

- maintained binding smoke tests pass or are skipped only for missing local
  toolchains under existing repo conventions
- docs show SQL examples and limitations
- no binding implements tokenization/ranking independently

### Phase 6: Benchmarks, Release Assets, And Hardening

Scope:

- native and polyglot benchmark coverage
- SQLite FTS5 and DuckDB FTS comparisons
- Doctor findings for stale/corrupt FTS indexes
- release chart integration

Acceptance:

- benchmark commands are documented
- FTS performance appears in machine-readable benchmark output
- release docs do not overclaim beyond measured results

## 15. Definition Of Done

The roadmap item can move from `TODO` to delivered context only when:

- ADR 0175 and ADR 0176 are implemented or superseded by accepted follow-up
  ADRs.
- FTS DDL, query, ranking, phrase, and prefix behavior are implemented.
- Incremental maintenance works through normal transactions.
- Reopen, crash recovery, verify, and rebuild semantics are tested.
- SQL integration tests and relevant binding smoke tests pass.
- Documentation and changelog are updated.
- Benchmarks compare DecentDB FTS against SQLite FTS5 and DuckDB FTS.
- `FUTURE_WINS.md` is updated to remove the active item and add concise
  delivered context.

## 16. Risks And Required Discipline

- **Storage bloat:** positions and term frequencies can grow quickly. Chunked
  postings and benchmarked maintenance cost are mandatory.
- **Analyzer compatibility:** changing tokenization after release can make
  persisted indexes invalid. Analyzer identity/version must be persisted.
- **Ranking ambiguity:** BM25 ordering can tie. Tests need stable secondary
  ordering when exact rows are asserted.
- **Security leakage:** FTS indexes contain searchable terms from indexed
  columns. TDE protects local bytes at rest, but masks do not redact index terms.
  Docs must explain that applications should not index columns whose terms must
  not be searchable.
- **Binding drift:** bindings should validate the SQL surface but not fork FTS
  semantics into language code.
- **Portability:** all mandatory tokenizer/ranking/index code must work on
  native, WASM, and mobile targets.

## 17. Files Likely To Change During Implementation

Likely core files/modules:

- `crates/decentdb/src/catalog/schema.rs`
- `crates/decentdb/src/sql/`
- `crates/decentdb/src/exec/ddl.rs`
- `crates/decentdb/src/exec/dml.rs`
- `crates/decentdb/src/exec/mod.rs`
- `crates/decentdb/src/planner/`
- `crates/decentdb/src/search/`
- `crates/decentdb/src/db.rs`
- `crates/decentdb/src/tooling.rs`
- `crates/decentdb/src/c_api.rs` only if metadata helpers or ABI constants
  change
- `include/decentdb.h` only if C ABI changes
- `crates/decentdb-benchmark/`
- maintained binding smoke tests under `tests/bindings/` and binding packages

Likely docs:

- files listed in section 11
- `docs/about/changelog.md`
- `design/FUTURE_WINS.md`
- ADR README if follow-up ADRs are added
