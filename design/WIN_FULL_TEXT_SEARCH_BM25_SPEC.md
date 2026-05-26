# Full-Text Search With BM25 Ranking

**Date:** 2026-05-26
**Status:** Active implementation spec seed
**Future Version:** vNext
**Roadmap:** [`FUTURE_WINS.md`](FUTURE_WINS.md)
**Document Type:** Implementation SPEC
**Audience:** Core engine developers, SQL planner/executor maintainers, storage
maintainers, binding maintainers, documentation authors, benchmark maintainers,
coding agents

**Governing ADRs:**

- [`adr/0175-native-full-text-search-query-surface-and-ranking.md`](adr/0175-native-full-text-search-query-surface-and-ranking.md)
- [`adr/0176-full-text-search-storage-durability-and-binding-contract.md`](adr/0176-full-text-search-storage-durability-and-binding-contract.md)

**Related inputs:**

- [`FUTURE_WINS.md`](FUTURE_WINS.md)
- [`docs/design/spec.md`](../docs/design/spec.md)
- [`adr/0007-trigram-postings-storage-strategy.md`](adr/0007-trigram-postings-storage-strategy.md)
- [`adr/0008-trigram-pattern-length-guardrails.md`](adr/0008-trigram-pattern-length-guardrails.md)
- [`adr/0052-trigram-durability.md`](adr/0052-trigram-durability.md)
- [`adr/0063-trigram-postings-paging-format.md`](adr/0063-trigram-postings-paging-format.md)
- [`adr/0112-cost-based-optimizer-with-stats.md`](adr/0112-cost-based-optimizer-with-stats.md)
- [`STABLE_TOOLING_METADATA_CONTRACT.md`](STABLE_TOOLING_METADATA_CONTRACT.md)

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

SELECT id, title, bm25(idx_docs_search) AS rank
FROM docs
WHERE idx_docs_search MATCH $1
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

- Indexed expressions are out of scope for v1. FTS keys must be plain `TEXT`
  columns.
- Multi-column FTS indexes are supported by treating selected columns as fields
  in one document.
- `UNIQUE`, `INCLUDE`, and partial FTS indexes are out of scope for v1.
- Analyzer options are persisted in catalog metadata and cannot silently change
  after index creation.
- If analyzer changes are later supported, they require `ALTER INDEX ... REBUILD`
  semantics.

### 5.2 Query Predicate

The primary predicate is:

```sql
WHERE index_name MATCH query_expression
```

Rules:

- `index_name` must resolve to a full-text index visible to the query.
- The FTS index's base table must be present in the query scope.
- `query_expression` is a normal SQL expression that evaluates to `TEXT`.
- Prepared statement parameters are supported.
- `MATCH` returns a boolean predicate and can be combined with scalar filters.

Example:

```sql
SELECT id, title
FROM docs
WHERE idx_docs_search MATCH $1
  AND tenant_id = $2
ORDER BY bm25(idx_docs_search) DESC
LIMIT 10;
```

### 5.3 Ranking

`bm25(index_name)` returns `FLOAT64`.

Rules:

- Higher scores sort as better matches.
- Calling `bm25(index_name)` is valid only when the same query block contains a
  compatible `index_name MATCH ...` predicate.
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

where `N` is document count, `df` is document frequency, `tf` is term frequency,
`dl` is indexed-token count for the document, and `avgdl` is average document
length for the index.

### 5.4 Query Language

The initial FTS query language should support:

- bare terms: `database`
- quoted phrases: `"embedded database"`
- whitespace-separated terms as `AND`
- explicit `OR`
- unary exclusion: `-draft`
- suffix prefix terms when enabled: `dece*`
- escaping quotes and operator characters

Out of scope for v1:

- `NEAR`
- nested proximity expressions
- regex
- fuzzy edit-distance syntax
- user-defined query parsers

Invalid FTS query syntax must produce a normal SQL error with enough context for
binding tests and application diagnostics.

## 6. Analyzer Policy

Analyzer configuration is part of the persisted index definition.

Minimum analyzer policy:

| Option | v1 Requirement |
|---|---|
| `tokenizer` | `unicode` tokenizer implemented in Rust and portable to WASM/mobile |
| `language` | `simple` required; `english` may be added only through a pure-Rust portable implementation |
| `stopwords` | `none` required; built-in language list optional but must be persisted by name/version |
| `stemming` | `none` required; language stemming optional but must be deterministic and versioned |
| `prefix` | disabled by default; explicit minimum prefix lengths such as `'2,3'` |
| case folding | deterministic Unicode-aware lowercasing policy |
| diacritics | explicit preserve/remove policy before index creation |

Analyzer changes are compatibility-sensitive. Any implementation must persist a
stable analyzer identifier and version so a future DecentDB release can detect
when an FTS index needs rebuild because tokenization behavior changed.

## 7. Storage Model

FTS index storage is engine-owned derived secondary index data.

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

The implementation must decide whether the catalog/index metadata change bumps
the on-disk format version. If it does, ADR 0131 applies and the
`decentdb-migrate` read-only migration parser must be updated.

## 8. Transaction, Recovery, And Rebuild Semantics

Required transaction behavior:

- Inserted rows become searchable at the same transaction visibility boundary as
  the base table rows.
- Updated indexed columns remove old tokens and add new tokens.
- Deleted rows are removed from search results.
- Rollback reverts all FTS maintenance.
- Same-transaction reads follow DecentDB's normal read-your-writes rules.
- Row policies filter FTS candidates before rows are returned.

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

## 9. Planner And Executor Integration

The planner must recognize `index_name MATCH expr` as an FTS access path.

Required planner behavior:

- Use FTS candidate retrieval before evaluating expensive row predicates.
- Combine FTS with scalar filters, row policies, joins, `ORDER BY`, and `LIMIT`.
- Recognize `ORDER BY bm25(index_name) DESC LIMIT N` as a top-k search shape.
- Preserve deterministic results for tied scores by requiring a stable secondary
  ordering in tests where exact order matters.
- Include FTS access in `EXPLAIN`.
- Surface planner stats through existing or extended metadata where useful.

Required executor behavior:

- Phrase search verifies token positions, not string contains.
- Prefix search consults the term dictionary and applies configured prefix
  limits.
- `bm25(index_name)` uses the matched query terms and persisted statistics.
- FTS results remain ordinary rows and ordinary scalar values to the rest of the
  execution pipeline.

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
| Python | DB-API smoke test; SQLAlchemy dialect docs/helper for `MATCH` and `bm25` if the dialect owns expression helpers. |
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
- `docs/user-guide/sql-reference.md`: `MATCH`, `bm25`, rebuild/verify syntax.
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

### 12.2 SQL Integration Tests

- Create/drop/reopen FTS indexes.
- Single-term, multi-term, OR, exclusion, phrase, and prefix queries.
- `ORDER BY bm25(index) DESC LIMIT N`.
- Tied-score deterministic secondary ordering.
- Multi-column FTS indexes.
- Scalar filters combined with `MATCH`.
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
- Row policies filter FTS results.
- Column masks apply to projected columns without corrupting ranking.

### 12.4 Binding Tests

Each maintained binding should have a focused smoke test:

1. open database
2. create table and FTS index
3. insert several rows
4. run parameterized ranked search
5. assert row order and rank type
6. update/delete rows
7. reopen and search again

### 12.5 Differential And Property Tests

- Compare token/query behavior against a frozen DecentDB expected corpus.
- Differential benchmark/query checks against SQLite FTS5 where semantics align.
- Property tests for postings invariants: sorted ids, no duplicates after update,
  valid positions, and no deleted row returned.

## 13. Benchmark Plan

Benchmarks must be added before claiming completion.

Required workloads:

- notes corpus: short title/body documents
- messages corpus: many short rows with updates/deletes
- documentation corpus: longer documents with phrase and prefix queries
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
- Benchmark fixture schemas and corpora are selected.
- Parser/storage follow-up decisions are captured before code begins.

### Phase 1: Analyzer, Query Parser, And Ranking Core

Scope:

- portable tokenizer/analyzer module
- FTS query parser
- BM25 scoring unit module
- no persistent index yet

Acceptance:

- unit tests for analyzer/query/ranking pass
- analyzer config structs can serialize/deserialize deterministically
- no new native dependencies

### Phase 2: Catalog And Persistent FTS Index Storage

Scope:

- `IndexKind::FullText`
- analyzer metadata in catalog/introspection
- term dictionary, postings, document stats, and index metadata storage
- rebuild from base table

Acceptance:

- create/drop/reopen FTS indexes
- rebuild produces deterministic postings and stats
- format-version decision documented and implemented if needed

### Phase 3: Transactional Maintenance And Recovery

Scope:

- insert/update/delete maintenance
- rollback and transaction visibility
- crash/reopen checks
- verify and stale-index detection

Acceptance:

- no committed row is silently missing from FTS results after recovery
- verify reports corrupt/stale index data
- rebuild repairs derived index state

### Phase 4: Planner, Executor, Phrase/Prefix, And BM25 SQL

Scope:

- `index MATCH query`
- `bm25(index)`
- phrase and prefix execution
- scalar filter and join integration
- `EXPLAIN`
- top-k optimization

Acceptance:

- ranked search integration tests pass
- phrase search uses positions
- planner chooses FTS access path where applicable
- rank is returned as ordinary `FLOAT64`

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

- `crates/decentdb/src/sql/`
- `crates/decentdb/src/exec/ddl.rs`
- `crates/decentdb/src/exec/dml.rs`
- `crates/decentdb/src/exec/mod.rs`
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

