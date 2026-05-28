# Native Full-Text Search Query Surface And Ranking
**Date:** 2026-05-26
**Status:** Accepted

### Decision

DecentDB will implement full-text search as a native index mode with a
planner-visible SQL surface, not as a virtual-table-only feature or a
language-binding search helper.

The accepted user-facing direction is:

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

`fulltext_match(index_name_text, query_expression)` is a boolean predicate over
a named full-text index. `index_name_text` is a string literal in v1.
`bm25(index_name_text)` returns a `FLOAT64` score for rows matched by the same
query block. Higher BM25 scores sort as better matches.

`fulltext_match(...)` is supported in the `WHERE` clause of a `SELECT` query
block in v1, including CTE, subquery, and `EXISTS` query blocks. `bm25(...)` is
valid only in the `SELECT` list and `ORDER BY` of the same query block that owns
the matching `fulltext_match(...)` predicate.

DecentDB will not patch `libpg_query` for v1 FTS. `index_name MATCH
query_expression` may be added later only through a follow-up ADR. If added, it
must be a contextual FTS predicate form, not a globally reserved keyword that
breaks ordinary identifiers.

The v1 FTS query language will include bare terms, quoted phrases, explicit
`OR`, unary exclusion, and suffix prefix terms when prefix search is enabled.
Whitespace-separated terms are `AND` by default.

Mandatory tokenizer, query parser, and ranking behavior must be implemented in
portable Rust. Mandatory FTS behavior must not depend on a native C/C++ search
library or on language bindings.

BM25 scoring uses the non-negative IDF variant:

```text
idf(t) = ln(1 + (N - df(t) + 0.5) / (df(t) + 0.5))
```

where `N` counts non-empty indexed documents. This is chosen deliberately to
avoid negative scores for high-frequency terms and must be documented in
benchmarks when comparing rankings with SQLite FTS5 or DuckDB FTS.

### Rationale

Full-text search is a high-impact adoption blocker because SQLite FTS5 is part
of the expected embedded database feature set. DecentDB's existing trigram index
solves substring search, but it does not provide lexical tokenization, phrase
queries, term frequency, document length statistics, or BM25 ranking.

A native index mode keeps FTS aligned with DecentDB's existing SQL and planner
architecture:

- `CREATE INDEX ... USING fulltext` matches the existing trigram index shape.
- `fulltext_match(...)` can be planned as an index access path while staying
  compatible with the existing PostgreSQL parser.
- `bm25(...)` is an ordinary scalar projection/order expression.
- Prepared statements and all maintained bindings can use the same SQL path.
- Future tooling metadata can describe FTS indexes without modeling a virtual
  table subsystem.

The SQL surface is intentionally not a SQLite FTS5 clone. DecentDB should expose
a simple embedded-database search model without inheriting virtual-table
quirks.

### Alternatives Considered

- **SQLite-style virtual tables.** Rejected for v1. It would create a separate
  table-like subsystem, complicate catalog semantics, and make FTS feel bolted
  on rather than planner-native.
- **`index_name MATCH query` in v1.** Rejected for v1 because DecentDB currently
  normalizes SQL through `libpg_query`, and PostgreSQL does not parse this
  MySQL/SQLite-style expression form. Patching the C parser is too broad for the
  first FTS implementation. A parser sugar layer may be reconsidered later.
- **Only a table-valued function such as `fts_search(index, query)`.** Rejected
  as the primary v1 surface. It is binding-friendly but awkward for scalar
  filters, joins, and optimizer integration. A helper may be added later if
  needed.
- **Reuse trigram indexes for ranking.** Rejected. Trigram postings do not carry
  the token, frequency, document length, and position statistics required for
  BM25 and phrase search.
- **External search library as the core engine.** Rejected for mandatory v1
  behavior because portability to WASM/mobile and long-term format control are
  core requirements.
- **Binding-level FTS APIs.** Rejected. Bindings should validate and document
  FTS, but they must not own query parsing, tokenization, or ranking semantics.

### Trade-offs

- `fulltext_match('index', query)` is more verbose than `index MATCH query`, but
  it avoids parser patches and remains easy to bind from every language.
- `bm25('index')` depends on a matching FTS predicate in the same query block.
  This requires semantic validation in the planner/executor. V1 permits it only
  in the `SELECT` list and `ORDER BY`.
- The parser-compatible function surface is less concise for hand-written SQL,
  but it keeps FTS available to all bindings without introducing a custom parser
  fork.
- Default `AND` behavior is predictable and SQLite-adjacent, but applications
  that want broad recall must use explicit `OR` or a future query option.
- Multi-column ranking as one concatenated document is simpler than BM25F, but
  it does not provide first-class field weighting in v1.

### References

- `design/WIN_FULL_TEXT_SEARCH_BM25_SPEC.md`
- `design/FUTURE_WINS.md`
- `design/adr/0176-full-text-search-storage-durability-and-binding-contract.md`
- SQLite FTS5 documentation
- DuckDB full-text search documentation
