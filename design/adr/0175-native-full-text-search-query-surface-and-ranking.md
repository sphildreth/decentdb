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

SELECT id, title, bm25(idx_docs_search) AS rank
FROM docs
WHERE idx_docs_search MATCH $1
ORDER BY rank DESC
LIMIT 20;
```

`MATCH` is a boolean predicate over a named full-text index. `bm25(index_name)`
returns a `FLOAT64` score for rows matched by the same query block. Higher BM25
scores sort as better matches.

The v1 FTS query language will include bare terms, quoted phrases, explicit
`OR`, unary exclusion, and suffix prefix terms when prefix search is enabled.
Whitespace-separated terms are `AND` by default.

Mandatory tokenizer, query parser, and ranking behavior must be implemented in
portable Rust. Mandatory FTS behavior must not depend on a native C/C++ search
library or on language bindings.

### Rationale

Full-text search is a high-impact adoption blocker because SQLite FTS5 is part
of the expected embedded database feature set. DecentDB's existing trigram index
solves substring search, but it does not provide lexical tokenization, phrase
queries, term frequency, document length statistics, or BM25 ranking.

A native index mode keeps FTS aligned with DecentDB's existing SQL and planner
architecture:

- `CREATE INDEX ... USING fulltext` matches the existing trigram index shape.
- `MATCH` can be planned as an index access path.
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
- **Only a table-valued function such as `fts_search(index, query)`.** Rejected
  as the primary surface. It is binding-friendly but awkward for scalar filters,
  joins, and optimizer integration. A helper may be added later if needed.
- **Reuse trigram indexes for ranking.** Rejected. Trigram postings do not carry
  the token, frequency, document length, and position statistics required for
  BM25 and phrase search.
- **External search library as the core engine.** Rejected for mandatory v1
  behavior because portability to WASM/mobile and long-term format control are
  core requirements.
- **Binding-level FTS APIs.** Rejected. Bindings should validate and document
  FTS, but they must not own query parsing, tokenization, or ranking semantics.

### Trade-offs

- The `index_name MATCH query` syntax introduces a DecentDB-specific SQL form,
  but it keeps the feature concise and avoids virtual table mechanics.
- `bm25(index_name)` depends on a matching FTS predicate in the same query block.
  This requires semantic validation in the planner/executor.
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

