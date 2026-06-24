# ADR 0197: Fulltext Runtime Index Delta Overlays

**Date:** 2026-06-24
**Status:** Proposed

## Context

The fresh full benchmark run in `.tmp/perf-validate/20260624-171109` shows the
largest remaining SQLite wins in Showdown GLM52 mutation paths:

| Benchmark target | SQLite | DecentDB | Gap |
|---|---:|---:|---:|
| Showdown GLM52 native defaults `Bulk DELETE` | 0.002926s | 0.487579s | 166.629x |
| Showdown GLM52 scale `Bulk DELETE` | 0.002769s | 0.152178s | 54.964x |
| Showdown GLM52 scale `UPDATE RETURNING` | 0.002675s | 0.211947s | 79.247x |

Profiling of resident bulk delete found about 132ms spent updating
`idx_movies_search_ft` for a 500-row delete. The structural problem is not the
row-id range lookup. Runtime indexes are held behind shared ownership, so the
first small mutation to a large fulltext index can clone the entire runtime
fulltext structure before applying a tiny delete. That cost is paid before the
remaining persistence work covered by ADR 0196.

ADR 0175 defines fulltext search as a planner-visible native index. ADR 0176
already requires FTS maintenance to preserve read-your-writes semantics and
allows transaction-local delta overlays. This ADR narrows that accepted storage
direction into a concrete runtime-index representation needed for the current
performance gaps.

This ADR does not authorize weakening WAL durability, changing benchmark lanes,
or changing the public SQL fulltext surface.

## Decision

Implement fulltext runtime indexes as an immutable base plus a mutable delta
overlay instead of cloning the full runtime index on first write.

The initial runtime shape should be:

- `FullTextIndexBase`: immutable, reference-counted term dictionary, postings,
  document statistics, analyzer metadata, and base generation.
- `FullTextIndexDelta`: mutable transaction/runtime overlay containing deleted
  row IDs, replacement document generations, inserted/updated postings,
  inserted/updated document statistics, and logical statistic deltas.
- `RuntimeIndex::FullText`: a wrapper that owns the base and overlay and can be
  cheaply cloned for transaction snapshots without copying base postings.

Fulltext queries must merge base and overlay state:

- base postings are filtered by overlay tombstones and document generations;
- overlay postings are unioned into candidate streams;
- BM25 document count, average length, document length, and term/document
  frequency use the logical merged view;
- stale or unverifiable index state still follows the existing stale-index or
  rebuild path.

Mutation paths update only the overlay for ordinary inserts, deletes, and
indexed-column updates. They must not clone the entire fulltext base for small
row sets.

Overlay compaction is an internal implementation detail. It may merge overlay
state into a new immutable base when configurable thresholds are crossed, such
as overlay row count, overlay postings bytes, or overlay/base size ratio.
Compaction must preserve transaction visibility and must not change SQL result
ordering except for already-accepted equal-rank tie behavior.

Rollback discards the transaction's overlay changes through the existing
runtime snapshot/transaction rollback mechanism. Commit makes the overlay part
of the connection-visible runtime state and persists derived index state through
the ordinary index maintenance path. The first implementation phase must avoid
new persistent format keys. If efficient persistence requires new FTS key
families, page layouts, manifest versions, or WAL records, that work requires a
separate format/WAL ADR and the ADR 0131 migration obligations.

## Alternatives Considered

1. **Keep cloning fulltext runtime indexes.** Rejected. It turns small DML into
   work proportional to full index size and is the measured source of a
   top-five benchmark gap.
2. **Mark fulltext indexes stale after every write and rebuild lazily.**
   Rejected. It weakens the read-your-writes and committed-query behavior
   required by ADR 0176.
3. **Eagerly purge every old posting from the base index.** Rejected for the
   hot path. It creates random-write and clone amplification. Compaction or
   rebuild can purge obsolete postings later.
4. **Persist a new granular FTS layout immediately.** Rejected for this ADR.
   It may be worthwhile later, but it is a file-format decision.
5. **Drop the fulltext index from the benchmark schema.** Rejected. That would
   change the benchmark lane instead of fixing the engine.

## Consequences

### Positive

- Small deletes and updates no longer pay full fulltext-index clone cost.
- The runtime representation aligns with ADR 0176's read-your-writes overlay
  direction.
- Query correctness is preserved by treating fulltext data as derived state
  merged with current table visibility.
- The first phase can be implemented without weakening durability or changing
  the public SQL/C ABI contract.

### Negative

- Fulltext query execution must merge base and overlay streams.
- BM25 scoring must account for overlay statistic deltas.
- Compaction thresholds add memory-accounting and test complexity.
- A later persistent granular FTS layout may still be needed for checkpoint and
  reopen performance.

## Implementation Phases

1. Add microbenchmarks for fulltext-index delete/update maintenance that split
   lookup, table mutation, fulltext maintenance, ordinary index maintenance, and
   persist/commit.
2. Refactor runtime fulltext storage into base plus overlay without changing the
   SQL surface.
3. Update insert, delete, and indexed-column update paths to write overlay
   deltas instead of cloning the base.
4. Update fulltext match and BM25 execution to merge base plus overlay.
5. Add overlay compaction and rebuild fallback after correctness and hot-path
   wins are proven.

## Validation Requirements

Correctness:

- fulltext delete removes candidates immediately in the same transaction;
- fulltext update cannot match old terms for the same row ID;
- rollback restores pre-transaction fulltext visibility;
- commit and reopen preserve fulltext query correctness or mark/rebuild stale
  derived state according to existing rules;
- BM25 values are computed from the logical merged index state;
- branch/snapshot visibility and stale-index behavior remain correct.

Performance:

- fulltext maintenance for the 500-row Showdown delete must be proportional to
  touched documents, not full index size;
- fresh `python scripts/benchmark_runner.py --profile full` output must move
  these rows under `DecentDB better at:`:
  - Showdown GLM52 native defaults `Bulk DELETE`;
  - Showdown GLM52 scale `Bulk DELETE`;
- if `UPDATE RETURNING` still loses after this ADR, its remaining cost must be
  attributed to ADR 0198 or ADR 0196 with phase timing.

Required checks before implementation is complete:

```bash
cargo fmt --check
cargo check -p decentdb
cargo test -p decentdb --lib
cargo clippy -p decentdb --all-targets --all-features -- -D warnings
python scripts/do-pre-commit-checks.py --mode fast
python scripts/benchmark_runner.py --profile full
```

## References

- `design/adr/0175-native-full-text-search-query-surface-and-ranking.md`
- `design/adr/0176-full-text-search-storage-durability-and-binding-contract.md`
- `design/adr/0184-default-fast-planner-and-runtime-contract.md`
- `design/adr/0196-persisted-dml-and-cascade-delete-performance.md`
- `.tmp/perf-validate/20260624-171109`
