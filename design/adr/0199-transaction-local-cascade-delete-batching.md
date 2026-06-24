# ADR 0199: Transaction-Local Cascade Delete Batching

**Date:** 2026-06-24
**Status:** Proposed

## Context

The fresh full benchmark run in `.tmp/perf-validate/20260624-171109` shows
MovieDB cascade delete still losing badly:

| Benchmark target | SQLite | DecentDB | Gap |
|---|---:|---:|---:|
| MovieDB scratch `Cascade delete batch` | 0.118674s | 2.364792s | 19.927x |

The benchmark deletes 10 `Movies` rows inside one explicit transaction through
10 ordinary `execute` calls:

```sql
DELETE FROM Movies WHERE Id = CAST(? AS UUID)
```

The affected child tables include `Roles`, `Reviews`, `MovieTags`, and
`Watchlist`, all with `ON DELETE CASCADE`. Child row discovery can use explicit
or auto-created FK indexes, but the current execution shape applies cascade
effects statement-by-statement and can repeatedly compact or rewrite large
resident child tables.

ADR 0196 accepts batched cascade delete as part of the performance path. This
ADR narrows the required semantics: batching must preserve statement visibility,
not merely defer all child work until commit.

This ADR does not authorize changing FK semantics, rowcount semantics,
benchmark SQL, or durability settings.

## Decision

Implement transaction-local cascade row-change batching through a logical
row-change delta layer.

For eligible cascade deletes, each statement must still:

- find parent rows through the ordinary prepared delete path;
- run restrict/no-action checks before destructive effects;
- discover child row IDs through the same FK matching semantics as the fallback
  path;
- make parent and child deletions visible to subsequent statements in the same
  transaction;
- return the same parent affected-row count as the current path.

The optimization is in physical application, not logical visibility. Child row
deletions are recorded into a transaction-local table delta as tombstones keyed
by table and row ID. Reads, later FK checks, index lookups, and subsequent DML
must consult the delta so same-transaction semantics match the existing path.
Physical resident compaction, persisted-paged rewrites, and index maintenance
may then be batched once per affected table at transaction flush/commit or at a
required semantic barrier.

The initial optimized scope should exclude:

- target or child DELETE triggers unless trigger ordering is proven equivalent;
- unsupported sync/reactive mutation recording;
- stale or missing FK indexes that require full child scans;
- branch/snapshot states whose visibility cannot yet merge the delta layer;
- savepoint behavior until rollback of per-savepoint row-change deltas is
  implemented.

Unsupported cases must fall back to the existing statement-by-statement cascade
path.

## Alternatives Considered

1. **Require callers or benchmarks to use a batch API.** Rejected. The current
   gap occurs through ordinary repeated `execute` calls inside a transaction.
2. **Defer cascade effects until commit.** Rejected. It breaks
   same-transaction statement visibility.
3. **Only add more FK indexes.** Rejected as insufficient. Child lookup is not
   the only cost; repeated physical compaction and maintenance dominate.
4. **Switch `embedded_fast()` back to paged row storage.** Rejected. ADR 0195
   deliberately selected resident storage for that profile's hot-read/write
   trade-off.
5. **Apply one `retain_rows` pass per statement.** Rejected as insufficient for
   the benchmark shape. The work must be amortized across the transaction.

## Consequences

### Positive

- Cascade deletes can become proportional to affected child rows plus one
  physical flush per child table, rather than repeated full child-table passes.
- SQL visibility and rowcount semantics remain intact.
- The same row-change delta layer can support ADR 0198 vectorized DML and
  ADR 0196 persisted DML.

### Negative

- Reads, FK checks, and indexes need a merged view over base state plus
  transaction-local tombstones.
- Savepoint and rollback accounting become more complex.
- Trigger and sync correctness will limit initial fast-path eligibility.
- The implementation must avoid duplicating row-change semantics separately for
  cascade, ordinary DELETE, and vectorized DML.

## Implementation Phases

1. Add a MovieDB cascade microbenchmark that splits parent lookup, child lookup,
   child logical delete, physical child flush, index maintenance, persist, and
   commit.
2. Add a transaction-local row-change delta abstraction shared by cascade and
   future vectorized DML.
3. Teach row-source reads and FK/index lookups to apply tombstone deltas.
4. Use the delta for eligible cascade child deletions and flush once per child
   table at transaction end or barrier.
5. Add savepoint support and broaden eligibility only after the core path is
   correct.

## Validation Requirements

Correctness:

- repeated parent deletes remove all matching child rows immediately within the
  transaction;
- rollback restores parent and child rows;
- duplicate and missing parent IDs produce the same affected-row counts as the
  fallback path;
- restrict/no-action failures happen before destructive effects become visible;
- child FK indexes remain fresh after commit;
- trigger, sync, branch, and savepoint cases either match fallback semantics or
  fall back explicitly.

Performance:

- fresh `python scripts/benchmark_runner.py --profile full` output must move
  MovieDB scratch `Cascade delete batch` under `DecentDB better at:`;
- the implementation report must include phase timing proving repeated physical
  child-table compaction has been removed from the hot loop.

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

- `design/adr/0184-default-fast-planner-and-runtime-contract.md`
- `design/adr/0195-embedded-fast-profile-and-resident-read-fast-path.md`
- `design/adr/0196-persisted-dml-and-cascade-delete-performance.md`
- `.tmp/perf-validate/20260624-171109`
