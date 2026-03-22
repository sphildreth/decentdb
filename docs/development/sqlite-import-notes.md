# SQLite import notes

This page captures practical guidance for importing SQLite data into DecentDB and for running benchmarks that compare the two.

## File size comparisons (what to measure)

When comparing file sizes, make sure you’re measuring *equivalent end states*:

- **Checkpoint + vacuum before measuring**: WAL and freelist pages can make a database look larger than its steady-state size.
- **Warm-cache vs cold-cache**: page-cache effects can dominate runtime measurements.
- **Query execution vs result formatting**: CLI output formatting (especially JSON) can dwarf execution time for large result sets.

## Common sources of “bloat” during imports

These are expected behaviors that can temporarily inflate file size during ingestion:

- **WAL growth**: if you bulk-load a lot of data and don’t checkpoint, you’ll see the `.wal` file grow.
- **B+Tree fragmentation during random inserts**: if rows are inserted in an order that causes many splits, you may see more pages allocated than a fully-compacted layout.
- **Index rebuild strategies**: building indexes incrementally during ingest is usually more expensive than bulk-load with index rebuild.

## Recommended import workflow

For large imports, prefer a bulk-load path that minimizes index maintenance work during ingest and rebuilds once.

- Use a single transaction (or large batches) rather than per-row commits.
- If your workflow allows it, **disable index maintenance during ingest** and rebuild indexes once at the end.
- After import, run **checkpoint** and **vacuum** before taking size measurements.

## Benchmarking guidance

To get stable performance measurements:

- Prefer a long-lived session (e.g., a REPL) rather than repeatedly opening/closing the database.
- Measure execution time separately from result rendering.
- Run queries in loops and report median/p95 (single runs are noisy).

## LIKE performance and trigram indexes

For substring searches such as `LIKE '%pattern%'`, a trigram index is typically required for good performance.

- Create a trigram index on the target column when you rely on substring LIKE.
- Patterns shorter than the trigram length may fall back to scans by design.

## Troubleshooting checklist

If your imported DB looks unexpectedly large or slow:

- Verify you checkpointed and vacuumed.
- List indexes and confirm you don’t have redundant indexes on the same `(table, column)`.
- Ensure the right index kind exists (B+Tree for equality, trigram for substring LIKE).
- Confirm you are measuring query execution rather than output formatting.
