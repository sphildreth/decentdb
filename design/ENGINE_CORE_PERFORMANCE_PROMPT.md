# Engine Core Performance Improvement Prompt

Use this prompt with a coding agent working in the DecentDB repository.

## Objective

Improve DecentDB engine-core performance and memory efficiency without weakening
durability, correctness, or public API/ABI stability.

Primary goals:

- Restore raw Rust prepared insert throughput toward the historical high-water
  mark of `> 1,000,000 rows/sec` where the workload and durability contract are
  comparable.
- Improve all benchmark KPIs tracked by `crates/decentdb-benchmark`, with no
  meaningful regressions in unrelated metrics.
- Reduce peak RSS for multi-million-row insert and query workloads. Peak RSS
  should be no worse than SQLite on the same benchmark and should move toward
  the existing engine-memory target of `< 800 MB` for a 5M-row load where that
  target applies.
- Keep disk footprint competitive with SQLite and do not trade lower RSS for
  unacceptable storage amplification.

Do not trust numbers embedded in this prompt as the current truth. Run fresh
baselines first and treat the new artifacts as authoritative.

## Current Evidence To Re-check

Recent raw Rust baseline artifacts exist at:

- `/tmp/tmp-opus47-decentdb-net-tests/rust-baseline/results/rust-baseline-full.json`
- `/tmp/tmp-opus47-decentdb-net-tests/rust-baseline/results/comparison-full.md`

Those artifacts showed, for one full run:

- `seed_artists`: about `668k rows/sec` in JSON, with the comparison report also
  showing a prior `792k rows/sec` run.
- `seed_albums`: about `626k rows/sec` in JSON, with the comparison report also
  showing a prior `787k rows/sec` run.
- `seed_songs`: about `524k rows/sec` in JSON, with the comparison report also
  showing a prior `672k rows/sec` run.
- Peak RSS: `2.31 GB` for a final DB size of about `145 MB`.
- RSS climbed heavily during aggregate/top-N/view read phases after seeding,
  suggesting large intermediate executor buffers in addition to insert-path
  residency.

Use these only as leads. Re-run before changing code.

## Non-negotiable Constraints

- Preserve ACID correctness. Do not weaken `WalSyncMode::Full`, commit ordering,
  recovery, checkpoint safety, reader snapshot isolation, or one-writer/many-reader
  semantics to win benchmarks.
- Do not change on-disk format, WAL format, C ABI, concurrency model, or add major
  dependencies without first creating an ADR and getting explicit approval.
- Do not change benchmark targets to make results look better.
- Do not remove correctness checks unless profiling proves they are hot and the
  replacement preserves equivalent behavior.
- Do not introduce a global allocator change as the default library behavior.
- Keep edits small and benchmark-driven. One performance idea per iteration.
- Use `.tmp/` for throwaway scripts, profiling output, flamegraphs, and temporary
  reports.

## Required Reading

Read these before coding:

- `AGENTS.md`
- `design/PRD.md`
- `design/TESTING_STRATEGY.md`
- `design/AGENT_BENCHMARK_LOOP.md`
- `design/RUST_BENCHMARK_PLAN.md`
- `design/BENCHMARKING_GUIDE.md`
- `benchmarks/targets.toml`
- `design/adr/0138-post-checkpoint-heap-release.md`
- `design/adr/0141-paged-on-disk-wal-index.md`
- `design/adr/0144-persistent-primary-key-index.md`
- `design/adr/0145-paged-table-row-source.md`

Also inspect the current code around:

- `crates/decentdb/src/db.rs`
- `crates/decentdb/src/config.rs`
- `crates/decentdb/src/exec/dml.rs`
- `crates/decentdb/src/exec/mod.rs`
- `crates/decentdb/src/exec/row.rs`
- `crates/decentdb/src/wal/`
- `crates/decentdb/src/storage/`
- `crates/decentdb-benchmark/src/scenarios.rs`
- `crates/decentdb-benchmark/src/profiles.rs`
- `crates/decentdb-benchmark/src/targets.rs`

## Baseline Commands

Start with the repository-native benchmark loop:

```bash
cargo run -p decentdb-benchmark --release -- run --profile dev --all
summary=$(ls -t build/bench/runs/*/summary.json | head -n1)
cargo run -p decentdb-benchmark -- baseline set --name engine-perf-start --input "$summary"
cargo run -p decentdb-benchmark -- report --latest-run --format markdown --audience agent_brief --output .tmp/engine-perf-start.md
```

Then run focused insert/memory probes if the temporary raw-baseline project still
exists:

```bash
cargo build --release --manifest-path /tmp/tmp-opus47-decentdb-net-tests/rust-baseline/Cargo.toml
/tmp/tmp-opus47-decentdb-net-tests/rust-baseline/target/release/seed-only medium
/tmp/tmp-opus47-decentdb-net-tests/rust-baseline/target/release/memory-probe --rows 5000000 --batch 50000 --cache_mb 64 --label engine-perf-start
```

If that project no longer exists, recreate equivalent probes under `.tmp/` or add
the missing measurement to `crates/decentdb-benchmark` instead of relying on stale
artifacts.

For binding-visible comparison, also run the relevant .NET/Python benchmark or
use existing repo scripts. Capture DecentDB and SQLite peak RSS from the same
host, same scale, same durability settings, and same filesystem.

## Priority Investigation Lanes

### Lane 1: Prepared Insert Throughput Regression

Focus files:

- `crates/decentdb/src/db.rs`
- `crates/decentdb/src/exec/dml.rs`
- `crates/decentdb/src/exec/mod.rs`
- `crates/decentdb/src/wal/`
- `crates/decentdb/src/storage/`

Relevant current paths:

- `SqlTransaction::prepare` and `PreparedStatement::execute_in`
- `Db::execute_prepared_in_exclusive_state`
- `Db::try_execute_prepared_insert_in_runtime_state`
- `EngineRuntime::execute_prepared_simple_insert`
- `EngineRuntime::apply_prepared_simple_insert_candidate`
- `EngineRuntime::append_stored_row_to_table_row_source`
- `EngineRuntime::persist_to_db`
- `commit_exclusive_sql_txn`, `commit_if_latest`, and WAL append/sync paths

Questions to answer with profiling before coding:

- Is the regression mostly CPU in executor/value/index maintenance, WAL/pager
  persistence, checkpoint interaction, or benchmark-side data generation?
- Does the hot loop repeatedly do work that should be hoisted to prepare time?
- Does the insert path rebuild or refresh index/table state more often than
  necessary inside one explicit transaction?
- Is per-row `Vec<Value>` construction, `Value::Text` cloning, index-key encoding,
  `BTreeMap` lookup, or manifest mutation dominating CPU?
- Does a recently added memory feature, such as paged row sources, persistent PK
  index handling, WAL index spill, or checkpoint trimming, affect the raw insert
  hot path even when disabled by default?

Preferred fixes:

- Hoist invariant validation and lookup work from per-row execution into prepared
  insert plans where schema/index epochs make that safe.
- Reuse transaction-local prepared insert runtime state instead of revalidating or
  reloading table/index dependencies per row.
- Avoid avoidable allocations in the per-row insert path, especially transient
  vectors and cloned schema/index metadata.
- Add a direct transaction-local batch insert path only if it preserves existing
  semantics and is exercised by real binding or benchmark code.
- Keep WAL write batching and checkpoint scheduling correct under
  `WalSyncMode::Full`; relaxed or async modes may be measured but must not replace
  the durable baseline.

### Lane 2: Peak RSS During Large Inserts

Focus files:

- `crates/decentdb/src/config.rs`
- `crates/decentdb/src/exec/mod.rs`
- `crates/decentdb/src/exec/dml.rs`
- `crates/decentdb/src/wal/`
- `crates/decentdb/src/storage/`

Questions to answer:

- During a 5M or 10M row insert, how much RSS is anonymous heap vs file-backed
  page cache? Use `/proc/self/status` fields such as `VmRSS`, `RssAnon`, and
  `RssFile`, not just total RSS.
- Which engine state accounts for resident rows, WAL versions, cached payloads,
  runtime indexes, pager cache, manifest/chunk state, and allocator slack?
- Does `DbConfig::cache_size_mb` actually bound the cache-backed portion of RSS?
- Are appended rows retained in both `TableRowSource` and encoded overflow/WAL
  forms longer than needed after commit or checkpoint?
- Does `release_freed_memory_after_checkpoint` help end-of-load RSS but not peak
  RSS? If so, identify the live owner at peak rather than adding more trimming.

Preferred fixes:

- Reduce live duplicated row representations during large explicit transactions.
- Prefer append-only or paged persistence paths that avoid holding full table
  images when the statement shape only appends.
- Make benchmark memory reports split load-phase peak, query-phase peak, anon RSS,
  file RSS, DB size, WAL size, resident row count, and WAL resident/on-disk
  version counts.
- If a memory fix needs a new storage layout or default-on paged storage, stop and
  write an ADR first.

### Lane 3: Peak RSS During Heavy Reads

Focus files:

- `crates/decentdb/src/exec/mod.rs`
- `crates/decentdb/src/exec/row.rs`
- `crates/decentdb-benchmark/src/scenarios.rs`

Known likely contributors:

- `Dataset` stores `Arc<Vec<Vec<Value>>>`, so scans/joins/grouping/sorting can
  materialize many full rows.
- `build_select_dataset` may build a full source dataset before filtering,
  grouping, or projection.
- `evaluate_grouped_select` stores `BTreeMap<Vec<u8>, Vec<usize>>`, retaining
  source rows plus per-group row-index lists.
- `sort_dataset` builds a full `sort_keys` vector, a full order vector, and a
  second row vector during reorder.
- `apply_select_distinct` may clone or unwrap entire row vectors.

Questions to answer:

- Which query in the raw full benchmark causes the largest RSS jump:
  aggregate durations, top-10 artists, top-10 albums, view-first-1000, or songs
  for artist via view?
- Does the optimized row-source lane cover these query shapes after reopen and
  during same-handle execution?
- Are there bounded top-N opportunities for `ORDER BY ... LIMIT` that can avoid
  sorting full intermediate outputs?
- Can common grouped aggregate shapes store aggregate state directly instead of
  retaining all source rows and row indexes?

Preferred fixes:

- Add narrow streaming or bounded-memory helpers for the exact benchmark query
  shapes before attempting broad executor rewrites.
- Keep existing row-source fast paths first and preserve fallback behavior for
  unsupported SQL features.
- Add tests that assert reopened paged tables stay off resident full-table
  materialization for the optimized shape.

### Lane 4: Benchmark Instrumentation Quality

Focus files:

- `crates/decentdb-benchmark/src/scenarios.rs`
- `crates/decentdb-benchmark/src/types.rs`
- `crates/decentdb-benchmark/src/report.rs`
- `benchmarks/targets.toml`

Required improvements if missing:

- Emit peak RSS, end RSS, `RssAnon`, `RssFile`, DB size, WAL size, and peak/DB
  ratio for large-load and complex scenarios.
- Separate insert-phase peak from query-phase peak.
- Add SQLite peak RSS comparison for matching scale and durability where the
  benchmark claims SQLite parity.
- Preserve machine-readable JSON so future agents can rank regressions.

Do not spend the whole task improving instrumentation if a clear engine hotspot
is already proven. Add only the measurement needed to avoid guessing.

## Iteration Workflow

For each small change:

```bash
cargo fmt --check
cargo test -p decentdb --lib
cargo run -p decentdb-benchmark --release -- run --profile smoke --scenario complex_ecommerce
```

If the focused run is promising:

```bash
cargo run -p decentdb-benchmark --release -- run --profile dev --all
candidate=$(ls -t build/bench/runs/*/summary.json | head -n1)
cargo run -p decentdb-benchmark -- compare --candidate "$candidate" --baseline-name engine-perf-start
cargo run -p decentdb-benchmark -- report --latest-compare --format markdown --audience agent_brief --output .tmp/engine-perf-compare.md
```

Before declaring done:

```bash
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

If storage, WAL, checkpoint, or recovery behavior changed, also run the relevant
crash/recovery/migration tests and document why they cover the changed invariant.

## Success Criteria

A successful agent run must produce:

- Fresh baseline numbers and candidate numbers from release builds.
- A concise profiling or measurement note identifying the bottleneck actually
  fixed.
- Code changes scoped to one performance idea.
- Tests for correctness and, where practical, regression coverage for the hot
  path or memory behavior.
- Benchmark comparison showing a net improvement in targeted metrics and no
  unacceptable regressions elsewhere.
- Peak RSS evidence for the large insert/read workload, including comparison to
  SQLite if SQLite parity is claimed.
- No clippy warnings and no test failures.

## Reporting Format

At the end, report:

- Baseline command and artifact path.
- Candidate command and artifact path.
- Targeted metric deltas, especially insert rows/sec and peak RSS.
- Any regressions and whether they are noise, expected tradeoffs, or blockers.
- Exact files changed and the reason for each change.
- Tests and benchmarks run.
- Any ADR-required follow-up that was discovered but not implemented.
