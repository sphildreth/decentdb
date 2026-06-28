# Rust Baseline Improvements

Date: 2026-06-15
Status: Draft implementation plan

This document consolidates the local review of
`benchmarks/rust-baseline/README.md`, the current
`benchmarks/rust-baseline/src/main.rs` implementation, and the external coding
agent feedback. It is intentionally implementation-heavy: future agents should
be able to pick a phase, edit the named files, and avoid re-deciding benchmark
scope.

## Verdict

`benchmarks/rust-baseline` is a useful raw-engine diagnostic benchmark. It is
not yet a complete showcase benchmark for DecentDB against popular embedded
database engines.

The current runner does benchmark an important workload:

- A deterministic music-library schema with `artists`, `albums`, `songs`, five
  secondary indexes, and the `v_artist_songs` view.
- Bulk-load throughput into a durable WAL database.
- Single-threaded post-load query totals for `COUNT(*)`, aggregate durations,
  point lookup by artist id, grouped Top-N queries, and view scans.
- DecentDB default and `resident-hot-read` profiles, with the tuned profile
  explicitly separated from the historical default profile.
- SQLite through `rusqlite` using WAL, `synchronous=FULL`, and disabled
  autocheckpointing, followed by an explicit checkpoint before reads.
- Per-step wall-clock duration, seed throughput, peak RSS, database size, WAL
  size, checkpoint size details, and historical HTML reporting.
- A separate DecentDB-only plan-cache guardrail suite with p95 and p99 metrics.

The current runner does not yet benchmark several things that matter for a
public "showcase" claim:

- The music-library workload compares DecentDB only with SQLite. DuckDB already
  exists in the native public benchmark and Python framework, but not here.
- The main workload records each query shape once. It does not produce p50,
  p95, p99, max, or standard deviation for the music-library query paths.
- It does not exercise DecentDB's one-writer, many-readers concurrency model.
- It does not measure reader latency while a writer is committing.
- It does not measure durable single-row commit latency in this workload.
- It does not measure update or delete behavior.
- It does not measure cold-process open, first-query latency, or recovery
  reopen behavior.
- It does not generate side-by-side latest-run ratios in the HTML report.
- The README wording overstates the "no FFI" claim unless read very carefully:
  DecentDB is exercised through a native Rust API; SQLite is exercised through
  `rusqlite`, which crosses into SQLite's C API.

The right direction is to keep this benchmark as the rich music-library
raw-engine diagnostic and extend it with targeted modes. Do not turn it into a
polyglot binding benchmark or a key-value benchmark.

## Relationship To Existing Benchmark Surfaces

Do not duplicate work that already belongs elsewhere.

| Surface | Path | Role | This plan's relationship |
|---|---|---|---|
| Public native README benchmark | `crates/decentdb/benches/embedded_compare.rs` | Small, high-iteration latency and throughput suite for DecentDB, SQLite, and DuckDB. Produces `data/bench_summary.json`. | Keep as the public chart source. Use its metric names and sampling discipline as the model for rust-baseline latency modes. |
| Rust music-library baseline | `benchmarks/rust-baseline` | Large deterministic raw-engine diagnostic for realistic joins, views, aggregates, bulk load, and total runtime. | Extend here. This document is about this surface. |
| Python embedded comparison | `benchmarks/python_embedded_compare` | Cross-runtime and binding-aware comparison across SQLite, DuckDB, DecentDB, JDBC engines, Firebird, LiteDB, and related drivers. | Keep binding and cross-runtime overhead work there. Do not add Python, Node, Go, or .NET binding measurements to rust-baseline. |
| Agent benchmark loop | `design/AGENT_BENCHMARK_LOOP.md` | Broader desired benchmark taxonomy for commit, cold lookup, recovery, and read-under-write. | Reuse metric semantics where they fit. Do not implement the whole archived taxonomy in one patch. |
| Metric improvement tracker | `design/_archive/METRIC_IMPROVEMENTS_PLAN.md` | Historical tracker for public metrics and rust-baseline totals. | Active performance work now lives in `design/WIN_PERFORMANCE_IMPROVEMENTS_01.md`. |

## Consolidated Findings

### Accepted Findings

The following external-agent findings are valid and should drive work:

1. Add DuckDB to the music-library benchmark.
   - The existing public native benchmark already includes DuckDB.
   - The Python framework already includes DuckDB.
   - The music-library query mix includes aggregates, grouped Top-N queries, and
     view scans, which are exactly the kinds of shapes where a columnar embedded
     engine provides useful contrast.

2. Add latency distributions to the main query paths.
   - Single-shot timings are useful for total-runtime diagnostics but too noisy
     for public claims.
   - The runner should report p50, p95, p99, max, mean, standard deviation,
     operation count, warmup count, and operations per second for repeated
     query cases.

3. Add concurrent reader coverage.
   - DecentDB's mission model is one writer and many reader threads.
   - The existing `rust-baseline` runner is strictly serial.
   - The public native benchmark has `concurrent_read_p95_ms`; the music
     workload should have a corresponding suite using music-library queries.

4. Add read-under-write coverage.
   - Concurrent reads alone prove read parallelism.
   - Reader latency while a writer commits proves the practical shape of the
     one-writer, many-readers model.

5. Add durable commit latency in this workload.
   - Bulk seed loops commit once per table. That is a bulk-load metric, not a
     single-row durable write metric.
   - The public native benchmark has `commit_p95_ms`; rust-baseline should
     expose an analogous music-library durable commit case.

6. Add update and delete cases.
   - The current workload inserts and then reads.
   - A relational embedded database showcase should include basic mutation
     latency, even if it stays separate from the historical total-runtime path.

7. Add cold-open and first-query measurements.
   - Embedded engines are often opened on demand in desktop, mobile, CLI, and
     edge processes.
   - `connect_open` in the current runner only measures creation/open during a
     fresh run. It does not distinguish warm same-process reopen, cold-process
     open, first `COUNT(*)`, or first indexed lookup.

8. Improve README framing.
   - Document that DecentDB uses a native Rust API while SQLite uses `rusqlite`
     over the SQLite C API.
   - Document the workload class as "bulk-load then read-only" for the current
     historical path.
   - Document default profile versus tuned showcase profile without changing
     the historical default.
   - Document whether `huge` is a routine tier or a long-running showcase tier.

9. Improve reporting.
   - The HTML report should show latest side-by-side engine ratios when matching
     scale/profile results exist.
   - The report should display latency-suite tables, not only historical total
     runtime charts.

10. Track memory as a benchmark result, not a README aside.
    - The README's 2.2 GB RSS observation is useful, but it reads like a bug
      report embedded in user-facing benchmark docs.
    - RSS, anonymous RSS, file RSS, database bytes, WAL bytes, and configured
      cache/profile metadata should be retained in JSON and displayed in the
      report.

### Findings Accepted With Scope Changes

These findings are useful but belong in a narrower form:

1. "Add other engines such as LMDB, RocksDB, redb, or sled."
   - Scope decision: do not add key-value engines to the music-library
     relational runner.
   - Reason: mapping joins, SQL views, grouping, and transactions onto KV
     engines would become a new workload with subjective adapter choices.
   - Future location: a separate KV-specific benchmark plan, if desired.

2. "Add binding overhead."
   - Scope decision: do not add binding benchmarks to rust-baseline.
   - Reason: rust-baseline is the raw-engine ceiling and should remain free of
     binding/runtime costs.
   - Future location: `benchmarks/python_embedded_compare`,
     `tests/bindings/`, and binding-specific benchmark directories.

3. "Make resident-hot-read the default."
   - Scope decision: keep `default` as the historical default.
   - Add a showcase matrix that runs both `default` and `resident-hot-read`
     and labels them clearly.
   - Reason: silently changing the default would break historical comparisons
     and obscure the memory/performance tradeoff.

4. "Add SQLite NORMAL/OFF durability modes."
   - Scope decision: add `sqlite-wal-normal` later as an explicitly labeled
     exploratory profile; do not add `OFF` as a primary showcase profile.
   - Reason: the primary comparison must preserve durable ACID semantics.
     `NORMAL` is useful context, but `OFF` is not a durable comparison.

5. "Add a full-table scan benchmark."
   - Scope decision: clarify existing scan coverage and add a materializing
     full-scan case only as a lower-priority item.
   - Reason: `COUNT(*)` and aggregate queries already exercise table-wide
     access, but they do not measure materializing every row and column.

### Findings Not Adopted For This Plan

These items are not part of this implementation plan:

1. Binary/library size tracking.
   - Useful for embedded distribution, but it belongs in release/package size
     checks rather than this runtime benchmark.

2. Compression ratio, vacuum, reindex, and page fill-factor.
   - Useful storage diagnostics, but they are not necessary for the first
     showcase correction. Keep them for a storage-efficiency plan.

3. Large blob or wide-row benchmarking.
   - Valid future work, especially for bindings and mobile, but not required to
     fix the current showcase gap.

4. Graph, JSON/document, or time-series workloads.
   - Useful only if DecentDB commits to those product surfaces as first-class
     benchmark stories.

## Phase Map

Only use `DONE` and `TODO` in the phase status column.

| Phase | Status | Purpose | Primary files |
|---:|---|---|---|
| 0 | DONE | Review current README, implementation, existing benchmark docs, and external-agent findings. | This document |
| 1 | DONE | Tighten benchmark contract, README framing, result metadata, and historical compatibility. | `benchmarks/rust-baseline/README.md`, `benchmarks/rust-baseline/src/main.rs` |
| 2 | DONE | Add high-iteration latency-suite mode for music-library reads. | `benchmarks/rust-baseline/src/main.rs`, report HTML code |
| 3 | DONE | Add concurrent-read and read-under-write suites. | `benchmarks/rust-baseline/src/main.rs` |
| 4 | DONE | Add durable commit, update, delete, cold-open, and recovery-reopen suites. | `benchmarks/rust-baseline/src/main.rs`, optional helper child mode |
| 5 | DONE | Add DuckDB engine support for the music-library workload. | `benchmarks/rust-baseline/Cargo.toml`, `benchmarks/rust-baseline/src/main.rs`, README |
| 6 | DONE | Improve HTML report with cross-engine ratios, latency tables, and memory views. | `benchmarks/rust-baseline/src/main.rs` |
| 7 | DONE | Add optional exploratory profiles and deferred workload extensions. | README, future design docs as needed |

## Phase 1: Benchmark Contract And Metadata

Status: DONE

### Goal

Make the current benchmark's scope impossible to misread, and add metadata that
lets old and new JSON files coexist.

### Required README Edits

Edit `benchmarks/rust-baseline/README.md`.

1. Replace the current "no FFI" paragraph with exact access-path wording:

   - DecentDB path:
     `DecentDB is called through the native Rust crate API and does not cross
     the C ABI or language binding layers inside the timed loop.`
   - SQLite path:
     `SQLite is called through rusqlite, which is a Rust wrapper over SQLite's
     C API. SQLite results therefore include the normal rusqlite/SQLite C API
     crossing cost.`
   - Future DuckDB path:
     `DuckDB results, when enabled, use duckdb-rs and should be labeled as
     duckdb-rs over DuckDB's native engine.`
   - Keep the "raw-engine ceiling" idea only for DecentDB. Do not imply every
     engine has zero FFI overhead.

2. Add a "Workload Class" section:

   - Historical main path: `bulk_load_then_read_only_music_library`.
   - Bulk seed policy: one explicit transaction per logical seed table.
   - Query policy today: one measured execution per query shape.
   - Durability policy: DecentDB durable WAL profile, SQLite WAL FULL,
     explicit checkpoint before query timings.
   - Non-goals: binding overhead, polyglot runtime overhead, KV adapter
     comparisons, non-durable write shortcuts.

3. Add a "Showcase Matrix" section:

   - Historical default:
     `--engine decentdb --profile default`.
   - Tuned DecentDB read-heavy profile:
     `--engine decentdb --profile resident-hot-read`.
   - SQLite durable baseline:
     `--engine sqlite`.
   - DuckDB engine-default row:
     `--engine duckdb` after Phase 5 lands.
   - State explicitly that all rows must be reported with profile labels.

4. Add runtime expectations for scale tiers:

   - `smoke`: quick local sanity check.
   - `medium`: local development comparison.
   - `full`: release-quality raw-engine cross-check.
   - `huge`: long-running stress/showcase tier, not required for every PR.

   Do not invent exact runtimes without measuring them in the same patch. If a
   patch has current measured runtimes, add them with command, machine label,
   and date.

5. Move the "Engine memory observation" section out of the README prose:

   - Keep a short link-style note:
     `Memory behavior is tracked in JSON and in design/WIN_PERFORMANCE_IMPROVEMENTS_01.md.`
   - Do not delete the concern. Preserve it in this document or the metric plan
     after implementation adds structured memory reporting.

### Required JSON Metadata

Edit `RunReport` in `benchmarks/rust-baseline/src/main.rs`.

Add these fields with serde defaults so old checked-in result files still load:

```rust
#[serde(default = "default_result_schema_version")]
result_schema_version: u32,
#[serde(default)]
measurement_family: String,
#[serde(default)]
engine_access_path: String,
#[serde(default)]
durability_profile: String,
#[serde(default)]
workload_class: String,
#[serde(default)]
cache_profile: String,
#[serde(default)]
query_repetition_policy: String,
#[serde(default)]
cold_state_policy: String,
```

Use these exact values for the existing main path:

| Field | DecentDB default | DecentDB resident-hot-read | SQLite |
|---|---|---|---|
| `result_schema_version` | `2` | `2` | `2` |
| `measurement_family` | `music_library_total_runtime` | `music_library_total_runtime` | `music_library_total_runtime` |
| `engine_access_path` | `decentdb_native_rust` | `decentdb_native_rust` | `sqlite_rusqlite_c_api` |
| `durability_profile` | `decentdb_durable_wal_default` | `decentdb_durable_wal_default` | `sqlite_wal_full` |
| `workload_class` | `bulk_load_then_read_only_music_library` | same | same |
| `cache_profile` | `decentdb_default_low_memory` | `decentdb_resident_hot_read` | `sqlite_default_cache` |
| `query_repetition_policy` | `single_execution_per_query_shape` | same | same |
| `cold_state_policy` | `same_process_fresh_create_then_query` | same | same |

The old `binding` field must remain for compatibility. Do not rename it in this
phase.

### Required CLI Validation

Update CLI validation so impossible combinations fail early:

- `--profile` remains valid only with `--engine decentdb`.
- `--report-file` remains valid only with `--report` or `--benchmark`.
- New metadata fields must be populated for every `RunReport`, including
  report files generated by old command forms.

### Required Tests And Validation

Run:

```bash
cargo test --manifest-path benchmarks/rust-baseline/Cargo.toml
cargo run --manifest-path benchmarks/rust-baseline/Cargo.toml --release --bin rust-baseline -- --engine decentdb --scale smoke --out-dir ../../.tmp/rust-baseline-phase1
cargo run --manifest-path benchmarks/rust-baseline/Cargo.toml --release --bin rust-baseline -- --report --out-dir benchmarks/rust-baseline/results --report-file ../../.tmp/rust-baseline-phase1/report.html
```

Acceptance criteria:

- Old checked-in JSON files still load in `--report`.
- New smoke JSON contains all metadata fields above.
- README no longer implies SQLite is zero-FFI.
- No benchmark behavior changes except metadata and documentation.

## Phase 2: Music-Library Latency Suite

Status: DONE

### Goal

Add repeated, percentile-based measurements for the music-library read paths
without replacing the historical total-runtime path.

### CLI

Add a new flag:

```text
--latency-suite
```

Rules:

- `--latency-suite` runs after schema creation, seed, and checkpoint for the
  selected engine, scale, and profile.
- It may be combined with a single `--scale`.
- It may be combined with `--benchmark`; in suite mode it runs latency cases
  for each scale after the existing total-runtime steps.
- It must not be combined with `--plan-cache-benchmark`.
- It must write latency metrics into the same JSON file as the main run under a
  new `latency_cases` array.

Add optional arguments:

```text
--latency-iterations <N>       default 10000
--latency-warmup <N>           default 200
--heavy-latency-iterations <N> default 200
--heavy-latency-warmup <N>     default 20
```

Do not use adaptive iteration counts in the first implementation. Use the exact
defaults above so runs are comparable.

### JSON Shape

Add:

```rust
#[derive(Clone, Default, Serialize, Deserialize)]
struct LatencyCaseMetric {
    name: String,
    query_shape: String,
    iterations: u64,
    warmup_iterations: u64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    max_ns: u64,
    mean_ns: f64,
    stddev_ns: f64,
    operations_per_second: f64,
    rows_per_iteration: Option<u64>,
    extra: serde_json::Map<String, serde_json::Value>,
}
```

Add `latency_cases: Vec<LatencyCaseMetric>` to `RunReport` with
`#[serde(default)]`.

Percentile rules:

- Sort a copy of the sample vector with `sort_unstable`.
- Use nearest-rank indexing:
  `ceil(percentile / 100 * len) - 1`, clamped to `[0, len - 1]`.
- Include p50, p95, p99, and max.
- Do not include warmup samples in the output samples.
- Use nanoseconds in JSON. The HTML report can display us or ms.

### Required Latency Cases

Implement these exact cases for DecentDB and SQLite. Phase 5 must add the same
cases for DuckDB.

| Case name | Iterations | SQL shape | Parameter generator | Row materialization |
|---|---:|---|---|---|
| `artist_pk_lookup_full_row` | `--latency-iterations` | `SELECT id, name, country, formed_year FROM artists WHERE id = ?` | `artist_id = 1 + ((i * 8191) % scale.artists)` | Materialize all selected columns and assert one row. |
| `artist_pk_lookup_name_only` | `--latency-iterations` | `SELECT name FROM artists WHERE id = ?` | same as above | Materialize the text column and assert one row. |
| `song_pk_range_50` | `--latency-iterations` | `SELECT id, title, duration_ms FROM songs WHERE id >= ? AND id < ? ORDER BY id LIMIT 50` | `start = 1 + ((i * 1019) % max(1, total_songs - 100)); end = start + 100` | Materialize all returned columns and assert row count is at most 50. |
| `songs_by_artist_secondary_index_50` | `--latency-iterations` | `SELECT id, title, duration_ms FROM songs WHERE artist_id = ? ORDER BY id LIMIT 50` | same artist id generator | Materialize all returned columns. Row count may be 0 only if seed plan proves no songs for that artist; record row count. |
| `song_album_join_by_song_id` | `--latency-iterations` | `SELECT s.id, s.title, al.title FROM songs s JOIN albums al ON al.id = s.album_id WHERE s.id = ?` | `song_id = 1 + ((i * 4099) % total_songs)` | Materialize all selected columns and assert one row. |
| `artist_song_count_aggregate` | `--latency-iterations` | `SELECT COUNT(*), SUM(duration_ms) FROM songs WHERE artist_id = ?` | same artist id generator | Materialize both scalar values. |
| `view_artist_filter` | `--heavy-latency-iterations` | `SELECT album_title, song_title, duration_ms FROM v_artist_songs WHERE artist_id = ?` | same artist id generator | Materialize every returned column and row. |
| `top10_artists_by_songs` | `--heavy-latency-iterations` | Existing top-10 artists SQL | no parameters | Materialize all 10 rows. |

Use prepared statements for all repeated cases.

DecentDB requirements:

- Prepare each case once before warmup.
- Use `PreparedStatement::execute` or the existing prepared API that matches
  the current engine API. Do not call unprepared `db.execute` inside the
  repeated timed loop.
- Keep `Value` buffers reusable where the existing code patterns make that
  straightforward.

SQLite requirements:

- Prepare each case once before warmup.
- Use `rusqlite` prepared statements and parameter binding.
- Materialize all selected columns in the timed loop.

### Validation

Run:

```bash
cargo test --manifest-path benchmarks/rust-baseline/Cargo.toml
cargo run --manifest-path benchmarks/rust-baseline/Cargo.toml --release --bin rust-baseline -- --engine decentdb --scale smoke --latency-suite --out-dir ../../.tmp/rust-baseline-latency-decentdb
cargo run --manifest-path benchmarks/rust-baseline/Cargo.toml --release --bin rust-baseline -- --engine sqlite --scale smoke --latency-suite --out-dir ../../.tmp/rust-baseline-latency-sqlite
```

Acceptance criteria:

- JSON includes exactly the required case names.
- Every case has `iterations`, `warmup_iterations`, p50, p95, p99, max, mean,
  stddev, and operations per second.
- DecentDB and SQLite run the same logical SQL shape and parameter sequence.
- Historical single-shot step timings remain present and unchanged.

## Phase 3: Concurrent Reads And Read Under Write

Status: DONE

### Goal

Exercise the DecentDB concurrency model directly in the music-library workload.

### CLI

Add:

```text
--concurrency-suite
--reader-thread-counts <LIST> default 1,2,4,8
--concurrent-reads-per-thread <N> default 25000
--writer-commits <N> default 1000
```

Rules:

- `--concurrency-suite` runs after seed and checkpoint.
- It may be combined with `--latency-suite`.
- It must not be combined with `--plan-cache-benchmark`.
- It writes `concurrency_cases` into the same JSON file.

### Schema Addition

Create a benchmark-only write table after the normal schema is created:

```sql
CREATE TABLE write_events (
    id INTEGER PRIMARY KEY,
    artist_id INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX idx_write_events_artist ON write_events (artist_id);
```

This table must not be used by the historical query steps. It exists only for
write and read-under-write suites so the music-library read dataset remains
stable.

If adding the table to the main `DDL` changes historical `schema_create`
timings, record the change in the README and JSON metadata. Prefer adding it
only when a write/concurrency suite is requested if that keeps historical
comparisons cleaner.

### JSON Shape

Add:

```rust
#[derive(Clone, Default, Serialize, Deserialize)]
struct ConcurrencyCaseMetric {
    name: String,
    reader_threads: usize,
    reads_per_thread: u64,
    writer_commits: u64,
    reader_p50_ns: u64,
    reader_p95_ns: u64,
    reader_p99_ns: u64,
    reader_max_ns: u64,
    reader_operations_per_second: f64,
    writer_p50_ns: Option<u64>,
    writer_p95_ns: Option<u64>,
    writer_p99_ns: Option<u64>,
    writer_operations_per_second: Option<f64>,
    reader_degradation_ratio_vs_isolated: Option<f64>,
    extra: serde_json::Map<String, serde_json::Value>,
}
```

Add `concurrency_cases: Vec<ConcurrencyCaseMetric>` to `RunReport` with
`#[serde(default)]`.

### Required Cases

1. `concurrent_artist_pk_lookup_isolated`
   - For each thread count in `--reader-thread-counts`, start that many reader
     threads.
   - Each thread performs `--concurrent-reads-per-thread` prepared
     `artist_pk_lookup_name_only` operations.
   - Parameter generator:
     `artist_id = 1 + (((thread_index * reads_per_thread + i) * 8191) % scale.artists)`.
   - Record combined reader latencies.
   - This case has no writer latencies.

2. `artist_pk_lookup_under_insert_writer`
   - For each thread count in `--reader-thread-counts`, start reader threads as
     above.
   - Simultaneously run one writer performing `--writer-commits` durable
     single-row inserts into `write_events`.
   - Writer row:
     `id = 1_000_000_000 + i`,
     `artist_id = 1 + ((i * 8191) % scale.artists)`,
     `payload = format!("event {i}")`.
   - Each writer iteration must be a durable commit boundary comparable to
     normal autocommit semantics for that engine.
   - Record reader latencies and writer latencies.
   - Compute `reader_degradation_ratio_vs_isolated` as:
     `under_write.reader_p95_ns / isolated.reader_p95_ns` for the same engine,
     scale, profile, and thread count.

### Engine-Specific Concurrency Rules

DecentDB:

- Use the same safe sharing pattern already used in
  `crates/decentdb/benches/embedded_compare.rs`: clone or share `Db` through
  `Arc` only if the type supports it safely.
- Do not introduce unsafe code for benchmark concurrency.
- Reader threads must use prepared statements created per thread.
- The writer must use the same `Db` handle only if the public API guarantees it
  is safe. Otherwise open or clone according to existing DecentDB patterns.

SQLite:

- Use one SQLite connection per thread.
- Each connection must set `journal_mode=WAL` and `synchronous=FULL`.
- Each reader thread prepares its own statement once.
- The writer uses a separate connection and commits each insert durably.

DuckDB, after Phase 5:

- If DuckDB connections cannot be used safely across threads, record a
  `concurrent_mode` extra value of `single_thread_fallback`.
- Do not fake multi-threaded DuckDB numbers.

### Validation

Run:

```bash
cargo test --manifest-path benchmarks/rust-baseline/Cargo.toml
cargo run --manifest-path benchmarks/rust-baseline/Cargo.toml --release --bin rust-baseline -- --engine decentdb --scale smoke --concurrency-suite --reader-thread-counts 1,2,4 --concurrent-reads-per-thread 1000 --writer-commits 100 --out-dir ../../.tmp/rust-baseline-concurrency-decentdb
cargo run --manifest-path benchmarks/rust-baseline/Cargo.toml --release --bin rust-baseline -- --engine sqlite --scale smoke --concurrency-suite --reader-thread-counts 1,2,4 --concurrent-reads-per-thread 1000 --writer-commits 100 --out-dir ../../.tmp/rust-baseline-concurrency-sqlite
```

Acceptance criteria:

- JSON contains isolated and under-writer cases for each requested thread count.
- Reader p95 degradation ratio is present for every under-writer case.
- Writer inserts are visible in `write_events` after the suite completes.
- The historical read-only query step results are not affected by `write_events`.

## Phase 4: Writes, CRUD, Cold Open, And Recovery

Status: DONE

### Goal

Cover embedded database behaviors that the current bulk-load/read-only path
does not represent.

### CLI

Add:

```text
--write-suite
--cold-suite
--write-iterations <N> default 1000
```

Rules:

- Both suites run after seed and checkpoint.
- They may be combined with `--latency-suite` and `--concurrency-suite`.
- They must not be combined with `--plan-cache-benchmark`.
- `--cold-suite` may use child processes, but child process outputs must be
  written under `.tmp/` or the selected `--out-dir`, not the repo root.

### Write Suite Cases

Use the `write_events` table from Phase 3.

Add `write_cases: Vec<LatencyCaseMetric>` to `RunReport`; reuse the same
latency metric struct from Phase 2.

Required cases:

1. `durable_insert_autocommit`
   - `--write-iterations` rows.
   - One durable commit boundary per row.
   - Insert into `write_events`.
   - Record p50, p95, p99, max, mean, stddev, operations per second.

2. `durable_insert_batch_10`
   - `--write-iterations` rows.
   - Commit every 10 rows.
   - Record per-commit latency, not per-row latency.
   - Add `rows_per_commit = 10` in `extra`.

3. `update_by_pk_autocommit`
   - Pre-seed `write_events` with `--write-iterations` rows in one explicit
     transaction before timing.
   - For each timed iteration, update one row by primary key:
     `UPDATE write_events SET payload = ? WHERE id = ?`.
   - One durable commit boundary per update.

4. `delete_by_pk_autocommit`
   - Pre-seed a separate id range in `write_events`.
   - For each timed iteration, delete one row by primary key:
     `DELETE FROM write_events WHERE id = ?`.
   - One durable commit boundary per delete.

### Cold Suite Cases

Add `cold_cases: Vec<LatencyCaseMetric>` to `RunReport`.

Required cases:

1. `same_process_reopen_first_count`
   - Close/drop the database handle after seed and checkpoint.
   - Reopen in the same process.
   - Time open plus first `SELECT COUNT(*) FROM songs`.
   - Record one sample per iteration.
   - Default iterations: 30, regardless of `--latency-iterations`.
   - Add `cold_state_policy = same_process_reopen`.

2. `same_process_reopen_first_artist_lookup`
   - Same as above, but first query is:
     `SELECT name FROM artists WHERE id = ?`.
   - Use the artist id generator with iteration index.

3. `cold_process_open_first_count`
   - Parent process seeds and checkpoints once.
   - Parent spawns the same binary in a private helper mode to open the existing
     database and run first `SELECT COUNT(*) FROM songs`.
   - Helper mode must write one small JSON object to a temp file under
     `--out-dir`.
   - Parent imports the helper sample.
   - Default iterations: 10.
   - Add `cold_state_policy = child_process_reopen_os_cache_unspecified`.

4. `recovery_reopen_first_count`
   - Parent creates a database with committed rows in the WAL that have not
     been checkpointed.
   - Parent closes the handle without calling checkpoint.
   - Child helper opens the database and runs `SELECT COUNT(*) FROM songs`.
   - Record reopen plus first-query latency.
   - Verify count equals expected seeded count.
   - This is recovery reopen, not a simulated torn-write crash. Do not claim
     power-loss coverage from this case.

### Helper CLI

Add hidden helper flags:

```text
--cold-helper
--cold-helper-query <count_songs|artist_lookup>
--cold-helper-output <PATH>
```

Rules:

- Hidden helper mode must require `--db-path`, `--engine`, and
  `--cold-helper-output`.
- Hidden helper mode must not delete the database files.
- Hidden helper mode must not write a normal run report.
- Hidden helper mode must exit nonzero if the query result is wrong.

### Validation

Run:

```bash
cargo test --manifest-path benchmarks/rust-baseline/Cargo.toml
cargo run --manifest-path benchmarks/rust-baseline/Cargo.toml --release --bin rust-baseline -- --engine decentdb --scale smoke --write-suite --write-iterations 100 --out-dir ../../.tmp/rust-baseline-write-decentdb
cargo run --manifest-path benchmarks/rust-baseline/Cargo.toml --release --bin rust-baseline -- --engine sqlite --scale smoke --write-suite --write-iterations 100 --out-dir ../../.tmp/rust-baseline-write-sqlite
cargo run --manifest-path benchmarks/rust-baseline/Cargo.toml --release --bin rust-baseline -- --engine decentdb --scale smoke --cold-suite --out-dir ../../.tmp/rust-baseline-cold-decentdb
cargo run --manifest-path benchmarks/rust-baseline/Cargo.toml --release --bin rust-baseline -- --engine sqlite --scale smoke --cold-suite --out-dir ../../.tmp/rust-baseline-cold-sqlite
```

Acceptance criteria:

- Durable autocommit insert latency is represented separately from bulk seed
  throughput.
- Update and delete cases are present for both DecentDB and SQLite.
- Same-process and child-process cold cases are labeled separately.
- Recovery reopen validates row counts and does not claim power-loss testing.

## Phase 5: DuckDB Music-Library Engine

Status: DONE

### Goal

Add DuckDB to the same music-library workload so the benchmark can honestly
compare DecentDB against more than SQLite.

### Dependency

Edit `benchmarks/rust-baseline/Cargo.toml`:

```toml
duckdb = { version = "1.0", features = ["bundled"] }
```

This version and feature set should match the existing native public benchmark
dependency in `crates/decentdb/Cargo.toml`. If the required version or feature
set differs, stop and decide whether an ADR is needed for the dependency change.

### CLI

Extend:

```rust
enum BenchmarkEngine {
    DecentDb,
    Sqlite,
    DuckDb,
}
```

Use exact labels:

- CLI value: `duckdb`.
- `binding`: `DuckDbRs`.
- `benchmark_profile`: `duckdb-engine-default`.
- `engine_access_path`: `duckdb_rs_c_api`.
- `durability_profile`: `duckdb_engine_default`.
- `cache_profile`: `duckdb_threads_1`.

### DuckDB Setup

Implement `run_duckdb_benchmark` parallel to `run_sqlite_benchmark`.

Required setup:

```sql
SET threads = 1;
```

Use the same music-library schema semantics. Type name differences are allowed
only when DuckDB requires them. Keep table names, column names, indexes, and
view names identical.

Transaction policy:

- Seed artists in one explicit transaction.
- Seed albums in one explicit transaction.
- Seed songs in one explicit transaction.
- Run `CHECKPOINT` after seed if the DuckDB API supports it in this context.
- Record checkpoint duration and database/WAL bytes when available.
- If WAL bytes cannot be found consistently, record `0` and add
  `extra.duckdb_wal_size_available = false`.

Query policy:

- Implement every current historical query shape.
- Implement every Phase 2 latency case.
- Implement Phase 4 write cases where DuckDB supports the semantics.
- For concurrency cases, use the explicit fallback rules from Phase 3.

### Reporting

DuckDB must not be merged under SQLite or DecentDB profile groupings.

The HTML report must show DuckDB as a separate engine/profile row:

```text
DuckDB / duckdb-engine-default
```

### Validation

Run:

```bash
cargo test --manifest-path benchmarks/rust-baseline/Cargo.toml
cargo run --manifest-path benchmarks/rust-baseline/Cargo.toml --release --bin rust-baseline -- --engine duckdb --scale smoke --out-dir ../../.tmp/rust-baseline-duckdb
cargo run --manifest-path benchmarks/rust-baseline/Cargo.toml --release --bin rust-baseline -- --engine duckdb --scale smoke --latency-suite --out-dir ../../.tmp/rust-baseline-duckdb-latency
```

Acceptance criteria:

- DuckDB smoke run completes with the same seed counts as DecentDB and SQLite.
- DuckDB JSON uses the exact labels above.
- DuckDB report rows appear independently.
- README examples include DuckDB.

## Phase 6: HTML Report Improvements

Status: DONE

### Goal

Make the historical report useful as a comparison artifact, not only a trend
viewer.

### Required Features

1. Latest-run comparison table.
   - For each scale, find the latest run for each engine/profile combination.
   - Show total runtime, peak RSS, database bytes, WAL bytes, and each
     historical step duration.
   - Show ratios:
     - `SQLite / DecentDB` for durations where lower is better.
     - `DuckDB / DecentDB` for durations where lower is better.
     - `DecentDB / SQLite` for throughput where higher is better.
   - Label the ratio direction in the table header.

2. Latency-suite table.
   - For each scale and engine/profile, show p50, p95, p99, max, and ops/sec
     for each `latency_cases` entry.
   - Add ratio columns when matching case names exist across engines.

3. Concurrency-suite table.
   - Group by case name and reader thread count.
   - Show isolated p95, under-writer p95, writer p95, and degradation ratio.
   - Highlight degradation ratios above 2.0x.

4. Write-suite table.
   - Show p50, p95, p99, max, ops/sec for commit/update/delete cases.
   - Keep bulk seed throughput visually separate from durable autocommit
     latency.

5. Cold-suite table.
   - Show same-process reopen and cold-process reopen separately.
   - Show recovery reopen separately.
   - Never combine cold-process and warm same-process samples.

6. Memory table.
   - Show peak RSS, anonymous RSS, file RSS, database bytes, WAL bytes, and
     cache/profile metadata.
   - Do not treat file-backed RSS as heap without labeling it.

### Backward Compatibility

Existing checked-in result files do not have `latency_cases`,
`concurrency_cases`, `write_cases`, or `cold_cases`. The report must treat
missing arrays as empty arrays.

Acceptance criteria:

- `--report` succeeds on the existing checked-in `results/*.json`.
- `--report` succeeds on a mixed directory containing old files and new files.
- The report clearly separates DecentDB default, DecentDB resident-hot-read,
  SQLite WAL FULL, and DuckDB engine-default rows.

## Phase 7: Optional Exploratory Profiles And Deferred Workloads

Status: DONE

### SQLite WAL NORMAL

Add only after Phases 1 through 6 are complete.

Required labels:

- CLI value: `--sqlite-profile wal-normal`.
- `benchmark_profile`: `sqlite-wal-normal`.
- `durability_profile`: `sqlite_wal_normal`.
- README wording: `exploratory relaxed-sync profile; not the primary durable
  ACID comparison`.

Do not add `synchronous=OFF` to the primary benchmark matrix.

### Index Creation Isolation

Add separate schema timing steps:

- `schema_create_tables`
- `schema_create_indexes`
- `schema_create_views`

Keep the existing `schema_create` field either as:

- the sum of those three new steps in report-only derived output, or
- a retained compatibility step with sub-step extras.

Do not break historical reports.

### Materializing Full Scan

Add a lower-priority latency case:

```sql
SELECT id, album_id, artist_id, title, duration_ms FROM songs
```

Rules:

- Use a row cap argument:
  `--full-scan-row-limit <N> default 100000`.
- Name the case `songs_full_scan_materialized`.
- Report rows/sec and MB/sec in `extra`.

### Wide Row And Blob Workload

Do not add this to the music-library runner without a separate mini-design.

If implemented later, use a separate suite name and separate tables so it does
not distort historical music-library comparisons.

### KV Engine Comparisons

Do not add LMDB, RocksDB, redb, sled, or similar engines to this SQL
music-library runner.

If DecentDB needs a KV comparison story, create a dedicated benchmark with:

- Explicit key and value encoding.
- Explicit transaction semantics.
- No SQL joins or views.
- Separate result files and report labels.

## Implementation Guardrails

1. Preserve durability.
   - Do not weaken DecentDB WAL sync behavior to improve benchmark numbers.
   - Do not compare durable DecentDB against non-durable SQLite or DuckDB rows
     without loud profile labels.

2. Preserve historical comparability.
   - The existing main run should still produce the same 13 historical steps
     unless a phase explicitly changes that behavior.
   - New suites must add arrays or new sections; they must not remove existing
     fields.

3. Keep benchmark-only code out of the engine core.
   - Do not add SQLite or DuckDB behavior to `crates/decentdb`.
   - Do not optimize engine internals only for benchmark SQL strings.

4. Use `.tmp/` for local run outputs in documentation and validation commands.
   - Do not add new generated files under the repo root.
   - Do not commit local benchmark output unless a human explicitly asks for
     updated baseline artifacts.

5. Keep old JSON loading.
   - Any new field in `RunReport` must have a serde default.
   - Report code must tolerate missing arrays and missing metadata.

6. Add tests where practical.
   - Unit-test percentile indexing.
   - Unit-test parameter generators for deterministic bounds.
   - Unit-test old JSON deserialization if a fixture is easy to add.

7. Keep README claims tied to actual commands.
   - Do not say a suite exists until the CLI flag and JSON output exist.
   - Do not publish ratios without recording the command, scale, profile, and
     date used to produce them.

## Completion Criteria

This plan is complete when:

- `benchmarks/rust-baseline/README.md` accurately describes DecentDB, SQLite,
  and DuckDB access paths and workload classes.
- The main rust-baseline workload remains runnable for DecentDB and SQLite at
  all existing scales.
- DuckDB is available for at least smoke and medium music-library runs, with
  full and huge documented as supported or explicitly skipped with reasons.
- Latency-suite JSON reports p50, p95, p99, max, mean, stddev, warmup count,
  iteration count, and ops/sec for all required read cases.
- Concurrency-suite JSON reports isolated and read-under-write p95 values for
  1, 2, 4, and 8 reader threads.
- Write-suite JSON reports durable autocommit insert, batch insert, update, and
  delete latency distributions.
- Cold-suite JSON reports same-process reopen, cold-process first query, and
  recovery reopen separately.
- The HTML report shows latest side-by-side ratios for matching engine/profile
  runs and remains backward compatible with checked-in historical result files.
- `cargo test --manifest-path benchmarks/rust-baseline/Cargo.toml` passes.
- Smoke runs pass for every implemented engine and suite.
