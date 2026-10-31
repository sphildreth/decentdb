# DecentDB rust-baseline benchmark

This benchmark is the apples-to-apples Rust runner for the music-library
workload used to compare DecentDB against SQLite and DuckDB. By default it runs
DecentDB directly through the Rust crate, without building or linking SQLite or
DuckDB, without DecentDB's optional Lua extension runtime, and without compiling
report generation or the optional diagnostic suites into the canonical binary.
Opt-in Cargo features expose those tools and comparison engines without
changing the main workload or its JSON schema.

This default restores the DecentDB-only dependency boundary used by the
pre-v2.13 historical runner. DecentDB later enabled Lua extensions in its core
crate's default feature set, but this workload does not execute extensions; the
benchmark therefore disables dependency defaults and makes Lua an explicit
diagnostic opt-in. A binary built with `extended-suites`, `lua-extensions`,
`sqlite`, `duckdb`, or `comparisons` is useful for diagnostics or cross-engine
measurements, but its linked code and resident file pages make it ineligible for
canonical `RustRaw` peak-RSS records.

The canonical binary also parses its small fixed CLI directly with the Rust
standard library, avoiding the diagnostic CLI framework's linked code in RSS
measurements. Its supported engine, profile, scale, output, database, seed,
help, and version options are unchanged. `extended-suites` opts into Clap for
the larger diagnostic command surface.

Canonical runs also omit the human-oriented per-step timing/RSS progress lines;
the versioned JSON report is the authoritative metric output. This keeps output
formatting code from becoming part of later RSS samples. Diagnostic builds with
`extended-suites` retain the detailed console progress display.

Cargo features are intentionally explicit:

- `extended-suites` adds `--benchmark`, report generation, plan-cache testing,
  and the latency, concurrency, write, and cold/recovery suites.
- `lua-extensions` enables DecentDB's optional Lua extension runtime for
  diagnostic runs.
- `sqlite` or `duckdb` adds only that comparison engine's single-scale main
  workload.
- `comparisons` enables both comparison engines and `extended-suites`,
  retaining the complete historical runner capability.

Every main-workload JSON report records the binary's optional build provenance
in `compiled_optional_features`, sorted by feature name. A canonical
`RustRaw` leader/RSS-gate run requires this field to be exactly `[]`.
Feature-enabled reports identify the linked surface explicitly; for example,
`lua-extensions` records `["lua-extensions"]`, while `comparisons` records
`["duckdb", "extended-suites", "sqlite"]`. Historical reports that predate
this field remain readable and default it to an empty list during parsing.

Current DecentDB reports also serialize
`checkpoint_durability_contract` as
`main-db-sync-before-wal-truncate-v1`. This marker identifies the durable
checkpoint ordering used by the measured engine: the main database is synced
before the WAL is truncated. Canonical leader/RSS gates require this exact
contract marker as well as an empty `compiled_optional_features` list. Older
reports without the marker remain parseable, but do not provide this explicit
durability proof.

The canonical result still serializes `latency_cases`, `concurrency_cases`,
`write_cases`, and `cold_cases`; they remain empty arrays unless an extended
suite populates them.

The SQLite and DuckDB paths exist only in this benchmark crate. They do not add
SQLite or DuckDB tests, dependencies, or comparison behavior to the DecentDB
engine core.

For the current cross-benchmark performance plan, see
`../../docs/user-guide/performance.md`. The public README charts are
driven by `cargo bench -p decentdb --bench embedded_compare` and
`data/bench_summary.json`; this rust-baseline runner is the larger diagnostic
surface for music-library totals, point lookups, joins, views, and grouped
aggregates.

## Engine access paths

**DecentDB** is called through the native Rust crate API and does not cross the
C ABI or language binding layers inside the timed loop.

**SQLite**, when built with `--features sqlite` (or `comparisons`), is called
through `rusqlite`, which is a Rust wrapper over SQLite's C API. SQLite results
therefore include the normal rusqlite/SQLite C API crossing cost.

**DuckDB** results, when built with `--features duckdb` (or `comparisons`), use
`duckdb-rs` and should be labeled as `duckdb-rs` over DuckDB's native engine.

The "raw-engine ceiling" idea applies only to DecentDB — the timings here
represent the theoretical engine ceiling that any binding could approach but
never beat. The other engines carry their respective FFI and wrapper costs in
the timed path.

## Workload class

- Historical main path: `bulk_load_then_read_only_music_library`.
- Bulk seed policy: one explicit transaction per logical seed table.
- Query policy (historical): one measured execution per query shape.
- Durability policy: DecentDB durable WAL profile, SQLite WAL FULL, DuckDB
  engine-default durability, explicit checkpoint before query timings.
- Non-goals: binding overhead, polyglot runtime overhead, KV adapter
  comparisons, non-durable write shortcuts.

## Showcase matrix

| Engine | Profile flag(s) | Label |
|---|---|---|
| DecentDB | `--profile default` | `decentdb_native_rust / decentdb_durable_wal_default / decentdb_default_low_memory` |
| DecentDB | `--profile resident-hot-read` | `decentdb_native_rust / decentdb_durable_wal_default / decentdb_resident_hot_read` |
| SQLite | (none; only `sqlite-wal-full`) | `sqlite_rusqlite_c_api / sqlite_wal_full / sqlite_default_cache` |
| SQLite | `--sqlite-profile wal-normal` (exploratory) | `sqlite_rusqlite_c_api / sqlite_wal_normal / sqlite_default_cache` |
| DuckDB | (none) | `duckdb_rs_c_api / duckdb_engine_default / duckdb_threads_1` |

All rows must be reported with profile labels. The `--profile` flag is only
valid with `--engine decentdb`.

## Scale tiers

| name | artists | albums (target) | songs cap | Runtime tier |
|---|---|---|---|---|
| smoke | 500 | 5,000 | 50,000 | Quick local sanity check |
| medium | 5,000 | 50,000 | 500,000 | Local development comparison |
| full | 50,000 | 500,000 | 5,000,000 | Release-quality raw-engine cross-check |
| huge | 250,000 | 2,500,000 | 25,000,000 | Long-running stress/showcase tier; not required for every PR |

Memory behavior is tracked in JSON and in
`docs/user-guide/performance.md`.

The default DecentDB path links the `decentdb` crate directly (path-dep against
`../../crates/decentdb`) and uses the engine's hot-path API:

- `Db::create()` to make a fresh database
- `db.transaction()` to acquire an exclusive `SqlTransaction`
- `txn.prepare(sql)` once per INSERT shape
- `prepared.execute_in(&mut txn, &[Value::..., ...])` per row
- `txn.commit()` per logical batch

The SQLite path uses `rusqlite` against the same generated workload, with
`journal_mode=WAL`, `synchronous=FULL`, and `wal_autocheckpoint=0`. Each seed
phase runs in one explicit `BEGIN IMMEDIATE` transaction. After seeding, both
engines run a measured WAL checkpoint before query timing starts: DecentDB uses
`Db::checkpoint_wal()` and SQLite uses `PRAGMA wal_checkpoint(TRUNCATE)`. Query
timing materializes every returned column before counting a row.

## Schema and queries

- `artists`, `albums`, `songs` tables with the same columns/PKs.
- 5 secondary indexes (`idx_albums_artist`, `idx_songs_album`, etc.).
- `v_artist_songs` view joining all three.
- 13 instrumented steps: `connect_open`, `schema_create`, three seed loops,
  `checkpoint_after_seed`, and seven query shapes including `COUNT(*)`,
  aggregates, by-id lookup, Top-10 artists/albums by song count, and view
  scans.

The **seed plan** uses a SplitMix64 RNG seeded with 42 (deterministic, but
distinct from .NET's `System.Random`), so the actual song counts differ
slightly across the two test families even at the same scale name. This is
intentional and unavoidable without re-implementing .NET's `Random`; the
counts are reported as `Plan: artists=… total_albums=… total_songs=…`.

## Build & run

```bash
cd /home/steven/src/github/decentdb/benchmarks/rust-baseline
# Canonical DecentDB-only build: this is the binary used for RustRaw records.
cargo build --release
./target/release/rust-baseline --engine decentdb --scale smoke
./target/release/rust-baseline --engine decentdb --scale medium
./target/release/rust-baseline --engine decentdb --scale full
./target/release/rust-baseline --engine decentdb --scale huge
./target/release/rust-baseline --engine decentdb --scale full --profile resident-hot-read

# Opt-in diagnostics/report build. This binary is not used for RustRaw RSS records.
cargo build --release --features extended-suites
./target/release/rust-baseline --engine decentdb --benchmark
./target/release/rust-baseline --engine decentdb --scale smoke --latency-suite
./target/release/rust-baseline --engine decentdb --scale smoke --concurrency-suite --writer-commits 100
./target/release/rust-baseline --engine decentdb --scale smoke --write-suite --write-iterations 100
./target/release/rust-baseline --engine decentdb --scale smoke --cold-suite
./target/release/rust-baseline --plan-cache-benchmark --out-dir ../../.tmp/rust-baseline-plan-cache
./target/release/rust-baseline --report
./target/release/rust-baseline --report --report-file /tmp/rust-baseline-report.html

# Opt-in cross-engine build. This binary is not used for RustRaw records.
cargo build --release --features comparisons
./target/release/rust-baseline --engine sqlite --benchmark
./target/release/rust-baseline --engine duckdb --benchmark
./target/release/rust-baseline --engine sqlite --scale smoke
./target/release/rust-baseline --engine duckdb --scale smoke
```

The narrower `--features sqlite` and `--features duckdb` builds expose just one
comparison engine for single-scale main-workload runs. Add
`extended-suites` when that engine also needs `--benchmark` or a diagnostic
suite. Without the corresponding feature, the runner rejects unavailable
engine and suite flags. Rebuild without any optional features before collecting
canonical DecentDB records:

```bash
cargo build --release --bin rust-baseline
./target/release/rust-baseline --engine decentdb --scale full
```

With `extended-suites`, `--benchmark` runs `smoke`, `medium`, `full`, and `huge`
in order before regenerating the report. Use a single `--scale` for an
individual comparison run, especially when the larger DuckDB tiers are not
needed.

Use `--benchmark` to run all scales in order (`smoke`, `medium`, `full`,
`huge`) for the selected engine/profile and then generate the same HTML report
as `--report`. Suite mode uses the default per-engine/per-scale database paths
and rejects `--db-path`; use single-scale mode when you need to pin an exact
database file.

With `extended-suites`, use `--plan-cache-benchmark` for the DecentDB-only
plan-cache guardrail suite. It writes a JSON report with enabled/disabled
results for repeated parameterized point-lookup preparation, one-shot literal
SQL overhead, and warm 1,000-statement churn p95/p99. This mode is separate from
the music-library comparison and is intended to prove the connection-local
plan-cache win without mixing it into SQLite comparison totals.

## Repeatable record validation

The authoritative target set is
`results/history-manifest.json`, not an ambient directory scan. It freezes 95
known DecentDB `RustRaw` / `default` music-library records and their SHA-256
digests, including intentional experimental records that had already
contributed to the literal best-recorded bar. Missing, changed, malformed, or
duplicate manifest entries fail the gate. Additional JSON files beside the
manifest are reported and ignored; they never become targets automatically.

Keep new candidate evidence outside `benchmarks/rust-baseline/results` and do
not copy it into the frozen history before evaluation. The gate rejects any
candidate whose resolved path, byte digest, or canonical report identity
duplicates a manifested record. After an independently passing result is
accepted, a later change may deliberately add it to a new manifest revision;
that must be an explicit baseline update, not a side effect of running the
gate.

The latency, concurrency, write, and cold arrays are empty in canonical
history. Those optional suites are separate regression guardrails rather than
historical record targets. SQLite, DuckDB, and `resident-hot-read` results also
remain separate profile/binding cohorts.

Use two target tables when evaluating a default-profile DecentDB candidate:

- **Raw all-history:** the best positive value recorded for each metric among
  all frozen-manifest `RustRaw` / `default` runs. This is the aspirational record
  table requested for performance work, but it spans engine, storage, runner,
  and durability eras. In particular, pre-August checkpoint times did not
  include the main-database sync now required before WAL truncation.
- **Current durability contract:** frozen runs starting with the
  2026-08-01 13:03/13:04 UTC baseline. Those are the first runs in which
  checkpoint copied pages to the main file, synced that file, and only then
  truncated the WAL. Use this cohort for a correctness-equivalent checkpoint
  and total-runtime comparison.

`scripts/agg_rust_baseline.py` prints both tables, the source file for every
target, candidate medians and ranges, and the number of candidate trials that
co-lead each target. Comparisons are inclusive: equality with the record is a
co-leading result. Each scale has 18 target rows:

- total runtime, all 13 canonical step durations, and peak RSS are
  lower-is-better;
- the three seed throughputs are higher-is-better.

The eight critical rows are total runtime, the three seed durations,
`checkpoint_after_seed`, and the three seed throughputs. Legacy 12-step runs
remain eligible for common step, throughput, and peak-RSS records, but their
partial totals cannot become a 13-step total-runtime target. Per-step RSS and
its anonymous/file components are integrity-checked but are not additional
cross-era target rows.

Database and WAL sizes are equality constraints from the first
current-durability run at each scale. They are deliberately not minimized
independently: an old uncheckpointed run can have an 8 KiB database and a large
WAL, while a checkpointed run has the opposite layout, so the two independent
minima cannot represent one valid populated database.

Every candidate must declare
`checkpoint_durability_contract = "main-db-sync-before-wal-truncate-v1"`,
`compiled_optional_features = []`, the exact ordered 13-step contract, fixed
seed/query counts, empty optional-suite arrays, internally consistent
throughput and RSS, and a checkpoint that grows or preserves the database while
shrinking the WAL to the final reported sizes. Schema-v3 candidates also record
runner-contract version 1, seed 42, the concrete process argument vector, and
the SHA-256 digest of the executable. Every measured query records a row count
and an order-insensitive, type-aware result checksum; query-specific invariants
are checked after the timed result has been fully materialized. Runner
provenance, including argument capture and executable hashing, is finalized only
after all timed work and RSS samples, so provenance instrumentation cannot warm
additional code pages before the memory metric.

### Final 9×4 record gate

Build once, stop other builds/tests/benchmarks, and freeze the candidate binary
before collecting final trials. `--require-leading` automatically requires all
four scales, exactly nine trials, at least five co-leading trials for every row,
and at least seven co-leading trials for every critical row. It also requires
every median to co-lead, exact storage equality, and provenance validation.

The current root must contain exactly `trial-01` through `trial-09`. Each trial
must contain exactly the four `smoke`, `medium`, `full`, and `huge`
directories, with one JSON result in each. A result's recorded database path
must be exactly
`<current-root>/trial-NN/<scale>/run-rust-<scale>.ddb`. Use a new evidence root;
never reuse a cell or place historical JSON under the current root.

The following protocol creates all provenance evidence under the same
`--current-root` before the first trial, then reverses scale order on alternate
trials to reduce order and thermal bias:

```bash
# Run from the repository root.
set -euo pipefail
cargo build \
  --manifest-path benchmarks/rust-baseline/Cargo.toml \
  --release \
  --bin rust-baseline

evidence_root="$PWD/.tmp/rust-baseline-final"
test ! -e "$evidence_root"
current_root="$evidence_root/trials"
binary_path="$PWD/benchmarks/rust-baseline/target/release/rust-baseline"
python scripts/capture_rust_baseline_environment.py \
  --current-root "$current_root" \
  --binary "$binary_path"

for trial in 01 02 03 04 05 06 07 08 09; do
  if ((10#$trial % 2)); then
    scales=(smoke medium full huge)
  else
    scales=(huge full medium smoke)
  fi
  for scale in "${scales[@]}"; do
    trial_dir="$current_root/trial-$trial/$scale"
    mkdir -p "$trial_dir"
    "$binary_path" \
      --engine decentdb \
      --profile default \
      --scale "$scale" \
      --out-dir "$trial_dir" \
      --db-path "$trial_dir/run-rust-$scale.ddb" \
      > "$trial_dir/run.log" 2>&1
  done
done

python scripts/agg_rust_baseline.py \
  --current-root "$current_root" \
  --history-dir benchmarks/rust-baseline/results \
  --history-manifest benchmarks/rust-baseline/results/history-manifest.json \
  --environment-manifest "$current_root/environment-manifest.json" \
  --target-cohort raw \
  --require-leading \
  | tee "$evidence_root/aggregate.txt"
```

The capture helper derives the Git HEAD revision, inventories and hashes
tracked and non-ignored untracked source files, records the canonical command,
records platform/toolchain data, hashes the frozen binary, and atomically writes
all evidence plus `environment-manifest.json` under `current_root`. It excludes
`.git`, `.tmp`, `target`, Python caches, benchmark `results` directories, and
secret-like files such as `.env`, private keys, and credentials. It records
paths, modes, sizes, and digests, never source contents or environment
variables, and refuses to run after any `trial-*` directory or candidate result
exists.

The exact canonical argument template written by the helper and required by
the gate is:

```json
[
  "{binary}",
  "--engine",
  "decentdb",
  "--profile",
  "default",
  "--scale",
  "{scale}",
  "--out-dir",
  "{trial_dir}",
  "--db-path",
  "{db_path}"
]
```

The gate hashes the compact JSON representation of that array and requires the
binary, source-state, command, and platform artifacts to retain their recorded
bytes. Each report must contain that template expanded to its absolute binary,
trial-directory, scale, and database paths; its executable digest must match the
captured binary. Do not rebuild the binary or edit the source after capture.
All 36 reports must record one engine version, unique content/identity/database
paths, identical semantic query evidence within each scale, identical fixed seed
counts, and the scale-specific database/WAL sizes.
Keep a same-machine frozen pre-change binary and alternate control and candidate
execution order when proving causation; the frozen historical JSON does not
contain equivalent machine/compiler/source fingerprints.

Do not improve a recorded number by changing the seed, scale, default profile,
sync policy, checkpoint boundary, query order, result materialization, cache
residency, or by moving work outside an existing timed step. Because each main
query is a single measured execution and RSS is sampled after each step, use
the nine-run medians for the record decision and also inspect external maximum
RSS (for example `/usr/bin/time -v`) when a change creates large transient
buffers.

### Loose exploratory aggregation

For iteration, omit both `--require-leading` and `--expected-trials`. This mode
still validates candidate contracts and manifest separation, but it does not
require the 9×4 directory matrix or environment manifest and returns success
after printing comparisons even when metrics trail:

```bash
python scripts/agg_rust_baseline.py \
  --current-root .tmp/rust-baseline-iteration \
  --history-dir benchmarks/rust-baseline/results \
  --scales smoke \
  --minimum-trials 1
```

`--expected-trials N` enables an exact `trial-01..N × requested-scales` matrix
without enabling the final record decision. `--minimum-trials` is the loose
mode's lower-bound count. `--minimum-leaders` and
`--minimum-critical-leaders` may raise leader-count requirements; the final
gate always floors them at five and seven. `--target-cohort` selects the
enforced table only with `--require-leading`; both tables are always printed.
`--history-manifest` and `--environment-manifest` override their defaults of
`<history-dir>/history-manifest.json` and
`<current-root>/environment-manifest.json`, respectively. `--scales` requires
one or more unique scale names, and the final gate requires all four.

Build with `--features extended-suites` and run the optional suites separately
at `smoke` with their documented default iteration counts, plus
`--plan-cache-benchmark`, after the canonical record gate. Combining a
write/concurrency suite with a canonical result changes the schema and final
database/WAL sizes, so that result is not a storage-size comparison against the
historical main path.

To run the full DecentDB-vs-SQLite comparison into a temporary output
directory, use the feature-linked comparison binary below. Its DecentDB result
is suitable for side-by-side timing, but not for a canonical `RustRaw` RSS
record:

```bash
cd /home/steven/src/github/decentdb/benchmarks/rust-baseline
cargo build --release --features sqlite,extended-suites
OUT="$PWD/../../.tmp/rust-baseline-compare/results"
mkdir -p "$OUT"
./target/release/rust-baseline --engine decentdb --benchmark --out-dir "$OUT"
./target/release/rust-baseline \
  --engine sqlite \
  --benchmark \
  --out-dir "$OUT" \
  --report-file "$OUT/report.html"
```

## Profiles

`--profile` applies only to `--engine decentdb`. The default profile uses
`DbConfig::default()`: durable WAL, deferred table
materialization, and paged row storage with post-commit re-deferral. It is the
low-memory profile and should remain the default historical comparison.

`--profile resident-hot-read` is a durable tuned profile for workloads that bulk
load data and immediately run read-heavy analytics on the same handle. It sets
`retain_paged_row_sources_after_commit=true`, keeping just-written paged row
sources resident after commit instead of dropping them back to the deferred set.
This is a fair profile only when reported separately from default because it
trades higher process memory for lower repeated read cost.

SQLite runs always use benchmark profile `sqlite-wal-full` and reject
DecentDB-only profiles.

## Results

JSON reports are written to
`results/<datetime>-rust-baseline-<profile>-<scale>.json` where `<datetime>` is
`YYYY-MM-DD-HHMM` (e.g., `2026-04-26-1430`). DecentDB default runs use
`default`; tuned DecentDB runs use their selected profile name; SQLite runs use
`sqlite-wal-full`. Older checked-in reports omit the profile segment and are
treated as the default profile. This timestamped naming enables historical
comparisons across multiple runs:

```
results/
├── 2026-03-24-1200-rust-baseline-full.json
├── 2026-04-26-1430-rust-baseline-default-full.json
├── 2026-06-11-1215-rust-baseline-sqlite-wal-full-full.json
└── ...
```

Each JSON report records `binding`, `benchmark_profile`, `engine_version`,
database/WAL size after the run, peak RSS, total runtime, and every
instrumented step. The `checkpoint_after_seed` step records checkpoint duration
plus WAL/database bytes before and after the checkpoint in its `extra` object.
Use `binding` to separate DecentDB (`RustRaw`) from SQLite (`SQLiteRusqlite`)
when comparing runs programmatically.

### Historical HTML report

`--report` is a **report-only** mode when used by itself: it does not run a
benchmark. When the input directory contains `history-manifest.json`, report
generation loads every manifested result and verifies its SHA-256; those runs
are labeled `verified` in the report. Additional `*rust-baseline*.json` files
present beside the manifest — typically fresh local `--benchmark` results that
have not been accepted into the canonical history yet — are still included but
labeled `provisional`, so the report always reflects the latest local work
while keeping the pinned history checksum-verified. A tampered manifested
result still fails report generation; an unreadable provisional file is
skipped with a warning instead of taking down the report. A directory without
a manifest retains the convenient ad-hoc behavior of loading all
`*rust-baseline*.json` results. Runs are grouped by scale (`smoke`,
`medium`, `full`, `huge`) and written to `results/report.html` by default.
`--benchmark` runs the suite first and then performs this report generation
step automatically.

Result JSON files are written atomically (temporary file plus rename) so an
interrupted run cannot leave a truncated result that would break later report
loading.

The generated report includes:

- overview cards summarizing run counts and latest results
- one section per scale in chronological order
- charts for total runtime, peak RSS, per-step duration trends, and seed
  throughput trends
- raw run-history tables and per-step summary tables so regressions and
  improvements are easy to spot over time

Use `--report-file <path>` with `--report` or `--benchmark` to override the
output path.
