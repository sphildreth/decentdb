# DecentDB Benchmark User Guide

Last updated: March 31, 2026

This guide explains how to use `decentdb-benchmark` as the primary macro benchmark tool for DecentDB.

> TL;DR (most common workflow)
>
> 1. Run baseline benchmark and capture printed `summary=...` path as `SUMMARY1`.
> 2. Save baseline snapshot: `baseline set --name local-dev --input SUMMARY1`.
> 3. Make code changes to optimize.
> 4. Run benchmark again with same profile/scenarios/seed and capture `summary=...` as `SUMMARY2`.
> 5. Compare: `compare --baseline-name local-dev --candidate SUMMARY2`.
> 6. Render readable output from compare artifact via `report --format text` (or `--format markdown`).
>
> Copy/paste commands (from repo root):
>
> ```bash
> # 1) baseline run
> cargo run --quiet -p decentdb-benchmark -- run \
>   --profile smoke \
>   --scenario durable_commit_single \
>   --scenario point_lookup_warm \
>   --scenario storage_efficiency \
>   --seed 12345
>
> # 2) set named baseline (replace SUMMARY1 with printed summary path)
> cargo run --quiet -p decentdb-benchmark -- baseline set \
>   --name local-dev \
>   --input SUMMARY1
>
> # 3) make code changes
>
> # 4) candidate run (same knobs as baseline)
> cargo run --quiet -p decentdb-benchmark -- run \
>   --profile smoke \
>   --scenario durable_commit_single \
>   --scenario point_lookup_warm \
>   --scenario storage_efficiency \
>   --seed 12345
>
> # 5) compare (replace SUMMARY2 with candidate summary path)
> cargo run --quiet -p decentdb-benchmark -- compare \
>   --baseline-name local-dev \
>   --candidate SUMMARY2
>
> # 6) report (replace COMPARE_JSON with printed/known compare artifact path)
> cargo run --quiet -p decentdb-benchmark -- report \
>   --compare COMPARE_JSON \
>   --format text
> ```
>
> Keep environment stable for useful comparisons: same host, same profile, same scenarios, same seed.

> TL;DR (today dashboard HTML)
>
> ```bash
> # Latest run -> HTML dashboard with all measured metric values
> cargo run --quiet -p decentdb-benchmark -- report \
>   --latest-run \
>   --format html \
>   --output build/bench/reports/today-dashboard.html
>
> # Latest compare -> HTML dashboard for regressions/improvements
> cargo run --quiet -p decentdb-benchmark -- report \
>   --latest-compare \
>   --format html \
>   --output build/bench/reports/today-compare.html
> ```
>
> Then open:
>
> ```bash
> xdg-open build/bench/reports/today-dashboard.html
> ```

> One-command script (recommended):
>
> ```bash
> ./scripts/run-and-display-benchmark-report.sh
> ```

It covers:

- Running benchmark scenarios (`run`)
- Creating and managing named baseline snapshots (`baseline set`)
- Comparing runs and ranking optimization opportunities (`compare`)
- Rendering human and agent-focused reports (`report`)
- Inspecting on-disk storage attribution (`inspect-storage`)
- Understanding artifact layout, grading, strictness, and common pitfalls

## 1. What `decentdb-benchmark` Is

`decentdb-benchmark` is a Rust CLI crate in this repository:

- crate: `crates/decentdb-benchmark`
- command: `decentdb-benchmark`
- canonical invocation during development: `cargo run -p decentdb-benchmark -- <subcommand> ...`

It is JSON-first. The source of truth is machine-readable artifacts under `build/bench/`.

Reports are renderers over those artifacts. They do not recalculate benchmark logic.

## 2. Alignment with DecentDB Product Tenets

The benchmark tool is designed to validate and track progress against the 7 core tenets defined in `design/PRD.md`. Every scenario and metric should be understood in the context of these product pillars:

### 2.1 Tenet #1: ACID Compliance is Foremost

**PRD Principle:** Data integrity is non-negotiable. The engine must survive sudden power loss, kernel panics, and process crashes without corruption.

**Benchmark Coverage:**
- `durable_commit_single` — validates single-row durable write latency and correctness
- `durable_commit_batch` — validates batch write durability and atomicity
- `recovery_reopen` — validates crash recovery and database reopen integrity
- `checkpoint` — validates checkpoint correctness and WAL truncation

**Key Metrics:** `commit_latency_us`, `recovery_time_ms`, `wal_bytes_flushed`, `checkpoint_duration_ms`

**Interpretation:** Regressions in durability scenarios are critical. A performance improvement that compromises durability is unacceptable. Always verify that durability metrics remain within acceptable bounds when optimizing write paths.

### 2.2 Tenet #2: Uncompromising Performance

**PRD Principle:** Performance must beat SQLite on all fronts. Zero-copy deserialization, lock-free snapshot isolation, optimized WAL appending.

**Benchmark Coverage:**
- `point_lookup_warm` — validates read performance with hot cache
- `point_lookup_cold` — validates read performance with cold cache
- `range_scan_warm` — validates sequential read throughput
- `read_under_write` — validates concurrent reader performance under write load

**Key Metrics:** `point_lookup_latency_us`, `range_scan_throughput_rows_per_sec`, `read_under_write_p50_us`, `read_under_write_p99_us`

**Interpretation:** These metrics directly measure competitive positioning. Use `compare` to track progress against baselines and targets. The `optimization_opportunities` ranking helps prioritize work that closes gaps to target performance.

### 2.3 Tenet #3: Minimal Disk Footprint

**PRD Principle:** File size on disk must be optimized. Explicit byte-aligned layouts, efficient B-Tree node packing.

**Benchmark Coverage:**
- `storage_efficiency` — measures bytes per row, page utilization, and overhead ratios
- `inspect-storage` command — detailed page-level attribution for any `.ddb` file

**Key Metrics:** `bytes_per_row`, `page_utilization_percent`, `overflow_page_count`, `wal_overhead_bytes`

**Interpretation:** Storage efficiency improvements compound over large datasets. Use `inspect-storage` to diagnose bloat from overflow pages, low page utilization, or WAL accumulation.

### 2.4 Tenet #4: World-Class Documentation

**PRD Principle:** Documentation must be accurate, continuously updated, and contain helpful examples.

**Benchmark Coverage:** This guide itself is part of the documentation tenet. Benchmark artifacts (`summary.json`, compare JSON) are machine-readable and can be incorporated into documentation, CI reports, and dashboards.

**Interpretation:** When adding new scenarios or metrics, update this guide and ensure the `report` command produces clear, actionable output for both human and agent audiences.

### 2.5 Tenet #5: Best-in-Class Tooling & Bindings

**PRD Principle:** DecentDB must feel native in modern tech stacks. Python, .NET, Go, Java, Node.js, Dart.

**Benchmark Coverage:** The benchmark tool itself is a reference CLI application. Binding-specific benchmarks live in `bindings/*/benchmarks/` and should use consistent patterns.

**Interpretation:** When engine changes affect binding performance, run binding-specific benchmarks in addition to core engine benchmarks. The C ABI boundary (`include/decentdb.h`) is the stable contract that bindings rely on.

### 2.6 Tenet #6: Fantastic CLI Experience

**PRD Principle:** The CLI must provide best-in-class UX with rich terminal formatting, helpful errors, and easy import/export.

**Benchmark Coverage:** The `decentdb-benchmark` CLI demonstrates good CLI practices:
- Clear subcommand structure (`run`, `compare`, `baseline`, `report`, `inspect-storage`)
- Deterministic, parseable output (`run_id=`, `summary=`, `compare_id=`)
- Helpful error messages with actionable fixes
- Multiple output formats (`text`, `markdown`, `html`)
- Audience-aware reporting (`human`, `agent_brief`)

**Interpretation:** CLI improvements should maintain these patterns. New commands should follow the same JSON-first artifact model.

### 2.7 Tenet #7: Fast Developer Feedback Loop

**PRD Principle:** CI/CD must respect developer time. PR checks under 10 minutes. Long-running tests moved to nightly.

**Benchmark Coverage:**
- `smoke` profile — designed for fast PR validation (~seconds to minutes)
- `dev` profile — balanced for local development iteration
- `nightly` profile — comprehensive for overnight CI runs

**Interpretation:**
- Use `--profile smoke` for PR checks and quick validation
- Use `--profile dev` for local development iteration
- Use `--profile nightly --release` for authoritative overnight runs
- The `--dry-run` flag enables fast plan validation without execution

**Workflow Guidance:**
```bash
# Fast PR check (under 10 minutes target)
cargo run -p decentdb-benchmark -- run --profile smoke --all

# Authoritative overnight run
cargo run -p decentdb-benchmark --release -- run --profile nightly --all
```

### 2.8 Tenet Trade-offs and Priorities

When benchmark results show tension between tenets, follow the priority order:

1. **ACID Compliance (Tenet #1)** — Never compromise durability for performance
2. **Performance (Tenet #2)** — Optimize after correctness is proven
3. **Disk Footprint (Tenet #3)** — Optimize after performance targets are met
4. **Documentation, Tooling, CLI, Feedback Loop (Tenets #4-7)** — Continuous improvement

The `compare` command's `optimization_opportunities` ranking respects this priority by weighting durability metrics higher than throughput metrics.

## 3. Prerequisites

From repo root (`/home/steven/source/decentdb`):

```bash
cargo run -p decentdb-benchmark -- --help
```

You should see commands:

- `run`
- `compare`
- `baseline`
- `report`
- `inspect-storage`

Optional but useful for analysis examples:

- `jq` (for JSON querying)

## 4. Command Quick Reference

```bash
# Run scenarios and produce run artifacts
cargo run -p decentdb-benchmark -- run --profile smoke --all

# Set a named baseline snapshot from a run summary
cargo run -p decentdb-benchmark -- baseline set --name smoke-local --input build/bench/runs/<run-id>/summary.json

# Compare candidate run against named baseline
cargo run -p decentdb-benchmark -- compare --baseline-name smoke-local --candidate build/bench/runs/<run-id>/summary.json

# Render report from compare artifact
cargo run -p decentdb-benchmark -- report --compare build/bench/compares/<compare-id>.json --format markdown

# Render HTML dashboard report to a file
cargo run -p decentdb-benchmark -- report --latest-run --format html --output build/bench/reports/today-dashboard.html

# Render compact agent-focused text report
cargo run -p decentdb-benchmark -- report --compare build/bench/compares/<compare-id>.json --format text --audience agent_brief

# Inspect an existing .ddb file
cargo run -p decentdb-benchmark -- inspect-storage --db-path /path/to/file.ddb
```

## 5. Artifact Model and Directory Layout

`decentdb-benchmark` uses two roots:

- scratch: `.tmp/decentdb-benchmark/`
- retained artifacts: `build/bench/`

Defaults can be overridden on `run` with:

- `--scratch-root <path>`
- `--artifact-root <path>`

### 5.1 Retained artifact layout

Typical layout:

```text
build/bench/
  runs/
    <run-id>/
      manifest.json
      summary.json
      scenarios/
        <scenario-id>.json
      artifacts/
        <scenario-id>/
          <retained-files-from-scenario>
  baselines/
    <baseline-name>.json
  compares/
    <compare-id>.json
```

### 5.2 Real run example

```text
build/bench/runs/unix-1774818065252-smoke-38bf1b9/
  manifest.json
  summary.json
  scenarios/durable_commit_single.json
  artifacts/
```

## 6. `run` Command

Help:

```bash
cargo run -p decentdb-benchmark -- run --help
```

### 6.1 Scenarios

Implemented scenario IDs:

- `durable_commit_single`
- `durable_commit_batch`
- `point_lookup_warm`
- `point_lookup_cold`
- `range_scan_warm`
- `checkpoint`
- `recovery_reopen`
- `read_under_write`
- `storage_efficiency`
- `memory_footprint`

You can select one or many scenarios using repeated `--scenario` flags, or all scenarios with `--all`.

### 6.2 Profile presets

`--profile` options:

- `smoke`
- `dev`
- `nightly`
- `custom`

Default: `dev`

Preset defaults:

### smoke

- rows: `10000`
- point_reads: `5000`
- range_scan_rows: `128`
- range_scans: `400`
- durable_commits: `500`
- batch_size: `25`
- cold_batches: `5`
- reader_threads: `2`
- writer_ops: `300`
- warmup_ops: `100`
- trials: `1`
- seed: `42`

### dev

- rows: `100000`
- point_reads: `25000`
- range_scan_rows: `256`
- range_scans: `1000`
- durable_commits: `2500`
- batch_size: `50`
- cold_batches: `8`
- reader_threads: `4`
- writer_ops: `1500`
- warmup_ops: `250`
- trials: `2`
- seed: `42`

### nightly

- rows: `1000000`
- point_reads: `200000`
- range_scan_rows: `512`
- range_scans: `8000`
- durable_commits: `15000`
- batch_size: `100`
- cold_batches: `16`
- reader_threads: `8`
- writer_ops: `12000`
- warmup_ops: `1000`
- trials: `3`
- seed: `42`

### custom

`custom` starts from dev-like defaults but requires at least one override.

Valid overrides:

- `--rows`
- `--point-reads`
- `--range-scan-rows`
- `--range-scans`
- `--durable-commits`
- `--batch-size`
- `--cold-batches`
- `--reader-threads`
- `--writer-ops`
- `--warmup-ops`
- `--trials`
- `--seed`

All numeric overrides except seed must be `> 0`.

### 6.3 `run` examples

### Example A: smoke all scenarios

```bash
cargo run -p decentdb-benchmark -- run --profile smoke --all
```

### Example B: one scenario

```bash
cargo run -p decentdb-benchmark -- run --profile smoke --scenario durable_commit_single
```

### Example C: multiple scenarios

```bash
cargo run -p decentdb-benchmark -- run \
  --profile dev \
  --scenario point_lookup_warm \
  --scenario range_scan_warm \
  --scenario read_under_write
```

### Example D: custom profile

```bash
cargo run -p decentdb-benchmark -- run \
  --profile custom \
  --rows 250000 \
  --point-reads 80000 \
  --range-scans 2000 \
  --trials 2 \
  --scenario point_lookup_warm
```

### Example E: deterministic seed override

```bash
cargo run -p decentdb-benchmark -- run \
  --profile smoke \
  --seed 20260329 \
  --scenario point_lookup_warm
```

### Example F: dry run (plan/paths validation only)

```bash
cargo run -p decentdb-benchmark -- run \
  --profile custom \
  --rows 20000 \
  --trials 1 \
  --scenario point_lookup_warm \
  --scenario range_scan_warm \
  --dry-run
```

Dry-run summary has:

- `status: "dry_run"`
- scenarios with `status: "skipped"`
- warning: `"dry run did not execute scenarios"`

### Example G: custom artifact roots

```bash
cargo run -p decentdb-benchmark -- run \
  --profile smoke \
  --all \
  --scratch-root .tmp/my-bench-scratch \
  --artifact-root build/my-bench-artifacts
```

### Example H: release build execution

Use `--release` for performance-significant runs:

```bash
cargo run -p decentdb-benchmark --release -- run --profile nightly --all
```

### 6.4 `run` output behavior

On success, command prints:

- `run_id=<id>`
- `manifest=<path>`
- `summary=<path>`

When targets are available, it also prints grade information:

- `grade=<red|yellow|green|gold|elite>` or
- `grade=partial` when scope is partial

If any scenario fails, `run` still writes artifacts but exits with an error and summary status reflects incomplete execution.

### 6.5 Key run artifact files

### `manifest.json`

Contains:

- run identity and start time
- selected scenarios
- resolved profile values
- command line used
- important paths
- environment capture (build profile, OS, arch, git SHA/branch, rustc, etc.)

### `summary.json`

Contains:

- top-level run status
- scenario-level status and headline metrics
- warnings
- target assessment object when grading ran

### `scenarios/<scenario>.json`

Contains detailed metrics and context for one scenario:

- `metrics` map (full metrics, not just headline)
- `warnings`, `notes`
- scale values
- histogram summary (if applicable)
- VFS stats (if collected)
- retained artifact paths

## 7. Target Grading (`benchmarks/targets.toml`)

By default, runs are assessed against `benchmarks/targets.toml`.

Important metadata fields in that file:

- `authoritative_build = "release"`
- `authoritative_benchmark_profile = "nightly"`
- `authoritative_host_class = "linux_x86_64_local_ssd"`

### 7.1 Grading statuses

Per-metric statuses include:

- `below_floor`
- `below_target`
- `target_met`
- `stretch_met`
- plus missing/mismatch statuses when not gradeable

Run-level grade can be:

- `red`
- `yellow`
- `green`
- `gold`
- `elite`
- or `null` with `scope: "partial"`

`scope: "partial"` means not enough target metrics were present in the run to produce a complete grade.

### 7.2 Example: inspect assessment quickly

```bash
jq '.target_assessment | {scope, overall_grade, matched_metrics, total_metrics}' \
  build/bench/runs/<run-id>/summary.json
```

```bash
jq '.target_assessment.metrics[] | select(.status == "below_floor" or .status == "below_target") | {target_id, status, current, target, floor, likely_owners}' \
  build/bench/runs/<run-id>/summary.json
```

## 8. `baseline set` Command

Help:

```bash
cargo run -p decentdb-benchmark -- baseline set --help
```

Creates or replaces a local named snapshot under:

- `build/bench/baselines/<name>.json`

### 8.1 Baseline naming rules

Name must match:

- `[A-Za-z0-9._-]+`

Examples:

- `main-nightly-linux`
- `smoke-local`
- `release_2026_03_29`

### 8.2 Baseline examples

```bash
cargo run -p decentdb-benchmark -- baseline set \
  --name smoke-local \
  --input build/bench/runs/<run-id>/summary.json
```

```bash
cargo run -p decentdb-benchmark -- baseline set \
  --name main-nightly-linux \
  --input build/bench/runs/<nightly-run-id>/summary.json
```

Overwrite behavior is explicit and deterministic: running `baseline set` again with the same name replaces that JSON snapshot file.

### 8.3 List baselines

```bash
ls -1 build/bench/baselines
```

## 9. `compare` Command

Help:

```bash
cargo run -p decentdb-benchmark -- compare --help
```

Compares candidate run summary against either:

- explicit baseline summary (`--baseline <summary.json>`), or
- named baseline snapshot (`--baseline-name <name>`)

Writes output compare artifact under:

- `build/bench/compares/<compare-id>.json`

Also prints full compare JSON to stdout.

### 9.1 Compare examples

### Example A: compare against named baseline

```bash
cargo run -p decentdb-benchmark -- compare \
  --baseline-name smoke-local \
  --candidate build/bench/runs/<candidate-run-id>/summary.json
```

### Example B: compare against explicit baseline summary

```bash
cargo run -p decentdb-benchmark -- compare \
  --baseline build/bench/runs/<baseline-run-id>/summary.json \
  --candidate build/bench/runs/<candidate-run-id>/summary.json
```

### Example C: custom target file and artifact root

```bash
cargo run -p decentdb-benchmark -- compare \
  --baseline-name main-nightly-linux \
  --candidate build/bench/runs/<candidate-run-id>/summary.json \
  --targets benchmarks/targets.toml \
  --artifact-root build/bench
```

### 9.2 Compare output model

Top-level sections include:

- metadata (`compare_id`, schema version, source paths)
- context (`candidate` and `baseline` run context)
- strictness/trust block
- totals
- `metrics[]` detailed per-metric comparison rows
- `top_regressions[]`
- `top_improvements[]`
- `optimization_opportunities[]`
- storage compare block (if available)
- warnings

### 9.3 Metric matching and direction

Metrics are matched by:

- `scenario + metric`

Metric direction comes from targets metadata:

- `smaller_is_better`
- `larger_is_better`

### 9.4 Noise-band model

For target-backed metrics with candidate and baseline values:

- `noise_band = max(absolute_threshold, relative_threshold * abs(baseline_value))`

Default per-metric thresholds are explicit in code (`compare.rs`) for key KPI metrics.
Fallback defaults are:

- absolute: `0.0`
- relative: `0.10`

Metric status can be:

- `improvement`
- `regression`
- `unchanged_within_noise`
- `missing_metric`
- `missing_target_metadata`

### 9.5 Strictness and context safety

Compare always emits output, even if context differs, but marks trust explicitly.

`strictness` includes:

- `strict`
- `meaningful`
- `incompatible_context`
- `candidate_authoritative`
- `baseline_authoritative`
- `comparison_authoritative`
- reasons list
- expected authoritative context from targets metadata

Current strict checks include profile/build/os/arch/status context compatibility.

### 9.6 Optimization opportunities ranking

`optimization_opportunities[]` includes, for each ranked item:

- scenario and metric
- current value
- baseline value
- target value
- direction
- delta percent
- status relative to noise
- priority score
- likely owners
- score components

Score is pragmatic and transparent. It combines:

- regression beyond noise
- target gap ratio
- metric weight
- priority/signature boost

### 9.7 Real compare examples from local artifacts

Named baseline compare:

```bash
cargo run -p decentdb-benchmark -- compare \
  --baseline-name smoke-durable-local \
  --candidate build/bench/runs/unix-1774818065252-smoke-38bf1b9/summary.json
```

This produced:

- compare artifact: `build/bench/compares/unix-1774818074641-unix-1774818065252-smoke-38bf1b9-vs-unix-1774818061241-smoke-38bf1b9.json`
- strictness: `strict=true`, `meaningful=true`, `comparison_authoritative=false`
- non-empty `optimization_opportunities`

Cross-context compare example (dry-run candidate):

```bash
cargo run -p decentdb-benchmark -- compare \
  --baseline-name smoke-durable-local \
  --candidate build/bench/runs/unix-1774818273060-custom-1f1877a/summary.json
```

This produced strictness warnings such as:

- `comparison context mismatch: profile candidate=custom baseline=smoke`
- `comparison input is not a passed run: candidate.status=dry_run`

and `strict=false`, `meaningful=false`.

### 9.8 Query compare JSON with `jq`

Top-level strictness and totals:

```bash
jq '{compare_id, strictness, totals}' build/bench/compares/<compare-id>.json
```

Top 5 optimization opportunities:

```bash
jq '.optimization_opportunities[:5] | map({metric_id, priority_score, delta_percent, status_relative_to_noise, likely_owners})' \
  build/bench/compares/<compare-id>.json
```

Top regressions:

```bash
jq '.top_regressions | map({metric_id, delta_percent, current_value, baseline_value})' \
  build/bench/compares/<compare-id>.json
```

Only metrics that are true regressions:

```bash
jq '.metrics[] | select(.status == "regression") | {metric_id, current_value, baseline_value, directional_delta_percent, noise_band}' \
  build/bench/compares/<compare-id>.json
```

## 10. `report` Command

Help:

```bash
cargo run -p decentdb-benchmark -- report --help
```

`report` renders either:

- run summary (`--input` or `--latest-run`), or
- compare artifact (`--compare` or `--latest-compare`)

Input source rules:

- choose exactly one run source or compare source
- do not combine run and compare sources in one command

Latest discovery:

- `--latest-run` picks newest `summary.json` under `build/bench/runs/`
- `--latest-compare` picks newest compare JSON under `build/bench/compares/`
- `--artifact-root` overrides `build/bench` for latest discovery

Formats:

- `--format markdown`
- `--format text`
- `--format html`

Audiences:

- `--audience human` (default)
- `--audience agent_brief`

Output behavior:

- print to stdout by default
- use `--output <path>` to write report file directly

### 10.1 Report examples from run summary

Markdown snapshot:

```bash
cargo run -p decentdb-benchmark -- report \
  --input build/bench/runs/<run-id>/summary.json \
  --format markdown
```

Text snapshot:

```bash
cargo run -p decentdb-benchmark -- report \
  --input build/bench/runs/<run-id>/summary.json \
  --format text
```

HTML dashboard from explicit run:

```bash
cargo run -p decentdb-benchmark -- report \
  --input build/bench/runs/<run-id>/summary.json \
  --format html \
  --output build/bench/reports/run-dashboard.html
```

HTML dashboard from latest run (recommended):

```bash
cargo run -p decentdb-benchmark -- report \
  --latest-run \
  --format html \
  --output build/bench/reports/today-dashboard.html
```

Agent brief from run summary:

```bash
cargo run -p decentdb-benchmark -- report \
  --input build/bench/runs/<run-id>/summary.json \
  --format text \
  --audience agent_brief
```

### 10.2 Report examples from compare artifact

Markdown human report:

```bash
cargo run -p decentdb-benchmark -- report \
  --compare build/bench/compares/<compare-id>.json \
  --format markdown
```

Text human report:

```bash
cargo run -p decentdb-benchmark -- report \
  --compare build/bench/compares/<compare-id>.json \
  --format text
```

HTML compare dashboard from explicit artifact:

```bash
cargo run -p decentdb-benchmark -- report \
  --compare build/bench/compares/<compare-id>.json \
  --format html \
  --output build/bench/reports/compare-dashboard.html
```

HTML compare dashboard from latest compare (recommended):

```bash
cargo run -p decentdb-benchmark -- report \
  --latest-compare \
  --format html \
  --output build/bench/reports/today-compare.html
```

Text agent brief:

```bash
cargo run -p decentdb-benchmark -- report \
  --compare build/bench/compares/<compare-id>.json \
  --format text \
  --audience agent_brief
```

Markdown agent brief:

```bash
cargo run -p decentdb-benchmark -- report \
  --compare build/bench/compares/<compare-id>.json \
  --format markdown \
  --audience agent_brief
```

### Save report to file

```bash
cargo run -p decentdb-benchmark -- report \
  --compare build/bench/compares/<compare-id>.json \
  --format markdown \
  --output build/bench/compares/<compare-id>.md
```

One-command "where are we today?" dashboard:

```bash
cargo run --quiet -p decentdb-benchmark -- report \
  --latest-run \
  --format html \
  --output build/bench/reports/today-dashboard.html

xdg-open build/bench/reports/today-dashboard.html
```

## 11. `inspect-storage` Command

Help:

```bash
cargo run -p decentdb-benchmark -- inspect-storage --help
```

Purpose:

- inspect an existing `.ddb` file
- produce storage/page attribution JSON

### Example A: print JSON to stdout

```bash
cargo run -p decentdb-benchmark -- inspect-storage --db-path /path/to/my.ddb
```

### Example B: write JSON to file

```bash
cargo run -p decentdb-benchmark -- inspect-storage \
  --db-path /path/to/my.ddb \
  --output build/bench/storage/my-db-inspection.json
```

### Example C: inspect a benchmark trial database

```bash
cargo run -p decentdb-benchmark -- inspect-storage \
  --db-path .tmp/decentdb-benchmark/<run-id>/durable_commit_single/trial-1/durable_commit_single.ddb
```

### 11.1 Inspection output highlights

Top-level fields include:

- db and wal paths
- page size and page counts
- byte attribution categories
- overflow breakdown
- attribution precision notes
- warnings
- per-table overflow usage

Useful quick query:

```bash
jq '{page_size, page_count, db_file_bytes, wal_file_bytes, page_counts, bytes, warnings}' \
  build/bench/storage/my-db-inspection.json
```

## 12. Recommended End-to-End Workflows

### 12.1 Fast local dev loop (single scenario)

```bash
# 1) run scenario
cargo run -p decentdb-benchmark -- run --profile smoke --scenario point_lookup_warm

# 2) set baseline once
cargo run -p decentdb-benchmark -- baseline set --name smoke-point --input build/bench/runs/<baseline-run>/summary.json

# 3) re-run after code change
cargo run -p decentdb-benchmark -- run --profile smoke --scenario point_lookup_warm

# 4) compare
cargo run -p decentdb-benchmark -- compare --baseline-name smoke-point --candidate build/bench/runs/<new-run>/summary.json

# 5) report
cargo run -p decentdb-benchmark -- report --compare build/bench/compares/<compare-id>.json --format text --audience agent_brief
```

### 12.2 Broader PR validation loop

```bash
# candidate run
cargo run -p decentdb-benchmark -- run --profile dev --all

# compare against named baseline
cargo run -p decentdb-benchmark -- compare --baseline-name main-dev-linux --candidate build/bench/runs/<candidate-run>/summary.json

# generate markdown report for PR comment
cargo run -p decentdb-benchmark -- report --compare build/bench/compares/<compare-id>.json --format markdown > /tmp/bench-report.md
```

### 12.3 Authoritative release-style run

```bash
cargo run -p decentdb-benchmark --release -- run --profile nightly --all
```

Then compare to a release/nightly baseline that matches host class.

## 13. Common Errors and Fixes

### 13.1 `profile=custom requires at least one override`

Cause:

- used `--profile custom` without any override flag.

Fix:

```bash
cargo run -p decentdb-benchmark -- run --profile custom --rows 20000 --scenario point_lookup_warm
```

### 13.2 `provide either --baseline or --baseline-name, not both`

Cause:

- both compare baseline inputs were passed.

Fix:

- choose exactly one baseline source.

### 13.3 `missing baseline input; provide --baseline or --baseline-name`

Cause:

- compare was invoked without baseline.

Fix:

- pass `--baseline` or `--baseline-name`.

### 13.4 baseline name validation error

Cause:

- invalid characters in `--name`.

Fix:

- use only letters, digits, `.`, `_`, `-`.

### 13.5 report input errors

Examples:

- `choose one run input (--input or --latest-run) and one compare input (--compare or --latest-compare)`
- `report requires exactly one source: --input/--latest-run or --compare/--latest-compare`

Fix:

- pass exactly one of: `--input`, `--latest-run`, `--compare`, `--latest-compare`.

### 13.6 Compare says strict=false

Likely causes:

- profile mismatch between candidate and baseline
- build profile mismatch
- OS/arch mismatch
- one input run status is not `passed`

Action:

- inspect `strictness.reasons[]` in compare JSON
- rerun candidate in matching context for meaningful comparison

### 13.7 Partial grading (`grade=partial`)

Cause:

- not all target metrics were available in this run.

Typical example:

- running one scenario only.

Action:

- run a broader scenario set (often `--all`) for complete grading scope.

## 14. Practical Automation Snippets

### 14.1 Capture run id from command output

```bash
RUN_ID=$(cargo run -p decentdb-benchmark -- run --profile smoke --scenario durable_commit_single \
  | awk -F= '/^run_id=/{print $2}')

echo "$RUN_ID"
```

### 14.2 Set baseline from latest run directory

```bash
LATEST_RUN=$(ls -1 build/bench/runs | tail -n 1)
cargo run -p decentdb-benchmark -- baseline set \
  --name smoke-latest \
  --input "build/bench/runs/${LATEST_RUN}/summary.json"
```

### 14.3 Compare latest run against named baseline and capture compare path

```bash
COMPARE_JSON=$(cargo run -p decentdb-benchmark -- compare \
  --baseline-name smoke-latest \
  --candidate "build/bench/runs/${LATEST_RUN}/summary.json" \
  | jq -r '.output_path')

echo "$COMPARE_JSON"
```

### 14.4 Emit compact agent brief report in CI logs

```bash
cargo run -p decentdb-benchmark -- report \
  --compare "$COMPARE_JSON" \
  --format text \
  --audience agent_brief
```

## 15. Example Session (Copy/Paste)

```bash
# run candidate
cargo run -p decentdb-benchmark -- run --profile smoke --scenario durable_commit_single

# assume this printed run_id=unix-1774818065252-smoke-38bf1b9

# baseline from previous run
cargo run -p decentdb-benchmark -- baseline set \
  --name smoke-durable-local \
  --input build/bench/runs/unix-1774818061241-smoke-38bf1b9/summary.json

# compare
cargo run -p decentdb-benchmark -- compare \
  --baseline-name smoke-durable-local \
  --candidate build/bench/runs/unix-1774818065252-smoke-38bf1b9/summary.json

# render reports
cargo run -p decentdb-benchmark -- report \
  --compare build/bench/compares/unix-1774818074641-unix-1774818065252-smoke-38bf1b9-vs-unix-1774818061241-smoke-38bf1b9.json \
  --format markdown

cargo run -p decentdb-benchmark -- report \
  --compare build/bench/compares/unix-1774818074641-unix-1774818065252-smoke-38bf1b9-vs-unix-1774818061241-smoke-38bf1b9.json \
  --format text --audience agent_brief
```

## 16. Advanced Notes

- `report` is intentionally a renderer; benchmark logic lives in `run` and `compare` artifacts.
- Compare is designed to be useful for both humans and coding agents directly from structured JSON.
- Cross-context compare is permitted, but trust flags and warnings indicate whether the comparison is meaningful.
- For optimization planning, prefer consuming compare JSON directly rather than scraping markdown.

## 17. FAQ

### Q: Should I run with `--release`?

For serious performance tracking, yes. Targets metadata expects release/nightly authoritative context.

### Q: Can I compare runs from different hosts?

Yes, technically. But strictness may drop and warnings will flag context mismatch. Treat such compares as directional only.

### Q: Why are opportunities present even when there are no regressions?

Opportunity ranking considers both regression beyond noise and target gap. A metric can be stable vs baseline but still far from target, making it a valid optimization candidate.

### Q: What if a metric is not in `targets.toml`?

It appears with `status = "missing_target_metadata"` in compare output.

### Q: How do I keep baselines local and deterministic?

Use `baseline set` snapshots in `build/bench/baselines/`. They are explicit JSON snapshots, not moving links.
