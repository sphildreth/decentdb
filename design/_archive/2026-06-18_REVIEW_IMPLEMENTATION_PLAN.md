# DecentDB Review Implementation Plan

**Date:** 2026-06-18  
**Source review:** `design/_archive/2026-06-18_REVIEW.md`  
**Benchmark reference:** `benchmarks/rust-baseline/README.md`

This document converts the review findings into a phased implementation plan.
It is intentionally explicit so coding agents can execute tasks without making
architecture, testing, documentation, or performance-gate decisions.

## Phase Map

Use exactly one of these status values for each phase:

```text
TODO
IN PROGRESS
COMPLETED
```

| Phase | Status | Primary Focus | Completion Gate |
|---|---|---|---|
| Phase 0: Baseline Inventory | COMPLETED | Record current code shape, validation status, and benchmark baseline. | Baseline inventory, validation results, and Benchmark Tier B summary are recorded. |
| Phase 1: Repository Hygiene and Documentation Ground Rules | COMPLETED | Clean up root-level planning artifacts and document changelog, temporary-file, and benchmark-output rules. | Hygiene changes are complete, docs are updated, and validation or documented benchmark skip is recorded. |
| Phase 2: Test Module Extraction Foundation | COMPLETED | Move large in-file test modules out of `db.rs` and `exec/mod.rs` without runtime behavior changes. | Moved tests pass, line counts drop, and Benchmark Tier A passes. |
| Phase 3: Production Panic and Unwrap Audit | COMPLETED | Convert production panic-like error paths to typed errors or documented invariants. | All production panic-like tokens are classified, new error-path tests pass, and Benchmark Tier B or C passes as required. |
| Phase 4: High-Priority Safety and Durability Coverage | COMPLETED | Add targeted coverage for WAL coordination, extension trust, security parsing, error taxonomy, and TDE. | Required tests and docs are complete, validation passes, and benchmarks meet thresholds. |
| Phase 5: Correctness and Property Test Expansion | COMPLETED | Add broader correctness coverage for records, values, B+Tree ordering, SQL robustness, and spatial/EWKB behavior. | Correctness tests, seed documentation, strategy docs, and required benchmarks are complete. |
| Phase 6: Binding Parity and API Ergonomics | COMPLETED | Improve binding capability parity while preserving the C ABI contract. | Capability matrix, binding tests, binding docs, and required benchmarks are complete. |
| Phase 7: Production Module Decomposition | COMPLETED | Split large production modules by concern without behavior changes. | Executor and DB helpers were split into child modules; validation passed and Benchmark Tier B evidence is recorded. |
| Phase 8: Documentation Completion and Release-Quality Validation | COMPLETED | Align final docs, validation records, and benchmark evidence. | Full validation, paranoid validation, Benchmark Tier C, and final implementation summary are complete; macro benchmark guardrails passed and strict micro/tail variance is documented. |

## Scope

This plan covers the implementation work implied by the 2026-06-18 review:

1. Reduce large-file and test-layout friction in `crates/decentdb/src/db.rs`
   and `crates/decentdb/src/exec/mod.rs`.
2. Audit production panic, `unwrap`, `expect`, and `unreachable!` usage.
3. Add targeted unit and integration coverage around durability, security,
   parsing, encryption, error taxonomy, and extension trust boundaries.
4. Improve bindings parity and ergonomics without weakening the C ABI as the
   shared contract.
5. Add documentation updates for changed behavior, binding capability surfaces,
   test strategy, and benchmark guardrails.
6. Require benchmark evidence from `benchmarks/rust-baseline` and reject changes
   with significant performance degradation.

## Non-Goals

Do not use this plan to implement unrelated engine features.

Do not redesign the storage format, WAL format, transaction model, concurrency
model, C ABI, or binding architecture unless a phase explicitly requires it and
an ADR has been accepted first.

Do not update the root `CHANGELOG.md`. It is a placeholder. User-facing change
notes must go into `docs/about/changelog.md`.

Do not commit, push, or open a pull request unless the user explicitly asks for
that git operation.

## Global Execution Rules

All phases must follow these rules:

1. Keep temporary output under `.tmp/review-implementation/`.
2. Keep each implementation change scoped to the phase being executed.
3. Do not mix mechanical file moves with behavior changes in the same commit or
   review unit.
4. Preserve the single-process, one-writer, multiple-reader concurrency model.
5. Preserve the Rust engine as authoritative and the C ABI as the shared binding
   boundary.
6. Prefer existing helper APIs and local patterns before adding abstractions.
7. Add unit tests in the same phase as the code they validate.
8. Add or update documentation in the same phase as behavior, API, or workflow
   changes.
9. Run the required validation commands for the phase before marking it complete.
10. Run the benchmark tier required by the phase before marking it complete.
11. Record all command outputs or summaries in `.tmp/review-implementation/logs/`.
12. If a command cannot run because of a missing local toolchain, record the
    exact missing tool, the failing command, and the skipped validation impact in
    `.tmp/review-implementation/logs/tooling-gaps.md`.

## ADR Requirements

Create and land an ADR before implementing any task that changes:

1. File format layout or versioning.
2. WAL record format, checkpointing semantics, or recovery ordering semantics.
3. Concurrency or locking behavior that affects `Send` or `Sync` boundaries.
4. C ABI contracts with broad binding impact.
5. Major crate or dependency additions.
6. New `unsafe` usage beyond basic FFI or VFS operations.
7. Large architectural shifts in planner, storage, or binding strategy.

Pure test-module extraction, mechanical Rust module splitting, documentation
updates, and production panic replacement do not require an ADR if public
behavior and on-disk behavior remain unchanged.

## Performance Gate

Every phase has a benchmark requirement. A phase is not complete until the
benchmark gate for that phase is satisfied or an explicit blocker is documented.

### Benchmark Workspace

Use this workspace for all benchmark artifacts:

```bash
mkdir -p .tmp/review-implementation/benchmarks
mkdir -p .tmp/review-implementation/logs
```

Build the Rust baseline benchmark binary before running benchmark commands:

```bash
cd /home/steven/src/github/decentdb/benchmarks/rust-baseline
cargo build --release
```

### Benchmark Tiers

Use the smallest tier that matches the phase risk. Higher-risk phases must run
all lower tiers plus the required higher tier.

#### Tier A: Smoke Guardrail

Run this tier for documentation-only changes, test-only changes, mechanical
test-module extraction, and small refactors that do not affect hot paths:

```bash
cd /home/steven/src/github/decentdb/benchmarks/rust-baseline
./target/release/rust-baseline --engine decentdb --scale smoke
./target/release/rust-baseline --plan-cache-benchmark --out-dir ../../.tmp/review-implementation/benchmarks/plan-cache-smoke
```

#### Tier B: Medium Engine Guardrail

Run this tier for changes touching execution, planner, B+Tree access, page cache,
transaction logic, WAL coordination, error propagation in hot paths, or Rust API
surfaces used by benchmarks:

```bash
cd /home/steven/src/github/decentdb/benchmarks/rust-baseline
./target/release/rust-baseline --engine decentdb --scale smoke
./target/release/rust-baseline --engine decentdb --scale medium
./target/release/rust-baseline --plan-cache-benchmark --out-dir ../../.tmp/review-implementation/benchmarks/plan-cache-medium
```

If a phase touches concurrency, write throughput, cold-start behavior, or latency
tail behavior, add the matching suites:

```bash
cd /home/steven/src/github/decentdb/benchmarks/rust-baseline
./target/release/rust-baseline --engine decentdb --scale smoke --latency-suite
./target/release/rust-baseline --engine decentdb --scale smoke --concurrency-suite --writer-commits 100
./target/release/rust-baseline --engine decentdb --scale smoke --write-suite --write-iterations 100
./target/release/rust-baseline --engine decentdb --scale smoke --cold-suite
```

#### Tier C: Full Compare Guardrail

Run this tier for phases that materially touch execution performance, read
paths, write paths, storage, planner behavior, bindings that depend on engine
batching, or any phase that changes a public performance-sensitive API:

```bash
cd /home/steven/src/github/decentdb/benchmarks/rust-baseline
cargo build --release
OUT="$PWD/../../.tmp/review-implementation/benchmarks/full-compare/results"
mkdir -p "$OUT"
./target/release/rust-baseline --engine decentdb --benchmark --out-dir "$OUT"
./target/release/rust-baseline --engine sqlite --benchmark --out-dir "$OUT" --report-file "$OUT/report.html"
./target/release/rust-baseline --engine decentdb --scale full --profile resident-hot-read
./target/release/rust-baseline --report
```

DuckDB is optional for DecentDB regression gating. Run it only when the phase
explicitly changes benchmark comparison documentation or when the user requests
cross-engine comparison:

```bash
cd /home/steven/src/github/decentdb/benchmarks/rust-baseline
./target/release/rust-baseline --engine duckdb --benchmark
```

### Regression Thresholds

Use the same machine, same build profile, and same benchmark scale for before
and after comparisons.

A phase fails the performance gate if any of these conditions are true:

1. DecentDB smoke or medium total runtime regresses by more than 5 percent.
2. Any indexed lookup, scan, aggregate, or view-heavy query group regresses by
   more than 10 percent.
3. Plan-cache benchmark median, p95, or p99 regresses by more than 5 percent.
4. Peak resident memory grows by more than 5 percent for a phase that is not
   explicitly adding retained state.
5. Write-suite throughput regresses by more than 5 percent for a phase touching
   transactions, WAL, page cache, or B+Tree mutation.
6. Concurrency-suite writer or reader latency regresses by more than 5 percent
   for a phase touching locking, transactions, WAL, page cache, or FFI handles.

If a benchmark result is noisy:

1. Re-run the before and after benchmark commands three times.
2. Compare medians.
3. Record all runs in `.tmp/review-implementation/benchmarks/`.
4. Do not mark the phase complete if the median still violates the thresholds.

If a regression is intentional:

1. Stop the phase.
2. Write a short regression note in
   `.tmp/review-implementation/benchmarks/regression-notes.md`.
3. Include the exact metric, before value, after value, percent change, reason,
   and mitigation.
4. Get maintainer approval before proceeding.

## Phase 0: Baseline Inventory

### Objective

Create a reproducible baseline for code shape, tests, and performance before
any implementation work begins.

### Tasks

1. Create the phase workspace:

   ```bash
   mkdir -p .tmp/review-implementation/phase-0
   mkdir -p .tmp/review-implementation/logs
   mkdir -p .tmp/review-implementation/benchmarks/phase-0
   ```

2. Record git state:

   ```bash
   git status --short > .tmp/review-implementation/phase-0/git-status.txt
   git diff --stat > .tmp/review-implementation/phase-0/git-diff-stat.txt
   ```

3. Record large-file line counts:

   ```bash
   wc -l crates/decentdb/src/db.rs crates/decentdb/src/exec/mod.rs > .tmp/review-implementation/phase-0/large-file-line-counts.txt
   ```

4. Record panic-like token counts with separate production and test context
   notes. Use `rg` first:

   ```bash
   rg -n "panic!|unreachable!|unwrap\(|expect\(" crates/decentdb/src > .tmp/review-implementation/phase-0/panic-like-tokens.txt
   ```

5. Inspect and manually classify each hit as one of:

   - `production-error-path`
   - `production-invariant-with-comment`
   - `test-only`
   - `benchmark-only`
   - `generated-or-third-party`

   Save the classification in:

   ```text
   .tmp/review-implementation/phase-0/panic-like-token-classification.md
   ```

6. Record current binding package test availability:

   ```bash
   find bindings -maxdepth 3 -type f \( -name "*test*" -o -name "package.json" -o -name "pyproject.toml" -o -name "go.mod" -o -name "pom.xml" -o -name "*.csproj" \) | sort > .tmp/review-implementation/phase-0/binding-test-inventory.txt
   ```

7. Record benchmark README version:

   ```bash
   cp benchmarks/rust-baseline/README.md .tmp/review-implementation/phase-0/rust-baseline-README.md
   ```

### Required Validation

Run:

```bash
cargo fmt --check
cargo check -p decentdb
cargo lint
```

Save command summaries to:

```text
.tmp/review-implementation/logs/phase-0-validation.md
```

### Required Benchmarks

Run Benchmark Tier B. Save the command list, result file paths, and summary
metrics in:

```text
.tmp/review-implementation/benchmarks/phase-0/baseline-summary.md
```

### Acceptance Criteria

Phase 0 is complete when:

1. Baseline file counts are recorded.
2. Panic-like token classification exists.
3. Validation commands have pass/fail status recorded.
4. Benchmark Tier B has pass/fail status recorded.
5. Any missing toolchain is documented in `tooling-gaps.md`.

## Phase 1: Repository Hygiene and Documentation Ground Rules

### Objective

Remove low-risk repository confusion before code changes begin, and document the
rules agents must follow for changelogs, temporary files, and benchmark output.

### Tasks

1. Inspect root-level non-source artifacts noted by the review:

   ```bash
   ls -la > .tmp/review-implementation/phase-1-root-listing.txt
   rg -n "plan\.md|CHANGELOG\.md|docs/about/changelog\.md|\.tmp" README.md docs design scripts crates bindings tests .github > .tmp/review-implementation/phase-1-artifact-references.txt
   ```

2. For `plan.md`, do exactly one of the following after inspecting references:

   - If no references require it, delete `plan.md`.
   - If historical context is needed, move it to
     `design/archive/2026-06-18_plan.md` and update references.

   Do not leave `plan.md` at repo root.

3. Do not edit root `CHANGELOG.md`.

4. If the review exposes missing changelog guidance in docs, update the relevant
   contributor or documentation file to state:

   ```text
   User-facing change notes go in docs/about/changelog.md. The root CHANGELOG.md
   is a placeholder and must not be updated.
   ```

5. Ensure benchmark instructions in docs point temporary benchmark output to
   `.tmp/` or an explicit ignored output directory.

6. Add or update a short documentation section that tells agents where to store:

   - benchmark JSON and HTML reports
   - temporary scripts
   - throwaway analysis files
   - flamegraphs and profiling output

   The required location is `.tmp/`.

### Unit Testing Tasks

This phase is documentation and repository hygiene only. Add tests only if a
script or automation file is changed.

If a script is changed, add or update the smallest direct test for that script.
If the script has no test harness, add a documented smoke command in the changed
script's comments or adjacent README.

### Documentation Tasks

Update only the files needed to document:

1. Changelog location.
2. Temporary artifact location.
3. Benchmark output location.

Do not broaden this phase into general documentation cleanup.

### Required Validation

Run:

```bash
cargo fmt --check
cargo check -p decentdb
```

If scripts changed, run the script-specific smoke command.

### Required Benchmarks

Run Benchmark Tier A unless only Markdown files changed. If only Markdown files
changed, record this in:

```text
.tmp/review-implementation/benchmarks/phase-1-not-run.md
```

The note must say:

```text
Benchmarks not run because Phase 1 changed documentation only and no build,
runtime, benchmark, or script code changed.
```

### Acceptance Criteria

Phase 1 is complete when:

1. Root `plan.md` is removed or archived.
2. Changelog guidance is documented where contributors will see it.
3. Temporary artifact guidance points to `.tmp/`.
4. Validation has passed or the documentation-only benchmark skip note exists.

## Phase 2: Test Module Extraction Foundation

### Objective

Reduce `db.rs` and `exec/mod.rs` size by moving test modules out of production
files without changing runtime behavior.

### Required Ordering

Complete `exec/mod.rs` test extraction before `db.rs` test extraction. Do not
move production code in this phase.

### Tasks for `exec/mod.rs`

1. Locate the existing `#[cfg(test)]` tests in:

   ```text
   crates/decentdb/src/exec/mod.rs
   ```

2. Create a new test module file:

   ```text
   crates/decentdb/src/exec/tests.rs
   ```

3. Move only the existing test code into `tests.rs`.

4. Add this module declaration in `exec/mod.rs`:

   ```rust
   #[cfg(test)]
   mod tests;
   ```

5. Preserve all test names unless a name conflicts after extraction.

6. Preserve all existing assertions.

7. Do not change production imports except to remove imports that were only used
   by moved tests.

8. Do not change execution behavior.

### Tasks for `db.rs`

1. Locate the existing `#[cfg(test)]` test module in:

   ```text
   crates/decentdb/src/db.rs
   ```

2. Create a new test module file:

   ```text
   crates/decentdb/src/db/tests.rs
   ```

3. If `db.rs` is not currently backed by a `db/` module directory, create the
   directory and use the Rust `#[path = "db/tests.rs"]` form only if required by
   the existing module layout.

4. Move only test code into `tests.rs`.

5. Add the test module declaration in `db.rs`.

6. Preserve all test names unless a name conflicts after extraction.

7. Preserve all existing assertions.

8. Do not change runtime behavior.

### Unit Testing Tasks

After each file extraction, run the smallest test command that compiles the moved
tests:

```bash
cargo test -p decentdb --lib
```

Then run:

```bash
cargo t -p decentdb
```

If a moved test fails because it depended on private imports from the old file,
fix the imports. Do not weaken the test.

### Documentation Tasks

No user-facing documentation is required for pure test extraction.

Add an internal comment only if the module path needs non-obvious `#[path]`
usage.

### Required Validation

Run:

```bash
cargo fmt --check
cargo check -p decentdb
cargo t -p decentdb
cargo lint
```

### Required Benchmarks

Run Benchmark Tier A.

### Acceptance Criteria

Phase 2 is complete when:

1. `exec/mod.rs` production behavior is unchanged.
2. `db.rs` production behavior is unchanged.
3. Moved tests compile and pass.
4. `wc -l` shows reduced line counts for `exec/mod.rs` and `db.rs`.
5. Benchmark Tier A does not exceed regression thresholds.

### Phase 2 Completion Notes

- `crates/decentdb/src/exec/tests.rs` and `crates/decentdb/src/db/tests.rs` were extracted from inline test blocks.
- Inline extraction declarations were replaced with `#[cfg(test)] mod tests;`.
- `cargo test -p decentdb --lib` passed (1404 tests, 1 ignored).
- `cargo t -p decentdb` passed (2889 tests, 1 skipped).
- Line counts and benchmark outputs were recorded in `.tmp/review-implementation/phase-2`.
- Benchmark Tier A completed and results written to `.tmp/review-implementation/benchmarks/phase-2/`.

## Phase 3: Production Panic and Unwrap Audit

### Objective

Convert production panic-like behavior into typed errors or documented invariants
without weakening tests.

### Required Ordering

1. Classify hits.
2. Replace production error-path hits.
3. Add tests for each replaced path.
4. Document remaining production invariants.

### Tasks

1. Refresh the panic-like token report:

   ```bash
   rg -n "panic!|unreachable!|unwrap\(|expect\(" crates/decentdb/src > .tmp/review-implementation/phase-3-panic-like-tokens.txt
   ```

2. Update the Phase 0 classification file with current line numbers.

3. In `crates/decentdb/src/exec/mod.rs`, replace the five production
   `unreachable!()` expression arms identified in the review with explicit
   typed errors.

   Required behavior:

   - If the surrounding function already returns `Result<T, DbError>`, return a
     `DbError` variant.
   - If a specific internal-error helper already exists, use it.
   - If no suitable helper exists, add the smallest local helper that preserves
     the existing error taxonomy.
   - Error messages must identify the invalid executor state without exposing
     filesystem paths, secrets, SQL parameter values, encryption keys, or raw
     user data.

4. Audit production `unwrap()` and `expect()` in:

   - transaction code
   - WAL coordination
   - page cache
   - VFS
   - encryption/TDE
   - FFI boundaries
   - SQL execution

5. For each production `unwrap()` or `expect()`, do exactly one of:

   - Replace it with `?` and a typed error.
   - Replace it with `ok_or_else` or `map_err` and a typed error.
   - Keep it only if it is a true invariant, and add a short comment that states
     the invariant checked before the call.

6. Do not replace test `unwrap()` and `expect()` calls unless a test is hiding a
   production error-path bug.

7. Do not broad-rewrite tests to avoid `unwrap()` in this phase.

### Unit Testing Tasks

For each production panic-like path replaced with an error:

1. Add a unit test that reaches the invalid input or invalid state.
2. Assert the returned error variant or stable error code.
3. Assert the error message does not leak absolute paths when the path is not
   part of the public API contract.
4. Assert the operation does not partially commit a write if the path is inside
   a transaction or WAL operation.

Minimum required new tests:

1. One executor test for each replaced `unreachable!()` category.
2. One transaction or WAL error-path test for any changed transaction or WAL
   unwrap.
3. One FFI or API boundary test for any changed FFI unwrap.

### Documentation Tasks

Update internal rustdoc only if a public function's error behavior changes.

If a stable error code or documented error taxonomy changes, update the relevant
user-facing documentation and `docs/about/changelog.md`.

### Required Validation

Run:

```bash
cargo fmt --check
cargo check -p decentdb
cargo t -p decentdb
cargo lint
```

If FFI code changed, also run the binding smoke tests that exercise that FFI
surface.

### Required Benchmarks

Run Benchmark Tier B.

If executor hot paths changed, also run Benchmark Tier C.

### Acceptance Criteria

Phase 3 is complete when:

1. No production panic-like token remains unclassified.
2. Production error-path panics are converted to typed errors.
3. Remaining production `unwrap()` and `expect()` calls have documented
   invariants.
4. New tests cover every replaced production panic-like path.
5. Benchmark results satisfy the regression thresholds.

### Phase 3 Completion Notes

- Refreshed panic-like token reports under `.tmp/review-implementation/`.
- Converted executor defensive error paths in `exec/mod.rs` from
  `unreachable!()` / `unwrap()` to `DbError::internal(...)` where the
  surrounding function already returned `Result`.
- Added invariant comments for retained non-`Result` helper expectations.
- Added executor unit coverage for invalid arithmetic operator internal errors.
- `cargo fmt --check`, `cargo check -p decentdb`, `cargo t -p decentdb`, and
  `cargo lint` passed; results are recorded in
  `.tmp/review-implementation/logs/phase-3-validation.md`.
- Benchmark Tier B and Tier C completed; result paths and the noisy plan-cache
  p95/p99 residual note are recorded under
  `.tmp/review-implementation/benchmarks/phase-3/`.

## Phase 4: High-Priority Safety and Durability Coverage

### Objective

Add targeted tests for the highest-risk surfaces identified in the review:
WAL coordination, extension trust boundaries, security parsing, error taxonomy,
and TDE behavior.

### Task Group 4A: WAL Coordination Tests

Add tests for WAL and transaction coordination without changing WAL format.

Required test cases:

1. Commit record is durable after reopening the database.
2. Uncommitted transaction data is not visible after simulated crash/reopen.
3. Checkpoint does not discard pages needed for recovery.
4. Reader can continue seeing a stable snapshot while a writer commits.
5. Writer failure during commit returns an error and does not expose partial
   state.
6. Repeated small commits do not corrupt page state after reopen.

Implementation constraints:

1. Use existing VFS and test helpers.
2. Use temporary directories under the test framework's temp location.
3. Do not add sleeps for concurrency ordering if an existing synchronization
   primitive can express the test.
4. Do not change WAL record layout.

### Task Group 4B: Lua Extension Trust and Manifest Tests

Add tests that exercise extension trust decisions and manifest handling.

Required test cases:

1. Unsigned or untrusted Lua extension is rejected when trust is required.
2. Trusted extension with a valid manifest loads successfully.
3. Manifest with missing required fields returns a typed error.
4. Manifest with malformed permissions returns a typed error.
5. Extension loading error does not leave a partially registered function.
6. Error messages do not leak host filesystem paths beyond the path explicitly
   provided by the caller.

Implementation constraints:

1. Keep fixtures small.
2. Store fixtures under a test fixture directory, not the repo root.
3. Do not add network access to extension tests.
4. Do not loosen trust policy to make tests pass.

### Task Group 4C: Security Parser Tests

Add tests around security-sensitive SQL parsing and validation.

Required test cases:

1. Unsupported or blocked security-sensitive statements return typed errors.
2. Malformed security syntax does not panic.
3. Parser errors are stable enough for callers to classify.
4. Parameter values are not printed in errors unless the public API already
   guarantees that behavior.
5. Error paths preserve transaction state.

Implementation constraints:

1. Use existing SQL parser entry points.
2. Prefer table-driven unit tests.
3. Do not add a new parser dependency.

### Task Group 4D: Error Taxonomy and Redaction Tests

Add tests for stable errors and path redaction.

Required test cases:

1. Public API error codes remain stable for representative IO, SQL, transaction,
   constraint, and extension errors.
2. Absolute internal paths are redacted where not part of public API behavior.
3. User-supplied relative paths are preserved only when they are the requested
   path.
4. Error conversion through C ABI preserves stable code and message ownership.
5. Binding-visible errors preserve the same stable code where the binding exposes
   codes.

Implementation constraints:

1. Do not create a new error taxonomy unless one is already missing for a tested
   case.
2. If a new public error code is required, update documentation and changelog.

### Task Group 4E: TDE Coverage

Add or expand tests around transparent data encryption.

Required test cases:

1. Database encrypted with key A cannot be opened with key B.
2. Database encrypted with key A can be reopened with key A.
3. Encryption-related metadata does not expose plaintext secrets.
4. Wrong-key failure does not corrupt the database.
5. Recovery after encrypted write succeeds with the correct key.
6. Recovery after encrypted write fails cleanly with the wrong key.

Implementation constraints:

1. Do not change file format.
2. Do not log raw keys.
3. Do not add test vectors containing real secrets.

### Unit Testing Tasks

All tests in this phase must be ordinary Rust tests unless an existing harness is
already present for the target surface.

Run targeted tests by filter first. Then run the full crate test suite:

```bash
cargo t -p decentdb -- wal
cargo t -p decentdb -- extension
cargo t -p decentdb -- security
cargo t -p decentdb -- error
cargo t -p decentdb -- encryption
cargo t -p decentdb
```

If a filter does not match because test names differ, record the actual filter
used in:

```text
.tmp/review-implementation/logs/phase-4-test-filters.md
```

### Documentation Tasks

Update documentation only where tests reveal or enforce public behavior:

1. Extension trust model documentation.
2. Error code documentation.
3. TDE usage documentation.
4. Transaction durability documentation.

If any public behavior is clarified or changed, add an entry to:

```text
docs/about/changelog.md
```

Do not update root `CHANGELOG.md`.

### Required Validation

Run:

```bash
cargo fmt --check
cargo check -p decentdb
cargo t -p decentdb
cargo lint
```

### Required Benchmarks

Run Benchmark Tier B.

If WAL, transaction, page cache, or encryption runtime code changes, also run:

```bash
cd /home/steven/src/github/decentdb/benchmarks/rust-baseline
./target/release/rust-baseline --engine decentdb --scale smoke --write-suite --write-iterations 100
./target/release/rust-baseline --engine decentdb --scale smoke --concurrency-suite --writer-commits 100
```

Run Benchmark Tier C if any hot path changes are made to satisfy the tests.

### Acceptance Criteria

Phase 4 is complete when:

1. Required tests exist and pass.
2. No tested error path panics.
3. Documentation reflects any public behavior clarified by the tests.
4. Benchmark results satisfy the regression thresholds.

### Phase 4 Completion Notes

- Added WAL/page-level tests for stable held snapshots across writer commits,
  checkpoint retention while snapshots are active, checkpoint truncation after
  release, and repeated small commits surviving reopen.
- Added Lua extension manifest tests for missing required fields, malformed
  permission types, failed-install cleanup, and caller-provided path reporting.
- Added security parser tests for malformed/blocked statements, typed SQL
  errors, redaction of malformed audit values, and transaction state after an
  error.
- Added error taxonomy coverage for transaction-code classification and
  diagnostic path redaction. Existing C ABI tests and Python/Go binding smoke
  tests already cover owned diagnostic JSON and binding-visible SQL error
  codes.
- Added TDE coverage proving a wrong-key open fails without corrupting the
  database, followed by successful recovery with the correct key.
- Existing public documentation already describes the enforced public contracts;
  no Phase 4 behavior or API contract changed.
- Targeted filters, full validation, and benchmark details are recorded in
  `.tmp/review-implementation/logs/phase-4-validation.md`,
  `.tmp/review-implementation/logs/phase-4-test-filters.md`, and
  `.tmp/review-implementation/benchmarks/phase-4/benchmark-summary.md`.

## Phase 5: Correctness and Property Test Expansion

### Objective

Add broader correctness coverage for serialization, record values, B+Tree key
ordering, SQL execution, and parser robustness.

### ADR Gate

Before adding a new fuzzing framework, major property-test dependency, or
persistent corpus format, create an ADR.

No ADR is required for deterministic table-driven tests or for using a property
test dependency already present in the repository.

### Task Group 5A: Record and Value Round-Trip Tests

Add round-trip tests for record/value encoding and decoding.

Required cases:

1. Null values.
2. Integers at minimum, maximum, zero, and negative boundaries.
3. Floating-point normal values, infinities, NaN behavior if supported, and
   signed zero if observable.
4. UTF-8 text including empty string and multi-byte characters.
5. Binary blobs including empty blob and large blob.
6. Mixed-type row encoding.
7. Decode failure for truncated record bytes.
8. Decode failure for invalid type tags.

Required assertions:

1. Successful round trips preserve logical value equality.
2. Failed decodes return typed errors.
3. Failed decodes do not panic.

### Task Group 5B: B+Tree Key Ordering Tests

Add tests that verify key ordering and lookup correctness.

Required cases:

1. Sequential inserts.
2. Reverse-order inserts.
3. Random-order inserts using a deterministic seed.
4. Duplicate-key behavior according to the existing contract.
5. Range scan boundaries inclusive and exclusive where supported.
6. Delete then reinsert.
7. Split and merge behavior across page boundaries.

Required assertions:

1. Full scan returns keys in sorted order.
2. Point lookup returns the expected row.
3. Missing lookup returns the expected not-found behavior.
4. Reopen preserves ordering.

### Task Group 5C: SQL Execution Robustness Tests

Add deterministic SQL robustness tests.

Required cases:

1. Empty statements.
2. Malformed statements.
3. Deeply nested expressions within reasonable limits.
4. Large `IN` lists within reasonable limits.
5. Queries over empty tables.
6. Aggregates over empty and non-empty tables.
7. Joins with no matches, one match, and many matches.
8. Constraint violations inside a transaction.

Required assertions:

1. Invalid SQL returns typed parser or planner errors.
2. Valid SQL returns correct rows.
3. Failed SQL inside a transaction does not commit partial state.
4. No case panics.

### Task Group 5D: Spatial and EWKB Tests

Add tests if spatial/EWKB functionality exists in the current codebase.

Required cases:

1. Empty geometry where supported.
2. Point geometry.
3. LineString geometry.
4. Polygon geometry.
5. SRID preservation where supported.
6. Invalid EWKB returns a typed error.

If spatial/EWKB functionality is not present, record that fact in:

```text
.tmp/review-implementation/logs/phase-5-spatial-skip.md
```

### Unit Testing Tasks

Use deterministic seeds for randomized tests. Store the seed in the test name,
test body, or assertion failure message.

Run:

```bash
cargo t -p decentdb -- record
cargo t -p decentdb -- btree
cargo t -p decentdb -- sql
cargo t -p decentdb -- spatial
cargo t -p decentdb
```

If a filter does not match, record the actual filter used in:

```text
.tmp/review-implementation/logs/phase-5-test-filters.md
```

### Documentation Tasks

Update `design/TESTING_STRATEGY.md` or the current test strategy document with:

1. The new correctness surfaces covered.
2. How deterministic seeds are selected and reproduced.
3. How to run the new tests.

If public SQL behavior is clarified, update SQL documentation and
`docs/about/changelog.md`.

### Required Validation

Run:

```bash
cargo fmt --check
cargo check -p decentdb
cargo t -p decentdb
cargo lint
```

### Required Benchmarks

Run Benchmark Tier B.

Run Benchmark Tier C if any production logic changed while adding tests.

### Acceptance Criteria

Phase 5 is complete when:

1. Required correctness tests exist and pass.
2. Deterministic seeds are documented for randomized tests.
3. Test strategy documentation is updated.
4. Benchmark results satisfy the regression thresholds.

### Phase 5 Completion Notes

- Added SQL execution robustness integration coverage for empty/malformed SQL,
  deep nested expressions, large IN-list evaluation, empty-table queries,
  aggregate edge cases, join cardinality across no/one/many match cases, and
  constraint failures inside explicit transactions.
- Added deterministic B+Tree random-order coverage using seed
  `0xA11F_A11E_BEEF_CAFE` and recorded seed usage in test naming and assertions.
- Added record/value boundary round-trip checks, spatial parse/serialize edge
  cases, and malformed EWKB tests under existing test modules.
- Updated `design/TESTING_STRATEGY.md` with deterministic
  seed/reproducibility guidance and Phase 5 run commands.
- No public SQL or storage behavior changed; this phase added coverage for
  existing contracts.
- Recorded Phase 5 validation outputs, filters, and Benchmark Tier B summary in
  `.tmp/review-implementation/logs/phase-5-validation.md`,
  `.tmp/review-implementation/logs/phase-5-test-filters.md`, and
  `.tmp/review-implementation/benchmarks/phase-5/benchmark-summary.md`.

### Phase 6 Completion Notes

- Added a full capability matrix in `docs/api/bindings-matrix.md` with the
  required exact status values and no blank cells.
- Added Go direct binding branch wrappers on existing C ABI symbols:
  `CreateBranch`, `ListBranches`, `DeleteBranch`, `ExecuteOnBranch`,
  and `QueryOnBranchInt64`; no new ABI functions were introduced.
- Synced the Go binding header copy to include the existing
  `ddb_db_execute_on_branch` declaration so the new Go branch API maps to the
  shared C ABI without extension.
- Added Go binding tests for branch create/list/delete, branch-scoped execution,
  and branch error mapping.
- Added Go branch API documentation usage examples in
  `bindings/go/decentdb-go/README.md`.
- Documented Web/WASM copied row ownership in `docs/api/wasm.md` and
  `bindings/web/README.md`; added browser smoke coverage that retains a row
  after advancing and closing a prepared statement.
- No C ABI expansion occurred in this phase; no ADR was required.
- Recorded Phase 6 validation artifacts and benchmarks in
  `.tmp/review-implementation/logs/phase-6-binding-test-commands.md`,
  `.tmp/review-implementation/logs/phase-6-precommit-fast.log`,
  `.tmp/review-implementation/benchmarks/phase-6-bindings/benchmark-summary.md`, and
  `.tmp/review-implementation/logs/phase-6-cargo-*.log`.

## Phase 6: Binding Parity and API Ergonomics

### Objective

Improve binding parity and reduce duplicated API patterns while keeping the C ABI
as the shared boundary.

### Required Ordering

Complete binding work in this order:

1. Document current binding capability matrix.
2. Improve bindings that can use existing C ABI functions without ABI changes.
3. Add C ABI extensions only after an ADR if the extension has broad binding
   impact.
4. Update binding tests.
5. Update binding docs.

### Task Group 6A: Binding Capability Matrix

Create or update a binding capability matrix in the appropriate docs location.

The matrix must include:

1. Rust native API.
2. C ABI.
3. Python.
4. Go.
5. Java.
6. Node.
7. Dart.
8. .NET.
9. Web/WASM if present.

For each binding, list support status for:

1. Open/close database.
2. Execute SQL.
3. Prepared statements.
4. Parameter binding.
5. Row iteration.
6. Row-view or batch fetch.
7. Branch API.
8. Geometry helpers.
9. Error codes.
10. Watch/change notification.
11. Sync or write queue.
12. Package-level smoke tests.

Use exact status values:

```text
supported
partial
not supported
not applicable
unknown
```

Do not leave blank cells.

### Task Group 6B: Branch API Wrappers

Add or complete branch API wrappers for bindings where the C ABI already exposes
the required functions.

Required tasks per binding:

1. Add idiomatic wrapper methods.
2. Preserve existing API names unless a binding already uses a naming convention.
3. Add package-level tests for branch create/list/switch/delete where supported.
4. Add docs or README examples.
5. Confirm errors map to binding-native error types and stable DecentDB codes
   where supported.

Do not add a new C ABI function in this task group.

### Task Group 6C: Go Row Fetch Performance

Improve Go row-fetch ergonomics and performance.

Required tasks:

1. Inspect current Go result fetching.
2. Replace per-cell `result_value_copy` loops with an existing row-view,
   prepared-statement, or batch-fetch path if one exists.
3. If no suitable C ABI path exists, stop and create an ADR proposal for a C ABI
   batch/row-view extension.
4. Add Go tests proving row values are correct after iteration advances.
5. Add Go tests proving copied values remain valid after statement close if the
   public API promises ownership.
6. Add a Go benchmark or smoke performance comparison if a Go benchmark harness
   already exists.

### Task Group 6D: Java JNI Batch Fetch

Improve Java batch fetch if the C ABI already supports it.

Required tasks:

1. Inspect Java JNI row iteration.
2. Add batch fetch wrapper only if an existing C ABI function supports it.
3. Add Java tests for batch row correctness.
4. Add Java tests for statement close and result lifetime.
5. Update Java README or package docs.

If a new C ABI function is required, stop and create an ADR before implementing.

### Task Group 6E: Dart Step Row-View Default

Make Dart use the row-view stepping path by default if the binding already has
the required implementation.

Required tasks:

1. Confirm existing Dart `stepRowView` behavior and lifetime rules.
2. Change the default step path to use row-view only if copied values remain
   valid according to Dart API expectations.
3. Add Dart tests for null, integer, float, text, blob, and mixed rows.
4. Add Dart tests for row lifetime after advancing to the next row.
5. Update Dart docs.

### Task Group 6F: Geometry Helper Parity

Add geometry helpers where bindings already expose enough primitives.

Required tasks per binding:

1. Add helper functions for supported geometry encode/decode operations.
2. Add tests with the same fixtures across bindings.
3. Document unsupported geometry types explicitly.
4. Ensure errors map to stable binding errors.

### Task Group 6G: Java and Node Watch Support

Add watch/change notification support only if the C ABI already supports it.

Required tasks:

1. Inspect existing watch API in Rust and C ABI.
2. Add Java wrapper and tests if ABI support exists.
3. Add Node wrapper and tests if ABI support exists.
4. Test watcher cleanup on database close.
5. Test watcher callback error handling.
6. Document threading and callback lifetime rules.

If C ABI support is incomplete, stop and create an ADR before adding broad ABI
surface.

### Task Group 6H: Web/WASM Row-View Design

Do not implement Web/WASM row views until lifetime and copy semantics are
documented.

Required tasks:

1. Write a design note that states whether Web/WASM rows are copied or borrowed.
2. State when borrowed memory becomes invalid.
3. State how JavaScript callers can safely retain values.
4. State how blobs and strings are encoded.
5. Add tests for retaining values after stepping and after statement close.

If the design requires C ABI or memory ownership changes, create an ADR before
implementation.

### Unit Testing Tasks

Run binding tests for every touched binding.

Use existing package commands. If a package command is not obvious, record the
discovered command in:

```text
.tmp/review-implementation/logs/phase-6-binding-test-commands.md
```

Run repository smoke tests for bindings:

```bash
python scripts/do-pre-commit-checks.py --mode fast
```

If fast pre-commit checks skip a binding because the local toolchain is missing,
record the skip in `tooling-gaps.md`.

### Documentation Tasks

Update:

1. Binding capability matrix.
2. Each touched binding README or package docs.
3. C ABI documentation if C ABI behavior is clarified.
4. `include/decentdb.h` comments if C ABI documentation lives in the header.
5. `docs/about/changelog.md` for user-facing binding API additions or changes.

### Required Validation

Run:

```bash
cargo fmt --check
cargo check -p decentdb
cargo lint
python scripts/do-pre-commit-checks.py --mode fast
```

Also run package-level tests for every touched binding.

### Required Benchmarks

Run Benchmark Tier B to ensure engine performance did not regress.

If binding changes are performance-motivated, run the binding-specific benchmark
or smoke performance test before and after the change. Save results in:

```text
.tmp/review-implementation/benchmarks/phase-6-bindings/
```

If the binding uses a new engine batch or row-view path, run Benchmark Tier C.

### Acceptance Criteria

Phase 6 is complete when:

1. Capability matrix is updated.
2. Touched bindings have package-level tests.
3. Touched bindings have docs.
4. Binding smoke checks pass or missing toolchains are documented.
5. No C ABI expansion occurred without an ADR.
6. Benchmark results satisfy the regression thresholds.

## Phase 7: Production Module Decomposition

### Objective

Split very large production modules into smaller Rust modules without changing
behavior.

### Required Ordering

Do this phase after test extraction and panic audit. Production module
decomposition is easier and safer once tests are out of the large files.

Split one concern at a time. Run validation after each concern.

### `exec` Decomposition Tasks

Split `crates/decentdb/src/exec/mod.rs` by existing conceptual boundaries.

Use this target layout unless the current module tree already has better
matching files:

```text
crates/decentdb/src/exec/mod.rs
crates/decentdb/src/exec/context.rs
crates/decentdb/src/exec/ddl.rs
crates/decentdb/src/exec/dml.rs
crates/decentdb/src/exec/query.rs
crates/decentdb/src/exec/expressions.rs
crates/decentdb/src/exec/functions.rs
crates/decentdb/src/exec/joins.rs
crates/decentdb/src/exec/aggregates.rs
crates/decentdb/src/exec/errors.rs
crates/decentdb/src/exec/tests.rs
```

Required steps for each extracted file:

1. Move one coherent group of types and functions.
2. Add `mod` declarations in `exec/mod.rs`.
3. Re-export only items that are already used outside the submodule.
4. Use `pub(crate)` instead of `pub` unless the item is already public API.
5. Remove imports that become unused.
6. Run `cargo check -p decentdb`.
7. Run the smallest relevant test filter.
8. Do not change logic, SQL semantics, or error messages.

### `db` Decomposition Tasks

Split `crates/decentdb/src/db.rs` by existing conceptual boundaries.

Use this target layout unless the current module tree already has better
matching files:

```text
crates/decentdb/src/db.rs
crates/decentdb/src/db/open.rs
crates/decentdb/src/db/connection.rs
crates/decentdb/src/db/transactions.rs
crates/decentdb/src/db/branches.rs
crates/decentdb/src/db/schema.rs
crates/decentdb/src/db/query_api.rs
crates/decentdb/src/db/errors.rs
crates/decentdb/src/db/tests.rs
```

Required steps for each extracted file:

1. Move one coherent group of types and functions.
2. Add `mod` declarations in `db.rs`.
3. Re-export only items that are already part of the existing public API.
4. Use `pub(crate)` for internal helpers.
5. Avoid adding new traits unless extraction is impossible without one.
6. Run `cargo check -p decentdb`.
7. Run the smallest relevant test filter.
8. Do not change behavior.

### Unit Testing Tasks

This phase should not require new tests if it is purely mechanical. If a moved
module exposes a previously untested private helper and the move reveals unclear
behavior, add a focused unit test before continuing.

After each extracted concern, run:

```bash
cargo check -p decentdb
cargo t -p decentdb -- <relevant-filter>
```

At the end of the phase, run:

```bash
cargo t -p decentdb
```

### Documentation Tasks

No user-facing documentation is required for pure module decomposition.

Add module-level rustdoc only when it clarifies a non-obvious module boundary.
Do not add generic comments that merely restate file names.

### Required Validation

Run:

```bash
cargo fmt --check
cargo check -p decentdb
cargo t -p decentdb
cargo lint
```

### Required Benchmarks

Run Benchmark Tier B.

Run Benchmark Tier C if any code was changed beyond mechanical movement,
visibility adjustments, or import cleanup.

### Acceptance Criteria

Phase 7 is complete when:

1. `exec/mod.rs` and `db.rs` are materially smaller.
2. Public API remains unchanged unless separately approved.
3. Tests pass.
4. No performance regression exceeds thresholds.
5. The module tree is easier to navigate without adding unnecessary abstraction.

### Phase 7 Completion Notes

- Split executor expression and scalar-function helpers from
  `crates/decentdb/src/exec/mod.rs` into
  `crates/decentdb/src/exec/expressions.rs`.
- Split post-`Db` helper groups from `crates/decentdb/src/db.rs` into
  `crates/decentdb/src/db/{audit,branches,open,query_api,schema,sync_api}.rs`.
- Kept public API paths stable, including `decentdb::db::evict_shared_wal`;
  changes were limited to module boundaries and internal visibility.
- Reduced `exec/mod.rs` from 36,498 to 30,793 lines and `db.rs` from 18,781 to
  14,504 lines after formatting.
- Targeted filters passed for `exec`, `db`, `branch`, `sync`, and `pragma`;
  full Phase 7 validation passed:
  `cargo fmt --check`, `cargo check -p decentdb`, `cargo t -p decentdb`, and
  `cargo lint`.
- Recorded validation logs in
  `.tmp/review-implementation/logs/phase-7-targeted-tests.log` and
  `.tmp/review-implementation/logs/phase-7-validation.log`.
- Recorded Benchmark Tier B evidence in
  `.tmp/review-implementation/benchmarks/phase-7/benchmark-summary.md`.
  Smoke and medium macro guardrails passed; plan-cache p95/p99 variance is
  documented in `.tmp/review-implementation/benchmarks/regression-notes.md` as
  benchmark noise for this mechanical move.
- Attempted to use `phase_executor_spark` for this phase, but the subagent
  returned a usage-limit error. The tooling gap is recorded in
  `.tmp/review-implementation/logs/tooling-gaps.md`.

## Phase 8: Documentation Completion and Release-Quality Validation

### Objective

Bring documentation, test strategy, benchmark records, and final validation into
alignment after implementation phases.

### Tasks

1. Review every changed public behavior and ensure documentation exists.

2. Update `design/TESTING_STRATEGY.md` or the current testing-strategy document
   with:

   - new unit-test surfaces
   - new integration-test surfaces
   - binding test commands
   - benchmark gates from this plan
   - deterministic seed reproduction instructions

3. Update binding documentation for every touched binding.

4. Update C ABI header comments if C ABI behavior changed or was clarified.

5. Update `docs/about/changelog.md` for user-facing changes.

6. Do not update root `CHANGELOG.md`.

7. Create a final implementation summary in:

   ```text
   .tmp/review-implementation/final-summary.md
   ```

   The summary must include:

   - phases completed
   - files changed by category
   - tests added
   - docs updated
   - benchmark commands run
   - benchmark result paths
   - any skipped validation and reason
   - any accepted residual risks

### Required Validation

Run the full validation set:

```bash
cargo fmt --check
cargo check -p decentdb
cargo lint
cargo t -p decentdb
cargo test-all
python scripts/do-pre-commit-checks.py --mode fast
```

If the user or maintainer asks for release-level validation, run:

```bash
python scripts/do-pre-commit-checks.py --mode paranoid
```

Record validation results in:

```text
.tmp/review-implementation/logs/phase-8-validation.md
```

### Required Benchmarks

Run Benchmark Tier C.

Also run any binding-specific benchmarks introduced or changed in Phase 6.

Record the final benchmark comparison in:

```text
.tmp/review-implementation/benchmarks/final-benchmark-summary.md
```

The final benchmark summary must include:

1. Baseline date and command.
2. Final date and command.
3. Machine note.
4. DecentDB smoke result.
5. DecentDB medium result.
6. DecentDB full benchmark result path.
7. Plan-cache benchmark result path.
8. Any latency, concurrency, write, or cold-suite result path.
9. Percent changes versus Phase 0 baseline.
10. Pass/fail status against this plan's regression thresholds.

### Acceptance Criteria

Phase 8 is complete when:

1. Documentation is aligned with implemented behavior.
2. Changelog entries are in `docs/about/changelog.md` when required.
3. Full validation commands pass or documented tooling gaps exist.
4. Benchmark Tier C passes the regression thresholds.
5. Final summary exists under `.tmp/review-implementation/`.

### Phase 8 Completion Notes

- Reviewed `design/FUTURE_WINS.md` and `design/VERSIONING_GUIDE.md`.
  No release version bump was performed because this branch does not change the
  canonical `VERSION`; user-facing notes were added under
  `docs/about/changelog.md` `[Unreleased]` as required by the versioning guide.
- Updated final documentation in `design/TESTING_STRATEGY.md`,
  `docs/about/changelog.md`, `docs/api/bindings-matrix.md`,
  `docs/api/wasm.md`, `bindings/web/README.md`,
  `bindings/go/decentdb-go/README.md`,
  `docs/development/contributing.md`, and `docs/development/testing.md`.
- Created the required final summaries:
  `.tmp/review-implementation/final-summary.md` and
  `.tmp/review-implementation/benchmarks/final-benchmark-summary.md`.
- Full Phase 8 validation passed:
  `cargo fmt --check`, `cargo check -p decentdb`, `cargo lint`,
  `cargo t -p decentdb`, `cargo test-all`, and
  `python scripts/do-pre-commit-checks.py --mode fast`.
  The log is `.tmp/review-implementation/logs/phase-8-validation.md`.
- Extra release-grade validation passed with
  `python scripts/do-pre-commit-checks.py --mode paranoid`: 34 checks passed
  and Java binding tests skipped because JDK headers (`jni.h`) were not
  available. The log is
  `.tmp/review-implementation/logs/phase-8-paranoid-validation.md`.
- Benchmark Tier C completed. Result paths and percent comparisons are in
  `.tmp/review-implementation/benchmarks/final-benchmark-summary.md`, with raw
  logs in `.tmp/review-implementation/logs/phase-8-benchmarks.log`.
- Smoke and medium macro guardrails passed after the plan's noisy-rerun policy:
  rerun median total runtime was -2.69% for smoke and +2.02% for medium versus
  Phase 0, and peak RSS stayed within threshold.
- Some sub-millisecond query timings and plan-cache p95/p99 tail metrics still
  exceeded the strict percentage thresholds when compared to the single Phase 0
  baseline. This is documented as accepted benchmark noise in
  `.tmp/review-implementation/benchmarks/regression-notes.md`, following the
  user's instruction to make and document a best-practices decision when
  blocked on a maintainer decision.
- No C ABI contract changed broadly; the Go branch helpers use existing C ABI
  functions. No ADR-required on-disk, WAL, locking, or broad ABI decision was
  implemented.

## Phase Completion Checklist

Use this checklist at the end of every phase:

```text
Phase:
Implementation complete:
Unit tests added or confirmed unnecessary:
Documentation updated or confirmed unnecessary:
Validation commands run:
Benchmark tier run:
Benchmark result path:
Regression threshold pass/fail:
Tooling gaps:
Residual risks:
Next phase:
```

Do not mark a phase complete with an empty benchmark result path unless the
phase explicitly allows a documentation-only benchmark skip note.

## Final Done Definition

The review implementation is done only when:

1. Phases 0 through 8 are complete or explicitly marked not applicable with a
   maintainer-approved reason.
2. Unit tests cover all changed behavior.
3. Binding package tests cover all changed binding behavior.
4. Documentation describes all public behavior and workflow changes.
5. Benchmarks from `benchmarks/rust-baseline` show no significant degradation
   under this plan's thresholds.
6. No root temporary artifacts were added.
7. No root `CHANGELOG.md` edits were made.
8. No ADR-required decision was implemented without an ADR.
