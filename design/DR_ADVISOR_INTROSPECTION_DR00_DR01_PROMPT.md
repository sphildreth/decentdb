# Coding Prompt: Implement Doctor / Advisor / Introspection DR-00 and DR-01

Use this prompt with a coding agent working in the DecentDB repository root.

Your task is to implement only slices **DR-00** and **DR-01** from
`design/DR_ADVISOR_INTROSPECTION_PLAN.md`.

Do not start DR-02 or any later slice. This prompt is intentionally limited to:

1. Confirming and preserving the plan/slice-map contract.
2. Adding the Rust doctor domain model and serialization tests.

No CLI command, fact collection, rule engine, index verification, or `--fix`
execution should be implemented in this task.

---

## 1. Required Reading Before Editing

Read these files before making changes:

1. `AGENTS.md`
2. `.github/instructions/rust.instructions.md`
3. `design/DR_ADVISOR_INTROSPECTION_PLAN.md`
4. `crates/decentdb/src/metadata.rs`
5. `crates/decentdb/src/lib.rs`
6. `crates/decentdb/src/error.rs`
7. Nearby module patterns in `crates/decentdb/src/`, especially modules that
   define public typed data structures and tests.

The plan document is the source of truth. If this prompt and the plan conflict,
follow `design/DR_ADVISOR_INTROSPECTION_PLAN.md`.

---

## 2. Scope Boundaries

### You must implement

- DR-00: scope/constant/output-contract confirmation.
- DR-01: Rust doctor domain model and report serialization.

### You must not implement

- `decentdb doctor` CLI command.
- Any CLI argument parsing.
- Any database opening or fact collection.
- Any doctor rule evaluation.
- Any actual findings beyond test fixtures.
- Any index verification integration.
- Any `--fix` planning or execution.
- Any mutation of database files.
- Any C ABI changes.
- Any binding changes.
- Any new dependency.

### Files expected to change

Expected:

- `design/DR_ADVISOR_INTROSPECTION_PLAN.md`
- `crates/decentdb/src/doctor.rs`
- `crates/decentdb/src/lib.rs`

Optional only if required by the existing crate structure:

- `crates/decentdb/src/mod.rs` or equivalent module registration file if one
  exists.

Do not modify unrelated files.

---

## 3. DR-00 Requirements

DR-00 is a documentation/status slice. It exists to ensure agents do not make
untracked product decisions during implementation.

### Required DR-00 work

1. In `design/DR_ADVISOR_INTROSPECTION_PLAN.md`, update the slice map status for
   DR-00 from `Pending` to `In Progress` before starting.
2. Review the plan for internal consistency around DR-00 and DR-01.
3. If you find a contradiction that directly blocks DR-01, fix the plan in the
   smallest possible way.
4. Do not change the v1 finding catalog, CLI contract, fix-action catalog, or
   later slice scope unless there is a clear contradiction that blocks DR-01.
5. After DR-00 is complete, update the slice map status for DR-00 to `Done`.

### DR-00 acceptance criteria

- The slice map remains at the top of
  `design/DR_ADVISOR_INTROSPECTION_PLAN.md`.
- DR-00 is marked `Done`.
- DR-01 is marked `In Progress` before code work starts.
- The plan still has balanced Markdown code fences.
- No later slices are marked done.

---

## 4. DR-01 Implementation Goal

Add a typed Rust domain model for doctor reports that can be serialized to JSON
and later rendered by CLI code.

This is model-only work. The code should support test construction of reports,
summary calculation, deterministic sorting, and JSON serialization.

---

## 5. Required Rust Module

Create:

```text
crates/decentdb/src/doctor.rs
```

Register it from the crate root in the same style as existing modules.

Re-export stable public types from `crates/decentdb/src/lib.rs` only when doing
so is needed by the module visibility conventions used in this crate. Prefer a
small, explicit public surface.

---

## 6. Required Types

Define typed structures/enums covering the semantic concepts required by the
plan.

At minimum, implement these public types or direct equivalents with the same
meaning:

```rust
pub struct DoctorOptions {
    pub checks: DoctorCheckSelection,
    pub verify_indexes: DoctorIndexVerification,
    pub include_recommendations: bool,
    pub path_mode: DoctorPathMode,
    pub fix: bool,
}

pub struct DoctorReport {
    pub schema_version: u32,
    pub mode: DoctorMode,
    pub status: DoctorStatus,
    pub database: DoctorDatabaseSummary,
    pub summary: DoctorSummary,
    pub pre_fix_findings: Vec<DoctorFinding>,
    pub findings: Vec<DoctorFinding>,
    pub fixes: Vec<DoctorFix>,
    pub collected: DoctorCollectedFacts,
}

pub enum DoctorMode {
    Check,
    Fix,
}

pub enum DoctorStatus {
    Ok,
    Warning,
    Error,
}

pub enum DoctorSeverity {
    Info,
    Warning,
    Error,
}

pub enum DoctorCategory {
    Header,
    Storage,
    Wal,
    Fragmentation,
    Schema,
    Statistics,
    Indexes,
    Compatibility,
}

pub struct DoctorFinding {
    pub id: String,
    pub severity: DoctorSeverity,
    pub category: DoctorCategory,
    pub title: String,
    pub message: String,
    pub evidence: Vec<DoctorEvidence>,
    pub recommendation: Option<DoctorRecommendation>,
}

pub struct DoctorFix {
    pub id: String,
    pub finding_id: String,
    pub status: DoctorFixStatus,
    pub message: String,
    pub evidence_before: Vec<DoctorEvidence>,
    pub evidence_after: Vec<DoctorEvidence>,
}

pub enum DoctorFixStatus {
    Planned,
    Applied,
    Skipped,
    Failed,
}
```

You may add fields/types needed to make this model complete, testable, and
ergonomic. Do not remove any required semantic concept.

---

## 7. Required Supporting Types

Implement these supporting concepts.

### 7.1 DoctorSummary

Must include:

- `info_count`
- `warning_count`
- `error_count`
- `highest_severity`
- `checked_categories`

`highest_severity` should serialize as:

- `"ok"` when there are no findings
- `"info"`
- `"warning"`
- `"error"`

If it is cleaner, introduce a separate type for highest severity rather than
reusing `DoctorSeverity`.

### 7.2 DoctorDatabaseSummary

Model the fields needed by the v1 JSON shape:

- `path`
- `wal_path`
- `format_version`
- `page_size`
- `page_count`
- `schema_cookie`

Use owned strings for paths in DR-01. Later slices can decide how to populate
them from `Path`/`StorageInfo`.

### 7.3 DoctorEvidence

Evidence must be structured, not free-form text only.

Use fields equivalent to:

- `field: String`
- `value: ...`
- `unit: Option<String>`

For `value`, choose a representation that serializes predictably without adding
dependencies. A small enum is preferred over `serde_json::Value` if that keeps
the public type cleaner.

### 7.4 DoctorRecommendation

Must include:

- `summary`
- `commands`
- `safe_to_automate`

For DR-01, recommendations are only test data. Later slices will populate them.

### 7.5 DoctorCollectedFacts

For DR-01, this may be an intentionally small placeholder structure, but it must
serialize as a JSON object and leave room for:

- storage facts
- header facts
- schema facts
- verified index facts

Do not implement database collection in DR-01.

### 7.6 Option helper types

Implement minimal typed option helpers needed by `DoctorOptions`:

- `DoctorCheckSelection`
- `DoctorIndexVerification`
- `DoctorPathMode`

These can be simple structs/enums with defaults. Do not implement CLI parsing.

---

## 8. Serialization Requirements

Use `serde::Serialize`.

Serialization must be stable and match the plan’s JSON naming style:

- struct fields: `snake_case`
- enum variants: lowercase snake-case strings
  - `DoctorMode::Check` -> `"check"`
  - `DoctorMode::Fix` -> `"fix"`
  - `DoctorStatus::Ok` -> `"ok"`
  - `DoctorSeverity::Warning` -> `"warning"`
  - `DoctorCategory::Wal` -> `"wal"`
  - `DoctorFixStatus::Applied` -> `"applied"`

Prefer serde attributes such as:

```rust
#[serde(rename_all = "snake_case")]
```

or:

```rust
#[serde(rename_all = "lowercase")]
```

as appropriate.

---

## 9. Sorting Requirements

Implement deterministic sorting for `DoctorFinding`.

Sort order:

1. Severity rank:
   - `error`
   - `warning`
   - `info`
2. Category order:
   - `header`
   - `storage`
   - `wal`
   - `fragmentation`
   - `schema`
   - `statistics`
   - `indexes`
   - `compatibility`
3. Finding ID lexicographically.

Expose this through a small helper such as:

```rust
pub fn sort_findings(findings: &mut [DoctorFinding])
```

or by sorting inside a `DoctorReport::new(...)` constructor.

Do not rely on derived enum ordering unless it explicitly matches the order
above and is tested.

---

## 10. Summary Calculation Requirements

Add a constructor or helper that calculates:

- report status
- summary counts
- highest severity
- sorted findings

Recommended shape:

```rust
impl DoctorReport {
    pub fn new(
        mode: DoctorMode,
        database: DoctorDatabaseSummary,
        checked_categories: Vec<DoctorCategory>,
        pre_fix_findings: Vec<DoctorFinding>,
        findings: Vec<DoctorFinding>,
        fixes: Vec<DoctorFix>,
        collected: DoctorCollectedFacts,
    ) -> Self
}
```

The exact signature may differ, but callers must not have to hand-maintain
summary counts or status.

Rules:

- Summary/status are based on final `findings`, not `pre_fix_findings`.
- `pre_fix_findings` must also be sorted.
- `schema_version` must be `1`.
- `mode` must be preserved.

---

## 11. Tests Required for DR-01

Add unit tests next to the new module.

Required tests:

1. Report with no findings has `DoctorStatus::Ok`.
2. Report with only info findings has `DoctorStatus::Ok` or an explicitly
   documented status behavior if the plan requires otherwise.
3. Report with a warning finding has `DoctorStatus::Warning`.
4. Report with an error finding has `DoctorStatus::Error`.
5. Summary counts are correct for mixed severities.
6. `highest_severity` serializes as expected.
7. Findings sort in this order:
   - error before warning
   - warning before info
   - category order after severity
   - finding ID after category
8. `pre_fix_findings` are also sorted.
9. JSON serialization uses expected lowercase/snake-case enum strings.
10. JSON output includes:
    - `schema_version`
    - `mode`
    - `status`
    - `summary`
    - `pre_fix_findings`
    - `findings`
    - `fixes`
    - `collected`
11. Fix statuses serialize as:
    - `planned`
    - `applied`
    - `skipped`
    - `failed`

If using `serde_json` in tests, confirm it is already available in the crate or
workspace. Do not add it as a new dependency without approval.

---

## 12. DR-01 Definition of Done

DR-01 is done only when:

1. `crates/decentdb/src/doctor.rs` exists.
2. The doctor domain model compiles.
3. Report construction calculates status and summary fields.
4. Finding sorting is deterministic and tested.
5. JSON serialization is tested.
6. No CLI code is added.
7. No database fact collection is added.
8. No fix execution is added.
9. DR-01 is marked `Done` in the slice map.
10. DR-02 remains `Pending`.

---

## 13. Required Validation Commands

Run these commands before finishing:

```bash
cargo fmt --check
cargo test -p decentdb -- doctor
cargo check -p decentdb
```

If these pass quickly, also run:

```bash
cargo clippy -p decentdb --all-targets --all-features -- -D warnings
```

If a command cannot be run, document the exact command, the error, and whether
the failure is environmental or caused by your changes.

---

## 14. Final Response Requirements

When finished, report:

1. Files changed.
2. Whether DR-00 and DR-01 are marked `Done`.
3. Validation commands run and their results.
4. Any intentionally deferred work.

Do not claim DR-02 or later slices are complete.

