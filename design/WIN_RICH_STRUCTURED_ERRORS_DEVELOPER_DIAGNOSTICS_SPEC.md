# Rich Structured Errors And Developer Diagnostics

**Date:** 2026-05-28
**Status:** TODO
**Future Version:** vNext
**Roadmap:** [`FUTURE_WINS.md`](FUTURE_WINS.md)
**Document Type:** Implementation SPEC
**Audience:** Core engine maintainers, C ABI maintainers, CLI maintainers,
binding maintainers, WASM/browser maintainers, sync maintainers, Doctor and
tooling authors, documentation authors, coding agents

`vNext` means the first release bucket after 2.7.0 only after this scope is
explicitly accepted. It is not a promise that every possible subcode lands in
the first implementation slice.

**Governing ADRs:**

- [`adr/0010-error-handling-strategy.md`](adr/0010-error-handling-strategy.md)
- [`adr/0118-rust-ffi-panic-safety.md`](adr/0118-rust-ffi-panic-safety.md)
- [`adr/0185-rich-structured-error-diagnostics-contract.md`](adr/0185-rich-structured-error-diagnostics-contract.md)

**Required follow-up ADRs before implementation:**

- New top-level numeric C ABI status codes beyond the existing broad categories.
- Any error or diagnostic change that exposes raw parameters, full SQL text,
  raw audit context values, encryption keys, or unredacted filesystem paths by
  default.
- Support-bundle or telemetry behavior that stores diagnostics outside the
  caller's process.
- Any file-format, WAL-format, checkpoint, recovery, or concurrency semantic
  change discovered while adding diagnostics.
- A binding-specific diagnostic API that cannot be represented through the
  shared Rust/C ABI contract.

**Related inputs:**

- [`FUTURE_WINS.md`](FUTURE_WINS.md)
- [`docs/api/error-codes.md`](../docs/api/error-codes.md)
- [`include/decentdb.h`](../include/decentdb.h)
- [`design/adr/0163-operational-sys-metrics.md`](adr/0163-operational-sys-metrics.md)
- [`design/adr/0174-local-data-security-tde-policies-masking-audit-context.md`](adr/0174-local-data-security-tde-policies-masking-audit-context.md)
- [`design/adr/0179-cross-process-public-contract-bindings-and-diagnostics.md`](adr/0179-cross-process-public-contract-bindings-and-diagnostics.md)
- [`docs/user-guide/write-concurrency.md`](../docs/user-guide/write-concurrency.md)
- [`docs/user-guide/security.md`](../docs/user-guide/security.md)
- [`docs/user-guide/sync/operations.md`](../docs/user-guide/sync/operations.md)
- [`STABLE_TOOLING_METADATA_CONTRACT.md`](STABLE_TOOLING_METADATA_CONTRACT.md)

---

## 1. Executive Summary

DecentDB already exposes broad engine error categories through Rust and the C
ABI. Those categories are good compatibility anchors, but they are not enough
for application developers, bindings, CLIs, browser runtimes, sync tools, or
coding agents to decide what to do next.

Today many failure paths reduce to:

```text
numeric status + human-readable message
```

That is too coarse for common cases:

- a duplicate unique key and a missing table are both ordinary user mistakes,
  but they need different handling and docs;
- queue timeouts, busy writer locks, and queue closure are all operational
  failures, but only some are safely retryable;
- process coordination errors should point at `sys.process_*` and Doctor
  surfaces instead of forcing users to guess which process owns the file;
- sync scope, changeset, branch, policy, mask, TDE, and format errors need safe
  structured context without leaking sensitive values;
- bindings need the same machine-readable details instead of parsing English
  message text differently in each language.

This win adds a stable, versioned diagnostic layer below all maintained
bindings. Existing broad codes stay stable. The new contract adds subcodes,
safe context fields, retry/permanence classification, optional SQLSTATE
mappings, documentation anchors, remediation hints, and Doctor handoff.

## 2. Product Goals

- Preserve the existing broad numeric error categories for compatibility.
- Add stable string subcodes for actionable error classes.
- Make human-readable messages helpful but explicitly non-contractual.
- Provide optional SQLSTATE-compatible mappings where the semantic fit is clear.
- Attach structured context for database objects, runtime blockers, WAL/format
  details, sync/branch context, and policy/security context when known.
- Classify errors with machine-readable `retryable` and `permanent` booleans.
- Provide safe remediation hints and documentation anchors for common failures.
- Apply one redaction policy across Rust, C ABI, CLI, HTTP console, WASM, and
  maintained bindings.
- Expose the same diagnostic shape through Python, Go, Node, .NET, Java, Dart,
  WASM/browser, and CLI JSON.
- Hand off deeper inspection to Doctor and `sys.*` surfaces without making
  ordinary error paths expensive.

## 3. Non-Goals

- No change to database file format, WAL format, checkpoint semantics, or
  durability behavior.
- No promise that every internal error has perfect structured context in the
  first slice.
- No stable contract based on `Display` text or localized/user-facing message
  strings.
- No raw parameter values, raw encryption keys, unredacted audit context values,
  full SQL text, or full filesystem paths in default diagnostics.
- No always-on tracing, stack capture, or Doctor execution on the hot path.
- No binding-specific diagnostic taxonomy independent of the Rust/C ABI
  contract.
- No broad binding rewrites beyond the projection required to expose the shared
  diagnostic object.
- No public support-bundle format in this win. A future support bundle may
  consume diagnostics, but it needs its own redaction and artifact contract.

## 4. Current Context

The current engine has these foundations:

- `DbErrorCode` defines stable numeric categories in Rust.
- `DbError` variants map to those categories and carry message strings.
- `include/decentdb.h` exposes the same numeric statuses.
- `ddb_last_error_message()` returns the most recent human-readable error text.
- The C ABI catches panics at FFI boundaries.
- CLI/HTTP paths already produce JSON-shaped errors in some places.
- Bindings already map broad categories into language-specific exception
  families.
- Process coordination, sync, browser, mobile, security, and operational
  metrics already have queryable diagnostic surfaces that structured errors can
  point to.

Current limitations:

- `ddb_last_error_message()` is message-only; there is no shared diagnostic JSON
  accessor.
- Message text is doing too much work for programmatic callers.
- CLI JSON errors are inconsistent across commands and usually lack stable
  codes beyond generic request-level values.
- Bindings expose different levels of detail.
- Some bindings can drift from the C ABI status set. For example, Dart must
  model all current statuses before it can reliably project structured
  diagnostics.
- Existing docs list broad error categories but do not define subcodes, context
  fields, redaction, retryability, or binding projection rules.

## 5. Public Diagnostic Contract

### 5.1 Layers

Every surfaced DecentDB error has three layers:

| Layer | Stable? | Purpose |
|---|---|---|
| Numeric category | Yes | C ABI and compatibility branching. |
| Diagnostic subcode | Yes | Precise machine-readable condition. |
| Human message | No | Helpful explanation for people. |

The category remains the compatibility floor. The subcode is the normal
programmatic key for retries, docs, hints, and tooling. The message may change
without a compatibility promise.

### 5.2 JSON Shape

The C ABI and JSON-facing surfaces use this shape:

```json
{
  "version": 1,
  "code": 3,
  "code_name": "ERR_CONSTRAINT",
  "subcode": "constraint.unique",
  "sqlstate": "23505",
  "message": "unique constraint violated",
  "retryable": false,
  "permanent": true,
  "redaction": "default",
  "relation": "users",
  "column": "email",
  "constraint": "users_email_key",
  "hint": "Choose a different value or use an upsert path.",
  "docs": "errors/constraint-unique"
}
```

Required fields:

| Field | Meaning |
|---|---|
| `version` | Diagnostic schema version. Initial value is `1`. |
| `code` | Existing stable numeric category. |
| `code_name` | Stable string name for the broad category. |
| `subcode` | Stable string identifier for the specific condition. |
| `message` | Human-readable text. Not a programmatic contract. |
| `retryable` | Whether retrying the same logical operation may succeed without changing input. |
| `permanent` | Whether the same inputs/state should keep failing until the caller or environment changes. |
| `redaction` | Redaction policy applied to this diagnostic. Initial value is `default`. |

`retryable` and `permanent` are independent flags, not automatic inverses:

| Flags | Meaning |
|---|---|
| `retryable=false`, `permanent=true` | Caller must change input, schema, configuration, permissions, or durable state. Example: `constraint.unique`. |
| `retryable=true`, `permanent=false` | Ordinary transient condition. Retrying with backoff may succeed. |
| `retryable=true`, `permanent=true` | State-change-required retry. The same caller input may succeed later only after external or engine state changes, such as a writer lock clearing, queue pressure draining, disk space being freed, or sync retention blockers being resolved. |
| `retryable=false`, `permanent=false` | Reserved for caller-driven cancellation or ambiguous internal failures where DecentDB cannot make a safe retry recommendation. |

Optional common fields:

| Field | Meaning |
|---|---|
| `sqlstate` | Optional SQLSTATE-compatible code where useful. |
| `relation` | Table, view, index target relation, or scope table name. |
| `column` | Column name when known. |
| `index` | Index name when known. |
| `constraint` | Constraint name or generated stable constraint identity. |
| `policy` | Policy or mask name when known. |
| `branch` | Branch, snapshot, or ref name when known and safe. |
| `sync_scope` | Sync scope name when known and safe. |
| `sync_peer` | Peer identifier when safe. |
| `changeset_id` | Public changeset identifier when safe. |
| `process_owner` | Redacted process owner metadata for coordination blockers. |
| `wal` | WAL context such as LSN/frame/checksum class, redacted as needed. |
| `format` | Database or WAL format version context. |
| `parameter` | Parameter index/name/type context without raw value. |
| `path` | Redacted path descriptor, never a raw absolute path by default. |
| `hint` | Static or sanitized remediation hint. |
| `docs` | Stable documentation anchor. |
| `doctor` | Structured handoff to Doctor or `sys.*` inspection. |
| `details` | Small structured details map for low-risk non-sensitive fields. |

Named fields are reserved for the explicit optional fields in this table. Small,
non-sensitive facts that do not have a named field belong under `details`.

Unknown optional fields must be ignored by consumers. Removing a required field,
renaming a subcode, or changing a subcode's meaning is a compatibility break.
JSON serialization must omit absent optional fields rather than emitting
`null`, and deterministic key order should be used for snapshots, docs examples,
and C ABI tests.

### 5.3 Rust Shape

The engine should introduce a typed diagnostic structure equivalent to:

```rust
pub struct DbDiagnostic {
    pub version: u16,
    pub code: DbErrorCode,
    pub subcode: &'static str,
    pub sqlstate: Option<&'static str>,
    pub message: String,
    pub retryable: bool,
    pub permanent: bool,
    pub context: DbDiagnosticContext,
    pub hint: Option<&'static str>,
    pub docs: Option<&'static str>,
    pub doctor: Option<DbDoctorHandoff>,
}
```

The exact Rust type can differ, but the implementation must make it hard to
create a stable diagnostic with unredacted dynamic fields accidentally.

`DbDiagnosticContext` should be a typed context struct with optional fields that
mirror the JSON optional fields in Section 5.2. Dynamic values that can contain
paths, parameters, SQL text, audit context, credentials, or open options should
use redacted wrapper types instead of plain `String`.

`DbError::code()` and `DbError::numeric_code()` continue working. Existing
message constructors remain as compatibility shims. They should assign
category-specific fallback subcodes such as `sql.unknown`,
`constraint.unknown`, `io.unknown`, or `internal.unknown` until the call site is
converted. New code paths must use structured constructors that set a stable
subcode and context explicitly.

### 5.4 C ABI Shape

The C ABI should add one primary accessor:

```c
ddb_status_t ddb_last_error_json(char **out_json);
```

Rules:

- The returned JSON string is owned by the caller and freed with
  `ddb_string_free`.
- If there is no last error, return `DDB_OK` and set `*out_json` to `NULL`.
- Calling the accessor must not replace the last diagnostic.
- The existing `ddb_last_error_message()` remains available.
- The C ABI version must be bumped.
- Every maintained binding must update ABI expectations and smoke tests.

The ABI bump is expressed through the existing runtime `ddb_abi_version()`
query. Bindings must compare that runtime value against their compiled
expectation. The public header may also define a compile-time `DDB_ABI_VERSION`
macro when the implementation lands, but the runtime function remains the
authoritative compatibility check.

A borrowed-pointer accessor can be added later if profiling proves the owned
JSON path too costly, but the first stable API should prefer clear ownership.

## 6. Initial Subcode Catalog

The first implementation does not need to cover every error path. It should
cover high-friction cases where structured handling immediately helps
applications and bindings.

Initial categories use the existing broad numeric error family and the public
compatibility effect, not a perfect domain taxonomy. SQL-surfaced security,
sync, branch, and extension validation errors may remain under `ERR_SQL` in the
first slice because adding `ERR_SECURITY`, `ERR_SYNC`, or `ERR_BRANCH` requires
a follow-up ADR and C ABI decision. TDE open failures stay under `ERR_IO` or
`ERR_CORRUPTION` when they are observed as file/open/header validation failures.

The table lists `retryable` to keep the catalog compact. Unless an implemented
subcode documents a more specific value, first-slice retryable rows should use
`permanent=true` as state-change-required retries, and non-retryable rows should
also use `permanent=true` except caller-driven cancellation such as
`queue.canceled`, which should use `permanent=false`.

| Area | Category | Subcode | SQLSTATE | Retryable | Key fields |
|---|---|---|---|---:|---|
| SQL parse | `ERR_SQL` | `sql.syntax` | `42601` | No | `details.position` when known |
| SQL name resolution | `ERR_SQL` | `sql.relation_not_found` | `42P01` | No | `relation` |
| SQL name resolution | `ERR_SQL` | `sql.column_not_found` | `42703` | No | `relation`, `column` |
| SQL name resolution | `ERR_SQL` | `sql.ambiguous_column` | `42702` | No | `column` |
| SQL parameters | `ERR_SQL` | `sql.parameter_missing` | `07002` | No | `parameter` |
| SQL parameters | `ERR_SQL` | `sql.parameter_type_mismatch` | `42804` | No | `parameter`, `column` |
| SQL capability | `ERR_SQL` | `sql.unsupported_feature` | `0A000` | No | `details.feature` |
| Constraints | `ERR_CONSTRAINT` | `constraint.unique` | `23505` | No | `relation`, `column`, `index`, `constraint` |
| Constraints | `ERR_CONSTRAINT` | `constraint.not_null` | `23502` | No | `relation`, `column`, `constraint` |
| Constraints | `ERR_CONSTRAINT` | `constraint.check` | `23514` | No | `relation`, `constraint` |
| Constraints | `ERR_CONSTRAINT` | `constraint.foreign_key` | `23503` | No | `relation`, `column`, `constraint` |
| Transactions | `ERR_TRANSACTION` | `transaction.no_active_transaction` | `25000` | No | none |
| Transactions | `ERR_TRANSACTION` | `transaction.invalid_state` | `25000` | No | `details.state` |
| Queue | `ERR_TIMEOUT` | `queue.write_timeout` | `HYT00` | Yes | `details.timeout_ms` |
| Queue | `ERR_CANCELED` | `queue.canceled` | `57014` | No | none |
| Queue | `ERR_QUEUE_FULL` | `queue.full` | `HYT00` | Yes | `details.capacity` |
| Queue | `ERR_QUEUE_CLOSED` | `queue.closed` | `08003` | No | none |
| Locking | `ERR_BUSY` | `busy.writer_lock` | `55P03` | Yes | `process_owner`, `doctor` |
| Process coordination | `ERR_TIMEOUT` | `coordination.lock_timeout` | `55P03` | Yes | `process_owner`, `doctor` |
| Process coordination | `ERR_IO` | `coordination.sidecar_unavailable` | none | No | `path`, `doctor` |
| I/O | `ERR_IO` | `io.permission_denied` | none | No | `path` |
| I/O | `ERR_IO` | `io.disk_full` | none | Yes | `path` |
| I/O | `ERR_IO` | `io.not_found` | none | No | `path` |
| Format | `ERR_UNSUPPORTED_FORMAT_VERSION` | `format.unsupported_version` | none | No | `format` |
| Corruption | `ERR_CORRUPTION` | `corruption.database_header` | none | No | `format`, `doctor` |
| Corruption | `ERR_CORRUPTION` | `corruption.page_checksum` | none | No | `details.page_id`, `doctor` |
| Corruption | `ERR_CORRUPTION` | `corruption.wal_frame` | none | No | `wal`, `doctor` |
| Corruption | `ERR_CORRUPTION` | `corruption.wal_replay` | none | No | `wal`, `doctor` |
| Security | `ERR_SQL` | `security.policy_denied` | `42501` | No | `relation`, `policy` |
| Security | `ERR_SQL` | `security.mask_expression_invalid` | `42601` | No | `relation`, `column`, `policy` |
| Encryption | `ERR_IO` | `tde.key_required` | none | No | `path` |
| Encryption | `ERR_CORRUPTION` | `tde.key_mismatch` | none | No | `path` |
| Sync | `ERR_SQL` | `sync.scope_not_found` | none | No | `sync_scope` |
| Sync | `ERR_CONSTRAINT` | `sync.changeset_conflict` | none | No | `changeset_id`, `sync_scope` |
| Sync | `ERR_TRANSACTION` | `sync.retention_blocked` | none | Yes | `sync_peer`, `doctor` |
| Branch | `ERR_SQL` | `branch.not_found` | none | No | `branch` |
| Branch | `ERR_CONSTRAINT` | `branch.merge_conflict` | none | No | `branch`, `doctor` |
| Extension | `ERR_SQL` | `extension.untrusted_package` | none | No | `details.package` |
| Panic | `ERR_PANIC` | `internal.panic_captured` | `XX000` | No | none |
| Internal | `ERR_INTERNAL` | `internal.invariant` | `XX000` | No | `doctor` |

`queue.write_timeout` uses `ERR_TIMEOUT` because a request was admitted and then
timed out while waiting. `queue.full` uses `ERR_QUEUE_FULL` because admission was
rejected immediately by bounded backpressure.

Network/remoting errors are out of scope for the core engine v1 catalog unless
the failing operation is owned by the engine. Relay or transport layers may wrap
engine diagnostics in command-level errors until a future ADR accepts an
engine-owned network diagnostic family.

Subcodes can be added over time. The first slice should prioritize correctness
and stable coverage for common errors over exhaustive taxonomy.

## 7. Redaction Rules

Default diagnostics must be safe for logs, CLI JSON, binding exceptions, and
ordinary support tickets.

### 7.1 Never Include By Default

- Raw parameter values.
- Raw `BLOB` or large `TEXT` values.
- `encryption_key`, `encryption_key_hex`, `tde_key`, or `tde_key_hex`.
- Raw key-provider output.
- Raw audit context values.
- Bearer tokens, sync auth tokens, or relay credentials.
- Full absolute filesystem paths.
- Raw SQL text unless it is a DecentDB-internal command verb such as
  `CHECKPOINT` or `VACUUM`. A hardcoded SQL statement is still SQL text and must
  not be included by default.

### 7.2 Allowed Default Context

- Relation, column, index, constraint, policy, branch, scope, and peer names,
  unless a future security profile marks those identifiers sensitive.
- Parameter index, name, and expected/actual type class, without values.
- Redacted path descriptor such as basename plus stable hash, or a relative path
  already supplied by the caller.
- Process id and connection id for local coordination blockers, unless a
  platform support policy later restricts them.
- WAL LSN/frame/page identifiers that do not reveal row data.
- Static hints and static documentation anchors.

### 7.3 Binding Behavior

Bindings must not append unredacted SQL and parameter values to native
diagnostics by default. If a binding keeps a developer-debug context feature, it
must use an explicit opt-in and document that the caller is responsible for
handling sensitive values.

## 8. Binding Projection

All maintained bindings must expose the same diagnostic information in their
native idiom.

| Surface | Required projection |
|---|---|
| Rust | `DbError::diagnostic()` returns a typed diagnostic. |
| C ABI | `ddb_last_error_json(char **out_json)` returns versioned JSON. |
| Python | DB-API exception families remain; exceptions gain `.diagnostic`, `.code`, `.subcode`, `.sqlstate`, `.retryable`, `.permanent`. |
| Go | `DecentDBError` gains diagnostic fields and keeps sentinel wrapping for busy/timeout/queue cases. |
| Node | `Error` objects include `code`, `nativeCode`, `subcode`, `sqlstate`, `retryable`, `permanent`, and `diagnostic`. |
| .NET | `DecentDBException` gains diagnostic properties and ADO.NET maps SQLState-like concepts where applicable. |
| Java | `SQLException` vendor code and SQLState remain; typed diagnostic detail is available from DecentDB exception helpers, with raw JSON available for forward compatibility. |
| Dart | `DecentDbException` gains `diagnostic`. |
| WASM/browser | Worker and TypeScript APIs expose the same diagnostic object in rejected promises/errors. |
| CLI/HTTP | JSON error responses include the diagnostic object; table/text output keeps concise messages plus docs/hint where useful. |

Bindings should preserve source-compatible broad exception families where
possible. Structured diagnostics should make existing exceptions more useful,
not force every caller to catch a new root error type.

## 9. CLI, HTTP, WASM, And JSON Bridges

JSON-facing surfaces should standardize on:

```json
{
  "ok": false,
  "error": {
    "code": "ERR_SQL",
    "native_code": 5,
    "subcode": "sql.relation_not_found",
    "message": "relation not found",
    "diagnostic": {
      "version": 1,
      "code": 5,
      "code_name": "ERR_SQL",
      "subcode": "sql.relation_not_found",
      "message": "relation not found",
      "retryable": false,
      "permanent": true,
      "redaction": "default",
      "relation": "users",
      "docs": "errors/sql-relation-not-found"
    }
  }
}
```

Command-specific request validation errors may keep command-level codes such as
`INVALID_REQUEST`, but when the error originated in the engine they should carry
the engine diagnostic as `diagnostic`.

Sync JSON bridge errors, branch JSON bridge errors, browser worker errors, and
HTTP console errors must all use the same shape.

## 10. Doctor Handoff

Diagnostics should include `doctor` only when the next useful step requires
deeper inspection.

Examples:

```json
{
  "doctor": {
    "kind": "process_coordination",
    "command": "decentdb doctor --db <redacted> --format=json",
    "sql": [
      "SELECT * FROM sys.process_coordination",
      "SELECT * FROM sys.process_lock_metrics"
    ]
  }
}
```

```json
{
  "doctor": {
    "kind": "sync",
    "command": "decentdb sync doctor --db <redacted> --format=json"
  }
}
```

Doctor handoff must be informational. Error construction must stay cheap unless
the caller explicitly runs a Doctor or diagnostics command. The `<redacted>`
token in `doctor.command` is a literal placeholder, not a template expansion.
Callers must supply their own database path or connection context when running
the suggested command.

## 11. Implementation Phases

### Phase 1: Core Contract

- Add the Rust diagnostic types and serialization.
- Add constructors for the highest-priority subcodes.
- Define deterministic diagnostic JSON serialization before exposing it through
  the C ABI: required fields are always present, absent optional fields are
  omitted, and key order is stable for tests and docs.
- Preserve existing `DbError` category behavior and display text.
- Add redaction helpers for parameters, paths, open options, audit context, and
  sync/auth tokens.
- Add engine tests for diagnostic schema, subcodes, retry/permanence, and
  redaction.

### Phase 2: C ABI And CLI

- Add `ddb_last_error_json(char **out_json)`.
- Bump the C ABI version.
- Update `include/decentdb.h`.
- Update C ABI tests for ownership, thread-local last error behavior, panic
  diagnostics, and no-last-error behavior.
- Update CLI, HTTP console, sync JSON bridges, branch JSON bridges, and WASM
  worker errors to include diagnostics.

### Phase 3: Binding Projection

- Update Python, Go, Node, .NET, Java, Dart, and browser TypeScript bindings.
- Ensure every binding knows all current broad status codes.
- Add smoke tests for SQL, constraint, queue/busy, and redaction-sensitive
  errors where practical.
- Update binding docs and examples.

### Phase 4: Diagnostic Coverage Expansion

- Convert high-frequency SQL planner/executor errors to stable subcodes.
- Convert constraint enforcement paths to object-aware diagnostics.
- Convert WAL, format, process coordination, sync, branch, security, TDE, and
  extension errors where structured context is already available.
- Add Doctor handoff for process, WAL/corruption, sync, and branch conflict
  families.

### Phase 5: Documentation And Release Guardrails

- Rewrite `docs/api/error-codes.md` around category plus subcode diagnostics.
- Add troubleshooting pages for common docs anchors.
- Add release validation that verifies diagnostic consistency across maintained
  bindings. This should be a CI-invoked script or staged pre-commit check that
  runs the relevant Rust validation plus binding smoke tests and asserts that
  the accepted first-slice subcodes are projected without message parsing. Each
  maintained binding should either cover every first-slice subcode through a
  shared diagnostic fixture or explicitly record why a subcode is unreachable
  from that binding's public surface.
- Add a compatibility checklist for new subcodes and diagnostic fields.

## 12. Testing Requirements

- Rust unit tests for every stable subcode introduced in the first slice.
- Snapshot or schema tests for diagnostic JSON required fields.
- C ABI tests for `ddb_last_error_json` ownership, lifetime, thread-local
  behavior, no-last-error behavior (`DDB_OK` plus `NULL`), and the invariant
  that reading JSON does not mutate the last diagnostic or message.
- Panic-boundary tests that return `ERR_PANIC` plus `internal.panic_captured`.
- Redaction tests for SQL parameters, open options, TDE keys, sync tokens, audit
  context, and paths.
- CLI JSON tests for at least one SQL error, one constraint error, and one
  process/sync/Doctor handoff path where available.
- Binding smoke tests that assert subcode and retryability without parsing
  message text.
- Compatibility tests proving existing numeric status values do not change.
- Fuzz or table-driven tests for path/open-option redaction helpers.

## 13. Definition Of Done

This win is complete when:

- The ADR and this spec are accepted.
- Rust has a typed diagnostic object with stable serialization.
- The C ABI exposes structured last-error JSON and bumps ABI version.
- Existing broad numeric categories remain unchanged.
- The first subcode catalog is implemented for SQL, constraints, queue/busy,
  process coordination, I/O, format/corruption, security/TDE, sync, branch, and
  panic/internal families at the accepted phase depth.
- CLI/HTTP/WASM JSON errors carry diagnostics consistently.
- Python, Go, Node, .NET, Java, Dart, and browser TypeScript expose diagnostics
  idiomatically.
- Redaction tests prove sensitive values are absent by default.
- Doctor handoff exists for errors that require deeper process, WAL, sync, or
  branch inspection.
- `docs/api/error-codes.md` and binding docs describe the stable contract.
- Release validation includes cross-binding diagnostic smoke coverage.

## 14. Compatibility Rules

- Existing numeric broad codes must not change value.
- Existing public functions must keep behavior unless a separate ADR accepts a
  breaking change.
- Adding optional diagnostic fields is backwards-compatible.
- Adding new subcodes is backwards-compatible when existing subcode meanings do
  not change.
- Renaming a subcode is a breaking change.
- Moving an existing condition to a different subcode is a breaking change
  unless the old subcode remains as an alias for at least one compatibility
  cycle.
- Changing `retryable` or `permanent` semantics for an existing subcode requires
  release-note visibility and test review.
- Message text may change at any time and must not be asserted as stable except
  in narrowly scoped human-output tests.

A compatibility cycle means at least one public release after the replacement
subcode ships with the old subcode still accepted or emitted as an alias. If the
change also affects C ABI shape or binding compatibility, the alias must remain
until at least the next C ABI version bump after that public release unless a
follow-up ADR accepts a shorter migration.

## 15. Initial Defaults For Previously Open Questions

These defaults close the initial design questions for the first implementation
slice. Future changes to these defaults should be treated as compatibility work
and reviewed against ADR 0185.

### 15.1 Path Redaction

Path redaction should default to a structured descriptor instead of a raw path
string:

```json
{
  "kind": "database",
  "display": "app.ddb",
  "fingerprint": "sha256:1a2b3c4d5e6f"
}
```

Rules:

- Use a caller-supplied display label when available.
- Otherwise use the basename plus a short SHA-256 fingerprint of the canonical
  path.
- Do not include parent directories by default.
- Use `kind` values such as `database`, `wal`, `coordination_sidecar`,
  `sync_journal`, `backup_destination`, or `unknown`.
- The initial fingerprint should use 12 lowercase hexadecimal characters unless
  implementation or collision tests justify a different length.

### 15.2 Binding Diagnostic Shape

Bindings should expose immutable typed diagnostic objects plus the original raw
JSON or map for forward compatibility.

Required fields and common optional fields should get convenience properties.
Unknown future fields must remain accessible through the raw JSON/map so callers
do not need a binding release before inspecting a newly added optional field.

### 15.3 SQL Source Spans

SQL source spans should be included only when the parser or resolver provides
reliable byte offsets.

The first slice should include syntax positions where the parser already
reports reliable locations. It should not invent spans for semantic errors.
Semantic errors should use structured fields such as `relation`, `column`,
`parameter`, and `details` instead.

### 15.4 Documentation Anchors

Documentation anchors should be stable relative IDs, not full URLs.

Examples:

- `errors/constraint-unique`
- `errors/sql-relation-not-found`
- `errors/queue-write-timeout`

Docs tooling, CLI output, website output, and hosted API references can resolve
those anchors to versioned URLs later. The diagnostic contract should only
promise the stable anchor ID.

### 15.5 Debug-Only Detail

Debug builds should not change the public diagnostic schema.

Extra local-only debugging detail may exist behind an explicit unsafe or
developer diagnostic flag, but it must not flow through default C ABI, binding,
CLI, WASM, HTTP, or support JSON output. Default diagnostics should remain
redacted and schema-compatible across debug and release builds.
