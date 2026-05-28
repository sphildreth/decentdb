# Rich Structured Error Diagnostics Contract
**Date:** 2026-05-28
**Status:** Accepted

### Decision

DecentDB will keep the existing broad numeric error categories as the
compatibility layer and add a versioned structured diagnostic object as the
stable machine-readable error detail contract.

The existing category/status values remain stable:

- `DDB_ERR_IO`
- `DDB_ERR_CORRUPTION`
- `DDB_ERR_CONSTRAINT`
- `DDB_ERR_TRANSACTION`
- `DDB_ERR_SQL`
- `DDB_ERR_INTERNAL`
- `DDB_ERR_PANIC`
- `DDB_ERR_UNSUPPORTED_FORMAT_VERSION`
- `DDB_ERR_BUSY`
- `DDB_ERR_TIMEOUT`
- `DDB_ERR_CANCELED`
- `DDB_ERR_QUEUE_FULL`
- `DDB_ERR_QUEUE_CLOSED`

The new stable detail key is a string `subcode`, not human-readable message
text. Message text may improve over time and must not be used as a programmatic
contract.

The diagnostic object is versioned JSON at the C ABI boundary and a typed Rust
structure inside the engine. It must include these stable fields:

- `version`
- `code`
- `code_name`
- `subcode`
- `message`
- `retryable`
- `permanent`
- `redaction`

It may include these optional fields when known and safe:

- `sqlstate`
- `relation`
- `column`
- `index`
- `constraint`
- `policy`
- `branch`
- `sync_scope`
- `sync_peer`
- `changeset_id`
- `process_owner`
- `wal`
- `format`
- `parameter`
- `path`
- `docs`
- `hint`
- `doctor`
- `details`

The C ABI must continue exposing `ddb_last_error_message()`. Structured
diagnostics require a new C ABI function that returns the diagnostic JSON, and
the C ABI version must be bumped when that function lands. Maintained bindings
must update their ABI expectation and smoke tests as part of the same
implementation slice.

Bindings must project the same diagnostic into idiomatic exception/error
objects while preserving their existing language-specific exception families
where possible. Python can continue mapping constraints to `IntegrityError`,
Java can continue mapping to `SQLException`/SQLState classes, and Go can keep
sentinel errors for busy/timeout/queue outcomes. The structured diagnostic is
additional detail, not a reason to fragment error behavior per binding.

SQLSTATE mappings are optional and compatibility-oriented. A SQLSTATE value may
be populated when there is a clear PostgreSQL/ODBC-compatible semantic match,
but DecentDB's `code` and `subcode` remain authoritative.

Diagnostics must be redacted by default. They must not contain raw parameter
values, encryption keys, sensitive open options, unredacted audit context
values, or full filesystem paths unless a future explicit support-bundle policy
allows that data under opt-in redaction rules. Diagnostic hints and docs anchors
must be static or sanitized so they cannot leak user data through generated
text.

Retryability and permanence must be machine-readable booleans. They describe
the error instance, not just the category. For example, `busy.writer_lock` is
retryable while `constraint.unique` is not; `io.permission_denied` is not
retryable until external permissions change, while `timeout.write_queue_wait`
may be retryable with backoff or capacity changes.

Doctor handoff is represented by a structured `doctor` field when an error
needs deeper inspection. The engine must not run Doctor automatically on ordinary
error paths. The diagnostic can name the relevant command or SQL inspection
surface, such as `decentdb doctor`, `decentdb sync doctor`, or
`sys.process_lock_metrics`.

### Rationale

DecentDB already has useful broad categories, but they are too coarse for
applications, bindings, tools, and coding agents to respond correctly. A unique
constraint failure, a missing table, a writer lock timeout, a stale coordination
sidecar, a sync scope error, and a corrupt WAL frame should not require parsing
English text.

Keeping the existing numeric codes preserves compatibility for C callers and
bindings that already branch on status values. Adding string subcodes gives
DecentDB room to grow without consuming a new numeric status for every detailed
condition.

The C ABI is the authoritative shared boundary for maintained bindings. Putting
the structured contract below the bindings prevents Python, Go, Node, .NET,
Java, Dart, and WASM from each inventing different field names, retry rules, and
redaction behavior.

SQLSTATE is valuable for JDBC, ADO.NET, ODBC-style integrations, and developer
familiarity, but it cannot be the only contract because DecentDB has native
embedded concepts such as WAL coordination, local sync scopes, branches, TDE
open options, and Doctor handoff.

### Alternatives Considered

1. **Keep improving message text only.** Rejected. Clear messages help people,
   but message parsing is brittle and cannot safely drive retries, migrations,
   or support tooling.
2. **Add many new numeric status codes.** Rejected as the primary mechanism.
   Numeric statuses are useful for coarse categories at the C ABI boundary, but
   detailed conditions evolve too often to make every subcase a top-level ABI
   status.
3. **Expose SQLSTATE only.** Rejected. SQLSTATE is useful where it matches SQL
   semantics, but it does not cover DecentDB-specific runtime, WAL, sync,
   branch, and support-diagnostic concepts cleanly.
4. **Let each binding define richer errors independently.** Rejected. That
   fragments behavior and makes cross-language documentation, tests, and agent
   tooling unreliable.
5. **Attach full SQL text and parameters by default.** Rejected. It makes
   debugging convenient but violates the redaction requirements needed for
   TDE, audit context, support bundles, and production logs.
6. **Always run Doctor when an error occurs.** Rejected. Doctor may be slower
   and broader than the failed operation. Error paths should point to Doctor
   without doing expensive inspection unless explicitly requested.

### Trade-offs

- A stable diagnostic schema adds implementation and test work across every
  maintained binding.
- Subcodes need governance. Adding one is cheap, but renaming or changing its
  meaning is a compatibility break.
- Redaction rules make diagnostics less exhaustive by default. Support bundles
  may need later opt-in detail policies.
- Optional SQLSTATE mappings help ecosystem integrations but require careful
  review to avoid misleading compatibility claims.

### Consequences

- `design/WIN_RICH_STRUCTURED_ERRORS_DEVELOPER_DIAGNOSTICS_SPEC.md` becomes
  the implementation source for phases, field semantics, subcode catalog, and
  acceptance tests.
- `docs/api/error-codes.md` must be rewritten around broad category plus
  subcode diagnostics when implementation lands.
- `include/decentdb.h` must gain a structured last-error JSON accessor and bump
  the C ABI version when the function is implemented.
- Maintained bindings must expose diagnostics and add smoke tests for at least
  one SQL error, one constraint error, one retryable queue/busy error, and one
  redaction-sensitive error path where practical.
- CLI, HTTP console, WASM/browser, sync JSON bridges, and Doctor handoff must
  use the same diagnostic shape.
- New diagnostic fields must be added compatibly. Removing fields, changing a
  subcode meaning, or exposing sensitive data requires a follow-up ADR.

### References

- `design/WIN_RICH_STRUCTURED_ERRORS_DEVELOPER_DIAGNOSTICS_SPEC.md`
- `design/FUTURE_WINS.md`
- `design/adr/0010-error-handling-strategy.md`
- `design/adr/0118-rust-ffi-panic-safety.md`
- `design/adr/0163-operational-sys-metrics.md`
- `design/adr/0174-local-data-security-tde-policies-masking-audit-context.md`
- `design/adr/0179-cross-process-public-contract-bindings-and-diagnostics.md`
- `docs/api/error-codes.md`
- `include/decentdb.h`
