# Error Codes

DecentDB now uses stable broad categories plus structured diagnostics for machine-readable behavior.

For vNext, bindings should treat the broad status as the compatibility floor and
`subcode` plus `retryable/permanent` as the primary handling key.

- `DDB_ABI_VERSION` is **7**.
- `ddb_last_error_json(char **out_json)` is the stable C ABI accessor for
  structured diagnostics.
- The first public slice uses a stable `subcode` family with optional SQLSTATE
  and bounded redacted context.

## Broad Status Families

Broad categories stay stable for compatibility and broad branching.

| Category | Meaning |
|---|---|
| `ERR_OK` | Success |
| `ERR_IO` | I/O failure |
| `ERR_CORRUPTION` | Corruption or invalid DB state |
| `ERR_CONSTRAINT` | Constraint violation |
| `ERR_TRANSACTION` | Transaction error |
| `ERR_SQL` | SQL parse, bind, or execution error |
| `ERR_INTERNAL` | Internal engine error |
| `ERR_PANIC` | Panic captured at ABI boundary |
| `ERR_UNSUPPORTED_FORMAT_VERSION` | Database format is newer than engine support |
| `ERR_BUSY` | Resource is busy |
| `ERR_TIMEOUT` | Timed out before run/complete |
| `ERR_CANCELED` | Request canceled before execution |
| `ERR_QUEUE_FULL` | Writer queue capacity exhausted |
| `ERR_QUEUE_CLOSED` | Writer queue is shutting down or closed |

## Structured Diagnostic JSON

The structured diagnostic is versioned and returned by `ddb_last_error_json`:

```json
{
  "version": 1,
  "code": 5,
  "code_name": "ERR_SQL",
  "subcode": "sql.relation_not_found",
  "sqlstate": "42P01",
  "message": "relation not found",
  "retryable": false,
  "permanent": true,
  "redaction": "default",
  "relation": "users",
  "docs": "errors/sql-relation-not-found"
}
```

Required fields are:

- `version`: diagnostic schema version, starts at `1`.
- `code`: numeric category.
- `code_name`: broad category name, without ABI prefix.
- `subcode`: stable machine-readable condition.
- `message`: human-readable text (non-contractual).
- `retryable`: may a retry succeed without changing input.
- `permanent`: will the same inputs keep failing until external state or policy changes.
- `redaction`: active policy (`default` initially).
- `docs`: stable diagnostics anchor.

Optional fields are omitted if unknown:

- `sqlstate`: SQLSTATE-compatible code where unambiguous.
- `relation`, `column`, `index`, `constraint`, `policy`, `branch`, `sync_scope`,
  `sync_peer`, `changeset_id`.
- `process_owner`, `wal`, `format`, `parameter`, `path`, `hint`, `doctor`,
  `details`.

## First Slice Subcodes (vNext)

These are the first diagnostics expected for stable projection.

| Category | Subcode | SQLSTATE | Retryable | Permanent | Docs |
|---|---|---|---:|---:|---|
| `ERR_SQL` | `sql.syntax` | `42601` | No | Yes | `errors/sql-syntax` |
| `ERR_SQL` | `sql.relation_not_found` | `42P01` | No | Yes | `errors/sql-relation-not-found` |
| `ERR_SQL` | `sql.column_not_found` | `42703` | No | Yes | `errors/sql-column-not-found` |
| `ERR_SQL` | `sql.ambiguous_column` | `42702` | No | Yes | `errors/sql-ambiguous-column` |
| `ERR_SQL` | `sql.parameter_missing` | `07002` | No | Yes | `errors/sql-parameter-missing` |
| `ERR_SQL` | `sql.parameter_type_mismatch` | `42804` | No | Yes | `errors/sql-parameter-type-mismatch` |
| `ERR_SQL` | `sql.unsupported_feature` | `0A000` | No | Yes | `errors/sql-unsupported-feature` |
| `ERR_CONSTRAINT` | `constraint.unique` | `23505` | No | Yes | `errors/constraint-unique` |
| `ERR_CONSTRAINT` | `constraint.not_null` | `23502` | No | Yes | `errors/constraint-not-null` |
| `ERR_CONSTRAINT` | `constraint.check` | `23514` | No | Yes | `errors/constraint-check` |
| `ERR_CONSTRAINT` | `constraint.foreign_key` | `23503` | No | Yes | `errors/constraint-foreign-key` |
| `ERR_TRANSACTION` | `transaction.no_active_transaction` | `25000` | No | Yes | `errors/transaction-no-active-transaction` |
| `ERR_TRANSACTION` | `transaction.invalid_state` | `25000` | No | Yes | `errors/transaction-invalid-state` |
| `ERR_TIMEOUT` | `queue.write_timeout` | `HYT00` | Yes | Yes | `errors/queue-write-timeout` |
| `ERR_CANCELED` | `queue.canceled` | `57014` | No | No | `errors/queue-canceled` |
| `ERR_QUEUE_FULL` | `queue.full` | `HYT00` | Yes | Yes | `errors/queue-full` |
| `ERR_QUEUE_CLOSED` | `queue.closed` | `08003` | No | Yes | `errors/queue-closed` |
| `ERR_BUSY` | `busy.writer_lock` | `55P03` | Yes | Yes | `errors/busy-writer-lock` |
| `ERR_BUSY` | `busy.reader_conflict` | `55P03` | Yes | Yes | `errors/busy-reader-conflict` |
| `ERR_TIMEOUT` | `coordination.lock_timeout` | `55P03` | Yes | Yes | `errors/coordination-lock-timeout` |
| `ERR_IO` | `coordination.sidecar_unavailable` | None | No | Yes | `errors/coordination-sidecar-unavailable` |
| `ERR_IO` | `io.permission_denied` | None | No | Yes | `errors/io-permission-denied` |
| `ERR_IO` | `io.disk_full` | None | Yes | Yes | `errors/io-disk-full` |
| `ERR_IO` | `io.not_found` | None | No | Yes | `errors/io-not-found` |
| `ERR_UNSUPPORTED_FORMAT_VERSION` | `format.unsupported_version` | None | No | Yes | `errors/format-unsupported-version` |
| `ERR_CORRUPTION` | `corruption.database_header` | None | No | Yes | `errors/corruption-database-header` |
| `ERR_CORRUPTION` | `corruption.page_checksum` | None | No | Yes | `errors/corruption-page-checksum` |
| `ERR_CORRUPTION` | `corruption.wal_frame` | None | No | Yes | `errors/corruption-wal-frame` |
| `ERR_CORRUPTION` | `corruption.wal_replay` | None | No | Yes | `errors/corruption-wal-replay` |
| `ERR_IO` | `tde.key_required` | None | No | Yes | `errors/tde-key-required` |
| `ERR_CORRUPTION` | `tde.key_mismatch` | None | No | Yes | `errors/tde-key-mismatch` |
| `ERR_SQL` | `security.policy_denied` | `42501` | No | Yes | `errors/security-policy-denied` |
| `ERR_SQL` | `security.mask_expression_invalid` | `42601` | No | Yes | `errors/security-mask-expression-invalid` |
| `ERR_SQL` | `sync.scope_not_found` | None | No | Yes | `errors/sync-scope-not-found` |
| `ERR_TRANSACTION` | `sync.retention_blocked` | None | Yes | Yes | `errors/sync-retention-blocked` |
| `ERR_SQL` | `branch.not_found` | None | No | Yes | `errors/branch-not-found` |
| `ERR_CONSTRAINT` | `branch.merge_conflict` | None | No | Yes | `errors/branch-merge-conflict` |
| `ERR_SQL` | `extension.untrusted_package` | None | No | Yes | `errors/extension-untrusted-package` |
| `ERR_PANIC` | `internal.panic_captured` | `XX000` | No | No | `errors/internal-panic-captured` |
| `ERR_INTERNAL` | `internal.invariant` | `XX000` | No | Yes | `errors/internal-invariant` |

Keep this list as the implementation expands.

## CLI / HTTP / WASM Output

CLI and HTTP JSON responses should include both the legacy short code and diagnostic
object:

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
      "sqlstate": "42P01",
      "retryable": false,
      "permanent": true,
      "redaction": "default",
      "relation": "users",
      "docs": "errors/sql-relation-not-found"
    }
  }
}
```

Do not replace the message from the diagnostic object and do not rely on parsing
free-text for behavior.

## Redaction by Default

The default diagnostics contract excludes:

- SQL parameter values
- Raw SQL text
- Encryption keys or derived key material
- Full paths
- Sync tokens
- Raw audit context values

Allowed default context includes relation/object metadata, redacted path
descriptors, process or WAL identifiers, and bounded technical counters.

## Binding Projection

All maintained bindings should keep existing exception families and add structured
diagnostic access:

- `code` and `native_code` where relevant.
- `subcode` and optional `sqlstate`.
- `retryable` and `permanent`.
- Raw diagnostic JSON or equivalent map for forward compatibility.

Bindings should parse `diagnostic.subcode` and `diagnostic.code_name` for automation
rather than matching `message` strings.

## Troubleshooting Anchors

The `docs` field uses stable anchor IDs. See:

- [`errors/sql-relation-not-found`](../user-guide/error-diagnostics.md#errors/sql-relation-not-found)
- [`errors/constraint-unique`](../user-guide/error-diagnostics.md#errors/constraint-unique)
- [`errors/queue-write-timeout`](../user-guide/error-diagnostics.md#errors/queue-write-timeout)
- [`errors/busy-writer-lock`](../user-guide/error-diagnostics.md#errors/busy-writer-lock)
- [`errors/io-disk-full`](../user-guide/error-diagnostics.md#errors/io-disk-full)
- [`errors/corruption-page-checksum`](../user-guide/error-diagnostics.md#errors/corruption-page-checksum)
- [`errors/sync-retention-blocked`](../user-guide/error-diagnostics.md#errors/sync-retention-blocked)
- [`errors/branch-merge-conflict`](../user-guide/error-diagnostics.md#errors/branch-merge-conflict)

## Compatibility Checklist

- Existing broad status values must not change.
- Required diagnostic fields may not be removed.
- New optional fields are backward-compatible.
- Subcodes must keep stable spelling and meaning during a compatibility cycle.
- Changing `retryable` or `permanent` semantics requires release note coverage.
- Message text changes are allowed and should not be used as contracts.

## Release Guardrails

Run the phase-5 consistency check when preparing a release:

```bash
python scripts/validate_error_diagnostics.py
```

That check validates:

- First-slice subcodes are documented.
- Troubleshooting anchor coverage for all first-slice anchors.
- Required docs touchpoints (`error-codes`, `c-cpp`, and `doctor`).
- Binding smoke fixture coverage status.
