# Error Diagnostics and Troubleshooting

This page resolves stable diagnostic anchors returned in the `docs` field of
structured diagnostics.

Every anchored entry links to concise remediation guidance for the first-slice
subcodes.

## Common Anchor Pattern

An error response contains:

- `subcode` for machine handling
- `docs` for the human troubleshooting anchor
- `retryable` and `permanent` for guidance automation
- `doctor` when deeper inspection is needed

## <a id="errors/sql-syntax"></a> `errors/sql-syntax`

- Fix the SQL statement syntax.
- If generated SQL is involved, log the generated text under a debug switch and
  reproduce with prepared statements disabled.
- For schema-level syntax failures on DDL, validate SQL profile compatibility.

## <a id="errors/sql-relation-not-found"></a> `errors/sql-relation-not-found`

- Confirm `db.prepare` and statement execution use the expected attached schema.
- Ensure migrations or DDL were run before the failing statement.
- Check the current default database/branch before retrying.

## <a id="errors/sql-column-not-found"></a> `errors/sql-column-not-found`

- Confirm the query projection and object shape.
- Verify quoted identifier case and quoting style.
- Align app-side serializers with current schema and migration level.

## <a id="errors/sql-ambiguous-column"></a> `errors/sql-ambiguous-column`

- Narrow references with table qualification in each ambiguous table.
- Reduce wildcard projection in joins where duplicate column names exist.

## <a id="errors/sql-parameter-missing"></a> `errors/sql-parameter-missing`

- Verify bound parameter count and ordering.
- Ensure caller code always binds required positional values.

## <a id="errors/sql-parameter-type-mismatch"></a> `errors/sql-parameter-type-mismatch`

- Match parameter type and column type expectations.
- Check migrations for schema drift.

## <a id="errors/sql-unsupported-feature"></a> `errors/sql-unsupported-feature`

- Check profile SQL feature support docs.
- Confirm the current database feature profile and runtime capabilities.

## <a id="errors/constraint-unique"></a> `errors/constraint-unique`

- Resolve duplicates before insert/update.
- Switch conflict behavior to update path (`ON CONFLICT`) where supported.

## <a id="errors/constraint-not-null"></a> `errors/constraint-not-null`

- Write explicit values for non-null fields.
- Update migration defaults or backfill before write.

## <a id="errors/constraint-check"></a> `errors/constraint-check`

- Check check constraints and generated values.
- Validate application values before commit.

## <a id="errors/constraint-foreign-key"></a> `errors/constraint-foreign-key`

- Ensure parent rows exist before inserting child rows.
- Clean dependent rows when deleting referenced parents.

## <a id="errors/sync-changeset-conflict"></a> `errors/sync-changeset-conflict`

- Inspect conflict diagnostics from `sync scope` tooling and journal metadata.
- Retry after conflict resolver workflow or manual merge path.

## <a id="errors/transaction-no-active-transaction"></a> `errors/transaction-no-active-transaction`

- Open an explicit transaction before using transaction-only operations.
- Ensure commit/rollback paths are deterministic.

## <a id="errors/transaction-invalid-state"></a> `errors/transaction-invalid-state`

- Verify lifecycle transitions around nested transaction operations.
- Keep savepoint and autocommit handling consistent per command path.

## <a id="errors/queue-write-timeout"></a> `errors/queue-write-timeout`

- Reduce burst concurrency or increase queue timeout in a controlled retry policy.
- Reduce transaction duration so queue throughput remains stable.

## <a id="errors/queue-canceled"></a> `errors/queue-canceled`

- Confirm the request was not canceled by explicit timeout or lifecycle close.
- Retry only after caller intent and open-handle state are valid.

## <a id="errors/queue-full"></a> `errors/queue-full`

- Increase queue capacity or reduce in-process concurrent writers.
- Use direct transaction APIs where queue usage is not required.

## <a id="errors/queue-closed"></a> `errors/queue-closed`

- Keep handle lifecycle explicit around shutdown.
- Re-open a fresh handle after close/shutdown completion.

## <a id="errors/busy-writer-lock"></a> `errors/busy-writer-lock`

- Retry with backoff.
- Inspect `sys.process_lock_metrics` and `sys.process_readers` for active hold.
- Use `doctor` handoff when lock-holder ownership is unclear.

## <a id="errors/busy-reader-conflict"></a> `errors/busy-reader-conflict`

- Retry after read-side coordination releases.
- Review read transaction length where applicable.

## <a id="errors/coordination-lock-timeout"></a> `errors/coordination-lock-timeout`

- Review process coordination timeout and sidecar ownership configuration.
- Verify sidecar file visibility and restart stale holders if needed.

## <a id="errors/coordination-sidecar-unavailable"></a> `errors/coordination-sidecar-unavailable`

- Verify writable filesystem support and sidecar recreation capability.
- Confirm `process_coordination` mode with deployment constraints.
- Run diagnostics with `decentdb doctor --format json`.

## <a id="errors/io-permission-denied"></a> `errors/io-permission-denied`

- Check directory permissions and storage mount mode.
- Confirm runtime identity can read/write the configured paths.

## <a id="errors/io-disk-full"></a> `errors/io-disk-full`

- Increase available storage.
- Rotate WAL and checkpoint retention.
- Retry after cleanup or capacity expansion.

## <a id="errors/io-not-found"></a> `errors/io-not-found`

- Verify path, directory, and filename case sensitivity.
- Re-check backup/restore inputs and migration source paths.

## <a id="errors/format-unsupported-version"></a> `errors/format-unsupported-version`

- Upgrade engine binaries to match database format.
- Run migration tooling only through supported release lanes.

## <a id="errors/corruption-database-header"></a> `errors/corruption-database-header`

- Run a read-only verification workflow.
- Use `decentdb doctor --format json` and compare `collected` diagnostics.

## <a id="errors/corruption-page-checksum"></a> `errors/corruption-page-checksum`

- Stop writes and preserve copies.
- Use backup and verify tooling to isolate damaged pages.

## <a id="errors/corruption-wal-frame"></a> `errors/corruption-wal-frame`

- Inspect WAL and checkpoint settings for truncation behavior.
- Follow WAL handoff and reader retention guidance before repair attempts.

## <a id="errors/corruption-wal-replay"></a> `errors/corruption-wal-replay`

- Compare recovery sequence and retained frame set.
- Coordinate any restore attempts with `doctor`/sync consistency checks.

## <a id="errors/tde-key-required"></a> `errors/tde-key-required`

- Check open-option and key-provider configuration.
- Verify key rotation and fallback behavior in managed key stores.

## <a id="errors/tde-key-mismatch"></a> `errors/tde-key-mismatch`

- Confirm key identity and expected scope for the target database.
- Confirm environment-specific key lookup behavior.

## <a id="errors/security-policy-denied"></a> `errors/security-policy-denied`

- Verify policy name and active policy scope.
- Check caller role or effective policy context.

## <a id="errors/security-mask-expression-invalid"></a> `errors/security-mask-expression-invalid`

- Validate mask expression syntax during schema design.
- Check schema migration order and expression compatibility.

## <a id="errors/sync-scope-not-found"></a> `errors/sync-scope-not-found`

- Verify scope name and schema cookie match the configured sync surface.
- Re-run scope bootstrap when environment moved across branches.

## <a id="errors/sync-retention-blocked"></a> `errors/sync-retention-blocked`

- Inspect sync peer metrics and retention blockers.
- Resolve open branch or shape blockers and retry later.

## <a id="errors/branch-not-found"></a> `errors/branch-not-found`

- Confirm branch exists in branch metadata.
- Check default/active branch context before dispatching branch commands.

## <a id="errors/branch-merge-conflict"></a> `errors/branch-merge-conflict`

- Inspect conflict metadata and resolve deterministic merge policy.
- Retry after manual branch reconciliation.

## <a id="errors/extension-untrusted-package"></a> `errors/extension-untrusted-package`

- Confirm extension package signature and trust metadata.
- Reinstall from a trusted extension source.

## <a id="errors/internal-panic-captured"></a> `errors/internal-panic-captured`

- Treat this as an engine stability regression signal.
- File a reproducible issue and include diagnostic JSON, release hash, and SQL
  reproduction steps.

## <a id="errors/internal-invariant"></a> `errors/internal-invariant`

- Escalate via support.
- Preserve diagnostic logs and a deterministic reproducer from the same runtime
  family.

## Doctor Handoff

Some diagnostics include a `doctor` object with `kind` and `command` fields for
deeper local inspection. This handoff is redacted:

```json
{
  "kind": "process_coordination",
  "command": "decentdb doctor --db <redacted> --format=json"
}
```

Run the provided command with the application database path for the current
process context.
