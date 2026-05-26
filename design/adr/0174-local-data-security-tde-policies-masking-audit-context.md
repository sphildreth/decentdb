# ADR 0174: Local Data Security TDE, Policies, Masking, and Audit Context

**Status:** Accepted
**Date:** 2026-05-26

## Context

Future Win #1 calls for local data security that makes DecentDB stronger than
typical embedded engines while preserving the core one-writer/many-readers
architecture. The feature needs to cover encryption-at-rest for local files,
row policies, masked projections, and audit context that embedders can set and
query.

The storage stack already funnels database, WAL, and sync journal I/O through
the VFS abstraction. Hidden `__decentdb_*` tables already exist for durable
engine metadata. Those two boundaries let us add security without weakening
the pager, WAL, or catalog invariants.

## Decision

### Transparent local file encryption

DecentDB will implement TDE as an optional VFS wrapper configured through
`DbConfig::encryption`.

- The wrapper encrypts logical bytes for `Database`, `Wal`, and `SyncJournal`
  files.
- Each encrypted physical file starts with a small plaintext TDE prefix that
  stores the algorithm version, file kind, random per-file salt, and a key
  verifier. The prefix never stores key material.
- Logical file offsets remain unchanged for the pager, WAL, and sync journal.
  The wrapper maps logical offset `N` to physical offset `prefix_len + N`.
- WAL truncation to logical length `0` keeps the prefix physically present.
- `Db::save_as` preserves encryption when the source database is encrypted.

The initial algorithm is `ChaCha20-SHA256`:

- Application key bytes are SHA-256-derived into a master key.
- Each file derives an independent stream key and nonce from the master key,
  file salt, and file kind.
- Existing page/header/WAL validation remains responsible for corruption
  detection. This is encryption-at-rest confidentiality, not a new keyed
  authenticated page format.

This avoids a database page-format or WAL-format change. A future authenticated
page format can be introduced with a format-version ADR if DecentDB needs
per-page AEAD tags.

### Key handling

`DbEncryptionConfig` accepts application-owned key bytes. DecentDB does not
perform platform key storage, password prompts, or telemetry of key material.

- Key `Debug` output is redacted.
- Key buffers are zeroized on drop where DecentDB owns a copy.
- Wrong keys fail at open via the per-file verifier before page decoding.
- Opening an encrypted database without an encryption config fails as a
  corrupted/unknown header, and callers can retry with `DbConfig::encryption`.

### Row policies and masks

Policies and masks are durable metadata in hidden system tables:

- `__decentdb_policies(policy_name, table_name, using_sql, enabled, created_at_micros)`
- `__decentdb_masks(mask_name, table_name, column_name, expression_sql, enabled, created_at_micros)`

`CREATE POLICY`, `DROP POLICY`, `ALTER POLICY ENABLE/DISABLE`,
`CREATE MASK`, `DROP MASK`, and `ALTER MASK ENABLE/DISABLE` mutate those
tables. The engine applies enabled policies when building base table datasets
for user queries and applies enabled masks during projection.

Policies do not affect internal integrity checks, catalog maintenance,
constraint enforcement, index rebuilds, or sync/change capture. Internal code
that must see physical rows uses existing internal table sources.

### Audit context

Audit context is per-`Db` handle session state. Embedders can set it through
Rust and C ABI APIs, and SQL can read it with `current_audit_context(key)`.
`SET AUDIT CONTEXT key = value` updates it from SQL.

The current context is queryable through `sys_audit_context`. Security DDL and
user writes append durable rows to `__decentdb_audit_events` with the actor,
tenant, operation, target object, statement text where available, and context
JSON. Audit rows are normal encrypted database data when TDE is enabled.

## Consequences

- No file-format bump is required for TDE because logical database bytes and
  WAL bytes are unchanged.
- Encrypted files are not readable by pre-TDE builds because the physical file
  begins with the TDE prefix rather than the database or WAL magic.
- Copying encrypted files must include the prefix. Logical backup APIs that use
  `Db::save_as` preserve encryption by default.
- Security metadata is durable and branch/sync-visible as ordinary hidden
  tables unless higher-level sync scope configuration excludes it.
- The first policy/mask implementation intentionally prioritizes correctness
  over planner pushdown. Fast paths are bypassed when security metadata is
  active so the central evaluator applies row filtering and masking uniformly.

