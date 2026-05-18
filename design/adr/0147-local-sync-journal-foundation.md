# ADR 0147: Local Sync Journal Foundation
**Date:** 2026-05-17
**Status:** Accepted

## Context

DecentDB needs first-class local-first sync support as defined in
`design/WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`. Before peer networking,
import/export apply, scoped sync, conflict resolution, or branch/merge can be
built, the engine must have a durable local changeset capture mechanism. Every
committed INSERT, UPDATE, or DELETE on a persistent user table must produce a
machine-readable journal record when sync is enabled.

Without a journal, consumers cannot reconstruct which rows changed in which
transaction, which is the fundamental building block for any sync protocol.

## Decision

The initial sync journal uses a **separate sidecar file** named
`<db path>.sync-journal` with **newline-delimited JSON records** (one complete
JSON object per line, no trailing comma, no top-level array wrapper).

### Journal record JSON shape

```json
{"schema_version":1,"sequence":1,"replica_id":"node-1","transaction_lsn":5,"table":"users","operation":"insert","primary_key":{"id":42},"after":{"id":42,"name":"Alice"},"schema_cookie":3,"committed_at_micros":1715911200000000}
```

- `schema_version`: always `1` for this format
- `sequence`: monotonic u64, scoped to the replica, incremented per journal record
- `replica_id`: user-supplied string identifying this replica
- `transaction_lsn`: the WAL LSN produced by the commit that generated this record
- `table`: the persistent user table name
- `operation`: `"insert"`, `"update"`, or `"delete"`
- `primary_key`: JSON object mapping primary key column names to projected values
- `after`: JSON object of the full post-mutation row for insert/update; `null` for delete
- `schema_cookie`: the catalog schema cookie at commit time
- `committed_at_micros`: `i64` microsecond timestamp from `SystemTime`; informational only

### Durability

Journal append is **flushed with `sync_data()` (fdatasync)** before the
transaction is reported committed when sync is enabled. If sync is disabled,
no journal file I/O occurs.

### Limitation

The current commit plumbing writes WAL pages and the commit frame before
journal records. If journal append fails after a successful WAL commit, the
error is returned to the caller and the transaction is reported as failed,
but the WAL has already been durably written. The database remains
**consistent** (the user data is intact in the WAL), but the sync journal may
be missing records for that transaction. A future slice can address this by
either writing the journal before the WAL commit frame or by adding journal
recovery that detects orphaned LSNs.

### Metadata table

Sync metadata is stored in an internal table `__decentdb_sync_metadata`:

| Column | Type | Purpose |
|---|---|---|
| `key` | TEXT PRIMARY KEY | Metadata key |
| `value` | TEXT NOT NULL | Metadata value |

Keys:
- `enabled`: `"true"` or `"false"`
- `replica_id`: user-supplied string
- `next_sequence`: decimal u64 string

This table is created lazily only when `sync_init_replica` or
`sync_set_enabled(true)` is called. It is filtered from user-facing table
lists (`list_tables`, `get_schema_snapshot`) by filtering names that start
with `__decentdb_`.

### Capture scope

- Only successful committed INSERT/UPDATE/DELETE on persistent user tables
- No DDL, temp table, or internal table mutations
- Rollbacks produce no journal records
- Savepoint rollback does not journal rolled-back changes

### Sync-disabled overhead

When sync is disabled, the commit path incurs exactly **one cheap branch**
(`if self.sync_is_enabled()`) to skip journal capture.

## Rationale

1. **Sidecar file** over embedding in the WAL: the sync journal has different
   lifecycle requirements (longer retention, different truncation policy, may
   be read independently). The WAL is checkpoint-cycled for crash recovery;
   the sync journal persists across checkpoints.

2. **Newline-delimited JSON** over binary: human-debuggable, trivial to
   parse in any language, suitable for the MVP. A future binary format
   (e.g., Protocol Buffers or a custom columnar layout) can be introduced
   with a format version bump.

3. **Metadata table** over file header: allows atomic read-modify-write via
   SQL transactions, integrates with existing engine infrastructure, and
   survives checkpoints/VACUUM without special handling.

4. **`sync_data()` not `sync_all()`** for journal writes: fdatasync is
   sufficient for append-only data where the file size metadata is not
   critical for correctness.

## Alternatives Considered

1. **Embed sync records in WAL commit frames**: rejected because the WAL is
   checkpoint-cycled for crash recovery on a different schedule than sync
   journal retention. Mixing the two would force either premature sync
   journal truncation or bloated WAL files.

2. **Store journal in a SQL table**: rejected because append-only write path
   through the engine's full SQL machinery adds overhead (parsing, planning,
   constraint checking, index maintenance) compared to raw file append +
   fsync.

3. **Use `serde_json` Value-based records**: rejected for the public API
   (records should have typed fields), but used internally for serialization
   of `primary_key` and `after` fields which are dynamic JSON objects.

4. **Binary format (Protobuf/Cap'n Proto)**: deferred to a future slice.
   JSON is adequate for the MVP and the format version field allows future
   migration.

## Trade-offs

**Positive:**
- Minimal engine intrusion: sync.rs is a self-contained module
- No WAL format changes
- No database file format version bump
- Sync-disabled overhead is one branch

**Negative:**
- JSON is not the most compact format; large batch commits will produce
  proportionally large journal records
- The WAL-may-have-been-written limitation means edge-case recovery requires
  detecting orphaned journal LSNs in a future slice
- Metadata stored in a SQL table means metadata reads go through the SQL
  execution path; acceptable for infrequent metadata reads

## Implementation Notes

1. `FileKind::SyncJournal` is added to the VFS for proper I/O classification
   and fault-injection support.
2. Sync mutation records are collected during DML execution and flushed after
   WAL commit succeeds.
3. The sync module exposes only stable public types through `lib.rs`; internal
   types remain `pub(crate)`.
4. CLI commands are grouped under `decentdb sync` with JSON and table output
   formats.

## References

- `design/WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md` — full sync spec
- `design/adr/0147-local-sync-journal-foundation.md` — this document
- `crates/decentdb/src/sync.rs` — implementation
- `crates/decentdb/src/vfs/mod.rs` — `FileKind` enum
