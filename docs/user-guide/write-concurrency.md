# Write Concurrency

DecentDB keeps a one-writer/many-readers concurrency model. The engine does not
hide a multi-writer server behind the API. Instead, it provides cross-process
WAL coordination for local on-disk databases and an engine-owned write queue for
applications and bindings that want predictable concurrent writes without
building their own writer dispatcher.

## Direct Writes

The existing direct APIs remain the lowest-overhead path. Use them when your
application already has one writer, runs bulk loads, or manages explicit
transactions directly.

Direct writes keep the usual durable behavior: under the default
`WalSyncMode::Full`, success means the WAL commit has been synced before the
call returns.

## Queued Writes

Queued APIs submit self-contained SQL work into a bounded FIFO queue. An admitted
request either:

- runs through the existing single writer and returns its normal SQL result;
- fails with the original SQL, constraint, transaction, or I/O error;
- times out before execution starts; or
- is canceled before execution starts.

Once execution has started, DecentDB returns the definitive execution or commit
result. Cancellation during commit or WAL sync does not pretend that the
transaction was canceled.

Queued execution rejects explicit transaction-control SQL (`BEGIN`, `COMMIT`,
`ROLLBACK`, and savepoint control). Use direct transaction APIs for long-lived
explicit transactions.

For concurrent read/write workloads, use separate reader handles or binding
connections and route write workers through the shared queued writer handle or
binding queue mode. This matches the one-writer/many-readers model used by the
native benchmark coverage.

## Strict Group Commit

Queued writes use strict group commit by default. Several ready queued commits
may share one physical WAL sync:

```text
write tx1 commit frame
write tx2 commit frame
write tx3 commit frame
sync WAL once
ack tx1, tx2, tx3
```

This is different from async commit. Strict group commit shares sync cost but
does not acknowledge a successful queued write before the covering sync
completes under synchronous WAL modes.

`WalSyncMode::AsyncCommit` remains a separate opt-in mode that acknowledges
commits before the covering fsync.

## Cross-Process WAL Coordination

Native local files now coordinate through a rebuildable `<database>.coord`
sidecar. The sidecar serializes writer and checkpoint ownership with OS file
locks, tracks reader slots from every process, and publishes WAL/checkpoint
generations so each process refreshes its local WAL index before reads, writes,
and checkpoints.

The default mode is `auto`: local on-disk databases use process coordination
when the VFS supports byte-range file locks, while in-memory and unsupported
VFSes stay in single-process mode. Use `required` when an application must fail
instead of silently running without cross-process protection. Use
`single_process_unsafe` only for immutable inspection or tests that knowingly
avoid concurrent native process access.

Queryable diagnostics:

```sql
SELECT * FROM sys.process_coordination;
SELECT * FROM sys.process_readers;
SELECT * FROM sys.process_lock_metrics;
```

`decentdb doctor` includes WAL findings for cross-process reader retention and
currently held writer/checkpoint locks.

## Configuration

Rust `DbConfig` and C ABI open options expose:

| Option | Default | Meaning |
|---|---:|---|
| `process_coordination` | `auto` | `auto`, `required`, or `single_process_unsafe`. |
| `process_coordination_timeout_ms` | `30000` | Bounded wait for cross-process writer/checkpoint/reader-slot locks. |
| `write_queue_enabled` | `false` | Lets high-level bindings opt into queued execution for their normal paths. Explicit queued APIs can still be called directly. |
| `write_queue_capacity` | `1024` | Maximum admitted requests waiting for execution. |
| `write_queue_default_timeout_ms` | `0` | Default queue timeout; `0` means no configured default. |
| `write_queue_strict_group_commit` | `true` | Enables strict queued group commit. |
| `write_queue_max_batch` | `64` | Maximum ready requests drained in one queue executor pass. |
| `write_queue_max_group_delay_us` | `0` | Optional delay to collect more ready writes; the default avoids sleeping for single-writer workloads. |

## C ABI

The stable C ABI exposes:

- `ddb_db_execute_queued(...)`
- `ddb_db_write_queue_metrics(...)`
- `DDB_ERR_BUSY`
- `DDB_ERR_TIMEOUT`
- `DDB_ERR_CANCELED`
- `DDB_ERR_QUEUE_FULL`
- `DDB_ERR_QUEUE_CLOSED`

Pass `DDB_WRITE_QUEUE_TIMEOUT_DEFAULT` to `ddb_db_execute_queued` to use the
database configured default timeout. Pass `0` for immediate timeout behavior.

## Metrics

Queue metrics include current depth, admissions, rejections, timeouts,
cancellations, executions, commits, failures, grouped batches, physical syncs,
and estimated physical syncs saved. These counters are now also available via
`SELECT * FROM sys.write_queue_metrics;` alongside Rust and C ABI snapshots.

## Limitations

- The write queue is in-process. Cross-process coordination serializes file
  ownership but does not create a durable inter-process job queue.
- Network filesystems, cloud-synced folders, and unproven FUSE-like filesystems
  are unsupported for coordinated writes in v1.
- Queued explicit transaction leases are not part of this release.
- Some high-level provider prepared-statement paths remain direct until the C
  ABI grows a queued prepared-statement contract. Use the binding's explicit
  queued helper when you need queued execution for self-contained SQL today.
- Browser/Web strict durability depends on platform storage guarantees and is
  documented by the web binding when unsupported.
