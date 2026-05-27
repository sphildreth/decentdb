# Cross-Process WAL Coordination

**Date:** 2026-05-27
**Status:** Design accepted; implementation not started
**Future Version:** vNext
**Roadmap:** [`FUTURE_WINS.md`](FUTURE_WINS.md)
**Document Type:** Implementation SPEC
**Audience:** Core engine developers, WAL/pager maintainers, VFS maintainers,
CLI maintainers, binding maintainers, documentation authors, benchmark
maintainers, coding agents

**Governing ADRs:**

- [`adr/0177-cross-process-coordination-sidecar-and-locking.md`](adr/0177-cross-process-coordination-sidecar-and-locking.md)
- [`adr/0178-cross-process-reader-retention-and-wal-refresh.md`](adr/0178-cross-process-reader-retention-and-wal-refresh.md)
- [`adr/0179-cross-process-public-contract-bindings-and-diagnostics.md`](adr/0179-cross-process-public-contract-bindings-and-diagnostics.md)
- [`adr/0180-database-identity-for-coordination-sidecars.md`](adr/0180-database-identity-for-coordination-sidecars.md)

**Implementation status, 2026-05-27:** Not implemented. DecentDB has
same-process shared WAL visibility through ADR 0117 and an in-process write
queue through ADR 0162. This spec defines the next runtime contract: safe
coordination when multiple native OS processes open the same on-disk database.

**Related inputs:**

- [`FUTURE_WINS.md`](FUTURE_WINS.md)
- [`SPEC.md`](SPEC.md)
- [`TESTING_STRATEGY.md`](TESTING_STRATEGY.md)
- [`adr/0016-database-header-checksum.md`](adr/0016-database-header-checksum.md)
- [`adr/0018-checkpointing-reader-count-mechanism.md`](adr/0018-checkpointing-reader-count-mechanism.md)
- [`adr/0019-wal-retention-for-active-readers.md`](adr/0019-wal-retention-for-active-readers.md)
- [`adr/0117-shared-wal-registry.md`](adr/0117-shared-wal-registry.md)
- [`adr/0119-rust-vfs-pread-pwrite.md`](adr/0119-rust-vfs-pread-pwrite.md)
- [`adr/0131-legacy-format-migrations.md`](adr/0131-legacy-format-migrations.md)
- [`adr/0141-paged-on-disk-wal-index.md`](adr/0141-paged-on-disk-wal-index.md)
- [`adr/0058-background-incremental-checkpoint-worker.md`](adr/0058-background-incremental-checkpoint-worker.md)
- [`adr/0162-engine-owned-write-queue-strict-group-commit.md`](adr/0162-engine-owned-write-queue-strict-group-commit.md)
- [`adr/0163-operational-sys-metrics.md`](adr/0163-operational-sys-metrics.md)
- [`docs/user-guide/write-concurrency.md`](../docs/user-guide/write-concurrency.md)
- [`docs/architecture/wal.md`](../docs/architecture/wal.md)

---

## 1. Executive Summary

DecentDB currently supports one writer and many readers inside one process, and
separately opened handles in that process share a WAL instance through a
process-global registry. That makes committed data visible across same-process
connections without reopening.

That is not enough for desktop and local-first applications. Real applications
often have an app process, CLI process, background sync process, helper process,
or support/doctor process touching the same database file. Today those processes
do not share the in-memory WAL registry, writer lock, reader registry, WAL index,
or checkpoint retention state.

This win extends DecentDB's one-writer/many-readers model across native OS
process boundaries for local on-disk databases. The goal is not hidden
multi-writer MVCC and not a server. The goal is boring, safe file coordination:
only one process writes or checkpoints at a time; readers in every process keep
stable snapshots; checkpoints never truncate WAL frames required by readers in
another process; stale owners are diagnosable and recoverable; and every binding
inherits the behavior through normal open options and SQL.

This is a top-priority adoption win because SQLite's practical process-safe file
access is a major reason teams choose it for Electron, Tauri, desktop, CLI,
agent, and local-first applications.

## 2. Product Goals

- Safe concurrent native OS process opens for the same on-disk database file.
- Preserve DecentDB's one-writer/many-readers model across processes.
- Preserve durable commit semantics for the existing WAL sync modes.
- Allow app, CLI, background worker, and support tooling processes to coexist.
- Keep readers nonblocking for other readers.
- Serialize writers and checkpoint/truncation operations with bounded wait and
  clear timeout behavior.
- Preserve snapshot isolation when readers and checkpointers live in different
  processes.
- Refresh per-process WAL indexes when another process appends, checkpoints, or
  recovers the WAL.
- Detect and clean stale reader slots after process crashes without breaking
  valid long-running readers.
- Recover cleanly when a process crashes before, during, or after WAL append,
  commit, checkpoint, or coordination metadata publication.
- Expose process-level blockers through SQL diagnostics and CLI Doctor.
- Make binding behavior consistent through C ABI open options and normal SQL.
- Keep the implementation portable across Linux, macOS, and Windows local
  filesystems.

## 3. Non-Goals

- No multi-writer engine. There is still only one active writer.
- No server process, daemon, broker, or lock manager requirement.
- No hidden cross-process global write queue in v1.
- No distributed lock manager.
- No network filesystem support guarantee in v1. NFS, SMB, cloud-synced folders,
  and FUSE-like filesystems are explicitly unsupported unless a later support
  matrix proves a specific configuration safe.
- No mmap-backed shared page cache. ADR 0119's positional I/O direction remains
  intact.
- No browser OPFS multi-tab rewrite. Browser owner coordination remains related
  but distinct from native OS process coordination.
- No automatic killing of other processes. DecentDB may report blockers and clean
  stale slots after proof of death, but it must not terminate host processes.
- No new per-binding write coordination implementation. The Rust engine owns the
  process coordination contract.
- No coordinated live-WAL read-only opens without writable coordination sidecar
  access in v1. Immutable/forensic open modes that never register readers are a
  separate future design and must not be implied by this feature.

## 4. Current Context

Relevant shipped foundations:

- ADR 0117: same-process handles share a WAL through a process-global registry
  keyed by canonical path.
- ADR 0162: queued writes are engine-owned but per `Db`; separately opened
  handles serialize through the shared WAL writer lock but own independent queue
  admission state.
- ADR 0019: WAL frames required by active readers must never be truncated.
- ADR 0119: DecentDB uses positional I/O, not mmap, to avoid cross-process
  truncation/SIGBUS hazards.
- ADR 0141: WAL indexes may use paged sidecars, but in-memory WAL state remains
  process-local.
- ADR 0163: `sys.*` operational metrics are the canonical diagnostics surface.

Current limitations:

- Process A and Process B do not share `SharedWalInner`.
- Process B cannot rely on Process A's in-memory WAL index.
- Process B cannot see Process A's active readers when checkpointing.
- Process B cannot know whether Process A is the current writer except through
  best-effort OS file behavior.
- `decentdb checkpoint`, `decentdb doctor`, import/export, sync workers, and
  support tools cannot safely assume the app process is coordinated.

## 5. User-Facing Contract

### 5.1 Default Behavior

For native local on-disk databases, cross-process coordination is enabled by
default once this win ships.

`:memory:` databases remain process-local and do not participate.

Custom VFS implementations must either implement the process-locking capability
required by this spec or opt into an explicitly unsafe single-process mode for
testing/special deployments. Safe local file behavior is the default.

### 5.2 Coordination Modes

Add a Rust configuration enum and C ABI open option:

```text
process_coordination = auto | required | single_process_unsafe
```

Semantics:

| Mode | Meaning |
|---|---|
| `auto` | Default. Enable cross-process coordination on supported native local on-disk files. `:memory:` remains local. Unsupported VFS opens fail unless the VFS is explicitly memory-only. |
| `required` | Open fails unless the VFS provides the full coordination contract. Useful for tests and production apps that want no silent fallback. |
| `single_process_unsafe` | Bypasses cross-process coordination for benchmarks, legacy tests, or tightly controlled single-process embeddings. Must be documented as unsafe when multiple processes can access the same file. |

The existing write queue options remain separate. The queue is still in-process
in v1. Cross-process serialization happens at the WAL writer/checkpoint locks.
Queued write executors acquire the cross-process writer lock for one bounded
local drain batch, not indefinitely. The batch is capped by
`write_queue_max_batch` and `write_queue_max_group_delay_us`; the lock is
released before the executor attempts another drain pass. This preserves strict
group commit inside a process while giving other processes regular writer-lock
opportunities.

### 5.3 Busy Timeout Behavior

Existing busy/queue timeout concepts must be applied consistently:

- Direct writes waiting for another process's writer lock honor the configured
  busy timeout.
- Queued writes can wait in the local process queue and then wait for the
  cross-process writer lock when executing.
- Checkpoints waiting for a writer lock honor the checkpoint/busy timeout policy
  defined by the implementation ADR.
- Timeouts return stable busy/timeout errors through Rust and C ABI.
- Error context should identify that the blocker is cross-process when known.

### 5.4 Read-Only Opens

Read-only opens still need coordination if they read a database with a live WAL.
V1 requires write access to the coordination sidecar even when the database file
itself is opened read-only, because reader slots and liveness records must be
updated.

If a process cannot create or update the coordination sidecar, the open must fail
with a clear error. `single_process_unsafe` is not a coordinated read-only mode;
it is an explicit safety opt-out for controlled single-process deployments.
A future immutable/snapshot mode may support fully checkpointed read-only
inspection without sidecar writes, but that is outside v1.

### 5.5 Process Safety Claims

After this win, DecentDB may claim:

- multiple native processes can safely open the same local database file;
- at most one writer/checkpointer mutates WAL/database state at a time;
- readers in different processes get stable snapshots;
- stale reader slots from dead processes are recoverable;
- CLI/app/background-worker coexistence is supported on the documented platforms.

DecentDB must not claim:

- multi-writer parallel commits;
- network filesystem safety;
- server-grade distributed coordination;
- browser OPFS and native file locking are the same contract.

## 6. Coordination Files And Locking Model

### 6.1 Sidecar Files

V1 uses a coordination sidecar next to the database:

```text
database.ddb
database.ddb.wal
database.ddb.walidx      # existing/future WAL index sidecar where configured
database.ddb.coord       # new coordination sidecar
```

The exact suffix is part of the ADR contract and should be treated as stable
once released.

The coordination sidecar contains:

- magic/version/endian marker;
- database identity fingerprint;
- coordinator generation;
- last published WAL end LSN and WAL file length;
- last checkpoint generation and checkpoint LSN;
- writer/checkpoint owner metadata;
- fixed 64-slot reader table;
- metrics counters needed by diagnostics;
- checksums for records that must survive torn sidecar writes.

Checksum granularity:

- the sidecar header has its own checksum over the fixed header fields;
- each variable or repeated record family, including reader slots and owner
  records, has an independent checksum and generation;
- a torn reader-slot or owner-record write invalidates that record but does not
  invalidate the entire sidecar;
- a torn or invalid header write requires sidecar repair/rebuild under the
  coordinator init lock;
- invalid records that cannot be proven safe are treated as active retention
  blockers until repair proves otherwise.

The coordination sidecar is not user data and should be rebuildable from the
database header and WAL under the coordinator initialization lock. Its own
sidecar format has an independent version. Per ADR 0180, this feature also
requires adding a stable database identity to the database header; that header
change requires a database format version bump and the `decentdb-migrate`
read-only parser update required by ADR 0131.

### 6.2 Lock Ranges

V1 uses byte-range locks on the coordination sidecar through a VFS locking
abstraction:

| Lock | Purpose |
|---|---|
| Coordinator init lock | Create/repair/reinitialize the sidecar safely. |
| Writer lock | Serialize WAL appends and write transactions across processes. |
| Checkpoint lock | Aliases the writer lock in v1. Checkpoint copyback/truncation and write transactions are mutually exclusive across processes. |
| Reader slot lock | Prove ownership/liveness for an active reader slot. |
| Metadata publish lock | Serialize updates to coordination header/generation fields if not covered by writer/checkpoint locks. |

The VFS abstraction must map to native local file locking on Linux, macOS, and
Windows. If a platform cannot provide the required lock behavior, coordination is
unsupported on that platform/VFS.

### 6.3 No Shared Memory Requirement

V1 must not require mmap or shared-memory pages. The sidecar is read and written
with positional I/O. This keeps the implementation aligned with ADR 0119 and
reduces crash behavior surprises.

### 6.4 Sidecar Lifecycle

The coordination sidecar is rebuildable. Losing `<db>.coord` is safe if no other
process is concurrently attached; the next opener rebuilds it under the
coordinator init lock by reading the database header and WAL.

Sidecar rebuild uses a two-phase protocol:

1. acquire the coordinator init lock;
2. create or update the sidecar header with a `rebuilding` state, generation,
   and valid header checksum;
3. release the init lock while scanning a large WAL, so other openers can see
   the rebuilding state and wait or time out rather than racing initialization;
4. reacquire the init lock, validate that the database identity and WAL
   generation inputs have not changed, and publish the completed sidecar;
5. if validation fails, retry or fail with a clear busy/rebuild error.

If an implementation temporarily holds the init lock across the full scan, it
must honor the configured busy timeout and document that WAL size directly
affects first-open latency after sidecar loss. A partially rebuilt sidecar is
treated the same as a corrupt sidecar: the next opener acquires the init lock
and rebuilds again.

Tools that move, rename, copy, delete, restore, or replace a database should
treat `.ddb`, `.ddb.wal`, `.ddb.walidx` where present, and `.ddb.coord` as one
database artifact set. If a stale `.coord` exists beside a replaced database,
the database identity fingerprint must prevent accidental reuse. Doctor must
report sidecar/database identity mismatch and the engine must rebuild or fail
safe rather than trusting mismatched coordination metadata.

## 7. Writer Coordination

### 7.1 Writer Acquire

Before any write transaction mutates WAL state, the process must:

1. Ensure the coordination sidecar exists and is valid.
2. Acquire the cross-process writer lock.
3. Refresh its local WAL index to the latest published or recoverable WAL end.
4. Reconcile database header/checkpoint generation changes made by other
   processes.
5. Begin the existing single-writer transaction path.

If the writer lock cannot be acquired within the configured timeout, return a
busy/timeout error with process-level context when available.

### 7.2 Commit Publish Ordering

After appending WAL frames and the commit frame, the writer must publish new
coordination metadata in an order that never makes another process observe a
commit that is not readable/recoverable according to the configured sync mode.

Publication rules by sync mode:

| Sync mode | Publication ordering |
|---|---|
| `Full` | Write WAL frames and commit frame, perform the required WAL durability sync, publish sidecar WAL end/generation, then return success. |
| `Normal` | Write WAL frames and commit frame, perform the existing `sync_data` durability step, publish sidecar WAL end/generation, then return success. |
| `AsyncCommit` | Write WAL frames and commit frame, update the async flusher dirty watermark, publish sidecar WAL end/generation, then return success. Other processes may observe the commit before the physical sync, matching the documented async durability window; cross-process `sync`/durability barriers must observe the dirty watermark before returning. |
| `TestingOnlyUnsafeNoSync` | Test-only. Write WAL frames and commit frame, publish sidecar WAL end/generation, then return success with no durability guarantee. This mode must not be documented as production-safe. |

The invariant is that sidecar publication must not make visible a commit beyond
what the selected WAL sync mode already permits. Sidecar publication is a
visibility signal, not an upgrade or downgrade of the chosen durability mode.
For `AsyncCommit`, cross-process readers accept the same durability risk as
same-process readers: the commit is visible after publication but is not
guaranteed durable until the async flusher or an explicit durability barrier
syncs it. A `Full` writer in another process must not treat an
`AsyncCommit`-published LSN as durable merely because it saw the sidecar
generation; durability barriers must consult the shared dirty watermark or force
the WAL sync before reporting durable completion.

If a process crashes after durable WAL commit but before sidecar publication,
the next opener/writer/checkpointer must recover by scanning WAL and advancing
the sidecar.

If a process crashes after sidecar publication but before returning success to
the caller, recovery follows normal WAL rules: the commit is present if the WAL
commit frame is valid.

### 7.3 Explicit Transactions

Long explicit write transactions hold the cross-process writer lock for their
write lifetime. This matches the one-writer model and must be documented.

Doctor must report a long-held writer lock with duration and owner metadata when
available. Documentation should recommend bounded busy timeouts for applications
that use explicit write transactions from multiple processes.

Queued explicit transaction leases remain out of scope for v1.

## 8. Reader Registry And Snapshot Protocol

### 8.1 Reader Slot Contents

V1 uses a fixed table of 64 reader slots per database. This keeps the sidecar
format bounded and testable. If all slots are unavailable, starting a read
transaction fails with a specific reader-slot-exhaustion error that is surfaced
through Rust, C ABI, CLI, and bindings. Increasing the slot count or making it
configurable requires a sidecar-format compatibility decision.

Each active read transaction records a slot containing at least:

- slot id;
- process id;
- process start token or nonce;
- connection id;
- reader id/generation;
- snapshot LSN;
- checkpoint generation observed at start;
- start timestamp;
- last heartbeat/update timestamp;
- flags: active, initializing, stale, read-only, owner-verified.

No sensitive SQL text, user data, TDE key material, or indexed terms may be
stored in the sidecar.

Owner liveness proof must avoid PID reuse. Phase 1 must define platform-specific
tokens before implementation:

- Linux: PID plus `/proc/<pid>/stat` start time and boot id when available;
- macOS: PID plus process start time from the supported system API;
- Windows: process id plus process creation time or an owned process handle where
  available.

If a platform cannot provide a reliable owner token, stale cleanup on that
platform must remain conservative and treat uncertain slots as active.

### 8.2 Begin Read Protocol

The begin-read protocol must prevent this race:

1. Reader captures old WAL LSN.
2. Checkpointer sees no active reader.
3. Checkpointer truncates frames needed by reader.
4. Reader registers too late.

Safe v1 protocol:

1. Allocate and lock a reader slot.
2. Mark slot initializing with a conservative retention value.
3. Refresh local WAL state and capture snapshot LSN.
4. Publish snapshot LSN into the slot.
5. Mark slot active.
6. Recheck checkpoint generation/WAL generation; retry if a concurrent
   checkpoint invalidated the captured view.

Checkpointers must treat initializing slots as retention blockers.

The retry loop is bounded. V1 permits at most 8 begin-read retries with
exponential backoff starting at 100 microseconds and capped at 5 milliseconds.
After that, the reader waits for a stable generation by polling the sidecar
generation with the same capped backoff, subject to the configured busy timeout.
If the timeout expires, begin-read returns a busy/timeout error rather than
spinning indefinitely. The reader must not fall back to an unregistered or stale
snapshot.

### 8.3 End Read Protocol

On read transaction end:

1. While still holding the reader slot lock, write a complete inactive slot
   record with a new generation and valid checksum, or clear the slot with an
   atomic record update as defined by the sidecar format.
2. Release the reader slot lock.
3. Optionally increment diagnostic counters.

If another process observes a partially cleared or checksum-invalid slot, it
must treat the slot as active/retention-blocking until repair or stale-slot
proof says otherwise.

Process exit should release OS locks. Slot records may remain and are cleaned by
stale-slot detection.

### 8.4 Heartbeats

Long-running readers should update heartbeat fields at bounded intervals. The
heartbeat is diagnostic and helps stale-slot cleanup. It is not the only proof of
liveness; the reader slot lock is the primary proof when the platform supports
the required lock behavior.

## 9. Checkpoint And WAL Retention

Checkpointing across processes must follow ADR 0019.

Before truncating WAL frames, a checkpointer must compute:

```text
safe_truncate_lsn = min(
  active cross-process reader snapshot LSNs,
  in-process reader snapshot LSNs,
  retained branch snapshot LSN,
  sync/shape retention blockers,
  any other durable retention source
)
```

Rules:

- Checkpoint copyback may copy committed pages through the checkpoint target LSN.
- WAL truncation must not remove frames needed by any retention source.
- The background checkpoint worker participates like any other checkpointer: it
  must acquire the cross-process writer/checkpoint lock before copyback or
  truncation. If it cannot use the coordination path safely, it must be disabled
  while cross-process coordination is active.
- If reader slots cannot be read safely, the checkpoint must skip WAL truncation.
  It may still perform safe copyback, but it must not guess a truncate point.
- If stale slots are found, cleanup must prove staleness before ignoring them.
- A process performing checkpoint must publish checkpoint generation changes so
  other processes can refresh pager/header state.

## 10. WAL Index Refresh And Local Cache Invalidation

Every process keeps a local WAL index. Another process may append, truncate, or
recover WAL state.

A process must refresh before:

- starting a read transaction;
- acquiring a write transaction;
- checkpointing;
- answering metadata/Doctor queries that report WAL state;
- reading a page when local generations show a concurrent checkpoint occurred.

Refresh flow:

1. Read coordination header.
2. Compare published WAL generation/end LSN/checkpoint generation with local
   observed values.
3. If checkpoint generation, WAL generation, WAL file length, or truncation
   markers show that local WAL offsets may no longer exist, reload the database
   header/page count and rebuild or repair the WAL index from the remaining WAL.
4. If the WAL only advanced by append, incrementally scan from the local WAL end
   and update the local index.
5. If sidecar metadata is stale or behind the WAL, recover by scanning WAL under
   the coordinator init or writer lock.

Refresh must not trust sidecar metadata without validating WAL frame checksums
and commit markers.

## 11. Stale Owner And Crash Recovery

### 11.1 Writer Crash

If a writer process crashes while holding the writer lock:

- OS locks should release automatically.
- The next writer/open/checkpoint recovers WAL exactly as startup recovery does.
- Torn or incomplete frames are ignored according to existing WAL recovery rules.
- Coordination sidecar state is advanced or repaired after WAL recovery.

### 11.2 Reader Crash

If a reader process crashes:

- Its reader slot lock should release automatically.
- The slot record may remain.
- A checkpointer or Doctor may mark the slot stale only after proving the slot
  lock is free and the owner token is no longer live or no longer owns the lock.
- Stale cleanup must be idempotent.
- If lock liveness cannot be proven with certainty, DecentDB must treat the slot
  as active and retention-blocking. False retention is acceptable; false stale
  cleanup is not.

### 11.3 Hung But Live Process

A live process with a long read transaction is not stale. It may block WAL
truncation. Doctor should report it as a retention blocker, including age and
process metadata, but DecentDB must not auto-clear it solely because it is old.

### 11.4 Sidecar Corruption

If the sidecar is missing, truncated, or checksum-invalid:

- Acquire coordinator init lock.
- Rebuild sidecar metadata from database header and WAL.
- Preserve active reader locks if possible; if impossible, fail safely instead
  of truncating WAL.
- Emit Doctor-visible diagnostics.

## 12. Security And TDE Interaction

The coordination sidecar must not contain plaintext user data.

Allowed metadata:

- process ids;
- connection ids;
- LSNs;
- file lengths;
- timestamps/ages;
- counters;
- non-secret database identity values and fingerprints that do not reveal key
  material.

TDE rules:

- Opening an encrypted database in another process requires the same key
  material as normal.
- A process that cannot decrypt the database must not join coordination as a
  reader or writer.
- The sidecar must contain enough unencrypted, non-secret identity metadata for a
  process to detect that the sidecar belongs to a different database and return a
  clear mismatch/decryption error. ADR 0180 defines the required 128-bit
  non-secret database identity in the database header and the sidecar fingerprint
  derived from it.
- The sidecar must not include TDE keys, key hashes suitable for offline attack,
  SQL text, table names, row values, indexed terms, or audit context values.
- Doctor output must not leak protected data while explaining process blockers.

## 13. Branch, Sync, FTS, And Derived State

Cross-process coordination is below these features:

- Branch operations use the same writer/checkpoint locks.
- Branch snapshot retention contributes to WAL retention.
- Sync changeset apply/create uses normal read/write coordination.
- Sync shape retention contributes to checkpoint truncation safety.
- FTS/spatial/trigram indexes are maintained by the normal write path and do not
  need per-binding coordination.
- `ALTER INDEX ... REBUILD`, branch restore, import, backup replacement, and
  database copy operations require appropriate exclusive coordination.

## 14. CLI And Tooling Contract

CLI commands must work while an application process has the database open when
the command's operation is otherwise safe.

Required CLI behavior:

- `decentdb exec` waits or times out on cross-process writer lock for writes.
- Read-only `decentdb exec --sql ...` statements start registered
  cross-process readers.
- `decentdb checkpoint` respects cross-process reader retention and skips
  truncation when reader slots cannot be read safely.
- `decentdb doctor` reports writer owner, checkpoint owner, active readers,
  stale slots, WAL growth due to retention, and unsupported filesystem findings.
- Import/restore/database replacement commands require exclusive coordination
  and must fail clearly if other processes are attached.

CLI output should include stable machine-readable JSON fields for process
coordination diagnostics.

## 15. SQL Diagnostics

Add read-only `sys.*` views:

### 15.1 `sys.process_coordination`

One row per database handle:

| Column | Type | Meaning |
|---|---|---|
| `mode` | TEXT | `auto`, `required`, or `single_process_unsafe`. |
| `enabled` | BOOL | Whether process coordination is active for this handle. |
| `supported` | BOOL | Whether the VFS supports process coordination. |
| `coord_path` | TEXT | Coordination sidecar path. |
| `coord_version` | INT64 | Sidecar format version. |
| `coordinator_generation` | INT64 | Current coordination generation. |
| `wal_end_lsn` | INT64 | Latest published WAL end LSN. |
| `checkpoint_generation` | INT64 | Latest published checkpoint generation. |
| `last_refresh_lsn` | INT64 | This handle's observed WAL end LSN. |
| `last_refresh_age_ms` | INT64 | Age of this handle's last coordination refresh. |

### 15.2 `sys.process_readers`

One row per active or stale reader slot visible to the current process:

| Column | Type | Meaning |
|---|---|---|
| `slot_id` | INT64 | Reader slot id. |
| `pid` | INT64 | Owner process id when available. |
| `connection_id` | TEXT | Non-secret connection identifier. |
| `snapshot_lsn` | INT64 | Reader snapshot LSN. |
| `age_ms` | INT64 | Reader age. |
| `heartbeat_age_ms` | INT64 | Heartbeat age. |
| `state` | TEXT | `initializing`, `active`, `stale`, or `clearing`. |
| `retention_blocking` | BOOL | Whether the slot currently blocks truncation. |

### 15.3 `sys.process_lock_metrics`

Counters and current state:

| Column | Type | Meaning |
|---|---|---|
| `writer_lock_waits` | INT64 | Number of writer lock waits by this process. |
| `writer_lock_timeouts` | INT64 | Writer lock timeout count. |
| `current_writer_pid` | INT64 | Current writer-lock owner process id when known, otherwise NULL. |
| `current_writer_lock_age_ms` | INT64 | Age of the current writer-lock hold when known, otherwise NULL. |
| `current_checkpoint_pid` | INT64 | Current checkpoint owner process id when known, otherwise NULL. |
| `current_checkpoint_lock_age_ms` | INT64 | Age of the current checkpoint lock hold when known, otherwise NULL. |
| `checkpoint_lock_waits` | INT64 | Checkpoint lock waits. |
| `reader_slots_allocated` | INT64 | Reader slots allocated. |
| `stale_slots_cleaned` | INT64 | Stale slots cleaned by this process. |
| `wal_refreshes` | INT64 | WAL refresh operations. |
| `wal_refresh_failures` | INT64 | WAL refresh failures. |

Exact column names may be refined during implementation, but the final set must
be documented and regression-tested before release.

V1 metrics are process-local snapshots. The coordination sidecar stores only
metadata required for ownership, liveness, generation, retention, and safe
recovery. Durable aggregate metric counters are out of scope for v1 because they
would add sidecar write contention without improving correctness.

## 16. Rust API And C ABI Contract

### 16.1 Rust

Add:

- `DbConfig::process_coordination`
- a typed `ProcessCoordinationMode`
- internal VFS lock capability APIs
- structured process coordination metrics

Public Rust errors must distinguish:

- unsupported coordination mode/VFS;
- writer/checkpoint busy;
- lock timeout;
- coordination sidecar corrupt/unrecoverable;
- reader slot exhaustion.

Lock wait errors preserve the current distinction:

- immediate or no-wait lock/resource unavailability maps to `BUSY`;
- elapsed configured wait time maps to `TIMEOUT`;
- unsupported VFS/filesystem coordination maps to an unsupported/configuration
  error, not `BUSY`;
- reader-slot exhaustion must use a stable distinct error if available, or map to
  `BUSY` with structured context until a dedicated C ABI status is added.

### 16.2 C ABI

Use existing open-with-options functions for:

```text
process_coordination=auto
process_coordination=required
process_coordination=single_process_unsafe
busy_timeout_ms=5000
```

No dedicated per-binding process API is required for v1 if `sys.*` views and
existing SQL execution expose diagnostics. Add C ABI functions only if SQL
diagnostics cannot cover a required binding use case.

If new C ABI functions or status codes are added, bump the C ABI version and
update all binding ABI expectations.

## 17. Binding Requirements

Bindings must not implement their own file locks or WAL coordination. They use
the C ABI and engine behavior.

Required binding work:

| Binding | Required additions |
|---|---|
| Python | Open-option docs; multiprocessing smoke test with writer process, reader process, and checkpoint process. |
| Node.js | Docs for Electron/Tauri-style helper process usage; child-process smoke test. |
| Dart | Open-option docs; isolate/process guidance for desktop Flutter; smoke test where toolchain supports process spawning. |
| .NET | Connection-string option mapping if not already generic; two-process ADO.NET smoke test; Dapper docs note. |
| Go | DSN option mapping if needed; `exec.Command` two-process smoke test. |
| Java/JDBC | JDBC property mapping if needed; separate-JVM smoke test. |
| Web/WASM | No native process-lock claim. Docs cross-link browser owner routing as distinct. |

Bindings should add examples showing:

- app process writes while CLI reads;
- background sync worker applies changesets while UI reads;
- checkpoint/doctor command reports a long reader instead of corrupting data.

## 18. Documentation Requirements

Update:

- `docs/user-guide/write-concurrency.md`
- `docs/user-guide/transactions.md`
- `docs/architecture/wal.md`
- `docs/api/configuration.md`
- `docs/api/cli-reference.md`
- binding READMEs and package docs
- troubleshooting/Doctor docs
- `docs/about/changelog.md` on implementation

Docs must clearly state:

- one writer/many readers remains the model;
- local native filesystems are supported;
- network/cloud-synced filesystems are unsupported unless explicitly listed;
- coordinated read-only processes need sidecar write permission in v1;
- v1 has a fixed 64-reader-slot limit across all processes and returns a clear
  exhaustion error when no slot is available;
- long readers can block WAL truncation;
- how to diagnose process-level blockers;
- how busy timeouts behave.

## 19. Testing Plan

### 19.1 Unit Tests

- VFS byte-range lock acquire/release/try-timeout behavior.
- Coordination sidecar header encode/decode/checksum.
- Reader slot allocation, heartbeat, cleanup, exhaustion.
- Process id/start-token serialization.
- WAL generation comparison and refresh decision logic.
- Unsupported VFS mode errors.

### 19.2 Single-Process Integration Tests

Even before spawning child processes, simulate multiple process handles by using
separate coordination participants:

- writer lock prevents concurrent writer;
- checkpoint sees reader slots;
- stale slot cleanup is idempotent;
- sidecar rebuild from WAL/database header.
- low-level unit tests may use `single_process_unsafe` when process coordination
  is unrelated to the test target, but storage/concurrency integration tests must
  exercise the default `auto` path. CI should keep a focused process-coordination
  suite rather than adding sidecar overhead to every narrow unit test.

### 19.3 Multi-Process Integration Tests

Add Rust test helper binaries that can:

- open database;
- hold reader snapshot;
- perform write;
- checkpoint;
- crash at named failpoints;
- report JSON status to parent test.

Required scenarios:

1. Process A writes; Process B opens/reads and sees commit without app restart.
2. Process A holds reader snapshot; Process B writes; Process C checkpoints;
   WAL is not truncated past A.
3. Process A holds reader snapshot and exits normally; Process B checkpoints and
   truncates when safe.
4. Process A is force-killed while reader slot is active (`SIGKILL` on Unix,
   `TerminateProcess` on Windows); Process B detects stale slot and later
   truncates safely.
5. Process A crashes mid-write before commit; Process B recovers and ignores
   torn frames.
6. Process A crashes after durable commit but before sidecar publish; Process B
   scans WAL and publishes recovered state.
7. Process A checkpoints; Process B refreshes pager/header state before next
   read/write.
8. Two writer processes contend; one waits or times out according to configured
   timeout.
9. CLI `exec`, read-only `exec --sql`, `checkpoint`, and `doctor` run while
   another process has the DB open.
10. Database replacement/import fails while another process is attached.
11. Two processes simultaneously first-open a database with no `.coord`; one
    initializes the sidecar while the other waits and then observes the same
    valid sidecar identity/generation.
12. A stale `.coord` from a moved/replaced database is detected by identity
    mismatch and repaired or rejected safely.
13. Two processes attempt first open/create with incompatible database creation
    options such as different page sizes. The database header is authoritative:
    the losing opener either adopts the existing header-compatible settings or
    fails with a clear configuration/format error.

### 19.4 Crash And Fault Injection

Add failpoints:

- after writer lock acquire;
- after WAL page frames write;
- after commit frame write before sync;
- after sync before sidecar publish;
- during sidecar header write;
- during checkpoint copyback before sidecar publish;
- during checkpoint after sidecar publish before WAL truncate;
- during reader-slot clear;
- during stale-slot cleanup.

Crash tests must verify no committed data is lost and no uncommitted data
appears.

### 19.5 Binding Smoke Tests

At minimum:

- Python multiprocessing;
- .NET separate process or helper executable;
- Node child process;
- Go child process;
- Java separate JVM;
- Dart process where the toolchain is available.

Binding checks may skip gracefully when toolchains are unavailable, following
existing repo conventions.

### 19.6 Platform Matrix

Release-blocking once claimed:

- Linux local filesystem;
- macOS local filesystem;
- Windows NTFS.

Candidate/nonblocking until promoted:

- WSL interop paths;
- APFS external drive;
- case-insensitive/case-sensitive path variants.

Explicitly unsupported in v1:

- NFS;
- SMB;
- cloud-synced folders such as Dropbox/iCloud/OneDrive;
- FUSE filesystems unless a later ADR approves a tested configuration.

## 20. Benchmark And Regression Plan

Add benchmark profiles for:

- same-process direct write baseline with coordination enabled;
- cross-process writer contention timeout overhead;
- cross-process read start/finish overhead;
- checkpoint with many reader slots;
- WAL refresh after another process appends many frames;
- CLI read/write latency while app process is open.

Guardrails:

- Primary guardrail: coordination enabled should add at most 2% p95 regression
  on the single-process read transaction start benchmark when no external
  process has changed WAL generation. This starts report-only until baselines are
  stable, then becomes release-blocking for claimed platforms.
- Secondary reported metric: p50 read transaction start overhead should stay at
  or below 5 microseconds on the benchmark host, but this does not replace the
  p95 guardrail.
- Writer lock acquisition on uncontended local filesystem should be measured and
  tracked.
- Refresh cost should be proportional to new WAL frames, not full database size.

## 21. Phasing

### Phase 0: Spec, ADRs, Harness

- Land this spec and governing ADRs.
- Land the database identity/header ADR and migration-parser plan required by
  ADR 0180.
- Add process test helper binary design.
- Add no-op/skipped test scaffolding where useful.

### Phase 1: VFS Locking And Coordination Sidecar

- Add VFS lock capability abstraction.
- Implement native local file byte-range locks for Linux/macOS/Windows.
- Define sidecar format and encode/decode tests.
- Add sidecar creation/rebuild under init lock.
- Add unsupported VFS behavior.

### Phase 2: Cross-Process Writer And Checkpoint Locks

- Acquire writer lock around write transactions.
- Integrate queued writes by holding the cross-process writer lock for one
  bounded local queue drain batch, then releasing it before another drain pass.
- Publish WAL end/generation after commit.
- Serialize checkpoint/copyback/truncation.
- Add timeout/error behavior.

Phase 2 must not be released without Phase 3. If Phase 2 is merged behind an
internal flag before reader slots are implemented, checkpoint truncation must be
disabled whenever cross-process coordination is active, because writer
serialization alone does not protect readers in other processes.

### Phase 3: Reader Registry And Retention

- Add reader slot lifecycle.
- Integrate cross-process readers into safe truncate calculation.
- Add stale slot detection and cleanup.
- Add long-reader diagnostics.

### Phase 4: WAL Refresh And Recovery

- Refresh local WAL index before read/write/checkpoint.
- Detect external checkpoint generation changes.
- Recover sidecar after crash windows.
- Add failpoint coverage.

### Phase 5: CLI, Sys Views, Bindings

- Add `sys.process_*` views.
- Add `decentdb doctor` process coordination findings.
- Update CLI checkpoint/import/restore behavior.
- Add binding docs and smoke tests.

### Phase 6: Platform Hardening And Release Guardrails

- Promote Linux/macOS/Windows tests to release-blocking.
- Add benchmarks.
- Update user docs and changelog.
- Run full pre-commit and binding validation.

## 22. Acceptance Criteria

The feature is complete only when:

- Multiple native processes can safely read/write/checkpoint one local database.
- Cross-process writes remain serialized and durable.
- Cross-process readers preserve snapshot isolation.
- Checkpoint never truncates WAL required by another process's reader.
- WAL index refresh handles external append, checkpoint, truncation, and recovery.
- Stale reader slots from crashed processes are cleaned safely.
- CLI coexistence scenarios are documented and tested.
- Binding smoke tests prove behavior through maintained bindings.
- Unsupported filesystems/modes fail clearly.
- `sys.*` and Doctor diagnostics expose process blockers.
- Unit, integration, crash, and binding tests pass on claimed platforms.
- Documentation and `docs/about/changelog.md` are updated.

## 23. Open Questions For Implementation

No open questions remain for the accepted v1 design. New implementation
discoveries that affect file format, locking semantics, C ABI status mapping, or
platform support must be captured in a follow-up ADR before code lands.
