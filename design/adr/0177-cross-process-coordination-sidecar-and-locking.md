# Cross-Process Coordination Sidecar And Locking
**Date:** 2026-05-27
**Status:** Accepted

### Decision

DecentDB will coordinate native OS processes that open the same local on-disk
database through a DecentDB-owned coordination sidecar and portable byte-range
file locks.

The sidecar path is:

```text
<database path>.coord
```

The sidecar is separate from the database file and WAL file. It contains
coordination metadata only, not user data. It has its own magic, version,
database identity fingerprint, generation counters, WAL/checkpoint publication
metadata, owner records, reader slot records, and checksums for torn-write
detection.

V1 uses byte-range locks on the coordination sidecar for:

- coordinator initialization/repair;
- writer ownership;
- checkpoint ownership;
- reader slot ownership/liveness;
- metadata publication if not already protected by the writer/checkpoint lock.

The VFS layer must grow a process-locking capability abstraction. Native local
filesystem VFS implementations for Linux, macOS, and Windows must implement that
capability before DecentDB claims cross-process coordination on those platforms.
VFS implementations that cannot provide the required lock semantics must fail
safe under `process_coordination=auto|required`.

DecentDB will not use mmap or shared-memory pages for v1 coordination. Sidecar
state is read and written with positional I/O, keeping the design aligned with
ADR 0119.

The coordination sidecar is rebuildable from the database header and WAL under
the coordinator initialization lock. It is not authoritative for committed user
data. The WAL and database remain the durable source of truth.

### Rationale

Cross-process safety needs a coordination primitive visible outside one Rust
process. The existing same-process shared WAL registry cannot help another OS
process because its locks, WAL index, reader registry, and queue state are
process-local memory.

Byte-range file locks provide these properties:

- OS-mediated mutual exclusion between unrelated processes;
- automatic release when a process exits or crashes;
- the ability to use separate lock ranges for writer/checkpoint/reader-slot
  ownership;
- no daemon or server process requirement;
- no unsafe shared memory requirements.

A DecentDB sidecar allows the engine to store structured diagnostics and
generation counters next to the locks. Relying on locks alone would serialize
writers but would not tell readers/checkpointers which WAL generation, snapshot
LSN, or checkpoint generation other processes are using.

Keeping the sidecar rebuildable avoids making coordination metadata another
durable source of user data. If the sidecar is missing or corrupt, DecentDB can
repair it by holding the initialization lock and scanning the database/WAL.

### Alternatives Considered

1. **Keep coordination in process memory only.** Rejected. It cannot protect
   independent OS processes.
2. **Whole-file database lock.** Rejected. It would block readers unnecessarily
   and would not provide reader retention metadata for safe checkpoints.
3. **A server/daemon lock manager.** Rejected. DecentDB is an embedded engine
   and should not require a background service for local process safety.
4. **mmap shared-memory sidecar.** Rejected for v1. It introduces unsafe memory
   and truncation hazards that ADR 0119 intentionally avoids.
5. **Use the WAL file itself for lock ranges.** Rejected. WAL truncation,
   replacement, and recovery make it a poor stable lock namespace.
6. **Depend on platform-specific named mutexes.** Rejected for the primary
   design. They complicate database-file portability and do not naturally
   travel with the database path.
7. **Best-effort advisory docs only.** Rejected. The feature must be safe by
   default, not a user-managed convention.

### Trade-offs

- Byte-range file locking has platform differences. The VFS abstraction must
  hide differences only where correctness is preserved.
- Some filesystems may appear local but not provide correct lock behavior.
  V1 must document unsupported filesystems and fail safe when unsupported
  behavior is detectable.
- Read-only database access may still require write access to the sidecar for
  reader registration.
- The sidecar adds another file that backup/support tooling must understand.
- Rebuilding a corrupt sidecar requires exclusive initialization coordination and
  may block opens briefly.
- The sidecar format needs compatibility tests even though it is not database
  user data.

### Consequences

- `DbConfig` and C ABI open options need a process coordination mode.
- The native VFS needs lock-range APIs and platform implementations.
- Import/restore/database replacement commands must coordinate through the
  sidecar before replacing files.
- Doctor and `sys.*` views can report meaningful process-level blockers.
- No database format version bump is required unless implementation changes the
  database header or WAL frame format. The sidecar has an independent format
  version.

### References

- `design/WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md`
- `design/adr/0117-shared-wal-registry.md`
- `design/adr/0119-rust-vfs-pread-pwrite.md`
- `design/adr/0162-engine-owned-write-queue-strict-group-commit.md`

