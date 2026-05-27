# Cross-Process Public Contract, Bindings, And Diagnostics
**Date:** 2026-05-27
**Status:** Accepted

### Decision

Cross-process WAL coordination will be exposed as an engine-owned runtime
contract, not as separate binding-specific lock implementations.

The public configuration surface is:

```text
process_coordination=auto|required|single_process_unsafe
```

This option is accepted by Rust configuration and C ABI open-with-options
strings. Bindings that already pass through C ABI open options should document
the option. Bindings with connection-string or DSN parsers should map the option
to the same native open option instead of implementing their own locks.

Native local on-disk databases use `auto` by default. Unsupported VFS or
filesystem configurations must fail safe unless the caller explicitly uses
`single_process_unsafe`.

Coordinated read-only opens require write access to the coordination sidecar in
v1. If the sidecar cannot be created or updated, open fails with a clear error.
`single_process_unsafe` is not a coordinated read-only mode; it is an explicit
safety opt-out for controlled single-process deployments.

Diagnostics are exposed primarily through SQL and CLI:

- `sys.process_coordination`
- `sys.process_readers`
- `sys.process_lock_metrics`
- `decentdb doctor` process-coordination findings
- JSON output for CLI/Doctor automation

V1 should avoid new per-binding APIs. If implementation requires new C ABI
functions or status codes, the C ABI version must be bumped and every maintained
binding must update its ABI expectation and smoke tests.

Cross-process writer/checkpoint waits use existing busy/timeout semantics where
possible. Error messages should identify process coordination blockers when the
owner metadata is known.

Doctor must include findings for long-held writer locks, active reader retention
blockers, stale reader slots, sidecar/database identity mismatches, unsupported
coordination filesystems, and WAL growth caused by cross-process retention.
SQL diagnostics must include current writer/checkpoint holder metadata when
known, including process id and lock age, in addition to cumulative wait and
timeout counters.

Bindings must add smoke tests or examples that prove independent host processes
can safely use the same database file through the public binding API.

### Rationale

The Rust engine is authoritative for database file safety. If Python, .NET,
Node, Dart, Java, or Go each implemented file locking independently, their
behavior would drift and correctness would depend on package-specific details.
The C ABI is the shared boundary, so process coordination belongs below it.

SQL diagnostics fit DecentDB's existing operational direction from ADR 0163.
They are usable from every binding, from the CLI, and from support tooling
without adding new per-language APIs.

`single_process_unsafe` is intentionally explicit. Some tests and embedded
deployments may need the old no-sidecar behavior, but the option name must make
the risk visible.

### Alternatives Considered

1. **Per-binding lock files.** Rejected. This would fragment correctness and
   break interoperability between bindings.
2. **Require applications to appoint a single owner process manually.** Rejected.
   Useful as an application pattern, but it does not solve CLI/tool coexistence
   or SQLite-like process-safe file access.
3. **Expose only Rust APIs and leave C ABI/bindings unchanged.** Rejected.
   Cross-process coordination is valuable precisely because desktop and
   local-first apps commonly use bindings.
4. **Add a large binding-specific process API surface in v1.** Rejected. Normal
   open options and SQL diagnostics should be enough unless implementation
   proves otherwise.
5. **Silently fall back to unsafe single-process behavior.** Rejected. Safety
   must be the default.

### Trade-offs

- Some existing deployments on unusual filesystems may see open failures until
  they opt into `single_process_unsafe` or move the database to a supported
  local filesystem.
- SQL diagnostics require stable schema contracts and tests.
- Binding documentation needs to explain sidecar permissions and unsupported
  filesystems clearly.
- If new C ABI status codes are needed, all bindings need coordinated updates.
- The default `auto` path should be exercised by process-coordination
  integration tests. Narrow unit tests may use `single_process_unsafe` when the
  coordination sidecar is unrelated to the behavior under test.

### Consequences

- Update `docs/api/configuration.md`, `docs/user-guide/write-concurrency.md`,
  `docs/architecture/wal.md`, CLI docs, and binding docs.
- Add binding smoke tests that spawn separate processes where toolchains are
  available.
- Add release validation for CLI/app coexistence.
- Add Doctor findings for writer owner, active reader blockers, stale slots,
  unsupported coordination, and WAL growth due to cross-process retention.
- Update `docs/about/changelog.md` when implementation lands.

### References

- `design/WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md`
- `design/adr/0162-engine-owned-write-queue-strict-group-commit.md`
- `design/adr/0163-operational-sys-metrics.md`
- `docs/user-guide/write-concurrency.md`
- `include/decentdb.h`
