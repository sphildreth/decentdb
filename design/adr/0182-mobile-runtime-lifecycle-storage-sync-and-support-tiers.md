# Mobile Runtime Lifecycle, Storage, Sync, And Support Tiers
**Date:** 2026-05-27
**Status:** Accepted

### Decision

DecentDB mobile support will publish an explicit runtime contract for app-owned
database handles, app-private storage, background execution, relay sync, and
support tiers.

Normal mobile apps use a single app process by default. Cross-process
coordination is documented as off for the default mobile profile unless an app
deliberately shares a database across processes or extensions. App extensions,
widgets, foreground services, content providers, and app-group sharing are
deferred from Tier 1 until platform-specific tests prove file locking,
coordination sidecars, WAL retention, stale reader cleanup, entitlements, and
kill recovery.

The default live database location is app-private storage:

- iOS: Application Support for production database files.
- Android: app-specific internal files for production database files.

The mobile package must expose helpers for backup-eligible and no-backup
locations. The default app-private location is backup-eligible only when the app
has an explicit restore and identity plan. A named no-backup helper is required
for device-local replicas and sync-derived caches. Live databases should not be
placed in cloud-synced folders, external/shared storage, temporary directories,
or user-visible Documents by default.

The database set includes the main database file, WAL sidecar, sync journal,
coordination sidecar when enabled, and future backup/PITR manifests. Move,
delete, restore, and support workflows must not silently operate on only the
main file unless they are using a DecentDB API that creates a consistent
artifact.

Mobile lifecycle guidance is:

- open the database during an active app or feature lifetime;
- close handles explicitly when ownership ends;
- treat Dart finalizers as cleanup fallback, not lifecycle policy;
- finish, cancel, or reject in-flight statements/transactions before close;
- on background/inactive, stop admitting new UI writes, finish or cancel active
  app work, optionally checkpoint, and close if no background task owns the
  database;
- rely on normal WAL recovery for process kill and crash recovery;
- explain that checkpointing improves startup/storage behavior but is not
  required for committed durability when WAL sync mode is durable.

Mobile background sync is opportunistic. iOS and Android background schedulers
must be described as best-effort. Sync examples must use apply-before-ack:
apply the relay changeset in a durable local transaction, then ack the relay. If
the task is killed before ack, the relay should redeliver from the previous
durable checkpoint.

Support tiers are based on tests, not build possibility:

- Tier 1 requires release-blocking simulator/emulator coverage plus a documented
  real-device lane or device-lab process.
- Tier 2 may use release-blocking simulator/emulator coverage without real
  device automation.
- Candidate covers buildable/example-only paths without release-blocking
  lifecycle coverage.
- Unsupported covers unsafe, untested, or intentionally excluded environments.

Android emulator and iOS simulator lanes are release-blocking for the first
implementation. A platform must not be labeled Tier 1 until real-device
validation exists.

### Rationale

Mobile correctness failures usually happen around lifecycle and storage rather
than SQL execution. The engine already has WAL recovery, process coordination,
write queue semantics, sync changesets, and TDE. The mobile product risk is
whether applications use those surfaces with safe paths, correct sidecar
handling, and honest background execution expectations.

Mobile OS schedulers do not provide continuous execution guarantees. DecentDB's
sync contract therefore must be local durability first: once a client acks a
relay message, the local apply must already be committed. This is consistent
with the production relay and browser apply-before-ack pattern.

Support tiers prevent accidental overclaiming. Simulator and emulator coverage
is useful and should block regressions, but real mobile storage and process
behavior still needs device validation before production claims are made.

### Alternatives Considered

1. **Claim support when the library builds for mobile targets.** Rejected.
   Build success does not prove lifecycle, storage, or sync correctness.
2. **Keep databases in user-visible Documents by default.** Rejected. It
   encourages live-file sharing/cloud-sync workflows that are unsafe for a WAL
   database unless explicitly designed.
3. **Default to no-backup directories for every app.** Rejected. Some apps need
   local state restored. The SDK should expose both choices and force an
   identity/restore decision.
4. **Treat background sync as guaranteed.** Rejected. The OS controls task
   scheduling and termination.
5. **Enable app-extension/widget sharing in Tier 1.** Rejected for v1. It
   broadens the locking, entitlement, and kill-recovery matrix before normal
   mobile app support is stable.
6. **Make cross-process coordination required for every mobile open.** Rejected.
   Normal mobile apps are single app-process deployments. Shared-access profiles
   can opt in after validation.

### Trade-offs

- Conservative support labels may make early mobile support look narrower, but
  they protect durability claims.
- Requiring real-device validation for Tier 1 adds release overhead.
- Exposing both backup and no-backup helpers creates more documentation, but
  avoids a one-size-fits-all identity mistake.
- Deferring app extensions/widgets may delay some product use cases, but keeps
  the first mobile runtime contract tractable.

### Consequences

- Add mobile docs for storage locations, sidecars, backup/restore, lifecycle,
  and background sync.
- Add Flutter integration tests for foreground open/query/close,
  background/foreground callbacks, process kill/reopen, WAL recovery, encrypted
  reopen, and relay apply-before-ack.
- Add diagnostics that report platform, artifact ABI, DecentDB ABI, path class,
  open-option summary, support tier, and recent DecentDB errors.
- Keep app-extension/widget support outside Tier 1 until a follow-up ADR/spec
  and tests accept that profile.

### References

- `design/WIN_MOBILE_PRODUCTION_RUNTIME_SDK_HARDENING_SPEC.md`
- `design/adr/0166-production-sync-relay-boundary-and-identity.md`
- `design/adr/0167-public-changeset-api.md`
- `design/adr/0168-sync-shape-streaming-subscriptions.md`
- `design/adr/0177-cross-process-coordination-sidecar-and-locking.md`
- `design/adr/0178-cross-process-reader-retention-and-wal-refresh.md`
- `design/adr/0179-cross-process-public-contract-bindings-and-diagnostics.md`
- `docs/user-guide/sync/relay.md`
- `docs/user-guide/write-concurrency.md`

