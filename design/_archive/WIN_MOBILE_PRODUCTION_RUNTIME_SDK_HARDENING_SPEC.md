# Mobile Production Runtime And SDK Hardening

**Date:** 2026-05-27
**Status:** Implemented
**Future Version:** vNext
**Roadmap:** [`FUTURE_WINS.md`](../FUTURE_WINS.md)
**Document Type:** Implementation SPEC
**Audience:** Dart/Flutter binding maintainers, C ABI maintainers, VFS and WAL
maintainers, sync maintainers, security maintainers, release/package
maintainers, documentation authors, benchmark maintainers, coding agents

**Governing ADRs and delivered inputs:**

- [ADR 0162: Engine-Owned Write Queue Strict Group Commit](../adr/0162-engine-owned-write-queue-strict-group-commit.md)
- [ADR 0166: Production Sync Relay Boundary And Identity](../adr/0166-production-sync-relay-boundary-and-identity.md)
- [ADR 0167: Public Changeset API](../adr/0167-public-changeset-api.md)
- [ADR 0168: Sync Shape Streaming Subscriptions](../adr/0168-sync-shape-streaming-subscriptions.md)
- [ADR 0174: Local Data Security TDE, Policies, Masking, and Audit Context](../adr/0174-local-data-security-tde-policies-masking-audit-context.md)
- [ADR 0177: Cross-Process Coordination Sidecar And Locking](../adr/0177-cross-process-coordination-sidecar-and-locking.md)
- [ADR 0178: Cross-Process Reader Retention And WAL Refresh](../adr/0178-cross-process-reader-retention-and-wal-refresh.md)
- [ADR 0179: Cross-Process Public Contract, Bindings, And Diagnostics](../adr/0179-cross-process-public-contract-bindings-and-diagnostics.md)
- [ADR 0180: Database Identity For Coordination Sidecars](../adr/0180-database-identity-for-coordination-sidecars.md)
- [ADR 0181: Mobile Flutter Package And Native Artifact Contract](../adr/0181-mobile-flutter-package-and-native-artifact-contract.md)
- [ADR 0182: Mobile Runtime Lifecycle, Storage, Sync, And Support Tiers](../adr/0182-mobile-runtime-lifecycle-storage-sync-and-support-tiers.md)
- [ADR 0183: Mobile TDE Key Provider And Platform Keystore Boundary](../adr/0183-mobile-tde-key-provider-and-platform-keystore-boundary.md)
- [`docs/api/dart.md`](../../docs/api/dart.md)
- [`bindings/dart/README.md`](../../bindings/dart/README.md)
- [`docs/user-guide/security.md`](../../docs/user-guide/security.md)
- [`docs/user-guide/sync/relay.md`](../../docs/user-guide/sync/relay.md)
- [`docs/user-guide/write-concurrency.md`](../../docs/user-guide/write-concurrency.md)
- [`include/decentdb.h`](../../include/decentdb.h)

**ADR coverage and follow-up ADR triggers:**

- ADR 0181 settles the v1 Flutter package, native artifact, iOS/Android target,
  release workflow, and stable C ABI boundary decisions.
- ADR 0182 settles the v1 mobile app-process ownership, storage, lifecycle,
  apply-before-ack sync, and support-tier decisions.
- ADR 0183 settles the v1 TDE key-provider and platform key-store boundary.
- A follow-up ADR is still required before claiming mobile app-extension/widget
  shared database access, changing VFS locking, changing background durability
  semantics, adding new stable C ABI entry points, changing existing open-option
  meaning, or making DecentDB own platform key storage, prompts, rotation,
  unwrapping, escrow, or recovery.

**Implementation status, 2026-05-27:** Implemented as the first Flutter mobile
production-runtime baseline. The delivered work adds the separate
`decentdb_flutter` package shell, Android/iOS artifact build and install
scripts, a dedicated mobile artifact workflow, Android/iOS default Dart native
loading, typed native load/ABI mismatch errors, redacted open-option helpers,
mobile key-provider/path/database-set helpers, `AsyncDatabase` option and
owning-isolate hardening, Dart public changeset wrappers with
`applyBeforeAck`, a reference Flutter app, package tests, documentation, and
changelog updates. The support matrix remains Tier 2/Candidate until real-device
release-blocking lanes are added; no Tier 1 mobile claim is made by this spec.

---

## 1. Executive Summary

DecentDB should be a credible local-first database for mobile apps, not only a
native library that can theoretically be loaded by Flutter. Mobile teams need
clear answers to practical production questions:

- Which iOS and Android environments are supported?
- How is the native library packaged into an app?
- Where should database, WAL, sync-journal, backup, and export files live?
- What happens when the app is backgrounded, killed, upgraded, or restored from
  backup?
- How should TDE keys be stored and supplied without logging or persisting
  secrets in the database directory?
- How does relay sync apply data durably before acknowledging receipt when
  mobile background execution is best-effort?
- What tests prove the runtime survives real device and simulator lifecycle
  events?

This win makes Flutter mobile the first-class mobile path and turns the current
Dart FFI binding into a production SDK story. It should not invent a second
engine API. The Rust engine and C ABI remain authoritative; mobile code should
add packaging, path/key/lifecycle helpers, examples, diagnostics, and release
validation around the existing contract.

## 2. Product Goals

- Define explicit iOS and Android support tiers, including minimum OS/API
  versions, architectures, test requirements, and unsupported environments.
- Package DecentDB for Flutter mobile with reproducible native artifacts and a
  reference app that works on Android emulator/device and iOS simulator/device.
- Provide mobile storage guidance for app-private database files, WAL sidecars,
  sync journals, backups, exports, and no-backup/cache directories.
- Define a mobile lifecycle contract for open handles, prepared statements,
  foreground/background transitions, suspend/resume, checkpointing, app kill,
  crash recovery, and app upgrades.
- Provide TDE key-store guidance that composes with platform Keychain/Keystore
  APIs while keeping DecentDB's key contract application-owned.
- Provide mobile relay-sync examples that apply changesets locally before acking
  relay delivery and that do not overpromise background execution guarantees.
- Add simulator/device smoke tests and release guardrails before claiming a
  mobile platform as supported.
- Keep direct Swift, Kotlin, and React Native SDKs as follow-on candidates until
  Flutter mobile proves the package, lifecycle, and C ABI contract.

## 3. Non-Goals

- No claim that mobile background tasks run continuously or on a fixed schedule.
  iOS and Android background execution remains OS-controlled and best-effort.
- No hidden fallback to SQLite, browser storage, cloud storage, or in-memory
  databases for production mobile durability claims.
- No cross-process app-extension/widget database sharing in the first supported
  tier unless it is explicitly tested with mobile file locking and WAL
  retention semantics.
- No platform key storage inside the core Rust engine. TDE accepts key bytes;
  mobile helpers may retrieve them from host key stores, but the engine should
  not own OS identity, prompts, biometrics, or key rotation in this win.
- No broad Swift/Kotlin/React Native API duplication in the first slice.
- No mobile-specific SQL dialect or divergent query behavior.
- No arbitrary native extension loading on mobile.
- No server-style authentication model. The host app still decides who can open
  a database handle; DecentDB enforces local policies, masks, audit context, and
  encryption once opened.

## 4. Current Context

Delivered foundations to reuse:

- Stable C ABI in `include/decentdb.h`.
- Dart package in `bindings/dart/dart` that loads a native DecentDB library.
- Dart open/create/open-existing/memory modes, `close()`, `Finalizer` fallback,
  transactions, prepared statements, paging, checkpoint, save-as, branch
  workflow APIs, schema/tooling metadata, and typed value mappings.
- Dart async facade in `async_database.dart` for isolate-backed execution.
- Dart open options for process coordination and write queue parameters.
- C ABI open options for TDE key material, including `encryption_key`,
  `encryption_key_hex`, `tde_key`, and `tde_key_hex`. This option-string key
  transport is an accepted ADR 0174 trade-off, not a mobile-specific invention.
- TDE v1 through Rust config and C ABI open options, including encryption of the
  database, WAL, and sync journal.
- Production relay and public changeset APIs.
- Browser apply-before-ack helper, which is a useful pattern for mobile sync.
- C ABI reactive watch and change-stream entry points, which are useful
  foundations for mobile reactive UI but are not yet validated under mobile
  lifecycle/restart conditions.
- Cross-process WAL coordination on native OS platforms.

Remaining promotion gaps:

- Real-device Android and iOS lanes are still required before promoting either
  platform from Tier 2/Candidate to Tier 1.
- Dart/mobile reactive watch wrappers remain deferred until backgrounding,
  worker-isolate shutdown, close, and process-restart semantics are tested and
  documented.
- Direct Swift, Kotlin, React Native, app-extension, widget, and multiprocess
  shared-database profiles remain follow-on candidates.
- Runtime mobile benchmark latency baselines are advisory until a device or
  simulator benchmark lane writes accepted thresholds.

## 5. Definition Of Done

This win is complete only when all of these are true:

- A mobile support matrix is documented with Tier 1, Tier 2, candidate, and
  unsupported environments.
- Flutter mobile can consume DecentDB without users hand-copying native
  libraries into ad hoc project locations.
- Android and iOS native artifacts are reproducibly built, versioned, and
  release-packaged.
- A reference Flutter mobile app demonstrates create/open, encrypted open,
  migrations/schema setup, prepared statements, transactions, checkpoint,
  export/save-as, relay apply-before-ack, and lifecycle close/reopen.
- Mobile storage, backup, export, and no-backup guidance is documented for iOS
  and Android.
- TDE key-store guidance is documented for iOS Keychain and Android Keystore,
  including redaction and key-loss behavior.
- Mobile lifecycle rules are documented and tested: foreground open, background
  close/checkpoint policy, crash recovery, process kill/reopen, and upgrade
  reopen.
- Relay sync mobile examples apply locally durably before acking relay delivery.
- Simulator and device smoke tests exist for all Tier 1 claims.
- Mobile benchmark guardrails record package size, cold open, first query,
  prepared lookup loop, checkpoint/export, sync apply, and memory growth. The
  first release may use broad advisory thresholds captured from CI/device
  baselines, but supported mobile claims must fail CI on severe regressions once
  those baselines are accepted.
- The reference Flutter app is either the integration-test host or is exercised
  by the same integration scenarios, so documented examples cannot drift from
  release validation.
- Uninstalling or resetting DecentDB data from a mobile app is documented,
  including cleanup of the full v1 database set and any app-owned key/identity
  consequences.
- Docs and `docs/about/changelog.md` are updated when implementation lands.

## 6. Support Tiers

Support tiers must describe what is tested, not what might work.

| Tier | Meaning | Release claim |
|---|---|---|
| Tier 1 | Release-blocking smoke and lifecycle coverage exists on simulator/emulator and at least one real-device lane or documented device lab lane. | Supported production target. |
| Tier 2 | Automated simulator/emulator coverage exists, but real-device coverage is manual or candidate-only. | Supported with caveats. |
| Candidate | Build path exists and examples run locally, but release-blocking coverage is incomplete. | Preview only. |
| Unsupported | Known unsafe, untested, or outside DecentDB's runtime contract. | No durability/support claim. |

Candidate initial matrix:

| Platform | Initial target | Proposed tier after this win | Notes |
|---|---|---|---|
| Android Flutter, app-private internal storage | API 26+; `arm64-v8a` device; `x86_64` emulator | Tier 2 until a documented real-device lane exists; Tier 1 after that gate passes | Final API floor should match Rust/NDK and Flutter stable constraints measured during implementation. |
| iOS Flutter, app-private Application Support storage | iOS 15+; arm64 device; simulator lane | Tier 2 until a documented real-device lane exists; Tier 1 after that gate passes | Requires accepted XCFramework/static-link story plus real-device validation for Tier 1. |
| Flutter desktop | Existing Linux/macOS/Windows package path | Existing supported surface | Not part of the mobile claim except for shared Dart API regression tests. |
| Android app widgets, services, multiprocess providers sharing one DB | TBD | Candidate/Unsupported | Requires explicit cross-process mobile validation. |
| iOS app extensions sharing one DB through app groups | TBD | Candidate/Unsupported | Requires explicit file-lock, WAL-retention, and entitlement validation. |
| React Native, Swift, Kotlin direct SDKs | TBD | Candidate | Should follow Flutter proof unless product demand justifies parallel work. |
| Cloud-synced directories, external SD/shared storage, user-visible Documents by default | N/A | Unsupported | App may export copies intentionally, but production DB files should remain app-private. |

Adding `armeabi-v7a` requires a spec update or ADR, a concrete promotion reason
such as a partner requirement or measured install-base need, plus
release-blocking artifact-size, emulator/device, ABI-version, and smoke
coverage for that ABI.

## 7. Package And SDK Architecture

This section restates the implementation-facing requirements from ADR 0181.
If ADR 0181 changes, this section must be updated in the same branch.

### 7.1 Package Shape

Accepted package shape:

- Keep `bindings/dart/dart` as the pure Dart C ABI wrapper.
- Add a thin Flutter mobile package, tentatively `decentdb_flutter`, that:
  - bundles Android and iOS native artifacts;
  - exposes mobile path helpers;
  - exposes mobile lifecycle helpers;
  - exposes platform key-provider examples or adapters;
  - delegates all SQL/database work to the existing `decentdb` Dart package.

This avoids forcing Flutter dependencies into pure Dart/CLI users while keeping
the database API in one place. Adding Flutter plugin platform directories
directly to the existing `decentdb` package is no longer the v1 direction and
requires new evidence that a separate package creates material user friction.

### 7.2 Native Artifacts

Android package requirements:

- Build `libdecentdb.so` for at least `arm64-v8a` and `x86_64`.
- Omit `armeabi-v7a` in v1 unless the promotion gate in the support matrix is
  met.
- Package libraries under the Flutter/Gradle-native layout so apps do not set
  `libraryPath` manually.
- Android Dart loading should use the packaged native library name, normally
  `DynamicLibrary.open('libdecentdb.so')`, when the `.so` is under the standard
  Flutter/Gradle native library layout.
- Verify symbol exports, ABI version, and C ABI layout on each architecture.
- Document minimum Android API/NDK level and how release artifacts are produced.

iOS package requirements:

- Produce an XCFramework or equivalent Flutter-compatible package for iOS
  device and simulator.
- Use static-library-compatible Rust artifacts as the preferred XCFramework
  input. Add `staticlib` to the Rust crate build outputs if implementation
  proves it is needed for the accepted iOS link model.
- For the static/XCFramework link model, iOS Dart loading should prefer
  `DynamicLibrary.process()` or generated plugin registration. If a dynamic
  framework path is chosen, validate `DynamicLibrary.open()` explicitly.
- Validate whether the Dart FFI loading mechanism is consistent between iOS
  simulator and device builds for the chosen link model; document any
  simulator/device difference in the package README.
- Verify bitcode/symbol/signing expectations for current Xcode/Flutter stable.
- Document minimum iOS version and whether simulator/device support differs.

Shared requirements:

- Native artifacts must be built from the same DecentDB version as the Dart
  package metadata.
- ABI mismatch must fail clearly at startup through a typed Dart exception or a
  stable DecentDB mobile error wrapper around the existing ABI check. The error
  must include expected ABI, loaded ABI, artifact path or package source when
  known, and recovery guidance to align the Flutter package and native
  artifact versions.
- Release packages must include license notices and artifact checksums.
- No mobile package should download executable native code at runtime.

### 7.3 C ABI Boundary

The C ABI remains the only stable native boundary. Mobile work should prefer:

- existing `ddb_db_open*_with_options` entry points;
- existing prepared statement APIs;
- existing sync JSON/public changeset entry points;
- existing checkpoint/save-as APIs;
- existing write queue and process coordination open options.

Add C ABI only when a real mobile gap cannot be solved in Dart/plugin code. Any
new C ABI surface must follow the repository's ABI versioning and binding
update rules.

## 8. Mobile Storage Contract

This section restates the storage-facing requirements from ADR 0182. If ADR
0182 changes, this section must be updated in the same branch.

### 8.1 Database Locations

Recommended defaults:

- iOS: app-private Application Support directory for production database files.
  Use Documents only for explicit user-visible exports. Avoid tmp/cache for
  durable databases.
- Android: app-specific internal files directory for production database files.
  Use no-backup storage only when the application explicitly wants device-local
  state that should not be restored by OS backup. Avoid external/shared storage
  for live databases.

The SDK should make the safe default easy:

```dart
final db = await DecentDbMobile.openAppDatabase('app.ddb');
```

The important contract is that users should not guess live database paths or
native library paths in the normal Flutter-plugin path.

The explicit `libraryPath` fallback is only for custom/non-plugin loading:

```dart
final path = await DecentDbMobile.appDatabasePath('app.ddb');
final libraryPath = '/custom/path/to/libdecentdb.so';
final db = Database.open(path, libraryPath: libraryPath);
```

The `.ddb` extension is the recommended DecentDB database-file extension in
docs and examples. The engine does not require that extension; applications may
use another name when their storage policy requires it.

### 8.2 Sidecar Files

Docs and helpers must treat these as a single database set:

- main `.ddb` file;
- WAL sidecar;
- sync journal sidecar;
- coordination sidecar when process coordination is enabled.

This is the v1 mobile database-set contract. Any new authoritative sidecar or
manifest that mobile backup/restore must preserve requires an ADR or an update
to this spec. Move, delete, export, or restore workflows must not silently copy
only the main file unless the API explicitly creates a consistent backup/export
artifact.

The database-set contract is governed by the DecentDB database format version
and the mobile package version. Mobile backup/restore must preserve every file
in the v1 set that is present at the time of backup. The v1 mobile package does
not bundle `decentdb-migrate`; mobile apps should only claim in-app file-format
upgrade support when the Rust engine supports that upgrade on open or a
mobile-safe migration workflow is explicitly added and tested.

### 8.3 OS Backup And Cloud Sync

Default live database placement should not imply cloud-sync safety.

- App backup/restore behavior must be documented per platform.
- Live databases should not be placed in cloud-synced folders.
- If an app opts into OS backup for database files, restore behavior must
  include the whole database set and handle device identity/relay peer identity
  intentionally.
- Export/share workflows should use consistent backup/export APIs rather than
  exposing live files directly.

## 9. Lifecycle Contract

This section restates the implementation-facing requirements from ADR 0182.
If ADR 0182 changes, this section must be updated in the same branch.

Mobile apps have discontinuous execution. The SDK should make the safe path
boring and explicit.

### 9.1 Open And Close

- Apps should open a database when the app or feature enters an active state and
  close it explicitly when the owning component is done.
- Finalizers are a cleanup fallback, not a lifecycle policy.
- `close()` should either reject active statements/transactions or document the
  cleanup order clearly through the Dart API.
- Long-lived singleton handles are acceptable for foreground apps when lifecycle
  callbacks checkpoint/close according to policy.
- After process kill, app restart, isolate restart, or native-handle loss, all
  previous `Database` and `Statement` Dart objects are invalid. The app must
  reopen the database and recreate prepared statements. WAL recovery reclaims
  committed database state; native statement handles are process memory and are
  not durable resources.

### 9.2 Dart Isolates And Async Access

Mobile apps should not share one native `Database` handle across arbitrary Dart
isolates. The recommended mobile pattern is one owning isolate per database
handle, with app/UI code calling through an async facade or command queue. The
existing `async_database.dart` facade should be evaluated as the default mobile
pattern and extended only where mobile lifecycle or sync ergonomics require it.

For `AsyncDatabase`, the worker isolate is the owning isolate. Foreground,
background, close, checkpoint, and cancellation policies apply to that worker,
not merely to the UI isolate that holds futures. When the worker is closed or
terminated, pending futures must settle with a typed closed/canceled error, and
callers must reopen the database before issuing more work.

If an app opens separate handles from multiple isolates or processes, it must
use the documented process-coordination profile and accept the single-writer
contract. Tier 1 mobile support does not include multi-isolate shared-handle
access.

### 9.3 Foreground And Background

Recommended default policy:

- Foreground/resume: open or verify handle, run recovery if needed, then serve
  app queries.
- Background/inactive: stop admitting new UI writes, finish or cancel in-flight
  app work, checkpoint if configured, close if the app does not need a
  background task, and release native handles.
- Termination/crash: rely on WAL recovery; do not require application-managed
  repair for normal crashes.

Docs must be clear that `checkpoint()` improves startup/storage behavior but is
not required for committed durability when WAL sync mode is durable.

### 9.4 Background Sync

Background sync must be framed as opportunistic:

- Use OS schedulers to request work, not to promise exact delivery time.
- Keep tasks short and idempotent.
- Open the DB, apply relay changes locally, commit, then ack.
- If apply fails or the task is suspended before ack, the relay should redeliver
  from the durable checkpoint.
- Upload/push local changes only after they are committed locally.

The SDK may provide helper patterns, but it must not claim that iOS or Android
will run sync continuously.

### 9.5 Multiprocess And Extensions

Initial Tier 1 should be single app process. App extensions, widgets,
foreground services, or content providers sharing one database file must remain
candidate/unsupported until tests prove:

- file locks work on the target platform;
- process coordination sidecars are durable in the chosen app group/storage
  location;
- WAL retention and reader slot cleanup handle killed secondary processes;
- all participants use compatible DecentDB versions and open options.

## 10. TDE And Platform Key Stores

This section restates the implementation-facing requirements from ADR 0183.
If ADR 0183 changes, this section must be updated in the same branch.

TDE v1 accepts application-owned key bytes and encrypts the database, WAL, and
sync journal through the VFS layer. Mobile hardening should make this easy to
use without moving key ownership into the engine.

### 10.1 Required Guidance

Docs and examples must cover:

- generating high-entropy database keys;
- storing/wrapping keys in iOS Keychain and Android Keystore;
- passing key bytes to DecentDB only for open/create;
- redacting options and logs that contain key material, including
  `encryption_key`, `encryption_key_hex`, `tde_key`, and `tde_key_hex` in raw C
  ABI open-option strings;
- handling wrong-key, missing-key, biometric-lockout, device-restore, and
  reinstall scenarios;
- clearing temporary Dart/native key buffers as far as the platform permits;
- separating sync authentication credentials from database encryption keys.

Dart key clearing is best-effort. A `Uint8List` returned from a provider can be
overwritten by application code, but Dart GC, copies, and FFI conversions may
leave additional memory copies outside deterministic control. Mobile helpers
should minimize copies, prefer short-lived buffers, document the limitation, and
use FFI allocation/free or platform secure-storage APIs where that materially
reduces exposure. The spec must not imply C-style guaranteed zeroization for all
Dart-managed key bytes.

The FFI native allocation used to pass an options string can also contain key
material. `package:ffi` allocation/free does not guarantee zeroization of the
native copy after use. Mobile code should treat raw option strings containing
keys as sensitive, minimize their lifetime, avoid diagnostics/logging of raw
options, and prefer redacted structured summaries everywhere outside the
immediate open/create call.

### 10.2 API Shape

Accepted target helper shape:

```dart
abstract interface class DecentDbKeyProvider {
  Future<Uint8List> loadDatabaseKey();
}
```

The mobile plugin may ship examples or adapters for platform key stores. It
should not make the core engine responsible for prompting the user, choosing
biometric policies, or rotating keys.

### 10.3 Out Of Scope For This Win

- online key rotation;
- authenticated page/chunk encryption;
- remote KMS integrations;
- engine-owned key escrow;
- sync relay storage of database encryption keys.

Those belong under the authenticated encryption/key-rotation future win unless
implementation feedback proves a narrower prerequisite is required.

## 11. Sync Relay Mobile Contract

This section follows ADR 0166, ADR 0167, ADR 0168, and ADR 0182. If those ADRs
change the relay, public changeset, shape subscription, or mobile lifecycle
contract, this section must be updated in the same branch.

Mobile sync should reuse public changesets and the production relay protocol.

Required examples:

- foreground pull/apply/ack;
- background task pull/apply/ack with best-effort caveats;
- local changeset push after durable local commit;
- conflict inspection and retry guidance;
- scoped/tenant shape subscription;
- encrypted local database with relay sync credentials stored separately.

Apply-before-ack rule:

```dart
// Target API shape. Current Dart sync support may need wrappers over the
// existing C ABI JSON/public changeset entry points before this is available.
await db.transaction(() async {
  await db.sync.applyChangeset(message.changeset);
});
await subscription.ack(message);
```

The actual Dart API may differ, but the order must not. If the app acks before
the local commit is durable, a killed process can lose data while the relay
believes the client has advanced.

## 12. SDK Surface

The first mobile SDK should be intentionally small.

Required Flutter/mobile helpers:

- native library resolution for Android/iOS;
- safe app database path helper;
- optional no-backup database path helper;
- lifecycle observer helper or documented integration pattern;
- TDE key-provider example interface;
- relay apply-before-ack example helper or recipe;
- diagnostics surface that reports platform, artifact ABI, DecentDB ABI,
  database path class, redacted open options summary, and support tier.
- typed startup errors for native library load failure and ABI mismatch, with
  recovery guidance for package/artifact version alignment.
- `redactSensitiveOpenOptions()` or an equivalent `SanitizedOptions` helper that
  strips key material from `encryption_key`, `encryption_key_hex`, `tde_key`,
  and `tde_key_hex` before diagnostics, logs, support JSON, or troubleshooting
  output.
- documented reactive watch support status. If watch/change-stream APIs are
  exposed in the mobile package, their background, close, and restart semantics
  must be documented and tested; otherwise the mobile docs must mark reactive
  watch wrappers as deferred.

The existing Dart `Database` API should remain the main database API. Additions
to the pure Dart package should be limited to mobile-neutral improvements such
as better library loading, open options, structured errors, and sync helpers.

## 13. Tests And Validation

### 13.1 Unit And Package Tests

- Existing Dart package tests remain required.
- Add mobile-neutral Dart tests for library resolution and mobile option
  construction where possible without Flutter.
- Add C ABI smoke checks for any new exported surface.

### 13.2 Flutter Integration Tests

Minimum integration coverage:

- Android emulator create/open/query/close.
- iOS simulator create/open/query/close.
- encrypted create/open/reopen.
- prepared statement paging.
- transaction commit and rollback.
- checkpoint and reopen.
- save-as/export and restore/import pattern.
- app background/foreground lifecycle callback path.
- process kill or forced restart with WAL recovery.
- relay apply-before-ack with a mock relay or local relay.
- package asset loading without manual `libraryPath`.
- reference Flutter app scenarios for the same create/open, encrypted reopen,
  lifecycle, and sync apply-before-ack paths. The reference app may be the test
  host, or tests may drive it through a shared scenario harness.
- if mobile watch/change-stream APIs are exposed, validate watch delivery across
  foreground changes and verify watches are closed/invalidated predictably after
  background close or process restart.

Tier 1 additionally requires at least one real-device lane or documented
release-blocking device-lab process for each claimed platform.

### 13.3 Benchmark Guardrails

Record at least:

- native artifact size per architecture;
- app package size delta;
- cold open;
- warm open;
- first query;
- prepared point lookup loop;
- insert transaction batch;
- checkpoint;
- encrypted open overhead;
- sync changeset apply;
- memory before/after large result paging.

Guardrails should start broad and tighten from measured CI/device baselines.
Initial benchmark output is advisory until baselines are accepted; after that,
severe regressions in supported mobile lanes should fail release validation.
Durable defaults must not be weakened for mobile benchmark wins.

## 14. Documentation

Docs must include:

- mobile support matrix;
- Flutter install and packaging guide;
- Android and iOS path/storage recommendations;
- lifecycle cookbook for foreground/background/terminate;
- TDE key-store cookbook;
- relay sync apply-before-ack cookbook;
- troubleshooting table for library load, ABI mismatch, missing key, wrong key,
  busy/locked database, background task suspension, and restore issues;
- ABI mismatch troubleshooting must name the Dart exception/error shape, show
  expected vs loaded ABI values, and tell users to align the `decentdb`,
  `decentdb_flutter`, and packaged native artifact versions.
- diagnostics and troubleshooting output must only show redacted open-option
  summaries; raw options containing `encryption_key`, `encryption_key_hex`,
  `tde_key`, or `tde_key_hex` must never appear in logs or support JSON.
- release artifact verification instructions;
- unsupported environments and why they are unsupported.

Update targets when implementation lands:

- `docs/api/dart.md`;
- `bindings/dart/README.md`;
- new Flutter/mobile package README;
- `docs/index.md` language binding table;
- `docs/user-guide/security.md` for mobile key-store examples;
- `docs/user-guide/sync/relay.md` for mobile apply-before-ack examples;
- `docs/about/changelog.md`.

## 15. Phased Implementation Plan

Phase dependencies:

| Phase | Prerequisites |
|---|---|
| 0. ADR/spec validation and inventory | None. |
| 1. Native artifact build and loader | Phase 0 locked target matrix and package shape. |
| 2. Flutter mobile package and example app | Phase 1 packaged Android/iOS artifacts and loading smoke. |
| 3. TDE and key-store recipes | Phase 1 native artifacts, Phase 2 package path, and redacted options helper. |
| 4. Lifecycle and recovery | Phase 2 app/test host and async/owning-isolate policy. |
| 5. Relay sync mobile examples | Phase 2 app/test host plus Dart wrappers for public changeset apply/export, relay ack, and conflict inspection. |
| 6. Benchmarks, support matrix, and release guardrails | Prior phases plus mobile CI infrastructure. |

### Phase 0: ADR/Spec Validation And Inventory

- Validate and lock the accepted ADR/spec decisions for the first implementation
  slice; do not reopen package shape, mobile runtime contract, or key-store
  boundary without new evidence.
- Inventory Flutter stable, Rust target, Android NDK, iOS/Xcode, and CI
  constraints.
- Validate the initial Tier 2 simulator/emulator matrix and define explicit
  real-device promotion gates for Tier 1.
- Confirm the separate `decentdb_flutter` plugin layout and mobile workflow
  trigger policy.

### Phase 1: Native Artifact Build And Loader

- Add reproducible Android builds for selected ABIs.
- Add reproducible iOS XCFramework/static/dynamic build path.
- Extend Dart/native loading to support mobile link models.
- Add ABI/version smoke checks for packaged artifacts.

### Phase 2: Flutter Mobile Package And Example App

- Add the separate `decentdb_flutter` mobile plugin/package.
- Add a reference Flutter app.
- Demonstrate app-private paths, create/open, prepared statements, transaction,
  checkpoint, close, and reopen.
- Ensure users do not hand-copy native libraries in the happy path.

### Phase 3: TDE And Key-Store Recipes

- Add encrypted mobile create/open example.
- Add Keychain/Keystore example adapters or documented integration.
- Add wrong-key and missing-key tests.
- Verify logs and diagnostics redact key material.
- Add mobile-neutral redaction tests for raw C ABI open options containing
  `encryption_key`, `encryption_key_hex`, `tde_key`, and `tde_key_hex`.

### Phase 4: Lifecycle And Recovery

- Add lifecycle observer/example policy.
- Add background/foreground integration tests.
- Add process kill/reopen or crash-recovery tests.
- Add storage location and sidecar handling docs.

### Phase 5: Relay Sync Mobile Examples

- Add mobile apply-before-ack example.
- Add mock/local relay integration coverage.
- Document background scheduler limitations and retry/idempotency behavior.

### Phase 6: Benchmarks, Support Matrix, And Release Guardrails

- Add mobile benchmark scripts.
- Record initial baselines.
- Promote support tiers only for platforms with passing coverage.
- Add release packaging checks and documentation.
- Update changelog.

## 16. Acceptance Criteria

These criteria refine the Definition of Done in section 5 into implementation
and validation checks. If section 5 changes, this section must be reviewed in
the same branch.

- Mobile spec/ADR decisions are settled before implementation that affects
  lifecycle, key storage, packaging contracts, or C ABI.
- Flutter mobile package path works on Android and iOS without manual native
  library copying.
- Tier 1 support claims have release-blocking tests.
- Tier 2 support claims have release-blocking simulator/emulator tests and
  clearly documented real-device promotion gates.
- Mobile docs clearly describe app-private storage, sidecars, backups, TDE key
  handling, lifecycle, background sync, and unsupported environments.
- Mobile docs state the v1 database-set contract and require ADR/spec updates
  before adding authoritative sidecars to mobile backup/restore obligations.
- Mobile docs state that database handles and prepared statements are invalid
  after process/isolate restart and must be recreated after reopen.
- Mobile docs state the Dart key zeroing limitation and the best-effort buffer
  handling policy.
- Mobile diagnostics and support output redact TDE key open options.
- Mobile async/isolate docs identify the worker isolate as the database owner
  and specify pending-future behavior on close/termination.
- Sync examples preserve apply-before-ack ordering.
- TDE examples never log key material or store it beside the database.
- Benchmarks and package-size guardrails run in release validation.
- Existing Dart desktop/CLI behavior remains compatible.
- `docs/about/changelog.md` is updated when implementation lands.

## 17. Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Flutter packaging choices break pure Dart users | Prefer a thin Flutter plugin over adding Flutter dependencies to the pure Dart package. |
| iOS link model conflicts with current `cdylib` crate type | Validate early; add `staticlib`/XCFramework path if needed without changing C ABI semantics. |
| Mobile background sync is overpromised | Document OS best-effort scheduling and require apply-before-ack idempotency. |
| Keys leak through open option strings or logs | Provide key-provider recipes, redaction checks, and docs warning against logging options containing key material. |
| App restore duplicates relay/device identity | Document identity handling and keep sync credentials separate from encryption keys. |
| App extensions/widgets share DB unsafely | Keep out of Tier 1 until cross-process mobile tests prove lock and WAL behavior. |
| Package size becomes uncompetitive | Track artifact and app-size deltas in benchmark guardrails. |
| Native artifacts drift from Dart package version | Add ABI/version checks and release artifact verification. |

## 18. Implementation Decisions

These decisions are accepted for the first mobile production-runtime pass.
Future reviewers should challenge them only with concrete implementation,
packaging, or support evidence.

1. Mobile will ship as a separate thin `decentdb_flutter` package that depends
   on the existing pure Dart `decentdb` package. This preserves the desktop/CLI
   Dart surface, keeps Flutter dependencies out of pure Dart consumers, and
   gives mobile packaging/lifecycle helpers a clear home. Reconsider this only
   if package fragmentation makes installation materially harder for users.
2. The initial Android target is API 26+, with `arm64-v8a` for production
   devices and `x86_64` for emulator CI. `armeabi-v7a` is unsupported unless
   demand justifies the extra binary and test matrix.
3. The initial iOS target is iOS 15+. Package iOS as an XCFramework built from
   static-library-compatible Rust artifacts for device and simulator slices.
   GitHub-hosted macOS Actions are sufficient for unsigned XCFramework builds
   and simulator tests. Signed app/device validation requires Apple signing
   secrets and a separate real-device lane or documented device-lab process.
4. A new ADR is not required for packaging, path helpers, Flutter plugin shape,
   or simulator smoke tests. A new ADR is required before claiming mobile
   app-extension/widget shared database access, changing VFS locking, changing
   background durability semantics, or adding engine-owned key-store behavior.
5. Android emulator and iOS simulator lanes are release-blocking CI for the
   first implementation. A platform must not be labeled Tier 1 until a
   documented real-device lane exists. Until then, clean simulator/emulator
   coverage is Tier 2 or Candidate.
6. Mobile helpers will include a small key-provider interface plus reference
   Keychain/Keystore adapters in the Flutter package or example app. The core
   Dart package and Rust engine remain key-store agnostic.
7. App-extension/widget shared database access is explicitly deferred from Tier
   1. Candidate tests may be added after normal app-process mobile support is
   stable, because extension/widget sharing changes the locking, path,
   entitlement, and kill-recovery matrix.
8. Dart sync ergonomics must include first-class wrappers for public changeset
   apply/export, relay shape subscription, durable ack, conflict inspection, and
   a mobile `applyBeforeAck` helper. Common mobile relay examples must not
   require applications to assemble raw JSON strings manually.
9. Mobile release artifacts start in a separate mobile workflow, triggered
   manually and on tags, until build time, signing needs, Flutter setup, and
   simulator reliability are understood. Merge into the main release workflow
   only after the artifact contract is stable.
10. Normal mobile apps default to single app-process database use. Cross-process
    coordination is documented as off unless the app deliberately shares a
    database across processes/extensions. Set `process_coordination=required`
    only for a separately tested shared-access profile.

## 19. Resolved Guidance

- Release artifacts inherit the existing release asset pattern for the first
  unsigned native packages. Add checksums and provenance before promoting mobile
  artifacts to Tier 1. Do not block simulator-only prototype builds on app
  signing.
- Storage helpers should expose both backup-eligible and no-backup locations.
  The default app-private Application Support/internal files location is
  backup-eligible only when the app has an explicit restore/identity plan.
  Provide a named no-backup helper for device-local replicas and sync-derived
  caches.
- Examples default to one database per app profile/account. Tenant separation
  should use schema/sync scopes unless isolation, export/delete, or key policy
  requires separate files.
- Mobile support diagnostics start as a redacted diagnostics JSON generated
  from `sys.*` views, package/ABI metadata, path class, open-option summary, and
  recent DecentDB errors. Portable support bundles are deferred to the separate
  support-bundle future win.
- The first mobile claim pins to the current stable Flutter channel during
  implementation and documents the exact tested version range at release time.
  Do not claim broad Flutter-version compatibility before CI has a version
  matrix.
- Mobile examples include a small branch/snapshot smoke only if the existing
  Dart branch APIs work unchanged in mobile packaging. Branch UX must not block
  the core open/query/TDE/sync lifecycle claim.
- iOS and Android package builds may pass without Flutter integration tests only
  for early candidate artifacts. Supported release claims require Flutter app
  smoke coverage. A packaged native library without a Flutter app smoke test
  remains Candidate.
- Restoring an encrypted mobile database without the platform-stored key is
  unrecoverable unless the application has its own key escrow or backup policy.
  DecentDB should surface a clear wrong-key or missing-key error and must avoid
  silent database recreation.
