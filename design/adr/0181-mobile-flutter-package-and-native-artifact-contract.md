# Mobile Flutter Package And Native Artifact Contract
**Date:** 2026-05-27
**Status:** Accepted

### Decision

DecentDB mobile support will start with Flutter and Dart, using a separate thin
`decentdb_flutter` package that depends on the existing pure Dart `decentdb`
package. The pure Dart package remains the C ABI wrapper for desktop, CLI, and
non-Flutter Dart users. The Flutter package owns mobile-native packaging,
platform path helpers, lifecycle helpers, mobile diagnostics, and reference
integration examples.

The Rust engine and stable C ABI remain the authoritative native boundary. The
first mobile pass must not add a mobile-specific native API layer or duplicate
the database surface in Swift/Kotlin. Mobile packaging must reuse existing C ABI
entry points for open/create/open-existing, prepared statements, transactions,
checkpoint/save-as, branch workflows, write queue options, process coordination,
TDE open options, and sync JSON/public changeset APIs. If implementation later
requires new C ABI functions or status codes, the C ABI version must be bumped
and every maintained binding must update its ABI expectation and smoke tests.

Initial Android support targets:

- Android API 26+;
- `arm64-v8a` for production devices;
- `x86_64` for emulator CI;
- no `armeabi-v7a` support unless measured demand justifies the additional
  binary and validation matrix.

Initial iOS support targets:

- iOS 15+;
- an XCFramework containing device and simulator slices;
- static-library-compatible Rust artifacts as the preferred link input;
- Flutter-compatible loading/registration that does not require applications to
  manually pass a `libraryPath` for the packaged happy path.

Mobile release artifacts begin in a separate mobile workflow, triggered manually
and on tags. That workflow may later merge into the main release workflow after
build time, Flutter setup, simulator reliability, artifact shape, and signing
needs are stable.

GitHub-hosted macOS Actions are acceptable for unsigned iOS XCFramework builds
and iOS simulator tests. Signed app builds, installable IPA production
artifacts, and real-device validation require Apple signing secrets and either a
separate real-device lane or a documented device-lab process.

Mobile packages must not download executable native code at runtime. Release
artifacts must be built from the same DecentDB version as Dart package metadata,
must include license notices, and must fail clearly when the native ABI version
does not match the Dart binding expectation.

### Rationale

Flutter is the lowest-friction mobile path because DecentDB already has a Dart
binding over the C ABI. Reusing that binding keeps SQL behavior, typed values,
statement paging, transactions, branch workflows, and sync wrappers aligned with
existing tests instead of creating another SDK contract.

A separate Flutter package avoids forcing Flutter dependencies and platform
folders into pure Dart users. It also gives mobile-specific concerns a clear
home: Android/iOS artifacts, platform path helpers, app lifecycle integration,
Keychain/Keystore examples, and simulator/device tests.

XCFramework is the standard distribution shape for iOS native libraries across
device and simulator slices. Static-library-compatible Rust artifacts avoid
relying on iOS dynamic-library behavior that is harder for Flutter applications
to consume consistently.

A separate mobile workflow keeps the main release pipeline stable while the
mobile package matures. Mobile builds require Flutter setup, iOS tooling, and
simulator/device validation that differ materially from the existing desktop
artifact workflow.

### Alternatives Considered

1. **Add Flutter platform folders directly to `bindings/dart/dart`.** Deferred.
   This may become acceptable if a separate package creates too much install
   friction, but it would couple pure Dart users to Flutter packaging concerns.
2. **Create Swift and Kotlin SDKs first.** Rejected for v1. It duplicates the
   binding surface before the mobile runtime contract is proven.
3. **Ship only raw Android/iOS native libraries.** Rejected. It would leave
   users to solve Flutter loading, paths, lifecycle, and diagnostics manually.
4. **Merge mobile artifact builds into the existing release workflow
   immediately.** Rejected. Build time, simulator reliability, signing, and
   artifact shape should stabilize in a separate workflow first.
5. **Support all Android ABIs in v1.** Rejected. `arm64-v8a` and emulator
   `x86_64` cover the production and CI baseline. Wider ABI support should be
   justified by demand and tests.
6. **Runtime download of native libraries.** Rejected. Mobile apps should bundle
   reviewed native code and satisfy app-store/platform expectations.

### Trade-offs

- Two Dart-facing packages create one more install concept for users, but keep
  responsibilities clean.
- iOS static/XCFramework packaging may require adding `staticlib` to the Rust
  crate build outputs.
- Simulator coverage can be automated on GitHub-hosted macOS runners, but real
  device validation needs additional infrastructure.
- Deferring Swift/Kotlin SDKs may leave some native-mobile teams waiting, but it
  prevents API fragmentation before the Flutter path is proven.

### Consequences

- Add a mobile package directory, likely under `bindings/dart/flutter` or
  `bindings/flutter`, with clear dependency on `bindings/dart/dart`.
- Add mobile artifact build scripts for Android and iOS.
- Add a separate GitHub Actions mobile workflow.
- Add ABI/version smoke checks for packaged Android and iOS artifacts.
- Update `docs/api/dart.md`, `bindings/dart/README.md`, package READMEs, release
  documentation, and `docs/about/changelog.md` when implementation lands.

### References

- `design/WIN_MOBILE_PRODUCTION_RUNTIME_SDK_HARDENING_SPEC.md`
- `include/decentdb.h`
- `bindings/dart/README.md`
- `docs/api/dart.md`
- `design/adr/0160-binding-native-semantic-data-types.md`
- `design/adr/0179-cross-process-public-contract-bindings-and-diagnostics.md`

