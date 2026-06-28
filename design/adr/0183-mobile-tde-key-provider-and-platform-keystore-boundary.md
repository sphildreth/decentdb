# Mobile TDE Key Provider And Platform Keystore Boundary
**Date:** 2026-05-27
**Status:** Accepted

### Decision

Mobile TDE support will keep DecentDB's existing key ownership model from ADR
0174: the engine accepts application-owned key bytes at create/open time and
does not store, rotate, unwrap, prompt for, escrow, or authenticate platform
keys.

The Flutter mobile package will provide a small key-provider abstraction and
reference Keychain/Keystore adapters or examples. The core Rust engine and pure
Dart package remain platform key-store agnostic.

The mobile key-provider abstraction should be narrow:

```dart
abstract interface class DecentDbKeyProvider {
  Future<Uint8List> loadOrCreateKey(String databaseId);
}
```

The exact Dart type may change during implementation, but the boundary must
remain the same: the provider returns bytes to the database open/create path;
DecentDB does not own user prompts, biometrics, keychain access groups, Android
Keystore authentication policies, key rotation, remote KMS access, or recovery
escrow.

Docs and examples must cover:

- generating high-entropy database keys;
- storing or wrapping keys in iOS Keychain and Android Keystore;
- passing key bytes to DecentDB only for create/open;
- avoiding logs or diagnostics that include key material or option strings with
  key material, including `encryption_key`, `encryption_key_hex`, `tde_key`,
  and `tde_key_hex`;
- clearing temporary Dart/native key buffers as far as the platform permits;
- handling missing keys, wrong keys, biometric lockout, reinstall, backup
  restore, and device migration;
- keeping sync authentication credentials separate from database encryption
  keys.

Restoring an encrypted mobile database without the platform-stored key is
unrecoverable unless the application has its own key escrow or backup policy.
DecentDB must surface a clear missing-key or wrong-key error and must not
silently recreate an encrypted database over restored data.

Dart key clearing is best-effort. A `Uint8List` returned from a provider can be
overwritten by application code, but Dart GC, copies, and FFI conversions may
leave additional memory copies outside deterministic control. Mobile helpers
should minimize copies, prefer short-lived buffers, document the limitation, and
use FFI allocation/free or platform secure-storage APIs where that materially
reduces exposure. The ADR does not require or imply C-style guaranteed
zeroization for all Dart-managed key bytes.

The C ABI option-string key transport from ADR 0174 is an accepted trade-off,
not a mobile-specific invention. The FFI native allocation used to pass an
options string can contain key material and `package:ffi` allocation/free does
not guarantee zeroization of the native copy after use. Mobile code must treat
raw option strings containing keys as sensitive, minimize their lifetime, avoid
diagnostics/logging of raw options, and provide a Dart-side
`redactSensitiveOpenOptions()` or equivalent sanitized-options helper for
support output.

Online key rotation, authenticated page/chunk encryption, remote KMS
integrations, and engine-owned key escrow are out of scope for this mobile win.
Those belong under future authenticated encryption/key-rotation work unless a
new ADR narrows the scope.

### Rationale

ADR 0174 intentionally made TDE an encryption-at-rest feature with
application-owned key bytes. That keeps the storage engine portable and avoids
embedding platform identity, prompts, biometrics, and recovery policy in the
core engine.

Mobile apps still need practical key-store examples. A provider interface gives
Flutter applications a safe, testable shape without turning DecentDB into an OS
secret manager. Reference adapters help users avoid storing encryption keys next
to the database while keeping policy choices in application code.

Key loss must be explicit. TDE is doing its job when data cannot be decrypted
without the key. Silent recreation would convert a security/lifecycle problem
into data loss.

### Alternatives Considered

1. **Store keys inside DecentDB metadata.** Rejected. It defeats local file
   encryption and mixes secret management with database storage.
2. **Make the Rust engine call platform key stores directly.** Rejected. It
   would add platform dependencies and UI/prompt policy to the core engine.
3. **Ship concrete Keychain/Keystore behavior in the pure Dart package.**
   Rejected. Pure Dart and desktop users should not depend on Flutter/mobile
   platform plugins.
4. **Require every app to write key-store code from scratch.** Rejected.
   Reference adapters/examples reduce adoption risk without changing engine
   ownership.
5. **Silently create a new database when a restored encrypted DB cannot be
   opened.** Rejected. This hides unrecoverable key loss and can destroy user
   data.

### Trade-offs

- Applications remain responsible for key lifecycle decisions, including backup
  and recovery.
- Reference adapters must avoid overclaiming security properties that depend on
  app entitlements, device settings, biometric policy, and OS version.
- Some users may want turnkey key rotation. That requires a separate security
  design because it affects file/key semantics beyond mobile packaging.

### Consequences

- Add mobile TDE examples for iOS Keychain and Android Keystore.
- Add missing-key and wrong-key mobile tests.
- Add redaction tests for diagnostics and logs that include open option
  summaries.
- Document key-loss and backup/restore behavior prominently.
- Keep sync credentials and database encryption keys separate in examples.

### References

- `design/_archive/WIN_MOBILE_PRODUCTION_RUNTIME_SDK_HARDENING_SPEC.md`
- `design/adr/0174-local-data-security-tde-policies-masking-audit-context.md`
- `docs/user-guide/security.md`
- `docs/api/configuration.md`
