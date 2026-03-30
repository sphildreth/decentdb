# ADR 0131: Handling Legacy Database Format Versions

**Date:** 2026-03-30
**Status:** Accepted

## Context
DecentDB strictly bounds its supported database file format to the current version (e.g., `DB_FORMAT_VERSION = 8`). When users attempt to open databases created by older versions of the engine, the engine safely fails with an error indicating an unsupported format version.

Historically, this error was reported as a generic `DbError::Corruption` with an embedded error string. As tools and bindings (like Dart and Decent Bench) begin encountering databases from prior test runs or legacy installations, a string-matched corruption error is inadequate. We need a way to reliably surface the unsupported format version to callers so they can take appropriate action, such as displaying a friendly message or invoking a migration.

Additionally, to keep the core engine lean and avoid bloating it with legacy file parsers or inline migration steps, we need an established pattern for how older database files are read, inspected, and eventually migrated without modifying the core `Db::open` path.

## Decision
We establish the following strategy for handling legacy database formats:

1. **Specific Error Code:** We introduce a distinct, stable numeric error code `DDB_ERR_UNSUPPORTED_FORMAT_VERSION` (`8`) in the C ABI, mapped from `DbError::UnsupportedFormatVersion` in Rust. This allows bindings to cleanly differentiate between actual file corruption and version mismatch errors.
2. **"Loose" Header Decoding:** The engine provides a `decode_loose` path for reading the database header. This bypasses the format version assertion to allow tools (like `decentdb-cli info`) to gracefully read metadata from unopenable files instead of hard-failing.
3. **Out-of-band Migrations:** The core engine will *not* contain logic to parse or migrate old file formats inline during `Db::open`. Instead, migrations will be handled out-of-band via a dedicated `migrate` subcommand in the `decentdb-cli`. The CLI will coordinate reading old files and writing them into the new format.

## Consequences

**Positive:**
- The core engine's critical path remains simple and fast, freed from the burden of maintaining N prior versions of storage logic.
- Bindings have a structural way to identify and handle version mismatches.
- `decentdb-cli` acts as the definitive tool for interacting with or upgrading older files, clarifying the boundary of responsibilities.

**Negative:**
- Users cannot transparently open old files with the embedded library; an explicit upgrade step via the CLI or a bundled migration logic is strictly required.

## Implementation Plan
- [x] Implement `DbError::UnsupportedFormatVersion` and update the C header with `DDB_ERR_UNSUPPORTED_FORMAT_VERSION`.
- [x] Expose `decode_loose` and static fallback inspection methods.
- [x] Update `decentdb-cli info` to leverage the loose header read and display format version gracefully.
- [ ] Future: Add a `migrate` subcommand to `decentdb-cli` utilizing specific migration binaries or snapshots.
