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
3. **Out-of-band Migrations:** The core engine and primary `decentdb-cli` will *not* contain logic to parse or migrate old file formats inline. Instead, migrations will be handled out-of-band via a dedicated standalone tool: `decentdb-migrate`.
   - `decentdb-migrate` will contain custom, read-only implementations of older format versions.
   - It will read the old data and stream it directly into the current engine's standard write paths.
   - This protects the core library and primary CLI from being bloated with years of dead-code parsers and legacy layout structs.

## Consequences

**Positive:**
- The core engine's critical path remains simple and fast, freed from the burden of maintaining N prior versions of storage logic.
- `decentdb-cli` does not suffer from binary bloat as legacy versions accumulate.
- Bindings have a structural way to identify and handle version mismatches.
- Users receive a seamless, native migration experience via a dedicated, first-party tool.

**Negative:**
- Users cannot transparently open old files with the embedded library; an explicit upgrade step via `decentdb-migrate` is strictly required.

## Implementation Plan
- [x] Implement `DbError::UnsupportedFormatVersion` and update the C header with `DDB_ERR_UNSUPPORTED_FORMAT_VERSION`.
- [x] Expose `decode_loose` and static fallback inspection methods.
- [x] Update `decentdb-cli info` to leverage the loose header read and display format version gracefully.
- [x] Add advisory message to `decentdb-cli` directing users to the migration tool.
- [x] Scaffold the standalone `decentdb-migrate` crate.
- [x] Implement legacy read-only parsers (starting with Nim-era v3) in `decentdb-migrate`.

## Ongoing Maintenance Rule
Whenever the core database format version is bumped (e.g., from `8` to `9`), the author of the format change MUST simultaneously contribute a read-only parser for the *previous* format version to the `decentdb-migrate` crate. This ensures that users always have a continuous, uninterrupted upgrade path across versions without polluting the core engine crate.
