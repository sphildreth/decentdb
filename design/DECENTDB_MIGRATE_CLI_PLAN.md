# DecentDB Migration CLI Plan

**Status:** Draft / Active Implementation
**Date:** 2026-03-30

## 1. Introduction and Motivation

DecentDB is an embedded database engine that prioritizes performance, memory safety, and an extremely lean core footprint. As the project evolves, the on-disk file format inevitably changes to support new features, optimizations, or fundamental architecture shifts (such as the rewrite from Nim to Rust).

When a user attempts to open an older database file (e.g., format version 3) with the current engine (e.g., format version 8), the engine rightly rejects it. However, the user needs a native, first-party path to migrate their data to the current version.

### The Problem with "In-Core" Migrations
Building backward-compatible parsers into the core engine (`decentdb`) or the primary CLI (`decentdb-cli`) violates our core principles:
1. **Bloat:** Accumulating N versions of B-Tree, catalog, and page parsing logic will heavily bloat the binary size and compile times.
2. **Complexity:** Mixing legacy layout definitions with current layout definitions risks confusing the critical path and introducing bugs.
3. **Maintenance Burden:** The primary `decentdb-cli` is a high-traffic tool used for daily operations. It should not be burdened with dead-code parsers for 4-year-old formats.

### The Solution: `decentdb-migrate`
To resolve this, we are creating a dedicated, standalone executable and crate: `decentdb-migrate`. This tool serves one single purpose: safely extracting data from unsupported legacy formats and writing it into a new database file using the current format.

## 2. Architectural Strategy

The migration tool operates on a strict **Legacy-Read, Current-Write** architecture.

- **The Write Side:** The tool depends on the *current* `decentdb` crate workspace dependency. It uses the standard, fully-supported `Db::open_or_create` and SQL execution paths to write the destination database.
- **The Read Side:** The tool contains bespoke, read-only modules for historical format versions (e.g., `src/v3/`, `src/v4/`). These modules are completely stripped down. They do not contain WAL logic, transaction coordination, or query planners. They only contain the bare-minimum byte-parsing logic required to:
  1. Read the fixed header.
  2. Traverse the B-Tree pages.
  3. Deserialize catalog entries (schema).
  4. Deserialize table rows into primitive values.

This approach guarantees that legacy code is perfectly isolated from the current engine, while providing the user with a seamless native upgrade experience.

## 3. The Migration Pipeline

When a user executes `decentdb-migrate <source.ddb> <dest.ddb>`, the application performs the following pipeline:

1. **Header Inspection:** Reads the first 128 bytes of `<source.ddb>` using a loose parser to extract the `format_version`.
2. **Parser Selection:** Matches the `format_version` to a bundled legacy read module (e.g., if version == 3, instantiate `v3::Reader`). If the version is not supported, it yields a clear error.
3. **Destination Setup:** Opens or creates `<dest.ddb>` using the current `decentdb` engine (version 8).
4. **Schema Migration:**
   - The legacy reader traverses the old catalog tree.
   - It extracts table and index definitions.
   - It translates these definitions into standard DDL (`CREATE TABLE ...`) and executes them on `<dest.ddb>`.
5. **Data Streaming:**
   - For each table, the legacy reader traverses the old data B-Tree.
   - It yields rows of data.
   - The orchestrator batches these rows and executes `INSERT` statements into `<dest.ddb>` using the current engine's bulk-load or standard execution paths.
6. **Verification:** Validates row counts between the source and destination to ensure a lossless migration.

## 4. Supporting the Nim Era (Version 3)

The immediate driver for this tool is migrating users from the older Nim-based implementation (Format Version 3) to the current Rust-based implementation (Format Version 8).

To support Version 3, we will reverse-engineer a read-only parser based on the legacy Nim source code (specifically looking at `db_header.nim`, `storage.nim`, and B-Tree traversal logic).

The `v3` Rust module will need:
- Re-implementation of the Nim-era page header and cell layout structures.
- Logic to decode Nim's varint and payload format.
- Logic to traverse interior and leaf pages of the old B-Tree.

## 5. Step-by-Step Implementation Guide

1. **Scaffold Workspace:** Create `crates/decentdb-migrate` and add it to the workspace.
2. **Update Documentation:** Update ADR 0131 to formally recognize `decentdb-migrate` as the official migration path, superseding the idea of external legacy CLI orchestration.
3. **Update Primary CLI:** Modify `decentdb-cli migrate` (or remove it entirely) to point users to the new `decentdb-migrate` tool.
4. **Implement Orchestrator:** Set up `clap` in `decentdb-migrate` taking `--source` and `--dest`. Build the skeleton pipeline that reads the header and routes to a generic `LegacyReader` trait.
5. **Implement v3 Reader (Iterative):** Begin translating the Nim Version 3 read logic into Rust, starting with the header and catalog page, then moving to data pages and payload decoding.
6. **Documentation & Changelog:** Create the user guide at `docs/user-guide/migration.md` and update `docs/about/changelog.md`.

## 6. Testing Strategy

To ensure data integrity during migration from legacy formats, `decentdb-migrate` requires a robust testing strategy:

1. **Test Assets:**
   - Pre-generate version 3 (Nim-era) database files encompassing various scenarios: empty databases, single-table databases, multi-table databases, databases with large payloads (requiring overflow pages), and databases with varied data types (integers, floats, text, blobs).
   - Store these assets in a dedicated `tests/fixtures/legacy_v3/` directory or download them during test setup if size is a concern.

2. **Unit Testing the Reader:**
   - Write isolated tests within the `v3` module to parse specific known good pages (e.g., a catalog page, an interior B-Tree page, a leaf page) from the fixture databases.
   - Assert that the decoded row data matches expected hardcoded values.

3. **Integration Testing the CLI:**
   - Create a Rust integration test suite (e.g., `crates/decentdb-migrate/tests/integration_test.rs`).
   - The test will invoke the `decentdb-migrate` binary (or orchestrator library function) with a fixture source database and a temporary destination path.
   - After migration, the test will open the newly created destination database using the *current* `decentdb` engine crate.
   - It will run `SELECT COUNT(*)` and `SELECT *` queries to strictly verify that row counts and data values perfectly match the expected legacy state.
   - Verify that the migration tool exits gracefully when provided with an already-current database or an unsupported legacy version.
