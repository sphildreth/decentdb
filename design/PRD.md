# DecentDB: 1.0.0 Product Requirements & Goals

This document outlines the uncompromising goals for the DecentDB 1.0.0 release. All architectural decisions, pull requests, and AI-generated code must be evaluated against these 7 pillars.

## 1. ACID Compliance is Forefront
Data integrity is non-negotiable. It does not matter how fast the engine is; if it loses data, it is worthless. 
- Must survive sudden power loss, kernel panics, and process crashes without corruption.
- Write-Ahead Log (WAL) and `fsync` policies must be rigorously mathematically sound and verified via crash-injection testing.

## 2. Uncompromising Performance
Performance must beat SQLite on all fronts.
- **Reads:** Zero-copy deserialization and lock-free snapshot isolation for concurrent readers.
- **Writes:** Optimized WAL appending and background checkpointing.
- **Memory:** Absolute control over the buffer pool to prevent cache thrashing.

## 3. Minimal Disk Footprint
The size of files on disk must be optimized as much as possible. Smaller is better, provided it does not compromise ACID guarantees or performance.
- Use explicit byte-aligned memory layouts (`#[repr(C)]`, `#[repr(packed)]`) to eliminate padding bloat.
- Pack B-Tree nodes efficiently.

## 4. World-Class Documentation
Documentation must be accurate, continuously updated, and contain helpful examples.
- Hosted at [https://decentdb.org](https://decentdb.org) via GitHub Pages.
- All documentation lives natively in the `docs/` directory.
- Rust public APIs must use rustdoc with executable doctests to ensure examples never go out of date.

## 5. Best-in-Class Tooling & Bindings
DecentDB must feel like a native citizen in modern tech stacks. We require top-tier support for:
- Python (SQLAlchemy / DB-API)
- .NET Core Entity Framework
- .NET + Dapper
- Go (`database/sql`)
- Java (JDBC)
- Node.js
- Dart (FFI)

*Strategy:* Maintain a strict, highly optimized C-ABI boundary (`extern "C"`) in the Rust core to ensure all foreign bindings have zero-overhead access to the engine.

## 6. Fantastic CLI Experience
The DecentDB CLI tool must provide a best-in-class UX for creating, manipulating, querying, and updating `.ddb` database files.
- Should include rich terminal formatting, helpful error messages, and easy import/export capabilities.
- Should feel as robust as `psql` or the `sqlite3` CLI.

## 7. Fast Developer Feedback Loop
CI/CD must respect developer time.
- **PR Checks:** Must complete in **under 10 minutes**. This includes fast linting (`cargo clippy`), unit tests, and basic integration tests.
- **Overnight Actions:** All long-running tests (differential fuzzing against SQLite, million-row benchmarks, and intensive crash-injection loops) must be moved to nightly/overnight GitHub Actions.