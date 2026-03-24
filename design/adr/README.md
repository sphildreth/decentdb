# Architecture Decision Records (ADRs)

This directory contains the historical and active ADRs for DecentDB. 

> **Note on historical context:** Some earlier ADRs (0001 through 0117) reference
> older file paths or module names. The *architectural decisions* (for example
> WAL formats, B-Tree layouts, and SQL semantics) remain valid and binding for
> the current Rust engine.

### Recent Rust-Specific ADRs:
- **0118-rust-ffi-panic-safety.md**: Mandates `catch_unwind` on all C-ABI boundaries.
- **0119-rust-vfs-pread-pwrite.md**: Mandates standard file positional I/O over `unsafe mmap` for the Virtual File System.
- **0120-core-storage-engine-btree.md**: Formalizes the choice of an optimized B+Tree over an LSM-Tree for the core storage engine.
